defmodule Electric.Integration.OraclePropertyTest do
  @moduledoc """
  Property-based oracle tests that run many parallel shapes with generated
  where clauses and mutations.

  Reproduce failures with: mix test --include oracle --seed <seed>

  Configuration via environment variables:
    - SHAPE_COUNT: Number of shapes to run in parallel (default: 100)
    - SHAPE_NAME: After generation, run only the named shape (for example,
      `shape_7`) without changing the seeded generator sequence.
    - SHAPE_NAMES: Comma-separated variant of SHAPE_NAME for reducing failures
      that require shared dependency consumers.
    - BATCH_COUNT: Number of batches per test (default: 10)
    - BATCH_LIMIT: After generation, execute only the first N batches without
      changing the seeded generator sequence.
    - TXNS_PER_BATCH: Number of transactions per batch (default: 10)
    - MUTATIONS_PER_TXN: Number of mutations per transaction (default: 5)
    - RUN_COUNT: Number of property test iterations (default: 1)
    - LONG_POLL_TIMEOUT: Server long-poll timeout in ms (default: 100)
    - RESTART_SERVER_EVERY: Stop and restart the sync stack every N batches to
      test server-side restore-from-file (default: 0, disabled). After each
      restart, fresh clients reconnect and check_initial_state asserts the
      restored state matches the oracle.
    - RESTART_CLIENT_EVERY: Throw away clients (poll cursors, materialized
      rows) and reconnect every M batches to test that fresh polls correctly
      assemble snapshot + log (default: 0, disabled). Independent of
      RESTART_SERVER_EVERY.
  """

  use ExUnit.Case, async: false
  use ExUnitProperties

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.IntegrationSetup
  import Support.OracleHarness
  alias Support.OracleHarness.StandardSchema
  alias Support.OracleHarness.WhereClauseGenerator

  @moduletag :oracle
  @moduletag timeout: :infinity
  @moduletag :tmp_dir
  @moduletag capture_log: false

  @default_long_poll_timeout 100
  @default_shape_count 100
  @default_batch_count 10
  @default_txns_per_batch 10
  @default_mutations_per_txn 5

  setup [:with_unique_db]
  setup :use_persistent_slot
  setup :with_complete_stack

  # Use a short long_poll_timeout to speed up tests - shapes with no changes
  # will get up_to_date faster instead of waiting 4 seconds for the default timeout.
  # Scale server and client connection pools to the shape count so we don't
  # hit Finch pool exhaustion with many concurrent long-polling shapes.
  setup ctx do
    long_poll_timeout = env_int("LONG_POLL_TIMEOUT") || @default_long_poll_timeout
    shape_count = env_int("SHAPE_COUNT") || @default_shape_count

    ctx =
      with_electric_client(ctx,
        router_opts: [long_poll_timeout: long_poll_timeout],
        num_clients: shape_count,
        # A transient 503 is itself a failure in this restart oracle. Do not
        # let the generic HTTP client's five-minute retry window hide the
        # first bad server state and erase the useful lifecycle evidence.
        fetch_timeout: 1
      )

    StandardSchema.setup_standard_schema(ctx)
    ctx
  end

  # The replication slot must survive the StackSupervisor restart used by
  # RESTART_SERVER_EVERY, otherwise Electric correctly treats the new slot
  # as a slot-loss event and purges all on-disk shape data — defeating the
  # restore-from-file scenario. Always run with a persistent slot; the slot
  # is dropped automatically with the per-test database in `after_suite`.
  defp use_persistent_slot(_ctx) do
    shape_count = env_int("SHAPE_COUNT") || @default_shape_count

    %{
      replication_opts_overrides: [slot_temporary?: false],
      db_pool_size: shape_count |> max(2) |> min(32)
    }
  end

  test "shapes with generated where clauses and mutations", ctx do
    run_count = env_int("RUN_COUNT") || 1
    shape_count = env_int("SHAPE_COUNT") || @default_shape_count
    batch_count = env_int("BATCH_COUNT") || @default_batch_count
    batch_limit = env_int("BATCH_LIMIT")
    txns_per_batch = env_int("TXNS_PER_BATCH") || @default_txns_per_batch
    mutations_per_txn = env_int("MUTATIONS_PER_TXN") || @default_mutations_per_txn
    restart_server_every = env_int("RESTART_SERVER_EVERY") || 0
    restart_client_every = env_int("RESTART_CLIENT_EVERY") || 0
    shape_names = System.get_env("SHAPE_NAMES") || System.get_env("SHAPE_NAME")

    total_mutations = batch_count * txns_per_batch * mutations_per_txn

    try do
      check all shapes <- WhereClauseGenerator.shapes_gen(shape_count),
                mutations <- StandardSchema.mutations_gen(total_mutations),
                max_runs: run_count do
        shapes = select_shapes!(shapes, shape_names)
        transactions = Enum.chunk_every(mutations, mutations_per_txn)
        batches = Enum.chunk_every(transactions, txns_per_batch)
        batches = if batch_limit, do: Enum.take(batches, batch_limit), else: batches

        test_against_oracle(ctx, shapes, batches,
          restart_server_every: restart_server_every,
          restart_client_every: restart_client_every
        )
      end
    after
      # A persistent test slot can keep DROP DATABASE waiting while the
      # restarted stack is still winding down. Stop it before ExUnit enters
      # after-suite database cleanup so the command itself exits cleanly.
      stop_supervised(Electric.StackSupervisor)
    end
  end

  defp select_shapes!(shapes, nil), do: shapes

  defp select_shapes!(shapes, shape_names) do
    requested = String.split(shape_names, ",", trim: true)

    case Enum.filter(shapes, &(&1.name in requested)) do
      [] -> raise "generated shapes not found: #{inspect(requested)}"
      selected -> selected
    end
  end
end
