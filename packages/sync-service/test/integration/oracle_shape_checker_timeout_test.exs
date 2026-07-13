defmodule Electric.Integration.OracleShapeCheckerTimeoutTest do
  use ExUnit.Case, async: false
  use Repatch.ExUnit

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.IntegrationSetup

  alias Support.OracleHarness
  alias Support.OracleHarness.ShapeChecker

  @moduletag :oracle
  @moduletag timeout: :infinity
  @moduletag :tmp_dir

  defmodule TimeoutFetch do
    @behaviour Electric.Client.Fetch

    @impl true
    def validate_opts(opts), do: {:ok, opts}

    @impl true
    def fetch(_request, _opts), do: {:error, :timeout}
  end

  setup [:with_unique_db]
  setup :with_complete_stack

  setup ctx do
    ctx =
      with_electric_client(ctx,
        router_opts: [long_poll_timeout: 5_000],
        num_clients: 1
      )

    OracleHarness.apply_sql(ctx, [
      "CREATE TABLE oracle_timeout_items (id TEXT PRIMARY KEY)",
      "INSERT INTO oracle_timeout_items (id) VALUES ('item-1')"
    ])

    ctx
  end

  test "a blocked poll fails as a shape convergence timeout, not a view mismatch", ctx do
    shape = %{
      name: "blocked_poll",
      table: "oracle_timeout_items",
      where: nil,
      columns: ["id"],
      pk: ["id"],
      optimized: false
    }

    {:ok, checker} = ShapeChecker.start_link(ctx, shape, ctx.db_conn, timeout_ms: 5_000)
    Process.unlink(checker)

    assert :ok = ShapeChecker.check_initial_state(checker)
    :sys.replace_state(checker, &%{&1 | timeout_ms: 50})

    test_pid = self()

    Repatch.patch(
      Electric.ShapeCache.ShapeStatus,
      :fetch_shape_by_handle,
      [mode: :shared],
      fn stack_id, handle ->
        if self() == checker do
          send(test_pid, :timeout_diagnostics_blocked)

          receive do
            :release_timeout_diagnostics -> :error
          end
        else
          Repatch.real(Electric.ShapeCache.ShapeStatus.fetch_shape_by_handle(stack_id, handle))
        end
      end
    )

    Repatch.patch(
      Electric.Shapes.ConsumerRegistry,
      :consumer_snapshot,
      [mode: :shared],
      fn stack_id ->
        if self() == checker do
          send(test_pid, :timeout_consumer_snapshot_blocked)

          receive do
            :release_timeout_diagnostics -> %{}
          end
        else
          Repatch.real(Electric.Shapes.ConsumerRegistry.consumer_snapshot(stack_id))
        end
      end
    )

    Repatch.patch(
      Electric.Shapes.ConsumerRegistry,
      :active_consumer_count,
      [mode: :shared],
      fn stack_id ->
        if self() == checker, do: send(test_pid, :timeout_active_consumer_count_called)
        Repatch.real(Electric.Shapes.ConsumerRegistry.active_consumer_count(stack_id))
      end
    )

    Repatch.allow(test_pid, checker)

    started_at = System.monotonic_time(:millisecond)
    task = Task.async(fn -> catch_exit(ShapeChecker.check_transaction(checker, "no_change")) end)
    result = Task.yield(task, 500)
    elapsed_ms = System.monotonic_time(:millisecond) - started_at

    if is_nil(result) do
      send(checker, :release_timeout_diagnostics)
      Task.yield(task, 500)
    end

    assert {:ok, reason} = result
    assert {{%ExUnit.AssertionError{} = error, _stacktrace}, {GenServer, :call, _args}} = reason
    assert Exception.message(error) =~ "Shape convergence timeout"
    refute Exception.message(error) =~ "View mismatch"
    refute_received :timeout_diagnostics_blocked
    refute_received :timeout_consumer_snapshot_blocked
    assert_received :timeout_active_consumer_count_called
    assert elapsed_ms < 500
  end

  test "a fetcher-originated timeout remains an ordinary poll error", ctx do
    shape = %{
      name: "fetch_timeout",
      table: "oracle_timeout_items",
      where: nil,
      columns: ["id"],
      pk: ["id"],
      optimized: false
    }

    {:ok, checker} = ShapeChecker.start_link(ctx, shape, ctx.db_conn, timeout_ms: 5_000)
    Process.unlink(checker)

    assert :ok = ShapeChecker.check_initial_state(checker)

    :sys.replace_state(checker, fn state ->
      %{state | client: %{state.client | fetch: {TimeoutFetch, []}}}
    end)

    reason = catch_exit(ShapeChecker.check_transaction(checker, "fetch_timeout"))

    assert {{%ExUnit.AssertionError{} = error, _stacktrace}, {GenServer, :call, _args}} = reason
    assert Exception.message(error) =~ "Poll error"
    refute Exception.message(error) =~ "Shape convergence timeout"
  end
end
