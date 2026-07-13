defmodule Electric.Integration.OracleCausalOrderTest do
  @moduledoc """
  Deterministic regressions for ordering root-table replication and nested
  dependency moves from the same PostgreSQL transaction.
  """

  use ExUnit.Case, async: false

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.IntegrationSetup

  alias Support.OracleHarness
  alias Support.OracleHarness.StandardSchema

  @moduletag :oracle
  @moduletag timeout: :infinity
  @moduletag :tmp_dir

  setup [:with_unique_db]
  setup :use_persistent_slot
  setup :with_complete_stack

  setup ctx do
    ctx =
      with_electric_client(ctx,
        router_opts: [long_poll_timeout: 5_000],
        num_clients: 1
      )

    StandardSchema.setup_standard_schema(ctx)
    ctx
  end

  test "same-transaction nested dependency move cannot overtake its root fragment", ctx do
    # Put l3-3 outside the intermediate dependency view before the outer shape
    # snapshots. Its level_4 children must therefore be absent from the
    # materialized client and Materializer index.
    OracleHarness.apply_sql(ctx, [
      "UPDATE level_3 SET level_2_id = 'l2-4' WHERE id = 'l3-3'"
    ])

    shapes = [
      %{
        name: "same_txn_nested_dependency_and_root_move",
        table: "level_4",
        where:
          "(level_3_id IN (SELECT id FROM level_3 WHERE level_2_id IN " <>
            "(SELECT id FROM level_2 WHERE level_1_id = 'l1-1'))) OR (value LIKE '%5')",
        columns: ["id", "level_3_id", "value"],
        pk: ["id"],
        optimized: true
      }
    ]

    # These statements must share one PostgreSQL transaction. The level_3 root
    # fragment moves l3-3 into the old dependency view, while the level_2
    # update simultaneously expands that view to include l2-4. If the derived
    # dependency payload runs first, Shape.convert_change/3 misclassifies the
    # root transition as an update and Materializer crashes on the missing key.
    batches = [
      [
        [
          %{
            name: "move_l3_3_into_l2_1",
            sql: "UPDATE level_3 SET level_2_id = 'l2-1' WHERE id = 'l3-3'"
          },
          %{
            name: "move_l2_4_into_l1_1",
            sql: "UPDATE level_2 SET level_1_id = 'l1-1' WHERE id = 'l2-4'"
          }
        ]
      ],
      [
        [
          %{
            name: "post_restore_consistency_checkpoint",
            sql: "SELECT 1"
          }
        ]
      ]
    ]

    # ShapeChecker also verifies stream semantics: an update for a row absent
    # from the initial client state fails, and optimized shapes may not rotate
    # through a 409 must-refetch.
    assert :ok =
             OracleHarness.test_against_oracle(ctx, shapes, batches,
               oracle_pool_size: 1,
               timeout_ms: 20_000,
               restart_server_every: 1
             )
  end

  test "restored nested shape does not replay a phantom delete for a root no-op", ctx do
    # Recreate the durable state from the failing property-test seed before the
    # shape starts. l4-15 is below an active l3-5, but l3-5's l2-1 ancestor is
    # inactive, so the row must not be present in the initial shape snapshot.
    OracleHarness.apply_sql_transaction(ctx, [
      "UPDATE level_3 SET level_2_id = 'l2-1', active = true WHERE id = 'l3-5'",
      "UPDATE level_2 SET active = false WHERE id = 'l2-1'",
      "UPDATE level_4 SET level_3_id = 'l3-5' WHERE id = 'l4-15'",
      "UPDATE level_3 SET active = false WHERE id = 'l3-4'"
    ])

    shapes = [
      %{
        name: "restored_nested_shape",
        table: "level_4",
        where:
          "(level_3_id = 'l3-3') OR " <>
            "(level_3_id IN (SELECT id FROM level_3 WHERE active = true AND " <>
            "level_2_id IN (SELECT id FROM level_2 WHERE active = true)))",
        columns: ["id", "level_3_id", "value"],
        pk: ["id"],
        optimized: true
      }
    ]

    batches = [
      [
        [
          %{
            name: "move_l4_15_to_an_excluded_parent",
            sql: "UPDATE level_4 SET level_3_id = 'l3-4' WHERE id = 'l4-15'"
          },
          %{
            name: "activate_l2_1",
            sql: "UPDATE level_2 SET active = true WHERE id = 'l2-1'"
          }
        ]
      ],
      [
        [
          %{
            name: "post_restore_consistency_checkpoint",
            sql: "SELECT 1"
          }
        ]
      ]
    ]

    # In PostgreSQL, l4-15 is outside the predicate both before and after the
    # first transaction. The dependency move does add l3-5's other children.
    # After the restart, a stale root-delivery frontier replays the root update
    # against that post-transaction dependency state and emits an impossible
    # delete for l4-15. ShapeChecker rejects that operation even though the
    # final row set would otherwise still match the oracle.
    assert :ok =
             OracleHarness.test_against_oracle(ctx, shapes, batches,
               oracle_pool_size: 1,
               timeout_ms: 20_000,
               restart_server_every: 1
             )
  end

  test "restored three-level dependency removes rows when ancestor predicates collapse", ctx do
    # This is the reduced post-batch-1 state from property seed 424242. Every
    # level_3 row initially resolves through an inactive level_1 ancestor, so
    # the nested branch contains every level_4 row when the shape snapshots.
    OracleHarness.apply_sql_transaction(ctx, [
      "UPDATE level_1 SET active = false WHERE id IN ('l1-2', 'l1-4', 'l1-5')",
      "UPDATE level_1 SET active = true WHERE id IN ('l1-1', 'l1-3')",
      "UPDATE level_2 SET level_1_id = 'l1-2' WHERE id IN ('l2-1', 'l2-2')",
      "UPDATE level_2 SET level_1_id = 'l1-5' WHERE id = 'l2-3'",
      "UPDATE level_2 SET level_1_id = 'l1-4' WHERE id = 'l2-4'",
      "UPDATE level_2 SET level_1_id = 'l1-3' WHERE id = 'l2-5'",
      "UPDATE level_3 SET level_2_id = 'l2-4' WHERE id IN ('l3-1', 'l3-5')",
      "UPDATE level_3 SET level_2_id = 'l2-3' WHERE id IN ('l3-2', 'l3-3')",
      "UPDATE level_3 SET level_2_id = 'l2-2' WHERE id = 'l3-4'"
    ])

    shapes = [
      %{
        name: "restored_three_level_dependency_collapse",
        table: "level_4",
        where:
          "(id IN ('l4-11', 'l4-4')) OR " <>
            "(level_3_id IN (SELECT id FROM level_3 WHERE level_2_id IN " <>
            "(SELECT id FROM level_2 WHERE level_1_id IN " <>
            "(SELECT id FROM level_1 WHERE active = false))))",
        columns: ["id", "level_3_id", "value"],
        pk: ["id"],
        optimized: true
      }
    ]

    # Restart after a no-op checkpoint, then replay the dependency-only
    # transactions that collapsed the nested set from all five level_3 rows
    # to l3-1. The direct-id branch keeps l4-11 and l4-4 independently.
    batches = [
      [[mutation("SELECT 1")]],
      [
        [
          mutation("UPDATE level_3 SET level_2_id = 'l2-5' WHERE id = 'l3-3'"),
          mutation("UPDATE level_1 SET active = NOT active WHERE id = 'l1-2'"),
          mutation("UPDATE level_3 SET level_2_id = 'l2-3' WHERE id = 'l3-4'")
        ],
        [
          mutation("UPDATE level_2 SET level_1_id = 'l1-2' WHERE id = 'l2-5'"),
          mutation("UPDATE level_2 SET level_1_id = 'l1-4' WHERE id = 'l2-3'"),
          mutation("UPDATE level_3 SET level_2_id = 'l2-1' WHERE id = 'l3-1'")
        ],
        [
          mutation("UPDATE level_1 SET active = NOT active WHERE id = 'l1-2'"),
          mutation("UPDATE level_1 SET active = NOT active WHERE id = 'l1-3'"),
          mutation("UPDATE level_3 SET level_2_id = 'l2-2' WHERE id = 'l3-2'")
        ],
        [
          mutation("UPDATE level_2 SET level_1_id = 'l1-3' WHERE id = 'l2-4'"),
          mutation("UPDATE level_1 SET active = NOT active WHERE id = 'l1-2'"),
          mutation("UPDATE level_2 SET level_1_id = 'l1-4' WHERE id = 'l2-2'")
        ],
        [
          mutation("UPDATE level_1 SET active = NOT active WHERE id = 'l1-5'"),
          mutation("UPDATE level_1 SET active = NOT active WHERE id = 'l1-2'"),
          mutation("UPDATE level_3 SET level_2_id = 'l2-3' WHERE id = 'l3-1'")
        ],
        [mutation("UPDATE level_2 SET level_1_id = 'l1-2' WHERE id = 'l2-5'")],
        [mutation("UPDATE level_3 SET level_2_id = 'l2-5' WHERE id = 'l3-1'")],
        [
          mutation("UPDATE level_3 SET level_2_id = 'l2-3' WHERE id = 'l3-4'"),
          mutation("UPDATE level_3 SET level_2_id = 'l2-5' WHERE id = 'l3-4'")
        ],
        [
          mutation("UPDATE level_1 SET active = NOT active WHERE id = 'l1-4'"),
          mutation("UPDATE level_2 SET level_1_id = 'l1-5' WHERE id = 'l2-1'"),
          mutation("UPDATE level_3 SET level_2_id = 'l2-1' WHERE id = 'l3-4'")
        ],
        [
          mutation("UPDATE level_2 SET level_1_id = 'l1-2' WHERE id = 'l2-5'"),
          mutation("UPDATE level_3 SET level_2_id = 'l2-2' WHERE id = 'l3-3'"),
          mutation("UPDATE level_1 SET active = NOT active WHERE id = 'l1-3'")
        ]
      ]
    ]

    assert :ok =
             OracleHarness.test_against_oracle(ctx, shapes, batches,
               oracle_pool_size: 1,
               timeout_ms: 20_000,
               restart_server_every: 1
             )
  end

  defp mutation(sql), do: %{name: sql, sql: sql}

  defp use_persistent_slot(_ctx) do
    %{replication_opts_overrides: [slot_temporary?: false]}
  end
end
