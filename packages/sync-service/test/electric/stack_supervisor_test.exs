defmodule Electric.StackSupervisorTest do
  use ExUnit.Case, async: true
  use Repatch.ExUnit

  alias Electric.StackSupervisor

  import Support.ComponentSetup

  describe "initialization" do
    test "seeds subquery safety limits before starting the replay coordinator" do
      overrides = [
        materializer_replay_memory_limit_bytes: 2_001,
        materializer_replay_max_pending: 202,
        materializer_replay_idle_timeout_ms: 2_003,
        materializer_live_max_subscribers: 204,
        materializer_live_backlog_memory_limit_bytes: 2_005,
        materializer_causal_call_timeout_ms: 2_006,
        causal_drain_max_concurrency: 207,
        causal_drain_timeout_ms: 2_008,
        subquery_buffer_max_transactions: 209,
        subquery_deferred_event_memory_limit_bytes: 2_010
      ]

      opts =
        Electric.Application.configuration(
          [
            stack_id: "stack-supervisor-wiring-#{System.unique_integer([:positive])}",
            persistent_kv: Electric.PersistentKV.Memory.new!()
          ] ++ overrides
        )

      assert {:ok, config} =
               NimbleOptions.validate(Map.new(opts), StackSupervisor.opts_schema())

      assert {:ok, {_supervisor_flags, children}} = StackSupervisor.init(config)

      stack_config_index = Enum.find_index(children, &(&1.id == Electric.StackConfig))

      replay_coordinator_index =
        Enum.find_index(
          children,
          &(&1.id == Electric.Shapes.Consumer.Materializer.ReplayCoordinator)
        )

      assert replay_coordinator_index == stack_config_index + 1

      stack_config_child = Enum.at(children, stack_config_index)

      assert {Electric.StackConfig, :start_link, [stack_config_opts]} = stack_config_child.start
      seed_config = Keyword.fetch!(stack_config_opts, :seed_config)

      assert Keyword.take(seed_config, Keyword.keys(overrides)) == overrides
    end
  end

  describe "Telemetry" do
    setup [:with_stack_id_from_test]

    test "count_shapes/2 emits split shape metrics", ctx do
      stack_id = ctx.stack_id

      Repatch.patch(Electric.ShapeCache, :shape_counts, fn _stack_id ->
        %{total: 7, indexed: 4, unindexed: 3}
      end)

      Repatch.patch(Electric.Shapes.ConsumerRegistry, :active_consumer_count, fn _stack_id ->
        2
      end)

      handler_id = {__MODULE__, make_ref()}

      :telemetry.attach_many(
        handler_id,
        [
          [:electric, :shapes, :total_shapes],
          [:electric, :shapes, :active_shapes]
        ],
        fn event_name, measurements, metadata, pid ->
          send(pid, {event_name, measurements, metadata})
        end,
        self()
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      StackSupervisor.Telemetry.count_shapes(stack_id, %{})

      assert_receive {[:electric, :shapes, :total_shapes],
                      %{count: 7, count_indexed: 4, count_unindexed: 3}, %{stack_id: ^stack_id}}

      assert_receive {[:electric, :shapes, :active_shapes], %{count: 2}, %{stack_id: ^stack_id}}
    end
  end
end
