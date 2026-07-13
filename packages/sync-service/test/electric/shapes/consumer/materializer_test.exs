defmodule Electric.Shapes.Consumer.MaterializerTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog
  import Support.ComponentSetup
  use Repatch.ExUnit

  alias Electric.Shapes.Shape
  alias Electric.LogItems
  alias Electric.Replication.Changes
  alias Electric.ShapeCache.Storage
  alias Electric.Shapes.ConsumerRegistry
  alias Electric.Replication.LogOffset
  alias Electric.Shapes.Consumer.Materializer
  alias Electric.Shapes.Consumer.Materializer.ReplayCoordinator

  @moduletag :tmp_dir

  setup [
    :with_stack_id_from_test,
    :with_materializer_replay_coordinator,
    :with_async_deleter,
    :with_pure_file_storage,
    :with_consumer_registry
  ]

  @shape %Shape{
    root_table: {"public", "items"},
    root_table_id: 1,
    root_pk: ["id"],
    storage: %{compaction: :disabled}
  }

  setup %{storage: storage, stack_id: stack_id} = ctx do
    ConsumerRegistry.register_consumer(self(), "test", stack_id)

    Storage.for_shape("test", storage) |> Storage.start_link()
    writer = Storage.for_shape("test", storage) |> Storage.init_writer!(@shape)
    Storage.for_shape("test", storage) |> Storage.mark_snapshot_as_started()
    Storage.hibernate(writer)

    snapshot_data =
      Map.get(ctx, :snapshot_data, [])
      |> case do
        [] -> []
        [x | _] = items when is_map(x) -> make_snapshot_data(items)
        [x | _] = items when is_binary(x) -> items
        {items, opts} -> make_snapshot_data(items, opts)
      end

    Storage.for_shape("test", storage)
    |> then(&Storage.make_new_snapshot!(snapshot_data, &1))

    {:ok, shape_handle: "test", shape_storage: Storage.for_shape("test", storage), writer: writer}
  end

  test "can get ready",
       %{storage: storage, stack_id: stack_id, shape_handle: shape_handle} = ctx do
    {:ok, _pid} =
      Materializer.start_link(%{
        stack_id: stack_id,
        shape_handle: shape_handle,
        storage: storage,
        columns: ["value"],
        materialized_type: {:array, :int8}
      })

    respond_to_call(:await_snapshot_start, :started)

    respond_to_call(
      :subscribe_materializer,
      {:ok, LogOffset.last_before_real_offsets()}
    )

    assert Materializer.wait_until_ready(ctx) == :ok
  end

  test "subscribe waits for a materializer that is still initializing" do
    materializer =
      spawn(fn ->
        receive do
          {:"$gen_call", {caller, reference}, {:subscribe, nil}} ->
            Process.sleep(5_100)
            send(caller, {reference, :ok})
        end
      end)

    assert Materializer.subscribe(materializer) == :ok
  end

  test "new changes are materialized correctly",
       %{storage: storage, stack_id: stack_id, shape_handle: shape_handle} = ctx do
    {:ok, _pid} =
      Materializer.start_link(%{
        stack_id: stack_id,
        shape_handle: shape_handle,
        storage: storage,
        columns: ["value"],
        materialized_type: {:array, :int8}
      })

    respond_to_call(:await_snapshot_start, :started)

    respond_to_call(
      :subscribe_materializer,
      {:ok, LogOffset.last_before_real_offsets()}
    )

    assert Materializer.wait_until_ready(ctx) == :ok

    Materializer.new_changes(ctx, [
      %Changes.NewRecord{key: "1", record: %{"value" => "1"}},
      %Changes.NewRecord{key: "2", record: %{"value" => "2"}},
      %Changes.NewRecord{key: "3", record: %{"value" => "3"}}
    ])

    assert Materializer.get_link_values(ctx) == MapSet.new([1, 2, 3])
  end

  test "reclaims cached link values after every materializer lifetime", ctx do
    table = Materializer.link_values_table_name(ctx.stack_id)
    shape_handle = ctx.shape_handle
    baseline_size = :ets.info(table, :size)

    for value <- 1..3 do
      ctx = with_materializer(ctx)

      Materializer.new_changes(ctx, [
        %Changes.NewRecord{key: Integer.to_string(value), record: %{"value" => "#{value}"}}
      ])

      assert [{^shape_handle, cached_link_values}] = :ets.lookup(table, shape_handle)
      assert cached_link_values == MapSet.new([value])

      materializer_pid = Materializer.whereis(ctx)
      materializer_ref = Process.monitor(materializer_pid)
      Process.unlink(materializer_pid)
      GenServer.stop(materializer_pid, :shutdown)

      assert_receive {:DOWN, ^materializer_ref, :process, ^materializer_pid, :shutdown}
      assert [] == :ets.lookup(table, shape_handle)
      assert :ets.info(table, :size) == baseline_size
    end
  end

  describe "materializing non-pk selected columns" do
    test "runtime insert of a new value is seen & causes a move-in", ctx do
      ctx = with_materializer(ctx)

      Materializer.new_changes(ctx, [
        %Changes.NewRecord{key: "1", record: %{"value" => "1"}}
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new([1])

      assert_receive {:materializer_changes, _, %{move_in: [{1, "1"}]}}
    end

    test "fragmented list changes publish the transaction's final source offset", ctx do
      ctx = with_materializer(ctx)
      first_fragment_offset = LogOffset.new(100, 0)
      final_fragment_offset = LogOffset.new(100, 2)

      Materializer.new_changes(
        ctx,
        [%Changes.NewRecord{key: "1", record: %{"value" => "1"}}],
        commit: false,
        xid: 42,
        end_offset: first_fragment_offset
      )

      Materializer.new_changes(
        ctx,
        [%Changes.NewRecord{key: "2", record: %{"value" => "2"}}],
        commit: false,
        xid: 42,
        end_offset: final_fragment_offset
      )

      Materializer.new_changes(ctx, [], xid: 42, end_offset: final_fragment_offset)

      assert_receive {:materializer_changes, _, payload}
      assert payload.lsn == final_fragment_offset
      assert Enum.sort(payload.move_in) == [{1, "1"}, {2, "2"}]
    end

    test "defers derived moves and link values until the source offset is durable", ctx do
      ctx = with_materializer(ctx)
      move_offset = LogOffset.new(100, 0)

      writer =
        Storage.append_to_log!(main_log_insert(move_offset, "1", "1"), ctx.writer)

      _writer = Storage.hibernate(writer)

      Materializer.new_changes(
        ctx,
        {LogOffset.last_before_real_offsets(), move_offset},
        defer_until_durable: true
      )

      assert Materializer.get_link_values(ctx) == MapSet.new()
      refute_received {:materializer_changes, _, _}

      assert :ok = Materializer.durable_up_to(ctx, move_offset)
      assert Materializer.get_link_values(ctx) == MapSet.new([1])
      assert_receive {:materializer_changes, _, %{move_in: [{1, "1"}], lsn: ^move_offset}}
    end

    @tag with_pure_file_storage_opts: [chunk_bytes_threshold: 300]
    test "live multi-chunk storage ranges stop at their exact upper offset", ctx do
      ctx = with_materializer(ctx)
      offsets = for n <- 1..4, do: LogOffset.new(100 + n, 0)

      writer =
        offsets
        |> Enum.with_index(1)
        |> Enum.flat_map(fn {offset, n} ->
          value = Integer.to_string(n)
          main_log_insert(offset, value, value)
        end)
        |> Storage.append_to_log!(ctx.writer)

      _writer = Storage.hibernate(writer)
      range_end = Enum.at(offsets, 2)

      Materializer.new_changes(ctx, {LogOffset.last_before_real_offsets(), range_end})

      assert Materializer.get_link_values(ctx) == MapSet.new([1, 2, 3])
    end

    test "causal subscribers inherit queued batches and reserve later batches synchronously",
         ctx do
      ctx = with_materializer(ctx)
      first_offset = LogOffset.new(100, 0)
      second_offset = LogOffset.new(200, 0)

      Materializer.new_changes(
        ctx,
        [%Changes.NewRecord{key: "1", record: %{"value" => "1"}}],
        defer_until_durable: true,
        end_offset: first_offset
      )

      subscriber = spawn_causal_subscriber(self(), ctx)

      assert_receive {:causal_subscribed, ^subscriber,
                      {:ok, seed, initial_offset, [{first_token, first_bytes}]}}

      assert seed == MapSet.new()
      assert initial_offset == LogOffset.last_before_real_offsets()
      assert Materializer.causal_token_offset(first_token) == first_offset
      assert first_bytes > 0

      Materializer.new_changes(
        ctx,
        [%Changes.NewRecord{key: "2", record: %{"value" => "2"}}],
        defer_until_durable: true,
        end_offset: second_offset
      )

      assert_receive {:causal_reserved, ^subscriber, "test", second_token, ^second_offset}
      refute_received {:causal_message, ^subscriber, {:materializer_changes, _, _payload}}

      assert :ok = Materializer.durable_up_to(ctx, second_offset)

      assert_receive {:causal_message, ^subscriber,
                      {:materializer_changes, "test",
                       %{lsn: ^first_offset, causal_token: ^first_token}}}

      assert_receive {:causal_message, ^subscriber,
                      {:materializer_changes, "test",
                       %{lsn: ^second_offset, causal_token: ^second_token}}}

      state = :sys.get_state(Materializer.whereis(ctx))
      assert state.completed_event_batch_count == 0
      assert state.completed_event_batch_bytes == 0
      assert :queue.is_empty(state.completed_event_batches)
    end

    test "forwarded causal fences are inherited and end without a local batch", ctx do
      ctx = with_materializer(ctx)
      offset = LogOffset.new(100, 0)
      causal_token = Materializer.new_causal_token(offset)

      assert :ok = Materializer.forward_causal_begin(ctx, causal_token)
      subscriber = spawn_causal_subscriber(self(), ctx)

      assert_receive {:causal_subscribed, ^subscriber,
                      {:ok, _seed, _durable_offset, [{^causal_token, pending_bytes}]}}

      assert pending_bytes > 0

      assert :ok = Materializer.forward_causal_end(ctx, causal_token)

      assert_receive {:causal_message, ^subscriber,
                      {:materializer_causal_end, "test", ^causal_token}}

      refute_received {:causal_message, ^subscriber, {:materializer_changes, _, _payload}}
    end

    test "bounds durability-gated live batches by count before the queue can grow", ctx do
      ctx = with_materializer(ctx)
      materializer_pid = Materializer.whereis(ctx)
      Process.unlink(materializer_pid)
      ref = Process.monitor(materializer_pid)

      :sys.replace_state(materializer_pid, fn state ->
        %{state | live_backlog_max_pending: 1}
      end)

      Materializer.new_changes(
        ctx,
        [%Changes.NewRecord{key: "1", record: %{"value" => "1"}}],
        defer_until_durable: true,
        end_offset: LogOffset.new(100, 0)
      )

      state = :sys.get_state(materializer_pid)
      assert state.completed_event_batch_count == 1
      assert state.completed_event_batch_bytes > 0
      assert :queue.len(state.completed_event_batches) == 1

      assert catch_exit(
               Materializer.new_changes(
                 ctx,
                 [%Changes.NewRecord{key: "2", record: %{"value" => "2"}}],
                 defer_until_durable: true,
                 end_offset: LogOffset.new(200, 0)
               )
             )

      assert_receive {:DOWN, ^ref, :process, ^materializer_pid, _reason}
    end

    test "rejects one live batch larger than the materializer byte budget", ctx do
      ctx = with_materializer(ctx)
      materializer_pid = Materializer.whereis(ctx)
      Process.unlink(materializer_pid)
      ref = Process.monitor(materializer_pid)

      :sys.replace_state(materializer_pid, fn state ->
        %{state | live_backlog_memory_limit_bytes: 1}
      end)

      assert catch_exit(
               Materializer.new_changes(
                 ctx,
                 [%Changes.NewRecord{key: "1", record: %{"value" => "1"}}],
                 defer_until_durable: true,
                 end_offset: LogOffset.new(100, 0)
               )
             )

      assert_receive {:DOWN, ^ref, :process, ^materializer_pid, _reason}
    end

    test "bounds one fragmented source transaction before completed-batch admission", ctx do
      ctx = with_materializer(ctx)
      materializer_pid = Materializer.whereis(ctx)
      Process.unlink(materializer_pid)
      ref = Process.monitor(materializer_pid)

      Materializer.new_changes(
        ctx,
        [%Changes.NewRecord{key: "1", record: %{"value" => "1"}}],
        commit: false,
        xid: 42,
        end_offset: LogOffset.new(100, 0)
      )

      state = :sys.get_state(materializer_pid)
      retained_bytes = state.pending_change_bytes + state.pending_event_bytes
      assert retained_bytes > 0
      assert state.completed_event_batch_count == 0

      :sys.replace_state(materializer_pid, fn state ->
        %{state | live_backlog_memory_limit_bytes: retained_bytes}
      end)

      assert catch_exit(
               Materializer.new_changes(
                 ctx,
                 [%Changes.NewRecord{key: "2", record: %{"value" => "2"}}],
                 commit: false,
                 xid: 42,
                 end_offset: LogOffset.new(100, 1)
               )
             )

      assert_receive {:DOWN, ^ref, :process, ^materializer_pid, _reason}
    end

    test "a rejected causal re-subscription removes the prior monitor and live slot", ctx do
      ctx = with_materializer(ctx)
      materializer_pid = Materializer.whereis(ctx)
      parent = self()

      subscriber =
        spawn(fn ->
          initial = Materializer.subscribe_causally(materializer_pid, nil)
          send(parent, {:initial_causal_subscription, self(), initial})

          receive do
            :resubscribe_invalid ->
              result = Materializer.subscribe_causally(materializer_pid, :invalid)
              send(parent, {:invalid_causal_subscription, self(), result})
          end

          receive do: (:stop -> :ok)
        end)

      assert_receive {:initial_causal_subscription, ^subscriber, {:ok, _, _, []}}
      send(subscriber, :resubscribe_invalid)

      assert_receive {:invalid_causal_subscription, ^subscriber, {:error, :invalid_replay_cursor}}

      state = :sys.get_state(materializer_pid)
      refute MapSet.member?(state.causal_subscribers, subscriber)
      refute MapSet.member?(state.subscribers, subscriber)
      refute Map.has_key?(state.subscriber_monitors, subscriber)
      send(subscriber, :stop)
    end

    test "bounds the live subscriber monitor and fan-out set", ctx do
      ctx = with_materializer(ctx)
      materializer_pid = Materializer.whereis(ctx)

      :sys.replace_state(materializer_pid, fn state ->
        %{state | live_max_subscribers: 1}
      end)

      subscriber = spawn_causal_subscriber(self(), ctx)

      on_exit(fn ->
        if Process.alive?(subscriber), do: Process.exit(subscriber, :kill)
      end)

      assert_receive {:causal_subscribed, ^subscriber, {:error, :live_subscriber_limit}}

      state = :sys.get_state(materializer_pid)
      refute MapSet.member?(state.subscribers, subscriber)
      refute MapSet.member?(state.causal_subscribers, subscriber)
      refute Map.has_key?(state.subscriber_monitors, subscriber)
    end

    test "one hung causal subscriber does not prevent a healthy sibling from receiving", ctx do
      ctx = with_materializer(ctx)
      materializer_pid = Materializer.whereis(ctx)

      :sys.replace_state(materializer_pid, fn state ->
        %{state | causal_call_timeout_ms: 100}
      end)

      hung = spawn_hung_causal_subscriber(self(), ctx)
      healthy = spawn_causal_subscriber(self(), ctx)

      on_exit(fn ->
        for pid <- [hung, healthy], Process.alive?(pid), do: Process.exit(pid, :kill)
      end)

      assert_receive {:hung_causal_subscribed, ^hung, {:ok, _, _, []}}
      assert_receive {:causal_subscribed, ^healthy, {:ok, _, _, []}}

      offset = LogOffset.new(100, 0)

      Materializer.new_changes(
        ctx,
        [%Changes.NewRecord{key: "1", record: %{"value" => "1"}}],
        end_offset: offset
      )

      assert_receive {:causal_reserved, ^healthy, "test", healthy_token, ^offset}

      assert_receive {:causal_message, ^healthy,
                      {:materializer_changes, "test",
                       %{causal_token: ^healthy_token, lsn: ^offset}}}

      assert Process.alive?(healthy)
      assert Process.alive?(materializer_pid)
      refute MapSet.member?(:sys.get_state(materializer_pid).subscribers, hung)
    end

    test "bounds one causal delivery deadline across every hung subscriber", ctx do
      ctx = with_materializer(ctx)
      materializer_pid = Materializer.whereis(ctx)

      :sys.replace_state(materializer_pid, fn state ->
        %{state | causal_call_timeout_ms: 80}
      end)

      hung_subscribers =
        for _ <- 1..4 do
          spawn_delivery_hung_causal_subscriber(self(), ctx)
        end

      on_exit(fn ->
        for pid <- hung_subscribers, Process.alive?(pid), do: Process.exit(pid, :kill)
      end)

      for subscriber <- hung_subscribers do
        assert_receive {:delivery_hung_causal_subscribed, ^subscriber, {:ok, _, _, []}}
      end

      started_at = System.monotonic_time(:millisecond)

      Materializer.new_changes(
        ctx,
        [%Changes.NewRecord{key: "1", record: %{"value" => "1"}}],
        end_offset: LogOffset.new(100, 0)
      )

      elapsed_ms = System.monotonic_time(:millisecond) - started_at

      assert elapsed_ms < 200,
             "causal delivery multiplied one 80ms deadline across subscribers: #{elapsed_ms}ms"

      assert :sys.get_state(materializer_pid).subscribers == MapSet.new([self()])
    end

    test "delivers released causal batches synchronously instead of bursting mailboxes", ctx do
      ctx = with_materializer(ctx)
      subscriber = spawn_blocking_causal_subscriber(self(), ctx)

      on_exit(fn ->
        if Process.alive?(subscriber), do: Process.exit(subscriber, :kill)
      end)

      assert_receive {:blocking_causal_subscribed, ^subscriber, {:ok, _, _, []}}

      first_offset = LogOffset.new(100, 0)
      second_offset = LogOffset.new(200, 0)

      Materializer.new_changes(
        ctx,
        [%Changes.NewRecord{key: "1", record: %{"value" => "1"}}],
        defer_until_durable: true,
        end_offset: first_offset
      )

      Materializer.new_changes(
        ctx,
        [%Changes.NewRecord{key: "2", record: %{"value" => "2"}}],
        defer_until_durable: true,
        end_offset: second_offset
      )

      publisher = Task.async(fn -> Materializer.durable_up_to(ctx, second_offset) end)

      assert_receive {:causal_delivery_waiting, ^subscriber, ^first_offset}
      refute_receive {:causal_delivery_waiting, ^subscriber, ^second_offset}, 100
      send(subscriber, {:release_causal_delivery, first_offset})

      assert_receive {:causal_delivery_waiting, ^subscriber, ^second_offset}
      send(subscriber, {:release_causal_delivery, second_offset})

      assert Task.await(publisher) == :ok
    end

    test "does not advertise a physical mid-transaction flush as a replay cursor", ctx do
      ctx = with_materializer(ctx)
      initial_offset = LogOffset.last_before_real_offsets()
      first_mid_offset = LogOffset.new(100, 0)
      first_final_offset = LogOffset.new(100, 2)

      Materializer.new_changes(
        ctx,
        [%Changes.NewRecord{key: "1", record: %{"value" => "1"}}],
        commit: false,
        xid: 42,
        end_offset: first_mid_offset,
        defer_until_durable: true
      )

      Materializer.new_changes(
        ctx,
        [],
        xid: 42,
        end_offset: first_final_offset,
        defer_until_durable: true
      )

      assert :ok = Materializer.durable_up_to(ctx, first_mid_offset)
      assert Materializer.get_link_values(ctx) == MapSet.new()
      assert {:ok, initial_values, ^initial_offset} = Materializer.subscribe(ctx)
      assert initial_values == MapSet.new()
      refute_received {:materializer_changes, _, _}

      assert :ok = Materializer.durable_up_to(ctx, first_final_offset)
      assert Materializer.get_link_values(ctx) == MapSet.new([1])
      assert {:ok, first_values, ^first_final_offset} = Materializer.subscribe(ctx)
      assert first_values == MapSet.new([1])

      assert_receive {:materializer_changes, _, %{move_in: [{1, "1"}], lsn: ^first_final_offset}}

      second_mid_offset = LogOffset.new(200, 0)
      second_final_offset = LogOffset.new(200, 2)

      Materializer.new_changes(
        ctx,
        [%Changes.NewRecord{key: "2", record: %{"value" => "2"}}],
        commit: false,
        xid: 43,
        end_offset: second_mid_offset,
        defer_until_durable: true
      )

      Materializer.new_changes(
        ctx,
        [],
        xid: 43,
        end_offset: second_final_offset,
        defer_until_durable: true
      )

      assert :ok = Materializer.durable_up_to(ctx, second_mid_offset)
      assert Materializer.get_link_values(ctx) == MapSet.new([1])
      assert {:ok, still_first_values, ^first_final_offset} = Materializer.subscribe(ctx)
      assert still_first_values == MapSet.new([1])
      refute_received {:materializer_changes, _, _}

      assert :ok = Materializer.durable_up_to(ctx, second_final_offset)
      assert Materializer.get_link_values(ctx) == MapSet.new([1, 2])
      assert {:ok, second_values, ^second_final_offset} = Materializer.subscribe(ctx)
      assert second_values == MapSet.new([1, 2])

      assert_receive {:materializer_changes, _, %{move_in: [{2, "2"}], lsn: ^second_final_offset}}
    end

    test "keeps volatile link values out of ETS after publishing an earlier durable batch", ctx do
      ctx = with_materializer(ctx)
      durable_offset = LogOffset.new(100, 0)
      volatile_offset = LogOffset.new(200, 0)

      writer =
        Storage.append_to_log!(main_log_insert(durable_offset, "1", "1"), ctx.writer)

      writer = Storage.hibernate(writer)

      Materializer.new_changes(
        ctx,
        {LogOffset.last_before_real_offsets(), durable_offset},
        defer_until_durable: true
      )

      assert :ok = Materializer.durable_up_to(ctx, durable_offset)

      writer =
        Storage.append_to_log!(main_log_insert(volatile_offset, "2", "2"), writer)

      _writer = Storage.hibernate(writer)

      Materializer.new_changes(
        ctx,
        {durable_offset, volatile_offset},
        defer_until_durable: true
      )

      table = Materializer.link_values_table_name(ctx.stack_id)
      shape_handle = ctx.shape_handle
      assert [{^shape_handle, cached_link_values}] = :ets.lookup(table, shape_handle)
      assert cached_link_values == MapSet.new([1])
      assert Materializer.get_link_values(ctx) == MapSet.new([1])

      assert_receive {:materializer_changes, _, %{move_in: [{1, "1"}], lsn: ^durable_offset}}
      refute_received {:materializer_changes, _, %{move_in: [{2, "2"}]}}

      assert :ok = Materializer.durable_up_to(ctx, volatile_offset)
      assert Materializer.get_link_values(ctx) == MapSet.new([1, 2])
      assert_receive {:materializer_changes, _, %{move_in: [{2, "2"}], lsn: ^volatile_offset}}
    end

    test "publishes a deferred batch when durability arrived before its range", ctx do
      ctx = with_materializer(ctx)
      move_offset = LogOffset.new(100, 0)

      assert :ok = Materializer.durable_up_to(ctx, move_offset)

      assert {:ok, published_link_values, published_offset} = Materializer.subscribe(ctx)
      assert published_link_values == MapSet.new()
      assert published_offset == LogOffset.last_before_real_offsets()

      writer =
        Storage.append_to_log!(main_log_insert(move_offset, "1", "1"), ctx.writer)

      _writer = Storage.hibernate(writer)

      Materializer.new_changes(
        ctx,
        {LogOffset.last_before_real_offsets(), move_offset},
        defer_until_durable: true
      )

      assert Materializer.get_link_values(ctx) == MapSet.new([1])
      assert_receive {:materializer_changes, _, %{move_in: [{1, "1"}], lsn: ^move_offset}}
    end

    test "retains the maximum early durability signal across an older notification", ctx do
      ctx = with_materializer(ctx)
      move_offset = LogOffset.new(200, 0)

      assert :ok = Materializer.durable_up_to(ctx, move_offset)
      assert :ok = Materializer.durable_up_to(ctx, LogOffset.new(100, 0))

      writer =
        Storage.append_to_log!(main_log_insert(move_offset, "1", "1"), ctx.writer)

      _writer = Storage.hibernate(writer)

      Materializer.new_changes(
        ctx,
        {LogOffset.last_before_real_offsets(), move_offset},
        defer_until_durable: true
      )

      assert Materializer.get_link_values(ctx) == MapSet.new([1])
      assert_receive {:materializer_changes, _, %{move_in: [{1, "1"}], lsn: ^move_offset}}
    end

    test "publishes a normalized cursor-only payload for a durable no-op transaction", ctx do
      ctx = with_materializer(ctx)
      no_op_offset = LogOffset.new(100, 0)

      Materializer.new_changes(
        ctx,
        [],
        defer_until_durable: true,
        end_offset: no_op_offset
      )

      refute_received {:materializer_changes, _, _}
      assert :ok = Materializer.durable_up_to(ctx, no_op_offset)

      assert_receive {:materializer_changes, _, payload}
      assert payload == %{move_in: [], move_out: [], txids: [], lsn: no_op_offset}
    end

    test "the supplied xid is included in the broadcast payload's txids", ctx do
      ctx = with_materializer(ctx)

      Materializer.new_changes(
        ctx,
        [%Changes.NewRecord{key: "1", record: %{"value" => "1"}}],
        xid: 4242
      )

      assert_receive {:materializer_changes, _, %{move_in: [{1, "1"}], txids: [4242]}}
    end

    test "txids accumulate across multiple non-committing batches", ctx do
      ctx = with_materializer(ctx)

      Materializer.new_changes(
        ctx,
        [%Changes.NewRecord{key: "1", record: %{"value" => "1"}}],
        xid: 100,
        commit: false
      )

      Materializer.new_changes(
        ctx,
        [%Changes.NewRecord{key: "2", record: %{"value" => "2"}}],
        xid: 200,
        commit: true
      )

      assert_receive {:materializer_changes, _,
                      %{move_in: [{1, "1"}, {2, "2"}], txids: [100, 200]}}
    end

    @tag snapshot_data: [%Changes.NewRecord{record: %{"id" => "1", "value" => "10"}}]
    test "on-load insert of a new value is seen & does not cause a move-in", ctx do
      ctx = with_materializer(ctx)

      assert Materializer.get_link_values(ctx) == MapSet.new([10])

      refute_received {:materializer_changes, _, _}
    end

    @tag snapshot_data: [%Changes.NewRecord{record: %{"id" => "1", "value" => "10"}}]
    test "runtime update of a value is seen & causes a move-out & move-in", ctx do
      ctx = with_materializer(ctx)

      Materializer.new_changes(
        ctx,
        [
          %Changes.UpdatedRecord{
            record: %{"id" => "1", "value" => "11"},
            old_record: %{"id" => "1", "value" => "10"}
          }
        ]
        |> prep_changes()
      )

      assert Materializer.get_link_values(ctx) == MapSet.new([11])

      assert_receive {:materializer_changes, _, %{move_out: [{10, "10"}], move_in: [{11, "11"}]}}
    end

    @tag snapshot_data: [
           %Changes.NewRecord{record: %{"id" => "1", "value" => "10"}},
           Changes.UpdatedRecord.new(
             record: %{"id" => "1", "value" => "11"},
             old_record: %{"id" => "1", "value" => "10"}
           )
         ]
    test "on-load update of a value is seen & does not cause events", ctx do
      ctx = with_materializer(ctx)
      assert Materializer.get_link_values(ctx) == MapSet.new([11])
      refute_received {:materializer_changes, _, _}
    end

    @tag snapshot_data: [%Changes.NewRecord{record: %{"id" => "1", "value" => "10"}}]
    test "runtime delete of a value is seen & causes a move-out", ctx do
      ctx = with_materializer(ctx)

      Materializer.new_changes(
        ctx,
        [%Changes.DeletedRecord{old_record: %{"id" => "1", "value" => "10"}}] |> prep_changes()
      )

      assert Materializer.get_link_values(ctx) == MapSet.new([])

      assert_receive {:materializer_changes, _, %{move_out: [{10, "10"}]}}
    end

    @tag snapshot_data: [
           %Changes.NewRecord{record: %{"id" => "1", "value" => "10"}},
           %Changes.DeletedRecord{old_record: %{"id" => "1", "value" => "10"}}
         ]
    test "on-load delete of a value is seen & does not cause events", ctx do
      ctx = with_materializer(ctx)

      assert Materializer.get_link_values(ctx) == MapSet.new([])

      refute_received {:materializer_changes, _, _}
    end

    @tag snapshot_data: [%Changes.NewRecord{record: %{"id" => "1", "value" => "10"}}]
    test "insert of a value that's already present in the shape does not cause events", ctx do
      ctx = with_materializer(ctx)

      Materializer.new_changes(
        ctx,
        [%Changes.NewRecord{record: %{"id" => "2", "value" => "10"}}] |> prep_changes()
      )

      assert Materializer.get_link_values(ctx) == MapSet.new([10])

      refute_received {:materializer_changes, _, _}
    end

    @tag snapshot_data: [
           %Changes.NewRecord{record: %{"id" => "1", "value" => "10"}},
           %Changes.NewRecord{record: %{"id" => "2", "value" => "20"}}
         ]
    test "update of a value to a present value causes just a move-out", ctx do
      ctx = with_materializer(ctx)

      assert Materializer.get_link_values(ctx) == MapSet.new([10, 20])

      Materializer.new_changes(
        ctx,
        [
          %Changes.UpdatedRecord{
            record: %{"id" => "1", "value" => "20"},
            old_record: %{"id" => "1", "value" => "10"}
          }
        ]
        |> prep_changes()
      )

      assert Materializer.get_link_values(ctx) == MapSet.new([20])

      assert_received {:materializer_changes, _, %{move_out: [{10, "10"}]}}
    end

    @tag snapshot_data: [
           %Changes.NewRecord{record: %{"id" => "1", "value" => "10"}},
           %Changes.NewRecord{record: %{"id" => "2", "value" => "10"}}
         ]
    test "update of a value to a non-present value causes a move-in", ctx do
      ctx = with_materializer(ctx)

      assert Materializer.get_link_values(ctx) == MapSet.new([10])

      Materializer.new_changes(
        ctx,
        [
          %Changes.UpdatedRecord{
            record: %{"id" => "1", "value" => "20"},
            old_record: %{"id" => "1", "value" => "10"}
          }
        ]
        |> prep_changes()
      )

      assert Materializer.get_link_values(ctx) == MapSet.new([10, 20])

      assert_received {:materializer_changes, _, %{move_in: [{20, "20"}]}}
    end

    @tag snapshot_data: [
           %Changes.NewRecord{record: %{"id" => "1", "value" => "10"}},
           %Changes.NewRecord{record: %{"id" => "2", "value" => "20"}},
           %Changes.NewRecord{record: %{"id" => "3", "value" => "10"}}
         ]
    test "update between otherwise present values causes no events", ctx do
      ctx = with_materializer(ctx)

      assert Materializer.get_link_values(ctx) == MapSet.new([10, 20])

      Materializer.new_changes(
        ctx,
        [
          %Changes.UpdatedRecord{
            record: %{"id" => "1", "value" => "20"},
            old_record: %{"id" => "1", "value" => "10"}
          }
        ]
        |> prep_changes()
      )

      assert Materializer.get_link_values(ctx) == MapSet.new([10, 20])

      refute_received {:materializer_changes, _, _}
    end

    @tag snapshot_data: [
           %Changes.NewRecord{record: %{"id" => "1", "value" => "10"}},
           %Changes.NewRecord{record: %{"id" => "2", "value" => "10"}}
         ]
    test "delete of an otherwise present value causes no events", ctx do
      ctx = with_materializer(ctx)

      Materializer.new_changes(
        ctx,
        [%Changes.DeletedRecord{old_record: %{"id" => "1", "value" => "10"}}] |> prep_changes()
      )

      assert Materializer.get_link_values(ctx) == MapSet.new([10])

      refute_received {:materializer_changes, _, _}
    end

    @tag snapshot_data: [
           %Changes.NewRecord{record: %{"id" => "1", "value" => "10"}},
           %Changes.NewRecord{record: %{"id" => "2", "value" => "10"}}
         ]
    test "insert of an otherwise present value causes no events", ctx do
      ctx = with_materializer(ctx)

      Materializer.new_changes(
        ctx,
        [%Changes.NewRecord{record: %{"id" => "3", "value" => "10"}}] |> prep_changes()
      )

      assert Materializer.get_link_values(ctx) == MapSet.new([10])

      refute_received {:materializer_changes, _, _}
    end

    @tag snapshot_data: [
           %Changes.NewRecord{record: %{"id" => "1", "value" => "10"}}
         ]
    test "insert of a PK we've already seen raises", ctx do
      ctx = with_materializer(ctx)

      assert Materializer.get_link_values(ctx) == MapSet.new([10])

      pid = GenServer.whereis(Materializer.name(ctx))
      Process.unlink(pid)

      try do
        Materializer.new_changes(
          ctx,
          [%Changes.NewRecord{record: %{"id" => "1", "value" => "10"}}] |> prep_changes()
        )
      catch
        :exit, {{reason, _}, _} ->
          assert reason.message =~ ~r/Key .* already exists/
      end
    end

    test "delete of a PK we've not seen throws an error", ctx do
      ctx = with_materializer(ctx)

      assert Materializer.get_link_values(ctx) == MapSet.new([])

      pid = GenServer.whereis(Materializer.name(ctx))
      Process.unlink(pid)

      capture_log(fn ->
        try do
          Materializer.new_changes(
            ctx,
            [%Changes.DeletedRecord{old_record: %{"id" => "1", "value" => "10"}}]
            |> prep_changes()
          )
        catch
          :exit, {{reason, _}, _} ->
            assert %KeyError{key: _} = reason
        end
      end)
    end

    @tag snapshot_data: {
           [%Changes.NewRecord{record: %{"id" => "1", "value" => "10"}}],
           [pk_cols: ["id"]]
         }
    test "update that changes the primary key is handled correctly", ctx do
      ctx = with_materializer(ctx)

      assert Materializer.get_link_values(ctx) == MapSet.new([10])

      # Update where the PK changes from "1" to "2"
      Materializer.new_changes(
        ctx,
        [
          %Changes.UpdatedRecord{
            record: %{"id" => "2", "value" => "20"},
            old_record: %{"id" => "1", "value" => "10"}
          }
        ]
        |> prep_changes()
      )

      assert Materializer.get_link_values(ctx) == MapSet.new([20])

      assert_receive {:materializer_changes, _, %{move_out: [{10, "10"}], move_in: [{20, "20"}]}}
    end

    @tag snapshot_data: {
           [%Changes.NewRecord{record: %{"id" => "1", "value" => "10"}}],
           [pk_cols: ["id"]]
         }
    test "update that changes the primary key but keeps the same value", ctx do
      ctx = with_materializer(ctx)

      assert Materializer.get_link_values(ctx) == MapSet.new([10])

      # Update where the PK changes but tracked value stays the same
      Materializer.new_changes(
        ctx,
        [
          %Changes.UpdatedRecord{
            record: %{"id" => "2", "value" => "10"},
            old_record: %{"id" => "1", "value" => "10"}
          }
        ]
        |> prep_changes()
      )

      # Value should still be present
      assert Materializer.get_link_values(ctx) == MapSet.new([10])

      # No events since the tracked value didn't change
      refute_received {:materializer_changes, _, _}
    end

    @tag snapshot_data: {
           [
             %Changes.NewRecord{
               record: %{"id" => "1", "value" => "10"},
               move_tags: ["tag_a"]
             }
           ],
           [pk_cols: ["id"]]
         }
    test "update that changes PK and tag updates tag indices correctly", ctx do
      ctx = with_materializer(ctx)

      assert Materializer.get_link_values(ctx) == MapSet.new([10])

      # Update where PK changes and tag changes
      Materializer.new_changes(
        ctx,
        [
          %Changes.UpdatedRecord{
            record: %{"id" => "2", "value" => "20"},
            old_record: %{"id" => "1", "value" => "10"},
            move_tags: ["tag_b"],
            removed_move_tags: ["tag_a"]
          }
        ]
        |> prep_changes()
      )

      assert Materializer.get_link_values(ctx) == MapSet.new([20])
      assert_receive {:materializer_changes, _, %{move_out: [{10, "10"}], move_in: [{20, "20"}]}}

      # move_out for old tag should find nothing (old_key fully removed)
      Materializer.new_changes(ctx, [
        %{headers: %{event: "move-out", patterns: [%{pos: 0, value: "tag_a"}]}}
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new([20])
      refute_received {:materializer_changes, _, _}

      # move_out for new tag should remove the row using the new key
      Materializer.new_changes(ctx, [
        %{headers: %{event: "move-out", patterns: [%{pos: 0, value: "tag_b"}]}}
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new([])
      assert_receive {:materializer_changes, _, %{move_out: [{20, "20"}], move_in: []}}
    end

    @tag snapshot_data: {
           [
             %Changes.NewRecord{
               record: %{"id" => "1", "value" => "10"},
               move_tags: ["tag_a"]
             }
           ],
           [pk_cols: ["id"]]
         }
    test "update that changes PK but keeps same tag cleans up stale tag entry", ctx do
      ctx = with_materializer(ctx)

      assert Materializer.get_link_values(ctx) == MapSet.new([10])

      # Update where PK changes but tag stays the same
      Materializer.new_changes(
        ctx,
        [
          %Changes.UpdatedRecord{
            record: %{"id" => "2", "value" => "20"},
            old_record: %{"id" => "1", "value" => "10"},
            move_tags: ["tag_a"],
            removed_move_tags: []
          }
        ]
        |> prep_changes()
      )

      assert Materializer.get_link_values(ctx) == MapSet.new([20])
      assert_receive {:materializer_changes, _, %{move_out: [{10, "10"}], move_in: [{20, "20"}]}}

      # move_out for tag_a should remove the row using the new key, not crash
      # looking for the old key
      Materializer.new_changes(ctx, [
        %{headers: %{event: "move-out", patterns: [%{pos: 0, value: "tag_a"}]}}
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new([])
      assert_receive {:materializer_changes, _, %{move_out: [{20, "20"}], move_in: []}}
    end

    test "events are accumulated across uncommitted fragments", ctx do
      ctx = with_materializer(ctx)

      Materializer.new_changes(
        ctx,
        [
          %Changes.NewRecord{key: "1", record: %{"value" => "1"}},
          %Changes.NewRecord{key: "2", record: %{"value" => "2"}},
          %Changes.NewRecord{key: "3", record: %{"value" => "3"}}
        ],
        commit: false
      )

      refute_received {:materializer_changes, _, _}

      Materializer.new_changes(ctx, [
        %Changes.NewRecord{key: "4", record: %{"value" => "4"}},
        %Changes.NewRecord{key: "5", record: %{"value" => "5"}}
      ])

      assert_receive {:materializer_changes, _, %{move_in: move_ins, move_out: []}}
      assert [{1, "1"}, {2, "2"}, {3, "3"}, {4, "4"}, {5, "5"}] == Enum.sort(move_ins)
    end

    test "moves are correctly tracked across multiple calls", ctx do
      ctx = with_materializer(ctx)

      Materializer.new_changes(ctx, [
        %Changes.NewRecord{key: "1", record: %{"value" => "1"}},
        %Changes.NewRecord{key: "2", record: %{"value" => "2"}},
        %Changes.NewRecord{key: "3", record: %{"value" => "1"}}
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new([1, 2])

      assert_receive {:materializer_changes, _, %{move_in: move_in}}
      assert Enum.sort(move_in) == [{1, "1"}, {2, "2"}]

      Materializer.new_changes(ctx, [
        %Changes.UpdatedRecord{
          key: "2",
          record: %{"value" => "3"},
          old_record: %{"value" => "2"}
        },
        %Changes.DeletedRecord{key: "3", old_record: %{"value" => "1"}},
        %Changes.UpdatedRecord{key: "1", record: %{"other" => "1"}, old_record: %{"other" => "0"}}
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new([1, 3])

      assert_receive {:materializer_changes, _, %{move_out: [{2, "2"}], move_in: [{3, "3"}]}}
    end
  end

  describe "same-batch move event cancellation" do
    test "insert and delete in same batch emits no events", ctx do
      ctx = with_materializer(ctx)

      apply_changes(ctx, [
        insert("1", "10"),
        delete("1", "10")
      ])

      refute_received {:materializer_changes, _, _}
    end

    @tag snapshot_data: [%Changes.NewRecord{record: %{"id" => "1", "value" => "10"}}]
    test "existing value removed and re-added emits no events", ctx do
      ctx = with_materializer(ctx)

      apply_changes(ctx, [
        update("1", "10", "20"),
        update("1", "20", "10")
      ])

      refute_received {:materializer_changes, _, _}
    end

    test "two move_ins and one move_out emits net one move_in", ctx do
      ctx = with_materializer(ctx)

      apply_changes(ctx, [
        insert("1", "10"),
        delete("1", "10"),
        insert("2", "10")
      ])

      assert_moved_in(["10"])
    end

    test "cancellation does not affect unrelated values", ctx do
      ctx = with_materializer(ctx)

      apply_changes(ctx, [
        insert("1", "10"),
        delete("1", "10"),
        insert("2", "20")
      ])

      assert_moved_in(["20"])
    end

    @tag snapshot_data: [%Changes.NewRecord{record: %{"id" => "1", "value" => "10"}}]
    test "net move_out survives when more outs than ins", ctx do
      ctx = with_materializer(ctx)

      apply_changes(ctx, [
        delete("1", "10"),
        insert("2", "10"),
        delete("2", "10")
      ])

      assert_moved_out(["10"])
    end

    test "net move_in survives when more ins than outs", ctx do
      ctx = with_materializer(ctx)

      apply_changes(ctx, [
        insert("1", "10"),
        update("1", "10", "20"),
        update("1", "20", "10")
      ])

      assert_moved_in(["10"])
    end

    defp insert(id, value),
      do: %Changes.NewRecord{record: %{"id" => id, "value" => value}}

    defp update(id, old_value, new_value),
      do: %Changes.UpdatedRecord{
        record: %{"id" => id, "value" => new_value},
        old_record: %{"id" => id, "value" => old_value}
      }

    defp delete(id, value),
      do: %Changes.DeletedRecord{old_record: %{"id" => id, "value" => value}}

    defp apply_changes(ctx, changes),
      do: Materializer.new_changes(ctx, prep_changes(changes))

    defp assert_moved_in(values) do
      assert_receive {:materializer_changes, _, events}
      assert Enum.sort(events.move_in) == Enum.map(values, &{String.to_integer(&1), &1})
    end

    defp assert_moved_out(values) do
      assert_receive {:materializer_changes, _, events}
      assert Enum.sort(events.move_out) == Enum.map(values, &{String.to_integer(&1), &1})
    end
  end

  describe "tag-only updates (value unchanged)" do
    @tag snapshot_data: [%Changes.NewRecord{record: %{"id" => "1", "value" => "10"}}]
    test "update with tag change but unchanged value updates tags without events", ctx do
      ctx = with_materializer(ctx)

      assert Materializer.get_link_values(ctx) == MapSet.new([10])

      # Update where tags change but the tracked value stays the same
      Materializer.new_changes(
        ctx,
        [
          %Changes.UpdatedRecord{
            key: ~s("public"."test_table"/"1"),
            record: %{"id" => "1", "value" => "10"},
            old_record: %{"id" => "1", "value" => "10"},
            move_tags: ["new_tag"],
            removed_move_tags: ["old_tag"]
          }
        ]
      )

      # Value should still be present
      assert Materializer.get_link_values(ctx) == MapSet.new([10])

      # No move events should be emitted since the value didn't change
      refute_received {:materializer_changes, _, _}
    end

    @tag snapshot_data: {
           [%Changes.NewRecord{record: %{"id" => "1", "value" => "10"}, move_tags: ["old_tag"]}],
           []
         }
    test "tag is updated so subsequent move_out for old tag finds nothing", ctx do
      ctx = with_materializer(ctx)

      assert Materializer.get_link_values(ctx) == MapSet.new([10])

      # Update that changes the tag from old_tag to new_tag but keeps value the same
      Materializer.new_changes(
        ctx,
        [
          %Changes.UpdatedRecord{
            key: ~s("public"."test_table"/"1"),
            record: %{"id" => "1", "value" => "10"},
            old_record: %{"id" => "1", "value" => "10"},
            move_tags: ["new_tag"],
            removed_move_tags: ["old_tag"]
          }
        ]
      )

      # No events from the tag-only update
      refute_received {:materializer_changes, _, _}

      # Now send a move_out for the OLD tag - should find nothing since the row moved to new_tag
      Materializer.new_changes(ctx, [
        %{headers: %{event: "move-out", patterns: [%{pos: 0, value: "old_tag"}]}}
      ])

      # Value should still be present (row wasn't removed)
      assert Materializer.get_link_values(ctx) == MapSet.new([10])

      # No move events since the row was already moved to new_tag
      refute_received {:materializer_changes, _, _}
    end

    @tag snapshot_data: {
           [%Changes.NewRecord{record: %{"id" => "1", "value" => "10"}, move_tags: ["old_tag"]}],
           []
         }
    test "move_out for new tag after tag update removes the row", ctx do
      ctx = with_materializer(ctx)

      assert Materializer.get_link_values(ctx) == MapSet.new([10])

      # Update that changes the tag from old_tag to new_tag
      Materializer.new_changes(
        ctx,
        [
          %Changes.UpdatedRecord{
            key: ~s("public"."test_table"/"1"),
            record: %{"id" => "1", "value" => "10"},
            old_record: %{"id" => "1", "value" => "10"},
            move_tags: ["new_tag"],
            removed_move_tags: ["old_tag"]
          }
        ]
      )

      refute_received {:materializer_changes, _, _}

      # Now send a move_out for the NEW tag - should find and remove the row
      Materializer.new_changes(ctx, [
        %{headers: %{event: "move-out", patterns: [%{pos: 0, value: "new_tag"}]}}
      ])

      # Value should be gone
      assert Materializer.get_link_values(ctx) == MapSet.new([])

      # Should emit move_out event
      assert_receive {:materializer_changes, _, %{move_out: [{10, "10"}]}}
    end

    @tag snapshot_data: {
           [
             %Changes.NewRecord{record: %{"id" => "1", "value" => "10"}, move_tags: ["tag_a"]},
             %Changes.NewRecord{record: %{"id" => "2", "value" => "20"}, move_tags: ["tag_a"]}
           ],
           []
         }
    test "multiple rows with same tag, one updates tag, move_out only affects remaining", ctx do
      ctx = with_materializer(ctx)

      assert Materializer.get_link_values(ctx) == MapSet.new([10, 20])

      # Row 1 moves from tag_a to tag_b, row 2 stays in tag_a
      Materializer.new_changes(
        ctx,
        [
          %Changes.UpdatedRecord{
            key: ~s("public"."test_table"/"1"),
            record: %{"id" => "1", "value" => "10"},
            old_record: %{"id" => "1", "value" => "10"},
            move_tags: ["tag_b"],
            removed_move_tags: ["tag_a"]
          }
        ]
      )

      refute_received {:materializer_changes, _, _}

      # move_out for tag_a should only affect row 2 (row 1 moved to tag_b)
      Materializer.new_changes(ctx, [
        %{headers: %{event: "move-out", patterns: [%{pos: 0, value: "tag_a"}]}}
      ])

      # Only value 10 should remain (row 1 is now under tag_b)
      assert Materializer.get_link_values(ctx) == MapSet.new([10])

      # Should emit move_out only for row 2's value
      assert_receive {:materializer_changes, _, %{move_out: [{20, "20"}]}}
    end
  end

  describe "move_out events" do
    test "runtime move_out event removes rows matching the pattern", ctx do
      ctx = with_materializer(ctx)

      # Insert records with move_tags
      Materializer.new_changes(ctx, [
        %Changes.NewRecord{key: "1", record: %{"value" => "10"}, move_tags: ["tag1"]},
        %Changes.NewRecord{key: "2", record: %{"value" => "20"}, move_tags: ["tag2"]},
        %Changes.NewRecord{key: "3", record: %{"value" => "30"}, move_tags: ["tag1"]}
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new([10, 20, 30])
      assert_receive {:materializer_changes, _, %{move_in: _}}

      # Send move_out event to remove rows with tag1
      Materializer.new_changes(ctx, [
        %{headers: %{event: "move-out", patterns: [%{pos: 0, value: "tag1"}]}}
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new([20])
      assert_receive {:materializer_changes, _, %{move_out: move_out}}
      assert Enum.sort(move_out) == [{10, "10"}, {30, "30"}]
    end

    test "runtime move_out event with multiple patterns removes all matching rows", ctx do
      ctx = with_materializer(ctx)

      Materializer.new_changes(ctx, [
        %Changes.NewRecord{key: "1", record: %{"value" => "10"}, move_tags: ["tag1"]},
        %Changes.NewRecord{key: "2", record: %{"value" => "20"}, move_tags: ["tag2"]},
        %Changes.NewRecord{key: "3", record: %{"value" => "30"}, move_tags: ["tag3"]}
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new([10, 20, 30])
      assert_receive {:materializer_changes, _, %{move_in: _}}

      # Remove rows with tag1 or tag3
      Materializer.new_changes(ctx, [
        %{
          headers: %{
            event: "move-out",
            patterns: [%{pos: 0, value: "tag1"}, %{pos: 0, value: "tag3"}]
          }
        }
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new([20])
      assert_receive {:materializer_changes, _, %{move_out: move_out}}
      assert Enum.sort(move_out) == [{10, "10"}, {30, "30"}]
    end

    test "runtime move_out event for non-existent pattern causes no events", ctx do
      ctx = with_materializer(ctx)

      Materializer.new_changes(ctx, [
        %Changes.NewRecord{key: "1", record: %{"value" => "10"}, move_tags: ["tag1"]}
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new([10])
      assert_receive {:materializer_changes, _, %{move_in: [{10, "10"}]}}

      # Try to remove rows with non-existent tag
      Materializer.new_changes(ctx, [
        %{headers: %{event: "move-out", patterns: [%{pos: 0, value: "non_existent"}]}}
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new([10])
      refute_received {:materializer_changes, _, _}
    end

    test "runtime move_out event removes row but value remains if another row has same value",
         ctx do
      ctx = with_materializer(ctx)

      Materializer.new_changes(ctx, [
        %Changes.NewRecord{key: "1", record: %{"value" => "10"}, move_tags: ["tag1"]},
        %Changes.NewRecord{key: "2", record: %{"value" => "10"}, move_tags: ["tag2"]}
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new([10])
      assert_receive {:materializer_changes, _, %{move_in: [{10, "10"}]}}

      # Remove only tag1 row
      Materializer.new_changes(ctx, [
        %{headers: %{event: "move-out", patterns: [%{pos: 0, value: "tag1"}]}}
      ])

      # Value 10 should still be present because key "2" still has it
      assert Materializer.get_link_values(ctx) == MapSet.new([10])
      refute_received {:materializer_changes, _, _}
    end

    @tag snapshot_data: {
           [
             %Changes.NewRecord{record: %{"id" => "1", "value" => "10"}, move_tags: ["tag1"]},
             %Changes.NewRecord{record: %{"id" => "2", "value" => "20"}, move_tags: ["tag2"]}
           ],
           []
         }
    test "on-load tags are tracked and can be removed by runtime move_out", ctx do
      ctx = with_materializer(ctx)

      # Both values should be present after on-load
      assert Materializer.get_link_values(ctx) == MapSet.new([10, 20])

      # Now send move_out event to remove rows with tag1
      Materializer.new_changes(ctx, [
        %{headers: %{event: "move-out", patterns: [%{pos: 0, value: "tag1"}]}}
      ])

      # Only value 20 should remain after move_out
      assert Materializer.get_link_values(ctx) == MapSet.new([20])
      assert_receive {:materializer_changes, _, %{move_out: [{10, "10"}]}}
    end

    @tag snapshot_data: {
           [
             %Changes.NewRecord{record: %{"id" => "1", "value" => "10"}, move_tags: ["tag1"]},
             %Changes.NewRecord{record: %{"id" => "2", "value" => "10"}, move_tags: ["tag2"]}
           ],
           []
         }
    test "on-load tags tracked correctly when values are duplicated", ctx do
      ctx = with_materializer(ctx)

      # Value 10 should be present (from both rows)
      assert Materializer.get_link_values(ctx) == MapSet.new([10])

      # Remove rows with tag1
      Materializer.new_changes(ctx, [
        %{headers: %{event: "move-out", patterns: [%{pos: 0, value: "tag1"}]}}
      ])

      # Value 10 should still be present because key "2" still has it
      assert Materializer.get_link_values(ctx) == MapSet.new([10])
      refute_received {:materializer_changes, _, _}
    end

    @tag snapshot_data: {
           [
             %Changes.NewRecord{record: %{"id" => "1", "value" => "10"}, move_tags: ["tag1"]},
             %Changes.NewRecord{record: %{"id" => "2", "value" => "20"}, move_tags: ["tag1"]},
             %Changes.NewRecord{record: %{"id" => "3", "value" => "30"}, move_tags: ["tag2"]}
           ],
           []
         }
    test "on-load tags with multiple rows sharing same tag can all be removed", ctx do
      ctx = with_materializer(ctx)

      assert Materializer.get_link_values(ctx) == MapSet.new([10, 20, 30])

      # Remove all rows with tag1
      Materializer.new_changes(ctx, [
        %{headers: %{event: "move-out", patterns: [%{pos: 0, value: "tag1"}]}}
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new([30])
      assert_receive {:materializer_changes, _, %{move_out: move_out}}
      assert Enum.sort(move_out) == [{10, "10"}, {20, "20"}]
    end

    @tag snapshot_data: [
           ~s({"key":"\\"public\\".\\"test_table\\"/\\"1\\"","value":{"id":"1","value":"10"},"headers":{"operation":"insert","tags":["tag1"]}}),
           ~s({"key":"\\"public\\".\\"test_table\\"/\\"2\\"","value":{"id":"2","value":"20"},"headers":{"operation":"insert","tags":["tag2"]}}),
           ~s({"headers":{"event":"move-out","patterns":[{"pos":0,"value":"tag1"}]}})
         ]
    test "on-load move_out event in snapshot data is processed correctly", ctx do
      ctx = with_materializer(ctx)

      # Only value 20 should remain after on-load processing of move_out
      assert Materializer.get_link_values(ctx) == MapSet.new([20])
      refute_received {:materializer_changes, _, _}
    end

    @tag snapshot_data: [
           ~s({"key":"\\"public\\".\\"test_table\\"/\\"1\\"","value":{"id":"1","value":"10"},"headers":{"operation":"insert","tags":["tag1"]}}),
           ~s({"key":"\\"public\\".\\"test_table\\"/\\"2\\"","value":{"id":"2","value":"10"},"headers":{"operation":"insert","tags":["tag2"]}}),
           ~s({"headers":{"event":"move-out","patterns":[{"pos":0,"value":"tag1"}]}})
         ]
    test "on-load move_out event with duplicate values keeps remaining row's value", ctx do
      ctx = with_materializer(ctx)

      # Value 10 should still be present because key "2" still has it
      assert Materializer.get_link_values(ctx) == MapSet.new([10])
      refute_received {:materializer_changes, _, _}
    end

    test "runtime move-in tags are tracked correctly if read from a storage range",
         %{
           shape_storage: shape_storage,
           writer: writer
         } = ctx do
      ctx = with_materializer(ctx)

      Storage.write_move_in_snapshot!(
        [
          [
            ~s("public"."test_table"/"1"),
            ["tag1"],
            ~s({"key":"\\"public\\".\\"test_table\\"/\\"1\\"","value":{"id":"1","value":"10"},"headers":{"operation":"insert","tags":["tag1"]}})
          ]
        ],
        "test",
        shape_storage
      )

      {range, writer} =
        Storage.append_move_in_snapshot_to_log!(
          "test",
          writer
        )

      Materializer.new_changes(ctx, range)

      assert Materializer.get_link_values(ctx) == MapSet.new([10])

      {range, _writer} =
        Storage.append_control_message!(
          Jason.encode!(%{headers: %{event: "move-out", patterns: [%{pos: 0, value: "tag1"}]}}),
          writer
        )

      Materializer.new_changes(ctx, range)

      assert Materializer.get_link_values(ctx) == MapSet.new()
    end
  end

  describe "DNF: multiple tags per row with active_conditions" do
    test "insert with active_conditions where row is not initially included", ctx do
      ctx = with_materializer(ctx)

      # Row has two disjunct tags but active_conditions says position 0 is false
      # Tag "hash_a/" participates in position 0, tag "/hash_b" participates in position 1
      Materializer.new_changes(ctx, [
        %Changes.NewRecord{
          key: "1",
          record: %{"value" => "10"},
          move_tags: ["hash_a/", "/hash_b"],
          active_conditions: [false, false]
        }
      ])

      # Row is not included because no disjunct has all positions active
      assert Materializer.get_link_values(ctx) == MapSet.new()
      refute_received {:materializer_changes, _, _}
    end

    test "insert with active_conditions where one disjunct is satisfied", ctx do
      ctx = with_materializer(ctx)

      Materializer.new_changes(ctx, [
        %Changes.NewRecord{
          key: "1",
          record: %{"value" => "10"},
          move_tags: ["hash_a/", "/hash_b"],
          active_conditions: [true, false]
        }
      ])

      # First disjunct "hash_a/" has position 0 active → included
      assert Materializer.get_link_values(ctx) == MapSet.new([10])
      assert_receive {:materializer_changes, _, %{move_in: [{10, "10"}]}}
    end

    test "move-in broadcast activates a previously excluded row", ctx do
      ctx = with_materializer(ctx)

      # Insert with position 0 inactive
      Materializer.new_changes(ctx, [
        %Changes.NewRecord{
          key: "1",
          record: %{"value" => "10"},
          move_tags: ["hash_a/", "/hash_b"],
          active_conditions: [false, false]
        }
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new()
      refute_received {:materializer_changes, _, _}

      # Move-in at position 0 with value "hash_a"
      Materializer.new_changes(ctx, [
        %{headers: %{event: "move-in", patterns: [%{pos: 0, value: "hash_a"}]}}
      ])

      # Now position 0 is true, first disjunct "hash_a/" is satisfied
      assert Materializer.get_link_values(ctx) == MapSet.new([10])
      assert_receive {:materializer_changes, _, %{move_in: [{10, "10"}]}}
    end

    test "move-out does not remove row when another disjunct still holds", ctx do
      ctx = with_materializer(ctx)

      # Insert with both positions active
      Materializer.new_changes(ctx, [
        %Changes.NewRecord{
          key: "1",
          record: %{"value" => "10"},
          move_tags: ["hash_a/", "/hash_b"],
          active_conditions: [true, true]
        }
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new([10])
      assert_receive {:materializer_changes, _, %{move_in: [{10, "10"}]}}

      # Move-out at position 0 - but position 1 still holds via second disjunct
      Materializer.new_changes(ctx, [
        %{headers: %{event: "move-out", patterns: [%{pos: 0, value: "hash_a"}]}}
      ])

      # Row should still be included because disjunct "/hash_b" at position 1 is still true
      assert Materializer.get_link_values(ctx) == MapSet.new([10])
      refute_received {:materializer_changes, _, _}
    end

    test "move-out removes row when last active disjunct becomes false", ctx do
      ctx = with_materializer(ctx)

      # Insert with only position 1 active
      Materializer.new_changes(ctx, [
        %Changes.NewRecord{
          key: "1",
          record: %{"value" => "10"},
          move_tags: ["hash_a/", "/hash_b"],
          active_conditions: [false, true]
        }
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new([10])
      assert_receive {:materializer_changes, _, %{move_in: [{10, "10"}]}}

      # Move-out at position 1 - now no disjunct holds
      Materializer.new_changes(ctx, [
        %{headers: %{event: "move-out", patterns: [%{pos: 1, value: "hash_b"}]}}
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new()
      assert_receive {:materializer_changes, _, %{move_out: [{10, "10"}]}}
    end

    test "move-in on already-present row is a no-op for value counts", ctx do
      ctx = with_materializer(ctx)

      # Insert with position 0 active
      Materializer.new_changes(ctx, [
        %Changes.NewRecord{
          key: "1",
          record: %{"value" => "10"},
          move_tags: ["hash_a/", "/hash_b"],
          active_conditions: [true, false]
        }
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new([10])
      assert_receive {:materializer_changes, _, %{move_in: [{10, "10"}]}}

      # Move-in at position 1 - row was already included via position 0
      Materializer.new_changes(ctx, [
        %{headers: %{event: "move-in", patterns: [%{pos: 1, value: "hash_b"}]}}
      ])

      # No value count change
      assert Materializer.get_link_values(ctx) == MapSet.new([10])
      refute_received {:materializer_changes, _, _}
    end

    test "multi-position disjunct requires all positions active", ctx do
      ctx = with_materializer(ctx)

      # Tag "hash_a/1" means positions 0 AND 1 must be active for this disjunct
      Materializer.new_changes(ctx, [
        %Changes.NewRecord{
          key: "1",
          record: %{"value" => "10"},
          move_tags: ["hash_a/1"],
          active_conditions: [true, false]
        }
      ])

      # Position 1 is false, so the disjunct is not satisfied
      assert Materializer.get_link_values(ctx) == MapSet.new()
      refute_received {:materializer_changes, _, _}
    end

    test "multi-position disjunct becomes satisfied when all positions active", ctx do
      ctx = with_materializer(ctx)

      # Tag "hash_a/1" needs both positions active
      Materializer.new_changes(ctx, [
        %Changes.NewRecord{
          key: "1",
          record: %{"value" => "10"},
          move_tags: ["hash_a/1"],
          active_conditions: [false, true]
        }
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new()
      refute_received {:materializer_changes, _, _}

      # Move-in at position 0 makes both positions active
      Materializer.new_changes(ctx, [
        %{headers: %{event: "move-in", patterns: [%{pos: 0, value: "hash_a"}]}}
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new([10])
      assert_receive {:materializer_changes, _, %{move_in: [{10, "10"}]}}
    end

    test "composite-key tag indexing works for position lookup", ctx do
      ctx = with_materializer(ctx)

      # Two rows with different position-0 hashes
      Materializer.new_changes(ctx, [
        %Changes.NewRecord{
          key: "1",
          record: %{"value" => "10"},
          move_tags: ["hash_x/"],
          active_conditions: [true, false]
        },
        %Changes.NewRecord{
          key: "2",
          record: %{"value" => "20"},
          move_tags: ["hash_y/"],
          active_conditions: [true, false]
        }
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new([10, 20])
      assert_receive {:materializer_changes, _, %{move_in: _}}

      # Move-out only for hash_x at position 0
      Materializer.new_changes(ctx, [
        %{headers: %{event: "move-out", patterns: [%{pos: 0, value: "hash_x"}]}}
      ])

      assert Materializer.get_link_values(ctx) == MapSet.new([20])
      assert_receive {:materializer_changes, _, %{move_out: [{10, "10"}]}}
    end
  end

  defp subscribe_for_replay(materializer, cursor) do
    assert {:pending, _current_offset} = Materializer.subscribe(materializer, cursor)

    assert_receive {:materializer_replay_ready, _shape_handle, result}, 1_000

    if match?({:ok, _seed, _target}, result) do
      assert :pending = Materializer.next_replay(materializer, self())
      assert_receive {:materializer_replay_ready, _shape_handle}, 1_000
    end

    result
  end

  defp respond_to_call(request, response) do
    receive do
      {:"$gen_call", {from, ref}, {^request, _arg}} ->
        send(from, {ref, response})

      {:"$gen_call", {from, ref}, ^request} ->
        send(from, {ref, response})
    end
  end

  defp with_materializer(ctx, opts \\ []) do
    {:ok, _pid} =
      Materializer.start_link(%{
        stack_id: ctx.stack_id,
        shape_handle: ctx.shape_handle,
        storage: ctx.storage,
        columns: Keyword.get(opts, :columns, ["value"]),
        materialized_type: Keyword.get(opts, :materialized_type, {:array, :int8})
      })

    respond_to_call(:await_snapshot_start, :started)

    respond_to_call(
      :subscribe_materializer,
      {:ok, LogOffset.last_before_real_offsets()}
    )

    assert Materializer.wait_until_ready(ctx) == :ok
    Materializer.subscribe(ctx)

    ctx
  end

  defp make_snapshot_data(changes, opts \\ []) do
    pk_cols = Keyword.get(opts, :pk_cols, ["id"])

    changes
    |> prep_changes(opts)
    |> Enum.flat_map(&LogItems.from_change(&1, 1, pk_cols, :default))
    |> Enum.map(fn {_offset, item} -> Jason.encode!(item) end)
  end

  # Build a single main-log insert log item at `offset` introducing `value`,
  # encoded the same way the source consumer would write it (headers carry the
  # `lsn`/`op_position` used to reconstruct the offset during replay).
  defp main_log_insert(offset, id, value, last? \\ true) do
    change =
      %Changes.NewRecord{
        relation: {"public", "test_table"},
        key: ~s|"public"."test_table"/"#{id}"|,
        record: %{"id" => id, "value" => value},
        log_offset: offset,
        move_tags: [],
        last?: last?
      }
      |> Changes.fill_key(["id"])

    change
    |> then(&LogItems.from_change(&1, 1, ["id"], :default))
    |> Enum.map(fn {item_offset, item} ->
      {item_offset, change.key, :insert, Jason.encode!(item)}
    end)
  end

  defp main_log_control(offset) do
    json = Jason.encode!(%{headers: %{control: "up_to_date", last: true}})
    [{offset, 0, "", ?c, 0, byte_size(json), json}]
  end

  defp main_log_delete(offset, id, value) do
    change =
      %Changes.DeletedRecord{
        relation: {"public", "test_table"},
        key: ~s|"public"."test_table"/"#{id}"|,
        old_record: %{"id" => id, "value" => value},
        log_offset: offset,
        move_tags: [],
        last?: true
      }
      |> Changes.fill_key(["id"])

    change
    |> then(&LogItems.from_change(&1, 1, ["id"], :default))
    |> Enum.map(fn {item_offset, item} ->
      {item_offset, change.key, :delete, Jason.encode!(item)}
    end)
  end

  defp prep_changes(changes, opts \\ []) do
    pk_cols = Keyword.get(opts, :pk_cols, ["id"])
    relation = Keyword.get(opts, :relation, {"public", "test_table"})

    changes
    |> Enum.map(&Map.put(&1, :relation, relation))
    |> Enum.map(&Map.put(&1, :log_offset, LogOffset.first()))
    |> Enum.map(&Changes.fill_key(&1, pk_cols))
  end

  describe "startup offset coordination" do
    test "no duplicate when offset coordination prevents overlap", ctx do
      shape_handle = "offset-test-#{System.unique_integer()}"

      # Setup storage with a record at offset first()
      storage = Storage.for_shape(shape_handle, ctx.storage)
      Storage.start_link(storage)
      writer = Storage.init_writer!(storage, @shape)
      Storage.mark_snapshot_as_started(storage)

      first_offset = LogOffset.first()

      writer =
        Storage.append_to_log!(
          [
            {first_offset, ~s|"public"."test_table"/"1"|, :insert,
             ~s|{"key":"\\"public\\".\\"test_table\\"/\\"1\\"","value":{"id":"1","value":"10"},"headers":{"operation":"insert"}}|}
          ],
          writer
        )

      Storage.hibernate(writer)

      ConsumerRegistry.register_consumer(self(), shape_handle, ctx.stack_id)

      {:ok, _pid} =
        Materializer.start_link(%{
          stack_id: ctx.stack_id,
          shape_handle: shape_handle,
          storage: ctx.storage,
          columns: ["value"],
          materialized_type: {:array, :int8}
        })

      respond_to_call(:await_snapshot_start, :started)

      # Return offset BEFORE the record so the Materializer reads nothing from storage
      respond_to_call(:subscribe_materializer, {:ok, LogOffset.before_all()})

      mat_ctx = %{stack_id: ctx.stack_id, shape_handle: shape_handle}

      assert Materializer.wait_until_ready(mat_ctx) == :ok

      # Send the same record via new_changes — should NOT crash because
      # offset coordination ensured the Materializer didn't read it from storage
      Materializer.new_changes(mat_ctx, [
        %Changes.NewRecord{
          relation: {"public", "test_table"},
          key: ~s|"public"."test_table"/"1"|,
          record: %{"id" => "1", "value" => "10"},
          move_tags: []
        }
      ])

      assert Materializer.get_link_values(mat_ctx) == MapSet.new([10])
    end
  end

  describe "startup history replay" do
    # Storage.get_log_stream/3 returns at most one chunk per call (one
    # snapshot chunk, or one main-log chunk). On startup the Materializer
    # must iterate chunks until it reaches `subscribed_offset` so it
    # correctly replays the source shape's full persisted history. If it
    # reads only the first chunk, post-snapshot updates persisted to the
    # main log are silently dropped, leaving `value_counts` reflecting only
    # the snapshot.
    test "replays main-log entries persisted before subscription", ctx do
      shape_handle = "history-test-#{System.unique_integer()}"

      storage = Storage.for_shape(shape_handle, ctx.storage)
      Storage.start_link(storage)
      writer = Storage.init_writer!(storage, @shape)
      Storage.mark_snapshot_as_started(storage)

      # Snapshot with one row at value=10
      Storage.make_new_snapshot!(
        make_snapshot_data([%Changes.NewRecord{record: %{"id" => "1", "value" => "10"}}]),
        storage
      )

      # Main-log entry that updates the row to value=99 — written to disk
      # before the Materializer subscribes.
      log_offset = LogOffset.new(100, 0)

      writer =
        Storage.append_to_log!(
          [
            {log_offset, ~s|"public"."test_table"/"1"|, :update,
             ~s|{"key":"\\"public\\".\\"test_table\\"/\\"1\\"","value":{"id":"1","value":"99"},"headers":{"operation":"update"}}|}
          ],
          writer
        )

      Storage.hibernate(writer)

      ConsumerRegistry.register_consumer(self(), shape_handle, ctx.stack_id)

      {:ok, _pid} =
        Materializer.start_link(%{
          stack_id: ctx.stack_id,
          shape_handle: shape_handle,
          storage: ctx.storage,
          columns: ["value"],
          materialized_type: {:array, :int8}
        })

      respond_to_call(:await_snapshot_start, :started)
      # Subscribe at an offset past the persisted UPDATE — the Materializer
      # must walk the snapshot AND the main log to reach this point.
      respond_to_call(:subscribe_materializer, {:ok, log_offset})

      mat_ctx = %{stack_id: ctx.stack_id, shape_handle: shape_handle}
      assert Materializer.wait_until_ready(mat_ctx) == :ok

      # If only snapshot chunk 0 was read, value_counts would contain 10
      # (the snapshot value) and the persisted UPDATE would be lost. The
      # iteration fix guarantees the UPDATE is replayed.
      assert Materializer.get_link_values(mat_ctx) == MapSet.new([99])
    end

    # When the source shape's persisted main log spans MORE than one chunk,
    # `Storage.get_log_stream/3` returns the *entire* main-log range in a
    # single call (chunking only applies to the snapshot). Startup replay must
    # therefore stop iterating as soon as it steps into the main log:
    # continuing to advance through chunk boundaries would re-read main-log
    # entries it has already applied, and re-applying a `NewRecord` for a key
    # that already exists raises "Key already exists", crashing the
    # materializer and the dependent shape's consumer. This test guards that
    # each persisted entry is applied exactly once.
    @tag with_pure_file_storage_opts: [chunk_bytes_threshold: 10]
    test "does not re-read main-log entries when the main log spans multiple chunks", ctx do
      shape_handle = "multichunk-test-#{System.unique_integer()}"

      storage = Storage.for_shape(shape_handle, ctx.storage)
      Storage.start_link(storage)
      writer = Storage.init_writer!(storage, @shape)
      Storage.mark_snapshot_as_started(storage)

      # Snapshot with one row at value=10.
      Storage.make_new_snapshot!(
        make_snapshot_data([%Changes.NewRecord{record: %{"id" => "1", "value" => "10"}}]),
        storage
      )

      # Two main-log INSERTs persisted before the materializer subscribes.
      # With a tiny `chunk_bytes_threshold` each lands in its own main-log
      # chunk, so the main log spans more than one chunk — the condition under
      # which a second read of the same range would re-apply the insert for
      # key "3".
      offset_2 = LogOffset.new(100, 0)
      offset_3 = LogOffset.new(200, 0)

      writer =
        Storage.append_to_log!(
          [
            {offset_2, ~s|"public"."test_table"/"2"|, :insert,
             ~s|{"key":"\\"public\\".\\"test_table\\"/\\"2\\"","value":{"id":"2","value":"20"},"headers":{"operation":"insert"}}|}
          ],
          writer
        )

      writer =
        Storage.append_to_log!(
          [
            {offset_3, ~s|"public"."test_table"/"3"|, :insert,
             ~s|{"key":"\\"public\\".\\"test_table\\"/\\"3\\"","value":{"id":"3","value":"30"},"headers":{"operation":"insert"}}|}
          ],
          writer
        )

      Storage.hibernate(writer)

      # Sanity check: the main log really does span more than one chunk. The
      # first main-log chunk (the one reached from the end of the snapshot)
      # must end strictly before the last persisted offset. Without this, a
      # single read would cover the whole main log and the multi-chunk case
      # under test wouldn't be exercised.
      first_main_chunk_end =
        Storage.get_chunk_end_log_offset(LogOffset.last_before_real_offsets(), storage)

      assert not is_nil(first_main_chunk_end)
      assert LogOffset.compare(first_main_chunk_end, offset_3) == :lt

      ConsumerRegistry.register_consumer(self(), shape_handle, ctx.stack_id)

      {:ok, _pid} =
        Materializer.start_link(%{
          stack_id: ctx.stack_id,
          shape_handle: shape_handle,
          storage: ctx.storage,
          columns: ["value"],
          materialized_type: {:array, :int8}
        })

      respond_to_call(:await_snapshot_start, :started)
      # Subscribe past both persisted INSERTs so startup replay walks the
      # snapshot and the whole multi-chunk main log.
      respond_to_call(:subscribe_materializer, {:ok, offset_3})

      mat_ctx = %{stack_id: ctx.stack_id, shape_handle: shape_handle}
      assert Materializer.wait_until_ready(mat_ctx) == :ok

      # The snapshot value and both persisted INSERTs must each be applied
      # exactly once.
      assert Materializer.get_link_values(mat_ctx) == MapSet.new([10, 20, 30])
    end
  end

  describe "move replay on subscribe" do
    # A subscriber that is behind (`from_lsn` < the materializer's applied
    # position) is caught up by replaying only the moves it missed, each tagged
    # with its source LSN, and is handed the link values as of `from_lsn` to seed
    # its dependency view.
    setup ctx do
      shape_handle = "replay-test-#{System.unique_integer([:positive])}"

      storage = Storage.for_shape(shape_handle, ctx.storage)
      Storage.start_link(storage)
      writer = Storage.init_writer!(storage, @shape)
      Storage.mark_snapshot_as_started(storage)

      # Snapshot establishes value 10.
      Storage.make_new_snapshot!(
        make_snapshot_data([%Changes.NewRecord{record: %{"id" => "1", "value" => "10"}}]),
        storage
      )

      # Two main-log inserts at distinct offsets, each introducing a new value
      # (a move-in): value 20 at (100,0), value 30 at (200,0).
      writer =
        Storage.append_to_log!(
          main_log_insert(LogOffset.new(100, 0), "2", "20"),
          writer
        )

      writer =
        Storage.append_to_log!(
          main_log_insert(LogOffset.new(200, 0), "3", "30"),
          writer
        )

      writer = Storage.hibernate(writer)

      ConsumerRegistry.register_consumer(self(), shape_handle, ctx.stack_id)

      {:ok, _pid} =
        Materializer.start_link(%{
          stack_id: ctx.stack_id,
          shape_handle: shape_handle,
          storage: ctx.storage,
          columns: ["value"],
          materialized_type: {:array, :int8}
        })

      respond_to_call(:await_snapshot_start, :started)
      # Subscribed offset past both main-log entries so the materializer applies
      # the full history at startup.
      respond_to_call(:subscribe_materializer, {:ok, LogOffset.new(200, 0)})

      mat_ctx = %{stack_id: ctx.stack_id, shape_handle: shape_handle}
      assert Materializer.wait_until_ready(mat_ctx) == :ok
      assert Materializer.get_link_values(mat_ctx) == MapSet.new([10, 20, 30])

      ctx
      |> Map.put(:mat_ctx, mat_ctx)
      |> Map.put(:replay_writer, writer)
      |> Map.put(:replay_storage, storage)
    end

    test "keeps the Materializer responsive while a worker is blocked scanning a replay seed",
         %{mat_ctx: mat_ctx} do
      gate = block_replay_seed_scans()
      cursor = LogOffset.new(50, 0)
      target = LogOffset.new(200, 0)

      assert {:pending, ^target} = Materializer.subscribe(mat_ctx, cursor)
      assert_receive {:replay_seed_scan_blocked, ^gate, worker_pid}

      materializer_pid = Materializer.whereis(mat_ctx)
      assert :ok = GenServer.call(materializer_pid, :wait_until_ready, 100)

      state = :sys.get_state(materializer_pid)
      session = Map.fetch!(state.replay_sessions, self())
      assert session.worker_pid == worker_pid
      assert session.status == :seeding
      refute Map.has_key?(session, :replay_state)
      refute Map.has_key?(session, :value_counts)
      refute Map.has_key?(session, :index)

      send(worker_pid, {:release_replay_seed_scan, gate})

      assert_receive {:materializer_replay_ready, _shape_handle, {:ok, seed, ^target}}
      assert seed == MapSet.new([10])

      # Re-subscribing at the current cursor cancels and joins live delivery,
      # releasing the worker's stack-wide lease for the rest of the suite.
      assert {:ok, current_values, ^target} = Materializer.subscribe(mat_ctx, target)
      assert current_values == MapSet.new([10, 20, 30])
    end

    test "keeps a legitimately progressing replay alive past the coordinator idle deadline",
         %{mat_ctx: mat_ctx} do
      coordinator = GenServer.whereis(ReplayCoordinator.name(mat_ctx.stack_id))
      materializer_pid = Materializer.whereis(mat_ctx)

      :sys.replace_state(coordinator, &%{&1 | idle_timeout_ms: 70})
      :sys.replace_state(materializer_pid, &%{&1 | replay_progress_interval_ms: 10})
      slow_replay_scans(40)

      assert {:pending, target} = Materializer.subscribe(mat_ctx, LogOffset.new(50, 0))

      assert_receive {:materializer_replay_ready, _shape_handle, {:ok, seed, ^target}}, 1_000
      assert seed == MapSet.new([10])

      assert :pending = Materializer.next_replay(mat_ctx, self())
      assert_receive {:materializer_replay_ready, _shape_handle}, 1_000

      assert {:ok, %{lsn: %LogOffset{tx_offset: 100}}} =
               Materializer.next_replay(mat_ctx, self())
    end

    test "counts a worker that is actively seeding against the pending replay cap",
         %{mat_ctx: mat_ctx} do
      materializer_pid = Materializer.whereis(mat_ctx)
      :sys.replace_state(materializer_pid, &%{&1 | replay_max_pending: 1})
      gate = block_replay_seed_scans()
      parent = self()
      cursor = LogOffset.new(50, 0)
      target = LogOffset.new(200, 0)

      first = spawn_replay_subscriber(parent, :first, mat_ctx, cursor)

      assert_receive {:first, :subscribed, ^first, {:pending, ^target}}
      assert_receive {:replay_seed_scan_blocked, ^gate, worker_pid}

      second = spawn_replay_subscriber(parent, :second, mat_ctx, cursor)

      on_exit(fn ->
        for pid <- [first, second], Process.alive?(pid), do: Process.exit(pid, :kill)
      end)

      assert_receive {:second, :subscribed, ^second, {:error, :replay_queue_full}}

      state = :sys.get_state(materializer_pid)
      assert map_size(state.replay_sessions) == 1
      assert state.pending_replay_cursors == %{}

      send(worker_pid, {:release_replay_seed_scan, gate})

      assert_receive {:first, :message, ^first,
                      {:materializer_replay_ready, _shape_handle, {:ok, _, _}}}
    end

    test "a re-subscribe cancels the old worker and stale seed completion cannot install",
         %{mat_ctx: mat_ctx} do
      gate = block_replay_seed_scans()
      materializer_pid = Materializer.whereis(mat_ctx)
      target = LogOffset.new(200, 0)

      assert {:pending, ^target} =
               Materializer.subscribe(mat_ctx, LogOffset.new(50, 0))

      assert_receive {:replay_seed_scan_blocked, ^gate, worker_pid}
      session = :sys.get_state(materializer_pid).replay_sessions[self()]

      assert {:ok, current_values, ^target} = Materializer.subscribe(mat_ctx, target)
      assert current_values == MapSet.new([10, 20, 30])

      worker_ref = Process.monitor(worker_pid)
      assert_receive {:DOWN, ^worker_ref, :process, ^worker_pid, _reason}

      # A completion already queued by the cancelled worker is harmless because
      # installation is fenced by both its job reference and worker pid.
      send(
        materializer_pid,
        {:replay_worker_seed_built, session.job_ref, worker_pid}
      )

      state = :sys.get_state(materializer_pid)
      assert state.replay_sessions == %{}
      assert MapSet.member?(state.subscribers, self())
      refute_received {:materializer_replay_ready, _shape_handle, _result}
    end

    test "rejects a replay whose retained worker heap exceeds the configured bound",
         %{mat_ctx: mat_ctx} do
      materializer_pid = Materializer.whereis(mat_ctx)
      memory_limit = 1_024

      :sys.replace_state(
        materializer_pid,
        &%{&1 | replay_memory_limit_bytes: memory_limit}
      )

      assert {:error, {:replay_process_memory_limit_exceeded, attempted_bytes, ^memory_limit}} =
               subscribe_for_replay(mat_ctx, LogOffset.new(50, 0))

      assert attempted_bytes > memory_limit
      state = :sys.get_state(materializer_pid)
      assert state.replay_sessions == %{}
      refute Map.has_key?(state.subscriber_monitors, self())
      refute MapSet.member?(state.causal_subscribers, self())
    end

    test "phase-two seed failure invalidates and fully removes the live subscriber",
         %{mat_ctx: mat_ctx} do
      materializer_pid = Materializer.whereis(mat_ctx)
      assert {:pending, target} = Materializer.subscribe(mat_ctx, LogOffset.new(50, 0))
      assert_receive {:materializer_replay_ready, _shape_handle, {:ok, _seed, ^target}}

      :sys.replace_state(materializer_pid, &%{&1 | replay_memory_limit_bytes: 1_024})

      assert :pending = Materializer.next_replay(mat_ctx, self())
      assert_receive {:materializer_shape_invalidated, _shape_handle}

      state = :sys.get_state(materializer_pid)
      assert state.replay_sessions == %{}
      refute Map.has_key?(state.subscriber_monitors, self())
      refute MapSet.member?(state.subscribers, self())
      refute MapSet.member?(state.causal_subscribers, self())
    end

    test "phase-two timeout invalidates and fully removes the live subscriber",
         %{mat_ctx: mat_ctx} do
      materializer_pid = Materializer.whereis(mat_ctx)
      coordinator = ReplayCoordinator.name(mat_ctx.stack_id)
      assert {:pending, target} = Materializer.subscribe(mat_ctx, LogOffset.new(50, 0))
      assert_receive {:materializer_replay_ready, _shape_handle, {:ok, _seed, ^target}}

      :sys.replace_state(coordinator, &%{&1 | idle_timeout_ms: 50})
      gate = block_replay_seed_scans()

      assert :pending = Materializer.next_replay(mat_ctx, self())
      assert_receive {:replay_seed_scan_blocked, ^gate, _worker_pid}
      assert_receive {:materializer_shape_invalidated, _shape_handle}, 1_000

      state = :sys.get_state(materializer_pid)
      assert state.replay_sessions == %{}
      refute Map.has_key?(state.subscriber_monitors, self())
      refute MapSet.member?(state.subscribers, self())
      refute MapSet.member?(state.causal_subscribers, self())
    end

    test "an idle replay worker crash invalidates its subscriber", %{mat_ctx: mat_ctx} do
      parent = self()

      subscriber =
        spawn_replay_subscriber(parent, :subscriber, mat_ctx, LogOffset.new(50, 0))

      on_exit(fn ->
        if Process.alive?(subscriber), do: Process.exit(subscriber, :kill)
      end)

      assert_receive {:subscriber, :subscribed, ^subscriber, {:pending, _target}}

      assert_receive {:subscriber, :message, ^subscriber,
                      {:materializer_replay_ready, _shape_handle, {:ok, _, _}}}

      assert :pending = Materializer.next_replay(mat_ctx, subscriber)
      assert_receive {:subscriber, :message, ^subscriber, {:materializer_replay_ready, _handle}}

      materializer_pid = Materializer.whereis(mat_ctx)
      session = :sys.get_state(materializer_pid).replay_sessions[subscriber]
      assert session.status == :ready

      Process.exit(session.worker_pid, :kill)

      assert_receive {:subscriber, :message, ^subscriber,
                      {:materializer_shape_invalidated, _shape_handle}}

      state = :sys.get_state(materializer_pid)
      assert state.replay_sessions == %{}
      refute Map.has_key?(state.subscriber_monitors, subscriber)
      refute MapSet.member?(state.causal_subscribers, subscriber)
    end

    test "worker death in the payload delivery window always replies to the pull caller",
         %{mat_ctx: mat_ctx} do
      assert {:ok, _seed, _target} =
               subscribe_for_replay(mat_ctx, LogOffset.new(50, 0))

      materializer_pid = Materializer.whereis(mat_ctx)
      subscriber_pid = self()
      session = :sys.get_state(materializer_pid).replay_sessions[subscriber_pid]
      true = :erlang.suspend_process(session.worker_pid)

      pull =
        Task.async(fn ->
          Materializer.next_replay(mat_ctx, subscriber_pid)
        end)

      session = wait_for_replay_in_flight(materializer_pid, subscriber_pid)
      request_ref = session.in_flight.request_ref

      # Deterministically put the owner into the exact window after a payload is
      # ready but before the suspended worker can execute GenServer.reply/2.
      send(
        materializer_pid,
        {:replay_worker_pull_result, session.job_ref, session.worker_pid, request_ref,
         :payload_ready}
      )

      delivering = :sys.get_state(materializer_pid).replay_sessions[subscriber_pid]
      assert delivering.in_flight.phase == :delivering

      Process.exit(session.worker_pid, :kill)

      assert {:error, {:replay_worker_failed, :killed}} = Task.await(pull, 1_000)
      assert_receive {:materializer_shape_invalidated, _shape_handle}
    end

    test "terminating the Materializer kills its owned replay worker", %{mat_ctx: mat_ctx} do
      gate = block_replay_seed_scans()
      materializer_pid = Materializer.whereis(mat_ctx)
      Process.unlink(materializer_pid)

      assert {:pending, _target} =
               Materializer.subscribe(mat_ctx, LogOffset.new(50, 0))

      assert_receive {:replay_seed_scan_blocked, ^gate, worker_pid}
      worker_ref = Process.monitor(worker_pid)
      materializer_ref = Process.monitor(materializer_pid)

      :ok = GenServer.stop(materializer_pid, :shutdown)

      assert_receive {:DOWN, ^materializer_ref, :process, ^materializer_pid, :shutdown}
      assert_receive {:DOWN, ^worker_ref, :process, ^worker_pid, :killed}
    end

    test "does not start a replay worker before the coordinator attaches its lease",
         %{mat_ctx: mat_ctx} do
      coordinator = GenServer.whereis(ReplayCoordinator.name(mat_ctx.stack_id))
      materializer_pid = Materializer.whereis(mat_ctx)
      holder_job = make_ref()
      gate = block_replay_seed_scans()

      assert :ok = ReplayCoordinator.request(mat_ctx.stack_id, self(), holder_job)
      assert_receive {:replay_coordinator_granted, ^holder_job}

      assert {:pending, _target} = Materializer.subscribe(mat_ctx, LogOffset.new(50, 0))

      assert :sys.get_state(materializer_pid).replay_sessions[self()].status ==
               :waiting_for_stack_lease

      :ok = :sys.suspend(materializer_pid)

      on_exit(fn ->
        for pid <- [materializer_pid, coordinator] do
          if is_pid(pid) and Process.alive?(pid) do
            try do
              :sys.resume(pid)
            catch
              :exit, _reason -> :ok
            end
          end
        end
      end)

      assert :ok = ReplayCoordinator.release(mat_ctx.stack_id, self(), holder_job)
      :ok = :sys.suspend(coordinator)
      :ok = :sys.resume(materializer_pid)

      refute_receive {:replay_seed_scan_blocked, ^gate, _worker_pid}, 100

      :ok = :sys.resume(coordinator)
      assert_receive {:replay_seed_scan_blocked, ^gate, worker_pid}
      send(worker_pid, {:release_replay_seed_scan, gate})

      assert_receive {:materializer_replay_ready, _shape_handle, {:ok, _, _target}}
    end

    test "abrupt owner death removes a queued stack replay lease without spawning a worker",
         %{mat_ctx: mat_ctx} do
      parent = self()
      holder_job = make_ref()

      lease_holder =
        spawn(fn ->
          receive do
            {:replay_coordinator_granted, ^holder_job} ->
              send(parent, {:stack_replay_lease_held, self()})
              receive do: (:release_stack_replay_lease -> :ok)
          end
        end)

      on_exit(fn ->
        if Process.alive?(lease_holder), do: Process.exit(lease_holder, :kill)
      end)

      assert :ok =
               ReplayCoordinator.request(mat_ctx.stack_id, lease_holder, holder_job)

      assert_receive {:stack_replay_lease_held, ^lease_holder}
      materializer_pid = Materializer.whereis(mat_ctx)
      Process.unlink(materializer_pid)
      materializer_ref = Process.monitor(materializer_pid)

      assert {:pending, _target} =
               Materializer.subscribe(mat_ctx, LogOffset.new(50, 0))

      session = :sys.get_state(materializer_pid).replay_sessions[self()]
      assert session.status == :waiting_for_stack_lease
      assert is_nil(session.worker_pid)

      Process.exit(materializer_pid, :kill)
      assert_receive {:DOWN, ^materializer_ref, :process, ^materializer_pid, :killed}

      coordinator = ReplayCoordinator.name(mat_ctx.stack_id)
      assert :queue.len(:sys.get_state(coordinator).queue) == 0

      send(lease_holder, :release_stack_replay_lease)
      assert Support.TestUtils.wait_until(fn -> is_nil(:sys.get_state(coordinator).active) end)
    end

    test "stack replay coordinator expires one live worker that stops making progress",
         %{mat_ctx: mat_ctx} do
      coordinator = ReplayCoordinator.name(mat_ctx.stack_id)

      :sys.replace_state(coordinator, fn state ->
        %{state | idle_timeout_ms: 50}
      end)

      job_ref = make_ref()
      assert :ok = ReplayCoordinator.request(mat_ctx.stack_id, self(), job_ref)
      assert_receive {:replay_coordinator_granted, ^job_ref}

      worker = spawn(fn -> receive do: (:stop -> :ok) end)
      worker_ref = Process.monitor(worker)

      assert :ok =
               ReplayCoordinator.attach_worker(
                 mat_ctx.stack_id,
                 self(),
                 job_ref,
                 worker
               )

      assert_receive {:replay_worker_timeout, ^job_ref, ^worker}, 1_000
      assert_receive {:DOWN, ^worker_ref, :process, ^worker, :killed}, 1_000
      assert Support.TestUtils.wait_until(fn -> is_nil(:sys.get_state(coordinator).active) end)
    end

    test "coordinator restart invalidates every materializer session instead of orphaning it",
         %{mat_ctx: mat_ctx} do
      parent = self()
      materializer_pid = Materializer.whereis(mat_ctx)
      coordinator = GenServer.whereis(ReplayCoordinator.name(mat_ctx.stack_id))

      assert {:ok, _values, _offset} = Materializer.subscribe(mat_ctx)

      stale = spawn_replay_subscriber(parent, :stale, mat_ctx, LogOffset.new(50, 0))

      on_exit(fn ->
        if Process.alive?(stale), do: Process.exit(stale, :kill)
      end)

      assert_receive {:stale, :subscribed, ^stale, {:pending, _target}}

      assert_receive {:stale, :message, ^stale,
                      {:materializer_replay_ready, _shape_handle, {:ok, _, _}}}

      assert :sys.get_state(materializer_pid).replay_sessions[stale].status == :seed_only

      Process.unlink(materializer_pid)
      materializer_ref = Process.monitor(materializer_pid)
      Process.exit(coordinator, :kill)

      assert_receive {:materializer_shape_invalidated, _shape_handle}

      assert_receive {:stale, :message, ^stale, {:materializer_shape_invalidated, _shape_handle}}

      assert_receive {:DOWN, ^materializer_ref, :process, ^materializer_pid,
                      {:replay_coordinator_down, :killed}}

      assert Support.TestUtils.wait_until(fn ->
               case GenServer.whereis(ReplayCoordinator.name(mat_ctx.stack_id)) do
                 pid when is_pid(pid) -> pid != coordinator and Process.alive?(pid)
                 _ -> false
               end
             end)
    end

    test "stack replay coordinator bounds waiting jobs before spawning workers",
         %{mat_ctx: mat_ctx} do
      coordinator = ReplayCoordinator.name(mat_ctx.stack_id)

      :sys.replace_state(coordinator, fn state ->
        %{state | max_pending: 1}
      end)

      active_job = make_ref()
      rejected_job = make_ref()
      rejected_owner = spawn(fn -> receive do: (:stop -> :ok) end)

      on_exit(fn ->
        if Process.alive?(rejected_owner), do: Process.exit(rejected_owner, :kill)
      end)

      assert :ok = ReplayCoordinator.request(mat_ctx.stack_id, self(), active_job)
      assert_receive {:replay_coordinator_granted, ^active_job}

      assert {:error, :replay_stack_queue_full} =
               ReplayCoordinator.request(mat_ctx.stack_id, rejected_owner, rejected_job)

      assert :queue.len(:sys.get_state(coordinator).queue) == 0
      assert :ok = ReplayCoordinator.release(mat_ctx.stack_id, self(), active_job)
    end

    test "abnormal source death invalidates live, active replay, and queued subscribers",
         %{mat_ctx: mat_ctx} do
      gate = block_replay_seed_scans()
      parent = self()
      cursor = LogOffset.new(50, 0)

      live = spawn_replay_subscriber(parent, :live, mat_ctx, nil)
      active = spawn_replay_subscriber(parent, :active, mat_ctx, cursor)
      queued = spawn_replay_subscriber(parent, :queued, mat_ctx, cursor)

      on_exit(fn ->
        for pid <- [live, active, queued], Process.alive?(pid), do: Process.exit(pid, :kill)
      end)

      assert_receive {:live, :subscribed, ^live, {:ok, _, _}}
      assert_receive {:active, :subscribed, ^active, {:pending, _target}}
      assert_receive {:replay_seed_scan_blocked, ^gate, _worker_pid}
      assert_receive {:queued, :subscribed, ^queued, {:pending, _target}}

      materializer_pid = Materializer.whereis(mat_ctx)
      Process.unlink(materializer_pid)
      materializer_ref = Process.monitor(materializer_pid)

      send(
        materializer_pid,
        {{:consumer_down, mat_ctx.shape_handle}, make_ref(), :process, self(), :boom}
      )

      for {label, pid} <- [live: live, active: active, queued: queued] do
        assert_receive {^label, :message, ^pid, {:materializer_shape_invalidated, _shape_handle}}
      end

      assert_receive {:DOWN, ^materializer_ref, :process, ^materializer_pid, :shutdown}
    end

    test "admits only one rebuilt replay state per stack", ctx do
      second_handle = "second-replay-test-#{System.unique_integer([:positive])}"
      first_handle = ctx.mat_ctx.shape_handle
      first_offset = LogOffset.new(100, 0)
      first_target = LogOffset.new(200, 0)
      second_target = LogOffset.new(100, 0)
      second_storage = Storage.for_shape(second_handle, ctx.storage)
      Storage.start_link(second_storage)
      second_writer = Storage.init_writer!(second_storage, @shape)
      Storage.mark_snapshot_as_started(second_storage)

      Storage.make_new_snapshot!(
        make_snapshot_data([
          %Changes.NewRecord{record: %{"id" => "snapshot", "value" => "40"}}
        ]),
        second_storage
      )

      second_writer =
        Storage.append_to_log!(
          main_log_insert(LogOffset.new(100, 0), "second", "50"),
          second_writer
        )

      Storage.hibernate(second_writer)
      ConsumerRegistry.register_consumer(self(), second_handle, ctx.stack_id)

      {:ok, _second_materializer} =
        Materializer.start_link(%{
          stack_id: ctx.stack_id,
          shape_handle: second_handle,
          storage: ctx.storage,
          columns: ["value"],
          materialized_type: {:array, :int8}
        })

      respond_to_call(:await_snapshot_start, :started)
      respond_to_call(:subscribe_materializer, {:ok, second_target})

      second_ctx = %{stack_id: ctx.stack_id, shape_handle: second_handle}
      assert :ok = Materializer.wait_until_ready(second_ctx)
      gate = block_replay_seed_scans()

      assert {:pending, _target} =
               Materializer.subscribe(ctx.mat_ctx, LogOffset.new(50, 0))

      assert_receive {:replay_seed_scan_blocked, ^gate, first_worker}

      assert {:pending, ^second_target} =
               Materializer.subscribe(second_ctx, LogOffset.new(50, 0))

      second_session =
        second_ctx
        |> Materializer.whereis()
        |> :sys.get_state()
        |> Map.fetch!(:replay_sessions)
        |> Map.fetch!(self())

      assert second_session.status == :waiting_for_stack_lease
      assert is_nil(second_session.worker_pid)
      refute_receive {:replay_seed_scan_blocked, ^gate, _other_worker}, 100

      send(first_worker, {:release_replay_seed_scan, gate})

      assert_receive {:materializer_replay_ready, ^first_handle, {:ok, _, ^first_target}}
      assert_receive {:replay_seed_scan_blocked, ^gate, second_worker}, 1_000

      current_second_session =
        second_ctx
        |> Materializer.whereis()
        |> :sys.get_state()
        |> Map.fetch!(:replay_sessions)
        |> Map.fetch!(self())

      assert second_worker == current_second_session.worker_pid
      send(second_worker, {:release_replay_seed_scan, gate})

      assert_receive {:materializer_replay_ready, ^second_handle,
                      {:ok, second_seed, ^second_target}}

      assert second_seed == MapSet.new([40])

      first_materializer = Materializer.whereis(ctx.mat_ctx)
      second_materializer = Materializer.whereis(second_ctx)
      assert :sys.get_state(first_materializer).replay_sessions[self()].status == :seed_only
      assert :sys.get_state(second_materializer).replay_sessions[self()].status == :seed_only

      assert :pending = Materializer.next_replay(ctx.mat_ctx, self())
      assert_receive {:replay_seed_scan_blocked, ^gate, first_replay_worker}
      send(first_replay_worker, {:release_replay_seed_scan, gate})
      assert_receive {:materializer_replay_ready, ^first_handle}

      assert {:ok, %{lsn: ^first_offset}} =
               Materializer.next_replay(ctx.mat_ctx, self())

      assert {:ok, %{lsn: ^first_target}} =
               Materializer.next_replay(ctx.mat_ctx, self())

      assert :done = Materializer.next_replay(ctx.mat_ctx, self())

      assert :pending = Materializer.next_replay(second_ctx, self())
      assert_receive {:replay_seed_scan_blocked, ^gate, second_replay_worker}
      send(second_replay_worker, {:release_replay_seed_scan, gate})
      assert_receive {:materializer_replay_ready, ^second_handle}

      assert {:ok, %{lsn: ^second_target}} = Materializer.next_replay(second_ctx, self())
      assert :done = Materializer.next_replay(second_ctx, self())

      assert {:ok, _, ^second_target} =
               Materializer.subscribe(second_ctx, second_target)
    end

    test "replays only moves after from_lsn, tagged with per-range source LSNs",
         %{mat_ctx: mat_ctx} do
      # Behind at (100,0): the move-in for value 20 (at (100,0)) is already
      # applied, only value 30 (at (200,0)) must be replayed.
      assert {:ok, seed, applied_offset} =
               subscribe_for_replay(mat_ctx, LogOffset.new(100, 0))

      # Seed view is the link values as of (100,0): value 30 not yet included.
      assert seed == MapSet.new([10, 20])
      assert applied_offset == LogOffset.new(200, 0)
      refute_received {:materializer_changes, _handle, _payload}

      assert {:ok,
              %{
                move_in: [{30, "30"}],
                move_out: [],
                lsn: %LogOffset{tx_offset: 200, op_offset: 0}
              }} = Materializer.next_replay(mat_ctx, self())

      assert :done = Materializer.next_replay(mat_ctx, self())

      # The already-applied move-in for value 20 is NOT replayed.
      refute_received {:materializer_changes, _handle, %{move_in: [{20, "20"}]}}
    end

    test "replays every move when from_lsn is before all main-log moves",
         %{mat_ctx: mat_ctx} do
      assert {:ok, _seed, _applied} =
               subscribe_for_replay(mat_ctx, LogOffset.new(50, 0))

      refute_received {:materializer_changes, _handle, _payload}

      assert {:ok,
              %{
                move_in: [{20, "20"}],
                move_out: [],
                lsn: %LogOffset{tx_offset: 100, op_offset: 0}
              }} = Materializer.next_replay(mat_ctx, self())

      assert {:ok,
              %{
                move_in: [{30, "30"}],
                move_out: [],
                lsn: %LogOffset{tx_offset: 200, op_offset: 0}
              }} = Materializer.next_replay(mat_ctx, self())

      assert :done = Materializer.next_replay(mat_ctx, self())
    end

    test "keeps distinct logical commits that share one PostgreSQL tx offset", ctx do
      shape_handle = "same-tx-replay-test-#{System.unique_integer([:positive])}"
      storage = Storage.for_shape(shape_handle, ctx.storage)
      Storage.start_link(storage)
      writer = Storage.init_writer!(storage, @shape)
      Storage.mark_snapshot_as_started(storage)

      Storage.make_new_snapshot!(
        make_snapshot_data([%Changes.NewRecord{record: %{"id" => "1", "value" => "10"}}]),
        storage
      )

      first_boundary = LogOffset.new(100, 0)
      second_boundary = LogOffset.new(100, 1)
      writer = Storage.append_to_log!(main_log_insert(first_boundary, "2", "20"), writer)
      writer = Storage.append_to_log!(main_log_insert(second_boundary, "3", "30"), writer)
      Storage.hibernate(writer)

      ConsumerRegistry.register_consumer(self(), shape_handle, ctx.stack_id)

      {:ok, _pid} =
        Materializer.start_link(%{
          stack_id: ctx.stack_id,
          shape_handle: shape_handle,
          storage: ctx.storage,
          columns: ["value"],
          materialized_type: {:array, :int8}
        })

      respond_to_call(:await_snapshot_start, :started)
      respond_to_call(:subscribe_materializer, {:ok, second_boundary})

      mat_ctx = %{stack_id: ctx.stack_id, shape_handle: shape_handle}
      assert Materializer.wait_until_ready(mat_ctx) == :ok

      assert {:ok, _seed, ^second_boundary} =
               subscribe_for_replay(mat_ctx, LogOffset.last_before_real_offsets())

      assert {:ok, %{move_in: [{20, "20"}], lsn: ^first_boundary}} =
               Materializer.next_replay(mat_ctx, self())

      assert {:ok, %{move_in: [{30, "30"}], lsn: ^second_boundary}} =
               Materializer.next_replay(mat_ctx, self())

      assert :done = Materializer.next_replay(mat_ctx, self())
    end

    test "fails closed when durable history has no logical commit delimiter", ctx do
      shape_handle = "missing-boundary-replay-test-#{System.unique_integer([:positive])}"
      storage = Storage.for_shape(shape_handle, ctx.storage)
      Storage.start_link(storage)
      writer = Storage.init_writer!(storage, @shape)
      Storage.mark_snapshot_as_started(storage)
      Storage.make_new_snapshot!([], storage)

      target_offset = LogOffset.new(100, 0)
      writer = Storage.append_to_log!(main_log_insert(target_offset, "1", "10", false), writer)
      Storage.hibernate(writer)

      ConsumerRegistry.register_consumer(self(), shape_handle, ctx.stack_id)

      {:ok, materializer_pid} =
        Materializer.start_link(%{
          stack_id: ctx.stack_id,
          shape_handle: shape_handle,
          storage: ctx.storage,
          columns: ["value"],
          materialized_type: {:array, :int8}
        })

      respond_to_call(:await_snapshot_start, :started)
      respond_to_call(:subscribe_materializer, {:ok, target_offset})

      mat_ctx = %{stack_id: ctx.stack_id, shape_handle: shape_handle}
      assert Materializer.wait_until_ready(mat_ctx) == :ok

      assert {:ok, _seed, ^target_offset} =
               subscribe_for_replay(mat_ctx, LogOffset.last_before_real_offsets())

      assert {:error, {:missing_replay_boundary, ^target_offset, ^target_offset}} =
               Materializer.next_replay(mat_ctx, self())

      assert Process.alive?(materializer_pid)
      assert :sys.get_state(materializer_pid).replay_sessions == %{}
    end

    test "a large stale tail queues no subscriber messages before bounded pulls", ctx do
      shape_handle = "large-replay-test-#{System.unique_integer([:positive])}"
      storage = Storage.for_shape(shape_handle, ctx.storage)
      Storage.start_link(storage)
      writer = Storage.init_writer!(storage, @shape)
      Storage.mark_snapshot_as_started(storage)

      Storage.make_new_snapshot!(
        make_snapshot_data([%Changes.NewRecord{record: %{"id" => "snapshot", "value" => "10"}}]),
        storage
      )

      writer =
        Enum.reduce(1..64, writer, fn n, writer ->
          Storage.append_to_log!(
            main_log_insert(
              LogOffset.new(n * 100, 0),
              Integer.to_string(n),
              Integer.to_string(n + 100)
            ),
            writer
          )
        end)

      Storage.hibernate(writer)
      target_offset = LogOffset.new(6_400, 0)

      ConsumerRegistry.register_consumer(self(), shape_handle, ctx.stack_id)

      {:ok, _pid} =
        Materializer.start_link(%{
          stack_id: ctx.stack_id,
          shape_handle: shape_handle,
          storage: ctx.storage,
          columns: ["value"],
          materialized_type: {:array, :int8}
        })

      respond_to_call(:await_snapshot_start, :started)
      respond_to_call(:subscribe_materializer, {:ok, target_offset})

      mat_ctx = %{stack_id: ctx.stack_id, shape_handle: shape_handle}
      assert Materializer.wait_until_ready(mat_ctx) == :ok

      assert {:ok, seed, ^target_offset} =
               subscribe_for_replay(mat_ctx, LogOffset.last_before_real_offsets())

      assert seed == MapSet.new([10])

      refute_received {:materializer_changes, ^shape_handle, _payload}

      assert {:ok,
              %{
                move_in: [{101, "101"}],
                move_out: [],
                lsn: %LogOffset{tx_offset: 100, op_offset: 0}
              }} = Materializer.next_replay(mat_ctx, self())

      # One pull returns one transaction; the remaining 63 are retained in the
      # server-side session rather than flooding the subscriber mailbox.
      refute_received {:materializer_changes, ^shape_handle, _payload}
    end

    test "rejects one oversized source transaction without materializing it in a replay payload",
         ctx do
      shape_handle = "oversized-replay-test-#{System.unique_integer([:positive])}"
      storage = Storage.for_shape(shape_handle, ctx.storage)
      Storage.start_link(storage)
      writer = Storage.init_writer!(storage, @shape)
      Storage.mark_snapshot_as_started(storage)

      Storage.make_new_snapshot!(
        make_snapshot_data([%Changes.NewRecord{record: %{"id" => "snapshot", "value" => "10"}}]),
        storage
      )

      replay_memory_limit_bytes = 32_768
      transaction_size = 256

      transaction =
        Enum.flat_map(1..transaction_size, fn n ->
          main_log_insert(
            LogOffset.new(100, n),
            Integer.to_string(n),
            Integer.to_string(n + 100),
            n == transaction_size
          )
        end)

      oversized_boundary = LogOffset.new(100, transaction_size)
      writer = Storage.append_to_log!(transaction, writer)
      target_offset = LogOffset.new(200, 0)
      writer = Storage.append_to_log!(main_log_insert(target_offset, "tail", "999"), writer)
      Storage.hibernate(writer)

      ConsumerRegistry.register_consumer(self(), shape_handle, ctx.stack_id)

      {:ok, materializer_pid} =
        Materializer.start_link(%{
          stack_id: ctx.stack_id,
          shape_handle: shape_handle,
          storage: ctx.storage,
          columns: ["value"],
          materialized_type: {:array, :int8},
          replay_memory_limit_bytes: replay_memory_limit_bytes
        })

      respond_to_call(:await_snapshot_start, :started)
      respond_to_call(:subscribe_materializer, {:ok, target_offset})

      mat_ctx = %{stack_id: ctx.stack_id, shape_handle: shape_handle}
      assert Materializer.wait_until_ready(mat_ctx) == :ok

      assert {:ok, seed, ^target_offset} =
               subscribe_for_replay(mat_ctx, LogOffset.last_before_real_offsets())

      assert seed == MapSet.new([10])

      assert {:error, {limit_reason, attempted, ^replay_memory_limit_bytes}} =
               Materializer.next_replay(mat_ctx, self())

      assert limit_reason in [
               :replay_memory_limit_exceeded,
               :replay_process_memory_limit_exceeded
             ]

      assert attempted > replay_memory_limit_bytes
      assert Process.alive?(materializer_pid)
      assert :sys.get_state(materializer_pid).replay_sessions == %{}

      # The same budget also bounds seed reconstruction when the oversized
      # transaction is before (rather than after) the persisted outer cursor.
      assert {:error, {seed_limit_reason, seed_attempted, ^replay_memory_limit_bytes}} =
               subscribe_for_replay(mat_ctx, oversized_boundary)

      assert seed_limit_reason in [
               :replay_memory_limit_exceeded,
               :replay_process_memory_limit_exceeded
             ]

      assert seed_attempted > replay_memory_limit_bytes
    end

    test "serializes stale seed workers without retaining parallel replay states",
         %{mat_ctx: mat_ctx} do
      parent = self()
      cursor = LogOffset.new(50, 0)

      start_subscriber = fn label ->
        spawn(fn ->
          result = Materializer.subscribe(mat_ctx, cursor)
          send(parent, {label, :subscribed, self(), result})

          forward_replay_subscriber_messages(parent, label)
        end)
      end

      first = start_subscriber.(:first)
      on_exit(fn -> if Process.alive?(first), do: Process.exit(first, :kill) end)

      assert_receive {:first, :subscribed, ^first, {:pending, %LogOffset{tx_offset: 200}}}

      assert_receive {:first, :message, ^first,
                      {:materializer_replay_ready, _handle,
                       {:ok, first_seed, %LogOffset{tx_offset: 200}}}}

      assert first_seed == MapSet.new([10])

      second = start_subscriber.(:second)
      on_exit(fn -> if Process.alive?(second), do: Process.exit(second, :kill) end)

      assert_receive {:second, :subscribed, ^second, {:pending, %LogOffset{tx_offset: 200}}}

      assert_receive {:second, :message, ^second,
                      {:materializer_replay_ready, _handle,
                       {:ok, second_seed, %LogOffset{tx_offset: 200}}}}

      assert second_seed == MapSet.new([10])

      state = mat_ctx |> Materializer.whereis() |> :sys.get_state()
      assert map_size(state.replay_sessions) == 2
      assert Map.has_key?(state.replay_sessions, first)
      assert Map.has_key?(state.replay_sessions, second)

      assert Enum.all?(state.replay_sessions, fn {_pid, session} ->
               session.status == :seed_only and is_nil(session.worker_pid)
             end)

      assert :pending = Materializer.next_replay(mat_ctx, first)
      assert_receive {:first, :message, ^first, {:materializer_replay_ready, _handle}}

      assert {:ok, %{lsn: %LogOffset{tx_offset: 100}}} =
               Materializer.next_replay(mat_ctx, first)

      assert {:ok, %{lsn: %LogOffset{tx_offset: 200}}} =
               Materializer.next_replay(mat_ctx, first)

      assert :done = Materializer.next_replay(mat_ctx, first)

      state = mat_ctx |> Materializer.whereis() |> :sys.get_state()
      assert map_size(state.replay_sessions) == 1
      assert Map.has_key?(state.replay_sessions, second)
      refute Map.has_key?(state.pending_replay_cursors, second)
    end

    test "removes one seed-only owner without disturbing another stale subscriber",
         %{mat_ctx: mat_ctx} do
      parent = self()
      cursor = LogOffset.new(50, 0)

      start_subscriber = fn label ->
        spawn(fn ->
          result = Materializer.subscribe(mat_ctx, cursor)
          send(parent, {label, :subscribed, self(), result})

          forward_replay_subscriber_messages(parent, label)
        end)
      end

      first = start_subscriber.(:first)
      assert_receive {:first, :subscribed, ^first, {:pending, _target}}
      assert_receive {:first, :message, ^first, {:materializer_replay_ready, _, {:ok, _, _}}}

      second = start_subscriber.(:second)
      on_exit(fn -> if Process.alive?(second), do: Process.exit(second, :kill) end)
      assert_receive {:second, :subscribed, ^second, {:pending, _target}}

      assert_receive {:second, :message, ^second,
                      {:materializer_replay_ready, _handle,
                       {:ok, seed, %LogOffset{tx_offset: 200}}}}

      assert seed == MapSet.new([10])

      Process.exit(first, :kill)

      assert Support.TestUtils.wait_until(fn ->
               state = mat_ctx |> Materializer.whereis() |> :sys.get_state()
               not Map.has_key?(state.replay_sessions, first)
             end)

      state = mat_ctx |> Materializer.whereis() |> :sys.get_state()
      assert map_size(state.replay_sessions) == 1
      assert Map.has_key?(state.replay_sessions, second)
    end

    test "rejects a subscriber cursor ahead of durable source history", %{mat_ctx: mat_ctx} do
      assert {:error, :cursor_ahead_of_materializer} =
               Materializer.subscribe(mat_ctx, LogOffset.new(300, 0))

      refute_received {:materializer_changes, _handle, _payload}
    end

    test "replay emits a cursor-only payload for a persisted non-change transaction", ctx do
      shape_handle = "noop-replay-test-#{System.unique_integer([:positive])}"
      storage = Storage.for_shape(shape_handle, ctx.storage)
      Storage.start_link(storage)
      writer = Storage.init_writer!(storage, @shape)
      Storage.mark_snapshot_as_started(storage)

      Storage.make_new_snapshot!(
        make_snapshot_data([%Changes.NewRecord{record: %{"id" => "1", "value" => "10"}}]),
        storage
      )

      no_op_offset = LogOffset.new(100, 0)
      writer = Storage.append_to_log!(main_log_control(no_op_offset), writer)
      Storage.hibernate(writer)

      ConsumerRegistry.register_consumer(self(), shape_handle, ctx.stack_id)

      {:ok, _pid} =
        Materializer.start_link(%{
          stack_id: ctx.stack_id,
          shape_handle: shape_handle,
          storage: ctx.storage,
          columns: ["value"],
          materialized_type: {:array, :int8}
        })

      respond_to_call(:await_snapshot_start, :started)
      respond_to_call(:subscribe_materializer, {:ok, no_op_offset})

      mat_ctx = %{stack_id: ctx.stack_id, shape_handle: shape_handle}
      assert Materializer.wait_until_ready(mat_ctx) == :ok

      assert {:ok, seed, ^no_op_offset} =
               subscribe_for_replay(mat_ctx, LogOffset.last_before_real_offsets())

      assert seed == MapSet.new([10])

      assert {:ok, payload} = Materializer.next_replay(mat_ctx, self())
      assert payload == %{move_in: [], move_out: [], txids: [], lsn: no_op_offset}
      assert :done = Materializer.next_replay(mat_ctx, self())
    end

    test "catch-up extends to new durable work and hands off to live delivery without a gap",
         %{mat_ctx: mat_ctx, replay_writer: writer} do
      cursor = LogOffset.new(100, 0)
      original_target = LogOffset.new(200, 0)
      extended_target = LogOffset.new(300, 0)
      live_offset = LogOffset.new(400, 0)

      assert {:ok, _seed, ^original_target} = subscribe_for_replay(mat_ctx, cursor)

      assert {:ok, %{move_in: [{30, "30"}], lsn: ^original_target}} =
               Materializer.next_replay(mat_ctx, self())

      writer =
        Storage.append_to_log!(main_log_insert(extended_target, "4", "40"), writer)

      writer = Storage.hibernate(writer)
      Materializer.new_changes(mat_ctx, {original_target, extended_target})
      refute_received {:materializer_changes, _, %{lsn: ^extended_target}}

      assert {:ok, %{move_in: [{40, "40"}], lsn: ^extended_target}} =
               Materializer.next_replay(mat_ctx, self())

      assert :done = Materializer.next_replay(mat_ctx, self())

      writer = Storage.append_to_log!(main_log_insert(live_offset, "5", "50"), writer)
      _writer = Storage.hibernate(writer)
      Materializer.new_changes(mat_ctx, {extended_target, live_offset})

      assert_receive {:materializer_changes, _,
                      %{move_in: [{50, "50"}], move_out: [], lsn: ^live_offset}}
    end

    @tag with_pure_file_storage_opts: [chunk_bytes_threshold: 10]
    test "reconstructs a replay seed without applying the first main-log chunk twice", ctx do
      shape_handle = "delete-replay-test-#{System.unique_integer([:positive])}"
      storage = Storage.for_shape(shape_handle, ctx.storage)
      Storage.start_link(storage)
      writer = Storage.init_writer!(storage, @shape)
      Storage.mark_snapshot_as_started(storage)

      Storage.make_new_snapshot!(
        make_snapshot_data([%Changes.NewRecord{record: %{"id" => "1", "value" => "10"}}]),
        storage
      )

      delete_offset = LogOffset.new(100, 0)
      insert_offset = LogOffset.new(200, 0)

      writer = Storage.append_to_log!(main_log_delete(delete_offset, "1", "10"), writer)
      writer = Storage.append_to_log!(main_log_insert(insert_offset, "1", "10"), writer)
      Storage.hibernate(writer)

      first_main_chunk_end =
        Storage.get_chunk_end_log_offset(LogOffset.last_before_real_offsets(), storage)

      assert first_main_chunk_end == delete_offset

      first_snapshot_chunk =
        Storage.get_chunk_end_log_offset(LogOffset.before_all(), storage)

      offset_after_first_snapshot_chunk =
        Storage.get_chunk_end_log_offset(first_snapshot_chunk, storage)

      last_snapshot_chunk =
        if LogOffset.compare(
             offset_after_first_snapshot_chunk,
             LogOffset.last_before_real_offsets()
           ) == :gt,
           do: first_snapshot_chunk,
           else: offset_after_first_snapshot_chunk

      assert [%{"headers" => %{"operation" => "delete"}}] =
               Storage.get_log_stream(
                 last_snapshot_chunk,
                 LogOffset.last_before_real_offsets(),
                 storage
               )
               |> Enum.map(&Jason.decode!/1)

      assert [] =
               Storage.get_log_stream_with_offsets(
                 last_snapshot_chunk,
                 LogOffset.last_before_real_offsets(),
                 storage
               )
               |> Enum.to_list()

      ConsumerRegistry.register_consumer(self(), shape_handle, ctx.stack_id)

      {:ok, materializer} =
        Materializer.start_link(%{
          stack_id: ctx.stack_id,
          shape_handle: shape_handle,
          storage: ctx.storage,
          columns: ["value"],
          materialized_type: {:array, :int8}
        })

      respond_to_call(:await_snapshot_start, :started)
      respond_to_call(:subscribe_materializer, {:ok, insert_offset})

      mat_ctx = %{stack_id: ctx.stack_id, shape_handle: shape_handle}
      assert Materializer.wait_until_ready(mat_ctx) == :ok
      assert Materializer.get_link_values(mat_ctx) == MapSet.new([10])

      Process.unlink(materializer)

      assert {:ok, seed, ^insert_offset} = subscribe_for_replay(mat_ctx, delete_offset)
      assert seed == MapSet.new()

      assert {:ok,
              %{
                move_in: [{10, "10"}],
                move_out: [],
                lsn: ^insert_offset
              }} = Materializer.next_replay(mat_ctx, self())

      assert :done = Materializer.next_replay(mat_ctx, self())
    end

    test "does not replay when from_lsn is at or past the applied position",
         %{mat_ctx: mat_ctx} do
      assert {:ok, seed, _applied} =
               Materializer.subscribe(mat_ctx, LogOffset.new(200, 0))

      # Caught up: seed is the current link values and nothing is replayed.
      assert seed == MapSet.new([10, 20, 30])
      refute_received {:materializer_changes, _handle, _payload}
      assert :done = Materializer.next_replay(mat_ctx, self())
    end

    @tag chunk_size: 1
    test "replays persisted move control messages at their authoritative log offsets", ctx do
      shape_handle = "nested-replay-test-#{System.unique_integer([:positive])}"
      storage = Storage.for_shape(shape_handle, ctx.storage)
      Storage.start_link(storage)
      writer = Storage.init_writer!(storage, @shape)
      Storage.mark_snapshot_as_started(storage)

      Storage.make_new_snapshot!(
        make_snapshot_data([
          %Changes.NewRecord{
            record: %{"id" => "1", "value" => "10"},
            move_tags: ["nested-tag"],
            active_conditions: [true]
          }
        ]),
        storage
      )

      cursor = LogOffset.new(100, 0)
      writer = Storage.append_to_log!(main_log_insert(cursor, "2", "20"), writer)

      {{^cursor, ignored_offset}, writer} =
        Storage.append_control_message!(
          Jason.encode!(%{headers: %{control: "up_to_date"}}),
          writer
        )

      {{^ignored_offset, control_offset}, writer} =
        Storage.append_control_message!(
          Jason.encode!(%{
            headers: %{
              event: "move-out",
              patterns: [%{pos: 0, value: "nested-tag"}],
              txids: [42],
              last: true
            }
          }),
          writer
        )

      Storage.hibernate(writer)

      ConsumerRegistry.register_consumer(self(), shape_handle, ctx.stack_id)

      {:ok, _pid} =
        Materializer.start_link(%{
          stack_id: ctx.stack_id,
          shape_handle: shape_handle,
          storage: ctx.storage,
          columns: ["value"],
          materialized_type: {:array, :int8}
        })

      respond_to_call(:await_snapshot_start, :started)
      respond_to_call(:subscribe_materializer, {:ok, control_offset})

      mat_ctx = %{stack_id: ctx.stack_id, shape_handle: shape_handle}
      assert Materializer.wait_until_ready(mat_ctx) == :ok
      assert Materializer.get_link_values(mat_ctx) == MapSet.new([20])

      assert {:ok, seed, ^control_offset} = subscribe_for_replay(mat_ctx, cursor)
      assert seed == MapSet.new([10, 20])

      assert {:ok,
              %{
                move_in: [],
                move_out: [{10, "10"}],
                lsn: ^control_offset,
                txids: [42]
              }} = Materializer.next_replay(mat_ctx, self())

      assert :done = Materializer.next_replay(mat_ctx, self())
    end

    @tag chunk_size: 1
    test "replays persisted move-in rows at their authoritative log offsets", ctx do
      shape_handle = "move-in-replay-test-#{System.unique_integer([:positive])}"
      storage = Storage.for_shape(shape_handle, ctx.storage)
      Storage.start_link(storage)
      writer = Storage.init_writer!(storage, @shape)
      Storage.mark_snapshot_as_started(storage)

      Storage.make_new_snapshot!(
        make_snapshot_data([%Changes.NewRecord{record: %{"id" => "1", "value" => "10"}}]),
        storage
      )

      cursor = LogOffset.new(100, 0)
      writer = Storage.append_to_log!(main_log_insert(cursor, "2", "20"), writer)

      move_in_item =
        Jason.encode!(%{
          key: ~s|"public"."test_table"/"3"|,
          value: %{"id" => "3", "value" => "30"},
          headers: %{operation: "insert", tags: ["nested-tag"], active_conditions: [true]}
        })

      Storage.write_move_in_snapshot!(
        [[~s|"public"."test_table"/"3"|, ["nested-tag"], move_in_item]],
        "nested-move-in",
        storage
      )

      {{^cursor, move_in_offset}, writer} =
        Storage.append_move_in_snapshot_to_log!("nested-move-in", writer)

      causal_origin = LogOffset.new(900, 4)

      marker =
        Jason.encode!(%{
          headers: %{
            event: "move-out",
            patterns: [],
            txids: [],
            last: true,
            generated_move_boundary: 1,
            causal_origin: to_string(causal_origin),
            causal_depth: 2
          }
        })

      {{^move_in_offset, move_boundary}, writer} =
        Storage.append_control_message!(marker, writer)

      Storage.hibernate(writer)

      ConsumerRegistry.register_consumer(self(), shape_handle, ctx.stack_id)

      {:ok, _pid} =
        Materializer.start_link(%{
          stack_id: ctx.stack_id,
          shape_handle: shape_handle,
          storage: ctx.storage,
          columns: ["value"],
          materialized_type: {:array, :int8}
        })

      respond_to_call(:await_snapshot_start, :started)
      respond_to_call(:subscribe_materializer, {:ok, move_boundary})

      mat_ctx = %{stack_id: ctx.stack_id, shape_handle: shape_handle}
      assert Materializer.wait_until_ready(mat_ctx) == :ok
      assert Materializer.get_link_values(mat_ctx) == MapSet.new([10, 20, 30])

      assert {:ok, seed, ^move_boundary} = subscribe_for_replay(mat_ctx, cursor)
      assert seed == MapSet.new([10, 20])

      assert {:ok,
              %{
                move_in: [{30, "30"}],
                move_out: [],
                lsn: ^move_boundary,
                txids: [],
                causal_origin: ^causal_origin,
                causal_depth: 2
              }} = Materializer.next_replay(mat_ctx, self())

      assert :done = Materializer.next_replay(mat_ctx, self())
    end

    @tag chunk_size: 1
    test "fails closed for generated move boundaries without valid causal metadata", ctx do
      causal_origin = LogOffset.new(900, 4)

      cases = [
        {:legacy, %{event: "move-out", patterns: [], txids: [], last: true},
         :replay_causal_origin_unavailable},
        {:missing_origin,
         %{
           event: "move-out",
           patterns: [],
           txids: [],
           last: true,
           generated_move_boundary: 1,
           causal_depth: 1
         }, :invalid_replay_causal_origin},
        {:non_real_origin,
         %{
           event: "move-out",
           patterns: [],
           txids: [],
           last: true,
           generated_move_boundary: 1,
           causal_origin: "0_0",
           causal_depth: 1
         }, :invalid_replay_causal_origin},
        {:invalid_depth,
         %{
           event: "move-out",
           patterns: [],
           txids: [],
           last: true,
           generated_move_boundary: 1,
           causal_origin: to_string(causal_origin),
           causal_depth: -1
         }, :invalid_replay_causal_depth},
        {:unsupported_version,
         %{
           event: "move-out",
           patterns: [],
           txids: [],
           last: true,
           generated_move_boundary: 2,
           causal_origin: to_string(causal_origin),
           causal_depth: 1
         }, :unsupported_generated_move_boundary}
      ]

      Enum.each(cases, fn {label, marker_headers, expected_reason} ->
        shape_handle =
          "invalid-causal-boundary-#{label}-#{System.unique_integer([:positive])}"

        storage = Storage.for_shape(shape_handle, ctx.storage)
        Storage.start_link(storage)
        writer = Storage.init_writer!(storage, @shape)
        Storage.mark_snapshot_as_started(storage)
        Storage.make_new_snapshot!([], storage)

        cursor = LogOffset.new(100, 0)
        writer = Storage.append_to_log!(main_log_insert(cursor, "1", "10"), writer)

        {{^cursor, move_boundary}, writer} =
          Storage.append_control_message!(
            Jason.encode!(%{headers: marker_headers}),
            writer
          )

        Storage.hibernate(writer)
        ConsumerRegistry.register_consumer(self(), shape_handle, ctx.stack_id)

        {:ok, materializer_pid} =
          Materializer.start_link(%{
            stack_id: ctx.stack_id,
            shape_handle: shape_handle,
            storage: ctx.storage,
            columns: ["value"],
            materialized_type: {:array, :int8}
          })

        respond_to_call(:await_snapshot_start, :started)
        respond_to_call(:subscribe_materializer, {:ok, move_boundary})

        mat_ctx = %{stack_id: ctx.stack_id, shape_handle: shape_handle}
        assert Materializer.wait_until_ready(mat_ctx) == :ok
        assert {:ok, _seed, ^move_boundary} = subscribe_for_replay(mat_ctx, cursor)

        replay_result = Materializer.next_replay(mat_ctx, self())

        if expected_reason == :replay_causal_origin_unavailable do
          assert {:error, {^expected_reason, ^move_boundary}} = replay_result
        else
          assert {:error, {^expected_reason, ^move_boundary, _detail}} = replay_result
        end

        assert Process.alive?(materializer_pid)
        assert :sys.get_state(materializer_pid).replay_sessions == %{}
      end)
    end
  end

  describe "startup race condition handling" do
    # Tests for the race condition where Consumer dies between await_snapshot_start
    # and subscribe_materializer. See concurrency_analysis/MATERIALIZER_RACE_ANALYSIS.md

    test "shuts down gracefully when await_snapshot_start returns error",
         %{storage: storage, stack_id: stack_id, shape_handle: shape_handle} do
      # Trap exits so the test process doesn't die when Materializer shuts down
      Process.flag(:trap_exit, true)

      {:ok, pid} =
        Materializer.start_link(%{
          stack_id: stack_id,
          shape_handle: shape_handle,
          storage: storage,
          columns: ["value"],
          materialized_type: {:array, :int8}
        })

      ref = Process.monitor(pid)

      respond_to_call(:await_snapshot_start, {:error, "Consumer terminated"})

      assert_receive {:DOWN, ^ref, :process, ^pid, :shutdown}
    end

    test "shuts down gracefully when Consumer dies during await_snapshot_start call",
         %{storage: storage, stack_id: stack_id} do
      # This test exercises the try/catch by having the "consumer" die mid-call.
      # We spawn a short-lived process as the consumer that dies before responding.
      Process.flag(:trap_exit, true)

      # Use a unique shape handle for this test
      dying_handle = "dying-consumer-#{System.unique_integer()}"

      # Set up storage for the dying handle
      Storage.for_shape(dying_handle, storage) |> Storage.start_link()
      writer = Storage.for_shape(dying_handle, storage) |> Storage.init_writer!(@shape)
      Storage.for_shape(dying_handle, storage) |> Storage.mark_snapshot_as_started()
      Storage.hibernate(writer)
      Storage.for_shape(dying_handle, storage) |> then(&Storage.make_new_snapshot!([], &1))

      # Spawn a process that will die immediately when it receives the call
      dying_consumer =
        spawn(fn ->
          receive do
            {:"$gen_call", _from, :await_snapshot_start} ->
              # Die without responding - this causes GenServer.call to exit with :noproc
              exit(:normal)
          end
        end)

      # Register it as the consumer
      ConsumerRegistry.register_consumer(dying_consumer, dying_handle, stack_id)

      {:ok, pid} =
        Materializer.start_link(%{
          stack_id: stack_id,
          shape_handle: dying_handle,
          storage: storage,
          columns: ["value"],
          materialized_type: {:array, :int8}
        })

      ref = Process.monitor(pid)

      # The Materializer should shut down gracefully when the GenServer.call exits.
      # We accept :shutdown (normal case) or :noproc (if process exited before monitor was set up)
      assert_receive {:DOWN, ^ref, :process, ^pid, reason}, 1000
      assert reason in [:shutdown, :noproc]
    end

    test "shuts down gracefully when Consumer dies during subscribe_materializer call",
         %{storage: storage, stack_id: stack_id} do
      # This test exercises the try/catch for subscribe_materializer failure
      Process.flag(:trap_exit, true)

      dying_handle = "dying-consumer-subscribe-#{System.unique_integer()}"

      Storage.for_shape(dying_handle, storage) |> Storage.start_link()
      writer = Storage.for_shape(dying_handle, storage) |> Storage.init_writer!(@shape)
      Storage.for_shape(dying_handle, storage) |> Storage.mark_snapshot_as_started()
      Storage.hibernate(writer)
      Storage.for_shape(dying_handle, storage) |> then(&Storage.make_new_snapshot!([], &1))

      # Spawn a process that responds to await_snapshot_start but dies on subscribe
      dying_consumer =
        spawn(fn ->
          receive do
            {:"$gen_call", {from, ref}, :await_snapshot_start} ->
              # Respond successfully to await_snapshot_start
              send(from, {ref, :started})
          end

          receive do
            {:"$gen_call", _from, {:subscribe_materializer, _}} ->
              # Die without responding
              exit(:normal)
          end
        end)

      ConsumerRegistry.register_consumer(dying_consumer, dying_handle, stack_id)

      {:ok, pid} =
        Materializer.start_link(%{
          stack_id: stack_id,
          shape_handle: dying_handle,
          storage: storage,
          columns: ["value"],
          materialized_type: {:array, :int8}
        })

      ref = Process.monitor(pid)

      # We accept :shutdown (normal case) or :noproc (if process exited before monitor was set up)
      assert_receive {:DOWN, ^ref, :process, ^pid, reason}, 1000
      assert reason in [:shutdown, :noproc]
    end
  end

  defp block_replay_seed_scans do
    Support.TestUtils.activate_mocks_for_descendant_procs(Materializer)
    test_pid = self()
    gate = make_ref()

    Repatch.patch(Storage, :get_log_stream_with_offsets, [mode: :shared], fn
      min_offset, max_offset, storage ->
        blocked_key = {__MODULE__, gate}

        unless Process.get(blocked_key, false) do
          Process.put(blocked_key, true)
          send(test_pid, {:replay_seed_scan_blocked, gate, self()})

          receive do
            {:release_replay_seed_scan, ^gate} -> :ok
          after
            5_000 -> exit(:replay_seed_scan_gate_timeout)
          end
        end

        Repatch.real(Storage.get_log_stream_with_offsets(min_offset, max_offset, storage))
    end)

    gate
  end

  defp slow_replay_scans(delay_ms) do
    Repatch.patch(Storage, :get_log_stream_with_offsets, fn min_offset, max_offset, storage ->
      Repatch.real(Storage.get_log_stream_with_offsets(min_offset, max_offset, storage))
      |> Stream.each(fn _item -> Process.sleep(delay_ms) end)
    end)
  end

  defp spawn_replay_subscriber(parent, label, materializer, cursor) do
    spawn(fn ->
      result = Materializer.subscribe(materializer, cursor)
      send(parent, {label, :subscribed, self(), result})
      forward_replay_subscriber_messages(parent, label)
    end)
  end

  defp spawn_causal_subscriber(parent, materializer) do
    spawn(fn ->
      result = Materializer.subscribe_causally(Materializer.whereis(materializer), nil)
      send(parent, {:causal_subscribed, self(), result})
      forward_causal_subscriber_messages(parent)
    end)
  end

  defp spawn_blocking_causal_subscriber(parent, materializer) do
    spawn(fn ->
      result = Materializer.subscribe_causally(Materializer.whereis(materializer), nil)
      send(parent, {:blocking_causal_subscribed, self(), result})
      block_causal_deliveries(parent)
    end)
  end

  defp spawn_hung_causal_subscriber(parent, materializer) do
    spawn(fn ->
      result = Materializer.subscribe_causally(Materializer.whereis(materializer), nil)
      send(parent, {:hung_causal_subscribed, self(), result})

      receive do
        {:"$gen_call", _from,
         {:reserve_materializer_batch, _dependency_handle, _causal_token, _offset,
          _expected_resolution_bytes}} ->
          receive do: (:never -> :ok)
      end
    end)
  end

  defp spawn_delivery_hung_causal_subscriber(parent, materializer) do
    spawn(fn ->
      result = Materializer.subscribe_causally(Materializer.whereis(materializer), nil)
      send(parent, {:delivery_hung_causal_subscribed, self(), result})
      hang_on_causal_delivery()
    end)
  end

  defp hang_on_causal_delivery do
    receive do
      {:"$gen_call", from,
       {:reserve_materializer_batch, _dependency_handle, _causal_token, _offset,
        _expected_resolution_bytes}} ->
        GenServer.reply(from, :ok)
        hang_on_causal_delivery()

      {:"$gen_call", from,
       {:prepare_materializer_batch, _dependency_handle, _causal_token,
        _expected_resolution_bytes}} ->
        GenServer.reply(from, :ok)
        hang_on_causal_delivery()

      {:"$gen_call", _from, {:deliver_materializer_batch, _dependency_handle, _payload}} ->
        receive do: (:never -> :ok)
    end
  end

  defp block_causal_deliveries(parent) do
    receive do
      {:"$gen_call", from,
       {:reserve_materializer_batch, _dependency_handle, _causal_token, _offset,
        _expected_resolution_bytes}} ->
        GenServer.reply(from, :ok)
        block_causal_deliveries(parent)

      {:"$gen_call", from, {:deliver_materializer_batch, _dependency_handle, %{lsn: offset}}} ->
        send(parent, {:causal_delivery_waiting, self(), offset})
        assert_receive {:release_causal_delivery, ^offset}
        GenServer.reply(from, :ok)
        block_causal_deliveries(parent)
    end
  end

  defp forward_causal_subscriber_messages(parent) do
    receive do
      {:"$gen_call", from,
       {:reserve_materializer_batch, dependency_handle, causal_token, offset,
        _expected_resolution_bytes}} ->
        send(parent, {:causal_reserved, self(), dependency_handle, causal_token, offset})
        GenServer.reply(from, :ok)
        forward_causal_subscriber_messages(parent)

      {:"$gen_call", from,
       {:prepare_materializer_batch, _dependency_handle, _causal_token,
        _expected_resolution_bytes}} ->
        GenServer.reply(from, :ok)
        forward_causal_subscriber_messages(parent)

      {:"$gen_call", from, {:deliver_materializer_batch, dependency_handle, payload}} ->
        send(
          parent,
          {:causal_message, self(), {:materializer_changes, dependency_handle, payload}}
        )

        GenServer.reply(from, :ok)
        forward_causal_subscriber_messages(parent)

      {:"$gen_call", from, {:deliver_materializer_causal_end, dependency_handle, causal_token}} ->
        send(
          parent,
          {:causal_message, self(), {:materializer_causal_end, dependency_handle, causal_token}}
        )

        GenServer.reply(from, :ok)
        forward_causal_subscriber_messages(parent)

      message ->
        send(parent, {:causal_message, self(), message})
        forward_causal_subscriber_messages(parent)
    end
  end

  defp wait_for_replay_in_flight(materializer_pid, subscriber_pid) do
    Enum.reduce_while(1..100, nil, fn _, _ ->
      session = :sys.get_state(materializer_pid).replay_sessions[subscriber_pid]

      if session && session.in_flight do
        {:halt, session}
      else
        Process.sleep(5)
        {:cont, nil}
      end
    end) ||
      flunk("replay pull was not dispatched")
  end

  defp forward_replay_subscriber_messages(parent, label) do
    receive do
      message ->
        send(parent, {label, :message, self(), message})
        forward_replay_subscriber_messages(parent, label)
    end
  end
end
