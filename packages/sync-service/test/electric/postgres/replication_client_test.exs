defmodule Electric.Postgres.ReplicationClientTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  import Support.ComponentSetup

  import Support.DbSetup, except: [with_publication: 1]
  import Support.DbStructureSetup

  alias Electric.Replication.LogOffset
  alias Electric.LsnTracker
  alias Electric.Postgres.CausalMarker
  alias Electric.Postgres.Lsn
  alias Electric.Postgres.ReplicationClient
  alias Electric.Replication.ShapeLogCollector
  alias Electric.Shapes.ConsumerRegistry

  alias Electric.Replication.Changes.Relation
  alias Electric.Replication.Changes.DeletedRecord
  alias Electric.Replication.Changes.NewRecord
  alias Electric.Replication.Changes.UpdatedRecord
  alias Electric.Replication.Changes.TransactionFragment
  alias Electric.Replication.TransactionBuilder

  # Larger than average timeout for assertions that require
  # seeing changes back from the database, as it can be especially
  # slow on CI/Docker etc
  @assert_receive_db_timeout 2000

  defmodule MockConnectionManager do
    def receive_casts(test_pid) do
      receive do
        message ->
          if response = process_message(message) do
            send(test_pid, response)
          end

          receive_casts(test_pid)
      end
    end

    defp process_message({:"$gen_cast", :replication_client_started}), do: nil
    defp process_message({:"$gen_cast", {:pg_info_obtained, _}}), do: nil
    defp process_message({:"$gen_cast", {:pg_system_identified, _}}), do: nil
    defp process_message({:"$gen_cast", :replication_client_lock_acquired}), do: :lock_acquired

    defp process_message({:"$gen_cast", {:replication_client_lock_acquisition_failed, err}}),
      do: {:lock_acquisition_failed, err}

    defp process_message({:"$gen_cast", :replication_client_created_new_slot}), do: nil

    defp process_message({:"$gen_cast", {:replication_client_caught_up, replication_client_pid}}),
      do: {:replication_caught_up, replication_client_pid}

    defp process_message({:"$gen_cast", :replication_client_streamed_first_message}),
      do: {self(), :streaming_started}
  end

  defmodule MockTransactionProcessor do
    use GenServer, restart: :temporary

    alias Electric.Replication.Changes.TransactionFragment

    def start_link(test_pid) do
      GenServer.start_link(__MODULE__, test_pid, name: __MODULE__)
    end

    @impl true
    def init(test_pid) do
      {:ok, %{test_pid: test_pid, should_crash?: false, delay: 0}}
    end

    def handle_event(event) do
      GenServer.call(__MODULE__, {:handle_event, event}, :infinity)
    end

    def handle_event_async(event) do
      case GenServer.whereis(__MODULE__) do
        nil ->
          exit({:noproc, {__MODULE__, :handle_event_async, [event]}})

        pid ->
          monitor_ref = Process.monitor(pid)
          send(pid, {:"$gen_call", {self(), monitor_ref}, {:handle_event, event}})
          monitor_ref
      end
    end

    def toggle_crash(should_crash?) do
      GenServer.call(__MODULE__, {:toggle_crash, should_crash?})
    end

    def set_delay(delay_ms) do
      GenServer.call(__MODULE__, {:set_delay, delay_ms})
    end

    @impl true
    def handle_call(
          {:handle_event, %TransactionFragment{} = txn_fragment},
          {replication_client_pid, _ref},
          state
        ) do
      if state.delay > 0, do: Process.sleep(state.delay)

      if state.should_crash? do
        raise "Interrupting transaction processing abnormally"
      end

      send(state.test_pid, {:from_replication, txn_fragment})

      if txn_fragment.changes == [] and not is_nil(txn_fragment.commit) do
        send(
          replication_client_pid,
          {:flush_boundary_updated, Electric.Postgres.Lsn.to_integer(txn_fragment.lsn)}
        )
      end

      {:reply, :ok, state}
    end

    def handle_call({:handle_event, %Relation{} = relation}, _from, state) do
      send(state.test_pid, {:from_replication, [relation]})
      {:reply, :ok, state}
    end

    def handle_call({:toggle_crash, should_crash?}, _from, state) do
      {:reply, :ok, %{state | should_crash?: should_crash?}}
    end

    def handle_call({:set_delay, delay_ms}, _from, state) do
      {:reply, :ok, %{state | delay: delay_ms}}
    end
  end

  setup do
    # Spawn a dummy process to serve as the black hole for the messages that
    # ReplicationClient normally sends to Connection.Manager.
    pid = spawn_link(MockConnectionManager, :receive_casts, [self()])
    %{connection_manager: pid}
  end

  setup :with_stack_id_from_test
  setup :with_slot_name

  setup %{stack_id: stack_id} do
    start_supervised!(
      {Task.Supervisor,
       name: Electric.ProcessRegistry.name(stack_id, Electric.StackTaskSupervisor)}
    )

    %{consumer_registry_table: ConsumerRegistry.registry_table(stack_id)}
  end

  describe "shape collector processing readiness" do
    test "retries a pending not-ready event before external health becomes active", ctx do
      Registry.register(
        Electric.ProcessRegistry.registry_name(ctx.stack_id),
        {ShapeLogCollector, nil},
        nil
      )

      event = make_ref()

      state = %ReplicationClient.State{
        stack_id: ctx.stack_id,
        handle_event: {__MODULE__, :test_handle_event_async, [self()]},
        publication_name: "test_publication",
        event_retry_wait: {:collector_processing, event}
      }

      assert {:noreply,
              %{event_retry_wait: nil, shape_log_collector_processing_pid: processing_pid}} =
               ReplicationClient.handle_info(
                 {ReplicationClient, :shape_log_collector_processing_started, self()},
                 state
               )

      assert processing_pid == self()
      assert_receive {:process_event, ^event, retry_time}, 200
      assert retry_time > 0
    end

    test "remembers processing readiness when its signal beats the async not-ready reply", ctx do
      Registry.register(
        Electric.ProcessRegistry.registry_name(ctx.stack_id),
        {ShapeLogCollector, nil},
        nil
      )

      event = make_ref()
      async_ref = make_ref()
      started_at = System.monotonic_time(:millisecond)

      state = %ReplicationClient.State{
        stack_id: ctx.stack_id,
        handle_event: {__MODULE__, :test_handle_event_async, [self()]},
        publication_name: "test_publication",
        pending_event: {async_ref, event, 1_000, started_at}
      }

      assert {:noreply, processing_state} =
               ReplicationClient.handle_info(
                 {ReplicationClient, :shape_log_collector_processing_started, self()},
                 state
               )

      assert processing_state.shape_log_collector_processing_pid == self()
      assert processing_state.pending_event == state.pending_event

      assert {:noreply, retrying_state} =
               ReplicationClient.handle_info(
                 {async_ref, {:error, :not_ready}},
                 processing_state
               )

      assert retrying_state.pending_event == nil
      assert retrying_state.event_retry_wait == nil
      assert_receive {:process_event, ^event, retry_time}, 200
      assert retry_time > 0
    end

    test "ignores a delayed processing signal from a replaced collector", ctx do
      Registry.register(
        Electric.ProcessRegistry.registry_name(ctx.stack_id),
        {ShapeLogCollector, nil},
        nil
      )

      stale_collector = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(stale_collector, :kill) end)

      event = make_ref()

      state = %ReplicationClient.State{
        stack_id: ctx.stack_id,
        handle_event: {__MODULE__, :test_handle_event_async, [self()]},
        publication_name: "test_publication",
        event_retry_wait: {:collector_processing, event}
      }

      assert {:noreply, ^state} =
               ReplicationClient.handle_info(
                 {ReplicationClient, :shape_log_collector_processing_started, stale_collector},
                 state
               )

      refute_receive {:process_event, ^event, _}, 100
    end

    test "not-ready retries keep one direct wait and register no stale active waiters", ctx do
      for cycle <- 1..100 do
        event = {:event, cycle}
        async_ref = make_ref()

        state = %ReplicationClient.State{
          stack_id: ctx.stack_id,
          handle_event: {__MODULE__, :test_handle_event_async, [self()]},
          publication_name: "test_publication",
          pending_event: {async_ref, event, 1_000, System.monotonic_time(:millisecond)}
        }

        assert {:noreply, waiting_state} =
                 ReplicationClient.handle_info({async_ref, {:error, :not_ready}}, state)

        assert waiting_state.pending_event == nil
        assert waiting_state.event_retry_wait == {:collector_processing, event}
      end
    end

    test "processing notification fails fast when the per-stack registry is unavailable" do
      missing_stack = "missing-registry-#{System.unique_integer([:positive])}"

      assert {:process_registry_unavailable, ^missing_stack} =
               catch_exit(
                 ReplicationClient.notify_shape_log_collector_processing_started(
                   missing_stack,
                   self()
                 )
               )
    end
  end

  describe "ReplicationClient init" do
    setup [:with_unique_db, :with_basic_tables, :with_status_monitor, :with_lsn_tracker]

    test "creates an empty publication on startup if requested",
         %{db_conn: conn, connection_manager: connection_manager, slot_name: slot_name} = ctx do
      replication_opts = [
        connection_opts: ctx.db_config,
        stack_id: ctx.stack_id,
        publication_name: ctx.slot_name,
        try_creating_publication?: true,
        slot_name: ctx.slot_name,
        handle_event: {__MODULE__, :test_handle_event_async, [self()]},
        connection_manager: connection_manager
      ]

      start_client(ctx, replication_opts: replication_opts)

      assert %{rows: [[^slot_name]]} =
               Postgrex.query!(conn, "SELECT pubname FROM pg_publication", [])

      assert %{rows: []} = Postgrex.query!(conn, "SELECT pubname FROM pg_publication_tables", [])
    end
  end

  describe "startup replication catch-up" do
    setup :with_registry

    test "retries a pre-catch-up connection error without waiting for external active status",
         ctx do
      start_supervised!({Electric.StatusMonitor, stack_id: ctx.stack_id})

      event = make_ref()
      async_ref = make_ref()

      state =
        ReplicationClient.State.new(
          stack_id: ctx.stack_id,
          connection_manager: ctx.connection_manager,
          handle_event: {__MODULE__, :test_handle_event_async, [self()]},
          publication_name: "test_publication",
          try_creating_publication?: false,
          slot_name: "test_slot"
        )

      state = %{
        state
        | startup_wal_flush_lsn: 100,
          pending_event: {async_ref, event, 1_000, System.monotonic_time(:millisecond)}
      }

      assert {:noreply, retrying_state} =
               ReplicationClient.handle_info(
                 {async_ref, {:error, :connection_not_available}},
                 state
               )

      assert retrying_state.pending_event == nil
      assert retrying_state.event_retry_wait == nil
      assert :sys.get_state(Electric.StatusMonitor.name(ctx.stack_id)).waiters == MapSet.new()

      assert_receive {:retry_connection_event, ^event, deadline}, 200
      remaining = deadline - System.monotonic_time(:millisecond)
      assert remaining < 1_000
      assert remaining > 0
    end

    test "post-catch-up connection retries preserve one budget and leave no stale active waiter",
         ctx do
      start_supervised!({Electric.StatusMonitor, stack_id: ctx.stack_id})

      event = make_ref()
      first_ref = make_ref()

      state =
        ReplicationClient.State.new(
          stack_id: ctx.stack_id,
          connection_manager: ctx.connection_manager,
          handle_event: {__MODULE__, :test_handle_event_async, [self()]},
          publication_name: "test_publication",
          try_creating_publication?: false,
          slot_name: "test_slot"
        )

      state = %{
        state
        | replication_caught_up?: true,
          pending_event: {first_ref, event, 1_000, System.monotonic_time(:millisecond)}
      }

      assert {:noreply, first_retry_state} =
               ReplicationClient.handle_info(
                 {first_ref, {:error, :connection_not_available}},
                 state
               )

      assert_receive {:retry_connection_event, ^event, first_deadline}, 200
      assert first_retry_state.connection_retry_deadline == first_deadline
      first_remaining = first_deadline - System.monotonic_time(:millisecond)
      assert first_remaining < 1_000
      assert first_remaining > 0
      assert :sys.get_state(Electric.StatusMonitor.name(ctx.stack_id)).waiters == MapSet.new()

      second_ref = make_ref()

      second_attempt = %{
        first_retry_state
        | pending_event: {second_ref, event, first_remaining, System.monotonic_time(:millisecond)}
      }

      assert {:noreply, second_retry_state} =
               ReplicationClient.handle_info(
                 {second_ref, {:error, :connection_not_available}},
                 second_attempt
               )

      assert second_retry_state.event_retry_wait == nil
      assert_receive {:retry_connection_event, ^event, second_deadline}, 200
      assert second_retry_state.connection_retry_deadline == second_deadline
      second_remaining = second_deadline - System.monotonic_time(:millisecond)
      assert second_remaining < first_remaining
      assert second_deadline == first_deadline
      assert :sys.get_state(Electric.StatusMonitor.name(ctx.stack_id)).waiters == MapSet.new()
    end

    test "disconnects when a pre-catch-up connection retry exhausts its delivery budget", ctx do
      event = make_ref()
      async_ref = make_ref()

      state =
        ReplicationClient.State.new(
          stack_id: ctx.stack_id,
          connection_manager: ctx.connection_manager,
          handle_event: {__MODULE__, :test_handle_event_async, [self()]},
          publication_name: "test_publication",
          try_creating_publication?: false,
          slot_name: "test_slot"
        )

      state = %{
        state
        | startup_wal_flush_lsn: 100,
          pending_event: {async_ref, event, 1, System.monotonic_time(:millisecond) - 10}
      }

      assert {:disconnect, {:event_delivery_retry_budget_exhausted, :connection_not_available}} =
               ReplicationClient.handle_info(
                 {async_ref, {:error, :connection_not_available}},
                 state
               )

      refute_receive {:process_event, ^event, _}, 100
      refute_receive {:retry_connection_event, ^event, _}, 0
    end

    test "disconnects a scheduled connection retry once its absolute deadline expires", ctx do
      event = make_ref()
      deadline = System.monotonic_time(:millisecond) - 1

      state =
        ReplicationClient.State.new(
          stack_id: ctx.stack_id,
          connection_manager: ctx.connection_manager,
          handle_event: {__MODULE__, :test_handle_event_async, [self()]},
          publication_name: "test_publication",
          try_creating_publication?: false,
          slot_name: "test_slot"
        )

      state = %{state | connection_retry_deadline: deadline}

      assert {:disconnect, {:event_delivery_retry_budget_exhausted, :connection_not_available}} =
               ReplicationClient.handle_info(
                 {:retry_connection_event, event, deadline},
                 state
               )
    end

    test "can reach the startup causal frontier after a bounded connection retry", ctx do
      target_lsn = Lsn.from_integer(100)
      now = System.monotonic_time()

      event = %TransactionFragment{
        lsn: target_lsn,
        commit: %Electric.Replication.Changes.Commit{
          commit_timestamp: DateTime.utc_now(),
          received_at: now,
          initial_receive_lag: 0,
          tx_started_at: now
        }
      }

      async_ref = make_ref()

      state =
        ReplicationClient.State.new(
          stack_id: ctx.stack_id,
          connection_manager: ctx.connection_manager,
          handle_event: {__MODULE__, :test_handle_event_async, [self()]},
          publication_name: "test_publication",
          try_creating_publication?: false,
          slot_name: "test_slot"
        )

      state = %{
        state
        | startup_wal_flush_lsn: Lsn.to_integer(target_lsn),
          pending_event: {async_ref, event, 1_000, System.monotonic_time(:millisecond)}
      }

      assert {:noreply, retrying_state} =
               ReplicationClient.handle_info(
                 {async_ref, {:error, :connection_not_available}},
                 state
               )

      assert_receive {:retry_connection_event, ^event, deadline}, 200
      remaining = deadline - System.monotonic_time(:millisecond)
      assert remaining > 0

      success_ref = make_ref()

      success_state = %{
        retrying_state
        | pending_event: {success_ref, event, remaining, System.monotonic_time(:millisecond)}
      }

      assert {:noreply_and_resume, _acks, processed_state} =
               ReplicationClient.handle_info({success_ref, :ok}, success_state)

      refute processed_state.replication_caught_up?

      draining_state =
        complete_causal_marker_transaction(
          processed_state,
          Lsn.to_integer(target_lsn),
          :flush_before_ack
        )

      caught_up_state = finish_causal_catch_up(draining_state)
      assert caught_up_state.replication_caught_up?
      assert_receive {:replication_caught_up, replication_client_pid}
      assert replication_client_pid == self()
    end

    test "waits for the durable boundary after processing the startup target", ctx do
      ref = make_ref()
      target_lsn = Lsn.from_integer(100)
      now = System.monotonic_time()

      event = %TransactionFragment{
        lsn: target_lsn,
        commit: %Electric.Replication.Changes.Commit{
          commit_timestamp: DateTime.utc_now(),
          received_at: now,
          initial_receive_lag: 0,
          tx_started_at: now
        }
      }

      state =
        ReplicationClient.State.new(
          stack_id: ctx.stack_id,
          connection_manager: ctx.connection_manager,
          handle_event: {__MODULE__, :test_handle_event_async, [self()]},
          publication_name: "test_publication",
          try_creating_publication?: false,
          slot_name: "test_slot"
        )

      state = %{
        state
        | startup_wal_flush_lsn: Lsn.to_integer(target_lsn),
          pending_event: {ref, event, 1_000, System.monotonic_time(:millisecond)}
      }

      assert {:noreply_and_resume, _acks, processed_state} =
               ReplicationClient.handle_info({ref, :ok}, state)

      refute processed_state.replication_caught_up?
      refute processed_state.flush_up_to_date?
      assert processed_state.received_wal == Lsn.to_integer(target_lsn)
      assert processed_state.flushed_wal < Lsn.to_integer(target_lsn)
      refute_receive {:replication_caught_up, _}, 100

      keepalive_wal = Lsn.to_integer(target_lsn) + 1
      keepalive = <<?k, keepalive_wal::64, 0::64, 0::8>>

      assert {:noreply, [], keepalive_state} =
               ReplicationClient.handle_data(keepalive, processed_state)

      refute keepalive_state.replication_caught_up?
      assert keepalive_state.received_wal == Lsn.to_integer(target_lsn)
      assert keepalive_state.flushed_wal < Lsn.to_integer(target_lsn)
      refute_receive {:replication_caught_up, _}, 100

      {marker_event, dispatched_state, commit_lsn} =
        dispatch_causal_marker_transaction(
          keepalive_state,
          Lsn.to_integer(target_lsn)
        )

      ref = make_ref()

      dispatched_state = %{
        dispatched_state
        | pending_event: {ref, marker_event, 1_000, System.monotonic_time(:millisecond)}
      }

      assert {:noreply_and_resume, _acks, marked_state} =
               ReplicationClient.handle_info({ref, :ok}, dispatched_state)

      refute marked_state.replication_caught_up?
      assert marked_state.last_processed_causal_marker_lsn == Lsn.to_integer(target_lsn)
      assert marked_state.causal_catch_up_task == nil

      assert {:noreply, [_status_update], draining_state} =
               ReplicationClient.handle_info(
                 {:flush_boundary_updated, commit_lsn},
                 marked_state
               )

      refute draining_state.replication_caught_up?
      assert draining_state.causal_catch_up_task != nil
      caught_up_state = finish_causal_catch_up(draining_state)

      assert caught_up_state.replication_caught_up?
      assert caught_up_state.flush_up_to_date?
      assert caught_up_state.flushed_wal == Lsn.to_integer(target_lsn)
      assert_receive {:replication_caught_up, replication_client_pid}
      assert replication_client_pid == self()
    end

    test "does not signal readiness for a committed transaction below the startup target", ctx do
      ref = make_ref()
      observed_lsn = Lsn.from_integer(99)
      now = System.monotonic_time()

      event = %TransactionFragment{
        lsn: observed_lsn,
        commit: %Electric.Replication.Changes.Commit{
          commit_timestamp: DateTime.utc_now(),
          received_at: now,
          initial_receive_lag: 0,
          tx_started_at: now
        }
      }

      state =
        ReplicationClient.State.new(
          stack_id: ctx.stack_id,
          connection_manager: ctx.connection_manager,
          handle_event: {__MODULE__, :test_handle_event_async, [self()]},
          publication_name: "test_publication",
          try_creating_publication?: false,
          slot_name: "test_slot"
        )

      state = %{
        state
        | startup_wal_flush_lsn: 100,
          pending_event: {ref, event, 1_000, System.monotonic_time(:millisecond)}
      }

      assert {:noreply_and_resume, _acks, not_caught_up_state} =
               ReplicationClient.handle_info({ref, :ok}, state)

      refute not_caught_up_state.replication_caught_up?
      refute_receive {:replication_caught_up, _}, 100
    end

    test "does not signal readiness for an idle keepalive below the startup target", ctx do
      target_wal = lsn_to_wal("0/10")
      observed_wal = lsn_to_wal("0/F")

      state =
        ReplicationClient.State.new(
          stack_id: ctx.stack_id,
          handle_event: nil,
          publication_name: "",
          try_creating_publication?: false,
          slot_name: "",
          connection_manager: ctx.connection_manager
        )

      state = %{state | startup_wal_flush_lsn: target_wal}
      keepalive = <<?k, observed_wal::64, 0::64, 0::8>>

      assert {:noreply, [], not_caught_up_state} =
               ReplicationClient.handle_data(keepalive, state)

      refute not_caught_up_state.replication_caught_up?
      refute_receive {:replication_caught_up, _}, 100
    end

    test "does not signal readiness for a keepalive while a transaction is in progress", ctx do
      target_wal = lsn_to_wal("0/10")

      state =
        ReplicationClient.State.new(
          stack_id: ctx.stack_id,
          handle_event: nil,
          publication_name: "",
          try_creating_publication?: false,
          slot_name: "",
          connection_manager: ctx.connection_manager
        )

      converter = %{state.message_converter | txn_fragment: %TransactionFragment{}}

      state = %{
        state
        | startup_wal_flush_lsn: target_wal,
          message_converter: converter
      }

      keepalive = <<?k, target_wal::64, 0::64, 0::8>>

      assert {:noreply, [], not_caught_up_state} =
               ReplicationClient.handle_data(keepalive, state)

      refute not_caught_up_state.replication_caught_up?
      refute_receive {:replication_caught_up, _}, 100
    end

    test "a keepalive ahead of queued work neither broadcasts nor authorizes causal progress",
         ctx do
      target_wal = lsn_to_wal("0/10")
      keepalive_wal = lsn_to_wal("0/20")

      :ok = LsnTracker.initialize(ctx.stack_id)
      assert {:ok, _} = LsnTracker.subscribe_to_global_lsn_updates(ctx.stack_id)

      state =
        ReplicationClient.State.new(
          stack_id: ctx.stack_id,
          handle_event: nil,
          publication_name: "",
          try_creating_publication?: false,
          slot_name: "",
          connection_manager: ctx.connection_manager
        )

      state = %{state | startup_wal_flush_lsn: target_wal}
      keepalive = <<?k, keepalive_wal::64, 0::64, 0::8>>

      assert {:noreply, [], keepalive_state} =
               ReplicationClient.handle_data(keepalive, state)

      refute keepalive_state.replication_caught_up?
      assert keepalive_state.received_wal == 0
      assert keepalive_state.flushed_wal == 0
      assert keepalive_state.last_processed_causal_marker_lsn == 0
      assert keepalive_state.causal_catch_up_task == nil
      refute_receive {:global_last_seen_lsn, _}, 100
      refute_receive {:replication_caught_up, _}, 100

      {marker_event, dispatched_state, commit_lsn} =
        dispatch_causal_marker_transaction(keepalive_state, target_wal)

      assert dispatched_state.received_wal == 0
      assert dispatched_state.flushed_wal == 0
      assert dispatched_state.last_processed_causal_marker_lsn == 0
      refute_receive {:global_last_seen_lsn, _}, 0

      draining_state =
        acknowledge_causal_marker_transaction(
          dispatched_state,
          marker_event,
          commit_lsn,
          :flush_before_ack
        )

      assert draining_state.last_processed_causal_marker_lsn == target_wal

      caught_up_state = finish_causal_catch_up(draining_state)

      assert caught_up_state.replication_caught_up?
      assert_receive {:replication_caught_up, replication_client_pid}
      assert replication_client_pid == self()

      duplicate_state = complete_causal_marker_transaction(caught_up_state, target_wal)

      assert duplicate_state.replication_caught_up?
      refute_receive {:replication_caught_up, _}, 100
    end

    test "foreign and invalid logical messages are inert and never log their content", ctx do
      target_wal = lsn_to_wal("0/10")
      secret = "secret-canary-#{System.unique_integer([:positive])}"

      :ok = LsnTracker.initialize(ctx.stack_id)
      assert {:ok, _} = LsnTracker.subscribe_to_global_lsn_updates(ctx.stack_id)

      state =
        ReplicationClient.State.new(
          stack_id: ctx.stack_id,
          handle_event: nil,
          publication_name: "",
          try_creating_publication?: false,
          slot_name: "",
          connection_manager: ctx.connection_manager
        )

      converter = %{state.message_converter | txn_fragment: %TransactionFragment{xid: 42}}
      state = %{state | startup_wal_flush_lsn: target_wal, message_converter: converter}

      log =
        capture_log(fn ->
          foreign = logical_message_data(target_wal, "customer-prefix", secret)

          assert {:noreply, foreign_state} =
                   ReplicationClient.handle_data(foreign, state)

          assert foreign_state.message_converter == converter
          assert foreign_state.last_processed_causal_marker_lsn == 0

          nonempty_marker =
            logical_message_data(target_wal, CausalMarker.prefix(), secret)

          assert {:noreply, invalid_state} =
                   ReplicationClient.handle_data(nonempty_marker, foreign_state)

          assert invalid_state.message_converter == converter
          assert invalid_state.last_processed_causal_marker_lsn == 0

          oversized =
            logical_message_data(
              target_wal,
              "customer-prefix",
              secret <> String.duplicate("x", 10_000)
            )

          assert {:noreply, oversized_state} =
                   ReplicationClient.handle_data(oversized, invalid_state)

          assert oversized_state.message_converter == converter
          assert oversized_state.last_processed_causal_marker_lsn == 0

          malformed =
            logical_message_data(target_wal, CausalMarker.prefix(), <<>>, declared_size: 1)

          assert {:noreply, malformed_state} =
                   ReplicationClient.handle_data(malformed, oversized_state)

          assert malformed_state.message_converter == converter
          assert malformed_state.last_processed_causal_marker_lsn == 0
        end)

      refute log =~ secret
      refute_receive {:global_last_seen_lsn, _}, 100
      refute_receive {:replication_caught_up, _}, 100
    end

    test "a non-startup marker stays inert while its empty transaction dispatches normally",
         ctx do
      target_wal = lsn_to_wal("0/10")
      marker_wal = lsn_to_wal("0/20")
      commit_end_wal = marker_wal + 1
      xid = 73

      state =
        ReplicationClient.State.new(
          stack_id: ctx.stack_id,
          handle_event: {__MODULE__, :test_handle_event_async_including_empty, [self()]},
          publication_name: "",
          try_creating_publication?: false,
          slot_name: "",
          connection_manager: ctx.connection_manager
        )

      state = %{state | startup_wal_flush_lsn: target_wal}
      begin_message = <<?B, marker_wal::64, 0::64, xid::32>>

      assert {:noreply, begun_state} =
               ReplicationClient.handle_data(xlog_data(begin_message, marker_wal), state)

      assert {:noreply, marked_state} =
               ReplicationClient.handle_data(causal_marker_data(marker_wal), begun_state)

      assert marked_state.pending_causal_marker_lsn == nil
      assert marked_state.pending_causal_marker_xid == nil

      commit_message = <<?C, 0, marker_wal::64, commit_end_wal::64, 0::64>>

      assert {:noreply_and_pause, [], dispatched_state} =
               ReplicationClient.handle_data(
                 xlog_data(commit_message, commit_end_wal),
                 marked_state
               )

      assert_receive {:process_event, event, retry_budget}, 200

      assert %TransactionFragment{
               xid: ^xid,
               changes: [],
               commit: %Electric.Replication.Changes.Commit{}
             } = event

      assert dispatched_state.event_causal_marker_lsn == nil
      assert dispatched_state.event_causal_marker_xid == nil

      assert {:noreply, applying_state} =
               ReplicationClient.handle_info(
                 {:process_event, event, retry_budget},
                 dispatched_state
               )

      assert_receive {:from_replication, ^event}, 200
      assert {ref, ^event, _, _} = applying_state.pending_event
      assert_receive {^ref, :ok}, 200

      assert {:noreply_and_resume, _acks, acknowledged_state} =
               ReplicationClient.handle_info({ref, :ok}, applying_state)

      assert acknowledged_state.last_processed_causal_marker_lsn == 0
      assert acknowledged_state.causal_catch_up_task == nil
    end

    test "binds only the exact target marker to its commit and preserves it across retry", ctx do
      marker_wal = lsn_to_wal("0/10")
      transaction_final_wal = lsn_to_wal("0/20")
      xid = 77
      commit_end_lsn = transaction_final_wal + 1
      test_pid = self()
      consumer_pid = spawn(fn -> causal_waiting_consumer(test_pid) end)

      on_exit(fn ->
        if Process.alive?(consumer_pid), do: Process.exit(consumer_pid, :kill)
      end)

      :ok = ConsumerRegistry.register_consumer(consumer_pid, "final-lsn-consumer", ctx.stack_id)

      state =
        ReplicationClient.State.new(
          stack_id: ctx.stack_id,
          handle_event: nil,
          publication_name: "",
          try_creating_publication?: false,
          slot_name: "",
          connection_manager: ctx.connection_manager
        )

      state = %{state | startup_wal_flush_lsn: marker_wal}
      begin_message = <<?B, transaction_final_wal::64, 0::64, xid::32>>

      assert {:noreply, begun_state} =
               ReplicationClient.handle_data(xlog_data(begin_message, marker_wal), state)

      assert {:noreply, stale_marker_state} =
               ReplicationClient.handle_data(
                 causal_marker_data(marker_wal - 1),
                 begun_state
               )

      assert stale_marker_state.pending_causal_marker_lsn == nil
      assert stale_marker_state.last_processed_causal_marker_lsn == 0
      assert stale_marker_state.causal_catch_up_task == nil

      assert {:noreply, marked_state} =
               ReplicationClient.handle_data(
                 causal_marker_data(marker_wal),
                 stale_marker_state
               )

      assert marked_state.pending_causal_marker_lsn == marker_wal
      assert marked_state.pending_causal_marker_xid == xid
      assert marked_state.last_processed_causal_marker_lsn == 0
      assert marked_state.causal_catch_up_task == nil

      commit_message = <<?C, 0, transaction_final_wal::64, commit_end_lsn::64, 0::64>>

      assert {:noreply_and_pause, [], dispatched_state} =
               ReplicationClient.handle_data(
                 xlog_data(commit_message, commit_end_lsn),
                 marked_state
               )

      assert_receive {:process_event, event, retry_budget}, 200
      assert %TransactionFragment{xid: ^xid, lsn: event_lsn} = event
      assert Lsn.to_integer(event_lsn) == transaction_final_wal
      assert dispatched_state.startup_wal_flush_lsn == transaction_final_wal
      assert dispatched_state.pending_causal_marker_lsn == nil
      assert dispatched_state.event_causal_marker_lsn == transaction_final_wal
      assert dispatched_state.event_causal_marker_xid == xid
      assert dispatched_state.last_processed_causal_marker_lsn == 0

      first_ref = make_ref()

      dispatched_state = %{
        dispatched_state
        | pending_event: {first_ref, event, retry_budget, System.monotonic_time(:millisecond)}
      }

      assert {:noreply, [_status_update], flushed_state} =
               ReplicationClient.handle_info(
                 {:flush_boundary_updated, marker_wal},
                 dispatched_state
               )

      assert flushed_state.last_processed_causal_marker_lsn == 0
      assert flushed_state.causal_catch_up_task == nil

      assert {:noreply, retrying_state} =
               ReplicationClient.handle_info(
                 {first_ref, {:error, :connection_not_available}},
                 flushed_state
               )

      assert retrying_state.event_causal_marker_lsn == transaction_final_wal
      assert retrying_state.event_causal_marker_xid == xid
      assert retrying_state.last_processed_causal_marker_lsn == 0
      assert_receive {:retry_connection_event, ^event, _deadline}, 200

      success_ref = make_ref()

      success_state = %{
        retrying_state
        | pending_event: {success_ref, event, retry_budget, System.monotonic_time(:millisecond)}
      }

      assert {:noreply_and_resume, _acks, draining_state} =
               ReplicationClient.handle_info({success_ref, :ok}, success_state)

      assert draining_state.event_causal_marker_lsn == nil
      assert draining_state.event_causal_marker_xid == nil
      assert draining_state.last_processed_causal_marker_lsn == transaction_final_wal
      assert draining_state.causal_catch_up_task == nil

      assert {:noreply, [_status_update], draining_state} =
               ReplicationClient.handle_info(
                 {:flush_boundary_updated, transaction_final_wal},
                 draining_state
               )

      assert draining_state.causal_catch_up_task != nil
      assert_receive {:causal_frontier_waiting, ^consumer_pid, ^transaction_final_wal}
      send(consumer_pid, :release_causal_frontier)

      caught_up_state = finish_causal_catch_up(draining_state)
      assert caught_up_state.replication_caught_up?
      assert_receive {:replication_caught_up, replication_client_pid}
      assert replication_client_pid == self()
    end

    test "does not advertise readiness until the consumer causal frontier drains", ctx do
      test_pid = self()
      consumer_pid = spawn_link(fn -> causal_waiting_consumer(test_pid) end)
      :ok = ConsumerRegistry.register_consumer(consumer_pid, "blocked-consumer", ctx.stack_id)

      target_wal = lsn_to_wal("0/10")

      state =
        ReplicationClient.State.new(
          stack_id: ctx.stack_id,
          handle_event: nil,
          publication_name: "",
          try_creating_publication?: false,
          slot_name: "",
          connection_manager: ctx.connection_manager
        )

      state = %{state | startup_wal_flush_lsn: target_wal}

      draining_state = complete_causal_marker_transaction(state, target_wal)

      assert draining_state.causal_catch_up_task != nil
      refute draining_state.replication_caught_up?
      assert_receive {:causal_frontier_waiting, ^consumer_pid, ^target_wal}
      refute_receive {:replication_caught_up, _}, 100

      send(consumer_pid, :release_causal_frontier)
      caught_up_state = finish_causal_catch_up(draining_state)

      assert caught_up_state.replication_caught_up?
      assert_receive {:replication_caught_up, replication_client_pid}
      assert replication_client_pid == self()
    end

    test "drains a replacement consumer registered while the prior frontier call is blocked",
         ctx do
      test_pid = self()
      prior_consumer = spawn_link(fn -> causal_waiting_consumer(test_pid) end)
      replacement_consumer = spawn_link(fn -> causal_waiting_consumer(test_pid) end)
      shape_handle = "replaced-consumer"

      :ok = ConsumerRegistry.register_consumer(prior_consumer, shape_handle, ctx.stack_id)

      target_wal = lsn_to_wal("0/10")

      state =
        ReplicationClient.State.new(
          stack_id: ctx.stack_id,
          handle_event: nil,
          publication_name: "",
          try_creating_publication?: false,
          slot_name: "",
          connection_manager: ctx.connection_manager
        )

      state = %{state | startup_wal_flush_lsn: target_wal}

      draining_state = complete_causal_marker_transaction(state, target_wal)

      assert {_task_pid, task_ref, ^target_wal} = draining_state.causal_catch_up_task
      assert_receive {:causal_frontier_waiting, ^prior_consumer, ^target_wal}

      :ok = ConsumerRegistry.remove_consumer(shape_handle, ctx.stack_id)
      :ok = ConsumerRegistry.register_consumer(replacement_consumer, shape_handle, ctx.stack_id)
      send(prior_consumer, :release_causal_frontier)

      assert_receive {:causal_frontier_waiting, ^replacement_consumer, ^target_wal}, 500
      refute_receive {^task_ref, :ok}, 0
      refute_receive {:replication_caught_up, _}, 0

      send(replacement_consumer, :release_causal_frontier)
      caught_up_state = finish_causal_catch_up(draining_state)

      assert caught_up_state.replication_caught_up?
      assert_receive {:replication_caught_up, replication_client_pid}
      assert replication_client_pid == self()
    end

    test "retries a stable registry pass when nested causal reservations advance its generation",
         ctx do
      test_pid = self()
      level_a = spawn_link(fn -> causal_first_pass_gate(test_pid) end)
      level_b = spawn_link(fn -> causal_immediate_consumer(test_pid, :level_b) end)
      level_c = spawn_link(fn -> causal_installable_consumer(test_pid, false) end)

      :ok = ConsumerRegistry.register_consumer(level_a, "level-a", ctx.stack_id)
      :ok = ConsumerRegistry.register_consumer(level_b, "level-b", ctx.stack_id)
      :ok = ConsumerRegistry.register_consumer(level_c, "level-c", ctx.stack_id)

      target_wal = lsn_to_wal("0/10")

      state =
        ReplicationClient.State.new(
          stack_id: ctx.stack_id,
          handle_event: nil,
          publication_name: "",
          try_creating_publication?: false,
          slot_name: "",
          connection_manager: ctx.connection_manager
        )

      state = %{state | startup_wal_flush_lsn: target_wal}

      draining_state = complete_causal_marker_transaction(state, target_wal)

      assert {_task_pid, task_ref, ^target_wal} = draining_state.causal_catch_up_task
      assert_receive {:causal_first_pass_waiting, ^level_a, ^target_wal}
      assert_receive {:causal_frontier_pass, :level_b, ^target_wal}
      assert_receive {:causal_frontier_pass, ^level_c, ^target_wal}

      send(level_c, {:install_causal_reservation, self()})
      assert_receive {:causal_reservation_installed, ^level_c}

      # Model level A admitting work in B, which synchronously admits work in
      # C after C already returned from this registry pass. Registry membership
      # is unchanged; only the target-scoped generation can force a second pass.
      :ok = ConsumerRegistry.mark_causal_work_created(ctx.stack_id, target_wal)
      :ok = ConsumerRegistry.mark_causal_work_created(ctx.stack_id, target_wal)
      send(level_a, :release_first_causal_pass)

      assert_receive {:causal_frontier_waiting, ^level_c, ^target_wal}, 500
      refute_receive {^task_ref, :ok}, 0
      refute_receive {:replication_caught_up, _}, 0

      send(level_c, :release_causal_frontier)
      caught_up_state = finish_causal_catch_up(draining_state)

      assert caught_up_state.replication_caught_up?
      assert_receive {:replication_caught_up, replication_client_pid}
      assert replication_client_pid == self()
    end

    test "does not retry the causal drain for work newer than its startup target", ctx do
      test_pid = self()
      consumer_pid = spawn_link(fn -> causal_first_pass_gate(test_pid) end)
      :ok = ConsumerRegistry.register_consumer(consumer_pid, "target-scoped", ctx.stack_id)

      target_wal = lsn_to_wal("0/10")

      state =
        ReplicationClient.State.new(
          stack_id: ctx.stack_id,
          handle_event: nil,
          publication_name: "",
          try_creating_publication?: false,
          slot_name: "",
          connection_manager: ctx.connection_manager
        )

      state = %{state | startup_wal_flush_lsn: target_wal}
      draining_state = complete_causal_marker_transaction(state, target_wal)

      assert_receive {:causal_first_pass_waiting, ^consumer_pid, ^target_wal}

      for newer_offset <- (target_wal + 1)..(target_wal + 1_000) do
        assert :ok =
                 ConsumerRegistry.mark_causal_work_created(ctx.stack_id, newer_offset)
      end

      send(consumer_pid, :release_first_causal_pass)
      caught_up_state = finish_causal_catch_up(draining_state)

      assert caught_up_state.replication_caught_up?
      refute_receive {:causal_frontier_pass, ^consumer_pid, ^target_wal}, 0
      assert_receive {:replication_caught_up, replication_client_pid}
      assert replication_client_pid == self()
    end

    test "disconnects with an attributable error when the causal frontier deadline expires",
         ctx do
      timeout_ms = 100
      Electric.StackConfig.put(ctx.stack_id, :causal_drain_timeout_ms, timeout_ms)

      test_pid = self()
      consumer_pid = spawn_link(fn -> causal_waiting_consumer(test_pid) end)
      :ok = ConsumerRegistry.register_consumer(consumer_pid, "blocked-consumer", ctx.stack_id)

      target_wal = lsn_to_wal("0/10")

      state =
        ReplicationClient.State.new(
          stack_id: ctx.stack_id,
          handle_event: nil,
          publication_name: "",
          try_creating_publication?: false,
          slot_name: "",
          connection_manager: ctx.connection_manager
        )

      state = %{state | startup_wal_flush_lsn: target_wal}

      draining_state = complete_causal_marker_transaction(state, target_wal)

      assert {task_pid, task_ref, ^target_wal} = draining_state.causal_catch_up_task
      assert_receive {:causal_frontier_waiting, ^consumer_pid, ^target_wal}

      timeout_reason =
        {:causal_frontier_timeout, ctx.stack_id, target_wal, timeout_ms}

      assert_receive {^task_ref, {:error, ^timeout_reason}}, 500
      assert_receive {:causal_frontier_waiter_down, ^consumer_pid}, 500
      task_monitor = Process.monitor(task_pid)
      assert_receive {:DOWN, ^task_monitor, :process, ^task_pid, _reason}, 500

      log =
        capture_log(fn ->
          assert {:disconnect, {:causal_catch_up_failed, ^timeout_reason}} =
                   ReplicationClient.handle_info(
                     {task_ref, {:error, timeout_reason}},
                     draining_state
                   )
        end)

      assert log =~ "Causal startup frontier timed out"
      assert log =~ ctx.stack_id
      assert log =~ "#{timeout_ms}ms"
    end

    test "tears down the nested causal worker and its waiter with the owning task", ctx do
      test_pid = self()
      consumer_pid = spawn_link(fn -> causal_waiting_consumer(test_pid) end)
      :ok = ConsumerRegistry.register_consumer(consumer_pid, "blocked-consumer", ctx.stack_id)

      target_wal = lsn_to_wal("0/10")

      state =
        ReplicationClient.State.new(
          stack_id: ctx.stack_id,
          handle_event: nil,
          publication_name: "",
          try_creating_publication?: false,
          slot_name: "",
          connection_manager: ctx.connection_manager
        )

      state = %{state | startup_wal_flush_lsn: target_wal}

      draining_state = complete_causal_marker_transaction(state, target_wal)

      assert {owner_task, owner_ref, ^target_wal} = draining_state.causal_catch_up_task
      assert_receive {:causal_frontier_waiting, ^consumer_pid, ^target_wal}

      {:monitors, monitors} = Process.info(owner_task, :monitors)

      nested_worker =
        monitors
        |> Enum.flat_map(fn
          {:process, pid} when pid != test_pid -> [pid]
          _ -> []
        end)
        |> List.first()

      assert is_pid(nested_worker)
      nested_worker_ref = Process.monitor(nested_worker)

      Process.exit(owner_task, :kill)

      assert_receive {:DOWN, ^owner_ref, :process, ^owner_task, :killed}
      assert_receive {:DOWN, ^nested_worker_ref, :process, ^nested_worker, _reason}, 500
      assert_receive {:causal_frontier_waiter_down, ^consumer_pid}, 500
    end

    test "tears down the causal task, nested worker, and waiter when ReplicationClient dies",
         ctx do
      test_pid = self()
      consumer_pid = spawn_link(fn -> causal_waiting_consumer(test_pid) end)
      :ok = ConsumerRegistry.register_consumer(consumer_pid, "blocked-consumer", ctx.stack_id)

      target_wal = lsn_to_wal("0/10")

      owner_pid =
        spawn(fn ->
          state =
            ReplicationClient.State.new(
              stack_id: ctx.stack_id,
              handle_event: nil,
              publication_name: "",
              try_creating_publication?: false,
              slot_name: "",
              connection_manager: ctx.connection_manager
            )

          state = %{state | startup_wal_flush_lsn: target_wal}

          draining_state = complete_causal_marker_transaction(state, target_wal)

          send(test_pid, {:causal_catch_up_owner_started, self(), draining_state})
          Process.sleep(:infinity)
        end)

      owner_ref = Process.monitor(owner_pid)

      assert_receive {:causal_catch_up_owner_started, ^owner_pid, draining_state}, 500
      assert {task_pid, _task_ref, ^target_wal} = draining_state.causal_catch_up_task
      assert_receive {:causal_frontier_waiting, ^consumer_pid, ^target_wal}, 500
      task_monitor = Process.monitor(task_pid)

      {:monitors, monitors} = Process.info(task_pid, :monitors)

      nested_worker =
        Enum.find_value(monitors, fn
          {:process, pid} when pid != owner_pid -> pid
          _ -> nil
        end)

      assert is_pid(nested_worker)
      nested_worker_ref = Process.monitor(nested_worker)

      Process.exit(owner_pid, :kill)

      assert_receive {:DOWN, ^owner_ref, :process, ^owner_pid, :killed}, 500
      assert_receive {:DOWN, ^task_monitor, :process, ^task_pid, :normal}, 500
      assert_receive {:DOWN, ^nested_worker_ref, :process, ^nested_worker, _reason}, 500
      assert_receive {:causal_frontier_waiter_down, ^consumer_pid}, 500
    end

    test "fails closed when the consumer registry disappears during causal catch-up", ctx do
      :ets.delete(ctx.consumer_registry_table)

      target_wal = lsn_to_wal("0/10")

      state =
        ReplicationClient.State.new(
          stack_id: ctx.stack_id,
          handle_event: nil,
          publication_name: "",
          try_creating_publication?: false,
          slot_name: "",
          connection_manager: ctx.connection_manager
        )

      state = %{state | startup_wal_flush_lsn: target_wal}

      draining_state = complete_causal_marker_transaction(state, target_wal)

      assert {task_pid, task_ref, ^target_wal} = draining_state.causal_catch_up_task

      assert_receive {:DOWN, ^task_ref, :process, ^task_pid, reason}, 500
      refute_receive {^task_ref, :ok}, 0
      refute_receive {:replication_caught_up, _}, 0

      assert {:disconnect, {:causal_catch_up_failed, ^reason}} =
               ReplicationClient.handle_info(
                 {:DOWN, task_ref, :process, task_pid, reason},
                 draining_state
               )
    end

    test "bounds causal frontier fan-out with a validated per-stack override", ctx do
      assert ReplicationClient.causal_drain_max_concurrency(ctx.stack_id, 0) == 1
      assert ReplicationClient.causal_drain_max_concurrency(ctx.stack_id, 10) == 10
      assert ReplicationClient.causal_drain_max_concurrency(ctx.stack_id, 100_000) == 32

      Electric.StackConfig.put(ctx.stack_id, :causal_drain_max_concurrency, 2)
      assert ReplicationClient.causal_drain_max_concurrency(ctx.stack_id, 100_000) == 2

      Electric.StackConfig.put(ctx.stack_id, :causal_drain_max_concurrency, 0)

      assert_raise ArgumentError, ~r/must be a positive integer/, fn ->
        ReplicationClient.causal_drain_max_concurrency(ctx.stack_id, 100_000)
      end
    end

    test "uses a validated per-stack causal frontier timeout", ctx do
      assert ReplicationClient.causal_drain_timeout_ms(ctx.stack_id) == :timer.minutes(10)

      Electric.StackConfig.put(ctx.stack_id, :causal_drain_timeout_ms, 123)
      assert ReplicationClient.causal_drain_timeout_ms(ctx.stack_id) == 123

      Electric.StackConfig.put(ctx.stack_id, :causal_drain_timeout_ms, 0)

      assert_raise ArgumentError, ~r/must be a positive integer/, fn ->
        ReplicationClient.causal_drain_timeout_ms(ctx.stack_id)
      end
    end
  end

  describe "ReplicationClient against real db" do
    setup [
      :with_unique_db,
      :with_basic_tables,
      :with_publication,
      :with_replication_opts,
      :with_status_monitor,
      :with_lsn_tracker
    ]

    test "calls a provided function when receiving it from the PG", %{db_conn: conn} = ctx do
      start_client(ctx)

      insert_item(conn, "test value")

      assert %NewRecord{record: %{"value" => "test value"}} = receive_tx_change()
    end

    test "logs a message when connected & replication has started", %{db_conn: conn} = ctx do
      log =
        ExUnit.CaptureLog.capture_log(fn ->
          start_client(ctx)

          insert_item(conn, "test value")

          assert %NewRecord{record: %{"value" => "test value"}} = receive_tx_change()
        end)

      log =~ "Started replication from postgres"
    end

    test "works with an existing publication", %{replication_opts: replication_opts} = ctx do
      replication_opts = Keyword.put(replication_opts, :try_creating_publication?, true)
      start_client(ctx, replication_opts: replication_opts)
    end

    test "works with an existing replication slot",
         %{db_conn: conn, slot_name: slot_name} = ctx do
      pid = start_client(ctx)
      assert_receive {:replication_caught_up, ^pid}, @assert_receive_db_timeout

      assert %{
               "slot_name" => ^slot_name,
               "temporary" => false,
               "confirmed_flush_lsn" => flush_lsn
             } = fetch_slot_info(conn, slot_name)

      # Check that the slot remains even when the replication client goes down
      true = Process.unlink(pid)
      true = Process.exit(pid, :kill)

      assert %{"slot_name" => ^slot_name, "confirmed_flush_lsn" => ^flush_lsn} =
               fetch_slot_info(conn, slot_name)

      # Check that the replication client works when the replication slot already exists
      restarted_pid = start_client(ctx)
      assert_receive {:replication_caught_up, ^restarted_pid}, @assert_receive_db_timeout

      assert %{"slot_name" => ^slot_name, "confirmed_flush_lsn" => restarted_flush_lsn} =
               fetch_slot_info(conn, slot_name)

      assert Lsn.compare(flush_lsn, restarted_flush_lsn) == :lt
    end

    test "can replay already seen transaction", %{db_conn: conn} = ctx do
      pid = start_client(ctx)

      insert_item(conn, "test value")

      assert %NewRecord{
               record: %{"value" => "test value"},
               log_offset: %LogOffset{tx_offset: tx_lsn}
             } = receive_tx_change()

      insert_item(conn, "another value")
      assert %NewRecord{record: %{"value" => "another value"}} = receive_tx_change()

      # Verify that raising in the transaction callback crashes the connection process
      monitor = Process.monitor(pid)
      Process.unlink(pid)

      on_exit(fn -> Process.alive?(pid) && Process.exit(pid, :kill) end)

      send(pid, {:flush_boundary_updated, tx_lsn})

      interrupt_val = "interrupt #{inspect(pid)}"
      insert_item(conn, interrupt_val)
      refute_receive {:from_replication, _}, 50
      Process.exit(pid, :some_reason)

      assert_receive {:DOWN, ^monitor, :process, ^pid, :some_reason}, @assert_receive_db_timeout

      assert_received :lock_acquired
      assert_receive {:replication_caught_up, ^pid}, @assert_receive_db_timeout
      refute_received _

      # Now, when we restart the connection process, it replays transactions from the last
      # confirmed one
      restarted_pid = start_client(ctx)

      assert %NewRecord{record: %{"value" => "another value"}} = receive_tx_change()

      assert {replayed_lsn, %NewRecord{record: %{"value" => ^interrupt_val}}} =
               receive_tx_change_with_lsn()

      # In production the shape log collector advances this boundary after the
      # replayed transaction is durable. This direct ReplicationClient test uses
      # a message-only callback, so model that durability acknowledgement here.
      send(restarted_pid, {:flush_boundary_updated, Lsn.to_integer(replayed_lsn)})

      assert_received :lock_acquired
      assert_receive {:replication_caught_up, ^restarted_pid}, @assert_receive_db_timeout
      refute_receive _
    end

    @tag handle_event: {MockTransactionProcessor, :handle_event_async, []}
    test "holds processing of transaction until ready", %{db_conn: conn} = ctx do
      client_pid = start_client(ctx)

      # should not process the transaction but also should not die
      insert_item(conn, "test value 1")
      refute_receive {:from_replication, _}, 50
      assert Process.alive?(client_pid)

      processor_pid =
        start_supervised!({MockTransactionProcessor, self()}, id: make_ref())

      # once we start streaming we should see it processed
      assert %NewRecord{record: %{"value" => "test value 1"}} = receive_tx_change()

      # should have same behaviour mid-processing
      processor_ref = Process.monitor(processor_pid)
      MockTransactionProcessor.toggle_crash(true)
      insert_item(conn, "test value 2")

      assert_receive {:DOWN, ^processor_ref, :process, ^processor_pid, _reason},
                     @assert_receive_db_timeout

      refute_receive {:from_replication, _}, 50
      assert Process.alive?(client_pid)

      start_supervised!({MockTransactionProcessor, self()}, id: make_ref())
      assert %NewRecord{record: %{"value" => "test value 2"}} = receive_tx_change()
    end

    @tag database_settings: ["wal_sender_timeout='3s'"]
    @tag handle_event: {MockTransactionProcessor, :handle_event_async, []}
    test "connection survives wal_sender_timeout when event handler is unavailable",
         %{db_conn: conn} = ctx do
      client_pid = start_client(ctx)
      ref = Process.monitor(client_pid)

      # Insert data without MockTransactionProcessor started — events crash
      # with :noproc, triggering the retry loop. The gen_statem is free between
      # retries, allowing the keepalive timer to send StandbyStatusUpdate messages.
      insert_item(conn, "test value 1")

      # Wait ~2x wal_sender_timeout (3s). Without the keepalive fix, PG kills
      # the connection and the process crashes with "tcp send: closed".
      # The refute_receive is both the wait AND the assertion: if the process
      # dies during this window, the test fails immediately.
      refute_receive {:DOWN, ^ref, :process, ^client_pid, _}, 6_000

      # Start the handler — pending event should be retried and succeed.
      start_supervised({MockTransactionProcessor, self()})
      assert %NewRecord{record: %{"value" => "test value 1"}} = receive_tx_change()

      # Insert more data that requires a LIVE connection to receive new WAL.
      insert_item(conn, "test value 2")
      assert %NewRecord{record: %{"value" => "test value 2"}} = receive_tx_change()
    end

    @tag database_settings: ["wal_sender_timeout='3s'"]
    @tag handle_event: {MockTransactionProcessor, :handle_event_async, []}
    test "connection survives wal_sender_timeout when event handler is slow",
         %{db_conn: conn} = ctx do
      start_supervised({MockTransactionProcessor, self()})

      client_pid = start_client(ctx)
      ref = Process.monitor(client_pid)
      assert_receive {:replication_caught_up, ^client_pid}, @assert_receive_db_timeout
      MockTransactionProcessor.set_delay(6_000)

      # Insert data — the event handler will take 6s to respond (2x wal_sender_timeout).
      # Because apply_event dispatches via $gen_call, the gen_statem is free to send
      # keepalives. Without the async fix, the gen_statem would be blocked for 6s and
      # PG would kill the connection after 3s.
      insert_item(conn, "slow value")

      # The process must survive the full processing window.
      refute_receive {:DOWN, ^ref, :process, ^client_pid, _}, 8_000

      # The slow event should have been processed.
      assert %NewRecord{record: %{"value" => "slow value"}} = receive_tx_change()

      # Verify the connection is still live by sending more data with no delay.
      MockTransactionProcessor.set_delay(0)
      insert_item(conn, "fast value")
      assert %NewRecord{record: %{"value" => "fast value"}} = receive_tx_change()
    end

    @tag handle_event: {MockTransactionProcessor, :handle_event_async, []}
    test "aborts held processing of transaction on exit", %{db_conn: conn} = ctx do
      client_pid = start_client(ctx)
      insert_item(conn, "test value 1")
      refute_receive {:from_replication, _}, 50
      ref = Process.monitor(client_pid)
      Process.unlink(client_pid)
      Process.exit(client_pid, :shutdown)
      assert_receive {:DOWN, ^ref, :process, ^client_pid, :shutdown}, 500
    end

    # Regression test for https://github.com/electric-sql/electric/issues/1548
    test "fares well when multiple concurrent transactions are writing to WAL",
         %{db_conn: conn} = ctx do
      client_pid = start_client(ctx)

      num_txn = 2
      num_ops = 8
      max_sleep = 20

      # Insert `num_txn` transactions, each in a separate process. Every transaction has
      # `num_ops` INSERTs with a random delay between each operation.
      # The end result is that INSERTs from different transactions get interleaved in
      # the WAL, challenging any assumptions in ReplicationClient about cross-transaction operation
      # ordering.
      Enum.each(1..num_txn, fn i ->
        tx_fun = fn conn ->
          pid_str = inspect(self())

          Enum.each(1..num_ops, fn j ->
            insert_item(conn, "#{i}-#{j} in process #{pid_str}")
            Process.sleep(:rand.uniform(max_sleep))
          end)
        end

        spawn_link(Postgrex, :transaction, [conn, tx_fun])
      end)

      # Receive every transaction sent by ReplicationClient to the test process.
      set =
        Enum.reduce(1..num_txn, MapSet.new(1..num_txn), fn _, set ->
          {_lsn, records} = receive_transaction()
          assert num_ops == length(records)

          [%NewRecord{record: %{"value" => val}} | _] = records
          {i, _} = Integer.parse(val)

          MapSet.delete(set, i)
        end)

      # Make sure there are no extraneous messages left.
      assert MapSet.size(set) == 0
      assert_received :lock_acquired
      assert_receive {:replication_caught_up, ^client_pid}, @assert_receive_db_timeout
      refute_receive _
    end

    # Set the DB's display settings to something else than Electric.Postgres.display_settings
    @tag database_settings: [
           "DateStyle='Postgres, DMY'",
           "TimeZone='CET'",
           "extra_float_digits=-1",
           "bytea_output='escape'",
           "IntervalStyle='postgres'"
         ]
    @tag additional_fields:
           "date DATE, timestamptz TIMESTAMPTZ, float FLOAT8, bytea BYTEA, interval INTERVAL"
    test "returns data formatted according to display settings", %{db_conn: conn} = ctx do
      start_client(ctx)

      Postgrex.query!(
        conn,
        """
        INSERT INTO items (
          id, value, date, timestamptz, float, bytea, interval
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7
        )
        """,
        [
          Ecto.UUID.bingenerate(),
          "test value",
          ~D[2022-05-17],
          ~U[2022-01-12 00:01:00.00Z],
          1.234567890123456,
          <<0x5, 0x10, 0xFA>>,
          %Postgrex.Interval{
            days: 1,
            months: 0,
            # 12 hours, 59 minutes, 10 seconds
            secs: 46750,
            microsecs: 0
          }
        ]
      )

      # Check that the incoming data is formatted according to Electric.Postgres.display_settings
      assert %NewRecord{
               record: %{
                 "date" => "2022-05-17",
                 "timestamptz" => "2022-01-12 00:01:00+00",
                 "float" => "1.234567890123456",
                 "bytea" => "\\x0510fa",
                 "interval" => "P1DT12H59M10S"
               }
             } = receive_tx_change()
    end

    test "exits with irrecoverable slot error with large transactions", %{db_conn: conn} = ctx do
      pid =
        start_client(ctx,
          replication_opts: Keyword.put(ctx.replication_opts, :max_txn_size, 5000)
        )

      monitor = Process.monitor(pid)
      Process.unlink(pid)
      on_exit(fn -> Process.alive?(pid) && Process.exit(pid, :kill) end)

      insert_item(conn, gen_random_string(5001))

      # Verify that passing the txn size limit crashes the process

      assert_receive {
                       :DOWN,
                       ^monitor,
                       :process,
                       ^pid,
                       {:irrecoverable_slot,
                        {:exceeded_max_tx_size,
                         "Collected transaction exceeds limit of 5000 bytes."}}
                     },
                     @assert_receive_db_timeout
    end

    test "exits with irrecoverable slot error for invalid replica identity",
         %{db_conn: conn} = ctx do
      pid = start_client(ctx)

      monitor = Process.monitor(pid)
      Process.unlink(pid)
      on_exit(fn -> Process.alive?(pid) && Process.exit(pid, :kill) end)

      {_id, bin_uuid} = gen_uuid()

      Postgrex.query!(conn, "INSERT INTO items (id, value) VALUES ($1, $2)", [bin_uuid, "test"])
      assert %NewRecord{record: %{"value" => "test"}} = receive_tx_change()
      Postgrex.query!(conn, "UPDATE items SET value = $2 WHERE id = $1", [bin_uuid, "new"])

      # Verify that receiving updates without old values causes an exit
      assert_receive {
                       :DOWN,
                       ^monitor,
                       :process,
                       ^pid,
                       {:irrecoverable_slot, {:replica_not_full, msg}}
                     },
                     @assert_receive_db_timeout

      assert msg =~
               "Received an update from PG for public.items that did not have old data included in the message."
    end

    @tag with_empty_publication?: true
    @tag handle_event: {MockTransactionProcessor, :handle_event_async, []}
    test "flushes are advanced based on standby messages when publication is otherwise silent",
         %{db_conn: conn} = ctx do
      pid = start_client(ctx)
      confirmed_flush_lsn = get_confirmed_flush_lsn(conn, ctx.slot_name)

      # Hold the startup marker until after observing the slot boundary. The
      # marker is an empty transaction for an empty publication, but the test
      # processor still models ShapeLogCollector's durable flush callback.
      start_supervised({MockTransactionProcessor, self()})
      assert_receive {:replication_caught_up, ^pid}, @assert_receive_db_timeout

      # This is to pass the time instead of a sleep - if PG is responsive, it probably has processed the wal acknowledge too
      Postgrex.query!(conn, "SELECT * FROM items", [])

      new_confirmed_flush_lsn = get_confirmed_flush_lsn(conn, ctx.slot_name)
      assert Lsn.compare(confirmed_flush_lsn, new_confirmed_flush_lsn) == :lt
    end

    @tag with_empty_publication?: true
    test "flushes are not advancing if something isn't actually getting flushed",
         %{db_conn: conn} = ctx do
      Postgrex.query!(conn, "ALTER PUBLICATION #{ctx.slot_name} SET TABLE serial_ids", [])

      pid = start_client(ctx)

      Postgrex.query!(conn, "INSERT INTO serial_ids (id) VALUES (1)", [])

      assert {lsn1, %NewRecord{record: %{"id" => "1"}}} = receive_tx_change_with_lsn()

      confirmed_flush_lsn1 = get_confirmed_flush_lsn(conn, ctx.slot_name)

      assert Lsn.to_integer(confirmed_flush_lsn1) < Lsn.to_integer(lsn1)

      for _ <- 1..10 do
        insert_item(conn, "test value")
      end

      send(pid, {:flush_boundary_updated, 100})

      # This is to pass the time instead of a sleep - if PG is responsive, it probably has processed the wal acknowledge too
      Postgrex.query!(conn, "SELECT * FROM items", [])

      # Still same LSN
      assert confirmed_flush_lsn1 ==
               get_confirmed_flush_lsn(conn, ctx.slot_name)
    end

    @tag with_empty_publication?: true
    test "flushes are advanced once confirmed, and advance beyond the last seen txn once up-to-date",
         %{db_conn: conn} = ctx do
      Postgrex.query!(conn, "ALTER PUBLICATION #{ctx.slot_name} SET TABLE serial_ids", [])

      pid = start_client(ctx)

      Postgrex.query!(conn, "INSERT INTO serial_ids (id) VALUES (1)", [])

      {lsn1, %NewRecord{record: %{"id" => "1"}}} = receive_tx_change_with_lsn()

      confirmed_flush_lsn1 = get_confirmed_flush_lsn(conn, ctx.slot_name)

      assert Lsn.to_integer(confirmed_flush_lsn1) < Lsn.to_integer(lsn1)

      Postgrex.query!(conn, "INSERT INTO serial_ids (id) VALUES (2)", [])

      {lsn2, %NewRecord{record: %{"id" => "2"}}} = receive_tx_change_with_lsn()

      send(pid, {:flush_boundary_updated, Lsn.to_integer(lsn1)})

      # Unrelated writes
      for _ <- 1..10, do: insert_item(conn, "test value")

      assert Lsn.increment(lsn1, 1) == get_confirmed_flush_lsn(conn, ctx.slot_name)

      send(pid, {:flush_boundary_updated, Lsn.to_integer(lsn2)})
      Process.sleep(50)

      # This is to pass the time instead of a sleep - if PG is responsive, it probably has processed the wal acknowledge too
      Postgrex.query!(conn, "SELECT * FROM items", [])

      # We should be using a higher LSN - one of the received ones - as "last seen", but in some race conditions
      # we might not have had a reason to respond to PG yet, so we're using `>=`
      assert Lsn.to_integer(get_confirmed_flush_lsn(conn, ctx.slot_name)) >=
               Lsn.to_integer(Lsn.increment(lsn2, 1))
    end
  end

  defp get_confirmed_flush_lsn(conn, slot_name) do
    %Postgrex.Result{rows: [[confirmed_flush_lsn]]} =
      Postgrex.query!(
        conn,
        "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
        [slot_name]
      )

    confirmed_flush_lsn
  end

  describe "ReplicationClient against real db (toast)" do
    setup [
      :with_unique_db,
      :with_basic_tables,
      :with_publication,
      :with_replication_opts,
      :with_status_monitor,
      :with_lsn_tracker
    ]

    setup %{db_conn: conn} = ctx do
      Postgrex.query!(
        conn,
        "CREATE TABLE items2 (id UUID PRIMARY KEY, val1 TEXT, val2 TEXT, num INTEGER)",
        []
      )

      Postgrex.query!(conn, "ALTER TABLE items2 REPLICA IDENTITY FULL", [])

      start_client(ctx)

      :ok
    end

    test "detoasts column values in deletes", %{db_conn: conn} do
      {id, bin_uuid} = gen_uuid()
      long_string_1 = gen_random_string(2500)
      long_string_2 = gen_random_string(3000)

      Postgrex.query!(conn, "INSERT INTO items2 (id, val1, val2) VALUES ($1, $2, $3)", [
        bin_uuid,
        long_string_1,
        long_string_2
      ])

      assert %NewRecord{
               record: %{"id" => ^id, "val1" => ^long_string_1, "val2" => ^long_string_2},
               relation: {"public", "items2"}
             } = receive_tx_change()

      Postgrex.query!(conn, "DELETE FROM items2 WHERE id = $1", [bin_uuid])

      assert %DeletedRecord{
               old_record: %{"id" => ^id, "val1" => ^long_string_1, "val2" => ^long_string_2},
               relation: {"public", "items2"}
             } = receive_tx_change()
    end

    test "detoasts column values in updates", %{db_conn: conn} do
      {id, bin_uuid} = gen_uuid()
      long_string_1 = gen_random_string(2500)
      long_string_2 = gen_random_string(3000)

      Postgrex.query!(conn, "INSERT INTO items2 (id, val1, val2) VALUES ($1, $2, $3)", [
        bin_uuid,
        long_string_1,
        long_string_2
      ])

      assert %NewRecord{
               record: %{"id" => ^id, "val1" => ^long_string_1, "val2" => ^long_string_2},
               relation: {"public", "items2"}
             } = receive_tx_change()

      Postgrex.query!(conn, "UPDATE items2 SET num = 11 WHERE id = $1", [bin_uuid])

      assert %UpdatedRecord{
               record: %{
                 "id" => ^id,
                 "val1" => ^long_string_1,
                 "val2" => ^long_string_2,
                 "num" => "11"
               },
               changed_columns: changed_columns,
               relation: {"public", "items2"}
             } = receive_tx_change()

      assert MapSet.new(["num"]) == changed_columns
    end
  end

  describe "ReplicationClient lock handling" do
    setup [
      :with_unique_db,
      :with_basic_tables,
      :with_publication,
      :with_replication_opts,
      :with_status_monitor,
      :with_lsn_tracker
    ]

    test "should acquire an advisory lock on startup", ctx do
      log =
        capture_log(fn ->
          start_client(ctx)
          assert_receive :lock_acquired
        end)

      # should have logged lock acquisition process
      lock_name = ctx.slot_name
      assert log =~ "Acquiring lock from postgres with name #{lock_name}"
      assert log =~ "Lock acquired from postgres with name #{lock_name}"

      # should have acquired an advisory lock on PG
      assert %Postgrex.Result{rows: [[false]]} =
               Postgrex.query!(
                 ctx.db_conn,
                 "SELECT pg_try_advisory_lock(hashtext('#{lock_name}'))",
                 []
               )
    end

    test "should wait if lock is already acquired", ctx do
      # grab lock with one connection
      start_client(ctx, id: :lock_client1)

      assert_receive :lock_acquired

      # try to grab the same lock using a different connection
      new_stack_id = ctx.stack_id <> "_new"
      start_link_supervised!({Electric.ProcessRegistry, stack_id: new_stack_id})

      start_link_supervised!(
        {Task.Supervisor,
         name: Electric.ProcessRegistry.name(new_stack_id, Electric.StackTaskSupervisor)}
      )

      ConsumerRegistry.registry_table(new_stack_id)
      with_lsn_tracker(%{ctx | stack_id: new_stack_id})

      start_client(
        ctx,
        id: :lock_client2,
        stack_id: new_stack_id,
        replication_opts: Keyword.put(ctx.replication_opts, :stack_id, new_stack_id),
        wait_for_start: false
      )

      # should fail to grab it
      refute_receive {:lock_acquisition_failed, _}, 1000

      # should immediately grab it once previous lock is released
      stop_supervised!(:lock_client1)
      assert_receive :lock_acquired
      stop_supervised(:lock_client2)
    end

    test "should exit if timed out on connection ", ctx do
      client_pid = start_client(ctx, timeout: 1, wait_for_start: false)
      Process.unlink(client_pid)

      ref = Process.monitor(client_pid)

      assert_receive {:DOWN, ^ref, _, _, %DBConnection.ConnectionError{message: message}},
                     1000

      assert message =~ "tcp"
    end
  end

  test "responds to a status request without treating keepalive wal_end as processed", ctx do
    state =
      ReplicationClient.State.new(
        stack_id: ctx.stack_id,
        handle_event: nil,
        publication_name: "",
        try_creating_publication?: false,
        slot_name: "",
        connection_manager: ctx.connection_manager
      )

    state = %{state | received_wal: lsn_to_wal("0/0"), flushed_wal: lsn_to_wal("0/0")}
    pg_wal = lsn_to_wal("0/10")

    assert {:noreply,
            [<<?r, received_wal::64, flushed_wal::64, flushed_wal::64, _time::64, 0::8>>], state} =
             ReplicationClient.handle_data(<<?k, pg_wal::64, 0::64, 1::8>>, state)

    assert state.received_wal == 0
    assert received_wal == state.received_wal + 1
    assert flushed_wal == state.flushed_wal + 1
  end

  defp with_publication(%{db_conn: conn, slot_name: slot_name} = ctx) do
    if Map.get(ctx, :with_empty_publication?, false) do
      Postgrex.query!(conn, "CREATE PUBLICATION #{slot_name}", [])
    else
      Postgrex.query!(conn, "CREATE PUBLICATION #{slot_name} FOR ALL TABLES", [])
    end

    :ok
  end

  defp with_replication_opts(ctx) do
    %{
      replication_opts: [
        connection_opts: ctx.db_config,
        stack_id: ctx.stack_id,
        publication_name: ctx.slot_name,
        try_creating_publication?: false,
        slot_name: ctx.slot_name,
        handle_event:
          Map.get(
            ctx,
            :handle_event,
            {__MODULE__, :test_handle_event_async, [self()]}
          ),
        connection_manager: ctx.connection_manager
      ]
    }
  end

  # Special handling for the items table to enable testing of various edge cases that depend on the result of transaction processing.
  def test_handle_event(
        %TransactionFragment{
          changes: [%NewRecord{relation: {"public", "items"}} = change]
        } = txn_fragment,
        test_pid
      ) do
    case Map.fetch!(change.record, "value") do
      "interrupt #PID" <> pid_str ->
        pid = pid_str |> String.to_charlist() |> :erlang.list_to_pid()

        if pid == self() do
          raise "Interrupting transaction processing abnormally"
        else
          send(test_pid, {:from_replication, txn_fragment})
          :ok
        end

      _ ->
        send(test_pid, {:from_replication, txn_fragment})
        :ok
    end
  end

  def test_handle_event(
        %TransactionFragment{
          commit: %Electric.Replication.Changes.Commit{},
          changes: [],
          change_count: 0
        },
        _test_pid
      ),
      do: :ok

  def test_handle_event(%TransactionFragment{} = txn_fragment, test_pid) do
    send(test_pid, {:from_replication, txn_fragment})
    :ok
  end

  def test_handle_event(%Relation{} = relation, test_pid) do
    send(test_pid, {:from_replication, [relation]})
    :ok
  end

  # Async variant for the $gen_call-based apply_event. Runs the handler inline
  # (in the gen_statem process) and queues the result as a {ref, result} message.
  # Since these test handlers are fast (no delay), this doesn't block keepalives.
  def test_handle_event_async(event, test_pid) do
    ref = make_ref()

    try do
      result = test_handle_event(event, test_pid)
      send(self(), {ref, result})
      acknowledge_empty_test_transaction(event, self())
    catch
      kind, reason ->
        send(self(), {:DOWN, ref, :process, self(), {kind, reason, __STACKTRACE__}})
    end

    ref
  end

  def test_handle_event_async_including_empty(event, test_pid) do
    ref = make_ref()
    send(test_pid, {:from_replication, event})
    send(self(), {ref, :ok})
    ref
  end

  defp acknowledge_empty_test_transaction(
         %TransactionFragment{
           lsn: lsn,
           commit: %Electric.Replication.Changes.Commit{},
           changes: [],
           change_count: 0
         },
         replication_client_pid
       ) do
    send(replication_client_pid, {:flush_boundary_updated, Lsn.to_integer(lsn)})
  end

  defp acknowledge_empty_test_transaction(_event, _replication_client_pid), do: :ok

  defp gen_random_string(length) do
    Stream.repeatedly(fn -> :rand.uniform(125 - 32) + 32 end)
    |> Enum.take(length)
    |> List.to_string()
  end

  defp lsn_to_wal(lsn_str) when is_binary(lsn_str),
    do: lsn_str |> Lsn.from_string() |> Lsn.to_integer()

  defp causal_marker_data(lsn) when is_integer(lsn) and lsn >= 0 do
    logical_message_data(lsn, CausalMarker.prefix(), <<>>)
  end

  defp logical_message_data(lsn, prefix, content, opts \\ [])
       when is_integer(lsn) and lsn >= 0 and is_binary(prefix) and is_binary(content) do
    declared_size = Keyword.get(opts, :declared_size, byte_size(content))

    logical_message =
      <<?M, 1::8, lsn::64, prefix::binary, 0, declared_size::32, content::binary>>

    xlog_data(logical_message, lsn)
  end

  defp dispatch_causal_marker_transaction(state, marker_lsn, opts \\ []) do
    xid = Keyword.get(opts, :xid, 42)
    commit_end_lsn = marker_lsn + 1

    begin_message = <<?B, marker_lsn::64, 0::64, xid::32>>

    assert {:noreply, begun_state} =
             ReplicationClient.handle_data(xlog_data(begin_message, marker_lsn), state)

    assert begun_state.pending_causal_marker_lsn == nil

    assert {:noreply, marked_state} =
             ReplicationClient.handle_data(causal_marker_data(marker_lsn), begun_state)

    assert marked_state.pending_causal_marker_lsn == marker_lsn
    assert marked_state.pending_causal_marker_xid == xid
    assert marked_state.causal_catch_up_task == nil

    commit_message = <<?C, 0, marker_lsn::64, commit_end_lsn::64, 0::64>>

    assert {:noreply_and_pause, [], dispatched_state} =
             ReplicationClient.handle_data(
               xlog_data(commit_message, commit_end_lsn),
               marked_state
             )

    assert_receive {:process_event, event, _time_remaining}, 200
    assert %TransactionFragment{xid: ^xid, commit: %Electric.Replication.Changes.Commit{}} = event
    assert dispatched_state.pending_causal_marker_lsn == nil
    assert dispatched_state.event_causal_marker_lsn == marker_lsn
    assert dispatched_state.event_causal_marker_xid == xid
    assert dispatched_state.causal_catch_up_task == nil

    {event, dispatched_state, marker_lsn}
  end

  defp acknowledge_causal_marker_transaction(
         dispatched_state,
         event,
         commit_lsn,
         order
       ) do
    ref = make_ref()

    dispatched_state = %{
      dispatched_state
      | pending_event: {ref, event, 1_000, System.monotonic_time(:millisecond)}
    }

    case order do
      :flush_before_ack ->
        assert {:noreply, [_status_update], flushed_state} =
                 ReplicationClient.handle_info(
                   {:flush_boundary_updated, commit_lsn},
                   dispatched_state
                 )

        assert {:noreply_and_resume, _acks, acknowledged_state} =
                 ReplicationClient.handle_info({ref, :ok}, flushed_state)

        acknowledged_state

      :ack_before_flush ->
        assert {:noreply_and_resume, _acks, acknowledged_state} =
                 ReplicationClient.handle_info({ref, :ok}, dispatched_state)

        assert {:noreply, [_status_update], flushed_state} =
                 ReplicationClient.handle_info(
                   {:flush_boundary_updated, commit_lsn},
                   acknowledged_state
                 )

        flushed_state
    end
  end

  defp complete_causal_marker_transaction(state, marker_lsn, order \\ :flush_before_ack) do
    {event, dispatched_state, commit_lsn} =
      dispatch_causal_marker_transaction(state, marker_lsn)

    acknowledge_causal_marker_transaction(dispatched_state, event, commit_lsn, order)
  end

  defp xlog_data(logical_message, wal) do
    <<?w, wal::64, wal::64, 0::64, logical_message::binary>>
  end

  defp finish_causal_catch_up(%{causal_catch_up_task: {_pid, ref, _target}} = state) do
    assert_receive {^ref, :ok}, @assert_receive_db_timeout
    assert {:noreply, caught_up_state} = ReplicationClient.handle_info({ref, :ok}, state)
    caught_up_state
  end

  defp causal_waiting_consumer(test_pid) do
    receive do
      {:"$gen_call", from, {:await_causal_frontier, target}} ->
        send(test_pid, {:causal_frontier_waiting, self(), target})
        caller = elem(from, 0)
        caller_ref = Process.monitor(caller)

        receive do
          :release_causal_frontier ->
            Process.demonitor(caller_ref, [:flush])
            GenServer.reply(from, :ok)

          {:DOWN, ^caller_ref, :process, ^caller, _reason} ->
            send(test_pid, {:causal_frontier_waiter_down, self()})
        end

        causal_waiting_consumer(test_pid)
    end
  end

  defp causal_first_pass_gate(test_pid, first_pass? \\ true) do
    receive do
      {:"$gen_call", from, {:await_causal_frontier, target}} when first_pass? ->
        send(test_pid, {:causal_first_pass_waiting, self(), target})

        receive do
          :release_first_causal_pass -> GenServer.reply(from, :ok)
        end

        causal_first_pass_gate(test_pid, false)

      {:"$gen_call", from, {:await_causal_frontier, target}} ->
        send(test_pid, {:causal_frontier_pass, self(), target})
        GenServer.reply(from, :ok)
        causal_first_pass_gate(test_pid, false)
    end
  end

  defp causal_immediate_consumer(test_pid, label) do
    receive do
      {:"$gen_call", from, {:await_causal_frontier, target}} ->
        send(test_pid, {:causal_frontier_pass, label, target})
        GenServer.reply(from, :ok)
        causal_immediate_consumer(test_pid, label)
    end
  end

  defp causal_installable_consumer(test_pid, reservation_installed?) do
    receive do
      {:install_causal_reservation, caller} ->
        send(caller, {:causal_reservation_installed, self()})
        causal_installable_consumer(test_pid, true)

      {:"$gen_call", from, {:await_causal_frontier, target}}
      when reservation_installed? ->
        send(test_pid, {:causal_frontier_waiting, self(), target})

        receive do
          :release_causal_frontier -> GenServer.reply(from, :ok)
        end

        causal_installable_consumer(test_pid, false)

      {:"$gen_call", from, {:await_causal_frontier, target}} ->
        send(test_pid, {:causal_frontier_pass, self(), target})
        GenServer.reply(from, :ok)
        causal_installable_consumer(test_pid, reservation_installed?)
    end
  end

  defp fetch_slot_info(conn, target_slot_name) do
    %Postgrex.Result{columns: cols, rows: rows} =
      Postgrex.query!(conn, "SELECT * FROM pg_replication_slots", [])

    [row] = Enum.filter(rows, fn [slot_name | _] -> slot_name == target_slot_name end)

    Enum.zip(cols, row) |> Map.new()
  end

  defp insert_item(conn, val) do
    Postgrex.query!(conn, "INSERT INTO items (id, value) VALUES ($1, $2)", [
      Ecto.UUID.bingenerate(),
      val
    ])
  end

  defp gen_uuid do
    id = Ecto.UUID.generate()
    {:ok, bin_uuid} = Ecto.UUID.dump(id)
    {id, bin_uuid}
  end

  defp receive_tx_change do
    {_lsn, change} = receive_tx_change_with_lsn()
    change
  end

  defp receive_tx_change_with_lsn do
    {lsn, [change]} = receive_transaction()
    {lsn, change}
  end

  defp receive_transaction(builder \\ TransactionBuilder.new())

  defp receive_transaction(builder) do
    receive do
      {:from_replication, %TransactionFragment{} = txn_fragment} ->
        case TransactionBuilder.build(txn_fragment, builder) do
          {[], builder} ->
            receive_transaction(builder)

          {[txn], builder} ->
            if txn.changes == [] do
              # Replication startup emits a transactional logical marker. Its
              # zero-change commit is a real durability barrier for
              # ShapeLogCollector, but transaction tests are waiting for the
              # first user-visible row change.
              receive_transaction(builder)
            else
              {txn.lsn, txn.changes}
            end
        end

      # Discard Relation messages - they're not relevant for transaction tests
      {:from_replication, [%Relation{} | _]} ->
        receive_transaction(builder)
    after
      @assert_receive_db_timeout ->
        raise "Expected transaction"
    end
  end

  defp start_client(ctx, overrides \\ []) do
    ctx = Enum.into(overrides, ctx)

    client_pid =
      start_link_supervised!(%{
        id: ctx[:id] || ReplicationClient,
        start:
          {ReplicationClient, :start_link,
           [
             [
               stack_id: ctx.stack_id,
               replication_opts: ctx.replication_opts,
               timeout: Map.get(ctx, :timeout, nil)
             ]
           ]},
        restart: :temporary
      })

    conn_mgr = ctx.connection_manager

    if Map.get(ctx, :wait_for_start, true) do
      assert_receive {^conn_mgr, :streaming_started}, @assert_receive_db_timeout
    end

    client_pid
  end
end
