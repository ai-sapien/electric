defmodule Electric.Shapes.Consumer do
  use GenServer, restart: :temporary

  alias Electric.Shapes.Consumer.EventHandler
  alias Electric.Shapes.Consumer.EventHandlerBuilder
  alias Electric.Shapes.Consumer.EventHandler.Subqueries.Buffering
  alias Electric.Shapes.Consumer.Effects
  alias Electric.Shapes.Consumer.InitialSnapshot
  alias Electric.Shapes.Consumer.PendingTxn
  alias Electric.Shapes.Consumer.SetupEffects
  alias Electric.Shapes.Consumer.State
  alias Electric.Shapes.Consumer.Subqueries.MoveQueue

  import Electric.Shapes.Consumer.State, only: :macros
  require Electric.Replication.LogOffset
  require Electric.Shapes.Shape

  alias Electric.Replication.LogOffset
  alias Electric.Shapes.Consumer.Materializer
  alias Electric.Shapes.ConsumerRegistry
  alias Electric.LogItems

  alias Electric.Postgres.Inspector
  alias Electric.Replication.Changes
  alias Electric.Replication.Changes.Transaction
  alias Electric.Replication.Changes.TransactionFragment
  alias Electric.Replication.ShapeLogCollector
  alias Electric.Replication.TransactionBuilder
  alias Electric.ShapeCache
  alias Electric.ShapeCache.ShapeCleaner
  alias Electric.Shapes
  alias Electric.Shapes.Shape
  alias Electric.SnapshotError
  alias Electric.Telemetry.OpenTelemetry
  alias Electric.Utils

  require Logger
  require TransactionFragment

  @default_snapshot_timeout 45_000
  @stop_and_clean_timeout 30_000
  @stop_and_clean_reason ShapeCleaner.consumer_cleanup_reason()
  @word_size :erlang.system_info(:wordsize)

  # Minimum wall-clock interval (ms) between consumer-forced full GC sweeps. Caps how much CPU
  # a busy consumer spends on full sweeps to at most one per @gc_min_interval_ms regardless of
  # fragment rate.
  @gc_min_interval_ms 1_000

  @type initialize_shape_opts() :: %{
          :action => :create | :restore,
          optional(:otel_ctx) => OpenTelemetry.otel_ctx() | nil,
          optional(:feature_flags) => [binary()],
          optional(:is_subquery_shape?) => boolean()
        }

  def name(stack_id, shape_handle) when is_binary(shape_handle) do
    ConsumerRegistry.name(stack_id, shape_handle)
  end

  def register_for_changes(stack_id, shape_handle) do
    ref = make_ref()
    Registry.register(Electric.StackSupervisor.registry_name(stack_id), shape_handle, ref)
    ref
  end

  @spec initialize_shape(pid(), Shape.t(), initialize_shape_opts()) :: :ok
  def initialize_shape(consumer_pid, shape, %{action: action} = opts)
      when action in [:create, :restore] do
    send(consumer_pid, {:initialize_shape, shape, opts})
    :ok
  end

  @doc false
  @spec await_initialization_registered(pid(), timeout()) :: :ok
  def await_initialization_registered(consumer_pid, timeout) when is_pid(consumer_pid) do
    GenServer.call(consumer_pid, :await_initialization_registered, timeout)
  end

  @spec await_snapshot_start(Electric.stack_id(), Electric.shape_handle(), timeout()) ::
          :started | {:error, any()}
  def await_snapshot_start(stack_id, shape_handle, timeout \\ @default_snapshot_timeout)
      when is_binary(stack_id) and is_binary(shape_handle) do
    stack_id
    |> consumer_pid(shape_handle)
    |> GenServer.call(:await_snapshot_start, timeout)
  end

  @spec subscribe_materializer(Electric.stack_id(), Electric.shape_handle(), pid()) ::
          {:ok, LogOffset.t()}
  def subscribe_materializer(stack_id, shape_handle, pid) do
    stack_id
    |> consumer_pid(shape_handle)
    |> GenServer.call({:subscribe_materializer, pid}, :infinity)
  end

  @spec reserve_materializer_batch(
          pid(),
          Shape.handle(),
          Materializer.causal_token(),
          LogOffset.t(),
          non_neg_integer(),
          timeout()
        ) :: :ok | {:error, :count_limit | :memory_limit}
  def reserve_materializer_batch(
        consumer_pid,
        dependency_handle,
        causal_token,
        %LogOffset{} = offset,
        expected_resolution_bytes,
        timeout \\ :infinity
      )
      when is_pid(consumer_pid) and is_binary(dependency_handle) and
             is_integer(expected_resolution_bytes) and expected_resolution_bytes >= 0 do
    GenServer.call(
      consumer_pid,
      {:reserve_materializer_batch, dependency_handle, causal_token, offset,
       expected_resolution_bytes},
      timeout
    )
  end

  @doc false
  @spec prepare_materializer_batch(
          pid(),
          Shape.handle(),
          Materializer.causal_token(),
          non_neg_integer(),
          timeout()
        ) :: :ok | {:error, :memory_limit | :unknown_reservation}
  def prepare_materializer_batch(
        consumer_pid,
        dependency_handle,
        causal_token,
        expected_resolution_bytes,
        timeout
      )
      when is_pid(consumer_pid) and is_binary(dependency_handle) and
             is_integer(expected_resolution_bytes) and expected_resolution_bytes >= 0 do
    GenServer.call(
      consumer_pid,
      {:prepare_materializer_batch, dependency_handle, causal_token, expected_resolution_bytes},
      timeout
    )
  end

  @doc false
  @spec deliver_materializer_batch(pid(), Shape.handle(), map(), timeout()) ::
          :ok | {:error, term()}
  def deliver_materializer_batch(consumer_pid, dependency_handle, payload, timeout)
      when is_pid(consumer_pid) and is_binary(dependency_handle) and is_map(payload) do
    GenServer.call(
      consumer_pid,
      {:deliver_materializer_batch, dependency_handle, payload},
      timeout
    )
  end

  @doc false
  @spec deliver_materializer_causal_end(
          pid(),
          Shape.handle(),
          Materializer.causal_token(),
          timeout()
        ) :: :ok | {:error, term()}
  def deliver_materializer_causal_end(
        consumer_pid,
        dependency_handle,
        causal_token,
        timeout
      )
      when is_pid(consumer_pid) and is_binary(dependency_handle) do
    GenServer.call(
      consumer_pid,
      {:deliver_materializer_causal_end, dependency_handle, causal_token},
      timeout
    )
  end

  @doc false
  @spec await_causal_frontier(pid(), non_neg_integer()) :: :ok | {:error, :consumer_stopped}
  def await_causal_frontier(consumer_pid, target_tx_offset)
      when is_pid(consumer_pid) and is_integer(target_tx_offset) and target_tx_offset >= 0 do
    GenServer.call(consumer_pid, {:await_causal_frontier, target_tx_offset}, :infinity)
  end

  @spec whereis(Electric.stack_id(), Electric.shape_handle()) :: pid() | nil
  def whereis(stack_id, shape_handle) do
    consumer_pid(stack_id, shape_handle)
  end

  def stop(nil, _reason) do
    :ok
  end

  def stop(pid, reason) when is_pid(pid) do
    if Process.alive?(pid) do
      GenServer.call(pid, {:stop, reason}, @stop_and_clean_timeout)
    else
      :ok
    end
  catch
    :exit, _reason -> :ok
  end

  def stop(stack_id, shape_handle, reason) do
    # if consumer is present, terminate it gracefully
    stack_id
    |> consumer_pid(shape_handle)
    |> stop(reason)
  end

  defp consumer_pid(stack_id, shape_handle) do
    ConsumerRegistry.whereis(stack_id, shape_handle)
  end

  @doc """
  Set the adaptive-GC heap threshold (bytes, or nil to disable) for a single stack.

  Consumers cache this value at startup (see `State.new/2`), so the new threshold only
  applies to consumers started after this call — already-running consumers keep the
  threshold they read when they booted. Safe to call from IEx.
  """
  @spec set_gc_heap_threshold(Electric.stack_id(), non_neg_integer() | nil) :: :ok
  def set_gc_heap_threshold(stack_id, threshold_bytes)
      when is_nil(threshold_bytes) or (is_integer(threshold_bytes) and threshold_bytes >= 0) do
    Electric.StackConfig.put(stack_id, :consumer_gc_heap_threshold, threshold_bytes)
    :ok
  end

  def start_link(%{stack_id: stack_id, shape_handle: shape_handle} = _config) do
    GenServer.start_link(__MODULE__, %{stack_id: stack_id, shape_handle: shape_handle},
      name: name(stack_id, shape_handle),
      spawn_opt: Electric.StackConfig.spawn_opts(stack_id, :consumer)
    )
  end

  @impl GenServer
  def init(%{stack_id: stack_id, shape_handle: shape_handle}) do
    activate_mocked_functions_from_test_process()

    Process.set_label({:consumer, shape_handle})
    Process.flag(:trap_exit, true)

    metadata = [shape_handle: shape_handle, stack_id: stack_id]
    Logger.metadata(metadata)
    Electric.Telemetry.Sentry.set_tags_context(metadata)

    # Shape initialization will be complete when we receive a message {:initialize_shape,
    # <shape>, <shape_opts>} which the ShapeCache is expected to send as soon as this process
    # is alive.
    {:ok, State.new(stack_id, shape_handle)}
  end

  @impl GenServer
  def handle_continue(:stop_and_clean, state) do
    stop_and_clean(state)
  end

  def handle_continue(:consume_buffer, state) do
    state =
      state
      |> process_buffered_txn_fragments()
      |> maybe_activate_materializer_subscription()
      |> maybe_mark_restore_ready()

    if state.terminating? do
      {:noreply, state, {:continue, :stop_and_clean}}
    else
      {:noreply, state, next_after_move(state, {:continue, :maybe_gc})}
    end
  end

  def handle_continue(:process_deferred_materializer_move, state) do
    process_deferred_materializer_move(state)
  end

  def handle_continue(:process_materializer_replay, state) do
    process_materializer_replay(state)
  end

  def handle_continue(:process_deferred_replication_event, state) do
    process_deferred_replication_event(state)
  end

  def handle_continue({:mark_deferred_root_post_snapshot, xid}, state) do
    handle_apply_event_result(
      state,
      apply_event(state, {:deferred_root_post_snapshot, xid})
    )
  end

  def handle_continue(:commit_move_before_deferred_root, state) do
    previous_offset = state.latest_offset
    final_state = maybe_commit_move_transaction(state)
    {final_state, notification} = move_safe_notification(final_state, previous_offset)

    handle_apply_event_result(
      state,
      {final_state, notification, 0, 0}
    )
  end

  def handle_continue(
        :process_pending_global_lsn,
        %State{pending_global_last_seen_lsn: lsn} = state
      )
      when is_integer(lsn) do
    if root_replication_pending?(state) or not is_nil(state.pending_initialization) do
      {:noreply, state, next_after_move(state)}
    else
      state = %{state | pending_global_last_seen_lsn: nil}
      handle_apply_event_result(state, apply_global_lsn(state, lsn))
    end
  end

  # Deferred adaptive GC. Reached via {:continue, :maybe_gc} after a fragment (or a
  # full buffer drain) has been processed, so a forced full sweep runs off the
  # reply/critical path rather than blocking the ShapeLogCollector. Re-establishes
  # the hibernate_after timeout that the {:continue, …} return could not carry.
  def handle_continue(:maybe_gc, state) do
    state = maybe_garbage_collect(state)
    {:noreply, state, next_after_move(state)}
  end

  @impl GenServer
  # Any incoming message counts as activity: cancel the pending suspend timer (if
  # any) and recurse for actual handling of the call.
  def handle_call(msg, from, %{suspend_timer: ref} = state) when not is_nil(ref) do
    handle_call(msg, from, cancel_suspend_timer(state))
  end

  def handle_call({:monitor, pid}, _from, %{monitors: monitors} = state) do
    ref = make_ref()
    {:reply, ref, %{state | monitors: [{pid, ref} | monitors]}, state.hibernate_after}
  end

  # ShapeCache sends initialization and performs this call from the same
  # process. Erlang's per-sender mailbox ordering therefore makes this reply a
  # registration barrier: storage-backed state is installed before callers may
  # start a dependency Materializer or open collector processing.
  def handle_call(:await_initialization_registered, _from, state) do
    {:reply, :ok, state, state.hibernate_after}
  end

  def handle_call(:await_snapshot_start, _from, %{restore_ready?: true} = state)
      when is_snapshot_started(state) do
    {:reply, :started, state, state.hibernate_after}
  end

  def handle_call(:await_snapshot_start, from, state) do
    Logger.debug("Starting a wait on the snapshot #{state.shape_handle} for #{inspect(from)}}")
    state = State.add_waiter(state, from)
    {:noreply, state, state.hibernate_after}
  end

  def handle_call({:await_causal_frontier, target_tx_offset}, from, %State{} = state)
      when is_integer(target_tx_offset) and target_tx_offset >= 0 do
    if causal_frontier_pending?(state, target_tx_offset) do
      caller_monitor = Process.monitor(elem(from, 0), tag: :causal_drain_waiter_down)
      waiters = [{from, target_tx_offset, caller_monitor} | state.causal_drain_waiters]
      {:noreply, %{state | causal_drain_waiters: waiters}, state.hibernate_after}
    else
      {:reply, :ok, state, state.hibernate_after}
    end
  end

  def handle_call(
        {:reserve_materializer_batch, dependency_handle, causal_token, %LogOffset{} = offset,
         expected_resolution_bytes},
        _from,
        %State{} = state
      ) do
    if dependency_handle in state.shape.shape_dependencies_handles and
         Materializer.causal_token_offset(causal_token) == offset do
      case reserve_materializer_batches(
             state,
             dependency_handle,
             [{causal_token, expected_resolution_bytes}]
           ) do
        {:ok, state} ->
          {:reply, :ok, state, next_after_move(state)}

        {:error, reason, state} ->
          reject_materializer_reservation(state, dependency_handle, reason)
      end
    else
      {:stop, {:unknown_materializer_dependency, dependency_handle}, state}
    end
  end

  def handle_call(
        {:prepare_materializer_batch, dependency_handle, causal_token, expected_resolution_bytes},
        _from,
        %State{} = state
      ) do
    case prepare_reserved_materializer_batch(
           state,
           dependency_handle,
           causal_token,
           expected_resolution_bytes
         ) do
      {:ok, state} ->
        {:reply, :ok, state, next_after_move(state)}

      {:error, :memory_limit, state} ->
        reject_materializer_reservation(state, dependency_handle, :memory_limit)

      :unreserved ->
        Logger.warning("Materializer tried to prepare an unknown causal reservation",
          dependency_shape_handle: dependency_handle,
          shape_handle: state.shape_handle
        )

        reject_materializer_reservation(state, dependency_handle, :unknown_reservation)
    end
  end

  def handle_call(
        {:deliver_materializer_batch, dependency_handle, payload},
        _from,
        %State{} = state
      ) do
    synchronous_materializer_reply(
      handle_info({:materializer_changes, dependency_handle, payload}, state)
    )
  end

  def handle_call(
        {:deliver_materializer_causal_end, dependency_handle, causal_token},
        _from,
        %State{} = state
      ) do
    synchronous_materializer_reply(
      handle_info({:materializer_causal_end, dependency_handle, causal_token}, state)
    )
  end

  def handle_call(
        {:handle_event, event, trace_context},
        _from,
        %{pending_initialization: pending_initialization} = state
      )
      when not is_nil(pending_initialization) do
    # A dependency materializer is asynchronously preparing this shape's stale
    # replay seed. Keep the Consumer responsive and apply replication in order
    # after initialization instead of blocking the global ShapeLogCollector.
    defer_replication_event(event, trace_context, state)
  end

  def handle_call(
        {:handle_event, event, trace_context},
        _from,
        %{
          materializer_barrier_active?: barrier_active?,
          deferred_replication_event_count: deferred_count
        } = state
      )
      when barrier_active? or deferred_count > 0 do
    defer_replication_event(event, trace_context, state)
  end

  def handle_call({:handle_event, event, trace_context}, _from, state) do
    OpenTelemetry.set_current_context(trace_context)

    case handle_event(event, state) do
      %{terminating?: true} = state ->
        {:reply, :ok, state, {:continue, :stop_and_clean}}

      state ->
        state =
          state
          |> record_processed_replication_event(event)
          |> maybe_activate_materializer_subscription()
          |> maybe_mark_restore_ready()

        {:reply, :ok, state, next_after_move(state, {:continue, :maybe_gc})}
    end
  end

  def handle_call(
        {:subscribe_materializer, _pid},
        _from,
        %{pending_materializer_subscription: pending} = state
      )
      when not is_nil(pending) do
    {:reply, {:error, :subscription_pending}, state, state.hibernate_after}
  end

  def handle_call({:subscribe_materializer, pid}, from, state) do
    Logger.debug("Subscribing materializer for #{state.shape_handle}")

    if materializer_subscription_blocked?(state) do
      # Do not subscribe halfway through a fragmented transaction: changes
      # before this call were not sent live, while changes after it would be.
      # Waiting lets the materializer replay the entire committed transaction
      # exactly once from storage.
      {:noreply, %{state | pending_materializer_subscription: {from, pid}}, state.hibernate_after}
    else
      {reply, state} = activate_materializer_subscription(pid, state)
      {:reply, reply, state, state.hibernate_after}
    end
  end

  def handle_call({:stop, reason}, _from, state) do
    {reason, state} = stop_with_reason(reason, state)
    {:stop, reason, :ok, state}
  end

  @impl GenServer
  # Cancel the suspend timer on activity, then recurse for the actual handling of the cast.
  def handle_cast(msg, %{suspend_timer: ref} = state) when not is_nil(ref) do
    handle_cast(msg, cancel_suspend_timer(state))
  end

  def handle_cast(
        {:pg_snapshot_known, shape_handle, {xmin, xmax, xip_list} = snapshot},
        %{shape_handle: shape_handle} = state
      ) do
    Logger.debug(
      "Snapshot known for shape_handle: #{shape_handle} xmin: #{xmin}, xmax: #{xmax}, xip_list: #{inspect(xip_list)}"
    )

    {:noreply, State.set_initial_snapshot(state, snapshot), {:continue, :consume_buffer}}
  end

  def handle_cast({:snapshot_started, shape_handle}, %{shape_handle: shape_handle} = state) do
    Logger.debug("Snapshot started shape_handle: #{shape_handle}")
    {:noreply, State.mark_snapshot_started(state, state.restore_ready?), state.hibernate_after}
  end

  def handle_cast(
        {:snapshot_failed, shape_handle, %SnapshotError{} = error},
        %{shape_handle: shape_handle} = state
      ) do
    if error.type == :schema_changed do
      # Schema changed while we were creating stuff, which means shape is functionally invalid.
      # Return a 409 to trigger a fresh start with validation against the new schema.
      %{shape: %Shape{root_table_id: root_table_id}} = state
      clean_table(root_table_id, state)
    end

    state
    |> State.reply_to_snapshot_waiters({:error, error})
    |> stop_and_clean()
  end

  def handle_cast({:snapshot_exists, shape_handle}, %{shape_handle: shape_handle} = state) do
    {:noreply, State.mark_snapshot_started(state, state.restore_ready?), state.hibernate_after}
  end

  @impl GenServer
  def handle_info(:suspend_timeout, %{suspend_timer: ref} = state) when not is_nil(ref) do
    state = %{state | suspend_timer: nil}

    if consumer_suspend_enabled?(state) and consumer_can_suspend?(state) do
      Logger.debug(fn -> ["Suspending consumer ", to_string(state.shape_handle)] end)
      {:stop, ShapeCleaner.consumer_suspend_reason(), state}
    else
      # Conditions changed - just restart the hibernate timeout
      {:noreply, state, state.hibernate_after}
    end
  end

  # Timer already cancelled. Ignore the trigger.
  def handle_info(:suspend_timeout, state) do
    {:noreply, state, state.hibernate_after}
  end

  # Any incoming message counts as activity: cancel the pending suspend timer (if any)
  # and recurse for the actual handling of the message.
  def handle_info(msg, %{suspend_timer: ref} = state) when not is_nil(ref) do
    handle_info(msg, cancel_suspend_timer(state))
  end

  def handle_info({:initialize_shape, shape, opts}, state) do
    %{stack_id: stack_id, shape_handle: shape_handle} = state

    state = State.initialize_shape(state, shape, opts)

    stack_storage = ShapeCache.Storage.for_stack(stack_id)
    storage = ShapeCache.Storage.for_shape(shape_handle, stack_storage)

    # TODO: Remove. Only needed for InMemoryStorage
    case ShapeCache.Storage.start_link(storage) do
      {:ok, _pid} -> :ok
      :ignore -> :ok
    end

    writer = ShapeCache.Storage.init_writer!(storage, shape)

    state = State.initialize(state, storage, writer)

    state = %{
      state
      | restore_ready?: opts.action != :restore or shape.shape_dependencies_handles == []
    }

    finish_initialization(state, opts.action, Map.get(opts, :otel_ctx, nil))
  end

  def handle_info({ShapeCache.Storage, :flushed, flushed_offset}, state) do
    state =
      if (is_write_unit_txn(state.write_unit) or is_nil(state.pending_txn)) and
           not state.move_transaction_open? do
        # We're not currently in the middle of processing a transaction. This flushed offset is either
        # from a previously processed transaction or a non-commit fragment of the most recently
        # seen transaction. Notify ShapeLogCollector about it immediately.
        flushed_offset = more_recent_offset(state.pending_flush_offset, flushed_offset)

        state
        |> Map.put(:pending_flush_offset, nil)
        |> confirm_flushed_and_notify(flushed_offset)
      else
        # Storage has signaled latest flushed offset in the middle of processing a multi-fragment
        # transaction. Save it for later, to be handled when the commit fragment arrives.
        updated_offset = more_recent_offset(state.pending_flush_offset, flushed_offset)
        %{state | pending_flush_offset: updated_offset}
      end

    {:noreply, state, state.hibernate_after}
  end

  def handle_info({:global_last_seen_lsn, lsn}, state) do
    state = %{
      state
      | last_observed_global_lsn: max(state.last_observed_global_lsn, lsn)
    }

    # The registry broadcast can reach this consumer before initialization has
    # built the event handler; applying then crashes the consumer mid-startup
    # (the 2026-07-17 recreate-storm, SAP-8006), so the LSN is stashed and
    # drained by :process_pending_global_lsn once initialization completes.
    if root_replication_pending?(state) or not is_nil(state.pending_initialization) do
      pending_lsn = max(state.pending_global_last_seen_lsn || 0, lsn)
      state = %{state | pending_global_last_seen_lsn: pending_lsn}
      {:noreply, state, next_after_move(state)}
    else
      handle_apply_event_result(state, apply_global_lsn(state, lsn))
    end
  end

  # This is part of the storage module contract - messages tagged storage should be applied to the writer state.
  def handle_info({ShapeCache.Storage, message}, state) do
    writer = ShapeCache.Storage.apply_message(state.writer, message)
    {:noreply, %{state | writer: writer}, state.hibernate_after}
  end

  def handle_info(
        {:materializer_changes, dep_handle, %{move_in: move_in, move_out: move_out} = payload},
        state
      ) do
    event = {:materializer_changes, dep_handle, payload}

    case fill_reserved_materializer_batch(state, dep_handle, payload, event) do
      {:ok, state} ->
        if not is_nil(state.pending_initialization) or not is_nil(state.pending_txn) or
             state.move_transaction_open? or state.pending_materializer_replay_count > 0 do
          {:noreply, state, state.hibernate_after}
        else
          {:noreply, state, next_after_move(state)}
        end

      {:error, state} ->
        {:noreply, state, {:continue, :stop_and_clean}}

      :unreserved when is_map_key(payload, :causal_token) ->
        Logger.warning("Received an unreserved materializer causal payload; invalidating shape",
          dependency_shape_handle: dep_handle,
          shape_handle: state.shape_handle
        )

        stop_and_clean(state)

      :unreserved ->
        causal_origin = materializer_payload_causal_origin(payload)

        if not is_nil(state.pending_initialization) or not is_nil(state.pending_txn) or
             state.move_transaction_open? or state.pending_materializer_replay_count > 0 or
             state.deferred_materializer_move_count > 0 do
          defer_materializer_move(event, state)
        else
          handle_materializer_move(
            event,
            move_in,
            move_out,
            state,
            nil,
            causal_origin,
            materializer_payload_causal_depth(payload)
          )
        end
    end
  end

  def handle_info({:materializer_causal_end, dep_handle, causal_token}, state) do
    case fill_reserved_materializer_end(state, dep_handle, causal_token) do
      {:ok, state} ->
        {:noreply, state, next_after_move(state)}

      :unreserved ->
        Logger.warning("Received an unknown materializer causal end; invalidating shape",
          dependency_shape_handle: dep_handle,
          shape_handle: state.shape_handle
        )

        stop_and_clean(state)
    end
  end

  def handle_info({:materializer_replay_ready, dep_handle}, state) do
    state =
      case :queue.peek(state.pending_materializer_replays) do
        {:value, {^dep_handle, _materializer_pid}} ->
          %{state | materializer_replay_waiting?: false}

        _other ->
          state
      end

    {:noreply, state, next_after_move(state)}
  end

  def handle_info(:recheck_causal_drain_waiters, state) do
    state = reply_causal_drain_waiters(state)
    {:noreply, state, next_after_move(state)}
  end

  def handle_info({:causal_drain_waiter_down, ref, :process, _pid, _reason}, state) do
    waiters =
      Enum.reject(state.causal_drain_waiters, fn {_from, _target, waiter_ref} ->
        waiter_ref == ref
      end)

    {:noreply, %{state | causal_drain_waiters: waiters}, state.hibernate_after}
  end

  def handle_info(
        {:materializer_replay_ready, dep_handle, {:ok, seed_view, applied_offset}},
        %State{
          pending_dependency_subscription: {dep_handle, materializer_pid, _subscription_ref},
          pending_initialization: {action, otel_ctx}
        } = state
      ) do
    from_lsn = Map.get(state.move_positions, dep_handle)

    result =
      state
      |> Map.put(:pending_dependency_subscription, nil)
      |> apply_materializer_subscription(
        dep_handle,
        materializer_pid,
        from_lsn,
        seed_view,
        applied_offset
      )

    case result do
      {:ok, state} ->
        finish_initialization(state, action, otel_ctx)

      {:error, :multiple_stale_dependencies, state} ->
        Logger.warning(
          "More than one stale dependency requires replay; invalidating outer shape",
          shape_handle: state.shape_handle,
          dependency_shape_handle: dep_handle,
          existing_replay_dependencies:
            Enum.map(:queue.to_list(state.pending_materializer_replays), &elem(&1, 0))
        )

        stop_and_clean(%{state | pending_initialization: nil})
    end
  end

  def handle_info(
        {:materializer_replay_ready, dep_handle, {:error, reason}},
        %State{
          pending_dependency_subscription: {dep_handle, _materializer_pid, _subscription_ref}
        } =
          state
      ) do
    Logger.warning("Dependency replay could not be resumed; invalidating outer shape",
      dependency_shape_handle: dep_handle,
      reason: inspect(reason)
    )

    stop_and_clean(%{state | pending_dependency_subscription: nil, pending_initialization: nil})
  end

  def handle_info(
        {:dependency_subscription_result, dep_handle, materializer_pid, subscription_ref,
         {:ok, seed_view, applied_offset, pending_batch_offsets}},
        %State{
          pending_dependency_subscription: {dep_handle, materializer_pid, subscription_ref},
          pending_initialization: {action, otel_ctx}
        } = state
      ) do
    from_lsn = Map.get(state.move_positions, dep_handle)

    result =
      state
      |> Map.put(:pending_dependency_subscription, nil)
      |> apply_materializer_subscription(
        dep_handle,
        materializer_pid,
        from_lsn,
        seed_view,
        applied_offset
      )

    case result do
      {:ok, state} ->
        case reserve_materializer_batches(state, dep_handle, pending_batch_offsets) do
          {:ok, state} ->
            finish_initialization(state, action, otel_ctx)

          {:error, reason, state} ->
            Logger.warning("Dependency subscription reservations exceeded the outer limit",
              dependency_shape_handle: dep_handle,
              shape_handle: state.shape_handle,
              reason: reason
            )

            stop_and_clean(%{state | pending_initialization: nil})
        end

      {:error, :multiple_stale_dependencies, state} ->
        Logger.warning(
          "More than one stale dependency requires replay; invalidating outer shape",
          shape_handle: state.shape_handle,
          dependency_shape_handle: dep_handle,
          existing_replay_dependencies:
            Enum.map(:queue.to_list(state.pending_materializer_replays), &elem(&1, 0))
        )

        stop_and_clean(%{state | pending_initialization: nil})
    end
  end

  def handle_info(
        {:dependency_subscription_result, dep_handle, materializer_pid, subscription_ref,
         {:pending, _current_offset}},
        %State{
          pending_dependency_subscription: {dep_handle, materializer_pid, subscription_ref}
        } = state
      ) do
    # The Materializer owns the bounded seed worker and will send
    # :materializer_replay_ready directly to this Consumer.
    {:noreply, state, state.hibernate_after}
  end

  def handle_info(
        {:dependency_subscription_result, dep_handle, materializer_pid, subscription_ref,
         {:error, reason}},
        %State{
          pending_dependency_subscription: {dep_handle, materializer_pid, subscription_ref}
        } = state
      ) do
    Logger.warning("Dependency materializer rejected replay subscription",
      dependency_shape_handle: dep_handle,
      shape_handle: state.shape_handle,
      reason: inspect(reason)
    )

    stop_and_clean(%{state | pending_dependency_subscription: nil, pending_initialization: nil})
  end

  # A seed worker can win the race with the small task that forwards the
  # initial `{:pending, offset}` reply. Once initialization has advanced, that
  # stale acknowledgement is harmless.
  def handle_info({:dependency_subscription_result, _dep, _pid, _ref, _result}, state) do
    {:noreply, state, state.hibernate_after}
  end

  def handle_info({:pg_snapshot_known, snapshot}, state) do
    Logger.debug(fn -> "Snapshot known for active move-in" end)
    handle_apply_event_result(state, apply_event(state, {:pg_snapshot_known, snapshot}))
  end

  def handle_info(
        {:query_move_in_complete, snapshot_name, row_count, row_bytes, move_in_lsn},
        state
      ) do
    Logger.debug(fn ->
      "Consumer query move in complete for #{state.shape_handle} with #{row_count} rows from #{snapshot_name} (#{row_bytes} bytes)"
    end)

    handle_apply_event_result(
      state,
      apply_event(
        state,
        {:query_move_in_complete, snapshot_name, row_count, row_bytes, move_in_lsn}
      )
    )
  end

  def handle_info({:query_move_in_error, error, stacktrace}, state) do
    Logger.error(
      "Error querying move in for #{state.shape_handle}: #{Exception.format(:error, error, stacktrace)}"
    )

    reraise(error, stacktrace)

    # No-op as the raise will crash the process
    stop_and_clean(state)
  end

  def handle_info({:materializer_shape_invalidated, shape_handle}, state) do
    Logger.warning("Materializer shape invalidated for shape", shape_handle: shape_handle)
    stop_and_clean(state)
  end

  def handle_info({:materializer_down, _ref, :process, pid, reason}, state) do
    Logger.warning(
      "Materializer down for consumer: #{state.shape_handle} (#{inspect(pid)}) (#{inspect(reason)})"
    )

    handle_materializer_down(reason, state)
  end

  def handle_info({{:dependency_materializer_down, handle}, _ref, :process, pid, reason}, state) do
    Logger.warning(
      "Materializer down for a dependency: #{handle} (#{inspect(pid)}) (#{inspect(reason)})"
    )

    handle_materializer_down(reason, state)
  end

  # We're trapping exists so that `terminate` is called to clean up the writer,
  # otherwise we respect the OTP exit protocol. Since nothing is linked to the consumer
  # we shouldn't see this...
  def handle_info({:EXIT, _pid, reason}, state) do
    Logger.error("Caught EXIT: #{inspect(reason)}")
    {:stop, reason, state}
  end

  # Set new values for hibernate_after and suspend_after, and set a jittered
  # timeout between hibernate_after and jitter_period to spread hibernation
  # events. Each consumer will hibernate at the jittered timeout, then schedule
  # suspension for suspend_after ms later.
  def handle_info({:configure_suspend, hibernate_after, suspend_after, jitter_period}, state) do
    state = %{state | hibernate_after: hibernate_after, suspend_after: suspend_after}
    {:noreply, state, Enum.random(hibernate_after..jitter_period)}
  end

  def handle_info(:timeout, state) do
    state = %{state | writer: ShapeCache.Storage.hibernate(state.writer)}

    state =
      if consumer_suspend_enabled?(state) and consumer_can_suspend?(state),
        do: schedule_suspend_timer(state),
        else: state

    {:noreply, state, :hibernate}
  end

  defp synchronous_materializer_reply({:noreply, state, timeout_or_continue}),
    do: {:reply, :ok, state, timeout_or_continue}

  defp synchronous_materializer_reply({:stop, reason, state}),
    do: {:stop, reason, {:error, reason}, state}

  defp reject_materializer_reservation(state, dependency_handle, reason) do
    Logger.warning("Rejecting a materializer causal reservation and invalidating outer shape",
      dependency_shape_handle: dependency_handle,
      shape_handle: state.shape_handle,
      reason: reason
    )

    {:reply, {:error, reason}, mark_for_removal(state), {:continue, :stop_and_clean}}
  end

  defp process_deferred_materializer_move(%{pending_txn: pending_txn} = state)
       when not is_nil(pending_txn) do
    {:noreply, state, state.hibernate_after}
  end

  defp process_deferred_materializer_move(%{move_transaction_open?: true} = state) do
    {:noreply, state, state.hibernate_after}
  end

  defp process_deferred_materializer_move(state) do
    case :queue.out(state.deferred_materializer_moves) do
      {:empty, _queue} ->
        {:noreply, state, state.hibernate_after}

      {{:value,
        {{:reserved_materializer_batch, _dep_handle, _offset, _token, _downstream_token, nil},
         _bytes}}, _queue} ->
        # A later dependency can become durable before an earlier one. Keep its
        # payload in its ordered slot and wait for the head reservation rather
        # than reverting to arrival order.
        {:noreply, state, state.hibernate_after}

      {{:value,
        {{:reserved_materializer_batch, _dep_handle, _offset, causal_token, downstream_token,
          {:materializer_changes, _event_dep_handle, payload} = event}, event_bytes}}, queue} ->
        state = remove_deferred_materializer_move(state, queue, event_bytes)

        handle_materializer_move(
          event,
          payload.move_in,
          payload.move_out,
          state,
          downstream_token,
          Materializer.causal_token_offset(causal_token),
          Materializer.causal_token_depth(causal_token)
        )

      {{:value,
        {{:reserved_materializer_batch, _dep_handle, _offset, _token, downstream_token,
          :causal_end}, event_bytes}}, queue} ->
        state = remove_deferred_materializer_move(state, queue, event_bytes)

        state =
          state
          |> forward_completed_causal_token(downstream_token, false)
          |> maybe_release_materializer_barrier()
          |> maybe_activate_materializer_subscription()
          |> maybe_mark_restore_ready()

        {:noreply, state, next_after_move(state)}

      {{:value, {{:materializer_changes, _dep_handle, payload} = event, event_bytes}}, queue} ->
        state = remove_deferred_materializer_move(state, queue, event_bytes)

        handle_materializer_move(
          event,
          payload.move_in,
          payload.move_out,
          state,
          nil,
          materializer_payload_causal_origin(payload),
          materializer_payload_causal_depth(payload)
        )

      {{:value, {{:materializer_replay, dep_handle, payload}, event_bytes}}, queue} ->
        state =
          state
          |> remove_deferred_materializer_move(queue, event_bytes)
          |> Map.put(:materializer_replay_lookahead?, false)

        event = {:materializer_changes, dep_handle, payload}

        handle_materializer_move(
          event,
          payload.move_in,
          payload.move_out,
          state,
          nil,
          materializer_payload_causal_origin(payload),
          materializer_payload_causal_depth(payload)
        )
    end
  end

  defp remove_deferred_materializer_move(state, queue, event_bytes) do
    state = %{
      state
      | deferred_materializer_moves: queue,
        deferred_materializer_move_count: state.deferred_materializer_move_count - 1,
        deferred_event_bytes: state.deferred_event_bytes - event_bytes
    }

    maybe_release_causal_lsn_subscription(state)
  end

  defp process_materializer_replay(%{move_transaction_open?: true} = state) do
    {:noreply, state, state.hibernate_after}
  end

  defp process_materializer_replay(%{materializer_replay_lookahead?: true} = state) do
    {:noreply, state, next_after_move(state)}
  end

  defp process_materializer_replay(%{materializer_replay_waiting?: true} = state) do
    {:noreply, state, state.hibernate_after}
  end

  defp process_materializer_replay(state) do
    case :queue.peek(state.pending_materializer_replays) do
      :empty ->
        state = maybe_release_materializer_barrier(state)
        {:noreply, state, next_after_move(state)}

      {:value, {dep_handle, materializer_pid}} ->
        case Materializer.next_replay(materializer_pid, self()) do
          {:ok, %{move_in: _, move_out: _} = payload} ->
            event = {:materializer_replay, dep_handle, payload}
            state = %{state | materializer_replay_waiting?: false}

            case enqueue_deferred_materializer_move(event, state) do
              {:ok, state} ->
                # Keep exactly one replay transaction as lookahead in the
                # deferred scheduler. Root replication from the same source
                # transaction must be able to overtake it, while a later root
                # transaction proves that this replay transaction committed
                # first. Pull the next replay item only after this one has
                # fully committed.
                {:noreply, state, next_after_move(state)}

              {:error, state} ->
                {:noreply, state, {:continue, :stop_and_clean}}
            end

          {:done, pending_batch_offsets} ->
            case reserve_materializer_batches(state, dep_handle, pending_batch_offsets) do
              {:ok, state} ->
                finish_materializer_replay(state)

              {:error, reason, state} ->
                Logger.warning("Dependency replay reservations exceeded the outer shape limit",
                  dependency_shape_handle: dep_handle,
                  shape_handle: state.shape_handle,
                  reason: reason
                )

                stop_and_clean(state)
            end

          :done ->
            finish_materializer_replay(state)

          :pending ->
            # Another outer consumer owns this source materializer's one
            # bounded replay state. Promotion sends :materializer_replay_ready;
            # do not spin or retain a second full source index here.
            {:noreply, %{state | materializer_replay_waiting?: true}, state.hibernate_after}

          {:error, reason} ->
            Logger.warning("Dependency replay exceeded its safe history boundary",
              dependency_shape_handle: dep_handle,
              reason: inspect(reason)
            )

            stop_and_clean(state)
        end
    end
  end

  defp finish_materializer_replay(state) do
    {_entry, queue} = :queue.out(state.pending_materializer_replays)

    state = %{
      state
      | pending_materializer_replays: queue,
        pending_materializer_replay_count: state.pending_materializer_replay_count - 1,
        materializer_replay_lookahead?: false,
        materializer_replay_waiting?: false
    }

    state =
      state
      |> maybe_release_materializer_barrier()
      |> maybe_activate_materializer_subscription()
      |> maybe_mark_restore_ready()

    {:noreply, state, next_after_move(state)}
  end

  defp process_deferred_replication_event(state) do
    case :queue.out(state.deferred_replication_events) do
      {:empty, _queue} ->
        {:noreply, state, state.hibernate_after}

      {{:value, {event, trace_context, event_bytes}}, queue} ->
        OpenTelemetry.set_current_context(trace_context)

        state = %{
          state
          | deferred_replication_events: queue,
            deferred_replication_event_count: state.deferred_replication_event_count - 1,
            deferred_event_bytes: state.deferred_event_bytes - event_bytes
        }

        case handle_event(event, state) do
          %{terminating?: true} = state ->
            {:noreply, state, {:continue, :stop_and_clean}}

          state ->
            state =
              state
              |> record_processed_replication_event(event)
              |> maybe_activate_materializer_subscription()
              |> maybe_mark_restore_ready()

            {:noreply, state, next_after_move(state, {:continue, :maybe_gc})}
        end
    end
  end

  defp handle_materializer_move(
         {:materializer_changes, dep_handle, payload} = event,
         move_in,
         move_out,
         state,
         downstream_causal_token,
         causal_origin,
         causal_depth
       ) do
    Logger.debug(fn ->
      "Consumer reacting to #{length(move_in)} move ins and #{length(move_out)} move outs from its #{dep_handle} dependency"
    end)

    if not is_nil(downstream_causal_token) and
         not is_nil(state.active_downstream_causal_token) do
      raise "started a materializer causal batch while another batch was active"
    end

    state =
      state
      |> maybe_acquire_materializer_frontier(causal_origin)
      |> Map.put(:active_downstream_causal_token, downstream_causal_token)
      |> record_pending_move_lsn(dep_handle, payload)
      |> record_pending_move_causal_origin(causal_origin, causal_depth)
      |> maybe_begin_move_transaction(payload)

    handle_apply_event_result(state, apply_event(state, event))
  end

  defp defer_materializer_move(event, state) do
    case enqueue_deferred_materializer_move(event, state) do
      {:ok, state} ->
        {:noreply, state, next_after_move(state)}

      {:error, state} ->
        {:noreply, state, {:continue, :stop_and_clean}}
    end
  end

  defp enqueue_deferred_materializer_move(event, state) do
    event_bytes = deferred_event_size(event)
    attempted_bytes = state.deferred_event_bytes + event_bytes

    cond do
      state.deferred_materializer_move_count >= deferred_materializer_move_limit(state) ->
        state = handle_event_error(state, :buffer_overflow)
        {:error, state}

      attempted_bytes > deferred_event_memory_limit(state) ->
        state =
          handle_event_error(
            state,
            {:buffer_memory_overflow, attempted_bytes, deferred_event_memory_limit(state)}
          )

        {:error, state}

      true ->
        queue =
          (:queue.to_list(state.deferred_materializer_moves) ++ [{event, event_bytes}])
          |> sort_deferred_materializer_batches()
          |> :queue.from_list()

        replay_lookahead? =
          state.materializer_replay_lookahead? or
            match?({:materializer_replay, _dep_handle, _payload}, event)

        state =
          state
          |> Effects.acquire_global_lsn_subscription(:causal_barrier)
          |> Map.merge(%{
            deferred_materializer_moves: queue,
            deferred_materializer_move_count: state.deferred_materializer_move_count + 1,
            deferred_event_bytes: attempted_bytes,
            materializer_barrier_active?: true,
            materializer_replay_lookahead?: replay_lookahead?
          })

        {:ok, state}
    end
  end

  defp defer_replication_event(event, trace_context, state) do
    event_bytes = deferred_event_size({event, trace_context})
    attempted_bytes = state.deferred_event_bytes + event_bytes

    cond do
      state.deferred_replication_event_count >= deferred_materializer_move_limit(state) ->
        state = handle_event_error(state, :buffer_overflow)
        {:reply, :ok, state, {:continue, :stop_and_clean}}

      attempted_bytes > deferred_event_memory_limit(state) ->
        state =
          handle_event_error(
            state,
            {:buffer_memory_overflow, attempted_bytes, deferred_event_memory_limit(state)}
          )

        {:reply, :ok, state, {:continue, :stop_and_clean}}

      true ->
        queue = :queue.in({event, trace_context, event_bytes}, state.deferred_replication_events)

        state = %{
          state
          | deferred_replication_events: queue,
            deferred_replication_event_count: state.deferred_replication_event_count + 1,
            deferred_event_bytes: attempted_bytes
        }

        {:reply, :ok, state, next_after_move(state)}
    end
  end

  defp deferred_event_size(term), do: :erlang.external_size(term)

  defp reserve_materializer_batches(state, _dependency_handle, []), do: {:ok, state}

  defp reserve_materializer_batches(state, dependency_handle, causal_reservations) do
    {reservations, _new_tokens} =
      Enum.map_reduce(causal_reservations, MapSet.new(), fn
        {causal_token, expected_resolution_bytes}, new_tokens ->
          offset = Materializer.causal_token_offset(causal_token)

          if materializer_reservation_exists?(state, dependency_handle, causal_token) or
               MapSet.member?(new_tokens, causal_token) do
            raise ShapeCache.Storage.Error,
              message:
                "duplicate materializer causal reservation for #{state.shape_handle} from #{dependency_handle}"
          end

          downstream_token = new_downstream_causal_token(state, causal_token)

          reservation =
            {:reserved_materializer_batch, dependency_handle, offset, causal_token,
             downstream_token, nil}

          charged_bytes =
            expected_resolution_bytes + deferred_event_size(reservation)

          {{reservation, charged_bytes}, MapSet.put(new_tokens, causal_token)}
      end)

    attempted_count = state.deferred_materializer_move_count + length(reservations)
    reservation_bytes = Enum.reduce(reservations, 0, fn {_entry, bytes}, acc -> acc + bytes end)
    attempted_bytes = state.deferred_event_bytes + reservation_bytes

    cond do
      attempted_count > deferred_materializer_move_limit(state) ->
        Logger.warning("Subquery pending materializer batch count exceeded",
          shape_handle: state.shape_handle,
          dependency_shape_handle: dependency_handle,
          attempted_count: attempted_count,
          limit_count: deferred_materializer_move_limit(state)
        )

        {:error, :count_limit, state}

      attempted_bytes > deferred_event_memory_limit(state) ->
        Logger.warning("Subquery pending materializer batch memory exceeded",
          shape_handle: state.shape_handle,
          dependency_shape_handle: dependency_handle,
          attempted_bytes: attempted_bytes,
          limit_bytes: deferred_event_memory_limit(state)
        )

        {:error, :memory_limit, state}

      true ->
        state = Effects.acquire_global_lsn_subscription(state, :causal_barrier)

        earliest_tx_offset =
          Enum.min_by(reservations, fn
            {{:reserved_materializer_batch, _dependency_handle, %LogOffset{tx_offset: tx_offset},
              _causal_token, _downstream_token, nil}, _bytes} ->
              tx_offset
          end)
          |> then(fn
            {{:reserved_materializer_batch, _dependency_handle, %LogOffset{tx_offset: tx_offset},
              _causal_token, _downstream_token, nil}, _bytes} ->
              tx_offset
          end)

        :ok =
          ConsumerRegistry.mark_causal_work_created(state.stack_id, earliest_tx_offset)

        # Reserve the same causal slot through every dependent materializer only
        # after this Consumer has proved it can retain its own bounded slot.
        # The upstream Materializer is synchronously waiting on this call, so no
        # payload can overtake the reservations before they are installed.
        Enum.each(reservations, fn
          {{:reserved_materializer_batch, _dependency_handle, _offset, _causal_token,
            downstream_token, nil}, _bytes} ->
            forward_causal_begin(state, downstream_token)
        end)

        queue =
          state.deferred_materializer_moves
          |> :queue.to_list()
          |> Kernel.++(reservations)
          |> sort_deferred_materializer_batches()
          |> :queue.from_list()

        {:ok,
         %{
           state
           | deferred_materializer_moves: queue,
             deferred_materializer_move_count: attempted_count,
             deferred_event_bytes: attempted_bytes,
             materializer_barrier_active?: true
         }}
    end
  end

  defp prepare_reserved_materializer_batch(
         state,
         dependency_handle,
         causal_token,
         expected_resolution_bytes
       ) do
    entries = :queue.to_list(state.deferred_materializer_moves)

    case resize_first_materializer_batch_reservation(
           entries,
           dependency_handle,
           causal_token,
           expected_resolution_bytes
         ) do
      {:ok, entries, previous_bytes, prepared_bytes} ->
        attempted_bytes = state.deferred_event_bytes - previous_bytes + prepared_bytes

        if attempted_bytes > deferred_event_memory_limit(state) do
          {:error, :memory_limit, state}
        else
          {:ok,
           %{
             state
             | deferred_materializer_moves: :queue.from_list(entries),
               deferred_event_bytes: attempted_bytes
           }}
        end

      :error ->
        :unreserved
    end
  end

  defp resize_first_materializer_batch_reservation(
         entries,
         dependency_handle,
         causal_token,
         expected_resolution_bytes
       ) do
    {entries, result} =
      Enum.map_reduce(entries, :not_found, fn
        {{:reserved_materializer_batch, ^dependency_handle, offset, ^causal_token,
          downstream_token, nil} = reservation, previous_bytes},
        :not_found ->
          prepared_bytes =
            max(previous_bytes, expected_resolution_bytes + deferred_event_size(reservation))

          {
            {{:reserved_materializer_batch, dependency_handle, offset, causal_token,
              downstream_token, nil}, prepared_bytes},
            {:found, previous_bytes, prepared_bytes}
          }

        entry, result ->
          {entry, result}
      end)

    case result do
      {:found, previous_bytes, prepared_bytes} ->
        {:ok, entries, previous_bytes, prepared_bytes}

      :not_found ->
        :error
    end
  end

  defp fill_reserved_materializer_batch(state, dependency_handle, payload, event) do
    case Map.get(payload, :causal_token) do
      {:causal_batch, ref, %LogOffset{}, depth} = causal_token
      when is_reference(ref) and is_integer(depth) and depth >= 0 ->
        fill_reserved_materializer_resolution(
          state,
          dependency_handle,
          causal_token,
          event
        )

      _ ->
        :unreserved
    end
  end

  defp fill_reserved_materializer_end(state, dependency_handle, causal_token) do
    case fill_reserved_materializer_resolution(
           state,
           dependency_handle,
           causal_token,
           :causal_end
         ) do
      {:error, _state} -> :unreserved
      result -> result
    end
  end

  defp fill_reserved_materializer_resolution(
         state,
         dependency_handle,
         causal_token,
         resolution
       ) do
    entries = :queue.to_list(state.deferred_materializer_moves)

    case fill_first_materializer_batch_reservation(
           entries,
           dependency_handle,
           causal_token,
           resolution
         ) do
      {:ok, entries} ->
        {:ok, %{state | deferred_materializer_moves: :queue.from_list(entries)}}

      {:too_large, actual_bytes, reserved_bytes} ->
        Logger.warning("Materializer causal payload exceeded its prepared reservation",
          shape_handle: state.shape_handle,
          dependency_shape_handle: dependency_handle,
          actual_bytes: actual_bytes,
          reserved_bytes: reserved_bytes
        )

        {:error, mark_for_removal(state)}

      :error ->
        :unreserved
    end
  end

  defp fill_first_materializer_batch_reservation(
         entries,
         dependency_handle,
         causal_token,
         resolution
       ) do
    {entries, result} =
      Enum.map_reduce(entries, :not_found, fn
        {{:reserved_materializer_batch, ^dependency_handle, offset, ^causal_token,
          downstream_token, nil}, reserved_bytes},
        :not_found ->
          resolved_entry =
            {:reserved_materializer_batch, dependency_handle, offset, causal_token,
             downstream_token, resolution}

          actual_bytes = deferred_event_size(resolved_entry)

          if actual_bytes <= reserved_bytes do
            {{resolved_entry, reserved_bytes}, :found}
          else
            {{resolved_entry, reserved_bytes}, {:too_large, actual_bytes, reserved_bytes}}
          end

        entry, result ->
          {entry, result}
      end)

    case result do
      :found ->
        {:ok, entries}

      {:too_large, actual_bytes, reserved_bytes} ->
        {:too_large, actual_bytes, reserved_bytes}

      :not_found ->
        :error
    end
  end

  # Materializer replay, live dependency moves, and forwarded causal
  # reservations share one scheduler. Ordering only the reservations lets a
  # later live move overtake an earlier replay item after restart.
  defp sort_deferred_materializer_batches(entries) do
    Enum.sort(entries, &materializer_batch_before?/2)
  end

  defp materializer_batch_before?(left, right) do
    case {materializer_batch_offset(left), materializer_batch_offset(right)} do
      {%LogOffset{} = left_offset, %LogOffset{} = right_offset} ->
        compare_materializer_batch_positions(left, left_offset, right, right_offset)

      {%LogOffset{}, nil} ->
        true

      {nil, %LogOffset{}} ->
        false

      {nil, nil} ->
        materializer_batch_tiebreaker(left) <= materializer_batch_tiebreaker(right)
    end
  end

  defp compare_materializer_batch_positions(left, left_offset, right, right_offset) do
    left_depth = materializer_batch_causal_depth(left)
    right_depth = materializer_batch_causal_depth(right)

    cond do
      left_offset.tx_offset < right_offset.tx_offset ->
        true

      left_offset.tx_offset > right_offset.tx_offset ->
        false

      left_depth < right_depth ->
        true

      left_depth > right_depth ->
        false

      true ->
        case LogOffset.compare(left_offset, right_offset) do
          :lt -> true
          :gt -> false
          :eq -> materializer_batch_tiebreaker(left) <= materializer_batch_tiebreaker(right)
        end
    end
  end

  defp materializer_batch_causal_depth(
         {{:reserved_materializer_batch, _handle, _offset, token, _downstream, _resolution},
          _bytes}
       ),
       do: Materializer.causal_token_depth(token)

  defp materializer_batch_causal_depth({{kind, _handle, payload}, _bytes})
       when kind in [:materializer_replay, :materializer_changes],
       do: Map.get(payload, :causal_depth, 0)

  defp materializer_batch_causal_depth(_entry), do: 0

  defp materializer_batch_tiebreaker({{:materializer_replay, handle, _payload}, _bytes}),
    do: {handle, 0, nil}

  defp materializer_batch_tiebreaker({{:materializer_changes, handle, _payload}, _bytes}),
    do: {handle, 1, nil}

  defp materializer_batch_tiebreaker(
         {{:reserved_materializer_batch, handle, _offset, token, _downstream, _resolution},
          _bytes}
       ),
       do: {handle, 2, token}

  defp materializer_reservation_exists?(state, dependency_handle, causal_token) do
    state.deferred_materializer_moves
    |> :queue.to_list()
    |> Enum.any?(fn
      {{:reserved_materializer_batch, ^dependency_handle, _offset, ^causal_token,
        _downstream_token, _resolution}, _bytes} ->
        true

      _entry ->
        false
    end)
  end

  defp new_downstream_causal_token(%{materializer_subscribed?: false}, _causal_token), do: nil

  defp new_downstream_causal_token(%State{}, causal_token) do
    Materializer.new_causal_token(
      Materializer.causal_token_offset(causal_token),
      Materializer.causal_token_depth(causal_token) + 1
    )
  end

  defp forward_causal_begin(_state, nil), do: :ok

  defp forward_causal_begin(%State{} = state, token),
    do:
      Materializer.forward_causal_begin(
        materializer_ref(state),
        token,
        materializer_causal_call_timeout(state)
      )

  defp materializer_ref(%State{} = state),
    do: Map.take(state, [:stack_id, :shape_handle])

  defp materializer_causal_call_timeout(%State{stack_id: stack_id}) do
    Electric.StackConfig.lookup(
      stack_id,
      :materializer_causal_call_timeout_ms,
      Electric.Config.default(:materializer_causal_call_timeout_ms)
    )
  end

  defp deferred_event_memory_limit(%State{stack_id: stack_id}) do
    Electric.StackConfig.lookup(
      stack_id,
      :subquery_deferred_event_memory_limit_bytes,
      Electric.Config.default(:subquery_deferred_event_memory_limit_bytes)
    )
  end

  defp deferred_materializer_move_limit(%State{
         event_handler: %{shape_info: %{buffer_max_transactions: limit}}
       }),
       do: limit

  defp deferred_materializer_move_limit(%State{event_handler: nil, stack_id: stack_id}) do
    Electric.StackConfig.lookup(
      stack_id,
      :subquery_buffer_max_transactions,
      Electric.Config.default(:subquery_buffer_max_transactions)
    )
  end

  defp materializer_subscription_blocked?(%State{} = state) do
    not is_nil(state.pending_initialization) or not is_nil(state.pending_txn) or
      state.move_transaction_open? or state.pending_materializer_replay_count > 0 or
      state.deferred_materializer_move_count > 0 or
      state.deferred_replication_event_count > 0 or state.materializer_barrier_active? or
      not is_nil(state.pending_global_last_seen_lsn) or
      not is_nil(state.active_downstream_causal_token) or
      not is_nil(state.completed_downstream_causal_token) or
      not move_pipeline_fully_drained?(state.event_handler)
  end

  defp activate_materializer_subscription(pid, %State{} = state) do
    writer = ShapeCache.Storage.hibernate(state.writer)
    {:ok, storage_offset} = ShapeCache.Storage.fetch_latest_offset(state.storage)

    if LogOffset.is_real_offset(state.latest_offset) and
         LogOffset.compare(storage_offset, state.latest_offset) == :lt do
      raise ShapeCache.Storage.Error,
        message:
          "could not flush source shape #{state.shape_handle} to its latest committed offset " <>
            "before materializer subscription: latest=#{inspect(state.latest_offset)} " <>
            "durable=#{inspect(storage_offset)}"
    end

    # Snapshot creation can advance storage's physical virtual-chunk offset
    # after the Consumer initialized. On first creation the live cursor remains
    # `0_infinity`, while restore reads the final physical chunk as `0_N`; both
    # mean "the complete initial snapshot". Persist one canonical logical
    # boundary so a strict restored subscription is neither falsely ahead nor
    # tied to storage chunking. Real log positions remain exact.
    durable_offset =
      if LogOffset.is_real_offset(state.latest_offset),
        do: state.latest_offset,
        else: LogOffset.last_before_real_offsets()

    Process.monitor(pid, tag: :materializer_down)

    state = %{
      state
      | writer: writer,
        durable_offset: durable_offset,
        materializer_subscribed?: true,
        pending_materializer_subscription: nil
    }

    {{:ok, durable_offset}, state}
  end

  defp maybe_activate_materializer_subscription(
         %State{pending_materializer_subscription: nil} = state
       ),
       do: state

  defp maybe_activate_materializer_subscription(%State{} = state) do
    if materializer_subscription_blocked?(state) do
      state
    else
      {from, pid} = state.pending_materializer_subscription
      {reply, state} = activate_materializer_subscription(pid, state)
      GenServer.reply(from, reply)
      state
    end
  end

  defp maybe_release_materializer_barrier(%State{} = state) do
    barrier_active? =
      state.move_transaction_open? or state.pending_materializer_replay_count > 0 or
        state.deferred_materializer_move_count > 0 or
        not move_pipeline_fully_drained?(state.event_handler)

    %{state | materializer_barrier_active?: barrier_active?}
  end

  defp maybe_mark_restore_ready(%State{restore_ready?: true} = state), do: state

  defp maybe_mark_restore_ready(%State{} = state) do
    restore_drained? =
      is_nil(state.pending_initialization) and
        is_nil(state.pending_dependency_subscription) and
        state.pending_materializer_replay_count == 0 and
        :queue.is_empty(state.pending_materializer_replays) and
        state.deferred_materializer_move_count == 0 and
        :queue.is_empty(state.deferred_materializer_moves) and
        state.deferred_replication_event_count == 0 and
        :queue.is_empty(state.deferred_replication_events) and
        is_nil(state.pending_global_last_seen_lsn) and
        is_nil(state.pending_txn) and
        not state.move_transaction_open? and
        not state.materializer_barrier_active? and
        is_nil(state.active_downstream_causal_token) and
        is_nil(state.completed_downstream_causal_token) and
        state.pending_move_lsns == %{} and
        MapSet.size(state.global_lsn_subscription_reasons) == 0 and
        not is_nil(state.event_handler) and
        move_pipeline_fully_drained?(state.event_handler)

    if restore_drained? do
      state = %{state | restore_ready?: true}

      if is_snapshot_started(state),
        do: State.reply_to_snapshot_waiters(state, :started),
        else: state
    else
      state
    end
  end

  defp consumer_suspend_enabled?(%{stack_id: stack_id}) do
    Electric.StackConfig.lookup(stack_id, :shape_enable_suspend?, true)
  end

  defp consumer_can_suspend?(state) do
    is_snapshot_started(state) and not Shape.has_dependencies(state.shape) and
      not state.materializer_subscribed? and is_nil(state.pending_txn)
  end

  defp schedule_suspend_timer(%{suspend_after: nil} = state), do: state

  defp schedule_suspend_timer(%{suspend_after: suspend_after} = state) do
    ref = :erlang.send_after(suspend_after, self(), :suspend_timeout)
    %{state | suspend_timer: ref}
  end

  defp cancel_suspend_timer(%{suspend_timer: ref} = state) do
    :erlang.cancel_timer(ref)
    %{state | suspend_timer: nil}
  end

  @impl GenServer
  def terminate(reason, state) do
    Logger.debug(fn ->
      case reason do
        {error, stacktrace} when is_tuple(error) and is_list(stacktrace) ->
          "Shapes.Consumer terminating with reason: #{Exception.format(:error, error, stacktrace)}"

        other ->
          "Shapes.Consumer terminating with reason: #{inspect(other)}"
      end
    end)

    state = fail_causal_drain_waiters(state)

    # always need to terminate writer to remove the writer ets (which belongs
    # to this process). leads to unecessary writes in the case of a deleted
    # shape but the alternative is leaking ets tables.
    state = terminate_writer(state)

    ShapeCleaner.handle_writer_termination(state.stack_id, state.shape_handle, reason)

    State.reply_to_snapshot_waiters(state, {:error, "Shape terminated before snapshot was ready"})
  end

  # Any relation that gets let through by the `ShapeLogCollector` (as coupled with `Shapes.Dispatcher`)
  # is a signal that we need to terminate the shape.
  defp handle_event(%Changes.Relation{}, state) do
    %{shape: %Shape{root_table_id: root_table_id, root_table: root_table}} = state

    Logger.notice(
      "Schema for the table #{Utils.inspect_relation(root_table)} changed - terminating shape #{state.shape_handle}"
    )

    # We clean up the relation info from ETS as it has changed and we want
    # to source the fresh info from postgres for the next shape creation
    clean_table(root_table_id, state)

    state
    |> State.reply_to_snapshot_waiters(
      {:error, "Shape relation changed before snapshot was ready"}
    )
    |> mark_for_removal()
  end

  defp handle_event({:global_last_seen_lsn, _lsn} = event, state) do
    {:global_last_seen_lsn, lsn} = event

    case apply_global_lsn(state, lsn) do
      {:error, reason} ->
        handle_event_error(state, reason)

      {state, notification, _num_changes, _total_size} ->
        if notification do
          :ok = notify_new_changes(state, notification)
        end

        state
    end
  end

  defp handle_event(%TransactionFragment{} = txn_fragment, state) do
    Logger.debug(fn -> "Txn fragment received in Shapes.Consumer: #{inspect(txn_fragment)}" end)
    handle_txn_fragment(txn_fragment, state)
  end

  # Adaptive GC check. Always invoked from a {:continue, :maybe_gc} handler so the
  # forced full sweep runs off the ShapeLogCollector's synchronous publish path: the
  # SLC has already received :ok by the time this runs. Because the SLC publishes
  # fragments to a given consumer sequentially, the continue typically completes
  # before the next fragment arrives, so it does not block steady-state throughput.

  # Fast path: adaptive GC is disabled — skip all process_info/time calls.
  defp maybe_garbage_collect(%State{gc_heap_threshold: nil} = state), do: state

  defp maybe_garbage_collect(%State{gc_heap_threshold: threshold_bytes} = state) do
    {:total_heap_size, heap_words} = :erlang.process_info(self(), :total_heap_size)
    heap_bytes = heap_words * @word_size
    now = System.monotonic_time(:millisecond)

    if should_force_gc?(heap_bytes, threshold_bytes, state.last_forced_gc_at, now) do
      :erlang.garbage_collect()
      %{state | last_forced_gc_at: now}
    else
      state
    end
  end

  @doc false
  # Decide whether to force a full GC sweep: heap (bytes) must be over the
  # threshold (bytes) AND at least @gc_min_interval_ms must have elapsed since the
  # last forced GC. last_gc_at / now_ms are monotonic milliseconds; last_gc_at is
  # nil if this consumer has never forced a GC (always fire on first over-threshold
  # event). Passing explicit min_interval_ms enables deterministic unit tests.
  @spec should_force_gc?(
          non_neg_integer(),
          non_neg_integer() | nil,
          integer() | nil,
          integer(),
          non_neg_integer()
        ) :: boolean()
  def should_force_gc?(
        heap_bytes,
        threshold_bytes,
        last_gc_at,
        now_ms,
        min_interval_ms \\ @gc_min_interval_ms
      )

  def should_force_gc?(_heap_bytes, nil, _last_gc_at, _now_ms, _min_interval_ms), do: false

  def should_force_gc?(heap_bytes, threshold_bytes, last_gc_at, now_ms, min_interval_ms) do
    heap_bytes > threshold_bytes and
      (is_nil(last_gc_at) or now_ms - last_gc_at >= min_interval_ms)
  end

  # A consumer process starts with buffering?=true before it has PG snapshot info (xmin, xmax, xip_list).
  # In this phase we have to buffer incoming txn fragments because we can't yet decide what to
  # do with the transaction: skip it or write it to the shape log.
  #
  # When snapshot info arrives, `process_buffered_txn_fragments/1` will be called to process
  # buffered fragments in order.
  defp handle_txn_fragment(
         %TransactionFragment{} = txn_fragment,
         %State{buffering?: true} = state
       ) do
    State.add_to_buffer(state, txn_fragment)
  end

  # Dependency state and the collector's positive/negative root-delivery
  # frontier are committed atomically. A persistent slot may nevertheless
  # replay those PostgreSQL transactions after a crash; never evaluate them
  # again against the already-advanced dependency view.
  defp handle_txn_fragment(
         %TransactionFragment{
           last_log_offset: %LogOffset{tx_offset: tx_offset}
         } = txn_fragment,
         %State{root_delivery_tx_offset: root_delivery_tx_offset} = state
       )
       when tx_offset <= root_delivery_tx_offset do
    skip_txn_fragment(state, txn_fragment)
  end

  # Skip transactions already applied and persisted (e.g. replayed from the persistent
  # replication slot on restart) - ones at or below `latest_offset`.
  #
  # Storage restores `latest_offset` at a committed shape boundary and the
  # replication slot replays whole PostgreSQL transactions. Within that
  # transaction, however, the final shape-visible operation can precede later
  # filtered source operations. This clause skips fragments wholly at-or-below
  # the shape cursor; the following clause handles higher operation offsets that
  # share its real transaction offset.
  defp handle_txn_fragment(%TransactionFragment{last_log_offset: offset} = txn_fragment, state)
       when LogOffset.is_log_offset_lte(offset, state.latest_offset) do
    skip_txn_fragment(state, txn_fragment)
  end

  # Storage persists the final shape-visible row as `latest_offset`. If the
  # remaining source fragments only contain filtered changes, their operation
  # offsets can be greater even though they belong to the same already-applied
  # PostgreSQL transaction. Once an earlier fragment was skipped there is no
  # pending transaction, so skip the rest of that real transaction by its
  # shared transaction offset as well.
  defp handle_txn_fragment(
         %TransactionFragment{last_log_offset: %LogOffset{tx_offset: tx_offset}} = txn_fragment,
         %State{
           pending_txn: nil,
           latest_offset: %LogOffset{tx_offset: tx_offset} = latest_offset
         } = state
       )
       when LogOffset.is_real_offset(latest_offset) do
    skip_txn_fragment(state, txn_fragment)
  end

  # Short-circuit clauses for the most common case of a single-fragment transaction
  defp handle_txn_fragment(%TransactionFragment{} = txn_fragment, state)
       when TransactionFragment.complete_transaction?(txn_fragment) and
              needs_initial_filtering(state) do
    case InitialSnapshot.filter(state.initial_snapshot_state, state.storage, txn_fragment.xid) do
      {:consider_flushed, initial_snapshot_state} ->
        # This transaction is already included in the snapshot, flush it immediately and skip
        # writing it to the shape log.
        state = %{state | initial_snapshot_state: initial_snapshot_state}
        consider_flushed(state, txn_fragment.last_log_offset)

      {:continue, initial_snapshot_state} ->
        # The transaction is not part of the initial snapshot.
        state = %{state | initial_snapshot_state: initial_snapshot_state}
        build_and_handle_txn(txn_fragment, state)
    end
  end

  defp handle_txn_fragment(%TransactionFragment{} = txn_fragment, state)
       when TransactionFragment.complete_transaction?(txn_fragment) do
    build_and_handle_txn(txn_fragment, state)
  end

  # pending_txn struct is initialized to keep track of all fragments comprising this txn and
  # store the "consider_flushed" state on it.
  defp handle_txn_fragment(
         %TransactionFragment{has_begin?: true, xid: xid} = txn_fragment,
         %State{pending_txn: nil} = state
       ) do
    txn = PendingTxn.new(xid)
    state = %{state | pending_txn: txn}
    handle_txn_fragment(txn_fragment, state)
  end

  # Upon seeing the first fragment of a new transaction, check if its xid is already included in the
  # initial snapshot. If it is, all subsequent fragments of this transaction will be ignored.
  #
  # Initial filtering is giving us the advantage of not accumulating fragments for a
  # transaction that is going to be skipped anyway. This works for any value of state.write_unit.
  defp handle_txn_fragment(
         %TransactionFragment{has_begin?: true, xid: xid} = txn_fragment,
         %State{} = state
       )
       when needs_initial_filtering(state) do
    state =
      case InitialSnapshot.filter(state.initial_snapshot_state, state.storage, xid) do
        {:consider_flushed, initial_snapshot_state} ->
          # This transaction is already included in the snapshot, so mark it as flushed to
          # ignore any of its follow-up fragments.
          %{
            state
            | pending_txn: PendingTxn.consider_flushed(state.pending_txn),
              initial_snapshot_state: initial_snapshot_state
          }

        {:continue, initial_snapshot_state} ->
          # The transaction is not part of the initial snapshot.
          %{state | initial_snapshot_state: initial_snapshot_state}
      end

    process_txn_fragment(txn_fragment, state)
  end

  defp handle_txn_fragment(txn_fragment, state), do: process_txn_fragment(txn_fragment, state)

  # Defensive: this is only ever reached with a `pending_txn` set — a transaction's
  # BEGIN fragment creates it (see the `has_begin?` clauses above) before any fragment
  # gets here. The only way to arrive with `pending_txn: nil` is a transaction that
  # straddled `latest_offset`: its BEGIN fragment skipped by the already-applied clause
  # while a later fragment was not. The commit-aligned restore + whole-transaction replay
  # invariant (documented on that clause) rules this out; fail loudly with a clear message
  # rather than crash on `nil.consider_flushed?` if it is ever violated.
  defp process_txn_fragment(%TransactionFragment{last_log_offset: offset}, %State{
         pending_txn: nil
       }) do
    raise "consumer received a transaction fragment at #{inspect(offset)} with no pending " <>
            "transaction — a transaction straddling latest_offset was partially skipped, " <>
            "which should be impossible"
  end

  defp process_txn_fragment(
         %TransactionFragment{} = txn_fragment,
         %State{pending_txn: txn} = state
       ) do
    cond do
      not is_nil(txn.last_fragment_offset) and
          LogOffset.compare(txn_fragment.last_log_offset, txn.last_fragment_offset) != :gt ->
        state

      # Fragments of a transaction whose xid is already in the initial snapshot are
      # skipped here. (Offset-based dedup of replayed transactions is handled earlier,
      # in `handle_txn_fragment/2`.)
      txn.consider_flushed? ->
        skip_txn_fragment(state, txn_fragment)

      # With write_unit=txn all fragments are buffered until the Commit change is seen. At that
      # point, a transaction struct is produced from the buffered fragments and is written to
      # storage.
      is_write_unit_txn(state.write_unit) ->
        {txns, transaction_builder} =
          TransactionBuilder.build(txn_fragment, state.transaction_builder)

        state = %{state | transaction_builder: transaction_builder}

        case txns do
          [] ->
            state

          [txn] ->
            Logger.debug(fn -> "Txn assembled in Shapes.Consumer: #{inspect(txn)}" end)
            handle_txn(txn, %{state | pending_txn: nil})
        end

      true ->
        # If we've ended up in this branch, we know for sure that the current fragment is only
        # one of two or more for the current transaction.
        state
        |> write_txn_fragment_to_storage(txn_fragment)
        |> maybe_complete_pending_txn(txn_fragment)
    end
  end

  defp skip_txn_fragment(state, %TransactionFragment{commit: nil}), do: state

  # The last fragment of the currently pending transaction.
  defp skip_txn_fragment(state, %TransactionFragment{} = txn_fragment) do
    %{state | pending_txn: nil}
    |> consider_flushed(txn_fragment.last_log_offset)
    |> clear_pending_flush_offset()
  end

  # This function does similar things to do_handle_txn/2 but with the following simplifications:
  #   - it doesn't account for move-ins or move-outs or converting update operations into insert/delete
  #   - the fragment is written directly to storage if it has changes matching this shape
  #   - if the fragment has a commit message, the ShapeLogCollector is informed about the new flush boundary
  defp write_txn_fragment_to_storage(
         state,
         %TransactionFragment{changes: changes, xid: xid} = fragment
       ) do
    %{
      shape: shape,
      stack_id: stack_id,
      shape_handle: shape_handle
    } = state

    case convert_fragment_changes(changes, stack_id, shape_handle, shape) do
      :includes_truncate ->
        handle_txn_with_truncate(xid, state)

      {reversed_changes, 0} ->
        Logger.debug(fn ->
          "No relevant changes found for #{inspect(shape)} in txn fragment of txn #{xid}"
        end)

        write_converted_fragment_changes(state, Enum.reverse(reversed_changes), fragment)

      {reversed_changes, _num_changes, _last_log_offset} ->
        write_converted_fragment_changes(state, Enum.reverse(reversed_changes), fragment)
    end
  end

  defp write_converted_fragment_changes(
         %State{pending_txn: txn} = state,
         converted_changes,
         %TransactionFragment{xid: xid, commit: commit, last_log_offset: fragment_offset}
       ) do
    {changes_to_write, pending_changes} =
      split_fragment_changes(txn.pending_changes ++ converted_changes, commit)

    txn = %{
      txn
      | pending_changes: pending_changes,
        last_fragment_offset: fragment_offset
    }

    case changes_to_write do
      [] ->
        %{state | pending_txn: txn}

      changes_to_write ->
        timestamp = System.monotonic_time()
        {lines, total_size} = prepare_log_entries(changes_to_write, xid, state.shape)
        writer = ShapeCache.Storage.append_fragment_to_log!(lines, state.writer)
        {last_log_offset, _key, _operation, _json} = List.last(lines)

        # The Materializer must see all transaction changes for correct tracking
        # of move-ins and move-outs. The final empty notification in
        # maybe_complete_pending_txn/2 publishes the accumulated events.
        :ok =
          notify_materializer_of_new_changes(state, changes_to_write,
            commit: false,
            xid: xid,
            end_offset: last_log_offset
          )

        txn =
          PendingTxn.update_with_changes(
            txn,
            System.monotonic_time() - timestamp,
            length(lines),
            total_size
          )

        %{state | writer: writer, latest_offset: last_log_offset, pending_txn: txn}
    end
  end

  defp split_fragment_changes([], _commit), do: {[], []}

  defp split_fragment_changes(changes, nil) do
    {last, preceding} = List.pop_at(changes, -1)
    {preceding, [last]}
  end

  defp split_fragment_changes(changes, _commit) do
    {last, preceding} = List.pop_at(changes, -1)
    {preceding ++ [%{last | last?: true}], []}
  end

  defp convert_fragment_changes(changes, stack_id, shape_handle, shape, extra_refs \\ nil) do
    Enum.reduce_while(changes, {[], 0}, fn
      %Changes.TruncatedRelation{}, _acc ->
        {:halt, :includes_truncate}

      change, {changes, count} = acc ->
        # Apply Shape.convert_change to each change to:
        # 1. Filter out changes not matching the shape's table
        # 2. Apply WHERE clause filtering
        case Shape.convert_change(shape, change,
               stack_id: stack_id,
               shape_handle: shape_handle,
               extra_refs: extra_refs
             ) do
          [] ->
            {:cont, acc}

          [change] ->
            {:cont, {[change | changes], count + 1}}
        end
    end)
    |> case do
      {[change | _] = changes, num_changes} ->
        {changes, num_changes, LogItems.expected_offset_after_split(change)}

      acc ->
        acc
    end
  end

  defp maybe_complete_pending_txn(%State{} = state, %TransactionFragment{commit: nil}),
    do: state

  defp maybe_complete_pending_txn(%State{terminating?: true} = state, _fragment) do
    # If we're terminating (e.g., due to truncate), don't complete the transaction
    state
  end

  defp maybe_complete_pending_txn(%State{} = state, txn_fragment) do
    %{pending_txn: txn, writer: writer} = state

    # Only notify if we actually wrote changes
    if txn.num_changes > 0 do
      # The held-back final shape row was written with `last=true`, so storage
      # can publish the fragmented transaction without a synthetic marker.
      state = %{state | writer: ShapeCache.Storage.signal_txn_commit!(txn.xid, writer)}

      :ok = notify_new_changes_with_offset(state, [], state.latest_offset, xid: txn.xid)

      lag = calculate_replication_lag(txn_fragment.commit.commit_timestamp)

      OpenTelemetry.add_span_attributes(
        num_bytes: txn.total_bytes,
        actual_num_changes: txn.num_changes,
        replication_lag: lag
      )

      Electric.Telemetry.OpenTelemetry.execute(
        [:electric, :storage, :transaction_stored],
        %{
          duration: txn.storage_duration,
          bytes: txn.total_bytes,
          count: 1,
          operations: txn.num_changes,
          replication_lag: lag
        },
        Map.new(State.telemetry_attrs(state))
      )

      Logger.debug(fn ->
        "Processed the final fragment for transaction xid=#{txn.xid}, total_changes=#{txn.num_changes}"
      end)

      %{
        state
        | pending_txn: nil,
          txn_offset_mapping:
            state.txn_offset_mapping ++ [{state.latest_offset, txn_fragment.last_log_offset}]
      }
    else
      Logger.debug(fn ->
        "No relevant changes written in transaction xid=#{txn.xid}"
      end)

      %{state | pending_txn: nil}
      |> consider_flushed(txn_fragment.last_log_offset)
    end
    |> clear_pending_flush_offset()
  end

  def process_buffered_txn_fragments(%State{buffer: buffer} = state) do
    Logger.debug(fn -> "Consumer catching up on #{length(buffer)} transaction fragments" end)
    {txn_fragments, state} = State.pop_buffered(state)

    Enum.reduce_while(txn_fragments, state, fn txn_fragment, state ->
      state = handle_txn_fragment(txn_fragment, state)

      if state.terminating? do
        {:halt, state}
      else
        {:cont, state}
      end
    end)
  end

  defp build_and_handle_txn(%TransactionFragment{} = txn_fragment, %State{} = state) do
    {[txn], _} = TransactionBuilder.build(txn_fragment, TransactionBuilder.new())
    handle_txn(txn, state)
  end

  defp handle_txn(txn, %State{} = state) do
    ot_attrs =
      [xid: txn.xid, total_num_changes: txn.num_changes] ++ State.telemetry_attrs(state)

    OpenTelemetry.with_child_span(
      "shape_write.consumer.handle_txn",
      ot_attrs,
      state.stack_id,
      fn -> do_handle_txn(txn, state) end
    )
  end

  defp do_handle_txn(%Transaction{} = txn, state) do
    timestamp = System.monotonic_time()

    case apply_event(state, txn) do
      {:error, reason} ->
        handle_event_error(state, reason)

      {state, notification, num_changes, total_size} ->
        if notification do
          :ok = notify_new_changes(state, notification, xid: txn.xid)

          OpenTelemetry.add_span_attributes(%{
            num_bytes: total_size,
            actual_num_changes: num_changes
          })

          lag = calculate_replication_lag(txn.commit_timestamp)
          OpenTelemetry.add_span_attributes(replication_lag: lag)

          Electric.Telemetry.OpenTelemetry.execute(
            [:electric, :storage, :transaction_stored],
            %{
              duration: System.monotonic_time() - timestamp,
              bytes: total_size,
              count: 1,
              operations: num_changes,
              replication_lag: lag
            },
            Map.new(State.telemetry_attrs(state))
          )

          state
        else
          state
        end
    end
  end

  defp handle_apply_event_result(state, {:error, reason}) do
    state = handle_event_error(state, reason)
    {:noreply, state, {:continue, :stop_and_clean}}
  end

  defp handle_apply_event_result(_old_state, {state, notification, _num_changes, _total_size}) do
    causal_token = state.completed_downstream_causal_token

    if notification do
      opts = if is_nil(causal_token), do: [], else: [causal_token: causal_token]
      :ok = notify_new_changes(state, notification, opts)
    end

    state =
      state
      |> forward_completed_causal_token(causal_token, not is_nil(notification))
      |> Map.put(:completed_downstream_causal_token, nil)
      |> maybe_release_materializer_barrier()
      |> maybe_release_causal_lsn_subscription()
      |> maybe_activate_materializer_subscription()
      |> maybe_mark_restore_ready()

    {:noreply, state, next_after_move(state)}
  end

  defp next_after_move(state, fallback \\ nil) do
    state
    |> schedule_causal_drain_waiter_recheck()
    |> do_next_after_move(fallback)
  end

  defp schedule_causal_drain_waiter_recheck(%State{causal_drain_waiters: []} = state), do: state

  defp schedule_causal_drain_waiter_recheck(%State{} = state) do
    if Enum.any?(state.causal_drain_waiters, fn {_from, target_tx_offset, _waiter_ref} ->
         not causal_frontier_pending?(state, target_tx_offset)
       end) do
      send(self(), :recheck_causal_drain_waiters)
    end

    state
  end

  defp do_next_after_move(
         %State{
           pending_initialization: pending_initialization
         } = state,
         fallback
       )
       when not is_nil(pending_initialization),
       do: next_after_move_fallback(state, fallback)

  defp do_next_after_move(%State{pending_txn: pending_txn} = state, fallback)
       when not is_nil(pending_txn) do
    # A causal dependency fence may arrive between fragments of the same root
    # transaction. Keep consuming queued root fragments through commit, while
    # never allowing the causal continuation to split that transaction.
    case :queue.peek(state.deferred_replication_events) do
      :empty -> next_after_move_fallback(state, fallback)
      {:value, _event} -> {:continue, :process_deferred_replication_event}
    end
  end

  defp do_next_after_move(
         %State{
           move_transaction_open?: true,
           pending_move_causal_origin: %LogOffset{tx_offset: causal_tx_offset},
           deferred_replication_event_count: deferred_count,
           deferred_replication_events: deferred_events
         } = state,
         fallback
       )
       when deferred_count > 0 do
    # A root fragment at or before the active dependency's causal transaction
    # must be folded into that transaction before it commits. A later root can
    # overtake an intervening dependency only when the active query snapshot
    # already contains it; otherwise the active move must splice first and leave
    # the root queued for the next dependency view.
    case :queue.peek(deferred_events) do
      {:value, entry} ->
        case replication_event_offset(entry) do
          %LogOffset{tx_offset: root_tx_offset} when root_tx_offset <= causal_tx_offset ->
            {:continue, :process_deferred_replication_event}

          %LogOffset{} = root_offset ->
            case schedule_later_deferred_root(state, entry, root_offset) do
              :process ->
                {:continue, :process_deferred_replication_event}

              {:post_snapshot, xid} ->
                {:continue, {:mark_deferred_root_post_snapshot, xid}}

              :commit_move ->
                {:continue, :commit_move_before_deferred_root}

              {:invalid, reason} ->
                Logger.warning("Malformed deferred root replication event; invalidating shape",
                  shape_handle: state.shape_handle,
                  reason: reason,
                  root_offset: to_string(root_offset)
                )

                {:continue, :stop_and_clean}

              :wait ->
                next_after_move_fallback(state, fallback)
            end

          nil ->
            {:continue, :process_deferred_replication_event}
        end

      :empty ->
        next_after_move_fallback(state, fallback)
    end
  end

  defp do_next_after_move(
         %State{
           pending_global_last_seen_lsn: lsn,
           deferred_replication_event_count: 0,
           pending_txn: nil,
           pending_initialization: nil
         },
         _fallback
       )
       when is_integer(lsn),
       do: {:continue, :process_pending_global_lsn}

  defp do_next_after_move(%State{move_transaction_open?: true} = state, fallback),
    do: next_after_move_fallback(state, fallback)

  defp do_next_after_move(
         %State{
           pending_materializer_replay_count: count,
           materializer_replay_waiting?: true
         } = state,
         fallback
       )
       when count > 0,
       do: next_after_move_fallback(state, fallback)

  defp do_next_after_move(
         %State{
           pending_materializer_replay_count: count,
           materializer_replay_lookahead?: false,
           materializer_replay_waiting?: false
         },
         _fallback
       )
       when count > 0,
       do: {:continue, :process_materializer_replay}

  defp do_next_after_move(%State{} = state, fallback) do
    case next_deferred_work(state) do
      :materializer -> {:continue, :process_deferred_materializer_move}
      :replication -> {:continue, :process_deferred_replication_event}
      :wait -> state.hibernate_after
      :none -> next_after_move_fallback(state, fallback)
    end
  end

  defp next_after_move_fallback(%State{} = state, nil), do: state.hibernate_after
  defp next_after_move_fallback(%State{}, fallback), do: fallback

  defp reply_causal_drain_waiters(%State{causal_drain_waiters: []} = state), do: state

  defp reply_causal_drain_waiters(%State{} = state) do
    {ready, pending} =
      Enum.split_with(state.causal_drain_waiters, fn {_from, target_tx_offset, _waiter_ref} ->
        not causal_frontier_pending?(state, target_tx_offset)
      end)

    Enum.each(ready, fn {from, _target_tx_offset, waiter_ref} ->
      Process.demonitor(waiter_ref, [:flush])
      GenServer.reply(from, :ok)
    end)

    %{state | causal_drain_waiters: pending}
  end

  defp fail_causal_drain_waiters(%State{} = state) do
    Enum.each(state.causal_drain_waiters, fn {from, _target_tx_offset, waiter_ref} ->
      Process.demonitor(waiter_ref, [:flush])
      GenServer.reply(from, {:error, :consumer_stopped})
    end)

    %{state | causal_drain_waiters: []}
  end

  defp causal_frontier_pending?(%State{} = state, target_tx_offset) do
    # A consumer can be registered before its asynchronous initialization
    # message arrives. Dependency replay setup also discovers source offsets
    # asynchronously, so neither state has a safe target-specific answer yet.
    initialization_or_replay_unknown? =
      is_nil(state.event_handler) or not is_nil(state.pending_initialization) or
        not is_nil(state.pending_dependency_subscription) or
        state.pending_materializer_replay_count > 0 or
        not :queue.is_empty(state.pending_materializer_replays)

    deferred_at_or_before_target? =
      state.deferred_materializer_moves
      |> :queue.to_list()
      |> Enum.any?(fn entry ->
        causal_offset_at_or_before?(materializer_batch_offset(entry), target_tx_offset)
      end)

    active_at_or_before_target? =
      causal_token_at_or_before?(state.active_downstream_causal_token, target_tx_offset) or
        causal_token_at_or_before?(state.completed_downstream_causal_token, target_tx_offset)

    pending_move_at_or_before_target? =
      causal_offset_at_or_before?(state.pending_move_causal_origin, target_tx_offset)

    deferred_root_at_or_before_target? =
      state.deferred_replication_events
      |> :queue.to_list()
      |> Enum.any?(fn entry ->
        case replication_event_offset(entry) do
          %LogOffset{} = offset -> causal_offset_at_or_before?(offset, target_tx_offset)
          nil -> true
        end
      end)

    # A fragment currently being assembled has no target-comparable offset on
    # State. Fail closed until its commit is handled rather than letting a
    # startup waiter overtake an in-progress root transaction.
    root_transaction_in_progress? = not is_nil(state.pending_txn)

    pending_global_at_or_before_target? =
      is_integer(state.pending_global_last_seen_lsn) and
        state.pending_global_last_seen_lsn <= target_tx_offset

    # An open move transaction must always identify the PostgreSQL transaction
    # that caused it. Dependency-local replay cursors cannot answer a root
    # causal-cutoff query, so fail closed when that origin is unavailable.
    open_move_without_origin? =
      state.move_transaction_open? and is_nil(state.pending_move_causal_origin)

    state.terminating? or initialization_or_replay_unknown? or deferred_at_or_before_target? or
      active_at_or_before_target? or pending_move_at_or_before_target? or
      deferred_root_at_or_before_target? or root_transaction_in_progress? or
      pending_global_at_or_before_target? or open_move_without_origin?
  end

  defp causal_token_at_or_before?(nil, _target_tx_offset), do: false

  defp causal_token_at_or_before?(causal_token, target_tx_offset) do
    causal_token
    |> Materializer.causal_token_offset()
    |> causal_offset_at_or_before?(target_tx_offset)
  end

  defp causal_offset_at_or_before?(%LogOffset{tx_offset: tx_offset}, target_tx_offset),
    do: tx_offset <= target_tx_offset

  defp causal_offset_at_or_before?(_offset, _target_tx_offset), do: false

  defp next_deferred_work(state) do
    materializer = :queue.peek(state.deferred_materializer_moves)
    replication = :queue.peek(state.deferred_replication_events)

    case {materializer, replication} do
      {:empty, :empty} ->
        :none

      {:empty, {:value, _event}} ->
        :replication

      {{:value, materializer_entry}, :empty} ->
        if materializer_batch_runnable?(state, materializer_entry),
          do: :materializer,
          else: :wait

      {{:value, materializer_entry}, {:value, replication_entry}} ->
        case compare_deferred_offsets(
               materializer_batch_offset(materializer_entry),
               replication_event_offset(replication_entry)
             ) do
          :replication ->
            :replication

          :materializer ->
            # A queued root transaction after the dependency transaction is
            # itself proof that the collector completed the earlier commit,
            # even if its asynchronous global-LSN message has not run yet.
            if materializer_batch_resolved?(materializer_entry),
              do: :materializer,
              else: :wait
        end
    end
  end

  defp materializer_batch_resolved?(
         {{:reserved_materializer_batch, _dependency_handle, _offset, _token, _downstream_token,
           nil}, _bytes}
       ),
       do: false

  defp materializer_batch_resolved?(_entry), do: true

  defp materializer_batch_runnable?(state, materializer_entry) do
    materializer_batch_resolved?(materializer_entry) and
      materializer_replication_fence_satisfied?(state, materializer_entry)
  end

  defp materializer_replication_fence_satisfied?(
         state,
         materializer_entry
       ) do
    case materializer_batch_offset(materializer_entry) do
      %LogOffset{tx_offset: tx_offset} ->
        state.last_seen_global_lsn >= tx_offset or
          state.last_processed_replication_tx_offset >= tx_offset

      nil ->
        true
    end
  end

  defp materializer_batch_offset(
         {{:reserved_materializer_batch, _dependency_handle, %LogOffset{} = offset, _token,
           _downstream_token, _resolution}, _bytes}
       ),
       do: offset

  defp materializer_batch_offset(
         {{:materializer_changes, _dependency_handle, %{lsn: %LogOffset{} = local} = payload},
          _bytes}
       ) do
    Map.get(payload, :causal_origin, local)
  end

  defp materializer_batch_offset(
         {{:materializer_replay, _dependency_handle, %{lsn: %LogOffset{} = local} = payload},
          _bytes}
       ) do
    Map.get(payload, :causal_origin, local)
  end

  defp materializer_batch_offset(_entry), do: nil

  defp materializer_payload_causal_origin(payload) do
    Map.get(payload, :causal_origin, Map.get(payload, :lsn))
  end

  defp materializer_payload_causal_depth(payload) do
    if is_nil(materializer_payload_causal_origin(payload)),
      do: nil,
      else: Map.get(payload, :causal_depth, 0)
  end

  defp replication_event_offset({%TransactionFragment{last_log_offset: offset}, _ctx, _bytes}),
    do: offset

  defp replication_event_offset(_entry), do: nil

  defp replication_event_xid({%TransactionFragment{xid: xid}, _ctx, _bytes}), do: xid
  defp replication_event_xid(_entry), do: nil

  defp schedule_later_deferred_root(
         %State{event_handler: %Buffering{active_move: active_move}} = state,
         entry,
         %LogOffset{} = root_offset
       ) do
    if earlier_materializer_batch_pending?(state, root_offset) do
      cond do
        not is_nil(active_move.boundary_txn_count) ->
          :wait

        is_nil(active_move.snapshot) ->
          :wait

        is_nil(replication_event_xid(entry)) ->
          {:invalid, :missing_transaction_xid}

        Transaction.visible_in_snapshot?(replication_event_xid(entry), active_move.snapshot) ->
          :process

        true ->
          {:post_snapshot, replication_event_xid(entry)}
      end
    else
      :process
    end
  end

  defp schedule_later_deferred_root(
         %State{event_handler: %EventHandler.Subqueries.Steady{}} = state,
         _entry,
         %LogOffset{} = root_offset
       ) do
    if earlier_materializer_batch_pending?(state, root_offset) do
      if move_pipeline_fully_drained?(state.event_handler) and move_root_frontier_ready?(state),
        do: :commit_move,
        else: :wait
    else
      :process
    end
  end

  defp schedule_later_deferred_root(%State{}, _entry, %LogOffset{}), do: :process

  defp earlier_materializer_batch_pending?(%State{} = state, %LogOffset{} = root_offset) do
    state.deferred_materializer_moves
    |> :queue.to_list()
    |> Enum.any?(fn entry ->
      case materializer_batch_offset(entry) do
        %LogOffset{} = materializer_offset ->
          LogOffset.compare(materializer_offset, root_offset) == :lt

        nil ->
          false
      end
    end)
  end

  defp compare_deferred_offsets(%LogOffset{} = materializer, %LogOffset{} = replication) do
    if replication.tx_offset <= materializer.tx_offset,
      do: :replication,
      else: :materializer
  end

  defp compare_deferred_offsets(_materializer, nil), do: :replication
  defp compare_deferred_offsets(nil, %LogOffset{}), do: :materializer

  defp record_processed_replication_event(
         %State{} = state,
         %TransactionFragment{
           commit: %Changes.Commit{},
           last_log_offset: %LogOffset{tx_offset: tx_offset}
         }
       ) do
    %{
      state
      | last_processed_replication_tx_offset:
          max(state.last_processed_replication_tx_offset, tx_offset)
    }
  end

  defp record_processed_replication_event(%State{} = state, _event), do: state

  defp root_replication_pending?(%State{} = state) do
    state.deferred_replication_event_count > 0 or
      not :queue.is_empty(state.deferred_replication_events) or
      not is_nil(state.pending_txn)
  end

  defp maybe_release_causal_lsn_subscription(%State{} = state) do
    has_deferred_causal_work? =
      state.move_transaction_open? or
        state.deferred_materializer_moves
        |> :queue.to_list()
        |> Enum.any?(&(not is_nil(materializer_batch_offset(&1))))

    if has_deferred_causal_work? do
      state
    else
      Effects.release_global_lsn_subscription(state, :causal_barrier)
    end
  end

  # Belt-and-braces for any event path not deferred during initialization: a
  # nil handler must invalidate this one shape through the standard cleanup,
  # never crash the consumer with {:badmap, nil} (SAP-8006).
  defp apply_event(%State{event_handler: nil}, event) do
    {:error, {:event_before_initialization, event_tag(event)}}
  end

  defp apply_event(state, event) do
    case EventHandler.handle_event(state.event_handler, event) do
      {:error, reason} ->
        {:error, reason}

      {:ok, new_handler, effects} ->
        state = %{state | event_handler: new_handler}
        previous_offset = state.latest_offset

        result = Effects.execute(effects, state)

        final_state = maybe_commit_move_transaction(result.state)
        {final_state, notification} = move_safe_notification(final_state, previous_offset)

        {final_state, notification, result.num_changes, result.total_size}
    end
  end

  defp event_tag(event) when is_tuple(event), do: elem(event, 0)
  defp event_tag(%struct{}), do: struct
  defp event_tag(event) when is_atom(event), do: event
  defp event_tag(_event), do: :unknown

  defp apply_global_lsn(state, lsn) do
    state = %{
      state
      | last_observed_global_lsn: max(state.last_observed_global_lsn, lsn),
        last_seen_global_lsn: max(state.last_seen_global_lsn, lsn)
    }

    case apply_event(state, {:global_last_seen_lsn, lsn}) do
      {:error, reason} ->
        {:error, reason}

      {state, notification, num_changes, total_size} ->
        {state, notification, num_changes, total_size}
    end
  end

  # Stash the source LSN carried by the active materializer payload. It becomes
  # durable only when that payload's complete move pipeline commits.
  defp record_pending_move_lsn(state, dep_handle, payload) do
    case Map.get(payload, :lsn) do
      nil ->
        state

      %LogOffset{} = lsn ->
        %{state | pending_move_lsns: Map.put(state.pending_move_lsns, dep_handle, lsn)}

      invalid ->
        raise ArgumentError, "invalid materializer move offset: #{inspect(invalid)}"
    end
  end

  defp record_pending_move_causal_origin(
         state,
         %LogOffset{} = causal_origin,
         incoming_causal_depth
       )
       when is_integer(incoming_causal_depth) and incoming_causal_depth >= 0 do
    case state.pending_move_causal_origin do
      nil ->
        %{
          state
          | pending_move_causal_origin: causal_origin,
            pending_move_causal_depth: incoming_causal_depth + 1
        }

      ^causal_origin ->
        %{
          state
          | pending_move_causal_depth:
              max(state.pending_move_causal_depth, incoming_causal_depth + 1)
        }

      existing ->
        raise ArgumentError,
              "dependency move combined different causal origins: " <>
                "#{inspect(existing)} and #{inspect(causal_origin)}"
    end
  end

  defp record_pending_move_causal_origin(state, nil, nil), do: state

  defp record_pending_move_causal_origin(_state, origin, depth) do
    raise ArgumentError,
          "invalid materializer causal origin: #{inspect(origin)} depth=#{inspect(depth)}"
  end

  defp maybe_begin_move_transaction(state, %{lsn: %LogOffset{}}) do
    cond do
      state.move_transaction_open? ->
        state

      ShapeCache.Storage.supports_move_transactions?(state.writer) ->
        %{
          state
          | writer: ShapeCache.Storage.begin_move_transaction!(state.writer),
            move_transaction_open?: true,
            move_transaction_start_offset: state.latest_offset
        }

      true ->
        raise ShapeCache.Storage.Error,
          message:
            "Storage adapter does not support atomic dependency-move transactions for shape #{state.shape_handle}"
    end
  end

  defp maybe_begin_move_transaction(state, _payload), do: state

  defp move_safe_notification(%State{move_transaction_open?: true} = state, previous_offset) do
    pending_start =
      if state.latest_offset != previous_offset do
        state.pending_move_notification_start || previous_offset
      else
        state.pending_move_notification_start
      end

    {%{state | pending_move_notification_start: pending_start}, nil}
  end

  defp move_safe_notification(%State{} = state, previous_offset) do
    start_offset = state.pending_move_notification_start || previous_offset
    state = %{state | pending_move_notification_start: nil}

    if state.latest_offset != start_offset do
      {state, {{start_offset, state.latest_offset}, state.latest_offset}}
    else
      {state, nil}
    end
  end

  # Once the whole subquery move pipeline is drained, atomically publish every
  # spliced log entry together with the latest source cursor for each dependency.
  # Until then PureFileStorage may physically flush bytes, but restart recovery
  # keeps the prior durable boundary and trims the partial transaction.
  defp maybe_commit_move_transaction(%State{pending_move_lsns: pending} = state)
       when pending == %{},
       do: state

  defp maybe_commit_move_transaction(%State{move_transaction_open?: true} = state) do
    if move_pipeline_fully_drained?(state.event_handler) and
         move_root_frontier_ready?(state),
       do: commit_move_transaction(state),
       else: state
  end

  defp maybe_commit_move_transaction(%State{}) do
    raise "materializer move cursor recorded without an open storage transaction"
  end

  defp commit_move_transaction(%State{} = state) do
    applied_root_delivery_tx_offset =
      max(state.last_seen_global_lsn, state.last_processed_replication_tx_offset)

    causal_tx_offset =
      case state.pending_move_causal_origin do
        %LogOffset{tx_offset: tx_offset} -> tx_offset
        nil -> nil
      end

    root_delivery_tx_offset =
      cond do
        is_nil(causal_tx_offset) ->
          nil

        applied_root_delivery_tx_offset >= causal_tx_offset ->
          max(state.root_delivery_tx_offset, applied_root_delivery_tx_offset)

        later_deferred_root_proves_causal_frontier?(state, causal_tx_offset) ->
          max(state.root_delivery_tx_offset, causal_tx_offset)

        true ->
          nil
      end

    if is_nil(root_delivery_tx_offset) do
      raise ShapeCache.Storage.Error,
        message:
          "cannot commit dependency move before its root-delivery frontier: " <>
            "shape=#{state.shape_handle} causal=#{inspect(state.pending_move_causal_origin)} " <>
            "applied=#{applied_root_delivery_tx_offset}"
    end

    # Ordinary PostgreSQL transactions mark their final shape-visible row with
    # `headers.last=true`. Generated dependency moves can span several async
    # effects and share the same PostgreSQL tx offset, so append one valid,
    # semantically empty move event as their final row. Materializer replay can
    # then discover every logical commit boundary directly from the log without
    # a second per-shape boundary journal or another fsync on the write path.
    state =
      if state.latest_offset != state.move_transaction_start_offset,
        do: elem(append_replay_boundary_marker(state), 0),
        else: state

    positions =
      Enum.reduce(state.pending_move_lsns, state.move_positions, fn {handle, lsn}, acc ->
        Map.update(acc, handle, lsn, &LogOffset.max(&1, lsn))
      end)

    writer =
      ShapeCache.Storage.commit_move_transaction!(
        positions,
        root_delivery_tx_offset,
        state.writer
      )

    if not is_nil(state.completed_downstream_causal_token) do
      raise "committed a materializer causal batch before its predecessor was forwarded"
    end

    state = %{
      state
      | writer: writer,
        move_positions: positions,
        pending_move_lsns: %{},
        pending_move_causal_origin: nil,
        pending_move_causal_depth: nil,
        root_delivery_tx_offset: root_delivery_tx_offset,
        root_delivery_tx_offset_persisted?: true,
        completed_downstream_causal_token: state.active_downstream_causal_token,
        active_downstream_causal_token: nil,
        move_transaction_open?: false,
        move_transaction_start_offset: nil,
        materializer_barrier_active?:
          state.deferred_materializer_move_count > 0 or
            state.pending_materializer_replay_count > 0,
        pending_flush_offset: nil
    }

    # The storage commit is the durable boundary for the whole dependency
    # pipeline. Release replication-slot progress synchronously; relying on a
    # later writer message leaves cursor-only moves stuck indefinitely.
    state = confirm_flushed_and_notify(state, state.latest_offset)

    state
  end

  defp move_root_frontier_ready?(
         %State{
           pending_move_causal_origin: %LogOffset{tx_offset: causal_tx_offset}
         } = state
       ) do
    max(state.last_seen_global_lsn, state.last_processed_replication_tx_offset) >=
      causal_tx_offset or
      later_deferred_root_proves_causal_frontier?(state, causal_tx_offset)
  end

  defp move_root_frontier_ready?(%State{pending_move_causal_origin: nil}), do: false

  defp maybe_acquire_materializer_frontier(state, %LogOffset{}) do
    Effects.acquire_global_lsn_subscription(state, :causal_barrier)
  end

  defp maybe_acquire_materializer_frontier(state, nil), do: state

  defp maybe_acquire_materializer_frontier(_state, invalid) do
    raise ArgumentError, "invalid materializer causal origin: #{inspect(invalid)}"
  end

  defp later_deferred_root_proves_causal_frontier?(state, causal_tx_offset) do
    state.deferred_replication_events
    |> :queue.to_list()
    |> Enum.any?(fn entry ->
      case replication_event_offset(entry) do
        %LogOffset{tx_offset: root_tx_offset} -> root_tx_offset > causal_tx_offset
        nil -> false
      end
    end)
  end

  defp forward_completed_causal_token(state, nil, _emitted_batch?), do: state

  defp forward_completed_causal_token(state, _causal_token, true) do
    # notify_new_changes/3 already converted this forwarded fence into the
    # downstream materializer batch carrying the same token.
    state
  end

  defp forward_completed_causal_token(state, causal_token, false) do
    :ok =
      Materializer.forward_causal_end(
        materializer_ref(state),
        causal_token,
        materializer_causal_call_timeout(state)
      )

    state
  end

  defp append_replay_boundary_marker(%State{} = state) do
    marker =
      Jason.encode!(%{
        headers: %{
          event: "move-out",
          patterns: [],
          txids: [],
          last: true,
          generated_move_boundary: 1,
          causal_origin: to_string(state.pending_move_causal_origin),
          causal_depth: state.pending_move_causal_depth
        }
      })

    {{_, marker_offset}, writer} =
      ShapeCache.Storage.append_control_message!(marker, state.writer)

    {%{state | writer: writer, latest_offset: marker_offset}, byte_size(marker)}
  end

  defp move_pipeline_fully_drained?(%EventHandler.Subqueries.Steady{queue: queue}),
    do: MoveQueue.length(queue) == 0

  defp move_pipeline_fully_drained?(%EventHandler.Subqueries.Buffering{}), do: false
  defp move_pipeline_fully_drained?(_handler), do: true

  defp handle_event_error(state, {:truncate, xid}) do
    handle_txn_with_truncate(xid, state)
  end

  defp handle_event_error(state, {:event_before_initialization, event_tag}) do
    Logger.error(
      "event_before_initialization: consumer received #{inspect(event_tag)} " <>
        "before its event handler was built - terminating shape",
      shape_handle: state.shape_handle
    )

    mark_for_removal(state)
  end

  defp handle_event_error(state, :unsupported_subquery) do
    mark_for_removal(state)
  end

  defp handle_event_error(state, :buffer_overflow) do
    Logger.warning("Subquery buffer overflow for #{state.shape_handle} - terminating shape")

    mark_for_removal(state)
  end

  defp handle_event_error(state, {:buffer_memory_overflow, attempted_bytes, limit_bytes}) do
    Logger.warning(
      "Subquery deferred-event memory limit exceeded for #{state.shape_handle} - terminating shape",
      attempted_bytes: attempted_bytes,
      limit_bytes: limit_bytes
    )

    mark_for_removal(state)
  end

  defp handle_txn_with_truncate(xid, state) do
    # TODO: This is a very naive way to handle truncations: if ANY relevant truncates are
    #       present in the transaction, we're considering the whole transaction empty, and
    #       just rotate the shape handle. "Correct" way to handle truncates is to be designed.
    Logger.warning(
      "Truncate operation encountered while processing txn #{xid} for #{state.shape_handle}"
    )

    mark_for_removal(state)
  end

  defp notify_new_changes(state, notification, opts \\ [])
  defp notify_new_changes(_state, nil, _opts), do: :ok

  defp notify_new_changes(state, {changes, upper_bound}, opts) do
    notify_new_changes_with_offset(state, changes, upper_bound, opts)
  end

  @spec notify_new_changes_with_offset(
          state :: map(),
          changes_or_bounds :: list(Changes.change()) | {LogOffset.t(), LogOffset.t()},
          latest_log_offset :: LogOffset.t(),
          opts :: keyword()
        ) :: :ok
  defp notify_new_changes_with_offset(state, changes_or_bounds, latest_log_offset, opts) do
    opts = Keyword.put(opts, :end_offset, latest_log_offset)
    :ok = notify_materializer_of_new_changes(state, changes_or_bounds, opts)
    :ok = notify_clients_of_new_changes(state, latest_log_offset)
  end

  @spec notify_clients_of_new_changes(
          state :: map(),
          latest_log_offset :: LogOffset.t()
        ) :: :ok
  defp notify_clients_of_new_changes(state, latest_log_offset) do
    Registry.dispatch(
      Electric.StackSupervisor.registry_name(state.stack_id),
      state.shape_handle,
      fn registered ->
        Logger.debug(fn ->
          "Notifying ~#{length(registered)} clients about new changes to #{state.shape_handle}"
        end)

        for {pid, ref} <- registered,
            do: send(pid, {ref, :new_changes, latest_log_offset})
      end
    )
  end

  @spec notify_materializer_of_new_changes(
          state :: map(),
          changes_or_bounds :: list(Changes.change()) | {LogOffset.t(), LogOffset.t()},
          opts :: keyword()
        ) :: :ok
  defp notify_materializer_of_new_changes(
         %{materializer_subscribed?: true} = state,
         changes_or_bounds,
         opts
       ) do
    ensure_materializer_notification_fits!(state, changes_or_bounds)
    opts = Keyword.put(opts, :defer_until_durable, true)
    Materializer.new_changes(Map.take(state, [:stack_id, :shape_handle]), changes_or_bounds, opts)
  catch
    # The consumer monitors the materializer; if the materializer died the
    # :DOWN message is already in our mailbox and handle_materializer_down/2
    # will run after the current handle_event/handle_call completes.
    # Treat a `:noproc` (or transient `:normal`/`:shutdown` exit) here as
    # the same condition: don't crash the consumer (which would route into
    # the abnormal-shutdown path of handle_writer_termination and remove
    # the shape from disk).
    :exit, {:noproc, _} -> :ok
    :exit, :noproc -> :ok
    :exit, {:normal, _} -> :ok
    :exit, {:shutdown, _} -> :ok
  end

  defp notify_materializer_of_new_changes(_state, _changes_or_bounds, _opts), do: :ok

  defp ensure_materializer_notification_fits!(state, changes) when is_list(changes) do
    attempted_bytes = :erlang.external_size(changes)

    limit_bytes =
      Electric.StackConfig.lookup(
        state.stack_id,
        :materializer_live_backlog_memory_limit_bytes,
        Electric.Config.default(:materializer_live_backlog_memory_limit_bytes)
      )

    if attempted_bytes > limit_bytes do
      Logger.error("Source transaction exceeds the bounded Materializer handoff",
        shape_handle: state.shape_handle,
        attempted_bytes: attempted_bytes,
        limit_bytes: limit_bytes
      )

      raise ShapeCache.Storage.Error,
        message:
          "materializer source handoff exceeded for #{state.shape_handle}: " <>
            "attempted=#{attempted_bytes} limit=#{limit_bytes}"
    end

    :ok
  end

  defp ensure_materializer_notification_fits!(_state, _range), do: :ok

  # termination and cleanup is now done in stages.
  # 1. register that we want the shape data to be cleaned up.
  # 2. request a notification when all active shape data reads are complete
  # 3. exit the process when we receive that notification

  defp mark_for_removal(%{terminating?: true} = state) do
    state
  end

  defp mark_for_removal(state) do
    %{state | terminating?: true}
  end

  defp stop_with_reason(reason, state) do
    {reason, state} =
      case reason do
        # map reason to a clean shutdown to avoid exceptions/errors
        {:error, _} = error ->
          state = state |> State.reply_to_snapshot_waiters(error) |> mark_for_removal()
          {@stop_and_clean_reason, state}

        reason ->
          {reason, %{state | terminating?: true}}
      end

    {reason, state}
  end

  defp stop_and_clean(state) do
    {:stop, @stop_and_clean_reason, mark_for_removal(state)}
  end

  defp prepare_log_entries(changes, xid, shape) do
    changes
    |> Stream.flat_map(
      &LogItems.from_change(&1, xid, Shape.pk(shape, &1.relation), shape.replica)
    )
    |> Enum.map_reduce(0, fn {offset, %{key: key, headers: %{operation: operation}} = log_item},
                             total_size ->
      json_line = Jason.encode!(log_item)
      line_tuple = {offset, key, operation, json_line}
      {line_tuple, total_size + byte_size(json_line)}
    end)
  end

  defp calculate_replication_lag(nil), do: 0

  defp calculate_replication_lag(commit_timestamp) do
    # Compute time elapsed since commit
    # since we are comparing PG's clock with our own
    # there may be a slight skew so we make sure not to report negative lag.
    # Since the lag is only useful when it becomes significant, a slight skew doesn't matter.
    now = DateTime.utc_now()
    Kernel.max(0, DateTime.diff(now, commit_timestamp, :millisecond))
  end

  defp consider_flushed(%State{} = state, log_offset) do
    if state.txn_offset_mapping == [] do
      # No relevant txns have been observed and unflushed, we can notify immediately
      ShapeLogCollector.notify_flushed(state.stack_id, state.shape_handle, log_offset)
      state
    else
      # We're looking to "relabel" the next flush to include this txn, so we're looking for the
      # boundary that has a highest boundary less than this offset
      new_boundary = log_offset

      {head, tail} =
        Enum.split_while(
          state.txn_offset_mapping,
          &(LogOffset.compare(elem(&1, 1), new_boundary) == :lt)
        )

      case Enum.reverse(head) do
        [] ->
          # Nothing lower than this, any flush will advance beyond this txn point
          state

        [{offset, _} | rest] ->
          # Found one to relabel the upper boundary to include this txn
          %{state | txn_offset_mapping: Enum.reverse([{offset, new_boundary} | rest], tail)}
      end
    end
  end

  defp confirm_flushed_and_notify(state, flushed_offset) do
    {state, txn_offset} = State.align_offset_to_txn_boundary(state, flushed_offset)
    durable_offset = more_recent_offset(state.durable_offset, flushed_offset)
    state = %{state | durable_offset: durable_offset}

    :ok = notify_materializer_of_durability(state, durable_offset)
    ShapeLogCollector.notify_flushed(state.stack_id, state.shape_handle, txn_offset)
    state
  end

  defp notify_materializer_of_durability(%{materializer_subscribed?: true} = state, offset) do
    Materializer.durable_up_to(Map.take(state, [:stack_id, :shape_handle]), offset)
  catch
    :exit, {:noproc, _} -> :ok
    :exit, :noproc -> :ok
    :exit, {:normal, _} -> :ok
    :exit, {:shutdown, _} -> :ok
  end

  defp notify_materializer_of_durability(_state, _offset), do: :ok

  # After a pending transaction completes and txn_offset_mapping is populated,
  # process the deferred flushed offset (if any).
  #
  # Even if the most recent transaction is skipped or no changes from it end up satisfying the
  # shape's `where` condition, Storage may have signaled a flush offset from the previous transaction
  # while we were still processing fragments of the current one. Therefore this function must
  # be called any time `state.pending_txn` is reset to nil in a multi-fragment transaction
  # processing setting.
  defp clear_pending_flush_offset(%{pending_flush_offset: nil} = state), do: state

  defp clear_pending_flush_offset(%{pending_flush_offset: flushed_offset} = state) do
    %{state | pending_flush_offset: nil}
    |> confirm_flushed_and_notify(flushed_offset)
  end

  defp more_recent_offset(nil, offset), do: offset
  defp more_recent_offset(offset, nil), do: offset
  defp more_recent_offset(offset1, offset2), do: LogOffset.max(offset1, offset2)

  defp initialize_event_handler(%State{} = state, action) do
    with {:ok, handler, setup_effects} <- EventHandlerBuilder.build(state, action),
         {:ok, state} <- SetupEffects.execute(setup_effects, %{state | event_handler: handler}) do
      # Replay seeds are only needed to construct the initial handler views.
      # Keeping them here would pin the original full MapSets for this
      # Consumer's lifetime after the live persistent views start evolving.
      {:ok, %{state | dep_seed_views: %{}}}
    else
      {:error, %State{} = state} ->
        {:error, state}
    end
  end

  defp finish_initialization(%State{} = state, action, otel_ctx) do
    state = %{state | pending_initialization: nil}

    case subscribe_to_materializers(state, action) do
      {:ok, state} ->
        case initialize_event_handler(state, action) do
          {:ok, state} ->
            Logger.debug("Writer for #{state.shape_handle} initialized")

            # We start the snapshotter even if there's a snapshot because it also performs the call
            # to PublicationManager.add_shape/3. We *could* do that call here and avoid spawning a
            # process if the shape already has a snapshot but the current semantics rely on being able
            # to wait for the snapshot asynchronously and if we called publication manager here it would
            # block and prevent await_snapshot_start calls from adding snapshot subscribers.

            {:ok, _pid} =
              Shapes.DynamicConsumerSupervisor.start_snapshotter(
                state.stack_id,
                %{
                  stack_id: state.stack_id,
                  shape: state.shape,
                  shape_handle: state.shape_handle,
                  storage: state.storage,
                  otel_ctx: otel_ctx
                }
              )

            state =
              state
              |> maybe_activate_materializer_subscription()
              |> maybe_mark_restore_ready()

            {:noreply, state, next_after_move(state)}

          {:error, state} ->
            stop_and_clean(state)
        end

      {:pending, state} ->
        {:noreply, %{state | pending_initialization: {action, otel_ctx}}}

      :error ->
        stop_and_clean(state)
    end
  end

  # Subscribe to each dependency materializer, passing the persisted per-dep
  # moves-position so the materializer replays any moves this consumer missed
  # across a restart. Captures the returned seed views (as-of the position) for
  # seeding the event handler's dependency views, and baselines a position for
  # dependencies that don't have one yet so a first missed move can be replayed.
  #
  # Returns `{:ok, state}` with `dep_seed_views`/`move_positions` populated,
  # `{:pending, state}` when a stale seed will arrive asynchronously, or
  # `:error` if any dependency materializer cannot serve the subscription.
  defp subscribe_to_materializers(state, action) do
    case validate_restored_move_positions(state, action) do
      :error ->
        :error

      :ok ->
        do_subscribe_to_materializers_and_persist(state, action)
    end
  end

  defp do_subscribe_to_materializers_and_persist(state, action) do
    case do_subscribe_to_materializers(state) do
      {:ok, %State{} = state} ->
        {:ok, persist_initial_dependency_state(state, action)}

      {:pending, %State{} = state} ->
        {:pending, state}

      :error ->
        :error
    end
  end

  defp persist_initial_dependency_state(
         %State{shape: %{shape_dependencies_handles: []}} = state,
         _action
       ),
       do: state

  defp persist_initial_dependency_state(%State{} = state, :create) do
    writer = ShapeCache.Storage.begin_move_transaction!(state.writer)

    writer =
      ShapeCache.Storage.commit_move_transaction!(
        state.move_positions,
        state.root_delivery_tx_offset,
        writer
      )

    %{state | writer: writer, root_delivery_tx_offset_persisted?: true}
  end

  defp persist_initial_dependency_state(
         %State{root_delivery_tx_offset_persisted?: true} = state,
         :restore
       ),
       do: state

  defp validate_restored_move_positions(
         %State{shape: %{shape_dependencies_handles: []}},
         :restore
       ),
       do: :ok

  defp validate_restored_move_positions(
         %State{
           shape: %{shape_dependencies_handles: handles},
           move_positions: positions,
           root_delivery_tx_offset_persisted?: root_delivery_persisted?
         } = state,
         :restore
       ) do
    missing = Enum.reject(handles, &Map.has_key?(positions, &1))

    cond do
      missing != [] ->
        Logger.warning(
          "Restored outer shape is missing durable dependency replay cursors; invalidating shape",
          shape_handle: state.shape_handle,
          missing_dependency_shape_handles: missing
        )

        :error

      not root_delivery_persisted? ->
        Logger.warning(
          "Restored outer shape is missing its durable root-delivery frontier; invalidating shape",
          shape_handle: state.shape_handle
        )

        :error

      true ->
        :ok
    end
  end

  defp validate_restored_move_positions(_state, _action), do: :ok

  defp do_subscribe_to_materializers(state) do
    Enum.reduce_while(state.shape.shape_dependencies_handles, {:ok, state}, fn shape_handle,
                                                                               {:ok, state} ->
      if Map.has_key?(state.dep_seed_views, shape_handle) do
        {:cont, {:ok, state}}
      else
        name = Materializer.name(state.stack_id, shape_handle)

        with pid when is_pid(pid) <- GenServer.whereis(name),
             true <- Process.alive?(pid) do
          Process.monitor(pid,
            tag: {:dependency_materializer_down, shape_handle}
          )

          from_lsn = Map.get(state.move_positions, shape_handle)
          subscription_ref = make_ref()
          consumer_pid = self()

          supervisor =
            Electric.ProcessRegistry.name(state.stack_id, Electric.StackTaskSupervisor)

          case Task.Supervisor.start_child(supervisor, fn ->
                 result =
                   try do
                     Materializer.subscribe_causally(pid, from_lsn, consumer_pid)
                   catch
                     :exit, reason -> {:error, {:exit, reason}}
                   end

                 send(
                   consumer_pid,
                   {:dependency_subscription_result, shape_handle, pid, subscription_ref, result}
                 )
               end) do
            {:ok, _task_pid} ->
              {:halt,
               {:pending,
                %{
                  state
                  | pending_dependency_subscription: {shape_handle, pid, subscription_ref}
                }}}

            {:error, reason} ->
              Logger.warning("Could not start dependency subscription task",
                dependency_shape_handle: shape_handle,
                shape_handle: state.shape_handle,
                reason: inspect(reason)
              )

              {:halt, :error}
          end
        else
          _ ->
            Logger.warning(
              "Materializer for shape is not alive, invalidating shape",
              shape_handle: shape_handle,
              state_shape_handle: state.shape_handle
            )

            {:halt, :error}
        end
      end
    end)
  end

  defp apply_materializer_subscription(
         %State{} = state,
         shape_handle,
         materializer_pid,
         from_lsn,
         seed_view,
         applied_offset
       ) do
    replay_pending? =
      match?(%LogOffset{}, from_lsn) and LogOffset.compare(from_lsn, applied_offset) == :lt

    if replay_pending? and state.pending_materializer_replay_count > 0 do
      {:error, :multiple_stale_dependencies, state}
    else
      {pending_materializer_replays, pending_materializer_replay_count} =
        if replay_pending? do
          {
            :queue.in(
              {shape_handle, materializer_pid},
              state.pending_materializer_replays
            ),
            state.pending_materializer_replay_count + 1
          }
        else
          {state.pending_materializer_replays, state.pending_materializer_replay_count}
        end

      {:ok,
       %{
         state
         | dep_seed_views: Map.put(state.dep_seed_views, shape_handle, seed_view),
           move_positions: Map.put_new(state.move_positions, shape_handle, applied_offset),
           pending_materializer_replays: pending_materializer_replays,
           pending_materializer_replay_count: pending_materializer_replay_count,
           materializer_barrier_active?: state.materializer_barrier_active? or replay_pending?
       }}
    end
  end

  defp clean_table(table_oid, state) do
    inspector = Electric.StackConfig.lookup!(state.stack_id, :inspector)
    Inspector.clean(table_oid, inspector)
  end

  defp handle_materializer_down(reason, state) do
    case {reason, state.terminating?} do
      {_, true} -> {:noreply, state}
      {{:shutdown, _}, false} -> {:stop, ShapeCleaner.consumer_suspend_reason(), state}
      {:shutdown, false} -> {:stop, ShapeCleaner.consumer_suspend_reason(), state}
      _ -> stop_and_clean(state)
    end
  end

  defp terminate_writer(state) do
    {writer, state} = Map.pop(state, :writer)

    try do
      if writer, do: ShapeCache.Storage.terminate(writer)
    rescue
      # In the case of shape removal, the deletion of the storage directory
      # may happen before we have a chance to terminate the storage
      File.Error -> :ok
    end

    state
  end

  if Mix.env() == :test do
    def activate_mocked_functions_from_test_process do
      Support.TestUtils.activate_mocked_functions_for_module(__MODULE__)
    end
  else
    def activate_mocked_functions_from_test_process, do: :noop
  end
end
