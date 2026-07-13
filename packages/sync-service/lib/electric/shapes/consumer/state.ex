defmodule Electric.Shapes.Consumer.State do
  @moduledoc false
  alias Electric.Shapes.Consumer.InitialSnapshot
  alias Electric.Shapes.Shape
  alias Electric.Replication.TransactionBuilder
  alias Electric.Postgres.SnapshotQuery
  alias Electric.Replication.LogOffset
  alias Electric.ShapeCache.Storage

  require Logger

  @write_unit_txn :txn
  @write_unit_txn_fragment :txn_fragment

  defstruct [
    :stack_id,
    :shape_handle,
    :shape,
    :hibernate_after,
    :latest_offset,
    :storage,
    :writer,
    # Highest shape-log offset whose bytes and transaction boundary are durable.
    # Materializers may apply a newer volatile tail internally, but only moves
    # through this offset may propagate to dependent shapes.
    durable_offset: nil,
    initial_snapshot_state: InitialSnapshot.new(nil),
    event_handler: nil,
    transaction_builder: TransactionBuilder.new(),
    buffer: [],
    txn_offset_mapping: [],
    materializer_subscribed?: false,
    # A materializer may attach to an already-running source consumer while a
    # fragmented root transaction or dependency move is still in flight. Its
    # call stays pending until that transaction commits, then the writer is
    # synchronously flushed before the materializer receives its replay bound.
    pending_materializer_subscription: nil,
    # Per-dependency "moves-applied-up-to" source LSNs (`%{dep_handle => LogOffset}`),
    # persisted so that after a restart the outer subquery consumer can ask each
    # dependency materializer to replay the moves it missed and dedup by position.
    move_positions: %{},
    # Per-dependency seed views (`%{dep_handle => MapSet}`) captured from the
    # materializer at subscribe time (as-of `move_positions`), used to seed the
    # event handler's dependency views so replayed moves are not
    # redundancy-eliminated.
    dep_seed_views: %{},
    # Restart replay is pulled one source transaction at a time from dependency
    # materializers. This bounds the consumer mailbox/heap instead of letting a
    # synchronous subscribe call enqueue the entire missed tail at once.
    pending_materializer_replays: :queue.new(),
    pending_materializer_replay_count: 0,
    # Exactly one replay payload may be present in the global deferred-work
    # scheduler. When false while a replay is pending, the next replay offset is
    # unknown and newer queued work must wait until lookahead is pulled.
    materializer_replay_lookahead?: false,
    # A replay pull that returns :pending is owned by the source coordinator.
    # Do not retry it because unrelated mailbox traffic arrived; only the
    # coordinator's readiness notification may clear this gate.
    materializer_replay_waiting?: false,
    # Stale dependency subscriptions are prepared asynchronously by the source
    # materializer. Keep initialization resumable so this Consumer never blocks
    # the ShapeLogCollector in a long GenServer.call while waiting its turn.
    pending_initialization: nil,
    pending_dependency_subscription: nil,
    # Restored subquery shapes are not safe to expose merely because their
    # persisted snapshot exists. Keep snapshot waiters parked until dependency
    # seed preparation, replay, deferred root work, and any open move have all
    # drained against the live collector frontier.
    restore_ready?: true,
    # Per-dependency source LSN of the most recently received (not yet committed)
    # materializer move (`%{dep_handle => LogOffset}`). These positions commit
    # atomically with the whole dependency-move storage transaction.
    pending_move_lsns: %{},
    # PostgreSQL position that causally produced the open dependency move. It
    # is distinct from `pending_move_lsns`, whose values are dependency-local
    # replay cursors for nested generated shapes. The depth is the token this
    # shape's generated boundary must present to its next dependent.
    pending_move_causal_origin: nil,
    pending_move_causal_depth: nil,
    # Materializer payloads received while another dependency move is querying
    # are processed FIFO after that move commits. The queue is bounded by the
    # owning subquery buffer limit so sustained churn invalidates the shape
    # instead of holding one unbounded transaction open.
    deferred_materializer_moves: :queue.new(),
    deferred_materializer_move_count: 0,
    # A live causal reservation is propagated to this Consumer's own
    # Materializer before the source collector can advance. Keep the downstream
    # token attached until the corresponding dependency move transaction has
    # fully committed and its derived notification has been handed off.
    active_downstream_causal_token: nil,
    completed_downstream_causal_token: nil,
    # Startup readiness waiters are target-scoped: traffic newer than a
    # waiter's sampled PostgreSQL WAL cut must not delay it indefinitely.
    causal_drain_waiters: [],
    # Once a later dependency payload has arrived behind an asynchronous move,
    # root replication events that arrive after it must not be evaluated against
    # the older dependency view. Keep them in a bounded FIFO until every earlier
    # materializer payload has committed.
    materializer_barrier_active?: false,
    deferred_replication_events: :queue.new(),
    deferred_replication_event_count: 0,
    # Global-LSN delivery is shared by move-in splicing and causal ordering.
    # Keep explicit owners so one protocol cannot unregister the other while it
    # is still waiting for the collector's post-layer transaction frontier.
    global_lsn_subscription_reasons: MapSet.new(),
    # Highest post-layer collector frontier delivered to this Consumer. It is
    # not safe for scheduling until every earlier deferred root event has
    # reached the EventHandler.
    last_observed_global_lsn: 0,
    # Highest collector frontier applied to the EventHandler.
    last_seen_global_lsn: 0,
    # A collector frontier cannot be exposed to an active move-in until every
    # root event queued before it has reached the EventHandler. Otherwise the
    # move-in snapshot can splice before those transactions are classified as
    # visible before/after the snapshot.
    pending_global_last_seen_lsn: nil,
    # Highest root transaction this Consumer has completely handled. A root
    # fragment from the same transaction is an equivalent causal fence once its
    # transaction builder has reached commit.
    last_processed_replication_tx_offset: 0,
    # Highest PostgreSQL transaction for which this shape has durably applied
    # either the routed root fragment or the collector's negative-delivery
    # acknowledgement. It advances atomically with dependency moves so replay
    # never evaluates an old root change against a newer dependency view.
    root_delivery_tx_offset: 0,
    root_delivery_tx_offset_persisted?: false,
    # Approximate serialized bytes retained across both deferred queues. Count
    # limits alone do not protect the VM from one very large transaction being
    # copied into many replay-waiting outer Consumers.
    deferred_event_bytes: 0,
    # Dependency moves may span an asynchronous query and several storage
    # appends. While this is true, storage keeps their durable transaction
    # boundary at the last fully applied move so an abrupt restart can trim the
    # whole partial pipeline instead of replaying into half-applied rows.
    move_transaction_open?: false,
    # Outer-log position before the current generated dependency move began.
    # If the move writes any rows, commit appends a valid `last=true` marker;
    # cursor-only/no-op moves advance only their persisted source position.
    move_transaction_start_offset: nil,
    # First outer-log offset written by the open dependency move. Notifications
    # are held until the move and its source cursor commit together.
    pending_move_notification_start: nil,
    terminating?: false,
    buffering?: false,
    # Based on the write unit value, consumer will either buffer txn fragments in memory until
    # it sees a commit (write_unit=txn) or it will write each received txn fragment to storage
    # immediately (write_unit=txn_fragment).
    # When true, stream fragments directly to storage without buffering
    write_unit: @write_unit_txn,
    # Tracks in-progress transaction, initialized when a txn fragment with has_begin?=true is seen.
    # It is used to check whether the entire txn is visible in the snapshot and to mark it
    # as flushed in order to handle its remaining fragments appropriately.
    pending_txn: nil,
    # When a {Storage, :flushed, offset} message arrives during a pending
    # transaction, we defer the notification and store the max flushed offset
    # here. Multiple deferred notifications are collapsed into a single most recent offset.
    pending_flush_offset: nil,
    # Reference of the pending suspend timer, or nil if none is armed. The timer
    # is armed when the consumer settles into hibernation and is cancelled as soon
    # as any message arrives (activity), so at most one is ever live at a time.
    suspend_timer: nil,
    # How long after hibernation to suspend (in ms)
    suspend_after: nil,
    # Monotonic millisecond timestamp of the last consumer-forced GC (nil if never).
    # Used by hysteresis logic in maybe_garbage_collect/1 to cap forced-GC frequency.
    last_forced_gc_at: nil,
    # Adaptive-GC heap threshold (bytes) cached at consumer startup, or nil when
    # disabled.
    gc_heap_threshold: nil
  ]

  @type pg_snapshot() :: SnapshotQuery.pg_snapshot()
  @type uninitialized_t() :: term()

  @typedoc """
  State of the consumer process.

  ## Flush notification

  When a transaction is flushed, we need to notify the shape log collector
  with latest written offset. Latest written offset however might not be
  last one in the transaction, so to correctly notify the collector, we need
  to align the offset to the transaction boundary.
  To do this, after processing the transaction we store the mapping from the
  last relevant one to last one generally in the transaction and use that
  to map back the flushed offset to the transaction boundary.

  ## Buffering

  Consumer will be buffering transactions in 2 cases: when we're waiting for initial
  snapshot information, or when an active subquery move-in is being spliced into the log.

  Buffer is stored in reverse order.
  """
  @type t() :: term()

  defguard is_snapshot_started(state)
           when is_struct(state.initial_snapshot_state, InitialSnapshot) and
                  state.initial_snapshot_state.snapshot_started?

  defguard needs_initial_filtering(state)
           when is_struct(state.initial_snapshot_state, InitialSnapshot) and
                  state.initial_snapshot_state.filtering?

  @spec new(Electric.stack_id(), Shape.handle(), Shape.t()) :: uninitialized_t()
  def new(stack_id, shape_handle, shape) do
    stack_id
    |> new(shape_handle)
    |> initialize_shape(shape, %{})
  end

  @spec new(Electric.stack_id(), Shape.handle()) :: uninitialized_t()
  def new(stack_id, shape_handle) do
    %__MODULE__{
      stack_id: stack_id,
      shape_handle: shape_handle,
      hibernate_after:
        Electric.StackConfig.lookup(
          stack_id,
          :shape_hibernate_after,
          Electric.Config.default(:shape_hibernate_after)
        ),
      suspend_after:
        Electric.StackConfig.lookup(
          stack_id,
          :shape_suspend_after,
          Electric.Config.default(:shape_suspend_after)
        ),
      gc_heap_threshold: Electric.StackConfig.lookup(stack_id, :consumer_gc_heap_threshold, nil),
      buffering?: true
    }
  end

  @spec initialize_shape(uninitialized_t(), Shape.t(), map()) :: uninitialized_t()
  def initialize_shape(%__MODULE__{} = state, shape, opts) do
    feature_flags = Map.get(opts, :feature_flags, [])
    is_subquery_shape? = Map.get(opts, :is_subquery_shape?, false)

    %{
      state
      | shape: shape,
        # Enable direct fragment-to-storage streaming for shapes without subquery dependencies
        # and if the current shape itself isn't an inner shape of a shape with subqueries.
        write_unit:
          if "allow_subqueries" in feature_flags or shape.shape_dependencies != [] or
               is_subquery_shape? do
            @write_unit_txn
          else
            @write_unit_txn_fragment
          end
    }
  end

  @doc """
  After the storage is ready, initialize the state with info from storage and writer state.
  """
  @spec initialize(uninitialized_t(), Storage.shape_storage(), Storage.writer_state()) :: t()
  def initialize(%__MODULE__{} = state, storage, writer) do
    %__MODULE__{} = state = validate_storage_capabilities(state, storage)
    :ok = validate_dependency_move_capability(state, writer)

    {:ok, latest_offset} = Storage.fetch_latest_offset(storage)
    {:ok, pg_snapshot} = Storage.fetch_pg_snapshot(storage)
    {:ok, move_positions} = Storage.fetch_move_positions(storage)

    {:ok, stored_root_delivery_tx_offset} =
      Storage.fetch_root_delivery_tx_offset(storage)

    root_delivery_tx_offset = stored_root_delivery_tx_offset || 0

    initial_snapshot_state =
      InitialSnapshot.reinitialize(state.initial_snapshot_state, pg_snapshot)

    %__MODULE__{
      state
      | latest_offset: latest_offset,
        durable_offset: latest_offset,
        storage: storage,
        writer: writer,
        move_positions: move_positions,
        root_delivery_tx_offset: root_delivery_tx_offset,
        root_delivery_tx_offset_persisted?: is_integer(stored_root_delivery_tx_offset),
        last_processed_replication_tx_offset:
          max(state.last_processed_replication_tx_offset, root_delivery_tx_offset),
        initial_snapshot_state: initial_snapshot_state,
        buffering?: InitialSnapshot.needs_buffering?(initial_snapshot_state)
    }
  end

  defp validate_storage_capabilities(
         %__MODULE__{write_unit: @write_unit_txn_fragment} = state,
         storage
       ) do
    if Storage.supports_txn_fragment_streaming?(storage) do
      state
    else
      {mod, _opts} = storage

      Logger.warning(
        "Storage backend #{inspect(mod)} does not support txn fragment streaming. " <>
          "Falling back to full-transaction buffering for shape #{state.shape_handle}. " <>
          "Use PureFileStorage for optimal performance with fragment streaming."
      )

      %{state | write_unit: @write_unit_txn}
    end
  end

  defp validate_storage_capabilities(state, _storage), do: state

  defp validate_dependency_move_capability(
         %__MODULE__{shape: %{shape_dependencies: []}},
         _writer
       ),
       do: :ok

  defp validate_dependency_move_capability(%__MODULE__{} = state, writer) do
    if Storage.supports_move_transactions?(writer) do
      :ok
    else
      {mod, _writer_state} = writer

      raise Storage.Error,
        message:
          "Storage backend #{inspect(mod)} cannot atomically persist dependency moves for shape #{state.shape_handle}"
    end
  end

  @doc """
  For the given physical flush offset, remove every covered write mapping and
  return the latest transaction boundary made durable by that flush.

  A storage flush may coalesce bytes beyond one or more mapped shape writes.
  In that case every transaction boundary relabelled onto a covered write is
  durable, even when none of the write offsets exactly matches the flush. Once
  a mapping is covered, its transaction boundary is authoritative: generated
  or filtered shape-log offsets can sort after the source transaction boundary,
  so the physical offset must not be folded into the boundary maximum.
  """
  @spec align_offset_to_txn_boundary(t(), LogOffset.t()) :: {t(), LogOffset.t()}
  def align_offset_to_txn_boundary(
        %__MODULE__{txn_offset_mapping: txn_offset_mapping} = state,
        offset
      ) do
    {covered, pending} =
      Enum.split_while(txn_offset_mapping, fn {written_offset, _boundary} ->
        LogOffset.compare(written_offset, offset) != :gt
      end)

    transaction_offset =
      case covered do
        [] ->
          offset

        [{_written_offset, boundary} | rest] ->
          Enum.reduce(rest, boundary, fn {_written_offset, candidate}, latest ->
            LogOffset.max(latest, candidate)
          end)
      end

    {%{state | txn_offset_mapping: pending}, transaction_offset}
  end

  @spec add_to_buffer(t(), TransactionFragment.t()) :: t()
  def add_to_buffer(%__MODULE__{buffer: buffer} = state, txn) do
    %{state | buffer: [txn | buffer]}
  end

  @spec pop_buffered(t()) :: {[TransactionFragment.t()], t()}
  def pop_buffered(%__MODULE__{buffer: buffer} = state) do
    {Enum.reverse(buffer), %{state | buffer: [], buffering?: false}}
  end

  @spec add_waiter(t(), GenServer.from()) :: t()
  def add_waiter(%__MODULE__{initial_snapshot_state: initial_snapshot_state} = state, from) do
    %{
      state
      | initial_snapshot_state: InitialSnapshot.add_waiter(initial_snapshot_state, from)
    }
  end

  def set_initial_snapshot(
        %__MODULE__{initial_snapshot_state: initial_snapshot_state} = state,
        snapshot
      ) do
    initial_snapshot_state =
      InitialSnapshot.set_initial_snapshot(initial_snapshot_state, state.storage, snapshot)

    %{
      state
      | initial_snapshot_state: initial_snapshot_state,
        buffering?: InitialSnapshot.needs_buffering?(initial_snapshot_state)
    }
  end

  def mark_snapshot_started(state, reply_waiters? \\ true)

  def mark_snapshot_started(
        %__MODULE__{initial_snapshot_state: initial_snapshot_state} = state,
        reply_waiters?
      ) do
    initial_snapshot_state =
      InitialSnapshot.mark_snapshot_started(
        initial_snapshot_state,
        state.stack_id,
        state.shape_handle,
        state.storage,
        reply_waiters?
      )

    %{state | initial_snapshot_state: initial_snapshot_state}
  end

  def reply_to_snapshot_waiters(state, reason) do
    initial_snapshot_state =
      InitialSnapshot.reply_to_waiters(state.initial_snapshot_state, reason)

    %{state | initial_snapshot_state: initial_snapshot_state}
  end

  def initial_snapshot_xmin(%__MODULE__{initial_snapshot_state: %{pg_snapshot: {xmin, _, _}}}),
    do: xmin

  def initial_snapshot_xmin(%__MODULE__{}), do: nil

  def telemetry_attrs(%__MODULE__{stack_id: stack_id, shape_handle: shape_handle, shape: shape}) do
    [
      "shape.handle": shape_handle,
      "shape.root_table": shape.root_table,
      "shape.where": if(not is_nil(shape.where), do: shape.where.query, else: nil),
      stack_id: stack_id
    ]
  end

  defguard is_write_unit_txn(write_unit) when write_unit == @write_unit_txn
  defguard is_write_unit_txn_fragment(write_unit) when write_unit == @write_unit_txn_fragment
end
