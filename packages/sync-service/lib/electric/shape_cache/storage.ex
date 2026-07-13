defmodule Electric.ShapeCache.Storage do
  @moduledoc """
  Behaviour and dispatch layer for shape-log storage adapters.

  Implementations may support ordinary shapes without implementing the optional
  replay callbacks. Shapes with subquery dependencies additionally require an
  exact offset-preserving log stream and atomic dependency-move transactions, so
  adapters serving those shapes must implement `get_log_stream_with_offsets/3`,
  `begin_move_transaction!/1`, and `commit_move_transaction!/3`.

  Those callbacks remain optional at the behaviour level so existing custom
  adapters stay source-compatible for ordinary shapes. `ShapeCache` validates
  the stricter subquery contract before creating a subquery shape and returns a
  precise error listing any missing callbacks. `get_log_replay_safe_cursor/1`
  is also optional; its dispatch fallback conservatively treats the adapter's
  latest offset as the earliest safe replay cursor.
  """
  import Electric.Replication.LogOffset, only: [is_log_offset_lt: 2]

  alias Electric.Shapes.Shape
  alias Electric.Shapes.Querying
  alias Electric.Replication.LogOffset

  @subquery_required_callbacks [
    get_log_stream_with_offsets: 3,
    fetch_root_delivery_tx_offset: 1,
    begin_move_transaction!: 1,
    commit_move_transaction!: 3
  ]

  defmodule Error do
    defexception [:message]

    @type t() :: %__MODULE__{message: String.t()}
  end

  @type shape_handle :: Electric.shape_handle()
  @type pg_snapshot :: %{
          xmin: pos_integer(),
          xmax: pos_integer(),
          xip_list: [pos_integer()],
          filter_txns?: boolean()
        }
  @type offset :: LogOffset.t()
  @typedoc """
  Per-dependency "moves-applied-up-to" source LSN positions for an outer
  subquery consumer, keyed by the dependency's shape handle.
  """
  @type move_positions :: %{shape_handle() => LogOffset.t()}
  @type root_delivery_tx_offset :: non_neg_integer() | nil

  @type compiled_opts :: term()
  @type shape_opts :: term()
  @type writer_state :: term()

  @type storage :: {module(), compiled_opts()}
  @type shape_storage :: {module(), shape_opts()}

  @type operation_type :: :insert | :update | :delete
  @type log_item ::
          {LogOffset.t(), key :: String.t(), operation_type :: operation_type(),
           Querying.json_iodata()}
  @type log :: Enumerable.t(Querying.json_iodata())
  @type offset_log :: Enumerable.t({LogOffset.t() | nil, Querying.json_iodata()})

  @typedoc """
  A move-in snapshot row, represented as a 3-element list `[key, tags, json]`:

    * `key` – the row's key
    * `tags` – the row's tag list
    * `json` – the encoded log item body
  """
  @type row :: [String.t() | [String.t()] | Querying.json_iodata()]

  @doc "Validate and initialise storage base configuration from application configuration"
  @callback shared_opts(term()) :: compiled_opts()

  @doc "Initialise shape-specific opts from the shared, global, configuration"
  @callback for_shape(shape_handle(), compiled_opts()) :: shape_opts()

  @doc "Start any stack-wide processes required for storage to operate"
  @callback stack_start_link(compiled_opts()) :: GenServer.on_start()

  @doc "Start any shape-specific processes required to run the storage backend"
  @callback start_link(shape_opts()) :: GenServer.on_start()

  @doc "Prepare the in-process writer state, returning an accumulator."
  @callback init_writer!(shape_opts(), shape_definition :: Shape.t()) :: writer_state()

  @doc "Retrieve all stored shape handles"
  @callback get_all_stored_shape_handles(compiled_opts()) ::
              {:ok, MapSet.t(shape_handle())} | {:error, term()}

  @doc "Get the total disk usage for all shapes"
  @callback get_total_disk_usage(compiled_opts()) :: non_neg_integer()

  @doc """
  Get the latest offset for the shape storage.

  If the instance is new, then it MUST return `{:ok, LogOffset.last_before_real_offsets()}`.
  """
  @callback fetch_latest_offset(shape_opts()) :: {:ok, offset()} | {:error, term()}

  @doc """
  Get the current pg_snapshot for the shape storage.
  """
  @callback fetch_pg_snapshot(shape_opts()) :: {:ok, pg_snapshot() | nil} | {:error, term()}

  @callback set_pg_snapshot(pg_snapshot(), shape_opts()) :: :ok

  @doc """
  Persist the per-dependency moves-applied-up-to positions for an outer
  subquery consumer.
  """
  @callback set_move_positions!(move_positions(), shape_opts()) :: :ok

  @doc """
  Fetch the per-dependency moves-applied-up-to positions for an outer subquery
  consumer. Returns `{:ok, %{}}` when none have been persisted yet.
  """
  @callback fetch_move_positions(shape_opts()) :: {:ok, move_positions()} | {:error, term()}

  @doc """
  Fetch the highest PostgreSQL transaction offset whose root-table effects
  (including a negative routing decision) are durably reflected by this
  shape's dependency view.

  `nil` means the storage predates this replay-safety metadata. Restored
  subquery shapes must invalidate that state instead of replaying root changes
  against a newer dependency view.
  """
  @callback fetch_root_delivery_tx_offset(shape_opts()) ::
              {:ok, root_delivery_tx_offset()} | {:error, term()}

  @doc """
  Return the earliest exclusive log cursor from which replay is safe.

  Cursors before this value must be invalidated rather than replayed. This can
  happen when storage predates replay metadata or when compaction has removed
  history needed to reconstruct generated moves.
  """
  @callback get_log_replay_safe_cursor(shape_opts()) :: LogOffset.t()

  @doc """
  Begin a dependency-move transaction on the writer.

  Storage implementations that support this capability defer publishing both
  the appended log boundary and dependency positions until
  `commit_move_transaction!/3` succeeds.
  """
  @callback begin_move_transaction!(writer_state()) :: writer_state() | no_return()

  @doc """
  Atomically publish a dependency-move transaction's log boundary and source
  positions.
  """
  @callback commit_move_transaction!(
              move_positions(),
              non_neg_integer(),
              writer_state()
            ) ::
              writer_state() | no_return()

  @doc "Check if snapshot for a given shape handle already exists"
  @callback snapshot_started?(shape_opts()) :: boolean()

  @doc """
  Make a new snapshot for a shape handle based on the meta information about the table and a stream of plain string rows

  Should raise an error if making the snapshot had failed for any reason.
  """
  @callback make_new_snapshot!(
              Querying.json_result_stream(),
              shape_opts()
            ) :: :ok

  @callback mark_snapshot_as_started(shape_opts()) :: :ok

  @doc """
  Write a move in snapshot to the storage. Should write it alongside the main log,
  with stiching being done via a separate call `append_move_in_snapshot_to_log!`.

  The stream items are `[key, tags, json]`, where `tags` is the row tag list
  and `json` is the encoded log item body.
  """
  @callback write_move_in_snapshot!(
              Enumerable.t(row()),
              name :: String.t(),
              shape_opts()
            ) :: :ok

  @doc """
  Splice a move in snapshot into the main log.

  Since snapshot doesn't have an offset associated, the offsets are inferred at splice time, and the range is returned.
  Range is a tuple of {starting_offset, ending_offset}, with starting offset being right before the first item in
  the snapshot to match usage of `get_log_stream/3`

  An optional `skip_row?` predicate can filter rows during splicing: it receives
  `(key, tags)` and returns `true` if the row should be skipped.
  """
  @callback append_move_in_snapshot_to_log!(
              name :: String.t(),
              writer_state(),
              skip_row? :: (String.t(), [String.t()] -> boolean())
            ) ::
              {inserted_range :: {LogOffset.t(), LogOffset.t()}, writer_state()} | no_return()

  @doc """
  Append a control message to the log that doesn't have an offset associated with it.

  Since control message doesn't have an offset associated, the offsets are inferred at append time,
  and the range is returned. Range is a tuple of {starting_offset, ending_offset}, with starting offset
  being right before the control message to match usage of `get_log_stream/3`
  """
  @callback append_control_message!(control_message :: map() | binary(), writer_state()) ::
              {inserted_range :: {LogOffset.t(), LogOffset.t()}, writer_state()} | no_return()

  @doc """
  Append log items from one transaction to the log.

  Each storage implementation is responsible for handling transient errors
  using some retry strategy.

  If the backend fails to write within the expected time, or some other error
  occurs, then this should raise.
  """
  @callback append_to_log!(Enumerable.t(log_item()), writer_state()) ::
              writer_state() | no_return()

  @doc """
  Append log items from a transaction fragment.

  Called potentially multiple times per transaction for shapes that stream
  fragments directly to storage without waiting for the complete transaction.
  Unlike `append_to_log!/2`, this does not assume transaction completion.

  Transaction commits should be signaled separately via `signal_txn_commit!/2`
  to allow storage to calculate chunk boundaries at transaction boundaries.
  """
  @callback append_fragment_to_log!(Enumerable.t(log_item()), writer_state()) ::
              writer_state() | no_return()

  @doc """
  Signal that a transaction has committed.

  Used by storage to calculate chunk boundaries at transaction boundaries.
  Called after all fragments for a transaction have been written via
  `append_fragment_to_log!/2`.
  """
  @callback signal_txn_commit!(xid :: pos_integer(), writer_state()) ::
              writer_state() | no_return()

  @doc "Get stream of the log for a shape since a given offset"
  @callback get_log_stream(offset :: LogOffset.t(), max_offset :: LogOffset.t(), shape_opts()) ::
              log()

  @doc """
  Get a stream of the log together with the authoritative offsets assigned by
  storage.

  Main-log entries are returned as `{offset, item}`. Initial snapshot entries
  have no main-log offset and are returned as `{nil, item}`.
  """
  @callback get_log_stream_with_offsets(
              offset :: LogOffset.t(),
              max_offset :: LogOffset.t(),
              shape_opts()
            ) :: offset_log()

  @optional_callbacks get_log_replay_safe_cursor: 1,
                      get_log_stream_with_offsets: 3,
                      begin_move_transaction!: 1,
                      commit_move_transaction!: 3,
                      fetch_root_delivery_tx_offset: 1

  @doc """
  Get the last exclusive offset of the chunk starting from the given offset.

  If chunk has not finished accumulating, `nil` is returned.

  If chunk has finished accumulating, the last offset of the chunk is returned.
  """
  @callback get_chunk_end_log_offset(LogOffset.t(), shape_opts()) :: LogOffset.t() | nil

  @doc """
  Close all active resources and persist any pending writes on system/process shutdown
  """
  @callback terminate(writer_state()) :: any()

  @doc """
  Commit any pending writes to disk and close open resources that can be safely reopened later.
  """
  @callback hibernate(writer_state()) :: writer_state()

  @doc """
  Clean up snapshots/logs for a shape handle by deleting whole directory.

  Is expected to be only called once the storage has been stopped.
  """
  @callback cleanup!(shape_opts()) :: any()
  @callback cleanup!(compiled_opts(), shape_handle()) :: any()

  @doc """
  Cleanup all shape data and metadata from storage.
  """
  @callback cleanup_all!(shape_opts()) :: any()

  @doc """
  Whether this storage backend supports streaming transaction fragments
  directly to storage via `append_fragment_to_log!/2` and `signal_txn_commit!/2`.

  Storage backends that return `false` will only receive complete transactions
  via `append_to_log!/2`.
  """
  @callback supports_txn_fragment_streaming?() :: boolean()

  @doc """
  Compact operations in the log keeping the last N complete chunks intact
  """
  @callback compact(shape_opts(), keep_complete_chunks :: pos_integer()) :: :ok

  @behaviour __MODULE__

  @last_log_offset LogOffset.last()

  @doc """
  Apply a message to the writer state.

  In-process writer may send messages to self, in the form of
  `{#{inspect(__MODULE__)}, message}`, which must be handled using this function
  and the return of the function must be used as the new writer state.
  """
  def apply_message({mod, writer_state}, {m, f, a}) do
    {mod, apply(m, f, [writer_state | a])}
  end

  def for_stack(stack_id, opts \\ []) do
    {mod, storage_opts} = Electric.StackConfig.lookup!(stack_id, Electric.ShapeCache.Storage)

    # is_map guard: TestStorage uses tuples for opts where read_only? is a no-op
    if opts[:read_only?] == true and is_map(storage_opts),
      do: {mod, Map.put(storage_opts, :read_only?, true)},
      else: {mod, storage_opts}
  end

  def opts_for_stack(stack_id) do
    {_module, opts} = Electric.StackConfig.lookup!(stack_id, Electric.ShapeCache.Storage)
    opts
  end

  def opt_for_stack(stack_id, opt_name) do
    opts = opts_for_stack(stack_id)
    Map.fetch!(opts, opt_name)
  end

  @spec child_spec(shape_storage()) :: Supervisor.child_spec()
  def child_spec({module, shape_opts}) do
    %{
      id: {module, :per_consumer},
      start: {module, :start_link, [shape_opts]},
      restart: :transient
    }
  end

  @spec stack_child_spec(storage()) :: Supervisor.child_spec()
  def stack_child_spec({module, stack_opts}) do
    %{
      id: module,
      start: {__MODULE__, :stack_start_link, [{module, stack_opts}]},
      restart: :permanent
    }
  end

  @impl __MODULE__
  def shared_opts({module, opts}) do
    {module, module.shared_opts(opts)}
  end

  @impl __MODULE__
  def for_shape(shape_handle, {mod, opts}) do
    {mod, mod.for_shape(shape_handle, opts)}
  end

  @impl __MODULE__
  def stack_start_link({mod, opts} = storage) do
    Electric.StackConfig.put(opts.stack_id, __MODULE__, storage)
    mod.stack_start_link(opts)
  end

  @impl __MODULE__
  def start_link({mod, shape_opts}) do
    mod.start_link(shape_opts)
  end

  @impl __MODULE__
  def init_writer!({mod, shape_opts}, shape_definition) do
    {mod, mod.init_writer!(shape_opts, shape_definition)}
  end

  @impl __MODULE__
  def get_all_stored_shape_handles({mod, opts}) do
    mod.get_all_stored_shape_handles(opts)
  end

  @impl __MODULE__
  def get_total_disk_usage({mod, opts}) do
    mod.get_total_disk_usage(opts)
  end

  @impl __MODULE__
  def fetch_latest_offset({mod, shape_opts}) do
    mod.fetch_latest_offset(shape_opts)
  end

  @impl __MODULE__
  def fetch_pg_snapshot({mod, shape_opts}) do
    mod.fetch_pg_snapshot(shape_opts)
  end

  @impl __MODULE__
  def set_pg_snapshot(pg_snapshot, {mod, shape_opts}) do
    mod.set_pg_snapshot(pg_snapshot, shape_opts)
  end

  @impl __MODULE__
  def set_move_positions!(move_positions, {mod, shape_opts}) do
    mod.set_move_positions!(move_positions, shape_opts)
  end

  @impl __MODULE__
  def fetch_move_positions({mod, shape_opts}) do
    mod.fetch_move_positions(shape_opts)
  end

  @impl __MODULE__
  def fetch_root_delivery_tx_offset({mod, shape_opts}) do
    if Code.ensure_loaded?(mod) and
         function_exported?(mod, :fetch_root_delivery_tx_offset, 1) do
      mod.fetch_root_delivery_tx_offset(shape_opts)
    else
      {:ok, nil}
    end
  end

  @impl __MODULE__
  def get_log_replay_safe_cursor({mod, shape_opts}) do
    if Code.ensure_loaded?(mod) and function_exported?(mod, :get_log_replay_safe_cursor, 1) do
      mod.get_log_replay_safe_cursor(shape_opts)
    else
      case mod.fetch_latest_offset(shape_opts) do
        {:ok, %LogOffset{} = cursor} ->
          cursor

        {:error, reason} ->
          raise Error,
            message:
              "Storage adapter #{inspect(mod)} cannot determine a conservative replay-safe cursor: " <>
                inspect(reason)
      end
    end
  end

  @doc "Return whether a storage writer supports atomic dependency-move transactions."
  def supports_move_transactions?({mod, _writer_state}) do
    Code.ensure_loaded?(mod) and
      function_exported?(mod, :begin_move_transaction!, 1) and
      function_exported?(mod, :commit_move_transaction!, 3)
  end

  @doc "Return whether a storage reader supports exact, offset-preserving history reads."
  def supports_offset_preserving_log_stream?({mod, _shape_opts}) do
    Code.ensure_loaded?(mod) and function_exported?(mod, :get_log_stream_with_offsets, 3)
  end

  @doc """
  Validate the storage capabilities required by a shape.

  Ordinary shapes retain compatibility with storage adapters that predate the
  subquery replay contract. A shape with dependencies is rejected before cache
  creation unless its adapter implements every callback needed for exact replay
  and atomic dependency-move persistence.
  """
  @spec validate_shape_capabilities(Shape.t(), storage() | shape_storage()) ::
          :ok | {:error, Error.t()}
  def validate_shape_capabilities(%Shape{shape_dependencies: []}, _storage), do: :ok

  def validate_shape_capabilities(%Shape{}, {mod, _opts}) do
    loaded? = Code.ensure_loaded?(mod)

    missing_callbacks =
      @subquery_required_callbacks
      |> Enum.reject(fn {name, arity} -> loaded? and function_exported?(mod, name, arity) end)
      |> Enum.sort_by(fn {name, arity} -> {Atom.to_string(name), arity} end)

    case missing_callbacks do
      [] ->
        :ok

      missing_callbacks ->
        callbacks = Enum.map_join(missing_callbacks, ", ", &format_callback/1)

        {:error,
         %Error{
           message:
             "Storage adapter #{inspect(mod)} cannot serve subquery shapes; " <>
               "missing required callbacks: #{callbacks}"
         }}
    end
  end

  defp format_callback({name, arity}), do: "#{name}/#{arity}"

  @impl __MODULE__
  def begin_move_transaction!({mod, writer_state}) do
    if supports_move_transactions?({mod, writer_state}) do
      {mod, mod.begin_move_transaction!(writer_state)}
    else
      raise Error,
        message: "Storage adapter #{inspect(mod)} does not support dependency-move transactions"
    end
  end

  @impl __MODULE__
  def commit_move_transaction!(move_positions, root_delivery_tx_offset, {mod, writer_state}) do
    if supports_move_transactions?({mod, writer_state}) do
      {mod, mod.commit_move_transaction!(move_positions, root_delivery_tx_offset, writer_state)}
    else
      raise Error,
        message: "Storage adapter #{inspect(mod)} does not support dependency-move transactions"
    end
  end

  @impl __MODULE__
  def snapshot_started?({mod, shape_opts}) do
    mod.snapshot_started?(shape_opts)
  end

  @impl __MODULE__
  def make_new_snapshot!(stream, {mod, shape_opts}) do
    mod.make_new_snapshot!(stream, shape_opts)
  end

  @impl __MODULE__
  def mark_snapshot_as_started({mod, shape_opts}) do
    mod.mark_snapshot_as_started(shape_opts)
  end

  @impl __MODULE__
  def write_move_in_snapshot!(stream, name, {mod, shape_opts}) do
    mod.write_move_in_snapshot!(stream, name, shape_opts)
  end

  @impl __MODULE__
  def append_move_in_snapshot_to_log!(
        name,
        {mod, writer_state},
        skip_row? \\ fn _, _ -> false end
      ) do
    {inserted_range, new_writer_state} =
      mod.append_move_in_snapshot_to_log!(name, writer_state, skip_row?)

    {inserted_range, {mod, new_writer_state}}
  end

  @impl __MODULE__
  def append_control_message!(control_message, state)
      when is_map(control_message) do
    append_control_message!(Jason.encode!(control_message), state)
  end

  def append_control_message!(control_message, {mod, writer_state})
      when is_binary(control_message) do
    {inserted_range, new_writer_state} =
      mod.append_control_message!(control_message, writer_state)

    {inserted_range, {mod, new_writer_state}}
  end

  @impl __MODULE__
  def append_to_log!(log_items, {mod, shape_opts}) do
    {mod, mod.append_to_log!(log_items, shape_opts)}
  end

  @impl __MODULE__
  def supports_txn_fragment_streaming? do
    raise "supports_txn_fragment_streaming?/0 should be called on a specific storage module, " <>
            "or use supports_txn_fragment_streaming?/1 with a storage tuple"
  end

  @doc """
  Check if a storage backend supports txn fragment streaming.

  Takes a storage tuple `{module, opts}` and delegates to the module's
  `supports_txn_fragment_streaming?/0` callback.
  """
  def supports_txn_fragment_streaming?({mod, _opts}) do
    mod.supports_txn_fragment_streaming?()
  end

  @impl __MODULE__
  def append_fragment_to_log!(log_items, {mod, shape_opts}) do
    {mod, mod.append_fragment_to_log!(log_items, shape_opts)}
  end

  @impl __MODULE__
  def signal_txn_commit!(xid, {mod, shape_opts}) do
    {mod, mod.signal_txn_commit!(xid, shape_opts)}
  end

  @impl __MODULE__
  def get_log_stream(offset, max_offset \\ @last_log_offset, storage)

  def get_log_stream(offset, max_offset, {mod, shape_opts})
      when max_offset == @last_log_offset or not is_log_offset_lt(max_offset, offset) do
    mod.get_log_stream(offset, max_offset, shape_opts)
  end

  def get_log_stream(offset, max_offset, _storage) when is_log_offset_lt(max_offset, offset) do
    []
  end

  @doc """
  Get a shape log stream with the authoritative storage offset for each item.

  The lower bound is exclusive and the upper bound is inclusive, matching
  `get_log_stream/3`. Snapshot entries are paired with `nil` because they do
  not have main-log offsets.
  """
  @impl __MODULE__
  def get_log_stream_with_offsets(offset, max_offset \\ @last_log_offset, storage)

  def get_log_stream_with_offsets(offset, max_offset, {mod, shape_opts})
      when max_offset == @last_log_offset or not is_log_offset_lt(max_offset, offset) do
    if Code.ensure_loaded?(mod) and function_exported?(mod, :get_log_stream_with_offsets, 3) do
      mod.get_log_stream_with_offsets(offset, max_offset, shape_opts)
    else
      raise Error,
        message: "Storage adapter #{inspect(mod)} does not support offset-preserving log streams"
    end
  end

  def get_log_stream_with_offsets(offset, max_offset, _storage)
      when is_log_offset_lt(max_offset, offset) do
    []
  end

  @impl __MODULE__
  def get_chunk_end_log_offset(offset, {mod, shape_opts}) do
    mod.get_chunk_end_log_offset(offset, shape_opts)
  end

  @impl __MODULE__
  def terminate({mod, writer_state}) do
    mod.terminate(writer_state)
  end

  @impl __MODULE__
  def hibernate({mod, writer_state}) do
    {mod, mod.hibernate(writer_state)}
  end

  @impl __MODULE__
  def cleanup!({mod, shape_opts}) do
    mod.cleanup!(shape_opts)
  end

  @impl __MODULE__
  def cleanup!({mod, stack_opts}, shape_handle) do
    mod.cleanup!(stack_opts, shape_handle)
  end

  @impl __MODULE__
  def cleanup_all!({mod, opts}) do
    mod.cleanup_all!(opts)
  end

  @impl __MODULE__
  def compact({mod, shape_opts}, keep_complete_chunks \\ 2)
      when is_integer(keep_complete_chunks) and keep_complete_chunks >= 0 do
    mod.compact(shape_opts, keep_complete_chunks)
  end

  def trigger_compaction(server, {module, _opts}, keep_complete_chunks \\ 2)
      when is_integer(keep_complete_chunks) and keep_complete_chunks >= 0 do
    send(server, {__MODULE__, {module, :compact, [keep_complete_chunks]}})
  end
end
