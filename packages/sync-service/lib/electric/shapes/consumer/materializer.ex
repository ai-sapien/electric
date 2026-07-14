defmodule Electric.Shapes.Consumer.Materializer do
  # The lifecycle of a materializer is linked to its source consumer. If the consumer
  # goes down for any reason other than a clean supervisor/stack shutdown then we
  # need to invalidate all dependent outer shapes.
  #
  # restart: :temporary because the materalizer crashing brings down dependent shapes
  # and restarting would make no sense.
  use GenServer, restart: :temporary

  require Logger

  alias Electric.Utils
  alias Electric.Replication.Changes
  alias Electric.Shapes.Consumer
  alias Electric.ShapeCache.Storage
  alias Electric.Replication.LogOffset
  alias Electric.Replication.Eval
  alias Electric.Shapes.Shape
  alias __MODULE__.ReplayCoordinator

  import Electric.Replication.LogOffset
  import Electric, only: [is_stack_id: 1, is_shape_handle: 1]
  import Shape, only: :macros

  # `max_heap_size` is an absolute worker heap limit, while the configured
  # replay budget measures growth beyond the empty worker. Leave a small fixed
  # allowance for the process, schema template, monitors, and lease machinery.
  @replay_worker_base_heap_allowance_bytes 262_144
  @replay_progress_max_interval_ms 1_000

  @type causal_depth() :: non_neg_integer()
  @type causal_token() :: {:causal_batch, reference(), LogOffset.t(), causal_depth()}
  @type causal_reservation() :: {causal_token(), non_neg_integer()}

  def name(stack_id, shape_handle) when is_stack_id(stack_id) and is_shape_handle(shape_handle) do
    Electric.ProcessRegistry.name(stack_id, __MODULE__, shape_handle)
  end

  def name(%{
        stack_id: stack_id,
        shape_handle: shape_handle
      }) do
    name(stack_id, shape_handle)
  end

  def whereis(%{stack_id: stack_id, shape_handle: shape_handle}),
    do: whereis(stack_id, shape_handle)

  def whereis(stack_id, shape_handle), do: GenServer.whereis(name(stack_id, shape_handle))

  @spec new_changes(map(), list(Changes.change()) | {LogOffset.t(), LogOffset.t()}, keyword()) ::
          :ok
  def new_changes(state, changes, opts \\ []) do
    commit? = Keyword.get(opts, :commit, true)
    xid = Keyword.get(opts, :xid)
    defer_until_durable? = Keyword.get(opts, :defer_until_durable, false)
    end_offset = Keyword.get(opts, :end_offset)
    causal_token = Keyword.get(opts, :causal_token)

    GenServer.call(
      name(state),
      {:new_changes, changes, xid, commit?, defer_until_durable?, end_offset, causal_token},
      :infinity
    )
  end

  @doc false
  @spec new_causal_token(LogOffset.t(), causal_depth()) :: causal_token()
  def new_causal_token(%LogOffset{} = offset, depth \\ 0)
      when is_integer(depth) and depth >= 0,
      do: {:causal_batch, make_ref(), offset, depth}

  @doc false
  @spec causal_token_offset(causal_token()) :: LogOffset.t()
  def causal_token_offset({:causal_batch, ref, %LogOffset{} = offset, depth})
      when is_reference(ref) and is_integer(depth) and depth >= 0,
      do: offset

  @doc false
  @spec causal_token_depth(causal_token()) :: causal_depth()
  def causal_token_depth({:causal_batch, ref, %LogOffset{}, depth})
      when is_reference(ref) and is_integer(depth) and depth >= 0,
      do: depth

  @doc false
  @spec forward_causal_begin(map(), causal_token(), timeout()) :: :ok
  def forward_causal_begin(state, token, timeout \\ :infinity)

  def forward_causal_begin(
        state,
        {:causal_batch, ref, %LogOffset{}, depth} = token,
        timeout
      )
      when is_reference(ref) and is_integer(depth) and depth >= 0 do
    GenServer.call(name(state), {:forward_causal_begin, token}, timeout)
  end

  @doc false
  @spec forward_causal_end(map(), causal_token(), timeout()) :: :ok
  def forward_causal_end(state, token, timeout \\ :infinity)

  def forward_causal_end(
        state,
        {:causal_batch, ref, %LogOffset{}, depth} = token,
        timeout
      )
      when is_reference(ref) and is_integer(depth) and depth >= 0 do
    GenServer.call(name(state), {:forward_causal_end, token}, timeout)
  end

  @spec durable_up_to(map(), LogOffset.t()) :: :ok
  def durable_up_to(state, %LogOffset{} = offset) do
    GenServer.call(name(state), {:durable_up_to, offset}, :infinity)
  end

  def wait_until_ready(state) do
    GenServer.call(name(state), :wait_until_ready, :infinity)
  end

  @doc """
  Creates the per-stack ETS table that caches link values for all materializers
  in a stack. Called by `ConsumerRegistry` during stack initialization. Idempotent —
  safe to call when the table already exists.
  """
  @spec init_link_values_table(stack_id :: term()) :: :ets.table() | :undefined
  def init_link_values_table(stack_id) do
    :ets.new(link_values_table_name(stack_id), [
      :named_table,
      :public,
      :set,
      read_concurrency: true,
      write_concurrency: true
    ])
  rescue
    ArgumentError -> :ets.whereis(link_values_table_name(stack_id))
  end

  @doc """
  Returns the current set of materialized link values for a shape.
  Checks the shared ETS cache first (written after each committed transaction);
  falls back to a synchronous GenServer call if the cache has no entry yet.
  """
  def get_link_values(%{stack_id: stack_id, shape_handle: shape_handle} = opts) do
    table = link_values_table_name(stack_id)

    case :ets.lookup(table, shape_handle) do
      [{^shape_handle, values}] -> values
      _ -> genserver_get_link_values(opts)
    end
  rescue
    ArgumentError -> genserver_get_link_values(opts)
  end

  defp genserver_get_link_values(opts) do
    GenServer.call(name(opts), :get_link_values)
  catch
    :exit, reason ->
      raise "Materializer for stack #{inspect(opts.stack_id)} and handle " <>
              "#{inspect(opts.shape_handle)} is not available: #{inspect(reason)}"
  end

  def get_all_as_refs(shape, stack_id) when are_deps_filled(shape) do
    shape.shape_dependencies_handles
    |> Enum.with_index()
    |> Map.new(fn {shape_handle, index} ->
      {["$sublink", Integer.to_string(index)],
       get_link_values(%{
         shape_handle: shape_handle,
         stack_id: stack_id
       })}
    end)
  end

  @doc """
  Subscribe `pid` to this materializer's move events.

  `from_lsn` is the source LSN (LogOffset) up to which the subscribing outer
  consumer has already applied moves from this dependency, or `nil` for a fresh
  subscription. When `from_lsn` is behind the materializer's durable position,
  this creates a pull replay session for `pid`; call `next_replay/2` until it
  returns `:done` to catch up and atomically join live delivery.

  Returns `{:ok, seed_link_values, durable_offset}` when ready immediately. A
  stale subscriber returns `{:pending, durable_offset}` and later receives
  `{:materializer_replay_ready, shape_handle, result}` after the one bounded
  replay slot has reconstructed its seed. This keeps outer Consumers responsive
  while stale subscribers are serialized.
  """
  def subscribe(pid, from_lsn \\ nil)

  def subscribe(pid, from_lsn) when is_pid(pid),
    do: GenServer.call(pid, {:subscribe, from_lsn}, :infinity)

  def subscribe(opts, from_lsn) when is_map(opts),
    do: GenServer.call(name(opts), {:subscribe, from_lsn}, :infinity)

  def subscribe(stack_id, shape_handle) when is_stack_id(stack_id),
    do: subscribe(%{stack_id: stack_id, shape_handle: shape_handle})

  @doc false
  @spec subscribe_causally(pid(), LogOffset.t() | nil) ::
          {:ok, MapSet.t(), LogOffset.t(), [causal_reservation()]}
          | {:pending, LogOffset.t()}
          | {:error, term()}
  def subscribe_causally(pid, from_lsn) when is_pid(pid),
    do: GenServer.call(pid, {:subscribe, from_lsn, :causal}, :infinity)

  @doc false
  @spec subscribe_causally(pid(), LogOffset.t() | nil, pid()) ::
          {:ok, MapSet.t(), LogOffset.t(), [causal_reservation()]}
          | {:pending, LogOffset.t()}
          | {:error, term()}
  def subscribe_causally(pid, from_lsn, subscriber_pid)
      when is_pid(pid) and is_pid(subscriber_pid) do
    GenServer.call(pid, {:subscribe, from_lsn, :causal, subscriber_pid}, :infinity)
  end

  @doc """
  Pull at most one source transaction from a stale subscriber's replay session.

  Returns a normalized move payload, `:pending` while another stale subscriber
  owns the single bounded replay state, or `:done`. Causal subscribers receive
  `{:done, reservations}` so the outer Consumer can install byte-charged fences
  before processing later work. The done transition atomically joins live
  delivery with the final replay cursor check.
  """
  @spec next_replay(pid() | map(), pid()) ::
          {:ok, map()}
          | {:error, term()}
          | :pending
          | :done
          | {:done, [causal_reservation()]}
  def next_replay(materializer, subscriber_pid)

  def next_replay(materializer, subscriber_pid)
      when is_pid(materializer) and is_pid(subscriber_pid) do
    GenServer.call(materializer, {:next_replay, subscriber_pid}, :infinity)
  end

  def next_replay(opts, subscriber_pid) when is_map(opts) and is_pid(subscriber_pid) do
    GenServer.call(name(opts), {:next_replay, subscriber_pid}, :infinity)
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts,
      name: name(opts),
      spawn_opt: Electric.StackConfig.spawn_opts(opts.stack_id, :consumer_materializer)
    )
  end

  def init(opts) do
    %{stack_id: stack_id, shape_handle: shape_handle} = opts

    replay_memory_limit_bytes =
      Map.get_lazy(opts, :replay_memory_limit_bytes, fn ->
        Electric.StackConfig.lookup(
          stack_id,
          :materializer_replay_memory_limit_bytes,
          Electric.Config.default(:materializer_replay_memory_limit_bytes)
        )
      end)

    replay_max_pending =
      Map.get_lazy(opts, :replay_max_pending, fn ->
        Electric.StackConfig.lookup(
          stack_id,
          :materializer_replay_max_pending,
          Electric.Config.default(:materializer_replay_max_pending)
        )
      end)

    replay_idle_timeout_ms =
      Map.get_lazy(opts, :replay_idle_timeout_ms, fn ->
        Electric.StackConfig.lookup(
          stack_id,
          :materializer_replay_idle_timeout_ms,
          Electric.Config.default(:materializer_replay_idle_timeout_ms)
        )
      end)

    replay_progress_interval_ms =
      replay_idle_timeout_ms
      |> div(3)
      |> Kernel.max(1)
      |> Kernel.min(@replay_progress_max_interval_ms)

    live_max_subscribers =
      Map.get_lazy(opts, :live_max_subscribers, fn ->
        Electric.StackConfig.lookup(
          stack_id,
          :materializer_live_max_subscribers,
          Electric.Config.default(:materializer_live_max_subscribers)
        )
      end)

    live_backlog_memory_limit_bytes =
      Map.get_lazy(opts, :live_backlog_memory_limit_bytes, fn ->
        Electric.StackConfig.lookup(
          stack_id,
          :materializer_live_backlog_memory_limit_bytes,
          Electric.Config.default(:materializer_live_backlog_memory_limit_bytes)
        )
      end)

    live_backlog_max_pending =
      Map.get_lazy(opts, :live_backlog_max_pending, fn ->
        Electric.StackConfig.lookup(
          stack_id,
          :subquery_buffer_max_transactions,
          Electric.Config.default(:subquery_buffer_max_transactions)
        )
      end)

    causal_call_timeout_ms =
      Map.get_lazy(opts, :causal_call_timeout_ms, fn ->
        Electric.StackConfig.lookup(
          stack_id,
          :materializer_causal_call_timeout_ms,
          Electric.Config.default(:materializer_causal_call_timeout_ms)
        )
      end)

    Process.set_label({:materializer, shape_handle})
    metadata = [stack_id: stack_id, shape_handle: shape_handle]
    Logger.metadata(metadata)
    Electric.Telemetry.Sentry.set_tags_context(metadata)

    replay_coordinator_pid = GenServer.whereis(ReplayCoordinator.name(stack_id))

    if is_nil(replay_coordinator_pid) do
      {:stop, :replay_coordinator_unavailable}
    else
      replay_coordinator_monitor_ref = Process.monitor(replay_coordinator_pid)

      state =
        Map.merge(opts, %{
          index: %{},
          tag_indices: %{},
          value_counts: %{},
          pending_events: %{},
          pending_change_bytes: 0,
          pending_event_bytes: 0,
          offset: LogOffset.before_all(),
          # The highest source LSN (LogOffset) up to which changes have been
          # applied to `value_counts`. Used to tag emitted moves with their
          # source LSN and to bound move replay on subscribe.
          applied_offset: LogOffset.before_all(),
          # The highest durability notification observed from the source consumer.
          # This may be ahead of `applied_offset` when storage durability is
          # published before the corresponding source range reaches us.
          durability_watermark: LogOffset.before_all(),
          # The highest source LSN whose derived moves and link values have been
          # published. Keep this distinct from `durability_watermark`: subscribers
          # must never observe a cursor ahead of the materializer state itself.
          durable_offset: LogOffset.before_all(),
          durable_link_values: MapSet.new(),
          completed_event_batches: :queue.new(),
          completed_event_batch_count: 0,
          completed_event_batch_bytes: 0,
          subscribed_offset: nil,
          ref: nil,
          subscribers: MapSet.new(),
          # Production outer Consumers opt into a synchronous reservation before
          # durability-gated batches are accepted. Generic subscribers retain the
          # existing message-only protocol used by diagnostics and tests.
          causal_subscribers: MapSet.new(),
          # A causal token forwarded by an upstream dependency blocks every live
          # descendant until this source Consumer either turns it into one local
          # materializer batch or explicitly ends the no-change path.
          forwarded_causal_tokens: MapSet.new(),
          forwarded_causal_token_bytes: 0,
          # At most one stale subscriber has a replay worker for this materializer.
          # The worker owns the rebuilt state for its entire lifetime; this
          # GenServer retains only lifecycle metadata. A stack-wide lease further
          # ensures that distinct source materializers cannot each retain a
          # replay-sized index at the same time.
          replay_sessions: %{},
          pending_replay_subscribers: :queue.new(),
          pending_replay_cursors: %{},
          subscriber_monitors: %{},
          replay_memory_limit_bytes: replay_memory_limit_bytes,
          replay_max_pending: replay_max_pending,
          replay_progress_interval_ms: replay_progress_interval_ms,
          replay_coordinator_pid: replay_coordinator_pid,
          replay_coordinator_monitor_ref: replay_coordinator_monitor_ref,
          live_max_subscribers: live_max_subscribers,
          live_backlog_memory_limit_bytes: live_backlog_memory_limit_bytes,
          live_backlog_max_pending: live_backlog_max_pending,
          causal_call_timeout_ms: causal_call_timeout_ms
        })

      {:ok, state, {:continue, :start_materializer}}
    end
  end

  def handle_continue(:start_materializer, state) do
    %{stack_id: stack_id, shape_handle: shape_handle} = state

    stack_storage = Storage.for_stack(stack_id)
    shape_storage = Storage.for_shape(shape_handle, stack_storage)

    try do
      unless Storage.supports_offset_preserving_log_stream?(shape_storage) do
        {mod, _shape_opts} = shape_storage

        raise Storage.Error,
          message:
            "Storage adapter #{inspect(mod)} cannot provide exact history required by materializer #{shape_handle}"
      end

      case Consumer.await_snapshot_start(stack_id, shape_handle, :infinity) do
        :started ->
          {:ok, subscribed_offset} =
            Consumer.subscribe_materializer(stack_id, shape_handle, self())

          Process.monitor(Consumer.whereis(stack_id, shape_handle),
            tag: {:consumer_down, state.shape_handle}
          )

          {:noreply, %{state | subscribed_offset: subscribed_offset},
           {:continue, {:read_stream, shape_storage}}}

        {:error, _reason} ->
          {:stop, :shutdown, state}
      end
    rescue
      error in Storage.Error ->
        Logger.error(
          "Materializer startup rejected its storage adapter: #{Exception.message(error)}"
        )

        {:stop, :shutdown, state}
    catch
      # GenServer.call fails with :exit when Consumer is dead or dies mid-call
      :exit, reason ->
        Logger.warning("Materializer startup failed with exit reason: #{inspect(reason)}")
        {:stop, :shutdown, state}
    end
  end

  def handle_continue({:read_stream, storage}, state) do
    state = read_history_up_to_subscribed(state, storage)
    # After the startup replay, everything up to `subscribed_offset` has been
    # applied; seed `applied_offset` accordingly so live moves and replay are
    # tagged/bounded from the right position.
    state =
      if is_nil(state.subscribed_offset),
        do: state,
        else: %{
          state
          | applied_offset: state.subscribed_offset,
            durability_watermark: state.subscribed_offset,
            durable_offset: state.subscribed_offset,
            durable_link_values: link_values_from_counts(state.value_counts)
        }

    write_link_values(state)
    {:noreply, state}
  end

  @doc """
  Replay all of the source shape's persisted history (snapshot + log) up to
  `state.subscribed_offset` so the materializer's value_counts reflect the
  on-disk state on startup.

  `Storage.get_log_stream_with_offsets/3` returns at most one chunk per call
  for snapshot chunks and exactly the requested main-log range. We iterate
  through snapshot chunks using `Storage.get_chunk_end_log_offset/2`, then
  stop once the main log has been read through the subscribed boundary.

  The subscribed_offset is the Consumer's latest_offset at the time of
  subscription. We only read up to this offset to avoid duplicates — any
  changes after this offset will be delivered via new_changes messages
  from the Consumer.
  """
  def read_history_up_to_subscribed(state, storage, apply_fun \\ &default_history_apply/2)

  def read_history_up_to_subscribed(state, storage, apply_fun) do
    read_history_up_to_subscribed(state, storage, apply_fun, &exact_history_stream/3)
  end

  defp read_history_up_to_subscribed(state, storage, apply_fun, stream_fun) do
    cond do
      is_nil(state.subscribed_offset) ->
        state

      is_log_offset_lte(state.subscribed_offset, state.offset) ->
        state

      true ->
        stream = stream_fun.(state.offset, state.subscribed_offset, storage)
        state = apply_fun.(stream, state)

        # If the read just covered the main log (because either the
        # current offset is already past the snapshot or the next chunk
        # boundary jumps into real-offset territory), `stream_main_log`
        # returned the whole range up to `subscribed_offset` in a single
        # call and we're done.
        if is_real_offset(state.offset) or is_last_virtual_offset(state.offset) do
          %{state | offset: state.subscribed_offset}
        else
          next_offset = Storage.get_chunk_end_log_offset(state.offset, storage)

          cond do
            is_nil(next_offset) ->
              # No further chunks past this offset — we've reached the end.
              %{state | offset: state.subscribed_offset}

            is_log_offset_lte(next_offset, state.offset) ->
              # Defensive: chunk_end did not advance. Stop to avoid an
              # infinite loop. This shouldn't happen in normal operation.
              Logger.warning(
                "Materializer chunk iteration did not advance past " <>
                  "#{inspect(state.offset)} (chunk_end=#{inspect(next_offset)}); " <>
                  "stopping replay at subscribed_offset to avoid an infinite loop",
                shape_handle: state.shape_handle
              )

              %{state | offset: state.subscribed_offset}

            is_log_offset_lte(state.subscribed_offset, next_offset) ->
              %{state | offset: state.subscribed_offset}

            is_real_offset(next_offset) ->
              # The next chunk is in the main log, which means the call
              # we just made (with `state.offset` past the last snapshot
              # chunk) already streamed the entire main log up to
              # `subscribed_offset`. Stop — iterating further would
              # re-read entries we've already applied.
              %{state | offset: state.subscribed_offset}

            true ->
              read_history_up_to_subscribed(
                %{state | offset: next_offset},
                storage,
                apply_fun,
                stream_fun
              )
          end
        end
    end
  end

  # Default apply function for `read_history_up_to_subscribed/3`: apply the
  # decoded stream to `value_counts`, discarding the emitted move events (the
  # startup replay only needs the resulting state).
  defp default_history_apply(stream, state) do
    {state, _events} = stream |> decode_json_stream() |> apply_changes(state)
    state
  end

  # Reconstruct the exact dependency view at the persisted subscriber cursor.
  # Initial snapshot entries have virtual chunk offsets rather than per-row
  # offsets, so replay that section with the existing snapshot iterator. Read
  # the main-log section separately through the strictly bounded offset-aware
  # storage API; ordinary get_log_stream/3 reads whole storage chunks and may
  # intentionally include entries past its requested max offset.
  defp replay_seed_at(seed_template, from_lsn, storage, progress) do
    safe_cursor = Storage.get_log_replay_safe_cursor(storage)

    if is_log_offset_lt(from_lsn, safe_cursor) do
      {:error, {:replay_history_unavailable, safe_cursor}}
    else
      try do
        snapshot_end = LogOffset.min(from_lsn, LogOffset.last_before_real_offsets())

        seed0 =
          Map.merge(seed_template, %{
            index: %{},
            tag_indices: %{},
            value_counts: %{},
            offset: LogOffset.before_all(),
            subscribed_offset: snapshot_end,
            replay_bytes: 0
          })

        seed_state =
          read_history_up_to_subscribed(
            seed0,
            storage,
            fn stream, state -> bounded_history_apply(stream, state, progress) end,
            &exact_history_stream/3
          )

        seed_state =
          if is_log_offset_lt(snapshot_end, from_lsn) do
            state =
              Storage.get_log_stream_with_offsets(snapshot_end, from_lsn, storage)
              |> Stream.map(fn {_offset, item} -> item end)
              |> bounded_history_apply(seed_state, progress)

            %{state | offset: from_lsn, subscribed_offset: from_lsn}
          else
            seed_state
          end

        {:ok, compact_replay_state(seed_state), seed_state.replay_bytes}
      catch
        {:replay_memory_limit_exceeded, attempted_bytes, limit_bytes} ->
          {:error, {:replay_memory_limit_exceeded, attempted_bytes, limit_bytes}}

        {:replay_process_memory_limit_exceeded, attempted_bytes, limit_bytes} ->
          {:error, {:replay_process_memory_limit_exceeded, attempted_bytes, limit_bytes}}
      end
    end
  end

  defp compact_replay_state(state) do
    Map.take(state, [
      :shape_handle,
      :columns,
      :materialized_type,
      :index,
      :tag_indices,
      :value_counts
    ])
  end

  defp bounded_history_apply(stream, state, progress) do
    Enum.reduce(stream, state, fn item, state ->
      progress.()
      replay_bytes = state.replay_bytes + replay_item_cost(item)
      enforce_replay_memory_limit!(replay_bytes, state.replay_memory_limit_bytes)

      {state, _events} =
        [item]
        |> decode_json_stream()
        |> apply_changes(state)

      state = %{state | replay_bytes: replay_bytes}
      enforce_replay_process_memory_limit!(state)
      state
    end)
  end

  defp replay_item_cost(item), do: byte_size(item) + 64

  defp enforce_replay_memory_limit!(attempted_bytes, limit_bytes)
       when attempted_bytes > limit_bytes do
    throw({:replay_memory_limit_exceeded, attempted_bytes, limit_bytes})
  end

  defp enforce_replay_memory_limit!(_attempted_bytes, _limit_bytes), do: :ok

  # `Process.info/2` is constant-time and measures the worker's retained heap
  # without repeatedly traversing the rebuilt maps. The baseline excludes the
  # deliberately small worker/template overhead so this bound applies to the
  # additional replay state rather than BEAM process bookkeeping.
  defp enforce_replay_process_memory_limit!(state) do
    enforce_replay_process_memory_limit!(
      state.replay_memory_baseline_bytes,
      state.replay_memory_limit_bytes
    )
  end

  defp enforce_replay_process_memory_limit!(memory_baseline_bytes, memory_limit_bytes) do
    {:memory, current_bytes} = Process.info(self(), :memory)
    attempted_bytes = Kernel.max(current_bytes - memory_baseline_bytes, 0)

    if attempted_bytes > memory_limit_bytes do
      throw({:replay_process_memory_limit_exceeded, attempted_bytes, memory_limit_bytes})
    end

    :ok
  end

  defp exact_history_stream(min_offset, max_offset, storage) do
    Storage.get_log_stream_with_offsets(min_offset, max_offset, storage)
    |> Stream.map(fn {_offset, item} -> item end)
  end

  defp decode_txids(%{"txids" => txids}), do: validate_txids!(txids)
  defp decode_txids(_headers), do: []

  defp validate_txids!(txids) when is_list(txids) do
    if Enum.all?(txids, &(is_integer(&1) and &1 > 0)) do
      txids
    else
      raise ArgumentError, "persisted txids must be a list of positive integers"
    end
  end

  defp validate_txids!(_txids) do
    raise ArgumentError, "persisted txids must be a list of positive integers"
  end

  defp decode_change(%{
         "key" => key,
         "value" => value,
         "headers" => %{"operation" => operation} = headers
       }) do
    case operation do
      "insert" ->
        %Changes.NewRecord{
          key: key,
          record: value,
          move_tags: Map.get(headers, "tags", []),
          active_conditions: Map.get(headers, "active_conditions", [])
        }

      "update" ->
        %Changes.UpdatedRecord{
          key: key,
          record: value,
          move_tags: Map.get(headers, "tags", []),
          removed_move_tags: Map.get(headers, "removed_tags", []),
          active_conditions: Map.get(headers, "active_conditions", [])
        }

      "delete" ->
        %Changes.DeletedRecord{
          key: key,
          old_record: value,
          move_tags: Map.get(headers, "tags", []),
          active_conditions: Map.get(headers, "active_conditions", [])
        }
    end
  end

  defp decode_change(%{"headers" => %{"event" => event, "patterns" => patterns}})
       when event in ["move-out", "move-in"] do
    patterns =
      Enum.map(patterns, fn %{"pos" => pos, "value" => value} ->
        %{pos: pos, value: value}
      end)

    %{headers: %{event: event, patterns: patterns}}
  end

  defp pull_next_replay_payload(
         session,
         target_offset,
         storage,
         replay_memory_limit_bytes,
         progress
       ) do
    if is_log_offset_lte(target_offset, session.cursor) do
      :caught_up
    else
      safe_cursor = Storage.get_log_replay_safe_cursor(storage)

      if is_log_offset_lt(session.cursor, safe_cursor) do
        {:error, {:replay_history_unavailable, safe_cursor}}
      else
        case fold_next_storage_transaction(
               session,
               target_offset,
               storage,
               replay_memory_limit_bytes,
               progress
             ) do
          :none ->
            payload = normalize_move_payload(%{}, target_offset, [])
            {:ok, payload, %{session | cursor: target_offset}}

          {:ok, replay_offset, replay_state, replay_bytes, events, txids, causal_metadata} ->
            events =
              if events == %{},
                do: %{},
                else: finalized_pending_events(Map.put(events, :txids, txids))

            payload =
              normalize_move_payload(
                events,
                replay_offset,
                MapSet.to_list(txids),
                causal_metadata
              )

            {:ok, payload,
             %{
               session
               | cursor: replay_offset,
                 replay_state: replay_state,
                 replay_bytes: replay_bytes
             }}

          {:error, reason} ->
            {:error, reason}
        end
      end
    end
  end

  # Fold one logical source commit in a single lazy pass. Ordinary transactions
  # end on their real final change (`headers.last=true`); generated dependency
  # moves append one empty move-out marker with the same flag. The cumulative
  # byte budget bounds both a single huge PostgreSQL transaction and a long tail
  # that would otherwise rebuild a second unbounded source index.
  defp fold_next_storage_transaction(session, target_offset, storage, limit_bytes, progress) do
    initial =
      {:reading, session.replay_state, %{}, MapSet.new(), session.replay_bytes, nil}

    try do
      Storage.get_log_stream_with_offsets(session.cursor, target_offset, storage)
      |> Enum.reduce_while(initial, fn {offset, item},
                                       {:reading, replay_state, events, txids, replay_bytes,
                                        _last_offset} ->
        progress.()
        replay_bytes = replay_bytes + replay_item_cost(item)

        if replay_bytes > limit_bytes do
          {:halt, {:error, {:replay_memory_limit_exceeded, replay_bytes, limit_bytes}}}
        else
          decoded = Jason.decode!(item)
          headers = Map.get(decoded, "headers", %{})

          {replay_state, events, txids} =
            apply_replay_item(decoded, headers, replay_state, events, txids)

          enforce_replay_process_memory_limit!(
            session.replay_memory_baseline_bytes,
            limit_bytes
          )

          if Map.get(headers, "last", false) do
            case replay_causal_metadata(decoded, headers, offset) do
              {:ok, causal_metadata} ->
                {:halt, {:ok, offset, replay_state, replay_bytes, events, txids, causal_metadata}}

              {:error, reason} ->
                {:halt, {:error, reason}}
            end
          else
            {:cont, {:reading, replay_state, events, txids, replay_bytes, offset}}
          end
        end
      end)
      |> case do
        {:reading, _replay_state, _events, _txids, _replay_bytes, nil} ->
          :none

        {:reading, _replay_state, _events, _txids, _replay_bytes, last_offset} ->
          {:error, {:missing_replay_boundary, last_offset, target_offset}}

        result ->
          result
      end
    catch
      {:replay_process_memory_limit_exceeded, attempted_bytes, ^limit_bytes} ->
        {:error, {:replay_process_memory_limit_exceeded, attempted_bytes, limit_bytes}}
    end
  end

  defp apply_replay_item(decoded, headers, replay_state, events, txids) do
    if Map.has_key?(decoded, "key") or Map.has_key?(headers, "event") do
      change = decode_change(decoded)
      {replay_state, new_events} = apply_changes([change], replay_state)

      txids =
        MapSet.union(
          txids,
          headers |> decode_txids() |> MapSet.new()
        )

      {replay_state, merge_events(events, new_events), txids}
    else
      {replay_state, events, txids}
    end
  end

  defp replay_causal_metadata(decoded, headers, replay_offset) do
    case Map.get(headers, "generated_move_boundary") do
      1 ->
        decode_generated_move_causal_metadata(headers, replay_offset)

      nil ->
        if legacy_generated_move_boundary?(decoded, headers) do
          {:error, {:replay_causal_origin_unavailable, replay_offset}}
        else
          {:ok, nil}
        end

      version ->
        {:error, {:unsupported_generated_move_boundary, replay_offset, version}}
    end
  end

  defp decode_generated_move_causal_metadata(headers, replay_offset) do
    causal_origin = Map.get(headers, "causal_origin")
    causal_depth = Map.get(headers, "causal_depth")

    case causal_origin do
      causal_origin when is_binary(causal_origin) ->
        case LogOffset.from_string(causal_origin) do
          {:ok, %LogOffset{} = parsed_origin} when is_real_offset(parsed_origin) ->
            if is_integer(causal_depth) and causal_depth >= 0 do
              {:ok, {parsed_origin, causal_depth}}
            else
              {:error, {:invalid_replay_causal_depth, replay_offset, causal_depth}}
            end

          _invalid_origin ->
            {:error, {:invalid_replay_causal_origin, replay_offset, causal_origin}}
        end

      _invalid_origin ->
        {:error, {:invalid_replay_causal_origin, replay_offset, causal_origin}}
    end
  end

  defp legacy_generated_move_boundary?(decoded, headers) do
    not Map.has_key?(decoded, "key") and
      Map.get(headers, "event") == "move-out" and
      Map.get(headers, "patterns") == []
  end

  defp normalize_move_payload(events, lsn, txids \\ [], causal_metadata \\ nil) do
    payload = %{
      move_in: Map.get(events, :move_in, []),
      move_out: Map.get(events, :move_out, []),
      txids: Map.get(events, :txids, Enum.sort(Enum.uniq(txids))),
      lsn: lsn
    }

    case causal_metadata do
      nil ->
        payload

      {%LogOffset{} = causal_origin, causal_depth}
      when is_integer(causal_depth) and causal_depth >= 0 ->
        Map.merge(payload, %{causal_origin: causal_origin, causal_depth: causal_depth})
    end
  end

  def handle_call(:get_link_values, _from, %{durable_link_values: link_values} = state) do
    {:reply, link_values, state}
  end

  def handle_call(:wait_until_ready, _from, state) do
    {:reply, :ok, state}
  end

  def handle_call(
        {:new_changes, {_range_start, range_end}, _xid, _commit?, _defer_until_durable?,
         _end_offset, causal_token},
        _from,
        %{applied_offset: applied_offset} = state
      )
      when is_log_offset_lte(range_end, applied_offset) do
    # This range has already been applied — either during the startup history
    # replay (`read_history_up_to_subscribed`) or a previous `new_changes` call.
    # This happens on restart when the persistent replication slot re-delivers
    # already-persisted transactions. Re-applying them would raise
    # "Key already exists" in `apply_changes/2`, so skip the range entirely.
    {:reply, :ok, end_forwarded_causal_token(state, causal_token)}
  end

  def handle_call(
        {:new_changes, {range_start, range_end}, xid, commit?, defer_until_durable?, _end_offset,
         causal_token},
        _from,
        state
      ) do
    stack_storage = Storage.for_stack(state.stack_id)
    storage = Storage.for_shape(state.shape_handle, stack_storage)

    # Track the source LSN of this batch so emitted moves can be tagged with it
    # (used by outer consumers to dedup/replay moves across a restart).
    state = %{state | applied_offset: range_end}

    state =
      exact_history_stream(range_start, range_end, storage)
      |> decode_json_stream_with_txids()
      |> apply_and_accumulate_events(xid, state)
      |> maybe_flush_pending_events(commit?, defer_until_durable?, causal_token)

    {:reply, :ok, state}
  end

  def handle_call(
        {:new_changes, changes, xid, commit?, defer_until_durable?, end_offset, causal_token},
        _from,
        state
      )
      when is_list(changes) do
    state = advance_applied_offset(state, end_offset)

    state =
      changes
      |> apply_and_accumulate_events(xid, state)
      |> maybe_flush_pending_events(commit?, defer_until_durable?, causal_token)

    {:reply, :ok, state}
  end

  def handle_call({:subscribe, from_lsn}, from, state) do
    handle_subscribe(from_lsn, from, false, state)
  end

  def handle_call({:subscribe, from_lsn, :causal}, from, state) do
    handle_subscribe(from_lsn, from, true, state)
  end

  def handle_call({:subscribe, from_lsn, :causal, subscriber_pid}, from, state)
      when is_pid(subscriber_pid) do
    handle_subscribe(from_lsn, {subscriber_pid, elem(from, 1)}, true, state)
  end

  def handle_call({:next_replay, subscriber_pid}, from, state) do
    case Map.fetch(state.replay_sessions, subscriber_pid) do
      :error ->
        reply =
          if Map.has_key?(state.pending_replay_cursors, subscriber_pid),
            do: :pending,
            else: replay_done_reply(state, subscriber_pid)

        {:reply, reply, state}

      {:ok, %{status: :publishing_seed} = session} ->
        session = %{session | replay_requested?: true}

        {:reply, :pending,
         %{
           state
           | replay_sessions: Map.put(state.replay_sessions, subscriber_pid, session)
         }}

      {:ok, %{status: :seed_only} = session} ->
        case request_replay_lease(state, subscriber_pid, session, :replay) do
          {:ok, state} ->
            {:reply, :pending, state}

          {:error, reason, state} ->
            {:reply, {:error, reason}, remove_replay_subscriber(state, subscriber_pid, reason)}
        end

      {:ok, %{status: status}} when status != :ready ->
        {:reply, :pending, state}

      {:ok, %{in_flight: %{phase: :delivering}, queued_pull_from: nil} = session} ->
        session = %{session | queued_pull_from: from}

        {:noreply,
         %{
           state
           | replay_sessions: Map.put(state.replay_sessions, subscriber_pid, session)
         }}

      {:ok, %{in_flight: in_flight}} when not is_nil(in_flight) ->
        {:reply, :pending, state}

      {:ok, session} ->
        {:noreply, dispatch_replay_pull(state, subscriber_pid, session, from)}
    end
  end

  def handle_call({:durable_up_to, offset}, _from, state) do
    durability_watermark = LogOffset.max(state.durability_watermark, offset)
    state = %{state | durability_watermark: durability_watermark}

    {:reply, :ok, publish_durable_batches(state, durability_watermark)}
  end

  def handle_call({:forward_causal_begin, token}, _from, state) do
    if MapSet.member?(state.forwarded_causal_tokens, token) do
      raise ArgumentError, "duplicate forwarded materializer causal token"
    end

    fence_bytes = causal_end_message_bytes(state, token)
    ensure_live_backlog_capacity!(state, 1, fence_bytes, :forwarded_causal_fence)

    state = reserve_token_with_live_causal_subscribers(state, token, fence_bytes)

    state = %{
      state
      | forwarded_causal_tokens: MapSet.put(state.forwarded_causal_tokens, token),
        forwarded_causal_token_bytes: state.forwarded_causal_token_bytes + fence_bytes
    }

    {:reply, :ok, state}
  end

  def handle_call({:forward_causal_end, token}, _from, state) do
    unless MapSet.member?(state.forwarded_causal_tokens, token) do
      raise ArgumentError, "unknown forwarded materializer causal token"
    end

    state = state |> delete_forwarded_causal_token(token) |> publish_causal_end(token)
    {:reply, :ok, state}
  end

  defp handle_subscribe(from_lsn, {pid, _ref}, causal?, state) do
    state =
      state
      |> remove_replay_subscriber(pid)
      |> schedule_replay_promotion()

    cond do
      is_nil(from_lsn) and live_subscriber_limit_reached?(state, pid) ->
        reject_live_subscriber(state)

      is_nil(from_lsn) ->
        state =
          state
          |> put_causal_subscriber(pid, causal?)
          |> ensure_replay_subscriber_monitor(pid)
          |> Map.update!(:subscribers, &MapSet.put(&1, pid))

        {:reply, subscription_ready_reply(state, causal?), state}

      not match?(%LogOffset{}, from_lsn) ->
        {:reply, {:error, :invalid_replay_cursor}, state}

      is_log_offset_lt(state.durable_offset, from_lsn) ->
        # An ahead cursor indicates rolled-back/corrupt subscriber metadata. It
        # cannot be treated as caught up without silently skipping source moves.
        {:reply, {:error, :cursor_ahead_of_materializer}, state}

      is_log_offset_lte(state.durable_offset, from_lsn) and
          live_subscriber_limit_reached?(state, pid) ->
        reject_live_subscriber(state)

      is_log_offset_lte(state.durable_offset, from_lsn) ->
        state =
          state
          |> put_causal_subscriber(pid, causal?)
          |> ensure_replay_subscriber_monitor(pid)
          |> Map.update!(:subscribers, &MapSet.put(&1, pid))

        {:reply, subscription_ready_reply(state, causal?), state}

      replay_admission_count(state) >= state.replay_max_pending ->
        Logger.warning("Rejecting stale materializer subscriber because replay queue is full",
          replay_max_pending: state.replay_max_pending
        )

        {:reply, {:error, :replay_queue_full}, state}

      true ->
        state =
          state
          |> put_causal_subscriber(pid, causal?)
          |> ensure_replay_subscriber_monitor(pid)
          |> Map.update!(:pending_replay_subscribers, &:queue.in(pid, &1))
          |> Map.update!(:pending_replay_cursors, &Map.put(&1, pid, from_lsn))
          |> schedule_replay_promotion()

        {:reply, {:pending, state.durable_offset}, state}
    end
  end

  defp advance_applied_offset(state, nil), do: state

  defp advance_applied_offset(state, %LogOffset{} = end_offset) do
    %{state | applied_offset: LogOffset.max(state.applied_offset, end_offset)}
  end

  defp ensure_replay_subscriber_monitor(state, pid) do
    if Map.has_key?(state.subscriber_monitors, pid) do
      state
    else
      ref = Process.monitor(pid)
      %{state | subscriber_monitors: Map.put(state.subscriber_monitors, pid, ref)}
    end
  end

  defp replay_admission_count(state) do
    map_size(state.pending_replay_cursors) + map_size(state.replay_sessions)
  end

  defp live_subscriber_limit_reached?(state, pid) do
    not MapSet.member?(state.subscribers, pid) and
      MapSet.size(state.subscribers) >= state.live_max_subscribers
  end

  defp reject_live_subscriber(state) do
    Logger.warning("Rejecting live materializer subscriber because the fan-out limit is full",
      live_max_subscribers: state.live_max_subscribers
    )

    {:reply, {:error, :live_subscriber_limit}, state}
  end

  defp start_replay_worker(state, subscriber_pid, from_lsn) do
    session = %{
      job_ref: nil,
      worker_pid: nil,
      monitor_ref: nil,
      subscriber_pid: subscriber_pid,
      from_lsn: from_lsn,
      purpose: :seed,
      status: :new,
      replay_requested?: false,
      in_flight: nil,
      queued_pull_from: nil
    }

    case request_replay_lease(state, subscriber_pid, session, :seed) do
      {:ok, state} ->
        state

      {:error, reason, state} ->
        send(
          subscriber_pid,
          {:materializer_replay_ready, state.shape_handle, {:error, reason}}
        )

        state
        |> remove_replay_subscriber(subscriber_pid, reason)
        |> schedule_replay_promotion()
    end
  end

  defp request_replay_lease(state, subscriber_pid, session, purpose) do
    job_ref = make_ref()

    case ReplayCoordinator.request(state.stack_id, self(), job_ref) do
      :ok ->
        session = %{
          session
          | job_ref: job_ref,
            worker_pid: nil,
            monitor_ref: nil,
            purpose: purpose,
            status: :waiting_for_stack_lease,
            in_flight: nil,
            queued_pull_from: nil
        }

        {:ok,
         %{
           state
           | replay_sessions: Map.put(state.replay_sessions, subscriber_pid, session)
         }}

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp spawn_replay_worker(state, subscriber_pid, session) do
    stack_storage = Storage.for_stack(state.stack_id)
    storage = Storage.for_shape(state.shape_handle, stack_storage)
    materializer_pid = self()
    ancestors = Process.get(:"$ancestors")

    seed_template = %{
      shape_handle: state.shape_handle,
      columns: state.columns,
      materialized_type: state.materialized_type,
      replay_memory_limit_bytes: state.replay_memory_limit_bytes,
      replay_progress_interval_ms: state.replay_progress_interval_ms
    }

    word_size = :erlang.system_info(:wordsize)

    max_heap_words =
      div(
        state.replay_memory_limit_bytes + @replay_worker_base_heap_allowance_bytes +
          word_size - 1,
        word_size
      )

    {worker_pid, monitor_ref} =
      :erlang.spawn_opt(
        fn ->
          Process.put(:"$ancestors", ancestors)

          replay_worker_await_start(
            materializer_pid,
            session.job_ref,
            state.stack_id,
            seed_template,
            session.from_lsn,
            storage
          )
        end,
        [
          :monitor,
          {:max_heap_size, %{size: max_heap_words, kill: true, error_logger: false}}
        ]
      )

    case ReplayCoordinator.attach_worker(
           state.stack_id,
           self(),
           session.job_ref,
           worker_pid
         ) do
      :ok ->
        session = %{
          session
          | worker_pid: worker_pid,
            monitor_ref: monitor_ref,
            status: :seeding
        }

        state = %{
          state
          | replay_sessions: Map.put(state.replay_sessions, subscriber_pid, session)
        }

        send(worker_pid, {:begin_replay, self(), session.job_ref})
        state

      {:error, reason} ->
        Process.demonitor(monitor_ref, [:flush])
        Process.exit(worker_pid, :kill)

        notify_replay_failure(state, subscriber_pid, session, reason)

        state
        |> remove_replay_subscriber(subscriber_pid, reason)
        |> schedule_replay_promotion()
    end
  end

  defp replay_worker_await_start(
         owner,
         job_ref,
         stack_id,
         seed_template,
         from_lsn,
         storage
       ) do
    owner_monitor = Process.monitor(owner)

    receive do
      {:begin_replay, ^owner, ^job_ref} ->
        replay_worker_start(
          owner,
          owner_monitor,
          job_ref,
          stack_id,
          seed_template,
          from_lsn,
          storage
        )

      {:shutdown, ^owner} ->
        :ok

      {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
        :ok
    end
  end

  defp replay_worker_start(
         owner,
         owner_monitor,
         job_ref,
         stack_id,
         seed_template,
         from_lsn,
         storage
       ) do
    activate_mocked_functions_from_test_process()
    Process.set_label({:materializer_replay, seed_template.shape_handle})

    if not Process.alive?(owner), do: exit(:normal)

    progress =
      replay_progress_reporter(
        stack_id,
        owner,
        job_ref,
        seed_template.replay_progress_interval_ms
      )

    {:memory, memory_baseline_bytes} = Process.info(self(), :memory)

    seed_template =
      Map.put(seed_template, :replay_memory_baseline_bytes, memory_baseline_bytes)

    case replay_seed_at(seed_template, from_lsn, storage, progress) do
      {:ok, replay_state, replay_bytes} ->
        :ok = ReplayCoordinator.progress(stack_id, owner, job_ref, self())
        send(owner, {:replay_worker_seed_built, job_ref, self()})

        replay_worker_loop(owner, owner_monitor, job_ref, stack_id, storage, %{
          cursor: from_lsn,
          replay_state: replay_state,
          replay_bytes: replay_bytes,
          replay_memory_baseline_bytes: memory_baseline_bytes,
          replay_memory_limit_bytes: seed_template.replay_memory_limit_bytes,
          progress: progress
        })

      {:error, reason} ->
        send(owner, {:replay_worker_seed_error, job_ref, self(), reason})
    end
  end

  defp replay_progress_reporter(stack_id, owner, job_ref, interval_ms) do
    progress_key = {__MODULE__, :last_replay_progress}
    Process.put(progress_key, System.monotonic_time(:millisecond))

    fn ->
      now = System.monotonic_time(:millisecond)
      last_progress = Process.get(progress_key, now)

      if now - last_progress >= interval_ms do
        :ok = ReplayCoordinator.progress(stack_id, owner, job_ref, self())
        Process.put(progress_key, now)
      end

      :ok
    end
  end

  defp notify_replay_failure(state, subscriber_pid, %{purpose: :seed}, reason) do
    send(
      subscriber_pid,
      {:materializer_replay_ready, state.shape_handle, {:error, reason}}
    )
  end

  defp notify_replay_failure(state, subscriber_pid, _session, _reason) do
    send(subscriber_pid, {:materializer_shape_invalidated, state.shape_handle})
  end

  defp replay_worker_loop(owner, owner_monitor, job_ref, stack_id, storage, session) do
    receive do
      {:publish_seed, ^owner, subscriber_pid, shape_handle, durable_offset} ->
        :ok = ReplayCoordinator.progress(stack_id, owner, job_ref, self())

        try do
          seed_link_values =
            bounded_link_values_from_counts(
              session.replay_state.value_counts,
              session.replay_memory_baseline_bytes,
              session.replay_memory_limit_bytes,
              session.progress
            )

          enforce_replay_process_memory_limit!(
            session.replay_memory_baseline_bytes,
            session.replay_memory_limit_bytes
          )

          send(
            subscriber_pid,
            {:materializer_replay_ready, shape_handle, {:ok, seed_link_values, durable_offset}}
          )

          send(owner, {:replay_worker_seed_published, job_ref, self()})
        catch
          {:replay_process_memory_limit_exceeded, attempted_bytes, limit_bytes} ->
            send(
              owner,
              {:replay_worker_seed_error, job_ref, self(),
               {:replay_process_memory_limit_exceeded, attempted_bytes, limit_bytes}}
            )
        end

        replay_worker_await_retirement(owner, owner_monitor)

      {:next_replay, ^owner, request_ref, from, target_offset} ->
        :ok = ReplayCoordinator.progress(stack_id, owner, job_ref, self())

        case pull_next_replay_payload(
               session,
               target_offset,
               storage,
               session.replay_memory_limit_bytes,
               session.progress
             ) do
          {:ok, payload, session} ->
            :ok = ReplayCoordinator.progress(stack_id, owner, job_ref, self())

            send(
              owner,
              {:replay_worker_pull_result, job_ref, self(), request_ref, :payload_ready}
            )

            replay_worker_await_payload_delivery(
              owner,
              owner_monitor,
              job_ref,
              stack_id,
              storage,
              session,
              request_ref,
              from,
              payload
            )

          {:error, reason} ->
            :ok = ReplayCoordinator.progress(stack_id, owner, job_ref, self())

            send(
              owner,
              {:replay_worker_pull_result, job_ref, self(), request_ref, {:error, reason}}
            )

            replay_worker_loop(owner, owner_monitor, job_ref, stack_id, storage, session)

          :caught_up ->
            :ok = ReplayCoordinator.progress(stack_id, owner, job_ref, self())

            send(
              owner,
              {:replay_worker_pull_result, job_ref, self(), request_ref, :caught_up}
            )

            replay_worker_loop(owner, owner_monitor, job_ref, stack_id, storage, session)
        end

      {:shutdown, ^owner} ->
        :ok

      {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
        :ok

      _other ->
        replay_worker_loop(owner, owner_monitor, job_ref, stack_id, storage, session)
    end
  end

  defp replay_worker_await_retirement(owner, owner_monitor) do
    receive do
      {:shutdown, ^owner} ->
        :ok

      {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
        :ok
    end
  end

  defp replay_worker_await_payload_delivery(
         owner,
         owner_monitor,
         job_ref,
         stack_id,
         storage,
         session,
         request_ref,
         from,
         payload
       ) do
    receive do
      {:deliver_replay_payload, ^owner, ^request_ref} ->
        :ok = ReplayCoordinator.progress(stack_id, owner, job_ref, self())
        GenServer.reply(from, {:ok, payload})

        send(
          owner,
          {:replay_worker_payload_delivered, job_ref, self(), request_ref}
        )

        replay_worker_loop(owner, owner_monitor, job_ref, stack_id, storage, session)

      {:shutdown, ^owner} ->
        :ok

      {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
        :ok
    end
  end

  defp schedule_replay_promotion(state) do
    unless replay_lease_in_use?(state), do: send(self(), :promote_replay_subscriber)
    state
  end

  defp replay_lease_in_use?(state) do
    Enum.any?(state.replay_sessions, fn {_pid, session} ->
      session.status != :seed_only
    end)
  end

  defp dispatch_replay_pull(state, subscriber_pid, session, from) do
    request_ref = make_ref()
    target_offset = state.durable_offset
    send(session.worker_pid, {:next_replay, self(), request_ref, from, target_offset})

    session = %{
      session
      | in_flight: %{
          request_ref: request_ref,
          from: from,
          target_offset: target_offset,
          phase: :reading
        }
    }

    %{state | replay_sessions: Map.put(state.replay_sessions, subscriber_pid, session)}
  end

  defp cancel_replay_session(state, subscriber_pid, reply_reason \\ nil) do
    case Map.pop(state.replay_sessions, subscriber_pid) do
      {nil, _sessions} ->
        state

      {session, sessions} ->
        if reply_reason && session.in_flight do
          GenServer.reply(session.in_flight.from, {:error, reply_reason})
        end

        if reply_reason && session.queued_pull_from do
          GenServer.reply(session.queued_pull_from, {:error, reply_reason})
        end

        stop_owned_replay_worker(session)
        release_replay_lease(state.stack_id, session.job_ref)

        %{state | replay_sessions: sessions}
    end
  end

  defp retire_replay_worker(stack_id, session) do
    stop_owned_replay_worker(session)
    release_replay_lease(stack_id, session.job_ref)

    %{
      session
      | job_ref: nil,
        worker_pid: nil,
        monitor_ref: nil,
        purpose: nil,
        in_flight: nil,
        queued_pull_from: nil
    }
  end

  defp release_replay_lease(_stack_id, nil), do: :ok

  defp release_replay_lease(stack_id, job_ref) do
    ReplayCoordinator.release(stack_id, self(), job_ref)
  catch
    :exit, reason when reason in [:noproc, :normal, :shutdown] -> :ok
    :exit, {reason, _call} when reason in [:noproc, :normal, :shutdown] -> :ok
  end

  defp stop_owned_replay_worker(%{worker_pid: worker_pid, monitor_ref: monitor_ref})
       when is_pid(worker_pid) and is_reference(monitor_ref) do
    if Process.alive?(worker_pid) do
      Process.exit(worker_pid, :kill)

      receive do
        {:DOWN, ^monitor_ref, :process, ^worker_pid, _reason} -> :ok
      after
        1_000 -> Process.demonitor(monitor_ref, [:flush])
      end
    else
      Process.demonitor(monitor_ref, [:flush])
    end
  end

  defp stop_owned_replay_worker(_session), do: :ok

  defp remove_replay_subscriber(state, pid, reply_reason \\ :replay_cancelled) do
    pending_replay_subscribers =
      state.pending_replay_subscribers
      |> :queue.to_list()
      |> Enum.reject(&(&1 == pid))
      |> :queue.from_list()

    state = cancel_replay_session(state, pid, reply_reason)
    {monitor_ref, subscriber_monitors} = Map.pop(state.subscriber_monitors, pid)
    if is_reference(monitor_ref), do: Process.demonitor(monitor_ref, [:flush])

    %{
      state
      | subscribers: MapSet.delete(state.subscribers, pid),
        causal_subscribers: MapSet.delete(state.causal_subscribers, pid),
        pending_replay_subscribers: pending_replay_subscribers,
        pending_replay_cursors: Map.delete(state.pending_replay_cursors, pid),
        subscriber_monitors: subscriber_monitors
    }
  end

  defp put_causal_subscriber(state, pid, true),
    do: Map.update!(state, :causal_subscribers, &MapSet.put(&1, pid))

  defp put_causal_subscriber(state, _pid, false), do: state

  defp subscription_ready_reply(state, true) do
    {:ok, state.durable_link_values, state.durable_offset, pending_causal_tokens(state)}
  end

  defp subscription_ready_reply(state, false) do
    {:ok, state.durable_link_values, state.durable_offset}
  end

  defp replay_done_reply(state, subscriber_pid) do
    if MapSet.member?(state.causal_subscribers, subscriber_pid),
      do: {:done, pending_causal_tokens(state)},
      else: :done
  end

  defp pending_causal_tokens(state) do
    completed_reservations =
      state.completed_event_batches
      |> :queue.to_list()
      |> Enum.map(fn {_offset, _events, token, delivery_bytes} ->
        {token, delivery_bytes}
      end)

    forwarded_reservations =
      Enum.map(state.forwarded_causal_tokens, fn token ->
        {token, causal_end_message_bytes(state, token)}
      end)

    (completed_reservations ++ forwarded_reservations)
    |> Enum.sort(fn {left, _left_bytes}, {right, _right_bytes} ->
      left_offset = causal_token_offset(left)
      right_offset = causal_token_offset(right)

      cond do
        left_offset.tx_offset < right_offset.tx_offset ->
          true

        left_offset.tx_offset > right_offset.tx_offset ->
          false

        causal_token_depth(left) < causal_token_depth(right) ->
          true

        causal_token_depth(left) > causal_token_depth(right) ->
          false

        true ->
          case LogOffset.compare(left_offset, right_offset) do
            :lt -> true
            :gt -> false
            :eq -> left <= right
          end
      end
    end)
  end

  defp promote_next_replay_subscriber(state) do
    if replay_lease_in_use?(state) do
      state
    else
      do_promote_next_replay_subscriber(state)
    end
  end

  defp do_promote_next_replay_subscriber(state) do
    case :queue.out(state.pending_replay_subscribers) do
      {:empty, _queue} ->
        state

      {{:value, pid}, queue} ->
        {from_lsn, pending_replay_cursors} = Map.pop(state.pending_replay_cursors, pid)

        state = %{
          state
          | pending_replay_subscribers: queue,
            pending_replay_cursors: pending_replay_cursors
        }

        cond do
          is_nil(from_lsn) or not Process.alive?(pid) ->
            do_promote_next_replay_subscriber(state)

          true ->
            start_replay_worker(state, pid, from_lsn)
        end
    end
  end

  # if the supervisor is going down then this process will also be taken down
  # but let's state the dependency explictly.
  def handle_info({{:consumer_down, _}, _ref, :process, _pid, :shutdown}, state) do
    {:stop, :shutdown, state}
  end

  def handle_info({{:consumer_down, _}, _ref, :process, _pid, {:shutdown, reason}}, state)
      when reason != :cleanup do
    {:stop, :shutdown, state}
  end

  # notify subscribers of the shape removal if the consumer exit reason is
  # anything other than a clean supervisor shutdown.
  def handle_info({{:consumer_down, _}, _ref, :process, _pid, _reason}, state) do
    for pid <- replay_subscriber_pids(state) do
      send(pid, {:materializer_shape_invalidated, state.shape_handle})
    end

    {:stop, :shutdown, state}
  end

  def handle_info(
        {:replay_coordinator_granted, job_ref},
        state
      ) do
    state =
      case find_replay_session_by_job(state, job_ref) do
        {:ok, subscriber_pid, %{worker_pid: nil} = session} ->
          if Process.alive?(subscriber_pid) do
            spawn_replay_worker(state, subscriber_pid, session)
          else
            state
            |> cancel_replay_session(subscriber_pid)
            |> schedule_replay_promotion()
          end

        _stale_or_started ->
          state
      end

    {:noreply, state}
  end

  def handle_info({:replay_worker_seed_built, job_ref, worker_pid}, state) do
    case find_matching_replay_session(state, job_ref, worker_pid) do
      :error ->
        {:noreply, state}

      {:ok, subscriber_pid, %{purpose: :seed} = session} ->
        if Process.alive?(subscriber_pid) do
          session = %{session | status: :publishing_seed}

          send(
            worker_pid,
            {:publish_seed, self(), subscriber_pid, state.shape_handle, state.durable_offset}
          )

          {:noreply,
           %{
             state
             | replay_sessions: Map.put(state.replay_sessions, subscriber_pid, session)
           }}
        else
          {:noreply,
           state
           |> remove_replay_subscriber(subscriber_pid)
           |> schedule_replay_promotion()}
        end

      {:ok, subscriber_pid, %{purpose: :replay} = session} ->
        if Process.alive?(subscriber_pid) do
          session = %{session | status: :ready}
          send(subscriber_pid, {:materializer_replay_ready, state.shape_handle})

          {:noreply,
           %{
             state
             | replay_sessions: Map.put(state.replay_sessions, subscriber_pid, session)
           }}
        else
          {:noreply,
           state
           |> remove_replay_subscriber(subscriber_pid)
           |> schedule_replay_promotion()}
        end
    end
  end

  def handle_info({:replay_worker_seed_published, job_ref, worker_pid}, state) do
    case find_matching_replay_session(state, job_ref, worker_pid) do
      {:ok, subscriber_pid, %{purpose: :seed, status: :publishing_seed} = session} ->
        session = retire_replay_worker(state.stack_id, session)

        state = %{
          state
          | replay_sessions:
              Map.put(state.replay_sessions, subscriber_pid, %{session | status: :seed_only})
        }

        state =
          if session.replay_requested? and Process.alive?(subscriber_pid) do
            case request_replay_lease(state, subscriber_pid, session, :replay) do
              {:ok, state} ->
                state

              {:error, reason, state} ->
                send(subscriber_pid, {:materializer_shape_invalidated, state.shape_handle})
                remove_replay_subscriber(state, subscriber_pid, reason)
            end
          else
            state
          end

        {:noreply, schedule_replay_promotion(state)}

      _stale_or_wrong_phase ->
        {:noreply, state}
    end
  end

  def handle_info({:replay_worker_timeout, job_ref, worker_pid}, state) do
    case find_replay_session_by_job(state, job_ref) do
      {:ok, subscriber_pid, session}
      when is_nil(worker_pid) or session.worker_pid == worker_pid ->
        if Process.alive?(subscriber_pid),
          do: notify_replay_failure(state, subscriber_pid, session, :replay_idle_timeout)

        {:noreply,
         state
         |> remove_replay_subscriber(subscriber_pid, :replay_idle_timeout)
         |> schedule_replay_promotion()}

      _stale ->
        {:noreply, state}
    end
  end

  def handle_info({:replay_worker_seed_error, job_ref, worker_pid, reason}, state) do
    case find_matching_replay_session(state, job_ref, worker_pid) do
      :error ->
        {:noreply, state}

      {:ok, subscriber_pid, session} ->
        if Process.alive?(subscriber_pid),
          do: notify_replay_failure(state, subscriber_pid, session, reason)

        {:noreply,
         state
         |> remove_replay_subscriber(subscriber_pid, {:replay_seed_failed, reason})
         |> schedule_replay_promotion()}
    end
  end

  def handle_info(
        {:replay_worker_pull_result, job_ref, worker_pid, request_ref, result},
        state
      ) do
    case find_matching_replay_session(state, job_ref, worker_pid) do
      {:ok, subscriber_pid,
       %{in_flight: %{request_ref: ^request_ref, from: from, target_offset: target_offset}} =
           session} ->
        case result do
          :payload_ready ->
            session = %{session | in_flight: %{session.in_flight | phase: :delivering}}
            send(worker_pid, {:deliver_replay_payload, self(), request_ref})

            {:noreply,
             %{
               state
               | replay_sessions: Map.put(state.replay_sessions, subscriber_pid, session)
             }}

          {:error, reason} ->
            {:noreply,
             state
             |> remove_replay_subscriber(subscriber_pid, reason)
             |> schedule_replay_promotion()}

          :caught_up ->
            if is_log_offset_lte(state.durable_offset, target_offset) do
              if live_subscriber_limit_reached?(state, subscriber_pid) do
                GenServer.reply(from, {:error, :live_subscriber_limit})

                {:noreply,
                 state
                 |> cancel_replay_session(subscriber_pid)
                 |> remove_replay_subscriber(subscriber_pid)
                 |> schedule_replay_promotion()}
              else
                state =
                  state
                  |> cancel_replay_session(subscriber_pid)
                  |> Map.update!(:subscribers, &MapSet.put(&1, subscriber_pid))
                  |> schedule_replay_promotion()

                GenServer.reply(from, replay_done_reply(state, subscriber_pid))

                {:noreply, state}
              end
            else
              {:noreply, dispatch_replay_pull(state, subscriber_pid, session, from)}
            end
        end

      _stale_or_unknown ->
        {:noreply, state}
    end
  end

  def handle_info(
        {:replay_worker_payload_delivered, job_ref, worker_pid, request_ref},
        state
      ) do
    case find_matching_replay_session(state, job_ref, worker_pid) do
      {:ok, subscriber_pid,
       %{in_flight: %{request_ref: ^request_ref, phase: :delivering}} = session} ->
        {queued_pull_from, session} =
          Map.get_and_update!(session, :queued_pull_from, fn from -> {from, nil} end)

        session = %{session | in_flight: nil}

        state = %{
          state
          | replay_sessions: Map.put(state.replay_sessions, subscriber_pid, session)
        }

        if queued_pull_from do
          {:noreply, dispatch_replay_pull(state, subscriber_pid, session, queued_pull_from)}
        else
          {:noreply, state}
        end

      _stale_or_unknown ->
        {:noreply, state}
    end
  end

  def handle_info(
        {:DOWN, ref, :process, pid, reason},
        %{replay_coordinator_monitor_ref: ref, replay_coordinator_pid: pid} = state
      ) do
    for subscriber_pid <- replay_subscriber_pids(state) do
      send(subscriber_pid, {:materializer_shape_invalidated, state.shape_handle})
    end

    Logger.error("Materializer replay coordinator terminated; invalidating dependency shape",
      coordinator_pid: inspect(pid),
      reason: inspect(reason)
    )

    {:stop, {:replay_coordinator_down, reason}, state}
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    state =
      cond do
        Map.get(state.subscriber_monitors, pid) == ref ->
          state
          |> remove_replay_subscriber(pid)
          |> schedule_replay_promotion()

        match?({:ok, _, _}, find_replay_session_by_monitor(state, ref, pid)) ->
          {:ok, subscriber_pid, _session} = find_replay_session_by_monitor(state, ref, pid)

          if Process.alive?(subscriber_pid) do
            send(subscriber_pid, {:materializer_shape_invalidated, state.shape_handle})
          end

          Logger.warning("Materializer replay worker terminated unexpectedly",
            subscriber_pid: inspect(subscriber_pid),
            reason: inspect(reason)
          )

          state
          |> remove_replay_subscriber(
            subscriber_pid,
            {:replay_worker_failed, reason}
          )
          |> schedule_replay_promotion()

        true ->
          state
      end

    {:noreply, state}
  end

  def handle_info(:promote_replay_subscriber, state) do
    {:noreply, promote_next_replay_subscriber(state)}
  end

  def handle_info(message, state)
      when is_tuple(message) and
             elem(message, 0) in [
               :replay_coordinator_granted,
               :replay_worker_timeout,
               :replay_worker_seed_built,
               :replay_worker_seed_published,
               :replay_worker_seed_error,
               :replay_worker_pull_result,
               :replay_worker_payload_delivered
             ],
      do: {:noreply, state}

  defp replay_subscriber_pids(state) do
    state.subscribers
    |> MapSet.union(MapSet.new(Map.keys(state.replay_sessions)))
    |> MapSet.union(MapSet.new(Map.keys(state.pending_replay_cursors)))
  end

  defp find_matching_replay_session(state, job_ref, worker_pid) do
    Enum.find_value(state.replay_sessions, :error, fn {subscriber_pid, session} ->
      if session.job_ref == job_ref and session.worker_pid == worker_pid,
        do: {:ok, subscriber_pid, session},
        else: false
    end)
  end

  defp find_replay_session_by_job(state, job_ref) do
    Enum.find_value(state.replay_sessions, :error, fn {subscriber_pid, session} ->
      if session.job_ref == job_ref,
        do: {:ok, subscriber_pid, session},
        else: false
    end)
  end

  defp find_replay_session_by_monitor(state, monitor_ref, worker_pid) do
    Enum.find_value(state.replay_sessions, :error, fn {subscriber_pid, session} ->
      if session.monitor_ref == monitor_ref and session.worker_pid == worker_pid,
        do: {:ok, subscriber_pid, session},
        else: false
    end)
  end

  def terminate(_reason, state) do
    delete_link_values(state.stack_id, state.shape_handle)

    Enum.each(state.replay_sessions, fn {_subscriber_pid, session} ->
      stop_owned_replay_worker(session)
      release_replay_lease(state.stack_id, session.job_ref)
    end)

    :ok
  end

  @spec link_values_table_name(Electric.stack_id()) :: atom()
  def link_values_table_name(stack_id) do
    :"Electric.Materializer.LinkValues:#{stack_id}"
  end

  @doc """
  Removes the cached link values for `shape_handle` from the shared ETS table.
  Safe to call even if the table does not exist (e.g. after a stack shutdown).
  """
  @spec delete_link_values(Electric.stack_id(), Electric.shape_handle()) :: :ok
  def delete_link_values(stack_id, shape_handle) do
    :ets.delete(link_values_table_name(stack_id), shape_handle)
    :ok
  rescue
    ArgumentError ->
      Logger.debug(fn ->
        "delete_link_values: link-values table for stack #{inspect(stack_id)} " <>
          "not found when deleting handle #{inspect(shape_handle)}"
      end)

      :ok
  end

  defp link_values_from_counts(value_counts) do
    MapSet.new(Map.keys(value_counts))
  end

  # Do not materialize `Map.keys/1` and then build a second full collection in
  # one uninterruptible allocation. The worker constructs the subscriber seed
  # incrementally and checks its retained heap after every inserted value.
  defp bounded_link_values_from_counts(
         value_counts,
         memory_baseline_bytes,
         memory_limit_bytes,
         progress
       ) do
    Enum.reduce(value_counts, MapSet.new(), fn {value, _count}, link_values ->
      progress.()
      link_values = MapSet.put(link_values, value)
      enforce_replay_process_memory_limit!(memory_baseline_bytes, memory_limit_bytes)
      link_values
    end)
  end

  defp write_link_values(%{
         stack_id: stack_id,
         shape_handle: shape_handle,
         durable_link_values: link_values
       }) do
    :ets.insert(
      link_values_table_name(stack_id),
      {shape_handle, link_values}
    )
  rescue
    ArgumentError ->
      Logger.warning(
        "write_link_values: link-values ETS table missing for stack #{inspect(stack_id)} " <>
          "— cache will fall back to GenServer calls for handle #{inspect(shape_handle)}"
      )

      :ok
  end

  defp decode_json_stream(stream) do
    stream
    |> decode_json_items()
    |> Stream.map(&decode_change/1)
  end

  defp decode_json_stream_with_txids(stream) do
    stream
    |> decode_json_items()
    |> Stream.map(fn %{"headers" => headers} = decoded ->
      {decode_change(decoded), decode_txids(headers)}
    end)
  end

  defp decode_json_items(stream) do
    stream
    |> Stream.map(&Jason.decode!/1)
    |> Stream.filter(fn decoded ->
      Map.has_key?(decoded, "key") || Map.has_key?(decoded["headers"], "event")
    end)
  end

  defp cast!(record, %{columns: columns, materialized_type: {:array, {:row, types}}}) do
    original_strings = Enum.map(columns, &Map.fetch!(record, &1))

    {:ok, values} =
      Enum.zip(original_strings, types)
      |> Utils.map_while_ok(fn {const, type} ->
        Eval.Env.parse_const(Eval.Env.new(), const, type)
      end)

    {List.to_tuple(values), List.to_tuple(original_strings)}
  end

  defp cast!(record, %{columns: [column], materialized_type: {:array, type}}) do
    original_string = Map.fetch!(record, column)
    {:ok, value} = Eval.Env.parse_const(Eval.Env.new(), original_string, type)
    {value, original_string}
  end

  defp value_to_string(value, %{materialized_type: {:array, {:row, type}}}) do
    value
    |> Tuple.to_list()
    |> Enum.zip_with(type, &Eval.Env.const_to_pg_string(Eval.Env.new(), &1, &2))
    |> List.to_tuple()
  end

  defp value_to_string(value, %{materialized_type: {:array, type}}) do
    Eval.Env.const_to_pg_string(Eval.Env.new(), value, type)
  end

  defp apply_and_accumulate_events(changes, xid, state) do
    Enum.reduce(changes, state, fn persisted_change, state ->
      change_bytes = :erlang.external_size(persisted_change)
      ensure_pending_transaction_capacity!(state, change_bytes, 0, :changes)
      attempted_change_bytes = state.pending_change_bytes + change_bytes
      {change, persisted_txids} = split_persisted_txids(persisted_change)
      persisted_txids = MapSet.new(persisted_txids)

      {state, events} = apply_changes([change], state)
      events = with_txids(events, xid, persisted_txids)
      event_bytes = :erlang.external_size(events)
      state = %{state | pending_change_bytes: attempted_change_bytes}

      ensure_pending_transaction_capacity!(state, 0, event_bytes, :events)
      attempted_event_bytes = state.pending_event_bytes + event_bytes

      %{
        state
        | pending_events: merge_events(state.pending_events, events),
          pending_change_bytes: attempted_change_bytes,
          pending_event_bytes: attempted_event_bytes
      }
    end)
  end

  defp ensure_pending_transaction_capacity!(state, change_delta, event_delta, kind) do
    attempted_bytes =
      state.completed_event_batch_bytes + state.forwarded_causal_token_bytes +
        state.pending_change_bytes + change_delta + state.pending_event_bytes + event_delta

    if attempted_bytes > state.live_backlog_memory_limit_bytes do
      raise_pending_transaction_limit!(state, kind, attempted_bytes)
    end

    :ok
  end

  defp raise_pending_transaction_limit!(state, kind, attempted_bytes) do
    Logger.error("Materializer source transaction exceeded its bounded live allowance",
      shape_handle: state.shape_handle,
      retained_kind: kind,
      attempted_bytes: attempted_bytes,
      limit_bytes: state.live_backlog_memory_limit_bytes
    )

    raise RuntimeError,
          "materializer source transaction exceeded for #{state.shape_handle} " <>
            "(#{kind}=#{attempted_bytes}/#{state.live_backlog_memory_limit_bytes})"
  end

  defp split_persisted_txids({change, txids}), do: {change, txids}

  defp split_persisted_txids(%{headers: %{txids: txids}} = change),
    do: {change, validate_txids!(txids)}

  defp split_persisted_txids(change), do: {change, []}

  defp with_txids(events, xid, persisted_txids) when events == %{} do
    if MapSet.size(persisted_txids) == 0 do
      events
    else
      # A move-in control precedes the rows from its snapshot. Keep its txids in
      # the pending batch so those rows inherit the attribution; finalization
      # still drops this txid-only map when the whole batch produces no moves.
      %{txids: MapSet.union(xid_set(xid), persisted_txids)}
    end
  end

  defp with_txids(events, xid, persisted_txids) do
    Map.put(events, :txids, MapSet.union(xid_set(xid), persisted_txids))
  end

  defp xid_set(nil), do: MapSet.new()
  defp xid_set(xid) when is_integer(xid) and xid > 0, do: MapSet.new([xid])

  defp maybe_flush_pending_events(state, false, _defer_until_durable?, nil), do: state

  defp maybe_flush_pending_events(_state, false, _defer_until_durable?, _causal_token) do
    raise ArgumentError, "a causal token may only be attached to a committed materializer batch"
  end

  defp maybe_flush_pending_events(state, true, true, causal_token) do
    events = finalized_pending_events(state.pending_events)

    {token, delivery_bytes, state} =
      prepare_causal_batch(state, causal_token, state.applied_offset, events, true)

    state = %{
      state
      | pending_events: %{},
        pending_change_bytes: 0,
        pending_event_bytes: 0,
        completed_event_batches:
          :queue.in(
            {state.applied_offset, events, token, delivery_bytes},
            state.completed_event_batches
          ),
        completed_event_batch_count: state.completed_event_batch_count + 1,
        completed_event_batch_bytes: state.completed_event_batch_bytes + delivery_bytes
    }

    # A dependency-move storage commit can publish durability synchronously
    # before its Consumer sends the corresponding range to this materializer.
    # Re-check the durable watermark so signal-before-data cannot strand a batch.
    publish_durable_batches(state, state.durability_watermark)
  end

  defp maybe_flush_pending_events(state, true, false, causal_token) do
    events = finalized_pending_events(state.pending_events)

    {token, _delivery_bytes, state} =
      prepare_causal_batch(state, causal_token, state.applied_offset, events, false)

    state = publish_events(state, state.applied_offset, events, token)

    state = %{
      state
      | pending_events: %{},
        pending_change_bytes: 0,
        pending_event_bytes: 0,
        durability_watermark: LogOffset.max(state.durability_watermark, state.applied_offset),
        durable_offset: state.applied_offset,
        durable_link_values: link_values_from_counts(state.value_counts)
    }

    write_link_values(state)
    state
  end

  defp prepare_causal_batch(state, nil, %LogOffset{} = offset, events, retained?) do
    token = new_causal_token(offset)
    delivery_bytes = causal_delivery_bytes(state, offset, events, token)
    ensure_causal_batch_capacity!(state, delivery_bytes, retained?, :local_batch)

    state = reserve_token_with_live_causal_subscribers(state, token, delivery_bytes)
    {token, delivery_bytes, state}
  end

  defp prepare_causal_batch(state, token, %LogOffset{} = offset, events, _retained?) do
    if MapSet.member?(state.forwarded_causal_tokens, token) do
      delivery_bytes = causal_delivery_bytes(state, offset, events, token)
      fence_bytes = causal_end_message_bytes(state, token)

      ensure_live_backlog_capacity!(
        state,
        0,
        delivery_bytes - fence_bytes,
        :forwarded_batch
      )

      state =
        state
        |> prepare_token_with_live_causal_subscribers(token, delivery_bytes)
        |> delete_forwarded_causal_token(token)

      {token, delivery_bytes, state}
    else
      raise ArgumentError, "materializer batch used an unknown forwarded causal token"
    end
  end

  defp ensure_causal_batch_capacity!(state, delivery_bytes, true, context),
    do: ensure_live_backlog_capacity!(state, 1, delivery_bytes, context)

  defp ensure_causal_batch_capacity!(state, delivery_bytes, false, context) do
    ensure_live_backlog_capacity!(state, 1, delivery_bytes, context)
  end

  defp ensure_live_backlog_capacity!(state, count_delta, bytes_delta, context) do
    attempted_count =
      state.completed_event_batch_count + MapSet.size(state.forwarded_causal_tokens) + count_delta

    attempted_bytes =
      state.completed_event_batch_bytes + state.forwarded_causal_token_bytes +
        state.pending_change_bytes + state.pending_event_bytes + bytes_delta

    if attempted_count > state.live_backlog_max_pending or
         attempted_bytes > state.live_backlog_memory_limit_bytes do
      raise_live_backlog_limit!(state, attempted_count, attempted_bytes, context)
    end

    :ok
  end

  defp raise_live_backlog_limit!(state, attempted_count, attempted_bytes, context) do
    Logger.error("Materializer live causal backlog exceeded its bounded capacity",
      shape_handle: state.shape_handle,
      context: context,
      attempted_count: attempted_count,
      limit_count: state.live_backlog_max_pending,
      attempted_bytes: attempted_bytes,
      limit_bytes: state.live_backlog_memory_limit_bytes
    )

    raise RuntimeError,
          "materializer live causal backlog exceeded for #{state.shape_handle} " <>
            "(count=#{attempted_count}/#{state.live_backlog_max_pending}, " <>
            "bytes=#{attempted_bytes}/#{state.live_backlog_memory_limit_bytes})"
  end

  defp causal_delivery_bytes(state, lsn, events, token)
       when events == %{} and not is_real_offset(lsn),
       do: causal_end_message_bytes(state, token)

  defp causal_delivery_bytes(state, lsn, events, token) do
    payload = normalize_move_payload(events, lsn) |> Map.put(:causal_token, token)
    :erlang.external_size({:materializer_changes, state.shape_handle, payload})
  end

  defp causal_end_message_bytes(state, token),
    do: :erlang.external_size({:materializer_causal_end, state.shape_handle, token})

  defp reserve_token_with_live_causal_subscribers(state, token, expected_resolution_bytes) do
    offset = causal_token_offset(token)
    subscribers = MapSet.intersection(state.subscribers, state.causal_subscribers)

    Logger.debug("Reserving a transitive materializer causal batch",
      shape_handle: state.shape_handle,
      causal_offset: to_string(offset),
      causal_subscriber_count: MapSet.size(subscribers)
    )

    send_live_causal_requests(state, subscribers, :reserve, fn ->
      {:reserve_materializer_batch, state.shape_handle, token, offset, expected_resolution_bytes}
    end)
  end

  defp prepare_token_with_live_causal_subscribers(state, token, expected_resolution_bytes) do
    subscribers = MapSet.intersection(state.subscribers, state.causal_subscribers)

    send_live_causal_requests(state, subscribers, :prepare, fn ->
      {:prepare_materializer_batch, state.shape_handle, token, expected_resolution_bytes}
    end)
  end

  defp send_live_causal_requests(state, subscribers, operation, request) do
    deadline = System.monotonic_time(:millisecond) + state.causal_call_timeout_ms

    requests =
      Enum.map(subscribers, fn subscriber_pid ->
        {subscriber_pid, :gen_server.send_request(subscriber_pid, request.())}
      end)

    Enum.reduce(requests, state, fn {subscriber_pid, request_id}, state ->
      timeout = Kernel.max(deadline - System.monotonic_time(:millisecond), 0)

      case :gen_server.receive_response(request_id, timeout) do
        {:reply, :ok} ->
          state

        {:reply, {:error, reason}} ->
          drop_causal_subscriber(state, subscriber_pid, operation, reason, false)

        {:error, reason} ->
          drop_causal_subscriber(state, subscriber_pid, operation, reason, true)

        :timeout ->
          drop_causal_subscriber(state, subscriber_pid, operation, :operation_deadline, true)
      end
    end)
  end

  defp call_live_causal_subscribers(state, subscribers, operation, call) do
    deadline = System.monotonic_time(:millisecond) + state.causal_call_timeout_ms
    subscribers = Enum.to_list(subscribers)
    subscriber_count = length(subscribers)

    subscribers
    |> Enum.with_index()
    |> Enum.reduce(state, fn {subscriber_pid, index}, state ->
      remaining_budget = Kernel.max(deadline - System.monotonic_time(:millisecond), 1)
      remaining_subscribers = subscriber_count - index
      call_timeout = Kernel.max(div(remaining_budget, remaining_subscribers), 1)

      try do
        case call.(subscriber_pid, call_timeout) do
          :ok ->
            state

          {:error, reason} ->
            drop_causal_subscriber(state, subscriber_pid, operation, reason, false)
        end
      catch
        :exit, reason ->
          drop_causal_subscriber(state, subscriber_pid, operation, reason, true)
      end
    end)
  end

  defp drop_causal_subscriber(state, subscriber_pid, operation, reason, force_kill?) do
    Logger.warning("Dropping an unavailable derived shape during causal delivery",
      shape_handle: state.shape_handle,
      subscriber_pid: inspect(subscriber_pid),
      operation: operation,
      reason: inspect(reason),
      force_kill?: force_kill?
    )

    if force_kill? and Process.alive?(subscriber_pid) do
      Process.exit(
        subscriber_pid,
        {:materializer_causal_delivery_failed, state.shape_handle, operation}
      )
    end

    remove_replay_subscriber(state, subscriber_pid)
  end

  defp end_forwarded_causal_token(state, nil), do: state

  defp end_forwarded_causal_token(state, token) do
    if MapSet.member?(state.forwarded_causal_tokens, token) do
      state |> delete_forwarded_causal_token(token) |> publish_causal_end(token)
    else
      raise ArgumentError, "deduplicated materializer batch used an unknown causal token"
    end
  end

  defp delete_forwarded_causal_token(state, token) do
    %{
      state
      | forwarded_causal_tokens: MapSet.delete(state.forwarded_causal_tokens, token),
        forwarded_causal_token_bytes:
          state.forwarded_causal_token_bytes - causal_end_message_bytes(state, token)
    }
  end

  defp publish_causal_end(state, token) do
    subscribers = MapSet.intersection(state.subscribers, state.causal_subscribers)

    Logger.debug("Releasing a transitive materializer causal batch without a local payload",
      shape_handle: state.shape_handle,
      causal_offset: token |> causal_token_offset() |> to_string(),
      causal_subscriber_count: MapSet.size(subscribers)
    )

    send_live_causal_requests(state, subscribers, :deliver_end, fn ->
      {:deliver_materializer_causal_end, state.shape_handle, token}
    end)
  end

  defp finalized_pending_events(events) do
    case cancel_matching_move_events(events) do
      empty when empty == %{} -> %{}
      events -> finalize_txids(events)
    end
  end

  defp publish_durable_batches(state, offset) do
    publish_offset = LogOffset.min(offset, state.applied_offset)

    {batches, remaining} =
      take_completed_batches(state.completed_event_batches, publish_offset, [])

    {state, durable_link_values, durable_offset, published_bytes} =
      Enum.reduce(
        batches,
        {state, state.durable_link_values, state.durable_offset, 0},
        fn {lsn, events, token, delivery_bytes},
           {state, link_values, _durable_offset, published_bytes} ->
          state = publish_events(state, lsn, events, token)

          {
            state,
            apply_events_to_link_values(link_values, events),
            lsn,
            published_bytes + delivery_bytes
          }
        end
      )

    state = %{
      state
      | completed_event_batches: remaining,
        completed_event_batch_count: state.completed_event_batch_count - length(batches),
        completed_event_batch_bytes: state.completed_event_batch_bytes - published_bytes,
        # A storage watermark can land inside a fragmented transaction. Only a
        # completed batch represents a replay-safe logical boundary and a link
        # view we can publish consistently, so never expose the raw watermark
        # itself as the subscriber cursor.
        durable_offset: LogOffset.max(state.durable_offset, durable_offset),
        durable_link_values: durable_link_values
    }

    write_link_values(state)
    state
  end

  defp take_completed_batches(queue, max_offset, acc) do
    case :queue.peek(queue) do
      {:value, {offset, _events, _token, _delivery_bytes} = batch}
      when is_log_offset_lte(offset, max_offset) ->
        take_completed_batches(:queue.drop(queue), max_offset, [batch | acc])

      _ ->
        {Enum.reverse(acc), queue}
    end
  end

  defp publish_events(state, lsn, events, token)
       when events == %{} and not is_real_offset(lsn),
       do: publish_causal_end(state, token)

  defp publish_events(state, lsn, events, token) do
    payload = normalize_move_payload(events, lsn)
    causal_payload = Map.put(payload, :causal_token, token)
    causal_subscribers = MapSet.intersection(state.subscribers, state.causal_subscribers)

    state =
      call_live_causal_subscribers(
        state,
        causal_subscribers,
        :deliver_batch,
        fn subscriber_pid, timeout ->
          Consumer.deliver_materializer_batch(
            subscriber_pid,
            state.shape_handle,
            causal_payload,
            timeout
          )
        end
      )

    state.subscribers
    |> MapSet.difference(state.causal_subscribers)
    |> Enum.each(fn subscriber_pid ->
      send(subscriber_pid, {:materializer_changes, state.shape_handle, payload})
    end)

    state
  end

  defp apply_events_to_link_values(link_values, events) do
    link_values =
      Enum.reduce(Map.get(events, :move_out, []), link_values, fn {value, _original}, acc ->
        MapSet.delete(acc, value)
      end)

    Enum.reduce(Map.get(events, :move_in, []), link_values, fn {value, _original}, acc ->
      MapSet.put(acc, value)
    end)
  end

  defp finalize_txids(events) do
    Map.update(events, :txids, [], &Enum.sort(&1))
  end

  defp merge_events(pending, new) when pending == %{}, do: new
  defp merge_events(pending, new) when new == %{}, do: pending

  defp merge_events(pending, new) do
    %{
      move_in: Map.get(new, :move_in, []) ++ Map.get(pending, :move_in, []),
      move_out: Map.get(new, :move_out, []) ++ Map.get(pending, :move_out, []),
      txids:
        MapSet.union(Map.get(pending, :txids, MapSet.new()), Map.get(new, :txids, MapSet.new()))
    }
  end

  # A value's count can cross the 0↔1 boundary multiple times in a single batch
  # (e.g., toggled twice in one transaction: 0→1 move_in, 1→0 move_out, 0→1 move_in).
  # Emitting both move_in and move_out for the same value causes the consumer to
  # fire a move-in query while simultaneously marking the value's tag as moved-out,
  # which filters out the query results - losing the data entirely.
  #
  # We resolve this by sorting events by value, then walking through the list
  # cancelling adjacent move_in/move_out pairs for the same value.
  defp cancel_matching_move_events(events) do
    ins = events |> Map.get(:move_in, []) |> Enum.sort_by(fn {v, _} -> v end)
    outs = events |> Map.get(:move_out, []) |> Enum.sort_by(fn {v, _} -> v end)

    case cancel_sorted_pairs(ins, outs, %{move_in: [], move_out: []}) do
      empty when empty == %{} -> empty
      result -> Map.put(result, :txids, Map.get(events, :txids, MapSet.new()))
    end
  end

  defp cancel_sorted_pairs([{v, _} | ins], [{v, _} | outs], acc),
    do: cancel_sorted_pairs(ins, outs, acc)

  defp cancel_sorted_pairs([{v1, _} = i | ins], [{v2, _} | _] = outs, acc) when v1 < v2,
    do: cancel_sorted_pairs(ins, outs, %{acc | move_in: [i | acc.move_in]})

  defp cancel_sorted_pairs([{v1, _} | _] = ins, [{v2, _} = o | outs], acc) when v2 < v1,
    do: cancel_sorted_pairs(ins, outs, %{acc | move_out: [o | acc.move_out]})

  defp cancel_sorted_pairs([], [], %{move_in: [], move_out: []}), do: %{}

  defp cancel_sorted_pairs(ins, outs, acc),
    do: %{acc | move_in: ins ++ acc.move_in, move_out: outs ++ acc.move_out}

  defp apply_changes(changes, state) do
    {{index, tag_indices}, {value_counts, events}} =
      Enum.reduce(
        changes,
        {{state.index, state.tag_indices}, {state.value_counts, []}},
        fn
          %Changes.NewRecord{
            key: key,
            record: record,
            move_tags: move_tags,
            active_conditions: ac
          },
          {{index, tag_indices}, counts_and_events} ->
            {value, original_string} = cast!(record, state)
            if is_map_key(index, key), do: raise("Key #{key} already exists")
            included? = evaluate_inclusion(move_tags, ac)

            index =
              Map.put(index, key, %{
                value: value,
                tags: move_tags,
                active_conditions: ac,
                included?: included?
              })

            tag_indices = add_row_to_tag_indices(tag_indices, key, move_tags)

            counts_and_events =
              if included?,
                do: increment_value(counts_and_events, value, original_string),
                else: counts_and_events

            {{index, tag_indices}, counts_and_events}

          %Changes.UpdatedRecord{
            key: key,
            old_key: old_key,
            record: record,
            move_tags: move_tags,
            removed_move_tags: removed_move_tags,
            active_conditions: ac
          },
          {{index, tag_indices}, counts_and_events} ->
            # When the primary key doesn't change, old_key may be nil; default to key
            old_key = old_key || key

            # TODO: this is written as if it supports multiple selected columns, but it doesn't for now
            columns_present = Enum.any?(state.columns, &is_map_key(record, &1))
            has_tag_updates = removed_move_tags != []
            pk_changed = old_key != key
            has_ac_update = ac != [] and is_map_key(index, old_key)

            if columns_present or has_tag_updates or has_ac_update or pk_changed do
              old_entry = Map.fetch!(index, old_key)

              # When the primary key changes, re-index every existing tag for the new key.
              tags_to_remove =
                if pk_changed,
                  do: old_entry.tags,
                  else: removed_move_tags

              new_tags =
                if has_tag_updates or move_tags != [], do: move_tags, else: old_entry.tags

              new_ac = if ac != [], do: ac, else: old_entry.active_conditions
              new_included? = evaluate_inclusion(new_tags, new_ac)

              tag_indices =
                tag_indices
                |> remove_row_from_tag_indices(old_key, tags_to_remove)
                |> add_row_to_tag_indices(key, new_tags)

              if columns_present do
                {value, original_string} = cast!(record, state)
                old_value = old_entry.value

                index =
                  index
                  |> Map.delete(old_key)
                  |> Map.put(key, %{
                    value: value,
                    tags: new_tags,
                    active_conditions: new_ac,
                    included?: new_included?
                  })

                cond do
                  old_entry.included? and new_included? and old_value != value ->
                    {{index, tag_indices},
                     counts_and_events
                     |> decrement_value(old_value, value_to_string(old_value, state))
                     |> increment_value(value, original_string)}

                  old_entry.included? and not new_included? ->
                    {{index, tag_indices},
                     decrement_value(
                       counts_and_events,
                       old_value,
                       value_to_string(old_value, state)
                     )}

                  not old_entry.included? and new_included? ->
                    {{index, tag_indices},
                     increment_value(counts_and_events, value, original_string)}

                  true ->
                    # Skip decrement/increment dance if value hasn't changed to avoid
                    # spurious move_out/move_in events when only the tag changed
                    {{index, tag_indices}, counts_and_events}
                end
              else
                index =
                  index
                  |> Map.delete(old_key)
                  |> Map.put(key, %{
                    old_entry
                    | tags: new_tags,
                      active_conditions: new_ac,
                      included?: new_included?
                  })

                cond do
                  old_entry.included? and not new_included? ->
                    {{index, tag_indices},
                     decrement_value(
                       counts_and_events,
                       old_entry.value,
                       value_to_string(old_entry.value, state)
                     )}

                  not old_entry.included? and new_included? ->
                    {{index, tag_indices},
                     increment_value(
                       counts_and_events,
                       old_entry.value,
                       value_to_string(old_entry.value, state)
                     )}

                  true ->
                    {{index, tag_indices}, counts_and_events}
                end
              end
            else
              # Nothing relevant to this materializer has been updated
              {{index, tag_indices}, counts_and_events}
            end

          %Changes.DeletedRecord{key: key, move_tags: move_tags},
          {{index, tag_indices}, counts_and_events} ->
            {entry, index} = Map.pop!(index, key)
            tag_indices = remove_row_from_tag_indices(tag_indices, key, move_tags)

            if entry.included? do
              {{index, tag_indices},
               decrement_value(
                 counts_and_events,
                 entry.value,
                 value_to_string(entry.value, state)
               )}
            else
              {{index, tag_indices}, counts_and_events}
            end

          %{headers: %{event: event, patterns: patterns}},
          {{index, tag_indices}, counts_and_events}
          when event in ["move-out", "move-in"] ->
            new_condition = event == "move-in"
            affected = collect_affected_keys(tag_indices, patterns)

            {{index, tag_indices}, counts_and_events} =
              Enum.reduce(
                affected,
                {{index, tag_indices}, counts_and_events},
                fn {key, matched_positions}, acc ->
                  entry = Map.fetch!(index, key)

                  process_move_event(
                    entry,
                    key,
                    matched_positions,
                    new_condition,
                    acc,
                    state
                  )
                end
              )

            {{index, tag_indices}, counts_and_events}
        end
      )

    events = Enum.group_by(events, &elem(&1, 0), &elem(&1, 1))

    {%{state | index: index, value_counts: value_counts, tag_indices: tag_indices}, events}
  end

  defp increment_value({value_counts, events}, value, original_string) do
    case Map.fetch(value_counts, value) do
      {:ok, count} ->
        {Map.put(value_counts, value, count + 1), events}

      :error ->
        {Map.put(value_counts, value, 1), [{:move_in, {value, original_string}} | events]}
    end
  end

  defp decrement_value({value_counts, events}, value, original_string) do
    # If we're decrementing, it must have been added before
    case Map.fetch!(value_counts, value) do
      1 ->
        {Map.delete(value_counts, value), [{:move_out, {value, original_string}} | events]}

      count ->
        {Map.put(value_counts, value, count - 1), events}
    end
  end

  # Position-aware tag indexing: tags are "/" separated strings where each slot
  # corresponds to a DNF position. Non-empty slots are indexed as {pos, hash}.
  # For backward compat, flat tags (no "/") are treated as position 0.
  defp add_row_to_tag_indices(tag_indices, key, move_tags) do
    Enum.reduce(move_tags, tag_indices, fn tag, acc when is_binary(tag) ->
      tag
      |> parse_tag_slots()
      |> Enum.reduce(acc, fn
        {"", _pos}, acc ->
          acc

        {hash, pos}, acc ->
          Map.update(acc, {pos, hash}, MapSet.new([key]), &MapSet.put(&1, key))
      end)
    end)
  end

  defp remove_row_from_tag_indices(tag_indices, key, move_tags) do
    Enum.reduce(move_tags, tag_indices, fn tag, acc when is_binary(tag) ->
      tag
      |> parse_tag_slots()
      |> Enum.reduce(acc, fn
        {"", _pos}, acc ->
          acc

        {hash, pos}, acc ->
          case Map.fetch(acc, {pos, hash}) do
            {:ok, v} ->
              new_mapset = MapSet.delete(v, key)

              if MapSet.size(new_mapset) == 0 do
                Map.delete(acc, {pos, hash})
              else
                Map.put(acc, {pos, hash}, new_mapset)
              end

            :error ->
              acc
          end
      end)
    end)
  end

  defp parse_tag_slots(tag) do
    tag |> String.split("/") |> Enum.with_index()
  end

  # Collect keys affected by move patterns, returning %{key => MapSet<positions>}
  defp collect_affected_keys(tag_indices, patterns) do
    Enum.reduce(patterns, %{}, fn %{pos: pos, value: value}, acc ->
      case Map.get(tag_indices, {pos, value}) do
        nil ->
          acc

        keys ->
          Enum.reduce(keys, acc, fn key, acc ->
            Map.update(acc, key, MapSet.new([pos]), &MapSet.put(&1, pos))
          end)
      end
    end)
  end

  defp process_move_event(entry, key, matched_positions, new_condition, {{idx, ti}, ce}, state) do
    case entry.active_conditions do
      [] when new_condition == false ->
        # No DNF, move-out: remove row entirely (backward compat)
        ti = remove_row_from_tag_indices(ti, key, entry.tags)
        idx = Map.delete(idx, key)
        {{idx, ti}, decrement_value(ce, entry.value, value_to_string(entry.value, state))}

      [] ->
        # No DNF, move-in: no-op
        {{idx, ti}, ce}

      ac ->
        # DNF: flip matched positions, re-evaluate inclusion
        new_ac = flip_active_conditions(ac, matched_positions, new_condition)
        new_included? = evaluate_inclusion(entry.tags, new_ac)

        cond do
          entry.included? and not new_included? ->
            # Remove row entirely to avoid stale tag_indices. If the row
            # should become included again later, it will re-enter via a
            # move-in query or NewRecord with fresh tags and ac.
            ti = remove_row_from_tag_indices(ti, key, entry.tags)
            idx = Map.delete(idx, key)
            {{idx, ti}, decrement_value(ce, entry.value, value_to_string(entry.value, state))}

          not entry.included? and new_included? ->
            idx =
              Map.put(idx, key, %{
                entry
                | active_conditions: new_ac,
                  included?: new_included?
              })

            {{idx, ti}, increment_value(ce, entry.value, value_to_string(entry.value, state))}

          true ->
            idx =
              Map.put(idx, key, %{
                entry
                | active_conditions: new_ac,
                  included?: new_included?
              })

            {{idx, ti}, ce}
        end
    end
  end

  defp flip_active_conditions(ac, positions, new_value) do
    ac
    |> Enum.with_index()
    |> Enum.map(fn {val, idx} ->
      if MapSet.member?(positions, idx), do: new_value, else: val
    end)
  end

  # Evaluate whether a row is included based on its tags and active_conditions.
  # A row is included if any disjunct (tag) has all participating positions active.
  defp evaluate_inclusion([], _ac), do: true
  defp evaluate_inclusion(_tags, []), do: true

  defp evaluate_inclusion(tags, ac) do
    Enum.any?(tags, fn tag ->
      tag
      |> parse_tag_slots()
      |> Enum.all?(fn
        {"", _pos} -> true
        {_hash, pos} -> Enum.at(ac, pos, true)
      end)
    end)
  end

  if Mix.env() == :test do
    def activate_mocked_functions_from_test_process do
      Support.TestUtils.activate_mocked_functions_for_module(__MODULE__)
    end
  else
    def activate_mocked_functions_from_test_process, do: :noop
  end
end
