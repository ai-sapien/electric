defmodule Electric.Postgres.ReplicationClient do
  @moduledoc """
  A client module for Postgres logical replication.
  """
  use Electric.Postgres.ReplicationConnection

  alias Electric.Postgres.LogicalReplication.Decoder
  alias Electric.Postgres.LogicalReplication.Messages, as: LR
  alias Electric.Postgres.Lsn
  alias Electric.Postgres.CausalMarker
  alias Electric.Postgres.ReplicationClient.MessageConverter
  alias Electric.Postgres.ReplicationClient.ConnectionSetup
  alias Electric.Replication.ShapeLogCollector
  alias Electric.Replication.Changes.TransactionFragment
  alias Electric.Replication.Changes.Relation
  alias Electric.Shapes.Consumer
  alias Electric.Shapes.ConsumerRegistry
  alias Electric.Telemetry.OpenTelemetry
  alias Electric.Telemetry.Sampler

  require Logger

  @type step ::
          :disconnected
          | :connected
          | :identify_system
          | :query_pg_info
          | :acquire_lock
          | :create_publication
          | :check_if_publication_exists
          | :drop_slot
          | :create_slot
          | :query_slot_flushed_lsn
          | :set_display_setting
          | :ready_to_stream
          | :refresh_wal_target
          | :start_streaming
          | :streaming

  defmodule State do
    @enforce_keys [:handle_event, :publication_name]
    defstruct [
      :stack_id,
      :connection_manager,
      :handle_event,
      :publication_name,
      :lock_acquired?,
      :try_creating_publication?,
      :recreate_slot?,
      :start_streaming?,
      :pg_version,
      :slot_name,
      :slot_temporary?,
      :display_settings,
      :message_converter,
      :publication_owner?,
      :replication_idle_timeout,
      wal_sender_timeout: 60_000,
      step: :disconnected,
      event_retry_wait: nil,
      connection_retry_deadline: nil,
      shape_log_collector_processing_pid: nil,
      pending_event: nil,
      startup_wal_flush_lsn: nil,
      pending_causal_marker_lsn: nil,
      pending_causal_marker_xid: nil,
      event_causal_marker_lsn: nil,
      event_causal_marker_xid: nil,
      last_processed_causal_marker_lsn: 0,
      replication_caught_up?: false,
      causal_catch_up_task: nil,
      received_wal: 0,
      flushed_wal: 0,
      last_seen_txn_lsn: Lsn.from_integer(0),
      last_seen_txn_timestamp: nil,
      flush_up_to_date?: true
    ]

    @type t() :: %__MODULE__{
            stack_id: String.t(),
            connection_manager: pid(),
            handle_event: {module(), atom(), [term()]},
            publication_name: String.t(),
            try_creating_publication?: boolean(),
            recreate_slot?: boolean(),
            start_streaming?: boolean(),
            pg_version: non_neg_integer(),
            slot_name: String.t(),
            slot_temporary?: boolean(),
            display_settings: [String.t()],
            message_converter: MessageConverter.t(),
            publication_owner?: boolean(),
            replication_idle_timeout: non_neg_integer(),
            wal_sender_timeout: non_neg_integer(),
            step: Electric.Postgres.ReplicationClient.step(),
            event_retry_wait: {:collector_processing, term()} | nil,
            connection_retry_deadline: integer() | nil,
            shape_log_collector_processing_pid: pid() | nil,
            pending_event: {reference(), term(), non_neg_integer(), integer()} | nil,
            startup_wal_flush_lsn: non_neg_integer() | nil,
            pending_causal_marker_lsn: non_neg_integer() | nil,
            pending_causal_marker_xid: non_neg_integer() | nil,
            event_causal_marker_lsn: non_neg_integer() | nil,
            event_causal_marker_xid: non_neg_integer() | nil,
            last_processed_causal_marker_lsn: non_neg_integer(),
            replication_caught_up?: boolean(),
            causal_catch_up_task: {pid(), reference(), non_neg_integer()} | nil,
            received_wal: non_neg_integer(),
            flushed_wal: non_neg_integer(),
            last_seen_txn_lsn: Lsn.t(),
            last_seen_txn_timestamp: integer(),
            flush_up_to_date?: boolean()
          }

    @opts_schema NimbleOptions.new!(
                   stack_id: [required: true, type: :string],
                   connection_manager: [required: true, type: :pid],
                   handle_event: [required: true, type: :mfa],
                   publication_name: [required: true, type: :string],
                   try_creating_publication?: [required: true, type: :boolean],
                   start_streaming?: [type: :boolean, default: true],
                   slot_name: [required: true, type: :string],
                   slot_temporary?: [type: :boolean, default: false],
                   replication_idle_timeout: [type: :non_neg_integer, default: 0],
                   # Set a reasonable limit for the maximum size of a transaction that
                   # we can handle, above which we would exit as we run the risk of running
                   # out of memmory.
                   # TODO: stream out transactions and collect on disk to avoid this
                   max_txn_size: [type: {:or, [:non_neg_integer, nil]}, default: nil],
                   # Maximum number of changes to buffer before flushing a transaction fragment.
                   # Smaller values result in more message passing overhead but lower memory usage.
                   # The minimum allowed value is 2.
                   max_batch_size: [type: :non_neg_integer, default: 100]
                 )

    @spec new(Access.t()) :: t()
    def new(opts) do
      opts = NimbleOptions.validate!(opts, @opts_schema)
      settings = [display_settings: Electric.Postgres.display_settings()]
      opts = settings ++ opts

      {max_txn_size, opts} = Keyword.pop!(opts, :max_txn_size)
      {max_batch_size, opts} = Keyword.pop!(opts, :max_batch_size)

      # Assert the implicit requirement
      true = max_batch_size >= 2

      struct!(
        __MODULE__,
        opts ++
          [
            message_converter:
              MessageConverter.new(
                max_tx_size: max_txn_size,
                max_batch_size: max_batch_size
              )
          ]
      )
    end
  end

  # @type state :: State.t()

  @repl_msg_x_log_data ?w
  @repl_msg_primary_keepalive ?k
  @repl_msg_standby_status_update ?r

  @default_connect_timeout 30_000
  @idle_check_interval Electric.Config.min_replication_idle_timeout()

  # Maximum keepalive interval. Caps the derived interval (wal_sender_timeout/3)
  # so we stay responsive even if wal_sender_timeout is set very high or changes
  # on the source PG after we've connected.
  @max_keepalive_interval 15_000

  # Delay before retrying a failed event dispatch.
  @event_retry_delay 50

  @max_logical_message_bytes 1_024

  # Maximum time to spend retrying a crashed event handler before giving up.
  @max_event_retry_time 10 * 60_000

  @spec start_link(Keyword.t()) :: :gen_statem.start_ret()
  def start_link(opts) do
    config = Map.new(opts)

    # Disable the reconnection logic in Postgex.ReplicationConnection to force it to exit with
    # the connection error. Without this, we may observe undesirable restarts in tests between
    # one test process exiting and the next one starting.
    start_opts =
      [
        name: name(config.stack_id),
        timeout: Access.get(opts, :timeout, @default_connect_timeout),
        auto_reconnect: false,
        sync_connect: false
      ] ++ Electric.Utils.deobfuscate_password(config.replication_opts[:connection_opts])

    Electric.Postgres.ReplicationConnection.start_link(
      __MODULE__,
      Keyword.delete(config.replication_opts, :connection_opts),
      start_opts
    )
  end

  def name(stack_id) do
    Electric.ProcessRegistry.name(stack_id, __MODULE__)
  end

  @doc false
  @spec notify_shape_log_collector_processing_started(String.t(), pid()) :: :ok
  def notify_shape_log_collector_processing_started(stack_id, collector_pid)
      when is_pid(collector_pid) do
    case GenServer.whereis(name(stack_id)) do
      nil -> :ok
      pid -> send(pid, {__MODULE__, :shape_log_collector_processing_started, collector_pid})
    end

    :ok
  rescue
    ArgumentError ->
      # A missing per-stack registry means the notification cannot be routed
      # reliably. Crash the announcing collector so supervision retries the
      # whole startup edge instead of silently losing the only wakeup.
      exit({:process_registry_unavailable, stack_id})
  end

  # This is a send() and not a call() to prevent the caller (the Connection.Manager process) from
  # getting blocked when the replication connection is blocked some replication slot condition
  # that doesn't let it start streaming immediately.
  def start_streaming(client) do
    send(client, :start_streaming)
  end

  @doc false
  @spec causal_drain_max_concurrency(Electric.stack_id(), non_neg_integer()) :: pos_integer()
  def causal_drain_max_concurrency(stack_id, consumer_count)
      when is_binary(stack_id) and is_integer(consumer_count) and consumer_count >= 0 do
    configured_limit =
      Electric.StackConfig.lookup(
        stack_id,
        :causal_drain_max_concurrency,
        Electric.Config.default(:causal_drain_max_concurrency)
      )

    if not (is_integer(configured_limit) and configured_limit > 0) do
      raise ArgumentError,
            "causal_drain_max_concurrency must be a positive integer, got: #{inspect(configured_limit)}"
    end

    consumer_count
    |> max(1)
    |> min(configured_limit)
  end

  @doc false
  @spec causal_drain_timeout_ms(Electric.stack_id()) :: pos_integer()
  def causal_drain_timeout_ms(stack_id) when is_binary(stack_id) do
    configured_timeout =
      Electric.StackConfig.lookup(
        stack_id,
        :causal_drain_timeout_ms,
        Electric.Config.default(:causal_drain_timeout_ms)
      )

    if not (is_integer(configured_timeout) and configured_timeout > 0) do
      raise ArgumentError,
            "causal_drain_timeout_ms must be a positive integer, got: #{inspect(configured_timeout)}"
    end

    configured_timeout
  end

  def stop(client, reason) do
    Electric.Postgres.ReplicationConnection.call(client, {:stop, reason})
  end

  # The `Postgrex.ReplicationConnection` behaviour does not follow the gen server conventions and
  # establishes its own instead. Unless the `sync_connect: false` option is passed to `start_link()`, the
  # connection process will try opening a replication connection to Postgres before returning
  # from its `init()` callback.
  #
  # The callbacks `init()`, `handle_connect()` and `handle_result()` defined in this module
  # would all be invoked inside the connection process' `init()` callback in that case. Once
  # any of the callbacks return `{:stream, ...}`, the connection process finishes its
  # initialization and switches into the logical streaming mode to start receiving logical
  # messages from Postgres, invoking the `handle_data()` callback for each one.
  #
  # TODO(alco): this needs additional info about :noreply and :query return tuples.
  @impl true
  def init(replication_opts) do
    state = State.new(replication_opts)

    Process.set_label({:replication_client, state.stack_id})

    Logger.metadata(stack_id: state.stack_id, is_connection_process?: true)
    Electric.Telemetry.Sentry.set_tags_context(stack_id: state.stack_id)

    {:ok, state}
  end

  # `Postgrex.ReplicationConnection` opens a new replication connection to Postgres and then
  # gives us a chance to execute one or more queries before switching into the logical
  # streaming mode. It doesn't give us the connection socket but instead takes the query returned
  # by one of our `handle_connect/1`, `handle_result/2` or `handle_info/2` callbacks, executes
  # it, invokes the `handle_result/2` callback on the result which may return another query to
  # execute, executes that, and so it goes on and on, recursively, until a callback returns
  # `{:noreply, ...}` or `{:streaming, ...}`.
  #
  # To execute a series of queries one after the other, we define an ad-hoc state
  # machine that starts from the :connected state in `handle_connect/1`, then transitions to
  # the next step and returns the appropriate query to `Postgrex.ReplicationConnection` for execution,
  # This is all implemented in a separate module named `Electric.Postgres.ReplicationClient.ConnectionSetup`
  # to separate the connection setup logic from logical streaming.

  @impl true
  def handle_connect(state) do
    %{state | step: :connected}
    |> notify_connection_opened()
    |> ConnectionSetup.start()
  end

  @impl true
  def handle_result(result_list_or_error, state) do
    {current_step, next_step, extra_info, updated_state, return_val} =
      ConnectionSetup.process_query_result(result_list_or_error, state)

    if current_step == :identify_system,
      do: notify_system_identified(state, extra_info)

    if current_step == :query_pg_info,
      do: notify_pg_info_obtained(state, extra_info)

    if current_step == :acquire_lock do
      case extra_info do
        :lock_acquired -> notify_lock_acquired(state)
        {:lock_acquisition_failed, error} -> notify_lock_acquisition_error(state, error)
      end
    end

    # for new slots, always reset the last processed LSN
    if current_step == :create_slot and extra_info == :created_new_slot do
      Electric.LsnTracker.set_last_processed_lsn(state.stack_id, updated_state.flushed_wal)
      notify_created_new_slot(state)
    end

    # for existing slots, populate the last processed LSN if not present
    if current_step == :query_slot_flushed_lsn,
      do:
        Electric.LsnTracker.initialize_last_processed_lsn(
          state.stack_id,
          updated_state.flushed_wal
        )

    if next_step == :ready_to_stream,
      do: notify_ready_to_stream(state)

    return_val
  end

  @impl true
  def handle_call({:stop, reason}, from, _state) do
    Logger.notice(
      "Replication client #{inspect(self())} is stopping after receiving stop request from #{inspect(elem(from, 0))} with reason #{inspect(reason)}"
    )

    {:disconnect, reason}
  end

  @impl true
  def handle_info({:flush_boundary_updated, lsn}, state) do
    state =
      if Lsn.from_integer(lsn) == state.last_seen_txn_lsn do
        %{
          state
          | flush_up_to_date?: true,
            flushed_wal: state.received_wal,
            received_wal: max(lsn, state.received_wal)
        }
      else
        %{state | flushed_wal: max(lsn, state.flushed_wal), received_wal: state.received_wal}
      end

    state = maybe_mark_replication_caught_up(state)

    {:noreply, [encode_standby_status_update(state)], state}
  end

  def handle_info(
        {ref, :ok},
        %State{causal_catch_up_task: {_task_pid, ref, target}} = state
      )
      when is_reference(ref) do
    Process.demonitor(ref, [:flush])
    state = %{state | causal_catch_up_task: nil}

    if not state.replication_caught_up? and state.startup_wal_flush_lsn == target and
         state.received_wal >= target and state.flushed_wal >= target and
         state.last_processed_causal_marker_lsn == target do
      Logger.notice(
        "Replication caught up to startup WAL target #{Lsn.from_integer(target)} " <>
          "and drained its causal consumer frontier " <>
          "(received=#{Lsn.from_integer(state.received_wal)}, " <>
          "flushed=#{Lsn.from_integer(state.flushed_wal)})"
      )

      :ok =
        Electric.Connection.Manager.replication_client_caught_up(
          state.connection_manager,
          self()
        )

      {:noreply, %{state | replication_caught_up?: true}}
    else
      {:noreply, maybe_mark_replication_caught_up(state)}
    end
  end

  def handle_info(
        {ref,
         {:error, {:causal_frontier_timeout, stack_id, target, timeout_ms} = timeout_reason}},
        %State{causal_catch_up_task: {_task_pid, ref, target}}
      )
      when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    Logger.error(
      "Causal startup frontier timed out for stack #{inspect(stack_id)} after #{timeout_ms}ms " <>
        "at target #{Lsn.from_integer(target)}"
    )

    {:disconnect, {:causal_catch_up_failed, timeout_reason}}
  end

  def handle_info(
        {:DOWN, ref, :process, task_pid, reason},
        %State{causal_catch_up_task: {task_pid, ref, target}}
      ) do
    Logger.error(
      "Causal startup frontier worker failed for target #{Lsn.from_integer(target)}: " <>
        inspect(reason)
    )

    {:disconnect, {:causal_catch_up_failed, reason}}
  end

  @impl true
  def handle_info(:start_streaming, %State{step: :ready_to_stream} = state) do
    ConnectionSetup.start_streaming(state)
  end

  def handle_info(:start_streaming, %State{step: step} = state) do
    Logger.debug("Replication client requested to start streaming while step=#{step}")
    {:noreply, state}
  end

  def handle_info(:check_if_idle, %State{last_seen_txn_timestamp: txn_ts} = state) do
    time_diff = System.convert_time_unit(System.monotonic_time() - txn_ts, :native, :millisecond)

    if time_diff >= state.replication_idle_timeout do
      {:disconnect, {:shutdown, {:connection_idle, time_diff}}}
    else
      {:noreply, state}
    end
  end

  # Periodic keepalive: send a StandbyStatusUpdate to prevent wal_sender_timeout.
  # This fires every @keepalive_interval ms regardless of whether the socket is
  # paused. Sending the same LSN without advancement is safe — it only resets
  # PostgreSQL's last_reply_timestamp.
  def handle_info(:send_keepalive, %State{step: :streaming} = state) do
    {:noreply, [encode_standby_status_update(state)], state}
  end

  def handle_info(:send_keepalive, state) do
    {:noreply, state}
  end

  # Event processing messages — see dispatch_event/2 and apply_event/3 below.
  def handle_info({:process_event, event, time_remaining}, state),
    do: apply_event(event, time_remaining, state)

  # A connection retry carries one absolute deadline across every attempt. This
  # keeps the socket paused without coupling replication progress to externally
  # visible stack health, which itself depends on replication catching up.
  def handle_info(
        {:retry_connection_event, event, deadline},
        %State{connection_retry_deadline: deadline} = state
      ) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining > 0 do
      apply_event(event, remaining, state)
    else
      connection_retry_budget_exhausted()
    end
  end

  # An event can complete or change failure mode before an already-delivered
  # retry timer is handled. Never let that stale timer start a second attempt.
  def handle_info({:retry_connection_event, _event, _deadline}, state),
    do: {:noreply, state}

  # Shape restore deliberately opens collector processing before advertising
  # the stack as externally active. If an event reached the collector just
  # before that transition, retry it on this internal readiness notification
  # instead of waiting for external health and deadlocking restore replay.
  def handle_info(
        {__MODULE__, :shape_log_collector_processing_started, collector_pid},
        state
      ) do
    if current_shape_log_collector_pid(state) == collector_pid do
      state = %{state | shape_log_collector_processing_pid: collector_pid}

      case state.event_retry_wait do
        {:collector_processing, event} ->
          retry_pending_event(event, %{state | event_retry_wait: nil})

        _ ->
          {:noreply, state}
      end
    else
      # A delayed signal from the collector that belonged to a previous stack
      # incarnation must not open processing for its replacement.
      {:noreply, state}
    end
  end

  # Async event handler replied :ok — demonitor, ack transaction, resume socket.
  def handle_info(
        {ref, :ok},
        %State{pending_event: {ref, event, _time_remaining, _start_time}} = state
      )
      when is_reference(ref) do
    Process.demonitor(ref, [:flush])
    state = %{state | pending_event: nil, connection_retry_deadline: nil}
    {acks, state} = acknowledge_transaction(event, state)
    state = update_flush_up_to_date(event, state)
    state = promote_causal_marker_after_event(event, state)
    state = maybe_mark_replication_caught_up(state)
    {:noreply_and_resume, acks, state}
  end

  # Async event handler replied with a recoverable error — wait and retry.
  def handle_info(
        {ref, {:error, error}},
        %State{pending_event: {ref, event, time_remaining, start_time}} = state
      )
      when is_reference(ref) and error in [:not_ready, :connection_not_available] do
    Process.demonitor(ref, [:flush])
    state = %{state | pending_event: nil}

    case error do
      :not_ready ->
        state = %{state | connection_retry_deadline: nil}

        if shape_log_collector_processing?(state) do
          retry_pending_event(event, state)
        else
          # Collector processing is an internal startup level, not external
          # stack health. Hold the single paused-socket event directly and let
          # the collector's level-triggered signal wake it; registering an
          # :active waiter here would leak one stale StatusMonitor waiter per
          # pre-active retry cycle.
          {:noreply, %{state | event_retry_wait: {:collector_processing, event}}}
        end

      :connection_not_available ->
        deadline = state.connection_retry_deadline || start_time + time_remaining
        retry_connection_event(event, deadline, state)
    end
  end

  # Async event handler crashed — retry with budget.
  def handle_info(
        {:DOWN, ref, :process, _pid, reason},
        %State{pending_event: {ref, event, time_remaining, start_time}} = state
      ) do
    remaining = time_remaining - (System.monotonic_time(:millisecond) - start_time)
    state = %{state | pending_event: nil, connection_retry_deadline: nil}

    if remaining > 0 do
      Logger.error(
        "Error processing replication event (#{remaining}ms retry budget left): " <>
          inspect(reason)
      )

      Process.send_after(self(), {:process_event, event, remaining}, @event_retry_delay)
      {:noreply, state}
    else
      Logger.error("Exhausted retry budget processing replication event: " <> inspect(reason))

      exit(reason)
    end
  end

  # This callback is invoked when the connection process receives a shutdown signal.
  def handle_info({:EXIT, _pid, :shutdown}, _state) do
    Logger.debug("Replication client #{inspect(self())} received shutdown signal, stopping")
    {:disconnect, :shutdown}
  end

  # Some other exit reason we're not expecting: disconnect and shut down.
  def handle_info({:EXIT, _pid, reason}, _state) do
    {:disconnect, reason}
  end

  # The implementation of Postgrex.ReplicationConnection doesn't give us a convenient way to
  # check whether the START_REPLICATION_SLOT statement succeeded before switching the
  # connection into streaming mode. Returning {:query, "START_REPLICATION_SLOT ...", state}
  # works fine when the query result is an error: it is then passed to the handle_result()
  # callback. But if streaming starts without issues, a function clause error is encountered
  # inside Postgrex.ReplicationConnection because it expects the connection to already have
  # been switched into streaming mode by returning {:stream, "START_REPLICATION_SLOT ...", [], state}.
  #
  # Hence this function clause of `handle_data()` that notifies the connection manager about
  # successful streaming start as soon as it receives the first replication message from
  # Postgres.
  @impl true
  @spec handle_data(binary(), State.t()) ::
          {:noreply, State.t()}
          | {:noreply, list(binary()), State.t()}
          | {:noreply_and_pause, list(binary()), State.t()}
          | {:disconnect, term()}
  def handle_data(data, %State{step: :start_streaming} = state) do
    # Modify the state as if we've just seen a transaction so that in the future we have a
    # starting point to check how long the stream has been idle for.
    state = %{state | step: :streaming, last_seen_txn_timestamp: System.monotonic_time()}

    if state.replication_idle_timeout > 0 do
      :timer.send_interval(@idle_check_interval, :check_if_idle)
    end

    # Start a periodic keepalive timer. This sends StandbyStatusUpdate messages
    # to PostgreSQL at regular intervals, preventing wal_sender_timeout from
    # firing even when the socket is paused for backpressure.
    #
    # The interval is derived from PostgreSQL's wal_sender_timeout (queried during
    # connection setup): timeout/3 provides a safe margin, matching the heuristic
    # used by pg_recvlogical and other replication clients.
    keepalive_interval = keepalive_interval(state.wal_sender_timeout)
    :timer.send_interval(keepalive_interval, :send_keepalive)

    Logger.debug(
      "Keepalive interval set to #{keepalive_interval}ms (wal_sender_timeout=#{state.wal_sender_timeout}ms)"
    )

    notify_seen_first_message(state)
    handle_data(data, state)
  end

  def handle_data(<<@repl_msg_primary_keepalive, wal_end::64, _clock::64, reply>>, state) do
    Logger.debug(fn ->
      "Primary Keepalive: wal_end=#{wal_end} (#{Lsn.from_integer(wal_end)}) reply=#{reply}"
    end)

    case reply do
      1 ->
        {:noreply, [encode_standby_status_update(state)], state}

      0 ->
        {:noreply, [], state}
    end
  end

  def handle_data(
        <<@repl_msg_x_log_data, _wal_start::64, _server_wal_end::64, _clock::64, ?M,
          _rest::binary>> = frame,
        %State{} = state
      ) do
    <<_header::binary-size(25), logical_message::binary>> = frame

    cond do
      byte_size(logical_message) > @max_logical_message_bytes ->
        count_ignored_logical_message(state, :oversized, byte_size(logical_message))
        {:noreply, state}

      true ->
        case {CausalMarker.decode_wire(logical_message), state.message_converter.txn_fragment} do
          {{:ok, marker_lsn}, %TransactionFragment{xid: xid}}
          when is_integer(xid) ->
            marker_lsn = Lsn.to_integer(marker_lsn)

            if marker_lsn == state.startup_wal_flush_lsn do
              {:noreply,
               %{
                 state
                 | pending_causal_marker_lsn: marker_lsn,
                   pending_causal_marker_xid: xid
               }}
            else
              count_ignored_logical_message(state, :non_startup_causal_marker, 0)
              {:noreply, state}
            end

          {{:ok, _marker_lsn}, _converter_state} ->
            count_ignored_logical_message(state, :marker_outside_transaction, 0)
            {:noreply, state}

          {:not_marker, _converter_state} ->
            count_ignored_logical_message(state, :foreign_or_invalid, 0)
            {:noreply, state}
        end
    end
  end

  def handle_data(
        <<@repl_msg_x_log_data, _wal_start::64, _server_wal_end::64, _clock::64, data::binary>>,
        %State{} = state
      ) do
    msg = Decoder.decode(data)

    case MessageConverter.convert(msg, state.message_converter) do
      {:error, reason} ->
        {:disconnect, {:irrecoverable_slot, reason}}

      {:buffering, converter} ->
        {:noreply, %{state | message_converter: converter}}

      {:ok, event, converter} ->
        state =
          state
          |> Map.put(:message_converter, converter)
          |> associate_pending_causal_marker(msg, event)

        dispatch_event(event, state)
    end
  end

  defp associate_pending_causal_marker(
         %State{
           pending_causal_marker_lsn: marker_lsn,
           pending_causal_marker_xid: xid,
           event_causal_marker_lsn: nil,
           event_causal_marker_xid: nil
         } = state,
         %LR.Commit{},
         %TransactionFragment{xid: xid, lsn: event_lsn, commit: commit}
       )
       when is_integer(marker_lsn) and is_integer(xid) and not is_nil(commit) do
    transaction_final_lsn = Lsn.to_integer(event_lsn)

    if transaction_final_lsn >= marker_lsn do
      %{
        state
        | # The logical message record LSN identifies the exact marker emitted by
          # the startup query, but it is not the causal boundary of its enclosing
          # transaction. Use Begin.final_lsn for flushing and downstream draining
          # so WAL committed between the marker record and this transaction's
          # commit cannot fall outside the startup frontier.
          startup_wal_flush_lsn: transaction_final_lsn,
          pending_causal_marker_lsn: nil,
          pending_causal_marker_xid: nil,
          event_causal_marker_lsn: transaction_final_lsn,
          event_causal_marker_xid: xid
      }
    else
      %{state | pending_causal_marker_lsn: nil, pending_causal_marker_xid: nil}
    end
  end

  defp associate_pending_causal_marker(
         %State{pending_causal_marker_lsn: marker_lsn} = state,
         %LR.Commit{},
         _event
       )
       when is_integer(marker_lsn) do
    %{state | pending_causal_marker_lsn: nil, pending_causal_marker_xid: nil}
  end

  defp associate_pending_causal_marker(state, _message, _event), do: state

  defp promote_causal_marker_after_event(
         %TransactionFragment{xid: xid, commit: commit},
         %State{event_causal_marker_lsn: marker_lsn, event_causal_marker_xid: xid} = state
       )
       when not is_nil(commit) and is_integer(marker_lsn) and is_integer(xid) do
    %{
      state
      | event_causal_marker_lsn: nil,
        event_causal_marker_xid: nil,
        last_processed_causal_marker_lsn: marker_lsn
    }
  end

  defp promote_causal_marker_after_event(_event, state), do: state

  defp count_ignored_logical_message(state, reason, bytes) do
    :telemetry.execute(
      [:electric, :postgres, :replication, :logical_message_ignored],
      %{count: 1, bytes: bytes},
      %{stack_id: state.stack_id, reason: reason}
    )
  end

  # Dispatch event processing asynchronously. Pauses the socket so we don't
  # receive more data until processing completes. The gen_statem remains
  # responsive to handle_info messages (keepalive timer, flush_boundary_updated,
  # EXIT signals) while providing backpressure to the replication stream.
  #
  # maybe_update_flush_up_to_date and acknowledge_transaction are intentionally
  # deferred to apply_event's success path, preserving the original semantics
  # where they only ran after handle_event succeeded.
  defp dispatch_event(event, state) do
    send(self(), {:process_event, event, @max_event_retry_time})
    {:noreply_and_pause, [], %{state | connection_retry_deadline: nil}}
  end

  # Dispatch the event handler as a non-blocking $gen_call. The MFA returns a
  # monitor ref; the gen_statem returns immediately and handles the reply (or
  # :DOWN on crash) in handle_info. This keeps the gen_statem responsive to
  # keepalive timers while the handler processes the event.
  defp apply_event(event, time_remaining, state) do
    {m, f, args} = state.handle_event
    start_time = System.monotonic_time(:millisecond)

    try do
      ref = apply(m, f, [event | args])

      {:noreply, %{state | pending_event: {ref, event, time_remaining, start_time}}}
    catch
      kind, reason ->
        remaining = time_remaining - (System.monotonic_time(:millisecond) - start_time)
        state = %{state | connection_retry_deadline: nil}

        if remaining > 0 do
          Logger.error(
            "Error dispatching replication event (#{remaining}ms retry budget left): " <>
              Exception.format(kind, reason, __STACKTRACE__)
          )

          Process.send_after(self(), {:process_event, event, remaining}, @event_retry_delay)
          {:noreply, state}
        else
          Logger.error(
            "Exhausted retry budget dispatching replication event: " <>
              Exception.format(kind, reason, __STACKTRACE__)
          )

          :erlang.raise(kind, reason, __STACKTRACE__)
        end
    end
  end

  defp retry_connection_event(event, deadline, state) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining > 0 do
      delay = min(@event_retry_delay, remaining)
      Process.send_after(self(), {:retry_connection_event, event, deadline}, delay)
      {:noreply, %{state | connection_retry_deadline: deadline}}
    else
      connection_retry_budget_exhausted()
    end
  end

  defp connection_retry_budget_exhausted do
    Logger.error("Exhausted retry budget while the event handler connection was unavailable")
    {:disconnect, {:event_delivery_retry_budget_exhausted, :connection_not_available}}
  end

  defp retry_pending_event(event, state) do
    Process.send_after(self(), {:process_event, event, @max_event_retry_time}, @event_retry_delay)
    {:noreply, %{state | connection_retry_deadline: nil}}
  end

  defp shape_log_collector_processing?(state) do
    is_pid(state.shape_log_collector_processing_pid) and
      current_shape_log_collector_pid(state) == state.shape_log_collector_processing_pid
  end

  defp current_shape_log_collector_pid(%State{stack_id: stack_id}) when is_binary(stack_id) do
    GenServer.whereis(ShapeLogCollector.name(stack_id))
  rescue
    ArgumentError -> nil
  end

  defp current_shape_log_collector_pid(_state), do: nil

  defp acknowledge_transaction(%TransactionFragment{commit: nil}, state), do: {[], state}

  defp acknowledge_transaction(%TransactionFragment{lsn: lsn, commit: commit}, state) do
    if Sampler.sample_metrics?() do
      alias Electric.Replication.Changes.Commit

      OpenTelemetry.execute(
        [:electric, :postgres, :replication, :transaction_received],
        %{
          monotonic_time: System.monotonic_time(),
          receive_lag: Commit.calculate_final_receive_lag(commit, System.monotonic_time()),
          bytes: commit.transaction_size,
          count: 1,
          operations: commit.txn_change_count
        },
        %{stack_id: state.stack_id}
      )
    end

    state =
      %{
        state
        | last_seen_txn_lsn: lsn,
          last_seen_txn_timestamp: System.monotonic_time()
      }
      |> update_received_wal(Lsn.to_integer(lsn))

    {[encode_standby_status_update(state)], state}
  end

  defp acknowledge_transaction(%Relation{}, state), do: {[], state}

  defp update_flush_up_to_date(%TransactionFragment{commit: nil}, state),
    do: %{state | flush_up_to_date?: false}

  defp update_flush_up_to_date(%TransactionFragment{}, state) do
    %{
      state
      | flush_up_to_date?: state.flushed_wal >= Lsn.to_integer(state.last_seen_txn_lsn)
    }
  end

  defp update_flush_up_to_date(%Relation{}, state), do: state

  defp encode_standby_status_update(state) do
    Logger.debug(fn ->
      "Standby status update: received_wal=#{Lsn.from_integer(state.received_wal)}, flushed_wal=#{Lsn.from_integer(state.flushed_wal)}"
    end)

    <<
      @repl_msg_standby_status_update,
      state.received_wal + 1::64,
      state.flushed_wal + 1::64,
      state.flushed_wal + 1::64,
      current_time()::64,
      0
    >>
  end

  # Derive keepalive interval from PostgreSQL's wal_sender_timeout.
  # Uses min(timeout/3, 15s): timeout/3 provides a safe margin for low timeouts,
  # while the 15s cap ensures responsiveness even if wal_sender_timeout is very
  # high or changes on the source PG after we've connected.
  defp keepalive_interval(0), do: @max_keepalive_interval

  defp keepalive_interval(wal_sender_timeout_ms),
    do: min(div(wal_sender_timeout_ms, 3), @max_keepalive_interval)

  @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
  defp current_time(), do: System.os_time(:microsecond) - @epoch

  defp update_received_wal(state, wal) when is_number(wal) and wal >= state.received_wal,
    do: %{state | received_wal: wal}

  defp update_received_wal(state, wal) when is_number(wal), do: state

  defp maybe_mark_replication_caught_up(
         %State{
           replication_caught_up?: false,
           causal_catch_up_task: nil,
           startup_wal_flush_lsn: target,
           stack_id: stack_id,
           received_wal: received_wal,
           flushed_wal: flushed_wal,
           last_processed_causal_marker_lsn: processed_marker_lsn
         } = state
       )
       when is_integer(target) and received_wal >= target and flushed_wal >= target and
              processed_marker_lsn == target do
    supervisor = Electric.ProcessRegistry.name(stack_id, Electric.StackTaskSupervisor)
    owner = self()
    timeout_ms = causal_drain_timeout_ms(stack_id)
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    task_fun = fn ->
      await_consumer_causal_frontier_while_owner_alive(
        owner,
        stack_id,
        target,
        deadline,
        timeout_ms
      )
    end

    task = Task.Supervisor.async_nolink(supervisor, task_fun)

    %{state | causal_catch_up_task: {task.pid, task.ref, target}}
  end

  defp maybe_mark_replication_caught_up(state), do: state

  defp await_consumer_causal_frontier_while_owner_alive(
         owner,
         stack_id,
         target,
         deadline,
         timeout_ms
       ) do
    token =
      case ConsumerRegistry.activate_causal_drain(stack_id, target) do
        {:ok, token} -> token
        {:error, reason} -> exit({:causal_drain_activation_failed, reason})
      end

    try do
      owner_ref = Process.monitor(owner)
      outer = self()
      result_ref = make_ref()

      {worker_pid, worker_ref} =
        :erlang.spawn_opt(
          fn ->
            send(outer, {result_ref, await_consumer_causal_frontier(stack_id, target, token)})
          end,
          [:link, :monitor]
        )

      remaining = max(deadline - System.monotonic_time(:millisecond), 0)

      receive do
        {^result_ref, result} ->
          Process.demonitor(worker_ref, [:flush])
          Process.demonitor(owner_ref, [:flush])
          result

        {:DOWN, ^owner_ref, :process, ^owner, _reason} ->
          terminate_causal_frontier_worker(worker_pid, worker_ref)
          exit(:normal)

        {:DOWN, ^worker_ref, :process, ^worker_pid, reason} ->
          Process.demonitor(owner_ref, [:flush])
          exit({:causal_frontier_worker_failed, reason})
      after
        remaining ->
          terminate_causal_frontier_worker(worker_pid, worker_ref)
          Process.demonitor(owner_ref, [:flush])
          {:error, {:causal_frontier_timeout, stack_id, target, timeout_ms}}
      end
    after
      ConsumerRegistry.deactivate_causal_drain(stack_id, token)
    end
  end

  defp terminate_causal_frontier_worker(worker_pid, worker_ref) do
    Process.unlink(worker_pid)
    Process.exit(worker_pid, :kill)

    receive do
      {:DOWN, ^worker_ref, :process, ^worker_pid, _reason} -> :ok
    end
  end

  # Root flush closes creation of causal work at or below the sampled startup
  # cut: every derived batch recursively installs its consumer reservation
  # before the collector event can acknowledge. A fixed-point snapshot is
  # therefore sufficient, while still catching consumers lazily created by
  # restore during the first pass. Raw registry entries are intentional: a
  # dead/stale consumer must be removed or replaced, never mistaken for drained.
  defp await_consumer_causal_frontier(stack_id, target, token) do
    generation = ConsumerRegistry.causal_generation(stack_id)
    snapshot = ConsumerRegistry.consumer_snapshot(stack_id)

    all_drained? =
      snapshot
      |> Task.async_stream(
        fn {_shape_handle, consumer_pid} ->
          try do
            Consumer.await_causal_frontier(consumer_pid, target)
          catch
            :exit, reason -> {:error, {:exit, reason}}
          end
        end,
        max_concurrency: causal_drain_max_concurrency(stack_id, map_size(snapshot)),
        ordered: false,
        timeout: :infinity
      )
      |> Enum.all?(&match?({:ok, :ok}, &1))

    final_snapshot = ConsumerRegistry.consumer_snapshot(stack_id)
    final_generation = ConsumerRegistry.causal_generation(stack_id)

    if all_drained? and final_snapshot == snapshot and final_generation == generation do
      case ConsumerRegistry.close_causal_drain(stack_id, target, generation, token) do
        :ok ->
          :ok

        :retry ->
          Process.sleep(10)
          await_consumer_causal_frontier(stack_id, target, token)
      end
    else
      Process.sleep(10)
      await_consumer_causal_frontier(stack_id, target, token)
    end
  end

  defp notify_connection_opened(%State{connection_manager: manager} = state) do
    :ok = Electric.Connection.Manager.replication_client_started(manager)
    state
  end

  defp notify_system_identified(%State{connection_manager: manager} = state, info) do
    :ok = Electric.Connection.Manager.pg_system_identified(manager, info)
    state
  end

  defp notify_pg_info_obtained(%State{connection_manager: manager} = state, pg_info) do
    :ok = Electric.Connection.Manager.pg_info_obtained(manager, pg_info)
    state
  end

  defp notify_lock_acquisition_error(%State{connection_manager: manager} = state, error) do
    :ok = Electric.Connection.Manager.replication_client_lock_acquisition_failed(manager, error)
    state
  end

  defp notify_lock_acquired(%State{connection_manager: manager} = state) do
    :ok = Electric.Connection.Manager.replication_client_lock_acquired(manager)
    state
  end

  defp notify_created_new_slot(%State{connection_manager: manager} = state) do
    :ok = Electric.Connection.Manager.replication_client_created_new_slot(manager)
    state
  end

  defp notify_ready_to_stream(%State{connection_manager: manager} = state) do
    :ok = Electric.Connection.Manager.replication_client_ready_to_stream(manager)
    state
  end

  defp notify_seen_first_message(%State{connection_manager: manager} = state) do
    :ok = Electric.Connection.Manager.replication_client_streamed_first_message(manager)
    state
  end
end
