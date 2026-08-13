defmodule Electric.Shapes.ConsumerRegistry do
  alias Electric.ShapeCache
  alias Electric.Telemetry.OpenTelemetry

  import Electric, only: [is_stack_id: 1, is_shape_handle: 1]

  require Logger

  defstruct table: nil,
            stack_id: nil

  @type stack_id() :: Electric.stack_id()
  @type stack_ref() :: stack_id() | [stack_id: stack_id()] | %{stack_id: stack_id()}
  @type shape_handle() :: Electric.shape_handle()
  @type t() :: %__MODULE__{
          table: :ets.table(),
          stack_id: stack_id()
        }

  @consumer_suspend_reason Electric.ShapeCache.ShapeCleaner.consumer_suspend_reason()
  # The metadata row is
  # {key, active_target, generation, drain_owner, in_flight_topology_mutations}.
  # A topology mutation publishes its owner token before touching consumer rows,
  # then removes the token and advances the generation after the change. This
  # lets close_causal_drain/4 serialize atomically with both sides of the ETS
  # insert/delete instead of observing the gap between them.
  @causal_epoch_key :electric_causal_epoch

  def name(stack_id, shape_handle) when is_stack_id(stack_id) and is_shape_handle(shape_handle) do
    {:via, __MODULE__, {stack_id, shape_handle}}
  end

  def register_name({stack_id, shape_handle}, pid)
      when is_stack_id(stack_id) and is_shape_handle(shape_handle) do
    if register_consumer!(pid, shape_handle, ets_name(stack_id)), do: :yes, else: :no
  end

  # This is intentionally a no-op. The ETS entry is removed explicitly via
  # remove_consumer/2 as part of shape cleanup in ShapeCleaner, not
  # automatically when the consumer process exits.
  #
  # If we removed the ETS entry here on process exit, there's a race: the SLC
  # could receive an operation for the shape, see no consumer registered, start
  # a new one, and _then_ get the "remove shape" call for the old handle —
  # leaving an orphan consumer process.
  #
  # A crashed consumer is never restarted by a supervisor. Its shape handle is
  # invalidated and a fresh shape (with a new handle and new consumer) is
  # created on the next client request. But since shape invalidation is async,
  # we keep the entry in the registry in the meantime to avoid accidentally
  # restarting the consumer for it to process new transactions when the shape
  # is already on the way out.
  def unregister_name({_stack_id, _shape_handle}) do
    :ok
  end

  def whereis_name({stack_id, shape_handle}) do
    whereis(stack_id, shape_handle) || :undefined
  end

  @spec whereis(stack_ref(), shape_handle()) :: pid() | nil
  def whereis(stack_ref, shape_handle) when is_shape_handle(shape_handle) do
    consumer_pid(shape_handle, ets_name(stack_ref))
  end

  @spec active_consumer_count(stack_id()) :: non_neg_integer()
  def active_consumer_count(stack_id) when is_binary(stack_id) do
    table = ets_name(stack_id)

    case :ets.info(table, :size) do
      :undefined -> 0
      size -> max(size - metadata_entry_count(table), 0)
    end
  rescue
    ArgumentError -> 0
  end

  @doc false
  @spec consumer_snapshot(stack_id()) :: %{shape_handle() => pid()}
  def consumer_snapshot(stack_id) when is_binary(stack_id) do
    # A missing table means the shape subsystem is restarting, not that it has
    # zero causal work. Let ETS raise so startup readiness fails closed and is
    # retried against the replacement registry.
    stack_id
    |> ets_name()
    |> :ets.tab2list()
    |> Enum.filter(&consumer_entry?/1)
    |> Map.new()
  end

  @doc false
  @spec activate_causal_drain(stack_id(), non_neg_integer()) ::
          {:ok, {pid(), reference()}} | {:error, term()}
  def activate_causal_drain(stack_id, target)
      when is_binary(stack_id) and is_integer(target) and target >= 0 do
    table = ets_name(stack_id)
    token = {self(), make_ref()}
    claim_causal_drain(table, target, token)
  end

  @doc false
  @spec deactivate_causal_drain(stack_id(), {pid(), reference()}) :: :ok
  def deactivate_causal_drain(stack_id, token) when is_binary(stack_id) do
    deactivate_causal_drain_table(ets_name(stack_id), token)
  end

  @doc false
  @spec close_causal_drain(
          stack_id(),
          non_neg_integer(),
          non_neg_integer(),
          {pid(), reference()}
        ) :: :ok | :retry
  def close_causal_drain(stack_id, target, expected_generation, token)
      when is_binary(stack_id) and is_integer(target) and target >= 0 and
             is_integer(expected_generation) and expected_generation >= 0 do
    table = ets_name(stack_id)

    case :ets.lookup(table, @causal_epoch_key) do
      [
        {@causal_epoch_key, ^target, ^expected_generation, ^token, []} = current
      ] ->
        replacement = {@causal_epoch_key, nil, expected_generation, nil, []}

        if replace_epoch_row(table, current, replacement) == 1,
          do: :ok,
          else: :retry

      [
        {@causal_epoch_key, ^target, ^expected_generation, ^token, topology_mutations} = current
      ] ->
        reap_dead_topology_mutations(table, current, topology_mutations)

      _ ->
        :retry
    end
  end

  defp deactivate_causal_drain_table(table, token) do
    case :ets.lookup(table, @causal_epoch_key) do
      [{@causal_epoch_key, target, generation, ^token, topology_mutations}] ->
        current = {@causal_epoch_key, target, generation, token, topology_mutations}
        replacement = {@causal_epoch_key, nil, generation, nil, topology_mutations}

        if replace_epoch_row(table, current, replacement) == 1,
          do: :ok,
          else: deactivate_causal_drain_table(table, token)

      _ ->
        :ok
    end
  end

  @doc false
  @spec causal_generation(stack_id()) :: non_neg_integer()
  def causal_generation(stack_id) when is_binary(stack_id) do
    :ets.lookup_element(ets_name(stack_id), @causal_epoch_key, 3)
  end

  @doc false
  @spec mark_causal_work_created(stack_id(), non_neg_integer()) :: :ok
  def mark_causal_work_created(stack_id, tx_offset)
      when is_binary(stack_id) and is_integer(tx_offset) and tx_offset >= 0 do
    table = ets_name(stack_id)

    case :ets.lookup(table, @causal_epoch_key) do
      [{@causal_epoch_key, target, generation, owner, topology_mutations} = current]
      when is_integer(target) and tx_offset <= target ->
        replacement =
          {@causal_epoch_key, target, generation + 1, owner, topology_mutations}

        if replace_epoch_row(table, current, replacement) == 0,
          do: mark_causal_work_created(stack_id, tx_offset)

      _ ->
        :ok
    end

    :ok
  end

  @doc false
  @spec with_consumer_topology_mutation(stack_id(), (-> result)) :: result when result: term()
  def with_consumer_topology_mutation(stack_id, fun)
      when is_binary(stack_id) and is_function(fun, 0) do
    do_with_consumer_topology_mutation(ets_name(stack_id), fun)
  end

  @spec register_consumer(pid(), shape_handle(), stack_id()) :: {:ok, non_neg_integer()}
  def register_consumer(pid, shape_handle, stack_id) when is_binary(stack_id) do
    register_consumer(pid, shape_handle, ets_name(stack_id))
  end

  @spec register_consumer(pid(), shape_handle(), t()) :: {:ok, non_neg_integer()}
  def register_consumer(pid, shape_handle, %__MODULE__{table: table}) do
    register_consumer(pid, shape_handle, table)
  end

  @spec register_consumer(pid(), shape_handle(), :ets.table()) :: {:ok, non_neg_integer()}
  def register_consumer(pid, shape_handle, table) when is_atom(table) or is_reference(table) do
    register_consumer!(pid, shape_handle, table)
    :ok
  end

  defp register_consumer!(pid, shape_handle, table)
       when is_pid(pid) and (is_atom(table) or is_reference(table)) do
    do_with_consumer_topology_mutation(table, fn ->
      :ets.insert_new(table, [{shape_handle, pid}])
    end)
  end

  @spec publish(%{shape_handle() => term()}, t()) :: %{shape_handle() => term()}
  def publish(events_by_handle, _registry_state) when events_by_handle == %{}, do: %{}

  def publish(events_by_handle, registry_state) do
    {suspended, undeliverable} = resolve_and_broadcast(events_by_handle, registry_state)

    # Retry suspended consumers once with fresh consumer processes.
    # We don't expect new suspensions here since we're targeting previously
    # suspended consumers explicitly.
    Enum.each(suspended, fn {handle, _event} -> remove_consumer(handle, registry_state) end)
    {still_suspended, retry_undeliverable} = resolve_and_broadcast(suspended, registry_state)

    removed_shapes =
      if still_suspended != %{} do
        handles = Map.keys(still_suspended)
        Logger.warning(["Consumers still suspended after retry: ", inspect(handles)])
        Electric.ShapeCache.ShapeCleaner.remove_shapes(registry_state.stack_id, handles)
        Map.new(handles, &{&1, {:publish, :shape_removed}})
      else
        %{}
      end

    undeliverable
    |> Map.merge(retry_undeliverable)
    |> Map.merge(removed_shapes)
  end

  defp resolve_and_broadcast(events_by_handle, _registry_state)
       when events_by_handle == %{}, do: {%{}, %{}}

  defp resolve_and_broadcast(events_by_handle, %{table: table} = registry_state) do
    {to_broadcast, undeliverable} =
      Enum.reduce(events_by_handle, {[], %{}}, fn {handle, event}, {acc, undeliverable} ->
        case resolve_consumer(handle, table, registry_state) do
          {:ok, pid} ->
            {[{handle, event, pid} | acc], undeliverable}

          {:error, :no_shape} ->
            {acc, Map.put(undeliverable, handle, {:publish, :no_shape})}

          {:error, reason} ->
            failure = {:publish, {:consumer_start_failed, reason}}
            {acc, Map.put(undeliverable, handle, failure)}
        end
      end)

    {suspended, crashed_or_missing} = broadcast(to_broadcast)
    {suspended, Map.merge(undeliverable, crashed_or_missing)}
  end

  @spec remove_consumer(shape_handle(), t()) :: :ok
  def remove_consumer(shape_handle, %__MODULE__{table: table}) do
    do_remove_consumer(shape_handle, table)
  end

  @spec remove_consumer(shape_handle(), stack_id()) :: :ok
  def remove_consumer(shape_handle, stack_id) when is_stack_id(stack_id) do
    do_remove_consumer(shape_handle, ets_name(stack_id))
  end

  @spec do_remove_consumer(shape_handle(), :ets.table()) :: :ok
  defp do_remove_consumer(shape_handle, table) when is_atom(table) or is_reference(table) do
    do_with_consumer_topology_mutation(table, fn -> :ets.delete(table, shape_handle) end)
    :ok
  rescue
    # ShapeCleaner may observe a consumer DOWN while the stack supervisor is
    # concurrently replacing the registry. The old table is already empty in
    # that case, so removal has reached its intended state.
    ArgumentError -> :ok
  end

  @doc """
  Calls many GenServers asynchronously with per-handle messages and waits
  for their responses before returning.

  Returns a tuple `{suspended, crashed}` where:
  - `suspended` is a map of `shape_handle => event` for handles whose consumers
    suspended (these should be retried by the caller)
  - `crashed` is a map of `shape_handle => exit_reason` for handles whose consumers
    crashed (these should NOT be retried)

  There is no timeout so if the GenServers do not respond or die, this
  function will block indefinitely.
  """
  @spec broadcast([{shape_handle(), term(), pid() | nil}]) ::
          {%{shape_handle() => term()}, %{shape_handle() => term()}}
  def broadcast(handle_event_pids) do
    # Based on OTP GenServer.call, see:
    # https://github.com/erlang/otp/blob/090c308d7c925e154240685174addaa516ea2f69/lib/stdlib/src/gen.erl#L243
    #
    # Filter out nil pids to handle the race condition where a shape is removed
    # from ShapeStatus but events still arrive for it (EventRouter removal is async).
    # When start_consumer_for_handle returns {:error, :no_shape}, the pid is nil.
    handle_event_pids
    |> Enum.reject(fn {_handle, _event, pid} -> is_nil(pid) end)
    |> Enum.map(fn {handle, event, pid} ->
      ref = Process.monitor(pid)
      send(pid, {:"$gen_call", {self(), ref}, event})
      {handle, event, ref}
    end)
    |> Enum.reduce({%{}, %{}}, fn {handle, event, ref}, {suspended, crashed} ->
      receive do
        {^ref, _reply} ->
          Process.demonitor(ref, [:flush])
          {suspended, crashed}

        {:DOWN, ^ref, _, _, @consumer_suspend_reason} ->
          # Consumer is in the act of suspending as the txn arrives.
          # Return for retry (publish/2 will start a new consumer instance).
          {Map.put(suspended, handle, event), crashed}

        {:DOWN, ^ref, _, _, reason} ->
          # Consumer crashed — do not retry, return the crash reason.
          {suspended, Map.put(crashed, handle, reason)}
      end
    end)
    |> tap(fn
      {suspended, crashed} when suspended == %{} and crashed == %{} ->
        :ok

      {suspended, crashed} ->
        if suspended != %{} do
          Logger.debug(fn ->
            ["Re-trying suspended shape handles ", inspect(Map.keys(suspended))]
          end)
        end

        if crashed != %{} do
          Logger.warning(fn ->
            ["Consumer processes crashed or missing during broadcast: ", inspect(crashed)]
          end)
        end
    end)
  end

  @doc """
  Dynamically (re-)enable consumer suspension on all running consumers.

  This allows for dynamically re-configuring consumer suspension even if it was
  disabled, because the configuration message will have the side-effect of
  waking all consumers from hibernation.

  The `jitter_period` value allows for spreading the hibernation of existing
  consumers over a time period to avoid a sudden rush of hibernation events.
  Each consumer picks a random timeout between `hibernate_after` and `jitter_period`,
  then hibernates and schedules suspension for `suspend_after` ms later.

  To re-enable consumer suspend:

      # hibernation timeout: 1 min, suspend timeout: 4 min, jitter window: 20 min
      # Consumers will hibernate between 1-20 min, then suspend 4 min after hibernating
      Electric.Shapes.ConsumerRegistry.enable_suspend(stack_id, 60_000, 4 * 60_000, 60_000 * 20)

  Disabling suspension is as easy as:

      Electric.StackConfig.put(stack_id, :shape_enable_suspend?, false)

  """
  @spec enable_suspend(stack_id(), pos_integer(), pos_integer(), pos_integer()) ::
          consumer_count :: non_neg_integer()
  def enable_suspend(stack_id, hibernate_after, suspend_after, jitter_period)
      when is_integer(hibernate_after) and is_integer(suspend_after) and
             is_integer(jitter_period) and jitter_period > hibernate_after do
    Electric.StackConfig.put(stack_id, :shape_hibernate_after, hibernate_after)
    Electric.StackConfig.put(stack_id, :shape_suspend_after, suspend_after)
    Electric.StackConfig.put(stack_id, :shape_enable_suspend?, true)

    :ets.foldl(
      fn
        {shape_handle, pid}, n when is_binary(shape_handle) and is_pid(pid) ->
          if Process.alive?(pid),
            do: send(pid, {:configure_suspend, hibernate_after, suspend_after, jitter_period})

          n + 1

        _metadata, n ->
          n
      end,
      0,
      ets_name(stack_id)
    )
  end

  defp consumer_pid(handle, table) do
    :ets.lookup_element(table, handle, 2, nil)
  rescue
    ArgumentError -> nil
  end

  defp resolve_consumer(handle, table, state) do
    case consumer_pid(handle, table) do
      pid when is_pid(pid) -> {:ok, pid}
      nil -> start_consumer(handle, state)
    end
  end

  defp start_consumer(handle, %__MODULE__{stack_id: stack_id} = state) do
    OpenTelemetry.with_span(
      "consumer_registry.start_consumer",
      ["shape.handle": handle],
      state.stack_id,
      fn ->
        otel_ctx = OpenTelemetry.get_current_context()

        case ShapeCache.start_consumer_for_handle(handle, stack_id, otel_ctx: otel_ctx) do
          {:ok, pid} ->
            Logger.debug(fn -> ["Started consumer for existing handle ", handle] end)

            {:ok, pid}

          {:error, :no_shape} ->
            {:error, :no_shape}

          {:error, reason} ->
            # ShapeCache fails closed and removes an unrestorable shape before
            # returning this error. Keep the failure local to this handle so a
            # single corrupt shape cannot crash the ShapeLogCollector and take
            # every other shape offline.
            Logger.error(
              "Unable to start consumer for shape #{handle}: #{inspect(reason)}; " <>
                "marking the shape undeliverable"
            )

            {:error, reason}
        end
      end
    )
  end

  @doc false
  def registry_table(stack_id) do
    table =
      :ets.new(ets_name(stack_id), [
        :public,
        :named_table,
        write_concurrency: :auto,
        read_concurrency: true
      ])

    true = :ets.insert(table, {@causal_epoch_key, nil, 0, nil, []})
    table
  end

  def new(stack_id, opts \\ []) when is_binary(stack_id) do
    table = registry_table(stack_id)
    Electric.Shapes.Consumer.Materializer.init_link_values_table(stack_id)

    state = struct(__MODULE__, Keyword.merge(opts, stack_id: stack_id, table: table))

    {:ok, state}
  end

  defp ets_name(opts) when is_list(opts) or is_map(opts) do
    ets_name(Access.fetch!(opts, :stack_id))
  end

  defp ets_name(stack_id) when is_stack_id(stack_id) do
    :"#{inspect(__MODULE__)}:#{stack_id}"
  end

  defp claim_causal_drain(table, target, token) do
    case :ets.lookup(table, @causal_epoch_key) do
      [{@causal_epoch_key, nil, generation, nil, topology_mutations} = current] ->
        replacement = {@causal_epoch_key, target, generation, token, topology_mutations}

        if replace_epoch_row(table, current, replacement) == 1,
          do: {:ok, token},
          else: claim_causal_drain(table, target, token)

      [
        {@causal_epoch_key, _active_target, generation, {owner_pid, _owner_ref},
         topology_mutations} = current
      ] ->
        if Process.alive?(owner_pid) do
          {:error, {:causal_drain_already_active, owner_pid}}
        else
          replacement = {@causal_epoch_key, target, generation, token, topology_mutations}

          if replace_epoch_row(table, current, replacement) == 1,
            do: {:ok, token},
            else: claim_causal_drain(table, target, token)
        end
    end
  end

  defp replace_epoch_row(table, current, replacement) do
    :ets.select_replace(table, [{current, [], [{:const, replacement}]}])
  end

  defp do_with_consumer_topology_mutation(table, fun) do
    token = {self(), make_ref()}
    begin_consumer_topology_mutation(table, token)

    try do
      fun.()
    after
      finish_consumer_topology_mutation(table, token)
    end
  end

  defp begin_consumer_topology_mutation(table, token) do
    case :ets.lookup(table, @causal_epoch_key) do
      [{@causal_epoch_key, target, generation, owner, topology_mutations} = current] ->
        {live_mutations, dead_mutations} = partition_topology_mutations(topology_mutations)

        replacement =
          {@causal_epoch_key, target, generation + dead_generation_bump(dead_mutations), owner,
           [token | live_mutations]}

        if replace_epoch_row(table, current, replacement) == 1,
          do: :ok,
          else: begin_consumer_topology_mutation(table, token)
    end
  end

  defp finish_consumer_topology_mutation(table, token) do
    case :ets.lookup(table, @causal_epoch_key) do
      [{@causal_epoch_key, target, generation, owner, topology_mutations} = current] ->
        case List.delete(topology_mutations, token) do
          ^topology_mutations ->
            raise "consumer topology mutation token is not active"

          remaining ->
            replacement = {@causal_epoch_key, target, generation + 1, owner, remaining}

            if replace_epoch_row(table, current, replacement) == 1,
              do: :ok,
              else: finish_consumer_topology_mutation(table, token)
        end
    end
  end

  defp reap_dead_topology_mutations(table, current, topology_mutations) do
    {live_mutations, dead_mutations} = partition_topology_mutations(topology_mutations)

    case dead_mutations do
      [] ->
        :retry

      _ ->
        # A dead owner may have changed the consumer table before it died. Drop
        # its token, but conservatively advance the generation and force a new
        # fixed-point pass rather than letting this close attempt succeed.
        {@causal_epoch_key, target, generation, owner, ^topology_mutations} = current

        replacement =
          {@causal_epoch_key, target, generation + 1, owner, live_mutations}

        _ = replace_epoch_row(table, current, replacement)
        :retry
    end
  end

  defp partition_topology_mutations(topology_mutations) do
    Enum.split_with(topology_mutations, fn {owner_pid, _owner_ref} ->
      Process.alive?(owner_pid)
    end)
  end

  defp dead_generation_bump([]), do: 0
  defp dead_generation_bump(_dead_mutations), do: 1

  defp metadata_entry_count(table) do
    case :ets.lookup(table, @causal_epoch_key) do
      [] -> 0
      [_metadata] -> 1
    end
  end

  defp consumer_entry?({shape_handle, pid}),
    do: is_binary(shape_handle) and is_pid(pid)

  defp consumer_entry?(_entry), do: false
end
