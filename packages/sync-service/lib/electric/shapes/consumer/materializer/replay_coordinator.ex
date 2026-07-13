defmodule Electric.Shapes.Consumer.Materializer.ReplayCoordinator do
  @moduledoc false

  use GenServer

  require Logger

  def name(stack_id) do
    Electric.ProcessRegistry.name(stack_id, __MODULE__)
  end

  def start_link(opts) do
    stack_id = Keyword.fetch!(opts, :stack_id)
    GenServer.start_link(__MODULE__, opts, name: name(stack_id))
  end

  def request(stack_id, owner, job_ref)
      when is_pid(owner) and is_reference(job_ref) do
    GenServer.call(name(stack_id), {:request, owner, job_ref})
  end

  def attach_worker(stack_id, owner, job_ref, worker_pid)
      when is_pid(owner) and is_reference(job_ref) and is_pid(worker_pid) do
    GenServer.call(name(stack_id), {:attach_worker, owner, job_ref, worker_pid})
  end

  def progress(stack_id, owner, job_ref, worker_pid)
      when is_pid(owner) and is_reference(job_ref) and is_pid(worker_pid) do
    GenServer.call(name(stack_id), {:progress, owner, job_ref, worker_pid})
  end

  def release(stack_id, owner, job_ref)
      when is_pid(owner) and is_reference(job_ref) do
    GenServer.call(name(stack_id), {:release, owner, job_ref})
  end

  @impl GenServer
  def init(opts) do
    stack_id = Keyword.fetch!(opts, :stack_id)

    max_pending =
      Keyword.get_lazy(opts, :max_pending, fn ->
        Electric.StackConfig.lookup(
          stack_id,
          :materializer_replay_max_pending,
          Electric.Config.default(:materializer_replay_max_pending)
        )
      end)

    idle_timeout_ms =
      Keyword.get_lazy(opts, :idle_timeout_ms, fn ->
        Electric.StackConfig.lookup(
          stack_id,
          :materializer_replay_idle_timeout_ms,
          Electric.Config.default(:materializer_replay_idle_timeout_ms)
        )
      end)

    {:ok,
     %{
       stack_id: stack_id,
       max_pending: max_pending,
       idle_timeout_ms: idle_timeout_ms,
       active: nil,
       queue: :queue.new()
     }}
  end

  @impl GenServer
  def handle_call({:request, owner, job_ref}, _from, state) do
    key = {owner, job_ref}

    cond do
      request_exists?(state, key) ->
        {:reply, {:error, :duplicate_replay_lease}, state}

      replay_admission_count(state) >= state.max_pending ->
        Logger.warning("Rejecting stack replay lease because its bounded queue is full",
          stack_id: state.stack_id,
          replay_max_pending: state.max_pending
        )

        {:reply, {:error, :replay_stack_queue_full}, state}

      is_nil(state.active) ->
        entry = new_entry(owner, job_ref)
        state = activate_entry(entry, state)
        {:reply, :ok, state}

      true ->
        entry = new_entry(owner, job_ref)
        {:reply, :ok, %{state | queue: :queue.in(entry, state.queue)}}
    end
  end

  def handle_call({:attach_worker, owner, job_ref, worker_pid}, _from, state) do
    case state.active do
      %{
        owner: ^owner,
        job_ref: ^job_ref,
        worker_pid: nil,
        terminating?: false
      } = active ->
        if Process.alive?(worker_pid) do
          worker_ref = Process.monitor(worker_pid)
          active = %{active | worker_pid: worker_pid, worker_ref: worker_ref}
          {:reply, :ok, %{state | active: arm_timeout(active, state)}}
        else
          {:reply, {:error, :worker_not_alive}, state}
        end

      _ ->
        {:reply, {:error, :stale_replay_lease}, state}
    end
  end

  def handle_call({:progress, owner, job_ref, worker_pid}, _from, state) do
    case state.active do
      %{
        owner: ^owner,
        job_ref: ^job_ref,
        worker_pid: ^worker_pid,
        terminating?: false
      } = active ->
        {:reply, :ok, %{state | active: arm_timeout(active, state)}}

      _ ->
        {:reply, {:error, :stale_replay_lease}, state}
    end
  end

  def handle_call({:release, owner, job_ref}, _from, state) do
    {:reply, :ok, release_request(state, {owner, job_ref})}
  end

  @impl GenServer
  def handle_info({:replay_idle_timeout, key, generation}, state) do
    case state.active do
      %{key: ^key, timeout_generation: ^generation} = active ->
        Logger.warning("Materializer replay lease exceeded its no-progress deadline",
          stack_id: state.stack_id,
          owner_pid: inspect(active.owner),
          worker_pid: inspect(active.worker_pid),
          replay_idle_timeout_ms: state.idle_timeout_ms
        )

        send(active.owner, {:replay_worker_timeout, active.job_ref, active.worker_pid})
        {:noreply, terminate_active_worker(state)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    state =
      case state.active do
        %{owner: ^pid, owner_ref: ^ref} ->
          terminate_active_worker(state)

        %{worker_pid: ^pid, worker_ref: ^ref} ->
          state |> clear_active() |> promote_next()

        _ ->
          remove_queued_monitor(state, ref, pid)
      end

    {:noreply, state}
  end

  defp new_entry(owner, job_ref) do
    %{
      key: {owner, job_ref},
      owner: owner,
      job_ref: job_ref,
      owner_ref: Process.monitor(owner),
      worker_pid: nil,
      worker_ref: nil,
      terminating?: false,
      timeout_ref: nil,
      timeout_generation: 0
    }
  end

  defp activate_entry(entry, state) do
    send(entry.owner, {:replay_coordinator_granted, entry.job_ref})
    %{state | active: arm_timeout(entry, state)}
  end

  defp arm_timeout(entry, state) do
    if is_reference(entry.timeout_ref), do: Process.cancel_timer(entry.timeout_ref)
    generation = entry.timeout_generation + 1

    timeout_ref =
      Process.send_after(
        self(),
        {:replay_idle_timeout, entry.key, generation},
        state.idle_timeout_ms
      )

    %{entry | timeout_ref: timeout_ref, timeout_generation: generation}
  end

  defp release_request(state, key) do
    case state.active do
      %{key: ^key} ->
        terminate_active_worker(state)

      _ ->
        {removed, queued} =
          state.queue
          |> :queue.to_list()
          |> Enum.split_with(&(&1.key == key))

        Enum.each(removed, &cleanup_entry/1)
        %{state | queue: :queue.from_list(queued)}
    end
  end

  defp terminate_active_worker(%{active: active} = state) do
    if is_pid(active.worker_pid) and Process.alive?(active.worker_pid) do
      if is_reference(active.timeout_ref), do: Process.cancel_timer(active.timeout_ref)
      Process.exit(active.worker_pid, :kill)

      %{
        state
        | active: %{active | terminating?: true, timeout_ref: nil}
      }
    else
      state |> clear_active() |> promote_next()
    end
  end

  defp remove_queued_monitor(state, ref, pid) do
    {removed, queued} =
      state.queue
      |> :queue.to_list()
      |> Enum.split_with(&(&1.owner == pid and &1.owner_ref == ref))

    Enum.each(removed, &cleanup_entry/1)
    %{state | queue: :queue.from_list(queued)}
  end

  defp clear_active(state) do
    cleanup_entry(state.active)
    %{state | active: nil}
  end

  defp cleanup_entry(entry) do
    if is_reference(entry.timeout_ref), do: Process.cancel_timer(entry.timeout_ref)
    Process.demonitor(entry.owner_ref, [:flush])
    if is_reference(entry.worker_ref), do: Process.demonitor(entry.worker_ref, [:flush])
    :ok
  end

  defp promote_next(state) do
    case :queue.out(state.queue) do
      {:empty, queue} ->
        %{state | queue: queue}

      {{:value, entry}, queue} ->
        state = %{state | queue: queue}

        if Process.alive?(entry.owner) do
          activate_entry(entry, state)
        else
          cleanup_entry(entry)
          promote_next(state)
        end
    end
  end

  defp request_exists?(state, key) do
    match?(%{key: ^key}, state.active) or
      Enum.any?(:queue.to_list(state.queue), &(&1.key == key))
  end

  defp replay_admission_count(state) do
    if(is_nil(state.active), do: 0, else: 1) + :queue.len(state.queue)
  end
end
