defmodule Electric.Client.Fetch.Monitor do
  @moduledoc false

  # Companion process that registers processes listening for the result of a
  # given client request.
  #
  # Separates the list of subscribers from the actual request process so that
  # if the request process crashes the list of subscribers is retained and also
  # so that registering subscribers can happen while the request process is
  # blocked performing its actual HTTP request.

  use GenServer

  alias Electric.Client.Fetch

  require Logger

  @request_timeout {Electric.Client.Fetch.Pool, :request_timeout}

  @type deadline :: integer() | :infinity
  @type registration ::
          {reference(), reference(), pid(), reference(), reference(), deadline()}

  def name(request_id) do
    {:via, Registry, {Electric.Client.Registry, {__MODULE__, request_id}}}
  end

  def child_spec({request_id, _request, _client} = args) do
    child_spec(request_id, args)
  end

  def child_spec({request_id, _request, _client, _deadline} = args) do
    child_spec(request_id, args)
  end

  def child_spec({request_id, _request, _client, _deadline, _requester_pid} = args) do
    child_spec(request_id, args)
  end

  defp child_spec(request_id, args) do
    %{
      id: {__MODULE__, request_id},
      start: {__MODULE__, :start_link, [args]},
      # don't restart on error because it would lose the subscriber list
      # we instead want the requesting processes to know about the failure
      restart: :temporary,
      type: :worker
    }
  end

  def start_link({request_id, request, client}) do
    start_link({request_id, request, client, :infinity})
  end

  def start_link({request_id, request, client, deadline}) do
    start_link({request_id, request, client, deadline, nil})
  end

  def start_link({request_id, request, client, deadline, requester_pid}) do
    GenServer.start_link(
      __MODULE__,
      {request_id, request, client, deadline, requester_pid},
      name: name(request_id)
    )
  end

  @spec register(pid(), pid(), timeout()) :: registration() | {:error, term()}
  def register(monitor_pid, listener_pid, timeout \\ :infinity)

  def register(monitor_pid, listener_pid, timeout)
      when timeout == :infinity or (is_integer(timeout) and timeout >= 0) do
    register_until(monitor_pid, listener_pid, deadline_after(timeout))
  end

  @doc false
  @spec register_until(pid(), pid(), deadline()) :: registration() | {:error, term()}
  def register_until(monitor_pid, listener_pid, deadline)
      when deadline == :infinity or is_integer(deadline) do
    # Register the calling pid with the monitor and the monitor with the
    # calling pid. The separate registration ref lets us retract a call that
    # reached the monitor only after GenServer.call/3 timed out.
    caller_monitor_ref = Process.monitor(monitor_pid)
    registration_ref = make_ref()
    reply_alias = Process.alias()

    case remaining_ms(deadline) do
      0 ->
        Process.demonitor(caller_monitor_ref, [:flush])
        Process.unalias(reply_alias)
        {:error, @request_timeout}

      call_timeout ->
        try do
          case GenServer.call(
                 monitor_pid,
                 {:register, listener_pid, registration_ref, reply_alias, deadline},
                 call_timeout
               ) do
            {:ok, monitor_caller_ref} ->
              {
                caller_monitor_ref,
                monitor_caller_ref,
                monitor_pid,
                registration_ref,
                reply_alias,
                deadline
              }

            {:error, @request_timeout} = error ->
              Process.demonitor(caller_monitor_ref, [:flush])
              Process.unalias(reply_alias)
              error
          end
        catch
          :exit, reason ->
            Process.demonitor(caller_monitor_ref, [:flush])
            Process.unalias(reply_alias)
            GenServer.cast(monitor_pid, {:cancel_registration, registration_ref})
            exit(reason)
        end
    end
  end

  @spec wait(registration()) :: Fetch.Response.t() | {:error, term()}
  def wait(
        {_caller_ref, _listener_ref, _monitor_pid, _registration_ref, _reply_alias, deadline} =
          registration
      ) do
    wait_until(registration, deadline)
  end

  @doc false
  @spec wait_until(registration(), deadline()) :: Fetch.Response.t() | {:error, term()}
  def wait_until(
        {caller_monitor_ref, monitor_caller_ref, _monitor_pid, _registration_ref, reply_alias,
         _deadline},
        :infinity
      ) do
    receive do
      {:response, ^monitor_caller_ref, response} ->
        Process.demonitor(caller_monitor_ref, [:flush])
        Process.unalias(reply_alias)
        response

      {:DOWN, ^caller_monitor_ref, :process, _pid, reason} ->
        Process.unalias(reply_alias)
        monitor_down!(reason)
    end
  end

  def wait_until(
        {caller_monitor_ref, monitor_caller_ref, _monitor_pid, _registration_ref, reply_alias,
         _deadline} = registration,
        deadline
      )
      when is_integer(deadline) do
    case remaining_ms(deadline) do
      0 ->
        timeout_registration(registration)

      timeout ->
        receive do
          {:response, ^monitor_caller_ref, response} ->
            Process.demonitor(caller_monitor_ref, [:flush])
            Process.unalias(reply_alias)
            response

          {:DOWN, ^caller_monitor_ref, :process, _pid, reason} ->
            Process.unalias(reply_alias)
            monitor_down!(reason)
        after
          timeout -> timeout_registration(registration)
        end
    end
  end

  defp monitor_down!(reason) do
    raise Electric.Client.Error,
      message: "#{Fetch.Monitor} process died with reason #{inspect(reason)}"
  end

  defp timeout_registration(
         {caller_monitor_ref, _monitor_caller_ref, monitor_pid, registration_ref, reply_alias,
          _deadline}
       ) do
    Process.demonitor(caller_monitor_ref, [:flush])
    Process.unalias(reply_alias)
    GenServer.cast(monitor_pid, {:cancel_registration, registration_ref})
    {:error, @request_timeout}
  end

  def reply(pid, response) when is_pid(pid) do
    GenServer.call(pid, {:reply, response})
  end

  @impl true
  def init({request_id, request, client, deadline, requester_pid}) do
    Process.flag(:trap_exit, true)

    state = %{
      request_id: request_id,
      request: request,
      client: client,
      subscribers: [],
      response: nil,
      registration_timer_ref: schedule_registration_deadline(deadline),
      startup_requester_ref: monitor_requester(requester_pid)
    }

    {:ok, state}
  end

  @impl true
  def handle_continue(:start_request, state) do
    %{request_id: request_id, request: request, client: client} = state

    {:ok, _pid} = Fetch.Request.start_link({request_id, request, client, self()})

    {:noreply, state}
  end

  @impl true
  def handle_call(
        {:register, listener_pid, registration_ref, reply_alias, deadline},
        _from,
        state
      ) do
    cond do
      deadline_expired?(deadline) and state.subscribers == [] ->
        {:stop, {:shutdown, :registration_deadline}, {:error, @request_timeout}, state}

      deadline_expired?(deadline) ->
        {:reply, {:error, @request_timeout}, state}

      true ->
        start_request? = state.subscribers == []
        state = clear_startup_guards(state)
        {ref, state} = add_subscriber(listener_pid, registration_ref, reply_alias, state)

        if start_request? do
          {:reply, {:ok, ref}, state, {:continue, :start_request}}
        else
          {:reply, {:ok, ref}, state}
        end
    end
  end

  def handle_call({:reply, response}, _from, %{subscribers: subscribers} = state) do
    case response do
      %{status: status} ->
        Logger.debug(
          fn ->
            "Returning response #{status}"
          end,
          request_id: state.request_id
        )

      {:error, %{status: _} = response} ->
        Logger.warning(
          fn ->
            "Request failed: #{inspect(response)}"
          end,
          request_id: state.request_id
        )

      {:error, reason} ->
        Logger.error(
          fn ->
            "Request failed: #{inspect(reason)}"
          end,
          request_id: state.request_id
        )
    end

    for {_pid, ref, _registration_ref, reply_alias} <- subscribers do
      send(reply_alias, {:response, ref, response})
    end

    {:stop, {:shutdown, :normal}, state}
  end

  @impl true
  def handle_cast({:cancel_registration, registration_ref}, state) do
    case pop_subscriber_by_registration(state, registration_ref) do
      {nil, state} ->
        {:noreply, state}

      {{_pid, listener_ref, ^registration_ref, _reply_alias}, state} ->
        Process.demonitor(listener_ref, [:flush])
        continue_or_stop(state)
    end
  end

  @impl true
  def handle_info(
        {:DOWN, ref, :process, requester_pid, reason},
        %{startup_requester_ref: ref, subscribers: []} = state
      ) do
    Logger.debug(fn ->
      "Initial requester #{inspect(requester_pid)} exited with reason #{inspect(reason)} before registering. Stopping request monitor."
    end)

    {:stop, {:shutdown, :requester_down}, state}
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    Logger.debug(fn ->
      [
        message:
          "Listener #{inspect(pid)} exited with reason #{inspect(reason)}. Removing from subscribers",
        request_id: state.request_id
      ]
    end)

    {_subscriber, state} = pop_subscriber_by_listener(state, ref)

    continue_or_stop(state)
  end

  def handle_info(:registration_deadline, %{subscribers: []} = state) do
    {:stop, {:shutdown, :registration_deadline}, state}
  end

  def handle_info(:registration_deadline, state), do: {:noreply, state}

  def handle_info({:EXIT, pid, reason}, state) do
    Logger.debug(fn ->
      "Request process #{inspect(pid)} exited with reason #{inspect(reason)} before issuing a reply. Using reason as an error and exiting."
    end)

    for {_pid, ref, _registration_ref, reply_alias} <- state.subscribers do
      send(reply_alias, {:response, ref, {:error, reason}})
    end

    {:stop, {:shutdown, :normal}, state}
  end

  defp add_subscriber(listener_pid, registration_ref, reply_alias, state) do
    ref = Process.monitor(listener_pid)

    Logger.debug(
      fn -> "Registering listener pid #{inspect(listener_pid)}" end,
      request_id: state.request_id
    )

    state =
      Map.update!(
        state,
        :subscribers,
        &[{listener_pid, ref, registration_ref, reply_alias} | &1]
      )

    {ref, state}
  end

  defp pop_subscriber_by_listener(%{subscribers: subscribers} = state, ref) do
    case List.keytake(subscribers, ref, 1) do
      nil -> {nil, state}
      {subscriber, subscribers} -> {subscriber, %{state | subscribers: subscribers}}
    end
  end

  defp pop_subscriber_by_registration(%{subscribers: subscribers} = state, registration_ref) do
    case List.keytake(subscribers, registration_ref, 2) do
      nil -> {nil, state}
      {subscriber, subscribers} -> {subscriber, %{state | subscribers: subscribers}}
    end
  end

  defp schedule_registration_deadline(:infinity), do: nil

  defp schedule_registration_deadline(deadline) do
    Process.send_after(self(), :registration_deadline, remaining_ms(deadline))
  end

  defp cancel_registration_deadline(%{registration_timer_ref: nil} = state), do: state

  defp cancel_registration_deadline(%{registration_timer_ref: timer_ref} = state) do
    Process.cancel_timer(timer_ref)

    receive do
      :registration_deadline -> :ok
    after
      0 -> :ok
    end

    %{state | registration_timer_ref: nil}
  end

  defp monitor_requester(requester_pid) when is_pid(requester_pid),
    do: Process.monitor(requester_pid)

  defp monitor_requester(nil), do: nil

  defp clear_startup_guards(state) do
    state
    |> cancel_registration_deadline()
    |> demonitor_startup_requester()
  end

  defp demonitor_startup_requester(%{startup_requester_ref: nil} = state), do: state

  defp demonitor_startup_requester(%{startup_requester_ref: requester_ref} = state) do
    Process.demonitor(requester_ref, [:flush])
    %{state | startup_requester_ref: nil}
  end

  defp deadline_after(:infinity), do: :infinity

  defp deadline_after(timeout) do
    System.monotonic_time(:millisecond) + timeout
  end

  defp remaining_ms(:infinity), do: :infinity

  defp remaining_ms(deadline) do
    max(deadline - System.monotonic_time(:millisecond), 0)
  end

  defp deadline_expired?(:infinity), do: false
  defp deadline_expired?(deadline), do: remaining_ms(deadline) == 0

  # The request is linked to this monitor, so stopping after the final
  # subscriber leaves also cancels the now-unobservable in-flight fetch.
  defp continue_or_stop(%{subscribers: []} = state),
    do: {:stop, {:shutdown, :no_subscribers}, state}

  defp continue_or_stop(state), do: {:noreply, state}
end
