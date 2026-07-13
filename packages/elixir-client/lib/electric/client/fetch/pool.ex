defmodule Electric.Client.Fetch.Pool do
  @moduledoc """
  Coaleses requests so that multiple client instances making the same
  (potentially long-polling) request will all use the same request process.
  """

  alias Electric.Client
  alias Electric.Client.Fetch

  require Logger

  @request_timeout {__MODULE__, :request_timeout}

  @callback request(Client.t(), Fetch.Request.t(), opts :: Keyword.t()) ::
              Fetch.Response.t() | {:error, Fetch.Response.t() | term()}

  @behaviour __MODULE__

  @impl Electric.Client.Fetch.Pool
  def request(%Client{} = client, %Fetch.Request{} = request, opts) do
    timeout = request_timeout!(opts)
    deadline = deadline_after(timeout)

    do_request(client, request, deadline)
  end

  defp do_request(client, request, deadline) do
    request_id = request_id(client, request)

    # The monitor process is unique to the request and launches the actual
    # request as a linked process.
    #
    # This coalesces requests, so no matter how many simultaneous
    # clients we have, we only ever make one request to the backend.
    case start_monitor(request_id, request, client, deadline, self()) do
      {:ok, monitor_pid} ->
        register_and_wait(monitor_pid, client, request, deadline)

      {:retry, reason} ->
        retry_or_timeout(client, request, deadline, reason)

      error ->
        error
    end
  end

  defp register_and_wait(monitor_pid, client, request, deadline) do
    if deadline_expired?(deadline) do
      {:error, @request_timeout}
    else
      try do
        case Fetch.Monitor.register_until(monitor_pid, self(), deadline) do
          {:error, @request_timeout} = error ->
            error

          registration ->
            Fetch.Monitor.wait_until(registration, deadline)
        end
      catch
        :exit, reason ->
          retry_or_timeout(client, request, deadline, reason)
      end
    end
  end

  defp retry_or_timeout(client, request, deadline, reason) do
    if deadline_expired?(deadline) do
      {:error, @request_timeout}
    else
      Logger.debug(fn ->
        "Request process ended with reason #{inspect(reason)} before we could register. Re-attempting."
      end)

      do_request(client, request, deadline)
    end
  end

  defp start_monitor(request_id, request, client, :infinity, requester_pid) do
    DynamicSupervisor.start_child(
      Electric.Client.RequestSupervisor,
      {Electric.Client.Fetch.Monitor, {request_id, request, client, :infinity, requester_pid}}
    )
    |> return_existing()
  end

  defp start_monitor(request_id, request, client, deadline, requester_pid) do
    case remaining_ms(deadline) do
      0 ->
        {:error, @request_timeout}

      remaining_ms ->
        task =
          Task.async(fn ->
            try do
              {:result,
               DynamicSupervisor.start_child(
                 Electric.Client.RequestSupervisor,
                 {Electric.Client.Fetch.Monitor,
                  {request_id, request, client, deadline, requester_pid}}
               )}
            catch
              :exit, reason -> {:exit, reason}
            end
          end)

        case Task.yield(task, remaining_ms) do
          {:ok, {:result, result}} ->
            return_existing(result)

          {:ok, {:exit, reason}} ->
            {:retry, reason}

          nil ->
            Task.shutdown(task, :brutal_kill)
            {:error, @request_timeout}
        end
    end
  end

  defp return_existing({:ok, pid}), do: {:ok, pid}
  defp return_existing({:error, {:already_started, pid}}), do: {:ok, pid}
  defp return_existing(error), do: error

  defp request_timeout!(opts) do
    case Keyword.get(opts, :timeout, :infinity) do
      :infinity ->
        :infinity

      timeout when is_integer(timeout) and timeout >= 0 ->
        timeout

      timeout ->
        raise ArgumentError,
              "expected :timeout to be a non-negative integer or :infinity, got: #{inspect(timeout)}"
    end
  end

  defp deadline_after(:infinity), do: :infinity

  defp deadline_after(timeout) do
    System.monotonic_time(:millisecond) + timeout
  end

  defp remaining_ms(deadline) do
    max(deadline - System.monotonic_time(:millisecond), 0)
  end

  defp deadline_expired?(:infinity), do: false
  defp deadline_expired?(deadline), do: remaining_ms(deadline) == 0

  defp request_id(%Client{fetch: {fetch_impl, _}}, %Fetch.Request{} = request) do
    {
      fetch_impl,
      URI.to_string(request.endpoint),
      request.headers,
      Fetch.Request.params(request)
    }
  end
end
