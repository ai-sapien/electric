defmodule Electric.Client.Fetch.PoolTest do
  use ExUnit.Case, async: false

  alias Electric.Client
  alias Electric.Client.Fetch
  alias Electric.Client.Fetch.Monitor
  alias Electric.Client.Fetch.Pool
  alias Electric.Client.ShapeDefinition
  alias Electric.Client.ShapeState

  @request_timeout {Pool, :request_timeout}

  defmodule BlockingFetch do
    @behaviour Electric.Client.Fetch

    @impl true
    def validate_opts(opts), do: {:ok, opts}

    @impl true
    def fetch(_request, opts) do
      test_pid = Keyword.fetch!(opts, :test_pid)
      send(test_pid, {:fetch_started, self()})

      receive do
        {:release, result} -> result
      end
    end
  end

  defmodule RecordingPool do
    @behaviour Electric.Client.Fetch.Pool

    @impl true
    def request(_client, _request, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:pool_opts, opts})
      {:error, :recorded}
    end
  end

  test "poll only overrides configured pool options when a timeout is explicit" do
    marker = make_ref()
    client = client(self())

    client = %{
      client
      | pool: {RecordingPool, test_pid: self(), marker: marker}
    }

    assert {:error, %Client.Error{resp: :recorded}} =
             Client.poll(client, shape(), ShapeState.new())

    assert_receive {:pool_opts, default_opts}
    assert default_opts[:marker] == marker
    refute Keyword.has_key?(default_opts, :timeout)

    client = %{
      client
      | pool: {RecordingPool, test_pid: self(), marker: marker, timeout: 1_000}
    }

    assert {:error, %Client.Error{resp: :recorded}} =
             Client.poll(client, shape(), ShapeState.new())

    assert_receive {:pool_opts, configured_opts}
    assert configured_opts[:timeout] == 1_000

    assert {:error, %Client.Error{resp: :recorded}} =
             Client.poll(client, shape(), ShapeState.new(), timeout: 25)

    assert_receive {:pool_opts, explicit_opts}
    assert explicit_opts[:marker] == marker
    assert explicit_opts[:timeout] == 25
  end

  test "a poll timeout cancels the request when it is the last subscriber" do
    client = client(self())
    started_at = System.monotonic_time(:millisecond)

    task =
      Task.async(fn ->
        Client.poll(client, shape(), ShapeState.new(), timeout: 50)
      end)

    assert_receive {:fetch_started, request_pid}
    request_ref = Process.monitor(request_pid)

    assert {:ok, {:error, %Client.Error{resp: @request_timeout}}} = Task.yield(task, 500)
    assert System.monotonic_time(:millisecond) - started_at < 500
    assert_receive {:DOWN, ^request_ref, :process, ^request_pid, _reason}, 500
  end

  test "one subscriber timing out does not cancel a request shared with another subscriber" do
    client = client(self())

    long_poll =
      Task.async(fn ->
        Client.poll(client, shape(), ShapeState.new(), timeout: 1_000)
      end)

    assert_receive {:fetch_started, request_pid}
    request_ref = Process.monitor(request_pid)
    [monitor_pid] = Process.info(request_pid, :links) |> elem(1)

    short_poll =
      Task.async(fn ->
        Client.poll(client, shape(), ShapeState.new(), timeout: 100)
      end)

    assert_eventually(fn -> subscriber_count(monitor_pid) == 2 end)

    assert {:ok, {:error, %Client.Error{resp: @request_timeout}}} =
             Task.yield(short_poll, 500)

    assert Process.alive?(request_pid)
    refute_receive {:DOWN, ^request_ref, :process, ^request_pid, _reason}, 50

    send(request_pid, {:release, {:error, :released}})

    assert {:ok, {:error, %Client.Error{resp: :released}}} = Task.yield(long_poll, 500)
    assert_receive {:DOWN, ^request_ref, :process, ^request_pid, _reason}, 500
  end

  test "the last subscriber exiting cancels an infinite poll" do
    client = client(self())

    task =
      Task.async(fn ->
        Client.poll(client, shape(), ShapeState.new(), timeout: :infinity)
      end)

    assert_receive {:fetch_started, request_pid}
    request_ref = Process.monitor(request_pid)

    assert Task.shutdown(task, :brutal_kill) == nil
    assert_receive {:DOWN, ^request_ref, :process, ^request_pid, _reason}, 500
  end

  test "an invalid timeout is rejected before a request starts" do
    client = client(self())

    assert_raise ArgumentError, ~r/expected :timeout/, fn ->
      Client.poll(client, shape(), ShapeState.new(), timeout: -1)
    end

    refute_receive {:fetch_started, _request_pid}
  end

  test "the request deadline includes monitor startup backlog and leaves no orphan monitor" do
    client = client(self())
    request = Client.request(Client.for_shape(client, shape()), [])
    request_id = request_id(client, request)
    supervisor = Process.whereis(Electric.Client.RequestSupervisor)

    on_exit(fn ->
      case GenServer.whereis(Monitor.name(request_id)) do
        pid when is_pid(pid) -> DynamicSupervisor.terminate_child(supervisor, pid)
        nil -> :ok
      end
    end)

    :sys.suspend(supervisor)

    task =
      Task.async(fn ->
        Client.poll(client, shape(), ShapeState.new(), timeout: 50)
      end)

    result =
      try do
        result = Task.yield(task, 500)

        if is_nil(result) do
          Task.shutdown(task, :brutal_kill)
        end

        result
      after
        :sys.resume(supervisor)
      end

    assert {:ok, {:error, %Client.Error{resp: @request_timeout}}} = result
    assert_eventually(fn -> is_nil(GenServer.whereis(Monitor.name(request_id))) end)
    refute_receive {:fetch_started, _request_pid}
  end

  test "a timed-out registration removes its caller monitor" do
    client = client(self())
    request = Client.request(client, [])
    request_id = make_ref()

    {:ok, monitor_pid} = Monitor.start_link({request_id, request, client})
    Process.unlink(monitor_pid)

    on_exit(fn ->
      if Process.alive?(monitor_pid), do: Process.exit(monitor_pid, :kill)
    end)

    :sys.suspend(monitor_pid)

    started_at = System.monotonic_time(:millisecond)

    assert catch_exit(
             try do
               Monitor.register(monitor_pid, self(), 25)
             after
               :sys.resume(monitor_pid)
             end
           )

    elapsed_ms = System.monotonic_time(:millisecond) - started_at

    assert_eventually(fn -> not Process.alive?(monitor_pid) end)

    stale_down? =
      receive do
        {:DOWN, _ref, :process, ^monitor_pid, _reason} -> true
      after
        100 -> false
      end

    refute stale_down?
    refute_receive {:fetch_started, _request_pid}
    assert elapsed_ms < 500
  end

  test "the caller deadline expires while a registered monitor is backlogged" do
    client = client(self())

    task =
      Task.async(fn ->
        Client.poll(client, shape(), ShapeState.new(), timeout: 100)
      end)

    assert_receive {:fetch_started, request_pid}
    request_ref = Process.monitor(request_pid)
    [monitor_pid] = Process.info(request_pid, :links) |> elem(1)
    :sys.suspend(monitor_pid)

    result =
      try do
        Task.yield(task, 500)
      after
        :sys.resume(monitor_pid)
      end

    if is_nil(result), do: Task.shutdown(task, :brutal_kill)

    assert {:ok, {:error, %Client.Error{resp: @request_timeout}}} = result
    assert_receive {:DOWN, ^request_ref, :process, ^request_pid, _reason}, 500
  end

  test "an expired late registration does not cancel an existing subscriber" do
    client = client(self())

    long_poll =
      Task.async(fn ->
        try do
          Client.poll(client, shape(), ShapeState.new(), timeout: 1_000)
        rescue
          error -> {:raised, error}
        end
      end)

    assert_receive {:fetch_started, request_pid}
    request_ref = Process.monitor(request_pid)
    [monitor_pid] = Process.info(request_pid, :links) |> elem(1)
    :sys.suspend(monitor_pid)

    short_poll =
      Task.async(fn ->
        Client.poll(client, shape(), ShapeState.new(), timeout: 50)
      end)

    short_result =
      try do
        Task.yield(short_poll, 500)
      after
        :sys.resume(monitor_pid)
      end

    assert {:ok, {:error, %Client.Error{resp: @request_timeout}}} = short_result
    assert Process.alive?(request_pid)

    Process.sleep(25)
    assert Process.alive?(request_pid)
    refute_receive {:DOWN, ^request_ref, :process, ^request_pid, _reason}

    send(request_pid, {:release, {:error, :released}})

    assert {:ok, {:error, %Client.Error{resp: :released}}} = Task.yield(long_poll, 500)
    assert_receive {:DOWN, ^request_ref, :process, ^request_pid, _reason}, 500
  end

  test "a response queued before timeout cancellation is revoked from the caller mailbox" do
    client = client(self())
    test_pid = self()

    caller =
      spawn(fn ->
        result = Client.poll(client, shape(), ShapeState.new(), timeout: 1_000)
        send(test_pid, {:poll_result, self(), result})

        receive do
          :inspect_mailbox ->
            {:messages, messages} = Process.info(self(), :messages)
            send(test_pid, {:caller_messages, self(), messages})
        end
      end)

    on_exit(fn ->
      if Process.alive?(caller), do: Process.exit(caller, :kill)
    end)

    assert_receive {:fetch_started, request_pid}, 500
    [monitor_pid] = Process.info(request_pid, :links) |> elem(1)
    :sys.suspend(monitor_pid)

    large_response = :binary.copy("response", 128_000)
    send(request_pid, {:release, {:error, large_response}})

    poll_result =
      try do
        assert_eventually(fn ->
          Process.info(monitor_pid, :message_queue_len) == {:message_queue_len, 1}
        end)

        receive do
          {:poll_result, ^caller, result} -> result
        after
          1_500 -> :missing_poll_result
        end
      after
        :sys.resume(monitor_pid)
      end

    assert {:error, %Client.Error{resp: @request_timeout}} = poll_result
    assert_eventually(fn -> not Process.alive?(monitor_pid) end)

    send(caller, :inspect_mailbox)
    assert_receive {:caller_messages, ^caller, []}
  end

  test "an infinite request leaves no startup monitor when its caller dies in supervisor backlog" do
    client = client(self())
    request = Client.request(Client.for_shape(client, shape()), [])
    request_id = request_id(client, request)
    supervisor = Process.whereis(Electric.Client.RequestSupervisor)

    on_exit(fn ->
      case GenServer.whereis(Monitor.name(request_id)) do
        pid when is_pid(pid) -> DynamicSupervisor.terminate_child(supervisor, pid)
        nil -> :ok
      end
    end)

    :sys.suspend(supervisor)

    caller =
      spawn(fn ->
        Client.poll(client, shape(), ShapeState.new(), timeout: :infinity)
      end)

    caller_ref = Process.monitor(caller)

    try do
      assert_eventually(fn ->
        Process.info(supervisor, :message_queue_len) != {:message_queue_len, 0}
      end)

      Process.exit(caller, :kill)
      assert_receive {:DOWN, ^caller_ref, :process, ^caller, :killed}
    after
      :sys.resume(supervisor)
    end

    :sys.get_state(supervisor)

    assert_eventually(fn -> is_nil(GenServer.whereis(Monitor.name(request_id))) end)
    refute_receive {:fetch_started, _request_pid}
  end

  test "a timeout returned by the fetcher is not classified as a pool deadline" do
    client = client(self())

    task =
      Task.async(fn ->
        Client.poll(client, shape(), ShapeState.new(), timeout: 1_000)
      end)

    assert_receive {:fetch_started, request_pid}
    send(request_pid, {:release, {:error, :timeout}})

    assert {:ok, {:error, %Client.Error{resp: :timeout}}} = Task.yield(task, 500)
  end

  defp client(test_pid) do
    unique_id = System.unique_integer([:positive])

    Client.new!(
      base_url: "http://pool-test-#{unique_id}.invalid",
      fetch: {BlockingFetch, test_pid: test_pid}
    )
  end

  defp shape, do: ShapeDefinition.new!("items")

  defp request_id(%Client{fetch: {fetch_impl, _}}, %Fetch.Request{} = request) do
    {
      fetch_impl,
      URI.to_string(request.endpoint),
      request.headers,
      Fetch.Request.params(request)
    }
  end

  defp subscriber_count(monitor_pid) do
    monitor_pid
    |> :sys.get_state()
    |> Map.fetch!(:subscribers)
    |> length()
  end

  defp assert_eventually(fun, attempts \\ 50)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")
end
