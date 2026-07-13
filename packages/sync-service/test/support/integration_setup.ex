defmodule Support.IntegrationSetup do
  @moduledoc """
  Helper functions for setting up integration tests that need an HTTP server.
  """

  import Support.ComponentSetup, only: [build_router_opts: 2]

  @doc """
  Starts a Bandit HTTP server and creates an Electric.Client.

  Returns a map with:
  - `client` - Electric.Client configured to connect to the server
  - `base_url` - The base URL of the server
  - `server_pid` - The Bandit server process
  - `port` - The port the server is listening on
  """
  def with_electric_client(ctx, opts \\ []) do
    :ok = Electric.StatusMonitor.wait_until_active(ctx.stack_id, timeout: 2000)

    router_opts = build_router_opts(ctx, Keyword.get(opts, :router_opts, []))
    num_clients = Keyword.get(opts, :num_clients, 1)
    fetch_timeout = Keyword.get(opts, :fetch_timeout)

    {:ok, server_pid} =
      ExUnit.Callbacks.start_supervised(
        {Bandit,
         plug: {Electric.Plug.Router, router_opts},
         port: 0,
         ip: :loopback,
         thousand_island_options: [num_acceptors: 1]}
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(server_pid)
    base_url = "http://localhost:#{port}"

    fetch_opts =
      if num_clients > 1 do
        finch_name = :"Electric.Client.Finch.Test.#{System.unique_integer([:positive])}"

        {:ok, _} =
          ExUnit.Callbacks.start_supervised(
            {Finch, name: finch_name, pools: %{default: [size: num_clients]}}
          )

        [request: [finch: finch_name]]
      else
        []
      end

    fetch_opts =
      if is_nil(fetch_timeout),
        do: fetch_opts,
        else: Keyword.put(fetch_opts, :timeout, fetch_timeout)

    client_opts =
      if fetch_opts == [], do: [], else: [fetch: {Electric.Client.Fetch.HTTP, fetch_opts}]

    {:ok, client} = Electric.Client.new([base_url: base_url] ++ client_opts)

    Map.merge(ctx, %{
      client: client,
      base_url: base_url,
      server_pid: server_pid,
      port: port
    })
  end
end
