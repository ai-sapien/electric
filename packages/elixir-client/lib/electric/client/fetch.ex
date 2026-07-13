defmodule Electric.Client.Fetch do
  alias Electric.Client.Fetch.{Request, Response}
  alias Electric.Client

  @callback validate_opts(keyword()) :: {:ok, keyword()} | {:error, term()}
  @callback fetch(Request.t(), keyword()) :: {:ok, Response.t()} | {:error, Response.t() | term()}

  @behaviour Electric.Client.Fetch.Pool

  @spec request(Client.t(), Request.t(), keyword()) ::
          Response.t() | {:error, Response.t() | term()}
  def request(client, request, opts \\ [])

  @impl Electric.Client.Fetch.Pool
  def request(%Client{} = client, %Request{} = request, request_opts) do
    %{pool: {module, pool_opts}} = client
    apply(module, :request, [client, request, Keyword.merge(pool_opts, request_opts)])
  end
end
