defmodule Electric.Postgres.CausalMarker do
  @moduledoc false

  alias Electric.Postgres.Lsn
  alias Electric.Utils

  @prefix "electric.causal-frontier.v1"

  @spec prefix() :: String.t()
  def prefix, do: @prefix

  @spec emit_query() :: String.t()
  def emit_query do
    "SELECT pg_logical_emit_message(true, #{Utils.quote_string(@prefix)}, '')::text"
  end

  @spec snapshot_query() :: String.t()
  def snapshot_query do
    "SELECT pg_current_snapshot(), " <>
      "pg_logical_emit_message(true, #{Utils.quote_string(@prefix)}, '')"
  end

  @spec decode_wire(binary()) :: {:ok, Lsn.t()} | :not_marker
  def decode_wire(<<?M, 1, encoded_lsn::binary-size(8), rest::binary>>) do
    if rest == <<@prefix::binary, 0, 0::32>>,
      do: {:ok, Lsn.decode_bin(encoded_lsn)},
      else: :not_marker
  end

  def decode_wire(_message), do: :not_marker
end
