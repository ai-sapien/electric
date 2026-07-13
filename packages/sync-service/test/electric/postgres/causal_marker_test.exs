defmodule Electric.Postgres.CausalMarkerTest do
  use ExUnit.Case, async: false

  alias Electric.Postgres.CausalMarker
  alias Electric.Postgres.LogicalReplication.Decoder
  alias Electric.Postgres.LogicalReplication.Messages, as: LR
  alias Electric.Postgres.Lsn

  import Support.ComponentSetup, only: [with_slot_name: 1]
  import Support.DbSetup, only: [with_publication: 1, with_unique_db: 1]

  test "uses the PostgreSQL 14-compatible three-argument emit function" do
    assert CausalMarker.emit_query() =~
             "pg_logical_emit_message(true, '#{CausalMarker.prefix()}', '')"

    refute CausalMarker.emit_query() =~ ", true)"

    assert CausalMarker.snapshot_query() =~ "pg_current_snapshot()"

    assert CausalMarker.snapshot_query() =~
             "pg_logical_emit_message(true, '#{CausalMarker.prefix()}', '')"
  end

  test "decodes only the exact tiny pgoutput wire record" do
    lsn = Lsn.from_integer(42)
    encoded_lsn = Lsn.encode_bin(lsn)
    prefix = CausalMarker.prefix()

    assert {:ok, ^lsn} =
             CausalMarker.decode_wire(<<?M, 1, encoded_lsn::binary, prefix::binary, 0, 0::32>>)

    assert :not_marker =
             CausalMarker.decode_wire(<<?M, 0, encoded_lsn::binary, prefix::binary, 0, 0::32>>)

    assert :not_marker =
             CausalMarker.decode_wire(
               <<?M, 1, encoded_lsn::binary, prefix::binary, 0, 1::32, "x">>
             )
  end

  describe "PostgreSQL transaction boundary" do
    setup [:with_unique_db, :with_publication, :with_slot_name]

    test "a committed marker is B/M/C and a rolled-back marker emits nothing", ctx do
      with_logical_slot(ctx, fn ->
        assert {:ok, marker_lsn} =
                 Postgrex.transaction(ctx.db_conn, fn conn ->
                   Postgrex.query!(
                     conn,
                     "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
                     []
                   )

                   %Postgrex.Result{rows: [[_snapshot, marker_lsn]]} =
                     Postgrex.query!(conn, CausalMarker.snapshot_query(), [])

                   marker_lsn
                 end)

        assert [begin_wire, marker_wire, commit_wire] = slot_changes(ctx)
        assert %LR.Begin{final_lsn: ^marker_lsn} = Decoder.decode(begin_wire)
        assert {:ok, ^marker_lsn} = CausalMarker.decode_wire(marker_wire)
        assert %LR.Commit{lsn: ^marker_lsn, end_lsn: end_lsn} = Decoder.decode(commit_wire)
        assert Lsn.to_integer(end_lsn) > Lsn.to_integer(marker_lsn)

        test_pid = self()

        assert {:error, :deliberate_rollback} =
                 Postgrex.transaction(ctx.db_conn, fn conn ->
                   Postgrex.query!(
                     conn,
                     "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
                     []
                   )

                   %Postgrex.Result{rows: [[_snapshot, rolled_back_lsn]]} =
                     Postgrex.query!(conn, CausalMarker.snapshot_query(), [])

                   send(test_pid, {:rolled_back_marker_lsn, rolled_back_lsn})
                   Postgrex.rollback(conn, :deliberate_rollback)
                 end)

        assert_receive {:rolled_back_marker_lsn, %Lsn{}}
        assert slot_changes(ctx) == []
      end)
    end
  end

  defp with_logical_slot(ctx, fun) do
    Postgrex.query!(
      ctx.db_conn,
      "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
      [ctx.slot_name]
    )

    try do
      fun.()
    after
      Postgrex.query!(ctx.db_conn, "SELECT pg_drop_replication_slot($1)", [ctx.slot_name])
    end
  end

  defp slot_changes(ctx) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        ctx.db_conn,
        """
        SELECT data
        FROM pg_logical_slot_get_binary_changes(
          $1, NULL, NULL,
          'proto_version', '1',
          'publication_names', $2,
          'messages', 'true'
        )
        """,
        [ctx.slot_name, ctx.publication_name]
      )

    Enum.map(rows, fn [wire] -> wire end)
  end
end
