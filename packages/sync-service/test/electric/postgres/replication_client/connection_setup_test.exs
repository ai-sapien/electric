defmodule Electric.Postgres.ReplicationClient.ConnectionSetupTest do
  use ExUnit.Case, async: true

  alias Electric.Postgres.ReplicationClient.ConnectionSetup
  alias Electric.Postgres.ReplicationClient.State
  alias Electric.Postgres.CausalMarker
  alias Electric.Postgres.Lsn

  defp base_state(overrides) do
    Map.merge(
      %State{
        handle_event: fn _, _ -> :ok end,
        publication_name: "test_pub",
        slot_name: "test_slot",
        display_settings: ["SET dummy = 'test'"],
        flushed_wal: 0
      },
      overrides
    )
  end

  describe "process_query_result/2 returns updated state" do
    test "identify_system captures the startup WAL catch-up target" do
      startup_lsn = "0/1A2B3C4"
      expected_wal = startup_lsn |> Lsn.from_string() |> Lsn.to_integer()

      state = base_state(%{step: :identify_system})

      identify_result = [
        %Postgrex.Result{
          command: :identify,
          columns: ["systemid", "timeline", "xlogpos", "dbname"],
          rows: [["1234", "1", startup_lsn, "postgres"]],
          num_rows: 1
        }
      ]

      {_step, _next_step, _extra_info, updated_state, _return_val} =
        ConnectionSetup.process_query_result(identify_result, state)

      assert updated_state.startup_wal_flush_lsn == expected_wal
      refute updated_state.replication_caught_up?
    end

    test "create_slot result includes updated flushed_wal in returned state" do
      slot_lsn = "0/1A2B3C4"
      expected_wal = slot_lsn |> Lsn.from_string() |> Lsn.to_integer()

      state = base_state(%{step: :create_slot})

      create_result = [
        %Postgrex.Result{
          command: :create,
          columns: ["slot_name", "consistent_point", "snapshot_name", "output_plugin"],
          rows: [["test_slot", slot_lsn, nil, "pgoutput"]],
          num_rows: 1
        }
      ]

      {_step, _next_step, :created_new_slot, updated_state, _return_val} =
        ConnectionSetup.process_query_result(create_result, state)

      assert updated_state.flushed_wal == expected_wal
    end

    test "query_slot_flushed_lsn result includes updated flushed_wal in returned state" do
      slot_lsn = "0/5D6E7F8"
      expected_wal = slot_lsn |> Lsn.from_string() |> Lsn.to_integer()

      state = base_state(%{step: :query_slot_flushed_lsn, flushed_wal: 0})

      query_result = [
        %Postgrex.Result{
          command: :select,
          columns: ["confirmed_flush_lsn"],
          rows: [[slot_lsn]],
          num_rows: 1
        }
      ]

      {_step, _next_step, _extra_info, updated_state, _return_val} =
        ConnectionSetup.process_query_result(query_result, state)

      assert updated_state.flushed_wal == expected_wal
    end
  end

  describe "start_streaming/1" do
    test "emits a logical marker as the startup target immediately before replication" do
      initial_lsn = "0/100"
      refreshed_lsn = "0/200"

      state =
        base_state(%{
          step: :ready_to_stream,
          startup_wal_flush_lsn: initial_lsn |> Lsn.from_string() |> Lsn.to_integer(),
          pending_causal_marker_lsn: 1,
          pending_causal_marker_xid: 2,
          event_causal_marker_lsn: 3,
          event_causal_marker_xid: 4,
          last_processed_causal_marker_lsn: 5
        })

      assert {:query, refresh_query, refresh_state} =
               ConnectionSetup.start_streaming(state)

      assert refresh_query == CausalMarker.emit_query()
      assert refresh_state.step == :refresh_wal_target

      refresh_result = [
        %Postgrex.Result{
          command: :select,
          columns: ["pg_logical_emit_message"],
          rows: [[refreshed_lsn]],
          num_rows: 1
        }
      ]

      assert {:refresh_wal_target, :start_streaming, nil, updated_state,
              {:stream, start_replication, [], returned_state}} =
               ConnectionSetup.process_query_result(refresh_result, refresh_state)

      expected_wal = refreshed_lsn |> Lsn.from_string() |> Lsn.to_integer()
      assert updated_state.startup_wal_flush_lsn == expected_wal
      assert updated_state.pending_causal_marker_lsn == nil
      assert updated_state.pending_causal_marker_xid == nil
      assert updated_state.event_causal_marker_lsn == nil
      assert updated_state.event_causal_marker_xid == nil
      assert updated_state.last_processed_causal_marker_lsn == 0
      assert returned_state.startup_wal_flush_lsn == expected_wal
      assert returned_state.step == :start_streaming
      assert start_replication =~ "START_REPLICATION SLOT"
      assert start_replication =~ "messages 'true'"
    end

    test "automatic startup also refreshes the WAL target after setup work" do
      state =
        base_state(%{
          step: :set_display_setting,
          display_settings: [],
          start_streaming?: true
        })

      result = [%Postgrex.Result{command: :set, rows: [], num_rows: 0}]

      assert {:set_display_setting, :refresh_wal_target, nil, _updated_state,
              {:query, refresh_query, refresh_state}} =
               ConnectionSetup.process_query_result(result, state)

      assert refresh_query == CausalMarker.emit_query()
      assert refresh_state.step == :refresh_wal_target
    end
  end
end
