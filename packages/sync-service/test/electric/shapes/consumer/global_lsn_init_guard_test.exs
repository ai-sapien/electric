defmodule Electric.Shapes.Consumer.GlobalLsnInitGuardTest do
  # Regression coverage for the 2026-07-17 production incident (SAP-8006):
  # a `{:global_last_seen_lsn, lsn}` broadcast delivered while a consumer was
  # still initializing (event_handler not yet built) crashed the consumer with
  # `{:badmap, nil}`. Under post-restart load — hundreds of shapes recreating
  # at once — the crash/invalidate/recreate cycle stormed for ~40 minutes.
  use ExUnit.Case, async: true

  alias Electric.Shapes.Consumer
  alias Electric.Shapes.Consumer.State
  alias Electric.Shapes.Shape

  import Support.ComponentSetup, only: [with_stack_id_from_test: 1]

  setup [:with_stack_id_from_test]

  defp uninitialized_state(stack_id) do
    shape = %Shape{root_table: {"public", "items"}, root_table_id: 1}
    State.new(stack_id, "test-handle", shape)
  end

  test "stashes a global LSN broadcast while initialization is pending",
       %{stack_id: stack_id} do
    state = %{uninitialized_state(stack_id) | pending_initialization: {:create, nil}}
    assert is_nil(state.event_handler)

    assert {:noreply, state, _next} = Consumer.handle_info({:global_last_seen_lsn, 42}, state)

    assert state.pending_global_last_seen_lsn == 42
    assert state.last_observed_global_lsn == 42
  end

  test "keeps the newest LSN when broadcasts stack up during initialization",
       %{stack_id: stack_id} do
    state = %{uninitialized_state(stack_id) | pending_initialization: {:create, nil}}

    assert {:noreply, state, _next} = Consumer.handle_info({:global_last_seen_lsn, 42}, state)
    assert {:noreply, state, _next} = Consumer.handle_info({:global_last_seen_lsn, 41}, state)
    assert state.pending_global_last_seen_lsn == 42

    assert {:noreply, state, _next} = Consumer.handle_info({:global_last_seen_lsn, 50}, state)
    assert state.pending_global_last_seen_lsn == 50
  end

  test "keeps a stashed LSN parked when a drain fires mid-initialization",
       %{stack_id: stack_id} do
    state = %{
      uninitialized_state(stack_id)
      | pending_initialization: {:create, nil},
        pending_global_last_seen_lsn: 42
    }

    assert {:noreply, state, next} =
             Consumer.handle_continue(:process_pending_global_lsn, state)

    assert state.pending_global_last_seen_lsn == 42
    refute next == {:continue, :process_pending_global_lsn}
  end

  test "an event reaching a missing handler invalidates the shape instead of crashing",
       %{stack_id: stack_id} do
    # No pending_initialization and no handler models the incident's exact
    # crash window; the consumer must clean itself up as one shape, not die
    # with {:badmap, nil} and take its dependents through an opaque cascade.
    state = uninitialized_state(stack_id)
    assert is_nil(state.event_handler)
    assert is_nil(state.pending_initialization)

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert {:noreply, _state, {:continue, :stop_and_clean}} =
                 Consumer.handle_info({:global_last_seen_lsn, 42}, state)
      end)

    assert log =~ "event_before_initialization"
  end
end
