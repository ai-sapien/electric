defmodule Electric.Shapes.Consumer.EffectsTest do
  use ExUnit.Case, async: true
  use Repatch.ExUnit, assert_expectations: true

  alias Electric.Postgres.SnapshotQuery
  alias Electric.Shapes.Consumer.Effects
  alias Electric.Shapes.Shape

  test "a rolled-back move-in transaction reports only an error" do
    Repatch.patch(SnapshotQuery, :execute_for_shape, [mode: :shared], fn
      _pool, "shape", %Shape{}, opts ->
        assert opts[:causal_marker?]
        {:error, :commit_failed}
    end)

    shape = %Shape{root_table: {"public", "items"}, root_table_id: 1}

    assert {:query_move_in_error, error, stacktrace} =
             Effects.execute_move_in_query("stack", "shape", shape, self(), fn _, _, _ ->
               flunk("query function must not be called by the rollback stub")
             end)

    assert %RuntimeError{message: message} = error
    assert message =~ "move-in snapshot transaction failed"
    assert message =~ ":commit_failed"
    assert is_list(stacktrace) and stacktrace != []
    refute_receive {:query_move_in_complete, _, _, _, _}
  end
end
