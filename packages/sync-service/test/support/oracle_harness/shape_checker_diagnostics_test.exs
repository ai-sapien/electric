defmodule Support.OracleHarness.ShapeCheckerDiagnosticsTest do
  use ExUnit.Case, async: false
  use Repatch.ExUnit

  alias Support.OracleHarness.ShapeChecker

  test "shape mismatch diagnostics cap graph cardinality and never copy large process state" do
    marker = "large-state-marker-#{System.unique_integer([:positive])}"

    {:ok, large_state_pid} =
      Agent.start_link(fn ->
        %{
          marker =>
            for index <- 1..100_000, into: %{} do
              {index, String.duplicate("x", 32)}
            end
        }
      end)

    test_pid = self()

    Repatch.patch(
      Electric.ShapeCache.ShapeStatus,
      :fetch_shape_by_handle,
      [mode: :shared],
      fn _stack_id, handle ->
        send(test_pid, {:diagnostic_shape_fetched, handle})

        dependencies =
          for index <- 1..128, into: MapSet.new() do
            "#{handle}/#{index}"
          end

        {:ok, %{root_table: "items", shape_dependencies_handles: dependencies}}
      end
    )

    Repatch.patch(
      Electric.Shapes.Consumer,
      :whereis,
      [mode: :shared],
      fn _stack_id, _handle -> large_state_pid end
    )

    Repatch.patch(
      Electric.Shapes.Consumer.Materializer,
      :whereis,
      [mode: :shared],
      fn _stack_id, _handle -> large_state_pid end
    )

    :sys.suspend(large_state_pid)

    diagnostics =
      try do
        ShapeChecker.bounded_shape_tree_diagnostics(%{
          stack_id: "diagnostic-stack",
          poll_state: %{shape_handle: "root"}
        })
      after
        :sys.resume(large_state_pid)
      end

    fetched_handles = drain_fetched_handles([])
    rendered = inspect(diagnostics, pretty: true, limit: :infinity)

    refute Map.get(diagnostics, :diagnostic_timeout?, false)
    assert diagnostic_node_count(diagnostics) <= diagnostics.diagnostic_limits.max_nodes
    assert length(fetched_handles) <= diagnostics.diagnostic_limits.max_nodes
    assert diagnostics.truncated?
    assert byte_size(rendered) < 100_000
    refute rendered =~ marker
  end

  test "shape mismatch diagnostics enforce one total wall-time budget" do
    Repatch.patch(
      Electric.ShapeCache.ShapeStatus,
      :fetch_shape_by_handle,
      [mode: :shared],
      fn _stack_id, _handle ->
        Process.sleep(5_000)
        :error
      end
    )

    started_at = System.monotonic_time(:millisecond)

    diagnostics =
      ShapeChecker.bounded_shape_tree_diagnostics(%{
        stack_id: "diagnostic-stack",
        poll_state: %{shape_handle: "root"}
      })

    elapsed_ms = System.monotonic_time(:millisecond) - started_at

    assert diagnostics.diagnostic_timeout?
    assert elapsed_ms < 1_000
  end

  test "large row mismatches produce bounded counts, samples, and assertion output" do
    row_count = 10_000
    state = %{pk: ["id"], columns: ["id", "value"]}

    oracle_rows =
      for id <- 1..row_count do
        %{"id" => Integer.to_string(id), "value" => "expected-#{id}"}
      end

    materialized_rows =
      for id <- 1..row_count do
        %{"id" => Integer.to_string(id), "value" => "actual-#{id}"}
      end

    started_at = System.monotonic_time(:millisecond)

    summary =
      ShapeChecker.bounded_row_mismatch_summary(state, materialized_rows, oracle_rows)

    error =
      assert_raise ExUnit.AssertionError, fn ->
        ShapeChecker.raise_bounded_row_mismatch!(state, "large_diff", true, summary)
      end

    elapsed_ms = System.monotonic_time(:millisecond) - started_at

    assert summary.materialized_row_count == row_count
    assert summary.oracle_row_count == row_count
    assert summary.missing_row_count == row_count
    assert summary.unexpected_row_count == row_count
    assert length(summary.missing_rows_sample) == summary.sample_limit
    assert length(summary.unexpected_rows_sample) == summary.sample_limit
    assert byte_size(Exception.message(error)) < 20_000
    assert elapsed_ms < 2_000
  end

  test "shape lookup failures are marked truncated while an ordinary missing shape is complete" do
    Repatch.patch(
      Electric.ShapeCache.ShapeStatus,
      :fetch_shape_by_handle,
      [mode: :shared],
      fn _stack_id, handle ->
        case handle do
          "raised" -> raise "shape lookup raised"
          "exited" -> exit(:shape_lookup_exited)
          "missing" -> :error
        end
      end
    )

    Repatch.patch(
      Electric.Shapes.Consumer,
      :whereis,
      [mode: :shared],
      fn _stack_id, _handle -> nil end
    )

    Repatch.patch(
      Electric.Shapes.Consumer.Materializer,
      :whereis,
      [mode: :shared],
      fn _stack_id, _handle -> nil end
    )

    raised = bounded_diagnostics_for("raised")
    exited = bounded_diagnostics_for("exited")
    missing = bounded_diagnostics_for("missing")

    assert raised.truncated?
    assert raised.shape_error =~ "shape lookup raised"
    assert exited.truncated?
    assert exited.shape_error =~ "shape_lookup_exited"
    refute missing.truncated?
    refute Map.has_key?(missing, :shape_error)
  end

  defp drain_fetched_handles(handles) do
    receive do
      {:diagnostic_shape_fetched, handle} -> drain_fetched_handles([handle | handles])
    after
      0 -> Enum.reverse(handles)
    end
  end

  defp diagnostic_node_count(%{shape_handle: _handle, dependencies: dependencies}) do
    1 + Enum.sum(Enum.map(dependencies, &diagnostic_node_count/1))
  end

  defp diagnostic_node_count(%{shape_handle: _handle}), do: 1

  defp bounded_diagnostics_for(handle) do
    ShapeChecker.bounded_shape_tree_diagnostics(%{
      stack_id: "diagnostic-stack",
      poll_state: %{shape_handle: handle}
    })
  end
end
