defmodule Support.OracleHarness.ShapeChecker do
  @moduledoc """
  GenServer that verifies a single shape's consistency with the Postgres oracle.

  Each checker:
  - Maintains its own materialized view from Electric shape changes
  - Queries the oracle (Postgres) via a shared pool
  - Verifies consistency between materialized client state and oracle

  ## Usage

      {:ok, pid} = ShapeChecker.start_link(ctx, shape, oracle_pool, timeout_ms: 10_000)

      # Check initial snapshot matches oracle
      ShapeChecker.check_initial_state(pid)

      # After mutations, check transaction result matches oracle
      ShapeChecker.check_transaction(pid, "txn_1")

  """

  use GenServer

  import ExUnit.Assertions

  alias Electric.Client
  alias Electric.Client.Message.ChangeMessage
  alias Electric.Client.Message.ControlMessage
  alias Electric.Client.ShapeDefinition
  alias Electric.Client.ShapeState

  @diagnostic_max_nodes 32
  @diagnostic_max_depth 6
  @diagnostic_max_dependencies_per_node 8
  @diagnostic_total_timeout_ms 250
  @diagnostic_inspect_limit 256
  @diagnostic_printable_limit 2_048
  @diagnostic_error_max_bytes 512
  @mismatch_sample_limit 5
  @mismatch_max_columns_per_sample 16
  @mismatch_inspect_limit 64
  @mismatch_printable_limit 512

  defstruct [
    :stack_id,
    :stack_supervisor,
    :component_pids,
    :name,
    :table,
    :where,
    :columns,
    :pk,
    :optimized,
    :client,
    :shape_def,
    :oracle_pool,
    :timeout_ms,
    poll_state: nil,
    rows: %{},
    # Cached oracle state from previous check (used as "before" for next check)
    oracle_before: nil
  ]

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Starts a ShapeChecker GenServer.
  """
  def start_link(ctx, shape, oracle_pool, opts \\ []) do
    GenServer.start_link(__MODULE__, {ctx, shape, oracle_pool, opts})
  end

  @doc """
  Checks the initial state: queries oracle, polls shape until up_to_date,
  and verifies they match. Caches oracle state for subsequent transaction checks.

  Raises on mismatch or timeout.
  """
  def check_initial_state(pid) do
    GenServer.call(pid, :check_initial_state, :infinity)
  end

  @doc """
  Checks after a transaction: polls shape until up_to_date, queries oracle,
  and verifies they match. Uses cached "before" state for logging.

  Raises on mismatch or timeout.
  """
  def check_transaction(pid, txn_name) do
    GenServer.call(pid, {:check_transaction, txn_name}, :infinity)
  end

  # ---------------------------------------------------------------------------
  # GenServer Callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init({ctx, shape, oracle_pool, opts}) do
    validate_identifier!(shape.table, "table")
    Enum.each(shape.columns, &validate_identifier!(&1, "column"))
    Enum.each(shape.pk, &validate_identifier!(&1, "pk column"))

    shape_def = ShapeDefinition.new!(shape.table, where: shape.where)
    timeout_ms = Keyword.fetch!(opts, :timeout_ms)

    state = %__MODULE__{
      stack_id: ctx.stack_id,
      stack_supervisor: ctx.stack_supervisor,
      component_pids: component_pids(ctx.stack_id),
      name: shape.name,
      table: shape.table,
      where: shape.where,
      columns: shape.columns,
      pk: shape.pk,
      optimized: Map.get(shape, :optimized, false),
      client: ctx.client,
      shape_def: shape_def,
      oracle_pool: oracle_pool,
      timeout_ms: timeout_ms,
      poll_state: ShapeState.new()
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:check_initial_state, _from, state) do
    log("Checking initial state for shape=#{state.name}")

    # Get oracle state (this becomes our "before" for the first transaction)
    oracle = query_oracle(state)

    # Poll until up_to_date
    state = await_up_to_date(state)

    # Verify consistency
    assert_consistent!(state, "initial_snapshot", oracle, oracle)

    # Cache oracle state for next check
    state = %{state | oracle_before: oracle}

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:check_transaction, txn_name}, _from, state) do
    # Poll until up_to_date
    state = await_up_to_date(state)

    # Get new oracle state
    oracle_after = query_oracle(state)

    # Verify consistency (uses cached oracle_before)
    assert_consistent!(state, txn_name, state.oracle_before, oracle_after)

    # Cache new oracle state for next check
    state = %{state | oracle_before: oracle_after}

    {:reply, :ok, state}
  end

  # ---------------------------------------------------------------------------
  # Polling Logic
  # ---------------------------------------------------------------------------

  defp await_up_to_date(state) do
    started_at_ms = System.monotonic_time(:millisecond)
    deadline_ms = started_at_ms + state.timeout_ms
    do_await(state, started_at_ms, deadline_ms)
  end

  defp do_await(state, started_at_ms, deadline_ms) do
    remaining_ms = deadline_ms - System.monotonic_time(:millisecond)

    if remaining_ms <= 0 do
      convergence_timeout!(state, started_at_ms)
    else
      poll_result =
        Client.poll(state.client, state.shape_def, state.poll_state,
          replica: :full,
          timeout: remaining_ms
        )

      if System.monotonic_time(:millisecond) >= deadline_ms do
        convergence_timeout!(state, started_at_ms)
      end

      case poll_result do
        {:ok, messages, new_state} ->
          state = %{state | poll_state: new_state}
          state = apply_messages(state, messages)

          if new_state.up_to_date? do
            handle_up_to_date(state, started_at_ms, deadline_ms)
          else
            do_await(state, started_at_ms, deadline_ms)
          end

        {:must_refetch, messages, new_state} ->
          if state.optimized do
            diagnostics = must_refetch_diagnostics(state, new_state)

            flunk(
              "Unexpected 409 (must-refetch) in optimized shape=#{state.name} where=#{state.where}; " <>
                "diagnostics=#{inspect(diagnostics, pretty: true, limit: :infinity)}"
            )
          end

          state = %{state | poll_state: new_state, rows: %{}}
          state = apply_messages(state, messages)
          do_await(state, started_at_ms, deadline_ms)

        {:error, %Client.Error{resp: {Electric.Client.Fetch.Pool, :request_timeout}}} ->
          convergence_timeout!(state, started_at_ms)

        {:error, error} ->
          diagnostics = poll_error_diagnostics(state)

          flunk(
            "Poll error for shape=#{state.name} where=#{state.where}: #{inspect(error)}; " <>
              "diagnostics=#{inspect(diagnostics, pretty: true, limit: :infinity)}"
          )
      end
    end
  end

  defp convergence_timeout!(state, started_at_ms) do
    elapsed_ms = System.monotonic_time(:millisecond) - started_at_ms

    diagnostics =
      Map.merge(timeout_diagnostics(state), %{
        poll_state:
          Map.take(state.poll_state, [:shape_handle, :offset, :next_cursor, :up_to_date?]),
        materialized_row_count: map_size(state.rows)
      })

    flunk(
      "Shape convergence timeout for shape=#{state.name} where=#{state.where} " <>
        "after #{elapsed_ms}ms (limit=#{state.timeout_ms}ms); " <>
        "diagnostics=#{inspect(diagnostics, pretty: true, limit: :infinity)}"
    )
  end

  defp must_refetch_diagnostics(state, new_poll_state) do
    handles =
      [state.poll_state.shape_handle, new_poll_state.shape_handle]
      |> Enum.filter(&is_binary/1)
      |> Enum.uniq()

    %{
      old_poll_state:
        Map.take(state.poll_state, [:shape_handle, :offset, :next_cursor, :up_to_date?]),
      new_poll_state:
        Map.take(new_poll_state, [:shape_handle, :offset, :next_cursor, :up_to_date?]),
      handles: Map.new(handles, &{&1, handle_diagnostics(state.stack_id, &1)})
    }
  end

  defp handle_diagnostics(stack_id, handle) do
    consumer = Electric.Shapes.Consumer.whereis(stack_id, handle)

    shape =
      case Electric.ShapeCache.ShapeStatus.fetch_shape_by_handle(stack_id, handle) do
        {:ok, shape} ->
          %{
            root_table: shape.root_table,
            dependency_handles: shape.shape_dependencies_handles
          }

        :error ->
          nil
      end

    %{
      shape_status_present?: Electric.ShapeCache.ShapeStatus.has_shape_handle?(stack_id, handle),
      shape_status_activated?:
        Electric.ShapeCache.ShapeStatus.shape_has_been_activated?(stack_id, handle),
      snapshot_started?: Electric.ShapeCache.ShapeStatus.snapshot_started?(stack_id, handle),
      shape: shape,
      consumer: inspect(consumer),
      consumer_alive?: is_pid(consumer) and Process.alive?(consumer)
    }
  rescue
    error -> %{diagnostic_error: Exception.message(error)}
  catch
    :exit, reason -> %{diagnostic_exit: inspect(reason)}
  end

  defp poll_error_diagnostics(state) do
    current_component_pids = component_pids(state.stack_id)
    consumer_snapshot = Electric.Shapes.ConsumerRegistry.consumer_snapshot(state.stack_id)
    service_status = Electric.StatusMonitor.service_status(state.stack_id)

    %{
      service_status: service_status,
      status: Electric.StatusMonitor.status(state.stack_id),
      timeout_message:
        if(service_status == :active,
          do: nil,
          else: Electric.StatusMonitor.timeout_message(state.stack_id)
        ),
      initial_component_pids: state.component_pids,
      current_component_pids: current_component_pids,
      components_replaced?: current_component_pids != state.component_pids,
      stack_supervisor: pid_diagnostics(state.stack_supervisor),
      consumer_count: map_size(consumer_snapshot),
      current_handle:
        case state.poll_state.shape_handle do
          handle when is_binary(handle) -> handle_diagnostics(state.stack_id, handle)
          nil -> nil
        end
    }
  rescue
    error -> %{diagnostic_error: Exception.message(error)}
  catch
    :exit, reason -> %{diagnostic_exit: inspect(reason)}
  end

  # Timeout reporting runs after the convergence budget has already expired,
  # so it must only use local, non-blocking process and ETS lookups. In
  # particular, the ShapeStatus helpers used by poll_error_diagnostics/1 may
  # reach the shape metadata database and must not extend a 50ms deadline by
  # seconds when that subsystem is unhealthy.
  defp timeout_diagnostics(state) do
    current_component_pids = component_pids(state.stack_id)
    service_status = Electric.StatusMonitor.service_status(state.stack_id)

    %{
      service_status: service_status,
      status: Electric.StatusMonitor.status(state.stack_id),
      timeout_message:
        if(service_status == :active,
          do: nil,
          else: Electric.StatusMonitor.timeout_message(state.stack_id)
        ),
      initial_component_pids: state.component_pids,
      current_component_pids: current_component_pids,
      components_replaced?: current_component_pids != state.component_pids,
      stack_supervisor: pid_diagnostics(state.stack_supervisor),
      consumer_count: Electric.Shapes.ConsumerRegistry.active_consumer_count(state.stack_id),
      current_handle: timeout_handle_diagnostics(state.stack_id, state.poll_state.shape_handle)
    }
  rescue
    error -> %{diagnostic_error: Exception.message(error)}
  catch
    :exit, reason -> %{diagnostic_exit: inspect(reason)}
  end

  defp timeout_handle_diagnostics(stack_id, handle) when is_binary(handle) do
    consumer = Electric.Shapes.Consumer.whereis(stack_id, handle)

    %{
      consumer: inspect(consumer),
      consumer_alive?: is_pid(consumer) and Process.alive?(consumer)
    }
  end

  defp timeout_handle_diagnostics(_stack_id, nil), do: nil

  defp component_pids(stack_id) do
    %{
      status_monitor: named_pid(Electric.StatusMonitor.name(stack_id)),
      shape_log_collector: named_pid(Electric.Replication.ShapeLogCollector.name(stack_id)),
      request_batcher:
        named_pid(Electric.Replication.ShapeLogCollector.RequestBatcher.name(stack_id)),
      shape_cache: named_pid(Electric.ShapeCache.name(stack_id))
    }
  end

  defp named_pid(name), do: name |> GenServer.whereis() |> pid_diagnostics()

  defp pid_diagnostics(pid) when is_pid(pid),
    do: %{pid: inspect(pid), alive?: Process.alive?(pid)}

  defp pid_diagnostics(nil), do: nil

  defp handle_up_to_date(state, started_at_ms, deadline_ms) do
    oracle_rows = query_oracle(state)
    materialized = materialized_rows(state)

    if materialized == oracle_rows do
      state
    else
      # Electric reported up_to_date but data doesn't match yet - keep polling
      do_await(state, started_at_ms, deadline_ms)
    end
  end

  # ---------------------------------------------------------------------------
  # Message Application
  # ---------------------------------------------------------------------------

  defp apply_messages(state, messages) do
    Enum.reduce(messages, state, &apply_message/2)
  end

  defp apply_message(%ChangeMessage{headers: %{operation: :insert}, value: value}, state) do
    key = key_from_value(state.pk, value)

    if Map.has_key?(state.rows, key) do
      flunk("shape=#{state.name}: insert for row that already exists: #{inspect(key)}")
    end

    row = Map.take(value, state.columns)
    %{state | rows: Map.put(state.rows, key, row)}
  end

  defp apply_message(%ChangeMessage{headers: %{operation: :update}, value: value}, state) do
    key = key_from_value(state.pk, value)

    if not Map.has_key?(state.rows, key) do
      flunk("shape=#{state.name}: update for row that does not exist: #{inspect(key)}")
    end

    row = Map.take(value, state.columns)
    %{state | rows: Map.put(state.rows, key, row)}
  end

  defp apply_message(%ChangeMessage{headers: %{operation: :delete}, value: value}, state) do
    key = key_from_value(state.pk, value)

    if not Map.has_key?(state.rows, key) do
      flunk("shape=#{state.name}: delete for row that does not exist: #{inspect(key)}")
    end

    %{state | rows: Map.delete(state.rows, key)}
  end

  defp apply_message(%ControlMessage{}, state), do: state
  defp apply_message(_other, state), do: state

  defp key_from_value(pk, value) do
    pk
    |> Enum.map(&Map.get(value, &1))
    |> List.to_tuple()
  end

  # ---------------------------------------------------------------------------
  # Oracle Queries
  # ---------------------------------------------------------------------------

  defp query_oracle(state) do
    where_sql = state.where || "TRUE"
    columns_sql = Enum.map(state.columns, &quote_ident/1) |> Enum.join(", ")
    order_sql = Enum.map(state.pk, &quote_ident/1) |> Enum.join(", ")

    sql =
      "SELECT #{columns_sql} FROM #{quote_ident(state.table)} WHERE #{where_sql} ORDER BY #{order_sql}"

    %Postgrex.Result{columns: columns, rows: rows} =
      Postgrex.query!(state.oracle_pool, sql, [])

    Enum.map(rows, fn row ->
      columns
      |> Enum.zip(row)
      |> Map.new(fn {col, val} -> {col, to_string_value(val)} end)
    end)
  end

  # ---------------------------------------------------------------------------
  # Assertions
  # ---------------------------------------------------------------------------

  defp assert_consistent!(state, step_name, oracle_before, oracle_after) do
    view_changed? = oracle_before != oracle_after
    materialized = materialized_rows(state)

    if materialized != oracle_after do
      mismatch_summary = bounded_row_mismatch_summary(state, materialized, oracle_after)

      IO.puts(
        "[oracle] View mismatch in step=#{step_name} shape=#{state.name} where=#{state.where} view_changed?=#{view_changed?}"
      )

      IO.puts(
        "[oracle] Shape diagnostics: " <>
          inspect(bounded_shape_tree_diagnostics(state),
            pretty: true,
            limit: @diagnostic_inspect_limit,
            printable_limit: @diagnostic_printable_limit,
            width: 120
          )
      )

      raise_bounded_row_mismatch!(state, step_name, view_changed?, mismatch_summary)
    end

    view_status = if view_changed?, do: "changed", else: "unchanged"
    log("  #{step_name} shape=#{state.name} (#{view_status}) PASS")

    :ok
  end

  defp materialized_rows(state) do
    state.rows
    |> Map.values()
    |> Enum.sort_by(&key_from_value(state.pk, &1))
  end

  @doc false
  def bounded_row_mismatch_summary(state, materialized_rows, oracle_rows)
      when is_list(materialized_rows) and is_list(oracle_rows) do
    accumulator = %{
      materialized_row_count: 0,
      oracle_row_count: 0,
      missing_row_count: 0,
      unexpected_row_count: 0,
      missing_rows_sample: [],
      unexpected_rows_sample: []
    }

    materialized_rows
    |> merge_row_mismatches(oracle_rows, state, accumulator)
    |> Map.update!(:missing_rows_sample, &Enum.reverse/1)
    |> Map.update!(:unexpected_rows_sample, &Enum.reverse/1)
    |> Map.put(:sample_limit, @mismatch_sample_limit)
  end

  @doc false
  def raise_bounded_row_mismatch!(state, step_name, view_changed?, summary) do
    flunk(
      "View mismatch in step=#{step_name} shape=#{Map.get(state, :name)} " <>
        "where=#{Map.get(state, :where)} view_changed?=#{view_changed?}; " <>
        "row_summary=#{inspect(summary, pretty: true, limit: @mismatch_inspect_limit, printable_limit: @mismatch_printable_limit, width: 120)}"
    )
  end

  defp merge_row_mismatches([], [], _state, accumulator), do: accumulator

  defp merge_row_mismatches([materialized | rest], [], state, accumulator) do
    merge_row_mismatches(rest, [], state, record_unexpected(accumulator, state, materialized))
  end

  defp merge_row_mismatches([], [oracle | rest], state, accumulator) do
    merge_row_mismatches([], rest, state, record_missing(accumulator, state, oracle))
  end

  defp merge_row_mismatches(
         [materialized | materialized_rest] = materialized_rows,
         [oracle | oracle_rest] = oracle_rows,
         state,
         accumulator
       ) do
    materialized_key = key_from_value(state.pk, materialized)
    oracle_key = key_from_value(state.pk, oracle)

    cond do
      materialized_key == oracle_key and materialized == oracle ->
        accumulator = %{
          accumulator
          | materialized_row_count: accumulator.materialized_row_count + 1,
            oracle_row_count: accumulator.oracle_row_count + 1
        }

        merge_row_mismatches(materialized_rest, oracle_rest, state, accumulator)

      materialized_key == oracle_key ->
        accumulator =
          accumulator
          |> record_unexpected(state, materialized)
          |> record_missing(state, oracle)

        merge_row_mismatches(materialized_rest, oracle_rest, state, accumulator)

      materialized_key < oracle_key ->
        accumulator = record_unexpected(accumulator, state, materialized)
        merge_row_mismatches(materialized_rest, oracle_rows, state, accumulator)

      true ->
        accumulator = record_missing(accumulator, state, oracle)
        merge_row_mismatches(materialized_rows, oracle_rest, state, accumulator)
    end
  end

  defp record_missing(accumulator, state, row) do
    %{
      accumulator
      | oracle_row_count: accumulator.oracle_row_count + 1,
        missing_row_count: accumulator.missing_row_count + 1,
        missing_rows_sample: maybe_add_row_sample(accumulator.missing_rows_sample, state, row)
    }
  end

  defp record_unexpected(accumulator, state, row) do
    %{
      accumulator
      | materialized_row_count: accumulator.materialized_row_count + 1,
        unexpected_row_count: accumulator.unexpected_row_count + 1,
        unexpected_rows_sample:
          maybe_add_row_sample(accumulator.unexpected_rows_sample, state, row)
    }
  end

  defp maybe_add_row_sample(samples, state, row) do
    if length(samples) < @mismatch_sample_limit do
      [bounded_row_sample(state, row) | samples]
    else
      samples
    end
  end

  defp bounded_row_sample(state, row) do
    sampled_columns =
      state.pk
      |> Kernel.++(state.columns)
      |> Enum.uniq()
      |> Enum.take(@mismatch_max_columns_per_sample)

    sampled_values = Map.take(row, sampled_columns)

    %{
      key: key_from_value(state.pk, row),
      values: sampled_values,
      row_column_count: map_size(row),
      omitted_column_count: max(map_size(row) - map_size(sampled_values), 0)
    }
  end

  @doc false
  def bounded_shape_tree_diagnostics(%{
        stack_id: stack_id,
        poll_state: %{shape_handle: handle}
      })
      when is_binary(handle) do
    limits = diagnostic_limits()

    task =
      Task.async(fn ->
        try do
          context = %{
            deadline_ms: System.monotonic_time(:millisecond) + @diagnostic_total_timeout_ms,
            node_count: 0,
            seen: MapSet.new(),
            truncated?: false
          }

          case collect_shape_diagnostics(stack_id, handle, 0, context) do
            {:ok, diagnostics, context} -> {:ok, diagnostics, context}
            {:omitted, context} -> {:ok, %{shape_handle: handle}, context}
          end
        rescue
          error -> {:error, bounded_error(Exception.message(error))}
        catch
          kind, reason -> {:error, bounded_error("#{kind}: #{safe_inspect(reason)}")}
        end
      end)

    case Task.yield(task, @diagnostic_total_timeout_ms) do
      {:ok, {:ok, diagnostics, context}} ->
        Map.merge(diagnostics, %{
          diagnostic_limits: limits,
          truncated?: context.truncated?
        })

      {:ok, {:error, error}} ->
        %{
          shape_handle: handle,
          diagnostic_error: error,
          diagnostic_limits: limits,
          truncated?: true
        }

      {:exit, reason} ->
        %{
          shape_handle: handle,
          diagnostic_exit: bounded_error(safe_inspect(reason)),
          diagnostic_limits: limits,
          truncated?: true
        }

      nil ->
        Task.shutdown(task, :brutal_kill)

        %{
          shape_handle: handle,
          diagnostic_timeout?: true,
          diagnostic_limits: limits,
          truncated?: true
        }
    end
  end

  def bounded_shape_tree_diagnostics(_state) do
    %{
      shape_handle: nil,
      diagnostic_limits: diagnostic_limits(),
      truncated?: false
    }
  end

  defp diagnostic_limits do
    %{
      max_nodes: @diagnostic_max_nodes,
      max_depth: @diagnostic_max_depth,
      max_dependencies_per_node: @diagnostic_max_dependencies_per_node,
      total_timeout_ms: @diagnostic_total_timeout_ms
    }
  end

  defp collect_shape_diagnostics(stack_id, handle, depth, context) do
    cond do
      diagnostic_budget_expired?(context) ->
        {:omitted, %{context | truncated?: true}}

      context.node_count >= @diagnostic_max_nodes ->
        {:omitted, %{context | truncated?: true}}

      MapSet.member?(context.seen, handle) ->
        {:ok, %{shape_handle: handle, repeated?: true}, context}

      true ->
        context = %{
          context
          | node_count: context.node_count + 1,
            seen: MapSet.put(context.seen, handle)
        }

        {shape, shape_error} = fetch_shape_for_diagnostics(stack_id, handle)
        context = if shape_error, do: %{context | truncated?: true}, else: context
        dependency_handles = if shape, do: shape.shape_dependencies_handles, else: []
        dependency_count = Enum.count(dependency_handles)

        dependency_sample =
          dependency_handles
          |> Enum.take(@diagnostic_max_dependencies_per_node)
          |> Enum.sort()

        {dependencies, context} =
          collect_dependency_diagnostics(
            stack_id,
            dependency_sample,
            depth,
            dependency_count,
            context
          )

        omitted_dependency_count = max(dependency_count - length(dependencies), 0)

        context =
          if omitted_dependency_count > 0,
            do: %{context | truncated?: true},
            else: context

        diagnostics = %{
          shape_handle: handle,
          root_table: shape && shape.root_table,
          dependency_count: dependency_count,
          dependency_handles_sample: dependency_sample,
          omitted_dependency_count: omitted_dependency_count,
          consumer: consumer_diagnostics(stack_id, handle),
          materializer: materializer_diagnostics(stack_id, handle),
          dependencies: dependencies
        }

        diagnostics =
          if shape_error, do: Map.put(diagnostics, :shape_error, shape_error), else: diagnostics

        {:ok, diagnostics, context}
    end
  end

  defp collect_dependency_diagnostics(
         _stack_id,
         _dependency_sample,
         depth,
         dependency_count,
         context
       )
       when depth >= @diagnostic_max_depth do
    context = if dependency_count > 0, do: %{context | truncated?: true}, else: context
    {[], context}
  end

  defp collect_dependency_diagnostics(
         stack_id,
         dependency_sample,
         depth,
         _dependency_count,
         context
       ) do
    Enum.reduce_while(dependency_sample, {[], context}, fn dependency_handle,
                                                           {diagnostics, context} ->
      case collect_shape_diagnostics(stack_id, dependency_handle, depth + 1, context) do
        {:ok, dependency_diagnostics, context} ->
          {:cont, {[dependency_diagnostics | diagnostics], context}}

        {:omitted, context} ->
          {:halt, {diagnostics, context}}
      end
    end)
    |> then(fn {diagnostics, context} -> {Enum.reverse(diagnostics), context} end)
  end

  defp fetch_shape_for_diagnostics(stack_id, handle) do
    case Electric.ShapeCache.ShapeStatus.fetch_shape_by_handle(stack_id, handle) do
      {:ok, shape} -> {shape, nil}
      :error -> {nil, nil}
      other -> {nil, bounded_error("unexpected shape lookup result: #{safe_inspect(other)}")}
    end
  rescue
    error -> {nil, bounded_error(Exception.message(error))}
  catch
    :exit, reason -> {nil, bounded_error("exit: #{safe_inspect(reason)}")}
  end

  defp diagnostic_budget_expired?(context) do
    System.monotonic_time(:millisecond) >= context.deadline_ms
  end

  defp consumer_diagnostics(stack_id, handle) do
    stack_id
    |> Electric.Shapes.Consumer.whereis(handle)
    |> process_diagnostics()
  end

  defp materializer_diagnostics(stack_id, handle) do
    stack_id
    |> Electric.Shapes.Consumer.Materializer.whereis(handle)
    |> process_diagnostics()
  end

  defp process_diagnostics(pid) when is_pid(pid) do
    details =
      case Process.info(pid, [
             :message_queue_len,
             :status,
             :current_function,
             :memory,
             :heap_size,
             :total_heap_size,
             :stack_size,
             :reductions,
             :garbage_collection
           ]) do
        nil -> %{}
        details -> Map.new(details)
      end

    details
    |> Map.put(:pid, inspect(pid))
    |> Map.put(:alive?, Process.alive?(pid))
  end

  defp process_diagnostics(nil), do: %{pid: nil, alive?: false}

  defp bounded_error(message) when is_binary(message) do
    if byte_size(message) <= @diagnostic_error_max_bytes do
      message
    else
      binary_part(message, 0, @diagnostic_error_max_bytes) <> "…"
    end
  end

  defp safe_inspect(value) do
    inspect(value, limit: 20, printable_limit: @diagnostic_error_max_bytes)
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp validate_identifier!(value, label) do
    if String.match?(value, ~r/^[A-Za-z_][A-Za-z0-9_]*$/) do
      :ok
    else
      raise ArgumentError, "invalid #{label} identifier: #{inspect(value)}"
    end
  end

  defp quote_ident(value), do: ~s|"#{value}"|

  defp to_string_value(nil), do: nil
  defp to_string_value(true), do: "true"
  defp to_string_value(false), do: "false"
  defp to_string_value(val) when is_binary(val), do: val
  defp to_string_value(val), do: to_string(val)

  defp log(message) do
    IO.puts("[oracle] #{message}")
  end
end
