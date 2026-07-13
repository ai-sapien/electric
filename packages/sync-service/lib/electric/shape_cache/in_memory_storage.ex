defmodule Electric.ShapeCache.InMemoryStorage do
  use Agent

  alias Electric.ConcurrentStream
  alias Electric.Replication.LogOffset
  alias Electric.Telemetry.OpenTelemetry
  alias Electric.ShapeCache.Storage

  alias __MODULE__, as: MS

  import Electric.Replication.LogOffset, only: :macros

  @behaviour Electric.ShapeCache.Storage

  @snapshot_start_index 0
  @snapshot_end_index :end
  @pg_snapshot_key :pg_snapshot
  @move_positions_key :move_positions
  @root_delivery_tx_offset_key :root_delivery_tx_offset
  @latest_offset_key :latest_offset

  defstruct [
    :table_base_name,
    :snapshot_table,
    :log_table,
    :chunk_checkpoint_table,
    :shape_handle,
    :stack_id,
    :move_transaction_ref,
    :move_transaction_offset
  ]

  @impl Electric.ShapeCache.Storage
  def shared_opts(opts) do
    stack_id = Access.fetch!(opts, :stack_id)
    table_base_name = Access.get(opts, :table_base_name, inspect(__MODULE__))

    %{
      table_base_name: table_base_name,
      stack_id: stack_id
    }
  end

  def name(stack_id, shape_handle) when is_binary(shape_handle) do
    Electric.ProcessRegistry.name(stack_id, __MODULE__, shape_handle)
  end

  @impl Electric.ShapeCache.Storage
  def for_shape(shape_handle, %{shape_handle: shape_handle} = opts) do
    opts
  end

  def for_shape(shape_handle, %{
        table_base_name: table_base_name,
        stack_id: stack_id
      }) do
    snapshot_table_name = :"#{table_base_name}.Snapshot_#{shape_handle}"
    log_table_name = :"#{table_base_name}.Log_#{shape_handle}"

    chunk_checkpoint_table_name =
      :"#{table_base_name}.ChunkCheckpoint_#{shape_handle}"

    %__MODULE__{
      table_base_name: table_base_name,
      shape_handle: shape_handle,
      snapshot_table: snapshot_table_name,
      log_table: log_table_name,
      chunk_checkpoint_table: chunk_checkpoint_table_name,
      stack_id: stack_id
    }
  end

  @impl Electric.ShapeCache.Storage
  def stack_start_link(_), do: :ignore

  @impl Electric.ShapeCache.Storage
  def start_link(%MS{} = opts) do
    if is_nil(opts.shape_handle),
      do: raise(Storage.Error, "cannot start an un-attached storage instance")

    if is_nil(opts.stack_id), do: raise(Storage.Error, "stack_id cannot be nil")

    Agent.start_link(
      fn ->
        %{
          snapshot_table: storage_table(opts.snapshot_table),
          log_table: storage_table(opts.log_table),
          chunk_checkpoint_table: storage_table(opts.chunk_checkpoint_table)
        }
      end,
      name: name(opts.stack_id, opts.shape_handle)
    )
  end

  defp storage_table(name) do
    :ets.new(name, [:public, :named_table, :ordered_set])
  end

  @impl Electric.ShapeCache.Storage
  def init_writer!(%MS{} = opts, _shape_definition) do
    # A writer can die after promoting only part of a dependency move but
    # before publishing the new boundary/cursors. Those rows are invisible
    # while the boundary is old; trim them before this writer can append a
    # later ordinary transaction that would otherwise expose the abandoned
    # prefix. Hidden staging rows are likewise owned by the dead writer.
    trim_unpublished_move_rows!(opts)
    %{opts | move_transaction_ref: nil, move_transaction_offset: nil}
  end

  @impl Electric.ShapeCache.Storage
  def fetch_latest_offset(%MS{} = opts) do
    {:ok, current_offset(opts)}
  end

  @impl Electric.ShapeCache.Storage
  def fetch_pg_snapshot(%MS{} = opts) do
    {:ok, pg_snapshot(opts)}
  end

  defp pg_snapshot(opts) do
    case :ets.lookup(opts.snapshot_table, @pg_snapshot_key) do
      [{@pg_snapshot_key, pg_snapshot}] -> pg_snapshot
      [] -> nil
    end
  end

  defp current_offset(opts) do
    with [] <- :ets.lookup(opts.snapshot_table, @latest_offset_key),
         [] <- :ets.lookup(opts.snapshot_table, snapshot_end()) do
      LogOffset.last_before_real_offsets()
    else
      [{_, offset}] -> offset
    end
  end

  @impl Electric.ShapeCache.Storage
  def set_pg_snapshot(pg_snapshot, %MS{} = opts) do
    :ets.insert(opts.snapshot_table, {@pg_snapshot_key, pg_snapshot})
    :ok
  end

  @impl Electric.ShapeCache.Storage
  def set_move_positions!(move_positions, %MS{} = opts) do
    existing_positions? = :ets.member(opts.snapshot_table, @move_positions_key)
    :ets.insert(opts.snapshot_table, {@move_positions_key, move_positions})

    if not existing_positions? do
      :ets.insert_new(opts.snapshot_table, {@root_delivery_tx_offset_key, 0})
    end

    :ok
  end

  @impl Electric.ShapeCache.Storage
  def fetch_move_positions(%MS{} = opts) do
    case :ets.lookup(opts.snapshot_table, @move_positions_key) do
      [{@move_positions_key, move_positions}] -> {:ok, move_positions}
      [] -> {:ok, %{}}
    end
  end

  @impl Electric.ShapeCache.Storage
  def fetch_root_delivery_tx_offset(%MS{} = opts) do
    case :ets.lookup(opts.snapshot_table, @root_delivery_tx_offset_key) do
      [{@root_delivery_tx_offset_key, offset}] -> {:ok, offset}
      [] -> {:ok, nil}
    end
  end

  @impl Electric.ShapeCache.Storage
  def get_log_replay_safe_cursor(%MS{}), do: LogOffset.before_all()

  @impl Electric.ShapeCache.Storage
  def begin_move_transaction!(%MS{move_transaction_ref: nil, log_table: log_table} = opts) do
    # A prior writer may have disappeared without committing. Hidden move rows
    # are never readable, and a new writer can discard them before starting.
    :ets.match_delete(log_table, {{:move_offset, :_, :_}, :_})
    trim_unpublished_move_rows!(opts)

    %{
      opts
      | move_transaction_ref: make_ref(),
        move_transaction_offset: current_offset(opts)
    }
  end

  def begin_move_transaction!(%MS{}) do
    raise Storage.Error, "dependency-move transaction is already open"
  end

  @impl Electric.ShapeCache.Storage
  def commit_move_transaction!(
        move_positions,
        root_delivery_tx_offset,
        %MS{
          move_transaction_ref: ref,
          move_transaction_offset: offset,
          log_table: log_table,
          snapshot_table: snapshot_table
        } = opts
      )
      when is_reference(ref) and is_integer(root_delivery_tx_offset) and
             root_delivery_tx_offset >= 0 do
    ensure_root_delivery_frontier_monotonic!(snapshot_table, root_delivery_tx_offset)
    promote_move_rows_in_bounded_batches!(log_table, ref)

    :ets.match_delete(log_table, {{:move_offset, ref, :_}, :_})

    # One ETS insert publishes the readable boundary and its source cursors.
    # Readers clamp the promoted rows above to that boundary, so they cannot
    # observe the rows without the matching cursor state.
    :ets.insert(snapshot_table, [
      {@latest_offset_key, offset},
      {@move_positions_key, move_positions},
      {@root_delivery_tx_offset_key, root_delivery_tx_offset}
    ])

    %{opts | move_transaction_ref: nil, move_transaction_offset: nil}
  end

  def commit_move_transaction!(_move_positions, _root_delivery_tx_offset, %MS{}) do
    raise Storage.Error, "no dependency-move transaction is open"
  end

  defp ensure_root_delivery_frontier_monotonic!(snapshot_table, new_offset) do
    case :ets.lookup(snapshot_table, @root_delivery_tx_offset_key) do
      [{@root_delivery_tx_offset_key, current_offset}] when current_offset > new_offset ->
        raise Storage.Error,
          message: "cannot regress root-delivery frontier from #{current_offset} to #{new_offset}"

      _ ->
        :ok
    end
  end

  @move_promotion_batch_size 500

  defp promote_move_rows_in_bounded_batches!(log_table, ref) do
    promote_next_move_row_batch(log_table, ref, {:move_offset, ref, -1})
  end

  defp promote_next_move_row_batch(log_table, ref, cursor) do
    {rows, next_cursor} =
      take_move_row_batch(log_table, ref, cursor, @move_promotion_batch_size, [])

    case rows do
      [] ->
        :ok

      rows ->
        :ets.insert(
          log_table,
          Enum.map(rows, fn {{:move_offset, ^ref, item_offset}, json} ->
            {{:offset, item_offset}, json}
          end)
        )

        # Once a batch has a public copy, its staging rows are no longer
        # needed. If the writer dies before publishing the boundary, startup
        # trims those public rows again. Removing each staged batch here keeps
        # peak ETS usage bounded instead of retaining a full second copy of a
        # large move until the very end.
        Enum.each(rows, fn {key, _json} ->
          :ets.delete(log_table, key)
        end)

        # Continue from the last deleted staging key. Newly inserted public
        # keys are 2-tuples and sort before this 3-tuple cursor, so traversal
        # remains linear without relying on an ETS continuation across writes.
        promote_next_move_row_batch(log_table, ref, next_cursor)
    end
  end

  defp take_move_row_batch(_log_table, _ref, cursor, 0, rows) do
    {Enum.reverse(rows), cursor}
  end

  defp take_move_row_batch(log_table, ref, cursor, remaining, rows) do
    case :ets.next_lookup(log_table, cursor) do
      {{:move_offset, ^ref, _item_offset} = key, [{_key, json}]} ->
        take_move_row_batch(log_table, ref, key, remaining - 1, [{key, json} | rows])

      _end_or_different_move ->
        {Enum.reverse(rows), cursor}
    end
  end

  defp trim_unpublished_move_rows!(%MS{} = opts) do
    boundary = opts |> current_offset() |> storage_offset()

    :ets.match_delete(opts.log_table, {{:move_offset, :_, :_}, :_})

    :ets.select_delete(opts.log_table, [
      {{{:offset, :"$1"}, :_}, [{:>, :"$1", {:const, boundary}}], [true]}
    ])

    :ets.select_delete(opts.chunk_checkpoint_table, [
      {{:"$1", :_}, [{:>, :"$1", {:const, boundary}}], [true]}
    ])

    :ok
  end

  @impl Electric.ShapeCache.Storage
  def get_all_stored_shape_handles(_opts), do: {:ok, MapSet.new()}

  @impl Electric.ShapeCache.Storage
  def get_total_disk_usage(_opts), do: 0

  @impl Electric.ShapeCache.Storage
  def snapshot_started?(%MS{} = opts) do
    try do
      :ets.member(opts.snapshot_table, snapshot_start())
    rescue
      ArgumentError ->
        false
    end
  end

  defp snapshot_key(chunk_key, index) do
    {chunk_key, index}
  end

  defp snapshot_chunk_start(chunk_key), do: snapshot_key(chunk_key, @snapshot_start_index)
  defp snapshot_chunk_end(chunk_key), do: snapshot_key(chunk_key, @snapshot_end_index)

  defp snapshot_start(), do: snapshot_chunk_start(storage_offset(LogOffset.before_all()))

  defp snapshot_end(),
    do: snapshot_chunk_end(storage_offset(LogOffset.last_before_real_offsets()))

  defp get_offset_indexed_stream(offset, max_offset, offset_indexed_table, project_item) do
    offset = storage_offset(offset)
    max_offset = storage_offset(max_offset)

    Stream.unfold({:offset, offset}, fn cursor ->
      next_visible_log_item(offset_indexed_table, cursor, max_offset, project_item)
    end)
  end

  defp next_visible_log_item(table, cursor, max_offset, project_item) do
    case :ets.next_lookup(table, cursor) do
      :"$end_of_table" ->
        nil

      {{:offset, position}, _} when position > max_offset ->
        nil

      {{:offset, position} = key, [{_, item}]} ->
        {project_item.(LogOffset.new(position), item), key}

      {{:move_offset, _ref, _position}, _items} ->
        # Public offset keys are 2-tuples while staging keys are 3-tuples, so
        # ordered_set places the complete staging suffix after every public
        # log row. Stop at its first key instead of walking a potentially huge
        # unpublished move merely to discover that none of it is readable.
        nil

      {hidden_key, _items} ->
        next_visible_log_item(table, hidden_key, max_offset, project_item)
    end
  end

  defp get_offset_indexed_stream(offset, max_offset, offset_indexed_table) do
    get_offset_indexed_stream(offset, max_offset, offset_indexed_table, fn _, item -> item end)
  end

  @snapshot_boundary_offset LogOffset.last_before_real_offsets()
  @impl Electric.ShapeCache.Storage
  def get_log_stream(offset, max_offset, %MS{} = opts)
      when is_log_offset_lt(offset, @snapshot_boundary_offset) do
    case :ets.lookup_element(opts.snapshot_table, snapshot_end(), 2, nil) do
      nil -> stream_from_snapshot(offset, max_offset, opts)
      max when is_log_offset_lt(offset, max) -> stream_from_snapshot(offset, max_offset, opts)
      _ -> get_visible_log_stream(offset, max_offset, opts)
    end
  end

  def get_log_stream(offset, max_offset, %MS{} = opts) do
    get_visible_log_stream(offset, max_offset, opts)
  end

  @impl Electric.ShapeCache.Storage
  def get_log_stream_with_offsets(offset, max_offset, %MS{} = opts)
      when is_log_offset_lt(offset, @snapshot_boundary_offset) do
    case :ets.lookup_element(opts.snapshot_table, snapshot_end(), 2, nil) do
      nil ->
        stream_from_snapshot_with_offsets(offset, max_offset, opts)

      max when is_log_offset_lt(offset, max) ->
        stream_from_snapshot_with_offsets(offset, max_offset, opts)

      _ ->
        get_visible_log_stream_with_offsets(offset, max_offset, opts)
    end
  end

  def get_log_stream_with_offsets(offset, max_offset, %MS{} = opts) do
    get_visible_log_stream_with_offsets(offset, max_offset, opts)
  end

  defp get_visible_log_stream(offset, max_offset, %MS{} = opts) do
    get_offset_indexed_stream(
      offset,
      LogOffset.min(max_offset, current_offset(opts)),
      opts.log_table
    )
  end

  defp get_visible_log_stream_with_offsets(offset, max_offset, %MS{} = opts) do
    get_offset_indexed_stream_with_offsets(
      offset,
      LogOffset.min(max_offset, current_offset(opts)),
      opts.log_table
    )
  end

  defp get_offset_indexed_stream_with_offsets(offset, max_offset, offset_indexed_table) do
    get_offset_indexed_stream(offset, max_offset, offset_indexed_table, fn offset, item ->
      {offset, item}
    end)
  end

  defp stream_from_snapshot_with_offsets(offset, max_offset, opts) do
    offset
    |> stream_from_snapshot(max_offset, opts)
    |> Stream.map(&{nil, &1})
  end

  defp stream_from_snapshot(offset, max_offset, %MS{} = opts) do
    ConcurrentStream.stream_to_end(
      excluded_start_key: snapshot_chunk_end(storage_offset(offset)),
      end_marker_key: snapshot_chunk_end(storage_offset(max_offset)),
      poll_time_in_ms: 10,
      stream_fun: fn excluded_start_key, included_end_key ->
        if !snapshot_started?(opts), do: raise(Storage.Error, "Snapshot no longer available")

        :ets.select(
          opts.snapshot_table,
          [
            {{:"$1", :"$2"},
             [
               {:andalso, {:>, :"$1", {:const, excluded_start_key}},
                {:"=<", :"$1", {:const, included_end_key}}}
             ], [{{:"$1", :"$2"}}]}
          ]
        )
      end
    )
    |> Stream.map(fn {_, item} -> item end)
    |> Stream.reject(&is_nil/1)
  end

  @impl Electric.ShapeCache.Storage
  def get_chunk_end_log_offset(offset, _) when is_min_offset(offset),
    do: LogOffset.first()

  def get_chunk_end_log_offset(offset, %MS{} = opts) do
    case :ets.next_lookup(opts.chunk_checkpoint_table, storage_offset(offset)) do
      :"$end_of_table" ->
        nil

      {chunk_offset, _} ->
        chunk_offset = LogOffset.new(chunk_offset)

        if LogOffset.is_log_offset_lte(chunk_offset, current_offset(opts)),
          do: chunk_offset,
          else: nil
    end
  end

  @impl Electric.ShapeCache.Storage
  def make_new_snapshot!(data_stream, %MS{stack_id: stack_id} = opts) do
    OpenTelemetry.with_span(
      "storage.make_new_snapshot",
      [storage_impl: "in_memory", "shape.handle": opts.shape_handle],
      stack_id,
      fn ->
        table = opts.snapshot_table
        chunk_checkpoint_table = opts.chunk_checkpoint_table

        data_stream
        |> Stream.with_index(1)
        |> Stream.transform(
          fn -> 0 end,
          fn
            {:chunk_boundary, _}, chunk_num ->
              chunk_offset = storage_offset(LogOffset.new(0, chunk_num))

              {[
                 {chunk_offset, :snapshot_checkpoint},
                 {snapshot_chunk_end(chunk_offset), nil}
               ], chunk_num + 1}

            {line, index}, chunk_num ->
              chunk_offset = storage_offset(LogOffset.new(0, chunk_num))
              {[{snapshot_key(chunk_offset, index), line}], chunk_num}
          end,
          fn chunk_num ->
            chunk_offset = storage_offset(LogOffset.new(0, chunk_num))

            {[{chunk_offset, :snapshot_checkpoint}, {snapshot_chunk_end(chunk_offset), nil}],
             chunk_num}
          end,
          fn _ -> nil end
        )
        |> Stream.chunk_every(500)
        |> Stream.flat_map(fn chunk ->
          {checkpoints, data} = Enum.split_with(chunk, &match?({_, :snapshot_checkpoint}, &1))

          :ets.insert(chunk_checkpoint_table, checkpoints)
          :ets.insert(table, data)
          Enum.map(checkpoints, &elem(&1, 0))
        end)
        |> Enum.max()
        |> then(fn max_chunk ->
          :ets.insert(table, {snapshot_end(), LogOffset.new(max_chunk)})
        end)

        :ok
      end
    )
  end

  @impl Electric.ShapeCache.Storage
  def mark_snapshot_as_started(%MS{} = opts) do
    :ets.insert(opts.snapshot_table, {snapshot_start(), 0})
    :ok
  end

  @impl Electric.ShapeCache.Storage
  def append_to_log!(log_items, %MS{} = opts) do
    log_table = opts.log_table
    chunk_checkpoint_table = opts.chunk_checkpoint_table

    {processed_log_items, last_offset} =
      Enum.map_reduce(log_items, nil, fn
        {:chunk_boundary, offset}, curr ->
          {{storage_offset(offset), :checkpoint}, curr}

        {offset, _key, _op_type, json_log_item}, _ ->
          key = log_item_key(opts, offset)
          {{key, json_log_item}, offset}
      end)

    processed_log_items
    |> Enum.split_with(fn item -> match?({_, :checkpoint}, item) end)
    |> then(fn {checkpoints, log_items} ->
      :ets.insert(chunk_checkpoint_table, checkpoints)
      :ets.insert(log_table, log_items)

      if is_nil(opts.move_transaction_ref) do
        publish_log_offset!(opts, last_offset)
      end
    end)

    if is_nil(opts.move_transaction_ref) do
      send(self(), {Storage, :flushed, elem(List.last(log_items), 0)})
    end

    update_move_transaction_offset(opts, last_offset)
  end

  @impl Electric.ShapeCache.Storage
  def supports_txn_fragment_streaming?, do: false

  @impl Electric.ShapeCache.Storage
  def append_fragment_to_log!(_log_items, %MS{} = _opts) do
    raise "Not implemented; InMemoryStorage does not support txn fragment streaming. Use PureFileStorage instead."
  end

  @impl Electric.ShapeCache.Storage
  def signal_txn_commit!(_xid, %MS{} = _opts) do
    raise "Not implemented; InMemoryStorage does not support txn fragment streaming. Use PureFileStorage instead."
  end

  @impl Electric.ShapeCache.Storage
  def write_move_in_snapshot!(stream, name, %MS{log_table: log_table}) do
    stream
    |> Stream.map(fn [key, tags, json] -> {{:movein, {name, key}}, {tags, json}} end)
    |> Stream.chunk_every(500)
    |> Stream.each(&:ets.insert(log_table, &1))
    |> Stream.run()

    :ok
  end

  @impl Electric.ShapeCache.Storage
  def append_control_message!(control_message, %MS{log_table: log_table} = opts) do
    initial_offset = writer_offset(opts)
    new_offset = LogOffset.increment(initial_offset)

    :ets.insert(log_table, {log_item_key(opts, new_offset), control_message})

    if is_nil(opts.move_transaction_ref) do
      publish_log_offset!(opts, new_offset)
    end

    {{initial_offset, new_offset}, update_move_transaction_offset(opts, new_offset)}
  end

  @impl Electric.ShapeCache.Storage
  def append_move_in_snapshot_to_log!(name, %MS{log_table: log_table} = opts, skip_row?) do
    initial_offset = writer_offset(opts)
    ref = make_ref()

    Stream.unfold({initial_offset, {:movein, {name, nil}}}, fn {offset, last_key} ->
      case :ets.next_lookup(log_table, last_key) do
        {{:movein, {^name, _}} = ets_key, [{{:movein, {^name, key}}, {tags, json}}]} ->
          if skip_row?.(key, tags) do
            {[], {offset, ets_key}}
          else
            offset = LogOffset.increment(offset)
            {{log_item_key(opts, offset), json}, {offset, ets_key}}
          end

        _ ->
          send(self(), {ref, offset})
          nil
      end
    end)
    |> Stream.reject(&(&1 == []))
    |> Stream.chunk_every(500)
    |> Stream.each(&:ets.insert(log_table, &1))
    |> Stream.run()

    :ets.match_delete(log_table, {{:movein, {name, :_}}, :_})

    resulting_offset = receive(do: ({^ref, offset} -> offset))

    if is_nil(opts.move_transaction_ref) do
      # Standalone move-in appends were historically readable immediately.
      # Keep the published boundary aligned with those visible rows now that
      # readers clamp streams to the latest committed offset.
      publish_log_offset!(opts, resulting_offset)
    end

    {{initial_offset, resulting_offset}, update_move_transaction_offset(opts, resulting_offset)}
  end

  defp writer_offset(%MS{move_transaction_offset: %LogOffset{} = offset}), do: offset
  defp writer_offset(%MS{} = opts), do: current_offset(opts)

  defp log_item_key(%MS{move_transaction_ref: ref}, offset) when is_reference(ref),
    do: {:move_offset, ref, storage_offset(offset)}

  defp log_item_key(%MS{}, offset), do: {:offset, storage_offset(offset)}

  defp update_move_transaction_offset(%MS{move_transaction_ref: ref} = opts, offset)
       when is_reference(ref),
       do: %{opts | move_transaction_offset: offset}

  defp update_move_transaction_offset(%MS{} = opts, _offset), do: opts

  defp publish_log_offset!(opts, offset),
    do: :ets.insert(opts.snapshot_table, {@latest_offset_key, offset})

  @impl Electric.ShapeCache.Storage
  def cleanup!(%MS{} = opts) do
    for table <- tables(opts),
        do: ignoring_exceptions(fn -> :ets.delete(table) end, ArgumentError)

    :ok
  end

  @impl Electric.ShapeCache.Storage
  def cleanup!(%MS{shape_handle: shape_handle} = opts, shape_handle) do
    cleanup!(opts)
  end

  def cleanup!(%{table_base_name: _table_base_name, stack_id: _stack_id} = opts, shape_handle) do
    shape_handle
    |> for_shape(opts)
    |> cleanup!()
  end

  @impl Electric.ShapeCache.Storage
  def cleanup_all!(%{table_base_name: table_base_name} = _opts) do
    :ets.all()
    |> Enum.filter(&is_atom/1)
    |> Enum.filter(fn name ->
      String.starts_with?(Atom.to_string(name), "#{table_base_name}.")
    end)
    |> Enum.each(&ignoring_exceptions(fn -> :ets.delete(&1) end, ArgumentError))

    :ok
  end

  defp ignoring_exceptions(fun, exception) do
    fun.()
  rescue
    error ->
      if error.__struct__ == exception do
        :ok
      else
        reraise(error, __STACKTRACE__)
      end
  end

  defp tables(%MS{} = opts) do
    [
      opts.snapshot_table,
      opts.log_table,
      opts.chunk_checkpoint_table
    ]
  end

  # Turns a LogOffset into a tuple representation
  # for storing in the ETS table
  defp storage_offset(offset) do
    LogOffset.to_tuple(offset)
  end

  @impl Electric.ShapeCache.Storage
  def compact(_opts, _offset), do: :ok

  @impl Electric.ShapeCache.Storage
  def terminate(_opts), do: :ok

  @impl Electric.ShapeCache.Storage
  def hibernate(opts), do: opts
end
