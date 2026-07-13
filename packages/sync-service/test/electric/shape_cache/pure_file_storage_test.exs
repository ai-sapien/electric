defmodule Electric.ShapeCache.PureFileStorageTest do
  use ExUnit.Case, async: true
  use Repatch.ExUnit

  import Support.ComponentSetup
  import Support.TestUtils
  import Electric.ShapeCache.PureFileStorage.SharedRecords

  alias Electric.Replication.Changes
  alias Electric.Replication.LogOffset
  alias Electric.ShapeCache.Storage
  alias Electric.ShapeCache.PureFileStorage
  alias Electric.ShapeCache.PureFileStorage.ChunkIndex
  alias Electric.ShapeCache.PureFileStorage.LogFile
  alias Electric.Shapes.Shape

  @moduletag :tmp_dir
  setup [
    :with_stack_id_from_test,
    :with_async_deleter
  ]

  @shape_handle "the-shape-handle"
  @shape %Shape{
    root_table: {"public", "items"},
    root_table_id: 1,
    root_pk: ["id"],
    selected_columns: ["id"],
    explicitly_selected_columns: ["id"],
    where:
      Electric.Replication.Eval.Parser.parse_and_validate_expression!("id != '1'",
        refs: %{["id"] => :text}
      )
  }

  @xid 100
  @lsn 100

  @fragments txn_fragments(@xid, @lsn, [
               %{
                 changes: [
                   %Changes.NewRecord{
                     relation: {"public", "test_table"},
                     record: %{"id" => "5"},
                     log_offset: LogOffset.new(@lsn, 0)
                   },
                   %Changes.UpdatedRecord{
                     relation: {"public", "test_table"},
                     old_record: %{"id" => "1"},
                     record: %{"id" => "1", "foo" => "bar"},
                     log_offset: LogOffset.new(@lsn, 2),
                     changed_columns: MapSet.new(["foo"])
                   }
                 ]
               },
               %{
                 changes: [
                   %Changes.UpdatedRecord{
                     relation: {"public", "test_table"},
                     old_record: %{"id" => "3"},
                     record: %{"id" => "3", "another" => "update"},
                     log_offset: LogOffset.new(@lsn, 4),
                     changed_columns: MapSet.new(["another"])
                   }
                 ]
               },
               %{
                 changes: [
                   %Changes.NewRecord{
                     relation: {"public", "test_table"},
                     record: %{"id" => "6"},
                     log_offset: LogOffset.new(@lsn, 6)
                   }
                 ]
               },
               %{
                 changes: [
                   %Changes.DeletedRecord{
                     relation: {"public", "test_table"},
                     old_record: %{"id" => "2"},
                     log_offset: LogOffset.new(@lsn, 8),
                     last?: true
                   }
                 ]
               }
             ])

  defp start_storage(ctx) do
    base_opts =
      PureFileStorage.shared_opts(
        stack_id: ctx.stack_id,
        storage_dir: ctx.tmp_dir,
        chunk_bytes_threshold: ctx[:chunk_size] || 10 * 1024 * 1024,
        max_deferred_chunk_index_bytes: ctx[:max_deferred_chunk_index_bytes],
        flush_period: ctx[:flush_period] || 1000
      )

    storage_base = {PureFileStorage, base_opts}
    start_link_supervised!(Storage.stack_child_spec(storage_base))

    %{base_opts: base_opts, opts: PureFileStorage.for_shape(@shape_handle, base_opts)}
  end

  describe "reads without writer -" do
    setup [:start_storage]

    test "snapshot only reads from disk", %{opts: opts} do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([~S|{"test": 1}|, ~S|{"test": 2}|], opts)
      PureFileStorage.terminate(writer)

      assert PureFileStorage.get_log_stream(
               LogOffset.before_all(),
               LogOffset.last_before_real_offsets(),
               opts
             )
             |> Enum.to_list() == [~S|{"test": 1}|, ~S|{"test": 2}|]
    end

    test "active log reads", %{opts: opts, stack_id: stack_id} do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([], opts)

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(10, 0), "test_key", :insert, ~S|{"test": 1}|},
            {LogOffset.new(11, 0), "test_key", :insert, ~S|{"test": 2}|}
          ],
          writer
        )

      # This cleans the shapes ets cache, retaining only the values
      # required for the read-path
      PureFileStorage.terminate(writer)

      assert PureFileStorage.fetch_latest_offset(opts) ==
               {:ok, LogOffset.new(11, 0)}

      assert PureFileStorage.fetch_pg_snapshot(opts) ==
               {:ok, %{xmin: 100}}

      assert PureFileStorage.get_log_stream(LogOffset.new(0, 0), LogOffset.last(), opts)
             |> Enum.to_list() == [~S|{"test": 1}|, ~S|{"test": 2}|]

      # this simulates a cold start
      stack_ets = PureFileStorage.stack_ets(stack_id)
      :ets.delete(stack_ets, @shape_handle)

      assert PureFileStorage.fetch_latest_offset(opts) ==
               {:ok, LogOffset.new(11, 0)}

      assert PureFileStorage.fetch_pg_snapshot(opts) ==
               {:ok, %{xmin: 100}}

      assert PureFileStorage.get_log_stream(LogOffset.new(0, 0), LogOffset.last(), opts)
             |> Enum.to_list() == [~S|{"test": 1}|, ~S|{"test": 2}|]
    end

    test "reads survive the later deletion of the snapshot file", %{
      opts: opts,
      base_opts: base_opts
    } do
      opts = %{opts | snapshot_file_timeout: 50}
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([~S|{"test": 1}|, ~S|{"test": 2}|], opts)
      PureFileStorage.terminate(writer)

      stream =
        PureFileStorage.get_log_stream(
          LogOffset.before_all(),
          LogOffset.last_before_real_offsets(),
          opts
        )

      File.rename!(
        PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle),
        PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle <> "-deleted")
      )

      assert Enum.to_list(stream) == []
    end

    test "reads to a deleted shape do not raise", %{opts: opts, base_opts: base_opts} do
      opts = %{opts | snapshot_file_timeout: 50}
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([~S|{"test": 1}|, ~S|{"test": 2}|], opts)
      PureFileStorage.terminate(writer)

      File.rename!(
        PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle),
        PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle <> "-deleted")
      )

      stream =
        PureFileStorage.get_log_stream(
          LogOffset.before_all(),
          LogOffset.last_before_real_offsets(),
          opts
        )

      assert Enum.to_list(stream) == []
    end

    test "enoent returned from an existing shape does raise", %{opts: opts} do
      opts = %{opts | snapshot_file_timeout: 50}
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([~S|{"test": 1}|, ~S|{"test": 2}|], opts)
      PureFileStorage.terminate(writer)

      chunk_file_path = PureFileStorage.Snapshot.chunk_file_path(opts, 0)

      File.rm!(chunk_file_path)

      assert_raise File.Error, fn ->
        PureFileStorage.get_log_stream(
          LogOffset.before_all(),
          LogOffset.last_before_real_offsets(),
          opts
        )
        |> Enum.to_list()
      end
    end
  end

  describe "read-through cache -" do
    setup [:start_storage]

    test "is always populated", %{
      opts: opts,
      stack_id: stack_id
    } do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([], opts)

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test": 1}|}],
          writer
        )

      stack_ets = PureFileStorage.stack_ets(stack_id)

      # Should populate read through cache even while writer is active
      assert PureFileStorage.snapshot_started?(opts) == true
      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, LogOffset.new(10, 0)}

      assert [_] = :ets.lookup(stack_ets, @shape_handle)

      PureFileStorage.terminate(writer)

      # terminating the writer does not delete the entry, just cleans the writer-only fields
      assert [_] = :ets.lookup(stack_ets, @shape_handle)

      assert PureFileStorage.snapshot_started?(opts) == true
      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, LogOffset.new(10, 0)}

      # Verify cache is retained when writer is re-activated
      PureFileStorage.init_writer!(opts, @shape)

      assert [_] = :ets.lookup(stack_ets, @shape_handle)
    end

    test "is cleaned of writer metadata after writer termination", %{
      opts: opts,
      stack_id: stack_id
    } do
      import Electric.ShapeCache.PureFileStorage.SharedRecords,
        only: [storage_meta: 0, storage_meta: 1]

      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([], opts)

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test": 1}|}],
          writer
        )

      stack_ets = PureFileStorage.stack_ets(stack_id)

      # Should populate read through cache even while writer is active
      assert PureFileStorage.snapshot_started?(opts) == true
      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, LogOffset.new(10, 0)}

      assert [storage_meta() = meta] = :ets.lookup(stack_ets, @shape_handle)

      assert storage_meta(ets_table: ets_table) = meta
      assert is_list(:ets.info(ets_table))

      PureFileStorage.terminate(writer)

      # terminating the writer does not delete the entry, just cleans the writer-only fields
      assert [storage_meta() = meta] = :ets.lookup(stack_ets, @shape_handle)

      assert storage_meta(ets_table: nil) = meta
      assert :undefined == :ets.info(ets_table)
    end

    test "subsequent reads use cached values without disk access", %{
      opts: opts,
      base_opts: base_opts
    } do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([], opts)

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test": 1}|}],
          writer
        )

      PureFileStorage.terminate(writer)

      # First read populates cache
      assert PureFileStorage.snapshot_started?(opts) == true
      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, LogOffset.new(10, 0)}

      # Delete shape failes to confirm cache is used
      File.rm_rf!(Path.join([base_opts.base_path, @shape_handle]))

      assert PureFileStorage.snapshot_started?(opts) == true
      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, LogOffset.new(10, 0)}
    end

    test "cache is cleaned up on shape deletion", %{opts: opts, stack_id: stack_id} do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([], opts)
      PureFileStorage.terminate(writer)

      # Populate cache
      PureFileStorage.snapshot_started?(opts)

      stack_ets = PureFileStorage.stack_ets(stack_id)
      assert [_meta] = :ets.lookup(stack_ets, @shape_handle)

      # Verify cache entry is removed on cleanup
      {PureFileStorage, base_opts} = Storage.for_stack(stack_id)
      PureFileStorage.cleanup!(base_opts, @shape_handle)
      assert :ets.lookup(stack_ets, @shape_handle) == []
    end

    test "reads work for missing metadata", %{opts: opts} do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.terminate(writer)

      assert PureFileStorage.snapshot_started?(opts) == false

      assert PureFileStorage.fetch_latest_offset(opts) ==
               {:ok, LogOffset.last_before_real_offsets()}
    end
  end

  describe "key index writes" do
    setup [:start_storage]

    test "are correct", %{opts: opts} do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([], opts)

      suffix = PureFileStorage.latest_name(opts)

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|},
            {LogOffset.new(11, 0), "test_key", :update, ~S|{"test":2}|},
            {LogOffset.new(12, 0), "test_key", :delete, ~S|{"test":2}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)

      key_file = PureFileStorage.key_file(opts, suffix)

      PureFileStorage.KeyIndex.create_from_log(
        PureFileStorage.json_file(opts, suffix),
        key_file
      )

      assert File.exists?(key_file)

      assert PureFileStorage.KeyIndex.read_key_file(PureFileStorage.key_file(opts, suffix)) == [
               {"test_key", LogOffset.new(10, 0), ?i, 0, byte_size(~S|{"test":1}|)},
               {"test_key", LogOffset.new(11, 0), ?u, 48, byte_size(~S|{"test":2}|)},
               {"test_key", LogOffset.new(12, 0), ?d, 96, byte_size(~S|{"test":2}|)}
             ]
    end

    @tag chunk_size: 5
    test "are correct with small chunks too", %{opts: opts} do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([], opts)

      writer =
        for i <- 1..10 do
          %Changes.UpdatedRecord{
            relation: {"public", "test_table"},
            old_record: %{"id" => "sameid", "name" => "Test#{i - 1}"},
            record: %{"id" => "sameid", "name" => "Test#{i}"},
            log_offset: LogOffset.new(i, 0),
            changed_columns: MapSet.new(["name"])
          }
        end
        |> changes_to_log_items()
        |> PureFileStorage.append_to_log!(writer)

      PureFileStorage.terminate(writer)

      key_file = PureFileStorage.key_file(opts, PureFileStorage.latest_name(opts))

      PureFileStorage.KeyIndex.create_from_log(
        PureFileStorage.json_file(opts, PureFileStorage.latest_name(opts)),
        key_file
      )

      assert File.exists?(key_file)

      assert PureFileStorage.KeyIndex.read_key_file(key_file) == [
               {"\"public\".\"test_table\"/\"sameid\"", LogOffset.new(1, 0), 117, 0, 191},
               {"\"public\".\"test_table\"/\"sameid\"", LogOffset.new(2, 0), 117, 251, 191},
               {"\"public\".\"test_table\"/\"sameid\"", LogOffset.new(3, 0), 117, 502, 191},
               {"\"public\".\"test_table\"/\"sameid\"", LogOffset.new(4, 0), 117, 753, 191},
               {"\"public\".\"test_table\"/\"sameid\"", LogOffset.new(5, 0), 117, 1004, 191},
               {"\"public\".\"test_table\"/\"sameid\"", LogOffset.new(6, 0), 117, 1255, 191},
               {"\"public\".\"test_table\"/\"sameid\"", LogOffset.new(7, 0), 117, 1506, 191},
               {"\"public\".\"test_table\"/\"sameid\"", LogOffset.new(8, 0), 117, 1757, 191},
               {"\"public\".\"test_table\"/\"sameid\"", LogOffset.new(9, 0), 117, 2008, 191},
               {"\"public\".\"test_table\"/\"sameid\"", LogOffset.new(10, 0), 117, 2259, 193}
             ]
    end
  end

  describe "chunk reads" do
    setup [:start_storage, :with_started_writer]

    @tag chunk_size: 1
    test "constructing an exact cross-generation stream opens no log files", %{
      writer: writer,
      opts: opts
    } do
      %{writer: writer, first_offset: first_offset, last_offset: last_offset} =
        prepare_cross_generation_replay(writer, opts)

      patch_log_file_opens(self())

      stream =
        PureFileStorage.get_log_stream_with_offsets(
          LogOffset.first(),
          last_offset,
          %{opts | read_only?: true}
        )

      refute_receive {:log_file_opened, _path, _file}

      assert [{^first_offset, _json}] = Enum.take(stream, 1)
      assert_receive {:log_file_opened, _path, _file}

      PureFileStorage.terminate(writer)
    end

    @tag chunk_size: 1
    test "taking one exact row closes every cross-generation descriptor", %{
      writer: writer,
      opts: opts
    } do
      %{writer: writer, first_offset: first_offset, last_offset: last_offset} =
        prepare_cross_generation_replay(writer, opts)

      patch_log_file_opens(self())

      stream =
        PureFileStorage.get_log_stream_with_offsets(
          LogOffset.first(),
          last_offset,
          %{opts | read_only?: true}
        )

      assert [{^first_offset, _json}] = Enum.take(stream, 1)

      opened_files =
        for _ <- 1..2 do
          assert_receive {:log_file_opened, _path, file}
          file
        end

      Enum.each(opened_files, &assert_file_closed/1)
      refute_receive {:log_file_opened, _path, _file}

      PureFileStorage.terminate(writer)
    end

    @tag chunk_size: 1
    test "a failed second-generation open closes the first before one fresh retry", %{
      writer: writer,
      opts: opts
    } do
      %{
        writer: writer,
        first_offset: first_offset,
        first_json: first_json,
        last_offset: last_offset,
        last_json: last_json
      } = prepare_cross_generation_replay(writer, opts)

      test_pid = self()
      open_count_key = {__MODULE__, :cross_generation_open_count}
      first_file_key = {__MODULE__, :first_generation_file}

      Repatch.patch(PureFileStorage, :safely_open_file!, fn reader_opts, path, modes ->
        open_count = Process.get(open_count_key, 0) + 1
        Process.put(open_count_key, open_count)

        case open_count do
          2 ->
            raise File.Error, path: path, reason: :enoent

          3 ->
            first_file = Process.get(first_file_key)
            refute is_nil(first_file)
            assert_file_closed(first_file)
            send(test_pid, :first_generation_closed_before_retry)
            open_and_record_log_file(reader_opts, path, modes, test_pid, open_count)

          _ ->
            result = open_and_record_log_file(reader_opts, path, modes, test_pid, open_count)

            if open_count == 1 do
              {:ok, file} = result
              Process.put(first_file_key, file)
            end

            result
        end
      end)

      stream =
        PureFileStorage.get_log_stream_with_offsets(
          LogOffset.first(),
          last_offset,
          %{opts | read_only?: true}
        )

      assert Enum.map(stream, &decode_offset_row/1) == [
               {first_offset, Jason.decode!(first_json)},
               {last_offset, Jason.decode!(last_json)}
             ]

      assert_receive :first_generation_closed_before_retry
      assert Process.get(open_count_key) == 4

      opened_files =
        for open_count <- [1, 3, 4] do
          assert_receive {:log_file_opened, ^open_count, _path, file}
          file
        end

      Enum.each(opened_files, &assert_file_closed/1)
      refute_receive {:log_file_opened, _open_count, _path, _file}

      PureFileStorage.terminate(writer)
    end

    @tag chunk_size: 1
    test "an exact cross-generation stream is reusable and closes each enumeration", %{
      writer: writer,
      opts: opts
    } do
      %{
        writer: writer,
        first_offset: first_offset,
        first_json: first_json,
        last_offset: last_offset,
        last_json: last_json
      } = prepare_cross_generation_replay(writer, opts)

      patch_log_file_opens(self())

      stream =
        PureFileStorage.get_log_stream_with_offsets(
          LogOffset.first(),
          last_offset,
          %{opts | read_only?: true}
        )

      expected = [
        {first_offset, Jason.decode!(first_json)},
        {last_offset, Jason.decode!(last_json)}
      ]

      assert Enum.map(stream, &decode_offset_row/1) == expected
      assert Enum.map(stream, &decode_offset_row/1) == expected

      opened_files =
        for _ <- 1..4 do
          assert_receive {:log_file_opened, _path, file}
          file
        end

      Enum.each(opened_files, &assert_file_closed/1)
      refute_receive {:log_file_opened, _path, _file}

      PureFileStorage.terminate(writer)
    end

    test "ordinary partial streams defer opening and close on early halt", %{
      writer: writer,
      opts: opts
    } do
      first_offset = LogOffset.new(10, 0)
      first_json = ~S|{"test":"first"}|

      writer =
        PureFileStorage.append_to_log!(
          [
            {first_offset, "first", :insert, first_json},
            {LogOffset.new(11, 0), "second", :insert, ~S|{"test":"second"}|}
          ],
          writer
        )

      writer = PureFileStorage.hibernate(writer)
      patch_log_file_opens(self())

      stream = PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.last(), opts)

      refute_receive {:log_file_opened, _path, _file}
      assert Enum.take(stream, 1) == [first_json]
      assert_receive {:log_file_opened, _path, file}
      assert_file_closed(file)
      refute_receive {:log_file_opened, _path, _file}

      PureFileStorage.terminate(writer)
    end

    @tag chunk_size: 100
    test "exact replay does not use eager whole-chunk readers", %{
      writer: writer,
      opts: opts
    } do
      long_word = String.duplicate("0", 100)

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(10, 0), "first", :insert, ~s|{"test":"#{long_word}1"}|},
            {LogOffset.new(11, 0), "second", :insert, ~s|{"test":"#{long_word}2"}|},
            {LogOffset.new(12, 0), "third", :insert, ~s|{"test":"#{long_word}3"}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)

      Repatch.patch(LogFile, :stream_jsons, fn
        _reader_opts, _path, _start_position, _end_position, _min_offset, _project_item ->
          flunk("exact replay used the eager whole-chunk reader")
      end)

      Repatch.patch(LogFile, :stream_jsons_until_offset_from_open_file, fn
        _reader_opts, _path, _start_position, _min_offset, _max_offset, _project_item ->
          flunk("exact replay used the legacy single-file resource")
      end)

      stream =
        PureFileStorage.get_log_stream_with_offsets(
          LogOffset.new(9, 0),
          LogOffset.new(12, 0),
          opts
        )

      first_offset = LogOffset.new(10, 0)
      assert [{^first_offset, _first_json}] = Enum.take(stream, 1)
    end

    @tag chunk_size: 100
    test "an exact stream returns empty when the shape is removed before enumeration", %{
      writer: writer,
      opts: opts,
      base_opts: base_opts
    } do
      long_word = String.duplicate("0", 100)
      first_offset = LogOffset.new(10, 0)
      second_offset = LogOffset.new(11, 0)
      third_offset = LogOffset.new(12, 0)
      first_json = ~s|{"test":"#{long_word}1"}|
      second_json = ~s|{"test":"#{long_word}2"}|
      third_json = ~s|{"test":"#{long_word}3"}|

      writer =
        PureFileStorage.append_to_log!(
          [
            {first_offset, "first", :insert, first_json},
            {second_offset, "second", :insert, second_json},
            {third_offset, "third", :insert, third_json}
          ],
          writer
        )

      PureFileStorage.terminate(writer)

      stream =
        PureFileStorage.get_log_stream_with_offsets(
          LogOffset.new(9, 0),
          third_offset,
          opts
        )

      File.rename!(
        PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle),
        PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle <> "-deleted")
      )

      assert Enum.to_list(stream) == []
    end

    @tag chunk_size: 100
    test "a captured multi-chunk exact replay can be enumerated more than once", %{
      writer: writer,
      opts: opts
    } do
      long_word = String.duplicate("0", 100)
      first_offset = LogOffset.new(10, 0)
      last_offset = LogOffset.new(12, 0)
      first_json = ~s|{"test":"#{long_word}1"}|

      writer =
        PureFileStorage.append_to_log!(
          [
            {first_offset, "first", :insert, first_json},
            {LogOffset.new(11, 0), "second", :insert, ~s|{"test":"#{long_word}2"}|},
            {last_offset, "third", :insert, ~s|{"test":"#{long_word}3"}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)

      stream =
        PureFileStorage.get_log_stream_with_offsets(
          LogOffset.new(9, 0),
          last_offset,
          opts
        )

      assert Enum.take(stream, 1) == [{first_offset, first_json}]
      assert length(Enum.to_list(stream)) == 3
    end

    @tag chunk_size: 100
    test "correctly finds a chunk to read from", %{writer: writer, opts: opts} do
      long_word = String.duplicate("0", 100)

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(10, 0), "test_key", :insert, ~s|{"test":"#{long_word}1"}|},
            {LogOffset.new(11, 0), "test_key", :update, ~s|{"test":"#{long_word}2"}|},
            {LogOffset.new(12, 0), "test_key", :delete, ~s|{"test":"#{long_word}3"}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)

      assert PureFileStorage.ChunkIndex.read_chunk_file(
               PureFileStorage.chunk_file(opts, PureFileStorage.latest_name(opts))
             ) == [
               {{LogOffset.new(10, 0), LogOffset.new(10, 0)}, {0, 150}, {0, 0}},
               {{LogOffset.new(11, 0), LogOffset.new(11, 0)}, {150, 300}, {0, 0}},
               {{LogOffset.new(12, 0), LogOffset.new(12, 0)}, {300, 450}, {0, 0}}
             ]

      assert PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.new(10, 0), opts)
             |> Enum.to_list() == [~s|{"test":"#{long_word}1"}|]

      assert PureFileStorage.get_log_stream(LogOffset.new(10, 0), LogOffset.new(11, 0), opts)
             |> Enum.to_list() == [~s|{"test":"#{long_word}2"}|]

      assert PureFileStorage.get_log_stream(LogOffset.new(11, 0), LogOffset.new(12, 0), opts)
             |> Enum.to_list() == [~s|{"test":"#{long_word}3"}|]
    end

    test "a stream returns empty when the shape is removed before enumeration", %{
      writer: writer,
      opts: opts,
      base_opts: base_opts
    } do
      long_word = String.duplicate("0", 100)

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(10, 0), "test_key", :insert, ~s|{"test":"#{long_word}1"}|},
            {LogOffset.new(11, 0), "test_key", :update, ~s|{"test":"#{long_word}2"}|},
            {LogOffset.new(12, 0), "test_key", :delete, ~s|{"test":"#{long_word}3"}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)

      stream = PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.last(), opts)

      File.rename!(
        PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle),
        PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle <> "-deleted")
      )

      assert Enum.to_list(stream) == []
    end

    test "a captured partial-chunk stream can be enumerated more than once", %{
      writer: writer,
      opts: opts
    } do
      first_json = ~S|{"test":"first"}|
      second_json = ~S|{"test":"second"}|

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(10, 0), "first", :insert, first_json},
            {LogOffset.new(11, 0), "second", :insert, second_json}
          ],
          writer
        )

      PureFileStorage.terminate(writer)

      stream = PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.last(), opts)

      assert Enum.take(stream, 1) == [first_json]
      assert Enum.to_list(stream) == [first_json, second_json]
    end

    test "returns an empty stream if the files have been deleted", %{writer: writer, opts: opts} do
      long_word = String.duplicate("0", 100)

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(10, 0), "test_key", :insert, ~s|{"test":"#{long_word}1"}|},
            {LogOffset.new(11, 0), "test_key", :update, ~s|{"test":"#{long_word}2"}|},
            {LogOffset.new(12, 0), "test_key", :delete, ~s|{"test":"#{long_word}3"}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)

      shape_data_dir = PureFileStorage.shape_data_dir(opts)

      File.rm_rf!(shape_data_dir)

      assert [] =
               PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.last(), opts)
               |> Enum.to_list()
    end

    test "returns an empty stream if the shape has been deleted", %{writer: writer, opts: opts} do
      long_word = String.duplicate("0", 100)

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(10, 0), "test_key", :insert, ~s|{"test":"#{long_word}1"}|},
            {LogOffset.new(11, 0), "test_key", :update, ~s|{"test":"#{long_word}2"}|},
            {LogOffset.new(12, 0), "test_key", :delete, ~s|{"test":"#{long_word}3"}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)
      PureFileStorage.cleanup!(opts)

      assert [] =
               PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.last(), opts)
               |> Enum.to_list()
    end

    test "raises if the files are gone but the shape still exists", %{writer: writer, opts: opts} do
      long_word = String.duplicate("0", 100)

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(10, 0), "test_key", :insert, ~s|{"test":"#{long_word}1"}|},
            {LogOffset.new(11, 0), "test_key", :update, ~s|{"test":"#{long_word}2"}|},
            {LogOffset.new(12, 0), "test_key", :delete, ~s|{"test":"#{long_word}3"}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)

      json_file = PureFileStorage.json_file(opts, "latest.0")

      File.rm!(json_file)

      assert_raise File.Error, fn ->
        PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.last(), opts)
        |> Enum.to_list()
      end
    end

    test "returns empty stream if some files deleted and the shape has gone", %{
      writer: writer,
      opts: opts,
      base_opts: base_opts
    } do
      long_word = String.duplicate("0", 100)

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(10, 0), "test_key", :insert, ~s|{"test":"#{long_word}1"}|},
            {LogOffset.new(11, 0), "test_key", :update, ~s|{"test":"#{long_word}2"}|},
            {LogOffset.new(12, 0), "test_key", :delete, ~s|{"test":"#{long_word}3"}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)

      File.rename!(
        PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle),
        PureFileStorage.shape_data_dir(base_opts.base_path, @shape_handle <> "-deleted")
      )

      assert [] =
               PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.last(), opts)
               |> Enum.to_list()
    end

    test "correctly skips over lines when max offset is less than the one written", %{
      writer: writer,
      opts: opts
    } do
      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(10, 0), "test_key", :insert, ~s|{"test":"1"}|},
            {LogOffset.new(12, 0), "test_key", :update, ~s|{"test":"2"}|},
            {LogOffset.new(14, 0), "test_key", :delete, ~s|{"test":"3"}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)

      assert PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.new(11, 0), opts)
             |> Enum.to_list() == [~s|{"test":"1"}|]
    end
  end

  describe "crash recovery" do
    # These tests make use of known log file structures to test that the storage recovers correctly
    # If underlying file structure changes, these tests will need to be updated.
    setup [:start_storage, :with_started_writer]

    setup %{writer: writer, opts: opts} = ctx do
      if Map.get(ctx, :init_log, true) do
        writer =
          PureFileStorage.append_to_log!(
            [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|}],
            writer
          )

        PureFileStorage.terminate(writer)

        assert PureFileStorage.fetch_latest_offset(opts) == {:ok, LogOffset.new(10, 0)}

        assert PureFileStorage.fetch_pg_snapshot(opts) == {:ok, %{xmin: 100}}
      end

      :ok
    end

    test "incomplete transaction write before crash is discarded", %{opts: opts} do
      # Transaction got partially flushed, but not "closed" i.e. persisted boundary wasn't updated
      File.open!(
        PureFileStorage.json_file(opts, PureFileStorage.latest_name(opts)),
        [:append, :raw],
        fn file ->
          json = Jason.encode!(%{test: 2})

          IO.binwrite(
            file,
            <<LogOffset.to_int128(LogOffset.new(11, 0))::binary, 4::32, "test"::binary, ?i::8,
              0::8, byte_size(json)::64, json::binary>>
          )
        end
      )

      writer = PureFileStorage.init_writer!(opts, @shape)

      assert PureFileStorage.fetch_latest_offset(opts) ==
               {:ok, LogOffset.new(10, 0)}

      assert PureFileStorage.fetch_pg_snapshot(opts) ==
               {:ok, %{xmin: 100}}

      # After recovery we see the same line
      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(11, 0), "test", :insert, ~S|{"test":2}|},
            {LogOffset.new(12, 0), "test", :insert, ~S|{"test":3}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)

      assert [~S|{"test":1}|, ~S|{"test":2}|, ~S|{"test":3}|] =
               PureFileStorage.get_log_stream(
                 LogOffset.last_before_real_offsets(),
                 LogOffset.last(),
                 opts
               )
               |> Enum.to_list()
    end

    test "chunk boundary without an actual write is trimmed", %{opts: opts} do
      # Transaction got partially flushed, but not "closed" i.e. persisted boundary wasn't updated
      File.open!(
        PureFileStorage.chunk_file(opts, PureFileStorage.latest_name(opts)),
        [:append, :raw],
        fn file ->
          IO.binwrite(
            file,
            <<LogOffset.to_int128(LogOffset.new(20, 0))::binary, 100::64, 100::64>>
          )
        end
      )

      # And a partial write cut midline just for good measure
      File.open!(
        PureFileStorage.json_file(opts, PureFileStorage.latest_name(opts)),
        [:append, :raw],
        fn file ->
          IO.binwrite(
            file,
            <<LogOffset.to_int128(LogOffset.new(20, 0))::binary, 4::32, "test"::binary, ?i::8,
              0::8, 0::32>>
          )
        end
      )

      assert PureFileStorage.ChunkIndex.read_chunk_file(
               PureFileStorage.chunk_file(opts, PureFileStorage.latest_name(opts))
             ) == [
               {{LogOffset.new(10, 0), LogOffset.new(20, 0)}, {0, 100}, {0, 100}}
             ]

      writer = PureFileStorage.init_writer!(opts, @shape)

      assert PureFileStorage.fetch_latest_offset(opts) ==
               {:ok, LogOffset.new(10, 0)}

      assert PureFileStorage.fetch_pg_snapshot(opts) ==
               {:ok, %{xmin: 100}}

      # After recovery we see the same line
      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(11, 0), "test", :insert, ~S|{"test":2}|},
            {LogOffset.new(12, 0), "test", :insert, ~S|{"test":3}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)
      assert PureFileStorage.get_chunk_end_log_offset(LogOffset.new(10, 0), opts) == nil

      assert [~S|{"test":1}|, ~S|{"test":2}|, ~S|{"test":3}|] =
               PureFileStorage.get_log_stream(
                 LogOffset.last_before_real_offsets(),
                 LogOffset.last(),
                 opts
               )
               |> Enum.to_list()

      assert PureFileStorage.ChunkIndex.read_chunk_file(
               PureFileStorage.chunk_file(opts, PureFileStorage.latest_name(opts))
             ) == [
               {{LogOffset.new(10, 0), nil}, {0, nil}, {0, nil}}
             ]
    end

    @tag init_log: false
    test "correctly handles incomplete chunks as part of the recovery", %{opts: opts} do
      path = PureFileStorage.chunk_file(opts, PureFileStorage.latest_name(opts))

      File.mkdir_p!(Path.dirname(path))

      File.open!(
        path,
        [:append, :raw],
        fn file ->
          IO.binwrite(
            file,
            <<LogOffset.to_int128(LogOffset.new(20, 0))::binary, 100::64, 100::64>>
          )
        end
      )

      writer = PureFileStorage.init_writer!(opts, @shape)

      assert PureFileStorage.fetch_latest_offset(opts) ==
               {:ok, LogOffset.new(0, 0)}

      assert PureFileStorage.fetch_pg_snapshot(opts) ==
               {:ok, %{xmin: 100}}

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(11, 0), "test", :insert, ~S|{"test":2}|},
            {LogOffset.new(12, 0), "test", :insert, ~S|{"test":3}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)
      assert PureFileStorage.get_chunk_end_log_offset(LogOffset.new(10, 0), opts) == nil

      assert [~S|{"test":2}|, ~S|{"test":3}|] =
               PureFileStorage.get_log_stream(
                 LogOffset.last_before_real_offsets(),
                 LogOffset.last(),
                 opts
               )
               |> Enum.to_list()

      assert PureFileStorage.ChunkIndex.read_chunk_file(
               PureFileStorage.chunk_file(opts, PureFileStorage.latest_name(opts))
             ) == [
               {{LogOffset.new(11, 0), nil}, {0, nil}, {0, nil}}
             ]
    end
  end

  describe "chunk writes - " do
    setup [:start_storage, :with_started_writer]

    @tag chunk_size: 11
    test "chunk size is counted by JSON size and not full entry size", %{
      writer: writer,
      opts: opts
    } do
      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|}],
          writer
        )

      PureFileStorage.terminate(writer)

      # Chunk shoudn't be closed, because byte_size(~S|{"test":1}|) == 10 < 11
      refute PureFileStorage.get_chunk_end_log_offset(LogOffset.new(9, 0), opts) ==
               LogOffset.new(10, 0)
    end
  end

  describe "resumption" do
    setup [:start_storage]

    @tag chunk_size: 30
    test "correctly continues a chunk after a reboot", %{opts: opts} do
      %{writer: writer} = with_started_writer(%{opts: opts})

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|}],
          writer
        )

      PureFileStorage.terminate(writer)

      assert PureFileStorage.get_chunk_end_log_offset(LogOffset.new(10, 0), opts) == nil

      writer = PureFileStorage.init_writer!(opts, @shape)

      writer =
        PureFileStorage.append_to_log!(
          [
            {LogOffset.new(11, 0), "test_key", :insert, ~S|{"test":2}|},
            {LogOffset.new(12, 0), "test_key", :insert, ~S|{"test":3}|},
            {LogOffset.new(13, 0), "test_key", :insert, ~S|{"test":4}|}
          ],
          writer
        )

      PureFileStorage.terminate(writer)

      assert PureFileStorage.get_chunk_end_log_offset(LogOffset.new(10, 0), opts) ==
               LogOffset.new(11, 0)

      assert PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.new(13, 0), opts)
             |> Enum.to_list() == [~S|{"test":1}|, ~S|{"test":2}|, ~S|{"test":3}|, ~S|{"test":4}|]
    end

    test "get_chunk_end_log_offset/2 returns nil when no chunk file is found", %{
      base_opts: base_opts,
      opts: opts
    } do
      chunk_index_path =
        Path.join([base_opts.base_path, @shape_handle, "log", "log.latest.0.chunk.bin"])

      refute File.exists?(chunk_index_path)

      assert nil == PureFileStorage.get_chunk_end_log_offset(LogOffset.new(1, 0), opts)
    end
  end

  describe "flush timer" do
    setup [:start_storage, :with_started_writer]
    @describetag flush_period: 100

    test "flush message arrives after flush period", %{writer: writer} do
      PureFileStorage.append_to_log!(
        [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|}],
        writer
      )

      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [0]}}
    end

    @tag flush_period: 100
    test "multiple writes cause only one flush message", %{writer: writer} do
      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|}],
          writer
        )

      PureFileStorage.append_to_log!(
        [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|}],
        writer
      )

      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [0]}}
      refute_receive {Storage, {PureFileStorage, :perform_scheduled_flush, _}}, 200
    end

    @tag flush_period: 50
    test "state after flush is correct", %{writer: writer} do
      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|}],
          writer
        )

      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [0]}}

      writer = PureFileStorage.perform_scheduled_flush(writer, 0)

      PureFileStorage.append_to_log!(
        [{LogOffset.new(11, 0), "test_key", :insert, ~S|{"test":1}|}],
        writer
      )

      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [1]}}
    end

    @tag flush_period: 50
    test "hibernate with empty buffer doesn't schedule a flush", %{writer: writer} do
      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|}],
          writer
        )

      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [0]}}

      writer = PureFileStorage.perform_scheduled_flush(writer, 0)

      PureFileStorage.append_to_log!(
        [{LogOffset.new(11, 0), "test_key", :insert, ~S|{"test":1}|}],
        writer
      )

      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [1]}}
      assert_receive {Storage, :flushed, _last_seen_offset}, 200
      refute_receive {Storage, {PureFileStorage, :perform_scheduled_flush, _}}, 200

      _writer = PureFileStorage.hibernate(writer)

      refute_receive {Storage, :flushed, _last_seen_offset}, 200
    end

    @flush_alignment_bytes 64 * 1024
    @tag flush_period: 100
    test "should run scheduled flush after empty buffer alignment", %{writer: writer} do
      large_data = String.duplicate("x", @flush_alignment_bytes + 1000)

      # write small piece of data to trigger scheduling of flush
      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|}],
          writer
        )

      # write large piece of data that goes over buffer limit and forces a flush
      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(11, 0), "test_key", :insert, ~s|{"test":"#{large_data}"}|}],
          writer
        )

      # next small piece of data should schedule new flush with larger flush counter
      PureFileStorage.append_to_log!(
        [{LogOffset.new(12, 0), "test_key", :insert, ~S|{"test":1}|}],
        writer
      )

      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [times_flushed]}}
      assert times_flushed > 0
      refute_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [_]}}
    end
  end

  describe "schedule_compaction/1" do
    setup [:start_storage]

    test "sends a message to the calling process within the predefined time period", ctx do
      compaction_config = Map.put(ctx.base_opts.compaction_config, :period, 5)
      PureFileStorage.schedule_compaction(compaction_config)

      assert_receive {Storage, {PureFileStorage, :scheduled_compaction, [^compaction_config]}},
                     500
    end
  end

  describe "hibernation" do
    setup [:start_storage]

    test "correctly continues writing after hibernation", %{opts: opts} do
      %{writer: writer} = with_started_writer(%{opts: opts})

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test":1}|}],
          writer
        )

      writer = PureFileStorage.hibernate(writer)

      assert PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.new(13, 0), opts)
             |> Enum.to_list() == [~S|{"test":1}|]

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(11, 0), "test_key", :insert, ~S|{"test":2}|}],
          writer
        )

      PureFileStorage.terminate(writer)

      assert PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.new(12, 0), opts)
             |> Enum.to_list() == [~S|{"test":1}|, ~S|{"test":2}|]
    end
  end

  describe "ETS read/write race condition" do
    setup [:start_storage, :with_started_writer]
    @describetag flush_period: 50

    test "reader falls back to disk when ETS is empty due to concurrent flush", %{
      writer: writer,
      opts: opts,
      stack_id: stack_id
    } do
      import Electric.ShapeCache.PureFileStorage.SharedRecords

      # Write data - goes to ETS buffer
      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test": 1}|}],
          writer
        )

      # Wait for and trigger flush - data now on disk, ETS cleared
      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [0]}}
      writer = PureFileStorage.perform_scheduled_flush(writer, 0)

      # Get the fresh metadata after flush
      stack_ets = PureFileStorage.stack_ets(stack_id)
      [fresh_meta] = :ets.lookup(stack_ets, @shape_handle)

      assert storage_meta(last_persisted_offset: fresh_last_persisted, ets_table: ets_ref) =
               fresh_meta

      assert fresh_last_persisted == LogOffset.new(10, 0)
      assert :ets.info(ets_ref, :size) == 0

      # Create stale metadata - same as fresh but with old last_persisted
      # This simulates what a reader would see if it read metadata BEFORE the flush
      stale_meta =
        storage_meta(fresh_meta,
          last_persisted_offset: LogOffset.last_before_real_offsets(),
          # Also set last_seen so upper_read_bound covers our data
          last_seen_txn_offset: LogOffset.new(10, 0)
        )

      # Insert stale metadata - reader will think data is in ETS
      :ets.insert(stack_ets, stale_meta)

      # Reader sees stale metadata (last_persisted = before_real), tries ETS, gets empty.
      # Fix detects empty ETS and falls back to disk using upper_read_bound.
      result =
        PureFileStorage.get_log_stream(
          LogOffset.new(0, 0),
          LogOffset.last(),
          opts
        )
        |> Enum.to_list()

      assert result == [~S|{"test": 1}|],
             "Reader should fall back to disk when ETS is empty"

      PureFileStorage.terminate(writer)
    end

    test "reader falls back to disk when ETS returns partial data due to concurrent flush", %{
      writer: writer,
      opts: opts,
      stack_id: stack_id
    } do
      import Electric.ShapeCache.PureFileStorage.SharedRecords

      # Write multiple entries - all go to ETS buffer
      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test": 1}|}],
          writer
        )

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(11, 0), "test_key", :insert, ~S|{"test": 2}|}],
          writer
        )

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(12, 0), "test_key", :insert, ~S|{"test": 3}|}],
          writer
        )

      # Wait for and trigger flush - all data now on disk, ETS cleared
      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [0]}}
      writer = PureFileStorage.perform_scheduled_flush(writer, 0)

      # Get the fresh metadata after flush
      stack_ets = PureFileStorage.stack_ets(stack_id)
      [fresh_meta] = :ets.lookup(stack_ets, @shape_handle)

      assert storage_meta(last_persisted_offset: fresh_last_persisted, ets_table: ets_ref) =
               fresh_meta

      assert fresh_last_persisted == LogOffset.new(12, 0)
      assert :ets.info(ets_ref, :size) == 0

      # Simulate partial ETS state: put only SOME entries back in ETS
      # This simulates what a reader would see if ETS was cleared mid-iteration
      :ets.insert(ets_ref, {LogOffset.to_tuple(LogOffset.new(10, 0)), ~S|{"test": 1}|})
      # Entry at offset 11 and 12 are "missing" - simulating they were cleared mid-read

      # Create stale metadata - reader thinks all data should be in ETS
      stale_meta =
        storage_meta(fresh_meta,
          last_persisted_offset: LogOffset.last_before_real_offsets(),
          # Set last_seen so upper_read_bound covers all our data
          last_seen_txn_offset: LogOffset.new(12, 0)
        )

      # Insert stale metadata
      :ets.insert(stack_ets, stale_meta)

      # Reader sees stale metadata, tries ETS, gets only entry at offset 10.
      # Fix detects partial read (last_offset < upper_read_bound) and falls back to disk.
      result =
        PureFileStorage.get_log_stream(
          LogOffset.new(0, 0),
          LogOffset.last(),
          opts
        )
        |> Enum.to_list()

      assert result == [~S|{"test": 1}|, ~S|{"test": 2}|, ~S|{"test": 3}|],
             "Reader should fall back to disk when ETS returns partial data"

      PureFileStorage.terminate(writer)
    end

    test "reader falls back to disk when ETS table is deleted (pure ETS path)", %{
      writer: writer,
      opts: opts,
      stack_id: stack_id
    } do
      import Electric.ShapeCache.PureFileStorage.SharedRecords

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test": 1}|}],
          writer
        )

      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [0]}}
      _writer = PureFileStorage.perform_scheduled_flush(writer, 0)

      stack_ets = PureFileStorage.stack_ets(stack_id)
      [fresh_meta] = :ets.lookup(stack_ets, @shape_handle)
      assert storage_meta(ets_table: ets_ref) = fresh_meta

      # Stale last_persisted <= min_offset forces the pure ETS branch in stream_main_log
      stale_meta =
        storage_meta(fresh_meta,
          last_persisted_offset: LogOffset.last_before_real_offsets(),
          last_seen_txn_offset: LogOffset.new(10, 0)
        )

      :ets.insert(stack_ets, stale_meta)
      :ets.delete(ets_ref)

      result =
        PureFileStorage.get_log_stream(
          LogOffset.new(5, 0),
          LogOffset.last(),
          opts
        )
        |> Enum.to_list()

      assert result == [~S|{"test": 1}|]

      PureFileStorage.cleanup!(opts)
    end

    test "reader falls back to disk when ETS table is deleted (mixed disk + ETS path)", %{
      writer: writer,
      opts: opts,
      stack_id: stack_id
    } do
      import Electric.ShapeCache.PureFileStorage.SharedRecords

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"test": 1}|}],
          writer
        )

      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [0]}}
      writer = PureFileStorage.perform_scheduled_flush(writer, 0)

      writer =
        PureFileStorage.append_to_log!(
          [{LogOffset.new(11, 0), "test_key2", :insert, ~S|{"test": 2}|}],
          writer
        )

      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [1]}}
      _writer = PureFileStorage.perform_scheduled_flush(writer, 1)

      stack_ets = PureFileStorage.stack_ets(stack_id)
      [fresh_meta] = :ets.lookup(stack_ets, @shape_handle)
      assert storage_meta(ets_table: ets_ref) = fresh_meta

      # Stale last_persisted between the two offsets forces the mixed disk+ETS branch
      stale_meta =
        storage_meta(fresh_meta,
          last_persisted_offset: LogOffset.new(10, 0),
          last_seen_txn_offset: LogOffset.new(11, 0)
        )

      :ets.insert(stack_ets, stale_meta)
      :ets.delete(ets_ref)

      result =
        PureFileStorage.get_log_stream(
          LogOffset.new(0, 0),
          LogOffset.last(),
          opts
        )
        |> Enum.to_list()

      assert result == [~S|{"test": 1}|, ~S|{"test": 2}|]

      PureFileStorage.cleanup!(opts)
    end
  end

  describe "remove_unnested_storage/1" do
    test "removes un-nested storage directories but leaves the nested ones", ctx do
      base_opts =
        PureFileStorage.shared_opts(
          stack_id: ctx.stack_id,
          storage_dir: ctx.tmp_dir,
          chunk_bytes_threshold: ctx[:chunk_size] || 10 * 1024 * 1024,
          flush_period: 1000
        )

      nested_dirs = [
        PureFileStorage.shape_data_dir(base_opts.base_path, "128584483-1770721672609826"),
        PureFileStorage.shape_data_dir(base_opts.base_path, "35237783-1770721660697706")
      ]

      unnested_dirs = [
        Path.join(base_opts.base_path, "128584483-1770721672609826"),
        Path.join(base_opts.base_path, "35237783-1770721660697706")
      ]

      for dir <- nested_dirs ++ unnested_dirs do
        File.mkdir_p!(dir)
      end

      storage_base = {PureFileStorage, base_opts}
      start_link_supervised!(Storage.stack_child_spec(storage_base))
      assert validate_dir_cleanup(nested_dirs, unnested_dirs)
    end

    defp validate_dir_cleanup(nested_dirs, unnested_dirs, n \\ 50)

    defp validate_dir_cleanup(nested_dirs, unnested_dirs, 0) do
      validate_dir_required_state?(nested_dirs, unnested_dirs)
    end

    defp validate_dir_cleanup(nested_dirs, unnested_dirs, n) do
      if validate_dir_required_state?(nested_dirs, unnested_dirs) do
        true
      else
        Process.sleep(10)
        validate_dir_cleanup(nested_dirs, unnested_dirs, n - 1)
      end
    end

    defp validate_dir_required_state?(nested_dirs, unnested_dirs) do
      Enum.concat(
        Enum.map(nested_dirs, fn path ->
          File.dir?(path)
        end),
        Enum.map(unnested_dirs, fn path ->
          not File.dir?(path)
        end)
      )
      |> Enum.all?()
    end
  end

  describe "append_fragment_to_log!()" do
    @describetag flush_period: 10

    setup [:start_storage, :with_started_writer]

    test "writes items to the log without assuming they add up to a complete transaction",
         %{opts: opts, writer: writer} do
      # Verify the initial state of storage
      assert {:ok, LogOffset.new(0, 0)} == PureFileStorage.fetch_latest_offset(opts)

      last_before_real_offset = LogOffset.last_before_real_offsets()

      assert %{
               last_persisted_offset: ^last_before_real_offset,
               last_seen_txn_offset: ^last_before_real_offset,
               last_persisted_txn_offset: ^last_before_real_offset
             } = storage_internal_state(opts)

      # Write a couple of txn fragments to the shape log.
      # For every fragment we verify that storage doesn't consider to have stored a complete transaction
      Enum.each(@fragments, fn fragment ->
        log_items = changes_to_log_items(fragment.changes, xid: @xid)
        writer = PureFileStorage.append_fragment_to_log!(log_items, writer)
        assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [0]}}

        # Since storage code isn't executed inside a Consumer process here, we have to call the function ourselves
        # to update the internal state of storage.
        PureFileStorage.perform_scheduled_flush(writer, 0)

        # Last persisted offset advances as each new txn fragment gets written.
        # Transaction offsets remain virtual since we've only written a txn fragment and not a full txn.
        offset = fragment.last_log_offset

        assert %{
                 last_persisted_offset: ^offset,
                 last_seen_txn_offset: ^last_before_real_offset,
                 last_persisted_txn_offset: ^last_before_real_offset
               } = storage_internal_state(opts)

        assert {:ok, LogOffset.new(0, 0)} == PureFileStorage.fetch_latest_offset(opts)

        assert [] == get_log_items_from_storage(LogOffset.first(), LogOffset.last(), opts)
      end)
    end

    @tag chunk_size: 10 * 1024
    test "active and read-only chunk reads hide a flushed fragment in a completed chunk", %{
      opts: opts,
      writer: writer
    } do
      baseline_offset = LogOffset.new(10, 0)
      fragment_offset = LogOffset.new(11, 0)
      baseline_json = ~S|{"value":"baseline"}|
      fragment_json = Jason.encode!(%{value: String.duplicate("x", 70 * 1024)})

      writer =
        PureFileStorage.append_to_log!(
          [{baseline_offset, "baseline", :insert, baseline_json}],
          writer
        )

      writer = PureFileStorage.hibernate(writer)

      writer =
        PureFileStorage.append_fragment_to_log!(
          [{fragment_offset, "fragment", :insert, fragment_json}],
          writer
        )

      assert %{
               last_persisted_offset: ^fragment_offset,
               last_seen_txn_offset: ^baseline_offset,
               last_persisted_txn_offset: ^baseline_offset
             } = storage_internal_state(opts)

      chunk_file = PureFileStorage.chunk_file(opts, PureFileStorage.latest_name(opts))

      assert [{{^baseline_offset, nil}, _, _}] =
               PureFileStorage.ChunkIndex.read_chunk_file(chunk_file)

      for reader_opts <- [opts, %{opts | read_only?: true}] do
        assert PureFileStorage.fetch_latest_offset(reader_opts) == {:ok, baseline_offset}
        assert PureFileStorage.get_chunk_end_log_offset(LogOffset.first(), reader_opts) == nil

        assert [^baseline_json] =
                 PureFileStorage.get_log_stream(
                   LogOffset.first(),
                   LogOffset.last(),
                   reader_opts
                 )
                 |> Enum.to_list()
      end

      writer = PureFileStorage.signal_txn_commit!(@xid, writer)

      assert [{{^baseline_offset, ^fragment_offset}, _, _}] =
               PureFileStorage.ChunkIndex.read_chunk_file(chunk_file)

      for reader_opts <- [opts, %{opts | read_only?: true}] do
        assert PureFileStorage.fetch_latest_offset(reader_opts) == {:ok, fragment_offset}

        assert [^baseline_json, ^fragment_json] =
                 PureFileStorage.get_log_stream(
                   LogOffset.first(),
                   LogOffset.last(),
                   reader_opts
                 )
                 |> Enum.to_list()
      end

      PureFileStorage.terminate(writer)
    end

    @tag chunk_size: 10 * 1024
    test "keeps completed fragment chunks invisible to legacy readers through crash and commit",
         %{
           opts: opts,
           writer: writer
         } do
      baseline_offset = LogOffset.new(10, 0)
      fragment_offset = LogOffset.new(11, 0)
      baseline_json = ~S|{"value":"baseline"}|
      fragment_json = Jason.encode!(%{value: String.duplicate("x", 70 * 1024)})
      reader_opts = %{opts | read_only?: true}

      writer =
        PureFileStorage.append_to_log!(
          [{baseline_offset, "baseline", :insert, baseline_json}],
          writer
        )

      writer = PureFileStorage.hibernate(writer)
      chunk_file = PureFileStorage.chunk_file(opts, PureFileStorage.latest_name(opts))
      chunk_file_before_fragment = File.read!(chunk_file)

      writer =
        PureFileStorage.append_fragment_to_log!(
          [{fragment_offset, "fragment", :insert, fragment_json}],
          writer
        )

      # Version-1 readers do not clamp completed chunks to the published
      # transaction offset. The shared index must therefore remain incomplete
      # until the fragment's transaction commits.
      assert File.read!(chunk_file) == chunk_file_before_fragment
      assert legacy_whole_chunk_stream(reader_opts) == [baseline_json]

      writer = PureFileStorage.hibernate(writer)
      simulate_writer_crash(writer, opts)
      restarted_writer = PureFileStorage.init_writer!(opts, @shape)

      assert File.read!(chunk_file) == chunk_file_before_fragment
      assert PureFileStorage.fetch_latest_offset(reader_opts) == {:ok, baseline_offset}
      assert legacy_whole_chunk_stream(reader_opts) == [baseline_json]

      restarted_writer =
        PureFileStorage.append_fragment_to_log!(
          [{fragment_offset, "fragment", :insert, fragment_json}],
          restarted_writer
        )

      restarted_writer = PureFileStorage.signal_txn_commit!(@xid, restarted_writer)

      assert PureFileStorage.fetch_latest_offset(reader_opts) == {:ok, fragment_offset}
      assert legacy_whole_chunk_stream(reader_opts) == [baseline_json, fragment_json]

      PureFileStorage.terminate(restarted_writer)
    end

    @tag chunk_size: 10 * 1024
    test "keeps a complete append's chunk invisible until its cursor is published", %{
      opts: opts,
      writer: writer
    } do
      baseline_offset = LogOffset.new(10, 0)
      transaction_offset = LogOffset.new(11, 0)
      baseline_json = ~S|{"value":"baseline"}|
      transaction_json = Jason.encode!(%{value: String.duplicate("x", 70 * 1024)})
      reader_opts = %{opts | read_only?: true}

      writer =
        PureFileStorage.append_to_log!(
          [{baseline_offset, "baseline", :insert, baseline_json}],
          writer
        )

      writer = PureFileStorage.hibernate(writer)
      chunk_file = PureFileStorage.chunk_file(opts, PureFileStorage.latest_name(opts))
      chunk_file_before_transaction = File.read!(chunk_file)
      observation_key = make_ref()

      transaction_lines =
        Stream.resource(
          fn -> :emit_transaction end,
          fn
            :emit_transaction ->
              {[{transaction_offset, "transaction", :insert, transaction_json}], :observe}

            :observe ->
              observation =
                Task.async(fn ->
                  {File.read!(chunk_file), legacy_whole_chunk_stream(reader_opts)}
                end)
                |> Task.await()

              Process.put(observation_key, observation)
              {:halt, :done}
          end,
          fn _state -> :ok end
        )

      writer = PureFileStorage.append_to_log!(transaction_lines, writer)

      assert Process.get(observation_key) ==
               {chunk_file_before_transaction, [baseline_json]}

      assert PureFileStorage.fetch_latest_offset(reader_opts) == {:ok, transaction_offset}
      assert legacy_whole_chunk_stream(reader_opts) == [baseline_json, transaction_json]

      PureFileStorage.terminate(writer)
    end
  end

  describe "signal_txn_commit!()" do
    @describetag flush_period: 10

    setup [:start_storage, :with_started_writer]

    test "signals the commit boundary to the storage allowing it to advance the txn offset",
         %{opts: opts, writer: writer} do
      # Verify the initial state of storage
      assert {:ok, LogOffset.new(0, 0)} == PureFileStorage.fetch_latest_offset(opts)

      last_before_real_offset = LogOffset.last_before_real_offsets()

      assert %{
               last_persisted_offset: ^last_before_real_offset,
               last_seen_txn_offset: ^last_before_real_offset,
               last_persisted_txn_offset: ^last_before_real_offset
             } = storage_internal_state(opts)

      # Write a couple of txn fragments to the shape log.
      {writer, last_offset} =
        Enum.reduce(@fragments, {writer, nil}, fn fragment, {writer, _} ->
          log_items = changes_to_log_items(fragment.changes, xid: @xid)
          writer = PureFileStorage.append_fragment_to_log!(log_items, writer)
          {writer, fragment.last_log_offset}
        end)

      # Last persisted offset advances as each new txn fragment gets written.
      # Transaction offsets remain virtual since we've only written a txn fragment and not a full txn.
      assert_receive {Storage, {PureFileStorage, :perform_scheduled_flush, [0]}}
      writer = PureFileStorage.perform_scheduled_flush(writer, 0)

      assert %{
               last_persisted_offset: ^last_offset,
               last_seen_txn_offset: ^last_before_real_offset,
               last_persisted_txn_offset: ^last_before_real_offset
             } = storage_internal_state(opts)

      assert [] == get_log_items_from_storage(LogOffset.first(), LogOffset.last(), opts)

      # Signal the end of the transaction
      PureFileStorage.signal_txn_commit!(@xid, writer)

      assert %{
               last_persisted_offset: ^last_offset,
               last_seen_txn_offset: ^last_offset,
               last_persisted_txn_offset: ^last_offset
             } = storage_internal_state(opts)

      assert [i1, i2, i3, i4, i5] =
               get_log_items_from_storage(LogOffset.first(), LogOffset.last(), opts)

      lsn = to_string(@lsn)

      assert %{
               "headers" => %{
                 "lsn" => lsn,
                 "op_position" => 0,
                 "operation" => "insert",
                 "relation" => ["public", "test_table"],
                 "txids" => [@xid]
               },
               "key" => ~s'"public"."test_table"/"5"',
               "value" => %{"id" => "5"}
             } == i1

      assert %{
               "headers" => %{
                 "lsn" => lsn,
                 "op_position" => 2,
                 "operation" => "update",
                 "relation" => ["public", "test_table"],
                 "txids" => [@xid]
               },
               "key" => ~s'"public"."test_table"/"1"',
               "value" => %{"foo" => "bar", "id" => "1"}
             } == i2

      assert %{
               "headers" => %{
                 "lsn" => lsn,
                 "op_position" => 4,
                 "operation" => "update",
                 "relation" => ["public", "test_table"],
                 "txids" => [@xid]
               },
               "key" => ~s'"public"."test_table"/"3"',
               "value" => %{"another" => "update", "id" => "3"}
             } == i3

      assert %{
               "headers" => %{
                 "lsn" => lsn,
                 "op_position" => 6,
                 "operation" => "insert",
                 "relation" => ["public", "test_table"],
                 "txids" => [@xid]
               },
               "key" => ~s'"public"."test_table"/"6"',
               "value" => %{"id" => "6"}
             } == i4

      assert %{
               "headers" => %{
                 "lsn" => lsn,
                 "op_position" => 8,
                 "operation" => "delete",
                 "relation" => ["public", "test_table"],
                 "txids" => [@xid],
                 "last" => true
               },
               "key" => ~s'"public"."test_table"/"2"',
               "value" => %{"id" => "2"}
             } == i5
    end
  end

  describe "log replay history" do
    setup [:start_storage, :with_started_writer]

    test "upgrades a legacy log at its current durable cursor and keeps that baseline", %{
      opts: opts,
      writer: writer
    } do
      legacy_cursor = LogOffset.new(10, 0)
      new_cursor = LogOffset.new(11, 0)

      writer =
        PureFileStorage.append_to_log!(
          [{legacy_cursor, "legacy", :insert, ~S|{"value":"legacy"}|}],
          writer
        )

      writer = PureFileStorage.hibernate(writer)
      File.rm!(metadata_path(opts, :log_replay_history_start))
      simulate_writer_crash(writer, opts)
      writer = PureFileStorage.init_writer!(opts, @shape)

      assert PureFileStorage.get_log_replay_safe_cursor(opts) == legacy_cursor

      writer =
        PureFileStorage.append_to_log!(
          [{new_cursor, "new", :insert, ~S|{"value":"new"}|}],
          writer
        )

      writer = PureFileStorage.hibernate(writer)
      simulate_writer_crash(writer, opts)
      restarted_writer = PureFileStorage.init_writer!(opts, @shape)

      assert PureFileStorage.get_log_replay_safe_cursor(opts) == legacy_cursor

      PureFileStorage.terminate(restarted_writer)
    end
  end

  describe "storage format fencing" do
    setup [:start_storage, :with_started_writer]

    test "upgrades stable legacy generation metadata before serving reads", %{
      opts: opts,
      writer: writer
    } do
      offset = LogOffset.new(10, 0)

      writer =
        PureFileStorage.append_to_log!(
          [{offset, "legacy", :insert, ~S|{"value":"legacy"}|}],
          writer
        )

      writer = PureFileStorage.hibernate(writer)
      expected_latest_name = PureFileStorage.latest_name(opts)
      expected_compaction_boundary = PureFileStorage.compaction_boundary(opts)

      # Model a cache created by the immediately preceding storage format,
      # which published the two legacy metadata files but had no combined
      # durable generation record yet.
      File.rm!(metadata_path(opts, :read_generation))
      simulate_writer_crash(writer, opts)
      restarted_writer = PureFileStorage.init_writer!(opts, @shape)

      assert %{
               version: 1,
               latest_name: ^expected_latest_name,
               compaction_boundary: ^expected_compaction_boundary
             } = metadata_on_disk(opts, :read_generation)

      assert [~S|{"value":"legacy"}|] ==
               PureFileStorage.get_log_stream(LogOffset.first(), LogOffset.last(), opts)
               |> Enum.to_list()

      PureFileStorage.terminate(restarted_writer)
    end

    test "read-only readers fail closed on an invalid durable read generation", %{
      opts: opts,
      writer: writer
    } do
      write_legacy_metadata!(opts, :read_generation, %{
        version: 999,
        latest_name: "latest.0",
        compaction_boundary: {LogOffset.before_all(), nil}
      })

      assert_raise Storage.Error, ~r/Invalid durable read generation/, fn ->
        PureFileStorage.get_log_stream(
          LogOffset.first(),
          LogOffset.last(),
          %{opts | read_only?: true}
        )
        |> Enum.to_list()
      end

      PureFileStorage.terminate(writer)
    end

    test "makes a version-1 rollback reject a cache written with the new durability contract", %{
      opts: opts,
      writer: writer
    } do
      assert PureFileStorage.snapshot_started?(opts)
      assert 2 == metadata_on_disk(opts, :version)

      PureFileStorage.terminate(writer)

      rollback_opts = %{opts | version: 1}
      rollback_writer = PureFileStorage.init_writer!(rollback_opts, @shape)

      refute PureFileStorage.snapshot_started?(rollback_opts)

      PureFileStorage.terminate(rollback_writer)
    end

    test "rebuilds a version-1 cache before reading a stale combined transaction cursor", %{
      opts: opts,
      writer: writer
    } do
      baseline_offset = LogOffset.new(10, 0)
      legacy_offset = LogOffset.new(20, 0)
      positions = %{"dependency" => LogOffset.new(100, 0)}

      writer =
        PureFileStorage.append_to_log!(
          [{baseline_offset, "baseline", :insert, ~S|{"value":"baseline"}|}],
          writer
        )

      :ok = PureFileStorage.set_move_positions!(positions, opts)

      writer =
        PureFileStorage.append_to_log!(
          [{legacy_offset, "legacy", :insert, ~S|{"value":"legacy"}|}],
          writer
        )

      writer = PureFileStorage.hibernate(writer)

      assert 2 == metadata_on_disk(opts, :version)

      # Model a rollback writer that advanced only the legacy cursor while a
      # now-stale combined transaction-state file remained in the directory.
      write_legacy_metadata!(opts, :version, 1)
      write_legacy_metadata!(opts, :last_persisted_txn_offset, legacy_offset)

      write_legacy_metadata!(opts, :transaction_state, %{
        transaction_state_on_disk(opts)
        | last_persisted_txn_offset: baseline_offset
      })

      simulate_writer_crash(writer, opts)
      restarted_writer = PureFileStorage.init_writer!(opts, @shape)

      refute PureFileStorage.snapshot_started?(opts)

      assert PureFileStorage.fetch_latest_offset(opts) ==
               {:ok, LogOffset.last_before_real_offsets()}

      assert PureFileStorage.fetch_move_positions(opts) == {:ok, %{}}
      refute File.exists?(metadata_path(opts, :transaction_state))
      assert 2 == metadata_on_disk(opts, :version)

      PureFileStorage.terminate(restarted_writer)
    end
  end

  describe "compaction attempt lifecycle" do
    setup [:start_storage, :with_started_writer]

    @tag chunk_size: 1
    test "read-only readers use one durable generation instead of torn legacy metadata", %{
      opts: opts,
      writer: writer
    } do
      {writer, baseline_offset} = append_compaction_candidate(writer)
      assert :ok = PureFileStorage.compact(opts, 0)

      assert_receive {Storage,
                      {PureFileStorage, :handle_compaction_finished,
                       [^baseline_offset, compacted_suffix, log_file_pos]}},
                     5_000

      writer =
        PureFileStorage.handle_compaction_finished(
          writer,
          baseline_offset,
          compacted_suffix,
          log_file_pos
        )

      latest_name = PureFileStorage.latest_name(opts)
      compaction_boundary = PureFileStorage.compaction_boundary(opts)

      write_legacy_metadata!(opts, :read_generation, %{
        version: 1,
        latest_name: latest_name,
        compaction_boundary: compaction_boundary
      })

      # Model a read-only process that read the old boundary immediately before
      # the writer published the new latest-name file. The combined generation
      # remains authoritative even though the legacy files now form a torn pair.
      write_legacy_metadata!(
        opts,
        :compaction_boundary,
        {LogOffset.before_all(), nil}
      )

      reader_opts = %{opts | read_only?: true}

      assert PureFileStorage.latest_name(reader_opts) == latest_name
      assert PureFileStorage.compaction_boundary(reader_opts) == compaction_boundary
      assert PureFileStorage.get_log_replay_safe_cursor(reader_opts) == baseline_offset

      assert ["baseline"] ==
               PureFileStorage.get_log_stream(LogOffset.first(), LogOffset.last(), reader_opts)
               |> Enum.map(&Jason.decode!/1)
               |> Enum.map(& &1["key"])

      PureFileStorage.terminate(writer)
    end

    @tag chunk_size: 1
    test "restart reconciles a combined generation published before its legacy mirrors", %{
      opts: opts,
      writer: writer
    } do
      old_latest_name = PureFileStorage.latest_name(opts)
      old_compaction_boundary = PureFileStorage.compaction_boundary(opts)
      {writer, baseline_offset} = append_compaction_candidate(writer)
      assert :ok = PureFileStorage.compact(opts, 0)

      assert_receive {Storage,
                      {PureFileStorage, :handle_compaction_finished,
                       [^baseline_offset, compacted_suffix, log_file_pos]}},
                     5_000

      writer =
        PureFileStorage.handle_compaction_finished(
          writer,
          baseline_offset,
          compacted_suffix,
          log_file_pos
        )

      new_latest_name = PureFileStorage.latest_name(opts)
      new_compaction_boundary = PureFileStorage.compaction_boundary(opts)

      assert new_latest_name != old_latest_name
      assert new_compaction_boundary == {baseline_offset, compacted_suffix}

      # Model a crash after the authoritative combined rename and before the
      # compatibility files were mirrored. Startup must repair those mirrors
      # before orphan cleanup, or it can delete the newly published files.
      write_legacy_metadata!(opts, :latest_name, old_latest_name)
      write_legacy_metadata!(opts, :compaction_boundary, old_compaction_boundary)

      writer = PureFileStorage.hibernate(writer)
      simulate_writer_crash(writer, opts)
      restarted_writer = PureFileStorage.init_writer!(opts, @shape)

      assert metadata_on_disk(opts, :latest_name) == new_latest_name
      assert metadata_on_disk(opts, :compaction_boundary) == new_compaction_boundary

      assert %{
               version: 1,
               latest_name: ^new_latest_name,
               compaction_boundary: ^new_compaction_boundary
             } = metadata_on_disk(opts, :read_generation)

      assert File.exists?(PureFileStorage.json_file(opts, new_latest_name))
      assert File.exists?(PureFileStorage.chunk_file(opts, new_latest_name))
      assert File.exists?(PureFileStorage.json_file(opts, compacted_suffix))
      assert File.exists?(PureFileStorage.chunk_file(opts, compacted_suffix))

      assert ["baseline"] ==
               PureFileStorage.get_log_stream(LogOffset.first(), LogOffset.last(), opts)
               |> Enum.map(&Jason.decode!/1)
               |> Enum.map(& &1["key"])

      PureFileStorage.terminate(restarted_writer)
    end

    @tag chunk_size: 1
    test "a deleted captured chunk index retries once with the fresh generation", %{
      opts: opts,
      writer: writer
    } do
      {writer, baseline_offset} = append_compaction_candidate(writer)

      old_chunk_path =
        PureFileStorage.chunk_file(opts, PureFileStorage.latest_name(opts))

      assert :ok = PureFileStorage.compact(opts, 0)

      assert_receive {Storage,
                      {PureFileStorage, :handle_compaction_finished,
                       [^baseline_offset, compacted_suffix, log_file_pos]}},
                     5_000

      gate = make_ref()

      Repatch.patch(ChunkIndex, :fetch_chunk, fn path, offset ->
        unless Process.get(gate, false) do
          Process.put(gate, true)

          writer =
            PureFileStorage.handle_compaction_finished(
              writer,
              baseline_offset,
              compacted_suffix,
              log_file_pos
            )

          Process.put({gate, :writer}, writer)
          File.rm(old_chunk_path)
        end

        Repatch.real(ChunkIndex.fetch_chunk(path, offset))
      end)

      reader_opts = %{opts | read_only?: true}

      result =
        PureFileStorage.get_log_stream(LogOffset.first(), LogOffset.last(), reader_opts)
        |> Enum.map(&Jason.decode!/1)
        |> Enum.map(& &1["key"])

      compacted_writer = Process.get({gate, :writer})
      PureFileStorage.terminate(compacted_writer)
      assert result == ["baseline"]
    end

    @tag chunk_size: 1
    test "a chunk-end lookup retries a deleted captured index with the fresh generation", %{
      opts: opts,
      writer: writer
    } do
      {writer, baseline_offset} = append_compaction_candidate(writer)

      old_chunk_path =
        PureFileStorage.chunk_file(opts, PureFileStorage.latest_name(opts))

      assert :ok = PureFileStorage.compact(opts, 0)

      assert_receive {Storage,
                      {PureFileStorage, :handle_compaction_finished,
                       [^baseline_offset, compacted_suffix, log_file_pos]}},
                     5_000

      gate = make_ref()

      Repatch.patch(ChunkIndex, :fetch_chunk, fn path, offset ->
        unless Process.get(gate, false) do
          Process.put(gate, true)

          writer =
            PureFileStorage.handle_compaction_finished(
              writer,
              baseline_offset,
              compacted_suffix,
              log_file_pos
            )

          Process.put({gate, :writer}, writer)
          File.rm(old_chunk_path)
        end

        Repatch.real(ChunkIndex.fetch_chunk(path, offset))
      end)

      reader_opts = %{opts | read_only?: true}

      result =
        PureFileStorage.get_chunk_end_log_offset(LogOffset.new(9, 0), reader_opts)

      compacted_writer = Process.get({gate, :writer})
      PureFileStorage.terminate(compacted_writer)
      assert result == baseline_offset
    end

    test "temporary workspaces are isolated by shape and attempt", %{
      opts: opts,
      writer: writer
    } do
      first_attempt = PureFileStorage.compaction_tmp_dir(opts, 1)
      second_attempt = PureFileStorage.compaction_tmp_dir(opts, 2)

      other_shape = %{opts | shape_handle: opts.shape_handle <> "-other"}
      other_shape_attempt = PureFileStorage.compaction_tmp_dir(other_shape, 1)

      refute first_attempt == second_attempt
      refute first_attempt == other_shape_attempt
      assert Path.dirname(first_attempt) == Path.dirname(second_attempt)
      refute Path.dirname(first_attempt) == Path.dirname(other_shape_attempt)

      PureFileStorage.terminate(writer)
    end

    @tag chunk_size: 1
    test "concurrent callers atomically claim one compaction startup", %{
      opts: opts,
      writer: writer
    } do
      {writer, baseline_offset} = append_compaction_candidate(writer)
      test_pid = self()
      gate = make_ref()

      Repatch.patch(PureFileStorage, :start_compaction_attempt, [mode: :shared], fn
        owner, worker_opts, ^baseline_offset, file_pos, attempt_id ->
          send(test_pid, {:compaction_start_claimed, gate, self()})

          receive do
            {:continue_compaction_start, ^gate} ->
              Repatch.real(
                PureFileStorage.start_compaction_attempt(
                  owner,
                  worker_opts,
                  baseline_offset,
                  file_pos,
                  attempt_id
                )
              )
          end
      end)

      first_caller =
        Task.async(fn ->
          result = PureFileStorage.compact(opts, 0)
          send(test_pid, {:first_compaction_call_returned, self(), result})

          receive do
            {Storage,
             {PureFileStorage, :handle_compaction_finished,
              [^baseline_offset, _suffix, _log_file_pos]}} = result_message ->
              send(test_pid, {:first_compaction_result, self(), result_message})

              receive do
                :release_first_compaction_caller -> :ok
              end
          end
        end)

      first_caller_pid = first_caller.pid

      assert_receive {:compaction_start_claimed, ^gate, ^first_caller_pid}

      second_caller = Task.async(fn -> PureFileStorage.compact(opts, 0) end)
      assert :already_in_progress == Task.await(second_caller, 5_000)

      send(first_caller_pid, {:continue_compaction_start, gate})
      assert_receive {:first_compaction_call_returned, ^first_caller_pid, :ok}

      assert_receive {:first_compaction_result, ^first_caller_pid,
                      {Storage,
                       {PureFileStorage, :handle_compaction_finished,
                        [^baseline_offset, suffix, log_file_pos]}}},
                     5_000

      writer =
        PureFileStorage.handle_compaction_finished(
          writer,
          baseline_offset,
          suffix,
          log_file_pos
        )

      send(first_caller_pid, :release_first_compaction_caller)
      assert :ok = Task.await(first_caller, 5_000)

      Repatch.restore(PureFileStorage, :start_compaction_attempt, 5, mode: :shared)
      PureFileStorage.terminate(writer)
    end

    @tag chunk_size: 1
    test "a task start failure leaves no durable compaction lock", %{opts: opts, writer: writer} do
      {writer, baseline_offset} = append_compaction_candidate(writer)

      Repatch.patch(PureFileStorage, :start_compaction_attempt, fn
        _owner, _opts, ^baseline_offset, _file_pos, _attempt_id ->
          {:error, :max_children}
      end)

      assert_raise Storage.Error, ~r/failed to start compaction attempt.*max_children/, fn ->
        PureFileStorage.compact(opts, 0)
      end

      refute File.exists?(metadata_path(opts, :compaction_started?))
      PureFileStorage.terminate(writer)
    end

    @tag chunk_size: 1
    test "a crashed compaction task clears its token and permits an immediate retry", %{
      opts: opts,
      writer: writer
    } do
      {writer, baseline_offset} = append_compaction_candidate(writer)
      test_pid = self()
      gate = make_ref()

      activate_mocks_for_descendant_procs(PureFileStorage)

      Repatch.patch(PureFileStorage, :make_compacted_files, [mode: :shared], fn
        _owner, worker_opts, ^baseline_offset, _file_pos, attempt_id ->
          attempt_dir = PureFileStorage.compaction_tmp_dir(worker_opts, attempt_id)
          File.mkdir_p!(attempt_dir)
          File.write!(Path.join(attempt_dir, "started"), "started")
          send(test_pid, {:compaction_worker_blocked, gate, self()})

          receive do
            {:crash_compaction_worker, ^gate} -> raise "injected compaction failure"
          end
      end)

      assert :ok = PureFileStorage.compact(opts, 0)
      assert_receive {:compaction_worker_blocked, ^gate, worker_pid}

      assert {:running, attempt_id, owner, task_pid} =
               metadata_on_disk(opts, :compaction_started?)

      assert owner == self()
      attempt_dir = PureFileStorage.compaction_tmp_dir(opts, attempt_id)
      assert File.exists?(attempt_dir)

      task_monitor = Process.monitor(task_pid)
      send(worker_pid, {:crash_compaction_worker, gate})
      assert_receive {:DOWN, ^task_monitor, :process, ^task_pid, _reason}, 5_000

      assert false == metadata_on_disk(opts, :compaction_started?)
      refute File.exists?(attempt_dir)

      Repatch.restore(PureFileStorage, :make_compacted_files, 5, mode: :shared)
      assert :ok = PureFileStorage.compact(opts, 0)

      assert_receive {Storage,
                      {PureFileStorage, :handle_compaction_finished,
                       [^baseline_offset, suffix, log_file_pos]}},
                     5_000

      writer =
        PureFileStorage.handle_compaction_finished(
          writer,
          baseline_offset,
          suffix,
          log_file_pos
        )

      PureFileStorage.terminate(writer)
    end

    @tag chunk_size: 1
    test "owner death cancels an in-flight compaction before a replacement starts", %{
      opts: opts,
      writer: writer
    } do
      {writer, baseline_offset} = append_compaction_candidate(writer)
      test_pid = self()
      gate = make_ref()

      activate_mocks_for_descendant_procs(PureFileStorage)

      Repatch.patch(PureFileStorage, :make_compacted_files, [mode: :shared], fn
        owner, worker_opts, ^baseline_offset, _file_pos, attempt_id ->
          attempt_dir = PureFileStorage.compaction_tmp_dir(worker_opts, attempt_id)
          File.mkdir_p!(attempt_dir)
          File.write!(Path.join(attempt_dir, "started"), "started")
          send(test_pid, {:owned_compaction_worker_blocked, gate, owner, self()})
          Process.sleep(:infinity)
      end)

      owner =
        spawn(fn ->
          receive do
            {:start_owned_compaction, ^gate} ->
              :ok = PureFileStorage.compact(opts, 0)
              send(test_pid, {:owned_compaction_started, self()})
              Process.sleep(:infinity)
          end
        end)

      Repatch.allow(test_pid, owner)
      send(owner, {:start_owned_compaction, gate})

      assert_receive {:owned_compaction_worker_blocked, ^gate, ^owner, worker_pid}
      assert_receive {:owned_compaction_started, ^owner}

      assert {:running, attempt_id, ^owner, task_pid} =
               metadata_on_disk(opts, :compaction_started?)

      attempt_dir = PureFileStorage.compaction_tmp_dir(opts, attempt_id)
      task_monitor = Process.monitor(task_pid)
      worker_monitor = Process.monitor(worker_pid)
      owner_monitor = Process.monitor(owner)

      Process.exit(owner, :kill)
      assert_receive {:DOWN, ^owner_monitor, :process, ^owner, :killed}
      assert_receive {:DOWN, ^worker_monitor, :process, ^worker_pid, :killed}, 5_000
      assert_receive {:DOWN, ^task_monitor, :process, ^task_pid, _reason}, 5_000

      assert false == metadata_on_disk(opts, :compaction_started?)
      refute File.exists?(attempt_dir)

      Repatch.restore(PureFileStorage, :make_compacted_files, 5, mode: :shared)
      assert :ok = PureFileStorage.compact(opts, 0)

      assert_receive {Storage,
                      {PureFileStorage, :handle_compaction_finished,
                       [^baseline_offset, suffix, log_file_pos]}},
                     5_000

      writer =
        PureFileStorage.handle_compaction_finished(
          writer,
          baseline_offset,
          suffix,
          log_file_pos
        )

      PureFileStorage.terminate(writer)
    end

    test "startup removes an unpublished latest generation left by a crash", %{
      opts: opts,
      writer: writer
    } do
      orphan_suffix = "latest.9999999999999.999"

      orphan_paths = [
        PureFileStorage.json_file(opts, orphan_suffix),
        PureFileStorage.chunk_file(opts, orphan_suffix),
        PureFileStorage.key_file(opts, orphan_suffix)
      ]

      Enum.each(orphan_paths, &File.write!(&1, "orphan"))
      assert Enum.all?(orphan_paths, &File.exists?/1)

      writer = PureFileStorage.hibernate(writer)
      simulate_writer_crash(writer, opts)
      restarted_writer = PureFileStorage.init_writer!(opts, @shape)

      refute Enum.any?(orphan_paths, &File.exists?/1)
      PureFileStorage.terminate(restarted_writer)
    end

    @tag chunk_size: 1_000
    test "streams captured before compaction use the current equivalent generation", %{
      opts: opts,
      writer: writer
    } do
      first_offset = LogOffset.new(10, 0)
      tail_offset = LogOffset.new(20, 0)

      first_json =
        Jason.encode!(%{
          key: "first",
          value: %{id: "first", padding: String.duplicate("x", 2_000)},
          headers: %{operation: "insert"}
        })

      tail_json =
        Jason.encode!(%{
          key: "tail",
          value: %{id: "tail"},
          headers: %{operation: "insert"}
        })

      writer =
        PureFileStorage.append_to_log!(
          [{first_offset, "first", :insert, first_json}],
          writer
        )

      writer =
        PureFileStorage.append_to_log!(
          [{tail_offset, "tail", :insert, tail_json}],
          writer
        )

      writer = PureFileStorage.hibernate(writer)
      old_latest_suffix = PureFileStorage.latest_name(opts)
      old_latest_path = PureFileStorage.json_file(opts, old_latest_suffix)

      captured_stream =
        PureFileStorage.get_log_stream(LogOffset.first(), LogOffset.last(), opts)

      captured_exact_stream =
        PureFileStorage.get_log_stream_with_offsets(
          LogOffset.first(),
          tail_offset,
          opts
        )

      assert :ok = PureFileStorage.compact(opts, 0)

      assert_receive {Storage,
                      {PureFileStorage, :handle_compaction_finished,
                       [^first_offset, compacted_suffix, log_file_pos]}},
                     5_000

      writer =
        PureFileStorage.handle_compaction_finished(
          writer,
          first_offset,
          compacted_suffix,
          log_file_pos
        )

      assert wait_until(fn -> not File.exists?(old_latest_path) end, 5_000)

      expected_jsons = [Jason.decode!(first_json), Jason.decode!(tail_json)]
      assert Enum.map(captured_stream, &Jason.decode!/1) == expected_jsons

      assert Enum.map(captured_exact_stream, &decode_offset_row/1) == [
               {first_offset, Enum.at(expected_jsons, 0)},
               {tail_offset, Enum.at(expected_jsons, 1)}
             ]

      PureFileStorage.terminate(writer)
    end

    @tag chunk_size: 1
    test "a later compaction reclaims the previous compacted generation", %{
      opts: opts,
      writer: writer
    } do
      {writer, first_offset} = append_compaction_candidate(writer)
      assert :ok = PureFileStorage.compact(opts, 0)

      assert_receive {Storage,
                      {PureFileStorage, :handle_compaction_finished,
                       [^first_offset, first_suffix, first_log_file_pos]}},
                     5_000

      writer =
        PureFileStorage.handle_compaction_finished(
          writer,
          first_offset,
          first_suffix,
          first_log_file_pos
        )

      second_offset = LogOffset.new(20, 0)

      second_json =
        Jason.encode!(%{
          key: "second",
          value: %{id: "second"},
          headers: %{operation: "insert"}
        })

      writer =
        PureFileStorage.append_to_log!(
          [{second_offset, "second", :insert, second_json}],
          writer
        )

      assert :ok = PureFileStorage.compact(opts, 0)

      assert_receive {Storage,
                      {PureFileStorage, :handle_compaction_finished,
                       [^second_offset, second_suffix, second_log_file_pos]}},
                     5_000

      refute second_suffix == first_suffix

      writer =
        PureFileStorage.handle_compaction_finished(
          writer,
          second_offset,
          second_suffix,
          second_log_file_pos
        )

      first_generation_paths = [
        PureFileStorage.json_file(opts, first_suffix),
        PureFileStorage.chunk_file(opts, first_suffix),
        PureFileStorage.key_file(opts, first_suffix)
      ]

      assert wait_until(
               fn -> not Enum.any?(first_generation_paths, &File.exists?/1) end,
               5_000
             )

      PureFileStorage.terminate(writer)
    end

    @tag chunk_size: 1
    test "restart preserves a compacted generation published before a latest-name crash", %{
      opts: opts,
      writer: writer
    } do
      {writer, baseline_offset} = append_compaction_candidate(writer)
      test_pid = self()

      compaction_owner =
        spawn(fn ->
          :ok = PureFileStorage.compact(opts, 0)

          receive do
            {Storage,
             {PureFileStorage, :handle_compaction_finished,
              [^baseline_offset, _suffix, _log_file_pos]}} = result ->
              send(test_pid, {:compaction_result_for_publish_crash, self(), result})
              Process.sleep(:infinity)
          end
        end)

      assert_receive {:compaction_result_for_publish_crash, ^compaction_owner,
                      {Storage,
                       {PureFileStorage, :handle_compaction_finished,
                        [^baseline_offset, compacted_suffix, log_file_pos]}}},
                     5_000

      Repatch.patch(PureFileStorage, :publish_latest_read_generation!, fn
        _opts, _latest_name, _cached_chunk_boundaries ->
          raise "injected latest generation publication crash"
      end)

      assert_raise RuntimeError, "injected latest generation publication crash", fn ->
        PureFileStorage.handle_compaction_finished(
          writer,
          baseline_offset,
          compacted_suffix,
          log_file_pos
        )
      end

      Repatch.restore(PureFileStorage, :publish_latest_read_generation!, 3)
      assert PureFileStorage.compaction_boundary(opts) == {baseline_offset, compacted_suffix}

      owner_monitor = Process.monitor(compaction_owner)
      Process.exit(compaction_owner, :kill)
      assert_receive {:DOWN, ^owner_monitor, :process, ^compaction_owner, :killed}

      simulate_writer_crash(writer, opts)
      restarted_writer = PureFileStorage.init_writer!(opts, @shape)

      assert false == metadata_on_disk(opts, :compaction_started?)
      assert PureFileStorage.compaction_boundary(opts) == {baseline_offset, compacted_suffix}
      assert File.exists?(PureFileStorage.json_file(opts, compacted_suffix))
      assert File.exists?(PureFileStorage.chunk_file(opts, compacted_suffix))
      assert File.exists?(PureFileStorage.key_file(opts, compacted_suffix))

      assert ["baseline"] ==
               PureFileStorage.get_log_stream(LogOffset.first(), LogOffset.last(), opts)
               |> Enum.map(&Jason.decode!/1)
               |> Enum.map(& &1["key"])

      PureFileStorage.terminate(restarted_writer)
    end
  end

  describe "dependency move transactions" do
    setup [:start_storage, :with_started_writer]

    @tag chunk_size: 10 * 1024
    test "a fresh reader sees every committed small move in the current chunk", %{
      base_opts: base_opts,
      writer: writer
    } do
      {writer, expected} =
        Enum.reduce(1..3, {writer, []}, fn n, {writer, expected} ->
          offset = LogOffset.new(10 + n, 0)
          json = Jason.encode!(%{value: "move-#{n}"})

          writer = PureFileStorage.begin_move_transaction!(writer)
          writer = PureFileStorage.append_to_log!([{offset, "move-#{n}", :insert, json}], writer)

          writer =
            PureFileStorage.commit_move_transaction!(
              %{"dependency" => LogOffset.new(100 + n, 0)},
              10 + n,
              writer
            )

          {writer, [json | expected]}
        end)

      fresh_reader = PureFileStorage.for_shape(@shape_handle, base_opts)
      {:ok, latest_offset} = PureFileStorage.fetch_latest_offset(fresh_reader)

      chunk_end =
        PureFileStorage.get_chunk_end_log_offset(
          LogOffset.last_before_real_offsets(),
          fresh_reader
        ) || latest_offset

      assert chunk_end == latest_offset

      assert Enum.reverse(expected) ==
               PureFileStorage.get_log_stream(
                 LogOffset.last_before_real_offsets(),
                 chunk_end,
                 fresh_reader
               )
               |> Enum.to_list()

      PureFileStorage.terminate(writer)
    end

    @tag chunk_size: 10 * 1024
    test "reuses a preexisting incomplete chunk as the move addressability bridge", %{
      opts: opts,
      writer: writer
    } do
      baseline_offset = LogOffset.new(10, 0)
      move_offset = LogOffset.new(11, 0)
      positions = %{"dependency" => LogOffset.new(101, 0)}
      chunk_file = PureFileStorage.chunk_file(opts, PureFileStorage.latest_name(opts))

      writer =
        PureFileStorage.append_to_log!(
          [{baseline_offset, "baseline", :insert, ~S|{"value":"baseline"}|}],
          writer
        )

      assert [{{^baseline_offset, nil}, _, _}] =
               PureFileStorage.ChunkIndex.read_chunk_file(chunk_file)

      writer = PureFileStorage.begin_move_transaction!(writer)

      assert [{{^baseline_offset, nil}, _, _}] =
               PureFileStorage.ChunkIndex.read_chunk_file(chunk_file)

      chunk_file_before_move = File.read!(chunk_file)

      writer =
        PureFileStorage.append_to_log!(
          [{move_offset, "move", :insert, ~S|{"value":"move"}|}],
          writer
        )

      assert File.read!(chunk_file) == chunk_file_before_move

      for reader_opts <- [opts, %{opts | read_only?: true}] do
        assert [~S|{"value":"baseline"}|] ==
                 PureFileStorage.get_log_stream(
                   LogOffset.first(),
                   LogOffset.last(),
                   reader_opts
                 )
                 |> Enum.to_list()
      end

      writer = PureFileStorage.commit_move_transaction!(positions, 11, writer)

      # The stable opening half-entry addresses both records. Keeping the small
      # move in the existing chunk avoids introducing an artificial HTTP page.
      assert [{{^baseline_offset, nil}, _, _}] =
               PureFileStorage.ChunkIndex.read_chunk_file(chunk_file)

      for reader_opts <- [opts, %{opts | read_only?: true}] do
        assert [~S|{"value":"baseline"}|, ~S|{"value":"move"}|] ==
                 PureFileStorage.get_log_stream(
                   LogOffset.first(),
                   LogOffset.last(),
                   reader_opts
                 )
                 |> Enum.to_list()
      end

      PureFileStorage.terminate(writer)
    end

    @tag chunk_size: 25
    test "publishes a deferred close only after an extending move commits", %{
      opts: opts,
      writer: writer
    } do
      baseline_offset = LogOffset.new(10, 0)
      move_offset = LogOffset.new(11, 0)
      baseline_json = ~S|{"value":"baseline"}|
      move_json = ~S|{"value":"move"}|
      chunk_file = PureFileStorage.chunk_file(opts, PureFileStorage.latest_name(opts))

      writer =
        PureFileStorage.append_to_log!(
          [{baseline_offset, "baseline", :insert, baseline_json}],
          writer
        )

      assert [{{^baseline_offset, nil}, _, _}] =
               PureFileStorage.ChunkIndex.read_chunk_file(chunk_file)

      chunk_file_before_move = File.read!(chunk_file)
      writer = PureFileStorage.begin_move_transaction!(writer)

      writer =
        PureFileStorage.append_to_log!(
          [{move_offset, "move", :insert, move_json}],
          writer
        )

      # The closing half-entry would make an old reader's whole-chunk fast path
      # expose the move, so it remains staged while the cursor is unchanged.
      assert File.read!(chunk_file) == chunk_file_before_move

      assert [^baseline_json] =
               PureFileStorage.get_log_stream(LogOffset.first(), LogOffset.last(), opts)
               |> Enum.to_list()

      writer =
        PureFileStorage.commit_move_transaction!(
          %{"dependency" => LogOffset.new(101, 0)},
          11,
          writer
        )

      assert [{{^baseline_offset, ^move_offset}, _, _}] =
               PureFileStorage.ChunkIndex.read_chunk_file(chunk_file)

      assert [^baseline_json, ^move_json] =
               PureFileStorage.get_log_stream(LogOffset.first(), LogOffset.last(), opts)
               |> Enum.to_list()

      PureFileStorage.terminate(writer)
    end

    test "refuses to begin a move while a root transaction fragment is incomplete", %{
      opts: opts,
      writer: writer
    } do
      fragment_offset = LogOffset.new(10, 0)

      writer =
        PureFileStorage.append_fragment_to_log!(
          [{fragment_offset, "fragment", :insert, ~S|{"value":"fragment"}|}],
          writer
        )

      assert_raise Storage.Error,
                   ~r/cannot begin a dependency-move transaction while a root transaction fragment is open/,
                   fn -> PureFileStorage.begin_move_transaction!(writer) end

      writer = PureFileStorage.signal_txn_commit!(@xid, writer)
      writer = PureFileStorage.begin_move_transaction!(writer)
      writer = PureFileStorage.commit_move_transaction!(%{}, 10, writer)

      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, fragment_offset}

      PureFileStorage.terminate(writer)
    end

    @tag chunk_size: 1
    test "defers compaction finalization until an open move commits", %{
      opts: opts,
      writer: writer
    } do
      baseline_offset = LogOffset.new(10, 0)
      move_offset = LogOffset.new(10, 5)
      positions = %{"dependency" => LogOffset.new(101, 0)}

      baseline_json =
        Jason.encode!(%{
          key: "baseline",
          value: %{id: "baseline"},
          headers: %{operation: "insert"}
        })

      move_json =
        Jason.encode!(%{key: "move", value: %{id: "move"}, headers: %{operation: "insert"}})

      writer =
        PureFileStorage.append_to_log!(
          [{baseline_offset, "baseline", :insert, baseline_json}],
          writer
        )

      old_latest = PureFileStorage.latest_name(opts)
      assert :ok = PureFileStorage.compact(opts, 0)

      assert_receive {Storage,
                      {PureFileStorage, :handle_compaction_finished,
                       [^baseline_offset, compacted_suffix, log_file_pos]}},
                     5_000

      writer = PureFileStorage.begin_move_transaction!(writer)

      writer =
        PureFileStorage.append_to_log!(
          [{move_offset, "move", :insert, move_json}],
          writer
        )

      writer =
        PureFileStorage.handle_compaction_finished(
          writer,
          baseline_offset,
          compacted_suffix,
          log_file_pos
        )

      # The old latest file still owns staged chunk positions until commit.
      assert PureFileStorage.latest_name(opts) == old_latest

      writer = PureFileStorage.commit_move_transaction!(positions, 10, writer)

      refute PureFileStorage.latest_name(opts) == old_latest
      assert PureFileStorage.compaction_boundary(opts) == {baseline_offset, compacted_suffix}
      assert PureFileStorage.get_log_replay_safe_cursor(opts) == baseline_offset
      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, move_offset}

      assert ["baseline", "move"] ==
               PureFileStorage.get_log_stream(LogOffset.first(), LogOffset.last(), opts)
               |> Enum.map(&Jason.decode!/1)
               |> Enum.map(& &1["key"])

      PureFileStorage.terminate(writer)
      restarted_writer = PureFileStorage.init_writer!(opts, @shape)

      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, move_offset}
      assert PureFileStorage.get_log_replay_safe_cursor(opts) == baseline_offset

      assert ["baseline", "move"] ==
               PureFileStorage.get_log_stream(LogOffset.first(), LogOffset.last(), opts)
               |> Enum.map(&Jason.decode!/1)
               |> Enum.map(& &1["key"])

      PureFileStorage.terminate(restarted_writer)
    end

    @tag chunk_size: 1
    test "restart discards a compaction deferred by an uncommitted move and permits another", %{
      opts: opts,
      writer: writer
    } do
      baseline_offset = LogOffset.new(10, 0)
      move_offset = LogOffset.new(11, 0)

      baseline_json =
        Jason.encode!(%{
          key: "baseline",
          value: %{id: "baseline"},
          headers: %{operation: "insert"}
        })

      move_json =
        Jason.encode!(%{key: "move", value: %{id: "move"}, headers: %{operation: "insert"}})

      writer =
        PureFileStorage.append_to_log!(
          [{baseline_offset, "baseline", :insert, baseline_json}],
          writer
        )

      test_pid = self()

      compaction_owner =
        spawn(fn ->
          :ok = PureFileStorage.compact(opts, 0)

          receive do
            {Storage,
             {PureFileStorage, :handle_compaction_finished,
              [^baseline_offset, _suffix, _log_file_pos]}} = result ->
              send(test_pid, {:owned_compaction_result, self(), result})
              Process.sleep(:infinity)
          end
        end)

      assert_receive {:owned_compaction_result, ^compaction_owner,
                      {Storage,
                       {PureFileStorage, :handle_compaction_finished,
                        [^baseline_offset, stale_suffix, stale_log_file_pos]}}},
                     5_000

      writer = PureFileStorage.begin_move_transaction!(writer)

      writer =
        PureFileStorage.append_to_log!(
          [{move_offset, "move", :insert, move_json}],
          writer
        )

      writer =
        PureFileStorage.handle_compaction_finished(
          writer,
          baseline_offset,
          stale_suffix,
          stale_log_file_pos
        )

      assert {:finished, _attempt_id, ^compaction_owner, _task_pid, ^stale_suffix} =
               metadata_on_disk(opts, :compaction_started?)

      assert File.exists?(PureFileStorage.json_file(opts, stale_suffix))

      owner_monitor = Process.monitor(compaction_owner)
      Process.exit(compaction_owner, :kill)
      assert_receive {:DOWN, ^owner_monitor, :process, ^compaction_owner, :killed}

      writer = PureFileStorage.hibernate(writer)
      simulate_writer_crash(writer, opts)
      restarted_writer = PureFileStorage.init_writer!(opts, @shape)

      assert false == metadata_on_disk(opts, :compaction_started?)
      refute File.exists?(PureFileStorage.json_file(opts, stale_suffix))
      refute File.exists?(PureFileStorage.chunk_file(opts, stale_suffix))
      refute File.exists?(PureFileStorage.key_file(opts, stale_suffix))

      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, baseline_offset}

      assert [^baseline_json] =
               PureFileStorage.get_log_stream(LogOffset.first(), LogOffset.last(), opts)
               |> Enum.to_list()

      # The stale durable lock must not leave compaction permanently reporting
      # `:already_in_progress` after the writer restarts.
      assert :ok = PureFileStorage.compact(opts, 0)

      assert_receive {Storage,
                      {PureFileStorage, :handle_compaction_finished,
                       [^baseline_offset, recovered_suffix, recovered_log_file_pos]}},
                     5_000

      refute recovered_suffix == stale_suffix

      restarted_writer =
        PureFileStorage.handle_compaction_finished(
          restarted_writer,
          baseline_offset,
          recovered_suffix,
          recovered_log_file_pos
        )

      assert PureFileStorage.compaction_boundary(opts) == {baseline_offset, recovered_suffix}

      PureFileStorage.terminate(restarted_writer)
    end

    @tag chunk_size: 1
    test "an addressability bridge is safe before commit and complete after cursor publication",
         %{
           opts: opts,
           writer: writer
         } do
      staged = stage_chunk_index_intent(opts, writer)

      :ok = PureFileStorage.publish_chunk_index_addressability_bridge!(opts, staged.intent)

      assert File.read!(staged.chunk_file) ==
               staged.chunk_file_before_move <> staged.intent.addressability_bridge

      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, staged.baseline_offset}

      for reader_opts <- [opts, %{opts | read_only?: true}] do
        assert [~S|{"value":"baseline"}|] ==
                 PureFileStorage.get_log_stream(
                   LogOffset.first(),
                   LogOffset.last(),
                   reader_opts
                 )
                 |> Enum.to_list()
      end

      :ok =
        PureFileStorage.commit_transaction_state!(
          opts,
          staged.move_offset,
          staged.new_positions,
          staged.root_delivery_tx_offset,
          staged.intent
        )

      for reader_opts <- [opts, %{opts | read_only?: true}] do
        assert [~S|{"value":"baseline"}|, ~S|{"value":"move"}|] ==
                 PureFileStorage.get_log_stream(
                   LogOffset.first(),
                   LogOffset.last(),
                   reader_opts
                 )
                 |> Enum.to_list()
      end

      :ok = PureFileStorage.reconcile_chunk_index_intent!(opts)
      writer = PureFileStorage.hibernate(staged.writer)
      simulate_writer_crash(writer, opts)
      restarted_writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.terminate(restarted_writer)
    end

    @tag chunk_size: 1
    test "position persistence preserves a pending chunk-index recovery intent", %{
      opts: opts,
      writer: writer
    } do
      staged = stage_chunk_index_intent(opts, writer)
      :ok = PureFileStorage.publish_chunk_index_addressability_bridge!(opts, staged.intent)

      :ok =
        PureFileStorage.commit_transaction_state!(
          opts,
          staged.move_offset,
          staged.new_positions,
          staged.root_delivery_tx_offset,
          staged.intent
        )

      :ok = PureFileStorage.set_move_positions!(staged.new_positions, opts)

      assert %{chunk_index_intent: intent} = transaction_state_on_disk(opts)
      assert intent == staged.intent

      :ok = PureFileStorage.reconcile_chunk_index_intent!(opts)
      assert %{chunk_index_intent: nil} = transaction_state_on_disk(opts)

      writer = PureFileStorage.hibernate(staged.writer)
      simulate_writer_crash(writer, opts)
      restarted_writer = PureFileStorage.init_writer!(opts, @shape)

      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, staged.move_offset}
      PureFileStorage.terminate(restarted_writer)
    end

    @tag chunk_size: 1
    test "startup discards an addressability bridge when the cursor was never published", %{
      opts: opts,
      writer: writer
    } do
      staged = stage_chunk_index_intent(opts, writer)

      :ok = PureFileStorage.publish_chunk_index_addressability_bridge!(opts, staged.intent)

      assert File.read!(staged.chunk_file) ==
               staged.chunk_file_before_move <> staged.intent.addressability_bridge

      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, staged.baseline_offset}

      writer = PureFileStorage.hibernate(staged.writer)
      simulate_writer_crash(writer, opts)
      restarted_writer = PureFileStorage.init_writer!(opts, @shape)

      assert File.read!(staged.chunk_file) == staged.chunk_file_before_move
      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, staged.baseline_offset}

      assert [~S|{"value":"baseline"}|] ==
               PureFileStorage.get_log_stream(LogOffset.first(), LogOffset.last(), opts)
               |> Enum.to_list()

      PureFileStorage.terminate(restarted_writer)
    end

    @tag chunk_size: 1
    test "keeps chunk-index entries invisible to old readers until move commit", %{
      opts: opts,
      writer: writer
    } do
      baseline_offset = LogOffset.new(10, 0)
      move_offset = LogOffset.new(11, 0)
      positions = %{"dependency" => LogOffset.new(101, 0)}
      chunk_file = PureFileStorage.chunk_file(opts, PureFileStorage.latest_name(opts))

      writer =
        PureFileStorage.append_to_log!(
          [{baseline_offset, "baseline", :insert, ~S|{"value":"baseline"}|}],
          writer
        )

      chunk_file_before_move = File.read!(chunk_file)

      assert [{{^baseline_offset, ^baseline_offset}, _, _}] =
               PureFileStorage.ChunkIndex.read_chunk_file(chunk_file)

      writer = PureFileStorage.begin_move_transaction!(writer)

      writer =
        PureFileStorage.append_to_log!(
          [{move_offset, "move", :insert, ~S|{"value":"move"}|}],
          writer
        )

      # A rolling-deploy reader may not have the newer published-offset clamp,
      # so assert directly against the shared file it would inspect.
      assert File.read!(chunk_file) == chunk_file_before_move

      writer = PureFileStorage.commit_move_transaction!(positions, 11, writer)

      assert [
               {{^baseline_offset, ^baseline_offset}, _, _},
               {{^move_offset, ^move_offset}, _, _}
             ] = PureFileStorage.ChunkIndex.read_chunk_file(chunk_file)

      PureFileStorage.terminate(writer)
    end

    @tag chunk_size: 1
    test "discards staged chunk-index entries when an open move crashes", %{
      opts: opts,
      writer: writer
    } do
      baseline_offset = LogOffset.new(10, 0)
      move_offset = LogOffset.new(11, 0)
      old_positions = %{"dependency" => LogOffset.new(100, 0)}
      old_root_delivery_tx_offset = nil
      chunk_file = PureFileStorage.chunk_file(opts, PureFileStorage.latest_name(opts))

      writer =
        PureFileStorage.append_to_log!(
          [{baseline_offset, "baseline", :insert, ~S|{"value":"baseline"}|}],
          writer
        )

      :ok = PureFileStorage.set_move_positions!(old_positions, opts)
      chunk_file_before_move = File.read!(chunk_file)
      writer = PureFileStorage.begin_move_transaction!(writer)

      writer =
        PureFileStorage.append_to_log!(
          [{move_offset, "move", :insert, ~S|{"value":"move"}|}],
          writer
        )

      writer = PureFileStorage.hibernate(writer)
      assert File.read!(chunk_file) == chunk_file_before_move

      simulate_writer_crash(writer, opts)
      restarted_writer = PureFileStorage.init_writer!(opts, @shape)

      assert File.read!(chunk_file) == chunk_file_before_move
      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, baseline_offset}
      assert PureFileStorage.fetch_move_positions(opts) == {:ok, old_positions}

      assert PureFileStorage.fetch_root_delivery_tx_offset(opts) ==
               {:ok, old_root_delivery_tx_offset}

      assert [{{^baseline_offset, ^baseline_offset}, _, _}] =
               PureFileStorage.ChunkIndex.read_chunk_file(chunk_file)

      PureFileStorage.terminate(restarted_writer)
    end

    @tag chunk_size: 1
    test "startup publishes a committed chunk-index intent after a pre-index crash", %{
      opts: opts,
      writer: writer
    } do
      %{
        writer: writer,
        chunk_file: chunk_file,
        chunk_file_before_move: chunk_file_before_move,
        move_offset: move_offset,
        root_delivery_tx_offset: root_delivery_tx_offset,
        new_positions: new_positions,
        intent: intent
      } = persist_staged_chunk_index_intent(opts, writer)

      assert File.read!(chunk_file) ==
               chunk_file_before_move <> intent.addressability_bridge

      assert transaction_state_on_disk(opts).chunk_index_intent == intent
      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, move_offset}

      for reader_opts <- [opts, %{opts | read_only?: true}] do
        assert [~S|{"value":"baseline"}|, ~S|{"value":"move"}|] ==
                 PureFileStorage.get_log_stream(
                   LogOffset.first(),
                   LogOffset.last(),
                   reader_opts
                 )
                 |> Enum.to_list()
      end

      simulate_writer_crash(writer, opts)
      restarted_writer = PureFileStorage.init_writer!(opts, @shape)

      assert File.read!(chunk_file) == chunk_file_before_move <> intent.bytes
      assert transaction_state_on_disk(opts).chunk_index_intent == nil
      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, move_offset}
      assert PureFileStorage.fetch_move_positions(opts) == {:ok, new_positions}

      assert PureFileStorage.fetch_root_delivery_tx_offset(opts) ==
               {:ok, root_delivery_tx_offset}

      assert [~S|{"value":"baseline"}|, ~S|{"value":"move"}|] ==
               PureFileStorage.get_log_stream(LogOffset.first(), LogOffset.last(), opts)
               |> Enum.to_list()

      PureFileStorage.terminate(restarted_writer)
    end

    @tag chunk_size: 1
    test "startup completes a partially written committed chunk-index intent", %{
      opts: opts,
      writer: writer
    } do
      %{
        writer: writer,
        chunk_file: chunk_file,
        chunk_file_before_move: chunk_file_before_move,
        baseline_offset: baseline_offset,
        move_offset: move_offset,
        intent: intent
      } = persist_staged_chunk_index_intent(opts, writer)

      partial_size = 17
      bridge_size = byte_size(intent.addressability_bridge)
      remaining = binary_part(intent.bytes, bridge_size, byte_size(intent.bytes) - bridge_size)
      partial = binary_part(remaining, 0, partial_size)
      append_and_sync!(chunk_file, partial)

      assert File.read!(chunk_file) ==
               chunk_file_before_move <> intent.addressability_bridge <> partial

      simulate_writer_crash(writer, opts)
      restarted_writer = PureFileStorage.init_writer!(opts, @shape)

      assert File.read!(chunk_file) == chunk_file_before_move <> intent.bytes
      assert transaction_state_on_disk(opts).chunk_index_intent == nil

      assert [
               {{^baseline_offset, ^baseline_offset}, _, _},
               {{^move_offset, ^move_offset}, _, _}
             ] = PureFileStorage.ChunkIndex.read_chunk_file(chunk_file)

      PureFileStorage.terminate(restarted_writer)
    end

    @tag chunk_size: 1
    test "startup does not duplicate an already-published chunk-index intent", %{
      opts: opts,
      writer: writer
    } do
      %{
        writer: writer,
        chunk_file: chunk_file,
        chunk_file_before_move: chunk_file_before_move,
        baseline_offset: baseline_offset,
        move_offset: move_offset,
        intent: intent
      } = persist_staged_chunk_index_intent(opts, writer)

      assert ^intent = PureFileStorage.publish_chunk_index_intent!(opts, intent)
      published_chunk_file = File.read!(chunk_file)
      assert published_chunk_file == chunk_file_before_move <> intent.bytes
      assert transaction_state_on_disk(opts).chunk_index_intent == intent

      simulate_writer_crash(writer, opts)
      restarted_writer = PureFileStorage.init_writer!(opts, @shape)

      assert File.read!(chunk_file) == published_chunk_file
      assert transaction_state_on_disk(opts).chunk_index_intent == nil

      assert [
               {{^baseline_offset, ^baseline_offset}, _, _},
               {{^move_offset, ^move_offset}, _, _}
             ] = PureFileStorage.ChunkIndex.read_chunk_file(chunk_file)

      PureFileStorage.terminate(restarted_writer)
    end

    test "a physical flush during an open move does not advance the restart boundary", %{
      opts: opts,
      writer: writer
    } do
      baseline_offset = LogOffset.new(10, 0)
      move_offset = LogOffset.new(11, 0)
      old_positions = %{"dependency" => LogOffset.new(100, 0)}
      old_root_delivery_tx_offset = 0
      move_json = Jason.encode!(%{value: String.duplicate("x", 70 * 1024)})

      writer =
        PureFileStorage.append_to_log!(
          [{baseline_offset, "baseline", :insert, ~S|{"value":"baseline"}|}],
          writer
        )

      writer = PureFileStorage.hibernate(writer)
      assert_receive {Storage, :flushed, ^baseline_offset}
      :ok = PureFileStorage.set_move_positions!(old_positions, opts)

      writer = PureFileStorage.begin_move_transaction!(writer)

      writer =
        PureFileStorage.append_to_log!(
          [{move_offset, "move", :insert, move_json}],
          writer
        )

      # The move row exceeds WriteLoop's 64KB buffer threshold, so it has
      # already been datasync'd without relying on hibernate's final flush.
      assert %{
               last_persisted_offset: ^move_offset,
               last_seen_txn_offset: ^baseline_offset,
               last_persisted_txn_offset: ^baseline_offset
             } = storage_internal_state(opts)

      # This is the public chunked reader, not the exact offset-preserving
      # replay reader. The fsynced move tail must remain invisible here too.
      assert [~S|{"value":"baseline"}|] ==
               PureFileStorage.get_log_stream(LogOffset.first(), LogOffset.last(), opts)
               |> Enum.to_list()

      refute_receive {Storage, :flushed, ^move_offset}, 10

      writer = PureFileStorage.hibernate(writer)

      assert %{
               last_persisted_txn_offset: ^baseline_offset,
               move_positions: ^old_positions,
               root_delivery_tx_offset: ^old_root_delivery_tx_offset
             } = transaction_state_on_disk(opts)

      simulate_writer_crash(writer, opts)
      restarted_writer = PureFileStorage.init_writer!(opts, @shape)

      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, baseline_offset}
      assert PureFileStorage.fetch_move_positions(opts) == {:ok, old_positions}

      assert PureFileStorage.fetch_root_delivery_tx_offset(opts) ==
               {:ok, old_root_delivery_tx_offset}

      assert [~S|{"value":"baseline"}|] ==
               PureFileStorage.get_log_stream(LogOffset.first(), LogOffset.last(), opts)
               |> Enum.to_list()

      PureFileStorage.terminate(restarted_writer)
    end

    test "commit publishes the move boundary, positions, and root frontier in one record", %{
      opts: opts,
      writer: writer
    } do
      baseline_offset = LogOffset.new(10, 0)
      move_offset = LogOffset.new(11, 0)
      old_positions = %{"dependency" => LogOffset.new(100, 0)}
      new_positions = %{"dependency" => LogOffset.new(101, 0)}

      writer =
        PureFileStorage.append_to_log!(
          [{baseline_offset, "baseline", :insert, ~S|{"value":"baseline"}|}],
          writer
        )

      writer = PureFileStorage.hibernate(writer)
      assert_receive {Storage, :flushed, ^baseline_offset}
      :ok = PureFileStorage.set_move_positions!(old_positions, opts)
      writer = PureFileStorage.begin_move_transaction!(writer)

      writer =
        PureFileStorage.append_to_log!(
          [{move_offset, "move", :insert, ~S|{"value":"move"}|}],
          writer
        )

      new_root_delivery_tx_offset = 11

      writer =
        PureFileStorage.commit_move_transaction!(
          new_positions,
          new_root_delivery_tx_offset,
          writer
        )

      assert %{
               version: 2,
               last_persisted_txn_offset: ^move_offset,
               move_positions: ^new_positions,
               root_delivery_tx_offset: ^new_root_delivery_tx_offset
             } = transaction_state_on_disk(opts)

      # New readers must prefer the combined record over stale legacy files.
      write_legacy_metadata!(opts, :last_persisted_txn_offset, baseline_offset)
      write_legacy_metadata!(opts, :move_positions, old_positions)

      writer = PureFileStorage.hibernate(writer)
      simulate_writer_crash(writer, opts)
      restarted_writer = PureFileStorage.init_writer!(opts, @shape)

      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, move_offset}
      assert PureFileStorage.fetch_move_positions(opts) == {:ok, new_positions}

      assert PureFileStorage.fetch_root_delivery_tx_offset(opts) ==
               {:ok, new_root_delivery_tx_offset}

      assert [~S|{"value":"baseline"}|, ~S|{"value":"move"}|] ==
               PureFileStorage.get_log_stream(LogOffset.first(), LogOffset.last(), opts)
               |> Enum.to_list()

      PureFileStorage.terminate(restarted_writer)
    end

    test "ordinary flushes advance the combined record without changing move positions", %{
      opts: opts,
      writer: writer
    } do
      positions = %{"dependency" => LogOffset.new(100, 0)}
      offset = LogOffset.new(10, 0)
      root_delivery_tx_offset = 0

      :ok = PureFileStorage.set_move_positions!(positions, opts)

      writer =
        PureFileStorage.append_to_log!(
          [{offset, "ordinary", :insert, ~S|{"value":"ordinary"}|}],
          writer
        )

      writer = PureFileStorage.hibernate(writer)

      assert %{
               last_persisted_txn_offset: ^offset,
               move_positions: ^positions,
               root_delivery_tx_offset: ^root_delivery_tx_offset
             } = transaction_state_on_disk(opts)

      PureFileStorage.terminate(writer)
    end

    test "legacy metadata remains without a provable frontier when positions are next persisted",
         %{
           opts: opts,
           writer: writer
         } do
      legacy_offset = LogOffset.last_before_real_offsets()
      positions = %{"dependency" => LogOffset.new(99, 0)}
      new_offset = LogOffset.new(10, 0)

      write_legacy_metadata!(opts, :last_persisted_txn_offset, legacy_offset)
      write_legacy_metadata!(opts, :move_positions, positions)
      refute File.exists?(metadata_path(opts, :transaction_state))

      writer = PureFileStorage.hibernate(writer)
      simulate_writer_crash(writer, opts)
      writer = PureFileStorage.init_writer!(opts, @shape)

      assert %{
               last_persisted_txn_offset: ^legacy_offset
             } = storage_internal_state(opts)

      assert PureFileStorage.fetch_move_positions(opts) == {:ok, positions}
      assert PureFileStorage.fetch_root_delivery_tx_offset(opts) == {:ok, nil}

      writer =
        PureFileStorage.append_to_log!(
          [{new_offset, "ordinary", :insert, ~S|{"value":"ordinary"}|}],
          writer
        )

      writer = PureFileStorage.hibernate(writer)

      # Standalone hot-path flushes retain the legacy one-file boundary until a
      # dependency consumer explicitly activates the combined cursor record.
      refute File.exists?(metadata_path(opts, :transaction_state))
      :ok = PureFileStorage.set_move_positions!(positions, opts)

      assert %{
               version: 2,
               last_persisted_txn_offset: ^new_offset,
               move_positions: ^positions,
               root_delivery_tx_offset: nil
             } = transaction_state_on_disk(opts)

      PureFileStorage.terminate(writer)
    end

    test "a version-1 transaction state has no provable root delivery frontier", %{
      opts: opts,
      writer: writer
    } do
      boundary = LogOffset.new(10, 0)
      positions = %{"dependency" => LogOffset.new(100, 0)}

      write_legacy_metadata!(opts, :transaction_state, %{
        version: 1,
        last_persisted_txn_offset: boundary,
        move_positions: positions
      })

      assert PureFileStorage.fetch_move_positions(opts) == {:ok, positions}
      assert PureFileStorage.fetch_root_delivery_tx_offset(opts) == {:ok, nil}

      :ok = PureFileStorage.set_move_positions!(positions, opts)
      assert PureFileStorage.fetch_root_delivery_tx_offset(opts) == {:ok, nil}

      PureFileStorage.terminate(writer)
    end

    test "a cursor-only move commit updates positions without advancing the log boundary", %{
      opts: opts,
      writer: writer
    } do
      offset = LogOffset.new(10, 0)
      positions = %{"dependency" => LogOffset.new(101, 0)}

      writer =
        PureFileStorage.append_to_log!(
          [{offset, "ordinary", :insert, ~S|{"value":"ordinary"}|}],
          writer
        )

      writer = PureFileStorage.hibernate(writer)
      assert_receive {Storage, :flushed, ^offset}
      writer = PureFileStorage.begin_move_transaction!(writer)
      root_delivery_tx_offset = 11

      writer =
        PureFileStorage.commit_move_transaction!(
          positions,
          root_delivery_tx_offset,
          writer
        )

      assert %{
               last_persisted_txn_offset: ^offset,
               move_positions: ^positions,
               root_delivery_tx_offset: ^root_delivery_tx_offset
             } = transaction_state_on_disk(opts)

      writer = PureFileStorage.hibernate(writer)
      simulate_writer_crash(writer, opts)
      restarted_writer = PureFileStorage.init_writer!(opts, @shape)

      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, offset}
      assert PureFileStorage.fetch_move_positions(opts) == {:ok, positions}

      assert PureFileStorage.fetch_root_delivery_tx_offset(opts) ==
               {:ok, root_delivery_tx_offset}

      PureFileStorage.terminate(restarted_writer)
    end
  end

  describe "dependency move chunk-index staging limit" do
    setup [:start_storage]

    @tag chunk_size: 1
    @tag max_deferred_chunk_index_bytes: 128
    test "fails closed and trims partially flushed move data after restart", %{opts: opts} do
      parent = self()
      baseline_offset = LogOffset.new(10, 0)
      old_positions = %{"dependency" => LogOffset.new(100, 0)}
      latest_name = PureFileStorage.latest_name(opts)
      json_file = PureFileStorage.json_file(opts, latest_name)
      chunk_file = PureFileStorage.chunk_file(opts, latest_name)

      {writer_pid, monitor_ref} =
        spawn_monitor(fn ->
          writer = PureFileStorage.init_writer!(opts, @shape)
          PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
          PureFileStorage.mark_snapshot_as_started(opts)
          PureFileStorage.make_new_snapshot!([], opts)

          writer =
            PureFileStorage.append_to_log!(
              [{baseline_offset, "baseline", :insert, ~S|{"value":"baseline"}|}],
              writer
            )

          writer = PureFileStorage.hibernate(writer)
          :ok = PureFileStorage.set_move_positions!(old_positions, opts)

          send(parent, {
            :baseline_persisted,
            self(),
            File.stat!(json_file).size,
            File.read!(chunk_file)
          })

          writer = PureFileStorage.begin_move_transaction!(writer)

          move_rows =
            for op <- 1..3 do
              {LogOffset.new(11, op), "move-#{op}", :insert, ~S|{"value":"move"}|}
            end

          try do
            PureFileStorage.append_to_log!(move_rows, writer)
          rescue
            error in Storage.Error ->
              exit({:storage_error, error.message})
          end
        end)

      assert_receive {:baseline_persisted, ^writer_pid, baseline_json_size,
                      chunk_file_before_move}

      assert_receive {:DOWN, ^monitor_ref, :process, ^writer_pid,
                      {:storage_error, error_message}},
                     5_000

      assert error_message =~ "attempted_bytes=160"
      assert error_message =~ "limit_bytes=128"
      assert error_message =~ "shape_handle=\"#{@shape_handle}\""
      assert File.stat!(json_file).size > baseline_json_size
      assert File.read!(chunk_file) == chunk_file_before_move
      assert transaction_state_on_disk(opts).chunk_index_intent == nil

      # The crashed process owned the volatile writer ETS table. Remove its
      # stale stack registration before starting the replacement writer.
      :ets.delete(opts.stack_ets, @shape_handle)
      restarted_writer = PureFileStorage.init_writer!(opts, @shape)

      assert File.stat!(json_file).size == baseline_json_size
      assert File.read!(chunk_file) == chunk_file_before_move
      assert transaction_state_on_disk(opts).chunk_index_intent == nil
      assert PureFileStorage.fetch_latest_offset(opts) == {:ok, baseline_offset}
      assert PureFileStorage.fetch_move_positions(opts) == {:ok, old_positions}

      assert [~S|{"value":"baseline"}|] ==
               PureFileStorage.get_log_stream(LogOffset.first(), LogOffset.last(), opts)
               |> Enum.to_list()

      PureFileStorage.terminate(restarted_writer)
    end
  end

  defp with_started_writer(%{opts: opts}) do
    writer = PureFileStorage.init_writer!(opts, @shape)
    PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
    PureFileStorage.mark_snapshot_as_started(opts)
    PureFileStorage.make_new_snapshot!([], opts)

    %{writer: writer}
  end

  defp get_log_items_from_storage(min_offset, max_offset, storage_impl) do
    PureFileStorage.get_log_stream(min_offset, max_offset, storage_impl)
    |> Enum.map(&Jason.decode!/1)
  end

  defp storage_internal_state(opts) do
    [metadata] = :ets.lookup(opts.stack_ets, @shape_handle)

    %{
      last_persisted_txn_offset: storage_meta(metadata, :last_persisted_txn_offset),
      last_persisted_offset: storage_meta(metadata, :last_persisted_offset),
      last_seen_txn_offset: storage_meta(metadata, :last_seen_txn_offset),
      last_snapshot_chunk: storage_meta(metadata, :last_snapshot_chunk),
      cached_chunk_boundaries: storage_meta(metadata, :cached_chunk_boundaries)
    }
  end

  defp transaction_state_on_disk(opts) do
    metadata_on_disk(opts, :transaction_state)
  end

  defp metadata_on_disk(opts, key) do
    opts
    |> metadata_path(key)
    |> File.read!()
    |> :erlang.binary_to_term()
  end

  defp write_legacy_metadata!(opts, key, value) do
    File.write!(metadata_path(opts, key), :erlang.term_to_binary(value))
  end

  defp metadata_path(opts, key) do
    PureFileStorage.shape_data_dir(opts, ["metadata", "#{key}.bin"])
  end

  defp persist_staged_chunk_index_intent(opts, writer) do
    staged = stage_chunk_index_intent(opts, writer)

    :ok = PureFileStorage.publish_chunk_index_addressability_bridge!(opts, staged.intent)

    :ok =
      PureFileStorage.commit_transaction_state!(
        opts,
        staged.move_offset,
        staged.new_positions,
        staged.root_delivery_tx_offset,
        staged.intent
      )

    Map.put(staged, :writer, PureFileStorage.hibernate(staged.writer))
  end

  defp stage_chunk_index_intent(opts, writer) do
    baseline_offset = LogOffset.new(10, 0)
    move_offset = LogOffset.new(11, 0)
    root_delivery_tx_offset = 11
    old_positions = %{"dependency" => LogOffset.new(100, 0)}
    new_positions = %{"dependency" => LogOffset.new(101, 0)}
    latest_name = PureFileStorage.latest_name(opts)
    chunk_file = PureFileStorage.chunk_file(opts, latest_name)
    json_file = PureFileStorage.json_file(opts, latest_name)

    writer =
      PureFileStorage.append_to_log!(
        [{baseline_offset, "baseline", :insert, ~S|{"value":"baseline"}|}],
        writer
      )

    :ok = PureFileStorage.set_move_positions!(old_positions, opts)
    chunk_file_before_move = File.read!(chunk_file)
    log_start_position = File.stat!(json_file).size
    writer = PureFileStorage.begin_move_transaction!(writer)

    writer =
      PureFileStorage.append_to_log!(
        [{move_offset, "move", :insert, ~S|{"value":"move"}|}],
        writer
      )

    log_end_position = File.stat!(json_file).size

    intent_bytes =
      PureFileStorage.ChunkIndex.make_half_entry(move_offset, log_start_position, 0) <>
        PureFileStorage.ChunkIndex.make_half_entry(move_offset, log_end_position, 0)

    intent =
      PureFileStorage.new_chunk_index_intent!(
        opts,
        latest_name,
        intent_bytes,
        binary_part(intent_bytes, 0, div(byte_size(intent_bytes), 2))
      )

    %{
      writer: writer,
      chunk_file: chunk_file,
      chunk_file_before_move: chunk_file_before_move,
      baseline_offset: baseline_offset,
      move_offset: move_offset,
      root_delivery_tx_offset: root_delivery_tx_offset,
      new_positions: new_positions,
      intent: intent
    }
  end

  defp append_compaction_candidate(writer) do
    offset = LogOffset.new(10, 0)

    baseline_json =
      Jason.encode!(%{
        key: "baseline",
        value: %{id: "baseline"},
        headers: %{operation: "insert"}
      })

    writer =
      PureFileStorage.append_to_log!(
        [{offset, "baseline", :insert, baseline_json}],
        writer
      )

    {writer, offset}
  end

  defp prepare_cross_generation_replay(writer, opts) do
    first_offset = LogOffset.new(10, 0)
    last_offset = LogOffset.new(20, 0)

    first_json =
      Jason.encode!(%{
        key: "first",
        value: %{id: "first"},
        headers: %{operation: "insert"}
      })

    last_json =
      Jason.encode!(%{
        key: "last",
        value: %{id: "last"},
        headers: %{operation: "insert"}
      })

    writer =
      PureFileStorage.append_to_log!(
        [{first_offset, "first", :insert, first_json}],
        writer
      )

    writer = PureFileStorage.hibernate(writer)
    assert :ok = PureFileStorage.compact(opts, 0)

    assert_receive {Storage,
                    {PureFileStorage, :handle_compaction_finished,
                     [^first_offset, compacted_suffix, log_file_pos]}},
                   5_000

    writer =
      PureFileStorage.handle_compaction_finished(
        writer,
        first_offset,
        compacted_suffix,
        log_file_pos
      )

    writer =
      PureFileStorage.append_to_log!(
        [{last_offset, "last", :insert, last_json}],
        writer
      )

    %{
      writer: PureFileStorage.hibernate(writer),
      first_offset: first_offset,
      first_json: first_json,
      last_offset: last_offset,
      last_json: last_json
    }
  end

  defp patch_log_file_opens(test_pid) do
    Repatch.patch(PureFileStorage, :safely_open_file!, fn reader_opts, path, modes ->
      case Repatch.real(PureFileStorage.safely_open_file!(reader_opts, path, modes)) do
        {:ok, file} = result ->
          send(test_pid, {:log_file_opened, path, file})
          result

        other ->
          other
      end
    end)
  end

  defp open_and_record_log_file(reader_opts, path, modes, test_pid, open_count) do
    case Repatch.real(PureFileStorage.safely_open_file!(reader_opts, path, modes)) do
      {:ok, file} = result ->
        send(test_pid, {:log_file_opened, open_count, path, file})
        result

      other ->
        other
    end
  end

  defp assert_file_closed(file) do
    assert {:error, _reason} = :file.position(file, :cur)
  end

  defp decode_offset_row({offset, json}), do: {offset, Jason.decode!(json)}

  defp append_and_sync!(path, bytes) do
    File.open!(path, [:append, :raw, :sync], fn file ->
      :ok = IO.binwrite(file, bytes)
      :ok = :file.datasync(file)
    end)
  end

  # Version-1 readers consumed a complete chunk wholesale and only applied the
  # published upper bound to the final incomplete chunk. Keep that behavior in
  # this compatibility harness so rolling-deploy regressions stay observable.
  defp legacy_whole_chunk_stream(opts) do
    latest_name = PureFileStorage.latest_name(opts)
    chunk_file = PureFileStorage.chunk_file(opts, latest_name)
    json_file = PureFileStorage.json_file(opts, latest_name)
    min_offset = LogOffset.first()
    max_offset = PureFileStorage.fetch_latest_offset(opts) |> elem(1)

    case ChunkIndex.fetch_chunk(chunk_file, min_offset) do
      {:ok, nil, {start_position, nil}} ->
        LogFile.stream_jsons_until_offset(
          opts,
          json_file,
          start_position,
          min_offset,
          max_offset
        )
        |> Enum.to_list()

      {:ok, _chunk_end, {start_position, end_position}} ->
        LogFile.stream_jsons(
          opts,
          json_file,
          start_position,
          end_position,
          min_offset
        )

      :error ->
        []
    end
  end

  defp simulate_writer_crash(writer_state(ets: ets), opts) do
    :ets.delete(ets)
    :ets.delete(opts.stack_ets, @shape_handle)
  end
end
