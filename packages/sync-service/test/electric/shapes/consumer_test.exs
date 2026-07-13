defmodule Electric.Shapes.ConsumerTest do
  use ExUnit.Case, async: true
  use Repatch.ExUnit, assert_expectations: true

  alias Electric.LsnTracker
  alias Electric.Postgres.Lsn
  alias Electric.Postgres.ReplicationClient
  alias Electric.Replication.Changes.Relation
  alias Electric.Replication.Changes
  alias Electric.Replication.LogOffset
  alias Electric.Replication.ShapeLogCollector
  alias Electric.ShapeCache
  alias Electric.ShapeCache.Storage
  alias Electric.Shapes
  alias Electric.Shapes.Shape
  alias Electric.Shapes.Consumer
  alias Electric.Shapes.Consumer.Materializer
  alias Electric.Shapes.ConsumerRegistry

  alias Support.StubInspector

  import Support.ComponentSetup

  import Support.TestUtils,
    only: [
      expect_calls: 2,
      patch_shape_status: 1,
      expect_shape_status: 1,
      patch_snapshotter: 1,
      assert_shape_cleanup: 1,
      register_as_replication_client: 1,
      complete_txn_fragment: 3,
      txn_fragments: 3,
      txn_fragment: 4
    ]

  @receive_timeout 1_000

  @base_inspector StubInspector.new(
                    tables: [
                      "test_table",
                      "other_table",
                      "something else",
                      {"random", "definitely_different"}
                    ],
                    columns: [
                      %{name: "id", type: "int8", pk_position: 0},
                      %{name: "value", type: "text"}
                    ]
                  )
  @shape_handle1 "#{inspect(__MODULE__)}-shape1"
  @shape1 Shape.new!("public.test_table", inspector: @base_inspector)

  @shape_handle2 "#{inspect(__MODULE__)}-shape2"
  @shape2 Shape.new!("public.other_table", inspector: @base_inspector)

  @shape_handle3 "#{inspect(__MODULE__)}-shape3"
  @shape3 Shape.new!("public.test_table",
            inspector: @base_inspector,
            where: "id = 1"
          )

  @shape_with_compaction Shape.new!("public.test_table",
                           inspector: @base_inspector,
                           storage: %{compaction: :enabled}
                         )

  @shape_with_subquery Shape.new!("public.test_table",
                         inspector: @base_inspector,
                         where: "id IN (SELECT id FROM public.other_table)"
                       )

  @shape_with_subquery_or_value Shape.new!("public.test_table",
                                  inspector: @base_inspector,
                                  where:
                                    "id IN (SELECT id FROM public.other_table) OR value = 'causal-root'"
                                )

  @shape_with_two_subqueries Shape.new!("public.test_table",
                               inspector: @base_inspector,
                               where:
                                 ~S|id IN (SELECT id FROM public.other_table) AND id IN (SELECT id FROM public."something else")|
                             )

  @shape_with_two_subqueries_reversed Shape.new!("public.test_table",
                                        inspector: @base_inspector,
                                        where:
                                          ~S|id IN (SELECT id FROM public."something else") AND id IN (SELECT id FROM public.other_table)|
                                      )

  @shape_with_nested_subquery Shape.new!("public.test_table",
                                inspector: @base_inspector,
                                where:
                                  ~S|id IN (SELECT id FROM public.other_table WHERE id IN (SELECT id FROM public."something else" WHERE value = 'visible'))|
                              )

  @shape_position %{
    @shape_handle1 => %{
      latest_offset: LogOffset.new(Lsn.from_string("0/10"), 0),
      snapshot_xmin: 100
    },
    @shape_handle2 => %{
      latest_offset: LogOffset.new(Lsn.from_string("0/50"), 0),
      snapshot_xmin: 120
    },
    @shape_handle3 => %{
      latest_offset: LogOffset.new(Lsn.from_string("0/1"), 0),
      snapshot_xmin: 10
    }
  }

  @moduletag :tmp_dir

  setup :with_stack_id_from_test

  defp shape_status(shape_handle, ctx) do
    get_in(ctx, [:shape_position, shape_handle]) || raise "invalid shape_handle #{shape_handle}"
  end

  defp log_offset(shape_handle, ctx) do
    get_in(ctx, [:shape_position, shape_handle, :latest_offset]) ||
      raise "invalid shape_handle #{shape_handle}"
  end

  defp snapshot_xmin(shape_handle, ctx) do
    get_in(ctx, [:shape_position, shape_handle, :snapshot_xmin]) ||
      raise "invalid shape_handle #{shape_handle}"
  end

  defp lsn(shape_handle, ctx) do
    %{tx_offset: offset} = log_offset(shape_handle, ctx)
    Lsn.from_integer(offset)
  end

  # Block until `pid` is hibernating, then return its armed suspend-timer ref
  # (or nil if none is armed).
  defp await_hibernation(pid, timeout \\ 2_000) do
    poll_until(timeout, fn ->
      case Process.info(pid, :current_function) do
        {:current_function, {:gen_server, :loop_hibernate, _}} ->
          {:ok, :sys.get_state(pid).suspend_timer}

        _ ->
          :retry
      end
    end)
  end

  # Repeatedly evaluate `fun` until it returns `{:ok, value}` (returning value)
  # or `timeout` ms elapse (failing the test).
  defp poll_until(timeout, fun) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_poll_until(deadline, fun)
  end

  defp do_poll_until(deadline, fun) do
    case fun.() do
      {:ok, value} ->
        value

      :retry ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(50)
          do_poll_until(deadline, fun)
        else
          flunk("poll_until/2 timed out waiting for condition")
        end
    end
  end

  defp assert_two_dependency_restore_replay(shape, expected_dependency_relations, ctx) do
    {shape_handle, _} = ShapeCache.get_or_create_shape_handle(shape, ctx.stack_id)
    :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

    {:ok, persisted_shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)

    dependencies =
      Enum.map(persisted_shape.shape_dependencies_handles, fn dependency_handle ->
        {:ok, dependency_shape} =
          Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, dependency_handle)

        {dependency_handle, dependency_shape.root_table}
      end)

    assert Enum.map(dependencies, &elem(&1, 1)) == expected_dependency_relations

    shape_storage = Storage.for_shape(shape_handle, ctx.storage)
    {:ok, baseline_positions} = Storage.fetch_move_positions(shape_storage)

    assert Map.keys(baseline_positions) |> MapSet.new() ==
             dependencies |> Enum.map(&elem(&1, 0)) |> MapSet.new()

    consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
    consumer_ref = Process.monitor(consumer_pid)
    assert :ok = Consumer.stop(consumer_pid, :shutdown)
    assert_receive {:DOWN, ^consumer_ref, :process, ^consumer_pid, :shutdown}, @receive_timeout
    :ok = Electric.Shapes.ConsumerRegistry.remove_consumer(shape_handle, ctx.stack_id)

    assert Support.TestUtils.wait_until(
             fn -> is_nil(Consumer.whereis(ctx.stack_id, shape_handle)) end,
             @receive_timeout
           )

    lsn = Lsn.from_integer(910)

    changes =
      dependencies
      |> Enum.with_index()
      |> Enum.flat_map(fn {{_dependency_handle, relation}, index} ->
        row = %{"id" => Integer.to_string(index + 1), "value" => "temporary"}
        first_offset = index * 4

        [
          %Changes.NewRecord{
            relation: relation,
            record: row,
            log_offset: LogOffset.new(lsn, first_offset)
          },
          %Changes.DeletedRecord{
            relation: relation,
            old_record: row,
            log_offset: LogOffset.new(lsn, first_offset + 2)
          }
        ]
      end)

    assert :ok =
             ShapeLogCollector.handle_event(
               complete_txn_fragment(910, lsn, changes),
               ctx.stack_id
             )

    dependency_materializers =
      Map.new(dependencies, fn {dependency_handle, _relation} ->
        materializer_pid = Materializer.whereis(ctx.stack_id, dependency_handle)
        assert is_pid(materializer_pid)
        {dependency_handle, materializer_pid}
      end)

    assert :advanced =
             poll_until(@receive_timeout * 10, fn ->
               positions =
                 Map.new(dependency_materializers, fn {dependency_handle, materializer_pid} ->
                   {dependency_handle, :sys.get_state(materializer_pid).durable_offset}
                 end)

               advanced? =
                 Enum.all?(positions, fn {dependency_handle, position} ->
                   LogOffset.compare(position, Map.fetch!(baseline_positions, dependency_handle)) ==
                     :gt
                 end)

               if advanced?, do: {:ok, :advanced}, else: :retry
             end)

    restore_task =
      Task.async(fn -> ShapeCache.start_consumer_for_handle(shape_handle, ctx.stack_id) end)

    assert {:error, _reason} = Task.await(restore_task, @receive_timeout * 5)

    assert :purged =
             poll_until(@receive_timeout * 5, fn ->
               if not Electric.ShapeCache.ShapeStatus.has_shape_handle?(
                    ctx.stack_id,
                    shape_handle
                  ) and
                    is_nil(Consumer.whereis(ctx.stack_id, shape_handle)) do
                 {:ok, :purged}
               else
                 :retry
               end
             end)

    {fresh_handle, _offset} = ShapeCache.get_or_create_shape_handle(shape, ctx.stack_id)
    refute fresh_handle == shape_handle
    assert :started = ShapeCache.await_snapshot_start(fresh_handle, ctx.stack_id)
  end

  describe "event handling" do
    setup [
      :with_registry,
      :with_in_memory_storage,
      :with_shape_status,
      :with_lsn_tracker,
      :with_persistent_kv,
      :with_status_monitor,
      :with_dynamic_consumer_supervisor,
      :with_noop_publication_manager,
      :with_shape_cleaner
    ]

    setup(ctx) do
      shapes = Map.get(ctx, :shapes, %{@shape_handle1 => @shape1, @shape_handle2 => @shape2})
      shape_position = Map.get(ctx, :shape_position, @shape_position)
      [shape_position: shape_position, shapes: shapes]
    end

    setup(ctx) do
      start_link_supervised!({
        ShapeLogCollector.Supervisor,
        stack_id: ctx.stack_id, persistent_kv: ctx.persistent_kv, inspector: @base_inspector
      })

      ShapeLogCollector.mark_as_ready(ctx.stack_id)

      :ok
    end

    setup(ctx) do
      %{latest_offset: _offset1, snapshot_xmin: xmin1} = shape_status(@shape_handle1, ctx)
      %{latest_offset: _offset2, snapshot_xmin: xmin2} = shape_status(@shape_handle2, ctx)

      storage =
        Support.TestStorage.wrap(ctx.storage, %{
          @shape_handle1 => [
            {:mark_snapshot_as_started, []},
            {:set_pg_snapshot, [%{xmin: xmin1, xmax: xmin1 + 1, xip_list: [xmin1]}]}
          ],
          @shape_handle2 => [
            {:mark_snapshot_as_started, []},
            {:set_pg_snapshot, [%{xmin: xmin2, xmax: xmin2 + 1, xip_list: [xmin2]}]}
          ]
        })

      Electric.StackConfig.put(ctx.stack_id, Electric.ShapeCache.Storage, storage)
      Electric.StackConfig.put(ctx.stack_id, :inspector, @base_inspector)

      patch_shape_status(
        fetch_shape_by_handle: fn _, shape_handle -> Map.fetch(ctx.shapes, shape_handle) end
      )

      Support.TestUtils.activate_mocks_for_descendant_procs(Electric.Shapes.Consumer)
      Support.TestUtils.activate_mocks_for_descendant_procs(Electric.ShapeCache.ShapeCleaner)

      consumers =
        for {shape_handle, shape} <- ctx.shapes do
          %{latest_offset: _offset} = shape_status(shape_handle, ctx)

          {:ok, consumer} =
            start_supervised(
              {Shapes.Consumer,
               %{
                 shape_handle: shape_handle,
                 stack_id: ctx.stack_id
               }},
              id: {Shapes.Consumer, shape_handle}
            )

          Shapes.Consumer.initialize_shape(consumer, shape, %{action: :create})

          assert_receive {Support.TestStorage, :init_writer!, ^shape_handle, ^shape}

          :started = Consumer.await_snapshot_start(ctx.stack_id, shape_handle)

          consumer
        end

      [consumers: consumers]
    end

    test "appends to log when xid >= xmin", ctx do
      xid = 150
      xmin = snapshot_xmin(@shape_handle1, ctx)
      last_log_offset = log_offset(@shape_handle1, ctx)
      lsn = lsn(@shape_handle1, ctx)
      next_lsn = Lsn.increment(lsn, 1)
      next_log_offset = LogOffset.new(next_lsn, 0)

      ref = make_ref()

      Registry.register(ctx.registry, @shape_handle1, ref)

      txn =
        complete_txn_fragment(xmin, lsn, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "1"},
            log_offset: last_log_offset
          }
        ])

      assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)
      assert_receive {^ref, :new_changes, ^last_log_offset}, @receive_timeout
      assert_receive {Support.TestStorage, :append_to_log!, @shape_handle1, _}
      refute_storage_calls_for_txn_fragment(@shape_handle2)

      txn2 =
        complete_txn_fragment(xid, next_lsn, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "1"},
            log_offset: next_log_offset
          }
        ])

      assert :ok = ShapeLogCollector.handle_event(txn2, ctx.stack_id)
      assert_receive {^ref, :new_changes, ^next_log_offset}, @receive_timeout
      assert_receive {Support.TestStorage, :append_to_log!, @shape_handle1, _}
      refute_storage_calls_for_txn_fragment(@shape_handle2)
    end

    test "correctly writes only relevant changes to multiple shape logs", ctx do
      expected_log_offset = log_offset(@shape_handle1, ctx)
      lsn = lsn(@shape_handle1, ctx)

      change1_offset = expected_log_offset
      change2_offset = LogOffset.increment(expected_log_offset, 1)
      change3_offset = LogOffset.increment(expected_log_offset, 2)

      xid = 150

      ref1 = make_ref()
      ref2 = make_ref()

      Registry.register(ctx.registry, @shape_handle1, ref1)
      Registry.register(ctx.registry, @shape_handle2, ref2)

      txn =
        complete_txn_fragment(xid, lsn, [
          %Changes.NewRecord{
            relation: {"public", "something else"},
            record: %{"id" => "3"},
            log_offset: change3_offset
          },
          %Changes.NewRecord{
            relation: {"public", "other_table"},
            record: %{"id" => "2"},
            log_offset: change2_offset
          },
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "1"},
            log_offset: change1_offset
          }
        ])

      assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)

      assert_receive {^ref1, :new_changes, ^change1_offset}, @receive_timeout
      assert_receive {^ref2, :new_changes, ^change2_offset}, @receive_timeout

      assert_receive {Support.TestStorage, :append_to_log!, @shape_handle1,
                      [{_offset, _key, _type, serialized_record}]}

      assert %{"value" => %{"id" => "1"}} = Jason.decode!(serialized_record)

      assert_receive {Support.TestStorage, :append_to_log!, @shape_handle2,
                      [{_offset, _key, _type, serialized_record}]}

      assert %{"value" => %{"id" => "2"}} = Jason.decode!(serialized_record)
    end

    @tag shapes: %{
           @shape_handle1 =>
             Shape.new!("public.test_table", where: "id != 1", inspector: @base_inspector),
           @shape_handle2 =>
             Shape.new!("public.test_table", where: "id = 1", inspector: @base_inspector)
         }
    test "doesn't append to log when change is irrelevant for active shapes", ctx do
      xid = 150
      lsn = Lsn.from_string("0/10")
      last_log_offset = LogOffset.new(lsn, 0)

      ref1 = Shapes.Consumer.register_for_changes(ctx.stack_id, @shape_handle1)
      ref2 = Shapes.Consumer.register_for_changes(ctx.stack_id, @shape_handle2)

      txn =
        complete_txn_fragment(xid, lsn, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "1"},
            log_offset: last_log_offset
          }
        ])

      assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)

      assert_receive {Support.TestStorage, :append_to_log!, @shape_handle2, _}
      refute_storage_calls_for_txn_fragment(@shape_handle1)

      refute_receive {^ref1, :new_changes, _}
      assert_receive {^ref2, :new_changes, _}
    end

    test "handles truncate without appending to log", ctx do
      xid = 150
      lsn = Lsn.from_string("0/10")
      last_log_offset = LogOffset.new(lsn, 0)

      expect_shape_status(remove_shape: {fn _, @shape_handle1 -> :ok end, at_least: 1})

      txn =
        complete_txn_fragment(xid, lsn, [
          %Changes.TruncatedRelation{
            relation: {"public", "test_table"},
            log_offset: last_log_offset
          }
        ])

      assert_consumer_shutdown(ctx.stack_id, @shape_handle1, fn ->
        assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)
      end)

      assert_shape_cleanup(@shape_handle1)

      refute_receive {Electric.ShapeCache.ShapeCleaner, :cleanup, @shape_handle2}
    end

    @tag shapes: %{
           @shape_handle1 =>
             Shape.new!("test_table",
               where: "id LIKE 'test'",
               inspector:
                 StubInspector.new(%{
                   {"public", "test_table"} => %{
                     columns: [%{name: "id", type: "text", pk_position: 0}]
                   }
                 })
             )
         }
    test "handles truncate when shape has a where clause", ctx do
      xid = 150
      lsn = Lsn.from_string("0/10")
      last_log_offset = LogOffset.new(lsn, 0)

      expect_shape_status(remove_shape: {fn _, @shape_handle1 -> :ok end, at_least: 1})

      txn =
        complete_txn_fragment(xid, lsn, [
          %Changes.TruncatedRelation{
            relation: {"public", "test_table"},
            log_offset: last_log_offset
          }
        ])

      assert_consumer_shutdown(ctx.stack_id, @shape_handle1, fn ->
        assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)
      end)

      refute_storage_calls_for_txn_fragment(@shape_handle1)

      assert_shape_cleanup(@shape_handle1)

      refute_receive {Electric.ShapeCache.ShapeCleaner, :cleanup, @shape_handle2}
    end

    test "notifies listeners of new changes", ctx do
      xid = 150
      lsn = Lsn.from_string("0/10")
      last_log_offset = LogOffset.new(lsn, 0)

      ref = make_ref()
      Registry.register(ctx.registry, @shape_handle1, ref)

      txn =
        complete_txn_fragment(xid, lsn, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "1"},
            log_offset: last_log_offset
          }
        ])

      assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)
      assert_receive {^ref, :new_changes, ^last_log_offset}, @receive_timeout
      assert_receive {Support.TestStorage, :append_to_log!, @shape_handle1, _}
    end

    test "does not route relation to shapes if relation didn't change", ctx do
      rel =
        %Relation{
          id: @shape1.root_table_id,
          schema: elem(@shape1.root_table, 0),
          table: elem(@shape1.root_table, 1),
          columns: [%{name: "id", type_oid: {1, 1}}, %{name: "value", type_oid: {2, 1}}]
        }

      ref1 = Process.monitor(Consumer.whereis(ctx.stack_id, @shape_handle1))

      ref2 = Process.monitor(Consumer.whereis(ctx.stack_id, @shape_handle2))

      patch_shape_status(
        remove_shape: fn _, shape_handle ->
          raise "Unexpected call to remove_shape: #{shape_handle}"
        end
      )

      assert :ok = ShapeLogCollector.handle_event(rel, ctx.stack_id)

      Repatch.patch(Electric.Shapes.Filter, :affected_shapes, [mode: :shared], fn
        _, _ ->
          raise "Unexpected call to Filter.affected_shapes/2 for unchanged duplicate relation"
      end)

      assert :ok = ShapeLogCollector.handle_event(rel, ctx.stack_id)

      refute_receive {:DOWN, ^ref1, :process, _, _}
      refute_receive {:DOWN, ^ref2, :process, _, _}
    end

    test "cleans shapes affected by a relation rename", ctx do
      {orig_schema, _} = @shape1.root_table
      cleaned_oid = @shape1.root_table_id

      rel = %Relation{
        id: cleaned_oid,
        schema: orig_schema,
        table: "definitely_different",
        columns: []
      }

      ref1 = Process.monitor(Consumer.whereis(ctx.stack_id, @shape_handle1))

      ref2 = Process.monitor(Consumer.whereis(ctx.stack_id, @shape_handle2))

      # also cleans up inspector cache and shape status cache
      expect_calls(
        Electric.Postgres.Inspector,
        clean: fn ^cleaned_oid, _ -> true end
      )

      expect_shape_status(remove_shape: {fn _, @shape_handle1 -> :ok end, at_least: 1})

      assert :ok = ShapeLogCollector.handle_event(rel, ctx.stack_id)

      assert_receive {:DOWN, ^ref1, :process, _, {:shutdown, :cleanup}}
      refute_receive {:DOWN, ^ref2, :process, _, _}

      assert_shape_cleanup(@shape_handle1)
    end

    test "cleans shapes affected by a relation change", ctx do
      ref1 = Process.monitor(Consumer.whereis(ctx.stack_id, @shape_handle1))
      ref2 = Process.monitor(Consumer.whereis(ctx.stack_id, @shape_handle2))

      {orig_schema, orig_table} = @shape1.root_table
      cleaned_oid = @shape1.root_table_id

      rel_before = %Relation{
        id: @shape1.root_table_id,
        schema: orig_schema,
        table: orig_table,
        columns: [%{name: "id", type_oid: {1, 1}}, %{name: "value", type_oid: {2, 1}}]
      }

      assert :ok = ShapeLogCollector.handle_event(rel_before, ctx.stack_id)

      refute_receive {:DOWN, _, :process, _, _}

      rel_changed = %{
        rel_before
        | columns: [%{name: "id", type_oid: {999, 1}}, %{name: "value", type_oid: {2, 1}}],
          affected_columns: ["id"]
      }

      # also cleans up inspector cache and shape status cache
      expect_calls(
        Electric.Postgres.Inspector,
        clean: fn ^cleaned_oid, _ -> true end
      )

      expect_shape_status(remove_shape: {fn _, @shape_handle1 -> :ok end, at_least: 1})

      assert :ok = ShapeLogCollector.handle_event(rel_changed, ctx.stack_id)

      assert_receive {:DOWN, ^ref1, :process, _, {:shutdown, :cleanup}}
      refute_receive {:DOWN, ^ref2, :process, _, _}

      assert_shape_cleanup(@shape_handle1)

      refute_receive {Electric.ShapeCache.ShapeCleaner, :cleanup, @shape_handle2}
    end

    test "notifies live listeners when invalidated", ctx do
      ref1 = Process.monitor(Consumer.whereis(ctx.stack_id, @shape_handle1))

      {orig_schema, orig_table} = @shape1.root_table
      cleaned_oid = @shape1.root_table_id

      rel_before = %Relation{
        id: @shape1.root_table_id,
        schema: orig_schema,
        table: orig_table,
        columns: [%{name: "id", type_oid: {1, 1}}, %{name: "value", type_oid: {2, 1}}]
      }

      assert :ok = ShapeLogCollector.handle_event(rel_before, ctx.stack_id)

      refute_receive {:DOWN, _, :process, _, _}

      live_ref = make_ref()
      Registry.register(ctx.registry, @shape_handle1, live_ref)

      rel_changed = %{
        rel_before
        | columns: [%{name: "id", type_oid: {999, 1}}, %{name: "value", type_oid: {2, 1}}],
          affected_columns: ["id"]
      }

      expect_calls(
        Electric.Postgres.Inspector,
        clean: fn cleaned_oid1, _ -> assert cleaned_oid1 == cleaned_oid end
      )

      expect_shape_status(remove_shape: {fn _, @shape_handle1 -> :ok end, at_least: 1})

      assert :ok = ShapeLogCollector.handle_event(rel_changed, ctx.stack_id)

      assert_receive {:DOWN, ^ref1, :process, _, {:shutdown, :cleanup}}
      assert_receive {^live_ref, :shape_rotation}
      refute_receive {Electric.ShapeCache.ShapeCleaner, :cleanup, @shape_handle2}
    end

    test "consumer crashing stops affected consumer", ctx do
      ref1 = Process.monitor(Consumer.whereis(ctx.stack_id, @shape_handle1))
      ref2 = Process.monitor(Consumer.whereis(ctx.stack_id, @shape_handle2))

      expect_shape_status(remove_shape: {fn _, @shape_handle1 -> :ok end, at_least: 1})

      GenServer.cast(Consumer.whereis(ctx.stack_id, @shape_handle1), :unexpected_cast)

      assert_shape_cleanup(@shape_handle1)

      refute_receive {Electric.ShapeCache.ShapeCleaner, :cleanup, @shape_handle2}

      assert_receive {:DOWN, ^ref1, :process, _, _}
      refute_receive {:DOWN, ^ref2, :process, _, _}
    end
  end

  describe "transaction handling with real storage" do
    @describetag :tmp_dir
    @describetag with_pure_file_storage_opts: [flush_period: 1]

    setup do
      %{inspector: @base_inspector, pool: nil}
    end

    setup [
      :with_registry,
      :with_pure_file_storage,
      :with_shape_status,
      :with_lsn_tracker,
      :with_log_chunking,
      :with_persistent_kv,
      :with_materializer_replay_coordinator,
      :with_async_deleter,
      :with_shape_cleaner,
      :with_shape_log_collector,
      :with_noop_publication_manager,
      :with_status_monitor
    ]

    setup(ctx) do
      delay_snapshot_creation? = Map.get(ctx, :delay_snapshot_creation?)
      test_pid = self()

      patch_snapshotter(fn parent, shape_handle, _shape, %{snapshot_fun: snapshot_fun} ->
        if delay_snapshot_creation? do
          receive do
            {^test_pid, :resume} -> :ok
          end
        end

        pg_snapshot = ctx[:pg_snapshot] || {10, 11, [10]}
        GenServer.cast(parent, {:pg_snapshot_known, shape_handle, pg_snapshot})
        GenServer.cast(parent, {:snapshot_started, shape_handle})
        snapshot_fun.([])
      end)

      :ok
    end

    setup(ctx) do
      Electric.StackConfig.put(
        ctx.stack_id,
        :shape_hibernate_after,
        Map.get(ctx, :hibernate_after, 10_000)
      )

      Electric.StackConfig.put(
        ctx.stack_id,
        :shape_suspend_after,
        Map.get(ctx, :shape_suspend_after, 60_000)
      )

      if not Map.get(ctx, :allow_subqueries, true) do
        Electric.StackConfig.put(ctx.stack_id, :feature_flags, [])
      end

      :ok
    end

    setup ctx do
      %{consumer_supervisor: consumer_supervisor, shape_cache: shape_cache} =
        Support.ComponentSetup.with_shape_cache(ctx)

      %{
        consumer_supervisor: consumer_supervisor,
        shape_cache: shape_cache
      }
    end

    test "duplicate transaction handling is idempotent", ctx do
      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)
      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)

      xid = 11
      lsn = Lsn.from_integer(10)

      txn =
        complete_txn_fragment(xid, lsn, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "1"},
            log_offset: LogOffset.new(lsn, 0)
          },
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "2"},
            log_offset: LogOffset.new(lsn, 2)
          }
        ])

      consumer_pid = Shapes.Consumer.whereis(ctx.stack_id, shape_handle)
      shape_storage = Storage.for_shape(shape_handle, ctx.storage)
      enable_storage_tracer_for(consumer_pid)

      # The event is a transaction fragment containing the entire transaction, therefore
      # we expect a single Storage.append_to_log!() call for it.
      assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)

      assert [
               {Storage, :append_to_log!,
                [
                  [
                    {_, ~s'"public"."test_table"/"1"', :insert, _},
                    {_, ~s'"public"."test_table"/"2"', :insert, _}
                  ],
                  _
                ]}
             ] = Support.Trace.collect_traced_calls()

      last_log_offset = LogOffset.new(lsn, 2)
      assert_receive {^ref, :new_changes, ^last_log_offset}

      assert [op1, op2] =
               get_log_items_from_storage(LogOffset.last_before_real_offsets(), shape_storage)

      # If we encounter & store the same transaction, no new storage calls are expected.
      # In fact, ShapeLogCollector will simply drop this txn since it's already seen its offset before.
      assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)

      assert [] == Support.Trace.collect_traced_calls()

      # We should not re-process the same transaction
      refute_receive {^ref, :new_changes, _}

      assert [op1, op2] ==
               get_log_items_from_storage(LogOffset.last_before_real_offsets(), shape_storage)
    end

    test "skips an already-applied transaction replayed past a fresh log collector", ctx do
      # Simulates a restart: the persistent replication slot replays a transaction
      # the consumer has already applied and persisted. A freshly-started
      # ShapeLogCollector hasn't seen the offset, so (unlike the test above) it
      # won't drop it — the consumer itself must skip it, because its restored
      # `latest_offset` is already at/past the transaction. Otherwise the fragment
      # is re-written to the log (duplicating ops) and re-notified to dependent
      # materializers, which re-apply it and crash.
      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)
      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)

      xid = 11
      lsn = Lsn.from_integer(10)

      txn =
        complete_txn_fragment(xid, lsn, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "1"},
            log_offset: LogOffset.new(lsn, 0)
          }
        ])

      consumer_pid = Shapes.Consumer.whereis(ctx.stack_id, shape_handle)
      shape_storage = Storage.for_shape(shape_handle, ctx.storage)

      # First delivery: applied normally, advancing the consumer's latest_offset.
      assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)
      last_log_offset = LogOffset.new(lsn, 0)
      assert_receive {^ref, :new_changes, ^last_log_offset}

      assert [op1] =
               get_log_items_from_storage(LogOffset.last_before_real_offsets(), shape_storage)

      # Replay the same, already-applied transaction straight to the consumer,
      # bypassing the collector's own offset de-dup (as happens on restart with a
      # fresh collector). The consumer must skip it: no storage write, no
      # notification, log unchanged.
      enable_storage_tracer_for(consumer_pid)

      assert :ok =
               GenServer.call(
                 Shapes.Consumer.name(ctx.stack_id, shape_handle),
                 {:handle_event, txn, Electric.Telemetry.OpenTelemetry.get_current_context()},
                 :infinity
               )

      assert [] == Support.Trace.collect_traced_calls()
      refute_receive {^ref, :new_changes, _}

      assert [op1] ==
               get_log_items_from_storage(LogOffset.last_before_real_offsets(), shape_storage)
    end

    @tag allow_subqueries: false
    test "duplicate txn fragment handling is idempotent", ctx do
      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)
      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)

      xid = 11
      lsn = Lsn.from_integer(10)

      [f1, f2, f3, f4] =
        txn_fragments(xid, lsn, [
          %{
            has_begin?: true,
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "1"},
                log_offset: LogOffset.new(lsn, 0)
              },
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "2"},
                log_offset: LogOffset.new(lsn, 2)
              }
            ]
          },
          %{
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "3"},
                log_offset: LogOffset.new(lsn, 4)
              }
            ]
          },
          %{
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "4"},
                log_offset: LogOffset.new(lsn, 6)
              }
            ]
          },
          %{
            has_commit?: true,
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "5"},
                log_offset: LogOffset.new(lsn, 8)
              }
            ]
          }
        ])

      consumer_pid = Shapes.Consumer.whereis(ctx.stack_id, shape_handle)
      enable_storage_tracer_for(consumer_pid)

      assert :ok = ShapeLogCollector.handle_event(f1, ctx.stack_id)

      assert [
               {Storage, :append_fragment_to_log!,
                [
                  [
                    {_, ~s'"public"."test_table"/"1"', :insert, _}
                  ],
                  _
                ]}
             ] = Support.Trace.collect_traced_calls()

      # Repeat and observe idempotency
      assert :ok = ShapeLogCollector.handle_event(f1, ctx.stack_id)
      assert [] == Support.Trace.collect_traced_calls()

      assert :ok = ShapeLogCollector.handle_event(f2, ctx.stack_id)
      assert :ok = ShapeLogCollector.handle_event(f3, ctx.stack_id)

      assert [
               {Storage, :append_fragment_to_log!,
                [[{_, ~s'"public"."test_table"/"2"', :insert, _}], _]},
               {Storage, :append_fragment_to_log!,
                [[{_, ~s'"public"."test_table"/"3"', :insert, _}], _]}
             ] = Support.Trace.collect_traced_calls()

      # Repeat and observe idempotency
      assert :ok = ShapeLogCollector.handle_event(f2, ctx.stack_id)
      assert :ok = ShapeLogCollector.handle_event(f3, ctx.stack_id)
      assert [] == Support.Trace.collect_traced_calls()

      assert :ok = ShapeLogCollector.handle_event(f4, ctx.stack_id)

      assert [
               {Storage, :append_fragment_to_log!,
                [
                  [
                    {_, ~s'"public"."test_table"/"4"', :insert, _},
                    {_, ~s'"public"."test_table"/"5"', :insert, _}
                  ],
                  _
                ]},
               {Storage, :signal_txn_commit!, [^xid, _]}
             ] = Support.Trace.collect_traced_calls()

      last_log_offset = LogOffset.new(lsn, 8)
      assert_receive {^ref, :new_changes, ^last_log_offset}

      # Repeat and observe idempotency
      assert :ok = ShapeLogCollector.handle_event(f4, ctx.stack_id)
      assert [] == Support.Trace.collect_traced_calls()
      refute_receive {^ref, :new_changes, _}
    end

    @tag allow_subqueries: false
    test "skips an already-applied multi-fragment transaction replayed past a fresh log collector",
         ctx do
      # Multi-fragment variant of "skips an already-applied transaction replayed
      # past a fresh log collector". On restart the persistent slot can replay a
      # multi-statement transaction the consumer has already applied. This drives
      # the offset-dedup on the multi-fragment path (BEGIN / middle / COMMIT
      # fragments), not the single-fragment fast path — the consumer must skip
      # every fragment without re-writing or re-notifying.
      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)
      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)

      xid = 11
      lsn = Lsn.from_integer(10)

      [f1, f2, f3] =
        txn_fragments(xid, lsn, [
          %{
            has_begin?: true,
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "1"},
                log_offset: LogOffset.new(lsn, 0)
              }
            ]
          },
          %{
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "2"},
                log_offset: LogOffset.new(lsn, 2)
              }
            ]
          },
          %{
            has_commit?: true,
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "3"},
                log_offset: LogOffset.new(lsn, 4)
              }
            ]
          }
        ])

      consumer_pid = Shapes.Consumer.whereis(ctx.stack_id, shape_handle)
      shape_storage = Storage.for_shape(shape_handle, ctx.storage)

      # First delivery via the collector: applied normally, advancing latest_offset
      # to the commit offset.
      Enum.each([f1, f2, f3], &assert(:ok = ShapeLogCollector.handle_event(&1, ctx.stack_id)))

      commit_offset = LogOffset.new(lsn, 4)
      assert_receive {^ref, :new_changes, ^commit_offset}

      assert [_, _, _] =
               ops =
               get_log_items_from_storage(LogOffset.last_before_real_offsets(), shape_storage)

      # Replay every fragment straight to the consumer, bypassing the collector's
      # offset de-dup (as on restart with a fresh collector). The consumer's restored
      # latest_offset is already at the commit, so all fragments — including the
      # BEGIN fragment, which now skips without ever setting up `pending_txn` — must
      # be skipped: no storage writes, no notification, log unchanged.
      enable_storage_tracer_for(consumer_pid)

      Enum.each([f1, f2, f3], fn f ->
        assert :ok =
                 GenServer.call(
                   Shapes.Consumer.name(ctx.stack_id, shape_handle),
                   {:handle_event, f, Electric.Telemetry.OpenTelemetry.get_current_context()},
                   :infinity
                 )
      end)

      assert [] == Support.Trace.collect_traced_calls()
      refute_receive {^ref, :new_changes, _}

      assert ops ==
               get_log_items_from_storage(LogOffset.last_before_real_offsets(), shape_storage)
    end

    @tag allow_subqueries: false
    test "skips replayed trailing fragments after the final shape-visible change", ctx do
      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)
      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)

      xid = 11
      lsn = Lsn.from_integer(10)

      [begin_fragment, commit_fragment] =
        txn_fragments(xid, lsn, [
          %{
            has_begin?: true,
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "1"},
                log_offset: LogOffset.new(lsn, 0)
              }
            ]
          },
          %{
            has_commit?: true,
            changes: [
              %Changes.NewRecord{
                relation: {"public", "other_table"},
                record: %{"id" => "2"},
                log_offset: LogOffset.new(lsn, 2)
              }
            ]
          }
        ])

      consumer_pid = Shapes.Consumer.whereis(ctx.stack_id, shape_handle)
      shape_storage = Storage.for_shape(shape_handle, ctx.storage)

      Enum.each([begin_fragment, commit_fragment], fn fragment ->
        assert :ok = ShapeLogCollector.handle_event(fragment, ctx.stack_id)
      end)

      shape_offset = LogOffset.new(lsn, 0)
      assert_receive {^ref, :new_changes, ^shape_offset}
      assert {:ok, ^shape_offset} = Storage.fetch_latest_offset(shape_storage)

      assert [stored_op] =
               get_log_items_from_storage(
                 LogOffset.last_before_real_offsets(),
                 shape_storage
               )

      # A fresh collector can replay the complete PostgreSQL transaction. The
      # persisted shape cursor points at its final matching row, not at the
      # later filtered commit fragment, so both fragments must still be treated
      # as one already-applied transaction.
      enable_storage_tracer_for(consumer_pid)

      Enum.each([begin_fragment, commit_fragment], fn fragment ->
        assert :ok =
                 GenServer.call(
                   consumer_pid,
                   {:handle_event, fragment,
                    Electric.Telemetry.OpenTelemetry.get_current_context()},
                   :infinity
                 )
      end)

      assert Process.alive?(consumer_pid)
      assert [] == Support.Trace.collect_traced_calls()
      refute_receive {^ref, :new_changes, _}

      assert [stored_op] ==
               get_log_items_from_storage(
                 LogOffset.last_before_real_offsets(),
                 shape_storage
               )
    end

    @tag pg_snapshot: {10, 13, [10, 12]},
         delay_snapshot_creation?: true,
         with_pure_file_storage_opts: [flush_period: 1]
    test "transactions are buffered until snapshot xmin is known", ctx do
      register_as_replication_client(ctx.stack_id)

      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)
      assert_receive {:snapshot, ^shape_handle, snapshotter_pid}

      lsn1 = Lsn.from_integer(9)
      lsn2 = Lsn.from_integer(10)
      lsn3 = Lsn.from_integer(11)
      lsn4 = Lsn.from_integer(12)
      lsn5 = Lsn.from_integer(13)

      # This transaction will be considered flushed because its xid < snapshot's xmin
      txn1 =
        complete_txn_fragment(9, lsn1, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "1"},
            log_offset: LogOffset.new(lsn1, 0)
          },
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "2"},
            log_offset: LogOffset.new(lsn1, 2)
          }
        ])

      # This transaction will be written to storage because its xid is in snapshot's xip_list
      txn2 =
        complete_txn_fragment(10, lsn2, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "3"},
            log_offset: LogOffset.new(lsn2, 0)
          },
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "4"},
            log_offset: LogOffset.new(lsn2, 2)
          }
        ])

      # This transaction will be considered flushed because its xid > snapshot's xmin but it's not in xip_list
      txn3 =
        complete_txn_fragment(11, lsn3, [
          %Changes.UpdatedRecord{
            key: ~s'"public"."test_table"/"1"',
            relation: {"public", "test_table"},
            old_record: %{"id" => "1"},
            record: %{"id" => "1", "ha" => "ha"},
            log_offset: LogOffset.new(lsn3, 0)
          }
        ])

      # This transaction will be written to storage because its xid is in snapshot's xip_list
      txn4 =
        complete_txn_fragment(12, lsn4, [
          %Changes.DeletedRecord{
            relation: {"public", "test_table"},
            old_record: %{"id" => "3"},
            log_offset: LogOffset.new(lsn4, 0)
          }
        ])

      # This transaction will be written to storage (with no filtering applied) because its xid >= snapshot's xmax
      txn5 =
        complete_txn_fragment(13, lsn5, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "5"},
            log_offset: LogOffset.new(lsn5, 0)
          }
        ])

      ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)

      consumer_pid = Shapes.Consumer.whereis(ctx.stack_id, shape_handle)
      enable_storage_tracer_for(consumer_pid)

      Enum.each([txn1, txn2, txn3, txn4, txn5], fn txn ->
        assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)
      end)

      # No storage calls and no new changes at this point because the consumer process does not yet have snapshot info.
      assert [] == Support.Trace.collect_traced_calls()
      refute_receive {^ref, :new_changes, _}
      refute_receive {:flush_boundary_updated, _}

      shape_storage = Storage.for_shape(shape_handle, ctx.storage)

      assert [] ==
               get_log_items_from_storage(LogOffset.last_before_real_offsets(), shape_storage)

      # Make the actual snapshot
      send(snapshotter_pid, {self(), :resume})
      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      # Verify storage calls and new change notifications
      last_log_offset_txn2 = LogOffset.new(lsn2, 2)
      assert_receive {^ref, :new_changes, ^last_log_offset_txn2}
      last_log_offset_txn4 = LogOffset.new(lsn4, 0)
      assert_receive {^ref, :new_changes, ^last_log_offset_txn4}
      last_log_offset_txn5 = LogOffset.new(lsn5, 0)
      assert_receive {^ref, :new_changes, ^last_log_offset_txn5}
      refute_receive {^ref, :new_changes, _}

      assert [
               {Storage, :append_to_log!, [log_items_txn2, _]},
               {Storage, :append_to_log!, [log_items_txn4, _]},
               {Storage, :append_to_log!, [log_items_txn5, _]}
             ] = Support.Trace.collect_traced_calls()

      traced_log_items =
        Stream.concat([log_items_txn2, log_items_txn4, log_items_txn5])
        |> Enum.map(fn {_log_offset, _key, _op, json} -> Jason.decode!(json) end)

      assert 4 == length(traced_log_items)

      assert traced_log_items ==
               get_log_items_from_storage(LogOffset.last_before_real_offsets(), shape_storage)

      # Verify that the last transaction is successfully flushed and the replication client can confirm its offset
      tx_offset = last_log_offset_txn5.tx_offset
      assert_receive {:flush_boundary_updated, ^tx_offset}
    end

    @tag allow_subqueries: false,
         delay_snapshot_creation?: true,
         with_pure_file_storage_opts: [flush_period: 1]
    test "transaction fragments are buffered until snapshot xmin is known", ctx do
      register_as_replication_client(ctx.stack_id)

      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)
      assert_receive {:snapshot, ^shape_handle, snapshotter_pid}

      xid1 = 90
      lsn1 = Lsn.from_integer(9)

      xid2 = 100
      lsn2 = Lsn.from_integer(10)

      txn1_fragments =
        txn_fragments(xid1, lsn1, [
          %{
            has_begin?: true,
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "1"},
                log_offset: LogOffset.new(lsn1, 0)
              },
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "2"},
                log_offset: LogOffset.new(lsn1, 2)
              }
            ]
          },
          %{
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "3"},
                log_offset: LogOffset.new(lsn1, 4)
              }
            ]
          },
          %{
            has_commit?: true,
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "4"},
                log_offset: LogOffset.new(lsn1, 6)
              }
            ]
          }
        ])

      txn2_fragments =
        txn_fragments(xid2, lsn2, [
          %{
            has_begin?: true,
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "5"},
                log_offset: LogOffset.new(lsn2, 0)
              },
              %Changes.UpdatedRecord{
                relation: {"public", "test_table"},
                old_record: %{"id" => "1"},
                record: %{"id" => "1", "foo" => "bar"},
                log_offset: LogOffset.new(lsn2, 2),
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
                log_offset: LogOffset.new(lsn2, 4),
                changed_columns: MapSet.new(["another"])
              }
            ]
          },
          %{
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "6"},
                log_offset: LogOffset.new(lsn2, 6)
              }
            ]
          },
          %{
            has_commit?: true,
            changes: [
              %Changes.DeletedRecord{
                relation: {"public", "test_table"},
                old_record: %{"id" => "2"},
                log_offset: LogOffset.new(lsn2, 8)
              }
            ]
          }
        ])

      ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)

      consumer_pid = Shapes.Consumer.whereis(ctx.stack_id, shape_handle)
      enable_storage_tracer_for(consumer_pid)

      Enum.each(txn1_fragments, fn fragment ->
        assert :ok = ShapeLogCollector.handle_event(fragment, ctx.stack_id)
      end)

      [txn2_f1, txn2_f2, txn2_f3, txn2_f4] = txn2_fragments
      assert :ok = ShapeLogCollector.handle_event(txn2_f1, ctx.stack_id)
      assert :ok = ShapeLogCollector.handle_event(txn2_f2, ctx.stack_id)

      # No storage calls and no new changes at this point because the consumer process does not yet have snapshot info.
      assert [] == Support.Trace.collect_traced_calls()

      refute_receive {^ref, :new_changes, _}
      refute_receive {:flush_boundary_updated, _}

      shape_storage = Storage.for_shape(shape_handle, ctx.storage)

      assert [] ==
               get_log_items_from_storage(LogOffset.last_before_real_offsets(), shape_storage)

      # The latest storage offset is the initial value since no snapshot has been written yet
      assert {:ok, LogOffset.last_before_real_offsets()} ==
               Storage.fetch_latest_offset(shape_storage)

      # Make the actual snapshot
      send(snapshotter_pid, {self(), :resume})
      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      # Observe that the first txn gets written to storage and flushed, but the second one is still in progress.
      last_log_offset = LogOffset.new(lsn1, 6)
      assert_receive {^ref, :new_changes, ^last_log_offset}

      assert [
               # 1st txn
               {Storage, :append_fragment_to_log!,
                [[{_, ~s'"public"."test_table"/"1"', :insert, _}] = log_items1, _]},
               {Storage, :append_fragment_to_log!,
                [[{_, ~s'"public"."test_table"/"2"', :insert, _}] = log_items2, _]},
               {Storage, :append_fragment_to_log!,
                [
                  [
                    {_, ~s'"public"."test_table"/"3"', :insert, _},
                    {_, ~s'"public"."test_table"/"4"', :insert, _}
                  ] = log_items3,
                  _
                ]},
               {Storage, :signal_txn_commit!, [^xid1, _]},
               # 2nd txn, incomplete
               {Storage, :append_fragment_to_log!,
                [[{_, ~s'"public"."test_table"/"5"', :insert, _}] = log_items_txn2_1, _]},
               {Storage, :append_fragment_to_log!,
                [[{_, ~s'"public"."test_table"/"1"', :update, _}] = log_items_txn2_2, _]}
             ] = Support.Trace.collect_traced_calls()

      traced_log_items =
        Stream.concat([log_items1, log_items2, log_items3])
        |> Enum.map(fn {_log_offset, _key, _op, json} -> Jason.decode!(json) end)

      assert 4 == length(traced_log_items)

      assert traced_log_items ==
               get_log_items_from_storage(LogOffset.last_before_real_offsets(), shape_storage)

      assert {:ok, last_log_offset} == Storage.fetch_latest_offset(shape_storage)

      # Feed the remaining txn2 fragments to the consumer and observe the 2nd transaction getting flushed
      assert :ok = ShapeLogCollector.handle_event(txn2_f3, ctx.stack_id)

      # 2nd txn is still not visible in storage
      assert [] == get_log_items_from_storage(last_log_offset, shape_storage)

      assert :ok = ShapeLogCollector.handle_event(txn2_f4, ctx.stack_id)

      last_log_offset = LogOffset.new(lsn2, 8)
      assert_receive {^ref, :new_changes, ^last_log_offset}

      tx_offset = last_log_offset.tx_offset
      assert_receive {:flush_boundary_updated, ^tx_offset}

      assert [
               {Storage, :append_fragment_to_log!,
                [[{_, ~s'"public"."test_table"/"3"', :update, _}] = log_items_txn2_3, _]},
               {Storage, :append_fragment_to_log!,
                [
                  [
                    {_, ~s'"public"."test_table"/"6"', :insert, _},
                    {_, ~s'"public"."test_table"/"2"', :delete, _}
                  ] = log_items_txn2_4,
                  _
                ]},
               {Storage, :signal_txn_commit!, [^xid2, _]}
             ] = Support.Trace.collect_traced_calls()

      traced_log_items =
        Stream.concat([log_items_txn2_1, log_items_txn2_2, log_items_txn2_3, log_items_txn2_4])
        |> Enum.map(fn {_log_offset, _key, _op, json} -> Jason.decode!(json) end)

      assert 5 == length(traced_log_items)

      assert traced_log_items ==
               get_log_items_from_storage(LogOffset.new(lsn1, 6), shape_storage)

      assert {:ok, last_log_offset} == Storage.fetch_latest_offset(shape_storage)
    end

    @tag allow_subqueries: false,
         pg_snapshot: {10, 13, [10]},
         with_pure_file_storage_opts: [flush_period: 1]
    test "fragments that belong to transactions already included in the snapshot are skipped",
         ctx do
      register_as_replication_client(ctx.stack_id)

      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)
      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      lsn1 = Lsn.from_integer(9)
      lsn2 = Lsn.from_integer(10)
      lsn3 = Lsn.from_integer(11)

      # Txn 1 (xid=9 < xmin=10): will be considered flushed, all fragments skipped
      txn1_fragments =
        txn_fragments(9, lsn1, [
          %{
            has_begin?: true,
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "1"},
                log_offset: LogOffset.new(lsn1, 0)
              },
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "2"},
                log_offset: LogOffset.new(lsn1, 2)
              }
            ]
          },
          %{
            has_commit?: true,
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "3"},
                log_offset: LogOffset.new(lsn1, 4)
              }
            ]
          }
        ])

      # Txn 2 (xid=10 in xip_list): will be written to storage
      txn2_fragments =
        txn_fragments(10, lsn2, [
          %{
            has_begin?: true,
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "10"},
                log_offset: LogOffset.new(lsn2, 0)
              }
            ]
          },
          %{
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "11"},
                log_offset: LogOffset.new(lsn2, 2)
              }
            ]
          },
          %{
            has_commit?: true,
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "12"},
                log_offset: LogOffset.new(lsn2, 4)
              }
            ]
          }
        ])

      # Txn 3 (xid=11, >= xmin but not in xip_list): will be considered flushed
      txn3_fragments =
        txn_fragments(11, lsn3, [
          %{
            has_begin?: true,
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "20"},
                log_offset: LogOffset.new(lsn3, 0)
              }
            ]
          },
          %{
            has_commit?: true,
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "21"},
                log_offset: LogOffset.new(lsn3, 2)
              }
            ]
          }
        ])

      ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)

      consumer_pid = Shapes.Consumer.whereis(ctx.stack_id, shape_handle)
      enable_storage_tracer_for(consumer_pid)

      # Send all fragments before snapshot is known - they should be buffered
      Enum.each(txn1_fragments ++ txn2_fragments ++ txn3_fragments, fn frag ->
        assert :ok = ShapeLogCollector.handle_event(frag, ctx.stack_id)
      end)

      # Verify storage calls
      # Only txn2 (xid=10, in xip_list) should be written to storage
      # txn1 (xid=9 < xmin) and txn3 (xid=11, not in xip_list) should be skipped
      txn2_offset1 = LogOffset.new(lsn2, 0)
      txn2_offset2 = LogOffset.new(lsn2, 2)
      txn2_offset3 = LogOffset.new(lsn2, 4)

      assert [
               {Storage, :append_fragment_to_log!,
                [[{^txn2_offset1, ~s'"public"."test_table"/"10"', :insert, _}], _]},
               {Storage, :append_fragment_to_log!,
                [
                  [
                    {^txn2_offset2, ~s'"public"."test_table"/"11"', :insert, _},
                    {^txn2_offset3, ~s'"public"."test_table"/"12"', :insert, _}
                  ],
                  _
                ]},
               {Storage, :signal_txn_commit!, [10, _]}
             ] = Support.Trace.collect_traced_calls()

      last_log_offset = txn2_offset3
      assert_receive {^ref, :new_changes, ^last_log_offset}
      refute_receive {^ref, :new_changes, _}

      # Verify the shape log only contains txn2's records
      shape_storage = Storage.for_shape(shape_handle, ctx.storage)

      assert [
               %{"key" => ~s'"public"."test_table"/"10"', "value" => %{"id" => "10"}},
               %{"key" => ~s'"public"."test_table"/"11"', "value" => %{"id" => "11"}},
               %{
                 "key" => ~s'"public"."test_table"/"12"',
                 "value" => %{"id" => "12"},
                 "headers" => %{"last" => true}
               }
             ] = get_log_items_from_storage(LogOffset.last_before_real_offsets(), shape_storage)

      # Verify flush boundary is updated to the last transaction's offset
      # txn3 (lsn3) is the last transaction processed, even though it was skipped
      tx_offset = Lsn.to_integer(lsn3)
      assert_receive {:flush_boundary_updated, ^tx_offset}
    end

    test "restarting a consumer doesn't lower the last known offset when only snapshot is present",
         ctx do
      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      assert {_, offset1} = ShapeCache.resolve_shape_handle(shape_handle, @shape1, ctx.stack_id)
      assert offset1 == LogOffset.last_before_real_offsets()

      ref = ctx.consumer_supervisor |> GenServer.whereis() |> Process.monitor()
      # Stop the consumer and the shape cache server to simulate a restart
      stop_supervised!(ctx.consumer_supervisor)
      assert_receive {:DOWN, ^ref, :process, _pid, _reason}, 1000

      shape_cache_pid = ctx.stack_id |> ShapeCache.name() |> GenServer.whereis()
      assert is_pid(shape_cache_pid)
      ref = Process.monitor(shape_cache_pid)
      stop_supervised!(ctx.shape_cache)
      assert_receive {:DOWN, ^ref, :process, _pid, _reason}, 1000

      stop_supervised!("shape_task_supervisor")

      # Restart the shape cache and the consumers
      Support.ComponentSetup.with_shape_cache(ctx)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      assert {_, offset2} = ShapeCache.resolve_shape_handle(shape_handle, @shape1, ctx.stack_id)

      assert LogOffset.compare(offset2, offset1) != :lt
    end

    @tag with_pure_file_storage_opts: [flush_period: 50]
    test "should correctly normalize a flush boundary to txn", ctx do
      register_as_replication_client(ctx.stack_id)

      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape3, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      lsn = Lsn.from_integer(10)

      txn =
        complete_txn_fragment(10, lsn, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "1"},
            log_offset: LogOffset.new(lsn, 2)
          },
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "21"},
            log_offset: LogOffset.new(lsn, 0)
          }
        ])

      assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)

      assert_receive {:flush_boundary_updated, 10}, 1_000
    end

    @tag pg_snapshot: {10, 15, [12]}
    test "should notify txns skipped because of xmin/xip as flushed", ctx do
      register_as_replication_client(ctx.stack_id)

      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)
      lsn1 = Lsn.from_integer(300)
      lsn2 = Lsn.from_integer(301)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      txn =
        complete_txn_fragment(2, lsn1, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "21"},
            log_offset: LogOffset.new(lsn1, 0)
          }
        ])

      txn2 =
        complete_txn_fragment(11, lsn2, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "21"},
            log_offset: LogOffset.new(lsn2, 0)
          }
        ])

      assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)
      assert :ok = ShapeLogCollector.handle_event(txn2, ctx.stack_id)

      assert_receive {:flush_boundary_updated, 300}, 1_000
      assert_receive {:flush_boundary_updated, 301}, 1_000
    end

    @tag hibernate_after: 10, shape_suspend_after: 20
    @tag with_pure_file_storage_opts: [flush_period: 1]
    @tag suspend: true
    test "should suspend after hibernate_after + shape_suspend_after ms", ctx do
      register_as_replication_client(ctx.stack_id)

      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)
      lsn1 = Lsn.from_integer(300)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      assert is_pid(consumer_pid)
      ref = Process.monitor(consumer_pid)

      txn =
        complete_txn_fragment(2, lsn1, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "21"},
            log_offset: LogOffset.new(lsn1, 0)
          }
        ])

      assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)

      assert_receive {:flush_boundary_updated, 300}, 1_000

      # The consumer hibernates, then suspends shape_suspend_after later;
      assert_receive {:DOWN, ^ref, :process, ^consumer_pid, {:shutdown, :suspend}}, 200

      refute Consumer.whereis(ctx.stack_id, shape_handle)
    end

    @tag hibernate_after: 10, shape_suspend_after: 10
    @tag with_pure_file_storage_opts: [flush_period: 1]
    @tag suspend: true
    test "should hibernate not suspend if has dependencies", ctx do
      register_as_replication_client(ctx.stack_id)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      lsn1 = Lsn.from_integer(300)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      assert is_pid(consumer_pid)

      assert {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)

      assert [dependent_shape_handle] = shape.shape_dependencies_handles

      txn =
        complete_txn_fragment(2, lsn1, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "21"},
            log_offset: LogOffset.new(lsn1, 0)
          }
        ])

      assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)

      assert_receive {:flush_boundary_updated, 300}, 1_000

      # A shape with dependencies hibernates but never arms a suspend timer
      # (consumer_can_suspend? is false), so it can never suspend. Observing a
      # nil suspend_timer once hibernated proves this deterministically.
      assert is_nil(await_hibernation(consumer_pid))

      dependent_consumer_pid = Consumer.whereis(ctx.stack_id, dependent_shape_handle)
      assert is_nil(await_hibernation(dependent_consumer_pid))

      assert is_pid(Consumer.whereis(ctx.stack_id, shape_handle))
    end

    @tag hibernate_after: 10,
         shape_suspend_after: 20,
         with_pure_file_storage_opts: [flush_period: 1]
    @tag suspend: true
    test "should hibernate not suspend while a multi-fragment transaction is pending", ctx do
      register_as_replication_client(ctx.stack_id)

      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)
      lsn1 = Lsn.from_integer(300)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      assert is_pid(consumer_pid)
      ref = Process.monitor(consumer_pid)

      # The begin fragment of a multi-fragment transaction leaves the consumer
      # holding a pending_txn until the matching commit fragment arrives.
      begin_fragment =
        txn_fragment(
          2,
          lsn1,
          [
            %Changes.NewRecord{
              relation: {"public", "test_table"},
              record: %{"id" => "21"},
              log_offset: LogOffset.new(lsn1, 0)
            }
          ],
          has_begin?: true,
          has_commit?: false
        )

      assert :ok = ShapeLogCollector.handle_event(begin_fragment, ctx.stack_id)

      # The idle timer (hibernate_after: 10ms) fires, but with a transaction still
      # pending the consumer must hibernate rather than suspend, so it survives to
      # receive the rest of the transaction. Suspending here would drop pending_txn
      # and crash on the next fragment (issue #4501).
      refute_receive {:DOWN, ^ref, :process, ^consumer_pid, {:shutdown, :suspend}}, 400

      assert {:current_function, {:gen_server, :loop_hibernate, 4}} =
               Process.info(consumer_pid, :current_function)

      assert is_pid(Consumer.whereis(ctx.stack_id, shape_handle))

      # Completing the transaction clears pending_txn, so the consumer is free to
      # suspend on the next idle timeout.
      commit_fragment =
        txn_fragment(
          2,
          lsn1,
          [
            %Changes.NewRecord{
              relation: {"public", "test_table"},
              record: %{"id" => "22"},
              log_offset: LogOffset.new(lsn1, 1)
            }
          ],
          has_begin?: false,
          has_commit?: true
        )

      assert :ok = ShapeLogCollector.handle_event(commit_fragment, ctx.stack_id)

      assert_receive {:flush_boundary_updated, 300}, 1_000

      assert_receive {:DOWN, ^ref, :process, ^consumer_pid, {:shutdown, :suspend}}

      refute Consumer.whereis(ctx.stack_id, shape_handle)
    end

    @tag with_pure_file_storage_opts: [flush_period: 1]
    @tag suspend: false
    test "ConsumerRegistry.enable_suspend should suspend hibernated consumers", ctx do
      register_as_replication_client(ctx.stack_id)

      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)
      lsn1 = Lsn.from_integer(300)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      assert is_pid(consumer_pid)
      ref = Process.monitor(consumer_pid)

      txn =
        complete_txn_fragment(2, lsn1, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "21"},
            log_offset: LogOffset.new(lsn1, 0)
          }
        ])

      assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)

      assert_receive {:flush_boundary_updated, 300}, 1_000

      # Suspend is disabled (@tag suspend: false), so the consumer never suspends
      # on its own and stays alive.
      refute_receive {:DOWN, ^ref, :process, ^consumer_pid, {:shutdown, :suspend}}, 100
      assert Consumer.whereis(ctx.stack_id, shape_handle)

      # hibernate_after=5, shape_suspend_after=5, jitter_period=10
      Shapes.ConsumerRegistry.enable_suspend(ctx.stack_id, 5, 5, 10)

      # Enabling suspend on the live consumer makes it suspend on the next cycle.
      assert_receive {:DOWN, ^ref, :process, ^consumer_pid, {:shutdown, :suspend}}, 200
      refute Consumer.whereis(ctx.stack_id, shape_handle)
    end

    @tag hibernate_after: 10,
         shape_suspend_after: 150,
         with_pure_file_storage_opts: [flush_period: 1]
    @tag suspend: true
    test "should hibernate first then suspend after shape_suspend_after ms", ctx do
      register_as_replication_client(ctx.stack_id)

      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)
      lsn1 = Lsn.from_integer(300)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      assert is_pid(consumer_pid)
      ref = Process.monitor(consumer_pid)

      txn =
        complete_txn_fragment(2, lsn1, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "21"},
            log_offset: LogOffset.new(lsn1, 0)
          }
        ])

      assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)

      assert_receive {:flush_boundary_updated, 300}, 1_000

      # The consumer hibernates first (for GC) and arms a suspend timer rather
      # than suspending directly. Observing an armed timer while hibernated
      # proves the "hibernate, then suspend" ordering without racing the clock.
      assert is_reference(await_hibernation(consumer_pid))
      assert Process.alive?(consumer_pid)

      # It then suspends once shape_suspend_after elapses.
      assert_receive {:DOWN, ^ref, :process, ^consumer_pid, {:shutdown, :suspend}}, 300

      refute Consumer.whereis(ctx.stack_id, shape_handle)
    end

    @tag hibernate_after: 10,
         shape_suspend_after: 200,
         with_pure_file_storage_opts: [flush_period: 1]
    @tag suspend: true
    test "activity during hibernation cancels pending suspend", ctx do
      register_as_replication_client(ctx.stack_id)

      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)
      lsn1 = Lsn.from_integer(300)
      lsn2 = Lsn.from_integer(301)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      assert is_pid(consumer_pid)
      ref = Process.monitor(consumer_pid)

      txn1 =
        complete_txn_fragment(2, lsn1, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "21"},
            log_offset: LogOffset.new(lsn1, 0)
          }
        ])

      assert :ok = ShapeLogCollector.handle_event(txn1, ctx.stack_id)
      assert_receive {:flush_boundary_updated, 300}, 1_000

      # Once hibernated, a suspend timer is armed.
      ref1 = await_hibernation(consumer_pid)
      assert is_reference(ref1)

      # Activity (a new transaction) must cancel that timer.
      txn2 =
        complete_txn_fragment(3, lsn2, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "22"},
            log_offset: LogOffset.new(lsn2, 0)
          }
        ])

      assert :ok = ShapeLogCollector.handle_event(txn2, ctx.stack_id)
      assert_receive {:flush_boundary_updated, 301}, 1_000

      # After re-hibernating, a *fresh* timer is armed and the original one reads
      # as cancelled - proving the activity reset the suspend cycle rather than
      # letting the original timer fire.
      ref2 = await_hibernation(consumer_pid)
      assert is_reference(ref2)
      assert ref2 != ref1
      assert :erlang.read_timer(ref1) == false

      # No suspend happened.
      refute_receive {:DOWN, ^ref, :process, ^consumer_pid, {:shutdown, :suspend}}, 0

      # Process should still be alive (hibernated again)
      assert Process.alive?(consumer_pid)
    end

    @tag with_pure_file_storage_opts: [compaction_period: 5, keep_complete_chunks: 133]
    test "compaction is scheduled and invoked for a shape that has compaction enabled", ctx do
      parent = self()
      ref = make_ref()

      fun = fn _shape_opts, 133 ->
        send(parent, {:consumer_did_invoke_compact, ref})
        :ok
      end

      Repatch.patch(Electric.ShapeCache.PureFileStorage, :compact, [mode: :shared], fun)
      Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)

      {_shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_compaction, ctx.stack_id)

      assert_receive {:consumer_did_invoke_compact, ^ref}
    end

    test "terminating the consumers cleans up its entry from Storage ETS", ctx do
      import Electric.ShapeCache.PureFileStorage.SharedRecords, only: [storage_meta: 2]

      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      assert {_, offset1} = ShapeCache.resolve_shape_handle(shape_handle, @shape1, ctx.stack_id)
      assert offset1 == LogOffset.last_before_real_offsets()

      table = Electric.ShapeCache.PureFileStorage.stack_ets(ctx.stack_id)

      assert [shape_meta] = :ets.tab2list(table)
      assert storage_meta(shape_meta, :shape_handle) == shape_handle
      assert storage_meta(shape_meta, :last_persisted_offset) == offset1

      assert :ok == Consumer.stop(ctx.stack_id, shape_handle, "reason")

      assert_receive {Electric.ShapeCache.ShapeCleaner, :cleanup, ^shape_handle}

      assert [] == :ets.tab2list(table)
    end

    @tag allow_subqueries: false, with_pure_file_storage_opts: [flush_period: 1]
    test "writes txn fragments to storage immediately but keeps txn boundaries when flushing",
         ctx do
      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)

      register_as_replication_client(ctx.stack_id)

      xid = 11
      lsn = Lsn.from_integer(10)

      fragments =
        txn_fragments(xid, lsn, [
          %{
            has_begin?: true,
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "1"},
                log_offset: LogOffset.new(lsn, 0)
              },
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "2"},
                log_offset: LogOffset.new(lsn, 2)
              }
            ]
          },
          %{
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "3"},
                log_offset: LogOffset.new(lsn, 4)
              }
            ]
          },
          %{
            changes: [
              %Changes.NewRecord{
                relation: {"public", "test_table"},
                record: %{"id" => "4"},
                log_offset: LogOffset.new(lsn, 6)
              }
            ]
          }
        ])

      expected_log_items = [
        [{LogOffset.new(lsn, 0), ~s'"public"."test_table"/"1"', :insert}],
        [{LogOffset.new(lsn, 2), ~s'"public"."test_table"/"2"', :insert}],
        [{LogOffset.new(lsn, 4), ~s'"public"."test_table"/"3"', :insert}]
      ]

      consumer_pid = Shapes.Consumer.whereis(ctx.stack_id, shape_handle)
      shape_storage = Storage.for_shape(shape_handle, ctx.storage)
      enable_storage_tracer_for(consumer_pid)

      Enum.zip(fragments, expected_log_items)
      |> Enum.each(fn {fragment, expected_log_items} ->
        assert :ok = ShapeLogCollector.handle_event(fragment, ctx.stack_id)

        assert [{Storage, :append_fragment_to_log!, [log_items, _]}] =
                 Support.Trace.collect_traced_calls()

        assert expected_log_items ==
                 Enum.map(log_items, fn {log_offset, key, op, _json} -> {log_offset, key, op} end)
      end)

      # Nothing should be returned from the shape log until a fragment containing Commit is stored
      assert [] ==
               get_log_items_from_storage(LogOffset.last_before_real_offsets(), shape_storage)

      # The latest storage offset corresponds to the only persisted snapshot chunk
      assert {:ok, LogOffset.new(0, 0)} == Storage.fetch_latest_offset(shape_storage)

      refute_receive {^ref, :new_changes, _}
      refute_receive {:flush_boundary_updated, _}

      commit_fragment =
        txn_fragment(
          xid,
          lsn,
          [
            %Changes.NewRecord{
              relation: {"public", "test_table"},
              record: %{"id" => "5"},
              log_offset: LogOffset.new(lsn, 8)
            }
          ],
          has_commit?: true
        )

      assert :ok = ShapeLogCollector.handle_event(commit_fragment, ctx.stack_id)

      last_log_offset = LogOffset.new(lsn, 8)

      assert [
               {Storage, :append_fragment_to_log!,
                [
                  [
                    {_, ~s'"public"."test_table"/"4"', :insert, _},
                    {^last_log_offset, ~s'"public"."test_table"/"5"', :insert, _json}
                  ],
                  _
                ]},
               {Storage, :signal_txn_commit!, [^xid, _]}
             ] = Support.Trace.collect_traced_calls()

      assert [
               %{"key" => ~s'"public"."test_table"/"1"', "value" => %{"id" => "1"}},
               %{"key" => ~s'"public"."test_table"/"2"', "value" => %{"id" => "2"}},
               %{"key" => ~s'"public"."test_table"/"3"', "value" => %{"id" => "3"}},
               %{"key" => ~s'"public"."test_table"/"4"', "value" => %{"id" => "4"}},
               %{
                 "key" => ~s'"public"."test_table"/"5"',
                 "value" => %{"id" => "5"},
                 "headers" => %{"last" => true}
               }
             ] = get_log_items_from_storage(LogOffset.last_before_real_offsets(), shape_storage)

      assert {:ok, last_log_offset} == Storage.fetch_latest_offset(shape_storage)

      assert_receive {^ref, :new_changes, ^last_log_offset}

      offset = last_log_offset.tx_offset
      assert_receive {:flush_boundary_updated, ^offset}
    end

    @tag allow_subqueries: false, with_pure_file_storage_opts: [flush_period: 1]
    test "flush notification for multi-fragment txn is not lost when storage flushes before commit fragment",
         %{stack_id: stack_id} = ctx do
      # Regression test for https://github.com/electric-sql/electric/issues/3985
      # Updated for deferred flush notification fix (#4063).
      #
      # When a multi-fragment transaction's non-commit fragments are flushed to disk
      # before the commit fragment is processed by ShapeLogCollector, the flush
      # notification was lost because FlushTracker wasn't tracking the shape's offsets.
      # This caused the shape to be stuck in the FlushTracker, blocking
      # the global flush offset from advancing.
      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, stack_id)
      shape_storage = Storage.for_shape(shape_handle, ctx.storage)

      ref = Shapes.Consumer.register_for_changes(stack_id, shape_handle)

      register_as_replication_client(stack_id)

      xid = 11
      lsn = Lsn.from_integer(10)

      # Create non-commit fragments with matching changes
      fragment1 =
        txn_fragment(
          xid,
          lsn,
          [
            %Changes.NewRecord{
              relation: {"public", "test_table"},
              record: %{"id" => "1"},
              log_offset: LogOffset.new(lsn, 0)
            },
            %Changes.NewRecord{
              relation: {"public", "test_table"},
              record: %{"id" => "2"},
              log_offset: LogOffset.new(lsn, 2)
            }
          ],
          has_begin?: true
        )

      fragment2 =
        txn_fragment(
          xid,
          lsn,
          [
            %Changes.NewRecord{
              relation: {"public", "test_table"},
              record: %{"id" => "3"},
              log_offset: LogOffset.new(lsn, 4)
            }
          ],
          []
        )

      Support.Trace.trace_shape_log_collector_calls(
        pid: Shapes.Consumer.whereis(stack_id, shape_handle),
        functions: [:notify_flushed]
      )

      # Send non-commit fragments. With flush_period: 1ms, the storage will flush
      # almost immediately after writing.
      assert :ok = ShapeLogCollector.handle_event(fragment1, stack_id)
      assert :ok = ShapeLogCollector.handle_event(fragment2, stack_id)

      # With deferred flush notifications, notify_flushed is NOT called
      # after non-commit fragments. The flush is deferred until the commit.
      assert [] == Support.Trace.collect_traced_calls()

      # Now send the commit fragment. The commit fragment itself has NO matching
      # changes for the shape — all changes were in earlier fragments.
      commit_fragment =
        txn_fragment(
          xid,
          lsn,
          [
            %Changes.NewRecord{
              relation: {"public", "other_table"},
              record: %{"id" => "99"},
              log_offset: LogOffset.new(lsn, 6)
            }
          ],
          has_commit?: true
        )

      assert :ok = ShapeLogCollector.handle_event(commit_fragment, ctx.stack_id)
      assert_receive {^ref, :new_changes, _}, @receive_timeout

      # The commit fragment had no shape-visible row. Fragment streaming keeps
      # one relevant change pending, so the real final row can carry `last=true`
      # without manufacturing a public no-op event or losing replay boundaries.
      assert %{
               "key" => ~s'"public"."test_table"/"3"',
               "headers" => %{
                 "operation" => "insert",
                 "last" => true
               }
             } =
               LogOffset.last_before_real_offsets()
               |> get_log_items_from_storage(shape_storage)
               |> List.last()

      # The deferred flush notification is sent after the commit. The exact
      # offset depends on alignment with txn_offset_mapping, so we only
      # verify that notify_flushed was called for this shape.
      assert Enum.any?(Support.Trace.collect_traced_calls(), fn
               {ShapeLogCollector, :notify_flushed, [^stack_id, ^shape_handle, _offset]} -> true
               _ -> false
             end)

      # Flush boundary advances.
      tx_offset = commit_fragment.last_log_offset.tx_offset
      assert_receive {:flush_boundary_updated, ^tx_offset}, @receive_timeout
    end

    @tag allow_subqueries: false, with_pure_file_storage_opts: [flush_period: 10_000]
    test "flush notification offset is aligned when storage flushes before commit arrives at consumer",
         %{stack_id: stack_id} do
      # Regression test for https://github.com/electric-sql/electric/issues/4063
      #
      # When a non-commit fragment has enough data to trigger a buffer-size
      # flush (>= 64KB), the :flushed message is placed in the consumer's
      # mailbox during processing. The consumer process ends up handling the :flushed message
      # before receiving the commit fragment. But since the offset it sends to FlushTracker
      # predates the commit fragment's offset, the FlushTracker keeps the shape in the
      # "pending" state and there's no follow-up notification from the consumer that would
      # unblock it.
      #
      # A high flush_period prevents timer-based flushes so the only flush
      # comes from the buffer-size trigger, making the test deterministic.

      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, stack_id)
      :started = ShapeCache.await_snapshot_start(shape_handle, stack_id)

      ref = Shapes.Consumer.register_for_changes(stack_id, shape_handle)
      register_as_replication_client(stack_id)

      xid = 11
      lsn = Lsn.from_integer(10)
      relevant_change_offset = LogOffset.new(lsn, 0)

      # The fragment has a large shape-relevant record (>64KB) that triggers a
      # buffer-size flush during write, PLUS a non-matching record at a higher
      # offset. This means the source fragment's last_log_offset is higher than
      # the shape's last written offset — just like in production where
      # transactions touch multiple tables.
      padding = String.duplicate("x", 70_000)

      non_commit_fragment =
        txn_fragment(
          xid,
          lsn,
          [
            %Changes.NewRecord{
              relation: {"public", "test_table"},
              record: %{"id" => "1", "value" => padding},
              log_offset: relevant_change_offset
            },
            # This change does NOT match shape1 (test_table) but raises the
            # fragment's last_log_offset above the shape's written offset.
            %Changes.NewRecord{
              relation: {"public", "other_table"},
              record: %{"id" => "2"},
              log_offset: LogOffset.new(lsn, 50)
            }
          ],
          has_begin?: true
        )

      # Commit fragment has only a change for a different table. The consumer
      # writes nothing for it but still finalises the pending transaction,
      # populating txn_offset_mapping.
      commit_fragment =
        txn_fragment(
          xid,
          lsn,
          [
            %Changes.NewRecord{
              relation: {"public", "other_table"},
              record: %{"id" => "99"},
              log_offset: LogOffset.new(lsn, 100)
            }
          ],
          has_commit?: true
        )

      # Send non-commit fragment. The large record triggers a buffer flush,
      # placing {Storage, :flushed, offset} in the consumer's mailbox.
      Support.Trace.trace_shape_log_collector_calls(
        pid: Shapes.Consumer.whereis(stack_id, shape_handle),
        functions: [:notify_flushed]
      )

      assert :ok = ShapeLogCollector.handle_event(non_commit_fragment, stack_id)

      # With deferred flush notifications, the consumer does NOT call notify_flushed
      # after the non-commit fragment. The :flushed message is saved for later.
      assert [] == Support.Trace.collect_traced_calls()

      # Send the commit fragment to finalize the transaction.
      assert :ok = ShapeLogCollector.handle_event(commit_fragment, stack_id)

      # Consumer has processed the relevant change...
      assert_receive {^ref, :new_changes, ^relevant_change_offset}, @receive_timeout

      # The deferred flush notification is sent after the commit with the
      # aligned offset (the commit fragment's last_log_offset).
      commit_last_log_offset = commit_fragment.last_log_offset

      assert [
               {ShapeLogCollector, :notify_flushed,
                [^stack_id, ^shape_handle, ^commit_last_log_offset]}
             ] = Support.Trace.collect_traced_calls()

      # Flush boundary advances correctly.
      tx_offset = commit_fragment.last_log_offset.tx_offset
      assert_receive {:flush_boundary_updated, ^tx_offset}, @receive_timeout
    end

    @tag with_pure_file_storage_opts: [flush_period: 10_000]
    test "coalesced storage flush carries a later no-op transaction boundary", %{
      stack_id: stack_id
    } do
      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape3, stack_id)
      :started = ShapeCache.await_snapshot_start(shape_handle, stack_id)
      register_as_replication_client(stack_id)

      consumer_pid = Shapes.Consumer.whereis(stack_id, shape_handle)

      Support.Trace.trace_shape_log_collector_calls(
        pid: consumer_pid,
        functions: [:notify_flushed]
      )

      write_lsn = Lsn.from_integer(10)
      written_offset = LogOffset.new(write_lsn, 0)

      assert :ok =
               ShapeLogCollector.handle_event(
                 complete_txn_fragment(11, write_lsn, [
                   %Changes.NewRecord{
                     relation: {"public", "test_table"},
                     record: %{"id" => "1"},
                     log_offset: written_offset
                   }
                 ]),
                 stack_id
               )

      no_op_lsn = Lsn.from_integer(20)

      no_op_txn =
        complete_txn_fragment(12, no_op_lsn, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "2"},
            log_offset: LogOffset.new(no_op_lsn, 0)
          }
        ])

      assert :ok =
               GenServer.call(
                 consumer_pid,
                 {:handle_event, no_op_txn,
                  Electric.Telemetry.OpenTelemetry.get_current_context()},
                 :infinity
               )

      no_op_boundary = no_op_txn.last_log_offset

      assert :sys.get_state(consumer_pid).txn_offset_mapping == [
               {written_offset, no_op_boundary}
             ]

      assert [] == Support.Trace.collect_traced_calls()

      # A physical flush may include bytes beyond the mapped shape write (for
      # example, a later generated control entry). It still covers every
      # transaction boundary relabelled onto the earlier write.
      coalesced_physical_flush = LogOffset.new(write_lsn, 2)
      send(consumer_pid, {Storage, :flushed, coalesced_physical_flush})
      assert :sys.get_state(consumer_pid).txn_offset_mapping == []

      assert [
               {ShapeLogCollector, :notify_flushed, [^stack_id, ^shape_handle, ^no_op_boundary]}
             ] = Support.Trace.collect_traced_calls()
    end

    @tag allow_subqueries: false, with_pure_file_storage_opts: [flush_period: 1]
    test "dead consumer doesn't block flush notifications from advancing as live consumers flush to storage",
         ctx do
      {shape_handle1, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)
      {shape_handle2, _} = ShapeCache.get_or_create_shape_handle(@shape2, ctx.stack_id)
      ref1 = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle1)
      ref2 = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle2)

      :started = ShapeCache.await_snapshot_start(shape_handle1, ctx.stack_id)
      :started = ShapeCache.await_snapshot_start(shape_handle2, ctx.stack_id)

      register_as_replication_client(ctx.stack_id)

      lsn1 = Lsn.from_integer(10)

      # First txn affects both shapes
      txn1 =
        complete_txn_fragment(11, lsn1, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "1"},
            log_offset: LogOffset.new(lsn1, 0)
          },
          %Changes.NewRecord{
            relation: {"public", "other_table"},
            record: %{"id" => "1"},
            log_offset: LogOffset.new(lsn1, 2)
          }
        ])

      assert :ok = ShapeLogCollector.handle_event(txn1, ctx.stack_id)
      assert_receive {^ref1, :new_changes, _}, @receive_timeout
      assert_receive {^ref2, :new_changes, _}, @receive_timeout

      # Both consumers flush. We get two flush boundary notifications because
      # at the time of the first consumer flush FlushTracker didn't yet have a real
      # last_global_flushed_offset, so it eagerly confirms that a "virtual previous txn" with
      # offset (10 - 1) - 1 has definitely been flushed.
      # When the second consumer's flush arrives at it, it can see that there are no more
      # pending flushes for lsn=10 and so it has definitely been flushed now.
      assert_receive {:flush_boundary_updated, 8}, @receive_timeout
      assert_receive {:flush_boundary_updated, 10}, @receive_timeout

      # Terminate the consumer for shape2. Using :shutdown as the exit reason
      # means ShapeCleaner.handle_writer_termination/3 does NOT remove the shape
      # from ShapeLogCollector, so it stays in the FlushTracker indefinitely.
      dead_consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle2)
      dead_ref = Process.monitor(dead_consumer_pid)
      Process.exit(dead_consumer_pid, :shutdown)
      assert_receive {:DOWN, ^dead_ref, :process, ^dead_consumer_pid, :shutdown}

      lsn2 = Lsn.from_integer(20)

      # Second txn affects both shapes, but the dead consumer won't flush
      txn2 =
        complete_txn_fragment(12, lsn2, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "2"},
            log_offset: LogOffset.new(lsn2, 0)
          },
          %Changes.NewRecord{
            relation: {"public", "other_table"},
            record: %{"id" => "2"},
            log_offset: LogOffset.new(lsn2, 2)
          }
        ])

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          # By the time this call to handle_event() returns, the dead consumer will have been
          # removed from FlushTracker's state, so it can advance its confirmed flushed offset to
          # the last processed transaction.
          assert :ok = ShapeLogCollector.handle_event(txn2, ctx.stack_id)
        end)

      assert log =~
               ~s'Consumer processes crashed or missing during broadcast: %{#{inspect(shape_handle2)} => :noproc}'

      assert_receive {^ref1, :new_changes, _}, @receive_timeout
      assert_receive {:flush_boundary_updated, 20}, @receive_timeout

      lsn3 = Lsn.from_integer(30)

      # Third txn affects only the live shape
      txn3 =
        complete_txn_fragment(13, lsn3, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "3"},
            log_offset: LogOffset.new(lsn3, 0)
          }
        ])

      assert :ok = ShapeLogCollector.handle_event(txn3, ctx.stack_id)
      assert_receive {^ref1, :new_changes, _}, @receive_timeout

      # shape1 has flushed all the way through lsn 30 so that's what we expect FlushTracker to
      # advance its confirmed offset to.
      assert_receive {:flush_boundary_updated, 30}, @receive_timeout
    end

    test "UPDATE during pending move-in is converted to INSERT and query result skips duplicate key",
         ctx do
      # This test exposes an edge case where:
      # 1. A move-in query starts (snapshot xmin = 90)
      # 2. An UPDATE (xid = 100) arrives and is converted to INSERT
      # 3. Move-in query completes with the same key
      # 4. EXPECTED: Query result should skip the key (already processed at xid 100 > snapshot xmin 90)
      # 5. ACTUAL BUG: Query result creates a duplicate INSERT

      parent = self()

      # Mock query_move_in_async to simulate a query without hitting the database
      Repatch.patch(
        Electric.Shapes.Consumer.Effects,
        :query_move_in_async,
        [mode: :shared],
        fn _task_sup, _consumer_state, _buffering_state, consumer_pid ->
          send(parent, {:query_requested, consumer_pid})

          :ok
        end
      )

      Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [_dep_handle] = shape.shape_dependencies_handles

      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)

      ShapeLogCollector.handle_event(
        complete_txn_fragment(100, Lsn.from_integer(50), [
          %Changes.NewRecord{
            relation: {"public", "other_table"},
            record: %{"id" => "1"},
            log_offset: LogOffset.new(Lsn.from_integer(50), 0)
          }
        ]),
        ctx.stack_id
      )

      assert_receive {:query_requested, ^consumer_pid}, @receive_timeout

      # Snapshot here is intentionally before the update to make sure the update is considered shadowing
      send(consumer_pid, {:pg_snapshot_known, {90, 95, []}})

      # Now send an UPDATE (xid = 100) before move-in query completes
      # This should be converted to INSERT
      lsn = Lsn.from_integer(100)
      xid = 100

      txn =
        complete_txn_fragment(xid, lsn, [
          %Changes.UpdatedRecord{
            relation: {"public", "test_table"},
            old_record: %{"id" => "1"},
            key: ~s'"public"."test_table"/"1"',
            record: %{"id" => "1", "value" => "updated"},
            log_offset: LogOffset.new(lsn, 0)
          }
        ])

      assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)

      shape_storage = Storage.for_shape(shape_handle, ctx.storage)

      send_stored_move_in_complete(
        consumer_pid,
        shape_storage,
        [
          [
            ~s'"public"."test_table"/"1"',
            [],
            Jason.encode!(%{
              "key" => ~s'"public"."test_table"/"1"',
              "value" => %{"id" => "1", "value" => "old"},
              "headers" => %{
                "operation" => "insert",
                "relation" => ["public", "test_table"]
              }
            })
          ]
        ],
        Lsn.from_integer(100)
      )

      assert_receive {^ref, :new_changes, _offset}, @receive_timeout

      # Check storage for operations
      assert :updated =
               poll_until(@receive_timeout, fn ->
                 items =
                   get_log_items_from_storage(
                     LogOffset.last_before_real_offsets(),
                     shape_storage
                   )

                 if Enum.any?(items, &match?(%{"headers" => %{"operation" => "update"}}, &1)),
                   do: {:ok, :updated},
                   else: :retry
               end)

      assert [
               %{"headers" => %{"event" => "move-in"}},
               %{
                 "headers" => %{"operation" => "insert"},
                 "key" => ~s'"public"."test_table"/"1"',
                 "value" => %{"id" => "1", "value" => "old"}
               },
               %{
                 "headers" => %{
                   "control" => "snapshot-end",
                   "xmin" => "90",
                   "xmax" => "95",
                   "xip_list" => []
                 }
               },
               %{
                 "headers" => %{"operation" => "update", "txids" => [100]},
                 "key" => ~s'"public"."test_table"/"1"'
               },
               %{
                 "headers" => %{"event" => "move-out", "last" => true}
               }
             ] = get_log_items_from_storage(LogOffset.last_before_real_offsets(), shape_storage)
    end

    test "consumer splices a pending move-in on global_last_seen_lsn broadcast", ctx do
      parent = self()

      Repatch.patch(
        Electric.Shapes.Consumer.Effects,
        :query_move_in_async,
        [mode: :shared],
        fn _task_sup, _consumer_state, _buffering_state, consumer_pid ->
          send(parent, {:query_requested, consumer_pid})
          :ok
        end
      )

      Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)

      ShapeLogCollector.handle_event(
        complete_txn_fragment(100, Lsn.from_integer(50), [
          %Changes.NewRecord{
            relation: {"public", "other_table"},
            record: %{"id" => "1"},
            log_offset: LogOffset.new(Lsn.from_integer(50), 0)
          }
        ]),
        ctx.stack_id
      )

      assert_receive {:query_requested, ^consumer_pid}, @receive_timeout

      send(consumer_pid, {:pg_snapshot_known, {100, 300, []}})

      shape_storage = Storage.for_shape(shape_handle, ctx.storage)

      send_stored_move_in_complete(
        consumer_pid,
        shape_storage,
        [
          [
            ~s'"public"."test_table"/"1"',
            [],
            Jason.encode!(%{
              "key" => ~s'"public"."test_table"/"1"',
              "value" => %{"id" => "1", "value" => "old"},
              "headers" => %{
                "operation" => "insert",
                "relation" => ["public", "test_table"]
              }
            })
          ]
        ],
        Lsn.from_integer(100)
      )

      refute_receive {^ref, :new_changes, _}, 100

      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 100)
      assert_receive {^ref, :new_changes, _offset}, @receive_timeout

      assert [
               %{"headers" => %{"event" => "move-in"}},
               %{
                 "headers" => %{"operation" => "insert"},
                 "key" => ~s'"public"."test_table"/"1"',
                 "value" => %{"id" => "1", "value" => "old"}
               },
               %{
                 "headers" => %{
                   "control" => "snapshot-end",
                   "xmin" => "100",
                   "xmax" => "300",
                   "xip_list" => []
                 }
               },
               %{
                 "headers" => %{"event" => "move-out", "last" => true}
               }
             ] = get_log_items_from_storage(LogOffset.last_before_real_offsets(), shape_storage)
    end

    test "fragmented root begin cannot advance the durable frontier before commit", ctx do
      parent = self()

      Repatch.patch(
        Electric.Shapes.Consumer.Effects,
        :query_move_in_async,
        [mode: :shared],
        fn _task_sup, _consumer_state, _buffering_state, consumer_pid ->
          send(parent, {:query_requested, consumer_pid})
          :ok
        end
      )

      Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery_or_value, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      shape_storage = Storage.for_shape(shape_handle, ctx.storage)

      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 50)

      send(
        consumer_pid,
        {:materializer_changes, dep_handle,
         %{
           move_in: [{1, "1"}],
           move_out: [],
           txids: [50],
           lsn: LogOffset.new(50, 0)
         }}
      )

      assert_receive {:query_requested, ^consumer_pid}, @receive_timeout

      root_change = %Changes.NewRecord{
        relation: {"public", "test_table"},
        record: %{"id" => "2", "value" => "causal-root"},
        key: ~s'"public"."test_table"/"2"',
        log_offset: LogOffset.new(100, 0)
      }

      [root_begin, root_commit] =
        txn_fragments(100, Lsn.from_integer(100), [
          %{changes: [root_change], has_begin?: true, has_commit?: false},
          %{changes: [], has_begin?: false, has_commit?: true}
        ])

      root_commit = %{root_commit | last_log_offset: LogOffset.new(100, 1)}

      assert :ok = ShapeLogCollector.handle_event(root_begin, ctx.stack_id)

      assert :fragment_pending =
               poll_until(@receive_timeout, fn ->
                 state = :sys.get_state(consumer_pid)

                 if not is_nil(state.pending_txn),
                   do: {:ok, :fragment_pending},
                   else: :retry
               end)

      # The move-in snapshot finishes after PostgreSQL committed tx100, while
      # this Consumer has only received its BEGIN fragment. Neither the in-memory
      # nor durable root frontier may claim tx100 was evaluated until the
      # matching COMMIT fragment reaches the handler.
      send(consumer_pid, {:pg_snapshot_known, {90, 200, []}})

      send_stored_move_in_complete(
        consumer_pid,
        shape_storage,
        [
          [
            ~s'"public"."test_table"/"1"',
            [],
            Jason.encode!(%{
              "key" => ~s'"public"."test_table"/"1"',
              "value" => %{"id" => "1", "value" => "dependency-row"},
              "headers" => %{
                "operation" => "insert",
                "relation" => ["public", "test_table"]
              }
            })
          ]
        ],
        Lsn.from_integer(100)
      )

      # Both messages above were sent by this test process, so this synchronous
      # call is handled only after the Consumer has observed query completion.
      before_commit = :sys.get_state(consumer_pid)

      assert before_commit.move_transaction_open?
      assert before_commit.pending_txn != nil
      assert before_commit.last_processed_replication_tx_offset < 100

      assert {:ok, durable_before_commit} =
               Storage.fetch_root_delivery_tx_offset(shape_storage)

      assert durable_before_commit < 100

      waiter = Task.async(fn -> Consumer.await_causal_frontier(consumer_pid, 100) end)
      assert Task.yield(waiter, 100) == nil

      assert :ok = ShapeLogCollector.handle_event(root_commit, ctx.stack_id)
      assert :ok = Task.await(waiter, @receive_timeout)

      assert :transaction_complete =
               poll_until(@receive_timeout, fn ->
                 state = :sys.get_state(consumer_pid)

                 if is_nil(state.pending_txn) and
                      state.last_processed_replication_tx_offset == 100 do
                   {:ok, :transaction_complete}
                 else
                   :retry
                 end
               end)
    end

    test "move-in applies its global frontier only after every deferred root reaches the handler",
         ctx do
      parent = self()

      Repatch.patch(
        Electric.Shapes.Consumer.Effects,
        :query_move_in_async,
        [mode: :shared],
        fn _task_sup, _consumer_state, _buffering_state, consumer_pid ->
          send(parent, {:query_requested, consumer_pid})
          :ok
        end
      )

      Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)
      shape_storage = Storage.for_shape(shape_handle, ctx.storage)

      # Seed row 1 through the dependency so the client-visible shape already
      # contains the logical row before the second move-in starts.
      send(
        consumer_pid,
        {:materializer_changes, dep_handle,
         %{
           move_in: [{1, "1"}],
           move_out: [],
           txids: [40],
           lsn: LogOffset.new(40, 0)
         }}
      )

      assert_receive {:query_requested, ^consumer_pid}, @receive_timeout
      send(consumer_pid, {:pg_snapshot_known, {1, 40, []}})

      send_stored_move_in_complete(
        consumer_pid,
        shape_storage,
        [
          [
            ~s'"public"."test_table"/"1"',
            [],
            Jason.encode!(%{
              "key" => ~s'"public"."test_table"/"1"',
              "value" => %{"id" => "1", "value" => "before"},
              "headers" => %{
                "operation" => "insert",
                "relation" => ["public", "test_table"]
              }
            })
          ]
        ],
        Lsn.from_integer(40)
      )

      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 40)
      assert_receive {^ref, :new_changes, _offset}, @receive_timeout

      # A dependency transaction starts a move-in query for id=2. Its snapshot
      # includes the later root PK update, but replication for that update is
      # queued behind the open move transaction.
      send(
        consumer_pid,
        {:materializer_changes, dep_handle,
         %{
           move_in: [{2, "2"}],
           move_out: [],
           txids: [50],
           lsn: LogOffset.new(50, 0)
         }}
      )

      assert_receive {:query_requested, ^consumer_pid}, @receive_timeout
      send(consumer_pid, {:pg_snapshot_known, {90, 200, []}})

      send_stored_move_in_complete(
        consumer_pid,
        shape_storage,
        [
          [
            ~s'"public"."test_table"/"2"',
            [],
            Jason.encode!(%{
              "key" => ~s'"public"."test_table"/"2"',
              "value" => %{"id" => "2", "value" => "after"},
              "headers" => %{
                "operation" => "insert",
                "relation" => ["public", "test_table"]
              }
            })
          ]
        ],
        Lsn.from_integer(100)
      )

      # A second dependency transaction committed before the root transaction,
      # so it sorts ahead of that root in the normal deferred-work scheduler.
      # It must not let the later global frontier splice the active snapshot
      # before the root transaction has been classified against that snapshot.
      send(
        consumer_pid,
        {:materializer_changes, dep_handle,
         %{
           move_in: [],
           move_out: [],
           txids: [75],
           lsn: LogOffset.new(75, 0)
         }}
      )

      assert :dependency_deferred =
               poll_until(@receive_timeout, fn ->
                 if :sys.get_state(consumer_pid).deferred_materializer_move_count == 1,
                   do: {:ok, :dependency_deferred},
                   else: :retry
               end)

      update =
        Changes.UpdatedRecord.new(%{
          relation: {"public", "test_table"},
          old_record: %{"id" => "1", "value" => "before"},
          record: %{"id" => "2", "value" => "after"},
          old_key: ~s'"public"."test_table"/"1"',
          key: ~s'"public"."test_table"/"2"',
          log_offset: LogOffset.new(100, 0)
        })

      [root_begin, root_commit] =
        txn_fragments(100, Lsn.from_integer(100), [
          %{changes: [update], has_begin?: true, has_commit?: false},
          %{changes: [], has_begin?: false, has_commit?: true}
        ])

      root_commit = %{root_commit | last_log_offset: LogOffset.new(100, 1)}

      assert :ok = ShapeLogCollector.handle_event(root_begin, ctx.stack_id)

      assert :root_pending =
               poll_until(@receive_timeout, fn ->
                 state = :sys.get_state(consumer_pid)

                 if state.deferred_replication_event_count > 0 or not is_nil(state.pending_txn),
                   do: {:ok, :root_pending},
                   else: :retry
               end)

      waiter = Task.async(fn -> Consumer.await_causal_frontier(consumer_pid, 100) end)
      assert Task.yield(waiter, 100) == nil

      assert :ok = ShapeLogCollector.handle_event(root_commit, ctx.stack_id)

      final_state =
        poll_until(@receive_timeout, fn ->
          state = :sys.get_state(consumer_pid)

          if state.last_seen_global_lsn >= 100,
            do: {:ok, state},
            else: :retry
        end)

      assert final_state.last_observed_global_lsn >= 100
      assert final_state.last_seen_global_lsn >= 100
      assert :ok = Task.await(waiter, @receive_timeout)

      assert_receive {^ref, :new_changes, _offset}, @receive_timeout

      assert :ordered =
               poll_until(@receive_timeout, fn ->
                 operations =
                   get_log_items_from_storage(
                     LogOffset.last_before_real_offsets(),
                     shape_storage
                   )
                   |> Enum.flat_map(fn
                     %{
                       "key" => key,
                       "headers" => %{"operation" => operation}
                     }
                     when key in [
                            ~s'"public"."test_table"/"1"',
                            ~s'"public"."test_table"/"2"'
                          ] ->
                       [{key, operation}]

                     _ ->
                       []
                   end)

                 if operations == [
                      {~s'"public"."test_table"/"1"', "insert"},
                      {~s'"public"."test_table"/"1"', "delete"},
                      {~s'"public"."test_table"/"2"', "insert"}
                    ],
                    do: {:ok, :ordered},
                    else: :retry
               end)
    end

    test "active snapshot reconciles a deferred root before the next view-changing dependency",
         ctx do
      parent = self()

      Repatch.patch(
        Electric.Shapes.Consumer.Effects,
        :query_move_in_async,
        [mode: :shared],
        fn _task_sup, _consumer_state, _buffering_state, consumer_pid ->
          send(parent, {:query_requested, consumer_pid})
          :ok
        end
      )

      Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      shape_storage = Storage.for_shape(shape_handle, ctx.storage)

      # Establish an existing row before opening the move whose query snapshot
      # includes a later root transaction.
      send(
        consumer_pid,
        {:materializer_changes, dep_handle,
         %{
           move_in: [{1, "1"}],
           move_out: [],
           txids: [40],
           lsn: LogOffset.new(40, 0)
         }}
      )

      assert_receive {:query_requested, ^consumer_pid}, @receive_timeout
      send(consumer_pid, {:pg_snapshot_known, {1, 40, []}})

      send_stored_move_in_complete(
        consumer_pid,
        shape_storage,
        [
          [
            ~s'"public"."test_table"/"1"',
            [],
            Jason.encode!(%{
              "key" => ~s'"public"."test_table"/"1"',
              "value" => %{"id" => "1", "value" => "before"},
              "headers" => %{
                "operation" => "insert",
                "relation" => ["public", "test_table"]
              }
            })
          ]
        ],
        Lsn.from_integer(40)
      )

      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 40)

      assert :seed_committed =
               poll_until(@receive_timeout, fn ->
                 state = :sys.get_state(consumer_pid)

                 if state.last_seen_global_lsn >= 40 and not state.move_transaction_open?,
                   do: {:ok, :seed_committed},
                   else: :retry
               end)

      send(
        consumer_pid,
        {:materializer_changes, dep_handle,
         %{
           move_in: [{2, "2"}],
           move_out: [],
           txids: [50],
           lsn: LogOffset.new(50, 0)
         }}
      )

      assert_receive {:query_requested, ^consumer_pid}, @receive_timeout
      send(consumer_pid, {:pg_snapshot_known, {90, 200, []}})

      send_stored_move_in_complete(
        consumer_pid,
        shape_storage,
        [
          [
            ~s'"public"."test_table"/"2"',
            [],
            Jason.encode!(%{
              "key" => ~s'"public"."test_table"/"2"',
              "value" => %{"id" => "2", "value" => "after"},
              "headers" => %{
                "operation" => "insert",
                "relation" => ["public", "test_table"]
              }
            })
          ]
        ],
        Lsn.from_integer(100)
      )

      # This resolved batch is ordered before root tx 100 and changes the
      # dependency view, so root tx 100 cannot safely overtake it into the
      # active move-in handler.
      send(
        consumer_pid,
        {:materializer_changes, dep_handle,
         %{
           move_in: [{3, "3"}],
           move_out: [],
           txids: [75],
           lsn: LogOffset.new(75, 0)
         }}
      )

      assert :dependency_deferred =
               poll_until(@receive_timeout, fn ->
                 if :sys.get_state(consumer_pid).deferred_materializer_move_count == 1,
                   do: {:ok, :dependency_deferred},
                   else: :retry
               end)

      update =
        Changes.UpdatedRecord.new(%{
          relation: {"public", "test_table"},
          old_record: %{"id" => "1", "value" => "before"},
          record: %{"id" => "2", "value" => "after"},
          old_key: ~s'"public"."test_table"/"1"',
          key: ~s'"public"."test_table"/"2"',
          log_offset: LogOffset.new(100, 0)
        })

      [root_begin, root_commit] =
        txn_fragments(100, Lsn.from_integer(100), [
          %{changes: [update], has_begin?: true, has_commit?: false},
          %{changes: [], has_begin?: false, has_commit?: true}
        ])

      root_commit = %{root_commit | last_log_offset: LogOffset.new(100, 1)}

      assert :ok = ShapeLogCollector.handle_event(root_begin, ctx.stack_id)

      assert :root_pending =
               poll_until(@receive_timeout, fn ->
                 state = :sys.get_state(consumer_pid)

                 if state.deferred_replication_event_count > 0 or not is_nil(state.pending_txn),
                   do: {:ok, :root_pending},
                   else: :retry
               end)

      consumer_ref = Process.monitor(consumer_pid)
      assert :ok = ShapeLogCollector.handle_event(root_commit, ctx.stack_id)

      # The active move must absorb root100 so its snapshot can reconcile the
      # visible transaction. Only after that atomic batch commits may dep75
      # start its own query against the converged database state.
      assert_receive {:query_requested, ^consumer_pid}, @receive_timeout
      refute_receive {:DOWN, ^consumer_ref, :process, ^consumer_pid, _reason}, 100

      send(consumer_pid, {:pg_snapshot_known, {90, 200, []}})

      send_stored_move_in_complete(
        consumer_pid,
        shape_storage,
        [
          [
            ~s'"public"."test_table"/"3"',
            [],
            Jason.encode!(%{
              "key" => ~s'"public"."test_table"/"3"',
              "value" => %{"id" => "3", "value" => "after-dependency"},
              "headers" => %{
                "operation" => "insert",
                "relation" => ["public", "test_table"]
              }
            })
          ]
        ],
        Lsn.from_integer(100)
      )

      assert :committed =
               poll_until(@receive_timeout, fn ->
                 with {:ok, positions} <- Storage.fetch_move_positions(shape_storage),
                      %LogOffset{tx_offset: 75} <- Map.get(positions, dep_handle),
                      {:ok, 100} <- Storage.fetch_root_delivery_tx_offset(shape_storage),
                      %{move_transaction_open?: false} <- :sys.get_state(consumer_pid) do
                   {:ok, :committed}
                 else
                   _ -> :retry
                 end
               end)

      assert Process.alive?(consumer_pid)
      Process.demonitor(consumer_ref, [:flush])
    end

    test "nonvisible root behind an earlier dependency progresses without a pre-broadcast frontier",
         ctx do
      parent = self()

      Repatch.patch(
        Electric.Shapes.Consumer.Effects,
        :query_move_in_async,
        [mode: :shared],
        fn _task_sup, _consumer_state, _buffering_state, consumer_pid ->
          send(parent, {:query_requested, consumer_pid})
          :ok
        end
      )

      Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      shape_storage = Storage.for_shape(shape_handle, ctx.storage)

      # Seed row 1 so the later PK update is routed to this outer shape.
      send(
        consumer_pid,
        {:materializer_changes, dep_handle,
         %{
           move_in: [{1, "1"}],
           move_out: [],
           txids: [40],
           lsn: LogOffset.new(40, 0)
         }}
      )

      assert_receive {:query_requested, ^consumer_pid}, @receive_timeout
      send(consumer_pid, {:pg_snapshot_known, {1, 40, []}})

      send_stored_move_in_complete(
        consumer_pid,
        shape_storage,
        [
          [
            ~s'"public"."test_table"/"1"',
            [],
            Jason.encode!(%{
              "key" => ~s'"public"."test_table"/"1"',
              "value" => %{"id" => "1", "value" => "before"},
              "headers" => %{
                "operation" => "insert",
                "relation" => ["public", "test_table"]
              }
            })
          ]
        ],
        Lsn.from_integer(40)
      )

      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 40)

      assert :seed_committed =
               poll_until(@receive_timeout, fn ->
                 state = :sys.get_state(consumer_pid)

                 if state.last_seen_global_lsn >= 40 and not state.move_transaction_open?,
                   do: {:ok, :seed_committed},
                   else: :retry
               end)

      # dep50 opens a move-in whose snapshot still sees root tx100 in progress.
      # Its query is complete, but causal tx50 remains unproven until the later
      # root reaches this Consumer.
      send(
        consumer_pid,
        {:materializer_changes, dep_handle,
         %{
           move_in: [{2, "2"}],
           move_out: [],
           txids: [50],
           lsn: LogOffset.new(50, 0)
         }}
      )

      assert_receive {:query_requested, ^consumer_pid}, @receive_timeout
      send(consumer_pid, {:pg_snapshot_known, {90, 200, [100]}})
      send_stored_move_in_complete(consumer_pid, shape_storage, [], Lsn.from_integer(100))

      assert :dep50_waiting =
               poll_until(@receive_timeout, fn ->
                 state = :sys.get_state(consumer_pid)

                 if state.move_transaction_open? and state.last_seen_global_lsn < 100,
                   do: {:ok, :dep50_waiting},
                   else: :retry
               end)

      # dep75 is view-changing and precedes root100, so root100 must not
      # overtake it. It remains queued behind the active dep50 move.
      send(
        consumer_pid,
        {:materializer_changes, dep_handle,
         %{
           move_in: [{3, "3"}],
           move_out: [],
           txids: [75],
           lsn: LogOffset.new(75, 0)
         }}
      )

      assert :dependency_deferred =
               poll_until(@receive_timeout, fn ->
                 if :sys.get_state(consumer_pid).deferred_materializer_move_count == 1,
                   do: {:ok, :dependency_deferred},
                   else: :retry
               end)

      update =
        Changes.UpdatedRecord.new(%{
          relation: {"public", "test_table"},
          old_record: %{"id" => "1", "value" => "before"},
          record: %{"id" => "2", "value" => "after"},
          old_key: ~s'"public"."test_table"/"1"',
          key: ~s'"public"."test_table"/"2"',
          log_offset: LogOffset.new(100, 0)
        })

      [root_begin, root_commit] =
        txn_fragments(100, Lsn.from_integer(100), [
          %{changes: [update], has_begin?: true, has_commit?: false},
          %{changes: [], has_begin?: false, has_commit?: true}
        ])

      root_commit = %{root_commit | last_log_offset: LogOffset.new(100, 1)}

      # There is intentionally no LsnTracker.broadcast_last_seen_lsn(..., 100)
      # before these fragments. The queued root itself must prove dep50 and
      # dep75 without exposing tx100 as applied before it is evaluated.
      assert :sys.get_state(consumer_pid).last_seen_global_lsn < 100
      assert :ok = ShapeLogCollector.handle_event(root_begin, ctx.stack_id)
      assert :ok = ShapeLogCollector.handle_event(root_commit, ctx.stack_id)

      # The scheduler must finish dep50, then start dep75 before evaluating the
      # nonvisible root. A deadlock leaves this query request absent.
      assert_receive {:query_requested, ^consumer_pid}, @receive_timeout

      send(consumer_pid, {:pg_snapshot_known, {90, 200, []}})

      send_stored_move_in_complete(
        consumer_pid,
        shape_storage,
        [
          [
            ~s'"public"."test_table"/"3"',
            [],
            Jason.encode!(%{
              "key" => ~s'"public"."test_table"/"3"',
              "value" => %{"id" => "3", "value" => "dependency-row"},
              "headers" => %{
                "operation" => "insert",
                "relation" => ["public", "test_table"]
              }
            })
          ]
        ],
        Lsn.from_integer(100)
      )

      assert :drained =
               poll_until(@receive_timeout, fn ->
                 with {:ok, positions} <- Storage.fetch_move_positions(shape_storage),
                      %LogOffset{tx_offset: 75} <- Map.get(positions, dep_handle),
                      {:ok, 100} <- Storage.fetch_root_delivery_tx_offset(shape_storage),
                      %{
                        move_transaction_open?: false,
                        deferred_materializer_move_count: 0,
                        deferred_replication_event_count: 0,
                        pending_txn: nil
                      } <- :sys.get_state(consumer_pid) do
                   {:ok, :drained}
                 else
                   _ -> :retry
                 end
               end)
    end

    test "consumer replays the latest broadcast when subscribing for a move-in", ctx do
      parent = self()

      Repatch.patch(
        Electric.Shapes.Consumer.Effects,
        :query_move_in_async,
        [mode: :shared],
        fn _task_sup, _consumer_state, _buffering_state, consumer_pid ->
          send(parent, {:query_requested, consumer_pid})
          :ok
        end
      )

      Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles

      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)

      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 100)

      send(
        consumer_pid,
        {:materializer_changes, dep_handle,
         %{
           move_in: [{1, "1"}],
           move_out: []
         }}
      )

      assert_receive {:query_requested, ^consumer_pid}, @receive_timeout

      send(consumer_pid, {:pg_snapshot_known, {100, 300, []}})

      shape_storage = Storage.for_shape(shape_handle, ctx.storage)

      send_stored_move_in_complete(
        consumer_pid,
        shape_storage,
        [
          [
            ~s'"public"."test_table"/"1"',
            [],
            Jason.encode!(%{
              "key" => ~s'"public"."test_table"/"1"',
              "value" => %{"id" => "1", "value" => "old"},
              "headers" => %{
                "operation" => "insert",
                "relation" => ["public", "test_table"]
              }
            })
          ]
        ],
        Lsn.from_integer(100)
      )

      assert_receive {^ref, :new_changes, _offset}, @receive_timeout

      assert [
               %{"headers" => %{"event" => "move-in"}},
               %{
                 "headers" => %{"operation" => "insert"},
                 "key" => ~s'"public"."test_table"/"1"',
                 "value" => %{"id" => "1", "value" => "old"}
               },
               %{
                 "headers" => %{
                   "control" => "snapshot-end",
                   "xmin" => "100",
                   "xmax" => "300",
                   "xip_list" => []
                 }
               }
             ] = get_log_items_from_storage(LogOffset.last_before_real_offsets(), shape_storage)
    end

    @tag with_pure_file_storage_opts: [flush_period: 10_000]
    test "materializer subscription flushes and replays a pre-existing volatile tail exactly once",
         ctx do
      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)
      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      register_as_replication_client(ctx.stack_id)

      row_offset = LogOffset.new(Lsn.from_integer(700), 0)

      txn =
        complete_txn_fragment(700, Lsn.from_integer(700), [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "1", "value" => "pre-subscription"},
            log_offset: row_offset
          }
        ])

      assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)

      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      pre_subscription = :sys.get_state(consumer_pid)
      assert pre_subscription.latest_offset == row_offset
      assert LogOffset.compare(pre_subscription.durable_offset, row_offset) == :lt

      {:ok, materializer_pid} =
        Materializer.start_link(%{
          stack_id: ctx.stack_id,
          shape_handle: shape_handle,
          columns: ["id"],
          materialized_type: {:array, :int8}
        })

      assert :ok =
               Materializer.wait_until_ready(%{
                 stack_id: ctx.stack_id,
                 shape_handle: shape_handle
               })

      assert {:ok, seed_values, ^row_offset} = Materializer.subscribe(materializer_pid)
      assert seed_values == MapSet.new([1])

      post_subscription = :sys.get_state(consumer_pid)
      assert post_subscription.durable_offset == row_offset
      assert post_subscription.materializer_subscribed?
      refute_received {:materializer_changes, ^shape_handle, _payload}
    end

    @tag allow_subqueries: false, with_pure_file_storage_opts: [flush_period: 10_000]
    test "materializer subscription waits for a fragmented transaction and replays it once",
         ctx do
      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)
      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      register_as_replication_client(ctx.stack_id)

      lsn = Lsn.from_integer(701)
      row_offset = LogOffset.new(lsn, 0)

      begin_fragment =
        txn_fragment(
          701,
          lsn,
          [
            %Changes.NewRecord{
              relation: {"public", "test_table"},
              record: %{"id" => "1", "value" => "fragmented"},
              log_offset: row_offset
            }
          ],
          has_begin?: true
        )

      commit_fragment =
        txn_fragment(
          701,
          lsn,
          [
            %Changes.NewRecord{
              relation: {"public", "other_table"},
              record: %{"id" => "unrelated"},
              log_offset: LogOffset.new(lsn, 2)
            }
          ],
          has_commit?: true
        )

      assert :ok = ShapeLogCollector.handle_event(begin_fragment, ctx.stack_id)

      {:ok, materializer_pid} =
        Materializer.start_link(%{
          stack_id: ctx.stack_id,
          shape_handle: shape_handle,
          columns: ["id"],
          materialized_type: {:array, :int8}
        })

      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)

      assert :subscription_waiting =
               poll_until(@receive_timeout, fn ->
                 case :sys.get_state(consumer_pid).pending_materializer_subscription do
                   nil -> :retry
                   _from -> {:ok, :subscription_waiting}
                 end
               end)

      # The public subscription call uses an infinite timeout because a real
      # fragmented transaction or nested move query can legitimately exceed
      # GenServer.call's five-second default.
      Process.sleep(5_100)
      assert Process.alive?(materializer_pid)

      # The Consumer returned from the pending subscribe call without replying,
      # so it remains able to accept and commit the final replication fragment.
      assert :ok = ShapeLogCollector.handle_event(commit_fragment, ctx.stack_id)

      materializer = %{stack_id: ctx.stack_id, shape_handle: shape_handle}
      assert :ok = Materializer.wait_until_ready(materializer)
      assert {:ok, seed_values, ^row_offset} = Materializer.subscribe(materializer_pid)
      assert seed_values == MapSet.new([1])
      refute_received {:materializer_changes, ^shape_handle, _payload}
    end

    test "cursor-only dependency move commits do not manufacture a log boundary",
         ctx do
      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      shape_storage = Storage.for_shape(shape_handle, ctx.storage)

      # The test snapshotter signals `snapshot_started` before its asynchronous
      # snapshot writer publishes `last_snapshot_chunk`. Capture the baseline
      # only after that storage boundary is visible so snapshot completion
      # cannot be mistaken for a cursor-only move boundary.
      pre_snapshot_boundary = LogOffset.last_before_real_offsets()

      start_boundary =
        poll_until(@receive_timeout, fn ->
          case Storage.fetch_latest_offset(shape_storage) do
            {:ok, ^pre_snapshot_boundary} ->
              :retry

            {:ok, boundary} ->
              {:ok, boundary}

            _ ->
              :retry
          end
        end)

      move_lsn = LogOffset.new(776, 0)
      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, move_lsn.tx_offset)

      send(
        consumer_pid,
        {:materializer_changes, dep_handle,
         %{move_in: [], move_out: [], txids: [], lsn: move_lsn}}
      )

      assert :frontier_observed =
               poll_until(@receive_timeout, fn ->
                 if :sys.get_state(consumer_pid).last_seen_global_lsn >= move_lsn.tx_offset,
                   do: {:ok, :frontier_observed},
                   else: :retry
               end)

      assert :committed =
               poll_until(@receive_timeout, fn ->
                 with {:ok, positions} <- Storage.fetch_move_positions(shape_storage),
                      ^move_lsn <- Map.get(positions, dep_handle) do
                   {:ok, :committed}
                 else
                   _ -> :retry
                 end
               end)

      {:ok, boundary} = Storage.fetch_latest_offset(shape_storage)
      assert boundary == start_boundary

      assert [] =
               Storage.get_log_stream_with_offsets(start_boundary, boundary, shape_storage)
               |> Enum.to_list()
    end

    test "later root proves a cursor-only move without overtaking an earlier dependency",
         ctx do
      parent = self()

      Repatch.patch(
        Electric.Shapes.Consumer.Effects,
        :query_move_in_async,
        [mode: :shared],
        fn _task_sup, _consumer_state, _buffering_state, consumer_pid ->
          send(parent, {:query_requested, consumer_pid})
          :ok
        end
      )

      Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery_or_value, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      changes_ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)
      shape_storage = Storage.for_shape(shape_handle, ctx.storage)

      tx50 = LogOffset.new(50, 0)

      assert :ok =
               Consumer.deliver_materializer_batch(
                 consumer_pid,
                 dep_handle,
                 %{move_in: [], move_out: [], txids: [50], lsn: tx50},
                 @receive_timeout
               )

      assert :cursor_waiting_for_root =
               poll_until(@receive_timeout, fn ->
                 state = :sys.get_state(consumer_pid)

                 if state.move_transaction_open? and state.pending_move_causal_origin == tx50 and
                      match?(
                        %Electric.Shapes.Consumer.EventHandler.Subqueries.Steady{},
                        state.event_handler
                      ) do
                   {:ok, :cursor_waiting_for_root}
                 else
                   :retry
                 end
               end)

      tx75 = LogOffset.new(75, 0)

      assert :ok =
               Consumer.deliver_materializer_batch(
                 consumer_pid,
                 dep_handle,
                 %{move_in: [{75, "75"}], move_out: [], txids: [75], lsn: tx75},
                 @receive_timeout
               )

      assert :sys.get_state(consumer_pid).deferred_materializer_move_count == 1

      root100 =
        complete_txn_fragment(100, Lsn.from_integer(100), [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "100", "value" => "causal-root"},
            key: ~s'"public"."test_table"/"100"',
            log_offset: LogOffset.new(100, 0)
          }
        ])

      assert :ok =
               GenServer.call(
                 consumer_pid,
                 {:handle_event, root100, Electric.Telemetry.OpenTelemetry.get_current_context()},
                 :infinity
               )

      # Root 100 proves that cursor-only tx50 is a durable prefix. Dependency
      # tx75 must become active before root 100 leaves the Consumer queue. Once
      # active, tx75's Buffering handler may safely hold the root until its
      # snapshot is known.
      assert_receive {:query_requested, ^consumer_pid}, @receive_timeout

      state = :sys.get_state(consumer_pid)
      assert state.pending_move_causal_origin == tx75

      assert %Electric.Shapes.Consumer.EventHandler.Subqueries.Buffering{
               active_move: %{
                 values: [{75, "75"}],
                 buffered_txn_count: 1,
                 buffered_txns: [%Changes.Transaction{xid: 100}]
               }
             } = state.event_handler

      assert {:ok, 50} = Storage.fetch_root_delivery_tx_offset(shape_storage)
      assert {:ok, positions} = Storage.fetch_move_positions(shape_storage)
      assert Map.fetch!(positions, dep_handle) == tx50
      refute_receive {^changes_ref, :new_changes, _offset}, 100
    end

    test "malformed deferred root xid fails closed instead of parking behind a dependency",
         ctx do
      parent = self()

      Repatch.patch(
        Electric.Shapes.Consumer.Effects,
        :query_move_in_async,
        [mode: :shared],
        fn _task_sup, _consumer_state, _buffering_state, consumer_pid ->
          send(parent, {:query_requested, consumer_pid})
          :ok
        end
      )

      Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)

      send(
        consumer_pid,
        {:materializer_changes, dep_handle,
         %{
           move_in: [{1, "1"}],
           move_out: [],
           txids: [50],
           lsn: LogOffset.new(50, 0)
         }}
      )

      assert_receive {:query_requested, ^consumer_pid}, @receive_timeout
      send(consumer_pid, {:pg_snapshot_known, {90, 200, []}})

      send(
        consumer_pid,
        {:materializer_changes, dep_handle,
         %{
           move_in: [{2, "2"}],
           move_out: [],
           txids: [75],
           lsn: LogOffset.new(75, 0)
         }}
      )

      assert :scheduler_ready =
               poll_until(@receive_timeout, fn ->
                 state = :sys.get_state(consumer_pid)

                 case state do
                   %{
                     deferred_materializer_move_count: 1,
                     event_handler: %Electric.Shapes.Consumer.EventHandler.Subqueries.Buffering{
                       active_move: %{snapshot: {90, 200, []}, boundary_txn_count: nil}
                     }
                   } ->
                     {:ok, :scheduler_ready}

                   _ ->
                     :retry
                 end
               end)

      malformed_root = complete_txn_fragment(nil, Lsn.from_integer(100), [])
      consumer_ref = Process.monitor(consumer_pid)

      assert :ok =
               GenServer.call(
                 consumer_pid,
                 {:handle_event, malformed_root,
                  Electric.Telemetry.OpenTelemetry.get_current_context()},
                 :infinity
               )

      assert_receive {:DOWN, ^consumer_ref, :process, ^consumer_pid, {:shutdown, :cleanup}},
                     @receive_timeout
    end

    test "dependency replay seeds are released after new and restored handler initialization",
         ctx do
      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      assert_seed_released = fn consumer_pid ->
        state = :sys.get_state(consumer_pid)

        assert state.dep_seed_views == %{}
        assert %{views: views} = state.event_handler
        assert map_size(views) == 1
      end

      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      assert_seed_released.(consumer_pid)

      old_ref = Process.monitor(consumer_pid)
      assert :ok = Consumer.stop(consumer_pid, :shutdown)
      assert_receive {:DOWN, ^old_ref, :process, ^consumer_pid, :shutdown}, @receive_timeout
      :ok = Electric.Shapes.ConsumerRegistry.remove_consumer(shape_handle, ctx.stack_id)

      assert Support.TestUtils.wait_until(
               fn -> is_nil(Consumer.whereis(ctx.stack_id, shape_handle)) end,
               @receive_timeout
             )

      assert {:ok, restored_pid} =
               ShapeCache.start_consumer_for_handle(shape_handle, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      assert_seed_released.(restored_pid)
    end

    test "restore with two stale dependencies fails closed to a fresh shape", ctx do
      assert_two_dependency_restore_replay(
        @shape_with_two_subqueries,
        [{"public", "other_table"}, {"public", "something else"}],
        ctx
      )
    end

    test "restore with two stale dependencies fails closed in the opposite dependency order",
         ctx do
      assert_two_dependency_restore_replay(
        @shape_with_two_subqueries_reversed,
        [{"public", "something else"}, {"public", "other_table"}],
        ctx
      )
    end

    test "replay A100 and A150 both precede live B200", ctx do
      assert_replay_sequence_precedes_live(
        @shape_with_two_subqueries,
        [LogOffset.new(100, 0), LogOffset.new(150, 0)],
        LogOffset.new(200, 0),
        ctx
      )
    end

    test "replay A100 and A150 precede live B200 in the opposite dependency order", ctx do
      assert_replay_sequence_precedes_live(
        @shape_with_two_subqueries_reversed,
        [LogOffset.new(100, 0), LogOffset.new(150, 0)],
        LogOffset.new(200, 0),
        ctx
      )
    end

    test "live B200 precedes replay A300 after replay lookahead discovers its offset", ctx do
      assert_replay_sequence_follows_live(
        @shape_with_two_subqueries,
        LogOffset.new(300, 0),
        LogOffset.new(200, 0),
        ctx
      )
    end

    test "pending A100 lookahead blocks B200 until the coordinator wakes it", ctx do
      assert_pending_replay_order(
        @shape_with_two_subqueries,
        LogOffset.new(100, 0),
        LogOffset.new(200, 0),
        :replay_first,
        ctx
      )
    end

    test "pending A300 lookahead blocks discovery then runs after B200", ctx do
      assert_pending_replay_order(
        @shape_with_two_subqueries_reversed,
        LogOffset.new(300, 0),
        LogOffset.new(200, 0),
        :live_first,
        ctx
      )
    end

    test "unknown-offset replication fails closed ahead of an unresolved dependency reservation",
         ctx do
      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      offset = LogOffset.new(75, 0)
      token = Materializer.new_causal_token(offset)

      assert :ok =
               Consumer.reserve_materializer_batch(
                 consumer_pid,
                 dep_handle,
                 token,
                 offset,
                 100
               )

      assert %{deferred_materializer_move_count: 1} = :sys.get_state(consumer_pid)

      Electric.StackConfig.put(ctx.stack_id, :inspector, @base_inspector)

      relation = %Relation{
        id: shape.root_table_id,
        schema: elem(shape.root_table, 0),
        table: elem(shape.root_table, 1),
        columns: []
      }

      consumer_ref = Process.monitor(consumer_pid)

      assert :ok =
               GenServer.call(
                 consumer_pid,
                 {:handle_event, relation,
                  Electric.Telemetry.OpenTelemetry.get_current_context()},
                 :infinity
               )

      assert_receive {:DOWN, ^consumer_ref, :process, ^consumer_pid, {:shutdown, :cleanup}},
                     @receive_timeout
    end

    test "reserved dependency batches wait for the earliest offset when payloads arrive reversed",
         ctx do
      parent = self()

      Repatch.patch(
        Electric.Shapes.Consumer.Effects,
        :query_move_in_async,
        [mode: :shared],
        fn _task_sup, _consumer_state, _buffering_state, consumer_pid ->
          send(parent, {:query_requested, consumer_pid})
          :ok
        end
      )

      Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_two_subqueries, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [first_dep, second_dep] = Enum.sort(shape.shape_dependencies_handles)
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      first_offset = LogOffset.new(800, 0)
      second_offset = LogOffset.new(900, 0)
      first_token = Materializer.new_causal_token(first_offset)
      second_token = Materializer.new_causal_token(second_offset)

      assert :ok =
               Consumer.reserve_materializer_batch(
                 consumer_pid,
                 first_dep,
                 first_token,
                 first_offset,
                 100_000
               )

      assert :ok =
               Consumer.reserve_materializer_batch(
                 consumer_pid,
                 second_dep,
                 second_token,
                 second_offset,
                 100_000
               )

      # Both source transactions have crossed the collector's post-layer
      # frontier; this test is about reservation ordering, not frontier gating.
      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 900)

      # The later payload is durable first. It fills its reserved slot but must
      # not start a move-in while the earlier dependency batch is outstanding.
      send(
        consumer_pid,
        {:materializer_changes, second_dep,
         %{
           move_in: [{2, "2"}],
           move_out: [],
           txids: [900],
           lsn: second_offset,
           causal_token: second_token
         }}
      )

      state = :sys.get_state(consumer_pid)
      assert state.deferred_materializer_move_count == 2
      assert state.pending_move_lsns == %{}
      refute_received {:query_requested, ^consumer_pid}

      send(
        consumer_pid,
        {:materializer_changes, first_dep,
         %{
           move_in: [{1, "1"}],
           move_out: [],
           txids: [800],
           lsn: first_offset,
           causal_token: first_token
         }}
      )

      assert_receive {:query_requested, ^consumer_pid}, @receive_timeout

      state = :sys.get_state(consumer_pid)
      assert state.pending_move_lsns == %{first_dep => first_offset}
      assert state.deferred_materializer_move_count == 1

      assert {:value,
              {{:reserved_materializer_batch, ^second_dep, ^second_offset, ^second_token,
                _downstream_token, {:materializer_changes, ^second_dep, _payload}}, _bytes}} =
               :queue.peek(state.deferred_materializer_moves)
    end

    test "same-transaction local dependency batches precede transitively forwarded batches",
         ctx do
      Repatch.patch(Materializer, :forward_causal_begin, [mode: :shared], fn _, _, _ -> :ok end)
      Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)

      :sys.replace_state(consumer_pid, fn state ->
        %{state | materializer_subscribed?: true}
      end)

      earlier_forwarded_offset = LogOffset.new(801, 8)
      forwarded_offset = LogOffset.new(802, 2)
      local_offset = LogOffset.new(802, 4)
      deeply_forwarded_offset = LogOffset.new(802, 0)
      later_local_offset = LogOffset.new(803, 0)
      earlier_forwarded_token = Materializer.new_causal_token(earlier_forwarded_offset, 2)
      forwarded_token = Materializer.new_causal_token(forwarded_offset, 1)
      local_token = Materializer.new_causal_token(local_offset)
      deeply_forwarded_token = Materializer.new_causal_token(deeply_forwarded_offset, 2)
      later_local_token = Materializer.new_causal_token(later_local_offset)

      # Recursive reservation reaches the outer Consumer before this layer has
      # necessarily processed and materialized its own root transaction.
      assert :ok =
               Consumer.reserve_materializer_batch(
                 consumer_pid,
                 dep_handle,
                 forwarded_token,
                 forwarded_offset,
                 100_000
               )

      assert :ok =
               Consumer.reserve_materializer_batch(
                 consumer_pid,
                 dep_handle,
                 local_token,
                 local_offset,
                 100_000
               )

      assert :ok =
               Consumer.reserve_materializer_batch(
                 consumer_pid,
                 dep_handle,
                 later_local_token,
                 later_local_offset,
                 100_000
               )

      assert :ok =
               Consumer.reserve_materializer_batch(
                 consumer_pid,
                 dep_handle,
                 deeply_forwarded_token,
                 deeply_forwarded_offset,
                 100_000
               )

      assert :ok =
               Consumer.reserve_materializer_batch(
                 consumer_pid,
                 dep_handle,
                 earlier_forwarded_token,
                 earlier_forwarded_offset,
                 100_000
               )

      assert [
               {{:reserved_materializer_batch, ^dep_handle, ^earlier_forwarded_offset,
                 ^earlier_forwarded_token, earlier_downstream, nil}, _earlier_bytes},
               {{:reserved_materializer_batch, ^dep_handle, ^local_offset, ^local_token,
                 local_downstream, nil}, _local_bytes},
               {{:reserved_materializer_batch, ^dep_handle, ^forwarded_offset, ^forwarded_token,
                 forwarded_downstream, nil}, _forwarded_bytes},
               {{:reserved_materializer_batch, ^dep_handle, ^deeply_forwarded_offset,
                 ^deeply_forwarded_token, deeply_forwarded_downstream, nil}, _deep_bytes},
               {{:reserved_materializer_batch, ^dep_handle, ^later_local_offset,
                 ^later_local_token, _later_downstream, nil}, _later_bytes}
             ] =
               consumer_pid
               |> :sys.get_state()
               |> Map.fetch!(:deferred_materializer_moves)
               |> :queue.to_list()

      assert Materializer.causal_token_depth(earlier_downstream) == 3
      assert Materializer.causal_token_depth(local_downstream) == 1
      assert Materializer.causal_token_depth(forwarded_downstream) == 2
      assert Materializer.causal_token_depth(deeply_forwarded_downstream) == 3
    end

    test "mixed deferred dependency entries use handle before entry type at equal offsets", ctx do
      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_two_subqueries, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [lower_handle, higher_handle] = Enum.sort(shape.shape_dependencies_handles)
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      offset = LogOffset.new(804, 0)
      token = Materializer.new_causal_token(offset)

      assert :ok =
               Consumer.reserve_materializer_batch(
                 consumer_pid,
                 lower_handle,
                 token,
                 offset,
                 100_000
               )

      send(
        consumer_pid,
        {:materializer_changes, higher_handle,
         %{move_in: [], move_out: [], txids: [804], lsn: offset}}
      )

      assert [
               {{:reserved_materializer_batch, ^lower_handle, ^offset, ^token, _downstream_token,
                 nil}, _reserved_bytes},
               {{:materializer_changes, ^higher_handle, %{lsn: ^offset}}, _live_bytes}
             ] =
               consumer_pid
               |> :sys.get_state()
               |> Map.fetch!(:deferred_materializer_moves)
               |> :queue.to_list()
    end

    test "oversized deferred dependency moves invalidate instead of accumulating in memory",
         ctx do
      Electric.StackConfig.put(ctx.stack_id, :subquery_deferred_event_memory_limit_bytes, 1)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)

      incomplete =
        txn_fragment(
          700,
          Lsn.from_integer(700),
          [
            %Changes.NewRecord{
              relation: {"public", "test_table"},
              record: %{"id" => "700"},
              log_offset: LogOffset.new(700, 0)
            }
          ],
          has_begin?: true
        )

      assert :ok =
               GenServer.call(
                 consumer_pid,
                 {:handle_event, incomplete,
                  Electric.Telemetry.OpenTelemetry.get_current_context()}
               )

      assert %Consumer.PendingTxn{} = :sys.get_state(consumer_pid).pending_txn

      ref = Process.monitor(consumer_pid)

      send(
        consumer_pid,
        {:materializer_changes, dep_handle,
         %{
           move_in: [{String.duplicate("x", 1_024), MapSet.new()}],
           move_out: [],
           txids: [],
           lsn: LogOffset.new(701, 0)
         }}
      )

      assert_receive {:DOWN, ^ref, :process, ^consumer_pid, {:shutdown, :cleanup}},
                     @receive_timeout
    end

    test "rejects a causal reservation before propagating it when the local buffer is full",
         ctx do
      parent = self()

      Repatch.patch(Materializer, :forward_causal_begin, [mode: :shared], fn _materializer,
                                                                             _token ->
        send(parent, :causal_reservation_propagated)
        :ok
      end)

      Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)

      :sys.replace_state(consumer_pid, fn state ->
        %{state | materializer_subscribed?: true}
      end)

      Electric.StackConfig.put(ctx.stack_id, :subquery_deferred_event_memory_limit_bytes, 1)

      offset = LogOffset.new(703, 0)
      token = Materializer.new_causal_token(offset)
      ref = Process.monitor(consumer_pid)

      assert {:error, :memory_limit} =
               Consumer.reserve_materializer_batch(
                 consumer_pid,
                 dep_handle,
                 token,
                 offset,
                 100
               )

      refute_received :causal_reservation_propagated

      assert_receive {:DOWN, ^ref, :process, ^consumer_pid, _reason},
                     @receive_timeout
    end

    test "an unknown causal prepare invalidates the detached outer shape", ctx do
      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      offset = LogOffset.new(704, 0)
      token = Materializer.new_causal_token(offset)
      ref = Process.monitor(consumer_pid)

      assert {:error, :unknown_reservation} =
               Consumer.prepare_materializer_batch(
                 consumer_pid,
                 dep_handle,
                 token,
                 100,
                 @receive_timeout
               )

      assert_receive {:DOWN, ^ref, :process, ^consumer_pid, {:shutdown, :cleanup}},
                     @receive_timeout
    end

    test "resolved causal work waits for the collector transaction frontier", ctx do
      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      offset = LogOffset.new(705, 0)
      token = Materializer.new_causal_token(offset)

      assert :ok =
               Consumer.reserve_materializer_batch(
                 consumer_pid,
                 dep_handle,
                 token,
                 offset,
                 100
               )

      subscriber = self()

      subscription =
        Task.async(fn ->
          Consumer.subscribe_materializer(ctx.stack_id, shape_handle, subscriber)
        end)

      assert Task.yield(subscription, 100) == nil
      assert :sys.get_state(consumer_pid).pending_materializer_subscription != nil

      assert :ok =
               Consumer.deliver_materializer_causal_end(
                 consumer_pid,
                 dep_handle,
                 token,
                 @receive_timeout
               )

      # Resolving the dependency payload is not itself permission to apply it.
      # The dependency layer runs before this Consumer's root layer, so causal
      # work at tx 705 must stay parked until the collector proves every layer
      # for that transaction has been published.
      assert Task.yield(subscription, 100) == nil
      assert :sys.get_state(consumer_pid).deferred_materializer_move_count == 1

      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 704)
      assert Task.yield(subscription, 100) == nil
      assert :sys.get_state(consumer_pid).deferred_materializer_move_count == 1

      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 705)
      assert {:ok, %LogOffset{}} = Task.await(subscription, @receive_timeout)
      assert :sys.get_state(consumer_pid).deferred_materializer_move_count == 0
    end

    test "causal frontier waiters drain only work at or before their cutoff", ctx do
      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      cutoff_offset = LogOffset.new(705, 0)
      later_offset = LogOffset.new(706, 0)
      cutoff_token = Materializer.new_causal_token(cutoff_offset)
      later_token = Materializer.new_causal_token(later_offset)
      assert {:ok, epoch_token} = ConsumerRegistry.activate_causal_drain(ctx.stack_id, 705)
      initial_generation = ConsumerRegistry.causal_generation(ctx.stack_id)

      assert :ok =
               Consumer.reserve_materializer_batch(
                 consumer_pid,
                 dep_handle,
                 cutoff_token,
                 cutoff_offset,
                 100
               )

      assert ConsumerRegistry.causal_generation(ctx.stack_id) == initial_generation + 1

      assert :ok =
               Consumer.reserve_materializer_batch(
                 consumer_pid,
                 dep_handle,
                 later_token,
                 later_offset,
                 100
               )

      assert ConsumerRegistry.causal_generation(ctx.stack_id) == initial_generation + 1
      assert :ok = ConsumerRegistry.deactivate_causal_drain(ctx.stack_id, epoch_token)

      waiter = Task.async(fn -> Consumer.await_causal_frontier(consumer_pid, 705) end)

      assert Task.yield(waiter, 100) == nil
      assert :ok = Consumer.await_causal_frontier(consumer_pid, 704)

      assert :ok =
               Consumer.deliver_materializer_causal_end(
                 consumer_pid,
                 dep_handle,
                 cutoff_token,
                 @receive_timeout
               )

      assert Task.yield(waiter, 100) == nil
      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 705)
      assert :ok = Task.await(waiter, @receive_timeout)

      # The unresolved later reservation remains queued and must not starve a
      # waiter whose startup cut ended at the previous transaction.
      assert :sys.get_state(consumer_pid).deferred_materializer_move_count == 1

      # The coordinator is deliberately independent of the ReplicationClient
      # process. Prove its owner monitor tears down the blocking worker and the
      # Consumer's caller monitor removes the otherwise-bare GenServer waiter.
      manager = spawn(fn -> receive do: (:stop -> :ok) end)
      parent = self()

      owner =
        spawn(fn ->
          state =
            ReplicationClient.State.new(
              stack_id: ctx.stack_id,
              connection_manager: manager,
              handle_event: nil,
              publication_name: "",
              try_creating_publication?: false,
              slot_name: ""
            )

          state = %{
            state
            | startup_wal_flush_lsn: 706,
              received_wal: 706,
              last_processed_causal_marker_lsn: 706
          }

          {:noreply, [_status_update], draining_state} =
            ReplicationClient.handle_info({:flush_boundary_updated, 706}, state)

          send(parent, {:causal_catch_up_owner, self(), draining_state.causal_catch_up_task})

          receive do
            :stop -> :ok
          end
        end)

      assert_receive {:causal_catch_up_owner, ^owner, {task_pid, _task_ref, 706}},
                     @receive_timeout

      assert :waiter_registered =
               poll_until(@receive_timeout, fn ->
                 if :sys.get_state(consumer_pid).causal_drain_waiters == [],
                   do: :retry,
                   else: {:ok, :waiter_registered}
               end)

      task_monitor = Process.monitor(task_pid)
      owner_monitor = Process.monitor(owner)
      Process.exit(owner, :kill)
      assert_receive {:DOWN, ^owner_monitor, :process, ^owner, :killed}, @receive_timeout
      assert_receive {:DOWN, ^task_monitor, :process, ^task_pid, _reason}, @receive_timeout

      assert :waiter_removed =
               poll_until(@receive_timeout, fn ->
                 if :sys.get_state(consumer_pid).causal_drain_waiters == [],
                   do: {:ok, :waiter_removed},
                   else: :retry
               end)

      send(manager, :stop)
    end

    test "active move newer than cutoff compares its causal origin instead of local cursor",
         ctx do
      parent = self()

      Repatch.patch(
        Electric.Shapes.Consumer.Effects,
        :query_move_in_async,
        [mode: :shared],
        fn _task_sup, _consumer_state, _buffering_state, consumer_pid ->
          send(parent, {:query_requested, consumer_pid})
          :ok
        end
      )

      Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      local_cursor = LogOffset.new(100, 2)
      causal_origin = LogOffset.new(900, 4)

      send(
        consumer_pid,
        {:materializer_changes, dep_handle,
         %{
           move_in: [{1, "1"}],
           move_out: [],
           txids: [900],
           lsn: local_cursor,
           causal_origin: causal_origin,
           causal_depth: 2
         }}
      )

      assert_receive {:query_requested, ^consumer_pid}, @receive_timeout

      assert :move_open =
               poll_until(@receive_timeout, fn ->
                 state = :sys.get_state(consumer_pid)

                 if state.move_transaction_open? and
                      Map.get(state.pending_move_lsns, dep_handle) == local_cursor and
                      state.pending_move_causal_origin == causal_origin do
                   {:ok, :move_open}
                 else
                   :retry
                 end
               end)

      waiter = Task.async(fn -> Consumer.await_causal_frontier(consumer_pid, 100) end)
      assert {:ok, :ok} = Task.yield(waiter, 100)
    end

    test "active move at cutoff blocks by causal origin despite a newer local cursor", ctx do
      parent = self()

      Repatch.patch(
        Electric.Shapes.Consumer.Effects,
        :query_move_in_async,
        [mode: :shared],
        fn _task_sup, _consumer_state, _buffering_state, consumer_pid ->
          send(parent, {:query_requested, consumer_pid})
          :ok
        end
      )

      Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      shape_storage = Storage.for_shape(shape_handle, ctx.storage)
      local_cursor = LogOffset.new(900, 4)
      causal_origin = LogOffset.new(100, 2)

      send(
        consumer_pid,
        {:materializer_changes, dep_handle,
         %{
           move_in: [{1, "1"}],
           move_out: [],
           txids: [100],
           lsn: local_cursor,
           causal_origin: causal_origin,
           causal_depth: 2
         }}
      )

      assert_receive {:query_requested, ^consumer_pid}, @receive_timeout
      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 100)

      assert :move_open_at_frontier =
               poll_until(@receive_timeout, fn ->
                 state = :sys.get_state(consumer_pid)

                 if state.move_transaction_open? and state.last_seen_global_lsn >= 100 and
                      Map.get(state.pending_move_lsns, dep_handle) == local_cursor and
                      state.pending_move_causal_origin == causal_origin do
                   {:ok, :move_open_at_frontier}
                 else
                   :retry
                 end
               end)

      waiter = Task.async(fn -> Consumer.await_causal_frontier(consumer_pid, 100) end)
      assert Task.yield(waiter, 100) == nil

      send(consumer_pid, {:pg_snapshot_known, {100, 101, []}})
      send_stored_move_in_complete(consumer_pid, shape_storage, [], Lsn.from_integer(100))

      assert :ok = Task.await(waiter, @receive_timeout)
    end

    test "same-transaction root replication runs before resolved causal work", ctx do
      parent = self()

      Repatch.patch(Materializer, :forward_causal_begin, [mode: :shared], fn _, _, _ -> :ok end)

      Repatch.patch(Materializer, :forward_causal_end, [mode: :shared], fn _, _, _ ->
        send(parent, {:causal_end_forwarding, self()})

        receive do
          :release_causal_end -> :ok
        after
          @receive_timeout * 5 -> raise "timed out waiting to release causal end"
        end
      end)

      Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery_or_value, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      changes_ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)

      :sys.replace_state(consumer_pid, fn state ->
        %{state | materializer_subscribed?: true}
      end)

      offset = LogOffset.new(706, 2)
      token = Materializer.new_causal_token(offset)

      assert :ok =
               Consumer.reserve_materializer_batch(
                 consumer_pid,
                 dep_handle,
                 token,
                 offset,
                 100
               )

      assert :ok =
               Consumer.deliver_materializer_causal_end(
                 consumer_pid,
                 dep_handle,
                 token,
                 @receive_timeout
               )

      assert :sys.get_state(consumer_pid).deferred_materializer_move_count == 1

      root_txn =
        complete_txn_fragment(706, Lsn.from_integer(706), [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "706", "value" => "causal-root"},
            key: ~s'"public"."test_table"/"706"',
            log_offset: LogOffset.new(706, 0)
          }
        ])

      assert :ok =
               GenServer.call(
                 consumer_pid,
                 {:handle_event, root_txn,
                  Electric.Telemetry.OpenTelemetry.get_current_context()},
                 :infinity
               )

      # Both messages come from the Consumer. Their order proves that the root
      # transaction committed before the same-transaction causal continuation.
      assert_receive {^changes_ref, :new_changes, _offset}, @receive_timeout
      assert_receive {:causal_end_forwarding, ^consumer_pid}, @receive_timeout
      send(consumer_pid, :release_causal_end)

      assert :drained =
               poll_until(@receive_timeout, fn ->
                 state = :sys.get_state(consumer_pid)

                 if state.deferred_materializer_move_count == 0 and
                      state.deferred_replication_event_count == 0 do
                   {:ok, :drained}
                 else
                   :retry
                 end
               end)

      state = :sys.get_state(consumer_pid)
      assert state.last_processed_replication_tx_offset == 706
      assert state.last_seen_global_lsn < 706
    end

    test "a queued later root transaction proves the earlier causal frontier", ctx do
      parent = self()

      Repatch.patch(Materializer, :forward_causal_begin, [mode: :shared], fn _, _, _ -> :ok end)

      Repatch.patch(Materializer, :forward_causal_end, [mode: :shared], fn _, _, _ ->
        send(parent, {:causal_end_forwarding, self()})

        receive do
          :release_causal_end -> :ok
        after
          @receive_timeout * 5 -> raise "timed out waiting to release causal end"
        end
      end)

      Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery_or_value, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      changes_ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)

      :sys.replace_state(consumer_pid, fn state ->
        %{state | materializer_subscribed?: true}
      end)

      causal_offset = LogOffset.new(707, 2)
      token = Materializer.new_causal_token(causal_offset)

      assert :ok =
               Consumer.reserve_materializer_batch(
                 consumer_pid,
                 dep_handle,
                 token,
                 causal_offset,
                 100
               )

      assert :ok =
               Consumer.deliver_materializer_causal_end(
                 consumer_pid,
                 dep_handle,
                 token,
                 @receive_timeout
               )

      later_root_txn =
        complete_txn_fragment(708, Lsn.from_integer(708), [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "708", "value" => "causal-root"},
            key: ~s'"public"."test_table"/"708"',
            log_offset: LogOffset.new(708, 0)
          }
        ])

      assert :ok =
               GenServer.call(
                 consumer_pid,
                 {:handle_event, later_root_txn,
                  Electric.Telemetry.OpenTelemetry.get_current_context()},
                 :infinity
               )

      # A later collector call cannot exist until tx 707 finished publishing.
      # Process the earlier causal slot first so tx 708 is evaluated against the
      # dependency view that tx 707 established.
      assert_receive {:causal_end_forwarding, ^consumer_pid}, @receive_timeout
      refute_receive {^changes_ref, :new_changes, _offset}, 100
      send(consumer_pid, :release_causal_end)
      assert_receive {^changes_ref, :new_changes, _offset}, @receive_timeout

      assert :drained =
               poll_until(@receive_timeout, fn ->
                 state = :sys.get_state(consumer_pid)

                 if state.deferred_materializer_move_count == 0 and
                      state.deferred_replication_event_count == 0 do
                   {:ok, :drained}
                 else
                   :retry
                 end
               end)
    end

    test "fragmented same-transaction root drains through commit before causal work", ctx do
      parent = self()

      Repatch.patch(Materializer, :forward_causal_begin, [mode: :shared], fn _, _, _ -> :ok end)

      Repatch.patch(Materializer, :forward_causal_end, [mode: :shared], fn _, _, _ ->
        send(parent, {:causal_end_forwarding, self()})

        receive do
          :release_causal_end -> :ok
        after
          @receive_timeout * 5 -> raise "timed out waiting to release causal end"
        end
      end)

      Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery_or_value, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      changes_ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)

      :sys.replace_state(consumer_pid, fn state ->
        %{state | materializer_subscribed?: true}
      end)

      offset = LogOffset.new(709, 4)
      token = Materializer.new_causal_token(offset)

      assert :ok =
               Consumer.reserve_materializer_batch(
                 consumer_pid,
                 dep_handle,
                 token,
                 offset,
                 100
               )

      assert :ok =
               Consumer.deliver_materializer_causal_end(
                 consumer_pid,
                 dep_handle,
                 token,
                 @receive_timeout
               )

      first_fragment =
        txn_fragment(
          709,
          709,
          [
            %Changes.NewRecord{
              relation: {"public", "test_table"},
              record: %{"id" => "709", "value" => "causal-root"},
              key: ~s'"public"."test_table"/"709"',
              log_offset: LogOffset.new(709, 0)
            }
          ],
          has_begin?: true
        )

      commit_fragment = txn_fragment(709, 709, [], has_commit?: true)

      assert :ok =
               GenServer.call(
                 consumer_pid,
                 {:handle_event, first_fragment,
                  Electric.Telemetry.OpenTelemetry.get_current_context()},
                 :infinity
               )

      assert :fragment_buffered =
               poll_until(@receive_timeout, fn ->
                 state = :sys.get_state(consumer_pid)

                 if not is_nil(state.pending_txn) and
                      state.deferred_replication_event_count == 0 do
                   {:ok, :fragment_buffered}
                 else
                   :retry
                 end
               end)

      refute_receive {:causal_end_forwarding, ^consumer_pid}, 100

      assert :ok =
               GenServer.call(
                 consumer_pid,
                 {:handle_event, commit_fragment,
                  Electric.Telemetry.OpenTelemetry.get_current_context()},
                 :infinity
               )

      assert_receive {^changes_ref, :new_changes, _offset}, @receive_timeout
      assert_receive {:causal_end_forwarding, ^consumer_pid}, @receive_timeout
      send(consumer_pid, :release_causal_end)

      assert :drained =
               poll_until(@receive_timeout, fn ->
                 state = :sys.get_state(consumer_pid)

                 if is_nil(state.pending_txn) and
                      state.deferred_materializer_move_count == 0 and
                      state.deferred_replication_event_count == 0 do
                   {:ok, :drained}
                 else
                   :retry
                 end
               end)
    end

    test "a pulled replay payload yields to a same-transaction root arriving afterwards", ctx do
      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery_or_value, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      changes_ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)

      replay_offset = LogOffset.new(720, 2)

      patch_next_replay(consumer_pid, self(), [
        {:ok, %{move_in: [], move_out: [], txids: [720], lsn: replay_offset}},
        :done
      ])

      arm_materializer_replay(consumer_pid, ctx.stack_id, dep_handle)
      send(consumer_pid, {:materializer_replay_ready, dep_handle})

      assert_receive {:next_replay_called, 1}, @receive_timeout

      assert :replay_waiting =
               poll_until(@receive_timeout, fn ->
                 state = :sys.get_state(consumer_pid)

                 if state.deferred_materializer_move_count == 1,
                   do: {:ok, :replay_waiting},
                   else: :retry
               end)

      refute_receive {:next_replay_called, 2}, 100

      root_txn =
        complete_txn_fragment(720, Lsn.from_integer(720), [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "720", "value" => "causal-root"},
            key: ~s'"public"."test_table"/"720"',
            log_offset: LogOffset.new(720, 0)
          }
        ])

      assert :ok =
               GenServer.call(
                 consumer_pid,
                 {:handle_event, root_txn,
                  Electric.Telemetry.OpenTelemetry.get_current_context()},
                 :infinity
               )

      assert_receive {^changes_ref, :new_changes, _offset}, @receive_timeout
      assert_receive {:next_replay_called, 2}, @receive_timeout
    end

    test "a same-transaction root already queued before replay pull still runs first", ctx do
      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery_or_value, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      changes_ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)

      replay_offset = LogOffset.new(721, 2)

      patch_next_replay(consumer_pid, self(), [
        {:ok, %{move_in: [], move_out: [], txids: [721], lsn: replay_offset}},
        :done
      ])

      root_txn =
        complete_txn_fragment(721, Lsn.from_integer(721), [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "721", "value" => "causal-root"},
            key: ~s'"public"."test_table"/"721"',
            log_offset: LogOffset.new(721, 0)
          }
        ])

      root_bytes = :erlang.external_size(root_txn)
      otel_ctx = Electric.Telemetry.OpenTelemetry.get_current_context()

      :sys.replace_state(consumer_pid, fn state ->
        materializer_pid = Materializer.whereis(ctx.stack_id, dep_handle)

        %{
          state
          | pending_materializer_replays: :queue.from_list([{dep_handle, materializer_pid}]),
            pending_materializer_replay_count: 1,
            materializer_barrier_active?: true,
            deferred_replication_events: :queue.from_list([{root_txn, otel_ctx, root_bytes}]),
            deferred_replication_event_count: 1,
            deferred_event_bytes: state.deferred_event_bytes + root_bytes
        }
      end)

      send(consumer_pid, {:materializer_replay_ready, dep_handle})

      assert_receive {:next_replay_called, 1}, @receive_timeout
      assert_receive {^changes_ref, :new_changes, _offset}, @receive_timeout
      assert_receive {:next_replay_called, 2}, @receive_timeout
    end

    test "a dependency-only replay waits for the collector global frontier", ctx do
      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      replay_offset = LogOffset.new(722, 0)

      patch_next_replay(consumer_pid, self(), [
        {:ok, %{move_in: [], move_out: [], txids: [722], lsn: replay_offset}},
        :done
      ])

      arm_materializer_replay(consumer_pid, ctx.stack_id, dep_handle)
      send(consumer_pid, {:materializer_replay_ready, dep_handle})

      assert_receive {:next_replay_called, 1}, @receive_timeout
      refute_receive {:next_replay_called, 2}, 100

      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 721)
      refute_receive {:next_replay_called, 2}, 100

      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 722)
      assert_receive {:next_replay_called, 2}, @receive_timeout

      assert :committed =
               poll_until(@receive_timeout, fn ->
                 state = :sys.get_state(consumer_pid)

                 if state.pending_materializer_replay_count == 0 and
                      Map.get(state.move_positions, dep_handle) == replay_offset do
                   {:ok, :committed}
                 else
                   :retry
                 end
               end)
    end

    test "dependency replay gates on causal origin while committing its local cursor", ctx do
      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      shape_storage = Storage.for_shape(shape_handle, ctx.storage)
      local_replay_cursor = LogOffset.new(100, 2)
      causal_origin = LogOffset.new(900, 4)

      patch_next_replay(consumer_pid, self(), [
        {:ok,
         %{
           move_in: [],
           move_out: [],
           txids: [900],
           lsn: local_replay_cursor,
           causal_origin: causal_origin,
           causal_depth: 2
         }},
        :done
      ])

      arm_materializer_replay(consumer_pid, ctx.stack_id, dep_handle)
      send(consumer_pid, {:materializer_replay_ready, dep_handle})

      assert_receive {:next_replay_called, 1}, @receive_timeout
      refute_receive {:next_replay_called, 2}, 100

      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 899)
      refute_receive {:next_replay_called, 2}, 100

      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 900)
      assert_receive {:next_replay_called, 2}, @receive_timeout

      assert :committed =
               poll_until(@receive_timeout, fn ->
                 with {:ok, positions} <- Storage.fetch_move_positions(shape_storage),
                      ^local_replay_cursor <- Map.get(positions, dep_handle),
                      {:ok, 900} <- Storage.fetch_root_delivery_tx_offset(shape_storage) do
                   {:ok, :committed}
                 else
                   _ -> :retry
                 end
               end)
    end

    test "pulls only one replay transaction until its move-in commit completes", ctx do
      parent = self()

      Repatch.patch(
        Electric.Shapes.Consumer.Effects,
        :query_move_in_async,
        [mode: :shared],
        fn _task_sup, _consumer_state, _buffering_state, consumer_pid ->
          send(parent, {:query_requested, consumer_pid})
          :ok
        end
      )

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      shape_storage = Storage.for_shape(shape_handle, ctx.storage)
      replay_offset = LogOffset.new(723, 0)

      patch_next_replay(consumer_pid, self(), [
        {:ok, %{move_in: [{1, "1"}], move_out: [], txids: [723], lsn: replay_offset}},
        :done
      ])

      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 723)
      arm_materializer_replay(consumer_pid, ctx.stack_id, dep_handle)
      send(consumer_pid, {:materializer_replay_ready, dep_handle})

      assert_receive {:next_replay_called, 1}, @receive_timeout
      assert_receive {:query_requested, ^consumer_pid}, @receive_timeout
      refute_receive {:next_replay_called, 2}, 100

      send(consumer_pid, {:pg_snapshot_known, {723, 724, []}})
      send_stored_move_in_complete(consumer_pid, shape_storage, [], Lsn.from_integer(723))

      assert_receive {:next_replay_called, 2}, @receive_timeout

      assert :committed =
               poll_until(@receive_timeout, fn ->
                 state = :sys.get_state(consumer_pid)

                 if Map.get(state.move_positions, dep_handle) == replay_offset,
                   do: {:ok, :committed},
                   else: :retry
               end)
    end

    test "replay queue overflow fails closed", ctx do
      Electric.StackConfig.put(ctx.stack_id, :subquery_buffer_max_transactions, 0)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      consumer_ref = Process.monitor(consumer_pid)

      patch_next_replay(consumer_pid, self(), [
        {:ok, %{move_in: [], move_out: [], txids: [724], lsn: LogOffset.new(724, 0)}}
      ])

      arm_materializer_replay(consumer_pid, ctx.stack_id, dep_handle)
      send(consumer_pid, {:materializer_replay_ready, dep_handle})

      assert_receive {:next_replay_called, 1}, @receive_timeout

      assert_receive {:DOWN, ^consumer_ref, :process, ^consumer_pid, {:shutdown, :cleanup}},
                     @receive_timeout
    end

    test "replay byte overflow fails closed", ctx do
      Electric.StackConfig.put(ctx.stack_id, :subquery_deferred_event_memory_limit_bytes, 1)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      consumer_ref = Process.monitor(consumer_pid)

      patch_next_replay(consumer_pid, self(), [
        {:ok,
         %{
           move_in: [],
           move_out: [String.duplicate("overflow", 128)],
           txids: [725],
           lsn: LogOffset.new(725, 0)
         }}
      ])

      arm_materializer_replay(consumer_pid, ctx.stack_id, dep_handle)
      send(consumer_pid, {:materializer_replay_ready, dep_handle})

      assert_receive {:next_replay_called, 1}, @receive_timeout

      assert_receive {:DOWN, ^consumer_ref, :process, ^consumer_pid, {:shutdown, :cleanup}},
                     @receive_timeout
    end

    test "move-in and causal frontier subscriptions release independently", ctx do
      alias Electric.Shapes.Consumer.Effects
      alias Electric.Shapes.Consumer.State

      state = %State{stack_id: ctx.stack_id}
      registry = Electric.StackSupervisor.registry_name(ctx.stack_id)

      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 42)

      state = Effects.acquire_global_lsn_subscription(state, :move_in)
      assert_receive {:global_last_seen_lsn, 42}

      state = Effects.acquire_global_lsn_subscription(state, :causal_barrier)
      assert_receive {:global_last_seen_lsn, 42}

      assert state.global_lsn_subscription_reasons == MapSet.new([:move_in, :causal_barrier])

      assert Enum.any?(
               Registry.lookup(registry, :global_lsn_updates),
               &match?({pid, _} when pid == self(), &1)
             )

      state = Effects.release_global_lsn_subscription(state, :move_in)
      assert state.global_lsn_subscription_reasons == MapSet.new([:causal_barrier])

      assert Enum.any?(
               Registry.lookup(registry, :global_lsn_updates),
               &match?({pid, _} when pid == self(), &1)
             )

      state = Effects.release_global_lsn_subscription(state, :causal_barrier)
      assert state.global_lsn_subscription_reasons == MapSet.new()

      refute Enum.any?(
               Registry.lookup(registry, :global_lsn_updates),
               &match?({pid, _} when pid == self(), &1)
             )
    end

    test "oversized root events behind a replay barrier invalidate instead of accumulating",
         ctx do
      Electric.StackConfig.put(ctx.stack_id, :subquery_deferred_event_memory_limit_bytes, 1)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)

      :sys.replace_state(consumer_pid, fn state ->
        %{state | materializer_barrier_active?: true}
      end)

      ref = Process.monitor(consumer_pid)

      txn =
        complete_txn_fragment(702, Lsn.from_integer(702), [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "702", "value" => String.duplicate("y", 1_024)},
            log_offset: LogOffset.new(702, 0)
          }
        ])

      assert :ok =
               GenServer.call(
                 consumer_pid,
                 {:handle_event, txn, Electric.Telemetry.OpenTelemetry.get_current_context()}
               )

      assert_receive {:DOWN, ^ref, :process, ^consumer_pid, {:shutdown, :cleanup}},
                     @receive_timeout
    end

    test "restore invalidates an outer shape when a dependency cursor is missing", ctx do
      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      shape_storage = Storage.for_shape(shape_handle, ctx.storage)

      {:ok, positions} = Storage.fetch_move_positions(shape_storage)
      assert map_size(positions) == 1
      :ok = Storage.set_move_positions!(%{}, shape_storage)

      old_ref = Process.monitor(consumer_pid)
      assert :ok = Consumer.stop(consumer_pid, :shutdown)
      assert_receive {:DOWN, ^old_ref, :process, ^consumer_pid, :shutdown}, @receive_timeout

      expected_error = "Failed to start consumer for #{shape_handle}"

      assert {:error, ^expected_error} =
               ShapeCache.start_consumer_for_handle(shape_handle, ctx.stack_id)

      # Restore waits for initialization, so the missing durable dependency
      # cursor is reported synchronously and the invalid outer shape is already
      # removed before the caller receives the error.
      assert Support.TestUtils.wait_until(
               fn -> is_nil(Consumer.whereis(ctx.stack_id, shape_handle)) end,
               @receive_timeout
             )

      refute ShapeCache.has_shape?(shape_handle, ctx.stack_id)
    end

    test "restore accepts an ordinary shape without a root-delivery frontier", ctx do
      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
      shape_storage = Storage.for_shape(shape_handle, ctx.storage)

      # The frontier proves how a subquery shape evaluated root transactions.
      # An ordinary shape has no dependency-local view that could change across
      # a restart, so it deliberately does not persist this record.
      assert {:ok, nil} = Storage.fetch_root_delivery_tx_offset(shape_storage)

      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      consumer_ref = Process.monitor(consumer_pid)

      assert :ok = Consumer.stop(consumer_pid, :shutdown)
      assert_receive {:DOWN, ^consumer_ref, :process, ^consumer_pid, :shutdown}, @receive_timeout
      :ok = ConsumerRegistry.remove_consumer(shape_handle, ctx.stack_id)

      assert {:ok, restored_pid} =
               ShapeCache.start_consumer_for_handle(shape_handle, ctx.stack_id)

      assert is_pid(restored_pid)
      assert Process.alive?(restored_pid)
      assert restored_pid != consumer_pid
      assert ShapeCache.has_shape?(shape_handle, ctx.stack_id)
    end

    test "materializer shutdown lazily restores a nested chain and keeps dependency flow live",
         ctx do
      test_pid = self()

      Repatch.patch(
        Electric.Shapes.Consumer.Effects,
        :query_move_in_async,
        [mode: :shared],
        fn _task_sup, _consumer_state, _buffering_state, consumer_pid ->
          send(test_pid, {:query_requested, consumer_pid})
          :ok
        end
      )

      Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)

      {outer_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_nested_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(outer_handle, ctx.stack_id)
      {:ok, outer_shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, outer_handle)
      [middle_handle] = outer_shape.shape_dependencies_handles
      {:ok, middle_shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, middle_handle)
      [leaf_handle] = middle_shape.shape_dependencies_handles

      outer_consumer_pid = Consumer.whereis(ctx.stack_id, outer_handle)
      middle_consumer_pid = Consumer.whereis(ctx.stack_id, middle_handle)
      leaf_consumer_pid = Consumer.whereis(ctx.stack_id, leaf_handle)

      assert Enum.all?(
               [outer_consumer_pid, middle_consumer_pid, leaf_consumer_pid],
               &is_pid/1
             )

      outer_storage = Storage.for_shape(outer_handle, ctx.storage)
      middle_storage = Storage.for_shape(middle_handle, ctx.storage)
      seed_ref = Shapes.Consumer.register_for_changes(ctx.stack_id, outer_handle)
      seed_lsn = Lsn.from_integer(100)

      # Seed the entire chain through the real leaf materializer. The move-in
      # query results are supplied deterministically because this unit harness
      # intentionally has no database pool.
      assert :ok =
               ShapeLogCollector.handle_event(
                 complete_txn_fragment(100, seed_lsn, [
                   %Changes.NewRecord{
                     relation: {"public", "something else"},
                     record: %{"id" => "1", "value" => "visible"},
                     log_offset: LogOffset.new(seed_lsn, 0)
                   }
                 ]),
                 ctx.stack_id
               )

      assert_receive {:query_requested, ^middle_consumer_pid}, @receive_timeout * 5
      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 100)
      send(middle_consumer_pid, {:pg_snapshot_known, {100, 101, []}})

      middle_tag =
        Electric.Shapes.SubqueryTags.make_value_hash(ctx.stack_id, middle_handle, "1")

      send_stored_move_in_complete(
        middle_consumer_pid,
        middle_storage,
        [
          [
            ~s'"public"."other_table"/"1"',
            [middle_tag],
            Jason.encode!(%{
              "key" => ~s'"public"."other_table"/"1"',
              "value" => %{"id" => "1", "value" => "middle"},
              "headers" => %{
                "operation" => "insert",
                "relation" => ["public", "other_table"],
                "tags" => [middle_tag],
                "active_conditions" => [true]
              }
            })
          ]
        ],
        seed_lsn
      )

      assert_receive {:query_requested, ^outer_consumer_pid}, @receive_timeout * 5
      send(outer_consumer_pid, {:pg_snapshot_known, {100, 101, []}})

      outer_tag = Electric.Shapes.SubqueryTags.make_value_hash(ctx.stack_id, outer_handle, "1")

      send_stored_move_in_complete(
        outer_consumer_pid,
        outer_storage,
        [
          [
            ~s'"public"."test_table"/"1"',
            [outer_tag],
            Jason.encode!(%{
              "key" => ~s'"public"."test_table"/"1"',
              "value" => %{"id" => "1", "value" => "outer"},
              "headers" => %{
                "operation" => "insert",
                "relation" => ["public", "test_table"],
                "tags" => [outer_tag],
                "active_conditions" => [true]
              }
            })
          ]
        ],
        seed_lsn
      )

      assert_receive {^seed_ref, :new_changes, _offset}, @receive_timeout * 5

      assert :seed_drained =
               poll_until(@receive_timeout * 5, fn ->
                 if Enum.all?([middle_consumer_pid, outer_consumer_pid], fn pid ->
                      state = :sys.get_state(pid)

                      not state.move_transaction_open? and
                        state.deferred_materializer_move_count == 0
                    end) do
                   {:ok, :seed_drained}
                 else
                   :retry
                 end
               end)

      {:ok, before_dependency_delete} = Storage.fetch_latest_offset(outer_storage)
      leaf_materializer_pid = Materializer.whereis(ctx.stack_id, leaf_handle)
      middle_materializer_pid = Materializer.whereis(ctx.stack_id, middle_handle)

      assert is_pid(leaf_materializer_pid)
      assert is_pid(middle_materializer_pid)

      old_consumers = %{
        outer_handle => outer_consumer_pid,
        middle_handle => middle_consumer_pid,
        leaf_handle => leaf_consumer_pid
      }

      consumer_refs =
        Map.new(old_consumers, fn {handle, pid} -> {handle, {pid, Process.monitor(pid)}} end)

      # The explicit tuple shutdown exercises that materializer-DOWN clause.
      # The middle materializer then exits with plain :shutdown when its source
      # consumer suspends, exercising the second clause on the outer consumer.
      assert :ok =
               GenServer.stop(leaf_materializer_pid, {:shutdown, :dependency_restart})

      suspend_reason = Electric.ShapeCache.ShapeCleaner.consumer_suspend_reason()

      Enum.each(consumer_refs, fn {_handle, {pid, ref}} ->
        assert_receive {:DOWN, ^ref, :process, ^pid, ^suspend_reason}, @receive_timeout * 5
      end)

      for handle <- [leaf_handle, middle_handle, outer_handle] do
        assert ShapeCache.has_shape?(handle, ctx.stack_id)
        assert is_nil(ConsumerRegistry.whereis(ctx.stack_id, handle))
      end

      assert {:ok, restored_outer_pid} =
               ShapeCache.start_consumer_for_handle(outer_handle, ctx.stack_id)

      assert :started = ShapeCache.await_snapshot_start(outer_handle, ctx.stack_id)
      assert restored_outer_pid != outer_consumer_pid

      for handle <- [leaf_handle, middle_handle, outer_handle] do
        restored_pid = Consumer.whereis(ctx.stack_id, handle)
        assert is_pid(restored_pid)
        assert Process.alive?(restored_pid)
        assert restored_pid != Map.fetch!(old_consumers, handle)
      end

      restored_leaf_materializer_pid = Materializer.whereis(ctx.stack_id, leaf_handle)
      restored_middle_materializer_pid = Materializer.whereis(ctx.stack_id, middle_handle)

      assert is_pid(restored_leaf_materializer_pid)
      assert restored_leaf_materializer_pid != leaf_materializer_pid
      assert is_pid(restored_middle_materializer_pid)
      assert restored_middle_materializer_pid != middle_materializer_pid

      # Only the leaf table changes after restoration. Its move-out must cross
      # both restored materializers and reach the outer shape under the same
      # persisted handle.
      restored_ref = Shapes.Consumer.register_for_changes(ctx.stack_id, outer_handle)
      delete_lsn = Lsn.from_integer(101)

      assert :ok =
               ShapeLogCollector.handle_event(
                 complete_txn_fragment(101, delete_lsn, [
                   %Changes.DeletedRecord{
                     relation: {"public", "something else"},
                     old_record: %{"id" => "1", "value" => "visible"},
                     log_offset: LogOffset.new(delete_lsn, 0)
                   }
                 ]),
                 ctx.stack_id
               )

      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 101)
      assert_receive {^restored_ref, :new_changes, _offset}, @receive_timeout * 5

      assert :dependency_move_reached_outer =
               poll_until(@receive_timeout * 5, fn ->
                 before_dependency_delete
                 |> Storage.get_log_stream(outer_storage)
                 |> Enum.map(&Jason.decode!/1)
                 |> Enum.any?(fn
                   %{"headers" => %{"event" => "move-out", "patterns" => patterns}} ->
                     patterns != []

                   _ ->
                     false
                 end)
                 |> case do
                   true -> {:ok, :dependency_move_reached_outer}
                   false -> :retry
                 end
               end)
    end

    test "consumer advances and persists the per-dependency moves-position on move application",
         ctx do
      parent = self()

      Repatch.patch(
        Electric.Shapes.Consumer.Effects,
        :query_move_in_async,
        [mode: :shared],
        fn _task_sup, _consumer_state, _buffering_state, consumer_pid ->
          send(parent, {:query_requested, consumer_pid})
          :ok
        end
      )

      Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles

      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)
      shape_storage = Storage.for_shape(shape_handle, ctx.storage)

      first_move_lsn = LogOffset.new(777, 0)
      final_move_lsn = LogOffset.new(778, 0)

      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 100)

      send(
        consumer_pid,
        {:materializer_changes, dep_handle,
         %{move_in: [{1, "1"}], move_out: [], lsn: first_move_lsn}}
      )

      assert_receive {:query_requested, ^consumer_pid}, @receive_timeout
      assert :sys.get_state(consumer_pid).move_transaction_open?
      refute_receive {^ref, :new_changes, _offset}, 100

      # Queue another payload while the first asynchronous query is in flight.
      # Payloads are serialized so each gets a bounded atomic transaction and a
      # publication boundary; sustained traffic cannot keep one transaction
      # open forever.
      send(
        consumer_pid,
        {:materializer_changes, dep_handle,
         %{move_in: [{2, "2"}], move_out: [], lsn: final_move_lsn}}
      )

      assert :sys.get_state(consumer_pid).deferred_materializer_move_count == 1

      # A root-table transaction arriving after the second dependency payload
      # must not be evaluated against the first payload's older dependency
      # view. The synchronous collector call is acknowledged, but the event is
      # held behind the dependency barrier until both moves commit.
      root_txn =
        complete_txn_fragment(779, Lsn.from_integer(779), [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "99", "value" => "after-dependency"},
            log_offset: LogOffset.new(Lsn.from_integer(779), 0)
          }
        ])

      assert :ok =
               GenServer.call(
                 consumer_pid,
                 {:handle_event, root_txn,
                  Electric.Telemetry.OpenTelemetry.get_current_context()},
                 :infinity
               )

      barrier_state = :sys.get_state(consumer_pid)
      assert barrier_state.materializer_barrier_active?
      assert barrier_state.move_transaction_open?
      assert barrier_state.deferred_replication_event_count == 1

      # While the move-in is still buffering the position has NOT advanced to the
      # move's LSN — it must only advance once the move is applied.
      {:ok, buffering_positions} = Storage.fetch_move_positions(shape_storage)
      refute Map.get(buffering_positions, dep_handle) in [first_move_lsn, final_move_lsn]

      send(consumer_pid, {:pg_snapshot_known, {100, 300, []}})

      send_stored_move_in_complete(
        consumer_pid,
        shape_storage,
        [
          [
            ~s'"public"."test_table"/"1"',
            [],
            Jason.encode!(%{
              "key" => ~s'"public"."test_table"/"1"',
              "value" => %{"id" => "1", "value" => "val"},
              "headers" => %{"operation" => "insert", "relation" => ["public", "test_table"]}
            })
          ]
        ],
        Lsn.from_integer(100)
      )

      assert_receive {^ref, :new_changes, _offset}, @receive_timeout * 5
      assert_receive {:query_requested, ^consumer_pid}, @receive_timeout

      {:ok, first_applied_positions} = Storage.fetch_move_positions(shape_storage)
      assert Map.get(first_applied_positions, dep_handle) == first_move_lsn

      {:ok, first_move_boundary} = Storage.fetch_latest_offset(shape_storage)

      {_offset, marker} =
        Storage.get_log_stream_with_offsets(
          LogOffset.last_before_real_offsets(),
          first_move_boundary,
          shape_storage
        )
        |> Enum.to_list()
        |> List.last()

      assert %{
               "headers" => %{
                 "event" => "move-out",
                 "patterns" => [],
                 "txids" => [],
                 "last" => true
               }
             } = Jason.decode!(marker)

      state_between_splices = :sys.get_state(consumer_pid)
      assert state_between_splices.move_transaction_open?
      assert state_between_splices.deferred_materializer_move_count == 0
      assert state_between_splices.materializer_barrier_active?
      assert state_between_splices.deferred_replication_event_count == 0

      # The root transaction can leave the Consumer-level queue only after the
      # second dependency move is active. Buffering it in that move proves it
      # will be classified against the post-778 dependency view, not the older
      # post-777 view.
      assert %Electric.Shapes.Consumer.EventHandler.Subqueries.Buffering{
               active_move: %{
                 values: [{2, "2"}],
                 buffered_txn_count: 1,
                 buffered_txns: [%Changes.Transaction{xid: 779}]
               }
             } = state_between_splices.event_handler

      test_pid = self()

      subscription_task =
        Task.async(fn ->
          Consumer.subscribe_materializer(ctx.stack_id, shape_handle, test_pid)
        end)

      # Subscription cannot expose the partially spliced second move. It stays
      # pending while the Consumer remains free to receive query completion.
      assert Task.yield(subscription_task, 50) == nil

      send(consumer_pid, {:pg_snapshot_known, {100, 300, []}})

      send_stored_move_in_complete(
        consumer_pid,
        shape_storage,
        [
          [
            ~s'"public"."test_table"/"2"',
            [],
            Jason.encode!(%{
              "key" => ~s'"public"."test_table"/"2"',
              "value" => %{"id" => "2", "value" => "second"},
              "headers" => %{"operation" => "insert", "relation" => ["public", "test_table"]}
            })
          ]
        ],
        Lsn.from_integer(100)
      )

      assert_receive {^ref, :new_changes, _offset}, @receive_timeout * 5
      refute :sys.get_state(consumer_pid).move_transaction_open?

      assert {:ok, {:ok, published_offset}} =
               Task.yield(subscription_task, @receive_timeout)

      assert published_offset == :sys.get_state(consumer_pid).latest_offset

      assert :drained =
               poll_until(@receive_timeout, fn ->
                 state = :sys.get_state(consumer_pid)

                 if not state.materializer_barrier_active? and
                      state.deferred_replication_event_count == 0 do
                   {:ok, :drained}
                 else
                   :retry
                 end
               end)

      # Draining the pipeline commits the splice boundary and dependency cursor
      # together. There is no post-flush mailbox window where data is durable
      # while the cursor still points behind it.
      {:ok, applied_positions} = Storage.fetch_move_positions(shape_storage)
      assert Map.get(applied_positions, dep_handle) == final_move_lsn
    end

    test "consumer startup seeds the stack-scoped subquery index", ctx do
      alias Electric.Shapes.Filter.Indexes.SubqueryIndex

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      # The consumer should have seeded the SubqueryIndex during initialization
      index = SubqueryIndex.for_stack(ctx.stack_id)
      assert index != nil

      # The shape should be registered with positions (by Filter.add_shape)
      assert SubqueryIndex.has_positions?(index, shape_handle)

      # The shape should be marked ready (no longer in fallback) once
      # the consumer has seeded the index. After await_snapshot_start returns
      # the consumer has completed initialization including subquery seeding.
      {:ok, _shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)

      # The consumer seeds the index via SubqueryIndex.for_stack, but the
      # index is also modified by the Filter (which runs in the
      # ShapeLogCollector process). Check that the shape has positions
      # and that membership entries are correct (empty views for a fresh shape).
      positions = SubqueryIndex.positions_for_shape(index, shape_handle)
      assert length(positions) > 0

      # Verify the index is accessible and has retained node registrations.
      assert positions == SubqueryIndex.positions_for_shape(index, shape_handle)
    end

    test "consumer steady dependency move_in adds value to the subquery index", ctx do
      alias Electric.Shapes.Filter.Indexes.SubqueryIndex

      parent = self()

      Repatch.patch(
        Electric.Shapes.Consumer.Effects,
        :query_move_in_async,
        [mode: :shared],
        fn _task_sup, _consumer_state, _buffering_state, consumer_pid ->
          send(parent, {:query_requested, consumer_pid})
          :ok
        end
      )

      Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      index = SubqueryIndex.for_stack(ctx.stack_id)
      {:ok, _shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)

      # Before any dependency changes, the index has empty membership
      refute SubqueryIndex.member?(index, shape_handle, ["$sublink", "0"], 1)

      # Send a new record for the dependency table to trigger a move_in
      ShapeLogCollector.handle_event(
        complete_txn_fragment(100, Lsn.from_integer(50), [
          %Changes.NewRecord{
            relation: {"public", "other_table"},
            record: %{"id" => "1"},
            log_offset: LogOffset.new(Lsn.from_integer(50), 0)
          }
        ]),
        ctx.stack_id
      )

      # Wait for the consumer to process the event and request a move_in query
      assert_receive {:query_requested, consumer_pid}, @receive_timeout

      # During buffering, the value should have been added to the index
      # (union for positive dependency: before ∪ after)
      assert SubqueryIndex.member?(index, shape_handle, ["$sublink", "0"], 1)

      # Complete the move_in query to transition back to steady state
      send(consumer_pid, {:pg_snapshot_known, {100, 300, []}})

      shape_storage = Storage.for_shape(shape_handle, ctx.storage)

      send_stored_move_in_complete(
        consumer_pid,
        shape_storage,
        [
          [
            ~s'"public"."test_table"/"1"',
            [],
            Jason.encode!(%{
              "key" => ~s'"public"."test_table"/"1"',
              "value" => %{"id" => "1", "value" => "val"},
              "headers" => %{
                "operation" => "insert",
                "relation" => ["public", "test_table"]
              }
            })
          ]
        ],
        Lsn.from_integer(100)
      )

      # Allow the consumer to process the completion
      assert :ok = LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, 100)
      ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)
      assert_receive {^ref, :new_changes, _offset}, @receive_timeout

      # After move_in completes, value should still be in the index (now steady state)
      assert SubqueryIndex.member?(index, shape_handle, ["$sublink", "0"], 1)
    end

    test "consumer cleanup removes shape rows from the subquery index", ctx do
      alias Electric.Shapes.Filter.Indexes.SubqueryIndex

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      index = SubqueryIndex.for_stack(ctx.stack_id)
      assert SubqueryIndex.has_positions?(index, shape_handle)

      # Monitor the consumer so we know when cleanup finishes
      consumer_name = Shapes.Consumer.name(ctx.stack_id, shape_handle)
      consumer_pid = GenServer.whereis(consumer_name)
      ref = Process.monitor(consumer_pid)

      expect_shape_status(remove_shape: fn _, ^shape_handle -> :ok end)
      ShapeCache.clean_shape(shape_handle, ctx.stack_id)

      # Wait for consumer to shut down, flushing any other messages first
      assert_receive {:DOWN, ^ref, :process, ^consumer_pid, _reason}, 5000

      # The ShapeLogCollector removes the shape from the filter asynchronously.
      # Wait briefly for it to process.
      Process.sleep(100)

      # After cleanup, the shape's rows should be removed from the index
      refute SubqueryIndex.has_positions?(index, shape_handle)
    end

    test "dependency consumer survives a :noproc from its materializer without removing the shape",
         ctx do
      # Bug 6 cascade route: during a stack restart's shutdown, the dependency
      # consumer's inline call into its materializer can race the
      # materializer's death and exit with :noproc. Without the catch in
      # notify_materializer_of_new_changes/3, that crashes the consumer with a
      # non-shutdown reason, which routes through handle_writer_termination and
      # removes the shape from disk — mid stack-shutdown that leaves the shape
      # half-removed and 409s on the next poll after restart. The catch must
      # absorb the exit so the pending :DOWN can drive a clean stop instead.

      # Make the dependency consumer's notification call into the materializer
      # exit exactly as a GenServer.call to an already-dead process would.
      Repatch.patch(Consumer.Materializer, :new_changes, [mode: :shared], fn _, _, _ ->
        exit({:noproc, {GenServer, :call, [:materializer, :new_changes, 5000]}})
      end)

      Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)

      # If the bug were present the consumer would crash and remove the shape;
      # assert remove_shape is never called.
      patch_shape_status(
        remove_shape: fn _, handle ->
          raise "Unexpected remove_shape for #{handle}"
        end
      )

      {shape_handle, _} =
        ShapeCache.get_or_create_shape_handle(@shape_with_subquery, ctx.stack_id)

      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      {:ok, shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
      [dep_handle] = shape.shape_dependencies_handles

      dep_consumer = Consumer.whereis(ctx.stack_id, dep_handle)
      assert is_pid(dep_consumer)
      ref = Process.monitor(dep_consumer)

      # A change to the dependency table makes the dependency consumer notify
      # its materializer — hitting the patched, exiting call.
      ShapeLogCollector.handle_event(
        complete_txn_fragment(100, Lsn.from_integer(50), [
          %Changes.NewRecord{
            relation: {"public", "other_table"},
            record: %{"id" => "1"},
            log_offset: LogOffset.new(Lsn.from_integer(50), 0)
          }
        ]),
        ctx.stack_id
      )

      # With the catch, the dependency consumer absorbs the :noproc and stays
      # alive; the shape is not removed.
      refute_receive {:DOWN, ^ref, :process, _, _}, 500
      assert Consumer.whereis(ctx.stack_id, dep_handle) == dep_consumer
    end
  end

  defp refute_storage_calls_for_txn_fragment(shape_handle) do
    refute_receive {Support.TestStorage, :append_to_log!, ^shape_handle, _}
    refute_receive {Support.TestStorage, :append_fragment_to_log!, ^shape_handle, _}
    refute_receive {Support.TestStorage, :signal_txn_commit!, ^shape_handle, _}
  end

  defp assert_consumer_shutdown(stack_id, shape_handle, fun, timeout \\ 5000) do
    monitors =
      for name <- [
            Shapes.Consumer.name(stack_id, shape_handle),
            Shapes.Consumer.Snapshotter.name(stack_id, shape_handle)
          ],
          pid = GenServer.whereis(name) do
        ref = Process.monitor(pid)
        {ref, pid}
      end

    fun.()

    for {ref, pid} <- monitors do
      assert_receive {:DOWN, ^ref, :process, ^pid, reason}
                     when reason in [:shutdown, {:shutdown, :cleanup}],
                     timeout
    end
  end

  defp enable_storage_tracer_for(consumer_pid) do
    Support.Trace.trace_storage_calls(
      pid: consumer_pid,
      functions: [:append_to_log!, :append_fragment_to_log!, :signal_txn_commit!]
    )
  end

  describe "process gc configuration" do
    setup [
      :with_registry,
      :with_in_memory_storage,
      :with_shape_status,
      :with_lsn_tracker,
      :with_persistent_kv,
      :with_status_monitor,
      :with_dynamic_consumer_supervisor,
      :with_noop_publication_manager,
      :with_shape_cleaner
    ]

    setup ctx do
      start_link_supervised!({
        ShapeLogCollector.Supervisor,
        stack_id: ctx.stack_id, persistent_kv: ctx.persistent_kv, inspector: @base_inspector
      })

      ShapeLogCollector.mark_as_ready(ctx.stack_id)
      [shape_position: @shape_position]
    end

    @tag process_spawn_opts: %{consumer: [fullsweep_after: 4, priority: :high]}
    test "spawn_opts are correctly passed to consumer process", ctx do
      support_test_storage_wrap(ctx, @shape_handle1, @shape1)

      {:ok, consumer} =
        start_supervised(
          {Consumer,
           %{
             shape_handle: @shape_handle1,
             stack_id: ctx.stack_id
           }},
          id: {Consumer, @shape_handle1}
        )

      Consumer.initialize_shape(consumer, @shape1, %{action: :create})
      assert_receive {Support.TestStorage, :init_writer!, @shape_handle1, @shape1}
      :started = Consumer.await_snapshot_start(ctx.stack_id, @shape_handle1)

      info = Process.info(consumer)

      assert info[:priority] == :high
      assert info[:garbage_collection][:fullsweep_after] == 4
    end
  end

  defp support_test_storage_wrap(ctx, shape_handle, shape) do
    %{snapshot_xmin: xmin} = shape_status(shape_handle, ctx)
    shapes = %{shape_handle => shape}

    storage =
      Support.TestStorage.wrap(ctx.storage, %{
        shape_handle => [
          {:mark_snapshot_as_started, []},
          {:set_pg_snapshot, [%{xmin: xmin, xmax: xmin + 1, xip_list: [xmin]}]}
        ]
      })

    Electric.StackConfig.put(ctx.stack_id, Electric.ShapeCache.Storage, storage)
    Electric.StackConfig.put(ctx.stack_id, :inspector, @base_inspector)

    patch_shape_status(fetch_shape_by_handle: fn _, sh -> Map.fetch(shapes, sh) end)

    Support.TestUtils.activate_mocks_for_descendant_procs(Consumer)
    Support.TestUtils.activate_mocks_for_descendant_procs(Electric.ShapeCache.ShapeCleaner)
    :ok
  end

  describe "should_force_gc?/5" do
    # All tests pass explicit now_ms / last_gc_at / min_interval_ms so they are
    # fully deterministic and do not depend on wall-clock time.

    test "false when threshold is nil (adaptive GC disabled)" do
      refute Electric.Shapes.Consumer.should_force_gc?(1_000_000, nil, nil, 5_000, 1_000)
    end

    test "true when heap over threshold and consumer has never forced a GC (last_gc_at nil)" do
      # 1_000 bytes > threshold of 1 byte
      assert Electric.Shapes.Consumer.should_force_gc?(1_000, 1, nil, 5_000, 1_000)
    end

    test "false when heap over threshold but interval has not elapsed" do
      # last_gc_at=4_500, now=5_000 → delta=500 < min_interval=1_000 → no GC
      refute Electric.Shapes.Consumer.should_force_gc?(1_000, 1, 4_500, 5_000, 1_000)
    end

    test "true when heap over threshold and interval has elapsed" do
      # last_gc_at=3_000, now=5_000 → delta=2_000 >= min_interval=1_000 → GC
      assert Electric.Shapes.Consumer.should_force_gc?(1_000, 1, 3_000, 5_000, 1_000)
    end

    test "true at exactly the min interval boundary" do
      # last_gc_at=4_000, now=5_000 → delta=1_000 == min_interval=1_000 → GC
      assert Electric.Shapes.Consumer.should_force_gc?(1_000, 1, 4_000, 5_000, 1_000)
    end

    test "false when heap is under threshold regardless of timing" do
      # heap=1 byte; threshold=1_000 bytes → under
      refute Electric.Shapes.Consumer.should_force_gc?(1, 1_000, nil, 5_000, 1_000)
    end

    test "false when heap is under threshold even if interval would have elapsed" do
      refute Electric.Shapes.Consumer.should_force_gc?(1, 1_000, 0, 5_000, 1_000)
    end

    test "false when heap exactly equals threshold (strict comparison)" do
      refute Electric.Shapes.Consumer.should_force_gc?(1_000, 1_000, nil, 5_000, 1_000)
    end
  end

  describe "adaptive GC after fragment processing" do
    @describetag :tmp_dir

    setup do
      %{inspector: @base_inspector, pool: nil}
    end

    setup [
      :with_registry,
      :with_pure_file_storage,
      :with_shape_status,
      :with_lsn_tracker,
      :with_log_chunking,
      :with_persistent_kv,
      :with_async_deleter,
      :with_shape_cleaner,
      :with_shape_log_collector,
      :with_noop_publication_manager,
      :with_status_monitor
    ]

    setup(ctx) do
      delay_snapshot_creation? = Map.get(ctx, :delay_snapshot_creation?)
      test_pid = self()

      patch_snapshotter(fn parent, shape_handle, _shape, %{snapshot_fun: snapshot_fun} ->
        if delay_snapshot_creation? do
          receive do
            {^test_pid, :resume} -> :ok
          end
        end

        pg_snapshot = {10, 11, [10]}
        GenServer.cast(parent, {:pg_snapshot_known, shape_handle, pg_snapshot})
        GenServer.cast(parent, {:snapshot_started, shape_handle})
        snapshot_fun.([])
      end)

      Electric.StackConfig.put(ctx.stack_id, :shape_hibernate_after, 10_000)
      :ok
    end

    setup ctx do
      %{consumer_supervisor: consumer_supervisor, shape_cache: shape_cache} =
        Support.ComponentSetup.with_shape_cache(ctx)

      %{
        consumer_supervisor: consumer_supervisor,
        shape_cache: shape_cache
      }
    end

    test "GC runs when heap exceeds tiny threshold", ctx do
      Electric.StackConfig.put(ctx.stack_id, :consumer_gc_heap_threshold, 1)

      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)
      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)

      xid = 11
      lsn = Lsn.from_integer(10)
      large_binary = :binary.copy(<<0>>, 200_000)

      txn =
        complete_txn_fragment(xid, lsn, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "1", "value" => large_binary},
            log_offset: LogOffset.new(lsn, 0)
          }
        ])

      assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)
      assert_receive {^ref, :new_changes, _}, @receive_timeout

      # GC runs in the deferred {:continue, :maybe_gc} after the reply. A synchronous
      # call is queued behind the pending continue, so :sys.get_state returns only
      # once the GC has run — and lets us read last_forced_gc_at, which the consumer
      # stamps iff it forced a sweep. That is a direct signal of our decision,
      # immune to natural BEAM GCs and heap-size timing.
      assert %{last_forced_gc_at: forced_at} = :sys.get_state(consumer_pid)

      refute is_nil(forced_at),
             "threshold=1 keeps the heap over threshold, so a forced GC should be recorded"
    end

    test "GC does not run when threshold is very large", ctx do
      # 1 GB threshold — the consumer heap will never reach this, so GC must NOT fire.
      Electric.StackConfig.put(ctx.stack_id, :consumer_gc_heap_threshold, 1_000_000_000)

      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)
      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)
      ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)

      xid = 11
      lsn = Lsn.from_integer(10)
      large_binary = :binary.copy(<<0>>, 200_000)

      txn =
        complete_txn_fragment(xid, lsn, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "1", "value" => large_binary},
            log_offset: LogOffset.new(lsn, 0)
          }
        ])

      assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)
      assert_receive {^ref, :new_changes, _}, @receive_timeout

      # Flush the deferred :maybe_gc continue and read the state. The heap stays well
      # under the 1 GB threshold, so the consumer must not have forced a sweep.
      assert %{last_forced_gc_at: forced_at} = :sys.get_state(consumer_pid)

      assert is_nil(forced_at),
             "no forced GC should be recorded while under threshold, got #{inspect(forced_at)}"
    end

    test "no GC by default (threshold=nil)", ctx do
      # Ensure no threshold is set (default behaviour)
      assert nil == Electric.StackConfig.lookup(ctx.stack_id, :consumer_gc_heap_threshold, nil)

      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)
      :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)

      ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)

      xid = 11
      lsn = Lsn.from_integer(10)

      txn =
        complete_txn_fragment(xid, lsn, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "1"},
            log_offset: LogOffset.new(lsn, 0)
          }
        ])

      # Should process without error even when no GC threshold is configured
      assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)
      assert_receive {^ref, :new_changes, _}, @receive_timeout
    end

    @tag delay_snapshot_creation?: true
    test "GC runs during buffered-fragment drain when heap exceeds threshold", ctx do
      # threshold=1 forces a GC once the buffered fragments are drained. The consumer
      # starts with buffering?=true; fragments sent before pg_snapshot_known land in the
      # buffer. When we unblock the snapshotter it fires pg_snapshot_known which triggers
      # :consume_buffer → drains the buffer → {:continue, :maybe_gc} runs the GC once.
      Electric.StackConfig.put(ctx.stack_id, :consumer_gc_heap_threshold, 1)

      {shape_handle, _} = ShapeCache.get_or_create_shape_handle(@shape1, ctx.stack_id)

      # The snapshotter is now running but blocked on `receive {^test_pid, :resume}`.
      assert_receive {:snapshot, ^shape_handle, snapshotter_pid}

      consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)

      # Send a large-payload fragment while buffering?=true — it goes into the buffer.
      large_binary = :binary.copy(<<0>>, 200_000)
      xid = 11
      lsn = Lsn.from_integer(10)

      txn =
        complete_txn_fragment(xid, lsn, [
          %Changes.NewRecord{
            relation: {"public", "test_table"},
            record: %{"id" => "1", "value" => large_binary},
            log_offset: LogOffset.new(lsn, 0)
          }
        ])

      assert :ok = ShapeLogCollector.handle_event(txn, ctx.stack_id)

      # Unblock the snapshotter: fires pg_snapshot_known → :consume_buffer → drains the
      # buffer, then defers a single GC via {:continue, :maybe_gc}.
      send(snapshotter_pid, {self(), :resume})

      ref = Shapes.Consumer.register_for_changes(ctx.stack_id, shape_handle)
      assert_receive {^ref, :new_changes, _}, @receive_timeout

      # The deferred GC runs in the :maybe_gc continue after the drain. Flush it with a
      # synchronous call (queued behind the continue), then check last_forced_gc_at —
      # the consumer stamps it iff it forced a sweep for the drained fragment.
      assert %{last_forced_gc_at: forced_at} = :sys.get_state(consumer_pid)

      refute is_nil(forced_at),
             "expected a forced GC to be recorded after the buffered-fragment drain"
    end
  end

  describe "set_gc_heap_threshold helpers" do
    # with_stack_id_from_test (line 87) already starts ProcessRegistry + StackConfig
    # for ctx.stack_id — no heavier setup is needed for these pure-config tests.

    test "set_gc_heap_threshold/2 writes the value into StackConfig", ctx do
      assert :ok = Electric.Shapes.Consumer.set_gc_heap_threshold(ctx.stack_id, 2_000_000)

      assert 2_000_000 ==
               Electric.StackConfig.lookup(ctx.stack_id, :consumer_gc_heap_threshold, nil)
    end

    test "set_gc_heap_threshold/2 accepts nil to disable", ctx do
      Electric.Shapes.Consumer.set_gc_heap_threshold(ctx.stack_id, 123)
      assert :ok = Electric.Shapes.Consumer.set_gc_heap_threshold(ctx.stack_id, nil)
      assert nil == Electric.StackConfig.lookup(ctx.stack_id, :consumer_gc_heap_threshold, nil)
    end
  end

  defp get_log_items_from_storage(offset, shape_storage) do
    Storage.get_log_stream(offset, shape_storage) |> Enum.map(&Jason.decode!/1)
  end

  defp send_stored_move_in_complete(consumer_pid, shape_storage, rows, lsn) do
    snapshot_name = Electric.Utils.uuid4()
    row_bytes = Enum.reduce(rows, 0, fn [_, _, json], acc -> acc + IO.iodata_length(json) end)

    Storage.write_move_in_snapshot!(rows, snapshot_name, shape_storage)

    send(
      consumer_pid,
      {:query_move_in_complete, snapshot_name, length(rows), row_bytes, lsn}
    )
  end

  defp assert_replay_sequence_precedes_live(shape, replay_offsets, live_offset, ctx) do
    consumer_pid =
      prepare_replay_and_live_dependency(shape, replay_offsets, live_offset, ctx)

    Enum.each(1..(length(replay_offsets) + 1), fn call ->
      assert next_scheduler_event(consumer_pid) == {:next_replay, call}
    end)

    assert next_scheduler_event(consumer_pid) == :live_dependency
  end

  defp assert_replay_sequence_follows_live(shape, replay_offset, live_offset, ctx) do
    consumer_pid =
      prepare_replay_and_live_dependency(shape, [replay_offset], live_offset, ctx)

    assert next_scheduler_event(consumer_pid) == {:next_replay, 1}
    assert next_scheduler_event(consumer_pid) == :live_dependency
    assert next_scheduler_event(consumer_pid) == {:next_replay, 2}
  end

  defp prepare_replay_and_live_dependency(shape, replay_offsets, live_offset, ctx) do
    replay_replies =
      Enum.map(replay_offsets, fn %LogOffset{tx_offset: tx_offset} = offset ->
        {:ok, %{move_in: [], move_out: [], txids: [tx_offset], lsn: offset}}
      end) ++ [:done]

    {consumer_pid, _replay_dep} =
      setup_replay_and_live_dependency(
        shape,
        replay_offsets,
        replay_replies,
        live_offset,
        ctx
      )

    consumer_pid
  end

  defp assert_pending_replay_order(shape, replay_offset, live_offset, expected_order, ctx) do
    replay_payload =
      {:ok,
       %{
         move_in: [],
         move_out: [],
         txids: [replay_offset.tx_offset],
         lsn: replay_offset
       }}

    {consumer_pid, replay_dep} =
      setup_replay_and_live_dependency(
        shape,
        [replay_offset],
        [:pending, replay_payload, :done],
        live_offset,
        ctx
      )

    assert next_scheduler_event(consumer_pid) == {:next_replay, 1}
    refute_receive {:scheduler_live_dependency_processed, ^consumer_pid}, 100

    waiting_state = :sys.get_state(consumer_pid)
    assert waiting_state.materializer_replay_waiting?
    assert waiting_state.deferred_materializer_move_count == 1
    refute :queue.is_empty(waiting_state.deferred_materializer_moves)

    send(consumer_pid, {:materializer_replay_ready, replay_dep})
    assert next_scheduler_event(consumer_pid) == {:next_replay, 2}

    case expected_order do
      :replay_first ->
        assert next_scheduler_event(consumer_pid) == {:next_replay, 3}
        assert next_scheduler_event(consumer_pid) == :live_dependency

      :live_first ->
        assert next_scheduler_event(consumer_pid) == :live_dependency
        assert next_scheduler_event(consumer_pid) == {:next_replay, 3}
    end
  end

  defp setup_replay_and_live_dependency(
         shape,
         replay_offsets,
         replay_replies,
         live_offset,
         ctx
       ) do
    test_pid = self()

    Repatch.patch(Materializer, :forward_causal_begin, [mode: :shared], fn _, _, _ -> :ok end)

    Repatch.patch(Materializer, :forward_causal_end, [mode: :shared], fn _, _, _ ->
      send(test_pid, {:scheduler_live_dependency_processed, self()})
      :ok
    end)

    {shape_handle, _} = ShapeCache.get_or_create_shape_handle(shape, ctx.stack_id)
    :started = ShapeCache.await_snapshot_start(shape_handle, ctx.stack_id)
    {:ok, persisted_shape} = Electric.Shapes.fetch_shape_by_handle(ctx.stack_id, shape_handle)
    [replay_dep, live_dep] = persisted_shape.shape_dependencies_handles
    consumer_pid = Consumer.whereis(ctx.stack_id, shape_handle)

    patch_next_replay(consumer_pid, test_pid, replay_replies)

    frontier =
      [live_offset | replay_offsets]
      |> Enum.map(& &1.tx_offset)
      |> Enum.max()

    arm_materializer_replay(consumer_pid, ctx.stack_id, replay_dep)

    :sys.replace_state(consumer_pid, fn state ->
      %{
        state
        | materializer_subscribed?: true,
          last_seen_global_lsn: frontier
      }
    end)

    live_token = Materializer.new_causal_token(live_offset)

    assert :ok =
             Consumer.reserve_materializer_batch(
               consumer_pid,
               live_dep,
               live_token,
               live_offset,
               100
             )

    assert :ok =
             Consumer.deliver_materializer_causal_end(
               consumer_pid,
               live_dep,
               live_token,
               @receive_timeout
             )

    {consumer_pid, replay_dep}
  end

  defp next_scheduler_event(consumer_pid) do
    receive do
      {:next_replay_called, call} ->
        {:next_replay, call}

      {:scheduler_live_dependency_processed, ^consumer_pid} ->
        :live_dependency
    after
      @receive_timeout ->
        flunk("timed out waiting for replay/live scheduler event")
    end
  end

  defp patch_next_replay(consumer_pid, test_pid, replies) do
    calls = :atomics.new(1, [])

    Repatch.patch(Materializer, :next_replay, [mode: :shared], fn _materializer, ^consumer_pid ->
      call = :atomics.add_get(calls, 1, 1)
      send(test_pid, {:next_replay_called, call})

      case Enum.fetch(replies, call - 1) do
        {:ok, reply} -> reply
        :error -> raise "unexpected Materializer.next_replay/2 call #{call}"
      end
    end)

    Repatch.allow(self(), consumer_pid)
    calls
  end

  defp arm_materializer_replay(consumer_pid, stack_id, dep_handle) do
    materializer_pid = Materializer.whereis(stack_id, dep_handle)
    assert is_pid(materializer_pid)

    :sys.replace_state(consumer_pid, fn state ->
      %{
        state
        | pending_materializer_replays: :queue.from_list([{dep_handle, materializer_pid}]),
          pending_materializer_replay_count: 1,
          materializer_barrier_active?: true
      }
    end)
  end
end
