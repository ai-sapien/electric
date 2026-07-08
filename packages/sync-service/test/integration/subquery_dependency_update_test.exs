defmodule Electric.Integration.SubqueryDependencyUpdateTest do
  @moduledoc """
  Tests for dependency tracking when intermediate rows move between parents.

  Scenario: A SaaS app where premium organizations get access to certain data.
  - Organizations can have a "premium" tag
  - Teams belong to organizations
  - Projects belong to teams
  - Tasks belong to projects

  Shape: "All tasks belonging to projects in teams under premium organizations"

  When a team moves from one premium org to another premium org, the tasks
  should remain in the shape. The dependency tracking must be updated so that
  removing the premium tag from the OLD org does not affect tasks that are
  now under a DIFFERENT org.
  """
  use ExUnit.Case, async: false

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup
  import Support.IntegrationSetup
  import Support.StreamConsumer

  alias Electric.Client
  alias Electric.Client.ShapeDefinition
  alias Electric.Client.Message.ChangeMessage
  alias Electric.ShapeCache
  alias Electric.Shapes.ConsumerRegistry

  @moduletag :tmp_dir

  # Shape: tasks in projects in teams in premium organizations
  @premium_tasks_where """
  project_id IN (
    SELECT id FROM projects WHERE team_id IN (
      SELECT id FROM teams WHERE org_id IN (
        SELECT id FROM organizations WHERE id IN (
          SELECT org_id FROM organization_tags WHERE tag = 'premium'
        )
      )
    )
  )
  """

  describe "dependency tracking when intermediate rows move between parents" do
    setup [:with_unique_db, :with_org_team_project_task_tables, :with_sql_execute]
    setup :with_complete_stack
    setup :with_electric_client

    @tag with_sql: [
           # Two organizations, both premium
           "INSERT INTO organizations (id, name) VALUES ('acme', 'Acme Corp'), ('globex', 'Globex Inc')",
           "INSERT INTO organization_tags (org_id, tag) VALUES ('acme', 'premium'), ('globex', 'premium')",
           # Engineering team belongs to Acme
           "INSERT INTO teams (id, name, org_id) VALUES ('engineering', 'Engineering', 'acme')",
           # Backend project belongs to Engineering team
           "INSERT INTO projects (id, name, team_id) VALUES ('backend', 'Backend API', 'engineering')",
           # A task in the Backend project
           "INSERT INTO tasks (id, title, project_id) VALUES ('task-1', 'Fix login bug', 'backend')"
         ]
    test "task remains when team moves to another premium org and old org loses tag", ctx do
      # SETUP:
      #   task-1 -> backend project -> engineering team -> acme org (PREMIUM)
      #
      # The shape includes task-1 because acme has the premium tag.

      shape = ShapeDefinition.new!("tasks", where: @premium_tasks_where)
      stream = Client.stream(ctx.client, shape, live: true)

      with_consumer stream do
        # Verify task-1 is in the shape initially
        assert_insert(consumer, %{"id" => "task-1", "title" => "Fix login bug"})
        assert_up_to_date(consumer)

        # MUTATION 1: Move engineering team from Acme to Globex
        # Both orgs have premium, so task-1 should STAY in the shape.
        # Path changes from: task-1 -> backend -> engineering -> acme (premium)
        #                to: task-1 -> backend -> engineering -> globex (premium)
        Postgrex.query!(
          ctx.db_conn,
          "UPDATE teams SET org_id = 'globex' WHERE id = 'engineering'",
          []
        )

        # Should NOT delete task-1 - it's still under a premium org
        messages_after_move = collect_messages(consumer, timeout: 1000)
        deletes_after_move = filter_deletes(messages_after_move)

        assert deletes_after_move == [],
               "Task should NOT be deleted when team moves to another premium org. " <>
                 "Got unexpected deletes: #{inspect(deletes_after_move)}"

        # MUTATION 2: Remove premium tag from Acme (the OLD org)
        # This should have NO EFFECT because:
        #   - Engineering team no longer belongs to Acme
        #   - task-1's path is now via Globex (which still has premium)
        Postgrex.query!(
          ctx.db_conn,
          "DELETE FROM organization_tags WHERE org_id = 'acme' AND tag = 'premium'",
          []
        )

        # Task should NOT be deleted - it's now connected via Globex
        messages_after_tag_removal = collect_messages(consumer, timeout: 1000)
        deletes_after_tag_removal = filter_deletes(messages_after_tag_removal)

        assert deletes_after_tag_removal == [],
               "Task should NOT be deleted when old org loses premium tag. " <>
                 "Task is now under Globex (which still has premium). " <>
                 "Got unexpected deletes: #{inspect(deletes_after_tag_removal)}"
      end
    end

    @tag with_sql: [
           # Four organizations: acme, globex, initech, umbrella - first 3 have premium
           "INSERT INTO organizations (id, name) VALUES ('acme', 'Acme'), ('globex', 'Globex'), ('initech', 'Initech'), ('umbrella', 'Umbrella')",
           "INSERT INTO organization_tags (org_id, tag) VALUES ('acme', 'premium'), ('globex', 'premium'), ('initech', 'premium')",
           # Teams under different orgs
           "INSERT INTO teams (id, name, org_id) VALUES ('team-a', 'Team A', 'acme'), ('team-b', 'Team B', 'globex'), ('team-c', 'Team C', 'initech'), ('team-d', 'Team D', 'umbrella')",
           # Projects under each team
           "INSERT INTO projects (id, name, team_id) VALUES ('proj-a', 'Project A', 'team-a'), ('proj-b', 'Project B', 'team-b'), ('proj-c', 'Project C', 'team-c'), ('proj-d', 'Project D', 'team-d')",
           # Tasks under each project
           "INSERT INTO tasks (id, title, project_id) VALUES ('task-a', 'Task A', 'proj-a'), ('task-b', 'Task B', 'proj-b'), ('task-c', 'Task C', 'proj-c'), ('task-d', 'Task D', 'proj-d')"
         ]
    test "multiple teams moving between premium orgs", ctx do
      # SETUP:
      #   task-a -> proj-a -> team-a -> acme (PREMIUM)     ✓ in shape
      #   task-b -> proj-b -> team-b -> globex (PREMIUM)   ✓ in shape
      #   task-c -> proj-c -> team-c -> initech (PREMIUM)  ✓ in shape
      #   task-d -> proj-d -> team-d -> umbrella (no tag)  ✗ not in shape

      shape = ShapeDefinition.new!("tasks", where: @premium_tasks_where)
      stream = Client.stream(ctx.client, shape, live: true)

      with_consumer stream do
        # Initial: 3 tasks from premium orgs
        initial_ids = collect_initial_inserts(consumer, 3)
        assert Enum.sort(initial_ids) == ["task-a", "task-b", "task-c"]
        assert_up_to_date(consumer)

        # Move team-c from Initech to Globex (both premium - no visible change expected)
        Postgrex.query!(ctx.db_conn, "UPDATE teams SET org_id = 'globex' WHERE id = 'team-c'", [])

        messages1 = collect_messages(consumer, timeout: 1000)

        assert filter_deletes(messages1) == [],
               "No deletes expected when moving between premium orgs"

        # Remove premium from Initech (team-c's OLD org)
        # task-c should stay because it's now under Globex
        Postgrex.query!(
          ctx.db_conn,
          "DELETE FROM organization_tags WHERE org_id = 'initech' AND tag = 'premium'",
          []
        )

        messages2 = collect_messages(consumer, timeout: 1000)
        deletes = filter_deletes(messages2)

        assert deletes == [],
               "task-c should NOT be deleted when Initech loses premium. " <>
                 "team-c is now under Globex (which still has premium). " <>
                 "Got unexpected deletes: #{inspect(deletes)}"
      end
    end
  end

  describe "subquery combined with other conditions" do
    # Tests for shapes that have a subquery ANDed with other non-subquery conditions.
    # The bug occurred when a change's sublink value was in a pending move-in, but
    # the record didn't match other parts of the WHERE clause. The old code would
    # incorrectly skip the change, assuming the move-in would cover it.

    setup [:with_unique_db, :with_simple_parent_child_tables, :with_sql_execute]
    setup :with_complete_stack
    setup :with_electric_client

    # Shape: children of active parents, but only if child is published
    # This combines a subquery with a simple column condition using AND
    @active_parents_published_children_where """
    parent_id IN (SELECT id FROM parents WHERE active = true) AND status = 'published'
    """

    @tag with_sql: [
           # Two parents: parent-a (active), parent-b (initially inactive)
           "INSERT INTO parents (id, name, active) VALUES ('parent-a', 'Parent A', true), ('parent-b', 'Parent B', false)",
           # A published child in parent-a (active) - in shape
           "INSERT INTO children (id, name, parent_id, status) VALUES ('child-1', 'Child One', 'parent-a', 'published')"
         ]
    test "child is deleted when moved to parent that satisfies subquery but child status fails",
         ctx do
      # SETUP:
      #   child-1 -> parent-a (ACTIVE), status=published -> in shape
      #   parent-b is NOT active
      #   Shape requires: parent is active AND status = 'published'
      #
      # In a SINGLE TRANSACTION we:
      #   1. Make parent-b active (triggers move-in for parent-b)
      #   2. Move child-1 to parent-b AND change status to 'draft'
      #
      # After the transaction:
      #   - parent-b IS now active (satisfies subquery)
      #   - But child-1 has status='draft' (fails the other condition)
      #   - child-1 should be DELETED from the shape (not covered by the move-in)

      shape = ShapeDefinition.new!("children", where: @active_parents_published_children_where)
      stream = Client.stream(ctx.client, shape, live: true)

      with_consumer stream do
        # Verify child-1 is in the shape initially
        assert_insert(consumer, %{"id" => "child-1", "name" => "Child One"})
        assert_up_to_date(consumer)

        # In a single transaction:
        # 1. Make parent-b active (triggers a move-in)
        # 2. Move child-1 to parent-b and change status to draft
        Postgrex.transaction(ctx.db_conn, fn conn ->
          Postgrex.query!(conn, "UPDATE parents SET active = true WHERE id = 'parent-b'", [])

          Postgrex.query!(
            conn,
            "UPDATE children SET parent_id = 'parent-b', status = 'draft' WHERE id = 'child-1'",
            []
          )
        end)

        # child-1 should be deleted because status='draft' fails the WHERE clause,
        # even though parent-b became active in the same transaction
        assert_delete(consumer, %{"id" => "child-1"})
      end
    end

    @tag with_sql: [
           # Two parents: parent-a (active), parent-b (initially inactive)
           "INSERT INTO parents (id, name, active) VALUES ('parent-a', 'Parent A', true), ('parent-b', 'Parent B', false)",
           # A published child in parent-b (inactive) - NOT in shape
           "INSERT INTO children (id, name, parent_id, status) VALUES ('child-1', 'Child One', 'parent-b', 'published')"
         ]
    test "child moves into shape when parent becomes active and child satisfies other conditions",
         ctx do
      # SETUP:
      #   child-1 -> parent-b (NOT active), status=published -> NOT in shape
      #   Shape requires: parent is active AND status = 'published'
      #
      # When parent-b becomes active:
      #   - parent-b IS now active (satisfies subquery)
      #   - child-1 has status='published' (satisfies other condition)
      #   - Both conditions satisfied, child-1 should be INSERTED into shape

      shape = ShapeDefinition.new!("children", where: @active_parents_published_children_where)
      stream = Client.stream(ctx.client, shape, live: true)

      with_consumer stream do
        # Initially no children in the shape (parent-b is not active)
        assert_up_to_date(consumer)

        # Make parent-b active
        Postgrex.query!(
          ctx.db_conn,
          "UPDATE parents SET active = true WHERE id = 'parent-b'",
          []
        )

        # child-1 should now appear in the shape (both conditions now satisfied)
        assert_insert(consumer, %{"id" => "child-1", "name" => "Child One"})
      end
    end
  end

  @message_artifact_where """
  id IN (
    SELECT artifact_id FROM message_artifacts WHERE message_id IN (
      SELECT id FROM messages WHERE chat_id = 'chat-1'
    )
  )
  """

  describe "existing root rows becoming eligible through nested links" do
    setup [:with_unique_db, :with_message_artifact_tables]
    setup :with_complete_stack
    setup :with_electric_client

    @tag replication_opts_overrides: [slot_temporary?: false]
    test "streams an artifact through a new outer shape that reuses restored dependencies", ctx do
      persisted_shape = ShapeDefinition.new!("artifacts", where: @message_artifact_where)

      initial_stream = Client.stream(ctx.client, persisted_shape, live: true)
      {:ok, initial_consumer} = Support.StreamConsumer.start(initial_stream)
      assert_up_to_date(initial_consumer)
      Support.StreamConsumer.stop(initial_consumer)

      shapes = ShapeCache.list_shapes(ctx.stack_id)

      {persisted_shape_handle, _shape} =
        Enum.find(shapes, fn {_handle, shape} ->
          shape.root_table == {"public", "artifacts"}
        end)

      dependency_handles =
        shapes
        |> Enum.map(&elem(&1, 0))
        |> Enum.reject(&(&1 == persisted_shape_handle))

      assert length(dependency_handles) == 2

      restart_complete_stack(ctx)

      assert Enum.all?(dependency_handles, fn handle ->
               is_nil(ConsumerRegistry.whereis(ctx.stack_id, handle))
             end)

      shape =
        ShapeDefinition.new!("artifacts",
          where: "(#{@message_artifact_where}) AND title != 'ignored'"
        )

      stream = Client.stream(ctx.client, shape, live: true)

      with_consumer stream do
        assert_up_to_date(consumer)

        assert length(ShapeCache.list_shapes(ctx.stack_id)) == 4

        assert Enum.all?(dependency_handles, fn handle ->
                 is_pid(ConsumerRegistry.whereis(ctx.stack_id, handle))
               end)

        Postgrex.query!(
          ctx.db_conn,
          "INSERT INTO artifacts (id, title) VALUES ('artifact-1', 'Revenue chart')",
          []
        )

        assert_up_to_date(consumer)

        Postgrex.transaction(ctx.db_conn, fn conn ->
          Postgrex.query!(
            conn,
            "INSERT INTO messages (id, chat_id) VALUES ('message-1', 'chat-1')",
            []
          )

          Postgrex.query!(
            conn,
            "INSERT INTO message_artifacts (message_id, artifact_id) VALUES ('message-1', 'artifact-1')",
            []
          )
        end)

        assert_insert(consumer, %{"id" => "artifact-1", "title" => "Revenue chart"})
      end
    end

    @tag replication_opts_overrides: [slot_temporary?: false]
    test "streams an existing artifact when a later link makes it eligible after restart", ctx do
      shape = ShapeDefinition.new!("artifacts", where: @message_artifact_where)

      initial_stream = Client.stream(ctx.client, shape, live: true)
      {:ok, initial_consumer} = Support.StreamConsumer.start(initial_stream)
      assert_up_to_date(initial_consumer)
      Support.StreamConsumer.stop(initial_consumer)

      restart_complete_stack(ctx)

      stream = Client.stream(ctx.client, shape, live: true)

      with_consumer stream do
        assert_up_to_date(consumer)

        Postgrex.query!(
          ctx.db_conn,
          "INSERT INTO messages (id, chat_id) VALUES ('message-1', 'chat-1')",
          []
        )

        Postgrex.query!(
          ctx.db_conn,
          "INSERT INTO artifacts (id, title) VALUES ('artifact-1', 'Revenue chart')",
          []
        )

        Postgrex.query!(
          ctx.db_conn,
          "INSERT INTO message_artifacts (message_id, artifact_id) VALUES ('message-1', 'artifact-1')",
          []
        )

        assert_insert(consumer, %{"id" => "artifact-1", "title" => "Revenue chart"})
      end
    end
  end

  # ---- Simple Parent/Child Schema for 1-level subquery tests ----

  def with_simple_parent_child_tables(%{db_conn: conn} = _context) do
    Postgrex.query!(
      conn,
      """
        CREATE TABLE parents (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          active BOOLEAN NOT NULL DEFAULT false
        )
      """,
      []
    )

    Postgrex.query!(
      conn,
      """
        CREATE TABLE children (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          parent_id TEXT NOT NULL REFERENCES parents(id) ON DELETE CASCADE,
          status TEXT NOT NULL DEFAULT 'draft'
        )
      """,
      []
    )

    %{tables: [{"public", "parents"}, {"public", "children"}]}
  end

  def with_message_artifact_tables(%{db_conn: conn} = _context) do
    Postgrex.query!(
      conn,
      """
        CREATE TABLE messages (
          id TEXT PRIMARY KEY,
          chat_id TEXT NOT NULL
        )
      """,
      []
    )

    Postgrex.query!(
      conn,
      """
        CREATE TABLE artifacts (
          id TEXT PRIMARY KEY,
          title TEXT NOT NULL
        )
      """,
      []
    )

    Postgrex.query!(
      conn,
      """
        CREATE TABLE message_artifacts (
          message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
          artifact_id TEXT NOT NULL REFERENCES artifacts(id) ON DELETE CASCADE,
          PRIMARY KEY (message_id, artifact_id)
        )
      """,
      []
    )

    %{
      tables: [
        {"public", "messages"},
        {"public", "artifacts"},
        {"public", "message_artifacts"}
      ]
    }
  end

  # ---- Helpers ----

  defp filter_deletes(messages) do
    messages
    |> Enum.filter(&match?(%ChangeMessage{headers: %{operation: :delete}}, &1))
    |> Enum.map(& &1.value)
  end

  defp collect_initial_inserts(consumer, expected_count) do
    {:ok, inserts} =
      await_count(consumer, expected_count,
        match: &match?(%ChangeMessage{headers: %{operation: :insert}}, &1)
      )

    Enum.map(inserts, & &1.value["id"])
  end

  # ---- Test Schema Setup ----

  def with_org_team_project_task_tables(%{db_conn: conn} = _context) do
    Postgrex.query!(
      conn,
      """
        CREATE TABLE organizations (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL
        )
      """,
      []
    )

    Postgrex.query!(
      conn,
      """
        CREATE TABLE organization_tags (
          org_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
          tag TEXT NOT NULL,
          PRIMARY KEY (org_id, tag)
        )
      """,
      []
    )

    Postgrex.query!(
      conn,
      """
        CREATE TABLE teams (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          org_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE
        )
      """,
      []
    )

    Postgrex.query!(
      conn,
      """
        CREATE TABLE projects (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          team_id TEXT NOT NULL REFERENCES teams(id) ON DELETE CASCADE
        )
      """,
      []
    )

    Postgrex.query!(
      conn,
      """
        CREATE TABLE tasks (
          id TEXT PRIMARY KEY,
          title TEXT NOT NULL,
          project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE
        )
      """,
      []
    )

    %{
      tables: [
        {"public", "organizations"},
        {"public", "organization_tags"},
        {"public", "teams"},
        {"public", "projects"},
        {"public", "tasks"}
      ]
    }
  end
end
