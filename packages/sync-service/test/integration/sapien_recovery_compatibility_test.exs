defmodule Electric.Integration.UpstreamRecoveryCompatibilityTest do
  @moduledoc """
  Checks permission-shaped subqueries against Postgres across server restarts.
  Existing client processes retain their handles and recover through the normal
  must-refetch protocol. Both revoked and newly authorized rows must converge
  without an application reload, including after nested changes and a backlog.
  """

  use ExUnit.Case, async: false

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.IntegrationSetup
  alias Support.OracleHarness

  @moduletag :oracle
  @moduletag timeout: :infinity
  @moduletag :tmp_dir

  setup [:with_unique_db]
  setup :use_persistent_slot
  setup :with_complete_stack

  setup ctx do
    ctx =
      with_electric_client(ctx,
        router_opts: [long_poll_timeout: 5000],
        num_clients: 1
      )

    setup_issue_tracker_schema(ctx)
    ctx
  end

  # See `oracle_property_test.exs`: the StackSupervisor restart needs the
  # replication slot to persist so Electric reconnects rather than treating
  # a new slot as a slot-loss event and purging on-disk shape data.
  defp use_persistent_slot(_ctx) do
    %{replication_opts_overrides: [slot_temporary?: false]}
  end

  # A three-table "issue tracker": teams own projects, which own issues.
  # `projects.active` drives the existing subquery shapes, while `teams.active`
  # drives the nested-subquery regression below. Issues are spread round-robin
  # across the projects so each project owns four (e.g. p1 owns i1, i6, i11,
  # i16).
  defp setup_issue_tracker_schema(ctx) do
    OracleHarness.apply_sql(ctx, [
      "DROP TABLE IF EXISTS issues CASCADE",
      "DROP TABLE IF EXISTS projects CASCADE",
      "DROP TABLE IF EXISTS teams CASCADE",
      """
      CREATE TABLE teams (
        id TEXT PRIMARY KEY,
        active BOOLEAN NOT NULL DEFAULT true
      )
      """,
      """
      CREATE TABLE projects (
        id TEXT PRIMARY KEY,
        team_id TEXT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
        active BOOLEAN NOT NULL DEFAULT true
      )
      """,
      """
      CREATE TABLE issues (
        id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
        title TEXT NOT NULL
      )
      """
    ])

    team_values = "('t1', true), ('t2', false)"
    project_ids = for n <- 1..5, do: "p#{n}"

    project_values =
      project_ids
      |> Enum.with_index()
      |> Enum.map_join(", ", fn {id, idx} ->
        team_id = if rem(idx, 2) == 0, do: "t1", else: "t2"
        "('#{id}', '#{team_id}', #{rem(idx, 2) == 0})"
      end)

    issue_values =
      for n <- 1..20 do
        project_id = Enum.at(project_ids, rem(n - 1, length(project_ids)))
        "('i#{n}', '#{project_id}', 'Issue #{n}')"
      end
      |> Enum.join(", ")

    OracleHarness.apply_sql(ctx, [
      "INSERT INTO teams (id, active) VALUES #{team_values}",
      "INSERT INTO projects (id, team_id, active) VALUES #{project_values}",
      "INSERT INTO issues (id, project_id, title) VALUES #{issue_values}"
    ])

    :ok
  end

  @tag :sapien_recovery_simple
  test "subquery client recovers permission changes across server restart", ctx do
    shapes = [
      %{
        name: "issues_of_active_projects",
        table: "issues",
        where: "project_id IN (SELECT id FROM projects WHERE active = true)",
        columns: ["id", "project_id", "title"],
        pk: ["id"],
        optimized: false
      }
    ]

    # Two batches with one mutation each. Restart fires after batch_1.
    # The mutations move rows in/out of the shape because they flip the
    # parent project's `active` flag — so the subquery's result set changes,
    # and the materializer is the component responsible for routing the
    # corresponding issue rows in or out.
    batches = [
      [
        [%{name: "deactivate_p1", sql: "UPDATE projects SET active = false WHERE id = 'p1'"}]
      ],
      [
        [%{name: "reactivate_p1", sql: "UPDATE projects SET active = true WHERE id = 'p1'"}]
      ]
    ]

    OracleHarness.test_against_oracle(ctx, shapes, batches,
      restart_server_every: 1,
      preserve_client_state_on_restart: true
    )
  end

  @tag :sapien_recovery_nested
  test "nested permission changes converge across repeated server restarts", ctx do
    shapes = [
      %{
        name: "issues_of_projects_in_active_teams",
        table: "issues",
        where:
          "project_id IN (SELECT id FROM projects WHERE team_id IN " <>
            "(SELECT id FROM teams WHERE active = true))",
        columns: ["id", "project_id", "title"],
        pk: ["id"],
        optimized: false
      }
    ]

    # Only dependencies change; retained clients must reflect revocation and
    # reauthorization after every restart, even when the server rotates handles.
    batches = [
      [[%{name: "deactivate_t1", sql: "UPDATE teams SET active = false WHERE id = 't1'"}]],
      [[%{name: "reactivate_t1", sql: "UPDATE teams SET active = true WHERE id = 't1'"}]],
      [[%{name: "deactivate_t1_again", sql: "UPDATE teams SET active = false WHERE id = 't1'"}]],
      [[%{name: "reactivate_t1_again", sql: "UPDATE teams SET active = true WHERE id = 't1'"}]]
    ]

    OracleHarness.test_against_oracle(ctx, shapes, batches,
      restart_server_every: 1,
      preserve_client_state_on_restart: true
    )
  end

  @tag :sapien_recovery_backlog
  @tag chunk_bytes_threshold: 200
  @tag stack_restart_timeout_ms: 30_000
  test "clients recover after a dependency backlog and accept later permission changes",
       ctx do
    shapes = [
      %{
        name: "issues_of_active_projects",
        table: "issues",
        where: "project_id IN (SELECT id FROM projects WHERE active = true)",
        columns: ["id", "project_id", "title"],
        pk: ["id"],
        optimized: false
      },
      %{
        name: "issues_of_inactive_projects",
        table: "issues",
        where: "project_id IN (SELECT id FROM projects WHERE active = false)",
        columns: ["id", "project_id", "title"],
        pk: ["id"],
        optimized: false
      }
    ]

    # Generate many persisted chunks before restart, then check both mutually
    # exclusive permission views against the database using the same clients.
    toggles =
      Enum.flat_map(1..100, fn _ ->
        [
          [%{name: "deactivate_p5", sql: "UPDATE projects SET active = false WHERE id = 'p5'"}],
          [%{name: "reactivate_p5", sql: "UPDATE projects SET active = true WHERE id = 'p5'"}]
        ]
      end)

    batch_2 = [
      [%{name: "deactivate_p3", sql: "UPDATE projects SET active = false WHERE id = 'p3'"}]
    ]

    batches = [toggles, batch_2]

    try do
      OracleHarness.test_against_oracle(ctx, shapes, batches,
        restart_server_every: 1,
        preserve_client_state_on_restart: true
      )
    after
      # This case leaves a large persistent-slot replay behind. Stop the
      # restarted stack before after-suite cleanup attempts to drop its test DB.
      stop_supervised(Electric.StackSupervisor)
    end
  end
end
