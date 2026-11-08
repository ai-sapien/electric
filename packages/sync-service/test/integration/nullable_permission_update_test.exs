defmodule Electric.Integration.NullablePermissionUpdateTest do
  use ExUnit.Case, async: false

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.IntegrationSetup
  import Support.StreamConsumer

  alias Electric.Client
  alias Electric.Client.Message.ChangeMessage
  alias Electric.Client.ShapeDefinition

  @moduletag :tmp_dir

  setup :with_unique_db

  setup %{db_conn: conn} do
    for statement <- [
          "CREATE TABLE permissions (revision INTEGER PRIMARY KEY)",
          "CREATE TABLE results (id TEXT PRIMARY KEY, revision INTEGER, viewer TEXT)",
          "INSERT INTO permissions VALUES (1)",
          "INSERT INTO results VALUES ('old', 1, NULL), ('replacement', 2, NULL), ('unauthorized', 3, NULL), ('direct', 3, 'viewer')"
        ] do
      Postgrex.query!(conn, statement, [])
    end

    :ok
  end

  setup :with_complete_stack
  setup :with_electric_client

  test "an existing HTTP stream receives the replacement when permission changes with a NULL alternative",
       ctx do
    shape =
      ShapeDefinition.new!("results",
        where: "revision IN (SELECT revision FROM permissions) OR viewer = 'viewer'"
      )

    stream = Client.stream(ctx.client, shape, live: true)

    with_consumer stream do
      assert {:ok, initial} = await_count(consumer, 2, match: &is_struct(&1, ChangeMessage))
      assert changes(initial) == [{:insert, "direct"}, {:insert, "old"}]
      assert_up_to_date(consumer)

      Postgrex.query!(ctx.db_conn, "UPDATE permissions SET revision = 2 WHERE revision = 1", [])

      assert {:ok, replacement} = await_count(consumer, 2, match: &is_struct(&1, ChangeMessage))
      assert changes(replacement) == [{:delete, "old"}, {:insert, "replacement"}]

      Postgrex.query!(ctx.db_conn, "DELETE FROM permissions WHERE revision = 2", [])

      assert {:ok, revoked} = await_count(consumer, 1, match: &is_struct(&1, ChangeMessage))
      assert changes(revoked) == [{:delete, "replacement"}]
    end
  end

  defp changes(messages) do
    messages
    |> Enum.map(fn %ChangeMessage{headers: %{operation: operation}, value: value} ->
      {operation, value["id"]}
    end)
    |> Enum.sort()
  end
end
