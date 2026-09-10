defmodule Electric.Postgres.IdentifiersTest do
  alias Electric.Postgres.Identifiers
  use ExUnit.Case, async: true
  doctest Identifiers, import: true

  describe "shorten/1" do
    test "preserves identifiers that fit PostgreSQL's byte limit" do
      identifier = String.duplicate("a", 63)

      assert Identifiers.shorten(identifier) == identifier
    end

    test "deterministically shortens overlong identifiers with a hash suffix" do
      identifier = "electric_publication_" <> String.duplicate("é", 30)

      shortened = Identifiers.shorten(identifier)

      assert byte_size(shortened) <= 63
      assert String.valid?(shortened)
      assert shortened == Identifiers.shorten(identifier)
      assert shortened =~ ~r/_[0-9a-f]{16}$/
    end

    test "keeps overlong identifiers with the same prefix distinct" do
      prefix = String.duplicate("a", 80)

      refute Identifiers.shorten(prefix <> "first") ==
               Identifiers.shorten(prefix <> "second")
    end
  end

  describe "validate_length/1" do
    test "measures the PostgreSQL limit in bytes" do
      assert :ok = Identifiers.validate_length(String.duplicate("é", 31))

      assert {:error, "Identifier is too long (max length is 63 bytes)"} =
               Identifiers.validate_length(String.duplicate("é", 32))
    end
  end
end
