defmodule KinesisClient.Stream.AppState.Ecto.ShardLeasesTest do
  use ExUnit.Case

  alias KinesisClient.Ecto.Repo
  alias KinesisClient.Stream.AppState.Ecto.ShardLeases

  test "get_shard_lease/2" do
    params = %{
      shard_id: "a.b.c"
    }

    {:ok, shard_lease} = ShardLeases.get_shard_lease(params, Repo)

    assert shard_lease.shard_id == "a.b.c"
    assert shard_lease.checkpoint == nil
    assert shard_lease.completed == false
    assert shard_lease.lease_count == 1
    assert shard_lease.lease_owner == "test_owner"
  end

  test "get_shard_lease_by_id/2" do
    {:ok, shard_lease} = ShardLeases.get_shard_lease_by_id("a.b.c", Repo)

    assert shard_lease.shard_id == "a.b.c"
    assert shard_lease.checkpoint == nil
    assert shard_lease.completed == false
    assert shard_lease.lease_count == 1
    assert shard_lease.lease_owner == "test_owner"
  end

  describe "insert_shard_lease/2" do
    test "returns a new shard_lease" do
      attrs = %{
        shard_id: "a.b.c",
        completed: false,
        lease_count: 1,
        lease_owner: "test_owner"
      }

      {:ok, shard_lease} = ShardLeases.insert_shard_lease(attrs, Repo)

      assert shard_lease.shard_id == "a.b.c"
      assert shard_lease.checkpoint == nil
      assert shard_lease.completed == false
      assert shard_lease.lease_count == 1
      assert shard_lease.lease_owner == "test_owner"
    end

    test "returns changeset error when a param is invalid" do
      attrs = %{
        shard_id: "a.b.c",
        completed: false,
        lease_count: "INVALID",
        lease_owner: "test_owner"
      }

      {:error, changeset} = ShardLeases.insert_shard_lease(attrs, Repo)

      expected_error = {"is invalid", [type: :integer, validation: :cast]}
      assert %Ecto.Changeset{errors: [lease_count: ^expected_error]} = changeset
    end
  end

  test "update_shard_lease/3" do
    {:ok, shard_lease} = ShardLeases.get_shard_lease_by_id("a.b.c", Repo)

    assert shard_lease.completed == false

    {:ok, updated_shard_lease} = ShardLeases.update_shard_lease(shard_lease, Repo, completed: true)

    assert updated_shard_lease.completed == true
  end
end
