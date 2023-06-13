defmodule KinesisClient.Stream.AppState.EctoTest do
  use ExUnit.Case

  alias KinesisClient.Ecto.Repo
  alias KinesisClient.Stream.AppState.Ecto

  test "creates a shard_lease" do
    assert Ecto.create_lease("", "a.b.c", "test_owner", repo: Repo) == :ok
  end

  test "gets a shard_lease" do
    shard_lease = Ecto.get_lease("", "a.b.c", repo: Repo)

    assert shard_lease.shard_id == "a.b.c"
    assert shard_lease.checkpoint == nil
    assert shard_lease.completed == false
    assert shard_lease.lease_count == 1
    assert shard_lease.lease_owner == "test_owner"
  end

  test "renews a shard_lease" do
    change = %{
      shard_id: "a.b.c",
      lease_owner: "test_owner",
      lease_count: 1
    }

    {:ok, lease_count} = Ecto.renew_lease("", change, repo: Repo)

    assert lease_count == 2
  end

  test "takes a shard_lease" do
    {:ok, lease_count} = Ecto.take_lease("", "a.b.c", "new_owner", 1, repo: Repo)

    assert lease_count == 2
  end

  test "returns error when taking a shard_lease" do
    {:error, error} = Ecto.take_lease("", "a.b.c", "test_owner", 1, repo: Repo)

    assert error == :lease_take_failed
  end

  test "updates shard_lease checkpoint" do
    assert Ecto.update_checkpoint("", "a.b.c", "test_owner", "checkpoint_1", repo: Repo) ==
             :ok
  end

  test "closes shard" do
    assert Ecto.close_shard("", "a.b.c", "test_owner", repo: Repo) == :ok
  end
end
