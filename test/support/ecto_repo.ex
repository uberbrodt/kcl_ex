defmodule KinesisClient.Ecto.Repo do
  alias Ecto.Changeset
  alias KinesisClient.Stream.AppState.Ecto.ShardLease

  def one(_query, _opts \\ []) do
    %ShardLease{
      shard_id: "a.b.c",
      checkpoint: nil,
      completed: false,
      lease_count: 1,
      lease_owner: "test_owner"
    }
  end

  def insert(changeset, opts \\ [])

  def insert(%Changeset{errors: [], changes: values}, _opts) do
    {:ok, struct(ShardLease, values)}
  end

  def insert(changeset, _opts) do
    {:error, changeset}
  end

  def update(changeset, opts \\ [])

  def update(%Changeset{errors: [], changes: values}, _opts) do
    {:ok, struct(ShardLease, values)}
  end

  def update(changeset, _opts) do
    {:error, changeset}
  end
end
