defmodule KinesisClient.Stream.AppState.Postgres.ShardLeases do
  alias KinesisClient.Stream.AppState.Postgres.ShardLease

  @spec get_shard_lease(map, Ecto.Repo.t()) :: {:error, :not_found} | {:ok, ShardLease.t()}
  def get_shard_lease(params, repo) do
    ShardLease.query()
    |> ShardLease.build_get_query(params)
    |> repo.one()
    |> case do
      nil -> {:error, :not_found}
      shard_lease -> {:ok, shard_lease}
    end
  end

  @spec get_shard_lease_by_id(String.t(), Ecto.Repo.t()) ::
          {:error, :not_found} | {:ok, ShardLease.t()}
  def get_shard_lease_by_id(shard_id, repo) do
    %{shard_id: shard_id}
    |> get_shard_lease(repo)
  end

  @spec update_shard_lease(ShardLease.t(), Ecto.Repo.t(), list) ::
          {:error, Ecto.Changeset.t()} | {:ok, ShardLease.t()}
  def update_shard_lease(shard_lease, repo, change \\ []) do
    shard_lease_changeset = Ecto.Changeset.change(shard_lease, change)

    case repo.update(shard_lease_changeset) do
      {:ok, shard_lease} -> {:ok, shard_lease}
      {:error, changeset} -> {:error, changeset}
    end
  end
end
