defmodule KinesisClient.Stream.AppState.Ecto.ShardLease do
  use Ecto.Schema

  import Ecto.Changeset
  import Ecto.Query

  @fields [:shard_id, :lease_owner, :lease_count, :checkpoint, :completed]

  @primary_key {:shard_id, :string, autogenerate: false}
  schema "shard_lease" do
    field(:lease_owner, :string)
    field(:lease_count, :integer)
    field(:checkpoint, :string)
    field(:completed, :boolean)
  end

  def changeset(shard_lease, attrs) do
    shard_lease
    |> cast(attrs, @fields)
    |> unique_constraint(:shard_id, name: :shard_lease_pkey)
  end

  def query() do
    from(sl in __MODULE__)
  end

  def build_get_query(query, params) do
    Enum.reduce(params, query, &query_by(&1, &2))
  end

  defp query_by({:shard_id, shard_id}, query) do
    query
    |> where([sl], sl.shard_id == ^shard_id)
  end

  defp query_by({:lease_owner, lease_owner}, query) do
    query
    |> where([sl], sl.lease_owner == ^lease_owner)
  end

  defp query_by({:lease_count, lease_count}, query) do
    query
    |> where([sl], sl.lease_count == ^lease_count)
  end
end
