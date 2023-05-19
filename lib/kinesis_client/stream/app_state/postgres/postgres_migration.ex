defmodule KinesisClient.Stream.AppState.Postgres.PostgresMigration do
  use Ecto.Migration

  def up do
    execute(
      "CREATE TABLE IF NOT EXISTS shard_lease (shard_id VARCHAR(255) PRIMARY KEY, checkpoint VARCHAR(255), lease_owner VARCHAR(255), lease_count INTEGER, completed BOOLEAN)"
    )

    create(unique_index(:shard_lease, [:shard_id]))
  end

  def down do
    execute("DROP TABLE IF EXISTS shard_lease")
  end
end
