defmodule KinesisClient.Stream.AppState.Adapter do
  @moduledoc false

  alias KinesisClient.Stream.AppState.ShardLease

  @doc """
  Implement to setup any backend storage. Should not clear data as this will be called everytime a
  `KinesisClient.Stream.Coordinator` process is started.
  """
  @callback initialize(app_name :: String.t(), opts :: keyword) :: :ok | {:error, any}

  @callback get_lease(app_name :: String.t(), shard_id :: String.t(), opts :: keyword) ::
              ShardLease.t() | :not_found | {:error, any}

  @callback create_lease(
              app_name :: String.t(),
              shard_id :: String.t(),
              lease_owner :: String.t(),
              opts :: keyword
            ) ::
              :ok | :already_exists | {:error, any}

  @callback update_checkpoint(
              app_name :: String.t(),
              shard_id :: String.t(),
              lease_owner :: String.t(),
              checkpoint :: String.t(),
              opts :: keyword
            ) ::
              :ok | {:error, any}

  @callback renew_lease(app_name :: String.t(), shard_lease :: ShardLease.t(), opts :: keyword) ::
              {:ok, new_lease_count :: integer} | {:error, any}

  @callback take_lease(
              app_name :: String.t(),
              shard_id :: String.t(),
              new_owner :: String.t(),
              lease_count :: integer,
              opts :: keyword
            ) ::
              {:ok, new_lease_count :: integer} | {:error, any}

  @callback close_shard(
              app_name :: String.t(),
              shard_id :: String.t(),
              lease_owner :: String.t(),
              opts :: keyword
            ) ::
              :ok | {:error, any}
end
