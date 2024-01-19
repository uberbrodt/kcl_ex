defmodule KinesisClient.Stream.Shard do
  @moduledoc """
  This module starts up lease management for a given shard and starts a pipeline to process
  records from the shard, once the lease is acquired.

  Some consideration could be made for combining the lease management and pipeline into a single
  process so that state management is more centralized.
  """

  use Supervisor, restart: :transient
  alias KinesisClient.Stream.Shard.{Lease, Pipeline}
  import KinesisClient.Util

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: args[:shard_name])
  end

  def init(opts) do
    lease_opts = [
      app_name: opts[:app_name],
      shard_id: opts[:shard_id],
      lease_owner: opts[:lease_owner]
    ]

    pipeline_opts = [
      consumer_name: opts[:consumer_name],
      app_name: opts[:app_name],
      shard_id: opts[:shard_id],
      lease_owner: opts[:lease_owner],
      stream_name: opts[:stream_name],
      shard_consumer: opts[:shard_consumer],
      processors: opts[:processors],
      batchers: opts[:batchers],
      coordinator_name: opts[:coordinator_name]
    ]

    lease_opts =
      lease_opts
      |> optional_kw(:app_state_opts, Keyword.get(opts, :app_state_opts))
      |> optional_kw(:renew_interval, Keyword.get(opts, :lease_renew_interval))
      |> optional_kw(:lease_expiry, Keyword.get(opts, :lease_expiry))

    children = [
      {Lease, lease_opts},
      {Pipeline, pipeline_opts}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  def start(supervisor, shard_info) do
    DynamicSupervisor.start_child(supervisor, {__MODULE__, shard_info})
  end

  def stop(shard) do
    Supervisor.stop(shard, :normal)
  end

  def name(app_name, stream_name, shard_id) do
    Module.concat([__MODULE__, app_name, stream_name, shard_id])
  end
end
