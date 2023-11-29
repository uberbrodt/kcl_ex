defmodule KinesisClient.Stream do
  @moduledoc """
  This is the entry point for processing the shards of a Kinesis Data Stream.
  """
  use Supervisor
  require Logger
  import KinesisClient.Util
  alias KinesisClient.Stream.Coordinator

  @doc """
  Starts a `KinesisClient.Stream` process.

  ## Options
    * `:stream_name` - Required. The Kinesis Data Stream to process.
    * `:app_name` - Required.This should be a unique name across all your applications and the DynamodDB
      tablespace in your AWS region
    * `:name` - The process name. Defaults to `KinesisClient.Stream`.
    * `:max_demand` - The maximum number of records to retrieve from Kinesis. Defaults to 100.
    * `:aws_region` - AWS region. Will rely on ExAws defaults if not set.
    * `:shard_supervisor` - The child_spec for the Supervisor that monitors the ProcessingPipelines.
      Must implement the DynamicSupervisor behaviour.
    * `:lease_renew_interval`(optional) - How long (in milliseconds) a lease will be held before a renewal is attempted.
    * `:lease_expiry`(optional) - The lenght of time in milliseconds that least lasts for. If a
      lease is not renewed within this time frame, then that lease is considered expired and can be
      taken by another process.
  """
  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  def init(opts) do
    stream_name = get_stream_name(opts)
    app_name = get_app_name(opts)
    worker_ref = "worker-#{:rand.uniform(10_000)}"
    {shard_supervisor_spec, shard_supervisor_name} = get_shard_supervisor(opts)
    coordinator_name = get_coordinator_name(opts)
    shard_consumer = get_shard_consumer(opts)

    shard_args = [
      consumer_name: opts[:name],
      app_name: opts[:app_name],
      coordinator_name: coordinator_name,
      stream_name: stream_name,
      lease_owner: worker_ref,
      shard_consumer: shard_consumer,
      processors: opts[:processors],
      batchers: opts[:batchers]
    ]

    shard_args =
      shard_args
      |> optional_kw(:app_state_opts, Keyword.get(opts, :app_state_opts))
      |> optional_kw(:lease_renew_interval, Keyword.get(opts, :lease_renew_interval))
      |> optional_kw(:lease_expiry, Keyword.get(opts, :lease_expiry))

    coordinator_args = [
      name: coordinator_name,
      stream_name: stream_name,
      app_name: app_name,
      app_state_opts: Keyword.get(opts, :app_state_opts, []),
      shard_supervisor_name: shard_supervisor_name,
      worker_ref: worker_ref,
      shard_args: shard_args
    ]

    children = [
      shard_supervisor_spec,
      {Coordinator, coordinator_args}
    ]

    Logger.debug(
      "Starting KinesisClient.Stream: [app_name: #{app_name}, stream_name: {stream_name}]"
    )

    Supervisor.init(children, strategy: :one_for_all)
  end

  defp get_coordinator_name(opts) do
    case Keyword.get(opts, :shard_supervisor) do
      nil ->
        Module.concat([KinesisClient.Stream.Coordinator, opts[:app_name], opts[:stream_name]])

      # Shard processes may be running on nodes different from the Coordinator if passed
      # :shard_supervisor is distributed,so use :global to allow inter-node communication.
      _ ->
        {:global,
         Module.concat([KinesisClient.Stream.Coordinator, opts[:app_name], opts[:stream_name]])}
    end
  end

  defp get_stream_name(opts) do
    case Keyword.get(opts, :stream_name) do
      nil -> raise ArgumentError, message: "Missing required option :stream_name"
      x when is_binary(x) -> x
      _ -> raise ArgumentError, message: ":stream_name must be a binary"
    end
  end

  defp get_app_name(opts) do
    case Keyword.get(opts, :app_name) do
      nil -> raise ArgumentError, message: "Missing required option :app_name"
      x when is_binary(x) -> x
      _ -> raise ArgumentError, message: ":app_name must be a binary"
    end
  end

  @spec get_shard_supervisor(keyword) ::
          {{module, keyword}, name :: any} | no_return
  defp get_shard_supervisor(opts) do
    name =
      Module.concat([
        KinesisClient.Stream.ShardSupervisor,
        opts[:app_name],
        opts[:stream_name]
      ])

    {{DynamicSupervisor, [strategy: :one_for_one, name: name]}, name}
  end

  defp get_shard_consumer(opts) do
    case Keyword.get(opts, :shard_consumer) do
      nil ->
        raise ArgumentError,
          message:
            "Missing required option :shard_processor. Must be a module implementing the Broadway behaviour"

      x when is_atom(x) ->
        x

      _ ->
        raise ArgumentError, message: ":shard_processor option must be a module name"
    end
  end
end
