defmodule KinesisClient.Stream.Shard.Pipeline do
  @moduledoc false
  use Broadway
  import KinesisClient.Util
  alias KinesisClient.Stream.Shard.Producer

  def start_link(opts) do
    producer_opts = [
      consumer_name: opts[:consumer_name],
      app_name: opts[:app_name],
      shard_id: opts[:shard_id],
      lease_owner: opts[:lease_owner],
      stream_name: opts[:stream_name],
      kinesis_opts: Keyword.get(opts, :kinesis_opts, []),
      app_state_opts: Keyword.get(opts, :app_state_opts, []),
      poll_interval: Keyword.get(opts, :poll_interval, 5_000),
      coordinator_name: opts[:coordinator_name],
      status: :stopped
    ]

    min_demand = Keyword.get(opts, :min_demand, 10)
    max_demand = Keyword.get(opts, :max_demand, 20)
    batch_size = Keyword.get(opts, :batch_size, 20)

    processor_concurrency = Keyword.get(opts, :processor_concurrency, 1)
    batcher_concurrency = Keyword.get(opts, :batcher_concurrency, 1)

    processor_opts =
      Keyword.get(opts, :processors,
        default: [
          concurrency: processor_concurrency,
          min_demand: min_demand,
          max_demand: max_demand
        ]
      )

    batcher_opts =
      Keyword.get(opts, :batchers,
        default: [concurrency: batcher_concurrency, batch_size: batch_size]
      )

    # pipeline context must be a map
    pipeline_context =
      opts
      |> Keyword.get(:pipeline_context, %{})
      |> Map.put(:shard_consumer, opts[:shard_consumer])

    pipeline_opts = [
      name: name(opts[:app_name], opts[:shard_id]),
      producer: [
        module: {Producer, producer_opts},
        concurrency: 1
      ],
      context: pipeline_context,
      processors: processor_opts,
      batchers: batcher_opts
    ]

    pipeline_opts = optional_kw(pipeline_opts, :partition_by, Keyword.get(opts, :partition_by))

    Broadway.start_link(__MODULE__, pipeline_opts)
  end

  def start(app_name, shard_id) do
    names = Broadway.producer_names(name(app_name, shard_id))

    errors =
      Enum.reduce(names, [], fn name, errs ->
        case Producer.start(name) do
          :ok ->
            errs

          other ->
            [other | errs]
        end
      end)

    case errors do
      [] -> :ok
      errors -> errors
    end
  end

  def stop(app_name, shard_id) do
    names = Broadway.producer_names(name(app_name, shard_id))

    errors =
      Enum.reduce(names, [], fn name, errs ->
        case Producer.stop(name) do
          :ok ->
            errs

          other ->
            [other | errs]
        end
      end)

    case errors do
      [] -> :ok
      errors -> errors
    end
  end

  @impl Broadway
  def handle_message(processor, msg, ctx) do
    module = Map.get(ctx, :shard_consumer)
    module.handle_message(processor, msg, ctx)
  end

  @impl Broadway
  def handle_batch(batcher, messages, batch_info, context) do
    module = Map.get(context, :shard_consumer)

    module.handle_batch(batcher, messages, batch_info, context)
  end

  @impl Broadway
  def handle_failed(messages, context) do
    module = Map.get(context, :shard_consumer)

    module.handle_failed(messages, context)
  end

  def name(app_name, shard_id) do
    Module.concat([__MODULE__, app_name, shard_id])
  end
end
