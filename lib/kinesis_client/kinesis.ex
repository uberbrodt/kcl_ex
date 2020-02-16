defmodule KinesisClient.Kinesis do
  @moduledoc """
  Behaviour that Kinesis client needs to support
  """

  def get_shard_iterator(stream_name, shard_id, shard_iterator_type, opts \\ []) do
    client(opts).get_shard_iterator(stream_name, shard_id, shard_iterator_type, opts)
  end

  def describe_stream(stream_name, opts \\ []) do
    client(opts).describe_stream(stream_name, opts)
  end

  def get_records(shard_iterator, opts \\ []) do
    client(opts).get_records(shard_iterator, opts)
  end

  defp client(opts) do
    Keyword.get(opts, :adapter, KinesisClient.Kinesis.AwsAdapter)
  end
end

defmodule KinesisClient.Kinesis.Adapter do
  @moduledoc false
  @type iterator_type :: :at_sequence_number | :after_sequence_number | :trim_horizon | :latest

  @callback get_shard_iterator(
              stream_name :: binary,
              shard_id :: binary,
              shard_iterator_type :: iterator_type,
              opts :: keyword
            ) :: {:ok, map}

  @callback describe_stream(stream_name :: binary, opts :: keyword) :: {:ok, map}

  @callback get_records(shard_iterator :: String.t(), opts :: keyword) :: {:ok, map}
end

defmodule KinesisClient.Kinesis.AwsAdapter do
  @moduledoc false
  alias ExAws.Kinesis
  alias KinesisClient.Kinesis.Adapter
  @behaviour Adapter

  @impl Adapter
  def describe_stream(stream_name, opts) do
    case Kinesis.describe_stream(stream_name, opts) |> ExAws.request() do
      {:ok, _} = reply -> reply
      error -> error
    end
  end

  @impl Adapter
  def get_shard_iterator(stream_name, shard_id, shard_iterator_type, opts) do
    request = Kinesis.get_shard_iterator(stream_name, shard_id, shard_iterator_type, opts)

    case request |> ExAws.request() do
      {:ok, _} = reply -> reply
      error -> error
    end
  end

  @impl Adapter
  def get_records(shard_iterator, opts) do
    case Kinesis.get_records(shard_iterator, opts) |> ExAws.request() do
      {:ok, _} = r -> r
      {:error, _} = e -> e
    end
  end
end
