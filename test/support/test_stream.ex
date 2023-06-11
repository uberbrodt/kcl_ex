defmodule KinesisClient.TestStream do
  @moduledoc false
  alias ExAws.Kinesis

  def describe_stream(stream_name) do
  end

  def create_stream(stream_name, shard_count) do
    case stream_name |> Kinesis.describe_stream() |> ExAws.request() do
      {:ok, _} = x -> x
      _ -> stream_name |> Kinesis.create_stream(shard_count) |> ExAws.request()
    end
  end
end
