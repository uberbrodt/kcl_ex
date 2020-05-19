defmodule KinesisClient.Stream.Shard.PipelineTest do
  use KinesisClient.Case

  alias KinesisClient.Stream.Shard.Pipeline
  alias KinesisClient.Stream.AppState.ShardLease

  test "can start producer" do
    app_name = "sdf9023kl"
    shard_id = "shard-1"

    opts = [
      app_name: app_name,
      app_state_opts: [adapter: AppStateMock],
      shard_id: shard_id,
      kinesis_opts: [adapter: KinesisMock],
      shard_consumer: KinesisClient.TestShardConsumer
    ]

    KinesisMock
    |> expect(:get_shard_iterator, fn in_stream_name, in_shard_id, in_shard_iterator_type, _ ->
      {:ok, %{shard_iterator: "foo"}}
    end)
    |> stub(:get_records, fn iterator, _opts ->
      {:ok,
       %{
         next_shard_iterator: "bar",
         millis_behind_latest: 100,
         records: [%{data: "", partition_key: "3qwc3", sequence_number: "12345"}]
       }}
    end)

    expect(AppStateMock, :get_lease, fn _, _, _ ->
      %ShardLease{checkpoint: nil}
    end)

    {:ok, pid} = start_supervised({Pipeline, opts})

    assert Process.alive?(pid)

    assert :ok == Pipeline.start(app_name, shard_id)
  end

  test "can stop producer" do
    app_name = "sdf9023kl"
    shard_id = "shard-1"

    opts = [app_name: app_name, shard_id: shard_id]

    {:ok, pid} = start_supervised({Pipeline, opts})

    assert Process.alive?(pid)

    assert :ok == Pipeline.stop(app_name, shard_id)
  end
end
