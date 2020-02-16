defmodule KinesisClient.Stream.Shard.ProducerTest do
  use KinesisClient.Case

  alias KinesisClient.Stream.Shard.Producer

  test "returns messages in response to demand if status is not :stopped" do
    opts = producer_opts(status: :started)
    {:ok, producer} = start_supervised({Producer, opts})
    {:ok, consumer} = start_supervised({KinesisClient.TestConsumer, self()})

    KinesisMock
    |> expect(:get_shard_iterator, fn _, _, _, _ ->
      {:ok, %{shard_iterator: "somesharditerator"}}
    end)
    |> expect(:get_records, fn _, _ ->
      records = [
        %{data: "foo"}
      ]

      {:ok, %{next_shard_iterator: "foo", millis_behind_latest: 5_000, records: records}}
    end)

    GenStage.sync_subscribe(consumer, to: producer)
    assert_receive {:consumer_events, [record]}, 5_000

    assert record[:data] == "foo"
  end

  test "stores demand if :status == :stopped" do
    opts = producer_opts()
    {:ok, producer} = start_supervised({Producer, opts})
    {:ok, consumer} = start_supervised({KinesisClient.TestConsumer, self()})
    GenStage.sync_subscribe(consumer, to: producer)

    assert Process.alive?(producer)
    assert_receive {:queuing_demand_while_stopped, _}, 5_000
    refute_receive {:consumer_events, _}, 5_000
  end

  test "stores partial demand if cannot totally fulfill consumer request" do
    opts = producer_opts(status: :started)
    {:ok, producer} = start_supervised({Producer, opts})
    {:ok, consumer} = start_supervised({KinesisClient.TestConsumer, self()})

    KinesisMock
    |> expect(:get_shard_iterator, fn _, _, _, _ ->
      {:ok, %{shard_iterator: "somesharditerator"}}
    end)
    |> expect(:get_records, fn _, opts ->
      count = opts[:limit] - 5
      records = Enum.map(0..count, fn _ -> %{data: "foo"} end)

      {:ok, %{next_shard_iterator: "foo", millis_behind_latest: 5_000, records: records}}
    end)

    GenStage.sync_subscribe(consumer, to: producer, max_demand: 10, min_demand: 0)
    assert_receive {:consumer_events, _}, 5_000
    assert_receive :poll_timer_executed, 10_000
  end

  defp producer_opts(overrides \\ []) do
    opts = [
      shard_id: "shardId-000000000000",
      stream_name: "kcl-ex-test-stream",
      kinesis_opts: [adapter: KinesisMock],
      status: :stopped,
      notify_pid: self()
    ]

    Keyword.merge(opts, overrides)
  end
end

defmodule KinesisClient.TestConsumer do
  use GenStage

  def start_link(notify_pid) do
    GenStage.start_link(__MODULE__, notify_pid)
  end

  def init(notify_pid) do
    {:consumer, %{notify_pid: notify_pid}}
  end

  def handle_events(events, _, %{notify_pid: pid} = state) do
    send(pid, {:consumer_events, events})

    {:noreply, [], state}
  end
end
