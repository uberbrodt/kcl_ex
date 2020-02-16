defmodule KinesisClient.Stream.CoordinatorTest do
  use KinesisClient.Case, async: false

  alias KinesisClient.Stream.Coordinator
  @stream_name "decline-roman-empire-test"
  @shard_count 6
  @supervisor_name MyShardSupervisor

  setup_all do
    KinesisClient.TestStream.create_stream(@stream_name, @shard_count)
    :ok
  end

  test "describes kinesis stream and starts shards" do
    {:ok, _} =
      start_supervised({DynamicSupervisor, [strategy: :one_for_one, name: @supervisor_name]})

    opts = coordinator_opts()

    expect(KinesisMock, :describe_stream, fn stream_name, _opts ->
      assert stream_name == @stream_name
      {:ok, KinesisClient.KinesisResponses.describe_stream()}
    end)

    {:ok, _} = start_supervised({Coordinator, opts})

    assert_receive {:shards, shards}, 5_000
    assert_receive {:shard_started, %{pid: pid}}, 5_000
    assert Process.alive?(pid) == true

    assert Enum.empty?(shards) == false
  end

  test "do multiple fetches of the stream description to get a full list of shards" do
    {:ok, _} =
      start_supervised({DynamicSupervisor, [strategy: :one_for_one, name: @supervisor_name]})

    opts = coordinator_opts()

    KinesisMock
    |> expect(:describe_stream, fn stream_name, _opts ->
      assert stream_name == @stream_name
      {:ok, KinesisClient.KinesisResponses.describe_stream(has_more_shards: true)}
    end)
    |> expect(:describe_stream, fn stream_name, _opts ->
      assert stream_name == @stream_name
      {:ok, KinesisClient.KinesisResponses.describe_stream(has_more_shards: false)}
    end)

    {:ok, _} = start_supervised({Coordinator, opts})

    assert_receive {:shards, shards}, 5_000
    assert_receive {:shard_started, %{pid: pid}}, 5_000
    assert Process.alive?(pid) == true

    assert Enum.empty?(shards) == false
  end

  test "handles split shards correctly" do
    {:ok, _} =
      start_supervised({DynamicSupervisor, [strategy: :one_for_one, name: @supervisor_name]})

    opts = coordinator_opts()

    stub(KinesisMock, :describe_stream, fn stream_name, _opts ->
      assert stream_name == @stream_name
      {:ok, KinesisClient.KinesisResponses.describe_stream_split()}
    end)

    {:ok, _} = start_supervised({Coordinator, opts})

    assert_receive {:shards, shards}, 5_000
    assert_receive {:shard_started, %{pid: pid, shard_id: "shardId-000000000000"}}, 5_000
    assert Process.alive?(pid) == true
    assert_receive {:shard_started, %{pid: pid, shard_id: "shardId-000000000001"}}, 5_000
    assert Process.alive?(pid) == true

    assert Enum.empty?(shards) == false
  end

  test "will retry initialization after :retry_timeout if stream status not ACTIVE" do
    {:ok, _} =
      start_supervised({DynamicSupervisor, [strategy: :one_for_one, name: @supervisor_name]})

    opts = coordinator_opts(retry_timeout: 100)

    expect(KinesisMock, :describe_stream, fn stream_name, _opts ->
      assert stream_name == @stream_name
      {:ok, KinesisClient.KinesisResponses.describe_stream(stream_status: "UPDATING")}
    end)

    {:ok, coordinator} = start_supervised({Coordinator, opts})
    assert_receive {:retrying_describe_stream, pid}, 5_000

    assert coordinator == pid
  end

  defp coordinator_opts(overrides \\ []) do
    coordinator_name = MyTestCoordinator
    app_name = "uberbrodt-kinesis-client-test-app"

    opts = [
      name: coordinator_name,
      app_name: app_name,
      stream_name: @stream_name,
      shard_supervisor_name: @supervisor_name,
      notify_pid: self(),
      kinesis_opts: [adapter: KinesisClient.KinesisMock],
      shard_args: [
        app_name: app_name,
        coordinator_name: coordinator_name,
        lease_owner: worker_ref(),
        app_state_opts: [adapter: AppStateMock]
      ]
    ]

    Keyword.merge(opts, overrides)
  end
end
