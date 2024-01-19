defmodule ReplTest do
  def start do
    opts = [
      stream_name: "kcl-ex-test-stream",
      app_name: "cbrodt-test-app",
      shard_consumer: ReplTestShardConsumer,
      processors: [
        default: [
          concurrency: 1,
          min_demand: 10,
          max_demand: 20
        ]
      ],
      batchers: [
        default: [
          concurrency: 1,
          batch_size: 40
        ]
      ]
    ]

    KinesisClient.Stream.start_link(opts)
  end

  def scan_table(table_name \\ "cbrodt-test-app") do
    ExAws.Dynamo.scan(table_name, limit: 20) |> ExAws.request()
  end

  def use_localstack_services do
    Application.put_env(:ex_aws, :dynamodb,
      scheme: "http://",
      host: "localhost",
      port: "4566",
      region: "us-east-1"
    )

    Application.put_env(:ex_aws, :kinesis,
      scheme: "http://",
      host: "localhost",
      port: "4566",
      region: "us-east-1"
    )
  end
end

defmodule ReplTestShardConsumer do
  @behaviour Broadway
  require Logger

  @impl Broadway
  def handle_message(_processor, %{data: data, metadata: metadata} = msg, _context) do
    %{id: id} = data |> Base.decode64!() |> Jason.decode!(keys: :atoms)
    Logger.info("Got message #{id}")
    path = Path.join("repl_output", "#{id}.json")
    :ok = File.write(path, Jason.encode!(%{data: data, metadata: metadata}))
    Logger.info("processing message with id: #{id}")
    Broadway.Message.put_batcher(msg, :default)
  end

  @impl Broadway
  def handle_batch(_batcher, messages, _batch_info, _context) do
    Logger.info("Got batch of size: #{length(messages)}")
    messages
  end

  @impl Broadway
  def handle_failed(messages, _context) do
    messages
  end
end

defmodule CBDevTesting do
  alias ExAws.Kinesis
  @coordinator_name MyTestCoordinator

  def update_shard_count(stream_name, shard_count) do
    operation = %ExAws.Operation.JSON{
      http_method: :post,
      headers: [
        {"x-amz-target", "Kinesis_20131202.UpdateShardCount"},
        {"content-type", "application/x-amz-json-1.1"}
      ],
      path: "/",
      data: %{
        ScalingType: "UNIFORM_SCALING",
        StreamName: stream_name,
        TargetShardCount: shard_count
      },
      service: :kinesis
    }

    ExAws.request(operation)
  end

  def describe_stream(stream_name) do
    Kinesis.describe_stream(stream_name) |> ExAws.request()
  end

  def describe_table(table_name) do
    ExAws.Dynamo.describe_table(table_name) |> ExAws.request()
  end

  def list_streams do
    Kinesis.list_streams() |> ExAws.request()
  end

  def create_stream(stream_name, shard_count) do
    stream_name |> Kinesis.create_stream(shard_count) |> ExAws.request()
  end

  def stream_name do
    "decline-roman-empire-dev"
  end

  def start_coordinator(_overrides) do
    opts = [
      name: @coordinator_name,
      app_name: "uberbrodt-kinesis-client-dev-app",
      stream_name: stream_name(),
      shard_supervisor_name: MyShardSupervisor,
      notify_pid: self()
    ]

    KinesisClient.Stream.Coordinator.start_link(opts)
  end

  def list_child_to_parent() do
    g = get_coordinator_graph()

    n = g |> :digraph.vertices() |> Enum.map(fn v -> {v, :digraph.in_neighbours(g, v)} end)

    Enum.sort(n, fn {_, n1}, {_, n2} -> length(n1) <= length(n2) end)
  end

  def list_parent_to_child() do
    g = get_coordinator_graph()

    n = g |> :digraph.vertices() |> Enum.map(fn v -> {v, :digraph.out_neighbours(g, v)} end)

    Enum.sort(n, fn {_, n1}, {_, n2} -> length(n1) >= length(n2) end)
  end

  def get_coordinator_graph() do
    GenServer.call(@coordinator_name, :get_graph)
  end
end
