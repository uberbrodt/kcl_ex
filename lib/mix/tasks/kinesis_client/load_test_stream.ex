defmodule Mix.Tasks.KinesisClient.LoadTestStream do
  @moduledoc "a task"
  use Mix.Task

  alias ExAws.Kinesis

  @stream_name "kcl-ex-test-stream"

  @impl Mix.Task
  def run(_args) do
    {:ok, _} = Application.ensure_all_started(:kinesis_client)
    test_data = File.read!(Path.join(:code.priv_dir(:kinesis_client), "test_data.json"))

    case @stream_name |> Kinesis.create_stream(4) |> ExAws.request() do
      {:ok, result} ->
        Mix.shell().info("Created stream #{@stream_name}: #{inspect(result)}")

      {:error, {:http_error, 400, %{"__type" => "ResourceNotFoundException"}}} ->
        Mix.shell().info("#{@stream_name} is already created")

      {:error, e} ->
        Mix.shell().error(inspect(e))
    end

    msgs =
      Enum.map(Jason.decode!(test_data), fn item ->
        %{data: Jason.encode!(item), partition_key: item["id"]}
      end)

    case @stream_name |> Kinesis.put_records(msgs) |> ExAws.request() do
      {:ok, output} -> Mix.shell().info("success!: #{inspect(output)}")
      {:error, err} -> Mix.shell().error(inspect(err))
    end
  end
end
