defmodule KinesisClient.StreamTest do
  use KinesisClient.Case

  alias KinesisClient.Stream

  test "start_link/1 fails if :stream_name opt is missing" do
    {:error, e} = start_supervised({Stream, []})
    error = e |> elem(0) |> elem(0)
    assert error == %ArgumentError{message: "Missing required option :stream_name"}
  end

  test "start_link/1 fails if :stream_name is not a binary" do
    {:error, e} = start_supervised({Stream, [stream_name: :foo]})
    error = e |> elem(0) |> elem(0)
    assert error == %ArgumentError{message: ":stream_name must be a binary"}
  end

  test "start_link/1 fails if :app_name opt is missing" do
    stream_name = "foo_stream"

    {:error, e} = start_supervised({Stream, [stream_name: stream_name]})
    error = e |> elem(0) |> elem(0)
    assert error == %ArgumentError{message: "Missing required option :app_name"}
  end

  test "start_link/1 fails if :app_name is not a binary" do
    {:error, e} = start_supervised({Stream, [stream_name: "foo", app_name: :foo_app]})
    error = e |> elem(0) |> elem(0)
    assert error == %ArgumentError{message: ":app_name must be a binary"}
  end

  test "start_link/1 succeeds" do
    stream_name = "foo_stream"
    app_name = "foo"

    opts = [
      stream_name: stream_name,
      app_name: app_name,
      shard_consumer: KinesisClient.TestShardConsumer
    ]

    {:ok, pid} = start_supervised({Stream, opts})
    assert Process.alive?(pid)
  end
end
