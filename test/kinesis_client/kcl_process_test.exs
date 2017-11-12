defmodule KinesisClient.KCLProcessTest do
  use ExUnit.Case
  alias KinesisClient.KCLProcess
  alias KinesisClient.RecordProcessor
  alias KinesisClient.IOProxy
  import TestHelper

  defmodule DoNothingRecordProcessor do
    @moduledoc """
      Overrides default RecordProcessor functions to prevent checkpointing
      to allow testing of basic message handling.
    """
    use RecordProcessor

    def process_records(data), do: nil
    def shutdown(args), do: nil
  end

  test "It should respond to init_processor and output a status message" do
        input_spec = %{
          :method => :init_processor,
          :action => "initialize",
          :input => ~s({"action":"initialize","shardId":"shard-000001"})
        }
        io = open_io(input_spec[:input])

        KCLProcess.run(DoNothingRecordProcessor, io)

        ~s({"action":"status","responseFor":"#{input_spec[:action]}"})
        |> assert_io io
  end

  test "It should respond to process_shards and output a status message" do
        input_spec = %{
          :method => :process_records,
          :action => "processRecords",
          :input => ~s({"action":"processRecords","records":[]})
        }
        io = open_io(input_spec[:input])

        KCLProcess.run(DoNothingRecordProcessor, io)

        ~s({"action":"status","responseFor":"#{input_spec[:action]}"})
        |> assert_io io
  end

  test "It should respond to shutdown and output a status message" do
        input_spec = %{
          :method => :shutdown,
          :action => "shutdown",
          :input => ~s({"action":"shutdown","reason":"TERMINATE"})
        }
        io = open_io(input_spec[:input])

        KCLProcess.run(DoNothingRecordProcessor, io)

        ~s({"action":"status","responseFor":"#{input_spec[:action]}"})
        |> assert_io io
  end
end
