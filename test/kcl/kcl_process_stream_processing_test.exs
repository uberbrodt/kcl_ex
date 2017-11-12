defmodule Kcl.KCLProcessStreamProcessingTest do
  use ExUnit.Case
  alias Kcl.KCLProcess
  alias Kcl.RecordProcessor
  alias Kcl.IOProxy
  import TestHelper

  defmodule TestRecordProcessor do
    @moduledoc """
      Uses default checkpointing behaviour of RecordProcessor module.
    """
    use RecordProcessor

    def process_records records do
        seq = records |> List.first |> Map.get "sequenceNumber"
        case checkpoint(seq) do
          :ok -> nil
          {:error, _} -> checkpoint(seq)
        end
    end
  end

  test "It should process a normal stream of actions and produce expected output" do
    input_string = """
    {"action":"initialize","shardId":"shardId-123"}
    {"action":"processRecords","records":[{"data":"bWVvdw==","partitionKey":"cat","sequenceNumber":"456"}]}
    {"action":"checkpoint","checkpoint":"456","error":"ThrottlingException"}
    {"action":"checkpoint","checkpoint":"456"}
    {"action":"shutdown","reason":"TERMINATE"}
    {"action":"checkpoint","checkpoint":"456"}
    """

    io = open_io input_string

    KCLProcess.run(TestRecordProcessor, io)

    # NOTE: The first checkpoint is expected to fail
    #       with a ThrottlingException and hence the
    #       retry.
    expected_output_string = """
    {"action":"status","responseFor":"initialize"}
    {"action":"checkpoint","checkpoint":"456"}
    {"action":"checkpoint","checkpoint":"456"}
    {"action":"status","responseFor":"processRecords"}
    {"action":"checkpoint","checkpoint":null}
    {"action":"status","responseFor":"shutdown"}
    """
    assert_io expected_output_string, io
  end
end
