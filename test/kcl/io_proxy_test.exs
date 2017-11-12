defmodule KCL.IOProxyTest do
  use ExUnit.Case
  alias KCL.IOProxy
  import TestHelper

  test "it should skip blank lines" do
    input_string = "    \nline1\n\n\n  \nline2\n   \n"
    io_streams = open_io(input_string)
    IOProxy.initialize(io_streams)
    assert IOProxy.read_line == "line1"
    assert IOProxy.read_line == "line2"
    assert IOProxy.read_line == nil
  end

  test "it should return nil on EOF" do
    input_string = "line1\n"
    io_streams = open_io(input_string)
    IOProxy.initialize(io_streams)
    assert IOProxy.read_line == "line1"
    assert IOProxy.read_line == nil
    assert IOProxy.read_line == nil
  end

  test "it should write an error message to the error stream" do
    io_streams = open_io("")
    IOProxy.initialize(io_streams)
    IOProxy.write_error("an error message")
    error_output = io_streams[:error] |> content |> String.strip
    assert error_output == "an error message"
  end

  test "it should write exception details to the error stream" do
    io_streams = open_io("")
    IOProxy.initialize(io_streams)
    try do
      raise "Test error"
    rescue
      e -> IOProxy.write_error(e)
    end
    error_output = io_streams[:error] |> content |> String.strip
    assert String.match?(error_output, ~r/RuntimeError.*Test error/)
  end

  test "it should write a valid JSON action to the output stream" do
    io_streams = open_io("")
    IOProxy.initialize(io_streams)
    IOProxy.write_action("status", %{"responseFor" => "initialize"})
    output_content = io_streams[:output] |> content |> String.strip
    assert output_content == ~s({"action":"status","responseFor":"initialize"})
  end
end
