defmodule Kcl.IOProxy do
  @moduledoc false
  def initialize io_streams = [input: _, output: _, error: _] do
    Agent.start_link(fn -> io_streams end, name: __MODULE__)
  end

  def read_line do
    do_read io_streams()[:input]
  end

  def write_action(action_name, properties = %{}) do
    data = Enum.into(properties, [])
    {:ok, json} = data |> Keyword.put(:action, action_name) |> JSX.encode
    IO.puts(io_streams()[:output], json)
  end

  def write_error(message) do
    IO.puts(io_streams()[:error], error_string(message))
  end

  defp error_string(message) when is_binary(message), do: message
  defp error_string(message), do: inspect message

  defp io_streams do
    Agent.get(__MODULE__, &(&1))
  end

  defp do_read input do
    line = IO.read(input, :line)
    case stripped = strip(line) do
      ""   -> do_read input
      :eof -> nil
      _    -> stripped
    end
  end

  defp strip(line) when is_binary(line) do
    String.replace line, ~r/\s/, ""
  end
  defp strip(line), do: line
end
