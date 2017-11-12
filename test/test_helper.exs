ExUnit.start()
 
defmodule TestHelper do
  use ExUnit.Case
  def open_io, do: open_io("")
  def open_io input_content do
    {:ok, input} = StringIO.open(input_content)
    {:ok, output} = StringIO.open ""
    {:ok, error} = StringIO.open ""
    [input: input, output: output, error: error]
  end

  def content stringio do
    {_, content} = StringIO.contents(stringio)
    content
  end

  def assert_io expected_output, [input: input, output: output, error: error] do
    assert clean(content(output)) == clean(expected_output)
    assert content(error) == ""
    assert IO.read(input, 1) == :eof
  end

  def clean string do
    String.replace string, ~r/\s+/, ""
  end
end
