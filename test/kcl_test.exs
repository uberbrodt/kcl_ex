defmodule KCLTest do
  use ExUnit.Case
  doctest KCL

  test "greets the world" do
    assert KCL.hello() == :world
  end
end
