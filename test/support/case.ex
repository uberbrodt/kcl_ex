defmodule KinesisClient.Case do
  @moduledoc false
  use ExUnit.CaseTemplate
  import Mox

  using do
    quote do
      alias KinesisClient.KinesisMock
      alias KinesisClient.Stream.AppStateMock
      import KinesisClient.TestUtil
      import Mox
    end
  end

  setup :set_mox_from_context
  setup :verify_on_exit!
end
