defmodule KinesisClient.Case do
  @moduledoc false
  use ExUnit.CaseTemplate

  import Mox

  using do
    quote do
      import KinesisClient.TestUtil
      import Mox

      alias KinesisClient.KinesisMock
      alias KinesisClient.Stream.AppStateMock
    end
  end

  setup :set_mox_from_context
  setup :verify_on_exit!
end
