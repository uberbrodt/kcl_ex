defmodule KinesisClient.TestUtil do
  @moduledoc false
  def random_string do
    min = String.to_integer("10000000", 36)
    max = String.to_integer("ZZZZZZZZ", 36)

    max
    |> Kernel.-(min)
    |> :rand.uniform()
    |> Kernel.+(min)
    |> Integer.to_string(36)
  end

  def worker_ref do
    "worker-#{:rand.uniform(10_000)}"
  end
end
