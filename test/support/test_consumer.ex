defmodule KinesisClient.TestConsumer do
  @moduledoc false
  use GenStage

  def start_link(notify_pid) do
    GenStage.start_link(__MODULE__, notify_pid)
  end

  def init(notify_pid) do
    {:consumer, %{notify_pid: notify_pid}}
  end

  def handle_events(events, _, %{notify_pid: pid} = state) do
    send(pid, {:consumer_events, events})

    {:noreply, [], state}
  end
end
