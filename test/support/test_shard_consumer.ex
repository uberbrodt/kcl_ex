defmodule KinesisClient.TestShardConsumer do
  @moduledoc false
  @behaviour Broadway

  @impl Broadway
  def handle_message(processor, msg, context) do
    case Map.get(context, :notify_pid) do
      nil -> send(self(), {:handle_message, processor, msg, context})
      pid when is_pid(pid) -> send(pid, {:handle_message, processor, msg, context})
    end

    msg
  end

  @impl Broadway
  def handle_batch(batcher, messages, batch_info, context) do
    case Map.get(context, :notify_pid) do
      nil -> send(self(), {:handle_batch, batcher, messages, batch_info, context})
      pid when is_pid(pid) -> send(pid, {:handle_batch, batcher, messages, batch_info, context})
    end

    messages
  end

  @impl Broadway
  def handle_failed(messages, context) do
    case Map.get(context, :notify_pid) do
      nil -> send(self(), {:handle_failed, messages, context})
      pid when is_pid(pid) -> send(pid, {:handle_failed, messages, context})
    end

    messages
  end
end
