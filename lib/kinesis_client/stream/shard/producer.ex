defmodule KinesisClient.Stream.Shard.Producer do
  @moduledoc """
  Producer GenStage used in `KinesisClient.Stream.ShardConsumer` Broadway pipeline.
  """
  use GenStage
  require Logger
  alias KinesisClient.Kinesis
  @behaviour Broadway.Producer

  defstruct [
    :kinesis_opts,
    :stream_name,
    :shard_id,
    :shard_iterator,
    :shard_iterator_type,
    :starting_sequence_number,
    :poll_interval,
    :poll_timer,
    :status,
    :notify_pid,
    demand: 0
  ]

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  def start(name) do
    GenServer.call(name, :start)
  end

  def stop(name) do
    GenServer.call(name, :stop)
  end

  @impl GenStage
  def init(opts) do
    state = %__MODULE__{
      shard_id: opts[:shard_id],
      kinesis_opts: opts[:kinesis_opts],
      stream_name: opts[:stream_name],
      status: opts[:status],
      shard_iterator_type: Keyword.get(opts, :shard_iterator_type, :trim_horizon),
      poll_interval: Keyword.get(opts, :poll_interval, 5_000),
      notify_pid: Keyword.get(opts, :notify_pid)
    }

    {:producer, state}
  end

  # Don't fetch from Kinesis if status is :stopped
  @impl GenStage
  def handle_demand(incoming_demand, %{demand: demand, status: :stopped} = state) do
    notify({:queuing_demand_while_stopped, incoming_demand}, state)
    {:noreply, [], %{state | demand: demand + incoming_demand}}
  end

  @impl GenStage
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    get_records(%{state | demand: demand + incoming_demand})
  end

  @impl GenStage
  def handle_info(:get_records, %{poll_timer: nil} = state) do
    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info(:get_records, state) do
    notify(:poll_timer_executed, state)
    get_records(state)
  end

  def handle_info(msg, state) do
    Logger.debug("ShardConsumer.Producer got an unhandled message #{inspect(msg)}")
    {:noreply, [], state}
  end

  defp get_records(%__MODULE__{shard_iterator: nil, kinesis_opts: opts} = state) do
    {:ok, %{shard_iterator: iterator}} =
      Kinesis.get_shard_iterator(
        state.stream_name,
        state.shard_id,
        state.shard_iterator_type,
        opts
      )

    get_records(%{state | shard_iterator: iterator})
  end

  defp get_records(%__MODULE__{demand: demand, kinesis_opts: kinesis_opts} = state) do
    {:ok,
     %{
       next_shard_iterator: next_iterator,
       millis_behind_latest: _millis_behind_latest,
       records: records
     }} = Kinesis.get_records(state.shard_iterator, Keyword.merge(kinesis_opts, limit: demand))

    new_demand = demand - length(records)

    poll_timer =
      case {records, new_demand} do
        {[], _} -> schedule_shard_poll(state.poll_interval)
        {_, 0} -> nil
        _ -> schedule_shard_poll(state.poll_interval)
      end

    new_state = %{
      state
      | demand: new_demand,
        poll_timer: poll_timer,
        shard_iterator: next_iterator
    }

    {:noreply, records, new_state}
  end

  defp schedule_shard_poll(interval) do
    Process.send_after(self(), :get_records, interval)
  end

  defp notify(message, %__MODULE__{notify_pid: notify_pid}) do
    case notify_pid do
      pid when is_pid(pid) ->
        send(pid, message)
        :ok

      nil ->
        :ok
    end
  end
end
