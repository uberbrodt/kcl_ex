defmodule KinesisClient.Stream.Shard.Producer do
  @moduledoc """
  Producer GenStage used in `KinesisClient.Stream.ShardConsumer` Broadway pipeline.
  """
  use GenStage
  require Logger
  alias KinesisClient.Kinesis
  alias KinesisClient.Stream.AppState
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
    :ack_ref,
    :app_name,
    :app_state_opts,
    :lease_owner,
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
      app_name: opts[:app_name],
      lease_owner: opts[:lease_owner],
      kinesis_opts: opts[:kinesis_opts],
      stream_name: opts[:stream_name],
      status: opts[:status],
      app_state_opts: Keyword.get(opts, :app_state_opts, []),
      shard_iterator_type: Keyword.get(opts, :shard_iterator_type, :trim_horizon),
      poll_interval: Keyword.get(opts, :poll_interval, 5_000),
      notify_pid: Keyword.get(opts, :notify_pid)
    }

    Logger.debug("Starting KinesisClient.Stream.Shard.Producer: #{inspect(state)}")
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

  @impl GenStage
  def handle_info({:ack, _ref, successful_msgs, []}, state) do
    %{metadata: %{"SequenceNumber" => checkpoint}} = successful_msgs |> Enum.reverse() |> hd()

    :ok =
      AppState.update_checkpoint(
        state.app_name,
        state.shard_id,
        state.lease_owner,
        checkpoint,
        state.app_state_opts
      )

    notify({:acked, %{checkpoint: checkpoint, success: successful_msgs, failed: []}}, state)

    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info({:ack, _ref, [], failed_msgs}, state) do
    {:noreply, failed_msgs, state}
  end

  @impl GenStage
  def handle_info({:ack, _ref, successful_msgs, failed_msgs}, state) do
    %{metadata: %{sequence_number: checkpoint}} = successful_msgs |> Enum.reverse() |> hd()

    :ok =
      AppState.update_checkpoint(
        state.shard_id,
        state.lease_owner,
        checkpoint,
        state.app_state_opts
      )

    {:noreply, failed_msgs, state}
  end

  @impl GenStage
  def handle_info(msg, state) do
    Logger.debug("ShardConsumer.Producer got an unhandled message #{inspect(msg)}")
    {:noreply, [], state}
  end

  @impl GenStage
  def handle_call(:start, from, state) do
    {:noreply, records, new_state} =
      case AppState.get_lease(state.app_name, state.shard_id, state.app_state_opts) do
        %{checkpoint: nil} ->
          get_records(%{
            state
            | status: :started,
              shard_iterator: nil,
              shard_iterator_type: :trim_horizon
          })

        %{checkpoint: seq_number} when is_binary(seq_number) ->
          get_records(%{
            state
            | status: :started,
              shard_iterator: nil,
              shard_iterator_type: :after_sequence_number,
              starting_sequence_number: seq_number
          })

        :not_found ->
          raise "No lease has been created for #{state.app_name}-#{state.shard_id}"
      end

    GenStage.reply(from, :ok)
    {:noreply, records, new_state}
  end

  @impl GenStage
  def handle_call(:stop, _from, state) do
    {:reply, :ok, [], %{state | status: :stopped}}
  end

  defp get_records(%__MODULE__{shard_iterator: nil, shard_iterator_type: :trim_horizon} = state) do
    {:ok, %{"ShardIterator" => iterator}} =
      Kinesis.get_shard_iterator(
        state.stream_name,
        state.shard_id,
        :trim_horizon,
        state.kinesis_opts
      )

    get_records(%{state | shard_iterator: iterator})
  end

  defp get_records(
         %__MODULE__{shard_iterator: nil, shard_iterator_type: :after_sequence_number} = state
       ) do
    {:ok, %{"ShardIterator" => iterator}} =
      Kinesis.get_shard_iterator(
        state.stream_name,
        state.shard_id,
        :after_sequence_number,
        Keyword.put(state.kinesis_opts, :starting_sequence_number, state.starting_sequence_number)
      )

    get_records(%{state | shard_iterator: iterator})
  end

  defp get_records(%__MODULE__{demand: demand, kinesis_opts: kinesis_opts} = state) do
    {:ok,
     %{
       "NextShardIterator" => next_iterator,
       "MillisBehindLatest" => _millis_behind_latest,
       "Records" => records
     }} = Kinesis.get_records(state.shard_iterator, Keyword.merge(kinesis_opts, limit: demand))

    new_demand = demand - length(records)

    messages = wrap_records(records)

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

    {:noreply, messages, new_state}
  end

  # convert Kinesis records to Broadway messages
  defp wrap_records(records) do
    Enum.map(records, fn %{"Data" => data} = record ->
      metadata = Map.delete(record, "Data")
      acknowledger = {Broadway.CallerAcknowledger, {self(), make_ref()}, nil}
      %Broadway.Message{data: data, metadata: metadata, acknowledger: acknowledger}
    end)
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
