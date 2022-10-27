defmodule KinesisClient.Stream.Shard.Producer do
  @moduledoc """
  Producer GenStage used in `KinesisClient.Stream.ShardConsumer` Broadway pipeline.
  """
  use GenStage
  require Logger
  alias KinesisClient.Kinesis
  alias KinesisClient.Stream.AppState
  alias KinesisClient.Stream.Coordinator
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
    :shard_closed_timer,
    shutdown_delay: 300_000,
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

    :telemetry.execute([:kinesis_client, :shard_producer, :started], %{}, telemetry_meta(state))

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
  def handle_demand(_, %{demand: demand, status: :closed} = state) do
    Logger.info("Shard is closed, not storing demand")
    {:noreply, [], %{state | demand: demand}}
  end

  @impl GenStage
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    Logger.debug("Received incoming demand: #{incoming_demand}")
    get_records(%{state | demand: demand + incoming_demand})
  end

  @impl GenStage
  def handle_info(:get_records, %{poll_timer: nil} = state) do
    Logger.debug("Poll timer is nil")
    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info(:get_records, state) do
    notify(:poll_timer_executed, state)

    Logger.debug(
      "Try to fulfill pending demand #{state.demand}: " <>
        "[app_name: #{state.app_name}, shard_id: #{state.shard_id}]"
    )

    get_records(%{state | poll_timer: nil})
  end

  def handle_info(:shard_closed, %{coordinator_name: coordinator, shard_id: shard_id} = state) do
    # just in case something goes awry, try and close the Shard in the future
    Logger.info(
      "Shard is closed, notifying Coordinator: [app_name: #{state.app_name}, " <>
        "shard_id: #{state.shard_id}]"
    )

    timer = Process.send_after(self(), :shard_closed, state.shutdown_delay)
    AppState.close_shard(state.app_name, state.shard_id, state.app_state_opts)
    :ok = Coordinator.close_shard(coordinator, shard_id)
    {:noreply, %{state | shard_closed_timer: timer}}
  end

  @impl GenStage
  def handle_info({:ack, _ref, successful_msgs, []}, state) do
    %{metadata: %{"SequenceNumber" => checkpoint}} = successful_msgs |> Enum.reverse() |> hd()

    :ok = update_checkpoint(state, checkpoint)
    notify({:acked, %{checkpoint: checkpoint, success: successful_msgs, failed: []}}, state)

    Logger.debug(
      "Acknowledged #{length(successful_msgs)} messages: [app_name: #{state.app_name} " <>
        "shard_id: #{state.shard_id}"
    )

    state = handle_closed_shard(state)

    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info({:ack, _ref, [], failed_msgs}, state) do
    Logger.debug("Retrying #{length(failed_msgs)} failed messages")

    state =
      case state.shard_closed_timer do
        nil ->
          state

        timer ->
          Process.cancel_timer(timer)
          %{state | shard_closed_timer: nil}
      end

    {:noreply, failed_msgs, state}
  end

  @impl GenStage
  def handle_info({:ack, _ref, successful_msgs, failed_msgs}, state) do
    %{metadata: %{"SequenceNumber" => checkpoint}} = successful_msgs |> Enum.reverse() |> hd()

    :ok = update_checkpoint(state, checkpoint)

    Logger.debug(
      "Acknowledged #{length(successful_msgs)} messages, Retrying #{length(failed_msgs)} failed messages"
    )

    state =
      case state.shard_closed_timer do
        nil ->
          state

        timer ->
          Process.cancel_timer(timer)
          %{state | shard_closed_timer: nil}
      end

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

  defp update_checkpoint(%__MODULE__{} = state, checkpoint) do
    meta = telemetry_meta(state)

    :telemetry.span([:kinesis_client, :shard_producer, :update_checkpoint], meta, fn ->
      :ok =
        AppState.update_checkpoint(
          state.app_name,
          state.shard_id,
          state.lease_owner,
          checkpoint,
          state.app_state_opts
        )

      {:ok, meta}
    end)
  end

  defp get_records(%__MODULE__{shard_iterator: nil} = state) do
    case get_shard_iterator(state) do
      {:ok, %{"ShardIterator" => nil}} ->
        {:noreply, [], %{status: :closed}}

      {:ok, %{"ShardIterator" => iterator}} ->
        get_records(%{state | shard_iterator: iterator})
    end
  end

  defp get_records(state) do
    meta = telemetry_meta(state)

    :telemetry.span([:kinesis_client, :shard_producer, :get_records], meta, fn ->
      {:ok, messages, new_state} = do_get_records(state)

      {
        {:noreply, messages, new_state},
        Map.put(meta, :messages_count, length(messages))
      }
    end)
  end

  defp do_get_records(%__MODULE__{demand: demand, kinesis_opts: kinesis_opts} = state) do
    # Cap get_records to retrieve the lesser of demand or the Kinesis AWS limit of 10k records per request
    limit = Enum.min([demand, 10_000])

    case Kinesis.get_records(state.shard_iterator, Keyword.merge(kinesis_opts, limit: limit)) do
      {:ok,
       %{
         "NextShardIterator" => next_iterator,
         "MillisBehindLatest" => _millis_behind_latest,
         "Records" => records
       }} ->
        new_demand = demand - length(records)

        messages = wrap_records(records)

        poll_timer =
          case {records, new_demand} do
            {[], _} ->
              schedule_shard_poll(state.poll_interval)

            {_, 0} ->
              # Don't repoll if there's no demand
              nil

            _ ->
              # Introduce jitter here for the immediate poll so that we execute somewhere
              # between 0 and the poll interval time
              Enum.random(0..state.poll_interval) |> schedule_shard_poll()
          end

        new_state = %{
          state
          | demand: new_demand,
            poll_timer: poll_timer,
            shard_iterator: next_iterator
        }

        {:ok, messages, new_state}

      {:error, reason} ->
        Logger.info(
          "Received error when getting records for #{state.app_name}-#{state.shard_id}: #{inspect(reason)}"
        )

        :telemetry.execute(
          [:kinesis_client, :shard_producer, :get_records_error],
          %{reason: reason},
          telemetry_meta(state)
        )

        # Retry the get records call again to see if the issue clears up
        poll_timer = schedule_shard_poll(state.poll_interval)
        new_state = %{state | poll_timer: poll_timer}

        {:ok, [], new_state}
    end
  end

  defp get_shard_iterator(%{shard_iterator_type: :after_sequence_number} = state) do
    Kinesis.get_shard_iterator(
      state.stream_name,
      state.shard_id,
      :after_sequence_number,
      Keyword.put(
        state.kinesis_opts,
        :starting_sequence_number,
        state.starting_sequence_number
      )
    )
  end

  defp get_shard_iterator(%{shard_iterator_type: :trim_horizon} = state) do
    Kinesis.get_shard_iterator(
      state.stream_name,
      state.shard_id,
      :trim_horizon,
      state.kinesis_opts
    )
  end

  # convert Kinesis records to Broadway messages
  defp wrap_records(records) do
    ref = make_ref()

    Enum.map(records, fn %{"Data" => data} = record ->
      metadata = Map.delete(record, "Data")
      acknowledger = {Broadway.CallerAcknowledger, {self(), ref}, nil}
      %Broadway.Message{data: data, metadata: metadata, acknowledger: acknowledger}
    end)
  end

  defp handle_closed_shard(%{status: :closed, shard_closed_timer: nil, shutdown_delay: delay} = s) do
    timer = Process.send_after(self(), :shard_closed, delay)

    %{s | shard_closed_timer: timer}
  end

  defp handle_closed_shard(
         %{status: :closed, shard_closed_timer: old_timer, shutdown_delay: delay} = s
       ) do
    Process.cancel_timer(old_timer)

    timer = Process.send_after(self(), :shard_closed, delay)

    %{s | shard_closed_timer: timer}
  end

  defp handle_closed_shard(state) do
    state
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

  defp telemetry_meta(%__MODULE__{} = state) do
    %{
      shard_id: state.shard_id,
      stream_name: state.stream_name,
      app_name: state.app_name
    }
  end
end
