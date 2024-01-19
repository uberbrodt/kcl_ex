defmodule KinesisClient.Stream.Shard.Producer do
  @moduledoc """
  Producer GenStage used in `KinesisClient.Stream.ShardConsumer` Broadway pipeline.

  Messages for the Kinesis shard are retrieved here and sent to the consumer. Once
  the Producer consumes the last records of a given Stream, AWS returns the child
  shards that continue the Stream for this shard. The Producer then notifies the
  Coordinator of the child shards so that they can be added to the Stream, and
  then the Producer signals for the shard to be stopped.
  """
  use GenStage
  require Logger
  alias KinesisClient.Kinesis
  alias KinesisClient.Stream.AppState
  alias KinesisClient.Stream.Coordinator
  @behaviour Broadway.Producer

  defstruct [
    :coordinator_name,
    :kinesis_opts,
    :consumer_name,
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
    :child_shards,
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
      coordinator_name: opts[:coordinator_name],
      shard_id: opts[:shard_id],
      app_name: opts[:app_name],
      lease_owner: opts[:lease_owner],
      kinesis_opts: opts[:kinesis_opts],
      consumer_name: opts[:consumer_name],
      stream_name: opts[:stream_name],
      status: opts[:status],
      app_state_opts: Keyword.get(opts, :app_state_opts, []),
      shard_iterator_type: Keyword.get(opts, :shard_iterator_type, :trim_horizon),
      poll_interval: Keyword.get(opts, :poll_interval, 5_000),
      notify_pid: Keyword.get(opts, :notify_pid)
    }

    :telemetry.execute([:kinesis_client, :shard_producer, :started], %{}, telemetry_meta(state))

    Logger.debug("[kcl_ex] Starting KinesisClient.Stream.Shard.Producer: #{inspect(state)}")
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
    Logger.info(
      "[kcl_ex] #{state.stream_name} shard #{state.shard_id} is closed, not storing demand"
    )

    {:noreply, [], %{state | demand: demand}}
  end

  @impl GenStage
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    Logger.debug(
      "[kcl_ex] Received incoming demand for #{state.stream_name} shard #{state.shard_id}: #{incoming_demand}"
    )

    get_records(%{state | demand: demand + incoming_demand})
  end

  @impl GenStage
  def handle_info(:get_records, %{poll_timer: nil} = state) do
    Logger.debug("[kcl_ex] Poll timer is nil for #{state.stream_name} shard #{state.shard_id}")
    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info(:get_records, state) do
    notify(:poll_timer_executed, state)

    Logger.debug(
      "[kcl_ex] Try to fulfill pending demand #{state.demand}: " <>
        "[app_name: #{state.app_name}, stream_name: #{state.stream_name}, shard_id: #{state.shard_id}]"
    )

    get_records(%{state | poll_timer: nil})
  end

  def handle_info(
        :shard_closed,
        %{coordinator_name: coordinator, shard_id: shard_id, child_shards: child_shards} = state
      ) do
    Logger.info(
      "[kcl_ex] Shard is closed, notifying Coordinator: [app_name: #{state.app_name}, " <>
        "shard_id: #{state.shard_id}, stream_name: #{state.stream_name}]"
    )

    :ok = Coordinator.continue_shard_with_children(coordinator, shard_id, child_shards)

    {:noreply, [], %{state | status: :closed}}
  end

  # TODO(KNO-2531) Switch to a custom Broadway.Acknowledger instead of relying
  # on Broadway.CallerAcknowledger so that we can more efficiently checkpoint
  # our progress in dynamo.
  @impl GenStage
  def handle_info({:ack, _ref, successful_msgs, failed_msgs}, state) do
    checkpoint = get_top_checkpoint(successful_msgs, failed_msgs)

    # -1 means there wasn't a reliable checkpoint figure present, so
    # we should not update the checkpoint at all.
    # This should never happen with conforming messages.
    if checkpoint != "-1" do
      :ok = update_checkpoint(state, checkpoint)
    else
      Logger.warn("""
        [kcl_ex] Unable to update checkpoint for app_name: #{state.app_name}, shard_id: #{state.shard_id}, stream_name: #{state.stream_name}

        This might happen for a few reasons (in order of likelihood):
        1. You are writing tests, and your messages need something like %{metadata: %{"SequenceNumber" => "SomeStringHere"}}
        2. Kinesis changed how it publishes messages
        3. We tried to ack an empty batch (Not possible. Then again, we also don't _really_ know why the sky is blue, so go check your confidence.)
      """)
    end

    notify(
      {:acked, %{checkpoint: checkpoint, success: successful_msgs, failed: failed_msgs}},
      state
    )

    Logger.debug(
      "Acknowledged #{length(successful_msgs) + length(failed_msgs)} messages: [app_name: #{state.app_name} " <>
        "shard_id: #{state.shard_id}, shard_name: #{state.stream_name}]"
    )

    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info(msg, state) do
    Logger.debug(
      "[kcl_ex] #{__MODULE__} got an unhandled message for stream #{state.stream_name} shard #{state.shard_id} #{inspect(msg)}"
    )

    {:noreply, [], state}
  end

  def get_top_checkpoint(successful_msgs, failed_msgs) do
    # "-1" used as our default value. Indicates to caller that no checkpoint should be made.
    ["-1" | successful_msgs]
    |> unwrap_sequence_numbers()
    # Batch sizes may be large, using list cons | is faster than ++
    # https://github.com/devonestes/fast-elixir#combining-lists-with--vs--code
    |> Enum.reduce(unwrap_sequence_numbers(failed_msgs), &[&1 | &2])
    |> Enum.max()
  end

  defp unwrap_sequence_numbers(messages) do
    Enum.map(messages, fn
      %{metadata: %{"SequenceNumber" => checkpoint}} -> checkpoint
      # Poorly formatted messages just turn into -1
      _ -> "-1"
    end)
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
          raise KinesisClient.Error,
            message: "No lease has been created for #{state.app_name}-#{state.shard_id}"
      end

    GenStage.reply(from, :ok)

    # new_state = handle_closed_shard(new_state)
    {:noreply, records, new_state}
  end

  @impl GenStage
  def handle_call(:stop, _from, state) do
    {:reply, :ok, [], %{state | status: :stopped}}
  end

  defp update_checkpoint(%__MODULE__{} = state, checkpoint) do
    meta = telemetry_meta(state)

    :telemetry.span([:kinesis_client, :shard_producer, :update_checkpoint], meta, fn ->
      case AppState.update_checkpoint(
             state.app_name,
             state.shard_id,
             state.lease_owner,
             checkpoint,
             state.app_state_opts
           ) do
        :ok ->
          :ok

        {:error, :lease_owner_match} ->
          raise KinesisClient.Error,
            message:
              "Checkpoint failed for #{state.app_name}-#{state.shard_id}: this consumer is not the lease owner"

        unknown ->
          raise KinesisClient.Error,
            message:
              "Checkpoint failed for #{state.app_name}-#{state.shard_id}: #{inspect(unknown)}"
      end

      {:ok, meta}
    end)
  end

  defp get_records(%__MODULE__{shard_iterator: nil} = state) do
    case get_shard_iterator(state) do
      # If a shard is not found, we treat it as closed
      # as shards that are truncated are no longer
      # available and thus show up as not found.
      {:error, {"ResourceNotFoundException", _}} ->
        {:noreply, [], %{state | status: :closed}}

      {:ok, %{"ShardIterator" => nil}} ->
        {:noreply, [], %{state | status: :closed}}

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
         "MillisBehindLatest" => millis_behind_latest,
         "Records" => records
       }} ->
        Logger.debug(
          "[kcl_ex] Received #{length(records)} records (#{millis_behind_latest} ms behind latest) for #{state.app_name}-#{state.shard_id}"
        )

        new_demand = demand - length(records)

        messages = wrap_records(records)

        :telemetry.execute(
          [:kinesis_client, :shard_producer, :get_records_millis_behind_latest],
          %{duration: millis_behind_latest},
          telemetry_meta(state)
        )

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

      {:ok, %{"ChildShards" => child_shards, "MillisBehindLatest" => _, "Records" => []}} ->
        Logger.debug(
          "[kcl_ex] #{state.app_name} shard #{state.shard_id} has child shards: #{inspect(child_shards)} - the current shard will now close"
        )

        state = handle_closed_shard(%{state | child_shards: child_shards})

        {:ok, [], state}

      {:error, reason} ->
        Logger.info(
          "[kcl_ex] Received error when getting records for #{state.app_name}-#{state.shard_id}: #{inspect(reason)}"
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

  defp handle_closed_shard(%{status: :closed} = state), do: state

  defp handle_closed_shard(state) do
    Process.send_after(self(), :shard_closed, 0)

    %{state | status: :closed}
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
      consumer_name: state.consumer_name,
      stream_name: state.stream_name,
      app_name: state.app_name
    }
  end
end
