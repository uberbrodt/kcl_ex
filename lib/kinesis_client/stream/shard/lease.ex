defmodule KinesisClient.Stream.Shard.Lease do
  @moduledoc false
  require Logger
  use GenServer
  alias KinesisClient.Stream.AppState
  alias KinesisClient.Stream.AppState.ShardLease
  alias KinesisClient.Stream.Shard.Pipeline

  @default_renew_interval 30_000
  # The amount of time that must have elapsed since the least_count was incremented in order to
  # consider the lease expired.
  @default_lease_expiry 90_001

  def start_link(opts) do
    name = name(opts[:app_name], opts[:shard_id])

    GenServer.start_link(__MODULE__, opts, name: name)
  end

  defstruct [
    :app_name,
    :shard_id,
    :lease_owner,
    :lease_count,
    :lease_count_increment_time,
    :app_state_opts,
    :renew_interval,
    :notify,
    :lease_expiry,
    :lease_holder
  ]

  @type t :: %__MODULE__{}

  @impl GenServer
  def init(opts) do
    state = %__MODULE__{
      app_name: opts[:app_name],
      shard_id: opts[:shard_id],
      lease_owner: opts[:lease_owner],
      app_state_opts: Keyword.get(opts, :app_state_opts, []),
      renew_interval: Keyword.get(opts, :renew_interval, @default_renew_interval),
      lease_expiry: Keyword.get(opts, :lease_expiry, @default_lease_expiry),
      lease_count_increment_time: current_time(),
      notify: Keyword.get(opts, :notify)
    }

    Process.send_after(self(), :take_or_renew_lease, state.renew_interval)

    Logger.debug("Starting KinesisClient.Stream.Lease: #{inspect(state)}")

    {:ok, state, {:continue, :initialize}}
  end

  @impl GenServer
  def handle_continue(:initialize, state) do
    new_state =
      case get_lease(state) do
        :not_found ->
          Logger.debug(
            "No existing lease record found in AppState: " <>
              "[app_name: #{state.app_name}, shard_id: #{state.shard_id}]"
          )

          create_lease(state)

        %ShardLease{} = s ->
          take_or_renew_lease(s, state)
      end

    if new_state.lease_holder do
      :ok = Pipeline.start(state.app_name, state.shard_id)
    end

    notify({:initialized, new_state}, state)

    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info(:take_or_renew_lease, state) do
    Process.send_after(self(), :take_or_renew_lease, state.renew_interval)

    reply =
      case get_lease(state) do
        %ShardLease{} = s ->
          {:noreply, take_or_renew_lease(s, state)}

        {:error, e} ->
          Logger.error("Error fetching shard #{state.share_id}: #{inspect(e)}")
          {:noreply, state}
      end

    reply
  end

  @spec take_or_renew_lease(shard_lease :: ShardLease.t(), state :: t()) :: t()
  defp take_or_renew_lease(shard_lease, %{lease_expiry: lease_expiry} = state) do
    cond do
      shard_lease.lease_owner == state.lease_owner ->
        renew_lease(shard_lease, state)

      current_time() - state.lease_count_increment_time > lease_expiry ->
        take_lease(shard_lease, state)

      true ->
        state =
          case shard_lease.lease_count != state.lease_count do
            true ->
              set_lease_count(shard_lease.lease_count, false, state)

            false ->
              %{state | lease_holder: false}
          end

        Logger.debug(
          "Lease is owned by another node, and could not be taken: [shard_id: #{state.shard_id}, " <>
            "lease_owner: #{state.lease_owner}, lease_count: #{state.lease_count}]"
        )

        notify({:tracking_lease, state}, state)
        state
    end
  end

  defp set_lease_count(lease_count, is_lease_holder, %__MODULE__{} = state) do
    %{
      state
      | lease_count: lease_count,
        lease_count_increment_time: current_time(),
        lease_holder: is_lease_holder
    }
  end

  defp get_lease(state) do
    AppState.get_lease(state.app_name, state.shard_id, state.app_state_opts)
  end

  @spec create_lease(state :: t()) :: t()
  defp create_lease(%{app_state_opts: opts, app_name: app_name, lease_owner: lease_owner} = state) do
    Logger.debug(
      "Creating lease: [app_name: #{app_name}, shard_id: #{state.shard_id}, lease_owner: " <>
        "#{lease_owner}]"
    )

    case AppState.create_lease(app_name, state.shard_id, lease_owner, opts) do
      :ok -> %{state | lease_holder: true, lease_count: 1}
      :already_exists -> %{state | lease_holder: false}
    end
  end

  @spec renew_lease(shard_lease :: ShardLease.t(), state :: t()) :: t()
  defp renew_lease(shard_lease, %{app_state_opts: opts, app_name: app_name} = state) do
    expected = shard_lease.lease_count + 1

    Logger.debug(
      "Renewing lease: [app_name: #{app_name}, shard_id: #{state.shard_id}, lease_owner: " <>
        "#{state.lease_owner}]"
    )

    case AppState.renew_lease(app_name, shard_lease, opts) do
      {:ok, ^expected} ->
        state = set_lease_count(expected, true, state)
        notify({:lease_renewed, state}, state)
        state

      {:error, :lease_renew_failed} ->
        Logger.debug(
          "Failed to renew lease, stopping producer: [app_name: #{app_name}, " <>
            "shard_id: #{state.shard_id}, lease_owner: #{state.lease_owner}]"
        )

        :ok = Pipeline.stop(app_name, state.shard_id)
        %{state | lease_holder: false, lease_count_increment_time: current_time()}

      {:error, e} ->
        Logger.error("Error trying to renew lease for #{state.shard_id}: #{inspect(e)}")
        state
    end
  end

  defp take_lease(_shard_lease, %{app_state_opts: opts, app_name: app_name} = state) do
    expected = state.lease_count + 1

    Logger.debug(
      "Attempting to take lease: [lease_owner: #{state.lease_owner}, shard_id: #{state.shard_id}]"
    )

    case AppState.take_lease(app_name, state.shard_id, state.lease_owner, state.lease_count, opts) do
      {:ok, ^expected} ->
        state = %{
          state
          | lease_holder: true,
            lease_count: expected,
            lease_count_increment_time: current_time()
        }

        notify({:lease_taken, state}, state)
        :ok = Pipeline.start(app_name, state.shard_id)
        state

      {:error, :lease_take_failed} ->
        # TODO
        # :ok = Processor.ensure_halted(state)
        %{state | lease_holder: false, lease_count_increment_time: current_time()}
    end
  end

  defp notify(_msg, %{notify: nil}) do
    :ok
  end

  defp notify(msg, %{notify: notify}) do
    send(notify, msg)
    :ok
  end

  defp current_time do
    System.monotonic_time(:millisecond)
  end

  def name(app_name, shard_id) do
    Module.concat([__MODULE__, app_name, shard_id])
  end
end
