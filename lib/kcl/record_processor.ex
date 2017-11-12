defmodule Kcl.RecordProcessor do
  @moduledoc false
  alias Kcl.IOProxy
  require Logger
  @default_options [
    sleep_seconds: 5,
    checkpoint_retries: 5,
    checkpoint_freq_seconds: 60
  ]

  defmacro __using__(_) do
    quote do
      import unquote(__MODULE__)
      use Timex
      require Logger
      alias Kcl.RecordProcessor

      def initialize(options) do
        RecordProcessor.initialize(options)
      end

      def process_records(records) do
        result = Enum.map records, &(handle_record(&1))
        check_checkpoint(false)
        result
      rescue
          e -> handle_processing_error(e)
      end

      defp handle_processing_error(e) do
        Logger.error "Error while processing records: #{inspect e}"
        check_checkpoint(true)
      end

      def handle_record record do
        result = record["data"]
        |> Base.decode64!
        |> process_record

        record["sequenceNumber"]
        |> parse_int
        |> update_largest_seq

        result
      end

      def state do
        RecordProcessor.state
      end

      def init_processor(_) do
        update fn state ->
          state
          |> Keyword.put(:largest_seq, nil)
          |> Keyword.put(:last_checkpoint_time, current_time())
        end
      end

      def process_record(data), do: data

      def shutdown("TERMINATE") do
        Logger.info "Terminating, will attempt to checkpoint."
        checkpoint(nil)
      end

      def shutdown(_) do
        Logger.info "Shutting down due to failover, will not checkpoint."
        nil
      end

      defoverridable [init_processor: 1, process_records: 1, shutdown: 1, process_record: 1]
    end
  end

  def initialize(options) do
    @default_options
    |> Keyword.merge(options)
    |> ensure_started
  end

  def ensure_started, do: ensure_started @default_options
  def ensure_started options do
    alive? = Process.registered |> Enum.member?(__MODULE__)

    if !alive? do
      Agent.start_link(fn -> options end, name: __MODULE__)
    end
  end

  def parse_int(val) when is_integer(val), do: val
  def parse_int(val), do: Integer.parse(val)

  def state do
    ensure_started()
    Agent.get(__MODULE__, &(&1))
  end

  def update fun do
    ensure_started()
    Agent.update(__MODULE__, fun)
  end

  def update_largest_seq({seq, _}), do: update_largest_seq seq
  def update_largest_seq seq do
    if seq > largest_seq() do
      update fn state ->
        Keyword.put state, :largest_seq, seq
      end
    end
  end

  defp do_checkpoint? do
    (current_time() - last_checkpoint_time()) > state()[:checkpoint_freq_seconds]
  end

  def check_checkpoint force_checkpoint do
    if do_checkpoint?() || force_checkpoint do
      checkpoint(state()[:largest_seq])
      update fn state ->
        Keyword.put(state, :last_checkpoint_time, current_time())
      end
    end
  end

  def current_time do
    DateTime.utc_now() |> Map.get(:second)
  end

  def last_checkpoint_time do
    Keyword.get(state(), :last_checkpoint_time, 0)
  end

  def largest_seq do
    Keyword.get(state(), :largest_seq, 0)
  end

  def checkpoint seq do
    try_checkpoint seq, Keyword.get(state(), :checkpoint_retries)
  end

  def try_checkpoint(_seq, attempts_remaining) when attempts_remaining <= 0 do
    Logger.error "Failed to checkpoint. Giving up."
    {:error, "CheckpointFailed"}
  end

  def try_checkpoint seq, attempts_remaining do
    Logger.info "Attempting checkpoint with #{attempts_remaining} attempts remaining."
    IOProxy.write_action("checkpoint", %{checkpoint: seq})
    line = IOProxy.read_line
    case JSX.decode(line) do
      {:ok, action} -> handle_action action
      {:error, _error} -> try_checkpoint seq, attempts_remaining - 1
    end
  end

  def handle_action action do
    case action do
      %{"action" => "checkpoint", "error" => error} -> {:error, error}
      _ -> :ok
    end
  end
end
