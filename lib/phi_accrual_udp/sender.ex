defmodule PhiAccrualUdp.Sender do
  @moduledoc """
  Periodic UDP heartbeat sender.

  Opens a UDP socket and periodically transmits a `phi_accrual_udp`
  heartbeat packet to each configured target. Fire-and-forget — UDP
  delivery failure is the receiver's problem to detect (which is
  exactly what `phi_accrual` is for).

  ## Targets

  Each target is a `{host, port}` tuple. Host can be an IP tuple, a
  charlist, or an atom; port is an integer. Resolution happens on
  every send so DNS changes are picked up without restart, at the
  cost of a lookup per tick.

      Sender.start_link(
        targets: [
          {{10, 0, 0, 2}, 4370},
          {~c"peer-c.internal", 4370}
        ],
        interval_ms: 1_000
      )

  ## Timestamp source

  By default the sender stamps packets with
  `:erlang.system_time(:millisecond)` (wall clock, NTP-corrected on
  most systems). Pass `:timestamp_fn` to override — for example, to
  use monotonic time if you don't trust the wall clock.

  ## Telemetry

      [:phi_accrual_udp, :sender, :started]
        measurements: %{}
        metadata:     %{interval_ms, target_count}

      [:phi_accrual_udp, :sender, :tick]
        measurements: %{sent, errors}
        metadata:     %{}
  """

  use GenServer
  require Logger

  alias PhiAccrualUdp.Packet

  @default_interval_ms 1_000

  @type target :: {:inet.ip_address() | charlist() | atom(), :inet.port_number()}

  @type opts :: [
          targets: [target()],
          interval_ms: pos_integer(),
          timestamp_fn: (-> non_neg_integer()),
          name: GenServer.name()
        ]

  @spec start_link(opts()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    targets = Keyword.get(opts, :targets, [])
    interval = Keyword.get(opts, :interval_ms, @default_interval_ms)
    ts_fn = Keyword.get(opts, :timestamp_fn, fn -> :erlang.system_time(:millisecond) end)

    case :gen_udp.open(0, [:binary, active: false]) do
      {:ok, socket} ->
        :telemetry.execute(
          [:phi_accrual_udp, :sender, :started],
          %{},
          %{interval_ms: interval, target_count: length(targets)}
        )

        schedule(interval)
        {:ok, %{socket: socket, targets: targets, interval: interval, ts_fn: ts_fn}}

      {:error, reason} ->
        {:stop, {:udp_open_failed, reason}}
    end
  end

  @impl true
  def handle_info(:tick, state) do
    packet = Packet.encode(state.ts_fn.())

    {sent, errors} =
      Enum.reduce(state.targets, {0, 0}, fn {host, port}, {ok, err} ->
        case :gen_udp.send(state.socket, host, port, packet) do
          :ok -> {ok + 1, err}
          {:error, _} -> {ok, err + 1}
        end
      end)

    :telemetry.execute(
      [:phi_accrual_udp, :sender, :tick],
      %{sent: sent, errors: errors},
      %{}
    )

    schedule(state.interval)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    if state[:socket], do: :gen_udp.close(state.socket)
    :ok
  end

  defp schedule(ms), do: Process.send_after(self(), :tick, ms)
end
