defmodule PhiAccrualUdp.Listener do
  @moduledoc """
  UDP listener that receives `phi_accrual_udp` heartbeats and feeds
  them into the `PhiAccrual` core detector.

  Opens a UDP socket on a configurable port, decodes incoming packets
  via `PhiAccrualUdp.Packet.decode/1`, and on success calls
  `PhiAccrual.observe(node, receipt_ts)` where `receipt_ts` comes from
  `:erlang.monotonic_time(:millisecond)` at the moment the packet was
  pulled from the socket. Decode failures are dropped and reported via
  telemetry.

  ## Mapping packet to node

  By default, the listener uses the sender's IP+port tuple as the node
  identifier (`{ip_tuple, port}`). Pass a `:node_resolver` function to
  map IP+port to your own node atoms — useful for static cluster
  topologies where you want stable node ids across sender restarts.

      Listener.start_link(
        port: 4370,
        node_resolver: fn
          {10, 0, 0, 1}, _ -> :node_a
          {10, 0, 0, 2}, _ -> :node_b
          ip, port -> {ip, port}
        end
      )

  ## Telemetry

      [:phi_accrual_udp, :listener, :started]
        measurements: %{}
        metadata:     %{port}

      [:phi_accrual_udp, :sample, :received]
        measurements: %{packet_timestamp_ms}
        metadata:     %{node, peer}
        # peer is {ip, port}; node is whatever node_resolver returned

      [:phi_accrual_udp, :decode, :error]
        measurements: %{packet_size}
        metadata:     %{reason, peer}
        # reason ∈ [:wrong_size, :bad_magic, :unsupported_version, :reserved_flags_set]

  ## Security caveat

  UDP is unauthenticated. Anyone who can reach the listener port can
  send packets that pass `Packet.decode/1` and feed observations into
  the estimator, potentially poisoning detection. In hostile networks,
  bind the socket to a private interface, firewall the port, or layer
  authentication on top via a `node_resolver` that rejects unknown
  peers.
  """

  use GenServer
  require Logger

  alias PhiAccrualUdp.Packet

  @default_port 4370

  @type opts :: [
          port: :inet.port_number(),
          node_resolver: (:inet.ip_address(), :inet.port_number() -> term()),
          name: GenServer.name()
        ]

  @spec start_link(opts()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc false
  def default_node_resolver(ip, port), do: {ip, port}

  @impl true
  def init(opts) do
    port = Keyword.get(opts, :port, @default_port)
    resolver = Keyword.get(opts, :node_resolver, &__MODULE__.default_node_resolver/2)

    case :gen_udp.open(port, [:binary, active: true, reuseaddr: true]) do
      {:ok, socket} ->
        :telemetry.execute([:phi_accrual_udp, :listener, :started], %{}, %{port: port})
        {:ok, %{socket: socket, port: port, resolver: resolver}}

      {:error, reason} ->
        {:stop, {:udp_open_failed, reason}}
    end
  end

  @impl true
  def handle_info({:udp, _socket, ip, port, packet}, state) do
    receipt_ts = :erlang.monotonic_time(:millisecond)
    peer = {ip, port}

    case Packet.decode(packet) do
      {:ok, %Packet{timestamp_ms: ts}} ->
        node = state.resolver.(ip, port)

        :telemetry.execute(
          [:phi_accrual_udp, :sample, :received],
          %{packet_timestamp_ms: ts},
          %{node: node, peer: peer}
        )

        PhiAccrual.observe(node, receipt_ts)

      {:error, reason} ->
        :telemetry.execute(
          [:phi_accrual_udp, :decode, :error],
          %{packet_size: byte_size(packet)},
          %{reason: reason, peer: peer}
        )
    end

    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    if state[:socket], do: :gen_udp.close(state.socket)
    :ok
  end
end
