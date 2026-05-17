defmodule PhiAccrualUdp.Listener do
  @moduledoc """
  UDP listener that receives `phi_accrual_udp` heartbeats and feeds
  them into the `PhiAccrual` core detector.

  Opens a UDP socket on a configurable port, decodes incoming
  packets via `PhiAccrualUdp.Packet.decode/1` (dual-decoding both
  v1 and v2 wire formats throughout 1.x), and on success calls
  `PhiAccrual.observe(node, receipt_ts)` where `receipt_ts` comes
  from `:erlang.monotonic_time(:millisecond)` at the moment the
  packet was pulled from the socket. Decode failures and resolver
  rejections are dropped and reported via telemetry.

  ## Mapping packet to node

  The listener resolves each packet's source to a `node` term
  before calling `PhiAccrual.observe/2`. Resolution is configurable
  via `:node_resolver`.

  ### Default resolver

  The default resolver returns tagged tuples that distinguish the
  identity source:

    * v2 packets → `{:sender_id, sender_id}`
    * v1 packets → `{:peer, ip, port}`

  Tagged tuples prevent accidental collision (a bare u64 could
  shadow other integer-shaped node identities) and let downstream
  code pattern-match on the identity source.

  ### Custom resolver

  Custom resolvers receive three arguments and return either a
  `node` term to use as the identity, or `{:reject, reason}` to
  drop the packet:

      resolver = fn
        _ip, _port, sender_id when is_integer(sender_id) ->
          lookup_by_id(sender_id) || {:reject, :unknown_sender}

        ip, port, nil ->
          # v1 packet, no sender_id available
          lookup_by_peer(ip, port) || {:reject, :unknown_peer}
      end

      Listener.start_link(port: 4370, node_resolver: resolver)

  ### Resolver safety

  The `:node_resolver` runs synchronously in the Listener process
  on every received packet. Keep it cheap and non-blocking: use
  `:persistent_term` for static lookup tables, ETS for dynamic
  ones. Avoid `GenServer.call/2`, network I/O, or anything else
  that can block — these will stall packet processing for every
  peer, not just the one being resolved.

  Resolvers must return a term to use as the node identity, or
  `{:reject, reason}` to drop the packet. `nil` is treated as a
  valid node identity — to drop, you must explicitly return
  `{:reject, reason}`.

  Exceptions raised by the resolver crash the Listener; the
  supervisor will restart it, but estimator state for all peers
  will reset. Validate inputs in your resolver and use
  `{:reject, reason}` for rejection paths, not exceptions.

  ## Address family and binding

    * `:inet6` (default `false`) — when `true`, opens the socket
      with the IPv6 family AND sets `{:ipv6_v6only, true}`
      explicitly. The library does not rely on OS defaults for
      `IPV6_V6ONLY` (which vary across Linux/BSD/Windows). The
      socket accepts ONLY IPv6 traffic.

    * `:ip` (default: bind to all interfaces) — bind address,
      passed through to `:gen_udp.open/2`. Must be an `:inet`
      address tuple (4-element for v4, 8-element for v6).
      Hostnames are not accepted by `:gen_udp` for this option;
      resolve them in the caller if needed.

  Dual-stack deployments (a single host serving both v4 and v6
  peers) should run **two listeners**, one per family, supervised
  together. Do not attempt to multiplex via OS-level dual-stack
  on a single v6 socket: v4 peers would arrive as v4-mapped-v6
  addresses, mutating the `{:peer, ip, port}` identity shape and
  breaking the stable-identity contract.

  IP tuples in resolver arguments differ in shape:

    * v4 → 4-element tuple, e.g. `{10, 0, 0, 1}`
    * v6 → 8-element tuple, e.g. `{0, 0, 0, 0, 0, 0, 0, 1}` (`::1`)

  Resolvers running across mixed-family deployments must handle
  both shapes. The cleanest answer is to configure stable
  `:sender_id` values on senders so the resolver can match on the
  third argument regardless of the source address family.

  ## Flow control

  The listener opens its UDP socket with `active: N` (rather than
  `active: true`), so the kernel delivers at most `N` packets to
  the GenServer mailbox before falling back to passive mode. On
  `:udp_passive`, the listener re-arms with another batch of `N`.
  This bounds per-burst mailbox growth under packet floods —
  important because decode cost is paid as soon as the packet
  enters the mailbox, well before any downstream `phi_accrual`
  shedding can help.

  Tune via the `:active_count` option (default `100`). Higher
  values amortize the re-arm syscall across more packets at the
  cost of larger worst-case mailbox bursts; lower values give
  tighter back-pressure but more re-arm overhead.

  ## Telemetry

      [:phi_accrual_udp, :listener, :started]
        measurements: %{}
        metadata:     %{port, inet6, ip}
        # ip is nil when not explicitly set (bind to all interfaces)

      [:phi_accrual_udp, :listener, :passive]
        measurements: %{}
        metadata:     %{port}
        # emitted each time the listener re-arms after consuming
        # `active_count` packets; useful for observing ingress
        # saturation

      [:phi_accrual_udp, :sample, :received]
        measurements: %{packet_timestamp_ms}
        metadata:     %{node, peer, wire_version}
        # wire_version is 1 or 2; group by wire_version to track
        # fleet migration progress (v1 ratio decays to 0 as senders
        # upgrade)

      [:phi_accrual_udp, :sample, :rejected]
        measurements: %{}
        metadata:     %{peer, sender_id, reason, wire_version}
        # emitted when the resolver returns {:reject, reason};
        # `reason` is whatever the resolver returned. Prefer
        # atoms or short tagged tuples to keep telemetry
        # cardinality bounded. `sender_id` is nil for rejected
        # v1 packets.

      [:phi_accrual_udp, :decode, :error]
        measurements: %{packet_size}
        metadata:     %{reason, peer}
        # reason ∈ [:wrong_size, :bad_magic, :unsupported_version,
        #           :reserved_flags_set, :reserved_sender_id]

  ## Security caveat

  UDP is unauthenticated. Anyone who can reach the listener port
  can send packets that pass `Packet.decode/1` and feed
  observations into the estimator. With v2, a hostile peer can
  also mint arbitrary `sender_id` values, creating unbounded
  cold-start estimator state on the receiver. In hostile
  networks, bind the socket to a private interface, firewall the
  port, or layer authentication on top via a `node_resolver` that
  returns `{:reject, reason}` for unknown peers.
  """

  use GenServer
  require Logger

  alias PhiAccrualUdp.Packet

  @default_port 4370
  @default_active_count 100

  @type node_resolver ::
          (:inet.ip_address(), :inet.port_number(), non_neg_integer() | nil ->
             term() | {:reject, term()})

  @type opts :: [
          port: :inet.port_number(),
          node_resolver: node_resolver(),
          active_count: pos_integer(),
          inet6: boolean(),
          ip: :inet.ip_address(),
          name: GenServer.name()
        ]

  @spec start_link(opts()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Build a child specification for use under a `Supervisor`.

  Honors the standard supervisor options `:id`, `:restart`, and
  `:shutdown` when present in the keyword list, alongside the
  Listener's own options. Useful for running multiple Listener
  instances under one supervisor — e.g., one per address family
  for dual-stack deployments:

      children = [
        {PhiAccrualUdp.Listener, port: 4370, id: :listener_v4},
        {PhiAccrualUdp.Listener,
          port: 4370, inet6: true, id: :listener_v6}
      ]

  Defaults: `id: PhiAccrualUdp.Listener`, `restart: :permanent`,
  `shutdown: 5_000`, `type: :worker`.
  """
  @spec child_spec(opts()) :: Supervisor.child_spec()
  def child_spec(opts) when is_list(opts) do
    {sup_opts, start_opts} = Keyword.split(opts, [:id, :restart, :shutdown])

    %{
      id: Keyword.get(sup_opts, :id, __MODULE__),
      start: {__MODULE__, :start_link, [start_opts]},
      type: :worker,
      restart: Keyword.get(sup_opts, :restart, :permanent),
      shutdown: Keyword.get(sup_opts, :shutdown, 5_000)
    }
  end

  @doc false
  def default_node_resolver(_ip, _port, sender_id) when is_integer(sender_id),
    do: {:sender_id, sender_id}

  def default_node_resolver(ip, port, nil),
    do: {:peer, ip, port}

  @impl true
  def init(opts) do
    port = Keyword.get(opts, :port, @default_port)
    resolver = Keyword.get(opts, :node_resolver, &__MODULE__.default_node_resolver/3)
    active_count = Keyword.get(opts, :active_count, @default_active_count)
    inet6 = Keyword.get(opts, :inet6, false)
    ip_opt = Keyword.fetch(opts, :ip)

    validate_resolver_arity!(resolver)

    open_opts =
      [:binary, active: active_count, reuseaddr: true]
      |> maybe_add_inet6(inet6)
      |> maybe_add_ip(ip_opt)

    case :gen_udp.open(port, open_opts) do
      {:ok, socket} ->
        :telemetry.execute(
          [:phi_accrual_udp, :listener, :started],
          %{},
          %{port: port, inet6: inet6, ip: elem_or_nil(ip_opt)}
        )

        {:ok, %{socket: socket, port: port, resolver: resolver, active_count: active_count}}

      {:error, reason} ->
        {:stop, {:udp_open_failed, reason}}
    end
  end

  defp maybe_add_inet6(opts, true), do: [:inet6, {:ipv6_v6only, true} | opts]
  defp maybe_add_inet6(opts, false), do: opts

  defp maybe_add_ip(opts, {:ok, ip}), do: [{:ip, ip} | opts]
  defp maybe_add_ip(opts, :error), do: opts

  defp elem_or_nil({:ok, v}), do: v
  defp elem_or_nil(:error), do: nil

  defp validate_resolver_arity!(fun) when is_function(fun) do
    case :erlang.fun_info(fun, :arity) do
      {:arity, 3} ->
        :ok

      {:arity, n} ->
        raise ArgumentError, """
        :node_resolver must be a 3-arity function:

            (ip, port, sender_id | nil) -> term | {:reject, reason}

        Got a #{n}-arity function. This is a breaking change from
        phi_accrual_udp 0.1.x where the resolver was 2-arity
        (ip, port). See CHANGELOG and the README upgrade-path
        section.
        """
    end
  end

  defp validate_resolver_arity!(other) do
    raise ArgumentError, ":node_resolver must be a 3-arity function, got: #{inspect(other)}"
  end

  @impl true
  def handle_info({:udp, _socket, ip, port, packet}, state) do
    receipt_ts = :erlang.monotonic_time(:millisecond)
    peer = {ip, port}

    case Packet.decode(packet) do
      {:ok, %Packet{version: wire_version, sender_id: sender_id, timestamp_ms: ts}} ->
        case state.resolver.(ip, port, sender_id) do
          {:reject, reason} ->
            :telemetry.execute(
              [:phi_accrual_udp, :sample, :rejected],
              %{},
              %{
                peer: peer,
                sender_id: sender_id,
                reason: reason,
                wire_version: wire_version
              }
            )

          node ->
            :telemetry.execute(
              [:phi_accrual_udp, :sample, :received],
              %{packet_timestamp_ms: ts},
              %{node: node, peer: peer, wire_version: wire_version}
            )

            PhiAccrual.observe(node, receipt_ts)
        end

      {:error, reason} ->
        :telemetry.execute(
          [:phi_accrual_udp, :decode, :error],
          %{packet_size: byte_size(packet)},
          %{reason: reason, peer: peer}
        )
    end

    {:noreply, state}
  end

  def handle_info({:udp_passive, socket}, %{socket: socket} = state) do
    :ok = :inet.setopts(socket, active: state.active_count)

    :telemetry.execute(
      [:phi_accrual_udp, :listener, :passive],
      %{},
      %{port: state.port}
    )

    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    if state[:socket], do: :gen_udp.close(state.socket)
    :ok
  end
end
