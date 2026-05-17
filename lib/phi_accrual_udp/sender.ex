defmodule PhiAccrualUdp.Sender do
  @moduledoc """
  Periodic UDP heartbeat sender.

  Opens a UDP socket and periodically transmits a `phi_accrual_udp`
  v2 heartbeat packet to each configured target. Fire-and-forget —
  UDP delivery failure is the receiver's problem to detect (which
  is exactly what `phi_accrual` is for).

  ## Required configuration

      Sender.start_link(
        sender_id: 0xA1B2C3D4_E5F60718,
        targets: [
          {{10, 0, 0, 2}, 4370},
          {~c"peer-c.internal", 4370}
        ],
        interval_ms: 1_000
      )

  ### `:sender_id`

  **Required.** A non-zero unsigned 64-bit integer that identifies
  this node on the wire. `0` is reserved at the packet-format level.

  Pick a stable identifier — node name hashed, partner ID, terminal
  ID, whatever your topology provides. The receiver uses this value
  (not the packet's source IP/port) as the default node identity,
  so a stable `sender_id` survives:

    * Sender restarts (which change the ephemeral source port)
    * NAT session recycling
    * Container reschedules that change IP

  Missing or zero `:sender_id` raises at `start_link/1`. That's
  intentional: there is no "anonymous" Sender mode.

  ### `:targets`

  Each target is a `{host, port}` tuple. Host can be an IP tuple,
  a charlist, or an atom; port is an integer. Resolution happens
  on every send so DNS changes are picked up without restart, at
  the cost of a resolver lookup per target per tick. For
  deployments where DNS reliability is uncertain, prefer
  pre-resolved IP tuples — see "Operational considerations" in
  the README.

  ## Address family and binding

    * `:inet6` (default `false`) — when `true`, opens the source
      socket with the IPv6 family AND sets `{:ipv6_v6only, true}`
      explicitly. All IP-tuple targets must be 8-element;
      `start_link/1` raises `ArgumentError` on mismatch.

    * `:ip` (default: kernel-chosen) — bind address for the
      Sender's source socket. **Operationally different from
      `PhiAccrualUdp.Listener`'s `:ip`.** The Listener's `:ip`
      only filters incoming traffic; the Sender's `:ip` sets the
      source address of outbound packets, which affects the
      kernel's routing-table choice. A misconfigured `:ip` on the
      Sender can cause packets to fail delivery silently (wrong
      gateway, no route to host). Ensure the configured source
      is on a routable path to all targets.

      During v1/v2 migration, changing the Sender's `:ip` also
      changes the `{:peer, ip, port}` identity seen by v1
      receivers (the v1 default node resolver keys on source
      address). v2 deployments using `:sender_id` are unaffected
      — that's one of the reasons `:sender_id` exists.

  Dual-stack deployments run **two Senders**, one per family.
  Mixing v4 and v6 targets in a single Sender is not supported
  — `:gen_udp` cannot send to both families from a single
  family-bound socket.

  Hostname targets are not validated against `:inet6` at
  `start_link/1` (resolution happens per-send, see
  `:targets` above). A hostname that resolves to the wrong
  family will surface as a per-target
  `[:sender, :send, :error]` event with `reason:
  :eafnosupport` (or similar). IP-tuple targets are validated
  at start.

  ## Concurrency and timeouts

  On each tick, every target is sent in parallel via
  `Task.async_stream/3`. A slow target (e.g., one whose DNS
  lookup stalls) only affects itself — it does not delay sends to
  other targets in the same tick.

    * `:max_send_concurrency` — maximum number of concurrent
      sends per tick. Default: `64`. The actual concurrency for
      a given tick is `min(length(targets), max_send_concurrency)`.
    * `:send_timeout_ms` — per-target send timeout. Default:
      `max(50, div(interval_ms, 2))`. Must be strictly less than
      `:interval_ms` — `start_link/1` raises otherwise, because
      a timeout >= interval means slow targets can pile up
      across ticks. Tasks that exceed the timeout are killed and
      surfaced as `[:sender, :send, :timeout]` telemetry.

  ## Timestamp source

  By default the sender stamps packets with
  `:erlang.system_time(:millisecond)` (wall clock, NTP-corrected
  on most systems). Pass `:timestamp_fn` to override — for
  example, to use monotonic time if you don't trust the wall
  clock. Receivers do not use the packet timestamp for the EWMA;
  it is diagnostic-only.

  ## Telemetry

      [:phi_accrual_udp, :sender, :started]
        measurements: %{}
        metadata:     %{interval_ms, target_count, sender_id,
                        max_send_concurrency, send_timeout_ms,
                        inet6, ip}
        # ip is nil when not explicitly set (kernel-chosen source)

      [:phi_accrual_udp, :sender, :send, :ok]
        measurements: %{duration}
        metadata:     %{target, sender_id}
        # one event per successful send per target per tick
        # duration in native time units (see System.convert_time_unit/3)
        # HIGH VOLUME: 1 event per target per tick. Subscribe only
        # if you need per-target latency histograms.

      [:phi_accrual_udp, :sender, :send, :error]
        measurements: %{duration}
        metadata:     %{target, sender_id, reason}
        # reason is whatever :gen_udp.send/4 returned, e.g.
        # :ehostunreach, :enetunreach, :emsgsize.

      [:phi_accrual_udp, :sender, :send, :timeout]
        measurements: %{duration}
        metadata:     %{target, sender_id}
        # the Task was killed by :send_timeout_ms. duration is the
        # configured timeout, in native time units.

      [:phi_accrual_udp, :sender, :tick]
        measurements: %{sent, errors, timeouts, duration}
        metadata:     %{sender_id}
        # aggregate counts across all targets for this tick
        # sent + errors + timeouts == target_count
        # duration is wall-clock of the parallel send phase, native units
  """

  use GenServer
  require Logger

  alias PhiAccrualUdp.Packet

  @default_interval_ms 1_000
  @default_max_concurrency 64
  @max_u64 0xFFFFFFFFFFFFFFFF

  @type target :: {:inet.ip_address() | charlist() | atom(), :inet.port_number()}

  @type opts :: [
          sender_id: pos_integer(),
          targets: [target()],
          interval_ms: pos_integer(),
          send_timeout_ms: pos_integer(),
          max_send_concurrency: pos_integer(),
          inet6: boolean(),
          ip: :inet.ip_address(),
          timestamp_fn: (-> non_neg_integer()),
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
  Sender's own options. Useful for running multiple Sender
  instances under one supervisor — e.g., one per family for
  dual-stack deployments, or one per logical target group:

      children = [
        {PhiAccrualUdp.Sender,
          sender_id: 0xA1, targets: v4_peers, id: :sender_v4},
        {PhiAccrualUdp.Sender,
          sender_id: 0xA1, targets: v6_peers,
          inet6: true, id: :sender_v6}
      ]

  Defaults: `id: PhiAccrualUdp.Sender`, `restart: :permanent`,
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

  @impl true
  def init(opts) do
    sender_id = validate_sender_id!(opts)
    targets = Keyword.get(opts, :targets, [])
    interval = validate_interval!(opts)
    send_timeout = validate_send_timeout!(opts, interval)
    max_concurrency = validate_max_concurrency!(opts)
    inet6 = Keyword.get(opts, :inet6, false)
    ip_opt = Keyword.fetch(opts, :ip)
    ts_fn = Keyword.get(opts, :timestamp_fn, fn -> :erlang.system_time(:millisecond) end)

    validate_target_families!(targets, inet6)

    open_opts =
      [:binary, active: false]
      |> maybe_add_inet6(inet6)
      |> maybe_add_ip(ip_opt)

    case :gen_udp.open(0, open_opts) do
      {:ok, socket} ->
        :telemetry.execute(
          [:phi_accrual_udp, :sender, :started],
          %{},
          %{
            interval_ms: interval,
            target_count: length(targets),
            sender_id: sender_id,
            max_send_concurrency: max_concurrency,
            send_timeout_ms: send_timeout,
            inet6: inet6,
            ip: elem_or_nil(ip_opt)
          }
        )

        schedule(interval)

        {:ok,
         %{
           socket: socket,
           sender_id: sender_id,
           targets: targets,
           interval: interval,
           send_timeout_ms: send_timeout,
           max_concurrency: max_concurrency,
           ts_fn: ts_fn
         }}

      {:error, reason} ->
        {:stop, {:udp_open_failed, reason}}
    end
  end

  defp validate_target_families!(targets, inet6) do
    Enum.each(targets, fn
      {host, _port} = target when is_tuple(host) ->
        case {inet6, tuple_size(host)} do
          {true, 4} ->
            raise ArgumentError, """
            :inet6 is true but target #{inspect(target)} uses a 4-element
            IPv4 tuple.

            Either set :inet6 to false (default) for v4 targets, or
            convert this target to an 8-element IPv6 tuple. Mixed-family
            deployments run two Senders, one per family.
            """

          {false, 8} ->
            raise ArgumentError, """
            :inet6 is false (default) but target #{inspect(target)} uses
            an 8-element IPv6 tuple.

            Either set :inet6 to true for v6 targets, or convert this
            target to a 4-element IPv4 tuple. Mixed-family deployments
            run two Senders, one per family.
            """

          {_, n} when n != 4 and n != 8 ->
            raise ArgumentError,
                  "target #{inspect(target)} has an invalid IP tuple " <>
                    "size (#{n}); expected 4 (v4) or 8 (v6)"

          _ ->
            :ok
        end

      _ ->
        # Hostname (charlist/string/atom) — not validated at start.
        # Resolution happens per-send; mismatches surface as
        # [:sender, :send, :error] telemetry with the resolver's reason.
        :ok
    end)
  end

  defp maybe_add_inet6(opts, true), do: [:inet6, {:ipv6_v6only, true} | opts]
  defp maybe_add_inet6(opts, false), do: opts

  defp maybe_add_ip(opts, {:ok, ip}), do: [{:ip, ip} | opts]
  defp maybe_add_ip(opts, :error), do: opts

  defp elem_or_nil({:ok, v}), do: v
  defp elem_or_nil(:error), do: nil

  defp validate_sender_id!(opts) do
    case Keyword.fetch(opts, :sender_id) do
      {:ok, id} when is_integer(id) and id > 0 and id <= @max_u64 ->
        id

      {:ok, bad} ->
        raise ArgumentError, """
        :sender_id must be a non-zero unsigned 64-bit integer (1..#{@max_u64}).
        Got: #{inspect(bad)}
        """

      :error ->
        raise ArgumentError, """
        :sender_id is required.

        Pick a stable non-zero u64 identifier for this node. The receiver
        uses this value as the default node identity, so it must survive
        Sender restarts and IP changes. See the Sender moduledoc and the
        README "Operational considerations" section.
        """
    end
  end

  defp validate_interval!(opts) do
    case Keyword.get(opts, :interval_ms, @default_interval_ms) do
      n when is_integer(n) and n > 0 ->
        n

      bad ->
        raise ArgumentError,
              ":interval_ms must be a positive integer, got: #{inspect(bad)}"
    end
  end

  defp validate_send_timeout!(opts, interval) do
    default = max(50, div(interval, 2))

    timeout =
      case Keyword.get(opts, :send_timeout_ms, default) do
        n when is_integer(n) and n > 0 ->
          n

        bad ->
          raise ArgumentError,
                ":send_timeout_ms must be a positive integer, got: #{inspect(bad)}"
      end

    if timeout >= interval do
      raise ArgumentError, """
      :send_timeout_ms (#{timeout}) must be strictly less than :interval_ms (#{interval}).

      A timeout equal to or greater than the tick interval lets slow
      targets pile up across ticks. Lower the timeout, raise the
      interval, or both.
      """
    end

    timeout
  end

  defp validate_max_concurrency!(opts) do
    case Keyword.get(opts, :max_send_concurrency, @default_max_concurrency) do
      n when is_integer(n) and n > 0 ->
        n

      bad ->
        raise ArgumentError,
              ":max_send_concurrency must be a positive integer, got: #{inspect(bad)}"
    end
  end

  @impl true
  def handle_info(:tick, state) do
    packet = Packet.encode(state.sender_id, state.ts_fn.())

    tick_start = System.monotonic_time()
    task_results = run_sends(state, packet)
    tick_duration = System.monotonic_time() - tick_start

    timeout_native =
      System.convert_time_unit(state.send_timeout_ms, :millisecond, :native)

    {sent, errors, timeouts} =
      state.targets
      |> Enum.zip(task_results)
      |> Enum.reduce({0, 0, 0}, fn pair, acc ->
        emit_per_target(pair, timeout_native, state.sender_id, acc)
      end)

    :telemetry.execute(
      [:phi_accrual_udp, :sender, :tick],
      %{sent: sent, errors: errors, timeouts: timeouts, duration: tick_duration},
      %{sender_id: state.sender_id}
    )

    schedule(state.interval)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp run_sends(%{targets: []}, _packet), do: []

  defp run_sends(state, packet) do
    socket = state.socket

    state.targets
    |> Task.async_stream(
      fn {host, port} ->
        start = System.monotonic_time()
        result = :gen_udp.send(socket, host, port, packet)
        {result, System.monotonic_time() - start}
      end,
      max_concurrency: min(length(state.targets), state.max_concurrency),
      timeout: state.send_timeout_ms,
      on_timeout: :kill_task,
      ordered: true
    )
    |> Enum.to_list()
  end

  defp emit_per_target({target, {:ok, {:ok, duration}}}, _to_native, sender_id, {ok, err, tmo}) do
    :telemetry.execute(
      [:phi_accrual_udp, :sender, :send, :ok],
      %{duration: duration},
      %{target: target, sender_id: sender_id}
    )

    {ok + 1, err, tmo}
  end

  defp emit_per_target(
         {target, {:ok, {{:error, reason}, duration}}},
         _to_native,
         sender_id,
         {ok, err, tmo}
       ) do
    :telemetry.execute(
      [:phi_accrual_udp, :sender, :send, :error],
      %{duration: duration},
      %{target: target, sender_id: sender_id, reason: reason}
    )

    {ok, err + 1, tmo}
  end

  defp emit_per_target({target, {:exit, :timeout}}, timeout_native, sender_id, {ok, err, tmo}) do
    :telemetry.execute(
      [:phi_accrual_udp, :sender, :send, :timeout],
      %{duration: timeout_native},
      %{target: target, sender_id: sender_id}
    )

    {ok, err, tmo + 1}
  end

  defp emit_per_target({target, {:exit, reason}}, _to_native, sender_id, {ok, err, tmo}) do
    # Task crashed for a non-timeout reason — surface as :error with
    # tagged reason. duration is 0 because we don't know how long the
    # crashed task ran.
    :telemetry.execute(
      [:phi_accrual_udp, :sender, :send, :error],
      %{duration: 0},
      %{target: target, sender_id: sender_id, reason: {:exit, reason}}
    )

    {ok, err + 1, tmo}
  end

  @impl true
  def terminate(_reason, state) do
    if state[:socket], do: :gen_udp.close(state.socket)
    :ok
  end

  defp schedule(ms), do: Process.send_after(self(), :tick, ms)
end
