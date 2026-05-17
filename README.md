# phi_accrual_udp

Dedicated UDP socket source for the
[`phi_accrual`](https://hex.pm/packages/phi_accrual) failure detector.
Heartbeats travel on their own port instead of riding BEAM
distribution, so a backed-up distribution channel cannot delay them.
Built for decision-grade failure detection — failover, leader
election, load shedding — where the cost of a false positive is high.

## Quick start

```elixir
# mix.exs
def deps do
  [
    {:phi_accrual, "~> 1.0"},
    {:phi_accrual_udp, "~> 1.0"}
  ]
end
```

In your supervision tree:

```elixir
children = [
  {PhiAccrualUdp.Listener, port: 4370},
  {PhiAccrualUdp.Sender,
    sender_id: 0xA1B2C3D4_E5F60718,
    targets: [{{10, 0, 0, 2}, 4370}, {{10, 0, 0, 3}, 4370}],
    interval_ms: 1_000}
]
```

`Listener` accepts heartbeats on UDP/4370 and feeds them into
`PhiAccrual`. `Sender` transmits a heartbeat to each target every
second. The detector uses local monotonic receipt time, never the
packet timestamp — sender and receiver clocks are uncorrelated in
general, so cross-node timestamps would corrupt the EWMA.

## What's different from `phi_accrual` alone?

`phi_accrual` is the failure detector core: it ingests observation
timestamps and produces φ values. This package is a transport — it
sends and receives UDP heartbeats and feeds them into the core via
`PhiAccrual.observe/2`. The two are split so the detector can work
with any transport: UDP for decision-grade detection, BEAM
distribution for observability-grade, custom transports for
application-specific signals.

If your detector is purely for monitoring (alerting, dashboards),
the bundled `PhiAccrual.Source.DistributionPing` is enough. If a
stalled detection means a stalled failover, you want a dedicated UDP
socket — that's this package. See the [`phi_accrual`
roadmap](https://hexdocs.pm/phi_accrual/readme.html#roadmap) for the
ecosystem rationale.

## Upgrading from `0.1.x`?

See [UPGRADING.md](UPGRADING.md). Headlines: upgrade receivers
before senders, `:sender_id` is now required, default node identity
shape changed.

## Wire format

### v2 (current, 20 bytes)

```
<<magic::16, version::8, flags::8, sender_id::64, timestamp::64>>

magic     = 0xCEA6   identifies a phi_accrual UDP heartbeat
version   = 0x02     this format
flags     = 0x00     reserved, must be zero in v2
sender_id = u64      operator-supplied non-zero identifier
timestamp = u64 ms   sender's choice of clock; diagnostic only
```

The receiver uses `sender_id` as the default node identity, not the
packet source IP/port. A stable `sender_id` survives sender restarts
(which change the ephemeral source port), NAT session recycling, and
container reschedules that change IP — all of which would otherwise
appear as estimator churn at the receiver.

The packet `timestamp` is **not** used for the EWMA. Receivers call
`:erlang.monotonic_time(:millisecond)` at receipt and pass that to
`PhiAccrual.observe/2`. The packet timestamp is diagnostic-only
(e.g., one-way delay when sender and receiver are NTP-synced).

### v1 (legacy, 12 bytes)

```
<<magic::16, version::8, flags::8, timestamp::64>>
```

v1 is accepted by `PhiAccrualUdp.Listener` throughout the 1.x series
for graceful migration from `0.1.x`. Senders shipped with 1.0 emit
v2 only. The v1 decoder is removed in 2.0. See
[UPGRADING.md](UPGRADING.md).

## Telemetry

All events live under the `[:phi_accrual_udp, ...]` namespace.

### Listener

```
[:phi_accrual_udp, :listener, :started]
  measurements: %{}
  metadata:     %{port, inet6, ip}
  # ip is nil when bound to all interfaces

[:phi_accrual_udp, :listener, :passive]
  measurements: %{}
  metadata:     %{port}
  # emitted each time the listener re-arms after consuming
  # :active_count packets; useful for observing ingress saturation

[:phi_accrual_udp, :sample, :received]
  measurements: %{packet_timestamp_ms}
  metadata:     %{node, peer, wire_version}
  # wire_version :: 1 | 2 — group by this field to track
  # fleet migration progress

[:phi_accrual_udp, :sample, :rejected]
  measurements: %{}
  metadata:     %{peer, sender_id, reason, wire_version}
  # emitted when :node_resolver returns {:reject, reason}.
  # sender_id is nil for rejected v1 packets.

[:phi_accrual_udp, :decode, :error]
  measurements: %{packet_size}
  metadata:     %{reason, peer}
  # reason ∈ [:wrong_size, :bad_magic, :unsupported_version,
  #           :reserved_flags_set, :reserved_sender_id]
```

### Sender

```
[:phi_accrual_udp, :sender, :started]
  measurements: %{}
  metadata:     %{interval_ms, target_count, sender_id,
                  max_send_concurrency, send_timeout_ms,
                  inet6, ip}

[:phi_accrual_udp, :sender, :send, :ok]
  measurements: %{duration}
  metadata:     %{target, sender_id}
  # one event per successful send per target per tick.
  # HIGH VOLUME — subscribe only for per-target latency histograms.
  # duration in native time units (use System.convert_time_unit/3).

[:phi_accrual_udp, :sender, :send, :error]
  measurements: %{duration}
  metadata:     %{target, sender_id, reason}
  # reason is what :gen_udp.send/4 returned (:ehostunreach, etc.)

[:phi_accrual_udp, :sender, :send, :timeout]
  measurements: %{duration}
  metadata:     %{target, sender_id}
  # task was killed by :send_timeout_ms

[:phi_accrual_udp, :sender, :tick]
  measurements: %{sent, errors, timeouts, duration}
  metadata:     %{sender_id}
  # aggregate per tick. sent + errors + timeouts == target_count.
  # duration is wall-clock of the parallel send phase, native units.
```

## Security

UDP is unauthenticated. Anyone reachable on the listener port can
send packets that pass `Packet.decode/1` and feed observations into
the estimator. With v2, a hostile peer can also mint arbitrary
`sender_id` values, creating unbounded cold-start estimator state at
the receiver — each fake ID spends 8 samples in `:insufficient_data`
before φ is reported, and the state accumulates.

In hostile networks:

  * **Bind to a private interface** — pass `:ip` to `Listener`,
    matching your private VLAN's address.
  * **Firewall the listener port** — restrict source IPs at the
    network layer.
  * **Reject unknown peers in `:node_resolver`** — return
    `{:reject, reason}` for `sender_id` values not in your
    allowlist. The library emits `[:sample, :rejected]` telemetry
    for rejected packets so you can alert on the rate.

The `:node_resolver` doubles as an application-layer authentication
boundary: it sees every successful decode and chooses whether to
feed it into the detector.

## Operational considerations

### Node identity via `:sender_id`

The default resolver returns `{:sender_id, id}` for v2 packets — your
operator-supplied `sender_id` becomes the key in `PhiAccrual`'s
estimator state. Identity survives sender restarts, NAT recycling,
and IP changes, which is the reason `sender_id` is required at
`start_link/1`.

For v1 packets during the 0.1.x → 1.x migration, the default
resolver returns `{:peer, ip, port}` — the source IP and port.
That's the failure mode `sender_id` was designed to fix: a v1
sender that restarts shows up as a brand-new peer (its ephemeral
source port changed), the old estimator goes `:stale`, the new one
cold-starts from `:insufficient_data`. Once that sender is on v2,
restarts no longer churn identity.

Custom resolvers receive
`(ip, port, sender_id | nil) -> term | {:reject, reason}` and can
map identity however your topology requires:

```elixir
resolver = fn
  _ip, _port, sender_id when is_integer(sender_id) ->
    Map.get(known_senders, sender_id) || {:reject, :unknown_sender}

  ip, port, nil ->
    # v1 packet during migration window
    {:peer, ip, port}
end

{PhiAccrualUdp.Listener, port: 4370, node_resolver: resolver}
```

Resolvers run synchronously in the Listener process on every packet.
Keep them cheap — use `:persistent_term` for static lookup tables,
ETS for dynamic ones. Avoid `GenServer.call/2`, network I/O, or
anything else that can block; a slow resolver stalls packet
processing for every peer, not just the one being resolved.

Exceptions raised by the resolver crash the `Listener`. The
supervisor restarts it but every estimator's state resets. Use
`{:reject, reason}` for rejection paths, not exceptions.

### DNS resolution in `Sender`

`Sender` resolves hostname targets on every tick via
`:gen_udp.send/4`. This is deliberate: rolling DNS changes (cluster
reconfig, container replacement) propagate without a Sender
restart. The cost is one resolver lookup per target per interval;
the OS resolver caches by default, so almost all hits are local.

If the resolver is slow, the Sender's parallel-send architecture
contains the blast radius. Each target's send runs in its own
`Task`; a stalled DNS lookup on one target only delays its own
send, not the others. After `:send_timeout_ms` (default
`max(50, div(interval_ms, 2))`) the task is killed and surfaced as
a `[:sender, :send, :timeout]` event with the offending target in
metadata.

For deployments where DNS is uncertain enough to skip entirely, use
pre-resolved IP tuples:

```elixir
{PhiAccrualUdp.Sender,
  sender_id: 0xA1,
  targets: [{{10, 0, 0, 2}, 4370}, {{10, 0, 0, 3}, 4370}],
  interval_ms: 1_000}
```

Trade-off: you lose dynamic DNS updates and must restart the
Sender to pick up topology changes.

### Dual-stack deployments (IPv4 + IPv6)

The library does not multiplex address families on a single socket.
When `inet6: true` is set, the Listener and Sender are strictly
IPv6 — `{:ipv6_v6only, true}` is set explicitly to avoid the
platform-default divergence between Linux, BSD, and Windows on
this socket option.

For dual-stack deployments, run two `Listener` and two `Sender`
instances under the same supervisor:

```elixir
children = [
  {PhiAccrualUdp.Listener, port: 4370, id: :listener_v4},
  {PhiAccrualUdp.Listener,
    port: 4370, inet6: true, id: :listener_v6},
  {PhiAccrualUdp.Sender,
    sender_id: 0xA1, targets: v4_peers, id: :sender_v4},
  {PhiAccrualUdp.Sender,
    sender_id: 0xA1, targets: v6_peers,
    inet6: true, id: :sender_v6}
]
```

Both `Listener`s and both `Sender`s can share the same `:sender_id`.
The resolver sees the same identity regardless of which family the
packet arrived on, so a peer reachable via both v4 and v6 produces
one estimator entry, not two.

(OS-level dual-stack via v4-mapped-v6 addresses would change the
`{:peer, ip, port}` tuple shape for v4 peers and break the
stable-identity contract. That's why this library doesn't do it.)

### Note on `Sender` `:ip` vs `Listener` `:ip`

These options look symmetric but are operationally different:

  * `Listener`'s `:ip` filters incoming traffic — packets to other
    interfaces are ignored. Pure ingress restriction.
  * `Sender`'s `:ip` sets the source address of outbound packets,
    which affects the kernel's routing-table decision. A
    misconfigured `:ip` on Sender can cause packets to fail
    delivery silently (wrong gateway, no route to host).

Verify that the configured Sender `:ip` is on a routable path to
all targets before deploying.

## Versioning policy

This package follows [Semantic Versioning](https://semver.org/)
starting with `1.0`. What counts as which kind of change:

**MINOR** (additive, non-breaking):

  * A new wire-format version alongside existing ones, with the
    existing decoders retained for a deprecation window.
  * New optional `start_link/1` keyword options. New public
    functions or modules.
  * New telemetry events. New measurements or metadata keys on
    existing events.
  * Allocating bits in the reserved `flags` byte, provided they
    have not previously been documented as carrying meaning.
  * New atoms added to enumerated `@type` aliases (e.g., a new
    `decode_reason`).

**MAJOR** (breaking):

  * Removing a wire-format version's decoder (ending a
    deprecation window).
  * Changing public function arities or callback signatures
    (notably `:node_resolver`). Adding required options to
    `start_link/1`. Removing or renaming public functions.
  * Removing telemetry events. Removing measurement or metadata
    keys. Changing the unit of an existing measurement.
  * Changing the shape of the default node-identity term passed
    to `PhiAccrual.observe/2`.
  * Changing the meaning of a previously-documented `flags` bit
    or any other on-wire field.

**Not covered by SemVer:** performance, internal implementation,
`@type` alias narrowings that match dialyzer success typing
without changing observable behavior, error message wording,
internal struct fields not exposed via `@type t()`.

The [CHANGELOG](CHANGELOG.md) is authoritative for what changed
in each release. The [UPGRADING.md](UPGRADING.md) document
covers MAJOR-version migration paths.

## License

Apache-2.0.
