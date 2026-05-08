# phi_accrual_udp

Dedicated UDP socket source for [`phi_accrual`](https://hex.pm/packages/phi_accrual). Escapes BEAM distribution head-of-line blocking that affects the bundled `PhiAccrual.Source.DistributionPing` reference source.

> ⚠️ **Alpha — `v0.1.x`.** Public API and wire format may change before `v1.0` based on real-deployment feedback. The packet format is deliberately conservative (magic + version + flags) to enable future evolution without breaking on-the-wire compatibility.

## Why a separate package

The core `phi_accrual` library is intentionally transport-agnostic. Heartbeat transports live in their own packages so consumers can mix and match — UDP for decision-grade detection, BEAM distribution for observability-grade, custom transports for application-specific signals. See the [phi_accrual roadmap](https://hexdocs.pm/phi_accrual/readme.html#roadmap) for the ecosystem rationale.

## Quick start

```elixir
# mix.exs
def deps do
  [
    {:phi_accrual, "~> 1.0"},
    {:phi_accrual_udp, "~> 0.1"}
  ]
end
```

In your supervision tree:

```elixir
children = [
  {PhiAccrualUdp.Listener, port: 4370},
  {PhiAccrualUdp.Sender,
    targets: [{{10, 0, 0, 2}, 4370}, {{10, 0, 0, 3}, 4370}],
    interval_ms: 1_000}
]
```

## Wire format (v1, 12 bytes fixed)

```
<<magic::16, version::8, flags::8, timestamp::64-unsigned>>

magic     = 0xCEA6   (identifies a phi_accrual UDP heartbeat)
version   = 0x01     (this format)
flags     = 0x00     (reserved, must be zero in v1)
timestamp = u64 ms   (sender's choice of clock; diagnostic only)
```

The receiver does **not** use the packet timestamp for the EWMA — it uses local monotonic receipt time, preserving `phi_accrual`'s clock discipline. The packet timestamp is diagnostic-only (e.g., one-way delay computation when NTP-synced).

## Telemetry

```
[:phi_accrual_udp, :listener, :started]
  metadata: %{port}

[:phi_accrual_udp, :listener, :passive]
  measurements: %{}
  metadata:     %{port}
  # emitted on each :udp_passive re-arm; observe ingress saturation

[:phi_accrual_udp, :sample, :received]
  measurements: %{packet_timestamp_ms}
  metadata:     %{node, peer}

[:phi_accrual_udp, :decode, :error]
  measurements: %{packet_size}
  metadata:     %{reason, peer}
  # reason ∈ [:wrong_size, :bad_magic, :unsupported_version, :reserved_flags_set]

[:phi_accrual_udp, :sender, :started]
  metadata: %{interval_ms, target_count}

[:phi_accrual_udp, :sender, :tick]
  measurements: %{sent, errors}
```

## Security

UDP is unauthenticated. Anyone who can reach the listener port can send packets that pass `Packet.decode/1` and corrupt detection. In hostile networks: bind to a private interface, firewall the port, or layer authentication via a `node_resolver` that rejects unknown peers.

## Operational considerations

### Node identity and Sender lifecycle

The default `node_resolver` returns `{ip, port}` of the packet's source. Combined with the bundled `PhiAccrualUdp.Sender` — which opens its socket on an ephemeral source port — this means:

* Every Sender restart produces a new `{ip, port}` tuple.
* The Listener treats the restarted Sender as a brand new peer.
* The previous peer's estimator goes `:stale` (false positive on a peer that's actually fine).
* The new peer's estimator restarts cold and spends 8 samples in `:insufficient_data` before φ is reported.
* Estimator state proliferates over time as Senders cycle.

The same applies under NAT session timeout (UDP NAT sessions typically expire in 30–180s; 1s heartbeats keep them warm but a brief outage can recycle them) and under container restarts that change IP.

For production deployments, supply a `:node_resolver` that maps `{ip, port}` to a stable application-level identifier — node name, hostname, partner ID, whatever your topology provides:

```elixir
resolver = fn
  {10, 0, 0, 1}, _ -> :node_a
  {10, 0, 0, 2}, _ -> :node_b
  ip, port ->
    # Reject unknown peers — also a useful security boundary.
    {:reject, {ip, port}}
end

{PhiAccrualUdp.Listener, port: 4370, node_resolver: resolver}
```

The default `{ip, port}` resolver is appropriate for development, demos, and deployments where you control the full Sender lifecycle and accept that restart = new peer.

### DNS resolution in Sender

`PhiAccrualUdp.Sender` resolves hostname targets on every tick via `:gen_udp.send/4`. This is deliberate: rolling DNS changes (cluster reconfig, container replacement) propagate without a Sender restart.

The cost is one resolver lookup per target per interval. The OS resolver caches by default, so almost all hits are local. At 50 targets and a 1-second interval that is 50 lookups/sec, almost all cached — negligible in normal operation.

The risk: if the resolver is slow or unreachable, every tick can stall in `:gen_udp.send/4`. The Sender is a single GenServer, so a slow lookup blocks all targets for that tick. Symptoms: `[:phi_accrual_udp, :sender, :tick]` telemetry shows degraded `sent` counts; receivers see heartbeat gaps and elevated φ.

For deployments where DNS reliability is uncertain, prefer pre-resolved IP tuples in the `:targets` list:

```elixir
{PhiAccrualUdp.Sender,
  targets: [{{10, 0, 0, 2}, 4370}, {{10, 0, 0, 3}, 4370}],
  interval_ms: 1_000}
```

IP tuples skip the resolver entirely. Trade off: you lose dynamic DNS updates and must restart the Sender to pick up topology changes.

## License

Apache-2.0.
