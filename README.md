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

## License

Apache-2.0.
