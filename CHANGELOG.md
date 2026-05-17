# Changelog

All notable changes to this project are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-05-14

**Upgrade receivers before senders.** `0.1.x` receivers reject the
new wire format with `:unsupported_version`. See
[UPGRADING.md](UPGRADING.md) for the full migration playbook.

This is the `1.0` consolidation release — public API, wire
format, telemetry schema, and default node-term shape are the
committed `1.0` contract under the versioning policy in the
[README](README.md#versioning-policy).

### Added

- **Wire format v2** (`PhiAccrualUdp.Packet`) — 20-byte format with
  `<<magic::16, version::8, flags::8, sender_id::64, timestamp::64>>`.
  Senders shipped with this release emit v2 only.
- **`Sender` `:sender_id` option** (required) — operator-supplied
  non-zero `u64`. Becomes the default node identity at the
  receiver, decoupling identity from ephemeral source port / NAT /
  container IP. Missing or zero raises `ArgumentError` at
  `start_link/1`.
- **Parallel per-tick sends** in `Sender`. Each target's
  `:gen_udp.send/4` runs in its own `Task` via
  `Task.async_stream/3`; a slow target only delays its own send.
  New options:
    - `:max_send_concurrency` (default `64`) — caps per-tick fanout.
    - `:send_timeout_ms` (default `max(50, div(interval_ms, 2))`)
      — per-target send timeout. Must be strictly less than
      `:interval_ms` or `start_link/1` raises.
- **IPv6 and interface binding** on both `Listener` and `Sender`:
    - `:inet6` (default `false`) — when `true`, the socket is
      opened with the IPv6 family AND `{:ipv6_v6only, true}` is
      set explicitly (no reliance on platform defaults).
    - `:ip` — bind address, IP tuple. On `Sender`, sets the source
      address for outbound packets (affects kernel routing); on
      `Listener`, filters incoming traffic.
  Dual-stack deployments run two instances per family. `Sender`
  validates target IP-tuple shapes against `:inet6` at
  `start_link/1`; hostname/family mismatches surface per-send via
  `[:sender, :send, :error]` telemetry.
- **`child_spec/1` overrides** on both `Listener` and `Sender`
  honor the standard supervisor options `:id`, `:restart`, and
  `:shutdown` directly in the keyword list — useful for the
  dual-stack pattern (one Listener per family under one
  supervisor).
- **New decode error reason** `:reserved_sender_id`, emitted when
  a v2 packet arrives with `sender_id == 0` (reserved at the
  wire-format level).
- **New telemetry events:**
    - `[:phi_accrual_udp, :sample, :rejected]` — emitted when the
      `:node_resolver` returns `{:reject, reason}`. Metadata:
      `%{peer, sender_id, reason, wire_version}`.
    - `[:phi_accrual_udp, :sender, :send, :ok | :error | :timeout]`
      — one event per target per tick. Measurements: `%{duration}`
      in native time units. The `:ok` variant is high-volume;
      subscribe only if you need per-target latency histograms.
- **New telemetry metadata keys:**
    - `:wire_version` (1 | 2) on `[:sample, :received]` and
      `[:sample, :rejected]`. Group by this field to track fleet
      migration progress.
    - `:sender_id` on `[:sender, :started]` and `[:sender, :tick]`.
    - `:inet6` and `:ip` on `[:listener, :started]` and
      `[:sender, :started]`.
    - `:max_send_concurrency` and `:send_timeout_ms` on
      `[:sender, :started]`.
- **New `:sender, :tick` measurements** `:timeouts` and
  `:duration` alongside the existing `:sent` and `:errors`.
  `sent + errors + timeouts == target_count`. `duration` is
  wall-clock of the parallel send phase, native time units.
- `Packet.current_version/0` returns `2`.
- Documentation: [UPGRADING.md](UPGRADING.md) covers the
  receiver-first upgrade order, configuration changes, default
  identity shape change, migration tracking, and deprecation
  timeline.
- CI: `.github/workflows/ci.yml` runs `mix hex.audit`,
  `mix compile --warnings-as-errors`, `mix test`, `mix credo`,
  and `mix dialyzer` (strict mode) across a three-job matrix:
  `{Elixir 1.15, OTP 26}`, `{1.19, OTP 26}`, `{1.19, OTP 28}`.

### Changed (breaking)

- **Default node identity** at the `Listener` is now a tagged
  tuple, distinguishing identity source:
    - v2 packets → `{:sender_id, sender_id}`
    - v1 packets → `{:peer, ip, port}` (3-tuple, flat)

  Previously v1 packets resolved to a bare `{ip, port}` 2-tuple.
  Operators using the default resolver see one cold-start per
  peer at upgrade (the old `{ip, port}`-keyed estimator goes
  `:stale`; the new tagged estimator warms up over 8 samples).
  Custom resolvers are unaffected.
- **`:node_resolver` signature** is now 3-arity:

      (ip, port, sender_id | nil) -> term | {:reject, reason}

  Third argument is `nil` for v1 packets and the integer
  `sender_id` for v2. `Listener.start_link/1` raises
  `ArgumentError` when a 2-arity resolver is supplied.
- **`Packet.encode/2`** is now `Packet.encode/3`:
  `Packet.encode(sender_id, timestamp_ms, opts \\ [])`.
- **`Packet.size/0`** is removed in favor of `Packet.size/1`
  taking `:v1` or `:v2`.

### Changed (internal)

- `@spec` contracts tightened across `Packet` to match dialyzer
  success typing — narrowings only, no behavior change:
  `decode/1` returns `{:error, decode_reason()}` enumerated alias;
  `encode/3` returns `<<_::160>>` literal binary size; `size/1`
  returns `12 | 20` literal union; `current_version/0` returns
  the literal `2`; `%Packet{}` `:flags` field is typed `0`.
- `Listener.handle_info({:udp_passive, _}, _)` now pattern-matches
  `:ok` from `:inet.setopts/2`. A failed re-arm crashes the
  Listener loudly instead of silently dropping flow control.

### Deprecated

- **Wire format v1** is dual-decoded by `Listener` throughout
  `1.x` for migration. The v1 decoder will be removed in `2.0`.

## [0.1.2] - 2026-05-08

### Documentation

- **Operational considerations section** added to the README,
  documenting two production footguns:
    - Sender restart producing a new `{ip, port}` peer identity
      from the Listener's perspective (because Sender uses
      ephemeral source ports). Recommends `:node_resolver` as the
      standard production setup.
    - DNS resolution cost and failure modes in Sender. Recommends
      pre-resolved IP tuples for deployments with unreliable DNS.
- Listener moduledoc reframes `:node_resolver` as the recommended
  production setup, not just a "static topology" option.
- Sender moduledoc expands the DNS resolution note.

No behaviour change.

## [0.1.1] - 2026-05-08

### Changed

- **Listener flow control.** `PhiAccrualUdp.Listener` now opens its
  UDP socket with `active: N` (default `N=100`, configurable via the
  `:active_count` option) instead of `active: true`. Re-arms on
  `:udp_passive`. This bounds the per-burst mailbox growth under
  packet floods.

### Added

- Telemetry event `[:phi_accrual_udp, :listener, :passive]`, emitted
  each time the listener re-arms after consuming `active_count`
  packets. Useful for observing ingress saturation.

## [0.1.0] - 2026-05-07

Initial public release. **Alpha** — public API and wire format may
change before `v1.0` based on real-deployment feedback.

### Added

- **Wire format v1** (`PhiAccrualUdp.Packet`) — 12-byte fixed format
  with magic `0xCEA6`, version `0x01`, reserved flags byte (must be
  zero in v1), and 64-bit unsigned millisecond timestamp.
- **UDP listener** (`PhiAccrualUdp.Listener`) — opens a UDP socket on
  a configurable port, decodes incoming packets, calls
  `PhiAccrual.observe/2` with local monotonic receipt time. Decode
  failures emit `[:phi_accrual_udp, :decode, :error]` telemetry with
  reason classification.
- **Periodic UDP sender** (`PhiAccrualUdp.Sender`) — sends heartbeat
  packets to a list of `{host, port}` targets at a configurable
  interval. Configurable timestamp source.
- **Custom node resolution** — listener accepts a `:node_resolver`
  function mapping `(ip, port)` to user-defined node identifiers.
  Default: `{ip, port}` tuple.
- **Telemetry schema** — `[:listener, :started]`, `[:sample, :received]`,
  `[:decode, :error]`, `[:sender, :started]`, `[:sender, :tick]`.

### Notes

- Wire format and telemetry schema are **not yet committed**. Both
  may change before `v1.0`. Magic/version/flags structure is
  deliberately chosen to permit format evolution without breaking
  on-the-wire compatibility for v1 senders.
- Receiver-driven clock discipline: the EWMA uses local monotonic
  receipt time, never the packet timestamp. This preserves
  `phi_accrual`'s contract that cross-node timestamps are
  meaningless.
