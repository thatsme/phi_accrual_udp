# Changelog

All notable changes to this project are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
