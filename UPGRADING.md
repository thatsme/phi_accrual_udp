# Upgrading from `0.1.x` to `1.0`

## TL;DR

1. **Upgrade receivers before senders.** `0.1.x` receivers reject `1.0`-format
   packets with `:unsupported_version`; `1.0` receivers accept both.
2. **Add `:sender_id` to every `Sender`** — a stable non-zero unsigned 64-bit
   integer. Missing `:sender_id` raises `ArgumentError` at `start_link/1`.
3. **If you have a custom `:node_resolver`**, update its arity from 2 to 3.
4. Expect every estimator to go `:stale` once during the migration cycle and
   recover after 8 samples. One-time cost.

## Upgrade order

Always upgrade `PhiAccrualUdp.Listener` instances before
`PhiAccrualUdp.Sender` instances.

  * `1.0` `Listener` dual-decodes both wire formats. `0.1.x` senders
    interoperate gracefully during the transition window.
  * `0.1.x` `Listener` only understands the legacy wire format. `1.0`
    senders emit the new format only, which `0.1.x` listeners reject
    with a `:decode :error` (reason `:unsupported_version`).

Reverse order produces a silent partition: senders transmit, receivers
reject, every estimator drifts to `:stale` simultaneously. Receiver-first
sequencing keeps heartbeats flowing throughout the rollout.

## Configuration changes

### `Sender` requires `:sender_id`

```elixir
# 0.1.x
{PhiAccrualUdp.Sender, targets: [...], interval_ms: 1_000}

# 1.0
{PhiAccrualUdp.Sender,
  sender_id: 0xA1B2C3D4_E5F60718,
  targets: [...],
  interval_ms: 1_000}
```

`sender_id` must be a non-zero unsigned 64-bit integer. Pick something
stable — a hash of your node name, a partner ID, a terminal ID. Missing
or zero raises `ArgumentError` at `start_link/1`. There is no
"anonymous" Sender mode; identity is required.

### `:node_resolver` is now 3-arity

```elixir
# 0.1.x signature: (ip, port) -> term
resolver = fn ip, port -> ... end

# 1.0 signature: (ip, port, sender_id | nil) -> term | {:reject, reason}
resolver = fn
  _ip, _port, sender_id when is_integer(sender_id) ->
    # v2 packet — sender_id is the operator-supplied identifier
    lookup_by_id(sender_id)

  ip, port, nil ->
    # v1 packet during the migration window — no sender_id available
    {:peer, ip, port}
end
```

A 2-arity resolver passed to `Listener.start_link/1` raises
`ArgumentError` at supervisor boot, pointing at this document.

Resolvers may also return `{:reject, reason}` to drop a packet without
observing it. The `Listener` emits
`[:phi_accrual_udp, :sample, :rejected]` telemetry for rejections.

### Default node identity changed shape

| Wire format | 0.1.x default identity | 1.0 default identity |
|-------------|------------------------|----------------------|
| v1 (legacy) | `{ip, port}` — bare tuple | `{:peer, ip, port}` — tagged |
| v2 (current) | (didn't exist) | `{:sender_id, sender_id}` — tagged |

Tagged-tuple defaults prevent accidental collision with other
integer-shaped node identities and let downstream code pattern-match
on the identity source.

**If you use the default resolver** (you didn't pass `:node_resolver`),
every existing estimator in `PhiAccrual`'s state goes orphaned on first
restart: it's keyed under the old `{ip, port}` shape, new packets arrive
under the new tagged shape, and the detector treats them as different
peers. The orphaned estimators eventually time out; the new estimators
cold-start in `:insufficient_data` for the first 8 samples (≈8 seconds
at the 1Hz default interval).

This is a **one-time cost at upgrade**. After the warmup, steady-state
operation resumes with the stable `sender_id`-keyed identity.

**If you use a custom resolver**, your identity shape is unchanged.
Only the default behavior shifted.

## Tracking migration progress

Every successful `[:phi_accrual_udp, :sample, :received]` event carries
a `:wire_version` metadata field (`1` or `2`). Group by it to compute
fleet migration progress:

```
v1_ratio = received_with_wire_version_1
           / (received_with_wire_version_1 + received_with_wire_version_2)
```

As senders upgrade, `received_with_wire_version_2` rises and the ratio
falls to 0. When the ratio stays at 0 across your monitoring window,
the migration is complete.

The same `:wire_version` field also appears on
`[:phi_accrual_udp, :sample, :rejected]` events — group by it if you're
filtering with a custom resolver during migration.

## Deprecation timeline

| Release | Behavior |
|---------|----------|
| `1.0`–`1.x` | `Listener` dual-decodes both wire formats. |
| `2.0` | Legacy decoder removed. Any remaining `0.1.x` senders stop being understood. |

There is no fixed date for `2.0`. The legacy decoder stays in place
until the migration cohort is fully retired.

## Other API changes

  * `Packet.encode/2` is now `Packet.encode/3` taking
    `(sender_id, timestamp_ms, opts \\ [])`.
  * `Packet.size/0` is removed in favor of `Packet.size(:v1)` (returns
    `12`) and `Packet.size(:v2)` (returns `20`).
  * `Sender` now dispatches sends in parallel via `Task.async_stream/3`.
    Per-target send timeout defaults to `max(50, div(interval_ms, 2))`
    and must be strictly less than `:interval_ms` — `start_link/1`
    raises otherwise.
  * `Sender` exposes two new options for fanout tuning:
    `:max_send_concurrency` (default `64`) and `:send_timeout_ms`.
  * IPv6 is now supported via `:inet6` (default `false`) on both
    `Listener` and `Sender`. Dual-stack deployments run two instances
    per family, not one socket per host.
  * Both `Listener` and `Sender` override `child_spec/1` to honor
    `:id`, `:restart`, and `:shutdown` directly in the keyword list —
    useful for the dual-stack pattern.

## New telemetry events

If you wire telemetry handlers, the `1.0` event set is:

  * `[:phi_accrual_udp, :sample, :rejected]` — emitted when the
    `:node_resolver` returns `{:reject, reason}`. Lets you alert on
    rejection rate.
  * `[:phi_accrual_udp, :sender, :send, :ok]` — per-target success,
    one event per target per tick. **High volume** — subscribe only
    if you need per-target latency histograms.
  * `[:phi_accrual_udp, :sender, :send, :error]` — `:gen_udp.send/4`
    refused the packet. Alert on rate.
  * `[:phi_accrual_udp, :sender, :send, :timeout]` — the per-target
    task exceeded `:send_timeout_ms`. Alert on rate.

The aggregate `[:phi_accrual_udp, :sender, :tick]` event gains
`:timeouts` and `:duration` measurements alongside the existing
`:sent` and `:errors`. `sent + errors + timeouts == target_count`.

`[:listener, :started]` and `[:sender, :started]` events now include
`:inet6` and `:ip` metadata reflecting the configured family and bind
address (the latter is `nil` when bound to all interfaces).

## New `decode :error` reason

`[:phi_accrual_udp, :decode, :error]` events may now carry
`reason: :reserved_sender_id`, emitted when a v2 packet arrives with
`sender_id == 0` (which is reserved at the wire-format level). This
is operator-visible only if a misconfigured sender or a hostile peer
emits malformed packets.
