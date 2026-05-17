defmodule PhiAccrualUdp do
  @moduledoc """
  Dedicated UDP socket source for [`phi_accrual`](https://hex.pm/packages/phi_accrual).

  Escapes BEAM distribution head-of-line blocking that affects the
  bundled `PhiAccrual.Source.DistributionPing` reference source.
  UDP heartbeats travel on their own socket, so a backed-up
  distribution port cannot delay them.

  ## Components

    * `PhiAccrualUdp.Packet` — wire format codec. v2 (20 bytes)
      is the current format; v1 (12 bytes) is dual-decoded
      throughout 1.x for graceful migration.
    * `PhiAccrualUdp.Listener` — UDP receiver; resolves each
      packet to a node identity and calls `PhiAccrual.observe/2`
      with local receipt time.
    * `PhiAccrualUdp.Sender` — periodic UDP transmitter to a list
      of targets. Requires a stable `:sender_id`.

  ## Quick start

      # In your supervision tree
      children = [
        {PhiAccrualUdp.Listener, port: 4370},
        {PhiAccrualUdp.Sender,
          sender_id: 0xA1B2C3D4_E5F60718,
          targets: [{{10, 0, 0, 2}, 4370}, {{10, 0, 0, 3}, 4370}],
          interval_ms: 1_000}
      ]

  ## Clock discipline

  This package preserves `phi_accrual`'s clock discipline: the
  detector reasons only about local monotonic receipt times. The
  packet timestamp field is diagnostic-only — it is NOT used for
  the EWMA. See `PhiAccrualUdp.Packet` moduledoc.
  """
end
