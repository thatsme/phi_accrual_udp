defmodule PhiAccrualUdp.Packet do
  @moduledoc """
  Wire format codec for `phi_accrual_udp` heartbeats.

  ## Formats

  ### v2 (current, 20 bytes)

      <<magic::16, version::8, flags::8, sender_id::64-unsigned, timestamp::64-unsigned>>

    * `magic` — `0xCEA6`. Two-byte magic identifying a phi_accrual UDP
      heartbeat. Anything else is rejected at decode time before
      reaching the estimator.
    * `version` — `0x02` for this format. Receivers reject unknown
      versions at decode time.
    * `flags` — reserved, MUST be `0` in v2. All 8 bits unassigned;
      future versions may allocate them.
    * `sender_id` — operator-supplied non-zero unsigned 64-bit
      identifier for this node. `0` is reserved and rejected at
      decode time (`:reserved_sender_id`). The listener uses
      `sender_id` (not packet source IP/port) as the default node
      identity, removing the ephemeral-port coupling that bites
      under restarts and NAT timeouts.
    * `timestamp` — 64-bit unsigned milliseconds, sender's choice
      of clock. **The receiver does NOT use this for the EWMA** —
      it uses local monotonic receipt time. The packet timestamp
      is diagnostic-only (e.g., one-way delay when NTP-synced).

  ### v1 (legacy, 12 bytes)

      <<magic::16, version::8, flags::8, timestamp::64-unsigned>>

  Retained for dual-decode through the entire 1.x series. Removed
  in 2.0. Listener accepts v1 packets gracefully; senders shipped
  with `phi_accrual_udp` 1.x do not emit v1.

  Decoded v1 packets carry `sender_id: nil` to signal absence of
  identity. The default node resolver maps these to
  `{:peer, ip, port}`.

  ## Why magic + version + flags

  Three bytes of overhead earn:

    * **Early reject of garbage.** A misconfigured sender or stray
      UDP packet on the listener port doesn't corrupt estimator
      state — it gets dropped with a
      `[:phi_accrual_udp, :decode, :error]` telemetry event.
    * **Format evolution.** Adding fields means bumping `version`
      and adding a new decode clause. v1 sat in production through
      the alpha and is now transitioning to v2 the same way v2
      will transition to v3 if needed.
    * **Operator visibility.** `tcpdump -X` shows `cea6 02...` for
      v2 traffic — distinguishable from random UDP noise at a
      glance.

  ## Clock discipline (read this)

  The packet timestamp is provenance, not signal. The receiver
  calls `:erlang.monotonic_time(:millisecond)` at receipt time
  and passes that to `PhiAccrual.observe/2`. Using the packet
  timestamp for the EWMA breaks clock discipline (sender and
  receiver clocks are uncorrelated in general) and corrupts
  the detector.
  """

  @magic 0xCEA6
  @version_v1 1
  @version_v2 2
  @current_version @version_v2

  @size_v1 12
  @size_v2 20

  @max_u64 0xFFFFFFFFFFFFFFFF

  @type wire_version :: 1 | 2

  @type decode_reason ::
          :wrong_size
          | :bad_magic
          | :unsupported_version
          | :reserved_flags_set
          | :reserved_sender_id

  @type t :: %__MODULE__{
          version: wire_version(),
          flags: 0,
          sender_id: non_neg_integer() | nil,
          timestamp_ms: non_neg_integer()
        }

  defstruct version: @current_version, flags: 0, sender_id: nil, timestamp_ms: 0

  @doc """
  Encode a v2 heartbeat packet.

  `sender_id` must be a non-zero unsigned 64-bit integer. `0` is
  reserved at the wire-format level and will be rejected by any
  receiver.

  `timestamp_ms` is whatever the sender records for diagnostic
  purposes — typically `:erlang.system_time(:millisecond)` if
  NTP-synced operators want one-way delay. Receivers do not use
  this field for the EWMA.
  """
  @spec encode(pos_integer(), non_neg_integer(), keyword()) :: <<_::160>>
  def encode(sender_id, timestamp_ms, opts \\ [])
      when is_integer(sender_id) and sender_id > 0 and sender_id <= @max_u64 and
             is_integer(timestamp_ms) and timestamp_ms >= 0 and timestamp_ms <= @max_u64 do
    flags = Keyword.get(opts, :flags, 0)

    <<@magic::16, @version_v2::8, flags::8, sender_id::64-unsigned,
      timestamp_ms::64-unsigned>>
  end

  @doc """
  Decode a heartbeat packet. Dispatches by size, then by version.

  Returns `{:ok, %Packet{}}` on success, or `{:error, reason}` for
  malformed input.

  Reasons:

    * `:wrong_size` — packet is not 12 (v1) or 20 (v2) bytes
    * `:bad_magic` — first two bytes do not match `0xCEA6`
    * `:unsupported_version` — version byte does not match the size
      (or is otherwise unknown)
    * `:reserved_flags_set` — flag bits other than 0
    * `:reserved_sender_id` — v2 packet with `sender_id == 0`
  """
  @spec decode(binary()) :: {:ok, t()} | {:error, decode_reason()}
  def decode(bin) when is_binary(bin) and byte_size(bin) == @size_v2, do: decode_v2(bin)
  def decode(bin) when is_binary(bin) and byte_size(bin) == @size_v1, do: decode_v1(bin)
  def decode(bin) when is_binary(bin), do: {:error, :wrong_size}

  defp decode_v2(<<@magic::16, @version_v2::8, 0::8, 0::64, _ts::64>>),
    do: {:error, :reserved_sender_id}

  defp decode_v2(<<@magic::16, @version_v2::8, 0::8, sid::64-unsigned, ts::64-unsigned>>),
    do: {:ok, %__MODULE__{version: @version_v2, flags: 0, sender_id: sid, timestamp_ms: ts}}

  defp decode_v2(<<@magic::16, @version_v2::8, _nonzero::8, _::64, _::64>>),
    do: {:error, :reserved_flags_set}

  defp decode_v2(<<@magic::16, _v::8, _::binary>>),
    do: {:error, :unsupported_version}

  defp decode_v2(_),
    do: {:error, :bad_magic}

  defp decode_v1(<<@magic::16, @version_v1::8, 0::8, ts::64-unsigned>>),
    do: {:ok, %__MODULE__{version: @version_v1, flags: 0, sender_id: nil, timestamp_ms: ts}}

  defp decode_v1(<<@magic::16, @version_v1::8, _nonzero::8, _::64>>),
    do: {:error, :reserved_flags_set}

  defp decode_v1(<<@magic::16, _v::8, _::binary>>),
    do: {:error, :unsupported_version}

  defp decode_v1(_),
    do: {:error, :bad_magic}

  @doc """
  Wire size in bytes for a given wire version.

      iex> PhiAccrualUdp.Packet.size(:v1)
      12
      iex> PhiAccrualUdp.Packet.size(:v2)
      20
  """
  @spec size(:v1 | :v2) :: 12 | 20
  def size(:v1), do: @size_v1
  def size(:v2), do: @size_v2

  @doc "Current wire version emitted by `encode/3`."
  @spec current_version() :: 2
  def current_version, do: @current_version
end
