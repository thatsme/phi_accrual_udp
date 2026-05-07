defmodule PhiAccrualUdp.Packet do
  @moduledoc """
  Wire format codec for `phi_accrual_udp` heartbeats.

  ## Format (v1, 12 bytes fixed)

      <<magic::16, version::8, flags::8, timestamp::64-unsigned>>

    * `magic` — `0xCEA6`. Two-byte magic identifying a phi_accrual UDP
      heartbeat. Anything else is rejected at decode time before
      reaching the estimator.
    * `version` — `0x01` for this format. Receivers MUST reject
      unknown versions at decode time. Future versions will be parsed
      by additional `decode/1` clauses.
    * `flags` — reserved, MUST be `0` in v1. Non-zero flags cause a
      decode error in v1; future versions may relax this.
    * `timestamp` — 64-bit unsigned milliseconds, sender's choice of
      clock. **The receiver does NOT use this for the EWMA** — it
      uses local monotonic receipt time. The packet timestamp is
      diagnostic only (e.g., one-way delay when NTP-synced).

  ## Why magic + version + flags

  Three bytes of overhead earn:

    * **Early reject of garbage.** A misconfigured sender or stray UDP
      packet on the listener port doesn't corrupt estimator state — it
      gets dropped with a `[:phi_accrual_udp, :decode, :error]`
      telemetry event.
    * **Format evolution.** Adding fields (sequence number, sender id,
      priority) means bumping `version` and adding a new `decode/1`
      clause. Old senders keep working until v1 is retired.
    * **Operator visibility.** `tcpdump -X` shows `cea6 01...` for
      valid traffic — distinguishable from random UDP noise at a
      glance.

  ## Clock discipline (read this)

  The packet timestamp is provenance, not signal. The receiver MUST
  call `:erlang.monotonic_time(:millisecond)` at receipt time and
  pass that to `PhiAccrual.observe/2`. Using the packet timestamp
  for the EWMA breaks clock discipline (sender and receiver clocks
  are uncorrelated in general) and corrupts the detector.
  """

  @magic 0xCEA6
  @version 1

  @type t :: %__MODULE__{
          version: non_neg_integer(),
          flags: non_neg_integer(),
          timestamp_ms: non_neg_integer()
        }

  defstruct version: @version, flags: 0, timestamp_ms: 0

  @doc """
  Encode a heartbeat packet.

  `timestamp_ms` is whatever the sender wants to record for diagnostic
  purposes — typically `:erlang.system_time(:millisecond)` if NTP-synced
  operators want one-way delay, or `:erlang.monotonic_time(:millisecond)`
  if local correlation is enough. Receivers do not use this field for
  the EWMA.
  """
  @spec encode(non_neg_integer(), keyword()) :: binary()
  def encode(timestamp_ms, opts \\ []) when is_integer(timestamp_ms) and timestamp_ms >= 0 do
    flags = Keyword.get(opts, :flags, 0)
    <<@magic::16, @version::8, flags::8, timestamp_ms::64-unsigned>>
  end

  @doc """
  Decode a heartbeat packet. Returns `{:ok, %Packet{}}` on success, or
  `{:error, reason}` for any malformed input.

  Reasons:

    * `:wrong_size` — packet is not exactly 12 bytes
    * `:bad_magic` — first two bytes do not match the magic constant
    * `:unsupported_version` — version byte is not `1`
    * `:reserved_flags_set` — flag bits other than 0 in v1
  """
  @spec decode(binary()) :: {:ok, t()} | {:error, atom()}
  def decode(<<@magic::16, @version::8, 0::8, ts::64-unsigned>>) do
    {:ok, %__MODULE__{version: @version, flags: 0, timestamp_ms: ts}}
  end

  def decode(<<@magic::16, @version::8, _nonzero::8, _::64>>) do
    {:error, :reserved_flags_set}
  end

  def decode(<<@magic::16, v::8, _::8, _::64>>) when v != @version do
    {:error, :unsupported_version}
  end

  def decode(<<m::16, _::binary>>) when m != @magic do
    {:error, :bad_magic}
  end

  def decode(bin) when is_binary(bin), do: {:error, :wrong_size}

  @doc "Wire size of a v1 packet, in bytes."
  @spec size() :: pos_integer()
  def size, do: 12
end
