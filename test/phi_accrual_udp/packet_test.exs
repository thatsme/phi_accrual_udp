defmodule PhiAccrualUdp.PacketTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias PhiAccrualUdp.Packet

  @max_u64 0xFFFFFFFFFFFFFFFF

  # Test-only helper: hand-roll a legacy v1 binary. Production code does
  # not emit v1 — receivers dual-decode it for the duration of 1.x.
  defp encode_v1(timestamp_ms, flags \\ 0) do
    <<0xCEA6::16, 1::8, flags::8, timestamp_ms::64-unsigned>>
  end

  describe "encode/3 + decode/1 (v2)" do
    test "round-trips a typical sender_id + timestamp" do
      bin = Packet.encode(0xA1B2C3D4_E5F60718, 1_234_567_890)
      assert byte_size(bin) == Packet.size(:v2)

      assert {:ok,
              %Packet{
                version: 2,
                flags: 0,
                sender_id: 0xA1B2C3D4_E5F60718,
                timestamp_ms: 1_234_567_890
              }} = Packet.decode(bin)
    end

    test "round-trips zero timestamp" do
      assert {:ok, %Packet{timestamp_ms: 0, sender_id: 1}} =
               Packet.decode(Packet.encode(1, 0))
    end

    test "round-trips max u64 timestamp" do
      assert {:ok, %Packet{timestamp_ms: @max_u64}} =
               Packet.decode(Packet.encode(1, @max_u64))
    end

    test "round-trips max u64 sender_id" do
      assert {:ok, %Packet{sender_id: @max_u64}} =
               Packet.decode(Packet.encode(@max_u64, 0))
    end

    property "any non-zero u64 sender_id + non-negative u64 timestamp round-trips" do
      check all(
              sid <- integer(1..@max_u64),
              ts <- integer(0..@max_u64)
            ) do
        assert {:ok, %Packet{sender_id: ^sid, timestamp_ms: ^ts, version: 2}} =
                 Packet.decode(Packet.encode(sid, ts))
      end
    end
  end

  describe "encode/3 validation" do
    test "rejects sender_id = 0 at encode time" do
      assert_raise FunctionClauseError, fn -> Packet.encode(0, 0) end
    end

    test "rejects negative sender_id" do
      assert_raise FunctionClauseError, fn -> Packet.encode(-1, 0) end
    end

    test "rejects sender_id > u64" do
      assert_raise FunctionClauseError, fn -> Packet.encode(@max_u64 + 1, 0) end
    end

    test "rejects non-integer sender_id" do
      assert_raise FunctionClauseError, fn -> Packet.encode(:nope, 0) end
    end

    test "rejects negative timestamp" do
      assert_raise FunctionClauseError, fn -> Packet.encode(1, -1) end
    end
  end

  describe "decode/1 (v1 legacy)" do
    test "decodes a v1 packet with sender_id: nil" do
      assert {:ok, %Packet{version: 1, sender_id: nil, timestamp_ms: 42, flags: 0}} =
               Packet.decode(encode_v1(42))
    end

    test "decodes v1 with zero timestamp" do
      assert {:ok, %Packet{version: 1, sender_id: nil, timestamp_ms: 0}} =
               Packet.decode(encode_v1(0))
    end

    test "decodes v1 with max u64 timestamp" do
      assert {:ok, %Packet{version: 1, sender_id: nil, timestamp_ms: @max_u64}} =
               Packet.decode(encode_v1(@max_u64))
    end

    test "rejects v1 with non-zero flags" do
      assert {:error, :reserved_flags_set} = Packet.decode(encode_v1(0, 1))
      assert {:error, :reserved_flags_set} = Packet.decode(encode_v1(0, 0xFF))
    end
  end

  describe "decode/1 error cases" do
    test ":wrong_size for short packets" do
      assert {:error, :wrong_size} = Packet.decode(<<>>)
      assert {:error, :wrong_size} = Packet.decode(<<0xCE>>)
      assert {:error, :wrong_size} = Packet.decode(<<0xCE, 0xA6, 2, 0, 0::32>>)
    end

    test ":wrong_size for packets that don't match v1 (12) or v2 (20) length" do
      # 13 bytes
      assert {:error, :wrong_size} = Packet.decode(<<0xCEA6::16, 1::8, 0::8, 0::64, 0::8>>)
      # 19 bytes
      assert {:error, :wrong_size} =
               Packet.decode(<<0xCEA6::16, 2::8, 0::8, 0::64, 0::56>>)

      # 21 bytes
      assert {:error, :wrong_size} =
               Packet.decode(<<0xCEA6::16, 2::8, 0::8, 0::64, 0::64, 0::8>>)
    end

    test ":bad_magic for v2-sized packets with wrong magic" do
      assert {:error, :bad_magic} =
               Packet.decode(<<0xDEAD::16, 2::8, 0::8, 1::64, 0::64>>)
    end

    test ":bad_magic for v1-sized packets with wrong magic" do
      assert {:error, :bad_magic} = Packet.decode(<<0xDEAD::16, 1::8, 0::8, 0::64>>)
    end

    test ":unsupported_version for v2-sized packet with unknown version" do
      assert {:error, :unsupported_version} =
               Packet.decode(<<0xCEA6::16, 99::8, 0::8, 0::64, 0::64>>)

      # v1 version byte in a v2-sized packet
      assert {:error, :unsupported_version} =
               Packet.decode(<<0xCEA6::16, 1::8, 0::8, 0::64, 0::64>>)
    end

    test ":unsupported_version for v1-sized packet with unknown version" do
      assert {:error, :unsupported_version} =
               Packet.decode(<<0xCEA6::16, 99::8, 0::8, 0::64>>)

      # v2 version byte in a v1-sized packet
      assert {:error, :unsupported_version} =
               Packet.decode(<<0xCEA6::16, 2::8, 0::8, 0::64>>)
    end

    test ":reserved_flags_set in v2 when any flag bit is non-zero" do
      assert {:error, :reserved_flags_set} =
               Packet.decode(<<0xCEA6::16, 2::8, 1::8, 1::64, 0::64>>)

      assert {:error, :reserved_flags_set} =
               Packet.decode(<<0xCEA6::16, 2::8, 0xFF::8, 1::64, 0::64>>)
    end

    test ":reserved_sender_id when v2 packet has sender_id = 0" do
      assert {:error, :reserved_sender_id} =
               Packet.decode(<<0xCEA6::16, 2::8, 0::8, 0::64, 0::64>>)

      # also rejects when timestamp is non-zero
      assert {:error, :reserved_sender_id} =
               Packet.decode(<<0xCEA6::16, 2::8, 0::8, 0::64, 1_234::64>>)
    end

    test "flags-error wins over sender_id-error when both are bad" do
      # If a packet has both non-zero flags AND sender_id = 0, the
      # flags error takes precedence. Document the precedence so a
      # future refactor doesn't accidentally reorder.
      assert {:error, :reserved_flags_set} =
               Packet.decode(<<0xCEA6::16, 2::8, 1::8, 0::64, 0::64>>)
    end
  end

  describe "size/1" do
    test ":v1 is 12 bytes" do
      assert Packet.size(:v1) == 12
    end

    test ":v2 is 20 bytes" do
      assert Packet.size(:v2) == 20
    end

    test "encode/3 produces a v2-sized binary" do
      assert byte_size(Packet.encode(1, 0)) == Packet.size(:v2)
      assert byte_size(Packet.encode(@max_u64, @max_u64)) == Packet.size(:v2)
    end
  end

  describe "current_version/0" do
    test "is 2" do
      assert Packet.current_version() == 2
    end
  end
end
