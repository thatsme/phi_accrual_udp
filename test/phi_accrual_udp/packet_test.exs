defmodule PhiAccrualUdp.PacketTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias PhiAccrualUdp.Packet

  describe "encode/2 + decode/1" do
    test "round-trips a basic timestamp" do
      bin = Packet.encode(1_234_567_890)
      assert byte_size(bin) == Packet.size()
      assert {:ok, %Packet{version: 1, flags: 0, timestamp_ms: 1_234_567_890}} = Packet.decode(bin)
    end

    test "round-trips zero timestamp" do
      assert {:ok, %Packet{timestamp_ms: 0}} = Packet.decode(Packet.encode(0))
    end

    test "round-trips max u64 timestamp" do
      max = 0xFFFFFFFFFFFFFFFF
      assert {:ok, %Packet{timestamp_ms: ^max}} = Packet.decode(Packet.encode(max))
    end

    property "any non-negative u64 timestamp round-trips" do
      check all(ts <- integer(0..0xFFFFFFFFFFFFFFFF)) do
        assert {:ok, %Packet{timestamp_ms: ^ts}} = Packet.decode(Packet.encode(ts))
      end
    end
  end

  describe "decode/1 error cases" do
    test ":wrong_size for short packets" do
      assert {:error, :wrong_size} = Packet.decode(<<>>)
      assert {:error, :wrong_size} = Packet.decode(<<0xCE>>)
      assert {:error, :wrong_size} = Packet.decode(<<0xCE, 0xA6, 1, 0, 0::32>>)
    end

    test ":wrong_size for long packets" do
      assert {:error, :wrong_size} = Packet.decode(<<0xCEA6::16, 1::8, 0::8, 0::64, 0::8>>)
    end

    test ":bad_magic for valid-size packets with wrong magic" do
      assert {:error, :bad_magic} = Packet.decode(<<0xDEAD::16, 1::8, 0::8, 0::64>>)
    end

    test ":unsupported_version for valid magic but version != 1" do
      assert {:error, :unsupported_version} = Packet.decode(<<0xCEA6::16, 2::8, 0::8, 0::64>>)
      assert {:error, :unsupported_version} = Packet.decode(<<0xCEA6::16, 99::8, 0::8, 0::64>>)
    end

    test ":reserved_flags_set when any flag bit is non-zero in v1" do
      assert {:error, :reserved_flags_set} = Packet.decode(<<0xCEA6::16, 1::8, 1::8, 0::64>>)
      assert {:error, :reserved_flags_set} = Packet.decode(<<0xCEA6::16, 1::8, 0xFF::8, 0::64>>)
    end
  end

  describe "size/0" do
    test "is 12 bytes" do
      assert Packet.size() == 12
    end

    test "matches actual encoded size" do
      assert byte_size(Packet.encode(0)) == Packet.size()
      assert byte_size(Packet.encode(0xFFFFFFFFFFFFFFFF)) == Packet.size()
    end
  end
end
