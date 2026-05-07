defmodule PhiAccrualUdp.SenderTest do
  use ExUnit.Case, async: false

  alias PhiAccrualUdp.{Sender, Packet}

  defp open_listener do
    {:ok, socket} = :gen_udp.open(0, [:binary, active: true])
    {:ok, port} = :inet.port(socket)
    {socket, port}
  end

  defp subscribe(events) do
    ref = make_ref()
    id = "sender-test-#{inspect(ref)}"

    :ok =
      :telemetry.attach_many(
        id,
        events,
        fn ev, m, md, {pid, r} -> send(pid, {:event, r, ev, m, md}) end,
        {self(), ref}
      )

    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(id) end)
    ref
  end

  test "sends a valid packet to each target on tick" do
    {socket_a, port_a} = open_listener()
    {socket_b, port_b} = open_listener()

    on_exit(fn ->
      :gen_udp.close(socket_a)
      :gen_udp.close(socket_b)
    end)

    name = :"sender_#{System.unique_integer([:positive])}"

    {:ok, _pid} =
      start_supervised(
        {Sender,
         name: name,
         interval_ms: 30,
         targets: [{{127, 0, 0, 1}, port_a}, {{127, 0, 0, 1}, port_b}]}
      )

    assert_receive {:udp, ^socket_a, _ip, _port, packet_a}, 500
    assert_receive {:udp, ^socket_b, _ip, _port, packet_b}, 500

    assert {:ok, %Packet{version: 1}} = Packet.decode(packet_a)
    assert {:ok, %Packet{version: 1}} = Packet.decode(packet_b)
  end

  test "emits :sender :tick telemetry" do
    {socket, port} = open_listener()
    on_exit(fn -> :gen_udp.close(socket) end)

    name = :"sender_#{System.unique_integer([:positive])}"
    ref = subscribe([[:phi_accrual_udp, :sender, :tick]])

    {:ok, _pid} =
      start_supervised(
        {Sender, name: name, interval_ms: 30, targets: [{{127, 0, 0, 1}, port}]}
      )

    assert_receive {:event, ^ref, _, %{sent: 1, errors: 0}, _}, 500
  end

  test "uses custom timestamp_fn" do
    {socket, port} = open_listener()
    on_exit(fn -> :gen_udp.close(socket) end)

    name = :"sender_#{System.unique_integer([:positive])}"

    {:ok, _pid} =
      start_supervised(
        {Sender,
         name: name,
         interval_ms: 30,
         timestamp_fn: fn -> 7777 end,
         targets: [{{127, 0, 0, 1}, port}]}
      )

    assert_receive {:udp, ^socket, _ip, _port, packet}, 500
    assert {:ok, %Packet{timestamp_ms: 7777}} = Packet.decode(packet)
  end
end
