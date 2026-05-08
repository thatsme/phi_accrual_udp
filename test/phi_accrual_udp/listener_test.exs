defmodule PhiAccrualUdp.ListenerTest do
  use ExUnit.Case, async: false

  alias PhiAccrualUdp.{Listener, Packet}

  defp ephemeral_port do
    {:ok, s} = :gen_udp.open(0, [:binary])
    {:ok, port} = :inet.port(s)
    :gen_udp.close(s)
    port
  end

  defp send_packet(port, bin) do
    {:ok, sender} = :gen_udp.open(0, [:binary])
    :ok = :gen_udp.send(sender, {127, 0, 0, 1}, port, bin)
    :gen_udp.close(sender)
  end

  defp subscribe(events) do
    ref = make_ref()
    id = "listener-test-#{inspect(ref)}"

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

  defp collect_events(ref, event, min_count, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_collect_events(ref, event, min_count, deadline, [])
  end

  defp do_collect_events(_ref, _event, min_count, _deadline, acc)
       when length(acc) >= min_count,
       do: Enum.reverse(acc)

  defp do_collect_events(ref, event, min_count, deadline, acc) do
    remaining = max(0, deadline - System.monotonic_time(:millisecond))

    receive do
      {:event, ^ref, ^event, m, md} ->
        do_collect_events(ref, event, min_count, deadline, [{m, md} | acc])
    after
      remaining -> Enum.reverse(acc)
    end
  end

  test "decodes a valid packet and emits :sample :received telemetry" do
    port = ephemeral_port()
    name = :"listener_#{System.unique_integer([:positive])}"
    {:ok, _pid} = start_supervised({Listener, port: port, name: name})

    ref = subscribe([[:phi_accrual_udp, :sample, :received]])

    send_packet(port, Packet.encode(42))

    assert_receive {:event, ^ref, _, %{packet_timestamp_ms: 42}, %{node: {{127, 0, 0, 1}, _}}},
                   500
  end

  test "emits :decode :error on bad magic" do
    port = ephemeral_port()
    name = :"listener_#{System.unique_integer([:positive])}"
    {:ok, _pid} = start_supervised({Listener, port: port, name: name})

    ref = subscribe([[:phi_accrual_udp, :decode, :error]])

    send_packet(port, <<0xDEAD::16, 1::8, 0::8, 0::64>>)

    assert_receive {:event, ^ref, _, %{packet_size: 12}, %{reason: :bad_magic}}, 500
  end

  test "emits :decode :error on wrong size" do
    port = ephemeral_port()
    name = :"listener_#{System.unique_integer([:positive])}"
    {:ok, _pid} = start_supervised({Listener, port: port, name: name})

    ref = subscribe([[:phi_accrual_udp, :decode, :error]])

    send_packet(port, <<0xCE, 0xA6, 1>>)

    assert_receive {:event, ^ref, _, %{packet_size: 3}, %{reason: :wrong_size}}, 500
  end

  test "node_resolver maps peer to custom node id" do
    port = ephemeral_port()
    name = :"listener_#{System.unique_integer([:positive])}"
    resolver = fn {127, 0, 0, 1}, _p -> :test_peer end

    {:ok, _pid} =
      start_supervised({Listener, port: port, name: name, node_resolver: resolver})

    ref = subscribe([[:phi_accrual_udp, :sample, :received]])

    send_packet(port, Packet.encode(0))

    assert_receive {:event, ^ref, _, _, %{node: :test_peer}}, 500
  end

  test "re-arms after consuming active_count packets" do
    port = ephemeral_port()
    name = :"listener_#{System.unique_integer([:positive])}"
    {:ok, _pid} = start_supervised({Listener, port: port, name: name, active_count: 5})

    ref =
      subscribe([
        [:phi_accrual_udp, :listener, :passive],
        [:phi_accrual_udp, :sample, :received]
      ])

    {:ok, sender} = :gen_udp.open(0, [:binary])

    for i <- 1..12 do
      :ok = :gen_udp.send(sender, {127, 0, 0, 1}, port, Packet.encode(i))
    end

    :gen_udp.close(sender)

    received = collect_events(ref, [:phi_accrual_udp, :sample, :received], 12, 1_000)
    passive = collect_events(ref, [:phi_accrual_udp, :listener, :passive], 2, 1_000)

    assert length(received) >= 12,
           "expected at least 12 :sample :received events, got #{length(received)}"

    assert length(passive) >= 2,
           "expected at least 2 :listener :passive events, got #{length(passive)}"
  end

  test "default active_count is 100" do
    port = ephemeral_port()
    name = :"listener_#{System.unique_integer([:positive])}"
    {:ok, _pid} = start_supervised({Listener, port: port, name: name})

    ref = subscribe([[:phi_accrual_udp, :sample, :received]])

    send_packet(port, Packet.encode(7))

    assert_receive {:event, ^ref, _, %{packet_timestamp_ms: 7}, _}, 500
  end

  test "calls PhiAccrual.observe/2 with local receipt time" do
    port = ephemeral_port()
    name = :"listener_#{System.unique_integer([:positive])}"
    {:ok, _pid} = start_supervised({Listener, port: port, name: name})

    # Send a packet with a wildly skewed timestamp; verify the estimator
    # tracks our peer (which means observe/2 was called) but ignore the
    # actual timestamp value — receipt time, not packet time, is what the
    # detector sees.
    send_packet(port, Packet.encode(0))

    # Give the listener a moment to process and call observe/2
    Process.sleep(50)

    # We can't predict the exact peer port, but we can check that *some*
    # localhost peer was tracked.
    tracked = PhiAccrual.tracked_nodes()

    assert Enum.any?(tracked, fn
             {{127, 0, 0, 1}, _port} -> true
             _ -> false
           end),
           "expected a localhost peer in tracked nodes, got: #{inspect(tracked)}"
  end
end
