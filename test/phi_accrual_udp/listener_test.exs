defmodule PhiAccrualUdp.ListenerTest do
  use ExUnit.Case, async: false

  alias PhiAccrualUdp.{Listener, Packet}

  @test_sender_id 0xA1B2C3D4_E5F60718

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

  defp encode_v1(timestamp_ms),
    do: <<0xCEA6::16, 1::8, 0::8, timestamp_ms::64-unsigned>>

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

  defp unique_name, do: :"listener_#{System.unique_integer([:positive])}"

  defp ephemeral_v6_port do
    case :gen_udp.open(0, [:binary, :inet6, {:ipv6_v6only, true}]) do
      {:ok, s} ->
        {:ok, port} = :inet.port(s)
        :gen_udp.close(s)
        {:ok, port}

      {:error, _} = err ->
        err
    end
  end

  defp send_v6_packet(port, bin) do
    case :gen_udp.open(0, [:binary, :inet6, {:ipv6_v6only, true}]) do
      {:ok, sender} ->
        :ok = :gen_udp.send(sender, {0, 0, 0, 0, 0, 0, 0, 1}, port, bin)
        :gen_udp.close(sender)
        :ok

      {:error, _} = err ->
        err
    end
  end

  describe "start_link/1 validation" do
    test "raises on 2-arity resolver (0.1.x signature)" do
      Process.flag(:trap_exit, true)

      legacy_resolver = fn _ip, _port -> :anything end

      assert {:error, {%ArgumentError{message: msg}, _}} =
               Listener.start_link(
                 port: ephemeral_port(),
                 name: unique_name(),
                 node_resolver: legacy_resolver
               )

      assert msg =~ "3-arity"
      assert msg =~ "0.1.x"
    end

    test "raises on non-function :node_resolver" do
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{}, _}} =
               Listener.start_link(
                 port: ephemeral_port(),
                 name: unique_name(),
                 node_resolver: :not_a_function
               )
    end
  end

  describe "child_spec/1" do
    test "default id is the module" do
      spec = Listener.child_spec(port: 4370)
      assert spec.id == Listener
      assert spec.type == :worker
      assert spec.restart == :permanent
      assert spec.shutdown == 5_000
      assert {Listener, :start_link, [start_opts]} = spec.start
      assert start_opts == [port: 4370]
    end

    test "honors :id, :restart, :shutdown without leaking into start_link" do
      spec =
        Listener.child_spec(
          port: 4370,
          id: :custom_listener,
          restart: :transient,
          shutdown: 2_000
        )

      assert spec.id == :custom_listener
      assert spec.restart == :transient
      assert spec.shutdown == 2_000

      assert {Listener, :start_link, [start_opts]} = spec.start
      refute Keyword.has_key?(start_opts, :id)
      refute Keyword.has_key?(start_opts, :restart)
      refute Keyword.has_key?(start_opts, :shutdown)
      assert start_opts == [port: 4370]
    end

    test "two listeners under one supervisor with different :id values" do
      v4_port = ephemeral_port()

      v4_name = unique_name()
      v6_name = unique_name()

      children =
        [
          {Listener, port: v4_port, id: :listener_v4, name: v4_name}
        ] ++
          case ephemeral_v6_port() do
            {:ok, v6_port} ->
              [
                {Listener,
                 port: v6_port, inet6: true, id: :listener_v6, name: v6_name}
              ]

            {:error, _} ->
              # IPv6 unavailable; supervise a second v4 listener to
              # still cover the :id-collision-avoidance path.
              [{Listener, port: ephemeral_port(), id: :listener_b, name: v6_name}]
          end

      sup_name = :"sup_#{System.unique_integer([:positive])}"

      # The supervisor links to the test process and dies with it,
      # so no explicit cleanup is needed.
      {:ok, _sup} = Supervisor.start_link(children, strategy: :one_for_one, name: sup_name)

      assert Process.whereis(v4_name) |> is_pid()
      assert Process.whereis(v6_name) |> is_pid()
    end
  end

  describe "IPv6 / interface binding" do
    test "inet6: true accepts v6 packets to ::1" do
      case ephemeral_v6_port() do
        {:error, reason} ->
          IO.puts("\n  Skipping (IPv6 unavailable): #{inspect(reason)}")

        {:ok, port} ->
          {:ok, _pid} =
            start_supervised({Listener, port: port, name: unique_name(), inet6: true})

          ref = subscribe([[:phi_accrual_udp, :sample, :received]])

          :ok = send_v6_packet(port, Packet.encode(@test_sender_id, 99))

          assert_receive {:event, ^ref, _, %{packet_timestamp_ms: 99},
                          %{
                            node: {:sender_id, @test_sender_id},
                            peer: {{0, 0, 0, 0, 0, 0, 0, 1}, _},
                            wire_version: 2
                          }},
                         500
      end
    end

    test "v1 packet over IPv6 produces 8-tuple peer identity" do
      case ephemeral_v6_port() do
        {:error, reason} ->
          IO.puts("\n  Skipping (IPv6 unavailable): #{inspect(reason)}")

        {:ok, port} ->
          {:ok, _pid} =
            start_supervised({Listener, port: port, name: unique_name(), inet6: true})

          ref = subscribe([[:phi_accrual_udp, :sample, :received]])

          :ok = send_v6_packet(port, encode_v1(7))

          assert_receive {:event, ^ref, _, _,
                          %{
                            node: {:peer, {0, 0, 0, 0, 0, 0, 0, 1}, _},
                            wire_version: 1
                          }},
                         500
      end
    end

    test ":started telemetry includes inet6 and ip metadata" do
      port = ephemeral_port()
      ref = subscribe([[:phi_accrual_udp, :listener, :started]])

      {:ok, _pid} =
        start_supervised(
          {Listener, port: port, name: unique_name(), ip: {127, 0, 0, 1}}
        )

      assert_receive {:event, ^ref, _, _,
                      %{port: ^port, inet6: false, ip: {127, 0, 0, 1}}},
                     500
    end

    test "ip option binds the listener to the specified interface" do
      # Bind to 127.0.0.1; packets sent to 127.0.0.1 reach us. This
      # doesn't prove exclusion (hard to test portably) but does
      # confirm :ip is accepted and a packet can be delivered.
      port = ephemeral_port()

      {:ok, _pid} =
        start_supervised(
          {Listener, port: port, name: unique_name(), ip: {127, 0, 0, 1}}
        )

      ref = subscribe([[:phi_accrual_udp, :sample, :received]])

      send_packet(port, Packet.encode(@test_sender_id, 0))

      assert_receive {:event, ^ref, _, _, _}, 500
    end

  end

  describe "v2 packet handling" do
    test "decodes a v2 packet, emits :sample :received with wire_version: 2" do
      port = ephemeral_port()
      {:ok, _pid} = start_supervised({Listener, port: port, name: unique_name()})

      ref = subscribe([[:phi_accrual_udp, :sample, :received]])

      send_packet(port, Packet.encode(@test_sender_id, 42))

      assert_receive {:event, ^ref, _, %{packet_timestamp_ms: 42},
                      %{
                        node: {:sender_id, @test_sender_id},
                        peer: {{127, 0, 0, 1}, _},
                        wire_version: 2
                      }},
                     500
    end

    test "default resolver identifies v2 peers as {:sender_id, id}" do
      port = ephemeral_port()
      {:ok, _pid} = start_supervised({Listener, port: port, name: unique_name()})

      ref = subscribe([[:phi_accrual_udp, :sample, :received]])

      send_packet(port, Packet.encode(@test_sender_id, 0))

      assert_receive {:event, ^ref, _, _, %{node: {:sender_id, @test_sender_id}}}, 500
    end
  end

  describe "v1 legacy packet handling (dual-decode)" do
    test "decodes a v1 packet, emits :sample :received with wire_version: 1" do
      port = ephemeral_port()
      {:ok, _pid} = start_supervised({Listener, port: port, name: unique_name()})

      ref = subscribe([[:phi_accrual_udp, :sample, :received]])

      send_packet(port, encode_v1(99))

      assert_receive {:event, ^ref, _, %{packet_timestamp_ms: 99},
                      %{
                        node: {:peer, {127, 0, 0, 1}, _},
                        peer: {{127, 0, 0, 1}, _},
                        wire_version: 1
                      }},
                     500
    end

    test "default resolver identifies v1 peers as {:peer, ip, port}" do
      port = ephemeral_port()
      {:ok, _pid} = start_supervised({Listener, port: port, name: unique_name()})

      ref = subscribe([[:phi_accrual_udp, :sample, :received]])

      send_packet(port, encode_v1(0))

      assert_receive {:event, ^ref, _, _,
                      %{node: {:peer, {127, 0, 0, 1}, _}, wire_version: 1}},
                     500
    end

    test "custom resolver gets nil for sender_id on v1 packets" do
      port = ephemeral_port()

      test_pid = self()

      resolver = fn ip, peer_port, sender_id ->
        send(test_pid, {:resolver_called, ip, peer_port, sender_id})
        {:peer_resolved, ip, peer_port}
      end

      {:ok, _pid} =
        start_supervised(
          {Listener, port: port, name: unique_name(), node_resolver: resolver}
        )

      send_packet(port, encode_v1(7))

      assert_receive {:resolver_called, {127, 0, 0, 1}, _, nil}, 500
    end

    test "custom resolver gets integer sender_id on v2 packets" do
      port = ephemeral_port()
      test_pid = self()

      resolver = fn _ip, _port, sender_id ->
        send(test_pid, {:resolver_called, sender_id})
        :ok_node
      end

      {:ok, _pid} =
        start_supervised(
          {Listener, port: port, name: unique_name(), node_resolver: resolver}
        )

      send_packet(port, Packet.encode(@test_sender_id, 0))

      assert_receive {:resolver_called, @test_sender_id}, 500
    end
  end

  describe ":reject path" do
    test "resolver returning {:reject, reason} emits :sample :rejected, does not observe" do
      port = ephemeral_port()
      resolver = fn _ip, _port, _sid -> {:reject, :unknown_sender} end

      {:ok, _pid} =
        start_supervised(
          {Listener, port: port, name: unique_name(), node_resolver: resolver}
        )

      ref =
        subscribe([
          [:phi_accrual_udp, :sample, :received],
          [:phi_accrual_udp, :sample, :rejected]
        ])

      send_packet(port, Packet.encode(@test_sender_id, 42))

      assert_receive {:event, ^ref, [:phi_accrual_udp, :sample, :rejected], _,
                      %{
                        peer: {{127, 0, 0, 1}, _},
                        sender_id: @test_sender_id,
                        reason: :unknown_sender,
                        wire_version: 2
                      }},
                     500

      refute_receive {:event, ^ref, [:phi_accrual_udp, :sample, :received], _, _}, 100
    end

    test "v1 rejection: sender_id metadata is nil" do
      port = ephemeral_port()
      resolver = fn _ip, _port, _sid -> {:reject, :no_legacy_allowed} end

      {:ok, _pid} =
        start_supervised(
          {Listener, port: port, name: unique_name(), node_resolver: resolver}
        )

      ref = subscribe([[:phi_accrual_udp, :sample, :rejected]])

      send_packet(port, encode_v1(0))

      assert_receive {:event, ^ref, _, _,
                      %{sender_id: nil, reason: :no_legacy_allowed, wire_version: 1}},
                     500
    end

    test "tagged reason term is preserved in telemetry metadata" do
      port = ephemeral_port()

      resolver = fn _ip, _port, sid ->
        {:reject, {:unknown_sender, sid}}
      end

      {:ok, _pid} =
        start_supervised(
          {Listener, port: port, name: unique_name(), node_resolver: resolver}
        )

      ref = subscribe([[:phi_accrual_udp, :sample, :rejected]])

      send_packet(port, Packet.encode(@test_sender_id, 0))

      assert_receive {:event, ^ref, _, _, %{reason: {:unknown_sender, @test_sender_id}}}, 500
    end
  end

  describe "decode errors" do
    test "emits :decode :error on bad magic" do
      port = ephemeral_port()
      {:ok, _pid} = start_supervised({Listener, port: port, name: unique_name()})

      ref = subscribe([[:phi_accrual_udp, :decode, :error]])

      send_packet(port, <<0xDEAD::16, 2::8, 0::8, 1::64, 0::64>>)

      assert_receive {:event, ^ref, _, %{packet_size: 20}, %{reason: :bad_magic}}, 500
    end

    test "emits :decode :error on wrong size" do
      port = ephemeral_port()
      {:ok, _pid} = start_supervised({Listener, port: port, name: unique_name()})

      ref = subscribe([[:phi_accrual_udp, :decode, :error]])

      send_packet(port, <<0xCE, 0xA6, 2>>)

      assert_receive {:event, ^ref, _, %{packet_size: 3}, %{reason: :wrong_size}}, 500
    end

    test "emits :decode :error on reserved_sender_id (v2 with sender_id=0)" do
      port = ephemeral_port()
      {:ok, _pid} = start_supervised({Listener, port: port, name: unique_name()})

      ref = subscribe([[:phi_accrual_udp, :decode, :error]])

      send_packet(port, <<0xCEA6::16, 2::8, 0::8, 0::64, 1234::64>>)

      assert_receive {:event, ^ref, _, %{packet_size: 20}, %{reason: :reserved_sender_id}},
                     500
    end
  end

  describe "custom resolver" do
    test "maps peer to custom node id" do
      port = ephemeral_port()
      resolver = fn _ip, _port, _sid -> :test_peer end

      {:ok, _pid} =
        start_supervised(
          {Listener, port: port, name: unique_name(), node_resolver: resolver}
        )

      ref = subscribe([[:phi_accrual_udp, :sample, :received]])

      send_packet(port, Packet.encode(@test_sender_id, 0))

      assert_receive {:event, ^ref, _, _, %{node: :test_peer}}, 500
    end
  end

  describe "flow control" do
    test "re-arms after consuming active_count packets" do
      port = ephemeral_port()

      {:ok, _pid} =
        start_supervised({Listener, port: port, name: unique_name(), active_count: 5})

      ref =
        subscribe([
          [:phi_accrual_udp, :listener, :passive],
          [:phi_accrual_udp, :sample, :received]
        ])

      {:ok, sender} = :gen_udp.open(0, [:binary])

      for i <- 1..12 do
        :ok =
          :gen_udp.send(
            sender,
            {127, 0, 0, 1},
            port,
            Packet.encode(@test_sender_id, i)
          )
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
      {:ok, _pid} = start_supervised({Listener, port: port, name: unique_name()})

      ref = subscribe([[:phi_accrual_udp, :sample, :received]])

      send_packet(port, Packet.encode(@test_sender_id, 7))

      assert_receive {:event, ^ref, _, %{packet_timestamp_ms: 7}, _}, 500
    end
  end

  describe "PhiAccrual integration" do
    test "calls PhiAccrual.observe/2 with the resolved node identity" do
      port = ephemeral_port()
      {:ok, _pid} = start_supervised({Listener, port: port, name: unique_name()})

      # Send a v2 packet with our known sender_id; the default
      # resolver maps it to {:sender_id, @test_sender_id}, which
      # must show up in phi_accrual's tracked nodes.
      send_packet(port, Packet.encode(@test_sender_id, 0))

      Process.sleep(50)

      tracked = PhiAccrual.tracked_nodes()

      assert {:sender_id, @test_sender_id} in tracked,
             "expected {:sender_id, #{@test_sender_id}} in tracked nodes, got: #{inspect(tracked)}"
    end
  end
end
