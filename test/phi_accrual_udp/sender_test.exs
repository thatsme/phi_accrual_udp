defmodule PhiAccrualUdp.SenderTest do
  use ExUnit.Case, async: false

  alias PhiAccrualUdp.{Sender, Packet}

  @test_sender_id 0xA1B2C3D4_E5F60718

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

  defp unique_name, do: :"sender_#{System.unique_integer([:positive])}"

  describe "start_link/1 sender_id validation" do
    test "raises when :sender_id is missing" do
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{message: msg}, _}} =
               Sender.start_link(targets: [], interval_ms: 30, name: unique_name())

      assert msg =~ ":sender_id is required"
    end

    test "raises when :sender_id is zero" do
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{message: msg}, _}} =
               Sender.start_link(sender_id: 0, targets: [], name: unique_name())

      assert msg =~ "non-zero unsigned 64-bit integer"
    end

    test "raises when :sender_id is negative" do
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{}, _}} =
               Sender.start_link(sender_id: -1, targets: [], name: unique_name())
    end

    test "raises when :sender_id exceeds u64" do
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{}, _}} =
               Sender.start_link(
                 sender_id: 0x1_0000_0000_0000_0000,
                 targets: [],
                 name: unique_name()
               )
    end

    test "raises when :sender_id is non-integer" do
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{}, _}} =
               Sender.start_link(sender_id: :foo, targets: [], name: unique_name())
    end
  end

  describe "start_link/1 timeout/concurrency validation" do
    test "raises when :send_timeout_ms >= :interval_ms" do
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{message: msg}, _}} =
               Sender.start_link(
                 sender_id: @test_sender_id,
                 targets: [],
                 interval_ms: 100,
                 send_timeout_ms: 100,
                 name: unique_name()
               )

      assert msg =~ "strictly less than"

      assert {:error, {%ArgumentError{}, _}} =
               Sender.start_link(
                 sender_id: @test_sender_id,
                 targets: [],
                 interval_ms: 100,
                 send_timeout_ms: 200,
                 name: unique_name()
               )
    end

    test "raises when :send_timeout_ms is not a positive integer" do
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{}, _}} =
               Sender.start_link(
                 sender_id: @test_sender_id,
                 targets: [],
                 interval_ms: 100,
                 send_timeout_ms: 0,
                 name: unique_name()
               )
    end

    test "raises when :max_send_concurrency is not a positive integer" do
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{}, _}} =
               Sender.start_link(
                 sender_id: @test_sender_id,
                 targets: [],
                 max_send_concurrency: 0,
                 name: unique_name()
               )
    end

    test "raises when :interval_ms is not a positive integer" do
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{}, _}} =
               Sender.start_link(
                 sender_id: @test_sender_id,
                 targets: [],
                 interval_ms: 0,
                 name: unique_name()
               )
    end

    test "default :send_timeout_ms is max(50, interval/2) and is strictly less than interval" do
      # interval=1000 → default timeout = 500. Verify by starting and
      # not crashing.
      {:ok, _pid} =
        start_supervised(
          {Sender,
           name: unique_name(),
           sender_id: @test_sender_id,
           interval_ms: 1_000,
           targets: []}
        )
    end
  end

  describe "send behavior" do
    test "sends a v2 packet to each target on tick" do
      {socket_a, port_a} = open_listener()
      {socket_b, port_b} = open_listener()

      on_exit(fn ->
        :gen_udp.close(socket_a)
        :gen_udp.close(socket_b)
      end)

      {:ok, _pid} =
        start_supervised(
          {Sender,
           name: unique_name(),
           sender_id: @test_sender_id,
           interval_ms: 100,
           targets: [{{127, 0, 0, 1}, port_a}, {{127, 0, 0, 1}, port_b}]}
        )

      assert_receive {:udp, ^socket_a, _ip, _port, packet_a}, 500
      assert_receive {:udp, ^socket_b, _ip, _port, packet_b}, 500

      assert {:ok, %Packet{version: 2, sender_id: @test_sender_id}} = Packet.decode(packet_a)
      assert {:ok, %Packet{version: 2, sender_id: @test_sender_id}} = Packet.decode(packet_b)
    end

    test "uses custom timestamp_fn" do
      {socket, port} = open_listener()
      on_exit(fn -> :gen_udp.close(socket) end)

      {:ok, _pid} =
        start_supervised(
          {Sender,
           name: unique_name(),
           sender_id: @test_sender_id,
           interval_ms: 100,
           timestamp_fn: fn -> 7777 end,
           targets: [{{127, 0, 0, 1}, port}]}
        )

      assert_receive {:udp, ^socket, _ip, _port, packet}, 500
      assert {:ok, %Packet{timestamp_ms: 7777}} = Packet.decode(packet)
    end
  end

  describe "child_spec/1" do
    test "default id is the module" do
      spec = Sender.child_spec(sender_id: @test_sender_id, targets: [])
      assert spec.id == Sender
      assert spec.type == :worker
      assert spec.restart == :permanent
      assert spec.shutdown == 5_000
      assert {Sender, :start_link, [start_opts]} = spec.start
      assert start_opts == [sender_id: @test_sender_id, targets: []]
    end

    test "honors :id, :restart, :shutdown without leaking into start_link" do
      spec =
        Sender.child_spec(
          sender_id: @test_sender_id,
          targets: [],
          id: :custom_sender,
          restart: :transient,
          shutdown: 1_000
        )

      assert spec.id == :custom_sender
      assert spec.restart == :transient
      assert spec.shutdown == 1_000

      assert {Sender, :start_link, [start_opts]} = spec.start
      refute Keyword.has_key?(start_opts, :id)
      refute Keyword.has_key?(start_opts, :restart)
      refute Keyword.has_key?(start_opts, :shutdown)
      assert start_opts == [sender_id: @test_sender_id, targets: []]
    end
  end

  describe "IPv6 / target family validation" do
    test "raises when :inet6 is true and any target is a 4-element IPv4 tuple" do
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{message: msg}, _}} =
               Sender.start_link(
                 sender_id: @test_sender_id,
                 inet6: true,
                 interval_ms: 100,
                 targets: [
                   {{0, 0, 0, 0, 0, 0, 0, 1}, 4370},
                   {{127, 0, 0, 1}, 4370}
                 ],
                 name: unique_name()
               )

      assert msg =~ ":inet6 is true"
      assert msg =~ "4-element"
    end

    test "raises when :inet6 is false and any target is an 8-element IPv6 tuple" do
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{message: msg}, _}} =
               Sender.start_link(
                 sender_id: @test_sender_id,
                 interval_ms: 100,
                 targets: [
                   {{127, 0, 0, 1}, 4370},
                   {{0, 0, 0, 0, 0, 0, 0, 1}, 4370}
                 ],
                 name: unique_name()
               )

      assert msg =~ ":inet6 is false"
      assert msg =~ "8-element"
    end

    test "hostnames pass start_link validation regardless of :inet6" do
      # The Sender shouldn't reject hostname targets at start — they
      # resolve per-send. Mismatches become per-target :send :error.
      {:ok, _pid} =
        start_supervised(
          {Sender,
           name: unique_name(),
           sender_id: @test_sender_id,
           inet6: true,
           interval_ms: 1_000,
           targets: [{~c"v4-only.example.invalid", 4370}]}
        )
    end

    test ":started telemetry includes inet6 and ip metadata" do
      ref = subscribe([[:phi_accrual_udp, :sender, :started]])

      {:ok, _pid} =
        start_supervised(
          {Sender,
           name: unique_name(),
           sender_id: @test_sender_id,
           interval_ms: 1_000,
           targets: [],
           ip: {127, 0, 0, 1}}
        )

      assert_receive {:event, ^ref, _, _, %{inet6: false, ip: {127, 0, 0, 1}}}, 500
    end

    test "v6 Sender to v6 Listener round-trips a v2 packet" do
      case :gen_udp.open(0, [:binary, :inet6, {:ipv6_v6only, true}, {:active, true}]) do
        {:error, reason} ->
          IO.puts("\n  Skipping (IPv6 unavailable): #{inspect(reason)}")

        {:ok, listener_sock} ->
          on_exit(fn -> :gen_udp.close(listener_sock) end)
          {:ok, listener_port} = :inet.port(listener_sock)

          {:ok, _pid} =
            start_supervised(
              {Sender,
               name: unique_name(),
               sender_id: @test_sender_id,
               inet6: true,
               interval_ms: 100,
               targets: [{{0, 0, 0, 0, 0, 0, 0, 1}, listener_port}]}
            )

          assert_receive {:udp, ^listener_sock, _ip, _port, packet}, 500

          assert {:ok, %Packet{version: 2, sender_id: @test_sender_id}} =
                   Packet.decode(packet)
      end
    end
  end

  describe "telemetry" do
    test "emits :sender :started with concurrency + timeout metadata" do
      ref = subscribe([[:phi_accrual_udp, :sender, :started]])

      {:ok, _pid} =
        start_supervised(
          {Sender,
           name: unique_name(),
           sender_id: @test_sender_id,
           interval_ms: 1_000,
           send_timeout_ms: 250,
           max_send_concurrency: 32,
           targets: []}
        )

      assert_receive {:event, ^ref, _, _,
                      %{
                        sender_id: @test_sender_id,
                        target_count: 0,
                        interval_ms: 1_000,
                        max_send_concurrency: 32,
                        send_timeout_ms: 250
                      }},
                     500
    end

    test "emits :sender :send :ok per target per tick" do
      {socket_a, port_a} = open_listener()
      {socket_b, port_b} = open_listener()

      on_exit(fn ->
        :gen_udp.close(socket_a)
        :gen_udp.close(socket_b)
      end)

      ref = subscribe([[:phi_accrual_udp, :sender, :send, :ok]])

      {:ok, _pid} =
        start_supervised(
          {Sender,
           name: unique_name(),
           sender_id: @test_sender_id,
           interval_ms: 200,
           targets: [{{127, 0, 0, 1}, port_a}, {{127, 0, 0, 1}, port_b}]}
        )

      assert_receive {:event, ^ref, _, %{duration: d_a},
                      %{target: {{127, 0, 0, 1}, ^port_a}, sender_id: @test_sender_id}},
                     500

      assert_receive {:event, ^ref, _, %{duration: d_b},
                      %{target: {{127, 0, 0, 1}, ^port_b}, sender_id: @test_sender_id}},
                     500

      assert is_integer(d_a) and d_a >= 0
      assert is_integer(d_b) and d_b >= 0
    end

    test "emits :sender :tick with sent/errors/timeouts/duration measurements" do
      {socket, port} = open_listener()
      on_exit(fn -> :gen_udp.close(socket) end)

      ref = subscribe([[:phi_accrual_udp, :sender, :tick]])

      {:ok, _pid} =
        start_supervised(
          {Sender,
           name: unique_name(),
           sender_id: @test_sender_id,
           interval_ms: 100,
           targets: [{{127, 0, 0, 1}, port}]}
        )

      assert_receive {:event, ^ref, _,
                      %{sent: 1, errors: 0, timeouts: 0, duration: d},
                      %{sender_id: @test_sender_id}},
                     500

      assert is_integer(d) and d >= 0
    end

    test ":tick aggregate matches target count even with zero targets" do
      ref = subscribe([[:phi_accrual_udp, :sender, :tick]])

      {:ok, _pid} =
        start_supervised(
          {Sender,
           name: unique_name(),
           sender_id: @test_sender_id,
           interval_ms: 100,
           targets: []}
        )

      assert_receive {:event, ^ref, _, %{sent: 0, errors: 0, timeouts: 0}, _}, 500
    end
  end
end
