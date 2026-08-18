defmodule SnabbkaffexTest do
  use ExUnit.Case, async: false
  use Snabbkaffex

  defmodule Worker do
    use Snabbkaffex, only: :trace

    def do_work(input) do
      tp(:worker_started, %{input: input})
      result = input * 2
      tp(:worker_finished, %{input: input, result: result})
      result
    end

    def span_work(input) do
      tp_span :worker_span, %{input: input} do
        input + 1
      end
    end

    def ping_after(ms, ref) do
      spawn(fn ->
        Process.sleep(ms)
        tp(:pong, %{ref: ref})
      end)

      :scheduled
    end

    def do_work_at_level(input) do
      tp(:worker_errored, %{input: input}, level: :error)
      :ok
    end

    def span_work_at_level(input) do
      tp_span :worker_span_lvl, %{input: input}, level: :info do
        input + 1
      end
    end

    def emit(kind, data), do: tp(kind, data)

    def diag_work(input, notify) do
      tp(:worker_diag, %{input: input, side: notify.()}, ignore_side_effects: true)
      :ok
    end

    # A "started" with no matching "finished" — an unmatched cause.
    def orphan_start(input), do: tp(:worker_started, %{input: input})
  end

  describe "tp + check_trace" do
    test "trace points are collected and queryable by kind" do
      check_trace(
        fn -> Worker.do_work(21) end,
        fn result, trace ->
          assert result == 42
          assert [%{input: 21}] = of_kind(trace, :worker_started)
          assert [%{input: 21, result: 42}] = of_kind(trace, :worker_finished)
        end
      )
    end

    test "events carry the $kind key and meta" do
      check_trace(
        fn -> Worker.do_work(1) end,
        fn trace ->
          assert [event] = of_kind(trace, :worker_started)
          assert %{:"$kind" => :worker_started, :"~meta" => meta} = event
          assert is_map(meta)
        end
      )
    end

    test "projection extracts a single field across events" do
      check_trace(
        fn ->
          Worker.do_work(2)
          Worker.do_work(3)
        end,
        fn trace ->
          inputs = trace |> of_kind(:worker_started) |> projection(:input)
          assert inputs == [2, 3]
        end
      )
    end

    test "projection of a list of fields returns tuples" do
      check_trace(
        fn -> Worker.do_work(2) end,
        fn trace ->
          assert [{2, 4}] = trace |> of_kind(:worker_finished) |> projection([:input, :result])
        end
      )
    end

    test "check_trace/3 accepts a run config" do
      check_trace(
        %{timeout: 100},
        fn -> Worker.do_work(7) end,
        fn trace ->
          assert [%{input: 7}] = of_kind(trace, :worker_started)
        end
      )
    end
  end

  describe "tp with ignore_side_effects: true" do
    test "records an event (and evaluates data) in :test, like tp/2" do
      parent = self()

      check_trace(
        fn -> Worker.diag_work(7, fn -> send(parent, :evaluated) end) end,
        fn trace ->
          # In :test the data expression runs, so the event is recorded...
          assert [%{input: 7}] = of_kind(trace, :worker_diag)
        end
      )

      # ...and the side effect in `data` did happen in :test.
      assert_received :evaluated
    end

    test "combining with :level raises at compile time" do
      assert_raise ArgumentError, ~r/mutually exclusive/, fn ->
        Code.eval_string(
          "tp(:k, %{a: 1}, level: :error, ignore_side_effects: true)",
          [],
          __ENV__
        )
      end
    end
  end

  describe "tp with an explicit level" do
    test "tp/3 still records an event in :test" do
      check_trace(
        fn -> Worker.do_work_at_level(3) end,
        fn result, trace ->
          assert result == :ok
          assert [%{input: 3}] = of_kind(trace, :worker_errored)
        end
      )
    end
  end

  describe "tp_span" do
    test "emits start and complete events and returns the block value" do
      check_trace(
        fn -> Worker.span_work(10) end,
        fn result, trace ->
          assert result == 11

          assert [%{:"$span" => :start}, %{:"$span" => {:complete, 11}}] =
                   of_kind(trace, :worker_span)
        end
      )
    end

    test "tp_span/4 with an explicit level emits start and complete" do
      check_trace(
        fn -> Worker.span_work_at_level(10) end,
        fn result, trace ->
          assert result == 11

          assert [%{:"$span" => :start}, %{:"$span" => {:complete, 11}}] =
                   of_kind(trace, :worker_span_lvl)
        end
      )
    end
  end

  describe "find_pairs" do
    test "pairs up causes with their effects" do
      check_trace(
        fn -> Worker.do_work(5) end,
        fn trace ->
          assert [{:pair, %{:"$kind" => :worker_started}, %{:"$kind" => :worker_finished}}] =
                   find_pairs(
                     trace,
                     %{:"$kind" => :worker_started},
                     %{:"$kind" => :worker_finished}
                   )
        end
      )
    end

    test "find_pairs/4 threads the guard through to the matcher" do
      check_trace(
        fn -> Worker.do_work(5) end,
        fn trace ->
          # A satisfied guard produces the pair; a rejecting guard produces
          # none. (A value-comparing guard is covered by the causality tests,
          # which run find_pairs/4 internally.)
          assert [{:pair, _, _}] =
                   find_pairs(
                     trace,
                     %{:"$kind" => :worker_started},
                     %{:"$kind" => :worker_finished},
                     true
                   )

          refute Enum.any?(
                   find_pairs(
                     trace,
                     %{:"$kind" => :worker_started},
                     %{:"$kind" => :worker_finished},
                     false
                   ),
                   &match?({:pair, _, _}, &1)
                 )
        end
      )
    end
  end

  describe "causality" do
    test "causality/3 (no guard) passes when every effect has a cause" do
      check_trace(
        fn ->
          Worker.do_work(5)
          Worker.do_work(6)
        end,
        fn trace ->
          assert causality(
                   trace,
                   %{:"$kind" => :worker_started},
                   %{:"$kind" => :worker_finished}
                 )
        end
      )
    end

    test "causality/4 with a guard ties inputs together" do
      check_trace(
        fn ->
          Worker.do_work(5)
          Worker.do_work(6)
        end,
        fn trace ->
          assert causality(
                   trace,
                   %{:"$kind" => :worker_started, input: i1},
                   %{:"$kind" => :worker_finished, input: i2},
                   i1 == i2
                 )
        end
      )
    end

    test "strict_causality passes when there are no unmatched causes" do
      check_trace(
        fn -> Worker.do_work(5) end,
        fn trace ->
          assert strict_causality(
                   trace,
                   %{:"$kind" => :worker_started},
                   %{:"$kind" => :worker_finished}
                 )
        end
      )
    end

    test "strict_causality fails on an unmatched cause" do
      # The snabbkaffe causality panic (`error({:panic, ...})`) is re-raised
      # verbatim as an ErlangError.
      assert_raise ErlangError, ~r/Causality violation/, fn ->
        check_trace(
          fn ->
            Worker.do_work(5)
            # extra "started" with no "finished" -> unmatched cause
            Worker.orphan_start(9)
          end,
          fn trace ->
            strict_causality(
              trace,
              %{:"$kind" => :worker_started},
              %{:"$kind" => :worker_finished}
            )
          end
        )
      end
    end
  end

  describe "check_trace re-raises the original error" do
    test "an assertion failing inside the check fun propagates verbatim" do
      assert_raise ExUnit.AssertionError, fn ->
        check_trace(
          fn -> Worker.do_work(1) end,
          fn trace ->
            assert of_kind(trace, :worker_started) == :deliberately_wrong
          end
        )
      end
    end

    test "an exception raised inside the run fun propagates verbatim" do
      assert_raise RuntimeError, ~r/boom in run/, fn ->
        check_trace(
          fn -> raise "boom in run" end,
          fn _trace -> true end
        )
      end
    end
  end

  describe "synchronisation" do
    test "block_until waits for an async event" do
      ref = make_ref()

      check_trace(
        fn ->
          Worker.ping_after(20, ref)
          assert {:ok, %{ref: ^ref}} = block_until(%{:"$kind" => :pong, ref: ^ref}, 1000)
        end,
        fn trace ->
          assert [%{ref: ^ref}] = of_kind(trace, :pong)
        end
      )
    end

    test "block_until/4 waits for N matching events" do
      ref = make_ref()

      check_trace(
        fn ->
          for _ <- 1..3, do: Worker.ping_after(5, ref)

          assert {:ok, events} =
                   block_until(%{:"$kind" => :pong, ref: ^ref}, 3, 2000, :infinity)

          assert length(events) == 3
        end,
        fn trace ->
          assert length(of_kind(trace, :pong)) == 3
        end
      )
    end

    test "wait_async_action runs the action and waits for the event" do
      ref = make_ref()

      check_trace(
        fn ->
          {action_ret, event} =
            wait_async_action %{:"$kind" => :pong, ref: ^ref}, timeout: 1000 do
              Worker.ping_after(20, ref)
            end

          assert action_ret == :scheduled
          assert {:ok, %{ref: ^ref}} = event
        end,
        fn _trace -> true end
      )
    end

    test "retry keeps calling the function until it stops raising" do
      counter = :counters.new(1, [])

      result =
        retry 10, 5 do
          :counters.add(counter, 1, 1)
          if :counters.get(counter, 1) < 3, do: raise("not yet")
          :done
        end

      assert result == :done
      assert :counters.get(counter, 1) == 3
    end
  end

  describe "nemesis: force_ordering" do
    test "delays an event until the continue event has fired" do
      check_trace(
        fn ->
          force_ordering(delay: %{:"$kind" => :fo_delayed}, until: %{:"$kind" => :fo_continue})

          test_pid = self()

          spawn(fn ->
            Worker.emit(:fo_delayed, %{})
            send(test_pid, :delayed_emitted)
          end)

          # The delayed trace point blocks inside its `tp` call, so nothing
          # should have been emitted yet.
          Process.sleep(100)
          refute_received :delayed_emitted

          Worker.emit(:fo_continue, %{})
          assert_receive :delayed_emitted, 1000
        end,
        fn trace ->
          # continue was recorded before delayed was released.
          assert [:fo_continue, :fo_delayed] =
                   trace |> of_kind([:fo_continue, :fo_delayed]) |> projection(:"$kind")
        end
      )
    end

    # The `:when` guard binds variables in both patterns and compares them.
    # Those variables are referenced only inside the guard, so the generated
    # standalone matcher must not leave them "unused" (a regression guard for
    # the compile-time warning that used to fire here).
    test "the :when guard ties the delay and until events together" do
      check_trace(
        fn ->
          # Hold a :fo_g_delayed until a :fo_g_continue with the SAME id fires.
          force_ordering(
            delay: %{:"$kind" => :fo_g_delayed, id: did},
            until: %{:"$kind" => :fo_g_continue, id: cid},
            when: cid == did
          )

          test_pid = self()

          spawn(fn ->
            Worker.emit(:fo_g_delayed, %{id: 7})
            send(test_pid, :g_delayed_emitted)
          end)

          Process.sleep(100)
          refute_received :g_delayed_emitted

          # A continue with a different id must NOT release it.
          Worker.emit(:fo_g_continue, %{id: 99})
          Process.sleep(100)
          refute_received :g_delayed_emitted

          # The matching id does.
          Worker.emit(:fo_g_continue, %{id: 7})
          assert_receive :g_delayed_emitted, 1000
        end,
        fn _trace -> true end
      )
    end

    test ":count releases only after that many until-events have fired" do
      check_trace(
        fn ->
          force_ordering(
            delay: %{:"$kind" => :fo_c_delayed},
            until: %{:"$kind" => :fo_c_continue},
            count: 2
          )

          test_pid = self()

          spawn(fn ->
            Worker.emit(:fo_c_delayed, %{})
            send(test_pid, :c_delayed_emitted)
          end)

          # One continue is not enough to release it.
          Worker.emit(:fo_c_continue, %{})
          Process.sleep(100)
          refute_received :c_delayed_emitted

          # The second one is.
          Worker.emit(:fo_c_continue, %{})
          assert_receive :c_delayed_emitted, 1000
        end,
        fn _trace -> true end
      )
    end

    test "missing :delay/:until and unknown options raise at compile time" do
      assert_raise ArgumentError, ~r/requires both :delay and :until/, fn ->
        Code.eval_string("force_ordering(delay: %{a: 1})", [], __ENV__)
      end

      assert_raise ArgumentError, ~r/unknown option/, fn ->
        Code.eval_string(
          "force_ordering(delay: %{a: 1}, until: %{b: 2}, bogus: 1)",
          [],
          __ENV__
        )
      end
    end
  end

  describe "nemesis: inject_crash" do
    test "crashes trace points matching the pattern with the given reason" do
      check_trace(
        fn ->
          inject_crash(%{:"$kind" => :crashy}, reason: :boom)

          {pid, ref} = spawn_monitor(fn -> Worker.emit(:crashy, %{}) end)
          assert_receive {:DOWN, ^ref, :process, ^pid, :boom}, 1000
        end,
        fn _trace -> true end
      )
    end

    test "inject_crash with no options crashes always and defaults the reason to :notmyday" do
      check_trace(
        fn ->
          inject_crash(%{:"$kind" => :crashy2})

          {pid, ref} = spawn_monitor(fn -> Worker.emit(:crashy2, %{}) end)
          assert_receive {:DOWN, ^ref, :process, ^pid, :notmyday}, 1000
        end,
        fn _trace -> true end
      )
    end

    test "random_crash: 1.0 always crashes; scenario: takes a raw fault scenario" do
      check_trace(
        fn ->
          inject_crash(%{:"$kind" => :randy}, random_crash: 1.0)
          {p1, r1} = spawn_monitor(fn -> Worker.emit(:randy, %{}) end)
          assert_receive {:DOWN, ^r1, :process, ^p1, :notmyday}, 1000

          inject_crash(%{:"$kind" => :customy}, scenario: always_crash(), reason: :custom)
          {p2, r2} = spawn_monitor(fn -> Worker.emit(:customy, %{}) end)
          assert_receive {:DOWN, ^r2, :process, ^p2, :custom}, 1000
        end,
        fn _trace -> true end
      )
    end

    test "rejects unknown options at compile time" do
      assert_raise ArgumentError, ~r/unknown option/, fn ->
        Code.eval_string("""
        import Snabbkaffex
        inject_crash(%{:"$kind" => :x}, bogus: 1)
        """)
      end
    end

    test "rejects more than one crash strategy at compile time" do
      assert_raise ArgumentError, ~r/at most one crash strategy/, fn ->
        Code.eval_string("""
        import Snabbkaffex
        inject_crash(%{:"$kind" => :x}, recover_after: 1, random_crash: 0.5)
        """)
      end
    end
  end

  describe "match_event" do
    test "builds a predicate function from a pattern" do
      pred = match_event(%{:"$kind" => :foo})

      assert pred.(%{:"$kind" => :foo, a: 1})
      refute pred.(%{:"$kind" => :bar})
    end
  end

  describe "give_or_take" do
    test "passes within deviation" do
      assert give_or_take(100, 5, 103)
    end

    test "raises outside deviation" do
      assert_raise RuntimeError, ~r/give_or_take failed/, fn ->
        give_or_take(100, 5, 200)
      end
    end
  end

  describe "trace queries: split / project / assert" do
    test "split_trace_at splits before/after the first match" do
      check_trace(
        fn ->
          Worker.do_work(1)
          Worker.do_work(2)
        end,
        fn trace ->
          {before, [at | _]} = split_trace_at(trace, %{:"$kind" => :worker_finished})
          # Nothing before the split point matches; the split point itself does.
          refute Enum.any?(before, &match?(%{:"$kind" => :worker_finished}, &1))
          assert %{:"$kind" => :worker_finished} = at
        end
      )
    end

    test "splitl_trace / splitr_trace segment the trace on a marker" do
      check_trace(
        fn ->
          Worker.do_work(1)
          Worker.do_work(2)
        end,
        fn trace ->
          events = of_kind(trace, [:worker_started, :worker_finished])

          # Both split the trace exhaustively (segments concat back to the input)
          # around the two markers, so there is more than one segment.
          segs_l = splitl_trace(events, %{:"$kind" => :worker_started})
          assert Enum.concat(segs_l) == events
          assert length(segs_l) > 1

          segs_r = splitr_trace(events, %{:"$kind" => :worker_finished})
          assert Enum.concat(segs_r) == events
          assert length(segs_r) > 1
        end
      )
    end

    test "projection_complete / projection_is_subset assert on projected values" do
      check_trace(
        fn ->
          Worker.do_work(1)
          Worker.do_work(2)
        end,
        fn trace ->
          started = of_kind(trace, :worker_started)
          # projection_complete: expected ⊆ traced. projection_is_subset: traced ⊆ expected.
          assert projection_complete(started, :input, [1, 2])
          assert projection_is_subset(started, :input, [1, 2, 3])

          assert_raise ErlangError, fn ->
            # 99 was never traced -> incomplete.
            projection_complete(started, :input, [1, 2, 99])
          end

          assert_raise ErlangError, fn ->
            # 2 was traced but isn't allowed -> not a subset.
            projection_is_subset(started, :input, [1])
          end
        end
      )
    end

    test "pair_max_depth reports nesting depth of pairs" do
      check_trace(
        fn -> Worker.do_work(1) end,
        fn trace ->
          pairs =
            find_pairs(trace, %{:"$kind" => :worker_started}, %{:"$kind" => :worker_finished})

          assert pair_max_depth(pairs) >= 1
        end
      )
    end

    test "erase_timestamps drops the meta so traces compare equal" do
      check_trace(
        fn -> Worker.do_work(1) end,
        fn trace ->
          assert is_list(erase_timestamps(trace))
        end
      )
    end
  end

  describe "list assertions" do
    test "unique passes on a trace with distinct events" do
      check_trace(
        fn -> Worker.do_work(1) end,
        fn trace -> assert unique(of_kind(trace, :worker_started)) end
      )
    end

    test "increasing / strictly_increasing assert or raise" do
      assert increasing([1, 2, 2, 3])
      assert strictly_increasing([1, 2, 3])

      assert_raise ErlangError, ~r/not strictly increasing/, fn ->
        strictly_increasing([1, 2, 2, 3])
      end
    end
  end

  describe "subscriptions" do
    test "subscribe + receive_events collects matching events" do
      ref = make_ref()

      check_trace(
        fn ->
          {:ok, sub} = subscribe(match_event(%{:"$kind" => :pong, ref: ^ref}), 3, 2000)
          for _ <- 1..3, do: Worker.ping_after(5, ref)
          assert {:ok, events} = receive_events(sub)
          assert length(events) == 3
        end,
        fn _trace -> true end
      )
    end
  end

  describe "statistics" do
    test "push_stat records datapoints that get_stats returns" do
      check_trace(
        fn ->
          push_stat(:latency, 10)
          push_stat(:latency, 20)
        end,
        fn _trace ->
          assert %{latency: _} = get_stats()
        end
      )
    end
  end

  describe "nemesis: recover_after / fix_crash" do
    test "recover_after crashes n times then recovers" do
      check_trace(
        fn ->
          ref = inject_crash(%{:"$kind" => :flaky}, recover_after: 1)

          {_, r1} = spawn_monitor(fn -> Worker.emit(:flaky, %{}) end)
          assert_receive {:DOWN, ^r1, :process, _, :notmyday}, 1000

          # Second emit no longer crashes.
          {_, r2} = spawn_monitor(fn -> Worker.emit(:flaky, %{}) end)
          assert_receive {:DOWN, ^r2, :process, _, :normal}, 1000

          fix_crash(ref)
        end,
        fn _trace -> true end
      )
    end
  end

  describe "__check__" do
    test "true / :ok pass" do
      assert Snabbkaffex.__check__(true)
      assert Snabbkaffex.__check__(:ok)
    end

    test "a snabbkaffe panic is turned into a raise" do
      assert_raise RuntimeError, ~r/snabbkaffe panic/, fn ->
        Snabbkaffex.__check__({:error, {:panic, :some_kind, [1, 2]}})
      end
    end

    test "any other result is turned into a raise" do
      assert_raise RuntimeError, ~r/check_trace failed/, fn ->
        Snabbkaffex.__check__({:error, :check_stage_failed})
      end
    end
  end

  # Multi-node tests for forward_trace/1 + unforward_trace/1. These boot a real
  # peer node (via OTP's :peer) sharing this node's code paths, so the peer can
  # load :snabbkaffe and emit trace points. Distribution is started in
  # test_helper.exs. `@tag :distributed` lets these be excluded on hosts where
  # spawning a peer isn't possible (`mix test --exclude distributed`).
  describe "forward_trace / unforward_trace across nodes (:peer)" do
    @describetag :distributed

    setup do
      # Unlinked: linking would tie the peer to the test process, which exits
      # before on_exit runs, leaving :peer.stop/1 nothing to stop.
      #
      # connection: 0 gives :peer a dedicated TCP control channel instead of the
      # default (which rides the Erlang distribution connection). Without it, the
      # Node.disconnect/1 in the netsplit tests would sever :peer's control link
      # too, and the peer would self-terminate as an orphan mid-test.
      {:ok, peer, node} =
        :peer.start(%{
          name: :peer.random_name(),
          host: ~c"127.0.0.1",
          connection: 0,
          args: [~c"-pa" | Enum.map(:code.get_path(), &to_charlist/1)]
        })

      on_exit(fn -> :peer.stop(peer) end)
      %{peer: peer, node: node}
    end

    # Emit a snabbkaffe trace point *on* the peer, going through its
    # persistent-term tp function (local_tp by default, remote_tp once
    # forwarding is on) — exactly as a `tp/2` in code running there would.
    #
    # Driven via :peer.call, which rides the peer's TCP control channel rather
    # than Erlang distribution. That's what lets us make the peer emit even
    # *while distribution is disconnected* (the netsplit tests below), without
    # the call itself re-establishing the connection the way :erpc/:rpc would.
    defp emit_on(peer, kind, data) do
      :peer.call(peer, :snabbkaffe, :tp, [__ENV__.line, :info, kind, data])
    end

    test "forwarded events from the peer land in the local collector",
         %{peer: peer, node: node} do
      check_trace(
        fn ->
          forward_trace(node)
          emit_on(peer, :remote_event, %{from: node, n: 1})
          # Give the async RPC back to the collector time to be recorded.
          {:ok, _} = block_until(%{"$kind": :remote_event}, 5_000)
        end,
        fn trace ->
          assert [%{from: ^node, n: 1}] = of_kind(trace, :remote_event)
        end
      )
    end

    test "unforward_trace stops events from flowing back", %{peer: peer, node: node} do
      check_trace(
        fn ->
          forward_trace(node)
          emit_on(peer, :remote_event, %{phase: :forwarded})
          {:ok, _} = block_until(%{"$kind": :remote_event, phase: :forwarded}, 5_000)

          # After unforwarding, the peer records locally: nothing reaches us.
          unforward_trace(node)
          emit_on(peer, :remote_event, %{phase: :after_unforward})
        end,
        fn trace ->
          kinds = of_kind(trace, :remote_event)
          assert [%{phase: :forwarded}] = kinds
          refute Enum.any?(kinds, &match?(%{phase: :after_unforward}, &1))
        end
      )
    end

    # The hazard unforward_trace/1 exists to avoid: with forwarding left on, the
    # peer's remote_tp does a synchronous rpc:call back to the collector node,
    # and that RPC re-establishes the very connection we deliberately cut.
    test "forwarding left on auto-reconnects a node you disconnect",
         %{peer: peer, node: node} do
      check_trace(
        fn ->
          forward_trace(node)
          assert node in Node.list()

          # Cut distribution, then make the peer emit (over the control channel,
          # so *our* trigger doesn't reconnect it — only remote_tp can).
          assert Node.disconnect(node)
          refute node in Node.list()
          emit_on(peer, :split_event, %{phase: :while_split})

          # remote_tp's RPC drags the connection (and the event) back to us.
          {:ok, _} = block_until(%{"$kind": :split_event}, 5_000)
          assert node in Node.list()
        end,
        fn trace ->
          assert [%{phase: :while_split}] = of_kind(trace, :split_event)
        end
      )
    end

    # The full netsplit cycle from unforward_trace/1's docstring:
    # forward → unforward (while reachable) → disconnect → reconnect → re-forward.
    test "unforward before a netsplit stops auto-reconnect; re-forward resumes delivery",
         %{peer: peer, node: node} do
      check_trace(
        fn ->
          # 1. Forwarding on: a normal emit reaches us.
          forward_trace(node)
          emit_on(peer, :cycle_event, %{phase: :before_split})
          {:ok, _} = block_until(%{"$kind": :cycle_event, phase: :before_split}, 5_000)

          # 2. Unforward while still reachable, then split.
          unforward_trace(node)
          assert Node.disconnect(node)
          refute node in Node.list()

          # 3. Emit during the split over the control channel: local_tp records
          #    it on the peer and makes no RPC, so this must NOT reconnect us.
          emit_on(peer, :cycle_event, %{phase: :while_split})
          refute node in Node.list()

          # 4. Reconnect, re-attach forwarding, and confirm delivery resumes.
          assert Node.connect(node)
          assert node in Node.list()
          forward_trace(node)
          emit_on(peer, :cycle_event, %{phase: :after_reconnect})
          {:ok, _} = block_until(%{"$kind": :cycle_event, phase: :after_reconnect}, 5_000)
        end,
        fn trace ->
          phases = trace |> of_kind(:cycle_event) |> projection(:phase)
          assert :before_split in phases
          assert :after_reconnect in phases
          # The emit that fired during the split went to the peer's own trace,
          # never to our collector.
          refute :while_split in phases
        end
      )
    end
  end
end
