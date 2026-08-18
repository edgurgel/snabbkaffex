defmodule Snabbkaffex do
  @moduledoc """
  A thin Elixir wrapper around the Erlang
  [snabbkaffe](https://github.com/kafka4beam/snabbkaffe) trace-based testing
  library.

  snabbkaffe ships its instrumentation as Erlang preprocessor macros
  (`-include_lib("snabbkaffe/include/trace.hrl")`) which are not usable from
  Elixir. This module provides the Elixir macro counterparts so that:

    * **Trace points** placed in production code (`tp/2`, `tp/3`, `tp_span/3`,
      `tp_span/4`) only become real snabbkaffe collector calls in `:test`.
      Compiled anywhere else they follow snabbkaffe's `trace_prod.hrl`
      convention: by default `data` is evaluated and its value thrown away,
      `level:` degrades to a `:logger` call, and `ignore_side_effects: true`
      drops to a bare `:ok` (see "Why discard in prod?" below).
    * **Test helpers** (`check_trace/2`, `block_until/2`, `of_kind/2`, ...) wrap
      the corresponding `:snabbkaffe` runtime calls. Most are plain functions;
      only the ones that take an Elixir *pattern* (`block_until`, `find_pairs`,
      `causality`, `force_ordering`, ...) need to be macros: they turn the
      pattern into a matcher function in place of snabbkaffe's `?match_event`.

  ## Why discard in prod?

  Snabbkaffex is a test-only dependency (`only: :test`). The trace-point macros
  live in regular `lib/` code, so they must compile to a cheap no-op when the
  collector is not present. Following snabbkaffe's own convention, the gate is
  `Mix.env() == :test`, evaluated *at macro-expansion time* so it reflects the
  environment of the module being compiled.

  To match the Erlang semantics precisely, the discarded `tp/2` still *evaluates*
  its data expression (so side effects behave identically across builds) and then
  throws the value away. Passing `level:` makes it degrade to a `:logger` call in
  non-test builds instead (just like `trace_prod.hrl`); passing
  `ignore_side_effects: true` skips the data expression entirely. The two are
  mutually exclusive (passing both raises at compile time), since they pull in
  opposite directions — keep the event in prod logs vs. drop it to nothing. See
  `tp/3`.

  ## Usage

  Instrument production code with `use Snabbkaffex, only: :trace`, which imports
  only the discardable trace-point macros (`tp/2`, `tp/3`, `tp_span/3`,
  `tp_span/4`) and none of the test-only helpers:

      defmodule MyServer do
        use Snabbkaffex, only: :trace

        def handle_event(e) do
          tp(:my_server_got_event, %{event: e})
          # ...
        end
      end

  And in a test, `use Snabbkaffex` (no options) to also get `check_trace/2`,
  `of_kind/2`, and friends. The trace collector is a global singleton, so a
  test module that runs traces **must** be `async: false`:

      use ExUnit.Case, async: false
      use Snabbkaffex

      test "server processes the event" do
        check_trace(
          fn -> MyServer.handle_event(:hello) end,
          fn trace ->
            assert [%{event: :hello}] = of_kind(trace, :my_server_got_event)
          end
        )
      end

  `use Snabbkaffex` emits a compile-time warning if the module is
  `use ExUnit.Case, async: true`, since concurrent tests would clobber each
  other's traces through the shared collector.

  ## Event shape

  A trace point `tp(:kind, %{a: 1})` is collected as a map
  `%{:"$kind" => :kind, :a => 1, :"~meta" => %{...}}`. When pattern matching on
  events, the kind lives under the `:"$kind"` key. Prefer `of_kind/2` to filter
  by kind so you rarely need to spell that out.

  ## Argument order

  Trace-querying functions (`of_kind/2`, `projection/2`, `find_pairs/3`,
  `causality/3`, ...) take the `trace` as their **first** argument, unlike the
  Erlang originals, so they read left-to-right in a pipe:

      trace
      |> of_kind(:worker_started)
      |> projection(:input)
  """

  # snabbkaffe's well-known map keys (see include/common.hrl).
  # The leading "$" makes the kind sort first when maps are printed; collected
  # events also carry metadata under the `:"~meta"` key.
  @snk_kind :"$kind"
  @snk_span :"$span"
  @snk_meta :"~meta"

  # Process-dictionary key under which a check function's raised exception is
  # stashed, so `check_trace/3` can re-raise it verbatim after snabbkaffe's
  # runner has swallowed it (see `__wrap_check__/1` and `__check__/1`).
  @check_error_key :"$snabbkaffex_check_error"

  # Trace-point macros safe to expose in production `lib/` code (the ones that
  # no-op outside `:test`). `use Snabbkaffex, only: :trace` imports just these.
  @trace_imports [tp: 2, tp: 3, tp_span: 3, tp_span: 4]

  @typedoc """
  A `check_trace/2,3` check function. Passes unless it raises (its return value
  is ignored, unlike Erlang snabbkaffe's `t::snabbkaffe.trace_spec/1`, which
  requires `ok`/`true`). Arity 1 receives the `trace`; arity 2 the run result
  and the `trace`.
  """
  @type check_fun :: (:snabbkaffe.trace() -> any) | (term, :snabbkaffe.trace() -> any)

  @typedoc "Field (or fields) to project out of each event; see `projection/2`."
  @type fields :: atom | [atom]

  @doc """
  Requires `Snabbkaffex` and imports its macros/helpers.

  Options:

    * `only: :trace` — import only the discardable trace-point macros (`tp/2`,
      `tp/3`, `tp_span/3`, `tp_span/4`). Use this in production `lib/` modules so
      you don't pull test-only helpers (`stop/0`, `retry/3`, ...) into their
      namespace.

  With no options, imports everything (intended for test modules). In that case
  Snabbkaffex also installs a `@before_compile` hook that warns if the module is
  `use ExUnit.Case, async: true`, because the trace collector is a global
  singleton and concurrent tests would corrupt each other's traces.
  """
  defmacro __using__(opts) do
    imports =
      case Keyword.get(opts, :only) do
        nil ->
          quote do: import(Snabbkaffex)

        :trace ->
          quote do: import(Snabbkaffex, only: unquote(@trace_imports))

        other ->
          raise ArgumentError,
                "use Snabbkaffex: unknown :only value #{inspect(other)} " <>
                  "(expected :trace or no :only)"
      end

    quote do
      require Snabbkaffex
      unquote(imports)
      @before_compile Snabbkaffex
    end
  end

  @doc false
  # Warn at compile time if a test module that pulls in Snabbkaffex is async.
  # `@ex_unit_module` is only set when `use ExUnit.Case` ran, so production
  # modules (where it's `nil`) are silently skipped.
  defmacro __before_compile__(env) do
    case Module.get_attribute(env.module, :ex_unit_module) do
      opts when is_list(opts) ->
        if Keyword.get(opts, :async, false) do
          IO.warn(
            "#{inspect(env.module)} uses Snabbkaffex with `async: true`. The snabbkaffe " <>
              "trace collector is a global singleton, so concurrent tests will clobber each " <>
              "other's traces. Set `use ExUnit.Case, async: false`.",
            Macro.Env.stacktrace(env)
          )
        end

      _ ->
        :ok
    end

    :ok
  end

  ##
  ## Trace points (discardable outside :test)
  ##

  @doc """
  Emit a trace point of the given `kind` carrying `data` (a map).

  In `:test` this records an event via the snabbkaffe collector. Outside `:test`
  the behaviour is controlled by `opts` (see `tp/3`); with no options `data` is
  evaluated for its side effects and the result discarded, returning `:ok`
  (mirroring snabbkaffe's `?tp/2`).
  """
  defmacro tp(kind, data), do: build_tp(kind, data, [], __CALLER__)

  @doc """
  `tp/2` with a literal keyword-list of `opts`:

    * `level: <atom>` — set the event's severity. In `:test` it's the collected
      event's level; outside `:test` the call degrades to a `:logger` call at that
      level, so the event stays visible in production logs (mirrors `?tp/3`).
    * `ignore_side_effects: true` — outside `:test` the whole call becomes a bare
      `:ok` and `data` is **not evaluated at all** (mirrors
      `?tp_ignore_side_effects_in_prod`). Use when building `data` is expensive
      or side-effecting and should cost nothing in production; the trade-off is
      that `data` must be purely diagnostic. Mutually exclusive with `:level`
      (passing both raises `ArgumentError` at compile time): `:level` keeps the
      event in production logs, whereas this drops it entirely outside `:test`.

  Examples:

      tp(:got_event, %{event: e}, level: :warning)
      tp(:snapshot, %{state: expensive_dump(s)}, ignore_side_effects: true)
  """
  defmacro tp(kind, data, opts), do: build_tp(kind, data, opts, __CALLER__)

  @doc """
  Trace a span around a `do` block: emits a `start` event before and a
  `{:complete, return}` event after, then returns the block's value.

  The keyword list carrying the `do:` block also accepts the same options as
  `tp/3` (`:level`, `:ignore_side_effects`). Unlike the Erlang macro (which
  expands `DATA` twice), `data` is evaluated exactly once. With
  `ignore_side_effects: true`, outside `:test` the span reduces to just the
  block and `data` is never evaluated.

      tp_span :fetch, %{id: id} do
        do_fetch(id)
      end

      tp_span :fetch, %{id: id}, level: :info do
        do_fetch(id)
      end
  """
  # `tp_span :k, data do .. end` -> opts == [do: block]
  # `tp_span :k, data, level: :info, do: block` (keyword form) also lands here.
  defmacro tp_span(kind, data, opts) do
    {block, opts} = pop_do!(opts)
    build_tp_span(kind, data, block, opts, __CALLER__)
  end

  # `tp_span :k, data, level: :info do .. end` — with a `do/end` block, Elixir
  # keeps the inline options and the block as *separate* trailing args.
  defmacro tp_span(kind, data, opts, do_block) do
    {block, _} = pop_do!(do_block)
    build_tp_span(kind, data, block, opts, __CALLER__)
  end

  ##
  ## Running a trace + checking it
  ##

  @doc """
  Run `run_fun`, collect the resulting trace, then validate it with `check_fun`.

  `run_fun` is a zero-arity function whose return value becomes the test result.
  `check_fun` is a function of either arity 1 (`trace`) or arity 2
  (`result, trace`). The check **passes unless it raises**: use ordinary ExUnit
  assertions; the function's return value is ignored (unlike Erlang snabbkaffe,
  which requires `true`/`ok`).

      check_trace(
        fn -> do_work() end,
        fn result, trace ->
          assert result == :ok
          assert [_] = of_kind(trace, :work_done)
        end
      )
  """
  @spec check_trace((-> term), check_fun) :: true
  def check_trace(run_fun, check_fun), do: check_trace(%{}, run_fun, check_fun)

  @doc """
  `check_trace/2` with a snabbkaffe run config (a map, e.g. `%{timeout: 100}`) or
  an integer statistics bucket.
  """
  @spec check_trace(:snabbkaffe.run_config() | integer, (-> term), check_fun) :: true
  def check_trace(config, run_fun, check_fun) do
    Process.delete(@check_error_key)
    __check__(:snabbkaffe.run(config, run_fun, __wrap_check__(check_fun)))
  end

  @doc false
  # Coerce a user check fun to snabbkaffe's "return true/ok to pass" contract:
  # in Elixir a check passes by not raising, so we discard its return value.
  def __wrap_check__(fun) when is_function(fun, 1) do
    fn trace -> __run_check__(fn -> fun.(trace) end) end
  end

  def __wrap_check__(fun) when is_function(fun, 2) do
    fn result, trace -> __run_check__(fn -> fun.(result, trace) end) end
  end

  def __wrap_check__(fun) do
    raise ArgumentError,
          "check_trace expects a check function of arity 1 (trace) or 2 (result, trace), " <>
            "got: #{inspect(fun)}"
  end

  # Run the user's check. On success return `true` (snabbkaffe's pass signal).
  # On failure, stash the exception (with its original stacktrace) so that
  # `check_trace/3` can re-raise it verbatim, then re-raise here so snabbkaffe
  # reports the spec as failed rather than logging an "invalid return value".
  defp __run_check__(fun) do
    try do
      fun.()
      true
    catch
      kind, reason ->
        Process.put(@check_error_key, {kind, reason, __STACKTRACE__})
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  @doc false
  def __check__(true), do: true
  def __check__(:ok), do: true

  # The run function itself raised: re-raise it with its original stacktrace.
  def __check__({:run_stage_failed, kind, reason, stacktrace}) do
    :erlang.raise(kind, reason, stacktrace)
  end

  def __check__({:error, {:panic, kind, args}}) do
    raise "snabbkaffe panic: #{inspect(kind)} #{inspect(args)}"
  end

  def __check__(other) do
    # If a check function raised, re-raise that exception verbatim (preserving
    # the ExUnit assertion message / stacktrace); otherwise report generically.
    case Process.delete(@check_error_key) do
      {kind, reason, stacktrace} -> :erlang.raise(kind, reason, stacktrace)
      nil -> raise "snabbkaffe check_trace failed: #{inspect(other)}"
    end
  end

  ##
  ## Collector lifecycle
  ##

  @doc "Start the snabbkaffe collector (idempotent)."
  @spec start_trace() :: :ok
  def start_trace, do: :snabbkaffe.start_trace()

  @doc "Stop the snabbkaffe collector."
  @spec stop() :: :ok
  def stop, do: :snabbkaffe.stop()

  @doc "Flush and return the collected trace, waiting `timeout` ms for silence first."
  @spec collect_trace() :: :snabbkaffe.trace()
  @spec collect_trace(integer) :: :snabbkaffe.trace()
  def collect_trace(timeout \\ 0), do: :snabbkaffe.collect_trace(timeout)

  @doc """
  Reset the collector and nemesis state (drops the buffered trace and any
  injected crashes/orderings) without stopping the collector.
  """
  defdelegate cleanup(), to: :snabbkaffe

  @doc """
  Start forwarding trace events emitted on the remote `node` to this collector.

  Used in multi-node tests so `tp` calls on `node` land in the local trace.
  """
  defdelegate forward_trace(node), to: :snabbkaffe

  @doc """
  Stop forwarding `node`'s trace events, reverting it to recording locally.

  A **Snabbkaffex addition** — snabbkaffe itself exposes no "unforward" call.
  `forward_trace/1` works by pointing `node`'s trace-point function (the
  `:snabbkaffe_tp_fun` persistent term) at `:snabbkaffe.remote_tp/5`, which RPCs
  every event back to this collector; this resets it to the default
  `:snabbkaffe.local_tp/5`, so `node` keeps its own trace instead.

  The motivating case is netsplit tests: with forwarding left on, every `tp/2`
  on the disconnected node does a synchronous RPC to the collector, which
  auto-reconnects a node you deliberately `Node.disconnect/1`'d. Call this while
  `node` is still reachable, *before* disconnecting; re-attach with
  `forward_trace/1` once reconnected.
  """
  @spec unforward_trace(node) :: :ok
  def unforward_trace(node) do
    :ok = :erpc.call(node, :persistent_term, :put, [:snabbkaffe_tp_fun, &:snabbkaffe.local_tp/5])
  end

  @doc "Dump `trace` to a file (as snabbkaffe does on failure) and return the path."
  defdelegate dump_trace(trace), to: :snabbkaffe

  ##
  ## Synchronisation
  ##

  @doc """
  Block until an event matching `pattern` is collected, or until `timeout`.

  `back_in_time` (ms) lets the matcher also consider recently-collected events,
  avoiding a race when the event fires before the call. Returns
  `{:ok, event}` or `:timeout`.
  """
  defmacro block_until(pattern, timeout \\ :infinity, back_in_time \\ :infinity) do
    quote do
      :snabbkaffe.block_until(
        unquote(matcher(pattern)),
        unquote(timeout),
        unquote(back_in_time)
      )
    end
  end

  @doc """
  `block_until/3` waiting for `n_events` events matching `pattern` (mirrors
  Erlang's `?block_until({Predicate, NEvents}, ...)` form). Events already in
  the collected trace count towards `n_events`, which makes it the right tool
  for waiting on the Nth occurrence of a recurring event (e.g. a node becoming
  `:ready` for the same view a second time, after churn). Returns
  `{:ok, [event]}` or `{:timeout, [partial]}`.
  """
  defmacro block_until(pattern, n_events, timeout, back_in_time) do
    quote do
      :snabbkaffe.block_until(
        {unquote(matcher(pattern)), unquote(n_events)},
        unquote(timeout),
        unquote(back_in_time)
      )
    end
  end

  @doc """
  Subscribe to an event matching `pattern`, run the `do` block, then wait (up to
  `timeout`) for the event. Returns `{block_return, {:ok, event} | :timeout}`.

  The block is the async action; it runs *before* the wait even though it reads
  last. Pass `timeout:` (ms, default `:infinity`) in the options alongside the
  block.

      wait_async_action %{"$kind": :work_done} do
        GenServer.cast(pid, :go)
      end

      wait_async_action %{"$kind": :work_done}, timeout: 100 do
        GenServer.cast(pid, :go)
      end
  """
  # `wait_async_action pattern, do: block` / `..., timeout: t, do: block` — the
  # keyword `do:` form lands here with the block inside `opts`.
  defmacro wait_async_action(pattern, opts) do
    {block, opts} = pop_do!(opts)
    build_wait_async_action(pattern, block, opts)
  end

  # `wait_async_action pattern, timeout: t do .. end` — with a `do/end` block,
  # Elixir keeps the inline options and the block as separate trailing args.
  defmacro wait_async_action(pattern, opts, do_block) do
    {block, _} = pop_do!(do_block)
    build_wait_async_action(pattern, block, opts)
  end

  @doc """
  Retry the `do` block up to `n` times, sleeping `interval` ms between attempts,
  until it stops raising. Returns the block's value.

      retry 10, 5 do
        assert something_eventually_true()
      end
  """
  defmacro retry(interval, n, opts) do
    {block, _} = pop_do!(opts)

    quote do
      :snabbkaffe.retry(unquote(interval), unquote(n), fn -> unquote(block) end)
    end
  end

  @doc """
  Subscribe to events matching `predicate`, returning `{:ok, subscription}`.

  Lower-level than `block_until/2`: pair with `receive_events/1` to collect the
  events later. `predicate` is a `fn event -> boolean end` — build one from a
  pattern with `match_event/1`. Optional `n_events` (default 1), `timeout`, and
  `back_in_time` mirror `:snabbkaffe.subscribe/1..4`.
  """
  defdelegate subscribe(predicate), to: :snabbkaffe
  defdelegate subscribe(predicate, timeout), to: :snabbkaffe
  defdelegate subscribe(predicate, n_events, timeout), to: :snabbkaffe
  defdelegate subscribe(predicate, n_events, timeout, back_in_time), to: :snabbkaffe

  @doc """
  Wait for the events of a `subscription` returned by `subscribe/1`. Returns
  `{:ok, [event]}` or `{:timeout, [partial]}`.
  """
  defdelegate receive_events(subscription), to: :snabbkaffe

  ##
  ## Trace querying
  ##

  @doc "Keep only events in `trace` whose `:\"$kind\"` is `kind` (or one of a list of kinds)."
  @spec of_kind(:snabbkaffe.trace(), :snabbkaffe.kind() | [:snabbkaffe.kind()]) ::
          :snabbkaffe.trace()
  def of_kind(trace, kind), do: :snabbkaffe.events_of_kind(kind, trace)

  @doc """
  Project `field` (atom) or `fields` (list of atoms) out of each event in `trace`.

  With a single atom, returns a list of values; with a list, a list of tuples.
  """
  @spec projection(:snabbkaffe.trace(), fields) :: list
  def projection(trace, fields), do: :snabbkaffe.projection(fields, trace)

  @doc """
  Find cause/effect pairs in `trace` matching `cause_pattern` and `effect_pattern`.
  """
  defmacro find_pairs(trace, cause_pattern, effect_pattern) do
    build_find_pairs(cause_pattern, effect_pattern, true, trace)
  end

  @doc "`find_pairs/3` with an extra `guard` expression over both events' bindings."
  defmacro find_pairs(trace, cause_pattern, effect_pattern, guard) do
    build_find_pairs(cause_pattern, effect_pattern, guard, trace)
  end

  @doc """
  Assert every effect event in `trace` was preceded by a matching cause event.
  Pairs may be nested. Raises on a causality violation; otherwise returns `true`
  if at least one pair was found, `false` if none.
  """
  defmacro causality(trace, cause_pattern, effect_pattern) do
    do_causality(false, cause_pattern, effect_pattern, true, trace)
  end

  @doc "`causality/3` with a `guard` expression over both events' bindings."
  defmacro causality(trace, cause_pattern, effect_pattern, guard) do
    do_causality(false, cause_pattern, effect_pattern, guard, trace)
  end

  @doc "Like `causality/3`, but additionally forbids unmatched effect events."
  defmacro strict_causality(trace, cause_pattern, effect_pattern) do
    do_causality(true, cause_pattern, effect_pattern, true, trace)
  end

  @doc "`strict_causality/3` with a `guard` expression over both events' bindings."
  defmacro strict_causality(trace, cause_pattern, effect_pattern, guard) do
    do_causality(true, cause_pattern, effect_pattern, guard, trace)
  end

  @doc "Keep only events in `trace` whose logger metadata `domain` equals `domain`."
  @spec of_domain(:snabbkaffe.trace(), [atom]) :: :snabbkaffe.trace()
  def of_domain(trace, domain) do
    Enum.filter(trace, &match?(%{@snk_meta => %{domain: ^domain}}, &1))
  end

  @doc "Keep only events in `trace` emitted on `node`."
  @spec of_node(:snabbkaffe.trace(), node) :: :snabbkaffe.trace()
  def of_node(trace, node) do
    Enum.filter(trace, &match?(%{@snk_meta => %{node: ^node}}, &1))
  end

  @doc "Strip timestamps from every event in `trace` (useful before comparing traces)."
  defdelegate erase_timestamps(trace), to: :snabbkaffe

  @doc "The maximum nesting depth among a list of `pairs` (from `find_pairs/3`)."
  defdelegate pair_max_depth(pairs), to: :snabbkaffe

  @doc """
  Split `trace` at the first event matching `pattern`, returning
  `{before, [match | after]}` (Elixir's `Enum.split_while/2` at the marker).
  """
  defmacro split_trace_at(trace, pattern) do
    quote do
      Enum.split_while(unquote(trace), unquote(inverse_matcher(pattern)))
    end
  end

  @doc """
  Split `trace` into segments, each **ending** with an event matching `pattern`;
  a trailing run of non-matching events forms the final segment. Mirrors
  `?splitl_trace`.

  E.g. splitting `[a, b, M, c, M, d]` on `M` gives `[[a, b, M], [c, M], [d]]`.
  """
  defmacro splitl_trace(trace, pattern) do
    quote do
      :snabbkaffe.splitl(unquote(inverse_matcher(pattern)), unquote(trace))
    end
  end

  @doc """
  Split `trace` into segments, each **starting** with an event matching
  `pattern`; a leading run of non-matching events forms the first segment.
  Mirrors `?splitr_trace`.

  E.g. splitting `[a, b, M, c, M, d]` on `M` gives `[[a, b], [M, c], [M, d]]`.
  """
  defmacro splitr_trace(trace, pattern) do
    quote do
      :snabbkaffe.splitr(unquote(inverse_matcher(pattern)), unquote(trace))
    end
  end

  ##
  ## Fault injection / scheduling (snabbkaffe_nemesis)
  ##

  @doc ~S"""
  Enforce an ordering between two classes of events: hold back every event
  matching the `:delay` pattern until events matching the `:until` pattern have
  been emitted.

  Takes a literal keyword list so the direction is obvious at the call site —
  "delay X until Y":

    * `:delay` (required) — pattern for the events to hold back.
    * `:until` (required) — pattern for the gate events that release them.
    * `:count` — release only after this many `:until` events have been seen
      (default `1`). Events already in the collected trace count towards it.
    * `:when` — a boolean guard over the variables bound in the `:delay` and
      `:until` patterns, used to tie the two together (e.g. by a shared id).
      Defaults to always matching.

  ## Examples

      # Hold worker :a's :store until worker :b's :miss has fired.
      force_ordering(
        delay: %{:"$kind" => :store, worker: :a},
        until: %{:"$kind" => :miss, worker: :b}
      )

      # Tie the two events together by id, and wait for two gate events.
      force_ordering(
        delay: %{:"$kind" => :store, id: sid},
        until: %{:"$kind" => :miss, id: mid},
        count: 2,
        when: sid == mid
      )

  Wraps snabbkaffe's `force_ordering` (whose positional argument order differs).
  """
  defmacro force_ordering(opts) do
    opts = validate_force_ordering!(opts)

    build_force_ordering(
      Keyword.fetch!(opts, :until),
      Keyword.get(opts, :count, 1),
      Keyword.fetch!(opts, :delay),
      Keyword.get(opts, :when, true)
    )
  end

  @force_ordering_keys [:delay, :until, :count, :when]

  defp validate_force_ordering!(opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError,
            "force_ordering expects a literal keyword list with :delay and :until, got: " <>
              Macro.to_string(opts)
    end

    keys = Keyword.keys(opts)

    unless :delay in keys and :until in keys do
      raise ArgumentError,
            "force_ordering requires both :delay and :until, got keys: #{inspect(keys)}"
    end

    case keys -- @force_ordering_keys do
      [] ->
        opts

      unknown ->
        raise ArgumentError,
              "force_ordering got unknown option(s): #{inspect(unknown)}. " <>
                "Valid options: #{inspect(@force_ordering_keys)}"
    end
  end

  @doc """
  Inject crashes at trace points matching `pattern`, returning a reference you
  can later pass to `fix_crash/1`.

  With no options the process crashes on **every** matching trace point. Pass at
  most one *strategy* option to shape when it crashes:

    * `recover_after: n` — crash the first `n` matches, then recover
    * `random_crash: p` — crash with probability `p` (a float in `0.0..1.0`)
    * `scenario: fun` — any snabbkaffe fault scenario, for the less common cases
      (e.g. `scenario: periodic_crash(10, 0.5, 0.0)`)

  The `reason:` option sets the exit reason for the crashing process (defaults to
  `:notmyday`).

      inject_crash(%{:"$kind" => :write})                        # crash every time
      inject_crash(%{:"$kind" => :write}, reason: :disk_full)    # ... with a reason
      inject_crash(%{:"$kind" => :before_ack}, recover_after: 1)
      inject_crash(%{:"$kind" => :flaky}, random_crash: 0.1)
      inject_crash(%{:"$kind" => :wave}, scenario: periodic_crash(10, 0.5, 0.0))
  """
  defmacro inject_crash(pattern, opts \\ []) do
    opts = validate_inject_crash!(opts)

    quote do
      :snabbkaffe_nemesis.inject_crash(
        unquote(matcher(pattern)),
        unquote(build_crash_scenario(opts)),
        unquote(Keyword.get(opts, :reason, :notmyday))
      )
    end
  end

  @crash_strategy_keys [:recover_after, :random_crash, :scenario]
  @inject_crash_keys [:reason | @crash_strategy_keys]

  defp validate_inject_crash!(opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError,
            "inject_crash expects a literal keyword list of options, got: " <>
              Macro.to_string(opts)
    end

    keys = Keyword.keys(opts)

    case keys -- @inject_crash_keys do
      [] ->
        :ok

      unknown ->
        raise ArgumentError,
              "inject_crash got unknown option(s): #{inspect(unknown)}. " <>
                "Valid options: #{inspect(@inject_crash_keys)}"
    end

    case Enum.filter(keys, &(&1 in @crash_strategy_keys)) do
      strategies when length(strategies) <= 1 ->
        opts

      strategies ->
        raise ArgumentError,
              "inject_crash accepts at most one crash strategy, got: #{inspect(strategies)}. " <>
                "Choose one of #{inspect(@crash_strategy_keys)}."
    end
  end

  # Returns the AST for the fault scenario. No strategy key means "always crash".
  defp build_crash_scenario(opts) do
    case Keyword.take(opts, @crash_strategy_keys) do
      [] -> quote(do: :snabbkaffe_nemesis.always_crash())
      [{:recover_after, n}] -> quote(do: :snabbkaffe_nemesis.recover_after(unquote(n)))
      [{:random_crash, p}] -> quote(do: :snabbkaffe_nemesis.random_crash(unquote(p)))
      [{:scenario, fun}] -> fun
    end
  end

  @doc "Remove a crash injected by `inject_crash/2` (given its reference)."
  defdelegate fix_crash(reference), to: :snabbkaffe_nemesis

  @doc """
  Fault scenario for `inject_crash/2`'s `scenario:` option: crash on every
  matching trace point. This is also `inject_crash`'s default when no strategy
  option is given.
  """
  defdelegate always_crash(), to: :snabbkaffe_nemesis

  @doc """
  Fault scenario: crash the first `n` times a trace point matches, then recover.
  Usually reached via `inject_crash(pattern, recover_after: n)`.
  """
  defdelegate recover_after(n), to: :snabbkaffe_nemesis

  @doc """
  Fault scenario: crash with the given `probability` (a float in `0.0..1.0`).
  Usually reached via `inject_crash(pattern, random_crash: probability)`.
  """
  defdelegate random_crash(probability), to: :snabbkaffe_nemesis

  @doc """
  Fault scenario for `inject_crash/2`'s `scenario:` option: crash periodically,
  driven by the count of matching trace
  points (not wall-clock time). Within each cycle of `period` matches the trace
  point stays healthy for the first `duty_cycle` fraction and crashes for the
  rest; `phase` (in radians, `0` to `2 * pi`) shifts the cycle.
  """
  defdelegate periodic_crash(period, duty_cycle, phase), to: :snabbkaffe_nemesis

  ##
  ## Assertions
  ##

  @doc """
  Assert `value` is within `deviation` of `expected`. Returns `true` or raises.
  """
  @spec give_or_take(number, number, number) :: true
  def give_or_take(expected, deviation, value)
      when is_number(expected) and is_number(deviation) and is_number(value) do
    if abs(value - expected) <= deviation do
      true
    else
      raise "give_or_take failed: expected #{inspect(value)} to be within " <>
              "#{inspect(deviation)} of #{inspect(expected)}"
    end
  end

  @doc "Assert every event in `trace` is unique. Returns `true` or raises."
  defdelegate unique(trace), to: :snabbkaffe

  @doc "Assert the numbers in `list` are non-decreasing. Returns `true` or raises."
  defdelegate increasing(list), to: :snabbkaffe

  @doc "Assert the numbers in `list` are strictly increasing. Returns `true` or raises."
  defdelegate strictly_increasing(list), to: :snabbkaffe

  @doc """
  Assert every value in `expected` appears in the projection of `fields` over
  `trace` (i.e. `expected` is a subset of what was traced). Returns `true` or
  raises. The complement of `projection_is_subset/3`.
  """
  @spec projection_complete(:snabbkaffe.trace(), fields, [term]) :: true
  def projection_complete(trace, fields, expected) do
    :snabbkaffe.projection_complete(fields, trace, expected)
  end

  @doc """
  Assert the projection of `fields` over `trace` contains no values outside
  `expected` (i.e. what was traced is a subset of `expected`). Returns `true`
  or raises.
  """
  @spec projection_is_subset(:snabbkaffe.trace(), fields, [term]) :: true
  def projection_is_subset(trace, fields, expected) do
    :snabbkaffe.projection_is_subset(fields, trace, expected)
  end

  ##
  ## Statistics
  ##

  @doc """
  Record a `value` for `metric`. With an extra leading argument, records a
  `{x, y}` datapoint (`push_stat(metric, x, y)`). See `analyze_statistics/0`.
  """
  defdelegate push_stat(metric, value), to: :snabbkaffe
  defdelegate push_stat(metric, x, y), to: :snabbkaffe

  @doc "Record many datapoints for `metric` at once (see `:snabbkaffe.push_stats/2,3`)."
  defdelegate push_stats(metric, values), to: :snabbkaffe
  defdelegate push_stats(metric, x, values), to: :snabbkaffe

  @doc "Print an analysis of all statistics collected via `push_stat/2`."
  defdelegate analyze_statistics(), to: :snabbkaffe

  @doc "Return the raw statistics map collected so far."
  defdelegate get_stats(), to: :snabbkaffe

  @doc """
  Build a predicate `fn event -> boolean end` from an Elixir `pattern`.

  This is the Elixir counterpart of snabbkaffe's `?match_event`, used internally
  by the pattern-taking macros and exposed for building snabbkaffe subscriptions
  directly (`:snabbkaffe.subscribe/1`, etc.).
  """
  defmacro match_event(pattern) do
    matcher(pattern)
  end

  ##
  ## Expansion-time helpers (run while a *caller* is being compiled)
  ##

  # Whether trace points should expand to live snabbkaffe calls. Evaluated inside
  # macro bodies, so `Mix.env/0` is the environment of the module being compiled.
  defp collector_enabled? do
    function_exported?(Mix, :env, 0) and Mix.env() == :test
  end

  # Parse the literal keyword-list `opts` of `tp`/`tp_span` at expansion time.
  # `:level`'s value and other entries may themselves be quoted expressions, but
  # `:ignore_side_effects` must be a literal boolean since it drives expansion.
  defp parse_tp_opts(opts) when is_list(opts) do
    ignore? = Keyword.get(opts, :ignore_side_effects, false)

    if not is_boolean(ignore?) do
      raise ArgumentError, ":ignore_side_effects must be a literal true or false"
    end

    explicit_level? = Keyword.has_key?(opts, :level)

    if ignore? and explicit_level? do
      raise ArgumentError,
            ":level and ignore_side_effects: true are mutually exclusive — :level keeps the " <>
              "event as a :logger call outside :test, while ignore_side_effects: true drops it " <>
              "to a bare :ok. Pick one."
    end

    {Keyword.get(opts, :level, :debug), explicit_level?, ignore?}
  end

  defp parse_tp_opts(_opts) do
    raise ArgumentError, "trace-point options must be a literal keyword list"
  end

  # Non-test expansion mirrors snabbkaffe's trace_prod.hrl:
  #   default            -> begin _ = EVT, ok end        (evaluate + discard)
  #   level: L           -> logger:log(L, EVT#{kind}, M)  (still logs in prod)
  #   ignore_side_effects-> ok                            (EVT never evaluated)
  defp build_tp(kind, data, opts, caller) do
    {level, explicit_level?, ignore?} = parse_tp_opts(opts)

    cond do
      collector_enabled?() ->
        quote do
          :snabbkaffe.tp(
            unquote(static_token(caller)),
            unquote(level),
            unquote(kind),
            unquote(data)
          )
        end

      ignore? ->
        quote do: :ok

      explicit_level? ->
        meta = log_metadata(caller)

        quote do
          :logger.log(
            unquote(level),
            Map.put(unquote(data), unquote(@snk_kind), unquote(kind)),
            unquote(Macro.escape(meta))
          )
        end

      true ->
        quote do
          _ = unquote(data)
          :ok
        end
    end
  end

  # Extract the `do:` block from a `tp_span` keyword list, leaving the rest as
  # options to pass through to the underlying `tp` calls.
  defp pop_do!(opts) when is_list(opts) do
    if Keyword.has_key?(opts, :do) do
      Keyword.pop(opts, :do)
    else
      raise ArgumentError, "tp_span requires a `do` block"
    end
  end

  defp pop_do!(_opts) do
    raise ArgumentError, "tp_span options must be a literal keyword list with a `do` block"
  end

  defp build_tp_span(kind, data, block, opts, _caller) do
    {_level, _explicit_level?, ignore?} = parse_tp_opts(opts)

    if ignore? and not collector_enabled?() do
      # Prod + ignore_side_effects: run the block only; never touch `data`.
      quote do: unquote(block)
    else
      # Both `tp` calls self-branch on the env via `tp/3`, so `data` (bound once)
      # is recorded in :test and discarded/logged in prod per `opts`.
      quote do
        data = unquote(data)

        Snabbkaffex.tp(unquote(kind), Map.put(data, unquote(@snk_span), :start), unquote(opts))
        ret = unquote(block)

        Snabbkaffex.tp(
          unquote(kind),
          Map.put(data, unquote(@snk_span), {:complete, ret}),
          unquote(opts)
        )

        ret
      end
    end
  end

  # snabbkaffe's ?__snkStaticUniqueToken: a fresh `fn -> {file, line} end` per call
  # site. Used as a stable identity for crash-injection points and trace dumps.
  defp static_token(caller) do
    quote do: fn -> {unquote(caller.file), unquote(caller.line)} end
  end

  defp log_metadata(caller) do
    base = %{line: caller.line, file: caller.file}

    case caller.function do
      {name, arity} -> Map.put(base, :mfa, {caller.module, name, arity})
      nil -> base
    end
  end

  # ?match_event(PATTERN): fn event -> match?(PATTERN, event) end
  defp matcher(pattern) do
    quote do
      fn __snk_event__ ->
        case __snk_event__ do
          unquote(pattern) ->
            unquote(mark_vars_used(pattern))
            true

          _ ->
            false
        end
      end
    end
  end

  # ?snk_int_inverse_match_arg(PATTERN): the negation, used by the split macros.
  defp inverse_matcher(pattern) do
    quote do
      fn __snk_event__ ->
        case __snk_event__ do
          unquote(pattern) ->
            unquote(mark_vars_used(pattern))
            false

          _ ->
            true
        end
      end
    end
  end

  # These predicates only test whether an event matches — they never read the
  # variables the pattern binds (any guard that needs them lives in `matcher2/3`,
  # which splices the pattern separately). So each bound variable would trigger
  # an "unused variable" warning at the *macro call site* (e.g. the `id` bound in
  # a `force_ordering` `:delay`/`:until` pattern that's only compared in `:when`).
  # We emit a `_ = {vars...}` marker that references them, which silences the
  # warning without touching the pattern's semantics: pins, repeated (equality)
  # variables, and wildcards all keep working. Underscored names are skipped —
  # they never warn, and `_` can't be referenced.
  defp mark_vars_used(pattern) do
    vars =
      pattern
      |> Macro.prewalk([], fn
        {name, _, ctx} = node, acc when is_atom(name) and is_atom(ctx) ->
          if referenceable_var?(name), do: {node, [{name, ctx} | acc]}, else: {node, acc}

        node, acc ->
          {node, acc}
      end)
      |> elem(1)
      |> Enum.uniq()
      |> Enum.map(fn {name, ctx} -> {name, [], ctx} end)

    quote do: _ = {unquote_splicing(vars)}
  end

  defp referenceable_var?(name), do: not String.starts_with?(Atom.to_string(name), "_")

  # ?snk_int_match_arg2(M1, M2, GUARD): a 2-arity predicate that matches the first
  # arg against `m1`, the second against `m2`, then evaluates `guard` (an ordinary
  # boolean expression, not an Elixir guard) with the bindings from both patterns.
  defp matcher2(m1, m2, guard) do
    quote do
      fn __snk_a__, __snk_b__ ->
        case __snk_a__ do
          unquote(m1) ->
            case __snk_b__ do
              unquote(m2) -> unquote(guard)
              _ -> false
            end

          _ ->
            false
        end
      end
    end
  end

  defp do_causality(strict?, cause_pattern, effect_pattern, guard, trace) do
    quote do
      :snabbkaffe.causality(
        unquote(strict?),
        unquote(matcher(cause_pattern)),
        unquote(matcher(effect_pattern)),
        unquote(matcher2(cause_pattern, effect_pattern, guard)),
        unquote(trace)
      )
    end
  end

  defp build_wait_async_action(pattern, block, opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError, "wait_async_action options must be a literal keyword list"
    end

    timeout = Keyword.get(opts, :timeout, :infinity)

    quote do
      :snabbkaffe.wait_async_action(
        fn -> unquote(block) end,
        unquote(matcher(pattern)),
        unquote(timeout)
      )
    end
  end

  defp build_find_pairs(cause_pattern, effect_pattern, guard, trace) do
    quote do
      :snabbkaffe.find_pairs(
        unquote(matcher(cause_pattern)),
        unquote(matcher(effect_pattern)),
        unquote(matcher2(cause_pattern, effect_pattern, guard)),
        unquote(trace)
      )
    end
  end

  # ?force_ordering(CONTINUE, N, DELAYED, GUARD): hold back DELAYED events until
  # N CONTINUE events have fired. Note the matcher argument order (delayed first).
  defp build_force_ordering(continue_pattern, n_events, delayed_pattern, guard) do
    quote do
      :snabbkaffe_nemesis.force_ordering(
        unquote(matcher(delayed_pattern)),
        unquote(n_events),
        unquote(matcher2(delayed_pattern, continue_pattern, guard))
      )
    end
  end
end
