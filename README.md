# Snabbkaffex

> **Snabbkaffex is a thin Elixir wrapper around the Erlang
> [snabbkaffe](https://github.com/kafka4beam/snabbkaffe) library.** It adds no
> testing engine of its own — all the collecting, matching, fault injection, and
> checking is done by snabbkaffe. Snabbkaffex only translates snabbkaffe's Erlang
> preprocessor macros into Elixir macros/functions and smooths a few rough edges
> for ExUnit. All credit for the underlying work goes to the snabbkaffe authors
> ([kafka4beam/snabbkaffe](https://github.com/kafka4beam/snabbkaffe)).

An Elixir interface to the Erlang [snabbkaffe](https://github.com/kafka4beam/snabbkaffe) trace-based testing library.

Instead of sprinkling `Process.sleep/1` and polling through your tests, you instrument your code with **trace points**, run it, and then make assertions against the **trace** which is the ordered list of events that were emitted. This decouples tests from implementation timing and makes concurrent/asynchronous code straightforward to test deterministically.

snabbkaffe ships its instrumentation as Erlang preprocessor macros (`-include_lib(...)`), which aren't usable from Elixir. Snabbkaffex provides the Elixir macro/function counterparts, plus a few ergonomic adjustments for Elixir and ExUnit:

* Check functions **pass unless they raise**, so you use ordinary ExUnit assertions (`assert`, `assert_receive`, ...) and get the real failure with message and stacktrace reported verbatim.
* Trace-querying functions take the `trace` as their **first argument**, so they pipe naturally.

## Installation

Where `snabbkaffex` belongs in `deps` depends on **where your trace points live**, because there are two separate needs:

* `snabbkaffex` (the macros) — `use Snabbkaffex`, `tp`, and `tp_span` are macros, so `Snabbkaffex` must be available at *compile time* in every environment where the instrumented code is compiled. This is true even outside `:test`, where the macros expand to a no-op: the module still has to exist for the macro to run and produce that no-op.
* `:snabbkaffe` (the engine) — the underlying Erlang library that actually collects and checks traces. It is only ever *called at runtime in `:test`* (outside `:test` the macros never emit a `:snabbkaffe` call).

In general you want to use it like this:

```elixir
def deps do
  [
    {:snabbkaffex, "~> 0.1.0"}
  ]
end
```

## Quick start

Instrument some code with `use Snabbkaffex, only: :trace`, which imports just the trace-point macros (`tp/2`, `tp/3`, `tp_span/3`, `tp_span/4`):

```elixir
defmodule MyApp.Worker do
  use Snabbkaffex, only: :trace

  def run(input) do
    tp(:worker_started, %{input: input})
    result = input * 2
    tp(:worker_finished, %{input: input, result: result})
    result
  end
end
```

Then test it. `use Snabbkaffex` (no options) brings in the full API. The trace collector is a **global singleton**, so a test module that runs traces must be `async: false`:

```elixir
defmodule MyApp.WorkerTest do
  use ExUnit.Case, async: false
  use Snabbkaffex

  test "run/1 emits start and finish events" do
    check_trace(
      # 1. Run the code under test; its return value becomes the result.
      fn -> MyApp.Worker.run(21) end,
      # 2. Assert on the result and the collected trace.
      fn result, trace ->
        assert result == 42
        assert [%{input: 21}] = of_kind(trace, :worker_started)
        assert [%{input: 21, result: 42}] = of_kind(trace, :worker_finished)
      end
    )
  end
end
```

(`use Snabbkaffex` emits a compile-time warning if the module is `async: true`, since concurrent tests would clobber each other's traces through the shared collector.)

## Event shape

A trace point `tp(:worker_started, %{input: 21})` is collected as a map:

```elixir
%{:"$kind" => :worker_started, :input => 21, :"~meta" => %{...}}
```

The event kind lives under the `:"$kind"` key and metadata (line, file, mfa, timestamp, ...) under `:"~meta"`. Prefer `of_kind/2` to filter by kind so you rarely need to spell `:"$kind"` out.

## Trace points

```elixir
tp(:kind, %{a: 1})                                  # debug event
tp(:kind, %{a: 1}, level: :warning)                 # explicit severity
tp(:kind, %{a: expensive()}, ignore_side_effects: true)

tp_span :work, %{id: id} do                         # start + {:complete, ret} events
  do_work(id)
end
```

Behaviour **outside `:test`** (controlled by the options):

| Form | Non-test behaviour |
| --- | --- |
| `tp(kind, data)` | evaluates `data` (side effects preserved), discards it → `:ok` |
| `tp(kind, data, level: :error)` | degrades to a `:logger` call, so it stays in production logs |
| `tp(kind, data, ignore_side_effects: true)` | pure `:ok`; `data` is **not evaluated at all** |

`:level` and `ignore_side_effects: true` are mutually exclusive. Passing both raises `ArgumentError` at compile time. They pull in opposite directions: `:level` keeps the event in production logs, while `ignore_side_effects: true` drops it to a bare `:ok` outside `:test`. Pick one.

`tp_span` accepts the same options alongside its `do` block. With `ignore_side_effects: true`, outside `:test` the span reduces to just the block.

## Running and checking a trace

`check_trace/2` runs a function, collects the trace, and passes it to a check function of arity 1 (`trace`) or 2 (`result, trace`). The check passes unless
it raises and its return value is ignored. `check_trace/3` takes a leading snabbkaffe run config (`%{timeout: 100}`) or an integer statistics bucket.

```elixir
check_trace(
  %{timeout: 100},
  fn -> MyApp.Worker.run(7) end,
  fn trace -> assert [%{input: 7}] = of_kind(trace, :worker_started) end
)
```

If an assertion inside the check (or an exception inside the run function) fires, Snabbkaffex re-raises it verbatim, so ExUnit shows the real error.

## Querying the trace

All of these take the `trace` first, so they pipe:

```elixir
trace
|> of_kind(:worker_started)      # keep events of a kind (or list of kinds)
|> projection(:input)            # extract a field (or list of fields → tuples)

find_pairs(trace, %{:"$kind" => :req}, %{:"$kind" => :resp})
causality(trace, %{:"$kind" => :req}, %{:"$kind" => :resp})          # assert cause precedes effect
strict_causality(trace, %{:"$kind" => :req}, %{:"$kind" => :resp})   # + forbid unmatched causes

# Pattern-matching guards can tie fields together across the two events:
causality(
  trace,
  %{:"$kind" => :req, id: i1},
  %{:"$kind" => :resp, id: i2},
  i1 == i2
)
```

Other helpers: `split_trace_at/2`, `splitl_trace/2`, `splitr_trace/2`, `of_domain/2`, `of_node/2`, `erase_timestamps/1`, `pair_max_depth/1`, and the assertions `unique/1`, `increasing/1`, `strictly_increasing/1`, `projection_complete/3`, `projection_is_subset/3`, `give_or_take/3`.

## Waiting for asynchronous events

When work happens in another process, block on the event instead of sleeping:

```elixir
check_trace(
  fn ->
    MyApp.ping_after(20, ref)
    assert {:ok, %{ref: ^ref}} = block_until(%{:"$kind" => :pong, ref: ^ref}, 1000)
  end,
  fn trace -> assert [%{ref: ^ref}] = of_kind(trace, :pong) end
)
```

* `block_until/2,3` — wait for one matching event. `block_until/4` waits for `n` matching events.
* `wait_async_action/3` — run an action, then wait for its resulting event.
* `subscribe/1..4` + `receive_events/1` — lower-level subscription (build the predicate with `match_event/1`).
* `retry/3` — retry a function until it stops raising.

## Fault injection and scheduling

Backed by `snabbkaffe_nemesis`:

```elixir
# Crash processes as they hit a trace point. With no strategy option it crashes
# on every match; pass one of recover_after:/random_crash:/scenario: to shape it.
ref = inject_crash(%{:"$kind" => :write}, reason: :disk_full)
inject_crash(%{:"$kind" => :flaky}, recover_after: 1)
inject_crash(%{:"$kind" => :flaky}, random_crash: 0.1)
inject_crash(%{:"$kind" => :wave}, scenario: periodic_crash(10, 0.5, 0.0))
fix_crash(ref)   # remove it again

# Hold back :delayed events until a :continue event has fired ("delay X until Y").
force_ordering(delay: %{:"$kind" => :delayed}, until: %{:"$kind" => :continue})
# ... optional `count:` (wait for N until-events) and `when:` (a guard tying the
# two patterns together by a shared field).
```

## Examples

The [`examples/`](https://github.com/edgurgel/snabbkaffex/tree/main/examples) directory has runnable, self-contained scripts. Each
one uses `Mix.install/2`, so you can run it straight from a checkout with no
project setup:

```sh
elixir examples/get_or_compute_race.exs
```

* [`get_or_compute_race.exs`](https://github.com/edgurgel/snabbkaffex/blob/main/examples/get_or_compute_race.exs) — a memoizing
  cache with a check-then-act race: two concurrent callers can both miss and
  both run the expensive computation. `force_ordering` pins the one bad
  interleaving so the bug reproduces on **every** run.
* [`retry_double_charge.exs`](https://github.com/edgurgel/snabbkaffex/blob/main/examples/retry_double_charge.exs) — an
  at-least-once job that charges a card and then acks. If the process dies in
  the window between the two, the retry charges again. `inject_crash` with
  `recover_after: 1` kills the first attempt at exactly that trace point (then
  lets the retry through), so the double-charge is reproduced deterministically
  instead of depending on a crash landing in a microseconds-wide gap.

## Multi-node

`forward_trace/1` makes a remote node's trace points land in this collector.
`unforward_trace/1` reverts it (useful before a deliberate `Node.disconnect/1`,
so forwarding messages or RPCs don't auto-reconnect the node).

## Documentation

Every function and macro is documented; see the `Snabbkaffex` moduledoc and `h Snabbkaffex.<name>` in IEx. For the underlying model and semantics, the [snabbkaffe README](https://github.com/kafka4beam/snabbkaffe) is the canonical reference.

## Credits

All the real work belongs to the [snabbkaffe](https://github.com/kafka4beam/snabbkaffe) authors. Snabbkaffex is just an Elixir-friendly veneer over their library.
