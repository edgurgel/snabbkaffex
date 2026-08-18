# A self-contained Snabbkaffex showcase: catching a concurrency bug that is
# almost impossible to reproduce with `Process.sleep/1`, deterministically, on
# every single run.
#
# Run it:
#
#     elixir examples/get_or_compute_race.exs

System.put_env("MIX_ENV", "test")

Mix.install([
  {:snabbkaffex, path: Path.expand("..", __DIR__)}
])

ExUnit.start()

# ---------------------------------------------------------------------------
# The code under test: a naive "get or compute" (memoizing) cache.
#
# `get_or_compute/4` looks up a key where on a miss it runs the (expensive) `fun`
# and stores the result. The bug: lookup and store are NOT atomic. Two
# processes can both look up the same key, both miss, and both run `fun`.
#
# This is the classic check-then-act race. Under normal tests it hides: the
# compute is so fast that a second caller almost always sees the stored value.
# ---------------------------------------------------------------------------
defmodule DedupCache do
  use Snabbkaffex, only: :trace

  # `store` is our shared state (a map). `compute_count` is a :counters ref we
  # bump every time the "expensive" work actually runs — that's how we detect
  # the double-compute.
  def get_or_compute(store, compute_count, key, fun, worker) do
    tp(:lookup, %{key: key, worker: worker})

    case Agent.get(store, &Map.fetch(&1, key)) do
      {:ok, value} ->
        tp(:hit, %{key: key, worker: worker})
        value

      :error ->
        tp(:miss, %{key: key, worker: worker})
        value = fun.()
        :counters.add(compute_count, 1, 1)
        tp(:store, %{key: key, worker: worker})
        Agent.update(store, &Map.put(&1, key, value))
        value
    end
  end
end

defmodule DedupCacheTest do
  use ExUnit.Case, async: false
  use Snabbkaffex

  setup do
    {:ok, store} = Agent.start_link(fn -> %{} end)
    compute_count = :counters.new(1, [])
    %{store: store, compute_count: compute_count}
  end

  # A baseline showing the cache looks perfectly correct when nothing races:
  # call it twice in one process and the second call is a hit.
  test "sequentially, the second caller hits the cache", ctx do
    check_trace(
      fn ->
        DedupCache.get_or_compute(ctx.store, ctx.compute_count, :pi, fn -> 3.14 end, :a)
        DedupCache.get_or_compute(ctx.store, ctx.compute_count, :pi, fn -> 3.14 end, :a)
      end,
      fn trace ->
        # Exactly one compute; the trace shows one miss followed by one hit.
        assert :counters.get(ctx.compute_count, 1) == 1
        assert [:miss, :hit] = trace |> of_kind([:miss, :hit]) |> projection(:"$kind")
      end
    )
  end

  # The same cache, now with two concurrent callers — and `force_ordering` to
  # pin the ONE interleaving that triggers the bug, so it reproduces every run
  # instead of one time in a thousand.
  #
  # force_ordering holds back every `delay:` event until a matching `until:`
  # event has fired. Here: hold worker `:a`'s `store` until worker `:b`'s `miss`
  # has happened. That pins the nasty schedule:
  #
  #   a: lookup → miss → (store BLOCKED, waiting for b's miss)
  #   b: lookup → miss  (a hasn't stored yet, so b also misses!) → store
  #   a: store released
  #
  # Both workers miss, both run `fun`. The dedup cache computed twice.
  test "concurrently, both callers miss and compute twice (the bug)", ctx do
    check_trace(
      fn ->
        force_ordering(
          delay: %{:"$kind" => :store, worker: :a},
          until: %{:"$kind" => :miss, worker: :b}
        )

        parent = self()

        for worker <- [:a, :b] do
          spawn(fn ->
            DedupCache.get_or_compute(ctx.store, ctx.compute_count, :pi, fn -> 3.14 end, worker)
            send(parent, {:done, worker})
          end)
        end

        assert_receive {:done, :a}, 2000
        assert_receive {:done, :b}, 2000
      end,
      fn trace ->
        # Deterministic proof of the race: two misses, and `fun` ran twice.
        assert [:a, :b] = trace |> of_kind(:miss) |> projection(:worker) |> Enum.sort()
        assert :counters.get(ctx.compute_count, 1) == 2

        # This is what a correct cache would guarantee and this one does not:
        # every miss's compute finishing before the next caller looks up.
        # We assert the *violation* here to make the bug explicit.
        assert length(of_kind(trace, :miss)) == 2,
               "expected the race: both workers missed the cache"
      end
    )
  end
end
