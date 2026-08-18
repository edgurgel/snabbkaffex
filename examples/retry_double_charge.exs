# A self-contained Snabbkaffex showcase: catching a "crash at exactly the wrong
# moment" bug — deterministically — with fault injection.
#
# Run it:
#
#     elixir examples/retry_double_charge.exs
#
# (Snabbkaffex trace points only record when Mix.env() == :test; the line below
# sets that before Mix.install so the script is a single `elixir ...` command.
# See examples/get_or_compute_race.exs for the full explanation.)
#
# The bug here is a classic of at-least-once systems: a job that is retried on
# crash, wrapped around a side effect that is NOT idempotent. If the process
# dies *after* the side effect but *before* it records that it's done, the retry
# runs the side effect a second time. With real infrastructure you'd need to
# kill the process in a window a few microseconds wide to see it. `inject_crash`
# aims a crash at that exact trace point, so it happens on demand, every run.

System.put_env("MIX_ENV", "test")

Mix.install([
  {:snabbkaffex, path: Path.expand("..", __DIR__)}
])

ExUnit.start()

# ---------------------------------------------------------------------------
# The code under test: charge a card, then record the ack.
#
# There is no idempotency key, so nothing stops a second run from charging
# again. The danger window is between `:charge` (the money moves) and `:ack`
# (we durably record that this order is settled).
# ---------------------------------------------------------------------------
defmodule Payments do
  use Snabbkaffex, only: :trace

  # `charges` is a :counters ref standing in for the external money movement —
  # we bump it every time a charge actually happens.
  def settle(charges, order_id) do
    tp(:charge, %{order_id: order_id})
    :counters.add(charges, 1, 1)

    # Crash-injection point: the gap where a real process might die (a deploy,
    # an OOM, a node loss) after charging but before acking.
    tp(:before_ack, %{order_id: order_id})

    tp(:ack, %{order_id: order_id})
    :ok
  end
end

# A stand-in for an at-least-once job runner — a supervisor restart, a broker
# redelivering an unacked message, Oban retrying a failed job. On *any* crash it
# re-runs the whole job from the top.
defmodule AtLeastOnce do
  def run(fun, attempts_left) do
    fun.()
  catch
    _kind, _reason when attempts_left > 0 ->
      run(fun, attempts_left - 1)
  end
end

defmodule PaymentsTest do
  use ExUnit.Case, async: false
  use Snabbkaffex

  setup do
    %{charges: :counters.new(1, [])}
  end

  # Nothing crashes: the job runs once, charges once, acks once. Looks correct —
  # and is, as long as no process ever dies at the wrong instant.
  test "the happy path charges exactly once", ctx do
    check_trace(
      fn -> AtLeastOnce.run(fn -> Payments.settle(ctx.charges, "order-1") end, 3) end,
      fn result, trace ->
        assert result == :ok
        assert :counters.get(ctx.charges, 1) == 1
        assert length(of_kind(trace, :charge)) == 1
        assert length(of_kind(trace, :ack)) == 1
      end
    )
  end

  # Now inject a crash at `:before_ack`. `recover_after: 1` crashes only the
  # first time that trace point is hit, then lets subsequent hits through — so
  # the retry can succeed. That pins the one schedule that exposes the bug:
  #
  #   attempt 1: charge (money moves!) → before_ack → CRASH
  #   attempt 2: charge (money moves AGAIN) → before_ack → ack → :ok
  #
  # Charged twice, acked once.
  test "a crash between charge and ack double-charges on retry (the bug)", ctx do
    check_trace(
      fn ->
        inject_crash(%{:"$kind" => :before_ack}, recover_after: 1, reason: :simulated_crash)
        AtLeastOnce.run(fn -> Payments.settle(ctx.charges, "order-1") end, 3)
      end,
      fn result, trace ->
        assert result == :ok

        # The retry re-ran the whole job, so the card was charged twice...
        assert :counters.get(ctx.charges, 1) == 2
        assert length(of_kind(trace, :charge)) == 2

        # ...but only the surviving attempt reached the ack.
        assert length(of_kind(trace, :ack)) == 1

        # And snabbkaffe recorded exactly one injected crash — proof the fault
        # fired where we aimed it, not somewhere incidental.
        assert length(of_kind(trace, :snabbkaffe_crash)) == 1
      end
    )
  end
end
