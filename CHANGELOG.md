# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-08-18

Initial release.

- Elixir trace-point macros (`tp/2`, `tp/3`, `tp_span/3`, `tp_span/4`) wrapping
  snabbkaffe's Erlang preprocessor macros, with production behaviour controlled
  by `:level` and `ignore_side_effects:`.
- `check_trace/2,3` running and collecting a trace, with checks that pass unless
  they raise so ordinary ExUnit assertions work.
- Trace-querying helpers (`of_kind/2`, `projection/2`, `find_pairs/3`,
  `causality/3,4`, `strict_causality/3,4`, and friends), all taking the trace
  first so they pipe.
- Waiting helpers (`block_until/2,3,4`, `wait_async_action/3`, `subscribe`,
  `receive_events/1`, `retry/3`).
- Fault injection and scheduling via `snabbkaffe_nemesis` (`inject_crash`,
  `fix_crash`, `force_ordering`).
- Multi-node trace forwarding (`forward_trace/1`, `unforward_trace/1`).

[0.1.0]: https://github.com/edgurgel/snabbkaffex/releases/tag/v0.1.0
