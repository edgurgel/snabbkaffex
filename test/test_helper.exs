# Start distribution so multi-node tests (`:peer`, `forward_trace/1`) can run.
# Harmless if the node is already alive (e.g. `mix test --sname ...`).
unless Node.alive?() do
  {:ok, _} = :net_kernel.start([:"snabbkaffex_ct@127.0.0.1", :longnames])
end

ExUnit.start(capture_log: true)
