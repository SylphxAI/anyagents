# Harness verification

Run the deterministic harness acceptance checks from a checkout:

```sh
bash scripts/verify-harnesses.sh
```

Prerequisites: Bash, a Unix host and the Rust version specified in
`Cargo.toml` (or newer). The first run downloads Cargo dependencies; no vendor
subscription, API key, installed vendor CLI or model call is required. The
script stops at the first failure and returns a nonzero exit status. Tests use
temporary data directories rather than your existing AnyAgents state.

## What a pass means

| Layer | Evidence |
| --- | --- |
| Native protocols | Each of Claude Code, Codex, OpenCode, agy, Muse and omp has an adapter `protocol` suite using scripted vendor processes. These exercise the native launch and wire contracts, turn completion, retries, and the adapter's steering or buffering behavior. |
| Shared transport | `anyagents-harness-stream` tests the shared stream/RPC machinery used by adapters, including dsh and grok. This is not a dedicated live dsh/grok compatibility check. |
| Server round trips | `server_harnesses` creates agents for all eight catalog harnesses and checks two successive correlated mailbox replies per harness through the real server, supervisor and MCP tool server. It substitutes a common contract double: passing does **not** prove native vendor protocol compatibility. |
| Native server integration | `server_opencode` additionally exercises the OpenCode adapter through a scripted `serve`/`api` CLI rather than the common contract double. |
| Reusable sign-in and API | `signin`, `mcp` and `identity` check persisted sign-in state, tools and capability identity with deterministic fixtures. They do not authenticate a real vendor subscription. |
| Durability and lifecycle | The invariants and supervisor suites check mailbox ordering, deduplication, retry/crash behavior and supervised agent lifecycle. |

For the complete repository gate (including gateway, channels and launcher):

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
npm test
```

## Boundaries and live acceptance

Scripted processes verify our implementation against fixtures, not whether a
new release of a vendor still implements those protocols. Before claiming a
particular dev machine is connected, separately check the installed version of
each selected vendor CLI, sign it in, create an agent with that harness, and
observe replies to two distinct mailbox messages. Check a mid-turn follow-up
where supported; adapters that buffer until the next turn must not be described
as supporting native mid-turn insertion. Keep credentials out of test logs.

Local execution runs as the host user; per-agent state directories and MCP
capabilities are **not** an OS security sandbox. Container/Pod/user isolation
requires a configured runtime boundary and a separate execution check on that
runtime. This script does not certify Docker, Kubernetes, cross-user isolation,
or live authentication. See [ADR 0002](adr/0002-runtime-drivers-and-capability-identity.md).
