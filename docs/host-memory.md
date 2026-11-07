# Host memory and history

AnyAgents pins learned memory across harnesses and searches authorized learned
facts, mailbox bodies and retained native history through `conversation`.
Native transcript and runtime-state writers remain unchanged in this delivery;
there is no dense model, embedding dependency or background inference service.

## Files and session boundaries

| Agent-root path | Purpose |
| --- | --- |
| `memory/MEMORY.md` | Learned-memory index, included only when a new host session is pinned |
| `memory/**` | Learned facts, preserving original bytes and Claude frontmatter |
| `skills/**` | Agent-authored overlay; shared global skills remain read-only |
| `history/owners/**` | Reconciled progress pages; searchable history, never prompt memory |
| `sessions/**` | Host generation, saved memory pins and vendor session aliases |
| `config/*transcript*.jsonl`, retained transcript paths | Native history indexed in place; existing writer keeps ownership |

The rendered memory section, including its wrapper and visible truncation
marker, is at most **150 lines and 4096 UTF-8 bytes**. All instruction mirrors
receive the same saved section. Later memory edits do not change an existing
session, including across daemon restart, configuration rendering or vendor-ID
binding. Missing or damaged saved pins refuse reuse instead of silently
reading current memory. Other learned files are available for retrieval but are
not concatenated into the pinned prompt.

A managed agent can start a fresh session through `agents` with
`action: "reset_session"` (alias `"new_session"`), the CLI
`anyagents agents reset-session <name>`, or `POST /v1/agents/{name}/session/reset`.
Management authority and process/state quiescence are rechecked under the
lifecycle lock before any pin or current-pointer mutation. A prepared intent
binds the new pin and old vendor pointer digest for crash recovery. Reset retires
the current pointer; it does not delete old pins, aliases, raw history or vendor
SQLite databases. An OpenCode session-start marker binds to the already-pinned
host generation, rather than reading memory again after dispatch.

Claude launches force `CLAUDE_CODE_DISABLE_AUTO_MEMORY=1` after caller-supplied
environment so the host owns learned-memory projection. Renderer and scripted
harness tests establish this integration; they are not vendor-runtime proof.

## Conversation and raw reads

`conversation` remains one of the six canonical tools. Its `scope` is `all`
(default), `memory` or `transcripts`; the latter includes mailbox turns, retained
native transcripts and reconciled history pages. Non-admin agents can read
self and descendants only. `all`, `*`, names and immutable IDs never widen that
boundary. Authorization applies before file access and result limits.

Lexical search uses full mailbox bodies and bounded host-file FTS windows.
The host SQLite index stores metadata, cursors, call correlation and a
contentless FTS index; authoritative host-file payloads stay in filesystem
sources. Window coordinates identify segment, byte offset, length, SHA256 and
sequence. A pre-upgrade mailbox prefix is repaired from its full authorized body
at runtime, inside an IMMEDIATE transaction; no state backfill lives in the SQL
schema migration. Broadcast deduplication hashes the full message, so equal
prefixes with different tails remain separate results.

Use a result's opaque `id` as input `raw_id` and its `raw.sha256` as input
`raw_sha256`, with optional `raw_offset` and `raw_length`, to retrieve exact
`raw_bytes`. Each request returns at most
**65536 bytes**. Callers cannot supply paths. The host resolves the authorized
owner and scope first, repeats validation under its lifecycle lock, checks the
whole-event digest and gates unresolved redaction before reading. The returned
window is a bounded read, not a claim that the whole event fits the response.

Native time comes from explicit `timestamp_ms`, including zero, or RFC3339
`timestamp`. Missing/invalid time stays `null` (CLI: `unknown`); time filters
exclude unknown times. It is never inferred from filesystem mtime. Learned-file
snapshot times describe the snapshot, not when a historical fact occurred.

Source-shaped tool fixtures cover top-level tool names, Responses
`function_call`/`function_call_output` and Claude `tool_use`/`tool_result` blocks.
Call-ID correlation persists across reopen and append, scoped to owner and
source. `tool_name` can match any tool in a multi-tool event; unresolved results
remain tools with unknown names. Metadata projection caps each encoded JSON
string at **4096 bytes** and each content array at **1024 tool blocks**. Large
payload strings are discarded only in the derivative metadata projection; raw
bytes and lexical coverage remain unchanged. Excessive tool-block metadata
refuses and rolls back the source refresh rather than publishing a partial
cursor. These fixtures are not captured vendor-runtime conformance evidence.

## Incremental indexing and migration

An unchanged inode/device, nonshrinking source and matching **64 KiB** frontier
window allow suffix-only indexing. Replacement, shrink or boundary mismatch
rebuilds the source. A host-driven low-priority verifier revisits older prefixes
(periodic eligibility after one hour, bounded work per pass). A same-size rewrite
outside the frontier window can remain stale until that verifier reaches it;
there is no immediate full-history integrity guarantee on the fast path. Source
cursor, document membership and call correlation publish atomically.

Only daemon authority migrates data. Stable registry ownership and aliases map
known per-agent Claude memory and owner progress pages. The resumable manifest
at `host-memory-migration/manifest-v1.json` records paths, counts, lengths,
checksums, statuses and readback receipts, not source contents. Verified
same-filesystem moves are atomic; staged recovery retains prepared evidence.
Ambiguous, dangling, active-writer and unreadable sources remain explicit
retentions or deferrals. A busy global migration lock is a retryable launch wait,
not a terminal sub-agent error. Migration errors cannot prevent daemon/admin
socket binding, and one owner's discovery failure does not block another owner.

Personal/global Claude projects, `.codex/memories` and Codex SQLite state remain
untouched. The exclusion inventory derives paths only from an absolute declared
`HOME`; it never discovers, opens, fingerprints or stages their contents. Native goals, continuation/session, placement and usage state remain
with their current writer; staged snapshots do not claim final catch-up. The
raw-segment append/restart/checksum/quarantine and rename-handoff primitives are
tested, but native raw writer cutover and raw rename are **not activated** here.
No compatibility symlink or multi-gigabyte raw import into SQLite is required.

## Measured canonical-route cost

On 2026-11-07 at 05:16 UTC, Linux x86_64 debug source
`452ecfef1cb667b364234572f8e6dc1e7c79aef4` measured a synthetic 34,369,948-byte
corpus: three owners, 96 mailbox messages, 384 native events and three memory
files. Parent and child were visible; the unrelated peer remained hidden and
its old mailbox rows were not upgraded. Corpus manifest SHA256:
`1d386a6d0f93935225a60d9912bb80e25ba5966d9cda1efe160a0a7be109410f`.

| Phase | Wall time | CPU / memory |
| --- | --- | --- |
| First upgrade: absent host index plus old mailbox prefixes | 30,951.228 ms | 17,301.426 ms user + 10,982.147 ms system |
| 20 unchanged-corpus calls with distinct query strings | p50 50.519 ms; p95 82.269 ms; max 246.977 ms | Per-call user/system times emitted in JSON receipt |
| Store reopen, persisted indexes | 55.308 ms | 34.356 ms user; 0 ms system |
| Process peak, including fixture setup | — | 22,500 KiB RSS |

This calls production `conversation_route`, including ACL checks, host refresh,
IMMEDIATE mailbox backfill, retrieval and JSON encoding. It excludes HTTP/socket
transport. OS page cache was uncontrolled; affinity was CPUs 0–95, cgroup quota
`7000000 100000`. These are debug synthetic latency measurements, not production
latency, recall@k, multilingual quality or dense-retrieval claims. First indexing
cost is material and is not represented by the warm numbers.

Reproduce in an existing owned disposable directory with the existing Cargo
target (Linux only):

```sh
ANYAGENTS_HISTORY_BENCHMARK=1 \
ANYAGENTS_BENCH_SOURCE_SHA="$(git rev-parse HEAD)" \
ANYAGENTS_HISTORY_BENCHMARK_DIR="$CARGO_TARGET_DIR" \
cargo test -p anyagents --bin anyagents canonical_history_latency --locked -- \
  --ignored --nocapture
```

The ignored test requires explicit opt-in, checks exact finite result counts and
hidden-owner exclusion, and emits a JSON receipt. Required alternate-UID
admission checks and their CI-only acceptance boundary are documented in
[verification.md](verification.md#host-memory-under-a-managed-uid).
