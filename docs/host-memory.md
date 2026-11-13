# Host memory and history

AnyAgents pins learned memory across harnesses and searches authorized learned
facts, mailbox bodies and every harness's native history through
`conversation`. Raw history is the only source of truth: apart from the
audited [redaction operation](redaction.md), AnyAgents never rewrites,
summarizes or deletes it, and every index over it is a rebuildable
projection whose results cite the raw bytes they came from. Native transcript
and runtime-state writers remain the harnesses' own.

Retrieval is hybrid: BM25 over the host index, dense candidates from a local
multilingual embedder fused by reciprocal rank, and a local cross-encoder
reranking the fused head. The model work runs in a separate process,
`anyagents memory-service`; without it the server ranks lexically and says so
in each response.

## Native history coverage

The host index reads each harness's history where AnyAgents points the
harness's configuration, below the agent root and never through a symlink:

| Harness | Source | Format |
| --- | --- | --- |
| any | `transcripts/**` | JSONL written by AnyAgents; a malformed line fails the refresh |
| Spiron | `config/sessions.sqlite3`, table `session_entries` | one event per row |
| Spiron (before 2026-11-08) | `config/sylphx-transcript.jsonl` | JSONL |
| Claude Code | `config/{,.claude/}projects/*/*.jsonl`, `projects/*/*/subagents/*.jsonl` | JSONL |
| Codex | `config/sessions/**/*.jsonl` (`CODEX_HOME`) | JSONL, item under `payload` |
| OpenCode | `config/data-home/opencode/opencode.db`, `config/.local/share/opencode/opencode.db`, table `session_message` | one message per row, tool parts included |
| AGY | `config/.gemini/antigravity-cli/brain/*/.system_generated/logs/transcript_full.jsonl` | JSONL, time in `created_at` |
| Muse | `config/data-home/muse/sessions/**/session.jsonl`, subagents included | JSONL frames |
| OMP | `config/.omp/agent/sessions/*/*.jsonl` (`PI_CODING_AGENT_DIR`) | JSONL |
| Grok | `config/.grok/sessions/*/*/chat_history.jsonl` | JSONL |

History is read where its harness keeps it; AnyAgents writes no copy. Spiron's
journal in `sessions.sqlite3` is its complete record, tool calls and results
included; a copy of the headless stream written beside it would be a second,
masked record that could disagree with the first. A `spiron-cloud` agent's
history lives with the cloud service and is not on the host; only its mailbox
is searchable here.

A harness's own JSONL can carry a line its writer tore or concatenated; such a
complete line is indexed as text with unknown metadata instead of failing the
agent's search. AGY's `transcript.jsonl` and the `chunks/` copies duplicate
`transcript_full.jsonl` and are not read; Muse's hidden `.msp-view-*`
snapshots are not history. dsh writes zstd-compressed `session.jsonl.zstd`,
which is not read until a raw coordinate can address bytes inside a compressed
frame; no dsh agent exists today.

A SQLite store is opened read-only. A reader of a WAL database creates the
`-wal` and `-shm` files when they are absent, owned by the reader, which could
leave the harness unable to open its own store; a store without both files is
therefore read as immutable (no lock, no new file), and a store with a live
writer is read through its WAL. A row's coordinate is
`<database>/<table>/<rowid>` with offset 0, the row's length and SHA-256: a
database is a regular file, so no real path collides with it. A refresh reads
rows past the published rowid frontier, plus, for OpenCode, rows rewritten
after the published `time_updated` mark, in one snapshot of the store. A
replaced database, a lower frontier or a changed frontier row rebuilds the
store; deleted rows remain published until hourly full verification and are
refused at readback by their digest. A store with an unknown layout is not
read. The walk stamps the store and its `-wal`, so new rows in the log are
seen before a checkpoint touches the database file.

## Learned memory

Learned memory is what an agent chooses to write with the `memory` tool:
topic files under `memory/`, and the `MEMORY.md` catalog AnyAgents rebuilds
deterministically from them on every write (`memory` action `reindex` rebuilds
it after an out-of-band edit). No model reads, merges, rewrites or deletes a
topic.

Sleep consolidation, which asked a hosted model every ten minutes per idle
agent to upsert and delete topics from the last 30 mailbox turns, was removed
rather than turned into a citation-backed note layer. Raw-history retrieval
with dense retrieval and reranking ranks first against agentic memory systems,
and even complete access to a transformed memory store trails raw evidence
(arXiv:2610.30755); short working contexts lose little when raw history can be
retrieved on demand (arXiv:2611.05306). The pass saw a clipped slice of the
mailbox and none of the tool history, replaced the agent's own notes in place,
and spent a paid model call per pass. A derived layer would need a generator
model again and could only add staleness between it and the evidence.
`memory/.sleep-state.json` files written by older builds are inert dotfiles,
never indexed; an operator may delete them.


## Files and session boundaries

| Agent-root path | Purpose |
| --- | --- |
| `memory/MEMORY.md` (default) | Caller-selectable learned-memory index, included only when a new host session is pinned |
| `memory/**` | Learned facts, preserving original bytes and Claude frontmatter |
| `skills/**` | Agent-authored overlay; shared global skills remain read-only |
| `history/archive/**` | Reconciled progress pages; searchable history, never prompt memory |
| `sessions/**` | Host generation, saved memory pins and vendor session aliases |
| `transcripts/**`, harness history under `config/` | Native history indexed in place ([coverage](#native-history-coverage)); each writer keeps ownership |

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

The host pin API accepts an optional source path independently of harness-native
memory. Relative paths resolve against the agent root; absolute paths select an
operator-authorized file. A missing source yields an empty snapshot, while
unreadable or invalid UTF-8 input fails session creation. Selecting no source
pins an empty section without creating or reading a memory directory. Existing
pins remain immutable even when source selection changes. Conventional wrappers
retain `memory/MEMORY.md` as a compatibility default; launch configuration can
select another source or disable host prompt memory. Native memory is separately
controlled by the caller's harness configuration, not by the pinning mechanism.
Renderer and scripted harness tests are not vendor-runtime proof.

## Conversation and raw reads

`conversation` remains one of the six canonical tools. Its `scope` is `all`
(default), `memory` or `transcripts`; the latter includes mailbox turns, retained
native transcripts and reconciled history pages. Non-admin agents can read
self and descendants only. `all`, `*`, names and immutable IDs never widen that
boundary. Authorization applies before file access and result limits.

Lexical search uses full mailbox bodies and bounded host-file FTS windows.
The host SQLite index stores metadata, cursors, call correlation and a
contentless FTS index; authoritative host-file payloads stay in filesystem
sources. Archive retrieval also reads the historical pre-upgrade directory
spelling; new migrations write only `history/archive`. Window coordinates identify segment, byte offset, length, SHA256 and
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

Outbound effects can retain pending history obligations under `outbound-history`.
Publication/reconciliation share a barrier with erasure: erasure takes the
exclusive `.publication.lock` before lifecycle and SQLite locks, so senders can
finish history settlement without lock inversion. A matching-agent or unknown,
unreadable, malformed, oversized or linked `.pending` obligation or `.tmp` copy
refuses erasure before any mutation, including when reusing an earlier receipt.
A settled named obligation does not prove that a crashed writer's synced
pre-rename temporary is absent; those copies remain in the same census.
Reconciliation owns these recovery records; erasure never edits or replays an external effect.
Valid foreign obligations do not block a target, and terminal `.done` metadata
must contain neither body bytes nor their digest.

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

## Hybrid retrieval and the memory service

`anyagents memory-service` is a separate process beside `anyagents serve`. It
reads the server's host index and raw sources read-only, writes only its own
dense index (`<data>/memory-service/dense-v1.db`, rebuildable, never
`state.db`), and answers on an owner-only socket
(`<data>/memory-service.sock`, mode `0600`) with one newline-delimited JSON
request and reply per connection:

| Request | Reply |
| --- | --- |
| `search {query, owners, source?, since_ms?, until_ms?, limit}` | dense candidates: document id, owner, cited raw range (absolute offset, length, SHA-256), score |
| `rerank {query, passages}` | one cross-encoder score per passage |
| `status` | model pair, embedded and pending documents, vectors |

The server decides access. It sends only the owners the caller may read, and
resolves each candidate the way it resolves a lexical hit: the document must
belong to an authorized owner and scope, match every structured filter of the
request, still have the digest the candidate was built from, not be excluded by
a pending redaction, and contain the cited range, whose bytes are read back and
checked against the range's own SHA-256 under the owner's lifecycle lock. A
candidate failing any check is dropped. Lexical and dense hits are fused by
reciprocal rank (k = 60); a document found by both keeps its lexical window.
The fused list is merged with mailbox results, and its first 20 turns are
reordered by the cross-encoder. A relevance search's response carries
`retrieval.stages` (`lexical`, `dense`, `rerank`) and, for a stage that did
not run, `retrieval.degraded` with the reason. Search waits at most 3 s for
the service and rerank 8 s; past that the server answers without the stage
and says so. Reads in time order, range reads and neighbor expansion rank
nothing and do not call the service.

Mailbox rows are ranked lexically and by the reranker, not embedded: the
messages a harness received and the `send` calls it made reach dense
retrieval through that harness's own history.

What is embedded: each host-index document's raw event is read through its
verified coordinate; its JSON string values (not keys, ids, digests,
timestamps or numbers; nested JSON strings are read for their own values) are
packed into pieces of at most the model's token budget, up to 16 pieces per
document, each piece citing the raw byte range of the values it holds. The
rest of a very large event stays reachable lexically. Identical text is
embedded once. Vectors are L2-normalised and stored as per-vector int8.

The background embedder takes new documents first and then the backlog newest
first; both marks commit with each document, so a restart resumes where it
stopped. It runs `--index-threads` workers (default 1) at nice 19 (`--nice`),
each a single-threaded session over its own stripe of documents with its own
marks, so the CPU it may take is exactly that many cores and only when no
normal-priority work wants them. Query embedding and reranking run on their
own threads at normal priority (`--query-threads`, default 2). An idle embedder checks for new documents every
30 s. Every 10 minutes it drops vectors of documents the host index no longer
holds at the same digest, so removed, redacted or rewritten history leaves
the dense index within that interval; until then the server's resolution
step refuses such candidates.

Models and runtime are pinned in `crates/anyagents-memory/src/pins.rs` by
Hugging Face commit and per-file SHA-256, and ONNX Runtime 1.31.0 by the
official release archive's SHA-256. `anyagents memory-service fetch`
downloads and verifies them into `<data>/memory-service/models`; `serve`
refuses to start unless every file matches its pin. ONNX Runtime is loaded at
run time (`ort` with `load-dynamic`); the server binary links no model
runtime.

## Retrieval quality on real history

`anyagents memory-service eval-sample --source <data> --agent <id>... --out <dir>`
copies chosen agents' mailboxes out of `state.db`, opened read-only, and
derives queries from the history itself: when the operator replies in
Telegram to an earlier message, the reply without its quote is a later
message that refers back to an earlier fact, and the quoted message (matched
on normalized text, earlier in the same mailbox, ignoring other replies'
quotes) is the document that answers it. `anyagents memory-service eval
--sample <dir>` indexes the sample with the production host index and the
production dense pipeline, searches only history before each query, and
scores five rankings of the same candidates: BM25 alone, dense alone, their
reciprocal-rank fusion, the fusion reranked (first 20), and BM25 reranked.
Nothing in the live system is written. The same code runs as
`cargo run -p anyagents-memory --features service --release --example
memory-eval` for development.

Results on 2026-11-13, from two lead agents whose operator talks to them in
Telegram, mostly in Cantonese. Recall@k counts a query as answered when the
quoted message is in the first k; MRR@10 and nDCG@10 use the same single
relevant message. The reranker is `hs-rerank-1` throughout.

5-day window: 6,941 messages, 170 queries (154 Cantonese, 5 Chinese, 11 English).

| Ranking | 311M-r2: R@10 | MRR@10 | nDCG@10 | 97M-r2: R@10 | MRR@10 | nDCG@10 |
| --- | --- | --- | --- | --- | --- | --- |
| BM25 | 0.253 | 0.131 | 0.160 | 0.253 | 0.131 | 0.160 |
| Dense | 0.294 | 0.134 | 0.172 | 0.235 | 0.106 | 0.136 |
| Hybrid (RRF) | 0.324 | 0.149 | 0.190 | 0.288 | 0.138 | 0.173 |
| BM25 + rerank | 0.265 | 0.147 | 0.174 | 0.265 | 0.147 | 0.174 |
| Hybrid + rerank | **0.347** | **0.176** | **0.216** | 0.335 | 0.169 | 0.208 |

10-day window: 13,656 messages, 380 queries (335 Cantonese, 16 Chinese, 29
English).

| Ranking | 97M-r2: R@10 | MRR@10 | nDCG@10 |
| --- | --- | --- | --- |
| BM25 | 0.226 | 0.114 | 0.139 |
| Dense | 0.176 | 0.081 | 0.104 |
| Hybrid (RRF) | 0.245 | 0.110 | 0.142 |
| BM25 + rerank | 0.245 | 0.127 | 0.154 |
| Hybrid + rerank | **0.300** | **0.146** | **0.182** |

The absolute numbers are low because the task is hard and strictly scored: a
short reply such as `點解又要再註冊多次嘅？` must find the one message it quoted
among thousands of status reports on the same subjects, and another message
carrying the same fact scores nothing. What the numbers show: hybrid
retrieval with reranking beats BM25 alone at every cutoff, by about a third in
MRR@10; fusion helps even when dense alone is weaker than BM25 (97M); the
reranker adds most of the gain at the top ranks. With 170 queries a
difference below about 0.03 is within noise. Reranking 20 passages took about
1.2 s on five threads of a shared, loaded host.

Embedding throughput in the same runs (model time per thread, nice 19, host
load about 26 on 70 cores): 311M-r2 int8 437 tokens/s, 97M-r2 int8 about
1,500 tokens/s. The dense index takes about 1.6 KB per 768-dimension vector
and 1.2 KB per 384-dimension vector, indexes included.

The default embedder is `granite-embedding-311m-multilingual-r2` (int8, IBM's
ONNX export, Apache-2.0): it is the better dense retriever on this history
(dense MRR@10 0.134 against 0.106) and keeps Cantonese-only characters whole,
where the 97M model's pruned vocabulary breaks all 26 tested ones into bytes;
the cost is about 3.4 times the CPU per token. The default reranker is
`hs-rerank-1` (MIT, mmBERT-small, about 5 pairs/s per thread at 512 tokens),
fast enough to rerank 20 passages inside the server's 8 s bound on two
threads. Larger cross-encoders (`bge-reranker-v2-m3`, Apache-2.0) score
higher on published multilingual benchmarks but take several seconds per
query on CPU. Every candidate in `pins.rs` can be selected with
`--embedder` / `--reranker` and evaluated with the same command.

## Incremental indexing and migration

An unchanged inode/device, nonshrinking source and matching **64 KiB** frontier
window allow suffix-only indexing. Replacement, shrink or boundary mismatch
rebuilds the source. A host-driven low-priority verifier revisits older prefixes
(periodic eligibility after one hour, bounded work per pass). A same-size rewrite
outside the frontier window can remain stale until that verifier reaches it;
there is no immediate full-history integrity guarantee on the fast path. Source
cursor, document membership and call correlation publish atomically.

Only daemon authority migrates data. Stable registry ownership and aliases map
known per-agent Claude memory and caller-selected progress pages. The resumable manifest
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
