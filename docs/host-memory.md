# Host memory and history

AnyAgents pins learned memory across harnesses and searches authorized learned
facts, mailbox bodies and every harness's native history through
`conversation`. Raw history is the only source of truth: apart from the
audited [redaction operation](redaction.md), AnyAgents never rewrites,
summarizes or deletes it, and every index over it is a rebuildable
projection whose results cite the raw bytes they came from. Native transcript
and runtime-state writers remain the harnesses' own.

Memory has three tiers. L1 is `MEMORY.md`, the catalog pinned into every
session; L2 is the topic files, written by the agent with the `memory` tool and
by sleep consolidation while it is idle; L3 is recall over raw history through
`conversation`. Raw history is the truth the other two point back to.

Retrieval is hybrid: BM25 over the host index (CJK text indexed as character
bigrams beside ASCII words), dense candidates from an embedding model fused by
reciprocal rank, and a cross-encoder reranking the fused head. Which embedding
and rerank models run is chosen through AnyAgents profiles
([providers](#embedding-and-rerank-providers)); with nothing configured, the
default for an open-source install, both are pinned local ONNX models and no
request leaves the host. The model work runs in a separate process,
`anyagents memory-service`; without it the server ranks lexically and reports
`retrieval.mode` `lexical`.

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

Every location is looked for under every agent, whatever its harness. A
harness directory the server may not read (Claude Code keeps `config/sessions`
owner-only under a seat, the path where Codex keeps its rollouts) holds nothing
it can index and does not stop the agent's other history from being indexed;
a private directory that does hold history, such as a seat's owner-only
Claude Code `projects/`, is not searchable until the server may read it.
AnyAgents' own `transcripts/` stays strict: an unreadable file there fails the
refresh.

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
after the published `time_updated` mark, a bounded batch at a time, each from
one snapshot of the store. A replaced database, a lower frontier or a changed
frontier row rebuilds the store; deleted rows remain published until hourly
full verification and are refused at readback by their digest. A store with an unknown layout is not
read. The walk stamps the store and its `-wal`, so new rows in the log are
seen before a checkpoint touches the database file.

## Learned memory

Learned memory is the topic files under `memory/` and the `MEMORY.md`
catalog AnyAgents rebuilds deterministically from them on every write (the
`memory` tool's `consolidate` action rebuilds it on demand). An agent writes
topics with the `memory` tool. While an agent is idle, the server's sleep
consolidation pass reviews its recent mailbox with a model, may add, update or
merge topics, and rebuilds `MEMORY.md`; its watermarks are in
`memory/.sleep-state.json`, a dotfile that is never indexed. Hybrid retrieval
leaves that pass as it is. It is to be rebuilt inside the agent's own harness
and session; until that exists, the server-side pass stays.

The pass destroys nothing. It only reads raw history. Before it replaces a
topic, the topic's current bytes are copied to
`history/archive/memory/<topic>.<ms>.md`; a topic it merges away moves there
instead of being deleted. The archive is searchable history (`scope:
transcripts`), never pinned into a prompt. Every topic it writes ends with a
`## Sources (raw history)` section citing the mailbox turns it was distilled
from, one line each: `raw_id` (the message id), `raw_sha256` (SHA-256 of the
body's exact bytes), byte `offset` and `length`, mailbox `seq` and
`direction`. Earlier citations are kept across rewrites (the newest 64). The
model names its source turns; a plan that names none is cited to the whole
window it read. An agent's own `memory` tool writes and deletes are not this
pass, and do what they say.


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
The mailbox search index (`rag_chunks`, `rag_chunks_fts`) is not in
`state.db`: it lives in `memory-index.db` beside it (`<file>.memory-index.db`
for a state file with another name), a projection of `messages` and
`outbound_messages` written in its own transactions, so projecting, rebuilding,
renaming or redacting index rows never holds the state writer. New rows are
projected after their commit (a writer that finds the index busy leaves them
to the next search) and every search first projects what is missing. A change
to a projected row records a pending entry in the index before the state
write and re-projects from what state committed after it, so an interruption is
repaired by the next sync. A missing, damaged or older-layout file is
recreated and refilled from state, more than one search's share in the
background. Standalone tool summaries have no mailbox row and are kept in the
index as recorded, across a rebuild of the layout. Opening a pre-split
database copies those summaries into the index (deciding which chunks have no
mailbox row from the chunk-id key alone) and renames state's search tables to
`retired_rag_chunks` and `retired_rag_chunks_fts` (migration 34): nothing
before the server answers reads or frees their pages, which on a large store
would outlast any readiness window. After startup they are dismantled in short
transactions, each holding the state writer at most half the time: each
outbound turn's mailbox position moves onto its row (until then the projection
reads it from the retired chunk), the rows are deleted in batches, and the
emptied tables are dropped. A process that stops part way is resumed by the
next open. Until they are gone, conversation redaction refuses, since they
still hold copies of mailbox bodies it does not scrub. The space they held is
reused by SQLite and returned to the file system only by a `VACUUM` of
`state.db`, run while no process has it open.
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
dense index (`<data>/memory-service/dense-<space>.db`, rebuildable, never
`state.db`), and answers on an owner-only socket
(`<data>/memory-service.sock`, mode `0600`) with one newline-delimited JSON
request and reply per connection:

| Request | Reply |
| --- | --- |
| `search {query, owners, source?, since_ms?, until_ms?, limit}` | dense candidates: document id, owner, cited raw range (absolute offset, length, SHA-256), score |
| `rerank {query, passages}` | one cross-encoder score per passage |
| `status` | model pair, embedded and pending documents, vectors, dense space, rebuild estimate and state, US$ spent |

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
`retrieval.mode` and `retrieval.stages` (`lexical`, `dense`, `rerank`). The
mode is `lexical` when no service runs on the data directory (nothing
configured, the default), and nothing is reported degraded; it is `hybrid`
when one does, and a stage the service could not run is then listed in
`retrieval.degraded` with the reason. Search waits at most 3 s for
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
stopped. Marks are named by the host index's generation (a new one each time
its documents table is created and rowids start over) and by the worker count:
a restart with another `--index-threads`, or a rebuilt host index, starts new
marks and walks the catalog again, paying only for documents not already
embedded at the same digest. A worker whose step fails (a model, the network,
a database) keeps running and retries after 30 s, doubling to 10 minutes;
`status.errors` and `status.indexing` name the error until a step succeeds,
and a rebuild reports `retrying: …`. When a model request fails partway
through a batch, the documents already embedded are stored, so their vectors
are not bought twice, and their spend counts either way. It runs `--index-threads` workers (default 1) at nice 19 (`--nice`),
each a single-threaded session over its own stripe of documents with its own
marks, so the CPU it may take is exactly that many cores and only when no
normal-priority work wants them. Query embedding and reranking run on their
own threads at normal priority (`--query-threads`, default 2). An idle embedder checks for new documents every
30 s. Every 10 minutes it drops vectors of documents the host index no longer
holds at the same digest, so removed, redacted or rewritten history leaves
the dense index within that interval; until then the server's resolution
step refuses such candidates.

### Embedding and rerank providers

Which embedder and reranker run is configuration:
`<data>/memory-service/config.json` (or `--config <file>`). With nothing
configured the service runs the pinned local pair below, offline: no network
call and no spend, the safe default for an open-source install. A remote model
is bound to one AnyAgents profile, `<provider>/<profile>` under
`<data>/profiles`, whose API key the service reads where the profile pool keeps
it, and to a model id:

```json
{
  "embedder": {"provider": "openai", "profile": "openrouter/default",
               "model": "qwen/qwen3-embedding-8b", "dimensions": 1024,
               "usd_per_million_tokens": 0.01},
  "reranker": {"provider": "rerank", "profile": "openrouter/default",
               "model": "mongodb/rerank-3"},
  "max_rebuild_usd": 10
}
```

The configuration a deployment runs is in
[Desk configuration](#desk-configuration-2026-11-14).

| Provider | Request |
| --- | --- |
| `local` (default) | a pinned ONNX model (`model` names a pin; default the pinned pair) |
| `openai` | `POST {base_url}/embeddings` with OpenAI's body (`model`, `input`, `encoding_format`, optional `dimensions`) |
| `rerank` | `POST {base_url}/rerank` with `model`, `query`, `documents`, `top_n`; scores from `results[]` or `data[]` (`index`, `relevance_score`): the Cohere, Voyage, Jina and OpenRouter shape |

`base_url` defaults to the provider's documented root for `openrouter`,
`google-gemini` (Google's OpenAI-compatible endpoint), `sylphx`, `openai`,
`voyage`, `cohere` and `jina`. `query_prefix`, `passage_prefix`, `body`,
`query_body` and `passage_body` add a model's instruction text or request
fields (for example `input_type`, or OpenRouter's `provider.sort`). Every
request to OpenRouter (by profile, or by a `base_url` on `openrouter.ai`)
carries `provider.data_collection = "deny"`, merged into whatever routing the
configuration sets: OpenRouter then routes only to upstream providers that
neither keep nor train on the text. That is code, not configuration, and
covers embedding, the width probe and rerank. A remote reranker reads the
first `passage_chars` characters of each passage (1,000 unless configured,
what the desk's pair was measured with), which also bounds what one search
costs. Remote calls retry throttling and upstream failures, and record the
cost the provider reports (`usage.cost`) or, failing that, the configured
price.

Every dense index is stamped with its space: provider, model, dimension and
preprocessing (projection, piece budget, token counter and passage prefix).
Each space has its own file, so vectors of two models never mix. When the
configured embedder names a new space, the service builds it in the
background like any backlog (nice 19, resumable, a bounded batch at a time;
remote requests are paced). Before it starts it estimates what the build
reads, from a uniform sample of 256 documents cut exactly as the embedder will
cut them, and what it costs at the model's price; the estimate is logged,
written beside the index (`dense-<space>.estimate.json`) and reported in
`status.rebuild`. A remote build starts only when its price is known and the
estimate is within `max_rebuild_usd` (US$10 by default); above it, or with no
price, `status.rebuild.state` says why it waits and nothing is embedded. While
the space is building, everything it embeds counts toward the rebuild, history
published meanwhile included, and all embedding stops (`stopped: …`) when the
spend the provider reports reaches the cap. Once every worker has embedded its
backlog the space is recorded as built: from then on new history is embedded
as it comes, and a restart with another `--index-threads` is not a rebuild.
Other spaces' files stay until the operator runs `anyagents memory-service
prune`, which removes them only once the configured space is built; nothing
removes a space on its own, so a wrong `--config` or a local override never
costs a paid index. A `--config` file must exist; only the default path may be
absent, and then the service says it runs the pinned local pair.
`--embedder`/`--reranker` replace the configured models and say so. Changing
the reranker, or only `query_prefix`, rebuilds nothing.

Spend outside a rebuild (query embeddings, rerank, and embedding new history
once the space is built) is capped per UTC day by `max_daily_usd` (US$1 by
default) and kept in `<data>/memory-service/spend.db`, so a restart does not
reset it. Once the day's spend reaches the cap, the service makes no remote
request until the next UTC day: it refuses query embedding and rerank with the
reason, so search answers from BM25 and lists `dense:` and `rerank:` in
`retrieval.degraded`, and new history waits. `status` reports `today_usd` and
`max_daily_usd`.

A cap holds only if every request is counted. A remote model therefore needs a
configured price (`usd_per_million_tokens`, or `usd_per_search` for a
reranker), used whenever an answer reports no `usage.cost`; `serve` refuses to
start with a remote model that has none. A remote query is cut to the same
piece budget as a passage, and a remote rerank query to `passage_chars`, so a
long query never sends or costs more than a passage.

`anyagents memory-service estimate` makes the same estimate for the configured
embedder over the host index as it stands, without calling a model, reading a
profile's key or writing anything, prints it with the state `serve` would
start in, and exits non-zero when `serve` would wait.

Models and runtime are pinned in `agent-memory` (`SylphxAI/agent-kit`, `crates/agent-memory/src/pins.rs`) by
Hugging Face commit and per-file SHA-256, and ONNX Runtime 1.31.0 by the
official release archive's SHA-256. `anyagents memory-service fetch`
downloads and verifies them into `<data>/memory-service/models`; `serve`
refuses to start unless every file matches its pin. ONNX Runtime is loaded at
run time (`ort` with `load-dynamic`); the server binary links no model
runtime.

### Desk configuration (2026-11-14)

The desk runs `qwen/qwen3-embedding-8b` and `mongodb/rerank-3` through the
`openrouter/default` profile, chosen on its own history
([comparison](#openrouter-models-compared-2026-11-14)).
`<data>/memory-service/config.json`:

```json
{
  "embedder": {
    "provider": "openai",
    "profile": "openrouter/default",
    "model": "qwen/qwen3-embedding-8b",
    "dimensions": 1024,
    "usd_per_million_tokens": 0.01,
    "query_prefix": "Instruct: Given a search query, retrieve relevant passages from earlier conversation history\nQuery: "
  },
  "reranker": {
    "provider": "rerank",
    "profile": "openrouter/default",
    "model": "mongodb/rerank-3",
    "usd_per_million_tokens": 0.05,
    "passage_chars": 1000
  },
  "max_rebuild_usd": 10,
  "max_daily_usd": 3
}
```

- The API key is the profile's (`refs.OPENROUTER_API_KEY` in its
  `.dsh/.credentials.yaml`); the file holds no secret.
  `provider.data_collection = "deny"` is sent on every request without being
  configured.
- `dimensions`: 1024 of the model's 4096 (Matryoshka), as measured.
- `usd_per_million_tokens`: OpenRouter's list price for the embedder, used for
  the estimate. The measured build was billed US$0.0111 per million tokens on
  average, because some requests route to a dearer upstream; the cap is held
  against the spend OpenRouter reports, not the estimate. The reranker's cost
  is reported per request (US$0.05 per million tokens in the measurement); its
  price here only covers an answer that reports none.
- `query_prefix`: Qwen3-Embedding's instruction for queries; passages carry
  none. It shapes queries only and is not part of the vector space.
- `passage_chars`: as measured; it also bounds one search's rerank to about
  20,000 characters.
- `max_rebuild_usd`: the owner's cap on one rebuild. `max_daily_usd`: room for
  a few thousand reranked searches (each at most about US$0.001) and a day of
  new history (tens of millions of tokens at US$0.01 per million).

The first start with this file builds the qwen3 space over everything the host
index holds. A review on 2026-11-14 sampled the live host index at about 1.5
to 2 billion tokens once every agent's native history is indexed: about US$15
to 22 at list price, above the US$10 cap. With this file `serve` therefore
waits (`waiting: estimated US$… exceeds max_rebuild_usd US$10.00`) and
embeds nothing until the operator, having run `estimate`, raises
`max_rebuild_usd`. At the measured 5,300 tokens a second per worker, four
workers (`--index-threads 4`) take roughly 20 to 26 hours, and the dense index
takes about 1.8 KB per 1024-dimension piece, roughly 10 to 13 GB. Each search
then costs one query embedding and one rerank of at most 20 passages, at most
about US$0.001.

To build it:

1. Deploy the server, and wait for its log to say
   `anyagents-host-history: pre-warm done: N agents' history indexed`. The
   rebuild's cap covers what it embeds while the space builds, history the
   pre-warm publishes meanwhile included; history published after the space
   is built is embedded as new history, outside the cap.
2. Write the configuration above.
3. `anyagents --data <data> memory-service estimate`: the documents, tokens
   and US$ the rebuild will read and spend, and the state `serve` would start
   in; it exits non-zero while `serve` would wait. Run it more than once (each
   reads another random sample), and raise `max_rebuild_usd` only after
   deciding on the figure.
4. `anyagents --data <data> memory-service serve --index-threads 4`, as the
   same user and groups as `anyagents serve`, so it can read every source the
   server indexed.
5. `anyagents --data <data> memory-service status` shows `rebuild.state`
   (`running`, then `done`), `rebuild.spent_usd` and the pending documents.
   A `conversation` search then reports `retrieval.stages`
   `["lexical", "dense", "rerank"]`.
6. After a change of embedding model, once `status` says `done`,
   `anyagents --data <data> memory-service prune` removes the old space's
   index. A first build has nothing to remove.

Stopping the service returns search to BM25 and says so in
`retrieval.degraded`; the dense index stays for a later start. Without the
file the service runs the pinned local pair, which is another space and builds
its own index on local CPU at no cost.

Rolling the server back past this change: an older server cannot read the
host index this one writes (Spiron journal rows are documents addressed as
`<database>/<table>/<rowid>`, which an older server fails to read back), so its
searches over Spiron history can fail. Stop it and delete
`<data>/host-history-index-v1.db` with its `-wal` and `-shm`; the index is a
rebuildable projection. It ignores the dense indexes and `spend.db`.

The older server rebuilds the index lazily: the first search that reaches an
agent's history indexes all of it inside that agent's lifecycle lock, so mail
to that agent waits until it is done. On the desk on 2026-11-14 that was about
39 agents with indexed history, about 6 GB raw, and the larger agents take
several minutes each. Either accept that wait, or warm the index before taking
traffic: start the older server with those agents paused, run one search per
agent (`anyagents --data <data> search <word> --agent <name> --scope
transcripts`), then resume them.

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
`cargo run -p agent-memory --features service --release --example
memory-eval` for development.

Results on 2026-11-13, from two lead agents whose operator talks to them in
Telegram, mostly in Cantonese. Recall@k counts a query as answered when the
quoted message is in the first k; MRR@10 and nDCG@10 use the same single
relevant message. The reranker is `hs-rerank-1` unless stated.

5-day window: 6,941 messages, 170 queries (154 Cantonese, 5 Chinese, 11 English).

| Ranking | 311M-r2: R@10 | MRR@10 | nDCG@10 | 97M-r2: R@10 | MRR@10 | nDCG@10 |
| --- | --- | --- | --- | --- | --- | --- |
| BM25 | 0.253 | 0.131 | 0.160 | 0.253 | 0.131 | 0.160 |
| Dense | 0.294 | 0.134 | 0.172 | 0.235 | 0.106 | 0.136 |
| Hybrid (RRF) | 0.324 | 0.149 | 0.190 | 0.288 | 0.138 | 0.173 |
| BM25 + rerank | 0.265 | 0.147 | 0.174 | 0.265 | 0.147 | 0.174 |
| Hybrid + rerank | 0.347 | 0.176 | 0.216 | 0.335 | 0.169 | 0.208 |
| Hybrid + rerank (`multilingual-reranker-small`) | **0.365** | **0.203** | **0.241** | | | |

With `multilingual-reranker-small` the reranked hybrid also puts the quoted
message first for 14.7% of queries, against 10.6% with `hs-rerank-1` and 8.2%
for BM25.

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
`Horizon-Labs/multilingual-reranker-small` (int8, Apache-2.0, mmBERT-small
distilled from a 4B reranker, 384 tokens per pair): on the 5-day window it
lifts the reranked hybrid's MRR@10 from 0.176 (`hs-rerank-1`) to 0.203.
Reranking 20 passages took about 2.8 s on four threads of the loaded host
against 1.3 s for `hs-rerank-1`, inside the server's 8 s bound; `hs-rerank-1`
(MIT) stays pinned as the faster choice. The larger `bge-reranker-v2-m3`
(int8) took about 7 s per query on four threads, too close to the bound, and
on the first 100 queries lifted the hybrid list's MRR@10 by 0.034, less than
the default's 0.054 over all 170. Every candidate in `pins.rs` can be selected with
`--embedder` / `--reranker` and evaluated with the same command.

### Providers compared (2026-11-13)

Which remote models our profiles actually offer, read from the profile pool,
the providers' model listings and their documentation:

| Profile | Embeddings | Rerank |
| --- | --- | --- |
| `openrouter/default` | 39 models (`/api/v1/embeddings/models`), among them `google/gemini-embedding-2`, `qwen/qwen3-embedding-8b`, `mongodb/voyage-4-large`, `cohere/embed-5-pro`, `openai/text-embedding-3-large` | 13 models (`/api/v1/models?output_modalities=rerank`): Cohere rerank 5/4/3.5, Voyage rerank-3/2.5, Qwen3-Reranker 0.6B/8B, Nemotron |
| `google-gemini/api` | `gemini-embedding-001`, `gemini-embedding-2`, `gemini-embedding-2-preview` (ListModels, `embedContent`) | none |
| `sylphx/*` | none routable for our key (the authenticated catalog lists no embedding row; `/v1/embeddings` answers `model_not_available`) | `/v1/rerank` is retired |
| coding plans (`qwen-token-plan`, `kimi-`, `glm-`, `minimax-code-plan`) | not usable: their keys are for interactive coding tools only | none |
| subscriptions (`anthropic`, `openai`, `google-antigravity`, `xai`, `cursor`, `muse`) and `deepseek/api` | none | none |

The OpenRouter account had US$0.01 of credit left (US$3,026.12 bought, all
used), so its candidates could not be run: the first request past a pilot was
refused with `402 in_flight_budget_exhausted`. The comparison below therefore
covers the Gemini API profile's two embedders and the local pair; the
OpenRouter candidates were run the next day
([below](#openrouter-models-compared-2026-11-14)).

A second sample of the same two lead agents (2026-11-08 14:33 to 2026-11-13
22:08 UTC: 7,538 messages, 188 queries) was built read-only into a private
directory and deleted after the runs. Remote embedders ran at 1024 dimensions
through Google's OpenAI-compatible endpoint; `gemini-embedding-2` takes its
task in the text (queries `task: search result | query: `, documents
`title: none | text: `), and without it scored far lower (dense MRR@10 0.080,
reranked hybrid 0.192). The reranker is the local
`multilingual-reranker-small` throughout.

| Ranking | R@1 | R@10 | MRR@10 | nDCG@10 |
| --- | --- | --- | --- | --- |
| BM25 | 0.074 | 0.245 | 0.121 | 0.151 |
| BM25 + rerank (no embeddings) | 0.117 | 0.293 | 0.170 | 0.200 |
| Dense, local `granite-embedding-311m` | 0.064 | 0.245 | 0.115 | 0.146 |
| Dense, `gemini-embedding-2` | 0.080 | 0.303 | 0.139 | 0.178 |
| Dense, `gemini-embedding-001` | 0.080 | 0.330 | 0.148 | 0.191 |
| Hybrid, local | 0.074 | 0.314 | 0.143 | 0.184 |
| Hybrid + rerank, local | 0.133 | 0.362 | 0.192 | 0.232 |
| Hybrid + rerank, `gemini-embedding-2` | 0.144 | 0.388 | 0.207 | 0.250 |
| Hybrid + rerank, `gemini-embedding-001` | **0.160** | **0.420** | **0.219** | **0.265** |

Reranking BM25 alone does not reach hybrid retrieval: it can only reorder the
lexical top 20, which holds the answer for 34% of queries, against 44% to 50%
for the fused lists, so embeddings stay. A cloud embedder adds about 0.06 to
recall@10 and 0.03 to MRR@10 over the local one; with 188 queries the MRR
difference is at the edge of noise, the recall difference is not. Embedding
the sample took about 2 minutes per remote model against 8 minutes on eight
local threads. Cost of the runs: about US$1.70 on the Gemini API profile at
list price (US$0.20 per million tokens for `gemini-embedding-2`, US$0.15 for
`gemini-embedding-001`, about 3.1 million tokens per build; the endpoint
reports no usage, so this is the estimate the run recorded), and US$0.006 on
OpenRouter, from its key usage before and after.

### OpenRouter models compared (2026-11-14)

With credit on the OpenRouter account, the same 188-query sample was run
through six remote embedders and two remote rerankers, all requests with
`provider.data_collection = "deny"` and remote passages cut to 1,000
characters. Each hybrid list is BM25 fused with that embedder's dense list.

| Ranking | R@1 | R@10 | MRR@10 | nDCG@10 |
| --- | --- | --- | --- | --- |
| BM25 | 0.074 | 0.245 | 0.121 | 0.151 |
| BM25 + `rerank-3` (no embeddings) | 0.181 | 0.298 | 0.215 | 0.234 |
| Hybrid + rerank, local pair (today's default) | 0.133 | 0.362 | 0.192 | 0.232 |
| Hybrid local + `rerank-3` | 0.202 | 0.399 | 0.260 | 0.293 |
| Dense, `qwen3-embedding-8b` | 0.128 | 0.383 | 0.199 | 0.243 |
| Dense, `voyage-4-large` | 0.122 | 0.388 | 0.203 | 0.247 |
| Dense, `embed-5-fast` | 0.160 | 0.404 | 0.223 | 0.265 |
| Hybrid `voyage-4-large` + `rerank-3` | 0.213 | 0.447 | 0.278 | 0.318 |
| Hybrid `embed-5-fast` + `rerank-3` | 0.223 | 0.431 | 0.284 | 0.318 |
| Hybrid `qwen3-embedding-8b` + `rerank-5-pro` | **0.245** | 0.426 | 0.291 | 0.323 |
| Hybrid `gemini-embedding-001` + `rerank-3` | 0.234 | 0.452 | **0.296** | 0.333 |
| Hybrid `qwen3-embedding-8b` + `rerank-3` | 0.229 | **0.463** | **0.296** | **0.335** |

`qwen3-embedding-8b` with `rerank-3` ties for the best MRR@10 and has the best
recall@10 and nDCG@10, against 0.192 MRR@10 for the local pair. It ties with
`gemini-embedding-001`, which costs fifteen times as much per token (US$0.15
against US$0.01 per million), so the desk runs qwen3. `rerank-3` beat
`rerank-5-pro` on MRR@10 with every embedder, local included, and answered
faster (0.44 s against 0.62 s per query, mean). Reranking BM25 alone with
`rerank-3` already passes the local pair's MRR@10, but it can only reorder the
lexical head: embeddings lift recall@10 from 0.298 to 0.463. The comparison
run reported US$0.49 of spend, most of it rerank.

## Incremental indexing and migration

An unchanged inode/device, nonshrinking source and matching **64 KiB** frontier
window allow suffix-only indexing. Replacement, shrink or boundary mismatch
rebuilds the source; the rebuild's first batch drops its old documents. A
same-size rewrite outside the frontier window can remain stale until full
verification reaches it; there is no immediate full-history integrity guarantee
on the fast path.

Indexing never holds a lock across a long read. A source is read in batches of
about **2 MiB** (or 512 store rows): each batch is scanned, hashed and
projected to search tokens with no lifecycle lock and no index transaction
held. Only publishing a batch takes the owner's lifecycle lock and one short
index write, and it publishes only if the source still holds what was read
(the same file, unchanged or grown by appends with the batch's 64 KiB boundary
unchanged; for a store, the same database with the batch's last row unchanged)
and no redaction of the owner happened since the read. A batch that fails a
check is dropped and read again later. Each batch's documents, call
correlations and cursor publish atomically; a cursor records whether its
source is complete.

A search indexes at most **4 MiB** of new history before it answers. A class
with more is queued for the background indexer, and the response carries
`retrieval.pending` (sources and bytes) and a `partial:` line in
`retrieval.degraded`: hits from that history may be missing until it is
indexed. The background indexer is one daemon thread at nice 19 and idle I/O
priority. It works through its queue in order: classes searches left
incomplete (at most **64 MiB** per class per step, then queued again behind
the rest) and sources due for full verification. With the queue empty it runs,
once per daemon start, a pre-warm pass over every agent (live agents first), so
history a new release covers is indexed gradually without waiting for a search. The maintenance
tick's verifier (hourly per source) never reads a source or waits for a lock:
it records the attempt and queues the source. Verification reads the whole
source in batches, republishes only documents whose bytes changed, and moves
the frontier and verification time only once it has read everything; a source
that fails to read keeps both.

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
