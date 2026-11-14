# 0011: `spiron.headless/v2`, the reference protocol AnyAgents drives agents with

Status: proposed. Date: 2026-11-14. Implements [0010](0010-execution-identity-and-trust.md) D6 for local Spiron and gives D7's remote-session contract its first wire protocol. Keepers ([0007](0007-process-architecture.md)) stay for third-party CLIs. Every frame's shape is fixed by the JSON Schema [`docs/schemas/spiron-headless-v2.schema.json`](../schemas/spiron-headless-v2.schema.json); this record says what the frames mean.

## Context

`spiron.headless/v1` is specified only in prose, in Spiron's `docs/spiron-cli/README.md`. Checked against Spiron `main` at `3716ec28f` and AnyAgents `main` at `8f8f8d4`:

- **What v1 already does well, and v2 keeps.** Capability negotiation in `ready`; one journal key per `request_id`, so a redelivered input is never shown to the model twice; consumption proof (`accepted` when a turn's opening input is journaled, `steer_consumed` when a steer is, `steer_ack` as admission only, `rejected` when nothing was consumed); `undelivered_steer_ids` for steering a turn ended without; `null`, never zero, for usage the provider did not report.
- **It runs over stdio only.** The process is the child of whoever holds its pipes. AnyAgents therefore runs each Spiron inside a keeper (0007) so a server restart does not end its turn. Spiron was 34 of the desk's 51 harness processes when 0007 was measured.
- **It has no sequence numbers and no attach.** A client that loses the connection loses every frame written while it was gone. The keeper exists to hold the adapter's memory of those frames.
- **It has aliases.** `user` for `prompt`, `interrupt` for `cancel`, `id` for `request_id`, and request ids of any JSON type.
- **It reports a context window nobody reads.** `result.context` is per turn; AnyAgents never reads it, and nothing tells a host the window or policy before the first turn.
- **It reports no cost.** AnyAgents records Spiron usage in tokens only (0009, audit).
- **Its only conformance test is a Python fake** in AnyAgents (`anyagents-harness-stream/tests/fake_spiron.py`), which can agree with the adapter and still disagree with Spiron.

## Decision

### D1. One protocol, three transports

| Transport | Framing | Who speaks first | Versions |
| --- | --- | --- | --- |
| **stdio** (`spiron --headless --json`) | one JSON object per line (JSONL), UTF-8 | the server: a v1 `ready` greeting | v1 by default; v2 after `attach` |
| **Unix socket** (`spiron --headless --json --listen <path>`) | one JSON object per line | the client: `attach` | v2 only |
| **wss** (Spiron cloud) | one JSON object per WebSocket text message, no newline | the client: `attach` | v2 only |

The frames are the same on every transport. Only framing, authentication and who speaks first differ.

- **stdio.** The process ends at end of input after draining accepted work, as in v1. No other client can follow, so end of input answers every pending host tool call with an error (`host_gone`).
- **Unix socket.** Spiron binds `<path>`, mode `0660`, and does not read stdin. It replaces only a socket no process serves; any other file at the path, or a socket another process serves, fails the start. Access control is the socket's directory and mode: the host picks a directory that only it and the agent's uid can enter (on the desk, the agent's config directory, `kyle:<seat group>`, `2770`). The process outlives every connection. It serves one attached client at a time: a new `attach` takes the session over, and the server closes the connection it replaces, as `tmux attach -d` does. While no client is attached, the turn goes on, every frame is journaled (D3), and a `host_tool_call` (D7) waits. The process ends on `stop` (D5) or a signal.
- **wss** (specified here, built later for Spiron cloud). The client opens the session's `attach_url` with `Authorization: Bearer <token>` and the WebSocket subprotocol `spiron.headless.v2`. The token is the short-lived, per-session, revocable token of 0010 D2; an expired or revoked token closes the socket with code `4401`. A second connection supersedes the first, which is closed with `4409`. Ping and pong are WebSocket control frames; the protocol adds no heartbeat. `create` is Spiron cloud's HTTPS call, not a frame: `POST /v1/headless-sessions` with `{model, instructions, host_tools, workdir}` (the same inputs as the CLI's flags) answers `{session_id, attach_url, token, expires_at}`. A token is renewed with `POST /v1/headless-sessions/{session_id}:token`.

**Rationale.** Framing is the only thing a transport should own. Over a socket the client speaks first, so it can say from where to replay before anything is sent. stdio keeps its server greeting because every v1 client waits for it.

**Precedent.** LSP runs one JSON-RPC protocol over stdio, pipes and sockets. The Docker API is the same over its Unix socket and TCP. WebSocket subprotocols (RFC 6455 §1.9) name the protocol a connection speaks.

### D2. Version negotiation: v1 and v2 side by side

- On stdio the first frame is a v1 `ready` that adds `protocols: ["spiron.headless/v1", "spiron.headless/v2"]`. A v1 client ignores the field and speaks v1, unchanged.
- A v2 client answers the greeting with `attach {protocol: "spiron.headless/v2", after_seq}`. From then on that connection is v2: the server answers with the replay and a v2 `ready` (D4), and every later frame is v2. While a v1 turn is open or v1 prompts wait, the switch is refused `turn_open`, so v1 identities never reach v2 frames.
- On a socket or wss the first frame must be `attach`. Anything else is answered `rejected {code: "not_attached"}` and the connection is closed. An `attach` naming a protocol the server does not speak is answered `rejected {code: "unsupported_protocol"}` and closed.
- A v1-only Spiron does not know `--listen` and exits with a usage error (status 2) before it starts a session. A host that launches with `--listen` and sees that falls back to v1 over stdio.
- A new optional feature is a field added with a default; a client ignores fields it does not know. Changing what a frame means, or what a client must send, is a new version.

**Rollout.** Spiron ships v2 first; old AnyAgents keeps speaking v1 to it over stdio. AnyAgents then launches every Spiron with `--listen` and keeps v1 for binaries that refuse it. v1 is removed from AnyAgents when no supported Spiron lacks v2, and from Spiron one release after no supported AnyAgents speaks v1.

**Precedent.** HTTP `Upgrade` and STARTTLS: the server greets in the old protocol and the client asks to switch. LSP's `initialize` exchanges capabilities and each side uses only what the other declared.

### D3. Sequence numbers and the frame journal

- Every frame on a v2 connection carries `seq`: an integer that starts at 1 for a session and grows by exactly one per frame. Numbering belongs to the session, not to a process or connection, so it continues across reconnects and across Spiron processes.
- A frame is written to Spiron's session database before it is sent. A frame a client was sent is therefore always in the journal; a client that sees seq `n` saw every frame before `n` (in its replay or live).
- Every v2 frame is journaled, `ready` included. The stdio v1 greeting and frames written before a session exists (startup `error`) are not, and carry no `seq`.
- Frames on a v1 connection carry no `seq` and are not journaled, so a replay carries v2 frames only.
- **Retention.** A process keeps every frame it journaled. When a new process opens the session (after it owns its socket, so a process that cannot start changes nothing), it discards the frames of earlier processes except the newest one, so numbering never restarts. It does so only while it holds the session's headless lock, which a live process holds for its whole life: a second process on a session another one still serves discards nothing. A cursor past every seq replays nothing. A session's journal therefore holds about one process's lifetime of frames.
- The frame journal is not the session journal. The session journal is what the model saw and keeps it in full; the frame journal is what the host was told.

**Rationale.** A host that reattaches needs the exact frames it missed, in order, once each. A number assigned by the writer, before the frame leaves, gives that without a second source of truth. Keeping one process's frames is enough for a client to reattach to a running process (the case D6 exists for); a process that died is replaced, and the mailbox redelivers past its cursor, deduplicated by request id (D6).

**Precedent.** Discord's gateway numbers each dispatch (`s`) and replays from the last one on resume. Server-Sent Events resume from `Last-Event-ID`. Kafka offsets: a consumer rebuilds its state by reading from an offset.

### D4. `attach` and `ready`

`attach {protocol, after_seq}` starts a v2 connection. `after_seq` is the last seq the client holds, `0` for everything the journal holds, or `null` for nothing (start live).

The server answers, in order:

1. every journaled frame with `seq > after_seq`, as journaled (the replay);
2. a new `ready`, which is the first frame of this connection that is not replay;
3. live frames.

`ready` carries:

| Field | Meaning |
| --- | --- |
| `protocol`, `protocols` | the version this connection speaks, and every version the binary speaks |
| `version`, `commit` | the Spiron build (`commit` is `null` for a build without one) |
| `model` | the model id requests are sent with |
| `context` | the effective context policy: `window_tokens` and `window_source` (`catalogue`, `config` or `fallback`), `reported_window_tokens` (what the model is told), `output_reserve_tokens`, `request_limit_bytes` (the largest request sent), `compaction_threshold_bytes` |
| `host_tools` | names of the host tools Spiron accepted (D7) |
| `replay` | `{after_seq, complete}`: `complete` is false when the journal no longer held every frame after `after_seq` |

`ready` is journaled like any frame, so it marks where each connection began. It carries the `request_id` of the `attach` it answers, which tells it apart from the `ready` frames of earlier connections in the replay.

**Rationale.** Sending `ready` after the replay keeps `seq` increasing on the wire, and tells the client exactly when it is caught up. Putting the policy and window in `ready` gives a host what it needs before the first turn, instead of a per-turn report it has to collect.

**Precedent.** Discord sends `RESUMED` after it has replayed missed events.

### D5. Inputs

Every input is one frame. There are no aliases.

| Input | Fields | Answers |
| --- | --- | --- |
| `attach` | `protocol`, `after_seq` | replay, then `ready` (D4) |
| `prompt` | `request_id`, `text` | with no turn open: opens one, `accepted`. With a turn open: steered into it, as `steer` |
| `steer` | `request_id`, `text` | `steer_ack`, later `steer_consumed`; `rejected` `no_active_turn` with no turn open, `turn_closed` once the turn takes no more input |
| `cancel` | `request_id` | `cancel_ack`; the turn ends with `result {status: "cancelled"}` |
| `stop` | `request_id` | admission closes; the open turn is cancelled and its `result` written; pending host tool calls are answered with an error; then `stopped`, and the process exits |
| `host_tool_result` | `call_id`, `output`, `is_error` | nothing on success; `rejected {code: "unknown_call"}` for a call that is not pending |

- `request_id` is a non-empty string, required on `prompt`, `steer`, `cancel` and `stop`. Every output frame carries the `request_id` it belongs to: a turn's frames carry the id of the input that opened it.
- An input may carry `session_id`; a different session is `rejected {code: "wrong_session"}`. An `attach` naming another session is refused this way on a new connection, which is closed; the client attached before stays.
- **Idempotency.** `prompt` and `steer` are journaled under `input:<request_id>`. A repeated input is never journaled or shown to the model again: a repeated `prompt` with no turn open answers `accepted {mode: "duplicate"}` and `result {duplicate: true}` without model work; repeated into a turn, it is consumed as `steer_consumed {duplicate: true}`.
- **Consumption proof.** Only `accepted` and `steer_consumed` prove an input is in the session journal. `steer_ack` is admission. A turn that ends before journaling its opening input did not start: that input and every steer admitted to it are `rejected {code: "turn_not_started"}`. `result.undelivered_steer_ids` lists steers acknowledged and never consumed; the host delivers them again.

Rejection codes: `empty_prompt`, `missing_request_id`, `no_active_turn`, `turn_closed`, `turn_not_started`, `wrong_session`, `unknown_command`, `unknown_call`, `not_attached`, `unsupported_protocol`, `already_attached`, `turn_open`, `stopping`.

### D6. Outputs

Each output carries `seq`, `type`, `session_id`, `request_id`, and the host identity Spiron was launched with (`agent_id`, `agent_name`, `profile`, each a string or `null`).

| Output | Fields |
| --- | --- |
| `ready` | D4 |
| `accepted` | `mode`: `turn` or `duplicate` |
| `steer_ack` | `accepted: true`, `turn_id` |
| `steer_consumed` | `steer_request_id`, `turn_id`, `duplicate` |
| `cancel_ack` | `turn_id`, `status: "cancelling"` |
| `rejected` | `code`, optional `error` |
| `text_delta` | `text`: a complete, credential-masked model message, never a raw provider chunk |
| `text`, `tool_call`, `tool_result`, `error` | the session journal's event, as Spiron's `AgentStreamEvent` serializes it |
| `host_tool_call` | `call_id`, `name`, `input` (D7) |
| `stream_gap` | `stream`, `skipped`: a live subscriber lagged; the journal keeps the full receipts |
| `protocol_error` | `code`: `invalid_json`, `invalid_utf8`, `stdin_read_failed` |
| `result` | `status` (`completed`, `cancelled`, `failed`), `is_error`, `error`, `duplicate`, `usage`, `cost_micros`, `context`, `undelivered_steer_ids` |
| `stopped` | the last frame after `stop` |

- `result` is the only terminal frame of a turn and follows its journal commit.
- `usage` is `{input_tokens, output_tokens, cached_tokens, cache_write_tokens}` summed over every request of the turn (compaction included), or `null` when no request reported usage. A field the provider did not report is `null`, not zero.
- `cost_micros` is what the turn's requests cost in millionths of a US dollar, as the gateway reported it (`x-sylphx-cost-usd`), summed; `null` when no request reported a cost.
- `context` is the turn's fitting report (compactions, omitted messages, provider overflows). It is diagnostic; the policy a host needs is in `ready`.

### D7. Host tools over the same connection

- A host declares tools at launch: `--host-tools <file>`, `{"tools": [{"name", "description", "input_schema"}]}`. A tool is accepted when its name matches `^[A-Za-z0-9_-]{1,64}$` and is no built-in or mounted tool's name; `ready.host_tools` lists the accepted names, and the model sees them for the life of the process.
- When the model calls one, Spiron journals the `tool_call` event, then writes `host_tool_call {call_id, name, input}`, where `call_id` is the model's tool call id (`tool_call.data.callId`). It waits for `host_tool_result {call_id, output, is_error}`. The model sees `output` as the tool's result, or `{"error": output}` when `is_error` is true.
- The first result for a call is used. A result for a call that is not pending is `rejected {code: "unknown_call"}`.
- Like every frame, `input` is credential-masked.
- A call waits across disconnects. A reattaching client finds unanswered calls in its replay: a `host_tool_call` with no later `tool_result` for its `call_id`. A host keeps the call ids it has run, so a replayed call is never run twice.
- `cancel` and `stop` answer every pending call with an error (`cancelled`) so the turn can end.

**Rationale.** AnyAgents serves coordination tools (`send`, `inbox`, `agents`, …). Today each Spiron starts an `anyagents mcp` child for them, which calls the server over its socket. A remote agent cannot do that (0010 D7). Calls over the connection the host already holds need no extra process, no extra credential and no `public_url` round trip, and they are journaled and replayed with everything else.

**Precedent.** MCP lets a server send requests to its client (sampling) over the same connection. The OpenAI Responses and Anthropic Messages APIs return client tool calls in the stream and take their results in the next input.

### D8. The remote-session contract (0010 D7) on v2

| D7 operation | v2 |
| --- | --- |
| `create` | local: launch `spiron --headless --json --listen <path> --session <id> …`, then `attach`. Cloud: `POST /v1/headless-sessions`, then `attach` over wss (D1) |
| `deliver` | `prompt` |
| `steer` | `steer`, or `prompt` while a turn is open |
| `events` | the output frames; `attach {after_seq}` resumes them |
| `interrupt` | `cancel` |
| `stop` | `stop` |
| `budget` | host-enforced: the host sums `result.cost_micros` and sends `cancel` or `stop` at its cap. v2 has no budget input |

Capability level, as 0010 D7 asks each adapter to declare: delivery `next-step`, idempotent by `request_id`, consumption proven by `accepted`/`steer_consumed`, events resumable by `seq`, interrupt yes, cost reported per turn.

### D9. How AnyAgents uses v2 locally

- **Launch.** The Spiron adapter launches `spiron --headless --json --listen <config dir>/spiron.sock` in its own process group (as every harness), stdin on `/dev/null` and output in `<config dir>/spiron.log`, connects, and sends `attach {after_seq: null}`. It speaks v2 only when the `ready` answering that `attach` says so. A binary that exits with a usage error naming `--listen` is launched again over stdio with v1, in a keeper, as before.
- **Launch record.** The server writes `<data>/spiron/<agent>.json` the moment the process exists, and completes it after its first `ready`: the socket, the process's identity (pid, start time, boot id, pid namespace), its seat, the session, and `after_seq`, which is the `seq` of the launch's first `ready` minus one. It lives in a directory only the server can write, because it names a process the server may signal.
- **Reattach after a restart.** At startup the supervisor finds each running agent's record, proves the process is the recorded one, takes the agent's lock, connects and sends `attach {after_seq, request_id}` from the record; the adapter is ready when the `ready` carrying that `request_id` arrives, after the whole replay. The replay rebuilds the adapter exactly as the last server had it: the open turn, every proof, and the served usage counted from the launch, so the usage note the supervisor keeps (0007 §4) is compared with the same totals. No keeper runs for Spiron.
- **The mailbox.** The cursor moves only on `accepted` or `steer_consumed`. A replayed proof for a message already past the cursor changes nothing; a message the server re-offers after a restart is answered from the replayed proofs, or written again and deduplicated by Spiron under its `request_id`.
- **Kernel shutdown.** The adapter closes its connection and leaves the process running. `stop` (or a kill of the recorded process group, when it does not end within its grace) is sent only when the agent stops or its harness is replaced.
- **The agent lock.** While a server runs, it holds the agent's lock. While none runs, the lock is free and the Spiron process runs unlocked; the next server takes the lock before it attaches. Every Spiron launch first ends the process a launch record still names, so an agent never has two.
- **v1 stays** for Spiron binaries without v2, for hosts without process identity proof (no `/proc`), and for the in-process host (tests, `agents flush`).

**Rationale.** The process that must survive a server restart is Spiron itself, and with sequence numbers and a journal it can hand a new server everything the old one knew. A record written at launch is enough, because replaying from the launch reproduces the adapter's state; nothing has to be written per frame.

**Deletes.** The keeper for each Spiron agent with a v2 binary (ADR 0010 D6). Later, with the last v1 binary: the v1 path in the Spiron adapter and `fake_spiron.py`.

### D10. Conformance

The conformance suite drives the real Spiron binary, built in Spiron's CI, through this contract over the Unix socket and stdio, and checks every frame against the schema: `ready`, deliver (`prompt` → `accepted`), `steer` → `steer_ack` → `steer_consumed`, a host tool call answered after a reattach, `result` with `usage` and `cost_micros`, the client killed mid-turn, the process still running, `attach {after_seq}` replaying exactly the missed frames, a redelivered `prompt` and `steer` answered as duplicates, no `seq` seen twice and none skipped, and `stop`. A Python fake is not conformance.

## Consequences

- A Spiron agent survives a server restart with no keeper process and no adapter memory outside Spiron.
- AnyAgents records Spiron cost per turn, not only tokens.
- The remote-session executor has one wire protocol for Spiron cloud, with the same frames, replay and proofs as local agents.
- Spiron's session database grows by one process's frames per session.

## Alternatives rejected

- **Keep a keeper per Spiron.** 0010 D6 deletes it: the keeper exists because v1 cannot replay.
- **JSON-RPC 2.0 framing.** v1 is already line frames with request ids; changing the envelope would make every client and server change twice for no new guarantee.
- **An in-memory replay ring in the Spiron process.** It loses the frames with the process and cannot be bounded by what a client has seen; the session database already holds a writer and a lock.
- **Per-process sequence numbers with an epoch.** Two numbers where one suffices; session numbering survives a reconnect to a new process.
- **Persist the adapter's state in AnyAgents on every frame.** A second copy of what Spiron's journal holds, written far more often than a launch record, and free to disagree with it.
- **Host tools through an MCP server the host runs.** That is the extra process, credential and network path D7 removes.
