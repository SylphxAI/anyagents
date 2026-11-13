# Mail delivery

Every input to an agent is a mailbox message with a per-agent sequence
number ([architecture.md](architecture.md#mailbox-one-log-per-agent)). This
document is the contract between the mailbox and a harness adapter: what the
adapter must prove before a message counts as delivered, and what each
harness can prove about a message that reaches its agent while a turn runs.
`tests/invariants` holds every adapter to it.

## The mechanism

One delivery loop per agent (`anyagents-agent`, `DeliveryLoop::run_once`)
offers each message past the delivery cursor to the adapter, in sequence
order, and offers it again on every later pass until it is committed. The
adapter answers with what the vendor has shown about that message:

| Answer | Meaning | The loop |
| --- | --- | --- |
| `Accepted` | The vendor took it, with the proof the harness's level requires. | Commits it, when every earlier message is committed. |
| `Duplicate` | The vendor's session already holds it from an earlier process. | Commits it as delivered; it starts no new work. |
| `InFlight` | Written to the vendor, not yet proved. | Keeps offering the messages behind it, commits none of them yet. |
| `Deferred` | The vendor cannot take it now, or refused it without applying it. | Ends the pass; the message stays in the mailbox. |
| `Quota` | The vendor asked to wait until a reset time. | Ends the pass. |
| `Dead` | The vendor process is gone. | The supervisor starts a new process. |
| `Failed` | A fault no retry can fix. | The supervisor restarts the process. |

The cursor moves only over a contiguous prefix of committed messages, so one
message the vendor has not proved is visible as the oldest undelivered one
and never skipped. Behind an `InFlight` message the loop writes the rest of
the inbox at once, which is how a whole inbox reaches a running turn as one
batch.

## Rules every level keeps

1. **Nothing is lost.** A message is committed only on the evidence its
   level names. A message past the cursor when a vendor process or the server
   dies is offered to the next process. Evidence a process gave before it
   died, or before a quota stopped it, is committed before any replacement
   starts, so the replacement is never handed what its predecessor took.
2. **Nothing is applied twice by AnyAgents.** An adapter writes a message to
   one vendor process at most once. A message an earlier process may have
   taken is written again only when the harness is idempotent (below) or
   when the vendor's own record shows it never arrived.
3. **Mailbox order.** Messages reach the vendor, and its model, in sequence
   order.
4. **A refusal costs no restart.** A vendor that refuses a message without
   applying it gets it again later (`Deferred`). An answer that leaves it
   unknown whether the vendor applied it is settled from the vendor's own
   record, or by an idempotent resend, before anything is written again.
5. **No delivery timeouts.** A delivery request is never re-sent because it
   was slow; a vendor that does not answer is stuck, and its health says so.

## Levels

A level is what a harness guarantees for a message that reaches its agent
while a turn runs. Each level keeps every guarantee of the one before it.
`HarnessHealth::delivery` reports the level of the running process, from its
handshake where the vendor names its version and capabilities; with no
process, the adapter's own.

### `turn-end`

A message for a busy agent is applied when the running turn ends: it starts
the next turn. The running turn never sees it. Committed when the vendor
takes it as the input of a turn.

### `mid-turn`

The vendor takes every message into the running turn as it arrives, in
order, without waiting for the turn to end; when its turn reads it is the
vendor's business. Committed on the vendor's acceptance (a written line, an
answered request, a durable inbox admission), which is the only proof.

If the vendor process dies with accepted messages its turn never read, the
interrupted turn's messages reach the next process inside a `[Recovery]`
note, never as input of their own; a vendor with a durable inbox delivers
them itself.

### `next-step`

The vendor proves, message by message, that the running turn consumed it at
its next step (after the running tool, before the next model request), every
pending message together and in order. The whole inbox is written at once;
each message is committed only on its own proof, over a contiguous prefix. A
message the vendor never consumed (its turn ended first, the vendor refused
it, the process died) is offered again. A proof stays valid after the
process that gave it exits.

### `idempotent`

Orthogonal to the level: a message an earlier process of the same session
consumed is recognised when it is handed over again and committed as a
`Duplicate`, never applied twice, either because the vendor deduplicates by
an id AnyAgents sends or because the adapter reads the vendor's durable
history before it writes. Without it, a crash between the vendor taking a
message and the cursor recording it can hand the message to the next
process again.

## The conformance suite

`tests/invariants/src/conformance.rs` runs the same eight scenarios against
every adapter, through a real `DeliveryLoop` and a scripted vendor process
that speaks the vendor's own protocol (`tests/invariants/fakes/`: the shared
session engine in `engine.py`, one `proto_<vendor>.py` per protocol). The
vendor's journal of what its model saw is the evidence; the cursor is the
commit.

| Scenario | Holds the adapter to |
| --- | --- |
| an idle agent takes a prompt | a turn opens with the message and it is committed |
| five messages reach a busy turn | `next-step`: all five written at once, none committed before the next step, then committed after one step consumed them together, in order; `mid-turn`: all five accepted at once; `turn-end`: the running turn never sees them |
| mail keeps its order across steps | sequence order over several steps |
| a crash after partial consumption loses nothing | the vendor dies after consuming two of five: each message reaches the model once, the unconsumed ones as input of their own (`next-step`, `turn-end`) or inside the recovery note (`mid-turn`) |
| a host restart redelivers without applying twice | `idempotent`: the cursor is lost after the vendor consumed three messages; a new process recognises all three |
| an offer twice reaches the vendor once | a message offered again to the process that took it is not written again |
| a refused message waits without a restart | the vendor refuses a message for its running turn: no restart, applied once later |
| a slow answer neither fails nor duplicates | every vendor answer is 1.5 s late: no failure, no restart, no second write |

A new harness gets the suite by implementing `Subject` (how to start the
adapter against the scripted vendor) and invoking `conformance_suite!` once,
and by teaching the scripted vendor its protocol.

## Harnesses

Each adapter crate exports the level it holds (`DELIVERY`); the server shows
the running process's, with what its handshake named, in `agents list`,
`agents inspect`, `status` and the agents table.

| Harness | Level | Idempotent | How a message reaches it, and the proof |
| --- | --- | --- | --- |
| `spiron` (legacy `sylphx`), `ready` names `steer_consumed` and `idempotent_input` | `next-step` | yes | `spiron --headless --json`. The whole inbox is written at once: a `prompt` when no turn runs, a `steer` into the running one. Spiron journals every pending input together, in order, at its next step; `accepted` (the turn journaled its prompt) and `steer_consumed` prove each, by `request_id`. Refusals (`turn_closed`, `no_active_turn`, `turn_not_started`) and `undelivered_steer_ids` go back to the mailbox. Spiron drops an input its journal already holds under `input:<request_id>` and answers it as a duplicate. Handshake: the `ready` frame's protocol and capabilities. |
| `spiron`, older `ready` | `turn-end` | no | One input per turn, none while a turn runs; `accepted` commits it. Gated on the capabilities, for a Spiron built before them. |
| `claude-code` | `next-step` | yes | `claude -p` with stream-json in and out and `--replay-user-messages`. Each line carries a `uuid` derived from its mailbox id. Claude Code queues a line that reaches a running turn and folds every queued line, in order, into the conversation at the next step; a line still queued when the turn ends starts the next turn. `command_lifecycle` `started`, the replay echo (`isReplay`) and the result's `user_message_uuids` prove each line by its uuid. Claude Code drops a line whose uuid its transcript holds as a user row; at launch on a resumed session the adapter reads the transcript (`$CLAUDE_CONFIG_DIR/projects/*/<session>.jsonl`), including lines folded in mid-turn, which Claude Code's own check misses. Handshake: `system` `init` `claude_code_version`. |
| `codex` | `next-step` | yes | `codex app-server` (JSON-RPC). `turn/start` when no turn runs, else `turn/steer` naming `expectedTurnId`; each carries the mailbox id as `clientUserMessageId`, and is sent without waiting for its answer. The answer only queues it; the `userMessage` item whose `clientId` is that id proves the turn consumed it at its next step. `-32600` refusals go back to the mailbox; any other error, or a message its turn ended without proving, is settled from `thread/items/list` before anything is written again; a resumed thread's newest turns are read first, and a message found there is a duplicate. Handshake: `initialize` `userAgent`. |
| `muse` | `next-step` | yes | `muse serve` (JSON-RPC). Every message is `turn/start` with `ifBusy: "steer"` and a `commandId` derived from its mailbox id; Muse replays a known command's acknowledgement, so a resend is safe. The `userMessage` `item/completed` carrying that `commandId` proves it; `turn/completed` settles the inputs its turn consumed, and a steer the turn never consumed stays in flight until the successor turn Muse names after its command proves it. `-32030` moves to the next command id; admission refusals and unknown outcomes wait under the same one. Handshake: `initialize` `serverInfo`. |
| `omp` | `next-step` | yes | `omp --mode rpc`, with steering mode `all`. A `prompt` with `streamingBehavior: "steer"` is proved by the user `message_end` whose text carries its marker, which OMP emits as the next step takes every queued steer in order; `success: false` hands it back, and a `prompt_result` for a message never proved means OMP dropped it, which also hands it back. At start the session file `get_state` names is read; a message it holds is a duplicate. Handshake: the `ready` frame's protocol version. |
| `grok` | `mid-turn` | no | `grok agent … stdio` (ACP), `follow_up_behavior = "steer"`. `session/prompt` is committed when Grok shows it admitted: its marker in `x.ai/queue/changed` or a `user_message_chunk`, or the answer to its request. A refusal before admission goes back to the mailbox; any other error counts as taken. A prompt is written only after every earlier one is admitted. Grok's consumption notice names no input and Grok deduplicates nothing. Handshake: `initialize` `agentInfo`. |
| `agy` | `mid-turn` | no | `agy --output-format stream-json`; a line on stdin runs a turn, committed once the cascade shows it took it. A message for a running cascade goes through the language server's `SendUserCascadeMessage` (`NEXT_INVOCATION`), one at a time, without blocking delivery; its answer proves the message is queued in the cascade's durable mailbox, which the next step takes in order. A refusal returns the message to the mailbox and it opens the next turn; a send whose answer never arrived is settled from the cascade mailbox and transcript before it is sent again. Nothing in AGY's answers names the message it consumed, and nothing deduplicates by an id we send. Without the language server the adapter reports `turn-end`. Handshake: the language server version in `cli.log`. |
| `opencode` | `mid-turn` | yes | `opencode serve` (HTTP), called through one-shot `opencode api` processes. Each message is `POST /api/session/{id}/prompt` with `delivery: "steer"` and its mailbox id as `id` (prefixed `msg_` when it is not already), one call at a time, in mailbox order, with no timeout. The answer is the session's durable inbox admitting it, which is the proof; OpenCode takes every pending steer into its running turn at the next step, in admission order. OpenCode admits an id once, from any process: an answer naming a record created before the call, which the conversation already holds (`GET …/message/{id}`), is a duplicate. A 400 or 409 goes back to the mailbox; a 5xx or a lost answer is settled by sending the same id once more; a 401 or 403 fails the process. The turn ends when `POST /api/experimental/session/{id}/wait`, asked after the newest admission, answers. OpenCode announces consumption only on its event stream (`session.inbox.delivered`), which the adapter does not read. Handshake: `GET /api/info`. |
| `dsh` | `mid-turn` | yes | The dsh web gateway. Each message is `session/prompt` with `mode: "steer"` and its mailbox id as `requestId`, one call at a time, in mailbox order, with no timeout; `accepted: true` is the session's inbox admitting it. dsh admits a `requestId` once, so an answer that leaves it unknown (`session/agent-busy`, `gateway/internal`, a 5xx, a lost connection) is settled by sending the same request once more. Errors dsh raises before its inbox go back to the mailbox; a 401 buys a new cookie first. Its answer does not say whether the request was new, so a message an earlier process applied is accepted again, never applied twice, rather than reported as a duplicate. Handshake: `dsh -V`. |
| `spiron-cloud`, `claude-cloud`, `openai-cloud`, `cursor-cloud` | `turn-end` | no | `anyagents cloud-harness` speaks Claude Code's stream-json for a cloud agent: one turn per line, the replay echo when the line's turn starts. |
