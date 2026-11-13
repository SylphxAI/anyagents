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
| `Deferred` | The vendor cannot take it now, or refused it without applying it, in a way a later offer can clear. | Ends the pass; the message stays in the mailbox. |
| `Rejected` | The vendor refused it for good, without applying it: no retry or new process can make it take this message. | Parks it past the cursor in `rejected_messages`, with the vendor's reason, tells an agent sender in its own mailbox (`[Undeliverable]`), and shows the reason as the agent's error. The messages behind it flow on. |
| `Quota` | The vendor asked to wait until a reset time. | Ends the pass. |
| `Dead` | The vendor process is gone. | The supervisor starts a new process. |
| `Failed` | A fault no retry can fix. | The supervisor restarts the process. |

The cursor moves only over a contiguous prefix of committed messages, so one
message the vendor has not proved is visible as the oldest undelivered one
and never skipped. Behind an `InFlight` message the loop writes the rest of
the inbox at once, which is how a whole inbox reaches a running turn as one
batch.

An adapter also answers `proven(id)`: what it already proved about a
message, without writing anything, whether or not its process still runs.
The supervisor commits those proofs (`DeliveryLoop::commit_proven`) whenever
it would otherwise skip a delivery pass: while a quota holds, while a harness
switch drains the old process, when the process has died, and before any
replacement. Before it hands mail to a replacement it stops the old process
and reads it to the end (`Harness::stop`), so a proof still in the pipe is
committed, not lost with the adapter that read it. A replacement on the same
harness resumes the same vendor session, so the old process is stopped and
committed before the new one launches: the two never share the session, and
a launch that fails leaves a dead process for the dead-process backoff. A
switch to another harness launches the new process first, and the old one
serves until it is up. An adapter reports its process dead only once its
output has ended, for the same reason, or once a child that exited has kept
its output open (a descendant holding it) for five seconds.

### Rejected messages

A message the vendor refused for good is parked in `rejected_messages` and
never quoted in a recovery note. An adapter says whether the reason belongs
to that message alone (`message_specific`: its size, an id another record
holds, an empty or invalid body); such a message is parked at once and
never counts toward the breaker, so two oversize messages are two parked
messages, not a wedged mailbox. Any other reason might hold for every
message, and a breaker keeps a vendor that refuses everything from parking
the whole mailbox: the same such reason twice in a row, or more than two in
one pass, fails the harness instead, and the message stays in the mailbox. `anyagents requeue <agent>` lists an agent's
parked messages, and `anyagents requeue <agent> <msg_id>` puts one back at
the end of its mailbox, from the same author. A sender notice a crash cut
off is sent when the agent's delivery loop next starts.

### Interrupted turns

When a process exits, or the server restarts, while a turn is open, the next
process is offered one `[Recovery]` note before any mail: it lists the
interrupted turn's messages, whole, and the outbound messages already sent,
and asks the agent to check the actual state before repeating anything that
may have taken effect. Those messages are older than any mail behind them,
so the note goes first, and new mail follows it once it is written. A
message a `mid-turn` vendor accepted but never read reaches the agent this
way.

### Where the adapter runs: a server restart, and a keeper that dies

The adapter runs in a keeper, a process of its own beside the vendor
process, not in the server
([ADR 0007](adr/0007-process-architecture.md)). A server that stops,
crashes or is upgraded ends no vendor process and loses no proof: the next
server takes the keeper over and offers the mail past the cursor again, and
the adapter answers what it already wrote from its own memory (`InFlight`,
`Accepted`, `Duplicate`) and writes nothing twice. No turn was interrupted,
so no `[Recovery]` note is sent beyond one the last server still owed. Every
call the server makes across the keeper is a read (`proven`, `health`) or a
command that is safe to repeat (`deliver` by id, `stop`), so an answer lost
with a dying server loses nothing.

A keeper that dies takes the adapter's memory with it, and the server ends
the vendor process the keeper held before anything replaces it. That is the
crash above, with one difference from a vendor process that dies under a
live adapter: a proof the server had not yet committed is gone with the
adapter (`proven` answers nothing for a keeper that is gone). Those messages
stay past the cursor and are offered to the next process, so nothing is
lost; an idempotent harness recognises what its predecessor consumed
(`Duplicate`), and a turn left open gets its `[Recovery]` note. A harness
that is not idempotent can apply such a message twice, within the window
between the vendor's proof and the server's commit, as when the server held
the adapter and crashed. A keeper whose answers the server cannot read is
stopped and replaced the same way.

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
   applying it gets it again later (`Deferred`) when a later offer can clear
   the refusal; a refusal nothing can clear parks it (`Rejected`) and tells
   its sender, rather than retrying forever or restarting the process. An
   answer that leaves it unknown whether the vendor applied it is settled
   from the vendor's own record, or by an idempotent resend, before anything
   is written again.
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
interrupted turn's messages reach the next process inside the `[Recovery]`
note (above), never as input of their own.

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

`tests/invariants/src/conformance.rs` runs the same eleven scenarios against
every adapter, through a real `DeliveryLoop` and a scripted vendor process
that speaks the vendor's own protocol (`tests/invariants/fakes/`: the shared
session engine in `engine.py`, one `proto_<vendor>.py` per protocol). The
vendor's journal of what its model saw is the evidence; the cursor is the
commit. A restart in the suite does what the supervisor does: it stops the
old process and commits its proofs first, and offers the recovery note
ahead of mail.

| Scenario | Holds the adapter to |
| --- | --- |
| an idle agent takes a prompt | a turn opens with the message and it is committed |
| five messages reach a busy turn | `next-step`: all five written at once, none committed before the next step, then committed after one step consumed them together, in order; `mid-turn`: all five accepted at once; `turn-end`: the running turn never sees them |
| mail keeps its order across steps | sequence order over several steps |
| a crash after partial consumption loses nothing | the vendor dies after consuming two of five: each message reaches the model once, the unconsumed ones as input of their own (`next-step`, `turn-end`) or inside the recovery note (`mid-turn`) |
| a crash between consumption and proof applies nothing twice | the vendor journals two of five messages, then dies before it writes their proof: nothing is lost, and an idempotent harness's next process recognises the two |
| a long interrupted message reaches the next process whole | a 4,000-character prompt is interrupted by a crash: the recovery note carries it whole |
| a host restart redelivers without applying twice | `idempotent`: the cursor is lost after the vendor consumed three messages; a new process recognises all three |
| an offer twice reaches the vendor once | a message offered again to the process that took it is not written again |
| a refused message waits without a restart | the vendor refuses a message for its running turn: no restart, applied once later |
| a slow answer neither fails nor duplicates | every vendor answer is 1.5 s late: no failure, no restart, no second write |
| a photo in the mailbox reaches the model as its path | a message naming a file in `<data>/media`, to an idle agent and to a busy one: the model is given the text, which names the file, and no image; the agent decides whether to look ([media.md](media.md)) |

A new harness gets the suite by implementing `Subject` (how to start the
adapter against the scripted vendor) and invoking `conformance_suite!` once,
and by teaching the scripted vendor its protocol.

`crates/anyagents-supervisor/tests/proofs.rs` holds the supervisor to the
same rules over a scripted adapter: a harness switch and a profile rotation
during a quota wait commit the proofs the old process gave before it
stopped; a crash puts the recovery note ahead of later mail and keeps long
messages whole; a crash between consumption and proof hands the line to the
next process; a poison message is parked, its sender told, and the mail
behind it flows without a restart.

## Harnesses

Each adapter crate exports the level it holds (`DELIVERY`); the server shows
the running process's, with what its handshake named, in `agents list`,
`agents inspect`, `status` and the agents table.

| Harness | Level | Idempotent | How a message reaches it, and the proof |
| --- | --- | --- | --- |
| `spiron` (legacy `sylphx`), `ready` names `steer_consumed` and `idempotent_input` | `next-step` | yes | `spiron --headless --json`. The whole inbox is written at once: a `prompt` when no turn runs, a `steer` into the running one. Spiron journals every pending input together, in order, at its next step; `accepted` (the turn journaled its prompt) and `steer_consumed` prove each, by `request_id`. `turn_closed` and `no_active_turn` go back to the mailbox, woken by the turn's `result` or the refusal itself; `turn_not_started` (the session is still leased) is offered again after a backoff from 1 s doubling to 30 s, and fails the process once refusals outlast Spiron's 180 s session lease; `empty_prompt`, `wrong_session`, `unknown_command` and `upgrade_retired` are rejected. A code the adapter does not know, or none, is never parked: it waits as `turn_not_started` does, and names itself in the fault if it outlasts the lease. `undelivered_steer_ids` go back to the mailbox. Spiron drops an input its journal already holds under `input:<request_id>` and answers it as a duplicate. Handshake: the `ready` frame's protocol and capabilities. |
| `spiron`, older `ready` | `turn-end` | no | One input per turn, none while a turn runs; `accepted` commits it. Gated on the capabilities, for a Spiron built before them. |
| `claude-code` | `next-step` | yes | `claude -p` with stream-json in and out and `--replay-user-messages`, and `CLAUDE_CODE_EAGER_FLUSH=1`. Each line carries a `uuid` derived from its mailbox id. Claude Code queues a line that reaches a running turn and folds every queued line, in order, into the conversation at the next step; a line still queued when the turn ends starts the next turn. `command_lifecycle` `started` and the replay echo (`isReplay`) show it took a line, but Claude Code prints them before it appends the line to the session transcript (a queue drained every 100 ms), so a line is committed only once its transcript row is on disk (`$CLAUDE_CONFIG_DIR/projects/*/<session>.jsonl`: a user row's `uuid` or `mergedSendUuids`, a folded line's `queued_command` `source_uuid`), or once a result names it in `user_message_uuids` (the eager flush appends every row before the result). The transcript is read from where the launch's scan ended, a row at a time, outside the adapter's lock, so a transcript of gigabytes or an invalid byte costs nothing. A line `cancelled` after `started`, or `discarded` with the session (Claude Code re-emits its uuid on the next worker), goes back to the mailbox unless the transcript already stores it; one Claude Code `refused` is rejected by that process. Claude Code drops a line whose uuid its transcript holds as a user row; at launch on a resumed session the adapter reads the transcript for the same marks, including lines folded in mid-turn, which Claude Code's own check misses. Handshake: `system` `init` `claude_code_version`. |
| `codex` | `next-step` | yes | `codex app-server` (JSON-RPC). `turn/start` when no turn runs, else `turn/steer` naming `expectedTurnId`; each carries the mailbox id as `clientUserMessageId`, and is sent without waiting for its answer. The answer only queues it; the `userMessage` item whose `clientId` is that id proves the turn consumed it at its next step. A `-32600` refusal the running turn clears goes back to the mailbox and is offered again when the turn's events say it changed; `-32602`, and a `-32600` on `turn/start`, are rejected. Any other error, or a message its turn ended without proving, is settled from `thread/items/list` before anything is written again; a resumed thread's newest turns are read first, and a message found there is a duplicate. A resume starts a new thread only when Codex says the thread does not exist (`no rollout found for thread id`, `invalid thread id`); any other failure fails the start. Handshake: `initialize` `userAgent`. |
| `muse` | `next-step` | yes | `muse serve` (JSON-RPC). Every message is `turn/start` with `ifBusy: "steer"` and a `commandId` derived from its mailbox id alone, the same in every process; Muse replays a known command's recorded decision, so a resend is safe. The `userMessage` `item/completed` carrying that `commandId` proves it (Muse stores input before it announces it); `turn/completed` settles the inputs its turn consumed, and a steer the turn never consumed stays in flight until the successor turn Muse names after its command proves it. On resume the adapter reads the session's record (the history `session/resume` returns, paged with `view/page` when over budget): a message it holds is a duplicate, never written again. `-32030` with reason `command_id_conflict` means the command was admitted before: it waits for its proof under the same id. A refusal whose `ErrorData.retryable` says it can succeed (`runtime_busy`, a turn-command deadline) goes again under the message's next attempt id, `command_id(mailbox id, attempt)`; Muse replays the decision it recorded for each id, so every process walks the same chain from attempt 0 and admits nothing twice. Any other `-32030`, and `-32002` (over the limit) and `-32602` (malformed) unless marked retryable, are rejected; admission refusals (`-32031`, `-32001`, …) and interrupted requests wait under the same id. A session record Muse cannot page fails the start. Handshake: `initialize` `serverInfo`. |
| `omp` | `next-step` | yes | `omp --mode rpc`, with steering mode `all`. A `prompt` with `streamingBehavior: "steer"` is proved by the user `message_end` whose text carries its marker, which OMP emits as the next step takes every queued steer in order (it queues the message's persistence first); `success: false` hands it back, and a `prompt_result` for a message never proved means OMP dropped it, which also hands it back. At start the session file `get_state` names is read; a message it holds is a duplicate. OMP writes a new session's file only with its first assistant message, so a missing file holds nothing, and the session is named for the next process only once its file exists (OMP exits on `--session` for one it never wrote). A file that exists and cannot be read, or a refused `get_state`, stops delivery. Handshake: the `ready` frame's protocol version. |
| `grok` | `mid-turn` | no | `grok agent … stdio` (ACP), `follow_up_behavior = "steer"`. `session/prompt` is committed when Grok shows it admitted: its marker in `x.ai/queue/changed` or a `user_message_chunk`, or the answer to its request. Grok has no refusal a later offer clears, so an error before admission rejects the message, never writing it twice. A prompt is written only after every earlier one is admitted. Grok's consumption notice names no input and Grok deduplicates nothing. Handshake: `initialize` `agentInfo`. |
| `agy` | `turn-end` | yes | `agy --output-format stream-json`; a line on stdin runs a turn. A message for a running cascade goes through the language server's `SendUserCascadeMessage` (`NEXT_INVOCATION`), which only queues it in the cascade's mailbox (`messages/`, with an `undelivered/` marker until taken): its answer proves nothing, and a cascade that errors or is retired never takes it. A message is committed only once the cascade took it: a `USER_INPUT` row in its transcript opening with the message's marker, or its mail in the cascade's `read.json`. While a cascade holds untaken mail, later mail is queued behind it in the same mailbox, never opened as a turn that could run first. Before a process resumes a cascade the adapter reads its record: a message its transcript holds is a duplicate, one still undelivered is held, not written again. Until a running turn's mailbox reads are proven against the vendor, the level reported is `turn-end`. Handshake: the language server version in `cli.log`. |
| `opencode` | `mid-turn` | yes | `opencode serve` 2.x (HTTP), called through one-shot `opencode api` processes; a 1.x service fails the start. Each message is `POST /api/session/{id}/prompt` with `delivery: "steer"` and its mailbox id as `id` (prefixed `msg_` when it is not already), one call at a time, in mailbox order, with no timeout. The answer is the session's durable inbox admitting it, which is the proof; OpenCode takes every pending steer into its running turn at the next step, in admission order. OpenCode admits an id once, from any process: an answer naming a record created before the call, which the conversation already holds (`GET …/message/{id}`), is a duplicate. A 400 or 409 is rejected; the readiness gate's 503 (`service_starting`) waits a second and is offered again; a 5xx or a lost answer is settled by sending the same id once more; a 401 or 403 fails the process. The turn ends when `POST /api/experimental/session/{id}/wait`, asked after the newest admission, answers. OpenCode announces consumption only on its event stream (`session.inbox.delivered`), which the adapter does not read. Handshake: `GET /api/info`. |
| `dsh` | `mid-turn` | yes | The dsh web gateway. Each message is `session/prompt` with `mode: "steer"` and its mailbox id as `requestId`, one call at a time, in mailbox order, with no timeout; `accepted: true` is dsh's in-memory inbox taking it. The admission is not durable: dsh writes its inbox to disk afterwards, and SIGTERM clears it, so mail accepted and never read reaches the next process inside the `[Recovery]` note. dsh admits a `requestId` once, so an answer that leaves it unknown (`session/agent-busy`, `gateway/internal`, a 5xx, a lost connection) is settled by sending the same request once more. A malformed request (`gateway/bad-request`, `arguments-invalid`, `input-invalid`, HTTP 400, 413, 415) is rejected; `session/not-found` re-creates the session and a 401 buys a new cookie before the same request goes once more; 403 and `session/writer-held` fail the process. Its answer does not say whether the request was new, so a message an earlier process applied is accepted again, never applied twice, rather than reported as a duplicate. Handshake: none (the gateway names no version). |
| `spiron-cloud`, `claude-cloud`, `openai-cloud`, `cursor-cloud` | `turn-end` | no | `anyagents cloud-harness` speaks Claude Code's stream-json for a cloud agent: one turn per line, the replay echo when the line's turn starts. |
