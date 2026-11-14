# 0006: Runtime state and lifecycle: one writer per agent, explicit persisted state machines

Status: proposed; amended on 2026-11-14 by [0009](0009-product-boundary.md): decisions 3, 8, 9 and 11 are kept, and decisions 1, 2 and 7 are deferred. Decision 11 now applies per agent ([0010](0010-execution-identity-and-trust.md) D5). Date: 2026-11-12. Part of [0003](0003-runtime-rebuild.md). Refines [0001](0001-server-first-one-binary.md) decisions 3 and 8 and the supervisor section of [architecture.md](../architecture.md#agents-create-and-supervise).

## Context

AnyAgents has no single owner for an agent's runtime state. The lifecycle field (`agents.state`), the delivery cursor, the open turn (`turn_recovery`), steer custody, quota, faults and process custody are each written by several independent parties:

- the per-agent supervisor thread;
- the supervisor router;
- HTTP lifecycle routes (pause, archive, delete, reset, wake);
- the one-second schedule census;
- the 60-second maintenance reaper;
- harness reader threads;
- out-of-process callers: the CLI, and mailbox inserts that change state as a side effect.

Each party decides "turn open", "busy" or "quota blocked" with its own predicate: usage deltas, `turns_served` counters, error-text substrings, flock probes or mailbox-lag reads. Several pieces of state live outside the database entirely:

- the Spiron steer hold, in a JSON file;
- the quota deadline, in memory;
- operator marker files (`.anyagents-quota-cleared-ms`, `.anyagents-profile-rotated-ms`);
- faults, in `.anyagents-last-error.json`.

This produces a recurring class of failures:

1. **Polling cost.** There's no event for "turn ended", "process tree empty" or "background command finished", so the supervisor polls instead: per-agent review timers, `/proc` walks, file-marker polls, a 200 ms socket poll, the one-second schedule census and its per-agent busy probes. An idle fleet consumes CPU in proportion to its size.
2. **Lock contention kills turns.** In one daemon, every agent loop and many request handlers open their own read-write SQLite connection (and rerun migrations at open). A write that exhausts its busy budget is then reported as a *turn fault*, and the live process is replaced.
3. **Orphaned process trees.** Shutdown signals only the harness's root pid, with no process group, grace period or wait for descendants. Grandchildren (the `anyagents mcp` child, tool subprocesses, a wrapper's real vendor process) survive and keep holding the vendor session. The replacement then fails with "session already in use", and two vendor processes can end up writing one vendor session store.
4. **Non-transactional lifecycle.** Pause takes no lifecycle lock and returns after a bounded wait even when the worker is still alive. Reset's flock probe then reports "already running" for a paused agent, and a worker that is mid-relaunch races seat admission.
5. **Stranded steer custody.** A hold file without a generation blocks startup forever when reconciliation can't read the vendor journal, and a hold from a retired session can't be told apart from a live one.
6. **Unsafe workspace cleanup.** Archive looks for Git repositories only at the first level of `work/`. Deeper repositories and loose files are deleted, and build output is deleted by directory *name*, including on every idle release.
7. **Quota by guesswork.** Reset times come from prose ("resets in …"), recursive key scans and magnitude-based guesses of the epoch unit. Typed `retry_after_seconds`, `Retry-After` and `x-ratelimit-reset` values are lost at the stream boundary, and schedule admission still blocks on error text after a known deadline has passed.
8. **Inferred turns.** A turn closes only when usage or a turn counter moves. A harness whose terminal frame moves neither leaves the turn open forever, and the mail behind it is never released.
9. **Dropped schedule ticks.** A blocked `every` tick is advanced past `now` and recorded as skipped.
10. **Non-atomic spawn.** Spawn reserves the name, then prepares the workdir, seat, worktree and bindings as separate steps, with no rollback. A failure partway, such as a seat that can't write a repository's metadata, leaves a ghost: the name reports "already exists", but no agent can be found under it.
11. **Shadowed role templates.** Template lookup prefers copies under each workspace's `.agents/agents/` over the configured catalog. A role added to the catalog is rejected for any agent holding a stale copy until someone refreshes that copy by hand.

## Decision

1. **Agent actor is the single writer.** The supervisor keeps a registry `AgentId → ActorHandle`. An actor is one task per agent with pending work. It holds the agent's process flock for its lifetime and is the only writer of that agent's runtime state: lifecycle state, delivery cursor, turns, steer holds, session lease, quota block, lifecycle-op journal and schedule pending counters.
   - Its inbox carries typed events: `MailCommitted{seq}`, `Harness{generation, kind}`, `Command{op, reply}`, `Timer{token}`, `ScheduleDue{id, due}` and `ChildChanged{child, state}`. `Harness` kinds are `Accepted`, `SteerAck`, `TurnStarted`, `TurnEnded{outcome, usage, undelivered}`, `RateLimited{reset}`, `Fault{kind}`, `Exited` and `GroupEmpty`.
   - HTTP routes and the CLI never write runtime fields. They send a `Command` and await its reply. Offline CLI (no daemon) runs the same transition code under the same flock.
2. **Events, not polls.**
   - Harness stdout produces `Harness` events.
   - A `pidfd`/`waitpid` watcher produces `Exited`.
   - Process-group or cgroup emptiness produces `GroupEmpty`. This replaces `/proc` scans for in-flight background work.
   - In-process store commits produce `MailCommitted`. Writes from other processes send a `wake.sock` datagram, which is only a hint: a blocking receive, no timeout loop.
   - Operator marker files become `Command`s.
   - Per-actor deadlines (backoff, quota reset, switch drain) are timers.

   An actor reconciles from durable state whenever it starts, so a lost hint never loses work. It exits when it is `Released` and its inbox is empty.
3. **Store write discipline.**
   - Each process has exactly one read-write connection per database. It's owned by a writer thread that executes typed operations and group-commits batches in one `BEGIN IMMEDIATE`.
   - `Store::open` returns a handle to that per-process registry, and migrations run once per process.
   - Reads use a read-only connection pool. No read path opens a write transaction.
   - `SQLITE_BUSY` can only come from another OS process. It's retried with bounded exponential backoff.
   - On exhaustion the actor keeps the transition pending in memory, arms a retry timer and stays in its current state. A storage error is never a turn fault and never replaces a process.

   The Store stream owns all of this schema and every typed operation.
4. **Explicit persisted state machines** (normative tables below). A turn closes **only** on the harness's explicit terminal event. Each adapter must emit `TurnEnded`, and usage is recorded from that event. Everywhere, "busy" is one predicate: `turn ∈ {Open, RecoveryOffered}` or `runtime ∉ {Live, Released}`.
5. **Session lease tied to process identity.** A `session_leases` row records `(vendor_session_id, generation, holder_pid, holder_start_time, pgid)`.
   - It's valid only while `holder_pid` is alive with the same start time.
   - Every harness child starts in its own session and process group (`setsid`). Seat mode uses a cgroup or the seat launcher.
   - Stop sends `SIGTERM` to the group, waits a grace period, sends `SIGKILL` to the group, and releases the lease only after `GroupEmpty`.
   - A new generation can't start while a valid lease exists.
   - At daemon start, a lease held by a dead process is released, and a live orphaned group is terminated first.
6. **Quota is a state, with a typed reset.** `quota_blocks(until_ms, source ∈ {known, estimated}, attempt)` is the only admission authority.
   - Adapters convert typed fields with explicit units (`resets_at_ms`, `retry_after_seconds`, `Retry-After` seconds or HTTP-date, `x-ratelimit-reset` Unix seconds) through one shared conversion module.
   - An unknown reset becomes an estimated deadline on a bounded schedule (1, 2, 4, 8, then 15 minutes).
   - When the timer fires, the block clears.
   - Prose parsers, magnitude unit guesses, recursive key scans and every error-substring check (schedule admission, auth and transient classification) are deleted. Adapters classify faults as `FaultKind{Auth, Quota{reset}, Transient, Fatal}`.
   - Harnesses that run behind a gateway must forward the typed retry fields in their event stream. That is a documented adapter contract.
7. **Lifecycle operations are journaled commands.** Each op writes a `lifecycle_ops` row with actor, reason, evidence and UTC times. It moves through the phases `Intent → Quiesce → Stop → Commit → Effects → Done`, and a crash resumes it at its recorded phase. The command's reply is sent only after `Done`.
   - **Pause:** hold delivery (mail stays queued), cancel or drain the turn per request, stop the process group, release the lease, commit `Paused`. Pause doesn't revoke execution principals.
   - **Reset:** allowed from `Paused`, `Released` or `Faulted`. Retire the generation and vendor session; move the interrupted turn to recovery; archive the old generation's steer holds with that generation. Reset moves out of agent creation into the lifecycle module.
   - **Archive:** pause, then workspace disposition (decision 8), grant revocation, commit `Archived`.
   - **Spawn:** reserve the name, prepare the workdir, admit the seat, provision the worktree and write the bindings. All of it is recorded in the op row. The name and the agent row become visible together, in the `Commit` phase. A failure before `Commit` rolls back every completed phase in reverse order: release the reservation, revoke the seat grants, remove only the directories this op created (each one journaled when created), and drop the bindings. After a crash, a spawn op that never committed rolls back at startup; it never resumes into a half-made agent.
   - **Delete:** archive, then refuse if disposition preserved anything, unless the caller names a destination to move it to.
8. **Workspace disposition is proof-based.** Walk the managed `work/` tree recursively for `.git` (directory or file) at any depth.
   - A repository is removed only when its status is empty, every local branch tip is contained in a remote-tracking ref, and it belongs to this agent: journaled, or created inside its own managed directory.
   - Build output is removed only when `git check-ignore` says it is ignored.
   - Any unclassifiable or non-Git path preserves the whole tree. Any error preserves.
   - Idle release removes only harness cache directories under the config directory, never anything under `work/`.
9. **Schedule ticks coalesce, never drop.** The scheduler is a timer heap on `next_fire_at_ms`, woken by schedule changes, and it sends `ScheduleDue` to the owning actor.
   - An eligible actor delivers immediately. A blocked one records `pending_since_ms = COALESCE(pending_since_ms, due)` and `pending_count += 1`.
   - On its next eligible transition it delivers one wake that names the count and first due time (source key `schedule:<id>:<pending_since_ms>`), then clears the counters.
   - External watches keep their own probe loop, but they only emit observations.
10. **One source for role templates.** Templates resolve only from the single catalog source the caller configures (a catalog directory under the data directory, or an explicit catalog path). Workspaces carry no override copies, and lookup never reads `<workdir>/.agents/agents/` or a parent workspace's copy. Changing a role means changing the catalog; agents created after that use the new version. An agent records the template's name and content digest at creation, for audit.
11. **No shared mutable Git metadata.** Each execution seat writes only its own repository's config and refs; the pool is read-only. Commit identity is never written into shared configuration.

### State machines

**Runtime (per agent, `agent_runtime`)**

| From | Event | To | Persisted with the transition |
|---|---|---|---|
| Released | pending mail, schedule or recovery | Starting{g+1} | lease acquired |
| Starting | child spawned in its own group | Live{g} | lease holder `(pid, start_time, pgid)` |
| Starting, Live | `Exited`, `Fault{Transient}` | Backoff{until} | attempt+1; open turn → Interrupted |
| Live | `RateLimited{reset}` | QuotaBlocked{until} | `quota_blocks` row |
| Backoff, QuotaBlocked | timer, `ClearQuota`, config change | Starting{g+1} | block cleared |
| Live | no open turn, mailbox drained, `GroupEmpty`, no running child | Released | lease released |
| any non-terminal | `Pause` | Draining → Paused | op journal |
| any | `Fault{Auth}`, or repeated identical fault past limit | Faulted{kind} | fault row |
| Paused, Faulted | `Resume` | Released | — |

**Turn (`turns`)**

| From | Event | To |
|---|---|---|
| none | `Accepted` while the harness is idle, or `TurnStarted` | Open |
| Open | `Accepted`, `SteerAck` | Open (`last_seq` advances) |
| Open | `TurnEnded{outcome}` | Closed{outcome} |
| Open | `Exited`, generation change | Interrupted |
| Interrupted | next `Live` with no earlier actionable mail | RecoveryOffered (`recovery_count+1`; beyond the limit → runtime Faulted) |
| RecoveryOffered | `Accepted` | Open (`recovers = previous turn`) |

**Steer custody (`steer_holds`, replaces the JSON hold file)**

| From | Event | To | Mailbox effect |
|---|---|---|---|
| — | before any input bytes are written | Held | cursor unchanged |
| Held | `SteerAck` / `accepted` | Acked | cursor advances |
| Acked | `TurnEnded` without it in `undelivered` | Committed (row removed) | — |
| Acked | listed in `undelivered`, or the vendor journal shows no model event after it | Unconsumed | rewound for exactly one redelivery |
| Acked | the journal shows it consumed and the turn is Interrupted | Consumed | resumed once with `[Recovery]` |
| Held, Acked | generation not live at startup | Reconciling | resolved to one of the above |
| Reconciling | journal unreadable | Reconciling + runtime `Faulted{steer_reconcile}`, retry timer | custody unchanged; `Reset` retires it |

A hold whose generation is not current never blocks a start.

## Invariants

| # | Invariant | Test |
|---|---|---|
| I1 | An idle fleet does no work without an event | extend `supervisor/tests/idle_cost.rs`: 100 idle agents for 60 s ⇒ under 1% of one core, zero store writes, zero `/proc` reads (counting hooks); the scheduler sleeps until the next due time |
| I2 | No lock error surfaces under load | `core/tests/writer_load.rs`: 100 actors × 1,000 operations alongside an external-process writer ⇒ no `SQLITE_BUSY` returned; exactly one read-write connection per process |
| I3 | Storage errors never fault a turn | inject `BUSY` while persisting `TurnEnded` ⇒ process stays `Live`; the transition commits after retry |
| I4 | No lost wake | property test over interleavings of mail commit, idle release, crash and dropped hints ⇒ every received row reaches the harness |
| I5 | Crash recovery converges | failpoint kill at every transition and lifecycle phase ⇒ after restart: one process per agent, valid lease, no duplicate delivery, each interrupted turn recovered once |
| I6 | One process tree per agent | fake harness that forks grandchildren ⇒ after pause, reset or archive, the group is empty and the lease released; a new generation starts without a session conflict |
| I7 | Steer custody is total | every steer state × crash ⇒ consumed input never replayed, unconsumed input redelivered exactly once, foreign-generation holds never block start |
| I8 | Turns close only on terminal events | adapter conformance fixtures: every harness emits `TurnEnded`; a usage-only change doesn't close a turn |
| I9 | Quota admission is typed | each reset field and unit ⇒ correct `until_ms`; error text containing "429" with a passed deadline ⇒ eligible; unknown reset follows the bounded schedule |
| I10 | Schedule ticks coalesce | ten ticks while blocked ⇒ exactly one delivery with count 10 after unblock, including across a daemon restart |
| I11 | Archive is data-safe | dirty repository at depth 1, 2 and 3, unpushed commit, tracked `dist/`, loose file ⇒ all preserved; a clean, pushed, owned repository ⇒ removed |
| I12 | Single writer per agent | test-build assertion: a write to actor-owned state from anything but the owning actor's writer operation fails the test |
| I13 | Lifecycle replies mean done | `Pause` returns only after the group is empty and the state is committed; `Reset` of a paused agent never reports busy |
| I14 | Spawn is atomic | failpoint at each spawn phase (reservation, workdir, seat, worktree, bindings, commit) and a kill mid-op ⇒ afterwards the name is free, no agent row, grant or binding remains, and only pre-existing directories are left; retrying the spawn with the same name succeeds |
| I15 | Role templates have one source | a catalog role plus a conflicting `<workdir>/.agents/agents/<role>.md` (and one in a parent workspace) ⇒ resolution returns the catalog version; a role added to the catalog is accepted immediately for an agent whose workspace holds stale copies; the purity/lint test fails if lookup code references a workspace template path |

## Migration (zero-loss)

One additive migration (Store stream) creates `agent_runtime`, `turns`, `steer_holds`, `session_leases`, `quota_blocks` and `lifecycle_ops`, and adds `schedules.pending_since_ms` and `schedules.pending_count`. A one-time backfill runs at daemon start in a single transaction, with idempotent file imports and before/after counts logged:

- **Agents:** `running` and `idle` → `Released` (actor activation reconciles pending work); `paused` → `Paused`; `archived` → `Archived`. `.anyagents-last-error.json` → `Faulted{legacy}`, keeping its text as evidence.
- **Turns:** a `turn_recovery` row with `opened_seq > completed_seq` → `Interrupted` turn (`first_seq = completed_seq + 1`, `last_seq = opened_seq`, `recovery_count` preserved). Otherwise → one `Closed` row. For one release the writer keeps `turn_recovery` updated too, so a downgrade stays correct.
- **Steers:** each hold file and its reconciled copy → `steer_holds` in `Reconciling` at the current generation. The file is renamed with a `.migrated` suffix and never deleted. Holds under retired-generation directories → archived rows.
- **Sessions:** vendor session ids stay where they are, and the lease table starts empty. A single boot-time sweep finds harness trees started by an earlier binary (matched by the agent's config directory in their command line or environment) and terminates each by process group before the first start. It's the only process-table walk, and it runs once.
- **Mail:** cursors are untouched, and id-based delivery dedupe stays.
- **Schedules:** rows are unchanged. Ticks skipped before the migration can't be reconstructed; coalescing applies from the migration on.
- **Ghost names:** the backfill lists each name reservation that has no agent row, or whose agent has no committed spawn. Each one is released, and the evidence is logged. Directories are left in place for operator review, never deleted.
- **Template copies:** per-workspace `.agents/agents/` copies are no longer read. They're left untouched, as user files. Before the switch, a one-time report lists each copy whose content differs from the catalog, so an intended customization can be moved into the catalog.
- **Operator markers:** a marker newer than the current process start becomes the matching `Command`, and the file is ignored afterwards.

## Delivery

Streams and file ownership follow [0003](0003-runtime-rebuild.md).

- **W0, independent fixes now.** The archive-safety fix (recursive repository detection, no name-based deletion in the archive reaper or in idle release; Lifecycle owner, Harness-boundary review for the supervisor hunk). Per-seat repositories over a read-only pool (#213). Schedule `every` coalescing in `schedule.rs`. #223 stays as merged. #210 does not merge separately; its journal-evidence classification and fixtures move into the steer machine below.
- **W1, Store** (`crates/anyagents-core/**`).
  - PR 1: the additive schema and typed operations from the Migration section, plus the backfill.
  - PR 2: the per-process single-writer registry behind `Store::open`, the read pool, and removal of per-open migrations and read-path write transactions. Tests I2, I3.
- **W2, Harness boundary** (`anyagents-supervisor`, `anyagents-agent`, `anyagents-harness*`, `vendor.rs`), first PR. Agent actor and registry, the three state machines, process groups and session leases, typed harness events (`TurnEnded`, `RateLimited`, `FaultKind`), the shared quota-conversion module, and steer holds in the store.
  - Removes: `Session::run` polling, the `Clock` review loop, `/proc` scans, the `stop_slot` spin, marker-file polls, usage-inferred turn completion, text fault classification, per-adapter reset parsers and the JSON hold file. Tests I1, I4–I9, I12.
  - Exposes `Supervisor::command(agent, op)` for Lifecycle.
- **W3, Lifecycle** (`server.rs`, `schedule.rs`, `agent.rs`, new lifecycle module).
  - Routes and CLI become commands, and reset moves out of agent creation (coordinated with Credentials). Spawn becomes a journaled command with phase rollback, and template resolution moves to the single catalog source; the spawn-path edits are coordinated with Credentials, which owns `create.rs`. Proof-based workspace disposition replaces the W0 reaper. The schedule heap and actor-side coalescing replace the census, the busy probes and quota text admission.
  - Removes: the pause/delete/archive wait loops, reset's flock probe and `agent_wake_block_reason`. Tests I10, I11, I13, I14, I15.

Order: W1 PR 1 unblocks W2 and W3. W3's disposition and schedule work can proceed against W1 alone, and its commands need W2's actor API.

## Consequences

- Idle cost is proportional to events, not to fleet size. A busy daemon's write throughput is bounded by one connection's group commits, not by contention between connections.
- Every runtime fact has one durable home and one writer, so `agents list`, schedules and delivery agree on "busy", "quota" and "turn open" by construction.
- Adapters take on a stricter contract: an explicit terminal event, typed fault kinds and typed reset fields. A harness that can't report turn end can't be admitted in `steer` mode without a documented, tested equivalent.
- Lifecycle calls can take longer (they wait for the process group to empty), and they now report completion truthfully.
- Archive keeps more data. Unclassifiable workspaces are kept and reported, never guessed away.
- The process-group requirement applies to every runtime boundary. Container and cluster runtimes satisfy it with their own process isolation.

## Alternatives rejected

- **Keep patching the per-thread loop (more backoff, more probes).** Each fix moves contention or polling somewhere else; the incident history shows the same class recurring.
- **A larger `busy_timeout` or more retries.** It hides contention and lengthens stalls; turns still fail when the budget runs out.
- **A global supervisor mutex instead of per-agent actors.** One agent's slow operation would stall the fleet.
- **Persist only in memory and rebuild from vendor transcripts after a crash.** That breaks [0001](0001-server-first-one-binary.md)'s rule that nothing reads a vendor transcript, and it loses custody across restarts.
- **Infer turn end from idle timeouts or usage deltas.** That is the current failure mode.
- **Delete build directories by well-known names.** Tracked directories share those names.
- **Per-workspace template copies with a refresh job.** Copies drift by construction and need a second writer to keep them in sync. One source removes the problem.
- **Clean up ghost names with a periodic sweep.** That leaves a window where the name is unusable, and it guesses which half-made state to keep; phase rollback makes ghosts impossible instead.
- **Drop blocked ticks ("missed ticks never queue").** That loses work silently; coalescing keeps a single delivery without a backlog.
