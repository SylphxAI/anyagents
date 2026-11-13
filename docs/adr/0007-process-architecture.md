# 0007: Process architecture: a small kernel, keepers that hold harnesses, services and connectors outside

Status: accepted for keepers (implemented); proposed for the service and connector splits. Date: 2026-11-13. Builds on [0001](0001-server-first-one-binary.md) (server first, one binary) and refines [0006](0006-runtime-state-and-lifecycle.md) decision 5 (process groups and session leases).

## Context

`anyagents serve` is one process that does everything. The binary crate `crates/anyagents` is about 105,000 lines of Rust. Measured on `c148eede`:

| Part | Lines | Runs in |
| --- | --- | --- |
| agent creation and reset (`create.rs`) | 16,000 | the server, the CLI |
| HTTP API and server loops (`server.rs`, `server/`) | 15,500 | the server |
| MCP tools (`mcp.rs`, `mcp_config.rs`) | 10,800 | `anyagents mcp` (a child of each harness), calling the server API |
| browser workstation (`browser*`) | 9,300 | the server |
| schedules and watches (`schedule*`) | 7,300 | the server |
| Telegram (`telegram/`, `channel.rs`, `anyagents-channel-telegram`) | 9,900 | the server |
| computer use (`computer*`) | 4,400 | the server |
| host memory and history (`host_memory.rs`, `anyagents-core` `host_*`) | 10,200 | the server |
| protocol gateway (`anyagents-gateway`, `gateway.rs`) | 5,300 | the server |

Every harness process was a child of `serve`, and the adapter that speaks its protocol was an object in `serve`. When `serve` stopped, the supervisor dropped every adapter, and each drop killed its process group (`TERM`, 800 ms, `KILL`). When `serve` crashed, each harness lost its stdin and exited at end of input; Spiron's headless controller "drains accepted work before process exit". Either way every running turn ended. The next server started each agent again with its vendor session, and an interrupted turn got a `[Recovery]` note asking the model to check what it had already done.

On the desk, `desk-services` runs `serve` and restarts it with `killpg` on the service's process group. At the time of writing `serve` had 125 children: 34 Spiron, 12 Claude Code and 5 Antigravity harnesses, 66 `xvfb-run` and 7 Chromium for the browser workstation; the store had 49 agents `running`, 65 `idle` and 19 `paused`. Every deploy, configuration restart or crash of `serve` interrupted all of that work.

The memory that makes delivery idempotent also lived in `serve`: "each adapter remembers the ids its session holds" ([architecture](../architecture.md#mailbox-one-log-per-agent)). Spiron's adapter remembers the `ready` capabilities, every input it wrote and has not seen consumed, and which turn is open; Claude Code's adapter holds its hook state; Codex's holds JSON-RPC request ids. A restart lost all of it with the process.

## Decision

### 1. What the kernel is

The kernel is `anyagents serve`. It keeps what must have exactly one owner:

- **identity:** agents, capabilities and the operator boundary, and every authorization decision;
- **the mailbox:** receive, sequence and the delivery cursor;
- **supervision:** which agents should run, and the per-agent worker that drives each one's harness;
- **schedules and lifecycle commands;**
- **the store:** the only writer of `state.db` ([0006](0006-runtime-state-and-lifecycle.md) decision 3).

Everything else is one of three kinds of process around it, each reaching the kernel through its API (the Unix socket the CLI uses) or through a keeper's socket:

- a **keeper** holds one harness process for the kernel;
- a **service** does work for agents that is not identity, mail or supervision (the browser and computer workstation, the protocol gateway, later history retrieval);
- a **connector** carries an outside channel into mailboxes and out of them (Telegram).

### 2. Keepers hold harness processes

One keeper per harness process. A keeper is `anyagents keeper --dir <dir>`: the same binary, dispatched before argument parsing so it runs nothing else. Its protocol and loop are the `anyagents-keeper` crate (standard library, `libc`, `serde`).

A keeper:

- starts the harness through the vendor adapter the kernel used to host, from a launch the kernel prepared (`AdapterLaunch`: program, arguments, environment, session, working directory). It owns the harness's process group, its pipes, and the adapter's memory of the session;
- holds a copy of the agent's process lock (`locks/<agent>.lock`) for as long as it lives;
- starts in its own session (`setsid`), with stdin and stdout on `/dev/null` and stderr on its own log. A `killpg` of the kernel's group never reaches it, and it never holds a pipe the kernel's supervisor reads;
- serves one owning kernel at a time on `<data>/keepers/<instance>/s` (directory `0700`, socket `0600`; a connection whose peer uid is not the keeper's own is closed);
- never ends its harness because a kernel left, cleanly or not. Only `stop` ends it, or the harness exiting on its own.

The adapter runs in the keeper, not only the pipes, because an adapter is one end of a stateful session. A keeper that held only the process and its descriptors would hand a new kernel a live session in the middle of a protocol it no longer knows. Recovering from that needs either a deterministic replay of every byte in both directions through each adapter, or a vendor that re-announces its state on request. Spiron could do the second; third-party CLIs cannot. With the adapter in the keeper, its state stays with the session it describes, and the kernel↔keeper protocol is the adapter contract itself.

What this takes from outside work:

- **containerd-shim and conmon.** One small process per container holds the container's stdio and exit status, reparented away from the daemon, so the daemon can restart. The daemon finds each shim again by a socket in the container's directory. So: one keeper per harness, a socket in the keeper's directory, attach after restart.
- **systemd's file descriptor store and socket activation.** What must survive a service restart is given to something that outlives the service, by passing descriptors. The agent lock is an `flock` on one open file description: it is held while any descriptor of it is open, in any process. It travels from kernel to keeper at spawn (inheritance) and from keeper to kernel on attach (`SCM_RIGHTS`), so the lock is held without a gap. The kernel's own per-process accounting (§4) is kept by the keeper for the next kernel, as `FDSTORE` keeps state for a restarted service.
- **tmux and abduco.** A session outlives its client; a new client attaches, and the newest one takes over (`attach -d`). A second kernel that says `hello` as owner supersedes the first: the first is told, and lets go without ending anything.
- **Erlang/OTP supervision.** Each harness is a child with its own crash domain and an explicit restart strategy. A supervisor that restarts re-links to children that are still alive instead of restarting them. Children that no longer belong (their agent is paused, archived or deleted) are terminated. This is the reconciliation of desired against actual state that a Kubernetes controller does.

**Granularity: one keeper per harness process, not one per host.** A host-wide keeper would be one process whose crash ends every harness, and which could itself only be upgraded by solving this problem again. Per-harness keepers fail one agent at a time. They are upgraded one at a time: a keeper runs the adapter code of the binary that started it, and the next process generation runs the new code. Since an agent is released to idle after each burst of work, generations turn over within hours. Each keeper is one more process per running agent. The adapter's threads move out of the kernel rather than being duplicated, and the keeper adds an accept thread and an event thread, both blocked when idle. Its memory has not yet been measured on the desk; stage 2 below measures it.

### 3. Kernel↔keeper protocol and versioning

- One JSON object per line over the keeper's Unix stream socket. A request is `{id, op, args}`; the answer is `{id, ok}` or `{id, err}`. Events are `{event}`: `activity` (something the kernel reads through the harness changed) and `superseded`.
- `hello {protocol: [min, max], owner}` agrees on the highest version both sides speak. The answer carries the keeper's and the harness's pids, the agent, the harness and model the process was launched for, the seat, the launch's labels, the kept note, the current health and, for an owner, the lock descriptor.
- The ops are the method names of the `Harness` contract and of `Hosted` (`session_id`, `session_invalidated`, `state_dir`), plus `note`, `detach` and `stop`.
- The op set grows without a version change. A keeper that does not know an op answers `unknown_op`, and the kernel uses the contract's default for that method, which is what the defaults are for. A new field in a carried type is added with a default. Changing what an op means, or adding a variant to a carried enum (a new `DeliverOutcome`, for example), is a new protocol version, and a kernel keeps speaking the previous version for at least one release.
- `stop` is frozen in every version. A kernel can always end a keeper it cannot otherwise speak to, and then starts a fresh one; that agent's work is interrupted once.
- **Skew.** A new kernel with old keepers (an upgrade), or an old kernel with new keepers (a rollback that still knows keepers), attaches at the highest shared version, or stops and relaunches when there is none. `anyagents keepers list` shows each keeper's protocol and binary, so an operator sees which processes still run older adapter code.

### 4. Delivery resumes from the cursor without loss or duplication

Two halves survive a kernel restart. The mailbox cursor survives because it is durable in the store; the adapter's memory survives because the keeper is still alive. The new kernel re-offers every message past the cursor, in sequence order, and the adapter answers each from what it holds:

- written and consumed: `Accepted` or `Duplicate`, so the cursor moves;
- written and still in flight: `InFlight`, and nothing is written again;
- not yet written: written now, once.

Two cases at the moment of the crash:

- A request the dying kernel was sending (a steer, a prompt) either reached the keeper as a whole line, or did not. A whole line is carried out even though nobody waits for its answer, and its re-offer is answered from memory. Bytes without a newline are dropped, so half a request never reaches the adapter, let alone the harness's stdin.
- A process taken over gets no `[Recovery]` note, because its turn never stopped. The turn closes in the process that ran it, and the kernel records the completion when the adapter reports it.

Usage is recorded once. After each usage record the kernel keeps its accounting for that process (served totals already recorded, turns recorded, the profile marker it launched under) in the keeper's note. The next kernel restores it, so a turn already recorded is not recorded again. One window remains: a crash between committing a usage row and writing the note records that burst twice. Closing it needs an idempotency key on usage rows, which is the Store stream's schema.

A configuration change made while no kernel ran is a switch, applied at the process's next boundary like any other: the keeper reports the harness and model the process was launched for, and the worker compares them with the agent's row.

### 5. Failure isolation

- **The kernel crashes or stops.** Nothing the harness can see changes. When a kernel stops on a signal or an error, it detaches every keeper and keeps its own copies of the locks from unlocking. At startup the supervisor reconciles: a running agent's worker takes its keeper over, and the keeper of an agent that no longer runs is stopped.
- **A keeper dies** (an adapter panic, a kill). Its connection closes without `superseded`. The kernel then ends the harness group the keeper recorded, as the seat when it runs in one, before anything starts a replacement. After that the normal path runs: backoff, a new keeper, resume of the vendor session, a `[Recovery]` note. A keeper found dead at startup (its socket refuses) is cleared the same way, from its record.
- **The harness dies.** The keeper stays and reports the process gone. The kernel stops that keeper and starts another, as before.
- **Two kernels** (the server lock already forbids it). The newer owner supersedes the older, and the older lets go without stopping anything.

### 6. Resource accounting

Each keeper is a process with a pid, and its harness is a process group. `anyagents keepers list` prints both for every keeper, so CPU and memory are attributable per agent. The next step, not implemented, is a cgroup v2 leaf per keeper where the deployment delegates a cgroup subtree. That would give memory limits and an exact `GroupEmpty` event for [0006](0006-runtime-state-and-lifecycle.md).

### 7. Seats and uids

`spawn_command` starts a harness in its OS seat (`anyagents-setpriv --reuid … --regid … --groups … --no-new-privs`) and strips the server-only environment; signals to a seat's group go through the seat launcher as that uid. A keeper runs as the server's own uid and starts its harness through the same `spawn_command`, so the seat boundary is unchanged. It runs as the server's uid, not the seat's, because:

- it holds the agent's process lock under the server-owned `locks/`;
- its socket must not be reachable from the harness it holds;
- it is the one that signals the seat group.

The launch, which carries the harness environment including gateway tokens, reaches the keeper through its stdin pipe and is never written to disk.

### 8. Services and connectors, subsystem by subsystem

| Subsystem | Decision | Why | What the split costs |
| --- | --- | --- | --- |
| Telegram | **Leaves first, as a connector.** | It is already a client-shaped thread family (long polls, offsets, outbound sends) inside the kernel. Every kernel stop waits up to 30 s to release every bot's poll. Out of process, a kernel restart no longer drops polls, and a connector crash no longer touches delivery. | An inbound API that stores an event with its source key (the mailbox already deduplicates on it). An outbound route: an agent's `send` to a `telegram:` target becomes an outbox entry the connector takes and acknowledges. The bot token is read by the connector, not the kernel. The channel's identity and bindings stay in the kernel. |
| Browser and computer workstation | **Leaves second, as a service.** | It owns long-lived Chromium and Xvfb processes (73 on the desk) that are the kernel's children, and it carries the largest dependency surface. As a service it is its own crash domain, and a kernel restart no longer reparents its processes. | The `browser` and `computer` tools call the service, with a capability the kernel mints per agent, instead of the kernel. Profile locks and the idle reaper move with it. |
| Protocol gateway | **Leaves third, as a service.** It stays in the kernel for now, and keeps its loopback port across restarts so a harness that outlived the kernel still reaches it. | API-key harnesses call it on every model request. In the kernel, a restart refuses those calls for the length of the restart. No running desk agent uses it today (3 archived OpenCode agents do). | Route and token state move to the service's own file; the kernel tells it when an agent's key changes. |
| History and memory retrieval | **Stays for now.** | Its index lives in `state.db`, beside the mailbox it indexes, and each query is an authorization decision over the caller's tree. A separate process would be a second writer, which [0006](0006-runtime-state-and-lifecycle.md) forbids. | It moves when its index moves to its own database file. Then it becomes a read-only service that answers queries scoped by a kernel-issued token. |
| Skills | **Stays.** | It is part of preparing a launch (canonical skills linked into an agent's config) plus one maintenance pass; there is no long-lived work to isolate. | — |
| Schedules and watches | **Stays.** | Schedules are kernel state ([0006](0006-runtime-state-and-lifecycle.md) decision 9). GitHub watch probes are observations that the kernel turns into mail. | A watch prober could later become a connector feeding observations through the same inbound API as Telegram. |

### 9. Where in-process hosting remains

`HarnessHost::InProcess` hosts the adapter in the calling process. It remains for two callers:

- a server embedded in a test binary, which has no keeper mode;
- the one-shot offline `anyagents agents flush`, whose harness ends when the command does. `flush` now takes the agent's process lock, so it never starts a second process beside a live keeper.

`anyagents serve` always uses keepers. **Removal note:** delete `InProcess` once the server tests that embed a server run against the real binary (or the test binary gains a keeper entry point), and `flush` is a kernel command.

## Invariants

| # | Invariant | Test |
| --- | --- | --- |
| K1 | A kernel crash mid-turn, with steers in flight, ends no harness; the harness consumes them while no kernel runs; the next kernel re-offers them and none is written twice | `anyagents-keeper` `a_kernel_crash_mid_turn_keeps_the_harness_and_its_pending_steers` |
| K2 | A crash, an upgrade to another binary and a clean stop of the real server each leave the same harness process; every message is answered exactly once | `anyagents` `server_harnesses::a_harness_outlives_a_server_crash_an_upgrade_and_a_clean_stop` |
| K3 | A keeper that dies takes its harness group with it before a replacement starts; one found dead at startup has its recorded group ended | `a_keeper_that_dies_takes_its_harness_with_it`, `a_keeper_found_dead_by_the_next_kernel_has_its_harness_ended` |
| K4 | A harness that dies is reported, and its keeper ends on stop | `a_harness_that_dies_is_reported_and_its_keeper_stops_on_drop` |
| K5 | A keeper speaking no shared protocol is refused at start and stopped on attach; an overlap is spoken at the highest shared version; an unknown op falls back to the contract default | `a_keeper_speaking_no_shared_protocol_is_refused_at_start_and_stopped_on_attach` |
| K6 | Half a request from a dying kernel is never acted on; a whole request whose answer was lost is carried out once | `a_request_cut_off_by_a_dying_kernel_is_never_acted_on`, `a_request_whose_answer_was_lost_is_answered_from_the_keeper_memory` |
| K7 | The process lock is held without a gap across a kernel restart, and released once every copy is gone | `a_detached_keeper_keeps_the_note_and_the_process_lock_for_the_next_kernel`; `anyagents-core` `a_shared_copy_holds_the_lock_only_until_the_owner_releases_it`, `a_retained_lock_stays_held_by_the_copy_a_keeper_keeps` |
| K8 | A worker takes over the process its last kernel left without a launch or a recovery note, resumes from the cursor, and keeps the process and lock when it stops with retention | `anyagents-supervisor` `custody::a_running_agent_takes_over_the_process_its_last_kernel_left`, `a_turn_in_a_process_taken_over_gets_no_recovery_note` |
| K9 | A process left for an agent that no longer runs is ended; one launched for another harness is switched at its boundary; the kept note restores usage accounting | `custody::a_process_left_for_an_agent_that_no_longer_runs_is_ended`, `a_process_launched_for_another_harness_is_switched_at_its_boundary`, `the_note_a_kernel_kept_restores_its_usage_accounting` |
| K10 | A newer kernel supersedes an older one without ending the harness | `a_second_kernel_supersedes_the_first_without_ending_the_harness` |

## Migration on the live desk

Each stage says what it interrupts.

1. **Deploy the keeper binary.** Restarting `serve` onto it is the last restart that interrupts work. The old binary's shutdown ends its in-process harnesses, as every restart has until now. The new kernel then starts each running agent's harness in a keeper, and each interrupted turn gets its `[Recovery]` note. Preconditions:
   - the pod's PID 1 reaps orphans (it is `sshd` today, which does), because keepers are reparented to it when a kernel exits;
   - `desk-services` keeps restarting `serve` by its own process group, which keepers are not in;
   - backups may skip `<data>/keepers/`, which holds only sockets, small records and keeper logs.
2. **Verify.** `anyagents keepers list` shows one keeper per running agent, and an `alive` harness. Restart `serve` (`desk-services restart anyagents`). Every harness pid is unchanged, no `[Recovery]` note is delivered, and mail sent before and after the restart is answered once. Record each keeper's resident memory. Interrupts nothing.
3. **Routine upgrades.** Replace the binary and restart `serve`; this interrupts nothing. Running keepers keep their adapter code until their process ends (idle release after each burst), and new processes run the new code. A protocol version change ships in a kernel that still speaks the previous version.
4. **Rollback to a binary without keepers.** First run `anyagents keepers stop --all`, which interrupts every running turn, as a restart did before keepers. Without that step, the old kernel finds each running agent's lock held and leaves the agent alone: no second process ever starts, and those agents stall until the keepers are stopped.
5. **Telegram connector** (next). It runs as its own `desk-services` service. At cutover each bot misses one poll cycle; nothing is lost, because offsets are saved only after storage.
6. **Workstation service**, then the **gateway service**. Each cutover restarts the browsers it moves: a page open at that moment is reloaded from its profile.

## Consequences

- A restart, crash or upgrade of the kernel no longer interrupts any agent's work.
- A kernel stops quickly: it no longer waits for the harnesses to end.
- An agent's harness costs one more process, and each delivery call is a local socket round trip instead of a function call. An idle agent makes no calls, so idle cost stays zero ([0006](0006-runtime-state-and-lifecycle.md) I1).
- Adapter code changes reach a running agent at its next process, not at the kernel's next restart.
- Rolling back to a binary without keepers needs one explicit command.
- Integration tests that kill a server must stop the keepers it left (`support::Keepers`).

## Alternatives rejected

- **Pass the pipe descriptors to the kernel (a keeper that holds only file descriptors).** A kernel that dies in the middle of a write leaves half a line in the harness's stdin. Bytes the dying kernel had read but not handled are lost. The adapter's session state is lost with it.
- **A keeper that journals both byte streams, with replay into a fresh adapter (tmux scrollback).** Every adapter would need deterministic replay of its own writes as well as the vendor's output. The cost grows with the length of a session, and timer-driven adapter state would still diverge.
- **One keeper per host.** One crash ends every harness, and upgrading it is the original problem.
- **Ask vendors to re-announce their session state on reconnect.** Possible for Spiron, impossible for third-party CLIs, and it still leaves the half-written line.
- **Keep harnesses as kernel children and make the kernel restart in place (`exec` itself, keeping descriptors).** A crash still ends everything, and an `exec` handover still needs every adapter to serialize its state.
