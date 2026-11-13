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
- starts in its own session (`setsid`), closes every descriptor it inherited except the lock, and keeps stdin and stdout on `/dev/null` and stderr on its own log, which it rotates at 1 MiB. A `killpg` of the kernel's group never reaches it, and it never holds a pipe or a file the kernel had open;
- serves one owning kernel at a time on `<data>/keepers/<instance>/s`. The directory must be this user's own (not a symlink, mode `0700`), the socket is `0600`, and a peer of another uid is answered `forbidden`. Only the current owner (the kernel whose `hello` took the keeper most recently) may do anything but `hello`; an observer or a superseded connection is answered `forbidden`. Every refusal is an answer, never a silent close, so a client cannot mistake a live keeper for a dead one;
- records its own identity and its harness's (pid, start time, boot id, pid namespace) in `keeper.json`, beside the agent and the lock path the kernel wrote before starting it;
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
- `stop` and the owning `hello` are frozen in every version. An owning `hello` takes the keeper over even when no version is shared (it is then answered `protocol`), so a kernel can always end a keeper it cannot otherwise speak to, and then starts a fresh one; that agent's work is interrupted once.
- **Deadlines.** The keeper's start (`ready`) has 180 s, after which the kernel ends the keeper and everything in the session it leads. `hello` has 10 s, and the keeper answers it from cached health without waiting on the adapter. Each call has 120 s (`quiet_for` its own patience on top). A call that passes its deadline marks the keeper wedged: no further call is sent, the harness reads as dead, and the replacement path stops the keeper over a fresh connection; a keeper whose adapter is held by a call that never returns ends the harness group from its recorded identity instead of through the adapter. An answer the kernel cannot parse is an error to the call that waits for it.
- `ANYAGENTS_KEEPER_PROTOCOL=<min>-<max>` pins the range a binary speaks, kernel and keepers alike, to exercise skew between two binaries in a test; nothing else reads it.
- **Skew.** A new kernel with old keepers (an upgrade), or an old kernel with new keepers (a rollback that still knows keepers), attaches at the highest shared version, or stops and relaunches when there is none. `anyagents keepers list` shows each keeper's protocol and binary, so an operator sees which processes still run older adapter code.

### 4. Delivery resumes from the cursor without loss or duplication

Two halves survive a kernel restart. The mailbox cursor survives because it is durable in the store; the adapter's memory survives because the keeper is still alive. The new kernel re-offers every message past the cursor, in sequence order, and the adapter answers each from what it holds:

- written and consumed: `Accepted` or `Duplicate`, so the cursor moves;
- written and still in flight: `InFlight`, and nothing is written again;
- not yet written: written now, once.

Two cases at the moment of the crash:

- A request the dying kernel was sending (a steer, a prompt) either reached the keeper as a whole line, or did not. A whole line is carried out even though nobody waits for its answer, and its re-offer is answered from memory. Bytes without a newline are dropped, so half a request never reaches the adapter, let alone the harness's stdin.
- A process taken over gets no `[Recovery]` note for a turn it is still running. The turn closes in the process that ran it, and the kernel records the completion when the adapter reports it. A recovery the last kernel still owed is not lost: whether one is owed is part of the note the kernel keeps with the process, and a process found idle while the store holds its turn open is owed one too.

Usage is recorded once. The kernel keeps its accounting for the process (served totals already recorded, turns recorded, lines written since, the profile marker it launched under, an owed recovery) in the keeper's note whenever it changes, before every wait. The next kernel restores it, so a turn already recorded is not recorded again. One window remains: a crash between committing a usage row and writing the note records that burst twice. Closing it needs an idempotency key on usage rows, which is the Store stream's schema.

A configuration change made while no kernel ran is a switch, applied at the process's next boundary like any other: the keeper reports the harness and model the process was launched for, and the worker compares them with the agent's row.

### 5. Failure isolation

- **The kernel crashes or stops.** Nothing the harness can see changes. When a kernel stops on a signal or an error, it detaches every keeper and keeps its own copies of the locks from unlocking. At startup the supervisor reconciles: a running agent's worker takes its keeper over, and the keeper of an agent that no longer runs is stopped.
- **A keeper dies** (an adapter panic, a kill). Its connection closes without `superseded`, and its process is gone (its exit status when this kernel started it, else its recorded identity). The kernel then ends the harness group the keeper recorded, as the seat when it runs in one, before anything starts a replacement. After that the normal path runs: backoff, a new keeper, resume of the vendor session, a `[Recovery]` note. If the keeper process is still alive, the kernel does not take it for dead: it stands down for that agent instead.
- **A keeper that does not answer** is gone only on proof. Only a socket that is missing or refuses (`ENOENT`, `ECONNREFUSED`) starts the question; a refusal, a closed connection or a missed deadline means the keeper may well be alive. Then its recorded process must not be running, and the agent's lock (which a live keeper holds for its whole life) must be free on a fresh descriptor. With that proof its directory is cleared and its harness group ended; without it nothing is touched, and the worker does not start another harness beside it.
- **No recorded pid is signalled on a bare number.** A pid is signalled only when the process behind it is the recorded one: same boot id, same pid namespace, same start time. A record from before a pod restart, a reboot or a restore names whatever process now has that number, so it signals nothing; its directory is cleared. A group whose leader has ended may still be signalled, because Linux does not reuse a pid while it numbers a live group. On a platform without `/proc` nothing recorded is ever signalled. A seat launcher named in a record is used only when it is a root- or server-owned file nobody else can write.
- **The harness dies.** The keeper stays and reports the process gone. The kernel stops that keeper and starts another, as before.
- **Two kernels** (the server lock already forbids it). The newer owner supersedes the older, and the older stands down for the agent: it neither ends the process nor starts another.
- **The gateway's port is taken across a restart** (by anything, a squatter included). The gateway binds another port, replaces every agent's gateway token, and the kernel replaces those agents' kept harnesses instead of taking them over, so none keeps sending its old token to the old port.

### 6. Resource accounting

Each keeper is a process with a pid, and its harness is a process group. `anyagents keepers list` prints both for every keeper, so CPU and memory are attributable per agent. The next step, not implemented, is a cgroup v2 leaf per keeper where the deployment delegates a cgroup subtree. That would give memory limits and an exact `GroupEmpty` event for [0006](0006-runtime-state-and-lifecycle.md).

### 7. Seats and uids

`spawn_command` starts a harness in its OS seat (`anyagents-setpriv --reuid … --regid … --groups … --no-new-privs`) and strips the server-only environment; signals to a seat's group go through the seat launcher as that uid. A keeper runs as the server's own uid and starts its harness through the same `spawn_command`, so the seat boundary is unchanged. It runs as the server's uid, not the seat's, because:

- it holds the agent's process lock under the server-owned `locks/`;
- its socket must not be reachable from the harness it holds;
- it is the one that signals the seat group.

The launch, which carries the harness environment including gateway tokens, reaches the keeper through its stdin pipe and is never written to disk.

Without seats (the single-user `local` mode of [0002](0002-runtime-drivers-and-capability-identity.md)) a harness runs as the server's own user. It can then reach its keeper's socket, as it can open the server's database: that mode is no boundary between a harness and the server, and keepers do not pretend to add one. Seats are the boundary.

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
| K10 | A newer kernel supersedes an older one without ending the harness; a superseded kernel stands down without ending it or starting another | `a_second_kernel_supersedes_the_first_without_ending_the_harness`; `custody::a_superseded_kernel_stands_down` |
| K11 | A stale record (another start time, another boot) never signals the process now at its pid | `a_stale_record_never_signals_the_process_that_now_has_its_pid`; `identity::a_live_process_is_itself_and_a_reused_pid_is_not` |
| K12 | A keeper is never taken for dead without proof: its process still runs, its lock is held, or its socket closes unanswered | `a_keeper_that_does_not_answer_is_never_treated_as_dead` |
| K13 | Every wait is bounded: a wedged adapter fails its call, still answers `hello`, and is stopped; a keeper that never gets ready is ended; an unreadable answer fails its call | `a_wedged_adapter_is_bounded_and_still_stopped`, `a_keeper_that_never_becomes_ready_is_ended`, `an_unreadable_answer_fails_the_call_that_waits_for_it` |
| K14 | Only the owner acts on a keeper; a keeper keeps no inherited descriptor; keepers live only in a private directory | `only_the_owner_may_act_on_a_keeper`, `a_keeper_closes_what_it_inherits`, `keepers_refuse_a_directory_that_is_not_private` |
| K15 | An owed recovery survives the kernel that owed it; usage is not recorded twice across a takeover; a process for an agent stopped meanwhile ends before its lock is released | `custody::an_owed_recovery_survives_the_kernel_that_owed_it`, `usage_a_previous_kernel_recorded_is_not_recorded_again`; `custody_tests::a_process_taken_over_for_a_stopped_agent_ends_before_its_lock_is_released` |
| K16 | Across protocol versions of the real binary, an overlap is taken over and no overlap is replaced, every message answered once; a real adapter (Spiron's) under a keeper writes no line twice across a crash and an upgrade | `server_harnesses::keepers_across_protocol_versions_are_taken_over_or_replaced`, `a_spiron_adapter_in_its_keeper_never_writes_a_line_twice_across_a_restart` |
| K17 | A gateway whose port was taken moves and replaces every token; an API-key agent's kept harness is then replaced, not taken over, and its turn answered once | `gateway::tests::the_gateway_keeps_its_port_and_rotates_tokens_when_it_cannot`, `server_harnesses::a_moved_gateway_replaces_the_kept_harnesses_of_api_key_agents` |
| K18 | `keepers stop` refuses while a server runs, and otherwise ends every harness and frees every lock | `server_harnesses::keepers_stop_only_when_no_server_runs` |

## Migration on the live desk

Each stage says what it interrupts.

1. **Deploy the keeper binary.** Restarting `serve` onto it is the last restart that interrupts work. The old binary's shutdown ends its in-process harnesses, as every restart has until now. The new kernel then starts each running agent's harness in a keeper, and each interrupted turn gets its `[Recovery]` note. Preconditions:
   - the pod's PID 1 reaps orphans (it is `sshd` today, which does), because keepers are reparented to it when a kernel exits;
   - `desk-services` keeps restarting `serve` by its own process group, which keepers are not in;
   - backups exclude `<data>/keepers/`. It holds only sockets, records and logs of processes that exist in one pod's lifetime; a restored record names nothing alive (it would only ever be cleared, never signalled, but it has no value to keep).
2. **Verify.** `anyagents keepers list` shows one keeper per running agent, and an `alive` harness. Restart `serve` (`desk-services restart anyagents`). Every harness pid is unchanged, no `[Recovery]` note is delivered, and mail sent before and after the restart is answered once. Record each keeper's resident memory. Interrupts nothing.
3. **Routine upgrades.** Replace the binary and restart `serve`; this interrupts nothing. Running keepers keep their adapter code until their process ends (idle release after each burst), and new processes run the new code. A protocol version change ships in a kernel that still speaks the previous version.
4. **Rollback to a binary without keepers.** In this order, as the server's own user:
   1. stop the server (`desk-services stop anyagents`, or pause its service so it is not restarted). Its keepers keep running;
   2. `anyagents --data <data> keepers stop --all`. It refuses while any server holds `<data>/server.lock`, and while run by another user than the owner of `<data>/keepers`; it holds the server lock while it works, ends every keeper and its harness (interrupting every running turn, as a restart did before keepers), checks that every agent lock it held is free, and exits non-zero if anything is left;
   3. install the old binary and start the server.

   Without step 2, the old kernel finds each running agent's lock held and leaves the agent alone: no second process ever starts, and those agents stall until the keepers are stopped.
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
