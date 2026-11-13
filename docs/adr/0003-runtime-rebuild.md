# 0003: Rebuild the runtime foundation around declared identity, managed binaries and single-writer state

Status: proposed. Date: 2026-11-12.

This is the umbrella decision. The detailed designs, with their invariants, tests and migrations, are:

- [0004: Identity and credentials](0004-identity-and-credentials.md)
- [0005: Managed harnesses and a declared toolchain](0005-managed-harnesses-and-declared-toolchain.md)
- [0006: Runtime state and lifecycle](0006-runtime-state-and-lifecycle.md)

## Context

AnyAgents has grown by fixing one failure at a time. Each fix was locally reasonable, and together they left three structural gaps. In every reliability failure we traced, the root cause was one of these gaps. The symptoms differ; the causes do not.

1. **Identity is discovered, not declared.** There is no credential store, only profile folders and a marker file per agent. Background routines copy credential files between profiles, agent directories and peer agents, and each one picks "newer", "default" or "first healthy" by its own rule. Tool identity (`gh`, `git`, `op`) comes from whatever `HOME` or environment the child process inherits. Results:
   - switching an agent's harness leaves the wrong token in place;
   - pausing an agent can rebind it to another account;
   - profile names stop matching the accounts inside them;
   - new agents land on a logged-out default;
   - commits are authored by whichever identity happened to be ambient;
   - abandoned sign-in folders show up as usable profiles.
2. **Binaries are guessed, not declared.** Harness CLIs and tools are found at the moment of use through `PATH`, `HOME`, `ANYAGENTS_*_BIN` overrides or hardcoded paths, with no version or integrity check. Results:
   - a tool found on one seat is missing on another;
   - harness versions drift between machines;
   - a vendor CLI updates itself in place;
   - a test helper can only be found by walking the build directory;
   - an external checkout provider can rewrite configuration shared by every seat.
3. **Agent state has no owner.** At least seven writers change an agent's state, from several thread families and from separate OS processes. "Busy" and "in a turn" are worked out separately, and differently, in each place. Results:
   - polling loops burn CPU while agents sit idle;
   - competing writer connections hit `database is locked`, and that error then ends the turn;
   - an orphaned process keeps holding a vendor session;
   - a stale steer record blocks startup until someone removes it by hand;
   - quota resets are lost;
   - scheduled ticks are dropped;
   - archive deletes work that was never pushed.

We stop patching these one at a time and rebuild each layer to a design whose invariants are tested.

## Decision

1. **AnyAgents owns identity (ADR 0004).**
   - One credential store holds typed records (`<kind>/<provider>/<name>`), and only that store's code writes to it. Every update is an atomic generation swap.
   - Agents reach records through typed bindings, one per slot: model provider, GitHub, git author, `op`, channel. Only an explicit, audited configure call changes a binding.
   - Credentials are written into a harness at launch, from the bindings alone. The launch first removes whatever earlier harnesses left behind. Nothing is discovered from `HOME`.
   - A missing or invalid credential stops the launch with a typed error that the operator can see. Nothing falls back to another identity.
   - This supersedes ADR 0002 decision 1 and the credential part of decision 3.
2. **AnyAgents owns harness binaries and declares every other tool (ADR 0005).**
   - Harness CLIs are managed. AnyAgents discovers the latest release, downloads and verifies it, installs it into versioned directories under `<data>`, smoke-tests it, switches to it atomically and rolls back if it fails. A running session keeps its version until its turn ends.
   - Every other tool is listed in a caller-owned manifest with an absolute path, an exact version and a sha256. The deployment image provides the tool and AnyAgents verifies it.
   - No code looks up a binary on `PATH`. Each child process gets a `PATH` that AnyAgents builds.
3. **Each agent's state has exactly one writer (ADR 0006).**
   - One actor per agent is the only writer of that agent's state, and it is driven by events, not polling.
   - The process has a single store writer connection.
   - The turn, session, steer and quota state machines are explicit and persisted, and they recover deterministically after a crash.
   - Session leases are tied to process identity. Each harness runs in its own process group, which is killed as a whole.
   - Spawn, pause, reset and archive are journaled commands. They reply only once their effects are complete, and they never delete anything that cannot be reproduced.
   - Spawn is atomic: either the agent fully exists, with its name, workdir, seat and bindings, or nothing does. A failed spawn never leaves a reserved name behind.
   - Role templates resolve from one catalog source. Agents keep no per-workdir copies that override it.
4. **One way in for users.**
   - `anyagents login <harness> [--account <name>] [--replace]` is the only sign-in command. The harness catalog maps each harness to its provider.
   - `anyagents login github` and `anyagents login op` cover tool identities. `anyagents credentials import` is the explicit one-time import of an existing credential. `sign-in <agent>` remains as a deprecated alias.
   - `anyagents doctor` proves the whole system live:
     - every credential record by a live probe;
     - every agent's bindings;
     - every declared tool against its manifest entry;
     - every managed harness against its install receipt;
     - tool identity from inside each seat.
5. **Interfaces between the three layers.**
   - Credentials and binaries meet only through `begin_login(harness, account) -> staging_dir`, then the managed launcher's sign-in command run with its config directory pointed at the staging directory, then `commit` or `abort` (atomic), and at launch `materialize(agent, harness) -> {config_dir, env}`.
   - The toolchain never reads credential bytes, and the credential store never resolves binaries.
   - All persistent schema, including `credential_bindings`, `harness_leases` and `session_leases`, belongs to the store layer, and the other layers use its typed API.
6. **Every stream removes the code path it replaces.** No fallbacks, compatibility branches or second paths are kept. Migrations are additive and preserve all existing data: profiles, OAuth tokens, cookies, browser profiles, sessions, open turns, steer records and schedules. Old data roots are renamed rather than deleted, and deleting them is a separate operator command.

## Delivery

Work runs as disjoint streams. Each set of files has exactly one writing stream. Another stream may change a file it does not own only by mechanically swapping a call site to a new API, and the owner reviews that change.

| Stream | Owns | Source |
|---|---|---|
| Store | `crates/anyagents-core/**` | 0006 (writer, schema); schema for 0004 and 0005 |
| Credentials | `crates/anyagents/src/credentials/**`, `create.rs`, `main.rs`, `signin.rs`, `channel.rs` | 0004 |
| Toolchain | `crates/anyagents-toolchain/**`, `browser.rs`, `computer*.rs`, `mcp.rs`, `worktree.rs`, `worktree_access.rs`, `sandbox.rs`, `schedule/github.rs`, `crates/anyagents-tools/**`, `clippy.toml`, `doctor.rs` | 0005; tool identity and doctor from 0004 |
| Install | `crates/anyagents-installer/**`, `install.rs` | 0005 |
| Harness boundary | `crates/anyagents-supervisor/**`, `crates/anyagents-agent/**`, `crates/anyagents-harness*/**`, `vendor.rs`, `spiron.rs`, `cloud_harness.rs`, `opencode_config.rs`, `muse_config.rs`, `mcp_config.rs` | 0006 (actor, process groups, typed events); 0004 (materialize); 0005 (managed launcher, leases, child `PATH`) |
| Lifecycle | `server.rs`, `schedule.rs`, `agent.rs`, and a new lifecycle module that takes reset out of `create.rs` | 0006 |

The streams are ordered in waves:

- **W0, now:**
  - the idle-cost fix (#223);
  - recursive, fail-closed workspace disposition on archive and idle release;
  - per-seat repositories (#213);
  - schedule tick coalescing, already in progress.
- **W1, in parallel:**
  - Store PR 1 (additive schema and typed operations) and PR 2 (the single-writer registry behind `Store::open`);
  - Credentials (store, resolution, `login`, migration);
  - Toolchain core (manifest, verification, removal of `PATH` lookups for non-harness callers);
  - Install (registry, receipts, upgrade lifecycle).
- **W2:**
  - Harness boundary, as three sequential PRs: (1) actor, state machines, process groups and typed events; (2) credential manifests and materialize; (3) managed launcher, version leases and child `PATH`;
  - tool identity and `doctor` in the Toolchain stream.
- **W3:** Lifecycle commands, the schedule timer heap, and removal of the last polling loops and text-matched errors.

The purity test starts with an allow-list of files that may still look up binaries. The list shrinks as each stream lands, and W3 deletes the last entries.

## Acceptance

- **Clean machines:** a clean Linux machine and a clean macOS host, with no harness CLIs installed, run `anyagents login` for every harness and then spawn an agent on every harness, with no manual environment setup. `anyagents doctor` passes on both.
- **Upgrades:** a forced upstream version bump is picked up automatically. A deliberately broken new version rolls back. A turn already running finishes on its original version.
- **24-hour fleet soak:**
  - zero `database is locked` errors;
  - zero dropped wakes or schedule ticks;
  - idle CPU near zero;
  - no orphaned harness processes;
  - no agent rebound without an explicit configure.
- **Tests:** every invariant listed in ADRs 0004–0006 has a test in CI on Linux and macOS.

## Consequences

- Operators manage one credential store and one toolchain manifest. Neither depends on `HOME`, `PATH`, environment overrides or folder names.
- Failures surface as typed, visible states (`credential_blocked`, `toolchain refused`, `faulted{kind}`). An agent does not quietly run as the wrong identity, on the wrong binary, or in an inconsistent state.
- AnyAgents takes on credential brokering and harness distribution, which it previously left to the environment. That scope is deliberate: every recent failure came from leaving those concerns implicit.
- Most of the code these layers replace is deleted, not wrapped.

## Alternatives rejected

- **Keep fixing symptoms.** Each fix added another heuristic writer or lookup, and the next failure came from two of them disagreeing.
- **Leave credentials and binaries to the environment (ADR 0002 as written).** Seat isolation already redirects `HOME`, so "the environment" differed by seat, harness and launch path. That made identity and binary resolution non-deterministic.
- **A single combined rewrite PR.** Too large to review and verify. Disjoint streams with one writer per file set give the same end state with reviewable steps.
