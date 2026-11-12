# Harness verification

Run the deterministic harness acceptance checks from a checkout:

```sh
bash scripts/verify-harnesses.sh
```

Prerequisites: Bash, a Unix host and the Rust version specified in
`Cargo.toml` (or newer). The first run downloads Cargo dependencies; no vendor
subscription, API key, installed vendor CLI or model call is required. The
script stops at the first failure and returns a nonzero exit status. Tests use
temporary data directories rather than your existing AnyAgents state.

## What a pass means

| Layer | Evidence |
| --- | --- |
| External Spiron protocol | `cargo test -p anyagents spiron_external --locked` launches a synthetic `ANYAGENTS_SPIRON_BIN` executable for `spiron` and the legacy selectors `sylphx`, `sylphx-agent`, `sylphx_agent` and `sylphx/default`. It checks canonical `spiron` agent records, MCP/workspace/skills/model/identity/state paths, correlated acceptance before mailbox release, busy steering, unrelated results, cancellation and wake/resume, usage and unchanged stored identity/mail/history/schedules/legacy files. Real Spiron authentication, model calls and legacy-state import are upstream integration requirements, not certified by this probe. |
| Native protocols | Each of Claude Code, Codex, OpenCode, agy, Muse and omp has an adapter `protocol` suite using scripted vendor processes. These exercise the native launch and wire contracts, turn completion, retries, and the adapter's steering or buffering behavior. |
| Shared transport | `anyagents-harness-stream` tests the shared stream/RPC machinery used by adapters, including dsh and grok. This is not a dedicated live dsh/grok compatibility check. |
| Server round trips | `server_harnesses` creates agents for all eight catalog harnesses and checks two successive correlated mailbox replies per harness through the real server, supervisor and MCP tool server. It substitutes a common contract double: passing does **not** prove native vendor protocol compatibility. |
| Native server integration | `server_opencode` additionally exercises the OpenCode adapter through a scripted `serve`/`api` CLI rather than the common contract double. |
| Reusable sign-in and API | `signin`, `mcp` and `identity` check persisted sign-in state, tools and capability identity with deterministic fixtures. They do not authenticate a real vendor subscription. |
| Durability and lifecycle | The invariants and supervisor suites check mailbox ordering, deduplication, retry/crash behavior and supervised agent lifecycle. |

For the complete repository gate (including gateway, channels and launcher):

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
npm test
```

## Synthetic agent identities

The shared server fixtures derive eight distinct nonzero synthetic agent UIDs
while excluding the executor's real UID. `operator()` and the explicit identity-
admission fixture use that same map; these numbers are fixture metadata, not
host-account provisioning. `server_harnesses` covers every candidate collision,
including real UID 2106, and runs the unchanged server against a deliberately
matching UID (refused before socket creation) and the collision-free fixture
(accepted). Both sides retain the same configured seat and launcher; protocol
round-trip fixtures separately use the seat-free `contract_server()` helper.
The strict product launch guard remains enabled; this startup check does not
claim a real cross-UID launch.

## Seat Git configuration

The required Linux managed-UID lane also exercises runtime-directory handoff.
It starts with daemon-owned `0700` cache/tmp directories and retained files;
the admitted seat must read and update those files and create children inside
nested cache directories. Foreign-owned descendants and peer symlink targets
retain their original permissions. Capability files are normalized to `0640`
for both daemon and seat ownership. Direct admission accepts `0600`, `0640` and
`0660` only for a readable regular non-symlink file in the exact admitted seat
group; outside-readable modes and a mismatched group refuse before execution.
The existing `verify-host-memory-seat.py` lane requires all of these receipts.

Seat Git configuration runs file-only edits in daemon-owned staging, with
`GIT_DIR=/dev/null` and inherited Git repository/parameter routing removed.
An absolute `git config --file` argument alone still allows Git to inspect the
caller's checkout. That checkout can be malformed or inaccessible after the
native test drops DAC override. The explicit non-repository metadata path also
supports data roots containing `:`, unlike Unix path-list discovery ceilings.

`cargo test -p anyagents --bin anyagents seat_git_configuration --locked` exercises
`ensure_seat_environment` from an unrelated malformed repository in isolated
subprocesses, including separate explicit Git routing controls and a colon-bearing
data root. It verifies exact trust for a synthetic non-repository workdir,
preserved caller settings, and refusal without replacement when the seat's own
configuration is invalid. The ordinary vendor suite does not execute the ignored
native-UID case: the Linux managed-UID lane must additionally verify that the
actual seat reads only its exact Git trust root alongside the existing memory,
skill and cross-seat isolation checks.

## OpenCode port-collision fixtures

The scripted child owns a competing socket before publishing its handoff barrier
by atomic rename, then deliberately attempts the conflicting bind. The transient
case releases that holder before acknowledging its bind failure and cannot exit
until the test acknowledges the release. Both cases require the witnessed bind
failure, startup retry and a different persisted port. The stored-port fixture
retains its socket throughout setup; restart checks accept a legal replacement
rather than assuming a released number stays available on a shared runner.
These cases run in the ordinary OpenCode `protocol` suite without product hooks
or an installed vendor CLI. The real-CLI case remains optional and ignored.

## Archived sub-agent cleanup

```sh
cargo test -p anyagents --bin anyagents archive_reap --locked
cargo test -p anyagents --test cli archive_reap --locked
```

The unit cases verify mail progress while cleanup is blocked in a git probe,
durable completion across restart,
wake/rearchive invalidation, failed cleanup retry, and preservation of shared,
external and unpushed work. Worktree admission also retains managed-config
provenance after the last reader retires and its private gitdir is removed,
without restoring retired access or accepting a replacement config link. The CLI cases execute the production binary with a
cleared environment and synthetic host home. They verify archive cleanup does
not recreate seat skill directories and real `serve` maintenance reaps a
legacy archive only once across restart. Unit-test isolation disables host-home
and maintenance paths, so unit tests alone do not cover that production branch.
These disposable fixtures do not certify adoption by an existing running daemon
or attribute a historical mailbox delay.

## Immediate stream receipts

The acknowledged-result fixtures wait for a ready frame processed by the real
stdout reader before arming the unchanged three-second post-write receipt bound.
Interpreter startup and imports have a separate setup deadline and failure
diagnostic. A deliberately slow startup exceeds the receipt bound but still
reaches the ordering assertion; an exited peer cannot satisfy readiness. These
checks retain busy-steer custody, typed rejection and no-replay assertions.

## Shared harness configuration

An admitted seat starts its program with `umask 0007`, after the UID/GID boundary
and without changing the daemon's process-wide mask. Config preparation grants
the admitted group read/write/search and setgid directories before any daemon
settings read, including existing vendor-created `0600` files. Umask cannot add
group rights to explicit `0600` replacements, so every launch repeats that repair.
The descriptor-bound walk refuses linked regular files and never follows symlinks;
capabilities remain `0640`, while unrelated host history and peer trees remain
private. The Linux managed-UID CI proof runs the actual AGY adapter twice with
restrictive settings replacements, verifies daemon writeback and retains peer
denial controls. Ordinary unit tests do not claim that privileged cross-UID proof.

## Sylphx transcript projection contract

Raw transcript history is append-only. Projection may prune or summarize context,
but every retained function output must follow its matching call, exactly once.
The stride window extends backward over any completed pair it would split,
including interleaved parallel calls. Context edits remove both sides of a pair,
including a result appended after the edit in the same batch. An interrupted
call may remain open; projection never executes it or invents its result. The
final request builder refuses malformed pairing before HTTP serialization.

A successful HTTP response without a valid Responses `output` array is a
separate response-envelope fault, not proof of an input-pairing defect. Its
error reports only envelope object/status/type metadata, not model text, tool
arguments, tool results or the full response body. Diagnosing a historical
missing-output fault still requires that response's envelope; input-window
fixtures alone cannot establish its cause.

## Boundaries and live acceptance

Scripted processes verify our implementation against fixtures, not whether a
new release of a vendor still implements those protocols. Before claiming a
particular dev machine is connected, separately check the installed version of
each selected vendor CLI, sign it in, create an agent with that harness, and
observe replies to two distinct mailbox messages. Check a mid-turn follow-up
where supported; adapters that buffer until the next turn must not be described
as supporting native mid-turn insertion. Keep credentials out of test logs.

Local execution runs as the host user; per-agent state directories and MCP
capabilities are **not** an OS security sandbox. Container/Pod/user isolation
requires a configured runtime boundary and a separate execution check on that
runtime. This script does not certify Docker, Kubernetes, cross-user isolation,
or live authentication. See [ADR 0002](adr/0002-runtime-drivers-and-capability-identity.md).

## Telegram batch checkpoints and replay

```sh
cargo test -p anyagents --test telegram --locked
```

Telegram ingestion commits mailbox rows before saving the batch's channel
checkpoint (`offset` and `chats`). A reader can therefore see the entire inbox
while the persisted offset still names the previous batch. The DM/topic
acceptance test waits for both the mailbox rows and the completed checkpoint
before asserting channel state; it does not assume those separate commits are
atomic. A disposable two-second pause immediately before the checkpoint write
reproduced CI run 42335689725's `offset = 0` versus `12` failure in the original
test. No pause or test retry belongs in the shipped ingestion path.

`a_restart_before_the_offset_checkpoint_replays_without_duplicate_mail` uses a
fixture-only SQLite trigger to reject the offset write while allowing inbox
commits. It kills the server with two messages stored and offset zero, removes
the trigger, and starts a fresh server with the same Telegram updates. The
checkpoint must advance to 12 without changing the two mailbox IDs or provider
origins. This verifies replay after an interrupted checkpoint without relying
on a sleep to hit the crash window.

## Desktop sessions (browser and computer)

`tools.browser` and session `tools.computer` share one desktop per profile: a
headless Wayland compositor (sway) running one headful Chromium-family browser,
with wayvnc behind the full-desktop noVNC takeover page. The
[`agent-workstation`](https://github.com/SylphxAI/agent-workstation) crate owns
the desktop, browser, takeover and input; AnyAgents owns caller policy
(profiles, secret references, snapshot ids, idle reaping, takeover delivery).

`workstation::headful::existing_profile_opens_with_logins_and_every_operation_works`
drives the tool surface end to end. It first writes the profile the way the
previous implementation did (a plain Chromium process on `user-data/`, plus its
`runtime.json`), then requires that the new session opens the same directory
with the cookie and local storage intact and retires the old bookkeeping. It
fills a `secret://` reference and checks the exact value reaches the form
without appearing in any response, rejects stale refs, takes a full-desktop
screenshot and zoom through `tools.computer`, types into the address bar with
the desktop keyboard, serves the takeover page behind its token, refuses agent
calls with `HUMAN_IN_CONTROL` until the person hands back, opens and closes
tabs, and reopens the profile after `close` with its login.

It runs only with `ANYAGENTS_DESKTOP_HEADFUL=1` and needs `sway`, `wayvnc` and a
Chromium-family browser. CI runs it on every Linux job:

```sh
ANYAGENTS_DESKTOP_HEADFUL=1 ANYAGENTS_CHROMIUM_BIN=/usr/bin/google-chrome \
  cargo test -p anyagents --bin anyagents --locked workstation::headful -- --nocapture
```

`ANYAGENTS_LEGACY_CHROMIUM_BIN` writes the legacy profile with a different
browser than the session uses, e.g. a newer distribution Chromium against an
older fingerprint build, which checks that a version downgrade keeps logins.

## Host memory under a managed UID

The active-writer, discovery-retry, busy-global-lock and daemon-launch Rust
fixtures run once in fresh single-test subprocesses before creating their
disposable lock files.
Unrelated parallel tests can fork while a fixture holds a flock; closing the
parent descriptor does not release an inherited open-file-description lock
until that fork executes. Isolation preserves immediate-reacquisition assertions
without treating a permitted `WouldBlock` as a product failure. The active-writer
case exposed the missing isolation in macOS jobs `128496005441` (main run
`42403446504`) and `128498340368` (PR #129 run `42404211626`). The daemon-launch
case failed at its second launch in macOS job `128500482068` (PR #122 run
`42404927262`). These four cover every same-file lock reacquisition in the six
`host_memory::tests` fixtures; the other two acquire the migration lock only once.
Child exit status and exactly one passed test are required; no assertion,
production lock behaviour or retry policy is changed.

The required public Linux CI lane builds the shipping `anyagents` binary and
runs `scripts/verify-host-memory-seat.py` with native `/usr/bin/setpriv`. The
entrypoints are `anyagents --data <synthetic-data> agent-run --config
<own-config> -- <probe>` and the real `vendor_launch` adapter path with a scripted
vendor child. Both use the same shared harness admission primitive that also
starts native workspace commands; a refused probe never returns a command to
its caller. No test launcher substitutes for those checks. The agent's actual UID 24031 and primary GID 24032 are asserted
independently; supplementary socket group 24033 is not a primary-GID proxy.

The CLI fixture proves learned-memory read/write and
readback, discovery/read through all three skill projections, writable own skill
overlay, and denied global/peer/host-private reads or writes as appropriate.
Missing memory, missing overlay, writable peer memory/skills/config and writable
global skills each refuse admission before the probe starts. The agent parent
remains traverse-only (`0711`) and the probe asserts it cannot be listed by the
seat. Production admission enumerates the complete peer inventory as the server
before dropping UID, then checks exact shell-quoted paths; seat-side glob
expansion is not accepted as evidence that no accessible peer exists. Inventory
errors refuse admission, except confirmed absence for a config-less boundary
that has no agent layout. Each admission check runs as a standalone subshell,
so a false left operand of `&&` cannot be hidden by a later passing check under
shell `errexit`. Protected bytes are hashed before and after. The receipt records source/binary/script/launcher hashes, each case's
exit and output, and confirmed fixture removal; CI prints it even after failure.
A native-setpriv preflight checks every fixture ancestor's traversal permission
with the actual UID/GID before admission. Production admission failures report
the static failed check label without command output or capability contents;
this separates fixture setup failures from a specific boundary refusal. Existing
ancestor permissions are never changed to make a fixture pass. The Linux CI
fixture uses a newly created leaf under `/var/tmp`: the runner's private home
ancestor makes its ordinary `RUNNER_TEMP` inaccessible to the distinct UID.
The receipt remains in `RUNNER_TEMP`, read only by the invoking CI process.
No account, ACL, real agent directory or daemon is changed.

The legacy first-party harness refreshed the embedding host's registered
peer principal inventory before every local workspace command. A child or sibling
created mid-turn in the same seat is admitted by its resolved UID, GID and exact
agent root, not directory group or mode. The unit regression creates a `02770`
child after retaining a running turn's environment, proves the stale snapshot
refuses, and exercises current-authority admission, wrong UID/GID refusal,
unregistered-peer refusal and failed-refresh refusal without a file write. Its
launcher mocks process-identity/procfs/stat reporting only; actual peer access
checks run on fixture directories. Real `setpriv` enforcement remains covered by
the managed-UID acceptance below.

Host-memory migration fixtures that release and immediately reacquire `flock`
locks run in single-test subprocesses with fixture directories created only after
`exec`. This isolates active-append, stopped-owner retry and daemon-launch checks
from concurrent test threads' forked descriptors on macOS without retrying or
weakening any lock assertion.


A required isolated Rust test invokes `vendor_launch` through the real Claude
adapter and native setpriv, not the CLI route. It removes the executor's DAC
override/read-search capabilities while retaining UID/GID transitions, mounts
skills under daemon umask `077`, and requires reads through the actual
`.claude`, `.agents` and `.agy` projections. A seat-owned `0700` vendor parent
must receive its projection through the narrow owner fallback without widening
that parent. It also checks legacy SQLite ownership handoff with exact bytes,
a second registered agent on the same UID/GID, writable foreign-seat
memory/skills/config refusals before vendor execution, search-only (`0711`)
foreign-tree refusal despite a known readable file, protected bytes/modes,
and fixture removal. The same required test exercises the real Muse workspace
session initializer with a fresh marker: each writable peer tree refuses before
any vendor process starts, while a valid boundary admits one initializer and
reuses its saved marker. It also launches the real Codex adapter with a preserved
foreign `.claude` parent and with a foreign `.claude/skills` mount. Personal bytes
remain unchanged and the active `.agents` skills remain readable. Admission
checks all three folders, not just the active harness. It honors intentionally
preserved personal projections outside foreign managed-seat roots rather than
requiring unrelated host names through them. Symlinked parents and mounts are
resolved under the admitted identity and refuse foreign managed-seat roots or
unresolvable targets; ordinary host-managed views still must read.
Same-seat exceptions require the host-resolved principal
and exact registered root, not merely ownership of a directory. Local unprivileged
tests separately exercise deterministic directory-to-symlink replacement during
descriptor-bound permission sharing. Passing CLI controls alone does not certify
this vendor proof or real vendor subscriptions.

A non-root local invocation deliberately refuses before creating a fixture. It
is not a skipped pass or actual alternate-UID evidence: acceptance requires the
required Linux job's receipt for the tested head. The CI invocation is documented
in `.github/workflows/ci.yml`; it never falls back when privilege is missing.

### Cross-seat worktree permission boundary

The Linux CI `Cross-seat worktree writes` step runs
`create::tests::privileged_cross_uid_spawn_can_create_commit_and_status` with
`ANYAGENTS_REQUIRE_PRIVILEGED_WORKTREE_TEST=1`. It creates three real OS users:
A provisions B, B commits, A still writes B-created Git content and commits using
an explicit protected hooks path. B cannot write common config/hooks/info,
write or unlink/replace its `config.worktree`, or alter `commondir`; C can use
its own checkout but cannot read B's HEAD/index, nor can B read C's metadata.
B's ordinary status, commit and local bare-remote push use its generated seat
config, with no `-c user.*` or author/committer environment overrides; the commit
records the exact caller-configured author and committer. A's common config retains its owner,
inode, content and owner read access during adoption. Parent config, remote-add
and `git push -u` each replace the target inode while retaining the managed link;
both active child UIDs must immediately read common config and run status, with
no launch, wake or inspection call between the write and probe. They still cannot
write/unlink config or create its lock, access sibling private metadata or write
hooks/info; an unrecorded UID cannot read config. Default ACL readbacks remain
confined to the dedicated config directory, and survivor/last-reader revocation
is tested across another parent rewrite. Adoption regressions cover an existing
Git config lock, interrupted publication with original bytes retained, and
replacement of the journal-pinned directory or link. A same-size clean-filter positive control
executes as B, then a forged `.git` pointer cannot execute that filter during
journal-pinned server cleanup or A/C seat commands in the foreign checkout.
A second payload selects a trusted common-config driver through B's checkout
and staged attributes after archive revocation. Cleanup must execute the driver
only as B; a B-readable execution receipt records that UID while a root-only
marker remains absent. An inspection worker is SIGKILLed mid-check without unwinding;
the durable open intent and read-only ACL are observed before startup recovery
removes the grant, reads it back and preserves the dirty checkout on a fresh check.
The temporary grant cannot write parent-owned common config or create objects.
A non-privileged counterpart requires pinned status to execute successfully,
checks the marker stays absent and rejects included or writable metadata config.
A cleanup regression snapshots every checkout and private-registration file and
requires byte-for-byte retention for an unpushed HEAD and for another unpushed
local branch beside a published detached HEAD; publishing both permits removal.
The same snapshot case removes or breaks the journaled checkout's `.git` pointer
with an unpushed HEAD and a report under `target/`: every checkout and private
registration byte must survive, without falling back to non-Git deletion.
Archive immediately revokes B's common-store access while preserving its external
checkout bytes; reap is idempotent and C remains admitted. A separate post-sharing
admission failure verifies rollback revocation. ACL readbacks cover every changed
object. A missing privilege is an explicit local skip, never the CI acceptance:
the required environment flag makes a privilege skip fail.

Three Linux recovery regressions plant a FIFO, a hard link or an outside-target
symlink during a durably open inspection. Each covers legacy and exact-object
journals through both primary and admin HTTP servers: the affected worktree is
quarantined, wake/reap/automatic harness launch refuse it, remaining targets and
an independent interrupted inspection are revoked, an active same-common seat's
ACLs remain unchanged, and outside-link contents and ACLs are preserved.

Shared refs are intentionally writable within one repository. These checks do not
claim branch-tip integrity for arbitrary agent shell commits or pushes.

## Scheduler isolation and clean-host server fixtures

The local scheduler owns an immediate startup pass and an independent one-second
loop for `at`, `every`, `file:`, `pid:` and `agent:idle:` schedules. Network-backed
GitHub and Spiron watches run on a separate loop; a pending external response must
not hold a later local tick. `local_scheduler_keeps_ticking_while_external_probe_is_blocked`
starts the production loops, holds a real HTTP response, then creates all five
local schedule kinds and verifies delivery before releasing that response. A
future timer and the pending external watch remain undelivered.

`overdue_at_delivers_on_first_local_tick_after_restart` reopens the database and
checks first-pass delivery of a one-day-overdue timer without duplicate delivery.
`overdue_at_records_busy_and_paused_deferrals_until_fired` verifies durable
`deferred:busy` and `deferred:paused` observations while preserving the due time,
then checks the transition to `fired` after the owner becomes eligible.

Readiness and sign-in server fixtures explicitly clear all six seat/principal
environment selectors and assert single-UID admission. They exercise token
routing without depending on a workstation's `anyagents-setpriv` installation.
Each complete server fixture runs in its own test process. Protected-root
admission canonicalizes configured aliases before comparing them with the target;
its regression covers the selected tree, a symlink spelling, a descendant and an
allowed sibling on Linux and macOS.

## Integration invariants

Seat admission inventories every foreign root, including unregistered roots and
roots beyond the first 32 peers. Sharing a primary GID does not make two different
UIDs the same principal. Missing optional peer subtrees are permitted; readable
foreign config, memory or skills refuse admission. A real authenticated daemon
socket remains usable, but a regular file at that path receives no exception.

History visibility is applied before candidate limits for every authorized-set
size. Host refresh holds lifecycle locks through publication and readback, includes
archived authorized history, and propagates malformed or unreadable source errors
in production as well as tests. Corpus size never disables mailbox backfill.

Unified code results expose a top-level delivery list collected by host dispatch.
Snippet return values, source text and console logs cannot populate it. Native
reply settlement requires an acknowledged message ID and the exact retained
recipient, including when sends share a parallel tool batch. Updated memory topic
files do not rewrite an existing host-session snapshot; a new explicit session
pins the next snapshot.

The regression checks are included in the normal workspace tests:

- Seat admission: more than 40 foreign roots, a readable unregistered tail root,
  mismatched UID/GID pairs and a registered exact-principal positive control.
- Scoped history: 18 authorized IDs competing with 2,010 hidden candidates.
- Code delivery: invented return-value receipts and a caught malformed send
  yield no host receipt; a real loopback send yields the returned message ID
  even when the snippet returns null.

## MCP server-socket fixture quarantine

`mcp_tools_use_the_server_socket` is quarantined after push CI 43264718226 on
`5c7e890a` failed macos on the same tree that was green in merge-group CI
43263904677 and PR CI 43262659260. The failure was an MCP tool-call
`io error: No such file or directory (os error 2)` at `mcp.rs:256`.
The pre-quarantine `sun_path` check is under the limit: the server bound
successfully, so its `<tempdir>/anyagents.sock` stayed within the macOS limit
(and the test asserts `<=107` bytes; Linux measured 31 bytes). Local
reproduction on Linux passed 4/4 on `5c7e890a`. Tracking:
https://github.com/SylphxAI/anyagents/issues/216. Run it explicitly when
changing MCP or server-socket paths:

```sh
cargo test -p anyagents --test mcp --locked \
  mcp_tools_use_the_server_socket -- --ignored --exact --nocapture
```
