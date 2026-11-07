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
| Sylphx request projection | `cargo test -p anyagents-harness-sylphx --locked` checks the native Responses request builder, pair-aware stride boundaries, context edits within tool batches, legacy transcript entries and response-envelope diagnostics without a model call. It verifies projected input and unchanged raw transcript bytes; it does not certify a deployed binary or recover a live agent. |
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

## Seat Git configuration

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
nonblocking descendant reconciliation, durable completion across restart,
wake/rearchive invalidation, failed cleanup retry, and preservation of shared,
external and unpushed work. Worktree admission also retains managed-config
provenance after the last reader retires and its private gitdir is removed,
without restoring retired access or accepting a replacement config link. The CLI cases execute the production binary with a
cleared environment and synthetic host home. They verify archive cleanup does
not recreate seat skill directories and real `serve-admin` maintenance reaps a
legacy archive only once across restart. Unit-test isolation disables host-home
and maintenance paths, so unit tests alone do not cover that production branch.
These disposable fixtures do not certify adoption by an existing running daemon
or attribute a historical mailbox delay.

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

## Browser takeover on phones

Takeover starts with the original desktop page, fitted to a full-viewport canvas.
The floating control pill expands to reveal typing, Enter, Backspace, reset/1:1,
and an opt-in phone layout. A single finger clicks or press-drags the remote page, including sliders.
Two fingers scroll the remote page or pinch to zoom locally. The explicit
“Touch: pan view” mode moves the zoomed local view without interacting with
the page. Mouse press-drag and wheel operate the remote page. Phone layout resizes only the browser window, leaving its
User-Agent, Client Hints, screen and DPR unchanged. The original window bounds
and state are restored on Done, agent release, expiry, navigation away, or a
lost viewer heartbeat (more than 10 seconds). The viewer polls state 800 ms
after each response. A lost heartbeat restores phone layout only; it does not
return control to the agent. Human control ends on Done/release, the absolute
takeover deadline (10 minutes by default), or a dead takeover listener. Viewer
heartbeats do not extend that deadline.

Takeover state is published by atomic replacement so concurrent listener reads
see a complete snapshot, never a truncated JSON file mistaken for an ended
session. The publication regression holds a reader open across replacement;
the viewport regression advances the reaper's explicit clock without writing
expired deadlines or stale heartbeats that race its background loop. Neither
regression needs timing sleeps or retries.

Takeover routes keyboard shortcuts globally, including when the canvas,
page background, stage or a HUD button has focus. Only local HUD text fields
keep their own typing and clipboard. Both Control and Command support
select-all, copy, cut, paste, undo and redo (Shift+Z or Control+Y). The remote
host maps a Mac viewer’s Command to Control on Linux/Windows. A native paste
event and a clipboard-read fallback share a single-insertion guard. Clipboard
permission refusal remains visible; failed inputs are never automatically replayed. Paste inserts local clipboard text into the remote focus;
copy and cut export the remote selection only after checking that the focus is
not a password field. The viewer keeps new-tab, close-tab and address-bar
shortcuts local. The floating pill also exposes Paste, Copy, Select all, Tab and Esc;
clipboard permission refusal displays a prompt instead of silently losing input.
On touch devices the native keyboard input overlays the remote field and tracks
its position through zoom and pan, so the device's Paste menu works in place.
No typed or pasted text is returned in input acknowledgements.

The server gives input a persistent CDP session independent of screenshots and
state inspection. At most 32 concurrent HTTP clients are admitted. Frame metadata
uses short locks; viewport/heartbeat/completion updates share a separate control
lock that input never waits on. Unsent mouse moves are coalesced without crossing
press/release/key boundaries, and coalesced wheels preserve total scroll distance.
The viewer polls JPEGs 32 ms after each completed frame and automatically retries
frame/state reads on the same URL after disconnection. Each fetch has a 5-second
deadline. Input failures report connection loss rather than replaying a possibly
committed action.

`tests/browser-takeover-interaction.mjs` drives the actual Rust HTTP server with
trusted Chromium input at 1440×900, 834×1194 and 390×844. It covers global focus
owners, native/fallback single paste, password paste, clipboard/undo/redo, IME,
local HUD isolation, sign-in clicks, desktop/touch slider drag, mouse/two-finger
scroll, same-URL reconnect and 20 input-to-painted-JPEG latency samples with a
150 ms maximum. Emulated touch is not a physical iPhone Safari receipt.
`npm test` checks bounded move/wheel queues and no replay on input failure. The
resource-free Rust regression holds an incomplete frame HTTP request open while
verifying a separate input request completes, distinguishing the old serial
listener. A manual `ci` dispatch with `takeover=true` executes the full ignored
Chromium fixture and uploads `takeover-evidence`; it does not remove the existing
startup quarantine from ordinary PR/merge-group runs.

`takeover_clipboard_ui_acceptance` covers the Chromium viewer clipboard and
keyboard regressions. It is temporarily quarantined by #118: Linux jobs
128295856130 (PR #114, run 42336924651) and 128298493004 (PR #115, run
42337744143) failed before the driver ran because the CDP port did not open
within the existing 12-second startup budget. Their discarded Chromium logs
cannot distinguish delayed startup from a crash. The startup path now waits
for a valid `/json/version` response under that same 12-second ceiling and
reports the last readiness failure, original child exit status, cleanup status,
and the final 8192 bytes of Chromium stdout/stderr. Deterministic controls cover
a TCP listener with incomplete/error responses before readiness, early exit,
stalled HTTP, missing logs and non-UTF-8 log tails. These controls improve
readiness and diagnosis; they do not establish the cause of those CI failures.
Keep the quarantine until captured failure evidence supports a causal repair.

The ordinary real-Chromium Rust fixtures share one in-process scenario guard,
held through teardown; resource-free startup controls still run concurrently.
This bounds browser-launch contention without changing the 12-second readiness
ceiling, skipping assertions or retrying interactions. PR #129 run
`42402630072`, Linux job `128493544529`, reported simultaneous startup timeouts
in the iframe, held-key, CAPTCHA and viewport fixtures. Each browser was still
alive before cleanup, with connection-refused as its last CDP readiness result.
The same candidate tree passed the workspace and full Chromium takeover fixture
in dispatch `42402639225`. This shows a nondeterministic startup failure, not its
underlying cause; the guard limits fixture contention and does not justify
removing the existing clipboard or OOPIF quarantine. Manual `ci` dispatch with
`takeover=true` also runs those two ignored fixtures explicitly after the full
mobile acceptance; any failed fixture fails the job without a retry.

Run the fixture explicitly for takeover changes:

```sh
cargo test -p anyagents --bin anyagents --locked \
  browser::tests::takeover_clipboard_ui_acceptance -- --ignored --exact --nocapture
```

The 1440×900 and 390×844 cases cover native and synthetic paste (one insertion),
copy/cut, undo, Tab/Shift+Tab, aligned touch input, permission refusal and password
export denial. The viewer starts in a dedicated process group. Teardown signals
the whole group, waits up to 5 seconds for graceful exit, then uses SIGKILL and
waits up to 5 more seconds if necessary. It removes the disposable profile only
after no non-zombie group members remain; zombies cannot write the profile. A
timeout fails cleanup and preserves the profile. There are no deletion or
interaction retries. This replaces the deletion-retry workaround for the Linux
`ENOTEMPTY` failure in CI run 42334433646, where Chromium helpers could still
write after the browser parent exited. `npm test` includes deterministic live-
descendant and graceful-exit controls; both Linux and macOS CI execute them.

Actual iPhone Safari keyboard and long-press menus require a
post-deployment device check; Chromium emulation does not certify those OS menus.

Frame metadata supplies the remote CSS viewport alongside the actual JPEG size
and reported DPR. A screenshot captured by a fresh CDP session can be 1920×1080
while the document reports DPR 2; dividing image coordinates by DPR alone is
incorrect. Input uses the measured CSS extent and the current zoom/pan transform.

Run the focused checks without an existing browser profile or external account:

```sh
npm test
cargo test -p anyagents --bin anyagents browser::tests:: --locked
```

The geometry suite covers DPR 1/2/3, zoom anchors and pan limits. The native
browser tests exercise trusted input, byte-identical browser identity, restored
metrics on every exit, and expiry before a queued toggle or Done. They require
Chromium; the existing browser suites skip when it is unavailable.

`takeover_focus_finds_cross_origin_input` is separately quarantined for two
intermittent Chromium failures: a CDP startup timeout and a post-startup
`focus.editable = false` assertion. On 2026-11-07, the latter also reproduced in
the unmodified `52c4432b362ac548434ab30cead122c5e2e244ed` test binary, before
the clipboard changes, using the same Chromium 156.0.8078.47. A bounded cohort
restricted eight concurrent fixture processes to one CPU to expose the race;
this is reproduction evidence, not a retry policy or a supported latency bound.
Failure-only diagnostics on both revisions found a focused parent iframe but
an unfocused child `BODY`, with the input present and both documents complete.
The cause remains unresolved; capturing a screenshot before the click did not
remove it. The focus detector and click dispatch were unchanged by the clipboard
feature. Run this test explicitly when reviewing takeover changes, so its
quarantine cannot hide a new regression:

```sh
cargo test -p anyagents --bin anyagents --locked \
  browser::tests::takeover_focus_finds_cross_origin_input -- --ignored --exact --nocapture
```

A green ordinary suite does not establish this ignored test passed. Its focused
assertions still require the first click to find the child input at offset
(60, 80); there is no interaction retry. Remove the quarantine only after both
failure modes have deterministic fixes.

The full UI acceptance test additionally requires Node.js 22 or newer (native
WebSocket support). It launches its own headless Chromium viewer and remote test
page, records trusted remote click coordinates within 2 CSS pixels, and saves
collapsed, expanded, zoomed, phone-layout and keyboard captures at 390×844,
588×1280, 1280×800 and 1440×900. It also checks pagehide restoration and visible
reconnection/recovery:

```sh
TAKEOVER_EVIDENCE_DIR="$(mktemp -d)" \
  cargo test -p anyagents --bin anyagents \
  browser::tests::mobile_takeover_ui_acceptance --locked -- --ignored --nocapture
```

Set `TAKEOVER_GOOGLE_ACCEPTANCE=1` for the additional network-dependent,
unauthenticated `accounts.google.com` phone-layout captures. No credentials are
entered. This is separate from the deterministic local fixture and does not
certify Google sign-in, a physical phone's keyboard, or deployed release bytes.

## Host memory under a managed UID

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
records the exact Kyle author and committer. A's common config retains its owner,
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
