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
external and unpushed work. The CLI cases execute the production binary with a
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
and an opt-in phone layout. Touch pinches zoom locally; dragging pans when
zoomed and scrolls the remote page at fit. Mouse dragging continues to operate
the remote page. Phone layout resizes only the browser window, leaving its
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

Desktop takeover accepts native modifier chords, including select-all, copy,
cut, paste and undo. Paste inserts local clipboard text into the remote focus;
copy and cut export the remote selection only after checking that the focus is
not a password field. The viewer keeps new-tab, close-tab and address-bar
shortcuts local. The floating pill also exposes Paste, Copy, Select all, Tab and Esc;
clipboard permission refusal displays a prompt instead of silently losing input.
On touch devices the native keyboard input overlays the remote field and tracks
its position through zoom and pan, so the device's Paste menu works in place.
No typed or pasted text is returned in input acknowledgements.

`takeover_clipboard_ui_acceptance` runs the Chromium viewer clipboard and
keyboard regressions in the ordinary native suite when Chromium is available.
The 1440×900 and 390×844 cases cover native and synthetic paste (one insertion),
copy/cut, undo, Tab/Shift+Tab, aligned touch input, permission refusal and password
export denial. Viewer teardown retries only recursive deletion of its disposable
profile (five retries, 100 ms linear backoff), because Chromium helpers can finish
profile writes after the browser parent exits. An exhausted cleanup retry still
fails the fixture; interaction assertions are never retried. This addresses the
Linux `ENOTEMPTY` cleanup failure in CI run 42334433646 after all clipboard
assertions passed.

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
entrypoint is `anyagents --data <synthetic-data> agent-run --config <own-config>
-- <probe>`: production `sandbox::run` executes its memory/skills and identity
admission checks before launching the probe. No test launcher substitutes for
those checks. The agent's actual UID 24031 and primary GID 24032 are asserted
independently; supplementary socket group 24033 is not a primary-GID proxy.

The disposable, invocation-owned fixture proves learned-memory read/write and
readback, discovery/read through all three skill projections, writable own skill
overlay, and denied global/peer/host-private reads or writes as appropriate.
Missing memory, missing overlay, writable peer memory and writable global skills
each refuse admission before the probe starts. Protected bytes are hashed before
and after. The receipt records source/binary/script/launcher hashes, each case's
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

A non-root local invocation deliberately refuses before creating a fixture. It
is not a skipped pass or actual alternate-UID evidence: acceptance requires the
required Linux job's receipt for the tested head. The CI invocation is documented
in `.github/workflows/ci.yml`; it never falls back when privilege is missing.
