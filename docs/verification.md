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
