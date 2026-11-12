# AnyAgents architecture

Status: design. Decisions behind it are in [adr/0001-server-first-one-binary.md](adr/0001-server-first-one-binary.md) and [adr/0002-runtime-drivers-and-capability-identity.md](adr/0002-runtime-drivers-and-capability-identity.md).

## The standard, and the shape

The established pattern for running vendor agents is a long-lived gateway that drives the vendor's own tool and injects input into the live run. OpenClaw names four ways to handle input that arrives during a run: steer it into the active turn, queue it as a followup for when the turn ends, interrupt the run, or collect several followups into one ([command queue](https://docs.openclaw.ai/concepts/queue)). AnyAgents follows that pattern and adds what a gateway alone does not give: a durable per-agent mailbox, harness setup and sign-in, agent-to-agent messaging, and usage accounting.

```
 clients:   CLI    MCP (one per agent)    TUI / GUI (later)    channels (Telegram, ...)
              \           |                     |                     /
               +----------+---------- API ------+--------------------+
                                       |
                                 anyagents serve
     +-------------+-------------+-------------+-------------+-------------+
     |  harnesses  |   agents    |   mailbox   |  channels   |    usage    |
     |  install,   |  create,    |  per-agent  |  adapters,  |  per-turn   |
     |  update,    |  supervise, |  log, seq,  |  relay      |  records,   |
     |  sign-in    |  adapters   |  three acks |             |  aggregates |
     +-------------+-------------+-------------+-------------+-------------+
                                       |
                    vendor processes: claude, codex, opencode, gemini, ...
```

Everything runs in one process, `anyagents serve`, with one embedded database. Each module below is one crate with one public interface; the protocol gateway is a sixth module used only in API-key mode.

## Three-axis vendor-neutral model (`Harness × Profile × Sandbox`)

AnyAgents never runs an internal LLM loop. Every agent is a pure composition of three orthogonal axes:

- **Harness (`anyagents-harness`, `spiron`, `cloud_harness`):** Which external single-agent loop executes turns (`spiron` CLI / `spiron-cloud` at `https://spiron.ai`, `claude-code` / `claude-cloud`, `codex` / `openai-cloud`, `opencode`, `agy`, `muse`, `omp`, `dsh`, `grok`, `cursor-cloud`).
- **Profile (`profiles/<provider>/<profile>`):** Which upstream subscription or API key authenticates (`anthropic`, `openai`, `google-antigravity`, `muse`, `openrouter`, `sylphx`, Coding Plans). Profile-to-harness resolution is strictly **fail-closed**: if a bound profile has no mapping for the target harness (for example, binding an OAuth-only subscription profile to `opencode` without an OpenCode provider/auth/key mapping), AnyAgents refuses configuration rather than writing an empty config that silently degrades to an external free tier.
- **Sandbox (`anyagents_tools::WorkspaceBackend` trait):** Where filesystem and shell operations execute—`LocalWorkspace` (default host directory or git worktree), `RemoteSandboxWorkspace` (Cloud Sandbox / E2B HTTP APIs), `DockerWorkspace` (`docker://<host>`), or custom trait implementations.

System prompt rendering (`write_agent_instructions_with_continuity`) injects a minimal, persona-neutral identity and routing envelope (`arXiv:2611.05306`, `arXiv:2611.03931`) so each sub-agent starts with an unpolluted short context window while tool schemas carry API parameter details.

## AnyAgents owns the state

An agent's identity, instructions, conversation history, mailbox, channels and credentials live in AnyAgents, never only in a vendor tool's local files. A vendor session is a working copy. So an agent can move to another harness or another model and keep its history, its mailbox and its Telegram bot.

- **Transcript.** The server records every turn from the harness stream: the input messages, the assistant text, tool calls and results as the harness reports them, and usage. This transcript, not the vendor's session file, is the agent's history.
- **Switching harness or model.** The supervisor applies changed configuration before its next dispatch when the retiring process is idle, faulted or dead. A healthy in-flight turn may finish at its normal boundary, with a 15-second maximum drain window; new mailbox messages wait in the store during that drain. Paused and archived agents launch the configured harness when explicitly woken. Replacement preserves identity, instructions, schedules, the delivery cursor and pending mail. Cross-harness continuity comes from the conversation store, not a copy of the retiring vendor transcript; already-delivered messages are not replayed. A tool call in flight in the old harness is not carried across.
- **Credentials.** Each auth profile has its own config directory under the AnyAgents data directory, and the harness is started with its documented config-directory setting pointed there (for example `CLAUDE_CONFIG_DIR`, `CODEX_HOME`). The vendor tool writes its sign-in credential into that directory; AnyAgents owns where it lives, backs it up with the rest of its state, and never reads the token itself. API keys are kept in AnyAgents' secret store (below).

## Three axes: harness, model, auth

An agent is configured on three independent choices:

| Axis | Values (first version) | Notes |
| --- | --- | --- |
| Harness | `spiron` (`sylphx` compatibility alias), `claude-code`, `codex`, `opencode`, `agy`, `muse`, `omp`, `dsh`, `grok`, and more by adapter | All harnesses are external processes; Spiron must be installed on `PATH` or selected with `ANYAGENTS_SPIRON_BIN`. |
| Model | `sylphx/openai/gpt-6.1-sol#xhigh`, Claude, GPT, Gemini, DeepSeek, Grok, MiMo and any model a configured provider serves | Chosen separately from the harness. |
| Auth | `sylphx` (RFC 8628 Device Flow), `subscription`, or `api-key` | Decides which (harness, model) pairs are valid. |

The axes are fully orthogonal only with an API key:

- **Subscription.** A vendor subscription signs in to that vendor's own tool and runs that vendor's models: a Claude subscription in Claude Code runs Claude models; a ChatGPT sign-in in Codex runs OpenAI models. AnyAgents runs the vendor's sign-in (see [Harnesses](#harnesses-install-update-sign-in)) and never routes a subscription credential through the gateway or any other endpoint, because the vendor's terms bind the subscription to its own tool. Each catalog entry lists the models its subscription covers, with the terms it was read from; a vendor whose terms have not been read is marked unread and offered with an API key only.
- **API key.** Any model in any harness. The harness is started with its base-URL setting pointed at the local gateway (for example `ANTHROPIC_BASE_URL` for Claude Code, `OPENAI_BASE_URL` for tools that speak the OpenAI APIs), and the gateway translates to the provider that serves the chosen model, using the user's key for that provider.

Clients show only valid combinations, so an invalid one is never offered rather than failing at run time.

## Protocol gateway (API-key mode)

A local HTTP server inside `anyagents serve`, bound to loopback, reachable only by the harness processes AnyAgents started (each gets a per-agent gateway token).

- **Inbound protocols:** the Anthropic Messages API (`/v1/messages`, streaming), and the OpenAI Chat Completions and Responses APIs (`/v1/chat/completions`, `/v1/responses`, streaming), because those are what the supported harnesses speak.
- **Outbound:** the provider's native API for the chosen model. When inbound and outbound are the same protocol the request passes through unchanged apart from the key and model; otherwise the gateway maps messages, system prompts, tool definitions, tool calls and results, streaming events, stop reasons and usage between them.
- **Usage at the source.** The gateway sees every token in API-key mode, so its usage records are exact and the harness's own report is used only as a cross-check.
- **Limits declared, not hidden.** A feature one side has and the other lacks (for example a provider without tool calling, or a thinking-block format with no equivalent) is declared on the model's catalog entry; such a pair is greyed out for the harnesses that need it.

The reference for this translation is the mature open-source proxies that already do it, such as LiteLLM's Anthropic-compatible endpoint; the gateway's conformance tests replay recorded request and stream pairs from each harness against each provider protocol.

## Resources and identifiers

The API is resource-oriented ([Google AIP-121](https://google.aip.dev/121)) and defined once in protobuf (`proto/anyagents/v1/`). The HTTP/JSON and gRPC surfaces, the CLI and the MCP tool schemas are generated from it.

| Resource | Name | Holds |
| --- | --- | --- |
| Harness | `harnesses/{harness}` | catalog entry, installed version, sign-in state, capabilities |
| Agent | `agents/{agent}` | harness, model, working directory, instructions, attached channels, state |
| Channel | `channels/{channel}` | adapter kind (for example `telegram`), its bot identity, the agents attached |
| Message | `agents/{agent}/messages/{message}` | one mailbox entry and its delivery state |
| Usage record | `agents/{agent}/usageRecords/{record}` | one turn's tokens, cost and quota state |

Identifiers are [TypeIDs](https://github.com/jetify-com/typeid) over UUIDv7 (`agt_`, `msg_`, `chn_`, `usg_`). A harness id is its catalog key (`claude-code`, `codex`, `opencode`, `gemini-cli`). Ids from a vendor or a channel (a vendor session id, a Telegram update id) keep their own format and are stored beside ours, never re-encoded.

## Harnesses: install, update, sign in

A harness is data: a catalog entry per vendor tool, shipped with the binary and overridable by the user. An entry names the install method per platform (`npm` into a private prefix, the vendor's install script with a private prefix, or a release binary with its checksum), the update method, the sign-in method, the programmatic mode, and the capabilities the adapter declares.

- **Lazy install.** Nothing is installed until an agent needs it. The first `agents.create` on a harness starts the install in the background and the agent waits in `INSTALLING`. Each harness lives under the AnyAgents data directory at a pinned version (`harnesses/<id>/<version>/`), never in a global location the user's own setup depends on.
- **Update.** The server checks for a new version on a schedule and applies it only when none of that harness's agents is in a turn. The previous version stays installed until the new one has answered once, so a bad update rolls back by switching the pin.
- **Sign in inside AnyAgents.** The server runs the vendor's own sign-in command for that harness and streams what it asks for (a link, a device code, a prompt) to the client that asked, through the API. The user completes it in a browser; the vendor tool writes its credential to its own store under the AnyAgents-managed config directory for that harness. AnyAgents records only the outcome (signed in, as whom when the vendor says, and when). An API-key sign-in is passed to the vendor tool through the variable it documents and kept in the OS keychain, or a 0600 file where there is no keychain.
- **Sign in one agent from the CLI.** Each agent runs with its own config directory (`agents/<id>/config`), so a harness signature is only usable by that agent when it was made in that directory. `anyagents sign-in <agent>` runs the vendor's command there, reads it out before the server is up, and prints only each link, device code or prompt as it arrives and the outcome on the last line. It never reads, prints or copies the credential. `anyagents harnesses list` prints each catalog key, the variable that points the vendor at its private directory, and the command that installs it, so a missing vendor program is one line away from fixed.
- **Sharing.** All agents on one harness share its sign-in by default, because a subscription belongs to a person, not to an agent. A separate profile per agent is an option on create.

## Agents: create and supervise

`agents.create(harness = "claude-code", name, model?, workdir?, instructions?, channels?)` is one call. It records the agent, ensures the harness is installed and signed in, registers the AnyAgents MCP server into that agent's harness configuration (so the agent has `send`, `inbox` and `agents` from its first turn), and starts the agent's vendor process.

AnyAgents is a message switchboard plus a process supervisor. It never models a harness's agent loop: it does not track turns, wait for one to end, or read a vendor's answer at the end of one.

Each agent is one long-lived vendor process in its programmatic mode, run under a supervisor:

- A per-agent lock: a second process for the same agent is refused, never started beside the first.
- Restart with exponential backoff and jitter after a crash, resuming the same vendor session id. Starting a new session in place of a failed resume is reported as a failed resume, not hidden.
- A quota or rate-limit answer from the vendor (for example an HTTP 429 with a reset time) is a health state with its reset time, not a crash and not a restart loop.
- A process that has gone is replaced after its backoff, resuming the session, and the lines after the one cursor go to the new process. A line already taken is never written twice: each adapter remembers the ids its session holds.

### New-agent workspace selection

`create::create` resolves workspace defaults for every spawn route. A new
sub-agent with neither `repo` nor `workdir` gets a private
`<data>/agents/<id>/work` directory, not its parent's directory. Each sibling
gets a different directory. `workdir: "private"` selects this explicitly;
`workdir: "inherit"` shares an existing parent directory only when requested.
An explicit path is honoured. `repo: "inherit"` or `workdir: "worktree"`
creates an isolated Git worktree instead of sharing the parent's worktree.

Root creation can receive the operator's `workdir_default`; without that
operator override it also uses its own managed directory. These defaults apply
only to newly created agents: stored workdirs are not migrated or rewritten.

### GitHub event watches

`github:pr:owner/repo#number` and `github:check:owner/repo@ref` use REST
through the authenticated `gh api` adapter, never a GraphQL check rollup.
PR observations combine the pull's state/head with every page of Check Runs
and legacy commit-status contexts. A numeric check reference shares the PR
watch's target cache. Watch syntax and wake transitions are unchanged.

All agents in one daemon share a serial, single-flight probe cache. Each target
starts at 60 seconds; unchanged observations double its interval to at most
300 seconds, and a state transition resets it to 60 seconds. Each REST page
retains its ETag and a compact observation summary; subsequent reads send
`If-None-Match`, including unchanged pages after a changed first page. A
3,000-check PR requires 30 Check Run pages, plus the pull and commit statuses,
not one expanded GraphQL query per watching agent. Inactive targets expire
after one hour, and obsolete head/page summaries are discarded.
If the `gh` adapter supplies a disk-cached JSON body with a `304 Not Modified`
response before this daemon has cached that page, the watch parses the body to
initialize its page summary. A bodyless 304 still requires an in-memory page;
without either source, the watch reports a payload error.

A 403/429 secondary-limit response or `Retry-After` pauses every GitHub watch
request until the cooldown expires. Each watch records `probe_error:rate_limited`
once while preserving its last successful baseline; repeated scheduler ticks
neither repeat the observation write nor wake an agent. The first eligible
probe after cooldown rereads REST state, so a merge or check completion during
the pause is delivered after recovery. Rate-limit responses without a stated
retry time use exponential cooldowns from 60 to 300 seconds. Permission and
payload errors remain distinct and never masquerade as completed checks.

### Archived sub-agent cleanup

An explicit archive creates a fresh `.anyagents-archived` generation in the
agent's config directory. Successful ephemeral cleanup copies that generation
to `.anyagents-reaped`; retained dirty, unpushed, external or shared worktrees do
not cause cleanup to repeat. A wake clears both markers before the agent resumes.
Missing or empty legacy archive generations are repaired under the lifecycle
lock, and unsuccessful cleanup stays pending for a later pass.

Both server modes run archive cleanup in their existing independent maintenance
worker, immediately at startup and then every 60 seconds. Mail reconciliation
never reaps directories or waits for a busy lifecycle lock: it skips that row
and continues with unrelated agents. Explicit online archive waits for process
down before cleaning the target; descendants remain pending until their
processes stop. Offline archive performs cleanup without a daemon. Cleanup does
not initialize seat skills or recreate an empty workdir; runtime preparation on
wake does that. Credential propagation may independently recreate small auth
directories, but their existence is not a cleanup trigger.

Online deletion waits up to two seconds for both stopped health and release of
the process lock. An optimistic stopped-health projection cannot remove identity,
credentials or files while the worker still holds that lock. If shutdown exceeds
the deadline, deletion returns a conflict and retains the archived identity and
resources so a later request can complete.

### The adapter contract

One adapter per harness. A new harness is an adapter plus a catalog entry; the mailbox, channels, `send` and usage do not change.

| Call | Obligation |
| --- | --- |
| `deliver(message)` | Put one mailbox message into the live vendor process through that harness's own input or steer API. Return `accepted` once the vendor has taken it (its acknowledgement, echo or written line), or the reason it could not. |
| `background()` | Ask the vendor tool to move running work off the turn, where the tool supports it. Never wrap the tool's own tools. |
| `resume(session)` | Reattach to the same vendor session id. |
| `cancel()` | The vendor's own interrupt for the current turn. |
| `health()` | Process alive, declared delivery mode, steer support (yes, no or unproven), and any current quota or sign-in fault. |
| `last_usage()` | Tokens and cost the vendor reported for the runs this process made, read once when the mailbox has drained. Never part of delivery. |

Every harness is admitted. The delivery mode is whatever that tool does best, declared rather than hidden. Every line is written the moment it arrives; the vendor decides whether it joins the running turn or starts the next one.

The native Sylphx HTTP transport sends JSON request bodies as bytes. Its curl
fallback reads the body from stdin, so growing conversation history does not
become one command-line argument or encounter the operating system's per-argument
size limit. HTTP refusals retain their status and response body.

| Harness | Mode | How a line reaches it |
| --- | --- | --- |
| `spiron` (legacy `sylphx` alias) | `steer` | External `spiron --headless --json` process. Correlated `user` JSONL receives `accepted` (new turn) or `steer_ack` (active turn) before mailbox acceptance. Only the matching terminal `result` closes a turn; `cancel_ack` alone does not. |
| `opencode` | `steer` | `POST /api/session/{id}/prompt` with `delivery: "steer"`; the session's own inbox decides whether it joins the running turn or starts the next one. Accepted when the prompt API answers with `data`. |
| `codex` | `steer` | `turn/start`, with `ifBusy: "steer"` while a turn is open. |
| `claude-code` | `steer` | Idle: a stream-json `user` message. Active turn: an SDK `PreToolUse` callback returns pending lines as `additionalContext` in the current transcript before the tool proceeds; stdin replay alone is not evidence of mid-turn delivery. |
| `muse` | `steer` | `turn/start` with `ifBusy: "steer"`. |
| `omp` | `steer` | Stream-json child stdin with `steer` delivery into the active turn. |
| `dsh` | `steer` | `POST /api/session/prompt` with `mode: "steer"` on the session controller HTTP gateway. |
| `grok` | `steer` | ACP JSON-RPC 2.0 `session/prompt` over stdio with `follow_up_behavior = "steer"`. |
| `agy` | `steer` | Stream-json child stdin plus native `LanguageServerService/SendUserCascadeMessage` Connect RPC steering into an open cascade turn. |
| `followup` mode | `followup` | The contract double (`ANYAGENTS_HARNESS_BIN`) reads one line per turn and answers with a `result`; a line written mid-turn waits for that turn to end. |

Claude Code initializes its SDK hook before accepting input and fails startup if
initialization is rejected or times out. A busy delivery is deferred, leaving the
mailbox cursor unchanged, until its hook response is written successfully.
Repeated offers of the same mailbox id are coalesced; messages retain their
sender, room context and reply reference. If the turn finishes without another
tool call, the undelivered line starts the next idle turn instead. The hook write
is a transport acceptance, not proof that the model acted on the message. Dedupe
between acceptance and cursor commit is local to the retained harness instance;
this does not add transactional exactly-once delivery across a vendor process
crash. Hook protocol, cursor replay and tool ordering are covered by scripted
fixtures, not a live authenticated Claude model test.


No harness here uses `queue`: none of them is a one-shot run that must wait.

The per-harness programmatic modes, sign-in methods and usage fields are listed with their sources in [harnesses.md](harnesses.md).

### OpenCode service isolation

Each OpenCode agent persists its service port in
`<agent>/config/opencode-service-port`. Allocation probes loopback ports
20000–29999, reuses the saved port when free, and reserves it while the child
starts. An occupied saved port or a bind race selects a replacement, persists it,
and logs the old and new numbers. Collision classification preserves a bounded,
private stderr signal from the child, including when the holder has already
released the port before a later bind probe. Raw stderr is not published.
Stopping the harness releases its socket and
in-process reservation; deleting the agent also removes the saved allocation
with its config directory.

The adapter uses `serve --hostname 127.0.0.1 --port <port>`, then points every
`api --server <url>` call at that child's announced endpoint. It refuses to call
`api` if the announcement is missing, so a failed startup cannot discover or
start the machine's default managed service. Agent config, data and state roots
are set before the first vendor command. Inherited server passwords are removed
only from the service launch so OpenCode generates and announces its own private
credential; subsequent API children receive that credential.

OpenCode v2.1.4 source, read 2026-11-07 UTC at
[`68bc26b`](https://github.com/anomalyco/opencode/tree/68bc26b7da1990ce10ffecfbc1e09eb13aa5d824),
confirms the explicit port in `packages/cli/src/commands/handlers/serve.ts:9–14`
and `packages/cli/src/server-process.ts:68–72`. The managed-service fallback is
in `packages/cli/src/services/server-connection.ts:29–55`; service setters stop
the managed service before writing (`services/service-config.ts:273–277`), so
this adapter does not invoke a setter or edit shared service configuration.

### Antigravity and Muse

- **Antigravity (`agy`).** The installed CLI documents a turn per stream-json input line and `--conversation <id>` for resume. The adapter reports followup/unproven. Its hook helpers are not wired to a confirmed customization root and have no vendor consumption acknowledgement. AGY 1.4.5's embedded lifecycle documentation specifies `hooks.json` in a customization root (for example `.agents/hooks.json`), with a named hook's flat `PreInvocation` command array and output `{"injectSteps":[{"userMessage":"..."}]}`. It does not specify the durable acceptance/replay contract needed to commit the mailbox cursor across hook abort or process restart. The existing `agy-hook` helper only offers `received` rows, while stream pipe writes already mark rows delivered; it cannot establish native mid-turn delivery. Do not advertise steer support or install a hook bound to a seat in a shared workspace until isolated hook discovery and consumption/recovery semantics are verified.
- **Muse (`muse serve`).** One JSON-RPC child that owns the session: `initialize`, `session/start` (or `session/resume`), `session/setApprovalMode` `allowAll`. Delivery mode is steer: `turn/start` when no turn is open, and `turn/start` with `ifBusy: "steer"` during one. `turn/started` opens a turn, `turn/completed` closes it with its usage or its error, and an `approval/request` is answered by the host. `MUSE_CONFIG_HOME` points the vendor at the agent's config directory, where `settings.json` names the MCP server and the `:unrestricted` permission profile. On Windows the clone is a directory junction at `%APPDATA%\meta\muse`.

### Managed local checkouts

Repository-backed local agents use a caller-configured checkout provider through
`ANYAGENTS_CHECKOUT_PROVIDER_BIN`. Repository slugs and GitHub URLs, `repo: "inherit"`, and
local repository paths use that provider for fetching and checkout. The provider
owns sparse-checkout and hook policy. Fetch, checkout and executable-launch failures
refuse the spawn with its diagnostic; an unavailable configured provider never
silently selects a different checkout mechanism.

Explicit `workdir` paths, including `/scratch/wt/...` and `/scratch/tmp/...`, are
honoured or refused, never replaced by the default directory. Before an agent
becomes running, its execution seat must be able to write and traverse its
working tree, the per-worktree Git directory and the common objects/refs
storage, and create lock files. New worktrees are shared with that seat's group.
On Linux, additive POSIX ACLs grant writes only to shared `objects/`, `refs/`,
`logs/refs/`, an existing `packed-refs`, and the child's private `worktrees/<name>/`.
The common root, `worktrees/` and `logs/` are traversal-only with no default ACLs;
the main worktree's `logs/HEAD` stays child-unwritable. Defaults
exist only inside the shared content subtrees, the private gitdir and a dedicated
`anyagents-config/` directory, never on mixed metadata ancestors. Common `config`
is an exact symlink to `anyagents-config/config`: the dedicated directory grants
recorded active readers traversal and read-only inheritance, so the parent's
ordinary `git config`, `git remote` and `git push -u` replacements remain readable
immediately, without a launch or wake. Its defaults grant no owning-group or
outsider access. Adoption holds Git's original `config.lock`, journals a
quarantined intent, hard-links the original inode into the dedicated directory
and atomically replaces the old pathname with the link, preserving config bytes
and ownership. The journal pins the directory and symlink identities, while Git
may replace only the target config inode. Interrupted adoption preserves the
original bytes and refuses launch/reap pending operator recovery. Revocation
removes access and default entries only when no active same-seat reader remains.
Retired server-owned journals retain the dedicated directory/link provenance for
later admission, even after their private gitdirs are reaped; that provenance
never restores a retired reader's access.
Common `config`, `hooks/` and `info/` are not child-writable. A parent-owned sticky
private gitdir protects `config.worktree` (and its unlink/rename), `commondir` and
`gitdir`; mutable HEAD/index files belong to the child so Git lock+rename works.
Temporary cleanup inspections persist each ACL target's exact path, device and
inode before granting read/traverse access. Startup recovers those objects through
no-follow descriptors; legacy intents use a best-effort no-follow tree walk.
An unsafe recovery target or failed revocation quarantines only the recorded
worktree and emits a warning, never aborting unrelated server startup.
Quarantine preserves checkout data and refuses automatic harness launch, wake and
reap until the operator repairs the recorded recovery state; it does not assert
that an unreachable or hard-linked object's residual ACL was removed.
Shared refs remain a same-repository integrity boundary: a child can move another
branch ref. This is not hostile-repository isolation, and AnyAgents does not
intercept arbitrary harness commit/push commands or provide a branch-tip guard.
AnyAgents production code never runs `git commit` or `git push`; the supported
source-publication path belongs to the caller's checkout and publication tools. Its Git metadata reads, skill sync and cleanup
commands pin `core.hooksPath=/dev/null` and disable fsmonitor. The parent commit
and its parent-owned hooks-path pin in the three-UID proof belong to the test fixture.
Shared common and worktree configs refuse `include`/`includeIf` rather than
following a potentially child-controlled configuration graph.
Caller-configured commit identity is set in the protected private
`config.worktree`, never by replacing the parent's common config. The consumer
must enable `extensions.worktreeConfig`; otherwise admission refuses. Seat Git
configuration resets legacy wildcard trust and lists only the seat's exact
checkout and recorded common gitdir, preserving other settings. Cleanup loads
its gitdir/worktree pair from the server journal, verifies the protected metadata
ownership and effective ACLs, and passes both paths explicitly: a child-edited
`.git` pointer never selects metadata. A missing, broken or changed pointer on a
journaled checkout preserves its entire workdir (including build-named output)
and emits a diagnostic; it cannot select non-Git directory deletion. Cleanup Git
runs as the journaled seat UID/GID, with no inherited secret environment or socket
group, because checkout and index attributes can select drivers from trusted
configuration. It excludes global/system config and attribute files and keeps
default dubious-ownership checks; unrecorded legacy or arbitrary repositories
are preserved rather than inspected through their `.git` pointer. After archive
revokes access, inspection adds only temporary read/traverse ACL entries to the
journaled metadata allowlist, never write or default entries, and retains active
same-seat bindings. Inspection intent is synced before granting; ACL changes are
serialized without holding the global journal lock during Git. Success and
failure both revoke and read back temporary access. Both server modes recover
any open inspection intent before starting consumers; failed recovery refuses
startup. A completed check never stands in for a fresh deletion eligibility check.
Deletion additionally requires an empty pinned status and zero commits in HEAD
or any local branch outside the recorded remote-tracking refs. A seat-owned
initial-head marker cannot waive that test. Dirty or unpushed checkouts and their
private registration remain; clean, pushed pairs are removed without invoking
`git worktree remove` or its nested Git subprocesses. Remote-tracking refs remain
within the documented shared-ref integrity boundary, not a live publication proof.
A server-owned grant journal is file- and directory-synced before sharing. Its
serialized transitions preserve bindings needed by another active agent on the
same OS seat; retiring one agent revokes its private metadata without removing
the surviving agent's shared-store access. Archive, reap and failed
admission revoke the seat's named group/user ACL entries and defaults through
opened descriptors, read the result back, and retain the journal on failure.
Revocation runs even when dirty or external checkout bytes must be retained.
An ACL write or readback failure is a typed permission error; non-Linux hosts
accept only already-permitted metadata access. Existing explicit directories are
validated without rewriting their permissions. Sharing does not follow symlinks
or alter hard-linked files that require permission changes.

For inherit/local sources, AnyAgents selects a GitHub remote (preferring
`origin`), reads its live advertised branches and tags, and requires both the
source HEAD and requested base to be reachable from them. Push unpublished
commits before spawning. If a remote tip is not available in local history,
fetch that remote before trying again. The consumer fetches the verified ref
and checks out the exact selected commit, including a published feature-branch
commit that is not on the default branch. Sources without a GitHub remote are
refused. Existing nonempty checkout targets are not silently adopted by
repository provisioning. `workdir: "private"` remains an empty directory and
needs no Git consumer. Cloud-sandbox workspace preparation is unchanged.

## Mailbox: one log per agent

Every input to an agent (a Telegram message, another agent's `send`, a CLI or TUI message) is appended to that agent's mailbox. The mailbox is the only path into a harness.

- **Sequence.** On durable receive the server assigns the next per-agent sequence number. Delivery, acknowledgement and replay key on that sequence. A message's source time (the Telegram `date`, the sender's clock) is kept as metadata and shown, but never used to order: a late, older event gets the next sequence and is not inserted behind messages already delivered.
- **One delivery cursor.** A row is `received` when its insert commits, and `delivered` when the harness's own input or steer API has taken it. There is no second cursor and no turn bookkeeping: a row is done the moment the vendor has it.
- **Contiguous cursor.** It advances only over a contiguous prefix of the sequence, one row at a time, so one row the harness did not take is visible as the oldest undelivered one rather than skipped.
- **Idempotent delivery.** After a crash, rows past the cursor are handed over again. Each adapter remembers the ids its session holds and drops one it has already written, so a replay never reaches the agent twice. Claude Code and Codex also carry the id in the line itself, and Codex sends it as `clientUserMessageId`. Inbound channel events dedupe on their source key (for Telegram, the bot and its update id).
- **Egress is the agent's own `send`.** An agent replies by calling its MCP `send` tool, or a channel tool; the switchboard routes that immediately. A reply to a Telegram message carries that chat as its target, so the agent's `send` reaches the same chat. Nothing reads a vendor's transcript to find an answer.
- **Retention.** Delivered messages are kept for a configurable period for `inbox` and audit, then compacted.

### Guarantees

These are invariants with tests (`tests/invariants/`), run against a scripted fake harness that implements each delivery mode and can be killed at any point:

1. No message that reached `received` is lost across a server or harness crash.
2. No message reaches the agent twice, including after a crash between the harness taking it and the cursor recording it.
3. Per agent, delivery follows sequence order. A late event never reorders what was already delivered.
4. No source starves another: order is receipt order, not a priority among channels.
5. Lag is always visible: `health` reports the age of the oldest message the harness has not taken, per agent.
6. A quota fault is a reported state with a reset time, and the server keeps the mailbox growing durably while it lasts.
7. One vendor process per agent.
8. A line is handed over once per burst: never twice across a restart, never lost across one.

## Channels

A channel adapter has an inbound side (a stream of events, each with its source key), an outbound side (post or reply to a target), and an identity (which bot or account it is). The built-in agent bus, which carries `send` between agents on the server, is not a channel adapter; it writes straight to mailboxes.

**Telegram (first version).** One bot per agent, created by the user with BotFather and recorded with `anyagents channels add telegram --agent <name> --token-file <path>`; the token is copied into AnyAgents' own credential store at `secrets/channels/<channel id>` (mode 0600) and never into the database, a log or a command line. `getMe` names the bot once, so the channel shows which bot it is. Token replacement keeps the channel ID and writes a fresh reference under `secrets/channel-bindings/<channel id>/`; it preserves the previous credential bytes if a later store update fails. This namespace stays separate from the original `secrets/channels/<channel id>` file.

- **Long polling.** A local server usually has no public URL, so the transport is `getUpdates` long polling, one poll per bot at a time on a thread that stops with the server. A webhook is the option for a server that is reachable; it is not built yet.
- **What one update becomes.** A typed message, or a media message with a caption, becomes one mailbox message. Its source key is the bot and the update id, so the same update stored twice is one row. `sender` is `telegram:user:<id>:<message id>`. `reply_to` is the target a bare reply goes to: `telegram:chat:<chat id>`, or `telegram:chat:<chat id>:<message thread id>` for a forum topic, so a reply lands in the same chat and the same topic. Stickers, edited messages and membership changes are not delivered.
- **Poll offset.** The next update id lives in the channel's `state` JSON and is saved only after the updates it covers are in the mailbox, so a restart asks for what it has not stored.
- **Sending.** The `send` tool takes `telegram:chat:<id>[:<topic>]`, and `telegram:user:<id>[:<message>]` where the message is what the reply answers, plus `reply_to` for an explicit Telegram message id. A forum post carries its `message_thread_id`. The returned id is the Telegram message id.
- **Bot to bot.** A bot generally does not receive messages sent by other bots ([bot FAQ](https://core.telegram.org/bots/faq#what-messages-will-my-bot-get)), and recent Bot API versions deliver only some of them. So a chat an agent's bot deals in is remembered in the channel state, and when an agent posts into a chat through its bot the server also delivers that post into the mailbox of every other agent on the same chat, keyed by the outbound message id; if Telegram also delivers the same post, the copy is dropped by its source key. The sender does not receive its own post.

## Send, inbox and agents (the MCP tools and three mounting modes)

`anyagents mcp --config <agent config dir>` (over `stdio`) and Streamable HTTP `POST /mcp` / `GET /mcp` (over the server's HTTP listener or public webhook port, authenticated via `Authorization: Bearer <mcp-capability>` or `x-anyagents-agent: <mcp-capability>`) expose AnyAgents' MCP surface. Each connection holds one opaque capability minted by the server for one agent instance, so the agent acts as exactly that agent and nothing a tool argument says.

AnyAgents selects the exposed MCP tool profile according to the execution boundary:

1. **`full` (Default for Local 3rd-Party CLI Harnesses over `stdio` — `claude-code`, `codex`, `agy`, `opencode`):** Exposes the local workspace `code` tool (`read`, `write`, `edit`, `bash`, `search` inside the agent's local worktree), `memory`, `skills`, `web_search`, `web_fetch`, `browser`, `computer`, and the coordination tools (`send`, `inbox`, `agents`, `groups`, `schedules`, `conversation`).
2. **`remote` (Default for Streamable HTTPS `POST /mcp` — Closed-Box 3rd-Party Cloud Agents such as Claude Cloud, OpenAI Responses/Codex Cloud, and Cursor Cloud Agents):** Closed-box vendor cloud containers keep their own native container file/bash sandbox tools; AnyAgents' remote `/mcp` omits host-local `code` (so a cloud agent never accidentally edits the orchestrator host's disk) while exposing `send`, `inbox`, `agents`, `groups`, `schedules`, `conversation`, `memory`, `skills`, `web_search`, `web_fetch`, `browser`, and `computer`.
3. **`coordination` (Default when `harness == "spiron"` or `--profile coordination` / `?profile=coordination`):** Because the first-party `spiron` harness natively embeds the shared capability core's `code`, file, `web_search`, `web_fetch`, `memory`, and `skills` tools, AnyAgents exposes only the orchestration and host-device tools (`send`, `inbox`, `agents`, `groups`, `schedules`, `conversation`, `browser`, `computer`), eliminating duplicate tool definitions and duplicate `<available_skills>` / `<memory>` prompt blocks.

## Identity: one authenticated connection per agent session

A capability is 32 random bytes the server mints for one agent instance. The server keeps only its hash, beside the agent's immutable id and a generation: minting a new one revokes the last, so a replaced session stops working at once. The capability is written `0600` into the agent's own config directory (`agents/<id>/config/mcp-capability`), or `0640` in that agent's own OS group when OS seats are configured, so its harness reads it and no other seat can. It is never an argument, a tool argument, an environment value or a log line.

The server decides the principal before any route runs:

- **An agent** is whatever agent the presented capability names, read from the store. A `sender`, an inbox path, a bot, a session id or an environment value never establishes identity. A body that names another sender is refused with no write.
- **The operator** is the deployment's own authority, for its administrative calls only: creating agents, signing a harness in, minting a capability, reading any agent's inbox. It presents a token from a file the environment names, and the server keeps only a digest of it.
- **A machine without an operator token** is the single-user route: it acts as the agent a call names, and says so once at start.

## Runtime boundaries: local, container, cluster and OS seat

AnyAgents separates **session identity** (`mcp-capability`) from the **runtime execution environment** ([ADR 0002](adr/0002-runtime-drivers-and-capability-identity.md)). AnyAgents never brokers external tool credentials (`kubectl`, `gh`, `ssh`, `1Password` or cloud IAM); external credentials and hardware belong to the runtime environment:

1. **Local host (`local`, default):** When no OS seats are configured, every harness process runs under the server user's own UID and keeps the user's real `HOME` directory. Host CLI tools, SSH keys, kubeconfigs and local hardware/GPUs work out of the box with zero configuration, while each agent's harness state (`CLAUDE_CONFIG_DIR`, `CODEX_HOME`, `XDG_CONFIG_HOME` / `XDG_DATA_HOME` / `XDG_STATE_HOME`, `AGY_CONFIG_DIR`, `MUSE_CONFIG_HOME`, `PI_CODING_AGENT_DIR`) and `0600` `mcp-capability` are isolated under `<data>/agents/<id>/config`.
2. **Container (`docker`) and cluster (`k8s`):** Each agent runs as the single user inside its own container or Pod, connects to the switchboard with its `mcp-capability`, and receives external credentials, volumes and GPU devices through standard Docker (`--env-file`, `-v`, `--gpus`) or Kubernetes (`Secret`, `ServiceAccount`, Workload Identity) primitives.
3. **Multi-seat host (`seat`, opt-in):** The operator supplies `ANYAGENTS_PRINCIPALS` as JSON keyed by immutable agent ID, with explicit `user`, `uid`, `gid`, and `socket_gid` fields. The UID must differ from root and the server's own UID; neither display names nor existing file owners admit a principal. Register agents before enabling this map and admit each ID before launching it. Legacy numeric seat variables select the boundary but cannot authorize an agent without this map. Every harness then starts inside a distinct managed OS seat (`setpriv --reuid <uid> --regid <gid> --groups <gid>,<socket group> --no-new-privs --`). `anyagents agent-run --config <dir> -- <command>` asserts the process's effective UID/GID, `NoNewPrivs = 1`, denial of the server's database/secrets/operator token and peers' config directories, and writability of its own config directory before executing the command.

### Legacy first-party workspace placement

The legacy first-party harness exposed `sandbox(action="placement")` to read its own
workspace routing, or `sandbox(action="placement", placement="local" | "cloud")`
to persist a choice for the next workspace tool. Readback names the effective
surface and worktree. This changes only the session's
`sylphx-workspace-placement.json` routing record: it does not change the agent,
Session, model, profile, credentials, pending operations, or a retained lease.
Without a selection the existing sandbox configuration remains authoritative.

The embedding runtime can restrict the supported surfaces with launch setting
`SYLPHX_WORKSPACE_PLACEMENTS=local,cloud` (an empty value grants neither).
Selections cannot override a disabled workspace. Invalid or ungranted choices
leave the saved record unchanged; every workspace tool revalidates it, including
revoked grants and malformed saved state. Explicit cloud placement fails closed
on a cloud error instead of silently running on the host. Lease release and
session pull do not select a placement; their receipts say so.

Local workspace commands and file operations use the existing server-supplied
`ANYAGENTS_SEAT_LAUNCH` / shared harness launcher boundary when configured.
File reads, writes, edits, directory listing, code mode and every patch backend
run under that delegated principal, not the in-process server's UID. A malformed,
unavailable or refusing configured launcher never falls back to server-side file
IO. Tool arguments cannot choose a principal. Without a configured seat, local
commands inherit the ordinary UID and native file operations retain their usual
behavior. Routing does not grant a new OS identity or external credential.

Source publication is not running-server activation. Activation must separately
identify the installed source and preserve existing workers and accepted or
unknown operations; actual same-agent tool/worktree/principal acceptance remains
a runtime check, not an implication of the placement receipt or CLI version.

## Replies and quotes

Each stored message carries the context the server authenticated for it: the author, the channel, the chat and forum topic, the mailbox reference a bare answer names, the destination that answer goes to, and the message it quotes. Every harness shows its model that context before the body, composed in one place (`anyagents_harness::compose_message`); the body's own text is never read as part of it.

`reply_to` is a mailbox reference (`anyagents:reply:<message id>`), a bare message id, or an address. A reference is resolved from the stored record, so a group message is answered in its group and its topic at the incoming message, and a direct message in its own chat. An address is used as written, which is how an agent starts a new conversation on purpose. A message naming a destination no record names is refused.

A message from Telegram keeps the message it quotes: the quoted author, as Telegram marked it (bot or person), its text or caption, and the ids that name it. The quoted text is fenced and neutralised, so a modelled line cannot forge a quoted author.

Named virtual groups store an immutable creator `owner_agent_id`, independent
of membership. Legacy groups retain NULL ownership (operator-owned), even when
their roster is empty. Existing members may invite agents to private groups;
nonmembers need the owner's or operator's invitation. `public` on `join` only
configures a newly created group and is ignored for existing groups. Visibility
changes through `members` or `leave` require the owner, an ancestor supervising
the owner, the operator or an explicitly privileged root admin. Removing another
agent requires authority over that target (self or ancestor), the group owner or
the operator. Batch authorization and all membership/visibility writes commit in
one SQLite transaction; refusals return HTTP 403 without partial group changes.
Agent creation's `group` option uses the same checks, and agent CLI mutations
require an authenticated server rather than an offline database fallback.

- `send(to: [address], text, reply_to?)`. An address is an agent (`agent:<name>`, or a bare name), a named group of agents (`group:<name>`), or a channel target (`telegram:chat:<id>[:<topic>]`, `telegram:user:<id>[:<message>]`). A list of one is a direct message; a mixed list delivers once per address. There is no default address.
- `inbox(since?)` reads this agent's own mailbox. It never sends.
- `conversation(...)` / `search(...)` run **Zero-Embedding Lossless Lexical + Structured Agentic History Search (`Raw-RAG`)**:
  - **Read authorization:** Non-admin agents read only their own and descendant mailboxes/transcripts. `agent: "all"` and `"*"` mean that authorized tree, not the workspace. Messaging scope, public reachability and shared groups do not grant private history access; explicit parent/sibling/peer reads return forbidden. `with`, sender, range and radius can only narrow the authorized rows. Operator and explicitly privileged root-admin reads are unrestricted and audited. Agent CLI reads require the authenticated server, without an offline SQLite fallback.
  - **No vector embeddings and no LLM-written memory mutations:** History stays 100% lossless in the append-only transcript and SQLite `rag_chunks_fts` (`unicode61` over raw text + hybrid ASCII identifier sub-tokens + CJK unigrams & bigrams). There is zero embedding API cost, zero embedding latency, and zero model-lock degradation. Exceptional erasure uses the permissioned, audited [redaction operation](redaction.md), preserving ids/order and replacing only explicitly selected content; ordinary coordinator history reads remain lossless.
  - **Multi-query & negative filtering:** `query` plus `queries: [...]` (OR-combined keyword/synonym sets) and `exclude: [...]` (negative filters).
  - **Tool history toggles:** `include_tools` (default `true`; `false` restricts search to messages without tool/system noise), `only_tools` (`true` restricts search to tool calls and outputs), and `tool_name` (filters tool history to a specific tool such as `"bash"`, `"browser"`, `"agents"`, `"schedule"`, `"send"`).
  - **Structured filters & ordering:** Filter by `with` (peer/channel/group), `direction` (`"in"` / `"out"`), `sender`, `role` (`"user"` / `"assistant"` / `"tool"`), `since_ms` / `until_ms`, and order by `sort = "relevance"` (BM25 + CJK bigram + exact-match boost + query coverage + recency), `"newest"` (`timestamp DESC`), or `"oldest"` (`timestamp ASC`).
  - **Range reads & contiguous episode windows:** Read transcript ranges (`range = "seq:<from>-<to>"` or `"transcript:<from>-<to>"`, or `from_seq` / `to_seq`) and expand `radius` (`0..=10`) contiguous neighboring events around each hit to preserve causal paths `P(path | result)`.
- Harness-local recall, context windowing and compaction belong to the external CLI, not AnyAgents. Spiron receives `--state-dir` and `--instructions-file` pointing into the existing agent configuration directory, plus workspace, skills and MCP paths. The adapter reads `spiron-session-id` or legacy `sylphx-session-id` and writes only the canonical session marker. Legacy transcript, window, cloud-session and Goal files remain available to Spiron's continuity reader; AnyAgents never rewrites or summarizes them.
- `agents()` lists the agents on the server with their harness, state and lag, so agents can see each other.

## Usage and cost

After each turn the adapter reads the usage the vendor tool reports and the server writes one usage record: agent, harness, model, input, output, cache-read and cache-write tokens, the cost the vendor reported (if it reports one), an estimated list-price cost from a versioned price table, the billing mode (subscription or API key), and any quota state the vendor reported. The `usage` API aggregates by agent, harness, model and day. Records are also exported as OpenTelemetry metrics using the [GenAI semantic conventions](https://opentelemetry.io/docs/specs/semconv/gen-ai/) when an endpoint is configured.

A subscription harness often reports tokens but no price; its record says `subscription` and shows the list-price estimate separately, never as money spent.

## Security model

- The API listens on a Unix socket readable only by the user. A TCP listener is opt-in and requires a bearer token.
- Each agent's MCP capability is scoped to that agent, and minted fresh for each of its instances; the last one stops working at once.
- The operator's authority is its own token, never an agent's.
- Secrets (channel tokens, API keys, the capability itself) live in the OS keychain or a 0600 file, never in the database, logs, tool arguments or usage records.
- On a deployment with an operator token, every harness runs in its own OS seat: a distinct managed uid, no new privileges, and no read of the server's files or a peer's config. A deployment without that seat refuses to start as the boundary.

## Agent scope and metadata updates

An explicit `scope` survives server restarts, runtime-config reconciliation,
provider-route refreshes and unrelated metadata updates. SQLite is authoritative:
`agents.launch_env` retains `ANYAGENTS_SCOPE` (and the legacy `ANYAGENTS_PUBLIC`
marker); only the scope setter owns these keys. Harness-environment replacement
merges them from the current row in the same write transaction.

New roots default to `public`; all new children default to `private`, regardless
of name or role. The store upgrade materializes each existing agent's effective
scope explicitly, including legacy name/role defaults and legacy public markers.
Description, instructions and role updates that omit both `scope` and `public`
preserve that explicit policy.

The legacy `public` boolean describes cross-tree reachability, not sibling
access. `true` selects `public`. `false` preserves an existing nonpublic scope;
for a public agent it restores the default above. An explicit `scope` takes
precedence when both fields are supplied. To enable sibling collaboration, send
`scope: "siblings"` explicitly.

Only a strict ancestor (`is_ancestor`: parent, grandparent, or higher ancestor in the supervision tree, excluding `self`), a root admin (`is_admin`), or the operator may edit an agent's `description`, `instructions`, `role`, `scope`, `public`, `name` (`rename`), `harness`, or lifecycle state (`reset`, `pause`, `archive`, `delete`, `wake`, `schedule`, and subtree-bounded `transfer`). An agent (`self` without admin authority) has read-only access to its own metadata and authorized tree (`conversation`) and cannot overwrite its own instructions, description, role, or scope. Unrelated non-admin agents cannot read or overwrite another subtree's private metadata. Cross-tree operations (`mint_capability`, `operator_inbox`, cross-tree `transfer`, root creation) require `is_admin` or the operator token.

Every accepted scope or legacy-public write, including an unchanged value,
atomically records its authenticated `actor`, `before_scope`, `after_scope` and
`source` (`scope` or `public`) in `admin_audit`. Internal store callers record
`system`; API callers record `operator` or `agent:<name>`. An operator can trace
policy changes with:

```sql
SELECT datetime(created_at / 1000, 'unixepoch') AS utc,
       actor, target, before_scope, after_scope, source
FROM admin_audit
WHERE source IS NOT NULL AND target = 'agent-name'
ORDER BY id;
```

## Storage

One embedded SQLite database in WAL mode under the data directory holds every resource, the mailboxes and the usage records. A message is `received` when its insert has committed. Schema changes are versioned migrations applied at server start.

### Host history index refresh

Host history keeps raw text in its source files. The rebuildable contentless FTS
index stores metadata, bounded search windows and SHA-256 raw coordinates, not a
second raw transcript. Each source frontier and its documents/windows commit
atomically. A source changing during publication defers that snapshot without
advancing its frontier. `memory` and `transcripts` queries refresh/prune only their
requested source class; `all` refreshes both.

An unchanged native JSONL source reuses its persisted cursor. For the same device
and inode with no size decrease, a changed source checks the last **64 KiB before
the complete-record frontier**, then scans/indexes only the suffix. Capturing the
new frontier adds one more bounded 64 KiB read. A changed boundary, shrink,
replacement inode or parser-policy change rebuilds the complete source. Incomplete
last records stay outside the committed frontier. Search result window reads
always verify their own SHA; explicit whole-event readback verifies the event SHA.

This is deliberately not an append-only proof: a same-size edit **outside** the
boundary may remain missing/stale in the index until full verification. The daemon
maintenance loop fully rebuilds at most **one due source per 60-second tick**, with
sources due **one hour after their last full verification**. Backlog, a continuously
changing source or failures can extend that detection delay; no fixed one-hour
upper bound is promised. Failed sources rotate behind other due sources without
publishing a new frontier. A stale returned window is refused immediately by its
SHA check. Full verification uses the same lifecycle locks and pending-redaction
path exclusions as query refresh; it never edits the source. This maintenance is
host-driven, not an LLM schedule. Native transcript writers remain active in PR1;
the host raw append port is not activated by this index.

The retained mailbox index no longer clips newly recorded inbound, outbound or
tool text at 2,400 characters; full cleaned bodies also define deduplication hashes.
The first authorized lexical/structured search upgrades pre-existing mailbox
chunks from their authoritative inbound/outbound bodies in one transaction,
reading 32 rows per batch. The policy marker and FTS/text changes commit together;
subsequent searches skip upgraded rows. This backfill may add first-search latency
and retains the legacy mailbox index representation; it is not the filesystem
index's suffix-only scanner. Hidden owners are neither read nor upgraded.
Redacted mailbox bodies remain authoritative on retry/restart. Historical
standalone tool summaries clipped by an older writer have no raw mailbox body to
recover; they are preserved as-is rather than claiming a reconstructed suffix.
Reconciled pages under `agents/<id>/history/archive` are indexed as whole historical
documents in `transcripts` scope even when they are Markdown rather than JSONL.

For an indexed filesystem result, the canonical `conversation` tool/HTTP route
accepts `raw_id` (the result ID), required `raw_sha256` (the result's whole-event
`raw.sha256`), optional `raw_offset` (zero-based event-relative bytes, default 0),
and `raw_length` (1–65,536 bytes, default 65,536). The range clips at the event end;
offset equal to the event length returns an empty end-of-event range. The host
resolves the ID inside the caller's authorized owner set and selected agent/scope,
then repeats that lookup under the owner lifecycle lock. It rejects missing or
pending-redaction documents, stale digests and changed raw bytes. Arbitrary paths
and caller-supplied raw coordinates are not read arguments. Peer/parent history
remains inaccessible regardless of a guessed document ID or shared group.

Explicit raw reads stream-authenticate the **complete event** SHA-256 with a
64 KiB buffer before returning at most 64 KiB. This costs O(event length), not
O(requested range), but does not allocate the entire event. The response's single
turn includes exact `raw_bytes` as a JSON byte array, a lossy convenience `text`
preview, the complete `raw` coordinate and the requested `window` coordinate/SHA.
The byte array is authoritative for invalid UTF-8, zero bytes and ranges splitting
a Unicode character. It uses the same six-tool MCP surface; no seventh tool or
native writer handoff is introduced.

## Host-owned memory and retained history

[Host memory](host-memory.md) documents pinned learned facts, ACL-scoped lexical
retrieval, resumable migration, retained native writers and measured limits.
## Methodology-neutral roles and lifecycle

`role` is an optional display label, not a lifecycle or residency policy.
Private `instructions` define an agent's responsibilities, reporting conventions
and decision to archive. AnyAgents ships no company hierarchy, standing-agent
policy or built-in role templates.

Callers may supply local Markdown templates through `archetype` (aliases:
`template`, `type`, `preset`). A name is a single ASCII filename stem containing
only letters, digits, `-` or `_`, without a `.md` suffix. When
`ANYAGENTS_OWNER_WORKDIR` is configured, its `.agents/agents` catalog is the
sole source for discovery, creation and role updates, even if missing or empty;
unknown roles fail closed. No workspace, home or data-directory fallback is used.
Without that configuration, lookup searches explicit and ancestor workspaces'
`.agents/agents`, the data directory's fallback catalogs, and the home catalog
when no workspace catalog exists. Both `<name>.md` and `<name>/agent.md` (or
`AGENT.md`) are supported. An explicit archetype must resolve. Without owner
configuration, creation rejects unknown roles when a catalog exists; role updates
retain free-form labels when no roles are discovered. A matching `role` or its
lowercase, space-to-hyphen form supplies defaults.
Templates prepend their Markdown body to caller instructions and supply
missing fields from YAML frontmatter (`description`/`summary`, `scope`,
`workdir`/`workspace`, `harness`, `model`, `role`, `profile`). Unknown keys,
invalid scopes, malformed or unclosed frontmatter and
symlinks in any template path are validation errors, including implicit lookup;
lookup never silently falls through after a refusal. Scope validation happens
before agent persistence. Without a template, omitted HTTP harness retains
`claude-code`; an explicit empty harness or `inherit` selects child defaults.

MCP agent lists first apply visibility, parent and state scope. `summary.total`
and health/state counts describe that full population, before `query` or
pagination. `summary.matched` counts query matches before truncation and
`summary.returned` counts the returned slice. Every matched list is ordered by
working/errored status, running, idle, paused, then other states, with name and
ID tie-breakers, before slicing. List `limit` defaults to 100 and is clamped to
1–100. `cursor` (alias `offset`) is a zero-based position; `next_cursor` is the
next position or null at the end. Both unified and legacy sub-agent tools return
it. Continue with the same query, parent and state. Pages reflect current state,
not a snapshot; concurrent population/state changes can shift positions.

Every agent, including a root or role-bearing child, may archive itself.
Archive is reversible: an intentional direct message wakes the identity again;
passive notifications do not. Archive preserves the browser profile and login
state; only `delete` removes the agent's browser profile. Existing clean/pushed
managed-worktree cleanup still applies, while dirty or unpushed work is retained.

After a completed turn, any agent releases its process to `idle` when its mailbox
is drained, its latest accepted turn is complete, it has no running children or
in-flight background commands, and it has no active error. An idle agent wakes
on new mail. Its ID, role, instructions, scope, session, mailbox cursor, pending
deliveries and schedules remain intact. There is no role-based residency flag.
Topology and child discovery follow relationships and explicit scope, never the
presence of a role label. The default CLI depth limit applies equally to all roles.

### External steering custody

A Spiron `steer_ack` proves socket acceptance, not journal commit. AnyAgents
writes an ID-only `spiron-unconfirmed-steers.json` hold in the agent's config
directory before any input bytes are dispatched; a hold-write error refuses the
unsent input without advancing its mailbox cursor, and confirmed terminal
completion without `undelivered_steers` clears the hold. Standalone adapter and
`DeliveryLoop::run_once` passes never silently replay raw input while a hold is
present.

Under `Supervisor`, mid-turn process exits, coordinator restarts, unconfirmed
`undelivered_steers` receipts, and transient upstream stream/server failures
automatically reconcile `spiron-unconfirmed-steers.json` against Spiron's local
`sessions.sqlite3` (`session_entries`), promote all messages of the interrupted
turn into `turn_recovery`, archive/clear the hold and stale error state, and
resume via an explicit `[Recovery]` turn instructing the agent to verify actual
state before repeating side effects. An explicit `reset-session` similarly
archives any retired-generation hold under `sessions/retired-<generation>/`,
clears `.anyagents-last-error.json`, and retains unfinished mail in
`turn_recovery` so the new session generation starts cleanly with `[Recovery]`.
