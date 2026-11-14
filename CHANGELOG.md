# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

- Keep role labels methodology-neutral: roots and labelled children can self-archive
  and release idle processes under the same rules. Preserve legacy effective scopes
  on upgrade, default all new children to private, and retain browser profiles until
  deletion. Discovery and generated lifecycle instructions no longer depend on roles.
- Schedule wakes share the one mailbox: a due `at` or `every` tick, or a triggered
  watch, is written at once like any other message and reaches a busy owner by
  steering, or a paused or quota-blocked owner on resume. Recurring ticks are no
  longer dropped as `skipped:busy`; a schedule keeps at most one undelivered wake.
- The daemon's own cost now follows events, not fleet, archive or history size.
  Every `Store` handle in a process shares one writer and one read pool, and
  migrations run once per process. Idle agents run no supervisor passes, and a
  streaming turn wakes the supervisor only on transitions. Schedules sleep until
  their next due time. Roster views read no vendor history. Archived agents are
  reaped once, with backoff on failure. History search re-walks only what changed
  and reads each file through one buffered descriptor. Each `anyagents mcp` child
  runs one runtime thread over a kept-alive connection. No history is deleted.
- A credential renewal never holds an agent's mail: a profile change takes
  effect at the agent's next idle point while mail keeps flowing.
- Record outcomes beside usage (ADR 0008). A PR watch's merge, close or review
  failure, and a verdict an ancestor or the operator records (`agents` `verdict`,
  `anyagents verdict`), become `outcomes` rows; reads through the `skills` tool
  become `skill_reads` with the digest of the bytes read. `GET /v1/outcomes`,
  `agents` `outcomes` and `anyagents outcomes` report successes, recovery after
  rework and cost per successful outcome by harness and model, role, profile,
  agent or skill. The parent reminder for idle task sub-agents now asks for a
  verdict. Usage docs no longer claim a price table or OpenTelemetry export.
- Remote (cloud) agents call back with their own short-lived credential. Each
  launch of a cloud harness or remote sandbox begins a remote session whose token
  is bound to that agent and session, stored as a SHA-256 digest, rotated before
  it expires (`remote_session_ttl_secs`, default one hour), and revoked on pause,
  archive, relaunch or `anyagents agents revoke-remote`. The long-lived local
  capability no longer leaves the host. Callback URLs derive from `public_url`
  under `[server]` in `<data>/anyagents.toml`; without it a remote agent is not
  created or started. Remote sessions get only the `remote` MCP profile, which
  now holds coordination, `memory` and `skills`: `browser`, `computer`, `device`,
  `media`, `web_search` and `web_fetch` run on or from the control-plane host and
  are no longer in it. The public listener's `/`, `/healthz` and
  `/v1/webhooks/healthz` return `{"ok":true}`; the serving identity stays on
  `anyagents.sock`.
- A separate public listener (`public_listen` under `[server]`) serves only
  readiness, Telegram webhooks and MCP for remote session tokens, and 404 for
  everything else; once it is configured, the local TCP listener defaults to
  loopback. Without `public_listen` nothing changes. An agent that runs off
  this host (a cloud harness or a remote-spawn sandbox) can create or move
  agents only onto cloud harnesses, and its remote session token stops
  authenticating once its harness moves back onto the host.

## 0.1.3

### Added

- Serving-process image identity in health probes: embedded version and source
  SHA, a once-computed executable hash, and admin-only PID and start time.
- Permissioned, audited conversation redaction through the API, CLI and MCP,
  preserving transcript sequence numbers and recording each erasure operation.

### Fixed

- Native context compaction stays within the active model budget, including
  tool-output overhead, so oversized requests do not wedge agent turns.
- Group ownership and membership permissions are enforced consistently.
- HTTP fallback provider keys stay out of child-process environments. Provider
  key environment aliases share one implementation.
- OpenCode port handoff and Telegram batch checkpoint fixtures use deterministic
  synchronization; an intermittent browser clipboard startup fixture is
  quarantined rather than retried.
- The completed-turn restart fixture waits for durable turn completion before
  killing the vendor process; usage accounting alone is not completion.
- The native retry fixture waits for completed turns before checking success or
  persistent failure, preserving exact retry-request and error assertions.

### Release verification

- Publication is bound to the release commit's exact successful push/main CI;
  package metadata and all platform identity files are read back after publish.
- The [release CI audit](docs/releases/v0.1.3-ci-audit.json) records all twelve PRs
  merged after v0.1.2 through source `648de67161412e2bda27b70f6f2239b57d23ef77`.
- #113 already shipped in v0.1.2's merge-queue base. #108, #115, #120, #121 and
  #122 were not merged at this cut and are not included.

### Included pull requests

- [#110](https://github.com/SylphxAI/anyagents/pull/110): fix: bind releases to exact successful main CI.
- [#111](https://github.com/SylphxAI/anyagents/pull/111): Enforce group ownership and membership permissions.
- [#112](https://github.com/SylphxAI/anyagents/pull/112): fix(auth): remove provider keys from curl environment.
- [#106](https://github.com/SylphxAI/anyagents/pull/106): feat: expose serving process identity in health probes.
- [#116](https://github.com/SylphxAI/anyagents/pull/116): Test OpenCode handoff collisions with owned sockets.
- [#118](https://github.com/SylphxAI/anyagents/pull/118): test(browser): quarantine intermittent clipboard fixture startup.
- [#119](https://github.com/SylphxAI/anyagents/pull/119): refactor(auth): share provider key environment aliases.
- [#117](https://github.com/SylphxAI/anyagents/pull/117): Keep native context compaction within the model budget.
- [#92](https://github.com/SylphxAI/anyagents/pull/92): feat: add audited conversation redaction.
- [#114](https://github.com/SylphxAI/anyagents/pull/114): fix(test): wait for the Telegram batch checkpoint.
- [#126](https://github.com/SylphxAI/anyagents/pull/126): test(supervisor): synchronize the completed-turn restart fixture.
- [#128](https://github.com/SylphxAI/anyagents/pull/128): test(sylphx): await retry turn completion before assertions.

## 0.1.2

### Added

- Explicit local or cloud workspace placement, isolated worktree lifecycles,
  and nested agent collaboration within supervised scopes.
- Mobile browser takeover controls, persistent takeover sessions, and native
  clipboard and keyboard shortcuts.
- Conditional GitHub REST probes shared across schedule watches.

### Fixed

- Conversation-history reads remain within the caller's supervised tree.
- Recovery stays behind undelivered mailbox entries. Error notification fixtures
  control rollout freshness, and HTTP fallback credentials stay out of argv.
- OpenCode agents keep independent service ports across restarts, replace
  occupied ports, and retry transient bind collisions without discovering a
  shared managed service.
- Agent metadata and runtime refreshes preserve scope. Archived agents resume
  only for intentional direct messages; scheduled messages wake idle harnesses.
- Pending harness, model and profile switches recover faulted turns. Agent and
  lifecycle locks are explicitly released even while a harness child is alive.
- Sylphx context compaction respects active model limits, preserves tool pairs,
  and recovers from transient transport failures. Profile refresh, quota
  balancing, execution placement and sandbox lease handling are corrected.
- Telegram messages retain group quote context and media attachments.
- Archive cleanup stays outside mailbox reconciliation. Archive and runtime
  switch fixtures now control their startup and replay ordering; one
  nondeterministic partial-cleanup fixture is quarantined.

### Included pull requests

- [#59](https://github.com/SylphxAI/anyagents/pull/59): fix(profiles): centralize anthropic oauth refresh and strip seat refresh tokens.
- [#60](https://github.com/SylphxAI/anyagents/pull/60): fix(sylphx): persist workspace placement and preserve delegated execution.
- [#61](https://github.com/SylphxAI/anyagents/pull/61): fix(sylphx): preserve function pairs across context boundaries.
- [#62](https://github.com/SylphxAI/anyagents/pull/62): fix(sylphx): sanitize orphaned tool call outputs after window stride and recover transient gateway errors.
- [#63](https://github.com/SylphxAI/anyagents/pull/63): fix(sylphx): fix cloud sandbox exec idempotency, exit-0 sentinel, and lease lifecycle.
- [#64](https://github.com/SylphxAI/anyagents/pull/64): fix(sylphx): drop raw orphan tool outputs in window projection without mutating transcript.
- [#65](https://github.com/SylphxAI/anyagents/pull/65): fix(sandbox): hydrate operator seat groups and include both seat gids on migration.
- [#66](https://github.com/SylphxAI/anyagents/pull/66): fix(sylphx,signin): single-writer admin delivery, gateway pacing, and agy sign-in completion.
- [#67](https://github.com/SylphxAI/anyagents/pull/67): fix(profiles): balance agents evenly across healthy profiles with blended 5h/7d quota.
- [#68](https://github.com/SylphxAI/anyagents/pull/68): fix(telegram,store): preserve group quote context and media attachments across multi-bot relays.
- [#69](https://github.com/SylphxAI/anyagents/pull/69): fix(schedule): keep github:pr watches active across check/queue transitions until merged or closed.
- [#70](https://github.com/SylphxAI/anyagents/pull/70): fix(mcp): keep async tools alive and separate spawn instructions.
- [#71](https://github.com/SylphxAI/anyagents/pull/71): feat: remove cloud-sandbox hybrid mode and enforce scoped agent hierarchy messaging.
- [#72](https://github.com/SylphxAI/anyagents/pull/72): fix(sylphx,server): honor local workspace placement and allow multi-level hierarchy messaging.
- [#73](https://github.com/SylphxAI/anyagents/pull/73): fix(server): reap subagent .gemini/tmp caches, safe.directory git worktrees, and dirty build outputs.
- [#74](https://github.com/SylphxAI/anyagents/pull/74): feat(workspace): enforce per-agent isolated workdir lifecycle and full archive reaping.
- [#75](https://github.com/SylphxAI/anyagents/pull/75): fix: allow idle local harnesses to receive scheduled wakes.
- [#79](https://github.com/SylphxAI/anyagents/pull/79): feat(browser): make takeover usable on phones.
- [#76](https://github.com/SylphxAI/anyagents/pull/76): feat(sylphx,telegram): sync local MoE topology, XML message envelope, and remote model context window updates.
- [#80](https://github.com/SylphxAI/anyagents/pull/80): fix(send): resume archived agents only for intentional direct sends.
- [#77](https://github.com/SylphxAI/anyagents/pull/77): fix(sylphx): compact against active model limits before dispatch.
- [#81](https://github.com/SylphxAI/anyagents/pull/81): fix(archive): keep cleanup out of mailbox reconciliation.
- [#82](https://github.com/SylphxAI/anyagents/pull/82): fix(agents): preserve scope across metadata and runtime refreshes.
- [#84](https://github.com/SylphxAI/anyagents/pull/84): fix(supervisor): apply pending runtime switches to faulted turns.
- [#85](https://github.com/SylphxAI/anyagents/pull/85): fix(opencode): reserve stable private service ports per agent.
- [#83](https://github.com/SylphxAI/anyagents/pull/83): fix(browser): preserve takeover sessions during state publication.
- [#86](https://github.com/SylphxAI/anyagents/pull/86): fix(opencode): preserve transient bind collision evidence.
- [#93](https://github.com/SylphxAI/anyagents/pull/93): fix(test): isolate archive maintenance startup from reconciliation.
- [#88](https://github.com/SylphxAI/anyagents/pull/88): Release agent flock explicitly when its guard drops.
- [#97](https://github.com/SylphxAI/anyagents/pull/97): fix(test): complete the runtime switch replay control.
- [#96](https://github.com/SylphxAI/anyagents/pull/96): test: quarantine nondeterministic archive partial-cleanup fixture.
- [#102](https://github.com/SylphxAI/anyagents/pull/102): fix(core): release lifecycle flock when its guard drops.
- [#104](https://github.com/SylphxAI/anyagents/pull/104): test: quarantine Chromium focus startup race.
- [#87](https://github.com/SylphxAI/anyagents/pull/87): Restrict conversation history to the caller’s supervised tree.
- [#105](https://github.com/SylphxAI/anyagents/pull/105): fix(supervisor): keep recovery behind undelivered mail.
- [#99](https://github.com/SylphxAI/anyagents/pull/99): fix(schedule): share conditional REST probes for GitHub watches.
- [#100](https://github.com/SylphxAI/anyagents/pull/100): fix(test): control rollout freshness in error notification fixture.
- [#107](https://github.com/SylphxAI/anyagents/pull/107): docs: describe the required merge queue.
- [#101](https://github.com/SylphxAI/anyagents/pull/101): fix(auth): keep HTTP fallback credentials out of argv.
- [#91](https://github.com/SylphxAI/anyagents/pull/91): feat(browser): support native takeover clipboard and keyboard shortcuts.
- [#113](https://github.com/SylphxAI/anyagents/pull/113): Test OpenCode port reuse with owned reservations (merge-queue base).

### Verification and exclusions

- #59–#66 merged before the ci-ok gate; verified by the release candidate run.
  The [release CI audit](docs/releases/v0.1.2-ci-audit.json) records each final
  PR head, merge SHA, merge time and available exact-head ci-ok receipt.
- #89's idle cache reaper, archetypes, list query/pagination and archive guard
  are not shipped: #98 reverts #89 after independent review reproduced three
  data-deletion defects. Subsequent unrelated changes remain included.

## 0.1.1

### Added

- Interactive browser takeover for CAPTCHA and two-factor authentication.
- Configurable sub-agent defaults and automatic idling for the Sylphx harness.
- Sylphx context editing, parallel tool execution, patch application and richer
  code tools, with shared and workspace guidance layered together.
- Core SDK session transfer, cloud-first fallback and quota synchronization.

### Changed

- Conversation history uses lossless lexical and structured search. SQLite
  readers use WAL without the writer lock, and search filters run in FTS5.
- Telegram replies use native rich messages. Sylphx prompt caching and usage
  records include cache-read telemetry.
- Rust packages inherit one workspace release version, kept in step with the
  npm launcher and its four platform packages.

### Fixed

- Large Sylphx conversations no longer exceed the operating system's argument
  limit when HTTP requests use the curl fallback. Request bodies stream through
  stdin with their original bytes and HTTP status preserved, including early
  server refusals.
- Transient Sylphx service failures retry without discarding the conversation;
  patch recounting and fuzzy matching are supported, and usage is flushed on stop.
- Dollar signs are escaped in Telegram rich messages.

## 0.1.0

Initial release, published on 2026-10-30.

### Added

- **A durable per-agent mailbox.** Every message goes through an ordered,
  idempotent log with a sequence number and one delivery cursor, and the
  delivery loop resumes cleanly across a restart. The guarantees are covered by
  a crash-injection suite, not by convention.
- **One local server, one binary.** `anyagents serve` is a single long-lived
  process with one embedded database, exposed on a Unix socket owned by the
  user. The command line, the per-agent MCP server and channels are all clients
  of that same local API.
- **Harness adapters.** Adapters for Claude Code, Codex, OpenCode, Antigravity
  and OMP, plus the installer that fetches a harness from its vendor's official
  channel and streams the vendor's own sign-in flow into the AnyAgents client.
- **A supervisor.** Each agent is one supervised vendor process; the supervisor
  keeps it running and records usage per turn.
- **Agent-to-agent messaging over MCP.** The `send`, `inbox` and `agents` tools
  let every agent on a server list the others and message them, with no external
  channel needed.
- **Telegram channel.** Attach an agent to a Telegram bot, send and receive chat
  through it, and refuse an ambiguous sender.
- **The protocol gateway.** A loopback HTTP server that speaks the Anthropic,
  OpenAI Chat Completions and OpenAI Responses APIs and translates between them,
  so in API-key mode any model runs in any harness. Requests route to the
  provider stored for the chosen model.
- **The npm launcher.** `@sylphx/anyagents` runs the native binary for the
  current platform, with one platform package per supported target.

### Changed

- **The switchboard no longer models a harness's turns.** A line is delivered
  when the harness's own input or steer API takes it, and that is the only
  cursor. OpenCode, Codex, Claude Code and Muse all take a line mid-turn, so
  every one of them steers; only Antigravity and the contract double wait for a
  turn to end. There is no turn-end detection, no answer scraping and no reply
  written back into the agent's own inbox: an agent replies with its MCP `send`.
- **OpenCode agents drain continuously.** The adapter wrote one prompt per turn
  and waited for a turn it could not reliably observe the end of, so its mailbox
  stalled after the first turn. Every line is now a prompt with
  `delivery: "steer"`, accepted when the prompt API answers.
- **One cursor, migrated.** `mailbox_cursors` collapses to `delivered_seq`, the
  higher of the old two cursors; the `processed_at_delivery` flag and the
  `failed_turns` table are dropped, and the stale `answer:<id>` rows are
  removed.

### Fixed

- **An OpenCode agent's mailbox stalled after its first turn or two.** The
  adapter wrote one prompt per turn and waited for a turn end it could not
  observe, so every later row stayed `received` and a server restart did not
  clear it. Every line is now a prompt with `delivery: "steer"`, taken when the
  prompt API answers, so the session's own inbox decides whether it joins the
  running turn or starts the next one.
- **Shutdown no longer abandons a Telegram long poll.** A held `getUpdates`
  blocks the bot, so the server now releases every bot's poll before it stops
  (one call with the shortest timeout and no offset), and each bot's thread
  joins only after its last offset is written. A poller is never left behind
  polling beside the next server.

### Changed

- **One authenticated capability per agent session.** The server mints a
  capability for each agent instance and keeps only its hash; the tool server
  read the agent's name from its config before, so any process on the machine
  could send as anyone. A replaced session's capability stops working at once,
  and an agent can no longer create agents, sign a harness in or mint a
  capability: those are the operator's own token.
- **Each message carries its trusted context.** Every harness shows its model
  who wrote the message, through which channel, in which chat and topic, the
  reply a bare answer names and where it goes, and what it quotes. A bare
  answer stays in the conversation: before this, an answer to a group message
  could be posted to the author's own chat, and a quote was dropped.
- **Telegram quotes are kept.** A message that quotes another keeps the quoted
  author, text and ids, fenced so a model cannot forge them.
- **A named send is one send.** A retry after a lost answer replays its receipt
  instead of delivering twice, and two identical messages stay two.
- **The OS seat is asserted, not assumed.** A server with an operator token
  starts only when each agent has its own uid and the launcher, and
  `anyagents agent-run` checks the effective uid, `NoNewPrivs` and the denied
  paths before any command runs.

### Not yet

- Telegram sign-in and the MCP registry entry.
- A TUI or GUI (the API is designed for them).
