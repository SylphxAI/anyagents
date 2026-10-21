# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

Nothing is released yet. This section records what has landed on `main` so far;
the first release will move it under a version heading.

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
- **Session resume on every harness.** An agent on Antigravity, Muse, Claude
  Code, Codex or OpenCode keeps the vendor session id it last ran beside its
  config and resumes it after a server or agent restart. A session the vendor
  no longer has starts a new one, and the new id is kept and the reason logged.
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

### Not yet

- Telegram sign-in, the MCP registry entry, and a published release.
- A TUI or GUI (the API is designed for them).
