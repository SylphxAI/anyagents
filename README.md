# AnyAgents

Run any coding agent harness as a long-lived agent on your own machine, container or cluster. AnyAgents installs and signs in Claude Code, Codex, OpenCode, Antigravity (`agy`), Muse, Oh My Pi (`omp`), DeepSeek Harness (`dsh`), Grok CLI (`grok`) and more on demand, keeps every agent running with real-time mid-turn steering, connects each one to Telegram, lets agents collaborate in groups or spawn subagents, and tracks usage and cost in one place.

> Status: early development. Working today: the durable per-agent mailbox with ordered, idempotent delivery and real-time FIFO steering, the server on a Unix socket (plus optional HTTP webhook listener), the `send`, `interact`, `inbox`, `conversation`, `agents`, `groups`, `spawn`, `archive` and `wake` MCP tools, a Telegram channel adapter (long polling and instant webhook push, DMs, supergroups, forum topics, reactions, polls, media and checklists), native adapters for `claude-code`, `codex`, `opencode`, `agy`, `muse`, `omp`, `dsh` and `grok`, a supervisor that keeps each running agent alive and auto-wakes archived agents on message arrival, per-turn usage accounting, and the built-in protocol gateway crate for API-key routing across Anthropic Messages and OpenAI Chat/Responses protocols. The design is in [docs/](docs/).

## Why

- **Any harness, any model.** Pick the harness, the model and how you pay separately. With an API key, any model runs in any harness through the built-in protocol gateway. With a subscription, the vendor's own tool runs the vendor's own models.
- **Your agents keep their state.** Identity, instructions, history, mailbox, groups and channels live in AnyAgents, so an agent can change harness or model without losing any of it.
- **Nothing gets lost, and messages steer live.** Every message goes through a durable per-agent mailbox with ordered, idempotent delivery, visible lag and real-time mid-turn steering into active sessions. The guarantees are tested with crash injection.
- **Zero-config local tools & clean sandbox boundaries.** In local mode (default), agents run under your own user and keep your `$HOME`, CLI tools and hardware while isolating each agent's harness state and `mcp-capability`. In Docker, Kubernetes or multi-seat hosts, external credentials and devices follow standard container, Pod or OS group primitives—with no custom credential broker inside AnyAgents ([ADR 0002](docs/adr/0002-runtime-drivers-and-capability-identity.md)).
- **Agents talk to each other.** Every agent on a server can list peers, join internal groups, read conversations, spawn subagents and message them directly.
- **One place for cost.** Tokens, cost and quota state per agent, harness, model and day.

## How it works

`anyagents serve` is one long-lived server. The CLI, an MCP server registered into each agent, and channels such as Telegram are its clients. Each agent is one vendor process in that vendor's programmatic mode, supervised by the server.

- [Vision](docs/vision.md): goal, first-version scope, boundaries.
- [Architecture](docs/architecture.md): state, the three axes, harnesses, the mailbox and its guarantees, runtime boundaries, channels, the gateway, usage.
- [Decisions](docs/adr/): [0001](docs/adr/0001-server-first-one-binary.md) (server-first single binary) and [0002](docs/adr/0002-runtime-drivers-and-capability-identity.md) (capability identity and runtime boundaries).

## Licence

[MIT](LICENSE)
