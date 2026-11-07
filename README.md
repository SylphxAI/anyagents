# AnyAgents

Run any coding agent harness as a long-lived agent on your own machine, container, cluster, or Sylphx Cloud Sandbox. AnyAgents ships a built-in native **Sylphx** harness (`--harness sylphx`) and installs and signs in Claude Code (`claude-code`), Codex (`codex`), OpenCode (`opencode`), Antigravity (`agy`), Muse (`muse`), Oh My Pi (`omp`), DeepSeek Harness (`dsh`), Grok CLI (`grok`) and more on demand, keeps every agent running with real-time mid-turn steering, connects each one to Telegram, lets agents collaborate in groups or spawn subagents, and tracks usage and cost in one place.

> Status: working today: the built-in `sylphx` native harness (with append-only KV-cache pinned context, autonomous `context_window_open` compaction, zero-embedding lossless lexical + structured `Raw-RAG` history search via `memory_recall` / `conversation` / `search`, Cloud Kata microVM Sandbox worktrees, and `Sylphx Agents` BaaS sessions), the durable per-agent mailbox with ordered, idempotent delivery and real-time FIFO steering, the server on a Unix socket (plus optional HTTP webhook listener), the `send`, `interact`, `inbox`, `conversation`, `agents`, `groups`, `schedule`, `browser`, `describe`, `spawn`, `archive` and `wake` MCP tools, a Telegram channel adapter (long polling and instant webhook push, DMs, supergroups, forum topics, reactions, polls, media and checklists), native adapters for `sylphx`, `claude-code`, `codex`, `opencode`, `agy`, `muse`, `omp`, `dsh` and `grok`, a supervisor that keeps each running agent alive and wakes archived agents on intentional, directly addressed sends (not system events, channel updates, schedules or broadcasts), per-turn usage accounting, and the built-in protocol gateway crate for API-key routing across Anthropic Messages and OpenAI Chat/Responses protocols. The design is in [docs/](docs/).

## Install

Prebuilt binaries are available through npm for macOS (arm64 and x64) and Linux (glibc, x64 and arm64). The npm launcher needs Node.js 18 or newer. Linux requires glibc 2.34 or newer and kernel 5.9 or newer.

```bash
npm install -g @sylphx/anyagents
anyagents --version
```

Homebrew also installs the native binary on macOS and Linux:

```bash
brew install sylphxai/tap/anyagents
anyagents --version
```

If Homebrew reports the tap as untrusted, run `brew trust --tap sylphxai/tap` and repeat the install command.

You can also download the native archives from [GitHub Releases](https://github.com/SylphxAI/anyagents/releases). Native Windows is not supported.

### Build from source

Install Rust 1.98 or newer, then build the existing CLI:

```bash
git clone https://github.com/SylphxAI/anyagents.git
cd anyagents
cargo install --locked --path crates/anyagents --bin anyagents
anyagents --version
```

## Why

- **Built-in `sylphx` harness + every third-party harness.** Start immediately with `--harness sylphx` (zero third-party CLI install required) or run `claude-code`, `codex`, `opencode`, `agy`, `muse`, `omp`, `dsh` and `grok` side-by-side in the same agent organization.
- **One `anyagents sign-in sylphx` unlocks the Sylphx Service Platform.** Authenticate once via RFC 8628 Device Flow (`https://api.sylphx.com`) to connect any agent or subagent to **Sylphx AI** (unified frontier model gateway at `/v1/responses`), **Sylphx Sandboxes** (isolated Cloud Kata microVM git worktrees via `SYLPHX_SANDBOX=cloud` or the `sandbox` tool), and **Sylphx Agents** BaaS (`/v1/{env}/agent_sessions`). When you want a 100% cloud-hosted multi-agent workspace with zero local daemon management, graduate directly to **[Spiron](https://spiron.ai)**—built on the same `Sylphx Agents` engine and coordination model.
- **Any harness, any model.** Pick the harness, the model and how you pay separately. With a Sylphx AI key or provider API key, any model runs in any harness through the built-in protocol gateway. With a subscription, the vendor's own tool runs the vendor's own models.
- **Your agents keep their state.** Identity, instructions, history, mailbox, groups and channels live in AnyAgents, so an agent can change harness or model without losing any of it.
- **Nothing gets lost, and messages steer live.** Every message goes through a durable per-agent mailbox with ordered, idempotent delivery, visible lag and real-time mid-turn steering into active sessions. The guarantees are tested with crash injection.
- **Zero-config local tools & clean sandbox boundaries.** In local mode (default), agents run under your own user and keep your `$HOME`, CLI tools and hardware while isolating each agent's harness state and `mcp-capability`. For isolated cloud execution, `--harness sylphx` provisions Cloud Kata microVM worktrees (`/home/user/workspace`) with ephemeral in-memory Git credentials (never written to disk) ([ADR 0002](docs/adr/0002-runtime-drivers-and-capability-identity.md)).
- **Groups have explicit owners.** New groups belong to their creator; existing groups without an owner remain operator-owned. Members may invite agents to private groups but cannot change visibility or evict unrelated members. `public` on a join only configures a new group; use `members` with `public` to change an existing group as its owner, the owner’s ancestor, an operator or a root admin.
- **Agents talk to each other.** Agents can list peers, join internal groups, spawn subagents and message reachable agents. Conversation reads are limited to the caller and its descendants; `agent: "all"` searches only that tree. Reading a parent, sibling, public peer or fellow group member’s private history is refused. Explicit operator and root-admin reads are unrestricted and audited.
- **One place for cost.** Tokens, cost and quota state per agent, harness, model and day.

## How it works

`anyagents serve` is one long-lived server. The CLI, an MCP server registered into each agent, and channels such as Telegram are its clients. Each agent is one vendor process in that vendor's programmatic mode, supervised by the server.

- [Vision](docs/vision.md): goal, first-version scope, boundaries.
- [Architecture](docs/architecture.md): state, the three axes, harnesses, the mailbox and its guarantees, runtime boundaries, channels, the gateway, usage.
- [Verification](docs/verification.md): reproducible scripted harness checks, coverage and live-acceptance boundaries.
- [Serving identity](docs/serving-identity.md): read back the version, source, binary hash and PID on both server sockets.
- [Decisions](docs/adr/): [0001](docs/adr/0001-server-first-one-binary.md) (server-first single binary) and [0002](docs/adr/0002-runtime-drivers-and-capability-identity.md) (capability identity and runtime boundaries).

## Licence

[MIT](LICENSE)
