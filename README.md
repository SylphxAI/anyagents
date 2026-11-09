# AnyAgents

Run coding agent harnesses as long-lived agents on your own machine, container or cluster. AnyAgents orchestrates external **Spiron** (`--harness spiron`), Claude Code (`claude-code`), Codex (`codex`), OpenCode (`opencode`), Antigravity (`agy`), Muse (`muse`), Oh My Pi (`omp`), DeepSeek Harness (`dsh`) and Grok CLI (`grok`). It owns durable mailboxes, supervision, groups, schedules, channel delivery, MCP access and usage accounting. Each external harness owns its model/tool loop, context windows and memory policy.

The legacy `sylphx` harness name selects the same external `spiron` adapter. Existing agent IDs, worktrees, mailbox cursors, conversations and schedules do not change. Install a Spiron release supporting `spiron.headless/v1` on `PATH`, or set `ANYAGENTS_SPIRON_BIN` to that executable. AnyAgents does not bundle an LLM harness. The design is in [docs/](docs/).

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

- **External harnesses, one coordinator.** Run `spiron`, `claude-code`, `codex`, `opencode`, `agy`, `muse`, `omp`, `dsh` and `grok` side-by-side in the same agent organization. `sylphx` remains a compatibility alias for `spiron`.
- **Vendor-owned sign-in.** AnyAgents launches each harness's sign-in command in its isolated configuration directory. Spiron owns its authentication and model runtime; AnyAgents supplies the authenticated MCP connection to its coordinator.
- **Any harness, any model.** Pick the harness, the model and how you pay separately. With a Sylphx AI key or provider API key, any model runs in any harness through the built-in protocol gateway. With a subscription, the vendor's own tool runs the vendor's own models.
- **Your agents keep their state.** Identity, instructions, history, mailbox, groups and channels live in AnyAgents, so an agent can change harness or model without losing any of it.
- **Nothing gets lost, and messages steer live.** Every message goes through a durable per-agent mailbox with ordered, idempotent delivery, visible lag and real-time mid-turn steering into active sessions. The guarantees are tested with crash injection.
- **Zero-config local tools & clean sandbox boundaries.** In local mode (default), agents run under your own user and keep your `$HOME`, CLI tools and hardware while isolating each agent's harness state and `mcp-capability`. Workspace execution belongs to the selected harness; AnyAgents passes the preserved worktree path and MCP configuration to that process.
- **Audited erasure for accidental captures.** Ancestors and admins can redact a body/span without changing message ids, order or cursors, including managed JSONL session copies. See [redaction](docs/redaction.md) for permissions, selectors and incomplete-copy receipts.
- **Groups have explicit owners.** New groups belong to their creator; existing groups without an owner remain operator-owned. Members may invite agents to private groups but cannot change visibility or evict unrelated members. `public` on a join only configures a new group; use `members` with `public` to change an existing group as its owner, the owner’s ancestor, an operator or a root admin.
- **Agents talk to each other.** Agents can list peers, join internal groups, spawn subagents and message reachable agents. Conversation reads are limited to the caller and its descendants; `agent: "all"` searches only that tree. Reading a parent, sibling, public peer or fellow group member’s private history is refused. Explicit operator and root-admin reads are unrestricted and audited.
- **One place for cost.** Tokens, cost and quota state per agent, harness, model and day.

## How it works

`anyagents serve` is one long-lived server. The CLI, an MCP server registered into each agent, and channels such as Telegram are its clients. Each agent is one vendor process in that vendor's programmatic mode, supervised by the server.

- [Vision](docs/vision.md): goal, first-version scope, boundaries.
- [Architecture](docs/architecture.md): state, the three axes, harnesses, the mailbox and its guarantees, runtime boundaries, channels, the gateway, usage.
- [Verification](docs/verification.md): reproducible scripted harness checks, coverage and live-acceptance boundaries.
- [Serving identity](docs/serving-identity.md): read back the version, source, binary hash and PID on both server sockets.
- [Desktop and mobile control](docs/computer-and-device.md): OS backends, Android/iOS setup, semantic snapshots and stale-ref handling.
- [Decisions](docs/adr/): [0001](docs/adr/0001-server-first-one-binary.md) (server-first single binary) and [0002](docs/adr/0002-runtime-drivers-and-capability-identity.md) (capability identity and runtime boundaries).

## Licence

[MIT](LICENSE)
