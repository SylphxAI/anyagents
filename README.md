# AnyAgents

Run coding agent harnesses as long-lived agents on your own machine, container or cluster. AnyAgents orchestrates external **Spiron** (`--harness spiron`), Claude Code (`claude-code`), Codex (`codex`), OpenCode (`opencode`), Antigravity (`agy`), Muse (`muse`), Oh My Pi (`omp`), DeepSeek Harness (`dsh`) and Grok CLI (`grok`). It owns durable mailboxes, supervision, groups, schedules, channel delivery, MCP access and usage accounting. Each external harness owns its model/tool loop, context windows and memory policy.

The legacy harness names `sylphx`, `sylphx-agent`, `sylphx_agent` and `sylphx/default` resolve to `spiron`; opening an existing `state.db` automatically migrates those agent harness selectors. Existing agent IDs, worktrees, mailbox cursors, conversations and schedules do not change. Sylphx model provider IDs and credential profiles (such as `sylphx/openai/gpt-6.1-sol#medium` and `sylphx/default`) remain unchanged. Install a Spiron release supporting `spiron.headless/v1` on `PATH`, or set `ANYAGENTS_SPIRON_BIN` to that executable. AnyAgents does not bundle an LLM harness. The design is in [docs/](docs/).

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

## Three Orthogonal Axes: Harness × Profile × Sandbox

AnyAgents is a **100% vendor-neutral multi-agent orchestrator**. It never implements an LLM loop; instead, each agent composes three independent axes:

1. **Harness (`--harness`) — Who runs the single-agent loop:** Local CLIs (`spiron`, `claude-code`, `codex`, `opencode`, `agy`, `muse`, `omp`, `dsh`, `grok`) and Cloud Harnesses (`spiron-cloud` at `https://spiron.ai`, `claude-cloud`, `openai-cloud`, `cursor-cloud`).
2. **Profile (`--profile`) — Whose credentials or subscription pays:** Peer provider pools (`anthropic`, `openai`, `google-antigravity`, `muse`, `openrouter`, `sylphx`, Coding Plans) live under `<data>/profiles/<provider>/<profile>`. Binding a profile without a target harness mapping **fails closed** instead of silently falling back to an unauthenticated free tier.

   - **Model catalogue.** Create, spawn and harness/model switches refuse a harness/model/profile triple whose profile does not serve the model. The catalogue is read from the profile's own provider model list with the profile's own credentials and endpoint, snapshotted on success (reused without a network read for 10 minutes, accepted for 24 hours when a live read fails); with neither a live read nor a valid snapshot the triple is refused. An optional `models` array in the profile's `profile.json` overrides the live list. Harness-written model caches are never treated as a catalogue. `anyagents profiles catalogue [<provider>[/<profile>]]` reads and snapshots catalogues locally and fails when any stays unknown.
   - **Default profile.** With no explicit profile, `<data>/profile-bindings.json` decides when present: `{"bindings": [{"harness": "spiron", "model": "lab/model-a", "profiles": ["provider/a", "provider/b"], "selection": "quota"}, {"harness": "spiron", "profiles": ["provider/c"]}]}`. A model-specific binding wins over a harness-only one; `selection: "ordered"` (default) takes the first eligible candidate, `"quota"` picks the best live remaining quota per currently assigned agent and refuses when no candidate's quota is readable. Without the file the provider's own profiles are candidates (`default` first). Either way a candidate must serve the model and be authenticated.
   - **Audit.** Every harness/model/profile change records the triple before and after it and its source (`api:cli`, `api:mcp`, `api`, `cli:local`, `system`) in `admin_audit`.
3. **Sandbox (`WorkspaceBackend`) — Where files and commands execute:** Implements the `anyagents_tools::WorkspaceBackend` trait—`LocalWorkspace` (local directory or git worktree by default), `RemoteSandboxWorkspace` (Cloud Sandbox / E2B HTTP APIs), `DockerWorkspace` (`docker://<host>`), or custom embedders.

## Why

- **External harnesses, one coordinator.** Run `spiron`, `claude-code`, `codex`, `opencode`, `agy`, `muse`, `omp`, `dsh` and `grok` side-by-side in the same agent organization with isolated per-agent context windows (`arXiv:2611.05306`) and persona-neutral system envelopes (`arXiv:2611.03931`).
- **Vendor-owned sign-in.** AnyAgents launches each harness's sign-in command in its isolated configuration directory and refuses unmapped profile-to-harness combinations fail-closed.
- **Decoupled Harness × Profile × Sandbox.** Switch an agent's harness, credential profile, or sandbox substrate independently without losing its identity, mailbox cursor, history, groups, or channels.
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
