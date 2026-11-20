# AnyAgents vision

## Goal

Run any coding agent harness as a long-lived agent on your own machine or server, and let those agents talk to people and to each other. One server installs, signs in and supervises Claude Code, Codex, OpenCode, Gemini CLI and the next harness, gives each agent its own channels, and shows what every agent costs and what its work achieved. Those outcomes are the evidence from which skills, memory and the choice of harness and model are to improve ([ADR 0008](adr/0008-quality-loop.md)).

AnyAgents is the master: the control plane for every harness, wherever each one runs, whether on this machine, a server, or a remote sandbox per agent. It owns profiles, identity, mailboxes, topology, schedules and the tool layer. Code Mode supplies every tool, including the file system and shell, and AnyAgents adds coordination, memory, skills and web. A vendor harness is the compliant channel to its model: AnyAgents always drives the vendor's own binary, because a subscription is for that tool. It disables the harness's built-in tools in favour of its own, and it never impersonates the vendor's client. See [ADR 0009](adr/0009-product-boundary.md) for where the core ends, which integrations are optional, and what belongs to a deployment.

## Name and position

AnyAgents is one home for every agent: any harness, any model, any channel. The name continues our open-source AnyMD family, so the brand stays consistent across our open-source tools. The npm name `anyagents` belongs to someone else, so the launcher package is `@sylphx/anyagents`.

AnyAgents is **100% vendor-neutral** and separates orchestration into orthogonal axes (`Harness × Runtime × Profile`):

1. **AnyAgents (`@sylphx/anyagents`, OSS Orchestrator):** Owns durable multi-agent identity, mailboxes, topology, groups, schedules, spend budgets, and MCP coordination. AnyAgents never implements an LLM loop and carries zero hardcoded lock-in to any single provider.
2. **Harnesses (`spiron`, `claude-code`, `codex`, `opencode`, `agy`, `muse`, `omp`, `dsh`, `grok`, `cursor`):** External single-agent runtimes composed cleanly across orthogonal execution runtimes (`local`, `sandbox[:<provider>]`, `cloud`, or shorthand `harness@runtime`). Spiron embeds the shared capability core natively, so AnyAgents mounts a coordination-only MCP profile without duplicating built-in tools.
3. **Runtimes, Profiles & Sandboxes:** Credential profiles (`anthropic`, `openai`, `google-antigravity`, `muse`, `openrouter`, `sylphx`, and Coding Plans) are peers with fail-closed harness mapping checks; execution plugs into host processes (`local`), `WorkspaceBackend` / `SandboxProvider` sandboxes (`sandbox`), or native ADR 0010 D7 in-process remote sessions (`RemoteSessionProvider` / `SpironCloudProvider` for `spiron@cloud` at `https://api.spiron.ai`, replacing the deprecated `cloud_harness.rs` stdio shim, plus serverless `CloudTurnDriver` / `GenericHttpCloudDriver`).

Peer-to-peer messaging between people's agent sessions, such as Reason Network, overlaps with one part of AnyAgents. AnyAgents also owns each agent's identity, history and channels, installs and signs in the harnesses, and routes any model through any harness, so an agent can change harness or model without losing either.

## Who it is for

People and teams who already use one or more agent harnesses and want them running all the time, reachable from Telegram (and later other channels), able to hand work to each other, and accounted for in one place.

## First version

1. **Lazy harness setup.** No harness is installed up front. Creating the first agent on a harness installs that harness in the background from the vendor's official channel, keeps it updated, and runs the vendor's own sign-in flow inside AnyAgents: the sign-in link or code appears in the AnyAgents client, so the user never opens the vendor CLI.
2. **Agents.** Create an agent on any harness in one step (Claude Code by default). A harness can run many agents. Harness, model and auth are separate choices: with an API key any model runs in any harness through the built-in protocol gateway; with a subscription, the vendor's own models run in the vendor's own tool.
3. **AnyAgents owns the state.** An agent's identity, instructions, history, mailbox, channels and credentials live in AnyAgents, so an agent can switch harness or model and keep its history and its channels.
4. **Channels.** Each agent can be attached to channels, or to none. The first version ships Telegram, behind a channel adapter interface that later channels implement.
5. **Agents see each other.** Every agent on a server can list the others and message them through one `send` tool, with no external channel needed. Every session is authenticated as exactly one agent, and an answer names the message it answers, so a group conversation stays in its group and its topic.
6. **Cost and usage.** Tokens, reported cost and quota state per agent, harness, model and day.
7. **Outcomes.** Merged, closed and review-failed pull requests from an agent's watches, and verdicts from its ancestors or the operator, recorded beside usage. Successes, recovery after rework and cost per successful outcome are reported per harness and model, role, profile, agent or skill.

7. **Computer use, browser included.** Each agent can have its own isolated desktop: one headless Wayland desktop with one headful browser, worked through the page (DevTools, with refs bound to their document) and through the pixels (screenshots, pointer, keyboard, clipboard), one agent at a time. A person can take over the whole desktop from any device, for a CAPTCHA or a sign-in, and hand it back. One open-source engine ([`agent-workstation`](https://github.com/SylphxAI/agent-workstation)) does this: Spiron embeds it and AnyAgents serves it to every other harness over MCP. No script changes what a site sees, and agents never bypass a CAPTCHA.

## Boundaries

- Server first. `anyagents serve` is the product. The CLI, the MCP server, a future TUI or GUI, and every channel are clients of the same API.
- One Rust binary is the server, the keeper that holds each harness process, the CLI and the MCP server. The npm package is a launcher for that binary.
- The server is a small kernel: identity, mailboxes, supervision, schedules and authorization. Harness processes live in keepers, so restarting or upgrading the server stops no agent ([ADR 0007](adr/0007-process-architecture.md)); services and connectors move out of the kernel's process one at a time, as that ADR orders them.
- Local by default: the API listens on a Unix socket owned by the user. Network access (`--listen` / `--webhook-listen`) is opt-in and authenticates every request via an operator token or per-agent `mcp-capability` (`POST /mcp` / `GET /mcp` for remote Cloud Agents).
- Three MCP mounting profiles prevent tool collisions across execution boundaries: `full` over `stdio` for local third-party CLIs (`code` + coordination + memory/skills + web + browser/computer), `remote` over HTTPS `/mcp` for closed-box third-party Cloud Agents (omitting host-local `code` so the cloud container uses its own native sandbox file/bash tools while sharing coordination, memory/skills, web, and host browser/computer), and `coordination` for `spiron` (omitting duplicate `code`/web/memory/skills so `spiron` uses its native capability core).
- Vendor sign-in credentials live in a config directory AnyAgents owns, written and read only by the vendor tool. AnyAgents never reads, sends or logs the token, and never routes a subscription through the gateway.
- External tool credentials and hardware belong to the runtime environment (`local` host `$HOME`, Docker container volumes/env, or Kubernetes `Secret` / `ServiceAccount`), never to a credential broker inside AnyAgents ([ADR 0002](adr/0002-runtime-drivers-and-capability-identity.md)).
- Stability is the bar. Every delivery guarantee in [architecture.md](architecture.md#guarantees) is a tested invariant, not a convention.

## Not in the first version

- Running a model. (The protocol gateway calls model providers only in API-key mode, with the user's key.)
- Channels other than Telegram.
- A TUI or GUI (the API is designed for them).
- Several hosts sharing one server.
- Session desktops on macOS or Windows hosts (they need Linux), and a device app for controlling a person's own computer through its accessibility APIs; host desktop control works as it does today until that app replaces it.
- An internal credential broker or custom container runtime: container (`docker`) and cluster (`k8s`) sandboxes use standard OCI and Kubernetes primitives ([ADR 0002](adr/0002-runtime-drivers-and-capability-identity.md)).

## Target metrics

- No message lost or delivered twice across a server crash, measured by the crash-injection suite.
- A server restart, crash or upgrade interrupts no running turn, measured by the keeper process tests and the server restart test.
- Delivery lag (receive to harness accept) under one second for a harness that accepts input during a turn; the lag of every other harness is reported, never hidden.
- Creating an agent on a harness that is not yet installed, through to its first answer, in one command and one sign-in.

## Where things live

- Design: [architecture.md](architecture.md), decisions in [adr/](adr/).
- API schema: `proto/anyagents/v1/` (the one source for the HTTP/JSON and gRPC API, the CLI and the MCP tools).
