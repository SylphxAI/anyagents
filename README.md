# AnyAgents

Run any coding agent harness as a long-lived agent on your own machine or server. AnyAgents installs and signs in Claude Code, Codex, OpenCode, Gemini CLI and more on demand, keeps every agent running, connects each one to Telegram, lets agents message each other, and shows what each one costs.

> Status: early development, nothing released yet. Working today: the durable per-agent mailbox with its delivery guarantees and tests, the server on a Unix socket, the `send`, `inbox` and `agents` MCP tools, the Claude Code and Codex adapters, a supervisor that keeps each running agent alive, usage recorded per turn, and the protocol gateway crate, which routes a harness's Anthropic Messages or OpenAI Chat requests to the stored provider and translates messages, tools and streaming events between the two. Not yet: the server starting the gateway for harnesses, harness install and sign-in, and Telegram. The design is in [docs/](docs/).

## Why

- **Any harness, any model.** Pick the harness, the model and how you pay separately. With an API key, any model runs in any harness through the built-in protocol gateway. With a subscription, the vendor's own tool runs the vendor's own models.
- **Your agents keep their state.** Identity, instructions, history, mailbox and channels live in AnyAgents, so an agent can change harness or model without losing any of it.
- **Nothing gets lost.** Every message goes through a durable per-agent mailbox with ordered, idempotent delivery and visible lag. The guarantees are tested with crash injection.
- **No setup up front.** A harness is downloaded the first time an agent needs it, and its sign-in happens inside AnyAgents.
- **Agents talk to each other.** Every agent on a server can list the others and message them, with or without an external channel.
- **One place for cost.** Tokens, cost and quota state per agent, harness, model and day.

## How it works

`anyagents serve` is one long-lived server. The CLI, an MCP server registered into each agent, and channels such as Telegram are its clients. Each agent is one vendor process in that vendor's programmatic mode, supervised by the server.

- [Vision](docs/vision.md): goal, first-version scope, boundaries.
- [Architecture](docs/architecture.md): state, the three axes, harnesses, the mailbox and its guarantees, channels, the gateway, usage.
- [Decisions](docs/adr/).

## Licence

[MIT](LICENSE)
