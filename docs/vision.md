# AnyAgents vision

## Goal

Run any coding agent harness as a long-lived agent on your own machine or server, and let those agents talk to people and to each other. One server installs, signs in and supervises Claude Code, Codex, OpenCode, Gemini CLI and the next harness, gives each agent its own channels, and shows what every agent costs.

AnyAgents drives each vendor's own tool. It never re-implements a vendor client and never lifts a subscription credential out of it, because the model works best in the harness it was trained in and the subscription is for that tool.

## Who it is for

People and teams who already use one or more agent harnesses and want them running all the time, reachable from Telegram (and later other channels), able to hand work to each other, and accounted for in one place.

## First version

1. **Lazy harness setup.** No harness is installed up front. Creating the first agent on a harness installs that harness in the background from the vendor's official channel, keeps it updated, and runs the vendor's own sign-in flow inside AnyAgents: the sign-in link or code appears in the AnyAgents client, so the user never opens the vendor CLI.
2. **Agents.** Create an agent on any harness in one step (Claude Code by default). A harness can run many agents. Harness, model and auth are separate choices: with an API key any model runs in any harness through the built-in protocol gateway; with a subscription, the vendor's own models run in the vendor's own tool.
3. **AnyAgents owns the state.** An agent's identity, instructions, history, mailbox, channels and credentials live in AnyAgents, so an agent can switch harness or model and keep its history and its channels.
4. **Channels.** Each agent can be attached to channels, or to none. The first version ships Telegram, behind a channel adapter interface that later channels implement.
5. **Agents see each other.** Every agent on a server can list the others and message them through one `send` tool, with no external channel needed.
6. **Cost and usage.** Tokens, reported cost and quota state per agent, harness, model and day.

## Boundaries

- Server first. `anyagents serve` is the product. The CLI, the MCP server, a future TUI or GUI, and every channel are clients of the same API.
- One Rust binary is the server, the CLI and the MCP server. The npm package is a launcher for that binary.
- Local by default: the API listens on a Unix socket owned by the user. Network access is opt-in and needs a token.
- Vendor sign-in credentials live in a config directory AnyAgents owns, written and read only by the vendor tool. AnyAgents never reads, sends or logs the token, and never routes a subscription through the gateway.
- Stability is the bar. Every delivery guarantee in [architecture.md](architecture.md#guarantees) is a tested invariant, not a convention.

## Not in the first version

- Running a model. (The protocol gateway calls model providers only in API-key mode, with the user's key.)
- Channels other than Telegram.
- A TUI or GUI (the API is designed for them).
- Several hosts sharing one server, and sandboxing agents from each other.

## Target metrics

- No message lost or delivered twice across a server crash, measured by the crash-injection suite.
- Delivery lag (receive to harness accept) under one second for a harness that accepts input during a turn; the lag of every other harness is reported, never hidden.
- Creating an agent on a harness that is not yet installed, through to its first answer, in one command and one sign-in.

## Where things live

- Design: [architecture.md](architecture.md), decisions in [adr/](adr/).
- API schema: `proto/anyagents/v1/` (the one source for the HTTP/JSON and gRPC API, the CLI and the MCP tools).
