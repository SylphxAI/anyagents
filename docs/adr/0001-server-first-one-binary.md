# 0001: A server that drives vendor harnesses, shipped as one Rust binary

Status: accepted. Date: 2026-10-18.

## Context

People run several agent harnesses (Claude Code, Codex, OpenCode, Gemini CLI and others). Keeping them running, reachable from a chat app, able to message each other and accounted for usually ends up as one glue script per harness. Each script reinvents the same parts (an inbox, ordering, acknowledgement, restart) and gets them subtly wrong in a different way: messages ordered by a timestamp that arrives late, an acknowledgement that skips a held message, a replay after every restart, a rate-limit answer treated as a crash, a process that looks alive while nothing is delivered.

## Decision

1. **Drive the vendor's own binary.** Each harness runs as its vendor tool in that tool's programmatic mode. AnyAgents never re-implements a vendor client. Subscription credentials stay with the vendor tool, in a config directory AnyAgents owns, and are never routed through another endpoint.
2. **Server first.** One long-lived server owns all state. The CLI, the MCP server, future TUI and GUI clients, and every channel are clients of its API.
3. **One mailbox per agent, ordered by a server sequence.** Every input is appended to the agent's mailbox and gets the next sequence number when it is durably stored. Delivery, acknowledgement and replay key on that sequence; source timestamps are metadata. One delivery cursor (received, then delivered) moves only over a contiguous prefix, and a row is done when the harness's own input or steer API has taken it. Delivery into a harness is idempotent by message id. Superseded in part: the earlier accepted/processed pair assumed the switchboard could see a harness's turns.
4. **Every harness is admitted at its best mode.** An adapter declares `steer`, `followup` or `queue`. A harness that cannot take input during a turn is supported, and its lag is shown.
5. **Harness, model and auth are separate axes.** With an API key, any model runs in any harness through a local protocol gateway that translates between the Anthropic Messages API and the OpenAI Chat Completions and Responses APIs. With a subscription, only the vendor's own models run in the vendor's own tool.
6. **One Rust binary.** The server, the CLI and the MCP server (on the official Rust SDK, `rmcp`) are one binary. An npm package launches the matching native binary, so `npx -y @sylphx/anyagents` works on every supported platform.
7. **One API schema.** The API is defined once in protobuf, resource-oriented per Google AIP; the HTTP/JSON and gRPC surfaces, the CLI and the MCP tool schemas derive from it.
8. **Embedded storage.** One SQLite database in WAL mode holds every resource, mailbox and usage record.
9. **MIT licence.**

## Consequences

- The delivery guarantees are written once, in one crate, and tested with a scripted fake harness and crash injection. A new harness inherits them by implementing five adapter calls.
- Adding a harness never touches the mailbox, channels, `send` or usage.
- Users install one thing. Vendor harnesses are installed only when an agent first needs them.
- SQLite limits a server to one host. That is the first version's scope; a shared store is a later decision.
- The protocol gateway is the largest new surface. Its correctness is held by conformance tests that replay recorded harness requests and provider streams.

## Alternatives rejected

- **Calling model APIs directly instead of running vendor harnesses.** Loses the tool each model was trained with, and breaks subscription terms.
- **One script per harness.** The failure described in the context.
- **Ordering by source timestamp.** A late message with an older time either reorders what was already delivered or is skipped by the acknowledgement cursor.
- **A TypeScript CLI and MCP server beside a Rust server.** Two languages and two copies of the API types for one product.
- **PostgreSQL.** A second server for every user to run, for a single-host tool.
- **Holding back harnesses that cannot steer.** Most useful harnesses would be excluded; declaring the mode and showing the lag is enough.
