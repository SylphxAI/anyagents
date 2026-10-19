# AnyAgents architecture

Status: design. Decisions behind it are in [adr/0001-server-first-one-binary.md](adr/0001-server-first-one-binary.md).

## The standard, and the shape

The established pattern for running vendor agents is a long-lived gateway that drives the vendor's own tool and injects input into the live run. OpenClaw names four ways to handle input that arrives during a run: steer it into the active turn, queue it as a followup for when the turn ends, interrupt the run, or collect several followups into one ([command queue](https://docs.openclaw.ai/concepts/queue)). AnyAgents follows that pattern and adds what a gateway alone does not give: a durable per-agent mailbox, harness setup and sign-in, agent-to-agent messaging, and usage accounting.

```
 clients:   CLI    MCP (one per agent)    TUI / GUI (later)    channels (Telegram, ...)
              \           |                     |                     /
               +----------+---------- API ------+--------------------+
                                       |
                                 anyagents serve
     +-------------+-------------+-------------+-------------+-------------+
     |  harnesses  |   agents    |   mailbox   |  channels   |    usage    |
     |  install,   |  create,    |  per-agent  |  adapters,  |  per-turn   |
     |  update,    |  supervise, |  log, seq,  |  relay      |  records,   |
     |  sign-in    |  adapters   |  three acks |             |  aggregates |
     +-------------+-------------+-------------+-------------+-------------+
                                       |
                    vendor processes: claude, codex, opencode, gemini, ...
```

Everything runs in one process, `anyagents serve`, with one embedded database. Each module below is one crate with one public interface; the protocol gateway is a sixth module used only in API-key mode.

## AnyAgents owns the state

An agent's identity, instructions, conversation history, mailbox, channels and credentials live in AnyAgents, never only in a vendor tool's local files. A vendor session is a working copy. So an agent can move to another harness or another model and keep its history, its mailbox and its Telegram bot.

- **Transcript.** The server records every turn from the harness stream: the input messages, the assistant text, tool calls and results as the harness reports them, and usage. This transcript, not the vendor's session file, is the agent's history.
- **Switching harness or model.** The vendor session id is kept per (agent, harness) and resumed while the agent stays on that harness. On a switch, the new harness starts a new vendor session primed from the transcript: the instructions, a summary of older turns, and the recent turns verbatim. A tool call in flight in the old harness is not carried across; the switch waits for the current turn to end or cancels it.
- **Credentials.** Each auth profile has its own config directory under the AnyAgents data directory, and the harness is started with its documented config-directory setting pointed there (for example `CLAUDE_CONFIG_DIR`, `CODEX_HOME`). The vendor tool writes its sign-in credential into that directory; AnyAgents owns where it lives, backs it up with the rest of its state, and never reads the token itself. API keys are kept in AnyAgents' secret store (below).

## Three axes: harness, model, auth

An agent is configured on three independent choices:

| Axis | Values (first version) | Notes |
| --- | --- | --- |
| Harness | `claude-code`, `codex`, `opencode`, `gemini-cli`, `omp`, and more by adapter | Installed lazily when first chosen. |
| Model | Claude, GPT, Gemini, DeepSeek, Grok, MiMo and any model a configured provider serves | Chosen separately from the harness. |
| Auth | `subscription` or `api-key` | Decides which (harness, model) pairs are valid. |

The axes are fully orthogonal only with an API key:

- **Subscription.** A vendor subscription signs in to that vendor's own tool and runs that vendor's models: a Claude subscription in Claude Code runs Claude models; a ChatGPT sign-in in Codex runs OpenAI models. AnyAgents runs the vendor's sign-in (see [Harnesses](#harnesses-install-update-sign-in)) and never routes a subscription credential through the gateway or any other endpoint, because the vendor's terms bind the subscription to its own tool. Each catalog entry lists the models its subscription covers, with the terms it was read from; a vendor whose terms have not been read is marked unread and offered with an API key only.
- **API key.** Any model in any harness. The harness is started with its base-URL setting pointed at the local gateway (for example `ANTHROPIC_BASE_URL` for Claude Code, `OPENAI_BASE_URL` for tools that speak the OpenAI APIs), and the gateway translates to the provider that serves the chosen model, using the user's key for that provider.

Clients show only valid combinations, so an invalid one is never offered rather than failing at run time.

## Protocol gateway (API-key mode)

A local HTTP server inside `anyagents serve`, bound to loopback, reachable only by the harness processes AnyAgents started (each gets a per-agent gateway token).

- **Inbound protocols:** the Anthropic Messages API (`/v1/messages`, streaming), and the OpenAI Chat Completions and Responses APIs (`/v1/chat/completions`, `/v1/responses`, streaming), because those are what the supported harnesses speak.
- **Outbound:** the provider's native API for the chosen model. When inbound and outbound are the same protocol the request passes through unchanged apart from the key and model; otherwise the gateway maps messages, system prompts, tool definitions, tool calls and results, streaming events, stop reasons and usage between them.
- **Usage at the source.** The gateway sees every token in API-key mode, so its usage records are exact and the harness's own report is used only as a cross-check.
- **Limits declared, not hidden.** A feature one side has and the other lacks (for example a provider without tool calling, or a thinking-block format with no equivalent) is declared on the model's catalog entry; such a pair is greyed out for the harnesses that need it.

The reference for this translation is the mature open-source proxies that already do it, such as LiteLLM's Anthropic-compatible endpoint; the gateway's conformance tests replay recorded request and stream pairs from each harness against each provider protocol.

## Resources and identifiers

The API is resource-oriented ([Google AIP-121](https://google.aip.dev/121)) and defined once in protobuf (`proto/anyagents/v1/`). The HTTP/JSON and gRPC surfaces, the CLI and the MCP tool schemas are generated from it.

| Resource | Name | Holds |
| --- | --- | --- |
| Harness | `harnesses/{harness}` | catalog entry, installed version, sign-in state, capabilities |
| Agent | `agents/{agent}` | harness, model, working directory, instructions, attached channels, state |
| Channel | `channels/{channel}` | adapter kind (for example `telegram`), its bot identity, the agents attached |
| Message | `agents/{agent}/messages/{message}` | one mailbox entry and its delivery state |
| Usage record | `agents/{agent}/usageRecords/{record}` | one turn's tokens, cost and quota state |

Identifiers are [TypeIDs](https://github.com/jetify-com/typeid) over UUIDv7 (`agt_`, `msg_`, `chn_`, `usg_`). A harness id is its catalog key (`claude-code`, `codex`, `opencode`, `gemini-cli`). Ids from a vendor or a channel (a vendor session id, a Telegram update id) keep their own format and are stored beside ours, never re-encoded.

## Harnesses: install, update, sign in

A harness is data: a catalog entry per vendor tool, shipped with the binary and overridable by the user. An entry names the install method per platform (`npm` into a private prefix, the vendor's install script with a private prefix, or a release binary with its checksum), the update method, the sign-in method, the programmatic mode, and the capabilities the adapter declares.

- **Lazy install.** Nothing is installed until an agent needs it. The first `agents.create` on a harness starts the install in the background and the agent waits in `INSTALLING`. Each harness lives under the AnyAgents data directory at a pinned version (`harnesses/<id>/<version>/`), never in a global location the user's own setup depends on.
- **Update.** The server checks for a new version on a schedule and applies it only when none of that harness's agents is in a turn. The previous version stays installed until the new one has answered once, so a bad update rolls back by switching the pin.
- **Sign in inside AnyAgents.** The server runs the vendor's own sign-in command for that harness and streams what it asks for (a link, a device code, a prompt) to the client that asked, through the API. The user completes it in a browser; the vendor tool writes its credential to its own store under the AnyAgents-managed config directory for that harness. AnyAgents records only the outcome (signed in, as whom when the vendor says, and when). An API-key sign-in is passed to the vendor tool through the variable it documents and kept in the OS keychain, or a 0600 file where there is no keychain.
- **Sharing.** All agents on one harness share its sign-in by default, because a subscription belongs to a person, not to an agent. A separate profile per agent is an option on create.

## Agents: create and supervise

`agents.create(harness = "claude-code", name, model?, workdir?, instructions?, channels?)` is one call. It records the agent, ensures the harness is installed and signed in, registers the AnyAgents MCP server into that agent's harness configuration (so the agent has `send`, `inbox` and `agents` from its first turn), and starts the agent's vendor process.

Each agent is one long-lived vendor process in its programmatic mode, run under a supervisor:

- A per-agent lock: a second process for the same agent is refused, never started beside the first.
- Restart with exponential backoff and jitter after a crash, resuming the same vendor session id. Starting a new session in place of a failed resume is reported as a failed resume, not hidden.
- A quota or rate-limit answer from the vendor (for example an HTTP 429 with a reset time) is a health state with its reset time, not a crash and not a restart loop.

### The adapter contract

One adapter per harness, five calls. A new harness is an adapter plus a catalog entry; the mailbox, channels, `send` and usage do not change.

| Call | Obligation |
| --- | --- |
| `deliver(message)` | Put one mailbox message into the live vendor process in that harness's best mode, carrying its message id. Return `accepted` once the harness has taken it (its echo or acknowledgement), or the reason it could not. |
| `background()` | Ask the vendor tool to move running work off the turn, where the tool supports it. Never wrap the tool's own tools. |
| `resume(session)` | Reattach to the same vendor session id. |
| `cancel()` | The vendor's own interrupt for the current turn. |
| `health()` | Process alive, turn state, declared delivery mode, steer support (yes, no or unproven), and any current quota or sign-in fault. |

Every harness is admitted. The delivery mode is whatever that tool does best, declared rather than hidden:

| Mode | Meaning | Example |
| --- | --- | --- |
| `steer` | Input enters the active turn, including while a tool runs. | Codex `app-server` `turn/steer`. |
| `followup` | The tool takes the input now and acts on it when the current turn ends. | OpenCode `serve`; a harness with a long-lived stream that reads the next line after the result. |
| `followup`, ending on quiet | The tool streams the answer and never prints a closing result line, so the turn is over once the stream has been silent for half a second. | None yet: the OpenCode adapter still ends a turn on its `result` line, so an agent on it is recorded only when the vendor sends one. |
| `queue` | The tool cannot take input during a turn. The adapter holds the message (it is already durable in the mailbox) and delivers it when the turn ends. | Any harness with only one-shot runs. |

The per-harness programmatic modes, sign-in methods and usage fields are listed with their sources in [harnesses.md](harnesses.md).

## Mailbox: one log per agent

Every input to an agent (a Telegram message, another agent's `send`, a CLI or TUI message) is appended to that agent's mailbox. The mailbox is the only path into a harness.

- **Sequence.** On durable receive the server assigns the next per-agent sequence number. Delivery, acknowledgement and replay key on that sequence. A message's source time (the Telegram `date`, the sender's clock) is kept as metadata and shown, but never used to order: a late, older event gets the next sequence and is not inserted behind messages already delivered.
- **Three acknowledgements**, each with its own cursor and lag:
  1. `received`: committed to the database. From here the message survives a crash.
  2. `accepted`: the adapter reports the harness took it.
  3. `processed`: a turn that included it has completed.
- **Contiguous cursors.** Each cursor advances only over a contiguous prefix of the sequence, so one stuck message is visible as the oldest unaccepted one rather than skipped.
- **Idempotent delivery.** After a crash, messages past the `accepted` cursor are delivered again. The adapter carries the message id into the harness and drops an id it has already written, so a replay never starts a second turn. The Claude Code and Codex adapters put that id on the first line of the user text as `anyagents-message:<id>`, and Codex also sends it as `clientUserMessageId`. Inbound channel events dedupe on their source key (for Telegram, the bot and its update id).
- **Retention.** Processed messages are kept for a configurable period for `inbox` and audit, then compacted.

### Guarantees

These are invariants with tests (`tests/invariants/`), run against a scripted fake harness that implements each delivery mode and can be killed at any point:

1. No message that reached `received` is lost across a server or harness crash.
2. No message starts two turns, including after a crash between `accepted` and its acknowledgement.
3. Per agent, delivery follows sequence order. A late event never reorders what was already delivered.
4. No source starves another: order is receipt order, not a priority among channels.
5. Lag is always visible: `health` reports the age of the oldest message not yet accepted and not yet processed, per agent.
6. A quota fault is a reported state with a reset time, and the server keeps the mailbox growing durably while it lasts.
7. One vendor process per agent.

## Channels

A channel adapter has an inbound side (a stream of events, each with its source key), an outbound side (post or reply to a target), and an identity (which bot or account it is). The built-in agent bus, which carries `send` between agents on the server, is not a channel adapter; it writes straight to mailboxes.

**Telegram (first version).** One bot per agent, created by the user with BotFather; the token is stored like any other secret above. A local server usually has no public URL, so the adapter long-polls `getUpdates` by default; a webhook is an option when the server is reachable. Telegram groups have one limit that shapes the design: a bot generally does not receive messages sent by other bots ([bot FAQ](https://core.telegram.org/bots/faq#what-messages-will-my-bot-get)), and recent Bot API versions deliver only some of them. So when an agent posts into a chat through its bot, the server also delivers that post into the mailbox of every other agent attached to the same chat, keyed by the outbound message id; if Telegram also delivers the same post, the copy is dropped by that key. The sender does not receive its own post.

## Send, inbox and agents (the MCP tools)

`anyagents mcp --agent <name>` is the MCP server registered into each agent's harness. It holds a capability bound to that one agent, so an agent can only send as itself.

- `send(to: [address], text, reply_to?)`. An address is an agent (`agent:<name>`, or a bare name), a named group of agents (`group:<name>`), or a channel target (`telegram:chat:<id>`, `telegram:user:<id>`). A list of one is a direct message; a mixed list delivers once per address. There is no default address.
- `inbox(since?)` reads this agent's own mailbox. It never sends.
- `agents()` lists the agents on the server with their harness, state and lag, so agents can see each other.

## Usage and cost

After each turn the adapter reads the usage the vendor tool reports and the server writes one usage record: agent, harness, model, input, output, cache-read and cache-write tokens, the cost the vendor reported (if it reports one), an estimated list-price cost from a versioned price table, the billing mode (subscription or API key), and any quota state the vendor reported. The `usage` API aggregates by agent, harness, model and day. Records are also exported as OpenTelemetry metrics using the [GenAI semantic conventions](https://opentelemetry.io/docs/specs/semconv/gen-ai/) when an endpoint is configured.

A subscription harness often reports tokens but no price; its record says `subscription` and shows the list-price estimate separately, never as money spent.

## Security model

- The API listens on a Unix socket readable only by the user. A TCP listener is opt-in and requires a bearer token.
- Each agent's MCP capability is scoped to that agent.
- Secrets (channel tokens, API keys) live in the OS keychain or a 0600 file, never in the database, logs or usage records.
- Agents run as the user who runs the server, with each vendor tool's own permission settings. Isolating agents from each other is out of scope for the first version.

## Storage

One embedded SQLite database in WAL mode under the data directory holds every resource, the mailboxes and the usage records. A message is `received` when its insert has committed. Schema changes are versioned migrations applied at server start.
