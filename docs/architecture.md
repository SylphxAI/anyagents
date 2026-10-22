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
| Harness | `claude-code`, `codex`, `opencode`, `agy`, `muse`, `gemini-cli`, `omp`, and more by adapter | Installed lazily when first chosen. |
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
- **Sign in one agent from the CLI.** Each agent runs with its own config directory (`agents/<id>/config`), so a harness signature is only usable by that agent when it was made in that directory. `anyagents sign-in <agent>` runs the vendor's command there, reads it out before the server is up, and prints only each link, device code or prompt as it arrives and the outcome on the last line. It never reads, prints or copies the credential. `anyagents harnesses list` prints each catalog key, the variable that points the vendor at its private directory, and the command that installs it, so a missing vendor program is one line away from fixed.
- **Sharing.** All agents on one harness share its sign-in by default, because a subscription belongs to a person, not to an agent. A separate profile per agent is an option on create.

## Agents: create and supervise

`agents.create(harness = "claude-code", name, model?, workdir?, instructions?, channels?)` is one call. It records the agent, ensures the harness is installed and signed in, registers the AnyAgents MCP server into that agent's harness configuration (so the agent has `send`, `inbox` and `agents` from its first turn), and starts the agent's vendor process.

AnyAgents is a message switchboard plus a process supervisor. It never models a harness's agent loop: it does not track turns, wait for one to end, or read a vendor's answer at the end of one.

Each agent is one long-lived vendor process in its programmatic mode, run under a supervisor:

- A per-agent lock: a second process for the same agent is refused, never started beside the first.
- Restart with exponential backoff and jitter after a crash, resuming the same vendor session id. Starting a new session in place of a failed resume is reported as a failed resume, not hidden.
- A quota or rate-limit answer from the vendor (for example an HTTP 429 with a reset time) is a health state with its reset time, not a crash and not a restart loop.
- A process that has gone is replaced after its backoff, resuming the session, and the lines after the one cursor go to the new process. A line already taken is never written twice: each adapter remembers the ids its session holds.

### The adapter contract

One adapter per harness. A new harness is an adapter plus a catalog entry; the mailbox, channels, `send` and usage do not change.

| Call | Obligation |
| --- | --- |
| `deliver(message)` | Put one mailbox message into the live vendor process through that harness's own input or steer API. Return `accepted` once the vendor has taken it (its acknowledgement, echo or written line), or the reason it could not. |
| `background()` | Ask the vendor tool to move running work off the turn, where the tool supports it. Never wrap the tool's own tools. |
| `resume(session)` | Reattach to the same vendor session id. |
| `cancel()` | The vendor's own interrupt for the current turn. |
| `health()` | Process alive, declared delivery mode, steer support (yes, no or unproven), and any current quota or sign-in fault. |
| `last_usage()` | Tokens and cost the vendor reported for the runs this process made, read once when the mailbox has drained. Never part of delivery. |

Every harness is admitted. The delivery mode is whatever that tool does best, declared rather than hidden. Every line is written the moment it arrives; the vendor decides whether it joins the running turn or starts the next one.

| Harness | Mode | How a line reaches it |
| --- | --- | --- |
| `opencode` | `steer` | `POST /api/session/{id}/prompt` with `delivery: "steer"`; the session's own inbox decides whether it joins the running turn or starts the next one. Accepted when the prompt API answers with `data`. |
| `codex` | `steer` | `turn/start`, with `ifBusy: "steer"` while a turn is open. |
| `claude-code` | `steer` | A `user` message on the stream-json child's stdin, at any point; the child echoes it, which is the acknowledgement. |
| `muse` | `steer` | `turn/start` with `ifBusy: "steer"`. |
| `agy` | `followup` | The CLI reads its next NDJSON line only after a result, so the adapter holds the line until then; mid-turn input is its `PreInvocation` hook (below). |
| `followup` mode | `followup` | The contract double (`ANYAGENTS_HARNESS_BIN`) reads one line per turn and answers with a `result`; a line written mid-turn waits for that turn to end. |

No harness here uses `queue`: none of them is a one-shot run that must wait.

The per-harness programmatic modes, sign-in methods and usage fields are listed with their sources in [harnesses.md](harnesses.md).

### Antigravity and Muse

- **Antigravity (`agy`).** One `agy --print --input-format stream-json --output-format stream-json` child, resumed with `--conversation <id>`. A `result` event closes the turn; a `result` that reports an error (`is_error`, or a subtype other than `success`) is a failed turn that carries the vendor's own text. Delivery mode is followup. Mid-turn input is the CLI's own hook: it runs the `PreInvocation` command in `settings.json` in its config directory before every model call, and a hook may answer `{"injectSteps":[{"userMessage": ...}]}`. `anyagents agy-hook` is that command; it reads the mailbox through the socket and answers with every message the harness has not taken yet, in sequence order and unfiltered, so a message that arrives while a turn runs is seen at the next step. `AGY_CONFIG_DIR` points the CLI at the agent's config directory.
- **Muse (`muse serve`).** One JSON-RPC child that owns the session: `initialize`, `session/start` (or `session/resume`), `session/setApprovalMode` `allowAll`. Delivery mode is steer: `turn/start` when no turn is open, and `turn/start` with `ifBusy: "steer"` during one. `turn/started` opens a turn, `turn/completed` closes it with its usage or its error, and an `approval/request` is answered by the host. `MUSE_CONFIG_HOME` points the vendor at the agent's config directory, where `settings.json` names the MCP server and the `:unrestricted` permission profile. On Windows the clone is a directory junction at `%APPDATA%\meta\muse`.

## Mailbox: one log per agent

Every input to an agent (a Telegram message, another agent's `send`, a CLI or TUI message) is appended to that agent's mailbox. The mailbox is the only path into a harness.

- **Sequence.** On durable receive the server assigns the next per-agent sequence number. Delivery, acknowledgement and replay key on that sequence. A message's source time (the Telegram `date`, the sender's clock) is kept as metadata and shown, but never used to order: a late, older event gets the next sequence and is not inserted behind messages already delivered.
- **One delivery cursor.** A row is `received` when its insert commits, and `delivered` when the harness's own input or steer API has taken it. There is no second cursor and no turn bookkeeping: a row is done the moment the vendor has it.
- **Contiguous cursor.** It advances only over a contiguous prefix of the sequence, one row at a time, so one row the harness did not take is visible as the oldest undelivered one rather than skipped.
- **Idempotent delivery.** After a crash, rows past the cursor are handed over again. Each adapter remembers the ids its session holds and drops one it has already written, so a replay never reaches the agent twice. Claude Code and Codex also carry the id in the line itself, and Codex sends it as `clientUserMessageId`. Inbound channel events dedupe on their source key (for Telegram, the bot and its update id).
- **Egress is the agent's own `send`.** An agent replies by calling its MCP `send` tool, or a channel tool; the switchboard routes that immediately. A reply to a Telegram message carries that chat as its target, so the agent's `send` reaches the same chat. Nothing reads a vendor's transcript to find an answer.
- **Retention.** Delivered messages are kept for a configurable period for `inbox` and audit, then compacted.

### Guarantees

These are invariants with tests (`tests/invariants/`), run against a scripted fake harness that implements each delivery mode and can be killed at any point:

1. No message that reached `received` is lost across a server or harness crash.
2. No message reaches the agent twice, including after a crash between the harness taking it and the cursor recording it.
3. Per agent, delivery follows sequence order. A late event never reorders what was already delivered.
4. No source starves another: order is receipt order, not a priority among channels.
5. Lag is always visible: `health` reports the age of the oldest message the harness has not taken, per agent.
6. A quota fault is a reported state with a reset time, and the server keeps the mailbox growing durably while it lasts.
7. One vendor process per agent.
8. A line is handed over once per burst: never twice across a restart, never lost across one.

## Channels

A channel adapter has an inbound side (a stream of events, each with its source key), an outbound side (post or reply to a target), and an identity (which bot or account it is). The built-in agent bus, which carries `send` between agents on the server, is not a channel adapter; it writes straight to mailboxes.

**Telegram (first version).** One bot per agent, created by the user with BotFather and recorded with `anyagents channels add telegram --agent <name> --token-file <path>`; the token is copied into AnyAgents' own credential store at `secrets/channels/<channel id>` (mode 0600) and never into the database, a log or a command line. `getMe` names the bot once, so the channel shows which bot it is.

- **Long polling.** A local server usually has no public URL, so the transport is `getUpdates` long polling, one poll per bot at a time on a thread that stops with the server. A webhook is the option for a server that is reachable; it is not built yet.
- **What one update becomes.** A typed message, or a media message with a caption, becomes one mailbox message. Its source key is the bot and the update id, so the same update stored twice is one row. `sender` is `telegram:user:<id>:<message id>`. `reply_to` is the target a bare reply goes to: `telegram:chat:<chat id>`, or `telegram:chat:<chat id>:<message thread id>` for a forum topic, so a reply lands in the same chat and the same topic. Stickers, edited messages and membership changes are not delivered.
- **Poll offset.** The next update id lives in the channel's `state` JSON and is saved only after the updates it covers are in the mailbox, so a restart asks for what it has not stored.
- **Sending.** The `send` tool takes `telegram:chat:<id>[:<topic>]`, and `telegram:user:<id>[:<message>]` where the message is what the reply answers, plus `reply_to` for an explicit Telegram message id. A forum post carries its `message_thread_id`. The returned id is the Telegram message id.
- **Bot to bot.** A bot generally does not receive messages sent by other bots ([bot FAQ](https://core.telegram.org/bots/faq#what-messages-will-my-bot-get)), and recent Bot API versions deliver only some of them. So a chat an agent's bot deals in is remembered in the channel state, and when an agent posts into a chat through its bot the server also delivers that post into the mailbox of every other agent on the same chat, keyed by the outbound message id; if Telegram also delivers the same post, the copy is dropped by its source key. The sender does not receive its own post.

## Send, inbox and agents (the MCP tools)

`anyagents mcp --config <agent config dir>` is the MCP server registered into each agent's harness. It holds one opaque capability, minted by the server for one agent instance, so the agent acts as exactly that agent and nothing a tool argument says.

## Identity: one authenticated connection per agent session

A capability is 32 random bytes the server mints for one agent instance. The server keeps only its hash, beside the agent's immutable id and a generation: minting a new one revokes the last, so a replaced session stops working at once. The capability is written 0640 into the agent's own config directory (`agents/<id>/config/mcp-capability`), in that agent's own OS group, so its harness reads it and no other seat can. It is never an argument, a tool argument, an environment value or a log line.

The server decides the principal before any route runs:

- **An agent** is whatever agent the presented capability names, read from the store. A `sender`, an inbox path, a bot, a session id or an environment value never establishes identity. A body that names another sender is refused with no write.
- **The operator** is the deployment's own authority, for its administrative calls only: creating agents, signing a harness in, minting a capability, reading any agent's inbox. It presents a token from a file the environment names, and the server keeps only a digest of it.
- **A machine without an operator token** is the single-user route: it acts as the agent a call names, and says so once at start. It is not the identity boundary.

## The OS boundary

A server started with an operator token is the identity boundary, and it only serves that way when every harness it starts is inside the agent's own OS seat: a distinct managed uid per agent, placed by an installed launcher (`setpriv --reuid <uid> --regid <gid> --groups <gid>,<socket group> --no-new-privs --`). The uid comes from the deployment's own start line, never from anything an agent writes.

`anyagents agent-run --config <dir> -- <command>` starts one agent command inside that seat, or refuses and prints the one admission it needs. Before a command starts, it asserts what the new process is: its effective uid is the configured one and not the server's, `NoNewPrivs` is 1, the server's database, secrets, socket and token and a peer's config are denied, and its own config is writable. A successful probe alone proves nothing.

Every harness adapter, and every process an adapter starts, takes the seat from the launch environment the server set for that one agent.

## Replies and quotes

Each stored message carries the context the server authenticated for it: the author, the channel, the chat and forum topic, the mailbox reference a bare answer names, the destination that answer goes to, and the message it quotes. Every harness shows its model that context before the body, composed in one place (`anyagents_harness::compose_message`); the body's own text is never read as part of it.

`reply_to` is a mailbox reference (`anyagents:reply:<message id>`), a bare message id, or an address. A reference is resolved from the stored record, so a group message is answered in its group and its topic at the incoming message, and a direct message in its own chat. An address is used as written, which is how an agent starts a new conversation on purpose. A message naming a destination no record names is refused.

A message from Telegram keeps the message it quotes: the quoted author, as Telegram marked it (bot or person), its text or caption, and the ids that name it. The quoted text is fenced and neutralised, so a modelled line cannot forge a quoted author.

- `send(to: [address], text, reply_to?)`. An address is an agent (`agent:<name>`, or a bare name), a named group of agents (`group:<name>`), or a channel target (`telegram:chat:<id>[:<topic>]`, `telegram:user:<id>[:<message>]`). A list of one is a direct message; a mixed list delivers once per address. There is no default address.
- `inbox(since?)` reads this agent's own mailbox. It never sends.
- `agents()` lists the agents on the server with their harness, state and lag, so agents can see each other.

## Usage and cost

After each turn the adapter reads the usage the vendor tool reports and the server writes one usage record: agent, harness, model, input, output, cache-read and cache-write tokens, the cost the vendor reported (if it reports one), an estimated list-price cost from a versioned price table, the billing mode (subscription or API key), and any quota state the vendor reported. The `usage` API aggregates by agent, harness, model and day. Records are also exported as OpenTelemetry metrics using the [GenAI semantic conventions](https://opentelemetry.io/docs/specs/semconv/gen-ai/) when an endpoint is configured.

A subscription harness often reports tokens but no price; its record says `subscription` and shows the list-price estimate separately, never as money spent.

## Security model

- The API listens on a Unix socket readable only by the user. A TCP listener is opt-in and requires a bearer token.
- Each agent's MCP capability is scoped to that agent, and minted fresh for each of its instances; the last one stops working at once.
- The operator's authority is its own token, never an agent's.
- Secrets (channel tokens, API keys, the capability itself) live in the OS keychain or a 0600 file, never in the database, logs, tool arguments or usage records.
- On a deployment with an operator token, every harness runs in its own OS seat: a distinct managed uid, no new privileges, and no read of the server's files or a peer's config. A deployment without that seat refuses to start as the boundary.

## Storage

One embedded SQLite database in WAL mode under the data directory holds every resource, the mailboxes and the usage records. A message is `received` when its insert has committed. Schema changes are versioned migrations applied at server start.
