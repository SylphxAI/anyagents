# 0009: The product boundary: the master, its integrations, and deployment profiles

Status: proposed. Date: 2026-11-13. Amends [0003](0003-runtime-rebuild.md) decision 6, [0004](0004-identity-and-credentials.md) decision 4 and [0005](0005-managed-harnesses-and-declared-toolchain.md) decisions 1 and 6–7, and restates the position in [vision](../vision.md).

## Context

AnyAgents is a general-purpose open-source project. We build it first for our own fleet, and it gives our own platforms (Spiron, our harness; Sylphx, our AI gateway and cloud) native, first-class support. That support should showcase them, but no user should need them. A user on a laptop with no Sylphx account must lose nothing else.

The owner's position, recorded on 2026-11-13:

- **AnyAgents is the master.** It is the control plane for every harness, wherever the harness runs: locally, on a server, or each agent in its own remote sandbox. It owns profiles (accounts and credentials), identity, mailboxes, topology, schedules, the tool layer, memory and skills.
- **A vendor harness is a compliant model channel.** Some subscriptions may only be used through the vendor's own client, and anything else risks the account. So AnyAgents always drives the vendor's own binary. It does not want that binary's built-in tools: Code Mode provides every tool, the file system included, and AnyAgents provides coordination, memory, skills and web.
- **Our tools are the product.** We use our own tools on every harness, even where a vendor's tool is better today. Measurements show where ours must improve; they never switch an agent back to the vendor's tool.
- **Compliance is best effort and deliberate.** Model traffic goes through the vendor's client, and nothing impersonates that client.

Three read-only audits of `acdb690` on 2026-11-13 found the opposite drift. Our own deployment (the desk: a Kubernetes pod with per-seat Unix uids, guard wrappers, a sha-pinned image, 1Password, our org's working rules) had been written into the product as universal rules. The findings below were checked against the code:

| Area | Finding | Evidence |
|---|---|---|
| Safety default | `serve` listens on `0.0.0.0:8788` unless `ANYAGENTS_WEBHOOK_LISTEN` says otherwise; `vision.md` and `SECURITY.md` say network access is opt-in | `server.rs` `webhook_listen_addr` |
| Spend and data | The 60 s maintenance pass runs LLM "sleep" consolidation whenever it finds a Sylphx key, including under the user's `$HOME` (`~/.config/sylphx/key` and others), against `api.sylphx.com` with `openai/gpt-6.1-sol` by default | `learned_memory.rs` `read_sylphx_key_candidate`, defaults |
| Compliance | AnyAgents refreshes Anthropic subscription tokens itself with Claude Code's client id, calls `api.anthropic.com/api/oauth/usage` and `/profile`, and refreshes Google tokens with an OAuth client id and secret extracted from the `agy` binary | `create.rs` `refresh_anthropic_profile_oauth_if_due`, `extract_agy_oauth_clients` |
| Fleet policy | A child of an `agy` agent always becomes a Spiron agent on a `sylphx/` model | `create.rs` `inherit_harness`, `spiron::DEFAULT_SPIRON_MODEL` |
| Fleet policy | The turn-ownership guard, "sub-agents never archive themselves", verdict prompts and `op://` instructions are fixed product behaviour | `schedule.rs` `guard_family`; `create.rs` system prompt |
| Tool layer | Every stdio harness gets the `full` MCP profile, whatever the docs say; Spiron keeps its own memory, skill and web tools as well, so it has two of each | `mcp.rs` `resolve_stdio` |
| Tool layer | Native web search is disabled, but the replacement needs an engine key; with none configured an agent has no web access | `mcp_config.rs`, `web.rs` |
| Hardening | Seat and ACL machinery (about 9k lines), the declared toolchain and the tool-credential broker are written as unconditional rules, even for a single user | ADR 0003–0005; `worktree_access.rs`, `seat_tree.rs`, `sandbox*.rs` |
| Desk specifics | `--repo` needs a checkout provider or a pool; the data directory is found through a desk pointer file; about 105 `ANYAGENTS_*` variables and no config file | `worktree.rs`, `data.rs`, `main.rs` |
| Spiron | No verifiable release source (we hand-install binaries); no cost in `result`; usage split into equal rows per finished id; Spiron history not indexed; protocol not specified outside a README | `catalog.rs`; `anyagents-supervisor` `record_usage_with`; `host_retrieval.rs` |

Two root causes recur:

- **ADR 0003 decision 6** ("no fallbacks, compatibility branches or second paths") was read as "every rule is mandatory". It treated a silent fallback and an explicit, documented mode as the same thing.
- **No continuous check runs a clean open-source install.** CI exercises the desk mode, so a desk-only assumption fails no test.

Disabling vendors' native tools does not belong in this list. It is the product, and this record keeps it.

## Decision

### 1. Three layers

| Layer | Contains | Rule |
|---|---|---|
| **Core (the master)** | Identity and authorization, mailboxes and delivery, topology and groups, schedules, supervision and keepers ([0007](0007-process-architecture.md)), profiles, managed harness installs, Code Mode, memory, skills, outcomes ([0008](0008-quality-loop.md)) | Works on a laptop with zero configuration, with safe defaults |
| **Integrations** | Spiron, Sylphx, Telegram and other channels, GitHub watches, secret-reference providers (1Password), cloud sandboxes | First-class and native, always optional. The core never requires one and never defaults to one; an integration is enabled by configuration or by a profile that names it |
| **Deployment profiles** | Seats (per-agent uids), the sha-pinned toolchain, guard wrappers, build-cache layout, account pools, subagent defaults, org working rules and prompts | Configuration that lives with the deployment (for the desk: `SylphxAI/infra` and `SylphxAI/owner`), never product code |

### 2. AnyAgents owns capabilities; harnesses supply the model

- The capability table is data: one documented table naming every capability and its owner. It replaces process-wide switches such as `ANYAGENTS_ALLOW_NATIVE_SUBAGENTS`.

| Capability | Owner on every harness |
|---|---|
| Subagents, messaging, schedules, timers, notifications | AnyAgents (coordination) |
| File system, shell, search, edit, patch | AnyAgents (Code Mode) |
| Web search and fetch, browser and computer, media | AnyAgents (Code Mode services) |
| Memory and skills | AnyAgents |
| The model loop, under the subscription's terms | The vendor harness |

- A vendor tool is disabled only through the vendor's supported configuration (flags, settings files, hooks); nothing patches a vendor binary.
- **No capability disappears.** A vendor tool is disabled only when AnyAgents' replacement works in that deployment. Web search ships a keyless default engine; configured engines improve on it.
- **No duplicates.** Spiron runs on the same tool engine natively (`coordination` plus the Code Mode services it lacks), not on its own tools plus the MCP set.
- **Our tools are measured against the vendors' tools.** A comparative evaluation and outcome data (0008) show where Code Mode loses, and that is where it is improved. The vendor tool is never a fallback.

### 3. Profiles and compliance

- The master is the single writer of every profile's credential lifecycle, so agents in many sandboxes never race a rotating refresh token.
- Every vendor-facing action runs through the vendor's own binary, in a staging config directory the master controls: sign-in, token refresh, and usage or quota queries where the vendor's client offers them. The master collects the result and materializes it per run ([0004](0004-identity-and-credentials.md) `materialize`).
- Nothing impersonates a vendor client. AnyAgents does not use a borrowed OAuth client id, a secret extracted from a binary, or its own requests to a vendor's account endpoints. Where a vendor's client offers no usage query, quota state comes from what the harness reports (rate-limit errors and their reset times), and account rotation reacts to it.
- Model traffic only ever leaves through the harness.

### 4. Executors anywhere, with least privilege

- A keeper ([0007](0007-process-architecture.md)) is the runner. On the same host it is a local process; in a remote sandbox it speaks the same kernel protocol over the network, with the same exactly-once and steering guarantees.
- Each run receives only its own profile's credentials, short-lived where the vendor allows, never a store of other organizations' keys.

### 5. Hardening is a mode, never the only shape

[0003](0003-runtime-rebuild.md) decision 6 is restated: **no silent fallbacks.** An explicit, documented, tested mode is not a fallback.

- **Toolchain** ([0005](0005-managed-harnesses-and-declared-toolchain.md)):
  - `detect` is the default. Ordinary tools resolve from the system directories plus directories the user configures, and versions are reported by `anyagents doctor`. The constructed helper `PATH` never includes a user-writable wrapper directory unless configured.
  - `strict` is opt-in. Tools are sha-pinned and enforced, and `anyagents toolchain snapshot` writes the manifest from what `detect` resolved.
  - Managed harnesses and runtimes stay managed in both modes.
- **Seats**: per-agent uids, ACLs and the seat launcher apply only when seats are configured. Single-user mode uses the user's own identity, `~/.gitconfig` and tools.
- **Tool credentials** ([0004](0004-identity-and-credentials.md)):
  - An unbound `tool:<name>` passes the user's environment through in single-user mode.
  - Brokering and fail-closed behaviour apply when bound or when seats are configured.
  - Credential kinds become an open `tool:<name>`.

### 6. Safe defaults

- No network listener unless configured. The desk configures one behind its tunnel.
- No background model calls unless a deployment configures an endpoint and model. There is no key discovery in `$HOME`, and no vendor default.
- Every place AnyAgents hands an agent elevated permission is documented. Because Code Mode is the tool layer, Code Mode's workspace boundary is the safety boundary in every mode, single-user included.

### 7. Integrations

| Integration | First-class means |
|---|---|
| Spiron | A managed source with signed per-platform releases and a version that identifies the build; a doctor that starts `--headless` and checks the `ready` capabilities; `cost_micros` and the served model in `result`; one usage row per turn; history indexed from its journal; `spiron.headless/v1` published as a versioned specification with schemas and a conformance suite, as the reference for steering with proven consumption; the cloud adapter on the public messages-and-receipts API with the mailbox id as the idempotency key |
| Sylphx | Provider and engine presets (models, web, media) selected by a profile, never core defaults |
| Telegram and channels | A connector. Rich actions (polls, checklists, articles) belong to a channel tool that exists only when that channel is attached; core `send` is `{to, text, reply_to, attachments}` |
| GitHub, Spiron promises | Watch sources behind one interface, not cases inside the scheduler |
| Secret references | A provider interface; `op://` is one provider |

### 8. Deployment profiles

The desk's profile holds:

- seat names and groups;
- guard wrappers and the constructed `PATH`;
- `toolchain.toml` and `harnesses.toml`;
- build-cache environment;
- subagent defaults (for example leads on AGY, subagents on Spiron);
- our org's working rules, including the turn-ownership reminders and verdict prompts, as role templates and configuration.

The product ships the mechanisms (events such as `[Sub-Agent Completed]`, a configurable guard, role templates) and no policy text.

### 9. One configuration file

`<data>/anyagents.toml`, with per-agent overrides, holds settings that today are environment variables. Environment variables remain for bootstrap only, such as the data directory.

## Acceptance

- **Clean install.** A CI job on macOS and on Ubuntu, with no Sylphx account and no desk files, goes from `npm i -g @sylphx/anyagents` to a running Claude Code and a running Codex agent following the README. It creates a subagent and searches the web.
- **Purity.** `tests/open_source_purity.rs` also rejects desk paths and names (`/scratch/`, `/workspace-data`, `desk-`, seat names) in shipped code.
- **Capability table.** For every harness, a test asserts that its native tools for each AnyAgents-owned capability are disabled and that the replacement is present and works.
- **No impersonation.** No product code holds a vendor OAuth client id or reads a client secret from a vendor binary; a purity test enforces it.
- **Safe defaults.** With no configuration, nothing listens on a network interface, and no model endpoint is called outside a harness.

## Delivery

Ordered by risk. Each step says what it interrupts on a running deployment.

1. **Safety and compliance.** Listener default; background model calls opt-in with no key discovery; vendor-facing actions through vendor binaries; least-privilege credentials per seat. Interrupts nothing beyond a daemon restart; the desk sets its listener and consolidation endpoint in its profile first.
2. **Spiron native.** Managed source and doctor; cost and served model; one usage row per turn; history indexing; protocol specification; the Spiron tool profile.
3. **Capability table and Code Mode completeness.** Keyless web default; the table replaces the env switches; Spiron runs without duplicates; the Code Mode comparative evaluation.
4. **Modes.** `detect`/`strict` toolchain with `snapshot`; seats and the tool broker only when configured; one config file.
5. **Policy out of the product.** The `agy`→Spiron default, guard text and prompts move into the desk's profile and role templates; integrations move behind their interfaces.

## Consequences

- An open-source user gets a working, safe agent host with no setup, and sees Spiron and Sylphx as the best-supported options rather than requirements.
- The desk keeps its hardening, set explicitly in its profile instead of implicitly in the code.
- Some current desk behaviour becomes configuration the desk must carry: the listener, consolidation, subagent defaults and guard text. Each move ships together with the desk's profile change, so nothing regresses there.

## Alternatives rejected

- **Re-enabling vendors' native tools where they measure better.** Rejected by the owner. Our tool layer is the product, and measurements drive its improvement.
- **Leaving token refresh to each harness in each sandbox.** Refresh tokens rotate, so copies in many sandboxes invalidate each other. A single writer is required, and the vendor's binary performs the refresh.
- **Keeping hardening mandatory and documenting the setup.** That fails the zero-configuration goal, and every OS update breaks sha-pinned system tools.
