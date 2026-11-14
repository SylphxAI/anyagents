# 0010: Execution, identity and trust model

Status: accepted. Date: 2026-11-14. Decided by the owner on 2026-11-14. With [0009](0009-product-boundary.md), supersedes [0002](0002-runtime-drivers-and-capability-identity.md). Simplifies [0004](0004-identity-and-credentials.md) (D4), amends [0005](0005-managed-harnesses-and-declared-toolchain.md) (D8), restates [0006](0006-runtime-state-and-lifecycle.md) decision 11 per agent (D5), and limits [0007](0007-process-architecture.md) to third-party CLIs (D6).

## Context

AnyAgents must run agents in three places: on one machine, from a local control plane that sends some agents to remote sandboxes or vendor clouds, and from a hosted control plane where every agent has its own sandbox. Today it has one execution shape, the desk's: every agent is a process on the server's host, separated from the others by a Unix uid per seat. Checked against `main` at `7aa9ac4`:

- **Seats are file surgery.** A seat is made by changing the ownership and ACLs of files that the server and other agents also use. `seat_tree.rs` copies files into a seat by running `sh -c 'cat > …'` as that seat. `worktree_access.rs` grants ACLs on a shared bare repository's objects, refs and config. A setuid `anyagents-setpriv` launcher switches the uid. Four of the last six commits on `main` (#251, #253, #255, #256) repair this machinery, and #257, which extended it, was closed.
- **Credentials are copied.** Credential files move between profiles, agent directories and peers through the routines ADR 0004 lists. AnyAgents also refreshes vendor OAuth tokens itself, with client ids borrowed from vendor clients (0009, audit).
- **The listener is public.** `serve` binds `0.0.0.0:8788` unless told otherwise.
- **Cloud agents do not work.** The four cloud harnesses in `cloud_harness.rs` (`claude-cloud`, `openai-cloud`, `cursor-cloud`, `spiron-cloud`) each run a local `anyagents cloud-harness` process that speaks stream-json. They have been verified not to work as cloud agents.
- **User-installed clients cannot start.** `spiron`, `agy` and `muse` have no install source in the catalog (`Unconfigured`). A clean install cannot start them until someone writes a hash-pinned `harnesses.toml` entry by hand. On 2026-11-14, a model change on an `agy` agent failed with `unverifiable source for agy`, although its binary did not change (#259).

## Threat model

- Defend against an agent that is prompt-injected or mistaken. It must not exfiltrate secrets, damage the host, or damage other agents.
- In remote modes only (D1 modes 2 and 3), also defend against network attackers between the control plane and its agents.
- Do not defend against the owner of the local machine.

So local mode needs no PKI and no heavy authentication. The boundary that matters is between each agent and everything else.

## Decision

### D1. Three deployment modes, one codebase

1. **Local.** The control plane and every agent run on one machine.
2. **Hybrid.** A local control plane runs some agents remotely, as vendor cloud agents or in remote sandboxes.
3. **Cloud.** A hosted control plane runs every agent in its own sandbox.

Only the executor driver (D3, D6) and the secret backend (D4) differ between modes. The mailbox, identity, schedules, supervision, store and API are the same code in all three.

**Rationale.** A mode with its own code paths breaks where nobody tests it, and that is how desk-only assumptions reached the product (0009). With exactly two variation points, every other component is tested once, on a laptop, and stays correct when its agents move to a cloud. Hybrid is not a special case: it is local mode with some agents on a remote driver.

**Precedent.** Kubernetes serves one API from `kind` on a laptop and from a managed cloud cluster; what differs is the plugins behind fixed interfaces (container runtime, storage, cloud provider). Buildkite and GitHub Actions run one job protocol on self-hosted and hosted runners.

**Deletes.** ADR 0002's four runtime models (`local`, `docker`, `k8s`, `seat`) as separate contracts, and every mode switch outside the two drivers.

### D2. Identity

- The control plane issues each agent its identity: a capability token scoped to that agent's own mailbox and grants (D4). Only the token names an agent. A name, a sender field or an environment value never does.
- **Local transport:** a Unix socket plus the token. No TLS.
- **Remote transport:** TLS plus short-lived, per-session, revocable tokens.
- The default bind is a Unix socket or loopback, never `0.0.0.0`.
- A deployment with remote agents declares `public_url`, the address those agents call back. The desk already has one: Cloudflare fronts it and routes only `/`, `/healthz` and `/v1/webhooks/*`.

**Rationale.** Under the threat model, local mode has to tell agents apart, not keep out the machine's owner. A socket protected by file permissions plus a per-agent token does that and needs nothing provisioned. A remote agent's traffic crosses a network, so the channel is encrypted, the token expires with its session, and revoking the session ends access at once. Both modes use the same capability, so authorization is written once. With nothing bound to a public interface by default, a laptop install exposes nothing.

**Precedent.** Docker's daemon listens on a Unix socket by default and expects TLS on TCP. PostgreSQL authenticates socket peers through the operating system and network clients over TLS. Kubernetes bound service-account tokens are issued per workload, scoped to an audience, time-limited and revoked with their pod. GitLab's `external_url` and Grafana's `root_url` declare a deployment's public address.

**Deletes.** The `0.0.0.0:8788` default listener; the group-readable (`0640`) capability files and socket group that seats needed; identity taken from a name instead of a token.

### D3. Isolation: AnyAgents declares policy, a driver enforces it

- Each agent has one `SandboxSpec`:
  - read-write and read-only mounts;
  - a network egress policy;
  - resource limits;
  - credential grants (D4).
- A pluggable driver enforces the spec:
  - `host`: trusted, no isolation;
  - `os` (the default): Landlock plus seccomp on Linux, Seatbelt on macOS;
  - `container`: Docker or Podman;
  - `remote`: a Sylphx Sandbox, a Kubernetes pod, or a vendor cloud agent.
- Resource limits come from a cgroup v2 child per agent where the host provides one.
- The kernel never changes file ownership across uids.

Verified on the desk on 2026-11-14: Landlock ABI 7 is available unprivileged, and user namespaces are disabled (`user.max_user_namespaces = 0`), so bubblewrap cannot run there.

Under `os`, each part of the spec is enforced by one mechanism:

- **Files.** Landlock.
- **Ports.** Landlock's TCP bind and connect rules can restrict ports, but not hosts.
- **Egress by host or domain.** This goes through the D4 tier-1 proxy. It is not claimed where no proxy is configured.
- **Privilege.** Unprivileged Landlock requires `no_new_privs`. That also neutralises setuid binaries such as `sudo` inside the sandbox, so an agent that shares the server's uid cannot gain the server's privileges through them.

**Rationale.** A uid seat makes isolation a property of every file that agents and the server share. Each new shared file (a Git hub, a SQLite sidecar, a vendor cache) then needs its own ownership repair, and each repair can lock an agent out. A sandbox restricts the process instead, when it starts, and leaves the files alone. Landlock denies tracing a process outside the sandbox, and since ABI 6 it also scopes signals and abstract Unix sockets to the sandbox, so agents that share a uid still cannot trace, signal or reach each other through those channels. With one declared spec, the same agent runs under `os` on a laptop, `container` on a server and `remote` in a cloud (D1), and AnyAgents never has to write a sandbox of its own. `os` is the default because it needs no daemon, no root and no user namespaces.

**Precedent.** Codex CLI sandboxes commands with Landlock and seccomp on Linux and with Seatbelt on macOS. Claude Code uses Seatbelt on macOS and bubblewrap on Linux; bubblewrap needs user namespaces. The OCI runtime specification declares one `config.json` (mounts, namespaces, cgroup limits) that interchangeable runtimes enforce (runc, crun, gVisor), and Kubernetes `RuntimeClass` picks the runtime per pod. systemd gives each service its own delegated cgroup.

**Deletes.**

- multi-uid seat surgery (`seat_tree.rs`);
- the setuid `anyagents-setpriv` launcher and the `agent-run` uid checks built on it (`sandbox.rs`);
- the shared Git hub permission and witness machinery: the ACL grants and config symlink in `worktree_access.rs`, and the witness work of the closed #257;
- `ANYAGENTS_TRUSTED_METADATA_GIDS`.

### D4. Credentials: authorize and broker, never a vault

AnyAgents decides which agent may use which credential, and delivers it. It is not a vault, and it never impersonates a vendor client. Delivery tiers, most preferred first:

1. **Never enters the sandbox.** An egress proxy or gateway attaches the credential on the way out: model API keys through the Sylphx gateway, and vendor vaults such as Claude Managed Agents' vault credentials, which the sandbox sees only as a placeholder that is substituted at egress.
2. **Minted on demand, short-lived and scoped:** GitHub App installation tokens; OpenBao or 1Password dynamic or scoped credentials; cloud OIDC federation or namespaced service accounts.
3. **A vendor subscription login that must live with the vendor client:** materialised into that agent's harness config only. Each account has one refresh writer, and the vendor's own binary performs the refresh.

Secret storage is a pluggable backend: the OS keychain locally; OpenBao, 1Password or a cloud secret manager in deployments. An agent receives a credential only through a grant in its `SandboxSpec` (D3).

**Rationale.** A prompt injection can exfiltrate any secret its agent can read. So the best credential is one the agent never holds. The next best expires within the hour and opens only what the task needs. A long-lived subscription login is the exception that vendor terms force. Secret storage is already solved by audited products, so AnyAgents keeps only the authorization decision and the delivery path. Refresh tokens rotate and a used one is invalidated, so copies refreshed in several places invalidate each other. One writer per account, running the vendor's own binary, is both correct and compliant (0009 decision 3).

**Precedent.** Claude Managed Agents substitutes vault credentials at egress, and pushes to attached repositories through a proxy that adds the token, so code in the container can read neither. GitHub Actions exchanges an OIDC token for short-lived cloud credentials instead of storing keys. Vault and OpenBao issue dynamic secrets on leases. Kubernetes projects short-lived service-account tokens into pods. The OAuth 2.0 Security Best Current Practice (RFC 9700) recommends refresh-token rotation, which is why refresh needs a single writer.

**Deletes.** The seven credential-copy routines in `create.rs`, among them `seed_harness_credentials` with its peer harvest, `sync_fresher_bound_agent_credentials`, `propagate_profile_files_to_bound_agents` and `deduplicate_same_account_profiles`; and OAuth refresh with borrowed client ids (`refresh_anthropic_profile_oauth_if_due`, `extract_agy_oauth_clients`).

### D5. Git: one repository per agent

- Each agent gets its own repository, cloned with `--reference` (git alternates) to a read-only object pool.
- No shared mutable Git metadata: an agent's config, refs, index and hooks are its own.
- Pushes use the agent's tier-2 token (D4).

**Rationale.** Worktrees of one shared repository share config, refs and packed refs between agents. That is why the shared repository needed ACLs on every shared directory and a witness for its config. A clone with alternates shares only objects, which are immutable and read-only, so an agent can write everything it owns and nothing else, with ordinary permissions and no sandbox exceptions. Because objects stay shared, a new clone costs a checkout, not a download. Only the control plane's fetches write the pool, and the pool is never pruned, because pruning a reference repository can corrupt the clones that borrow from it (git-clone(1)).

**Precedent.** Buildkite agents clone with `--reference` to a local mirror, and Jenkins' Git plugin offers reference repositories for the same reason. GitHub stores a fork network's objects once and shares them through alternates.

**Deletes.** Agent checkouts as `git worktree`s of a shared bare pool, with the ACL grants on its `objects/`, `refs/`, `logs/` and `packed-refs`; the `anyagents-config/` config symlink; per-checkout `--worktree` configuration ([0005](0005-managed-harnesses-and-declared-toolchain.md) decision 9).

### D6. Executors

One executor interface with three kinds:

- **Local process**, inside a D3 sandbox.
- **Keeper**, for a third-party CLI that cannot reattach to its session ([0007](0007-process-architecture.md)).
- **Remote session**, with no local process. The kernel holds many async connections, one per remote agent.

Local Spiron reattaches natively through its session protocol (sequence numbers, attach and replay) and needs no keeper.

**Rationale.** Whatever must survive a control-plane restart has to live outside the control plane. A third-party CLI keeps its session state in its own process and protocol, so a keeper must hold it (0007). A harness we write can number its events and replay from a sequence number on attach, so the control plane reconnects after a restart with nothing in between, and no process per agent. A remote agent already runs elsewhere. Representing it by a local process, as `cloud_harness.rs` does, adds a process and a crash domain per agent and gains nothing, so a remote agent is a connection instead.

**Precedent.** containerd's shim and conmon hold a container's stdio so the daemon can restart (0007). Discord's gateway resumes a session from its id and last sequence number and replays what was missed. Server-Sent Events resume from `Last-Event-ID`. Nomad runs local, container and remote tasks (its ECS driver) behind one task-driver interface.

**Deletes.** The keeper for each Spiron agent, once Spiron reattaches natively. Spiron was 34 of the desk's 51 harness processes when 0007 was measured. Also the local process for each remote agent.

### D7. Remote agents

- One remote-session contract: `create`, `deliver`/`steer`, `events`, `interrupt`, `stop`, `budget`.
- One adapter per provider. Each adapter declares its capability level, as local harnesses declare delivery levels.
- Providers, in this order:
  1. **Spiron cloud:** agents-engine with Sylphx sandboxes and agent-workstation, on the native protocol.
  2. **Claude Managed Agents:** a container per session, queued mid-run messages, interrupt, repository resources, vault credentials, session budgets in dollars.
  3. **Cursor Cloud Agents** (`/v1/agents`).
  4. **OpenAI Agents API** (the Codex harness, September 2026).
- Remote agents get only the coordination MCP profile (`send`, `inbox`, `agents`, `groups`, `schedules`, `conversation`, `memory`, `skills`), through `public_url` (D2).
- Browser, computer and shell run in the agent's own sandbox, never on the control-plane host.
- Every metered remote agent has a budget cap.

**Rationale.** Providers differ in what they can do during a run: steer, queue, interrupt, cap spend. One contract with a declared capability level lets the kernel deliver at each provider's best level and show what is missing, as [0001](0001-server-first-one-binary.md) decision 4 does for local harnesses. Spiron cloud comes first because we control both ends and it uses no desk resources. Managed Agents comes next because it already offers the whole contract. A remote agent's tools must run where its sandbox is: lending the host's browser or computer to an agent outside the host would give an unsandboxed tool to the least trusted executor. Without a cap, a metered agent can spend without limit while nobody watches.

**Precedent.** LSP clients and servers exchange capabilities in `initialize`, and each side uses only what the other declared. Terraform has one provider protocol and many providers. Claude Managed Agents enforces a hard dollar budget per session.

**Deletes.** `cloud_harness.rs` (`claude-cloud`, `openai-cloud`, `cursor-cloud`, `spiron-cloud`), when the adapters land. Also the `remote` MCP profile, which gives remote agents the host's `browser` and `computer`.

### D8. Binary resolution

- Catalog sources:
  - `npm`, checked against the registry's integrity hash;
  - `github-release`, checked against a checksum and a signature;
  - `system`, an absolute path and a recorded version, with no hash. This is the default for a vendor client the user installed.
- Hash-pinned manifests (`harnesses.toml` `local` entries, `toolchain.toml`) are an opt-in hardened mode.
- A model or profile change never triggers an installation.

**Rationale.** A vendor client that the user installed and keeps updated is a binary the user already trusts. Refusing it for lack of a publisher digest means a clean install cannot start Spiron, Antigravity or Muse, and pinning its hash breaks at the vendor's next update. Anything AnyAgents downloads is still verified. Deployments that want an immutable toolchain keep hash pinning. Installing is a separate act from configuring, because an install triggered by a configuration edit turns that edit into an outage (#259).

**Precedent.** mise and asdf accept `system` as a version: the tool already on the machine. rustup adopts a toolchain by path (`rustup toolchain link`). npm checks `dist.integrity`. Nix pins every input by hash, as a way of working that a user opts into.

**Deletes.** `Unconfigured` as the source of user-installed clients (`spiron`, `agy`, `muse`), and with it the hand-written hash-pinned entry a clean install needs before those harnesses start; installation on a model or profile change in `configure_agent_runtime`.

## Consequences

- On a laptop, agents run under `os` with no root, no extra users and no network listener, and AnyAgents stores no secret of its own.
- To leave seats, the desk needs a one-time ownership change of seat-owned agent files back to the server's uid. The deployment performs it at the cut-over; the kernel never does (D3).
- Remote modes depend on a `public_url` reachable over TLS. The desk already has one.

## Alternatives rejected

- **Keep uid seats and repair them one by one.** See the context: every shared file needs its own ownership repair.
- **bubblewrap or user namespaces as the default.** Unavailable where `user.max_user_namespaces` is 0, as on the desk. Landlock needs neither, and hosts that run containers have the `container` driver.
- **A vault of our own.** Audited products already store secrets. D4 keeps only the authorization decision and delivery.
- **Mutual TLS with a local CA.** The threat model excludes the machine's owner. Socket permissions plus a per-agent token already separate agents.
