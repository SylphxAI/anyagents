# 0002: Capability identity and runtime boundaries across local, container and cluster deployments

Status: accepted. Date: 2026-10-23.

## Context

Open-source users run AnyAgents in three different deployment models:

1. **Local host (`local`, default):** a developer laptop, workstation or dedicated home/lab machine where every agent is trusted to use the host user's own CLI tools (`git`, `gh`, `ssh`, `kubectl`, `1Password`), local filesystems and hardware/GPUs without extra setup.
2. **Container sandbox (`docker`):** each agent runs inside its own container with an isolated root filesystem and network namespace, receiving only the workspace volumes, environment variables, SSH agent sockets or GPU devices the operator mounts into that container.
3. **Cluster pod (`k8s`) or multi-seat host (`seat`):** each agent runs in its own Kubernetes Pod / MicroVM (receiving credentials through standard Kubernetes `Secret` mounts, `ServiceAccount` tokens and Workload Identity) or in an explicitly provisioned Linux OS seat (`setpriv --reuid <uid> --regid <gid> --no-new-privs`).

When an earlier iteration required every authenticated deployment to run agents under distinct Linux `/etc/passwd` users (`seat-<name>`) on a shared host filesystem and overwrote `HOME` to `<data>/agents/<id>/config`, standard CLI tools failed (`0600`/`0700` UID ownership checks in `ssh`, `op`, `gh` and `kubectl`, plus Linux `fs.protected_regular` lock checks in shared directories). Trying to work around those failures inside AnyAgents risked pulling an ever-growing "Credential Broker Socket" and host-specific dotfile copying layer into the switchboard.

## Decision

1. **AnyAgents never brokers external tool credentials.** AnyAgents is an agent switchboard and harness supervisor. The only credential AnyAgents mints for an agent is its mailbox capability (`<data>/agents/<id>/config/mcp-capability`, presented as `x-anyagents-agent`) and, in API-key model-routing mode, its loopback protocol-gateway token. External CLI credentials, cloud identities and hardware access belong to the runtime environment (`local` host `$HOME`, Docker mounts/env, or Kubernetes `Secret` / `ServiceAccount`).
2. **Capability authentication holds in every runtime mode.** Each agent instance receives a fresh 256-bit `mcp-capability` file in `<data>/agents/<id>/config/mcp-capability`. The server authenticates agent calls strictly from that capability and never from a `sender` field, agent name or environment variable. Starting the server with an `operator-token` enforces capability authentication on the API socket in single-UID (`local`, `docker`, `k8s`) and multi-UID (`seat`) deployments alike; `/usr/local/bin/anyagents-setpriv` is required only when OS seats (`ANYAGENTS_AGENTS`, `ANYAGENTS_AGENT_UID` or `ANYAGENTS_SOCKET_GID`) are configured.
3. **Isolate harness state, not the user's `$HOME`, in local mode.** Every supported vendor harness is pointed at `<data>/agents/<id>/config` through its native configuration variable (`CLAUDE_CONFIG_DIR`, `CODEX_HOME`, `XDG_CONFIG_HOME` / `XDG_DATA_HOME` / `XDG_STATE_HOME`, `AGY_CONFIG_DIR`, `MUSE_CONFIG_HOME`, `PI_CODING_AGENT_DIR`) or config flag. In default single-UID mode, the child process keeps the user's real `HOME` so host tools, SSH keys, kubeconfigs and hardware work out of the box with zero configuration. `HOME` is redirected to `<data>/agents/<id>/config` only when running inside a separate OS seat UID or for a harness whose CLI reads only `$HOME`.
4. **Three runtime models share one switchboard contract:**
   - **Local (`local`, default):** same OS UID as the server, real `$HOME`, per-agent harness config directory and `0600` `mcp-capability`.
   - **Container (`docker`) and Cluster (`k8s`):** single UID inside each container or Pod, connecting to the switchboard over a mounted socket or authenticated endpoint with its `mcp-capability`, and receiving external credentials via standard container/Pod primitives.
   - **OS Seat (`seat`, opt-in):** distinct host UID per agent via `anyagents-setpriv` when `ANYAGENTS_AGENTS`, `ANYAGENTS_AGENT_UID` or `ANYAGENTS_SOCKET_GID` is configured, with `0640` group-scoped capability files and dynamically discovered `seat-*` accounts.
5. **Upstream purity enforced by test.** The repository contains no hardcoded user home paths, machine directories, domain names or deployment-specific agent names. Host home discovery uses the OS user database (`getpwuid` / `HOME`), and file references in config files (`{file:~/...}`) are parsed generically. An automated test (`crates/anyagents/tests/open_source_purity.rs`) gates the workspace in CI.

## Consequences

- **Zero-config local experience:** running `anyagents serve` and `anyagents agents create` on a laptop or workstation gives every agent immediate access to the user's tools and hardware while keeping each agent's session, mailbox, instructions and MCP identity isolated.
- **No Credential Broker scope creep:** AnyAgents stays focused on durable messaging, real-time steering, harness lifecycle and channel adapters rather than re-implementing OS, Docker or Kubernetes secret management.
- **Portable open-source codebase:** every path and seat lookup is data-driven, tested on Linux and macOS in CI, and free of host-specific assumptions.

## Alternatives rejected

- **Building a custom Credential Broker Socket inside AnyAgents.** Duplicates `ssh-agent`, `1Password`, Docker volume/env injection and Kubernetes `Secret` / Workload Identity, and would require custom proxy adapters for every CLI tool an agent might run.
- **Mandating multi-UID `/etc/passwd` `seat-*` accounts whenever an operator token is used.** Breaks single-user workstations, macOS hosts, Docker containers and Kubernetes Pods where the container or user account is already the intended OS principal.
- **Overwriting `HOME` to `<data>/agents/<id>/config` in local single-UID mode.** Breaks every standard CLI tool (`git`, `ssh`, `gh`, `kubectl`, `op`, package managers and compiler caches) that reads dotfiles from `$HOME`.
