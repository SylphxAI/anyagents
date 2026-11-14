# 0012: Agent storage: one root per agent and a build budget

Status: proposed. Date: 2026-11-14. Follows [0010](0010-execution-identity-and-trust.md) D3, where each agent declares its own read-write mounts.

## Context

Measured on the desk on 2026-11-14:

- Agent build output under `{scratch}/builds/{id}` was 324 GiB on 787 GB of local NVMe, and nothing bounded it. One paused agent alone held 194 GiB of `cargo/debug`.
- Because local disk kept filling, the desk moved Cargo output to its three-times-replicated `/workspace-data` volume. Rebuildable output then cost three times its size there, and builds ran about 10% slower.
- The scratch volume cannot grow: its node has about 58 GB of thin-pool room left, shared with database volumes.

## Decision

1. **One storage root per agent.** `{scratch}/agents/{id}` holds the agent's `build/` (Cargo, Gradle and Xcode output) and `tmp/` (its `TMPDIR`), and nothing that cannot be rebuilt. It is named by the agent's identity and granted only to that agent. The harness gets `CARGO_TARGET_DIR`, `TMPDIR` and `ANYAGENTS_AGENT_BUILD_DIR` pointing into it, so host build wrappers can scope their output under it. `{scratch}/builds/{id}` and `{scratch}/tmp/{id}`, the earlier layout, still count as the agent's. Source and other state stay on the workspaces and state tiers.
2. **One build budget for the scratch volume.** All build output on it stays under `ANYAGENTS_SCRATCH_BUILD_BUDGET` (300 GiB by default; `0` turns it off). That covers every agent's, plus host build output no agent owns, named by `ANYAGENTS_SCRATCH_BUILD_EXTRA_DIRS` (for example `/scratch/build-targets/cargo-*`). A server task checks it every ten minutes. Over budget, it removes build directories (one checkout's target, one Cargo profile; each entry of an extra directory) until the total is under 90% of the budget. It takes those of agents that are gone or archived first, then of agents whose harness is down, then host output, then running agents with no turn open, least recently written first within each. A directory written in the last half hour, or belonging to a running agent with a turn open, is never removed. When what is left is still over budget, or the scratch filesystem is 90% full, the operator inbox is told, at most every six hours.
3. **Dry run first.** Until `ANYAGENTS_SCRATCH_BUILD_BUDGET_DRY_RUN=0`, each check logs what it would remove and removes nothing, so a deployment reads one plan before it enforces.

Archive cleanup is unchanged here: it already removes the agent's `build/` and `tmp/`.

## Consequences

- Build output can go back to local disk: a host points its build wrappers at the agent's root, names its own build directories as extra directories, and drops the replicated-volume default.
- Rebuilds after an eviction cost time, not data.
- A deployment that never sets `ANYAGENTS_SCRATCH_BUILD_BUDGET_DRY_RUN=0` only gets the log and the operator reports.
