# 0012: A build budget for the scratch volume

Status: proposed. Date: 2026-11-14. Follows [0010](0010-execution-identity-and-trust.md) D3, where each agent declares its own read-write mounts.

## Context

Measured on the desk on 2026-11-14:

- Agent build output under `{scratch}/builds/{id}` was 324 GiB on 787 GB of local NVMe, and nothing bounded it. One paused agent alone held 194 GiB of `cargo/debug`.
- Because local disk kept filling, the desk moved Cargo output to its three-times-replicated `/workspace-data` volume. Rebuildable output then cost three times its size there, and builds ran about 10% slower.
- The scratch volume cannot grow: its node has about 58 GB of thin-pool room left, shared with database volumes.
- Not everything in a build root is build output. One agent keeps its sub-agents' checkouts, with unpushed commits, under `{scratch}/builds/{id}/workers/`.

## Decision

1. **The build root stays where it is.** An agent's build output stays in `{scratch}/builds/{id}` and its `TMPDIR` in `{scratch}/tmp/{id}`. Moving them would rebuild every agent's output beside the old copy on a volume that cannot grow. The harness also gets `ANYAGENTS_AGENT_BUILD_DIR`, naming its build root, so host build wrappers can put their output under it. Archive cleanup removes both directories, as before.
2. **One build budget for the scratch volume.** All build output on it stays under `ANYAGENTS_SCRATCH_BUILD_BUDGET` (300 GiB by default; `0` turns it off). That covers every agent's, plus host build output no agent owns, named by `ANYAGENTS_SCRATCH_BUILD_EXTRA_DIRS` (for example `/scratch/build-targets/cargo-*`), less what `ANYAGENTS_SCRATCH_BUILD_EXTRA_EXCLUDE` names (output a host keeps on purpose). A server task checks it every ten minutes.
3. **Only provable build output is removed.** A directory is removed only when it is a Cargo target directory (`.rustc_info.json`) or profile (`.fingerprint`), carries a valid `CACHEDIR.TAG`, or is a tool cache the server itself assigned in the build root: a seat's private tool cache (`cache/<tool>`), Gradle's caches, Xcode's DerivedData entries. Nothing with `.git` at its top is ever build output. Everything else is counted toward the budget and named in the operator report, and never touched.
4. **Order and protection.** Over budget, the task removes those directories until the total is under 90% of the budget: those of agents that are gone or archived first, then of agents whose harness is down, then host output, then running agents with no turn open, least recently written first within each. Never removed: a directory written in the last half hour (whoever owns it), anything of an agent with a turn open or whose row cannot be read, and anything before every running agent's harness has reported to the supervisor since the server started. Each removal takes the agent's lifecycle lock and reads the agent again, takes the desk Cargo wrapper's scope lease (`.desk-cache-locks/<key>.lock`) and Cargo's `.cargo-lock` without waiting, measures the last write again, and renames the directory aside before deleting it.
5. **Reports.** When what is left is still over budget, or the scratch filesystem is 90% full, the operator inbox is told, at most every six hours, naming who holds the space.
6. **Dry run first.** Until `ANYAGENTS_SCRATCH_BUILD_BUDGET_DRY_RUN=0`, each check logs what it would remove and removes nothing, so a deployment reads one plan before it enforces.

## Consequences

- Build output can go back to local disk: a host points its build wrappers at the agent's build root, names its own build directories as extra directories, and drops the replicated-volume default.
- Rebuilds after an eviction cost time, not data.
- Work an agent keeps in a build root is never lost to the budget, but it holds space the budget cannot free; the report names it.
- A deployment that never sets `ANYAGENTS_SCRATCH_BUILD_BUDGET_DRY_RUN=0` only gets the log and the operator reports.
