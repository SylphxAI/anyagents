# 0012: Agent storage: one root per agent, a build budget, and rescue on archive

Status: proposed. Date: 2026-11-14. Amends [0006](0006-runtime-state-and-lifecycle.md) decision 8 (workspace disposition). Follows [0010](0010-execution-identity-and-trust.md) D3, where an agent's read-write mounts are declared per agent, and D5, one repository per agent (#278), which it builds on.

## Context

Measured on the desk on 2026-11-14:

- Agent build output under `{scratch}/builds/{id}` is 324 GiB on 787 GB of local NVMe, and nothing bounds it. One paused agent alone holds 194 GiB of `cargo/debug`.
- Archived agents' work directories are never reclaimed when they hold anything unpushed or uncommitted, or when AnyAgents did not record them. 77 archived agents still hold 34.2 GB under `{data}/agents/*/work`, and 68 more hold checkouts under `{workspaces}`. Only about 45 of 250 agents run at a time, out of 7,900 ever created.
- Root agents' scratch directories are never removed when they are archived.
- Because local disk kept filling, the desk moved its Cargo output to the three-times-replicated `/workspace-data` volume. Rebuildable output then cost three times its size there, and builds ran about 10% slower.

## Decision

1. **One storage root per agent.** `{scratch}/agents/{id}` holds the agent's `build/` (Cargo, Gradle and Xcode output) and `tmp/` (its `TMPDIR`), and nothing that cannot be rebuilt. It is named by the agent's identity and granted only to that agent. The harness gets `CARGO_TARGET_DIR`, `TMPDIR` and `ANYAGENTS_AGENT_BUILD_DIR` pointing into it, so host build wrappers can scope their output under it. Source and other state stay on the workspaces and state tiers.
2. **A global build budget.** All agents' build output together stays under `ANYAGENTS_SCRATCH_BUILD_BUDGET` (300 GiB by default; `0` turns it off). A server maintenance task checks it every ten minutes. Over budget, it removes whole build directories (one checkout's target, one Cargo profile) until the total is under 90% of the budget: first those of agents that are gone or archived, then of agents whose harness is down, then of running agents with no turn open, least recently written first within each. A directory written in the last half hour, or belonging to a running agent with a turn open, is never removed. When what is left is still over budget, or the scratch filesystem is 90% full, the operator inbox is told, at most every six hours. `ANYAGENTS_SCRATCH_BUILD_BUDGET_DRY_RUN=1` logs the plan and removes nothing.
3. **Archive removes the root and rescues the work.** Archiving any agent, root or sub-agent, removes its storage root (and the legacy `{scratch}/builds/{id}` and `{scratch}/tmp/{id}`) at once. A sub-agent's managed work directory is then reclaimed checkout by checkout. Every checkout is the agent's own repository ([0010](0010-execution-identity-and-trust.md) D5); a linked worktree from before that is adopted in place first, as archive already did:
   - a checkout that passes the clean check (empty status; HEAD and every local branch on a remote-tracking ref) is deleted;
   - any other is saved to the rescue area (`ANYAGENTS_RESCUE_DIR`, default `{data}/rescue/{id}/{time}/`) and then deleted. The save is a `git bundle` plus a tar of the uncommitted files. A clone with its own object store is bundled with `--all`, so it restores on its own. A repository that borrows from the pool is bundled down to what the remotes have (`HEAD --branches --not --remotes`): bundling all its refs would copy the pool's history once per checkout, and the pool is never pruned;
   - files outside any checkout are saved as one tar, without build output, tool homes or caches;
   - a checkout that cannot be adopted, read or saved stays where it is, with only its build output removed, and the operator inbox is told.

   Git reads run as the agent's seat through the same hardened command as the clean check (no hooks, fsmonitor or user configuration), and tar runs as the seat. Nothing is deleted before its rescue is complete on disk, with a `manifest.json` that says how to restore it.
4. **Old archives are revisited once.** `.anyagents-reaped` records the cleanup revision. An archive an earlier revision finished, but whose storage is still on disk, is cleaned again under this revision.

This replaces 0006 decision 8's rule that any dirty, unpushed or unclassifiable path preserves the whole tree in place. Work is still never lost, but it moves to one place an operator can review, instead of keeping every archived checkout and its build output on disk forever.

## Consequences

- Build output can go back to local disk: a desk can point its build wrappers at the agent's root and drop the replicated-volume default.
- Rebuilds after an eviction cost time, not data.
- The rescue area grows with archived work that was never pushed. It is not pruned automatically.
- While seats exist, rescue Git runs as the seat. Under 0010's `os` driver, where agents share the server's uid, it has to run inside the agent's sandbox instead, like the clean check it shares its command with.
