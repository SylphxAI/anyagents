# 0013: Archive cleanup rescues work, then deletes it

Status: proposed. Date: 2026-11-14. Amends [0006](0006-runtime-state-and-lifecycle.md) decision 8 (workspace disposition). Builds on [0010](0010-execution-identity-and-trust.md) D5, one repository per agent.

## Context

Archive cleanup deleted only checkouts that were clean and fully pushed. It kept every other checkout of an archived agent in place, with no end date. On the desk on 2026-11-14 that was 77 archived agents' work directories, 31.8 GiB under `{data}/agents/*/work`. There were also 67 under `{workspaces}`, and the agents behind them will never run again.

Keeping the checkouts whole protects the work, but nobody reviews it, and it holds the disk forever. Deleting it would lose what no remote has: unpushed branches, the stash, local tags, notes, commits only a reflog reaches, and uncommitted files.

## Decision

1. **The clean check refuses; the rescue saves.** A checkout is deleted as it is only when the clean check (`is_git_worktree_safe_to_reap`) and the rescue's own reading both find nothing a remote lacks:
   - an empty status;
   - every commit that HEAD, a branch, `refs/stash`, a tag, a note or any reflog reaches is on a remote-tracking ref;
   - no tag the pool lacks (every tag, for a clone with its own objects).
2. **Anything else is saved first.** It goes to `{data}/rescue/{id}/{time}/` (`ANYAGENTS_RESCUE_DIR`):
   - a `git bundle`. A clone with its own objects is bundled with `--all`, so it restores on its own. A repository that borrows from the pool is bundled down to what the remotes have: HEAD, the branches, the stash, local tags, notes, and its reflog-only commits under `refs/anyagents-rescue/reflog-<n>`. Bundling all its refs would copy the pool's whole history once per checkout, and the pool is never pruned;
   - a tar of its uncommitted files, without build output;
   - every ref it had, and a `manifest.json` that says how to restore it.

   Files beside checkouts are saved as one tar, without build output or caches. Nothing is deleted before its rescue is complete on disk.
3. **What cannot be read stays.** A checkout that cannot be adopted, read or saved stays where it is, with only its build output removed, and the operator inbox is told.
4. **Git runs as the agent.** Reads go through the same hardened command as the clean check, as the agent's seat. Tar runs as the seat too.
5. **Old archives are revisited once.** `.anyagents-reaped` records the cleanup revision. An archive an earlier revision finished, whose work directory is still on disk, is cleaned again under this revision. One maintenance pass spends at most 20 seconds on cleanups.

## Consequences

- No work is lost, and nothing is kept in place forever: the rescue area is one place an operator can review.
- The rescue area grows with archived work no remote has. It is not pruned automatically.
- Under 0010's `os` driver, where agents share the server's uid, rescue Git has to run inside the agent's sandbox, like the clean check it shares its command with.
