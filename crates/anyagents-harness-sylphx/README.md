# Sylphx harness configuration

Local execution is available without a cloud environment. Cloud execution and
teleportation require an explicitly selected authorized environment in
`SYLPHX_AGENT_ENV`, retained session metadata, or `.config/sylphx/context.json`.
Resource names accept `orgs/<org>/projects/<project>/envs/<env>` (including the
long-form collection names), `<org>/<project>/<env>`, or an authenticated
`environments/<id>` resource. There is no compiled-in tenant or `default`/`prod`
alias. `cloud_only` without an environment is an error.

`SYLPHX_EXECUTION_MODE` or `sylphx-agent.json`'s `execution_mode` selects
`auto_cloud_first`, `local_only`, or `cloud_only`; the persistent
`sylphx-workspace-placement.json` record takes precedence. Unknown, unreadable,
or malformed selected settings are errors, not default selections. A submitted
or uncertain cloud turn is never automatically replayed on another session or
locally. Restore/reconcile the retained session before resuming.

Shared `AGENTS.md` instructions precede workspace `AGENTS.md` instructions, with
workspace instructions appearing later. Headings and repository names do not
change this order. Retained context is included only from the transcript and
explicit workspace-owned files listed in `sylphx-agent.json`, for example:

```json
{
  "execution_mode": "local_only",
  "context_sources": ["notes/current-context.md"]
}
```

Context source paths must resolve to regular files inside the selected workspace;
symlinks escaping it are refused. No specially named state file is auto-loaded.

Device login with multiple organization credentials requires `SYLPHX_ORG_ID` or
an authenticated server `active_org` selection. A single organization needs no
extra selection. Credential files are staged privately before atomic replacement.

Required transcript, goal, binding, cursor, fingerprint, tool-result, and terminal
transport failures are exposed to the caller. Failed cloud tool-result submission
retains a durable receipt for resubmission without reexecuting the local tool;
an interrupted tool with unknown effects retains an unresolved receipt instead.
A reply is fulfilled only by a successful delivery receipt for the exact retained
address, including its topic. Calling `send`, unrelated delivery, and failed
receipt decoding do not settle that obligation.

SDK placement/binding loaders and session construction return `Result` so callers
must handle invalid or inaccessible state rather than treating it as absence.
