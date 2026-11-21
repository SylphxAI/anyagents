# Device login and Knowledge access

Request only the OAuth scopes your task needs:

    anyagents auth login --url https://coordinator.example.com --scope knowledge:read
    anyagents auth login --url https://coordinator.example.com --scope knowledge:read,knowledge:write

Repeat `--scope` or separate values with commas or quoted spaces. The CLI adds the usual OpenID identity scopes. Run `anyagents auth status` to see what the issuer granted. For existing CLI operator workflows, sign in again with `--scope 'admin:*'`. The issuer must grant the requested capabilities and include them in the signed access token's space-separated `scope` claim; a missing or malformed claim is rejected. Console sessions retain their existing permissions.

| Scope | Authority |
| --- | --- |
| `knowledge:read` | Read, list and search Knowledge, including revision history. |
| `knowledge:write` | Write and review changes. Does not grant general read access. Rollback to a previous body requires both read and write. |
| `knowledge:grant` | Create, list and revoke Knowledge grants. Does not authorize unrelated operator endpoints. |
| `admin:*` | Full operator and Knowledge access. |

The CLI normally requests an RFC 8707 resource indicator. If the issuer explicitly rejects it with `invalid_target`, the CLI retries once without it and omits it for the rest of that login. The Coordinator still validates the signed token audience. An explicitly configured audience remains mandatory; otherwise a token for the registered device client can be accepted in addition to one for the Coordinator URL.

## Grant access and review drafts

In the Console's Knowledge grants view, select a live agent (including standing agents) or enter an existing group name. The Knowledge API accepts exactly one of `agent_id` or `group_id` on a grant. A group grant applies to current members; removing a member removes that source of access without altering any separate direct grant. Grant access only to groups whose membership policy matches the intended audience.

Live agents can propose drafts in `org:*` without a grant. For a `domain:<slug>`, they need a matching read, write or propose grant. Drafting does not reveal or change the live document. A reviewer with write capability can promote a draft in one action; the draft's expected version must still match, and the resulting revision preserves its SIM fields. Proposing never confers write access.
