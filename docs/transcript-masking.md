# Tool-output masking

AnyAgents masks tool-history text before storing or indexing it, using the
shared deterministic `anyagents_core::masking` implementation. Coordinator
transcript reads and history search consume those masked records. Existing
records are not rewritten by this change.

Model requests, harness-local transcripts and tool-result truncation belong to
the external harness. AnyAgents does not dispatch model tools or submit Cloud
Agent tool results. Spiron must apply its own masking before those boundaries;
coordinator indexing cannot sanitize a separate harness's local state or provider
requests.

The masker recognizes explicit `password`, `passwd`, `api_key`, `apikey`,
`access_token`, `refresh_token`, `client_secret` and `authorization` assignments,
including quoted JSON/string values. It also recognizes long tokens carrying the
configured GitHub, Stripe, OpenAI key and Slack token prefixes. These are
finite syntax rules, not an entropy or personal-data classifier. Unlabelled
player passwords, arbitrary personal information, unknown credential formats,
encoded credentials and incomplete token fragments are not guaranteed to match.
Use the supported historical redaction operation for captured data that needs
manual removal; masking does not revoke an exposed credential.

Structural JSON keys and non-string values remain intact. Credential references
(`op://`, `op-otp://`, `secret://`) and existing tombstones remain references or
tombstones. Non-secret output retains its exact text bytes; the transcript's
existing JSON serialization is unchanged. No secret value or digest is retained
for masking diagnostics.

Regression tests exercise persisted tool-history text and search, with Unicode,
quoted values, credential references and byte-identical non-secret controls.
Store regression tests also verify masked durable event payloads. These fixtures
call no external model or provider.

The external CLI transport applies the same masker as durable history. Spiron
text deltas are concatenated without inserted newlines and exposed only after
terminal completion, so frame boundaries cannot split a credential match.
Structured error strings are masked before being retained or persisted; JSON
keys, request correlation and usage numbers remain structural protocol data.
The finite patterns also include case-insensitive bearer values, `sk-` tokens,
credential-suffixed environment/header keys, URL userinfo and query credentials.
This is defense in depth for AnyAgents ingestion, not a substitute for Spiron's
own database, provider-request and tool-output masking.

Nonzero child exits retain their exit status and masked stderr in supervisor
health and `.anyagents-last-error.json`. A separate drainer consumes the entire
stderr pipe while retaining at most 16 KiB. Until pipe EOF or on overflow, the last incomplete line
is withheld before masking and an explicit truncation marker is appended. Safe
complete diagnostics, Unicode and successful-exit semantics are preserved.
