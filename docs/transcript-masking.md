# Tool-output masking

Native Sylphx tool dispatch and tool-history indexing share the deterministic
`anyagents_core::masking` implementation. The common dispatch boundary masks each
complete tool result before applying its observation size limit. Bash, file-read
and MCP producers return complete text to that boundary, so middle truncation
cannot separate a recognized credential's label from its value. Both native
sequential/parallel model requests and Cloud Agent `:submitToolResult` submissions
consume only the masked dispatch result.

Transcript appends and tool-history indexing also sanitize their inputs before
serialization or indexing. Appends return the masked record for active context;
nested native-turn snapshots use the same append boundary. These persistence
checks remain a defense for records produced outside tool dispatch. Existing
records are not rewritten by this change.

The masker recognizes explicit `password`, `passwd`, `api_key`, `apikey`,
`access_token`, `refresh_token`, `client_secret` and `authorization` assignments,
including quoted JSON/string values. It also recognizes long tokens carrying the
configured GitHub, Stripe, OpenAI project-key and Slack token prefixes. These are
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

Regression tests exercise the actual transcript file, nested snapshots, returned
active-context records, persisted tool-history text and search, with Unicode,
quoted values, credential references and byte-identical non-secret controls.
Loopback Responses API and Cloud Agent fixtures verify actual next-request and
`:submitToolResult` payloads, persisted transcripts, intact call ids and actionable
diagnostics. Large bash, file-read and MCP results include a credential straddling
the old middle-truncation boundary. No external model or provider is called by
these fixtures.
