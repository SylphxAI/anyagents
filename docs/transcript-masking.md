# Tool-output masking

Native Sylphx transcript appends and tool-history indexing share the deterministic
`anyagents_core::masking` boundary. It replaces recognized credential values with
`[redacted]` before serialization or indexing. The append operation also returns
the masked record, so sequential and parallel tool results enter active context
in the same form that is stored. Nested native-turn snapshots pass through the
same append boundary. Existing records are not rewritten by this change.

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
