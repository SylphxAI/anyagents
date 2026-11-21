# Send a file from a remote sandbox

Upload the sandbox-local file to the coordinator, then pass the returned `source` to the AnyAgents `send` MCP tool. The file does not need a public URL.

```sh
curl --fail-with-body --silent --show-error \
  --header "Authorization: Bearer $(cat "$ANYAGENTS_MCP_TOKEN_FILE")" \
  --form 'file=@report.pdf' \
  "${ANYAGENTS_API_URL%/}/v1/media"
```

Use the sandbox's existing remote session token (`aars_*`). If the harness provides the token directly instead of a file, supply it as the Bearer credential. Do not print or pass the token in the URL. The endpoint also accepts raw file bytes (`--data-binary @report.pdf`), with a filename supplied via `?filename=report.pdf` or `X-Filename`.

A successful upload returns HTTP `201` with `media_id`, `source`, `filename`, `content_type`, and `bytes`. Pass `source` as returned:

```json
{
  "to": ["parent"],
  "text": "Here is the report.",
  "media": {"kind": "document", "source": "media://<id>"}
}
```

The same source works for Telegram attachments. For `set_chat_photo`, use `action: "set_chat_photo"`, a `target` of `telegram:chat:<chat_id>`, and `media.kind: "photo"`. The uploaded file must be a valid JPEG or PNG (up to 10 MiB), and the selected bot must have permission to change chat info. The upload endpoint accepts a single nonempty file up to 25 MiB; the destination may impose a smaller limit.

An upload reference is scoped to the uploading agent, not a bearer link. When an internal send succeeds, the recipient gets its own copy. HTTP MCP still rejects host paths and Telegram file IDs. The coordinator uses the configured private S3 bucket (`private-media/<agent-id>/…` under the configured prefix), PostgreSQL deployment blobs if configured without S3, or daemon-owned local storage. Uploads are reusable and do not expire automatically; operators should account for them in storage retention policy.
