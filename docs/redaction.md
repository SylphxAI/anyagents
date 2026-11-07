# Redacting a captured secret

`conversation.redact` is an exceptional, audited erasure, not ordinary retention
or context compaction. Only the target's **ancestors**, a root with administrative
authority, or the operator may invoke it. A target cannot redact itself; shared
groups, public scope and peer access do not grant this permission.

Pause or archive the target first and wait for its harness process to exit. The
operation refuses a running target or an occupied harness lock. It does not stop
or restart a daemon. This prevents an active local model from immediately writing
the erased text back from memory.

## Interfaces

MCP, using the existing `agents` tool:

```json
{"action":"redact","name":"target-agent","message_id":"transcript:42","span":{"unit":"chars","start":12,"end":26},"reason":"Accidental credential capture"}
```

Admin CLI, through the running authenticated API (no offline bypass):

```sh
anyagents redact target-agent --message-id transcript:42 \
  --unit chars --start 12 --end 26 --reason 'Accidental credential capture'
```

API: `POST /v1/agents/{agent}/conversation/redact`, authenticated with the existing
operator token or agent capability. The JSON body is:

```json
{"message_id":"transcript:42","span":{"unit":"chars","start":12,"end":26},"reason":"Accidental credential capture"}
```

- Supply exactly one `message_id` or `seq`. A `seq` is a target mailbox sequence,
  **not** a transcript line or the search index's non-unique sequence coordinate.
- `message_id` accepts inbound `msg_…`, outbound `out_msg_…`, indexed `tool_…`
  ids, or `transcript:<zero-based-line>` in `config/sylphx-transcript.jsonl`.
  A transcript selector addresses the decoded `output` or `content` string in
  that line, preserving its call id/type and all line positions. Complex
  non-string transcript content is refused rather than guessed.
- Omit `span` to replace the whole decoded body with `[redacted]`. Otherwise
  supply `unit`, `start` and `end`: **end-exclusive** UTF-8 byte offsets (`bytes`)
  or Unicode scalar offsets (`chars`, not grapheme clusters or UTF-16 code units).
  Empty, reversed, out-of-range and split-UTF-8 spans are refused, not truncated.
- Never repeat sensitive text or its digest in `reason`. The implementation
  refuses a reason containing the removed text or its content digest.

## Guarantees and scope

The operation preserves ids, sequence numbers, cursors, timestamps, direction and
routing fields, and changes only the selected body span. Other mailbox entries
are untouched. The same logical body copied into the target's local managed
JSONL files is scrubbed at the equivalent decoded span, including JSON escaping,
nested serialized JSON and envelope prefixes. Unrelated lines retain their exact
serialized bytes. Exact identical full-body occurrences are treated as copies.

Canonical inbound/outbound bodies, the corresponding RAG chunk, its content hash,
FTS content/tokens and a matching Telegram edit baseline are updated. FTS is
rebuilt with secure deletion, SQLite secure deletion is enabled and the WAL is
checkpointed/truncated. The audit records actor, time, permission, reason, target,
span, removed byte/character counts and copy outcomes—**never original text or a
hash of it**. Replay of the original target/span, including equivalent mailbox
id/seq selectors, returns the existing audit id and receipt without a second edit.
Coordinates are otherwise relative to the body's current version.

An inbound redaction also binds its recipient and provider `(chat_id, message_id)`
when present. Later deliveries or edits with those coordinates return the existing
redacted row without changing its body, metadata, edit baseline, index or audit,
regardless of source key, relay/direct delivery, or age. Any redacted mailbox row
is excluded from duplicate body enrichment. This freezes the selected message,
including after a partial-span redaction; new provider edits are not accepted as
permission to overwrite it. Other recipients, chats and provider message ids
remain independent.

Read `full_scrub`, `scrubbed_paths` and `unscrubbed` in the returned receipt.
Unreadable/invalid JSONL, symlinked/hard-linked sessions, known binary harness
stores and retained remote-session metadata are explicitly listed by path; no
full scrub is claimed when one is unresolved. This release supports local JSONL
session stores, including Sylphx and Claude Code, not binary/remote harness stores.
External providers, OS/filesystem snapshots, backups and copies outside the
target's managed config directory are outside the local erasure guarantee.

A receipt is persisted with pending file paths **before** file mutation. If the
process stops mid-operation, replay returns that existing incomplete receipt;
it never invents success or persists sensitive recovery data. Operators must
resolve any listed copies separately before treating an incident as contained.
A partial filesystem failure does not roll back already-erased canonical data.

## Prevention boundary

Sylphx currently appends tool outputs directly to its lossless transcript. There
is no generic secret-masking stage at `append_transcript_message`; credential
reference handling can keep known credentials out of tool inputs, but arbitrary
player-provided strings in successful tool results are not classified or masked.
Redaction deliberately does not add a speculative secret classifier.
