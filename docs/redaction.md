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
FTS content/tokens and every bound Telegram edit baseline are updated. A
whole-body erasure (including an explicit span covering the entire body)
unconditionally tombstones every account-bound baseline, even if a later quote,
envelope or edit made its text differ from the current body. Each baseline is
read back and verified before success is claimed. FTS is rebuilt with secure
deletion, SQLite secure deletion is enabled before the rewrite and the WAL is
checkpointed/truncated. The audit records actor, time, permission, reason, target,
span, removed byte/character counts and copy outcomes—**never original text or a
hash of it**. Replay of the original target/span, including equivalent mailbox
id/seq selectors, returns the existing audit id and receipt without a second edit.
A replay verifies the current canonical tombstone and, for a whole-body erasure,
every bound baseline before returning that receipt; if either has drifted, the
operation refuses instead of presenting a stale receipt as proof of erasure.
Replays still require a quiescent target. Coordinates are otherwise relative to
the body's current version.

An inbound redaction binds its recipient and **every retained provider origin and
source-key alias**, not only the row's current `(chat_id, message_id)`. In basic
Telegram groups, an origin includes its **bot/account namespace**: the direct
adapter's bot key or the relay's receiving account, plus chat and message id.
The early erasure guard and ordinary numeric matcher use the same namespace;
a numeric collision from another bot is a normal independent delivery, never
an erased alias. Exact source keys remain exact. Unknown relay/source formats
are limited to their exact keys; a missing basic-group account is not inferred
from a number. Non-basic chats retain the ordinary matcher's shared-chat
coordinate semantics. Ingress retains both sides of source deduplication,
relay/direct coalescence and origin promotion, including intentional late-quote redelivery. Later deliveries or edits
using any known, correctly namespaced alias return the selected redacted row
without changing its body, metadata, edit baseline, index or audit, regardless of
age. A known alias can add another routing identity without accepting its content. All matching edit
baselines under retained account-qualified origins are scrubbed. A legacy
baseline without provable account ownership is preserved and explicitly listed
in `unscrubbed`, rather than erasing another bot's data by a colliding number.
A redacted identity is excluded from duplicate body enrichment, including after
partial-span erasure; a new edit is not permission to overwrite it. Other recipients, chats and independent
provider messages remain independent; a separate existing mailbox entry is not
erased implicitly. Rows created by an older binary retain their still-available
origin/source key on first use, but aliases already discarded before this release
cannot be reconstructed.

Read `full_scrub`, `scrubbed_paths` and `unscrubbed` in the returned receipt.
Unreadable/invalid JSONL, symlinked/hard-linked sessions, known binary harness
stores and retained remote-session metadata are explicitly listed by path; no
full scrub is claimed when one is unresolved. Baseline write/verification
failures and unknown legacy baseline accounts are also listed. If a SQLite
reader prevents WAL truncation, `full_scrub=false` and the WAL path records the
residual window: old bytes may remain until that reader releases its snapshot
and a successful `wal_checkpoint(TRUNCATE)` completes. Logical SELECT/search
erasure alone is not a physical-erasure receipt. This release supports local JSONL
session stores, including Sylphx and Claude Code, not binary/remote harness stores.
External providers, OS/filesystem snapshots, backups and copies outside the
target's managed config directory are outside the local erasure guarantee.

A receipt is persisted with pending file paths **before** file mutation. If the
process stops mid-file-operation, replay returns that existing incomplete receipt;
it never invents success or persists sensitive recovery data. Operators must
resolve any listed copies separately before treating an incident as contained.
A partial filesystem failure does not roll back already-erased canonical data.

## Prevention boundary

Sylphx currently appends tool outputs directly to its lossless transcript. There
is no generic secret-masking stage at `append_transcript_message`; credential
reference handling can keep known credentials out of tool inputs, but arbitrary
player-provided strings in successful tool results are not classified or masked.
Redaction deliberately does not add a speculative secret classifier.
