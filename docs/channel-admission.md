# Telegram caller and root seat admission

## Telegram inbound callers

Set `allowed_user_ids` and/or `allowed_chat_ids` in a Telegram channel's `config`:

```json
{"allowed_user_ids":[123456789],"allowed_chat_ids":[-1009876543210]}
```

An update must match each configured list. Omit a list to leave that dimension unrestricted. An empty list denies all updates, and a missing ID cannot match a list for that dimension. User IDs are positive integers; chat IDs are nonzero integers.

If neither list is configured, the daemon can set `ANYAGENTS_TELEGRAM_ALLOWED_IDS` (or `ALLOWED_TELEGRAM_IDS` if the first variable is unset). Use a comma-separated list or JSON array of integers. Positive numbers match sender IDs; negative numbers match chat IDs. An empty fallback denies all on channels that use it. A malformed channel allowlist or malformed selected environment value denies all. Without channel or environment policy, routing is unchanged.

The same policy applies to webhooks, polling, and delivery to peer bots. Denied updates advance the update cursor but do not link a chat or deliver a message. This policy does not restrict outbound sends.

## Receiver-scoped ingress blocks

The coordinator checks Telegram ingress rules after the channel caller allowlist and before durable mailbox delivery, for both polling and webhooks. The same check runs separately for each agent receiving a group relay. A blocked delivery adds no mailbox entry and does not wake that agent. Allowed external input includes a host-generated `sender_awareness` header; display names and message text never confer authority.

Set `ANYAGENTS_TELEGRAM_OWNER_IDS`, `ANYAGENTS_TELEGRAM_ADMIN_IDS`, or `ANYAGENTS_TELEGRAM_TRUSTED_IDS` to JSON arrays of positive Telegram user IDs on the coordinator. Unset arrays are empty; malformed settings reject ingress and block creation rather than risk misclassifying a protected sender. Owner and admin messages bypass receiver blocks, not the separate caller allowlist. Authenticated agent-to-agent delivery is not subject to external blocks. Trusted external users are distinct from protected senders and may be blocked.

Agents manage their own rules with the `ingress_guard` MCP tool. Operators use `GET /admin/api/agents/{id}/ingress-blocks` to list, `POST` to block, and `DELETE` to unblock. For example, send this JSON to block a sender:

```json
{"op":"block_sender","target":{"channel":"telegram","scope":{"kind":"sender","sender_id":"123456789"}},"reason":"Repeated spam"}
```

Use the same target with `"op":"unblock_sender"` to remove the rule. Use `{"op":"list_blocks"}` in MCP. For a known group, use `{"kind":"chat","chat_id":"-1009876543210"}` as the scope. Omit `expires_at_ms` for a permanent rule or supply a future Unix timestamp in milliseconds. A permanent group block may also make an exclusively assigned bot leave. Leaving is best-effort and skipped if it could cut off configured protected users; unblocking does not rejoin a group.

## Bootstrap a root agent from a pre-created seat

Pre-create a non-root OS user named `seat-<name>` and configure `ANYAGENTS_SOCKET_GID`. On a confined server, send `POST /v1/agents` with the operator token in `x-anyagents-operator`:

```json
{"name":"worker","harness":"spiron","scope":"isolated","workdir":"/path/to/work"}
```

Optionally pass `"seat_user":"seat-other"` to select a different pre-created seat and/or `"seat_uid":2101` to assert the OS UID. Omit `parent` for a root agent. Agent capabilities cannot use these fields, even if they can create roots. Deployments with a static `ANYAGENTS_PRINCIPALS` map remain governed by that map and do not accept explicit seat selection.

The server rejects UIDs equal to zero or the daemon UID, as well as usernames or UIDs already granted to another agent. A new grant belongs to the immutable agent ID, so renaming the agent does not move the seat. Workdir and execution checks still apply. The successful response includes `seat_user` and `seat_uid`.
