//! Provider origin deduplication, alias coalescing, quote enrichment, and edit diffing.

use super::helpers::message_from_row_with_cursor_db;
use super::ingress::history_read_probe;
use super::mailbox::cursor;
use super::topology::{
    has_quoted_block, resolve_mentions, strip_quoted_block, telegram_sender_user_key,
};
use super::types::{Ack, Message, Origin};
use super::{MESSAGE_COLS, now_ms};
use crate::db::{DbExec, DbExecExt};
use crate::db_params;
use crate::error::Error;
use crate::id::AgentId;

pub(super) fn message_by_source(
    conn: &dyn DbExec,
    agent_id: &AgentId,
    source_key: &str,
) -> Result<Option<Message>, Error> {
    let found = conn.db_query_opt(
        concat!(
            "SELECT ",
            message_cols!(),
            " FROM messages WHERE agent_id = ?1 AND source_key = ?2"
        ),
        &db_params![agent_id.as_str(), source_key],
        |row| message_from_row_with_cursor_db(row, 0),
    )?;
    let Some(mut msg) = found else {
        return Ok(None);
    };
    let delivered = cursor(conn, &msg.agent_id)?;
    msg.ack = if msg.seq <= delivered {
        Ack::Delivered
    } else {
        Ack::Received
    };
    Ok(Some(msg))
}

/// Namespace of the ordinary numeric-origin matcher. Telegram basic-group
/// ids are bot/account-local; a relay's receiver is NOT the target agent's bot.
/// Unknown relay formats are scoped to their exact key, never guessed globally.
/// Non-basic chats retain the ordinary matcher's shared chat-id semantics.
pub(crate) fn origin_namespace(chat_id: i64, source_key: Option<&str>) -> String {
    if !(chat_id < 0 && chat_id > -1_000_000_000_000) {
        return "shared-chat".into();
    }
    let Some(key) = source_key.filter(|k| !k.is_empty()) else {
        return "unscoped".into();
    };
    if let Some(rest) = key.strip_prefix("tg-in:") {
        let parts: Vec<_> = rest.split(':').collect();
        if let [chat, account, message] = parts.as_slice()
            && chat.parse::<i64>() == Ok(chat_id)
            && !account.is_empty()
            && message.parse::<i64>().is_ok()
        {
            return format!("relay:{account}");
        }
        return format!("source:{key}");
    }
    if key.starts_with("room:") {
        return format!("source:{key}");
    }
    // Direct adapter keys are <bot>:<update_id>. The explicit webhook fixture
    // form also carries its account before the final event component.
    if let Some((account, _event)) = key.rsplit_once(':') {
        return format!("direct:{account}");
    }
    format!("source:{key}")
}

fn message_origin_namespace_matches(
    conn: &dyn DbExec,
    message: &Message,
    chat: i64,
    incoming_key: Option<&str>,
) -> Result<bool, Error> {
    let namespace = origin_namespace(chat, incoming_key);
    if namespace == "unscoped" {
        return Ok(false);
    }
    let (retained, matched): (i64, bool) = conn.db_query_one(
        "SELECT COUNT(*), COALESCE(MAX(CASE WHEN a.namespace = ?3 THEN 1 ELSE 0 END), 0)
         FROM message_origin_aliases a
         JOIN message_identities i ON i.agent_id = a.agent_id AND i.identity_id = a.identity_id
         WHERE i.agent_id = ?1 AND i.target_id = ?2 AND a.chat_id = ?4 AND a.message_id = ?5",
        &db_params![
            message.agent_id.as_str(),
            message.id.as_str(),
            namespace,
            chat,
            message.origin.and_then(|o| o.message_id)
        ],
        |r| Ok((r.get::<i64>(0)?, r.get::<i64>(1)? != 0)),
    )?;
    Ok(if retained != 0 {
        matched
    } else {
        namespace == origin_namespace(chat, message.source_key.as_deref())
    })
}

/// Retain both sides of every coalescence before an origin/source promotion.
/// This also lazily seeds a pre-upgrade row's still-available canonical identity.
/// Callers share ingress/redaction's IMMEDIATE transaction; errors roll it back.
pub(super) fn retain_message_aliases(
    conn: &dyn DbExec,
    message: &Message,
    incoming_origin: Option<Origin>,
    incoming_source_key: Option<&str>,
) -> Result<String, Error> {
    let seeded = conn.db_execute(
        "INSERT INTO message_identities (agent_id, target_id, identity_id) VALUES (?1, ?2, ?2) ON CONFLICT DO NOTHING",
        &db_params![message.agent_id.as_str(), message.id.as_str()],
    )?;
    let identity_id: String = conn.db_query_one(
        "SELECT identity_id FROM message_identities WHERE agent_id = ?1 AND target_id = ?2",
        &db_params![message.agent_id.as_str(), message.id.as_str()],
        |r| r.get(0),
    )?;
    // Only seed the visible route once. After promotion source_key may be
    // cleared; re-inferring its account then would manufacture a broad alias.
    for (origin, key) in [
        (
            message.origin.filter(|_| seeded != 0),
            message.source_key.as_deref(),
        ),
        (incoming_origin, incoming_source_key),
    ] {
        if let Some(Origin {
            chat_id: Some(chat_id),
            message_id: Some(message_id),
            ..
        }) = origin
        {
            conn.db_execute(
                "INSERT INTO message_origin_aliases (agent_id, identity_id, namespace, chat_id, message_id)
                 VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT DO NOTHING",
                &db_params![message.agent_id.as_str(), identity_id, origin_namespace(chat_id, key), chat_id, message_id],
            )?;
        }
    }
    for key in [message.source_key.as_deref(), incoming_source_key]
        .into_iter()
        .flatten()
        .filter(|key| !key.is_empty())
    {
        conn.db_execute(
            "INSERT INTO message_source_aliases (agent_id, identity_id, source_key)
             VALUES (?1, ?2, ?3) ON CONFLICT DO NOTHING",
            &db_params![message.agent_id.as_str(), identity_id, key],
        )?;
    }
    Ok(identity_id)
}

/// Resolve every retained origin/source alias of an explicitly redacted row,
/// before any ordinary writer. No bounded history scan or body matching. A
/// missing canonical row is a storage error, not permission to append plaintext.
pub(super) fn redacted_message_by_ingress(
    conn: &dyn DbExec,
    agent_id: &AgentId,
    origin: Option<Origin>,
    source_key: Option<&str>,
) -> Result<Option<Message>, Error> {
    let target_id: Option<String> = conn.db_query_opt(
        "SELECT r.target_id FROM message_origin_aliases a
         JOIN message_identities i ON i.agent_id = a.agent_id AND i.identity_id = a.identity_id
         JOIN conversation_redactions r ON r.agent_id = i.agent_id AND r.target_id = i.target_id
         WHERE a.agent_id = ?1 AND a.chat_id = ?2 AND a.message_id = ?3 AND a.namespace = ?5 AND a.namespace != 'unscoped'
         UNION ALL
         SELECT r.target_id FROM message_source_aliases a
         JOIN message_identities i ON i.agent_id = a.agent_id AND i.identity_id = a.identity_id
         JOIN conversation_redactions r ON r.agent_id = i.agent_id AND r.target_id = i.target_id
         WHERE a.agent_id = ?1 AND a.source_key = ?4
         LIMIT 1",
        &db_params![
            agent_id.as_str(),
            origin.and_then(|o| o.chat_id),
            origin.and_then(|o| o.message_id),
            source_key,
            origin
                .and_then(|o| o.chat_id)
                .map(|chat| origin_namespace(chat, source_key))
        ],
        |r| r.get(0),
    )?;
    let Some(target_id) = target_id else {
        return Ok(None);
    };
    let delivered = cursor(conn, agent_id)?;
    let row = conn.db_query_one(
        &format!("SELECT {MESSAGE_COLS} FROM messages WHERE agent_id = ?1 AND id = ?2"),
        &db_params![agent_id.as_str(), target_id],
        |r| message_from_row_with_cursor_db(r, delivered),
    )?;
    Ok(Some(row))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn message_by_origin(
    conn: &dyn DbExec,
    agent_id: &AgentId,
    chat_id: i64,
    message_id: i64,
    incoming_sender: Option<&str>,
    incoming_source_time_ms: Option<i64>,
    incoming_body: &str,
    incoming_source_key: Option<&str>,
    incoming_reply_to_author: Option<&str>,
    redelivery_identity: &mut Option<String>,
    merged: &mut Vec<String>,
) -> Result<Option<Message>, Error> {
    let rows = conn.db_query_vec(
        &format!(
            "SELECT {MESSAGE_COLS} FROM messages
             WHERE agent_id = ?1 AND origin IS NOT NULL ORDER BY seq DESC LIMIT 64"
        ),
        &db_params![agent_id.as_str()],
        |row| message_from_row_with_cursor_db(row, 0),
    )?;
    if rows.is_empty() {
        return Ok(None);
    }
    let delivered = cursor(conn, agent_id)?;
    let in_user = telegram_sender_user_key(incoming_sender);
    let in_is_relay =
        incoming_source_key.is_some_and(|k| k.starts_with("tg-in:") || k.starts_with("room:"));
    let is_basic_group = chat_id < 0 && chat_id > -1_000_000_000_000;
    for mut msg in rows {
        msg.ack = if msg.seq <= delivered {
            Ack::Delivered
        } else {
            Ack::Received
        };
        let Some(origin) = msg.origin else {
            continue;
        };
        if origin.chat_id != Some(chat_id) {
            continue;
        }
        let stored_is_relay = msg
            .source_key
            .as_deref()
            .is_some_and(|k| k.starts_with("tg-in:") || k.starts_with("room:"));
        let incoming_is_meta_event = incoming_body.starts_with("[Reaction ")
            || incoming_body.starts_with("[Deleted message #");
        let stored_is_meta_event =
            msg.text.starts_with("[Reaction ") || msg.text.starts_with("[Deleted message #");
        let matched_by_id = origin.message_id == Some(message_id)
            && message_origin_namespace_matches(conn, &msg, chat_id, incoming_source_key)?
            && if incoming_is_meta_event || stored_is_meta_event {
                strip_quoted_block(&msg.text) == strip_quoted_block(incoming_body)
                    && in_user == telegram_sender_user_key(msg.sender.as_deref())
            } else {
                true
            };
        // Basic Telegram groups (`chat_type == "group"`, negative non-`-100...` IDs)
        // assign each bot its own account-local `message_id` for the same human
        // message. Match by `(chat_id, sender user, source_time_ms, body)` so cross-bot
        // passive relay and direct webhook delivery coalesce into a single message.
        let matched_by_group_relay = (in_is_relay
            || stored_is_relay
            || (is_basic_group && origin.message_id == Some(message_id)))
            && normalize_telegram_body_for_dedup(strip_quoted_block(&msg.text))
                == normalize_telegram_body_for_dedup(strip_quoted_block(incoming_body))
            && in_user.is_some()
            && in_user == telegram_sender_user_key(msg.sender.as_deref())
            && match (msg.source_time_ms, incoming_source_time_ms) {
                (Some(a), Some(b)) => (a - b).abs() <= 5_000,
                _ => (now_ms() - msg.received_at_ms).abs() <= 15_000,
            };
        if matched_by_id || matched_by_group_relay {
            // Keep BOTH the old and incoming route before promotion clears the
            // source key or replaces the canonical origin, even without a body
            // change and before the late-quote redelivery branch returns None.
            let identity_id = retain_message_aliases(
                conn,
                &msg,
                Some(Origin {
                    chat_id: Some(chat_id),
                    message_id: Some(message_id),
                    thread_id: origin.thread_id,
                }),
                incoming_source_key,
            )?;
            // This canonical identity (including intentional late-quote
            // redeliveries) may have been redacted. Re-resolve after retaining
            // the new alias and return the actual frozen row before either
            // body UPDATE. This shares ingress/redaction's IMMEDIATE transaction.
            let redacted_target: Option<String> = conn.db_query_opt(
                "SELECT r.target_id FROM conversation_redactions r
                 JOIN message_identities i ON i.agent_id=r.agent_id AND i.target_id=r.target_id
                 WHERE i.agent_id=?1 AND i.identity_id=?2 LIMIT 1",
                &db_params![agent_id.as_str(), identity_id],
                |r| r.get(0),
            )?;
            if let Some(target) = redacted_target {
                let redacted = conn.db_query_one(
                    &format!("SELECT {MESSAGE_COLS} FROM messages WHERE agent_id=?1 AND id=?2"),
                    &db_params![agent_id.as_str(), target],
                    |r| message_from_row_with_cursor_db(r, delivered),
                )?;
                return Ok(Some(redacted));
            }
            let was_unquoted = !has_quoted_block(&msg.text) && has_quoted_block(incoming_body);
            let best_body = if was_unquoted || incoming_body.len() > msg.text.len() {
                Some(incoming_body.to_string())
            } else {
                None
            };
            if !in_is_relay && stored_is_relay {
                let mut updated_origin = origin;
                updated_origin.message_id = Some(message_id);
                let mut updated_context = msg.context.clone();
                updated_context.message_id = Some(message_id);
                if updated_context.reply_to_author.is_none()
                    && let Some(rta) = incoming_reply_to_author
                {
                    updated_context.reply_to_author = Some(rta.to_string());
                }
                let new_body = best_body.as_deref().unwrap_or(&msg.text);
                conn.db_execute(
                    "UPDATE messages SET origin = ?1, context = ?2, source_key = NULL, body = ?3 WHERE id = ?4",
                    &db_params![
                        serde_json::to_string(&updated_origin).unwrap_or_default(),
                        serde_json::to_string(&updated_context).ok(),
                        new_body,
                        msg.id.as_str(),
                    ],
                )?;
                msg.origin = Some(updated_origin);
                msg.context = updated_context;
                msg.source_key = None;
                if let Some(b) = best_body {
                    if b != msg.text {
                        merged.push(msg.id.to_string());
                    }
                    msg.text = b;
                }
            } else if best_body.is_some()
                || (msg.context.reply_to_author.is_none() && incoming_reply_to_author.is_some())
            {
                let mut updated_context = msg.context.clone();
                if updated_context.reply_to_author.is_none()
                    && let Some(rta) = incoming_reply_to_author
                {
                    updated_context.reply_to_author = Some(rta.to_string());
                }
                let new_body = best_body.as_deref().unwrap_or(&msg.text);
                conn.db_execute(
                    "UPDATE messages SET body = ?1, context = ?2 WHERE id = ?3",
                    &db_params![
                        new_body,
                        serde_json::to_string(&updated_context).ok(),
                        msg.id.as_str(),
                    ],
                )?;
                msg.context = updated_context;
                if let Some(b) = best_body {
                    if b != msg.text {
                        merged.push(msg.id.to_string());
                    }
                    msg.text = b;
                }
            }
            if was_unquoted && delivered >= msg.seq {
                *redelivery_identity = Some(identity_id);
                return Ok(None);
            }
            return Ok(Some(msg));
        }
    }
    Ok(None)
}

fn normalize_telegram_body_for_dedup(s: &str) -> String {
    let mut lines = Vec::new();
    for line in s.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("[Local media (") && trimmed.ends_with(']') {
            continue;
        }
        if (trimmed.starts_with("[Attachment (") || trimmed.starts_with("[Sticker"))
            && let Some(pos) = line.find("file_id=")
        {
            let after = &line[pos + "file_id=".len()..];
            let end_rel = after.find([',', ']']).unwrap_or(after.len());
            let mut norm = String::with_capacity(line.len());
            norm.push_str(&line[..pos + "file_id=".len()]);
            norm.push('*');
            norm.push_str(&after[end_rel..]);
            lines.push(norm);
        } else {
            lines.push(line.to_string());
        }
    }
    lines.join("\n")
}

pub(super) fn resolve_quote_author(conn: &dyn DbExec, quote: &crate::Quote) -> Option<String> {
    let author = quote.author.trim();
    if author.is_empty() {
        return None;
    }
    let resolved = resolve_mentions(conn, author);
    for entry in &resolved {
        if let Some((_, rest)) = entry.split_once("(agent:")
            && let Some(agent) = rest.strip_suffix(')')
        {
            return Some(format!("{author} [agent:{agent}]"));
        }
    }
    if quote.author_is_bot {
        let first_word = author
            .split(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '-')
            .next()
            .unwrap_or("");
        if !first_word.is_empty()
            && let Ok(Some(agent)) = conn.db_query_opt(
                "SELECT name FROM agents WHERE LOWER(name) = LOWER(?1)",
                &db_params![first_word],
                |row| row.get::<String>(0),
            )
        {
            return Some(format!("{author} [agent:{agent}]"));
        }
    }
    Some(author.to_string())
}

pub(super) fn enrich_quote_from_store(
    conn: &dyn DbExec,
    quote: &mut crate::Quote,
    incoming_origin: Option<Origin>,
) {
    let chat_id = quote
        .reference
        .as_ref()
        .map(|r| r.chat_id)
        .or_else(|| incoming_origin.and_then(|o| o.chat_id));
    let Some(chat_id) = chat_id else {
        return;
    };
    let mut stored_excerpt = quote
        .reference
        .as_ref()
        .and_then(|r| resolve_origin_message_author_and_excerpt(conn, r.chat_id, r.message_id))
        .map(|(_, excerpt)| excerpt);
    if stored_excerpt.is_none() {
        let base_quote_text: String = quote
            .text
            .lines()
            .filter(|line| {
                let t = line.trim();
                !((t.starts_with("[Attachment (") || t.starts_with("[Local media ("))
                    && t.ends_with(']'))
            })
            .collect::<Vec<_>>()
            .join("\n")
            .trim()
            .to_string();
        if !base_quote_text.is_empty() {
            let dest_pattern = format!("telegram:chat:{chat_id}%");
            if let Ok(bodies) = conn.db_query_vec(
                "SELECT body FROM outbound_messages
                 WHERE destination LIKE ?1
                 ORDER BY sent_at_ms DESC LIMIT 32",
                &db_params![dest_pattern],
                |row| row.get::<String>(0),
            ) {
                for body in bodies {
                    let clean = strip_quoted_block(&body).trim().to_string();
                    if clean.starts_with(&base_quote_text) && clean.len() > base_quote_text.len() {
                        stored_excerpt = Some(clean);
                        break;
                    }
                }
            }
        }
    }
    let Some(excerpt) = stored_excerpt else {
        return;
    };
    if quote.text.trim().is_empty() {
        quote.text = excerpt;
        return;
    }
    for line in excerpt.lines() {
        let trimmed = line.trim();
        if (trimmed.starts_with("[Attachment (") || trimmed.starts_with("[Local media ("))
            && trimmed.ends_with(']')
            && !quote.text.contains(trimmed)
        {
            if !quote.text.ends_with('\n') {
                quote.text.push('\n');
            }
            quote.text.push_str(trimmed);
        }
    }
}

pub(super) fn resolve_origin_message_author_and_excerpt(
    conn: &dyn DbExec,
    chat_id: i64,
    message_id: i64,
) -> Option<(String, String)> {
    history_read_probe();
    let pattern = format!("%\"message_id\":{message_id}%");
    let rows = conn
        .db_query_vec(
            "SELECT sender, body, origin FROM messages
             WHERE origin LIKE ?1
               AND body NOT LIKE '[Reaction %'
               AND body NOT LIKE '[Deleted message #%'
             ORDER BY rowid DESC LIMIT 32",
            &db_params![pattern],
            |row| {
                Ok((
                    row.get::<Option<String>>(0)?,
                    row.get::<String>(1)?,
                    row.get::<Option<String>>(2)?,
                ))
            },
        )
        .ok()?;
    for (sender, body, origin_raw) in rows {
        let Some(origin_str) = origin_raw else {
            continue;
        };
        let Ok(origin) = serde_json::from_str::<Origin>(&origin_str) else {
            continue;
        };
        if origin.chat_id == Some(chat_id) && origin.message_id == Some(message_id) {
            let author = sender.unwrap_or_else(|| "unknown".to_string());
            let clean = strip_quoted_block(&body).trim().to_string();
            return Some((author, clean));
        }
    }
    None
}

pub(super) fn parse_edited_message_text(text: &str) -> Option<(i64, &str)> {
    let rest = text.strip_prefix("[Edited message #")?;
    let (id_part, body_part) = rest.split_once("]:")?;
    let msg_id = id_part.trim().parse::<i64>().ok()?;
    let raw = body_part.strip_prefix(' ').unwrap_or(body_part);
    Some((msg_id, raw))
}

/// The last version of an edited message this agent recorded: one indexed
/// lookup, made inside the ingress transaction.
pub(super) fn stored_message_version(
    conn: &dyn DbExec,
    agent_id: &AgentId,
    chat_id: i64,
    message_id: i64,
    source_key: Option<&str>,
) -> Option<String> {
    conn.db_query_opt(
        "SELECT body FROM message_versions
         WHERE agent_id = ?1 AND chat_id = ?2 AND message_id = ?3 AND origin_namespace = ?4",
        &db_params![
            agent_id.as_str(),
            chat_id,
            message_id,
            origin_namespace(chat_id, source_key)
        ],
        |row| row.get::<String>(0),
    )
    .ok()
    .flatten()
}

/// The text of an edited message's original delivery, found in the stored
/// history when no version was recorded. This reads across the whole mailbox
/// history, so ingress runs it on a read snapshot before taking the writer.
pub(super) fn original_message_text(
    conn: &dyn DbExec,
    agent_id: &AgentId,
    chat_id: i64,
    message_id: i64,
    source_key: Option<&str>,
) -> Option<String> {
    history_read_probe();
    let pattern = format!("%\"message_id\":{message_id}%");
    let rows = conn
        .db_query_vec(
            &format!(
                "SELECT {MESSAGE_COLS} FROM messages WHERE origin LIKE ?1
               AND body NOT LIKE '[Edited message #%'
               AND body NOT LIKE '[Reaction %'
               AND body NOT LIKE '[Deleted message #%'
             ORDER BY CASE WHEN agent_id=?2 THEN 0 ELSE 1 END, rowid DESC LIMIT 32"
            ),
            &db_params![pattern, agent_id.as_str()],
            |row| message_from_row_with_cursor_db(row, 0),
        )
        .ok()?;
    for message in rows {
        if let Some(origin) = message.origin
            && origin.chat_id == Some(chat_id)
            && origin.message_id == Some(message_id)
            && message_origin_namespace_matches(conn, &message, chat_id, source_key).ok()?
        {
            return Some(strip_quoted_block(&message.text).trim().to_string());
        }
    }
    None
}

pub(super) fn latest_message_for_origin(
    conn: &dyn DbExec,
    agent_id: &AgentId,
    chat_id: i64,
    message_id: i64,
    source_key: Option<&str>,
) -> Result<Option<Message>, Error> {
    let pattern = format!("%\"message_id\":{message_id}%");
    let rows = conn.db_query_vec(
        &format!(
            "SELECT {MESSAGE_COLS} FROM messages
             WHERE agent_id = ?1 AND origin LIKE ?2
             ORDER BY seq DESC LIMIT 16"
        ),
        &db_params![agent_id.as_str(), pattern],
        |row| message_from_row_with_cursor_db(row, 0),
    )?;
    for mut msg in rows {
        if let Some(origin) = msg.origin
            && origin.chat_id == Some(chat_id)
            && origin.message_id == Some(message_id)
            && message_origin_namespace_matches(conn, &msg, chat_id, source_key)?
        {
            let delivered = cursor(conn, agent_id)?;
            msg.ack = if msg.seq <= delivered {
                Ack::Delivered
            } else {
                Ack::Received
            };
            return Ok(Some(msg));
        }
    }
    Ok(None)
}

pub fn format_edited_message_diff(old: &str, new: &str) -> String {
    let old_trimmed = old.trim();
    let new_trimmed = new.trim();
    if old_trimmed.is_empty() {
        return new_trimmed.to_string();
    }
    let old_lines: Vec<&str> = old_trimmed.lines().map(str::trim_end).collect();
    let new_lines: Vec<&str> = new_trimmed.lines().map(str::trim_end).collect();
    let n = old_lines.len();
    let m = new_lines.len();
    if n * m > 120_000 {
        return new_trimmed.to_string();
    }
    let mut dp = vec![0u16; (n + 1) * (m + 1)];
    let stride = m + 1;
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            dp[i * stride + j] = if old_lines[i] == new_lines[j] {
                dp[(i + 1) * stride + j + 1].saturating_add(1)
            } else {
                dp[(i + 1) * stride + j].max(dp[i * stride + j + 1])
            };
        }
    }
    #[derive(Clone, Copy)]
    enum DiffOp<'a> {
        Equal(&'a str),
        Remove(&'a str),
        Add(&'a str),
    }
    let mut ops = Vec::with_capacity(n + m);
    let (mut i, mut j) = (0, 0);
    while i < n && j < m {
        if old_lines[i] == new_lines[j] {
            ops.push(DiffOp::Equal(old_lines[i]));
            i += 1;
            j += 1;
        } else if dp[(i + 1) * stride + j] >= dp[i * stride + j + 1] {
            ops.push(DiffOp::Remove(old_lines[i]));
            i += 1;
        } else {
            ops.push(DiffOp::Add(new_lines[j]));
            j += 1;
        }
    }
    while i < n {
        ops.push(DiffOp::Remove(old_lines[i]));
        i += 1;
    }
    while j < m {
        ops.push(DiffOp::Add(new_lines[j]));
        j += 1;
    }
    let mut keep = vec![false; ops.len()];
    for (idx, op) in ops.iter().enumerate() {
        if matches!(op, DiffOp::Remove(_) | DiffOp::Add(_)) {
            let lo = idx.saturating_sub(1);
            let hi = (idx + 1).min(ops.len().saturating_sub(1));
            for k in &mut keep[lo..=hi] {
                *k = true;
            }
        }
    }
    let mut out = Vec::new();
    let mut skipped_run = false;
    for (idx, op) in ops.iter().enumerate() {
        if !keep[idx] {
            if !skipped_run {
                out.push("...".to_string());
                skipped_run = true;
            }
            continue;
        }
        skipped_run = false;
        match op {
            DiffOp::Equal(line) => out.push(format!("  {line}")),
            DiffOp::Remove(line) => out.push(format!("- {line}")),
            DiffOp::Add(line) => out.push(format!("+ {line}")),
        }
    }
    out.join("\n")
}
