//! Cross-bot Telegram group content-identity deduplication and transaction serialization.
//!
//! In Telegram basic groups (`chat_id < 0`), Telegram assigns each bot
//! its own account-local `message_id` for the exact same human message. Deduplicating only
//! on `source_key = telegram:<chat_id>:<message_id>` lets every bot in the group deliver a
//! separate copy into the same agent's mailbox.
//!
//! This module computes a canonical cross-bot content-identity alias key
//! `(chat_id, sender_user, source_time_ms, sha256(normalized_body))`, persists it in
//! `message_source_aliases`, checks it in O(1) inside `receive_on`, and serializes
//! concurrent PostgreSQL webhook transactions per `(agent_id, chat_id, sender_user)` with
//! `pg_advisory_xact_lock` so simultaneous multi-bot deliveries collapse to strictly 1 row.

use super::helpers::message_from_row_with_cursor_db;
use super::mailbox::cursor;
use super::origin::normalize_telegram_body_for_dedup;
use super::topology::{strip_quoted_block, telegram_sender_user_key};
use super::types::{Ack, Message, Origin};
use crate::db::{DbBackend, DbExec, DbExecExt};
use crate::db_params;
use crate::error::Error;
use crate::id::AgentId;
use crate::learned_memory::sha256;

const TG_GROUP_DEDUP_PREFIX: &str = "tg-group-dedup:";

/// Extract the Telegram group `chat_id` (`< 0`) from an explicit `Origin` or a
/// Telegram `source_key` (`telegram:<chat_id>:<msg_id>` / `tg-in:<chat_id>:...`).
pub(super) fn telegram_group_chat_id(
    origin: Option<Origin>,
    source_key: Option<&str>,
) -> Option<i64> {
    if let Some(chat_id) = origin.and_then(|o| o.chat_id) {
        return (chat_id < 0).then_some(chat_id);
    }
    let key = source_key?.trim();
    let rest = key
        .strip_prefix("telegram:")
        .or_else(|| key.strip_prefix("tg-in:"))?;
    let first = rest.split(':').next()?;
    let chat_id = first.parse::<i64>().ok()?;
    (chat_id < 0).then_some(chat_id)
}

fn canonical_group_sender_key(sender: Option<&str>) -> Option<String> {
    if let Some(key) = telegram_sender_user_key(sender) {
        return Some(key);
    }
    let trimmed = sender.map(str::trim).filter(|s| !s.is_empty())?;
    Some(trimmed.to_ascii_lowercase())
}

/// Compute a canonical cross-bot content-identity dedup alias key for a Telegram
/// group message (`chat_id < 0`). Returns `None` for non-group messages, edits,
/// or messages lacking sender/timestamp/body identity.
pub(super) fn telegram_group_dedup_key(
    origin: Option<Origin>,
    source_key: Option<&str>,
    sender: Option<&str>,
    source_time_ms: Option<i64>,
    raw_text: &str,
) -> Option<String> {
    if raw_text.starts_with("[Edited ") {
        return None;
    }
    let chat_id = telegram_group_chat_id(origin, source_key)?;
    let sender_key = canonical_group_sender_key(sender)?;
    let ts_ms = source_time_ms?;
    let normalized = normalize_telegram_body_for_dedup(strip_quoted_block(raw_text));
    if normalized.is_empty() {
        return None;
    }
    let digest = sha256(normalized.as_bytes());
    Some(format!(
        "{TG_GROUP_DEDUP_PREFIX}{chat_id}:{sender_key}:{ts_ms}:{digest}"
    ))
}

/// Acquire a transaction-scoped PostgreSQL advisory lock serializing concurrent
/// multi-bot webhooks for the same `(agent_id, chat_id, sender)` in a group chat.
/// No-op on SQLite (which already serializes writers via `IMMEDIATE` transactions).
pub(super) fn lock_telegram_group_dedup(
    tx: &dyn DbExec,
    agent_id: &AgentId,
    origin: Option<Origin>,
    source_key: Option<&str>,
    sender: Option<&str>,
) -> Result<(), Error> {
    if tx.backend() != DbBackend::Postgres {
        return Ok(());
    }
    let Some(chat_id) = telegram_group_chat_id(origin, source_key) else {
        return Ok(());
    };
    let sender_key = canonical_group_sender_key(sender).unwrap_or_default();
    let scope = format!("tg-group-dedup:{chat_id}:{sender_key}");
    let _ = tx.db_query_opt(
        "SELECT pg_advisory_xact_lock(hashtext(?1), hashtext(?2))",
        &db_params![agent_id.as_str(), scope],
        |_| Ok(()),
    )?;
    Ok(())
}

/// Resolve an existing canonical message by a retained `message_source_aliases` key.
pub(super) fn message_by_source_alias(
    conn: &dyn DbExec,
    agent_id: &AgentId,
    alias_key: &str,
) -> Result<Option<Message>, Error> {
    let target_id: Option<String> = conn.db_query_opt(
        "SELECT i.target_id
         FROM message_source_aliases a
         JOIN message_identities i
           ON i.agent_id = a.agent_id AND i.identity_id = a.identity_id
         WHERE a.agent_id = ?1 AND a.source_key = ?2
         LIMIT 1",
        &db_params![agent_id.as_str(), alias_key],
        |row| row.get(0),
    )?;
    let Some(target_id) = target_id else {
        return Ok(None);
    };
    let found = conn.db_query_opt(
        concat!(
            "SELECT ",
            message_cols!(),
            " FROM messages WHERE agent_id = ?1 AND id = ?2"
        ),
        &db_params![agent_id.as_str(), target_id],
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

/// Persist an additional synthetic dedup alias into `message_source_aliases` for
/// the canonical identity of `message`.
pub(super) fn retain_dedup_alias(
    conn: &dyn DbExec,
    agent_id: &AgentId,
    identity_id: &str,
    dedup_key: Option<&str>,
) -> Result<(), Error> {
    let Some(key) = dedup_key.filter(|k| !k.is_empty()) else {
        return Ok(());
    };
    conn.db_execute(
        "INSERT INTO message_source_aliases (agent_id, identity_id, source_key)
         VALUES (?1, ?2, ?3) ON CONFLICT DO NOTHING",
        &db_params![agent_id.as_str(), identity_id, key],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Incoming, NewAgent, ReceiveOutcome, Store};

    #[test]
    fn four_cross_bot_basic_group_webhooks_with_distinct_message_ids_dedup_to_one() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("director", "claude-code"))
            .unwrap();
        let chat_id = -4_200_001_001i64;
        let ts = 1_763_600_000_000i64;
        let sender = "telegram:user:99887766:alice";

        let bot_deliveries = [
            (4065i64, "telegram:-4200001001:4065"),
            (15714i64, "telegram:-4200001001:15714"),
            (83i64, "telegram:-4200001001:83"),
            (5671i64, "telegram:-4200001001:5671"),
        ];

        let mut canonical_id = None;
        let mut new_count = 0usize;
        let mut dup_count = 0usize;
        for (msg_id, src_key) in bot_deliveries {
            let outcome = store
                .receive(
                    &agent.id,
                    Incoming {
                        text: "Ship the release checklist today.".into(),
                        sender: Some(sender.into()),
                        source_time_ms: Some(ts),
                        source_key: Some(src_key.into()),
                        reply_to: Some(format!("telegram:chat:{chat_id}")),
                        quote: None,
                        origin: Some(Origin {
                            chat_id: Some(chat_id),
                            thread_id: None,
                            message_id: Some(msg_id),
                        }),
                        received_at_ms: Some(ts + 50),
                    },
                )
                .unwrap();
            match outcome {
                ReceiveOutcome::New(msg) => {
                    new_count += 1;
                    canonical_id = Some(msg.id);
                }
                ReceiveOutcome::Duplicate(msg) => {
                    dup_count += 1;
                    assert_eq!(Some(msg.id), canonical_id);
                }
            }
        }
        assert_eq!(new_count, 1, "strictly 1 message should be stored");
        assert_eq!(dup_count, 3, "other 3 cross-bot copies must deduplicate");

        let inbox = store.inbox(&agent.id, None).unwrap();
        assert_eq!(inbox.len(), 1);

        // Even after > 64 unrelated messages push the original past the 64-row origin
        // scan window, a late 5th bot delivery for the same basic-group message still
        // deduplicates via `message_source_aliases`.
        for i in 0..70 {
            let out = store
                .receive(
                    &agent.id,
                    Incoming {
                        text: format!("unrelated message {i}"),
                        sender: Some(sender.into()),
                        source_time_ms: Some(ts + 10_000 + i * 1_000),
                        source_key: Some(format!("telegram:{chat_id}:{}", 20_000 + i)),
                        reply_to: Some(format!("telegram:chat:{chat_id}")),
                        quote: None,
                        origin: Some(Origin {
                            chat_id: Some(chat_id),
                            thread_id: None,
                            message_id: Some(20_000 + i),
                        }),
                        received_at_ms: Some(ts + 10_000 + i * 1_000),
                    },
                )
                .unwrap();
            assert!(matches!(out, ReceiveOutcome::New(_)));
        }

        let late_fifth = store
            .receive(
                &agent.id,
                Incoming {
                    text: "Ship the release checklist today.".into(),
                    sender: Some(sender.into()),
                    source_time_ms: Some(ts),
                    source_key: Some("telegram:-4200001001:99999".into()),
                    reply_to: Some(format!("telegram:chat:{chat_id}")),
                    quote: None,
                    origin: Some(Origin {
                        chat_id: Some(chat_id),
                        thread_id: None,
                        message_id: Some(99_999),
                    }),
                    received_at_ms: Some(ts + 90_000),
                },
            )
            .unwrap();
        match late_fifth {
            ReceiveOutcome::Duplicate(msg) => assert_eq!(Some(msg.id), canonical_id),
            ReceiveOutcome::New(_) => {
                panic!("late 5th cross-bot webhook should deduplicate via source alias")
            }
        }
    }
}
