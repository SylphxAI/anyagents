//! Message ingress pipeline: admission, history enrichment, deduplication, and wakes.

use super::helpers::{agent_by_id, agent_exists};
use super::mailbox::next_seq;
use super::origin::{
    enrich_quote_from_store, format_edited_message_diff, latest_message_for_origin,
    message_by_origin, message_by_source, origin_namespace, original_message_text,
    parse_edited_message_text, redacted_message_by_ingress,
    resolve_origin_message_author_and_excerpt, resolve_quote_author, retain_message_aliases,
    stored_message_version,
};
use super::topology::{
    chat_attached_agent_count, compute_and_diff_topology_event, resolve_chat_metadata,
    resolve_conversation_participants, resolve_mentions, resolve_sender_relationship_and_role,
};
use super::types::{Ack, Agent, Incoming, Message, ReceiveOutcome, Wake};
use super::{Store, now_ms};
use crate::db::{DbExec, DbExecExt};
use crate::db_params;
use crate::error::Error;
use crate::id::{AgentId, MessageId};

pub(super) enum ReceiveTx {
    Early(Option<ReceiveOutcome>),
    Inserted {
        message: Message,
        woke_agent: bool,
        is_passive_reaction: bool,
        recipient: Box<Agent>,
    },
}

/// History an incoming line refers to, read before the ingress transaction
/// (see [`Store::ingress_history`]).
#[derive(Default)]
pub(super) struct IngressHistory {
    /// Author and excerpt of the message a reaction reacts to.
    reacted_to: Option<(String, String)>,
    /// The original text of an edited message that has no recorded version.
    edited_from: Option<String>,
    /// For a reaction in a group chat: how many agents the chat is attached
    /// to, capped at the 16 the lookup reads. One or none makes the reaction
    /// targeted rather than passive.
    reaction_chat_agents: Option<usize>,
}

#[cfg(test)]
thread_local! {
    /// Runs where ingress reads stored history; a test writes from another
    /// thread here to prove the writer is free.
    pub(super) static HISTORY_READ_PROBE: std::cell::RefCell<Option<Box<dyn Fn()>>> =
        const { std::cell::RefCell::new(None) };
}

pub(super) fn history_read_probe() {
    #[cfg(test)]
    HISTORY_READ_PROBE.with(|probe| {
        if let Some(probe) = probe.borrow().as_ref() {
            probe();
        }
    });
}

impl Store {
    /// Append one message, or return the row already stored for this source key.
    /// The sequence is assigned inside the insert transaction. This does not
    /// change an archived recipient’s state.
    pub fn receive(&self, agent_id: &AgentId, incoming: Incoming) -> Result<ReceiveOutcome, Error> {
        self.receive_with_admission(agent_id, incoming, false, |_| Ok(true))
            .map(|outcome| outcome.expect("unconditional message admission"))
    }

    /// What ingress reads from the stored conversation history, read on a
    /// snapshot before the writer is taken. These lookups match a channel's
    /// message by text patterns, not by key, so they can visit every stored
    /// message: inside the ingress transaction one Telegram reply held the
    /// writer, and so every write of the daemon, for as long as that scan
    /// took. They only enrich the delivered text; a duplicate found later
    /// under the writer simply discards them.
    fn ingress_history(
        &self,
        incoming: &mut Incoming,
        source_key: Option<&str>,
        agent_id: &AgentId,
    ) -> IngressHistory {
        let origin = incoming.origin;
        let reacted = incoming.text.starts_with("[Reaction ");
        let edited = parse_edited_message_text(&incoming.text).map(|(id, _)| id);
        if incoming.quote.is_none() && !reacted && edited.is_none() {
            return IngressHistory::default();
        }
        let reader = self.read_conn();
        if let Some(quote) = incoming.quote.as_mut() {
            enrich_quote_from_store(&reader, quote, origin);
        }
        let reacted_to = origin
            .filter(|_| reacted)
            .and_then(|o| o.chat_id.zip(o.message_id))
            .and_then(|(chat_id, message_id)| {
                resolve_origin_message_author_and_excerpt(&reader, chat_id, message_id)
            });
        // The recorded version is re-read under the writer; only its absence
        // sends ingress to the history, as before.
        let edited_from = edited.and_then(|edited_id| {
            let chat_id = origin.and_then(|o| o.chat_id)?;
            let message_id = origin.and_then(|o| o.message_id).unwrap_or(edited_id);
            if stored_message_version(&reader, agent_id, chat_id, message_id, source_key).is_some()
            {
                return None;
            }
            original_message_text(&reader, agent_id, chat_id, message_id, source_key)
        });
        // Only a group chat's reaction needs the count; a private chat's is
        // always targeted.
        let reaction_chat_agents = origin
            .filter(|_| reacted)
            .and_then(|o| o.chat_id)
            .filter(|&chat_id| chat_id <= 0)
            .map(|chat_id| chat_attached_agent_count(&reader, chat_id));
        IngressHistory {
            reacted_to,
            edited_from,
            reaction_chat_agents,
        }
    }

    pub(super) fn receive_with_admission(
        &self,
        agent_id: &AgentId,
        incoming: Incoming,
        wake_created: bool,
        admit: impl FnOnce(&dyn DbExec) -> Result<bool, Error>,
    ) -> Result<Option<ReceiveOutcome>, Error> {
        self.receive_with_turn_admission(agent_id, incoming, wake_created, admit, None)
    }

    /// For a mounted stdio tool call, bind a newly committed delivery to the
    /// exact task opener. Neither a skipped delivery nor an idempotent replay
    /// can count as this turn's explicit send.
    pub fn receive_turn_send(
        &self,
        agent_id: &AgentId,
        incoming: Incoming,
        sender: &AgentId,
        token: &str,
    ) -> Result<ReceiveOutcome, Error> {
        self.receive_with_turn_admission(
            agent_id,
            incoming,
            false,
            |_| Ok(true),
            Some((sender, token)),
        )
        .map(|outcome| outcome.expect("unconditional turn send admission"))
    }

    fn receive_with_turn_admission(
        &self,
        agent_id: &AgentId,
        incoming: Incoming,
        wake_created: bool,
        admit: impl FnOnce(&dyn DbExec) -> Result<bool, Error>,
        turn: Option<(&AgentId, &str)>,
    ) -> Result<Option<ReceiveOutcome>, Error> {
        let mut incoming = incoming;
        let source_key = incoming.source_key.take().filter(|key| !key.is_empty());
        let received_at_ms = incoming.received_at_ms.unwrap_or_else(now_ms);
        let history = self.ingress_history(&mut incoming, source_key.as_deref(), agent_id);
        // Stored bodies a channel retry merged a fuller copy into.
        let mut merged = Vec::new();
        let tx_outcome = self.write_tx(|tx| {
            if !admit(tx)? {
                return Ok(ReceiveTx::Early(None));
            }
            if let Some((sender, token)) = turn {
                let sender_agent = agent_by_id(tx, sender)?;
                let active = sender_agent.is_task_subagent()
                    && sender_agent.state != "archived"
                    && tx.db_query_one(
                        "SELECT EXISTS(SELECT 1 FROM task_turn_contexts
                         WHERE child_id = ?1 AND token = ?2 AND closed_delivery_seq IS NULL)",
                        &db_params![sender.as_str(), token],
                        |row| row.get::<bool>(0),
                    )?;
                if !active {
                    return Err(Error::PermissionDenied(
                        "invalid or closed task turn context".into(),
                    ));
                }
            }
            let outcome = Self::receive_on(
                tx,
                agent_id,
                incoming,
                source_key,
                received_at_ms,
                history,
                wake_created,
                None,
                &mut merged,
            )?;
            if let Some((sender, token)) = turn
                && matches!(outcome, ReceiveTx::Inserted { .. })
            {
                tx.db_execute(
                    "UPDATE task_turn_contexts SET sent_to_parent_id = ?3
                     WHERE child_id = ?1 AND token = ?2 AND ?3 = (SELECT parent_id FROM agents WHERE id = ?1)",
                    &db_params![sender.as_str(), token, agent_id.as_str()],
                )?;
            }
            Ok(outcome)
        })?;
        Ok(self.finish_receive(tx_outcome, &merged))
    }

    /// Append under an existing writer. Call finish_receive only after commit.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn receive_on(
        tx: &dyn DbExec,
        agent_id: &AgentId,
        incoming: Incoming,
        source_key: Option<String>,
        received_at_ms: i64,
        history: IngressHistory,
        wake_created: bool,
        trusted_event: Option<&str>,
        merged: &mut Vec<String>,
    ) -> Result<ReceiveTx, Error> {
        // Author validity shares the write transaction with delivery. A sender
        // deleted while waiting for publication cannot borrow a same-spelled
        // display name or produce a stale-principal mailbox/context receipt.
        if let Some(author) = incoming
            .sender
            .as_deref()
            .map(str::trim)
            .and_then(|sender| sender.strip_prefix("agent:"))
            .and_then(|reference| AgentId::parse(reference).ok())
        {
            agent_exists(tx, &author)?;
        }
        agent_exists(tx, agent_id)?;
        // Erasure is authoritative over provider retries/edits, including basic
        // group relays and rows outside the normal bounded origin scan. Refuse
        // resurrection before source dedup, quote/mention enrichment, baseline
        // writes, mailbox allocation or indexing can retain the incoming text.
        let ingress_origin = incoming.origin.map(|mut origin| {
            origin.message_id = origin
                .message_id
                .or_else(|| parse_edited_message_text(&incoming.text).map(|(id, _)| id));
            origin
        });
        super::dedup::lock_telegram_group_dedup(
            tx,
            agent_id,
            ingress_origin,
            source_key.as_deref(),
            incoming.sender.as_deref(),
        )?;
        let group_dedup_key = super::dedup::telegram_group_dedup_key(
            ingress_origin,
            source_key.as_deref(),
            incoming.sender.as_deref(),
            incoming.source_time_ms,
            &incoming.text,
        );
        if let Some(existing) =
            redacted_message_by_ingress(tx, agent_id, ingress_origin, source_key.as_deref())?
        {
            // A known alias can reveal another route (for example a new source
            // key on a known origin). Retain that link without changing the
            // canonical message or audit, so a source-only retry stays erased.
            let id = retain_message_aliases(tx, &existing, ingress_origin, source_key.as_deref())?;
            super::dedup::retain_dedup_alias(tx, agent_id, &id, group_dedup_key.as_deref())?;
            return Ok(ReceiveTx::Early(Some(ReceiveOutcome::Duplicate(existing))));
        }
        if let Some(key) = &source_key
            && let Some(existing) = message_by_source(tx, agent_id, key)?
        {
            let id = retain_message_aliases(tx, &existing, ingress_origin, Some(key))?;
            super::dedup::retain_dedup_alias(tx, agent_id, &id, group_dedup_key.as_deref())?;
            return Ok(ReceiveTx::Early(Some(ReceiveOutcome::Duplicate(existing))));
        }
        let mut body = incoming.body();
        let mut reply_to_author = incoming
            .quote
            .as_ref()
            .and_then(|quote| resolve_quote_author(tx, quote));
        if let Some(origin) = incoming.origin
            && let (Some(chat_id), Some(message_id)) = (origin.chat_id, origin.message_id)
            && let Some((orig_sender, orig_text)) = history.reacted_to
        {
            if reply_to_author.is_none() {
                reply_to_author = Some(orig_sender.clone());
            }
            if incoming.quote.is_none() && !orig_text.is_empty() {
                let synth_quote = crate::Quote {
                    author: orig_sender.clone(),
                    author_is_bot: orig_sender.starts_with("agent:"),
                    sender_id: None,
                    text: orig_text,
                    reference: Some(crate::QuoteRef {
                        chat_id,
                        message_id,
                    }),
                    mentions: Vec::new(),
                };
                body = crate::quote::compose(&incoming.text, Some(&synth_quote));
            }
        }
        let mut redelivery_identity = None;
        if let Some(origin) = incoming.origin
            && let (Some(chat_id), Some(message_id)) = (origin.chat_id, origin.message_id)
            && !incoming.text.starts_with("[Edited ")
            && let Some(existing) = message_by_origin(
                tx,
                agent_id,
                chat_id,
                message_id,
                incoming.sender.as_deref(),
                incoming.source_time_ms,
                &body,
                source_key.as_deref(),
                reply_to_author.as_deref(),
                &mut redelivery_identity,
                merged,
            )?
        {
            let id = retain_message_aliases(tx, &existing, ingress_origin, source_key.as_deref())?;
            super::dedup::retain_dedup_alias(tx, agent_id, &id, group_dedup_key.as_deref())?;
            return Ok(ReceiveTx::Early(Some(ReceiveOutcome::Duplicate(existing))));
        }
        if redelivery_identity.is_none()
            && let Some(dedup_key) = group_dedup_key.as_deref()
            && let Some(existing) = super::dedup::message_by_source_alias(tx, agent_id, dedup_key)?
        {
            let id = retain_message_aliases(tx, &existing, ingress_origin, source_key.as_deref())?;
            super::dedup::retain_dedup_alias(tx, agent_id, &id, Some(dedup_key))?;
            return Ok(ReceiveTx::Early(Some(ReceiveOutcome::Duplicate(existing))));
        }
        if let Some((edited_msg_id, new_raw_text)) = parse_edited_message_text(&incoming.text) {
            let msg_id = incoming
                .origin
                .and_then(|o| o.message_id)
                .unwrap_or(edited_msg_id);
            if let Some(chat_id) = incoming.origin.and_then(|o| o.chat_id) {
                let prev_text =
                    stored_message_version(tx, agent_id, chat_id, msg_id, source_key.as_deref())
                        .or(history.edited_from);
                let _ = tx.db_execute(
                    "INSERT INTO message_versions (agent_id, chat_id, message_id, body, updated_at_ms, origin_namespace)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT (agent_id, chat_id, message_id) DO UPDATE SET
                         body = excluded.body,
                         updated_at_ms = excluded.updated_at_ms
                     WHERE message_versions.origin_namespace = excluded.origin_namespace",
                    &db_params![
                        agent_id.as_str(),
                        chat_id,
                        msg_id,
                        new_raw_text.trim(),
                        received_at_ms,
                        origin_namespace(chat_id, source_key.as_deref())
                    ],
                );
                if let Some(prev) = prev_text {
                    if prev.trim() == new_raw_text.trim()
                        && let Some(existing) = latest_message_for_origin(
                            tx,
                            agent_id,
                            chat_id,
                            msg_id,
                            source_key.as_deref(),
                        )?
                    {
                        retain_message_aliases(
                            tx,
                            &existing,
                            ingress_origin,
                            source_key.as_deref(),
                        )?;
                        return Ok(ReceiveTx::Early(Some(ReceiveOutcome::Duplicate(existing))));
                    }
                    let diff = format_edited_message_diff(&prev, new_raw_text);
                    let formatted_edit = format!("[Edited message #{msg_id}]:\n{diff}");
                    body = match incoming.quote.as_ref() {
                        Some(q) => crate::quote::compose(&formatted_edit, Some(q)),
                        None => formatted_edit,
                    };
                }
            }
        } else if let Some(origin) = incoming.origin
            && let (Some(chat_id), Some(message_id)) = (origin.chat_id, origin.message_id)
            && !incoming.text.starts_with("[Reaction ")
            && !incoming.text.starts_with("[Deleted message #")
            && !incoming.text.starts_with("[Pinned message #")
            && !incoming.text.starts_with("[pinned message")
            && !incoming.text.starts_with("[Member ")
        {
            let _ = tx.db_execute(
                "INSERT INTO message_versions (agent_id, chat_id, message_id, body, updated_at_ms, origin_namespace)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT (agent_id, chat_id, message_id) DO NOTHING",
                &db_params![
                    agent_id.as_str(),
                    chat_id,
                    message_id,
                    incoming.text.trim(),
                    received_at_ms,
                    origin_namespace(chat_id, source_key.as_deref())
                ],
            );
        }
        let mentions = resolve_mentions(tx, &incoming.text);
        let recipient_agent = agent_by_id(tx, agent_id).ok();
        let recipient = recipient_agent
            .as_ref()
            .map(|a| format!("agent:{}", a.name));
        let chat_id_val = incoming.origin.and_then(|origin| origin.chat_id);
        let (chat_title, chat_type) = match chat_id_val {
            Some(cid) => resolve_chat_metadata(tx, cid),
            None => (None, None),
        };
        let event = if let Some(event) = trusted_event {
            Some(event.to_string())
        } else if incoming.text.starts_with("[Pinned message #")
            || incoming.text.starts_with("[pinned message")
        {
            Some("pinned".to_string())
        } else if incoming.text.starts_with("[Edited message #") {
            Some("edited".to_string())
        } else if incoming.text.starts_with("[Deleted message #") {
            Some("deleted".to_string())
        } else if incoming.text.starts_with("[Reaction ") {
            let is_targeted_reaction = chat_id_val.is_none_or(|cid| cid > 0)
                || recipient_agent.as_ref().is_some_and(|a| {
                    let agent_tag = format!("agent:{}", a.name);
                    let bracket_tag = format!("[agent:{}]", a.name);
                    reply_to_author.as_deref().is_some_and(|rta| {
                        rta.eq_ignore_ascii_case(&agent_tag)
                            || rta.ends_with(&bracket_tag)
                            || rta.eq_ignore_ascii_case(&a.name)
                    })
                })
                || history
                    .reaction_chat_agents
                    .is_some_and(|agents| agents <= 1);
            if is_targeted_reaction {
                Some("reaction".to_string())
            } else {
                Some("reaction_passive".to_string())
            }
        } else if incoming.text.starts_with("[Member joined group:")
            || incoming.text.starts_with("[Added to group chat")
        {
            Some("member_joined".to_string())
        } else if incoming.text.starts_with("[Member removed/left group:")
            || incoming.text.starts_with("[Removed from group chat")
        {
            Some("member_left".to_string())
        } else if incoming.text.starts_with("[Sub-Agent Completed]") {
            Some("subagent_completed".to_string())
        } else {
            None
        };
        let resolved_destination = incoming.reply_to.clone().or_else(|| {
            source_key
                .as_deref()
                .and_then(|k| k.strip_prefix("room:"))
                .and_then(|rest| rest.split(':').next())
                .map(|room| {
                    if room.starts_with("group:") || room.starts_with("telegram:") {
                        room.to_string()
                    } else {
                        format!("group:{room}")
                    }
                })
                .or_else(|| {
                    incoming
                        .sender
                        .as_deref()
                        .filter(|s| s.starts_with("agent:"))
                        .map(str::to_string)
                })
        });
        let participants =
            resolve_conversation_participants(tx, chat_id_val, resolved_destination.as_deref());
        let (sender_role, sender_rel) = resolve_sender_relationship_and_role(
            tx,
            recipient_agent.as_ref(),
            incoming.sender.as_deref(),
        );
        let topology = if event.as_deref() == Some("reaction_passive") {
            None
        } else {
            compute_and_diff_topology_event(tx, recipient_agent.as_ref(), received_at_ms)
        };
        // The trusted header is built from the record that arrives, before any
        // field is moved into the row: the author, the channel, the chat and
        // topic, and the reply a bare answer reaches.
        let context = anyagents_harness::Context {
            sender: incoming.sender.clone(),
            sender_name: None,
            sender_role,
            sender_rel,
            recipient,
            channel: incoming
                .sender
                .as_deref()
                .filter(|sender| sender.starts_with("telegram:"))
                .map(|_| "telegram".to_string())
                .or_else(|| chat_id_val.map(|_| "telegram".to_string())),
            chat_id: chat_id_val,
            chat_title,
            chat_type,
            thread_id: incoming.origin.and_then(|origin| origin.thread_id),
            message_id: incoming.origin.and_then(|origin| origin.message_id),
            destination: resolved_destination,
            event,
            reply_to_author,
            mentions,
            participants,
            topology,
            org_instructions: None,
            reply_to_message: None,
            timestamp: None,
            workspace: None,
        };
        let message = Message {
            id: MessageId::generate(),
            agent_id: agent_id.clone(),
            seq: next_seq(tx, agent_id)?,
            text: body,
            sender: incoming.sender,
            source_time_ms: incoming.source_time_ms,
            source_key,
            reply_to: incoming.reply_to,
            received_at_ms,
            ack: Ack::Received,
            delivered_at_ms: None,
            origin: incoming.origin,
            context,
        };
        tx.db_execute(
            "INSERT INTO messages (
                id, agent_id, seq, source_time_ms, source_key, sender, body, reply_to, received_at_ms, origin, context
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            &db_params![
                message.id.as_str(),
                message.agent_id.as_str(),
                message.seq,
                message.source_time_ms,
                message.source_key,
                message.sender,
                message.text,
                message.reply_to,
                message.received_at_ms,
                message
                    .origin
                    .map(|origin| serde_json::to_string(&origin).unwrap_or_default()),
                serde_json::to_string(&message.context).ok(),
            ],
        )?;
        if let Some(identity_id) = redelivery_identity {
            tx.db_execute(
                "INSERT INTO message_identities (agent_id, target_id, identity_id) VALUES (?1, ?2, ?3)",
                &db_params![agent_id.as_str(), message.id.as_str(), identity_id],
            )?;
        }
        let inserted_identity =
            retain_message_aliases(tx, &message, ingress_origin, message.source_key.as_deref())?;
        super::dedup::retain_dedup_alias(
            tx,
            agent_id,
            &inserted_identity,
            group_dedup_key.as_deref(),
        )?;
        let is_passive_reaction = message.context.event.as_deref() == Some("reaction_passive");
        let woke_agent = if wake_created {
            tx.db_execute(
                "UPDATE agents SET state = 'running'
                 WHERE id = ?1 AND state IN ('idle', 'created')",
                &db_params![agent_id.as_str()],
            )? > 0
        } else if is_passive_reaction {
            false
        } else {
            tx.db_execute(
                "UPDATE agents SET state = 'running' WHERE id = ?1 AND state = 'idle'",
                &db_params![agent_id.as_str()],
            )? > 0
        };
        let agent = agent_by_id(tx, agent_id)?;
        Ok(ReceiveTx::Inserted {
            message,
            woke_agent,
            is_passive_reaction,
            recipient: Box::new(agent),
        })
    }

    pub(super) fn finish_receive(
        &self,
        tx_outcome: ReceiveTx,
        merged: &[String],
    ) -> Option<ReceiveOutcome> {
        self.reproject_merged(merged);
        match tx_outcome {
            ReceiveTx::Early(outcome) => outcome,
            ReceiveTx::Inserted {
                message,
                woke_agent,
                is_passive_reaction,
                recipient,
            } => {
                self.project_mailbox();
                if woke_agent {
                    self.notify(Wake::Agent {
                        agent_id: message.agent_id.clone(),
                    });
                }
                if !is_passive_reaction {
                    self.notify(Wake::Mail {
                        agent_id: message.agent_id.clone(),
                    });
                    self.announce_held_mail(&message, &recipient);
                }
                Some(ReceiveOutcome::New(message))
            }
        }
    }
}
