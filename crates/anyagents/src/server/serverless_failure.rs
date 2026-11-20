//! Serverless turn outcome delivery: outbound reply fallback, terminal bootstrap
//! failure classification, and fail-closed parent notification on child errors.

use anyagents_core::stateless::PendingRemoteTurn;
use anyagents_core::{Agent, AgentId, Incoming, ReceiveOutcome, Store};

use super::serverless::ReplyTarget;

pub(super) fn is_suppressed_fallback_reply(text: &str) -> bool {
    let trimmed = text.trim();
    trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("completed")
        || trimmed.eq_ignore_ascii_case("NO_REPLY")
}

/// Deterministic bootstrap failures (e.g. failed private repo clone) should
/// advance the failed message cursor once surfaced to the parent so `/tick`
/// does not loop retrying an impossible checkout forever.
pub(super) fn is_terminal_bootstrap_failure(error_text: &str) -> bool {
    let lower = error_text.trim().to_ascii_lowercase();
    lower.starts_with("repo clone failed") || lower.contains("repo clone failed for ")
}

/// Deliver a completed turn's fallback reply (to Telegram or parent/peer mailbox)
/// OR immediately notify the parent agent when a child turn fails.
///
/// Returns `(delivered_reply, notified_agent_id)` where `notified_agent_id`
/// names an agent mailbox that received a new message and should be woken in
/// serverless mode.
pub(super) fn deliver_settled_turn_outcome(
    store: &Store,
    turn: &PendingRemoteTurn,
    agent: Option<&Agent>,
) -> (bool, Option<AgentId>) {
    if turn.status == "failed" {
        if let Some(parent_id) = notify_parent_on_failed_turn(store, turn, agent) {
            return (true, Some(parent_id));
        }
        return (false, None);
    }

    let already_replied_via_mcp = store
        .turn_ownership_snapshot(&turn.agent_id)
        .ok()
        .and_then(|snap| snap.last_outbound_at_ms)
        .is_some_and(|ts| ts >= turn.created_at_ms);

    if already_replied_via_mcp
        || turn.status != "completed"
        || turn
            .result_text
            .as_deref()
            .is_none_or(is_suppressed_fallback_reply)
    {
        return (false, None);
    }

    let Some(ref reply_text) = turn.result_text else {
        return (false, None);
    };
    let Some(rt_val) = turn
        .metadata_json
        .as_ref()
        .and_then(|m| m.get("reply_target"))
    else {
        return (false, None);
    };
    let Ok(reply_target) = serde_json::from_value::<ReplyTarget>(rt_val.clone()) else {
        return (false, None);
    };

    match reply_target {
        ReplyTarget::Agent { agent_id: target } => {
            if let Ok(target_aid) = AgentId::parse(&target) {
                let mut incoming = Incoming::new(reply_text.clone());
                incoming.sender = Some(format!("agent:{}", turn.agent_id));
                incoming.source_key = Some(format!("serverless-reply:{}", turn.turn_id));
                if let Ok(outcome) = store.receive(&target_aid, incoming) {
                    let wake = matches!(outcome, ReceiveOutcome::New(_)).then_some(target_aid);
                    return (true, wake);
                }
            }
            (false, None)
        }
        ReplyTarget::Telegram {
            chat_id,
            thread_id,
            reply_to_message_id,
            bot,
        } => {
            let from_name = agent
                .map(|a| a.name.as_str())
                .unwrap_or(turn.agent_id.as_str());
            match crate::telegram::post_text(
                store,
                store.data_dir(),
                from_name,
                bot.as_deref(),
                chat_id,
                reply_text,
                reply_to_message_id,
                thread_id,
            ) {
                Ok(_) => (true, None),
                Err(err) => {
                    let err_str = err.to_string();
                    if reply_to_message_id.is_some()
                        && (err_str.contains("invalid_reply")
                            || err_str.contains("message to be replied not found")
                            || err_str.contains("replied message not found"))
                    {
                        match crate::telegram::post_text(
                            store,
                            store.data_dir(),
                            from_name,
                            bot.as_deref(),
                            chat_id,
                            reply_text,
                            None,
                            thread_id,
                        ) {
                            Ok(_) => (true, None),
                            Err(retry_err) => {
                                eprintln!(
                                    "anyagents-serverless: telegram reply fallback delivery failed for {} (chat {}): {retry_err}",
                                    turn.agent_id, chat_id
                                );
                                (false, None)
                            }
                        }
                    } else {
                        eprintln!(
                            "anyagents-serverless: telegram reply delivery failed for {} (chat {}): {err}",
                            turn.agent_id, chat_id
                        );
                        (false, None)
                    }
                }
            }
        }
    }
}

fn notify_parent_on_failed_turn(
    store: &Store,
    turn: &PendingRemoteTurn,
    agent: Option<&Agent>,
) -> Option<AgentId> {
    let err_text = turn
        .error_text
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())?;
    let target_parent = turn
        .metadata_json
        .as_ref()
        .and_then(|m| m.get("reply_target"))
        .and_then(|v| serde_json::from_value::<ReplyTarget>(v.clone()).ok())
        .and_then(|rt| match rt {
            ReplyTarget::Agent { agent_id } => AgentId::parse(&agent_id).ok(),
            ReplyTarget::Telegram { .. } => None,
        })
        .or_else(|| agent.and_then(|a| a.parent_id.clone()))?;
    if target_parent == turn.agent_id {
        return None;
    }
    let child_label = agent
        .map(|a| a.name.as_str())
        .unwrap_or(turn.agent_id.as_str());
    let mut incoming = Incoming::new(format!(
        "Sub-agent `{child_label}` failed turn `{}`: {err_text}",
        turn.turn_id
    ));
    incoming.sender = Some(format!("agent:{}", turn.agent_id));
    incoming.source_key = Some(format!("serverless-turn-failed:{}", turn.turn_id));
    match store.receive(&target_parent, incoming) {
        Ok(ReceiveOutcome::New(_)) => Some(target_parent),
        Ok(ReceiveOutcome::Duplicate(_)) => None,
        Err(err) => {
            eprintln!(
                "anyagents-serverless: parent failure notification failed for {} -> {}: {err}",
                turn.agent_id, target_parent
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use anyagents_core::stateless::{NewRemoteTurn, SettleRemoteTurn};
    use anyagents_core::{NewAgent, now_ms};
    use serde_json::json;

    use super::*;
    use crate::server::serverless::settle_remote_turn_and_deliver;

    #[test]
    fn repo_clone_failure_notifies_parent_immediately_and_stops_retry_loop() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let parent = store
            .create_agent(NewAgent::new("parent-agent", "codex"))
            .unwrap();
        let mut child_new = NewAgent::new("child-repo-agent", "codex");
        child_new.parent_id = Some(parent.id.clone());
        let child = store.create_agent(child_new).unwrap();

        let mut spawn_msg = Incoming::new("Inspect the private repo.");
        spawn_msg.sender = Some(format!("agent:{}", parent.id));
        let ReceiveOutcome::New(m1) = store.receive(&child.id, spawn_msg).unwrap() else {
            panic!("expected new spawn message");
        };

        store
            .remote_turn_store()
            .create_turn(&NewRemoteTurn {
                turn_id: "turn_repo_fail_1".to_string(),
                agent_id: child.id.clone(),
                harness_kind: "sandbox".to_string(),
                provider_kind: Some("sylphx".to_string()),
                remote_run_id: None,
                callback_token_hash: "hash_repo_fail".to_string(),
                cursor: None,
                status: "dispatched".to_string(),
                metadata_json: Some(json!({
                    "message_seq": m1.seq,
                    "opener_id": m1.id.to_string(),
                    "reply_target": { "kind": "agent", "agent_id": parent.id.as_str() },
                })),
                next_poll_at_ms: None,
            })
            .unwrap();

        let settled = settle_remote_turn_and_deliver(
            &store,
            "turn_repo_fail_1",
            SettleRemoteTurn {
                status: "failed".to_string(),
                result_text: None,
                error_text: Some(
                    "repo clone failed for https://github.com/Org/PrivateRepo.git: authentication failed"
                        .to_string(),
                ),
                usage_json: None,
                cursor: None,
                settled_at_ms: now_ms(),
            },
            None,
            0,
        )
        .unwrap()
        .unwrap();

        assert!(settled.settled_now);
        assert!(settled.delivered_reply);
        assert_eq!(settled.notified_agent_id.as_ref(), Some(&parent.id));
        // Deterministic repo clone bootstrap failure advances the cursor so it does not loop.
        assert_eq!(store.delivered_cursor(&child.id).unwrap(), m1.seq);

        let parent_inbox = store.list_undelivered(&parent.id, 10).unwrap();
        assert_eq!(parent_inbox.len(), 1);
        assert!(
            parent_inbox[0]
                .text
                .contains("repo clone failed for https://github.com/Org/PrivateRepo.git"),
            "{}",
            parent_inbox[0].text
        );
        assert_eq!(
            parent_inbox[0].sender.as_deref(),
            Some(format!("agent:{}", child.id).as_str())
        );

        // Idempotent replay must not duplicate the parent notification.
        let replay = settle_remote_turn_and_deliver(
            &store,
            "turn_repo_fail_1",
            SettleRemoteTurn {
                status: "failed".to_string(),
                result_text: None,
                error_text: Some("repo clone failed".to_string()),
                usage_json: None,
                cursor: None,
                settled_at_ms: now_ms(),
            },
            None,
            0,
        )
        .unwrap()
        .unwrap();
        assert!(!replay.settled_now);
        assert_eq!(store.list_undelivered(&parent.id, 10).unwrap().len(), 1);
    }
}
