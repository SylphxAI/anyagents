//! Telegram authority and optional provider effect for the canonical ingress port.

use agent_ingress::{
    BlockMatch, BlockScope, ChatType, IngressBlockStore as _, IngressDecision, SenderIdentity,
    SenderRelationship,
};
use anyagents_channel_telegram::{BotCall, Memory};
use anyagents_core::{AgentId, Channel, Error, IngressBlocks, Store, now_ms};
use serde::Serialize;

use super::{SHORT_TIMEOUT_S, call_api, channel_token};

#[derive(Default)]
pub(crate) struct Authority {
    owner: Vec<i64>,
    admin: Vec<i64>,
    trusted: Vec<i64>,
}

impl Authority {
    pub(crate) fn load() -> Result<Self, Error> {
        fn ids(key: &str) -> Result<Vec<i64>, Error> {
            let Some(raw) = std::env::var_os(key) else {
                return Ok(Vec::new());
            };
            raw.to_str()
                .and_then(|s| serde_json::from_str::<Vec<i64>>(s).ok())
                .filter(|ids| ids.iter().all(|id| *id > 0))
                .ok_or_else(|| {
                    Error::Message(format!(
                        "invalid {key}: expected a JSON array of positive user IDs"
                    ))
                })
        }
        Ok(Self {
            owner: ids("ANYAGENTS_TELEGRAM_OWNER_IDS")?,
            admin: ids("ANYAGENTS_TELEGRAM_ADMIN_IDS")?,
            trusted: ids("ANYAGENTS_TELEGRAM_TRUSTED_IDS")?,
        })
    }

    pub(crate) fn relationship(&self, sender_id: Option<i64>) -> SenderRelationship {
        match sender_id {
            Some(id) if self.owner.contains(&id) => SenderRelationship::Owner,
            Some(id) if self.admin.contains(&id) => SenderRelationship::Admin,
            Some(id) if self.trusted.contains(&id) => SenderRelationship::Trusted,
            _ => SenderRelationship::Stranger,
        }
    }
}

pub(crate) fn chat_type(kind: &str) -> ChatType {
    match kind {
        "private" => ChatType::Direct,
        "group" | "supergroup" => ChatType::Group,
        "channel" => ChatType::Channel,
        _ => ChatType::Unknown,
    }
}

pub(super) fn admit(
    store: &Store,
    agent: &AgentId,
    authority: &Authority,
    sender_id: Option<i64>,
    chat_id: i64,
    kind: ChatType,
) -> Result<Option<SenderIdentity>, Error> {
    let identity = SenderIdentity {
        channel: "telegram".into(),
        sender_id: sender_id.map_or_else(|| "unknown".into(), |id| id.to_string()),
        chat_id: Some(chat_id.to_string()),
        chat_type: kind,
        username: None,
        display_name: None,
        relationship: authority.relationship(sender_id),
    };
    // Both ingress entry points run on blocking workers. The async port
    // delegates to the synchronous Store (including its PostgreSQL bridge).
    match futures::executor::block_on(
        IngressBlocks::new(store, agent).is_blocked(&identity, now_ms()),
    ) {
        Ok(IngressDecision::Allow) => Ok(Some(identity)),
        Ok(IngressDecision::Drop { rule_id, .. }) => {
            eprintln!("telegram ingress dropped: agent={agent} rule={rule_id}");
            Ok(None)
        }
        Err(err) => Err(Error::Message(err.to_string())),
    }
}

/// Only canonical numeric provider IDs are accepted. A room is resolved from
/// this agent's persisted bindings; a private chat ID is its user's ID.
pub(crate) fn resolve_target(
    target: &BlockMatch,
    authority: &Authority,
    bindings: &[(Channel, Memory)],
) -> Option<(SenderRelationship, ChatType)> {
    if target.channel != "telegram" {
        return None;
    }
    match &target.scope {
        BlockScope::Sender { sender_id } => {
            let id = numeric_id(sender_id)?;
            (id > 0).then(|| (authority.relationship(Some(id)), ChatType::Unknown))
        }
        BlockScope::Chat { chat_id } => {
            let id = numeric_id(chat_id)?;
            if id > 0 {
                return Some((authority.relationship(Some(id)), ChatType::Direct));
            }
            bindings
                .iter()
                .flat_map(|(_, memory)| &memory.chats)
                .find_map(|link| {
                    (link.chat_id == id)
                        .then_some(link.chat_type.as_deref())
                        .flatten()
                        .map(chat_type)
                        .filter(|kind| *kind == ChatType::Group)
                        .map(|kind| (SenderRelationship::Stranger, kind))
                })
        }
    }
}

pub(crate) fn numeric_id(raw: &str) -> Option<i64> {
    let id = raw.parse::<i64>().ok()?;
    (id != 0 && raw == id.to_string()).then_some(id)
}

pub(crate) fn bindings(store: &Store, agent: &AgentId) -> Result<Vec<(Channel, Memory)>, Error> {
    store
        .list_channels()?
        .into_iter()
        .filter(|channel| channel.kind == "telegram" && channel.agents.contains(agent))
        .map(|channel| Memory::try_from_state(&channel.state).map(|mem| (channel, mem)))
        .collect()
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum LeaveResult {
    Skipped {
        reason: &'static str,
    },
    Attempted {
        left: usize,
        already_absent: usize,
        failed: usize,
    },
}

pub(crate) fn leave_group(
    store: &Store,
    agent: &AgentId,
    authority: &Authority,
    chat_id: i64,
    expires: bool,
) -> LeaveResult {
    let skip = |reason| LeaveResult::Skipped { reason };
    if expires {
        return skip("temporary block; leaving cannot be reversed on expiry");
    }
    // Telegram cannot reliably establish non-membership of other users for a
    // non-admin bot or guarantee a protected user will not join later. Honor
    // every configured protected user by leaving their ingress route intact.
    if !authority.owner.is_empty() || !authority.admin.is_empty() {
        return skip("leaving could cut off configured protected senders");
    }
    let (Ok(bindings), Ok(recipient)) = (bindings(store, agent), store.agent(agent)) else {
        return skip("bot bindings unavailable");
    };
    let (mut left, mut already_absent, mut failed) = (0, 0, 0);
    for (channel, memory) in bindings {
        if channel.agents.as_slice() != [agent.clone()]
            || channel.credential_ref.is_none()
            || !memory.chats.iter().any(|link| {
                (link.agent == agent.as_str() || link.agent == recipient.name)
                    && link.chat_id == chat_id
                    && link
                        .chat_type
                        .as_deref()
                        .is_some_and(|kind| chat_type(kind) == ChatType::Group)
            })
        {
            continue;
        }
        let call = BotCall {
            method: "leaveChat".into(),
            body: serde_json::json!({ "chat_id": chat_id }),
        };
        let result = channel_token(store, store.data_dir(), &channel)
            .and_then(|token| call_api(&token, &call, SHORT_TIMEOUT_S, Some(&channel.id)));
        match result {
            Ok(body)
                if serde_json::from_str::<serde_json::Value>(&body)
                    .ok()
                    .and_then(|v| v.get("result").and_then(serde_json::Value::as_bool))
                    == Some(true) =>
            {
                left += 1
            }
            Err(err) if is_already_absent(&err) => already_absent += 1,
            _ => failed += 1,
        }
    }
    if left + already_absent + failed == 0 {
        return skip("no exclusively assigned Telegram bot in this group");
    }
    LeaveResult::Attempted {
        left,
        already_absent,
        failed,
    }
}

fn is_already_absent(err: &Error) -> bool {
    let msg = err.to_string().to_ascii_lowercase();
    msg.contains("bot is not a member") || msg.contains("bot was kicked")
}
