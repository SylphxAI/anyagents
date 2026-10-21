//! Resolve replies from an agent's own stored mailbox, not model-provided
//! provider coordinates. Explicit addresses can start a new conversation.
use anyagents_core::{Address, Message};
use crate::auth::Principal;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Destination {
    Telegram { chat: String, thread: Option<String>, reply: Option<String> },
    Agent(String),
    Group(String),
}
impl Destination {
    pub fn address(&self) -> String {
        match self {
            Self::Telegram { chat, thread, .. } => match thread {
                Some(thread) => format!("telegram:chat:{chat}:{thread}"),
                None => format!("telegram:chat:{chat}"),
            },
            Self::Agent(name) => format!("agent:{name}"),
            Self::Group(name) => format!("group:{name}"),
        }
    }
    pub fn reply(&self) -> Option<&str> {
        match self { Self::Telegram { reply, .. } => reply.as_deref(), _ => None }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Origin {
    pub chat_id: Option<i64>,
    pub thread_id: Option<i64>,
    pub message_id: Option<i64>,
}
pub fn telegram_message_id(sender: Option<&str>) -> Option<i64> {
    let sender = sender?;
    if let Some(rest) = sender.strip_prefix("telegram:chat:") {
        let (_, after) = rest.split_once(':')?;
        return after.parse().ok();
    }
    if let Some(rest) = sender.strip_prefix("telegram:user:") {
        let (_, message) = rest.split_once(':')?;
        return message.parse().ok();
    }
    None
}
pub fn origin(message: &Message) -> Origin {
    let sender = message.sender.as_deref();
    let chat = sender
        .and_then(|value| value.strip_prefix("telegram:chat:"))
        .and_then(|rest| rest.split(':').next())
        .and_then(|chat| chat.parse().ok())
        .or_else(|| message.reply_to.as_deref()
            .and_then(|value| value.strip_prefix("telegram:chat:"))
            .and_then(|rest| rest.split(':').next())
            .and_then(|chat| chat.parse().ok()));
    let thread = sender
        .and_then(|value| value.strip_prefix("telegram:chat:"))
        .and_then(|rest| rest.split_once(':'))
        .and_then(|(_, thread)| thread.parse().ok())
        .or_else(|| message.reply_to.as_deref()
            .and_then(|value| value.strip_prefix("telegram:chat:"))
            .and_then(|rest| rest.split_once(':'))
            .and_then(|(_, thread)| thread.parse().ok()));
    Origin { chat_id: chat, thread_id: thread, message_id: telegram_message_id(sender) }
}
fn answered_message(message: &Message) -> Option<&str> {
    message.source_key.as_deref()?.strip_prefix("answer:")
}
pub fn resolve(
    principal: &Principal,
    own: impl Fn(&str) -> Result<Option<Message>, String>,
    reply_to: Option<&str>,
) -> Result<Option<Destination>, String> {
    let Some(reply_to) = reply_to.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if let Some(name) = anyagents_harness::ref_message(reply_to) {
        let message = own(name)?
            .ok_or_else(|| "that reply names no message in this mailbox".to_string())?;
        return destination_through(principal, &message, &own).map(Some);
    }
    // A bare mailbox/provider id is reply metadata, not a destination.
    if !reply_to.contains(':') { return Ok(None); }
    let address = Address::parse(reply_to).map_err(|err| err.to_string())?;
    let destination = match address {
        Address::TelegramChat { chat, thread } => Destination::Telegram { chat, thread, reply: None },
        Address::TelegramUser { user, message } => Destination::Telegram { chat: user, thread: None, reply: message },
        Address::Agent(name) => Destination::Agent(name),
        Address::Group(name) => Destination::Group(name),
    };
    Ok(Some(destination))
}
#[cfg(test)]
pub fn destination_of(principal: &Principal, message: &Message) -> Result<Destination, String> {
    destination_through(principal, message, &|_| Ok(None))
}
fn destination_through(
    principal: &Principal,
    message: &Message,
    own: &dyn Fn(&str) -> Result<Option<Message>, String>,
) -> Result<Destination, String> {
    let origin = origin(message);
    if let Some(chat) = origin.chat_id {
        return Ok(Destination::Telegram {
            chat: chat.to_string(), thread: origin.thread_id.map(|thread| thread.to_string()),
            reply: origin.message_id.map(|id| id.to_string()),
        });
    }
    if let Some(answered) = answered_message(message)
        && let Ok(Some(asked)) = own(answered) {
        return destination_through(principal, &asked, own);
    }
    let sender = message.sender.as_deref()
        .ok_or_else(|| "that message names no one to answer".to_string())?;
    if let Some(name) = sender.strip_prefix("agent:") {
        return Ok(Destination::Agent(name.to_string()));
    }
    if let Some(rest) = sender.strip_prefix("telegram:user:") {
        return Ok(Destination::Telegram {
            chat: rest.split(':').next().unwrap_or(rest).to_string(), thread: None,
            reply: rest.split_once(':').map(|(_, id)| id.to_string()),
        });
    }
    if let Some(room) = message.reply_to.as_deref().and_then(|value| value.strip_prefix("group:")) {
        return Ok(Destination::Group(room.to_string()));
    }
    let _ = principal;
    Err("that message names no destination this agent may answer".to_string())
}
#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_core::{AgentId, MessageId};
    fn message(sender: Option<&str>, reply_to: Option<&str>) -> Message {
        Message {
            id: MessageId::parse("msg_01h455vb4pex5vsknk084sn02q").unwrap(),
            agent_id: AgentId::parse("agt_01h455vb4pex5vsknk084sn02q").unwrap(),
            seq: 1, text: "hi".into(), sender: sender.map(str::to_string),
            source_time_ms: None, source_key: None, reply_to: reply_to.map(str::to_string),
            received_at_ms: 0, ack: anyagents_core::Ack::Received, delivered_at_ms: None,
        }
    }
    fn agent() -> Principal {
        Principal::Agent { agent_id: AgentId::parse("agt_01h455vb4pex5vsknk084sn02q").unwrap(), name: "sage".into() }
    }
    #[test]
    fn a_group_quote_is_answered_in_its_group_and_topic() {
        let incoming = message(Some("telegram:user:42:61"), Some("telegram:chat:-100:3"));
        let found = destination_of(&agent(), &incoming).unwrap();
        assert_eq!(found, Destination::Telegram { chat: "-100".into(), thread: Some("3".into()), reply: Some("61".into()) });
        assert_eq!(found.address(), "telegram:chat:-100:3");
    }
    #[test]
    fn a_direct_message_is_answered_in_its_own_chat() {
        let incoming = message(Some("telegram:user:42:9"), Some("telegram:chat:42"));
        let found = destination_of(&agent(), &incoming).unwrap();
        assert_eq!(found.address(), "telegram:chat:42"); assert_eq!(found.reply(), Some("9"));
    }
    #[test]
    fn an_agent_message_is_answered_to_that_agent() {
        assert_eq!(destination_of(&agent(), &message(Some("agent:argon"), None)).unwrap().address(), "agent:argon");
    }
    #[test]
    fn a_reference_is_resolved_from_the_stored_record_and_unknown_is_refused() {
        let incoming = message(Some("telegram:user:42:61"), Some("telegram:chat:-100:3"));
        let id = incoming.id.to_string(); let reference = anyagents_harness::reply_ref(&id);
        let resolved = resolve(&agent(), |name| Ok((name == id).then(|| incoming.clone())), Some(&reference)).unwrap();
        assert_eq!(resolved.unwrap().address(), "telegram:chat:-100:3");
        assert!(resolve(&agent(), |_| Ok(None), Some("anyagents:reply:msg_01h455vb4pex5vsknk084sn02r")).is_err());
    }
    #[test]
    fn a_plain_destination_is_used_as_written() {
        assert_eq!(resolve(&agent(), |_| Ok(None), Some("telegram:chat:-5:9")).unwrap().unwrap().address(), "telegram:chat:-5:9");
    }
    #[test]
    fn the_origin_reads_both_sender_conventions() {
        let group = origin(&message(Some("telegram:chat:-100:3"), Some("telegram:chat:-100:3")));
        assert_eq!((group.chat_id, group.thread_id, group.message_id), (Some(-100), Some(3), Some(3)));
        let user = origin(&message(Some("telegram:user:42:61"), Some("telegram:chat:42")));
        assert_eq!((user.chat_id, user.thread_id, user.message_id), (Some(42), None, Some(61)));
    }
    #[test]
    fn bare_message_ids_are_metadata_not_destinations() {
        for id in ["msg_01h455vb4pex5vsknk084sn02q", "55"] {
            assert_eq!(resolve(&agent(), |_| Ok(None), Some(id)).unwrap(), None);
        }
    }
}
