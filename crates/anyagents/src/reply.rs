//! The reply binding: an answer names the mailbox message it answers, and the
//! server turns that name into the destination it authenticated for that
//! message.
//!
//! A group message is answered in its group and its forum topic, at the message
//! that arrived; a direct message in its own chat. A model never has to carry a
//! chat id, a topic or a message id to keep a conversation in place, and an
//! answer to a group message can never become a private one.
//!
//! `reply_to` is a mailbox reference (`anyagents:reply:<message id>`) or a
//! destination written plainly (`telegram:chat:<id>[:<topic>]`,
//! `telegram:user:<id>[:<message>]`, `agent:<name>`, `group:<name>`). A
//! reference is resolved from the stored record; a destination is used as
//! written, which is how an agent starts a new conversation on purpose. A bare
//! number or a bare message id is neither, and is treated as metadata rather
//! than as a place to post.

use anyagents_core::{Address, Message, Origin};

use crate::auth::Principal;

/// The prefix of a mailbox reference: `anyagents:reply:<message id>`.
pub const REPLY_PREFIX: &str = "anyagents:reply:";

/// The reply reference a message names. A whole agent's answer resolves to a
/// mailbox reference, not to a chat id, so an answer can never land in another
/// conversation.
#[allow(dead_code)]
pub fn reply_ref(message_id: &str) -> String {
    anyagents_harness::reply_ref(message_id)
}

/// The message id a reply reference names, when it is one.
pub fn ref_message(reference: &str) -> Option<&str> {
    let id = reference.strip_prefix(REPLY_PREFIX)?;
    if id.is_empty() || id.contains(':') {
        None
    } else {
        Some(id)
    }
}

/// Where one answer goes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Destination {
    /// The provider chat and forum topic to post in, and the message it
    /// answers.
    Telegram {
        chat: String,
        thread: Option<String>,
        reply: Option<String>,
    },
    Agent(String),
    Group(String),
}

impl Destination {
    /// The address the send tool takes.
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

    /// The provider message id this answer replies to, when there is one.
    pub fn reply(&self) -> Option<&str> {
        match self {
            Self::Telegram { reply, .. } => reply.as_deref(),
            _ => None,
        }
    }
}

/// The origin of a stored message: the provider coordinates the channel wrote
/// with it, or, for a message stored before those were kept, the ones its
/// sender convention implies.
pub fn origin(message: &Message) -> Origin {
    if let Some(origin) = message.origin {
        return origin;
    }
    let sender = message.sender.as_deref();
    let reply_target = message.reply_to.as_deref();
    let chat = chat_of(sender).or_else(|| chat_of(reply_target));
    let thread = thread_of(sender).or_else(|| thread_of(reply_target));
    Origin {
        chat_id: chat,
        thread_id: thread,
        message_id: telegram_message_id(sender),
    }
}

fn chat_of(value: Option<&str>) -> Option<i64> {
    value?
        .strip_prefix("telegram:chat:")?
        .split(':')
        .next()?
        .parse()
        .ok()
}

fn thread_of(value: Option<&str>) -> Option<i64> {
    value?
        .strip_prefix("telegram:chat:")?
        .split_once(':')?
        .1
        .parse()
        .ok()
}

/// The telegram message id in `telegram:chat:<chat>[:<thread>]`, or in the
/// sender convention `telegram:user:<id>:<message>`.
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

/// The message an agent's own recorded answer refers to, by convention
/// `answer:<message id>` in its source key.
fn answered_message(message: &Message) -> Option<&str> {
    message.source_key.as_deref()?.strip_prefix("answer:")
}

/// Resolve `reply_to` for one agent's answer, against the agent's own mailbox.
pub fn resolve(
    principal: &Principal,
    own: impl Fn(&str) -> Result<Option<Message>, String>,
    reply_to: Option<&str>,
) -> Result<Option<Destination>, String> {
    let Some(reply_to) = reply_to.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if let Some(name) = ref_message(reply_to) {
        let message =
            own(name)?.ok_or_else(|| "that reply names no message in this mailbox".to_string())?;
        return destination_through(principal, &message, &own).map(Some);
    }
    // A bare message id names a message this agent holds. It is answered where
    // that message came from, exactly like a reference.
    if !reply_to.contains(':')
        && let Some(message) = own(reply_to)?
    {
        return destination_through(principal, &message, &own).map(Some);
    }
    // A bare message id names a message this agent holds: it is resolved like a
    // reference, so an answer goes where that message came from.
    if !reply_to.contains(':') {
        if let Some(message) = own(reply_to)? {
            return destination_through(principal, &message, &own).map(Some);
        }
        return Ok(None);
    }
    // A destination written in full.
    let Ok(Address::Agent(name)) = Address::parse(reply_to) else {
        return plain_destination(reply_to);
    };
    Ok(Some(Destination::Agent(name)))
}

/// A destination written in full.
fn plain_destination(reply_to: &str) -> Result<Option<Destination>, String> {
    if !reply_to.contains(':') {
        return Ok(None);
    }
    let Ok(address) = Address::parse(reply_to) else {
        return Ok(None);
    };
    let destination = match address {
        Address::TelegramChat { chat, thread } => Destination::Telegram {
            chat,
            thread,
            reply: None,
        },
        Address::TelegramUser { user, message } => Destination::Telegram {
            chat: user,
            thread: None,
            reply: message,
        },
        Address::Agent(name) => Destination::Agent(name),
        Address::Group(name) => Destination::Group(name),
    };
    Ok(Some(destination))
}

/// Where an answer to `message` goes, from the record that stored it.
#[cfg(test)]
pub fn destination_of(principal: &Principal, message: &Message) -> Result<Destination, String> {
    destination_through(principal, message, &|_| Ok(None))
}

/// [`destination_of`], able to follow one step of an agent's own reply back to
/// the message that asked it.
fn destination_through(
    principal: &Principal,
    message: &Message,
    own: &dyn Fn(&str) -> Result<Option<Message>, String>,
) -> Result<Destination, String> {
    let origin = origin(message);
    if let Some(chat) = origin.chat_id {
        return Ok(Destination::Telegram {
            chat: chat.to_string(),
            thread: origin.thread_id.map(|thread| thread.to_string()),
            reply: origin.message_id.map(|id| id.to_string()),
        });
    }
    // A recorded answer is answered to whoever asked it.
    if let Some(answered) = answered_message(message)
        && let Ok(Some(asked)) = own(answered)
    {
        return destination_through(principal, &asked, own);
    }
    let sender = message
        .sender
        .as_deref()
        .ok_or_else(|| "that message names no one to answer".to_string())?;
    if let Some(name) = sender.strip_prefix("agent:") {
        return Ok(Destination::Agent(name.to_string()));
    }
    // A person on a channel writes as `telegram:user:<id>[:<message>]`; an
    // answer to them goes to their own chat.
    if let Some(rest) = sender.strip_prefix("telegram:user:") {
        return Ok(Destination::Telegram {
            chat: rest.split(':').next().unwrap_or(rest).to_string(),
            thread: None,
            reply: rest.split_once(':').map(|(_, id)| id.to_string()),
        });
    }
    if let Some(room) = message
        .reply_to
        .as_deref()
        .and_then(|value| value.strip_prefix("group:"))
    {
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
            seq: 1,
            text: "hi".into(),
            sender: sender.map(str::to_string),
            source_time_ms: None,
            source_key: None,
            reply_to: reply_to.map(str::to_string),
            received_at_ms: 0,
            ack: anyagents_core::Ack::Received,
            delivered_at_ms: None,
            origin: None,
            context: anyagents_harness::Context::default(),
        }
    }

    fn agent() -> Principal {
        Principal::Agent {
            agent_id: AgentId::parse("agt_01h455vb4pex5vsknk084sn02q").unwrap(),
            name: "sage".into(),
        }
    }

    #[test]
    fn a_group_quote_is_answered_in_its_group_and_topic_at_the_message() {
        let incoming = message(Some("telegram:user:42:61"), Some("telegram:chat:-100:3"));
        let found = destination_of(&agent(), &incoming).unwrap();
        assert_eq!(
            found,
            Destination::Telegram {
                chat: "-100".into(),
                thread: Some("3".into()),
                reply: Some("61".into()),
            }
        );
        assert_eq!(found.address(), "telegram:chat:-100:3");
        // Never the sender's own chat: an answer to a group stays in it.
        assert!(!found.address().contains("42"));
    }

    #[test]
    fn a_direct_message_is_answered_in_its_own_chat() {
        let incoming = message(Some("telegram:user:42:9"), Some("telegram:chat:42"));
        let found = destination_of(&agent(), &incoming).unwrap();
        assert_eq!(found.address(), "telegram:chat:42");
        assert_eq!(found.reply(), Some("9"));
    }

    #[test]
    fn the_recorded_origin_wins_over_a_guessed_one() {
        let mut incoming = message(Some("telegram:user:42:61"), Some("telegram:chat:42"));
        incoming.origin = Some(Origin {
            chat_id: Some(-100),
            thread_id: Some(3),
            message_id: Some(61),
        });
        assert_eq!(
            destination_of(&agent(), &incoming).unwrap().address(),
            "telegram:chat:-100:3"
        );
    }

    #[test]
    fn an_agent_message_is_answered_to_that_agent() {
        let incoming = message(Some("agent:gamma"), None);
        assert_eq!(
            destination_of(&agent(), &incoming).unwrap().address(),
            "agent:gamma"
        );
    }

    #[test]
    fn a_reference_is_resolved_from_the_stored_record_and_unknown_is_refused() {
        let incoming = message(Some("telegram:user:42:61"), Some("telegram:chat:-100:3"));
        let id = incoming.id.to_string();
        let reference = reply_ref(&id);
        let resolved = resolve(
            &agent(),
            |name| Ok((name == id).then(|| incoming.clone())),
            Some(&reference),
        )
        .unwrap();
        assert_eq!(resolved.unwrap().address(), "telegram:chat:-100:3");
        let missing = resolve(
            &agent(),
            |_| Ok(None),
            Some("anyagents:reply:msg_01h455vb4pex5vsknk084sn02r"),
        );
        assert!(missing.is_err());
    }

    #[test]
    fn a_plain_destination_is_used_as_written() {
        let found = resolve(&agent(), |_| Ok(None), Some("telegram:chat:-5:9")).unwrap();
        assert_eq!(found.unwrap().address(), "telegram:chat:-5:9");
    }

    #[test]
    fn the_origin_reads_both_sender_conventions() {
        let group = origin(&message(
            Some("telegram:chat:-100:3"),
            Some("telegram:chat:-100:3"),
        ));
        assert_eq!(
            (group.chat_id, group.thread_id, group.message_id),
            (Some(-100), Some(3), Some(3))
        );
        let user = origin(&message(
            Some("telegram:user:42:61"),
            Some("telegram:chat:42"),
        ));
        assert_eq!(
            (user.chat_id, user.thread_id, user.message_id),
            (Some(42), None, Some(61))
        );
    }

    #[test]
    fn bare_message_ids_are_metadata_not_destinations() {
        for id in ["msg_01h455vb4pex5vsknk084sn02q", "55"] {
            assert_eq!(resolve(&agent(), |_| Ok(None), Some(id)).unwrap(), None);
        }
    }
}
