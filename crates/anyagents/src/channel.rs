//! `send`, `inbox` and `channels`: the operator's messages and channel
//! records.
//!
//! Under the boundary every call goes through the server, which decides who a
//! message is from before it writes anything. On a single-user machine there is
//! no server to ask for a channel secret, and the store is the one place the
//! message can land.

use std::path::Path;

use anyagents_core::{
    Address, CredentialStore, Error, FileCredentialStore, Incoming, NewChannel, Store,
};

use crate::agent::{named, open};
use crate::api::{InboxResponse, SendRequest, SendResponse};
use crate::client;
use crate::server;
use crate::telegram;

/// The operator's authority when it holds one, else none: only a deployment
/// without the boundary answers an unauthenticated call.
fn authority(data: &Path) -> client::Authority {
    match crate::operator::operator_token(data) {
        Some(_) => client::Authority::Operator,
        None => client::Authority::None,
    }
}

#[allow(clippy::too_many_arguments)]
pub fn send(
    data: &Path,
    to: &str,
    text: String,
    source_key: Option<String>,
    source_time_ms: Option<i64>,
    reply_to: Option<String>,
    from: Option<String>,
) -> Result<(), Error> {
    if server::socket_path(data).exists() {
        let body = SendRequest {
            sender: from.clone(),
            // A reply names where it goes; the address is only what was typed.
            to: if reply_to.is_some() {
                Vec::new()
            } else {
                vec![to.to_string()]
            },
            text: text.clone(),
            reply_to: reply_to.clone(),
            quote: None,
            idempotency_key: source_key.clone(),
        };
        let sent = client::call::<SendResponse>(
            data,
            "POST",
            "/v1/send",
            Some(serde_json::to_vec(&body).map_err(|err| Error::Message(err.to_string()))?),
            &authority(data),
        )
        .map_err(Error::Message)?;
        if let Some(sent) = sent {
            for delivery in sent.deliveries {
                if delivery.duplicate {
                    println!("{}\t{}\tduplicate", delivery.id, delivery.seq);
                } else {
                    println!("{}\t{}", delivery.id, delivery.seq);
                }
            }
            return Ok(());
        }
    }
    send_stored(data, to, text, source_key, source_time_ms, reply_to, from)
}

/// The single-user path: the store itself, with the sender as named.
#[allow(clippy::too_many_arguments)]
fn send_stored(
    data: &Path,
    to: &str,
    text: String,
    source_key: Option<String>,
    source_time_ms: Option<i64>,
    reply_to: Option<String>,
    from: Option<String>,
) -> Result<(), Error> {
    let address = Address::parse(to)?;
    let name = match address {
        Address::Agent(name) => name,
        Address::Group(room) => {
            let store = open(data)?;
            let delivered = store.receive_room(
                &room,
                Incoming {
                    text,
                    sender: from,
                    source_time_ms,
                    source_key,
                    reply_to,
                    quote: None,
                    origin: None,
                    received_at_ms: None,
                },
            )?;
            if delivered.is_empty() {
                return Err(Error::Message(format!(
                    "room {room} has no agent to deliver to"
                )));
            }
            for (agent, outcome) in delivered {
                let message = match outcome {
                    anyagents_core::ReceiveOutcome::New(message)
                    | anyagents_core::ReceiveOutcome::Duplicate(message) => message,
                };
                println!("{}\t{}\t{}", agent.name, message.id, message.seq);
            }
            return Ok(());
        }
        Address::TelegramChat { chat, thread } => {
            return send_telegram(
                data,
                &chat,
                thread.as_deref(),
                &text,
                reply_to.as_deref(),
                None,
                from.as_deref(),
            );
        }
        Address::TelegramUser { user, message } => {
            return send_telegram(
                data,
                &user,
                None,
                &text,
                reply_to.as_deref(),
                message.as_deref(),
                from.as_deref(),
            );
        }
    };
    let store = open(data)?;
    let agent = store.agent_by_name(&name)?;
    let outcome = store.receive(
        &agent.id,
        Incoming {
            text,
            sender: from,
            source_time_ms,
            source_key,
            reply_to,
            quote: None,
            origin: None,
            received_at_ms: None,
        },
    )?;
    match outcome {
        anyagents_core::ReceiveOutcome::New(message) => println!("{}\t{}", message.id, message.seq),
        anyagents_core::ReceiveOutcome::Duplicate(message) => {
            println!("{}\t{}\tduplicate", message.id, message.seq);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn send_telegram(
    data: &Path,
    chat: &str,
    thread: Option<&str>,
    text: &str,
    reply_to: Option<&str>,
    address_reply: Option<&str>,
    from: Option<&str>,
) -> Result<(), Error> {
    let chat_id: i64 = chat
        .parse()
        .map_err(|_| Error::Message("telegram chat id is not a number".into()))?;
    let thread_id = match thread {
        None => None,
        Some(raw) => Some(parse_id(raw, "telegram topic id")?),
    };
    let reply_to = match reply_to.or(address_reply) {
        None => None,
        Some(raw) => Some(parse_id(raw, "reply_to is not a telegram message id")?),
    };
    let from = from.ok_or_else(|| {
        Error::Message("a telegram post needs an explicit sender, an agent's own bot".into())
    })?;
    let store = open(data)?;
    let message_id = telegram::post_text(&store, data, from, chat_id, text, reply_to, thread_id)?;
    println!("{message_id}");
    Ok(())
}

fn parse_id(raw: &str, what: &'static str) -> Result<i64, Error> {
    raw.parse().map_err(|_| Error::Message(what.into()))
}

pub fn inbox(data: &Path, agent: &str, since: Option<i64>) -> Result<(), Error> {
    if server::socket_path(data).exists() && crate::operator::operator_token(data).is_some() {
        let who = named(data, agent)?.id;
        let _ = &who;
        let path = match since {
            Some(since) => format!("/v1/agents/{who}/inbox?since={since}"),
            None => format!("/v1/agents/{who}/inbox"),
        };
        let read = client::call::<InboxResponse>(data, "GET", &path, None, &authority(data))
            .map_err(Error::Message)?;
        if let Some(read) = read {
            print_messages(read);
            return Ok(());
        }
    }
    let store = open(data)?;
    let agent = store.agent_by_name(agent)?;
    print_messages(InboxResponse {
        messages: store
            .inbox(&agent.id, since)?
            .into_iter()
            .map(|message| crate::api::MessageView {
                id: message.id.to_string(),
                seq: message.seq,
                text: message.text,
                sender: message.sender,
                reply_to: message.reply_to,
                source_time_ms: message.source_time_ms,
                received_at_ms: message.received_at_ms,
                ack: message.ack.to_string(),
                delivered_at_ms: message.delivered_at_ms,
            })
            .collect(),
    });
    Ok(())
}

fn print_messages(read: InboxResponse) {
    for message in read.messages {
        let ack = message.ack.clone();
        println!(
            "{}\t{}\t{}\t{}",
            message.seq, ack, message.received_at_ms, message.text
        );
    }
}

/// `channels add` and `channels list`.
pub fn channels(data: &Path, action: crate::ChannelsCommand) -> Result<(), Error> {
    match action {
        crate::ChannelsCommand::Add {
            kind,
            agent,
            token_file,
        } => add_channel(data, &kind, &agent, &token_file),
        crate::ChannelsCommand::List => list_channels(data),
    }
}

/// Record a channel and copy its secret into the credential store.
pub fn add_channel(
    data: &Path,
    kind: &str,
    agent_name: &str,
    token_file: &Path,
) -> Result<(), Error> {
    if kind != "telegram" {
        return Err(Error::Message(format!("unsupported channel kind: {kind}")));
    }
    let token = std::fs::read(token_file)?;
    let token =
        String::from_utf8(token).map_err(|_| Error::Message("token file is not text".into()))?;
    let token = token.trim();
    if token.is_empty() || token.chars().any(|ch| ch.is_whitespace() || ch == '/') {
        return Err(Error::Message(
            "token is empty or contains whitespace or a slash".into(),
        ));
    }
    let store = open(data)?;
    let agent = store.agent_by_name(agent_name)?;
    record(&store, data, kind, &agent.id, token.as_bytes())?;
    let recorded = store
        .list_channels()?
        .into_iter()
        .find(|channel| channel.agents.contains(&agent.id))
        .ok_or_else(|| Error::Message("no channel was recorded".into()))?;
    println!("{}\t{}", recorded.id, recorded.kind);
    Ok(())
}

fn record(
    store: &Store,
    data: &Path,
    kind: &str,
    agent_id: &anyagents_core::AgentId,
    secret: &[u8],
) -> Result<(), Error> {
    let channel = store.create_channel(NewChannel {
        kind: kind.to_string(),
        identity: None,
        config: "{}".to_string(),
        credential_ref: None,
    })?;
    let credential_ref = format!("channels/{}", channel.id);
    FileCredentialStore::new(data).write(&credential_ref, secret)?;
    store.set_credential_ref(&channel.id, &credential_ref)?;
    store.attach_channel(&channel.id, agent_id)?;
    Ok(())
}

pub fn list_channels(data: &Path) -> Result<(), Error> {
    let store = open(data)?;
    for channel in store.list_channels()? {
        let names: Vec<String> = channel
            .agents
            .iter()
            .filter_map(|id| store.agent(id).ok().map(|agent| agent.name))
            .collect();
        println!(
            "{}\t{}\t{}\t{}",
            channel.id,
            channel.kind,
            channel.credential_ref.as_deref().unwrap_or("-"),
            names.join(",")
        );
    }
    Ok(())
}
