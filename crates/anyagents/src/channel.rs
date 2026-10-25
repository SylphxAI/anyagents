//! `send`, `inbox` and `channels`: the operator's messages and channel
//! records.
//!
//! Under the boundary every call goes through the server, which decides who a
//! message is from before it writes anything. On a single-user machine there is
//! no server to ask for a channel secret, and the store is the one place the
//! message can land.

use std::path::Path;

use anyagents_core::{
    Address, CredentialStore, Error, FileCredentialStore, Incoming, NewChannel, Quote, Store,
};

use crate::agent::{named, open};
use crate::api::{
    ConversationResponse, InboxResponse, SendArticle, SendChecklist, SendLocation, SendMedia,
    SendPoll, SendRequest, SendResponse,
};
use crate::client;
use crate::server;
use crate::telegram;

pub struct SendOptions {
    pub to: String,
    pub text: String,
    pub source_key: Option<String>,
    pub source_time_ms: Option<i64>,
    pub reply_to: Option<String>,
    pub from: Option<String>,
    pub quote: Option<String>,
    pub media: Option<SendMedia>,
    pub location: Option<SendLocation>,
    pub poll: Option<SendPoll>,
    pub checklist: Option<SendChecklist>,
    pub article: Option<SendArticle>,
}

pub fn send_simple(data: &Path, to: &str, text: String) -> Result<(), Error> {
    send_with_options(
        data,
        SendOptions {
            to: to.to_string(),
            text,
            source_key: None,
            source_time_ms: None,
            reply_to: None,
            from: None,
            quote: None,
            media: None,
            location: None,
            poll: None,
            checklist: None,
            article: None,
        },
    )
}

pub fn send_with_options(data: &Path, opts: SendOptions) -> Result<(), Error> {
    let quote_obj = opts
        .quote
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|t| Quote {
            text: t.to_string(),
            ..Quote::default()
        });
    if server::socket_path(data).exists() {
        let auth = client::authority(data);
        let sender = match (&opts.from, &auth) {
            (Some(f), _) => Some(f.clone()),
            (None, client::Authority::Operator) => Some("operator".to_string()),
            (None, _) => None,
        };
        let recipients: Vec<String> = opts
            .to
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        let body = SendRequest {
            sender,
            to: if opts
                .reply_to
                .as_deref()
                .is_some_and(|r| crate::reply::ref_message(r).is_some())
                && (recipients.is_empty() || (recipients.len() == 1 && recipients[0] == "-"))
            {
                Vec::new()
            } else {
                recipients
            },
            text: opts.text.clone(),
            reply_to: opts.reply_to.clone(),
            quote: quote_obj.clone(),
            media: opts.media.clone(),
            location: opts.location.clone(),
            poll: opts.poll.clone(),
            checklist: opts.checklist.clone(),
            article: opts.article.clone(),
            idempotency_key: opts.source_key.clone(),
        };
        let sent = client::call::<SendResponse>(
            data,
            "POST",
            "/v1/send",
            Some(serde_json::to_vec(&body).map_err(|err| Error::Message(err.to_string()))?),
            &auth,
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
    send_stored(
        data,
        &opts.to,
        opts.text,
        opts.source_key,
        opts.source_time_ms,
        opts.reply_to,
        opts.from,
        quote_obj,
    )
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
    quote: Option<Quote>,
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
                    quote,
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
            quote,
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

pub fn inbox(
    data: &Path,
    agent: Option<&str>,
    since: Option<i64>,
    limit: Option<usize>,
) -> Result<(), Error> {
    let target = match agent.map(str::trim).filter(|s| !s.is_empty()) {
        Some(a) => a.to_string(),
        None => client::agent_identity()
            .map(|id| id.to_string())
            .ok_or_else(|| Error::Message("inbox needs an agent name or id".into()))?,
    };
    if client::can_call_server(data) {
        let who = named(data, &target)?.id;
        let mut params = Vec::new();
        if let Some(s) = since {
            params.push(format!("since={s}"));
        }
        if let Some(l) = limit {
            params.push(format!("limit={l}"));
        }
        let path = if params.is_empty() {
            format!("/v1/agents/{who}/inbox")
        } else {
            format!("/v1/agents/{who}/inbox?{}", params.join("&"))
        };
        let read =
            client::call::<InboxResponse>(data, "GET", &path, None, &client::authority(data))
                .map_err(Error::Message)?;
        if let Some(read) = read {
            print_messages(read);
            return Ok(());
        }
    }
    let store = open(data)?;
    let agent = store.agent_by_name(&target)?;
    let mut rows = store.inbox(&agent.id, since)?;
    if let Some(limit) = limit {
        if since.is_none() && rows.len() > limit {
            rows.drain(..rows.len() - limit);
        } else if rows.len() > limit {
            rows.truncate(limit);
        }
    }
    print_messages(InboxResponse {
        messages: rows
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

pub fn conversation(
    data: &Path,
    agent: Option<&str>,
    with: Option<&str>,
    since_ms: Option<i64>,
    limit: Option<usize>,
) -> Result<(), Error> {
    let target = match agent.map(str::trim).filter(|s| !s.is_empty()) {
        Some(a) => a.to_string(),
        None => client::agent_identity()
            .map(|id| id.to_string())
            .ok_or_else(|| Error::Message("conversation needs an agent name or id".into()))?,
    };
    if client::can_call_server(data) {
        let mut params = Vec::new();
        if let Some(w) = with.map(str::trim).filter(|s| !s.is_empty()) {
            params.push(format!("with={w}"));
        }
        if let Some(s) = since_ms {
            params.push(format!("since_ms={s}"));
        }
        if let Some(l) = limit {
            params.push(format!("limit={l}"));
        }
        let path = if params.is_empty() {
            format!("/v1/agents/{target}/conversation")
        } else {
            format!("/v1/agents/{target}/conversation?{}", params.join("&"))
        };
        let read = client::call::<ConversationResponse>(
            data,
            "GET",
            &path,
            None,
            &client::authority(data),
        )
        .map_err(Error::Message)?;
        if let Some(read) = read {
            for turn in read.turns {
                println!(
                    "{}\t{}\t{}\t{}\t{}\t{}",
                    turn.timestamp_ms,
                    turn.direction,
                    turn.id,
                    turn.from,
                    turn.to,
                    turn.text.replace('\n', "\\n"),
                );
            }
            return Ok(());
        }
    }
    let store = open(data)?;
    let found = store.agent_by_name(&target)?;
    for turn in store.conversation(&found.id, with, since_ms, limit)? {
        println!(
            "{}\t{}\t{}\t{}\t{}\t{}",
            turn.timestamp_ms,
            turn.direction,
            turn.id,
            turn.from,
            turn.to,
            turn.text.replace('\n', "\\n"),
        );
    }
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
    if server::socket_path(data).exists() || client::agent_identity().is_some() {
        let body = serde_json::json!({"kind": kind, "agent": agent_name, "token": token});
        let result = client::call::<serde_json::Value>(
            data,
            "POST",
            "/v1/channels",
            Some(body.to_string().into_bytes()),
            &client::authority(data),
        )
        .map_err(Error::Message)?;
        println!(
            "{}",
            result.ok_or_else(|| Error::Message("the server answered nothing".into()))?
        );
        return Ok(());
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

pub(crate) fn record(
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
    if server::socket_path(data).exists() || client::agent_identity().is_some() {
        let result = client::call::<serde_json::Value>(
            data,
            "GET",
            "/v1/channels",
            None,
            &client::authority(data),
        )
        .map_err(Error::Message)?;
        println!(
            "{}",
            result.ok_or_else(|| Error::Message("the server answered nothing".into()))?
        );
        return Ok(());
    }
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
