//! `send`, `inbox` and `channels`: the operator's messages and channel
//! records.
//!
//! Under the boundary every call goes through the server, which decides who a
//! message is from before it writes anything. On a single-user machine there is
//! no server to ask for a channel secret, and the store is the one place the
//! message can land.

use std::collections::HashSet;
use std::io::{IsTerminal, Write};
use std::path::Path;

use crate::{ConversationArgs, HistoryOptions, InboxArgs};

use anyagents_core::{Address, Error, Incoming, Quote};

use crate::agent::open;
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

pub fn inbox(data: &Path, args: InboxArgs) -> Result<(), Error> {
    let target = args
        .agent
        .clone()
        .or_else(|| client::agent_identity().map(|id| id.to_string()))
        .ok_or_else(|| Error::Message("inbox needs an agent name or id".into()))?;
    let mut since = args.since;
    let mut initial = true;
    loop {
        // Once following, drain the complete new range before advancing the cursor.
        let limit = if args.all || !initial {
            None
        } else {
            Some(args.limit.unwrap_or(20))
        };
        let read = if client::can_call_server(data) {
            let mut params = vec![format!("unread={}", args.unread)];
            if let Some(since) = since {
                params.push(format!("since={since}"));
            }
            if let Some(limit) = limit {
                params.push(format!("limit={limit}"));
            }
            if let Some(sender) = &args.sender {
                params.push(format!("sender={}", urlencoding_simple(sender)));
            }
            client::call::<InboxResponse>(
                data,
                "GET",
                &format!(
                    "/v1/agents/{}/inbox?{}",
                    urlencoding_simple(&target),
                    params.join("&")
                ),
                None,
                &client::authority(data),
            )
            .map_err(Error::Message)?
            .ok_or_else(|| Error::Message("the server answered nothing".into()))?
        } else {
            if client::agent_identity().is_some() {
                return Err(Error::Message(
                    "agent inbox reads require the authenticated server".into(),
                ));
            }
            let store = open(data)?;
            let agent = store.agent_by_name(&target)?;
            InboxResponse {
                messages: store
                    .inbox_query_filtered(
                        &agent.id,
                        since,
                        limit,
                        args.unread,
                        args.sender.as_deref(),
                    )?
                    .into_iter()
                    .map(|m| crate::api::MessageView {
                        id: m.id.to_string(),
                        seq: m.seq,
                        text: m.text,
                        sender: m.sender,
                        reply_to: m.reply_to,
                        source_time_ms: m.source_time_ms,
                        received_at_ms: m.received_at_ms,
                        ack: m.ack.to_string(),
                        delivered_at_ms: m.delivered_at_ms,
                    })
                    .collect(),
            }
        };
        if let Some(last) = read.messages.last() {
            since = Some(last.seq);
        }
        if args.json {
            if args.follow {
                for message in &read.messages {
                    println!(
                        "{}",
                        serde_json::to_string(message)
                            .map_err(|e| Error::Message(e.to_string()))?
                    );
                }
            } else {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&read)
                        .map_err(|e| Error::Message(e.to_string()))?
                );
            }
        } else if args.full || !std::io::stdout().is_terminal() {
            for m in &read.messages {
                println!(
                    "[#{}] {}  IN   {} -> {}  ({})\n{}\n",
                    m.seq,
                    crate::ui::format_timestamp_ms(m.received_at_ms),
                    m.sender.as_deref().unwrap_or("unknown"),
                    target,
                    m.ack,
                    m.text
                );
            }
        } else {
            crate::ui::print_inbox_pretty(&target, &read.messages);
        }
        if !args.follow {
            return Ok(());
        }
        std::io::stdout().flush()?;
        initial = false;
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub fn emit_turns(
    turns: &[crate::api::ConversationTurnView],
    full: bool,
    json: bool,
) -> Result<(), Error> {
    if json {
        for turn in turns {
            println!(
                "{}",
                serde_json::to_string(turn).map_err(|e| Error::Message(e.to_string()))?
            );
        }
    } else if full || !std::io::stdout().is_terminal() {
        for t in turns {
            println!(
                "[#{}] {}  {:<4} {} -> {}\n{}\n",
                t.seq,
                t.timestamp_ms
                    .map_or_else(|| "unknown".into(), crate::ui::format_timestamp_ms),
                t.direction.to_uppercase(),
                t.from,
                t.to,
                t.text
            );
        }
    } else {
        crate::ui::print_conversation_pretty("", None, turns);
    }
    Ok(())
}

pub fn conversation(data: &Path, args: ConversationArgs) -> Result<(), Error> {
    let query = args.query.as_deref().or(args.text.as_deref());
    let target = args
        .agent
        .clone()
        .or_else(|| {
            if query.is_some() {
                Some("all".into())
            } else {
                client::agent_identity().map(|id| id.to_string())
            }
        })
        .ok_or_else(|| Error::Message("conversation needs an agent name or id".into()))?;
    let mut options = args.options;
    let mut seen = HashSet::new();
    loop {
        let read = read_conversation(data, &target, query, &options)?;
        let turns: Vec<_> = read
            .turns
            .into_iter()
            .filter(|t| seen.insert(t.id.clone()))
            .collect();
        if options.json && !options.follow {
            println!(
                "{}",
                serde_json::to_string_pretty(&ConversationResponse { turns, ..read })
                    .map_err(|e| Error::Message(e.to_string()))?
            );
            return Ok(());
        }
        emit_turns(&turns, options.full, options.json)?;
        if !options.follow {
            return Ok(());
        }
        // Outbound replies share the current inbound sequence. Re-read the
        // boundary and deduplicate IDs so replies at that sequence cannot vanish.
        if let Some(seq) = turns.iter().map(|t| t.seq).max() {
            options.from_seq = Some(options.from_seq.unwrap_or(seq).max(seq));
        }
        options.all = true;
        std::io::stdout().flush()?;
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub fn read_conversation(
    data: &Path,
    target: &str,
    query: Option<&str>,
    options: &HistoryOptions,
) -> Result<ConversationResponse, Error> {
    let direction = if options.inbound {
        Some("in")
    } else if options.outbound {
        Some("out")
    } else {
        options.direction.as_deref()
    };
    let limit = if options.all {
        None
    } else {
        Some(options.limit.unwrap_or(20))
    };
    if client::can_call_server(data) {
        let mut params = Vec::new();
        for (key, value) in [
            ("query", query),
            ("scope", options.scope.as_deref()),
            ("with", options.with.as_deref()),
            ("sender", options.sender.as_deref()),
            ("direction", direction),
            ("tool_name", options.tool.as_deref()),
            ("sort", options.sort.as_deref()),
            ("exclude", options.exclude.as_deref()),
        ] {
            if let Some(value) = value {
                params.push(format!("{key}={}", urlencoding_simple(value)));
            }
        }
        for (key, value) in [
            ("since_ms", options.since),
            ("until_ms", options.until),
            ("from_seq", options.from_seq),
            ("to_seq", options.to_seq),
        ] {
            if let Some(value) = value {
                params.push(format!("{key}={value}"));
            }
        }
        if let Some(limit) = limit {
            params.push(format!("limit={limit}"));
        }
        if options.all {
            params.push("all=true".into());
        }
        if let Some(radius) = options.radius {
            params.push(format!("radius={radius}"));
        }
        if options.tools || options.only_tools || options.tool.is_some() {
            params.push("include_tools=true".into());
        } else {
            params.push("include_tools=false".into());
        }
        if options.only_tools {
            params.push("only_tools=true".into());
        }
        return client::call::<ConversationResponse>(
            data,
            "GET",
            &format!(
                "/v1/agents/{}/conversation?{}",
                urlencoding_simple(target),
                params.join("&")
            ),
            None,
            &client::authority(data),
        )
        .map_err(Error::Message)?
        .ok_or_else(|| Error::Message("the server answered nothing".into()));
    }
    if client::agent_identity().is_some() {
        return Err(Error::Message(
            "agent conversation reads require the authenticated server".into(),
        ));
    }
    let store = open(data)?;
    let structured = query.is_some()
        || direction.is_some()
        || options.sender.is_some()
        || options.until.is_some()
        || options.from_seq.is_some()
        || options.to_seq.is_some()
        || options.tools
        || options.only_tools
        || options.tool.is_some()
        || options.sort.is_some()
        || options.radius.is_some()
        || options.exclude.is_some();
    let canonical = if matches!(target, "all" | "*") {
        None
    } else {
        Some(store.agent_by_name(target)?)
    };
    let turns = if options.scope.as_deref().is_some_and(|scope| scope != "all") {
        return Err(Error::Message(
            "offline memory/transcript scope reads require the server".into(),
        ));
    } else if structured {
        store.search_history(&anyagents_core::HistorySearchQuery {
            agent: Some(
                canonical
                    .as_ref()
                    .map_or(target, |a| a.name.as_str())
                    .to_string(),
            ),
            query: query.map(str::to_string),
            with_peer: options.with.clone(),
            direction: direction.map(str::to_string),
            sender: options.sender.clone(),
            include_tools: options.tools || options.only_tools || options.tool.is_some(),
            only_tools: options.only_tools,
            tool_name: options.tool.clone(),
            since_ms: options.since,
            until_ms: options.until,
            from_seq: options.from_seq,
            to_seq: options.to_seq,
            sort: anyagents_core::HistorySort::parse(options.sort.as_deref()),
            radius: options.radius.unwrap_or(0),
            limit: limit.unwrap_or(0),
            exclude: options
                .exclude
                .as_deref()
                .unwrap_or_default()
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect(),
            ..Default::default()
        })?
    } else if let Some(agent) = &canonical {
        store.conversation(&agent.id, options.with.as_deref(), options.since, limit)?
    } else {
        store.conversation_all(options.with.as_deref(), options.since, limit)?
    };
    Ok(ConversationResponse {
        agent: canonical
            .as_ref()
            .map_or(target, |a| a.name.as_str())
            .to_string(),
        agent_id: canonical
            .as_ref()
            .map(|a| a.id.to_string())
            .unwrap_or_else(|| "all".into()),
        turns: turns
            .into_iter()
            .map(|t| crate::api::ConversationTurnView {
                seq: t.seq,
                id: t.id,
                direction: t.direction,
                from: t.from,
                to: t.to,
                text: t.text,
                reply_to: t.reply_to,
                timestamp_ms: Some(t.timestamp_ms),
                raw_bytes: None,
                source: None,
                raw: None,
                window: None,
                tool_name: None,
                tool_names: Vec::new(),
                role: None,
            })
            .collect(),
    })
}

pub(crate) fn urlencoding_simple(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'@') {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// `channels add`, `channels request-bot`, and `channels list`.
pub fn channels(data: &Path, action: crate::ChannelsCommand) -> Result<(), Error> {
    match action {
        crate::ChannelsCommand::Add {
            kind,
            agent,
            token_file,
            bot_id,
            bot_username,
            manager,
        } => add_channel_with_options(
            data,
            &kind,
            &agent,
            token_file.as_deref(),
            bot_id,
            bot_username.as_deref(),
            manager.as_deref(),
        ),
        crate::ChannelsCommand::RequestBot {
            agent,
            bot_username,
            manager,
        } => add_channel_with_options(
            data,
            "telegram",
            &agent,
            None,
            None,
            bot_username.as_deref(),
            manager.as_deref(),
        ),
        crate::ChannelsCommand::List => list_channels(data),
    }
}

/// Record a channel and copy its secret into the credential store.
#[allow(dead_code)]
pub fn add_channel(
    data: &Path,
    kind: &str,
    agent_name: &str,
    token_file: &Path,
) -> Result<(), Error> {
    add_channel_with_options(data, kind, agent_name, Some(token_file), None, None, None)
}

pub fn add_channel_with_options(
    data: &Path,
    kind: &str,
    agent_name: &str,
    token_file: Option<&Path>,
    bot_id: Option<i64>,
    bot_username: Option<&str>,
    manager: Option<&str>,
) -> Result<(), Error> {
    if kind != "telegram" {
        return Err(Error::Message(format!("unsupported channel kind: {kind}")));
    }
    let token_opt = if let Some(tf) = token_file {
        let raw = std::fs::read(tf)?;
        let text =
            String::from_utf8(raw).map_err(|_| Error::Message("token file is not text".into()))?;
        let trimmed = text.trim().to_string();
        if trimmed.is_empty() || trimmed.chars().any(|ch| ch.is_whitespace() || ch == '/') {
            return Err(Error::Message(
                "token is empty or contains whitespace or a slash".into(),
            ));
        }
        Some(trimmed)
    } else {
        None
    };
    if client::can_call_server(data) {
        let body = serde_json::json!({
            "kind": kind,
            "agent": agent_name,
            "token": token_opt,
            "bot_id": bot_id,
            "bot_username": bot_username,
            "manager": manager,
        });
        match client::call::<serde_json::Value>(
            data,
            "POST",
            "/v1/channels",
            Some(body.to_string().into_bytes()),
            &client::authority(data),
        ) {
            Ok(result) => {
                println!(
                    "{}",
                    result.ok_or_else(|| Error::Message("the server answered nothing".into()))?
                );
                return Ok(());
            }
            Err(err) if err.contains("404") => {}
            Err(err) => return Err(Error::Message(err)),
        }
    }
    let store = open(data)?;
    let agent = store.agent_by_name(agent_name)?;
    if let Some(ref token) = token_opt {
        let _ = crate::telegram::bind_agent_telegram_token(&store, data, &agent, token, None)?;
        let recorded = store
            .list_channels()?
            .into_iter()
            .find(|channel| channel.agents.contains(&agent.id))
            .ok_or_else(|| Error::Message("no channel was recorded".into()))?;
        println!("{}\t{}", recorded.id, recorded.kind);
        return Ok(());
    }
    let out = crate::telegram::request_or_bind_telegram_channel(
        &store,
        data,
        None,
        &agent,
        None,
        bot_id,
        bot_username,
        manager,
        false,
    )?;
    println!("{out}");
    Ok(())
}

pub fn list_channels(data: &Path) -> Result<(), Error> {
    if !crate::ui::is_pretty() && client::can_call_server(data) {
        match client::call::<serde_json::Value>(
            data,
            "GET",
            "/v1/channels",
            None,
            &client::authority(data),
        ) {
            Ok(result) => {
                println!(
                    "{}",
                    result.ok_or_else(|| Error::Message("the server answered nothing".into()))?
                );
                return Ok(());
            }
            Err(err) if err.contains("404") => {}
            Err(err) => return Err(Error::Message(err)),
        }
    }
    let store = open(data)?;
    let channels = store.list_channels()?;
    if crate::ui::is_pretty() {
        if channels.is_empty() {
            println!("  {}", crate::ui::dim("No channels recorded."));
            return Ok(());
        }
        println!(
            "\n  {}  {} total\n",
            crate::ui::bold_cyan("Channels"),
            crate::ui::bold(&channels.len().to_string())
        );
        let mut table = crate::ui::Table::new(&[
            ("KIND", false),
            ("BOT", false),
            ("AGENTS", false),
            ("GROUPS / CHATS", false),
            ("ID", false),
        ]);
        for channel in channels {
            let names: Vec<String> = channel
                .agents
                .iter()
                .filter_map(|id| store.agent(id).ok().map(|agent| agent.name))
                .collect();
            let agents_cell = if names.is_empty() {
                crate::ui::dim("-")
            } else {
                crate::ui::bold(&names.join(", "))
            };
            let bot_cell = match channel.identity.as_deref() {
                Some(id) if !id.is_empty() => {
                    let uname = id.split_once(':').map(|(_, u)| u).unwrap_or(id);
                    if uname.starts_with('@') {
                        crate::ui::cyan(uname)
                    } else {
                        crate::ui::cyan(&format!("@{uname}"))
                    }
                }
                _ => crate::ui::dim("-"),
            };
            let chats_cell = summarize_channel_chats(&channel.state);
            table.add_row(vec![
                crate::ui::cyan(&channel.kind),
                bot_cell,
                agents_cell,
                chats_cell,
                crate::ui::dim(channel.id.as_ref()),
            ]);
        }
        table.print();
        println!();
        return Ok(());
    }
    for channel in channels {
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

fn summarize_channel_chats(state_json: &str) -> String {
    let Ok(val) = serde_json::from_str::<serde_json::Value>(state_json) else {
        return crate::ui::dim("-");
    };
    let Some(chats) = val.get("chats").and_then(|c| c.as_array()) else {
        return crate::ui::dim("-");
    };
    let mut groups: Vec<String> = Vec::new();
    let mut dms = 0usize;
    for entry in chats {
        let chat_id = entry.get("chat_id").and_then(|v| v.as_i64()).unwrap_or(0);
        if chat_id < 0 {
            let title = entry
                .get("title")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .unwrap_or_else(|| chat_id.to_string());
            if !groups.contains(&title) {
                groups.push(title);
            }
        } else if chat_id > 0 {
            dms += 1;
        }
    }
    if groups.is_empty() && dms == 0 {
        return crate::ui::dim("-");
    }
    let mut parts = Vec::new();
    if !groups.is_empty() {
        parts.push(groups.join(", "));
    }
    if dms > 0 {
        parts.push(crate::ui::dim(&format!(
            "{dms} DM{}",
            if dms == 1 { "" } else { "s" }
        )));
    }
    parts.join(" · ")
}
