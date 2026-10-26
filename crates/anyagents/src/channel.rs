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
            print_messages(&target, read);
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
    print_messages(
        &target,
        InboxResponse {
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
        },
    );
    Ok(())
}

fn emit_turns(target: &str, query: Option<&str>, turns: Vec<crate::api::ConversationTurnView>) {
    if crate::ui::is_pretty() {
        crate::ui::print_conversation_pretty(target, query, &turns);
        return;
    }
    for turn in turns {
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
}

pub fn conversation(
    data: &Path,
    agent: Option<&str>,
    with: Option<&str>,
    since_ms: Option<i64>,
    limit: Option<usize>,
    query: Option<&str>,
) -> Result<(), Error> {
    let clean_query = query.map(str::trim).filter(|s| !s.is_empty());
    let target = match agent.map(str::trim).filter(|s| !s.is_empty()) {
        Some(a) => a.to_string(),
        None => {
            if clean_query.is_some() {
                "all".to_string()
            } else {
                client::agent_identity()
                    .map(|id| id.to_string())
                    .ok_or_else(|| {
                        Error::Message("conversation needs an agent name or id".into())
                    })?
            }
        }
    };
    if let Ok(store) = open(data) {
        let turns_res = if let Some(q) = clean_query {
            let qvec = embed_query_for_store(&store, data, q);
            store.search_conversation(Some(&target), with, since_ms, limit, q, qvec.as_deref())
        } else if target.eq_ignore_ascii_case("all") || target == "*" {
            store.conversation_all(with, since_ms, limit)
        } else {
            store
                .agent_by_name(&target)
                .and_then(|found| store.conversation(&found.id, with, since_ms, limit))
        };
        if let Ok(turns) = turns_res {
            let views = turns
                .into_iter()
                .map(|t| crate::api::ConversationTurnView {
                    id: t.id,
                    direction: t.direction,
                    from: t.from,
                    to: t.to,
                    text: t.text,
                    reply_to: t.reply_to,
                    timestamp_ms: t.timestamp_ms,
                })
                .collect();
            emit_turns(&target, clean_query, views);
            return Ok(());
        }
    }
    if client::can_call_server(data) {
        let mut params = Vec::new();
        if let Some(w) = with.map(str::trim).filter(|s| !s.is_empty()) {
            params.push(format!("with={}", urlencoding_simple(w)));
        }
        if let Some(s) = since_ms {
            params.push(format!("since_ms={s}"));
        }
        if let Some(l) = limit {
            params.push(format!("limit={l}"));
        }
        if let Some(q) = clean_query {
            params.push(format!("query={}", urlencoding_simple(q)));
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
            emit_turns(&target, clean_query, read.turns);
            return Ok(());
        }
    }
    let store = open(data)?;
    let turns = if let Some(q) = clean_query {
        let qvec = embed_query_for_store(&store, data, q);
        store.search_conversation(Some(&target), with, since_ms, limit, q, qvec.as_deref())?
    } else if target.eq_ignore_ascii_case("all") || target == "*" {
        store.conversation_all(with, since_ms, limit)?
    } else {
        let found = store.agent_by_name(&target)?;
        store.conversation(&found.id, with, since_ms, limit)?
    };
    let views = turns
        .into_iter()
        .map(|t| crate::api::ConversationTurnView {
            id: t.id,
            direction: t.direction,
            from: t.from,
            to: t.to,
            text: t.text,
            reply_to: t.reply_to,
            timestamp_ms: t.timestamp_ms,
        })
        .collect();
    emit_turns(&target, clean_query, views);
    Ok(())
}

fn urlencoding_simple(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'@') {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

pub(crate) fn resolve_embedding_endpoint_and_key(data: &Path) -> Option<(String, String)> {
    if let (Ok(url), Ok(key)) = (
        std::env::var("ANYAGENTS_EMBEDDING_URL"),
        std::env::var("ANYAGENTS_EMBEDDING_KEY"),
    ) {
        let u = url.trim();
        let k = key.trim();
        if !u.is_empty() && !k.is_empty() {
            return Some((u.to_string(), k.to_string()));
        }
    }
    if let Ok(key) = std::env::var("OPENROUTER_API_KEY") {
        let k = key.trim();
        if !k.is_empty() {
            return Some((
                "https://openrouter.ai/api/v1/embeddings".to_string(),
                k.to_string(),
            ));
        }
    }
    // Check `<data>/profiles/openrouter/default/` or `<data>/profiles/deepseek/`
    let candidates = [
        data.join("profiles/openrouter/default/api-key"),
        data.join("profiles/openrouter/default/.dsh/.credentials.yaml"),
        data.join("profiles/deepseek/api/.dsh/.credentials.yaml"),
        data.join("profiles/deepseek/web/.dsh/.credentials.yaml"),
    ];
    for path in candidates {
        let Ok(raw) = std::fs::read_to_string(&path) else {
            continue;
        };
        if path.file_name().is_some_and(|n| n == "api-key") {
            let k = raw.trim();
            if !k.is_empty() {
                return Some((
                    "https://openrouter.ai/api/v1/embeddings".to_string(),
                    k.to_string(),
                ));
            }
        }
        for line in raw.lines() {
            let trimmed = line.trim();
            if let Some(rest) = trimmed.strip_prefix("OPENROUTER_API_KEY:") {
                let k = rest.trim().trim_matches(['"', '\'']);
                if !k.is_empty() {
                    return Some((
                        "https://openrouter.ai/api/v1/embeddings".to_string(),
                        k.to_string(),
                    ));
                }
            }
        }
    }
    None
}

pub(crate) fn embed_query_for_store(
    store: &anyagents_core::Store,
    data: &Path,
    query: &str,
) -> Option<Vec<i8>> {
    let cleaned = anyagents_core::clean_rag_text(query);
    if cleaned.chars().count() < 2 {
        return None;
    }
    let chash = anyagents_core::rag_content_hash(&cleaned);
    if let Ok(Some(cached)) = store.get_embedding_i8(&chash) {
        return Some(cached);
    }
    let (model, dim) = store.locked_embedding_spec().ok()?;
    let (endpoint, api_key) = resolve_embedding_endpoint_and_key(data)?;
    let payload = serde_json::json!({
        "model": model,
        "input": [cleaned],
        "dimensions": dim,
    });
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(6)))
        .build()
        .new_agent();
    let resp = agent
        .post(&endpoint)
        .header("authorization", &format!("Bearer {api_key}"))
        .header("content-type", "application/json")
        .send(payload.to_string().as_bytes())
        .ok()?;
    let body = resp.into_body().read_to_string().ok()?;
    let val: serde_json::Value = serde_json::from_str(&body).ok()?;
    let arr = val.pointer("/data/0/embedding")?.as_array()?;
    if arr.len() != dim {
        return None;
    }
    let mut floats = Vec::with_capacity(dim);
    let mut norm_sq = 0.0_f64;
    for v in arr {
        let f = v.as_f64().unwrap_or(0.0);
        norm_sq += f * f;
        floats.push(f);
    }
    let norm = norm_sq.sqrt().max(1e-12);
    let vec_i8: Vec<i8> = floats
        .into_iter()
        .map(|f| ((f / norm) * 127.0).clamp(-127.0, 127.0).round() as i8)
        .collect();
    let _ = store.store_embedding_i8(&chash, &model, dim, &vec_i8);
    Some(vec_i8)
}

fn print_messages(target: &str, read: InboxResponse) {
    if crate::ui::is_pretty() {
        crate::ui::print_inbox_pretty(target, &read.messages);
        return;
    }
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
    if client::can_call_server(data) {
        let body = serde_json::json!({"kind": kind, "agent": agent_name, "token": token});
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
