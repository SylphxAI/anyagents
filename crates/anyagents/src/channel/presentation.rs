//! Human-only channel output. Machine serializers stay in the command paths.
use crate::api::{ConversationTurnView, MessageView};
use crate::ui::{self, Table};

pub(super) fn delivery(to: &str, id: &str, seq: Option<i64>, duplicate: bool) {
    if duplicate {
        ui::note(&format!("Already delivered to {to}; no new message sent."));
    } else {
        ui::success(&format!("Sent to {to}"));
    }
    if ui::is_verbose() {
        let detail = seq.map_or_else(|| id.to_owned(), |seq| format!("{id} · sequence {seq}"));
        ui::note(&detail);
    }
}

pub(super) fn inbox(target: &str, messages: &[MessageView], full: bool) {
    ui::heading(&format!("Inbox · {target}"));
    if messages.is_empty() {
        ui::note("No messages matched.");
        return;
    }
    if full {
        for message in messages {
            println!(
                "  {} {} {}  {}",
                ui::bold(message.sender.as_deref().unwrap_or("unknown")),
                ui::dim("→"),
                ui::bold(target),
                ui::dim(&message.ack)
            );
            if ui::is_verbose() {
                ui::note(&format!(
                    "{} · #{} · {}",
                    message.id,
                    message.seq,
                    ui::format_timestamp_ms(message.received_at_ms)
                ));
            }
            body(&message.text);
        }
    } else {
        let mut headers = vec![("FROM", false), ("STATUS", false), ("MESSAGE", false)];
        if ui::is_verbose() {
            headers = vec![
                ("SEQ", true),
                ("TIME", false),
                ("FROM", false),
                ("STATUS", false),
                ("MESSAGE", false),
            ];
        }
        let mut table = Table::new(&headers);
        for message in messages {
            let status = match message.ack.as_str() {
                "delivered" => ui::green("delivered"),
                "received" => ui::yellow("queued"),
                other => ui::dim(other),
            };
            let mut cells = vec![
                ui::cyan(message.sender.as_deref().unwrap_or("unknown")),
                status,
                message.text.replace('\n', " ↵ "),
            ];
            if ui::is_verbose() {
                cells.insert(0, ui::dim(&ui::format_timestamp_ms(message.received_at_ms)));
                cells.insert(0, ui::dim(&message.seq.to_string()));
            }
            table.add_row(cells);
        }
        table.print();
        println!();
    }
}

fn body(text: &str) {
    for line in text.split('\n') {
        println!("    {line}");
    }
    println!();
}

pub(super) fn conversation(target: &str, query: Option<&str>, turns: &[ConversationTurnView]) {
    let title = match (target.is_empty(), query) {
        (_, Some(query)) => format!("Search · {target} · “{query}”"),
        (false, None) => format!("Conversation · {target}"),
        (true, None) => "Conversation".to_owned(),
    };
    ui::heading(&title);
    if turns.is_empty() {
        ui::note("No conversation turns matched.");
        return;
    }
    for turn in turns {
        let direction = if turn.direction == "out" {
            ui::green("→ out")
        } else {
            ui::cyan("← in")
        };
        println!(
            "  {direction}  {} {} {}",
            ui::bold(&turn.from),
            ui::dim("→"),
            ui::bold(&turn.to)
        );
        if ui::is_verbose() {
            ui::note(&format!(
                "{} · #{} · {}",
                turn.id,
                turn.seq,
                turn.timestamp_ms
                    .map_or_else(|| "unknown time".into(), ui::format_timestamp_ms)
            ));
        }
        body(&turn.text);
    }
}

pub(super) fn channel_receipt(result: &serde_json::Value, agent: &str) {
    let field = |key: &str| result.get(key).and_then(serde_json::Value::as_str);
    let agent = field("agent").unwrap_or(agent);
    if field("status") == Some("pending_creation") {
        ui::heading("Bot creation requested");
        ui::note(&format!(
            "For {agent} · {}",
            field("suggested_bot_username").unwrap_or("Telegram bot")
        ));
        if let Some(manager) = field("manager_bot") {
            ui::note(&format!("Manager: {manager}"));
        }
        if let Some(url) = field("newbot_url") {
            ui::note(&format!("Complete bot creation: {url}"));
        }
    } else {
        let bot = field("bot_username").unwrap_or("Telegram channel");
        ui::success(&format!("Channel bound · {bot} → {agent}"));
    }
    if ui::is_verbose() {
        for key in ["channel_id", "manager_channel_id", "agent_id"] {
            if let Some(value) = field(key) {
                ui::note(&format!("{key}: {value}"));
            }
        }
    }
}

pub(super) struct ChannelRow {
    pub id: String,
    pub kind: String,
    pub bot: Option<String>,
    pub agents: Vec<String>,
    pub chats: String,
}

pub(super) fn channels(rows: Vec<ChannelRow>) {
    ui::heading("Channels");
    if rows.is_empty() {
        ui::note("No channels recorded.");
        return;
    }
    let mut headers = vec![
        ("KIND", false),
        ("BOT", false),
        ("AGENTS", false),
        ("GROUPS / CHATS", false),
    ];
    if ui::is_verbose() {
        headers.push(("ID", false));
    }
    let mut table = Table::new(&headers);
    for row in rows {
        let mut cells = vec![
            ui::cyan(&row.kind),
            row.bot.map_or_else(
                || ui::dim("—"),
                |bot| {
                    let username = bot.split_once(':').map_or(bot.as_str(), |(_, name)| name);
                    ui::cyan(&format!("@{}", username.trim_start_matches('@')))
                },
            ),
            if row.agents.is_empty() {
                ui::dim("—")
            } else {
                ui::bold(&row.agents.join(", "))
            },
            row.chats,
        ];
        if ui::is_verbose() {
            cells.push(ui::dim(&row.id));
        }
        table.add_row(cells);
    }
    table.print();
    println!();
}
