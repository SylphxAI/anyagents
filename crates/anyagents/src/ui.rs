//! Human-first terminal presentation for the `anyagents` CLI.
//!
//! When standard output is an interactive terminal (or `ANYAGENTS_PRETTY=1` is
//! set), commands render structured tables, hierarchical agent trees, semantic
//! status badges, and human-readable durations/timestamps. When piped or run in
//! automated harnesses (`!stdout.is_terminal()`), commands emit stable TSV.

use std::collections::HashMap;
use std::io::IsTerminal as _;

use crate::api::{AgentView, ConversationTurnView, MessageView};

/// True when stdout should use interactive human formatting.
pub fn is_pretty() -> bool {
    if std::env::var_os("ANYAGENTS_RAW").is_some_and(|v| v == "1" || v == "true") {
        return false;
    }
    if std::env::var_os("ANYAGENTS_PRETTY").is_some_and(|v| v == "1" || v == "true") {
        return true;
    }
    std::io::stdout().is_terminal()
}

/// True when ANSI color escapes should be emitted.
pub fn use_color() -> bool {
    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }
    if std::env::var_os("FORCE_COLOR").is_some_and(|v| v != "0")
        || std::env::var_os("ANYAGENTS_PRETTY").is_some_and(|v| v == "1" || v == "true")
    {
        return true;
    }
    std::io::stdout().is_terminal()
}

fn style(code: &str, text: &str) -> String {
    if use_color() {
        format!("\x1b[{code}m{text}\x1b[0m")
    } else {
        text.to_string()
    }
}

pub fn bold(text: &str) -> String {
    style("1", text)
}

pub fn dim(text: &str) -> String {
    style("2", text)
}

pub fn green(text: &str) -> String {
    style("32", text)
}

pub fn bold_green(text: &str) -> String {
    style("1;32", text)
}

pub fn yellow(text: &str) -> String {
    style("33", text)
}

pub fn red(text: &str) -> String {
    style("31", text)
}

pub fn cyan(text: &str) -> String {
    style("36", text)
}

pub fn bold_cyan(text: &str) -> String {
    style("1;36", text)
}

pub fn magenta(text: &str) -> String {
    style("35", text)
}

/// Visible character width of a string, ignoring ANSI CSI sequences.
pub fn visible_width(s: &str) -> usize {
    let mut width = 0;
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' {
            if chars.peek() == Some(&'[') {
                chars.next();
                for c in chars.by_ref() {
                    if ('@'..='~').contains(&c) {
                        break;
                    }
                }
            }
            continue;
        }
        width += 1;
    }
    width
}

/// Pad a potentially ANSI-styled string on the right to `target_width` visible chars.
pub fn pad_right(s: &str, target_width: usize) -> String {
    let vis = visible_width(s);
    if vis >= target_width {
        s.to_string()
    } else {
        format!("{s}{}", " ".repeat(target_width - vis))
    }
}

/// Pad a potentially ANSI-styled string on the left to `target_width` visible chars.
pub fn pad_left(s: &str, target_width: usize) -> String {
    let vis = visible_width(s);
    if vis >= target_width {
        s.to_string()
    } else {
        format!("{}{s}", " ".repeat(target_width - vis))
    }
}

/// Format a duration in milliseconds into a concise human string (`850ms`, `14s`, `21m 42s`, `19h 28m`, `4d 2h`).
pub fn format_duration_ms(ms: i64) -> String {
    if ms < 0 {
        return "0s".to_string();
    }
    if ms < 1_000 {
        return format!("{ms}ms");
    }
    let total_secs = ms / 1_000;
    if total_secs < 60 {
        return format!("{total_secs}s");
    }
    let total_mins = total_secs / 60;
    let rem_secs = total_secs % 60;
    if total_mins < 60 {
        if rem_secs == 0 {
            return format!("{total_mins}m");
        }
        return format!("{total_mins}m {rem_secs:02}s");
    }
    let total_hours = total_mins / 60;
    let rem_mins = total_mins % 60;
    if total_hours < 48 {
        if rem_mins == 0 {
            return format!("{total_hours}h");
        }
        return format!("{total_hours}h {rem_mins:02}m");
    }
    let days = total_hours / 24;
    let rem_hours = total_hours % 24;
    if rem_hours == 0 {
        format!("{days}d")
    } else {
        format!("{days}d {rem_hours}h")
    }
}

/// Format an epoch millisecond timestamp as `YYYY-MM-DD HH:MM:SS` (UTC).
pub fn format_timestamp_ms(ms: i64) -> String {
    if ms <= 0 {
        return "-".to_string();
    }
    let total_secs = ms / 1_000;
    let days = total_secs.div_euclid(86_400);
    let secs_of_day = total_secs.rem_euclid(86_400);
    let hour = secs_of_day / 3_600;
    let min = (secs_of_day % 3_600) / 60;
    let sec = secs_of_day % 60;

    // Howard Hinnant's civil_from_days
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    format!("{year:04}-{m:02}-{d:02} {hour:02}:{min:02}:{sec:02}")
}

/// Format token counts with thousands separators (`1,234,567` or `1.2M`).
pub fn format_tokens(n: i64) -> String {
    if n >= 1_000_000 {
        format!("{:.2}M", n as f64 / 1_000_000.0)
    } else if n >= 10_000 {
        format!("{:.1}k", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

/// Format an agent's combined `(state, process, turn_state, quota_reset_at_ms)` as a semantic status badge.
pub fn format_agent_status(
    state: &str,
    process: &str,
    turn_state: &str,
    quota_reset_at_ms: Option<i64>,
) -> String {
    let now = anyagents_core::now_ms();
    let on_quota = quota_reset_at_ms.is_some_and(|ms| ms > now && ms - now < 86_400_000 * 7);
    match (state, process, turn_state) {
        ("running", _, _) if on_quota => yellow("○ quota"),
        ("running", "alive", "working") => bold_green("● working"),
        ("running", "alive", _) => green("○ idle"),
        ("running", "down", _) | ("running", "offline", _) | ("running", "-", _) => {
            yellow("○ starting")
        }
        ("running", other, _) => cyan(&format!("● {other}")),
        ("paused", _, _) => yellow("◐ paused"),
        ("archived", _, _) => dim("▫ archived"),
        (other, _, _) => dim(other),
    }
}

/// Format an agent's mailbox lag (`oldest_undelivered_age_ms`) and quota reset.
pub fn format_mailbox_lag(
    state: &str,
    oldest_undelivered_age_ms: Option<i64>,
    quota_reset_at_ms: Option<i64>,
) -> String {
    let now = anyagents_core::now_ms();
    if let Some(reset_ms) = quota_reset_at_ms
        && reset_ms > now
        && reset_ms - now < 86_400_000 * 7
    {
        return red(&format!("! quota ({})", format_duration_ms(reset_ms - now)));
    }
    match (state, oldest_undelivered_age_ms) {
        ("archived", _) => dim("-"),
        ("paused", Some(ms)) if ms > 0 => dim(&format!("◐ hold ({})", format_duration_ms(ms))),
        ("paused", _) => dim("-"),
        (_, None) => green("✓ clear"),
        (_, Some(ms)) if ms < 5_000 => cyan(&format!("~ queued ({})", format_duration_ms(ms))),
        (_, Some(ms)) if ms < 120_000 => yellow(&format!("~ wait {}", format_duration_ms(ms))),
        (_, Some(ms)) => red(&format!("! lag {}", format_duration_ms(ms))),
    }
}

/// A self-sizing terminal table with left/right column alignment.
pub struct Table {
    headers: Vec<(&'static str, bool)>,
    rows: Vec<Vec<String>>,
    indent: &'static str,
}

impl Table {
    /// `headers`: slice of `(title, right_align)`.
    pub fn new(headers: &[(&'static str, bool)]) -> Self {
        Self {
            headers: headers.to_vec(),
            rows: Vec::new(),
            indent: "  ",
        }
    }

    pub fn add_row(&mut self, row: Vec<String>) {
        self.rows.push(row);
    }

    pub fn print(&self) {
        if self.headers.is_empty() {
            return;
        }
        let cols = self.headers.len();
        let mut widths: Vec<usize> = self
            .headers
            .iter()
            .map(|(h, _)| visible_width(h))
            .collect();
        for row in &self.rows {
            for (i, cell) in row.iter().enumerate().take(cols) {
                let w = visible_width(cell);
                if w > widths[i] {
                    widths[i] = w;
                }
            }
        }

        // Header line
        let mut header_line = String::from(self.indent);
        for (i, (title, right)) in self.headers.iter().enumerate() {
            let styled = dim(&bold(title));
            if i + 1 == cols && !*right {
                header_line.push_str(&styled);
            } else if *right {
                header_line.push_str(&pad_left(&styled, widths[i]));
                if i + 1 < cols {
                    header_line.push_str("  ");
                }
            } else {
                header_line.push_str(&pad_right(&styled, widths[i]));
                if i + 1 < cols {
                    header_line.push_str("  ");
                }
            }
        }
        println!("{header_line}");

        // Data rows
        for row in &self.rows {
            let mut line = String::from(self.indent);
            for (i, (_, right)) in self.headers.iter().enumerate() {
                let cell = row.get(i).map(String::as_str).unwrap_or("");
                if i + 1 == cols && !*right {
                    line.push_str(cell);
                } else if *right {
                    line.push_str(&pad_left(cell, widths[i]));
                    if i + 1 < cols {
                        line.push_str("  ");
                    }
                } else {
                    line.push_str(&pad_right(cell, widths[i]));
                    if i + 1 < cols {
                        line.push_str("  ");
                    }
                }
            }
            println!("{line}");
        }
    }
}

fn state_rank(state: &str, process: &str, turn_state: &str) -> u8 {
    match (state, process, turn_state) {
        ("running", "alive", "working") => 0,
        ("running", "alive", _) => 1,
        ("running", _, _) => 2,
        ("paused", _, _) => 3,
        ("archived", _, _) => 4,
        _ => 5,
    }
}

/// Render the agent list as a hierarchical parent -> sub-agent tree with status
/// badges, harness, model, human-readable mailbox state, and ID.
///
/// When `include_archived` is `false` (the default `anyagents agents list` /
/// `anyagents status` view), `archived` agents and sub-agents are omitted from
/// the table rows so historical sub-agents never clutter the live roster, while
/// the header notes how many archived records are hidden (`--state all`).
pub fn print_agents_table(
    agents: &[AgentView],
    profile_lookup: Option<&dyn Fn(&AgentView) -> String>,
    include_archived: bool,
) {
    if agents.is_empty() {
        println!("  {}", dim("No agents found."));
        return;
    }

    let now = anyagents_core::now_ms();
    let running = agents.iter().filter(|a| a.state == "running").count();
    let working = agents
        .iter()
        .filter(|a| a.state == "running" && a.process == "alive" && a.turn_state == "working")
        .count();
    let quota_count = agents
        .iter()
        .filter(|a| {
            a.state == "running"
                && a.quota_reset_at_ms
                    .is_some_and(|ms| ms > now && ms - now < 86_400_000 * 7)
        })
        .count();
    let idle = running
        .saturating_sub(working)
        .saturating_sub(quota_count);
    let paused = agents.iter().filter(|a| a.state == "paused").count();
    let archived = agents.iter().filter(|a| a.state == "archived").count();

    let visible_agents: Vec<&AgentView> = agents
        .iter()
        .filter(|a| include_archived || a.state != "archived")
        .collect();

    let mut summary = Vec::new();
    if running > 0 {
        let mut breakdown = Vec::new();
        if working > 0 {
            breakdown.push(format!("{working} working"));
        }
        if idle > 0 {
            breakdown.push(format!("{idle} idle"));
        }
        if quota_count > 0 {
            breakdown.push(format!("{quota_count} quota"));
        }
        let detail = if breakdown.is_empty() {
            bold_green(&format!("{running} active"))
        } else {
            format!(
                "{} ({})",
                bold_green(&format!("{running} active")),
                breakdown.join(", ")
            )
        };
        summary.push(detail);
    }
    if paused > 0 {
        summary.push(yellow(&format!("{paused} paused")));
    }
    if archived > 0 {
        if include_archived {
            summary.push(dim(&format!("{archived} archived")));
        } else {
            summary.push(dim(&format!("{archived} archived hidden (--state all)")));
        }
    }
    println!(
        "\n  {}  {}\n",
        bold_cyan("Agents"),
        summary.join(&dim(" · "))
    );

    if visible_agents.is_empty() {
        println!("  {}\n", dim("No active agents."));
        return;
    }

    let known_ids: std::collections::HashSet<&str> =
        visible_agents.iter().map(|a| a.id.as_str()).collect();
    let mut children_by_parent: HashMap<&str, Vec<&AgentView>> = HashMap::new();
    let mut roots: Vec<&AgentView> = Vec::new();

    for agent in visible_agents {
        if let Some(parent_id) = agent.parent.as_deref()
            && known_ids.contains(parent_id)
        {
            children_by_parent.entry(parent_id).or_default().push(agent);
        } else {
            roots.push(agent);
        }
    }

    roots.sort_by(|a, b| {
        state_rank(&a.state, &a.process, &a.turn_state)
            .cmp(&state_rank(&b.state, &b.process, &b.turn_state))
            .then_with(|| a.name.cmp(&b.name))
    });
    for list in children_by_parent.values_mut() {
        list.sort_by(|a, b| {
            state_rank(&a.state, &a.process, &a.turn_state)
                .cmp(&state_rank(&b.state, &b.process, &b.turn_state))
                .then_with(|| a.name.cmp(&b.name))
        });
    }

    let mut ordered: Vec<(String, &AgentView)> = Vec::with_capacity(agents.len());
    for root in roots {
        let root_label = match root.state.as_str() {
            "running" => bold(&root.name),
            "paused" => root.name.clone(),
            _ => dim(&root.name),
        };
        ordered.push((root_label, root));
        if let Some(kids) = children_by_parent.get(root.id.as_str()) {
            let count = kids.len();
            for (idx, kid) in kids.iter().enumerate() {
                let branch = if idx + 1 == count { "└─ " } else { "├─ " };
                let kid_name = if kid.state == "archived" {
                    dim(&kid.name)
                } else {
                    kid.name.clone()
                };
                ordered.push((format!("{}{kid_name}", dim(branch)), kid));
            }
        }
    }

    let mut headers = vec![
        ("AGENT", false),
        ("STATUS", false),
        ("HARNESS", false),
        ("MODEL", false),
    ];
    if profile_lookup.is_some() {
        headers.push(("PROFILE", false));
    }
    headers.push(("MAILBOX", false));
    headers.push(("ID", false));

    let mut table = Table::new(&headers);
    for (display_name, agent) in ordered {
        let is_dim = agent.state == "archived";
        let status = format_agent_status(
            &agent.state,
            &agent.process,
            &agent.turn_state,
            agent.quota_reset_at_ms,
        );
        let harness = if is_dim {
            dim(&agent.harness)
        } else {
            cyan(&agent.harness)
        };
        let model_raw = agent.model.as_deref().unwrap_or("-");
        let model = if is_dim {
            dim(model_raw)
        } else if model_raw.contains('#') {
            let (base, effort) = model_raw.split_once('#').unwrap_or((model_raw, ""));
            format!("{base}{}", magenta(&format!("#{effort}")))
        } else {
            model_raw.to_string()
        };
        let mailbox = format_mailbox_lag(
            &agent.state,
            agent.oldest_undelivered_age_ms,
            agent.quota_reset_at_ms,
        );
        let id = dim(&agent.id);

        let mut row = vec![display_name, status, harness, model];
        if let Some(lookup) = profile_lookup {
            let prof = lookup(agent);
            row.push(if is_dim { dim(&prof) } else { prof });
        }
        row.push(mailbox);
        row.push(id);
        table.add_row(row);
    }
    table.print();
    println!();
}

/// Pretty-print an agent's inbox messages.
pub fn print_inbox_pretty(agent_name: &str, messages: &[MessageView]) {
    if messages.is_empty() {
        println!("  {} {}", bold_cyan(agent_name), dim("inbox is empty."));
        return;
    }
    println!(
        "\n  {}  {} messages\n",
        bold_cyan(&format!("Inbox · {agent_name}")),
        bold(&messages.len().to_string())
    );
    let mut table = Table::new(&[
        ("SEQ", true),
        ("STATUS", false),
        ("TIME", false),
        ("FROM", false),
        ("MESSAGE", false),
    ]);
    for m in messages {
        let seq = dim(&format!("#{}", m.seq));
        let ack = match m.ack.as_str() {
            "delivered" => green("✓ delivered"),
            "received" => yellow("● queued"),
            other => dim(other),
        };
        let time = dim(&format_timestamp_ms(m.received_at_ms));
        let sender = cyan(m.sender.as_deref().unwrap_or("-"));
        let preview = m.text.replace('\n', " ↵ ");
        table.add_row(vec![seq, ack, time, sender, preview]);
    }
    table.print();
    println!();
}

/// Pretty-print two-way conversation turns (`conversation` and `search`).
pub fn print_conversation_pretty(target: &str, query: Option<&str>, turns: &[ConversationTurnView]) {
    if turns.is_empty() {
        println!("  {}", dim("No conversation turns matched."));
        return;
    }
    let header = match query {
        Some(q) => format!("Search \"{q}\" · {target} ({} turns)", turns.len()),
        None => format!("Conversation · {target} ({} turns)", turns.len()),
    };
    println!("\n  {}\n", bold_cyan(&header));
    for turn in turns {
        let ts = dim(&format_timestamp_ms(turn.timestamp_ms));
        let dir = if turn.direction == "out" {
            bold_green("→ out")
        } else {
            cyan("← in ")
        };
        let route = format!("{} {} {}", bold(&turn.from), dim("→"), bold(&turn.to));
        let id = dim(&format!("[{}]", turn.id));
        println!("  {ts}  {dir}  {route}  {id}");
        for line in turn.text.lines() {
            println!("      {line}");
        }
        println!();
    }
}
