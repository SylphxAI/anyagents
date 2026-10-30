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

/// Display column width of a single Unicode scalar value in a terminal.
pub fn char_display_width(ch: char) -> usize {
    let u = ch as u32;
    if u < 0x20 || (0x7f..=0x9f).contains(&u) {
        return 0;
    }
    // Zero-width space, joiners, variation selectors, and combining marks
    if matches!(
        u,
        0x0300..=0x036f
            | 0x1ab0..=0x1aff
            | 0x1dc0..=0x1dff
            | 0x200b..=0x200f
            | 0x20d0..=0x20ff
            | 0xfe00..=0xfe0f
            | 0xfe20..=0xfe2f
            | 0xfeff
            | 0xe0100..=0xe01ef
    ) {
        return 0;
    }
    // East Asian Wide (W), Fullwidth (F), CJK Unified Ideographs, Hangul, and Emoji blocks
    if matches!(
        u,
        0x1100..=0x115f
            | 0x2329..=0x232a
            | 0x2e80..=0x303e
            | 0x3040..=0x33ff
            | 0x3400..=0x4dbf
            | 0x4e00..=0xa4cf
            | 0xa960..=0xa97f
            | 0xac00..=0xd7a3
            | 0xf900..=0xfaff
            | 0xfe10..=0xfe19
            | 0xfe30..=0xfe6f
            | 0xff01..=0xff60
            | 0xffe0..=0xffe6
            | 0x1f000..=0x1fbff
            | 0x20000..=0x3fffd
    ) {
        return 2;
    }
    1
}

/// Visible terminal column width of a string, ignoring ANSI CSI sequences and accounting for CJK double-width characters.
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
        width += char_display_width(ch);
    }
    width
}

/// Truncate a potentially ANSI-styled string to fit within `max_cols` display columns, appending `…` when truncated.
pub fn truncate_visible(s: &str, max_cols: usize) -> String {
    if max_cols == 0 {
        return String::new();
    }
    if visible_width(s) <= max_cols {
        return s.to_string();
    }
    let limit = max_cols.saturating_sub(1);
    let mut out = String::new();
    let mut width = 0;
    let mut saw_ansi = false;
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' {
            saw_ansi = true;
            out.push(ch);
            if chars.peek() == Some(&'[') {
                if let Some(bracket) = chars.next() {
                    out.push(bracket);
                }
                for c in chars.by_ref() {
                    out.push(c);
                    if ('@'..='~').contains(&c) {
                        break;
                    }
                }
            }
            continue;
        }
        let cw = char_display_width(ch);
        if width + cw > limit {
            break;
        }
        out.push(ch);
        width += cw;
    }
    out.push('…');
    if saw_ansi {
        out.push_str("\x1b[0m");
    }
    out
}

/// Detect the active terminal width in columns (falls back to 120 when not attached to a tty).
pub fn terminal_width() -> usize {
    if let Ok(val) = std::env::var("COLUMNS")
        && let Ok(cols) = val.trim().parse::<usize>()
        && cols >= 40
    {
        return cols;
    }
    #[cfg(unix)]
    {
        for fd in [1, 2, 0] {
            unsafe {
                let mut ws: libc::winsize = std::mem::zeroed();
                if libc::ioctl(fd, libc::TIOCGWINSZ, &mut ws) == 0 && ws.ws_col >= 40 {
                    return ws.ws_col as usize;
                }
            }
        }
    }
    120
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

/// Format an agent's combined `(state, process, turn_state, quota_reset_at_ms, block_reason)` as a semantic status badge.
pub fn format_agent_status(
    state: &str,
    process: &str,
    turn_state: &str,
    quota_reset_at_ms: Option<i64>,
    block_reason: Option<&str>,
) -> String {
    let now = anyagents_core::now_ms();
    let on_quota = quota_reset_at_ms.is_some_and(|ms| ms > now && ms - now < 86_400_000 * 7);
    let is_blocked = block_reason.is_some_and(|r| !r.trim().is_empty()) && !on_quota;
    match (state, process, turn_state) {
        ("running", _, _) if on_quota => yellow("○ quota"),
        ("running", _, _) if is_blocked => red("○ blocked"),
        ("running", "alive", "working") => bold_green("● working"),
        ("running", "alive", _) => green("○ idle"),
        ("running", "down", _) | ("running", "offline", _) | ("running", "-", _) => {
            yellow("○ starting")
        }
        ("running", other, _) => cyan(&format!("● {other}")),
        ("idle", _, _) => dim("◌ idle"),
        ("paused", _, _) => yellow("◐ paused"),
        ("archived", _, _) => dim("▫ archived"),
        (other, _, _) => dim(other),
    }
}

/// A self-sizing terminal table with left/right column alignment and automatic terminal-width fitting.
pub struct Table {
    headers: Vec<(&'static str, bool)>,
    rows: Vec<Vec<String>>,
    indent: &'static str,
    fit_terminal: bool,
}

impl Table {
    /// `headers`: slice of `(title, right_align)`.
    pub fn new(headers: &[(&'static str, bool)]) -> Self {
        Self {
            headers: headers.to_vec(),
            rows: Vec::new(),
            indent: "  ",
            fit_terminal: true,
        }
    }

    pub fn with_fit_terminal(mut self, fit: bool) -> Self {
        self.fit_terminal = fit;
        self
    }

    pub fn add_row(&mut self, row: Vec<String>) {
        self.rows.push(row);
    }

    pub fn print(&self) {
        if self.headers.is_empty() {
            return;
        }
        let cols = self.headers.len();
        let mut widths: Vec<usize> = self.headers.iter().map(|(h, _)| visible_width(h)).collect();
        for row in &self.rows {
            for (i, cell) in row.iter().enumerate().take(cols) {
                let w = visible_width(cell);
                if w > widths[i] {
                    widths[i] = w;
                }
            }
        }

        let max_last_col = if self.fit_terminal && cols > 1 && !self.headers[cols - 1].1 {
            let prefix_cols: usize = visible_width(self.indent)
                + widths[..cols - 1].iter().sum::<usize>()
                + 2 * (cols - 1);
            Some(terminal_width().saturating_sub(prefix_cols).max(16))
        } else {
            None
        };

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
                    if let Some(max_w) = max_last_col {
                        line.push_str(&truncate_visible(cell, max_w));
                    } else {
                        line.push_str(cell);
                    }
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
        ("idle", _, _) => 2,
        ("paused", _, _) => 3,
        ("archived", _, _) => 4,
        _ => 5,
    }
}

/// Options controlling how the agent hierarchy table is rendered.
#[derive(Clone, Copy, Debug)]
pub struct AgentsTableOptions {
    pub include_archived: bool,
    /// Maximum sub-agent tree depth to expand (`Some(0)` = roots only with subtree summary;
    /// `Some(n)` = expand up to `n` levels deep; `None` = expand all levels).
    pub max_depth: Option<usize>,
    /// Only include root trees that have at least one visible sub-agent (`anyagents subagents`).
    pub only_with_subagents: bool,
    /// Show full columns including `ID` without terminal-width truncation (`--wide` / `-w`).
    pub wide: bool,
}

impl Default for AgentsTableOptions {
    fn default() -> Self {
        Self {
            include_archived: false,
            max_depth: Some(0),
            only_with_subagents: false,
            wide: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct SubtreeStats {
    total: usize,
    max_depth: usize,
    working: usize,
    ready_or_starting: usize,
    scaled_idle: usize,
    paused: usize,
    archived: usize,
    lagging: usize,
}

fn compute_subtree_stats<'a>(
    node_id: &'a str,
    children_by_parent: &HashMap<&'a str, Vec<&'a AgentView>>,
    now: i64,
    memo: &mut HashMap<&'a str, SubtreeStats>,
) -> SubtreeStats {
    if let Some(&cached) = memo.get(node_id) {
        return cached;
    }
    let mut stats = SubtreeStats::default();
    if let Some(kids) = children_by_parent.get(node_id) {
        for &kid in kids {
            let child_stats = compute_subtree_stats(&kid.id, children_by_parent, now, memo);
            stats.total += 1 + child_stats.total;
            stats.max_depth = stats.max_depth.max(1 + child_stats.max_depth);
            match (
                kid.state.as_str(),
                kid.process.as_str(),
                kid.turn_state.as_str(),
            ) {
                ("running", "alive", "working") => stats.working += 1,
                ("running", _, _) => stats.ready_or_starting += 1,
                ("idle", _, _) => stats.scaled_idle += 1,
                ("paused", _, _) => stats.paused += 1,
                ("archived", _, _) => stats.archived += 1,
                _ => stats.scaled_idle += 1,
            }
            let on_quota = kid
                .quota_reset_at_ms
                .is_some_and(|ms| ms > now && ms - now < 86_400_000 * 7);
            let is_blocked = kid
                .block_reason
                .as_deref()
                .is_some_and(|r| !r.trim().is_empty());
            let has_lag = kid.oldest_undelivered_age_ms.is_some_and(|ms| ms >= 120_000);
            if kid.state != "archived" && (on_quota || is_blocked || has_lag) {
                stats.lagging += 1;
            }
            stats.working += child_stats.working;
            stats.ready_or_starting += child_stats.ready_or_starting;
            stats.scaled_idle += child_stats.scaled_idle;
            stats.paused += child_stats.paused;
            stats.archived += child_stats.archived;
            stats.lagging += child_stats.lagging;
        }
    }
    memo.insert(node_id, stats);
    stats
}

fn format_subtree_breakdown(stats: &SubtreeStats) -> String {
    let mut parts = Vec::new();
    if stats.working > 0 {
        parts.push(bold_green(&format!("{}●", stats.working)));
    }
    if stats.ready_or_starting > 0 {
        parts.push(yellow(&format!("{}○", stats.ready_or_starting)));
    }
    if stats.scaled_idle > 0 {
        parts.push(dim(&format!("{}◌", stats.scaled_idle)));
    }
    if stats.paused > 0 {
        parts.push(yellow(&format!("{}◐", stats.paused)));
    }
    if stats.archived > 0 {
        parts.push(dim(&format!("{}▫", stats.archived)));
    }
    if stats.lagging > 0 {
        parts.push(red(&format!("!{}", stats.lagging)));
    }
    parts.join(" ")
}

/// Strip redundant provider/gateway prefixes from model IDs for compact display.
pub fn short_model_name(raw: &str) -> &str {
    let s = raw.trim();
    for prefix in [
        "sylphx/openai/",
        "sylphx/anthropic/",
        "sylphx/google/",
        "sylphx/xai/",
        "sylphx/",
        "openai/",
        "anthropic/",
        "google/",
        "xai/",
    ] {
        if let Some(rest) = s.strip_prefix(prefix) {
            return rest;
        }
    }
    s
}

fn format_status_with_alerts(agent: &AgentView, now: i64) -> String {
    let on_quota = agent
        .quota_reset_at_ms
        .is_some_and(|ms| ms > now && ms - now < 86_400_000 * 7);
    if agent.state == "running"
        && on_quota
        && let Some(reset_ms) = agent.quota_reset_at_ms
    {
        return yellow(&format!("○ quota ({})", format_duration_ms(reset_ms - now)));
    }
    let base = format_agent_status(
        &agent.state,
        &agent.process,
        &agent.turn_state,
        agent.quota_reset_at_ms,
        agent.block_reason.as_deref(),
    );
    if agent.state != "archived"
        && agent.state != "paused"
        && let Some(ms) = agent.oldest_undelivered_age_ms
    {
        if ms >= 120_000 {
            return format!("{base} {}", red(&format!("! lag {}", format_duration_ms(ms))));
        }
        if ms >= 5_000 {
            return format!("{base} {}", yellow(&format!("~ {}", format_duration_ms(ms))));
        }
    }
    base
}

fn clean_agent_description(raw: &str, is_subagent: bool) -> String {
    let mut line = raw.lines().next().unwrap_or("").trim();
    if is_subagent {
        let lower = line.to_ascii_lowercase();
        if lower.starts_with("sub-agent of ") || lower.starts_with("subagent of ") {
            if let Some(idx) = lower.find(". outcome:")
                && idx < 48
            {
                line = line[idx + ". outcome:".len()..].trim();
            } else if let Some(idx) = line.find(". ")
                && idx < 36
            {
                line = line[idx + 2..].trim();
            }
        } else if lower.starts_with("anyagents child of ")
            && let Some(idx) = line.find(". ")
            && idx < 40
        {
            line = line[idx + 2..].trim();
        }
    }
    line.to_string()
}

/// Render the agent list using default options (concise root overview with subtree depth/status summary).
pub fn print_agents_table(
    agents: &[AgentView],
    profile_lookup: Option<&dyn Fn(&AgentView) -> String>,
    include_archived: bool,
) {
    print_agents_table_with_options(
        agents,
        profile_lookup,
        AgentsTableOptions {
            include_archived,
            ..AgentsTableOptions::default()
        },
    );
}

/// Render the agent list as a clean root overview or multi-level hierarchical tree (`L1 -> L2 -> L3+`).
pub fn print_agents_table_with_options(
    agents: &[AgentView],
    profile_lookup: Option<&dyn Fn(&AgentView) -> String>,
    options: AgentsTableOptions,
) {
    if agents.is_empty() {
        println!("  {}", dim("No agents found."));
        return;
    }

    let include_archived = options.include_archived;
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
    let idle = running.saturating_sub(working).saturating_sub(quota_count);
    let scaled_idle = agents.iter().filter(|a| a.state == "idle").count();
    let paused = agents.iter().filter(|a| a.state == "paused").count();
    let archived = agents.iter().filter(|a| a.state == "archived").count();

    let all_by_id: HashMap<&str, &AgentView> = agents.iter().map(|a| (a.id.as_str(), a)).collect();
    let mut visible_ids: std::collections::HashSet<&str> = agents
        .iter()
        .filter(|a| include_archived || a.state != "archived")
        .map(|a| a.id.as_str())
        .collect();

    // Ensure any ancestor (even if archived) of a visible sub-agent is included as a parent
    // container so active sub-agents are never orphaned as fake root rows.
    let initial_visible: Vec<&str> = visible_ids.iter().copied().collect();
    for id in initial_visible {
        let mut cur = all_by_id.get(id).and_then(|a| a.parent.as_deref());
        let mut guard = 0usize;
        while let Some(pid) = cur {
            if guard > 32 {
                break;
            }
            guard += 1;
            if let Some(&parent_agent) = all_by_id.get(pid) {
                visible_ids.insert(parent_agent.id.as_str());
                cur = parent_agent.parent.as_deref();
            } else {
                break;
            }
        }
    }

    let visible_agents: Vec<&AgentView> = agents
        .iter()
        .filter(|a| visible_ids.contains(a.id.as_str()))
        .collect();

    let mut children_by_parent: HashMap<&str, Vec<&AgentView>> = HashMap::new();
    let mut roots: Vec<&AgentView> = Vec::new();

    for &agent in &visible_agents {
        if let Some(parent_id) = agent.parent.as_deref()
            && visible_ids.contains(parent_id)
        {
            children_by_parent.entry(parent_id).or_default().push(agent);
        } else {
            roots.push(agent);
        }
    }

    let mut stats_memo: HashMap<&str, SubtreeStats> = HashMap::new();
    for &agent in &visible_agents {
        compute_subtree_stats(&agent.id, &children_by_parent, now, &mut stats_memo);
    }

    if options.only_with_subagents {
        roots.retain(|r| stats_memo.get(r.id.as_str()).is_some_and(|s| s.total > 0));
    }

    // Sort roots: active Core Leads (with role) first, then active standalone roots, then archived containers.
    roots.sort_by(|a, b| {
        let a_archived = u8::from(a.state == "archived");
        let b_archived = u8::from(b.state == "archived");
        let a_has_role = u8::from(
            a.role
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .is_none(),
        );
        let b_has_role = u8::from(
            b.role
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .is_none(),
        );
        a_archived
            .cmp(&b_archived)
            .then_with(|| a_has_role.cmp(&b_has_role))
            .then_with(|| {
                state_rank(&a.state, &a.process, &a.turn_state)
                    .cmp(&state_rank(&b.state, &b.process, &b.turn_state))
            })
            .then_with(|| a.name.cmp(&b.name))
    });
    for list in children_by_parent.values_mut() {
        list.sort_by(|a, b| {
            state_rank(&a.state, &a.process, &a.turn_state)
                .cmp(&state_rank(&b.state, &b.process, &b.turn_state))
                .then_with(|| a.name.cmp(&b.name))
        });
    }

    let total_subagents: usize = roots
        .iter()
        .filter_map(|r| stats_memo.get(r.id.as_str()).map(|s| s.total))
        .sum();
    let global_max_depth: usize = roots
        .iter()
        .filter_map(|r| stats_memo.get(r.id.as_str()).map(|s| s.max_depth))
        .max()
        .unwrap_or(0);

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
    if scaled_idle > 0 {
        summary.push(dim(&format!("{scaled_idle} idle (process released)")));
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
    println!("\n  {}  {}", bold_cyan("Agents"), summary.join(&dim(" · ")));
    if total_subagents > 0 {
        let view_hint = match options.max_depth {
            Some(0) => "use --tree (-t) or --depth <N> (-d) to expand sub-agents".to_string(),
            Some(d) if d < global_max_depth => {
                format!("showing depth ≤ {d} of {global_max_depth} · use --tree (-t) or -d {global_max_depth} for all")
            }
            _ => format!("showing full tree ({global_max_depth} levels deep)"),
        };
        println!(
            "  {}      {}",
            "  ",
            dim(&format!(
                "{} roots · {} sub-agents (up to {} deep) · {}",
                roots.len(),
                total_subagents,
                global_max_depth,
                view_hint
            ))
        );
    }
    println!();

    if roots.is_empty() {
        println!("  {}\n", dim("No matching agents."));
        return;
    }

    struct RenderedNode<'a> {
        display_name: String,
        agent: &'a AgentView,
        depth: usize,
        parent_runtime: Option<(&'a str, Option<&'a str>)>,
    }

    fn walk_tree<'a>(
        node: &'a AgentView,
        depth: usize,
        ancestors_have_more: &mut Vec<bool>,
        is_last: bool,
        parent_runtime: Option<(&'a str, Option<&'a str>)>,
        children_by_parent: &HashMap<&'a str, Vec<&'a AgentView>>,
        max_depth: Option<usize>,
        out: &mut Vec<RenderedNode<'a>>,
    ) {
        let display_name = if depth == 0 {
            let role_tag = node
                .role
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty());
            match (node.state.as_str(), role_tag) {
                ("running", Some(r)) => format!("{} {}", bold(&node.name), dim(&format!("· {r}"))),
                ("running", None) => bold(&node.name),
                ("paused", Some(r)) => format!("{} {}", node.name, dim(&format!("· {r}"))),
                ("paused", None) => node.name.clone(),
                (_, Some(r)) => dim(&format!("{} · {r}", node.name)),
                (_, None) => dim(&node.name),
            }
        } else {
            let mut prefix = String::new();
            for &has_more in &ancestors_have_more[..ancestors_have_more.len().saturating_sub(1)] {
                if has_more {
                    prefix.push_str("│  ");
                } else {
                    prefix.push_str("   ");
                }
            }
            if is_last {
                prefix.push_str("└─ ");
            } else {
                prefix.push_str("├─ ");
            }
            let kid_name = if node.state == "archived" || node.state == "idle" {
                dim(&node.name)
            } else {
                node.name.clone()
            };
            format!("{}{kid_name}", dim(&prefix))
        };

        out.push(RenderedNode {
            display_name,
            agent: node,
            depth,
            parent_runtime,
        });

        if max_depth.is_some_and(|m| depth >= m) {
            return;
        }
        if let Some(kids) = children_by_parent.get(node.id.as_str()) {
            let count = kids.len();
            let my_runtime = Some((node.harness.as_str(), node.model.as_deref()));
            for (idx, &kid) in kids.iter().enumerate() {
                let child_is_last = idx + 1 == count;
                ancestors_have_more.push(!child_is_last);
                walk_tree(
                    kid,
                    depth + 1,
                    ancestors_have_more,
                    child_is_last,
                    my_runtime,
                    children_by_parent,
                    max_depth,
                    out,
                );
                ancestors_have_more.pop();
            }
        }
    }

    let mut ordered: Vec<RenderedNode<'_>> = Vec::with_capacity(visible_agents.len());
    let mut ancestors_have_more = Vec::new();
    for root in roots {
        walk_tree(
            root,
            0,
            &mut ancestors_have_more,
            true,
            None,
            &children_by_parent,
            options.max_depth,
            &mut ordered,
        );
    }

    let has_descriptions = ordered.iter().any(|n| {
        n.agent
            .description
            .as_deref()
            .is_some_and(|d| !d.trim().is_empty())
    });

    let mut headers = vec![
        ("AGENT", false),
        ("STATUS", false),
        ("TREE", false),
        ("RUNTIME", false),
    ];
    if profile_lookup.is_some() {
        headers.push(("PROFILE", false));
    }
    if has_descriptions {
        headers.push(("DESCRIPTION", false));
    }
    if options.wide {
        headers.push(("ID", false));
    }

    let mut table = Table::new(&headers).with_fit_terminal(!options.wide);
    for node in ordered {
        let agent = node.agent;
        let is_dim = agent.state == "archived" || agent.state == "idle";
        let status = format_status_with_alerts(agent, now);

        let stats = stats_memo
            .get(agent.id.as_str())
            .copied()
            .unwrap_or_default();
        let tree_cell = if node.depth == 0 {
            if stats.total == 0 {
                dim("-")
            } else {
                format!(
                    "{} {} ({})",
                    bold(&format!("{} sub", stats.total)),
                    dim(&format!("· {} deep", stats.max_depth)),
                    format_subtree_breakdown(&stats)
                )
            }
        } else {
            let lvl = dim(&format!("L{}", node.depth));
            if stats.total == 0 {
                lvl
            } else if options.max_depth.is_some_and(|m| node.depth >= m) {
                format!(
                    "{lvl} {} ({})",
                    cyan(&format!("+{} sub · {} deep", stats.total, stats.max_depth)),
                    format_subtree_breakdown(&stats)
                )
            } else {
                format!(
                    "{lvl} {}",
                    dim(&format!("· {} sub ({} deep)", stats.total, stats.max_depth))
                )
            }
        };

        let model_raw = agent.model.as_deref().unwrap_or("-");
        let mut short_model = short_model_name(model_raw);
        if agent.harness == "claude-code"
            && let Some(stripped) = short_model.strip_prefix("claude-")
        {
            short_model = stripped;
        }
        let same_as_parent = node.parent_runtime.is_some_and(|(ph, pm)| {
            ph == agent.harness.as_str() && pm == agent.model.as_deref()
        });
        let runtime = if is_dim || (node.depth > 0 && same_as_parent) {
            dim(&format!("{} · {short_model}", agent.harness))
        } else {
            let styled_model = if let Some((base, effort)) = short_model.split_once('#') {
                format!("{base}{}", magenta(&format!("#{effort}")))
            } else {
                short_model.to_string()
            };
            format!("{}{}{styled_model}", cyan(&agent.harness), dim(" · "))
        };

        let mut row = vec![node.display_name, status, tree_cell, runtime];
        if let Some(lookup) = profile_lookup {
            let prof = lookup(agent);
            row.push(if is_dim { dim(&prof) } else { prof });
        }
        if has_descriptions {
            let cleaned = agent
                .description
                .as_deref()
                .map(|s| clean_agent_description(s, node.depth > 0))
                .filter(|s| !s.is_empty());
            let desc_text = cleaned.as_deref().unwrap_or("-");
            row.push(if is_dim || desc_text == "-" {
                dim(desc_text)
            } else {
                desc_text.to_string()
            });
        }
        if options.wide {
            row.push(dim(&agent.id));
        }
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
pub fn print_conversation_pretty(
    target: &str,
    query: Option<&str>,
    turns: &[ConversationTurnView],
) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn visible_width_counts_cjk_as_double_width_and_ignores_ansi() {
        assert_eq!(visible_width("helios"), 6);
        assert_eq!(visible_width("實作"), 4);
        assert_eq!(visible_width("helios · 實作 · Building"), 24);
        assert_eq!(visible_width("\x1b[1mhelios\x1b[0m \x1b[2m· 實作\x1b[0m"), 13);
    }

    #[test]
    fn truncate_visible_respects_cjk_boundaries_and_resets_ansi() {
        let s = "香港麻雀大亨Owner | category: 現有遊戲";
        let t = truncate_visible(s, 14);
        // "香港麻雀大亨" is 12 cols + "O" (1 col) + "…" (1 col) = 14 cols
        assert_eq!(visible_width(&t), 14);
        assert!(t.ends_with('…'));

        let styled = "\x1b[2m香港麻雀大亨Owner\x1b[0m";
        let ts = truncate_visible(styled, 9);
        assert!(visible_width(&ts) <= 9);
        assert!(ts.ends_with("…\x1b[0m"));
    }

    #[test]
    fn short_model_strips_provider_prefixes() {
        assert_eq!(
            short_model_name("sylphx/openai/gpt-6.1-sol#xhigh"),
            "gpt-6.1-sol#xhigh"
        );
        assert_eq!(
            short_model_name("anthropic/claude-opus-5-5#xhigh"),
            "claude-opus-5-5#xhigh"
        );
    }

    #[test]
    fn clean_description_strips_subagent_boilerplate() {
        assert_eq!(
            clean_agent_description(
                "Sub-agent of Sage. Outcome: no live Fun, Big2 or HKMT player loses data",
                true
            ),
            "no live Fun, Big2 or HKMT player loses data"
        );
    }
}
