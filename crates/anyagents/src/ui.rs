//! Human-first terminal presentation for the `anyagents` CLI.
//!
//! When standard output is an interactive terminal (or `ANYAGENTS_PRETTY=1` is
//! set), commands render structured tables, hierarchical agent trees, semantic
//! status badges, and human-readable durations/timestamps. When piped or run in
//! automated harnesses (`!stdout.is_terminal()`), commands emit stable TSV.

use std::collections::HashMap;
use std::io::IsTerminal as _;

use crate::api::AgentView;

mod presentation;
pub use presentation::{error, heading, is_verbose, note, schedules, set_verbose, success, value};

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

/// Detect the active terminal width in columns (falls back to 120 without a tty).
pub fn terminal_width() -> usize {
    if let Ok(val) = std::env::var("COLUMNS")
        && let Ok(cols) = val.trim().parse::<usize>()
        && cols >= 20
    {
        return cols;
    }
    #[cfg(unix)]
    {
        for fd in [1, 2, 0] {
            unsafe {
                let mut ws: libc::winsize = std::mem::zeroed();
                if libc::ioctl(fd, libc::TIOCGWINSZ, &mut ws) == 0 && ws.ws_col >= 20 {
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
    format!("{year:04}-{m:02}-{d:02} {hour:02}:{min:02}:{sec:02}Z")
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
        ("paused", _, "faulted") => red("✗ faulted"),
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
    preserve_cells: bool,
}

impl Table {
    /// `headers`: slice of `(title, right_align)`.
    pub fn new(headers: &[(&'static str, bool)]) -> Self {
        Self {
            headers: headers.to_vec(),
            rows: Vec::new(),
            indent: "  ",
            fit_terminal: true,
            preserve_cells: false,
        }
    }

    pub fn with_fit_terminal(mut self, fit: bool) -> Self {
        self.fit_terminal = fit;
        self
    }

    /// Fall back to labeled rows rather than chopping identities or status.
    fn preserve_cells(mut self) -> Self {
        self.preserve_cells = true;
        self
    }

    pub fn add_row(&mut self, row: Vec<String>) {
        self.rows.push(row);
    }

    pub fn render_lines(&self) -> Vec<String> {
        if self.headers.is_empty() {
            return Vec::new();
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

        let term_w = terminal_width();
        let available = term_w.saturating_sub(visible_width(self.indent));
        let natural = widths.iter().sum::<usize>() + 2 * cols.saturating_sub(1);
        if self.fit_terminal && natural > available {
            let minimum = self
                .headers
                .iter()
                .map(|(h, _)| visible_width(h).max(6))
                .sum::<usize>()
                + 2 * cols.saturating_sub(1);
            if minimum > available || self.preserve_cells {
                let mut lines = Vec::new();
                for (n, row) in self.rows.iter().enumerate() {
                    if n > 0 {
                        lines.push(String::new());
                    }
                    for (i, (title, _)) in self.headers.iter().enumerate() {
                        let cell = row.get(i).map(String::as_str).unwrap_or("—");
                        let label = format!("{}{}  ", self.indent, dim(title));
                        let space = term_w.saturating_sub(visible_width(&label));
                        if self.preserve_cells {
                            lines.push(format!("{label}{cell}"));
                        } else {
                            lines.push(format!("{label}{}", truncate_visible(cell, space)));
                        }
                    }
                }
                return lines;
            }
            while widths.iter().sum::<usize>() + 2 * cols.saturating_sub(1) > available {
                let Some(i) = (0..cols)
                    .filter(|&i| widths[i] > visible_width(self.headers[i].0).max(6))
                    .max_by_key(|&i| widths[i])
                else {
                    break;
                };
                widths[i] -= 1;
            }
        }
        let max_last_col = if self.fit_terminal && cols > 1 && !self.headers[cols - 1].1 {
            let prefix_cols: usize = visible_width(self.indent)
                + widths[..cols - 1].iter().sum::<usize>()
                + 2 * (cols - 1);
            Some(term_w.saturating_sub(prefix_cols).max(16))
        } else {
            None
        };

        let mut out = Vec::with_capacity(1 + self.rows.len());

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
        if self.fit_terminal && visible_width(&header_line) > term_w {
            header_line = truncate_visible(&header_line, term_w);
        }
        out.push(header_line);

        // Data rows
        for row in &self.rows {
            let mut line = String::from(self.indent);
            for (i, (_, right)) in self.headers.iter().enumerate() {
                let cell = row.get(i).map(String::as_str).unwrap_or("");
                let fitted = if self.fit_terminal {
                    truncate_visible(cell, widths[i])
                } else {
                    cell.to_string()
                };
                let cell = fitted.as_str();
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
            if self.fit_terminal && visible_width(&line) > term_w {
                line = truncate_visible(&line, term_w);
            }
            out.push(line);
        }
        out
    }

    pub fn print(&self) {
        for line in self.render_lines() {
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
    pub title: &'static str,
    pub include_archived: bool,
    pub archived_count: Option<usize>,
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
            title: "Agents",
            include_archived: false,
            archived_count: None,
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
            let on_quota = is_quota_held(kid, now);
            match (
                kid.state.as_str(),
                kid.process.as_str(),
                kid.turn_state.as_str(),
            ) {
                ("running", "alive", "working") if is_healthy_active_turn(kid, on_quota) => {
                    stats.working += 1
                }
                ("running", _, _) => stats.ready_or_starting += 1,
                ("idle", _, _) => stats.scaled_idle += 1,
                ("paused", _, _) => stats.paused += 1,
                ("archived", _, _) => stats.archived += 1,
                _ => stats.scaled_idle += 1,
            }
            let is_blocked = kid
                .block_reason
                .as_deref()
                .is_some_and(|r| !r.trim().is_empty());
            let has_lag = !is_expected_turn_queue(kid, on_quota)
                && kid
                    .oldest_undelivered_age_ms
                    .is_some_and(|ms| ms >= 120_000);
            if kid.state != "archived"
                && kid.state != "paused"
                && (on_quota || is_blocked || has_lag)
            {
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

/// Compact model IDs for display:
/// - `<gateway>/<vendor>/<model>` -> `<gateway>/<model>` (preserves gateway routing identity without hardcoding gateway names)
/// - `<vendor>/<model>` -> `<model>` for standard model vendors
pub fn short_model_name(raw: &str) -> std::borrow::Cow<'_, str> {
    let s = raw.trim();
    if let Some((first, rest)) = s.split_once('/') {
        if let Some((_vendor, leaf)) = rest.split_once('/')
            && !first.is_empty()
            && !leaf.is_empty()
        {
            return std::borrow::Cow::Owned(format!("{first}/{leaf}"));
        }
        if matches!(
            first,
            "openai" | "anthropic" | "google" | "xai" | "deepseek" | "meta"
        ) && !rest.is_empty()
        {
            return std::borrow::Cow::Borrowed(rest);
        }
    }
    std::borrow::Cow::Borrowed(s)
}

fn is_healthy_active_turn(agent: &AgentView, on_quota: bool) -> bool {
    agent.state == "running"
        && agent.process == "alive"
        && agent.turn_state == "working"
        && !on_quota
        && agent
            .block_reason
            .as_deref()
            .is_none_or(|r| r.trim().is_empty())
        && agent
            .last_error
            .as_deref()
            .is_none_or(|e| e.trim().is_empty())
}

/// Mail that waits for a healthy turn by design: a `turn-end` harness takes
/// it when the turn ends, and a pending switch holds it for the replacement.
/// A `mid-turn` or `next-step` harness takes mail into the running turn, so
/// its mail waiting is lag, shown as lag: a stuck mailbox must not read as a
/// queue.
fn is_expected_turn_queue(agent: &AgentView, on_quota: bool) -> bool {
    is_healthy_active_turn(agent, on_quota)
        && (agent.delivery.level == "turn-end" || agent.pending_switch)
}

fn quota_remaining_ms(agent: &AgentView, now: i64) -> Option<i64> {
    agent
        .quota_reset_at_ms
        .and_then(|reset| reset.checked_sub(now))
        .filter(|ms| agent.state == "running" && (1..86_400_000 * 7).contains(ms))
}

fn is_quota_held(agent: &AgentView, now: i64) -> bool {
    quota_remaining_ms(agent, now).is_some()
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AgentAttention {
    Faulted,
    Paused,
    Quota(i64),
    SignIn,
    Error,
}

pub(crate) fn agent_attention(agent: &AgentView, now: i64) -> Option<AgentAttention> {
    match agent.display_state() {
        "faulted" => Some(AgentAttention::Faulted),
        "paused" => Some(AgentAttention::Paused),
        "running" | "idle" => {
            if let Some(ms) = quota_remaining_ms(agent, now) {
                Some(AgentAttention::Quota(ms))
            } else if agent.block_reason.as_deref() == Some("sign_in_required") {
                Some(AgentAttention::SignIn)
            } else if agent
                .last_error
                .as_deref()
                .is_some_and(|error| !error.trim().is_empty())
            {
                Some(AgentAttention::Error)
            } else {
                None
            }
        }
        _ => None,
    }
}

pub(crate) fn format_status_with_alerts(agent: &AgentView, now: i64) -> String {
    let mut base = match agent_attention(agent, now) {
        Some(AgentAttention::Quota(ms)) => {
            return yellow(&format!("◐ quota ↻{}", format_duration_ms(ms)));
        }
        Some(AgentAttention::Faulted) => red("✕ faulted"),
        Some(AgentAttention::Paused) => yellow("◐ paused"),
        Some(AgentAttention::SignIn) => red("✕ sign-in required"),
        Some(AgentAttention::Error) => red("✕ error"),
        None => format_agent_status(
            &agent.state,
            &agent.process,
            &agent.turn_state,
            None,
            agent.block_reason.as_deref(),
        ),
    };
    if is_healthy_active_turn(agent, false)
        && let Some(turn_ms) = agent.open_turn_age_ms.filter(|&ms| ms >= 5_000)
    {
        base = format!("{base} {}", dim(&format_duration_ms(turn_ms)));
    }
    if agent.state != "archived"
        && agent.state != "paused"
        && let Some(ms) = agent.oldest_undelivered_age_ms
    {
        if is_expected_turn_queue(agent, false) {
            if ms >= 5_000 {
                return format!(
                    "{base} {}",
                    dim(&format!("queued {}", format_duration_ms(ms)))
                );
            }
            return base;
        }
        if ms >= 120_000 {
            return format!(
                "{base} {}",
                red(&format!("! lag {}", format_duration_ms(ms)))
            );
        }
        if ms >= 5_000 {
            return format!(
                "{base} {}",
                yellow(&format!("~ {}", format_duration_ms(ms)))
            );
        }
    }
    base
}

fn clean_agent_description(
    raw: &str,
    agent_name: &str,
    role: Option<&str>,
    is_subagent: bool,
) -> String {
    let mut line = raw
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("");
    line = line.trim_start_matches('#').trim();

    // Strip redundant `<Name> · <Role>` prefix when it repeats the AGENT column
    let name_lower = agent_name.to_ascii_lowercase();
    if line
        .get(..agent_name.len())
        .is_some_and(|p| p.eq_ignore_ascii_case(&name_lower))
    {
        let after_name = line[agent_name.len()..].trim_start();
        if let Some(after_dot) = after_name
            .strip_prefix('·')
            .or_else(|| after_name.strip_prefix('-'))
            .or_else(|| after_name.strip_prefix(':'))
        {
            let mut rest = after_dot.trim_start();
            if let Some(r) = role.map(str::trim).filter(|s| !s.is_empty())
                && let Some(after_role) = rest.strip_prefix(r)
            {
                rest = after_role.trim_start();
            }
            if rest.starts_with('(')
                && let Some(close_idx) = rest.find(')')
            {
                let inside = rest[1..close_idx].trim();
                let after_paren = rest[close_idx + 1..]
                    .trim_start_matches(['.', ':', '-', '—'])
                    .trim();
                if !after_paren.is_empty() {
                    line = after_paren;
                } else if !inside.is_empty() {
                    line = inside;
                }
            } else if !rest.is_empty() {
                line = rest;
            }
        }
    }

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
        } else if (lower.contains(" sub-agent:") || lower.contains("'s sub-agent:"))
            && let Some(idx) = line.find(':')
            && idx < 48
        {
            line = line[idx + 1..].trim();
        }
    }

    // Strip leading "Goal:" / "Goal (...):" / "Sole writer:" / "Sole writer for" boilerplate
    let lower = line.to_ascii_lowercase();
    if let Some(rest) = lower.strip_prefix("goal") {
        let after_goal = line[4..].trim_start();
        if let Some(after_colon) = after_goal.strip_prefix(':') {
            line = after_colon.trim();
        } else if (rest.starts_with(' ') || rest.starts_with('('))
            && let Some(colon_idx) = after_goal.find("):")
        {
            let candidate = after_goal[colon_idx + 2..].trim();
            if !candidate.is_empty() {
                line = candidate;
            }
        }
    } else {
        for prefix in [
            "sole writer for ",
            "sole writer: ",
            "one writer: ",
            "one writer for ",
        ] {
            if lower.starts_with(prefix) {
                line = line[prefix.len()..].trim();
                break;
            }
        }
    }

    line.to_string()
}

/// Render the agent list using default options (concise root overview with subtree depth/status summary).
pub fn print_agents_table_with_options(
    agents: &[AgentView],
    profile_lookup: Option<&dyn Fn(&AgentView) -> String>,
    options: AgentsTableOptions,
) {
    if agents.is_empty() {
        println!(
            "\n  {}\n  {}",
            bold_cyan(options.title),
            dim("No agents found.")
        );
        return;
    }

    let include_archived = options.include_archived;
    let now = anyagents_core::now_ms();
    let archived = options
        .archived_count
        .unwrap_or_else(|| agents.iter().filter(|a| a.state == "archived").count());

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

    // Sort by lifecycle and name, never by role labels.
    roots.sort_by(|a, b| {
        let a_archived = u8::from(a.state == "archived");
        let b_archived = u8::from(b.state == "archived");
        a_archived
            .cmp(&b_archived)
            .then_with(|| {
                state_rank(&a.state, &a.process, &a.turn_state).cmp(&state_rank(
                    &b.state,
                    &b.process,
                    &b.turn_state,
                ))
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

    let working = visible_agents
        .iter()
        .filter(|a| is_healthy_active_turn(a, is_quota_held(a, now)))
        .count();
    let quota = visible_agents
        .iter()
        .filter(|a| is_quota_held(a, now))
        .count();
    let paused = visible_agents
        .iter()
        .filter(|a| a.display_state() == "paused")
        .count();
    let faulted = visible_agents
        .iter()
        .filter(|a| a.display_state() == "faulted")
        .count();
    let mut counts = Vec::new();
    for (count, label) in [
        (working, "working"),
        (quota, "quota-held"),
        (paused, "paused"),
        (faulted, "faulted"),
    ] {
        if count > 0 {
            counts.push(format!("{count} {label}"));
        }
    }
    let detail = if counts.is_empty() {
        String::new()
    } else {
        format!(" ({})", counts.join(", "))
    };
    let archive_note = if archived > 0 && (include_archived || options.wide) {
        format!(
            " · {archived} archived{}",
            if include_archived {
                ""
            } else {
                " hidden (--state all)"
            }
        )
    } else {
        String::new()
    };
    println!(
        "\n  {}  {} root agents · {total_subagents} sub-agents{detail}{archive_note}",
        bold_cyan(options.title),
        roots.len()
    );
    if options.title == "Agents"
        && options
            .max_depth
            .is_some_and(|depth| depth < global_max_depth)
    {
        println!(
            "  {}",
            dim("Use --tree or --depth <N> to expand sub-agents.")
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

    #[allow(clippy::too_many_arguments)]
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
            match node.state.as_str() {
                "running" => bold(&node.name),
                "paused" => node.name.clone(),
                _ => dim(&node.name),
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
            let kid_name = match node.state.as_str() {
                "archived" | "idle" => dim(&node.name),
                "running" => bold(&node.name),
                _ => node.name.clone(),
            };
            format!("{}{kid_name}", dim(&prefix))
        };

        out.push(RenderedNode {
            display_name,
            agent: node,
            depth,
            parent_runtime,
        });

        let stop_here = max_depth.is_some_and(|m| depth >= m);
        if let Some(kids) = children_by_parent.get(node.id.as_str()) {
            let visible_kids: Vec<&'a AgentView> =
                if stop_here { Vec::new() } else { kids.clone() };
            let count = visible_kids.len();
            let my_runtime = Some((node.harness.as_str(), node.model.as_deref()));
            for (idx, &kid) in visible_kids.iter().enumerate() {
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

    let mut headers = vec![
        ("AGENT", false),
        ("STATUS", false),
        (if options.wide { "RUNTIME" } else { "MODEL" }, false),
        ("SUBAGENTS", false),
        ("ROLE", false),
    ];
    if options.wide {
        headers.push(("AUTOMATIONS", false));
        if profile_lookup.is_some() {
            headers.push(("PROFILE", false));
        }
        headers.extend([("DESCRIPTION", false), ("ID", false)]);
    }

    let mut table = Table::new(&headers)
        .with_fit_terminal(!options.wide)
        .preserve_cells();
    for node in ordered {
        let agent = node.agent;
        let is_dim = agent.state == "archived" || agent.state == "idle";
        let status = format_status_with_alerts(agent, now);

        let stats = stats_memo
            .get(agent.id.as_str())
            .copied()
            .unwrap_or_default();
        let tree_cell = if !options.wide {
            match (stats.total, stats.working) {
                (0, _) => dim("—"),
                (total, 0) => total.to_string(),
                (total, working) => format!("{total} ({working} working)"),
            }
        } else if node.depth == 0 {
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
        let mut short_model = if options.wide {
            std::borrow::Cow::Borrowed(model_raw)
        } else {
            short_model_name(
                model_raw
                    .strip_prefix("sylphx/openai/")
                    .unwrap_or(model_raw),
            )
        };
        if !options.wide
            && agent.harness == "agy"
            && let Some(stripped) = short_model.strip_prefix("gemini-4-")
        {
            short_model = std::borrow::Cow::Owned(stripped.to_string());
        }
        if !options.wide
            && agent.harness == "claude-code"
            && let Some(stripped) = short_model.strip_prefix("claude-")
        {
            short_model = std::borrow::Cow::Owned(stripped.to_string());
        }
        let same_as_parent = node
            .parent_runtime
            .is_some_and(|(ph, pm)| ph == agent.harness.as_str() && pm == agent.model.as_deref());
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

        let role_cell = match agent
            .role
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            Some(r) if is_dim => dim(r),
            Some(r) => cyan(r),
            None => dim("-"),
        };

        let mut row = vec![node.display_name, status, runtime, tree_cell, role_cell];
        if options.wide {
            row.push(automation_summary(&agent.automations));
            if let Some(lookup) = profile_lookup {
                row.push(lookup(agent));
            }
            let cleaned = agent
                .description
                .as_deref()
                .map(|s| {
                    clean_agent_description(s, &agent.name, agent.role.as_deref(), node.depth > 0)
                })
                .filter(|s| !s.is_empty());
            let desc_text = cleaned.as_deref().unwrap_or("-");
            row.push(if is_dim || desc_text == "-" {
                dim(desc_text)
            } else {
                desc_text.to_string()
            });
            row.push(dim(&agent.id));
        }
        table.add_row(row);
    }
    table.print();
    println!();
}

/// Style the suffix after `<pct>% left` so reset countdowns (`↻40m`, `↻8h01m`) pop out
/// while parenthetical breakdowns and tier tags (`(max 20x)`, `(promax)`) are dimmed.
fn style_usage_suffix(suffix: &str, is_exhausted: bool) -> String {
    let mut out = String::new();
    let mut plain_buf = String::new();
    let mut chars = suffix.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '↻' {
            if !plain_buf.is_empty() {
                out.push_str(&dim(&plain_buf));
                plain_buf.clear();
            }
            let mut eta = String::from('↻');
            while let Some(&c) = chars.peek() {
                if c.is_ascii_alphanumeric() {
                    eta.push(c);
                    chars.next();
                } else {
                    break;
                }
            }
            if is_exhausted {
                out.push_str(&yellow(&eta));
            } else {
                out.push_str(&cyan(&eta));
            }
        } else {
            plain_buf.push(ch);
        }
    }
    if !plain_buf.is_empty() {
        out.push_str(&dim(&plain_buf));
    }
    out
}

/// Format a profile's `usage` string with semantic per-window colors and highlighted reset timers.
#[allow(dead_code)]
pub fn format_profile_usage(usage: Option<&str>) -> String {
    let Some(u) = usage.map(str::trim).filter(|s| !s.is_empty()) else {
        return dim("-");
    };
    let mut styled_segments = Vec::new();
    for seg in u.split(" · ") {
        if let Some((before_left, after_left)) = seg.split_once("% left")
            && let Some((label, pct_str)) = before_left.split_once(':')
        {
            let label = label.trim();
            let pct = pct_str.trim().parse::<i64>().unwrap_or(100);
            let styled_label = if label == "gemini" || label == "opus" {
                bold(&format!("{label}:"))
            } else {
                dim(&format!("{label}:"))
            };
            let pct_badge = format!("{pct}% left");
            let styled_pct = if pct <= 0 {
                red(&bold(&pct_badge))
            } else if pct <= 25 {
                yellow(&pct_badge)
            } else {
                green(&pct_badge)
            };
            let styled_after = style_usage_suffix(after_left, pct <= 0);
            styled_segments.push(format!("{styled_label} {styled_pct}{styled_after}"));
            continue;
        }
        if let Some((head, tail)) = seg.split_once(" (") {
            styled_segments.push(format!("{} {}", green(head), dim(&format!("({tail}"))));
        } else {
            styled_segments.push(green(seg));
        }
    }
    styled_segments.join(&dim(" · "))
}

fn profile_plan_and_status(kind: &str, usage: Option<&str>) -> (String, String) {
    let default_plan = |kind: &str| match kind {
        "oauth" | "subscription" => green("subscription"),
        "coding-plan" => cyan("coding plan"),
        "api-key" => dim("API key"),
        _ => dim(kind),
    };
    let style_plan_tier = |tier: &str| match tier {
        "ultra" | "max 20x" | "promax" => bold_cyan(tier),
        "free" => dim(tier),
        "api-key" => dim("API key"),
        _ => green(tier),
    };

    let Some(raw) = usage.map(str::trim).filter(|s| !s.is_empty()) else {
        return (default_plan(kind), dim("not reported"));
    };

    let mut core = raw;
    let mut extracted_tier: Option<&str> = None;
    if let Some(open_idx) = raw.rfind(" (")
        && raw.ends_with(')')
    {
        let tag = raw[open_idx + 2..raw.len() - 1].trim();
        let normalized = match tag {
            "Max Usage" => Some("max"),
            "ultra" | "pro" | "free" | "promax" | "plus" | "max 5x" | "max 20x" | "max"
            | "api-key" | "team" | "enterprise" => Some(tag),
            _ => None,
        };
        if let Some(t) = normalized {
            extracted_tier = Some(t);
            core = raw[..open_idx].trim();
        }
    }

    let plan_cell = match extracted_tier {
        Some(t) => style_plan_tier(t),
        None => default_plan(kind),
    };

    let windows = core
        .split(" · ")
        .map(str::trim)
        .filter(|seg| !seg.is_empty() && *seg != "-")
        .map(|seg| {
            if let Some((head, suffix)) = seg.split_once("% left")
                && let Some((label, number)) = head.split_once(':')
            {
                if let Ok(pct) = number.trim().parse::<u8>()
                    && pct <= 100
                {
                    let value = format!("{pct}% left");
                    let value = match pct {
                        0 => red(&value),
                        1..=25 => yellow(&value),
                        _ => green(&value),
                    };
                    return format!("{label}: {value}{}", style_usage_suffix(suffix, pct == 0));
                }
                return format!(
                    "{}{}",
                    dim(&format!("{label}: unavailable")),
                    style_usage_suffix(suffix, false)
                );
            }
            if seg.starts_with("oauth expired") || seg.starts_with("limited") {
                red(seg)
            } else {
                dim(seg)
            }
        })
        .collect::<Vec<_>>();
    let status = if windows.is_empty() {
        dim("not reported")
    } else {
        windows.join(&dim(" · "))
    };
    (plan_cell, status)
}

fn profile_account_label(profile: &str, account: Option<&str>) -> String {
    match account.map(str::trim).filter(|account| !account.is_empty()) {
        Some(account) if profile == account => account.to_owned(),
        Some(account) => format!("{account} · {profile}"),
        None => format!("Profile: {profile}"),
    }
}

pub(crate) fn print_profiles_pretty(entries: &[crate::create::ProviderProfileEntry]) {
    if entries.is_empty() {
        println!(
            "\n  {} {}\n",
            bold_cyan("Provider Profiles"),
            dim("none configured")
        );
        return;
    }
    let in_use = entries
        .iter()
        .filter(|entry| !entry.assigned_agents.is_empty() || entry.subagent_count > 0)
        .count();
    println!(
        "\n  {}  {} · {}\n",
        bold_cyan("Provider Profiles"),
        bold(&format!("{} configured", entries.len())),
        cyan(&format!("{in_use} in use")),
    );

    let mut table = Table::new(&[
        ("PROVIDER", false),
        ("ACCOUNT", false),
        ("PLAN", false),
        ("QUOTA / STATUS", false),
        ("AGENTS", false),
    ])
    .preserve_cells();

    for e in entries {
        let provider_cell = bold(&e.provider);
        let account_cell = profile_account_label(&e.profile, e.account.as_deref());
        let (plan_cell, status_cell) = profile_plan_and_status(&e.kind, e.usage.as_deref());

        let agents_cell = if e.assigned_agents.is_empty() && e.subagent_count == 0 {
            dim("none")
        } else {
            let mut parts = Vec::new();
            if !e.assigned_agents.is_empty() {
                let shown: Vec<&str> = e
                    .assigned_agents
                    .iter()
                    .take(3)
                    .map(String::as_str)
                    .collect();
                let base = bold(&shown.join(", "));
                if e.assigned_agents.len() > 3 {
                    parts.push(format!(
                        "{base} {}",
                        cyan(&format!("+{}", e.assigned_agents.len() - 3))
                    ));
                } else {
                    parts.push(base);
                }
            }
            if e.subagent_count > 0 {
                parts.push(dim(&format!("(+{} sub)", e.subagent_count)));
            }
            parts.join(" ")
        };

        table.add_row(vec![
            provider_cell,
            account_cell,
            plan_cell,
            status_cell,
            agents_cell,
        ]);
    }

    table.print();
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quota_status_preserves_every_period_and_reset() {
        let (plan, status) = profile_plan_and_status(
            "subscription",
            Some("limited ↻2h20m · 7d: 5% left ↻1d22h · mo: 80% left ↻9d (ultra)"),
        );
        assert!(plan.contains("ultra"), "{plan}");
        assert!(!status.contains("5h:"), "{status}");
        for evidence in [
            "7d:", "5% left", "↻1d22h", "mo:", "80% left", "↻9d", "limited", "↻2h20m",
        ] {
            assert!(status.contains(evidence), "{evidence} missing: {status}");
        }
    }

    #[test]
    fn profile_status_does_not_invent_quota_or_conflate_account_and_profile() {
        let (_, missing) = profile_plan_and_status("oauth", None);
        assert!(missing.contains("not reported"));
        for usage in ["5h: ?% left", "5h: 150% left"] {
            let (_, malformed) = profile_plan_and_status("oauth", Some(usage));
            assert!(malformed.contains("5h: unavailable"));
            assert!(!malformed.contains('%'));
        }
        assert_eq!(
            profile_account_label("work", Some("u@example.test")),
            "u@example.test · work"
        );
        assert_eq!(profile_account_label("work", None), "Profile: work");
    }

    #[test]
    fn visible_width_counts_cjk_as_double_width_and_ignores_ansi() {
        assert_eq!(visible_width("helper"), 6);
        assert_eq!(visible_width("實作"), 4);
        assert_eq!(visible_width("helper · 實作 · Building"), 24);
        assert_eq!(
            visible_width("\x1b[1mhelper\x1b[0m \x1b[2m· 實作\x1b[0m"),
            13
        );
    }

    #[test]
    fn truncate_visible_respects_cjk_boundaries_and_resets_ansi() {
        let s = "範例資料庫Owner | category: 現有遊戲";
        let t = truncate_visible(s, 14);
        // "範例資料庫" is 10 cols + "Own" (3 cols) + "…" (1 col) = 14 cols
        assert_eq!(visible_width(&t), 14);
        assert!(t.ends_with('…'));

        let styled = "\x1b[2m範例工作資料庫Owner\x1b[0m";
        let ts = truncate_visible(styled, 9);
        assert!(visible_width(&ts) <= 9);
        assert!(ts.ends_with("…\x1b[0m"));
    }

    #[test]
    fn short_model_preserves_gateway_and_strips_vendor_prefixes() {
        assert_eq!(
            short_model_name("sylphx/openai/gpt-6.1-sol#xhigh"),
            "sylphx/gpt-6.1-sol#xhigh"
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
                "Sub-agent of Coordinator. Outcome: preserve all documents during import",
                "coordinator-sub-1",
                None,
                true
            ),
            "preserve all documents during import"
        );
    }

    #[test]
    fn table_renders_role_column_right_after_agent_column() {
        let mut table = Table::new(&[
            ("AGENT", false),
            ("ROLE", false),
            ("STATUS", false),
            ("TREE", false),
            ("RUNTIME", false),
        ])
        .with_fit_terminal(false);
        table.add_row(vec![
            "alpha".to_string(),
            "director".to_string(),
            "○ idle".to_string(),
            "1 sub".to_string(),
            "agy · gemini".to_string(),
        ]);
        table.add_row(vec![
            "└─ beta".to_string(),
            "engineer".to_string(),
            "● working".to_string(),
            "L1".to_string(),
            "spiron · gpt".to_string(),
        ]);
        let lines = table.render_lines();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("AGENT"));
        assert!(lines[0].contains("ROLE"));
        assert!(!lines[1].contains("alpha · director"));
        assert!(lines[1].contains("alpha"));
        assert!(lines[1].contains("director"));
        assert!(!lines[2].contains("beta · engineer"));
        assert!(lines[2].contains("└─ beta"));
        assert!(lines[2].contains("engineer"));
    }

    #[test]
    fn status_formatting_separates_active_turn_duration_from_true_lag() {
        let sample_agent = |turn_state: &str,
                            level: &str,
                            open_turn_age_ms: Option<i64>,
                            oldest_undelivered_age_ms: Option<i64>|
         -> AgentView {
            serde_json::from_value(serde_json::json!({
                "has_open_turn": open_turn_age_ms.is_some(),
                "open_turn_age_ms": open_turn_age_ms,
                "id": "agt_sample",
                "name": "worker-1",
                "parent": "agt_root",
                "scope": "private",
                "harness": "spiron",
                "workdir": null,
                "state": "running",
                "process": "alive",
                "turn_state": turn_state,
                "delivery": {"level": level, "idempotent": false, "proof": "test"},
                "quota_reset_at_ms": null,
                "oldest_undelivered_age_ms": oldest_undelivered_age_ms,
                "install": "installed",
                "description": null,
                "instructions": null,
                "schedules": []
            }))
            .unwrap()
        };

        // 1. Healthy steered long turn: shows active turn duration, zero ! lag.
        let steered = sample_agent("working", "mid-turn", Some(56 * 60_000), None);
        let rendered_steered = format_status_with_alerts(&steered, 10_000_000);
        assert_eq!(
            rendered_steered,
            format!("{} {}", bold_green("● working"), dim("56m"))
        );
        assert!(!rendered_steered.contains("! lag"));

        // 2. Healthy turn-end turn with queued mail: shows dim queued age, zero ! lag.
        let followup = sample_agent("working", "turn-end", Some(20 * 60_000), Some(180_000));
        let rendered_followup = format_status_with_alerts(&followup, 10_000_000);
        assert!(rendered_followup.starts_with(&format!(
            "{} {}",
            bold_green("● working"),
            dim("20m")
        )));
        assert!(rendered_followup.contains("queued 3m"));
        assert!(!rendered_followup.contains("! lag"));

        let mut attention = steered;
        let now = 10_000_000;
        assert_eq!(agent_attention(&attention, now), None);
        attention.last_error = Some("private error details".into());
        assert_eq!(
            agent_attention(&attention, now),
            Some(AgentAttention::Error)
        );
        assert_eq!(format_status_with_alerts(&attention, now), red("✕ error"));
        attention.block_reason = Some("sign_in_required".into());
        assert_eq!(
            agent_attention(&attention, now),
            Some(AgentAttention::SignIn)
        );
        assert_eq!(
            format_status_with_alerts(&attention, now),
            red("✕ sign-in required")
        );
        attention.quota_reset_at_ms = Some(now + 60_000);
        assert_eq!(
            agent_attention(&attention, now),
            Some(AgentAttention::Quota(60_000))
        );
        assert_eq!(
            format_status_with_alerts(&attention, now),
            yellow("◐ quota ↻1m")
        );
        attention.state = "paused".into();
        assert_eq!(
            agent_attention(&attention, now),
            Some(AgentAttention::Paused)
        );
        assert_eq!(
            format_status_with_alerts(&attention, now),
            yellow("◐ paused")
        );
        attention.state = "faulted".into();
        assert_eq!(
            agent_attention(&attention, now),
            Some(AgentAttention::Faulted)
        );
        assert_eq!(format_status_with_alerts(&attention, now), red("✕ faulted"));

        // 3. Genuine undelivered backlog: renders ! lag and increments subtree lagging count.
        let stalled = sample_agent("idle", "mid-turn", None, Some(180_000));
        let rendered_stalled = format_status_with_alerts(&stalled, 10_000_000);
        assert!(rendered_stalled.contains("! lag 3m"));

        // 4. A mid-turn agent whose mail still waits is lag, not a queue,
        //    whatever the harness is called.
        let stuck_steer = sample_agent("working", "mid-turn", Some(20 * 60_000), Some(180_000));
        let rendered_stuck = format_status_with_alerts(&stuck_steer, 10_000_000);
        assert!(rendered_stuck.contains("! lag 3m"), "{rendered_stuck}");
        assert!(!rendered_stuck.contains("queued"));

        // 5. A next-step agent's mail that still waits is lag too: its turn
        //    takes mail at the next step, and a mailbox stuck behind a step
        //    that never comes must not read as a queue.
        let next_step = sample_agent("working", "next-step", Some(20 * 60_000), Some(180_000));
        let rendered_next = format_status_with_alerts(&next_step, 10_000_000);
        assert!(rendered_next.contains("! lag 3m"), "{rendered_next}");
        assert!(!rendered_next.contains("queued"));

        let mut children: HashMap<&str, Vec<&AgentView>> = HashMap::new();
        children.insert(
            "agt_root",
            vec![&steered, &followup, &stalled, &stuck_steer, &next_step],
        );
        let mut memo = HashMap::new();
        let stats = compute_subtree_stats("agt_root", &children, 10_000_000, &mut memo);
        assert_eq!(stats.working, 4);
        assert_eq!(stats.lagging, 3);
    }
}

/// Compact overview; full watch targets and prompts remain in schedules.
pub fn automation_summary(counts: &crate::api::AutomationCountsView) -> String {
    let mut parts = Vec::new();
    for (count, singular, plural) in [
        (counts.watches, "watch", "watches"),
        (counts.timers, "timer", "timers"),
        (counts.crons, "cron", "crons"),
    ] {
        if count > 0 {
            parts.push(format!(
                "{count} {}",
                if count == 1 { singular } else { plural }
            ));
        }
    }
    if parts.is_empty() {
        "-".into()
    } else {
        parts.join(" · ")
    }
}

#[cfg(test)]
mod automation_tests {
    use super::*;
    #[test]
    fn summaries_have_exact_counts_and_pluralization() {
        assert_eq!(automation_summary(&Default::default()), "-");
        assert_eq!(
            automation_summary(&crate::api::AutomationCountsView {
                total: 7,
                watches: 3,
                timers: 1,
                crons: 3,
            }),
            "3 watches · 1 timer · 3 crons"
        );
    }
}
