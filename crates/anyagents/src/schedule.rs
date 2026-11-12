//! Per-agent self-scheduled wakes (`at`, `every`) and event watches (`watch`).
//!
//! Every agent on any harness can schedule its own one-off wake (`at`), recurring
//! wake (`every`), or event watch (`watch`: GitHub PR/check terminal state, Spiron
//! promise state change pulled from the public API, or an AnyAgents agent going idle).
//!
//! A due tick or a triggered watch is written to the owner's mailbox at once, like
//! any other message. Mailbox delivery alone decides when the owner sees it: it is
//! steered into a running turn, or held while the owner is paused or quota-blocked
//! and delivered on resume. A schedule adds no new tick while its previous one is
//! still undelivered, so missed recurring ticks coalesce into one. Cancelling a
//! schedule takes effect immediately.

use std::collections::HashMap;
use std::time::Duration;

use anyagents_core::{Agent, AgentId, AgentSchedule, Incoming, NewSchedule, Store};
use anyagents_supervisor::HealthBoard;

use crate::api::ScheduleView;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum WatchTarget {
    GitHubPr {
        owner: String,
        repo: String,
        number: u64,
    },
    GitHubCheck {
        owner: String,
        repo: String,
        reference: String,
    },
    SpironPromise {
        id: String,
        kind: String,
    },
    AgentIdle {
        target: String,
    },
    ProcessExit {
        pid: u32,
    },
    PathChanged {
        path: String,
    },
}

/// Parse a duration string such as `"1m"`, `"30s"`, `"2h"`, `"1d"`, `"daily"`, `"hourly"`.
pub fn parse_duration_ms(raw: &str) -> Result<i64, String> {
    let trimmed = raw.trim();
    let cleaned = trimmed
        .strip_prefix("every ")
        .or_else(|| trimmed.strip_prefix("in "))
        .or_else(|| trimmed.strip_prefix('+'))
        .unwrap_or(trimmed)
        .trim();
    if cleaned.is_empty() {
        return Err("duration cannot be empty".to_string());
    }
    match cleaned.to_ascii_lowercase().as_str() {
        "daily" | "day" => return Ok(86_400_000),
        "hourly" | "hour" => return Ok(3_600_000),
        "weekly" | "week" => return Ok(7 * 86_400_000),
        _ => {}
    }
    if let Ok(secs) = cleaned.parse::<i64>() {
        if secs <= 0 {
            return Err("duration must be positive".to_string());
        }
        return Ok(secs.saturating_mul(1_000));
    }

    let mut total_ms: i64 = 0;
    let mut num_buf = String::new();
    let mut unit_buf = String::new();
    for c in cleaned.chars() {
        if c.is_whitespace() {
            continue;
        }
        if c.is_ascii_digit() {
            if !unit_buf.is_empty() {
                total_ms = total_ms.saturating_add(apply_unit(&num_buf, &unit_buf)?);
                num_buf.clear();
                unit_buf.clear();
            }
            num_buf.push(c);
        } else if c.is_ascii_alphabetic() {
            unit_buf.push(c);
        } else {
            return Err(format!("invalid duration `{raw}`"));
        }
    }
    if !num_buf.is_empty() {
        let u = if unit_buf.is_empty() { "s" } else { &unit_buf };
        total_ms = total_ms.saturating_add(apply_unit(&num_buf, u)?);
    } else {
        return Err(format!("invalid duration `{raw}`"));
    }
    if total_ms <= 0 {
        return Err("duration must be positive".to_string());
    }
    Ok(total_ms)
}

fn apply_unit(num_str: &str, unit_str: &str) -> Result<i64, String> {
    let val: i64 = num_str
        .parse()
        .map_err(|_| format!("invalid number `{num_str}`"))?;
    if val <= 0 {
        return Err("duration must be positive".to_string());
    }
    let mult: i64 = match unit_str.to_ascii_lowercase().as_str() {
        "ms" | "msec" | "msecs" => 1,
        "s" | "sec" | "secs" | "second" | "seconds" => 1_000,
        "m" | "min" | "mins" | "minute" | "minutes" => 60_000,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3_600_000,
        "d" | "day" | "days" => 86_400_000,
        "w" | "wk" | "wks" | "week" | "weeks" => 7 * 86_400_000,
        other => return Err(format!("unknown duration unit `{other}`")),
    };
    Ok(val.saturating_mul(mult))
}

/// Parse an `at` specification into a target Unix timestamp in milliseconds.
/// Accepts relative durations (`"1m"`, `"+30s"`, `"in 2h"`), RFC3339 timestamps,
/// or Unix epoch seconds/milliseconds.
pub fn parse_at_ms(raw: &str, now_ms: i64) -> Result<i64, String> {
    let s = raw.trim();
    if s.is_empty() {
        return Err("`at` cannot be empty".to_string());
    }
    if let Some(ms) = parse_rfc3339_ms(s) {
        return Ok(ms);
    }
    if s.chars().all(|c| c.is_ascii_digit())
        && let Ok(n) = s.parse::<i64>()
    {
        if n >= 100_000_000_000 {
            return Ok(n);
        }
        if n >= 1_000_000_000 {
            return Ok(n.saturating_mul(1_000));
        }
    }
    let dur = parse_duration_ms(s)?;
    Ok(now_ms.saturating_add(dur))
}

fn parse_rfc3339_ms(raw: &str) -> Option<i64> {
    let (date_part, time_part) = raw.split_once('T').or_else(|| raw.split_once('t'))?;
    let mut d_it = date_part.split('-');
    let year: i64 = d_it.next()?.parse().ok()?;
    let month: u32 = d_it.next()?.parse().ok()?;
    let day: u32 = d_it.next()?.parse().ok()?;
    if d_it.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let (hms_frac, offset_secs): (&str, i64) = if let Some(stripped) = time_part
        .strip_suffix('Z')
        .or_else(|| time_part.strip_suffix('z'))
    {
        (stripped, 0)
    } else if let Some((t, off)) = time_part.rsplit_once('+') {
        let mut o_it = off.split(':');
        let oh: i64 = o_it.next()?.parse().ok()?;
        let om: i64 = o_it.next().unwrap_or("0").parse().ok()?;
        (t, (oh * 3600) + (om * 60))
    } else if let Some((t, off)) = time_part.rsplit_once('-') {
        let mut o_it = off.split(':');
        let oh: i64 = o_it.next()?.parse().ok()?;
        let om: i64 = o_it.next().unwrap_or("0").parse().ok()?;
        (t, -((oh * 3600) + (om * 60)))
    } else {
        return None;
    };
    let (hms, frac_ms) = match hms_frac.split_once('.') {
        Some((h, f)) => {
            let mut digits: String = f.chars().take(3).collect();
            while digits.len() < 3 {
                digits.push('0');
            }
            (h, digits.parse::<i64>().ok()?)
        }
        None => (hms_frac, 0),
    };
    let mut t_it = hms.split(':');
    let hour: i64 = t_it.next()?.parse().ok()?;
    let min: i64 = t_it.next()?.parse().ok()?;
    let sec: i64 = t_it.next().unwrap_or("0").parse().ok()?;
    if !(0..=23).contains(&hour) || !(0..=59).contains(&min) || !(0..=60).contains(&sec) {
        return None;
    }
    let days = days_from_civil(year, month, day);
    let total_secs = days * 86_400 + hour * 3_600 + min * 60 + sec - offset_secs;
    Some(total_secs * 1_000 + frac_ms)
}

fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let m = i64::from(month);
    let d = i64::from(day);
    let y = if m <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Parse and validate a `watch` event specification.
pub fn parse_watch_spec(raw: &str) -> Result<WatchTarget, String> {
    let s = raw.trim();
    if s.is_empty() {
        return Err("`watch` cannot be empty".to_string());
    }
    // URL form: https://github.com/<owner>/<repo>/pull/<number>
    if let Some(rest) = s
        .strip_prefix("https://github.com/")
        .or_else(|| s.strip_prefix("http://github.com/"))
    {
        let parts: Vec<&str> = rest.trim_matches('/').split('/').collect();
        if parts.len() >= 4
            && (parts[2] == "pull" || parts[2] == "pulls")
            && let Ok(num) = parts[3].parse::<u64>()
        {
            return Ok(WatchTarget::GitHubPr {
                owner: parts[0].to_string(),
                repo: parts[1].to_string(),
                number: num,
            });
        }
    }

    // Agent idle forms:
    // "agent:idle:<name>", "idle:<name>", "agent:<name>:idle"
    if let Some(target) = s
        .strip_prefix("agent:idle:")
        .or_else(|| s.strip_prefix("idle:"))
    {
        let clean = target
            .trim()
            .strip_prefix("agent:")
            .unwrap_or(target.trim());
        if clean.is_empty() {
            return Err("`agent:idle:<name>` requires a target agent name or id".to_string());
        }
        return Ok(WatchTarget::AgentIdle {
            target: clean.to_string(),
        });
    }
    if let Some(mid) = s
        .strip_prefix("agent:")
        .and_then(|r| r.strip_suffix(":idle"))
    {
        let clean = mid.trim();
        if !clean.is_empty() {
            return Ok(WatchTarget::AgentIdle {
                target: clean.to_string(),
            });
        }
    }

    // Spiron promise / task forms:
    // "spiron:promise:<id>", "promise:<id>", "spiron:task:<id>", "spiron:<id>"
    if let Some(id) = s
        .strip_prefix("spiron:promise:")
        .or_else(|| s.strip_prefix("promise:"))
    {
        let clean = id.trim();
        if clean.is_empty() {
            return Err("`spiron:promise:<id>` requires a promise id".to_string());
        }
        return Ok(WatchTarget::SpironPromise {
            id: clean.to_string(),
            kind: "promise".to_string(),
        });
    }
    if let Some(id) = s.strip_prefix("spiron:task:") {
        let clean = id.trim();
        if clean.is_empty() {
            return Err("`spiron:task:<id>` requires a task id".to_string());
        }
        return Ok(WatchTarget::SpironPromise {
            id: clean.to_string(),
            kind: "task".to_string(),
        });
    }
    if let Some(id) = s.strip_prefix("spiron:") {
        let clean = id.trim();
        if clean.is_empty() {
            return Err("`spiron:<id>` requires a promise or task id".to_string());
        }
        return Ok(WatchTarget::SpironPromise {
            id: clean.to_string(),
            kind: "promise".to_string(),
        });
    }

    // GitHub check forms:
    // "github:check:owner/repo@ref", "github:checks:owner/repo#41", "gh:check:...", "check:..."
    if let Some(spec) = s
        .strip_prefix("github:checks:")
        .or_else(|| s.strip_prefix("github:check:"))
        .or_else(|| s.strip_prefix("gh:checks:"))
        .or_else(|| s.strip_prefix("gh:check:"))
        .or_else(|| s.strip_prefix("checks:"))
        .or_else(|| s.strip_prefix("check:"))
    {
        return parse_gh_check_coords(spec.trim());
    }

    // GitHub PR forms:
    // "github:pr:owner/repo#41", "gh:pr:owner/repo#41", "pr:owner/repo#41", "github:owner/repo#41"
    if let Some(spec) = s
        .strip_prefix("github:pr:")
        .or_else(|| s.strip_prefix("gh:pr:"))
        .or_else(|| s.strip_prefix("pr:"))
        .or_else(|| s.strip_prefix("github:"))
        .or_else(|| s.strip_prefix("gh:"))
    {
        if spec.contains('@') {
            return parse_gh_check_coords(spec.trim());
        }
        return parse_gh_pr_coords(spec.trim());
    }

    // Local process exit forms:
    // "pid:<pid>", "process:<pid>", "proc:<pid>", "pid:<pid>:exit"
    if let Some(pid_str) = s
        .strip_prefix("pid:")
        .or_else(|| s.strip_prefix("process:"))
        .or_else(|| s.strip_prefix("proc:"))
    {
        let clean = pid_str
            .trim()
            .strip_suffix(":exit")
            .unwrap_or(pid_str.trim());
        let pid = clean
            .parse::<u32>()
            .map_err(|_| format!("invalid process PID `{clean}` in watch spec `{raw}`"))?;
        if pid == 0 {
            return Err("process PID must be > 0".to_string());
        }
        return Ok(WatchTarget::ProcessExit { pid });
    }

    // Local file / path change forms:
    // "file:<path>", "path:<path>"
    if let Some(path_str) = s.strip_prefix("file:").or_else(|| s.strip_prefix("path:")) {
        let clean = path_str.trim();
        if clean.is_empty() {
            return Err("`file:<path>` requires a non-empty path".to_string());
        }
        return Ok(WatchTarget::PathChanged {
            path: clean.to_string(),
        });
    }

    Err(format!(
        "unsupported watch spec `{raw}`; expected `github:pr:<owner>/<repo>#<num>`, `github:check:<owner>/<repo>@<ref>`, `spiron:promise:<id>`, `agent:idle:<name>`, `pid:<pid>`, or `file:<path>`"
    ))
}

fn parse_gh_pr_coords(spec: &str) -> Result<WatchTarget, String> {
    if let Some((repo_part, num_part)) = spec.split_once('#')
        && let Some((owner, repo)) = repo_part.split_once('/')
        && let Ok(num) = num_part.trim().parse::<u64>()
        && !owner.trim().is_empty()
        && !repo.trim().is_empty()
    {
        return Ok(WatchTarget::GitHubPr {
            owner: owner.trim().to_string(),
            repo: repo.trim().to_string(),
            number: num,
        });
    }
    let parts: Vec<&str> = spec.trim_matches('/').split('/').collect();
    if parts.len() == 3
        && let Ok(num) = parts[2].parse::<u64>()
    {
        return Ok(WatchTarget::GitHubPr {
            owner: parts[0].to_string(),
            repo: parts[1].to_string(),
            number: num,
        });
    }
    if parts.len() == 4
        && (parts[2] == "pull" || parts[2] == "pulls")
        && let Ok(num) = parts[3].parse::<u64>()
    {
        return Ok(WatchTarget::GitHubPr {
            owner: parts[0].to_string(),
            repo: parts[1].to_string(),
            number: num,
        });
    }
    Err(format!(
        "invalid GitHub PR watch spec `{spec}`; expected `<owner>/<repo>#<number>`"
    ))
}

fn parse_gh_check_coords(spec: &str) -> Result<WatchTarget, String> {
    if let Some((repo_part, ref_part)) = spec.split_once('@').or_else(|| spec.split_once('#'))
        && let Some((owner, repo)) = repo_part.split_once('/')
        && !owner.trim().is_empty()
        && !repo.trim().is_empty()
        && !ref_part.trim().is_empty()
    {
        return Ok(WatchTarget::GitHubCheck {
            owner: owner.trim().to_string(),
            repo: repo.trim().to_string(),
            reference: ref_part.trim().to_string(),
        });
    }
    Err(format!(
        "invalid GitHub check watch spec `{spec}`; expected `<owner>/<repo>@<sha_or_ref>` or `<owner>/<repo>#<pr>`"
    ))
}

/// Create a new schedule for `agent` from `(at, every, watch, prompt)`.
pub fn create_agent_schedule(
    store: &Store,
    health: &HealthBoard,
    agent: &Agent,
    at: Option<&str>,
    every: Option<&str>,
    watch: Option<&str>,
    prompt: &str,
) -> Result<ScheduleView, String> {
    let clean_prompt = prompt.trim();
    if clean_prompt.is_empty() {
        return Err("`schedule` requires a non-empty `prompt`".to_string());
    }
    let at_opt = at.map(str::trim).filter(|s| !s.is_empty());
    let every_opt = every.map(str::trim).filter(|s| !s.is_empty());
    let watch_opt = watch.map(str::trim).filter(|s| !s.is_empty());
    let mode_count = usize::from(at_opt.is_some())
        + usize::from(every_opt.is_some())
        + usize::from(watch_opt.is_some());
    if mode_count == 0 {
        return Err("`schedule` requires one of `at`, `every`, or `watch`".to_string());
    }
    if mode_count > 1 {
        return Err("specify only one of `at`, `every`, or `watch` per schedule".to_string());
    }
    let now = anyagents_core::now_ms();
    let input = if let Some(at_str) = at_opt {
        let next_ms = parse_at_ms(at_str, now)?;
        NewSchedule {
            agent_id: agent.id.clone(),
            kind: "at".to_string(),
            spec: at_str.to_string(),
            prompt: clean_prompt.to_string(),
            interval_ms: None,
            next_fire_at_ms: Some(next_ms),
            watch_baseline: None,
        }
    } else if let Some(every_str) = every_opt {
        let interval = parse_duration_ms(every_str)?;
        if interval < 1_000 {
            return Err("`every` interval must be at least 1s".to_string());
        }
        NewSchedule {
            agent_id: agent.id.clone(),
            kind: "every".to_string(),
            spec: every_str.to_string(),
            prompt: clean_prompt.to_string(),
            interval_ms: Some(interval),
            next_fire_at_ms: Some(now.saturating_add(interval)),
            watch_baseline: None,
        }
    } else if let Some(watch_str) = watch_opt {
        let mut parsed = parse_watch_spec(watch_str)?;
        let mut resolved_spec = watch_str.to_string();
        if let WatchTarget::PathChanged { path } = &mut parsed {
            let p = std::path::PathBuf::from(&*path);
            let abs = if p.is_absolute() {
                p
            } else {
                agent
                    .workdir
                    .as_ref()
                    .map(std::path::PathBuf::from)
                    .unwrap_or_else(|| anyagents_core::agent_work_dir(store.data_dir(), &agent.id))
                    .join(p)
            };
            *path = abs.to_string_lossy().into_owned();
            resolved_spec = format!("file:{path}");
        }
        let baseline = initial_watch_baseline(store, health, &parsed, agent.id.as_str(), now);
        if let Some(ref b) = baseline
            && b.starts_with("probe_error:")
        {
            return Err(b.clone());
        }
        NewSchedule {
            agent_id: agent.id.clone(),
            kind: "watch".to_string(),
            spec: resolved_spec,
            prompt: clean_prompt.to_string(),
            interval_ms: None,
            next_fire_at_ms: None,
            watch_baseline: baseline,
        }
    } else {
        unreachable!()
    };

    let created = store
        .create_schedule(input)
        .map_err(|err| err.to_string())?;
    Ok(ScheduleView::from(created))
}

pub fn list_agent_schedules(
    store: &Store,
    agent_id: &anyagents_core::AgentId,
    active_only: bool,
) -> Result<Vec<ScheduleView>, String> {
    store
        .list_schedules(Some(agent_id), active_only)
        .map(|rows| rows.into_iter().map(ScheduleView::from).collect())
        .map_err(|err| err.to_string())
}

pub fn cancel_agent_schedule(
    store: &Store,
    agent_id: &anyagents_core::AgentId,
    id: &str,
) -> Result<usize, String> {
    store
        .cancel_schedule(Some(agent_id), id)
        .map_err(|err| err.to_string())
}

fn initial_watch_baseline(
    store: &Store,
    health: &HealthBoard,
    target: &WatchTarget,
    _agent_id: &str,
    now: i64,
) -> Option<String> {
    match target {
        WatchTarget::SpironPromise { id, kind } => {
            Some(probe_spiron_state(id, kind).unwrap_or_else(|err| err.observation().into()))
        }
        WatchTarget::AgentIdle { target } => {
            if let Ok(found) = store.agent_by_name(target) {
                if is_agent_busy(store, health, &found, now) {
                    Some("working".to_string())
                } else {
                    Some("idle".to_string())
                }
            } else {
                None
            }
        }
        WatchTarget::GitHubPr { .. } => Some("pending".to_string()),
        WatchTarget::GitHubCheck { .. } => Some("pending".to_string()),
        WatchTarget::ProcessExit { pid } => Some(
            if probe_process_alive(*pid) {
                "running"
            } else {
                "exited"
            }
            .to_string(),
        ),
        WatchTarget::PathChanged { path } => Some(probe_path_fingerprint(path)),
    }
}

/// Returns `Some(reason)` when the owning agent must NOT receive a scheduled wake
/// right now (`"paused"`, `"quota_blocked"`, or `"busy"`), or `None` when idle and eligible.
pub fn agent_wake_block_reason(
    store: &Store,
    health: &HealthBoard,
    agent: &Agent,
    now: i64,
) -> Option<&'static str> {
    if agent.state == "paused" {
        return Some("paused");
    }
    if agent.state == "archived" || crate::server::is_explicitly_archived(store.data_dir(), agent) {
        return Some("archived");
    }
    let cfg = anyagents_core::agent_config_dir(store.data_dir(), &agent.id);
    let (session_quota, session_error) = if agent.harness == "claude-code" {
        let (_, persisted_err) =
            anyagents_harness_agy::detect_agy_session_quota_and_error(&cfg, now);
        (
            anyagents_harness_claude::detect_session_quota_reset_ms(&cfg, now),
            persisted_err,
        )
    } else {
        anyagents_harness_agy::detect_agy_session_quota_and_error(&cfg, now)
    };
    let live = health.get(&agent.id);
    let effective_quota = live
        .as_ref()
        .and_then(|l| l.quota_reset_at_ms)
        .or(session_quota);
    let effective_err = live
        .as_ref()
        .and_then(|l| l.last_error.clone())
        .or(session_error);
    if effective_quota.is_some_and(|ms| ms > now) {
        return Some("quota_blocked");
    }
    if let Some(ref err) = effective_err {
        let lower = err.to_ascii_lowercase();
        if lower.contains("quota")
            || lower.contains("rate limit")
            || lower.contains("429")
            || lower.contains("credit balance is too low")
            || lower.contains("usage limit")
        {
            return Some("quota_blocked");
        }
    }
    if is_agent_busy(store, health, agent, now) {
        return Some("busy");
    }
    None
}

pub fn is_agent_busy(store: &Store, health: &HealthBoard, agent: &Agent, now: i64) -> bool {
    // Check first-party turn ownership even before health has been published.
    if crate::vendor::is_spiron_harness(&agent.harness) && store.try_lock_agent(&agent.id).is_err()
    {
        return true;
    }
    match store.has_open_turn(&agent.id) {
        Ok(false) => {}
        Ok(true) => return true,
        Err(err) => {
            eprintln!("Schedule turn state unavailable: {err}");
            return true;
        }
    }
    match store.lag(&agent.id, now) {
        Ok(lag) if lag.oldest_undelivered_age_ms.is_none() => {}
        Ok(_) => return true,
        Err(err) => {
            eprintln!("Schedule mailbox lag unavailable: {err}");
            return true;
        }
    }
    if let Some(live) = health.get(&agent.id)
        && live.process_alive
        && live.turn_state == anyagents_harness::TurnState::InTurn
    {
        return true;
    }
    false
}

/// Evaluate local timers and watches without issuing any external probe.
/// This is called on its own one-second loop, including the startup tick.
pub fn tick_local_schedules_once(store: &Store, health: &HealthBoard, now: i64) -> usize {
    if let Err(err) = store.restore_checks_completed_pr_watches() {
        eprintln!("Schedule recovery unavailable: {err}");
        return 0;
    }
    let fired = tick_schedule_partition(store, health, now, false);
    let _ = tick_task_subagent_turn_ownership(store, health, now);
    fired
}

/// Enforce event-driven Ping-Pong turn ownership between parents and `lifecycle: task`
/// sub-agents without mutating `SYSTEM_PROMPT.md` (preserving prompt cache):
/// - **Rule A (Ball in Child's Court)**: If an idle `task` sub-agent with no active schedules
///   finished a parent-initiated turn (`>= 15s` ago) without calling `send`, nudge it once to
///   either finish or report to `parent` via `send` (and if it exits a second time without
///   sending, alert `parent` once).
/// - **Rule B (Ball in Parent's Court)**: If an idle `task` sub-agent has reported to its
///   `parent` (`last_outbound >= last_inbound`), has no active schedules, and remains unarchived
///   for `>= 15m` after both the report and the parent's latest turn completed while the parent
///   is idle, deliver a single consolidated event notification to `parent` reminding it to either
///   send follow-up instructions or `archive` the completed task sub-agent(s).
pub fn tick_task_subagent_turn_ownership(store: &Store, health: &HealthBoard, now: i64) -> usize {
    let Ok(agents) = store.list_agents() else {
        return 0;
    };
    let agents_by_id: HashMap<AgentId, Agent> =
        agents.iter().cloned().map(|a| (a.id.clone(), a)).collect();
    let mut actions = 0usize;
    let mut waiting_by_parent: HashMap<AgentId, Vec<(String, String, i64)>> = HashMap::new();

    for child in &agents {
        let Some(parent_id) = child.parent_id.as_ref() else {
            continue;
        };
        let Some(parent) = agents_by_id.get(parent_id) else {
            continue;
        };
        if matches!(child.state.as_str(), "paused" | "archived")
            || crate::server::is_explicitly_archived(store.data_dir(), child)
        {
            continue;
        }
        if !child.is_task_subagent() {
            continue;
        }
        if agent_wake_block_reason(store, health, child, now).is_some() {
            continue;
        }
        if store
            .list_schedules(Some(&child.id), true)
            .is_ok_and(|s| !s.is_empty())
        {
            continue;
        }
        let Ok(snap) = store.turn_ownership_snapshot(&child.id) else {
            continue;
        };
        let last_in_ms = snap.last_delivered_at_ms.unwrap_or(0);
        let last_out_ms = snap.last_outbound_at_ms.unwrap_or(0);
        let parent_sender = format!("agent:{}", parent.name);

        if last_in_ms > 0
            && last_in_ms > last_out_ms
            && now.saturating_sub(last_in_ms) >= 15_000
            && snap
                .last_delivered_sender
                .as_deref()
                .is_some_and(|s| s == "parent" || s == parent_sender || s == "system:turn-guard")
        {
            let seq = snap.last_delivered_seq.unwrap_or(0);
            let already_nudged = snap
                .last_delivered_source_key
                .as_deref()
                .is_some_and(|k| k.starts_with("turn-guard:child-nudge:"));
            if already_nudged {
                if !matches!(parent.state.as_str(), "paused" | "archived")
                    && !crate::server::is_explicitly_archived(store.data_dir(), parent)
                    && agent_wake_block_reason(store, health, parent, now).is_none()
                {
                    let alert_key = format!("turn-guard:parent-alert:{}:{seq}", child.id);
                    if store
                        .has_message_source_key(&parent.id, &alert_key)
                        .is_ok_and(|exists| !exists)
                    {
                        let body = format!(
                            "[Sub-Agent Turn Ownership Guard] Task sub-agent `{}` (`role: {}`) exited its turn without calling `send` to report back and has no active schedules. Inspect it via `agents(action: \"inspect\", name: \"{}\")`, send it a follow-up instruction, or close it via `agents(action: \"archive\", name: \"{}\")`.",
                            child.name,
                            child.role.as_deref().unwrap_or("task"),
                            child.name,
                            child.name
                        );
                        let mut msg = Incoming::new(body);
                        msg.sender = Some("system:turn-guard".to_string());
                        msg.source_key = Some(alert_key);
                        msg.source_time_ms = Some(now);
                        if store.receive(&parent.id, msg).is_ok() {
                            crate::create::poke_agent_wake_socket(store.data_dir(), &parent.id);
                            crate::create::poke_agent_wake_socket_mail(
                                store.data_dir(),
                                &parent.id,
                            );
                            actions += 1;
                        }
                    }
                }
            } else {
                let nudge_key = format!("turn-guard:child-nudge:{}:{seq}", child.id);
                if store
                    .has_message_source_key(&child.id, &nudge_key)
                    .is_ok_and(|exists| !exists)
                {
                    let body = format!(
                        "[Turn Ownership Guard] You exited your turn as a `lifecycle: task` sub-agent without calling `send` to report to your parent (`agent:{}`) and without an active `schedule`. Complete any remaining work now or call `send` (`to: \"parent\"`) with your verified outcome or blocker.",
                        parent.name
                    );
                    let mut msg = Incoming::new(body);
                    msg.sender = Some("system:turn-guard".to_string());
                    msg.source_key = Some(nudge_key);
                    msg.source_time_ms = Some(now);
                    if store.receive(&child.id, msg).is_ok() {
                        crate::create::poke_agent_wake_socket(store.data_dir(), &child.id);
                        crate::create::poke_agent_wake_socket_mail(store.data_dir(), &child.id);
                        actions += 1;
                    }
                }
            }
        } else if last_out_ms > 0
            && last_out_ms >= last_in_ms
            && !matches!(parent.state.as_str(), "paused" | "archived")
            && !crate::server::is_explicitly_archived(store.data_dir(), parent)
            && agent_wake_block_reason(store, health, parent, now).is_none()
            && let Ok(parent_snap) = store.turn_ownership_snapshot(&parent.id)
        {
            let ref_ms = last_out_ms.max(parent_snap.last_delivered_at_ms.unwrap_or(0));
            if now.saturating_sub(ref_ms) >= 900_000 {
                waiting_by_parent
                    .entry(parent.id.clone())
                    .or_default()
                    .push((
                        child.name.clone(),
                        child.role.clone().unwrap_or_else(|| "task".to_string()),
                        last_out_ms,
                    ));
            }
        }
    }

    for (parent_id, mut children) in waiting_by_parent {
        children.sort_by(|a, b| a.0.cmp(&b.0));
        let fingerprint_raw = children
            .iter()
            .map(|(name, _, ts)| format!("{name}:{ts}"))
            .collect::<Vec<_>>()
            .join(",");
        let digest = anyagents_core::learned_memory::sha256(fingerprint_raw.as_bytes());
        let hash16 = &digest[..16.min(digest.len())];
        let reminder_key = format!("turn-guard:waiting-parent:{parent_id}:{hash16}");
        if store
            .has_message_source_key(&parent_id, &reminder_key)
            .is_ok_and(|exists| !exists)
        {
            let formatted = children
                .iter()
                .map(|(name, role, _)| format!("`{name}` (`{role}`)"))
                .collect::<Vec<_>>()
                .join(", ");
            let body = format!(
                "[Sub-Agent Turn Ownership Guard] The following `lifecycle: task` sub-agent(s) reported to you and are idle with no active schedules (`waiting_parent`): {formatted}. The ball is in your court: for each sub-agent, either send a follow-up instruction via `send` if work remains, or close it via `agents(action: \"archive\", name: \"<name>\")` if its deliverable is accepted."
            );
            let mut msg = Incoming::new(body);
            msg.sender = Some("system:turn-guard".to_string());
            msg.source_key = Some(reminder_key);
            msg.source_time_ms = Some(now);
            if store.receive(&parent_id, msg).is_ok() {
                crate::create::poke_agent_wake_socket(store.data_dir(), &parent_id);
                crate::create::poke_agent_wake_socket_mail(store.data_dir(), &parent_id);
                actions += 1;
            }
        }
    }

    actions
}

/// Evaluate network-backed watches on a separate loop. Slow external services
/// cannot delay a later local tick or consume a timer's due transition.
pub fn tick_external_watches_once(store: &Store, health: &HealthBoard, now: i64) -> usize {
    tick_schedule_partition(store, health, now, true)
}

fn is_external_watch(sched: &AgentSchedule) -> bool {
    sched.kind == "watch"
        && matches!(
            parse_watch_spec(&sched.spec),
            Ok(WatchTarget::GitHubPr { .. }
                | WatchTarget::GitHubCheck { .. }
                | WatchTarget::SpironPromise { .. })
        )
}

// Retain the complete single-pass fixture for existing cross-class contracts;
// production never serializes the two partitions through this helper.
#[cfg(test)]
fn tick_schedules_once(store: &Store, health: &HealthBoard, now: i64) -> usize {
    tick_local_schedules_once(store, health, now) + tick_external_watches_once(store, health, now)
}

fn tick_schedule_partition(store: &Store, health: &HealthBoard, now: i64, external: bool) -> usize {
    let schedules: Vec<_> = match store.list_schedules(None, true) {
        Ok(schedules) => schedules
            .into_iter()
            .filter(|sched| is_external_watch(sched) == external)
            .collect(),
        Err(err) => {
            eprintln!("Schedule census unavailable: {err}");
            return 0;
        }
    };
    if external {
        github::begin_cycle(&schedules, now);
    }
    let mut fired_count = 0;
    for sched in schedules {
        if (sched.kind == "at" || sched.kind == "every")
            && sched.next_fire_at_ms.is_none_or(|due| now < due)
        {
            continue;
        }
        let agent = match store.agent(&sched.agent_id) {
            Ok(agent) => agent,
            Err(anyagents_core::Error::AgentNotFound(_)) => {
                if let Err(err) = store.cancel_schedule(None, &sched.id) {
                    eprintln!("Schedule cancellation failed: {err}");
                }
                continue;
            }
            Err(err) => {
                eprintln!("Schedule agent state unavailable: {err}");
                continue;
            }
        };
        if agent.state == "archived" {
            if let Err(err) = store.cancel_schedule(None, &sched.id) {
                eprintln!("Schedule cancellation failed: {err}");
            }
            continue;
        }
        match sched.kind.as_str() {
            "at" => {
                let Some(due) = sched.next_fire_at_ms else {
                    continue;
                };
                if now < due {
                    continue;
                }
                if deliver_scheduled_wake(
                    store,
                    &agent,
                    &sched,
                    &format!("schedule:{}:{}", sched.id, due),
                    now,
                ) {
                    fired_count += 1;
                }
            }
            "every" => {
                let Some(due) = sched.next_fire_at_ms else {
                    continue;
                };
                if now < due {
                    continue;
                }
                let interval = sched.interval_ms.unwrap_or(60_000).max(1_000);
                let mut next_due = due.saturating_add(interval);
                while next_due <= now {
                    next_due = next_due.saturating_add(interval);
                }
                let Some(previous_pending) = previous_wake_pending(store, &agent, &sched) else {
                    continue;
                };
                if previous_pending {
                    // The undelivered tick already stands for this one; the owner
                    // receives a single wake and the schedule moves to its next slot.
                    if let Err(err) = store.claim_schedule_tick(
                        &sched.id,
                        Some(due),
                        "active",
                        Some(next_due),
                        None,
                        None,
                        Some("coalesced"),
                    ) {
                        eprintln!("Schedule {} coalesce write failed: {err}", sched.id);
                    }
                    continue;
                }
                if claim_and_deliver_scheduled_wake(
                    store,
                    &agent,
                    &sched,
                    Some(due),
                    "active",
                    Some(next_due),
                    None,
                    Some("fired"),
                    &sched.prompt,
                    &format!("schedule:{}:{}", sched.id, due),
                    now,
                ) {
                    fired_count += 1;
                }
            }
            "watch" => {
                let eval = evaluate_watch(store, health, &sched, now);
                let previous_pending = if eval.triggered {
                    let Some(pending) = previous_wake_pending(store, &agent, &sched) else {
                        continue;
                    };
                    pending
                } else {
                    false
                };
                if previous_pending {
                    // Record the latest observation without consuming the transition or
                    // moving the baseline; it is written once the pending wake is taken.
                    if eval.observed.is_some()
                        && eval.observed != sched.last_observed
                        && let Err(err) = store.update_schedule_watch_observation(
                            &sched.id,
                            sched.watch_baseline.as_deref(),
                            eval.observed.as_deref(),
                        )
                    {
                        eprintln!("Schedule {} observation write failed: {err}", sched.id);
                    }
                    continue;
                }
                if !eval.triggered {
                    if (eval.baseline.is_some() || eval.observed.is_some())
                        && (eval
                            .baseline
                            .as_ref()
                            .is_some_and(|b| Some(b) != sched.watch_baseline.as_ref())
                            || eval
                                .observed
                                .as_ref()
                                .is_some_and(|o| Some(o) != sched.last_observed.as_ref()))
                        && let Err(err) = store.update_schedule_watch_observation(
                            &sched.id,
                            eval.baseline.as_deref(),
                            eval.observed.as_deref(),
                        )
                    {
                        eprintln!("Schedule {} observation write failed: {err}", sched.id);
                    }
                    continue;
                }
                let (wake_prompt, source_key) = if let Some(ref tr) = eval.transition {
                    (
                        format!("[watch: {} transition={tr}]\n{}", sched.spec, sched.prompt),
                        format!("schedule:{}:watch:{tr}:{now}", sched.id),
                    )
                } else {
                    (sched.prompt.clone(), format!("schedule:{}:watch", sched.id))
                };
                if claim_and_deliver_scheduled_wake(
                    store,
                    &agent,
                    &sched,
                    None,
                    eval.next_state,
                    None,
                    eval.baseline.as_deref(),
                    eval.observed.as_deref(),
                    &wake_prompt,
                    &source_key,
                    now,
                ) {
                    fired_count += 1;
                }
            }
            _ => {}
        }
    }
    fired_count
}

fn deliver_scheduled_wake(
    store: &Store,
    agent: &Agent,
    sched: &AgentSchedule,
    source_key: &str,
    now: i64,
) -> bool {
    claim_and_deliver_scheduled_wake(
        store,
        agent,
        sched,
        sched.next_fire_at_ms,
        "fired",
        None,
        None,
        Some("fired"),
        &sched.prompt,
        source_key,
        now,
    )
}

#[allow(clippy::too_many_arguments)]
fn claim_and_deliver_scheduled_wake(
    store: &Store,
    agent: &Agent,
    sched: &AgentSchedule,
    expected_due: Option<i64>,
    next_state: &str,
    next_due: Option<i64>,
    baseline: Option<&str>,
    observed: Option<&str>,
    prompt: &str,
    source_key: &str,
    now: i64,
) -> bool {
    let result = (|| -> Result<bool, anyagents_core::Error> {
        // Fresh lifecycle and external retirement are serialized with archive.
        let Ok(_lifecycle) = store.try_lock_agent_lifecycle(&agent.id) else {
            return Ok(false);
        };
        let agent = store.agent(&agent.id)?;
        if agent.state == "archived"
            || crate::server::is_explicitly_archived(store.data_dir(), &agent)
        {
            return Ok(false);
        }
        let delivered = store.claim_schedule_tick_with_wake(
            &sched.id,
            expected_due,
            next_state,
            next_due,
            Some(now),
            baseline,
            observed,
            &agent.id,
            Incoming {
                text: prompt.into(),
                sender: Some(schedule_sender(&sched.id)),
                source_time_ms: Some(now),
                source_key: Some(source_key.into()),
                reply_to: None,
                quote: None,
                origin: None,
                received_at_ms: Some(now),
            },
        )?;
        if delivered {
            crate::create::poke_agent_wake_socket(store.data_dir(), &agent.id);
            crate::create::poke_agent_wake_socket_mail(store.data_dir(), &agent.id);
        }
        Ok(delivered)
    })();
    match result {
        Ok(delivered) => delivered,
        Err(err) => {
            eprintln!("Schedule {} not consumed: {err}", sched.id);
            false
        }
    }
}

/// Whether the schedule's previous wake still waits in the owner's mailbox, or
/// `None` when the mailbox cannot be read (the tick then waits for a later pass).
fn previous_wake_pending(store: &Store, agent: &Agent, sched: &AgentSchedule) -> Option<bool> {
    match store.has_undelivered_message_from(&agent.id, &schedule_sender(&sched.id)) {
        Ok(pending) => Some(pending),
        Err(err) => {
            eprintln!("Schedule {} mailbox state unavailable: {err}", sched.id);
            None
        }
    }
}

/// Mailbox sender of every wake a schedule writes.
fn schedule_sender(schedule_id: &str) -> String {
    format!("schedule:{schedule_id}")
}

struct WatchEval {
    triggered: bool,
    next_state: &'static str,
    transition: Option<String>,
    observed: Option<String>,
    baseline: Option<String>,
}

impl WatchEval {
    fn pending(baseline: Option<String>, observed: Option<String>) -> Self {
        Self {
            triggered: false,
            next_state: "active",
            transition: None,
            observed,
            baseline,
        }
    }

    fn fired(baseline: Option<String>, observed: Option<String>) -> Self {
        Self {
            triggered: true,
            next_state: "fired",
            transition: None,
            observed,
            baseline,
        }
    }
}

fn evaluate_watch(
    store: &Store,
    health: &HealthBoard,
    sched: &AgentSchedule,
    now: i64,
) -> WatchEval {
    let Ok(target) = parse_watch_spec(&sched.spec) else {
        return WatchEval::pending(
            sched.watch_baseline.clone(),
            Some("invalid_spec".to_string()),
        );
    };
    match target {
        WatchTarget::AgentIdle { target } => {
            let Ok(found) = store.agent_by_name(&target) else {
                return WatchEval::pending(
                    sched.watch_baseline.clone(),
                    Some("not_found".to_string()),
                );
            };
            if is_agent_busy(store, health, &found, now) {
                WatchEval::pending(Some("working".to_string()), Some("working".to_string()))
            } else {
                WatchEval::fired(sched.watch_baseline.clone(), Some("idle".to_string()))
            }
        }
        WatchTarget::SpironPromise { id, kind } => {
            let current = match probe_spiron_state(&id, &kind) {
                Ok(current) => current,
                Err(err) => {
                    return WatchEval::pending(
                        sched.watch_baseline.clone(),
                        Some(err.observation().into()),
                    );
                }
            };
            evaluate_spiron_observation(sched.watch_baseline.as_deref(), current)
        }
        WatchTarget::GitHubPr {
            owner,
            repo,
            number,
        } => {
            let Some((_, state_desc)) =
                probe_github_pr_terminal(&owner, &repo, number, sched.agent_id.as_str(), now)
            else {
                return WatchEval::pending(sched.watch_baseline.clone(), None);
            };
            evaluate_pr_observation(sched, state_desc)
        }
        WatchTarget::GitHubCheck {
            owner,
            repo,
            reference,
        } => {
            let Some((is_terminal, state_desc)) = probe_github_check_terminal(
                &owner,
                &repo,
                &reference,
                sched.agent_id.as_str(),
                now,
            ) else {
                return WatchEval::pending(sched.watch_baseline.clone(), None);
            };
            if is_terminal {
                WatchEval::fired(sched.watch_baseline.clone(), Some(state_desc))
            } else {
                WatchEval::pending(sched.watch_baseline.clone(), Some(state_desc))
            }
        }
        WatchTarget::ProcessExit { pid } => {
            if probe_process_alive(pid) {
                WatchEval::pending(Some("running".to_string()), Some("running".to_string()))
            } else {
                WatchEval::fired(sched.watch_baseline.clone(), Some("exited".to_string()))
            }
        }
        WatchTarget::PathChanged { path } => {
            let current = probe_path_fingerprint(&path);
            if current.starts_with("probe_error:") {
                return WatchEval::pending(sched.watch_baseline.clone(), Some(current));
            }
            match &sched.watch_baseline {
                Some(base) => {
                    if &current != base {
                        WatchEval::fired(Some(base.clone()), Some(format!("{base}->{current}")))
                    } else {
                        WatchEval::pending(Some(base.clone()), Some(current))
                    }
                }
                None => {
                    if current != "missing" {
                        WatchEval::fired(Some(current.clone()), Some(current))
                    } else {
                        WatchEval::pending(Some(current.clone()), Some(current))
                    }
                }
            }
        }
    }
}

fn evaluate_pr_observation(sched: &AgentSchedule, state_desc: String) -> WatchEval {
    if state_desc.starts_with("probe_error:") {
        return WatchEval::pending(sched.watch_baseline.clone(), Some(state_desc));
    }
    let prev = sched.watch_baseline.as_deref().unwrap_or("pending");
    if state_desc == "merged" || state_desc == "closed" {
        return WatchEval {
            triggered: true,
            next_state: "fired",
            transition: Some(state_desc.clone()),
            observed: Some(state_desc.clone()),
            baseline: Some(state_desc),
        };
    }
    if prev.starts_with("merge_queue:") && !state_desc.starts_with("merge_queue:") {
        return WatchEval {
            triggered: true,
            next_state: "active",
            transition: Some("evicted".to_string()),
            observed: Some(format!("evicted:{state_desc}")),
            baseline: Some(state_desc),
        };
    }
    if state_desc == "review_failed" && prev != "review_failed" {
        return WatchEval {
            triggered: true,
            next_state: "active",
            transition: Some("review_failed".to_string()),
            observed: Some("review_failed".to_string()),
            baseline: Some("review_failed".to_string()),
        };
    }
    if state_desc == "checks_completed" && prev != "checks_completed" {
        return WatchEval {
            triggered: true,
            next_state: "active",
            transition: Some("checks_completed".to_string()),
            observed: Some("checks_completed".to_string()),
            baseline: Some("checks_completed".to_string()),
        };
    }
    WatchEval::pending(Some(state_desc.clone()), Some(state_desc))
}

fn probe_process_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    #[cfg(target_os = "linux")]
    {
        let stat_path = format!("/proc/{pid}/stat");
        let Ok(raw) = std::fs::read_to_string(&stat_path) else {
            return false;
        };
        if let Some((_, rest)) = raw.rsplit_once(')') {
            let state_char = rest.trim_start().chars().next().unwrap_or(' ');
            if matches!(state_char, 'Z' | 'X' | 'x') {
                return false;
            }
        }
        true
    }
    #[cfg(not(target_os = "linux"))]
    {
        let rc = unsafe { libc::kill(pid as i32, 0) };
        if rc == 0 {
            return true;
        }
        std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }
}

fn probe_path_fingerprint(path: &str) -> String {
    match std::fs::metadata(path) {
        Ok(meta) => {
            let mtime_ms = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis())
                .unwrap_or(0);
            format!("exists:{mtime_ms}:{}", meta.len())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => "missing".to_string(),
        Err(e) => format!("probe_error:stat:{:?}", e.kind()),
    }
}

/// Failed initial probes are visible diagnostics, not provider lifecycle states.
/// Recovery establishes the first valid baseline rather than inventing a transition.
fn evaluate_spiron_observation(baseline: Option<&str>, current: String) -> WatchEval {
    match baseline.filter(|base| !base.starts_with("probe_error:")) {
        Some(base) if current != base => {
            WatchEval::fired(Some(base.into()), Some(format!("{base}->{current}")))
        }
        Some(base) => WatchEval::pending(Some(base.into()), Some(current)),
        None if is_spiron_terminal(&current) => {
            WatchEval::fired(Some(current.clone()), Some(current))
        }
        None => WatchEval::pending(Some(current.clone()), Some(current)),
    }
}

fn is_spiron_terminal(state: &str) -> bool {
    matches!(
        state.to_ascii_lowercase().as_str(),
        "passed"
            | "failed"
            | "dropped"
            | "completed"
            | "complete"
            | "done"
            | "succeeded"
            | "success"
            | "error"
            | "canceled"
            | "cancelled"
            | "expired"
            | "rejected"
    )
}

fn spiron_api_key() -> Option<String> {
    if let Ok(k) = std::env::var("SPIRON_API_KEY")
        && !k.trim().is_empty()
    {
        return Some(k.trim().to_string());
    }
    let key_file = std::env::var("SPIRON_KEY_FILE").ok()?;
    std::fs::read_to_string(key_file)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

#[derive(Debug, PartialEq, Eq)]
pub enum SpironProbeError {
    NotFound,
    Auth,
    Http,
    Transport,
    Payload,
}
impl SpironProbeError {
    fn observation(&self) -> &'static str {
        match self {
            Self::NotFound => "probe_error:not_found",
            Self::Auth => "probe_error:auth",
            Self::Http => "probe_error:http",
            Self::Transport => "probe_error:transport",
            Self::Payload => "probe_error:payload",
        }
    }
}

/// Distinguish a valid pending state from an unavailable observation.
pub fn probe_spiron_state(id: &str, kind: &str) -> Result<String, SpironProbeError> {
    let base = std::env::var("SPIRON_API_BASE").unwrap_or_else(|_| "https://api.spiron.ai".into());
    probe_spiron_at(&base, spiron_api_key().as_deref(), id, kind)
}

fn probe_spiron_at(
    base: &str,
    key: Option<&str>,
    id: &str,
    kind: &str,
) -> Result<String, SpironProbeError> {
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(5)))
        .max_redirects(0)
        .http_status_as_error(false)
        .build()
        .new_agent();
    let kinds = if kind == "task" {
        ["tasks", "promises"]
    } else {
        ["promises", "tasks"]
    };
    for resource in kinds {
        let url = format!("{}/v1/{resource}/{id}", base.trim_end_matches('/'));
        let mut req = agent.get(&url).header("accept", "application/json");
        if let Some(token) = key {
            req = req.header("authorization", &format!("Bearer {token}"));
        }
        let response = req.call().map_err(|_| SpironProbeError::Transport)?;
        match response.status().as_u16() {
            404 => continue,
            401 | 403 => return Err(SpironProbeError::Auth),
            200 => {}
            _ => return Err(SpironProbeError::Http),
        }
        let body = response
            .into_body()
            .read_to_string()
            .map_err(|_| SpironProbeError::Transport)?;
        let val: serde_json::Value =
            serde_json::from_str(&body).map_err(|_| SpironProbeError::Payload)?;
        return val
            .get("state")
            .or_else(|| val.get("status"))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|state| !state.is_empty())
            .map(str::to_string)
            .ok_or(SpironProbeError::Payload);
    }
    Err(SpironProbeError::NotFound)
}

mod github;

fn probe_github_pr_terminal(
    owner: &str,
    repo: &str,
    number: u64,
    agent_id: &str,
    now_ms: i64,
) -> Option<(bool, String)> {
    github::probe(owner, repo, &number.to_string(), true, agent_id, now_ms)
}

fn check_item_is_terminal(item: &serde_json::Value) -> bool {
    if let Some(status) = item.get("status").and_then(serde_json::Value::as_str) {
        return status.eq_ignore_ascii_case("COMPLETED");
    }
    if let Some(state) = item.get("state").and_then(serde_json::Value::as_str) {
        return matches!(
            state.to_ascii_uppercase().as_str(),
            "SUCCESS" | "FAILURE" | "ERROR" | "CANCELLED" | "TIMED_OUT" | "SKIPPED" | "NEUTRAL"
        );
    }
    false
}

fn probe_github_check_terminal(
    owner: &str,
    repo: &str,
    reference: &str,
    agent_id: &str,
    now_ms: i64,
) -> Option<(bool, String)> {
    let clean_ref = reference.strip_prefix('#').unwrap_or(reference);
    if clean_ref.chars().all(|c| c.is_ascii_digit())
        && let Ok(pr_num) = clean_ref.parse::<u64>()
    {
        return probe_github_pr_terminal(owner, repo, pr_num, agent_id, now_ms);
    }
    github::probe(owner, repo, clean_ref, false, agent_id, now_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_core::NewAgent;
    use anyagents_harness::{DeliveryMode, SteerSupport, TurnState};
    use anyagents_supervisor::LiveState;

    fn publish_turn(
        health: &HealthBoard,
        agent_id: &AgentId,
        turn_state: TurnState,
        quota_reset_at_ms: Option<i64>,
    ) {
        health.publish(
            agent_id,
            LiveState {
                process_alive: true,
                turn_state,
                delivery_mode: DeliveryMode::Steer,
                steer_support: SteerSupport::Yes,
                quota_reset_at_ms,
                restart_at_ms: None,
                pid: None,
                live_harness: None,
                live_model: None,
                process_started_at_ms: Some(1000),
                last_error: quota_reset_at_ms.map(|_| "quota exceeded".to_string()),
            },
        );
    }

    #[test]
    fn parses_durations_and_watch_specs() {
        assert_eq!(parse_duration_ms("1m").unwrap(), 60_000);
        assert_eq!(parse_duration_ms("+30s").unwrap(), 30_000);
        assert_eq!(parse_duration_ms("2h").unwrap(), 7_200_000);
        assert_eq!(parse_duration_ms("daily").unwrap(), 86_400_000);

        assert_eq!(
            parse_watch_spec("github:pr:AnotherOrg/anyagents#41").unwrap(),
            WatchTarget::GitHubPr {
                owner: "AnotherOrg".to_string(),
                repo: "anyagents".to_string(),
                number: 41,
            }
        );
        assert_eq!(
            parse_watch_spec("spiron:promise:prm_12345").unwrap(),
            WatchTarget::SpironPromise {
                id: "prm_12345".to_string(),
                kind: "promise".to_string(),
            }
        );
        assert_eq!(
            parse_watch_spec("agent:idle:worker-1").unwrap(),
            WatchTarget::AgentIdle {
                target: "worker-1".to_string(),
            }
        );
        assert_eq!(
            parse_watch_spec("pid:4242").unwrap(),
            WatchTarget::ProcessExit { pid: 4242 }
        );
        assert_eq!(
            parse_watch_spec("file:/tmp/out.json").unwrap(),
            WatchTarget::PathChanged {
                path: "/tmp/out.json".to_string(),
            }
        );
    }

    #[test]
    fn first_tick_admits_local_watches_and_timers_before_older_external_probes() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::sync::Arc;
        let _env = crate::server::tests::env_lock();
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let health = HealthBoard::default();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let external_owner = store
            .create_agent(NewAgent::new("external", "codex"))
            .unwrap();
        store
            .set_agent_running(&external_owner.id, "/work")
            .unwrap();
        let external = store
            .create_schedule(NewSchedule {
                agent_id: external_owner.id,
                kind: "watch".into(),
                spec: "github:pr:example-org/project#1".into(),
                prompt: "external".into(),
                interval_ms: None,
                next_fire_at_ms: None,
                watch_baseline: Some("pending".into()),
            })
            .unwrap();
        // Make the external watch strictly older, independent of clock resolution.
        rusqlite::Connection::open(dir.path().join("state.db"))
            .unwrap()
            .execute(
                "UPDATE agent_schedules SET created_at_ms=1 WHERE id=?1",
                [external.id.as_str()],
            )
            .unwrap();
        let target = store
            .create_agent(NewAgent::new("idle-target", "codex"))
            .unwrap();
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", "exit 0"])
            .spawn()
            .unwrap();
        let pid = child.id();
        assert!(child.wait().unwrap().success());
        let path = dir.path().join("ready");
        std::fs::write(&path, "ready").unwrap();
        let now = anyagents_core::now_ms();
        let mut owners = Vec::new();
        for (label, kind, spec) in [
            ("file", "watch", format!("file:{}", path.display())),
            ("process", "watch", format!("pid:{pid}")),
            ("idle", "watch", format!("agent:idle:{}", target.name)),
            ("timer", "at", "due".into()),
            ("interval", "every", "1s".into()),
        ] {
            let owner = store.create_agent(NewAgent::new(label, "codex")).unwrap();
            store.set_agent_running(&owner.id, "/work").unwrap();
            store
                .create_schedule(NewSchedule {
                    agent_id: owner.id.clone(),
                    kind: kind.into(),
                    spec,
                    prompt: label.into(),
                    interval_ms: (kind == "every").then_some(1000),
                    next_fire_at_ms: (kind != "watch").then_some(now - 1),
                    watch_baseline: (label == "file").then(|| "missing".into()),
                })
                .unwrap();
            owners.push(owner.id);
        }
        assert_eq!(store.list_schedules(None, true).unwrap()[0].id, external.id);
        let observing_store = Arc::clone(&store);
        let observed_owners = owners.clone();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut request = [0; 4096];
            let _ = stream.read(&mut request).unwrap();
            // This is the start of an actual external HTTP probe, before it
            // can finish or release the scheduler. Local admission must be done.
            let counts: Vec<_> = observed_owners
                .iter()
                .map(|id| observing_store.inbox(id, None).unwrap().len())
                .collect();
            stream
                .write_all(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                )
                .unwrap();
            counts
        });
        // SAFETY: serialized with all other process-wide GitHub API overrides.
        unsafe {
            std::env::set_var("ANYAGENTS_GITHUB_API_BASE", format!("http://{address}"));
        }
        let fired = tick_schedules_once(&store, &health, now);
        unsafe {
            std::env::remove_var("ANYAGENTS_GITHUB_API_BASE");
        }
        let counts_at_external_start = server.join().unwrap();
        assert_eq!(fired, 5);
        assert_eq!(counts_at_external_start, vec![1; 5]);
        for owner in owners {
            assert_eq!(store.inbox(&owner, None).unwrap().len(), 1);
        }
    }

    #[test]
    fn same_agent_ready_local_watches_enter_mailbox_together_even_when_paused() {
        for paused in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(dir.path().join("state.db")).unwrap();
            let health = HealthBoard::default();
            let owner = store.create_agent(NewAgent::new("owner", "codex")).unwrap();
            let target = store
                .create_agent(NewAgent::new("target", "codex"))
                .unwrap();
            let path = dir.path().join("ready");
            std::fs::write(&path, "ready").unwrap();
            let mut child = std::process::Command::new("/bin/sh")
                .args(["-c", "exit 0"])
                .spawn()
                .unwrap();
            let pid = child.id();
            assert!(child.wait().unwrap().success());
            for (spec, baseline) in [
                (format!("file:{}", path.display()), Some("missing".into())),
                (format!("pid:{pid}"), None),
                (format!("agent:idle:{}", target.name), None),
            ] {
                store
                    .create_schedule(NewSchedule {
                        agent_id: owner.id.clone(),
                        kind: "watch".into(),
                        spec,
                        prompt: "wake".into(),
                        interval_ms: None,
                        next_fire_at_ms: None,
                        watch_baseline: baseline,
                    })
                    .unwrap();
            }
            if paused {
                store.set_agent_state(&owner.id, "paused").unwrap();
            }
            let fired = tick_local_schedules_once(&store, &health, anyagents_core::now_ms());
            assert_eq!(fired, 3);
            let inbox = store.inbox(&owner.id, None).unwrap();
            assert_eq!(inbox.len(), fired);
            let schedules = store.list_schedules(Some(&owner.id), false).unwrap();
            assert_eq!(schedules.len(), 3);
            assert!(schedules.iter().all(|s| s.state == "fired"));
            assert_eq!(
                tick_local_schedules_once(&store, &health, anyagents_core::now_ms()),
                0
            );
            assert_eq!(store.inbox(&owner.id, None).unwrap().len(), fired);
        }
    }

    #[test]
    fn overdue_at_delivers_on_first_local_tick_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");
        let store = Store::open(&db).unwrap();
        let owner = store
            .create_agent(NewAgent::new("restart-owner", "codex"))
            .unwrap();
        let now = anyagents_core::now_ms();
        let sched = store
            .create_schedule(NewSchedule {
                agent_id: owner.id.clone(),
                kind: "at".into(),
                spec: "past".into(),
                prompt: "restart wake".into(),
                interval_ms: None,
                next_fire_at_ms: Some(now - 86_400_000),
                watch_baseline: None,
            })
            .unwrap();
        drop(store);
        let restarted = Store::open(&db).unwrap();
        let health = HealthBoard::default();
        assert_eq!(tick_local_schedules_once(&restarted, &health, now), 1);
        let inbox = restarted.inbox(&owner.id, None).unwrap();
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].text, "restart wake");
        let saved = restarted.list_schedules(Some(&owner.id), false).unwrap();
        assert_eq!(saved[0].id, sched.id);
        assert_eq!(saved[0].state, "fired");
        assert_eq!(saved[0].last_observed.as_deref(), Some("fired"));
        assert_eq!(
            tick_local_schedules_once(&restarted, &health, now + 1000),
            0
        );
        assert_eq!(restarted.inbox(&owner.id, None).unwrap().len(), 1);
    }

    #[test]
    fn overdue_at_enters_mailbox_while_owner_busy_or_paused() {
        for reason in ["busy", "paused"] {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("state.db");
            let store = Store::open(&db).unwrap();
            let owner = store
                .create_agent(NewAgent::new("held-owner", "codex"))
                .unwrap();
            let now = anyagents_core::now_ms();
            let due = now - 1000;
            store
                .create_schedule(NewSchedule {
                    agent_id: owner.id.clone(),
                    kind: "at".into(),
                    spec: "past".into(),
                    prompt: "held wake".into(),
                    interval_ms: None,
                    next_fire_at_ms: Some(due),
                    watch_baseline: None,
                })
                .unwrap();
            let health = HealthBoard::default();
            if reason == "paused" {
                store.set_agent_state(&owner.id, "paused").unwrap();
            } else {
                publish_turn(&health, &owner.id, TurnState::InTurn, None);
            }
            assert_eq!(
                tick_local_schedules_once(&store, &health, now),
                1,
                "{reason}"
            );
            let saved = store.list_schedules(Some(&owner.id), false).unwrap();
            assert_eq!(saved[0].state, "fired");
            assert_eq!(saved[0].last_observed.as_deref(), Some("fired"));
            assert_eq!(saved[0].last_fired_at_ms, Some(now));
            let inbox = store.inbox(&owner.id, None).unwrap();
            assert_eq!(inbox.len(), 1);
            assert_eq!(inbox[0].text, "held wake");
            // The mailbox, not the scheduler, holds it for a paused owner.
            if reason == "paused" {
                assert_eq!(store.agent(&owner.id).unwrap().state, "paused");
            }
            assert_eq!(tick_local_schedules_once(&store, &health, now + 1000), 0);
            drop(store);
            let store = Store::open(&db).unwrap();
            assert_eq!(tick_local_schedules_once(&store, &health, now + 2000), 0);
            assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 1);
        }
    }

    #[test]
    fn local_schedule_watch_fires_with_delivery_loop_lock_held() {
        for harness in ["agy", "claude-code", "codex", "opencode", "muse", "omp"] {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(dir.path().join("state.db")).unwrap();
            let health = HealthBoard::default();
            let owner = store.create_agent(NewAgent::new("owner", harness)).unwrap();
            store.set_agent_running(&owner.id, "/work").unwrap();
            // DeliveryLoop retains the lock for the process lifetime, even while idle.
            let _lock = store.try_lock_agent(&owner.id).unwrap();
            let mut live = LiveState {
                process_alive: true,
                turn_state: TurnState::Idle,
                delivery_mode: DeliveryMode::Steer,
                steer_support: SteerSupport::Yes,
                quota_reset_at_ms: None,
                restart_at_ms: None,
                pid: None,
                live_harness: Some(harness.to_string()),
                live_model: None,
                process_started_at_ms: Some(1000),
                last_error: None,
            };
            health.publish(&owner.id, live.clone());
            let now = anyagents_core::now_ms();
            assert!(!is_agent_busy(&store, &health, &owner, now), "{harness}");
            let path = dir.path().join("wake.trigger");
            std::fs::write(&path, "before").unwrap();
            create_agent_schedule(
                &store,
                &health,
                &owner,
                None,
                None,
                Some(&format!("file:{}", path.display())),
                "file changed",
            )
            .unwrap();
            assert_eq!(tick_schedules_once(&store, &health, now), 0);
            std::fs::write(&path, "after with different length").unwrap();
            // A turn in progress does not hold the wake back: it enters the mailbox.
            live.turn_state = TurnState::InTurn;
            health.publish(&owner.id, live);
            assert!(is_agent_busy(&store, &health, &owner, now));
            assert_eq!(tick_schedules_once(&store, &health, now + 1000), 1);
            let inbox = store.inbox(&owner.id, None).unwrap();
            assert_eq!(inbox.len(), 1);
            assert_eq!(inbox[0].text, "file changed");
            assert!(
                store
                    .list_schedules(Some(&owner.id), true)
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(tick_schedules_once(&store, &health, now + 3000), 0);
        }
    }

    #[test]
    fn spiron_schedule_owner_is_busy_while_turn_lock_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let mut owner = store
            .create_agent(NewAgent::new("owner", "spiron"))
            .unwrap();
        let now = anyagents_core::now_ms();
        for selector in [
            "spiron",
            "sylphx",
            "sylphx-agent",
            "sylphx_agent",
            "sylphx/default",
        ] {
            owner.harness = selector.into();
            assert!(!is_agent_busy(&store, &health, &owner, now));
            let lock = store.try_lock_agent(&owner.id).unwrap();
            assert!(is_agent_busy(&store, &health, &owner, now));
            drop(lock);
            assert!(!is_agent_busy(&store, &health, &owner, now));
        }
    }

    #[test]
    fn watch_schedule_enters_mailbox_while_owner_busy() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let owner = store
            .create_agent(NewAgent::new("owner-watch", "codex"))
            .unwrap();
        store.set_agent_running(&owner.id, "/work").unwrap();
        let child = store
            .create_agent(NewAgent::new("worker-sub", "opencode"))
            .unwrap();
        store.set_agent_running(&child.id, "/work").unwrap();

        create_agent_schedule(
            &store,
            &health,
            &owner,
            None,
            None,
            Some("agent:idle:worker-sub"),
            "subagent finished",
        )
        .unwrap();

        // The owner is mid-turn; the triggered watch is written to its mailbox
        // at once, where delivery steers it into the running turn.
        publish_turn(&health, &owner.id, TurnState::InTurn, None);
        let now = anyagents_core::now_ms();
        assert_eq!(tick_schedules_once(&store, &health, now), 1);
        let inbox = store.inbox(&owner.id, None).unwrap();
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].text, "subagent finished");
        assert!(
            store
                .list_schedules(Some(&owner.id), true)
                .unwrap()
                .is_empty()
        );
        assert_eq!(tick_schedules_once(&store, &health, now + 5_000), 0);
        assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 1);
    }

    #[test]
    fn every_ticks_reach_idle_busy_and_quota_blocked_owners_until_cancel() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let owner = store
            .create_agent(NewAgent::new("owner-opencode", "opencode"))
            .unwrap();
        store.set_agent_running(&owner.id, "/work").unwrap();
        let next_due = |store: &Store| {
            store.list_schedules(Some(&owner.id), true).unwrap()[0]
                .next_fire_at_ms
                .unwrap()
        };

        // Idle owner: a 1-minute schedule writes one wake when due.
        let sched = create_agent_schedule(
            &store,
            &health,
            &owner,
            None,
            Some("1m"),
            None,
            "run outcome loop",
        )
        .unwrap();
        let due = sched.next_fire_at_ms.unwrap();
        assert_eq!(tick_schedules_once(&store, &health, due - 1), 0);
        assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 0);
        assert_eq!(tick_schedules_once(&store, &health, due), 1);
        let inbox = store.inbox(&owner.id, None).unwrap();
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].text, "run outcome loop");
        assert_eq!(next_due(&store), due + 60_000);
        store.note_delivered(&owner.id, 1).unwrap();
        store.note_turn_completed(&owner.id).unwrap();

        // Busy owner: the tick is written to the mailbox for steering, exactly once.
        publish_turn(&health, &owner.id, TurnState::InTurn, None);
        let busy_due = next_due(&store);
        assert_eq!(tick_schedules_once(&store, &health, busy_due), 1);
        assert_eq!(tick_schedules_once(&store, &health, busy_due + 5_000), 0);
        let inbox = store.inbox(&owner.id, None).unwrap();
        assert_eq!(inbox.len(), 2);
        assert_eq!(inbox[1].text, "run outcome loop");
        let saved = &store.list_schedules(Some(&owner.id), true).unwrap()[0];
        assert_eq!(saved.last_observed.as_deref(), Some("fired"));
        assert_eq!(saved.next_fire_at_ms, Some(busy_due + 60_000));
        store.note_delivered(&owner.id, 2).unwrap();

        // Quota-blocked owner: the tick still enters the mailbox; delivery holds it.
        let quota_due = next_due(&store);
        publish_turn(
            &health,
            &owner.id,
            TurnState::Idle,
            Some(quota_due + 3_600_000),
        );
        assert_eq!(tick_schedules_once(&store, &health, quota_due), 1);
        assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 3);

        // Cancelled: nothing more.
        publish_turn(&health, &owner.id, TurnState::Idle, None);
        store.note_delivered(&owner.id, 3).unwrap();
        let cancelled_due = next_due(&store);
        assert_eq!(
            store.cancel_schedule(Some(&owner.id), &sched.id).unwrap(),
            1
        );
        assert_eq!(
            tick_schedules_once(&store, &health, cancelled_due + 1_000),
            0
        );
        assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 3);
    }

    #[test]
    fn long_pause_leaves_one_pending_every_tick() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let owner = store
            .create_agent(NewAgent::new("paused-owner", "codex"))
            .unwrap();
        store.set_agent_running(&owner.id, "/work").unwrap();
        let sched = create_agent_schedule(
            &store,
            &health,
            &owner,
            None,
            Some("1m"),
            None,
            "hourly check",
        )
        .unwrap();
        let due = sched.next_fire_at_ms.unwrap();
        store.set_agent_state(&owner.id, "paused").unwrap();

        // A thousand intervals pass while the owner is paused: one wake waits.
        let mut fired = 0;
        for step in 0..1_000 {
            fired += tick_schedules_once(&store, &health, due + step * 60_000);
        }
        assert_eq!(fired, 1);
        let inbox = store.inbox(&owner.id, None).unwrap();
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].text, "hourly check");
        let saved = &store.list_schedules(Some(&owner.id), true).unwrap()[0];
        assert_eq!(saved.last_observed.as_deref(), Some("coalesced"));
        assert_eq!(saved.last_fired_at_ms, Some(due));
        let resume_at = due + 999 * 60_000;
        assert_eq!(saved.next_fire_at_ms, Some(resume_at + 60_000));

        // Resume: the mailbox delivers the one pending tick, and the next slot
        // after it writes exactly one new wake.
        store.set_agent_running(&owner.id, "/work").unwrap();
        store.note_delivered(&owner.id, 1).unwrap();
        assert_eq!(tick_schedules_once(&store, &health, resume_at + 30_000), 0);
        assert_eq!(tick_schedules_once(&store, &health, resume_at + 60_000), 1);
        assert_eq!(tick_schedules_once(&store, &health, resume_at + 61_000), 0);
        assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 2);
    }

    #[test]
    fn legacy_queue_eviction_and_rate_errors_keep_existing_wake_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let owner = store
            .create_agent(NewAgent::new("legacy-pr-watch", "codex"))
            .unwrap();
        let mut sched = store
            .create_schedule(NewSchedule {
                agent_id: owner.id,
                kind: "watch".to_string(),
                spec: "github:pr:o/r#42".to_string(),
                prompt: "verify".to_string(),
                interval_ms: None,
                next_fire_at_ms: None,
                watch_baseline: Some("merge_queue:queued".to_string()),
            })
            .unwrap();
        let error = evaluate_pr_observation(&sched, "probe_error:rate_limited".to_string());
        assert!(!error.triggered);
        assert_eq!(error.baseline, sched.watch_baseline);
        let evicted = evaluate_pr_observation(&sched, "checks_completed".to_string());
        assert!(evicted.triggered);
        assert_eq!(evicted.transition.as_deref(), Some("evicted"));
        assert_eq!(evicted.next_state, "active");
        sched.watch_baseline = evicted.baseline;
        assert!(!evaluate_pr_observation(&sched, "checks_completed".to_string()).triggered);
    }

    #[test]
    fn github_pr_watch_stays_active_across_checks_and_review_failure_until_merged() {
        let _env = crate::server::tests::env_lock();
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Mutex};
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let stage = Arc::new(Mutex::new(0));
        let server_stage = Arc::clone(&stage);
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = Arc::clone(&calls);
        let server = std::thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let mut buf = [0u8; 2048];
                let len = stream.read(&mut buf).unwrap_or(0);
                let request = String::from_utf8_lossy(&buf[..len]);
                let stage = *server_stage.lock().unwrap();
                if stage == 99 {
                    break;
                }
                server_calls.fetch_add(1, Ordering::SeqCst);
                if stage == 5 {
                    let body = r#"{"message":"You have exceeded a secondary rate limit."}"#;
                    write!(stream, "HTTP/1.1 403 Forbidden\r\nRetry-After: 120\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
                    continue;
                }
                let body = if request.contains("/pulls/") {
                    if stage == 4 {
                        r#"{"state":"closed","merged":true,"head":{"sha":"abc"}}"#
                    } else {
                        r#"{"state":"open","merged":false,"head":{"sha":"abc"}}"#
                    }
                } else if request.contains("/check-runs") {
                    match stage {
                        0 => {
                            r#"{"total_count":1,"check_runs":[{"id":1,"name":"ci","status":"in_progress","conclusion":null}]}"#
                        }
                        2 => {
                            r#"{"total_count":1,"check_runs":[{"id":2,"name":"review","status":"completed","conclusion":"failure"}]}"#
                        }
                        _ => {
                            r#"{"total_count":1,"check_runs":[{"id":1,"name":"ci","status":"completed","conclusion":"success"}]}"#
                        }
                    }
                } else if request.contains("/status?") {
                    r#"{"statuses":[]}"#
                } else {
                    panic!("unexpected REST request: {request}");
                };
                write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            }
        });
        // This is the only test using this process-wide override.
        unsafe {
            std::env::set_var("ANYAGENTS_GITHUB_API_BASE", format!("http://{addr}"));
        }
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let owner = store
            .create_agent(NewAgent::new("owner-pr-watch", "codex"))
            .unwrap();
        store.set_agent_running(&owner.id, "/work").unwrap();
        let sched = create_agent_schedule(
            &store,
            &health,
            &owner,
            None,
            None,
            Some("github:pr:ExampleOrg/project#452"),
            "verify PR 452",
        )
        .unwrap();
        let now = anyagents_core::now_ms();
        github::begin_cycle(&store.list_schedules(None, true).unwrap(), now);
        let barrier = Arc::new(std::sync::Barrier::new(101));
        let handles: Vec<_> = (0..100)
            .map(|i| {
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    // Agent identity must not fragment the single-flight cache.
                    probe_github_check_terminal(
                        "ExampleOrg",
                        "project",
                        "#452",
                        &format!("agent{i}"),
                        now,
                    )
                })
            })
            .collect();
        barrier.wait();
        for handle in handles {
            assert_eq!(handle.join().unwrap().unwrap().1, "checks_in_progress");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        // Keep the manually advanced tick and subprocess completion clocks in
        // one time domain; production reads the wall clock after every probe.
        let now = anyagents_core::now_ms();
        let tick = |store: &Store, health: &HealthBoard, at| {
            github::with_completion_clock(at, || tick_schedules_once(store, health, at))
        };
        assert_eq!(tick(&store, &health, now), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        for (stage_num, transition) in [
            (1, "checks_completed"),
            (2, "review_failed"),
            (3, "checks_completed"),
        ] {
            *stage.lock().unwrap() = stage_num;
            let at = now + i64::from(stage_num) * 60_000;
            assert_eq!(tick(&store, &health, at), 1);
            let inbox = store.inbox(&owner.id, None).unwrap();
            let last = inbox.last().unwrap();
            assert!(last.text.starts_with(&format!("[watch: github:pr:ExampleOrg/project#452 transition={transition}]\nverify PR 452")), "{}", last.text);
            store.note_delivered(&owner.id, last.seq).unwrap();
            store.note_turn_completed(&owner.id).unwrap();
            assert_eq!(tick(&store, &health, at + 1_000), 0);
            let active = store.list_schedules(Some(&owner.id), true).unwrap();
            assert_eq!(active.len(), 1);
        }
        // A shared secondary-limit window records one typed error, retains
        // the successful baseline, and rereads the terminal state on recovery.
        *stage.lock().unwrap() = 5;
        assert_eq!(tick(&store, &health, now + 240_000), 0);
        let active = store.list_schedules(Some(&owner.id), true).unwrap();
        assert_eq!(
            active[0].watch_baseline.as_deref(),
            Some("checks_completed")
        );
        assert_eq!(
            active[0].last_observed.as_deref(),
            Some("probe_error:rate_limited")
        );
        let before = calls.load(Ordering::SeqCst);
        *stage.lock().unwrap() = 4;
        for at in [240_001, 300_000, 359_999, 360_000, 361_000] {
            assert_eq!(tick(&store, &health, now + at), 0);
        }
        assert_eq!(calls.load(Ordering::SeqCst), before);
        // Recovery is jittered across reset+60..300 seconds, not reset itself.
        assert_eq!(tick(&store, &health, now + 660_000), 1);
        assert_eq!(calls.load(Ordering::SeqCst), before + 1);
        let inbox = store.inbox(&owner.id, None).unwrap();
        assert!(inbox.last().unwrap().text.contains("transition=merged]"));
        assert!(
            store
                .list_schedules(Some(&owner.id), true)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store.list_schedules(Some(&owner.id), false).unwrap()[0].id,
            sched.id
        );
        *stage.lock().unwrap() = 99;
        let _ = std::net::TcpStream::connect(addr);
        server.join().unwrap();
        unsafe {
            std::env::remove_var("ANYAGENTS_GITHUB_API_BASE");
        }
    }

    #[test]
    fn selected_timer_and_watch_cannot_overwrite_a_fresh_archive() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        for kind in ["timer", "watch"] {
            let agent = store
                .create_agent(NewAgent::new(format!("owner-{kind}"), "opencode"))
                .unwrap();
            store.set_agent_state(&agent.id, "running").unwrap();
            let stale_agent = store.agent(&agent.id).unwrap();
            let watch = format!("file:{}", dir.path().join("ready").display());
            create_agent_schedule(
                &store,
                &health,
                &stale_agent,
                None,
                if kind == "timer" { Some("1m") } else { None },
                if kind == "watch" {
                    Some(watch.as_str())
                } else {
                    None
                },
                "follow-up",
            )
            .unwrap();
            store.set_agent_state(&agent.id, "archived").unwrap();
            let cfg = anyagents_core::agent_config_dir(dir.path(), &agent.id);
            std::fs::create_dir_all(&cfg).unwrap();
            std::fs::write(cfg.join(".anyagents-archived"), "archived").unwrap();
            let schedule = store
                .list_schedules(Some(&agent.id), true)
                .unwrap()
                .remove(0);
            deliver_scheduled_wake(
                &store,
                &stale_agent,
                &schedule,
                kind,
                anyagents_core::now_ms(),
            );
            assert_eq!(store.agent(&agent.id).unwrap().state, "archived");
            assert!(cfg.join(".anyagents-archived").exists());
            assert!(store.inbox(&agent.id, None).unwrap().is_empty());
        }
    }
    #[test]
    fn schedule_list_error_is_not_successful_empty() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("helper", "codex"))
            .unwrap();
        assert!(
            list_agent_schedules(&store, &agent.id, false)
                .unwrap()
                .is_empty()
        );
        let conn = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
        conn.execute_batch("ALTER TABLE agent_schedules RENAME TO unavailable_schedules")
            .unwrap();
        assert!(
            list_agent_schedules(&store, &agent.id, false)
                .unwrap_err()
                .contains("agent_schedules")
        );
    }
    #[test]
    fn unreadable_turn_state_never_proves_idle() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("helper", "codex"))
            .unwrap();
        let health = HealthBoard::default();
        assert!(!is_agent_busy(&store, &health, &agent, 1));
        let conn = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
        conn.execute_batch("ALTER TABLE turn_recovery RENAME TO unavailable_turns")
            .unwrap();
        assert!(is_agent_busy(&store, &health, &agent, 1));
        assert_eq!(
            agent_wake_block_reason(&store, &health, &agent, 1),
            Some("busy")
        );
        conn.execute_batch("ALTER TABLE unavailable_turns RENAME TO turn_recovery")
            .unwrap();
        assert!(!is_agent_busy(&store, &health, &agent, 1));
        conn.execute_batch("ALTER TABLE messages RENAME TO unavailable_messages")
            .unwrap();
        assert!(is_agent_busy(&store, &health, &agent, 1));
        conn.execute_batch("ALTER TABLE unavailable_messages RENAME TO messages")
            .unwrap();
        assert!(!is_agent_busy(&store, &health, &agent, 1));
    }
    #[test]
    fn spiron_failed_observations_are_typed_and_no_auth_fallback() {
        use std::io::{Read, Write};
        for (statuses, body, expected) in [
            (vec![200], r#"{"state":"pending"}"#, Ok("pending".into())),
            (vec![200], r#"{"state":"complete"}"#, Ok("complete".into())),
            (vec![401], "{}", Err(SpironProbeError::Auth)),
            (vec![404, 404], "{}", Err(SpironProbeError::NotFound)),
            (vec![200], "not json", Err(SpironProbeError::Payload)),
            (vec![200], "{}", Err(SpironProbeError::Payload)),
            (vec![503], "{}", Err(SpironProbeError::Http)),
            (
                vec![404, 200],
                r#"{"status":"running"}"#,
                Ok("running".into()),
            ),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let body = body.to_string();
            let task = std::thread::spawn(move || {
                for status in statuses {
                    let (mut socket, _) = listener.accept().unwrap();
                    socket
                        .set_read_timeout(Some(Duration::from_secs(3)))
                        .unwrap();
                    let mut bytes = Vec::new();
                    while !bytes.ends_with(b"\r\n\r\n") {
                        let mut b = [0];
                        socket.read_exact(&mut b).unwrap();
                        bytes.push(b[0]);
                    }
                    write!(socket,"HTTP/1.1 {status} Synthetic\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).unwrap();
                }
            });
            assert_eq!(probe_spiron_at(&base, None, "id", "promise"), expected);
            task.join().unwrap();
        }
    }

    #[test]
    fn failed_wake_insert_rolls_back_timer_claim_and_retries_once() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("helper", "codex"))
            .unwrap();
        let health = HealthBoard::default();
        let now = anyagents_core::now_ms();
        let schedule = store
            .create_schedule(NewSchedule {
                agent_id: agent.id.clone(),
                kind: "at".into(),
                spec: "now".into(),
                prompt: "one wake".into(),
                interval_ms: None,
                next_fire_at_ms: Some(now),
                watch_baseline: None,
            })
            .unwrap();
        let conn = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
        conn.execute_batch("CREATE TRIGGER reject_wake BEFORE INSERT ON messages BEGIN SELECT RAISE(ABORT, 'wake insert failure'); END;").unwrap();
        assert_eq!(tick_schedules_once(&store, &health, now), 0);
        let retained = store.get_schedule(&schedule.id).unwrap().unwrap();
        assert_eq!(retained.state, "active");
        assert_eq!(retained.last_fired_at_ms, None);
        assert!(store.inbox(&agent.id, None).unwrap().is_empty());
        conn.execute_batch("DROP TRIGGER reject_wake").unwrap();
        assert_eq!(tick_schedules_once(&store, &health, now + 1), 1);
        assert_eq!(
            store.get_schedule(&schedule.id).unwrap().unwrap().state,
            "fired"
        );
        assert_eq!(store.inbox(&agent.id, None).unwrap().len(), 1);
        assert_eq!(tick_schedules_once(&store, &health, now + 2), 0);
        assert_eq!(store.inbox(&agent.id, None).unwrap().len(), 1);
    }
    #[test]
    fn spiron_probe_recovery_is_not_a_fabricated_state_change() {
        for baseline in [
            None,
            Some("probe_error:auth"),
            Some("probe_error:not_found"),
        ] {
            let pending = evaluate_spiron_observation(baseline, "pending".into());
            assert!(!pending.triggered);
            assert_eq!(pending.baseline.as_deref(), Some("pending"));
            assert!(evaluate_spiron_observation(baseline, "completed".into()).triggered);
        }
        assert!(!evaluate_spiron_observation(Some("pending"), "pending".into()).triggered);
        assert!(evaluate_spiron_observation(Some("running"), "completed".into()).triggered);
    }

    #[test]
    fn held_lifecycle_lock_defers_only_that_agent_without_blocking_other_schedules() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let blocked = store
            .create_agent(NewAgent::new("blocked-owner", "codex"))
            .unwrap();
        let ready = store
            .create_agent(NewAgent::new("ready-owner", "codex"))
            .unwrap();
        let idle_target = store
            .create_agent(NewAgent::new("idle-target", "codex"))
            .unwrap();
        let now = anyagents_core::now_ms();
        let blocked_timer = store
            .create_schedule(NewSchedule {
                agent_id: blocked.id.clone(),
                kind: "at".into(),
                spec: "now".into(),
                prompt: "blocked timer".into(),
                interval_ms: None,
                next_fire_at_ms: Some(now),
                watch_baseline: None,
            })
            .unwrap();
        let blocked_watch = store
            .create_schedule(NewSchedule {
                agent_id: blocked.id.clone(),
                kind: "watch".into(),
                spec: format!("agent:idle:{}", idle_target.name),
                prompt: "blocked watch".into(),
                interval_ms: None,
                next_fire_at_ms: None,
                watch_baseline: None,
            })
            .unwrap();
        let ready_timer = store
            .create_schedule(NewSchedule {
                agent_id: ready.id.clone(),
                kind: "at".into(),
                spec: "now".into(),
                prompt: "ready timer".into(),
                interval_ms: None,
                next_fire_at_ms: Some(now),
                watch_baseline: None,
            })
            .unwrap();
        let ready_watch = store
            .create_schedule(NewSchedule {
                agent_id: ready.id.clone(),
                kind: "watch".into(),
                spec: format!("agent:idle:{}", idle_target.name),
                prompt: "ready watch".into(),
                interval_ms: None,
                next_fire_at_ms: None,
                watch_baseline: None,
            })
            .unwrap();

        let held = store.lock_agent_lifecycle(&blocked.id).unwrap();
        assert_eq!(tick_schedules_once(&store, &health, now), 2);
        assert!(store.inbox(&blocked.id, None).unwrap().is_empty());
        assert_eq!(
            store
                .get_schedule(&blocked_timer.id)
                .unwrap()
                .unwrap()
                .state,
            "active"
        );
        assert_eq!(
            store
                .get_schedule(&blocked_watch.id)
                .unwrap()
                .unwrap()
                .state,
            "active"
        );
        assert_eq!(store.inbox(&ready.id, None).unwrap().len(), 2);
        assert_eq!(
            store.get_schedule(&ready_timer.id).unwrap().unwrap().state,
            "fired"
        );
        assert_eq!(
            store.get_schedule(&ready_watch.id).unwrap().unwrap().state,
            "fired"
        );

        drop(held);
        assert_eq!(tick_schedules_once(&store, &health, now + 1), 2);
        assert_eq!(store.inbox(&blocked.id, None).unwrap().len(), 2);
        assert_eq!(
            store
                .get_schedule(&blocked_timer.id)
                .unwrap()
                .unwrap()
                .state,
            "fired"
        );
        assert_eq!(
            store
                .get_schedule(&blocked_watch.id)
                .unwrap()
                .unwrap()
                .state,
            "fired"
        );
    }

    #[test]
    fn task_subagent_turn_ownership_nudges_silent_child_and_reminds_parent_on_waiting_child() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let mut parent_new = NewAgent::new("parent-dir", "agy");
        parent_new.role = Some("director".to_string());
        let parent = store.create_agent(parent_new).unwrap();
        let mut child_new = NewAgent::new("child-eng", "opencode");
        child_new.role = Some("engineer".to_string());
        child_new.parent_id = Some(parent.id.clone());
        let child = store.create_agent(child_new).unwrap();
        let mut assistant_new = NewAgent::new("child-asst", "agy");
        assistant_new.role = Some("engineer".to_string());
        assistant_new.parent_id = Some(parent.id.clone());
        let assistant = store.create_agent(assistant_new).unwrap();
        store
            .set_agent_lifecycle(&assistant.id, "standing")
            .unwrap();

        // Deliver a task instruction from parent to both child (task) and assistant (standing)
        let mut task_msg = Incoming::new("Execute task");
        task_msg.sender = Some(format!("agent:{}", parent.name));
        store.receive(&child.id, task_msg.clone()).unwrap();
        store.note_delivered(&child.id, 1).unwrap();
        store.note_turn_completed(&child.id).unwrap();

        store.receive(&assistant.id, task_msg).unwrap();
        store.note_delivered(&assistant.id, 1).unwrap();
        store.note_turn_completed(&assistant.id).unwrap();

        let snap = store.turn_ownership_snapshot(&child.id).unwrap();
        let delivered_ms = snap.last_delivered_at_ms.unwrap();

        // Rule A: after 20s without calling `send`, task child is nudged once; standing assistant is not.
        assert_eq!(
            tick_task_subagent_turn_ownership(&store, &health, delivered_ms + 20_000),
            1
        );
        assert_eq!(
            tick_task_subagent_turn_ownership(&store, &health, delivered_ms + 25_000),
            0
        );
        let child_inbox = store.inbox(&child.id, None).unwrap();
        assert_eq!(child_inbox.len(), 2);
        assert!(child_inbox[1].text.contains("[Turn Ownership Guard]"));
        assert_eq!(store.inbox(&assistant.id, None).unwrap().len(), 1);

        // Child consumes the nudge and calls `send` (`record_outbound`) to parent
        store.note_delivered(&child.id, 2).unwrap();
        store.note_turn_completed(&child.id).unwrap();
        store
            .record_outbound(&child.id, "agent:parent-dir", None, "Task completed")
            .unwrap();
        let out_ms = store
            .turn_ownership_snapshot(&child.id)
            .unwrap()
            .last_outbound_at_ms
            .unwrap();

        // Rule B: after 15m of child waiting_parent while parent is idle, parent receives 1 consolidated reminder.
        assert_eq!(
            tick_task_subagent_turn_ownership(&store, &health, out_ms + 901_000),
            1
        );
        assert_eq!(
            tick_task_subagent_turn_ownership(&store, &health, out_ms + 950_000),
            0
        );
        let parent_inbox = store.inbox(&parent.id, None).unwrap();
        assert_eq!(parent_inbox.len(), 1);
        assert!(parent_inbox[0].text.contains("waiting_parent"));
        assert!(parent_inbox[0].text.contains("`child-eng` (`engineer`)"));
    }
}
