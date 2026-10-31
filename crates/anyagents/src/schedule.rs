//! Per-agent self-scheduled wakes (`at`, `every`) and event watches (`watch`).
//!
//! Every agent on any harness can schedule its own one-off wake (`at`), recurring
//! wake (`every`), or event watch (`watch`: GitHub PR/check terminal state, Spiron
//! promise state change pulled from the public API, or an AnyAgents agent going idle).
//!
//! Wakes fire only when the owning agent is idle and not quota-blocked.
//! A missed wake never fires twice, and cancelling a schedule takes effect immediately.

use std::time::Duration;

use anyagents_core::{Agent, AgentSchedule, Incoming, NewSchedule, Store};
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
    let mut chars = cleaned.chars().peekable();
    while let Some(c) = chars.next() {
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
    if let Some(path_str) = s
        .strip_prefix("file:")
        .or_else(|| s.strip_prefix("path:"))
    {
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
        let parsed = parse_watch_spec(watch_str)?;
        let baseline = initial_watch_baseline(store, health, &parsed, now);
        NewSchedule {
            agent_id: agent.id.clone(),
            kind: "watch".to_string(),
            spec: watch_str.to_string(),
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
) -> Vec<ScheduleView> {
    store
        .list_schedules(Some(agent_id), active_only)
        .unwrap_or_default()
        .into_iter()
        .map(ScheduleView::from)
        .collect()
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
    now: i64,
) -> Option<String> {
    match target {
        WatchTarget::SpironPromise { id, kind } => probe_spiron_state(id, kind),
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
        WatchTarget::GitHubPr { .. } | WatchTarget::GitHubCheck { .. } => {
            Some("pending".to_string())
        }
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
    if store.has_open_turn(&agent.id).unwrap_or(false) {
        return true;
    }
    if let Ok(lag) = store.lag(&agent.id, now)
        && lag.oldest_undelivered_age_ms.is_some()
    {
        return true;
    }
    if let Some(live) = health.get(&agent.id)
        && live.process_alive
        && live.turn_state == anyagents_harness::TurnState::InTurn
    {
        return true;
    }
    false
}

/// Evaluate all active schedules once at timestamp `now`.
/// Returns the number of wakes delivered.
pub fn tick_schedules_once(store: &Store, health: &HealthBoard, now: i64) -> usize {
    let Ok(schedules) = store.list_schedules(None, true) else {
        return 0;
    };
    let mut fired_count = 0;
    for sched in schedules {
        let Ok(agent) = store.agent(&sched.agent_id) else {
            let _ = store.cancel_schedule(None, &sched.id);
            continue;
        };
        let block = agent_wake_block_reason(store, health, &agent, now);
        match sched.kind.as_str() {
            "at" => {
                let Some(due) = sched.next_fire_at_ms else {
                    continue;
                };
                if now < due {
                    continue;
                }
                if let Some(reason) = block {
                    // Missed wake while busy/quota-blocked/paused: consume once, never fire twice.
                    let _ = store.claim_schedule_tick(
                        &sched.id,
                        Some(due),
                        "fired",
                        None,
                        None,
                        None,
                        Some(&format!("skipped:{reason}")),
                    );
                    continue;
                }
                if store
                    .claim_schedule_tick(
                        &sched.id,
                        Some(due),
                        "fired",
                        None,
                        Some(now),
                        None,
                        Some("fired"),
                    )
                    .unwrap_or(false)
                {
                    deliver_scheduled_wake(
                        store,
                        &agent,
                        &sched,
                        &format!("schedule:{}:{}", sched.id, due),
                        now,
                    );
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
                if let Some(reason) = block {
                    // Advance next_fire_at_ms past `now` without firing so missed ticks never queue or double-fire.
                    let _ = store.claim_schedule_tick(
                        &sched.id,
                        Some(due),
                        "active",
                        Some(next_due),
                        None,
                        None,
                        Some(&format!("skipped:{reason}")),
                    );
                    continue;
                }
                if store
                    .claim_schedule_tick(
                        &sched.id,
                        Some(due),
                        "active",
                        Some(next_due),
                        Some(now),
                        None,
                        Some("fired"),
                    )
                    .unwrap_or(false)
                {
                    deliver_scheduled_wake(
                        store,
                        &agent,
                        &sched,
                        &format!("schedule:{}:{}", sched.id, due),
                        now,
                    );
                    fired_count += 1;
                }
            }
            "watch" => {
                if block.is_some() {
                    // Defer evaluating one-shot event watches while the owning agent is busy,
                    // paused, or quota-blocked so external APIs are not polled needlessly and
                    // terminal events are delivered as soon as the agent becomes idle.
                    continue;
                }
                let eval = evaluate_watch(store, health, &sched, now);
                if !eval.triggered {
                    if eval.baseline.is_some() || eval.observed.is_some() {
                        let _ = store.update_schedule_watch_observation(
                            &sched.id,
                            eval.baseline.as_deref(),
                            eval.observed.as_deref(),
                        );
                    }
                    continue;
                }
                if store
                    .claim_schedule_tick(
                        &sched.id,
                        None,
                        "fired",
                        None,
                        Some(now),
                        eval.baseline.as_deref(),
                        eval.observed.as_deref(),
                    )
                    .unwrap_or(false)
                {
                    deliver_scheduled_wake(
                        store,
                        &agent,
                        &sched,
                        &format!("schedule:{}:watch", sched.id),
                        now,
                    );
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
) {
    if agent.state != "running" && agent.state != "paused" {
        let _ = store.set_agent_state(&agent.id, "running");
    }
    let _ = store.receive(
        &agent.id,
        Incoming {
            text: sched.prompt.clone(),
            sender: Some(format!("schedule:{}", sched.id)),
            source_time_ms: Some(now),
            source_key: Some(source_key.to_string()),
            reply_to: None,
            quote: None,
            origin: None,
            received_at_ms: Some(now),
        },
    );
    crate::create::poke_agent_wake_socket(store.data_dir(), &agent.id);
    crate::create::poke_agent_wake_socket_mail(store.data_dir(), &agent.id);
}

struct WatchEval {
    triggered: bool,
    observed: Option<String>,
    baseline: Option<String>,
}

fn evaluate_watch(
    store: &Store,
    health: &HealthBoard,
    sched: &AgentSchedule,
    now: i64,
) -> WatchEval {
    let Ok(target) = parse_watch_spec(&sched.spec) else {
        return WatchEval {
            triggered: false,
            observed: Some("invalid_spec".to_string()),
            baseline: sched.watch_baseline.clone(),
        };
    };
    match target {
        WatchTarget::AgentIdle { target } => {
            let Ok(found) = store.agent_by_name(&target) else {
                return WatchEval {
                    triggered: false,
                    observed: Some("not_found".to_string()),
                    baseline: sched.watch_baseline.clone(),
                };
            };
            if is_agent_busy(store, health, &found, now) {
                WatchEval {
                    triggered: false,
                    observed: Some("working".to_string()),
                    baseline: Some("working".to_string()),
                }
            } else {
                WatchEval {
                    triggered: true,
                    observed: Some("idle".to_string()),
                    baseline: sched.watch_baseline.clone(),
                }
            }
        }
        WatchTarget::SpironPromise { id, kind } => {
            let Some(current) = probe_spiron_state(&id, &kind) else {
                return WatchEval {
                    triggered: false,
                    observed: None,
                    baseline: sched.watch_baseline.clone(),
                };
            };
            match &sched.watch_baseline {
                Some(base) => {
                    if &current != base {
                        WatchEval {
                            triggered: true,
                            observed: Some(format!("{base}->{current}")),
                            baseline: Some(base.clone()),
                        }
                    } else {
                        WatchEval {
                            triggered: false,
                            observed: Some(current),
                            baseline: Some(base.clone()),
                        }
                    }
                }
                None => {
                    if is_spiron_terminal(&current) {
                        WatchEval {
                            triggered: true,
                            observed: Some(current.clone()),
                            baseline: Some(current),
                        }
                    } else {
                        WatchEval {
                            triggered: false,
                            observed: Some(current.clone()),
                            baseline: Some(current),
                        }
                    }
                }
            }
        }
        WatchTarget::GitHubPr {
            owner,
            repo,
            number,
        } => {
            let Some((is_terminal, state_desc)) =
                probe_github_pr_terminal(&owner, &repo, number, sched.agent_id.as_str(), now)
            else {
                return WatchEval {
                    triggered: false,
                    observed: None,
                    baseline: sched.watch_baseline.clone(),
                };
            };
            WatchEval {
                triggered: is_terminal,
                observed: Some(state_desc),
                baseline: sched.watch_baseline.clone(),
            }
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
                return WatchEval {
                    triggered: false,
                    observed: None,
                    baseline: sched.watch_baseline.clone(),
                };
            };
            WatchEval {
                triggered: is_terminal,
                observed: Some(state_desc),
                baseline: sched.watch_baseline.clone(),
            }
        }
        WatchTarget::ProcessExit { pid } => {
            if probe_process_alive(pid) {
                WatchEval {
                    triggered: false,
                    observed: Some("running".to_string()),
                    baseline: Some("running".to_string()),
                }
            } else {
                WatchEval {
                    triggered: true,
                    observed: Some("exited".to_string()),
                    baseline: sched.watch_baseline.clone(),
                }
            }
        }
        WatchTarget::PathChanged { path } => {
            let current = probe_path_fingerprint(&path);
            match &sched.watch_baseline {
                Some(base) => {
                    if &current != base {
                        WatchEval {
                            triggered: true,
                            observed: Some(format!("{base}->{current}")),
                            baseline: Some(base.clone()),
                        }
                    } else {
                        WatchEval {
                            triggered: false,
                            observed: Some(current),
                            baseline: Some(base.clone()),
                        }
                    }
                }
                None => {
                    if current != "missing" {
                        WatchEval {
                            triggered: true,
                            observed: Some(current.clone()),
                            baseline: Some(current),
                        }
                    } else {
                        WatchEval {
                            triggered: false,
                            observed: Some(current.clone()),
                            baseline: Some(current),
                        }
                    }
                }
            }
        }
    }
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
        Err(_) => "missing".to_string(),
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
    let key_file = std::env::var("SPIRON_KEY_FILE")
        .unwrap_or_else(|_| "/scratch/secrets/spiron/desk-ops.key".to_string());
    std::fs::read_to_string(key_file)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Pull a Spiron promise or task state from the Spiron public API (`GET /v1/promises/{id}` or `/v1/tasks/{id}`).
pub fn probe_spiron_state(id: &str, kind: &str) -> Option<String> {
    let base =
        std::env::var("SPIRON_API_BASE").unwrap_or_else(|_| "https://api.spiron.ai".to_string());
    let base = base.trim_end_matches('/');
    let key = spiron_api_key();
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(5)))
        .max_redirects(0)
        .build()
        .new_agent();

    let paths = if kind == "task" {
        [
            format!("{base}/v1/tasks/{id}"),
            format!("{base}/v1/promises/{id}"),
        ]
    } else {
        [
            format!("{base}/v1/promises/{id}"),
            format!("{base}/v1/tasks/{id}"),
        ]
    };

    for url in paths {
        let mut req = agent.get(&url).header("accept", "application/json");
        if let Some(ref token) = key {
            req = req.header("authorization", &format!("Bearer {token}"));
        }
        if let Ok(resp) = req.call()
            && let Ok(body) = resp.into_body().read_to_string()
            && let Ok(val) = serde_json::from_str::<serde_json::Value>(&body)
            && let Some(st) = val
                .get("state")
                .or_else(|| val.get("status"))
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
        {
            return Some(st.to_string());
        }
    }
    None
}

static GH_PROBE_CACHE: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, (i64, Option<(bool, String)>)>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

const GH_CLI_PROBE_TTL_MS: i64 = 15_000;

fn probe_github_pr_terminal(
    owner: &str,
    repo: &str,
    number: u64,
    agent_id: &str,
    now_ms: i64,
) -> Option<(bool, String)> {
    if let Ok(base) = std::env::var("ANYAGENTS_GITHUB_API_BASE")
        && !base.trim().is_empty()
    {
        let url = format!(
            "{}/repos/{owner}/{repo}/pulls/{number}",
            base.trim_end_matches('/')
        );
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(5)))
            .build()
            .new_agent();
        if let Ok(resp) = agent.get(&url).header("accept", "application/json").call()
            && let Ok(body) = resp.into_body().read_to_string()
            && let Ok(val) = serde_json::from_str::<serde_json::Value>(&body)
        {
            return evaluate_pr_json(&val);
        }
        return None;
    }

    let cache_key = format!("pr:{owner}/{repo}#{number}");
    if let Ok(guard) = GH_PROBE_CACHE.lock()
        && let Some((ts, cached)) = guard.get(&cache_key)
        && now_ms.saturating_sub(*ts) < GH_CLI_PROBE_TTL_MS
    {
        return cached.clone();
    }

    let out = std::process::Command::new("gh")
        .env("ANYAGENTS_AGENT_ID", agent_id)
        .args([
            "pr",
            "view",
            &number.to_string(),
            "--repo",
            &format!("{owner}/{repo}"),
            "--json",
            "state,mergedAt,statusCheckRollup",
        ])
        .output()
        .ok()?;
    let res = if out.status.success() {
        serde_json::from_slice::<serde_json::Value>(&out.stdout)
            .ok()
            .and_then(|val| evaluate_pr_json(&val))
    } else {
        None
    };
    if let Ok(mut guard) = GH_PROBE_CACHE.lock() {
        guard.insert(cache_key, (now_ms, res.clone()));
    }
    res
}

fn evaluate_pr_json(val: &serde_json::Value) -> Option<(bool, String)> {
    let state = val
        .get("state")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_ascii_uppercase();
    let merged = val
        .get("merged")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
        || val
            .get("mergedAt")
            .is_some_and(|v| !v.is_null() && v.as_str() != Some(""));
    if merged || state == "MERGED" {
        return Some((true, "merged".to_string()));
    }
    if state == "CLOSED" {
        return Some((true, "closed".to_string()));
    }
    if let Some(rollup) = val
        .get("statusCheckRollup")
        .and_then(serde_json::Value::as_array)
        && !rollup.is_empty()
    {
        let all_done = rollup.iter().all(check_item_is_terminal);
        if all_done {
            return Some((true, "checks_completed".to_string()));
        }
        return Some((false, "checks_in_progress".to_string()));
    }
    Some((false, state.to_ascii_lowercase()))
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
    if let Ok(base) = std::env::var("ANYAGENTS_GITHUB_API_BASE")
        && !base.trim().is_empty()
    {
        let url = format!(
            "{}/repos/{owner}/{repo}/commits/{clean_ref}/check-runs",
            base.trim_end_matches('/')
        );
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(5)))
            .build()
            .new_agent();
        if let Ok(resp) = agent.get(&url).header("accept", "application/json").call()
            && let Ok(body) = resp.into_body().read_to_string()
            && let Ok(val) = serde_json::from_str::<serde_json::Value>(&body)
        {
            return evaluate_check_runs_json(&val);
        }
        return None;
    }

    let cache_key = format!("check:{owner}/{repo}@{clean_ref}");
    if let Ok(guard) = GH_PROBE_CACHE.lock()
        && let Some((ts, cached)) = guard.get(&cache_key)
        && now_ms.saturating_sub(*ts) < GH_CLI_PROBE_TTL_MS
    {
        return cached.clone();
    }

    let out = std::process::Command::new("gh")
        .env("ANYAGENTS_AGENT_ID", agent_id)
        .args([
            "api",
            &format!("repos/{owner}/{repo}/commits/{clean_ref}/check-runs"),
        ])
        .output()
        .ok()?;
    let res = if out.status.success() {
        serde_json::from_slice::<serde_json::Value>(&out.stdout)
            .ok()
            .and_then(|val| evaluate_check_runs_json(&val))
    } else {
        None
    };
    if let Ok(mut guard) = GH_PROBE_CACHE.lock() {
        guard.insert(cache_key, (now_ms, res.clone()));
    }
    res
}

fn evaluate_check_runs_json(val: &serde_json::Value) -> Option<(bool, String)> {
    if let Some(status) = val.get("status").and_then(serde_json::Value::as_str) {
        let done = status.eq_ignore_ascii_case("completed");
        return Some((done, status.to_ascii_lowercase()));
    }
    let runs = val
        .get("check_runs")
        .and_then(serde_json::Value::as_array)?;
    if runs.is_empty() {
        return Some((false, "no_checks_yet".to_string()));
    }
    let all_done = runs.iter().all(|r| {
        r.get("status")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|s| s.eq_ignore_ascii_case("completed"))
    });
    if all_done {
        Some((true, "checks_completed".to_string()))
    } else {
        Some((false, "checks_in_progress".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_core::NewAgent;
    use anyagents_harness::{DeliveryMode, SteerSupport, TurnState};
    use anyagents_supervisor::LiveState;

    #[test]
    fn parses_durations_and_watch_specs() {
        assert_eq!(parse_duration_ms("1m").unwrap(), 60_000);
        assert_eq!(parse_duration_ms("+30s").unwrap(), 30_000);
        assert_eq!(parse_duration_ms("2h").unwrap(), 7_200_000);
        assert_eq!(parse_duration_ms("daily").unwrap(), 86_400_000);

        assert_eq!(
            parse_watch_spec("github:pr:SylphxAI/anyagents#41").unwrap(),
            WatchTarget::GitHubPr {
                owner: "SylphxAI".to_string(),
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
    fn watch_schedule_defers_while_owner_busy_and_fires_when_idle() {
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

        let _sched = create_agent_schedule(
            &store,
            &health,
            &owner,
            None,
            None,
            Some("agent:idle:worker-sub"),
            "subagent finished",
        )
        .unwrap();

        // While owner is busy (InTurn), even though worker-sub is idle, the one-shot watch
        // is deferred rather than swallowed as `skipped:busy`.
        health.publish(
            &owner.id,
            LiveState {
                process_alive: true,
                turn_state: TurnState::InTurn,
                delivery_mode: DeliveryMode::Steer,
                steer_support: SteerSupport::Yes,
                quota_reset_at_ms: None,
                restart_at_ms: None,
                pid: None,
                live_harness: Some("codex".to_string()),
                live_model: None,
                process_started_at_ms: Some(1000),
                last_error: None,
            },
        );
        let now = anyagents_core::now_ms();
        assert_eq!(tick_schedules_once(&store, &health, now), 0);
        assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 0);
        let active = store.list_schedules(Some(&owner.id), true).unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].state, "active");

        // Once owner finishes its turn and is Idle, the watch fires and wakes the owner.
        health.publish(
            &owner.id,
            LiveState {
                process_alive: true,
                turn_state: TurnState::Idle,
                delivery_mode: DeliveryMode::Steer,
                steer_support: SteerSupport::Yes,
                quota_reset_at_ms: None,
                restart_at_ms: None,
                pid: None,
                live_harness: Some("codex".to_string()),
                live_model: None,
                process_started_at_ms: Some(1000),
                last_error: None,
            },
        );
        assert_eq!(tick_schedules_once(&store, &health, now + 5_000), 1);
        let inbox = store.inbox(&owner.id, None).unwrap();
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].text, "subagent finished");
    }

    #[test]
    fn positive_and_negative_schedule_wakes_respect_busy_quota_and_cancel() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let owner = store
            .create_agent(NewAgent::new("owner-opencode", "opencode"))
            .unwrap();
        store.set_agent_running(&owner.id, "/work").unwrap();

        // 1. Positive test: idle OpenCode owner schedules a 1-minute wake and receives 1 prompt.
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

        // Consume turn 1 so owner is idle again
        store.note_delivered(&owner.id, 1).unwrap();
        store.note_turn_completed(&owner.id).unwrap();

        // 2. Negative test: busy owner gets NO wake, and missed wake never fires twice.
        let next_due = store.list_schedules(Some(&owner.id), true).unwrap()[0]
            .next_fire_at_ms
            .unwrap();
        health.publish(
            &owner.id,
            LiveState {
                process_alive: true,
                turn_state: TurnState::InTurn,
                delivery_mode: DeliveryMode::Steer,
                steer_support: SteerSupport::Yes,
                quota_reset_at_ms: None,
                restart_at_ms: None,
                pid: None,
                live_harness: Some("opencode".to_string()),
                live_model: None,
                process_started_at_ms: Some(1000),
                last_error: None,
            },
        );
        assert_eq!(tick_schedules_once(&store, &health, next_due), 0);
        assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 1);

        // Even when owner becomes idle 5s later, the missed wake does NOT fire late.
        health.publish(
            &owner.id,
            LiveState {
                process_alive: true,
                turn_state: TurnState::Idle,
                delivery_mode: DeliveryMode::Steer,
                steer_support: SteerSupport::Yes,
                quota_reset_at_ms: None,
                restart_at_ms: None,
                pid: None,
                live_harness: Some("opencode".to_string()),
                live_model: None,
                process_started_at_ms: Some(1000),
                last_error: None,
            },
        );
        assert_eq!(tick_schedules_once(&store, &health, next_due + 5_000), 0);
        assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 1);

        // 3. Negative test: quota-blocked owner gets NO wake.
        let third_due = store.list_schedules(Some(&owner.id), true).unwrap()[0]
            .next_fire_at_ms
            .unwrap();
        health.publish(
            &owner.id,
            LiveState {
                process_alive: true,
                turn_state: TurnState::Idle,
                delivery_mode: DeliveryMode::Steer,
                steer_support: SteerSupport::Yes,
                quota_reset_at_ms: Some(third_due + 3_600_000),
                restart_at_ms: None,
                pid: None,
                live_harness: Some("opencode".to_string()),
                live_model: None,
                process_started_at_ms: Some(1000),
                last_error: Some("quota exceeded".to_string()),
            },
        );
        assert_eq!(tick_schedules_once(&store, &health, third_due), 0);
        assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 1);

        // 4. Negative test: none after cancel.
        health.publish(
            &owner.id,
            LiveState {
                process_alive: true,
                turn_state: TurnState::Idle,
                delivery_mode: DeliveryMode::Steer,
                steer_support: SteerSupport::Yes,
                quota_reset_at_ms: None,
                restart_at_ms: None,
                pid: None,
                live_harness: Some("opencode".to_string()),
                live_model: None,
                process_started_at_ms: Some(1000),
                last_error: None,
            },
        );
        let fourth_due = store.list_schedules(Some(&owner.id), true).unwrap()[0]
            .next_fire_at_ms
            .unwrap();
        assert_eq!(
            store.cancel_schedule(Some(&owner.id), &sched.id).unwrap(),
            1
        );
        assert_eq!(tick_schedules_once(&store, &health, fourth_due + 1_000), 0);
        assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 1);
    }
}
