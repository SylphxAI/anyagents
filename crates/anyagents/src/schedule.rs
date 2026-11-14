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
//!
//! The server's timers ([`start_loops`]) hold the active schedules in memory and
//! sleep until the earliest due time, a watch's next probe, or a schedule change,
//! reading nothing in between. Watches with no change event (a file, a process,
//! an agent going idle, a remote state) are probed at their own interval.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use anyagents_core::{
    Agent, AgentId, AgentSchedule, Incoming, NewOutcome, NewSchedule, OutcomeKind, Store,
};
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

/// One correct call, quoted in every schedule error.
const SCHEDULE_EXAMPLE: &str = "`{action: \"create\", watch: \"github:pr:owner/repo#12\", prompt: \"PR 12 finished; review it\"}`";

/// What a schedule call does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScheduleVerb {
    List,
    Create,
    Cancel,
}

/// The trigger fields of a schedule call, as the model wrote them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ScheduleTrigger {
    pub at: Option<String>,
    pub every: Option<String>,
    pub watch: Option<String>,
    /// A trigger value under a generic name (`spec`, `kind`): the field the
    /// action names takes it.
    pub spec: Option<String>,
}

/// Fold the shapes models write into one verb: `action` naming the trigger
/// (`{action: "watch", watch}`, `{action: "at", spec}`), a mode under `kind`
/// (`{kind: "watch", watch}`), and a bare watch spec under `spec`.
pub(crate) fn resolve_schedule_call(
    action: Option<&str>,
    trigger: &mut ScheduleTrigger,
    has_id: bool,
) -> Result<ScheduleVerb, String> {
    let mut action = action
        .map(str::trim)
        .filter(|a| !a.is_empty())
        .map(str::to_ascii_lowercase);
    let spec = trigger
        .spec
        .take()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let mut spec = match spec {
        Some(mode) if matches!(mode.as_str(), "at" | "every" | "watch") => {
            if action.as_deref().is_none_or(is_create_word) {
                action = Some(mode);
            }
            None
        }
        other => other,
    };
    if let Some(mode @ ("at" | "every" | "watch")) = action.as_deref() {
        let slot = match mode {
            "at" => &mut trigger.at,
            "every" => &mut trigger.every,
            _ => &mut trigger.watch,
        };
        if slot.is_none() {
            *slot = spec.take();
        }
        if slot.is_none() {
            return Err(format!(
                "`action: \"{mode}\"` needs the `{mode}` value, e.g. {SCHEDULE_EXAMPLE}"
            ));
        }
        return Ok(ScheduleVerb::Create);
    }
    let has_trigger = trigger.at.is_some() || trigger.every.is_some() || trigger.watch.is_some();
    if !has_trigger && let Some(value) = spec {
        if !is_watch_spec(&value) {
            return Err(format!(
                "`spec: \"{value}\"` does not say which trigger it is; pass it as `at` (\"20m\" or an ISO time), `every` (\"15m\") or `watch`, e.g. {SCHEDULE_EXAMPLE}"
            ));
        }
        trigger.watch = Some(value);
    }
    let has_trigger = trigger.at.is_some() || trigger.every.is_some() || trigger.watch.is_some();
    match action.as_deref() {
        None if has_trigger => Ok(ScheduleVerb::Create),
        None if has_id => Ok(ScheduleVerb::Cancel),
        None => Ok(ScheduleVerb::List),
        Some(word) if is_create_word(word) => Ok(ScheduleVerb::Create),
        Some("list" | "ls" | "get" | "show" | "inspect" | "status") => Ok(ScheduleVerb::List),
        Some("cancel" | "delete" | "remove" | "clear" | "off" | "pause" | "disable" | "stop") => {
            Ok(ScheduleVerb::Cancel)
        }
        Some(other) => Err(format!(
            "unknown schedule action `{other}`. Use {SCHEDULE_EXAMPLE} (exactly one of `at`, `every`, `watch`), `{{action: \"list\"}}`, or `{{action: \"cancel\", id: \"sch_...\" | \"all\"}}`"
        )),
    }
}

fn is_create_word(word: &str) -> bool {
    matches!(word, "create" | "set" | "add" | "schedule" | "new")
}

fn is_watch_spec(value: &str) -> bool {
    [
        "github:", "spiron:", "agent:", "pid:", "process:", "file:", "path:",
    ]
    .iter()
    .any(|scheme| value.starts_with(scheme))
}

#[cfg(test)]
mod call_shape_tests {
    use super::*;

    fn resolve(
        action: Option<&str>,
        trigger: ScheduleTrigger,
    ) -> (Result<ScheduleVerb, String>, ScheduleTrigger) {
        let mut trigger = trigger;
        let verb = resolve_schedule_call(action, &mut trigger, false);
        (verb, trigger)
    }

    #[test]
    fn an_action_naming_the_trigger_creates_it() {
        for (mode, trigger) in [
            (
                "watch",
                ScheduleTrigger {
                    watch: Some("github:pr:o/r#1".into()),
                    ..Default::default()
                },
            ),
            (
                "at",
                ScheduleTrigger {
                    at: Some("20m".into()),
                    ..Default::default()
                },
            ),
            (
                "every",
                ScheduleTrigger {
                    every: Some("15m".into()),
                    ..Default::default()
                },
            ),
            (
                "WATCH",
                ScheduleTrigger {
                    spec: Some("agent:idle:worker".into()),
                    ..Default::default()
                },
            ),
        ] {
            let (verb, resolved) = resolve(Some(mode), trigger);
            assert_eq!(verb, Ok(ScheduleVerb::Create), "{mode}");
            assert!(resolved.spec.is_none());
        }
        let (_, resolved) = resolve(
            Some("at"),
            ScheduleTrigger {
                spec: Some("2026-11-13T23:28:00Z".into()),
                ..Default::default()
            },
        );
        assert_eq!(resolved.at.as_deref(), Some("2026-11-13T23:28:00Z"));
        // `{action: "watch", kind: "agent:idle:x"}`: `kind` carries the value.
        let (_, resolved) = resolve(
            Some("watch"),
            ScheduleTrigger {
                spec: Some("agent:idle:sage-m1".into()),
                ..Default::default()
            },
        );
        assert_eq!(resolved.watch.as_deref(), Some("agent:idle:sage-m1"));
    }

    #[test]
    fn a_mode_under_kind_and_a_bare_watch_spec_create() {
        let (verb, resolved) = resolve(
            None,
            ScheduleTrigger {
                every: Some("5m".into()),
                spec: Some("every".into()),
                ..Default::default()
            },
        );
        assert_eq!(verb, Ok(ScheduleVerb::Create));
        assert_eq!(resolved.every.as_deref(), Some("5m"));
        let (verb, resolved) = resolve(
            Some("create"),
            ScheduleTrigger {
                spec: Some("github:check:o/r@abc".into()),
                ..Default::default()
            },
        );
        assert_eq!(verb, Ok(ScheduleVerb::Create));
        assert_eq!(resolved.watch.as_deref(), Some("github:check:o/r@abc"));
    }

    #[test]
    fn existing_shapes_keep_their_meaning() {
        let (verb, _) = resolve(None, ScheduleTrigger::default());
        assert_eq!(verb, Ok(ScheduleVerb::List));
        let mut empty = ScheduleTrigger::default();
        assert_eq!(
            resolve_schedule_call(None, &mut empty, true),
            Ok(ScheduleVerb::Cancel)
        );
        for word in ["list", "inspect", "ls"] {
            assert_eq!(
                resolve(Some(word), ScheduleTrigger::default()).0,
                Ok(ScheduleVerb::List)
            );
        }
        for word in ["cancel", "delete", "stop"] {
            assert_eq!(
                resolve(Some(word), ScheduleTrigger::default()).0,
                Ok(ScheduleVerb::Cancel)
            );
        }
        let (verb, _) = resolve(
            Some("set"),
            ScheduleTrigger {
                at: Some("1m".into()),
                ..Default::default()
            },
        );
        assert_eq!(verb, Ok(ScheduleVerb::Create));
    }

    #[test]
    fn wrong_shapes_name_the_right_one() {
        let (verb, _) = resolve(Some("update"), ScheduleTrigger::default());
        let err = verb.unwrap_err();
        assert!(err.contains("unknown schedule action `update`"), "{err}");
        assert!(err.contains(SCHEDULE_EXAMPLE), "{err}");
        assert!(err.contains("\"cancel\""), "{err}");
        let (verb, _) = resolve(Some("watch"), ScheduleTrigger::default());
        let err = verb.unwrap_err();
        assert!(
            err.contains("`action: \"watch\"` needs the `watch` value"),
            "{err}"
        );
        let (verb, _) = resolve(
            Some("create"),
            ScheduleTrigger {
                spec: Some("20m".into()),
                ..Default::default()
            },
        );
        let err = verb.unwrap_err();
        assert!(err.contains("does not say which trigger"), "{err}");
    }
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
    if clean_prompt.is_empty() || crate::api::is_null_word(clean_prompt) {
        return Err(format!(
            "`schedule` requires a non-empty `prompt`: the message you get when it fires, e.g. {SCHEDULE_EXAMPLE}"
        ));
    }
    fn given(value: Option<&str>) -> Option<&str> {
        value
            .map(str::trim)
            .filter(|s| !s.is_empty() && !crate::api::is_null_word(s))
    }
    let at_opt = given(at);
    let every_opt = given(every);
    let watch_opt = given(watch);
    let set: Vec<&str> = [
        ("`at`", at_opt),
        ("`every`", every_opt),
        ("`watch`", watch_opt),
    ]
    .into_iter()
    .filter_map(|(name, value)| value.map(|_| name))
    .collect();
    if set.is_empty() {
        return Err(format!(
            "a schedule needs one trigger: `at` (\"20m\" or an ISO time), `every` (\"15m\") or `watch` (\"github:pr:owner/repo#12\", \"github:check:owner/repo@sha\", \"agent:idle:<name>\"), e.g. {SCHEDULE_EXAMPLE}"
        ));
    }
    if set.len() > 1 {
        return Err(format!(
            "a schedule has exactly one trigger, and this call set {}. Make one call per trigger, e.g. {SCHEDULE_EXAMPLE} and `{{action: \"create\", at: \"30m\", prompt: \"...\"}}`",
            set.join(" and ")
        ));
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
    schedules_changed(store.data_dir());
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
    let cancelled = store
        .cancel_schedule(Some(agent_id), id)
        .map_err(|err| err.to_string())?;
    if cancelled > 0 {
        schedules_changed(store.data_dir());
    }
    Ok(cancelled)
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

/// Returns `Some(reason)` when the agent cannot act on a turn-ownership nudge right
/// now (`"paused"`, `"archived"`, `"quota_blocked"` or `"busy"`), or `None` when idle
/// and eligible. Schedule wakes never consult it: they always enter the mailbox.
///
/// `archived` is whether the agent carries an explicit archive marker
/// ([`crate::server::is_explicitly_archived`]), read once by the caller.
fn agent_wake_block_reason(
    store: &Store,
    health: &HealthBoard,
    agent: &Agent,
    archived: bool,
    now: i64,
) -> Option<&'static str> {
    if agent.state == "paused" {
        return Some("paused");
    }
    if agent.state == "archived" || archived {
        return Some("archived");
    }
    // The typed reset deadline is the only quota authority: an error text that
    // mentions a quota or rate limit says nothing once its deadline has passed.
    let cfg = anyagents_core::agent_config_dir(store.data_dir(), &agent.id);
    let session_quota = if agent.harness == "claude-code" {
        anyagents_harness_claude::detect_session_quota_reset_ms(&cfg, now)
    } else {
        anyagents_harness_agy::detect_agy_session_quota_and_error(&cfg, now).0
    };
    let effective_quota = health
        .get(&agent.id)
        .and_then(|live| live.quota_reset_at_ms)
        .or(session_quota);
    if effective_quota.is_some_and(|ms| ms > now) {
        return Some("quota_blocked");
    }
    if is_agent_busy(store, health, agent, now) {
        return Some("busy");
    }
    None
}

pub fn is_agent_busy(store: &Store, health: &HealthBoard, agent: &Agent, now: i64) -> bool {
    // A published live turn proves busy without touching the store.
    if in_live_turn(health, &agent.id) {
        return true;
    }
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
    false
}

/// Whether the supervisor has published a live process in a turn for this agent.
fn in_live_turn(health: &HealthBoard, agent_id: &AgentId) -> bool {
    health.get(agent_id).is_some_and(|live| {
        live.process_alive && live.turn_state == anyagents_harness::TurnState::InTurn
    })
}

/// Probe cadence of a local watch (file, process, agent idle): none has a change
/// event this process can wait on.
const LOCAL_WATCH_PROBE_MS: i64 = 1_000;
/// Probe cadence of a Spiron watch, and recheck cadence of a triggered external
/// watch held by its undelivered previous wake. GitHub probes follow their own
/// adaptive per-target interval in [`github`].
const EXTERNAL_RECHECK_MS: i64 = 3_000;
/// Spacing before a due tick or triggered watch whose wake was not written (a
/// held lifecycle lock, a store error) is tried again.
const RETRY_MS: i64 = 1_000;
/// Longest a timer trusts its cached schedules without re-reading them. It covers
/// writes from another process and wall-clock steps; every change made through
/// this module wakes the timers at once.
const SAFETY_NET_MS: i64 = 60_000;
/// A task child that ended a parent-initiated turn without reporting is nudged
/// this long after the parent's message was delivered.
const CHILD_NUDGE_AFTER_MS: i64 = 15_000;
/// A parent is reminded of a reported, idle task child after this long.
const PARENT_REMINDER_AFTER_MS: i64 = 900_000;
/// How often the timer's turn-ownership guard re-reads the live fleet. It cannot
/// observe deliveries and sends as they happen, so it re-reads them within the
/// time a nudge takes to come due: it learns of a delivery before its nudge is due.
const GUARD_REFRESH_MS: i64 = CHILD_NUDGE_AFTER_MS;
/// How often a guard candidate held only by a block is compared, in memory,
/// with the supervisor's published live state.
const GUARD_WATCH_MS: i64 = 1_000;

/// One change feed per data directory: a schedule written in this process wakes
/// that directory's running timers at once.
static FEEDS: LazyLock<Mutex<HashMap<PathBuf, Arc<tokio::sync::watch::Sender<u64>>>>> =
    LazyLock::new(Mutex::default);

fn change_feed(data_dir: &Path) -> Arc<tokio::sync::watch::Sender<u64>> {
    let mut feeds = FEEDS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    Arc::clone(
        feeds
            .entry(data_dir.to_path_buf())
            .or_insert_with(|| Arc::new(tokio::sync::watch::channel(0).0)),
    )
}

/// Wake this process's schedule timers for `data_dir`: an active schedule was
/// created, changed or cancelled. [`create_agent_schedule`] and
/// [`cancel_agent_schedule`] call it; any other in-process writer of schedules
/// should too. A write from another process is read within [`SAFETY_NET_MS`].
pub fn schedules_changed(data_dir: &Path) {
    let feeds = FEEDS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(feed) = feeds.get(data_dir) {
        feed.send_modify(|generation| *generation = generation.wrapping_add(1));
    }
}

/// Start the server's schedule timers: one for local timers and watches (with
/// the turn-ownership guard), one for network-backed watches, so an in-flight
/// external request cannot delay a local tick. Both step at once at startup,
/// then each sleeps until its earliest due time or a schedule change.
pub fn start_loops(
    store: Arc<Store>,
    health: HealthBoard,
) -> (tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>) {
    let feed = change_feed(store.data_dir());
    let local = spawn_timer(
        Arc::clone(&store),
        health.clone(),
        feed.subscribe(),
        Timer::local(),
    );
    let external = spawn_timer(store, health, feed.subscribe(), Timer::external());
    (local, external)
}

fn spawn_timer(
    store: Arc<Store>,
    health: HealthBoard,
    mut changes: tokio::sync::watch::Receiver<u64>,
    timer: Timer,
) -> tokio::task::JoinHandle<()> {
    let timer = Arc::new(Mutex::new(timer));
    tokio::spawn(async move {
        loop {
            let generation = *changes.borrow_and_update();
            let (st, hb, tm) = (Arc::clone(&store), health.clone(), Arc::clone(&timer));
            let step = tokio::task::spawn_blocking(move || {
                let mut timer = tm.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                timer
                    .step(&st, &hb, generation, anyagents_core::now_ms())
                    .deadline
            })
            .await;
            let deadline = step.unwrap_or_else(|err| {
                eprintln!("Schedule pass failed: {err}");
                anyagents_core::now_ms().saturating_add(RETRY_MS)
            });
            let wait = deadline
                .saturating_sub(anyagents_core::now_ms())
                .clamp(0, SAFETY_NET_MS);
            tokio::select! {
                () = tokio::time::sleep(Duration::from_millis(wait as u64)) => {}
                Ok(()) = changes.changed() => {}
            }
        }
    })
}

/// One of the server's schedule timers. A step does work only when an entry is
/// due, the guard is due, or the schedules changed.
struct Timer {
    plan: Plan,
    /// The local timer also runs the turn-ownership guard.
    guard: Option<TurnGuard>,
    restored: bool,
    deadline: i64,
}

impl Timer {
    fn local() -> Self {
        Self {
            plan: Plan::new(false),
            guard: Some(TurnGuard::default()),
            restored: false,
            deadline: i64::MIN,
        }
    }

    fn external() -> Self {
        Self {
            plan: Plan::new(true),
            guard: None,
            restored: true,
            deadline: i64::MIN,
        }
    }

    /// Run whatever is due at `now`.
    fn step(&mut self, store: &Store, health: &HealthBoard, generation: u64, now: i64) -> Step {
        let mut step = Step {
            worked: false,
            fired: 0,
            deadline: self.deadline,
        };
        if now < self.deadline && generation == self.plan.generation {
            return step;
        }
        if !self.restored {
            step.worked = true;
            self.restored = restore_legacy_pr_watches_once(store);
            if !self.restored {
                self.deadline = now.saturating_add(RETRY_MS);
                step.deadline = self.deadline;
                return step;
            }
        }
        if self.plan.stale(generation, now) {
            step.worked = true;
            self.plan.reload(store, generation, now);
        }
        if self.plan.due(now) {
            step.worked = true;
            let pass = self.plan.pass(store, health, now);
            step.fired = pass.fired;
            if pass.wrote {
                self.plan.reload(store, generation, now);
            }
        }
        if let Some(guard) = &mut self.guard
            && guard.due(health, now)
        {
            step.worked = true;
            guard.run(store, health, now);
        }
        let mut deadline = self.plan.next_deadline();
        if let Some(guard) = &self.guard {
            deadline = deadline.min(guard.next_deadline(now));
        }
        // Anything still due after its own step waits out a retry, never a spin.
        self.deadline = if deadline <= now {
            now.saturating_add(RETRY_MS)
        } else {
            deadline
        };
        step.deadline = self.deadline;
        step
    }
}

/// What one timer step did.
struct Step {
    /// The step read or wrote the store.
    worked: bool,
    /// Wakes written.
    fired: usize,
    /// When the next step is due.
    deadline: i64,
}

/// Evaluate local timers and watches (and the turn-ownership rules) without
/// issuing any external probe: one full census of the store, which is a fresh
/// local timer's first step. [`start_loops`] keeps that timer and steps it again
/// only when something is due; tests drive single passes through this.
#[cfg(test)]
fn tick_local_schedules_once(store: &Store, health: &HealthBoard, now: i64) -> usize {
    Timer::local().step(store, health, 0, now).fired
}

/// Evaluate network-backed watches: one full census of the store. Slow external
/// services cannot delay a later local tick or consume a timer's due transition.
#[cfg(test)]
fn tick_external_watches_once(store: &Store, health: &HealthBoard, now: i64) -> usize {
    Timer::external().step(store, health, 0, now).fired
}

// Retain the complete single-pass fixture for existing cross-class contracts;
// production never serializes the two partitions through this helper.
#[cfg(test)]
fn tick_schedules_once(store: &Store, health: &HealthBoard, now: i64) -> usize {
    tick_local_schedules_once(store, health, now) + tick_external_watches_once(store, health, now)
}

/// Reopen the PR watches an earlier release retired when checks completed, once
/// per database in this process. No current path retires a PR watch that way, so
/// the repair (a full scan of every schedule ever made) has nothing to do after
/// its first run. Returns false while the store cannot run it.
fn restore_legacy_pr_watches_once(store: &Store) -> bool {
    static DONE: LazyLock<Mutex<HashSet<PathBuf>>> = LazyLock::new(Mutex::default);
    let mut done = DONE.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if done.contains(store.data_dir()) {
        return true;
    }
    match store.restore_checks_completed_pr_watches() {
        Ok(_) => {
            done.insert(store.data_dir().to_path_buf());
            true
        }
        Err(err) => {
            eprintln!("Schedule recovery unavailable: {err}");
            false
        }
    }
}

/// One full turn-ownership pass over the live fleet, as a census would run it.
#[cfg(test)]
fn tick_task_subagent_turn_ownership(store: &Store, health: &HealthBoard, now: i64) -> usize {
    TurnGuard::default().run(store, health, now)
}

/// A parent and its live `lifecycle: task` children.
struct Family {
    parent: Agent,
    children: Vec<Agent>,
}

/// A live task child the guard may act on: paused and archived ones never are.
fn guard_candidate(agent: &Agent) -> bool {
    agent.is_task_subagent() && !matches!(agent.state.as_str(), "paused" | "archived")
}

/// Group the live task children by parent. Archived children never act, so only
/// the live fleet is read; an archived parent of a live child is read on its own.
fn task_families(store: &Store, live: &[Agent]) -> Vec<Family> {
    let by_id: HashMap<&AgentId, &Agent> = live.iter().map(|a| (&a.id, a)).collect();
    let mut slots: HashMap<&AgentId, Option<usize>> = HashMap::new();
    let mut families: Vec<Family> = Vec::new();
    for child in live.iter().filter(|a| guard_candidate(a)) {
        let Some(parent_id) = child.parent_id.as_ref() else {
            continue;
        };
        let slot = *slots.entry(parent_id).or_insert_with(|| {
            let parent = match by_id.get(parent_id) {
                Some(parent) => (*parent).clone(),
                None => store.agent(parent_id).ok()?,
            };
            families.push(Family {
                parent,
                children: Vec::new(),
            });
            Some(families.len() - 1)
        });
        if let Some(slot) = slot {
            families[slot].children.push(child.clone());
        }
    }
    families
}

/// What one family's pass did and what it waits on.
#[derive(Default)]
struct GuardOutcome {
    actions: usize,
    /// Earliest later time a rule can hold for the snapshots read.
    next_at: Option<i64>,
    /// Agents whose block alone keeps a due rule from acting.
    blocked: Vec<AgentId>,
}

impl GuardOutcome {
    fn wait_until(&mut self, at: i64) {
        self.next_at = Some(self.next_at.map_or(at, |next| next.min(at)));
    }
}

/// Facts read at most once per agent in a pass: the archive marker, the block
/// reason, and a parent's ownership snapshot.
struct GuardMemo<'a> {
    store: &'a Store,
    health: &'a HealthBoard,
    now: i64,
    archived: HashMap<AgentId, bool>,
    blocked: HashMap<AgentId, bool>,
    snapshots: HashMap<AgentId, Option<anyagents_core::TurnOwnershipSnapshot>>,
}

impl<'a> GuardMemo<'a> {
    fn new(store: &'a Store, health: &'a HealthBoard, now: i64) -> Self {
        Self {
            store,
            health,
            now,
            archived: HashMap::new(),
            blocked: HashMap::new(),
            snapshots: HashMap::new(),
        }
    }

    fn archived(&mut self, agent: &Agent) -> bool {
        let store = self.store;
        *self
            .archived
            .entry(agent.id.clone())
            .or_insert_with(|| crate::server::is_explicitly_archived(store.data_dir(), agent))
    }

    /// [`agent_wake_block_reason`] is `Some`. A published live turn answers first,
    /// without the store or the session files.
    fn blocked(&mut self, agent: &Agent) -> bool {
        if let Some(&blocked) = self.blocked.get(&agent.id) {
            return blocked;
        }
        let blocked = in_live_turn(self.health, &agent.id) || {
            let archived = self.archived(agent);
            agent_wake_block_reason(self.store, self.health, agent, archived, self.now).is_some()
        };
        self.blocked.insert(agent.id.clone(), blocked);
        blocked
    }

    fn snapshot(&mut self, agent_id: &AgentId) -> Option<anyagents_core::TurnOwnershipSnapshot> {
        let store = self.store;
        self.snapshots
            .entry(agent_id.clone())
            .or_insert_with(|| store.turn_ownership_snapshot(agent_id).ok())
            .clone()
    }

    /// Whether a child a rule points at may be acted on now. A child held only by
    /// its block is recorded for watching.
    fn child_may_act(&mut self, child: &Agent, out: &mut GuardOutcome) -> bool {
        if self.archived(child)
            || self
                .store
                .list_schedules(Some(&child.id), true)
                .is_ok_and(|s| !s.is_empty())
        {
            return false;
        }
        if self.blocked(child) {
            out.blocked.push(child.id.clone());
            return false;
        }
        true
    }

    /// Whether a parent may be told about its child now. A parent held only by
    /// its block is recorded for watching.
    fn parent_may_act(&mut self, parent: &Agent, out: &mut GuardOutcome) -> bool {
        if matches!(parent.state.as_str(), "paused" | "archived") || self.archived(parent) {
            return false;
        }
        if self.blocked(parent) {
            out.blocked.push(parent.id.clone());
            return false;
        }
        true
    }
}

/// Apply both turn-ownership rules to one family. The cheapest decisive fact is
/// read first: a rule's time, then whether its action was already taken, and
/// only then the archive marker, schedules and block reasons.
fn guard_family(memo: &mut GuardMemo<'_>, family: &Family) -> GuardOutcome {
    let (store, now) = (memo.store, memo.now);
    let parent = &family.parent;
    let parent_sender = format!("agent:{}", parent.name);
    let mut out = GuardOutcome::default();
    let mut waiting: Vec<(String, String, i64)> = Vec::new();
    for child in &family.children {
        let Ok(snap) = store.turn_ownership_snapshot(&child.id) else {
            continue;
        };
        let last_in_ms = snap.last_delivered_at_ms.unwrap_or(0);
        let last_out_ms = snap.last_outbound_at_ms.unwrap_or(0);
        if last_in_ms > 0
            && last_in_ms > last_out_ms
            && snap
                .last_delivered_sender
                .as_deref()
                .is_some_and(|s| s == "parent" || s == parent_sender || s == "system:turn-guard")
        {
            let due = last_in_ms.saturating_add(CHILD_NUDGE_AFTER_MS);
            if now < due {
                out.wait_until(due);
                continue;
            }
            let seq = snap.last_delivered_seq.unwrap_or(0);
            let already_nudged = snap
                .last_delivered_source_key
                .as_deref()
                .is_some_and(|k| k.starts_with("turn-guard:child-nudge:"));
            let (recipient, key) = if already_nudged {
                (
                    parent,
                    format!("turn-guard:parent-alert:{}:{seq}", child.id),
                )
            } else {
                (child, format!("turn-guard:child-nudge:{}:{seq}", child.id))
            };
            if !store
                .has_message_source_key(&recipient.id, &key)
                .is_ok_and(|exists| !exists)
                || !memo.child_may_act(child, &mut out)
                || (already_nudged && !memo.parent_may_act(parent, &mut out))
            {
                continue;
            }
            let body = if already_nudged {
                format!(
                    "[Sub-Agent Turn Ownership Guard] Task sub-agent `{}` (`role: {}`) exited its turn without calling `send` to report back and has no active schedules. Inspect it via `agents(action: \"inspect\", name: \"{}\")`, send it a follow-up instruction, or close it via `agents(action: \"archive\", name: \"{}\")`.",
                    child.name,
                    child.role.as_deref().unwrap_or("task"),
                    child.name,
                    child.name
                )
            } else {
                format!(
                    "[Turn Ownership Guard] You exited your turn as a `lifecycle: task` sub-agent without calling `send` to report to your parent (`agent:{}`) and without an active `schedule`. Complete any remaining work now or call `send` (`to: \"parent\"`) with your verified outcome or blocker.",
                    parent.name
                )
            };
            if send_guard_message(store, &recipient.id, body, key, now) {
                out.actions += 1;
            }
        } else if last_out_ms > 0 && last_out_ms >= last_in_ms {
            // The reminder counts from the later of the report and the parent's
            // last delivery, so nothing is due before the report plus the wait.
            let earliest = last_out_ms.saturating_add(PARENT_REMINDER_AFTER_MS);
            if now < earliest {
                out.wait_until(earliest);
                continue;
            }
            if !memo.child_may_act(child, &mut out) || !memo.parent_may_act(parent, &mut out) {
                continue;
            }
            let Some(parent_snap) = memo.snapshot(&parent.id) else {
                continue;
            };
            let due = last_out_ms
                .max(parent_snap.last_delivered_at_ms.unwrap_or(0))
                .saturating_add(PARENT_REMINDER_AFTER_MS);
            if now < due {
                out.wait_until(due);
                continue;
            }
            waiting.push((
                child.name.clone(),
                child.role.clone().unwrap_or_else(|| "task".to_string()),
                last_out_ms,
            ));
        }
    }

    if !waiting.is_empty() {
        waiting.sort_by(|a, b| a.0.cmp(&b.0));
        let fingerprint_raw = waiting
            .iter()
            .map(|(name, _, ts)| format!("{name}:{ts}"))
            .collect::<Vec<_>>()
            .join(",");
        let digest = anyagents_core::learned_memory::sha256(fingerprint_raw.as_bytes());
        let hash16 = &digest[..16.min(digest.len())];
        let reminder_key = format!("turn-guard:waiting-parent:{}:{hash16}", parent.id);
        if store
            .has_message_source_key(&parent.id, &reminder_key)
            .is_ok_and(|exists| !exists)
        {
            let formatted = waiting
                .iter()
                .map(|(name, role, _)| format!("`{name}` (`{role}`)"))
                .collect::<Vec<_>>()
                .join(", ");
            let body = format!(
                "[Sub-Agent Turn Ownership Guard] The following `lifecycle: task` sub-agent(s) reported to you and are idle with no active schedules (`waiting_parent`): {formatted}. The ball is in your court: for each sub-agent, judge its work with `agents(action: \"verdict\", name: \"<name>\", verdict: \"accepted\" | \"rework\" | \"rejected\" | \"abandoned\", reason: \"...\")`, then either send a follow-up instruction via `send` if work remains, or close it via `agents(action: \"archive\", name: \"<name>\")`."
            );
            if send_guard_message(store, &parent.id, body, reminder_key, now) {
                out.actions += 1;
            }
        }
    }
    out
}

fn send_guard_message(
    store: &Store,
    to: &AgentId,
    body: String,
    source_key: String,
    now: i64,
) -> bool {
    let mut msg = Incoming::new(body);
    msg.sender = Some("system:turn-guard".to_string());
    msg.source_key = Some(source_key);
    msg.source_time_ms = Some(now);
    if store.receive(to, msg).is_err() {
        return false;
    }
    crate::create::poke_agent_wake_socket(store.data_dir(), to);
    crate::create::poke_agent_wake_socket_mail(store.data_dir(), to);
    true
}

/// The parts of an agent's published live state whose change can lift a block.
type LiveMark = (bool, anyagents_harness::TurnState, bool, Option<i64>);

fn live_mark(health: &HealthBoard, agent_id: &AgentId, now: i64) -> Option<LiveMark> {
    health.get(agent_id).map(|live| {
        (
            live.process_alive,
            live.turn_state,
            live.quota_reset_at_ms.is_some_and(|at| at > now),
            live.process_started_at_ms,
        )
    })
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
///
/// The local timer runs a full pass over the live fleet every [`GUARD_REFRESH_MS`],
/// and in between only the families whose rule comes due or whose blocking
/// agent's published live state changes.
#[derive(Default)]
struct TurnGuard {
    refresh_at: i64,
    families: Vec<FamilyWatch>,
}

struct FamilyWatch {
    family: Family,
    next_at: i64,
    watched: Vec<(AgentId, Option<LiveMark>)>,
}

impl FamilyWatch {
    fn new(family: Family, outcome: GuardOutcome, health: &HealthBoard, now: i64) -> Self {
        let mut watched: Vec<(AgentId, Option<LiveMark>)> = Vec::new();
        for id in outcome.blocked {
            if !watched.iter().any(|(seen, _)| *seen == id) {
                let mark = live_mark(health, &id, now);
                watched.push((id, mark));
            }
        }
        Self {
            family,
            next_at: outcome.next_at.unwrap_or(i64::MAX),
            watched,
        }
    }

    fn due(&self, health: &HealthBoard, now: i64) -> bool {
        now >= self.next_at
            || self
                .watched
                .iter()
                .any(|(id, mark)| live_mark(health, id, now) != *mark)
    }
}

impl TurnGuard {
    fn due(&self, health: &HealthBoard, now: i64) -> bool {
        now >= self.refresh_at || self.families.iter().any(|f| f.due(health, now))
    }

    fn run(&mut self, store: &Store, health: &HealthBoard, now: i64) -> usize {
        let mut memo = GuardMemo::new(store, health, now);
        let mut actions = 0;
        if now >= self.refresh_at {
            let Ok(live) = store.list_live_agents() else {
                self.refresh_at = now.saturating_add(RETRY_MS);
                return 0;
            };
            self.refresh_at = now.saturating_add(GUARD_REFRESH_MS);
            self.families = task_families(store, &live)
                .into_iter()
                .map(|family| {
                    let outcome = guard_family(&mut memo, &family);
                    actions += outcome.actions;
                    FamilyWatch::new(family, outcome, health, now)
                })
                .collect();
            return actions;
        }
        for watch in &mut self.families {
            if !watch.due(health, now) {
                continue;
            }
            // Rows held since the last full pass may be stale; act on fresh ones.
            let family = Family {
                parent: store
                    .agent(&watch.family.parent.id)
                    .unwrap_or_else(|_| watch.family.parent.clone()),
                children: watch
                    .family
                    .children
                    .iter()
                    .filter_map(|child| store.agent(&child.id).ok())
                    .filter(guard_candidate)
                    .collect(),
            };
            let outcome = guard_family(&mut memo, &family);
            actions += outcome.actions;
            *watch = FamilyWatch::new(family, outcome, health, now);
        }
        actions
    }

    fn next_deadline(&self, now: i64) -> i64 {
        self.families.iter().fold(self.refresh_at, |at, family| {
            let at = at.min(family.next_at);
            if family.watched.is_empty() {
                at
            } else {
                at.min(now.saturating_add(GUARD_WATCH_MS))
            }
        })
    }
}

/// One active schedule as a timer holds it, with its watch spec parsed once.
struct Entry {
    sched: AgentSchedule,
    /// The target of a watch whose spec parses.
    watch: Option<WatchTarget>,
    /// When the entry next needs a look: a timer's due time, a watch's next
    /// probe, or a retry.
    check_at: i64,
    /// The watched agent of an `agent:idle` watch as last resolved, so its
    /// published live turn answers without the store.
    idle_target: Option<AgentId>,
}

impl Entry {
    fn is_github(&self) -> bool {
        matches!(
            self.watch,
            Some(WatchTarget::GitHubPr { .. } | WatchTarget::GitHubCheck { .. })
        )
    }
}

fn is_external_target(target: &WatchTarget) -> bool {
    matches!(
        target,
        WatchTarget::GitHubPr { .. }
            | WatchTarget::GitHubCheck { .. }
            | WatchTarget::SpironPromise { .. }
    )
}

/// The active schedules of one partition: local timers and watches, or
/// network-backed watches.
struct Plan {
    external: bool,
    entries: Vec<Entry>,
    /// Change-feed generation the entries were read at.
    generation: u64,
    reload_at: i64,
}

#[derive(Default)]
struct PassOutcome {
    fired: usize,
    /// A schedule row was (or may have been) written, so the plan is re-read.
    wrote: bool,
}

#[derive(Default)]
struct EntryOutcome {
    fired: bool,
    wrote: bool,
}

impl EntryOutcome {
    const WROTE: Self = Self {
        fired: false,
        wrote: true,
    };
}

impl Plan {
    fn new(external: bool) -> Self {
        Self {
            external,
            entries: Vec::new(),
            generation: 0,
            reload_at: i64::MIN,
        }
    }

    fn stale(&self, generation: u64, now: i64) -> bool {
        generation != self.generation || now >= self.reload_at
    }

    fn reload(&mut self, store: &Store, generation: u64, now: i64) {
        match self.read(store) {
            Ok(entries) => {
                self.entries = entries;
                self.generation = generation;
                self.reload_at = now.saturating_add(SAFETY_NET_MS);
            }
            Err(err) => {
                eprintln!("Schedule census unavailable: {err}");
                self.reload_at = now.saturating_add(RETRY_MS);
            }
        }
    }

    /// Read this partition's active schedules, keeping each watch's next probe
    /// and each pending retry. A watch of an archived or deleted owner ends here,
    /// as every census pass has ended it; a timer's owner is read when it is due.
    fn read(&self, store: &Store) -> Result<Vec<Entry>, anyagents_core::Error> {
        let schedules = store.list_schedules(None, true)?;
        let previous: HashMap<&str, &Entry> = self
            .entries
            .iter()
            .map(|entry| (entry.sched.id.as_str(), entry))
            .collect();
        let mut live: Option<HashSet<AgentId>> = None;
        let mut entries = Vec::new();
        for sched in schedules {
            let watch = if sched.kind == "watch" {
                parse_watch_spec(&sched.spec).ok()
            } else {
                None
            };
            if watch.as_ref().is_some_and(is_external_target) != self.external {
                continue;
            }
            if sched.kind == "watch" {
                if live.is_none() {
                    live = Some(
                        store
                            .list_live_agents()?
                            .into_iter()
                            .map(|agent| agent.id)
                            .collect(),
                    );
                }
                if live
                    .as_ref()
                    .is_some_and(|live| !live.contains(&sched.agent_id))
                {
                    if let Err(err) = store.cancel_schedule(None, &sched.id) {
                        eprintln!("Schedule cancellation failed: {err}");
                    }
                    continue;
                }
            }
            let prior = previous.get(sched.id.as_str()).copied();
            let check_at = if sched.kind == "watch" {
                prior.map_or(i64::MIN, |p| p.check_at)
            } else {
                let due = sched.next_fire_at_ms.unwrap_or(i64::MAX);
                // A retry outlives the reload only while the slot it retries stands.
                prior
                    .filter(|p| p.sched.next_fire_at_ms == sched.next_fire_at_ms)
                    .map_or(due, |p| p.check_at.max(due))
            };
            let idle_target = prior.and_then(|p| p.idle_target.clone());
            entries.push(Entry {
                sched,
                watch,
                check_at,
                idle_target,
            });
        }
        Ok(entries)
    }

    fn due(&self, now: i64) -> bool {
        self.entries.iter().any(|entry| entry.check_at <= now)
    }

    fn next_deadline(&self) -> i64 {
        self.entries
            .iter()
            .map(|entry| entry.check_at)
            .fold(self.reload_at, i64::min)
    }

    /// Evaluate every entry whose check time has come.
    fn pass(&mut self, store: &Store, health: &HealthBoard, now: i64) -> PassOutcome {
        if self.external {
            github::begin_cycle(
                self.entries
                    .iter()
                    .filter_map(|e| Some((e.sched.id.as_str(), e.watch.as_ref()?))),
                now,
            );
        }
        let (mut order, rest): (Vec<usize>, Vec<usize>) =
            (0..self.entries.len()).partition(|&i| self.entries[i].check_at <= now);
        // GitHub answers come from one cache per target. Once a due watch has
        // probed its target, every other watch of it sees that answer now.
        if order.iter().any(|&i| self.entries[i].is_github()) {
            order.extend(rest.into_iter().filter(|&i| self.entries[i].is_github()));
        }
        let mut outcome = PassOutcome::default();
        for i in order {
            let step = evaluate_entry(store, health, &mut self.entries[i], now);
            outcome.fired += usize::from(step.fired);
            outcome.wrote |= step.wrote;
        }
        outcome
    }
}

enum Owner {
    Live(Box<Agent>),
    /// Archived or deleted: its schedule is cancelled.
    Gone,
    /// Unreadable now: the entry waits for a later look.
    Unknown,
}

fn schedule_owner(store: &Store, sched: &AgentSchedule) -> Owner {
    match store.agent(&sched.agent_id) {
        Ok(agent) if agent.state == "archived" => Owner::Gone,
        Ok(agent) => Owner::Live(Box::new(agent)),
        Err(anyagents_core::Error::AgentNotFound(_)) => Owner::Gone,
        Err(err) => {
            eprintln!("Schedule agent state unavailable: {err}");
            Owner::Unknown
        }
    }
}

fn cancel_for_gone_owner(store: &Store, sched: &AgentSchedule) -> EntryOutcome {
    if let Err(err) = store.cancel_schedule(None, &sched.id) {
        eprintln!("Schedule cancellation failed: {err}");
    }
    EntryOutcome::WROTE
}

fn evaluate_entry(
    store: &Store,
    health: &HealthBoard,
    entry: &mut Entry,
    now: i64,
) -> EntryOutcome {
    match entry.sched.kind.as_str() {
        "at" | "every" => evaluate_timer(store, entry, now),
        "watch" => evaluate_watch_entry(store, health, entry, now),
        _ => {
            entry.check_at = i64::MAX;
            EntryOutcome::default()
        }
    }
}

fn evaluate_timer(store: &Store, entry: &mut Entry, now: i64) -> EntryOutcome {
    let sched = &entry.sched;
    let Some(due) = sched.next_fire_at_ms else {
        entry.check_at = i64::MAX;
        return EntryOutcome::default();
    };
    if now < due {
        entry.check_at = due;
        return EntryOutcome::default();
    }
    // Until a re-read shows the slot claimed, this one waits out a retry.
    entry.check_at = now.saturating_add(RETRY_MS);
    let agent = match schedule_owner(store, sched) {
        Owner::Live(agent) => agent,
        Owner::Gone => return cancel_for_gone_owner(store, sched),
        Owner::Unknown => return EntryOutcome::default(),
    };
    let source_key = format!("schedule:{}:{}", sched.id, due);
    if sched.kind == "at" {
        return EntryOutcome {
            fired: deliver_scheduled_wake(store, &agent, sched, &source_key, now),
            wrote: true,
        };
    }
    let interval = sched.interval_ms.unwrap_or(60_000).max(1_000);
    let mut next_due = due.saturating_add(interval);
    while next_due <= now {
        next_due = next_due.saturating_add(interval);
    }
    let Some(previous_pending) = previous_wake_pending(store, &agent, sched) else {
        return EntryOutcome::default();
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
        return EntryOutcome::WROTE;
    }
    EntryOutcome {
        fired: claim_and_deliver_scheduled_wake(
            store,
            &agent,
            sched,
            Some(due),
            "active",
            Some(next_due),
            None,
            Some("fired"),
            &sched.prompt,
            &source_key,
            now,
        ),
        wrote: true,
    }
}

/// When a watch is probed next after a look at `now`.
fn watch_probe_at(watch: Option<&WatchTarget>, now: i64) -> i64 {
    match watch {
        // A spec never changes, so an invalid one is looked at once.
        None => i64::MAX,
        Some(WatchTarget::SpironPromise { .. }) => now.saturating_add(EXTERNAL_RECHECK_MS),
        Some(target @ (WatchTarget::GitHubPr { .. } | WatchTarget::GitHubCheck { .. })) => {
            github::next_probe_at(target)
                .unwrap_or(0)
                .max(now.saturating_add(RETRY_MS))
        }
        Some(_) => now.saturating_add(LOCAL_WATCH_PROBE_MS),
    }
}

fn evaluate_watch_entry(
    store: &Store,
    health: &HealthBoard,
    entry: &mut Entry,
    now: i64,
) -> EntryOutcome {
    let eval = evaluate_watch(
        store,
        health,
        &entry.sched,
        entry.watch.as_ref(),
        &mut entry.idle_target,
        now,
    );
    entry.check_at = watch_probe_at(entry.watch.as_ref(), now);
    let recheck_at = now.saturating_add(if entry.watch.as_ref().is_some_and(is_external_target) {
        EXTERNAL_RECHECK_MS
    } else {
        LOCAL_WATCH_PROBE_MS
    });
    let sched = &entry.sched;
    let changed = eval
        .baseline
        .as_ref()
        .is_some_and(|b| Some(b) != sched.watch_baseline.as_ref())
        || eval
            .observed
            .as_ref()
            .is_some_and(|o| Some(o) != sched.last_observed.as_ref());
    if !eval.triggered && !changed {
        return EntryOutcome::default();
    }
    let agent = match schedule_owner(store, sched) {
        Owner::Live(agent) => agent,
        Owner::Gone => return cancel_for_gone_owner(store, sched),
        Owner::Unknown => {
            entry.check_at = entry.check_at.min(recheck_at);
            return EntryOutcome::default();
        }
    };
    if !eval.triggered {
        if let Err(err) = store.update_schedule_watch_observation(
            &sched.id,
            eval.baseline.as_deref(),
            eval.observed.as_deref(),
        ) {
            eprintln!("Schedule {} observation write failed: {err}", sched.id);
        }
        return EntryOutcome::WROTE;
    }
    let Some(previous_pending) = previous_wake_pending(store, &agent, sched) else {
        entry.check_at = entry.check_at.min(recheck_at);
        return EntryOutcome::default();
    };
    // A pull request outcome is a fact about GitHub, not about the wake: it is
    // stored when first seen, even while an earlier wake waits, and always
    // before the wake that reports it is claimed.
    let watch = entry.watch.as_ref();
    let fact = || pull_request_outcome(store, &agent, sched, watch, &eval, now);
    if previous_pending {
        // Record the latest observation without consuming the transition or
        // moving the baseline; it is written once the pending wake is taken.
        entry.check_at = entry.check_at.min(recheck_at);
        if eval.observed.is_some() && eval.observed != sched.last_observed {
            if !record_outcome(store, sched, fact()) {
                entry.check_at = entry.check_at.min(now.saturating_add(RETRY_MS));
                return EntryOutcome::default();
            }
            if let Err(err) = store.update_schedule_watch_observation(
                &sched.id,
                sched.watch_baseline.as_deref(),
                eval.observed.as_deref(),
            ) {
                eprintln!("Schedule {} observation write failed: {err}", sched.id);
            }
            return EntryOutcome::WROTE;
        }
        return EntryOutcome::default();
    }
    if !record_outcome(store, sched, fact()) {
        entry.check_at = entry.check_at.min(now.saturating_add(RETRY_MS));
        return EntryOutcome::default();
    }
    let (wake_prompt, source_key) = if let Some(ref tr) = eval.transition {
        (
            format!("[watch: {} transition={tr}]\n{}", sched.spec, sched.prompt),
            format!("schedule:{}:watch:{tr}:{now}", sched.id),
        )
    } else {
        (sched.prompt.clone(), format!("schedule:{}:watch", sched.id))
    };
    let fired = claim_and_deliver_scheduled_wake(
        store,
        &agent,
        sched,
        None,
        eval.next_state,
        None,
        eval.baseline.as_deref(),
        eval.observed.as_deref(),
        &wake_prompt,
        &source_key,
        now,
    );
    if !fired {
        entry.check_at = entry.check_at.min(now.saturating_add(RETRY_MS));
    }
    EntryOutcome { fired, wrote: true }
}

/// The outcome a pull request watch transition records for its owner
/// (ADR 0008): a merge, a close, or a review failure on one head.
fn pull_request_outcome(
    store: &Store,
    agent: &Agent,
    sched: &AgentSchedule,
    target: Option<&WatchTarget>,
    eval: &WatchEval,
    now: i64,
) -> Option<NewOutcome> {
    let Some(WatchTarget::GitHubPr {
        owner,
        repo,
        number,
    }) = target
    else {
        return None;
    };
    let kind = match eval.transition.as_deref()? {
        "merged" => OutcomeKind::PrMerged,
        "closed" => OutcomeKind::PrClosed,
        "review_failed" => OutcomeKind::ReviewFailed,
        _ => return None,
    };
    let (_, head) = parse_pr_baseline(eval.baseline.as_deref());
    let config = anyagents_core::agent_config_dir(store.data_dir(), &agent.id);
    Some(NewOutcome::pull_request(
        agent.id.clone(),
        owner,
        repo,
        *number,
        kind,
        head,
        sched.created_at_ms,
        now,
        crate::create::bound_profile_name(&config),
    ))
}

/// Store `fact`, if there is one. False when it could not be stored: the
/// caller then leaves the transition unconsumed, so a later pass stores it
/// before anything else happens.
fn record_outcome(store: &Store, sched: &AgentSchedule, fact: Option<NewOutcome>) -> bool {
    let Some(fact) = fact else {
        return true;
    };
    match store.record_outcome(fact) {
        Ok(_) => true,
        Err(err) => {
            eprintln!("Schedule {} outcome not recorded: {err}", sched.id);
            false
        }
    }
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
    target: Option<&WatchTarget>,
    idle_target: &mut Option<AgentId>,
    now: i64,
) -> WatchEval {
    let Some(target) = target else {
        return WatchEval::pending(
            sched.watch_baseline.clone(),
            Some("invalid_spec".to_string()),
        );
    };
    match target {
        WatchTarget::AgentIdle { target } => {
            // The watched agent's published live turn answers without the store.
            if idle_target
                .as_ref()
                .is_some_and(|id| in_live_turn(health, id))
            {
                return WatchEval::pending(
                    Some("working".to_string()),
                    Some("working".to_string()),
                );
            }
            let Ok(found) = store.agent_by_name(target) else {
                return WatchEval::pending(
                    sched.watch_baseline.clone(),
                    Some("not_found".to_string()),
                );
            };
            *idle_target = Some(found.id.clone());
            if is_agent_busy(store, health, &found, now) {
                WatchEval::pending(Some("working".to_string()), Some("working".to_string()))
            } else {
                WatchEval::fired(sched.watch_baseline.clone(), Some("idle".to_string()))
            }
        }
        WatchTarget::SpironPromise { id, kind } => {
            let current = match probe_spiron_state(id, kind) {
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
            let Some((_, state_desc, head_sha)) =
                probe_github_pr_state(owner, repo, *number, sched.agent_id.as_str(), now)
            else {
                return WatchEval::pending(sched.watch_baseline.clone(), None);
            };
            evaluate_pr_observation(sched, state_desc, head_sha)
        }
        WatchTarget::GitHubCheck {
            owner,
            repo,
            reference,
        } => {
            let Some((is_terminal, state_desc)) =
                probe_github_check_terminal(owner, repo, reference, sched.agent_id.as_str(), now)
            else {
                return WatchEval::pending(sched.watch_baseline.clone(), None);
            };
            if is_terminal {
                WatchEval::fired(sched.watch_baseline.clone(), Some(state_desc))
            } else {
                WatchEval::pending(sched.watch_baseline.clone(), Some(state_desc))
            }
        }
        WatchTarget::ProcessExit { pid } => {
            if probe_process_alive(*pid) {
                WatchEval::pending(Some("running".to_string()), Some("running".to_string()))
            } else {
                WatchEval::fired(sched.watch_baseline.clone(), Some("exited".to_string()))
            }
        }
        WatchTarget::PathChanged { path } => {
            let current = probe_path_fingerprint(path);
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

fn parse_pr_baseline(baseline: Option<&str>) -> (&str, Option<&str>) {
    match baseline {
        None => ("pending", None),
        Some(raw) => match raw.split_once('@') {
            Some((state, head)) if !head.is_empty() => (state, Some(head)),
            _ => (raw, None),
        },
    }
}

fn format_pr_baseline(state: &str, head_sha: Option<&str>) -> String {
    match head_sha {
        Some(head) if !head.is_empty() => format!("{state}@{head}"),
        _ => state.to_string(),
    }
}

fn evaluate_pr_observation(
    sched: &AgentSchedule,
    state_desc: String,
    head_sha: Option<String>,
) -> WatchEval {
    if state_desc.starts_with("probe_error:") {
        return WatchEval::pending(sched.watch_baseline.clone(), Some(state_desc));
    }
    if state_desc == "merged" || state_desc == "closed" {
        return WatchEval {
            triggered: true,
            next_state: "fired",
            transition: Some(state_desc.clone()),
            observed: Some(state_desc.clone()),
            baseline: Some(state_desc),
        };
    }
    let (raw_prev, prev_head) = parse_pr_baseline(sched.watch_baseline.as_deref());
    let head_changed = match (prev_head, head_sha.as_deref()) {
        (Some(prev), Some(curr)) => prev != curr,
        _ => false,
    };
    let effective_head = head_sha.as_deref().filter(|h| !h.is_empty()).or(prev_head);
    let next_baseline = format_pr_baseline(&state_desc, effective_head);
    if raw_prev.starts_with("merge_queue:") && !state_desc.starts_with("merge_queue:") {
        return WatchEval {
            triggered: true,
            next_state: "active",
            transition: Some("evicted".to_string()),
            observed: Some(format!("evicted:{state_desc}")),
            baseline: Some(next_baseline),
        };
    }
    // A new head commit invalidates any prior check/review baseline for the
    // previous SHA so checks_completed -> checks_completed still wakes.
    let prev = if head_changed { "pending" } else { raw_prev };
    if state_desc == "review_failed" && prev != "review_failed" {
        return WatchEval {
            triggered: true,
            next_state: "active",
            transition: Some("review_failed".to_string()),
            observed: Some("review_failed".to_string()),
            baseline: Some(next_baseline),
        };
    }
    if state_desc == "checks_completed" && prev != "checks_completed" {
        return WatchEval {
            triggered: true,
            next_state: "active",
            transition: Some("checks_completed".to_string()),
            observed: Some("checks_completed".to_string()),
            baseline: Some(next_baseline),
        };
    }
    WatchEval::pending(Some(next_baseline), Some(state_desc))
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

/// One client for every Spiron probe, so repeated probes reuse its pooled
/// keep-alive connections instead of a new TLS handshake per watch per pass.
static SPIRON_HTTP: LazyLock<ureq::Agent> = LazyLock::new(|| {
    ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(5)))
        .max_redirects(0)
        .http_status_as_error(false)
        .build()
        .new_agent()
});

fn probe_spiron_at(
    base: &str,
    key: Option<&str>,
    id: &str,
    kind: &str,
) -> Result<String, SpironProbeError> {
    let agent = &*SPIRON_HTTP;
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

fn probe_github_pr_state(
    owner: &str,
    repo: &str,
    number: u64,
    agent_id: &str,
    now_ms: i64,
) -> Option<(bool, String, Option<String>)> {
    github::probe_pr(owner, repo, &number.to_string(), agent_id, now_ms)
}

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
    fn expired_quota_deadline_with_stale_rate_limit_text_is_not_blocked() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let owner = store
            .create_agent(NewAgent::new("recovered-owner", "codex"))
            .unwrap();
        store.set_agent_running(&owner.id, "/work").unwrap();
        let now = anyagents_core::now_ms();
        let mut live = LiveState {
            process_alive: true,
            turn_state: TurnState::Idle,
            delivery_mode: DeliveryMode::Steer,
            steer_support: SteerSupport::Yes,
            quota_reset_at_ms: Some(now - 1_000),
            restart_at_ms: None,
            pid: None,
            live_harness: None,
            live_model: None,
            process_started_at_ms: Some(1000),
            last_error: Some("HTTP 429: rate limit, quota exceeded".into()),
        };
        health.publish(&owner.id, live.clone());
        let owner = store.agent(&owner.id).unwrap();
        assert_eq!(
            agent_wake_block_reason(&store, &health, &owner, false, now),
            None
        );
        live.quota_reset_at_ms = Some(now + 60_000);
        health.publish(&owner.id, live.clone());
        assert_eq!(
            agent_wake_block_reason(&store, &health, &owner, false, now),
            Some("quota_blocked")
        );
        live.quota_reset_at_ms = Some(now - 1_000);
        health.publish(&owner.id, live);

        let sched = create_agent_schedule(
            &store,
            &health,
            &owner,
            None,
            Some("1m"),
            None,
            "recovered check",
        )
        .unwrap();
        let due = sched.next_fire_at_ms.unwrap();
        assert_eq!(tick_schedules_once(&store, &health, due), 1);
        let inbox = store.inbox(&owner.id, None).unwrap();
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].text, "recovered check");
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
        let error = evaluate_pr_observation(&sched, "probe_error:rate_limited".to_string(), None);
        assert!(!error.triggered);
        assert_eq!(error.baseline, sched.watch_baseline);
        let evicted = evaluate_pr_observation(&sched, "checks_completed".to_string(), None);
        assert!(evicted.triggered);
        assert_eq!(evicted.transition.as_deref(), Some("evicted"));
        assert_eq!(evicted.next_state, "active");
        sched.watch_baseline = evicted.baseline;
        assert!(!evaluate_pr_observation(&sched, "checks_completed".to_string(), None).triggered);
    }

    #[test]
    fn pr_watch_head_sha_transition_rules_and_legacy_baselines() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let owner = store
            .create_agent(NewAgent::new("pr-head-sha-watch", "codex"))
            .unwrap();
        let mut sched = store
            .create_schedule(NewSchedule {
                agent_id: owner.id,
                kind: "watch".to_string(),
                spec: "github:pr:o/r#42".to_string(),
                prompt: "verify".to_string(),
                interval_ms: None,
                next_fire_at_ms: None,
                watch_baseline: Some("pending".to_string()),
            })
            .unwrap();

        // Initial completion on sha1 triggers and encodes sha1 in baseline.
        let first = evaluate_pr_observation(
            &sched,
            "checks_completed".to_string(),
            Some("sha1".to_string()),
        );
        assert!(first.triggered);
        assert_eq!(first.transition.as_deref(), Some("checks_completed"));
        assert_eq!(first.observed.as_deref(), Some("checks_completed"));
        assert_eq!(first.baseline.as_deref(), Some("checks_completed@sha1"));
        sched.watch_baseline = first.baseline;

        // Same head_sha staying checks_completed does NOT re-trigger.
        let same_head = evaluate_pr_observation(
            &sched,
            "checks_completed".to_string(),
            Some("sha1".to_string()),
        );
        assert!(!same_head.triggered);
        assert_eq!(same_head.baseline.as_deref(), Some("checks_completed@sha1"));

        // New head_sha reaching checks_completed after an earlier checks_completed DOES trigger.
        let new_head = evaluate_pr_observation(
            &sched,
            "checks_completed".to_string(),
            Some("sha2".to_string()),
        );
        assert!(new_head.triggered);
        assert_eq!(new_head.transition.as_deref(), Some("checks_completed"));
        assert_eq!(new_head.observed.as_deref(), Some("checks_completed"));
        assert_eq!(new_head.baseline.as_deref(), Some("checks_completed@sha2"));
        sched.watch_baseline = new_head.baseline;

        // New head_sha still in progress resets baseline to checks_in_progress@sha3 without waking,
        // then triggers when sha3 reaches checks_completed.
        let new_head_pending = evaluate_pr_observation(
            &sched,
            "checks_in_progress".to_string(),
            Some("sha3".to_string()),
        );
        assert!(!new_head_pending.triggered);
        assert_eq!(
            new_head_pending.baseline.as_deref(),
            Some("checks_in_progress@sha3")
        );
        sched.watch_baseline = new_head_pending.baseline;
        let new_head_done = evaluate_pr_observation(
            &sched,
            "checks_completed".to_string(),
            Some("sha3".to_string()),
        );
        assert!(new_head_done.triggered);
        assert_eq!(
            new_head_done.baseline.as_deref(),
            Some("checks_completed@sha3")
        );

        // Legacy baseline without head_sha (e.g. "checks_completed") adopts the observed head_sha
        // without spurious re-triggering, then triggers when a newer head_sha completes.
        sched.watch_baseline = Some("checks_completed".to_string());
        let legacy_same = evaluate_pr_observation(
            &sched,
            "checks_completed".to_string(),
            Some("sha4".to_string()),
        );
        assert!(!legacy_same.triggered);
        assert_eq!(
            legacy_same.baseline.as_deref(),
            Some("checks_completed@sha4")
        );
        sched.watch_baseline = legacy_same.baseline;
        let legacy_next = evaluate_pr_observation(
            &sched,
            "checks_completed".to_string(),
            Some("sha5".to_string()),
        );
        assert!(legacy_next.triggered);
        assert_eq!(
            legacy_next.baseline.as_deref(),
            Some("checks_completed@sha5")
        );
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
        github::reset_probes_for_test();
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
        let watched = store.list_schedules(None, true).unwrap();
        let targets: Vec<_> = watched
            .iter()
            .map(|s| (s.id.as_str(), parse_watch_spec(&s.spec).unwrap()))
            .collect();
        github::begin_cycle(targets.iter().map(|(id, t)| (*id, t)), now);
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
            Some("checks_completed@abc")
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
        // The review failure and the merge are outcomes of the owner's work.
        let outcomes = store
            .outcome_report(
                std::slice::from_ref(&owner.id),
                None,
                now + 700_000,
                anyagents_core::OutcomeGroupBy::Agent,
            )
            .unwrap();
        let group = &outcomes.groups[0];
        assert_eq!(group.events.get("review_failed"), Some(&1));
        assert_eq!(group.events.get("pr_merged"), Some(&1));
        assert_eq!(
            (group.succeeded, group.reworked, group.recovered),
            (1, 1, 1)
        );
        *stage.lock().unwrap() = 99;
        let _ = std::net::TcpStream::connect(addr);
        server.join().unwrap();
        github::reset_probes_for_test();
        unsafe {
            std::env::remove_var("ANYAGENTS_GITHUB_API_BASE");
        }
    }

    #[test]
    fn pull_request_merge_is_recorded_while_an_earlier_wake_waits() {
        let _env = crate::server::tests::env_lock();
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let mut buf = [0u8; 2048];
                let len = stream.read(&mut buf).unwrap_or(0);
                let request = String::from_utf8_lossy(&buf[..len]);
                if request.contains("/stop") {
                    break;
                }
                assert!(request.contains("/pulls/"), "{request}");
                let body = r#"{"state":"closed","merged":true,"head":{"sha":"abc"}}"#;
                write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            }
        });
        struct EnvGuard;
        impl Drop for EnvGuard {
            fn drop(&mut self) {
                github::reset_probes_for_test();
                unsafe {
                    std::env::remove_var("ANYAGENTS_GITHUB_API_BASE");
                }
            }
        }
        github::reset_probes_for_test();
        unsafe {
            std::env::set_var("ANYAGENTS_GITHUB_API_BASE", format!("http://{addr}"));
        }
        let _guard = EnvGuard;

        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let owner = store
            .create_agent(NewAgent::new("owner-pending-merge", "codex"))
            .unwrap();
        store.set_agent_running(&owner.id, "/work").unwrap();
        let sched = create_agent_schedule(
            &store,
            &health,
            &owner,
            None,
            None,
            Some("github:pr:ExampleOrg/project#454"),
            "verify PR 454",
        )
        .unwrap();
        // An earlier wake from this watch is still in the mailbox.
        let mut earlier = Incoming::new("[watch: earlier]");
        earlier.sender = Some(schedule_sender(&sched.id));
        store.receive(&owner.id, earlier).unwrap();

        let now = anyagents_core::now_ms();
        let watched = store.list_schedules(None, true).unwrap();
        let targets: Vec<_> = watched
            .iter()
            .map(|s| (s.id.as_str(), parse_watch_spec(&s.spec).unwrap()))
            .collect();
        github::begin_cycle(targets.iter().map(|(id, t)| (*id, t)), now);
        let tick =
            |at| github::with_completion_clock(at, || tick_schedules_once(&store, &health, at));
        let merged = |until| {
            store
                .outcome_report(
                    std::slice::from_ref(&owner.id),
                    None,
                    until,
                    anyagents_core::OutcomeGroupBy::Agent,
                )
                .unwrap()
                .groups
                .first()
                .and_then(|group| group.events.get("pr_merged").copied())
        };

        assert_eq!(tick(now), 0);
        assert_eq!(merged(now), Some(1));
        assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 1);
        let active = store.list_schedules(Some(&owner.id), true).unwrap();
        assert_eq!(active[0].last_observed.as_deref(), Some("merged"));

        // Once the earlier wake is taken, the merge wakes the owner; the fact
        // stays one row.
        let pending = store.inbox(&owner.id, None).unwrap();
        store.note_delivered(&owner.id, pending[0].seq).unwrap();
        store.note_turn_completed(&owner.id).unwrap();
        assert_eq!(tick(now + 60_000), 1);
        assert!(
            store
                .inbox(&owner.id, None)
                .unwrap()
                .last()
                .unwrap()
                .text
                .contains("transition=merged]")
        );
        assert_eq!(merged(now + 60_000), Some(1));

        let _ = std::net::TcpStream::connect(addr)
            .and_then(|mut stream| stream.write_all(b"GET /stop HTTP/1.1\r\n\r\n"));
        server.join().unwrap();
    }

    #[test]
    fn github_pr_watch_wakes_on_new_head_sha_checks_completed() {
        let _env = crate::server::tests::env_lock();
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::sync::{Arc, Mutex};

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let stage = Arc::new(Mutex::new(0));
        let server_stage = Arc::clone(&stage);
        let server = std::thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let mut buf = [0u8; 2048];
                let len = stream.read(&mut buf).unwrap_or(0);
                let request = String::from_utf8_lossy(&buf[..len]);
                let stage = *server_stage.lock().unwrap();
                if stage == 99 {
                    break;
                }
                let sha = if stage == 0 { "sha111" } else { "sha222" };
                let body = if request.contains("/pulls/") {
                    format!(r#"{{"state":"open","merged":false,"head":{{"sha":"{sha}"}}}}"#)
                } else if request.contains("/check-runs") {
                    r#"{"total_count":1,"check_runs":[{"id":1,"name":"ci","status":"completed","conclusion":"success"}]}"#.to_string()
                } else if request.contains("/status?") {
                    r#"{"statuses":[]}"#.to_string()
                } else {
                    panic!("unexpected REST request: {request}");
                };
                write!(
                    stream,
                    "HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: {}
Connection: close

{}",
                    body.len(),
                    body
                )
                .unwrap();
            }
        });
        struct EnvGuard;
        impl Drop for EnvGuard {
            fn drop(&mut self) {
                github::reset_probes_for_test();
                unsafe {
                    std::env::remove_var("ANYAGENTS_GITHUB_API_BASE");
                }
            }
        }
        github::reset_probes_for_test();
        unsafe {
            std::env::set_var("ANYAGENTS_GITHUB_API_BASE", format!("http://{addr}"));
        }
        let _guard = EnvGuard;

        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let owner = store
            .create_agent(NewAgent::new("owner-head-sha-pr", "codex"))
            .unwrap();
        store.set_agent_running(&owner.id, "/work").unwrap();
        create_agent_schedule(
            &store,
            &health,
            &owner,
            None,
            None,
            Some("github:pr:ExampleOrg/project#453"),
            "verify PR 453",
        )
        .unwrap();

        let now = anyagents_core::now_ms();
        let watched = store.list_schedules(None, true).unwrap();
        let targets: Vec<_> = watched
            .iter()
            .map(|s| (s.id.as_str(), parse_watch_spec(&s.spec).unwrap()))
            .collect();
        github::begin_cycle(targets.iter().map(|(id, t)| (*id, t)), now);
        let tick = |store: &Store, health: &HealthBoard, at| {
            github::with_completion_clock(at, || tick_schedules_once(store, health, at))
        };

        // Initial checks_completed on sha111 wakes the agent.
        assert_eq!(tick(&store, &health, now), 1);
        let inbox = store.inbox(&owner.id, None).unwrap();
        let first = inbox.last().unwrap();
        assert!(first.text.contains("transition=checks_completed]"));
        assert_eq!(
            store.list_schedules(Some(&owner.id), true).unwrap()[0]
                .watch_baseline
                .as_deref(),
            Some("checks_completed@sha111")
        );
        store.note_delivered(&owner.id, first.seq).unwrap();
        store.note_turn_completed(&owner.id).unwrap();

        // Same head_sha staying checks_completed does not re-trigger.
        assert_eq!(tick(&store, &health, now + 60_000), 0);
        assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 1);

        // New head_sha (sha222) also finishing as checks_completed triggers a new wake.
        *stage.lock().unwrap() = 1;
        assert_eq!(tick(&store, &health, now + 180_000), 1);
        let inbox = store.inbox(&owner.id, None).unwrap();
        assert_eq!(inbox.len(), 2);
        assert!(
            inbox
                .last()
                .unwrap()
                .text
                .contains("transition=checks_completed]")
        );
        assert_eq!(
            store.list_schedules(Some(&owner.id), true).unwrap()[0]
                .watch_baseline
                .as_deref(),
            Some("checks_completed@sha222")
        );

        *stage.lock().unwrap() = 99;
        let _ = std::net::TcpStream::connect(addr);
        server.join().unwrap();
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
            agent_wake_block_reason(&store, &health, &agent, false, 1),
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
    fn spiron_probes_share_one_keep_alive_connection() {
        use std::io::{BufRead, BufReader, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        // The peer accepts exactly one connection and answers two requests on
        // it; a probe that dialed again would find no listener and fail.
        let peer = std::thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            drop(listener);
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut reader = BufReader::new(socket.try_clone().unwrap());
            let mut writer = socket;
            for state in ["pending", "complete"] {
                let mut line = String::new();
                while reader.read_line(&mut line).unwrap() > 0 && line != "\r\n" {
                    line.clear();
                }
                let body = format!(r#"{{"state":"{state}"}}"#);
                write!(
                    writer,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        assert_eq!(
            probe_spiron_at(&base, None, "id", "promise"),
            Ok("pending".into())
        );
        assert_eq!(
            probe_spiron_at(&base, None, "id", "promise"),
            Ok("complete".into())
        );
        peer.join().unwrap();
    }

    #[test]
    fn legacy_pr_watch_repair_runs_once_per_database() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");
        let store = Store::open(&db).unwrap();
        let health = HealthBoard::default();
        let owner = store
            .create_agent(NewAgent::new("legacy-owner", "codex"))
            .unwrap();
        let watch = store
            .create_schedule(NewSchedule {
                agent_id: owner.id.clone(),
                kind: "watch".into(),
                spec: "github:pr:o/r#7".into(),
                prompt: "verify".into(),
                interval_ms: None,
                next_fire_at_ms: None,
                watch_baseline: Some("pending".into()),
            })
            .unwrap();
        let retire = |conn: &rusqlite::Connection| {
            conn.execute(
                "UPDATE agent_schedules SET state='fired', last_observed='checks_completed' WHERE id=?1",
                [watch.id.as_str()],
            )
            .unwrap();
        };
        let raw = rusqlite::Connection::open(&db).unwrap();
        retire(&raw);
        // The first local pass reopens what an earlier release retired.
        assert!(restore_legacy_pr_watches_once(&store));
        let reopened = store.get_schedule(&watch.id).unwrap().unwrap();
        assert_eq!(reopened.state, "active");
        assert_eq!(reopened.watch_baseline.as_deref(), Some("checks_completed"));
        // Later passes, including from a reopened handle, never rescan.
        retire(&raw);
        drop(store);
        let store = Store::open(&db).unwrap();
        for at in 0..3 {
            tick_local_schedules_once(&store, &health, anyagents_core::now_ms() + at);
        }
        assert_eq!(
            store.get_schedule(&watch.id).unwrap().unwrap().state,
            "fired"
        );
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

    fn timer_schedule(store: &Store, owner: &AgentId, kind: &str, spec: &str, due: Option<i64>) {
        store
            .create_schedule(NewSchedule {
                agent_id: owner.clone(),
                kind: kind.into(),
                spec: spec.into(),
                prompt: format!("{kind} {spec}"),
                interval_ms: (kind == "every").then_some(3_600_000),
                next_fire_at_ms: due,
                watch_baseline: None,
            })
            .unwrap();
    }

    #[test]
    fn idle_timer_touches_the_store_only_at_its_deadlines() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let owner = store
            .create_agent(NewAgent::new("idle-owner", "codex"))
            .unwrap();
        let now = anyagents_core::now_ms();
        timer_schedule(&store, &owner.id, "at", "later", Some(now + 3_600_000));
        timer_schedule(&store, &owner.id, "every", "1h", Some(now + 3_600_000));
        let mut timer = Timer::local();
        let first = timer.step(&store, &health, 0, now);
        assert!(first.worked);
        let mut deadline = first.deadline;
        // Ten idle minutes, stepped every second as the old census was: the store
        // is read only when the guard re-reads the fleet (15 s) or the schedules
        // are re-read as a safety net (60 s), never on the seconds between.
        let mut worked_at = Vec::new();
        for second in 1..=600 {
            let at = now + second * 1_000;
            let step = timer.step(&store, &health, 0, at);
            if step.worked {
                assert!(at >= deadline, "worked at {second}s before its deadline");
                worked_at.push(second);
            } else {
                assert!(at < deadline);
            }
            deadline = step.deadline;
        }
        assert_eq!(worked_at, (1..=40).map(|n| n * 15).collect::<Vec<_>>());
        assert!(store.inbox(&owner.id, None).unwrap().is_empty());
        // Nothing was due, so nothing was written.
        let saved = store.list_schedules(Some(&owner.id), true).unwrap();
        assert!(saved.iter().all(|s| s.last_fired_at_ms.is_none()));
    }

    #[test]
    fn timer_fires_each_kind_at_its_due_time() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let now = anyagents_core::now_ms();
        let at_owner = store
            .create_agent(NewAgent::new("at-owner", "codex"))
            .unwrap();
        let every_owner = store
            .create_agent(NewAgent::new("every-owner", "codex"))
            .unwrap();
        let watch_owner = store
            .create_agent(NewAgent::new("watch-owner", "codex"))
            .unwrap();
        timer_schedule(&store, &at_owner.id, "at", "soon", Some(now + 2_500));
        timer_schedule(&store, &every_owner.id, "every", "1h", Some(now + 4_000));
        let path = dir.path().join("signal");
        store
            .create_schedule(NewSchedule {
                agent_id: watch_owner.id.clone(),
                kind: "watch".into(),
                spec: format!("file:{}", path.display()),
                prompt: "file appeared".into(),
                interval_ms: None,
                next_fire_at_ms: None,
                watch_baseline: Some("missing".into()),
            })
            .unwrap();
        let mut timer = Timer::local();
        let inbox = |id: &AgentId| store.inbox(id, None).unwrap().len();
        let deadline = timer.step(&store, &health, 0, now).deadline;
        // A local watch is probed each second; nothing else is due yet.
        assert_eq!(deadline, now + LOCAL_WATCH_PROBE_MS);
        timer.step(&store, &health, 0, now + 1_000);
        std::fs::write(&path, "ready").unwrap();
        let deadline = timer.step(&store, &health, 0, now + 2_000).deadline;
        assert_eq!(inbox(&watch_owner.id), 1);
        // The watch fired and is gone, so the next step is the `at` due time.
        assert_eq!(deadline, now + 2_500);
        assert!(!timer.step(&store, &health, 0, now + 2_499).worked);
        assert_eq!(inbox(&at_owner.id), 0);
        let deadline = timer.step(&store, &health, 0, now + 2_500).deadline;
        assert_eq!(inbox(&at_owner.id), 1);
        assert_eq!(deadline, now + 4_000);
        let deadline = timer.step(&store, &health, 0, now + 4_000).deadline;
        assert_eq!(inbox(&every_owner.id), 1);
        // The recurring schedule sleeps until its next slot or the guard's re-read.
        assert_eq!(
            deadline,
            (now + GUARD_REFRESH_MS).min(now + 4_000 + 3_600_000)
        );
        for id in [&at_owner.id, &every_owner.id, &watch_owner.id] {
            assert_eq!(inbox(id), 1);
        }
    }

    #[test]
    fn timer_coalesces_every_ticks_for_a_paused_owner() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let owner = store
            .create_agent(NewAgent::new("paused-timer-owner", "codex"))
            .unwrap();
        let now = anyagents_core::now_ms();
        store
            .create_schedule(NewSchedule {
                agent_id: owner.id.clone(),
                kind: "every".into(),
                spec: "1s".into(),
                prompt: "tick".into(),
                interval_ms: Some(1_000),
                next_fire_at_ms: Some(now + 1_000),
                watch_baseline: None,
            })
            .unwrap();
        store.set_agent_state(&owner.id, "paused").unwrap();
        let mut timer = Timer::local();
        let mut at = now;
        while at < now + 100_000 {
            at = timer.step(&store, &health, 0, at).deadline;
        }
        let inbox = store.inbox(&owner.id, None).unwrap();
        assert_eq!(inbox.len(), 1);
        let saved = &store.list_schedules(Some(&owner.id), true).unwrap()[0];
        assert_eq!(saved.last_observed.as_deref(), Some("coalesced"));
        assert_eq!(saved.last_fired_at_ms, Some(now + 1_000));
    }

    #[test]
    fn failed_delivery_retries_after_a_second_without_spinning() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let owner = store
            .create_agent(NewAgent::new("locked-owner", "codex"))
            .unwrap();
        let now = anyagents_core::now_ms();
        timer_schedule(&store, &owner.id, "at", "now", Some(now));
        let mut timer = Timer::local();
        let held = store.lock_agent_lifecycle(&owner.id).unwrap();
        let deadline = timer.step(&store, &health, 0, now).deadline;
        assert!(store.inbox(&owner.id, None).unwrap().is_empty());
        assert_eq!(deadline, now + RETRY_MS);
        assert!(!timer.step(&store, &health, 0, now + 500).worked);
        drop(held);
        timer.step(&store, &health, 0, now + RETRY_MS);
        assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 1);
    }

    #[test]
    fn schedule_change_wakes_the_running_timer_at_once() {
        use std::sync::Arc;
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let health = HealthBoard::default();
        let silent = store
            .create_agent(NewAgent::new("silent-owner", "codex"))
            .unwrap();
        let notified = store
            .create_agent(NewAgent::new("notified-owner", "codex"))
            .unwrap();
        let first = store
            .create_agent(NewAgent::new("first-owner", "codex"))
            .unwrap();
        timer_schedule(
            &store,
            &first.id,
            "at",
            "now",
            Some(anyagents_core::now_ms()),
        );
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let (local, external) = start_loops(Arc::clone(&store), health.clone());
            // The startup step fires what is already due.
            let started = std::time::Instant::now();
            while store.inbox(&first.id, None).unwrap().is_empty() {
                assert!(
                    started.elapsed() < Duration::from_secs(5),
                    "no startup step"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
            // A row written behind the module's back is not polled for: the timer
            // sleeps until a deadline or a change.
            timer_schedule(
                &store,
                &silent.id,
                "at",
                "now",
                Some(anyagents_core::now_ms()),
            );
            tokio::time::sleep(Duration::from_millis(1_500)).await;
            assert!(store.inbox(&silent.id, None).unwrap().is_empty());
            // A schedule created through this module wakes the timer at once; its
            // re-read also finds the earlier row.
            let created_at = std::time::Instant::now();
            create_agent_schedule(
                &store,
                &health,
                &notified,
                Some(&anyagents_core::now_ms().to_string()),
                None,
                None,
                "wake now",
            )
            .unwrap();
            while store.inbox(&notified.id, None).unwrap().is_empty()
                || store.inbox(&silent.id, None).unwrap().is_empty()
            {
                assert!(
                    created_at.elapsed() < Duration::from_secs(5),
                    "the change did not wake the timer"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            local.abort();
            external.abort();
        });
        runtime.shutdown_timeout(Duration::from_secs(5));
    }

    #[test]
    fn external_timer_sleeps_until_the_github_target_can_be_probed() {
        let _env = crate::server::tests::env_lock();
        let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = closed.local_addr().unwrap();
        drop(closed);
        let previous = std::env::var_os("ANYAGENTS_GITHUB_API_BASE");
        // SAFETY: serialized with all other process-wide GitHub API overrides.
        unsafe {
            std::env::set_var("ANYAGENTS_GITHUB_API_BASE", format!("http://{address}"));
        }
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let owner = store
            .create_agent(NewAgent::new("pr-owner", "codex"))
            .unwrap();
        store
            .create_schedule(NewSchedule {
                agent_id: owner.id.clone(),
                kind: "watch".into(),
                spec: "github:pr:example-org/sleepy#9".into(),
                prompt: "pr".into(),
                interval_ms: None,
                next_fire_at_ms: None,
                watch_baseline: Some("pending".into()),
            })
            .unwrap();
        let mut timer = Timer::external();
        let now = anyagents_core::now_ms();
        let step = github::with_completion_clock(now, || timer.step(&store, &health, 0, now));
        unsafe {
            match previous {
                Some(value) => std::env::set_var("ANYAGENTS_GITHUB_API_BASE", value),
                None => std::env::remove_var("ANYAGENTS_GITHUB_API_BASE"),
            }
        }
        assert!(step.worked);
        // The unreachable API is an observation; its target is not probed again
        // for a minute, and the external timer sleeps that long, not 3 s.
        let saved = &store.list_schedules(Some(&owner.id), true).unwrap()[0];
        assert_eq!(
            saved.last_observed.as_deref(),
            Some("probe_error:transport")
        );
        assert_eq!(step.deadline, now + 60_000);
        assert!(!timer.step(&store, &health, 0, now + 3_000).worked);
    }

    #[test]
    fn busy_idle_watch_target_answers_from_published_turn_without_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");
        let store = Store::open(&db).unwrap();
        let health = HealthBoard::default();
        let owner = store
            .create_agent(NewAgent::new("waiter", "codex"))
            .unwrap();
        let target = store
            .create_agent(NewAgent::new("worker", "codex"))
            .unwrap();
        let sched = store
            .create_schedule(NewSchedule {
                agent_id: owner.id.clone(),
                kind: "watch".into(),
                spec: "agent:idle:worker".into(),
                prompt: "worker idle".into(),
                interval_ms: None,
                next_fire_at_ms: None,
                watch_baseline: None,
            })
            .unwrap();
        let watch = parse_watch_spec(&sched.spec).ok();
        let mut hint = None;
        publish_turn(&health, &target.id, TurnState::InTurn, None);
        let now = anyagents_core::now_ms();
        let eval = evaluate_watch(&store, &health, &sched, watch.as_ref(), &mut hint, now);
        assert_eq!(eval.observed.as_deref(), Some("working"));
        assert_eq!(hint.as_ref(), Some(&target.id));
        // With the agent table unreadable, the published turn still answers.
        let raw = rusqlite::Connection::open(&db).unwrap();
        raw.execute_batch("ALTER TABLE agents RENAME TO unavailable_agents")
            .unwrap();
        let eval = evaluate_watch(&store, &health, &sched, watch.as_ref(), &mut hint, now);
        assert_eq!(eval.observed.as_deref(), Some("working"));
        assert!(!eval.triggered);
        raw.execute_batch("ALTER TABLE unavailable_agents RENAME TO agents")
            .unwrap();
        // Once the turn ends, the store decides, and the watch fires.
        publish_turn(&health, &target.id, TurnState::Idle, None);
        let eval = evaluate_watch(&store, &health, &sched, watch.as_ref(), &mut hint, now);
        assert!(eval.triggered);
        assert_eq!(eval.observed.as_deref(), Some("idle"));
    }

    #[test]
    fn turn_guard_holds_for_schedules_busy_children_and_busy_parents() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let parent = store
            .create_agent(NewAgent::new("hold-parent", "codex"))
            .unwrap();
        let mut child_new = NewAgent::new("hold-child", "codex");
        child_new.parent_id = Some(parent.id.clone());
        let child = store.create_agent(child_new).unwrap();
        let mut task = Incoming::new("Execute task");
        task.sender = Some(format!("agent:{}", parent.name));
        store.receive(&child.id, task).unwrap();
        store.note_delivered(&child.id, 1).unwrap();
        store.note_turn_completed(&child.id).unwrap();
        let delivered_at = |store: &Store| {
            store
                .turn_ownership_snapshot(&child.id)
                .unwrap()
                .last_delivered_at_ms
                .unwrap()
        };
        let due = delivered_at(&store) + 20_000;
        // An active schedule means the child still owns its next step.
        let sched = create_agent_schedule(&store, &health, &child, None, Some("1h"), None, "check")
            .unwrap();
        assert_eq!(tick_task_subagent_turn_ownership(&store, &health, due), 0);
        cancel_agent_schedule(&store, &child.id, &sched.id).unwrap();
        // A child mid-turn is not nudged; once idle, it is, once.
        publish_turn(&health, &child.id, TurnState::InTurn, None);
        assert_eq!(tick_task_subagent_turn_ownership(&store, &health, due), 0);
        publish_turn(&health, &child.id, TurnState::Idle, None);
        assert_eq!(tick_task_subagent_turn_ownership(&store, &health, due), 1);
        assert_eq!(tick_task_subagent_turn_ownership(&store, &health, due), 0);
        // The nudge is taken and the child ends again without reporting: the
        // parent is alerted, but not while it is mid-turn itself.
        store.note_delivered(&child.id, 2).unwrap();
        store.note_turn_completed(&child.id).unwrap();
        let alert_due = delivered_at(&store) + 20_000;
        publish_turn(&health, &parent.id, TurnState::InTurn, None);
        assert_eq!(
            tick_task_subagent_turn_ownership(&store, &health, alert_due),
            0
        );
        publish_turn(&health, &parent.id, TurnState::Idle, None);
        assert_eq!(
            tick_task_subagent_turn_ownership(&store, &health, alert_due),
            1
        );
        let parent_inbox = store.inbox(&parent.id, None).unwrap();
        assert!(
            parent_inbox
                .last()
                .unwrap()
                .text
                .contains("exited its turn without calling `send`")
        );
    }

    #[test]
    fn turn_guard_acts_when_due_or_when_a_blocking_turn_ends() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let parent = store
            .create_agent(NewAgent::new("guard-parent", "codex"))
            .unwrap();
        let mut child_new = NewAgent::new("guard-child", "codex");
        child_new.parent_id = Some(parent.id.clone());
        let child = store.create_agent(child_new).unwrap();
        assert!(child.is_task_subagent());
        let mut task = Incoming::new("Execute task");
        task.sender = Some(format!("agent:{}", parent.name));
        store.receive(&child.id, task).unwrap();
        store.note_delivered(&child.id, 1).unwrap();
        let delivered = store
            .turn_ownership_snapshot(&child.id)
            .unwrap()
            .last_delivered_at_ms
            .unwrap();
        publish_turn(&health, &child.id, TurnState::InTurn, None);
        let mut guard = TurnGuard::default();
        // The first full pass learns the delivery and waits for its nudge time.
        assert!(guard.due(&health, delivered + 5_000));
        assert_eq!(guard.run(&store, &health, delivered + 5_000), 0);
        let refresh = delivered + 5_000 + GUARD_REFRESH_MS;
        assert_eq!(
            guard.next_deadline(delivered + 5_000),
            delivered + CHILD_NUDGE_AFTER_MS
        );
        assert!(!guard.due(&health, delivered + 8_000));
        // Due, but the child is mid-turn: nothing is sent and its turn is watched.
        let due = delivered + CHILD_NUDGE_AFTER_MS;
        assert!(guard.due(&health, due));
        assert_eq!(guard.run(&store, &health, due), 0);
        assert!(!guard.due(&health, due + 200));
        assert_eq!(guard.next_deadline(due + 200), due + 200 + GUARD_WATCH_MS);
        // The turn ends: the watch, not the next full pass, sends the nudge.
        publish_turn(&health, &child.id, TurnState::Idle, None);
        store.note_turn_completed(&child.id).unwrap();
        let ended = due + 400;
        assert!(ended < refresh);
        assert!(guard.due(&health, ended));
        assert_eq!(guard.run(&store, &health, ended), 1);
        let inbox = store.inbox(&child.id, None).unwrap();
        assert!(
            inbox
                .last()
                .unwrap()
                .text
                .contains("[Turn Ownership Guard]")
        );
        // Settled: no watch, no work until the next full pass.
        assert!(!guard.due(&health, ended + 1_000));
        assert_eq!(guard.run(&store, &health, refresh), 0);
        assert_eq!(store.inbox(&child.id, None).unwrap().len(), inbox.len());
    }
}
