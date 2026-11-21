//! Per-agent self-scheduled wakes (`at`, `every`) and event watches (`watch`).
//!
//! Every agent on any harness can schedule its own one-off wake (`at`), recurring
//! wake (`every`), or event watch (`watch`: generic webhook/event trigger
//! `webhook:<topic>`, GitHub PR/check terminal state `github:pr:*`/`github:check:*`,
//! an AnyAgents agent going idle, a local process exiting, or a file changing).
//!
//! A due tick or a triggered watch is written to the owner's mailbox at once, like
//! any other message. Mailbox delivery alone decides when the owner sees it: it is
//! steered into a running turn, or held while the owner is paused or quota-blocked
//! and delivered on resume. A schedule adds no new tick while its previous one is
//! still undelivered, so missed recurring ticks coalesce into one. Persistent webhook
//! watches instead retain each distinct delivery. Cancelling a
//! schedule takes effect immediately.
//!
//! The server's loop ([`start_loops`]) holds active schedules in memory and wakes
//! reactively on schedule mutations, kernel `inotify`/`pidfd` events for local
//! paths and processes, and [`Store`]/[`HealthBoard`] state-change feeds for
//! `agent:idle` and turn-ownership blocks; authenticated webhook deliveries fire
//! matching `webhook:<topic>` watches directly via [`fire_webhook_event`].

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use anyagents_core::{Agent, AgentId, AgentSchedule, Incoming, NewSchedule, Store};
use anyagents_supervisor::HealthBoard;

use crate::api::ScheduleView;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum WatchTarget {
    Webhook {
        topic: String,
    },
    GitHubPr {
        owner: String,
        repo: String,
        number: u64,
    },
    GitHubCheck {
        owner: String,
        repo: String,
        reference: String,
        check_name: Option<String>,
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
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp_millis());
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

/// Parse and validate a `watch` event specification.
pub fn parse_watch_spec(raw: &str) -> Result<WatchTarget, String> {
    let s = raw.trim();
    if s.is_empty() {
        return Err("`watch` cannot be empty".to_string());
    }

    if let Some(parsed) = github::parse_github_watch_spec(s) {
        return parsed;
    }

    // Generic webhook / event trigger forms:
    // "webhook:<topic>", "event:<topic>"
    if let Some(topic) = s
        .strip_prefix("webhook:")
        .or_else(|| s.strip_prefix("event:"))
    {
        let clean = topic.trim();
        if clean.is_empty()
            || clean.len() > 128
            || !clean.bytes().all(|b| {
                b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':' | b'/' | b'*')
            })
        {
            return Err(format!(
                "invalid webhook topic `{clean}`; expected 1-128 ASCII letters, digits, or `-`, `_`, `.`, `:`, `/`, `*`"
            ));
        }
        return Ok(WatchTarget::Webhook {
            topic: clean.to_string(),
        });
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
        "unsupported watch spec `{raw}`; expected `github:pr:<owner>/<repo>#<num>`, `github:check:<owner>/<repo>@<ref>`, `webhook:<topic>`, `event:<topic>`, `agent:idle:<name>`, `pid:<pid>`, or `file:<path>`"
    ))
}

/// One correct call, quoted in every schedule error.
const SCHEDULE_EXAMPLE: &str = "`{action: \"create\", watch: \"webhook:deploy-ready\", prompt: \"Deploy finished; verify it\"}`";

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
    github::is_github_watch_scheme(value)
        || [
            "webhook:", "event:", "agent:", "pid:", "process:", "file:", "path:",
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
                    watch: Some("webhook:deploy-ready".into()),
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
                spec: Some("event:ci.finished".into()),
                ..Default::default()
            },
        );
        assert_eq!(verb, Ok(ScheduleVerb::Create));
        assert_eq!(resolved.watch.as_deref(), Some("event:ci.finished"));
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

/// Create a schedule with the default one-shot watch behavior.
#[cfg(test)]
pub fn create_agent_schedule(
    store: &Store,
    health: &HealthBoard,
    agent: &Agent,
    at: Option<&str>,
    every: Option<&str>,
    watch: Option<&str>,
    prompt: &str,
) -> Result<ScheduleView, String> {
    create_agent_schedule_with_mode(store, health, agent, at, every, watch, prompt, false)
}

#[allow(clippy::too_many_arguments)]
pub fn create_agent_schedule_with_mode(
    store: &Store,
    health: &HealthBoard,
    agent: &Agent,
    at: Option<&str>,
    every: Option<&str>,
    watch: Option<&str>,
    prompt: &str,
    persistent: bool,
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
            "a schedule needs one trigger: `at` (\"20m\" or an ISO time), `every` (\"15m\") or `watch` (\"github:pr:<owner>/<repo>#<num>\", \"github:check:<owner>/<repo>@<ref>\", \"webhook:<topic>\", \"agent:idle:<name>\", \"pid:<pid>\", \"file:<path>\"), e.g. {SCHEDULE_EXAMPLE}"
        ));
    }
    if set.len() > 1 {
        return Err(format!(
            "a schedule has exactly one trigger, and this call set {}. Make one call per trigger, e.g. {SCHEDULE_EXAMPLE} and `{{action: \"create\", at: \"30m\", prompt: \"...\"}}`",
            set.join(" and ")
        ));
    }
    if persistent
        && !matches!(
            watch_opt.map(parse_watch_spec),
            Some(Ok(WatchTarget::Webhook { .. }))
        )
    {
        return Err("`persistent` is only supported for webhook/event watches".into());
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
        let (resolved_spec, baseline) = match parsed {
            WatchTarget::PathChanged { path } => file::prepare(store, &agent.id, &path, now)?,
            _ => (
                watch_str.to_string(),
                initial_watch_baseline(store, health, &parsed, now),
            ),
        };
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

    let created = if persistent {
        store.create_persistent_webhook_watch(input)
    } else {
        store.create_schedule_jittered(input)
    }
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
    now: i64,
) -> Option<String> {
    match target {
        WatchTarget::Webhook { .. } => Some("listening".to_string()),
        WatchTarget::GitHubPr { .. } | WatchTarget::GitHubCheck { .. } => {
            Some("pending".to_string())
        }
        WatchTarget::AgentIdle { target } => {
            if let Ok(found) = store.agent_by_name(target) {
                Some(initial_agent_idle_baseline(store, health, &found, now))
            } else {
                None
            }
        }
        WatchTarget::ProcessExit { pid } => Some(
            if probe_process_alive(*pid) {
                "running"
            } else {
                "exited"
            }
            .to_string(),
        ),
        WatchTarget::PathChanged { .. } => unreachable!("file watches prepare their own baseline"),
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
    if agent_effective_quota(store, health, agent, now).is_some() {
        return Some("quota_blocked");
    }
    if is_agent_busy(store, health, agent, now) {
        return Some("busy");
    }
    None
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct IdleBaseline {
    await_turn: bool,
    turn_seq: i64,
    delivered_seq: i64,
    error_at_ms: i64,
}

impl IdleBaseline {
    fn encode(self) -> String {
        format!(
            "causal:{}:{}:{}:{}",
            u8::from(self.await_turn),
            self.turn_seq,
            self.delivered_seq,
            self.error_at_ms
        )
    }

    fn parse(raw: Option<&str>) -> Option<Self> {
        let rest = raw?.strip_prefix("causal:")?;
        let mut parts = rest.split(':');
        let await_turn = match parts.next()? {
            "1" => true,
            "0" => false,
            _ => return None,
        };
        let turn_seq = parts.next()?.parse().ok()?;
        let delivered_seq = parts.next()?.parse().ok()?;
        let error_at_ms = parts.next()?.parse().ok()?;
        if parts.next().is_some() {
            return None;
        }
        Some(Self {
            await_turn,
            turn_seq,
            delivered_seq,
            error_at_ms,
        })
    }
}

fn read_marker_ms(cfg: &Path, name: &str) -> i64 {
    std::fs::read_to_string(cfg.join(name))
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0)
}

type PersistedFault = (Option<i64>, Option<(String, i64)>);

fn persisted_agent_fault(cfg: &Path, harness: &str, now: i64) -> PersistedFault {
    let Ok(raw) = std::fs::read_to_string(cfg.join(".anyagents-last-error.json")) else {
        return (None, None);
    };
    let Ok(record) = serde_json::from_str::<serde_json::Value>(&raw) else {
        return (None, None);
    };
    let rotated = read_marker_ms(cfg, ".anyagents-profile-rotated-ms");
    let floor = if harness == "agy" {
        rotated.max(read_marker_ms(cfg, ".anyagents-quota-cleared-ms"))
    } else {
        rotated
    };
    let recorded_at = record
        .get("timestamp_ms")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    if recorded_at <= floor {
        return (None, None);
    }
    let error = record
        .get("error")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .filter(|s| !s.trim().is_empty())
        .map(|s| (s, recorded_at));
    if harness == "claude-code" {
        return (None, error);
    }
    match anyagents_harness::quota::recorded_reset_at_ms(&record) {
        Some(reset_ms) if reset_ms > now => (Some(reset_ms), error),
        Some(_) => (None, None),
        None => (None, error),
    }
}

fn agent_effective_quota(
    store: &Store,
    health: &HealthBoard,
    agent: &Agent,
    now: i64,
) -> Option<i64> {
    let cfg = anyagents_core::agent_config_dir(store.data_dir(), &agent.id);
    let session_quota = if agent.harness == "claude-code" {
        anyagents_harness_claude::detect_session_quota_reset_ms(&cfg, now)
    } else {
        anyagents_harness_agy::detect_agy_session_quota_and_error(&cfg, now).0
    };
    let (persisted_quota, _) = persisted_agent_fault(&cfg, &agent.harness, now);
    health
        .get(&agent.id)
        .and_then(|live| live.quota_reset_at_ms)
        .or(session_quota)
        .or(persisted_quota)
        .filter(|&ms| ms > now)
}

fn agent_lock_busy(store: &Store, health: &HealthBoard, agent: &Agent) -> bool {
    health.get(&agent.id).is_none_or(|live| !live.process_alive)
        && store.try_lock_agent(&agent.id).is_err()
}

/// Returns `Some((error, error_at_ms))` when `agent` is settled in an errored or
/// faulted turn state (never while actively `InTurn` or waiting on a quota hold).
fn agent_errored_state(
    store: &Store,
    health: &HealthBoard,
    agent: &Agent,
    now: i64,
) -> Option<(String, i64)> {
    if in_live_turn(health, &agent.id) || agent_lock_busy(store, health, agent) {
        return None;
    }
    if agent_effective_quota(store, health, agent, now).is_some() {
        return None;
    }
    let raw_turn_errors = store.turn_errors(&agent.id).ok().flatten();
    let err_at = raw_turn_errors
        .as_ref()
        .and_then(|e| e.last_error_at_ms.or(e.faulted_at_ms))
        .unwrap_or(0);
    let turn_errors = raw_turn_errors.filter(anyagents_core::TurnErrors::is_current);
    if agent.state == "paused"
        && turn_errors
            .as_ref()
            .is_some_and(|e| e.faulted_at_ms.is_some())
    {
        let text = turn_errors
            .and_then(|e| e.last_error)
            .unwrap_or_else(|| "faulted".to_string());
        return Some((text, err_at));
    }
    if agent.state != "running" && agent.state != "idle" {
        return None;
    }
    let cfg = anyagents_core::agent_config_dir(store.data_dir(), &agent.id);
    let (_, persisted) = persisted_agent_fault(&cfg, &agent.harness, now);
    let live_entry = health.get(&agent.id);
    let live_err = live_entry.as_ref().and_then(|live| live.last_error.clone());
    if let Some(err) = live_err
        && !err.trim().is_empty()
        && !crate::host_memory::is_migration_wait(&err)
    {
        return Some((err, err_at));
    }
    if let Some((err, recorded_at)) = persisted
        && !crate::host_memory::is_migration_wait(&err)
    {
        return Some((err, err_at.max(recorded_at)));
    }
    if live_entry.is_none()
        && let Some(te) = turn_errors
        && te.streak > 0
        && let Some(err) = te
            .last_error
            .filter(|s| !s.trim().is_empty() && !crate::host_memory::is_migration_wait(s))
    {
        return Some((err, err_at));
    }
    None
}

fn initial_agent_idle_baseline(
    store: &Store,
    health: &HealthBoard,
    found: &Agent,
    now: i64,
) -> String {
    let Ok(snap) = store.agent_turn_snapshot(&found.id) else {
        return "working".to_string();
    };
    let errored = agent_errored_state(store, health, found, now);
    let error_at_ms = errored.as_ref().map_or(0, |(_, at)| (*at).max(1));
    // Queued mail, live turns, or non-errored open turns must complete before firing.
    let await_turn = snap.pending_mail
        || in_live_turn(health, &found.id)
        || (snap.open_turn && errored.is_none());
    IdleBaseline {
        await_turn,
        turn_seq: snap.turn_seq,
        delivered_seq: snap.delivered_seq,
        error_at_ms,
    }
    .encode()
}

pub fn is_agent_busy(store: &Store, health: &HealthBoard, agent: &Agent, _now: i64) -> bool {
    // A published live turn proves busy without touching the store.
    if in_live_turn(health, &agent.id) {
        return true;
    }
    // Check turn lock ownership even before health has been published.
    if agent_lock_busy(store, health, agent) {
        return true;
    }
    match store.agent_turn_snapshot(&agent.id) {
        Ok(snap) => snap.open_turn || snap.pending_mail,
        Err(err) => {
            eprintln!("Schedule turn state unavailable: {err}");
            true
        }
    }
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
/// Spacing before a due tick or triggered watch whose wake was not written (a
/// held lifecycle lock, a store error) is tried again.
const RETRY_MS: i64 = 1_000;
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

/// Wake this process's schedule watchers for `data_dir`: an active schedule was
/// created, changed or cancelled.
pub fn schedules_changed(data_dir: &Path) {
    let feeds = FEEDS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(feed) = feeds.get(data_dir) {
        feed.send_modify(|generation| *generation = generation.wrapping_add(1));
    }
}

/// Start the server's schedule watch + turn-ownership loop (local timers are
/// owned by [`crate::timer::LocalTokioTimerBackend`]).
pub(crate) fn start_watch_loops(
    store: Arc<Store>,
    health: HealthBoard,
    stopping: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    let feed = change_feed(store.data_dir());
    spawn_local_timer(store, health, feed.subscribe(), stopping, true)
}

#[cfg(test)]
pub fn start_loops(
    store: Arc<Store>,
    health: HealthBoard,
    stopping: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    let feed = change_feed(store.data_dir());
    spawn_local_timer(store, health, feed.subscribe(), stopping, false)
}

fn spawn_local_timer(
    store: Arc<Store>,
    health: HealthBoard,
    mut changes: tokio::sync::watch::Receiver<u64>,
    mut stopping: tokio::sync::watch::Receiver<bool>,
    watches_only: bool,
) -> tokio::task::JoinHandle<()> {
    let timer = Timer::local_event_driven(watches_only);
    let stop = Arc::clone(&timer.stop);
    let timer = Arc::new(Mutex::new(timer));
    let mut store_changes = store.subscribe_changes();
    let mut health_changes = health.subscribe();
    tokio::spawn(async move {
        let mut tracked_local: Vec<WatchTarget> = Vec::new();
        let mut local_events = local::LocalEvents::new(std::iter::empty());
        loop {
            let generation = *changes.borrow_and_update();
            let _ = store_changes.borrow_and_update();
            let _ = health_changes.borrow_and_update();
            let (st, tm) = (Arc::clone(&store), Arc::clone(&timer));
            let prep = tokio::task::spawn_blocking(move || {
                let mut timer = tm.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                timer.prepare_local_watches(&st, generation, anyagents_core::now_ms())
            });
            let prepared = tokio::select! {
                biased;
                _ = stopping.wait_for(|stop| *stop) => break,
                res = prep => res.ok().flatten(),
            };
            if let Some(targets) = prepared
                && targets != tracked_local
            {
                local_events = local::LocalEvents::new(targets.clone());
                tracked_local = targets;
                let fallback = local_events.fallback().clone();
                let mut t = timer
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                t.set_local_fallback(fallback, anyagents_core::now_ms());
            }
            let (st, hb, tm) = (Arc::clone(&store), health.clone(), Arc::clone(&timer));
            let step = tokio::task::spawn_blocking(move || {
                let mut timer = tm.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                let out = timer.step(&st, &hb, generation, anyagents_core::now_ms());
                (
                    out.deadline,
                    timer.plan.local_kernel_targets(),
                    timer.has_agent_idle_watches(),
                    timer.has_guard_watches(),
                )
            });
            let (mut deadline, after_targets, watching_agents, watching_guard) = tokio::select! {
                biased;
                _ = stopping.wait_for(|stop| *stop) => break,
                step = step => step.unwrap_or_else(|err| {
                    eprintln!("Schedule pass failed: {err}");
                    (
                        anyagents_core::now_ms().saturating_add(RETRY_MS),
                        Vec::new(),
                        false,
                        false,
                    )
                }),
            };
            if after_targets != tracked_local {
                local_events = local::LocalEvents::new(after_targets.clone());
                tracked_local = after_targets;
                let fallback = local_events.fallback().clone();
                let now = anyagents_core::now_ms();
                let mut t = timer
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                t.set_local_fallback(fallback, now);
                deadline = t.recompute_deadline(now);
            }
            let sleep_wait = async {
                if deadline == i64::MAX {
                    std::future::pending::<()>().await;
                } else {
                    let wait = deadline.saturating_sub(anyagents_core::now_ms()).max(0) as u64;
                    tokio::time::sleep(Duration::from_millis(wait)).await;
                }
            };
            tokio::select! {
                biased;
                _ = stopping.wait_for(|stop| *stop) => break,
                () = sleep_wait => {}
                Ok(()) = changes.changed() => {}
                ready = local_events.changed() => {
                    let fallback = local_events.fallback().clone();
                    let mut t = timer.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                    t.mark_local_targets_due(&ready, &fallback, anyagents_core::now_ms());
                }
                Ok(()) = store_changes.changed(), if watching_agents => {
                    let mut t = timer.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                    t.mark_agent_idle_due();
                }
                Ok(()) = health_changes.changed(), if watching_agents || watching_guard => {
                    let mut t = timer.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                    t.mark_health_changed();
                }
            }
        }
        stop.store(true, Ordering::SeqCst);
    })
}

/// The server's schedule timer. A step does work only when an entry is due,
/// the guard is due, or an event/schedule feed woke it.
struct Timer {
    plan: Plan,
    /// The timer also runs the turn-ownership guard.
    guard: Option<TurnGuard>,
    deadline: i64,
    /// Set when the server stops: a step in progress evaluates no further entry.
    stop: Arc<AtomicBool>,
}

impl Timer {
    fn local() -> Self {
        Self {
            plan: Plan::new(false),
            guard: Some(TurnGuard::default()),
            deadline: i64::MIN,
            stop: Arc::default(),
        }
    }

    fn local_event_driven(watches_only: bool) -> Self {
        Self {
            plan: Plan::new_with_mode(true, watches_only),
            guard: Some(TurnGuard::default()),
            deadline: i64::MIN,
            stop: Arc::default(),
        }
    }

    /// Reload a stale local plan before arming kernel `inotify`/`pidfd` watches
    /// so registrations precede the initial predicate evaluation.
    fn prepare_local_watches(
        &mut self,
        store: &Store,
        generation: u64,
        now: i64,
    ) -> Option<Vec<WatchTarget>> {
        if !self.plan.stale(generation, now) {
            return None;
        }
        self.plan.reload(store, generation, now);
        self.deadline = i64::MIN;
        Some(self.plan.local_kernel_targets())
    }

    fn set_local_fallback(&mut self, fallback: HashSet<WatchTarget>, now: i64) {
        for entry in &mut self.plan.entries {
            if let Some(target) = entry.local_kernel_target()
                && fallback.contains(target)
            {
                entry.check_at = entry.check_at.min(now.saturating_add(LOCAL_WATCH_PROBE_MS));
            }
        }
        self.plan.local_fallback = fallback;
    }

    fn mark_local_targets_due(
        &mut self,
        ready: &HashSet<WatchTarget>,
        fallback: &HashSet<WatchTarget>,
        now: i64,
    ) {
        for entry in &mut self.plan.entries {
            let Some(target) = entry.local_kernel_target() else {
                continue;
            };
            if ready.contains(target) {
                entry.check_at = i64::MIN;
            } else if fallback.contains(target) {
                entry.check_at = entry.check_at.min(now.saturating_add(LOCAL_WATCH_PROBE_MS));
            }
        }
        self.plan.local_fallback = fallback.clone();
        self.deadline = i64::MIN;
    }

    fn mark_agent_idle_due(&mut self) {
        let mut any = false;
        for entry in &mut self.plan.entries {
            if matches!(entry.watch, Some(WatchTarget::AgentIdle { .. })) {
                entry.check_at = i64::MIN;
                any = true;
            }
        }
        if any {
            self.deadline = i64::MIN;
        }
    }

    fn mark_health_changed(&mut self) {
        self.mark_agent_idle_due();
        if self.has_guard_watches() {
            self.deadline = i64::MIN;
        }
    }

    fn has_agent_idle_watches(&self) -> bool {
        self.plan
            .entries
            .iter()
            .any(|e| matches!(e.watch, Some(WatchTarget::AgentIdle { .. })))
    }

    fn has_guard_watches(&self) -> bool {
        self.guard
            .as_ref()
            .is_some_and(TurnGuard::has_watched_agents)
    }

    fn recompute_deadline(&mut self, now: i64) -> i64 {
        let mut deadline = self.plan.next_deadline();
        if let Some(guard) = &self.guard {
            deadline = deadline.min(guard.next_deadline(now, self.plan.event_driven));
        }
        self.deadline = if deadline <= now {
            now.saturating_add(RETRY_MS)
        } else {
            deadline
        };
        self.deadline
    }

    /// Run whatever is due at `now`.
    fn step(&mut self, store: &Store, health: &HealthBoard, generation: u64, now: i64) -> Step {
        let mut step = Step {
            worked: false,
            fired: 0,
            fired_agents: Vec::new(),
            deadline: self.deadline,
        };
        if now < self.deadline && generation == self.plan.generation {
            return step;
        }
        if self.plan.stale(generation, now) {
            step.worked = true;
            self.plan.reload(store, generation, now);
        }
        if self.plan.due(now) {
            step.worked = true;
            let pass = self.plan.pass(store, health, now, &self.stop);
            step.fired = pass.fired;
            step.fired_agents = pass.fired_agents;
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
        step.deadline = self.recompute_deadline(now);
        step
    }
}

/// What one timer step did.
struct Step {
    /// The step read or wrote the store.
    worked: bool,
    /// Wakes written.
    fired: usize,
    /// Distinct agents woken by schedules that fired in this step.
    fired_agents: Vec<AgentId>,
    /// When the next step is due.
    deadline: i64,
}

/// Evaluate a single `watch` schedule without scanning unrelated rows.
/// Returns `(fired, next_probe_at_ms)`.
pub(crate) fn evaluate_single_watch_schedule(
    store: &Store,
    health: &HealthBoard,
    sched: &AgentSchedule,
    now: i64,
) -> (bool, Option<i64>) {
    let mut entry = Entry {
        sched: sched.clone(),
        watch: parse_watch_spec(&sched.spec).ok(),
        check_at: i64::MAX,
        idle_target: None,
        local_file: false,
    };
    let out = evaluate_watch_entry(store, health, &mut entry, false, &HashSet::new(), now);
    (
        out.fired,
        (entry.check_at < i64::MAX).then_some(entry.check_at),
    )
}

/// Evaluate due schedules during an operator-initiated maintenance pass.
pub(crate) fn evaluate_maintenance_schedules(
    store: &Store,
    health: &HealthBoard,
    now: i64,
) -> (usize, Vec<AgentId>) {
    let step = Timer::local().step(store, health, 0, now);
    (step.fired, step.fired_agents)
}

/// Evaluate local timers and watches (and the turn-ownership rules) without
/// issuing any external probe: one full census of the store, which is a fresh
/// local timer's first step. [`start_loops`] keeps that timer and steps it again
/// only when something is due; tests drive single passes through this.
#[cfg(test)]
fn tick_local_schedules_once(store: &Store, health: &HealthBoard, now: i64) -> usize {
    Timer::local().step(store, health, 0, now).fired
}

#[cfg(test)]
fn tick_schedules_once(store: &Store, health: &HealthBoard, now: i64) -> usize {
    tick_local_schedules_once(store, health, now)
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
                "[Sub-Agent Turn Ownership Guard] The following `lifecycle: task` sub-agent(s) reported to you and are idle with no active schedules (`waiting_parent`): {formatted}. The ball is in your court: for each sub-agent, review its report and either send a follow-up instruction via `send` if work remains, or close it via `agents(action: \"archive\", name: \"<name>\")`."
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

    fn has_watched_agents(&self) -> bool {
        self.families.iter().any(|f| !f.watched.is_empty())
    }

    fn next_deadline(&self, now: i64, event_driven: bool) -> i64 {
        self.families.iter().fold(self.refresh_at, |at, family| {
            let at = at.min(family.next_at);
            if family.watched.is_empty() || event_driven {
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
    local_file: bool,
}

impl Entry {
    fn local_kernel_target(&self) -> Option<&WatchTarget> {
        match self.watch.as_ref()? {
            target @ WatchTarget::PathChanged { .. } if self.local_file => Some(target),
            target @ WatchTarget::ProcessExit { .. } => Some(target),
            _ => None,
        }
    }
}

/// The active schedules managed by the schedule timer loop.
struct Plan {
    event_driven: bool,
    watches_only: bool,
    local_fallback: HashSet<WatchTarget>,
    entries: Vec<Entry>,
    /// Change-feed generation the entries were read at.
    generation: u64,
    reload_at: i64,
}

#[derive(Default)]
struct PassOutcome {
    fired: usize,
    fired_agents: Vec<AgentId>,
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
    fn new(event_driven: bool) -> Self {
        Self::new_with_mode(event_driven, false)
    }

    fn new_with_mode(event_driven: bool, watches_only: bool) -> Self {
        Self {
            event_driven,
            watches_only,
            local_fallback: HashSet::new(),
            entries: Vec::new(),
            generation: 0,
            reload_at: i64::MIN,
        }
    }

    fn local_kernel_targets(&self) -> Vec<WatchTarget> {
        let mut out = Vec::new();
        for entry in &self.entries {
            if let Some(target) = entry.local_kernel_target()
                && !out.contains(target)
            {
                out.push(target.clone());
            }
        }
        out
    }

    fn stale(&self, generation: u64, now: i64) -> bool {
        generation != self.generation || now >= self.reload_at
    }

    fn reload(&mut self, store: &Store, generation: u64, now: i64) {
        match self.read(store) {
            Ok(entries) => {
                self.entries = entries;
                self.generation = generation;
                self.reload_at = i64::MAX;
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
        let mut live: Option<HashMap<AgentId, anyagents_core::ExecutionRuntime>> = None;
        let mut entries = Vec::new();
        for sched in schedules {
            if self.watches_only && sched.kind != "watch" {
                continue;
            }
            let watch = if sched.kind == "watch" {
                parse_watch_spec(&sched.spec).ok()
            } else {
                None
            };
            if sched.kind == "watch" {
                if live.is_none() {
                    live = Some(
                        store
                            .list_live_agents()?
                            .into_iter()
                            .map(|agent| (agent.id, agent.runtime))
                            .collect(),
                    );
                }
                if live
                    .as_ref()
                    .is_some_and(|live| !live.contains_key(&sched.agent_id))
                {
                    if let Err(err) = store.cancel_schedule(None, &sched.id) {
                        eprintln!("Schedule cancellation failed: {err}");
                    }
                    continue;
                }
            }
            let local_file = matches!(watch, Some(WatchTarget::PathChanged { .. }))
                && live.as_ref().and_then(|live| live.get(&sched.agent_id))
                    == Some(&anyagents_core::ExecutionRuntime::Local)
                && !file::sandbox_baseline(&sched);
            let prior = previous.get(sched.id.as_str()).copied();
            let check_at = if sched.kind == "watch" {
                match watch.as_ref() {
                    // Webhook watches are push-triggered via `fire_webhook_event`.
                    Some(WatchTarget::Webhook { .. }) => i64::MAX,
                    _ => prior
                        .filter(|p| p.local_file == local_file)
                        .map_or(i64::MIN, |p| p.check_at),
                }
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
                local_file,
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
    fn pass(
        &mut self,
        store: &Store,
        health: &HealthBoard,
        now: i64,
        stop: &AtomicBool,
    ) -> PassOutcome {
        let order: Vec<usize> = (0..self.entries.len())
            .filter(|&i| self.entries[i].check_at <= now)
            .collect();
        let mut outcome = PassOutcome::default();
        for i in order {
            if stop.load(Ordering::SeqCst) {
                break;
            }
            let step = evaluate_entry(
                store,
                health,
                &mut self.entries[i],
                self.event_driven,
                &self.local_fallback,
                now,
            );
            if step.fired {
                outcome.fired += 1;
                let aid = self.entries[i].sched.agent_id.clone();
                if !outcome.fired_agents.contains(&aid) {
                    outcome.fired_agents.push(aid);
                }
            }
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
    event_driven: bool,
    local_fallback: &HashSet<WatchTarget>,
    now: i64,
) -> EntryOutcome {
    match entry.sched.kind.as_str() {
        "at" | "every" => evaluate_timer(store, entry, now),
        "watch" => evaluate_watch_entry(store, health, entry, event_driven, local_fallback, now),
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
    entry.check_at = now.saturating_add(RETRY_MS);
    match crate::timer::fire_schedule_by_id(
        store,
        &HealthBoard::default(),
        &crate::timer::NoopTimerBackend,
        &sched.id,
        Some(due),
        now,
    ) {
        Ok(res) if res.reason.as_deref() == Some("lifecycle_busy") => EntryOutcome::default(),
        Ok(res) => EntryOutcome {
            fired: res.fired,
            wrote: true,
        },
        Err(err) => {
            eprintln!("Schedule {} fire failed: {err}", sched.id);
            EntryOutcome::default()
        }
    }
}

/// When a watch is probed next after a look at `now`.
fn watch_probe_at(
    watch: Option<&WatchTarget>,
    event_driven: bool,
    local_fallback: &HashSet<WatchTarget>,
    now: i64,
) -> i64 {
    match watch {
        // A spec never changes, and webhook watches are push-triggered only.
        None | Some(WatchTarget::Webhook { .. }) => i64::MAX,
        Some(target @ (WatchTarget::GitHubPr { .. } | WatchTarget::GitHubCheck { .. })) => {
            github::next_probe_at(target, now).max(now.saturating_add(RETRY_MS))
        }
        Some(WatchTarget::AgentIdle { .. }) => {
            if event_driven {
                i64::MAX
            } else {
                now.saturating_add(LOCAL_WATCH_PROBE_MS)
            }
        }
        Some(target @ (WatchTarget::PathChanged { .. } | WatchTarget::ProcessExit { .. })) => {
            if event_driven && !local_fallback.contains(target) {
                i64::MAX
            } else {
                now.saturating_add(LOCAL_WATCH_PROBE_MS)
            }
        }
    }
}

fn evaluate_watch_entry(
    store: &Store,
    health: &HealthBoard,
    entry: &mut Entry,
    event_driven: bool,
    local_fallback: &HashSet<WatchTarget>,
    now: i64,
) -> EntryOutcome {
    let (eval, next_probe) = if let Some(WatchTarget::PathChanged { path }) = &entry.watch {
        let (eval, local) = file::evaluate(store, &entry.sched, path, now);
        let next = if local {
            watch_probe_at(entry.watch.as_ref(), event_driven, local_fallback, now)
        } else {
            now.saturating_add(file::PROBE_INTERVAL_MS)
        };
        (eval, next)
    } else {
        (
            evaluate_watch(
                store,
                health,
                &entry.sched,
                entry.watch.as_ref(),
                &mut entry.idle_target,
                now,
            ),
            watch_probe_at(entry.watch.as_ref(), event_driven, local_fallback, now),
        )
    };
    entry.check_at = next_probe;
    let recheck_at = now.saturating_add(LOCAL_WATCH_PROBE_MS);
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
    if previous_pending {
        // Record the latest observation without consuming the transition or
        // moving the baseline; it is written once the pending wake is taken.
        entry.check_at = entry.check_at.min(recheck_at);
        if eval.observed.is_some() && eval.observed != sched.last_observed {
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

#[cfg(test)]
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
        WatchTarget::Webhook { .. } => {
            WatchEval::pending(sched.watch_baseline.clone(), sched.last_observed.clone())
        }
        WatchTarget::GitHubPr { .. } | WatchTarget::GitHubCheck { .. } => {
            github::evaluate_github_watch(store, sched, target, now)
        }
        WatchTarget::AgentIdle { target } => {
            evaluate_agent_idle_watch(store, health, sched, target, idle_target, now)
        }
        WatchTarget::ProcessExit { pid } => {
            if probe_process_alive(*pid) {
                WatchEval::pending(Some("running".to_string()), Some("running".to_string()))
            } else {
                WatchEval::fired(sched.watch_baseline.clone(), Some("exited".to_string()))
            }
        }
        WatchTarget::PathChanged { .. } => unreachable!("file watches use a runtime-aware probe"),
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

fn evaluate_agent_idle_watch(
    store: &Store,
    health: &HealthBoard,
    sched: &AgentSchedule,
    target: &str,
    idle_target: &mut Option<AgentId>,
    now: i64,
) -> WatchEval {
    // The watched agent's published live turn answers without the store.
    if idle_target
        .as_ref()
        .is_some_and(|id| in_live_turn(health, id))
    {
        let baseline = match sched.watch_baseline.as_deref() {
            Some(raw) if raw.starts_with("causal:") => Some(raw.to_string()),
            _ => Some("working".to_string()),
        };
        return WatchEval::pending(baseline, Some("working".to_string()));
    }
    let Ok(found) = store.agent_by_name(target) else {
        return WatchEval::pending(sched.watch_baseline.clone(), Some("not_found".to_string()));
    };
    *idle_target = Some(found.id.clone());
    let base = IdleBaseline::parse(sched.watch_baseline.as_deref());
    if in_live_turn(health, &found.id) || agent_lock_busy(store, health, &found) {
        let next_base = if let Some(mut b) = base {
            if !b.await_turn
                && let Ok(snap) = store.agent_turn_snapshot(&found.id)
            {
                b.await_turn = true;
                b.turn_seq = snap.turn_seq;
                b.delivered_seq = snap.delivered_seq;
            }
            b.encode()
        } else {
            "working".to_string()
        };
        return WatchEval::pending(Some(next_base), Some("working".to_string()));
    }
    let Ok(snap) = store.agent_turn_snapshot(&found.id) else {
        return WatchEval::pending(sched.watch_baseline.clone(), Some("working".to_string()));
    };
    let errored = agent_errored_state(store, health, &found, now);
    let terminal_errored = base.is_some_and(|b| {
        snap.turn_seq > b.turn_seq
            && snap.outcome.as_deref() == Some("errored")
            && agent_effective_quota(store, health, &found, now).is_none()
    });
    let fresh_live_error = errored.as_ref().is_some_and(|(_, err_at)| match base {
        None => true,
        Some(b) if !b.await_turn => true,
        Some(b) => b.error_at_ms == 0 || *err_at > b.error_at_ms || snap.turn_seq > b.turn_seq,
    });
    if terminal_errored || fresh_live_error {
        return WatchEval {
            triggered: true,
            next_state: "fired",
            transition: Some("errored".to_string()),
            observed: Some("errored".to_string()),
            baseline: sched.watch_baseline.clone(),
        };
    }
    let busy = snap.open_turn || snap.pending_mail;
    if let Some(mut b) = base {
        if !b.await_turn && busy {
            b.await_turn = true;
            b.turn_seq = snap.turn_seq;
            b.delivered_seq = snap.delivered_seq;
            return WatchEval::pending(Some(b.encode()), Some("working".to_string()));
        }
        if busy || (b.await_turn && snap.turn_seq <= b.turn_seq) {
            return WatchEval::pending(Some(b.encode()), Some("working".to_string()));
        }
        return WatchEval {
            triggered: true,
            next_state: "fired",
            transition: Some("idle".to_string()),
            observed: Some("idle".to_string()),
            baseline: Some(b.encode()),
        };
    }
    if busy {
        WatchEval::pending(Some("working".to_string()), Some("working".to_string()))
    } else {
        WatchEval {
            triggered: true,
            next_state: "fired",
            transition: Some("idle".to_string()),
            observed: Some("idle".to_string()),
            baseline: sched.watch_baseline.clone(),
        }
    }
}

mod file;
mod github;
mod local;
mod webhook;
pub use webhook::fire_webhook_event;
pub(crate) use webhook::fresh_delivery_id;

#[cfg(test)]
pub(crate) use github::{RestoreEnv, reset_probes_for_test, spawn_mock_github};

#[cfg(test)]
mod tests {
    use super::*;

    fn next_recurring_slot(due: i64, interval: i64, now: i64) -> Option<i64> {
        Store::next_recurring_fire_ms("test", "test", due, due, interval, now)
    }
    use anyagents_core::NewAgent;
    use anyagents_harness::TurnState;
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
                delivery: None,
                quota_reset_at_ms,
                restart_at_ms: None,
                pid: None,
                live_harness: None,
                live_model: None,
                process_started_at_ms: Some(1000),
                last_error: quota_reset_at_ms.map(|_| "quota exceeded".to_string()),
                retry_at_ms: None,
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
            parse_watch_spec("webhook:deploy/prod").unwrap(),
            WatchTarget::Webhook {
                topic: "deploy/prod".to_string(),
            }
        );
        assert_eq!(
            parse_watch_spec("event:build.done").unwrap(),
            WatchTarget::Webhook {
                topic: "build.done".to_string(),
            }
        );
        assert_eq!(
            parse_watch_spec("webhook:*").unwrap(),
            WatchTarget::Webhook {
                topic: "*".to_string(),
            }
        );
        assert_eq!(
            parse_watch_spec("github:pr:AnotherOrg/anyagents#41").unwrap(),
            WatchTarget::GitHubPr {
                owner: "AnotherOrg".to_string(),
                repo: "anyagents".to_string(),
                number: 41,
            }
        );
        assert_eq!(
            parse_watch_spec("github:check:AnotherOrg/anyagents@deadbeef").unwrap(),
            WatchTarget::GitHubCheck {
                owner: "AnotherOrg".to_string(),
                repo: "anyagents".to_string(),
                reference: "deadbeef".to_string(),
                check_name: None,
            }
        );
        assert!(parse_watch_spec("spiron:promise:prm_12345").is_err());
        assert!(parse_watch_spec("webhook:").is_err());
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
    fn due_at_wakes_a_root_agent_scaled_to_zero() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let owner = store
            .create_agent(NewAgent::new("idle-root", "codex"))
            .unwrap();
        assert!(owner.parent_id.is_none());
        store.set_agent_running(&owner.id, "/work").unwrap();
        store.set_agent_state(&owner.id, "idle").unwrap();
        let now = anyagents_core::now_ms();
        store
            .create_schedule(NewSchedule {
                agent_id: owner.id.clone(),
                kind: "at".into(),
                spec: "in 1m".into(),
                prompt: "scheduled wake".into(),
                interval_ms: None,
                next_fire_at_ms: Some(now + 60_000),
                watch_baseline: None,
            })
            .unwrap();
        let health = HealthBoard::default();
        let wakes = store.subscribe();
        let mut timer = Timer::local();
        // Not yet due: the idle root stays released and is not woken.
        assert_eq!(timer.step(&store, &health, 0, now).fired, 0);
        assert_eq!(store.agent(&owner.id).unwrap().state, "idle");
        // Due: the wake is in its mailbox, and the row change the supervisor
        // starts a worker on is published with it.
        assert_eq!(timer.step(&store, &health, 0, now + 60_000).fired, 1);
        assert_eq!(store.agent(&owner.id).unwrap().state, "running");
        let inbox = store.inbox(&owner.id, None).unwrap();
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].text, "scheduled wake");
        let woke: Vec<_> = wakes.try_iter().collect();
        assert!(
            woke.iter().any(|wake| matches!(wake, anyagents_core::Wake::Agent { agent_id } if agent_id == &owner.id)),
            "{woke:?}"
        );
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
        for harness in [
            "agy",
            "claude-code",
            "codex",
            "opencode",
            "spiron",
            "muse",
            "omp",
        ] {
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
                delivery: None,
                quota_reset_at_ms: None,
                restart_at_ms: None,
                pid: None,
                live_harness: Some(harness.to_string()),
                live_model: None,
                process_started_at_ms: Some(1000),
                last_error: None,
                retry_at_ms: None,
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
    fn schedule_owner_is_busy_while_turn_lock_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let mut owner = store.create_agent(NewAgent::new("owner", "codex")).unwrap();
        let now = anyagents_core::now_ms();
        for harness in ["agy", "claude-code", "codex", "opencode", "spiron"] {
            owner.harness = harness.into();
            assert!(!is_agent_busy(&store, &health, &owner, now), "{harness}");
            let lock = store.try_lock_agent(&owner.id).unwrap();
            assert!(is_agent_busy(&store, &health, &owner, now), "{harness}");
            drop(lock);
            assert!(!is_agent_busy(&store, &health, &owner, now), "{harness}");
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
        assert_eq!(
            inbox[0].text,
            "[watch: agent:idle:worker-sub transition=idle]\nsubagent finished"
        );
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
            delivery: None,
            quota_reset_at_ms: Some(now - 1_000),
            restart_at_ms: None,
            pid: None,
            live_harness: None,
            live_model: None,
            process_started_at_ms: Some(1000),
            last_error: Some("HTTP 429: rate limit, quota exceeded".into()),
            retry_at_ms: None,
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
    fn recurring_slot_is_strictly_future_and_never_saturates() {
        assert_eq!(next_recurring_slot(10, 3, 10), Some(13));
        assert_eq!(next_recurring_slot(10, 3, 15), Some(16));
        assert_eq!(next_recurring_slot(10, 3, 16), Some(19));
        assert_eq!(
            next_recurring_slot(i64::MIN, 3, i64::MAX - 1),
            Some(i64::MAX)
        );
        assert_eq!(next_recurring_slot(i64::MAX - 1, 2, i64::MAX - 1), None);
        assert_eq!(next_recurring_slot(i64::MAX, 1, i64::MAX), None);
    }

    #[test]
    fn recurring_timer_reopens_after_downtime_and_coalesces_an_unread_wake() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let store = Store::open(&path).unwrap();
        let owner = store
            .create_agent(NewAgent::new("timer-owner", "codex"))
            .unwrap();
        store.set_agent_running(&owner.id, "/work").unwrap();
        store.set_agent_state(&owner.id, "paused").unwrap();
        let due = 60_000;
        let interval = 3_600_000; // timer_schedule's every interval.
        timer_schedule(&store, &owner.id, "every", "1h", Some(due));
        drop(store);

        let resumed = due + interval * 10_000_000_000 + 123;
        let store = Store::open(&path).unwrap();
        let health = HealthBoard::default();
        assert_eq!(tick_schedules_once(&store, &health, resumed), 1);
        assert_eq!(tick_schedules_once(&store, &health, resumed), 0);
        let inbox = store.inbox(&owner.id, None).unwrap();
        assert_eq!(inbox.len(), 1);
        let wake = inbox[0].seq;
        let next = resumed - 123 + interval;
        assert_eq!(
            store.list_schedules(Some(&owner.id), true).unwrap()[0].next_fire_at_ms,
            Some(next)
        );
        drop(store);

        // On another restart, a persisted unread wake stands for all missed slots.
        let store = Store::open(&path).unwrap();
        let health = HealthBoard::default();
        let resumed_again = next + interval * 5;
        assert_eq!(tick_schedules_once(&store, &health, resumed_again), 0);
        let saved = &store.list_schedules(Some(&owner.id), true).unwrap()[0];
        assert_eq!(saved.last_observed.as_deref(), Some("coalesced"));
        assert_eq!(saved.next_fire_at_ms, Some(resumed_again + interval));
        assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 1);

        store.set_agent_running(&owner.id, "/work").unwrap();
        store.note_delivered(&owner.id, wake).unwrap();
        assert_eq!(
            tick_schedules_once(&store, &health, resumed_again + interval - 1),
            0
        );
        assert_eq!(
            tick_schedules_once(&store, &health, resumed_again + interval),
            1
        );
        assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 2);
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
            let schedule = store
                .list_schedules(Some(&agent.id), true)
                .unwrap()
                .remove(0);
            store.set_agent_state(&agent.id, "archived").unwrap();
            assert!(
                store
                    .list_schedules(Some(&agent.id), true)
                    .unwrap()
                    .is_empty()
            );
            let cfg = anyagents_core::agent_config_dir(dir.path(), &agent.id);
            std::fs::create_dir_all(&cfg).unwrap();
            std::fs::write(cfg.join(".anyagents-archived"), "archived").unwrap();
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
    fn stopped_timer_evaluates_no_further_entry() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let owner = store
            .create_agent(NewAgent::new("stopped-owner", "codex"))
            .unwrap();
        let now = anyagents_core::now_ms();
        timer_schedule(&store, &owner.id, "at", "due", Some(now - 1));
        let mut timer = Timer::local();
        timer.stop.store(true, Ordering::SeqCst);
        let stopped = timer.step(&store, &health, 0, now);
        assert_eq!(stopped.fired, 0);
        assert!(store.inbox(&owner.id, None).unwrap().is_empty());
        // The entry is still due: the next step, at the retry deadline, fires it.
        assert_eq!(stopped.deadline, now + RETRY_MS);
        timer.stop.store(false, Ordering::SeqCst);
        assert_eq!(timer.step(&store, &health, 0, stopped.deadline).fired, 1);
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
            let (_stopping, stopped) = tokio::sync::watch::channel(false);
            let local = start_loops(Arc::clone(&store), health.clone(), stopped);
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
        });
        runtime.shutdown_timeout(Duration::from_secs(5));
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
            guard.next_deadline(delivered + 5_000, false),
            delivered + CHILD_NUDGE_AFTER_MS
        );
        assert_eq!(
            guard.next_deadline(delivered + 5_000, true),
            delivered + CHILD_NUDGE_AFTER_MS
        );
        assert!(!guard.due(&health, delivered + 8_000));
        // Due, but the child is mid-turn: nothing is sent and its turn is watched.
        let due = delivered + CHILD_NUDGE_AFTER_MS;
        assert!(guard.due(&health, due));
        assert_eq!(guard.run(&store, &health, due), 0);
        assert!(!guard.due(&health, due + 200));
        assert_eq!(
            guard.next_deadline(due + 200, false),
            due + 200 + GUARD_WATCH_MS
        );
        assert_eq!(guard.next_deadline(due + 200, true), refresh);
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

    #[test]
    fn agent_idle_causal_baseline_prevents_pre_boot_race_and_wakes_on_store_feed() {
        use std::sync::Arc;
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let health = HealthBoard::default();
        let parent = store
            .create_agent(NewAgent::new("causal-parent", "agy"))
            .unwrap();
        store.set_agent_running(&parent.id, "/work").unwrap();
        let child = store
            .create_agent(NewAgent::new("causal-child", "agy"))
            .unwrap();
        store.set_agent_running(&child.id, "/work").unwrap();
        store
            .receive(&child.id, Incoming::new("build slice"))
            .unwrap();
        // Pre-boot sidecar window: live health still reports TurnState::Idle
        // before mail is consumed.
        publish_turn(&health, &child.id, TurnState::Idle, None);
        let created = create_agent_schedule(
            &store,
            &health,
            &parent,
            None,
            None,
            Some("agent:idle:causal-child"),
            "slice finished",
        )
        .unwrap();
        assert!(
            created
                .watch_baseline
                .as_deref()
                .unwrap()
                .starts_with("causal:1:0:")
        );

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
            let local = start_loops(Arc::clone(&store), health.clone(), stop_rx);
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert!(store.inbox(&parent.id, None).unwrap().is_empty());

            // Mail is admitted, opening turn_recovery while turn_seq is still 0.
            store.note_delivered(&child.id, 1).unwrap();
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(store.inbox(&parent.id, None).unwrap().is_empty());

            // Turn completion commits: Store::subscribe_changes wakes the loop
            // directly in sub-second latency without 1 s polling.
            let completed_at = std::time::Instant::now();
            assert!(store.note_turn_completed_for(&child.id, 1).unwrap());
            while store.inbox(&parent.id, None).unwrap().is_empty() {
                assert!(
                    completed_at.elapsed() < Duration::from_millis(600),
                    "Store change feed did not wake agent:idle promptly"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let inbox = store.inbox(&parent.id, None).unwrap();
            assert_eq!(inbox.len(), 1);
            assert_eq!(
                inbox[0].text,
                "[watch: agent:idle:causal-child transition=idle]\nslice finished"
            );
            local.abort();
        });
        runtime.shutdown_timeout(Duration::from_secs(5));
    }

    #[test]
    fn agent_idle_wakes_immediately_with_errored_transition_and_holds_on_quota() {
        use std::sync::Arc;
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let health = HealthBoard::default();
        let parent = store
            .create_agent(NewAgent::new("err-parent", "codex"))
            .unwrap();
        store.set_agent_running(&parent.id, "/work").unwrap();
        let child = store
            .create_agent(NewAgent::new("err-child", "spiron"))
            .unwrap();
        store.set_agent_running(&child.id, "/work").unwrap();
        store.receive(&child.id, Incoming::new("run task")).unwrap();
        store.note_delivered(&child.id, 1).unwrap();
        publish_turn(&health, &child.id, TurnState::InTurn, None);

        create_agent_schedule(
            &store,
            &health,
            &parent,
            None,
            None,
            Some("agent:idle:err-child"),
            "inspect child",
        )
        .unwrap();

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
            let local = start_loops(Arc::clone(&store), health.clone(), stop_rx);
            tokio::time::sleep(Duration::from_millis(120)).await;

            // A quota hold with rate-limit error text is not an errored turn.
            let now = anyagents_core::now_ms();
            health.publish(
                &child.id,
                LiveState {
                    process_alive: true,
                    turn_state: TurnState::Idle,
                    delivery: None,
                    quota_reset_at_ms: Some(now + 60_000),
                    restart_at_ms: None,
                    pid: None,
                    live_harness: Some("spiron".into()),
                    live_model: None,
                    process_started_at_ms: Some(1_000),
                    last_error: Some("HTTP 429: rate_limit".into()),
                    retry_at_ms: None,
                },
            );
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(store.inbox(&parent.id, None).unwrap().is_empty());

            // Transition working -> errored while open_turn remains true during backoff.
            let errored_at = std::time::Instant::now();
            assert!(
                store
                    .note_turn_errored_for(&child.id, 1, "upstream unavailable", false)
                    .unwrap()
            );
            assert!(store.has_open_turn(&child.id).unwrap());
            health.publish(
                &child.id,
                LiveState {
                    process_alive: true,
                    turn_state: TurnState::Idle,
                    delivery: None,
                    quota_reset_at_ms: None,
                    restart_at_ms: Some(now + 30_000),
                    pid: None,
                    live_harness: Some("spiron".into()),
                    live_model: None,
                    process_started_at_ms: Some(1_000),
                    last_error: Some("upstream unavailable".into()),
                    retry_at_ms: None,
                },
            );
            while store.inbox(&parent.id, None).unwrap().is_empty() {
                assert!(
                    errored_at.elapsed() < Duration::from_millis(600),
                    "agent:idle did not wake parent on errored transition"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let inbox = store.inbox(&parent.id, None).unwrap();
            assert_eq!(inbox.len(), 1);
            assert_eq!(
                inbox[0].text,
                "[watch: agent:idle:err-child transition=errored]\ninspect child"
            );
            store.note_delivered(&parent.id, inbox[0].seq).unwrap();

            // Queueing fresh mail onto an errored child records a causal baseline
            // past the old error and fires transition=idle once the new turn finishes.
            store
                .receive(&child.id, Incoming::new("retry slice"))
                .unwrap();
            create_agent_schedule(
                &store,
                &health,
                &parent,
                None,
                None,
                Some("agent:idle:err-child"),
                "retry done",
            )
            .unwrap();
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert_eq!(store.inbox(&parent.id, None).unwrap().len(), 1);

            store.note_delivered(&child.id, 2).unwrap();
            publish_turn(&health, &child.id, TurnState::Idle, None);
            assert!(store.note_turn_completed_for(&child.id, 2).unwrap());
            let resumed = std::time::Instant::now();
            while store.inbox(&parent.id, None).unwrap().len() < 2 {
                assert!(
                    resumed.elapsed() < Duration::from_millis(600),
                    "agent:idle did not wake after recovery completion"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let inbox = store.inbox(&parent.id, None).unwrap();
            assert_eq!(
                inbox[1].text,
                "[watch: agent:idle:err-child transition=idle]\nretry done"
            );
            local.abort();
        });
        runtime.shutdown_timeout(Duration::from_secs(5));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn start_loops_wakes_kernel_path_and_pid_watches_sub_second() {
        use std::sync::Arc;
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let health = HealthBoard::default();
        let owner = store
            .create_agent(NewAgent::new("kernel-owner", "codex"))
            .unwrap();
        store.set_agent_running(&owner.id, "/work").unwrap();

        let shm_base = Path::new("/dev/shm");
        let signal_dir = if shm_base.is_dir() {
            tempfile::tempdir_in(shm_base).unwrap()
        } else {
            tempfile::tempdir().unwrap()
        };
        let signal_path = signal_dir.path().join("kernel-watch-signal");
        let mut proc_child = std::process::Command::new("/bin/sh")
            .args(["-c", "sleep 30"])
            .spawn()
            .unwrap();
        let pid = proc_child.id();

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
            let local = start_loops(Arc::clone(&store), health.clone(), stop_rx);
            create_agent_schedule(
                &store,
                &health,
                &owner,
                None,
                None,
                Some(&format!("file:{}", signal_path.display())),
                "file ready",
            )
            .unwrap();
            create_agent_schedule(
                &store,
                &health,
                &owner,
                None,
                None,
                Some(&format!("pid:{pid}")),
                "proc exited",
            )
            .unwrap();
            tokio::time::sleep(Duration::from_millis(120)).await;
            assert!(store.inbox(&owner.id, None).unwrap().is_empty());

            let triggered_at = std::time::Instant::now();
            std::fs::write(&signal_path, "done").unwrap();
            let _ = proc_child.kill();
            let _ = proc_child.wait();
            while store.inbox(&owner.id, None).unwrap().len() < 2 {
                assert!(
                    triggered_at.elapsed() < Duration::from_millis(600),
                    "kernel inotify/pidfd watches did not wake sub-second"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            local.abort();
        });
        runtime.shutdown_timeout(Duration::from_secs(5));
    }

    #[test]
    fn fire_webhook_event_wakes_matching_and_wildcard_watches_idempotently() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let exact_owner = store
            .create_agent(NewAgent::new("deploy-watcher", "codex"))
            .unwrap();
        let wildcard_owner = store
            .create_agent(NewAgent::new("all-events", "codex"))
            .unwrap();
        let other_owner = store
            .create_agent(NewAgent::new("other-watcher", "codex"))
            .unwrap();
        for a in [&exact_owner, &wildcard_owner, &other_owner] {
            store.set_agent_running(&a.id, "/work").unwrap();
        }
        create_agent_schedule(
            &store,
            &health,
            &exact_owner,
            None,
            None,
            Some("webhook:deploy/prod"),
            "verify deploy",
        )
        .unwrap();
        create_agent_schedule(
            &store,
            &health,
            &wildcard_owner,
            None,
            None,
            Some("event:*"),
            "audit event",
        )
        .unwrap();
        create_agent_schedule(
            &store,
            &health,
            &other_owner,
            None,
            None,
            Some("webhook:billing/invoice"),
            "check invoice",
        )
        .unwrap();

        let now = anyagents_core::now_ms();
        let payload = serde_json::json!({"release": "v2.0.0", "status": "ok"});
        let dispatch = fire_webhook_event(&store, "deploy/prod", "dlv-1", &payload, now).unwrap();
        assert_eq!(dispatch.matched, 2);
        assert_eq!(dispatch.fired_agents.len(), 2);

        let exact_inbox = store.inbox(&exact_owner.id, None).unwrap();
        assert_eq!(exact_inbox.len(), 1);
        assert!(
            exact_inbox[0]
                .text
                .contains("[webhook: deploy/prod delivery=dlv-1]")
        );
        assert!(exact_inbox[0].text.contains("verify deploy"));
        assert!(exact_inbox[0].text.contains("v2.0.0"));

        let wild_inbox = store.inbox(&wildcard_owner.id, None).unwrap();
        assert_eq!(wild_inbox.len(), 1);
        assert!(wild_inbox[0].text.contains("audit event"));

        assert!(store.inbox(&other_owner.id, None).unwrap().is_empty());

        // Retrying the same delivery after the one-shot watch fired is a no-op.
        let retry =
            fire_webhook_event(&store, "deploy/prod", "dlv-1", &payload, now + 1000).unwrap();
        assert_eq!(retry.matched, 0);
        assert_eq!(store.inbox(&exact_owner.id, None).unwrap().len(), 1);
    }

    #[test]
    fn create_agent_schedule_accepts_github_watches_and_jitters_hourly_every_schedules() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let a = store
            .create_agent(NewAgent::new("patrol-a", "codex"))
            .unwrap();
        let b = store
            .create_agent(NewAgent::new("patrol-b", "codex"))
            .unwrap();
        store.set_agent_running(&a.id, "/work").unwrap();
        store.set_agent_running(&b.id, "/work").unwrap();

        let mk_w =
            |w| create_agent_schedule(&store, &health, &a, None, None, Some(w), "w").unwrap();
        assert_eq!(
            mk_w("github:pr:owner/repo#123").watch_baseline.as_deref(),
            Some("pending")
        );
        assert_eq!(
            mk_w("github:check:owner/repo@abc1234")
                .watch_baseline
                .as_deref(),
            Some("pending")
        );

        let hour = 3_600_000i64;
        let sa = create_agent_schedule(&store, &health, &a, None, Some("1h"), None, "p").unwrap();
        let sb = create_agent_schedule(&store, &health, &b, None, Some("1h"), None, "p").unwrap();
        assert_ne!(sa.id, sb.id);
        for (s, aid) in [(&sa, &a.id), (&sb, &b.id)] {
            let raw = store.get_schedule(&s.id).unwrap().unwrap();
            let off = raw.next_fire_at_ms.unwrap() - (raw.created_at_ms + hour);
            assert_eq!(
                off,
                Store::recurring_schedule_jitter_ms(&raw.id, aid.as_str(), hour)
            );
            assert!((0..=180_000).contains(&off));
        }
        let fast = create_agent_schedule(&store, &health, &a, None, Some("5m"), None, "f").unwrap();
        let raw_f = store.get_schedule(&fast.id).unwrap().unwrap();
        assert_eq!(
            raw_f.next_fire_at_ms.unwrap() - (raw_f.created_at_ms + 300_000),
            0
        );
    }
}
