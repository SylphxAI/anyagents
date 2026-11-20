//! Event-driven schedule timer hexagonal port (`TimerBackend`), single-schedule
//! firing engine, and concrete local / external webhook adapters.

pub mod external;
pub mod local;

use std::sync::Arc;

use anyagents_core::{Agent, AgentSchedule, Incoming, Store};
use anyagents_supervisor::HealthBoard;
use serde::{Deserialize, Serialize};

pub use external::ExternalWebhookTimerBackend;
pub use local::LocalTokioTimerBackend;

/// Hexagonal port for scheduling and cancelling exact one-shot schedule triggers
/// with sub-second precision (millisecond epoch timestamps).
pub trait TimerBackend: Send + Sync {
    /// Arm a one-shot trigger for `schedule_id` at `fire_at_ms` (Unix epoch ms).
    ///
    /// Returns an optional external run/timer identifier to persist on the
    /// schedule row so cancellation survives stateless replica cold starts.
    fn schedule_one_shot(
        &self,
        schedule_id: &str,
        fire_at_ms: i64,
    ) -> Result<Option<String>, String>;

    /// Cancel a pending trigger for `schedule_id`, using the persisted
    /// `external_run_id` when available.
    fn cancel(&self, schedule_id: &str, external_run_id: Option<&str>) -> Result<(), String>;
}

/// No-op timer backend used in unit tests or serverless deployments without an
/// external scheduler configured.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopTimerBackend;

impl TimerBackend for NoopTimerBackend {
    fn schedule_one_shot(
        &self,
        _schedule_id: &str,
        _fire_at_ms: i64,
    ) -> Result<Option<String>, String> {
        Ok(None)
    }

    fn cancel(&self, _schedule_id: &str, _external_run_id: Option<&str>) -> Result<(), String> {
        Ok(())
    }
}

/// Build the deterministic run/timer ID for a `(schedule_id, fire_at_ms)` slot.
pub fn deterministic_run_id(schedule_id: &str, fire_at_ms: i64) -> String {
    format!("{schedule_id}-{fire_at_ms}")
}

/// Format a Unix epoch millisecond timestamp as RFC 3339 UTC with millisecond
/// precision (e.g. `2026-11-20T10:45:11.250Z`) using `jiff`.
pub fn format_fire_at_rfc3339_ms(fire_at_ms: i64) -> Result<String, String> {
    let ts = jiff::Timestamp::from_millisecond(fire_at_ms)
        .map_err(|err| format!("invalid fire_at_ms `{fire_at_ms}`: {err}"))?;
    Ok(format!("{ts:.3}"))
}

/// Structured outcome returned when firing one schedule by ID.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FireResult {
    pub fired: bool,
    pub schedule_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_run_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl FireResult {
    fn skipped(schedule_id: &str, reason: &str) -> Self {
        Self {
            fired: false,
            schedule_id: schedule_id.to_string(),
            agent_id: None,
            next_run_ms: None,
            reason: Some(reason.to_string()),
        }
    }
}

/// Select and initialize the active [`TimerBackend`] for the coordinator:
/// 1. When `ANYAGENTS_TIMER_SCHEDULER_URL` is configured, uses [`ExternalWebhookTimerBackend`].
/// 2. Otherwise in Pure Serverless mode, returns [`NoopTimerBackend`] (never spawns local loops).
/// 3. Otherwise in persistent local mode, creates [`LocalTokioTimerBackend`] and rehydrates
///    all active timer schedules from `Store` with zero 60s periodic polling.
pub fn build_timer_backend(
    store: Arc<Store>,
    health: HealthBoard,
    handle: tokio::runtime::Handle,
) -> Arc<dyn TimerBackend> {
    if let Some(ext) = ExternalWebhookTimerBackend::from_env(store.data_dir()) {
        return Arc::new(ext);
    }
    if crate::server::serverless::is_serverless_mode() {
        return Arc::new(NoopTimerBackend);
    }
    let local = LocalTokioTimerBackend::new(store, health, handle);
    if let Err(err) = local.rehydrate_from_store() {
        eprintln!("anyagents-timer: startup schedule rehydration warning: {err}");
    }
    Arc::new(local)
}

/// Arm `sched` on `backend` when active with a `next_fire_at_ms`, persisting
/// any returned external run ID on the schedule row.
pub fn arm_schedule_timer(
    store: &Store,
    backend: &dyn TimerBackend,
    sched: &AgentSchedule,
) -> Result<Option<String>, String> {
    if sched.state != "active" {
        return Ok(None);
    }
    let Some(fire_at_ms) = sched.next_fire_at_ms else {
        return Ok(None);
    };
    let ext_id = backend.schedule_one_shot(&sched.id, fire_at_ms)?;
    if let Err(err) = store.set_schedule_external_timer_id(&sched.id, ext_id.as_deref()) {
        eprintln!(
            "anyagents-timer: failed to persist external_timer_id for {}: {err}",
            sched.id
        );
    }
    Ok(ext_id)
}

/// Cancel a pending trigger on `backend` for a cancelled or archived schedule.
/// Uses persisted `external_timer_id` first, falling back to the deterministic
/// run ID when `next_fire_at_ms` was known.
pub fn cancel_schedule_timer(backend: &dyn TimerBackend, sched: &AgentSchedule) {
    let fallback_id = sched
        .external_timer_id
        .is_none()
        .then(|| {
            sched
                .next_fire_at_ms
                .map(|ms| deterministic_run_id(&sched.id, ms))
        })
        .flatten();
    let effective_id = sched
        .external_timer_id
        .as_deref()
        .or(fallback_id.as_deref());
    if let Err(err) = backend.cancel(&sched.id, effective_id) {
        eprintln!("anyagents-timer: cancel failed for {}: {err}", sched.id);
    }
}

/// Fire strictly one schedule identified by `schedule_id`, without scanning or
/// waking any unrelated schedule or idle tenant.
///
/// - Deleted, cancelled, paused, or already-fired schedules return idempotent
///   `FireResult { fired: false, reason: Some("not_found_or_inactive"), .. }`.
/// - One-shot (`at`) schedules transition atomically to `"fired"` and are NOT re-armed.
/// - Recurring (`every`) schedules compute and persist `next_fire_at_ms`, then
///   immediately re-arm the next one-shot on `timer_backend`.
/// - Event watches (`watch`) evaluate only this single watch and re-arm if still active
///   with a next probe timestamp.
pub fn fire_schedule_by_id(
    store: &Store,
    health: &HealthBoard,
    timer_backend: &dyn TimerBackend,
    schedule_id: &str,
    intended_at_ms: Option<i64>,
    now: i64,
) -> Result<FireResult, String> {
    let clean_id = schedule_id.trim();
    if clean_id.is_empty() {
        return Ok(FireResult::skipped(schedule_id, "not_found_or_inactive"));
    }
    let Some(sched) = store
        .get_schedule(clean_id)
        .map_err(|err| format!("schedule lookup failed: {err}"))?
    else {
        return Ok(FireResult::skipped(clean_id, "not_found_or_inactive"));
    };
    if sched.state != "active" {
        return Ok(FireResult::skipped(&sched.id, "not_found_or_inactive"));
    }

    let owner = match store.agent(&sched.agent_id) {
        Ok(agent)
            if agent.state == "archived"
                || crate::server::is_explicitly_archived(store.data_dir(), &agent) =>
        {
            let _ = store.cancel_schedule(None, &sched.id);
            return Ok(FireResult::skipped(&sched.id, "not_found_or_inactive"));
        }
        Ok(agent) => agent,
        Err(anyagents_core::Error::AgentNotFound(_)) => {
            let _ = store.cancel_schedule(None, &sched.id);
            return Ok(FireResult::skipped(&sched.id, "not_found_or_inactive"));
        }
        Err(err) => return Err(format!("owner lookup failed: {err}")),
    };

    match sched.kind.as_str() {
        "at" => fire_one_shot_at(store, &owner, &sched, intended_at_ms, now),
        "every" => fire_recurring_every(store, timer_backend, &owner, &sched, intended_at_ms, now),
        "watch" => fire_single_watch(store, health, timer_backend, &owner, &sched, now),
        _ => Ok(FireResult::skipped(&sched.id, "unsupported_kind")),
    }
}

fn fire_one_shot_at(
    store: &Store,
    owner: &Agent,
    sched: &AgentSchedule,
    intended_at_ms: Option<i64>,
    now: i64,
) -> Result<FireResult, String> {
    let expected_due = sched.next_fire_at_ms;
    if let (Some(intended), Some(stored)) = (intended_at_ms, expected_due)
        && intended != stored
    {
        return Ok(FireResult::skipped(&sched.id, "stale_slot"));
    }

    let Ok(_lifecycle) = store.try_lock_agent_lifecycle(&owner.id) else {
        return Ok(FireResult::skipped(&sched.id, "lifecycle_busy"));
    };
    let fresh_owner = store
        .agent(&owner.id)
        .map_err(|err| format!("owner reload failed: {err}"))?;
    if fresh_owner.state == "archived"
        || crate::server::is_explicitly_archived(store.data_dir(), &fresh_owner)
    {
        let _ = store.cancel_schedule(None, &sched.id);
        return Ok(FireResult::skipped(&sched.id, "not_found_or_inactive"));
    }

    let slot_ts = expected_due.unwrap_or(now);
    let source_key = format!("schedule:{}:{slot_ts}", sched.id);
    let delivered = store
        .claim_schedule_tick_with_wake(
            &sched.id,
            expected_due,
            "fired",
            None,
            Some(now),
            None,
            Some("fired"),
            &fresh_owner.id,
            Incoming {
                text: sched.prompt.clone(),
                sender: Some(format!("schedule:{}", sched.id)),
                source_time_ms: Some(now),
                source_key: Some(source_key),
                reply_to: None,
                quote: None,
                origin: None,
                received_at_ms: Some(now),
            },
        )
        .map_err(|err| format!("claim one-shot schedule failed: {err}"))?;

    if !delivered {
        return Ok(FireResult::skipped(&sched.id, "already_claimed"));
    }
    let _ = store.set_schedule_external_timer_id(&sched.id, None);
    crate::create::poke_agent_wake_socket(store.data_dir(), &fresh_owner.id);
    crate::create::poke_agent_wake_socket_mail(store.data_dir(), &fresh_owner.id);

    Ok(FireResult {
        fired: true,
        schedule_id: sched.id.clone(),
        agent_id: Some(fresh_owner.id.as_str().to_string()),
        next_run_ms: None,
        reason: None,
    })
}

fn fire_recurring_every(
    store: &Store,
    timer_backend: &dyn TimerBackend,
    owner: &Agent,
    sched: &AgentSchedule,
    intended_at_ms: Option<i64>,
    now: i64,
) -> Result<FireResult, String> {
    let expected_due = sched.next_fire_at_ms;
    if let (Some(intended), Some(stored)) = (intended_at_ms, expected_due)
        && intended < stored
    {
        return Ok(FireResult::skipped(&sched.id, "stale_slot"));
    }

    let due = expected_due.unwrap_or(now);
    let interval = sched.interval_ms.unwrap_or(60_000).max(1);
    let fire_ref_ms = now.max(due);
    let next_due = Store::next_recurring_fire_ms(
        &sched.id,
        sched.agent_id.as_str(),
        sched.created_at_ms,
        due,
        interval,
        fire_ref_ms,
    );
    let next_state = if next_due.is_some() {
        "active"
    } else {
        "fired"
    };

    let sender = format!("schedule:{}", sched.id);
    let previous_pending = store
        .has_undelivered_message_from(&owner.id, &sender)
        .map_err(|err| format!("mailbox pending check failed: {err}"))?;

    if previous_pending {
        let claimed = store
            .claim_schedule_tick(
                &sched.id,
                expected_due,
                next_state,
                next_due,
                None,
                None,
                Some("coalesced"),
            )
            .map_err(|err| format!("coalesce recurring schedule failed: {err}"))?;
        if !claimed {
            return Ok(FireResult::skipped(&sched.id, "already_claimed"));
        }
        rearm_after_claim(store, timer_backend, &sched.id, next_due);
        return Ok(FireResult {
            fired: false,
            schedule_id: sched.id.clone(),
            agent_id: Some(owner.id.as_str().to_string()),
            next_run_ms: next_due,
            reason: Some("coalesced".to_string()),
        });
    }

    let Ok(_lifecycle) = store.try_lock_agent_lifecycle(&owner.id) else {
        return Ok(FireResult::skipped(&sched.id, "lifecycle_busy"));
    };
    let fresh_owner = store
        .agent(&owner.id)
        .map_err(|err| format!("owner reload failed: {err}"))?;
    if fresh_owner.state == "archived"
        || crate::server::is_explicitly_archived(store.data_dir(), &fresh_owner)
    {
        let _ = store.cancel_schedule(None, &sched.id);
        return Ok(FireResult::skipped(&sched.id, "not_found_or_inactive"));
    }

    let source_key = format!("schedule:{}:{due}", sched.id);
    let delivered = store
        .claim_schedule_tick_with_wake(
            &sched.id,
            expected_due,
            next_state,
            next_due,
            Some(now),
            None,
            Some("fired"),
            &fresh_owner.id,
            Incoming {
                text: sched.prompt.clone(),
                sender: Some(sender),
                source_time_ms: Some(now),
                source_key: Some(source_key),
                reply_to: None,
                quote: None,
                origin: None,
                received_at_ms: Some(now),
            },
        )
        .map_err(|err| format!("claim recurring schedule failed: {err}"))?;

    if !delivered {
        return Ok(FireResult::skipped(&sched.id, "already_claimed"));
    }

    crate::create::poke_agent_wake_socket(store.data_dir(), &fresh_owner.id);
    crate::create::poke_agent_wake_socket_mail(store.data_dir(), &fresh_owner.id);
    rearm_after_claim(store, timer_backend, &sched.id, next_due);

    Ok(FireResult {
        fired: true,
        schedule_id: sched.id.clone(),
        agent_id: Some(fresh_owner.id.as_str().to_string()),
        next_run_ms: next_due,
        reason: None,
    })
}

fn fire_single_watch(
    store: &Store,
    health: &HealthBoard,
    timer_backend: &dyn TimerBackend,
    owner: &Agent,
    sched: &AgentSchedule,
    now: i64,
) -> Result<FireResult, String> {
    let (eff_health, _) =
        crate::server::serverless_watches::effective_health_with_remote_turns(store, health, now);
    let (fired, next_probe_ms) =
        crate::schedule::evaluate_single_watch_schedule(store, &eff_health, sched, now);
    let still_active = store
        .get_schedule(&sched.id)
        .ok()
        .flatten()
        .is_some_and(|s| s.state == "active");
    let next_run_ms = if still_active {
        next_probe_ms.filter(|&ts| ts < i64::MAX)
    } else {
        None
    };
    rearm_after_claim(store, timer_backend, &sched.id, next_run_ms);
    Ok(FireResult {
        fired,
        schedule_id: sched.id.clone(),
        agent_id: Some(owner.id.as_str().to_string()),
        next_run_ms,
        reason: (!fired).then(|| "watch_pending".to_string()),
    })
}

fn rearm_after_claim(
    store: &Store,
    timer_backend: &dyn TimerBackend,
    schedule_id: &str,
    next_due: Option<i64>,
) {
    if let Some(next_ms) = next_due {
        match timer_backend.schedule_one_shot(schedule_id, next_ms) {
            Ok(ext_id) => {
                let _ = store.set_schedule_external_timer_id(schedule_id, ext_id.as_deref());
            }
            Err(err) => {
                eprintln!(
                    "anyagents-timer: failed to re-arm schedule {schedule_id} at {next_ms}: {err}"
                );
            }
        }
    } else {
        let _ = store.set_schedule_external_timer_id(schedule_id, None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_fire_at_rfc3339_ms_via_jiff() {
        assert_eq!(
            format_fire_at_rfc3339_ms(1_763_635_511_250).unwrap(),
            "2025-11-20T10:45:11.250Z"
        );
        assert_eq!(
            format_fire_at_rfc3339_ms(1_700_000_000_000).unwrap(),
            "2023-11-14T22:13:20.000Z"
        );
    }
}
