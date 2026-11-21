//! Event-driven schedule timer hexagonal port (`TimerBackend`), single-schedule
//! firing engine, and concrete local / external webhook adapters.

pub mod external;
pub mod local;

use std::fmt::Write as _;
use std::sync::Arc;

use anyagents_core::{Agent, AgentSchedule, Incoming, Store};
use anyagents_supervisor::HealthBoard;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

pub use external::ExternalWebhookTimerBackend;
pub use local::LocalTokioTimerBackend;

/// Opt-in environment flag allowing serverless coordinators (`ANYAGENTS_SERVERLESS=true`,
/// e.g. always-on `min_instances >= 1` replicas without an external scheduler yet)
/// to use [`LocalTokioTimerBackend`] instead of [`NoopTimerBackend`].
pub const ENV_TIMER_LOCAL_FALLBACK: &str = "ANYAGENTS_TIMER_LOCAL_FALLBACK";

/// Return `true` when `ANYAGENTS_TIMER_LOCAL_FALLBACK` is enabled (`1`, `true`, `yes`, `on`).
pub fn timer_local_fallback_enabled() -> bool {
    std::env::var(ENV_TIMER_LOCAL_FALLBACK)
        .ok()
        .is_some_and(|raw| {
            matches!(
                raw.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
}

/// Per-coordinator limit for remote startup arms and remote fire callbacks.
pub(crate) const REMOTE_TIMER_CONCURRENCY: usize = 4;

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

    /// Restore a timer at startup. Remote adapters may retry transient failures
    /// with the same timer identity; local adapters need no retry policy.
    fn restore_one_shot(
        &self,
        schedule_id: &str,
        fire_at_ms: i64,
    ) -> Result<Option<String>, String> {
        self.schedule_one_shot(schedule_id, fire_at_ms)
    }

    /// Cancel a pending trigger for `schedule_id`, using the persisted
    /// `external_run_id` when available.
    fn cancel(&self, schedule_id: &str, external_run_id: Option<&str>) -> Result<(), String>;

    /// Returns `true` when this backend is a no-op placeholder that never fires timers.
    fn is_noop(&self) -> bool {
        false
    }

    /// Whether this backend submits timers over the network.
    fn is_remote(&self) -> bool {
        false
    }

    /// Short stable backend kind identifier for startup diagnostics and logs.
    fn backend_kind(&self) -> &'static str {
        "custom"
    }
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

    fn is_noop(&self) -> bool {
        true
    }

    fn backend_kind(&self) -> &'static str {
        "noop"
    }
}

const MAX_EXTERNAL_RUN_ID_LEN: usize = 63;

/// Build a URL-safe deterministic run/timer ID for a `(schedule_id, fire_at_ms)` slot
/// strictly matching `^[a-z0-9_-]{1,63}$`.
///
/// - Lowercases ASCII letters and maps any character outside `[a-z0-9_-]` (such as
///   `:` in `sys:*` timer IDs) to `-` so external delayed-run endpoints with strict
///   `runId` grammar and `/{runId}:cancel` suffix parsing accept both user schedules
///   and system maintenance timers cleanly.
/// - Deterministically compacts IDs longer than 63 characters (such as
///   `sys:turn-timeout:turn_...`) with a 16-hex-char SHA-256 digest of `schedule_id`
///   so `schedule_one_shot` and `cancel` derive the exact same `<= 63`-char run ID.
pub fn deterministic_run_id(schedule_id: &str, fire_at_ms: i64) -> String {
    let trimmed = schedule_id.trim();
    let safe: String = trimmed
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else if matches!(c, '_' | '-') {
                c
            } else {
                '-'
            }
        })
        .collect();
    let safe_non_empty = if safe.is_empty() {
        "timer".to_string()
    } else {
        safe
    };
    let candidate = format!("{safe_non_empty}-{fire_at_ms}");
    if candidate.len() <= MAX_EXTERNAL_RUN_ID_LEN {
        return candidate;
    }

    let digest = Sha256::digest(trimmed.as_bytes());
    let mut hash_hex = String::with_capacity(16);
    for byte in &digest[..8] {
        let _ = write!(&mut hash_hex, "{byte:02x}");
    }
    let suffix = format!("-{hash_hex}-{fire_at_ms}");
    let max_prefix = MAX_EXTERNAL_RUN_ID_LEN.saturating_sub(suffix.len());
    let prefix_len = max_prefix.min(16).min(safe_non_empty.len());
    let prefix = safe_non_empty[..prefix_len].trim_end_matches('-');
    let compact = if prefix.is_empty() {
        format!("{hash_hex}-{fire_at_ms}")
    } else {
        format!("{prefix}{suffix}")
    };
    if compact.len() <= MAX_EXTERNAL_RUN_ID_LEN {
        compact
    } else {
        compact[..MAX_EXTERNAL_RUN_ID_LEN].to_string()
    }
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
/// 2. Otherwise in serverless mode (`ANYAGENTS_SERVERLESS=true`):
///    - When `ANYAGENTS_TIMER_LOCAL_FALLBACK=true` (or `1`), uses [`LocalTokioTimerBackend`]
///      and rehydrates active schedules from `Store` (for always-on serverless replicas).
///    - Otherwise returns [`NoopTimerBackend`] with a loud `ERROR` startup diagnostic
///      reporting the exact count of unscheduled active schedules in `Store`.
/// 3. Otherwise in persistent local mode, creates [`LocalTokioTimerBackend`] and rehydrates
///    all active timer schedules from `Store` with zero 60s periodic polling.
pub fn build_timer_backend(
    store: Arc<Store>,
    health: HealthBoard,
    handle: tokio::runtime::Handle,
) -> Arc<dyn TimerBackend> {
    if let Some(ext) = ExternalWebhookTimerBackend::from_env(store.data_dir()) {
        eprintln!(
            "anyagents-timer: backend=external_webhook mode={} host={}",
            ext.mode().as_str(),
            ext.scheduler_host()
        );
        return Arc::new(ext);
    }
    if crate::server::serverless::is_serverless_mode() {
        if timer_local_fallback_enabled() {
            let local = LocalTokioTimerBackend::new(store, health, handle);
            match local.rehydrate_from_store() {
                Ok(armed) => {
                    eprintln!(
                        "anyagents-timer: backend=local_tokio serverless_fallback=true rehydrated={armed}"
                    );
                }
                Err(err) => {
                    eprintln!(
                        "anyagents-timer: backend=local_tokio serverless_fallback=true rehydrate_warning={err}"
                    );
                }
            }
            return Arc::new(local);
        }
        eprintln!(
            "ERROR anyagents-timer: ANYAGENTS_SERVERLESS=true without ANYAGENTS_TIMER_SCHEDULER_URL — schedules will NOT fire (NoopTimerBackend); set ANYAGENTS_TIMER_SCHEDULER_URL or ANYAGENTS_TIMER_LOCAL_FALLBACK=true"
        );
        let active_count = match store.list_schedules(None, true) {
            Ok(rows) => rows.len(),
            Err(err) => {
                eprintln!(
                    "ERROR anyagents-timer: failed to inspect active schedules under NoopTimerBackend: {err}"
                );
                0
            }
        };
        eprintln!(
            "anyagents-timer: backend=noop serverless=true unscheduled_active_schedules={active_count}"
        );
        if active_count > 0 {
            eprintln!(
                "ERROR anyagents-timer: {active_count} active schedule(s) in store are UNSCHEDULED and will NOT fire under NoopTimerBackend"
            );
        }
        return Arc::new(NoopTimerBackend);
    }
    let local = LocalTokioTimerBackend::new(store, health, handle);
    match local.rehydrate_from_store() {
        Ok(armed) => {
            eprintln!("anyagents-timer: backend=local_tokio rehydrated={armed}");
        }
        Err(err) => {
            eprintln!("anyagents-timer: backend=local_tokio rehydrate_warning={err}");
        }
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
    let Some(fire_at_ms) = sched.next_fire_at_ms else {
        return Ok(None);
    };
    arm_schedule_timer_at(store, backend, sched, fire_at_ms)
}

/// Arm active `sched` on `backend` at an explicit `fire_at_ms` timestamp
/// without mutating `sched.next_fire_at_ms` in `Store`, persisting any returned
/// external run ID on the schedule row.
///
/// Used when rehydrating overdue schedules (`max(next_fire_at_ms, now + 1_000)`)
/// or immediate watch probes so external delayed-run schedulers that reject past
/// `scheduleTime` values accept the trigger cleanly and fire immediately.
pub fn arm_schedule_timer_at(
    store: &Store,
    backend: &dyn TimerBackend,
    sched: &AgentSchedule,
    fire_at_ms: i64,
) -> Result<Option<String>, String> {
    if sched.state != "active" {
        return Ok(None);
    }
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
    // Reject triggers from earlier slots while allowing overdue catch-up triggers
    // whose scheduled `intended_at_ms` is `>= expected_due`.
    if let (Some(intended), Some(stored)) = (intended_at_ms, expected_due)
        && intended < stored
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
    use std::sync::Mutex;

    use anyagents_core::{NewAgent, NewSchedule};

    use super::*;

    struct EnvRestore(Vec<(&'static str, Option<std::ffi::OsString>)>);

    impl EnvRestore {
        fn capture(keys: &[&'static str]) -> Self {
            Self(keys.iter().map(|&k| (k, std::env::var_os(k))).collect())
        }
    }

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            for (k, v) in &self.0 {
                match v {
                    Some(val) => unsafe { std::env::set_var(k, val) },
                    None => unsafe { std::env::remove_var(k) },
                }
            }
        }
    }

    #[derive(Default)]
    struct RecordingTimer {
        scheduled: Mutex<Vec<(String, i64)>>,
    }

    impl TimerBackend for RecordingTimer {
        fn schedule_one_shot(
            &self,
            schedule_id: &str,
            fire_at_ms: i64,
        ) -> Result<Option<String>, String> {
            self.scheduled
                .lock()
                .unwrap()
                .push((schedule_id.to_string(), fire_at_ms));
            Ok(Some(deterministic_run_id(schedule_id, fire_at_ms)))
        }

        fn cancel(&self, _schedule_id: &str, _external_run_id: Option<&str>) -> Result<(), String> {
            Ok(())
        }
    }

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

    #[test]
    fn deterministic_run_id_strictly_matches_external_grammar_and_compacts_long_ids() {
        let is_valid = |id: &str| {
            !id.is_empty()
                && id.len() <= MAX_EXTERNAL_RUN_ID_LEN
                && id
                    .chars()
                    .all(|c| matches!(c, 'a'..='z' | '0'..='9' | '_' | '-'))
        };

        let fire_at = 1_795_185_946_450i64;
        let long_timeout = "sys:turn-timeout:turn_00000000000000000000000000000001";
        let long_timeout_other = "sys:turn-timeout:turn_00000000000000000000000000000002";
        let cloud_poll = "sys:cloud-poll:turn_00000000000000000000000000000001";
        let lease_renew = "sys:lease-renew:agt_00000000000000000000000001";
        let uppercase_colons = "SYS:Turn-Timeout:ABC_123:DEF";
        let very_long = format!("sys:custom:{}", "X".repeat(200));

        for sample in [
            "sch_abc123",
            long_timeout,
            long_timeout_other,
            cloud_poll,
            lease_renew,
            uppercase_colons,
            very_long.as_str(),
            "",
            "   ",
            "weird/id?with spaces:and@symbols",
        ] {
            let run_id = deterministic_run_id(sample, fire_at);
            assert!(
                is_valid(&run_id),
                "run_id `{run_id}` (len {}) for `{sample}` violates ^[a-z0-9_-]{{1,63}}$",
                run_id.len()
            );
            // Deterministic across schedule_one_shot and cancel calls.
            assert_eq!(run_id, deterministic_run_id(sample, fire_at));
        }

        // Short schedules keep exact backward-compatible `{safe}-{fire_at_ms}` format.
        assert_eq!(
            deterministic_run_id("sch_abc123", 1_763_635_511_250),
            "sch_abc123-1763635511250"
        );
        // Uppercase is lowercased and colons sanitized to `-`.
        assert_eq!(
            deterministic_run_id(uppercase_colons, fire_at),
            format!("sys-turn-timeout-abc_123-def-{fire_at}")
        );
        // Distinct >63-char turn IDs hash to distinct deterministic compacted run IDs.
        assert_ne!(
            deterministic_run_id(long_timeout, fire_at),
            deterministic_run_id(long_timeout_other, fire_at)
        );
        assert!(deterministic_run_id(long_timeout, fire_at).starts_with("sys-turn-timeout-"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)]
    async fn build_timer_backend_selection_modes() {
        let _env = crate::server::tests::env_lock();
        let _restore = EnvRestore::capture(&[
            "ANYAGENTS_SERVERLESS",
            "ANYAGENTS_TIMER_SCHEDULER_URL",
            "ANYAGENTS_TIMER_SCHEDULER_TOKEN",
            "ANYAGENTS_TIMER_SCHEDULER_MODE",
            "ANYAGENTS_TIMER_LOCAL_FALLBACK",
            "ANYAGENTS_PUBLIC_URL",
        ]);
        for key in [
            "ANYAGENTS_SERVERLESS",
            "ANYAGENTS_TIMER_SCHEDULER_URL",
            "ANYAGENTS_TIMER_SCHEDULER_TOKEN",
            "ANYAGENTS_TIMER_SCHEDULER_MODE",
            "ANYAGENTS_TIMER_LOCAL_FALLBACK",
            "ANYAGENTS_PUBLIC_URL",
        ] {
            unsafe { std::env::remove_var(key) };
        }

        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let agent = store
            .create_agent(NewAgent::new("timer-select-agent", "codex"))
            .unwrap();
        store.set_agent_running(&agent.id, "/work").unwrap();
        store
            .create_schedule(NewSchedule {
                agent_id: agent.id.clone(),
                kind: "every".into(),
                spec: "1h".into(),
                prompt: "hourly check".into(),
                interval_ms: Some(3_600_000),
                next_fire_at_ms: Some(anyagents_core::now_ms() + 3_600_000),
                watch_baseline: None,
            })
            .unwrap();

        let health = HealthBoard::default();
        let handle = tokio::runtime::Handle::current();

        // 1. Serverless + ANYAGENTS_TIMER_SCHEDULER_URL -> ExternalWebhookTimerBackend.
        unsafe {
            std::env::set_var("ANYAGENTS_SERVERLESS", "true");
            std::env::set_var(
                "ANYAGENTS_TIMER_SCHEDULER_URL",
                "https://sched.example.test/v1/workflows/timer/runs",
            );
            std::env::set_var("ANYAGENTS_PUBLIC_URL", "https://coord.example.test");
        }
        let tb_ext = build_timer_backend(Arc::clone(&store), health.clone(), handle.clone());
        assert!(!tb_ext.is_noop());
        assert_eq!(tb_ext.backend_kind(), "external_webhook");

        // 2. Serverless without scheduler URL + ANYAGENTS_TIMER_LOCAL_FALLBACK=true -> LocalTokioTimerBackend.
        unsafe {
            std::env::remove_var("ANYAGENTS_TIMER_SCHEDULER_URL");
            std::env::set_var("ANYAGENTS_TIMER_LOCAL_FALLBACK", "true");
        }
        let tb_fallback = build_timer_backend(Arc::clone(&store), health.clone(), handle.clone());
        assert!(!tb_fallback.is_noop());
        assert_eq!(tb_fallback.backend_kind(), "local_tokio");

        // Also accepts "1" for ANYAGENTS_TIMER_LOCAL_FALLBACK.
        unsafe {
            std::env::set_var("ANYAGENTS_TIMER_LOCAL_FALLBACK", "1");
        }
        let tb_fallback_one =
            build_timer_backend(Arc::clone(&store), health.clone(), handle.clone());
        assert!(!tb_fallback_one.is_noop());
        assert_eq!(tb_fallback_one.backend_kind(), "local_tokio");

        // 3. Serverless without scheduler URL or local fallback -> NoopTimerBackend (is_noop() == true).
        unsafe {
            std::env::remove_var("ANYAGENTS_TIMER_LOCAL_FALLBACK");
        }
        let tb_noop = build_timer_backend(Arc::clone(&store), health.clone(), handle.clone());
        assert!(tb_noop.is_noop());
        assert_eq!(tb_noop.backend_kind(), "noop");

        // 4. Persistent local mode (ANYAGENTS_SERVERLESS=false) -> LocalTokioTimerBackend.
        unsafe {
            std::env::set_var("ANYAGENTS_SERVERLESS", "false");
        }
        let tb_local = build_timer_backend(Arc::clone(&store), health, handle);
        assert!(!tb_local.is_noop());
        assert_eq!(tb_local.backend_kind(), "local_tokio");
    }

    #[test]
    fn arm_schedule_timer_at_preserves_db_due_and_overdue_oneshot_fires_cleanly() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let agent = store
            .create_agent(NewAgent::new("overdue-at-agent", "codex"))
            .unwrap();
        store.set_agent_running(&agent.id, "/work").unwrap();

        let now = anyagents_core::now_ms();
        let overdue_due = now - 5_000;
        let catchup_at = now + 1_000;
        let sched = store
            .create_schedule(NewSchedule {
                agent_id: agent.id.clone(),
                kind: "at".into(),
                spec: "1s".into(),
                prompt: "overdue one-shot wake".into(),
                interval_ms: None,
                next_fire_at_ms: Some(overdue_due),
                watch_baseline: None,
            })
            .unwrap();

        let recorder = RecordingTimer::default();
        let ext_id = arm_schedule_timer_at(&store, &recorder, &sched, catchup_at)
            .unwrap()
            .unwrap();
        assert_eq!(ext_id, deterministic_run_id(&sched.id, catchup_at));
        assert_eq!(
            recorder.scheduled.lock().unwrap().as_slice(),
            &[(sched.id.clone(), catchup_at)]
        );

        let persisted = store.get_schedule(&sched.id).unwrap().unwrap();
        assert_eq!(persisted.next_fire_at_ms, Some(overdue_due));
        assert_eq!(
            persisted.external_timer_id.as_deref(),
            Some(ext_id.as_str())
        );

        // Earlier stale slot is still rejected.
        let stale = fire_schedule_by_id(
            &store,
            &health,
            &recorder,
            &sched.id,
            Some(overdue_due - 1_000),
            catchup_at,
        )
        .unwrap();
        assert!(!stale.fired);
        assert_eq!(stale.reason.as_deref(), Some("stale_slot"));

        // Catch-up trigger (`intended_at_ms == catchup_at > overdue_due`) fires cleanly.
        let fired = fire_schedule_by_id(
            &store,
            &health,
            &recorder,
            &sched.id,
            Some(catchup_at),
            catchup_at,
        )
        .unwrap();
        assert!(fired.fired);
        assert_eq!(
            store.get_schedule(&sched.id).unwrap().unwrap().state,
            "fired"
        );
        assert_eq!(store.inbox(&agent.id, None).unwrap().len(), 1);
    }
}
