//! Event-driven serverless maintenance via `TimerBackend` + `/v1/internal/fire/{id}`:
//! - Step 2a (`sys:cloud-poll:<turn_id>`): on-demand cloud turn polling with bounded re-arm
//!   while running and cancel-on-settle + post-settlement mailbox pump.
//! - Step 2b (`sys:turn-timeout:<turn_id>`): exact stale sandbox timeout settlement with
//!   cancel-on-complete and heartbeat-aware deadline extension.
//! - Step 2c (`sys:reconcile:<agent_id>` + `startup_reconcile_serverless`): cold-start
//!   orphan open-turn recovery, timer rehydration for in-flight remote turns/leases, and
//!   undelivered-mailbox backlog draining.
//! - Step 3 (`sys:lease-renew:<agent_id>`): active sandbox lease renewal with cancel-on-close
//!   and zero wakes on idle tenants.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyagents_core::stateless::{PendingRemoteTurn, SandboxBinding, SettleRemoteTurn};
use anyagents_core::{AgentId, Store, now_ms};

use crate::sandbox_exec::{resolve_registry_for_binding, sandbox_handle_from_binding};
use crate::sandbox_providers::providers::{DEFAULT_MAX_LEASE_TTL_MS, ProviderKind};
use crate::serverless_cloud::resolve_cloud_driver_from_metadata;
use crate::timer::{ExternalWebhookTimerBackend, FireResult, NoopTimerBackend, TimerBackend};

use super::serverless::{
    DEFAULT_CLOUD_POLL_DELAY_MS, DEFAULT_LEASE_RENEW_WINDOW_MS, MAX_CLOUD_POLL_BACKOFF_MS,
    dispatch_next_mailbox_message_with_timer, is_serverless_mode, sandbox_stale_threshold_ms,
    settle_remote_turn_and_deliver,
};

pub(crate) const SYS_CLOUD_POLL_PREFIX: &str = "sys:cloud-poll:";
pub(crate) const SYS_TURN_TIMEOUT_PREFIX: &str = "sys:turn-timeout:";
pub(crate) const SYS_LEASE_RENEW_PREFIX: &str = "sys:lease-renew:";
pub(crate) const SYS_RECONCILE_PREFIX: &str = "sys:reconcile:";

const SYS_TIMERS_BLOB_NS: &str = "markers";
const MIN_LEASE_RENEW_LEAD_MS: i64 = 5_000;
const OVERDUE_SCHEDULE_CATCHUP_DELAY_MS: i64 = 1_000;
/// Bounded exponential backoff delays for transient serverless cold-wake / dispatch retries:
/// `15s -> 30s -> 60s` (3 attempts max, never an infinite hot loop).
pub(crate) const DISPATCH_RETRY_BACKOFF_MS: &[i64] = &[15_000, 30_000, 60_000];
const DISPATCH_RETRY_BLOB_PATH: &str = "sys_timers/dispatch_retry";
const STARTUP_ACTIVE_TURN_LIMIT: usize = 64;

/// Durable per-agent transient dispatch retry & error state stored in `agent_files`
/// (`namespace = "markers"`, `path = "sys_timers/dispatch_retry"`).
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct DispatchRetryState {
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_seq: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub last_error_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry_at_ms: Option<i64>,
}

pub(crate) fn is_system_timer_id(id: &str) -> bool {
    id.trim().starts_with("sys:")
}

pub(crate) fn cloud_poll_timer_id(turn_id: &str) -> String {
    format!("{SYS_CLOUD_POLL_PREFIX}{}", turn_id.trim())
}

pub(crate) fn turn_timeout_timer_id(turn_id: &str) -> String {
    format!("{SYS_TURN_TIMEOUT_PREFIX}{}", turn_id.trim())
}

pub(crate) fn lease_renew_timer_id(agent_id: &AgentId) -> String {
    format!("{SYS_LEASE_RENEW_PREFIX}{}", agent_id.as_str())
}

pub(crate) fn reconcile_timer_id(agent_id: &AgentId) -> String {
    format!("{SYS_RECONCILE_PREFIX}{}", agent_id.as_str())
}

fn sys_timer_blob_path(timer_id: &str) -> String {
    let key: String = timer_id
        .trim()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("sys_timers/{key}")
}

/// Resolve the active [`TimerBackend`] for serverless dispatch: prefers an explicit
/// injected backend (from `AppState` or tests), then `ExternalWebhookTimerBackend::from_env`,
/// falling back to `NoopTimerBackend`.
pub(crate) fn resolve_timer_backend(
    explicit: Option<&Arc<dyn TimerBackend>>,
    data_dir: &Path,
) -> Arc<dyn TimerBackend> {
    if let Some(tb) = explicit {
        return Arc::clone(tb);
    }
    if let Some(ext) = ExternalWebhookTimerBackend::from_env(data_dir) {
        return Arc::new(ext);
    }
    Arc::new(NoopTimerBackend)
}

/// Arm a system timer on `backend` and persist its external run ID in `agent_files`
/// (`ns = "sys_timers"`) so cancel-on-close works across stateless serverless cold starts.
pub(crate) fn arm_system_timer(
    store: &Store,
    backend: &dyn TimerBackend,
    agent_id: &AgentId,
    timer_id: &str,
    fire_at_ms: i64,
) -> Result<Option<String>, String> {
    let blob_path = sys_timer_blob_path(timer_id);
    let previous_ext_id = store
        .blob_store()
        .get_agent_file(agent_id, SYS_TIMERS_BLOB_NS, &blob_path)
        .ok()
        .flatten()
        .and_then(|entry| String::from_utf8(entry.content).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let ext_id = backend.schedule_one_shot(timer_id, fire_at_ms)?;

    if let Some(ref prev) = previous_ext_id
        && ext_id.as_deref() != Some(prev.as_str())
    {
        let _ = backend.cancel(timer_id, Some(prev.as_str()));
    }

    if let Some(ref id) = ext_id {
        let _ = store.blob_store().put_agent_file(
            agent_id,
            SYS_TIMERS_BLOB_NS,
            &blob_path,
            id.as_bytes(),
            false,
        );
    } else if previous_ext_id.is_some() {
        let _ = store
            .blob_store()
            .delete_agent_file(agent_id, SYS_TIMERS_BLOB_NS, &blob_path);
    }
    Ok(ext_id)
}

/// Cancel a pending system timer on `backend` using its persisted external run ID
/// (if any) and delete the persisted tracking blob.
pub(crate) fn cancel_system_timer(
    store: &Store,
    backend: &dyn TimerBackend,
    agent_id: &AgentId,
    timer_id: &str,
) {
    let blob_path = sys_timer_blob_path(timer_id);
    let ext_id = store
        .blob_store()
        .get_agent_file(agent_id, SYS_TIMERS_BLOB_NS, &blob_path)
        .ok()
        .flatten()
        .and_then(|entry| String::from_utf8(entry.content).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    if let Err(err) = backend.cancel(timer_id, ext_id.as_deref()) {
        eprintln!("anyagents-timer: system cancel failed for {timer_id}: {err}");
    }
    let _ = store
        .blob_store()
        .delete_agent_file(agent_id, SYS_TIMERS_BLOB_NS, &blob_path);
}

fn clear_system_timer_blob(store: &Store, agent_id: &AgentId, timer_id: &str) {
    let blob_path = sys_timer_blob_path(timer_id);
    let _ = store
        .blob_store()
        .delete_agent_file(agent_id, SYS_TIMERS_BLOB_NS, &blob_path);
}

pub(crate) fn arm_cloud_poll_timer(
    store: &Store,
    backend: &dyn TimerBackend,
    agent_id: &AgentId,
    turn_id: &str,
    fire_at_ms: i64,
) {
    let tid = cloud_poll_timer_id(turn_id);
    if let Err(err) = arm_system_timer(store, backend, agent_id, &tid, fire_at_ms) {
        eprintln!("anyagents-timer: failed to arm {tid}: {err}");
    }
}

pub(crate) fn cancel_cloud_poll_timer(
    store: &Store,
    backend: &dyn TimerBackend,
    agent_id: &AgentId,
    turn_id: &str,
) {
    cancel_system_timer(store, backend, agent_id, &cloud_poll_timer_id(turn_id));
}

pub(crate) fn arm_turn_timeout_timer(
    store: &Store,
    backend: &dyn TimerBackend,
    agent_id: &AgentId,
    turn_id: &str,
    fire_at_ms: i64,
) {
    let tid = turn_timeout_timer_id(turn_id);
    if let Err(err) = arm_system_timer(store, backend, agent_id, &tid, fire_at_ms) {
        eprintln!("anyagents-timer: failed to arm {tid}: {err}");
    }
}

pub(crate) fn cancel_turn_timeout_timer(
    store: &Store,
    backend: &dyn TimerBackend,
    agent_id: &AgentId,
    turn_id: &str,
) {
    cancel_system_timer(store, backend, agent_id, &turn_timeout_timer_id(turn_id));
}

pub(crate) fn lease_renew_fire_at_ms(expires_at_ms: Option<i64>, now: i64) -> i64 {
    let default_exp = now.saturating_add(DEFAULT_MAX_LEASE_TTL_MS as i64);
    let exp = expires_at_ms.unwrap_or(default_exp);
    exp.saturating_sub(DEFAULT_LEASE_RENEW_WINDOW_MS)
        .max(now.saturating_add(MIN_LEASE_RENEW_LEAD_MS))
}

pub(crate) fn arm_lease_renew_timer(
    store: &Store,
    backend: &dyn TimerBackend,
    agent_id: &AgentId,
    expires_at_ms: Option<i64>,
    now: i64,
) -> i64 {
    let renew_at = lease_renew_fire_at_ms(expires_at_ms, now);
    let tid = lease_renew_timer_id(agent_id);
    if let Err(err) = arm_system_timer(store, backend, agent_id, &tid, renew_at) {
        eprintln!("anyagents-timer: failed to arm {tid}: {err}");
    }
    renew_at
}

pub(crate) fn cancel_lease_renew_timer(
    store: &Store,
    backend: &dyn TimerBackend,
    agent_id: &AgentId,
) {
    cancel_system_timer(store, backend, agent_id, &lease_renew_timer_id(agent_id));
}

/// Read persisted transient dispatch retry & error state for `agent_id`.
pub(crate) fn read_dispatch_retry_state(
    store: &Store,
    agent_id: &AgentId,
) -> Option<DispatchRetryState> {
    store
        .blob_store()
        .get_agent_file(agent_id, SYS_TIMERS_BLOB_NS, DISPATCH_RETRY_BLOB_PATH)
        .ok()
        .flatten()
        .and_then(|entry| serde_json::from_slice::<DispatchRetryState>(&entry.content).ok())
}

fn write_dispatch_retry_state(store: &Store, agent_id: &AgentId, state: &DispatchRetryState) {
    if let Ok(bytes) = serde_json::to_vec(state) {
        let _ = store.blob_store().put_agent_file(
            agent_id,
            SYS_TIMERS_BLOB_NS,
            DISPATCH_RETRY_BLOB_PATH,
            &bytes,
            false,
        );
    }
}

/// Clear transient dispatch retry state and cancel any armed `sys:reconcile:<agent_id>` timer
/// (no-op on `backend` when no retry state or reconcile timer exists).
pub(crate) fn clear_dispatch_retry_state(
    store: &Store,
    backend: Option<&dyn TimerBackend>,
    agent_id: &AgentId,
) {
    let had_state = read_dispatch_retry_state(store, agent_id).is_some();
    if had_state {
        let _ = store.blob_store().delete_agent_file(
            agent_id,
            SYS_TIMERS_BLOB_NS,
            DISPATCH_RETRY_BLOB_PATH,
        );
    }
    let tid = reconcile_timer_id(agent_id);
    let timer_blob = sys_timer_blob_path(&tid);
    let had_timer = store
        .blob_store()
        .get_agent_file(agent_id, SYS_TIMERS_BLOB_NS, &timer_blob)
        .ok()
        .flatten()
        .is_some();
    if had_state || had_timer {
        if let Some(tb) = backend {
            cancel_system_timer(store, tb, agent_id, &tid);
        } else {
            clear_system_timer_blob(store, agent_id, &tid);
        }
    }
}

/// Record a transient serverless dispatch failure so `AgentView.last_error` surfaces it
/// visibly and arm a bounded idempotent backoff retry timer (`15s -> 30s -> 60s`, capped
/// at [`DISPATCH_RETRY_BACKOFF_MS.len()`] retries) when the agent still has undelivered
/// mailbox messages and no active remote turn.
pub(crate) fn record_dispatch_failure_and_arm_retry(
    store: &Store,
    backend: &dyn TimerBackend,
    agent_id: &AgentId,
    error: &str,
) -> DispatchRetryState {
    let Ok(agent) = store.agent(agent_id) else {
        clear_dispatch_retry_state(store, Some(backend), agent_id);
        return DispatchRetryState::default();
    };
    if matches!(agent.state.as_str(), "paused" | "archived") {
        clear_dispatch_retry_state(store, Some(backend), agent_id);
        return DispatchRetryState::default();
    }
    let has_active_turn = store
        .remote_turn_store()
        .list_active_for_agent(agent_id)
        .map(|turns| !turns.is_empty())
        .unwrap_or(false);
    if has_active_turn {
        return read_dispatch_retry_state(store, agent_id).unwrap_or_default();
    }

    let now = now_ms();
    let masked = super::secrets::mask_text_for_agent(store, agent_id, error);
    let trimmed_err = masked.trim().to_string();
    let target_seq = store
        .list_undelivered(agent_id, 20)
        .ok()
        .and_then(|msgs| msgs.last().map(|m| m.seq));
    let prev = read_dispatch_retry_state(store, agent_id);
    let prev_attempts = match &prev {
        Some(st) if st.target_seq == target_seq => st.attempts,
        _ => 0,
    };

    let terminal_bootstrap = super::serverless_failure::is_terminal_bootstrap_failure(&trimmed_err);
    let can_retry = target_seq.is_some()
        && !terminal_bootstrap
        && (prev_attempts as usize) < DISPATCH_RETRY_BACKOFF_MS.len();

    let (attempts, next_retry_at_ms) = if can_retry {
        let delay_ms = DISPATCH_RETRY_BACKOFF_MS[prev_attempts as usize];
        let fire_at = now.saturating_add(delay_ms);
        let tid = reconcile_timer_id(agent_id);
        if let Err(arm_err) = arm_system_timer(store, backend, agent_id, &tid, fire_at) {
            eprintln!("anyagents-timer: failed to arm {tid}: {arm_err}");
        }
        (prev_attempts + 1, Some(fire_at))
    } else {
        clear_system_timer_blob(store, agent_id, &reconcile_timer_id(agent_id));
        (prev_attempts, None)
    };

    let state = DispatchRetryState {
        attempts,
        target_seq,
        last_error: (!trimmed_err.is_empty()).then_some(trimmed_err),
        last_error_at_ms: now,
        next_retry_at_ms,
    };
    write_dispatch_retry_state(store, agent_id, &state);
    state
}

/// Cancel turn-scoped timers upon settlement, and cancel the agent's lease-renewal
/// timer when no remaining active remote turns or inflight children exist (`cancel-on-close`).
pub(crate) fn cancel_turn_timers_on_close(
    store: &Store,
    backend: &dyn TimerBackend,
    agent_id: &AgentId,
    turn_id: &str,
    is_cloud: bool,
    inflight_children: i32,
) {
    if is_cloud {
        cancel_cloud_poll_timer(store, backend, agent_id, turn_id);
    } else {
        cancel_turn_timeout_timer(store, backend, agent_id, turn_id);
    }
    let still_has_active_turn = store
        .remote_turn_store()
        .list_active_for_agent(agent_id)
        .map(|v| !v.is_empty())
        .unwrap_or(false);
    let still_has_children = inflight_children > 0
        || store
            .runtime_heartbeat(agent_id)
            .ok()
            .flatten()
            .is_some_and(|hb| hb.inflight_children > 0);
    if !still_has_active_turn && !still_has_children {
        cancel_lease_renew_timer(store, backend, agent_id);
    }
}

/// Cancel all system maintenance timers owned by `agent_id` (called on archive/delete).
pub(crate) fn cancel_all_agent_system_timers(
    store: &Store,
    backend: &dyn TimerBackend,
    agent_id: &AgentId,
    prior_active_turns: &[PendingRemoteTurn],
) {
    for turn in prior_active_turns {
        if turn.harness_kind == "sandbox" {
            cancel_turn_timeout_timer(store, backend, agent_id, &turn.turn_id);
        } else {
            cancel_cloud_poll_timer(store, backend, agent_id, &turn.turn_id);
        }
    }
    cancel_lease_renew_timer(store, backend, agent_id);
    cancel_system_timer(store, backend, agent_id, &reconcile_timer_id(agent_id));
    let _ = store.blob_store().delete_agent_file(
        agent_id,
        SYS_TIMERS_BLOB_NS,
        DISPATCH_RETRY_BLOB_PATH,
    );
}

// ============================================================================
// Step 2a: Event-driven cloud turn polling
// ============================================================================

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CloudPollOutcome {
    Skipped,
    Settled { settled_now: bool },
    StillRunning { next_at_ms: i64 },
}

/// Poll one active cloud turn (`2a`), settling if terminal or scheduling/re-arming
/// the next bounded poll if still running.
pub(crate) async fn poll_cloud_turn_once(
    store: Arc<Store>,
    timer_backend: Arc<dyn TimerBackend>,
    turn: PendingRemoteTurn,
    rearm_on_running: bool,
) -> Result<CloudPollOutcome, String> {
    let Some(ref remote_run_id) = turn.remote_run_id else {
        return Ok(CloudPollOutcome::Skipped);
    };
    let Ok(driver) =
        resolve_cloud_driver_from_metadata(&turn.harness_kind, turn.metadata_json.as_ref(), None)
    else {
        return Ok(CloudPollOutcome::Skipped);
    };
    let poll_res = tokio::time::timeout(
        Duration::from_secs(4),
        driver.poll_turn(remote_run_id, turn.cursor.as_deref()),
    )
    .await;

    match poll_res {
        Ok(Ok(polled)) if polled.status.is_terminal() => {
            let store_settle = Arc::clone(&store);
            let tb_close = Arc::clone(&timer_backend);
            let tid = turn.turn_id.clone();
            let aid = turn.agent_id.clone();
            let settled_now = tokio::task::spawn_blocking(move || -> Result<bool, String> {
                let res = settle_remote_turn_and_deliver(
                    &store_settle,
                    &tid,
                    SettleRemoteTurn {
                        status: polled.status.as_str().to_string(),
                        result_text: polled.result_text,
                        error_text: polled.error_text,
                        usage_json: polled.usage,
                        cursor: polled.cursor,
                        settled_at_ms: now_ms(),
                    },
                    None,
                    0,
                )?;
                cancel_turn_timers_on_close(&store_settle, tb_close.as_ref(), &aid, &tid, true, 0);
                Ok(res.is_some_and(|r| r.settled_now))
            })
            .await
            .map_err(|e| format!("cloud poll settle join error: {e}"))??;

            if settled_now && is_serverless_mode() {
                pump_agent_backlog_after_settlement(store, timer_backend, &turn.agent_id).await;
            }
            Ok(CloudPollOutcome::Settled { settled_now })
        }
        Ok(Ok(polled)) => {
            let next_delay = polled
                .next_poll_after_ms
                .map(|ms| i64::try_from(ms).unwrap_or(DEFAULT_CLOUD_POLL_DELAY_MS))
                .unwrap_or(DEFAULT_CLOUD_POLL_DELAY_MS)
                .clamp(DEFAULT_CLOUD_POLL_DELAY_MS, MAX_CLOUD_POLL_BACKOFF_MS);
            let next_at = now_ms().saturating_add(next_delay);
            let store_next = Arc::clone(&store);
            let tb_next = Arc::clone(&timer_backend);
            let tid = turn.turn_id.clone();
            let aid = turn.agent_id.clone();
            let cur = polled.cursor;
            let _ = tokio::task::spawn_blocking(move || {
                let _ = store_next.remote_turn_store().schedule_next_poll(
                    &tid,
                    Some(next_at),
                    cur.as_deref(),
                    now_ms(),
                );
                if rearm_on_running {
                    arm_cloud_poll_timer(&store_next, tb_next.as_ref(), &aid, &tid, next_at);
                }
            })
            .await;
            Ok(CloudPollOutcome::StillRunning {
                next_at_ms: next_at,
            })
        }
        Ok(Err(_)) | Err(_) => {
            let next_at = now_ms().saturating_add(DEFAULT_CLOUD_POLL_DELAY_MS * 2);
            let store_next = Arc::clone(&store);
            let tb_next = Arc::clone(&timer_backend);
            let tid = turn.turn_id.clone();
            let aid = turn.agent_id.clone();
            let _ = tokio::task::spawn_blocking(move || {
                let _ = store_next.remote_turn_store().schedule_next_poll(
                    &tid,
                    Some(next_at),
                    None,
                    now_ms(),
                );
                if rearm_on_running {
                    arm_cloud_poll_timer(&store_next, tb_next.as_ref(), &aid, &tid, next_at);
                }
            })
            .await;
            Ok(CloudPollOutcome::StillRunning {
                next_at_ms: next_at,
            })
        }
    }
}

async fn handle_cloud_poll_fire(
    store: Arc<Store>,
    timer_backend: Arc<dyn TimerBackend>,
    timer_id: &str,
    turn_id: &str,
) -> Result<FireResult, String> {
    let store_get = Arc::clone(&store);
    let tid = turn_id.to_string();
    let turn_opt = tokio::task::spawn_blocking(move || {
        store_get
            .remote_turn_store()
            .get_turn(&tid)
            .map_err(|e| format!("load cloud turn {tid}: {e}"))
    })
    .await
    .map_err(|e| format!("cloud poll load join error: {e}"))??;

    let Some(turn) = turn_opt else {
        return Ok(FireResult {
            fired: false,
            schedule_id: timer_id.to_string(),
            agent_id: None,
            next_run_ms: None,
            reason: Some("turn_not_found".to_string()),
        });
    };
    if !matches!(turn.status.as_str(), "dispatched" | "running" | "pending") {
        let store_clr = Arc::clone(&store);
        let aid = turn.agent_id.clone();
        let tid_str = timer_id.to_string();
        let _ = tokio::task::spawn_blocking(move || {
            clear_system_timer_blob(&store_clr, &aid, &tid_str);
        })
        .await;
        return Ok(FireResult {
            fired: false,
            schedule_id: timer_id.to_string(),
            agent_id: Some(turn.agent_id.as_str().to_string()),
            next_run_ms: None,
            reason: Some("turn_already_settled".to_string()),
        });
    }

    let aid_str = turn.agent_id.as_str().to_string();
    match poll_cloud_turn_once(store, timer_backend, turn, true).await? {
        CloudPollOutcome::Skipped => Ok(FireResult {
            fired: false,
            schedule_id: timer_id.to_string(),
            agent_id: Some(aid_str),
            next_run_ms: None,
            reason: Some("driver_unavailable".to_string()),
        }),
        CloudPollOutcome::Settled { settled_now } => Ok(FireResult {
            fired: settled_now,
            schedule_id: timer_id.to_string(),
            agent_id: Some(aid_str),
            next_run_ms: None,
            reason: (!settled_now).then(|| "already_settled".to_string()),
        }),
        CloudPollOutcome::StillRunning { next_at_ms } => Ok(FireResult {
            fired: true,
            schedule_id: timer_id.to_string(),
            agent_id: Some(aid_str),
            next_run_ms: Some(next_at_ms),
            reason: Some("still_running".to_string()),
        }),
    }
}

// ============================================================================
// Step 2b: Event-driven stale sandbox timeout settlement
// ============================================================================

async fn handle_turn_timeout_fire(
    store: Arc<Store>,
    timer_backend: Arc<dyn TimerBackend>,
    timer_id: &str,
    turn_id: &str,
) -> Result<FireResult, String> {
    let store_b = Arc::clone(&store);
    let tb_b = Arc::clone(&timer_backend);
    let tid = turn_id.to_string();
    let timer_id_owned = timer_id.to_string();

    let (result, drain_agent) =
        tokio::task::spawn_blocking(move || -> Result<(FireResult, Option<AgentId>), String> {
            let Some(turn) = store_b
                .remote_turn_store()
                .get_turn(&tid)
                .map_err(|e| format!("load sandbox turn {tid}: {e}"))?
            else {
                return Ok((
                    FireResult {
                        fired: false,
                        schedule_id: timer_id_owned,
                        agent_id: None,
                        next_run_ms: None,
                        reason: Some("turn_not_found".to_string()),
                    },
                    None,
                ));
            };
            if !matches!(turn.status.as_str(), "dispatched" | "running" | "pending") {
                clear_system_timer_blob(&store_b, &turn.agent_id, &timer_id_owned);
                return Ok((
                    FireResult {
                        fired: false,
                        schedule_id: timer_id_owned,
                        agent_id: Some(turn.agent_id.as_str().to_string()),
                        next_run_ms: None,
                        reason: Some("turn_already_settled".to_string()),
                    },
                    None,
                ));
            }
            let now = now_ms();
            let deadline = turn
                .last_event_at_ms
                .saturating_add(sandbox_stale_threshold_ms());
            if now < deadline {
                // Guest sent a fresh heartbeat after the original timer was armed: extend deadline.
                arm_turn_timeout_timer(&store_b, tb_b.as_ref(), &turn.agent_id, &tid, deadline);
                return Ok((
                    FireResult {
                        fired: false,
                        schedule_id: timer_id_owned,
                        agent_id: Some(turn.agent_id.as_str().to_string()),
                        next_run_ms: Some(deadline),
                        reason: Some("turn_still_fresh".to_string()),
                    },
                    None,
                ));
            }

            let settled =
                super::serverless_failure::settle_timed_out_sandbox_turn(&store_b, &turn, now);
            clear_system_timer_blob(&store_b, &turn.agent_id, &timer_id_owned);
            cancel_turn_timers_on_close(&store_b, tb_b.as_ref(), &turn.agent_id, &tid, false, 0);
            let _ = store_b.reconcile_stale_open_turn(&turn.agent_id);
            let should_drain = settled
                && super::serverless_failure::should_drain_after_failed_turn(&store_b, &turn);
            Ok((
                FireResult {
                    fired: settled,
                    schedule_id: timer_id_owned,
                    agent_id: Some(turn.agent_id.as_str().to_string()),
                    next_run_ms: None,
                    reason: (!settled).then(|| "already_settled".to_string()),
                },
                should_drain.then_some(turn.agent_id),
            ))
        })
        .await
        .map_err(|e| format!("turn timeout join error: {e}"))??;

    if let Some(aid) = drain_agent
        && is_serverless_mode()
    {
        pump_agent_backlog_after_settlement(store, timer_backend, &aid).await;
    }

    Ok(result)
}

// ============================================================================
// Step 3: Event-driven active sandbox lease renewal
// ============================================================================

/// Renew one ready sandbox binding's lease and persist the refreshed `lease_expires_at_ms`.
pub(crate) fn renew_binding_lease(store: &Store, binding: &SandboxBinding) -> bool {
    let cfg_dir = anyagents_core::agent_config_dir(store.data_dir(), &binding.agent_id);
    if crate::sandbox_providers::renew_lease_for_config_dir(&cfg_dir).is_ok_and(|renewed| renewed) {
        let mut updated = binding.clone();
        updated.lease_expires_at_ms =
            Some(now_ms().saturating_add(DEFAULT_MAX_LEASE_TTL_MS as i64));
        let _ = store.sandbox_binding_store().upsert(&updated);
        return true;
    }
    if binding.provider != "legacy_http"
        && binding.provider != "local"
        && let Ok(reg) = resolve_registry_for_binding(Some(store.data_dir()), binding)
        && reg.active_kind() != ProviderKind::Local
        && let Some(existing_handle) = sandbox_handle_from_binding(binding)
    {
        let mut req = reg.default_ensure_request(binding.agent_id.as_str());
        req.existing = Some(existing_handle);
        if let Ok(refreshed) = crate::sandbox_exec::block_on_provider(reg.active().ensure(&req)) {
            let mut updated = binding.clone();
            updated.lease_expires_at_ms = refreshed
                .expires_at
                .as_deref()
                .and_then(|s| s.parse::<jiff::Timestamp>().ok())
                .map(|ts| ts.as_millisecond())
                .or_else(|| Some(now_ms().saturating_add(DEFAULT_MAX_LEASE_TTL_MS as i64)));
            let _ = store.sandbox_binding_store().upsert(&updated);
            return true;
        }
    }
    false
}

pub(crate) fn binding_is_active(store: &Store, binding: &SandboxBinding) -> bool {
    let has_active_turn = store
        .remote_turn_store()
        .list_active_for_agent(&binding.agent_id)
        .map(|v| !v.is_empty())
        .unwrap_or(false);
    let has_inflight_children = store
        .runtime_heartbeat(&binding.agent_id)
        .ok()
        .flatten()
        .is_some_and(|hb| hb.inflight_children > 0);
    binding.idle_since_ms.is_none() || has_active_turn || has_inflight_children
}

async fn handle_lease_renew_fire(
    store: Arc<Store>,
    timer_backend: Arc<dyn TimerBackend>,
    timer_id: &str,
    agent_id_str: &str,
) -> Result<FireResult, String> {
    let Ok(aid) = AgentId::parse(agent_id_str) else {
        return Ok(FireResult {
            fired: false,
            schedule_id: timer_id.to_string(),
            agent_id: None,
            next_run_ms: None,
            reason: Some("invalid_agent_id".to_string()),
        });
    };
    let store_b = Arc::clone(&store);
    let tb_b = Arc::clone(&timer_backend);
    let timer_id_owned = timer_id.to_string();

    tokio::task::spawn_blocking(move || -> Result<FireResult, String> {
        let Some(binding) = store_b
            .sandbox_binding(&aid)
            .map_err(|e| format!("load sandbox binding {aid}: {e}"))?
        else {
            clear_system_timer_blob(&store_b, &aid, &timer_id_owned);
            return Ok(FireResult {
                fired: false,
                schedule_id: timer_id_owned,
                agent_id: Some(aid.as_str().to_string()),
                next_run_ms: None,
                reason: Some("no_active_sandbox".to_string()),
            });
        };
        if binding.lease_state != "ready" || !binding_is_active(&store_b, &binding) {
            // Idle or released tenant: stop renewing (zero wakes on idle tenants!).
            clear_system_timer_blob(&store_b, &aid, &timer_id_owned);
            return Ok(FireResult {
                fired: false,
                schedule_id: timer_id_owned,
                agent_id: Some(aid.as_str().to_string()),
                next_run_ms: None,
                reason: Some("sandbox_idle".to_string()),
            });
        }

        let renewed = renew_binding_lease(&store_b, &binding);
        let refreshed_exp = store_b
            .sandbox_binding(&aid)
            .ok()
            .flatten()
            .and_then(|b| b.lease_expires_at_ms)
            .or(binding.lease_expires_at_ms);
        let next_run_ms =
            arm_lease_renew_timer(&store_b, tb_b.as_ref(), &aid, refreshed_exp, now_ms());
        Ok(FireResult {
            fired: renewed,
            schedule_id: timer_id_owned,
            agent_id: Some(aid.as_str().to_string()),
            next_run_ms: Some(next_run_ms),
            reason: (!renewed).then(|| "renew_deferred".to_string()),
        })
    })
    .await
    .map_err(|e| format!("lease renew join error: {e}"))?
}

// ============================================================================
// Step 2c: Event-driven reconciliation, backlog pump, and startup recovery
// ============================================================================

/// Pump queued mailbox messages for `agent_id` after turn settlement.
/// Bounded retry arming & error recording are handled inside
/// [`dispatch_next_mailbox_message_with_timer`].
pub(crate) async fn pump_agent_backlog_after_settlement(
    store: Arc<Store>,
    timer_backend: Arc<dyn TimerBackend>,
    agent_id: &AgentId,
) {
    if let Err(err) = dispatch_next_mailbox_message_with_timer(
        Arc::clone(&store),
        agent_id,
        Some(Arc::clone(&timer_backend)),
    )
    .await
    {
        eprintln!(
            "anyagents-serverless: post-settlement backlog pump deferred for {agent_id}: {err}"
        );
    }
}

async fn handle_reconcile_fire(
    store: Arc<Store>,
    timer_backend: Arc<dyn TimerBackend>,
    timer_id: &str,
    agent_id_str: &str,
) -> Result<FireResult, String> {
    let Ok(aid) = AgentId::parse(agent_id_str) else {
        return Ok(FireResult {
            fired: false,
            schedule_id: timer_id.to_string(),
            agent_id: None,
            next_run_ms: None,
            reason: Some("invalid_agent_id".to_string()),
        });
    };
    let store_rec = Arc::clone(&store);
    let tb_rec = Arc::clone(&timer_backend);
    let aid_rec = aid.clone();
    let timer_id_clr = timer_id.to_string();
    let skip_reason =
        tokio::task::spawn_blocking(move || -> Result<Option<&'static str>, String> {
            clear_system_timer_blob(&store_rec, &aid_rec, &timer_id_clr);
            let Ok(agent) = store_rec.agent(&aid_rec) else {
                clear_dispatch_retry_state(&store_rec, Some(tb_rec.as_ref()), &aid_rec);
                return Ok(Some("agent_not_found"));
            };
            if matches!(agent.state.as_str(), "paused" | "archived") {
                clear_dispatch_retry_state(&store_rec, Some(tb_rec.as_ref()), &aid_rec);
                return Ok(Some("agent_paused_or_archived"));
            }
            let _ = store_rec.reconcile_stale_open_turn(&aid_rec);
            let msgs = store_rec
                .list_undelivered(&aid_rec, 1)
                .map_err(|e| format!("list undelivered for {aid_rec}: {e}"))?;
            if msgs.is_empty() {
                clear_dispatch_retry_state(&store_rec, Some(tb_rec.as_ref()), &aid_rec);
                return Ok(Some("no_undelivered_mail"));
            }
            Ok(None)
        })
        .await
        .map_err(|e| format!("reconcile join error: {e}"))??;

    if let Some(reason) = skip_reason {
        return Ok(FireResult {
            fired: false,
            schedule_id: timer_id.to_string(),
            agent_id: Some(aid.as_str().to_string()),
            next_run_ms: None,
            reason: Some(reason.to_string()),
        });
    }

    match dispatch_next_mailbox_message_with_timer(
        Arc::clone(&store),
        &aid,
        Some(Arc::clone(&timer_backend)),
    )
    .await
    {
        Ok(Some(_)) => Ok(FireResult {
            fired: true,
            schedule_id: timer_id.to_string(),
            agent_id: Some(aid.as_str().to_string()),
            next_run_ms: None,
            reason: None,
        }),
        Ok(None) => Ok(FireResult {
            fired: false,
            schedule_id: timer_id.to_string(),
            agent_id: Some(aid.as_str().to_string()),
            next_run_ms: None,
            reason: Some("agent_busy_or_idle_noop".to_string()),
        }),
        Err(err) => {
            eprintln!("anyagents-serverless: reconcile dispatch deferred for {aid}: {err}");
            let store_read = Arc::clone(&store);
            let aid_read = aid.clone();
            let next_at = tokio::task::spawn_blocking(move || {
                read_dispatch_retry_state(&store_read, &aid_read).and_then(|s| s.next_retry_at_ms)
            })
            .await
            .ok()
            .flatten();
            let reason = if next_at.is_some() {
                "dispatch_retry_armed"
            } else {
                "dispatch_retry_exhausted"
            };
            Ok(FireResult {
                fired: false,
                schedule_id: timer_id.to_string(),
                agent_id: Some(aid.as_str().to_string()),
                next_run_ms: next_at,
                reason: Some(reason.to_string()),
            })
        }
    }
}

/// Dispatch any `sys:*` event-driven maintenance timer arriving via `POST /v1/internal/fire/{id}`,
/// `POST /v1/internal/fire`, or `LocalTokioTimerBackend`.
pub(crate) async fn fire_system_timer(
    store: Arc<Store>,
    timer_backend: Arc<dyn TimerBackend>,
    timer_id: &str,
) -> Result<FireResult, String> {
    let clean = timer_id.trim();
    if let Some(turn_id) = clean.strip_prefix(SYS_CLOUD_POLL_PREFIX) {
        return handle_cloud_poll_fire(store, timer_backend, clean, turn_id).await;
    }
    if let Some(turn_id) = clean.strip_prefix(SYS_TURN_TIMEOUT_PREFIX) {
        return handle_turn_timeout_fire(store, timer_backend, clean, turn_id).await;
    }
    if let Some(agent_id) = clean.strip_prefix(SYS_LEASE_RENEW_PREFIX) {
        return handle_lease_renew_fire(store, timer_backend, clean, agent_id).await;
    }
    if let Some(agent_id) = clean.strip_prefix(SYS_RECONCILE_PREFIX) {
        return handle_reconcile_fire(store, timer_backend, clean, agent_id).await;
    }
    Ok(FireResult {
        fired: false,
        schedule_id: clean.to_string(),
        agent_id: None,
        next_run_ms: None,
        reason: Some("unknown_system_timer".to_string()),
    })
}

/// Reconcile stale serverless sandbox turns and orphaned `turn_recovery` open-turn
/// locks for a single agent before computing runtime cutover state.
///
/// In non-serverless mode this is a pure `store.has_open_turn(agent_id)` read.
/// In serverless mode, any stale sandbox turn (`last_event_at_ms <= now - threshold`)
/// is settled first, and if zero active remote turns remain the orphaned open-turn
/// lock is cleared so dead sandboxes never stall `cutover_status` forever.
pub(crate) fn reconcile_agent_open_turn_serverless(store: &Store, agent_id: &AgentId) -> bool {
    let mut open_turn = store.has_open_turn(agent_id).unwrap_or(false);
    if !open_turn || !is_serverless_mode() {
        return open_turn;
    }
    let now = now_ms();
    let stale_before = now.saturating_sub(sandbox_stale_threshold_ms());
    if let Ok(active_turns) = store.remote_turn_store().list_active_for_agent(agent_id) {
        for turn in &active_turns {
            if turn.harness_kind == "sandbox" && turn.last_event_at_ms <= stale_before {
                let _ = super::serverless_failure::settle_timed_out_sandbox_turn(store, turn, now);
            }
        }
        let no_live_remote_turns = store
            .remote_turn_store()
            .list_active_for_agent(agent_id)
            .is_ok_and(|turns| turns.is_empty());
        if no_live_remote_turns {
            let _ = store.reconcile_stale_open_turn(agent_id);
            open_turn = store.has_open_turn(agent_id).unwrap_or(false);
        }
    }
    open_turn
}

/// Summary of startup/cold-start serverless reconciliation & backlog drain (`2c`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct StartupReconcileReport {
    pub stale_open_turns_cleared: usize,
    pub stale_turns_settled: usize,
    pub timers_rearmed: usize,
    pub schedules_rearmed: usize,
    pub overdue_schedules_rearmed: usize,
    pub backlog_dispatched: usize,
}

/// Cold-start / startup reconciliation hook wired into `server/bootstrap.rs`:
/// 1. Settles already-stale sandbox turns (`2b`) and re-arms `sys:turn-timeout:*` /
///    `sys:cloud-poll:*` timers for still-live remote turns (`2a`, `2b`).
/// 2. Re-arms `sys:lease-renew:*` for active ready sandbox bindings (`3`).
/// 3. Rehydrates active user schedules (`every`, `at`, `watch`) onto `timer_backend`
///    when `!timer_backend.is_noop()`, including immediate catch-up for overdue rows.
/// 4. Reconciles orphaned `turn_recovery` open-turn locks and drains queued undelivered
///    mailbox messages for running idle agents (`2c`).
pub(crate) async fn startup_reconcile_serverless(
    store: Arc<Store>,
    timer_backend: Arc<dyn TimerBackend>,
) -> Result<StartupReconcileReport, String> {
    let store_b = Arc::clone(&store);
    let tb_b = Arc::clone(&timer_backend);
    let (mut report, pending_agents) = tokio::task::spawn_blocking(
        move || -> Result<(StartupReconcileReport, Vec<AgentId>), String> {
            let _lease = if store_b.is_postgres() {
                match store_b.try_acquire_coordinator_lease(
                    "maint:startup_reconcile",
                    Duration::from_secs(15),
                ) {
                    Ok(Some(guard)) => Some(guard),
                    Ok(None) => return Ok((StartupReconcileReport::default(), Vec::new())),
                    Err(_) => None,
                }
            } else {
                None
            };

            let now = now_ms();
            let stale_before = now.saturating_sub(sandbox_stale_threshold_ms());
            let mut report = StartupReconcileReport::default();
            let mut wake_agents: Vec<AgentId> = Vec::new();

            // 1. Settle already-stale sandbox turns and re-arm timers for live remote turns.
            if let Ok(stale) = store_b.remote_turn_store().list_stale_active(
                Some("sandbox"),
                stale_before,
                STARTUP_ACTIVE_TURN_LIMIT,
            ) {
                for turn in stale {
                    if super::serverless_failure::settle_timed_out_sandbox_turn(
                        &store_b, &turn, now,
                    ) {
                        report.stale_turns_settled += 1;
                        cancel_turn_timers_on_close(
                            &store_b,
                            tb_b.as_ref(),
                            &turn.agent_id,
                            &turn.turn_id,
                            false,
                            0,
                        );
                        if super::serverless_failure::should_drain_after_failed_turn(
                            &store_b, &turn,
                        ) && !wake_agents.contains(&turn.agent_id)
                        {
                            wake_agents.push(turn.agent_id);
                        }
                    }
                }
            }

            if let Ok(live_agents) = store_b.list_live_agents() {
                for agent in live_agents {
                    if let Ok(turns) = store_b.remote_turn_store().list_active_for_agent(&agent.id)
                    {
                        for turn in turns {
                            if turn.harness_kind == "sandbox" {
                                let deadline = turn
                                    .last_event_at_ms
                                    .saturating_add(sandbox_stale_threshold_ms());
                                arm_turn_timeout_timer(
                                    &store_b,
                                    tb_b.as_ref(),
                                    &turn.agent_id,
                                    &turn.turn_id,
                                    deadline,
                                );
                                report.timers_rearmed += 1;
                            } else {
                                let poll_at = turn.next_poll_at_ms.unwrap_or_else(|| {
                                    now.saturating_add(DEFAULT_CLOUD_POLL_DELAY_MS)
                                });
                                arm_cloud_poll_timer(
                                    &store_b,
                                    tb_b.as_ref(),
                                    &turn.agent_id,
                                    &turn.turn_id,
                                    poll_at,
                                );
                                report.timers_rearmed += 1;
                            }
                        }
                    }
                }
            }

            // 2. Re-arm lease renewal timers for active ready sandbox bindings.
            if let Ok(ready) = store_b.sandbox_binding_store().list_active_ready() {
                for binding in ready {
                    if binding_is_active(&store_b, &binding) {
                        arm_lease_renew_timer(
                            &store_b,
                            tb_b.as_ref(),
                            &binding.agent_id,
                            binding.lease_expires_at_ms,
                            now,
                        );
                        report.timers_rearmed += 1;
                    }
                }
            }

            // 3. Rehydrate active user schedules (`every`, `at`, `watch`) onto the active
            //    TimerBackend so coordinator deploys/restarts never strand existing schedules.
            if !tb_b.is_noop()
                && let Ok(active_schedules) = store_b.list_schedules(None, true)
            {
                let catchup_at = now.saturating_add(OVERDUE_SCHEDULE_CATCHUP_DELAY_MS);
                for sched in &active_schedules {
                    if sched.state != "active" {
                        continue;
                    }
                    let owner_archived_or_missing = match store_b.agent(&sched.agent_id) {
                        Ok(agent) => {
                            agent.state == "archived"
                                || super::is_explicitly_archived(store_b.data_dir(), &agent)
                        }
                        Err(anyagents_core::Error::AgentNotFound(_)) => true,
                        Err(_) => false,
                    };
                    if owner_archived_or_missing {
                        continue;
                    }
                    match sched.kind.as_str() {
                        "at" | "every" => {
                            let Some(due_ms) = sched.next_fire_at_ms else {
                                continue;
                            };
                            if due_ms <= now {
                                let fire_at = due_ms.max(catchup_at);
                                match crate::timer::arm_schedule_timer_at(
                                    &store_b,
                                    tb_b.as_ref(),
                                    sched,
                                    fire_at,
                                ) {
                                    Ok(_) => report.overdue_schedules_rearmed += 1,
                                    Err(err) => eprintln!(
                                        "anyagents-serverless: failed to re-arm overdue schedule {}: {err}",
                                        sched.id
                                    ),
                                }
                            } else {
                                match crate::timer::arm_schedule_timer(
                                    &store_b,
                                    tb_b.as_ref(),
                                    sched,
                                ) {
                                    Ok(_) => report.schedules_rearmed += 1,
                                    Err(err) => eprintln!(
                                        "anyagents-serverless: failed to re-arm schedule {}: {err}",
                                        sched.id
                                    ),
                                }
                            }
                        }
                        "watch" => {
                            if matches!(
                                crate::schedule::parse_watch_spec(&sched.spec),
                                Ok(crate::schedule::WatchTarget::Webhook { .. })
                            ) {
                                continue;
                            }
                            match sched.next_fire_at_ms {
                                Some(due_ms) if due_ms <= now => {
                                    let fire_at = due_ms.max(catchup_at);
                                    match crate::timer::arm_schedule_timer_at(
                                        &store_b,
                                        tb_b.as_ref(),
                                        sched,
                                        fire_at,
                                    ) {
                                        Ok(_) => report.overdue_schedules_rearmed += 1,
                                        Err(err) => eprintln!(
                                            "anyagents-serverless: failed to re-arm overdue watch {}: {err}",
                                            sched.id
                                        ),
                                    }
                                }
                                Some(_) => {
                                    match crate::timer::arm_schedule_timer(
                                        &store_b,
                                        tb_b.as_ref(),
                                        sched,
                                    ) {
                                        Ok(_) => report.schedules_rearmed += 1,
                                        Err(err) => eprintln!(
                                            "anyagents-serverless: failed to re-arm watch {}: {err}",
                                            sched.id
                                        ),
                                    }
                                }
                                None => {
                                    match crate::timer::arm_schedule_timer_at(
                                        &store_b,
                                        tb_b.as_ref(),
                                        sched,
                                        catchup_at,
                                    ) {
                                        Ok(_) => report.schedules_rearmed += 1,
                                        Err(err) => eprintln!(
                                            "anyagents-serverless: failed to re-arm watch probe {}: {err}",
                                            sched.id
                                        ),
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }

            // 4. Reconcile orphaned open turns and gather running agents with undelivered mail.
            report.stale_open_turns_cleared = if store_b.is_postgres() {
                store_b
                    .reconcile_all_stale_serverless_open_turns()
                    .unwrap_or(0)
            } else {
                let mut cleared = 0usize;
                if let Ok(live_agents) = store_b.list_live_agents() {
                    for agent in live_agents {
                        if agent.state != "running" {
                            continue;
                        }
                        let no_active = store_b
                            .remote_turn_store()
                            .list_active_for_agent(&agent.id)
                            .map(|v| v.is_empty())
                            .unwrap_or(false);
                        if no_active
                            && store_b.has_open_turn(&agent.id).unwrap_or(false)
                            && store_b
                                .reconcile_stale_open_turn(&agent.id)
                                .unwrap_or(false)
                        {
                            cleared += 1;
                        }
                    }
                }
                cleared
            };
            for aid in store_b
                .list_running_agents_with_undelivered(24)
                .unwrap_or_default()
            {
                if !wake_agents.contains(&aid) {
                    wake_agents.push(aid);
                }
            }
            Ok((report, wake_agents))
        },
    )
    .await
    .map_err(|e| format!("startup reconcile join error: {e}"))??;

    for aid in pending_agents {
        match dispatch_next_mailbox_message_with_timer(
            Arc::clone(&store),
            &aid,
            Some(Arc::clone(&timer_backend)),
        )
        .await
        {
            Ok(Some(_)) => {
                report.backlog_dispatched += 1;
            }
            Ok(None) => {}
            Err(err) => {
                eprintln!(
                    "anyagents-serverless: startup backlog dispatch deferred for {aid}: {err}"
                );
            }
        }
    }

    eprintln!(
        "anyagents-serverless: startup_reconcile complete backend={} schedules_rearmed={} overdue_schedules_rearmed={} timers_rearmed={} stale_turns_settled={} stale_open_turns_cleared={} backlog_dispatched={}",
        timer_backend.backend_kind(),
        report.schedules_rearmed,
        report.overdue_schedules_rearmed,
        report.timers_rearmed,
        report.stale_turns_settled,
        report.stale_open_turns_cleared,
        report.backlog_dispatched,
    );

    Ok(report)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use anyagents_core::stateless::{NewRemoteTurn, SandboxBinding, hash_callback_token};
    use anyagents_core::{Incoming, NewAgent, Store, now_ms};
    use anyagents_supervisor::HealthBoard;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use http_body_util::BodyExt as _;
    use serde_json::{Value, json};
    use tower::ServiceExt as _;

    use super::*;
    use crate::server::serverless::{ServerlessDispatchInput, dispatch_serverless_turn};
    use crate::server::types::AppState;

    #[derive(Default)]
    struct MockTimerRecorder {
        scheduled: Mutex<Vec<(String, i64)>>,
        cancelled: Mutex<Vec<(String, Option<String>)>>,
    }

    impl MockTimerRecorder {
        fn scheduled_ids(&self) -> Vec<String> {
            self.scheduled
                .lock()
                .unwrap()
                .iter()
                .map(|(id, _)| id.clone())
                .collect()
        }

        fn cancelled_entries(&self) -> Vec<(String, Option<String>)> {
            self.cancelled.lock().unwrap().clone()
        }
    }

    impl TimerBackend for MockTimerRecorder {
        fn schedule_one_shot(
            &self,
            schedule_id: &str,
            fire_at_ms: i64,
        ) -> Result<Option<String>, String> {
            self.scheduled
                .lock()
                .unwrap()
                .push((schedule_id.to_string(), fire_at_ms));
            Ok(Some(crate::timer::deterministic_run_id(
                schedule_id,
                fire_at_ms,
            )))
        }

        fn cancel(&self, schedule_id: &str, external_run_id: Option<&str>) -> Result<(), String> {
            self.cancelled
                .lock()
                .unwrap()
                .push((schedule_id.to_string(), external_run_id.map(str::to_string)));
            Ok(())
        }
    }

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

    fn make_state(
        store: Arc<Store>,
        data_dir: &Path,
        timer_backend: Arc<dyn TimerBackend>,
    ) -> AppState {
        AppState {
            store,
            health: HealthBoard::default(),
            sign_in: Arc::new(Mutex::new(crate::server::ProcessRunner)),
            auth: crate::auth::Mode::Trusted {
                data_dir: data_dir.to_path_buf(),
            },
            identity: Arc::new(crate::identity::ServingIdentity::capture()),
            profiles: Default::default(),
            timer_backend,
        }
    }

    /// Regression test for Step 2a: dispatching a cloud turn arms `sys:cloud-poll:<turn_id>`
    /// on `TimerBackend`; firing `/v1/internal/fire/{id}` polls the driver, re-arms while
    /// running, and stops re-arming once settled.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)]
    async fn event_driven_step_2a_cloud_turn_poll_arms_rearms_and_stops_on_settle() {
        let _env = crate::server::tests::env_lock();
        let _restore = EnvRestore::capture(&[
            "ANYAGENTS_SERVERLESS",
            "ANYAGENTS_TIMER_TOKEN",
            "ANYAGENTS_CLOUD_HTTP_URL",
            "ANYAGENTS_CLOUD_HTTP_TOKEN",
            "ANYAGENTS_PUBLIC_URL",
        ]);

        let poll_hits = Arc::new(AtomicUsize::new(0));
        let poll_hits_srv = Arc::clone(&poll_hits);
        let mock_cloud = Router::new()
            .route(
                "/turns",
                post(|| async {
                    Json(json!({
                        "run_id": "cloud_run_2a",
                        "status": "running",
                        "next_poll_after_ms": 5000
                    }))
                }),
            )
            .route(
                "/turns/cloud_run_2a",
                get(move || {
                    let n = poll_hits_srv.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if n == 0 {
                            Json(json!({
                                "status": "running",
                                "cursor": "cur-1",
                                "next_poll_after_ms": 5000
                            }))
                        } else {
                            Json(json!({
                                "status": "completed",
                                "result_text": "cloud 2a settled cleanly"
                            }))
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cloud_addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, mock_cloud).await;
        });

        unsafe {
            std::env::set_var("ANYAGENTS_SERVERLESS", "false");
            std::env::set_var("ANYAGENTS_TIMER_TOKEN", "maint-timer-secret-2a");
            std::env::set_var("ANYAGENTS_CLOUD_HTTP_URL", format!("http://{cloud_addr}"));
            std::env::set_var("ANYAGENTS_CLOUD_HTTP_TOKEN", "cloud-tok-2a");
            std::env::set_var("ANYAGENTS_PUBLIC_URL", "https://coord.example.test");
        }

        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let agent = store
            .create_agent(NewAgent::new("cloud-2a-agent", "http-cloud"))
            .unwrap();
        store.set_agent_running(&agent.id, "/work").unwrap();

        let recorder = Arc::new(MockTimerRecorder::default());
        let mut req = ServerlessDispatchInput::for_agent(agent.clone(), "run 2a task");
        req.timer_backend = Some(Arc::clone(&recorder) as Arc<dyn TimerBackend>);
        let dispatched = dispatch_serverless_turn(Arc::clone(&store), req)
            .await
            .unwrap();

        let expected_tid = cloud_poll_timer_id(&dispatched.turn.turn_id);
        assert_eq!(recorder.scheduled_ids(), vec![expected_tid.clone()]);

        let app = crate::server::build_public_router(make_state(
            Arc::clone(&store),
            dir.path(),
            Arc::clone(&recorder) as Arc<dyn TimerBackend>,
        ));

        // First fire: cloud driver returns still running -> re-arms next poll!
        let resp1 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/internal/fire/{expected_tid}"))
                    .header("authorization", "Bearer maint-timer-secret-2a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp1.status(), StatusCode::OK);
        let j1: Value =
            serde_json::from_slice(&resp1.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(j1["fired"], true);
        assert_eq!(j1["reason"], "still_running");
        assert!(j1["next_run_ms"].as_i64().is_some());
        assert_eq!(recorder.scheduled_ids().len(), 2);

        // Second fire: cloud driver returns completed -> settles and does NOT re-arm!
        let resp2 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/internal/fire/{expected_tid}"))
                    .header("authorization", "Bearer maint-timer-secret-2a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp2.status(), StatusCode::OK);
        let j2: Value =
            serde_json::from_slice(&resp2.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(j2["fired"], true);
        assert!(j2.get("next_run_ms").is_none());
        assert_eq!(recorder.scheduled_ids().len(), 2);

        let settled = store
            .remote_turn_store()
            .get_turn(&dispatched.turn.turn_id)
            .unwrap()
            .unwrap();
        assert_eq!(settled.status, "completed");
        assert_eq!(
            settled.result_text.as_deref(),
            Some("cloud 2a settled cleanly")
        );

        // Third replay fire: idempotent no-op, zero additional polls!
        let resp3 = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/internal/fire/{expected_tid}"))
                    .header("authorization", "Bearer maint-timer-secret-2a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp3.status(), StatusCode::OK);
        let j3: Value =
            serde_json::from_slice(&resp3.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(j3["fired"], false);
        assert_eq!(j3["reason"], "turn_already_settled");
        assert_eq!(poll_hits.load(Ordering::SeqCst), 2);
    }

    /// Regression test for Step 2b: normal guest completion cancels `sys:turn-timeout:<turn_id>`
    /// (and `sys:lease-renew:<agent_id>`); silent guest death triggers timeout settlement via
    /// `/v1/internal/fire/{id}` and unlocks subsequent undelivered backlog.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)]
    async fn event_driven_step_2b_sandbox_timeout_cancels_on_complete_and_settles_on_silent_death()
    {
        let _env = crate::server::tests::env_lock();
        let _restore = EnvRestore::capture(&[
            "ANYAGENTS_SERVERLESS",
            "ANYAGENTS_TIMER_TOKEN",
            "ANYAGENTS_SERVERLESS_SANDBOX_STALE_MS",
        ]);
        unsafe {
            std::env::set_var("ANYAGENTS_SERVERLESS", "false");
            std::env::set_var("ANYAGENTS_TIMER_TOKEN", "maint-timer-secret-2b");
            std::env::set_var("ANYAGENTS_SERVERLESS_SANDBOX_STALE_MS", "20");
        }

        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let agent = store
            .create_agent(NewAgent::new("sbx-2b-agent", "codex"))
            .unwrap();
        store.set_agent_running(&agent.id, "/work").unwrap();
        let recorder = Arc::new(MockTimerRecorder::default());
        let app = crate::server::build_public_router(make_state(
            Arc::clone(&store),
            dir.path(),
            Arc::clone(&recorder) as Arc<dyn TimerBackend>,
        ));

        // Part 1: Normal completion via `/v1/internal/guest/events` cancels timeout & lease timers!
        let cb_tok = "tok-2b-complete";
        store
            .remote_turn_store()
            .create_turn(&NewRemoteTurn {
                turn_id: "turn_2b_ok".into(),
                agent_id: agent.id.clone(),
                harness_kind: "sandbox".into(),
                provider_kind: Some("http".into()),
                remote_run_id: Some("pid:100".into()),
                callback_token_hash: hash_callback_token(cb_tok),
                cursor: None,
                status: "running".into(),
                metadata_json: None,
                next_poll_at_ms: None,
            })
            .unwrap();
        let timeout_ok_id = turn_timeout_timer_id("turn_2b_ok");
        let lease_id = lease_renew_timer_id(&agent.id);
        arm_turn_timeout_timer(
            &store,
            recorder.as_ref(),
            &agent.id,
            "turn_2b_ok",
            now_ms() + 1_000,
        );
        arm_lease_renew_timer(
            &store,
            recorder.as_ref(),
            &agent.id,
            Some(now_ms() + 600_000),
            now_ms(),
        );

        let complete_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/internal/guest/events")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {cb_tok}"))
                    .body(Body::from(
                        json!({
                            "event_type": "turn_complete",
                            "turn_id": "turn_2b_ok",
                            "result_text": "done cleanly"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(complete_resp.status(), StatusCode::OK);
        let cancelled = recorder.cancelled_entries();
        assert!(
            cancelled
                .iter()
                .any(|(id, ext)| id == &timeout_ok_id && ext.is_some()),
            "expected cancel-on-complete for {timeout_ok_id} with persisted ext_id, got {cancelled:?}"
        );
        assert!(
            cancelled
                .iter()
                .any(|(id, ext)| id == &lease_id && ext.is_some()),
            "expected cancel-on-close for {lease_id} with persisted ext_id, got {cancelled:?}"
        );

        // Part 2: Silent guest death -> firing `sys:turn-timeout:turn_2b_dead` settles stale turn!
        let anyagents_core::ReceiveOutcome::New(msg) =
            store.receive(&agent.id, Incoming::new("work 1")).unwrap()
        else {
            panic!("expected new message");
        };
        store.note_delivered(&agent.id, msg.seq).unwrap();
        store
            .remote_turn_store()
            .create_turn(&NewRemoteTurn {
                turn_id: "turn_2b_dead".into(),
                agent_id: agent.id.clone(),
                harness_kind: "sandbox".into(),
                provider_kind: Some("http".into()),
                remote_run_id: Some("pid:200".into()),
                callback_token_hash: hash_callback_token("tok-2b-dead"),
                cursor: None,
                status: "running".into(),
                metadata_json: Some(json!({ "message_seq": msg.seq })),
                next_poll_at_ms: None,
            })
            .unwrap();
        // Let the turn age past the 20ms stale threshold without any guest heartbeat.
        tokio::time::sleep(Duration::from_millis(40)).await;

        let timeout_dead_id = turn_timeout_timer_id("turn_2b_dead");
        let fire_resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/internal/fire")
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer maint-timer-secret-2b")
                    .body(Body::from(
                        json!({
                            "input": { "schedule_id": timeout_dead_id }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fire_resp.status(), StatusCode::OK);
        let j: Value =
            serde_json::from_slice(&fire_resp.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(j["fired"], true);
        let dead_after = store
            .remote_turn_store()
            .get_turn("turn_2b_dead")
            .unwrap()
            .unwrap();
        assert_eq!(dead_after.status, "failed");
        assert!(!store.has_open_turn(&agent.id).unwrap());
    }

    /// Regression test for Step 2c: cold-start `startup_reconcile_serverless` clears orphaned
    /// open turns, re-arms timers for live remote turns/leases, and dispatches undelivered mail.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)]
    async fn event_driven_step_2c_startup_reconcile_recovers_orphans_rearms_timers_and_pumps_backlog()
     {
        let _env = crate::server::tests::env_lock();
        let _restore = EnvRestore::capture(&[
            "ANYAGENTS_SERVERLESS",
            "ANYAGENTS_CLOUD_HTTP_URL",
            "ANYAGENTS_CLOUD_HTTP_TOKEN",
            "ANYAGENTS_PUBLIC_URL",
        ]);

        let mock_cloud = Router::new().route(
            "/turns",
            post(|| async {
                Json(json!({
                    "run_id": "backlog_run_2c",
                    "status": "running",
                    "next_poll_after_ms": 5000
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cloud_addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, mock_cloud).await;
        });

        unsafe {
            std::env::set_var("ANYAGENTS_SERVERLESS", "true");
            std::env::set_var("ANYAGENTS_CLOUD_HTTP_URL", format!("http://{cloud_addr}"));
            std::env::set_var("ANYAGENTS_CLOUD_HTTP_TOKEN", "tok-2c");
            std::env::set_var("ANYAGENTS_PUBLIC_URL", "https://coord.example.test");
        }

        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let agent = store
            .create_agent(NewAgent::new("backlog-2c-agent", "http-cloud"))
            .unwrap();
        store.set_agent_running(&agent.id, "/work").unwrap();

        // Simulate orphaned open turn (`has_open_turn == true` with 0 active remote turns)
        // plus a queued undelivered message waiting behind it!
        let anyagents_core::ReceiveOutcome::New(m1) = store
            .receive(&agent.id, Incoming::new("orphaned msg"))
            .unwrap()
        else {
            panic!("expected msg 1");
        };
        store.note_delivered(&agent.id, m1.seq).unwrap();
        assert!(store.has_open_turn(&agent.id).unwrap());
        store
            .receive(&agent.id, Incoming::new("queued backlog msg"))
            .unwrap();

        let recorder = Arc::new(MockTimerRecorder::default());
        let report = startup_reconcile_serverless(
            Arc::clone(&store),
            Arc::clone(&recorder) as Arc<dyn TimerBackend>,
        )
        .await
        .unwrap();

        assert_eq!(report.stale_open_turns_cleared, 1);
        assert_eq!(report.backlog_dispatched, 1);
        // Dispatching the backlog cloud turn also armed its `sys:cloud-poll:*` timer!
        assert!(
            recorder
                .scheduled_ids()
                .iter()
                .any(|id| id.starts_with(SYS_CLOUD_POLL_PREFIX)),
            "expected cloud-poll timer armed after startup backlog pump, got {:?}",
            recorder.scheduled_ids()
        );
    }

    /// Regression test for Step 3: firing `sys:lease-renew:<agent_id>` renews an active
    /// sandbox lease and re-arms; once the turn closes (sandbox idle), firing does NOT
    /// renew or re-arm (zero wakes on idle tenants).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)]
    async fn event_driven_step_3_active_sandbox_lease_renews_and_stops_when_idle() {
        let _env = crate::server::tests::env_lock();
        let _restore = EnvRestore::capture(&[
            "ANYAGENTS_SERVERLESS",
            "ANYAGENTS_TIMER_TOKEN",
            "ANYAGENTS_SANDBOX_PROVIDER",
            "ANYAGENTS_SANDBOX_HTTP_URL",
            "ANYAGENTS_SANDBOX_ENDPOINT",
        ]);

        let ensure_hits = Arc::new(AtomicUsize::new(0));
        let ensure_hits_srv = Arc::clone(&ensure_hits);
        let refreshed_exp = jiff::Timestamp::from_millisecond(now_ms() + 3_600_000)
            .unwrap()
            .to_string();
        let refreshed_exp_srv = refreshed_exp.clone();
        let mock_oci = Router::new().route(
            "/v1/sandboxes/ensure",
            post(move || {
                ensure_hits_srv.fetch_add(1, Ordering::SeqCst);
                let exp = refreshed_exp_srv.clone();
                async move {
                    Json(json!({
                        "sandbox_id": "sbx_step3_1",
                        "volume_id": "vol_step3_1",
                        "expires_at": exp,
                    }))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let oci_addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, mock_oci).await;
        });

        unsafe {
            std::env::set_var("ANYAGENTS_SERVERLESS", "false");
            std::env::set_var("ANYAGENTS_TIMER_TOKEN", "maint-timer-secret-3");
            std::env::set_var("ANYAGENTS_SANDBOX_PROVIDER", "http");
            std::env::set_var("ANYAGENTS_SANDBOX_HTTP_URL", format!("http://{oci_addr}"));
            std::env::remove_var("ANYAGENTS_SANDBOX_ENDPOINT");
        }

        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let agent = store
            .create_agent(NewAgent::new("lease-3-agent", "codex"))
            .unwrap();
        store.set_agent_running(&agent.id, "/work").unwrap();

        let now = now_ms();
        store
            .sandbox_binding_store()
            .upsert(&SandboxBinding {
                agent_id: agent.id.clone(),
                provider: "http".into(),
                image: None,
                template: None,
                remote_workdir: "/home/user/agent-data/workspace".into(),
                spec_json: "{}".into(),
                volume_id: "vol_step3_1".into(),
                volume_resource: String::new(),
                volume_size_gib: None,
                lease_id: Some("sbx_step3_1".into()),
                lease_resource: None,
                data_plane_resource: None,
                guest_endpoint: None,
                transport_url: None,
                transport_token: None,
                lease_state: "ready".into(),
                lease_expires_at_ms: Some(now + 60_000),
                idle_since_ms: None,
                idle_ttl_ms: 300_000,
                last_heartbeat_ms: Some(now),
                bundle_manifest_json: "{}".into(),
                generation: 1,
                updated_at_ms: now,
            })
            .unwrap();
        store
            .remote_turn_store()
            .create_turn(&NewRemoteTurn {
                turn_id: "turn_3_active".into(),
                agent_id: agent.id.clone(),
                harness_kind: "sandbox".into(),
                provider_kind: Some("http".into()),
                remote_run_id: Some("pid:300".into()),
                callback_token_hash: hash_callback_token("tok-3"),
                cursor: None,
                status: "running".into(),
                metadata_json: None,
                next_poll_at_ms: None,
            })
            .unwrap();

        let recorder = Arc::new(MockTimerRecorder::default());
        let app = crate::server::build_public_router(make_state(
            Arc::clone(&store),
            dir.path(),
            Arc::clone(&recorder) as Arc<dyn TimerBackend>,
        ));

        let lease_tid = lease_renew_timer_id(&agent.id);
        let resp1 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/internal/fire/{lease_tid}"))
                    .header("authorization", "Bearer maint-timer-secret-3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp1.status(), StatusCode::OK);
        let j1: Value =
            serde_json::from_slice(&resp1.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(j1["fired"], true);
        assert!(j1["next_run_ms"].as_i64().is_some());
        assert_eq!(ensure_hits.load(Ordering::SeqCst), 1);
        assert_eq!(recorder.scheduled_ids(), vec![lease_tid.clone()]);
        let updated_binding = store.sandbox_binding(&agent.id).unwrap().unwrap();
        assert!(updated_binding.lease_expires_at_ms.unwrap() > now + 1_800_000);

        // Close the turn & mark sandbox idle -> next fire must NOT renew or re-arm!
        store
            .remote_turn_store()
            .settle_turn(
                "turn_3_active",
                &SettleRemoteTurn {
                    status: "completed".into(),
                    result_text: Some("done".into()),
                    error_text: None,
                    usage_json: None,
                    cursor: None,
                    settled_at_ms: now_ms(),
                },
            )
            .unwrap();
        let mut idle_binding = updated_binding;
        idle_binding.idle_since_ms = Some(now_ms());
        store.sandbox_binding_store().upsert(&idle_binding).unwrap();

        let resp2 = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/internal/fire/{lease_tid}"))
                    .header("authorization", "Bearer maint-timer-secret-3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp2.status(), StatusCode::OK);
        let j2: Value =
            serde_json::from_slice(&resp2.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(j2["fired"], false);
        assert_eq!(j2["reason"], "sandbox_idle");
        assert!(j2.get("next_run_ms").is_none());
        assert_eq!(ensure_hits.load(Ordering::SeqCst), 1);
        assert_eq!(recorder.scheduled_ids().len(), 1);
    }

    /// Regression test: `startup_reconcile_serverless` re-arms active future `every`/`at`
    /// schedules, re-arms overdue (`next_fire_at_ms <= now`) schedules at `>= now + 1000`
    /// so external delayed-run schedulers never reject past timestamps, re-arms pollable
    /// watches while skipping push-only webhooks, skips cancelled/fired/paused schedules,
    /// and does not arm anything when `TimerBackend::is_noop()` is true.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_reconcile_rehydrates_future_overdue_and_watch_schedules_and_skips_inactive() {
        use anyagents_core::NewSchedule;

        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let agent = store
            .create_agent(NewAgent::new("sched-rehydrate-agent", "codex"))
            .unwrap();
        store.set_agent_running(&agent.id, "/work").unwrap();
        let peer = store
            .create_agent(NewAgent::new("watch-target-peer", "codex"))
            .unwrap();
        store.set_agent_running(&peer.id, "/work").unwrap();

        let now = now_ms();
        let future_every_due = now + 3_600_000;
        let future_at_due = now + 600_000;
        let overdue_every_due = now - 60_000;
        let overdue_at_due = now - 30_000;

        let future_every = store
            .create_schedule(NewSchedule {
                agent_id: agent.id.clone(),
                kind: "every".into(),
                spec: "1h".into(),
                prompt: "future hourly patrol".into(),
                interval_ms: Some(3_600_000),
                next_fire_at_ms: Some(future_every_due),
                watch_baseline: None,
            })
            .unwrap();
        let future_at = store
            .create_schedule(NewSchedule {
                agent_id: agent.id.clone(),
                kind: "at".into(),
                spec: "10m".into(),
                prompt: "future reminder".into(),
                interval_ms: None,
                next_fire_at_ms: Some(future_at_due),
                watch_baseline: None,
            })
            .unwrap();
        let overdue_every = store
            .create_schedule(NewSchedule {
                agent_id: agent.id.clone(),
                kind: "every".into(),
                spec: "5m".into(),
                prompt: "overdue recurring patrol".into(),
                interval_ms: Some(300_000),
                next_fire_at_ms: Some(overdue_every_due),
                watch_baseline: None,
            })
            .unwrap();
        let overdue_at = store
            .create_schedule(NewSchedule {
                agent_id: agent.id.clone(),
                kind: "at".into(),
                spec: "1s".into(),
                prompt: "overdue one-shot reminder".into(),
                interval_ms: None,
                next_fire_at_ms: Some(overdue_at_due),
                watch_baseline: None,
            })
            .unwrap();
        let active_watch = store
            .create_schedule(NewSchedule {
                agent_id: agent.id.clone(),
                kind: "watch".into(),
                spec: format!("agent:idle:{}", peer.name),
                prompt: "peer went idle".into(),
                interval_ms: None,
                next_fire_at_ms: None,
                watch_baseline: None,
            })
            .unwrap();
        let webhook_watch = store
            .create_schedule(NewSchedule {
                agent_id: agent.id.clone(),
                kind: "watch".into(),
                spec: "webhook:deploy".into(),
                prompt: "push webhook event".into(),
                interval_ms: None,
                next_fire_at_ms: None,
                watch_baseline: None,
            })
            .unwrap();

        // Inactive schedules that MUST be skipped: cancelled, fired, paused.
        let cancelled = store
            .create_schedule(NewSchedule {
                agent_id: agent.id.clone(),
                kind: "every".into(),
                spec: "15m".into(),
                prompt: "cancelled schedule".into(),
                interval_ms: Some(900_000),
                next_fire_at_ms: Some(now + 900_000),
                watch_baseline: None,
            })
            .unwrap();
        store
            .cancel_schedule(Some(&agent.id), &cancelled.id)
            .unwrap();

        let fired = store
            .create_schedule(NewSchedule {
                agent_id: agent.id.clone(),
                kind: "at".into(),
                spec: "1m".into(),
                prompt: "already fired schedule".into(),
                interval_ms: None,
                next_fire_at_ms: Some(now + 60_000),
                watch_baseline: None,
            })
            .unwrap();
        assert!(
            store
                .claim_schedule_tick(
                    &fired.id,
                    fired.next_fire_at_ms,
                    "fired",
                    None,
                    Some(now),
                    None,
                    Some("fired"),
                )
                .unwrap()
        );

        let paused = store
            .create_schedule(NewSchedule {
                agent_id: agent.id.clone(),
                kind: "every".into(),
                spec: "30m".into(),
                prompt: "paused schedule".into(),
                interval_ms: Some(1_800_000),
                next_fire_at_ms: Some(now + 1_800_000),
                watch_baseline: None,
            })
            .unwrap();
        assert!(
            store
                .claim_schedule_tick(
                    &paused.id,
                    paused.next_fire_at_ms,
                    "paused",
                    paused.next_fire_at_ms,
                    None,
                    None,
                    Some("paused"),
                )
                .unwrap()
        );

        // 1. NoopTimerBackend skips rehydration cleanly.
        let noop_report =
            startup_reconcile_serverless(Arc::clone(&store), Arc::new(NoopTimerBackend))
                .await
                .unwrap();
        assert_eq!(noop_report.schedules_rearmed, 0);
        assert_eq!(noop_report.overdue_schedules_rearmed, 0);

        // 2. Active TimerBackend rehydrates future, overdue, and pollable watch schedules.
        let recorder = Arc::new(MockTimerRecorder::default());
        let before_reconcile = now_ms();
        let report = startup_reconcile_serverless(
            Arc::clone(&store),
            Arc::clone(&recorder) as Arc<dyn TimerBackend>,
        )
        .await
        .unwrap();

        assert_eq!(report.schedules_rearmed, 3);
        assert_eq!(report.overdue_schedules_rearmed, 2);

        let scheduled: std::collections::HashMap<String, i64> =
            recorder.scheduled.lock().unwrap().iter().cloned().collect();
        assert_eq!(
            scheduled.len(),
            5,
            "unexpected scheduled set: {scheduled:?}"
        );

        // Future schedules keep their exact persisted next_fire_at_ms (deterministic runId!).
        assert_eq!(scheduled.get(&future_every.id), Some(&future_every_due));
        assert_eq!(scheduled.get(&future_at.id), Some(&future_at_due));

        // Overdue schedules are scheduled at >= now + 1_000 while DB preserves original due time.
        let overdue_every_armed = *scheduled.get(&overdue_every.id).unwrap();
        let overdue_at_armed = *scheduled.get(&overdue_at.id).unwrap();
        assert!(
            overdue_every_armed >= before_reconcile + OVERDUE_SCHEDULE_CATCHUP_DELAY_MS,
            "expected overdue every armed >= {}, got {overdue_every_armed}",
            before_reconcile + OVERDUE_SCHEDULE_CATCHUP_DELAY_MS
        );
        assert!(
            overdue_at_armed >= before_reconcile + OVERDUE_SCHEDULE_CATCHUP_DELAY_MS,
            "expected overdue at armed >= {}, got {overdue_at_armed}",
            before_reconcile + OVERDUE_SCHEDULE_CATCHUP_DELAY_MS
        );
        assert_eq!(
            store
                .get_schedule(&overdue_at.id)
                .unwrap()
                .unwrap()
                .next_fire_at_ms,
            Some(overdue_at_due)
        );
        assert_eq!(
            store
                .get_schedule(&overdue_every.id)
                .unwrap()
                .unwrap()
                .next_fire_at_ms,
            Some(overdue_every_due)
        );

        // Pollable watch armed for immediate catch-up probe; push-only webhook watch skipped.
        let watch_armed = *scheduled.get(&active_watch.id).unwrap();
        assert!(watch_armed >= before_reconcile + OVERDUE_SCHEDULE_CATCHUP_DELAY_MS);
        assert!(!scheduled.contains_key(&webhook_watch.id));

        // Cancelled, fired, and paused schedules were skipped.
        assert!(!scheduled.contains_key(&cancelled.id));
        assert!(!scheduled.contains_key(&fired.id));
        assert!(!scheduled.contains_key(&paused.id));

        // End-to-end proof: firing the catch-up trigger for both overdue schedules succeeds!
        let health = HealthBoard::default();
        let fired_at = crate::timer::fire_schedule_by_id(
            &store,
            &health,
            recorder.as_ref(),
            &overdue_at.id,
            Some(overdue_at_armed),
            overdue_at_armed,
        )
        .unwrap();
        assert!(
            fired_at.fired,
            "overdue one-shot catch-up failed: {fired_at:?}"
        );
        assert_eq!(
            store.get_schedule(&overdue_at.id).unwrap().unwrap().state,
            "fired"
        );

        let fired_every = crate::timer::fire_schedule_by_id(
            &store,
            &health,
            recorder.as_ref(),
            &overdue_every.id,
            Some(overdue_every_armed),
            overdue_every_armed,
        )
        .unwrap();
        assert!(
            fired_every.fired,
            "overdue recurring catch-up failed: {fired_every:?}"
        );
        assert!(fired_every.next_run_ms.unwrap() > overdue_every_armed);
    }

    /// Regression test: when initial serverless cold-wake dispatch fails transiently on a queued
    /// message, the failure is surfaced in `AgentView.last_error`, bounded backoff retries
    /// (`15s -> 30s -> 60s`, capped at 3 attempts) are armed on `TimerBackend` (never an infinite
    /// hot loop), and once the provider recovers firing `sys:reconcile:<agent_id>` drains the
    /// undelivered queue and clears `last_error` + `retry_at_ms`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)]
    async fn cold_wake_transient_dispatch_failure_arms_bounded_backoff_and_recovers_queue() {
        let _env = crate::server::tests::env_lock();
        let _restore = EnvRestore::capture(&[
            "ANYAGENTS_SERVERLESS",
            "ANYAGENTS_TIMER_TOKEN",
            "ANYAGENTS_CLOUD_HTTP_URL",
            "ANYAGENTS_CLOUD_HTTP_TOKEN",
            "ANYAGENTS_PUBLIC_URL",
        ]);

        let submit_attempts = Arc::new(AtomicUsize::new(0));
        let fail_limit = Arc::new(AtomicUsize::new(4));
        let submit_attempts_srv = Arc::clone(&submit_attempts);
        let fail_limit_srv = Arc::clone(&fail_limit);
        let mock_cloud = Router::new().route(
            "/turns",
            post(move || {
                let n = submit_attempts_srv.fetch_add(1, Ordering::SeqCst);
                let limit = fail_limit_srv.load(Ordering::SeqCst);
                async move {
                    if n < limit {
                        (
                            StatusCode::SERVICE_UNAVAILABLE,
                            Json(json!({ "error": "transient capacity contention 503" })),
                        )
                    } else {
                        (
                            StatusCode::OK,
                            Json(json!({
                                "run_id": format!("recovered_run_{n}"),
                                "status": "running",
                                "next_poll_after_ms": 5000
                            })),
                        )
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cloud_addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, mock_cloud).await;
        });

        unsafe {
            std::env::set_var("ANYAGENTS_SERVERLESS", "true");
            std::env::set_var("ANYAGENTS_TIMER_TOKEN", "cold-wake-retry-secret");
            std::env::set_var("ANYAGENTS_CLOUD_HTTP_URL", format!("http://{cloud_addr}"));
            std::env::set_var("ANYAGENTS_CLOUD_HTTP_TOKEN", "cloud-tok-retry");
            std::env::set_var("ANYAGENTS_PUBLIC_URL", "https://coord.example.test");
        }

        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let health = HealthBoard::default();
        let agent = store
            .create_agent(NewAgent::new("cold-wake-worker", "http-cloud"))
            .unwrap();
        store.set_agent_running(&agent.id, "/work").unwrap();
        let agent = store.agent(&agent.id).unwrap();

        // Enqueue initial prompt (`seq = 1`).
        store
            .receive(&agent.id, Incoming::new("initial spawn prompt"))
            .unwrap();

        let recorder = Arc::new(MockTimerRecorder::default());
        let app = crate::server::build_public_router(make_state(
            Arc::clone(&store),
            dir.path(),
            Arc::clone(&recorder) as Arc<dyn TimerBackend>,
        ));
        let rec_tid = reconcile_timer_id(&agent.id);

        // Attempt 1 (initial cold-wake dispatch): fails transiently -> surfaces error & arms 15s retry.
        let t0 = now_ms();
        let err1 = dispatch_next_mailbox_message_with_timer(
            Arc::clone(&store),
            &agent.id,
            Some(Arc::clone(&recorder) as Arc<dyn TimerBackend>),
        )
        .await
        .unwrap_err();
        assert!(err1.contains("503"), "unexpected err1: {err1}");

        let view1 = crate::server::agents_roster::agent_view(&store, &health, &agent).unwrap();
        assert!(
            view1
                .last_error
                .as_deref()
                .is_some_and(|e| e.contains("503")),
            "expected transient dispatch error surfaced in AgentView.last_error, got {:?}",
            view1.last_error
        );
        assert_eq!(view1.turn_state, "errored");
        assert!(view1.retry_at_ms.is_some_and(|ms| ms >= t0 + 14_000));
        assert!(view1.oldest_undelivered_age_ms.is_some());

        let armed_1 = recorder.scheduled.lock().unwrap().clone();
        assert_eq!(armed_1.len(), 1);
        assert_eq!(armed_1[0].0, rec_tid);
        assert!(armed_1[0].1 >= t0 + DISPATCH_RETRY_BACKOFF_MS[0] - 1_000);

        // Attempt 2 (reconcile timer fire): still failing -> backs off to 30s.
        let t1 = now_ms();
        let fire2 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/internal/fire/{rec_tid}"))
                    .header("authorization", "Bearer cold-wake-retry-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fire2.status(), StatusCode::OK);
        let j2: Value =
            serde_json::from_slice(&fire2.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(j2["fired"], false);
        assert_eq!(j2["reason"], "dispatch_retry_armed");
        assert!(j2["next_run_ms"].as_i64().unwrap() >= t1 + DISPATCH_RETRY_BACKOFF_MS[1] - 1_000);

        // Attempt 3 (reconcile timer fire): still failing -> backs off to 60s.
        let t2 = now_ms();
        let fire3 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/internal/fire/{rec_tid}"))
                    .header("authorization", "Bearer cold-wake-retry-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let j3: Value =
            serde_json::from_slice(&fire3.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(j3["reason"], "dispatch_retry_armed");
        assert!(j3["next_run_ms"].as_i64().unwrap() >= t2 + DISPATCH_RETRY_BACKOFF_MS[2] - 1_000);
        assert_eq!(recorder.scheduled.lock().unwrap().len(), 3);

        // Attempt 4 (final retry fires while still failing): cap reached -> NO further timer armed!
        let fire4 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/internal/fire/{rec_tid}"))
                    .header("authorization", "Bearer cold-wake-retry-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let j4: Value =
            serde_json::from_slice(&fire4.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(j4["fired"], false);
        assert_eq!(j4["reason"], "dispatch_retry_exhausted");
        assert!(j4.get("next_run_ms").is_none());
        assert_eq!(recorder.scheduled.lock().unwrap().len(), 3);

        // Now provider recovers (4 failures reached); firing reconcile/wake drains queue and clears error!
        let fire5 = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/internal/fire/{rec_tid}"))
                    .header("authorization", "Bearer cold-wake-retry-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let j5: Value =
            serde_json::from_slice(&fire5.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(j5["fired"], true);
        assert!(store.list_undelivered(&agent.id, 10).unwrap().is_empty());
        assert!(read_dispatch_retry_state(&store, &agent.id).is_none());

        let recovered = crate::server::agents_roster::agent_view(&store, &health, &agent).unwrap();
        assert_eq!(recovered.process, "up");
        assert_eq!(recovered.turn_state, "in_turn");
        assert_eq!(recovered.last_error, None);
        assert_eq!(recovered.retry_at_ms, None);
    }

    /// Regression test: spawning an agent with an initial prompt via `create_agent_route` in
    /// serverless mode commits `seq=1` strictly before dispatching, surfaces transient dispatch
    /// failures in `AgentView.last_error` + `retry_at_ms`, arms `sys:reconcile:<agent_id>`,
    /// and recovers cleanly once the provider comes back up.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)]
    async fn spawn_agent_route_serverless_cold_wake_commits_prompt_and_arms_retry_on_failure() {
        let _env = crate::server::tests::env_lock();
        let _restore = EnvRestore::capture(&[
            "ANYAGENTS_SERVERLESS",
            "ANYAGENTS_TIMER_TOKEN",
            "ANYAGENTS_CLOUD_HTTP_URL",
            "ANYAGENTS_CLOUD_HTTP_TOKEN",
            "ANYAGENTS_PUBLIC_URL",
            "ANYAGENTS_SANDBOX_MODE",
        ]);

        let submit_hits = Arc::new(AtomicUsize::new(0));
        let submit_hits_srv = Arc::clone(&submit_hits);
        let mock_cloud = Router::new().route(
            "/turns",
            post(move || {
                let n = submit_hits_srv.fetch_add(1, Ordering::SeqCst);
                async move {
                    if n == 0 {
                        (
                            StatusCode::SERVICE_UNAVAILABLE,
                            Json(json!({ "error": "initial spawn lease 503" })),
                        )
                    } else {
                        (
                            StatusCode::OK,
                            Json(json!({
                                "run_id": "spawn_recovered_run",
                                "status": "running",
                                "next_poll_after_ms": 5000
                            })),
                        )
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cloud_addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, mock_cloud).await;
        });

        unsafe {
            std::env::set_var("ANYAGENTS_SERVERLESS", "true");
            std::env::set_var("ANYAGENTS_TIMER_TOKEN", "spawn-cold-wake-secret");
            std::env::set_var("ANYAGENTS_CLOUD_HTTP_URL", format!("http://{cloud_addr}"));
            std::env::set_var("ANYAGENTS_CLOUD_HTTP_TOKEN", "cloud-tok-spawn");
            std::env::set_var("ANYAGENTS_PUBLIC_URL", "https://coord.example.test");
            std::env::remove_var("ANYAGENTS_SANDBOX_MODE");
        }

        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        store
            .hold_capability_key("test-capability-key-0123456789abcdef")
            .unwrap();
        let recorder = Arc::new(MockTimerRecorder::default());
        let state = make_state(
            Arc::clone(&store),
            dir.path(),
            Arc::clone(&recorder) as Arc<dyn TimerBackend>,
        );
        let body: crate::api::CreateAgentRequest = serde_json::from_value(json!({
            "name": "spawn-cold-worker",
            "harness": "claude-cloud",
            "runtime": "cloud",
            "prompt": "execute initial task"
        }))
        .unwrap();

        let (status, Json(created_view)) = crate::server::agents::create_agent_route(
            crate::auth::Principal::Operator,
            axum::extract::State(state.clone()),
            axum::http::HeaderMap::new(),
            Json(body),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::CREATED);
        let aid = AgentId::parse(&created_view.id).unwrap();
        // Initial prompt (`seq=1`) is already committed synchronously before `201 Created`.
        let undelivered = store.list_undelivered(&aid, 10).unwrap();
        assert_eq!(undelivered.len(), 1);
        assert_eq!(undelivered[0].seq, 1);

        // Wait briefly for the spawned initial dispatch task to hit the transient 503 and arm retry.
        let rec_tid = reconcile_timer_id(&aid);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !recorder.scheduled_ids().contains(&rec_tid) {
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for spawn cold-wake retry timer; scheduled={:?}",
                recorder.scheduled_ids()
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let agent = store.agent(&aid).unwrap();
        let failed_view =
            crate::server::agents_roster::agent_view(&store, &state.health, &agent).unwrap();
        assert!(
            failed_view
                .last_error
                .as_deref()
                .is_some_and(|e| e.contains("503")),
            "expected spawn dispatch error in AgentView.last_error, got {:?}",
            failed_view.last_error
        );
        assert!(failed_view.retry_at_ms.is_some());

        // Firing the armed reconcile timer recovers and drains `seq=1`.
        let app = crate::server::build_public_router(state.clone());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/internal/fire/{rec_tid}"))
                    .header("authorization", "Bearer spawn-cold-wake-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body_json: Value =
            serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(body_json["fired"], true);
        assert!(store.list_undelivered(&aid, 10).unwrap().is_empty());
        let ok_view =
            crate::server::agents_roster::agent_view(&store, &state.health, &agent).unwrap();
        assert_eq!(ok_view.process, "up");
        assert_eq!(ok_view.turn_state, "in_turn");
        assert_eq!(ok_view.last_error, None);
    }
}
