//! Universal Serverless Native Mid-Turn Steering Engine (`sandbox` + `cloud` + `local`).
//!
//! Coordinates live mid-turn steering when inbound mailbox messages arrive for a
//! serverless agent with an active `dispatched` / `running` remote turn:
//! - **`runtime: sandbox` / `local`:** atomically writes
//!   `<state_dir>/turns/<turn_id>.steer.d/<seq:020>-<msg_id>.json` (temp+rename via
//!   `SandboxProvider::exec` stdin) into the active sandbox lease for harnesses with
//!   live mid-turn steering wired in `anyagents-guest`. Never advances `delivered_seq`
//!   until verified `steer_consumed` callback proof arrives at `/v1/internal/guest/events`.
//! - **`runtime: cloud`:** pushes live mid-turn steers via `CloudTurnDriver::steer_turn`
//!   (wired on `SpironServerlessDriver` and `GenericHttpCloudDriver`) and advances
//!   `delivered_seq` strictly on verified consumption acknowledgement (`CloudSteerAck::Consumed`
//!   or authenticated cloud `steer_consumed` webhook).
//! - **Fail-closed safety:** if the harness is `TurnEnd`-only (e.g. `gemini`), the
//!   sandbox lease is unreachable, the turn is settling, or the provider refuses the
//!   steer, `delivered_seq` is NEVER advanced so the message remains safely in the
//!   mailbox for turn-end backlog drain (zero dropped or duplicate messages).

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyagents_core::stateless::{PendingRemoteTurn, RemoteTurnEventUpdate};
use anyagents_core::{Agent, ExecutionRuntime, Message, Store, now_ms};
use anyagents_guest::steer::{STEER_DIR_SUFFIX, SteerEnvelope, steer_filename};
use anyagents_harness::{Delivery, DeliveryLevel, compose_message};
use axum::Json;
use axum::http::StatusCode;
use serde_json::{Value, json};

use crate::sandbox_exec::{resolve_registry_for_binding, sandbox_handle_from_binding};
use crate::sandbox_providers::providers::{
    ExecRequest, ProviderKind, SandboxHandle, SandboxProvider as AsyncSandboxProvider,
};
use crate::serverless_cloud::{
    CloudSteerAck, CloudTurnSteerRequest, resolve_cloud_driver_from_metadata,
};

use super::serverless::{GuestEventPayload, is_serverless_cloud_harness, resolve_public_url};
use super::types::ApiError;

pub(crate) const MAX_STEERED_SEQ_META_KEY: &str = "max_steered_seq";
const CONSUMED_STEER_SEQS_META_KEY: &str = "consumed_steer_seqs";
const STEER_EXEC_TIMEOUT_MS: u64 = 5_000;

/// Atomic POSIX shell write script: reads JSON bytes from stdin into a hidden
/// dotfile in the target directory (`0600`), then atomically renames into place
/// so `anyagents-guest` never observes a partial write.
const ATOMIC_STEER_WRITE_SCRIPT: &str = "\
set -eu
dir=\"$1\"
final=\"$2\"
tmp=\"$dir/.tmp.$$.$3\"
mkdir -p \"$dir\"
chmod 700 \"$dir\" 2>/dev/null || true
cat > \"$tmp\"
chmod 600 \"$tmp\" 2>/dev/null || true
mv -f \"$tmp\" \"$dir/$final\"
";

/// Serverless `cloud` Spiron native mid-turn delivery declaration.
pub(crate) const SPIRON_SERVERLESS_CLOUD_DELIVERY: Delivery = Delivery {
    level: DeliveryLevel::NextStep,
    idempotent: true,
    proof: Cow::Borrowed(
        "spiron serverless cloud mid-turn steer with verified consumption receipt",
    ),
    peer: None,
};

/// Return `true` ONLY for harnesses whose live sandbox/local mid-turn steering
/// transport is wired and E2E-verified through `anyagents-guest`.
///
/// Fail-closed: `gemini` (one-shot `gemini -p`) and unknown harnesses return `false`.
pub(crate) fn supports_serverless_sandbox_steer(harness: &str) -> bool {
    matches!(
        anyagents_harness::normalize_harness_name(harness),
        "agy" | "claude-code" | "codex" | "opencode" | "omp" | "muse" | "spiron"
    )
}

/// Return `true` ONLY for cloud harnesses whose live serverless mid-turn steering
/// transport is wired and E2E-verified.
pub(crate) fn supports_serverless_cloud_steer(harness: &str) -> bool {
    matches!(
        anyagents_harness::normalize_harness_name(harness),
        "spiron" | "spiron-cloud" | "spiron_cloud"
    )
}

/// Honest delivery semantics for serverless `sandbox`, `local`, and `cloud` agents:
/// reports `DeliveryLevel::NextStep` (`delivery_mode: "steer"`, `steer_support: "yes"`)
/// strictly for harnesses whose live mid-turn steering transport is wired and E2E-verified,
/// failing closed to `DeliveryLevel::TurnEnd` (`followup` / `no`) on any unwired harness.
pub(crate) fn declared_delivery_for_serverless_parts(
    harness: &str,
    runtime: &ExecutionRuntime,
) -> Delivery {
    if runtime.is_cloud() || is_serverless_cloud_harness(harness) {
        if supports_serverless_cloud_steer(harness) {
            return SPIRON_SERVERLESS_CLOUD_DELIVERY;
        }
        return anyagents_harness_claude::CLOUD_DELIVERY;
    }
    match anyagents_harness::normalize_harness_name(harness) {
        "agy" => anyagents_harness_agy::DELIVERY,
        "claude-code" => anyagents_harness_claude::DELIVERY,
        "codex" => anyagents_harness_codex::DELIVERY,
        "opencode" => anyagents_harness_opencode::DELIVERY,
        "omp" => anyagents_harness_omp::DELIVERY,
        "muse" => anyagents_harness_muse::DELIVERY,
        "spiron" => anyagents_harness_spiron::DELIVERY,
        _ => anyagents_harness_stream::LINE_PER_TURN,
    }
}

/// Honest delivery semantics for a serverless [`Agent`].
pub(crate) fn declared_delivery_for_serverless_agent(agent: &Agent) -> Delivery {
    declared_delivery_for_serverless_parts(&agent.harness, &agent.runtime)
}

/// Outcome of attempting to steer inbound messages into an active serverless remote turn.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SteerPushOutcome {
    /// No new undelivered messages past the turn's opener / already-consumed steers.
    NoEligibleMessages,
    /// Atomically wrote `count` steer files into `<state_dir>/turns/<turn_id>.steer.d/`.
    PushedSandbox { count: usize },
    /// Pushed mid-turn steers to an active cloud turn.
    PushedCloud {
        consumed_count: usize,
        accepted_count: usize,
    },
    /// The previously active turn settled concurrently; caller should dispatch a new turn.
    TurnSettledRace,
    /// Fail-closed: this harness does not support live mid-turn steering (`TurnEnd`).
    Unsupported,
    /// Fail-closed: active lease/driver refused or failed steer; messages stay undelivered.
    Refused { reason: String },
}

/// Attempt to deliver new undelivered mailbox messages mid-turn into `active_turn`.
///
/// Guarantees fail-closed safety: never advances `delivered_seq` unless a verified
/// consumption proof is received (either synchronously from a cloud driver or via
/// authenticated `steer_consumed` callback from `anyagents-guest`).
pub(crate) async fn try_steer_active_turn(
    store: Arc<Store>,
    agent: Agent,
    active_turn: PendingRemoteTurn,
    undelivered: Vec<Message>,
    sandbox_provider: Option<Arc<dyn AsyncSandboxProvider>>,
    callback_base_url: Option<String>,
) -> SteerPushOutcome {
    let opener_seq = active_turn
        .metadata_json
        .as_ref()
        .and_then(|m| m.get("message_seq").and_then(Value::as_i64))
        .unwrap_or(0);
    let already_consumed = consumed_seqs_from_meta(active_turn.metadata_json.as_ref());
    let eligible: Vec<Message> = undelivered
        .into_iter()
        .filter(|m| m.seq > opener_seq && !already_consumed.contains(&m.seq))
        .collect();
    if eligible.is_empty() {
        return SteerPushOutcome::NoEligibleMessages;
    }

    if active_turn.harness_kind == "sandbox" {
        steer_active_sandbox_turn(store, agent, active_turn, eligible, sandbox_provider).await
    } else {
        steer_active_cloud_turn(store, agent, active_turn, eligible, callback_base_url).await
    }
}

async fn steer_active_sandbox_turn(
    store: Arc<Store>,
    agent: Agent,
    active_turn: PendingRemoteTurn,
    eligible: Vec<Message>,
    explicit_provider: Option<Arc<dyn AsyncSandboxProvider>>,
) -> SteerPushOutcome {
    if !supports_serverless_sandbox_steer(&agent.harness) {
        return SteerPushOutcome::Unsupported;
    }

    // Re-verify the turn is still active and resolve the live sandbox lease/provider.
    let store_r = Arc::clone(&store);
    let aid = agent.id.clone();
    let tid = active_turn.turn_id.clone();
    let meta_state_dir = active_turn
        .metadata_json
        .as_ref()
        .and_then(|m| m.get("state_dir").and_then(Value::as_str))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let meta_workdir = active_turn
        .metadata_json
        .as_ref()
        .and_then(|m| m.get("workdir").and_then(Value::as_str))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    type ResolvedSteerTarget = (Option<Arc<dyn AsyncSandboxProvider>>, SandboxHandle);
    let resolved =
        tokio::task::spawn_blocking(move || -> Result<Option<ResolvedSteerTarget>, String> {
            let Some(latest) = store_r
                .remote_turn_store()
                .get_turn(&tid)
                .map_err(|e| e.to_string())?
            else {
                return Ok(None);
            };
            if latest.is_terminal() {
                return Ok(None);
            }
            let binding = store_r.sandbox_binding(&aid).ok().flatten();
            if let Some(binding) = binding
                && binding.lease_state == "ready"
                && binding.provider != "legacy_http"
                && let Some(mut handle) = sandbox_handle_from_binding(&binding)
            {
                if let Some(ref sd) = meta_state_dir {
                    handle.state_dir = sd.clone();
                }
                if let Some(ref wd) = meta_workdir {
                    handle.workdir = wd.clone();
                }
                let provider = resolve_registry_for_binding(&store_r, &binding)
                    .ok()
                    .map(|reg| reg.active());
                return Ok(Some((provider, handle)));
            }
            if let Some(sd) = meta_state_dir {
                let wd = meta_workdir.unwrap_or_else(|| sd.clone());
                let handle = SandboxHandle {
                    provider: ProviderKind::Local,
                    sandbox_id: format!("active-{}", aid.as_str()),
                    volume_id: None,
                    mount_path: sd.clone(),
                    state_dir: sd,
                    workdir: wd,
                    transport_url: None,
                    transport_token: None,
                    expires_at: None,
                    metadata: BTreeMap::new(),
                };
                return Ok(Some((None, handle)));
            }
            Err("no active ready sandbox lease for mid-turn steer".to_string())
        })
        .await;

    let (resolved_provider, handle) = match resolved {
        Ok(Ok(Some(pair))) => pair,
        Ok(Ok(None)) => return SteerPushOutcome::TurnSettledRace,
        Ok(Err(reason)) => return SteerPushOutcome::Refused { reason },
        Err(err) => {
            return SteerPushOutcome::Refused {
                reason: format!("steer target resolution join error: {err}"),
            };
        }
    };

    let Some(provider) = explicit_provider.or(resolved_provider) else {
        return SteerPushOutcome::Refused {
            reason: "sandbox provider unavailable for mid-turn steer".to_string(),
        };
    };

    if validate_safe_turn_id(&active_turn.turn_id).is_err() {
        return SteerPushOutcome::Refused {
            reason: format!("invalid turn_id `{}`", active_turn.turn_id),
        };
    }

    let steer_dir = format!(
        "{}/turns/{}{STEER_DIR_SUFFIX}",
        handle.state_dir.trim_end_matches('/'),
        active_turn.turn_id
    );
    static NONCE_SEQ: AtomicU64 = AtomicU64::new(0);
    let mut pushed = 0usize;
    for msg in &eligible {
        let (prompt, mut outbound) = super::serverless_envelope::build_serverless_outbound_message(
            std::slice::from_ref(msg),
        );
        outbound.text = super::sandbox_media::hydrate_prompt(
            &store,
            &agent.id,
            provider.as_ref(),
            &handle,
            &prompt,
        )
        .await;
        let env = SteerEnvelope {
            turn_id: active_turn.turn_id.clone(),
            message_id: msg.id.to_string(),
            seq: msg.seq,
            outbound,
            submitted_at_ms: Some(now_ms()),
        };
        let bytes = match serde_json::to_vec(&env) {
            Ok(bytes) => bytes,
            Err(err) => {
                return SteerPushOutcome::Refused {
                    reason: format!("steer envelope serialization failed: {err}"),
                };
            }
        };
        let filename = steer_filename(msg.seq, msg.id.as_str());
        let nonce = format!("{}-{}", now_ms(), NONCE_SEQ.fetch_add(1, Ordering::Relaxed));
        let req = ExecRequest::argv([
            "/bin/sh".to_string(),
            "-c".to_string(),
            ATOMIC_STEER_WRITE_SCRIPT.to_string(),
            "_".to_string(),
            steer_dir.clone(),
            filename,
            nonce,
        ])
        .with_timeout_ms(STEER_EXEC_TIMEOUT_MS)
        .with_stdin(bytes);

        match provider.exec(&handle, &req).await {
            Ok(out) if out.exit_code == 0 => {
                pushed += 1;
            }
            Ok(out) => {
                return SteerPushOutcome::Refused {
                    reason: format!(
                        "sandbox steer write failed (exit {}): {}",
                        out.exit_code,
                        out.stderr.trim()
                    ),
                };
            }
            Err(err) => {
                return SteerPushOutcome::Refused {
                    reason: format!("sandbox steer exec failed: {err}"),
                };
            }
        }
    }

    // Re-check whether the turn settled right as we wrote the steer file.
    let store_chk = Arc::clone(&store);
    let tid_chk = active_turn.turn_id.clone();
    let settled_race = tokio::task::spawn_blocking(move || {
        store_chk
            .remote_turn_store()
            .get_turn(&tid_chk)
            .ok()
            .flatten()
            .is_some_and(|t| t.is_terminal())
    })
    .await
    .unwrap_or(false);
    if settled_race {
        return SteerPushOutcome::TurnSettledRace;
    }

    SteerPushOutcome::PushedSandbox { count: pushed }
}

async fn steer_active_cloud_turn(
    store: Arc<Store>,
    agent: Agent,
    active_turn: PendingRemoteTurn,
    eligible: Vec<Message>,
    callback_base_url: Option<String>,
) -> SteerPushOutcome {
    let Some(remote_run_id) = active_turn.remote_run_id.clone() else {
        return SteerPushOutcome::Refused {
            reason: "cloud turn has no remote_run_id yet".to_string(),
        };
    };
    let driver = match resolve_cloud_driver_from_metadata(
        &active_turn.harness_kind,
        active_turn.metadata_json.as_ref(),
        None,
    ) {
        Ok(d) => d,
        Err(reason) => return SteerPushOutcome::Refused { reason },
    };
    let remote_agent_id = active_turn
        .metadata_json
        .as_ref()
        .and_then(|m| m.get("remote_agent_id").and_then(Value::as_str))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    let base_url = callback_base_url.or_else(|| resolve_public_url(store.data_dir()));
    let mcp_servers = if let Some(base) = base_url {
        let store_mcp = Arc::clone(&store);
        let aid_mcp = agent.id.clone();
        tokio::task::spawn_blocking(move || {
            super::serverless_envelope::ensure_serverless_remote_session_token(
                &store_mcp, &aid_mcp, &base,
            )
            .ok()
            .map(|token| {
                let mcp_url = format!("{}{}", base.trim_end_matches('/'), crate::remote::MCP_PATH);
                json!([{
                    "name": "anyagents",
                    "url": mcp_url,
                    "bearer_token": token,
                }])
            })
        })
        .await
        .ok()
        .flatten()
    } else {
        None
    };

    let mut consumed_count = 0usize;
    let mut accepted_count = 0usize;
    for msg in eligible {
        let (_plain, outbound) = super::serverless_envelope::build_serverless_outbound_message(
            std::slice::from_ref(&msg),
        );
        let composed_prompt = compose_message(&outbound.id, &outbound.context, &outbound.text);
        let req = CloudTurnSteerRequest {
            turn_id: active_turn.turn_id.clone(),
            agent_id: agent.id.as_str().to_string(),
            remote_agent_id: remote_agent_id.clone(),
            remote_run_id: remote_run_id.clone(),
            message_id: msg.id.to_string(),
            seq: msg.seq,
            prompt: composed_prompt,
            mcp_servers: mcp_servers.clone(),
        };
        match driver.steer_turn(&req).await {
            Ok(CloudSteerAck::Consumed { proof, .. }) => {
                let store_c = Arc::clone(&store);
                let tid_c = active_turn.turn_id.clone();
                let mid_c = msg.id.to_string();
                let seq_c = msg.seq;
                let proof_c = proof.clone();
                let committed = tokio::task::spawn_blocking(move || {
                    apply_verified_steer_consumed(
                        &store_c,
                        &tid_c,
                        &mid_c,
                        seq_c,
                        &proof_c,
                        now_ms(),
                    )
                })
                .await;
                if matches!(committed, Ok(Ok(_))) {
                    consumed_count += 1;
                }
            }
            Ok(CloudSteerAck::Accepted { .. }) => {
                // Fail-closed: refresh last_event_at_ms without advancing delivered_seq.
                let store_a = Arc::clone(&store);
                let tid_a = active_turn.turn_id.clone();
                let seq_a = msg.seq;
                let _ = tokio::task::spawn_blocking(move || {
                    let _ = store_a.remote_turn_store().record_event(
                        &tid_a,
                        &RemoteTurnEventUpdate {
                            status: Some("running".to_string()),
                            cursor: Some(format!("steer_accepted:{seq_a}")),
                            remote_run_id: None,
                            last_event_at_ms: now_ms(),
                            next_poll_at_ms: None,
                        },
                    );
                })
                .await;
                accepted_count += 1;
            }
            Ok(CloudSteerAck::Refused { reason }) => {
                if consumed_count == 0 && accepted_count == 0 {
                    return SteerPushOutcome::Refused { reason };
                }
                break;
            }
            Err(reason) => {
                if consumed_count == 0 && accepted_count == 0 {
                    return SteerPushOutcome::Refused { reason };
                }
                break;
            }
        }
    }

    SteerPushOutcome::PushedCloud {
        consumed_count,
        accepted_count,
    }
}

/// Verified outcome of recording one mid-turn `steer_consumed` proof in durable storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SteerConsumeCommit {
    pub delivered_seq: i64,
    pub contiguous_ceiling: i64,
    pub advanced: bool,
}

/// Verify `(turn.agent_id, message_id)` matches `seq`, record the consumed steer in
/// `pending_remote_turns.metadata_json`, and advance `mailbox_cursors.delivered_seq`
/// strictly over the contiguous verified prefix (fail-closed on mismatch or gap).
pub(crate) fn apply_verified_steer_consumed(
    store: &Store,
    turn_id: &str,
    message_id: &str,
    seq: i64,
    proof: &str,
    event_ts_ms: i64,
) -> Result<SteerConsumeCommit, ApiError> {
    let trimmed_id = message_id.trim();
    if trimmed_id.is_empty() || seq <= 0 {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "steer_consumed requires non-empty message_id and positive seq",
        ));
    }

    let rts = store.remote_turn_store();
    let Some(turn) = rts.get_turn(turn_id).map_err(ApiError::from_store)? else {
        return Err(ApiError::new(StatusCode::NOT_FOUND, "turn not found"));
    };

    // Fail-closed integrity verification: `message_id` must belong to this agent at `seq`.
    let Some(stored_msg) = store
        .message(&turn.agent_id, trimmed_id)
        .map_err(ApiError::from_store)?
    else {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            format!("unknown steer message_id `{trimmed_id}` for agent"),
        ));
    };
    if stored_msg.seq != seq {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            format!(
                "steer seq mismatch for `{trimmed_id}`: expected {}, got {seq}",
                stored_msg.seq
            ),
        ));
    }

    let current_cursor = store
        .delivered_cursor(&turn.agent_id)
        .map_err(ApiError::from_store)?;
    let opener_seq = turn
        .metadata_json
        .as_ref()
        .and_then(|m| m.get("message_seq").and_then(Value::as_i64))
        .unwrap_or(0);

    let mut consumed_set = consumed_seqs_from_meta(turn.metadata_json.as_ref());
    consumed_set.insert(seq);

    // Advance strictly through the contiguous verified prefix starting at `max(current_cursor, opener_seq)`.
    let base = current_cursor.max(opener_seq);
    let mut ceiling = base;
    while consumed_set.contains(&(ceiling + 1)) {
        ceiling += 1;
    }

    let mut meta = turn
        .metadata_json
        .clone()
        .filter(Value::is_object)
        .unwrap_or_else(|| json!({}));
    if let Some(obj) = meta.as_object_mut() {
        let prev_max = obj
            .get(MAX_STEERED_SEQ_META_KEY)
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let next_max = prev_max.max(ceiling);
        if next_max > 0 {
            obj.insert(MAX_STEERED_SEQ_META_KEY.to_string(), json!(next_max));
        }
        let sorted_seqs: Vec<i64> = consumed_set.iter().copied().collect();
        obj.insert(CONSUMED_STEER_SEQS_META_KEY.to_string(), json!(sorted_seqs));
        obj.insert(
            "last_steer_proof".to_string(),
            json!({
                "message_id": trimmed_id,
                "seq": seq,
                "proof": proof,
                "at_ms": event_ts_ms,
            }),
        );
    }

    let _ = rts
        .update_remote_run(&turn.turn_id, None, None, None, Some(&meta))
        .map_err(ApiError::from_store)?;
    if !turn.is_terminal() {
        let _ = rts
            .record_event(
                &turn.turn_id,
                &RemoteTurnEventUpdate {
                    status: Some("running".to_string()),
                    cursor: Some(format!("steer_consumed:{seq}")),
                    remote_run_id: None,
                    last_event_at_ms: event_ts_ms,
                    next_poll_at_ms: None,
                },
            )
            .map_err(ApiError::from_store)?;
    }

    let mut delivered_seq = current_cursor;
    let mut advanced = false;
    if ceiling > base {
        if ceiling == current_cursor + 1 {
            store
                .note_delivered(&turn.agent_id, ceiling)
                .map_err(ApiError::from_store)?;
            delivered_seq = ceiling;
            advanced = true;
        } else {
            delivered_seq = store
                .mark_delivered_up_to(&turn.agent_id, ceiling)
                .map_err(ApiError::from_store)?;
            advanced = delivered_seq > current_cursor;
        }
    }

    Ok(SteerConsumeCommit {
        delivered_seq,
        contiguous_ceiling: ceiling,
        advanced,
    })
}

/// Handle `event_type = "steer_accepted"` from `/v1/internal/guest/events`.
/// Non-cursor-advancing observability event that refreshes turn heartbeat freshness.
pub(crate) fn handle_guest_steer_accepted(
    store: &Store,
    turn: &PendingRemoteTurn,
    payload: &GuestEventPayload,
    ts: i64,
) -> Result<Json<Value>, ApiError> {
    let seq = extract_guest_steer_seq(payload);
    let cursor = seq.map(|s| format!("steer_accepted:{s}"));
    let _ = store
        .remote_turn_store()
        .record_event(
            &turn.turn_id,
            &RemoteTurnEventUpdate {
                status: Some("running".to_string()),
                cursor,
                remote_run_id: None,
                last_event_at_ms: ts,
                next_poll_at_ms: None,
            },
        )
        .map_err(ApiError::from_store)?;
    Ok(Json(json!({
        "ok": true,
        "turn_id": turn.turn_id,
        "event_type": "steer_accepted",
        "message_id": payload.message_id,
        "seq": seq,
        "delivered_seq_advanced": false,
    })))
}

/// Handle `event_type = "steer_consumed"` from `/v1/internal/guest/events`.
pub(crate) fn handle_guest_steer_consumed(
    store: &Store,
    turn: &PendingRemoteTurn,
    payload: &GuestEventPayload,
    ts: i64,
) -> Result<Json<Value>, ApiError> {
    let message_id = payload.message_id.as_deref().unwrap_or("").trim();
    let Some(seq) = extract_guest_steer_seq(payload) else {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "steer_consumed missing positive seq",
        ));
    };
    let proof = payload
        .proof
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("guest:steer_consumed");
    let commit = apply_verified_steer_consumed(store, &turn.turn_id, message_id, seq, proof, ts)?;
    Ok(Json(json!({
        "ok": true,
        "turn_id": turn.turn_id,
        "event_type": "steer_consumed",
        "message_id": message_id,
        "seq": seq,
        "proof": proof,
        "delivered_seq": commit.delivered_seq,
        "delivered_seq_advanced": commit.advanced,
    })))
}

/// Handle `event_type = "steer_refused"` from `/v1/internal/guest/events` (fail-closed).
pub(crate) fn handle_guest_steer_refused(
    store: &Store,
    turn: &PendingRemoteTurn,
    payload: &GuestEventPayload,
    ts: i64,
) -> Result<Json<Value>, ApiError> {
    let seq = extract_guest_steer_seq(payload);
    let _ = store
        .remote_turn_store()
        .record_event(
            &turn.turn_id,
            &RemoteTurnEventUpdate {
                status: Some("running".to_string()),
                cursor: seq.map(|s| format!("steer_refused:{s}")),
                remote_run_id: None,
                last_event_at_ms: ts,
                next_poll_at_ms: None,
            },
        )
        .map_err(ApiError::from_store)?;
    Ok(Json(json!({
        "ok": true,
        "turn_id": turn.turn_id,
        "event_type": "steer_refused",
        "message_id": payload.message_id,
        "seq": seq,
        "delivered_seq_advanced": false,
    })))
}

/// Belt-and-suspenders commit of any verified `consumed_steers` attached to `turn_complete`
/// before settlement runs (idempotent if `steer_consumed` already arrived mid-turn).
pub(crate) fn apply_turn_complete_consumed_steers(
    store: &Store,
    turn: &PendingRemoteTurn,
    payload: &GuestEventPayload,
    ts: i64,
) {
    let Some(ref consumed) = payload.consumed_steers else {
        return;
    };
    let mut ordered = consumed.clone();
    ordered.sort_by_key(|c| c.seq);
    for item in ordered {
        let _ = apply_verified_steer_consumed(
            store,
            &turn.turn_id,
            &item.message_id,
            item.seq,
            &item.proof,
            ts,
        );
    }
}

/// Close durable mailbox cursor and `turn_recovery` ledger over `max_steered_seq`
/// when a remote turn completes after consuming mid-turn steers.
pub(crate) fn settle_steered_turn_cursor(store: &Store, turn: &PendingRemoteTurn) {
    if turn.status != "completed" {
        return;
    }
    let Some(steered_seq) = turn
        .metadata_json
        .as_ref()
        .and_then(|m| m.get(MAX_STEERED_SEQ_META_KEY).and_then(Value::as_i64))
        .filter(|&s| s > 0)
    else {
        return;
    };
    let _ = store.mark_delivered_up_to(&turn.agent_id, steered_seq);
    let opener = turn
        .metadata_json
        .as_ref()
        .and_then(|m| m.get("opener_id").and_then(Value::as_str));
    let _ = store.note_turn_completed_with_output_for(
        &turn.agent_id,
        steered_seq,
        opener.zip(turn.result_text.as_deref()),
    );
}

fn extract_guest_steer_seq(payload: &GuestEventPayload) -> Option<i64> {
    payload
        .message_seq
        .or_else(|| payload.seq.and_then(|s| i64::try_from(s).ok()))
        .filter(|&s| s > 0)
}

fn consumed_seqs_from_meta(meta: Option<&Value>) -> BTreeSet<i64> {
    let mut set = BTreeSet::new();
    let Some(arr) = meta
        .and_then(|m| m.get(CONSUMED_STEER_SEQS_META_KEY))
        .and_then(Value::as_array)
    else {
        return set;
    };
    for v in arr {
        if let Some(s) = v.as_i64().filter(|&s| s > 0) {
            set.insert(s);
        }
    }
    set
}

fn validate_safe_turn_id(turn_id: &str) -> Result<(), ()> {
    let t = turn_id.trim();
    if t.is_empty()
        || t.len() > 128
        || t.contains('/')
        || t.contains('\\')
        || t.contains("..")
        || t.bytes().any(|b| b.is_ascii_control())
    {
        return Err(());
    }
    Ok(())
}

#[cfg(test)]
#[path = "serverless_steer_tests.rs"]
mod tests;
