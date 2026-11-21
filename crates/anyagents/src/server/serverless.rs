//! Pure Serverless Coordinator engine (`ANYAGENTS_SERVERLESS=true`).
//!
//! Designed for scale-to-zero HTTP runtimes where coordinator pods may exit
//! seconds after returning `200 OK`:
//! - **Zero** long-lived background loops, `envd` streams, or Spiron SSE tasks.
//! - Synchronous (<1s) detached sandbox dispatch (`provider.ensure()` +
//!   `anyagents-guest turn --detached`) and async cloud turn submission
//!   (`CloudTurnDriver::submit_turn`), persisted in `public.pending_remote_turns`.
//! - Authenticated internal endpoints:
//!   - `POST /v1/internal/guest/events`: constant-time per-turn SHA-256 bearer auth,
//!     heartbeat + `inflight_children` updates, and idempotent turn settlement.
//!   - `POST /v1/internal/cloud/events`: constant-time per-turn bearer or HMAC-SHA256
//!     webhook verification, `CloudTurnDriver::parse_webhook`, and idempotent settlement.
//!   - `POST /v1/internal/fire/{id}` & `POST /v1/internal/fire`: event-driven
//!     single-schedule fire endpoints (implemented in `server/timer_fire.rs`).

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
#[cfg(test)]
use std::time::Instant;

use crate::sandbox_providers::providers::{ExecRequest, SandboxProvider as AsyncSandboxProvider};
use anyagents_core::stateless::{
    NewRemoteTurn, PendingRemoteTurn, RemoteTurnEventUpdate, SettleRemoteTurn, constant_time_eq,
    generate_callback_token, generate_turn_id, hash_callback_token, verify_callback_token_hash,
    verify_hmac_sha256_signature,
};
use anyagents_core::{
    Agent, AgentId, BillingMode, NewUsage, RuntimeHeartbeat, Store, now_ms, utc_day,
};
use axum::Json;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{ApiError, AppState, spawn_store};
use crate::sandbox_exec::{
    GuestHeartbeatFrame, apply_guest_heartbeat, resolve_registry_for_binding,
    resolve_remote_provider_registry, sandbox_handle_from_binding, sweep_idle_sandboxes,
};
use crate::serverless_cloud::{
    CloudTurnDriver, CloudTurnSubmitRequest, GenericHttpCloudDriver, SpironServerlessDriver,
    resolve_cloud_driver_from_metadata,
};

pub(super) const DEFAULT_CLOUD_POLL_DELAY_MS: i64 = 5_000;
pub(super) const MAX_CLOUD_POLL_BACKOFF_MS: i64 = 30_000;
const DEFAULT_SANDBOX_STALE_MS: i64 = 120_000;
pub(super) const DEFAULT_LEASE_RENEW_WINDOW_MS: i64 = 300_000;
const DETACHED_EXEC_TIMEOUT_MS: u64 = 15_000;
const TICK_CLOUD_POLL_LIMIT: usize = 32;
const TICK_STALE_SANDBOX_LIMIT: usize = 32;
const SYNCABLE_AUTH_FILES: &[&str] = &[
    ".gemini/antigravity-cli/antigravity-oauth-token",
    ".claude/.credentials.json",
    ".credentials.json",
    ".codex/auth.json",
];
const MAX_SYNCED_AUTH_FILE_BYTES: usize = 64 * 1024;
const SANDBOX_AUTH_PULL_TIMEOUT_MS: u64 = 5_000;

/// Return `true` when `ANYAGENTS_SERVERLESS=true` is enabled.
pub(crate) fn is_serverless_mode() -> bool {
    crate::sandbox_providers::serverless_enabled_from_env()
}

fn env_non_empty(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// Resolve the coordinator's public base URL for serverless callbacks:
/// `ANYAGENTS_PUBLIC_URL` -> `ANYAGENTS_WEBHOOK_URL` -> `[server] public_url` in `anyagents.toml`.
pub(crate) fn resolve_public_url(data_dir: &Path) -> Option<String> {
    if let Some(url) = env_non_empty("ANYAGENTS_PUBLIC_URL") {
        return Some(url.trim_end_matches('/').to_string());
    }
    if let Some(url) = env_non_empty("ANYAGENTS_WEBHOOK_URL") {
        return Some(url.trim_end_matches('/').to_string());
    }
    crate::remote::Settings::load(data_dir)
        .ok()
        .and_then(|s| s.public_url)
        .map(|u| u.trim_end_matches('/').to_string())
}

/// Build `<PUBLIC_URL>/v1/internal/guest/events`.
#[allow(dead_code)]
pub(crate) fn guest_events_callback_url(data_dir: &Path) -> Option<String> {
    resolve_public_url(data_dir).map(|base| format!("{base}/v1/internal/guest/events"))
}

/// Build `<PUBLIC_URL>/v1/internal/cloud/events`.
#[allow(dead_code)]
pub(crate) fn cloud_events_callback_url(data_dir: &Path) -> Option<String> {
    resolve_public_url(data_dir).map(|base| format!("{base}/v1/internal/cloud/events"))
}

pub(super) fn sandbox_stale_threshold_ms() -> i64 {
    env_non_empty("ANYAGENTS_SERVERLESS_SANDBOX_STALE_MS")
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|&ms| ms > 0)
        .unwrap_or(DEFAULT_SANDBOX_STALE_MS)
}

/// Return `true` when `harness` runs via an asynchronous cloud harness driver.
pub(crate) fn is_serverless_cloud_harness(harness: &str) -> bool {
    let lower = harness.trim().to_ascii_lowercase();
    crate::vendor::is_cloud_harness(&lower) || matches!(lower.as_str(), "http-cloud" | "http_cloud")
}

/// Outbound reply destination persisted in `pending_remote_turns.metadata_json` so any
/// replica can deliver the reply after scale-to-zero without holding caller state in memory.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ReplyTarget {
    Telegram {
        chat_id: i64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread_id: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reply_to_message_id: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bot: Option<String>,
    },
    Agent {
        agent_id: String,
    },
}

/// Input parameters for dispatching one remote turn synchronously (<1s) in serverless mode.
#[derive(Clone)]
pub(crate) struct ServerlessDispatchInput {
    pub agent: Agent,
    pub prompt: String,
    pub outbound_message: Option<anyagents_harness::OutboundMessage>,
    pub message_seq: Option<i64>,
    pub opener_id: Option<String>,
    pub reply_target: Option<ReplyTarget>,
    pub callback_base_url: Option<String>,
    pub cloud_endpoint: Option<String>,
    pub remote_agent_id: Option<String>,
    pub sandbox_provider: Option<Arc<dyn AsyncSandboxProvider>>,
    pub guest_binary: Option<String>,
    pub guest_program: Option<String>,
    pub guest_extra_args: Vec<String>,
    pub heartbeat_interval_ms: Option<u64>,
    pub timer_backend: Option<Arc<dyn crate::timer::TimerBackend>>,
}

impl ServerlessDispatchInput {
    pub(crate) fn for_agent(agent: Agent, prompt: impl Into<String>) -> Self {
        Self {
            agent,
            prompt: prompt.into(),
            outbound_message: None,
            message_seq: None,
            opener_id: None,
            reply_target: None,
            callback_base_url: None,
            cloud_endpoint: None,
            remote_agent_id: None,
            sandbox_provider: None,
            guest_binary: None,
            guest_program: None,
            guest_extra_args: Vec::new(),
            heartbeat_interval_ms: None,
            timer_backend: None,
        }
    }
}

/// Result of a synchronous (<1s) serverless turn dispatch.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct DispatchedRemoteTurn {
    pub turn: PendingRemoteTurn,
    /// One-time per-turn callback token handed to the detached guest / cloud harness
    /// (only its SHA-256 hash is persisted in PostgreSQL).
    pub callback_token: String,
}

async fn run_store_blocking<T, F>(f: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| format!("store worker join error: {e}"))?
}

/// Dispatch one remote turn synchronously in <1s without spawning any coordinator-side
/// background loops or long-lived pipes/SSE streams.
#[allow(dead_code)]
pub(crate) async fn dispatch_serverless_turn(
    store: Arc<Store>,
    input: ServerlessDispatchInput,
) -> Result<DispatchedRemoteTurn, String> {
    let agent_id = input.agent.id.to_string();
    dispatch_serverless_turn_opt(store, input)
        .await?
        .ok_or_else(|| anyagents_core::Error::TurnAlreadyActive(agent_id).to_string())
}

async fn dispatch_serverless_turn_opt(
    store: Arc<Store>,
    input: ServerlessDispatchInput,
) -> Result<Option<DispatchedRemoteTurn>, String> {
    let base_url = input
        .callback_base_url
        .clone()
        .or_else(|| resolve_public_url(store.data_dir()))
        .ok_or_else(|| {
            "serverless turn dispatch requires ANYAGENTS_PUBLIC_URL (or ANYAGENTS_WEBHOOK_URL)"
                .to_string()
        })?;
    let base_url = base_url.trim_end_matches('/').to_string();

    if input.agent.runtime.is_cloud() || is_serverless_cloud_harness(&input.agent.harness) {
        dispatch_cloud_turn(store, input, &base_url).await
    } else {
        dispatch_sandbox_turn(store, input, &base_url).await
    }
}

async fn dispatch_cloud_turn(
    store: Arc<Store>,
    input: ServerlessDispatchInput,
    base_url: &str,
) -> Result<Option<DispatchedRemoteTurn>, String> {
    let store_budget = Arc::clone(&store);
    let agent_budget = input.agent.clone();
    run_store_blocking(move || {
        super::runtime::check_remote_budget_cap(&store_budget, &agent_budget)
    })
    .await?;

    let canonical = input.agent.canonical_harness();
    let harness_kind = if canonical == "spiron" {
        "spiron"
    } else {
        "http_cloud"
    };

    let turn_id = generate_turn_id();
    let callback_token = generate_callback_token();
    let callback_token_hash = hash_callback_token(&callback_token);
    let callback_url = format!("{base_url}/v1/internal/cloud/events");

    // Load previously persisted Spiron/cloud remote agent ID from PG `agent_files`.
    let saved_remote_agent = if let Some(rid) = input.remote_agent_id.clone() {
        Some(rid)
    } else {
        let store_r = Arc::clone(&store);
        let aid = input.agent.id.clone();
        run_store_blocking(move || {
            Ok(store_r
                .blob_store()
                .get_agent_file(&aid, "spiron_cloud", "remote_agent_id")
                .ok()
                .flatten()
                .and_then(|b| String::from_utf8(b.content).ok())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()))
        })
        .await?
    };

    let cloud_endpoint = input
        .cloud_endpoint
        .clone()
        .or_else(|| {
            if harness_kind == "spiron" {
                env_non_empty("ANYAGENTS_SPIRON_CLOUD_URL")
                    .or_else(|| env_non_empty("SPIRON_API_URL"))
                    .or_else(|| env_non_empty("SPIRON_CLOUD_URL"))
                    .or_else(|| Some("https://api.spiron.ai".to_string()))
            } else {
                env_non_empty("ANYAGENTS_CLOUD_HTTP_URL")
            }
        })
        .ok_or_else(|| {
            format!("no cloud endpoint configured for serverless harness `{harness_kind}`")
        })?;

    let mut meta_val = super::runtime::build_cloud_turn_metadata(
        &input,
        &cloud_endpoint,
        saved_remote_agent.as_deref(),
    );

    let now = now_ms();
    let store_create = Arc::clone(&store);
    let new_turn = NewRemoteTurn {
        turn_id: turn_id.clone(),
        agent_id: input.agent.id.clone(),
        harness_kind: harness_kind.to_string(),
        provider_kind: Some(harness_kind.to_string()),
        remote_run_id: None,
        callback_token_hash,
        cursor: None,
        status: "dispatched".to_string(),
        metadata_json: Some(meta_val.clone()),
        next_poll_at_ms: Some(now.saturating_add(DEFAULT_CLOUD_POLL_DELAY_MS)),
    };
    let aid_mcp = input.agent.id.clone();
    let base_url_mcp = base_url.to_string();
    let Some((created, mcp_servers)) = run_store_blocking(move || {
        let created = match store_create.remote_turn_store().create_turn(&new_turn) {
            Ok(turn) => turn,
            Err(err) if err.is_active_turn_conflict() => return Ok(None),
            Err(err) => return Err(format!("failed to persist pending cloud turn: {err}")),
        };
        // Build per-turn Streamable HTTP MCP attachment without revoking sibling live tokens.
        let mcp_servers = super::serverless_envelope::ensure_serverless_remote_session_token(
            &store_create,
            &aid_mcp,
            &base_url_mcp,
        )
        .ok()
        .map(|token| {
            let mcp_url = format!("{base_url_mcp}{}", crate::remote::MCP_PATH);
            json!([{
                "name": "anyagents",
                "url": mcp_url,
                "bearer_token": token,
            }])
        });
        Ok(Some((created, mcp_servers)))
    })
    .await?
    else {
        return Ok(None);
    };

    let driver = resolve_cloud_driver_from_metadata(harness_kind, Some(&meta_val), None)?;
    let prompt = super::knowledge::cloud_wake_prompt(&store, &input).await;
    let submit_req = CloudTurnSubmitRequest {
        turn_id: turn_id.clone(),
        agent_id: input.agent.id.as_str().to_string(),
        remote_agent_id: saved_remote_agent,
        prompt,
        model: input.agent.model.clone(),
        instructions: input.agent.instructions.clone(),
        mcp_servers,
        callback_url: Some(callback_url),
        callback_token: Some(callback_token.clone()),
        metadata: BTreeMap::new(),
    };

    let submission = match driver.submit_turn(&submit_req).await {
        Ok(sub) => sub,
        Err(err) => {
            let store_fail = Arc::clone(&store);
            let tid_fail = turn_id.clone();
            let err_fail = err.clone();
            let _ = run_store_blocking(move || {
                let _ = store_fail.remote_turn_store().settle_turn(
                    &tid_fail,
                    &SettleRemoteTurn {
                        status: "failed".to_string(),
                        result_text: None,
                        error_text: Some(err_fail),
                        usage_json: None,
                        cursor: None,
                        settled_at_ms: now_ms(),
                    },
                );
                Ok(())
            })
            .await;
            return Err(err);
        }
    };

    if let Some(ref remote_aid) = submission.remote_agent_id
        && let Some(obj) = meta_val.as_object_mut()
    {
        obj.insert("remote_agent_id".into(), json!(remote_aid));
    }
    let next_poll = submission
        .next_poll_after_ms
        .map(|d| now_ms().saturating_add(i64::try_from(d).unwrap_or(DEFAULT_CLOUD_POLL_DELAY_MS)));
    let updated_meta = meta_val;

    let store_upd = Arc::clone(&store);
    let aid_upd = input.agent.id.clone();
    let tid_upd = turn_id.clone();
    let seq_upd = input.message_seq;
    let tb_upd = super::serverless_maintenance::resolve_timer_backend(
        input.timer_backend.as_ref(),
        store.data_dir(),
    );
    let final_turn = run_store_blocking(move || {
        if let Some(ref remote_aid) = submission.remote_agent_id {
            let _ = store_upd.blob_store().put_agent_file(
                &aid_upd,
                "spiron_cloud",
                "remote_agent_id",
                remote_aid.as_bytes(),
                false,
            );
        }
        let updated = store_upd
            .remote_turn_store()
            .update_remote_run(
                &tid_upd,
                Some(&submission.remote_run_id),
                Some(if submission.status.is_terminal() {
                    "running"
                } else {
                    submission.status.as_str()
                }),
                Some(next_poll),
                Some(&updated_meta),
            )
            .map_err(|e| format!("failed to update remote_run_id: {e}"))?
            .unwrap_or(created);

        if let Some(seq) = seq_upd {
            let _ = store_upd.note_delivered(&aid_upd, seq);
        }
        let _ = store_upd.update_runtime_heartbeat(
            &aid_upd,
            &RuntimeHeartbeat {
                owner_replica_id: Some(store_upd.replica_id().to_string()),
                turn_state: "in_turn".to_string(),
                retry_at_ms: None,
                heartbeat_at_ms: Some(now_ms()),
                idle_since_ms: None,
                inflight_children: 0,
            },
        );

        if submission.status.is_terminal() {
            let settled = settle_remote_turn_and_deliver(
                &store_upd,
                &tid_upd,
                SettleRemoteTurn {
                    status: submission.status.as_str().to_string(),
                    result_text: submission.result_text,
                    error_text: submission.error_text,
                    usage_json: submission.usage,
                    cursor: submission.cursor,
                    settled_at_ms: now_ms(),
                },
                None,
                0,
            )?;
            return Ok(settled.map(|s| s.turn).unwrap_or(updated));
        }

        // Event-driven Step 2a: arm exact cloud turn poll timer on TimerBackend.
        let poll_at =
            next_poll.unwrap_or_else(|| now_ms().saturating_add(DEFAULT_CLOUD_POLL_DELAY_MS));
        super::serverless_maintenance::arm_cloud_poll_timer(
            &store_upd,
            tb_upd.as_ref(),
            &aid_upd,
            &tid_upd,
            poll_at,
        );

        Ok(updated)
    })
    .await?;

    Ok(Some(DispatchedRemoteTurn {
        turn: final_turn,
        callback_token,
    }))
}

async fn dispatch_sandbox_turn(
    store: Arc<Store>,
    input: ServerlessDispatchInput,
    base_url: &str,
) -> Result<Option<DispatchedRemoteTurn>, String> {
    let provider: Arc<dyn AsyncSandboxProvider> = if let Some(p) = input.sandbox_provider.clone() {
        p
    } else if let Some(reg) = resolve_remote_provider_registry(Some(store.data_dir()))? {
        reg.active()
    } else {
        let store_b = Arc::clone(&store);
        let aid_b = input.agent.id.clone();
        let existing =
            run_store_blocking(move || Ok(store_b.sandbox_binding(&aid_b).ok().flatten())).await?;
        if let Some(existing) = existing
            && existing.provider != "legacy_http"
            && existing.provider != "local"
        {
            resolve_registry_for_binding(Some(store.data_dir()), &existing)?.active()
        } else {
            return Err(
                "serverless sandbox dispatch requires a configured SandboxProvider (`ANYAGENTS_SANDBOX_PROVIDER`)"
                    .to_string(),
            );
        }
    };

    let provider_kind_str = provider.kind().as_str().to_string();
    let turn_id = generate_turn_id();
    let callback_token = generate_callback_token();
    let callback_token_hash = hash_callback_token(&callback_token);
    let callback_url = format!("{base_url}/v1/internal/guest/events");

    let mut meta = serde_json::Map::new();
    meta.insert("agent_harness".into(), json!(input.agent.harness));
    if let Some(ref model) = input.agent.model {
        meta.insert("agent_model".into(), json!(model));
    }
    if let Some(seq) = input.message_seq {
        meta.insert("message_seq".into(), json!(seq));
    }
    if let Some(ref opener) = input.opener_id {
        meta.insert("opener_id".into(), json!(opener));
    }
    if let Some(ref rt) = input.reply_target {
        meta.insert(
            "reply_target".into(),
            serde_json::to_value(rt).unwrap_or(Value::Null),
        );
    }

    let store_create = Arc::clone(&store);
    let new_turn = NewRemoteTurn {
        turn_id: turn_id.clone(),
        agent_id: input.agent.id.clone(),
        harness_kind: "sandbox".to_string(),
        provider_kind: Some(provider_kind_str),
        remote_run_id: None,
        callback_token_hash,
        cursor: None,
        status: "dispatched".to_string(),
        metadata_json: Some(Value::Object(meta)),
        next_poll_at_ms: None,
    };
    let upgrade_provider = Arc::clone(&provider);
    let Some(created) = run_store_blocking(move || {
        let _guard = match store_create.try_lock_agent(&new_turn.agent_id) {
            Ok(guard) => guard,
            Err(anyagents_core::Error::AgentBusy) => {
                return Err("agent lease is being reconciled; retry dispatch".into());
            }
            Err(err) => return Err(err.to_string()),
        };
        if crate::sandbox_upgrade::recycle_at_boundary(
            &store_create,
            &new_turn.agent_id,
            Some(upgrade_provider.as_ref()),
        )? == crate::sandbox_upgrade::Action::DrainScheduled
        {
            return Ok(None);
        }
        match store_create.remote_turn_store().create_turn(&new_turn) {
            Ok(turn) => Ok(Some(turn)),
            Err(err) if err.is_active_turn_conflict() => Ok(None),
            Err(err) => Err(format!("failed to persist pending sandbox turn: {err}")),
        }
    })
    .await?
    else {
        return Ok(None);
    };

    let guest_bin = input
        .guest_binary
        .clone()
        .or_else(|| env_non_empty("ANYAGENTS_SANDBOX_GUEST_PATH"))
        .unwrap_or_else(|| "/usr/local/bin/anyagents-guest".to_string());

    let mut attempt = 0usize;
    let (ensured, out) = loop {
        let ensured_res = super::serverless_envelope::ensure_and_prepare_sandbox_for_turn(
            Arc::clone(&store),
            Arc::clone(&provider),
            input.agent.clone(),
            input.prompt.clone(),
            base_url.to_string(),
            input.guest_extra_args.clone(),
        )
        .await;

        let (ensured, effective_prompt) = match ensured_res {
            Ok(pair) => pair,
            Err((maybe_handle, err)) => {
                if let Some(handle) = maybe_handle.as_deref()
                    && attempt == 0
                    && super::serverless_failure::is_unhealthy_lease_exec_error(&err)
                {
                    super::serverless_failure::release_unhealthy_lease_before_retry(
                        &store,
                        &provider,
                        &input.agent.id,
                        handle,
                    )
                    .await;
                    attempt += 1;
                    continue;
                }
                super::serverless_failure::fail_remote_turn(
                    &store,
                    &input.agent.id,
                    &turn_id,
                    err.clone(),
                    maybe_handle.as_deref().map(|h| (&provider, h)),
                )
                .await;
                return Err(err);
            }
        };

        let store_env = Arc::clone(&store);
        let agent_env = input.agent.clone();
        let resolved_env = run_store_blocking(move || {
            let mut env = BTreeMap::new();
            super::secrets::merge_sandbox_exec_env(&store_env, &agent_env, &mut env)?;
            Ok(env)
        })
        .await;
        let mut exec_req = ExecRequest::argv(Vec::<String>::new())
            .with_cwd(ensured.handle.workdir.clone())
            .with_timeout_ms(DETACHED_EXEC_TIMEOUT_MS);
        match resolved_env {
            Ok(env) => exec_req.env.extend(env),
            Err(err) => {
                super::serverless_failure::fail_remote_turn(
                    &store,
                    &input.agent.id,
                    &turn_id,
                    err.clone(),
                    Some((&provider, &ensured.handle)),
                )
                .await;
                return Err(err);
            }
        }
        let message_file = if let Some(msg_json) =
            super::serverless_envelope::attach_outbound_message_env(
                &mut exec_req,
                input.outbound_message.clone(),
                &effective_prompt,
            ) {
            match super::serverless_envelope::stage_oversized_outbound_message_file(
                provider.as_ref(),
                &ensured.handle,
                &turn_id,
                &msg_json,
            )
            .await
            {
                Ok(path) => Some(path),
                Err(err) => {
                    if attempt == 0
                        && super::serverless_failure::is_unhealthy_lease_exec_error(&err)
                    {
                        super::serverless_failure::release_unhealthy_lease_before_retry(
                            &store,
                            &provider,
                            &input.agent.id,
                            &ensured.handle,
                        )
                        .await;
                        attempt += 1;
                        continue;
                    }
                    super::serverless_failure::fail_remote_turn(
                        &store,
                        &input.agent.id,
                        &turn_id,
                        err.clone(),
                        Some((&provider, &ensured.handle)),
                    )
                    .await;
                    return Err(err);
                }
            }
        } else {
            None
        };
        exec_req.command = super::serverless_envelope::build_detached_guest_turn_argv(
            guest_bin.clone(),
            &turn_id,
            &input.agent,
            &callback_url,
            &callback_token,
            &ensured.handle,
            input.guest_program.as_deref(),
            input.heartbeat_interval_ms,
            message_file.as_deref(),
            &input.guest_extra_args,
        );
        exec_req = exec_req.with_stdin(effective_prompt.into_bytes());

        match provider.exec(&ensured.handle, &exec_req).await {
            Ok(o) => break (ensured, o),
            Err(err) => {
                if attempt == 0 && super::serverless_failure::is_unhealthy_lease_exec_error(&err) {
                    super::serverless_failure::release_unhealthy_lease_before_retry(
                        &store,
                        &provider,
                        &input.agent.id,
                        &ensured.handle,
                    )
                    .await;
                    attempt += 1;
                    continue;
                }
                super::serverless_failure::fail_remote_turn(
                    &store,
                    &input.agent.id,
                    &turn_id,
                    err.clone(),
                    Some((&provider, &ensured.handle)),
                )
                .await;
                return Err(err);
            }
        }
    };

    if out.exit_code != 0 {
        let diag = format!(
            "detached guest launch failed (exit {}): {}",
            out.exit_code,
            out.stderr.trim()
        );
        let masked = super::serverless_failure::fail_remote_turn(
            &store,
            &input.agent.id,
            &turn_id,
            diag,
            Some((&provider, &ensured.handle)),
        )
        .await;
        return Err(masked);
    }

    let remote_run_id = serde_json::from_str::<Value>(out.stdout.trim())
        .ok()
        .and_then(|v| v.get("pid").and_then(Value::as_u64))
        .map(|pid| format!("pid:{pid}"));

    let store_upd = Arc::clone(&store);
    let aid_upd = input.agent.id.clone();
    let tid_upd = turn_id.clone();
    let seq_upd = input.message_seq;
    let state_dir_upd = ensured.handle.state_dir.clone();
    let workdir_upd = ensured.handle.workdir.clone();
    let tb_upd = super::serverless_maintenance::resolve_timer_backend(
        input.timer_backend.as_ref(),
        store.data_dir(),
    );
    let updated = run_store_blocking(move || {
        let now = now_ms();
        let mut updated_meta = created
            .metadata_json
            .clone()
            .filter(Value::is_object)
            .unwrap_or_else(|| json!({}));
        if let Some(obj) = updated_meta.as_object_mut() {
            obj.insert("state_dir".into(), json!(state_dir_upd));
            obj.insert("workdir".into(), json!(workdir_upd));
        }
        let updated = store_upd
            .remote_turn_store()
            .update_remote_run(
                &tid_upd,
                remote_run_id.as_deref(),
                Some("running"),
                None,
                Some(&updated_meta),
            )
            .map_err(|e| format!("failed to update sandbox remote_run_id: {e}"))?
            .unwrap_or(created);

        if let Some(seq) = seq_upd {
            let _ = store_upd.note_delivered(&aid_upd, seq);
        }
        let _ = store_upd.update_runtime_heartbeat(
            &aid_upd,
            &RuntimeHeartbeat {
                owner_replica_id: Some(store_upd.replica_id().to_string()),
                turn_state: "in_turn".to_string(),
                retry_at_ms: None,
                heartbeat_at_ms: Some(now),
                idle_since_ms: None,
                inflight_children: 0,
            },
        );

        // Event-driven Step 2b: arm exact stale sandbox timeout deadline timer.
        super::serverless_maintenance::arm_turn_timeout_timer(
            &store_upd,
            tb_upd.as_ref(),
            &aid_upd,
            &tid_upd,
            now.saturating_add(sandbox_stale_threshold_ms()),
        );
        // Event-driven Step 3: arm active sandbox lease-renewal timer before lease expiry.
        let binding_exp = store_upd
            .sandbox_binding(&aid_upd)
            .ok()
            .flatten()
            .and_then(|b| b.lease_expires_at_ms);
        super::serverless_maintenance::arm_lease_renew_timer(
            &store_upd,
            tb_upd.as_ref(),
            &aid_upd,
            binding_exp,
            now,
        );
        Ok(updated)
    })
    .await?;

    Ok(Some(DispatchedRemoteTurn {
        turn: updated,
        callback_token,
    }))
}

/// When `ANYAGENTS_SERVERLESS=true`, synchronously dispatch all pending undelivered
/// mailbox messages (batched up to 20) for `agent_id` once no live remote turn is running.
/// Automatically settles timed-out remote turns and clears orphaned `turn_recovery` locks
/// so dead detached turns can never deadlock subsequent messages.
pub(crate) async fn dispatch_next_mailbox_message_if_serverless(
    store: Arc<Store>,
    agent_id: &AgentId,
) -> Result<Option<DispatchedRemoteTurn>, String> {
    dispatch_next_mailbox_message_with_timer(store, agent_id, None).await
}

/// Same as [`dispatch_next_mailbox_message_if_serverless`], passing an explicit
/// [`crate::timer::TimerBackend`] so dispatched turns arm their event-driven timers directly.
pub(crate) async fn dispatch_next_mailbox_message_with_timer(
    store: Arc<Store>,
    agent_id: &AgentId,
    timer_backend: Option<Arc<dyn crate::timer::TimerBackend>>,
) -> Result<Option<DispatchedRemoteTurn>, String> {
    if !is_serverless_mode() {
        return Ok(None);
    }
    dispatch_next_mailbox_message_inner(store, agent_id, None, None, timer_backend).await
}

pub(super) async fn dispatch_next_mailbox_message_inner(
    store: Arc<Store>,
    agent_id: &AgentId,
    sandbox_provider: Option<Arc<dyn AsyncSandboxProvider>>,
    callback_base_url: Option<String>,
    timer_backend: Option<Arc<dyn crate::timer::TimerBackend>>,
) -> Result<Option<DispatchedRemoteTurn>, String> {
    enum DispatchPlan {
        Idle,
        Steer(Agent, Box<PendingRemoteTurn>, Vec<anyagents_core::Message>),
        Dispatch(Agent, Vec<anyagents_core::Message>),
    }

    for _attempt in 0..2 {
        let store_r = Arc::clone(&store);
        let aid = agent_id.clone();
        let plan = run_store_blocking(move || {
            let agent = store_r
                .agent(&aid)
                .map_err(|e| format!("load agent {aid}: {e}"))?;
            if matches!(agent.state.as_str(), "paused" | "archived") {
                return Ok(DispatchPlan::Idle);
            }
            let now = now_ms();
            let stale_before = now.saturating_sub(sandbox_stale_threshold_ms());
            let active = store_r
                .remote_turn_store()
                .list_active_for_agent(&aid)
                .map_err(|e| e.to_string())?;
            let mut live_turn: Option<PendingRemoteTurn> = None;
            for turn in active {
                if turn.harness_kind == "sandbox" && turn.last_event_at_ms <= stale_before {
                    super::serverless_failure::settle_timed_out_sandbox_turn(&store_r, &turn, now);
                } else if live_turn.is_none() {
                    live_turn = Some(turn);
                }
            }
            if let Some(turn) = live_turn {
                let msgs = store_r
                    .list_undelivered(&aid, 20)
                    .map_err(|e| e.to_string())?;
                if msgs.is_empty() {
                    return Ok(DispatchPlan::Idle);
                }
                return Ok(DispatchPlan::Steer(agent, Box::new(turn), msgs));
            }
            super::serverless_failure::clear_unhealthy_sandbox_lease(&store_r, &aid);
            if store_r.has_open_turn(&aid).unwrap_or(false) {
                let _ = store_r.reconcile_stale_open_turn(&aid);
            }
            let msgs = store_r
                .list_undelivered(&aid, 20)
                .map_err(|e| e.to_string())?;
            if msgs.is_empty() {
                return Ok(DispatchPlan::Idle);
            }
            Ok(DispatchPlan::Dispatch(agent, msgs))
        })
        .await?;

        match plan {
            DispatchPlan::Idle => return Ok(None),
            DispatchPlan::Steer(agent, active_turn, msgs) => {
                match super::serverless_steer::try_steer_active_turn(
                    Arc::clone(&store),
                    agent,
                    *active_turn,
                    msgs,
                    sandbox_provider.clone(),
                    callback_base_url.clone(),
                )
                .await
                {
                    super::serverless_steer::SteerPushOutcome::TurnSettledRace => continue,
                    _ => return Ok(None),
                }
            }
            DispatchPlan::Dispatch(agent, msgs) => {
                let last = msgs.last().expect("non-empty batch");
                let (prompt, mut outbound) =
                    super::serverless_envelope::build_serverless_outbound_message(&msgs);
                super::serverless_envelope::attach_deployment_skills_to_topology(
                    &mut outbound,
                    store.data_dir(),
                );
                // Prefer the latest human/Telegram reply target in the batch over older scheduled reminders.
                let reply_target = msgs
                    .iter()
                    .rev()
                    .find_map(|m| match infer_reply_target_from_message(m) {
                        Some(rt @ ReplyTarget::Telegram { .. }) => Some(rt),
                        _ => None,
                    })
                    .or_else(|| msgs.iter().rev().find_map(infer_reply_target_from_message));

                let mut input = ServerlessDispatchInput::for_agent(agent, prompt);
                input.outbound_message = Some(outbound);
                input.message_seq = Some(last.seq);
                input.opener_id = Some(last.id.to_string());
                input.reply_target = reply_target;
                input.sandbox_provider = sandbox_provider;
                input.callback_base_url = callback_base_url;
                let retry_timer = timer_backend.clone();
                input.timer_backend = timer_backend;
                let result = dispatch_serverless_turn_opt(Arc::clone(&store), input).await;
                let tb = super::serverless_maintenance::resolve_timer_backend(
                    retry_timer.as_ref(),
                    store.data_dir(),
                );
                let aid_done = agent_id.clone();
                match result {
                    Ok(Some(dispatched)) => {
                        let store_clr = Arc::clone(&store);
                        let _ = run_store_blocking(move || {
                            super::serverless_maintenance::clear_dispatch_retry_state(
                                &store_clr,
                                Some(tb.as_ref()),
                                &aid_done,
                            );
                            Ok(())
                        })
                        .await;
                        return Ok(Some(dispatched));
                    }
                    Ok(None) => return Ok(None),
                    Err(err) => {
                        let store_err = Arc::clone(&store);
                        let err_copy = err.clone();
                        let _ = run_store_blocking(move || {
                            super::serverless_maintenance::record_dispatch_failure_and_arm_retry(
                                &store_err,
                                tb.as_ref(),
                                &aid_done,
                                &err_copy,
                            );
                            Ok(())
                        })
                        .await;
                        return Err(err);
                    }
                }
            }
        }
    }
    Ok(None)
}

fn infer_reply_target_from_message(msg: &anyagents_core::Message) -> Option<ReplyTarget> {
    if let Some(ref origin) = msg.origin
        && let Some(chat_id) = origin.chat_id
    {
        return Some(ReplyTarget::Telegram {
            chat_id,
            thread_id: origin.thread_id,
            reply_to_message_id: origin.message_id,
            bot: None,
        });
    }
    if let Some(ref sender) = msg.sender
        && let Some(aid) = sender.strip_prefix("agent:")
        && AgentId::parse(aid).is_ok()
    {
        return Some(ReplyTarget::Agent {
            agent_id: aid.to_string(),
        });
    }
    None
}

// ============================================================================
// Idempotent Settlement + Persistence + Outbound Delivery
// ============================================================================

#[derive(Clone, Debug)]
pub(crate) struct SettleAndDeliverResult {
    pub turn: PendingRemoteTurn,
    pub settled_now: bool,
    pub delivered_reply: bool,
    pub notified_agent_id: Option<AgentId>,
}

/// Idempotently settle `turn_id` in `pending_remote_turns`, persist transcript + usage
/// records in PostgreSQL, update runtime & sandbox idle state, and trigger outbound
/// delivery (Telegram or parent/peer agent mailbox) strictly once.
pub(crate) fn settle_remote_turn_and_deliver(
    store: &Store,
    turn_id: &str,
    mut settlement: SettleRemoteTurn,
    session_id: Option<&str>,
    inflight_children: i32,
) -> Result<Option<SettleAndDeliverResult>, String> {
    if let Ok(Some(existing)) = store.remote_turn_store().get_turn(turn_id) {
        super::secrets::mask_settlement_for_agent(store, &existing.agent_id, &mut settlement);
    }
    let Some(outcome) = store
        .remote_turn_store()
        .settle_turn(turn_id, &settlement)
        .map_err(|e| format!("settle_turn failed: {e}"))?
    else {
        return Ok(None);
    };

    if !outcome.transitioned {
        return Ok(Some(SettleAndDeliverResult {
            turn: outcome.turn,
            settled_now: false,
            delivered_reply: false,
            notified_agent_id: None,
        }));
    }

    let turn = outcome.turn;
    let now = now_ms();
    let agent = store.agent(&turn.agent_id).ok();
    let harness_label = agent
        .as_ref()
        .map(|a| a.harness.clone())
        .or_else(|| {
            turn.metadata_json
                .as_ref()
                .and_then(|m| m.get("agent_harness").and_then(Value::as_str))
                .map(str::to_string)
        })
        .unwrap_or_else(|| turn.harness_kind.clone());
    let model_label = agent
        .as_ref()
        .and_then(|a| a.model.clone())
        .or_else(|| {
            turn.metadata_json
                .as_ref()
                .and_then(|m| m.get("agent_model").and_then(Value::as_str))
                .map(str::to_string)
        })
        .unwrap_or_else(|| "default".to_string());

    // 1. Persist vendor session ID pin into `agent_files` (shared across all replicas).
    if let Some(sid) = session_id.map(str::trim).filter(|s| !s.is_empty()) {
        let canonical = anyagents_harness::normalize_harness_name(&harness_label);
        let _ = store.blob_store().put_agent_file(
            &turn.agent_id,
            "session_pin",
            &format!("{canonical}-session-id"),
            format!("{sid}\n").as_bytes(),
            false,
        );
    }

    // 2. Persist turn transcript artifact into PG `agent_files` (`sessions/turns/<turn_id>.json`).
    let transcript_doc = json!({
        "turn_id": turn.turn_id,
        "agent_id": turn.agent_id.as_str(),
        "harness_kind": turn.harness_kind,
        "provider_kind": turn.provider_kind,
        "remote_run_id": turn.remote_run_id,
        "session_id": session_id,
        "status": turn.status,
        "result_text": turn.result_text,
        "error_text": turn.error_text,
        "usage": turn.usage_json,
        "settled_at_ms": now,
    });
    if let Ok(bytes) = serde_json::to_vec(&transcript_doc) {
        let _ = store.blob_store().put_agent_file(
            &turn.agent_id,
            "sessions",
            &format!("turns/{}.json", turn.turn_id),
            &bytes,
            false,
        );
    }

    // 3. Persist usage telemetry into `usage_records` when non-empty.
    if let Some(ref usage) = turn.usage_json {
        let input_tokens = usage
            .get("input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0) as i64;
        let output_tokens = usage
            .get("output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0) as i64;
        let cache_read_tokens = usage
            .get("cache_read_input_tokens")
            .or_else(|| usage.get("cache_read_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0) as i64;
        let cache_write_tokens = usage
            .get("cache_creation_input_tokens")
            .or_else(|| usage.get("cache_write_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0) as i64;
        let reported_cost_micros = usage
            .get("reported_cost_micros")
            .and_then(Value::as_i64)
            .or_else(|| {
                usage
                    .get("total_cost_usd")
                    .and_then(Value::as_f64)
                    .map(|usd| (usd * 1_000_000.0).round() as i64)
            });

        if input_tokens > 0
            || output_tokens > 0
            || cache_read_tokens > 0
            || cache_write_tokens > 0
            || reported_cost_micros.is_some()
        {
            let _ = store.record_usage(NewUsage {
                agent_id: turn.agent_id.clone(),
                harness: harness_label,
                model: Some(model_label),
                input_tokens,
                output_tokens,
                cache_read_tokens,
                cache_write_tokens,
                reported_cost_micros,
                estimated_cost_micros: reported_cost_micros,
                billing_mode: BillingMode::ApiKey,
                quota_state: None,
                day: utc_day(now),
            });
        }
    }

    // 4. Advance durable mailbox delivery cursor & turn terminal ledger.
    super::serverless_watches::settle_turn_mailbox_cursor(store, &turn);
    super::serverless_steer::settle_steered_turn_cursor(store, &turn);
    if turn.status != "completed"
        && let Some(seq) = turn
            .metadata_json
            .as_ref()
            .and_then(|m| m.get("message_seq").and_then(Value::as_i64))
        && super::serverless_failure::is_terminal_bootstrap_failure(
            turn.error_text.as_deref().unwrap_or(""),
        )
    {
        let _ = store.mark_delivered_up_to(&turn.agent_id, seq);
    }
    // 5. Reconcile open-turn locks, update `agent_runtime` heartbeat, and
    // detach crashed sandbox bindings (or mark healthy ones idle).
    super::serverless_failure::finalize_settled_turn_runtime_and_binding(
        store,
        &turn,
        now,
        inflight_children,
    );
    if turn.status == "completed" {
        super::serverless_maintenance::clear_dispatch_retry_state(store, None, &turn.agent_id);
    }

    // 6. Trigger outbound reply delivery or fail-closed parent error notification.
    let (delivered_reply, notified_agent_id) =
        super::serverless_failure::deliver_settled_turn_outcome(store, &turn, agent.as_ref());

    Ok(Some(SettleAndDeliverResult {
        turn,
        settled_now: true,
        delivered_reply,
        notified_agent_id,
    }))
}

// ============================================================================
// HTTP Endpoint 1: POST /v1/internal/guest/events
// ============================================================================

#[derive(Debug, Deserialize)]
pub(crate) struct GuestEventPayload {
    pub turn_id: String,
    pub event_type: String,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub timestamp_ms: Option<i64>,
    #[serde(default)]
    pub turn_state: Option<String>,
    #[serde(default)]
    pub active_turns: Option<u64>,
    #[serde(default)]
    pub queued_turns: Option<u64>,
    #[serde(default)]
    pub inflight_children: Option<i32>,
    #[serde(default)]
    pub busy: Option<bool>,
    #[serde(default)]
    pub seq: Option<u64>,
    #[serde(default)]
    pub message_id: Option<String>,
    #[serde(default)]
    pub message_seq: Option<i64>,
    #[serde(default)]
    pub proof: Option<String>,
    #[serde(default)]
    pub consumed_steers: Option<Vec<anyagents_guest::steer::ConsumedSteer>>,
    #[serde(default)]
    pub result_text: Option<String>,
    #[serde(default)]
    pub error_text: Option<String>,
    #[serde(default)]
    pub usage: Option<Value>,
    /// Optional base64-encoded allowlisted OAuth credential files refreshed natively
    /// by the vendor harness inside the sandbox during this turn.
    #[serde(default)]
    pub synced_auth_files: Option<BTreeMap<String, String>>,
}

fn is_syncable_auth_rel(rel: &str) -> bool {
    SYNCABLE_AUTH_FILES.contains(&rel)
}

fn auth_file_matches_profile_provider(rel: &str, provider: &str) -> bool {
    match rel {
        ".gemini/antigravity-cli/antigravity-oauth-token" => provider == "google-antigravity",
        ".claude/.credentials.json" | ".credentials.json" => provider == "anthropic",
        ".codex/auth.json" => provider == "openai",
        _ => false,
    }
}

fn harness_syncable_auth_candidates(harness: &str) -> &'static [&'static str] {
    match anyagents_harness::normalize_harness_name(harness) {
        "agy" => &[".gemini/antigravity-cli/antigravity-oauth-token"],
        "claude-code" => &[".claude/.credentials.json", ".credentials.json"],
        "codex" => &[".codex/auth.json"],
        _ => &[],
    }
}

fn extract_auth_token_and_expiry_ms(rel: &str, val: &Value) -> (Option<String>, Option<i64>) {
    match rel {
        ".gemini/antigravity-cli/antigravity-oauth-token" => {
            let tok = val
                .pointer("/token/access_token")
                .or_else(|| val.get("access_token"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            let expiry_ms = val
                .pointer("/token/expiry")
                .or_else(|| val.get("expiry"))
                .and_then(Value::as_str)
                .map(str::trim)
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.timestamp_millis());
            (tok, expiry_ms)
        }
        ".claude/.credentials.json" | ".credentials.json" => {
            let tok = val
                .pointer("/claudeAiOauth/accessToken")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            let expiry_ms = val
                .pointer("/claudeAiOauth/expiresAt")
                .and_then(Value::as_i64);
            (tok, expiry_ms)
        }
        ".codex/auth.json" => {
            let tok = val
                .pointer("/tokens/access_token")
                .or_else(|| val.get("OPENAI_API_KEY"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            let expiry_ms = val
                .get("last_refresh")
                .and_then(Value::as_str)
                .map(str::trim)
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.timestamp_millis());
            (tok, expiry_ms)
        }
        _ => (None, None),
    }
}

pub(super) fn is_incoming_auth_fresher(
    rel: &str,
    existing: Option<&[u8]>,
    incoming: &[u8],
) -> bool {
    let Ok(in_val) = serde_json::from_slice::<Value>(incoming) else {
        return false;
    };
    if !in_val.is_object() {
        return false;
    }
    let (in_tok, in_exp) = extract_auth_token_and_expiry_ms(rel, &in_val);
    if in_tok.is_none() {
        return false;
    }
    let Some(ex_bytes) = existing else {
        return true;
    };
    if ex_bytes == incoming {
        return false;
    }
    let Ok(ex_val) = serde_json::from_slice::<Value>(ex_bytes) else {
        return true;
    };
    let (ex_tok, ex_exp) = extract_auth_token_and_expiry_ms(rel, &ex_val);
    match (in_exp, ex_exp) {
        (Some(in_ms), Some(ex_ms)) => {
            if in_ms < ex_ms {
                return false;
            }
            if in_ms == ex_ms && in_tok == ex_tok {
                return false;
            }
            true
        }
        (None, Some(_)) => in_tok != ex_tok,
        _ => in_tok != ex_tok || in_exp != ex_exp,
    }
}

fn parse_valid_provider_profile(raw: &str) -> Option<(String, String)> {
    let trimmed = raw.trim();
    let (provider, profile) = trimmed.split_once('/')?;
    let provider = provider.trim();
    let profile = profile.trim();
    if provider.is_empty()
        || profile.is_empty()
        || provider.starts_with('.')
        || profile.starts_with('.')
        || profile.contains('/')
        || anyagents_core::stateless::validate_blob_path(&format!("{provider}/{profile}")).is_err()
    {
        return None;
    }
    Some((provider.to_string(), profile.to_string()))
}

fn resolve_agent_bound_profile_from_store(
    store: &Store,
    agent_id: &AgentId,
) -> Option<(String, String)> {
    let blobs = store.blob_store();
    for (ns, path) in [
        ("markers", ".anyagents-profile"),
        ("markers", ".anyagents-seeded-profile"),
        ("config", ".anyagents-profile"),
        ("config", ".anyagents-seeded-profile"),
    ] {
        if let Ok(Some(entry)) = blobs.get_agent_file(agent_id, ns, path)
            && let Ok(text) = String::from_utf8(entry.content)
            && let Some(pair) = parse_valid_provider_profile(&text)
        {
            return Some(pair);
        }
    }
    let cfg = anyagents_core::agent_config_dir(store.data_dir(), agent_id);
    crate::create::bound_profile_name(&cfg)
        .or_else(|| crate::create::seeded_profile_name(&cfg))
        .and_then(|s| parse_valid_provider_profile(&s))
}

fn decode_synced_auth_files(encoded: &BTreeMap<String, String>) -> BTreeMap<String, Vec<u8>> {
    let mut out = BTreeMap::new();
    for (rel, b64) in encoded {
        if !is_syncable_auth_rel(rel) {
            continue;
        }
        let Ok(bytes) = STANDARD.decode(b64.trim()) else {
            continue;
        };
        if bytes.is_empty() || bytes.len() > MAX_SYNCED_AUTH_FILE_BYTES {
            continue;
        }
        out.insert(rel.clone(), bytes);
    }
    out
}

/// Persist native-harness refreshed OAuth credential files back from a remote sandbox
/// into durable storage (`agent_files(secrets)` and the bound profile's
/// `deployment_blobs(profiles)` + local ephemeral profile mirror) under single-writer
/// monotonic freshness custody.
///
/// Returns the number of persisted updates across agent and profile stores.
pub(crate) fn persist_synced_auth_files(
    store: &Store,
    agent_id: &AgentId,
    files: &BTreeMap<String, Vec<u8>>,
) -> usize {
    if files.is_empty() {
        return 0;
    }
    let blobs = store.blob_store();
    let bound_profile = resolve_agent_bound_profile_from_store(store, agent_id);
    let cfg_dir = anyagents_core::agent_config_dir(store.data_dir(), agent_id);
    let mut updated = 0usize;

    for (rel, raw_bytes) in files {
        if !is_syncable_auth_rel(rel)
            || raw_bytes.is_empty()
            || raw_bytes.len() > MAX_SYNCED_AUTH_FILE_BYTES
        {
            continue;
        }
        // Validate JSON + non-empty token up front.
        if !is_incoming_auth_fresher(rel, None, raw_bytes) {
            continue;
        }

        // 1. Update agent's own encrypted `agent_files(secrets)` when fresher.
        let existing_agent = blobs
            .get_agent_file(agent_id, "secrets", rel)
            .ok()
            .flatten()
            .map(|e| e.content);
        if is_incoming_auth_fresher(rel, existing_agent.as_deref(), raw_bytes)
            && blobs
                .put_agent_file(agent_id, "secrets", rel, raw_bytes, true)
                .is_ok()
        {
            if cfg_dir.is_dir() {
                let _ = anyagents_harness::atomic_write_nonempty_file(
                    &cfg_dir.join(rel),
                    raw_bytes,
                    0o600,
                );
            }
            updated += 1;
        }

        // 2. Update bound provider profile in `deployment_blobs(profiles)` when fresher.
        if let Some((ref provider, ref profile)) = bound_profile
            && auth_file_matches_profile_provider(rel, provider)
        {
            let profile_blob_path = format!("{provider}/{profile}/{rel}");
            let existing_profile = blobs
                .get_deployment_blob("profiles", &profile_blob_path)
                .ok()
                .flatten()
                .map(|e| e.content);
            if is_incoming_auth_fresher(rel, existing_profile.as_deref(), raw_bytes)
                && blobs
                    .put_deployment_blob("profiles", &profile_blob_path, raw_bytes, true)
                    .is_ok()
            {
                let prof_dir = store
                    .data_dir()
                    .join("profiles")
                    .join(provider)
                    .join(profile);
                let _ = anyagents_harness::atomic_write_nonempty_file(
                    &prof_dir.join(rel),
                    raw_bytes,
                    0o600,
                );
                // Clear stale OAuth error/backoff caches locally and in Postgres so
                // `/v1/profiles` on every stateless replica immediately probes fresh quota.
                for stale_marker in [
                    ".anyagents-oauth-refresh-attempt-ms",
                    ".anyagents-last-usage.txt",
                ] {
                    let _ = std::fs::remove_file(prof_dir.join(stale_marker));
                    let _ = blobs.delete_deployment_blob(
                        "profiles",
                        &format!("{provider}/{profile}/{stale_marker}"),
                    );
                }
                updated += 1;
            }
        }
    }

    updated
}

/// Fallback sync-back for older sandbox images whose `anyagents-guest` binary does not yet
/// attach `synced_auth_files` to terminal callbacks: reads allowlisted OAuth files from the
/// active `ready` sandbox and persists any fresher native-refreshed token to Postgres.
async fn pull_sandbox_auth_files_if_needed(
    store: Arc<Store>,
    profiles: Arc<super::profiles::ProfileSnapshots>,
    agent_id: AgentId,
) {
    let store_r = Arc::clone(&store);
    let aid_r = agent_id.clone();
    let target = run_store_blocking(move || {
        let Ok(agent) = store_r.agent(&aid_r) else {
            return Ok(None);
        };
        let candidates = harness_syncable_auth_candidates(&agent.harness);
        if candidates.is_empty() {
            return Ok(None);
        }
        let Some(binding) = store_r.sandbox_binding(&aid_r).ok().flatten() else {
            return Ok(None);
        };
        if binding.lease_state != "ready"
            || binding.provider == "legacy_http"
            || binding.provider == "local"
        {
            return Ok(None);
        }
        let Some(handle) = sandbox_handle_from_binding(&binding) else {
            return Ok(None);
        };
        let reg = resolve_registry_for_binding(Some(store_r.data_dir()), &binding)?;
        Ok(Some((reg.active(), handle, candidates)))
    })
    .await;

    let Ok(Some((provider, handle, candidates))) = target else {
        return;
    };

    let mut fetched = BTreeMap::new();
    let state_root = handle.state_dir.trim_end_matches('/');
    for &rel in candidates {
        let req = ExecRequest::argv(vec!["cat".to_string(), format!("{state_root}/{rel}")])
            .with_timeout_ms(SANDBOX_AUTH_PULL_TIMEOUT_MS);
        if let Ok(out) = provider.exec(&handle, &req).await
            && out.exit_code == 0
            && !out.stdout.trim().is_empty()
            && out.stdout.len() <= MAX_SYNCED_AUTH_FILE_BYTES
        {
            fetched.insert(rel.to_string(), out.stdout.into_bytes());
        }
    }
    if fetched.is_empty() {
        return;
    }

    let store_w = Arc::clone(&store);
    let aid_w = agent_id.clone();
    if let Ok(updated) =
        run_store_blocking(move || Ok(persist_synced_auth_files(&store_w, &aid_w, &fetched))).await
        && updated > 0
    {
        profiles.invalidate(&store);
    }
}

fn is_secret_profile_rel(rel: &str) -> bool {
    let fname = rel.rsplit('/').next().unwrap_or(rel);
    fname.ends_with(".key")
        || fname == "antigravity-oauth-token"
        || fname == "credentials.json"
        || fname == ".credentials.json"
        || fname == "auth.json"
}

/// Persist all files under `<data>/profiles/<provider>/<profile>` into PostgreSQL
/// `deployment_blobs(namespace = 'profiles')`, encrypting credential files so every
/// stateless replica sees newly added API-key and OAuth profiles immediately.
pub(crate) fn persist_profile_dir_to_deployment_blobs(
    store: &Store,
    provider: &str,
    profile: &str,
) -> Result<usize, anyagents_core::Error> {
    let base_dir = store
        .data_dir()
        .join("profiles")
        .join(provider)
        .join(profile);
    if !base_dir.is_dir() {
        return Ok(0);
    }
    let blobs = store.blob_store();
    let mut stack = vec![base_dir.clone()];
    let mut persisted = 0usize;
    while let Some(dir) = stack.pop() {
        for entry_res in std::fs::read_dir(&dir)? {
            let entry = entry_res?;
            let path = entry.path();
            let meta = std::fs::symlink_metadata(&path)?;
            if meta.file_type().is_symlink() {
                continue;
            }
            if meta.is_dir() {
                stack.push(path);
                continue;
            }
            if !meta.is_file() {
                continue;
            }
            let Ok(rel_path) = path.strip_prefix(&base_dir) else {
                continue;
            };
            let rel = rel_path
                .components()
                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join("/");
            if rel.is_empty() || rel.ends_with(".tmp") || rel.ends_with(".new") {
                continue;
            }
            let blob_path = format!("{provider}/{profile}/{rel}");
            if anyagents_core::stateless::validate_blob_path(&blob_path).is_err() {
                continue;
            }
            let bytes = std::fs::read(&path)?;
            let encrypted = is_secret_profile_rel(&rel);
            blobs.put_deployment_blob("profiles", &blob_path, &bytes, encrypted)?;
            persisted += 1;
        }
    }
    for stale_marker in [
        ".anyagents-oauth-refresh-attempt-ms",
        ".anyagents-last-usage.txt",
    ] {
        let _ = blobs
            .delete_deployment_blob("profiles", &format!("{provider}/{profile}/{stale_marker}"));
    }
    Ok(persisted)
}

/// Persist allowlisted native-generated OAuth credential files from an ephemeral
/// serverless sign-in sandbox into `deployment_blobs(namespace = 'profiles')` and
/// the local ephemeral profile mirror, updating `profile.json` and clearing stale
/// OAuth backoff caches.
pub(crate) fn persist_signed_in_profile_auth_files(
    store: &Store,
    provider: &str,
    profile: &str,
    meta: &Value,
    files: &BTreeMap<String, Vec<u8>>,
) -> Result<usize, String> {
    if parse_valid_provider_profile(&format!("{provider}/{profile}")).is_none() {
        return Err(format!("invalid provider/profile `{provider}/{profile}`"));
    }
    let blobs = store.blob_store();
    let prof_dir = store
        .data_dir()
        .join("profiles")
        .join(provider)
        .join(profile);
    let _ = std::fs::create_dir_all(&prof_dir);
    let mut persisted = 0usize;

    for (rel, raw_bytes) in files {
        if !is_syncable_auth_rel(rel)
            || !auth_file_matches_profile_provider(rel, provider)
            || raw_bytes.is_empty()
            || raw_bytes.len() > MAX_SYNCED_AUTH_FILE_BYTES
        {
            continue;
        }
        if !is_incoming_auth_fresher(rel, None, raw_bytes) {
            continue;
        }
        let single_rel = rel.as_str();
        let targets: &[&str] = match (provider, single_rel) {
            ("openai", ".codex/auth.json") => &[".codex/auth.json", "auth.json"],
            ("anthropic", ".claude/.credentials.json" | ".credentials.json") => {
                &[".claude/.credentials.json", ".credentials.json"]
            }
            _ => std::slice::from_ref(&single_rel),
        };
        for &target_rel in targets {
            let blob_path = format!("{provider}/{profile}/{target_rel}");
            blobs
                .put_deployment_blob("profiles", &blob_path, raw_bytes, true)
                .map_err(|e| format!("failed to persist profile blob `{blob_path}`: {e}"))?;
            let _ = anyagents_harness::atomic_write_nonempty_file(
                &prof_dir.join(target_rel),
                raw_bytes,
                0o600,
            );
        }
        persisted += 1;
    }

    if persisted == 0 {
        return Ok(0);
    }

    let meta_bytes = serde_json::to_vec_pretty(meta).map_err(|err| err.to_string())?;
    blobs
        .put_deployment_blob(
            "profiles",
            &format!("{provider}/{profile}/profile.json"),
            &meta_bytes,
            false,
        )
        .map_err(|err| err.to_string())?;
    let _ = anyagents_harness::atomic_write_nonempty_file(
        &prof_dir.join("profile.json"),
        &meta_bytes,
        0o644,
    );
    for stale_marker in [
        ".anyagents-oauth-refresh-attempt-ms",
        ".anyagents-last-usage.txt",
    ] {
        let _ = std::fs::remove_file(prof_dir.join(stale_marker));
        let _ = blobs
            .delete_deployment_blob("profiles", &format!("{provider}/{profile}/{stale_marker}"));
    }
    Ok(persisted)
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    let raw = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .trim();
    raw.strip_prefix("Bearer ")
        .or_else(|| raw.strip_prefix("bearer "))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// `POST /v1/internal/guest/events`: authenticated detached guest push-callback ingress.
///
/// Verifies per-turn `Authorization: Bearer <callback-token>` via constant-time SHA-256
/// comparison against `pending_remote_turns.callback_token_hash` (fail-closed `401`).
pub(crate) async fn guest_events_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<GuestEventPayload>,
) -> Result<impl IntoResponse, ApiError> {
    let Some(token) = extract_bearer_token(&headers) else {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "missing Authorization: Bearer callback token",
        ));
    };
    if payload.turn_id.trim().is_empty() {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "missing turn_id"));
    }

    let store = Arc::clone(&state.store);
    let store_for_drain = Arc::clone(&state.store);
    let profiles_for_sync = Arc::clone(&state.profiles);
    let timer_for_close = Arc::clone(&state.timer_backend);
    let timer_for_drain = Arc::clone(&state.timer_backend);
    let normalized_event = payload.event_type.trim().to_ascii_lowercase();
    let is_completed_event = matches!(normalized_event.as_str(), "turn_complete" | "completed");
    let is_terminal_event = is_completed_event
        || matches!(
            normalized_event.as_str(),
            "turn_failed" | "failed" | "turn_cancelled" | "cancelled" | "canceled"
        );
    let (json_resp, settled_agent_id, notified_agent_id, synced_auth_updates) =
        spawn_store(move || {
            let Some(turn) = store
                .remote_turn_store()
                .verify_callback_token(&payload.turn_id, &token)
                .map_err(ApiError::from_store)?
            else {
                return Err(ApiError::new(
                    StatusCode::UNAUTHORIZED,
                    "invalid callback token for turn",
                ));
            };

            if let Some(ref claimed_aid) = payload.agent_id
                && claimed_aid.trim() != turn.agent_id.as_str()
            {
                return Err(ApiError::new(
                    StatusCode::UNAUTHORIZED,
                    "agent_id mismatch for callback token",
                ));
            }

            let synced_updates = if is_terminal_event
                && let Some(ref encoded) = payload.synced_auth_files
                && !encoded.is_empty()
            {
                let decoded = decode_synced_auth_files(encoded);
                persist_synced_auth_files(&store, &turn.agent_id, &decoded)
            } else {
                0
            };

            let ts = payload
                .timestamp_ms
                .filter(|&t| t > 0)
                .unwrap_or_else(now_ms);
            match payload.event_type.trim().to_ascii_lowercase().as_str() {
                "heartbeat" => {
                    let _ = store
                        .remote_turn_store()
                        .record_event(
                            &turn.turn_id,
                            &RemoteTurnEventUpdate {
                                status: Some("running".to_string()),
                                cursor: None,
                                remote_run_id: None,
                                last_event_at_ms: ts,
                                next_poll_at_ms: None,
                            },
                        )
                        .map_err(ApiError::from_store)?;

                    let active_turns = payload.active_turns.unwrap_or(1);
                    let queued_turns = payload.queued_turns.unwrap_or(0);
                    let inflight_children = payload.inflight_children.unwrap_or(0).max(0);
                    let turn_state = payload.turn_state.clone().unwrap_or_else(|| {
                        if active_turns > 0 || queued_turns > 0 {
                            "in_turn".to_string()
                        } else {
                            "idle".to_string()
                        }
                    });
                    let busy = payload.busy.unwrap_or(false)
                        || active_turns > 0
                        || queued_turns > 0
                        || inflight_children > 0
                        || turn_state != "idle";
                    let frame = GuestHeartbeatFrame {
                        timestamp_ms: ts,
                        turn_state,
                        active_turns,
                        queued_turns,
                        inflight_children,
                        busy,
                    };
                    apply_guest_heartbeat(&store, &turn.agent_id, &frame, ts)
                        .map_err(ApiError::from_store)?;

                    Ok((
                        Json(json!({
                            "ok": true,
                            "turn_id": turn.turn_id,
                            "event_type": "heartbeat",
                        })),
                        None::<(AgentId, bool)>,
                        None::<AgentId>,
                        0usize,
                    ))
                }
                "delta" => {
                    let cursor = payload.seq.map(|s| format!("delta:{s}"));
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
                    Ok((
                        Json(json!({
                            "ok": true,
                            "turn_id": turn.turn_id,
                            "event_type": "delta",
                        })),
                        None,
                        None,
                        0usize,
                    ))
                }
                "steer_accepted" => {
                    let resp = super::serverless_steer::handle_guest_steer_accepted(
                        &store, &turn, &payload, ts,
                    )?;
                    Ok((resp, None, None, 0usize))
                }
                "steer_consumed" => {
                    let resp = super::serverless_steer::handle_guest_steer_consumed(
                        &store, &turn, &payload, ts,
                    )?;
                    Ok((resp, None, None, 0usize))
                }
                "steer_refused" => {
                    let resp = super::serverless_steer::handle_guest_steer_refused(
                        &store, &turn, &payload, ts,
                    )?;
                    Ok((resp, None, None, 0usize))
                }
                "turn_complete" | "completed" => {
                    super::serverless_steer::apply_turn_complete_consumed_steers(
                        &store, &turn, &payload, ts,
                    );
                    let inflight = payload.inflight_children.unwrap_or(0).max(0);
                    let settled = settle_remote_turn_and_deliver(
                        &store,
                        &turn.turn_id,
                        SettleRemoteTurn {
                            status: "completed".to_string(),
                            result_text: payload.result_text,
                            error_text: None,
                            usage_json: payload.usage,
                            cursor: None,
                            settled_at_ms: ts,
                        },
                        payload.session_id.as_deref(),
                        inflight,
                    )
                    .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e))?
                    .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "turn not found"))?;
                    super::serverless_maintenance::cancel_turn_timers_on_close(
                        &store,
                        timer_for_close.as_ref(),
                        &settled.turn.agent_id,
                        &settled.turn.turn_id,
                        false,
                        inflight,
                    );
                    let drain_aid = settled
                        .settled_now
                        .then(|| (settled.turn.agent_id.clone(), true));
                    let notify_aid = settled.notified_agent_id;
                    Ok((
                        Json(json!({
                            "ok": true,
                            "turn_id": settled.turn.turn_id,
                            "status": settled.turn.status,
                            "settled": settled.settled_now,
                            "idempotent_replay": !settled.settled_now,
                            "delivered_reply": settled.delivered_reply,
                        })),
                        drain_aid,
                        notify_aid,
                        synced_updates,
                    ))
                }
                "turn_failed" | "failed" => {
                    let inflight = payload.inflight_children.unwrap_or(0).max(0);
                    let settled = settle_remote_turn_and_deliver(
                        &store,
                        &turn.turn_id,
                        SettleRemoteTurn {
                            status: "failed".to_string(),
                            result_text: None,
                            error_text: payload
                                .error_text
                                .or_else(|| Some("guest turn failed".to_string())),
                            usage_json: payload.usage,
                            cursor: None,
                            settled_at_ms: ts,
                        },
                        payload.session_id.as_deref(),
                        inflight,
                    )
                    .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e))?
                    .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "turn not found"))?;
                    if settled.settled_now
                        && super::runtime_issues::missing_guest_harness(
                            settled.turn.error_text.as_deref(),
                        )
                    {
                        super::runtime_issues::capture(
                            Arc::clone(&store),
                            Some(settled.turn.agent_id.clone()),
                            anyagents_core::RuntimeComponent::Harness,
                            "Guest harness could not start",
                            "No such file or directory (os error 2)",
                        );
                    }
                    super::serverless_maintenance::cancel_turn_timers_on_close(
                        &store,
                        timer_for_close.as_ref(),
                        &settled.turn.agent_id,
                        &settled.turn.turn_id,
                        false,
                        inflight,
                    );
                    let drain_aid = settled.settled_now.then(|| {
                        let drain_now = super::serverless_failure::should_drain_after_failed_turn(
                            &store,
                            &settled.turn,
                        );
                        (settled.turn.agent_id.clone(), drain_now)
                    });
                    let notify_aid = settled.notified_agent_id;
                    Ok((
                        Json(json!({
                            "ok": true,
                            "turn_id": settled.turn.turn_id,
                            "status": settled.turn.status,
                            "settled": settled.settled_now,
                            "idempotent_replay": !settled.settled_now,
                            "delivered_reply": settled.delivered_reply,
                        })),
                        drain_aid,
                        notify_aid,
                        synced_updates,
                    ))
                }
                "turn_cancelled" | "cancelled" | "canceled" => {
                    let inflight = payload.inflight_children.unwrap_or(0).max(0);
                    let settled = settle_remote_turn_and_deliver(
                        &store,
                        &turn.turn_id,
                        SettleRemoteTurn {
                            status: "cancelled".to_string(),
                            result_text: None,
                            error_text: payload.error_text,
                            usage_json: payload.usage,
                            cursor: None,
                            settled_at_ms: ts,
                        },
                        payload.session_id.as_deref(),
                        inflight,
                    )
                    .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e))?
                    .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "turn not found"))?;
                    super::serverless_maintenance::cancel_turn_timers_on_close(
                        &store,
                        timer_for_close.as_ref(),
                        &settled.turn.agent_id,
                        &settled.turn.turn_id,
                        false,
                        inflight,
                    );
                    let drain_aid = settled
                        .settled_now
                        .then(|| (settled.turn.agent_id.clone(), false));
                    Ok((
                        Json(json!({
                            "ok": true,
                            "turn_id": settled.turn.turn_id,
                            "status": settled.turn.status,
                            "settled": settled.settled_now,
                            "idempotent_replay": !settled.settled_now,
                        })),
                        drain_aid,
                        None,
                        synced_updates,
                    ))
                }
                other => Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    format!("unsupported guest event_type `{other}`"),
                )),
            }
        })
        .await?;
    if synced_auth_updates > 0 {
        state.profiles.invalidate(&state.store);
    }
    if is_terminal_event
        && is_serverless_mode()
        && let Some((drain_aid, should_drain)) = settled_agent_id
    {
        tokio::spawn(async move {
            if synced_auth_updates == 0 {
                pull_sandbox_auth_files_if_needed(
                    Arc::clone(&store_for_drain),
                    profiles_for_sync,
                    drain_aid.clone(),
                )
                .await;
            }
            if should_drain {
                super::serverless_maintenance::pump_agent_backlog_after_settlement(
                    Arc::clone(&store_for_drain),
                    Arc::clone(&timer_for_drain),
                    &drain_aid,
                )
                .await;
            }
            if let Some(notify_aid) = notified_agent_id {
                super::serverless_maintenance::pump_agent_backlog_after_settlement(
                    store_for_drain,
                    timer_for_drain,
                    &notify_aid,
                )
                .await;
            }
        });
    }
    Ok(json_resp)
}

// ============================================================================
// HTTP Endpoint 2: POST /v1/internal/cloud/events
// ============================================================================

#[derive(Debug, Default, Deserialize)]
pub(crate) struct CloudEventsQuery {
    #[serde(default)]
    pub driver: Option<String>,
}

fn headers_to_pairs(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .filter_map(|(k, v)| {
            v.to_str()
                .ok()
                .map(|val| (k.as_str().to_string(), val.to_string()))
        })
        .collect()
}

/// `POST /v1/internal/cloud/events`: authenticated push webhook ingress for Spiron and
/// vendor-neutral HTTP cloud harnesses.
///
/// Fails closed (`401`) unless authenticated by either:
/// 1. Per-turn `Authorization: Bearer <callback-token>` matching `callback_token_hash`
///    in constant time, OR
/// 2. Configured webhook HMAC-SHA256 / Bearer secret (`ANYAGENTS_SPIRON_WEBHOOK_SECRET`
///    or `ANYAGENTS_CLOUD_WEBHOOK_SECRET`).
pub(crate) async fn cloud_events_route(
    State(state): State<AppState>,
    Query(query): Query<CloudEventsQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let header_pairs = headers_to_pairs(&headers);
    let bearer_opt = extract_bearer_token(&headers);
    let driver_hint = query
        .driver
        .as_deref()
        .or_else(|| {
            headers
                .get("x-anyagents-cloud-driver")
                .and_then(|v| v.to_str().ok())
        })
        .map(|s| s.trim().to_ascii_lowercase());

    let store = Arc::clone(&state.store);
    let store_for_drain = Arc::clone(&state.store);
    let timer_for_close = Arc::clone(&state.timer_backend);
    let timer_for_drain = Arc::clone(&state.timer_backend);
    let (json_resp, settled_drain_aid) = spawn_store(move || {
        // Choose driver parser: explicit hint, or infer Spiron when Spiron signature header
        // or Spiron `message.*` event / `task_id` / `receipt_id` is present.
        let use_spiron = match driver_hint.as_deref() {
            Some("spiron" | "spiron-cloud" | "spiron_cloud") => true,
            Some("http_cloud" | "http-cloud" | "generic") => false,
            _ => {
                headers.contains_key("x-spiron-signature-256")
                    || serde_json::from_slice::<Value>(&body)
                        .ok()
                        .is_some_and(|v| {
                            let data = v.get("data").unwrap_or(&v);
                            data.get("task_id").is_some()
                                || data.get("receipt_id").is_some()
                                || v.get("event").and_then(Value::as_str).is_some_and(|e| {
                                    e.starts_with("message.") || e.starts_with("run.")
                                })
                        })
            }
        };

        let configured_secret = if use_spiron {
            env_non_empty("ANYAGENTS_SPIRON_WEBHOOK_SECRET")
                .or_else(|| env_non_empty("ANYAGENTS_CLOUD_WEBHOOK_SECRET"))
        } else {
            env_non_empty("ANYAGENTS_CLOUD_WEBHOOK_SECRET")
                .or_else(|| env_non_empty("ANYAGENTS_SPIRON_WEBHOOK_SECRET"))
        };

        // Check whether a configured shared webhook secret authenticates this request.
        let hmac_or_shared_ok = if let Some(ref secret) = configured_secret {
            let sig_ok = [
                "x-spiron-signature-256",
                "x-webhook-signature-256",
                "x-hub-signature-256",
            ]
            .iter()
            .filter_map(|name| headers.get(*name).and_then(|v| v.to_str().ok()))
            .any(|sig| verify_hmac_sha256_signature(secret.as_bytes(), &body, sig));
            let bearer_shared_ok = bearer_opt
                .as_deref()
                .is_some_and(|b| constant_time_eq(secret.as_bytes(), b.as_bytes()));
            sig_ok || bearer_shared_ok
        } else {
            false
        };

        // Parse payload through the canonical CloudTurnDriver::parse_webhook.
        let driver: Box<dyn CloudTurnDriver> = if use_spiron {
            Box::new(SpironServerlessDriver::new("https://unused.invalid", None))
        } else {
            Box::new(GenericHttpCloudDriver::new("https://unused.invalid", None))
        };
        let event = driver
            .parse_webhook(&header_pairs, &body)
            .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, e))?;

        // Locate the durable pending turn by `turn_id` first, else by `remote_run_id`.
        let rts = store.remote_turn_store();
        let turn_opt = if let Some(ref tid) = event.turn_id {
            rts.get_turn(tid).map_err(ApiError::from_store)?
        } else {
            None
        };
        let turn_opt = match turn_opt {
            Some(t) => Some(t),
            None => rts
                .find_by_remote_run_id(Some(&event.harness_kind), &event.remote_run_id)
                .map_err(ApiError::from_store)?
                .or_else(|| {
                    rts.find_by_remote_run_id(None, &event.remote_run_id)
                        .ok()
                        .flatten()
                }),
        };

        // Fail-closed authorization: must pass either configured webhook HMAC/secret OR
        // constant-time per-turn SHA-256 bearer token match!
        let Some(turn) = turn_opt else {
            return Err(ApiError::new(
                if hmac_or_shared_ok {
                    StatusCode::NOT_FOUND
                } else {
                    StatusCode::UNAUTHORIZED
                },
                if hmac_or_shared_ok {
                    "unknown cloud turn"
                } else {
                    "unauthorized cloud webhook"
                },
            ));
        };

        let per_turn_token_ok = bearer_opt
            .as_deref()
            .is_some_and(|tok| verify_callback_token_hash(&turn.callback_token_hash, tok));

        if !hmac_or_shared_ok && !per_turn_token_ok {
            return Err(ApiError::new(
                StatusCode::UNAUTHORIZED,
                "invalid cloud webhook signature or callback token",
            ));
        }

        let now = now_ms();
        if event.status.is_terminal() {
            let settled = settle_remote_turn_and_deliver(
                &store,
                &turn.turn_id,
                SettleRemoteTurn {
                    status: event.status.as_str().to_string(),
                    result_text: event.result_text,
                    error_text: event.error_text,
                    usage_json: event.usage,
                    cursor: event.cursor,
                    settled_at_ms: now,
                },
                None,
                0,
            )
            .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e))?
            .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "turn not found"))?;
            super::serverless_maintenance::cancel_turn_timers_on_close(
                &store,
                timer_for_close.as_ref(),
                &settled.turn.agent_id,
                &settled.turn.turn_id,
                true,
                0,
            );
            let drain_aid = settled.settled_now.then(|| settled.turn.agent_id.clone());
            Ok((
                Json(json!({
                    "ok": true,
                    "turn_id": settled.turn.turn_id,
                    "remote_run_id": event.remote_run_id,
                    "status": settled.turn.status,
                    "settled": settled.settled_now,
                    "idempotent_replay": !settled.settled_now,
                    "delivered_reply": settled.delivered_reply,
                })),
                drain_aid,
            ))
        } else {
            if let (Some(msg_id), Some(seq)) = (event.steer_message_id.as_deref(), event.steer_seq)
            {
                let proof = event.steer_proof.as_deref().unwrap_or("cloud:webhook");
                let commit = super::serverless_steer::apply_verified_steer_consumed(
                    &store,
                    &turn.turn_id,
                    msg_id,
                    seq,
                    proof,
                    now,
                )?;
                return Ok((
                    Json(json!({
                        "ok": true,
                        "turn_id": turn.turn_id,
                        "remote_run_id": event.remote_run_id,
                        "event_type": event.event_type,
                        "status": "running",
                        "settled": false,
                        "steer_consumed": true,
                        "message_id": msg_id,
                        "seq": seq,
                        "delivered_seq": commit.delivered_seq,
                    })),
                    None,
                ));
            }
            let _ = rts
                .record_event(
                    &turn.turn_id,
                    &RemoteTurnEventUpdate {
                        status: Some("running".to_string()),
                        cursor: event.cursor,
                        remote_run_id: Some(event.remote_run_id.clone()),
                        last_event_at_ms: now,
                        next_poll_at_ms: None,
                    },
                )
                .map_err(ApiError::from_store)?;
            Ok((
                Json(json!({
                    "ok": true,
                    "turn_id": turn.turn_id,
                    "remote_run_id": event.remote_run_id,
                    "status": "running",
                    "settled": false,
                })),
                None,
            ))
        }
    })
    .await?;

    if is_serverless_mode()
        && let Some(aid) = settled_drain_aid
    {
        tokio::spawn(async move {
            super::serverless_maintenance::pump_agent_backlog_after_settlement(
                store_for_drain,
                timer_for_drain,
                &aid,
            )
            .await;
        });
    }
    Ok(json_resp)
}

// ============================================================================
// Manual Operator Maintenance Pass (`POST /admin/api/tick`)
// ============================================================================

/// Structured JSON summary returned by `POST /admin/api/tick`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TickSummary {
    pub ok: bool,
    pub schedules_fired: usize,
    pub cloud_turns_polled: usize,
    pub turns_settled: usize,
    #[serde(default)]
    pub stale_open_turns_cleared: usize,
    #[serde(default)]
    pub backlog_dispatched: usize,
    pub leases_renewed: usize,
    pub sandboxes_swept: usize,
}

pub(crate) async fn run_tick_pass(
    store: Arc<Store>,
    health: anyagents_supervisor::HealthBoard,
) -> Result<TickSummary, ApiError> {
    let now = now_ms();
    let mut summary = TickSummary {
        ok: true,
        ..TickSummary::default()
    };

    // 1. Fire due schedules, watches & reminders, then dispatch woken agents in serverless mode.
    let (store_sched, health_sched) = (Arc::clone(&store), health.clone());
    let (schedules_fired, woken_agents) = spawn_store(move || {
        Ok(
            super::serverless_watches::tick_serverless_watches_and_schedules(
                &store_sched,
                &health_sched,
                now,
            ),
        )
    })
    .await?;
    summary.schedules_fired = schedules_fired;

    if is_serverless_mode() {
        for aid in woken_agents {
            let _ = dispatch_next_mailbox_message_if_serverless(Arc::clone(&store), &aid).await;
        }
    }

    // 2a. Poll due cloud turns (`next_poll_at_ms <= now`) via canonical maintenance helper.
    let store_due = Arc::clone(&store);
    let due_turns = spawn_store(move || {
        store_due
            .remote_turn_store()
            .list_due_polls(now, TICK_CLOUD_POLL_LIMIT)
            .map_err(ApiError::from_store)
    })
    .await?;

    let maint_timer = super::serverless_maintenance::resolve_timer_backend(None, store.data_dir());
    for turn in due_turns {
        match super::serverless_maintenance::poll_cloud_turn_once(
            Arc::clone(&store),
            Arc::clone(&maint_timer),
            turn,
            true,
        )
        .await
        {
            Ok(super::serverless_maintenance::CloudPollOutcome::Skipped) => {}
            Ok(super::serverless_maintenance::CloudPollOutcome::Settled { settled_now }) => {
                summary.cloud_turns_polled += 1;
                if settled_now {
                    summary.turns_settled += 1;
                }
            }
            Ok(super::serverless_maintenance::CloudPollOutcome::StillRunning { .. }) => {
                summary.cloud_turns_polled += 1;
            }
            Err(err) => {
                return Err(ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, err));
            }
        }
    }

    // 2b. Mark stale/dead sandbox turns whose heartbeats exceeded the stale threshold.
    let stale_before = now.saturating_sub(sandbox_stale_threshold_ms());
    let store_stale = Arc::clone(&store);
    let (stale_settled, wake_after_timeout) = spawn_store(move || {
        let stale = store_stale
            .remote_turn_store()
            .list_stale_active(Some("sandbox"), stale_before, TICK_STALE_SANDBOX_LIMIT)
            .map_err(ApiError::from_store)?;
        let mut count = 0usize;
        let mut wake = Vec::new();
        for turn in stale {
            if super::serverless_failure::settle_timed_out_sandbox_turn(
                &store_stale,
                &turn,
                now_ms(),
            ) {
                count += 1;
                if super::serverless_failure::should_drain_after_failed_turn(&store_stale, &turn) {
                    wake.push(turn.agent_id);
                }
            }
        }
        Ok((count, wake))
    })
    .await?;
    summary.turns_settled += stale_settled;

    // 2c. Reconcile orphaned `turn_recovery` locks (`has_open_turn=true` with zero active
    // remote turns) and drain queued undelivered mailbox messages for idle running agents.
    if is_serverless_mode() {
        let store_rec = Arc::clone(&store);
        let (cleared, mut pending_agents) = spawn_store(move || {
            let cleared = store_rec
                .reconcile_all_stale_serverless_open_turns()
                .unwrap_or(0);
            let agents = store_rec
                .list_running_agents_with_undelivered(24)
                .unwrap_or_default();
            Ok((cleared, agents))
        })
        .await?;
        for aid in wake_after_timeout {
            if !pending_agents.contains(&aid) {
                pending_agents.push(aid);
            }
        }
        summary.stale_open_turns_cleared = cleared;

        for aid in pending_agents {
            let store_disp = Arc::clone(&store);
            let handle = tokio::spawn(async move {
                match dispatch_next_mailbox_message_if_serverless(store_disp, &aid).await {
                    Ok(res) => Ok(res),
                    Err(err) => {
                        eprintln!(
                            "anyagents-serverless: tick backlog dispatch failed for `{aid}`: {err}"
                        );
                        Err(err)
                    }
                }
            });
            if let Ok(Ok(Ok(Some(_)))) =
                tokio::time::timeout(Duration::from_millis(750), handle).await
            {
                summary.backlog_dispatched += 1;
            }
        }
    }

    // 3. Renew leases for active non-idle sandbox bindings.
    let store_renew = Arc::clone(&store);
    let renewed = spawn_store(move || {
        let ready = store_renew
            .sandbox_binding_store()
            .list_active_ready()
            .map_err(ApiError::from_store)?;
        let mut count = 0usize;
        for binding in ready {
            if !super::serverless_maintenance::binding_is_active(&store_renew, &binding) {
                continue;
            }
            let due_for_renew = binding
                .lease_expires_at_ms
                .is_none_or(|exp| exp <= now.saturating_add(DEFAULT_LEASE_RENEW_WINDOW_MS));
            if !due_for_renew {
                continue;
            }
            if super::serverless_maintenance::renew_binding_lease(&store_renew, &binding) {
                count += 1;
            }
        }
        Ok(count)
    })
    .await?;
    summary.leases_renewed = renewed;

    // 4. Sweep idle sandboxes (`inflight_children == 0` and no active turn) under `maint:singleton`.
    let store_sweep = Arc::clone(&store);
    let swept = spawn_store(move || {
        let _singleton = if store_sweep.is_postgres() {
            match store_sweep
                .try_acquire_coordinator_lease("maint:singleton", Duration::from_secs(30))
            {
                Ok(Some(guard)) => Some(guard),
                Ok(None) => return Ok(0usize),
                Err(_) => return Ok(0usize),
            }
        } else {
            None
        };
        let report = sweep_idle_sandboxes(&store_sweep, store_sweep.data_dir(), now)
            .map_err(ApiError::from_store)?;
        Ok(report.released.len())
    })
    .await?;
    summary.sandboxes_swept = swept;

    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::super::serverless_failure::is_suppressed_fallback_reply;
    use super::super::timer_fire::clear_jwks_cache;
    use super::*;
    use crate::sandbox_providers::providers::{
        EnsureSandboxRequest, ExecOutput, ProviderHealth, ProviderKind, SandboxCapabilities,
        SandboxHandle,
    };
    use anyagents_core::{Incoming, NewAgent, NewSchedule, hmac_sha256_hex};
    use anyagents_supervisor::HealthBoard;
    use axum::body::Body;
    use axum::http::Request;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use http_body_util::BodyExt as _;
    use ring::rand::SystemRandom;
    use ring::signature::{Ed25519KeyPair, KeyPair as _};
    use sha2::{Digest, Sha256};
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tower::ServiceExt as _;

    fn test_pg_url() -> Option<String> {
        std::env::var("ANYAGENTS_TEST_POSTGRES_URL")
            .or_else(|_| std::env::var("TEST_DATABASE_URL"))
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    fn make_app_state(store: Arc<Store>, data_dir: &Path) -> AppState {
        AppState {
            store,
            health: HealthBoard::default(),
            sign_in: Arc::new(Mutex::new(super::super::ProcessRunner)),
            auth: crate::auth::Mode::Trusted {
                data_dir: data_dir.to_path_buf(),
            },
            identity: Arc::new(crate::identity::ServingIdentity::capture()),
            profiles: Default::default(),
            timer_backend: Arc::new(crate::timer::NoopTimerBackend),
        }
    }

    /// Mock SandboxProvider that runs a real local `anyagents-guest turn --detached` command
    /// inside an isolated temp sandbox root so scale-to-zero tests prove end-to-end callback
    /// delivery across router drops!
    struct RealGuestExecMockProvider {
        root: std::path::PathBuf,
        released: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl AsyncSandboxProvider for RealGuestExecMockProvider {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Http
        }

        fn capabilities(&self) -> SandboxCapabilities {
            SandboxCapabilities {
                persistent_volumes: true,
                volume_survives_release: true,
                ..SandboxCapabilities::default()
            }
        }

        async fn ensure(&self, req: &EnsureSandboxRequest) -> Result<SandboxHandle, String> {
            let state_dir = self.root.join("state");
            let workdir = self.root.join("workspace");
            std::fs::create_dir_all(&state_dir).map_err(|e| e.to_string())?;
            std::fs::create_dir_all(&workdir).map_err(|e| e.to_string())?;
            Ok(SandboxHandle {
                provider: ProviderKind::Http,
                sandbox_id: format!("sbx-{}", req.agent_id),
                volume_id: Some(format!("vol-{}", req.agent_id)),
                mount_path: self.root.display().to_string(),
                state_dir: state_dir.display().to_string(),
                workdir: workdir.display().to_string(),
                transport_url: None,
                transport_token: None,
                expires_at: None,
                metadata: BTreeMap::new(),
            })
        }

        async fn exec(
            &self,
            _handle: &SandboxHandle,
            req: &ExecRequest,
        ) -> Result<ExecOutput, String> {
            use std::io::Write as _;
            use std::process::{Command, Stdio};

            let (prog, args) = req
                .command
                .split_first()
                .ok_or_else(|| "empty exec argv".to_string())?;
            let mut cmd = Command::new(prog);
            cmd.args(args)
                .env("ANYAGENTS_GUEST_CALLBACK_BACKOFF_MS", "20")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            let mut child = cmd.spawn().map_err(|e| format!("spawn {prog}: {e}"))?;
            if let Some(ref stdin_bytes) = req.stdin
                && let Some(mut stdin_pipe) = child.stdin.take()
            {
                let _ = stdin_pipe.write_all(stdin_bytes);
            }
            let out = child.wait_with_output().map_err(|e| e.to_string())?;
            Ok(ExecOutput {
                exit_code: out.status.code().unwrap_or(-1),
                stdout: String::from_utf8_lossy(&out.stdout).to_string(),
                stderr: String::from_utf8_lossy(&out.stderr).to_string(),
            })
        }

        async fn sync_bundle_tar(
            &self,
            _handle: &SandboxHandle,
            _dest_dir: &str,
            _tar_gz: &[u8],
        ) -> Result<(), String> {
            Ok(())
        }

        async fn release(&self, handle: &SandboxHandle) -> Result<(), String> {
            self.released
                .lock()
                .unwrap()
                .push(handle.sandbox_id.clone());
            Ok(())
        }

        async fn destroy(&self, _handle: &SandboxHandle) -> Result<(), String> {
            Ok(())
        }

        async fn health(&self) -> Result<ProviderHealth, String> {
            Ok(ProviderHealth {
                provider: ProviderKind::Http,
                healthy: true,
                message: "ok".into(),
                latency_ms: Some(1),
            })
        }
    }

    fn sign_ed25519_jws(kp: &Ed25519KeyPair, kid: &str, claims: &Value) -> String {
        let header = json!({ "alg": "EdDSA", "typ": "JWT", "kid": kid });
        let h_b64 = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).unwrap());
        let p_b64 = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).unwrap());
        let signing_input = format!("{h_b64}.{p_b64}");
        let sig = kp.sign(signing_input.as_bytes());
        let s_b64 = URL_SAFE_NO_PAD.encode(sig.as_ref());
        format!("{signing_input}.{s_b64}")
    }

    fn jwks_doc_for(keys: &[(&str, &Ed25519KeyPair)]) -> Value {
        let arr: Vec<Value> = keys
            .iter()
            .map(|(kid, kp)| {
                json!({
                    "kty": "OKP",
                    "crv": "Ed25519",
                    "use": "sig",
                    "alg": "EdDSA",
                    "kid": kid,
                    "x": URL_SAFE_NO_PAD.encode(kp.public_key().as_ref()),
                })
            })
            .collect();
        json!({ "keys": arr })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)]
    async fn serverless_scale_to_zero_restart_survival_sandbox_turn_pg_and_local() {
        let _env = crate::server::tests::env_lock();
        let dir_a = crate::test_tempdir();
        let dir_b = crate::test_tempdir();
        let sbx_root = crate::test_tempdir();

        // Use real PostgreSQL 18 when ANYAGENTS_TEST_POSTGRES_URL is set, else shared local dir.
        let pg_url = test_pg_url();
        let tag = format!("{}_{}", std::process::id(), now_ms());
        let (store_a, shared_dir_for_b) = match pg_url.as_deref() {
            Some(url) => (
                Arc::new(
                    Store::open_postgres_isolated(url, dir_a.path(), format!("sl-a-{tag}"))
                        .expect("open PG replica A"),
                ),
                dir_b.path().to_path_buf(),
            ),
            None => (
                Arc::new(Store::open(dir_a.path().join("state.db")).unwrap()),
                dir_a.path().to_path_buf(),
            ),
        };

        let parent = store_a
            .create_agent(NewAgent::new(format!("sl_parent_{tag}"), "claude-code"))
            .unwrap();
        let worker = store_a
            .create_agent(NewAgent::new(format!("sl_worker_{tag}"), "claude-code"))
            .unwrap();

        // Bind a stable listener port that Router A will NOT serve — instead, we simulate
        // complete scale-to-zero by dropping Router A & Store A, then binding Router B on
        // the callback address so the real `anyagents-guest` detached child (or callback)
        // settles directly into Router B!
        let listener_b = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_b = listener_b.local_addr().unwrap();
        let public_url = format!("http://{addr_b}");

        // Prepare a fake Claude CLI script for our detached guest runner.
        let fake_claude = sbx_root.path().join("fake-claude");
        std::fs::write(
            &fake_claude,
            r#"#!/bin/sh
echo '{"type":"system","subtype":"init","session_id":"s2z-sess-1","claude_code_version":"1.0","peer":"claude-code 1.0"}'
while IFS= read -r line; do
  case "$line" in
    *'"type":"user"'*)
      uuid=$(printf '%s\n' "$line" | sed -n 's/.*"uuid":"\([^"]*\)".*/\1/p')
      printf '{"type":"user","isReplay":true,"uuid":"%s","message":{"role":"user","content":[{"type":"text","text":"work"}]}}\n' "$uuid"
      sleep 0.15
      printf '{"type":"assistant","session_id":"s2z-sess-1","message":{"role":"assistant","content":[{"type":"text","text":"survived scale to zero"}]}}\n'
      printf '{"type":"result","subtype":"success","is_error":false,"session_id":"s2z-sess-1","user_message_uuids":["%s"],"result":"survived scale to zero","usage":{"input_tokens":42,"output_tokens":17},"total_cost_usd":0.05}\n' "$uuid"
      ;;
  esac
done
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&fake_claude, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        // Router A receives inbound message & dispatches detached turn via Provider.
        let router_a =
            super::super::build_public_router(make_app_state(Arc::clone(&store_a), dir_a.path()));
        let _ = router_a; // Proves Router A existed and is dropped before callback arrives.

        let released = Arc::new(Mutex::new(Vec::new()));
        let provider: Arc<dyn AsyncSandboxProvider> = Arc::new(RealGuestExecMockProvider {
            root: sbx_root.path().to_path_buf(),
            released: Arc::clone(&released),
        });

        let msg = match store_a
            .receive(&worker.id, Incoming::new("Execute serverless task"))
            .unwrap()
        {
            anyagents_core::ReceiveOutcome::New(m) => m,
            anyagents_core::ReceiveOutcome::Duplicate(m) => m,
        };

        // Locate compiled `anyagents` multi-call binary (or `anyagents-guest` if sibling exists).
        let guest_bin = std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().and_then(Path::parent).map(Path::to_path_buf))
            .map(|target_debug| target_debug.join("anyagents-guest"))
            .filter(|p| p.is_file());

        let dispatched = if let Some(ref gbin) = guest_bin {
            let mut input = ServerlessDispatchInput::for_agent(worker.clone(), msg.text.clone());
            input.message_seq = Some(msg.seq);
            input.opener_id = Some(msg.id.to_string());
            input.reply_target = Some(ReplyTarget::Agent {
                agent_id: parent.id.as_str().to_string(),
            });
            input.callback_base_url = Some(public_url.clone());
            input.sandbox_provider = Some(Arc::clone(&provider));
            input.guest_binary = Some(gbin.display().to_string());
            input.guest_program = Some(fake_claude.display().to_string());
            input.guest_extra_args = vec!["--guest-bare".to_string()];
            input.heartbeat_interval_ms = Some(40);
            dispatch_serverless_turn(Arc::clone(&store_a), input)
                .await
                .expect("Router A detached sandbox dispatch")
        } else {
            // Even when running unit tests without sibling `anyagents-guest` binary prebuilt,
            // exercise the full Router A persist -> drop -> Router B HTTP callback path!
            let turn_id = generate_turn_id();
            let callback_token = generate_callback_token();
            let turn = store_a
                .remote_turn_store()
                .create_turn(&NewRemoteTurn {
                    turn_id,
                    agent_id: worker.id.clone(),
                    harness_kind: "sandbox".into(),
                    provider_kind: Some("http".into()),
                    remote_run_id: Some("pid:9999".into()),
                    callback_token_hash: hash_callback_token(&callback_token),
                    cursor: None,
                    status: "running".into(),
                    metadata_json: Some(json!({
                        "agent_harness": "claude-code",
                        "message_seq": msg.seq,
                        "opener_id": msg.id.to_string(),
                        "reply_target": { "kind": "agent", "agent_id": parent.id.as_str() }
                    })),
                    next_poll_at_ms: None,
                })
                .unwrap();
            let _ = store_a.note_delivered(&worker.id, msg.seq);
            DispatchedRemoteTurn {
                turn,
                callback_token,
            }
        };

        let turn_id = dispatched.turn.turn_id.clone();
        let callback_token = dispatched.callback_token.clone();

        // === SCALE TO ZERO: completely drop Router A & Store A! ===
        drop(store_a);
        drop(provider);

        // === COLD START: spin up brand-new stateless Router B sharing ONLY the database! ===
        let store_b = match pg_url.as_deref() {
            Some(url) => Arc::new(
                Store::open_postgres_isolated(url, &shared_dir_for_b, format!("sl-b-{tag}"))
                    .expect("open PG replica B"),
            ),
            None => Arc::new(Store::open(shared_dir_for_b.join("state.db")).unwrap()),
        };
        let router_b = super::super::build_public_router(make_app_state(
            Arc::clone(&store_b),
            &shared_dir_for_b,
        ));

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server_task = tokio::spawn(async move {
            let _ = axum::serve(listener_b, router_b)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        if guest_bin.is_none() {
            // Post heartbeat + turn_complete directly to Router B over real HTTP!
            let url = format!("{public_url}/v1/internal/guest/events");
            let hb_body = json!({
                "event_type": "heartbeat",
                "turn_id": turn_id,
                "agent_id": worker.id.as_str(),
                "active_turns": 1,
                "inflight_children": 2,
                "busy": true,
                "timestamp_ms": now_ms(),
            });
            let complete_body = json!({
                "event_type": "turn_complete",
                "turn_id": turn_id,
                "agent_id": worker.id.as_str(),
                "session_id": "s2z-sess-1",
                "result_text": "survived scale to zero",
                "usage": { "input_tokens": 42, "output_tokens": 17, "reported_cost_micros": 50000 },
                "inflight_children": 0,
                "timestamp_ms": now_ms(),
            });
            let tok_clone = callback_token.clone();
            tokio::task::spawn_blocking(move || {
                let agent: ureq::Agent = ureq::Agent::config_builder()
                    .http_status_as_error(false)
                    .build()
                    .into();
                let r1 = agent
                    .post(&url)
                    .header("authorization", &format!("Bearer {tok_clone}"))
                    .send_json(&hb_body)
                    .unwrap();
                assert_eq!(r1.status().as_u16(), 200);
                let r2 = agent
                    .post(&url)
                    .header("authorization", &format!("Bearer {tok_clone}"))
                    .send_json(&complete_body)
                    .unwrap();
                assert_eq!(r2.status().as_u16(), 200);
            })
            .await
            .unwrap();
        }

        // Wait for settlement in PG / shared store on Router B.
        let deadline = Instant::now() + Duration::from_secs(10);
        let settled = loop {
            if let Some(t) = store_b.remote_turn_store().get_turn(&turn_id).unwrap()
                && t.is_terminal()
            {
                break t;
            }
            assert!(
                Instant::now() < deadline,
                "turn `{turn_id}` did not settle on Router B within 10s"
            );
            tokio::time::sleep(Duration::from_millis(30)).await;
        };

        assert_eq!(settled.status, "completed");
        assert_eq!(
            settled.result_text.as_deref(),
            Some("survived scale to zero")
        );
        assert_eq!(settled.usage_json.as_ref().unwrap()["input_tokens"], 42);
        assert_eq!(settled.usage_json.as_ref().unwrap()["output_tokens"], 17);

        // Verify durable PG transcript & usage records & parent reply delivery on Router B!
        let transcript = store_b
            .blob_store()
            .get_agent_file(&worker.id, "sessions", &format!("turns/{turn_id}.json"))
            .unwrap()
            .expect("transcript persisted in PG agent_files");
        let transcript_val: Value = serde_json::from_slice(&transcript.content).unwrap();
        assert_eq!(transcript_val["result_text"], "survived scale to zero");

        let usage_rows = store_b.list_usage(Some(&worker.id)).unwrap();
        assert_eq!(usage_rows.len(), 1);
        assert_eq!(usage_rows[0].input_tokens, 42);
        assert_eq!(usage_rows[0].output_tokens, 17);

        let parent_inbox = store_b.inbox(&parent.id, None).unwrap();
        assert_eq!(parent_inbox.len(), 1);
        assert_eq!(parent_inbox[0].text, "survived scale to zero");

        // Duplicate replay of turn_complete must be strictly idempotent (zero duplicate usage/replies).
        let replay_url = format!("{public_url}/v1/internal/guest/events");
        let tid_replay = turn_id.clone();
        let tok_replay = callback_token.clone();
        let replay_status = tokio::task::spawn_blocking(move || {
            let agent: ureq::Agent = ureq::Agent::config_builder()
                .http_status_as_error(false)
                .build()
                .into();
            let mut resp = agent
                .post(&replay_url)
                .header("authorization", &format!("Bearer {tok_replay}"))
                .send_json(json!({
                    "event_type": "turn_complete",
                    "turn_id": tid_replay,
                    "result_text": "duplicate overwrite attempt",
                    "usage": { "input_tokens": 999, "output_tokens": 999 }
                }))
                .unwrap();
            let status = resp.status().as_u16();
            let body: Value = resp.body_mut().read_json().unwrap();
            (status, body)
        })
        .await
        .unwrap();
        assert_eq!(replay_status.0, 200);
        assert_eq!(replay_status.1["idempotent_replay"], true);
        assert_eq!(store_b.list_usage(Some(&worker.id)).unwrap().len(), 1);
        assert_eq!(store_b.inbox(&parent.id, None).unwrap().len(), 1);

        let _ = shutdown_tx.send(());
        let _ = server_task.await;
        let _ = store_b.delete_agent(&worker.id);
        let _ = store_b.delete_agent(&parent.id);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)]
    async fn serverless_cloud_spiron_and_generic_http_survive_restart_via_webhook_and_tick_idempotently()
     {
        let _env = crate::server::tests::env_lock();
        let dir_a = crate::test_tempdir();
        let dir_b = crate::test_tempdir();
        let pg_url = test_pg_url();
        let tag = format!("{}_{}", std::process::id(), now_ms());

        let (store_a, shared_dir_for_b) = match pg_url.as_deref() {
            Some(url) => (
                Arc::new(
                    Store::open_postgres_isolated(url, dir_a.path(), format!("cld-a-{tag}"))
                        .expect("open PG replica A"),
                ),
                dir_b.path().to_path_buf(),
            ),
            None => (
                Arc::new(Store::open(dir_a.path().join("state.db")).unwrap()),
                dir_a.path().to_path_buf(),
            ),
        };

        // Spin up mock Spiron server (for tick polling fallback) AND mock Generic HTTP Cloud server (for webhook push).
        let mock_spiron = axum::Router::new()
            .route(
                "/v1/agents",
                axum::routing::post(|| async { Json(json!({ "id": "spiron_agt_99" })) }),
            )
            .route(
                "/v1/agents/{aid}/messages",
                axum::routing::post(
                    |axum::extract::Path(aid): axum::extract::Path<String>| async move {
                        Json(json!({
                            "id": "rcpt_spiron_tick_1",
                            "agent_id": aid,
                            "status": "submitted"
                        }))
                    },
                ),
            )
            .route(
                "/v1/message-receipts/{task_id}",
                axum::routing::get(
                    |axum::extract::Path(task_id): axum::extract::Path<String>| async move {
                        Json(json!({
                            "id": task_id,
                            "status": "completed",
                            "reply": "settled via tick poll",
                            "usage": { "input_tokens": 30, "output_tokens": 12 }
                        }))
                    },
                ),
            )
            .route(
                "/v1/message-receipts/{task_id}/events",
                axum::routing::get(|| async {
                    Json(json!({ "next_cursor": "ev_done", "events": [] }))
                }),
            );
        let spiron_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let spiron_addr = spiron_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(spiron_listener, mock_spiron).await;
        });

        let mock_generic = axum::Router::new()
            .route(
                "/turns",
                axum::routing::post(|| async {
                    (
                        StatusCode::ACCEPTED,
                        Json(json!({ "id": "gen_run_webhook_1", "status": "running" })),
                    )
                }),
            )
            .route(
                "/turns/{id}",
                axum::routing::get(
                    |axum::extract::Path(id): axum::extract::Path<String>| async move {
                        Json(json!({
                            "id": id,
                            "status": "completed",
                            "result_text": "late tick poll should be ignored"
                        }))
                    },
                ),
            );
        let generic_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let generic_addr = generic_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(generic_listener, mock_generic).await;
        });

        // Router A creates agents and submits both cloud turns.
        let spiron_agent = store_a
            .create_agent(NewAgent::new(format!("spiron_cld_{tag}"), "spiron-cloud"))
            .unwrap();
        let generic_agent = store_a
            .create_agent(NewAgent::new(format!("http_cld_{tag}"), "http-cloud"))
            .unwrap();

        let mut spiron_req =
            ServerlessDispatchInput::for_agent(spiron_agent.clone(), "Hello Spiron");
        spiron_req.callback_base_url = Some("https://coord.example".into());
        spiron_req.cloud_endpoint = Some(format!("http://{spiron_addr}"));
        let spiron_dispatched = dispatch_serverless_turn(Arc::clone(&store_a), spiron_req)
            .await
            .expect("Router A submits Spiron turn");
        assert_eq!(
            spiron_dispatched.turn.remote_run_id.as_deref(),
            Some("rcpt_spiron_tick_1")
        );

        let mut generic_req =
            ServerlessDispatchInput::for_agent(generic_agent.clone(), "Hello Generic Cloud");
        generic_req.callback_base_url = Some("https://coord.example".into());
        generic_req.cloud_endpoint = Some(format!("http://{generic_addr}"));
        let generic_dispatched = dispatch_serverless_turn(Arc::clone(&store_a), generic_req)
            .await
            .expect("Router A submits Generic HTTP Cloud turn");
        assert_eq!(
            generic_dispatched.turn.remote_run_id.as_deref(),
            Some("gen_run_webhook_1")
        );

        // Make Spiron turn immediately due for tick polling.
        store_a
            .remote_turn_store()
            .schedule_next_poll(&spiron_dispatched.turn.turn_id, Some(1), None, now_ms())
            .unwrap();

        // === SCALE TO ZERO: drop Router A & Store A completely! ===
        drop(store_a);

        // === COLD START: spin up brand-new Router B sharing ONLY the database! ===
        let store_b = match pg_url.as_deref() {
            Some(url) => Arc::new(
                Store::open_postgres_isolated(url, &shared_dir_for_b, format!("cld-b-{tag}"))
                    .expect("open PG replica B"),
            ),
            None => Arc::new(Store::open(shared_dir_for_b.join("state.db")).unwrap()),
        };

        let prev_tick_tok = std::env::var_os("ANYAGENTS_TICK_TOKEN");
        let prev_wh_sec = std::env::var_os("ANYAGENTS_CLOUD_WEBHOOK_SECRET");
        unsafe {
            std::env::set_var("ANYAGENTS_TICK_TOKEN", "tick-secret-cloud-test");
            std::env::set_var("ANYAGENTS_CLOUD_WEBHOOK_SECRET", "whsec-cloud-test");
        }

        let router_b = super::super::build_public_router(make_app_state(
            Arc::clone(&store_b),
            &shared_dir_for_b,
        ));

        // 1. Router B handles `/v1/internal/cloud/events` webhook push for Generic HTTP Cloud!
        let wh_body = serde_json::to_vec(&json!({
            "event_type": "turn_complete",
            "turn_id": generic_dispatched.turn.turn_id,
            "remote_run_id": "gen_run_webhook_1",
            "status": "completed",
            "result_text": "settled via webhook push",
            "usage": { "input_tokens": 15, "output_tokens": 8 }
        }))
        .unwrap();
        let sig = format!("sha256={}", hmac_sha256_hex(b"whsec-cloud-test", &wh_body));
        let wh_resp = router_b
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/internal/cloud/events?driver=http_cloud")
                    .header("content-type", "application/json")
                    .header("x-webhook-signature-256", sig)
                    .body(Body::from(wh_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(wh_resp.status(), StatusCode::OK);
        let wh_json: Value =
            serde_json::from_slice(&wh_resp.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(wh_json["settled"], true);

        // 2. Create a due schedule on Router B and fire it via event-driven
        //    `POST /v1/internal/fire/{id}` (confirming legacy `/v1/internal/tick` is 404),
        //    then settle the due Spiron cloud turn via `run_tick_pass`.
        let due_sched = store_b
            .create_schedule(NewSchedule {
                agent_id: spiron_agent.id.clone(),
                kind: "at".into(),
                spec: "now".into(),
                prompt: "event-driven reminder".into(),
                interval_ms: None,
                next_fire_at_ms: Some(now_ms() - 1_000),
                watch_baseline: None,
            })
            .unwrap();

        let retired_tick = router_b
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/internal/tick")
                    .header("authorization", "Bearer tick-secret-cloud-test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(retired_tick.status(), StatusCode::NOT_FOUND);

        let fire_resp = router_b
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/internal/fire/{}", due_sched.id))
                    .header("authorization", "Bearer tick-secret-cloud-test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fire_resp.status(), StatusCode::OK);
        let fire_json: Value =
            serde_json::from_slice(&fire_resp.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(fire_json["fired"], true);
        assert_eq!(fire_json["schedule_id"], due_sched.id);

        let summary = run_tick_pass(Arc::clone(&store_b), HealthBoard::default())
            .await
            .unwrap();
        assert!(summary.ok);
        assert_eq!(summary.cloud_turns_polled, 1);
        assert_eq!(summary.turns_settled, 1);

        // 3. Verify BOTH turns settled cleanly with exact results & idempotency!
        let spiron_settled = store_b
            .remote_turn_store()
            .get_turn(&spiron_dispatched.turn.turn_id)
            .unwrap()
            .unwrap();
        assert_eq!(spiron_settled.status, "completed");
        assert_eq!(
            spiron_settled.result_text.as_deref(),
            Some("settled via tick poll")
        );

        let generic_settled = store_b
            .remote_turn_store()
            .get_turn(&generic_dispatched.turn.turn_id)
            .unwrap()
            .unwrap();
        assert_eq!(generic_settled.status, "completed");
        assert_eq!(
            generic_settled.result_text.as_deref(),
            Some("settled via webhook push")
        );

        // 4. Late Spiron webhook arriving AFTER tick already settled must be idempotent!
        let late_spiron_wh = serde_json::to_vec(&json!({
            "event": "message.completed",
            "data": {
                "task_id": "rcpt_spiron_tick_1",
                "provider_message_id": spiron_dispatched.turn.turn_id,
                "status": "completed",
                "reply": "late webhook overwrite attempt"
            }
        }))
        .unwrap();
        let late_sig = format!(
            "sha256={}",
            hmac_sha256_hex(b"whsec-cloud-test", &late_spiron_wh)
        );
        let late_resp = router_b
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/internal/cloud/events?driver=spiron")
                    .header("content-type", "application/json")
                    .header("x-spiron-signature-256", late_sig)
                    .body(Body::from(late_spiron_wh))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(late_resp.status(), StatusCode::OK);
        let late_json: Value =
            serde_json::from_slice(&late_resp.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(late_json["idempotent_replay"], true);
        assert_eq!(
            store_b
                .remote_turn_store()
                .get_turn(&spiron_dispatched.turn.turn_id)
                .unwrap()
                .unwrap()
                .result_text
                .as_deref(),
            Some("settled via tick poll")
        );

        match prev_tick_tok {
            Some(v) => unsafe { std::env::set_var("ANYAGENTS_TICK_TOKEN", v) },
            None => unsafe { std::env::remove_var("ANYAGENTS_TICK_TOKEN") },
        }
        match prev_wh_sec {
            Some(v) => unsafe { std::env::set_var("ANYAGENTS_CLOUD_WEBHOOK_SECRET", v) },
            None => unsafe { std::env::remove_var("ANYAGENTS_CLOUD_WEBHOOK_SECRET") },
        }
        let _ = store_b.delete_agent(&spiron_agent.id);
        let _ = store_b.delete_agent(&generic_agent.id);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)]
    async fn serverless_security_fail_closed_guest_tokens_tick_token_and_jwks_eddsa() {
        let _env = crate::server::tests::env_lock();
        clear_jwks_cache();

        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let agent = store
            .create_agent(NewAgent::new("sec_agent", "claude-code"))
            .unwrap();
        let agent_b = store
            .create_agent(NewAgent::new("sec_agent_b", "claude-code"))
            .unwrap();

        let tok_a = generate_callback_token();
        let tok_b = generate_callback_token();
        store
            .remote_turn_store()
            .create_turn(&NewRemoteTurn {
                turn_id: "turn_sec_a".into(),
                agent_id: agent.id.clone(),
                harness_kind: "sandbox".into(),
                provider_kind: Some("http".into()),
                remote_run_id: None,
                callback_token_hash: hash_callback_token(&tok_a),
                cursor: None,
                status: "running".into(),
                metadata_json: None,
                next_poll_at_ms: None,
            })
            .unwrap();
        store
            .remote_turn_store()
            .create_turn(&NewRemoteTurn {
                turn_id: "turn_sec_b".into(),
                agent_id: agent_b.id.clone(),
                harness_kind: "sandbox".into(),
                provider_kind: Some("http".into()),
                remote_run_id: None,
                callback_token_hash: hash_callback_token(&tok_b),
                cursor: None,
                status: "running".into(),
                metadata_json: None,
                next_poll_at_ms: None,
            })
            .unwrap();

        let app = super::super::build_public_router(make_app_state(Arc::clone(&store), dir.path()));

        // 1. `/v1/internal/guest/events` rejects missing, invalid, and wrong-turn bearer tokens (401)!
        for auth in [
            None,
            Some("Bearer wrong-token"),
            Some(format!("Bearer {tok_b}").as_str()),
        ] {
            let mut req = Request::builder()
                .method("POST")
                .uri("/v1/internal/guest/events")
                .header("content-type", "application/json");
            if let Some(hdr) = auth {
                req = req.header("authorization", hdr);
            }
            let resp = app
                .clone()
                .oneshot(
                    req.body(Body::from(
                        json!({
                            "event_type": "turn_complete",
                            "turn_id": "turn_sec_a",
                            "result_text": "forged"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        }
        assert_eq!(
            store
                .remote_turn_store()
                .get_turn("turn_sec_a")
                .unwrap()
                .unwrap()
                .status,
            "running"
        );

        // Save env vars before testing timer fire fail-closed + JWKS (including backward-compat aliases).
        let env_keys = [
            "ANYAGENTS_TIMER_TOKEN",
            "ANYAGENTS_TIMER_JWKS_URL",
            "ANYAGENTS_TIMER_SIGNATURE_HEADER",
            "ANYAGENTS_TIMER_ISS",
            "ANYAGENTS_TIMER_AUD",
            "ANYAGENTS_TIMER_SUB",
            "ANYAGENTS_TICK_TOKEN",
            "ANYAGENTS_TICK_JWKS_URL",
            "ANYAGENTS_TICK_SIGNATURE_HEADER",
            "ANYAGENTS_TICK_ISS",
            "ANYAGENTS_TICK_AUD",
            "ANYAGENTS_TICK_SUB",
        ];
        let saved: Vec<(&str, Option<std::ffi::OsString>)> =
            env_keys.iter().map(|k| (*k, std::env::var_os(k))).collect();
        for k in env_keys {
            unsafe { std::env::remove_var(k) };
        }

        // 2. `/v1/internal/fire/{id}` fails closed with 503 when neither token nor JWKS is set!
        let resp_503 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/internal/fire/sch_missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp_503.status(), StatusCode::SERVICE_UNAVAILABLE);

        // 3. With `ANYAGENTS_TICK_TOKEN` alias set, wrong/missing bearer gets 401, valid gets 200!
        unsafe { std::env::set_var("ANYAGENTS_TICK_TOKEN", "expected-tick-secret") };
        let resp_401 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/internal/fire/sch_missing")
                    .header("authorization", "Bearer wrong-tick-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp_401.status(), StatusCode::UNAUTHORIZED);

        let resp_200 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/internal/fire/sch_missing")
                    .header("authorization", "Bearer expected-tick-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp_200.status(), StatusCode::OK);

        // 4. Neutral JWKS / EdDSA verification (with `kid` rotation refetch + `exp`/`iss`/`aud`/`sub`/`bh`)!
        unsafe { std::env::remove_var("ANYAGENTS_TICK_TOKEN") };
        let rng = SystemRandom::new();
        let pkcs8_1 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let kp1 = Ed25519KeyPair::from_pkcs8(pkcs8_1.as_ref()).unwrap();
        let pkcs8_2 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let kp2 = Ed25519KeyPair::from_pkcs8(pkcs8_2.as_ref()).unwrap();

        let jwks_state = Arc::new(Mutex::new(jwks_doc_for(&[("k1", &kp1)])));
        let jwks_srv_state = Arc::clone(&jwks_state);
        let jwks_app = axum::Router::new().route(
            "/.well-known/jwks.json",
            axum::routing::get(move || {
                let st = Arc::clone(&jwks_srv_state);
                async move { Json(st.lock().unwrap().clone()) }
            }),
        );
        let jwks_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let jwks_addr = jwks_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(jwks_listener, jwks_app).await;
        });

        unsafe {
            std::env::set_var(
                "ANYAGENTS_TICK_JWKS_URL",
                format!("http://{jwks_addr}/.well-known/jwks.json"),
            );
            std::env::set_var("ANYAGENTS_TICK_SIGNATURE_HEADER", "X-Webhook-Signature");
            std::env::set_var("ANYAGENTS_TICK_ISS", "https://scheduler.example.test");
            std::env::set_var(
                "ANYAGENTS_TICK_AUD",
                "https://coord.example.test/v1/internal/fire",
            );
            std::env::set_var("ANYAGENTS_TICK_SUB", "timer-job-1");
        }

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let body_bytes = br#"{"trigger":"timer"}"#;
        let bh = URL_SAFE_NO_PAD.encode(Sha256::digest(body_bytes));

        let valid_jws = sign_ed25519_jws(
            &kp1,
            "k1",
            &json!({
                "iss": "https://scheduler.example.test",
                "aud": "https://coord.example.test/v1/internal/fire",
                "sub": "timer-job-1",
                "exp": now_secs + 300,
                "bh": bh,
            }),
        );
        let ok_jwks = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/internal/fire/sch_missing")
                    .header("x-webhook-signature", valid_jws.clone())
                    .body(Body::from(body_bytes.as_slice()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ok_jwks.status(), StatusCode::OK);

        // Tampered body fails `bh` check (401)!
        let bad_bh = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/internal/fire/sch_missing")
                    .header("x-webhook-signature", valid_jws)
                    .body(Body::from(&b"{\"trigger\":\"tampered\"}"[..]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bad_bh.status(), StatusCode::UNAUTHORIZED);

        // Expired token (> 60s skew) fails (401)!
        let expired_jws = sign_ed25519_jws(
            &kp1,
            "k1",
            &json!({
                "iss": "https://scheduler.example.test",
                "aud": "https://coord.example.test/v1/internal/fire",
                "sub": "timer-job-1",
                "exp": now_secs - 120,
            }),
        );
        let bad_exp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/internal/fire/sch_missing")
                    .header("x-webhook-signature", expired_jws)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bad_exp.status(), StatusCode::UNAUTHORIZED);

        // Key rotation: publish `k2` in JWKS and sign with `k2` -> cache misses `k2`, refetches, succeeds!
        *jwks_state.lock().unwrap() = jwks_doc_for(&[("k1", &kp1), ("k2", &kp2)]);
        let rotated_jws = sign_ed25519_jws(
            &kp2,
            "k2",
            &json!({
                "iss": "https://scheduler.example.test",
                "aud": "https://coord.example.test/v1/internal/fire",
                "sub": "timer-job-1",
                "exp": now_secs + 300,
            }),
        );
        let ok_rotated = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/internal/fire/sch_missing")
                    .header("x-webhook-signature", rotated_jws)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ok_rotated.status(), StatusCode::OK);

        // Restore env vars.
        for (k, val) in saved {
            match val {
                Some(v) => unsafe { std::env::set_var(k, v) },
                None => unsafe { std::env::remove_var(k) },
            }
        }
        clear_jwks_cache();
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn serverless_webhook_router_mounts_confined_api_only_with_operator_token() {
        let _env = crate::server::tests::env_lock();
        let saved: Vec<(&str, Option<std::ffi::OsString>)> = [
            "ANYAGENTS_SERVERLESS",
            "ANYAGENTS_OPERATOR_TOKEN",
            "ANYAGENTS_OPERATOR_TOKEN_FILE",
        ]
        .into_iter()
        .map(|k| (k, std::env::var_os(k)))
        .collect();

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let make_state = |auth: crate::auth::Mode| AppState {
            store: Arc::clone(&store),
            health: crate::server::HealthBoard::default(),
            sign_in: Arc::new(std::sync::Mutex::new(super::super::ProcessRunner)),
            auth,
            identity: Arc::new(crate::identity::ServingIdentity::capture()),
            profiles: Default::default(),
            timer_backend: Arc::new(crate::timer::NoopTimerBackend),
        };

        unsafe {
            std::env::set_var("ANYAGENTS_SERVERLESS", "true");
            std::env::remove_var("ANYAGENTS_OPERATOR_TOKEN");
            std::env::remove_var("ANYAGENTS_OPERATOR_TOKEN_FILE");
        }
        let no_op_app = crate::server::build_webhook_router(make_state(
            crate::auth::Mode::from_env(dir.path()),
        ));
        let res_no_op = no_op_app
            .oneshot(
                Request::builder()
                    .uri("/v1/agents")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res_no_op.status(), StatusCode::NOT_FOUND);

        unsafe {
            std::env::set_var(
                "ANYAGENTS_OPERATOR_TOKEN",
                "serverless-op-secret-0123456789",
            );
        }
        let auth = crate::auth::Mode::from_env(dir.path());
        assert!(auth.has_operator());
        let app = crate::server::build_webhook_router(make_state(auth));

        let health = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);

        let unauth = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/agents")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);

        let authed = app
            .oneshot(
                Request::builder()
                    .uri("/v1/agents")
                    .header(crate::operator::HEADER, "serverless-op-secret-0123456789")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(authed.status(), StatusCode::OK);

        for (k, val) in saved {
            match val {
                Some(v) => unsafe { std::env::set_var(k, v) },
                None => unsafe { std::env::remove_var(k) },
            }
        }
    }

    #[test]
    fn persist_synced_auth_files_updates_agent_and_profile_monotonically_and_rejects_disallowed() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("agy_sync_worker", "agy"))
            .unwrap();
        let agy_rel = ".gemini/antigravity-cli/antigravity-oauth-token";
        let prof_name = "seat-alpha";
        let prof_dir = dir
            .path()
            .join("profiles")
            .join("google-antigravity")
            .join(prof_name);
        std::fs::create_dir_all(&prof_dir).unwrap();

        // Bind agent to `google-antigravity/seat-alpha` via markers namespace.
        store
            .blob_store()
            .put_agent_file(
                &agent.id,
                "markers",
                ".anyagents-profile",
                format!("google-antigravity/{prof_name}\n").as_bytes(),
                false,
            )
            .unwrap();

        // Seed stale cache markers and older token in profile store.
        let old_tok =
            br#"{"token":{"access_token":"old-acc","refresh_token":"rt1","expiry":"2026-11-19T10:00:00Z"}}"#;
        store
            .blob_store()
            .put_deployment_blob(
                "profiles",
                &format!("google-antigravity/{prof_name}/{agy_rel}"),
                old_tok,
                true,
            )
            .unwrap();
        std::fs::write(
            prof_dir.join(".anyagents-oauth-refresh-attempt-ms"),
            b"9999999999999\n",
        )
        .unwrap();
        std::fs::write(
            prof_dir.join(".anyagents-last-usage.txt"),
            b"oauth expired\n",
        )
        .unwrap();

        // 1. Disallowed path + mismatched provider file must be rejected.
        let mut rejected = BTreeMap::new();
        rejected.insert(".ssh/id_rsa".to_string(), b"{\"k\":\"v\"}".to_vec());
        rejected.insert("../escape".to_string(), b"{\"k\":\"v\"}".to_vec());
        let bad_encoded = BTreeMap::from([
            (".ssh/id_rsa".to_string(), STANDARD.encode(b"{\"k\":\"v\"}")),
            (agy_rel.to_string(), "not-valid-base64!!!".to_string()),
        ]);
        assert!(decode_synced_auth_files(&bad_encoded).is_empty());
        assert_eq!(persist_synced_auth_files(&store, &agent.id, &rejected), 0);

        // 2. Newer native-refreshed token updates agent secrets + profile blob and clears stale markers.
        let fresh_tok =
            br#"{"token":{"access_token":"fresh-acc","refresh_token":"rt1","expiry":"2026-11-19T23:30:00Z"}}"#;
        let encoded = BTreeMap::from([(agy_rel.to_string(), STANDARD.encode(fresh_tok))]);
        let decoded = decode_synced_auth_files(&encoded);
        assert_eq!(decoded.len(), 1);
        let updated = persist_synced_auth_files(&store, &agent.id, &decoded);
        assert_eq!(updated, 2);

        let stored_agent = store
            .blob_store()
            .get_agent_file(&agent.id, "secrets", agy_rel)
            .unwrap()
            .expect("agent secret persisted");
        assert_eq!(stored_agent.content, fresh_tok);
        let stored_profile = store
            .blob_store()
            .get_deployment_blob(
                "profiles",
                &format!("google-antigravity/{prof_name}/{agy_rel}"),
            )
            .unwrap()
            .expect("profile blob persisted");
        assert_eq!(stored_profile.content, fresh_tok);
        assert!(
            !prof_dir
                .join(".anyagents-oauth-refresh-attempt-ms")
                .exists()
        );
        assert!(!prof_dir.join(".anyagents-last-usage.txt").exists());

        // 3. Monotonic guard: an older expiry token MUST NOT overwrite the newer token.
        let stale_tok =
            br#"{"token":{"access_token":"stale-acc","refresh_token":"rt1","expiry":"2026-11-19T12:00:00Z"}}"#;
        let stale_map = BTreeMap::from([(agy_rel.to_string(), stale_tok.to_vec())]);
        assert_eq!(persist_synced_auth_files(&store, &agent.id, &stale_map), 0);
        let still_fresh = store
            .blob_store()
            .get_deployment_blob(
                "profiles",
                &format!("google-antigravity/{prof_name}/{agy_rel}"),
            )
            .unwrap()
            .unwrap();
        assert_eq!(still_fresh.content, fresh_tok);
    }

    #[test]
    fn profile_secret_classification_work() {
        assert!(is_secret_profile_rel(".config/opencode/openrouter.key"));
        assert!(is_secret_profile_rel(
            ".gemini/antigravity-cli/antigravity-oauth-token"
        ));
        assert!(is_secret_profile_rel(".claude/.credentials.json"));
        assert!(is_secret_profile_rel("credentials.json"));
        assert!(is_secret_profile_rel(".codex/auth.json"));
        assert!(!is_secret_profile_rel("profile.json"));
        assert!(!is_secret_profile_rel(".config/opencode/opencode.json"));
    }

    #[test]
    fn persist_signed_in_profile_auth_files_persists_valid_and_rejects_disallowed() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();

        let claude_cred =
            br#"{"claudeAiOauth":{"accessToken":"tok-abc","expiresAt":4102444800000}}"#;
        let mut files = BTreeMap::new();
        files.insert(
            ".claude/.credentials.json".to_string(),
            claude_cred.to_vec(),
        );
        files.insert(".ssh/id_ed25519".to_string(), b"secret".to_vec());

        let persisted = persist_signed_in_profile_auth_files(
            &store,
            "anthropic",
            "web-seat",
            &json!({"provider":"anthropic", "kind":"subscription", "compatible_harnesses":["claude-code"]}),
            &files,
        )
        .unwrap();
        assert_eq!(persisted, 1);

        let stored = store
            .blob_store()
            .get_deployment_blob("profiles", "anthropic/web-seat/.claude/.credentials.json")
            .unwrap()
            .expect("anthropic credentials persisted");
        assert_eq!(stored.content, claude_cred);
        let mirror = store
            .blob_store()
            .get_deployment_blob("profiles", "anthropic/web-seat/.credentials.json")
            .unwrap()
            .expect("anthropic mirror credentials persisted");
        assert_eq!(mirror.content, claude_cred);
        let meta = store
            .blob_store()
            .get_deployment_blob("profiles", "anthropic/web-seat/profile.json")
            .unwrap()
            .expect("profile.json persisted");
        let meta_val: Value = serde_json::from_slice(&meta.content).unwrap();
        assert_eq!(meta_val["provider"], json!("anthropic"));
        assert_eq!(meta_val["kind"], json!("subscription"));
        assert!(
            store
                .blob_store()
                .get_deployment_blob("profiles", "anthropic/web-seat/.ssh/id_ed25519")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn settle_suppresses_stray_completed_and_no_reply_sentinels() {
        assert!(is_suppressed_fallback_reply(""));
        assert!(is_suppressed_fallback_reply("   \n "));
        assert!(is_suppressed_fallback_reply("completed"));
        assert!(is_suppressed_fallback_reply(" Completed \n"));
        assert!(is_suppressed_fallback_reply("NO_REPLY"));
        assert!(is_suppressed_fallback_reply(" no_reply "));
        assert!(!is_suppressed_fallback_reply(
            "Task completed successfully."
        ));

        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let sender = store
            .create_agent(NewAgent::new("silent-sender", "agy"))
            .unwrap();
        let peer = store
            .create_agent(NewAgent::new("silent-peer", "agy"))
            .unwrap();
        let now = now_ms();
        for (idx, text) in ["completed", "NO_REPLY", "   "].iter().enumerate() {
            let tid = format!("turn_suppress_{idx}");
            store
                .remote_turn_store()
                .create_turn(&NewRemoteTurn {
                    turn_id: tid.clone(),
                    agent_id: sender.id.clone(),
                    harness_kind: "sandbox".to_string(),
                    provider_kind: Some("sylphx".to_string()),
                    remote_run_id: None,
                    callback_token_hash: format!("hash_{idx}"),
                    cursor: None,
                    status: "dispatched".to_string(),
                    metadata_json: Some(json!({
                        "reply_target": { "kind": "agent", "agent_id": peer.id.as_str() }
                    })),
                    next_poll_at_ms: None,
                })
                .unwrap();
            let settled = settle_remote_turn_and_deliver(
                &store,
                &tid,
                SettleRemoteTurn {
                    status: "completed".to_string(),
                    result_text: Some((*text).to_string()),
                    error_text: None,
                    usage_json: None,
                    cursor: None,
                    settled_at_ms: now + (idx as i64) + 10,
                },
                None,
                0,
            )
            .unwrap()
            .unwrap();
            assert!(!settled.delivered_reply, "must suppress `{text}`");
        }
        assert!(store.list_undelivered(&peer.id, 10).unwrap().is_empty());
    }

    #[test]
    fn settle_completed_turn_advances_mailbox_cursor_while_failed_turn_preserves_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("cursor-advancer", "agy"))
            .unwrap();
        store.set_agent_running(&agent.id, "/work").unwrap();
        let _m1 = store.receive(&agent.id, Incoming::new("msg 1")).unwrap();
        let anyagents_core::ReceiveOutcome::New(m2) =
            store.receive(&agent.id, Incoming::new("msg 2")).unwrap()
        else {
            panic!("expected new message");
        };
        assert_eq!(m2.seq, 2);

        // Failed turn must NOT advance `mailbox_cursors.delivered_seq`.
        let now = now_ms();
        store
            .remote_turn_store()
            .create_turn(&NewRemoteTurn {
                turn_id: "turn_fail_cursor".to_string(),
                agent_id: agent.id.clone(),
                harness_kind: "sandbox".to_string(),
                provider_kind: Some("sylphx".to_string()),
                remote_run_id: None,
                callback_token_hash: "hash1".to_string(),
                cursor: None,
                status: "dispatched".to_string(),
                metadata_json: Some(json!({
                    "message_seq": m2.seq,
                    "opener_id": m2.id.to_string(),
                })),
                next_poll_at_ms: None,
            })
            .unwrap();
        let failed = settle_remote_turn_and_deliver(
            &store,
            "turn_fail_cursor",
            SettleRemoteTurn {
                status: "failed".to_string(),
                result_text: None,
                error_text: Some("transient error".to_string()),
                usage_json: None,
                cursor: None,
                settled_at_ms: now + 10,
            },
            None,
            0,
        )
        .unwrap()
        .unwrap();
        assert!(failed.settled_now);
        assert_eq!(store.delivered_cursor(&agent.id).unwrap(), 0);
        assert_eq!(store.list_undelivered(&agent.id, 10).unwrap().len(), 2);

        // Completed turn MUST advance `mailbox_cursors.delivered_seq` through `m2.seq`.
        store
            .remote_turn_store()
            .create_turn(&NewRemoteTurn {
                turn_id: "turn_ok_cursor".to_string(),
                agent_id: agent.id.clone(),
                harness_kind: "sandbox".to_string(),
                provider_kind: Some("sylphx".to_string()),
                remote_run_id: None,
                callback_token_hash: "hash2".to_string(),
                cursor: None,
                status: "dispatched".to_string(),
                metadata_json: Some(json!({
                    "message_seq": m2.seq,
                    "opener_id": m2.id.to_string(),
                })),
                next_poll_at_ms: None,
            })
            .unwrap();
        let ok = settle_remote_turn_and_deliver(
            &store,
            "turn_ok_cursor",
            SettleRemoteTurn {
                status: "completed".to_string(),
                result_text: Some("all done".to_string()),
                error_text: None,
                usage_json: None,
                cursor: None,
                settled_at_ms: now + 30,
            },
            None,
            0,
        )
        .unwrap()
        .unwrap();
        assert!(ok.settled_now);
        assert_eq!(store.delivered_cursor(&agent.id).unwrap(), m2.seq);
        assert!(store.list_undelivered(&agent.id, 10).unwrap().is_empty());
    }

    #[test]
    fn mid_turn_inbox_read_advances_delivered_cursor_on_complete_not_on_failure() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("inbox-watermark", "claude-code"))
            .unwrap();
        store.set_agent_running(&agent.id, "/work").unwrap();
        let now = now_ms();

        let anyagents_core::ReceiveOutcome::New(m1) = store
            .receive(&agent.id, anyagents_core::Incoming::new("initial prompt"))
            .unwrap()
        else {
            panic!("expected new m1");
        };
        let anyagents_core::ReceiveOutcome::New(_m2) = store
            .receive(&agent.id, anyagents_core::Incoming::new("mid-turn msg 2"))
            .unwrap()
        else {
            panic!("expected new m2");
        };
        let anyagents_core::ReceiveOutcome::New(m3) = store
            .receive(&agent.id, anyagents_core::Incoming::new("mid-turn msg 3"))
            .unwrap()
        else {
            panic!("expected new m3");
        };

        // Turn 1 dispatched for m1, agent reads up to m3 mid-turn via inbox(), then crashes -> cursor stays 0.
        store
            .remote_turn_store()
            .create_turn(&NewRemoteTurn {
                turn_id: "turn_inbox_fail".into(),
                agent_id: agent.id.clone(),
                harness_kind: "sandbox".into(),
                provider_kind: Some("sandbox".into()),
                remote_run_id: None,
                callback_token_hash: "h-fail".into(),
                cursor: None,
                status: "running".into(),
                metadata_json: Some(json!({ "message_seq": m1.seq })),
                next_poll_at_ms: None,
            })
            .unwrap();
        store
            .remote_turn_store()
            .record_agent_inbox_read_seq(&agent.id, m3.seq, now + 5)
            .unwrap();
        settle_remote_turn_and_deliver(
            &store,
            "turn_inbox_fail",
            SettleRemoteTurn {
                status: "failed".into(),
                result_text: None,
                error_text: Some("process crashed".into()),
                usage_json: None,
                cursor: None,
                settled_at_ms: now + 10,
            },
            None,
            0,
        )
        .unwrap();
        assert_eq!(store.delivered_cursor(&agent.id).unwrap(), 0);
        assert_eq!(store.list_undelivered(&agent.id, 10).unwrap().len(), 3);

        // Turn 2 dispatched for m1, agent reads up to m3 mid-turn via inbox(), then completes -> cursor jumps to m3.seq!
        store
            .remote_turn_store()
            .create_turn(&NewRemoteTurn {
                turn_id: "turn_inbox_ok".into(),
                agent_id: agent.id.clone(),
                harness_kind: "sandbox".into(),
                provider_kind: Some("sandbox".into()),
                remote_run_id: None,
                callback_token_hash: "h-ok".into(),
                cursor: None,
                status: "running".into(),
                metadata_json: Some(
                    json!({ "message_seq": m1.seq, "opener_id": m1.id.to_string() }),
                ),
                next_poll_at_ms: None,
            })
            .unwrap();
        store
            .remote_turn_store()
            .record_agent_inbox_read_seq(&agent.id, m3.seq, now + 20)
            .unwrap();
        settle_remote_turn_and_deliver(
            &store,
            "turn_inbox_ok",
            SettleRemoteTurn {
                status: "completed".into(),
                result_text: Some("answered all three".into()),
                error_text: None,
                usage_json: None,
                cursor: None,
                settled_at_ms: now + 30,
            },
            None,
            0,
        )
        .unwrap();
        assert_eq!(store.delivered_cursor(&agent.id).unwrap(), m3.seq);
        assert!(store.list_undelivered(&agent.id, 10).unwrap().is_empty());
    }

    type RecordedTars = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

    struct InspectingMockProvider {
        root: std::path::PathBuf,
        ensure_reqs: Arc<Mutex<Vec<EnsureSandboxRequest>>>,
        exec_reqs: Arc<Mutex<Vec<ExecRequest>>>,
        synced_tars: RecordedTars,
    }

    #[async_trait::async_trait]
    impl AsyncSandboxProvider for InspectingMockProvider {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Http
        }
        fn capabilities(&self) -> SandboxCapabilities {
            SandboxCapabilities {
                persistent_volumes: true,
                volume_survives_release: true,
                ..SandboxCapabilities::default()
            }
        }
        async fn ensure(&self, req: &EnsureSandboxRequest) -> Result<SandboxHandle, String> {
            self.ensure_reqs.lock().unwrap().push(req.clone());
            let sd = self.root.join(&req.agent_id).join("state");
            let wd = self.root.join(&req.agent_id).join("work");
            std::fs::create_dir_all(&sd).unwrap();
            std::fs::create_dir_all(&wd).unwrap();
            Ok(SandboxHandle {
                provider: ProviderKind::Http,
                sandbox_id: format!("sbx-{}", req.agent_id),
                volume_id: Some(format!("vol-{}", req.agent_id)),
                mount_path: self.root.join(&req.agent_id).display().to_string(),
                state_dir: sd.display().to_string(),
                workdir: wd.display().to_string(),
                transport_url: None,
                transport_token: None,
                expires_at: None,
                metadata: BTreeMap::new(),
            })
        }
        async fn exec(&self, _h: &SandboxHandle, req: &ExecRequest) -> Result<ExecOutput, String> {
            self.exec_reqs.lock().unwrap().push(req.clone());
            Ok(ExecOutput {
                exit_code: 0,
                stdout: r#"{"pid":1337}"#.to_string(),
                stderr: String::new(),
            })
        }
        async fn sync_bundle_tar(
            &self,
            _h: &SandboxHandle,
            dest_dir: &str,
            tar_gz: &[u8],
        ) -> Result<(), String> {
            self.synced_tars
                .lock()
                .unwrap()
                .push((dest_dir.to_string(), tar_gz.to_vec()));
            crate::sandbox_providers::providers::extract_bundle_tar_gz(
                tar_gz,
                std::path::Path::new(dest_dir),
            )?;
            Ok(())
        }
        async fn release(&self, _h: &SandboxHandle) -> Result<(), String> {
            Ok(())
        }
        async fn destroy(&self, _h: &SandboxHandle) -> Result<(), String> {
            Ok(())
        }
        async fn health(&self) -> Result<ProviderHealth, String> {
            Ok(ProviderHealth {
                provider: ProviderKind::Http,
                healthy: true,
                message: "ok".into(),
                latency_ms: Some(1),
            })
        }
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn dispatch_sandbox_turn_resolves_code_shape_and_volume_for_repo_agents_only() {
        let _env = crate::server::tests::env_lock();
        let prev_shape = std::env::var_os("ANYAGENTS_SANDBOX_CODE_SHAPE");
        let prev_vol = std::env::var_os("ANYAGENTS_SANDBOX_CODE_VOLUME_SIZE_GIB");
        unsafe {
            std::env::set_var("ANYAGENTS_SANDBOX_CODE_SHAPE", "linux-xl");
            std::env::set_var("ANYAGENTS_SANDBOX_CODE_VOLUME_SIZE_GIB", "40");
        }

        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let repo_agent = store
            .create_agent(NewAgent::new("repo-coder", "codex"))
            .unwrap();
        let plain_agent = store
            .create_agent(NewAgent::new("plain-assistant", "codex"))
            .unwrap();

        let repo_cfg = anyagents_core::agent_config_dir(store.data_dir(), &repo_agent.id);
        std::fs::create_dir_all(&repo_cfg).unwrap();
        std::fs::write(
            repo_cfg.join("repo-spec.json"),
            r#"{"repo":"https://github.com/example/sample.git","branch":"main"}"#,
        )
        .unwrap();

        let ensure_reqs = Arc::new(Mutex::new(Vec::new()));
        let provider: Arc<dyn AsyncSandboxProvider> = Arc::new(InspectingMockProvider {
            root: dir.path().join("sbx"),
            ensure_reqs: Arc::clone(&ensure_reqs),
            exec_reqs: Arc::new(Mutex::new(Vec::new())),
            synced_tars: Arc::new(Mutex::new(Vec::new())),
        });

        let mut repo_input = ServerlessDispatchInput::for_agent(repo_agent.clone(), "fix bug");
        repo_input.callback_base_url = Some("http://127.0.0.1:1".into());
        repo_input.sandbox_provider = Some(Arc::clone(&provider));
        dispatch_serverless_turn(Arc::clone(&store), repo_input)
            .await
            .unwrap();

        let mut plain_input = ServerlessDispatchInput::for_agent(plain_agent.clone(), "say hi");
        plain_input.callback_base_url = Some("http://127.0.0.1:1".into());
        plain_input.sandbox_provider = Some(Arc::clone(&provider));
        dispatch_serverless_turn(Arc::clone(&store), plain_input)
            .await
            .unwrap();

        match prev_shape {
            Some(v) => unsafe { std::env::set_var("ANYAGENTS_SANDBOX_CODE_SHAPE", v) },
            None => unsafe { std::env::remove_var("ANYAGENTS_SANDBOX_CODE_SHAPE") },
        }
        match prev_vol {
            Some(v) => unsafe { std::env::set_var("ANYAGENTS_SANDBOX_CODE_VOLUME_SIZE_GIB", v) },
            None => unsafe { std::env::remove_var("ANYAGENTS_SANDBOX_CODE_VOLUME_SIZE_GIB") },
        }

        let reqs = ensure_reqs.lock().unwrap();
        assert_eq!(reqs.len(), 2);
        assert_eq!(reqs[0].shape.as_deref(), Some("linux-xl"));
        assert_eq!(reqs[0].volume.as_ref().and_then(|v| v.size_gib), Some(40));
        assert_eq!(reqs[1].shape, None);
        assert_eq!(reqs[1].volume.as_ref().and_then(|v| v.size_gib), None);

        let repo_binding = store.sandbox_binding(&repo_agent.id).unwrap().unwrap();
        assert_eq!(repo_binding.volume_size_gib, Some(40));
        let spec: Value = serde_json::from_str(&repo_binding.spec_json).unwrap();
        assert_eq!(spec.get("shape").and_then(Value::as_str), Some("linux-xl"));
    }

    #[tokio::test]
    async fn dispatch_sandbox_turn_stages_oversized_outbound_message_via_message_file() {
        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let agent = store
            .create_agent(NewAgent::new("big-batch-agent", "codex"))
            .unwrap();
        store.set_agent_running(&agent.id, "/work").unwrap();

        let huge_text = "B".repeat(
            super::super::serverless_envelope::MAX_INLINE_GUEST_MESSAGE_B64_BYTES + 24 * 1024,
        );
        store
            .receive(&agent.id, Incoming::new(huge_text.clone()))
            .unwrap();

        let exec_reqs = Arc::new(Mutex::new(Vec::new()));
        let synced_tars = Arc::new(Mutex::new(Vec::new()));
        let provider: Arc<dyn AsyncSandboxProvider> = Arc::new(InspectingMockProvider {
            root: dir.path().join("sbx"),
            ensure_reqs: Arc::new(Mutex::new(Vec::new())),
            exec_reqs: Arc::clone(&exec_reqs),
            synced_tars: Arc::clone(&synced_tars),
        });

        let dispatched = dispatch_next_mailbox_message_inner(
            Arc::clone(&store),
            &agent.id,
            Some(provider),
            Some("http://127.0.0.1:1".into()),
            None,
        )
        .await
        .unwrap()
        .expect("oversized batch must dispatch via --message-file");

        let reqs = exec_reqs.lock().unwrap();
        assert_eq!(reqs.len(), 1);
        assert!(
            !reqs[0]
                .env
                .contains_key(anyagents_harness::GUEST_MESSAGE_B64_ENV),
            "oversized message must not populate ANYAGENTS_GUEST_MESSAGE_B64"
        );
        let flag_pos = reqs[0]
            .command
            .iter()
            .position(|a| a == "--message-file")
            .expect("argv must include --message-file");
        let remote_msg_path = &reqs[0].command[flag_pos + 1];
        assert!(
            remote_msg_path.ends_with(&format!("turns/{}.message.json", dispatched.turn.turn_id)),
            "unexpected message-file path: {remote_msg_path}"
        );
        let raw = std::fs::read_to_string(remote_msg_path).expect("message file extracted");
        let parsed: anyagents_harness::OutboundMessage = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed.text, huge_text);
        assert!(!synced_tars.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn dispatch_sandbox_turn_conflict_returns_ok_none_without_clobbering_live_turn() {
        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let agent = store
            .create_agent(NewAgent::new("race-agent", "codex"))
            .unwrap();
        let agent = store.set_agent_state(&agent.id, "running").unwrap();

        let mut binding = anyagents_core::stateless::SandboxBinding::new(agent.id.clone(), "http");
        binding.volume_id = "vol-live-1".into();
        binding.lease_id = Some("sbx-live-1".into());
        binding.lease_state = "ready".into();
        store.sandbox_binding_store().upsert(&binding).unwrap();

        store
            .remote_turn_store()
            .create_turn(&NewRemoteTurn {
                turn_id: "turn_live_winner".into(),
                agent_id: agent.id.clone(),
                harness_kind: "sandbox".into(),
                provider_kind: Some("http".into()),
                remote_run_id: Some("pid:100".into()),
                callback_token_hash: "h_live".into(),
                cursor: None,
                status: "running".into(),
                metadata_json: None,
                next_poll_at_ms: None,
            })
            .unwrap();

        let provider: Arc<dyn AsyncSandboxProvider> = Arc::new(InspectingMockProvider {
            root: dir.path().join("sbx"),
            ensure_reqs: Arc::new(Mutex::new(Vec::new())),
            exec_reqs: Arc::new(Mutex::new(Vec::new())),
            synced_tars: Arc::new(Mutex::new(Vec::new())),
        });
        let mut input = ServerlessDispatchInput::for_agent(agent.clone(), "racing prompt");
        input.callback_base_url = Some("http://127.0.0.1:1".into());
        input.sandbox_provider = Some(provider);

        let raced = dispatch_serverless_turn_opt(Arc::clone(&store), input)
            .await
            .expect("active-turn conflict must cleanly return Ok(None)");
        assert!(raced.is_none());

        // Live turn and active lease remain completely untouched.
        let live = store
            .remote_turn_store()
            .get_turn("turn_live_winner")
            .unwrap()
            .unwrap();
        assert_eq!(live.status, "running");
        let kept = store.sandbox_binding(&agent.id).unwrap().unwrap();
        assert_eq!(kept.lease_id.as_deref(), Some("sbx-live-1"));
        assert_eq!(kept.lease_state, "ready");
    }
}
