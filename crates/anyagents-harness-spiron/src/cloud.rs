//! First-class ADR 0010 D7 Spiron Cloud remote-session provider and in-process
//! [`SpironCloudHarness`].
//!
//! Talks directly to the Spiron Agent/message HTTP API with zero local child
//! processes, keepers, or stdio shims. Enforces origin/credential binding,
//! advisory config locking, strict UUID validation, `session-entry-v1`
//! capability negotiation, coordination-only Streamable HTTP MCP attachment
//! with per-turn token rotation, durable idempotent requests, real token/cost
//! accounting, and hard micro-USD budget caps.

use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyagents_harness::{
    RemoteBudget, RemoteCapabilities, RemoteConsumptionProof, RemoteDeliveryAck,
    RemoteDeliveryInput, RemoteEvent, RemoteSessionConfig, RemoteSessionError, RemoteSessionHandle,
    RemoteSessionHarness, RemoteSessionProvider, TurnUsage,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

use crate::cloud_api::{
    ACTIVE_RUN_FILE, ActiveRunState, DEFAULT_RETRY_AFTER_MS, HttpOutcome, PendingRecord, SpironApi,
    bind_origin_and_cred, build_capabilities, classify_http_error, classify_receipt_failure,
    classify_rejected_4xx, clear_active_run, declares_next_step, env_trimmed, extract_usage,
    is_spend_cap_error, now_epoch_ms, persist_json, persist_terminal, proto, ptr_micros, ptr_str,
    read_cloud_json, resolve_api_token, resolve_base_url, resolve_completed_reply,
    resolve_disabled_tools, resolve_or_create_agent, spend_cap_bounds, sync_mcp_servers, transport,
    validate_proof, validate_remote_mcp_url, validate_request_id, workspace_context,
};

/// Default Spiron Cloud API base URL.
pub const DEFAULT_SPIRON_CLOUD_URL: &str = "https://api.spiron.ai";
/// Server-written rotated remote session token filename inside `config_dir`.
pub const REMOTE_SESSION_TOKEN_FILE: &str = "remote-session-token";
/// Server-written public coordination MCP URL filename inside `config_dir`.
pub const REMOTE_MCP_URL_FILE: &str = ".anyagents-mcp-url";

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_TURN_BUDGET: Duration = Duration::from_secs(30 * 60);
const MAX_TRANSIENT_POLL_ERRORS: u32 = 5;

/// In-process [`anyagents_harness::Harness`] backed directly by [`SpironCloudProvider`].
pub type SpironCloudHarness = RemoteSessionHarness<SpironCloudProvider>;

/// Workspace spend-cap snapshot returned by `GET /v1/spend-cap`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpendCapStatus {
    pub reached: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spent_micros: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit_micros: Option<u64>,
}

/// Configuration for [`SpironCloudProvider`].
#[derive(Clone)]
pub struct SpironCloudConfig {
    pub base_url: String,
    pub api_token: Option<String>,
    pub config_dir: PathBuf,
    pub request_timeout: Duration,
    pub turn_budget: Duration,
    pub budget_cap_micros: Option<u64>,
    pub mcp_url_file: PathBuf,
    pub mcp_token_file: PathBuf,
    pub allow_loopback_mcp: bool,
    pub disabled_tools: Vec<String>,
}

impl fmt::Debug for SpironCloudConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpironCloudConfig")
            .field("base_url", &self.base_url)
            .field("api_token", &self.api_token.as_ref().map(|_| "[redacted]"))
            .field("config_dir", &self.config_dir)
            .field("request_timeout", &self.request_timeout)
            .field("turn_budget", &self.turn_budget)
            .field("budget_cap_micros", &self.budget_cap_micros)
            .field("allow_loopback_mcp", &self.allow_loopback_mcp)
            .finish()
    }
}

impl SpironCloudConfig {
    /// Resolve default endpoint (`ANYAGENTS_SPIRON_CLOUD_URL` -> `SPIRON_API_URL`
    /// -> `SPIRON_CLOUD_URL` -> `cloud.json` -> [`DEFAULT_SPIRON_CLOUD_URL`]).
    pub fn new(config_dir: impl Into<PathBuf>) -> Self {
        let config_dir = config_dir.into();
        let base_url = resolve_base_url(&config_dir);
        let disabled_tools = resolve_disabled_tools(&config_dir);
        Self {
            base_url,
            api_token: None,
            mcp_url_file: config_dir.join(REMOTE_MCP_URL_FILE),
            mcp_token_file: config_dir.join(REMOTE_SESSION_TOKEN_FILE),
            config_dir,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            turn_budget: DEFAULT_TURN_BUDGET,
            budget_cap_micros: None,
            allow_loopback_mcp: false,
            disabled_tools,
        }
    }

    pub fn from_config_dir(config_dir: impl Into<PathBuf>) -> Self {
        Self::new(config_dir)
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into().trim().trim_end_matches('/').to_string();
        self
    }

    pub fn with_api_token(mut self, token: impl Into<String>) -> Self {
        self.api_token = Some(token.into());
        self
    }

    pub fn with_budget_cap_micros(mut self, cap_micros: Option<u64>) -> Self {
        self.budget_cap_micros = cap_micros;
        self
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn with_turn_budget(mut self, turn_budget: Duration) -> Self {
        self.turn_budget = turn_budget;
        self
    }

    pub fn with_allow_loopback_mcp(mut self, allow: bool) -> Self {
        self.allow_loopback_mcp = allow;
        self
    }
}

/// Returns `true` when `token` is a Sylphx gateway key (`sylphx_sk_...`).
pub fn is_sylphx_gateway_key(token: &str) -> bool {
    token.trim().starts_with("sylphx_sk_")
}

/// Returns `true` when `token` matches a server-issued remote session token (`aars_` + 64 hex).
pub fn is_remote_session_token(token: &str) -> bool {
    token
        .trim()
        .strip_prefix("aars_")
        .is_some_and(|rest| rest.len() == 64 && rest.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// Validate a canonical 36-character UUID used in URL paths or durable proofs.
pub fn validate_uuid(id: &str, field: &str) -> Result<String, RemoteSessionError> {
    let ok = id.len() == 36
        && id.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        });
    if !ok {
        return Err(proto(format!("Spiron response has invalid {field}")));
    }
    Ok(id.to_string())
}

struct OpenSession {
    _lock: File,
    api: SpironApi,
    agent_id: String,
    scope: String,
}

struct ActiveReceipt {
    request_id: String,
    receipt_id: String,
    run_id: Option<String>,
    pending_path: PathBuf,
    pending: PendingRecord,
    raw_proof: Option<Value>,
    proof: Option<RemoteConsumptionProof>,
    proof_emitted: bool,
    deadline: Instant,
    transient_errors: u32,
}

impl ActiveReceipt {
    fn remote_run_id(&self) -> String {
        self.run_id
            .clone()
            .unwrap_or_else(|| self.receipt_id.clone())
    }
}

enum LoadedPending {
    Ready(PendingRecord),
    Reused(RemoteDeliveryAck),
}

/// First-class ADR 0010 D7 [`RemoteSessionProvider`] for Spiron Cloud.
pub struct SpironCloudProvider {
    config: SpironCloudConfig,
    session: Option<OpenSession>,
    strict: bool,
    capabilities: RemoteCapabilities,
    budget: Option<RemoteBudget>,
    spend_cap: Option<SpendCapStatus>,
    fallback_mcp: Option<(String, String)>,
    active: BTreeMap<String, ActiveReceipt>,
    buffered_events: Vec<RemoteEvent>,
    event_seq: u64,
    closed: bool,
}

impl SpironCloudProvider {
    pub fn new(config: SpironCloudConfig) -> Self {
        let budget = config.budget_cap_micros.map(RemoteBudget::new);
        Self {
            config,
            session: None,
            strict: false,
            capabilities: build_capabilities(false),
            budget,
            spend_cap: None,
            fallback_mcp: None,
            active: BTreeMap::new(),
            buffered_events: Vec::new(),
            event_seq: 0,
            closed: false,
        }
    }

    /// Create a Spiron Cloud session and wrap it in an in-process [`SpironCloudHarness`].
    pub fn start_harness(
        config: SpironCloudConfig,
        session_config: &RemoteSessionConfig,
    ) -> Result<SpironCloudHarness, RemoteSessionError> {
        RemoteSessionHarness::start(Self::new(config), session_config)
    }

    /// Same as [`Self::start_harness`] with an explicit event-poll cadence.
    pub fn start_harness_with_poll_interval(
        config: SpironCloudConfig,
        session_config: &RemoteSessionConfig,
        poll_interval: Duration,
    ) -> Result<SpironCloudHarness, RemoteSessionError> {
        RemoteSessionHarness::start_with_poll_interval(
            Self::new(config),
            session_config,
            poll_interval,
        )
    }

    pub fn agent_id(&self) -> Option<&str> {
        self.session.as_ref().map(|s| s.agent_id.as_str())
    }

    pub fn spend_cap_status(&self) -> Option<&SpendCapStatus> {
        self.spend_cap.as_ref()
    }

    pub fn spend_cap_reached(&self) -> bool {
        self.spend_cap.as_ref().is_some_and(|s| s.reached)
    }

    fn next_cursor(&mut self) -> String {
        self.event_seq = self.event_seq.saturating_add(1);
        format!("spiron-ev-{}", self.event_seq)
    }

    fn resolve_mcp(
        &self,
        url_ov: Option<&str>,
        tok_ov: Option<&str>,
    ) -> Result<Option<(String, String)>, RemoteSessionError> {
        if self.config.mcp_token_file.exists() {
            let tok = fs::read_to_string(&self.config.mcp_token_file)
                .map_err(|e| transport(format!("cannot read remote session token: {e}")))?
                .trim()
                .to_string();
            if !is_remote_session_token(&tok) {
                return Err(proto(
                    "remote-session-token does not hold a valid remote session token",
                ));
            }
            let raw_url = fs::read_to_string(&self.config.mcp_url_file)
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .or_else(|| {
                    url_ov
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string)
                })
                .or_else(|| env_trimmed("ANYAGENTS_PUBLIC_MCP_URL"))
                .or_else(|| {
                    read_cloud_json(&self.config.config_dir)
                        .and_then(|v| ptr_str(&v, &["/mcp_url"]).map(str::to_string))
                })
                .or_else(|| {
                    self.fallback_mcp
                        .as_ref()
                        .map(|(u, _)| u.trim().to_string())
                        .filter(|s| !s.is_empty())
                })
                .ok_or_else(|| {
                    proto("remote-session-token is present but no MCP callback URL is configured")
                })?;
            return Ok(Some((
                validate_remote_mcp_url(&self.config, &raw_url)?,
                tok,
            )));
        }
        let Some((url, tok)) = url_ov.zip(tok_ov).or_else(|| {
            self.fallback_mcp
                .as_ref()
                .map(|(u, t)| (u.as_str(), t.as_str()))
        }) else {
            return Ok(None);
        };
        if !is_remote_session_token(tok) {
            return Err(proto("MCP token is not a valid remote session token"));
        }
        Ok(Some((
            validate_remote_mcp_url(&self.config, url)?,
            tok.trim().to_string(),
        )))
    }

    fn probe_spend_cap(&mut self, api: &SpironApi) -> Result<(), RemoteSessionError> {
        match api.get_json("/v1/spend-cap") {
            Ok(HttpOutcome::Ok(v)) => {
                let reached = v
                    .get("reached")
                    .or_else(|| v.get("exceeded"))
                    .or_else(|| v.get("cap_reached"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let spent_micros = ptr_micros(
                    &v,
                    &["/spent_micros", "/current_spend_micros", "/used_micros"],
                    &["/spent_usd", "/current_spend_usd", "/used_usd"],
                );
                let limit_micros = ptr_micros(
                    &v,
                    &["/limit_micros", "/cap_micros", "/monthly_limit_micros"],
                    &["/limit_usd", "/cap_usd", "/monthly_limit_usd"],
                );
                self.spend_cap = Some(SpendCapStatus {
                    reached,
                    spent_micros,
                    limit_micros,
                });
                Ok(())
            }
            Ok(HttpOutcome::Error(err)) if err.status == 401 => Err(classify_http_error(&err)),
            _ => Ok(()),
        }
    }

    fn build_message_body(&self, input: &RemoteDeliveryInput) -> Result<Value, RemoteSessionError> {
        let content = match workspace_context(&self.config.config_dir)? {
            Some(ctx) => format!(
                "Remote workspace context (repository, branch, base): {ctx}\nPrepare this checkout in your cloud workspace before repository work; never use the controller filesystem.\n\n{}",
                input.text
            ),
            None => input.text.clone(),
        };
        let mut body = json!({ "content": content });
        if let Some((url, tok)) =
            self.resolve_mcp(input.mcp_url.as_deref(), input.mcp_token.as_deref())?
        {
            body["mcp_servers"] = json!([{ "name": "anyagents", "url": url, "bearer_token": tok }]);
        }
        Ok(body)
    }

    fn record_spend(&mut self, usage: &TurnUsage) -> Option<(u64, u64)> {
        let b = self.budget.as_mut()?;
        let cost = usage
            .reported_cost_micros
            .and_then(|c| u64::try_from(c).ok());
        b.record_spend(cost)
            .then_some((b.spent_micros, b.cap_micros))
    }

    fn load_or_create_pending(
        &mut self,
        request_id: &str,
        scope: &str,
        body: Value,
        path: &Path,
    ) -> Result<LoadedPending, RemoteSessionError> {
        match fs::read(path) {
            Ok(bytes) => {
                let rec: PendingRecord = serde_json::from_slice(&bytes)
                    .map_err(|_| proto("invalid Spiron pending request"))?;
                if rec.scope != scope {
                    return Err(proto(
                        "Spiron pending request belongs to another API or Agent",
                    ));
                }
                if rec.body.get("content") != body.get("content") {
                    return Err(proto(
                        "unfinished Spiron turn retained; mailbox UUID reused with different content",
                    ));
                }
                if rec.key != request_id {
                    return Err(proto("Spiron mailbox request key mismatch"));
                }
                if rec.completed.is_some() {
                    let remote_run_id = rec
                        .run_id
                        .or(rec.receipt)
                        .unwrap_or_else(|| request_id.to_string());
                    return Ok(LoadedPending::Reused(RemoteDeliveryAck::Duplicate {
                        remote_run_id,
                    }));
                }
                if let Some(receipt_id) = rec.receipt.clone() {
                    let receipt_id = validate_uuid(&receipt_id, "receipt_id")?;
                    let remote_run_id = rec.run_id.clone().unwrap_or_else(|| receipt_id.clone());
                    if self.active.contains_key(request_id) {
                        return Ok(LoadedPending::Reused(RemoteDeliveryAck::Duplicate {
                            remote_run_id,
                        }));
                    }
                    let raw_proof = rec.input_delivery.clone();
                    let proof = raw_proof.as_ref().map(validate_proof).transpose()?;
                    let proof_emitted = proof.is_some();
                    self.active.insert(
                        request_id.into(),
                        ActiveReceipt {
                            request_id: request_id.into(),
                            receipt_id,
                            run_id: rec.run_id.clone(),
                            pending_path: path.to_path_buf(),
                            pending: rec,
                            raw_proof,
                            proof: proof.clone(),
                            proof_emitted,
                            deadline: Instant::now() + self.config.turn_budget,
                            transient_errors: 0,
                        },
                    );
                    let ack = match proof {
                        Some(p) => RemoteDeliveryAck::Accepted {
                            remote_run_id,
                            session_id: Some(p.session_id.clone()),
                            consumed_proof: Some(p),
                            turn_finished: false,
                            usage: None,
                        },
                        None => RemoteDeliveryAck::InFlight { remote_run_id },
                    };
                    return Ok(LoadedPending::Reused(ack));
                }
                Ok(LoadedPending::Ready(rec))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let rec = PendingRecord {
                    scope: scope.into(),
                    key: request_id.into(),
                    body,
                    receipt: None,
                    run_id: None,
                    input_delivery: None,
                    completed: None,
                };
                persist_json(path, &rec, "Spiron pending request")?;
                Ok(LoadedPending::Ready(rec))
            }
            Err(_) => Err(transport("cannot read Spiron pending request")),
        }
    }

    fn poll_one_receipt(
        &mut self,
        item: &mut ActiveReceipt,
        agent_id: &str,
        events: &mut Vec<RemoteEvent>,
    ) -> Result<bool, RemoteSessionError> {
        if Instant::now() >= item.deadline {
            return Err(transport(
                "Spiron reconciliation deadline reached; input receipts retained",
            ));
        }
        let path = format!("/v1/message-receipts/{}", item.receipt_id);
        let receipt = match self.session.as_ref().unwrap().api.get_json(&path) {
            Ok(HttpOutcome::Ok(val)) => {
                item.transient_errors = 0;
                val
            }
            Ok(HttpOutcome::Error(err)) if (500..=599).contains(&err.status) => {
                item.transient_errors += 1;
                return if item.transient_errors > MAX_TRANSIENT_POLL_ERRORS {
                    Err(classify_http_error(&err))
                } else {
                    Ok(true)
                };
            }
            Ok(HttpOutcome::Error(err)) => return Err(classify_http_error(&err)),
            Err(RemoteSessionError::Transport(msg)) => {
                item.transient_errors += 1;
                return if item.transient_errors > MAX_TRANSIENT_POLL_ERRORS {
                    Err(RemoteSessionError::Transport(msg))
                } else {
                    Ok(true)
                };
            }
            Err(err) => return Err(err),
        };
        if receipt.get("id").and_then(Value::as_str) != Some(item.receipt_id.as_str())
            || receipt.get("agent_id").and_then(Value::as_str) != Some(agent_id)
        {
            return Err(proto("Spiron receipt identity mismatch; request retained"));
        }
        if self.strict {
            if receipt.get("provider_message_id").and_then(Value::as_str)
                != Some(item.pending.key.as_str())
            {
                return Err(proto(
                    "Spiron receipt request key mismatch; request retained",
                ));
            }
            if let Some(raw_proof) = receipt.get("input_delivery").filter(|v| !v.is_null()) {
                let parsed = validate_proof(raw_proof)?;
                if item.raw_proof.as_ref().is_some_and(|p| p != raw_proof) {
                    return Err(proto("Spiron input proof changed; request retained"));
                }
                if item.raw_proof.is_none() {
                    item.raw_proof = Some(raw_proof.clone());
                    item.proof = Some(parsed.clone());
                    item.pending.input_delivery = Some(raw_proof.clone());
                    persist_json(&item.pending_path, &item.pending, "Spiron input proof")?;
                }
                if !item.proof_emitted {
                    item.proof_emitted = true;
                    let cursor = self.next_cursor();
                    events.push(RemoteEvent::InputConsumed {
                        request_id: item.request_id.clone(),
                        remote_run_id: item.remote_run_id(),
                        proof: parsed,
                        cursor: Some(cursor),
                    });
                }
            } else if item.raw_proof.is_some() {
                return Err(proto("Spiron input proof disappeared; request retained"));
            }
        }
        let remote_run_id = item.remote_run_id();
        match receipt.get("status").and_then(Value::as_str) {
            Some("completed") => {
                if self.strict && item.proof.is_none() {
                    return Err(proto(
                        "completed Spiron receipt lacks durable input proof; request retained",
                    ));
                }
                let reply = resolve_completed_reply(
                    &self.session.as_ref().unwrap().api,
                    agent_id,
                    &item.receipt_id,
                    &receipt,
                    item.deadline,
                )?;
                let usage = extract_usage(&receipt);
                let crossed = self.record_spend(&usage);
                let sid = item
                    .proof
                    .as_ref()
                    .map_or_else(|| agent_id.to_string(), |p| p.session_id.clone());
                persist_terminal(&item.pending_path, &mut item.pending, receipt)?;
                let cursor = self.next_cursor();
                events.push(RemoteEvent::Completed {
                    request_id: item.request_id.clone(),
                    remote_run_id,
                    session_id: Some(sid),
                    reply_text: Some(reply),
                    usage: Some(usage),
                    cursor: Some(cursor),
                });
                if let Some((spent_micros, cap_micros)) = crossed {
                    events.push(RemoteEvent::BudgetExceeded {
                        spent_micros,
                        cap_micros,
                    });
                }
                Ok(false)
            }
            Some("failed") => {
                let (code, message, status, quota_reset_at_ms) = classify_receipt_failure(&receipt);
                let is_budget = status == Some(402);
                let (spent_micros, cap_micros) =
                    spend_cap_bounds(receipt.get("error"), self.budget);
                if is_budget {
                    self.spend_cap = Some(SpendCapStatus {
                        reached: true,
                        spent_micros: Some(spent_micros),
                        limit_micros: Some(cap_micros),
                    });
                }
                persist_terminal(&item.pending_path, &mut item.pending, receipt)?;
                let cursor = self.next_cursor();
                events.push(RemoteEvent::Failed {
                    request_id: item.request_id.clone(),
                    remote_run_id,
                    code,
                    message,
                    status,
                    quota_reset_at_ms,
                    cursor: Some(cursor),
                });
                if is_budget {
                    events.push(RemoteEvent::BudgetExceeded {
                        spent_micros,
                        cap_micros,
                    });
                }
                Ok(false)
            }
            Some("canceled" | "cancelled") => {
                persist_terminal(&item.pending_path, &mut item.pending, receipt)?;
                let cursor = self.next_cursor();
                events.push(RemoteEvent::Cancelled {
                    request_id: item.request_id.clone(),
                    remote_run_id,
                    cursor: Some(cursor),
                });
                Ok(false)
            }
            Some("submitted" | "working" | "input_required" | "queued" | "running" | "pending") => {
                Ok(true)
            }
            _ => Err(proto(
                "Spiron receipt has missing or unknown status; request retained",
            )),
        }
    }
}

impl RemoteSessionProvider for SpironCloudProvider {
    fn capabilities(&self) -> RemoteCapabilities {
        self.capabilities.clone()
    }

    fn create(
        &mut self,
        config: &RemoteSessionConfig,
    ) -> Result<RemoteSessionHandle, RemoteSessionError> {
        if self.closed {
            return Err(RemoteSessionError::Closed);
        }
        if let Some(cap) = config.budget_cap_micros {
            self.set_budget_cap(Some(cap));
        }
        if let (Some(url), Some(tok)) = (&config.mcp_url, &config.mcp_token) {
            self.fallback_mcp = Some((url.clone(), tok.clone()));
        }
        fs::create_dir_all(&self.config.config_dir)
            .map_err(|e| transport(format!("cannot create Spiron config dir: {e}")))?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.config.config_dir.join("spiron-cloud.lock"))
            .map_err(|_| transport("cannot open Spiron config lock"))?;
        lock.try_lock()
            .map_err(|_| proto("another Spiron controller owns this config"))?;

        let token = resolve_api_token(&self.config)?;
        let base_url = self
            .config
            .base_url
            .trim()
            .trim_end_matches('/')
            .to_string();
        let cred_hash = format!(
            "{:x}",
            Sha256::digest(token.as_deref().unwrap_or("").as_bytes())
        );
        bind_origin_and_cred(&self.config.config_dir, &base_url, &cred_hash)?;

        let api = SpironApi::new(base_url.clone(), token, self.config.request_timeout);
        let openapi = match api.get_json("/v1/openapi.json")? {
            HttpOutcome::Ok(val) => val,
            HttpOutcome::Error(err) => return Err(classify_http_error(&err)),
        };
        let strict = declares_next_step(&openapi);
        let capabilities = build_capabilities(strict);
        let (agent_id, fresh) = resolve_or_create_agent(&self.config, &api, config)?;
        let scope = format!("{base_url}/{agent_id}/{cred_hash}");
        let session = OpenSession {
            _lock: lock,
            api,
            agent_id: agent_id.clone(),
            scope,
        };
        let mcp_creds = self.resolve_mcp(None, None)?;
        sync_mcp_servers(
            &self.config.config_dir,
            &session.api,
            &session.agent_id,
            &session.scope,
            mcp_creds,
            fresh,
        )?;
        self.probe_spend_cap(&session.api)?;

        self.strict = strict;
        self.capabilities = capabilities.clone();
        self.session = Some(session);
        Ok(RemoteSessionHandle {
            session_id: agent_id.clone(),
            remote_agent_id: Some(agent_id),
            capabilities,
            cursor: None,
        })
    }

    fn deliver(
        &mut self,
        input: &RemoteDeliveryInput,
    ) -> Result<RemoteDeliveryAck, RemoteSessionError> {
        if self.closed {
            return Err(RemoteSessionError::Closed);
        }
        let Some((agent_id, scope)) = self
            .session
            .as_ref()
            .map(|s| (s.agent_id.clone(), s.scope.clone()))
        else {
            return Err(proto("Spiron cloud session has not been created"));
        };
        if let Some(b) = self.budget
            && b.is_exceeded()
        {
            return Ok(RemoteDeliveryAck::BudgetExceeded {
                spent_micros: b.spent_micros,
                cap_micros: b.cap_micros,
            });
        }
        if let Some(c) = self.spend_cap.as_ref()
            && c.reached
        {
            return Ok(RemoteDeliveryAck::BudgetExceeded {
                spent_micros: c.spent_micros.unwrap_or(0),
                cap_micros: c.limit_micros.unwrap_or(0),
            });
        }
        let request_id = validate_request_id(&input.request_id)?;
        if !self.strict && !self.active.is_empty() && !self.active.contains_key(&request_id) {
            return Ok(RemoteDeliveryAck::Deferred);
        }
        let body = self.build_message_body(input)?;
        let inputs_dir = self.config.config_dir.join("spiron-cloud-inputs");
        fs::create_dir_all(&inputs_dir)
            .map_err(|_| transport("cannot create Spiron input state directory"))?;
        let pending_path = inputs_dir.join(format!("{request_id}.json"));
        let mut pending =
            match self.load_or_create_pending(&request_id, &scope, body, &pending_path)? {
                LoadedPending::Ready(rec) => rec,
                LoadedPending::Reused(ack) => return Ok(ack),
            };

        let post_path = format!("/v1/agents/{agent_id}/messages");
        let receipt = match self.session.as_ref().unwrap().api.post_json(
            &post_path,
            &pending.key,
            &pending.body,
        )? {
            HttpOutcome::Ok(val) => val,
            HttpOutcome::Error(err) => {
                if matches!(err.status, 401 | 429) {
                    return Err(classify_http_error(&err));
                }
                if is_spend_cap_error(&err) {
                    let (spent_micros, cap_micros) =
                        spend_cap_bounds(err.body_json.as_ref(), self.budget);
                    self.spend_cap = Some(SpendCapStatus {
                        reached: true,
                        spent_micros: Some(spent_micros),
                        limit_micros: Some(cap_micros),
                    });
                    let _ = fs::remove_file(&pending_path);
                    return Ok(RemoteDeliveryAck::BudgetExceeded {
                        spent_micros,
                        cap_micros,
                    });
                }
                if (400..500).contains(&err.status) && err.status != 408 {
                    let _ = fs::remove_file(&pending_path);
                    return Ok(classify_rejected_4xx(&err));
                }
                return Err(transport(format!(
                    "Spiron API returned HTTP {}",
                    err.status
                )));
            }
        };

        if receipt.get("agent_id").and_then(Value::as_str) != Some(agent_id.as_str()) {
            return Err(proto("Spiron receipt Agent mismatch; request retained"));
        }
        let receipt_id = validate_uuid(
            receipt.get("id").and_then(Value::as_str).unwrap_or(""),
            "receipt_id",
        )?;
        let run_id = receipt
            .get("run_id")
            .and_then(Value::as_str)
            .map(|s| validate_uuid(s, "run_id"))
            .transpose()?;
        if self.strict
            && receipt.get("provider_message_id").and_then(Value::as_str)
                != Some(pending.key.as_str())
        {
            return Err(proto(
                "Spiron receipt request key mismatch; request retained",
            ));
        }
        let raw_proof = receipt
            .get("input_delivery")
            .filter(|v| !v.is_null())
            .cloned();
        let consumed_proof = if self.strict {
            raw_proof.as_ref().map(validate_proof).transpose()?
        } else {
            None
        };
        pending.receipt = Some(receipt_id.clone());
        pending.run_id = run_id.clone();
        if self.strict && raw_proof.is_some() {
            pending.input_delivery = raw_proof.clone();
        }
        persist_json(&pending_path, &pending, "Spiron pending receipt")?;

        let remote_run_id = run_id.clone().unwrap_or_else(|| receipt_id.clone());
        let deadline = Instant::now() + self.config.turn_budget;
        match receipt.get("status").and_then(Value::as_str) {
            Some("completed") => {
                if self.strict && consumed_proof.is_none() {
                    return Err(proto(
                        "completed Spiron receipt lacks durable input proof; request retained",
                    ));
                }
                let _reply = resolve_completed_reply(
                    &self.session.as_ref().unwrap().api,
                    &agent_id,
                    &receipt_id,
                    &receipt,
                    deadline,
                )?;
                let usage = extract_usage(&receipt);
                let _ = self.record_spend(&usage);
                persist_terminal(&pending_path, &mut pending, receipt)?;
                if self.active.is_empty() {
                    clear_active_run(&self.config.config_dir);
                }
                let session_id = consumed_proof
                    .as_ref()
                    .map(|p| p.session_id.clone())
                    .or(Some(agent_id));
                Ok(RemoteDeliveryAck::Accepted {
                    remote_run_id,
                    session_id,
                    consumed_proof,
                    turn_finished: true,
                    usage: Some(usage),
                })
            }
            Some("submitted" | "working" | "input_required" | "queued" | "running" | "pending") => {
                let _ = persist_json(
                    &self.config.config_dir.join(ACTIVE_RUN_FILE),
                    &ActiveRunState {
                        agent_id,
                        run_id: run_id.clone(),
                        receipt_id: Some(receipt_id.clone()),
                        idempotency_key: Some(pending.key.clone()),
                        started_at_ms: now_epoch_ms(),
                    },
                    "Spiron active run",
                );
                let proof_emitted = consumed_proof.is_some();
                self.active.insert(
                    request_id.clone(),
                    ActiveReceipt {
                        request_id,
                        receipt_id,
                        run_id,
                        pending_path,
                        pending,
                        raw_proof: if self.strict { raw_proof } else { None },
                        proof: consumed_proof.clone(),
                        proof_emitted,
                        deadline,
                        transient_errors: 0,
                    },
                );
                Ok(match consumed_proof {
                    Some(p) => RemoteDeliveryAck::Accepted {
                        remote_run_id,
                        session_id: Some(p.session_id.clone()),
                        consumed_proof: Some(p),
                        turn_finished: false,
                        usage: None,
                    },
                    None => RemoteDeliveryAck::InFlight { remote_run_id },
                })
            }
            Some("failed") => {
                persist_terminal(&pending_path, &mut pending, receipt.clone())?;
                if self.active.is_empty() {
                    clear_active_run(&self.config.config_dir);
                }
                let (code, message, status, quota_reset_at_ms) = classify_receipt_failure(&receipt);
                match status {
                    Some(401) => Err(RemoteSessionError::Auth(message)),
                    Some(429) => Err(RemoteSessionError::Quota {
                        reset_at_ms: quota_reset_at_ms.unwrap_or_else(|| {
                            now_epoch_ms().saturating_add(DEFAULT_RETRY_AFTER_MS)
                        }),
                        message,
                    }),
                    Some(402) => {
                        let (spent_micros, cap_micros) =
                            spend_cap_bounds(receipt.get("error"), self.budget);
                        self.spend_cap = Some(SpendCapStatus {
                            reached: true,
                            spent_micros: Some(spent_micros),
                            limit_micros: Some(cap_micros),
                        });
                        Ok(RemoteDeliveryAck::BudgetExceeded {
                            spent_micros,
                            cap_micros,
                        })
                    }
                    _ => {
                        let cursor = self.next_cursor();
                        self.buffered_events.push(RemoteEvent::Failed {
                            request_id,
                            remote_run_id: remote_run_id.clone(),
                            code,
                            message,
                            status,
                            quota_reset_at_ms,
                            cursor: Some(cursor),
                        });
                        Ok(RemoteDeliveryAck::InFlight { remote_run_id })
                    }
                }
            }
            Some("canceled" | "cancelled") => {
                persist_terminal(&pending_path, &mut pending, receipt)?;
                if self.active.is_empty() {
                    clear_active_run(&self.config.config_dir);
                }
                let cursor = self.next_cursor();
                self.buffered_events.push(RemoteEvent::Cancelled {
                    request_id,
                    remote_run_id: remote_run_id.clone(),
                    cursor: Some(cursor),
                });
                Ok(RemoteDeliveryAck::InFlight { remote_run_id })
            }
            _ => Err(proto(
                "Spiron receipt has missing or unknown status; request retained",
            )),
        }
    }

    fn poll_events(
        &mut self,
        _after_cursor: Option<&str>,
    ) -> Result<Vec<RemoteEvent>, RemoteSessionError> {
        if self.closed {
            return Ok(Vec::new());
        }
        let mut events = std::mem::take(&mut self.buffered_events);
        let Some(agent_id) = self.session.as_ref().map(|s| s.agent_id.clone()) else {
            return Ok(events);
        };
        let keys: Vec<String> = self.active.keys().cloned().collect();
        for key in keys {
            let Some(mut item) = self.active.remove(&key) else {
                continue;
            };
            match self.poll_one_receipt(&mut item, &agent_id, &mut events) {
                Ok(true) => {
                    self.active.insert(key, item);
                }
                Ok(false) => {}
                Err(err) => {
                    self.active.insert(key, item);
                    self.buffered_events.extend(events);
                    return Err(err);
                }
            }
        }
        if self.active.is_empty() {
            clear_active_run(&self.config.config_dir);
        }
        Ok(events)
    }

    fn interrupt(&mut self, remote_run_id: Option<&str>) -> Result<(), RemoteSessionError> {
        if self.closed {
            return Err(RemoteSessionError::Closed);
        }
        let Some(session) = self.session.as_ref() else {
            return Ok(());
        };
        if let Some(target) = remote_run_id {
            let target = validate_uuid(target, "receipt_id")?;
            let matched = self
                .active
                .iter()
                .find(|(_, it)| {
                    it.receipt_id == target || it.run_id.as_deref() == Some(target.as_str())
                })
                .map(|(k, it)| (k.clone(), it.receipt_id.clone()));
            let cancel_id = matched
                .as_ref()
                .map_or(target.as_str(), |(_, id)| id.as_str());
            session.api.cancel_receipt(cancel_id)?;
            if let Some((key, _)) = matched {
                self.active.remove(&key);
            }
            if self.active.is_empty() {
                clear_active_run(&self.config.config_dir);
            }
            return Ok(());
        }
        let targets: Vec<String> = self
            .active
            .values()
            .map(|it| it.receipt_id.clone())
            .collect();
        for id in targets {
            session.api.cancel_receipt(&id)?;
        }
        self.active.clear();
        clear_active_run(&self.config.config_dir);
        Ok(())
    }

    fn stop(&mut self) -> Result<(), RemoteSessionError> {
        if self.closed {
            return Ok(());
        }
        if let Some(session) = self.session.as_ref() {
            for item in self.active.values() {
                let _ = session.api.cancel_receipt(&item.receipt_id);
            }
        }
        self.active.clear();
        self.buffered_events.clear();
        clear_active_run(&self.config.config_dir);
        self.session = None;
        self.closed = true;
        Ok(())
    }

    fn budget(&self) -> Option<RemoteBudget> {
        self.budget
    }

    fn set_budget_cap(&mut self, cap_micros: Option<u64>) {
        self.config.budget_cap_micros = cap_micros;
        self.budget = cap_micros.map(|cap| RemoteBudget {
            cap_micros: cap,
            spent_micros: self.budget.map_or(0, |b| b.spent_micros),
        });
    }

    fn refresh_mcp_credentials(&self) -> Option<(String, String)> {
        self.resolve_mcp(None, None).ok().flatten()
    }
}
