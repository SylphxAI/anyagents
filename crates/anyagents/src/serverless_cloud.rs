//! Universal Serverless Cloud Harness Driver (`CloudTurnDriver`).
//!
//! Decouples asynchronous cloud agent turn submission from completion so a
//! stateless serverless coordinator holds ZERO open SSE streams or background
//! polling tasks across HTTP requests:
//! - [`SpironServerlessDriver`]: live Spiron Agents API (`POST /v1/agents/{id}/messages`
//!   with per-turn `mcp_servers`, `GET /v1/message-receipts/{task_id}`,
//!   `GET /v1/message-receipts/{task_id}/events`, `POST /v1/message-receipts/{task_id}:cancel`,
//!   and `/v1/webhooks` push event parsing).
//! - [`GenericHttpCloudDriver`]: vendor-neutral open HTTP contract (`POST /turns`,
//!   `GET /turns/{id}`, `POST /turns/{id}:cancel`, webhook callback) supporting any
//!   third-party cloud agent harness (Codex Cloud, Claude Cloud, Devin, custom SaaS).

use std::collections::{BTreeMap, HashMap};
use std::io::Read as _;
use std::time::Duration;

use anyagents_core::{constant_time_eq, verify_hmac_sha256_signature};
use anyagents_harness_spiron::cloud::operator_cloud_url;
use anyagents_harness_spiron::inference_key::{BYOK_ENV_SHORT, InferenceKey};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

mod spiron_poll;
mod transcript_events;

pub use transcript_events::CloudTranscriptEvent;
pub(crate) use transcript_events::parse_cloud_transcript_event;
use transcript_events::parse_webhook_transcript_events;

const DEFAULT_HTTP_TIMEOUT_SECS: u64 = 15;
const DEFAULT_NEXT_POLL_MS: u64 = 5_000;

/// Lifecycle state reported by a cloud harness turn submission, poll, or webhook.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloudTurnState {
    Dispatched,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl CloudTurnState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Dispatched => "dispatched",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

/// Request payload passed to [`CloudTurnDriver::submit_turn`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CloudTurnSubmitRequest {
    /// Coordinator-assigned unique turn ID (also sent as `Idempotency-Key`).
    pub turn_id: String,
    /// AnyAgents agent ID.
    pub agent_id: String,
    /// Optional provider-side agent/session ID (e.g. Spiron remote agent UUID).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_agent_id: Option<String>,
    /// User prompt content for this turn.
    pub prompt: String,
    /// Optional model identifier override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Optional system instructions (used when auto-creating a remote agent).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    /// Optional per-turn Streamable HTTP MCP servers attachment (`mcp_servers`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_servers: Option<Value>,
    /// Optional coordinator callback URL (`<PUBLIC_URL>/v1/internal/cloud/events`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub callback_url: Option<String>,
    /// Optional per-turn callback bearer token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub callback_token: Option<String>,
    /// Optional non-secret routing metadata.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, Value>,
}

/// Response returned by [`CloudTurnDriver::submit_turn`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CloudTurnSubmission {
    /// Provider-assigned run/receipt/task identifier (`pending_remote_turns.remote_run_id`).
    pub remote_run_id: String,
    /// Remote agent/session identifier (populated if newly created or returned by provider).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_agent_id: Option<String>,
    /// Initial state returned by the provider (`Dispatched`/`Running` or already terminal).
    pub status: CloudTurnState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_poll_after_ms: Option<u64>,
}

/// Response returned by [`CloudTurnDriver::poll_turn`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CloudTurnPollResult {
    pub remote_run_id: String,
    pub status: CloudTurnState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deltas: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transcript_events: Vec<CloudTranscriptEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_poll_after_ms: Option<u64>,
}

/// Request payload passed to [`CloudTurnDriver::steer_turn`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CloudTurnSteerRequest {
    /// Coordinator-assigned active turn ID.
    pub turn_id: String,
    /// AnyAgents agent ID.
    pub agent_id: String,
    /// Provider-side remote agent/session ID (e.g. Spiron remote agent UUID).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_agent_id: Option<String>,
    /// Active provider-assigned run/receipt/task ID.
    pub remote_run_id: String,
    /// Durable mailbox message ID (sent as `Idempotency-Key`).
    pub message_id: String,
    /// Monotonic mailbox sequence number.
    pub seq: i64,
    /// Full composed message text to inject mid-turn.
    pub prompt: String,
    /// Optional per-turn Streamable HTTP MCP servers attachment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_servers: Option<Value>,
}

/// Verified outcome returned by [`CloudTurnDriver::steer_turn`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CloudSteerAck {
    /// Provider proved the steer entered the running turn (safe to advance `delivered_seq`).
    Consumed {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        receipt_id: Option<String>,
        proof: String,
    },
    /// Provider accepted the steer asynchronously without a consumption proof yet.
    /// Fail-closed: coordinator keeps `delivered_seq` unadvanced until consumption proof arrives.
    Accepted {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        receipt_id: Option<String>,
    },
    /// Provider refused or does not support steering this active turn.
    /// Message remains safely undelivered in Postgres for turn-end drain.
    Refused { reason: String },
}

/// Normalized event returned by [`CloudTurnDriver::parse_webhook`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CloudWebhookEvent {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    pub remote_run_id: String,
    pub harness_kind: String,
    pub event_type: String,
    pub status: CloudTurnState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steer_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steer_seq: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steer_proof: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transcript_events: Vec<CloudTranscriptEvent>,
}

/// Universal async driver contract for pure serverless cloud agent harnesses.
#[async_trait]
pub trait CloudTurnDriver: Send + Sync {
    /// Canonical `pending_remote_turns.harness_kind` value (`"spiron"`, `"http_cloud"`, ...).
    fn harness_kind(&self) -> &'static str;

    /// Whether this driver has live native mid-turn steering wired.
    #[allow(dead_code)]
    fn supports_steer(&self) -> bool {
        false
    }

    /// Submit a turn asynchronously (`Prefer: respond-async`, zero long-lived SSE).
    async fn submit_turn(
        &self,
        req: &CloudTurnSubmitRequest,
    ) -> Result<CloudTurnSubmission, String>;

    /// Optional: push a mid-turn steer message into an active cloud turn.
    /// Default implementation fails closed (`Refused`) so messages stay safely
    /// undelivered for turn-end drain.
    async fn steer_turn(&self, _req: &CloudTurnSteerRequest) -> Result<CloudSteerAck, String> {
        Ok(CloudSteerAck::Refused {
            reason: format!(
                "cloud driver `{}` does not support mid-turn steering",
                self.harness_kind()
            ),
        })
    }

    /// Poll a pending cloud turn on `/v1/internal/tick` fallback.
    async fn poll_turn(
        &self,
        remote_run_id: &str,
        cursor: Option<&str>,
    ) -> Result<CloudTurnPollResult, String>;

    /// Request remote cancellation for an in-flight cloud turn.
    #[allow(dead_code)]
    async fn cancel_turn(&self, remote_run_id: &str) -> Result<(), String>;

    /// Verify (when configured) and parse a provider push webhook payload.
    fn parse_webhook(
        &self,
        headers: &[(String, String)],
        body: &[u8],
    ) -> Result<CloudWebhookEvent, String>;
}

fn validate_path_segment(id: &str, label: &str) -> Result<String, String> {
    let trimmed = id.trim();
    if trimmed.is_empty()
        || trimmed.len() > 200
        || trimmed.contains('/')
        || trimmed.contains('\\')
        || trimmed.contains("..")
        || trimmed.contains('?')
        || trimmed.contains('#')
        || trimmed.bytes().any(|b| b.is_ascii_control() || b == b' ')
    {
        return Err(format!("invalid {label} `{id}`"));
    }
    Ok(trimmed.to_string())
}

fn build_http_agent(timeout: Duration) -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(timeout))
        .http_status_as_error(false)
        .build()
        .into()
}

fn http_json(
    method: &str,
    url: &str,
    headers: &[(String, String)],
    body: Option<&Value>,
    timeout_secs: u64,
) -> Result<(u16, Value), String> {
    let agent = build_http_agent(Duration::from_secs(timeout_secs.max(1)));
    let mut resp = match method {
        "GET" => {
            let mut req = agent.get(url);
            for (k, v) in headers {
                req = req.header(k.as_str(), v.as_str());
            }
            req.call()
        }
        "POST" => {
            let mut req = agent.post(url);
            for (k, v) in headers {
                req = req.header(k.as_str(), v.as_str());
            }
            if let Some(payload) = body {
                req.send_json(payload)
            } else {
                req.send_empty()
            }
        }
        other => return Err(format!("unsupported HTTP method `{other}`")),
    }
    .map_err(|e| format!("HTTP {method} {url} failed: {e}"))?;

    let status = resp.status().as_u16();
    let mut raw = Vec::new();
    let _ = resp
        .body_mut()
        .as_reader()
        .take(2 * 1024 * 1024)
        .read_to_end(&mut raw);
    let parsed = if raw.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&raw)
            .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&raw).trim().to_string() }))
    };
    Ok((status, parsed))
}

fn header_ci<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

fn verify_optional_webhook_secret(
    secret: Option<&str>,
    headers: &[(String, String)],
    body: &[u8],
    sig_header_names: &[&str],
) -> Result<(), String> {
    let Some(secret) = secret.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(());
    };
    for hdr_name in sig_header_names {
        if let Some(sig) = header_ci(headers, hdr_name)
            && verify_hmac_sha256_signature(secret.as_bytes(), body, sig)
        {
            return Ok(());
        }
    }
    if let Some(auth) = header_ci(headers, "authorization") {
        let trimmed = auth.trim();
        let tok = trimmed
            .strip_prefix("Bearer ")
            .or_else(|| trimmed.strip_prefix("bearer "))
            .map(str::trim)
            .unwrap_or("");
        if !tok.is_empty() && constant_time_eq(secret.as_bytes(), tok.as_bytes()) {
            return Ok(());
        }
    }
    Err("cloud webhook authentication failed".to_string())
}

fn map_status_str(raw: Option<&str>) -> CloudTurnState {
    match raw.unwrap_or("").trim().to_ascii_lowercase().as_str() {
        "completed" | "complete" | "succeeded" | "success" | "done" => CloudTurnState::Completed,
        "failed" | "error" | "errored" | "timed_out" | "timeout" => CloudTurnState::Failed,
        "cancelled" | "canceled" | "aborted" => CloudTurnState::Cancelled,
        "dispatched" | "submitted" | "queued" | "pending" | "accepted" => {
            CloudTurnState::Dispatched
        }
        _ => CloudTurnState::Running,
    }
}

fn extract_error_string(val: &Value) -> Option<String> {
    if let Some(s) = val
        .get("error_text")
        .or_else(|| val.get("errorMessage"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        return Some(s.to_string());
    }
    match val.get("error") {
        Some(Value::String(s)) if !s.trim().is_empty() => Some(s.trim().to_string()),
        Some(Value::Object(map)) => map
            .get("message")
            .or_else(|| map.get("detail"))
            .or_else(|| map.get("code"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or_else(|| Some(Value::Object(map.clone()).to_string())),
        _ => None,
    }
}

fn extract_result_string(val: &Value) -> Option<String> {
    for key in [
        "result_text",
        "reply",
        "output_text",
        "result",
        "text",
        "content",
    ] {
        if let Some(s) = val
            .get(key)
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
        {
            return Some(s.to_string());
        }
    }
    None
}

// ============================================================================
// SpironServerlessDriver (Live Spiron Agents API)
// ============================================================================

/// Pure serverless driver for the live Spiron Agents API.
///
/// - Submit: `POST /v1/agents/{agent_id}/messages` (`Prefer: respond-async`, per-turn `mcp_servers`)
/// - Poll: `GET /v1/message-receipts/{task_id}` + `GET /v1/message-receipts/{task_id}/events`
/// - Cancel: `POST /v1/message-receipts/{task_id}:cancel`
/// - Push Webhook: `/v1/webhooks` HMAC-SHA256 / Bearer verification + event parsing
#[derive(Clone)]
pub struct SpironServerlessDriver {
    pub base_url: String,
    pub auth_token: Option<String>,
    pub remote_agent_id: Option<String>,
    inference_key: Option<InferenceKey>,
    pub webhook_secret: Option<String>,
    pub timeout_secs: u64,
}

impl std::fmt::Debug for SpironServerlessDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpironServerlessDriver")
            .field("base_url", &self.base_url)
            .field(
                "auth_token",
                &self.auth_token.as_ref().map(|_| "[redacted]"),
            )
            .field("remote_agent_id", &self.remote_agent_id)
            .field(
                "webhook_secret",
                &self.webhook_secret.as_ref().map(|_| "[redacted]"),
            )
            .field("timeout_secs", &self.timeout_secs)
            .finish()
    }
}

impl SpironServerlessDriver {
    pub fn new(base_url: impl Into<String>, auth_token: Option<String>) -> Self {
        Self {
            base_url: base_url.into().trim().trim_end_matches('/').to_string(),
            auth_token: auth_token
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
            remote_agent_id: None,
            inference_key: None,
            webhook_secret: None,
            timeout_secs: DEFAULT_HTTP_TIMEOUT_SECS,
        }
    }

    fn with_inference_key(mut self, key: Option<InferenceKey>) -> Self {
        self.inference_key = key;
        self
    }

    pub fn with_remote_agent_id(mut self, remote_agent_id: Option<String>) -> Self {
        self.remote_agent_id = remote_agent_id
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        self
    }

    pub fn with_webhook_secret(mut self, webhook_secret: Option<String>) -> Self {
        self.webhook_secret = webhook_secret
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        self
    }

    fn auth_headers(&self) -> Vec<(String, String)> {
        let mut h = vec![
            ("accept".to_string(), "application/json".to_string()),
            ("content-type".to_string(), "application/json".to_string()),
        ];
        if let Some(ref tok) = self.auth_token {
            h.push(("authorization".to_string(), format!("Bearer {tok}")));
        }
        h
    }
}

fn classify_cloud_steer_response(
    provider_label: &str,
    status: u16,
    body: &Value,
) -> Result<CloudSteerAck, String> {
    if matches!(status, 404 | 405 | 409 | 410 | 422 | 501) {
        let detail = extract_error_string(body)
            .unwrap_or_else(|| format!("{provider_label} steer refused (HTTP {status})"));
        return Ok(CloudSteerAck::Refused { reason: detail });
    }
    if !(200..300).contains(&status) {
        return Err(format!("{provider_label} steer returned HTTP {status}"));
    }
    let receipt_id = body
        .get("id")
        .or_else(|| body.get("receipt_id"))
        .or_else(|| body.get("task_id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let status_str = body
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    if matches!(
        status_str.as_str(),
        "refused" | "rejected" | "deferred" | "failed" | "cancelled" | "canceled"
    ) {
        let reason = extract_error_string(body)
            .unwrap_or_else(|| format!("{provider_label} steer status `{status_str}`"));
        return Ok(CloudSteerAck::Refused { reason });
    }
    let has_input_delivery = body.get("input_delivery").is_some_and(|v| !v.is_null());
    let explicit_consumed = body.get("consumed").and_then(Value::as_bool) == Some(true)
        || body.get("steered").and_then(Value::as_bool) == Some(true)
        || matches!(
            status_str.as_str(),
            "consumed" | "steered" | "delivered" | "completed"
        );
    if has_input_delivery || explicit_consumed {
        let proof = body
            .get("proof")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| {
                if has_input_delivery {
                    format!("{provider_label}:input_delivery")
                } else {
                    format!("{provider_label}:steered")
                }
            });
        return Ok(CloudSteerAck::Consumed { receipt_id, proof });
    }
    Ok(CloudSteerAck::Accepted { receipt_id })
}

fn extract_webhook_steer_fields(
    data: &Value,
    payload: &Value,
    event_type: &str,
) -> (Option<String>, Option<i64>, Option<String>) {
    let lower_ev = event_type.trim().to_ascii_lowercase();
    let has_input_delivery = data.get("input_delivery").is_some_and(|v| !v.is_null())
        || payload.get("input_delivery").is_some_and(|v| !v.is_null());
    let is_steer_event = matches!(
        lower_ev.as_str(),
        "steer_consumed" | "message.steered" | "turn.steered" | "input_delivery"
    ) || has_input_delivery;
    if !is_steer_event {
        return (None, None, None);
    }
    let msg_id = data
        .get("message_id")
        .or_else(|| data.get("provider_message_id"))
        .or_else(|| data.get("idempotency_key"))
        .or_else(|| payload.get("message_id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let seq = data
        .get("seq")
        .or_else(|| data.get("message_seq"))
        .or_else(|| payload.get("seq"))
        .and_then(Value::as_i64);
    let proof = data
        .get("proof")
        .or_else(|| payload.get("proof"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| has_input_delivery.then(|| "cloud:input_delivery".to_string()))
        .or_else(|| Some(format!("cloud:{lower_ev}")));
    (msg_id, seq, proof)
}

#[async_trait]
impl CloudTurnDriver for SpironServerlessDriver {
    fn harness_kind(&self) -> &'static str {
        "spiron"
    }

    fn supports_steer(&self) -> bool {
        true
    }

    async fn submit_turn(
        &self,
        req: &CloudTurnSubmitRequest,
    ) -> Result<CloudTurnSubmission, String> {
        let driver = self.clone();
        let req = req.clone();
        tokio::task::spawn_blocking(move || {
            if driver.inference_key.is_some() {
                InferenceKey::validate_target(&driver.base_url, driver.auth_token.as_deref())
                    .map_err(|err| err.to_string())?;
            }
            let mut remote_agent_id = req
                .remote_agent_id
                .clone()
                .or_else(|| driver.remote_agent_id.clone())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());

            // Auto-create a remote Spiron agent when none has been bound yet.
            if remote_agent_id.is_none() {
                let mut create_body = json!({
                    "name": format!("anyagents-{}", req.agent_id),
                });
                if let Some(ref m) = req.model {
                    create_body["model"] = json!(m);
                }
                if let Some(ref instr) = req.instructions {
                    create_body["instructions"] = json!(instr);
                }
                let mut headers = driver.auth_headers();
                headers.push((
                    "Idempotency-Key".to_string(),
                    format!("create-{}", req.turn_id),
                ));
                let create_url = format!("{}/v1/agents", driver.base_url);
                let (status, created) = http_json(
                    "POST",
                    &create_url,
                    &headers,
                    Some(&create_body),
                    driver.timeout_secs,
                )?;
                if !(200..300).contains(&status) {
                    return Err(format!("Spiron POST /v1/agents returned HTTP {status}"));
                }
                let created_id = created
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "Spiron POST /v1/agents response missing `id`".to_string())?;
                remote_agent_id = Some(validate_path_segment(created_id, "remote_agent_id")?);
            }

            let target_agent =
                validate_path_segment(remote_agent_id.as_deref().unwrap_or(""), "remote_agent_id")?;
            if let Some(key) = &driver.inference_key {
                key.install(
                    &driver.base_url,
                    driver.auth_token.as_deref(),
                    &target_agent,
                    Duration::from_secs(driver.timeout_secs),
                )
                .map_err(|err| err.to_string())?;
            }
            let mut msg_body = json!({ "content": req.prompt });
            // Per-turn Streamable HTTP MCP attachment (#5421).
            if let Some(ref mcp_servers) = req.mcp_servers {
                msg_body["mcp_servers"] = mcp_servers.clone();
            }

            let mut headers = driver.auth_headers();
            headers.push(("Idempotency-Key".to_string(), req.turn_id.clone()));
            headers.push(("Prefer".to_string(), "respond-async".to_string()));

            let url = format!("{}/v1/agents/{target_agent}/messages", driver.base_url);
            let (status, receipt) =
                http_json("POST", &url, &headers, Some(&msg_body), driver.timeout_secs)?;
            if !(200..300).contains(&status) {
                return Err(format!(
                    "Spiron POST /v1/agents/{target_agent}/messages returned HTTP {status}"
                ));
            }

            let task_id = receipt
                .get("id")
                .or_else(|| receipt.get("task_id"))
                .or_else(|| receipt.get("receipt_id"))
                .and_then(Value::as_str)
                .ok_or_else(|| "Spiron message receipt missing `id`/`task_id`".to_string())?;
            let task_id = validate_path_segment(task_id, "task_id")?;
            let state = map_status_str(receipt.get("status").and_then(Value::as_str));
            let result_text = extract_result_string(&receipt);
            let error_text = extract_error_string(&receipt);
            let usage = receipt.get("usage").cloned();
            let cursor = receipt
                .get("cursor")
                .and_then(Value::as_str)
                .map(str::to_string);

            Ok(CloudTurnSubmission {
                remote_run_id: task_id,
                remote_agent_id: Some(target_agent),
                status: state,
                result_text,
                error_text,
                usage,
                cursor,
                next_poll_after_ms: (!state.is_terminal()).then_some(DEFAULT_NEXT_POLL_MS),
            })
        })
        .await
        .map_err(|e| format!("spiron submit task join error: {e}"))?
    }

    async fn steer_turn(&self, req: &CloudTurnSteerRequest) -> Result<CloudSteerAck, String> {
        if self.inference_key.is_some() {
            // An active turn may already have selected a different credential.
            return Ok(CloudSteerAck::Refused {
                reason: "Spiron inference-key inputs wait for the next turn".into(),
            });
        }
        let driver = self.clone();
        let req = req.clone();
        tokio::task::spawn_blocking(move || {
            let remote_agent_id = req
                .remote_agent_id
                .clone()
                .or_else(|| driver.remote_agent_id.clone())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            let Some(raw_agent) = remote_agent_id else {
                return Ok(CloudSteerAck::Refused {
                    reason: "Spiron active turn missing remote_agent_id for mid-turn steer"
                        .to_string(),
                });
            };
            let target_agent = validate_path_segment(&raw_agent, "remote_agent_id")?;
            let remote_run_id = validate_path_segment(&req.remote_run_id, "remote_run_id")?;
            let msg_id = validate_path_segment(&req.message_id, "message_id")?;

            let mut msg_body = json!({ "content": req.prompt });
            if let Some(ref mcp_servers) = req.mcp_servers {
                msg_body["mcp_servers"] = mcp_servers.clone();
            }

            let mut headers = driver.auth_headers();
            headers.push(("Idempotency-Key".to_string(), msg_id));
            headers.push(("Prefer".to_string(), "respond-async".to_string()));
            headers.push(("X-AnyAgents-Delivery".to_string(), "steer".to_string()));
            headers.push(("X-AnyAgents-Run-Id".to_string(), remote_run_id));

            let url = format!("{}/v1/agents/{target_agent}/messages", driver.base_url);
            let (status, resp) =
                http_json("POST", &url, &headers, Some(&msg_body), driver.timeout_secs)?;
            classify_cloud_steer_response("spiron", status, &resp)
        })
        .await
        .map_err(|e| format!("spiron steer task join error: {e}"))?
    }

    async fn poll_turn(
        &self,
        remote_run_id: &str,
        cursor: Option<&str>,
    ) -> Result<CloudTurnPollResult, String> {
        let driver = self.clone();
        let task_id = validate_path_segment(remote_run_id, "task_id")?;
        let cursor_owned = cursor.map(str::to_string);
        tokio::task::spawn_blocking(move || {
            let headers = driver.auth_headers();
            let receipt_url = format!("{}/v1/message-receipts/{task_id}", driver.base_url);
            let (status, receipt) =
                http_json("GET", &receipt_url, &headers, None, driver.timeout_secs)?;
            if !(200..300).contains(&status) {
                return Err(format!(
                    "Spiron GET /v1/message-receipts/{task_id} returned HTTP {status}"
                ));
            }

            let durable_state = map_status_str(receipt.get("status").and_then(Value::as_str));
            let mut result_text = spiron_poll::reply_text(&receipt);
            let mut error_text = spiron_poll::error_text(&receipt);
            let has_durable_failure = matches!(
                durable_state,
                CloudTurnState::Failed | CloudTurnState::Cancelled
            );
            let mut events = spiron_poll::PollEvents::new(cursor_owned);
            if !(has_durable_failure
                || durable_state == CloudTurnState::Completed && result_text.is_some())
            {
                let events_url = format!("{receipt_url}/events");
                events.read(&events_url, &headers);
            }
            let mut state = if durable_state.is_terminal() {
                durable_state
            } else {
                events.terminal.unwrap_or(durable_state)
            };
            result_text = result_text.or(events.reply);
            if !durable_state.is_terminal() && events.terminal.is_some() {
                error_text = events.error;
            }
            let usage = receipt.get("usage").cloned().or(events.usage);

            // If completed without inline reply/events reply, check agent conversation projection.
            if state == CloudTurnState::Completed
                && result_text.is_none()
                && let Some(remote_agent) = driver.remote_agent_id.as_deref().or_else(|| {
                    receipt
                        .get("agent_id")
                        .and_then(Value::as_str)
                        .filter(|s| !s.trim().is_empty())
                })
                && let Ok(valid_agent) = validate_path_segment(remote_agent, "agent_id")
            {
                let conv_url = format!(
                    "{}/v1/agents/{valid_agent}/conversation?limit=50",
                    driver.base_url
                );
                if let Ok((200..=299, conv)) =
                    http_json("GET", &conv_url, &headers, None, driver.timeout_secs)
                    && let Some(items) = conv
                        .get("data")
                        .and_then(Value::as_array)
                        .or_else(|| conv.get("items").and_then(Value::as_array))
                {
                    let found = items.iter().rev().find_map(|item| {
                        let matches_task = item.get("task_id").and_then(Value::as_str)
                            == Some(task_id.as_str())
                            || item.get("receipt_id").and_then(Value::as_str)
                                == Some(task_id.as_str());
                        let is_assistant = matches!(
                            item.get("role").and_then(Value::as_str),
                            Some("agent" | "assistant")
                        );
                        if is_assistant && matches_task {
                            spiron_poll::reply_text(item)
                        } else {
                            None
                        }
                    });
                    if found.is_some() {
                        result_text = found;
                    }
                }
            }

            if state == CloudTurnState::Completed && result_text.is_none() {
                // A finite legacy snapshot may contain the complete reply as deltas.
                // SSE replay may be partial, so never publish just its last window.
                result_text =
                    (events.snapshot && !events.deltas.is_empty()).then(|| events.deltas.concat());
                if result_text.is_none() {
                    state = CloudTurnState::Failed;
                    error_text =
                        Some("Spiron completed the receipt without an assistant reply".into());
                }
            }
            if matches!(state, CloudTurnState::Failed | CloudTurnState::Cancelled) {
                result_text = None;
                error_text
                    .get_or_insert_with(|| format!("Spiron task {task_id} {}", state.as_str()));
            }
            Ok(CloudTurnPollResult {
                remote_run_id: task_id,
                status: state,
                result_text,
                error_text,
                usage,
                cursor: events.cursor,
                deltas: events.deltas,
                transcript_events: events.transcript_events,
                next_poll_after_ms: (!state.is_terminal()).then_some(DEFAULT_NEXT_POLL_MS),
            })
        })
        .await
        .map_err(|e| format!("spiron poll task join error: {e}"))?
    }

    async fn cancel_turn(&self, remote_run_id: &str) -> Result<(), String> {
        let driver = self.clone();
        let task_id = validate_path_segment(remote_run_id, "task_id")?;
        tokio::task::spawn_blocking(move || {
            let headers = driver.auth_headers();
            let url = format!("{}/v1/message-receipts/{task_id}:cancel", driver.base_url);
            let (status, _) = http_json("POST", &url, &headers, None, driver.timeout_secs)?;
            if status == 409 || (200..300).contains(&status) {
                return Ok(());
            }
            Err(format!(
                "Spiron POST /v1/message-receipts/{task_id}:cancel returned HTTP {status}"
            ))
        })
        .await
        .map_err(|e| format!("spiron cancel task join error: {e}"))?
    }

    fn parse_webhook(
        &self,
        headers: &[(String, String)],
        body: &[u8],
    ) -> Result<CloudWebhookEvent, String> {
        verify_optional_webhook_secret(
            self.webhook_secret.as_deref(),
            headers,
            body,
            &[
                "x-spiron-signature-256",
                "x-webhook-signature-256",
                "x-hub-signature-256",
            ],
        )?;

        let payload: Value = serde_json::from_slice(body)
            .map_err(|e| format!("invalid Spiron webhook JSON: {e}"))?;
        let data = payload
            .get("data")
            .filter(|v| v.is_object())
            .unwrap_or(&payload);

        let event_type = payload
            .get("event")
            .or_else(|| payload.get("event_type"))
            .or_else(|| payload.get("type"))
            .and_then(Value::as_str)
            .unwrap_or("message.updated")
            .to_string();

        let remote_run_id = data
            .get("task_id")
            .or_else(|| data.get("receipt_id"))
            .or_else(|| data.get("id"))
            .or_else(|| payload.get("task_id"))
            .or_else(|| payload.get("receipt_id"))
            .or_else(|| payload.get("id"))
            .and_then(Value::as_str)
            .ok_or_else(|| "Spiron webhook missing task_id / receipt_id / id".to_string())?;
        let remote_run_id = validate_path_segment(remote_run_id, "remote_run_id")?;

        let turn_id = data
            .get("turn_id")
            .or_else(|| data.get("provider_message_id"))
            .or_else(|| data.get("idempotency_key"))
            .or_else(|| payload.get("turn_id"))
            .or_else(|| payload.get("provider_message_id"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);

        let explicit_status = data
            .get("status")
            .or_else(|| payload.get("status"))
            .and_then(Value::as_str);
        let status = match explicit_status {
            Some(s) => map_status_str(Some(s)),
            None => match event_type.to_ascii_lowercase().as_str() {
                "message.completed" | "turn.completed" | "run.completed" | "turn_complete" => {
                    CloudTurnState::Completed
                }
                "message.failed" | "turn.failed" | "run.failed" | "turn_failed" => {
                    CloudTurnState::Failed
                }
                "message.canceled" | "message.cancelled" | "turn.cancelled" | "run.cancelled"
                | "turn_cancelled" => CloudTurnState::Cancelled,
                _ => CloudTurnState::Running,
            },
        };

        let result_text = extract_result_string(data).or_else(|| extract_result_string(&payload));
        let error_text = extract_error_string(data).or_else(|| extract_error_string(&payload));
        let usage = data.get("usage").or_else(|| payload.get("usage")).cloned();
        let cursor = data
            .get("cursor")
            .or_else(|| payload.get("cursor"))
            .and_then(Value::as_str)
            .map(str::to_string);

        let (steer_message_id, steer_seq, steer_proof) =
            extract_webhook_steer_fields(data, &payload, &event_type);
        let transcript_events =
            parse_webhook_transcript_events(&event_type, cursor.as_deref(), &payload);

        Ok(CloudWebhookEvent {
            turn_id,
            remote_run_id,
            harness_kind: self.harness_kind().to_string(),
            event_type,
            status,
            result_text,
            error_text,
            usage,
            cursor,
            steer_message_id,
            steer_seq,
            steer_proof,
            transcript_events,
        })
    }
}

// ============================================================================
// GenericHttpCloudDriver (Vendor-Neutral Open HTTP Contract)
// ============================================================================

/// Vendor-neutral serverless cloud agent driver implementing the open HTTP contract:
/// - `POST /turns` -> `{ id / remote_run_id, status, ... }`
/// - `GET /turns/{id}` (optional `?cursor=...`) -> `{ id, status, result_text, error_text, usage, cursor, deltas }`
/// - `POST /turns/{id}:cancel`
/// - Push webhook callback -> `{ turn_id, remote_run_id / id, event_type, status, result_text, error_text, usage, cursor }`
#[derive(Clone)]
pub struct GenericHttpCloudDriver {
    pub base_url: String,
    pub auth_token: Option<String>,
    pub webhook_secret: Option<String>,
    pub timeout_secs: u64,
}

impl std::fmt::Debug for GenericHttpCloudDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericHttpCloudDriver")
            .field("base_url", &self.base_url)
            .field(
                "auth_token",
                &self.auth_token.as_ref().map(|_| "[redacted]"),
            )
            .field(
                "webhook_secret",
                &self.webhook_secret.as_ref().map(|_| "[redacted]"),
            )
            .field("timeout_secs", &self.timeout_secs)
            .finish()
    }
}

impl GenericHttpCloudDriver {
    pub fn new(base_url: impl Into<String>, auth_token: Option<String>) -> Self {
        Self {
            base_url: base_url.into().trim().trim_end_matches('/').to_string(),
            auth_token: auth_token
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
            webhook_secret: None,
            timeout_secs: DEFAULT_HTTP_TIMEOUT_SECS,
        }
    }

    pub fn with_webhook_secret(mut self, webhook_secret: Option<String>) -> Self {
        self.webhook_secret = webhook_secret
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        self
    }

    fn auth_headers(&self) -> Vec<(String, String)> {
        let mut h = vec![
            ("accept".to_string(), "application/json".to_string()),
            ("content-type".to_string(), "application/json".to_string()),
        ];
        if let Some(ref tok) = self.auth_token {
            h.push(("authorization".to_string(), format!("Bearer {tok}")));
        }
        h
    }
}

#[async_trait]
impl CloudTurnDriver for GenericHttpCloudDriver {
    fn harness_kind(&self) -> &'static str {
        "http_cloud"
    }

    async fn submit_turn(
        &self,
        req: &CloudTurnSubmitRequest,
    ) -> Result<CloudTurnSubmission, String> {
        let driver = self.clone();
        let req = req.clone();
        tokio::task::spawn_blocking(move || {
            let mut headers = driver.auth_headers();
            headers.push(("Idempotency-Key".to_string(), req.turn_id.clone()));
            let payload = serde_json::to_value(&req).map_err(|e| e.to_string())?;
            let url = format!("{}/turns", driver.base_url);
            let (status, resp) =
                http_json("POST", &url, &headers, Some(&payload), driver.timeout_secs)?;
            if !(200..300).contains(&status) {
                return Err(format!("Cloud HTTP POST /turns returned HTTP {status}"));
            }
            let run_id = resp
                .get("remote_run_id")
                .or_else(|| resp.get("id"))
                .or_else(|| resp.get("run_id"))
                .or_else(|| resp.get("turn_id"))
                .and_then(Value::as_str)
                .ok_or_else(|| "Cloud HTTP POST /turns response missing `id`".to_string())?;
            let run_id = validate_path_segment(run_id, "remote_run_id")?;
            let state = map_status_str(resp.get("status").and_then(Value::as_str));
            let remote_agent_id = resp
                .get("remote_agent_id")
                .or_else(|| resp.get("session_id"))
                .and_then(Value::as_str)
                .map(str::to_string);
            Ok(CloudTurnSubmission {
                remote_run_id: run_id,
                remote_agent_id,
                status: state,
                result_text: extract_result_string(&resp),
                error_text: extract_error_string(&resp),
                usage: resp.get("usage").cloned(),
                cursor: resp
                    .get("cursor")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                next_poll_after_ms: (!state.is_terminal()).then_some(DEFAULT_NEXT_POLL_MS),
            })
        })
        .await
        .map_err(|e| format!("http_cloud submit task join error: {e}"))?
    }

    async fn steer_turn(&self, req: &CloudTurnSteerRequest) -> Result<CloudSteerAck, String> {
        let driver = self.clone();
        let req = req.clone();
        tokio::task::spawn_blocking(move || {
            let run_id = validate_path_segment(&req.remote_run_id, "remote_run_id")?;
            let msg_id = validate_path_segment(&req.message_id, "message_id")?;
            let mut headers = driver.auth_headers();
            headers.push(("Idempotency-Key".to_string(), msg_id));
            let payload = serde_json::to_value(&req).map_err(|e| e.to_string())?;
            let url = format!("{}/turns/{run_id}/steer", driver.base_url);
            let (status, resp) =
                http_json("POST", &url, &headers, Some(&payload), driver.timeout_secs)?;
            classify_cloud_steer_response("http_cloud", status, &resp)
        })
        .await
        .map_err(|e| format!("http_cloud steer task join error: {e}"))?
    }

    async fn poll_turn(
        &self,
        remote_run_id: &str,
        cursor: Option<&str>,
    ) -> Result<CloudTurnPollResult, String> {
        let driver = self.clone();
        let run_id = validate_path_segment(remote_run_id, "remote_run_id")?;
        let cursor_owned = cursor.map(str::to_string);
        tokio::task::spawn_blocking(move || {
            let headers = driver.auth_headers();
            let mut url = format!("{}/turns/{run_id}", driver.base_url);
            if let Some(ref c) = cursor_owned
                && !c.trim().is_empty()
            {
                url.push_str("?cursor=");
                url.push_str(c.trim());
            }
            let (status, resp) = http_json("GET", &url, &headers, None, driver.timeout_secs)?;
            if !(200..300).contains(&status) {
                return Err(format!(
                    "Cloud HTTP GET /turns/{run_id} returned HTTP {status}"
                ));
            }
            let state = map_status_str(resp.get("status").and_then(Value::as_str));
            let deltas: Vec<String> = resp
                .get("deltas")
                .and_then(Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            let mut next_seq = cursor_owned
                .as_deref()
                .and_then(|c| c.trim().parse::<i64>().ok())
                .filter(|&n| n > 0)
                .unwrap_or(0);
            let next_cursor = resp
                .get("next_cursor")
                .or_else(|| resp.get("cursor"))
                .and_then(Value::as_str)
                .map(str::to_string)
                .or(cursor_owned);
            let mut transcript_events = Vec::new();
            if let Some(arr) = resp.get("events").and_then(Value::as_array) {
                let mut call_names = HashMap::new();
                for item in arr {
                    if let Some(te) = parse_cloud_transcript_event(
                        None,
                        item.get("id").and_then(Value::as_str),
                        &mut next_seq,
                        &mut call_names,
                        item,
                    ) {
                        transcript_events.push(te);
                    }
                }
            }
            Ok(CloudTurnPollResult {
                remote_run_id: run_id,
                status: state,
                result_text: extract_result_string(&resp),
                error_text: extract_error_string(&resp),
                usage: resp.get("usage").cloned(),
                cursor: next_cursor,
                deltas,
                transcript_events,
                next_poll_after_ms: (!state.is_terminal()).then_some(DEFAULT_NEXT_POLL_MS),
            })
        })
        .await
        .map_err(|e| format!("http_cloud poll task join error: {e}"))?
    }

    async fn cancel_turn(&self, remote_run_id: &str) -> Result<(), String> {
        let driver = self.clone();
        let run_id = validate_path_segment(remote_run_id, "remote_run_id")?;
        tokio::task::spawn_blocking(move || {
            let headers = driver.auth_headers();
            let url = format!("{}/turns/{run_id}:cancel", driver.base_url);
            let (status, _) = http_json("POST", &url, &headers, None, driver.timeout_secs)?;
            if status == 409 || (200..300).contains(&status) {
                return Ok(());
            }
            Err(format!(
                "Cloud HTTP POST /turns/{run_id}:cancel returned HTTP {status}"
            ))
        })
        .await
        .map_err(|e| format!("http_cloud cancel task join error: {e}"))?
    }

    fn parse_webhook(
        &self,
        headers: &[(String, String)],
        body: &[u8],
    ) -> Result<CloudWebhookEvent, String> {
        verify_optional_webhook_secret(
            self.webhook_secret.as_deref(),
            headers,
            body,
            &["x-webhook-signature-256", "x-hub-signature-256"],
        )?;
        let payload: Value = serde_json::from_slice(body)
            .map_err(|e| format!("invalid generic cloud webhook JSON: {e}"))?;
        let data = payload
            .get("data")
            .filter(|v| v.is_object())
            .unwrap_or(&payload);

        let remote_run_id = data
            .get("remote_run_id")
            .or_else(|| data.get("id"))
            .or_else(|| data.get("run_id"))
            .or_else(|| payload.get("remote_run_id"))
            .or_else(|| payload.get("id"))
            .or_else(|| payload.get("run_id"))
            .and_then(Value::as_str)
            .ok_or_else(|| "generic cloud webhook missing `remote_run_id` / `id`".to_string())?;
        let remote_run_id = validate_path_segment(remote_run_id, "remote_run_id")?;

        let turn_id = data
            .get("turn_id")
            .or_else(|| payload.get("turn_id"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);

        let event_type = payload
            .get("event_type")
            .or_else(|| payload.get("event"))
            .or_else(|| payload.get("type"))
            .and_then(Value::as_str)
            .unwrap_or("turn_update")
            .to_string();

        let explicit_status = data
            .get("status")
            .or_else(|| payload.get("status"))
            .and_then(Value::as_str);
        let status = match explicit_status {
            Some(s) => map_status_str(Some(s)),
            None => match event_type.to_ascii_lowercase().as_str() {
                "turn_complete" | "turn.completed" | "completed" => CloudTurnState::Completed,
                "turn_failed" | "turn.failed" | "failed" => CloudTurnState::Failed,
                "turn_cancelled" | "turn.cancelled" | "cancelled" | "canceled" => {
                    CloudTurnState::Cancelled
                }
                _ => CloudTurnState::Running,
            },
        };

        let (steer_message_id, steer_seq, steer_proof) =
            extract_webhook_steer_fields(data, &payload, &event_type);

        let cursor = data
            .get("cursor")
            .or_else(|| payload.get("cursor"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let transcript_events =
            parse_webhook_transcript_events(&event_type, cursor.as_deref(), &payload);

        Ok(CloudWebhookEvent {
            turn_id,
            remote_run_id,
            harness_kind: self.harness_kind().to_string(),
            event_type,
            status,
            result_text: extract_result_string(data).or_else(|| extract_result_string(&payload)),
            error_text: extract_error_string(data).or_else(|| extract_error_string(&payload)),
            usage: data.get("usage").or_else(|| payload.get("usage")).cloned(),
            cursor,
            steer_message_id,
            steer_seq,
            steer_proof,
            transcript_events,
        })
    }
}

/// Resolve a stateless [`CloudTurnDriver`] purely from persisted turn metadata + environment
/// (zero coordinator memory required across scale-to-zero restarts).
pub fn resolve_cloud_driver_from_metadata(
    harness_kind: &str,
    metadata: Option<&Value>,
    webhook_secret_override: Option<String>,
) -> Result<Box<dyn CloudTurnDriver>, String> {
    resolve_cloud_driver(harness_kind, metadata, webhook_secret_override, None)
}

/// Resolve a driver for a message write. The inference key is only used on
/// this replica and never included in turn metadata or request DTOs.
pub(crate) async fn resolve_cloud_message_driver(
    store: std::sync::Arc<anyagents_core::Store>,
    agent_id: anyagents_core::AgentId,
    harness_kind: &str,
    metadata: Option<&Value>,
) -> Result<Box<dyn CloudTurnDriver>, String> {
    let key = if matches!(
        harness_kind.trim().to_ascii_lowercase().as_str(),
        "spiron" | "spiron-cloud" | "spiron_cloud"
    ) {
        tokio::task::spawn_blocking(move || resolve_inference_key(Some((&store, &agent_id))))
            .await
            .map_err(|_| "Spiron inference key resolution failed".to_string())??
    } else {
        None
    };
    resolve_cloud_driver(harness_kind, metadata, None, key)
}

pub(crate) fn resolve_inference_key(
    target: Option<(&anyagents_core::Store, &anyagents_core::AgentId)>,
) -> Result<Option<InferenceKey>, String> {
    if let Some((store, agent_id)) = target {
        let mut env = store
            .resolve_agent_secret_env(agent_id)
            .map_err(|_| "Could not resolve granted secrets for Spiron inference".to_string())?;
        if let Some(value) = env.remove(BYOK_ENV_SHORT) {
            return InferenceKey::new(value)
                .map(Some)
                .map_err(|err| err.to_string());
        }
    }
    InferenceKey::from_process_env().map_err(|err| err.to_string())
}

fn resolve_cloud_driver(
    harness_kind: &str,
    metadata: Option<&Value>,
    webhook_secret_override: Option<String>,
    inference_key: Option<InferenceKey>,
) -> Result<Box<dyn CloudTurnDriver>, String> {
    let endpoint_from_meta = metadata
        .and_then(|m| {
            m.get("cloud_endpoint")
                .or_else(|| m.get("endpoint"))
                .and_then(Value::as_str)
        })
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let remote_agent_id = metadata
        .and_then(|m| m.get("remote_agent_id").and_then(Value::as_str))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    match harness_kind.trim().to_ascii_lowercase().as_str() {
        "spiron" | "spiron-cloud" | "spiron_cloud" => {
            let operator_url = operator_cloud_url();
            let base_url = endpoint_from_meta.unwrap_or_else(|| operator_url.clone());
            if inference_key.is_some() && base_url.trim_end_matches('/') != operator_url {
                return Err(
                    "Spiron inference key requires the coordinator's configured origin".into(),
                );
            }
            let token = env_non_empty("ANYAGENTS_SPIRON_API_KEY")
                .or_else(|| env_non_empty("SPIRON_API_KEY"))
                .or_else(|| env_non_empty("SPIRON_TOKEN"));
            let secret = webhook_secret_override
                .or_else(|| env_non_empty("ANYAGENTS_SPIRON_WEBHOOK_SECRET"))
                .or_else(|| env_non_empty("ANYAGENTS_CLOUD_WEBHOOK_SECRET"));
            Ok(Box::new(
                SpironServerlessDriver::new(base_url, token)
                    .with_inference_key(inference_key)
                    .with_remote_agent_id(remote_agent_id)
                    .with_webhook_secret(secret),
            ))
        }
        "http_cloud" | "http-cloud" | "openai-cloud" | "claude-cloud" | "cursor-cloud" => {
            let base_url = endpoint_from_meta
                .or_else(|| env_non_empty("ANYAGENTS_CLOUD_HTTP_URL"))
                .ok_or_else(|| {
                    "GenericHttpCloudDriver requires `cloud_endpoint` in turn metadata or `ANYAGENTS_CLOUD_HTTP_URL`"
                        .to_string()
                })?;
            let token = env_non_empty("ANYAGENTS_CLOUD_HTTP_TOKEN");
            let secret =
                webhook_secret_override.or_else(|| env_non_empty("ANYAGENTS_CLOUD_WEBHOOK_SECRET"));
            Ok(Box::new(
                GenericHttpCloudDriver::new(base_url, token).with_webhook_secret(secret),
            ))
        }
        other => Err(format!("unsupported serverless cloud harness `{other}`")),
    }
}

fn env_non_empty(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_core::hmac_sha256_hex;
    use axum::extract::{Path as AxumPath, State};
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::{get, post, put};
    use axum::{Json, Router};
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn inference_key_setup_gates_serverless_messages() {
        const ID: &str = "00000000-0000-4000-8000-000000000001";
        const AUTH: &str = "spiron-test-bearer";
        let fixture = ["sylphx", "sk", "serverless-test-fixture"].join("_");
        for (status, configured, expected) in [
            (None, false, vec!["create", "message"]),
            (
                Some(StatusCode::OK),
                true,
                vec!["create", "setup", "message"],
            ),
            (
                Some(StatusCode::SERVICE_UNAVAILABLE),
                true,
                vec!["create", "setup"],
            ),
            (Some(StatusCode::FORBIDDEN), false, vec!["create", "setup"]),
            (Some(StatusCode::OK), false, vec!["create", "setup"]),
        ] {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let create_calls = Arc::clone(&calls);
            let setup_calls = Arc::clone(&calls);
            let message_calls = Arc::clone(&calls);
            let expected_key = fixture.clone();
            let create_key = fixture.clone();
            let message_key = fixture.clone();
            let app = Router::new()
                .route("/v1/agents", post(move |Json(body): Json<Value>| async move {
                    assert!(!body.to_string().contains(&create_key));
                    create_calls.lock().unwrap().push("create");
                    (StatusCode::CREATED, Json(json!({"id": ID})))
                }))
                .route("/v1/agents/{id}/inference-key", put(move |
                    AxumPath(id): AxumPath<String>, headers: HeaderMap, Json(body): Json<Value>
                | async move {
                    assert_eq!(id, ID);
                    assert_eq!(headers["authorization"].to_str().unwrap(), format!("Bearer {AUTH}"));
                    assert_eq!(body, json!({"sylphx_api_key": expected_key}));
                    setup_calls.lock().unwrap().push("setup");
                    (status.unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                     Json(json!({"byok_configured": configured, "untrusted": expected_key})))
                }))
                .route("/v1/agents/{id}/messages", post(move |
                    AxumPath(id): AxumPath<String>, headers: HeaderMap, Json(body): Json<Value>
                | async move {
                    assert_eq!(id, ID);
                    assert_eq!(headers["authorization"].to_str().unwrap(), format!("Bearer {AUTH}"));
                    assert!(!body.to_string().contains(&message_key));
                    message_calls.lock().unwrap().push("message");
                    (StatusCode::ACCEPTED, Json(json!({"id": "receipt_1"})))
                }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let driver = SpironServerlessDriver::new(format!("http://{addr}"), Some(AUTH.into()))
                .with_inference_key(status.map(|_| InferenceKey::new(fixture.clone()).unwrap()));
            assert!(!format!("{driver:?}").contains(&fixture));
            let request = CloudTurnSubmitRequest {
                turn_id: "turn_1".into(),
                agent_id: "agt_local_1".into(),
                remote_agent_id: None,
                prompt: "hello".into(),
                model: None,
                instructions: None,
                mcp_servers: None,
                callback_url: None,
                callback_token: None,
                metadata: BTreeMap::new(),
            };
            let outcome = driver.submit_turn(&request).await;
            if expected.contains(&"message") {
                assert_eq!(outcome.unwrap().remote_agent_id.as_deref(), Some(ID));
            } else {
                let err = outcome.unwrap_err();
                assert!(err.contains("message not sent"), "{err}");
                assert!(!err.contains(&fixture));
            }
            assert_eq!(*calls.lock().unwrap(), expected);
            server.abort();
        }
    }

    #[tokio::test]
    async fn spiron_serverless_driver_submits_with_mcp_polls_receipt_and_events_cancels_and_parses_webhook()
     {
        #[derive(Default)]
        struct MockSpiron {
            submitted_bodies: Mutex<Vec<Value>>,
            poll_count: Mutex<usize>,
            cancelled: Mutex<Vec<String>>,
        }

        let state = Arc::new(MockSpiron::default());
        let app = Router::new()
            .route(
                "/v1/agents/{agent_id}/messages",
                post(
                    |State(st): State<Arc<MockSpiron>>,
                     AxumPath(agent_id): AxumPath<String>,
                     headers: HeaderMap,
                     Json(body): Json<Value>| async move {
                        assert_eq!(headers.get("prefer").unwrap(), "respond-async");
                        assert!(headers.get("idempotency-key").is_some());
                        st.submitted_bodies.lock().unwrap().push(body);
                        Json(json!({
                            "id": "rcpt_spiron_001",
                            "agent_id": agent_id,
                            "status": "submitted"
                        }))
                    },
                ),
            )
            .route(
                "/v1/message-receipts/{task_id}",
                get(
                    |State(st): State<Arc<MockSpiron>>, AxumPath(task_id): AxumPath<String>| async move {
                        let mut count = st.poll_count.lock().unwrap();
                        *count += 1;
                        if *count == 1 {
                            Json(json!({
                                "id": task_id,
                                "status": "working"
                            }))
                        } else {
                            Json(json!({
                                "id": task_id,
                                "status": "completed",
                                "reply": "Spiron serverless complete",
                                "usage": { "input_tokens": 50, "output_tokens": 20 }
                            }))
                        }
                    },
                ),
            )
            .route(
                "/v1/message-receipts/{task_id}/events",
                get(|AxumPath(_task_id): AxumPath<String>| async move {
                    Json(json!({
                        "next_cursor": "ev_2",
                        "events": [
                            { "id": "ev_1", "delta": "working..." },
                            { "id": "ev_2", "delta": "done" }
                        ]
                    }))
                }),
            )
            .route(
                "/v1/message-receipts/{task_id}",
                post(
                    |State(st): State<Arc<MockSpiron>>, AxumPath(task_id): AxumPath<String>| async move {
                        st.cancelled.lock().unwrap().push(task_id.clone());
                        Json(json!({ "id": task_id, "status": "canceled" }))
                    },
                ),
            )
            .with_state(Arc::clone(&state));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let driver =
            SpironServerlessDriver::new(format!("http://{addr}"), Some("spiron_sk_test".into()))
                .with_remote_agent_id(Some("agt_remote_1".into()))
                .with_webhook_secret(Some("whsec_spiron_test".into()));

        let submitted = driver
            .submit_turn(&CloudTurnSubmitRequest {
                turn_id: "turn_spiron_1".into(),
                agent_id: "agt_local_1".into(),
                remote_agent_id: None,
                prompt: "Hello Spiron".into(),
                model: None,
                instructions: None,
                mcp_servers: Some(json!([{
                    "name": "anyagents",
                    "url": "https://coord.example/v1/webhooks/mcp",
                    "bearer_token": "rst_test_tok"
                }])),
                callback_url: None,
                callback_token: None,
                metadata: BTreeMap::new(),
            })
            .await
            .unwrap();
        assert_eq!(submitted.remote_run_id, "rcpt_spiron_001");
        assert_eq!(submitted.status, CloudTurnState::Dispatched);

        {
            let bodies = state.submitted_bodies.lock().unwrap();
            assert_eq!(bodies[0]["content"], "Hello Spiron");
            assert_eq!(bodies[0]["mcp_servers"][0]["name"], "anyagents");
        }

        let poll1 = driver.poll_turn("rcpt_spiron_001", None).await.unwrap();
        assert_eq!(poll1.status, CloudTurnState::Running);
        assert_eq!(poll1.cursor.as_deref(), Some("ev_2"));
        assert_eq!(poll1.deltas, vec!["working...", "done"]);

        let poll2 = driver
            .poll_turn("rcpt_spiron_001", poll1.cursor.as_deref())
            .await
            .unwrap();
        assert_eq!(poll2.status, CloudTurnState::Completed);
        assert_eq!(
            poll2.result_text.as_deref(),
            Some("Spiron serverless complete")
        );
        assert_eq!(poll2.usage.as_ref().unwrap()["input_tokens"], 50);

        // Webhook HMAC-SHA256 verification: valid passes, wrong signature fails closed.
        let wh_body = serde_json::to_vec(&json!({
            "event": "message.completed",
            "data": {
                "task_id": "rcpt_spiron_001",
                "provider_message_id": "turn_spiron_1",
                "status": "completed",
                "reply": "Webhook reply",
                "usage": { "input_tokens": 10, "output_tokens": 4 }
            }
        }))
        .unwrap();
        let sig = format!("sha256={}", hmac_sha256_hex(b"whsec_spiron_test", &wh_body));
        let parsed = driver
            .parse_webhook(&[("X-Spiron-Signature-256".into(), sig)], &wh_body)
            .unwrap();
        assert_eq!(parsed.remote_run_id, "rcpt_spiron_001");
        assert_eq!(parsed.turn_id.as_deref(), Some("turn_spiron_1"));
        assert_eq!(parsed.status, CloudTurnState::Completed);
        assert_eq!(parsed.result_text.as_deref(), Some("Webhook reply"));

        assert!(
            driver
                .parse_webhook(
                    &[("X-Spiron-Signature-256".into(), "sha256=deadbeef".into())],
                    &wh_body,
                )
                .is_err()
        );
        assert!(driver.parse_webhook(&[], &wh_body).is_err());

        // Cancel route check (`:cancel` matches `{task_id}` in axum 0.8 as `rcpt_spiron_001:cancel`).
        driver.cancel_turn("rcpt_spiron_001").await.unwrap();
        assert_eq!(
            state.cancelled.lock().unwrap().as_slice(),
            &["rcpt_spiron_001:cancel".to_string()]
        );
    }

    #[tokio::test]
    async fn generic_http_cloud_driver_supports_submit_poll_cancel_and_webhook() {
        let app = Router::new()
            .route(
                "/turns",
                post(|Json(body): Json<Value>| async move {
                    (
                        StatusCode::ACCEPTED,
                        Json(json!({
                            "id": "gen_run_99",
                            "status": "running",
                            "echo_turn_id": body["turn_id"],
                        })),
                    )
                }),
            )
            .route(
                "/turns/{id}",
                get(|AxumPath(id): AxumPath<String>| async move {
                    Json(json!({
                        "id": id,
                        "status": "completed",
                        "result_text": "Generic cloud answer",
                        "usage": { "input_tokens": 33, "output_tokens": 11 },
                        "cursor": "c-7"
                    }))
                })
                .post(|AxumPath(id): AxumPath<String>| async move {
                    Json(json!({ "id": id, "status": "cancelled" }))
                }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let driver = GenericHttpCloudDriver::new(format!("http://{addr}"), Some("tok_gen".into()))
            .with_webhook_secret(Some("whsec_generic".into()));

        let sub = driver
            .submit_turn(&CloudTurnSubmitRequest {
                turn_id: "turn_gen_1".into(),
                agent_id: "agt_1".into(),
                remote_agent_id: None,
                prompt: "Build it".into(),
                model: Some("codex-5".into()),
                instructions: None,
                mcp_servers: None,
                callback_url: Some("https://coord.example/v1/internal/cloud/events".into()),
                callback_token: Some("rtk_abc".into()),
                metadata: BTreeMap::new(),
            })
            .await
            .unwrap();
        assert_eq!(sub.remote_run_id, "gen_run_99");
        assert_eq!(sub.status, CloudTurnState::Running);

        let poll = driver.poll_turn("gen_run_99", None).await.unwrap();
        assert_eq!(poll.status, CloudTurnState::Completed);
        assert_eq!(poll.result_text.as_deref(), Some("Generic cloud answer"));
        assert_eq!(poll.cursor.as_deref(), Some("c-7"));

        driver.cancel_turn("gen_run_99").await.unwrap();

        let wh_body = serde_json::to_vec(&json!({
            "event_type": "turn_complete",
            "turn_id": "turn_gen_1",
            "remote_run_id": "gen_run_99",
            "status": "completed",
            "result_text": "Done via webhook"
        }))
        .unwrap();
        let sig = format!("sha256={}", hmac_sha256_hex(b"whsec_generic", &wh_body));
        let ev = driver
            .parse_webhook(&[("X-Webhook-Signature-256".into(), sig)], &wh_body)
            .unwrap();
        assert_eq!(ev.turn_id.as_deref(), Some("turn_gen_1"));
        assert_eq!(ev.remote_run_id, "gen_run_99");
        assert_eq!(ev.status, CloudTurnState::Completed);
        assert_eq!(ev.result_text.as_deref(), Some("Done via webhook"));
    }

    #[tokio::test]
    async fn cloud_drivers_steer_turn_and_parse_steer_consumed_webhooks_with_fail_closed_refusal() {
        #[derive(Default)]
        struct MockSteerServer {
            mode: Mutex<String>,
            headers_seen: Mutex<Vec<(String, String)>>,
            bodies_seen: Mutex<Vec<Value>>,
        }

        let st = Arc::new(MockSteerServer {
            mode: Mutex::new("consumed".into()),
            ..MockSteerServer::default()
        });
        let app = Router::new()
            .route(
                "/v1/agents/{agent_id}/messages",
                post(
                    |State(st): State<Arc<MockSteerServer>>,
                     headers: HeaderMap,
                     Json(body): Json<Value>| async move {
                        let delivery = headers
                            .get("x-anyagents-delivery")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("")
                            .to_string();
                        let run_id = headers
                            .get("x-anyagents-run-id")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("")
                            .to_string();
                        st.headers_seen.lock().unwrap().push((delivery, run_id));
                        st.bodies_seen.lock().unwrap().push(body);
                        let mode = st.mode.lock().unwrap().clone();
                        match mode.as_str() {
                            "consumed" => (
                                StatusCode::OK,
                                Json(json!({
                                    "id": "rcpt_steer_1",
                                    "status": "steered",
                                    "input_delivery": { "delivered": true, "step": 2 }
                                })),
                            ),
                            "accepted" => (
                                StatusCode::ACCEPTED,
                                Json(json!({
                                    "id": "rcpt_steer_2",
                                    "status": "accepted"
                                })),
                            ),
                            _ => (
                                StatusCode::CONFLICT,
                                Json(json!({
                                    "error": "turn already settling"
                                })),
                            ),
                        }
                    },
                ),
            )
            .route(
                "/turns/{run_id}/steer",
                post(
                    |State(st): State<Arc<MockSteerServer>>,
                     AxumPath(_run_id): AxumPath<String>,
                     Json(body): Json<Value>| async move {
                        st.bodies_seen.lock().unwrap().push(body);
                        let mode = st.mode.lock().unwrap().clone();
                        match mode.as_str() {
                            "consumed" => (
                                StatusCode::OK,
                                Json(json!({
                                    "id": "gen_steer_1",
                                    "status": "consumed",
                                    "consumed": true,
                                    "proof": "http_cloud:step2_consumed"
                                })),
                            ),
                            _ => (
                                StatusCode::NOT_IMPLEMENTED,
                                Json(json!({ "error": "steer not supported" })),
                            ),
                        }
                    },
                ),
            )
            .with_state(Arc::clone(&st));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let spiron = SpironServerlessDriver::new(format!("http://{addr}"), Some("sk_test".into()))
            .with_remote_agent_id(Some("agt_remote_spiron".into()))
            .with_webhook_secret(Some("whsec_steer".into()));
        assert!(spiron.supports_steer());

        let base_req = CloudTurnSteerRequest {
            turn_id: "turn_c_1".into(),
            agent_id: "agt_local_1".into(),
            remote_agent_id: Some("agt_remote_spiron".into()),
            remote_run_id: "rcpt_open_1".into(),
            message_id: "msg_steer_c1".into(),
            seq: 2,
            prompt: "Steer into step 2".into(),
            mcp_servers: None,
        };

        // 1. Spiron synchronous Consumed ack via `input_delivery`.
        let ack1 = spiron.steer_turn(&base_req).await.unwrap();
        assert!(
            matches!(
                ack1,
                CloudSteerAck::Consumed {
                    receipt_id: Some(ref r),
                    ref proof
                } if r == "rcpt_steer_1" && proof == "spiron:input_delivery"
            ),
            "{ack1:?}"
        );
        {
            let hdrs = st.headers_seen.lock().unwrap();
            assert_eq!(hdrs[0], ("steer".to_string(), "rcpt_open_1".to_string()));
        }

        // 2. Spiron async Accepted ack (does NOT fabricate consumption proof).
        *st.mode.lock().unwrap() = "accepted".into();
        let ack2 = spiron.steer_turn(&base_req).await.unwrap();
        assert_eq!(
            ack2,
            CloudSteerAck::Accepted {
                receipt_id: Some("rcpt_steer_2".into())
            }
        );

        // 3. Spiron HTTP 409 Refused fails closed cleanly.
        *st.mode.lock().unwrap() = "refused".into();
        let ack3 = spiron.steer_turn(&base_req).await.unwrap();
        assert!(
            matches!(ack3, CloudSteerAck::Refused { ref reason } if reason.contains("settling")),
            "{ack3:?}"
        );

        // 4. Spiron webhook `message.steered` parses steer_message_id / steer_seq / steer_proof.
        let wh_body = serde_json::to_vec(&json!({
            "event": "message.steered",
            "data": {
                "task_id": "rcpt_open_1",
                "turn_id": "turn_c_1",
                "status": "running",
                "message_id": "msg_steer_c1",
                "seq": 2,
                "input_delivery": { "delivered": true }
            }
        }))
        .unwrap();
        let sig = format!("sha256={}", hmac_sha256_hex(b"whsec_steer", &wh_body));
        let parsed = spiron
            .parse_webhook(&[("X-Spiron-Signature-256".into(), sig)], &wh_body)
            .unwrap();
        assert_eq!(parsed.status, CloudTurnState::Running);
        assert_eq!(parsed.steer_message_id.as_deref(), Some("msg_steer_c1"));
        assert_eq!(parsed.steer_seq, Some(2));
        assert_eq!(parsed.steer_proof.as_deref(), Some("cloud:input_delivery"));

        // 5. GenericHttpCloudDriver supports steer_turn when endpoint accepts & fails closed on 501.
        let generic = GenericHttpCloudDriver::new(format!("http://{addr}"), Some("tok_g".into()));
        *st.mode.lock().unwrap() = "consumed".into();
        let g1 = generic.steer_turn(&base_req).await.unwrap();
        assert!(
            matches!(
                g1,
                CloudSteerAck::Consumed { ref proof, .. } if proof == "http_cloud:step2_consumed"
            ),
            "{g1:?}"
        );
        *st.mode.lock().unwrap() = "refused".into();
        let g2 = generic.steer_turn(&base_req).await.unwrap();
        assert!(matches!(g2, CloudSteerAck::Refused { .. }), "{g2:?}");
    }
}
