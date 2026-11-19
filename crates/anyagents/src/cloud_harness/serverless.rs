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

use std::collections::BTreeMap;
use std::io::Read as _;
use std::time::Duration;

use anyagents_core::{constant_time_eq, verify_hmac_sha256_signature};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_poll_after_ms: Option<u64>,
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
}

/// Universal async driver contract for pure serverless cloud agent harnesses.
#[async_trait]
pub trait CloudTurnDriver: Send + Sync {
    /// Canonical `pending_remote_turns.harness_kind` value (`"spiron"`, `"http_cloud"`, ...).
    fn harness_kind(&self) -> &'static str;

    /// Submit a turn asynchronously (`Prefer: respond-async`, zero long-lived SSE).
    async fn submit_turn(
        &self,
        req: &CloudTurnSubmitRequest,
    ) -> Result<CloudTurnSubmission, String>;

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

fn build_http_agent(timeout_secs: u64) -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(timeout_secs.max(1))))
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
    let agent = build_http_agent(timeout_secs);
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
            webhook_secret: None,
            timeout_secs: DEFAULT_HTTP_TIMEOUT_SECS,
        }
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

#[async_trait]
impl CloudTurnDriver for SpironServerlessDriver {
    fn harness_kind(&self) -> &'static str {
        "spiron"
    }

    async fn submit_turn(
        &self,
        req: &CloudTurnSubmitRequest,
    ) -> Result<CloudTurnSubmission, String> {
        let driver = self.clone();
        let req = req.clone();
        tokio::task::spawn_blocking(move || {
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

            // Pull incremental events from `GET /v1/message-receipts/{task_id}/events` on tick fallback.
            let mut events_url =
                format!("{}/v1/message-receipts/{task_id}/events", driver.base_url);
            if let Some(ref after) = cursor_owned
                && !after.trim().is_empty()
            {
                events_url.push_str("?after=");
                events_url.push_str(after.trim());
            }
            let mut next_cursor = cursor_owned.clone();
            let mut deltas = Vec::new();
            let mut events_reply: Option<String> = None;
            let mut events_usage: Option<Value> = None;

            if let Ok((ev_status, ev_doc)) =
                http_json("GET", &events_url, &headers, None, driver.timeout_secs)
                && (200..300).contains(&ev_status)
            {
                if let Some(c) = ev_doc
                    .get("next_cursor")
                    .or_else(|| ev_doc.get("cursor"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    next_cursor = Some(c.to_string());
                }
                let arr_opt = ev_doc
                    .get("events")
                    .or_else(|| ev_doc.get("items"))
                    .and_then(Value::as_array)
                    .or_else(|| ev_doc.as_array());
                if let Some(items) = arr_opt {
                    for item in items {
                        if let Some(cid) = item
                            .get("id")
                            .or_else(|| item.get("cursor"))
                            .and_then(Value::as_str)
                            .filter(|s| !s.is_empty())
                        {
                            next_cursor = Some(cid.to_string());
                        }
                        if let Some(d) = item
                            .get("delta")
                            .or_else(|| item.get("text"))
                            .or_else(|| item.pointer("/data/delta"))
                            .or_else(|| item.pointer("/data/text"))
                            .and_then(Value::as_str)
                            .filter(|s| !s.is_empty())
                        {
                            deltas.push(d.to_string());
                        }
                        if let Some(r) = item
                            .get("reply")
                            .or_else(|| item.get("result"))
                            .or_else(|| item.pointer("/data/reply"))
                            .and_then(Value::as_str)
                            .filter(|s| !s.trim().is_empty())
                        {
                            events_reply = Some(r.to_string());
                        }
                        if let Some(u) = item.get("usage").or_else(|| item.pointer("/data/usage")) {
                            events_usage = Some(u.clone());
                        }
                    }
                }
            }

            let state = map_status_str(receipt.get("status").and_then(Value::as_str));
            let mut result_text = extract_result_string(&receipt)
                .or(events_reply)
                .or_else(|| (!deltas.is_empty()).then(|| deltas.join("")));
            let error_text = extract_error_string(&receipt);
            let usage = receipt.get("usage").cloned().or(events_usage);

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
                    && let Some(items) = conv.get("items").and_then(Value::as_array)
                {
                    let found = items.iter().rev().find_map(|item| {
                        let matches_task = item.get("task_id").and_then(Value::as_str)
                            == Some(task_id.as_str())
                            || item.get("receipt_id").and_then(Value::as_str)
                                == Some(task_id.as_str());
                        let is_assistant =
                            item.get("role").and_then(Value::as_str) == Some("assistant");
                        if is_assistant && matches_task {
                            extract_result_string(item)
                        } else {
                            None
                        }
                    });
                    if found.is_some() {
                        result_text = found;
                    }
                }
            }

            Ok(CloudTurnPollResult {
                remote_run_id: task_id,
                status: state,
                result_text,
                error_text,
                usage,
                cursor: next_cursor,
                deltas,
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
            let next_cursor = resp
                .get("next_cursor")
                .or_else(|| resp.get("cursor"))
                .and_then(Value::as_str)
                .map(str::to_string)
                .or(cursor_owned);
            Ok(CloudTurnPollResult {
                remote_run_id: run_id,
                status: state,
                result_text: extract_result_string(&resp),
                error_text: extract_error_string(&resp),
                usage: resp.get("usage").cloned(),
                cursor: next_cursor,
                deltas,
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

        Ok(CloudWebhookEvent {
            turn_id,
            remote_run_id,
            harness_kind: self.harness_kind().to_string(),
            event_type,
            status,
            result_text: extract_result_string(data).or_else(|| extract_result_string(&payload)),
            error_text: extract_error_string(data).or_else(|| extract_error_string(&payload)),
            usage: data.get("usage").or_else(|| payload.get("usage")).cloned(),
            cursor: data
                .get("cursor")
                .or_else(|| payload.get("cursor"))
                .and_then(Value::as_str)
                .map(str::to_string),
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
            let base_url = endpoint_from_meta
                .or_else(|| env_non_empty("ANYAGENTS_SPIRON_CLOUD_URL"))
                .or_else(|| env_non_empty("SPIRON_API_URL"))
                .or_else(|| env_non_empty("SPIRON_CLOUD_URL"))
                .unwrap_or_else(|| "https://api.spiron.ai".to_string());
            let token = env_non_empty("ANYAGENTS_SPIRON_API_KEY")
                .or_else(|| env_non_empty("SPIRON_API_KEY"))
                .or_else(|| env_non_empty("SPIRON_TOKEN"));
            let secret = webhook_secret_override
                .or_else(|| env_non_empty("ANYAGENTS_SPIRON_WEBHOOK_SECRET"))
                .or_else(|| env_non_empty("ANYAGENTS_CLOUD_WEBHOOK_SECRET"));
            Ok(Box::new(
                SpironServerlessDriver::new(base_url, token)
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
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use std::sync::{Arc, Mutex};

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
}
