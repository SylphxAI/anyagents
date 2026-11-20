//! Internal HTTP client, durable state persistence, and response codecs for
//! [`crate::cloud::SpironCloudProvider`].

use std::borrow::Cow;
use std::collections::HashSet;
use std::fs;
use std::io::Read as _;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyagents_harness::quota::{parse_rfc3339_utc_ms, reset_at_ms};
use anyagents_harness::{
    Delivery, DeliveryLevel, RemoteBudget, RemoteCapabilities, RemoteConsumptionProof,
    RemoteDeliveryAck, RemoteSessionConfig, RemoteSessionError, TurnUsage,
    atomic_write_nonempty_file, names_message_size,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

use crate::cloud::{
    DEFAULT_SPIRON_CLOUD_URL, SpironCloudConfig, is_sylphx_gateway_key, validate_uuid,
};

pub(crate) const DEFAULT_RETRY_AFTER_MS: i64 = 60_000;
pub(crate) const ACTIVE_RUN_FILE: &str = "spiron-cloud-active-run.json";
const MAX_CONVERSATION_PAGES: usize = 100;
const MAX_ERROR_BODY_BYTES: u64 = 16 * 1024;

pub(crate) fn proto(msg: impl Into<String>) -> RemoteSessionError {
    RemoteSessionError::Protocol(msg.into())
}

pub(crate) fn transport(msg: impl Into<String>) -> RemoteSessionError {
    RemoteSessionError::Transport(msg.into())
}

pub(crate) fn validate_request_id(id: &str) -> Result<String, RemoteSessionError> {
    let ok = !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'));
    ok.then(|| id.to_string())
        .ok_or_else(|| proto("invalid Spiron request_id"))
}

pub(crate) fn read_cloud_json(dir: &Path) -> Option<Value> {
    serde_json::from_str(&fs::read_to_string(dir.join("cloud.json")).ok()?).ok()
}

pub(crate) fn env_trimmed(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

pub(crate) fn ptr_str<'a>(val: &'a Value, ptrs: &[&str]) -> Option<&'a str> {
    ptrs.iter().find_map(|p| {
        val.pointer(p)?
            .as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty())
    })
}

fn ptr_u64(val: &Value, ptrs: &[&str]) -> Option<u64> {
    ptrs.iter().find_map(|p| val.pointer(p)?.as_u64())
}

pub(crate) fn ptr_micros(val: &Value, micros: &[&str], usd: &[&str]) -> Option<u64> {
    ptr_u64(val, micros).or_else(|| {
        usd.iter().find_map(|p| {
            let v = val.pointer(p)?.as_f64()?;
            (v.is_finite() && v >= 0.0).then(|| (v * 1_000_000.0).round() as u64)
        })
    })
}

pub(crate) fn resolve_base_url(dir: &Path) -> String {
    let json_url = read_cloud_json(dir)
        .and_then(|v| ptr_str(&v, &["/base_url", "/endpoint"]).map(str::to_string));
    [
        "ANYAGENTS_SPIRON_CLOUD_URL",
        "SPIRON_API_URL",
        "SPIRON_CLOUD_URL",
    ]
    .into_iter()
    .find_map(env_trimmed)
    .or(json_url)
    .unwrap_or_else(|| DEFAULT_SPIRON_CLOUD_URL.to_string())
    .trim()
    .trim_end_matches('/')
    .to_string()
}

pub(crate) fn resolve_disabled_tools(dir: &Path) -> Vec<String> {
    let cfg = read_cloud_json(dir);
    if let Some(arr) = cfg
        .as_ref()
        .and_then(|v| v.get("disabled_tools")?.as_array())
    {
        let list: Vec<String> = arr
            .iter()
            .filter_map(|v| {
                v.as_str()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
            })
            .collect();
        if !list.is_empty() {
            return list;
        }
    }
    let disable = env_trimmed("ANYAGENTS_CLOUD_DISABLE_BUILTIN_TOOLS")
        .is_some_and(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true"))
        || cfg.and_then(|v| v.get("disable_builtin_tools")?.as_bool()) == Some(true);
    if disable {
        vec!["memory".into(), "skills".into()]
    } else {
        Vec::new()
    }
}

fn read_json_token(path: &Path) -> Option<String> {
    let v: Value = serde_json::from_str(&fs::read_to_string(path).ok()?).ok()?;
    let ptrs = [
        "/api_key",
        "/apiKey",
        "/token",
        "/bearer_token",
        "/access_token",
        "/accessToken",
        "/jwt",
        "/spiron_api_key",
        "/SPIRON_API_KEY",
        "/SYLPHX_API_KEY",
        "/tokens/access_token",
    ];
    ptr_str(&v, &ptrs).map(str::to_string)
}

fn gateway_key_err() -> RemoteSessionError {
    RemoteSessionError::Auth(
        "only a Sylphx gateway key (sylphx_sk_...) is configured; spiron-cloud requires a Spiron Cloud API key (spiron_sk_...) or JWT"
            .into(),
    )
}

pub(crate) fn resolve_api_token(
    cfg: &SpironCloudConfig,
) -> Result<Option<String>, RemoteSessionError> {
    if let Some(explicit) = cfg.api_token.as_deref().map(str::trim) {
        if explicit.is_empty() {
            return Ok(None);
        }
        return if is_sylphx_gateway_key(explicit) {
            Err(gateway_key_err())
        } else {
            Ok(Some(explicit.into()))
        };
    }
    let files = [
        "cloud.json",
        ".spiron/auth.json",
        ".spiron/credentials.json",
        ".config/sylphx/auth.json",
        ".config/sylphx/credentials.json",
        "credentials.json",
        "auth.json",
    ];
    let envs = [
        "ANYAGENTS_SPIRON_API_KEY",
        "SPIRON_API_KEY",
        "SPIRON_TOKEN",
        "SYLPHX_API_KEY",
    ];
    let mut cands: Vec<String> = read_json_token(&cfg.config_dir.join(files[0]))
        .into_iter()
        .collect();
    cands.extend(envs.into_iter().filter_map(env_trimmed));
    if let Ok(raw) = fs::read_to_string(cfg.config_dir.join(".spiron/key"))
        && !raw.trim().is_empty()
    {
        cands.push(raw.trim().to_string());
    }
    cands.extend(
        files[1..]
            .iter()
            .filter_map(|rel| read_json_token(&cfg.config_dir.join(rel))),
    );
    let saw_gw = cands.iter().any(|t| is_sylphx_gateway_key(t));
    if let Some(tok) = cands.into_iter().find(|t| !is_sylphx_gateway_key(t)) {
        Ok(Some(tok))
    } else if saw_gw {
        Err(gateway_key_err())
    } else {
        Ok(None)
    }
}

pub(crate) fn validate_remote_mcp_url(
    cfg: &SpironCloudConfig,
    raw: &str,
) -> Result<String, RemoteSessionError> {
    let url = raw.trim();
    let reject = || proto("Cloud MCP callback requires HTTPS without credentials or fragment");
    if url.is_empty()
        || url
            .chars()
            .any(|c| c.is_whitespace() || c.is_control() || matches!(c, '#' | '@' | '\\'))
    {
        return Err(reject());
    }
    let (scheme, rest) = url.split_once("://").ok_or_else(reject)?;
    let auth = rest.split(['/', '?']).next().unwrap_or_default();
    let (host, port) = if let Some(b) = auth.strip_prefix('[') {
        let (h, after) = b.split_once(']').ok_or_else(reject)?;
        let port = if after.is_empty() {
            None
        } else {
            Some(after.strip_prefix(':').ok_or_else(reject)?)
        };
        (h, port)
    } else {
        auth.split_once(':')
            .map_or((auth, None), |(h, p)| (h, Some(p)))
    };
    if host.is_empty()
        || port.is_some_and(|p| p.is_empty() || !p.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err(reject());
    }
    let env_ok = [
        "ANYAGENTS_ALLOW_LOOPBACK_MCP",
        "ANYAGENTS_TEST_ALLOW_LOOPBACK_MCP",
    ]
    .into_iter()
    .any(|k| env_trimmed(k).is_some_and(|v| matches!(v.as_str(), "1" | "true")));
    let cfg_ok = cfg!(test)
        && read_cloud_json(&cfg.config_dir).and_then(|v| v.get("allow_loopback_mcp")?.as_bool())
            == Some(true);
    let loopback = (cfg.allow_loopback_mcp || env_ok || cfg_ok)
        && matches!(
            host.to_ascii_lowercase().as_str(),
            "127.0.0.1" | "localhost" | "::1"
        );
    match scheme.to_ascii_lowercase().as_str() {
        "https" => Ok(url.to_string()),
        "http" if loopback => Ok(url.to_string()),
        _ => Err(reject()),
    }
}

fn percent_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for b in input.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

pub(crate) fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}

fn save_private(path: &Path, bytes: &[u8]) -> Result<(), RemoteSessionError> {
    atomic_write_nonempty_file(path, bytes, 0o600)
        .map_err(|e| transport(format!("cannot persist Spiron state: {e}")))
}

pub(crate) fn persist_json<T: Serialize + ?Sized>(
    path: &Path,
    val: &T,
    what: &str,
) -> Result<(), RemoteSessionError> {
    let bytes = serde_json::to_vec(val).map_err(|e| proto(format!("cannot encode {what}: {e}")))?;
    save_private(path, &bytes)
}

fn new_idempotency_uuid(path: &Path) -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seed = format!(
        "{}:{}:{}:{:?}",
        path.display(),
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed),
        SystemTime::now().duration_since(UNIX_EPOCH),
    );
    let mut b = Sha256::digest(seed.as_bytes());
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let h: String = b[..16].iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &h[..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..]
    )
}

pub(crate) fn workspace_context(dir: &Path) -> Result<Option<Value>, RemoteSessionError> {
    match fs::read(dir.join(".anyagents-managed-worktree.json")) {
        Ok(bytes) => {
            let v: Value = serde_json::from_slice(&bytes)
                .map_err(|_| proto("invalid remote workspace marker"))?;
            Ok(
                (v.get("cloud_sandbox").and_then(Value::as_bool) == Some(true))
                    .then(|| json!({"repo": v["repo"], "branch": v["branch"], "base": v["base"]})),
            )
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(transport("cannot read remote workspace context")),
    }
}

fn github_repo(repo: &str) -> Option<String> {
    let repo = repo
        .strip_prefix("https://github.com/")
        .or_else(|| repo.strip_prefix("git@github.com:"))
        .unwrap_or(repo)
        .trim_end_matches(".git");
    let mut parts = repo.split('/');
    let (Some(a), Some(b), None) = (parts.next(), parts.next(), parts.next()) else {
        return None;
    };
    let ok = |s: &str| {
        !s.is_empty()
            && !matches!(s, "." | "..")
            && s.bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
    };
    (ok(a) && ok(b)).then(|| repo.to_string())
}

pub(crate) fn bind_origin_and_cred(
    dir: &Path,
    base_url: &str,
    cred_hash: &str,
) -> Result<(), RemoteSessionError> {
    let bindings = [
        (
            "spiron-cloud-origin.json",
            base_url,
            "Spiron identity belongs to another API origin",
            false,
        ),
        (
            "spiron-cloud-credential.json",
            cred_hash,
            "Spiron API key changed; explicit conversation migration required",
            true,
        ),
    ];
    for (file, expected, mismatch, is_auth) in bindings {
        let path = dir.join(file);
        match fs::read(&path) {
            Ok(b) => {
                let prev: String = serde_json::from_slice(&b)
                    .map_err(|_| proto(format!("invalid Spiron binding in {file}")))?;
                if prev != expected {
                    return Err(if is_auth {
                        RemoteSessionError::Auth(mismatch.into())
                    } else {
                        proto(mismatch)
                    });
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                persist_json(&path, expected, file)?;
            }
            Err(_) => return Err(transport(format!("cannot read Spiron binding in {file}"))),
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PendingRecord {
    pub scope: String,
    pub key: String,
    pub body: Value,
    #[serde(default)]
    pub receipt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_delivery: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ActiveRunState {
    pub agent_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    pub started_at_ms: i64,
}

pub(crate) fn persist_terminal(
    path: &Path,
    rec: &mut PendingRecord,
    receipt: Value,
) -> Result<(), RemoteSessionError> {
    if let Some(obj) = rec.body.as_object_mut() {
        obj.remove("mcp_servers");
    }
    rec.completed = Some(receipt);
    persist_json(path, rec, "Spiron terminal receipt")
}

pub(crate) fn clear_active_run(dir: &Path) {
    let _ = fs::remove_file(dir.join(ACTIVE_RUN_FILE));
}

pub(crate) fn declares_next_step(openapi: &Value) -> bool {
    openapi.get("x-spiron-input-delivery").is_some_and(|c| {
        c.get("protocol").and_then(Value::as_str) == Some("session-entry-v1")
            && c.get("level").and_then(Value::as_str) == Some("next-step")
            && c.get("idempotent").and_then(Value::as_bool) == Some(true)
            && c.get("proof").and_then(Value::as_str) == Some("input_delivery")
    })
}

pub(crate) fn build_capabilities(strict: bool) -> RemoteCapabilities {
    let delivery = if strict {
        Delivery {
            level: DeliveryLevel::NextStep,
            idempotent: true,
            proof: Cow::Borrowed(
                "session-entry-v1 input_delivery bound to API key, mailbox UUID and receipt",
            ),
            peer: Some("Spiron Agent/message API; session-entry-v1".into()),
        }
    } else {
        Delivery {
            level: DeliveryLevel::TurnEnd,
            idempotent: false,
            proof: Cow::Borrowed("terminal receipt only; peer lacks session-entry-v1"),
            peer: Some("Spiron Agent/message API".into()),
        }
    };
    RemoteCapabilities {
        provider: "spiron-cloud".into(),
        delivery,
        supports_steer: strict,
        supports_interrupt: true,
        supports_event_resume: true,
        supports_host_tools: false,
        supports_cost_reporting: true,
        supports_budget_cap: true,
    }
}

pub(crate) enum HttpOutcome {
    Ok(Value),
    Error(HttpError),
}

pub(crate) struct HttpError {
    pub status: u16,
    pub retry_after: Option<String>,
    pub body_json: Option<Value>,
    pub body_text: String,
}

pub(crate) struct SpironApi {
    client: ureq::Agent,
    pub base_url: String,
    token: Option<String>,
}

impl SpironApi {
    pub(crate) fn new(base_url: String, token: Option<String>, timeout: Duration) -> Self {
        let client: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(timeout))
            .http_status_as_error(false)
            .build()
            .into();
        Self {
            client,
            base_url,
            token,
        }
    }

    pub(crate) fn get_json(&self, path: &str) -> Result<HttpOutcome, RemoteSessionError> {
        let mut req = self
            .client
            .get(format!("{}{path}", self.base_url))
            .header("accept", "application/json");
        if let Some(tok) = &self.token {
            req = req.header("authorization", &format!("Bearer {tok}"));
        }
        Self::decode(
            req.call()
                .map_err(|_| transport("Spiron read failed; pending receipt retained"))?,
        )
    }

    pub(crate) fn post_json(
        &self,
        path: &str,
        key: &str,
        body: &Value,
    ) -> Result<HttpOutcome, RemoteSessionError> {
        let mut req = self
            .client
            .post(format!("{}{path}", self.base_url))
            .header("Idempotency-Key", key)
            .header("Prefer", "respond-async")
            .header("accept", "application/json");
        if let Some(tok) = &self.token {
            req = req.header("authorization", &format!("Bearer {tok}"));
        }
        Self::decode(req.send_json(body).map_err(|_| {
            transport("Spiron write acknowledgement uncertain; durable idempotency key retained")
        })?)
    }

    pub(crate) fn put_mcp_servers(
        &self,
        agent_id: &str,
        servers: &Value,
    ) -> Result<bool, RemoteSessionError> {
        let mut req = self
            .client
            .put(format!(
                "{}/v1/agents/{agent_id}/mcp-servers",
                self.base_url
            ))
            .header("content-type", "application/json")
            .header("accept", "application/json");
        if let Some(tok) = &self.token {
            req = req.header("authorization", &format!("Bearer {tok}"));
        }
        let resp = req
            .send_json(json!({ "mcp_servers": servers }))
            .map_err(|_| transport("Spiron MCP server registration failed"))?;
        let status = resp.status().as_u16();
        if (200..300).contains(&status) {
            Ok(true)
        } else if matches!(status, 404 | 405 | 501) {
            Ok(false)
        } else {
            Err(classify_http_error(&Self::read_err(status, resp)))
        }
    }

    pub(crate) fn cancel_receipt(&self, receipt_id: &str) -> Result<(), RemoteSessionError> {
        let id = validate_uuid(receipt_id, "receipt_id")?;
        let mut req = self
            .client
            .post(format!("{}/v1/message-receipts/{id}:cancel", self.base_url))
            .header("content-type", "application/json")
            .header("accept", "application/json");
        if let Some(tok) = &self.token {
            req = req.header("authorization", &format!("Bearer {tok}"));
        }
        let resp = req
            .send_empty()
            .map_err(|_| transport("Spiron cancellation request failed"))?;
        let status = resp.status().as_u16();
        // Per ADR-1515 / completion-race semantics, 404 and 409 are idempotent no-ops.
        if matches!(status, 404 | 409) {
            return Ok(());
        }
        if !(200..300).contains(&status) {
            return Err(classify_http_error(&Self::read_err(status, resp)));
        }
        let mut raw = String::new();
        let _ = resp
            .into_body()
            .into_reader()
            .take(MAX_ERROR_BODY_BYTES)
            .read_to_string(&mut raw);
        if raw.trim().is_empty() {
            return Ok(());
        }
        let r: Value =
            serde_json::from_str(raw.trim()).map_err(|_| proto("invalid Spiron cancel receipt"))?;
        if r.get("id").and_then(Value::as_str).is_some_and(|v| v != id) {
            return Err(proto("Spiron cancellation receipt identity mismatch"));
        }
        if let Some(st) = r.get("status").and_then(Value::as_str)
            && !matches!(
                st,
                "submitted"
                    | "working"
                    | "input_required"
                    | "queued"
                    | "running"
                    | "pending"
                    | "canceled"
                    | "cancelled"
                    | "completed"
                    | "failed"
            )
        {
            return Err(proto("Spiron cancellation was not confirmed"));
        }
        Ok(())
    }

    fn decode(resp: ureq::http::Response<ureq::Body>) -> Result<HttpOutcome, RemoteSessionError> {
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            return Ok(HttpOutcome::Error(Self::read_err(status, resp)));
        }
        let val = resp
            .into_body()
            .read_json::<Value>()
            .map_err(|_| proto("Spiron returned invalid JSON; pending state retained"))?;
        Ok(HttpOutcome::Ok(val))
    }

    fn read_err(status: u16, resp: ureq::http::Response<ureq::Body>) -> HttpError {
        let retry_after = resp
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let mut body_text = String::new();
        let _ = resp
            .into_body()
            .into_reader()
            .take(MAX_ERROR_BODY_BYTES)
            .read_to_string(&mut body_text);
        let body_json = serde_json::from_str::<Value>(&body_text).ok();
        HttpError {
            status,
            retry_after,
            body_json,
            body_text,
        }
    }
}

fn parse_retry_after_ms(
    header: Option<&str>,
    body: Option<&Value>,
    text: &str,
    now_ms: i64,
) -> i64 {
    if let Some(raw) = header.map(str::trim).filter(|s| !s.is_empty()) {
        if let Ok(secs) = raw.parse::<f64>()
            && secs.is_finite()
            && secs >= 0.0
        {
            return now_ms.saturating_add((secs * 1000.0).round() as i64);
        }
        if let Some(ms) = parse_rfc3339_utc_ms(raw) {
            return ms;
        }
    }
    if let Some(v) = body {
        let abs = [
            "/quota_reset_at_ms",
            "/resets_at_ms",
            "/reset_at_ms",
            "/error/quota_reset_at_ms",
        ];
        if let Some(ms) = abs.into_iter().find_map(|p| v.pointer(p)?.as_i64()) {
            return ms;
        }
        let rel_ms = [
            "/retry_after_ms",
            "/error/retry_after_ms",
            "/retry_delay_ms",
        ];
        if let Some(d) = rel_ms
            .into_iter()
            .find_map(|p| v.pointer(p)?.as_i64().filter(|&x| x >= 0))
        {
            return now_ms.saturating_add(d);
        }
        if let Some(s) = ["/retry_after", "/error/retry_after"]
            .into_iter()
            .find_map(|p| {
                v.pointer(p)?
                    .as_f64()
                    .filter(|x| x.is_finite() && *x >= 0.0)
            })
        {
            return now_ms.saturating_add((s * 1000.0).round() as i64);
        }
        if let Some(ms) = ptr_str(v, &["/resets_at", "/reset_at", "/error/resets_at"])
            .and_then(parse_rfc3339_utc_ms)
        {
            return ms;
        }
    }
    reset_at_ms(text, now_ms).unwrap_or_else(|| now_ms.saturating_add(DEFAULT_RETRY_AFTER_MS))
}

pub(crate) fn classify_http_error(err: &HttpError) -> RemoteSessionError {
    match err.status {
        401 => RemoteSessionError::Auth("Spiron API authentication failed (HTTP 401)".into()),
        429 => RemoteSessionError::Quota {
            reset_at_ms: parse_retry_after_ms(
                err.retry_after.as_deref(),
                err.body_json.as_ref(),
                &err.body_text,
                now_epoch_ms(),
            ),
            message: "Spiron API rate limited (HTTP 429)".into(),
        },
        402 => {
            let (spent_micros, cap_micros) = spend_cap_bounds(err.body_json.as_ref(), None);
            RemoteSessionError::BudgetExceeded {
                spent_micros,
                cap_micros,
            }
        }
        s if (500..=599).contains(&s) => transport(format!("Spiron API returned HTTP {s}")),
        s => proto(format!("Spiron API returned HTTP {s}")),
    }
}

fn ascii_code(raw: Option<&str>) -> Option<String> {
    let t = raw?.trim();
    (!t.is_empty()
        && t.len() <= 64
        && t.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.')))
    .then(|| t.to_string())
}

pub(crate) fn is_spend_cap_error(err: &HttpError) -> bool {
    err.status == 402
        || err.body_json.as_ref().is_some_and(|v| {
            ptr_str(v, &["/code", "/error/code", "/type", "/error/type"]).is_some_and(|c| {
                matches!(
                    c.to_ascii_lowercase().as_str(),
                    "spend_cap_reached"
                        | "spend_cap_exceeded"
                        | "budget_exceeded"
                        | "insufficient_credits"
                )
            }) || v.get("reached").and_then(Value::as_bool) == Some(true)
        })
}

pub(crate) fn spend_cap_bounds(val: Option<&Value>, fallback: Option<RemoteBudget>) -> (u64, u64) {
    let spent_m = [
        "/spent_micros",
        "/current_spend_micros",
        "/used_micros",
        "/error/spent_micros",
    ];
    let spent_u = [
        "/spent_usd",
        "/current_spend_usd",
        "/used_usd",
        "/error/spent_usd",
    ];
    let cap_m = [
        "/cap_micros",
        "/limit_micros",
        "/monthly_limit_micros",
        "/error/cap_micros",
        "/error/limit_micros",
    ];
    let cap_u = [
        "/cap_usd",
        "/limit_usd",
        "/monthly_limit_usd",
        "/error/cap_usd",
        "/error/limit_usd",
    ];
    let spent = val
        .and_then(|v| ptr_micros(v, &spent_m, &spent_u))
        .or_else(|| fallback.map(|b| b.spent_micros))
        .unwrap_or(0);
    let cap = val
        .and_then(|v| ptr_micros(v, &cap_m, &cap_u))
        .or_else(|| fallback.map(|b| b.cap_micros))
        .unwrap_or(0);
    (spent, cap)
}

pub(crate) fn classify_rejected_4xx(err: &HttpError) -> RemoteDeliveryAck {
    let code = ascii_code(
        err.body_json
            .as_ref()
            .and_then(|v| ptr_str(v, &["/error/code", "/code", "/error/type"])),
    )
    .unwrap_or_else(|| format!("http_{}", err.status));
    let msg = err
        .body_json
        .as_ref()
        .and_then(|v| ptr_str(v, &["/error/message", "/message", "/title"]))
        .unwrap_or(&err.body_text);
    let message_specific =
        err.status == 413 || names_message_size(msg) || names_message_size(&code);
    let reason = if message_specific {
        format!(
            "Spiron rejected message: payload too large (HTTP {})",
            err.status
        )
    } else {
        format!("Spiron rejected request (HTTP {}: {code})", err.status)
    };
    RemoteDeliveryAck::Rejected {
        code,
        reason,
        message_specific,
    }
}

pub(crate) fn extract_usage(r: &Value) -> TurnUsage {
    let cost_m = [
        "/cost_micros",
        "/total_cost_micros",
        "/usage/cost_micros",
        "/usage/total_cost_micros",
    ];
    let cost_u = [
        "/cost_usd",
        "/total_cost_usd",
        "/usage/cost_usd",
        "/usage/total_cost_usd",
    ];
    let cache_r = [
        "/usage/cache_read_tokens",
        "/usage/cached_tokens",
        "/usage/cache_read_input_tokens",
        "/usage/input_tokens_details/cached_tokens",
    ];
    let cache_w = [
        "/usage/cache_write_tokens",
        "/usage/cache_creation_input_tokens",
    ];
    TurnUsage {
        input_tokens: ptr_u64(r, &["/usage/input_tokens", "/usage/prompt_tokens"]).unwrap_or(0),
        output_tokens: ptr_u64(r, &["/usage/output_tokens", "/usage/completion_tokens"])
            .unwrap_or(0),
        cache_read_tokens: ptr_u64(r, &cache_r).unwrap_or(0),
        cache_write_tokens: ptr_u64(r, &cache_w).unwrap_or(0),
        reported_cost_micros: ptr_micros(r, &cost_m, &cost_u).and_then(|n| i64::try_from(n).ok()),
    }
}

fn extract_output_text(r: &Value) -> Option<String> {
    if let Some(items) = r.get("output").and_then(Value::as_array) {
        let mut parts = Vec::new();
        for item in items {
            if matches!(
                item.get("type").and_then(Value::as_str),
                None | Some("text" | "output_text")
            ) && let Some(t) = item
                .get("text")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            {
                parts.push(t.to_string());
            }
            if let Some(content) = item.get("content").and_then(Value::as_array) {
                for p in content {
                    if let Some(t) = p
                        .get("text")
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                    {
                        parts.push(t.to_string());
                    }
                }
            }
        }
        if !parts.is_empty() {
            return Some(parts.join("\n"));
        }
    }
    let obj = r.get("output")?.as_object()?;
    if obj.get("type").and_then(Value::as_str) != Some("text") {
        return None;
    }
    obj.get("text")?
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn fetch_conversation_reply(
    api: &SpironApi,
    agent_id: &str,
    receipt_id: &str,
    deadline: Instant,
) -> Result<Option<String>, RemoteSessionError> {
    let mut path = format!("/v1/agents/{agent_id}/conversation?limit=100");
    let (mut cursors, mut seen_ids, mut pages) = (HashSet::new(), HashSet::new(), Vec::new());
    loop {
        if Instant::now() >= deadline {
            return Err(transport("Spiron conversation read deadline reached"));
        }
        let page = match api.get_json(&path)? {
            HttpOutcome::Ok(val) => val,
            HttpOutcome::Error(err) => return Err(classify_http_error(&err)),
        };
        if page.get("agent_id").and_then(Value::as_str) != Some(agent_id) {
            return Err(proto("Spiron conversation identity mismatch"));
        }
        let entries = page
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| proto("invalid Spiron conversation page"))?;
        let mut texts = Vec::new();
        for entry in entries {
            if entry.get("task_id").and_then(Value::as_str) != Some(receipt_id)
                || entry.get("role").and_then(Value::as_str) != Some("agent")
            {
                continue;
            }
            let id = entry
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| proto("Spiron conversation item missing id"))?;
            if seen_ids.insert(id.to_string())
                && let Some(out) = entry.get("output")
                && out.get("type").and_then(Value::as_str) == Some("text")
                && let Some(text) = out
                    .get("text")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
            {
                texts.push(text.to_string());
            }
        }
        pages.push(texts);
        let has_more = page
            .get("has_more")
            .and_then(Value::as_bool)
            .ok_or_else(|| proto("Spiron conversation missing has_more"))?;
        if !has_more {
            break;
        }
        if pages.len() >= MAX_CONVERSATION_PAGES {
            return Err(proto("Spiron conversation page limit exceeded"));
        }
        let cursor = page
            .get("next_cursor")
            .and_then(Value::as_str)
            .ok_or_else(|| proto("Spiron conversation missing cursor"))?;
        if cursor.is_empty() || !cursors.insert(cursor.to_string()) {
            return Err(proto("invalid or repeated Spiron conversation cursor"));
        }
        path = format!(
            "/v1/agents/{agent_id}/conversation?limit=100&before={}",
            percent_encode(cursor)
        );
    }
    let reply = pages
        .into_iter()
        .rev()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");
    Ok((!reply.is_empty()).then_some(reply))
}

pub(crate) fn resolve_completed_reply(
    api: &SpironApi,
    agent_id: &str,
    receipt_id: &str,
    receipt: &Value,
    deadline: Instant,
) -> Result<String, RemoteSessionError> {
    if let Some(reply) = receipt
        .get("reply")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        return Ok(reply.to_string());
    }
    if let Some(reply) = extract_output_text(receipt) {
        return Ok(reply);
    }
    fetch_conversation_reply(api, agent_id, receipt_id, deadline)?
        .filter(|s| !s.is_empty())
        .ok_or_else(|| proto("completed Spiron receipt has no reply"))
}

pub(crate) fn validate_proof(raw: &Value) -> Result<RemoteConsumptionProof, RemoteSessionError> {
    let field = |n: &str| validate_uuid(raw.get(n).and_then(Value::as_str).unwrap_or(""), n);
    let recorded_at = raw
        .get("recorded_at")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| proto("Spiron input proof missing recorded_at"))?;
    Ok(RemoteConsumptionProof {
        session_id: field("session_id")?,
        entry_id: field("entry_id")?,
        turn_entry_id: Some(field("turn_entry_id")?),
        recorded_at: Some(recorded_at.to_string()),
    })
}

pub(crate) fn classify_receipt_failure(
    receipt: &Value,
) -> (String, String, Option<u16>, Option<i64>) {
    let err = receipt.get("error");
    let status = err
        .and_then(|e| {
            e.get("status")
                .or_else(|| e.get("apiErrorStatus"))?
                .as_u64()
        })
        .and_then(|n| u16::try_from(n).ok());
    match status {
        Some(401) => (
            "authentication_failed".into(),
            "Spiron turn failed: authentication error (HTTP 401)".into(),
            Some(401),
            None,
        ),
        Some(429) => {
            let txt = err
                .and_then(|e| {
                    e.get("message")
                        .and_then(Value::as_str)
                        .or_else(|| e.as_str())
                })
                .unwrap_or("");
            let reset = parse_retry_after_ms(None, err, txt, now_epoch_ms());
            (
                "rate_limit".into(),
                "Spiron turn failed: rate limited (HTTP 429)".into(),
                Some(429),
                Some(reset),
            )
        }
        Some(402) => (
            "budget_exceeded".into(),
            "Spiron turn failed: spend cap reached (HTTP 402)".into(),
            Some(402),
            None,
        ),
        other => {
            let code = ascii_code(err.and_then(|e| e.get("code")?.as_str()))
                .unwrap_or_else(|| "turn_failed".into());
            let msg = other.map_or_else(
                || format!("Spiron turn failed ({code})"),
                |s| format!("Spiron turn failed (HTTP {s}: {code})"),
            );
            (code, msg, other, None)
        }
    }
}

pub(crate) fn resolve_or_create_agent(
    config: &SpironCloudConfig,
    api: &SpironApi,
    cfg: &RemoteSessionConfig,
) -> Result<(String, bool), RemoteSessionError> {
    let id_path = config.config_dir.join("spiron-cloud-session-id");
    let saved = fs::read_to_string(&id_path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let resumed = cfg
        .resume_session_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or(saved);
    if let Some(existing) = resumed {
        let id = validate_uuid(&existing, "agent_id")?;
        save_private(&id_path, format!("{id}\n").as_bytes())?;
        return Ok((id, false));
    }
    let create_path = config.config_dir.join("spiron-cloud-create.json");
    let pending = match fs::read(&create_path) {
        Ok(b) => {
            let p: PendingRecord = serde_json::from_slice(&b)
                .map_err(|_| proto("invalid Spiron pending create request"))?;
            if p.scope != api.base_url {
                return Err(proto(
                    "Spiron pending create request belongs to another API origin",
                ));
            }
            p
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let name = if cfg.agent_name.trim().is_empty() {
                "Remote worker"
            } else {
                cfg.agent_name.trim()
            };
            // Coordination-only: never attach host browser/computer/shell tools.
            let mut body = json!({
                "name": name,
                "tools": { "disabled": config.disabled_tools }
            });
            if let Some(m) = cfg
                .model
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                body["model"] = json!(m);
            }
            if let Some(i) = cfg
                .instructions
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                body["instructions"] = json!(i);
            }
            if let Some(ctx) = workspace_context(&config.config_dir)?
                && let Some(repo) = ctx
                    .get("repo")
                    .and_then(Value::as_str)
                    .and_then(github_repo)
            {
                body["github"] = json!({ "repositories": [repo] });
            }
            let rec = PendingRecord {
                scope: api.base_url.clone(),
                key: new_idempotency_uuid(&create_path),
                body,
                receipt: None,
                run_id: None,
                input_delivery: None,
                completed: None,
            };
            persist_json(&create_path, &rec, "Spiron create request")?;
            rec
        }
        Err(_) => return Err(transport("cannot read Spiron pending create request")),
    };
    let created = match api.post_json("/v1/agents", &pending.key, &pending.body)? {
        HttpOutcome::Ok(val) => val,
        HttpOutcome::Error(err) => return Err(classify_http_error(&err)),
    };
    let id = validate_uuid(
        created.get("id").and_then(Value::as_str).unwrap_or(""),
        "agent_id",
    )?;
    save_private(&id_path, format!("{id}\n").as_bytes())?;
    let _ = fs::remove_file(&create_path);
    Ok((id, true))
}

pub(crate) fn sync_mcp_servers(
    config_dir: &Path,
    api: &SpironApi,
    agent_id: &str,
    scope: &str,
    creds: Option<(String, String)>,
    force: bool,
) -> Result<(), RemoteSessionError> {
    let Some((url, token)) = creds else {
        return Ok(());
    };
    let servers = json!([{ "name": "anyagents", "url": url, "bearer_token": token }]);
    let encoded = serde_json::to_vec(&json!({ "scope": scope, "servers": &servers }))
        .map_err(|e| proto(e.to_string()))?;
    let digest = format!("{:x}", Sha256::digest(&encoded));
    let sync_path = config_dir.join("spiron-cloud-mcp-sync.json");
    let unchanged = fs::read(&sync_path)
        .ok()
        .and_then(|b| serde_json::from_slice::<String>(&b).ok())
        .as_deref()
        == Some(digest.as_str());
    if !force && unchanged {
        return Ok(());
    }
    if api.put_mcp_servers(agent_id, &servers)? {
        persist_json(&sync_path, &digest, "Spiron MCP sync digest")?;
    }
    Ok(())
}
