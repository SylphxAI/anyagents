//! Spiron's public Agent/message API. Agent identity is not an engine session or receipt.
use super::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const HARNESS: &str = "spiron-cloud";
const TURN_BUDGET: Duration = crate::remote::LONGEST_TURN;

#[derive(Serialize, Deserialize)]
struct Pending {
    scope: String,
    key: String,
    body: Value,
    receipt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mailbox_uuid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    input_delivery: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    completed: Option<Value>,
}

const ACTIVE_RUN_FILE: &str = "spiron-cloud-active-run.json";

#[derive(Serialize, Deserialize, Clone)]
pub(super) struct ActiveRunState {
    pub(super) agent_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) receipt_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) idempotency_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) content: Option<String>,
    pub(super) started_at_ms: i64,
}

pub(super) fn persist_active_run(config: &Path, state: &ActiveRunState) -> Result<(), String> {
    let bytes = serde_json::to_vec(state).map_err(|e| e.to_string())?;
    save(&config.join(ACTIVE_RUN_FILE), &bytes)
}

pub(super) fn read_active_run(config: &Path) -> Result<Option<ActiveRunState>, String> {
    match std::fs::read(config.join(ACTIVE_RUN_FILE)) {
        Ok(bytes) => {
            let parsed: ActiveRunState = serde_json::from_slice(&bytes)
                .map_err(|_| "invalid Spiron active run state".to_string())?;
            validate_id(&parsed.agent_id, "agent_id")?;
            if let Some(ref receipt_id) = parsed.receipt_id {
                validate_id(receipt_id, "receipt_id")?;
            }
            if let Some(ref run_id) = parsed.run_id {
                validate_id(run_id, "run_id")?;
            }
            Ok(Some(parsed))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("cannot read Spiron active run state: {e}")),
    }
}

pub(super) fn clear_active_run(config: &Path) {
    let _ = std::fs::remove_file(config.join(ACTIVE_RUN_FILE));
}

fn save(path: &Path, value: &[u8]) -> Result<(), String> {
    anyagents_harness::atomic_write_nonempty_file(path, value, 0o600)
        .map_err(|e| format!("cannot persist Spiron state: {e}"))
}

fn pending(path: &Path, scope: &str, body: Value, key: Option<&str>) -> Result<Pending, String> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let p: Pending = serde_json::from_slice(&bytes)
                .map_err(|_| "invalid Spiron pending request".to_string())?;
            if p.scope != scope {
                return Err("Spiron pending request belongs to another API or Agent".into());
            }
            Ok(p)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let seed = format!(
                "{}:{}:{}:{:?}",
                path.display(),
                std::process::id(),
                SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                SystemTime::now().duration_since(UNIX_EPOCH)
            );
            let mut bytes = Sha256::digest(seed.as_bytes());
            bytes[6] = (bytes[6] & 15) | 64;
            bytes[8] = (bytes[8] & 63) | 128;
            let h = bytes[..16]
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            let p = Pending {
                scope: scope.to_string(),
                key: key.map(str::to_string).unwrap_or_else(|| {
                    format!(
                        "{}-{}-{}-{}-{}",
                        &h[..8],
                        &h[8..12],
                        &h[12..16],
                        &h[16..20],
                        &h[20..]
                    )
                }),
                body,
                receipt: None,
                run_id: None,
                mailbox_uuid: key.map(str::to_string),
                input_delivery: None,
                completed: None,
            };
            save(path, &serde_json::to_vec(&p).map_err(|e| e.to_string())?)?;
            Ok(p)
        }
        Err(e) => Err(format!("cannot read Spiron pending request: {e}")),
    }
}

pub(super) fn validate_id(id: &str, field: &str) -> Result<String, String> {
    // These IDs become path segments; never accept arbitrary URLs or slashes.
    if id.len() != 36
        || !id.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        })
    {
        return Err(format!("Spiron response has invalid {field}"));
    }
    Ok(id.to_string())
}

fn identifier(value: &Value, field: &str) -> Result<String, String> {
    validate_id(
        value.get(field).and_then(Value::as_str).unwrap_or(""),
        field,
    )
}

enum PostMessageResponse {
    Json(Value),
    Stream(CloudTurnOutcome),
}

struct Api {
    client: ureq::Agent,
    stream_client: ureq::Agent,
    base: String,
    token: Option<String>,
}

impl Api {
    fn new(config: &Path) -> Result<Self, CloudErrorDetail> {
        let token =
            resolve_cloud_auth_token_checked(config, HARNESS).map_err(|msg| CloudErrorDetail {
                status: Some(401),
                code: "authentication_failed",
                message: msg,
                quota_reset_at_ms: None,
            })?;
        let idle = cloud_idle_timeout();
        Ok(Self {
            client: ureq::Agent::config_builder()
                .timeout_global(Some(crate::remote::REQUEST_MARGIN))
                .http_status_as_error(false)
                .build()
                .into(),
            stream_client: ureq::Agent::config_builder()
                .timeout_connect(Some(Duration::from_secs(15)))
                .timeout_send_request(Some(Duration::from_secs(30)))
                .timeout_send_body(Some(Duration::from_secs(30)))
                .timeout_recv_response(Some(idle))
                .timeout_recv_body(None)
                .timeout_global(None)
                .http_status_as_error(false)
                .build()
                .into(),
            base: resolve_cloud_base_url(config, HARNESS),
            token,
        })
    }

    fn get(&self, path: &str) -> Result<Value, String> {
        self.get_detailed(path).map_err(|d| d.message)
    }

    fn get_detailed(&self, path: &str) -> Result<Value, CloudErrorDetail> {
        let mut req = self.client.get(format!("{}{path}", self.base));
        if let Some(token) = &self.token {
            req = req.header("authorization", &format!("Bearer {token}"));
        }
        let response = req.call().map_err(|_| {
            CloudErrorDetail::generic("Spiron read failed; pending receipt retained")
        })?;
        Self::decode_detailed(response)
    }

    fn post_detailed(&self, path: &str, p: &Pending) -> Result<Value, CloudErrorDetail> {
        let mut req = self
            .client
            .post(format!("{}{path}", self.base))
            .header("Idempotency-Key", &p.key)
            .header("Prefer", "respond-async");
        if let Some(token) = &self.token {
            req = req.header("authorization", &format!("Bearer {token}"));
        }
        let response = req.send_json(&p.body).map_err(|_| {
            CloudErrorDetail::generic(
                "Spiron write acknowledgement uncertain; durable idempotency key retained",
            )
        })?;
        Self::decode_detailed(response)
    }

    fn post_message_stream_or_json(
        &self,
        path: &str,
        p: &Pending,
        agent_id: &str,
        cancel_flag: &TurnCancel,
    ) -> Result<PostMessageResponse, CloudErrorDetail> {
        let mut req = self
            .stream_client
            .post(format!("{}{path}", self.base))
            .header("Idempotency-Key", &p.key)
            .header("Prefer", "respond-async")
            .header("Accept", "text/event-stream, application/json");
        if let Some(token) = &self.token {
            req = req.header("authorization", &format!("Bearer {token}"));
        }
        let response = req.send_json(&p.body).map_err(|_| {
            CloudErrorDetail::generic(
                "Spiron write acknowledgement uncertain; durable idempotency key retained",
            )
        })?;
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_ascii_lowercase();
        if (200..300).contains(&status) && content_type.contains("text/event-stream") {
            let reader = BufReader::new(response.into_body().into_reader());
            let outcome = read_sse_cloud_stream(
                reader,
                HARNESS,
                Some(agent_id),
                cloud_idle_timeout(),
                cancel_flag,
            )?;
            return Ok(PostMessageResponse::Stream(outcome));
        }
        Self::decode_detailed(response).map(PostMessageResponse::Json)
    }

    fn put_mcp_servers(&self, agent_id: &str, servers: &Value) -> Result<bool, CloudErrorDetail> {
        let path = format!("/v1/agents/{agent_id}/mcp-servers");
        let mut req = self
            .client
            .put(format!("{}{path}", self.base))
            .header("content-type", "application/json")
            .header("accept", "application/json");
        if let Some(token) = &self.token {
            req = req.header("authorization", &format!("Bearer {token}"));
        }
        let response = req
            .send_json(&json!({ "mcp_servers": servers }))
            .map_err(|_| CloudErrorDetail::generic("Spiron MCP server registration failed"))?;
        let status = response.status().as_u16();
        if (200..300).contains(&status) {
            return Ok(true);
        }
        if matches!(status, 404 | 405 | 501) {
            return Ok(false);
        }
        Self::decode_detailed(response).map(|_| true)
    }

    fn decode_detailed(
        response: ureq::http::Response<ureq::Body>,
    ) -> Result<Value, CloudErrorDetail> {
        let status = response.status().as_u16();
        if !(200..300).contains(&status) {
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            let mut raw = String::new();
            let _ = response
                .into_body()
                .into_reader()
                .take(16 * 1024)
                .read_to_string(&mut raw);
            if status == 429 || status == 401 {
                return Err(classify_http_error(
                    status,
                    retry_after.as_deref(),
                    &raw,
                    HARNESS,
                    "spiron",
                ));
            }
            if status == 503 {
                // Provider error bodies must not echo prompts/capabilities, while preserving HTTP 503 code.
                return Err(CloudErrorDetail {
                    status: Some(503),
                    code: "service_unavailable",
                    message: format!("Spiron API returned HTTP {status}"),
                    quota_reset_at_ms: None,
                });
            }
            // Provider error bodies can echo prompts, headers or MCP capabilities.
            return Err(CloudErrorDetail {
                status: Some(status),
                code: "cloud_error",
                message: format!("Spiron API returned HTTP {status}"),
                quota_reset_at_ms: None,
            });
        }
        response.into_body().read_json().map_err(|_| {
            CloudErrorDetail::generic("Spiron returned invalid JSON; pending state retained")
        })
    }
}

mod controller;
pub(super) use controller::serve;

/// One controller owns the config lock and credential identity. Message requests
/// do not compete for this lock, and key rotation cannot split a conversation.
struct Session {
    _lock: std::fs::File,
    api: Api,
    agent_id: String,
    scope: String,
}

impl Session {
    fn open(config: &Path, model: Option<&str>, resume: Option<&str>) -> Result<Self, String> {
        Self::open_detailed(config, model, resume).map_err(|d| d.message)
    }

    fn open_detailed(
        config: &Path,
        model: Option<&str>,
        resume: Option<&str>,
    ) -> Result<Self, CloudErrorDetail> {
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(config.join("spiron-cloud.lock"))
            .map_err(|_| CloudErrorDetail::generic("cannot open Spiron turn lock"))?;
        lock.try_lock()
            .map_err(|_| CloudErrorDetail::generic("another Spiron controller owns this config"))?;
        let api = Api::new(config)?;
        // Bind persisted identities and capabilities to the endpoint that created them.
        let origin = config.join("spiron-cloud-origin.json");
        match std::fs::read(&origin) {
            Ok(bytes) => {
                let bound: String = serde_json::from_slice(&bytes)
                    .map_err(|_| CloudErrorDetail::generic("invalid Spiron origin state"))?;
                if bound != api.base {
                    return Err(CloudErrorDetail::generic(
                        "Spiron identity belongs to another API origin",
                    ));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                save(
                    &origin,
                    &serde_json::to_vec(&api.base)
                        .map_err(|e| CloudErrorDetail::generic(e.to_string()))?,
                )
                .map_err(CloudErrorDetail::generic)?;
            }
            Err(_) => {
                return Err(CloudErrorDetail::generic("cannot read Spiron origin state"));
            }
        }
        // Changing API key changes the remote connection and conversation. Refuse
        // silently rebinding an existing identity, including across restarts.
        let credential = format!(
            "{:x}",
            Sha256::digest(api.token.as_deref().unwrap_or("").as_bytes())
        );
        let binding = config.join("spiron-cloud-credential.json");
        match std::fs::read(&binding) {
            Ok(bytes) => {
                let previous: String = serde_json::from_slice(&bytes)
                    .map_err(|_| CloudErrorDetail::generic("invalid Spiron credential binding"))?;
                if previous != credential {
                    return Err(CloudErrorDetail::generic(
                        "Spiron API key changed; explicit conversation migration required",
                    ));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => save(
                &binding,
                &serde_json::to_vec(&credential)
                    .map_err(|e| CloudErrorDetail::generic(e.to_string()))?,
            )
            .map_err(CloudErrorDetail::generic)?,
            Err(_) => {
                return Err(CloudErrorDetail::generic(
                    "cannot read Spiron credential binding",
                ));
            }
        }
        let identity_path = config.join(session_filename_for(HARNESS));
        let mut created_fresh = false;
        let agent_id = if let Some(id) = resume
            .map(str::to_string)
            .or_else(|| read_persisted_session_id(config, HARNESS))
        {
            identifier(&json!({"id": id}), "id").map_err(CloudErrorDetail::generic)?
        } else {
            let path = config.join("spiron-cloud-create.json");
            let mut body = json!({"name": "Remote worker", "tools": {"disabled": resolve_cloud_disabled_tools(config)}});
            if let Some(model) = model {
                body["model"] = json!(model);
            }
            if let Some(instructions) = build_cloud_system_prompt(config) {
                body["instructions"] = json!(instructions);
            }
            if let Some(context) = workspace_context(config).map_err(CloudErrorDetail::generic)?
                && let Some(repo) = context
                    .get("repo")
                    .and_then(Value::as_str)
                    .and_then(github_repository)
            {
                body["github"] = json!({"repositories": [repo]});
            }
            let request =
                pending(&path, &api.base, body, None).map_err(CloudErrorDetail::generic)?;
            let created = api.post_detailed("/v1/agents", &request)?;
            let id = identifier(&created, "id").map_err(CloudErrorDetail::generic)?;
            save(&identity_path, format!("{id}\n").as_bytes())
                .map_err(CloudErrorDetail::generic)?;
            std::fs::remove_file(path).map_err(|e| CloudErrorDetail::generic(e.to_string()))?;
            created_fresh = true;
            id
        };
        save(&identity_path, format!("{agent_id}\n").as_bytes())
            .map_err(CloudErrorDetail::generic)?;

        let scope = format!("{}/{}/{}", api.base, agent_id, credential);
        let session = Self {
            _lock: lock,
            api,
            agent_id,
            scope,
        };
        let explicit_sync = std::env::var("ANYAGENTS_SPIRON_SYNC_MCP_SERVERS")
            .ok()
            .is_some_and(|v| matches!(v.trim(), "1" | "true"))
            || read_cloud_json(config)
                .and_then(|v| v.get("sync_mcp_servers").and_then(Value::as_bool))
                == Some(true);
        if explicit_sync && config.join(crate::remote::TOKEN_FILE).exists() {
            session.sync_mcp_servers(config, created_fresh || explicit_sync)?;
        }
        Ok(session)
    }

    fn mcp_servers_payload(config: &Path) -> Result<Option<Value>, String> {
        if !config.join(crate::remote::TOKEN_FILE).exists() {
            return Ok(None);
        }
        let url = resolve_public_mcp_url(config)?;
        let validated = validate_remote_mcp_url(config, &url)?;
        let capability = crate::remote::session_token(config)?;
        Ok(Some(json!([{
            "name": "anyagents",
            "url": mcp_url_with_profile(&validated, remote_mcp_profile(config)),
            "bearer_token": capability
        }])))
    }

    fn sync_mcp_servers(&self, config: &Path, force: bool) -> Result<(), CloudErrorDetail> {
        let Some(servers) = Self::mcp_servers_payload(config).map_err(CloudErrorDetail::generic)?
        else {
            return Ok(());
        };
        let encoded = serde_json::to_vec(&json!({
            "scope": &self.scope,
            "servers": &servers
        }))
        .map_err(|e| CloudErrorDetail::generic(e.to_string()))?;
        let digest = format!("{:x}", Sha256::digest(&encoded));
        let sync_path = config.join("spiron-cloud-mcp-sync.json");
        if !force
            && let Ok(bytes) = std::fs::read(&sync_path)
            && serde_json::from_slice::<String>(&bytes).is_ok_and(|prev| prev == digest)
        {
            return Ok(());
        }
        if self.api.put_mcp_servers(&self.agent_id, &servers)? {
            let bytes = serde_json::to_vec(&digest)
                .map_err(|e| CloudErrorDetail::generic(e.to_string()))?;
            save(&sync_path, &bytes).map_err(CloudErrorDetail::generic)?;
        }
        Ok(())
    }

    fn body(config: &Path, prompt: &str) -> Result<Value, String> {
        let content = if let Some(context) = workspace_context(config)? {
            format!(
                "Remote workspace context (repository, branch, base): {context}\nPrepare this checkout in your cloud workspace before repository work; never use the controller filesystem.\n\n{prompt}"
            )
        } else {
            prompt.to_string()
        };
        let mut body = json!({"content": content});
        // Per-turn Streamable HTTP MCP attachment (live Spiron Agents API contract).
        // Never export the local mailbox capability; re-read the expiring remote token each turn.
        if let Some(servers) = Self::mcp_servers_payload(config)? {
            body["mcp_servers"] = servers;
        }
        Ok(body)
    }
}

/// Synchronous callers use the same session/request engine. The stream
/// controller supplies mailbox UUIDs; this older API retains its durable key.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn dispatch(
    config: &Path,
    model: Option<&str>,
    resume: Option<&str>,
    prompt: &str,
    cancel: &TurnCancel,
    active: &Mutex<Option<String>>,
) -> Result<CloudTurnOutcome, String> {
    dispatch_detailed(config, model, resume, prompt, cancel, active).map_err(|d| d.message)
}

pub(super) fn dispatch_detailed(
    config: &Path,
    model: Option<&str>,
    resume: Option<&str>,
    prompt: &str,
    cancel: &TurnCancel,
    active: &Mutex<Option<String>>,
) -> Result<CloudTurnOutcome, CloudErrorDetail> {
    let session = Session::open_detailed(config, model, resume)?;
    if cancel.is_cancelled() {
        return Err(CloudErrorDetail::generic(
            "cancelled before Spiron message submission",
        ));
    }
    let path = config.join("spiron-cloud-message.json");
    if !path.exists()
        && let Some(active_run) = read_active_run(config).map_err(CloudErrorDetail::generic)?
        && active_run.agent_id == session.agent_id
        && active_run.content.as_deref() == Some(prompt)
        && let Some(receipt_id) = active_run.receipt_id.clone().or(active_run.run_id.clone())
    {
        return reattach_active_run(&session, config, &active_run, &receipt_id, cancel, active);
    }
    let mut input = controller::Input::prepare(&session, config, path, None, prompt)
        .map_err(CloudErrorDetail::generic)?;
    let deadline = Instant::now() + TURN_BUDGET;
    let mut poll_interval = Duration::from_millis(250);
    let result = (|| loop {
        if let Some(outcome) = input.advance_detailed(&session, false, cancel)? {
            clear_active_run(config);
            return Ok(outcome);
        }
        if let Some(reason) = input.failure.take() {
            clear_active_run(config);
            return Err(CloudErrorDetail::generic(reason));
        }
        let active_id = input
            .request
            .run_id
            .clone()
            .or_else(|| input.request.receipt.clone());
        *active.lock().unwrap() = active_id;
        if cancel.is_cancelled() || Instant::now() >= deadline {
            if let Some(task) = &input.request.receipt {
                if let Some(run_id) = input
                    .request
                    .run_id
                    .as_deref()
                    .filter(|r| *r != task.as_str())
                {
                    cancel_spiron_run(
                        &session.api.base,
                        session.api.token.as_deref(),
                        &session.agent_id,
                        run_id,
                        Some(task),
                    )
                    .map_err(CloudErrorDetail::generic)?;
                } else {
                    cancel_cloud_turn(
                        &session.api.base,
                        HARNESS,
                        session.api.token.as_deref(),
                        task,
                    )
                    .map_err(CloudErrorDetail::generic)?;
                }
            }
            clear_active_run(config);
            return Err(CloudErrorDetail::generic(
                "Spiron cancellation requested; receipt retained for reconciliation",
            ));
        }
        cancel.wait(apply_poll_jitter(poll_interval));
        poll_interval = (poll_interval * 2).min(Duration::from_secs(5));
    })();
    *active.lock().unwrap() = None;
    result
}

fn reattach_active_run(
    session: &Session,
    config: &Path,
    active_run: &ActiveRunState,
    receipt_id: &str,
    cancel: &TurnCancel,
    active: &Mutex<Option<String>>,
) -> Result<CloudTurnOutcome, CloudErrorDetail> {
    let mut poll_interval = Duration::from_millis(250);
    let deadline = Instant::now() + TURN_BUDGET;
    let mut transient_errors = 0u32;
    let mut use_run_endpoint = false;
    loop {
        *active.lock().unwrap() = active_run
            .run_id
            .clone()
            .or_else(|| Some(receipt_id.to_string()));
        if cancel.is_cancelled() || Instant::now() >= deadline {
            if let Some(run_id) = &active_run.run_id {
                let _ = cancel_spiron_run(
                    &session.api.base,
                    session.api.token.as_deref(),
                    &session.agent_id,
                    run_id,
                    Some(receipt_id),
                );
            } else {
                let _ = cancel_cloud_turn(
                    &session.api.base,
                    HARNESS,
                    session.api.token.as_deref(),
                    receipt_id,
                );
            }
            *active.lock().unwrap() = None;
            return Err(CloudErrorDetail::generic(
                "Spiron cancellation requested during crash re-attach",
            ));
        }
        let path = if use_run_endpoint && let Some(run_id) = active_run.run_id.as_deref() {
            format!("/v1/agents/{}/runs/{run_id}", session.agent_id)
        } else {
            format!("/v1/message-receipts/{receipt_id}")
        };
        let val = match session.api.get_detailed(&path) {
            Ok(v) => {
                transient_errors = 0;
                v
            }
            Err(d) if d.status == Some(404) && !use_run_endpoint && active_run.run_id.is_some() => {
                use_run_endpoint = true;
                continue;
            }
            Err(d) if d.status.is_some_and(|s| (500..=599).contains(&s)) => {
                transient_errors += 1;
                if transient_errors > 5 {
                    *active.lock().unwrap() = None;
                    return Err(d);
                }
                cancel.wait(apply_poll_jitter(poll_interval));
                poll_interval = (poll_interval * 2).min(Duration::from_secs(5));
                continue;
            }
            Err(d) => {
                *active.lock().unwrap() = None;
                return Err(d);
            }
        };
        match val.get("status").and_then(Value::as_str) {
            Some("completed") => {
                let reply = val
                    .get("reply")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string);
                let reply = match reply {
                    Some(r) => r,
                    None => conversation_reply(
                        &session.api,
                        &session.agent_id,
                        receipt_id,
                        cancel,
                        deadline,
                    )
                    .map_err(CloudErrorDetail::generic)?
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        CloudErrorDetail::generic("completed Spiron run has no reply")
                    })?,
                };
                clear_active_run(config);
                *active.lock().unwrap() = None;
                return Ok(CloudTurnOutcome {
                    session_id: session.agent_id.clone(),
                    reply_text: reply,
                    input_tokens: 0,
                    output_tokens: 0,
                });
            }
            Some("failed" | "canceled" | "cancelled") => {
                let st = val["status"].as_str().unwrap_or("failed");
                clear_active_run(config);
                *active.lock().unwrap() = None;
                return Err(CloudErrorDetail::generic(format!("Spiron turn {st}")));
            }
            Some("submitted" | "working" | "input_required" | "queued" | "running" | "pending") => {
                cancel.wait(apply_poll_jitter(poll_interval));
                poll_interval = (poll_interval * 2).min(Duration::from_secs(5));
            }
            _ => {
                *active.lock().unwrap() = None;
                return Err(CloudErrorDetail::generic(
                    "Spiron re-attached receipt has missing or unknown status",
                ));
            }
        }
    }
}

fn workspace_context(config: &Path) -> Result<Option<Value>, String> {
    match std::fs::read(config.join(".anyagents-managed-worktree.json")) {
        Ok(bytes) => {
            let v: Value =
                serde_json::from_slice(&bytes).map_err(|_| "invalid remote workspace marker")?;
            if v.get("cloud_sandbox").and_then(Value::as_bool) != Some(true) {
                return Ok(None);
            }
            Ok(Some(
                json!({"repo": v["repo"], "branch": v["branch"], "base": v["base"]}),
            ))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err("cannot read remote workspace context".into()),
    }
}

fn github_repository(repo: &str) -> Option<String> {
    let repo = repo
        .strip_prefix("https://github.com/")
        .or_else(|| repo.strip_prefix("git@github.com:"))
        .unwrap_or(repo)
        .trim_end_matches(".git");
    let parts: Vec<_> = repo.split('/').collect();
    if parts.len() != 2
        || parts.iter().any(|s| {
            s.is_empty()
                || *s == "."
                || *s == ".."
                || !s
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        })
    {
        return None;
    }
    Some(repo.to_string())
}

fn conversation_reply(
    api: &Api,
    agent: &str,
    task: &str,
    cancel: &TurnCancel,
    deadline: Instant,
) -> Result<Option<String>, String> {
    let mut path = format!("/v1/agents/{agent}/conversation?limit=100");
    let mut cursors = std::collections::HashSet::new();
    let mut ids = std::collections::HashSet::new();
    let mut pages = Vec::new();
    loop {
        if cancel.is_cancelled() || Instant::now() >= deadline {
            return Err("Spiron conversation read interrupted".into());
        }
        let page = api.get(&path)?;
        if page.get("agent_id").and_then(Value::as_str) != Some(agent) {
            return Err("Spiron conversation identity mismatch".into());
        }
        let entries = page
            .get("data")
            .and_then(Value::as_array)
            .ok_or("invalid Spiron conversation page")?;
        let mut texts = Vec::new();
        for entry in entries {
            if entry.get("task_id").and_then(Value::as_str) != Some(task)
                || entry.get("role").and_then(Value::as_str) != Some("agent")
            {
                continue;
            }
            let id = entry
                .get("id")
                .and_then(Value::as_str)
                .ok_or("Spiron conversation item missing id")?;
            if !ids.insert(id.to_string()) {
                continue;
            }
            // Agent replies carry a typed OutputItem (user messages carry content).
            if let Some(output) = entry.get("output")
                && output.get("type").and_then(Value::as_str) == Some("text")
                && let Some(text) = output.get("text").and_then(Value::as_str)
                && !text.is_empty()
            {
                texts.push(text.to_string());
            }
        }
        pages.push(texts);
        if pages.len() >= 100 && page.get("has_more").and_then(Value::as_bool) == Some(true) {
            return Err("Spiron conversation page limit exceeded".into());
        }
        match page.get("has_more").and_then(Value::as_bool) {
            Some(false) => break,
            Some(true) => {}
            None => return Err("Spiron conversation missing has_more".into()),
        }
        let cursor = page
            .get("next_cursor")
            .and_then(Value::as_str)
            .ok_or("Spiron conversation missing cursor")?;
        if cursor.is_empty() || !cursors.insert(cursor.to_string()) {
            return Err("invalid or repeated Spiron conversation cursor".into());
        }
        let encoded: String = url::form_urlencoded::byte_serialize(cursor.as_bytes()).collect();
        path = format!("/v1/agents/{agent}/conversation?limit=100&before={encoded}");
    }
    // Each page is oldest first, but before follows pages towards older history.
    let reply = pages.into_iter().rev().flatten().collect::<Vec<_>>().join(
        "
",
    );
    Ok((!reply.is_empty()).then_some(reply))
}

#[cfg(test)]
mod tests;
