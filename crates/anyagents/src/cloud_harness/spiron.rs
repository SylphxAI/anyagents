//! Spiron's public Agent/message API. Agent identity is not an engine session or receipt.
use super::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const HARNESS: &str = "spiron-cloud";
const TURN_BUDGET: Duration = Duration::from_secs(1800);

#[derive(Serialize, Deserialize)]
struct Pending {
    scope: String,
    key: String,
    body: Value,
    receipt: Option<String>,
}

fn save(path: &Path, value: &[u8]) -> Result<(), String> {
    anyagents_harness::atomic_write_nonempty_file(path, value, 0o600)
        .map_err(|e| format!("cannot persist Spiron state: {e}"))
}

fn pending(path: &Path, scope: &str, body: Value) -> Result<Pending, String> {
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
                key: format!(
                    "{}-{}-{}-{}-{}",
                    &h[..8],
                    &h[8..12],
                    &h[12..16],
                    &h[16..20],
                    &h[20..]
                ),
                body,
                receipt: None,
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

struct Api {
    client: ureq::Agent,
    base: String,
    token: Option<String>,
}

impl Api {
    fn new(config: &Path) -> Self {
        Self {
            client: ureq::Agent::config_builder()
                .timeout_global(Some(Duration::from_secs(30)))
                .http_status_as_error(false)
                .build()
                .into(),
            base: resolve_cloud_base_url(config, HARNESS),
            token: resolve_cloud_auth_token(config, HARNESS),
        }
    }
    fn get(&self, path: &str) -> Result<Value, String> {
        let mut req = self.client.get(format!("{}{path}", self.base));
        if let Some(token) = &self.token {
            req = req.header("authorization", &format!("Bearer {token}"));
        }
        let response = req
            .call()
            .map_err(|_| "Spiron read failed; pending receipt retained".to_string())?;
        Self::decode(response)
    }
    fn post(&self, path: &str, p: &Pending) -> Result<Value, String> {
        let mut req = self
            .client
            .post(format!("{}{path}", self.base))
            .header("Idempotency-Key", &p.key)
            .header("Prefer", "respond-async");
        if let Some(token) = &self.token {
            req = req.header("authorization", &format!("Bearer {token}"));
        }
        let response = req.send_json(&p.body).map_err(|_| {
            "Spiron write acknowledgement uncertain; durable idempotency key retained".to_string()
        })?;
        Self::decode(response)
    }
    fn decode(mut response: ureq::http::Response<ureq::Body>) -> Result<Value, String> {
        let status = response.status().as_u16();
        if !(200..300).contains(&status) {
            // Provider error bodies can echo prompts, headers or MCP capabilities.
            return Err(format!("Spiron API returned HTTP {status}"));
        }
        response
            .body_mut()
            .read_json()
            .map_err(|_| "Spiron returned invalid JSON; pending state retained".to_string())
    }
}

pub(super) fn dispatch(
    config: &Path,
    model: Option<&str>,
    resume: Option<&str>,
    prompt: &str,
    cancel: &TurnCancel,
    active: &Mutex<Option<String>>,
) -> Result<CloudTurnOutcome, String> {
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(config.join("spiron-cloud.lock"))
        .map_err(|_| "cannot open Spiron turn lock")?;
    lock.try_lock()
        .map_err(|_| "another Spiron turn owns this config")?;
    let api = Api::new(config);
    // Bind persisted identities and capabilities to the endpoint that created them.
    let origin = config.join("spiron-cloud-origin.json");
    match std::fs::read(&origin) {
        Ok(bytes) => {
            let bound: String =
                serde_json::from_slice(&bytes).map_err(|_| "invalid Spiron origin state")?;
            if bound != api.base {
                return Err("Spiron identity belongs to another API origin".into());
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            save(
                &origin,
                &serde_json::to_vec(&api.base).map_err(|e| e.to_string())?,
            )?;
        }
        Err(_) => return Err("cannot read Spiron origin state".into()),
    }
    let deadline = Instant::now() + TURN_BUDGET;
    let identity_path = config.join(session_filename_for(HARNESS));
    let agent_id = if let Some(id) = resume
        .map(str::to_string)
        .or_else(|| read_persisted_session_id(config, HARNESS))
    {
        identifier(&json!({"id": id}), "id")?
    } else {
        let path = config.join("spiron-cloud-create.json");
        let mut body = json!({"name": "Remote worker", "tools": {"disabled": resolve_cloud_disabled_tools(config)}});
        if let Some(model) = model {
            body["model"] = json!(model);
        }
        if let Some(instructions) = build_cloud_system_prompt(config) {
            body["instructions"] = json!(instructions);
        }
        if let Some(context) = workspace_context(config)?
            && let Some(repo) = context
                .get("repo")
                .and_then(Value::as_str)
                .and_then(github_repository)
        {
            body["github"] = json!({"repositories": [repo]});
        }
        let request = pending(&path, &api.base, body)?;
        let created = api.post("/v1/agents", &request)?;
        let id = identifier(&created, "id")?;
        save(&identity_path, format!("{id}\n").as_bytes())?;
        std::fs::remove_file(path).map_err(|e| e.to_string())?;
        id
    };
    save(
        &identity_path,
        format!(
            "{agent_id}
"
        )
        .as_bytes(),
    )?;
    if cancel.is_cancelled() {
        return Err("cancelled before Spiron message submission".into());
    }
    let path = config.join("spiron-cloud-message.json");
    let content = if let Some(context) = workspace_context(config)? {
        format!("Remote workspace context (repository, branch, base): {context}
Prepare this checkout in your cloud workspace before repository work; never use the controller filesystem.

{prompt}")
    } else {
        prompt.to_string()
    };
    let mut body = json!({"content": content});
    // Never export the local mailbox capability. Re-read the expiring remote token each turn.
    if config.join(crate::remote::TOKEN_FILE).exists() {
        let url = resolve_public_mcp_url(config)?;
        let parsed = url::Url::parse(&url).map_err(|_| "invalid Spiron MCP callback URL")?;
        let loopback_test = cfg!(test)
            && read_cloud_json(config)
                .and_then(|v| v.get("allow_loopback_mcp").and_then(Value::as_bool))
                == Some(true)
            && parsed
                .host_str()
                .is_some_and(|h| matches!(h, "127.0.0.1" | "[::1]"));
        if (parsed.scheme() != "https" && !(parsed.scheme() == "http" && loopback_test))
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.fragment().is_some()
        {
            return Err(
                "Spiron MCP callback requires HTTPS without credentials or fragment".into(),
            );
        }
        let capability = crate::remote::session_token(config)?;
        body["mcp_servers"] = json!([{"name": "anyagents", "url": mcp_url_with_profile(&url, remote_mcp_profile(config)), "bearer_token": capability}]);
    }
    let expected_content = body["content"].clone();
    let mut request = pending(&path, &format!("{}/{}", api.base, agent_id), body)?;
    if request.body.get("content") != Some(&expected_content) {
        return Err(
            "unfinished Spiron turn retained; resume it before submitting another prompt".into(),
        );
    }
    let mut receipt = if let Some(task) = &request.receipt {
        identifier(&json!({"id": task}), "id")?;
        api.get(&format!("/v1/message-receipts/{task}"))?
    } else {
        let receipt = api.post(&format!("/v1/agents/{agent_id}/messages"), &request)?;
        request.receipt = Some(identifier(&receipt, "id")?);
        save(
            &path,
            &serde_json::to_vec(&request).map_err(|e| e.to_string())?,
        )?;
        receipt
    };
    let task = request.receipt.as_deref().ok_or("Spiron receipt missing")?;
    *active.lock().unwrap() = Some(task.to_string());
    let result = (|| {
        loop {
            if identifier(&receipt, "id")? != task
                || receipt.get("agent_id").and_then(Value::as_str) != Some(&agent_id)
            {
                return Err("Spiron receipt identity mismatch".into());
            }
            match receipt.get("status").and_then(Value::as_str) {
                Some("completed") => {
                    // The receipt is the completion authority; conversation pages are a projection.
                    let projection = conversation_reply(&api, &agent_id, task, cancel, deadline);
                    // Receipt.reply contains every joined reply; a projection may still lag.
                    let reply = match receipt
                        .get("reply")
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                    {
                        Some(reply) => reply.to_string(),
                        None => projection?
                            .filter(|s| !s.is_empty())
                            .ok_or("completed Spiron receipt has no reply")?,
                    };
                    std::fs::remove_file(&path).map_err(|e| e.to_string())?;
                    return Ok(CloudTurnOutcome {
                        session_id: agent_id.clone(),
                        reply_text: reply,
                        input_tokens: 0,
                        output_tokens: 0,
                    });
                }
                Some("failed" | "canceled") => {
                    std::fs::remove_file(&path).map_err(|e| e.to_string())?;
                    return Err(format!(
                        "Spiron turn {}",
                        receipt["status"].as_str().unwrap()
                    ));
                }
                Some("submitted" | "working" | "input_required") => {}
                _ => return Err("Spiron receipt has missing or unknown status".into()),
            }
            if cancel.is_cancelled() || Instant::now() >= deadline {
                cancel_cloud_turn(&api.base, HARNESS, api.token.as_deref(), task)?;
                return Err(
                    "Spiron cancellation requested; receipt retained for reconciliation".into(),
                );
            }
            cancel.wait(Duration::from_millis(250));
            receipt = api.get(&format!("/v1/message-receipts/{task}"))?;
        }
    })();
    *active.lock().unwrap() = None;
    result
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
