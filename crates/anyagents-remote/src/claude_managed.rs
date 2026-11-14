//! Claude Managed Agents (beta `managed-agents-2026-04-01`) as a remote-session
//! adapter, over the documented REST API; there is no Rust SDK.
//!
//! * The vendor agent definition (model, system prompt, tools and the
//!   AnyAgents coordination MCP server) and its environment are created once
//!   per AnyAgents agent, each recorded as soon as it exists, and the
//!   definition is updated in place, as a new version, when its model or
//!   prompt change.
//! * Each session gets its own vault holding the remote session token as a
//!   `static_bearer` credential for the coordination MCP URL. The token never
//!   enters the session's container: the vendor's MCP proxy attaches it at
//!   egress (ADR 0010 D4 tier 1). Rotation updates the credential in place.
//! * Every session is created with a hard dollar budget: what is left of the
//!   agent's cap. At it the vendor pauses the session; raising it resumes.
//! * A running session queues `user.message` events and takes them in order.
//!   A message counts as delivered only once the session processed it (its
//!   echo carries `processed_at`); `user.interrupt` stops the running turn.
//! * Events stream over SSE with no replay. On every reconnect the stream is
//!   opened first, then the history newer than the cursor is read newest
//!   first, stopping at the cursor, and replayed oldest first; the stream
//!   follows with what the history gave dropped by event id.
//! * Every request of every agent on one API key goes through one
//!   [`RequestBudget`], and a `retry-after` pauses all of them.

use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use anyagents_harness::{
    Delivery, DeliveryLevel, TurnUsage, message_id_in_text, names_message_size,
};
use futures::StreamExt;
use serde_json::{Value, json};

use crate::pacing::RequestBudget;
use crate::{
    Adapter, AdapterError, Admission, Budgeting, Capabilities, Coordination, Event, EventKind,
    Events, Fut, Grant, Handle, Snapshot, Spec, Stop, VendorStatus,
};

/// The adapter's name.
pub const ADAPTER: &str = "claude-managed";
pub const DEFAULT_BASE_URL: &str = "https://api.anthropic.com";
/// The model a session runs when the agent names none.
pub const DEFAULT_MODEL: &str = "claude-opus-5-5";
const API_VERSION: &str = "2026-10-21";
const BETA: &str = "managed-agents-2026-04-01";
/// The name the coordination MCP server has in the vendor agent definition.
const MCP_SERVER: &str = "anyagents";
/// One page of session history.
const PAGE: usize = 1000;
/// The most history pages one lookup reads, newest first, before it gives up.
const MAX_PAGES: usize = 50;
/// A control call that has not answered by then is a lost answer.
const CALL_TIMEOUT: Duration = Duration::from_secs(60);
/// The event stream carries heartbeats; this long without a byte is a dead
/// connection.
const STREAM_SILENCE: Duration = Duration::from_secs(120);
/// How long every caller of the account waits after a 429 or 529 that named
/// no `retry-after`.
const DEFAULT_PAUSE: Duration = Duration::from_secs(1);

/// What Claude Managed Agents guarantees for mail (`docs/delivery.md`).
pub const DELIVERY: Delivery = Delivery {
    level: DeliveryLevel::NextStep,
    idempotent: true,
    proof: Cow::Borrowed(
        "the session processed the user.message: its echo on the event stream or in the history \
         carries processed_at; a running session queues it and takes it in order; a message an \
         interrupted turn never processed is offered again; a lost answer is settled from the \
         session's history, by the message's marker, before anything is written again",
    ),
    peer: None,
};

/// The Claude Managed Agents adapter.
pub struct ClaudeManaged {
    http: reqwest::Client,
    base: String,
    key: String,
    /// An environment to run every session in, instead of one per agent.
    environment: Option<String>,
    /// The request budget of the account the key belongs to.
    pacer: Arc<RequestBudget>,
}

impl std::fmt::Debug for ClaudeManaged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClaudeManaged")
            .field("base", &self.base)
            .field("key", &"<redacted>")
            .field("environment", &self.environment)
            .finish()
    }
}

impl ClaudeManaged {
    /// An adapter calling `base_url` (the Anthropic API when `None`) with
    /// `api_key`, which comes from the agent's bound API-key profile. Every
    /// adapter on the same key shares one request budget.
    pub fn new(api_key: &str, base_url: Option<&str>) -> Result<Self, String> {
        let key = api_key.trim();
        if key.is_empty() || key.contains(char::is_whitespace) {
            return Err("the Anthropic API key is empty or not one line".into());
        }
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(STREAM_SILENCE)
            .build()
            .map_err(|err| err.to_string())?;
        let base = base_url
            .unwrap_or(DEFAULT_BASE_URL)
            .trim_end_matches('/')
            .to_string();
        let account = format!("{ADAPTER}:{base}:{}", fingerprint(&json!(key)));
        Ok(Self {
            http,
            pacer: RequestBudget::shared(&account),
            base,
            key: key.to_string(),
            environment: None,
        })
    }

    /// Run every session in this environment instead of one per agent.
    pub fn with_environment(mut self, environment: Option<String>) -> Self {
        self.environment = environment.filter(|id| !id.trim().is_empty());
        self
    }

    /// Use `pacer` instead of the account's shared request budget.
    pub fn with_request_budget(mut self, pacer: Arc<RequestBudget>) -> Self {
        self.pacer = pacer;
        self
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.http
            .request(method, format!("{}{path}", self.base))
            .header("x-api-key", &self.key)
            .header("anthropic-version", API_VERSION)
            .header("anthropic-beta", BETA)
    }

    /// The vendor asked to slow down: every caller of the account does.
    fn throttled(&self, status: u16, retry_after_ms: Option<u64>) {
        if matches!(status, 429 | 529) {
            self.pacer.pause_for(
                retry_after_ms
                    .map(Duration::from_millis)
                    .unwrap_or(DEFAULT_PAUSE),
            );
        }
    }

    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, Failure> {
        self.pacer.acquire().await;
        let mut request = self.request(method, path).timeout(CALL_TIMEOUT);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.map_err(Failure::transport)?;
        let status = response.status();
        let retry_after_ms = retry_after(response.headers());
        let text = response.text().await.map_err(Failure::transport)?;
        if status.is_success() {
            if text.trim().is_empty() {
                return Ok(Value::Null);
            }
            return serde_json::from_str(&text).map_err(|err| Failure {
                status: Some(status.as_u16()),
                sent: true,
                retry_after_ms: None,
                message: format!("unreadable answer from {path}: {err}"),
            });
        }
        self.throttled(status.as_u16(), retry_after_ms);
        Err(Failure {
            status: Some(status.as_u16()),
            sent: true,
            retry_after_ms,
            message: format!("{path}: HTTP {}: {}", status.as_u16(), error_message(&text)),
        })
    }

    async fn post(&self, path: &str, body: Value) -> Result<Value, AdapterError> {
        self.call(reqwest::Method::POST, path, Some(body))
            .await
            .map_err(Failure::into_error)
    }

    async fn get(&self, path: &str) -> Result<Value, AdapterError> {
        self.call(reqwest::Method::GET, path, None)
            .await
            .map_err(Failure::into_error)
    }

    async fn environment_for(&self, spec: &Spec, state: &Value) -> Result<String, AdapterError> {
        if let Some(id) = &self.environment {
            return Ok(id.clone());
        }
        if let Some(id) = state.get("environment_id").and_then(Value::as_str) {
            return Ok(id.to_string());
        }
        let created = self
            .post(
                "/v1/environments",
                json!({
                    "name": format!("anyagents-{}", slug(&spec.agent_name)),
                    "config": {
                        "type": "cloud",
                        "networking": {
                            "type": "limited",
                            "allow_package_managers": true,
                            "allow_mcp_servers": true,
                        },
                    },
                }),
            )
            .await?;
        required_id(&created, "environment")
    }

    async fn agent_for(
        &self,
        spec: &Spec,
        state: &Value,
    ) -> Result<(String, i64, String), AdapterError> {
        let definition = definition(spec);
        let fingerprint = fingerprint(&definition);
        if let Some(id) = state.get("agent_id").and_then(Value::as_str) {
            let version = state.get("agent_version").and_then(Value::as_i64);
            let same = state.get("definition").and_then(Value::as_str) == Some(&fingerprint);
            if same && let Some(version) = version {
                return Ok((id.to_string(), version, fingerprint));
            }
            // Same agent, changed model or prompt: a new version in place.
            let updated = self.post(&format!("/v1/agents/{id}"), definition).await?;
            let version = updated.get("version").and_then(Value::as_i64).unwrap_or(1);
            return Ok((id.to_string(), version, fingerprint));
        }
        let created = self.post("/v1/agents", definition).await?;
        let id = required_id(&created, "agent")?;
        let version = created.get("version").and_then(Value::as_i64).unwrap_or(1);
        Ok((id, version, fingerprint))
    }

    async fn create_session(
        &self,
        spec: &Spec,
        previous: &Value,
        checkpoint: &(dyn Fn(&Value) + Send + Sync),
    ) -> Result<Handle, AdapterError> {
        let grant = spec.grant.as_ref().ok_or_else(|| {
            AdapterError::Other("a Managed Agents session needs the coordination MCP grant".into())
        })?;
        // Each vendor-side object is recorded the moment it exists, so a
        // create that fails later is retried with it, not with a new one.
        let mut state = match previous {
            Value::Object(map) => Value::Object(map.clone()),
            _ => json!({}),
        };
        let environment = self.environment_for(spec, &state).await?;
        if self.environment.is_none() && state.get("environment_id").is_none() {
            state["environment_id"] = json!(environment);
            checkpoint(&state);
        }
        let (agent_id, agent_version, definition) = self.agent_for(spec, &state).await?;
        if state.get("agent_id").and_then(Value::as_str) != Some(agent_id.as_str())
            || state.get("agent_version").and_then(Value::as_i64) != Some(agent_version)
            || state.get("definition").and_then(Value::as_str) != Some(definition.as_str())
        {
            state["agent_id"] = json!(agent_id);
            state["agent_version"] = json!(agent_version);
            state["definition"] = json!(definition);
            checkpoint(&state);
        }
        let vault = self
            .post(
                "/v1/vaults",
                json!({ "display_name": format!("anyagents {}", spec.agent_name) }),
            )
            .await?;
        let vault_id = required_id(&vault, "vault")?;
        let made = async {
            let credential = self
                .post(
                    &format!("/v1/vaults/{vault_id}/credentials"),
                    json!({
                        "display_name": "AnyAgents coordination MCP",
                        "auth": {
                            "type": "static_bearer",
                            "mcp_server_url": grant.mcp_url,
                            "token": grant.token,
                        },
                    }),
                )
                .await?;
            let credential_id = required_id(&credential, "credential")?;
            let mut body = json!({
                "agent": { "type": "agent", "id": agent_id, "version": agent_version },
                "environment_id": environment,
                "vault_ids": [vault_id],
                "title": format!("anyagents {}", spec.agent_name),
                "metadata": { "anyagents_agent": spec.agent_id },
            });
            if let Some(cents) = spec.budget_cents {
                body["budget"] = budget(cents);
            }
            let session = self.post("/v1/sessions", body).await?;
            Ok::<_, AdapterError>((required_id(&session, "session")?, credential_id))
        }
        .await;
        let (session, credential_id) = match made {
            Ok(made) => made,
            Err(err) => {
                // Nothing will use the vault: purge the token it holds.
                let _ = self
                    .post(&format!("/v1/vaults/{vault_id}/archive"), json!({}))
                    .await;
                return Err(err);
            }
        };
        state["vault_id"] = json!(vault_id);
        state["credential_id"] = json!(credential_id);
        if self.environment.is_some() {
            state["environment_id"] = json!(environment);
        }
        Ok(Handle { session, state })
    }

    /// The message text carries its mailbox id as its first line
    /// (`anyagents-message:<id>`), which is how [`Self::session_holds`] finds
    /// it again; the vendor keeps no request id of its own.
    async fn send(&self, handle: &Handle, text: &str) -> Result<Admission, AdapterError> {
        let path = format!("/v1/sessions/{}/events", handle.session);
        let body = json!({
            "events": [{ "type": "user.message", "content": [{ "type": "text", "text": text }] }],
        });
        let failure = match self.call(reqwest::Method::POST, &path, Some(body)).await {
            Ok(_) => return Ok(Admission::InFlight),
            Err(failure) => failure,
        };
        let wait = |failure: Failure| {
            Ok(Admission::Wait {
                reason: failure.message,
                retry_after_ms: failure.retry_after_ms,
            })
        };
        match failure.status {
            // Never reached the vendor: not applied.
            None if !failure.sent => wait(failure),
            // Throttled before anything was applied.
            Some(429) | Some(529) => wait(failure),
            Some(413) => Ok(Admission::Refused {
                reason: failure.message,
                message_specific: true,
            }),
            Some(400) | Some(422) => {
                let lower = failure.message.to_ascii_lowercase();
                // At the budget cap, or while a tool decision is pending,
                // the session takes only settle events: not now, later.
                if [
                    "budget",
                    "settle",
                    "requires_action",
                    "only accepts",
                    "paused",
                ]
                .iter()
                .any(|word| lower.contains(word))
                {
                    wait(failure)
                } else {
                    let message_specific = names_message_size(&failure.message);
                    Ok(Admission::Refused {
                        reason: failure.message,
                        message_specific,
                    })
                }
            }
            Some(401) | Some(403) => Err(AdapterError::Auth(failure.message)),
            Some(404) | Some(410) => Err(AdapterError::Gone(failure.message)),
            // A lost answer, a timeout, a server error after the request
            // arrived, or a success whose body cannot be read: the vendor may
            // hold the message. The session's history settles it.
            _ => Ok(Admission::Unknown(failure.message)),
        }
    }

    /// The live SSE stream, as vendor events.
    async fn open_stream(
        &self,
        handle: &Handle,
    ) -> Result<
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<Value, AdapterError>> + Send>>,
        AdapterError,
    > {
        self.pacer.acquire().await;
        let path = format!("/v1/sessions/{}/events/stream", handle.session);
        let response = self
            .request(reqwest::Method::GET, &path)
            .header("accept", "text/event-stream")
            .send()
            .await
            .map_err(|err| Failure::transport(err).into_error())?;
        let status = response.status();
        if !status.is_success() {
            let retry_after_ms = retry_after(response.headers());
            self.throttled(status.as_u16(), retry_after_ms);
            let text = response.text().await.unwrap_or_default();
            return Err(Failure {
                status: Some(status.as_u16()),
                sent: true,
                retry_after_ms,
                message: format!("{path}: HTTP {}: {}", status.as_u16(), error_message(&text)),
            }
            .into_error());
        }
        let bytes = response.bytes_stream();
        let stream = futures::stream::unfold(
            (
                bytes,
                crate::sse::Decoder::default(),
                VecDeque::<Value>::new(),
            ),
            |(mut bytes, mut decoder, mut ready)| async move {
                loop {
                    if let Some(value) = ready.pop_front() {
                        return Some((Ok(value), (bytes, decoder, ready)));
                    }
                    match bytes.next().await {
                        None => return None,
                        Some(Err(err)) => {
                            return Some((
                                Err(AdapterError::Transient(format!("event stream: {err}"))),
                                (bytes, decoder, ready),
                            ));
                        }
                        Some(Ok(chunk)) => {
                            for data in decoder.feed(&chunk) {
                                if let Ok(value) = serde_json::from_str::<Value>(&data)
                                    && value.get("type").is_some()
                                {
                                    ready.push_back(value);
                                }
                            }
                        }
                    }
                }
            },
        );
        Ok(Box::pin(stream))
    }

    /// Attach: the stream is opened first and buffers; the history after
    /// `after` is read newest first, stopping at the cursor, and replayed
    /// oldest first (all of it, read oldest first, when there is no cursor);
    /// the stream follows with every event the history already gave dropped
    /// by id. An input's processed echo is the same event id as its queued
    /// one, and passes once.
    async fn attach_events(
        &self,
        handle: &Handle,
        after: Option<&str>,
    ) -> Result<Events, AdapterError> {
        let live = self.open_stream(handle).await?;
        let replayed = match after {
            None => self.read_history_oldest_first(handle).await?,
            Some(cursor) => {
                let (mut newer, found) = self
                    .read_history_newest_first(handle, |event| {
                        event.get("id").and_then(Value::as_str) == Some(cursor)
                    })
                    .await?;
                if found.is_some() {
                    newer.reverse();
                    newer
                } else {
                    // The history no longer holds the cursor: replaying what
                    // was read would count it twice, so nothing before now is
                    // replayed.
                    Vec::new()
                }
            }
        };
        // What the stream may show again: what the replay gave, with whether
        // an input was processed when it did.
        let mut seen: HashMap<String, bool> = replayed
            .iter()
            .filter_map(|event| {
                let id = event.get("id").and_then(Value::as_str)?;
                (!id.is_empty()).then(|| (id.to_string(), processed(event)))
            })
            .collect();
        let mut replay: Vec<Result<Event, AdapterError>> =
            replayed.iter().map(|value| Ok(map_event(value))).collect();
        replay.push(Ok(Event {
            cursor: String::new(),
            at_ms: None,
            kind: EventKind::CaughtUp,
        }));
        let live = live.filter_map(move |item| {
            let out = match item {
                Err(err) => Some(Err(err)),
                Ok(value) => {
                    let id = value.get("id").and_then(Value::as_str).unwrap_or("");
                    let now_processed = processed(&value);
                    let pass = id.is_empty()
                        || match seen.get(id) {
                            None => true,
                            // Queued before, processed now: the proof.
                            Some(false) => now_processed,
                            Some(true) => false,
                        };
                    if pass && !id.is_empty() {
                        seen.insert(id.to_string(), now_processed);
                    }
                    pass.then(|| Ok(map_event(&value)))
                }
            };
            std::future::ready(out)
        });
        Ok(Box::pin(futures::stream::iter(replay).chain(live)))
    }

    fn history_path(handle: &Handle, order: &str, page: Option<&str>) -> String {
        let mut path = format!(
            "/v1/sessions/{}/events?limit={PAGE}&order={order}",
            handle.session
        );
        if let Some(cursor) = page {
            path.push_str("&page=");
            path.push_str(&encode(cursor));
        }
        path
    }

    /// The whole history, oldest first.
    async fn read_history_oldest_first(&self, handle: &Handle) -> Result<Vec<Value>, AdapterError> {
        let mut events = Vec::new();
        let mut page: Option<String> = None;
        loop {
            let answer = self
                .get(&Self::history_path(handle, "asc", page.as_deref()))
                .await?;
            if let Some(data) = answer.get("data").and_then(Value::as_array) {
                events.extend(data.iter().cloned());
            }
            match answer.get("next_page").and_then(Value::as_str) {
                Some(next) if !next.is_empty() && page.as_deref() != Some(next) => {
                    page = Some(next.to_string());
                }
                _ => return Ok(events),
            }
        }
    }

    /// The history newest first, up to (not including) the first event
    /// `stop` matches, and that event when one did. Reads at most
    /// [`MAX_PAGES`] pages.
    async fn read_history_newest_first(
        &self,
        handle: &Handle,
        stop: impl Fn(&Value) -> bool,
    ) -> Result<(Vec<Value>, Option<Value>), AdapterError> {
        let mut events = Vec::new();
        let mut page: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let answer = self
                .get(&Self::history_path(handle, "desc", page.as_deref()))
                .await?;
            for event in answer
                .get("data")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if stop(event) {
                    return Ok((events, Some(event.clone())));
                }
                events.push(event.clone());
            }
            match answer.get("next_page").and_then(Value::as_str) {
                Some(next) if !next.is_empty() && page.as_deref() != Some(next) => {
                    page = Some(next.to_string());
                }
                _ => return Ok((events, None)),
            }
        }
        Err(AdapterError::Transient(format!(
            "the session's history is longer than {MAX_PAGES} pages past the point sought"
        )))
    }

    async fn read_snapshot(&self, handle: &Handle) -> Result<Snapshot, AdapterError> {
        let session = self
            .get(&format!("/v1/sessions/{}", handle.session))
            .await?;
        let status = match session.get("status").and_then(Value::as_str) {
            Some("running") => VendorStatus::Running,
            Some("rescheduling") => VendorStatus::Rescheduling,
            Some("terminated") => VendorStatus::Terminated,
            _ => VendorStatus::Idle,
        };
        Ok(Snapshot {
            status,
            cost_micros: cents(session.pointer("/usage/list_cost/amount")).map(micros),
            budget_cents: cents(session.pointer("/budget/max_list_cost/amount")),
        })
    }

    async fn end(&self, handle: &Handle) -> Result<(), AdapterError> {
        let _ = self.interrupt_session(handle).await;
        // The session reads as running for a moment after it went idle, and
        // refuses to be archived until then.
        let path = format!("/v1/sessions/{}/archive", handle.session);
        let mut archived = Err(AdapterError::Other("not tried".into()));
        for _ in 0..10 {
            archived = self
                .call(reqwest::Method::POST, &path, Some(json!({})))
                .await
                .map(|_| ())
                .map_err(Failure::into_error);
            match &archived {
                Ok(()) | Err(AdapterError::Gone(_)) => break,
                Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        }
        // The vault holds the session's token: archiving purges it.
        if let Some(vault) = handle.state.get("vault_id").and_then(Value::as_str) {
            match self
                .post(&format!("/v1/vaults/{vault}/archive"), json!({}))
                .await
            {
                Ok(_) | Err(AdapterError::Gone(_)) => {}
                Err(err) => return Err(err),
            }
        }
        match archived {
            Ok(()) | Err(AdapterError::Gone(_)) => Ok(()),
            Err(err) => Err(err),
        }
    }

    async fn interrupt_session(&self, handle: &Handle) -> Result<(), AdapterError> {
        let path = format!("/v1/sessions/{}/events", handle.session);
        match self
            .call(
                reqwest::Method::POST,
                &path,
                Some(json!({ "events": [{ "type": "user.interrupt" }] })),
            )
            .await
        {
            Ok(_) => Ok(()),
            // Nothing to stop.
            Err(Failure {
                status: Some(400), ..
            }) => Ok(()),
            Err(failure) => Err(failure.into_error()),
        }
    }

    /// Whether an input with this marker lies after the cursor: the history
    /// is read newest first and stops at the marker or at the cursor.
    async fn session_holds(
        &self,
        handle: &Handle,
        request_id: &str,
        after: Option<&str>,
    ) -> Result<bool, AdapterError> {
        let is_marker = |event: &Value| {
            matches!(
                map_event(event).kind,
                EventKind::Input { request_id: Some(ref id), .. } if id == request_id
            )
        };
        let is_cursor =
            |event: &Value| after.is_some() && event.get("id").and_then(Value::as_str) == after;
        let (_, stopped) = self
            .read_history_newest_first(handle, |event| is_marker(event) || is_cursor(event))
            .await?;
        Ok(stopped.is_some_and(|event| is_marker(&event)))
    }

    async fn custom_tool_result(
        &self,
        handle: &Handle,
        call_id: &str,
        output: &str,
        is_error: bool,
    ) -> Result<(), AdapterError> {
        self.post(
            &format!("/v1/sessions/{}/events", handle.session),
            json!({ "events": [{
                "type": "user.custom_tool_result",
                "custom_tool_use_id": call_id,
                "content": [{ "type": "text", "text": output }],
                "is_error": is_error,
            }] }),
        )
        .await
        .map(|_| ())
    }

    async fn rotate(&self, handle: &Handle, grant: &Grant) -> Result<(), AdapterError> {
        let vault = state_id(handle, "vault_id")?;
        let credential = state_id(handle, "credential_id")?;
        self.post(
            &format!("/v1/vaults/{vault}/credentials/{credential}"),
            json!({ "auth": { "type": "static_bearer", "token": grant.token } }),
        )
        .await
        .map(|_| ())
    }
}

impl Adapter for ClaudeManaged {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            adapter: ADAPTER,
            delivery: DELIVERY,
            interrupt: true,
            budget: Budgeting::Vendor,
            metered: true,
            coordination: Coordination::Mcp,
        }
    }

    fn create<'a>(
        &'a self,
        spec: &'a Spec,
        previous: &'a Value,
        checkpoint: &'a (dyn Fn(&Value) + Send + Sync),
    ) -> Fut<'a, Handle> {
        Box::pin(self.create_session(spec, previous, checkpoint))
    }

    fn attach<'a>(&'a self, handle: &'a Handle, after: Option<&'a str>) -> Fut<'a, Events> {
        Box::pin(self.attach_events(handle, after))
    }

    fn deliver<'a>(
        &'a self,
        handle: &'a Handle,
        _request_id: &'a str,
        text: &'a str,
    ) -> Fut<'a, Admission> {
        Box::pin(self.send(handle, text))
    }

    fn holds<'a>(
        &'a self,
        handle: &'a Handle,
        request_id: &'a str,
        after: Option<&'a str>,
    ) -> Fut<'a, bool> {
        Box::pin(self.session_holds(handle, request_id, after))
    }

    fn snapshot<'a>(&'a self, handle: &'a Handle) -> Fut<'a, Snapshot> {
        Box::pin(self.read_snapshot(handle))
    }

    fn interrupt<'a>(&'a self, handle: &'a Handle) -> Fut<'a, ()> {
        Box::pin(self.interrupt_session(handle))
    }

    fn stop<'a>(&'a self, handle: &'a Handle) -> Fut<'a, ()> {
        Box::pin(self.end(handle))
    }

    fn set_budget<'a>(&'a self, handle: &'a Handle, cents: i64) -> Fut<'a, ()> {
        Box::pin(async move {
            self.post(
                &format!("/v1/sessions/{}", handle.session),
                json!({ "budget": budget(cents) }),
            )
            .await
            .map(|_| ())
        })
    }

    fn grant<'a>(&'a self, handle: &'a Handle, grant: &'a Grant) -> Fut<'a, ()> {
        Box::pin(self.rotate(handle, grant))
    }

    fn tool_result<'a>(
        &'a self,
        handle: &'a Handle,
        call_id: &'a str,
        output: &'a str,
        is_error: bool,
    ) -> Fut<'a, ()> {
        Box::pin(self.custom_tool_result(handle, call_id, output, is_error))
    }
}

/// The vendor agent definition: what a session of this agent runs.
fn definition(spec: &Spec) -> Value {
    let model = spec
        .model
        .as_deref()
        .map(str::trim)
        .map(|model| model.strip_prefix("anthropic/").unwrap_or(model))
        .filter(|model| !model.is_empty())
        .unwrap_or(DEFAULT_MODEL);
    let mut definition = json!({
        "name": format!("anyagents-{}", slug(&spec.agent_name)),
        "model": model,
        "mcp_servers": [{
            "type": "url",
            "name": MCP_SERVER,
            "url": spec.grant.as_ref().map(|grant| grant.mcp_url.as_str()).unwrap_or_default(),
        }],
        // No one is at a console to approve a call: every tool runs. The
        // container is the session's own; the coordination tools are the
        // agent's own mailbox.
        "tools": [
            {
                "type": "agent_toolset_20260401",
                "default_config": { "permission_policy": { "type": "always_allow" } },
            },
            {
                "type": "mcp_toolset",
                "mcp_server_name": MCP_SERVER,
                "default_config": { "permission_policy": { "type": "always_allow" } },
            },
        ],
        "metadata": { "anyagents_agent": spec.agent_id },
    });
    if let Some(system) = spec.system.as_deref().filter(|s| !s.trim().is_empty()) {
        definition["system"] = json!(system);
    }
    definition
}

/// A stable digest of a definition, to tell whether it changed.
fn fingerprint(definition: &Value) -> String {
    let text = definition.to_string();
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in text.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    format!("{hash:016x}")
}

fn budget(cents: i64) -> Value {
    json!({
        "type": "limit",
        "max_list_cost": { "amount": cents.max(1).to_string(), "currency": "USD" },
    })
}

/// US cents in millionths of a dollar.
fn micros(cents: i64) -> i64 {
    cents.saturating_mul(10_000)
}

fn cents(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::String(text) => text.trim().parse().ok(),
        Value::Number(number) => number.as_i64(),
        _ => None,
    }
}

fn slug(name: &str) -> String {
    let slug: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .take(64)
        .collect();
    if slug.is_empty() {
        "agent".into()
    } else {
        slug
    }
}

fn encode(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn required_id(value: &Value, what: &str) -> Result<String, AdapterError> {
    value
        .get("id")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| AdapterError::Other(format!("the vendor created a {what} without an id")))
}

fn state_id<'a>(handle: &'a Handle, key: &str) -> Result<&'a str, AdapterError> {
    handle
        .state
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| AdapterError::Other(format!("the session's record names no {key}")))
}

fn error_message(body: &str) -> String {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|value| {
            value
                .pointer("/error/message")
                .or_else(|| value.get("message"))
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| body.chars().take(300).collect())
}

/// A failed HTTP call: the status when the vendor answered, and whether the
/// request may have reached it.
struct Failure {
    status: Option<u16>,
    sent: bool,
    /// What the vendor's `retry-after` asked for.
    retry_after_ms: Option<u64>,
    message: String,
}

impl Failure {
    fn transport(err: reqwest::Error) -> Self {
        Self {
            status: None,
            // A request that never connected was not applied.
            sent: !err.is_connect() && !err.is_builder(),
            retry_after_ms: None,
            message: format!("Anthropic API: {err}"),
        }
    }

    fn into_error(self) -> AdapterError {
        match self.status {
            Some(401) | Some(403) => AdapterError::Auth(self.message),
            Some(404) | Some(410) => AdapterError::Gone(self.message),
            Some(408) | Some(409) | Some(429) | None => AdapterError::Transient(self.message),
            Some(status) if status >= 500 => AdapterError::Transient(self.message),
            Some(_) => AdapterError::Other(self.message),
        }
    }
}

/// A `retry-after` header in seconds, as milliseconds.
fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    let seconds: f64 = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    (seconds.is_finite() && seconds >= 0.0).then(|| (seconds * 1000.0).min(3_600_000.0) as u64)
}

/// Whether a vendor event is an input the session already processed.
fn processed(event: &Value) -> bool {
    event.get("processed_at").is_some_and(|at| !at.is_null())
}

fn text_of(content: Option<&Value>) -> String {
    match content {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(blocks)) => blocks
            .iter()
            .filter_map(|block| match block {
                Value::String(text) => Some(text.clone()),
                Value::Object(_) => block
                    .get("text")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .or_else(|| {
                        block
                            .get("content")
                            .map(|inner| text_of(Some(inner)))
                            .filter(|text| !text.is_empty())
                    }),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n"),
        Some(Value::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

fn time_ms(value: Option<&Value>) -> Option<i64> {
    let text = value?.as_str()?;
    chrono::DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|time| time.timestamp_millis())
}

fn usage_of(value: Option<&Value>) -> TurnUsage {
    let read = |key: &str| {
        value
            .and_then(|usage| usage.get(key))
            .and_then(Value::as_u64)
            .unwrap_or(0)
    };
    TurnUsage {
        input_tokens: read("input_tokens"),
        output_tokens: read("output_tokens"),
        cache_read_tokens: read("cache_read_input_tokens"),
        cache_write_tokens: read("cache_creation_input_tokens"),
        reported_cost_micros: None,
    }
}

/// One Managed Agents event, as the executor reads it.
pub fn map_event(value: &Value) -> Event {
    let kind_name = value.get("type").and_then(Value::as_str).unwrap_or("");
    let cursor = value
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let at_ms = time_ms(value.get("processed_at")).or_else(|| time_ms(value.get("created_at")));
    // Status events are documented both with and without their namespace.
    let bare = kind_name.strip_prefix("session.").unwrap_or(kind_name);
    let kind = match (kind_name, bare) {
        ("user.message", _) => {
            let text = text_of(value.get("content"));
            EventKind::Input {
                request_id: message_id_in_text(&text).map(str::to_string),
                text,
                consumed: value.get("processed_at").is_some_and(|at| !at.is_null()),
            }
        }
        ("agent.message", _) => EventKind::Text(text_of(value.get("content"))),
        ("agent.custom_tool_use", _) => EventKind::HostToolCall {
            // `user.custom_tool_result` answers the event, by its id.
            call_id: cursor.clone(),
            name: value
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            input: value.get("input").cloned().unwrap_or(Value::Null),
        },
        ("agent.tool_use" | "agent.mcp_tool_use", _) => EventKind::ToolUse {
            name: value
                .get("name")
                .or_else(|| value.get("tool_name"))
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            input: value.get("input").cloned().unwrap_or(Value::Null),
        },
        ("agent.tool_result" | "agent.mcp_tool_result", _) => EventKind::ToolResult {
            text: text_of(value.get("content")),
            is_error: value
                .get("is_error")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        },
        ("span.model_request_end", _) => EventKind::Usage(usage_of(value.get("model_usage"))),
        (_, "usage") => match cents(value.pointer("/list_cost/amount")) {
            Some(cents) => EventKind::CostTotal {
                micros: micros(cents),
            },
            None => EventKind::Other(kind_name.to_string()),
        },
        (_, "status_running") => EventKind::TurnStarted,
        (_, "status_idle") => EventKind::TurnEnded(
            match value.pointer("/stop_reason/type").and_then(Value::as_str) {
                Some("requires_action") => Stop::RequiresAction,
                Some("budget_reached") => Stop::BudgetReached,
                Some("end_turn") | None => Stop::EndTurn,
                Some(other) => Stop::Failed(other.to_string()),
            },
        ),
        (_, "status_terminated") => EventKind::Terminated(
            value
                .pointer("/error/message")
                .or_else(|| value.get("reason"))
                .and_then(Value::as_str)
                .map(str::to_string),
        ),
        (_, "error") => EventKind::Error(
            value
                .pointer("/error/message")
                .or_else(|| value.get("message"))
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| value.to_string()),
        ),
        _ => EventKind::Other(kind_name.to_string()),
    };
    Event {
        cursor,
        at_ms,
        kind,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vendor_events_map_to_the_executors_kinds() {
        let ev = map_event(&json!({
            "type": "user.message", "id": "sevt_1", "processed_at": null,
            "content": [{"type": "text", "text": "anyagents-message:msg_1\nhi"}],
        }));
        assert_eq!(ev.cursor, "sevt_1");
        assert_eq!(
            ev.kind,
            EventKind::Input {
                request_id: Some("msg_1".into()),
                text: "anyagents-message:msg_1\nhi".into(),
                consumed: false
            }
        );
        let ev = map_event(&json!({
            "type": "span.model_request_end", "id": "sevt_2",
            "processed_at": "2026-04-07T04:11:32.189Z",
            "model_usage": {"input_tokens": 3, "output_tokens": 4,
                            "cache_read_input_tokens": 5, "cache_creation_input_tokens": 6},
        }));
        assert_eq!(ev.at_ms, Some(1_775_535_092_189));
        assert_eq!(
            ev.kind,
            EventKind::Usage(TurnUsage {
                input_tokens: 3,
                output_tokens: 4,
                cache_read_tokens: 5,
                cache_write_tokens: 6,
                reported_cost_micros: None
            })
        );
        for (stop, want) in [
            ("end_turn", Stop::EndTurn),
            ("budget_reached", Stop::BudgetReached),
            ("requires_action", Stop::RequiresAction),
            (
                "retries_exhausted",
                Stop::Failed("retries_exhausted".into()),
            ),
        ] {
            for kind in ["session.status_idle", "status_idle"] {
                let ev =
                    map_event(&json!({"type": kind, "id": "x", "stop_reason": {"type": stop}}));
                assert_eq!(ev.kind, EventKind::TurnEnded(want.clone()), "{kind} {stop}");
            }
        }
        let ev = map_event(&json!({
            "type": "session.usage", "id": "u", "list_cost": {"amount": "2513", "currency": "USD"},
        }));
        assert_eq!(ev.kind, EventKind::CostTotal { micros: 25_130_000 });
        let ev = map_event(&json!({
            "type": "agent.custom_tool_use", "id": "sevt_9", "name": "t", "input": {"a": 1},
        }));
        assert_eq!(
            ev.kind,
            EventKind::HostToolCall {
                call_id: "sevt_9".into(),
                name: "t".into(),
                input: json!({"a": 1})
            }
        );
        assert_eq!(
            map_event(&json!({"type": "agent.thinking", "id": "t"})).kind,
            EventKind::Other("agent.thinking".into())
        );
    }

    #[test]
    fn a_changed_model_or_prompt_changes_the_fingerprint_and_the_key_never_shows() {
        let spec = |model: &str| Spec {
            agent_id: "agt_1".into(),
            agent_name: "a b".into(),
            model: Some(model.into()),
            system: Some("be brief".into()),
            grant: Some(Grant {
                mcp_url: "https://x.example/v1/webhooks/mcp".into(),
                token: "aars_secret".into(),
            }),
            host_tools: Vec::new(),
            budget_cents: Some(100),
        };
        let one = definition(&spec("anthropic/claude-opus-5-5"));
        assert_eq!(one["model"], "claude-opus-5-5");
        assert_eq!(one["name"], "anyagents-a-b");
        assert!(!one.to_string().contains("aars_secret"));
        assert_ne!(
            fingerprint(&one),
            fingerprint(&definition(&spec("claude-sonnet-5-5")))
        );
        let adapter = ClaudeManaged::new("sk-ant-key", None).unwrap();
        assert!(!format!("{adapter:?}").contains("sk-ant-key"));
        assert!(ClaudeManaged::new("two words", None).is_err());
    }
}
