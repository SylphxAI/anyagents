//! External HTTP delayed-run and webhook [`TimerBackend`] for pure serverless
//! scale-to-zero deployments.
//!
//! Configured via vendor-neutral environment variables:
//! - `ANYAGENTS_TIMER_SCHEDULER_URL`: base endpoint for scheduling/cancelling timers.
//! - `ANYAGENTS_TIMER_SCHEDULER_TOKEN`: optional Bearer token for outbound requests.
//! - `ANYAGENTS_TIMER_SCHEDULER_MODE`: `"delayed_run"` or `"webhook"` (auto-detects
//!   `"delayed_run"` when the URL path ends with `/runs`, else `"webhook"`).
//! - `ANYAGENTS_PUBLIC_URL`: public coordinator base URL used to build
//!   `{public_url}/v1/internal/fire/{id}`.

use std::io::Read as _;
use std::path::Path;
use std::time::Duration;

use serde_json::{Value, json};
use url::Url;

use super::{TimerBackend, deterministic_run_id, format_fire_at_rfc3339_ms};

pub const ENV_TIMER_SCHEDULER_URL: &str = "ANYAGENTS_TIMER_SCHEDULER_URL";
pub const ENV_TIMER_SCHEDULER_TOKEN: &str = "ANYAGENTS_TIMER_SCHEDULER_TOKEN";
pub const ENV_TIMER_SCHEDULER_MODE: &str = "ANYAGENTS_TIMER_SCHEDULER_MODE";

const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// Wire protocol spoken with the configured external timer scheduler.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExternalSchedulerMode {
    /// Strict delayed-run protocol (`POST {url}?runId=...` with only `scheduleTime`
    /// and `input` top-level JSON fields; cancel via `POST {url}/{run_id}:cancel`).
    DelayedRun,
    /// Generic delayed-webhook protocol (`POST {url}` with `{id, run_at, callback_url, payload}`;
    /// cancel via `DELETE {url}/{run_id}`).
    Webhook,
}

impl ExternalSchedulerMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DelayedRun => "delayed_run",
            Self::Webhook => "webhook",
        }
    }

    pub fn resolve(mode_override: Option<&str>, scheduler_url: &str) -> Self {
        if let Some(raw) = mode_override.map(str::trim).filter(|s| !s.is_empty()) {
            match raw.to_ascii_lowercase().as_str() {
                "delayed_run" | "delayed-run" | "delayedrun" => return Self::DelayedRun,
                "webhook" => return Self::Webhook,
                _ => {}
            }
        }
        let path = Url::parse(scheduler_url)
            .ok()
            .map(|u| u.path().trim_end_matches('/').to_string())
            .unwrap_or_else(|| {
                scheduler_url
                    .split('?')
                    .next()
                    .unwrap_or(scheduler_url)
                    .trim_end_matches('/')
                    .to_string()
            });
        if path.ends_with("/runs") {
            Self::DelayedRun
        } else {
            Self::Webhook
        }
    }
}

/// Pure-OSS HTTP client adapter implementing [`TimerBackend`] for scale-to-zero
/// serverless coordinators.
#[derive(Clone, Debug)]
pub struct ExternalWebhookTimerBackend {
    scheduler_url: String,
    token: Option<String>,
    mode: ExternalSchedulerMode,
    public_url: String,
}

impl ExternalWebhookTimerBackend {
    pub fn new(
        scheduler_url: impl Into<String>,
        token: Option<String>,
        mode: ExternalSchedulerMode,
        public_url: impl Into<String>,
    ) -> Self {
        Self {
            scheduler_url: scheduler_url
                .into()
                .trim()
                .trim_end_matches('/')
                .to_string(),
            token: token
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty()),
            mode,
            public_url: public_url.into().trim().trim_end_matches('/').to_string(),
        }
    }

    /// Construct from neutral `ANYAGENTS_TIMER_SCHEDULER_*` environment variables
    /// when `ANYAGENTS_TIMER_SCHEDULER_URL` is configured.
    pub fn from_env(data_dir: &Path) -> Option<Self> {
        let scheduler_url = env_non_empty(ENV_TIMER_SCHEDULER_URL)?;
        let token = env_non_empty(ENV_TIMER_SCHEDULER_TOKEN);
        let mode_env = env_non_empty(ENV_TIMER_SCHEDULER_MODE);
        let mode = ExternalSchedulerMode::resolve(mode_env.as_deref(), &scheduler_url);
        let public_url =
            crate::server::serverless::resolve_public_url(data_dir).unwrap_or_default();
        Some(Self::new(scheduler_url, token, mode, public_url))
    }

    pub fn mode(&self) -> ExternalSchedulerMode {
        self.mode
    }

    /// Extract `host[:port]` from the configured scheduler URL for safe startup
    /// diagnostic logging without exposing paths, query strings, or credentials.
    pub fn scheduler_host(&self) -> String {
        Url::parse(&self.scheduler_url)
            .ok()
            .and_then(|u| {
                u.host_str().map(|host| match u.port() {
                    Some(port) => format!("{host}:{port}"),
                    None => host.to_string(),
                })
            })
            .unwrap_or_else(|| "<invalid-url>".to_string())
    }

    fn fire_url(&self, schedule_id: &str) -> Result<String, String> {
        if self.public_url.is_empty() {
            return Err(
                "ANYAGENTS_PUBLIC_URL is required when using ExternalWebhookTimerBackend"
                    .to_string(),
            );
        }
        Ok(format!(
            "{}/v1/internal/fire/{schedule_id}",
            self.public_url
        ))
    }

    fn http_agent() -> ureq::Agent {
        ureq::Agent::config_builder()
            .timeout_global(Some(HTTP_TIMEOUT))
            .http_status_as_error(false)
            .build()
            .into()
    }

    fn build_delayed_run_schedule_url(&self, run_id: &str) -> Result<String, String> {
        let mut parsed = Url::parse(&self.scheduler_url)
            .map_err(|err| format!("invalid scheduler URL `{}`: {err}", self.scheduler_url))?;
        parsed.query_pairs_mut().append_pair("runId", run_id);
        Ok(parsed.to_string())
    }

    fn build_cancel_url(&self, run_id: &str, suffix: &str) -> Result<String, String> {
        let mut parsed = Url::parse(&self.scheduler_url)
            .map_err(|err| format!("invalid scheduler URL `{}`: {err}", self.scheduler_url))?;
        let base_path = parsed.path().trim_end_matches('/');
        parsed.set_path(&format!("{base_path}/{run_id}{suffix}"));
        parsed.set_query(None);
        Ok(parsed.to_string())
    }
}

impl TimerBackend for ExternalWebhookTimerBackend {
    fn backend_kind(&self) -> &'static str {
        "external_webhook"
    }

    fn schedule_one_shot(
        &self,
        schedule_id: &str,
        fire_at_ms: i64,
    ) -> Result<Option<String>, String> {
        let clean_id = schedule_id.trim();
        if clean_id.is_empty() {
            return Err("schedule_id cannot be empty".to_string());
        }
        let run_id = deterministic_run_id(clean_id, fire_at_ms);
        let rfc3339 = format_fire_at_rfc3339_ms(fire_at_ms)?;
        let fire_url = self.fire_url(clean_id)?;

        let (target_url, payload) = match self.mode {
            ExternalSchedulerMode::DelayedRun => (
                self.build_delayed_run_schedule_url(&run_id)?,
                json!({
                    "scheduleTime": rfc3339,
                    "input": {
                        "schedule_id": clean_id,
                        "fire_url": fire_url,
                        "intended_at_ms": fire_at_ms,
                    }
                }),
            ),
            ExternalSchedulerMode::Webhook => (
                self.scheduler_url.clone(),
                json!({
                    "id": run_id,
                    "run_at": rfc3339,
                    "callback_url": fire_url,
                    "payload": {
                        "schedule_id": clean_id,
                        "intended_at_ms": fire_at_ms,
                    }
                }),
            ),
        };

        let body_bytes =
            serde_json::to_vec(&payload).map_err(|err| format!("serialize timer body: {err}"))?;
        let agent = Self::http_agent();
        let mut req = agent
            .post(&target_url)
            .header("content-type", "application/json")
            .header("accept", "application/json");
        if let Some(ref tok) = self.token {
            req = req.header("authorization", &format!("Bearer {tok}"));
        }
        let mut resp = req
            .send(&body_bytes[..])
            .map_err(|err| format!("timer schedule POST failed ({target_url}): {err}"))?;
        let status = resp.status().as_u16();
        let mut raw = Vec::new();
        let _ = resp
            .body_mut()
            .as_reader()
            .take(64 * 1024)
            .read_to_end(&mut raw);

        if !(200..300).contains(&status) && status != 409 {
            let snippet = String::from_utf8_lossy(&raw);
            return Err(format!(
                "timer scheduler returned HTTP {status} for {clean_id}: {}",
                snippet.trim()
            ));
        }

        let resolved_id = serde_json::from_slice::<Value>(&raw)
            .ok()
            .and_then(|v| {
                v.get("runId")
                    .or_else(|| v.get("run_id"))
                    .or_else(|| v.get("id"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
            })
            .unwrap_or(run_id);

        Ok(Some(resolved_id))
    }

    fn cancel(&self, _schedule_id: &str, external_run_id: Option<&str>) -> Result<(), String> {
        let Some(run_id) = external_run_id.map(str::trim).filter(|s| !s.is_empty()) else {
            return Ok(());
        };
        let agent = Self::http_agent();
        let (target_url, resp) = match self.mode {
            ExternalSchedulerMode::DelayedRun => {
                let url = self.build_cancel_url(run_id, ":cancel")?;
                let mut req = agent.post(&url).header("accept", "application/json");
                if let Some(ref tok) = self.token {
                    req = req.header("authorization", &format!("Bearer {tok}"));
                }
                let res = req
                    .send_empty()
                    .map_err(|err| format!("timer cancel POST failed ({url}): {err}"))?;
                (url, res)
            }
            ExternalSchedulerMode::Webhook => {
                let url = self.build_cancel_url(run_id, "")?;
                let mut req = agent.delete(&url).header("accept", "application/json");
                if let Some(ref tok) = self.token {
                    req = req.header("authorization", &format!("Bearer {tok}"));
                }
                let res = req
                    .call()
                    .map_err(|err| format!("timer cancel DELETE failed ({url}): {err}"))?;
                (url, res)
            }
        };
        let status = resp.status().as_u16();
        if (200..300).contains(&status) || status == 404 || status == 409 {
            return Ok(());
        }
        Err(format!(
            "timer cancel returned HTTP {status} ({target_url})"
        ))
    }
}

fn env_non_empty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::Router;
    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::{HeaderMap, Method, StatusCode, Uri};
    use axum::routing::any;

    use super::*;

    #[derive(Clone, Debug)]
    struct RecordedReq {
        method: String,
        path_and_query: String,
        auth: Option<String>,
        body: Option<Value>,
    }

    #[derive(Clone, Default)]
    struct MockSchedulerState {
        requests: Arc<Mutex<Vec<RecordedReq>>>,
        next_status: Arc<Mutex<StatusCode>>,
    }

    async fn mock_handler(
        State(state): State<MockSchedulerState>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: Bytes,
    ) -> (StatusCode, String) {
        let auth = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let parsed_body = if body.is_empty() {
            None
        } else {
            serde_json::from_slice::<Value>(&body).ok()
        };
        state.requests.lock().unwrap().push(RecordedReq {
            method: method.to_string(),
            path_and_query: uri
                .path_and_query()
                .map(|pq| pq.as_str().to_string())
                .unwrap_or_default(),
            auth,
            body: parsed_body,
        });
        let status = *state.next_status.lock().unwrap();
        let reply = if status.is_success() {
            r#"{"ok":true}"#.to_string()
        } else {
            r#"{"error":"conflict_or_missing"}"#.to_string()
        };
        (status, reply)
    }

    async fn spawn_mock_scheduler() -> (String, MockSchedulerState) {
        let state = MockSchedulerState {
            requests: Arc::new(Mutex::new(Vec::new())),
            next_status: Arc::new(Mutex::new(StatusCode::OK)),
        };
        let app = Router::new()
            .fallback(any(mock_handler))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}"), state)
    }

    #[test]
    fn mode_auto_detects_delayed_run_for_runs_path_and_webhook_otherwise() {
        assert_eq!(
            ExternalSchedulerMode::resolve(None, "https://sched.example.test/v1/workflows/w1/runs"),
            ExternalSchedulerMode::DelayedRun
        );
        assert_eq!(
            ExternalSchedulerMode::resolve(None, "https://sched.example.test/v1/timers"),
            ExternalSchedulerMode::Webhook
        );
        assert_eq!(
            ExternalSchedulerMode::resolve(
                Some("delayed_run"),
                "https://sched.example.test/custom"
            ),
            ExternalSchedulerMode::DelayedRun
        );
        assert_eq!(
            ExternalSchedulerMode::resolve(
                Some("webhook"),
                "https://sched.example.test/v1/workflows/w1/runs"
            ),
            ExternalSchedulerMode::Webhook
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delayed_run_mode_uses_strict_schedule_time_input_and_colon_cancel() {
        let (base, state) = spawn_mock_scheduler().await;
        let url = format!("{base}/v1/workflows/timer-dispatch/runs");
        let mode = ExternalSchedulerMode::resolve(None, &url);
        assert_eq!(mode, ExternalSchedulerMode::DelayedRun);

        let backend = ExternalWebhookTimerBackend::new(
            url,
            Some("sched-token-123".into()),
            mode,
            "https://coord.example.test/",
        );
        let fire_at_ms = 1_763_635_511_250i64;
        let b_clone = backend.clone();
        let ext_id = tokio::task::spawn_blocking(move || {
            b_clone.schedule_one_shot("sch_abc123", fire_at_ms)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(ext_id.as_deref(), Some("sch_abc123-1763635511250"));

        // Cancel returns 409 Conflict -> treated as idempotent success.
        *state.next_status.lock().unwrap() = StatusCode::CONFLICT;
        let b_cancel = backend.clone();
        let ext_clone = ext_id.clone();
        tokio::task::spawn_blocking(move || b_cancel.cancel("sch_abc123", ext_clone.as_deref()))
            .await
            .unwrap()
            .unwrap();

        // Cancel returns 404 Not Found -> also idempotent success.
        *state.next_status.lock().unwrap() = StatusCode::NOT_FOUND;
        tokio::task::spawn_blocking(move || backend.cancel("sch_abc123", ext_id.as_deref()))
            .await
            .unwrap()
            .unwrap();

        let reqs = state.requests.lock().unwrap().clone();
        assert_eq!(reqs.len(), 3);

        // 1. Schedule request: strict top-level keys (`scheduleTime`, `input` ONLY).
        assert_eq!(reqs[0].method, "POST");
        assert_eq!(
            reqs[0].path_and_query,
            "/v1/workflows/timer-dispatch/runs?runId=sch_abc123-1763635511250"
        );
        assert_eq!(reqs[0].auth.as_deref(), Some("Bearer sched-token-123"));
        let body = reqs[0].body.as_ref().unwrap();
        let top_keys: Vec<&String> = body.as_object().unwrap().keys().collect();
        assert_eq!(top_keys.len(), 2, "unexpected extra keys: {top_keys:?}");
        assert_eq!(body["scheduleTime"], "2025-11-20T10:45:11.250Z");
        assert_eq!(body["input"]["schedule_id"], "sch_abc123");
        assert_eq!(
            body["input"]["fire_url"],
            "https://coord.example.test/v1/internal/fire/sch_abc123"
        );
        assert_eq!(body["input"]["intended_at_ms"], fire_at_ms);

        // 2 & 3. Cancel requests: POST /{runId}:cancel.
        assert_eq!(reqs[1].method, "POST");
        assert_eq!(
            reqs[1].path_and_query,
            "/v1/workflows/timer-dispatch/runs/sch_abc123-1763635511250:cancel"
        );
        assert_eq!(reqs[1].auth.as_deref(), Some("Bearer sched-token-123"));
        assert_eq!(reqs[2].method, "POST");
        assert_eq!(
            reqs[2].path_and_query,
            "/v1/workflows/timer-dispatch/runs/sch_abc123-1763635511250:cancel"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn webhook_mode_posts_callback_payload_and_deletes_on_cancel() {
        let (base, state) = spawn_mock_scheduler().await;
        let url = format!("{base}/v1/timers");
        let mode = ExternalSchedulerMode::resolve(None, &url);
        assert_eq!(mode, ExternalSchedulerMode::Webhook);

        let backend = ExternalWebhookTimerBackend::new(
            url,
            Some("wh-token-999".into()),
            mode,
            "https://coord.example.test",
        );
        let fire_at_ms = 1_763_635_511_250i64;
        let b_clone = backend.clone();
        let ext_id =
            tokio::task::spawn_blocking(move || b_clone.schedule_one_shot("sch_wh1", fire_at_ms))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(ext_id.as_deref(), Some("sch_wh1-1763635511250"));

        *state.next_status.lock().unwrap() = StatusCode::NOT_FOUND;
        tokio::task::spawn_blocking(move || backend.cancel("sch_wh1", ext_id.as_deref()))
            .await
            .unwrap()
            .unwrap();

        let reqs = state.requests.lock().unwrap().clone();
        assert_eq!(reqs.len(), 2);
        assert_eq!(reqs[0].method, "POST");
        assert_eq!(reqs[0].path_and_query, "/v1/timers");
        assert_eq!(reqs[0].auth.as_deref(), Some("Bearer wh-token-999"));
        let body = reqs[0].body.as_ref().unwrap();
        assert_eq!(body["id"], "sch_wh1-1763635511250");
        assert_eq!(body["run_at"], "2025-11-20T10:45:11.250Z");
        assert_eq!(
            body["callback_url"],
            "https://coord.example.test/v1/internal/fire/sch_wh1"
        );
        assert_eq!(body["payload"]["schedule_id"], "sch_wh1");
        assert_eq!(body["payload"]["intended_at_ms"], fire_at_ms);

        assert_eq!(reqs[1].method, "DELETE");
        assert_eq!(reqs[1].path_and_query, "/v1/timers/sch_wh1-1763635511250");
        assert_eq!(reqs[1].auth.as_deref(), Some("Bearer wh-token-999"));
    }
}
