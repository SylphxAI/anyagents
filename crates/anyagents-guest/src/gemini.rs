//! Embedded guest adapter for Gemini CLI (`gemini`).
//!
//! Executes turns via `gemini -p <prompt> --output-format stream-json` (passing
//! `--model` and `--resume` when configured), streaming JSON lines or plain text
//! output into `GuestHarness` state with usage and session ID capture.

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use anyagents_harness::{
    DeliverOutcome, Delivery, DeliveryLevel, Harness, HarnessError, HarnessHealth, OutboundMessage,
    Proof, TurnState, TurnUsage, spawn_command,
};
use serde_json::Value;

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

#[derive(Default)]
struct GeminiState {
    session_id: Option<String>,
    admitted: HashMap<String, bool>,
    finished: HashMap<String, bool>,
    answers: HashMap<String, String>,
    failures: HashMap<String, String>,
    usage_by_id: HashMap<String, TurnUsage>,
    total_usage: TurnUsage,
    in_turn: bool,
    turns_served: u32,
    active_child: Option<Child>,
    watchers: Vec<Arc<dyn Fn() + Send + Sync>>,
}

impl GeminiState {
    fn notify(&self) {
        for cb in &self.watchers {
            cb();
        }
    }
}

pub(crate) struct GeminiGuestHarness {
    program: PathBuf,
    model: Option<String>,
    extra_args: Vec<String>,
    environment: Vec<(String, String)>,
    work_dir: PathBuf,
    shared: Arc<Mutex<GeminiState>>,
    worker: Option<JoinHandle<()>>,
}

impl GeminiGuestHarness {
    pub(crate) fn start(
        program: impl Into<PathBuf>,
        model: Option<&str>,
        session_id: Option<&str>,
        extra_args: &[String],
        environment: &[(&str, &str)],
        work_dir: &Path,
    ) -> Result<Self, HarnessError> {
        let program = program.into();
        if !program.is_file() {
            return Err(HarnessError::Failed(format!(
                "gemini program not found at {}",
                program.display()
            )));
        }
        let shared = Arc::new(Mutex::new(GeminiState {
            session_id: session_id
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
            ..GeminiState::default()
        }));
        Ok(Self {
            program,
            model: model
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
            extra_args: extra_args.to_vec(),
            environment: environment
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
            work_dir: work_dir.to_path_buf(),
            shared,
            worker: None,
        })
    }

    pub(crate) fn session_id(&self) -> Option<String> {
        lock(&self.shared).session_id.clone()
    }

    pub(crate) fn answer(&self, id: &str) -> Option<String> {
        lock(&self.shared).answers.get(id).cloned()
    }

    pub(crate) fn usage_for(&self, id: &str) -> Option<TurnUsage> {
        lock(&self.shared).usage_by_id.get(id).cloned()
    }

    pub(crate) fn failure_for(&self, id: &str) -> Option<String> {
        lock(&self.shared).failures.get(id).cloned()
    }
}

impl Harness for GeminiGuestHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let (session_id, msg_id) = {
            let mut st = lock(&self.shared);
            if st.finished.contains_key(&message.id) {
                return Ok(DeliverOutcome::Accepted {
                    turn_finished: true,
                });
            }
            if st.admitted.contains_key(&message.id) {
                return Ok(DeliverOutcome::InFlight);
            }
            if st.in_turn {
                return Ok(DeliverOutcome::Deferred);
            }
            st.in_turn = true;
            st.admitted.insert(message.id.clone(), false);
            (st.session_id.clone(), message.id.clone())
        };

        let mut args = vec![
            "-p".to_string(),
            message.text.clone(),
            "--output-format".to_string(),
            "stream-json".to_string(),
        ];
        if let Some(ref model) = self.model {
            args.push("--model".to_string());
            args.push(model.clone());
        }
        if let Some(ref sid) = session_id {
            args.push("--resume".to_string());
            args.push(sid.clone());
        }
        args.extend(self.extra_args.iter().cloned());

        let mut cmd = spawn_command(&self.program, &self.environment)
            .map_err(|e| HarnessError::Failed(format!("spawn gemini: {e}")))?;
        cmd.args(&args)
            .current_dir(&self.work_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                let mut st = lock(&self.shared);
                st.in_turn = false;
                st.failures
                    .insert(msg_id, format!("failed to spawn gemini: {e}"));
                st.notify();
                return Err(HarnessError::Failed(format!("spawn gemini: {e}")));
            }
        };
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        {
            let mut st = lock(&self.shared);
            st.active_child = Some(child);
            st.notify();
        }

        let shared = Arc::clone(&self.shared);
        let worker = thread::Builder::new()
            .name("anyagents-guest-gemini".into())
            .spawn(move || {
                let mut accumulated = String::new();
                let mut turn_usage = TurnUsage::default();
                let mut seen_error: Option<String> = None;

                if let Some(out) = stdout {
                    let reader = BufReader::new(out);
                    for line in reader.lines().map_while(Result::ok) {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        if let Ok(val) = serde_json::from_str::<Value>(trimmed) {
                            if let Some(sid) = val
                                .get("session_id")
                                .and_then(Value::as_str)
                                .map(str::trim)
                                .filter(|s| !s.is_empty())
                            {
                                lock(&shared).session_id = Some(sid.to_string());
                            }
                            let ty = val.get("type").and_then(Value::as_str).unwrap_or("");
                            match ty {
                                "message" | "assistant" => {
                                    let role = val
                                        .get("role")
                                        .or_else(|| val.pointer("/message/role"))
                                        .and_then(Value::as_str)
                                        .unwrap_or("assistant");
                                    if role == "assistant" {
                                        if let Some(content) =
                                            val.get("content").and_then(Value::as_str)
                                        {
                                            accumulated.push_str(content);
                                        } else if let Some(arr) = val
                                            .pointer("/message/content")
                                            .and_then(Value::as_array)
                                        {
                                            for item in arr {
                                                if let Some(t) =
                                                    item.get("text").and_then(Value::as_str)
                                                {
                                                    accumulated.push_str(t);
                                                }
                                            }
                                        }
                                        if !accumulated.is_empty() {
                                            let mut st = lock(&shared);
                                            st.answers.insert(msg_id.clone(), accumulated.clone());
                                            st.notify();
                                        }
                                    }
                                }
                                "result" => {
                                    if val.get("is_error").and_then(Value::as_bool) == Some(true)
                                        || val.get("status").and_then(Value::as_str)
                                            == Some("error")
                                    {
                                        let msg = val
                                            .get("result")
                                            .or_else(|| val.get("error"))
                                            .and_then(Value::as_str)
                                            .unwrap_or("gemini turn failed");
                                        seen_error = Some(msg.to_string());
                                    } else if let Some(r) = val
                                        .get("result")
                                        .or_else(|| val.get("response"))
                                        .and_then(Value::as_str)
                                        && !r.trim().is_empty()
                                    {
                                        accumulated = r.to_string();
                                    }
                                    if let Some(u) = val.get("usage").or_else(|| val.get("stats")) {
                                        turn_usage.input_tokens = u
                                            .get("input_tokens")
                                            .or_else(|| u.get("prompt_tokens"))
                                            .and_then(Value::as_u64)
                                            .unwrap_or(turn_usage.input_tokens);
                                        turn_usage.output_tokens = u
                                            .get("output_tokens")
                                            .or_else(|| u.get("candidates_tokens"))
                                            .and_then(Value::as_u64)
                                            .unwrap_or(turn_usage.output_tokens);
                                        turn_usage.cache_read_tokens = u
                                            .get("cache_read_input_tokens")
                                            .or_else(|| u.get("cached"))
                                            .and_then(Value::as_u64)
                                            .unwrap_or(turn_usage.cache_read_tokens);
                                    }
                                }
                                "error" => {
                                    let msg = val
                                        .get("message")
                                        .or_else(|| val.get("error"))
                                        .and_then(Value::as_str)
                                        .unwrap_or("gemini stream error");
                                    seen_error = Some(msg.to_string());
                                }
                                _ => {}
                            }
                        } else {
                            if !accumulated.is_empty() {
                                accumulated.push('\n');
                            }
                            accumulated.push_str(trimmed);
                            let mut st = lock(&shared);
                            st.answers.insert(msg_id.clone(), accumulated.clone());
                            st.notify();
                        }
                    }
                }

                let mut stderr_text = String::new();
                if let Some(mut err_pipe) = stderr {
                    use std::io::Read as _;
                    let _ = err_pipe.read_to_string(&mut stderr_text);
                }

                let exit_ok = {
                    let child_opt = lock(&shared).active_child.take();
                    match child_opt {
                        Some(mut c) => c.wait().map(|s| s.success()).unwrap_or(false),
                        None => false,
                    }
                };

                let mut st = lock(&shared);
                st.in_turn = false;
                st.turns_served = st.turns_served.saturating_add(1);
                st.total_usage.input_tokens = st
                    .total_usage
                    .input_tokens
                    .saturating_add(turn_usage.input_tokens);
                st.total_usage.output_tokens = st
                    .total_usage
                    .output_tokens
                    .saturating_add(turn_usage.output_tokens);
                st.total_usage.cache_read_tokens = st
                    .total_usage
                    .cache_read_tokens
                    .saturating_add(turn_usage.cache_read_tokens);
                st.usage_by_id.insert(msg_id.clone(), turn_usage);

                if let Some(err) = seen_error {
                    st.failures.insert(msg_id.clone(), err);
                } else if !exit_ok && accumulated.trim().is_empty() {
                    let diag = stderr_text.trim();
                    let err = if diag.is_empty() {
                        "gemini exited with non-zero status".to_string()
                    } else {
                        diag.to_string()
                    };
                    st.failures.insert(msg_id.clone(), err);
                } else {
                    if !accumulated.trim().is_empty() {
                        st.answers.insert(msg_id.clone(), accumulated);
                    }
                    st.finished.insert(msg_id, true);
                }
                st.notify();
            })
            .map_err(|e| HarnessError::Failed(e.to_string()))?;
        self.worker = Some(worker);

        Ok(DeliverOutcome::Accepted {
            turn_finished: false,
        })
    }

    fn proven(&self, message_id: &str) -> Option<Proof> {
        let st = lock(&self.shared);
        if st.finished.contains_key(message_id) {
            return Some(Proof::Taken {
                turn_finished: true,
            });
        }
        st.admitted.get(message_id).map(|_| Proof::Taken {
            turn_finished: false,
        })
    }

    fn stop(&mut self) {
        let child = lock(&self.shared).active_child.take();
        if let Some(mut c) = child {
            anyagents_harness::kill_child(&mut c, &self.environment);
            let _ = c.wait();
        }
        if let Some(handle) = self.worker.take() {
            let _ = handle.join();
        }
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        Ok(())
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        lock(&self.shared).session_id = Some(session.to_string());
        Ok(())
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        let child = lock(&self.shared).active_child.take();
        if let Some(mut c) = child {
            anyagents_harness::kill_child(&mut c, &self.environment);
            let _ = c.wait();
        }
        Ok(())
    }

    fn health(&self) -> HarnessHealth {
        let st = lock(&self.shared);
        HarnessHealth {
            process_alive: true,
            turn_state: if st.in_turn {
                TurnState::InTurn
            } else {
                TurnState::Idle
            },
            delivery: Delivery {
                level: DeliveryLevel::TurnEnd,
                idempotent: false,
                proof: std::borrow::Cow::Borrowed("gemini stream-json process exit"),
                peer: Some("gemini".to_string()),
            },
            quota_reset_at_ms: None,
            sign_in_fault: None,
        }
    }

    fn served_usage(&self) -> TurnUsage {
        lock(&self.shared).total_usage.clone()
    }

    fn turns_served(&self) -> u32 {
        lock(&self.shared).turns_served
    }

    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        lock(&self.shared).watchers.push(notify);
    }
}
