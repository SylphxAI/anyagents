//! ADR 0010 D7 Remote Session Port and in-process `Harness` bridge.
//!
//! Defines the single vendor-neutral remote-session contract required by
//! ADR 0010 D6/D7 (`create`, `deliver`/`steer`, `events`, `interrupt`, `stop`,
//! `budget`) plus [`RemoteSessionHarness`], which adapts any
//! [`RemoteSessionProvider`] directly into [`crate::Harness`] without spawning
//! a local OS child process, keeper, or stdio shim pipe.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    DeliverOutcome, Delivery, DeliveryLevel, Harness, HarnessError, HarnessHealth, OutboundMessage,
    Proof, TurnState, TurnUsage, compose_message,
};

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Capability level declared by a remote-session provider adapter (ADR 0010 D7).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteCapabilities {
    /// Canonical provider identifier (e.g. `"spiron-cloud"`, `"claude-managed"`).
    pub provider: String,
    /// Delivery guarantee and proof signal (`turn-end`, `mid-turn`, or `next-step`).
    pub delivery: Delivery,
    /// Whether the provider accepts steering input into an active turn.
    pub supports_steer: bool,
    /// Whether the provider supports remote cancellation/interrupt of an active run.
    pub supports_interrupt: bool,
    /// Whether `poll_events` can resume cleanly from a cursor (`Last-Event-ID` or `seq`).
    pub supports_event_resume: bool,
    /// Whether the provider supports host tools over the session transport (ADR 0011 D7).
    pub supports_host_tools: bool,
    /// Whether completed turns report `cost_micros`.
    pub supports_cost_reporting: bool,
    /// Whether spend budget enforcement is active for this session.
    pub supports_budget_cap: bool,
}

/// Hard spend cap tracked in millionths of a US dollar (`1_000_000` = US$1.00).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteBudget {
    /// Configured hard cap in micro-USD.
    pub cap_micros: u64,
    /// Cumulative observed spend in micro-USD for this session.
    pub spent_micros: u64,
}

impl RemoteBudget {
    pub const fn new(cap_micros: u64) -> Self {
        Self {
            cap_micros,
            spent_micros: 0,
        }
    }

    pub const fn remaining_micros(self) -> u64 {
        self.cap_micros.saturating_sub(self.spent_micros)
    }

    pub const fn is_exceeded(self) -> bool {
        self.cap_micros > 0 && self.spent_micros >= self.cap_micros
    }

    /// Accumulate reported turn spend and return `true` if the cap is now reached/exceeded.
    pub fn record_spend(&mut self, cost_micros: Option<u64>) -> bool {
        if let Some(cost) = cost_micros {
            self.spent_micros = self.spent_micros.saturating_add(cost);
        }
        self.is_exceeded()
    }
}

/// Parameters passed to [`RemoteSessionProvider::create`].
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RemoteSessionConfig {
    pub agent_id: String,
    pub agent_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    /// Coordination-only MCP callback URL (`<public_url>/v1/webhooks/mcp`, ADR 0010 D2/D7).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_url: Option<String>,
    /// Short-lived per-session remote capability token (never logged or echoed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_token: Option<String>,
    /// Existing provider-side session or remote agent ID to resume.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_session_id: Option<String>,
    /// Optional hard budget cap in micro-USD.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_cap_micros: Option<u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, Value>,
}

/// Handle returned by [`RemoteSessionProvider::create`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteSessionHandle {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_agent_id: Option<String>,
    pub capabilities: RemoteCapabilities,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// One mailbox message handed to [`RemoteSessionProvider::deliver`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteDeliveryInput {
    /// Mailbox message ID used as idempotency key (`Idempotency-Key` / `request_id`).
    pub request_id: String,
    /// Full composed mailbox message envelope.
    pub text: String,
    /// Whether a turn is currently open (steer into running turn when supported).
    pub steer: bool,
    /// Fresh coordination MCP URL for this turn, if available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_url: Option<String>,
    /// Fresh rotated remote session token for this turn, if available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_token: Option<String>,
}

/// Provider proof that a remote input was durably consumed by the remote session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteConsumptionProof {
    pub session_id: String,
    pub entry_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_entry_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recorded_at: Option<String>,
}

/// Immediate acknowledgement returned by [`RemoteSessionProvider::deliver`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RemoteDeliveryAck {
    /// Input accepted and remote run started or already proved consumed.
    Accepted {
        remote_run_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        consumed_proof: Option<RemoteConsumptionProof>,
        #[serde(default)]
        turn_finished: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage: Option<TurnUsage>,
    },
    /// Provider confirmed this `request_id` was already consumed earlier (idempotent replay).
    Duplicate { remote_run_id: String },
    /// Input accepted into the remote run queue while awaiting consumption proof.
    InFlight { remote_run_id: String },
    /// Provider cannot accept another input until the active turn ends.
    Deferred,
    /// Provider asked the caller to wait until `reset_at_ms`.
    Quota { reset_at_ms: i64 },
    /// Permanent refusal for this input or session.
    Rejected {
        code: String,
        reason: String,
        message_specific: bool,
    },
    /// Hard budget cap already reached before submitting this input.
    BudgetExceeded { spent_micros: u64, cap_micros: u64 },
}

/// Resumable event emitted by [`RemoteSessionProvider::poll_events`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RemoteEvent {
    /// Provider durably proved `request_id` entered the remote session.
    InputConsumed {
        request_id: String,
        remote_run_id: String,
        proof: RemoteConsumptionProof,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
    },
    /// Incremental assistant text chunk.
    TextDelta {
        remote_run_id: String,
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
    },
    /// Remote turn finished cleanly.
    Completed {
        request_id: String,
        remote_run_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reply_text: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage: Option<TurnUsage>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
    },
    /// Remote turn failed or hit a rate/auth fault.
    Failed {
        request_id: String,
        remote_run_id: String,
        code: String,
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<u16>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        quota_reset_at_ms: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
    },
    /// Remote turn ended due to cancellation/interrupt.
    Cancelled {
        request_id: String,
        remote_run_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
    },
    /// Remote session crossed its configured spend cap.
    BudgetExceeded { spent_micros: u64, cap_micros: u64 },
}

impl RemoteEvent {
    pub fn cursor(&self) -> Option<&str> {
        match self {
            Self::InputConsumed { cursor, .. }
            | Self::TextDelta { cursor, .. }
            | Self::Completed { cursor, .. }
            | Self::Failed { cursor, .. }
            | Self::Cancelled { cursor, .. } => cursor.as_deref(),
            Self::BudgetExceeded { .. } => None,
        }
    }
}

/// Errors returned by [`RemoteSessionProvider`] operations.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RemoteSessionError {
    #[error("remote session authentication failed: {0}")]
    Auth(String),
    #[error("remote session rate limited until {reset_at_ms}: {message}")]
    Quota { reset_at_ms: i64, message: String },
    #[error("remote session budget cap reached ({spent_micros}/{cap_micros} micro-USD)")]
    BudgetExceeded { spent_micros: u64, cap_micros: u64 },
    #[error("remote session transport error: {0}")]
    Transport(String),
    #[error("remote session protocol error: {0}")]
    Protocol(String),
    #[error("remote session is closed")]
    Closed,
}

/// ADR 0010 D7 provider-neutral remote-session contract (`create`,
/// `deliver`/`steer`, `events`, `interrupt`, `stop`, `budget`).
pub trait RemoteSessionProvider: Send + 'static {
    /// Declared capability profile for this provider adapter.
    fn capabilities(&self) -> RemoteCapabilities;

    /// Create or attach to the remote session (`create`).
    fn create(
        &mut self,
        config: &RemoteSessionConfig,
    ) -> Result<RemoteSessionHandle, RemoteSessionError>;

    /// Deliver or steer one idempotent mailbox input (`deliver` / `steer`).
    fn deliver(
        &mut self,
        input: &RemoteDeliveryInput,
    ) -> Result<RemoteDeliveryAck, RemoteSessionError>;

    /// Drain new resumable remote events after `after_cursor` (`events`).
    fn poll_events(
        &mut self,
        after_cursor: Option<&str>,
    ) -> Result<Vec<RemoteEvent>, RemoteSessionError>;

    /// Request remote cancellation of an active turn (`interrupt`).
    fn interrupt(&mut self, remote_run_id: Option<&str>) -> Result<(), RemoteSessionError>;

    /// End the remote session cleanly and release resources (`stop`).
    fn stop(&mut self) -> Result<(), RemoteSessionError>;

    /// Current spend budget state (`budget`), when a cap is configured.
    fn budget(&self) -> Option<RemoteBudget>;

    /// Update or clear the session's hard spend cap in micro-USD.
    fn set_budget_cap(&mut self, cap_micros: Option<u64>);

    /// Optional hook to refresh callback MCP URL & rotated token before each turn.
    fn refresh_mcp_credentials(&self) -> Option<(String, String)> {
        None
    }
}

struct BridgeState<A: RemoteSessionProvider> {
    adapter: A,
    alive: bool,
    stopping: bool,
    session_id: Option<String>,
    remote_agent_id: Option<String>,
    capabilities: RemoteCapabilities,
    cursor: Option<String>,
    /// Active remote runs (`request_id -> remote_run_id`).
    active_runs: HashMap<String, String>,
    /// Durable consumption proofs for `Harness::proven`.
    proofs: HashMap<String, Proof>,
    /// Final assistant replies keyed by opener `request_id`.
    replies: HashMap<String, String>,
    /// Runs whose `Completed` / `Failed` / `Cancelled` terminal frame arrived.
    finished: HashSet<String>,
    usage: TurnUsage,
    turns_served: u32,
    quota_reset_at_ms: Option<i64>,
    sign_in_fault: Option<String>,
    exit_diag: Option<String>,
    budget_fault: Option<String>,
    watchers: Vec<Arc<dyn Fn() + Send + Sync>>,
}

impl<A: RemoteSessionProvider> BridgeState<A> {
    fn notify_watchers(&self) {
        for watcher in &self.watchers {
            watcher();
        }
    }

    fn apply_usage(&mut self, usage: &TurnUsage) {
        self.usage.input_tokens = self.usage.input_tokens.saturating_add(usage.input_tokens);
        self.usage.output_tokens = self.usage.output_tokens.saturating_add(usage.output_tokens);
        self.usage.cache_read_tokens = self
            .usage
            .cache_read_tokens
            .saturating_add(usage.cache_read_tokens);
        self.usage.cache_write_tokens = self
            .usage
            .cache_write_tokens
            .saturating_add(usage.cache_write_tokens);
        if let Some(cost) = usage.reported_cost_micros {
            let next = self
                .usage
                .reported_cost_micros
                .unwrap_or(0)
                .saturating_add(cost.max(0));
            self.usage.reported_cost_micros = Some(next);
        }
    }

    fn apply_event(&mut self, event: RemoteEvent) {
        if let Some(cur) = event.cursor() {
            self.cursor = Some(cur.to_string());
        }
        match event {
            RemoteEvent::InputConsumed {
                request_id, proof, ..
            } => {
                if !proof.session_id.is_empty() {
                    self.session_id = Some(proof.session_id);
                }
                let turn_finished = self.finished.contains(&request_id);
                self.proofs
                    .entry(request_id)
                    .or_insert(Proof::Taken { turn_finished });
            }
            RemoteEvent::TextDelta { .. } => {}
            RemoteEvent::Completed {
                request_id,
                session_id,
                reply_text,
                usage,
                ..
            } => {
                if let Some(sid) = session_id.filter(|s| !s.trim().is_empty()) {
                    self.session_id = Some(sid);
                }
                if let Some(text) = reply_text {
                    self.replies.insert(request_id.clone(), text);
                }
                if let Some(turn_usage) = usage {
                    self.apply_usage(&turn_usage);
                }
                self.active_runs.remove(&request_id);
                self.finished.insert(request_id.clone());
                self.proofs.insert(
                    request_id,
                    Proof::Taken {
                        turn_finished: true,
                    },
                );
                self.turns_served = self.turns_served.saturating_add(1);
                if let Some(budget) = self.adapter.budget()
                    && budget.is_exceeded()
                {
                    self.budget_fault = Some(format!(
                        "remote budget cap exceeded ({}/{} micro-USD)",
                        budget.spent_micros, budget.cap_micros
                    ));
                }
            }
            RemoteEvent::Failed {
                request_id,
                code,
                message,
                status,
                quota_reset_at_ms,
                ..
            } => {
                self.active_runs.remove(&request_id);
                if status == Some(401) || code == "authentication_failed" {
                    self.sign_in_fault = Some(message.clone());
                }
                if let Some(reset_at) = quota_reset_at_ms {
                    self.quota_reset_at_ms = Some(reset_at);
                }
                self.exit_diag = Some(message);
            }
            RemoteEvent::Cancelled { request_id, .. } => {
                self.active_runs.remove(&request_id);
                self.finished.insert(request_id);
            }
            RemoteEvent::BudgetExceeded {
                spent_micros,
                cap_micros,
            } => {
                self.budget_fault = Some(format!(
                    "remote budget cap exceeded ({spent_micros}/{cap_micros} micro-USD)"
                ));
                let _ = self.adapter.interrupt(None);
                self.active_runs.clear();
            }
        }
    }
}

/// In-process [`Harness`] backed directly by an ADR 0010 D7 [`RemoteSessionProvider`].
///
/// Spawns zero local OS child processes and zero keeper processes; a single
/// lightweight background poller drains resumable remote events while any turn
/// is active and notifies supervisor watchers immediately.
pub struct RemoteSessionHarness<A: RemoteSessionProvider> {
    shared: Arc<Mutex<BridgeState<A>>>,
    wake: Arc<Condvar>,
    poller: Option<JoinHandle<()>>,
}

impl<A: RemoteSessionProvider> RemoteSessionHarness<A> {
    /// Create a remote session via `adapter.create(&config)` and start the
    /// lightweight event poller.
    pub fn start(adapter: A, config: &RemoteSessionConfig) -> Result<Self, RemoteSessionError> {
        Self::start_with_poll_interval(adapter, config, DEFAULT_POLL_INTERVAL)
    }

    /// Same as [`Self::start`] with an explicit event-poll cadence (useful in tests).
    pub fn start_with_poll_interval(
        mut adapter: A,
        config: &RemoteSessionConfig,
        poll_interval: Duration,
    ) -> Result<Self, RemoteSessionError> {
        if let Some(cap) = config.budget_cap_micros {
            adapter.set_budget_cap(Some(cap));
        }
        let handle = adapter.create(config)?;
        let shared = Arc::new(Mutex::new(BridgeState {
            adapter,
            alive: true,
            stopping: false,
            session_id: Some(handle.session_id),
            remote_agent_id: handle.remote_agent_id,
            capabilities: handle.capabilities,
            cursor: handle.cursor,
            active_runs: HashMap::new(),
            proofs: HashMap::new(),
            replies: HashMap::new(),
            finished: HashSet::new(),
            usage: TurnUsage::default(),
            turns_served: 0,
            quota_reset_at_ms: None,
            sign_in_fault: None,
            exit_diag: None,
            budget_fault: None,
            watchers: Vec::new(),
        }));
        let wake = Arc::new(Condvar::new());
        let poll_shared = Arc::clone(&shared);
        let poll_wake = Arc::clone(&wake);
        let poller = thread::Builder::new()
            .name("anyagents-remote-session-poller".to_string())
            .spawn(move || {
                loop {
                    let mut st = poll_shared.lock().unwrap_or_else(|e| e.into_inner());
                    while st.alive && !st.stopping && st.active_runs.is_empty() {
                        st = poll_wake.wait(st).unwrap_or_else(|e| e.into_inner());
                    }
                    if !st.alive || st.stopping {
                        break;
                    }
                    let cursor = st.cursor.clone();
                    let poll_res = st.adapter.poll_events(cursor.as_deref());
                    let mut changed = false;
                    match poll_res {
                        Ok(events) => {
                            if !events.is_empty() {
                                changed = true;
                                for ev in events {
                                    st.apply_event(ev);
                                }
                            }
                        }
                        Err(RemoteSessionError::Quota {
                            reset_at_ms,
                            message,
                        }) => {
                            st.quota_reset_at_ms = Some(reset_at_ms);
                            st.exit_diag = Some(message);
                            changed = true;
                        }
                        Err(RemoteSessionError::Auth(message)) => {
                            st.sign_in_fault = Some(message.clone());
                            st.exit_diag = Some(message);
                            st.active_runs.clear();
                            changed = true;
                        }
                        Err(RemoteSessionError::BudgetExceeded {
                            spent_micros,
                            cap_micros,
                        }) => {
                            st.budget_fault = Some(format!(
                                "remote budget cap exceeded ({spent_micros}/{cap_micros} micro-USD)"
                            ));
                            let _ = st.adapter.interrupt(None);
                            st.active_runs.clear();
                            changed = true;
                        }
                        Err(RemoteSessionError::Closed) => {
                            st.alive = false;
                            st.active_runs.clear();
                            changed = true;
                        }
                        Err(
                            RemoteSessionError::Transport(msg) | RemoteSessionError::Protocol(msg),
                        ) => {
                            st.exit_diag = Some(msg);
                            changed = true;
                        }
                    }
                    if changed {
                        st.notify_watchers();
                    }
                    if !st.alive || st.stopping {
                        break;
                    }
                    let _ = poll_wake
                        .wait_timeout(st, poll_interval)
                        .unwrap_or_else(|e| e.into_inner());
                }
            })
            .map_err(|e| RemoteSessionError::Transport(format!("failed to start poller: {e}")))?;

        Ok(Self {
            shared,
            wake,
            poller: Some(poller),
        })
    }

    /// Provider-assigned session identifier.
    pub fn session_id(&self) -> Option<String> {
        self.shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .session_id
            .clone()
    }

    /// Provider-assigned remote agent identifier, when distinct from session ID.
    pub fn remote_agent_id(&self) -> Option<String> {
        self.shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remote_agent_id
            .clone()
    }

    /// Declared ADR 0010 D7 provider capabilities.
    pub fn capabilities(&self) -> RemoteCapabilities {
        self.shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .capabilities
            .clone()
    }

    /// Current budget state, when a cap is configured.
    pub fn budget(&self) -> Option<RemoteBudget> {
        self.shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .adapter
            .budget()
    }

    /// Last completed reply recorded for `request_id`.
    pub fn reply_for(&self, request_id: &str) -> Option<String> {
        self.shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .replies
            .get(request_id)
            .cloned()
    }

    fn shutdown_internal(&mut self) -> Result<(), HarnessError> {
        let stop_res = {
            let mut st = self.shared.lock().unwrap_or_else(|e| e.into_inner());
            if !st.alive && st.stopping {
                Ok(())
            } else {
                st.stopping = true;
                // Drain one final batch of events so proofs are preserved before stopping.
                let cursor = st.cursor.clone();
                if let Ok(events) = st.adapter.poll_events(cursor.as_deref()) {
                    for ev in events {
                        st.apply_event(ev);
                    }
                }
                let res = st.adapter.stop();
                st.alive = false;
                st.active_runs.clear();
                st.notify_watchers();
                res
            }
        };
        self.wake.notify_all();
        if let Some(handle) = self.poller.take() {
            let _ = handle.join();
        }
        stop_res.map_err(|err| HarnessError::Failed(err.to_string()))
    }
}

impl<A: RemoteSessionProvider> Drop for RemoteSessionHarness<A> {
    fn drop(&mut self) {
        let _ = self.shutdown_internal();
    }
}

impl<A: RemoteSessionProvider> Harness for RemoteSessionHarness<A> {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let mut st = self.shared.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(proof) = st.proofs.get(&message.id).copied() {
            return Ok(match proof {
                Proof::Taken { turn_finished } => DeliverOutcome::Accepted { turn_finished },
                Proof::Duplicate => DeliverOutcome::Duplicate,
            });
        }
        if !st.alive {
            return Err(HarnessError::Dead);
        }
        if let Some(reset_at_ms) = st.quota_reset_at_ms {
            return Ok(DeliverOutcome::Quota { reset_at_ms });
        }
        if let Some(budget) = st.adapter.budget()
            && budget.is_exceeded()
        {
            let msg = format!(
                "remote budget cap exceeded ({}/{} micro-USD)",
                budget.spent_micros, budget.cap_micros
            );
            st.budget_fault = Some(msg.clone());
            return Ok(DeliverOutcome::rejected(msg, false));
        }

        let in_turn = !st.active_runs.is_empty();
        if in_turn && !st.capabilities.supports_steer {
            return Ok(DeliverOutcome::Deferred);
        }

        let (mcp_url, mcp_token) = match st.adapter.refresh_mcp_credentials() {
            Some((u, t)) => (Some(u), Some(t)),
            None => (None, None),
        };
        let composed = compose_message(&message.id, &message.context, &message.text);
        let input = RemoteDeliveryInput {
            request_id: message.id.clone(),
            text: composed,
            steer: in_turn,
            mcp_url,
            mcp_token,
        };

        let ack = match st.adapter.deliver(&input) {
            Ok(ack) => ack,
            Err(RemoteSessionError::Auth(msg)) => {
                st.sign_in_fault = Some(msg.clone());
                st.exit_diag = Some(msg.clone());
                return Err(HarnessError::Failed(msg));
            }
            Err(RemoteSessionError::Quota {
                reset_at_ms,
                message,
            }) => {
                st.quota_reset_at_ms = Some(reset_at_ms);
                st.exit_diag = Some(message);
                return Ok(DeliverOutcome::Quota { reset_at_ms });
            }
            Err(RemoteSessionError::BudgetExceeded {
                spent_micros,
                cap_micros,
            }) => {
                let msg =
                    format!("remote budget cap exceeded ({spent_micros}/{cap_micros} micro-USD)");
                st.budget_fault = Some(msg.clone());
                return Ok(DeliverOutcome::rejected(msg, false));
            }
            Err(RemoteSessionError::Closed) => {
                st.alive = false;
                return Err(HarnessError::Dead);
            }
            Err(RemoteSessionError::Transport(msg) | RemoteSessionError::Protocol(msg)) => {
                st.exit_diag = Some(msg.clone());
                return Err(HarnessError::Failed(msg));
            }
        };

        let outcome = match ack {
            RemoteDeliveryAck::Accepted {
                remote_run_id,
                session_id,
                consumed_proof,
                turn_finished,
                usage,
            } => {
                if let Some(sid) = session_id.filter(|s| !s.trim().is_empty()) {
                    st.session_id = Some(sid);
                }
                if let Some(proof) = consumed_proof
                    && !proof.session_id.is_empty()
                {
                    st.session_id = Some(proof.session_id);
                }
                if let Some(turn_usage) = usage {
                    st.apply_usage(&turn_usage);
                }
                if turn_finished {
                    st.finished.insert(message.id.clone());
                    st.turns_served = st.turns_served.saturating_add(1);
                } else {
                    st.active_runs.insert(message.id.clone(), remote_run_id);
                }
                st.proofs
                    .insert(message.id.clone(), Proof::Taken { turn_finished });
                DeliverOutcome::Accepted { turn_finished }
            }
            RemoteDeliveryAck::Duplicate { .. } => {
                st.proofs.insert(message.id.clone(), Proof::Duplicate);
                DeliverOutcome::Duplicate
            }
            RemoteDeliveryAck::InFlight { remote_run_id } => {
                st.active_runs.insert(message.id.clone(), remote_run_id);
                DeliverOutcome::InFlight
            }
            RemoteDeliveryAck::Deferred => DeliverOutcome::Deferred,
            RemoteDeliveryAck::Quota { reset_at_ms } => {
                st.quota_reset_at_ms = Some(reset_at_ms);
                DeliverOutcome::Quota { reset_at_ms }
            }
            RemoteDeliveryAck::Rejected {
                reason,
                message_specific,
                ..
            } => DeliverOutcome::rejected(reason, message_specific),
            RemoteDeliveryAck::BudgetExceeded {
                spent_micros,
                cap_micros,
            } => {
                let msg =
                    format!("remote budget cap exceeded ({spent_micros}/{cap_micros} micro-USD)");
                st.budget_fault = Some(msg.clone());
                DeliverOutcome::rejected(msg, false)
            }
        };

        st.notify_watchers();
        drop(st);
        self.wake.notify_all();
        Ok(outcome)
    }

    fn proven(&self, message_id: &str) -> Option<Proof> {
        self.shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .proofs
            .get(message_id)
            .copied()
    }

    fn stop(&mut self) {
        let _ = self.shutdown_internal();
    }

    fn stop_verified(&mut self) -> Result<(), HarnessError> {
        self.shutdown_internal()
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        Ok(())
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        let mut st = self.shared.lock().unwrap_or_else(|e| e.into_inner());
        let trimmed = session.trim();
        if !trimmed.is_empty() {
            st.session_id = Some(trimmed.to_string());
        }
        Ok(())
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        let mut st = self.shared.lock().unwrap_or_else(|e| e.into_inner());
        if !st.alive {
            return Err(HarnessError::Dead);
        }
        let active = st.active_runs.values().next().cloned();
        st.adapter
            .interrupt(active.as_deref())
            .map_err(|err| HarnessError::Failed(err.to_string()))?;
        st.active_runs.clear();
        st.notify_watchers();
        Ok(())
    }

    fn health(&self) -> HarnessHealth {
        let st = self.shared.lock().unwrap_or_else(|e| e.into_inner());
        HarnessHealth {
            process_alive: st.alive,
            turn_state: if st.active_runs.is_empty() {
                TurnState::Idle
            } else {
                TurnState::InTurn
            },
            delivery: st.capabilities.delivery.clone(),
            quota_reset_at_ms: st.quota_reset_at_ms,
            sign_in_fault: st.sign_in_fault.clone().or_else(|| st.budget_fault.clone()),
        }
    }

    fn served_usage(&self) -> TurnUsage {
        self.shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .usage
            .clone()
    }

    fn turns_served(&self) -> u32 {
        self.shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .turns_served
    }

    fn clear_quota(&mut self) {
        let mut st = self.shared.lock().unwrap_or_else(|e| e.into_inner());
        st.quota_reset_at_ms = None;
    }

    fn exit_diagnostic(&self) -> Option<String> {
        let st = self.shared.lock().unwrap_or_else(|e| e.into_inner());
        st.exit_diag.clone().or_else(|| st.budget_fault.clone())
    }

    fn process_id(&self) -> Option<u32> {
        // ADR 0010 D6: remote sessions hold no local OS process.
        None
    }

    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        let mut st = self.shared.lock().unwrap_or_else(|e| e.into_inner());
        st.watchers.push(notify);
    }
}

/// Deterministic in-memory [`RemoteSessionProvider`] for contract and budget tests.
#[derive(Debug)]
pub struct FakeRemoteProvider {
    pub capabilities: RemoteCapabilities,
    pub session_id: String,
    pub budget: Option<RemoteBudget>,
    pub cost_per_turn_micros: u64,
    pub delivered: Vec<RemoteDeliveryInput>,
    pub interrupted: Vec<Option<String>>,
    pub stopped: bool,
    pending_events: Vec<RemoteEvent>,
    seen_requests: HashSet<String>,
    run_seq: u64,
}

impl Default for FakeRemoteProvider {
    fn default() -> Self {
        Self {
            capabilities: RemoteCapabilities {
                provider: "fake-remote".to_string(),
                delivery: Delivery {
                    level: DeliveryLevel::NextStep,
                    idempotent: true,
                    proof: "fake-remote-proof".into(),
                    peer: Some("fake-remote/v1".to_string()),
                },
                supports_steer: true,
                supports_interrupt: true,
                supports_event_resume: true,
                supports_host_tools: false,
                supports_cost_reporting: true,
                supports_budget_cap: true,
            },
            session_id: "rsess_fake_1".to_string(),
            budget: None,
            cost_per_turn_micros: 0,
            delivered: Vec::new(),
            interrupted: Vec::new(),
            stopped: false,
            pending_events: Vec::new(),
            seen_requests: HashSet::new(),
            run_seq: 0,
        }
    }
}

impl RemoteSessionProvider for FakeRemoteProvider {
    fn capabilities(&self) -> RemoteCapabilities {
        self.capabilities.clone()
    }

    fn create(
        &mut self,
        config: &RemoteSessionConfig,
    ) -> Result<RemoteSessionHandle, RemoteSessionError> {
        if let Some(resume) = &config.resume_session_id {
            self.session_id = resume.clone();
        }
        if let Some(cap) = config.budget_cap_micros {
            self.budget = Some(RemoteBudget::new(cap));
        }
        Ok(RemoteSessionHandle {
            session_id: self.session_id.clone(),
            remote_agent_id: Some(format!("rem_{}", config.agent_id)),
            capabilities: self.capabilities.clone(),
            cursor: None,
        })
    }

    fn deliver(
        &mut self,
        input: &RemoteDeliveryInput,
    ) -> Result<RemoteDeliveryAck, RemoteSessionError> {
        if self.stopped {
            return Err(RemoteSessionError::Closed);
        }
        if let Some(budget) = self.budget
            && budget.is_exceeded()
        {
            return Ok(RemoteDeliveryAck::BudgetExceeded {
                spent_micros: budget.spent_micros,
                cap_micros: budget.cap_micros,
            });
        }
        if !self.seen_requests.insert(input.request_id.clone()) {
            return Ok(RemoteDeliveryAck::Duplicate {
                remote_run_id: format!("run_dup_{}", input.request_id),
            });
        }
        self.run_seq += 1;
        let run_id = format!("run_{}", self.run_seq);
        self.delivered.push(input.clone());
        let cost = self.cost_per_turn_micros;
        if let Some(budget) = &mut self.budget {
            budget.record_spend(Some(cost));
        }
        self.pending_events.push(RemoteEvent::Completed {
            request_id: input.request_id.clone(),
            remote_run_id: run_id.clone(),
            session_id: Some(self.session_id.clone()),
            reply_text: Some(format!("ack:{}", input.request_id)),
            usage: Some(TurnUsage {
                input_tokens: 100,
                output_tokens: 40,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
                reported_cost_micros: Some(cost as i64),
            }),
            cursor: Some(format!("ev_{}", self.run_seq)),
        });
        Ok(RemoteDeliveryAck::InFlight {
            remote_run_id: run_id,
        })
    }

    fn poll_events(
        &mut self,
        _after_cursor: Option<&str>,
    ) -> Result<Vec<RemoteEvent>, RemoteSessionError> {
        if self.stopped {
            return Ok(Vec::new());
        }
        Ok(std::mem::take(&mut self.pending_events))
    }

    fn interrupt(&mut self, remote_run_id: Option<&str>) -> Result<(), RemoteSessionError> {
        self.interrupted.push(remote_run_id.map(str::to_string));
        Ok(())
    }

    fn stop(&mut self) -> Result<(), RemoteSessionError> {
        self.stopped = true;
        Ok(())
    }

    fn budget(&self) -> Option<RemoteBudget> {
        self.budget
    }

    fn set_budget_cap(&mut self, cap_micros: Option<u64>) {
        self.budget = cap_micros.map(|cap| RemoteBudget {
            cap_micros: cap,
            spent_micros: self.budget.map_or(0, |b| b.spent_micros),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Context;

    fn sample_message(id: &str, seq: i64, text: &str) -> OutboundMessage {
        OutboundMessage {
            id: id.to_string(),
            seq,
            text: text.to_string(),
            sender: Some("operator".to_string()),
            reply_to: None,
            source_time_ms: None,
            context: Context::default(),
        }
    }

    #[test]
    fn remote_session_harness_delivers_tracks_usage_and_enforces_budget_cap() {
        let fake = FakeRemoteProvider {
            cost_per_turn_micros: 600_000, // $0.60 per turn
            ..Default::default()
        };

        let cfg = RemoteSessionConfig {
            agent_id: "agt_1".to_string(),
            agent_name: "planner".to_string(),
            budget_cap_micros: Some(1_000_000), // $1.00 hard cap
            ..Default::default()
        };

        let mut harness =
            RemoteSessionHarness::start_with_poll_interval(fake, &cfg, Duration::from_millis(10))
                .expect("start remote harness");

        // ADR 0010 D6: zero local OS child process.
        assert_eq!(harness.process_id(), None);
        assert_eq!(harness.session_id().as_deref(), Some("rsess_fake_1"));
        assert!(harness.health().process_alive);

        // Turn 1 ($0.60 spent, under $1.00 cap).
        let out1 = harness
            .deliver(&sample_message("msg_1", 1, "first"))
            .unwrap();
        assert_eq!(out1, DeliverOutcome::InFlight);

        // Wait briefly for background poller to settle Turn 1.
        for _ in 0..50 {
            if matches!(
                harness.proven("msg_1"),
                Some(Proof::Taken {
                    turn_finished: true
                })
            ) {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            harness.proven("msg_1"),
            Some(Proof::Taken {
                turn_finished: true
            })
        );
        assert_eq!(harness.turns_served(), 1);
        assert_eq!(harness.served_usage().reported_cost_micros, Some(600_000));

        // Redelivering msg_1 returns cached proof without hitting provider again.
        assert_eq!(
            harness
                .deliver(&sample_message("msg_1", 1, "first"))
                .unwrap(),
            DeliverOutcome::Accepted {
                turn_finished: true
            }
        );

        // Turn 2 ($1.20 cumulative -> crosses $1.00 cap).
        let out2 = harness
            .deliver(&sample_message("msg_2", 2, "second"))
            .unwrap();
        assert_eq!(out2, DeliverOutcome::InFlight);
        for _ in 0..50 {
            if harness.turns_served() == 2 {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(harness.turns_served(), 2);
        assert_eq!(harness.served_usage().reported_cost_micros, Some(1_200_000));
        assert!(harness.budget().unwrap().is_exceeded());

        // Turn 3 is refused fail-closed before dispatch because budget cap was reached.
        let out3 = harness
            .deliver(&sample_message("msg_3", 3, "third"))
            .unwrap();
        assert!(matches!(
            out3,
            DeliverOutcome::Rejected {
                message_specific: false,
                ..
            }
        ));

        harness.cancel().unwrap();
        harness.stop_verified().unwrap();
        assert!(!harness.health().process_alive);
        assert_eq!(
            harness.proven("msg_1"),
            Some(Proof::Taken {
                turn_finished: true
            })
        );
    }
}
