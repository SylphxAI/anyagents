//! The kernel side of a remote session: one [`Executor`] per remote agent.
//!
//! The executor is what the supervisor and the delivery loop drive, through
//! the same harness contract as a local process or a keeper. Behind it there
//! is no process: one task on the shared runtime holds the agent's connection
//! to its vendor session, applies each event once, and records what it
//! applied (the cursor, usage, the mailbox ids the session holds) in the
//! agent's `remote_executions` row before it applies the next.
//!
//! * **Lifecycle.** [`Executor::launch`] reconnects to the session the row
//!   names or creates one; [`Executor::reattach`] takes over a session a
//!   previous kernel held, keeping its accounting; dropping the executor
//!   settles the session by the agent's state ([`settle`]): a stopping
//!   kernel or a replaced process leaves it attached for the next one, a
//!   paused agent's session is interrupted and its token revoked, an archived
//!   or moved agent's session is ended.
//! * **Delivery** at the adapter's level: `mid-turn` on admission, `next-step`
//!   on the session's consumption proof. A lost answer is settled with
//!   [`Adapter::holds`] before anything is written again; a message the
//!   session held before this executor existed is a duplicate.
//! * **Events** become the agent's history (a transcript the host index
//!   reads), its usage and cost, and its turn state.
//! * **Budget.** A metered adapter never starts without a cap. At the cap the
//!   agent's mail is held as a quota wait until the cap is raised; when the
//!   kernel holds the cap, it interrupts the running turn there too.
//! * **Identity.** A session that calls back over MCP gets a remote session
//!   token minted at create, rotated before it expires and revoked when the
//!   session stops.

use std::collections::HashSet;
use std::future::Future;
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use anyagents_core::{
    Agent, AgentId, RemoteExecution, RemoteExecutionPatch, RemoteResume, RemoteSessionToken,
    RemoteTotals, Store, now_ms, remote_execution_status as status,
};
use anyagents_harness::{
    DeliverOutcome, Harness, HarnessError, HarnessHealth, OutboundMessage, Proof, TurnState,
    TurnUsage, compose_message,
};
use futures::StreamExt;
use serde_json::{Value, json};

use crate::{
    Adapter, AdapterError, Admission, Budgeting, Capabilities, Coordination, Event, EventKind,
    Grant, Handle, HostTool, Spec, Stop, VendorStatus,
};

/// How long a launch waits for the session's first replay.
const SYNC_WAIT: Duration = Duration::from_secs(20);
/// How long a delivery waits for a replay still in progress.
const DELIVER_SYNC_WAIT: Duration = Duration::from_secs(5);
/// How long a held agent waits before the cap is read again.
const BUDGET_RECHECK_MS: i64 = 5 * 60_000;
const RECONNECT_MIN: Duration = Duration::from_secs(1);
const RECONNECT_MAX: Duration = Duration::from_secs(30);
/// How often the connection looks at its token's lifetime.
const ROTATION_CHECK: Duration = Duration::from_secs(30);
/// Recent mailbox ids and tool calls kept with the cursor.
const KEEP_IDS: usize = 512;
/// One US cent in millionths of a dollar.
const MICROS_PER_CENT: i64 = 10_000;

/// The runtime every remote session's connection lives on.
fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("anyagents-remote")
            .enable_all()
            .build()
            .expect("the remote-session runtime starts")
    })
}

/// Run `future` to completion from synchronous code on any thread, including
/// one that is already inside a runtime.
pub fn block_on<F>(future: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::scope(|scope| {
            scope
                .spawn(|| runtime().block_on(future))
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
        })
    } else {
        runtime().block_on(future)
    }
}

/// Tools the kernel serves a session over its connection
/// ([`Coordination::HostTools`]).
pub trait HostTools: Send + Sync {
    fn tools(&self) -> Vec<HostTool>;
    /// Run one call as `agent`; the output, or the error the model is shown.
    fn call(&self, agent: &AgentId, name: &str, input: &Value) -> Result<String, String>;
}

/// What an executor is started with.
pub struct Config {
    pub store: Arc<Store>,
    pub agent: Agent,
    pub adapter: Arc<dyn Adapter>,
    pub system: Option<String>,
    /// The public MCP URL a session calls back to ([`Coordination::Mcp`]).
    pub mcp_url: Option<String>,
    pub token_ttl_ms: i64,
    /// The spend cap, in US cents.
    pub budget_cents: Option<i64>,
    pub host_tools: Option<Arc<dyn HostTools>>,
}

/// Why the agent's mail is held.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HoldKind {
    /// The vendor paused the session at its cap.
    Vendor,
    /// The kernel stopped the session at its cap.
    Kernel,
}

struct State {
    /// The replay after the cursor has been applied at least once.
    synced: bool,
    /// The session can still run.
    alive: bool,
    vendor_status: &'static str,
    running: bool,
    /// Written and not yet seen to start or end a turn.
    pending: bool,
    hold: Option<(HoldKind, i64)>,
    turn_fault: Option<String>,
    /// The vendor refused the credential the connection uses.
    auth_fault: Option<String>,
    last_error: Option<String>,
    terminated: Option<String>,
    /// Finished turns, their usage and cost: what the supervisor is told.
    totals: RemoteTotals,
    /// Usage of the turn still running, reported when it ends.
    open: TurnUsage,
    /// The highest whole-session cost the vendor reported.
    cost_seen: Option<i64>,
    base: RemoteTotals,
    resume: RemoteResume,
    /// Mailbox ids the session holds, from its own record.
    held: HashSet<String>,
    answered: HashSet<String>,
    /// Written by this executor and admitted (`mid-turn`).
    admitted: HashSet<String>,
    /// Written by this executor, waiting for consumption (`next-step`).
    inflight: HashSet<String>,
    consumed: HashSet<String>,
    /// Answers lost: settled from the session before another write.
    uncertain: HashSet<String>,
    budget_cents: Option<i64>,
    token_session: Option<String>,
    token_expires_at_ms: i64,
    note: Option<String>,
    started_at_ms: i64,
}

/// Work an applied event asks for.
enum Effect {
    Snapshot,
    EndTurn(Stop),
    Interrupt,
    HostCall {
        call_id: String,
        name: String,
        input: Value,
    },
}

impl State {
    fn from_row(row: &RemoteExecution, base: RemoteTotals, started_at_ms: i64) -> Self {
        let vendor_status = match row.vendor_status.as_deref() {
            Some("running") => "running",
            Some("budget_reached") => "budget_reached",
            Some("terminated") => "terminated",
            Some("rescheduling") => "rescheduling",
            _ => "idle",
        };
        let now = now_ms();
        Self {
            synced: false,
            alive: vendor_status != "terminated",
            vendor_status,
            running: vendor_status == "running",
            pending: false,
            hold: (vendor_status == "budget_reached")
                .then_some((HoldKind::Vendor, now + BUDGET_RECHECK_MS)),
            turn_fault: None,
            auth_fault: None,
            last_error: row.last_error.clone(),
            terminated: None,
            totals: row.totals.clone(),
            open: row.open_usage.clone(),
            cost_seen: row
                .cost_cents
                .map(|cents| cents.saturating_mul(MICROS_PER_CENT)),
            base,
            held: row.resume.held.iter().cloned().collect(),
            answered: row.resume.answered.iter().cloned().collect(),
            resume: row.resume.clone(),
            admitted: HashSet::new(),
            inflight: HashSet::new(),
            consumed: HashSet::new(),
            uncertain: HashSet::new(),
            budget_cents: row.budget_cents,
            token_session: row.token_session.clone(),
            token_expires_at_ms: 0,
            note: row.note.clone(),
            started_at_ms,
        }
    }

    /// What the session has cost so far, finished turns and the open one.
    fn cost_micros(&self) -> Option<i64> {
        let summed = match (
            self.totals.usage.reported_cost_micros,
            self.open.reported_cost_micros,
        ) {
            (None, None) => None,
            (done, open) => Some(done.unwrap_or(0) + open.unwrap_or(0)),
        };
        match (summed, self.cost_seen) {
            (None, None) => None,
            (a, b) => Some(a.unwrap_or(0).max(b.unwrap_or(0))),
        }
    }

    /// A turn ended (or paused): its usage and the session's cost become
    /// what the supervisor reads, and a finished turn is counted.
    fn end_turn(&mut self, stop: &Stop, cost: Option<i64>, now: i64) {
        if let Some(cost) = cost {
            self.cost_seen = Some(self.cost_seen.unwrap_or(0).max(cost));
        }
        let cost = self.cost_micros();
        let open = std::mem::take(&mut self.open);
        let totals = &mut self.totals.usage;
        totals.input_tokens += open.input_tokens;
        totals.output_tokens += open.output_tokens;
        totals.cache_read_tokens += open.cache_read_tokens;
        totals.cache_write_tokens += open.cache_write_tokens;
        totals.reported_cost_micros = cost;
        let was_running = self.running;
        self.running = false;
        self.vendor_status = "idle";
        match stop {
            Stop::EndTurn | Stop::Failed(_) | Stop::RequiresAction => {
                self.pending = false;
                if was_running {
                    self.totals.turns += 1;
                }
                if let Stop::Failed(error) = stop {
                    self.turn_fault = Some(format!("the remote turn failed: {error}"));
                }
                if matches!(self.hold, Some((HoldKind::Vendor, _))) {
                    self.hold = None;
                }
            }
            Stop::BudgetReached => {
                self.vendor_status = "budget_reached";
                self.hold = Some((HoldKind::Vendor, now + BUDGET_RECHECK_MS));
            }
        }
    }

    fn over_cap(&self) -> bool {
        match (self.budget_cents, self.cost_micros()) {
            (Some(cap), Some(cost)) => cost >= cap.saturating_mul(MICROS_PER_CENT),
            _ => false,
        }
    }

    fn remember(list: &mut Vec<String>, set: &mut HashSet<String>, id: &str) {
        if set.insert(id.to_string()) {
            list.push(id.to_string());
            if list.len() > KEEP_IDS {
                let drop = list.len() - KEEP_IDS;
                list.drain(..drop);
            }
        }
    }

    fn proof(&self, id: &str) -> Option<Proof> {
        if self.admitted.contains(id) || (self.inflight.contains(id) && self.consumed.contains(id))
        {
            return Some(Proof::Taken {
                turn_finished: false,
            });
        }
        (self.held.contains(id) && !self.inflight.contains(id)).then_some(Proof::Duplicate)
    }

    /// Apply one event, once.
    fn apply(&mut self, event: &Event, budgeting: Budgeting, now: i64) -> Vec<Effect> {
        let mut effects = Vec::new();
        match &event.kind {
            EventKind::CaughtUp => {
                self.synced = true;
                effects.push(Effect::Snapshot);
            }
            EventKind::Input {
                request_id: Some(id),
                ..
            } => Self::remember(&mut self.resume.held, &mut self.held, id),
            EventKind::Input { .. } => {}
            EventKind::Consumed { request_id, .. } => {
                Self::remember(&mut self.resume.held, &mut self.held, request_id);
                self.consumed.insert(request_id.clone());
            }
            EventKind::Returned {
                request_ids,
                reason,
            } => {
                for id in request_ids {
                    self.inflight.remove(id);
                    self.admitted.remove(id);
                }
                self.last_error = Some(format!("the session handed input back: {reason}"));
            }
            EventKind::Usage(usage) => {
                let open = &mut self.open;
                open.input_tokens += usage.input_tokens;
                open.output_tokens += usage.output_tokens;
                open.cache_read_tokens += usage.cache_read_tokens;
                open.cache_write_tokens += usage.cache_write_tokens;
                if let Some(cost) = usage.reported_cost_micros {
                    open.reported_cost_micros = Some(open.reported_cost_micros.unwrap_or(0) + cost);
                }
            }
            EventKind::CostTotal { micros } => {
                self.cost_seen = Some(self.cost_seen.unwrap_or(0).max(*micros));
            }
            EventKind::TurnStarted => {
                self.running = true;
                self.pending = false;
                self.vendor_status = "running";
                self.turn_fault = None;
                if matches!(self.hold, Some((HoldKind::Vendor, _))) {
                    self.hold = None;
                }
            }
            EventKind::TurnEnded(Stop::RequiresAction) => {
                // No one is there to decide: interrupting settles the pending
                // decision and ends the turn.
                self.last_error = Some(
                    "the session asked for a decision AnyAgents does not give; it was interrupted"
                        .into(),
                );
                effects.push(Effect::Interrupt);
            }
            // Finished once the vendor's cost of it is read.
            EventKind::TurnEnded(stop) => effects.push(Effect::EndTurn(stop.clone())),
            EventKind::Terminated(reason) => {
                self.alive = false;
                self.running = false;
                self.pending = false;
                self.vendor_status = "terminated";
                let why = reason
                    .clone()
                    .unwrap_or_else(|| "the remote session ended".into());
                self.terminated = Some(why.clone());
                self.last_error = Some(why);
            }
            EventKind::Error(error) => self.last_error = Some(error.clone()),
            EventKind::HostToolCall {
                call_id,
                name,
                input,
            } => {
                if !self.answered.contains(call_id) {
                    effects.push(Effect::HostCall {
                        call_id: call_id.clone(),
                        name: name.clone(),
                        input: input.clone(),
                    });
                }
            }
            EventKind::Text(_)
            | EventKind::ToolUse { .. }
            | EventKind::ToolResult { .. }
            | EventKind::Other(_) => {}
        }
        if !event.cursor.is_empty() {
            self.resume.cursor = Some(event.cursor.clone());
        }
        if budgeting == Budgeting::Kernel
            && matches!(
                event.kind,
                EventKind::Usage(_) | EventKind::CostTotal { .. }
            )
            && self.hold.is_none()
            && self.over_cap()
        {
            self.hold = Some((HoldKind::Kernel, now + BUDGET_RECHECK_MS));
            self.last_error = Some("the session reached its spend cap".into());
            if self.running {
                effects.push(Effect::Interrupt);
            }
        }
        effects
    }

    fn progress(&self) -> RemoteExecutionPatch {
        RemoteExecutionPatch {
            vendor_status: Some(self.vendor_status.to_string()),
            cost_cents: self.cost_micros().map(|micros| micros / MICROS_PER_CENT),
            totals: Some(self.totals.clone()),
            open_usage: Some(self.open.clone()),
            resume: Some(self.resume.clone()),
            last_error: Some(self.last_error.clone()),
            ..RemoteExecutionPatch::default()
        }
    }
}

type Notify = Arc<dyn Fn() + Send + Sync>;

struct Shared {
    store: Arc<Store>,
    agent_id: AgentId,
    harness: String,
    model: Option<String>,
    adapter: Arc<dyn Adapter>,
    caps: Capabilities,
    handle: Handle,
    mcp_url: Option<String>,
    ttl_ms: i64,
    transcript: PathBuf,
    host_tools: Option<Arc<dyn HostTools>>,
    state: Mutex<State>,
    changed: Condvar,
    closed: AtomicBool,
    activity: Mutex<Option<Notify>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|err| err.into_inner())
}

impl Shared {
    fn state(&self) -> MutexGuard<'_, State> {
        lock(&self.state)
    }

    fn notify(&self) {
        self.changed.notify_all();
        let notify = lock(&self.activity).clone();
        if let Some(notify) = notify {
            notify();
        }
    }

    fn persist(&self, patch: &RemoteExecutionPatch) {
        if let Err(err) = self.store.update_remote_execution(&self.agent_id, patch) {
            eprintln!(
                "anyagents-remote: {}: the session record was not written: {err}",
                self.agent_id
            );
        }
    }

    fn wait_synced(&self, patience: Duration) -> bool {
        let state = self.state();
        let (state, _) = self
            .changed
            .wait_timeout_while(state, patience, |state| {
                !state.synced && state.alive && !self.closed.load(Ordering::SeqCst)
            })
            .unwrap_or_else(|err| err.into_inner());
        state.synced
    }

    /// Append one event to the agent's history, where the host index reads it.
    fn record(&self, event: &Event) {
        let Some(line) = transcript_line(event) else {
            return;
        };
        let written = (|| -> std::io::Result<()> {
            if let Some(dir) = self.transcript.parent() {
                std::fs::create_dir_all(dir)?;
            }
            let mut options = std::fs::OpenOptions::new();
            options.create(true).append(true);
            #[cfg(unix)]
            std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
            let mut file = options.open(&self.transcript)?;
            file.write_all(format!("{line}\n").as_bytes())
        })();
        if let Err(err) = written {
            eprintln!(
                "anyagents-remote: {}: history not written: {err}",
                self.agent_id
            );
        }
    }

    async fn apply(&self, event: Event) {
        let (effects, patch) = {
            let mut state = self.state();
            let effects = state.apply(&event, self.caps.budget, now_ms());
            (effects, state.progress())
        };
        self.record(&event);
        self.persist(&patch);
        self.notify();
        for effect in effects {
            match effect {
                Effect::Snapshot => self.refresh().await,
                Effect::EndTurn(stop) => {
                    let cost = self
                        .adapter
                        .snapshot(&self.handle)
                        .await
                        .ok()
                        .and_then(|snapshot| snapshot.cost_micros);
                    let patch = {
                        let mut state = self.state();
                        state.end_turn(&stop, cost, now_ms());
                        state.progress()
                    };
                    self.persist(&patch);
                    self.notify();
                }
                Effect::Interrupt => {
                    if let Err(err) = self.adapter.interrupt(&self.handle).await {
                        self.state().last_error = Some(format!("interrupt: {err}"));
                    }
                }
                Effect::HostCall {
                    call_id,
                    name,
                    input,
                } => self.host_call(call_id, name, input).await,
            }
        }
    }

    /// What the vendor says about the session now: its cost, and whether it
    /// is over.
    async fn refresh(&self) {
        let Ok(snapshot) = self.adapter.snapshot(&self.handle).await else {
            return;
        };
        let patch = {
            let mut state = self.state();
            if let Some(cost) = snapshot.cost_micros {
                state.cost_seen = Some(state.cost_seen.unwrap_or(0).max(cost));
            }
            // Only an end is taken from the snapshot: the vendor's queryable
            // status trails its event stream, so a `running` here may be a
            // turn the events already ended.
            if snapshot.status == VendorStatus::Terminated {
                state.alive = false;
                state.vendor_status = "terminated";
            }
            state.progress()
        };
        self.persist(&patch);
        self.notify();
    }

    async fn host_call(&self, call_id: String, name: String, input: Value) {
        let (output, is_error) = match self.host_tools.clone() {
            None => (
                format!("AnyAgents serves no host tool `{name}` to this session"),
                true,
            ),
            Some(tools) => {
                let agent = self.agent_id.clone();
                let name = name.clone();
                match tokio::task::spawn_blocking(move || tools.call(&agent, &name, &input)).await {
                    Ok(Ok(output)) => (output, false),
                    Ok(Err(error)) => (error, true),
                    Err(err) => (format!("the host tool failed: {err}"), true),
                }
            }
        };
        match self
            .adapter
            .tool_result(&self.handle, &call_id, &output, is_error)
            .await
        {
            Ok(()) => {
                let patch = {
                    let mut state = self.state();
                    let state = &mut *state;
                    State::remember(&mut state.resume.answered, &mut state.answered, &call_id);
                    state.progress()
                };
                self.persist(&patch);
            }
            Err(err) => {
                self.state().last_error = Some(format!("host tool result for {call_id}: {err}"));
            }
        }
    }

    /// Hand the session a fresh token before the one it holds is half spent.
    async fn rotate_if_due(&self) {
        if self.caps.coordination != Coordination::Mcp {
            return;
        }
        let Some(mcp_url) = self.mcp_url.clone() else {
            return;
        };
        let (session, expires) = {
            let state = self.state();
            (state.token_session.clone(), state.token_expires_at_ms)
        };
        if expires.saturating_sub(now_ms()) >= self.ttl_ms / 2 {
            return;
        }
        let issued = match issue(&self.store, &self.agent_id, session.as_deref(), self.ttl_ms) {
            Ok(issued) => issued,
            Err(err) => {
                self.state().last_error = Some(err);
                return;
            }
        };
        let grant = Grant {
            mcp_url,
            token: issued.token.clone(),
        };
        match self.adapter.grant(&self.handle, &grant).await {
            Ok(()) => {
                {
                    let mut state = self.state();
                    state.token_session = Some(issued.session_id.clone());
                    state.token_expires_at_ms = issued.expires_at_ms;
                }
                self.persist(&RemoteExecutionPatch {
                    token_session: Some(issued.session_id),
                    ..RemoteExecutionPatch::default()
                });
            }
            Err(err) => {
                // A token the session never got is withdrawn.
                let _ = self.store.revoke_remote_session_token(&issued.token);
                self.state().last_error = Some(format!("token rotation: {err}"));
            }
        }
    }

    /// A failed attach or stream. `true` when the session can never be
    /// reached again.
    fn failed(&self, error: AdapterError) -> bool {
        let mut state = self.state();
        match error {
            AdapterError::Gone(why) => {
                state.alive = false;
                state.vendor_status = "terminated";
                state.terminated = Some(why.clone());
                state.last_error = Some(why);
                drop(state);
                self.persist(&RemoteExecutionPatch {
                    vendor_status: Some("terminated".into()),
                    ..RemoteExecutionPatch::default()
                });
                self.notify();
                true
            }
            AdapterError::Auth(why) => {
                state.auth_fault = Some(why);
                drop(state);
                self.notify();
                false
            }
            other => {
                state.last_error = Some(other.to_string());
                false
            }
        }
    }

    async fn run(self: Arc<Self>) {
        let mut backoff = RECONNECT_MIN;
        let mut rotation = tokio::time::interval(ROTATION_CHECK);
        rotation.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if self.closed.load(Ordering::SeqCst) {
                return;
            }
            let after = self.state().resume.cursor.clone();
            match self.adapter.attach(&self.handle, after.as_deref()).await {
                Err(error) => {
                    if self.failed(error) {
                        return;
                    }
                }
                Ok(mut events) => {
                    self.state().auth_fault = None;
                    loop {
                        tokio::select! {
                            _ = rotation.tick() => self.rotate_if_due().await,
                            item = events.next() => match item {
                                None => break,
                                Some(Err(error)) => {
                                    if self.failed(error) {
                                        return;
                                    }
                                    break;
                                }
                                Some(Ok(event)) => {
                                    backoff = RECONNECT_MIN;
                                    self.apply(event).await;
                                    if !self.state().alive {
                                        return;
                                    }
                                }
                            },
                        }
                    }
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(RECONNECT_MAX);
        }
    }
}

/// A token for the session: the next one of its token session while that is
/// live, else a new token session (which revokes the agent's earlier ones).
fn issue(
    store: &Store,
    agent: &AgentId,
    session: Option<&str>,
    ttl_ms: i64,
) -> Result<RemoteSessionToken, String> {
    session
        .and_then(|session| store.rotate_remote_session(agent, session, ttl_ms).ok())
        .map(Ok)
        .unwrap_or_else(|| store.begin_remote_session(agent, ttl_ms))
        .map_err(|err| format!("no remote session token: {err}"))
}

fn transcript_line(event: &Event) -> Option<Value> {
    let (role, kind, content, tool) = match &event.kind {
        EventKind::Input { text, .. } => ("user", "input", text.clone(), None),
        EventKind::Text(text) => ("assistant", "text", text.clone(), None),
        EventKind::ToolUse { name, input } => (
            "assistant",
            "tool_use",
            input.to_string(),
            Some(name.clone()),
        ),
        EventKind::ToolResult { text, is_error } => (
            "tool",
            if *is_error {
                "tool_error"
            } else {
                "tool_result"
            },
            text.clone(),
            None,
        ),
        EventKind::HostToolCall { name, input, .. } => (
            "assistant",
            "host_tool_call",
            input.to_string(),
            Some(name.clone()),
        ),
        EventKind::TurnEnded(Stop::Failed(error)) => ("system", "turn_failed", error.clone(), None),
        EventKind::TurnEnded(Stop::BudgetReached) => (
            "system",
            "budget_reached",
            "the session paused at its spend cap".into(),
            None,
        ),
        EventKind::Terminated(reason) => (
            "system",
            "terminated",
            reason.clone().unwrap_or_default(),
            None,
        ),
        EventKind::Error(error) => ("system", "error", error.clone(), None),
        _ => return None,
    };
    let mut line = json!({
        "role": role,
        "type": kind,
        "id": event.cursor,
        "content": content,
        "direction": if role == "user" { "in" } else { "out" },
    });
    if let Some(at) = event.at_ms {
        line["timestamp_ms"] = json!(at);
    }
    if let Some(tool) = tool {
        line["tool_name"] = json!(tool);
    }
    Some(line)
}

/// How an agent's remote session was settled when nothing held it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Settled {
    /// The agent still runs: the session stays attached for the next start.
    Kept,
    /// The agent sleeps: the session stays, unconnected.
    Parked,
    /// The agent was paused: its turn was interrupted and its token revoked.
    Paused,
    /// The session is over.
    Ended,
}

/// What the agent's state asks of its session.
fn decide(store: &Store, row: &RemoteExecution, alive: bool) -> Settled {
    if !alive {
        return Settled::Ended;
    }
    match store.agent(&row.agent_id) {
        Err(anyagents_core::Error::AgentNotFound(_)) => Settled::Ended,
        // Unknown: leave it for the next start to look again.
        Err(_) => Settled::Kept,
        Ok(agent) if agent.harness != row.harness || agent.state == "archived" => Settled::Ended,
        Ok(agent) if agent.state == "paused" || agent.state == "stopped" => Settled::Paused,
        Ok(agent) if agent.state == "running" => Settled::Kept,
        Ok(_) => Settled::Parked,
    }
}

/// Carry out `settled` on a session nothing is connected to.
fn carry_out(
    store: &Store,
    adapter: &dyn Adapter,
    agent: &AgentId,
    handle: Option<&Handle>,
    settled: Settled,
) {
    let patch = |state: &str| RemoteExecutionPatch {
        status: Some(state.to_string()),
        ..RemoteExecutionPatch::default()
    };
    let write = |patch: RemoteExecutionPatch| {
        if let Err(err) = store.update_remote_execution(agent, &patch) {
            eprintln!("anyagents-remote: {agent}: the session record was not written: {err}");
        }
    };
    match settled {
        Settled::Kept => {}
        Settled::Parked => write(patch(status::PARKED)),
        Settled::Paused => {
            if let Some(handle) = handle
                && let Err(err) = block_on(adapter.interrupt(handle))
            {
                eprintln!(
                    "anyagents-remote: {agent}: the paused session was not interrupted: {err}"
                );
            }
            let _ = store.revoke_remote_sessions(agent, None);
            write(patch(status::PARKED));
        }
        Settled::Ended => {
            if let Some(handle) = handle
                && let Err(err) = block_on(adapter.stop(handle))
            {
                eprintln!("anyagents-remote: {agent}: the session was not stopped: {err}");
            }
            let _ = store.revoke_remote_sessions(agent, None);
            write(patch(status::ENDED));
        }
    }
}

/// Settle an agent's remote session that no executor holds (a session a
/// previous kernel left for an agent that no longer runs), by the agent's
/// state.
pub fn settle(store: &Store, agent: &AgentId, adapter: &dyn Adapter) -> Settled {
    let Ok(Some(row)) = store.remote_execution(agent) else {
        return Settled::Kept;
    };
    if !row.is_live() {
        return Settled::Kept;
    }
    let settled = decide(
        store,
        &row,
        row.vendor_status.as_deref() != Some("terminated"),
    );
    let handle = row.vendor_session.clone().map(|session| Handle {
        session,
        state: row.adapter_state.clone(),
    });
    carry_out(store, adapter, agent, handle.as_ref(), settled);
    settled
}

/// Change an agent's spend cap from outside its executor (an operator call):
/// the vendor's cap when the vendor holds it, and the record, which the
/// executor reads again when its hold comes due. A vendor session paused at
/// the old cap resumes on its own.
pub fn change_budget(
    store: &Store,
    agent: &AgentId,
    adapter: &dyn Adapter,
    cents: i64,
) -> Result<(), String> {
    if cents <= 0 {
        return Err("a spend cap must be positive".into());
    }
    let row = store
        .remote_execution(agent)
        .map_err(|err| err.to_string())?
        .ok_or_else(|| "the agent has no remote session".to_string())?;
    if adapter.capabilities().budget == Budgeting::Vendor
        && row.is_live()
        && let Some(session) = row.vendor_session.clone()
    {
        let handle = Handle {
            session,
            state: row.adapter_state.clone(),
        };
        block_on(adapter.set_budget(&handle, cents)).map_err(|err| err.to_string())?;
    }
    store
        .update_remote_execution(
            agent,
            &RemoteExecutionPatch {
                budget_cents: Some(cents),
                ..RemoteExecutionPatch::default()
            },
        )
        .map_err(|err| err.to_string())?;
    Ok(())
}

/// Agents whose remote session a kernel held when it stopped: the next
/// kernel takes each over ([`take_over`]) or settles it ([`settle`]).
pub fn attached(store: &Store) -> Vec<AgentId> {
    store
        .remote_executions()
        .map(|rows| {
            rows.into_iter()
                .filter(RemoteExecution::is_attached)
                .map(|row| row.agent_id)
                .collect()
        })
        .unwrap_or_default()
}

/// Take over the remote session a previous kernel left attached for the
/// agent `config` names: its process lock first, then the session, with its
/// accounting. `None` when there is none to take over.
pub fn take_over(config: Config) -> Result<Option<(Executor, anyagents_core::AgentLock)>, String> {
    let lock = config
        .store
        .try_lock_agent(&config.agent.id)
        .map_err(|err| format!("the agent's lock: {err}"))?;
    Ok(Executor::reattach(config)?.map(|executor| (executor, lock)))
}

/// One agent's remote session, as the kernel drives it.
pub struct Executor {
    shared: Arc<Shared>,
    task: Option<tokio::task::JoinHandle<()>>,
    detached: bool,
}

impl Executor {
    /// Start the agent's remote session: reconnect to the one its record
    /// names while it can still run for this harness and model, or create one.
    pub fn launch(config: Config) -> Result<Self, String> {
        let caps = config.adapter.capabilities();
        if caps.metered && config.budget_cents.is_none_or(|cents| cents <= 0) {
            return Err(format!(
                "remote agent `{}` runs on a metered vendor and has no spend cap",
                config.agent.name
            ));
        }
        let store = Arc::clone(&config.store);
        let agent = &config.agent;
        let row = store
            .remote_execution(&agent.id)
            .map_err(|e| e.to_string())?;
        let reusable = row.as_ref().filter(|row| {
            row.is_live()
                && row.vendor_status.as_deref() != Some("terminated")
                && row.harness == agent.harness
                && row.adapter == caps.adapter
                && row.model == agent.model
        });
        if let Some(row) = reusable.cloned() {
            // A session the vendor no longer has is replaced below.
            if let Some(executor) = Self::reconnect(&config, &caps, &row, false)? {
                return Ok(executor);
            }
        } else if let Some(old) = row.as_ref().filter(|row| row.is_live())
            && old.adapter == caps.adapter
        {
            // The agent moved to another model: its old session ends.
            let handle = old.vendor_session.clone().map(|session| Handle {
                session,
                state: old.adapter_state.clone(),
            });
            carry_out(
                &store,
                config.adapter.as_ref(),
                &agent.id,
                handle.as_ref(),
                Settled::Ended,
            );
        }
        Self::create(&config, &caps, row.as_ref())
    }

    /// Take over the session a previous kernel left attached, with its
    /// accounting. `None` when there is none to take over.
    pub fn reattach(config: Config) -> Result<Option<Self>, String> {
        let caps = config.adapter.capabilities();
        let Some(row) = config
            .store
            .remote_execution(&config.agent.id)
            .map_err(|e| e.to_string())?
        else {
            return Ok(None);
        };
        if !row.is_attached() || row.harness != config.agent.harness || row.adapter != caps.adapter
        {
            return Ok(None);
        }
        Self::reconnect(&config, &caps, &row, true)
    }

    fn reconnect(
        config: &Config,
        caps: &Capabilities,
        row: &RemoteExecution,
        takeover: bool,
    ) -> Result<Option<Self>, String> {
        let store = &config.store;
        let agent = &config.agent;
        let handle = Handle {
            session: row.vendor_session.clone().unwrap_or_default(),
            state: row.adapter_state.clone(),
        };
        let issued = Self::hand_grant(config, caps, &handle, row.token_session.as_deref());
        let issued = match issued {
            Ok(issued) => issued,
            Err(AdapterError::Gone(_)) => {
                let _ = store.update_remote_execution(
                    &agent.id,
                    &RemoteExecutionPatch {
                        status: Some(status::ENDED.into()),
                        vendor_status: Some("terminated".into()),
                        ..RemoteExecutionPatch::default()
                    },
                );
                return Ok(None);
            }
            Err(err) => {
                return Err(format!(
                    "remote agent `{}` cannot reconnect: {err}",
                    agent.name
                ));
            }
        };
        let mut patch = RemoteExecutionPatch {
            status: Some(status::ATTACHED.into()),
            token_session: issued.as_ref().map(|t| t.session_id.clone()),
            ..RemoteExecutionPatch::default()
        };
        if let Some(cents) = config.budget_cents
            && row.budget_cents != Some(cents)
        {
            if caps.budget == Budgeting::Vendor
                && let Err(err) = block_on(config.adapter.set_budget(&handle, cents))
            {
                eprintln!(
                    "anyagents-remote: {}: the spend cap was not changed: {err}",
                    agent.id
                );
            } else {
                patch.budget_cents = Some(cents);
            }
        }
        // A new start is a new "process" for the supervisor: what it served
        // so far was accounted for under the last one.
        let (base, started) = if takeover {
            (
                row.base.clone(),
                row.process_started_at_ms.unwrap_or_else(now_ms),
            )
        } else {
            let now = now_ms();
            patch.base = Some(row.totals.clone());
            patch.process_started_at_ms = Some(now);
            // The last lineage's note would misstate this one's accounting.
            patch.note = Some(String::new());
            (row.totals.clone(), now)
        };
        store
            .update_remote_execution(&agent.id, &patch)
            .map_err(|e| e.to_string())?;
        let mut state = State::from_row(row, base, started);
        if let Some(cents) = patch.budget_cents {
            state.budget_cents = Some(cents);
        }
        if let Some(issued) = &issued {
            state.token_session = Some(issued.session_id.clone());
            state.token_expires_at_ms = issued.expires_at_ms;
        }
        if !takeover {
            state.note = None;
        }
        Ok(Some(Self::start(config, caps.clone(), handle, state)))
    }

    /// The token a reconnecting session calls back with, handed over.
    fn hand_grant(
        config: &Config,
        caps: &Capabilities,
        handle: &Handle,
        token_session: Option<&str>,
    ) -> Result<Option<RemoteSessionToken>, AdapterError> {
        if caps.coordination != Coordination::Mcp {
            return Ok(None);
        }
        let mcp_url = config.mcp_url.clone().ok_or_else(|| {
            AdapterError::Other("no public MCP URL for the session to call back to".into())
        })?;
        let issued = issue(
            &config.store,
            &config.agent.id,
            token_session,
            config.token_ttl_ms,
        )
        .map_err(AdapterError::Other)?;
        let grant = Grant {
            mcp_url,
            token: issued.token.clone(),
        };
        match block_on(config.adapter.grant(handle, &grant)) {
            Ok(()) => Ok(Some(issued)),
            Err(err) => {
                let _ = config.store.revoke_remote_session_token(&issued.token);
                Err(err)
            }
        }
    }

    fn create(
        config: &Config,
        caps: &Capabilities,
        previous: Option<&RemoteExecution>,
    ) -> Result<Self, String> {
        let store = &config.store;
        let agent = &config.agent;
        let previous_state = previous
            .filter(|row| row.adapter == caps.adapter)
            .map(|row| row.adapter_state.clone())
            .unwrap_or_else(|| json!({}));
        let row = store
            .begin_remote_execution(
                &agent.id,
                caps.adapter,
                &agent.harness,
                agent.model.as_deref(),
                config.budget_cents,
            )
            .map_err(|e| e.to_string())?;
        let issued = match caps.coordination {
            Coordination::Mcp => {
                if config.mcp_url.is_none() {
                    return Err(format!(
                        "remote agent `{}` has no public MCP URL to call back to",
                        agent.name
                    ));
                }
                Some(
                    store
                        .begin_remote_session(&agent.id, config.token_ttl_ms)
                        .map_err(|e| e.to_string())?,
                )
            }
            Coordination::HostTools => None,
        };
        let spec = Spec {
            agent_id: agent.id.to_string(),
            agent_name: agent.name.clone(),
            model: agent.model.clone(),
            system: config.system.clone(),
            grant: issued
                .as_ref()
                .zip(config.mcp_url.as_ref())
                .map(|(issued, url)| Grant {
                    mcp_url: url.clone(),
                    token: issued.token.clone(),
                }),
            host_tools: config
                .host_tools
                .as_ref()
                .map(|tools| tools.tools())
                .unwrap_or_default(),
            budget_cents: config.budget_cents,
        };
        let handle = match block_on(config.adapter.create(&spec, &previous_state)) {
            Ok(handle) => handle,
            Err(err) => {
                if let Some(issued) = &issued {
                    let _ = store.revoke_remote_session_token(&issued.token);
                }
                let why = format!(
                    "remote agent `{}`: the session was not created: {err}",
                    agent.name
                );
                let _ = store.update_remote_execution(
                    &agent.id,
                    &RemoteExecutionPatch {
                        status: Some(status::ENDED.into()),
                        last_error: Some(Some(why.clone())),
                        ..RemoteExecutionPatch::default()
                    },
                );
                return Err(why);
            }
        };
        let now = now_ms();
        let patch = RemoteExecutionPatch {
            status: Some(status::ATTACHED.into()),
            vendor_session: Some(handle.session.clone()),
            adapter_state: Some(handle.state.clone()),
            vendor_status: Some("idle".into()),
            token_session: issued.as_ref().map(|t| t.session_id.clone()),
            process_started_at_ms: Some(now),
            ..RemoteExecutionPatch::default()
        };
        store
            .update_remote_execution(&agent.id, &patch)
            .map_err(|e| e.to_string())?;
        let mut state = State::from_row(&row, RemoteTotals::default(), now);
        state.vendor_status = "idle";
        state.running = false;
        if let Some(issued) = &issued {
            state.token_session = Some(issued.session_id.clone());
            state.token_expires_at_ms = issued.expires_at_ms;
        }
        Ok(Self::start(config, caps.clone(), handle, state))
    }

    fn start(config: &Config, caps: Capabilities, handle: Handle, state: State) -> Self {
        let agent = &config.agent;
        let transcript = config
            .store
            .data_dir()
            .join("agents")
            .join(agent.id.as_str())
            .join("transcripts")
            .join(format!("remote-{}.jsonl", file_safe(&handle.session)));
        let shared = Arc::new(Shared {
            store: Arc::clone(&config.store),
            agent_id: agent.id.clone(),
            harness: agent.harness.clone(),
            model: agent.model.clone(),
            adapter: Arc::clone(&config.adapter),
            caps,
            handle,
            mcp_url: config.mcp_url.clone(),
            ttl_ms: config.token_ttl_ms,
            transcript,
            host_tools: config.host_tools.clone(),
            state: Mutex::new(state),
            changed: Condvar::new(),
            closed: AtomicBool::new(false),
            activity: Mutex::new(None),
        });
        let task = runtime().spawn(Arc::clone(&shared).run());
        shared.wait_synced(SYNC_WAIT);
        Self {
            shared,
            task: Some(task),
            detached: false,
        }
    }

    pub fn capabilities(&self) -> &Capabilities {
        &self.shared.caps
    }

    pub fn handle(&self) -> &Handle {
        &self.shared.handle
    }

    pub fn harness(&self) -> &str {
        &self.shared.harness
    }

    pub fn model(&self) -> Option<&str> {
        self.shared.model.as_deref()
    }

    pub fn started_at_ms(&self) -> i64 {
        self.shared.state().started_at_ms
    }

    /// What the last kernel kept with this session for the next one.
    pub fn note(&self) -> Option<String> {
        self.shared.state().note.clone()
    }

    /// Keep `note` with the session for the kernel that takes it over.
    pub fn set_note(&self, note: &str) {
        self.shared.state().note = Some(note.to_string());
        self.shared.persist(&RemoteExecutionPatch {
            note: Some(note.to_string()),
            ..RemoteExecutionPatch::default()
        });
    }

    /// Change the session's spend cap. A cap above what it has cost releases
    /// the mail held at the old one.
    pub fn set_budget(&self, cents: i64) -> Result<(), String> {
        if cents <= 0 {
            return Err("a spend cap must be positive".into());
        }
        if self.shared.caps.budget == Budgeting::Vendor {
            block_on(self.shared.adapter.set_budget(&self.shared.handle, cents))
                .map_err(|err| err.to_string())?;
        }
        {
            let mut state = self.shared.state();
            state.budget_cents = Some(cents);
            if !state.over_cap() {
                state.hold = None;
                if state.vendor_status == "budget_reached" {
                    state.vendor_status = "idle";
                }
            }
        }
        self.shared.persist(&RemoteExecutionPatch {
            budget_cents: Some(cents),
            ..RemoteExecutionPatch::default()
        });
        self.shared.notify();
        Ok(())
    }

    /// Leave the session running for the next kernel: only this connection
    /// ends.
    pub fn detach(mut self) {
        self.detached = true;
        self.close();
    }

    fn close(&mut self) {
        self.shared.closed.store(true, Ordering::SeqCst);
        if let Some(task) = self.task.take() {
            task.abort();
        }
        self.shared.changed.notify_all();
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        self.close();
        if self.detached {
            return;
        }
        let alive = self.shared.state().alive;
        let row = match self.shared.store.remote_execution(&self.shared.agent_id) {
            Ok(Some(row)) => row,
            // The agent and its record are gone: its session ends with it.
            Ok(None) => {
                if let Err(err) = block_on(self.shared.adapter.stop(&self.shared.handle)) {
                    eprintln!(
                        "anyagents-remote: {}: the session was not stopped: {err}",
                        self.shared.agent_id
                    );
                }
                return;
            }
            Err(_) => return,
        };
        let settled = decide(&self.shared.store, &row, alive);
        carry_out(
            &self.shared.store,
            self.shared.adapter.as_ref(),
            &self.shared.agent_id,
            Some(&self.shared.handle),
            settled,
        );
    }
}

fn file_safe(text: &str) -> String {
    text.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

impl Harness for Executor {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let shared = &self.shared;
        let id = message.id.as_str();
        let settle_lost = {
            let state = shared.state();
            if let Some(proof) = state.proof(id) {
                return Ok(match proof {
                    Proof::Taken { turn_finished } => DeliverOutcome::Accepted { turn_finished },
                    Proof::Duplicate => DeliverOutcome::Duplicate,
                });
            }
            if state.inflight.contains(id) {
                return Ok(DeliverOutcome::InFlight);
            }
            if !state.alive {
                return Err(HarnessError::Dead);
            }
            if let Some((_, until)) = state.hold {
                return Ok(DeliverOutcome::Quota { reset_at_ms: until });
            }
            state.uncertain.contains(id)
        };
        if !shared.wait_synced(DELIVER_SYNC_WAIT) {
            return Ok(DeliverOutcome::Deferred);
        }
        let next_step = shared.caps.delivery.level == anyagents_harness::DeliveryLevel::NextStep;
        if settle_lost {
            match block_on(shared.adapter.holds(&shared.handle, id)) {
                Ok(true) => {
                    let mut state = shared.state();
                    state.uncertain.remove(id);
                    return Ok(if next_step {
                        state.inflight.insert(id.to_string());
                        DeliverOutcome::InFlight
                    } else {
                        state.admitted.insert(id.to_string());
                        DeliverOutcome::Accepted {
                            turn_finished: false,
                        }
                    });
                }
                Ok(false) => {
                    shared.state().uncertain.remove(id);
                }
                Err(err) => {
                    shared.state().last_error = Some(err.to_string());
                    return Ok(DeliverOutcome::Deferred);
                }
            }
        }
        let text = compose_message(id, &message.context, &message.text);
        let answer = block_on(shared.adapter.deliver(&shared.handle, id, &text));
        let outcome = {
            let mut state = shared.state();
            match answer {
                Ok(Admission::Accepted { .. }) => {
                    state.admitted.insert(id.to_string());
                    state.pending = true;
                    Ok(DeliverOutcome::Accepted {
                        turn_finished: false,
                    })
                }
                Ok(Admission::InFlight) => {
                    state.inflight.insert(id.to_string());
                    state.pending = true;
                    Ok(DeliverOutcome::InFlight)
                }
                Ok(Admission::Duplicate) => {
                    let state = &mut *state;
                    State::remember(&mut state.resume.held, &mut state.held, id);
                    Ok(DeliverOutcome::Duplicate)
                }
                Ok(Admission::Refused {
                    reason,
                    message_specific,
                }) => Ok(DeliverOutcome::Rejected {
                    reason,
                    message_specific,
                }),
                Ok(Admission::Wait(reason)) => {
                    state.last_error = Some(reason);
                    Ok(DeliverOutcome::Deferred)
                }
                Ok(Admission::Unknown(reason)) => {
                    state.uncertain.insert(id.to_string());
                    state.last_error = Some(reason);
                    Ok(DeliverOutcome::Deferred)
                }
                Err(AdapterError::Auth(why)) => {
                    state.auth_fault = Some(why.clone());
                    Err(HarnessError::Failed(why))
                }
                Err(AdapterError::Gone(why)) => {
                    state.alive = false;
                    state.terminated = Some(why);
                    Err(HarnessError::Dead)
                }
                Err(err) => {
                    state.last_error = Some(err.to_string());
                    Ok(DeliverOutcome::Deferred)
                }
            }
        };
        shared.notify();
        outcome
    }

    fn proven(&self, message_id: &str) -> Option<Proof> {
        self.shared.state().proof(message_id)
    }

    /// The connection ends; what it proved stays readable.
    fn stop(&mut self) {
        self.close();
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        Ok(())
    }

    fn resume(&mut self, _session: &str) -> Result<(), HarnessError> {
        Ok(())
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        if !self.shared.caps.interrupt {
            return Err(HarnessError::Failed(
                "this remote session cannot be interrupted".into(),
            ));
        }
        block_on(self.shared.adapter.interrupt(&self.shared.handle))
            .map_err(|err| HarnessError::Failed(err.to_string()))
    }

    fn health(&self) -> HarnessHealth {
        let state = self.shared.state();
        HarnessHealth {
            process_alive: state.alive,
            turn_state: if state.running || state.pending || state.hold.is_some() {
                TurnState::InTurn
            } else {
                TurnState::Idle
            },
            delivery: self.shared.caps.delivery.clone(),
            quota_reset_at_ms: state.hold.map(|(_, until)| until),
            sign_in_fault: state
                .auth_fault
                .clone()
                .or_else(|| state.turn_fault.clone()),
        }
    }

    fn served_usage(&self) -> TurnUsage {
        let state = self.shared.state();
        state.totals.usage.difference(state.base.usage.clone())
    }

    fn turns_served(&self) -> u32 {
        let state = self.shared.state();
        state.totals.turns.saturating_sub(state.base.turns)
    }

    fn last_usage(&self) -> TurnUsage {
        self.served_usage()
    }

    /// The hold's time came: read the cap and the session again.
    fn clear_quota(&mut self) {
        let shared = &self.shared;
        let kind = match shared.state().hold {
            Some((kind, _)) => kind,
            None => return,
        };
        let snapshot = block_on(shared.adapter.snapshot(&shared.handle)).ok();
        // An operator may have changed the cap from outside this executor.
        let recorded_cap = shared
            .store
            .remote_execution(&shared.agent_id)
            .ok()
            .flatten()
            .and_then(|row| row.budget_cents);
        let mut state = shared.state();
        if recorded_cap.is_some() {
            state.budget_cents = recorded_cap;
        }
        if let Some(cost) = snapshot.as_ref().and_then(|s| s.cost_micros) {
            state.cost_seen = Some(state.cost_seen.unwrap_or(0).max(cost));
        }
        let released = match kind {
            HoldKind::Vendor => snapshot.as_ref().is_some_and(|snapshot| {
                snapshot.status == VendorStatus::Running
                    || snapshot
                        .budget_cents
                        .zip(snapshot.cost_micros)
                        .is_some_and(|(cap, cost)| cap.saturating_mul(MICROS_PER_CENT) > cost)
            }),
            HoldKind::Kernel => !state.over_cap(),
        };
        state.hold = if released {
            None
        } else {
            Some((kind, now_ms() + BUDGET_RECHECK_MS))
        };
    }

    fn exit_diagnostic(&self) -> Option<String> {
        let state = self.shared.state();
        state
            .terminated
            .clone()
            .or_else(|| state.last_error.clone())
    }

    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        *lock(&self.shared.activity) = Some(notify);
    }
}

impl anyagents_keeper::Hosted for Executor {
    fn session_id(&self) -> Option<String> {
        Some(self.shared.handle.session.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(cursor: &str, kind: EventKind) -> Event {
        Event {
            cursor: cursor.into(),
            at_ms: None,
            kind,
        }
    }

    fn fresh() -> State {
        let row = RemoteExecution {
            agent_id: AgentId::generate(),
            adapter: "a".into(),
            harness: "h".into(),
            model: None,
            vendor_session: Some("s".into()),
            adapter_state: json!({}),
            status: status::ATTACHED.into(),
            vendor_status: None,
            token_session: None,
            budget_cents: Some(100),
            cost_cents: None,
            totals: RemoteTotals::default(),
            open_usage: TurnUsage::default(),
            base: RemoteTotals::default(),
            resume: RemoteResume::default(),
            process_started_at_ms: None,
            note: None,
            last_error: None,
            created_at_ms: 0,
            updated_at_ms: 0,
        };
        State::from_row(&row, RemoteTotals::default(), 0)
    }

    /// Apply events as the connection does: a turn end is finished with
    /// the vendor's cost of it (none here).
    fn feed(state: &mut State, events: &[Event], budgeting: Budgeting) -> Vec<&'static str> {
        let mut done = Vec::new();
        for e in events {
            for effect in state.apply(e, budgeting, 0) {
                match effect {
                    Effect::EndTurn(stop) => state.end_turn(&stop, None, 0),
                    Effect::Interrupt => done.push("interrupt"),
                    Effect::Snapshot => done.push("snapshot"),
                    Effect::HostCall { .. } => done.push("host_call"),
                }
            }
        }
        done
    }

    #[test]
    fn turns_usage_cursor_and_held_ids_follow_the_events() {
        let mut state = fresh();
        let usage = TurnUsage {
            input_tokens: 7,
            output_tokens: 3,
            ..TurnUsage::default()
        };
        feed(
            &mut state,
            &[
                event(
                    "e1",
                    EventKind::Input {
                        request_id: Some("msg_a".into()),
                        text: String::new(),
                        consumed: false,
                    },
                ),
                event("e2", EventKind::TurnStarted),
                event("e3", EventKind::Usage(usage.clone())),
                event("e4", EventKind::CostTotal { micros: 50_000 }),
            ],
            Budgeting::Vendor,
        );
        // Nothing is reported while the turn runs.
        assert_eq!(state.totals, RemoteTotals::default());
        assert_eq!(state.cost_micros(), Some(50_000));
        feed(
            &mut state,
            &[event("", EventKind::TurnEnded(Stop::EndTurn))],
            Budgeting::Vendor,
        );
        assert_eq!(state.totals.turns, 1);
        assert_eq!(state.totals.usage.input_tokens, 7);
        assert_eq!(state.totals.usage.reported_cost_micros, Some(50_000));
        assert_eq!(state.open, TurnUsage::default());
        assert_eq!(state.resume.cursor.as_deref(), Some("e4"));
        assert_eq!(state.proof("msg_a"), Some(Proof::Duplicate));
        state.admitted.insert("msg_a".into());
        assert_eq!(
            state.proof("msg_a"),
            Some(Proof::Taken {
                turn_finished: false
            })
        );
        // An idle without a turn before it is no turn.
        feed(
            &mut state,
            &[event("e5", EventKind::TurnEnded(Stop::EndTurn))],
            Budgeting::Vendor,
        );
        assert_eq!(state.totals.turns, 1);
    }

    #[test]
    fn a_kernel_cap_interrupts_the_running_turn_and_a_vendor_pause_holds_until_it_resumes() {
        let mut state = fresh();
        let done = feed(
            &mut state,
            &[
                event("1", EventKind::TurnStarted),
                event(
                    "2",
                    EventKind::Usage(TurnUsage {
                        reported_cost_micros: Some(1_000_000),
                        ..TurnUsage::default()
                    }),
                ),
            ],
            Budgeting::Kernel,
        );
        assert_eq!(done, vec!["interrupt"]);
        assert!(matches!(state.hold, Some((HoldKind::Kernel, _))));

        let mut state = fresh();
        feed(
            &mut state,
            &[
                event("1", EventKind::TurnStarted),
                event("2", EventKind::TurnEnded(Stop::BudgetReached)),
            ],
            Budgeting::Vendor,
        );
        assert!(matches!(state.hold, Some((HoldKind::Vendor, _))));
        assert_eq!(state.totals.turns, 0, "a paused turn has not ended");
        feed(
            &mut state,
            &[
                event("3", EventKind::TurnStarted),
                event("4", EventKind::TurnEnded(Stop::EndTurn)),
            ],
            Budgeting::Vendor,
        );
        assert!(state.hold.is_none());
        assert_eq!(state.totals.turns, 1);
    }

    #[test]
    fn next_step_proof_waits_for_consumption_and_returned_input_is_offered_again() {
        let mut state = fresh();
        state.inflight.insert("msg_b".into());
        assert_eq!(state.proof("msg_b"), None);
        feed(
            &mut state,
            &[event(
                "9",
                EventKind::Consumed {
                    request_id: "msg_b".into(),
                    duplicate: false,
                },
            )],
            Budgeting::Kernel,
        );
        assert_eq!(
            state.proof("msg_b"),
            Some(Proof::Taken {
                turn_finished: false
            })
        );
        state.inflight.insert("msg_c".into());
        feed(
            &mut state,
            &[event(
                "10",
                EventKind::Returned {
                    request_ids: vec!["msg_c".into()],
                    reason: "turn_not_started".into(),
                },
            )],
            Budgeting::Kernel,
        );
        assert!(!state.inflight.contains("msg_c"));
        assert_eq!(state.proof("msg_c"), None);
    }
}
