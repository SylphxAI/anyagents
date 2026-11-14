//! The kernel side of a remote session: one [`Executor`] per remote agent.
//!
//! The executor is what the supervisor and the delivery loop drive, through
//! the same harness contract as a local process or a keeper. Behind it there
//! is no process: one task on the shared runtime holds the agent's connection
//! to its vendor session and applies each event once. An event and everything
//! it changes (the cursor, a turn it closes, usage, the mailbox ids the
//! session holds) reach the agent's `remote_executions` row in one write.
//!
//! * **Lifecycle.** [`Executor::launch`] reconnects to the session the row
//!   names or creates one; [`Executor::reattach`] takes over a session a
//!   previous kernel held, keeping its accounting; dropping the executor
//!   settles the session by the agent's state ([`settle`]): a stopping
//!   kernel or a replaced process leaves it attached for the next one, a
//!   paused agent's session is interrupted and its token revoked, an archived
//!   or moved agent's session is ended. A session is stopped before another
//!   replaces it, and a session the vendor keeps ending is replaced no faster
//!   than a doubling backoff.
//! * **Delivery** at the adapter's level: `mid-turn` on admission, `next-step`
//!   on the session's proof that it consumed the message. A message an
//!   interrupted turn never consumed is offered again. A lost answer is
//!   settled with [`Adapter::holds`] before anything is written again; a
//!   message the session held before this executor existed is a duplicate.
//!   Mail the vendor did not take waits out an exponential backoff with
//!   jitter, never less than the vendor's `retry-after`.
//! * **Events** become the agent's history (a transcript the host index
//!   reads), its usage and cost, and its turn state.
//! * **Budget.** The cap belongs to the agent: every session is created with
//!   what is left of it after the agent's earlier sessions, and none starts
//!   once it is spent. At the cap the agent's mail is held as a quota wait
//!   until the cap is raised; when the kernel holds the cap, it interrupts
//!   the running turn there too.
//! * **Identity.** A session that calls back over MCP gets a remote session
//!   token minted at create, rotated before it expires and revoked when the
//!   session stops.

use std::collections::HashSet;
use std::future::Future;
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use anyagents_core::{
    Agent, AgentId, RemoteExecution, RemoteExecutionPatch, RemoteResume, RemoteSessionToken,
    RemoteTotals, Store, now_ms, remote_execution_status as status,
};
use anyagents_harness::{
    DeliverOutcome, DeliveryLevel, Harness, HarnessError, HarnessHealth, OutboundMessage, Proof,
    TurnState, TurnUsage, compose_message,
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
/// The wait before replacing a session the vendor ended: this, doubling
/// with each one ended in a row, up to [`TERMINATION_BACKOFF_MAX_MS`].
const TERMINATION_BACKOFF_MS: i64 = 60_000;
const TERMINATION_BACKOFF_MAX_MS: i64 = 3_600_000;

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

/// Run `work` on the remote runtime's blocking pool, so a caller that must
/// not wait on a vendor (the supervisor's router) returns at once.
pub fn spawn_blocking(work: impl FnOnce() + Send + 'static) {
    drop(runtime().spawn_blocking(work));
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
    /// The agent's spend cap, in US cents, across all its sessions.
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
    /// The replay after the cursor has been applied since the last connect.
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
    /// Mailbox ids the session consumed, from its own record.
    consumed: HashSet<String>,
    /// Mailbox ids the session holds and has not consumed yet.
    queued: HashSet<String>,
    answered: HashSet<String>,
    /// Written by this executor and admitted (`mid-turn` proof).
    admitted: HashSet<String>,
    /// Written by this executor (or found queued), waiting for consumption.
    inflight: HashSet<String>,
    /// Answers lost: settled from the session before another write.
    uncertain: HashSet<String>,
    /// An interrupt was sent for the running turn.
    interrupted: bool,
    /// A turn completed since the record last said so.
    completed: bool,
    /// The agent's cap, and what its earlier sessions spent.
    cap_cents: Option<i64>,
    spent_before_micros: i64,
    /// Mail the vendor did not take waits until then.
    retry_at_ms: i64,
    retry_attempt: u32,
    token_session: Option<String>,
    token_expires_at_ms: i64,
    note: Option<String>,
    started_at_ms: i64,
}

/// Work an applied event asks for.
enum Effect {
    Snapshot,
    /// Ask whether inputs held behind the cursor were consumed since.
    Reconcile,
    Interrupt,
    HostCall {
        call_id: String,
        name: String,
        input: Value,
    },
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

fn forget(list: &mut Vec<String>, set: &mut HashSet<String>, id: &str) {
    if set.remove(id) {
        list.retain(|kept| kept != id);
    }
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
            consumed: row.resume.held.iter().cloned().collect(),
            queued: row.resume.queued.iter().cloned().collect(),
            answered: row.resume.answered.iter().cloned().collect(),
            resume: row.resume.clone(),
            admitted: HashSet::new(),
            inflight: HashSet::new(),
            uncertain: HashSet::new(),
            interrupted: false,
            completed: false,
            cap_cents: row.budget_cents,
            spent_before_micros: row.spent_before_micros,
            retry_at_ms: 0,
            retry_attempt: 0,
            token_session: row.token_session.clone(),
            token_expires_at_ms: 0,
            note: row.note.clone(),
            started_at_ms,
        }
    }

    /// What this session has cost so far, finished turns and the open one.
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

    /// What this session may still spend: the agent's cap less what its
    /// earlier sessions spent.
    fn allowance_micros(&self) -> Option<i64> {
        self.cap_cents.map(|cap| {
            cap.saturating_mul(MICROS_PER_CENT)
                .saturating_sub(self.spent_before_micros)
        })
    }

    fn over_cap(&self) -> bool {
        match self.allowance_micros() {
            Some(allowance) => self.cost_micros().unwrap_or(0) >= allowance,
            None => false,
        }
    }

    fn mark_consumed(&mut self, id: &str) {
        forget(&mut self.resume.queued, &mut self.queued, id);
        remember(&mut self.resume.held, &mut self.consumed, id);
    }

    fn proof(&self, id: &str) -> Option<Proof> {
        if self.consumed.contains(id) {
            // Written by this executor (an answer it lost included): taken;
            // by an earlier process: a duplicate.
            return Some(
                if self.inflight.contains(id)
                    || self.admitted.contains(id)
                    || self.uncertain.contains(id)
                {
                    Proof::Taken {
                        turn_finished: false,
                    }
                } else {
                    Proof::Duplicate
                },
            );
        }
        self.admitted.contains(id).then_some(Proof::Taken {
            turn_finished: false,
        })
    }

    /// A turn ended (or paused): it is closed here, with the event that ended
    /// it, so the cursor is never recorded past an end the record does not
    /// show. Its usage and the cost known so far become what the supervisor
    /// reads; a finished turn is counted.
    fn end_turn(&mut self, stop: &Stop, now: i64) {
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
                    self.completed = true;
                }
                if let Stop::Failed(error) = stop {
                    self.turn_fault = Some(format!("the remote turn failed: {error}"));
                }
                if matches!(self.hold, Some((HoldKind::Vendor, _))) {
                    self.hold = None;
                }
                if std::mem::take(&mut self.interrupted) {
                    // An interrupted turn may leave mail it never consumed:
                    // it goes back to the mailbox and is offered again.
                    let unconsumed: Vec<String> = self
                        .inflight
                        .iter()
                        .chain(self.queued.iter())
                        .filter(|id| !self.consumed.contains(*id))
                        .cloned()
                        .collect();
                    for id in unconsumed {
                        self.inflight.remove(&id);
                        forget(&mut self.resume.queued, &mut self.queued, &id);
                    }
                }
            }
            Stop::BudgetReached => {
                self.vendor_status = "budget_reached";
                self.hold = Some((HoldKind::Vendor, now + BUDGET_RECHECK_MS));
            }
        }
    }

    /// Apply one event, once.
    fn apply(&mut self, event: &Event, budgeting: Budgeting, now: i64) -> Vec<Effect> {
        let mut effects = Vec::new();
        match &event.kind {
            EventKind::CaughtUp => {
                self.synced = true;
                effects.push(Effect::Snapshot);
                if !self.queued.is_empty() {
                    effects.push(Effect::Reconcile);
                }
            }
            EventKind::Input {
                request_id: Some(id),
                consumed,
                ..
            } => {
                if *consumed {
                    self.mark_consumed(id);
                } else if !self.consumed.contains(id) {
                    remember(&mut self.resume.queued, &mut self.queued, id);
                }
            }
            EventKind::Input { .. } => {}
            EventKind::Consumed { request_id, .. } => self.mark_consumed(request_id),
            EventKind::Returned {
                request_ids,
                reason,
            } => {
                for id in request_ids {
                    self.inflight.remove(id);
                    self.admitted.remove(id);
                    forget(&mut self.resume.queued, &mut self.queued, id);
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
                self.interrupted = true;
                effects.push(Effect::Interrupt);
            }
            EventKind::TurnEnded(stop) => {
                self.end_turn(stop, now);
                // The vendor prices a turn after it ends: read it next.
                effects.push(Effect::Snapshot);
            }
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
            self.last_error = Some("the agent reached its spend cap".into());
            if self.running {
                self.interrupted = true;
                effects.push(Effect::Interrupt);
            }
        }
        effects
    }

    /// The vendor's word on what the session cost: what a turn that already
    /// ended cost is reported at once; a running turn's, when it ends.
    fn absorb_cost(&mut self, cost: Option<i64>) {
        let Some(cost) = cost else {
            return;
        };
        self.cost_seen = Some(self.cost_seen.unwrap_or(0).max(cost));
        if !self.running && self.open == TurnUsage::default() {
            let reported = &mut self.totals.usage.reported_cost_micros;
            *reported = Some(reported.unwrap_or(0).max(cost));
        }
    }

    fn progress(&mut self) -> RemoteExecutionPatch {
        RemoteExecutionPatch {
            vendor_status: Some(self.vendor_status.to_string()),
            cost_cents: self.cost_micros().map(|micros| micros / MICROS_PER_CENT),
            totals: Some(self.totals.clone()),
            open_usage: Some(self.open.clone()),
            resume: Some(self.resume.clone()),
            last_error: Some(self.last_error.clone()),
            terminations: std::mem::take(&mut self.completed).then_some(0),
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
    /// A wake is already scheduled for this time.
    wake_at: AtomicI64,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|err| err.into_inner())
}

impl Shared {
    fn state(&self) -> MutexGuard<'_, State> {
        lock(&self.state)
    }

    /// Wake whoever drives this agent: something it would act on changed.
    fn notify(&self) {
        self.changed.notify_all();
        let notify = lock(&self.activity).clone();
        if let Some(notify) = notify {
            notify();
        }
    }

    /// Wake the agent's driver at `at_ms`, once, however many deliveries
    /// were put off until then.
    fn wake_at(self: &Arc<Self>, at_ms: i64) {
        let scheduled = self.wake_at.load(Ordering::SeqCst);
        if scheduled >= at_ms && scheduled > now_ms() {
            return;
        }
        self.wake_at.store(at_ms, Ordering::SeqCst);
        let shared = Arc::clone(self);
        drop(runtime().spawn(async move {
            let wait = at_ms.saturating_sub(now_ms()).max(0) as u64;
            tokio::time::sleep(Duration::from_millis(wait)).await;
            if !shared.closed.load(Ordering::SeqCst) {
                shared.notify();
            }
        }));
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
        let (effects, patch, echo) = {
            let mut state = self.state();
            // An input's processed echo proves it; its text is already in
            // the history.
            let echo = matches!(
                &event.kind,
                EventKind::Input { request_id: Some(id), consumed: true, .. }
                    if state.queued.contains(id)
            );
            let effects = state.apply(&event, self.caps.budget, now_ms());
            (effects, state.progress(), echo)
        };
        if !echo {
            self.record(&event);
        }
        // The event, the cursor past it and whatever turn it closed are
        // written together.
        self.persist(&patch);
        self.notify();
        for effect in effects {
            match effect {
                Effect::Snapshot => self.refresh().await,
                Effect::Reconcile => self.reconcile().await,
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
            state.absorb_cost(snapshot.cost_micros);
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

    /// Inputs the session held unconsumed when the last connection ended
    /// may have been consumed since, on a record the replay starts after.
    async fn reconcile(&self) {
        let queued: Vec<String> = self.state().queued.iter().cloned().collect();
        match self.adapter.consumed(&self.handle, &queued).await {
            Ok(consumed) if !consumed.is_empty() => {
                let patch = {
                    let mut state = self.state();
                    for id in &consumed {
                        state.mark_consumed(id);
                    }
                    state.progress()
                };
                self.persist(&patch);
                self.notify();
            }
            Ok(_) => {}
            Err(err) => {
                self.state().last_error = Some(format!("reading what the session took: {err}"));
            }
        }
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
                    remember(&mut state.resume.answered, &mut state.answered, &call_id);
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
        state.synced = false;
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
                    // Until the next attach replays what was missed, a
                    // delivery would not know what the session holds.
                    self.state().synced = false;
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

/// Carry out `settled` on a session nothing is connected to. A session the
/// vendor ended (`terminated`) counts toward the replacement backoff.
fn carry_out(
    store: &Store,
    adapter: &dyn Adapter,
    agent: &AgentId,
    handle: Option<&Handle>,
    settled: Settled,
    terminated: bool,
) {
    let write = |patch: RemoteExecutionPatch| {
        if let Err(err) = store.update_remote_execution(agent, &patch) {
            eprintln!("anyagents-remote: {agent}: the session record was not written: {err}");
        }
    };
    let status_patch = |state: &str| RemoteExecutionPatch {
        status: Some(state.to_string()),
        ..RemoteExecutionPatch::default()
    };
    match settled {
        Settled::Kept => {}
        Settled::Parked => write(status_patch(status::PARKED)),
        Settled::Paused => {
            if let Some(handle) = handle
                && let Err(err) = block_on(adapter.interrupt(handle))
            {
                eprintln!(
                    "anyagents-remote: {agent}: the paused session was not interrupted: {err}"
                );
            }
            let _ = store.revoke_remote_sessions(agent, None);
            write(status_patch(status::PARKED));
        }
        Settled::Ended => {
            if let Some(handle) = handle
                && let Err(err) = block_on(adapter.stop(handle))
            {
                eprintln!("anyagents-remote: {agent}: the session was not stopped: {err}");
            }
            let _ = store.revoke_remote_sessions(agent, None);
            let terminations = store
                .remote_execution(agent)
                .ok()
                .flatten()
                .map_or(0, |row| row.terminations);
            write(RemoteExecutionPatch {
                status: Some(status::ENDED.into()),
                ended_at_ms: Some(now_ms()),
                terminations: terminated.then_some(terminations + 1),
                ..RemoteExecutionPatch::default()
            });
        }
    }
}

fn handle_of(row: &RemoteExecution) -> Option<Handle> {
    row.vendor_session.clone().map(|session| Handle {
        session,
        state: row.adapter_state.clone(),
    })
}

/// Settle an agent's remote session that no executor holds (a session a
/// previous kernel left for an agent that no longer runs, a parked session
/// whose agent was archived), by the agent's state.
pub fn settle(store: &Store, agent: &AgentId, adapter: &dyn Adapter) -> Settled {
    let Ok(Some(row)) = store.remote_execution(agent) else {
        return Settled::Kept;
    };
    if !row.is_live() {
        return Settled::Kept;
    }
    let alive = row.vendor_status.as_deref() != Some("terminated");
    let settled = decide(store, &row, alive);
    carry_out(
        store,
        adapter,
        agent,
        handle_of(&row).as_ref(),
        settled,
        !alive,
    );
    settled
}

/// End a session whose record is about to go with its agent: what is needed
/// to stop it is read now, and the vendor is called in the background.
pub fn end_before_delete(store: &Store, agent: &AgentId, adapter: Arc<dyn Adapter>) {
    let Ok(Some(row)) = store.remote_execution(agent) else {
        return;
    };
    let Some(handle) = handle_of(&row).filter(|_| row.is_live()) else {
        return;
    };
    let _ = store.revoke_remote_sessions(agent, None);
    let agent = agent.clone();
    spawn_blocking(move || {
        if let Err(err) = block_on(adapter.stop(&handle)) {
            eprintln!(
                "anyagents-remote: {agent}: the deleted agent's session was not stopped: {err}"
            );
        }
    });
}

/// Change an agent's spend cap from outside its executor (an operator call):
/// the vendor's cap for the session (the cap less what the agent's earlier
/// sessions spent) when the vendor holds it, and the record, which the
/// executor reads again when its hold comes due. A vendor session paused at
/// the old cap resumes on its own.
pub fn change_budget(
    store: &Store,
    agent: &AgentId,
    adapter: &dyn Adapter,
    cents: i64,
) -> Result<(), String> {
    let row = store
        .remote_execution(agent)
        .map_err(|err| err.to_string())?
        .ok_or_else(|| "the agent has no remote session".to_string())?;
    let allowance = session_allowance(cents, &row)?;
    if adapter.capabilities().budget == Budgeting::Vendor
        && row.is_live()
        && let Some(handle) = handle_of(&row)
    {
        block_on(adapter.set_budget(&handle, allowance)).map_err(|err| err.to_string())?;
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

fn dollars(micros: i64) -> String {
    let cents = micros / MICROS_PER_CENT;
    format!("${}.{:02}", cents / 100, cents % 100)
}

/// What the agent's current session may spend under a cap of `cents`: the
/// cap less the earlier sessions' spend, which must exceed what this session
/// already cost.
fn session_allowance(cents: i64, row: &RemoteExecution) -> Result<i64, String> {
    if cents <= 0 {
        return Err("a spend cap must be positive".into());
    }
    let allowance = cents.saturating_mul(MICROS_PER_CENT) - row.spent_before_micros;
    if allowance <= row.session_cost_micros() {
        return Err(format!(
            "a cap of {} is not above what the agent already spent ({})",
            dollars(cents.saturating_mul(MICROS_PER_CENT)),
            dollars(row.spent_micros())
        ));
    }
    Ok(allowance / MICROS_PER_CENT)
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
            match Self::reconnect(&config, &caps, &row, false) {
                Ok(executor) => return Ok(executor),
                // The session, or what it calls back with, is gone: it is
                // stopped before another replaces it.
                Err(AdapterError::Gone(why)) => {
                    eprintln!(
                        "anyagents-remote: {}: session {} is replaced: {why}",
                        agent.id,
                        row.vendor_session.as_deref().unwrap_or("-")
                    );
                    carry_out(
                        &store,
                        config.adapter.as_ref(),
                        &agent.id,
                        handle_of(&row).as_ref(),
                        Settled::Ended,
                        false,
                    );
                }
                Err(err) => {
                    return Err(format!(
                        "remote agent `{}` cannot reconnect: {err}",
                        agent.name
                    ));
                }
            }
        } else if let Some(old) = row.as_ref().filter(|row| row.is_live())
            && old.adapter == caps.adapter
        {
            // The agent moved to another model, or the vendor ended the
            // session: it is stopped before another replaces it.
            let terminated = old.vendor_status.as_deref() == Some("terminated");
            carry_out(
                &store,
                config.adapter.as_ref(),
                &agent.id,
                handle_of(old).as_ref(),
                Settled::Ended,
                terminated,
            );
        }
        let row = store
            .remote_execution(&agent.id)
            .map_err(|e| e.to_string())?;
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
        match Self::reconnect(&config, &caps, &row, true) {
            Ok(executor) => Ok(Some(executor)),
            Err(AdapterError::Gone(_)) => {
                // Left for the launch, which stops it and replaces it.
                let _ = config.store.update_remote_execution(
                    &config.agent.id,
                    &RemoteExecutionPatch {
                        vendor_status: Some("terminated".into()),
                        ..RemoteExecutionPatch::default()
                    },
                );
                Ok(None)
            }
            Err(err) => Err(format!(
                "remote agent `{}` cannot reconnect: {err}",
                config.agent.name
            )),
        }
    }

    fn reconnect(
        config: &Config,
        caps: &Capabilities,
        row: &RemoteExecution,
        takeover: bool,
    ) -> Result<Self, AdapterError> {
        let store = &config.store;
        let agent = &config.agent;
        let handle = handle_of(row).ok_or_else(|| AdapterError::Gone("no session".into()))?;
        let issued = Self::hand_grant(config, caps, &handle, row.token_session.as_deref())?;
        let mut patch = RemoteExecutionPatch {
            status: Some(status::ATTACHED.into()),
            token_session: issued.as_ref().map(|t| t.session_id.clone()),
            ..RemoteExecutionPatch::default()
        };
        if let Some(cents) = config.budget_cents
            && row.budget_cents != Some(cents)
        {
            let applied = match session_allowance(cents, row) {
                Ok(allowance) if caps.budget == Budgeting::Vendor => {
                    block_on(config.adapter.set_budget(&handle, allowance))
                        .map_err(|err| err.to_string())
                }
                Ok(_) => Ok(()),
                Err(why) => Err(why),
            };
            match applied {
                Ok(()) => patch.budget_cents = Some(cents),
                Err(why) => {
                    eprintln!(
                        "anyagents-remote: {}: the spend cap was not changed: {why}",
                        agent.id
                    );
                }
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
            .map_err(|e| AdapterError::Other(e.to_string()))?;
        let mut state = State::from_row(row, base, started);
        if let Some(cents) = patch.budget_cents {
            state.cap_cents = Some(cents);
        }
        if let Some(issued) = &issued {
            state.token_session = Some(issued.session_id.clone());
            state.token_expires_at_ms = issued.expires_at_ms;
        }
        if !takeover {
            state.note = None;
        }
        Ok(Self::start(config, caps.clone(), handle, state))
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
        let now = now_ms();
        // A session the vendor keeps ending is not replaced at the
        // supervisor's pace.
        if let Some(row) = previous
            && row.terminations > 0
            && let Some(ended) = row.ended_at_ms
        {
            let wait = TERMINATION_BACKOFF_MS
                .saturating_mul(1 << (row.terminations - 1).clamp(0, 16))
                .min(TERMINATION_BACKOFF_MAX_MS);
            if now < ended.saturating_add(wait) {
                return Err(format!(
                    "remote agent `{}`: its last {} session(s) ended at the vendor; the next starts \
                     no sooner than {} s from now",
                    agent.name,
                    row.terminations,
                    (ended + wait - now) / 1000
                ));
            }
        }
        // The cap is the agent's: a new session gets what is left of it.
        let spent = previous.map_or(0, RemoteExecution::spent_micros);
        let allowance_cents = match config.budget_cents {
            Some(cap) => {
                let left = cap.saturating_mul(MICROS_PER_CENT) - spent;
                if left < MICROS_PER_CENT {
                    return Err(format!(
                        "remote agent `{}` reached its spend cap: {} of {} spent; raise it with \
                         `anyagents agents budget {} <usd>`",
                        agent.name,
                        dollars(spent),
                        dollars(cap.saturating_mul(MICROS_PER_CENT)),
                        agent.name
                    ));
                }
                Some(left / MICROS_PER_CENT)
            }
            None => None,
        };
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
            budget_cents: allowance_cents,
        };
        // Each vendor-side object reaches the record as soon as it exists.
        let checkpoint_store = Arc::clone(store);
        let checkpoint_agent = agent.id.clone();
        let checkpoint = move |state: &Value| {
            let _ = checkpoint_store.update_remote_execution(
                &checkpoint_agent,
                &RemoteExecutionPatch {
                    adapter_state: Some(state.clone()),
                    ..RemoteExecutionPatch::default()
                },
            );
        };
        let handle = match block_on(
            config
                .adapter
                .create(&spec, &row.adapter_state, &checkpoint),
        ) {
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
            wake_at: AtomicI64::new(0),
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

    /// Change the agent's spend cap. A cap above what it has spent releases
    /// the mail held at the old one.
    pub fn set_budget(&self, cents: i64) -> Result<(), String> {
        let row = self
            .shared
            .store
            .remote_execution(&self.shared.agent_id)
            .map_err(|err| err.to_string())?
            .ok_or_else(|| "the agent has no remote session".to_string())?;
        let allowance = session_allowance(cents, &row)?;
        if self.shared.caps.budget == Budgeting::Vendor {
            block_on(
                self.shared
                    .adapter
                    .set_budget(&self.shared.handle, allowance),
            )
            .map_err(|err| err.to_string())?;
        }
        {
            let mut state = self.shared.state();
            state.cap_cents = Some(cents);
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

    /// What the session's record already says about a message: proven, in
    /// flight, or held by the session from an earlier process.
    fn known(&self, id: &str) -> Option<DeliverOutcome> {
        let mut state = self.shared.state();
        if let Some(proof) = state.proof(id) {
            return Some(match proof {
                Proof::Taken { turn_finished } => DeliverOutcome::Accepted { turn_finished },
                Proof::Duplicate => DeliverOutcome::Duplicate,
            });
        }
        if state.inflight.contains(id) {
            return Some(DeliverOutcome::InFlight);
        }
        if state.queued.contains(id) {
            state.uncertain.remove(id);
            state.inflight.insert(id.to_string());
            return Some(DeliverOutcome::InFlight);
        }
        None
    }

    /// Put off mail the vendor did not take: exponential backoff with
    /// jitter, never sooner than the vendor asked, and a wake when it is due.
    fn put_off(&self, retry_after_ms: Option<u64>, why: String) -> DeliverOutcome {
        let at = {
            let mut state = self.shared.state();
            state.retry_attempt = state.retry_attempt.saturating_add(1);
            let wait = crate::pacing::backoff_ms(state.retry_attempt, retry_after_ms);
            state.retry_at_ms = now_ms().saturating_add(i64::try_from(wait).unwrap_or(i64::MAX));
            state.last_error = Some(why);
            state.retry_at_ms
        };
        self.shared.wake_at(at);
        DeliverOutcome::Deferred
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
            !alive,
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
        let shared = Arc::clone(&self.shared);
        let id = message.id.as_str();
        let next_step = shared.caps.delivery.level == DeliveryLevel::NextStep;
        if let Some(outcome) = self.known(id) {
            return Ok(outcome);
        }
        let settle_lost = {
            let state = shared.state();
            if !state.alive {
                return Err(HarnessError::Dead);
            }
            if let Some((_, until)) = state.hold {
                return Ok(DeliverOutcome::Quota { reset_at_ms: until });
            }
            // Mail the vendor did not take waits out its backoff; a wake is
            // already set for when it is due.
            if now_ms() < state.retry_at_ms {
                return Ok(DeliverOutcome::Deferred);
            }
            state.uncertain.contains(id)
        };
        if !shared.wait_synced(DELIVER_SYNC_WAIT) {
            // The replay that follows the reconnect wakes the agent.
            return Ok(DeliverOutcome::Deferred);
        }
        // The replay may have just shown the session holds it.
        if let Some(outcome) = self.known(id) {
            return Ok(outcome);
        }
        let taken = |shared: &Shared| {
            let mut state = shared.state();
            state.retry_attempt = 0;
            state.retry_at_ms = 0;
            state.pending = true;
            if next_step {
                state.inflight.insert(id.to_string());
                DeliverOutcome::InFlight
            } else {
                state.admitted.insert(id.to_string());
                DeliverOutcome::Accepted {
                    turn_finished: false,
                }
            }
        };
        if settle_lost {
            let cursor = shared.state().resume.cursor.clone();
            match block_on(shared.adapter.holds(&shared.handle, id, cursor.as_deref())) {
                Ok(true) => {
                    shared.state().uncertain.remove(id);
                    return Ok(taken(&shared));
                }
                Ok(false) => {
                    shared.state().uncertain.remove(id);
                    // Before the cursor, the executor's own record answers.
                    if let Some(outcome) = self.known(id) {
                        return Ok(outcome);
                    }
                }
                Err(err) => return Ok(self.put_off(None, err.to_string())),
            }
        }
        let text = compose_message(id, &message.context, &message.text);
        match block_on(shared.adapter.deliver(&shared.handle, id, &text)) {
            Ok(Admission::Accepted { .. }) => {
                let mut state = shared.state();
                state.retry_attempt = 0;
                state.retry_at_ms = 0;
                state.pending = true;
                state.admitted.insert(id.to_string());
                Ok(DeliverOutcome::Accepted {
                    turn_finished: false,
                })
            }
            Ok(Admission::InFlight) => {
                let mut state = shared.state();
                state.retry_attempt = 0;
                state.retry_at_ms = 0;
                state.pending = true;
                state.inflight.insert(id.to_string());
                Ok(DeliverOutcome::InFlight)
            }
            Ok(Admission::Duplicate) => {
                let mut state = shared.state();
                state.mark_consumed(id);
                Ok(DeliverOutcome::Duplicate)
            }
            Ok(Admission::Refused {
                reason,
                message_specific,
            }) => Ok(DeliverOutcome::Rejected {
                reason,
                message_specific,
            }),
            Ok(Admission::Wait {
                reason,
                retry_after_ms,
            }) => Ok(self.put_off(retry_after_ms, reason)),
            Ok(Admission::Unknown(reason)) => {
                shared.state().uncertain.insert(id.to_string());
                Ok(self.put_off(None, reason))
            }
            Err(AdapterError::Auth(why)) => {
                shared.state().auth_fault = Some(why.clone());
                Err(HarnessError::Failed(why))
            }
            Err(AdapterError::Gone(why)) => {
                let mut state = shared.state();
                state.alive = false;
                state.terminated = Some(why);
                Err(HarnessError::Dead)
            }
            Err(err) => Ok(self.put_off(None, err.to_string())),
        }
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
        self.shared.state().interrupted = true;
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
            state.cap_cents = recorded_cap;
        }
        state.absorb_cost(snapshot.as_ref().and_then(|s| s.cost_micros));
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

    fn input(id: &str, consumed: bool) -> EventKind {
        EventKind::Input {
            request_id: Some(id.into()),
            text: String::new(),
            consumed,
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
            spent_before_micros: 0,
            terminations: 0,
            ended_at_ms: None,
            note: None,
            last_error: None,
            created_at_ms: 0,
            updated_at_ms: 0,
        };
        State::from_row(&row, RemoteTotals::default(), 0)
    }

    /// Apply events as the connection does.
    fn feed(state: &mut State, events: &[Event], budgeting: Budgeting) -> Vec<&'static str> {
        let mut done = Vec::new();
        for e in events {
            for effect in state.apply(e, budgeting, 0) {
                match effect {
                    Effect::Interrupt => done.push("interrupt"),
                    Effect::Snapshot => done.push("snapshot"),
                    Effect::Reconcile => done.push("reconcile"),
                    Effect::HostCall { .. } => done.push("host_call"),
                }
            }
        }
        done
    }

    #[test]
    fn a_turn_closes_with_the_event_that_ends_it_and_its_cost_follows() {
        let mut state = fresh();
        let usage = TurnUsage {
            input_tokens: 7,
            output_tokens: 3,
            ..TurnUsage::default()
        };
        feed(
            &mut state,
            &[
                event("e1", input("msg_a", false)),
                event("e2", EventKind::TurnStarted),
                event("e3", input("msg_a", true)),
                event("e4", EventKind::Usage(usage.clone())),
            ],
            Budgeting::Vendor,
        );
        // Nothing is reported while the turn runs.
        assert_eq!(state.totals, RemoteTotals::default());
        let done = feed(
            &mut state,
            &[event("e5", EventKind::TurnEnded(Stop::EndTurn))],
            Budgeting::Vendor,
        );
        assert_eq!(
            done,
            vec!["snapshot"],
            "the cost is read after the turn closed"
        );
        assert_eq!(state.totals.turns, 1);
        assert_eq!(state.totals.usage.input_tokens, 7);
        assert_eq!(state.resume.cursor.as_deref(), Some("e5"));
        let patch = state.progress();
        assert_eq!(patch.totals.unwrap().turns, 1, "closed in the same write");
        assert_eq!(patch.terminations, Some(0));
        // The vendor's price of the closed turn is reported on its own.
        state.absorb_cost(Some(50_000));
        assert_eq!(state.totals.usage.reported_cost_micros, Some(50_000));
        // Consumed by an earlier process: a duplicate; by this one: taken.
        assert_eq!(state.proof("msg_a"), Some(Proof::Duplicate));
        state.inflight.insert("msg_a".into());
        assert_eq!(
            state.proof("msg_a"),
            Some(Proof::Taken {
                turn_finished: false
            })
        );
        // An idle without a turn before it is no turn.
        feed(
            &mut state,
            &[event("e6", EventKind::TurnEnded(Stop::EndTurn))],
            Budgeting::Vendor,
        );
        assert_eq!(state.totals.turns, 1);
    }

    #[test]
    fn mail_an_interrupted_turn_never_consumed_is_offered_again() {
        let mut state = fresh();
        state.inflight.insert("msg_1".into());
        state.inflight.insert("msg_2".into());
        feed(
            &mut state,
            &[
                event("1", input("msg_1", false)),
                event("2", EventKind::TurnStarted),
                event("3", input("msg_1", true)),
                event("4", input("msg_2", false)),
            ],
            Budgeting::Vendor,
        );
        assert_eq!(state.proof("msg_2"), None, "queued is not consumed");
        state.interrupted = true;
        feed(
            &mut state,
            &[event("5", EventKind::TurnEnded(Stop::EndTurn))],
            Budgeting::Vendor,
        );
        assert!(state.proof("msg_1").is_some());
        assert!(!state.inflight.contains("msg_2") && !state.queued.contains("msg_2"));
        assert!(state.resume.queued.is_empty());
        assert_eq!(state.proof("msg_2"), None);
    }

    #[test]
    fn a_kernel_cap_counts_what_earlier_sessions_spent() {
        let mut state = fresh();
        state.spent_before_micros = 600_000; // 60 of the 100 cents
        let done = feed(
            &mut state,
            &[
                event("1", EventKind::TurnStarted),
                event(
                    "2",
                    EventKind::Usage(TurnUsage {
                        reported_cost_micros: Some(400_000),
                        ..TurnUsage::default()
                    }),
                ),
            ],
            Budgeting::Kernel,
        );
        assert_eq!(done, vec!["interrupt"]);
        assert!(matches!(state.hold, Some((HoldKind::Kernel, _))));
        assert!(state.interrupted);

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
