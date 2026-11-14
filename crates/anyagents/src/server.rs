//! HTTP/JSON mailbox API on a user-only unix socket.
//!
//! Every call is authenticated before it is routed: an agent instance presents
//! the mailbox capability the server minted for it, and the operator presents
//! its own token. A sender, an inbox, a bot or an agent name in a body is a
//! destination, never an identity.

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader};
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use anyagents_core::{Address, Agent, Error, Incoming, ReceiveOutcome, Store, now_ms};
use anyagents_gateway::MemoryRoutes;
use anyagents_installer::{CommandRunner, InstallError, LineSink, builtin_catalog, sign_in};
use anyagents_supervisor::{HealthBoard, Supervisor, SystemClock};
use axum::body::Body;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::StreamExt;
use serde::Deserialize;

use crate::api::{
    AgentView, AgentsResponse, ConversationResponse, ConversationTurnView, CreateAgentRequest,
    Delivery, GroupActionRequest, GroupView, GroupsResponse, InboxResponse, InteractRequest,
    InteractResponse, MessageView, OperatorInboxResponse, SendRequest, SendResponse,
};
use crate::auth::{self, Principal};
use crate::create::{self, CreateRequest};
use crate::identity::ServingIdentity;
use crate::operator;
use crate::vendor::{self, HarnessHost, VendorHarness};

mod file_cache;
#[cfg(test)]
mod latency_tests;
mod memory_backlog;
pub(crate) mod outbound_history;
mod profiles;
mod quota_rotation;
mod reap_queue;

pub(crate) use profiles::ServedProfile;

const SOCKET_NAME: &str = "anyagents.sock";
/// `sockaddr_un.sun_path` holds 108 bytes, including the trailing NUL.
const MAX_SOCKET_PATH: usize = 107;

/// WAL frame count above which a maintenance checkpoint is logged.
const WAL_REPORT_FRAMES: i64 = 16_384;

pub fn socket_path(data: &Path) -> PathBuf {
    data.join(SOCKET_NAME)
}

/// Run the server. Every harness runs in a keeper started from this binary,
/// so stopping, crashing or upgrading the server ends no agent's work.
pub fn run(data: &Path) -> Result<(), Error> {
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let host = crate::keeper::host().map_err(Error::Message)?;
    run_with_identity(
        data,
        Arc::new(Mutex::new(ProcessRunner)),
        stop_rx,
        Arc::new(ServingIdentity::capture()),
        host,
    )
}

/// Run the server with a sign-in runner the caller supplies, so no vendor
/// command is started. `stop` ends the server when the caller drops it.
/// Harnesses run in this process: a test binary has no keeper mode.
#[cfg(test)]
pub fn run_with(
    data: &Path,
    runner: Arc<Mutex<dyn CommandRunner + Send>>,
    stop: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), Error> {
    run_with_identity(
        data,
        runner,
        stop,
        Arc::new(ServingIdentity::capture()),
        HarnessHost::InProcess,
    )
}

fn run_with_identity(
    data: &Path,
    runner: Arc<Mutex<dyn CommandRunner + Send>>,
    mut stop: tokio::sync::oneshot::Receiver<()>,
    identity: Arc<ServingIdentity>,
    host: HarnessHost,
) -> Result<(), Error> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        tokio::select! {
            result = serve(data, runner, identity, host) => result,
            _ = &mut stop => Ok(()),
        }
    })
}

/// The supervisor of a serving kernel. However the kernel stops (a signal, an
/// error, a test dropping it), its harness processes are left to the next
/// kernel when they live in keepers; hosted in this process, they end.
struct Retaining(Option<Supervisor<VendorHarness>>, bool);

impl std::ops::Deref for Retaining {
    type Target = Supervisor<VendorHarness>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("supervisor until drop")
    }
}

impl Drop for Retaining {
    fn drop(&mut self) {
        if let Some(supervisor) = self.0.take() {
            // A keeper still starting is not waited for: it finishes its
            // start on its own, and the next kernel takes it over.
            if self.1 {
                anyagents_keeper::abandon_starts();
            }
            supervisor.retain_processes();
        }
    }
}

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) store: Arc<Store>,
    pub(crate) health: HealthBoard,
    pub(crate) sign_in: Arc<Mutex<dyn CommandRunner + Send>>,
    pub(crate) auth: auth::Mode,
    pub(crate) identity: Arc<ServingIdentity>,
    /// The last provider-profile listing, refreshed in the background.
    pub(crate) profiles: Arc<profiles::ProfileSnapshots>,
}

/// This process's capability key. Tests name one so a server and the fixture
/// it reads share the same key.
fn capability_key() -> Result<String, Error> {
    use std::io::Read as _;
    if let Ok(named) = std::env::var("ANYAGENTS_CAPABILITY_KEY")
        && named.len() >= 32
    {
        return Ok(named);
    }
    let mut bytes = [0u8; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn acquire_server_lock(data: &Path) -> Result<std::fs::File, Error> {
    std::fs::create_dir_all(data)?;
    let lock_path = data.join("server.lock");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd as _;
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            return Err(Error::Message(format!(
                "anyagents serve already running on {}",
                data.display()
            )));
        }
    }
    Ok(file)
}

fn active_seat_ids(store: &Store) -> Result<Vec<String>, Error> {
    Ok(store
        .list_live_agents()?
        .into_iter()
        .filter(|agent| agent.state != "archived")
        .map(|agent| agent.id.to_string())
        .collect())
}

async fn serve(
    data: &Path,
    sign_in: Arc<Mutex<dyn CommandRunner + Send>>,
    identity: Arc<ServingIdentity>,
    host: HarnessHost,
) -> Result<(), Error> {
    let _server_lock = acquire_server_lock(data)?;
    anyagents_toolchain::init_server(data, crate::sandbox::host_home_dir());
    crate::worktree_access::recover_inspections(data)?;
    let store = Arc::new(Store::open(data.join("state.db"))?);
    // The server lock excludes concurrent creation: no unadmitted row is in
    // flight, so each is a failed creation still holding its name.
    for (id, name) in store.release_unadmitted_names()? {
        eprintln!("released name `{name}` held by failed creation {id}");
    }
    crate::sandbox::reconcile_principals(&store)?;
    crate::host_memory::on_start(&store);
    let key = capability_key()?;
    store.hold_capability_key(&key)?;
    let agents = store.list_agents()?;
    // Keep small legacy deployments compatible without walking every archived
    // seat tree in a large registry. Wake/admission mints a capability on demand.
    let skip_archived_mint = agents.len() > 32;
    let mut mint_failures = Vec::new();
    for agent in &agents {
        if skip_archived_mint && agent.state == "archived" {
            continue;
        }
        if let Err(err) = crate::operator::ensure(&store, &agent.id) {
            mint_failures.push(format!("{}: {err}", agent.name));
        }
    }
    if !mint_failures.is_empty() {
        eprintln!(
            "warning: a capability was not minted for {}",
            mint_failures.join(", ")
        );
    }
    if crate::sandbox::seat_named() && crate::sandbox::configured_seat().is_none() {
        return Err(Error::Message(
            "a seat is named without its group: set ANYAGENTS_AGENT_UID, ANYAGENTS_AGENT_GID and \
             ANYAGENTS_SOCKET_GID together, or none"
                .into(),
        ));
    }
    // An operator token is the identity boundary; it holds only when every
    // harness it starts is in its own OS seat. Otherwise the boundary contract
    // says so, and no run claims it.
    let seats = active_seat_ids(&store)?;
    if operator::operator_token(data).is_some() && !crate::sandbox::launch_boundary(&seats) {
        return Err(Error::Message(
            "this server has an operator token, so it is the identity boundary, but no OS seat is \
             in place for its agents: configure ANYAGENTS_AGENT_UID, ANYAGENTS_AGENT_GID and \
             ANYAGENTS_SOCKET_GID and install /usr/local/bin/anyagents-setpriv, or start it without \
             an operator token on a single-user machine"
                .into(),
        ));
    }
    let auth = deployment_mode(data);
    if !auth.is_confined() {
        not_confined_note(data);
    }
    // The boundary contract is written and read back before the socket is
    // bound, so a client never reaches an API whose boundary is not in place.
    let boundary = anyagents_core::boundary::write(data)?;
    let path = socket_path(data);
    if path.as_os_str().len() > MAX_SOCKET_PATH {
        return Err(Error::Message(format!(
            "socket path is longer than {MAX_SOCKET_PATH} bytes"
        )));
    }
    // The gateway is up and every agent repointed before anything is bound or
    // started: a client that sees the socket must find a live origin, not the
    // port of the server that just stopped.
    let gateway = crate::gateway::start(data)?;
    // The API is bound before any vendor starts: an agent that is slow, silent
    // or wedged must never stop the server from answering.
    prepare_socket(&path)?;
    let api_listener = tokio::net::UnixListener::bind(&path)?;
    set_socket_mode(&path)?;
    identity.start_hash();
    if auth.is_confined() {
        crate::sandbox::share_socket_with_seats(&path);
    }
    let launch_store = Arc::clone(&store);
    // A harness kept from the last server on a gateway origin that moved
    // would send its token wherever that port now leads: it is replaced.
    let refused = if gateway.moved {
        gateway.agents
    } else {
        Vec::new()
    };
    let custody = crate::keeper::custody(data, &host, refused);
    let kept = matches!(host, HarnessHost::Keeper(_));
    let supervisor = Retaining(
        Some(
            Supervisor::<VendorHarness>::start_with_custody(
                Arc::clone(&store),
                Arc::new(SystemClock::new()),
                Arc::new(move |request| {
                    vendor::vendor_launch_hosted(request, &launch_store, &host)
                }),
                vendor::child_env(),
                custody,
            )
            .map_err(Error::Message)?,
        ),
        kept,
    );
    if auth.is_confined() {
        crate::sandbox::share_socket_with_seats(&data.join("wake.sock"));
    }
    let state = AppState {
        store: Arc::clone(&store),
        health: supervisor.board(),
        sign_in,
        auth,
        identity,
        profiles: Default::default(),
    };
    let _ = boundary;
    let listener = api_listener;
    let poller = crate::telegram::Poller::start(Arc::clone(&store), data.to_path_buf())
        .map_err(Error::Message)?;
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_none() && !crate::sandbox::is_isolated_test_env()
    {
        quota_rotation::start(Arc::clone(&store));
        let memory_backlog = memory_backlog::start(&store);
        let reaper_root = data.to_path_buf();
        let reaper_store = Arc::clone(&store);
        let reaper_health = state.health.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            let mut tick_count = 0u64;
            loop {
                interval.tick().await;
                tick_count = tick_count.wrapping_add(1);
                let run_verify = tick_count.is_multiple_of(15);
                let first_pass = tick_count == 1;
                let root = reaper_root.clone();
                let store = Arc::clone(&reaper_store);
                let health = reaper_health.clone();
                let memory_backlog = Arc::clone(&memory_backlog);
                let _ = tokio::task::spawn_blocking(move || {
                    // The daemon is the long-lived writer: keep the WAL bounded
                    // from here so it never grows while readers overlap.
                    match store.checkpoint_wal() {
                        Ok(c)
                            if c.passive_busy
                                || c.checkpointed_frames < c.log_frames
                                || c.log_frames > WAL_REPORT_FRAMES => eprintln!(
                            "anyagents-store: wal checkpoint log={} checkpointed={} busy={} truncated={}",
                            c.log_frames, c.checkpointed_frames, c.passive_busy, c.truncated
                        ),
                        Ok(_) => {}
                        Err(error) => eprintln!("anyagents-store: wal checkpoint failed: {error}"),
                    }
                    if let Err(error) = outbound_history::reconcile(&store) {
                        eprintln!("outbound history settlement remains pending: {error}");
                    }
                    if first_pass {
                        crate::vendor::settle_stale_spiron_holds(&store);
                    }
                    reap_archived_agents(&store, &root, &health);
                    consolidate_idle_agent_memory(&store, &root, &health, &memory_backlog);
                    if run_verify
                        && let Err(error) =
                            store.verify_one_host_history_source(anyagents_core::now_ms())
                    {
                        eprintln!("anyagents-host-history: deferred full verification: {error}");
                    }
                    crate::browser::reap_idle_browsers(&root);
                    if let Err(error) = crate::remote::rotate_due(&store) {
                        eprintln!("anyagents-remote: session rotation deferred: {error}");
                    }
                    record_live_quota_resets(&store, &root, &health);
                    crate::create::sync_fresher_bound_agent_credentials_in(&store, &root);
                    #[cfg(unix)]
                    crate::create::sync_canonical_skills(&root);
                })
                .await;
            }
        });
        let updater_root = data.to_path_buf();
        let updater_store = Arc::clone(&store);
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(6 * 3600));
            loop {
                interval.tick().await;
                let root = updater_root.clone();
                let store_for_update = Arc::clone(&updater_store);
                let _ = tokio::task::spawn_blocking(move || {
                    let catalog = match anyagents_installer::Catalog::load(&root) {
                        Ok(catalog) => catalog,
                        Err(err) => {
                            eprintln!("anyagents-updater: failed to load catalog overrides: {err}");
                            return;
                        }
                    };
                    for entry in catalog.harnesses().iter().chain(catalog.runtimes()) {
                        if !entry.source.is_managed()
                            || matches!(
                                entry.source,
                                anyagents_installer::source::Source::Unconfigured
                            )
                        {
                            continue;
                        }
                        match crate::install::update_harness_with_store(
                            &root,
                            &entry.id,
                            Arc::clone(&store_for_update)
                                as Arc<dyn anyagents_installer::LeaseView>,
                        ) {
                            Ok((before, after, true)) => {
                                eprintln!(
                                    "anyagents-updater: upgraded harness `{}` {} -> {}",
                                    entry.id, before, after
                                );
                            }
                            Ok((_, _, false)) => {}
                            Err(err) => {
                                eprintln!(
                                    "anyagents-updater: failed to update `{}`: {err}",
                                    entry.id
                                );
                            }
                        }
                    }
                })
                .await;
            }
        });
        start_schedule_loops(Arc::clone(&store), state.health.clone());
    }
    if let Some(addr) = webhook_listen_addr() {
        match tokio::net::TcpListener::bind(&addr).await {
            Ok(tcp_listener) => {
                if let Ok(local) = tcp_listener.local_addr() {
                    let addr_path = data.join("webhook.addr");
                    if let Err(err) = anyagents_harness::atomic_write_nonempty_file(
                        &addr_path,
                        format!("{local}\n").as_bytes(),
                        0o644,
                    ) {
                        eprintln!("cannot persist {}: {err}", addr_path.display());
                    }
                }
                let webhook_app = build_webhook_router(state.clone());
                tokio::spawn(async move {
                    let _ = axum::serve(tcp_listener, webhook_app).await;
                });
            }
            Err(err) => {
                eprintln!("warning: webhook TCP listener ({addr}) did not bind: {err}");
            }
        }
    }
    let app = build_router(state);
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
        })
        .await;
    // A long poll ends at Telegram, not here: signal the poller to stop first
    // so released long polls exit instead of re-entering a 30s wait, then
    // release every bot so each bot's last offset is written before shutdown.
    poller.signal_stop();
    if let Err(reason) = crate::telegram::poller::release_every_bot(
        &store,
        store.data_dir(),
        std::time::Duration::from_secs(30),
    ) {
        eprintln!("anyagents-telegram: shutdown did not release every bot: {reason}");
    }
    poller.stop();
    drop(supervisor);
    let _ = std::fs::remove_file(&path);
    result.map_err(Error::from)
}

pub(crate) fn has_explicit_archive_marker(
    data_dir: &Path,
    agent_id: &anyagents_core::AgentId,
) -> bool {
    anyagents_core::agent_config_dir(data_dir, agent_id)
        .join(".anyagents-archived")
        .exists()
}

pub(crate) fn is_explicitly_archived(data_dir: &Path, agent: &Agent) -> bool {
    agent.state == "archived" || has_explicit_archive_marker(data_dir, &agent.id)
}

/// The caller holds this agent's lifecycle lock; descendant locks follow only
/// downward, so a cascading archive cannot deadlock a direct child wake.
pub(crate) fn mark_explicitly_archived_locked(
    store: &Store,
    data_dir: &Path,
    agent: &Agent,
) -> Result<Vec<anyagents_core::AgentId>, anyagents_core::Error> {
    crate::worktree_access::revoke_recorded(data_dir, &agent.id)?;
    let mut archived = vec![agent.id.clone()];
    mark_archive_generation_locked(store, data_dir, agent);
    let mut queue = std::collections::VecDeque::new();
    queue.push_back(agent.id.clone());
    let mut seen = std::collections::HashSet::new();
    seen.insert(agent.id.clone());
    while let Some(pid) = queue.pop_front() {
        let Ok(mut children) = store.children_of(&pid) else {
            continue;
        };
        children.sort_by(|a, b| a.name.cmp(&b.name));
        for child in &children {
            if seen.insert(child.id.clone()) {
                queue.push_back(child.id.clone());
                let Ok(_child_lifecycle) = store.try_lock_agent_lifecycle(&child.id) else {
                    continue;
                };
                let Ok(child) = store.set_agent_state(&child.id, "archived") else {
                    continue;
                };
                crate::worktree_access::revoke_recorded(data_dir, &child.id)?;
                mark_archive_generation_locked(store, data_dir, &child);
                archived.push(child.id.clone());
            }
        }
    }
    Ok(archived)
}

/// Mark only this row while its lifecycle lock is held; descendant cleanup is
/// handled separately by maintenance.
fn mark_archive_generation_locked(store: &Store, data_dir: &Path, agent: &Agent) {
    let cfg = anyagents_core::agent_config_dir(data_dir, &agent.id);
    let _ = std::fs::create_dir_all(&cfg);
    let _ = std::fs::remove_file(cfg.join(".anyagents-reaped"));
    let archived_marker = cfg.join(".anyagents-archived");
    if let Err(err) = anyagents_harness::atomic_write_nonempty_file(
        &archived_marker,
        format!("{}:{}\n", now_ms(), anyagents_core::AgentId::generate()).as_bytes(),
        0o644,
    ) {
        eprintln!("cannot persist {}: {err}", archived_marker.display());
    }
    if agent.parent_id.is_some() {
        reap_queue::enqueue(data_dir, &agent.id);
    }
    if let Ok(schedules) = store.list_schedules(Some(&agent.id), true) {
        let any = !schedules.is_empty();
        for sched in schedules {
            let _ = store.cancel_schedule(Some(&agent.id), &sched.id);
        }
        if any {
            crate::schedule::schedules_changed(store.data_dir());
        }
    }
    crate::telegram::detach_archived_agent_from_telegram_groups(store, data_dir, agent, true);
}

/// The caller holds the lifecycle lock. The marker identifies this archive,
/// not directory existence: retained dirty worktrees are not pending cleanup.
pub(crate) fn reap_archived_agent_locked(
    store: &Store,
    data_dir: &Path,
    agent_id: &anyagents_core::AgentId,
) {
    let agent = match store.agent(agent_id) {
        Ok(agent) => agent,
        Err(Error::AgentNotFound(_)) => return reap_queue::dequeue(data_dir, agent_id),
        Err(_) => return reap_queue::note_failure(data_dir, agent_id, now_ms()),
    };
    if agent.state != "archived" || agent.parent_id.is_none() {
        return reap_queue::dequeue(data_dir, agent_id);
    }
    let cfg = anyagents_core::agent_config_dir(data_dir, agent_id);
    let archived = cfg.join(".anyagents-archived");
    if std::fs::read(&archived).is_ok_and(|generation| generation.is_empty())
        || std::fs::symlink_metadata(&archived)
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    {
        // Upgrade legacy or interrupted empty archive markers under the wake lock.
        mark_archive_generation_locked(store, data_dir, &agent);
    }
    let generation = std::fs::read(&archived).unwrap_or_default();
    if generation.is_empty() {
        return reap_queue::note_failure(data_dir, agent_id, now_ms());
    }
    let reaped = cfg.join(".anyagents-reaped");
    if std::fs::read(&reaped).is_ok_and(|done| done == generation) {
        return reap_queue::dequeue(data_dir, agent_id);
    }
    if !reap_subagent_ephemeral_dirs_in(store, data_dir, agent_id) {
        return reap_queue::note_failure(data_dir, agent_id, now_ms());
    }
    match anyagents_harness::atomic_write_nonempty_file(&reaped, &generation, 0o644) {
        Ok(()) => reap_queue::dequeue(data_dir, agent_id),
        Err(err) => {
            eprintln!("cannot persist {}: {err}", reaped.display());
            reap_queue::note_failure(data_dir, agent_id, now_ms());
        }
    }
}

/// The maintenance worker runs this at startup and each minute without waiting
/// for an agent lock. It visits only archives whose cleanup is owed and due.
fn reap_archived_agents(store: &Store, data_dir: &Path, health: &HealthBoard) {
    reap_archived_agents_at(store, data_dir, health, now_ms());
}

fn reap_archived_agents_at(store: &Store, data_dir: &Path, health: &HealthBoard, now: i64) {
    reap_queue::backfill(store, data_dir);
    for agent_id in reap_queue::due(data_dir, now) {
        let Ok(_lifecycle) = store.try_lock_agent_lifecycle(&agent_id) else {
            continue;
        };
        // A wake followed by a new self-archive may have started a process
        // since the archive. Check current health only after taking the lock.
        if health.get(&agent_id).is_some_and(|live| live.process_alive) {
            continue;
        }
        // Re-read under the lock: the queue may predate a direct wake.
        reap_archived_agent_locked(store, data_dir, &agent_id);
    }
}

/// Consolidate idle agents' topic files without changing the active session's
/// immutable snapshot. Updated files are pinned only at the next explicit session.
/// Only agents in the backlog are visited; one leaves once its consolidation
/// has caught up with its mailbox.
fn consolidate_idle_agent_memory(
    store: &Store,
    data_dir: &Path,
    health: &HealthBoard,
    backlog: &memory_backlog::MemoryBacklog,
) {
    let mut llm_consolidated_count = 0usize;
    for agent_id in backlog.pending() {
        let agent = match store.agent(&agent_id) {
            Ok(agent) if agent.state != "archived" => agent,
            Err(err) if !matches!(err, Error::AgentNotFound(_)) => continue,
            _ => {
                backlog.settle(&agent_id);
                continue;
            }
        };
        let agent_root = data_dir.join("agents").join(agent.id.as_str());
        if !agent_root.join("memory").is_dir() {
            backlog.settle(&agent_id);
            continue;
        }
        let Ok(_lifecycle) = store.try_lock_agent_lifecycle(&agent.id) else {
            continue;
        };
        if health.get(&agent.id).is_some_and(|live| live.process_alive) {
            continue;
        }
        if store.has_open_turn(&agent.id).unwrap_or(true) {
            continue;
        }
        let allow_llm = llm_consolidated_count < 1;
        let consolidated = store.consolidate_agent_sleep_memory_with_options(&agent.id, allow_llm);
        if let Ok((_, true)) = consolidated {
            llm_consolidated_count += 1;
        }
        if consolidated.is_ok() && store.sleep_memory_caught_up(&agent.id).unwrap_or(false) {
            backlog.settle(&agent_id);
        }
    }
}

/// Record the reset of each profile whose live agent the supervisor reports
/// quota-held, so profile listings and rotation see it. The board is in memory:
/// a fleet with no quota hold costs one indexed read.
fn record_live_quota_resets(store: &Store, data_dir: &Path, health: &HealthBoard) {
    let now = now_ms();
    for agent in store.list_live_agents().unwrap_or_default() {
        if let Some(reset_ms) = health
            .get(&agent.id)
            .and_then(|live| live.quota_reset_at_ms)
            .filter(|&reset_ms| reset_ms > now)
        {
            create::record_profile_quota_reset(data_dir, &agent, reset_ms);
        }
    }
}

pub(crate) fn clear_explicitly_archived(data_dir: &Path, agent_id: &anyagents_core::AgentId) {
    let cfg = anyagents_core::agent_config_dir(data_dir, agent_id);
    let _ = std::fs::remove_file(cfg.join(".anyagents-reaped"));
    let _ = std::fs::remove_file(cfg.join(".anyagents-archived"));
    reap_queue::dequeue(data_dir, agent_id);
}

/// Local timers and network watches run on separate timers, so an in-flight
/// external request cannot hold a local tick. Each sleeps until its next due
/// time or a schedule change.
fn start_schedule_loops(
    store: Arc<Store>,
    health: HealthBoard,
) -> (tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>) {
    crate::schedule::start_loops(store, health)
}

/// How this server authenticates. A machine started without an operator token
/// serves the agents on its own machine, and names them by the route it serves:
/// its data directory, as a start script exports it to every harness. With an
/// operator token the server is the identity boundary and reads nothing from a
/// name.
fn deployment_mode(data: &Path) -> auth::Mode {
    match auth::Mode::from_env(data) {
        auth::Mode::Confined {
            operator_digest: None,
        } => match std::fs::canonicalize(data) {
            Ok(dir) => {
                let dir = dir.display().to_string();
                // [`not_confined_note`] says it once, on stderr, and the note
                // names no secret.
                auth::Mode::Trusted {
                    data_dir: std::path::PathBuf::from(dir),
                }
            }
            Err(_) => auth::Mode::Confined {
                operator_digest: None,
            },
        },
        other => other,
    }
}

/// The one line a single-user deployment says at start.
pub fn not_confined_note(data: &Path) {
    let dir = std::fs::canonicalize(data).unwrap_or_else(|_| data.to_path_buf());
    eprintln!(
        "warning: single-user route: an unauthenticated call acts as the agent it names on {}; start with an operator token for the identity boundary",
        dir.display()
    );
}

/// Mint an agent's capability after its instance was made (a create, a new
/// sign-in), so its harness starts with one and the last one is retired.
fn capability_mark(store: Arc<Store>) -> impl Fn() -> Result<(), Error> + Send + Sync + 'static {
    move || {
        let agents = store.list_agents()?;
        for agent in &agents {
            crate::operator::mint(&store, &agent.id)?;
        }
        Ok(())
    }
}

/// `POST /v1/operator/agents/{name}/capability`: the operator or a root admin
/// gives one agent a fresh capability, retiring the last one. The value is
/// written into that agent's own config directory and never returned.
async fn mint_capability(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        require_admin_or_operator(&state.store, &principal, "agents.mint_capability", &name)?;
        let agent = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        operator::mint(&state.store, &agent.id).map_err(ApiError::from_store)?;
        Ok(Json(
            serde_json::json!({ "agent": agent.name, "minted": true }),
        ))
    })
    .await
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RevokeRemoteSessionsRequest {
    /// One session to revoke; every session of the agent when absent.
    #[serde(default)]
    session_id: Option<String>,
}

/// `POST /v1/operator/agents/{name}/remote-sessions/revoke`: the operator or a
/// root admin ends an agent's remote sessions now. Every token of a revoked
/// session stops authenticating on its next call.
async fn revoke_remote_sessions_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<RevokeRemoteSessionsRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        require_admin_or_operator(
            &state.store,
            &principal,
            "agents.revoke_remote_sessions",
            &name,
        )?;
        let agent = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        let session = body
            .session_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let revoked = crate::remote::revoke(&state.store, &agent.id, session)
            .map_err(ApiError::from_store)?;
        Ok(Json(serde_json::json!({
            "agent": agent.name,
            "session_id": session,
            "revoked": revoked,
        })))
    })
    .await
}

/// `GET /v1/webhooks/healthz` on `anyagents.sock`: the serving identity
/// (version, source SHA, binary SHA-256) for the operator and the release
/// tooling. The public listener answers [`public_healthz`] instead.
async fn webhook_healthz(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        Json(state.identity.status()),
    )
}

/// `GET /`, `/healthz` and `/v1/webhooks/healthz` on the public TCP listener:
/// readiness only. The build identity names the exact binary this server
/// runs, which tells a scanner which advisories apply, so it stays on the
/// Unix socket.
async fn public_healthz() -> impl IntoResponse {
    (
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({ "ok": true })),
    )
}

/// `POST /v1/webhooks/telegram/{channel_id}`: receives one update pushed by
/// Telegram, verifies `X-Telegram-Bot-Api-Secret-Token` against the channel's
/// derived HMAC-SHA256 secret, and routes the update into the durable mailbox.
async fn telegram_webhook(
    State(state): State<AppState>,
    AxumPath(channel_id): AxumPath<String>,
    headers: axum::http::HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let secret = headers
        .get("x-telegram-bot-api-secret-token")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let store = Arc::clone(&state.store);
    spawn_store(move || {
        crate::telegram::receive_webhook(
            &store,
            store.data_dir(),
            &channel_id,
            secret.as_deref(),
            &payload,
        )
        .map_err(|err| match err {
            crate::telegram::WebhookError::Unauthorized => {
                ApiError::new(StatusCode::UNAUTHORIZED, "invalid webhook secret token")
            }
            crate::telegram::WebhookError::NotFound(why) => {
                ApiError::new(StatusCode::NOT_FOUND, why)
            }
            crate::telegram::WebhookError::BadRequest(why) => {
                ApiError::new(StatusCode::BAD_REQUEST, why)
            }
            crate::telegram::WebhookError::Store(why) => {
                ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, why)
            }
        })?;
        Ok(Json(serde_json::json!({ "ok": true })))
    })
    .await
}

fn webhook_listen_addr() -> Option<String> {
    if let Ok(addr) = std::env::var("ANYAGENTS_WEBHOOK_LISTEN") {
        let trimmed = addr.trim();
        if trimmed.is_empty() || trimmed == "0" || trimmed.eq_ignore_ascii_case("off") {
            return None;
        }
        return Some(trimmed.to_string());
    }
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_some()
        || std::env::var_os("ANYAGENTS_TELEGRAM_API").is_some()
    {
        return None;
    }
    Some("0.0.0.0:8788".to_string())
}

pub(crate) fn build_webhook_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(public_healthz))
        .route("/healthz", get(public_healthz))
        .route("/v1/webhooks/healthz", get(public_healthz))
        .route("/v1/webhooks/telegram/{channel_id}", post(telegram_webhook))
        .route("/v1/webhooks/mcp", get(mcp_get_route).post(mcp_post_route))
        .route("/v1/mcp", get(mcp_get_route).post(mcp_post_route))
        .route("/mcp", get(mcp_get_route).post(mcp_post_route))
        .with_state(state)
}

pub(crate) fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/v1/agents", get(list_agents).post(create_agent_route))
        .route("/v1/agents/{name}", get(get_agent))
        .route("/v1/agents/{name}/admin", post(set_admin_route))
        .route("/v1/agents/{name}/rename", post(rename_agent_route))
        .route("/v1/agents/{name}/role", post(role_agent_route))
        .route("/v1/agents/{name}/transfer", post(transfer_agent_route))
        .route("/v1/agents/{name}/model", post(model_route))
        .route("/v1/agents/{name}/harness", post(harness_route))
        .route("/v1/agents/{name}/session/reset", post(reset_session_route))
        .route(
            "/v1/agents/{name}/schedules",
            get(list_schedules_route).post(schedules_action_route),
        )
        .route(
            "/v1/schedules",
            get(list_self_schedules_route).post(self_schedules_action_route),
        )
        .route(
            "/v1/agents/{name}/description",
            get(get_description_route).post(set_description_route),
        )
        .route(
            "/v1/agents/{name}/instructions",
            get(get_description_route).post(set_description_route),
        )
        .route("/v1/browser", post(browser_route))
        .route("/v1/web", post(web_route))
        .route("/v1/profiles", get(profiles_route).post(set_profile_route))
        .route("/v1/profiles/models", get(profile_models_route))
        .route(
            "/v1/subagent-defaults",
            get(get_subagent_defaults_route).post(set_subagent_defaults_route),
        )
        .route("/v1/channels", get(channels_route).post(add_channel_route))
        .route("/v1/agents/{name}/inbox", get(inbox))
        .route("/v1/agents/{name}/conversation", get(conversation_route))
        .route(
            "/v1/agents/{name}/conversation/redact",
            post(redact_conversation_route),
        )
        .route("/v1/agents/{name}/archive", post(archive_agent_route))
        .route("/v1/agents/{name}/pause", post(pause_agent_route))
        .route("/v1/agents/{name}/wake", post(wake_agent_route))
        .route(
            "/v1/agents/{name}",
            axum::routing::delete(delete_agent_route),
        )
        .route(
            "/v1/groups",
            get(list_groups_route).post(group_action_route),
        )
        .route("/v1/send", post(send))
        .route("/v1/interact", post(interact_route))
        .route("/v1/harnesses/{id}/sign-in", post(sign_in_route))
        .route(
            "/v1/operator/agents/{name}/capability",
            post(mint_capability),
        )
        .route("/v1/operator/agents/{name}/inbox", get(operator_inbox))
        .route("/v1/operator/inbox", get(operator_reports))
        .route(
            "/v1/operator/agents/{name}/remote-sessions/revoke",
            post(revoke_remote_sessions_route),
        )
        .route("/v1/whoami", get(whoami))
        .route("/v1/usage", get(usage_report))
        .route("/v1/outcomes", get(outcome_report))
        .route("/v1/agents/{name}/verdict", post(verdict_route))
        .route("/v1/skills/reads", post(skill_read_route))
        .route("/v1/outbound", get(outbound_list_route))
        .route("/v1/outbound/{key}/retry", post(outbound_retry_route))
        .route("/v1/outbound/{key}/discard", post(outbound_discard_route))
        .route("/v1/webhooks/healthz", get(webhook_healthz))
        .route("/v1/webhooks/telegram/{channel_id}", post(telegram_webhook))
        .route("/v1/webhooks/mcp", get(mcp_get_route).post(mcp_post_route))
        .route("/v1/mcp", get(mcp_get_route).post(mcp_post_route))
        .route("/mcp", get(mcp_get_route).post(mcp_post_route))
        .with_state(state)
}

#[derive(Debug, Default, Deserialize)]
struct McpRouteQuery {
    #[serde(default)]
    profile: Option<String>,
}

/// Who made one MCP call, and with which kind of credential.
struct McpCaller {
    agent: Agent,
    /// What the call's tools present on the internal socket: the agent's own
    /// local capability, which the server reads and which never leaves this
    /// host.
    internal_token: String,
    /// The remote session the presented token belongs to.
    remote: Option<anyagents_core::RemoteSessionGrant>,
}

impl McpCaller {
    /// The tool profile this call gets: `remote`, or `coordination` when it
    /// asks, for every credential kind. Over HTTP the tools run inside the
    /// server process as the server's own user, so `full` is never served
    /// here (403); it exists only over stdio, where the tool server runs as
    /// the agent.
    fn profile(&self, requested: Option<&str>) -> Result<crate::mcp::McpProfile, ApiError> {
        crate::mcp::McpProfile::resolve_http(requested).map_err(|msg| {
            let known = requested.and_then(crate::mcp::McpProfile::from_str_opt);
            let status = if known.is_some() {
                StatusCode::FORBIDDEN
            } else {
                StatusCode::BAD_REQUEST
            };
            ApiError::new(status, msg)
        })
    }
}

/// Authenticate one MCP call over HTTP. Only a remote session token (bound to
/// one agent and session, unexpired, unrevoked) is accepted. A valid local
/// mailbox capability is refused with 403, and anything else with 401.
fn authenticate_mcp_request(
    store: &Store,
    headers: &axum::http::HeaderMap,
) -> Result<McpCaller, ApiError> {
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| {
            let trimmed = raw.trim();
            trimmed
                .strip_prefix("Bearer ")
                .or_else(|| trimmed.strip_prefix("bearer "))
        })
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or_else(|| {
            headers
                .get(crate::operator::AGENT_HEADER)
                .and_then(|v| v.to_str().ok())
                .map(str::trim)
                .filter(|s| !s.is_empty())
        })
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::UNAUTHORIZED,
                "missing MCP capability token (expected `Authorization: Bearer <mcp-capability>` or `x-anyagents-agent`)",
            )
        })?;
    if token.starts_with(anyagents_core::REMOTE_SESSION_TOKEN_PREFIX) {
        let unauthenticated = || {
            ApiError::new(
                StatusCode::UNAUTHORIZED,
                "invalid, expired or revoked remote session token",
            )
        };
        let grant = store
            .authenticate_remote_session(token)
            .map_err(|_| unauthenticated())?;
        let agent = store
            .agent(&grant.agent_id)
            .map_err(|_| unauthenticated())?;
        operator::ensure(store, &agent.id).map_err(ApiError::from_store)?;
        let internal_token = crate::mcp::capability_of(&anyagents_core::agent_config_dir(
            store.data_dir(),
            &agent.id,
        ))
        .map_err(ApiError::from_store)?;
        return Ok(McpCaller {
            agent,
            internal_token,
            remote: Some(grant),
        });
    }
    // MCP over HTTP runs the tools inside the server process, as the server's
    // own user, on every listener including the Unix socket. A local agent's
    // tools run over stdio (`anyagents mcp`) as the agent itself, so its
    // capability is never accepted here.
    if store.authenticate_agent(token).is_ok() {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "MCP over HTTP accepts only remote session tokens; a local agent's tools run over stdio \
             (`anyagents mcp`), as the agent itself",
        ));
    }
    Err(ApiError::new(
        StatusCode::UNAUTHORIZED,
        "invalid or expired MCP capability token",
    ))
}

fn ensure_internal_socket_listener(state: &AppState) -> PathBuf {
    let data_dir = state.store.data_dir();
    let sock = socket_path(data_dir);
    if !sock.exists()
        && let Ok(listener) = tokio::net::UnixListener::bind(&sock)
    {
        let app = build_router(state.clone());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
    }
    sock
}

async fn mcp_get_route(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Query(query): Query<McpRouteQuery>,
) -> Result<axum::response::Response, ApiError> {
    let presented = headers.clone();
    let (caller, profile) = spawn_store(move || {
        let caller = authenticate_mcp_request(&state.store, &presented)?;
        let profile = caller.profile(query.profile.as_deref())?;
        Ok((caller, profile))
    })
    .await?;
    let agent = &caller.agent;
    let remote_session = caller.remote.as_ref().map(|grant| {
        serde_json::json!({
            "session_id": grant.session_id,
            "expires_at_ms": grant.expires_at_ms,
        })
    });
    let accept = headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if accept.contains("application/json") && !accept.contains("text/event-stream") {
        let body = serde_json::json!({
            "ok": true,
            "transport": "streamable-http",
            "protocolVersion": "2025-03-26",
            "agent": agent.name,
            "harness": agent.harness,
            "profile": profile.as_str(),
            "tools": profile.exposed_tools(),
            "remote_session": remote_session,
        });
        return Ok(axum::response::IntoResponse::into_response(Json(body)));
    }
    let event_payload = serde_json::json!({
        "type": "endpoint",
        "transport": "streamable-http",
        "protocolVersion": "2025-03-26",
        "agent": agent.name,
        "profile": profile.as_str(),
    });
    let sse_body = format!(": anyagents-mcp\nevent: endpoint\ndata: {event_payload}\n\n");
    Ok(axum::http::Response::builder()
        .status(StatusCode::OK)
        .header(
            axum::http::header::CONTENT_TYPE,
            "text/event-stream; charset=utf-8",
        )
        .header(axum::http::header::CACHE_CONTROL, "no-cache")
        .body(axum::body::Body::from(sse_body))
        .unwrap_or_else(|_| {
            axum::response::IntoResponse::into_response(StatusCode::INTERNAL_SERVER_ERROR)
        }))
}

async fn mcp_post_route(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Query(query): Query<McpRouteQuery>,
    Json(payload): Json<serde_json::Value>,
) -> Result<axum::response::Response, ApiError> {
    let mcp = spawn_store(move || {
        let caller = authenticate_mcp_request(&state.store, &headers)?;
        let profile = caller.profile(query.profile.as_deref())?;
        let McpCaller {
            agent,
            internal_token: token,
            ..
        } = caller;
        let config_dir = anyagents_core::agent_config_dir(state.store.data_dir(), &agent.id);
        let sock = ensure_internal_socket_listener(&state);
        let _ = std::fs::create_dir_all(&config_dir);
        Ok(crate::mcp::AgentMcp::new_for_http(
            crate::mcp::AgentRef {
                id: agent.id.clone(),
                name: agent.name.clone(),
                workdir: agent.workdir.as_deref().map(PathBuf::from),
            },
            sock,
            config_dir,
            token,
            profile,
        ))
    })
    .await?;
    // A tool call mixes file, process and socket work into its async steps
    // (`crate::mcp`), so the whole call is driven from a blocking-pool thread:
    // whatever it waits on holds that thread, never an async worker.
    let runtime = tokio::runtime::Handle::current();
    let answered =
        tokio::task::spawn_blocking(move || runtime.block_on(mcp.handle_jsonrpc_request(payload)))
            .await
            .map_err(|err| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    match answered {
        Some(response_json) => Ok(axum::response::IntoResponse::into_response((
            StatusCode::OK,
            Json(response_json),
        ))),
        None => Ok(axum::response::IntoResponse::into_response(
            StatusCode::ACCEPTED,
        )),
    }
}

/// Query parameters for `GET /v1/usage`. `agent` names a target by name or id
/// (or `self`); `include_descendants` (alias `subtree`) widens an explicit
/// target to its subtree; `since_ms`/`until_ms` bound the recording window
/// inclusively; `days` is a relative window from now; `group_by` is one of
/// `agent`, `day`, `model`, `harness`.
#[derive(Debug, Default, Deserialize)]
struct UsageQuery {
    agent: Option<String>,
    include_descendants: Option<bool>,
    subtree: Option<bool>,
    since_ms: Option<i64>,
    until_ms: Option<i64>,
    days: Option<i64>,
    group_by: Option<String>,
}

/// `GET /v1/usage`: structured usage aggregation over the daemon socket. The
/// caller is resolved from its credential, never from a parameter: the
/// operator, a public-scope caller and a root caller may aggregate the whole
/// workspace or any target, while every other caller is confined to itself
/// and its causal descendants, and any wider request fails closed.
/// Every agent, and the ones `principal` may read usage and outcomes for:
/// the operator, a public-scope caller and a root caller may read the whole
/// workspace or any target; every other caller only itself and its causal
/// descendants, and any wider request fails closed.
fn usage_scope(
    store: &Store,
    principal: &Principal,
    target: Option<&str>,
    include_descendants: Option<bool>,
) -> Result<(Vec<Agent>, Vec<anyagents_core::AgentId>), ApiError> {
    let caller = match principal {
        Principal::Operator => None,
        Principal::Agent { agent_id, .. } => Some(
            store
                .agent(agent_id)
                .map_err(|_| ApiError::forbidden("authenticated agent no longer exists"))?,
        ),
    };
    let agents = store.list_agents().map_err(ApiError::from_store)?;
    let agent_ids =
        anyagents_core::resolve_usage_agents(caller.as_ref(), &agents, target, include_descendants)
            .map_err(|err| match err {
                anyagents_core::UsageAccessError::Forbidden(why) => ApiError::forbidden(why),
                anyagents_core::UsageAccessError::Invalid(why) => {
                    ApiError::new(StatusCode::BAD_REQUEST, why)
                }
            })?;
    Ok((agents, agent_ids))
}

async fn usage_report(
    principal: Principal,
    State(state): State<AppState>,
    Query(query): Query<UsageQuery>,
) -> Result<Json<anyagents_core::UsageReport>, ApiError> {
    spawn_store(move || {
        let (agents, agent_ids) = usage_scope(
            &state.store,
            &principal,
            query.agent.as_deref(),
            query.include_descendants.or(query.subtree),
        )?;
        let group_by = match query.group_by.as_deref() {
            None => anyagents_core::UsageGroupBy::Agent,
            Some(raw) => anyagents_core::UsageGroupBy::parse(raw)
                .map_err(|why| ApiError::new(StatusCode::BAD_REQUEST, why))?,
        };
        let filter = anyagents_core::UsageFilter {
            since_ms: query.since_ms,
            until_ms: query.until_ms,
            days: query.days,
            group_by,
        };
        let names = agents
            .iter()
            .map(|agent| (agent.id.clone(), agent.name.clone()))
            .collect();
        let records = state.store.list_usage(None).map_err(ApiError::from_store)?;
        let report =
            anyagents_core::aggregate_usage(&records, &agent_ids, &names, filter, now_ms())
                .map_err(|why| ApiError::new(StatusCode::BAD_REQUEST, why))?;
        Ok(Json(report))
    })
    .await
}

/// `GET /v1/outcomes`: outcomes, recovery and cost per successful outcome
/// (ADR 0008), over the same caller boundary and query parameters as
/// `GET /v1/usage`. `group_by` is `model` (harness and model, the default),
/// `harness`, `role`, `profile`, `agent` or `skill`. The window bounds when
/// outcomes were observed; their cost may predate it.
async fn outcome_report(
    principal: Principal,
    State(state): State<AppState>,
    Query(query): Query<UsageQuery>,
) -> Result<Json<anyagents_core::OutcomeReport>, ApiError> {
    spawn_store(move || {
        let (_, agent_ids) = usage_scope(
            &state.store,
            &principal,
            query.agent.as_deref(),
            query.include_descendants.or(query.subtree),
        )?;
        let group_by =
            anyagents_core::OutcomeGroupBy::parse(query.group_by.as_deref().unwrap_or(""))
                .map_err(|why| ApiError::new(StatusCode::BAD_REQUEST, why))?;
        let now = now_ms();
        let (since, until) = anyagents_core::UsageFilter {
            since_ms: query.since_ms,
            until_ms: query.until_ms,
            days: query.days,
            group_by: anyagents_core::UsageGroupBy::Agent,
        }
        .window(now)
        .map_err(|why| ApiError::new(StatusCode::BAD_REQUEST, why))?;
        let report = state
            .store
            .outcome_report(&agent_ids, since, until.unwrap_or(now), group_by)
            .map_err(ApiError::from_store)?;
        Ok(Json(report))
    })
    .await
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VerdictRequest {
    /// `accepted`, `rework`, `rejected` or `abandoned`.
    verdict: String,
    #[serde(default, alias = "note")]
    reason: Option<String>,
}

/// `POST /v1/agents/{name}/verdict`: record a verdict on an agent's work
/// (ADR 0008). Only a strict ancestor, a root admin or the operator may judge
/// it; an agent never records an outcome of its own work.
async fn verdict_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<VerdictRequest>,
) -> Result<Json<anyagents_core::Outcome>, ApiError> {
    spawn_store(move || {
        let kind = anyagents_core::OutcomeKind::parse_verdict(&body.verdict).ok_or_else(|| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "verdict must be accepted, rework, rejected or abandoned",
            )
        })?;
        let target = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        let actor = match &principal {
            Principal::Operator => "operator".to_string(),
            Principal::Agent { agent_id, .. } if *agent_id == target.id => {
                return Err(ApiError::forbidden(
                    "an agent cannot record a verdict on its own work",
                ));
            }
            Principal::Agent { agent_id, name } => {
                if is_self_or_ancestor(&state.store, agent_id, &target) {
                    format!("agent:{name}")
                } else {
                    require_admin_or_operator(
                        &state.store,
                        &principal,
                        "agents.verdict",
                        &target.name,
                    )?
                }
            }
        };
        let config = anyagents_core::agent_config_dir(state.store.data_dir(), &target.id);
        let (outcome, _) = state
            .store
            .record_outcome(anyagents_core::NewOutcome::verdict(
                target.id.clone(),
                kind,
                actor,
                body.reason,
                now_ms(),
                create::bound_profile_name(&config),
            ))
            .map_err(ApiError::from_store)?;
        Ok(Json(outcome))
    })
    .await
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillReadRequest {
    skill: String,
    file: String,
    sha256: String,
}

/// `POST /v1/skills/reads`: the calling agent read one skill file whose bytes
/// had `sha256` (ADR 0008). Recorded for the authenticated agent only.
async fn skill_read_route(
    principal: Principal,
    State(state): State<AppState>,
    Json(body): Json<SkillReadRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let Principal::Agent { agent_id, .. } = principal else {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "skill reads are recorded for the calling agent",
        ));
    };
    spawn_store(move || {
        state
            .store
            .record_skill_read(&agent_id, &body.skill, &body.file, &body.sha256)
            .map_err(ApiError::from_store)?;
        Ok(Json(serde_json::json!({ "recorded": true })))
    })
    .await
}

/// `GET /v1/whoami`: the agent this call acts as. A tool server asks before it
/// speaks for anyone, and gets the id the server resolved from the capability,
/// never the name a caller wrote.
async fn whoami(
    State(state): State<AppState>,
    ctx: Whoami,
) -> Result<Json<serde_json::Value>, ApiError> {
    match ctx.principal {
        Principal::Agent { agent_id, name } => {
            spawn_store(move || {
                let agent_record = state.store.agent(&agent_id).ok();
                let workdir = agent_record.as_ref().and_then(|a| a.workdir.clone());
                let harness = agent_record.as_ref().map(|a| a.harness.clone());
                Ok(Json(serde_json::json!({
                    "agent_id": agent_id.as_str(),
                    "name": name,
                    "workdir": workdir,
                    "harness": harness,
                })))
            })
            .await
        }
        Principal::Operator => Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "the operator authenticated, but this call names an agent",
        )),
    }
}

/// The principal a `whoami` call resolved, from the capability header or, on a
/// deployment without the boundary, from the credential the tool server reads.
struct Whoami {
    principal: Principal,
}

impl axum::extract::FromRequestParts<AppState> for Whoami {
    type Rejection = auth::AuthRejection;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        if state.auth.is_trusted()
            && parts.headers.get(crate::operator::AGENT_HEADER).is_none()
            && let Some(id) = parts
                .headers
                .get("x-anyagents-agent-id")
                .and_then(|value| value.to_str().ok())
        {
            // A machine without the boundary: the tool server says which agent
            // its own config directory belongs to, and the server checks that
            // the agent exists. Only here, and only for this read.
            let store = Arc::clone(&state.store);
            let id = id.trim().to_string();
            let principal = auth::off_worker(move || auth::agent_by_name(&store, &id)).await?;
            return Ok(Self { principal });
        }
        use axum::extract::FromRequestParts;
        let principal =
            <Principal as FromRequestParts<AppState>>::from_request_parts(parts, state).await?;
        Ok(Self { principal })
    }
}

/// `GET /v1/operator/agents/{name}/inbox`: any agent's mailbox, for the
/// operator's or a root admin's reading. A different path from the agent's own
/// route, so a name can never be taken for an identity.
async fn operator_inbox(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Query(query): Query<Since>,
) -> Result<Json<InboxResponse>, ApiError> {
    spawn_store(move || {
        require_admin_or_operator(&state.store, &principal, "agents.operator_inbox", &name)?;
        let agent = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        inbox_of(
            &state.store,
            &agent.id,
            query.since,
            query.limit,
            query.unread.unwrap_or(false),
            query.sender.as_deref(),
        )
    })
    .await
}

/// `GET /v1/operator/inbox`: the reports agents sent to `operator`. Only the
/// operator reads them; every agent, a root admin included, may only write.
async fn operator_reports(
    principal: Principal,
    State(state): State<AppState>,
    Query(query): Query<Since>,
) -> Result<Json<OperatorInboxResponse>, ApiError> {
    if !principal.is_operator() {
        return Err(ApiError::forbidden(
            "only the operator reads the operator inbox; an agent may only send to `operator`",
        ));
    }
    spawn_store(move || {
        let messages = state
            .store
            .operator_inbox(query.since, query.limit)
            .map_err(ApiError::from_store)?;
        Ok(Json(OperatorInboxResponse {
            messages: messages.into_iter().map(Into::into).collect(),
        }))
    })
    .await
}

/// Only the operator administers the deployment.
fn require_operator(principal: &Principal) -> Result<(), ApiError> {
    if principal.is_operator() {
        Ok(())
    } else {
        Err(ApiError::forbidden(
            "an agent session cannot administer the server",
        ))
    }
}

/// Authority is read from SQLite on every call, not from the capability.
fn require_admin_or_operator(
    store: &Store,
    principal: &Principal,
    action: &str,
    target: &str,
) -> Result<String, ApiError> {
    let actor = match principal {
        Principal::Operator => "operator".to_string(),
        Principal::Agent { agent_id, name } => {
            let agent = store.agent(agent_id).map_err(ApiError::from_store)?;
            if agent.parent_id.is_some() || !agent.is_admin {
                return Err(ApiError::forbidden("administrative authority required"));
            }
            format!("agent:{name}")
        }
    };
    store
        .record_admin_audit(&actor, action, target)
        .map_err(ApiError::from_store)?;
    Ok(actor)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AdminRequest {
    admin: bool,
}

async fn set_admin_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<AdminRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_operator(&principal)?;
    spawn_store(move || {
        let agent = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        state
            .store
            .set_agent_admin(&agent.id, body.admin)
            .map_err(ApiError::from_store)?;
        state
            .store
            .record_admin_audit(
                "operator",
                if body.admin {
                    "agents.grant_admin"
                } else {
                    "agents.revoke_admin"
                },
                &agent.name,
            )
            .map_err(ApiError::from_store)?;
        Ok(Json(
            serde_json::json!({"agent": agent.name, "admin": body.admin}),
        ))
    })
    .await
}

// Socket requests carry secret values, never host filesystem paths.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelRequest {
    model: Option<String>,
    harness: Option<String>,
    base_url: Option<String>,
    key: Option<String>,
    profile: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HarnessRequest {
    harness: String,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    profile: Option<String>,
}

async fn reset_session_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        let found = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        let _lifecycle = state
            .store
            .try_lock_agent_lifecycle(&found.id)
            .map_err(ApiError::from_store)?;
        let found = state.store.agent(&found.id).map_err(ApiError::from_store)?;
        match &principal {
            Principal::Agent { agent_id, .. }
                if agent_id != &found.id && is_self_or_ancestor(&state.store, agent_id, &found) => {
            }
            _ => {
                require_admin_or_operator(
                    &state.store,
                    &principal,
                    "agents.reset_session",
                    &found.name,
                )?;
            }
        }
        create::reset_agent_session_locked(&state.store, &found)
            .map(Json)
            .map_err(ApiError::from_store)
    })
    .await
}

async fn harness_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    headers: axum::http::HeaderMap,
    Json(body): Json<HarnessRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let via_http = via_http_mcp(&headers);
    spawn_store(move || {
        let found = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        let actor = match &principal {
            Principal::Agent {
                agent_id,
                name: caller_name,
            } if agent_id != &found.id && is_self_or_ancestor(&state.store, agent_id, &found) => {
                format!("agent:{caller_name}")
            }
            _ => {
                require_admin_or_operator(&state.store, &principal, "agents.harness", &found.name)?
            }
        };
        require_remote_callback(state.store.data_dir(), &body.harness)?;
        refuse_host_placement_over_http(via_http, &body.harness)?;
        let out = create::set_agent_harness(
            &state.store,
            state.store.data_dir(),
            &actor,
            &found.name,
            &body.harness,
            body.model.as_deref(),
            body.profile.as_deref(),
        )
        .map_err(ApiError::from_store)?;
        // The agent's profile binding may have moved.
        state.profiles.invalidate(&state.store);
        Ok(Json(out))
    })
    .await
}

/// Whether this call was made by the tool server behind the HTTP MCP route.
/// That tool server runs inside the server process as the server's own user,
/// so a call it makes may not start or read anything on the host.
fn via_http_mcp(headers: &axum::http::HeaderMap) -> bool {
    headers.contains_key(crate::mcp::HTTP_MCP_HEADER)
}

/// Whether an agent's harness runs off this host.
fn runs_off_host(data_dir: &Path, agent: &Agent) -> bool {
    crate::remote::runs_off_host(
        agent,
        &anyagents_core::agent_config_dir(data_dir, &agent.id),
    )
}

/// Over HTTP MCP an agent is created on, or moved to, a cloud harness only:
/// any other harness would run on this host on the call's behalf.
fn refuse_host_placement_over_http(via_http: bool, harness: &str) -> Result<(), ApiError> {
    if via_http && !vendor::is_cloud_harness(harness) {
        return Err(ApiError::forbidden(format!(
            "a call over HTTP MCP places agents only on a cloud harness (`claude-cloud`, \
             `openai-cloud`, `cursor-cloud`, `spiron-cloud`); `{harness}` would run on this host"
        )));
    }
    Ok(())
}

/// Over HTTP MCP a media attachment is a URL. A local path would have the
/// server read a host file as its own user and upload it, and a bare name may
/// resolve against the server's working directory.
fn refuse_host_media_over_http(body: &SendRequest) -> Result<(), ApiError> {
    if let Some(media) = &body.media {
        let source = media.source.trim();
        if !(source.starts_with("https://") || source.starts_with("http://")) {
            return Err(ApiError::forbidden(
                "a call over HTTP MCP attaches media by `https://` URL only; a local path or file id \
                 would be read on this host",
            ));
        }
    }
    Ok(())
}

/// A harness that runs off this host can only call back through
/// `public_url`; refuse to create or move an agent onto one without it, before
/// anything is written.
fn require_remote_callback(data_dir: &Path, harness: &str) -> Result<(), ApiError> {
    if vendor::is_cloud_harness(harness) {
        crate::remote::require_public_url(data_dir)
            .map_err(|why| ApiError::new(StatusCode::BAD_REQUEST, why))?;
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TransferAgentRequest {
    parent: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

async fn transfer_agent_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<TransferAgentRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        let found = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        let is_ancestor = match &principal {
            Principal::Agent { agent_id, .. } => {
                agent_id != &found.id && is_self_or_ancestor(&state.store, agent_id, &found)
            }
            Principal::Operator => false,
        };
        let old_parent_name = found
            .parent_id
            .as_ref()
            .and_then(|pid| state.store.agent(pid).ok())
            .map(|p| p.name);
        let new_parent = match body
            .parent
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            None | Some("none" | "root" | "null") => None,
            Some(raw) => {
                let clean = raw.strip_prefix("agent:").unwrap_or(raw);
                let target = if clean.eq_ignore_ascii_case("self")
                    && let Principal::Agent { ref agent_id, .. } = principal
                {
                    state.store.agent(agent_id).map_err(ApiError::from_store)?
                } else {
                    state
                        .store
                        .agent_by_name(clean)
                        .map_err(ApiError::from_store)?
                };
                if target.state == "archived" {
                    return Err(ApiError::new(
                        StatusCode::BAD_REQUEST,
                        format!("cannot transfer to archived parent `{}`", target.name),
                    ));
                }
                Some(target)
            }
        };
        let actor = match &principal {
            Principal::Agent {
                agent_id,
                name: caller_name,
            } if is_ancestor
                && new_parent
                    .as_ref()
                    .is_some_and(|np| is_self_or_ancestor(&state.store, agent_id, np)) =>
            {
                format!("agent:{caller_name}")
            }
            _ => {
                require_admin_or_operator(&state.store, &principal, "agents.transfer", &found.name)?
            }
        };
        let updated = state
            .store
            .set_agent_parent(&found.id, new_parent.as_ref().map(|p| &p.id))
            .map_err(ApiError::from_store)?;

        let mut env_pairs = updated.launch_env.clone();
        env_pairs.retain(|(k, _)| k != "ANYAGENTS_PARENT_NAME");
        if let Some(ref np) = new_parent {
            env_pairs.push(("ANYAGENTS_PARENT_NAME".to_string(), np.name.clone()));
        }
        state.store.set_agent_launch_env(&updated.id, &env_pairs).map_err(ApiError::from_store)?;

        let config = anyagents_core::agent_config_dir(state.store.data_dir(), &updated.id);
        let work_dir = updated
            .workdir
            .as_deref()
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                state
                    .store
                    .data_dir()
                    .join("agents")
                    .join(updated.id.as_str())
                    .join("work")
            });
        create::reconcile_agent_runtime_config(
            &state.store,
            state.store.data_dir(),
            &updated,
            &config,
            &work_dir,
        ).map_err(ApiError::from_store)?;

        let reason_suffix = body
            .reason
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|r| format!(" Reason: {r}"))
            .unwrap_or_default();
        if let Some(ref np) = new_parent {
            let notice = format!(
                "[Custody Transferred] Your parent agent is now `agent:{}` (previously `{}`). Report subsequent progress and completion via `send(to: [\"parent\"], ...)` to `agent:{}`.{reason_suffix}",
                np.name,
                old_parent_name.as_deref().unwrap_or("root"),
                np.name
            );
            let mut incoming = Incoming::new(notice);
            incoming.sender = Some(format!("agent:{actor}"));
            state.store.receive(&updated.id, incoming).map_err(ApiError::from_store)?;
        }

        let mut view =
            serde_json::to_value(agent_view(&state.store, &state.health, &updated)?).unwrap();
        if let Some(obj) = view.as_object_mut() {
            obj.insert(
                "previous_parent".to_string(),
                serde_json::json!(old_parent_name),
            );
            obj.insert(
                "new_parent".to_string(),
                serde_json::json!(new_parent.as_ref().map(|p| p.name.as_str())),
            );
            obj.insert("transferred_by".to_string(), serde_json::json!(actor));
        }
        Ok(Json(view))
    })
    .await
}

#[derive(Deserialize, Default)]
struct ScheduleRequest {
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    at: Option<String>,
    #[serde(default)]
    every: Option<String>,
    #[serde(default)]
    watch: Option<String>,
    #[serde(default)]
    prompt: Option<String>,
    #[serde(default, alias = "schedule_id")]
    id: Option<String>,
    #[serde(default, alias = "name")]
    agent: Option<String>,
}

fn authorize_schedule_target(
    store: &Store,
    principal: &Principal,
    target_name: Option<&str>,
) -> Result<Agent, ApiError> {
    match (
        principal,
        target_name.map(str::trim).filter(|s| !s.is_empty()),
    ) {
        (Principal::Agent { agent_id, .. }, None) => {
            store.agent(agent_id).map_err(ApiError::from_store)
        }
        (Principal::Agent { agent_id, .. }, Some(name)) => {
            let clean = name.strip_prefix("agent:").unwrap_or(name);
            if clean.eq_ignore_ascii_case("self") {
                return store.agent(agent_id).map_err(ApiError::from_store);
            }
            let found = store.agent_by_name(clean).map_err(ApiError::from_store)?;
            if &found.id == agent_id
                || (agent_id != &found.id && is_self_or_ancestor(store, agent_id, &found))
            {
                return Ok(found);
            }
            require_admin_or_operator(store, principal, "agents.schedule", &found.name)?;
            Ok(found)
        }
        (Principal::Operator, Some(name)) => {
            let clean = name.strip_prefix("agent:").unwrap_or(name);
            store.agent_by_name(clean).map_err(ApiError::from_store)
        }
        (Principal::Operator, None) => Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "operator schedule call requires an agent name",
        )),
    }
}

async fn list_schedules_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        let target = authorize_schedule_target(&state.store, &principal, Some(&name))?;
        let schedules: Vec<crate::api::ScheduleView> = state
            .store
            .list_schedules(Some(&target.id), true)
            .map_err(ApiError::from_store)?
            .into_iter()
            .map(crate::api::ScheduleView::from)
            .collect();
        Ok(Json(serde_json::json!({
            "agent": target.name,
            "agent_id": target.id.as_str(),
            "schedules": schedules,
        })))
    })
    .await
}

async fn list_self_schedules_route(
    principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || match &principal {
        Principal::Agent { agent_id, name } => {
            let schedules: Vec<crate::api::ScheduleView> = state
                .store
                .list_schedules(Some(agent_id), true)
                .map_err(ApiError::from_store)?
                .into_iter()
                .map(crate::api::ScheduleView::from)
                .collect();
            Ok(Json(serde_json::json!({
                "agent": name,
                "agent_id": agent_id.as_str(),
                "schedules": schedules,
            })))
        }
        Principal::Operator => {
            let schedules: Vec<crate::api::ScheduleView> = state
                .store
                .list_schedules(None, true)
                .map_err(ApiError::from_store)?
                .into_iter()
                .map(crate::api::ScheduleView::from)
                .collect();
            Ok(Json(serde_json::json!({
                "schedules": schedules,
            })))
        }
    })
    .await
}

async fn schedules_action_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(mut body): Json<ScheduleRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    body.agent = Some(name);
    handle_schedule_action(principal, state, body).await
}

async fn self_schedules_action_route(
    principal: Principal,
    State(state): State<AppState>,
    Json(body): Json<ScheduleRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    handle_schedule_action(principal, state, body).await
}

async fn handle_schedule_action(
    principal: Principal,
    state: AppState,
    body: ScheduleRequest,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        let has_spec = body.at.as_deref().is_some_and(|s| !s.trim().is_empty())
            || body.every.as_deref().is_some_and(|s| !s.trim().is_empty())
            || body.watch.as_deref().is_some_and(|s| !s.trim().is_empty());
        let action = body
            .action
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(if has_spec {
                "create"
            } else if body.id.is_some() {
                "cancel"
            } else {
                "list"
            })
            .to_ascii_lowercase();

        match action.as_str() {
            "list" | "ls" => {
                if matches!(principal, Principal::Operator) && body.agent.is_none() {
                    let schedules: Vec<crate::api::ScheduleView> = state
                        .store
                        .list_schedules(None, true)
                        .map_err(ApiError::from_store)?
                        .into_iter()
                        .map(crate::api::ScheduleView::from)
                        .collect();
                    return Ok(Json(serde_json::json!({ "schedules": schedules })));
                }
                let target =
                    authorize_schedule_target(&state.store, &principal, body.agent.as_deref())?;
                let schedules: Vec<crate::api::ScheduleView> = state
                    .store
                    .list_schedules(Some(&target.id), true)
                    .map_err(ApiError::from_store)?
                    .into_iter()
                    .map(crate::api::ScheduleView::from)
                    .collect();
                Ok(Json(serde_json::json!({
                    "agent": target.name,
                    "agent_id": target.id.as_str(),
                    "schedules": schedules,
                })))
            }
            "create" | "set" | "add" | "schedule" => {
                let target =
                    authorize_schedule_target(&state.store, &principal, body.agent.as_deref())?;
                let prompt = body.prompt.as_deref().unwrap_or("");
                let created = crate::schedule::create_agent_schedule(
                    &state.store,
                    &state.health,
                    &target,
                    body.at.as_deref(),
                    body.every.as_deref(),
                    body.watch.as_deref(),
                    prompt,
                )
                .map_err(|msg| ApiError::new(StatusCode::BAD_REQUEST, msg))?;
                let schedules: Vec<crate::api::ScheduleView> = state
                    .store
                    .list_schedules(Some(&target.id), true)
                    .map_err(ApiError::from_store)?
                    .into_iter()
                    .map(crate::api::ScheduleView::from)
                    .collect();
                Ok(Json(serde_json::json!({
                    "agent": target.name,
                    "agent_id": target.id.as_str(),
                    "schedule": created,
                    "schedules": schedules,
                })))
            }
            "cancel" | "delete" | "remove" | "clear" | "off" | "pause" | "disable" | "stop" => {
                let target =
                    authorize_schedule_target(&state.store, &principal, body.agent.as_deref())?;
                let id_or_all = body
                    .id
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("all");
                let cancelled = state
                    .store
                    .cancel_schedule(Some(&target.id), id_or_all)
                    .map_err(ApiError::from_store)?;
                crate::schedule::schedules_changed(state.store.data_dir());
                let schedules: Vec<crate::api::ScheduleView> = state
                    .store
                    .list_schedules(Some(&target.id), true)
                    .map_err(ApiError::from_store)?
                    .into_iter()
                    .map(crate::api::ScheduleView::from)
                    .collect();
                Ok(Json(serde_json::json!({
                    "agent": target.name,
                    "agent_id": target.id.as_str(),
                    "cancelled": cancelled,
                    "schedules": schedules,
                })))
            }
            other => Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                format!(
                    "unknown schedule action `{other}`; expected `create`, `list`, or `cancel`"
                ),
            )),
        }
    })
    .await
}

async fn model_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<ModelRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        let actor = require_admin_or_operator(&state.store, &principal, "agents.model", &name)?;
        let agent = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        if body.profile.is_some() && (body.key.is_some() || body.base_url.is_some()) {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "profile cannot be combined with a key or base URL",
            ));
        }
        if body.key.is_none() && body.base_url.is_none() {
            let has_harness = body
                .harness
                .as_deref()
                .map(str::trim)
                .is_some_and(|s| !s.is_empty());
            let has_model = body
                .model
                .as_deref()
                .map(str::trim)
                .is_some_and(|s| !s.is_empty());
            let has_profile = body
                .profile
                .as_deref()
                .map(str::trim)
                .is_some_and(|s| !s.is_empty());
            if !has_harness && !has_model && !has_profile {
                return Err(ApiError::new(StatusCode::BAD_REQUEST, "model is required"));
            }
            let out = create::configure_agent_runtime(
                &state.store,
                state.store.data_dir(),
                &actor,
                &agent.name,
                body.harness.as_deref(),
                body.model.as_deref(),
                body.profile.as_deref(),
            )
            .map_err(ApiError::from_store)?;
            // The agent's profile binding may have moved.
            state.profiles.invalidate(&state.store);
            return Ok(Json(out));
        }
        let model_str = body
            .model
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ApiError::new(StatusCode::BAD_REQUEST, "model is required"))?;
        let key = body.key.as_deref().unwrap_or("").trim();
        if key.is_empty() || key.contains(char::is_whitespace) {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "a nonempty single-line key is required",
            ));
        }
        let base_url = body
            .base_url
            .as_deref()
            .ok_or_else(|| ApiError::new(StatusCode::BAD_REQUEST, "base URL is required"))?;
        crate::gateway::store_key(state.store.data_dir(), &agent, base_url, model_str, key)
            .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "could not bind provider key"))?;
        Ok(Json(
            serde_json::json!({"agent": agent.name, "model": model_str}),
        ))
    })
    .await
}

#[derive(Deserialize)]
struct RenameAgentRequest {
    #[serde(alias = "new_name", alias = "to")]
    name: String,
}

async fn rename_agent_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<RenameAgentRequest>,
) -> Result<Json<AgentView>, ApiError> {
    spawn_store(move || {
        let old = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        let is_ancestor = match &principal {
            Principal::Agent { agent_id, .. } => {
                *agent_id != old.id && is_self_or_ancestor(&state.store, agent_id, &old)
            }
            Principal::Operator => true,
        };
        if !is_ancestor {
            require_admin_or_operator(&state.store, &principal, "agents.rename", &old.name)?;
        }
        let updated = state
            .store
            .rename_agent(&old.id, &body.name)
            .map_err(ApiError::from_store)?;
        let data_dir = state.store.data_dir();
        if old.name != updated.name {
            let browsers_dir = data_dir.join("browsers");
            let old_browser = browsers_dir.join(&old.name);
            let new_browser = browsers_dir.join(&updated.name);
            if old_browser.is_dir()
                && !new_browser.exists()
                && std::fs::rename(&old_browser, &new_browser).is_ok()
            {
                #[cfg(unix)]
                {
                    let _ = std::os::unix::fs::symlink(&updated.name, &old_browser);
                }
            }
        }
        let cfg = anyagents_core::agent_config_dir(data_dir, &updated.id);
        let work_dir = updated
            .workdir
            .as_deref()
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| anyagents_core::agent_work_dir(data_dir, &updated.id));
        create::reconcile_agent_runtime_config(&state.store, data_dir, &updated, &cfg, &work_dir)
            .map_err(ApiError::from_store)?;
        crate::telegram::sync_agent_telegram_bot_name(&state.store, data_dir, &updated);
        Ok(Json(agent_view(&state.store, &state.health, &updated)?))
    })
    .await
}

#[derive(Deserialize, Default)]
struct RoleAgentRequest {
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    clear: Option<bool>,
}

async fn role_agent_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<RoleAgentRequest>,
) -> Result<Json<AgentView>, ApiError> {
    spawn_store(move || {
        let found = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        let is_ancestor = match &principal {
            Principal::Agent { agent_id, .. } => {
                *agent_id != found.id && is_self_or_ancestor(&state.store, agent_id, &found)
            }
            Principal::Operator => true,
        };
        if !is_ancestor {
            require_admin_or_operator(&state.store, &principal, "agents.role", &found.name)?;
        }
        let data_dir = state.store.data_dir();
        let parent_wd = found
            .parent_id
            .as_ref()
            .and_then(|pid| state.store.agent(pid).ok())
            .and_then(|p| p.workdir);
        let new_role = if body.clear.unwrap_or(false) {
            None
        } else if let Some(raw) = body
            .role
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            Some(
                create::normalize_or_validate_agent_role(
                    raw,
                    found.workdir.as_deref().map(Path::new),
                    parent_wd.as_deref().map(Path::new),
                    data_dir,
                )
                .map_err(ApiError::from_store)?,
            )
        } else {
            None
        };
        let updated = state
            .store
            .set_agent_role(&found.id, new_role.as_deref())
            .map_err(ApiError::from_store)?;
        let cfg = anyagents_core::agent_config_dir(data_dir, &updated.id);
        let work_dir = updated
            .workdir
            .as_deref()
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| anyagents_core::agent_work_dir(data_dir, &updated.id));
        create::reconcile_agent_runtime_config(&state.store, data_dir, &updated, &cfg, &work_dir)
            .map_err(ApiError::from_store)?;
        crate::telegram::sync_agent_telegram_bot_name(&state.store, data_dir, &updated);
        Ok(Json(agent_view(&state.store, &state.health, &updated)?))
    })
    .await
}

#[derive(Deserialize, Default)]
struct ProfileModelsQuery {
    #[serde(default)]
    profile: Option<String>,
}

async fn profile_models_route(
    principal: Principal,
    State(state): State<AppState>,
    Query(query): Query<ProfileModelsQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        require_admin_or_operator(&state.store, &principal, "profiles.models", "profiles")?;
        Ok(Json(create::probe_profile_models_catalog(
            state.store.data_dir(),
            query.profile.as_deref(),
        )))
    })
    .await
}

#[derive(Debug, Default, Deserialize)]
struct ProfilesQuery {
    /// Answer with a listing made after this call rather than the snapshot.
    #[serde(default)]
    fresh: Option<bool>,
}

/// `GET /v1/profiles`: the last listing at once, each entry marked with the
/// age of its usage reading; see [`profiles`].
async fn profiles_route(
    principal: Principal,
    State(state): State<AppState>,
    Query(query): Query<ProfilesQuery>,
) -> Result<Json<Vec<ServedProfile>>, ApiError> {
    let store = Arc::clone(&state.store);
    spawn_store(move || {
        require_admin_or_operator(&store, &principal, "profiles.list", "profiles").map(drop)
    })
    .await?;
    state
        .profiles
        .list(&state.store, query.fresh.unwrap_or(false))
        .await
        .map(Json)
        .map_err(|why| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, why))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SetProfileRequest {
    profile: String,
    key: String,
}

async fn set_profile_route(
    principal: Principal,
    State(state): State<AppState>,
    Json(body): Json<SetProfileRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        require_admin_or_operator(&state.store, &principal, "profiles.set", &body.profile)?;
        let (provider, profile) =
            create::set_provider_profile(state.store.data_dir(), &body.profile, &body.key)
                .map_err(ApiError::from_store)?;
        state.profiles.invalidate(&state.store);
        quota_rotation::profiles_changed(Some(provider.as_str()));
        Ok(Json(serde_json::json!({
            "provider": provider,
            "profile": profile,
        })))
    })
    .await
}

async fn get_subagent_defaults_route(
    _principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<create::SubagentDefaultsConfig>, ApiError> {
    spawn_store(move || Ok(Json(create::load_subagent_defaults(state.store.data_dir())))).await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SetSubagentDefaultsRequest {
    agent: Option<String>,
    clear: Option<bool>,
    mode: String,
    harness: Option<String>,
    model: Option<String>,
    profile: Option<String>,
}

async fn set_subagent_defaults_route(
    principal: Principal,
    State(state): State<AppState>,
    Json(body): Json<SetSubagentDefaultsRequest>,
) -> Result<Json<create::SubagentDefaultsConfig>, ApiError> {
    spawn_store(move || {
        let target_label = body.agent.as_deref().unwrap_or("global");
        require_admin_or_operator(
            &state.store,
            &principal,
            "subagents.set_default",
            target_label,
        )?;
        let mut cfg = create::load_subagent_defaults(state.store.data_dir());
        if body.clear.unwrap_or(false) {
            if let Some(ref ag) = body.agent {
                cfg.agents.remove(ag.trim());
            } else {
                cfg.default = create::SubagentDefaultSpec {
                    mode: "inherit".to_string(),
                    harness: None,
                    model: None,
                    profile: None,
                };
            }
        } else {
            let spec = create::SubagentDefaultSpec {
                mode: body.mode,
                harness: body
                    .harness
                    .as_deref()
                    .map(anyagents_harness::normalize_harness_name)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string),
                model: body.model.filter(|s| !s.trim().is_empty()),
                profile: body.profile.filter(|s| !s.trim().is_empty()),
            };
            if let Some(ag) = body
                .agent
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                cfg.agents.insert(ag.to_string(), spec);
            } else {
                cfg.default = spec;
            }
        }
        create::save_subagent_defaults(state.store.data_dir(), &cfg)
            .map_err(ApiError::from_store)?;
        Ok(Json(cfg))
    })
    .await
}

#[derive(Deserialize, Default)]
struct DescriptionRequest {
    #[serde(default)]
    description: Option<String>,
    #[serde(default, alias = "instruction", alias = "system_prompt")]
    instructions: Option<String>,
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    lifecycle: Option<String>,
    #[serde(default, alias = "allow_external")]
    public: Option<bool>,
    #[serde(default)]
    clear: Option<bool>,
}

async fn get_description_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        let rec = if name.eq_ignore_ascii_case("self") {
            match &principal {
                Principal::Agent { agent_id, .. } => {
                    state.store.agent(agent_id).map_err(ApiError::from_store)?
                }
                Principal::Operator => {
                    return Err(ApiError::new(
                        StatusCode::BAD_REQUEST,
                        "`self` requires an agent caller",
                    ));
                }
            }
        } else {
            state
                .store
                .agent_by_name(&name)
                .map_err(ApiError::from_store)?
        };
        let can_read_private = can_actor_read_agent_private(&state.store, &principal, &rec);
        let aliases = state.store.agent_aliases(&rec.id).unwrap_or_default();
        let cfg = anyagents_core::agent_config_dir(state.store.data_dir(), &rec.id);
        let system_prompt = if can_read_private {
            std::fs::read_to_string(cfg.join("SYSTEM_PROMPT.md")).ok()
        } else {
            None
        };
        let mut resp = serde_json::json!({
            "id": rec.id.as_str(),
            "name": rec.name,
            "role": rec.role,
            "scope": rec.scope(),
            "lifecycle": rec.lifecycle(),
            "public": rec.is_public(),
            "harness": rec.harness,
            "model": rec.model,
            "aliases": aliases,
            "description": rec.public_description(),
        });
        if can_read_private {
            resp["instructions"] = serde_json::json!(rec.instructions);
            resp["system_prompt"] = serde_json::json!(system_prompt);
        }
        Ok(Json(resp))
    })
    .await
}

async fn set_description_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<DescriptionRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        if name.eq_ignore_ascii_case("all")
            && body.description.is_none()
            && body.instructions.is_none()
            && body.role.is_none()
            && body.scope.is_none()
            && body.lifecycle.is_none()
            && body.public.is_none()
            && !body.clear.unwrap_or(false)
        {
            require_admin_or_operator(&state.store, &principal, "agents.sync_instructions", "all")?;
            create::sync_all_agent_instructions(&state.store, state.store.data_dir())
                .map_err(ApiError::from_store)?;
            return Ok(Json(serde_json::json!({
                "synced": "all",
            })));
        }
        let target = if name.eq_ignore_ascii_case("self") {
            match &principal {
                Principal::Agent { agent_id, .. } => {
                    state.store.agent(agent_id).map_err(ApiError::from_store)?
                }
                Principal::Operator => {
                    return Err(ApiError::new(
                        StatusCode::BAD_REQUEST,
                        "`self` requires an agent caller",
                    ));
                }
            }
        } else {
            state
                .store
                .agent_by_name(&name)
                .map_err(ApiError::from_store)?
        };
        let is_ancestor = match &principal {
            Principal::Agent { agent_id, .. } => {
                *agent_id != target.id && is_self_or_ancestor(&state.store, agent_id, &target)
            }
            Principal::Operator => true,
        };
        if !is_ancestor {
            require_admin_or_operator(
                &state.store,
                &principal,
                "agents.set_description",
                &target.name,
            )?;
        }
        let actor = match &principal {
            Principal::Operator => "operator".to_string(),
            Principal::Agent { name, .. } => format!("agent:{name}"),
        };
        let mut rec = target;
        if let Some(ref scope_val) = body.scope {
            if !is_ancestor {
                require_admin_or_operator(&state.store, &principal, "agents.scope", &rec.name)?;
            }
            rec = state
                .store
                .set_agent_scope_as(&rec.id, scope_val, &actor)
                .map_err(ApiError::from_store)?;
        } else if let Some(pub_val) = body.public {
            if !is_ancestor {
                require_admin_or_operator(&state.store, &principal, "agents.public", &rec.name)?;
            }
            rec = state
                .store
                .set_agent_public_as(&rec.id, pub_val, &actor)
                .map_err(ApiError::from_store)?;
        }
        if let Some(ref lifecycle_val) = body.lifecycle {
            if !is_ancestor {
                require_admin_or_operator(&state.store, &principal, "agents.lifecycle", &rec.name)?;
            }
            rec = state
                .store
                .set_agent_lifecycle_as(&rec.id, lifecycle_val, &actor)
                .map_err(ApiError::from_store)?;
        }
        if let Some(ref role_str) = body.role {
            if !is_ancestor {
                require_admin_or_operator(&state.store, &principal, "agents.role", &rec.name)?;
            }
            let new_role = if role_str.trim().is_empty() {
                None
            } else {
                let parent_wd = rec
                    .parent_id
                    .as_ref()
                    .and_then(|pid| state.store.agent(pid).ok())
                    .and_then(|p| p.workdir);
                Some(
                    create::normalize_or_validate_agent_role(
                        role_str,
                        rec.workdir.as_deref().map(Path::new),
                        parent_wd.as_deref().map(Path::new),
                        state.store.data_dir(),
                    )
                    .map_err(ApiError::from_store)?,
                )
            };
            rec = state
                .store
                .set_agent_role(&rec.id, new_role.as_deref())
                .map_err(ApiError::from_store)?;
            crate::telegram::sync_agent_telegram_bot_name(
                &state.store,
                state.store.data_dir(),
                &rec,
            );
        }
        if body.clear.unwrap_or(false) {
            rec = state
                .store
                .set_agent_description(&rec.id, None)
                .map_err(ApiError::from_store)?;
            rec = state
                .store
                .set_agent_instructions(&rec.id, None)
                .map_err(ApiError::from_store)?;
        } else {
            if let Some(ref desc) = body.description {
                let trimmed = desc.trim();
                let clean_opt = if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed)
                };
                rec = state
                    .store
                    .set_agent_description(&rec.id, clean_opt)
                    .map_err(ApiError::from_store)?;
                if body.instructions.is_none() && rec.instructions.is_none() {
                    rec = state
                        .store
                        .set_agent_instructions(&rec.id, clean_opt)
                        .map_err(ApiError::from_store)?;
                }
                crate::telegram::sync_agent_telegram_bot_name(
                    &state.store,
                    state.store.data_dir(),
                    &rec,
                );
            }
            if let Some(ref inst) = body.instructions {
                let trimmed = inst.trim();
                rec = state
                    .store
                    .set_agent_instructions(
                        &rec.id,
                        if trimmed.is_empty() {
                            None
                        } else {
                            Some(trimmed)
                        },
                    )
                    .map_err(ApiError::from_store)?;
            }
        }
        let cfg = anyagents_core::agent_config_dir(state.store.data_dir(), &rec.id);
        create::sync_agent_description_files(&state.store, &rec, &cfg)
            .map_err(ApiError::from_store)?;
        let aliases = state.store.agent_aliases(&rec.id).unwrap_or_default();
        Ok(Json(serde_json::json!({
            "id": rec.id.as_str(),
            "name": rec.name,
            "role": rec.role,
            "scope": rec.scope(),
            "lifecycle": rec.lifecycle(),
            "public": rec.is_public(),
            "aliases": aliases,
            "description": rec.public_description(),
            "instructions": rec.instructions,
            "system_prompt_synced": true,
        })))
    })
    .await
}

async fn browser_route(
    principal: Principal,
    State(state): State<AppState>,
    Json(body): Json<crate::browser::BrowserRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let default_profile = match &principal {
        Principal::Agent { name, .. } => name.clone(),
        Principal::Operator => "default".to_string(),
    };
    spawn_store(move || {
        let out = crate::browser::execute(state.store.data_dir(), &default_profile, body)
            .map_err(ApiError::from_store)?;
        Ok(Json(out))
    })
    .await
}

async fn channels_route(
    principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        require_admin_or_operator(&state.store, &principal, "channels.list", "channels")?;
        let channels: Vec<_> = state
            .store
            .list_channels()
            .map_err(ApiError::from_store)?
            .into_iter()
            .map(|c| {
                let mem = anyagents_channel_telegram::Memory::try_from_state(&c.state)
                    .map_err(ApiError::from_store)?;
                let bot_username = c
                    .identity
                    .clone()
                    .or_else(|| {
                        mem.bot
                            .as_deref()
                            .and_then(|b| b.split_once(':').map(|(_, u)| u.to_string()))
                    })
                    .map(|u| {
                        let clean = u.trim().trim_start_matches('@');
                        format!("@{clean}")
                    });
                let agents: Vec<String> = c
                    .agents
                    .iter()
                    .filter_map(|id| state.store.agent(id).ok().map(|a| a.name))
                    .collect();
                Ok(serde_json::json!({
                    "id": c.id.to_string(),
                    "kind": c.kind,
                    "bot": bot_username,
                    "agents": agents,
                    "pending_managed_bots": mem.pending_managed_bots,
                }))
            })
            .collect::<Result<Vec<_>, ApiError>>()?;
        Ok(Json(serde_json::json!({"channels": channels})))
    })
    .await
}

fn default_channel_kind() -> String {
    "telegram".to_string()
}

#[derive(Deserialize)]
struct ChannelRequest {
    #[serde(default = "default_channel_kind")]
    kind: String,
    agent: String,
    #[serde(default)]
    token: Option<String>,
    #[serde(default, alias = "managed_bot_id", alias = "user_id")]
    bot_id: Option<i64>,
    #[serde(
        default,
        alias = "username",
        alias = "suggested_username",
        alias = "bot"
    )]
    bot_username: Option<String>,
    #[serde(default)]
    manager: Option<String>,
    #[serde(default)]
    replace_token: Option<bool>,
}

fn is_self_or_ancestor(
    store: &Store,
    caller_id: &anyagents_core::AgentId,
    target: &anyagents_core::Agent,
) -> bool {
    if caller_id == &target.id {
        return true;
    }
    let mut cursor = target.parent_id.clone();
    let mut depth = 0usize;
    while let Some(pid) = cursor {
        if &pid == caller_id {
            return true;
        }
        cursor = store.agent(&pid).ok().and_then(|a| a.parent_id);
        depth += 1;
        if depth > 16 {
            break;
        }
    }
    false
}

/// An unrestricted reader's scope: every agent, archived ones included.
fn every_agent_id(store: &Store) -> Result<Vec<String>, ApiError> {
    Ok(store
        .agent_ids()
        .map_err(ApiError::from_store)?
        .iter()
        .map(ToString::to_string)
        .collect())
}

/// Resolve the caller's conversation visibility once, before selecting a
/// recent or Raw-RAG read path. Non-admin agents see only themselves and their
/// descendants; `all` is an alias for exactly that set. Administrative reads
/// are intentionally unrestricted and leave an audit row.
fn conversation_visibility(
    store: &Store,
    principal: &Principal,
    requested: &str,
) -> Result<(String, Option<Vec<String>>), ApiError> {
    let requested = requested.trim();
    match principal {
        Principal::Operator => {
            store
                .record_admin_audit("operator", "conversation.read", requested)
                .map_err(ApiError::from_store)?;
            Ok((requested.to_string(), None))
        }
        Principal::Agent { agent_id, .. } => {
            let caller = store.agent(agent_id).map_err(ApiError::from_store)?;
            let admin = caller.parent_id.is_none() && caller.is_admin;
            if admin {
                let requested = if requested.eq_ignore_ascii_case("self") {
                    caller.name.as_str()
                } else {
                    requested
                };
                store
                    .record_admin_audit(
                        &format!("agent:{}", caller.name),
                        "conversation.read",
                        requested,
                    )
                    .map_err(ApiError::from_store)?;
                return Ok((requested.to_string(), None));
            }

            if requested.eq_ignore_ascii_case("self") {
                return Ok((caller.name, Some(vec![caller.id.to_string()])));
            }
            let mut visible_ids = store
                .descendant_ids(&caller.id)
                .map_err(ApiError::from_store)?;
            visible_ids.insert(caller.id.clone());
            if requested.eq_ignore_ascii_case("all") || requested == "*" {
                let visible_agent_ids = visible_ids.iter().map(ToString::to_string).collect();
                return Ok(("all".to_string(), Some(visible_agent_ids)));
            }

            let target = store
                .agent_by_name(requested)
                .map_err(ApiError::from_store)?;
            if !visible_ids.contains(&target.id) {
                return Err(ApiError::forbidden(format!(
                    "agent `{}` cannot read private conversation history of `{}`",
                    caller.name, target.name
                )));
            }
            Ok((target.name, Some(vec![target.id.to_string()])))
        }
    }
}

async fn add_channel_route(
    principal: Principal,
    State(state): State<AppState>,
    Json(body): Json<ChannelRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        if body.kind != "telegram" {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid channel kind",
            ));
        }
        if let Some(ref tok) = body.token {
            let trimmed = tok.trim();
            if trimmed.is_empty() || trimmed.chars().any(|c| c.is_whitespace() || c == '/') {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "invalid channel kind or token",
                ));
            }
        }
        let target_clean = body
            .agent
            .trim()
            .strip_prefix("agent:")
            .unwrap_or(body.agent.trim());
        let agent = state
            .store
            .agent_by_name(target_clean)
            .map_err(ApiError::from_store)?;
        let caller_agent = match &principal {
            Principal::Agent { agent_id, .. } => Some(
                state
                    .store
                    .agent(agent_id)
                    .map_err(|_| ApiError::forbidden("authenticated agent no longer exists"))?,
            ),
            Principal::Operator => None,
        };
        let authorized_by_lineage = match &principal {
            Principal::Agent { agent_id, .. } => {
                is_self_or_ancestor(&state.store, agent_id, &agent)
            }
            Principal::Operator => true,
        };
        if !authorized_by_lineage {
            require_admin_or_operator(&state.store, &principal, "channels.add", &body.agent)?;
        }
        let result = crate::telegram::request_or_bind_telegram_channel(
            &state.store,
            state.store.data_dir(),
            caller_agent.as_ref(),
            &agent,
            body.token.as_deref(),
            body.bot_id,
            body.bot_username.as_deref(),
            body.manager.as_deref(),
            body.replace_token.unwrap_or(false),
        )
        .map_err(|err| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                format!("could not bind or request channel: {err}"),
            )
        })?;
        Ok(Json(result))
    })
    .await
}

/// A loopback port the operating system picked, held open until the caller
/// serves on it, or why none could be bound.
pub fn listen_on_loopback(port: u16) -> Result<(u16, tokio::net::TcpListener), Error> {
    // A non-blocking listener before it joins the runtime; the runtime makes it
    // blocking-polled itself.
    let address = std::net::TcpListener::bind(("127.0.0.1", port))?;
    address.set_nonblocking(true)?;
    let listener = tokio::net::TcpListener::from_std(address)?;
    let port = listener.local_addr()?.port();
    Ok((port, listener))
}

/// Serve the gateway on the loopback listener it was given, for the life of the
/// process. Each harness reaches it with its own token; nothing else can. It
/// runs on the server's runtime, so the caller must be inside one.
pub fn serve_gateway(
    listener: tokio::net::TcpListener,
    routes: MemoryRoutes,
    usage: anyagents_gateway::MemoryUsage,
) -> Result<(), Error> {
    let app = anyagents_gateway::router(routes, usage);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(())
}

fn prepare_socket(path: &Path) -> Result<(), Error> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_socket() => std::fs::remove_file(path)?,
        Ok(_) => {
            return Err(Error::Message(format!(
                "socket path exists and is not a socket: {}",
                path.display()
            )));
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err.into()),
    }
    Ok(())
}

fn set_socket_mode(path: &Path) -> Result<(), Error> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

/// One line of a sign-in, as the client reads it.
#[derive(serde::Serialize)]
struct SignInLine {
    kind: &'static str,
    text: String,
}

#[derive(Deserialize, Default)]
struct SignInQuery {
    #[serde(default)]
    profile: Option<String>,
}

/// `POST /v1/harnesses/{id}/sign-in`. The body stays open while the vendor
/// command runs. Each link or code is one JSON line, and the last line is the
/// outcome. The credential never appears here.
async fn sign_in_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<SignInQuery>,
) -> Result<Response, ApiError> {
    require_operator(&principal)?;
    let profile = query
        .profile
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("default")
        .to_string();
    let (tx, rx) = tokio::sync::mpsc::channel::<String>(16);
    let runner = Arc::clone(&state.sign_in);
    let root = state.store.data_dir().to_path_buf();
    let mark = capability_mark(Arc::clone(&state.store));
    let spawned = tokio::task::spawn_blocking(move || {
        let mut sink = ChannelSink { tx };
        let mut guard = runner.lock().unwrap_or_else(|err| err.into_inner());
        let result = sign_in(
            &builtin_catalog(),
            &root,
            &id,
            &profile,
            &mut *guard,
            &mut sink,
        );
        let line = match &result {
            Ok(outcome) => {
                // A new sign-in is a new instance: the harness restarts with a
                // capability this sign-in minted, and the last one is retired.
                if let Err(err) = mark() {
                    eprintln!("capability after sign-in: {err}");
                }
                SignInLine {
                    kind: "done",
                    text: outcome.account.clone().unwrap_or_default(),
                }
            }
            Err(err) => SignInLine {
                kind: "error",
                text: err.to_string(),
            },
        };
        sink.send(&line);
        result
    });
    // The task sends the final line before it returns, so the stream ends only
    // after the command has finished. Dropping the handle detaches it; the
    // channel keeps the lines ordered.
    drop(spawned);
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let body = Body::from_stream(stream.map(|line| Ok::<_, std::io::Error>(format!("{line}\n"))));
    Ok(Response::builder()
        .header("content-type", "application/x-ndjson")
        .body(body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response()))
}

struct ChannelSink {
    tx: tokio::sync::mpsc::Sender<String>,
}

impl ChannelSink {
    fn send(&self, line: &SignInLine) {
        let text = serde_json::to_string(line).unwrap_or_default();
        let _ = self.tx.blocking_send(text);
    }
}

impl anyagents_installer::SignInSink for ChannelSink {
    fn prompt(&mut self, prompt: &anyagents_installer::SignInPrompt) {
        let (kind, text) = match prompt {
            anyagents_installer::SignInPrompt::Link(text) => ("link", text.clone()),
            anyagents_installer::SignInPrompt::DeviceCode(text) => ("code", text.clone()),
            anyagents_installer::SignInPrompt::Prompt(text) => ("prompt", text.clone()),
        };
        self.send(&SignInLine { kind, text });
    }

    fn done(&mut self, _: &anyagents_installer::SignInOutcome) {}
}

/// Runs the vendor command for real. Each line it prints reaches the sink
/// before the command returns.
struct ProcessRunner;

impl CommandRunner for ProcessRunner {
    fn run(
        &mut self,
        program: &str,
        args: &[String],
        dir: &Path,
        env: &[(&str, &str)],
        on_line: &mut dyn LineSink,
    ) -> Result<(), InstallError> {
        let env_pairs: Vec<(String, String)> = env
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        let mut command = anyagents_harness::spawn_command(Path::new(program), &env_pairs)
            .map_err(|err| InstallError::Failed(err.to_string()))?;
        command
            .args(args)
            .current_dir(dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut child = command
            .spawn()
            .map_err(|err| InstallError::Failed(err.to_string()))?;
        if let Some(stdout) = child.stdout.take() {
            for line in BufReader::new(stdout).lines() {
                let line = line.map_err(|err| InstallError::Failed(err.to_string()))?;
                on_line.line(&line);
            }
        }
        let status = child
            .wait()
            .map_err(|err| InstallError::Failed(err.to_string()))?;
        if status.success() {
            Ok(())
        } else {
            Err(InstallError::Failed(format!("exit {status}")))
        }
    }
}

#[derive(Default, Deserialize)]
pub(crate) struct ListAgentsQuery {
    pub state: Option<String>,
    pub parent: Option<String>,
    pub name: Option<String>,
}

async fn get_agent(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<AgentView>, ApiError> {
    spawn_store(move || {
        let caller = match &principal {
            Principal::Operator => None,
            Principal::Agent { agent_id, .. } => Some(
                state
                    .store
                    .agent(agent_id)
                    .map_err(|_| ApiError::forbidden("authenticated agent no longer exists"))?,
            ),
        };
        let name = if name == "self" {
            caller.as_ref().map(|a| a.id.as_str()).unwrap_or(&name)
        } else {
            &name
        };
        let agent = state
            .store
            .agent_by_name(name)
            .map_err(ApiError::from_store)?;
        if let Some(caller) = &caller
            && caller.is_isolated()
            && !is_self_or_ancestor(&state.store, &caller.id, &agent)
            && caller.parent_id.as_ref() != Some(&agent.id)
            && !state.store.share_any_group_or_chat(&caller.id, &agent.id)
        {
            return Err(ApiError::forbidden("agent is outside the caller's scope"));
        }
        let mut view = agent_view(&state.store, &state.health, &agent)?;
        if let Some(caller) = &caller {
            view.relationship = Some(compute_caller_relationship(&state.store, caller, &agent));
            view.can_message = Some(
                can_actor_message_agent(&state.store, &Actor::Agent(caller.name.clone()), &agent)
                    .is_ok(),
            );
            if !can_actor_read_agent_private(&state.store, &principal, &agent) {
                view.instructions = None;
            }
        }
        Ok(Json(view))
    })
    .await
}

async fn list_agents(
    principal: Principal,
    State(state): State<AppState>,
    Query(query): Query<ListAgentsQuery>,
) -> Result<Json<AgentsResponse>, ApiError> {
    let operator = principal.is_operator();
    spawn_store(move || {
        let mut listed = list_agents_sync_for_principal(&state, &principal, &query)?;
        if !operator {
            listed.0.gateway = None;
        }
        Ok(listed)
    })
    .await
}

fn compute_caller_relationship(store: &Store, caller: &Agent, target: &Agent) -> String {
    relationship_with(caller, target, |ancestor, agent| {
        is_self_or_ancestor(store, ancestor, agent)
    })
}

fn relationship_with(
    caller: &Agent,
    target: &Agent,
    is_self_or_ancestor: impl Fn(&anyagents_core::AgentId, &Agent) -> bool,
) -> String {
    if caller.id == target.id {
        "self".to_string()
    } else if caller.parent_id.as_ref() == Some(&target.id) {
        "parent".to_string()
    } else if target.parent_id.as_ref() == Some(&caller.id) {
        "child".to_string()
    } else if caller.parent_id.is_some() && caller.parent_id == target.parent_id {
        "sibling".to_string()
    } else if is_self_or_ancestor(&target.id, caller) {
        "ancestor".to_string()
    } else if is_self_or_ancestor(&caller.id, target) {
        "descendant".to_string()
    } else if caller.parent_id.is_none() && target.parent_id.is_none() {
        "peer".to_string()
    } else {
        "other".to_string()
    }
}

/// An agent caller's view of one roster, read once per request instead of
/// once per listed agent. Every check is the predicate the single-agent routes
/// use; only where the rows come from differs.
struct RosterCaller<'a> {
    store: &'a Store,
    caller: &'a Agent,
    /// The roster rows, so an ancestor walk reads the store only for a parent
    /// outside the roster.
    rows: HashMap<&'a str, &'a Agent>,
    /// Agents that share a group or Telegram chat with the caller.
    sharing: HashSet<anyagents_core::AgentId>,
}

impl<'a> RosterCaller<'a> {
    fn new(store: &'a Store, caller: &'a Agent, roster: &'a [Agent]) -> Self {
        Self {
            store,
            caller,
            rows: roster.iter().map(|a| (a.id.as_str(), a)).collect(),
            sharing: store.agents_sharing_group_or_chat(&caller.id),
        }
    }

    /// [`is_self_or_ancestor`] over the roster rows.
    fn is_self_or_ancestor(&self, ancestor: &anyagents_core::AgentId, target: &Agent) -> bool {
        if ancestor == &target.id {
            return true;
        }
        let mut cursor = target.parent_id.clone();
        let mut depth = 0usize;
        while let Some(pid) = cursor {
            if &pid == ancestor {
                return true;
            }
            cursor = match self.rows.get(pid.as_str()) {
                Some(parent) => parent.parent_id.clone(),
                None => self.store.agent(&pid).ok().and_then(|a| a.parent_id),
            };
            depth += 1;
            if depth > 16 {
                break;
            }
        }
        false
    }

    /// The isolated-scope rule of [`get_agent`].
    fn sees(&self, agent: &Agent) -> bool {
        !(self.caller.is_isolated()
            && !self.is_self_or_ancestor(&self.caller.id, agent)
            && self.caller.parent_id.as_ref() != Some(&agent.id)
            && !self.sharing.contains(&agent.id))
    }

    fn annotate(&self, view: &mut AgentView, agent: &Agent) {
        view.relationship = Some(relationship_with(self.caller, agent, |ancestor, target| {
            self.is_self_or_ancestor(ancestor, target)
        }));
        view.can_message = Some(
            can_sender_message_agent_with(self.store, self.caller, agent, || {
                self.sharing.contains(&agent.id)
            })
            .is_ok(),
        );
        // [`can_actor_read_agent_private`] for this caller.
        let reads_private = self.is_self_or_ancestor(&self.caller.id, agent)
            || (self.caller.is_admin && self.caller.parent_id.is_none());
        if !reads_private {
            view.instructions = None;
        }
    }
}

fn list_agents_sync_for_principal(
    state: &AppState,
    principal: &Principal,
    query: &ListAgentsQuery,
) -> Result<Json<AgentsResponse>, ApiError> {
    let caller_agent = match principal {
        Principal::Operator => None,
        Principal::Agent { agent_id, .. } => Some(
            state
                .store
                .agent(agent_id)
                .map_err(|_| ApiError::forbidden("authenticated agent no longer exists"))?,
        ),
    };
    // Only the selected state and its ancestors are read; the archive is
    // counted by index instead of being deserialized row by row.
    let (roster, archived_count) = state
        .store
        .roster_agents(query.state.as_deref())
        .map_err(ApiError::from_store)?;
    let caller = caller_agent
        .as_ref()
        .map(|caller| RosterCaller::new(&state.store, caller, &roster));
    let visible: Vec<&Agent> = roster
        .iter()
        .filter(|agent| caller.as_ref().is_none_or(|caller| caller.sees(agent)))
        .collect();
    let mut listed = roster_views(&state.store, &state.health, &visible, archived_count, query)?;
    if let Some(caller) = &caller {
        let by_id: HashMap<&str, &Agent> = visible.iter().map(|a| (a.id.as_str(), *a)).collect();
        for view in &mut listed.agents {
            let agent = by_id
                .get(view.id.as_str())
                .copied()
                .expect("every view comes from a visible row");
            caller.annotate(view, agent);
        }
    }
    Ok(Json(listed))
}

/// Shared by the daemon and offline operator CLI. Filter before runtime reads.
pub(crate) fn roster_views(
    store: &Store,
    health: &HealthBoard,
    all_agents: &[&Agent],
    archived_count: usize,
    query: &ListAgentsQuery,
) -> Result<AgentsResponse, ApiError> {
    if let Some(state) = query.state.as_deref()
        && !matches!(
            state,
            "all" | "archived" | "running" | "idle" | "paused" | "created"
        )
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid agent state",
        ));
    }
    let by_id: HashMap<_, _> = all_agents.iter().map(|a| (a.id.as_str(), *a)).collect();
    let selected_name = query
        .name
        .as_deref()
        .map(|name| store.agent_by_name(name))
        .transpose()
        .map_err(ApiError::from_store)?;
    let parent = query
        .parent
        .as_deref()
        .filter(|p| !matches!(*p, "root" | "none" | "all" | "subagents"))
        .map(|p| store.agent_by_name(p))
        .transpose()
        .map_err(ApiError::from_store)?;
    let mut selected = HashSet::new();
    for agent in all_agents {
        let state_matches = match query.state.as_deref() {
            Some("all") => true,
            Some(state) => agent.state == state,
            None => agent.state != "archived",
        };
        let parent_matches = match query.parent.as_deref() {
            Some("root" | "none") => agent.parent_id.is_none(),
            _ => parent
                .as_ref()
                .is_none_or(|p| agent.parent_id.as_ref() == Some(&p.id)),
        };
        if state_matches
            && parent_matches
            && selected_name.as_ref().is_none_or(|a| a.id == agent.id)
        {
            selected.insert(agent.id.as_str());
        }
    }
    // Retain ancestor nodes for tree rendering without serializing the archive.
    if query
        .state
        .as_deref()
        .is_none_or(|s| !matches!(s, "all" | "archived"))
    {
        let initial: Vec<_> = selected.iter().copied().collect();
        for id in initial {
            let mut cursor = by_id.get(id).and_then(|a| a.parent_id.as_ref());
            let mut visited = HashSet::new();
            while let Some(parent) = cursor {
                if !visited.insert(parent.as_str()) {
                    break;
                }
                let Some(agent) = by_id.get(parent.as_str()) else {
                    break;
                };
                selected.insert(agent.id.as_str());
                cursor = agent.parent_id.as_ref();
            }
        }
    }
    let mut metadata = store.agent_query_metadata().map_err(ApiError::from_store)?;
    let files = file_cache::cache();
    files.catch_up();
    let seats = view_seats(files);
    let mut agents = Vec::with_capacity(selected.len());
    for agent in all_agents {
        if !selected.contains(agent.id.as_str()) {
            continue;
        }
        let view = agent_view_preloaded(store, health, agent, Some(&mut metadata), &seats)?;
        agents.push(view);
    }
    Ok(AgentsResponse {
        agents,
        archived_count,
        gateway: crate::gateway::origin(),
    })
}

pub(crate) fn compute_block_reason(
    quota_reset_at_ms: Option<i64>,
    last_error: Option<&str>,
    now_ms: i64,
) -> Option<String> {
    if quota_reset_at_ms.is_some_and(|ms| ms > now_ms) {
        return Some("quota_exhausted".to_string());
    }
    if let Some(err) = last_error.map(str::trim).filter(|s| !s.is_empty()) {
        let lower = err.to_ascii_lowercase();
        if crate::host_memory::is_migration_wait(err) {
            return Some("memory_migration_wait".to_string());
        }
        if lower.contains("quota")
            || lower.contains("resource_exhausted")
            || lower.contains("rate_limit")
        {
            return Some("quota_exhausted".to_string());
        }
        if lower.contains("sign in")
            || lower.contains("not logged in")
            || lower.contains("unauthorized")
        {
            return Some("sign_in_required".to_string());
        }
        return Some("delivery_error".to_string());
    }
    None
}

/// The marker files an agent view shows, all in the agent's config directory.
const LAST_ERROR: &str = ".anyagents-last-error.json";
const PROFILE_ROTATED: &str = ".anyagents-profile-rotated-ms";
const QUOTA_CLEARED: &str = ".anyagents-quota-cleared-ms";
const NOTIFIED_PARENT: &str = ".anyagents-notified-parent-error.json";
const BOUND_PROFILE: &str = ".anyagents-profile";

/// One agent's marker files, as the view cache holds them.
struct Markers<'a> {
    files: &'a file_cache::FileCache,
    cfg: &'a Path,
}

impl Markers<'_> {
    fn read(&self, name: &str) -> Option<std::sync::Arc<str>> {
        self.files.file(&self.cfg.join(name))
    }

    /// A millisecond timestamp an operator or rotation marker holds; 0 when
    /// absent.
    fn ms(&self, name: &str) -> i64 {
        self.read(name)
            .and_then(|s| s.trim().parse::<i64>().ok())
            .unwrap_or(0)
    }

    /// [`create::bound_profile_name`] from the cached marker.
    fn bound_profile(&self) -> Option<String> {
        self.read(BOUND_PROFILE)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    /// Whether [`create::maybe_notify_parent_on_error`] would act for this
    /// error: the same early returns and the same episode key it compares with
    /// the notice marker. Only then does the view call it, and it re-checks
    /// both files before it writes or sends anything.
    fn parent_notice_due(&self, agent: &Agent, rotated_ms: i64, err_msg: &str) -> bool {
        let trimmed = err_msg.trim();
        if agent.parent_id.is_none()
            || trimmed.is_empty()
            || crate::host_memory::is_migration_wait(trimmed)
        {
            return false;
        }
        let source_key = format!(
            "subagent-error:{}:{rotated_ms}:{trimmed}",
            agent.id.as_str()
        );
        !self
            .read(NOTIFIED_PARENT)
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
            .is_some_and(|val| {
                val.get("source_key").and_then(|v| v.as_str()) == Some(source_key.as_str())
            })
    }
}

/// The quota reset and fault text an adapter or the launcher last persisted in
/// `.anyagents-last-error.json`. A record from before the last profile rotation
/// (for AGY, also before the last quota clear) is stale, and an expired reset
/// clears its record. Claude Code persists no reset, so only its text counts.
fn persisted_fault(
    harness: &str,
    markers: &Markers<'_>,
    rotated_ms: i64,
    now: i64,
) -> (Option<i64>, Option<String>) {
    let Some(record) = markers
        .read(LAST_ERROR)
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
    else {
        return (None, None);
    };
    let floor = if harness == "agy" {
        rotated_ms.max(markers.ms(QUOTA_CLEARED))
    } else {
        rotated_ms
    };
    let recorded_at = record
        .get("timestamp_ms")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    if recorded_at <= floor {
        return (None, None);
    }
    let error = record
        .get("error")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    if harness == "claude-code" {
        return (None, error);
    }
    match record
        .get("quota_reset_at_ms")
        .and_then(serde_json::Value::as_i64)
    {
        Some(reset_ms) if reset_ms > now => (Some(reset_ms), error),
        Some(_) => (None, None),
        None => (None, error),
    }
}

fn archived_agent_view_fast(agent: &Agent) -> AgentView {
    let delivery: crate::api::DeliveryView =
        (&crate::vendor::declared_delivery(&agent.harness)).into();
    AgentView {
        has_open_turn: false,
        open_turn_age_ms: None,
        profile: None,
        id: agent.id.to_string(),
        name: agent.name.clone(),
        role: agent.role.clone(),
        aliases: Vec::new(),
        parent: agent.parent_id.as_ref().map(ToString::to_string),
        scope: agent.scope().to_string(),
        lifecycle: agent.lifecycle().to_string(),
        relationship: None,
        can_message: None,
        harness: agent.harness.clone(),
        model: agent.model.clone(),
        live_harness: None,
        live_model: None,
        process_started_at_ms: None,
        pending_switch: false,
        workdir: agent.workdir.clone(),
        state: agent.state.clone(),
        process: "down".to_string(),
        turn_state: "idle".to_string(),
        delivery_mode: delivery.legacy_mode(),
        steer_support: delivery.legacy_steer(),
        delivery,
        quota_reset_at_ms: None,
        block_reason: None,
        last_error: None,
        oldest_undelivered_age_ms: None,
        install: "archived".to_string(),
        description: agent.public_description(),
        instructions: agent.instructions.clone(),
        seat_user: None,
        seat_uid: None,
        seat_gid: None,
        schedules: Vec::new(),
        turn_errors: None,
        unsettled_outbound: Vec::new(),
    }
}

pub(crate) fn agent_view(
    store: &Store,
    health: &HealthBoard,
    agent: &Agent,
) -> Result<AgentView, ApiError> {
    let files = file_cache::cache();
    files.catch_up();
    agent_view_preloaded(store, health, agent, None, &view_seats(files))
}

type RosterSeats = HashMap<String, (String, u32, u32, u32)>;

/// [`crate::sandbox::roster_seats`], kept by the view cache: the admitted
/// principals come from the environment or from the registry file it names.
fn view_seats(files: &file_cache::FileCache) -> std::sync::Arc<RosterSeats> {
    let registry = std::env::var_os("ANYAGENTS_PRINCIPAL_REGISTRY").map(PathBuf::from);
    match registry {
        Some(registry) if std::env::var("ANYAGENTS_PRINCIPALS").is_err() => files.fact(
            format!("seats:{}", registry.display()),
            file_cache::Dep::File(registry),
            || std::sync::Arc::new(crate::sandbox::roster_seats()),
        ),
        // Read from the environment, or nothing to read.
        _ => std::sync::Arc::new(crate::sandbox::roster_seats()),
    }
}

fn agent_view_preloaded(
    store: &Store,
    health: &HealthBoard,
    agent: &Agent,
    mut metadata: Option<&mut anyagents_core::AgentQueryMetadata>,
    seats: &RosterSeats,
) -> Result<AgentView, ApiError> {
    if agent.state == "archived" {
        let mut view = archived_agent_view_fast(agent);
        if let Some(metadata) = metadata {
            view.aliases = metadata
                .aliases
                .remove(agent.id.as_str())
                .unwrap_or_default();
        } else {
            view.aliases = store.agent_aliases(&agent.id).unwrap_or_default();
        }
        return Ok(view);
    }
    let now = now_ms();
    let lag = if let Some(metadata) = metadata.as_ref() {
        anyagents_core::Lag {
            oldest_undelivered_age_ms: metadata
                .oldest_undelivered
                .get(agent.id.as_str())
                .map(|ms| now.saturating_sub(*ms).max(0)),
        }
    } else {
        store.lag(&agent.id, now).map_err(ApiError::from_store)?
    };
    let has_open_turn = if let Some(metadata) = metadata.as_ref() {
        metadata.open_turns.contains_key(agent.id.as_str())
    } else {
        store
            .has_open_turn(&agent.id)
            .map_err(ApiError::from_store)?
    };
    let cfg = anyagents_core::agent_config_dir(store.data_dir(), &agent.id);
    let files = file_cache::cache();
    let markers = Markers { files, cfg: &cfg };
    let rotated_ms = markers.ms(PROFILE_ROTATED);
    // The supervisor's board is the live state; a fault outlives its process
    // only as the record an adapter persisted. A view never reads a vendor
    // transcript, log or session store.
    let (session_quota, session_error) = persisted_fault(&agent.harness, &markers, rotated_ms, now);
    let (
        process,
        mut turn_state,
        delivery,
        quota,
        live_harness,
        live_model,
        process_started_at_ms,
        pending_switch,
        last_error,
    ) = match health.get(&agent.id) {
        Some(live) if !crate::vendor::is_spiron_harness(&agent.harness) || live.process_alive => {
            let effective_quota = live.quota_reset_at_ms.or(session_quota);
            let effective_err = live.last_error.clone().or(session_error);
            let working = live.process_alive
                && effective_quota.is_none()
                && effective_err.is_none()
                && (live.turn_state == anyagents_harness::TurnState::InTurn || has_open_turn);
            let switch_pending = live.process_alive
                && (live
                    .live_harness
                    .as_deref()
                    .is_some_and(|h| h != agent.harness)
                    || live.live_model != agent.model
                    || live.process_started_at_ms.is_some_and(|s| rotated_ms > s));
            (
                if live.process_alive { "alive" } else { "down" },
                if working { "working" } else { "idle" },
                live.delivery
                    .clone()
                    .unwrap_or_else(|| crate::vendor::declared_delivery(&agent.harness)),
                effective_quota,
                if live.process_alive {
                    live.live_harness.clone()
                } else {
                    Some(agent.harness.clone())
                },
                if live.process_alive {
                    live.live_model.clone()
                } else {
                    agent.model.clone()
                },
                live.process_started_at_ms,
                switch_pending,
                effective_err,
            )
        }
        _ => {
            let lock_busy = crate::vendor::is_spiron_harness(&agent.harness)
                && matches!(
                    store.try_lock_agent(&agent.id),
                    Err(anyagents_core::Error::AgentBusy)
                );
            let proc_str = if lock_busy { "alive" } else { "down" };
            let turn_str = if lock_busy { "working" } else { "idle" };
            (
                proc_str,
                turn_str,
                crate::vendor::declared_delivery(&agent.harness),
                session_quota,
                if proc_str == "alive" {
                    Some(agent.harness.clone())
                } else {
                    None
                },
                if proc_str == "alive" {
                    agent.model.clone()
                } else {
                    None
                },
                None,
                false,
                session_error,
            )
        }
    };
    // Rotation rewrites the agent's profile markers; an agent whose creation
    // is in flight or failed is never written to from a view.
    if agent.state != "created"
        && let Some(reset_ms) = quota.filter(|&ms| ms > now)
    {
        quota_rotation::queue(&agent.id, reset_ms);
    }
    if (agent.state == "running" || agent.state == "idle") && quota.is_none_or(|ms| ms <= now) {
        if let Some(ref err_msg) = last_error {
            turn_state = if crate::host_memory::is_migration_wait(err_msg) {
                "idle"
            } else {
                "errored"
            };
            if markers.parent_notice_due(agent, rotated_ms, err_msg) {
                create::maybe_notify_parent_on_error(store, agent, &cfg, err_msg);
            }
        } else if markers.read(NOTIFIED_PARENT).is_some() {
            let _ = std::fs::remove_file(cfg.join(NOTIFIED_PARENT));
        }
    }
    // Turns that ended in an error, and a parking for them (docs/delivery.md).
    let turn_errors = if let Some(metadata) = metadata.as_mut() {
        metadata.turn_errors.remove(agent.id.as_str())
    } else {
        store
            .turn_errors(&agent.id)
            .ok()
            .flatten()
            .filter(anyagents_core::TurnErrors::is_current)
    };
    let parked = agent.state == "paused"
        && turn_errors
            .as_ref()
            .is_some_and(|errors| errors.faulted_at_ms.is_some());
    let last_error = if parked {
        last_error.or_else(|| turn_errors.as_ref().and_then(|e| e.last_error.clone()))
    } else {
        last_error
    };
    if parked {
        turn_state = "faulted";
    }
    let block_reason = if parked {
        Some("faulted".to_string())
    } else {
        compute_block_reason(quota, last_error.as_deref(), now)
    };
    let open_turn_age_ms = if has_open_turn {
        if let Some(metadata) = metadata.as_ref() {
            metadata
                .open_turns
                .get(agent.id.as_str())
                .map(|ms| now.saturating_sub(*ms).max(0))
        } else {
            store.open_turn(&agent.id).ok().flatten().map(|open| {
                open.messages
                    .first()
                    .map(|m| now.saturating_sub(m.received_at_ms).max(0))
                    .unwrap_or_else(|| now.saturating_sub(open.opened_at_ms).max(0))
            })
        }
    } else {
        None
    };
    let oldest_undelivered_age_ms = match lag.oldest_undelivered_age_ms {
        Some(age) => Some(age),
        None if has_open_turn
            && (quota.is_some() || last_error.is_some() || turn_state != "working") =>
        {
            open_turn_age_ms
        }
        None => None,
    };
    let aliases = if let Some(metadata) = metadata.as_mut() {
        metadata
            .aliases
            .remove(agent.id.as_str())
            .unwrap_or_default()
    } else {
        store.agent_aliases(&agent.id).unwrap_or_default()
    };
    let seat_info = seats
        .get(agent.id.as_str())
        .map(|(user, uid, gid, _)| (user.clone(), *uid, *gid));
    let schedules = if let Some(metadata) = metadata.as_mut() {
        metadata
            .schedules
            .remove(agent.id.as_str())
            .unwrap_or_default()
    } else {
        store
            .list_schedules(Some(&agent.id), true)
            .unwrap_or_default()
    }
    .into_iter()
    .map(crate::api::ScheduleView::from)
    .collect();
    Ok(AgentView {
        has_open_turn,
        open_turn_age_ms,
        profile: markers.bound_profile(),
        id: agent.id.to_string(),
        name: agent.name.clone(),
        role: agent.role.clone(),
        aliases,
        parent: agent.parent_id.as_ref().map(ToString::to_string),
        scope: agent.scope().to_string(),
        lifecycle: agent.lifecycle().to_string(),
        relationship: None,
        can_message: None,
        harness: agent.harness.clone(),
        model: agent.model.clone(),
        live_harness,
        live_model,
        process_started_at_ms,
        pending_switch,
        workdir: agent.workdir.clone(),
        state: agent.state.clone(),
        process: process.to_string(),
        turn_state: turn_state.to_string(),
        delivery_mode: crate::api::DeliveryView::from(&delivery).legacy_mode(),
        steer_support: crate::api::DeliveryView::from(&delivery).legacy_steer(),
        delivery: (&delivery).into(),
        quota_reset_at_ms: quota,
        block_reason,
        last_error,
        oldest_undelivered_age_ms,
        install: install_state(store, files, agent),
        description: agent.public_description(),
        instructions: agent.instructions.clone(),
        seat_user: seat_info.as_ref().map(|(u, _, _)| u.clone()),
        seat_uid: seat_info.as_ref().map(|(_, uid, _)| *uid),
        seat_gid: seat_info.as_ref().map(|(_, _, gid)| *gid),
        schedules,
        turn_errors: turn_errors.map(Into::into),
        unsettled_outbound: outbound_history::unsettled(store)
            .iter()
            .filter(|held| held.agent_id == agent.id)
            .map(unsettled_view)
            .collect(),
    })
}

/// `GET /v1/outbound`: every sent message whose delivery could not be
/// confirmed, held instead of resent. Operator only.
async fn outbound_list_route(
    principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<crate::api::UnsettledOutboundResponse>, ApiError> {
    require_operator(&principal)?;
    spawn_store(move || {
        Ok(Json(crate::api::UnsettledOutboundResponse {
            unsettled: outbound_history::unsettled(&state.store)
                .iter()
                .map(unsettled_view)
                .collect(),
        }))
    })
    .await
}

/// `POST /v1/outbound/{key}/retry`: send a held message again, as its agent
/// and to its destination, through the ordinary send path. The held record
/// closes with the new receipt only when that send succeeds. Operator only.
async fn outbound_retry_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(key): AxumPath<String>,
) -> Result<Json<SendResponse>, ApiError> {
    require_operator(&principal)?;
    spawn_store(move || {
        let store = &state.store;
        let mut skipped = Vec::new();
        let deliveries = outbound_history::retry(store, &key, |held| {
            if non_text_post(&held.body) {
                return Err(ApiError::new(
                    StatusCode::CONFLICT,
                    format!(
                        "{key} was a media, location, poll, checklist or article post; its record keeps only its text. Discard it and ask the agent to post it again."
                    ),
                ));
            }
            let sender = store.agent(&held.agent_id).map_err(ApiError::from_store)?;
            let actor = Actor::Agent(sender.name.clone());
            let body = SendRequest {
                to: vec![held.destination.clone()],
                text: held.body.clone(),
                sender: Some(actor.as_address()),
                reply_to: held.reply_to.clone(),
                ..SendRequest::default()
            };
            let plan = plan_send(store, &Principal::Operator, &actor, &body)?;
            let response = send_sync(store, &actor, body, plan, None)?.0;
            skipped = response.skipped;
            Ok(response.deliveries)
        })?;
        Ok(Json(SendResponse {
            deliveries,
            skipped,
        }))
    })
    .await
}

/// `POST /v1/outbound/{key}/discard`: settle a held message without sending
/// it. Operator only.
async fn outbound_discard_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(key): AxumPath<String>,
) -> Result<Json<crate::api::UnsettledOutboundView>, ApiError> {
    require_operator(&principal)?;
    spawn_store(move || {
        outbound_history::discard(&state.store, &key)
            .map(|held| Json(unsettled_view(&held)))
            .map_err(ApiError::from_store)
    })
    .await
}

/// Whether a sent body is the text record of a post that was not plain text
/// (`format_internal_send_text`): a retry from it would post its description.
fn non_text_post(body: &str) -> bool {
    [
        "[Attachment (",
        "[Location GPS:",
        "[Venue: ",
        "[Poll: \"",
        "📰 ",
    ]
    .iter()
    .any(|marker| body.contains(marker))
        || body.starts_with("☑️ **")
}

/// How the API shows an outbound message whose delivery is unconfirmed.
pub(crate) fn unsettled_view(
    held: &outbound_history::Unsettled,
) -> crate::api::UnsettledOutboundView {
    crate::api::UnsettledOutboundView {
        key: held.effect_key.clone(),
        agent_id: held.agent_id.to_string(),
        destination: held.destination.clone(),
        since_ms: held.since_ms,
        error: held.error.clone(),
        excerpt: held.excerpt(),
    }
}

/// `installed` once this harness is on the machine, `needed` when the reason it
/// is not, `pending` when nothing has been recorded yet.
fn install_state(store: &Store, files: &file_cache::FileCache, agent: &Agent) -> String {
    match crate::install::recorded(&agent.harness) {
        Some(Ok(())) => "installed".to_string(),
        Some(Err(reason)) => reason,
        None => {
            if installed_on_disk(files, store.data_dir(), &agent.harness) {
                "installed".to_string()
            } else {
                "pending".to_string()
            }
        }
    }
}

/// Whether a harness this server has not recorded is on disk: its managed
/// receipt or launcher, all under `<root>/harnesses/<id>/`. The deprecated
/// `ANYAGENTS_<ID>_BIN` override names a program outside that tree, so it is
/// checked directly.
fn installed_on_disk(files: &file_cache::FileCache, root: &Path, harness: &str) -> bool {
    let harness = anyagents_harness::normalize_harness_name(harness).to_string();
    let check = {
        let root = root.to_path_buf();
        let harness = harness.clone();
        move || {
            crate::install::is_healthy(&root, &harness)
                || crate::install::program(&root, &harness).is_file()
        }
    };
    let override_var = format!("ANYAGENTS_{}_BIN", harness.to_uppercase().replace('-', "_"));
    if std::env::var_os(override_var).is_some() {
        return check();
    }
    files.fact(
        format!("installed:{}:{harness}", root.display()),
        file_cache::Dep::Dir(root.join("harnesses").join(&harness)),
        check,
    )
}

async fn create_agent_route(
    principal: Principal,
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(mut body): Json<CreateAgentRequest>,
) -> Result<(StatusCode, Json<AgentView>), ApiError> {
    let via_http = via_http_mcp(&headers);
    let caller_agent = match &principal {
        Principal::Operator => None,
        Principal::Agent { agent_id, name } => Some((agent_id.clone(), name.clone())),
    };
    spawn_store(move || {
        let requested_groups: Vec<String> = body
            .group
            .as_deref()
            .unwrap_or("")
            .split(',')
            .map(str::trim)
            .map(|s| s.strip_prefix("group:").unwrap_or(s))
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        let requested_group_names: Vec<&str> =
            requested_groups.iter().map(String::as_str).collect();
        let group_actor = group_actor(&principal);
        state
            .store
            .check_group_joins(&group_actor, &requested_group_names)
            .map_err(ApiError::from_store)?;
        let parent_agent = match (&caller_agent, body.parent.as_deref()) {
            (_, Some("root" | "")) => {
                require_admin_or_operator(
                    &state.store,
                    &principal,
                    "agents.create_root",
                    &body.name,
                )?;
                None
            }
            (_, Some(parent_ref)) => Some(
                state
                    .store
                    .agent_by_name(parent_ref)
                    .map_err(ApiError::from_store)?,
            ),
            (Some((caller_id, _)), None) => {
                Some(state.store.agent(caller_id).map_err(ApiError::from_store)?)
            }
            (None, None) => None,
        };
        create::apply_archetype_defaults(
            Some(&state.store),
            state.store.data_dir(),
            parent_agent.as_ref(),
            &mut body,
        )
        .map_err(ApiError::from_store)?;
        if let Some(scope) = body.scope.as_deref() {
            anyagents_core::parse_agent_scope(scope).map_err(ApiError::from_store)?;
        }
        if let Some(lifecycle) = body.lifecycle.as_deref() {
            anyagents_core::parse_agent_lifecycle(lifecycle).map_err(ApiError::from_store)?;
        }
        let parent_id = parent_agent.as_ref().map(|p| p.id.clone());
        let (harness, model, profile) = create::resolve_subagent_defaults(
            state.store.data_dir(),
            parent_agent.as_ref(),
            body.harness.as_deref().unwrap_or("claude-code"),
            body.model,
            body.profile,
        );
        require_remote_callback(state.store.data_dir(), &harness)?;
        refuse_host_placement_over_http(via_http, &harness)?;
        let mut agent = create::create(
            &state.store,
            state.store.data_dir(),
            CreateRequest {
                name: body.name,
                role: body.role,
                harness,
                model,
                profile,
                repo: body.repo,
                branch: body.branch,
                base: body.base,
                workdir: body.workdir.map(PathBuf::from),
                workdir_default: owner_workdir(),
                description: body.description,
                instructions: body.instructions,
                parent_id,
            },
        )
        .map_err(ApiError::from_store)?;
        let actor = match &principal {
            Principal::Operator => "operator".to_string(),
            Principal::Agent { name, .. } => format!("agent:{name}"),
        };
        let mut scope_changed = false;
        if let Some(ref scope_val) = body.scope {
            agent = state
                .store
                .set_agent_scope_as(&agent.id, scope_val, &actor)
                .map_err(ApiError::from_store)?;
            scope_changed = true;
        } else if body.public == Some(true) {
            agent = state
                .store
                .set_agent_public_as(&agent.id, true, &actor)
                .map_err(ApiError::from_store)?;
            scope_changed = true;
        } else if body.public == Some(false) {
            agent = state
                .store
                .set_agent_public_as(&agent.id, false, &actor)
                .map_err(ApiError::from_store)?;
            scope_changed = true;
        }
        if let Some(ref lifecycle_val) = body.lifecycle {
            agent = state
                .store
                .set_agent_lifecycle_as(&agent.id, lifecycle_val, &actor)
                .map_err(ApiError::from_store)?;
        }
        if scope_changed {
            let cfg = anyagents_core::agent_config_dir(state.store.data_dir(), &agent.id);
            create::sync_agent_description_files(&state.store, &agent, &cfg)
                .map_err(ApiError::from_store)?;
        }
        if !requested_group_names.is_empty() {
            let mut targets = vec![agent.id.clone()];
            if let Some((caller_id, _)) = &caller_agent {
                targets.push(caller_id.clone());
            }
            state
                .store
                .join_groups_as(&group_actor, &requested_group_names, &targets)
                .map_err(ApiError::from_store)?;
        }
        if !anyagents_core::agent_config_dir(state.store.data_dir(), &agent.id)
            .join("mcp-capability")
            .is_file()
        {
            operator::mint(&state.store, &agent.id).map_err(ApiError::from_store)?;
        }
        let mut has_prompt = false;
        if let Some(prompt) = body
            .prompt
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            has_prompt = true;
            let sender = caller_agent
                .as_ref()
                .map(|(id, _)| format!("agent:{id}"))
                .unwrap_or_else(|| "operator".to_string());
            let enqueue = || {
                let message = state
                    .store
                    .receive(
                        &agent.id,
                        Incoming {
                            text: prompt.to_string(),
                            sender: Some(sender),
                            source_time_ms: None,
                            source_key: None,
                            reply_to: None,
                            quote: None,
                            origin: None,
                            received_at_ms: None,
                        },
                    )
                    .map_err(ApiError::from_store)?;
                let (message, duplicate) = match message {
                    ReceiveOutcome::New(message) => (message, false),
                    ReceiveOutcome::Duplicate(message) => (message, true),
                };
                Ok(vec![Delivery {
                    to: format!("agent:{}", agent.name),
                    id: message.id.to_string(),
                    seq: message.seq,
                    duplicate,
                }])
            };
            if let Some((ref caller_id, _)) = caller_agent {
                outbound_history::deliver(
                    &state.store,
                    caller_id,
                    &format!("agent:{}", agent.name),
                    None,
                    prompt,
                    None,
                    false,
                    enqueue,
                )?;
            } else {
                enqueue()?;
            }
        }
        if has_prompt {
            create::poke_agent_wake_socket_mail(state.store.data_dir(), &agent.id);
        } else {
            create::poke_agent_wake_socket(state.store.data_dir(), &agent.id);
        }
        let view = agent_view(&state.store, &state.health, &agent)?;
        Ok((StatusCode::CREATED, Json(view)))
    })
    .await
}

async fn archive_agent_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<AgentView>, ApiError> {
    spawn_store(move || {
        let clean_name = name.trim().strip_prefix("agent:").unwrap_or(name.trim());
        let found = if clean_name.eq_ignore_ascii_case("self")
            && let Principal::Agent { ref agent_id, .. } = principal
        {
            state.store.agent(agent_id).map_err(ApiError::from_store)?
        } else {
            state
                .store
                .agent_by_name(clean_name)
                .map_err(ApiError::from_store)?
        };
        let is_self =
            matches!(&principal, Principal::Agent { agent_id, .. } if agent_id == &found.id);
        if found.parent_id.is_none() && !is_self {
            require_admin_or_operator(
                &state.store,
                &principal,
                "agents.archive_root",
                &found.name,
            )?;
        } else if matches!(
            &principal,
            Principal::Agent { agent_id, .. }
                if agent_id != &found.id && !is_self_or_ancestor(&state.store, agent_id, &found)
        ) {
            require_admin_or_operator(&state.store, &principal, "agents.archive", &found.name)?;
        }
        let _lifecycle = state
            .store
            .lock_agent_lifecycle(&found.id)
            .map_err(ApiError::from_store)?;
        let agent = state
            .store
            .set_agent_state(&found.id, "archived")
            .map_err(ApiError::from_store)?;
        mark_explicitly_archived_locked(&state.store, state.store.data_dir(), &agent)
            .map_err(ApiError::from_store)?;
        if let Some(sbx) = anyagents_tools::SandboxConfig::from_config_dir(
            &anyagents_core::agent_config_dir(state.store.data_dir(), &found.id),
        ) {
            let _ = anyagents_tools::SandboxProvider::pause(&sbx);
        }
        let mut process_guard = None;
        if !is_self {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            while std::time::Instant::now() < deadline {
                if state
                    .health
                    .get(&found.id)
                    .is_none_or(|live| !live.process_alive)
                    && let Ok(guard) = state.store.try_lock_agent(&found.id)
                {
                    process_guard = Some(guard);
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            if process_guard.is_none()
                && state
                    .health
                    .get(&found.id)
                    .is_none_or(|live| !live.process_alive)
            {
                process_guard = state.store.try_lock_agent(&found.id).ok();
            }
        }
        if process_guard.is_some()
            || (is_self
                && state
                    .health
                    .get(&found.id)
                    .is_none_or(|live| !live.process_alive))
        {
            reap_archived_agent_locked(&state.store, state.store.data_dir(), &found.id);
        }
        drop(process_guard);
        Ok(Json(agent_view(&state.store, &state.health, &agent)?))
    })
    .await
}

#[cfg(test)]
pub(crate) fn reap_subagent_ephemeral_dirs(
    data_dir: &Path,
    agent_id: &anyagents_core::AgentId,
) -> bool {
    let Ok(store) = Store::open(data_dir.join("state.db")) else {
        return false;
    };
    reap_subagent_ephemeral_dirs_in(&store, data_dir, agent_id)
}

fn reap_subagent_ephemeral_dirs_in(
    store: &Store,
    data_dir: &Path,
    agent_id: &anyagents_core::AgentId,
) -> bool {
    // Delete calls this after removing the row; all surviving references still
    // protect their workdirs. A failed read must never mean no shared workdirs.
    let agent = match store.agent(agent_id) {
        Ok(agent) => Some(agent),
        Err(Error::AgentNotFound(_)) => None,
        Err(_) => return false,
    };
    let parent = match agent.as_ref().and_then(|agent| agent.parent_id.as_ref()) {
        Some(parent_id) => match store.agent(parent_id) {
            Ok(parent) => Some(parent),
            Err(Error::AgentNotFound(_)) => None,
            Err(_) => return false,
        },
        None => None,
    };
    let Ok(unarchived) = store.unarchived_workdirs() else {
        return false;
    };
    let agent = agent.as_ref();
    let mut complete = true;
    let agent_root = data_dir.join("agents").join(agent_id.as_str());
    let config_dir = agent_root.join("config");
    let configured_work = anyagents_core::agent_work_dir(data_dir, agent_id);
    let legacy_work = anyagents_core::legacy_agent_work_dir(data_dir, agent_id);
    let seat_ids = detect_seat_ids(&config_dir)
        .or_else(|| detect_seat_ids(&configured_work))
        .or_else(|| detect_seat_ids(&legacy_work));
    // Quarantine protects recovery evidence, including build-output paths that
    // would otherwise be removed after a failed Git cleanliness check.
    if crate::worktree_access::reaping_blocked(data_dir, agent_id) {
        return false;
    }
    // Revoke before any deletion, including dirty/externally located checkouts.
    // A failed revoke retains the journal and makes the reap incomplete.
    if let Err(error) = crate::worktree_access::revoke_recorded(data_dir, agent_id) {
        eprintln!("agent {agent_id}: worktree grant revocation failed: {error}");
        return false;
    }
    for rel in [
        "cache-home",
        "cache",
        ".cache",
        ".local",
        "data-home",
        "state-home",
        "muse",
        ".omp",
        ".dsh",
        ".grok",
        ".gemini",
        ".npm",
        ".bun",
        ".cargo",
        ".rustup",
        ".tmp",
        "tmp",
        ".claude",
        ".codex",
        ".config",
        "opencode",
        "projects",
        "shell-snapshots",
        "session-env",
        "todos",
        "logs",
    ] {
        let target = config_dir.join(rel);
        complete &= remove_path_with_seat(&target, seat_ids);
    }
    complete &= remove_path_with_seat(&agent_root.join("tmp"), seat_ids);
    if let Some(scratch_dir) = anyagents_core::configured_scratch_dir() {
        complete &= remove_path_with_seat(
            &anyagents_core::agent_scratch_build_dir(&scratch_dir, agent_id),
            seat_ids,
        );
        complete &= remove_path_with_seat(
            &anyagents_core::agent_scratch_tmp_dir(&scratch_dir, agent_id),
            seat_ids,
        );
    }
    if agent.is_none() {
        let _ = std::fs::remove_file(
            data_dir
                .join("locks")
                .join(format!("{}.lock", agent_id.as_str())),
        );
    }
    let mut shared_workdirs = std::collections::HashSet::new();
    if let Some(agent) = agent {
        let _ = crate::browser::stop_profile(data_dir, &agent.name);
    }
    // Every other agent that is not archived, and the parent even when it is,
    // keeps its workdir.
    let protected = unarchived
        .iter()
        .filter(|(id, _)| id != agent_id)
        .map(|(_, workdir)| workdir.as_str())
        .chain(
            parent
                .as_ref()
                .filter(|parent| &parent.id != agent_id)
                .and_then(|parent| parent.workdir.as_deref()),
        );
    for wd in protected {
        let wd_path = PathBuf::from(wd);
        if let Ok(abs) = std::path::absolute(&wd_path) {
            shared_workdirs.insert(abs);
        }
        shared_workdirs.insert(wd_path);
    }
    let marker_path = config_dir.join(".anyagents-managed-worktree.json");
    let marker_val = std::fs::read_to_string(&marker_path)
        .ok()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok());
    let recorded_wt = marker_val
        .as_ref()
        .and_then(|v| v.get("path"))
        .and_then(|v| v.as_str())
        .map(PathBuf::from)
        .map(|p| std::path::absolute(&p).unwrap_or(p));

    let mut managed_work_dirs =
        vec![std::path::absolute(&configured_work).unwrap_or_else(|_| configured_work.clone())];
    let legacy_abs = std::path::absolute(&legacy_work).unwrap_or_else(|_| legacy_work.clone());
    if !managed_work_dirs.contains(&legacy_abs) && legacy_abs.exists() {
        managed_work_dirs.push(legacy_abs);
    }
    // Never reap an external `explicit-worktree` (`wt_path != &work_dir`) such as `/scratch/wt/...`
    // that a parent or operator created and lent to a child agent. Only reap the agent's own
    // managed workspace directory when not shared with another agent.
    let journaled_any = crate::worktree_access::grant_path(data_dir, agent_id)
        .symlink_metadata()
        .is_ok();
    for (idx, work_dir) in managed_work_dirs.into_iter().enumerate() {
        if shared_workdirs.contains(&work_dir) {
            continue;
        }
        let journaled = journaled_any
            && (idx == 0
                || crate::worktree_access::RecordedWorktree::load(data_dir, agent_id, &work_dir)
                    .is_some());
        if journaled
            && crate::worktree_access::RecordedWorktree::load(data_dir, agent_id, &work_dir)
                .is_none_or(|record| !record.checkout_pointer_matches())
        {
            eprintln!(
                "agent {agent_id}: preserving entire journaled checkout {}: missing or changed .git pointer",
                work_dir.display()
            );
            return complete;
        }
        if work_dir.join(".git").exists() || journaled {
            // A missing/broken child-owned pointer cannot turn a journaled
            // checkout into a disposable non-Git directory.
            if is_git_worktree_safe_to_reap(data_dir, agent_id, &work_dir) {
                complete &= reap_git_worktree_cleanly(data_dir, agent_id, &work_dir, seat_ids);
                let _ = std::fs::remove_file(&marker_path);
            } else {
                if crate::worktree_access::reaping_blocked(data_dir, agent_id) {
                    return false;
                }
                complete &= reap_build_outputs_in_worktree(&work_dir, seat_ids);
            }
        } else {
            let mut preserved_dirty_child = false;
            if let Ok(entries) = std::fs::read_dir(&work_dir) {
                for entry in entries.flatten() {
                    let child_path = entry.path();
                    if child_path.is_dir() && child_path.join(".git").exists() {
                        if is_git_worktree_safe_to_reap(data_dir, agent_id, &child_path) {
                            complete &= reap_git_worktree_cleanly(
                                data_dir,
                                agent_id,
                                &child_path,
                                seat_ids,
                            );
                        } else {
                            if crate::worktree_access::reaping_blocked(data_dir, agent_id) {
                                return false;
                            }
                            complete &= reap_build_outputs_in_worktree(&child_path, seat_ids);
                            preserved_dirty_child = true;
                        }
                    }
                }
            }
            if !preserved_dirty_child {
                complete &= remove_path_with_seat(&work_dir, seat_ids);
                if recorded_wt.as_ref().is_none_or(|p| !p.exists()) {
                    let _ = std::fs::remove_file(&marker_path);
                }
            }
        }
    }
    complete
}

fn reap_build_outputs_in_worktree(wt_path: &Path, seat_ids: Option<(u32, u32, u32)>) -> bool {
    let mut complete = true;
    const BUILD_DIRS: &[&str] = &["target", "node_modules", ".next", ".turbo", "dist"];
    for rel in BUILD_DIRS {
        complete &= remove_path_with_seat(&wt_path.join(rel), seat_ids);
    }
    for container in ["apps", "packages", "crates", "services"] {
        let dir = wt_path.join(container);
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) => {
                complete &= error.kind() == std::io::ErrorKind::NotFound;
                continue;
            }
        };
        for entry in entries {
            let Ok(entry) = entry else {
                complete = false;
                continue;
            };
            let path = entry.path();
            if path.is_dir() {
                for rel in BUILD_DIRS {
                    complete &= remove_path_with_seat(&path.join(rel), seat_ids);
                }
            }
        }
    }
    complete
}

pub(crate) fn is_git_worktree_safe_to_reap(
    data: &Path,
    id: &anyagents_core::AgentId,
    wt_path: &Path,
) -> bool {
    let Some(record) = crate::worktree_access::RecordedWorktree::load(data, id, wt_path) else {
        // Legacy or arbitrary repositories lack trusted metadata provenance.
        // Preserve them instead of letting Git follow a child-owned .git file.
        return false;
    };
    if !record.checkout_pointer_matches() {
        eprintln!(
            "agent {id}: preserving journaled checkout {}: missing or changed .git pointer",
            wt_path.display()
        );
        return false;
    }
    record
        .inspect(|| {
            let output = |args: &[&str]| record.command()?.args(args).output();
            let status_out = output(&["status", "--porcelain", "--untracked-files=normal"])?;
            if !status_out.status.success() {
                return Ok(false);
            }
            let status_text = String::from_utf8_lossy(&status_out.stdout);
            let has_substantive_dirty = status_text.lines().any(|line| {
                let path = line.get(3..).unwrap_or("").trim();
                !matches!(
                    path,
                    "CLAUDE.md"
                        | "AGENTS.md"
                        | "GEMINI.md"
                        | ".claude"
                        | ".gemini"
                        | "node_modules"
                        | "target"
                ) && !path.starts_with(".desk-worktree")
                    && !path.starts_with(".claude/")
                    && !path.starts_with(".gemini/")
                    && !path.starts_with("node_modules/")
                    && !path.starts_with("target/")
            });
            if has_substantive_dirty {
                return Ok(false);
            }
            // Require HEAD and every local branch history to be covered by at least
            // one recorded remote-tracking ref, including detached HEAD and branches
            // not currently checked out. Shared refs remain the documented same-repo
            // integrity boundary; this is not protection from forged remote refs.
            let unpushed = output(&[
                "rev-list",
                "HEAD",
                "--branches",
                "--not",
                "--remotes",
                "--count",
            ])?;
            Ok(unpushed.status.success() && unpushed.stdout == b"0\n")
        })
        .unwrap_or(false)
}

fn reap_git_worktree_cleanly(
    data: &Path,
    id: &anyagents_core::AgentId,
    wt_path: &Path,
    seat_ids: Option<(u32, u32, u32)>,
) -> bool {
    let Some(record) = crate::worktree_access::RecordedWorktree::load(data, id, wt_path) else {
        return false;
    };
    if record.validate().is_err() {
        return false;
    }
    // Git has no separate worktree registry: these two directories are the
    // checkout and registration. Avoid `git worktree remove`, which can launch
    // its own Git subprocesses inside the seat-controlled checkout.
    let mut complete = remove_path_with_seat(wt_path, seat_ids);
    // Only the journal's validated private dir is eligible, never a path read
    // back from a child-edited pointer after the check above.
    complete &= remove_path_with_seat(record.private(), seat_ids);
    complete
}

#[cfg(unix)]
fn detect_seat_ids(config_dir: &Path) -> Option<(u32, u32, u32)> {
    use std::os::unix::fs::MetadataExt as _;
    let (me, my_gid) = std::fs::metadata("/proc/self")
        .map(|m| (m.uid(), m.gid()))
        .unwrap_or((0, 0));
    let cfg_meta = std::fs::metadata(config_dir).ok()?;
    let gid = cfg_meta.gid();
    let sock_gid = config_dir
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| std::fs::metadata(p).ok())
        .map(|m| m.gid())
        .unwrap_or(gid);
    if cfg_meta.uid() != me && cfg_meta.uid() != 0 {
        return Some((cfg_meta.uid(), gid, sock_gid));
    }
    if let Ok(entries) = std::fs::read_dir(config_dir) {
        for entry in entries.flatten() {
            if let Ok(m) = entry.metadata() {
                if m.uid() != me && m.uid() != 0 {
                    return Some((m.uid(), gid, sock_gid));
                }
                if m.is_dir()
                    && let Ok(sub) = std::fs::read_dir(entry.path())
                {
                    for child in sub.flatten() {
                        if let Ok(cm) = child.metadata()
                            && cm.uid() != me
                            && cm.uid() != 0
                        {
                            return Some((cm.uid(), gid, sock_gid));
                        }
                    }
                }
            }
        }
    }
    if gid != my_gid && gid != 0 {
        return Some((gid, gid, sock_gid));
    }
    None
}

#[cfg(not(unix))]
fn detect_seat_ids(_config_dir: &Path) -> Option<(u32, u32, u32)> {
    None
}

fn remove_path_with_seat(target: &Path, seat_ids: Option<(u32, u32, u32)>) -> bool {
    let meta = match std::fs::symlink_metadata(target) {
        Ok(meta) => meta,
        Err(error) => return error.kind() == std::io::ErrorKind::NotFound,
    };
    if meta.file_type().is_symlink() || meta.is_file() {
        if std::fs::remove_file(target).is_ok() {
            return true;
        }
    } else if meta.is_dir() && std::fs::remove_dir_all(target).is_ok() {
        return true;
    }
    if !Path::new("/usr/local/bin/anyagents-setpriv").is_file() {
        return false;
    }
    let mut candidate_seats = Vec::new();
    if let Some(ids) = seat_ids {
        candidate_seats.push(ids);
    }
    #[cfg(unix)]
    if let Some(target_ids) = detect_seat_ids(target)
        && !candidate_seats.contains(&target_ids)
    {
        candidate_seats.push(target_ids);
    }
    for (uid, gid, sock_gid) in candidate_seats {
        let uid_s = uid.to_string();
        let gid_s = gid.to_string();
        let groups_s = if sock_gid != gid && sock_gid != 0 {
            format!("{gid},{sock_gid}")
        } else {
            gid_s.clone()
        };
        let _ = std::process::Command::new("/usr/local/bin/anyagents-setpriv")
            .args([
                "--reuid",
                &uid_s,
                "--regid",
                &gid_s,
                "--groups",
                &groups_s,
                "--no-new-privs",
                "--",
                "/bin/rm",
                "-rf",
            ])
            .arg(target)
            .stderr(std::process::Stdio::null())
            .status();
        if std::fs::remove_dir_all(target).is_ok()
            || std::fs::symlink_metadata(target)
                .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
        {
            return true;
        }
        let _ = std::fs::remove_file(target);
    }
    std::fs::symlink_metadata(target)
        .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
}

async fn pause_agent_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<AgentView>, ApiError> {
    spawn_store(move || {
        let found = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        if found.parent_id.is_none() {
            if matches!(&principal, Principal::Agent { agent_id, .. } if agent_id == &found.id) {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "a root agent cannot pause itself; pass a target agent name or id in `agent`",
                ));
            }
            require_admin_or_operator(&state.store, &principal, "agents.pause_root", &found.name)?;
        } else if matches!(
            &principal,
            Principal::Agent { agent_id, .. }
                if agent_id != &found.id && !is_self_or_ancestor(&state.store, agent_id, &found)
        ) {
            require_admin_or_operator(&state.store, &principal, "agents.pause", &found.name)?;
        }
        let agent = state
            .store
            .set_agent_state(&found.id, "paused")
            .map_err(ApiError::from_store)?;
        if let Some(sbx) = anyagents_tools::SandboxConfig::from_config_dir(
            &anyagents_core::agent_config_dir(state.store.data_dir(), &found.id),
        ) {
            let _ = anyagents_tools::SandboxProvider::pause(&sbx);
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            if state
                .health
                .get(&found.id)
                .is_none_or(|live| !live.process_alive)
                && state.store.try_lock_agent(&found.id).is_ok()
            {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        Ok(Json(agent_view(&state.store, &state.health, &agent)?))
    })
    .await
}

async fn delete_agent_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        let found = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        let _lifecycle = state
            .store
            .lock_agent_lifecycle(&found.id)
            .map_err(ApiError::from_store)?;
        let found = state.store.agent(&found.id).map_err(ApiError::from_store)?;
        if found.parent_id.is_none() {
            if matches!(&principal, Principal::Agent { agent_id, .. } if agent_id == &found.id) {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "a root agent cannot delete itself",
                ));
            }
            require_admin_or_operator(&state.store, &principal, "agents.delete_root", &found.name)?;
        } else if matches!(
            &principal,
            Principal::Agent { agent_id, .. }
                if agent_id != &found.id && !is_self_or_ancestor(&state.store, agent_id, &found)
        ) {
            require_admin_or_operator(&state.store, &principal, "agents.delete", &found.name)?;
        }
        state
            .store
            .set_agent_state(&found.id, "archived")
            .map_err(ApiError::from_store)?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        // Health is a projection: the supervisor may publish stopped before
        // its worker exits. Allow that normal shutdown to release actual
        // process custody within the same deadline, retaining the guard to commit.
        let _process = loop {
            if state
                .health
                .get(&found.id)
                .is_none_or(|live| !live.process_alive)
            {
                match state.store.try_lock_agent(&found.id) {
                    Ok(process) => break process,
                    Err(anyagents_core::Error::AgentBusy) => {}
                    Err(error) => return Err(ApiError::from_store(error)),
                }
            }
            if std::time::Instant::now() >= deadline {
                return Err(ApiError::new(
                    StatusCode::CONFLICT,
                    "agent is archived but its process has not stopped; identity and resources are retained",
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        };
        crate::worktree_access::revoke_recorded(state.store.data_dir(), &found.id)
            .map_err(ApiError::from_store)?;
        let deleted = state
            .store
            .delete_agent(&found.id)
            .map_err(ApiError::from_store)?;
        let agent_dir = state
            .store
            .data_dir()
            .join("agents")
            .join(deleted.id.as_str());
        if let Some(sbx) = anyagents_tools::SandboxConfig::from_config_dir(&agent_dir.join("config"))
        {
            let _ = anyagents_tools::SandboxProvider::destroy(&sbx);
        }
        let seat_ids = detect_seat_ids(&agent_dir.join("config"));
        let _ = crate::browser::stop_profile(state.store.data_dir(), &deleted.name);
        remove_path_with_seat(
            &state
                .store
                .data_dir()
                .join("browser-profiles")
                .join(&deleted.name),
            seat_ids,
        );
        reap_subagent_ephemeral_dirs_in(&state.store, state.store.data_dir(), &deleted.id);
        for private_tree in ["config", "memory", "skills"] {
            let tree_path = agent_dir.join(private_tree);
            remove_path_with_seat(&tree_path, seat_ids);
            let _ = std::fs::remove_dir_all(&tree_path);
        }
        if !agent_dir.join("work").exists() {
            remove_path_with_seat(&agent_dir, seat_ids);
            let _ = std::fs::remove_dir_all(&agent_dir);
        }
        Ok(Json(serde_json::json!({
            "id": deleted.id.as_str(),
            "name": deleted.name,
            "deleted": true,
        })))
    })
    .await
}

async fn wake_agent_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    headers: axum::http::HeaderMap,
) -> Result<Json<AgentView>, ApiError> {
    let via_http = via_http_mcp(&headers);
    spawn_store(move || {
        let found = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        if via_http && !runs_off_host(state.store.data_dir(), &found) {
            return Err(ApiError::forbidden(format!(
                "`{}` runs on this host; a call over HTTP MCP cannot wake it",
                found.name
            )));
        }
        if let Principal::Agent {
            agent_id: ref caller_id,
            name: ref caller_name,
        } = principal
        {
            let actor = Actor::Agent(caller_name.clone());
            if !is_self_or_ancestor(&state.store, caller_id, &found)
                && can_actor_message_agent(&state.store, &actor, &found).is_err()
            {
                require_admin_or_operator(&state.store, &principal, "agents.wake", &found.name)?;
            }
        }
        let _lifecycle = state
            .store
            .lock_agent_lifecycle(&found.id)
            .map_err(ApiError::from_store)?;
        let found = state.store.agent(&found.id).map_err(ApiError::from_store)?;
        prepare_agent_wake(&state.store, &found)?;
        let agent = state
            .store
            .set_agent_state(&found.id, "running")
            .map_err(ApiError::from_store)?;
        create::poke_agent_wake_socket(state.store.data_dir(), &agent.id);
        Ok(Json(agent_view(&state.store, &state.health, &agent)?))
    })
    .await
}

/// Restore the same runtime and archive-marker state for an explicit wake or
/// an intentional direct send. The caller commits `running` only after this
/// succeeds; direct sends commit it atomically with their mailbox append.
fn prepare_agent_wake(store: &Store, found: &Agent) -> Result<(), ApiError> {
    let cfg = anyagents_core::agent_config_dir(store.data_dir(), &found.id);
    if let Some(sbx) = anyagents_tools::SandboxConfig::from_config_dir(&cfg) {
        let _ = anyagents_tools::SandboxProvider::resume(&sbx);
    }
    let work_dir = found
        .workdir
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(|| anyagents_core::agent_work_dir(store.data_dir(), &found.id));
    if !work_dir.exists() {
        let _ = std::fs::create_dir_all(&work_dir);
    }
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
        || std::env::var_os("ANYAGENTS_SUPPRESS_CONFIG_WRITE").is_some()
    {
        create::reconcile_agent_runtime_config(store, store.data_dir(), found, &cfg, &work_dir)
            .map_err(|err| ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, err.to_string()))?;
    }
    let _ = std::fs::remove_file(cfg.join(".anyagents-last-error.json"));
    let _ = std::fs::remove_file(cfg.join(".anyagents-notified-parent-error.json"));
    let now = now_ms();
    create::write_profile_marker(&cfg, ".anyagents-quota-cleared-ms", &format!("{now}\n"))
        .map_err(ApiError::from_store)?;
    if !store.has_open_turn(&found.id).unwrap_or(false) {
        create::write_profile_marker(&cfg, ".anyagents-profile-rotated-ms", &format!("{now}\n"))
            .map_err(ApiError::from_store)?;
    }
    crate::worktree_access::restore_recorded(store.data_dir(), &found.id)
        .map_err(ApiError::from_store)?;
    clear_explicitly_archived(store.data_dir(), &found.id);
    // The gateway starts with routes for the live fleet only.
    if found.state == "archived"
        && let Err(err) = crate::gateway::add_route(store.data_dir(), found)
    {
        eprintln!("agent {}: gateway route not restored: {err}", found.id);
    }
    Ok(())
}

fn build_group_roster(store: &Store, members: &[String]) -> Vec<crate::api::GroupMemberView> {
    let mut out = Vec::with_capacity(members.len());
    for name in members {
        if let Ok(agent) = store.agent_by_name(name) {
            let description = agent.public_description();
            out.push(crate::api::GroupMemberView {
                id: agent.id.to_string(),
                name: agent.name,
                role: agent.role,
                harness: agent.harness,
                model: agent.model,
                state: agent.state,
                description,
            });
        }
    }
    out
}

async fn list_groups_route(
    _principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<GroupsResponse>, ApiError> {
    spawn_store(move || {
        let rows = state.store.list_groups().map_err(ApiError::from_store)?;
        let groups = rows
            .into_iter()
            .map(|(name, members)| {
                let public = state.store.is_group_public(&name);
                let roster = build_group_roster(&state.store, &members);
                Ok(GroupView {
                    owner_agent_id: state
                        .store
                        .group(&name)
                        .map_err(ApiError::from_store)?
                        .and_then(|g| g.owner_agent_id)
                        .map(|id| id.to_string()),
                    name,
                    public,
                    members,
                    roster,
                })
            })
            .collect::<Result<Vec<_>, ApiError>>()?;
        Ok(Json(GroupsResponse { groups }))
    })
    .await
}

fn group_actor(principal: &Principal) -> anyagents_core::GroupActor {
    match principal {
        Principal::Operator => anyagents_core::GroupActor::Operator,
        Principal::Agent { agent_id, .. } => anyagents_core::GroupActor::Agent(agent_id.clone()),
    }
}

async fn group_action_route(
    principal: Principal,
    State(state): State<AppState>,
    Json(body): Json<GroupActionRequest>,
) -> Result<Json<GroupView>, ApiError> {
    let default_agent = match &principal {
        Principal::Agent { name, .. } => Some(name.clone()),
        Principal::Operator => None,
    };
    spawn_store(move || {
        let group = body
            .group
            .trim()
            .strip_prefix("group:")
            .unwrap_or(body.group.trim())
            .to_string();
        if group.is_empty() {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "group name is empty",
            ));
        }
        let action = match body.action.trim() {
            "join" => anyagents_core::GroupAction::Join,
            "leave" => anyagents_core::GroupAction::Leave,
            "members" => anyagents_core::GroupAction::Members,
            other => {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    format!("unknown group action: {other}"),
                ));
            }
        };
        let actor = group_actor(&principal);
        let mut targets = Vec::new();
        if action != anyagents_core::GroupAction::Members {
            let who_raw = body
                .agent
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .or(default_agent)
                .ok_or_else(|| {
                    ApiError::new(StatusCode::BAD_REQUEST, "group action needs an agent name")
                })?;
            for who in who_raw
                .split(',')
                .map(str::trim)
                .map(|s| s.strip_prefix("agent:").unwrap_or(s))
                .filter(|s| !s.is_empty())
            {
                let agent = if who.eq_ignore_ascii_case("self")
                    && let Principal::Agent { ref agent_id, .. } = principal
                {
                    state.store.agent(agent_id).map_err(ApiError::from_store)?
                } else {
                    state
                        .store
                        .agent_by_name(who)
                        .map_err(ApiError::from_store)?
                };
                if action == anyagents_core::GroupAction::Join
                    && let Principal::Agent { ref name, .. } = principal
                {
                    can_actor_message_agent(&state.store, &Actor::Agent(name.clone()), &agent)?;
                }
                targets.push(agent.id);
            }
        }
        state
            .store
            .group_action_as(&actor, &group, action, &targets, body.public)
            .map_err(ApiError::from_store)?;
        let public = state.store.is_group_public(&group);
        let members = state
            .store
            .room_members(&group)
            .map_err(ApiError::from_store)?
            .unwrap_or_default();
        let roster = build_group_roster(&state.store, &members);
        Ok(Json(GroupView {
            owner_agent_id: state
                .store
                .group(&group)
                .map_err(ApiError::from_store)?
                .and_then(|g| g.owner_agent_id)
                .map(|id| id.to_string()),
            name: group,
            public,
            members,
            roster,
        }))
    })
    .await
}

#[derive(Deserialize)]
struct Since {
    since: Option<i64>,
    limit: Option<usize>,
    unread: Option<bool>,
    sender: Option<String>,
}

#[derive(Default, Deserialize)]
struct ConversationQuery {
    all: Option<bool>,
    raw_id: Option<String>,
    raw_sha256: Option<String>,
    raw_offset: Option<u64>,
    raw_length: Option<u64>,
    #[serde(default)]
    scope: anyagents_core::host_index::MemoryScope,
    #[serde(default, alias = "memory_topic", alias = "slug", alias = "file")]
    topic: Option<String>,
    #[serde(default, alias = "summary", alias = "memory_description")]
    description: Option<String>,
    #[serde(default, alias = "body", alias = "text", alias = "memory_content")]
    content: Option<String>,
    #[serde(default, alias = "remove")]
    delete: Option<bool>,
    with: Option<String>,
    since_ms: Option<i64>,
    until_ms: Option<i64>,
    from_seq: Option<i64>,
    to_seq: Option<i64>,
    direction: Option<String>,
    sender: Option<String>,
    include_tools: Option<bool>,
    only_tools: Option<bool>,
    tool_name: Option<String>,
    sort: Option<String>,
    radius: Option<usize>,
    limit: Option<usize>,
    #[serde(default, alias = "q", alias = "search")]
    query: Option<String>,
    #[serde(default)]
    queries: Option<String>,
    #[serde(default)]
    exclude: Option<String>,
}

async fn inbox(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Query(query): Query<Since>,
) -> Result<Json<InboxResponse>, ApiError> {
    // An agent's `inbox` is strictly its own incoming queue. To inspect another
    // agent's full two-way communication (`in` + `out` with users, groups, and
    // peers), agents use `/v1/agents/{name}/conversation`.
    spawn_store(move || {
        let agent_id = match &principal {
            Principal::Operator => {
                state
                    .store
                    .agent_by_name(&name)
                    .map_err(ApiError::from_store)?
                    .id
            }
            Principal::Agent {
                agent_id,
                name: own_name,
            } => {
                let matches_self = name == "self"
                    || name == agent_id.as_str()
                    || name == own_name.as_str()
                    || state
                        .store
                        .agent_by_name(&name)
                        .is_ok_and(|a| a.id == *agent_id);
                if !matches_self {
                    return Err(ApiError::forbidden(
                        "a session reads its own inbox, not another agent's; use conversation to read another agent's conversation",
                    ));
                }
                agent_id.clone()
            }
        };
        inbox_of(
            &state.store,
            &agent_id,
            query.since,
            query.limit,
            query.unread.unwrap_or(false),
            query.sender.as_deref(),
        )
    })
    .await
}

/// `conversation.redact`: authenticated ancestors and admins only. The store
/// repeats authorization inside the write transaction; peers and self are denied.
async fn redact_conversation_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(request): Json<anyagents_core::redaction::Request>,
) -> Result<Json<anyagents_core::redaction::Receipt>, ApiError> {
    spawn_store(move || {
        let target = state
            .store
            .agent_by_name(name.strip_prefix("agent:").unwrap_or(&name))
            .map_err(ApiError::from_store)?;
        let actor = match principal {
            Principal::Operator => anyagents_core::redaction::Actor::Operator,
            Principal::Agent { agent_id, .. } => anyagents_core::redaction::Actor::Agent(agent_id),
        };
        state
            .store
            .redact_conversation(actor, &target.id, &request)
            .map(Json)
            .map_err(ApiError::from_store)
    })
    .await
}

async fn conversation_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Query(mut query): Query<ConversationQuery>,
) -> Result<Json<ConversationResponse>, ApiError> {
    if query.all == Some(true) {
        query.limit = None;
    }
    spawn_store(move || {
        if let Some(topic) = query
            .topic
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            let is_write = query.content.is_some() || query.delete == Some(true);
            if !is_write {
                let (resolved_name, visible_agent_ids) =
                    conversation_visibility(&state.store, &principal, &name)?;
                let target = if resolved_name.eq_ignore_ascii_case("self")
                    || resolved_name.eq_ignore_ascii_case("all")
                    || resolved_name == "*"
                {
                    match &principal {
                        Principal::Agent { agent_id, .. } => {
                            state.store.agent(agent_id).map_err(ApiError::from_store)?
                        }
                        Principal::Operator => {
                            return Err(ApiError::new(
                                StatusCode::BAD_REQUEST,
                                "operator memory topic read requires an explicit agent name",
                            ));
                        }
                    }
                } else {
                    state
                        .store
                        .agent_by_name(
                            resolved_name
                                .strip_prefix("agent:")
                                .unwrap_or(&resolved_name),
                        )
                        .map_err(ApiError::from_store)?
                };
                if let Some(visible) = visible_agent_ids.as_ref()
                    && !visible
                        .iter()
                        .any(|v| v == target.id.as_str() || v == &target.name)
                {
                    return Err(ApiError::forbidden(
                        "cannot read private memory of another agent",
                    ));
                }
                let agent_root = state
                    .store
                    .data_dir()
                    .join("agents")
                    .join(target.id.as_str());
                let is_index_read = topic.eq_ignore_ascii_case("memory")
                    || topic.eq_ignore_ascii_case("memory.md")
                    || topic.eq_ignore_ascii_case("index");
                let (slug, filename) = if is_index_read {
                    ("MEMORY".to_string(), "MEMORY.md".to_string())
                } else {
                    let s = anyagents_core::learned_memory::sanitize_topic_slug(topic)
                        .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, e.to_string()))?;
                    let f = format!("{s}.md");
                    (s, f)
                };
                let path = agent_root.join("memory").join(&filename);
                if state
                    .store
                    .is_host_path_redacted(target.id.as_str(), &path)
                    .unwrap_or(true)
                {
                    return Err(ApiError::new(
                        StatusCode::NOT_FOUND,
                        format!("memory topic `{filename}` not found"),
                    ));
                }
                let mut bytes = Vec::new();
                let timestamp_ms = match anyagents_core::raw_segment::open_readonly(&path) {
                    Ok(mut file) => {
                        use std::io::Read as _;
                        file.read_to_end(&mut bytes)
                            .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, e.to_string()))?;
                        let text_preview = String::from_utf8_lossy(&bytes);
                        let fm_updated_ms =
                            anyagents_core::learned_memory::parse_topic_frontmatter_full(
                                &slug,
                                &text_preview,
                            )
                            .3
                            .filter(|&ts| ts > 0);
                        fm_updated_ms.or_else(|| {
                            file.metadata()
                                .ok()
                                .and_then(|m| m.modified().ok())
                                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                                .map(|d| d.as_millis() as i64)
                        })
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound && is_index_read => {
                        let rendered = format!(
                            "# {} — Learned Memory Index\n- No topic files recorded yet.\n",
                            target.name
                        );
                        bytes = rendered.into_bytes();
                        Some(anyagents_core::now_ms())
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        return Err(ApiError::new(
                            StatusCode::NOT_FOUND,
                            format!("memory topic `{filename}` not found"),
                        ));
                    }
                    Err(e) => {
                        return Err(ApiError::new(StatusCode::BAD_REQUEST, e.to_string()));
                    }
                };
                let text = String::from_utf8_lossy(&bytes).into_owned();
                let digest = anyagents_core::learned_memory::sha256(&bytes);
                let doc_id = format!(
                    "memory:{}:{}",
                    target.id,
                    anyagents_core::learned_memory::sha256(path.as_os_str().as_encoded_bytes())
                );
                let coord = anyagents_core::raw_segment::RawCoordinate {
                    segment: path,
                    offset: 0,
                    length: bytes.len() as u64,
                    sha256: digest,
                    seq: 1,
                };
                return Ok(Json(ConversationResponse {
                    agent: target.name.clone(),
                    agent_id: target.id.to_string(),
                    turns: vec![ConversationTurnView {
                        seq: 0,
                        id: doc_id,
                        text,
                        raw_bytes: None,
                        source: Some("memory".into()),
                        direction: "in".into(),
                        from: format!("agent:{}", target.name),
                        to: format!("agent:{}", target.name),
                        reply_to: Some(filename),
                        timestamp_ms,
                        raw: Some(coord.clone().into()),
                        window: Some(coord.into()),
                        tool_name: None,
                        tool_names: Vec::new(),
                        role: Some("memory".into()),
                    }],
                }));
            }
            let target = if name.eq_ignore_ascii_case("self")
                || name.eq_ignore_ascii_case("all")
                || name == "*"
            {
                match &principal {
                    Principal::Agent { agent_id, .. } => {
                        state.store.agent(agent_id).map_err(ApiError::from_store)?
                    }
                    Principal::Operator => {
                        return Err(ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "operator memory write requires an explicit agent name",
                        ));
                    }
                }
            } else {
                state
                    .store
                    .agent_by_name(name.strip_prefix("agent:").unwrap_or(&name))
                    .map_err(ApiError::from_store)?
            };
            let allowed = match &principal {
                Principal::Operator => true,
                Principal::Agent { agent_id, .. } => {
                    is_self_or_ancestor(&state.store, agent_id, &target)
                }
            };
            if !allowed {
                return Err(ApiError::forbidden(
                    "memory write requires self, ancestor, or operator authority",
                ));
            }
            let _guard = state
                .store
                .lock_agent_lifecycle(&target.id)
                .map_err(ApiError::from_store)?;
            memory_backlog::mark(state.store.data_dir(), &target.id);
            let agent_root = state
                .store
                .data_dir()
                .join("agents")
                .join(target.id.as_str());
            let now_ms = anyagents_core::now_ms();
            let turn = if query.delete == Some(true) {
                let (removed, index) = anyagents_core::learned_memory::delete_memory_topic(
                    &agent_root,
                    &target.name,
                    target.role.as_deref(),
                    topic,
                )
                .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, e.to_string()))?;
                let _ = state.store.sync_agent_memory_index(&target);
                ConversationTurnView {
                    seq: 0,
                    id: format!("memory:{}:deleted:{}", target.id, topic),
                    text: format!("deleted={removed}\n{index}"),
                    raw_bytes: None,
                    source: Some("memory".into()),
                    direction: "in".into(),
                    from: format!("agent:{}", target.name),
                    to: format!("agent:{}", target.name),
                    reply_to: None,
                    timestamp_ms: Some(now_ms),
                    raw: None,
                    window: None,
                    tool_name: None,
                    tool_names: Vec::new(),
                    role: Some("memory".into()),
                }
            } else {
                let body_text = query.content.as_deref().unwrap_or_default();
                let (path, entry, _) = anyagents_core::learned_memory::upsert_memory_topic(
                    &agent_root,
                    &target.name,
                    target.role.as_deref(),
                    topic,
                    query.description.as_deref(),
                    body_text,
                    now_ms,
                )
                .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, e.to_string()))?;
                let _ = state.store.sync_agent_memory_index(&target);
                let bytes = std::fs::read(&path).unwrap_or_default();
                let digest = anyagents_core::learned_memory::sha256(&bytes);
                let doc_id = format!(
                    "memory:{}:{}",
                    target.id,
                    anyagents_core::learned_memory::sha256(path.as_os_str().as_encoded_bytes())
                );
                let coord = anyagents_core::raw_segment::RawCoordinate {
                    segment: path,
                    offset: 0,
                    length: bytes.len() as u64,
                    sha256: digest,
                    seq: 1,
                };
                ConversationTurnView {
                    seq: 0,
                    id: doc_id,
                    text: String::from_utf8_lossy(&bytes).into_owned(),
                    raw_bytes: None,
                    source: Some("memory".into()),
                    direction: "in".into(),
                    from: format!("agent:{}", target.name),
                    to: format!("agent:{}", target.name),
                    reply_to: Some(entry.filename),
                    timestamp_ms: Some(now_ms),
                    raw: Some(coord.clone().into()),
                    window: Some(coord.into()),
                    tool_name: None,
                    tool_names: Vec::new(),
                    role: Some("memory".into()),
                }
            };
            return Ok(Json(ConversationResponse {
                agent: target.name,
                agent_id: target.id.to_string(),
                turns: vec![turn],
            }));
        }
        let (name, visible_agent_ids) = conversation_visibility(&state.store, &principal, &name)?;
        if query.raw_id.is_none()
            && (query.raw_sha256.is_some()
                || query.raw_offset.is_some()
                || query.raw_length.is_some())
        {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "raw_sha256/raw_offset/raw_length require raw_id",
            ));
        }
        if let Some(id) = query.raw_id.as_deref() {
            let digest = query
                .raw_sha256
                .as_deref()
                .filter(|s| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()))
                .ok_or_else(|| {
                    ApiError::new(
                        StatusCode::BAD_REQUEST,
                        "raw_sha256 must be the 64-character digest from the search result",
                    )
                })?;
            let length = query
                .raw_length
                .unwrap_or(anyagents_core::raw_segment::MAX_RAW_RANGE_BYTES);
            if id.trim().is_empty()
                || length == 0
                || length > anyagents_core::raw_segment::MAX_RAW_RANGE_BYTES
            {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "raw_id must be nonempty; raw_length must be 1..=65536",
                ));
            }
            let authorized = match visible_agent_ids.as_ref() {
                Some(ids) => ids.clone(),
                None => every_agent_id(&state.store)?,
            };
            let (hit, bytes) = state
                .store
                .read_host_file_range(
                    &authorized,
                    query.scope,
                    Some(&name),
                    anyagents_core::host_retrieval::HostRawRead {
                        id,
                        sha256: digest,
                        offset: query.raw_offset.unwrap_or(0),
                        length,
                    },
                )
                .map_err(|e| {
                    ApiError::new(
                        match &e {
                            anyagents_core::host_index::IndexError::Io(e)
                                if matches!(
                                    e.kind(),
                                    std::io::ErrorKind::WouldBlock
                                        | std::io::ErrorKind::InvalidData
                                ) =>
                            {
                                StatusCode::CONFLICT
                            }
                            _ => StatusCode::BAD_REQUEST,
                        },
                        e.to_string(),
                    )
                })?
                .ok_or_else(|| {
                    ApiError::new(
                        StatusCode::NOT_FOUND,
                        "raw document not found in authorized scope",
                    )
                })?;
            let agent = state
                .store
                .agent_by_name(&hit.metadata.agent_id)
                .map_err(ApiError::from_store)?;
            return Ok(Json(ConversationResponse {
                agent: agent.name,
                agent_id: agent.id.to_string(),
                turns: vec![ConversationTurnView {
                    seq: hit.raw.seq as i64,
                    id: hit.id,
                    text: String::from_utf8_lossy(&bytes).into_owned(),
                    raw_bytes: Some(bytes),
                    source: Some(
                        if hit.metadata.source == anyagents_core::host_index::MemoryScope::Memory {
                            "memory"
                        } else {
                            "transcripts"
                        }
                        .into(),
                    ),
                    direction: hit.metadata.direction,
                    from: hit.metadata.sender,
                    to: hit.metadata.destination,
                    reply_to: None,
                    timestamp_ms: hit.metadata.timestamp_ms,
                    raw: Some(hit.raw.into()),
                    window: Some(hit.window.into()),
                    tool_name: hit.metadata.tool_name,
                    tool_names: hit.metadata.tool_names,
                    role: hit.metadata.role,
                }],
            }));
        }
        let clean_q = query
            .query
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let extra_queries: Vec<String> = query
            .queries
            .as_deref()
            .unwrap_or("")
            .split('|')
            .flat_map(|s| s.split(','))
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        let exclude_terms: Vec<String> = query
            .exclude
            .as_deref()
            .unwrap_or("")
            .split('|')
            .flat_map(|s| s.split(','))
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        let has_structured_search = clean_q.is_some()
            || !extra_queries.is_empty()
            || !exclude_terms.is_empty()
            || query.until_ms.is_some()
            || query.from_seq.is_some()
            || query.to_seq.is_some()
            || query.direction.is_some()
            || query.sender.is_some()
            || query.include_tools == Some(false)
            || query.only_tools == Some(true)
            || query.tool_name.is_some()
            || query.sort.is_some()
            || query.radius.is_some_and(|r| r > 0);
        let is_all = name.eq_ignore_ascii_case("all") || name == "*";
        let file_request = anyagents_core::HistorySearchQuery {
            query: clean_q.map(str::to_string),
            queries: extra_queries.clone(),
            exclude: exclude_terms.clone(),
            agent: Some(name.clone()),
            with_peer: query.with.clone(),
            direction: query.direction.clone(),
            sender: query.sender.clone(),
            include_tools: query.include_tools.unwrap_or(true),
            only_tools: query.only_tools.unwrap_or(false),
            tool_name: query.tool_name.clone(),
            since_ms: query.since_ms,
            until_ms: query.until_ms,
            from_seq: query.from_seq,
            to_seq: query.to_seq,
            sort: anyagents_core::HistorySort::parse(query.sort.as_deref()),
            radius: query.radius.unwrap_or(0),
            limit: query
                .limit
                .unwrap_or(if has_structured_search { 20 } else { 50 })
                .max(1),
            visible_agent_ids: visible_agent_ids.clone(),
        };
        let authorized = match visible_agent_ids.as_ref() {
            Some(ids) => ids.clone(),
            None => every_agent_id(&state.store)?,
        };
        let should_search_host_files = has_structured_search
            || query.scope != anyagents_core::host_index::MemoryScope::All
            || std::env::var_os("ANYAGENTS_TESTS").is_some();
        let file_hits = if should_search_host_files {
            state
                .store
                .search_host_files(&authorized, query.scope, &file_request)
                .map_err(|e| ApiError::from_store(Error::Message(e.to_string())))?
        } else {
            Vec::new()
        };

        let (agent_name, agent_id_str, raw_turns) =
            if query.scope == anyagents_core::host_index::MemoryScope::Memory {
                if is_all {
                    ("all".into(), "all".into(), Vec::new())
                } else {
                    let a = state
                        .store
                        .agent_by_name(&name)
                        .map_err(ApiError::from_store)?;
                    (a.name, a.id.to_string(), Vec::new())
                }
            } else if has_structured_search {
                let search_req = anyagents_core::HistorySearchQuery {
                    query: clean_q.map(str::to_string),
                    queries: extra_queries,
                    exclude: exclude_terms,
                    agent: Some(name.clone()),
                    with_peer: query.with.clone(),
                    direction: query.direction.clone(),
                    sender: query.sender.clone(),
                    include_tools: query.include_tools.unwrap_or(true),
                    only_tools: query.only_tools.unwrap_or(false),
                    tool_name: query.tool_name.clone(),
                    since_ms: query.since_ms,
                    until_ms: query.until_ms,
                    from_seq: query.from_seq,
                    to_seq: query.to_seq,
                    sort: anyagents_core::HistorySort::parse(query.sort.as_deref()),
                    radius: query.radius.unwrap_or(0),
                    limit: if query.all == Some(true) {
                        0
                    } else {
                        query.limit.unwrap_or(20)
                    },
                    visible_agent_ids: visible_agent_ids.clone(),
                };
                let turns = state
                    .store
                    .search_history(&search_req)
                    .map_err(ApiError::from_store)?;
                let (aname, aid) = if is_all {
                    ("all".to_string(), "all".to_string())
                } else if let Ok(a) = state.store.agent_by_name(&name) {
                    (a.name, a.id.to_string())
                } else {
                    (name.clone(), name.clone())
                };
                (aname, aid, turns)
            } else if is_all {
                let turns = state
                    .store
                    .conversation_all_scoped(
                        query.with.as_deref(),
                        query.since_ms,
                        query.limit,
                        visible_agent_ids.as_deref(),
                    )
                    .map_err(ApiError::from_store)?;
                ("all".to_string(), "all".to_string(), turns)
            } else {
                let agent = state
                    .store
                    .agent_by_name(&name)
                    .map_err(ApiError::from_store)?;
                let turns = state
                    .store
                    .conversation_scoped(
                        &agent.id,
                        query.with.as_deref(),
                        query.since_ms,
                        query.limit,
                        visible_agent_ids.as_deref(),
                    )
                    .map_err(ApiError::from_store)?;
                (agent.name, agent.id.to_string(), turns)
            };
        let turns: Vec<_> = raw_turns
            .into_iter()
            .map(|t| ConversationTurnView {
                seq: t.seq,
                raw_bytes: None,
                source: Some("transcripts".into()),
                raw: None,
                window: None,
                tool_name: None,
                tool_names: Vec::new(),
                role: None,
                id: t.id,
                direction: t.direction,
                from: t.from,
                to: t.to,
                reply_to: t.reply_to,
                text: t.text,
                timestamp_ms: Some(t.timestamp_ms),
            })
            .collect();
        let mut ranked: Vec<_> = turns
            .into_iter()
            .enumerate()
            .map(|(rank, t)| (1.0 / (30.0 + rank as f64), t))
            .collect();
        for (rank, (hit, bytes)) in file_hits.into_iter().enumerate() {
            let reply_to = (hit.metadata.source == anyagents_core::host_index::MemoryScope::Memory)
                .then(|| {
                    hit.raw
                        .segment
                        .file_name()
                        .and_then(|s| s.to_str())
                        .map(str::to_string)
                })
                .flatten();
            ranked.push((
                1.0 / (30.0 + rank as f64),
                ConversationTurnView {
                    seq: 0,
                    raw_bytes: None,
                    id: hit.id,
                    direction: hit.metadata.direction,
                    from: hit.metadata.sender,
                    to: hit.metadata.destination,
                    reply_to,
                    text: String::from_utf8_lossy(&bytes).into_owned(),
                    timestamp_ms: hit.metadata.timestamp_ms,
                    source: Some(
                        if hit.metadata.source == anyagents_core::host_index::MemoryScope::Memory {
                            "memory"
                        } else {
                            "transcripts"
                        }
                        .into(),
                    ),
                    raw: Some(hit.raw.into()),
                    window: Some(hit.window.into()),
                    tool_name: hit.metadata.tool_name,
                    tool_names: hit.metadata.tool_names,
                    role: hit.metadata.role,
                },
            ));
        }
        let lexical = clean_q.is_some() || !file_request.queries.is_empty();
        if lexical
            && file_request.sort == anyagents_core::HistorySort::Relevance
            && file_request.radius == 0
        {
            ranked.sort_by(|a, b| {
                b.0.total_cmp(&a.0)
                    .then_with(|| b.1.timestamp_ms.cmp(&a.1.timestamp_ms))
            });
        } else {
            ranked.sort_by_key(|(_, t)| t.timestamp_ms);
            if file_request.sort == anyagents_core::HistorySort::Newest {
                ranked.reverse();
            }
        }
        if query.all != Some(true) && file_request.radius == 0 && ranked.len() > file_request.limit
        {
            if !lexical && file_request.sort == anyagents_core::HistorySort::Relevance {
                ranked.drain(..ranked.len() - file_request.limit);
            } else {
                ranked.truncate(file_request.limit);
            }
        }
        let turns = ranked.into_iter().map(|(_, t)| t).collect();
        Ok(Json(ConversationResponse {
            agent: agent_name,
            agent_id: agent_id_str,
            turns,
        }))
    })
    .await
}

/// One agent's messages past `since`, in sequence order.
fn inbox_of(
    store: &Store,
    agent_id: &anyagents_core::AgentId,
    since: Option<i64>,
    limit: Option<usize>,
    unread: bool,
    sender: Option<&str>,
) -> Result<Json<InboxResponse>, ApiError> {
    let rows = store
        .inbox_query_filtered(agent_id, since, limit, unread, sender)
        .map_err(ApiError::from_store)?;
    let messages = rows
        .into_iter()
        .map(|message| MessageView {
            id: message.id.to_string(),
            seq: message.seq,
            text: message.text,
            sender: message.sender,
            reply_to: message.reply_to,
            source_time_ms: message.source_time_ms,
            received_at_ms: message.received_at_ms,
            delivered_at_ms: message.delivered_at_ms,
            ack: message.ack.to_string(),
        })
        .collect();
    Ok(Json(InboxResponse { messages }))
}

async fn send(
    principal: Principal,
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(body): Json<SendRequest>,
) -> Result<Json<SendResponse>, ApiError> {
    let via_http = via_http_mcp(&headers);
    if via_http {
        refuse_host_media_over_http(&body)?;
    }
    // The authenticated principal decides who the message is from; a body
    // `sender` is only a check, and a disagreement is refused with no write.
    // On a machine without the boundary, its own agent route names the sender;
    // that is the one place a body chooses the author, and an inbox is still
    // only ever its own.
    spawn_store(move || {
        let route_named = match &principal {
            Principal::Agent { name, .. } if state.auth.is_trusted() => body
                .sender
                .as_deref()
                .and_then(|named| Actor::parse(named).ok())
                .filter(|named| named.as_address() != Actor::Agent(name.clone()).as_address()),
            _ => None,
        };
        let actor = match (&principal, route_named) {
            (_, Some(named)) => named,
            (Principal::Agent { agent_id, name }, None) => {
                if let Some(named) = body.sender.as_deref() {
                    Actor::parse(named).and_then(|named| {
                        let matches_self = named.as_address()
                            == Actor::Agent(name.clone()).as_address()
                            || match &named {
                                Actor::Agent(n) => state
                                    .store
                                    .agent_by_name(n)
                                    .is_ok_and(|a| a.id == *agent_id),
                                _ => false,
                            };
                        if matches_self {
                            Ok(())
                        } else {
                            Err(ApiError::forbidden(
                                "a session sends as itself, not as the sender it names",
                            ))
                        }
                    })?;
                }
                Actor::Agent(name.clone())
            }
            (Principal::Operator, _) => {
                let sender = body.sender.as_deref().ok_or_else(|| {
                    ApiError::new(
                        StatusCode::BAD_REQUEST,
                        "an operator send needs an explicit sender",
                    )
                })?;
                Actor::parse(sender)?
            }
        };
        let body = SendRequest {
            sender: Some(actor.as_address()),
            ..body
        };
        let mut plan = plan_send(&state.store, &principal, &actor, &body)?;
        // Over HTTP MCP a send never revives an archived host agent: that
        // would start a host process on the call's behalf. Such a send is
        // refused for it with the reason, as any send that cannot wake an
        // archived recipient is.
        plan.revive_host_agents = !via_http;
        let key_body = SendRequest {
            sender: plan
                .sender_id
                .as_ref()
                .map(|id| format!("agent:{id}"))
                .or_else(|| body.sender.clone()),
            ..body.clone()
        };
        let key = send_key(&key_body);
        send_sync(&state.store, &actor, body, plan, key)
    })
    .await
}

async fn interact_route(
    principal: Principal,
    State(state): State<AppState>,
    Json(body): Json<InteractRequest>,
) -> Result<Json<InteractResponse>, ApiError> {
    let actor_name = match &principal {
        Principal::Agent { name, .. } => name.clone(),
        Principal::Operator => {
            let sender = body.sender.as_deref().ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "an operator interact call needs an explicit sender",
                )
            })?;
            sender
                .trim()
                .strip_prefix("agent:")
                .unwrap_or(sender.trim())
                .to_string()
        }
    };
    let store = Arc::clone(&state.store);
    spawn_store(move || interact_sync(&store, &principal, &actor_name, body)).await
}

fn interact_sync(
    store: &Store,
    principal: &Principal,
    actor_name: &str,
    body: InteractRequest,
) -> Result<Json<InteractResponse>, ApiError> {
    let action = body.action.trim().to_ascii_lowercase();
    let raw_target = body.target.trim();
    if raw_target.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "interact requires a non-empty target",
        ));
    }
    let actor = Actor::Agent(actor_name.to_string());
    // Resolve target into optional (chat_id, message_id, thread_id, stored_message).
    let mut tg_coords: Option<(i64, i64, Option<i64>)> = None;
    let mut stored_msg: Option<anyagents_core::Message> = None;
    if let Some(msg_id_str) = raw_target
        .strip_prefix("anyagents:reply:")
        .or_else(|| raw_target.starts_with("msg_").then_some(raw_target))
    {
        if let Ok(Some(msg)) = own_message(store, principal, &actor, msg_id_str) {
            if let Some(ref origin) = msg.origin
                && let (Some(chat_id), Some(message_id)) = (origin.chat_id, origin.message_id)
            {
                tg_coords = Some((chat_id, message_id, origin.thread_id));
            }
            stored_msg = Some(msg);
        } else if let Ok(agent_row) = store.agent_by_name(actor_name)
            && let Ok(Some(msg)) = store.message(&agent_row.id, msg_id_str)
        {
            if let Some(ref origin) = msg.origin
                && let (Some(chat_id), Some(message_id)) = (origin.chat_id, origin.message_id)
            {
                tg_coords = Some((chat_id, message_id, origin.thread_id));
            }
            stored_msg = Some(msg);
        }
    } else if let Some(rest) = raw_target
        .strip_prefix("telegram:chat:")
        .or_else(|| raw_target.strip_prefix("telegram:user:"))
    {
        let parts: Vec<&str> = rest.split(':').collect();
        if parts.len() >= 2
            && let (Ok(chat_id), Ok(message_id)) = (
                parts[0].parse::<i64>(),
                parts[parts.len() - 1].parse::<i64>(),
            )
        {
            let thread_id = if parts.len() == 3 {
                parts[1].parse::<i64>().ok()
            } else {
                None
            };
            tg_coords = Some((chat_id, message_id, thread_id));
        }
    }
    let data_dir = store.data_dir();
    match action.as_str() {
        "react" => {
            let emoji = body.emoji.as_deref().map(str::trim);
            if let Some((chat_id, message_id, thread_id)) = tg_coords {
                crate::telegram::react_message(
                    store, data_dir, actor_name, chat_id, message_id, emoji, thread_id,
                )
                .map_err(ApiError::from_store)?;
                Ok(Json(InteractResponse {
                    ok: true,
                    action,
                    target: body.target,
                    detail: Some(format!(
                        "reacted {} on telegram:chat:{chat_id}:{message_id}",
                        emoji.unwrap_or("(cleared)")
                    )),
                }))
            } else if let Some(msg) = stored_msg {
                let e = emoji.filter(|s| !s.is_empty()).unwrap_or("👍");
                let summary = format!("[Reaction {e} on message {}]", msg.id);
                if let Some(ref sender) = msg.sender
                    && let Some(peer_name) = sender.strip_prefix("agent:")
                    && let Ok(peer) = store.agent_by_name(peer_name)
                    && peer.state != "archived"
                    && peer.state != "paused"
                    && !is_explicitly_archived(store.data_dir(), &peer)
                    && can_actor_message_agent(store, &actor, &peer).is_ok()
                {
                    let mut incoming = Incoming::new(summary.clone());
                    incoming.sender = Some(format!("agent:{actor_name}"));
                    incoming.reply_to = msg.reply_to.clone();
                    store
                        .receive(&peer.id, incoming)
                        .map_err(ApiError::from_store)?;
                }
                Ok(Json(InteractResponse {
                    ok: true,
                    action,
                    target: body.target,
                    detail: Some(summary),
                }))
            } else {
                Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "react target must be anyagents:reply:msg_... or telegram:chat:<chat_id>:<message_id>",
                ))
            }
        }
        "vote" => {
            if body.options.is_empty() {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "vote requires at least one option in `options`",
                ));
            }
            let resolved_opts =
                resolve_poll_options(stored_msg.as_ref().map(|m| m.text.as_str()), &body.options);
            let comment = body
                .text
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty());
            let ballot = match comment {
                Some(note) => format!("🗳️ **Poll Vote**: {} — {note}", resolved_opts.join(", ")),
                None => format!("🗳️ **Poll Vote**: {}", resolved_opts.join(", ")),
            };
            if let Some((chat_id, message_id, thread_id)) = tg_coords {
                let _ = crate::telegram::react_message(
                    store,
                    data_dir,
                    actor_name,
                    chat_id,
                    message_id,
                    Some("👍"),
                    thread_id,
                );
                let posted_id = crate::telegram::post_text(
                    store,
                    data_dir,
                    actor_name,
                    chat_id,
                    &ballot,
                    Some(message_id),
                    thread_id,
                )
                .map_err(ApiError::from_store)?;
                Ok(Json(InteractResponse {
                    ok: true,
                    action,
                    target: body.target,
                    detail: Some(format!(
                        "voted ({}) on telegram:chat:{chat_id}:{message_id} (receipt #{posted_id})",
                        resolved_opts.join(", ")
                    )),
                }))
            } else if let Some(msg) = stored_msg {
                if let Some(ref sender) = msg.sender
                    && let Some(peer_name) = sender.strip_prefix("agent:")
                    && let Ok(peer) = store.agent_by_name(peer_name)
                    && peer.state != "archived"
                    && peer.state != "paused"
                    && !is_explicitly_archived(store.data_dir(), &peer)
                    && can_actor_message_agent(store, &actor, &peer).is_ok()
                {
                    let mut incoming = Incoming::new(ballot.clone());
                    incoming.sender = Some(format!("agent:{actor_name}"));
                    incoming.reply_to = msg.reply_to.clone();
                    store
                        .receive(&peer.id, incoming)
                        .map_err(ApiError::from_store)?;
                }
                Ok(Json(InteractResponse {
                    ok: true,
                    action,
                    target: body.target,
                    detail: Some(ballot),
                }))
            } else {
                Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "vote target must be anyagents:reply:msg_... or telegram:chat:<chat_id>:<message_id>",
                ))
            }
        }
        "check" | "uncheck" => {
            if body.options.is_empty() {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "check/uncheck requires at least one task number or text in `options`",
                ));
            }
            let mark_done = action == "check";
            if let Some(ref msg) = stored_msg {
                let updated = toggle_checklist_items(&msg.text, &body.options, mark_done);
                if let Some((chat_id, message_id, thread_id)) = tg_coords {
                    crate::telegram::edit_message(
                        store, data_dir, actor_name, chat_id, message_id, &updated, thread_id,
                    )
                    .map_err(ApiError::from_store)?;
                }
                Ok(Json(InteractResponse {
                    ok: true,
                    action,
                    target: body.target,
                    detail: Some(updated),
                }))
            } else {
                Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "check/uncheck target must resolve to a stored checklist message",
                ))
            }
        }
        "edit" => {
            let new_text = body
                .text
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    ApiError::new(StatusCode::BAD_REQUEST, "edit requires non-empty `text`")
                })?;
            let (chat_id, message_id, thread_id) = tg_coords.ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "edit target must resolve to a Telegram message",
                )
            })?;
            crate::telegram::edit_message(
                store, data_dir, actor_name, chat_id, message_id, new_text, thread_id,
            )
            .map_err(ApiError::from_store)?;
            Ok(Json(InteractResponse {
                ok: true,
                action,
                target: body.target,
                detail: Some(format!("edited telegram:chat:{chat_id}:{message_id}")),
            }))
        }
        "delete" => {
            let (chat_id, message_id, _) = tg_coords.ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "delete target must resolve to a Telegram message",
                )
            })?;
            crate::telegram::delete_chat_message(store, data_dir, actor_name, chat_id, message_id)
                .map_err(ApiError::from_store)?;
            Ok(Json(InteractResponse {
                ok: true,
                action,
                target: body.target,
                detail: Some(format!("deleted telegram:chat:{chat_id}:{message_id}")),
            }))
        }
        "pin" | "unpin" => {
            let (chat_id, message_id, _) = tg_coords.ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "pin/unpin target must resolve to a Telegram message",
                )
            })?;
            let pin = action == "pin";
            crate::telegram::pin_message(store, data_dir, actor_name, chat_id, message_id, pin)
                .map_err(ApiError::from_store)?;
            Ok(Json(InteractResponse {
                ok: true,
                action,
                target: body.target,
                detail: Some(format!(
                    "{} telegram:chat:{chat_id}:{message_id}",
                    if pin { "pinned" } else { "unpinned" }
                )),
            }))
        }
        "stop_poll" | "close_poll" => {
            let (chat_id, message_id, _) = tg_coords.ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "stop_poll target must resolve to a Telegram poll message",
                )
            })?;
            crate::telegram::close_poll(store, data_dir, actor_name, chat_id, message_id)
                .map_err(ApiError::from_store)?;
            Ok(Json(InteractResponse {
                ok: true,
                action,
                target: body.target,
                detail: Some(format!("closed poll telegram:chat:{chat_id}:{message_id}")),
            }))
        }
        other => Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            format!(
                "unsupported interact action `{other}`; expected react, vote, check, uncheck, edit, delete, pin, unpin, or stop_poll"
            ),
        )),
    }
}

fn toggle_checklist_items(message_text: &str, selected: &[String], mark_done: bool) -> String {
    let target_box = if mark_done { "☑" } else { "☐" };
    message_text
        .lines()
        .map(|line| {
            let trimmed = line.trim_start();
            let rest = trimmed
                .strip_prefix("☐ ")
                .or_else(|| trimmed.strip_prefix("☑ "));
            if let Some(after_box) = rest {
                let idx_str = after_box.split('.').next().unwrap_or("").trim();
                let matched = selected.iter().any(|sel| {
                    let s = sel.trim();
                    s == idx_str
                        || after_box
                            .to_ascii_lowercase()
                            .contains(&s.to_ascii_lowercase())
                });
                if matched {
                    return format!("{target_box} {after_box}");
                }
            }
            line.to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn resolve_poll_options(message_text: Option<&str>, chosen: &[String]) -> Vec<String> {
    let mut parsed_options: Vec<String> = Vec::new();
    if let Some(text) = message_text
        && let Some(idx) = text.find("| Options: ")
    {
        let rest = &text[idx + "| Options: ".len()..];
        let end = rest.rfind(']').unwrap_or(rest.len());
        for item in rest[..end].split("; ") {
            let cleaned = item.split(" (").next().unwrap_or(item).trim().to_string();
            if !cleaned.is_empty() {
                parsed_options.push(cleaned);
            }
        }
    }
    chosen
        .iter()
        .map(|sel| {
            let trimmed = sel.trim();
            if let Ok(num) = trimmed.parse::<usize>()
                && num >= 1
                && num <= parsed_options.len()
            {
                return parsed_options[num - 1].clone();
            }
            trimmed.to_string()
        })
        .collect()
}

/// One send, resolved to the destinations it will write, before any of them is
/// validated or posted. `reply_to` is a mailbox reference the server resolves
/// against this agent's own mailbox, or a destination written plainly.
fn plan_send(
    store: &Store,
    principal: &Principal,
    actor: &Actor,
    body: &SendRequest,
) -> Result<SendPlan, ApiError> {
    let authenticated = match principal {
        Principal::Agent { agent_id, .. } => Some(
            store
                .agent(agent_id)
                .map_err(|_| ApiError::forbidden("authenticated agent no longer exists"))?,
        ),
        Principal::Operator => None,
    };
    let principal_id = authenticated.as_ref().map(|agent| agent.id.clone());
    let own_sender = match (principal, actor, authenticated.as_ref()) {
        (Principal::Agent { name, .. }, Actor::Agent(actor_name), Some(agent))
            if actor_name == name
                || actor_name == &agent.name
                || store
                    .agent_by_name(actor_name)
                    .is_ok_and(|resolved| resolved.id == agent.id) =>
        {
            Some(agent)
        }
        _ => None,
    };
    let mut plan = if let Some(sender) = own_sender {
        SendPlan::parse_for_sender(store, actor, body, Some(sender.clone()))?
    } else {
        SendPlan::parse(store, actor, body)?
    };
    plan.principal_id = principal_id;
    let has_telegram_target = plan
        .targets
        .iter()
        .any(|t| matches!(t, Target::Telegram { .. }));
    let reply = match crate::reply::resolve(
        principal,
        |id| own_message(store, principal, actor, id),
        body.reply_to.as_deref(),
    ) {
        Ok(dest) => dest,
        Err(_why) if !plan.targets.is_empty() && !has_telegram_target => None,
        Err(why) => return Err(ApiError::new(StatusCode::BAD_REQUEST, why)),
    };
    if let Some(destination) = reply {
        if plan.targets.is_empty() {
            plan.targets = vec![target_of(&destination)];
            plan.reply = destination.reply().map(str::to_string);
        } else {
            // When `to` addresses are explicitly given alongside `reply_to`:
            // - A Telegram target must name the same `chat` as a Telegram reply
            //   destination (so a group reply never goes to a private Telegram
            //   chat or another Telegram chat), and inherits the recorded topic
            //   and message id for that chat when not specified on the target.
            // - Internal `agent:<name>` and `group:<room>` targets in `to` are
            //   honoured as written without wiping out `plan.targets`.
            let mut matched_telegram = false;
            for target in &mut plan.targets {
                if let Target::Telegram {
                    address,
                    chat,
                    thread,
                    reply,
                } = target
                {
                    match &destination {
                        crate::reply::Destination::Telegram {
                            chat: wanted_chat,
                            thread: wanted_thread,
                            reply: wanted_reply,
                        } if chat == wanted_chat => {
                            if thread.is_none() {
                                *thread = wanted_thread.clone();
                                *address = destination.address();
                            }
                            if reply.is_none() {
                                *reply = wanted_reply.clone();
                            }
                            matched_telegram = true;
                        }
                        other => {
                            return Err(ApiError::new(
                                StatusCode::BAD_REQUEST,
                                format!(
                                    "this reply names one place ({}), and the addresses name another ({address})",
                                    other.address()
                                ),
                            ));
                        }
                    }
                }
            }
            if matched_telegram {
                plan.reply = destination.reply().map(str::to_string);
            }
        }
    }
    Ok(plan)
}

/// One message of the mailbox a reply speaks for, by id: the calling agent's
/// own mailbox, or, for the operator, the agent it is sending as.
fn own_message(
    store: &Store,
    principal: &Principal,
    actor: &Actor,
    id: &str,
) -> Result<Option<anyagents_core::Message>, String> {
    let agent_id = match (principal, actor) {
        (Principal::Agent { agent_id, .. }, _) => agent_id.clone(),
        (_, Actor::Agent(name)) => store.agent_by_name(name).map_err(|err| err.to_string())?.id,
        (Principal::Operator, Actor::Operator(_)) => return Ok(None),
    };
    store.message(&agent_id, id).map_err(|err| err.to_string())
}

fn target_of(destination: &crate::reply::Destination) -> Target {
    match destination {
        crate::reply::Destination::Telegram {
            chat,
            thread,
            reply,
        } => Target::Telegram {
            address: destination.address(),
            chat: chat.clone(),
            thread: thread.clone(),
            reply: reply.clone(),
        },
        crate::reply::Destination::Agent(name) => Target::Agent(name.clone()),
        crate::reply::Destination::Group(name) => Target::Room(name.clone()),
    }
}

/// One send, resolved to the destinations it will write, before any of them is
/// validated or posted.
#[derive(Debug)]
struct SendPlan {
    targets: Vec<Target>,
    /// The provider message id a reply answers, when the reply named one.
    reply: Option<String>,
    /// Immutable identity admitted at planning; a reused display name is not authority.
    sender_id: Option<anyagents_core::AgentId>,
    principal_id: Option<anyagents_core::AgentId>,
    /// Whether an authored send may revive an archived agent that runs on
    /// this host. False for a call made over HTTP MCP.
    revive_host_agents: bool,
}

#[derive(Debug)]
enum Target {
    Agent(String),
    Room(String),
    /// The operator's inbox: agents write, only the operator reads.
    Operator,
    Telegram {
        address: String,
        chat: String,
        thread: Option<String>,
        reply: Option<String>,
    },
}

impl SendPlan {
    fn parse(store: &Store, actor: &Actor, body: &SendRequest) -> Result<Self, ApiError> {
        let sender = match actor {
            Actor::Agent(name) => Some(
                store
                    .agent_by_name(name)
                    .map_err(|_| ApiError::forbidden("sender agent no longer exists"))?,
            ),
            Actor::Operator(_) => None,
        };
        Self::parse_for_sender(store, actor, body, sender)
    }

    fn parse_for_sender(
        store: &Store,
        _actor: &Actor,
        body: &SendRequest,
        sender: Option<Agent>,
    ) -> Result<Self, ApiError> {
        if body.to.is_empty() && body.reply_to.is_none() {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "send needs an address",
            ));
        }
        let mut targets = Vec::with_capacity(body.to.len());
        for raw in &body.to {
            match Address::parse(raw)
                .map_err(Error::from)
                .map_err(ApiError::from_store)?
            {
                Address::Agent(name)
                    if matches!(name.as_str(), "all" | "peers" | "siblings" | "team") =>
                {
                    if let Some(sender) = &sender
                        && let Some(ref parent_id) = sender.parent_id
                    {
                        let siblings: Vec<_> = store
                            .children_of(parent_id)
                            .map_err(ApiError::from_store)?
                            .into_iter()
                            .filter(|c| {
                                c.id != sender.id
                                    && c.state != "archived"
                                    && !is_explicitly_archived(store.data_dir(), c)
                                    && can_sender_message_agent(store, sender, c).is_ok()
                            })
                            .collect();
                        if siblings.is_empty() {
                            return Err(ApiError::forbidden(
                                "no reachable sibling sub-agents (`public: true` or shared group); message your parent (`to: [\"parent\"]`) instead",
                            ));
                        }
                        for sib in siblings {
                            targets.push(Target::Agent(sib.name));
                        }
                    } else if name == "siblings" {
                        return Err(ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "root agents have no parent; use `peers` or `group:owners` to message root peers",
                        ));
                    } else {
                        targets.push(Target::Room("all".to_string()));
                    }
                }
                Address::Group(room)
                    if matches!(room.as_str(), "all" | "peers" | "siblings" | "team") =>
                {
                    if let Some(sender) = &sender
                        && let Some(ref parent_id) = sender.parent_id
                    {
                        let siblings: Vec<_> = store
                            .children_of(parent_id)
                            .map_err(ApiError::from_store)?
                            .into_iter()
                            .filter(|c| {
                                c.id != sender.id
                                    && c.state != "archived"
                                    && !is_explicitly_archived(store.data_dir(), c)
                                    && can_sender_message_agent(store, sender, c).is_ok()
                            })
                            .collect();
                        if siblings.is_empty() {
                            return Err(ApiError::forbidden(
                                "no reachable sibling sub-agents (`public: true` or shared group); message your parent (`to: [\"parent\"]`) instead",
                            ));
                        }
                        for sib in siblings {
                            targets.push(Target::Agent(sib.name));
                        }
                    } else if room == "siblings" {
                        return Err(ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "root agents have no parent; use `peers` or `group:owners` to message root peers",
                        ));
                    } else {
                        targets.push(Target::Room("all".to_string()));
                    }
                }
                Address::Agent(name) if name == "parent" => {
                    let Some(sender) = &sender else {
                        return Err(ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "only an agent has a parent",
                        ));
                    };
                    let parent_id = sender.parent_id.as_ref().ok_or_else(|| {
                        ApiError::new(StatusCode::BAD_REQUEST, "this agent has no parent")
                    })?;
                    let parent = store
                        .agent_by_name(parent_id.as_str())
                        .map_err(ApiError::from_store)?;
                    targets.push(Target::Agent(parent.name));
                }
                Address::Agent(name) if name == "children" => {
                    let Some(sender) = &sender else {
                        return Err(ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "only an agent has children",
                        ));
                    };
                    let children: Vec<_> = store
                        .children_of(&sender.id)
                        .map_err(ApiError::from_store)?
                        .into_iter()
                        .filter(|c| {
                            c.state != "archived" && !is_explicitly_archived(store.data_dir(), c)
                        })
                        .collect();
                    if children.is_empty() {
                        return Err(ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "this agent has no active children",
                        ));
                    }
                    for child in children {
                        targets.push(Target::Agent(child.name));
                    }
                }
                Address::Group(room) if room == "children" => {
                    let Some(sender) = &sender else {
                        return Err(ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "only an agent has children",
                        ));
                    };
                    let children: Vec<_> = store
                        .children_of(&sender.id)
                        .map_err(ApiError::from_store)?
                        .into_iter()
                        .filter(|c| {
                            c.state != "archived" && !is_explicitly_archived(store.data_dir(), c)
                        })
                        .collect();
                    if children.is_empty() {
                        return Err(ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "this agent has no active children",
                        ));
                    }
                    for child in children {
                        targets.push(Target::Agent(child.name));
                    }
                }
                Address::Agent(name) if name == "operator" => {
                    if !targets.iter().any(|t| matches!(t, Target::Operator)) {
                        targets.push(Target::Operator);
                    }
                }
                Address::Agent(name) => targets.push(Target::Agent(name)),
                Address::TelegramChat { chat, thread } => {
                    targets.push(Target::Telegram {
                        address: raw.clone(),
                        chat,
                        thread,
                        reply: None,
                    });
                }
                Address::TelegramUser {
                    user,
                    message: reply,
                } => {
                    targets.push(Target::Telegram {
                        address: raw.clone(),
                        chat: user,
                        thread: None,
                        reply,
                    });
                }
                Address::Group(room) => targets.push(Target::Room(room)),
            }
        }
        Ok(Self {
            targets,
            reply: None,
            sender_id: sender.as_ref().map(|agent| agent.id.clone()),
            principal_id: None,
            revive_host_agents: true,
        })
    }
}

fn can_actor_read_agent_private(
    store: &Store,
    principal: &Principal,
    target: &anyagents_core::Agent,
) -> bool {
    match principal {
        Principal::Operator => true,
        Principal::Agent { agent_id, .. } => {
            is_self_or_ancestor(store, agent_id, target)
                || store
                    .agent(agent_id)
                    .is_ok_and(|caller| caller.is_admin && caller.parent_id.is_none())
        }
    }
}

/// Enforces sub-agent communication encapsulation across scopes (`isolated`, `private`, `siblings`, `public`):
/// - An `isolated` sub-agent can only message itself, its direct parent, its own direct children, or a shared group/chat.
/// - Otherwise, any agent can message:
///   1. Itself (`sender.id == recipient.id`)
///   2. Its own direct Child Sub-Agents (`recipient.parent_id == Some(sender.id)`)
///   3. Its own direct Parent Agent (`sender.parent_id == Some(recipient.id)`)
///   4. A Sibling Sub-Agent under the same Parent when both have `scope: "siblings"` or `"public"` (`allows_sibling_messages()`)
///   5. Any Root Agent (when `sender.allows_sibling_messages()`) or Sub-Agent with `scope: "public"` (`recipient.is_public_subagent()`)
///   6. Any agent with which it shares an internal virtual group (`group:<name>`) or external Telegram chat
fn can_actor_message_agent(
    store: &Store,
    actor: &Actor,
    recipient: &anyagents_core::Agent,
) -> Result<(), ApiError> {
    let Actor::Agent(sender_name) = actor else {
        return Ok(());
    };
    let sender = store
        .agent_by_name(sender_name)
        .map_err(|_| ApiError::forbidden("sender agent no longer exists"))?;
    can_sender_message_agent(store, &sender, recipient)
}

fn can_sender_message_agent(
    store: &Store,
    sender: &Agent,
    recipient: &Agent,
) -> Result<(), ApiError> {
    can_sender_message_agent_with(store, sender, recipient, || {
        store.share_any_group_or_chat(&sender.id, &recipient.id)
    })
}

/// [`can_sender_message_agent`] with the group-or-chat share answered by the
/// caller, so a roster can answer it once for every recipient.
fn can_sender_message_agent_with(
    store: &Store,
    sender: &Agent,
    recipient: &Agent,
    share_group_or_chat: impl FnOnce() -> bool,
) -> Result<(), ApiError> {
    if sender.id == recipient.id
        || recipient.parent_id.as_ref() == Some(&sender.id)
        || sender.parent_id.as_ref() == Some(&recipient.id)
    {
        return Ok(());
    }
    if sender.is_isolated() || recipient.is_isolated() {
        let sender_parent = sender
            .parent_id
            .as_ref()
            .and_then(|pid| store.agent(pid).ok().map(|p| p.name))
            .unwrap_or_else(|| "parent".to_string());
        return Err(ApiError::forbidden(format!(
            "sub-agent `{}` or `{}` is isolated (`scope: isolated`) and only communicates 1-to-1 with its direct parent and children; report to your parent `agent:{sender_parent}` (`to: [\"parent\"]`) instead",
            sender.name, recipient.name
        )));
    }
    if share_group_or_chat() || store.has_messaged(recipient, sender) {
        return Ok(());
    }
    let same_parent_siblings = sender.parent_id.is_some()
        && sender.parent_id == recipient.parent_id
        && sender.allows_sibling_messages()
        && recipient.allows_sibling_messages();
    if same_parent_siblings
        || recipient.is_public_subagent()
        || (recipient.parent_id.is_none() && sender.allows_sibling_messages())
    {
        return Ok(());
    }
    if recipient.parent_id.is_none() {
        let sender_parent = sender
            .parent_id
            .as_ref()
            .and_then(|pid| store.agent(pid).ok().map(|p| p.name))
            .unwrap_or_else(|| "parent".to_string());
        return Err(ApiError::forbidden(format!(
            "sub-agent `{}` is private (`public: false`) and cannot message root agent `{}` directly; report to your parent `agent:{sender_parent}` (`to: [\"parent\"]`) instead",
            sender.name, recipient.name
        )));
    }
    let owner_label = recipient
        .parent_id
        .as_ref()
        .and_then(|pid| store.agent(pid).ok().map(|p| p.name))
        .unwrap_or_else(|| "parent".to_string());
    Err(ApiError::forbidden(format!(
        "cannot message sub-agent `{}` directly because it belongs to parent `agent:{owner_label}` and does not accept external messages (`public: false`); message `agent:{owner_label}` instead, ask `agent:{owner_label}` to set `public: true`, or collaborate in a shared group",
        recipient.name
    )))
}

/// Every destination is checked before any write, so a refused address never
/// leaves half a send behind.
fn check_targets(
    store: &Store,
    actor: &Actor,
    body: &SendRequest,
    plan: &SendPlan,
) -> Result<(), ApiError> {
    // Authorization uses the immutable identity retained during planning, not
    // a mutable name that might have been assigned to a replacement agent.
    let sender =
        match actor {
            Actor::Agent(_) => Some(
                store
                    .agent(plan.sender_id.as_ref().ok_or_else(|| {
                        ApiError::forbidden("agent send has no admitted identity")
                    })?)
                    .map_err(|_| ApiError::forbidden("sender agent no longer exists"))?,
            ),
            Actor::Operator(_) => None,
        };
    for target in &plan.targets {
        match target {
            Target::Agent(name) => {
                let recipient = store.agent_by_name(name).map_err(ApiError::from_store)?;
                if let Some(sender) = &sender {
                    can_sender_message_agent(store, sender, &recipient)?;
                }
            }
            // Counted without writing, so an empty room fails the whole send
            // instead of silently delivering nothing.
            Target::Room(room) => {
                let clean_room = room.strip_prefix("group:").unwrap_or(room);
                if let Some(sender) = &sender {
                    if clean_room != "all"
                        && clean_room != "peers"
                        && !clean_room.starts_with("telegram:")
                        && !store.is_group_public(clean_room)
                        && !sender.is_admin
                        && let Ok(Some(members)) = store.room_members(room)
                    {
                        let is_member_or_parent = members.iter().any(|m| {
                            if m == &sender.name {
                                return true;
                            }
                            store
                                .agent_by_name(m)
                                .ok()
                                .and_then(|ma| ma.parent_id)
                                .as_ref()
                                == Some(&sender.id)
                        });
                        if !is_member_or_parent {
                            return Err(ApiError::forbidden(format!(
                                "cannot post to private group `{clean_room}` (`public: false`) because `{}` is not a member; join or ask a group member/parent to add you first",
                                sender.name
                            )));
                        }
                    } else if (clean_room == "all" || clean_room == "peers") && !sender.is_public()
                    {
                        return Err(ApiError::forbidden(format!(
                            "private sub-agent `{}` (`public: false`) cannot broadcast to `{clean_room}`; message `parent`, `children`, or a shared `group:<name>` instead",
                            sender.name
                        )));
                    }
                }
                if room_recipients(store, body, room).is_empty() {
                    return Err(ApiError::new(
                        StatusCode::BAD_REQUEST,
                        format!("room {room} has no agent to deliver to"),
                    ));
                }
            }
            Target::Telegram {
                chat,
                thread,
                reply,
                ..
            } => {
                let channel_actor = plan
                    .sender_id
                    .as_ref()
                    .map(|id| Actor::Agent(id.to_string()))
                    .unwrap_or_else(|| actor.clone());
                prepare_telegram(
                    store,
                    &channel_actor,
                    body,
                    chat,
                    thread.as_deref(),
                    reply.as_deref(),
                )?;
            }
            // An invalid or limited report is refused here without a write.
            // The binding decision is the first write of `send_sync`.
            Target::Operator => {
                let composed = anyagents_core::quote::compose(&body.text, body.quote.as_ref());
                let receipt = store
                    .check_operator_report(&operator_sender(actor, plan).0, &composed)
                    .map_err(ApiError::from_store)?;
                if let Some(anyagents_core::OperatorReceipt::Limited { retry_after_ms }) = receipt {
                    return Err(operator_limited(retry_after_ms));
                }
            }
        }
    }
    Ok(())
}

fn send_sync(
    store: &Store,
    actor: &Actor,
    body: SendRequest,
    plan: SendPlan,
    key: Option<String>,
) -> Result<Json<SendResponse>, ApiError> {
    // Reauthorize the immutable identities before replay, target checks or effects.
    if let Some(id) = &plan.principal_id {
        store
            .agent(id)
            .map_err(|_| ApiError::forbidden("authenticated agent no longer exists"))?;
    }
    let resolved_actor = match &plan.sender_id {
        Some(id) => Actor::Agent(
            store
                .agent(id)
                .map_err(|_| ApiError::forbidden("sender agent no longer exists"))?
                .name,
        ),
        None => actor.clone(),
    };
    let actor = &resolved_actor;
    let body = SendRequest {
        // A display name may be reassigned while a journal lock is held. Carry
        // the admitted identity through mailbox storage and trusted context.
        sender: Some(
            plan.sender_id
                .as_ref()
                .map(|id| format!("agent:{id}"))
                .unwrap_or_else(|| actor.as_address()),
        ),
        ..body
    };
    // A model's closing token is not a message and must not wake its target.
    // Keep this after identity reauthorization, including for replayed calls.
    if body.is_turn_closing_sentinel() {
        return Ok(Json(SendResponse {
            deliveries: Vec::new(),
            skipped: Vec::new(),
        }));
    }
    // A call repeated after its answer was lost is the same delivery: the
    // receipt it already produced comes back unchanged.
    if let Some(key) = key.as_deref()
        && let Some(text) = store.send_receipt(key).map_err(ApiError::from_store)?
    {
        let receipt: SendResponse = serde_json::from_str(&text)
            .map_err(|err| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        return Ok(Json(receipt));
    }
    // The request's `reply_to` is a reference, not a Telegram id; a Telegram
    // target carries the id the reference resolved to instead.
    let reply = plan.reply.clone();
    let effective_text = format_internal_send_text(&body);
    let body = SendRequest {
        text: effective_text,
        // A reply the caller resolved to a message id, or a reference the
        // server resolved to one, is what this post answers.
        reply_to: reply.clone().or_else(|| {
            body.reply_to
                .filter(|v| crate::reply::ref_message(v).is_none())
        }),
        ..body
    };
    check_targets(store, actor, &body, &plan)?;
    let sender_agent_id = plan.sender_id.clone();
    let composed_body = anyagents_core::quote::compose(&body.text, body.quote.as_ref());
    // The operator inbox decides its limits in its own write transaction, so
    // it is written before every other target: a refusal there, even one a
    // concurrent send caused after the check above, leaves nothing written.
    let operator_delivery = if plan.targets.iter().any(|t| matches!(t, Target::Operator)) {
        Some(deliver_operator(
            store,
            actor,
            &plan,
            body.reply_to.as_deref(),
            &composed_body,
        )?)
    } else {
        None
    };
    let mut deliveries = Vec::with_capacity(plan.targets.len());
    let mut skipped = Vec::new();
    let mut refused_archived = Vec::new();
    for (target_index, target) in plan.targets.iter().enumerate() {
        let history_key = key.as_ref().map(|key| format!("{key}:{target_index}"));
        match target {
            Target::Agent(name) => {
                // Only a single, explicitly named recipient of an authored
                // send may unarchive. Resolved replies and fan-out aliases do
                // not acquire that authority when expanded into agent targets.
                let wake_archived = matches!(actor, Actor::Agent(_))
                    || matches!(actor, Actor::Operator(sender) if sender == "operator");
                let wake_archived = wake_archived
                    && body.to.len() == 1
                    && matches!(Address::parse(&body.to[0]), Ok(Address::Agent(ref name))
                        if !matches!(name.as_str(), "all" | "peers" | "siblings" | "team" | "children" | "parent"))
                    && (plan.revive_host_agents
                        || store
                            .agent_by_name(name)
                            .is_ok_and(|agent| runs_off_host(store.data_dir(), &agent)));
                // An archived recipient this send cannot wake would never read
                // it: refused with the reason, never stored unread.
                if !wake_archived && let Some(refusal) = archived_refusal(store, name) {
                    refused_archived.push(refusal.reason.clone());
                    skipped.push(refusal);
                    continue;
                }
                let sent = if let Some(sid) = &sender_agent_id {
                    outbound_history::deliver(
                        store,
                        sid,
                        &format!("agent:{name}"),
                        body.reply_to.as_deref(),
                        &composed_body,
                        history_key.as_deref(),
                        true,
                        || {
                            Ok(vec![deliver_agent(
                                store,
                                &body,
                                name,
                                key.as_deref(),
                                wake_archived,
                            )?])
                        },
                    )?
                } else {
                    vec![deliver_agent(
                        store,
                        &body,
                        name,
                        key.as_deref(),
                        wake_archived,
                    )?]
                };
                deliveries.extend(sent);
            }
            Target::Room(room) => {
                let dest = if room.starts_with("group:") || room.starts_with("telegram:") {
                    room.clone()
                } else {
                    format!("group:{room}")
                };
                let mut all = if let Some(sid) = &sender_agent_id {
                    outbound_history::deliver(
                        store,
                        sid,
                        &dest,
                        body.reply_to.as_deref(),
                        &composed_body,
                        history_key.as_deref(),
                        true,
                        || deliver_room(store, &body, room, key.as_deref()),
                    )?
                } else {
                    deliver_room(store, &body, room, key.as_deref())?
                };
                let mut room_skip = room_skipped(store, &body, room);
                deliveries.append(&mut all);
                for s in room_skip.drain(..) {
                    if !skipped
                        .iter()
                        .any(|existing: &crate::api::SkippedDelivery| existing.to == s.to)
                    {
                        skipped.push(s);
                    }
                }
            }
            Target::Operator => deliveries.extend(operator_delivery.clone()),
            Target::Telegram {
                address,
                chat,
                thread,
                reply: address_reply,
            } => {
                let channel_actor = sender_agent_id
                    .as_ref()
                    .map(|id| Actor::Agent(id.to_string()))
                    .unwrap_or_else(|| actor.clone());
                let send = || {
                    deliver_telegram(
                        store,
                        &channel_actor,
                        &body,
                        address,
                        chat,
                        thread.as_deref(),
                        address_reply.as_deref().or(reply.as_deref()),
                    )
                    .map(|delivery| vec![delivery])
                };
                let sent = if let Some(sid) = &sender_agent_id {
                    outbound_history::deliver(
                        store,
                        sid,
                        address,
                        body.reply_to.as_deref(),
                        &composed_body,
                        history_key.as_deref(),
                        false,
                        send,
                    )?
                } else {
                    send()?
                };
                deliveries.extend(sent);
            }
        }
    }
    if deliveries.is_empty() && !refused_archived.is_empty() {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            format!(
                "nothing was sent: {}. Message a live agent instead, or wake this one by sending to it alone, by name.",
                refused_archived.join("; ")
            ),
        ));
    }
    let response = SendResponse {
        deliveries,
        skipped,
    };
    if let Some(key) = key.as_deref() {
        let text = serde_json::to_string(&response)
            .map_err(|err| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        store
            .record_send_receipt(key, &text)
            .map_err(ApiError::from_store)?;
    }
    Ok(Json(response))
}

/// The idempotency key of one call. The same call repeated after a lost
/// answer keeps the key its tool call was given; two identical calls without
/// one are two messages, so a key never comes from the text. Receipts live in
/// one store and must not depend on process entropy: retries after restart or
/// through the other server socket must retain their mailbox source key.
fn send_key(body: &SendRequest) -> Option<String> {
    let idempotency = body.idempotency_key.as_deref()?.trim();
    if idempotency.is_empty() {
        return None;
    }
    let sender = body.sender.as_deref().unwrap_or("");
    let reply_to = body.reply_to.as_deref().unwrap_or("");
    let to_joined = body.to.join("\x1f");
    let mut bytes = Vec::new();
    for part in [
        sender,
        idempotency,
        to_joined.as_str(),
        reply_to,
        body.text.as_str(),
    ] {
        bytes.extend_from_slice(part.as_bytes());
        bytes.push(0);
    }
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in &bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    Some(format!("send:{hash:016x}"))
}

/// Who writes a message.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Actor {
    Agent(String),
    /// The operator, as an explicit principal: `operator`, `telegram:user:<id>`
    /// or `telegram:chat:<id>`.
    Operator(String),
}

impl Actor {
    fn parse(raw: &str) -> Result<Self, ApiError> {
        if raw.trim() == "operator" {
            return Ok(Self::Operator("operator".to_string()));
        }
        match anyagents_core::Address::parse(raw) {
            Ok(anyagents_core::Address::Agent(name)) => Ok(Self::Agent(name)),
            Ok(
                address @ (anyagents_core::Address::TelegramUser { .. }
                | anyagents_core::Address::TelegramChat { .. }),
            ) => Ok(Self::Operator(address.canonical())),
            Ok(other) => Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                format!("{other} cannot write a message"),
            )),
            Err(err) => Err(ApiError::new(StatusCode::BAD_REQUEST, err.to_string())),
        }
    }

    /// The sender stored with the message.
    fn as_address(&self) -> String {
        match self {
            Self::Agent(name) => format!("agent:{name}"),
            Self::Operator(raw) => raw.clone(),
        }
    }
}

/// Everyone a room message will reach, without writing anything: the room's
/// members (its chat, when it names one) minus the sender.
fn body_author_is(body: &SendRequest, agent: &Agent) -> bool {
    let Some(sender) = body.sender.as_deref() else {
        return false;
    };
    let sender = sender.strip_prefix("agent:").unwrap_or(sender);
    match anyagents_core::AgentId::parse(sender) {
        Ok(id) => id == agent.id,
        Err(_) => sender == agent.name,
    }
}

fn room_recipients(store: &Store, body: &SendRequest, room: &str) -> Vec<Agent> {
    let members = match store.room_members(room) {
        Ok(members) => members,
        Err(_) => return Vec::new(),
    };
    store
        .list_live_agents()
        .unwrap_or_default()
        .into_iter()
        .filter(|agent| {
            (agent.state == "running" || agent.state == "idle")
                && !is_explicitly_archived(store.data_dir(), agent)
        })
        .filter(|agent| members.is_some() || agent.parent_id.is_none())
        .filter(|agent| !body_author_is(body, agent))
        .filter(|agent| {
            members
                .as_ref()
                .is_none_or(|members| members.iter().any(|name| name == &agent.name))
        })
        .collect()
}

/// Non-running agents in a room that a broadcast or group send skipped, with their state reason.
fn room_skipped(store: &Store, body: &SendRequest, room: &str) -> Vec<crate::api::SkippedDelivery> {
    let name = room.strip_prefix("group:").unwrap_or(room);
    if name == "all" || name == "peers" {
        return store
            .list_live_agents()
            .unwrap_or_default()
            .into_iter()
            .filter(|agent| agent.parent_id.is_none())
            .filter(|agent| !body_author_is(body, agent))
            .filter(|agent| {
                agent.state != "running" && agent.state != "idle" && agent.state != "archived"
            })
            .map(|agent| crate::api::SkippedDelivery {
                to: format!("agent:{}", agent.name),
                reason: agent.state,
            })
            .collect();
    }
    let members = match store.room_members(room) {
        Ok(Some(members)) => members,
        _ => return Vec::new(),
    };
    // One row per member name, in name order.
    let members: std::collections::BTreeMap<_, _> = members
        .iter()
        .filter_map(|member| store.agent_by_name(member).ok())
        .filter(|agent| members.contains(&agent.name))
        .map(|agent| (agent.name.clone(), agent))
        .collect();
    members
        .into_values()
        .filter(|agent| !body_author_is(body, agent))
        .filter(|agent| agent.state != "running" && agent.state != "idle")
        .map(|agent| crate::api::SkippedDelivery {
            to: format!("agent:{}", agent.name),
            reason: agent.state,
        })
        .collect()
}

/// One room message, one delivery per recipient.
fn deliver_room(
    store: &Store,
    body: &SendRequest,
    room: &str,
    key: Option<&str>,
) -> Result<Vec<Delivery>, ApiError> {
    let delivered = store
        .receive_room(
            room,
            Incoming {
                text: body.text.clone(),
                sender: body.sender.clone(),
                source_time_ms: None,
                source_key: key.map(str::to_string),
                reply_to: body.reply_to.clone(),
                quote: body.quote.clone(),
                origin: None,
                received_at_ms: None,
            },
        )
        .map_err(ApiError::from_store)?;
    Ok(delivered
        .into_iter()
        .map(|(agent, outcome)| {
            let (message, duplicate) = match outcome {
                ReceiveOutcome::New(message) => (message, false),
                ReceiveOutcome::Duplicate(message) => (message, true),
            };
            Delivery {
                to: format!("agent:{}", agent.name),
                id: message.id.to_string(),
                seq: message.seq,
                duplicate,
            }
        })
        .collect())
}

/// The refusal of a send to `name` when that agent is archived: it reads
/// nothing until it is resumed. Names who answers for it instead.
fn archived_refusal(store: &Store, name: &str) -> Option<crate::api::SkippedDelivery> {
    let recipient = store.agent_by_name(name).ok()?;
    if !is_explicitly_archived(store.data_dir(), &recipient) {
        return None;
    }
    let answers = match store.live_ancestor(&recipient).ok().flatten() {
        Some(parent) if recipient.parent_id.as_ref() == Some(&parent.id) => {
            format!("its parent is `{}`", parent.name)
        }
        Some(ancestor) => format!("the nearest running agent above it is `{}`", ancestor.name),
        None => "no running agent is above it".to_string(),
    };
    Some(crate::api::SkippedDelivery {
        to: format!("agent:{}", recipient.name),
        reason: format!("agent `{}` is archived; {answers}", recipient.name),
    })
}

fn deliver_agent(
    store: &Store,
    body: &SendRequest,
    name: &str,
    key: Option<&str>,
    wake_archived: bool,
) -> Result<Delivery, ApiError> {
    let agent = store.agent_by_name(name).map_err(ApiError::from_store)?;
    let _lifecycle = store
        .lock_agent_lifecycle(&agent.id)
        .map_err(ApiError::from_store)?;
    let agent = store.agent(&agent.id).map_err(ApiError::from_store)?;
    // A crash may leave the mailbox row without a whole-send receipt. Check it
    // before clearing markers/errors, not merely before the lifecycle SQL.
    if let Some(key) = key
        && let Some(message) = store
            .message_by_source_key(&agent.id, key)
            .map_err(ApiError::from_store)?
    {
        return Ok(Delivery {
            to: format!("agent:{}", agent.name),
            id: message.id.to_string(),
            seq: message.seq,
            duplicate: true,
        });
    }
    if wake_archived && is_explicitly_archived(store.data_dir(), &agent) {
        prepare_agent_wake(store, &agent)?;
    }
    let trimmed = body.text.trim_start();
    let is_auto_subagent_lifecycle =
        trimmed.starts_with("[Sub-Agent Completed]") || trimmed.starts_with("[Sub-Agent Error]");
    if !wake_archived
        && (agent.state == "idle" || agent.state == "created")
        && !is_explicitly_archived(store.data_dir(), &agent)
        && !(agent.parent_id.is_none() && is_auto_subagent_lifecycle)
    {
        let _ = store.set_agent_state(&agent.id, "running");
    }
    let incoming = Incoming {
        text: body.text.clone(),
        sender: body.sender.clone(),
        source_time_ms: None,
        source_key: key.map(str::to_string),
        reply_to: body.reply_to.clone(),
        quote: body.quote.clone(),
        origin: None,
        received_at_ms: None,
    };
    let outcome = if wake_archived {
        store.receive_direct_send(&agent.id, incoming)
    } else {
        store.receive(&agent.id, incoming)
    }
    .map_err(ApiError::from_store)?;
    let (message, duplicate) = match outcome {
        ReceiveOutcome::New(message) => (message, false),
        ReceiveOutcome::Duplicate(message) => (message, true),
    };
    if !duplicate {
        crate::create::poke_agent_wake_socket(store.data_dir(), &agent.id);
        crate::create::poke_agent_wake_socket_mail(store.data_dir(), &agent.id);
    }
    Ok(Delivery {
        to: format!("agent:{}", agent.name),
        id: message.id.to_string(),
        seq: message.seq,
        duplicate,
    })
}

/// Who a report to the operator is from: the admitted agent's immutable id,
/// which keys its rate limit, and its display name; or the operator's own
/// address.
fn operator_sender(actor: &Actor, plan: &SendPlan) -> (String, Option<String>) {
    match (&plan.sender_id, actor) {
        (Some(id), Actor::Agent(name)) => (format!("agent:{id}"), Some(name.clone())),
        (Some(id), Actor::Operator(_)) => (format!("agent:{id}"), None),
        (None, actor) => (actor.as_address(), None),
    }
}

fn operator_limited(retry_after_ms: i64) -> ApiError {
    let seconds = (retry_after_ms + 999) / 1000;
    ApiError::new(
        StatusCode::TOO_MANY_REQUESTS,
        format!(
            "the operator inbox takes at most {} reports a minute and {} an hour from one sender, \
             and {} a minute and {} an hour in all; nothing was sent. Retry in {seconds}s with \
             one report that covers everything open",
            anyagents_core::OPERATOR_BURST,
            anyagents_core::OPERATOR_HOURLY_CAP,
            anyagents_core::OPERATOR_FLEET_BURST,
            anyagents_core::OPERATOR_FLEET_HOURLY_CAP,
        ),
    )
}

/// Store one report, or count it on the report it repeats. A new report is
/// also recorded in its agent sender's own history.
fn deliver_operator(
    store: &Store,
    actor: &Actor,
    plan: &SendPlan,
    reply_to: Option<&str>,
    body: &str,
) -> Result<Delivery, ApiError> {
    let (sender, sender_name) = operator_sender(actor, plan);
    let receipt = store
        .report_to_operator(anyagents_core::OperatorReport {
            sender,
            sender_name,
            body: body.to_string(),
            received_at_ms: None,
        })
        .map_err(ApiError::from_store)?;
    let (message, duplicate) = match receipt {
        anyagents_core::OperatorReceipt::New(message) => (message, false),
        anyagents_core::OperatorReceipt::Duplicate(message) => (message, true),
        anyagents_core::OperatorReceipt::Limited { retry_after_ms } => {
            return Err(operator_limited(retry_after_ms));
        }
    };
    if !duplicate && let Some(sid) = &plan.sender_id {
        store
            .record_outbound_once(message.id.as_str(), sid, "operator", reply_to, body)
            .map_err(ApiError::from_store)?;
    }
    Ok(Delivery {
        to: "operator".to_string(),
        id: message.id.to_string(),
        seq: message.seq,
        duplicate,
    })
}

/// Resolve the bot, the chat, the topic and the reply before any network
/// call. This does not post.
fn prepare_telegram(
    store: &Store,
    actor: &Actor,
    body: &SendRequest,
    chat: &str,
    thread: Option<&str>,
    reply: Option<&str>,
) -> Result<(), ApiError> {
    let from = telegram_sender(actor)?;
    let (chat_id, thread_id, reply_to) = telegram_target(chat, thread, reply, body)?;
    crate::telegram::outbound_call(store, from, chat_id, &body.text, reply_to, thread_id)
        .map_err(ApiError::from_store)?;
    Ok(())
}

/// The bot a send posts through: always the authenticated actor's own.
fn telegram_sender(actor: &Actor) -> Result<&str, ApiError> {
    match actor {
        Actor::Agent(name) => Ok(name),
        Actor::Operator(_) => Err(ApiError::forbidden(
            "a post to a channel needs an agent's own bot",
        )),
    }
}

/// The chat, forum topic and replied-to message as numbers. The explicit
/// `reply_to` wins over the one an address names.
fn telegram_target(
    chat: &str,
    thread: Option<&str>,
    reply: Option<&str>,
    body: &SendRequest,
) -> Result<(i64, Option<i64>, Option<i64>), ApiError> {
    let chat_id: i64 = chat
        .parse()
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "telegram chat id is not a number"))?;
    let thread_id = match thread {
        None => None,
        Some(value) => Some(number(value, "telegram topic id is not a number")?),
    };
    let reply_to = telegram_reply(body, reply)?;
    Ok((chat_id, thread_id, reply_to))
}

fn number(value: &str, what: &'static str) -> Result<i64, ApiError> {
    value
        .parse()
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, what))
}

fn telegram_reply(body: &SendRequest, reply: Option<&str>) -> Result<Option<i64>, ApiError> {
    let candidate = reply.or_else(|| {
        body.reply_to
            .as_deref()
            .filter(|v| crate::reply::ref_message(v).is_none() && Address::parse(v).is_err())
    });
    match candidate {
        None => Ok(None),
        Some(value) => number(value, "reply_to is not a telegram message id").map(Some),
    }
}

fn format_internal_send_text(body: &SendRequest) -> String {
    let mut parts = Vec::new();
    if !body.text.trim().is_empty() {
        parts.push(body.text.clone());
    }
    if let Some(ref media) = body.media {
        parts.push(format!("[Attachment ({}): {}]", media.kind, media.source));
    }
    if let Some(ref loc) = body.location {
        match (&loc.title, &loc.address) {
            (Some(t), Some(a)) if !t.is_empty() && !a.is_empty() => {
                parts.push(format!(
                    "[Venue: {t} — {a} (GPS: {:.6}, {:.6})]",
                    loc.latitude, loc.longitude
                ));
            }
            _ => {
                parts.push(format!(
                    "[Location GPS: latitude={:.6}, longitude={:.6}]",
                    loc.latitude, loc.longitude
                ));
            }
        }
    }
    if let Some(ref poll) = body.poll {
        parts.push(format!(
            "[Poll: \"{}\" | Options: {}]",
            poll.question,
            poll.options.join(" / ")
        ));
    }
    if let Some(ref cl) = body.checklist {
        parts.push(anyagents_channel_telegram::format_checklist_markdown(
            &cl.title, &cl.tasks,
        ));
    }
    if let Some(ref art) = body.article {
        match (&art.url, &art.content) {
            (Some(u), _) if !u.trim().is_empty() => {
                parts.push(format!("📰 [**{}**]({})", art.title, u.trim()));
            }
            (_, Some(c)) if !c.trim().is_empty() => {
                parts.push(format!("📰 **{}**\n\n{}", art.title, c.trim()));
            }
            _ => {
                parts.push(format!("📰 **{}**", art.title));
            }
        }
    }
    if parts.is_empty() {
        body.text.clone()
    } else {
        parts.join("\n")
    }
}

/// `id` is the Telegram message id. This delivery has no mailbox sequence.
#[allow(clippy::too_many_arguments)]
fn deliver_telegram(
    store: &Store,
    actor: &Actor,
    body: &SendRequest,
    address: &str,
    chat: &str,
    thread: Option<&str>,
    reply: Option<&str>,
) -> Result<Delivery, ApiError> {
    let from = telegram_sender(actor)?;
    let (chat_id, thread_id, reply_to) = telegram_target(chat, thread, reply, body)?;
    let message_id = if let Some(ref media) = body.media {
        let caption = body
            .text
            .strip_suffix(&format!(
                "\n[Attachment ({}): {}]",
                media.kind, media.source
            ))
            .or_else(|| {
                body.text
                    .strip_prefix(&format!("[Attachment ({}): {}]", media.kind, media.source))
            })
            .map(str::trim)
            .filter(|s| !s.is_empty());
        crate::telegram::post_media(
            store,
            store.data_dir(),
            from,
            chat_id,
            &media.kind,
            &media.source,
            caption,
            media.title.as_deref(),
            media.performer.as_deref(),
            reply_to,
            thread_id,
        )
        .map_err(ApiError::from_store)?
    } else if let Some(ref loc) = body.location {
        crate::telegram::post_location(
            store,
            store.data_dir(),
            from,
            chat_id,
            loc.latitude,
            loc.longitude,
            loc.title.as_deref(),
            loc.address.as_deref(),
            reply_to,
            thread_id,
        )
        .map_err(ApiError::from_store)?
    } else if let Some(ref poll) = body.poll {
        crate::telegram::post_poll(
            store,
            store.data_dir(),
            from,
            chat_id,
            &poll.question,
            &poll.options,
            poll.is_anonymous,
            poll.allows_multiple_answers,
            reply_to,
            thread_id,
        )
        .map_err(ApiError::from_store)?
    } else if let Some(ref cl) = body.checklist {
        crate::telegram::post_checklist(
            store,
            store.data_dir(),
            from,
            chat_id,
            &cl.title,
            &cl.tasks,
            cl.business_connection_id.as_deref(),
            reply_to,
            thread_id,
        )
        .map_err(ApiError::from_store)?
    } else if let Some(ref art) = body.article {
        crate::telegram::post_article(
            store,
            store.data_dir(),
            from,
            chat_id,
            &art.title,
            art.content.as_deref(),
            art.url.as_deref(),
            art.author_name.as_deref(),
            None,
            reply_to,
            thread_id,
        )
        .map_err(ApiError::from_store)?
    } else {
        crate::telegram::post_text(
            store,
            store.data_dir(),
            from,
            chat_id,
            &body.text,
            reply_to,
            thread_id,
        )
        .map_err(ApiError::from_store)?
    };
    // Remember the chat this agent answers in, so a post it makes there reaches
    // the other agents on that chat as a mailbox message.
    crate::telegram::record_chat(store, from, chat_id).map_err(ApiError::from_store)?;
    Ok(Delivery {
        to: address.to_string(),
        id: message_id.to_string(),
        seq: 0,
        duplicate: false,
    })
}

async fn web_route(
    _principal: Principal,
    State(state): State<AppState>,
    Json(body): Json<crate::web::WebRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        crate::web::execute_web_request(Some(state.store.data_dir()), body)
            .map(Json)
            .map_err(|msg| ApiError::new(StatusCode::BAD_REQUEST, msg))
    })
    .await
}

async fn spawn_store<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
) -> Result<T, ApiError> {
    match tokio::task::spawn_blocking(work).await {
        Ok(result) => result,
        Err(err) => Err(ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            err.to_string(),
        )),
    }
}

#[derive(Debug)]
pub(crate) struct ApiError {
    status: StatusCode,
    pub(crate) message: String,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, message)
    }

    fn from_store(err: Error) -> Self {
        let status = match &err {
            Error::PermissionDenied(_) => StatusCode::FORBIDDEN,
            Error::AgentBusy => StatusCode::CONFLICT,
            Error::AgentNotFound(_) => StatusCode::NOT_FOUND,
            Error::GroupForbidden { .. } => StatusCode::FORBIDDEN,
            Error::AgentExists(_) => StatusCode::CONFLICT,
            Error::InvalidAddress(_)
            | Error::Message(_)
            | Error::WorktreeSourceUnpublished { .. } => StatusCode::BAD_REQUEST,
            Error::WorktreeProvisioning { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Error::WorkdirPermission { .. } => StatusCode::FORBIDDEN,
            Error::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self::new(status, err.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(serde_json::json!({ "error": self.message }));
        (self.status, body).into_response()
    }
}

/// The working directory every owner agent shares, when the deployment sets one.
/// `ANYAGENTS_OWNER_WORKDIR` names it; without it an agent keeps its own
/// managed directory.
pub fn owner_workdir() -> Option<std::path::PathBuf> {
    std::env::var_os("ANYAGENTS_OWNER_WORKDIR").map(std::path::PathBuf::from)
}

#[cfg(test)]
pub(crate) mod tests {
    #[test]
    fn memory_backlog_follows_wakes_and_idle_consolidation_visits_nothing() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("sleeper", "codex"))
            .unwrap();
        let root = dir.path().join("agents").join(agent.id.as_str());
        let sleep_state = root.join("memory/.sleep-state.json");
        let write_topic = |topic: &str| {
            anyagents_core::learned_memory::upsert_memory_topic(
                &root,
                "sleeper",
                None,
                topic,
                Some("fact"),
                "learned",
                anyagents_core::now_ms(),
            )
            .unwrap();
        };
        write_topic("first");
        let backlog = memory_backlog::start(&store);
        assert_eq!(backlog.pending(), std::slice::from_ref(&agent.id));
        consolidate_idle_agent_memory(&store, dir.path(), &HealthBoard::default(), &backlog);
        assert!(sleep_state.exists());
        assert!(backlog.pending().is_empty());

        // Nothing woke the agent, so a pass does not look at it.
        write_topic("second");
        std::fs::remove_file(&sleep_state).unwrap();
        consolidate_idle_agent_memory(&store, dir.path(), &HealthBoard::default(), &backlog);
        assert!(!sleep_state.exists(), "idle agent was visited");

        store
            .receive(&agent.id, anyagents_core::Incoming::new("new work"))
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while backlog.pending().is_empty() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(backlog.pending(), std::slice::from_ref(&agent.id));
        consolidate_idle_agent_memory(&store, dir.path(), &HealthBoard::default(), &backlog);
        assert!(sleep_state.exists(), "woken agent was not consolidated");
        assert!(backlog.pending().is_empty());
    }

    #[test]
    fn idle_memory_consolidation_preserves_selected_and_disabled_session_pins() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        for (name, source) in [
            ("default", Some(std::path::Path::new("memory/MEMORY.md"))),
            ("selected", Some(std::path::Path::new("selected.md"))),
            ("disabled", None),
        ] {
            let agent = store
                .create_agent(anyagents_core::NewAgent::new(name, "codex"))
                .unwrap();
            let root = dir.path().join("agents").join(agent.id.as_str());
            std::fs::create_dir_all(root.join("memory")).unwrap();
            std::fs::write(root.join("memory/MEMORY.md"), "original default").unwrap();
            std::fs::write(root.join("selected.md"), "original selected").unwrap();
            let pinned =
                anyagents_core::host_session::new_session_with_source(&root, "codex", source)
                    .unwrap();
            anyagents_core::learned_memory::upsert_memory_topic(
                &root,
                name,
                None,
                "new-topic",
                Some("new fact"),
                "changed after pin",
                anyagents_core::now_ms(),
            )
            .unwrap();
            std::fs::write(root.join("selected.md"), "changed selected").unwrap();
            let backlog = memory_backlog::MemoryBacklog::default();
            backlog.mark(&agent.id);
            consolidate_idle_agent_memory(&store, dir.path(), &HealthBoard::default(), &backlog);
            assert!(backlog.pending().is_empty(), "caught-up agent stays queued");
            assert_eq!(
                anyagents_core::host_session::active(&root).unwrap(),
                Some(pinned.clone())
            );
            let next =
                anyagents_core::host_session::new_session_with_source(&root, "codex", source)
                    .unwrap();
            assert_ne!(next.generation, pinned.generation);
            if source.is_some() {
                assert_ne!(next.memory.sha256, pinned.memory.sha256);
            } else {
                assert_eq!(next.memory.sha256, pinned.memory.sha256);
            }
        }
    }
    use super::*;

    #[test]
    fn an_agent_view_keeps_the_fields_a_client_from_before_delivery_requires() {
        // A CLI or MCP client from the release before `delivery` requires both.
        #[derive(serde::Deserialize)]
        struct OlderClientView {
            delivery_mode: String,
            steer_support: String,
        }
        let temp = crate::test_tempdir();
        let store = Store::open(temp.path().join("state.db")).unwrap();
        for (harness, mode, steer) in [
            ("spiron", "steer", "yes"),
            ("grok", "steer", "unproven"),
            ("spiron-cloud", "followup", "no"),
        ] {
            let agent = store
                .create_agent(anyagents_core::NewAgent::new(
                    format!("view-{harness}"),
                    harness,
                ))
                .unwrap();
            let json = serde_json::to_string(&archived_agent_view_fast(&agent)).unwrap();
            let older: OlderClientView = serde_json::from_str(&json).unwrap();
            assert_eq!(
                (older.delivery_mode.as_str(), older.steer_support.as_str()),
                (mode, steer),
                "{harness}"
            );
        }
    }
    use std::io::{Read, Write};
    use std::sync::MutexGuard;

    #[tokio::test]
    async fn single_agent_endpoint_resolves_identity_alias_and_enforces_scope() {
        let dir = crate::test_tempdir();
        let state = archetype_validation_state(dir.path());
        let root = state
            .store
            .create_agent(anyagents_core::NewAgent::new("root", "opencode"))
            .unwrap();
        let renamed = state.store.rename_agent(&root.id, "renamed").unwrap();
        for name in ["root", "renamed", renamed.id.as_str()] {
            let Json(view) = get_agent(
                Principal::Operator,
                State(state.clone()),
                AxumPath(name.to_string()),
            )
            .await
            .unwrap();
            assert_eq!(view.id, root.id.as_str());
            assert!(view.aliases.contains(&"root".into()));
        }
        let mut child = anyagents_core::NewAgent::new("isolated-child", "opencode");
        child.parent_id = Some(root.id.clone());
        let child = state.store.create_agent(child).unwrap();
        state.store.set_agent_scope(&child.id, "isolated").unwrap();
        let principal = Principal::Agent {
            agent_id: child.id.clone(),
            name: child.name.clone(),
        };
        let Json(own) = get_agent(
            principal.clone(),
            State(state.clone()),
            AxumPath("self".into()),
        )
        .await
        .unwrap();
        assert_eq!(own.id, child.id.as_str());
        let peer = state
            .store
            .create_agent(anyagents_core::NewAgent::new("peer", "opencode"))
            .unwrap();
        assert_eq!(
            get_agent(principal.clone(), State(state.clone()), AxumPath(peer.name))
                .await
                .unwrap_err()
                .status,
            StatusCode::FORBIDDEN
        );
        state.store.delete_agent(&child.id).unwrap();
        assert_eq!(
            get_agent(principal, State(state), AxumPath("self".into()))
                .await
                .unwrap_err()
                .status,
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn roster_omits_archive_except_ancestors_and_reports_count() {
        let dir = crate::test_tempdir();
        let state = archetype_validation_state(dir.path());
        let root = state
            .store
            .create_agent(anyagents_core::NewAgent::new("archived-parent", "opencode"))
            .unwrap();
        let mut child = anyagents_core::NewAgent::new("live-child", "opencode");
        child.parent_id = Some(root.id.clone());
        state.store.create_agent(child).unwrap();
        state.store.set_agent_state(&root.id, "archived").unwrap();
        for i in 0..40 {
            let a = state
                .store
                .create_agent(anyagents_core::NewAgent::new(
                    format!("old-{i}"),
                    "opencode",
                ))
                .unwrap();
            state.store.set_agent_state(&a.id, "archived").unwrap();
        }
        // A poisoned diagnostic path would fail if archived views read it.
        let cfg = anyagents_core::agent_config_dir(dir.path(), &root.id);
        std::fs::create_dir_all(cfg.join(".anyagents-profile-rotated-ms")).unwrap();
        let Json(default) = list_agents(
            Principal::Operator,
            State(state.clone()),
            Query(ListAgentsQuery::default()),
        )
        .await
        .unwrap();
        assert_eq!(default.archived_count, 41);
        assert_eq!(default.agents.len(), 2);
        let archived = default
            .agents
            .iter()
            .find(|a| a.id == root.id.as_str())
            .unwrap();
        assert_eq!(archived.install, "archived");
        assert!(archived.schedules.is_empty() && archived.oldest_undelivered_age_ms.is_none());
        for (filter, expected) in [("all", 42), ("archived", 41)] {
            let Json(list) = list_agents(
                Principal::Operator,
                State(state.clone()),
                Query(ListAgentsQuery {
                    state: Some(filter.into()),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
            assert_eq!(list.agents.len(), expected);
            assert_eq!(list.archived_count, 41);
        }
        let Json(list) = list_agents(
            Principal::Operator,
            State(state.clone()),
            Query(ListAgentsQuery {
                parent: Some("archived-parent".into()),
                name: Some("live-child".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        assert!(list.agents.iter().any(|a| a.name == "live-child"));
        assert_eq!(list.agents.len(), 2);
    }

    #[test]
    #[ignore = "manual benchmark against an isolated production snapshot"]
    fn production_roster_query_benchmark() {
        let path = std::env::var("ANYAGENTS_BENCH_DB").expect("isolated snapshot path");
        let start = std::time::Instant::now();
        let store = Store::open(path).unwrap();
        eprintln!(
            "store_open_ms={:.3}",
            start.elapsed().as_secs_f64() * 1000.0
        );
        let health = HealthBoard::default();
        let mut times = Vec::new();
        for _ in 0..20 {
            let start = std::time::Instant::now();
            let (agents, archived) = store.roster_agents(None).unwrap();
            let views = roster_views(
                &store,
                &health,
                &agents.iter().collect::<Vec<_>>(),
                archived,
                &ListAgentsQuery::default(),
            )
            .unwrap();
            let bytes = serde_json::to_vec(&views).unwrap();
            times.push(start.elapsed().as_secs_f64() * 1000.0);
            assert!(bytes.len() > 10);
        }
        times.sort_by(f64::total_cmp);
        eprintln!(
            "roster_ms min={:.3} median={:.3} max={:.3}",
            times[0], times[10], times[19]
        );
        let target = store.agent_by_name("vulcan").unwrap();
        let start = std::time::Instant::now();
        for _ in 0..100 {
            store
                .inbox_query(&target.id, None, Some(20), false)
                .unwrap();
        }
        eprintln!(
            "inbox20_mean_ms={:.3}",
            start.elapsed().as_secs_f64() * 10.0
        );
        let start = std::time::Instant::now();
        for _ in 0..100 {
            agent_view(&store, &health, &store.agent_by_name("vulcan").unwrap()).unwrap();
        }
        eprintln!(
            "single_agent_mean_ms={:.3}",
            start.elapsed().as_secs_f64() * 10.0
        );
    }

    #[test]
    fn roster_authorization_matches_the_single_agent_checks() {
        let dir = crate::test_tempdir();
        let state = archetype_validation_state(dir.path());
        let store = &state.store;
        let make = |name: &str, parent: Option<&Agent>, scope: Option<&str>| {
            let mut new = anyagents_core::NewAgent::new(name, "codex");
            new.parent_id = parent.map(|p| p.id.clone());
            new.instructions = Some(format!("{name} private instructions"));
            let agent = store.create_agent(new).unwrap();
            match scope {
                Some(scope) => store.set_agent_scope(&agent.id, scope).unwrap(),
                None => agent,
            }
        };
        let root = make("root", None, None);
        let admin = make("admin", None, None);
        store.set_agent_admin(&admin.id, true).unwrap();
        let peer = make("peer", None, None);
        let lead = make("lead", Some(&root), Some("siblings"));
        let sib = make("sib", Some(&root), Some("siblings"));
        let worker = make("worker", Some(&lead), None);
        let iso = make("iso", Some(&root), Some("isolated"));
        let iso_child = make("iso-child", Some(&iso), None);
        let public = make("public", Some(&peer), Some("public"));
        let private = make("private", Some(&peer), None);
        let gone = make("gone", Some(&lead), None);
        store.set_agent_state(&gone.id, "archived").unwrap();
        store.join_group("team", &iso.id).unwrap();
        store.join_group("team", &private.id).unwrap();
        store
            .record_outbound(&private.id, "agent:worker", None, "hello")
            .unwrap();
        let callers = [
            &root, &admin, &peer, &lead, &sib, &worker, &iso, &iso_child, &public, &private,
        ];
        for caller in callers {
            let caller = store.agent(&caller.id).unwrap();
            let principal = Principal::Agent {
                agent_id: caller.id.clone(),
                name: caller.name.clone(),
            };
            for filter in [None, Some("all"), Some("archived")] {
                let query = ListAgentsQuery {
                    state: filter.map(str::to_string),
                    ..Default::default()
                };
                let Json(listed) =
                    list_agents_sync_for_principal(&state, &principal, &query).unwrap();
                // The isolated-scope rule, checked row by row against the store.
                let (roster, archived) = store.roster_agents(filter).unwrap();
                let visible: Vec<&Agent> = roster
                    .iter()
                    .filter(|agent| {
                        !(caller.is_isolated()
                            && !is_self_or_ancestor(store, &caller.id, agent)
                            && caller.parent_id.as_ref() != Some(&agent.id)
                            && !store.share_any_group_or_chat(&caller.id, &agent.id))
                    })
                    .collect();
                let expected =
                    roster_views(store, &state.health, &visible, archived, &query).unwrap();
                let ids = |views: &[AgentView]| -> Vec<String> {
                    views.iter().map(|view| view.id.clone()).collect()
                };
                assert_eq!(
                    ids(&listed.agents),
                    ids(&expected.agents),
                    "{}",
                    caller.name
                );
                for view in &listed.agents {
                    let target = store.agent_by_name(&view.id).unwrap();
                    let context = format!("{} -> {}", caller.name, target.name);
                    assert_eq!(
                        view.relationship,
                        Some(compute_caller_relationship(store, &caller, &target)),
                        "{context}"
                    );
                    assert_eq!(
                        view.can_message,
                        Some(
                            can_actor_message_agent(
                                store,
                                &Actor::Agent(caller.name.clone()),
                                &target
                            )
                            .is_ok()
                        ),
                        "{context}"
                    );
                    assert_eq!(
                        view.instructions.is_some(),
                        can_actor_read_agent_private(store, &principal, &target),
                        "{context}"
                    );
                }
            }
        }
    }

    /// Vendor history each harness leaves behind that the view once scanned:
    /// a Claude Code session ending in a rate-limit rejection, an AGY CLI log
    /// ending in an error, and Codex rollouts ending in a 401 turn fault.
    fn write_vendor_history(cfg: &Path) {
        let reset_s = now_ms() / 1000 + 3600;
        let projects = cfg.join("projects/-work");
        std::fs::create_dir_all(&projects).unwrap();
        std::fs::write(
            projects.join("session.jsonl"),
            format!(
                "{}\n",
                serde_json::json!({
                    "type": "assistant",
                    "apiError": "usage_limit_reached",
                    "quotaLimits": {"status": "rejected", "resetsAt": reset_s},
                    "error": "rate_limit",
                    "apiErrorStatus": 429
                })
            ),
        )
        .unwrap();
        let logs = cfg.join(".gemini/antigravity-cli/log");
        std::fs::create_dir_all(&logs).unwrap();
        std::fs::write(
            logs.join("cli-previous-session.log"),
            "run ended with error and no response: previous agy session failed\n",
        )
        .unwrap();
        let sessions = cfg.join("sessions/2026/11/13");
        std::fs::create_dir_all(&sessions).unwrap();
        let fault = serde_json::json!({
            "type": "event_msg",
            "payload": {"type": "task_complete", "error": {"message": "unauthorized (401)"}}
        });
        std::fs::write(
            sessions.join("rollout-2026-11-13T00-00-00-x.jsonl"),
            format!("{fault}\n{fault}\n{fault}\n"),
        )
        .unwrap();
    }

    #[test]
    fn agent_views_never_read_vendor_history() {
        let _env = env_lock();
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        for harness in [
            "agy",
            "claude-code",
            "codex",
            "opencode",
            "muse",
            "omp",
            "spiron",
        ] {
            let agent = store
                .create_agent(anyagents_core::NewAgent::new(harness, harness))
                .unwrap();
            let cfg = anyagents_core::agent_config_dir(dir.path(), &agent.id);
            write_vendor_history(&cfg);
            for view in [
                agent_view(&store, &health, &agent).unwrap(),
                crate::agent::view_named(dir.path(), &agent.name).unwrap(),
            ] {
                assert_eq!(view.last_error, None, "{harness}");
                assert_eq!(view.quota_reset_at_ms, None, "{harness}");
                assert_eq!(view.block_reason, None, "{harness}");
            }
            let (agents, archived) = store.roster_agents(None).unwrap();
            let roster = roster_views(
                &store,
                &health,
                &agents.iter().collect::<Vec<_>>(),
                archived,
                &ListAgentsQuery::default(),
            )
            .unwrap();
            assert!(
                roster
                    .agents
                    .iter()
                    .all(|view| view.last_error.is_none() && view.quota_reset_at_ms.is_none()),
                "{harness}"
            );

            // What the adapter persisted is what the view reports.
            let reset_ms = now_ms() + 3_600_000;
            std::fs::write(
                cfg.join(".anyagents-last-error.json"),
                serde_json::json!({
                    "timestamp_ms": now_ms(),
                    "error": "current harness failed",
                    "quota_reset_at_ms": reset_ms,
                })
                .to_string(),
            )
            .unwrap();
            let expected_quota = (harness != "claude-code").then_some(reset_ms);
            for view in [
                agent_view(&store, &health, &agent).unwrap(),
                crate::agent::view_named(dir.path(), &agent.name).unwrap(),
            ] {
                assert_eq!(view.last_error.as_deref(), Some("current harness failed"));
                assert_eq!(view.quota_reset_at_ms, expected_quota, "{harness}");
            }
            std::fs::remove_file(cfg.join(".anyagents-last-error.json")).unwrap();
        }
    }

    #[test]
    fn agent_view_reports_the_live_board_over_persisted_faults() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("claude", "claude-code"))
            .unwrap();
        store.set_agent_state(&agent.id, "running").unwrap();
        let agent = store.agent(&agent.id).unwrap();
        let cfg = anyagents_core::agent_config_dir(dir.path(), &agent.id);
        write_vendor_history(&cfg);
        let reset_ms = now_ms() + 3_600_000;
        let live = |quota: Option<i64>, error: Option<&str>| anyagents_supervisor::LiveState {
            process_alive: true,
            turn_state: anyagents_harness::TurnState::Idle,
            delivery: Some(crate::vendor::declared_delivery("claude-code")),
            quota_reset_at_ms: quota,
            restart_at_ms: None,
            pid: Some(42),
            live_harness: Some("claude-code".into()),
            live_model: None,
            process_started_at_ms: Some(1),
            last_error: error.map(str::to_string),
        };
        health.publish(&agent.id, live(Some(reset_ms), None));
        let view = agent_view(&store, &health, &agent).unwrap();
        assert_eq!(view.process, "alive");
        assert_eq!(view.quota_reset_at_ms, Some(reset_ms));
        assert_eq!(view.block_reason.as_deref(), Some("quota_exhausted"));
        assert_eq!(view.turn_state, "idle");

        health.publish(&agent.id, live(None, None));
        let view = agent_view(&store, &health, &agent).unwrap();
        assert_eq!(view.quota_reset_at_ms, None);
        assert_eq!(view.block_reason, None);
        assert_eq!(view.turn_state, "idle");

        health.publish(&agent.id, live(None, Some("not logged in")));
        let view = agent_view(&store, &health, &agent).unwrap();
        assert_eq!(view.turn_state, "errored");
        assert_eq!(view.block_reason.as_deref(), Some("sign_in_required"));
    }

    #[test]
    fn spiron_agent_view_uses_turn_lock_with_missing_or_stale_health() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let mut agent = store
            .create_agent(anyagents_core::NewAgent::new("lock-owner", "spiron"))
            .unwrap();
        for selector in [
            "spiron",
            "sylphx",
            "sylphx-agent",
            "sylphx_agent",
            "sylphx/default",
        ] {
            // Exercise legacy in-memory records as well as canonical stored rows.
            agent.harness = selector.into();
            assert_eq!(agent_view(&store, &health, &agent).unwrap().process, "down");
            let lock = store.try_lock_agent(&agent.id).unwrap();
            let busy = agent_view(&store, &health, &agent).unwrap();
            assert_eq!(busy.process, "alive", "{selector}");
            assert_eq!(busy.turn_state, "working", "{selector}");
            health.publish(
                &agent.id,
                anyagents_supervisor::LiveState {
                    process_alive: false,
                    turn_state: anyagents_harness::TurnState::Idle,
                    delivery: Some(crate::vendor::declared_delivery("spiron")),
                    quota_reset_at_ms: None,
                    restart_at_ms: None,
                    pid: None,
                    live_harness: Some("spiron".into()),
                    live_model: None,
                    process_started_at_ms: None,
                    last_error: None,
                },
            );
            assert_eq!(
                agent_view(&store, &health, &agent).unwrap().turn_state,
                "working"
            );
            drop(lock);
            assert_eq!(agent_view(&store, &health, &agent).unwrap().process, "down");
        }
    }

    #[test]
    fn channel_roster_refuses_corrupt_persisted_state_without_mutation() {
        let dir = crate::test_tempdir();
        let state = archetype_validation_state(dir.path());
        let channel = state
            .store
            .create_channel(anyagents_core::NewChannel {
                kind: "telegram".into(),
                identity: Some("example_bot".into()),
                config: "{}".into(),
                credential_ref: None,
            })
            .unwrap();
        let db = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let Json(valid) = channels_route(Principal::Operator, State(state.clone()))
                .await
                .unwrap_or_else(|error| panic!("{}", error.message));
            assert_eq!(valid["channels"].as_array().unwrap().len(), 1);
            for invalid in ["{", "[]", "null"] {
                db.execute(
                    "UPDATE channels SET state = ?1 WHERE id = ?2",
                    rusqlite::params![invalid, channel.id.as_str()],
                )
                .unwrap();
                let error = channels_route(Principal::Operator, State(state.clone()))
                    .await
                    .expect_err("damaged channel state cannot become an empty view");
                assert!(error.message.contains("state"), "{}", error.message);
                let retained: String = db
                    .query_row(
                        "SELECT state FROM channels WHERE id = ?1",
                        [channel.id.as_str()],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert_eq!(retained, invalid);
            }
        });
    }

    #[test]
    fn deletion_retains_identity_and_resources_until_process_stop() {
        let dir = crate::test_tempdir();
        let state = archetype_validation_state(dir.path());
        let agent = state
            .store
            .create_agent(anyagents_core::NewAgent::new("busy", "opencode"))
            .unwrap();
        let config = anyagents_core::agent_config_dir(dir.path(), &agent.id);
        std::fs::create_dir_all(&config).unwrap();
        std::fs::write(config.join("preserve"), "retained state").unwrap();
        let mut live = anyagents_supervisor::LiveState {
            process_alive: true,
            turn_state: anyagents_harness::TurnState::Idle,
            delivery: Some(crate::vendor::declared_delivery(&agent.harness)),
            quota_reset_at_ms: None,
            restart_at_ms: None,
            pid: None,
            live_harness: Some(agent.harness.clone()),
            live_model: None,
            process_started_at_ms: None,
            last_error: None,
        };
        state.health.publish(&agent.id, live.clone());
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let refused = delete_agent_route(
                Principal::Operator,
                State(state.clone()),
                AxumPath(agent.name.clone()),
            )
            .await
            .expect_err("live process must retain identity");
            assert_eq!(refused.status, StatusCode::CONFLICT);
            assert_eq!(state.store.agent(&agent.id).unwrap().state, "archived");
            assert_eq!(
                std::fs::read_to_string(config.join("preserve")).unwrap(),
                "retained state"
            );
            let process = state.store.try_lock_agent(&agent.id).unwrap();
            live.process_alive = false;
            state.health.publish(&agent.id, live);
            let refused = delete_agent_route(
                Principal::Operator,
                State(state.clone()),
                AxumPath(agent.name.clone()),
            )
            .await
            .expect_err("optimistic health must not revoke process custody");
            assert_eq!(refused.status, StatusCode::CONFLICT);
            assert!(state.store.agent(&agent.id).is_ok());
            assert_eq!(
                std::fs::read_to_string(config.join("preserve")).unwrap(),
                "retained state"
            );
            drop(process);
            let _ = delete_agent_route(
                Principal::Operator,
                State(state.clone()),
                AxumPath(agent.name.clone()),
            )
            .await
            .unwrap_or_else(|error| panic!("{}", error.message));
            assert!(state.store.agent(&agent.id).is_err());
            assert!(!config.exists());
        });
    }

    #[test]
    fn deletion_waits_for_process_custody_after_optimistic_stopped_health() {
        let dir = crate::test_tempdir();
        let state = archetype_validation_state(dir.path());
        let agent = state
            .store
            .create_agent(anyagents_core::NewAgent::new("stopping", "opencode"))
            .unwrap();
        state.store.set_agent_running(&agent.id, "/work").unwrap();
        let config = anyagents_core::agent_config_dir(dir.path(), &agent.id);
        std::fs::create_dir_all(&config).unwrap();
        std::fs::write(config.join("preserve"), "retained state").unwrap();
        let process = state.store.try_lock_agent(&agent.id).unwrap();
        // An absent health row is the same optimistic stopped projection used
        // during supervisor shutdown; actual process custody remains occupied.
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let deleting = tokio::spawn(delete_agent_route(
                Principal::Operator,
                State(state.clone()),
                AxumPath(agent.name.clone()),
            ));
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while state.store.agent(&agent.id).unwrap().state != "archived" {
                assert!(std::time::Instant::now() < deadline);
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            tokio::time::sleep(std::time::Duration::from_millis(80)).await;
            assert!(
                !deleting.is_finished(),
                "shutdown must wait for actual custody"
            );
            assert_eq!(
                std::fs::read_to_string(config.join("preserve")).unwrap(),
                "retained state"
            );
            drop(process);
            let _ = deleting
                .await
                .unwrap()
                .unwrap_or_else(|error| panic!("{}", error.message));
            assert!(state.store.agent(&agent.id).is_err());
            assert!(!config.exists());
        });
    }

    #[test]
    fn deleted_authenticated_agent_cannot_read_roster_or_send() {
        let dir = crate::test_tempdir();
        let state = archetype_validation_state(dir.path());
        let root = state
            .store
            .create_agent(anyagents_core::NewAgent::new("private-root", "opencode"))
            .unwrap();
        state
            .store
            .set_agent_instructions(&root.id, Some("private instruction sentinel"))
            .unwrap();
        let mut new_child = anyagents_core::NewAgent::new("deleted-caller", "opencode");
        new_child.parent_id = Some(root.id.clone());
        let child = state.store.create_agent(new_child).unwrap();
        let principal = Principal::Agent {
            agent_id: child.id.clone(),
            name: child.name.clone(),
        };
        let actor = Actor::Agent(child.name.clone());
        let body = SendRequest {
            to: vec![root.name.clone()],
            text: "must not arrive".into(),
            ..Default::default()
        };
        let plan = plan_send(&state.store, &principal, &actor, &body)
            .unwrap_or_else(|e| panic!("{}", e.message));
        // Positive controls use the same authenticated handler and plan path.
        let visible =
            list_agents_sync_for_principal(&state, &principal, &ListAgentsQuery::default())
                .unwrap_or_else(|e| panic!("{}", e.message));
        assert!(
            visible
                .0
                .agents
                .iter()
                .all(|a| a.name != root.name || a.instructions.is_none())
        );
        assert!(can_actor_message_agent(&state.store, &actor, &root).is_ok());
        state.store.delete_agent(&child.id).unwrap();
        let denied =
            list_agents_sync_for_principal(&state, &principal, &ListAgentsQuery::default())
                .expect_err("deleted caller must not receive any roster");
        assert_eq!(denied.status, StatusCode::FORBIDDEN);
        let denied = send_sync(&state.store, &actor, body.clone(), plan, None)
            .expect_err("deleted principal must not deliver");
        assert_eq!(denied.status, StatusCode::FORBIDDEN);
        assert!(state.store.inbox(&root.id, None).unwrap().is_empty());
        assert!(plan_send(&state.store, &principal, &actor, &body).is_err());
        // Reusing the mutable display name does not restore the old principal.
        state
            .store
            .create_agent(anyagents_core::NewAgent::new(&child.name, "opencode"))
            .unwrap();
        let denied = plan_send(&state.store, &principal, &actor, &body)
            .expect_err("name reuse must not authorize old credentials");
        assert_eq!(denied.status, StatusCode::FORBIDDEN);
        let operator = list_agents_sync_for_principal(
            &state,
            &Principal::Operator,
            &ListAgentsQuery::default(),
        )
        .unwrap_or_else(|e| panic!("{}", e.message));
        assert!(operator.0.agents.iter().any(|a| a.name == root.name
            && a.instructions.as_deref() == Some("private instruction sentinel")));
    }

    #[test]
    fn renamed_authenticated_sender_does_not_acquire_reused_name_identity() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let original = store
            .create_agent(anyagents_core::NewAgent::new("original", "opencode"))
            .unwrap();
        let principal = Principal::Agent {
            agent_id: original.id.clone(),
            name: original.name.clone(),
        };
        let actor = Actor::Agent(original.name.clone());
        store.rename_agent(&original.id, "renamed").unwrap();
        let replacement = store
            .create_agent(anyagents_core::NewAgent::new("original", "opencode"))
            .unwrap();
        let receiver = store
            .create_agent(anyagents_core::NewAgent::new("recipient", "opencode"))
            .unwrap();
        let body = SendRequest {
            to: vec![receiver.name.clone()],
            text: "identity sentinel".into(),
            ..Default::default()
        };
        let plan = plan_send(&store, &principal, &actor, &body)
            .unwrap_or_else(|error| panic!("{}", error.message));
        assert_eq!(plan.sender_id.as_ref(), Some(&original.id));
        assert_ne!(plan.sender_id.as_ref(), Some(&replacement.id));
        let _ = send_sync(&store, &actor, body, plan, None)
            .unwrap_or_else(|error| panic!("{}", error.message));
        let received = store.inbox(&receiver.id, None).unwrap();
        assert_eq!(
            received[0].sender.as_deref(),
            Some(format!("agent:{}", original.id).as_str())
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn local_sender_identity_survives_rename_and_refuses_deletion_while_journal_waits() {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        use std::time::{Duration, Instant};
        for delete in [false, true] {
            let dir = crate::test_tempdir();
            let store = Store::open(dir.path().join("state.db")).unwrap();
            let original = store
                .create_agent(anyagents_core::NewAgent::new("author", "opencode"))
                .unwrap();
            store
                .set_agent_role(&original.id, Some("admitted role"))
                .unwrap();
            let mut child = anyagents_core::NewAgent::new("recipient", "opencode");
            child.parent_id = Some(original.id.clone());
            let recipient = store.create_agent(child).unwrap();
            let actor = Actor::Agent(original.name.clone());
            let body = SendRequest {
                to: vec![recipient.name.clone()],
                text: "queued identity sentinel".into(),
                ..Default::default()
            };
            let plan =
                SendPlan::parse(&store, &actor, &body).unwrap_or_else(|e| panic!("{}", e.message));
            let key = "identity-lock-test";
            let filename: String = format!("{key}:0")
                .as_bytes()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            let journal = dir.path().join("outbound-history");
            std::fs::create_dir_all(&journal).unwrap();
            let lock = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(journal.join(format!("{filename}.lock")))
                .unwrap();
            lock.lock().unwrap();
            let inode = lock.metadata().unwrap().ino();
            std::thread::scope(|scope| {
                let sending = scope.spawn(|| {
                    send_sync(&store, &actor, body, plan, Some(key.into())).map_err(|e| e.message)
                });
                let deadline = Instant::now() + Duration::from_secs(10);
                let waiting = loop {
                    let waiting =
                        std::fs::read_to_string("/proc/locks")
                            .unwrap()
                            .lines()
                            .any(|line| {
                                line.contains("-> FLOCK")
                                    && line.contains("WRITE")
                                    && line
                                        .split_whitespace()
                                        .any(|field| field.ends_with(&format!(":{inode}")))
                            });
                    if waiting || Instant::now() >= deadline {
                        break waiting;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                };
                // Release even on a failed observation, so the fixture cannot deadlock.
                if waiting {
                    if delete {
                        // Preserve the recipient while removing its parent; the
                        // production delete path separately refuses live children.
                        store.set_agent_parent(&recipient.id, None).unwrap();
                        store.delete_agent(&original.id).unwrap();
                    } else {
                        store.rename_agent(&original.id, "renamed").unwrap();
                    }
                    let replacement = store
                        .create_agent(anyagents_core::NewAgent::new("author", "opencode"))
                        .unwrap();
                    store
                        .set_agent_role(&replacement.id, Some("wrong role"))
                        .unwrap();
                }
                drop(lock);
                let result = sending.join().unwrap();
                if delete {
                    let error = result.expect_err("deleted admitted author cannot publish");
                    assert!(error.contains("agent not found"), "{error}");
                } else {
                    let _ = result.unwrap();
                }
                assert!(waiting, "actual journal-lock waiter must be observed");
            });
            let inbox = store.inbox(&recipient.id, None).unwrap();
            if delete {
                assert!(inbox.is_empty());
                continue;
            }
            assert_eq!(inbox.len(), 1);
            assert_eq!(
                inbox[0].sender.as_deref(),
                Some(format!("agent:{}", original.id).as_str())
            );
            assert_eq!(
                inbox[0].context.sender_role.as_deref(),
                Some("admitted role")
            );
            assert_eq!(inbox[0].context.sender_rel.as_deref(), Some("parent"));
        }
    }

    #[test]
    fn keyed_local_fanout_recovers_partial_delivery_without_duplicates() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let sender = store
            .create_agent(anyagents_core::NewAgent::new("sender", "opencode"))
            .unwrap();
        let first = store
            .create_agent(anyagents_core::NewAgent::new("first", "opencode"))
            .unwrap();
        let second = store
            .create_agent(anyagents_core::NewAgent::new("second", "opencode"))
            .unwrap();
        for agent in [&sender, &first, &second] {
            store.set_agent_state(&agent.id, "running").unwrap();
            store.join_group("room", &agent.id).unwrap();
        }
        let db = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
        db.execute_batch(&format!("CREATE TRIGGER fail_recipient BEFORE INSERT ON messages WHEN NEW.agent_id = '{}' BEGIN SELECT RAISE(ABORT, 'injected second recipient failure'); END;", second.id)).unwrap();
        let actor = Actor::Agent(sender.name.clone());
        let body = SendRequest {
            sender: Some(actor.as_address()),
            to: vec!["group:room".into()],
            text: "fanout sentinel".into(),
            ..Default::default()
        };
        let plan = SendPlan::parse(&store, &actor, &body)
            .unwrap_or_else(|error| panic!("{}", error.message));
        assert!(
            send_sync(
                &store,
                &actor,
                body.clone(),
                plan,
                Some("fanout-call".into())
            )
            .is_err()
        );
        assert_eq!(store.inbox(&first.id, None).unwrap().len(), 1);
        assert!(store.inbox(&second.id, None).unwrap().is_empty());
        db.execute_batch("DROP TRIGGER fail_recipient;").unwrap();
        let plan = SendPlan::parse(&store, &actor, &body)
            .unwrap_or_else(|error| panic!("{}", error.message));
        let _ = send_sync(&store, &actor, body, plan, Some("fanout-call".into()))
            .unwrap_or_else(|error| panic!("{}", error.message));
        assert_eq!(store.inbox(&first.id, None).unwrap().len(), 1);
        assert_eq!(store.inbox(&second.id, None).unwrap().len(), 1);
        assert_eq!(
            db.query_row("SELECT COUNT(*) FROM outbound_messages", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn missing_sender_cannot_bypass_private_room_checks() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let sender = store
            .create_agent(anyagents_core::NewAgent::new("sender", "opencode"))
            .unwrap();
        let recipient = store
            .create_agent(anyagents_core::NewAgent::new("recipient", "opencode"))
            .unwrap();
        store.join_group("private-room", &recipient.id).unwrap();
        let actor = Actor::Agent(sender.name.clone());
        let body = SendRequest {
            to: vec!["group:private-room".into()],
            text: "blocked".into(),
            ..Default::default()
        };
        let plan =
            SendPlan::parse(&store, &actor, &body).unwrap_or_else(|e| panic!("{}", e.message));
        store.delete_agent(&sender.id).unwrap();
        // A replacement with the same name must not acquire the old plan.
        store
            .create_agent(anyagents_core::NewAgent::new(&sender.name, "opencode"))
            .unwrap();
        let denied = check_targets(&store, &actor, &body, &plan)
            .expect_err("missing actor must not pass room authorization");
        assert_eq!(denied.status, StatusCode::FORBIDDEN);
        assert!(store.inbox(&recipient.id, None).unwrap().is_empty());
    }

    #[test]
    fn reset_session_requires_management_authority_and_quiescence_before_any_pin_change() {
        use anyagents_core::host_session;
        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let parent = store
            .create_agent(anyagents_core::NewAgent::new("reset-parent", "opencode"))
            .unwrap();
        let mut new_child = anyagents_core::NewAgent::new("reset-child", "opencode");
        new_child.parent_id = Some(parent.id.clone());
        let child = store.create_agent(new_child).unwrap();
        let peer = store
            .create_agent(anyagents_core::NewAgent::new("reset-peer", "opencode"))
            .unwrap();
        store.set_agent_state(&child.id, "paused").unwrap();
        let root = dir.path().join("agents").join(child.id.as_str());
        std::fs::create_dir_all(root.join("config")).unwrap();
        std::fs::create_dir_all(root.join("memory")).unwrap();
        std::fs::write(root.join("memory/MEMORY.md"), "before reset").unwrap();
        let old = host_session::ensure_active(&root, "opencode").unwrap();
        std::fs::write(root.join("config/opencode-session-id"), "old-id\n").unwrap();
        std::fs::write(root.join("config/raw.jsonl"), "raw retained").unwrap();
        std::fs::write(root.join("memory/MEMORY.md"), "after reset").unwrap();
        let before = std::fs::read(root.join("sessions/registry.json")).unwrap();
        let pins = || std::fs::read_dir(root.join("sessions")).unwrap().count();
        let before_pins = pins();
        let state = AppState {
            store: Arc::clone(&store),
            health: HealthBoard::default(),
            sign_in: Arc::new(Mutex::new(ProcessRunner)),
            auth: auth::Mode::Trusted {
                data_dir: dir.path().into(),
            },
            identity: Arc::new(ServingIdentity::capture()),
            profiles: Default::default(),
        };
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let principal = |a: &Agent| Principal::Agent {
                agent_id: a.id.clone(),
                name: a.name.clone(),
            };
            let unchanged = || {
                assert_eq!(
                    std::fs::read(root.join("sessions/registry.json")).unwrap(),
                    before
                );
                assert_eq!(
                    std::fs::read(root.join("config/opencode-session-id")).unwrap(),
                    b"old-id\n"
                );
                assert_eq!(pins(), before_pins);
            };
            let denied = reset_session_route(
                principal(&peer),
                State(state.clone()),
                AxumPath(child.name.clone()),
            )
            .await
            .unwrap_err();
            assert_eq!(denied.status, StatusCode::FORBIDDEN);
            unchanged();
            let process = store.try_lock_agent(&child.id).unwrap();
            let busy = reset_session_route(
                principal(&parent),
                State(state.clone()),
                AxumPath(child.name.clone()),
            )
            .await
            .unwrap_err();
            assert_eq!(busy.status, StatusCode::CONFLICT);
            unchanged();
            drop(process);
            let lifecycle = store.lock_agent_lifecycle(&child.id).unwrap();
            let busy = reset_session_route(
                principal(&parent),
                State(state.clone()),
                AxumPath(child.name.clone()),
            )
            .await
            .unwrap_err();
            assert_eq!(busy.status, StatusCode::CONFLICT);
            unchanged();
            drop(lifecycle);
            store.set_agent_state(&child.id, "running").unwrap();
            let busy = reset_session_route(
                principal(&parent),
                State(state.clone()),
                AxumPath(child.name.clone()),
            )
            .await
            .unwrap_err();
            assert_eq!(busy.status, StatusCode::CONFLICT);
            unchanged();
            store.set_agent_state(&child.id, "paused").unwrap();
            let Json(result) = reset_session_route(
                principal(&parent),
                State(state),
                AxumPath(child.name.clone()),
            )
            .await
            .unwrap_or_else(|e| panic!("{}", e.message));
            let active = host_session::active(&root).unwrap().unwrap();
            assert_eq!(result["generation"], active.generation);
            assert_ne!(active.generation, old.generation);
            assert!(active.memory.section.contains("after reset"));
            assert_eq!(
                std::fs::read(root.join("config/raw.jsonl")).unwrap(),
                b"raw retained"
            );
            assert_eq!(
                host_session::resume_vendor(&root, "opencode", "old-id").unwrap(),
                old
            );
        });
    }

    #[test]
    fn canonical_raw_ranges_bind_owned_digest_and_return_bounded_exact_bytes() {
        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let parent = store
            .create_agent(anyagents_core::NewAgent::new("range-parent", "spiron"))
            .unwrap();
        let mut new = anyagents_core::NewAgent::new("range-child", "spiron");
        new.parent_id = Some(parent.id.clone());
        let child = store.create_agent(new).unwrap();
        let peer = store
            .create_agent(anyagents_core::NewAgent::new("range-peer", "spiron"))
            .unwrap();
        let bytes: Vec<u8> = [
            b"rangeneedle ".as_slice(),
            &[0, 0xff, 0xfe],
            "繁體".as_bytes(),
            &vec![b'x'; 100_000],
        ]
        .concat();
        for a in [&parent, &child, &peer] {
            let root = dir.path().join("agents").join(a.id.as_str()).join("memory");
            std::fs::create_dir_all(&root).unwrap();
            std::fs::write(root.join("MEMORY.md"), &bytes).unwrap();
        }
        let state = AppState {
            store: Arc::clone(&store),
            health: HealthBoard::default(),
            sign_in: Arc::new(Mutex::new(ProcessRunner)),
            auth: auth::Mode::Trusted {
                data_dir: dir.path().into(),
            },
            identity: Arc::new(ServingIdentity::capture()),
            profiles: Default::default(),
        };
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let principal = || Principal::Agent { agent_id: parent.id.clone(), name: parent.name.clone() };
            let Json(all) = conversation_route(Principal::Operator, State(state.clone()), AxumPath("all".into()),
                Query(serde_json::from_value(serde_json::json!({"scope":"memory","query":"rangeneedle"})).unwrap())).await.unwrap_or_else(|e| panic!("{}", e.message));
            assert_eq!(all.turns.len(), 3);
            let find = |owner: &Agent| all.turns.iter().find(|t| t.raw.as_ref().unwrap().segment.contains(owner.id.as_str())).unwrap();
            let target = find(&child);
            let request = |id: &str, sha: &str, offset: u64, length: u64| serde_json::from_value(serde_json::json!({"scope":"memory","raw_id":id,"raw_sha256":sha,"raw_offset":offset,"raw_length":length})).unwrap();
            let Json(response) = conversation_route(principal(), State(state.clone()), AxumPath("all".into()),
                Query(request(&target.id, &target.raw.as_ref().unwrap().sha256, 11, 65536))).await.unwrap_or_else(|e| panic!("{}", e.message));
            let turn = &response.turns[0];
            assert_eq!(response.agent_id, child.id.as_str());
            assert_eq!(turn.raw_bytes.as_deref(), Some(&bytes[11..65547]));
            assert_eq!(turn.raw.as_ref().unwrap().length, bytes.len() as u64);
            assert_eq!(turn.window.as_ref().unwrap().length, 65536);
            assert_eq!(turn.window.as_ref().unwrap().sha256, anyagents_core::learned_memory::sha256(&bytes[11..65547]));
            assert!(turn.text.contains('�'), "text is explicitly a lossy preview");
            let peer_hit = find(&peer);
            // Make the hidden file fail if opened. Its ID must still be refused
            // as not found, not reach descriptor validation or reveal the path.
            std::fs::hard_link(&peer_hit.raw.as_ref().unwrap().segment, dir.path().join("hidden-hardlink")).unwrap();
            let denied = conversation_route(principal(), State(state.clone()), AxumPath("all".into()),
                Query(request(&peer_hit.id, &peer_hit.raw.as_ref().unwrap().sha256, 0, 32))).await.unwrap_err();
            assert_eq!(denied.status, StatusCode::NOT_FOUND);
            let denied = conversation_route(principal(), State(state.clone()), AxumPath(peer.name.clone()),
                Query(request(&peer_hit.id, &peer_hit.raw.as_ref().unwrap().sha256, 0, 32))).await.unwrap_err();
            assert_eq!(denied.status, StatusCode::FORBIDDEN);
            let denied = conversation_route(principal(), State(state.clone()), AxumPath(parent.name.clone()),
                Query(request(&target.id, &target.raw.as_ref().unwrap().sha256, 0, 32))).await.unwrap_err();
            assert_eq!(denied.status, StatusCode::NOT_FOUND, "explicit owner selector must bind the document");
            for (offset,length) in [(0,0),(0,65537),(u64::MAX,1),(bytes.len() as u64+1,1)] {
                let error = conversation_route(principal(), State(state.clone()), AxumPath("all".into()),
                    Query(request(&target.id, &target.raw.as_ref().unwrap().sha256, offset, length))).await.unwrap_err();
                assert_eq!(error.status, StatusCode::BAD_REQUEST);
            }
            let Json(tail) = conversation_route(principal(), State(state.clone()), AxumPath("all".into()),
                Query(request(&target.id, &target.raw.as_ref().unwrap().sha256, bytes.len() as u64-5, 32))).await.unwrap_or_else(|e| panic!("{}", e.message));
            assert_eq!(tail.turns[0].raw_bytes.as_deref(), Some(&bytes[bytes.len()-5..]));
            let error = conversation_route(principal(), State(state.clone()), AxumPath("all".into()),
                Query(request(&target.id, &"0".repeat(64), 0, 32))).await.unwrap_err();
            assert_eq!(error.status, StatusCode::CONFLICT);
            let error = conversation_route(principal(), State(state.clone()), AxumPath("all".into()),
                Query(request(&target.raw.as_ref().unwrap().segment, &target.raw.as_ref().unwrap().sha256, 0, 32))).await.unwrap_err();
            assert_eq!(error.status, StatusCode::NOT_FOUND, "a caller path is not a document ID");
            let mut edited = bytes.clone(); edited[99_000] = b'y';
            std::fs::write(&target.raw.as_ref().unwrap().segment, edited).unwrap();
            let error = conversation_route(principal(), State(state), AxumPath("all".into()),
                Query(request(&target.id, &target.raw.as_ref().unwrap().sha256, 0, 32))).await.unwrap_err();
            assert_eq!(error.status, StatusCode::CONFLICT, "mutation outside requested range must fail whole-event SHA");
        });
    }

    #[test]
    fn canonical_conversation_scopes_files_under_real_ancestor_authority() {
        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let parent = store
            .create_agent(anyagents_core::NewAgent::new("history-parent", "spiron"))
            .unwrap();
        let mut new = anyagents_core::NewAgent::new("history-child", "spiron");
        new.parent_id = Some(parent.id.clone());
        let child = store.create_agent(new).unwrap();
        let peer = store
            .create_agent(anyagents_core::NewAgent::new("history-peer", "spiron"))
            .unwrap();
        for a in [&parent, &child, &peer] {
            let root = dir.path().join("agents").join(a.id.as_str());
            std::fs::create_dir_all(root.join("memory")).unwrap();
            std::fs::create_dir_all(root.join("config")).unwrap();
            std::fs::write(
                root.join("memory/MEMORY.md"),
                format!("{} canonicalneedle {}", "padding ".repeat(1700), a.name),
            )
            .unwrap();
            std::fs::write(root.join("config/sylphx-transcript.jsonl"),serde_json::to_string(&serde_json::json!({"role":"tool","name":"browser","timestamp_ms":100,"content":format!("canonicalneedle {}",a.name)})).unwrap()+"\n").unwrap();
        }
        let state = AppState {
            store: Arc::clone(&store),
            health: HealthBoard::default(),
            sign_in: Arc::new(Mutex::new(ProcessRunner)),
            auth: auth::Mode::Trusted {
                data_dir: dir.path().into(),
            },
            identity: Arc::new(ServingIdentity::capture()),
            profiles: Default::default(),
        };
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let principal=||Principal::Agent {agent_id:parent.id.clone(),name:parent.name.clone()};
            for (scope,count) in [("all",4),("memory",2),("transcripts",2)] {
                let query=serde_json::from_value(serde_json::json!({"scope":scope,"query":"canonicalneedle"})).unwrap();
                let Json(response)=conversation_route(principal(),State(state.clone()),AxumPath("all".into()),Query(query)).await.unwrap_or_else(|e|panic!("{}",e.message));
                assert_eq!(response.turns.len(),count,"{scope}");assert!(response.turns.iter().all(|t|!t.text.contains("history-peer")));
                assert!(response.turns.iter().all(|t|t.raw.is_some() && t.window.is_some()));
                for t in &response.turns {let raw=t.raw.as_ref().unwrap();let bytes=std::fs::read(&raw.segment).unwrap();assert_eq!(anyagents_core::learned_memory::sha256(&bytes[raw.offset as usize..(raw.offset+raw.length) as usize]),raw.sha256);}
            }
            // Native records lacking time must remain null at the canonical
            // surface, never inherit a recent filesystem timestamp.
            let native = dir.path().join("agents").join(child.id.as_str()).join("config/sylphx-transcript.jsonl");
            use std::io::Write;
            std::fs::OpenOptions::new().append(true).open(native).unwrap().write_all(b"{\"role\":\"user\",\"content\":\"untimedcanonical\"}\n").unwrap();
            let Json(untimed)=conversation_route(principal(),State(state.clone()),AxumPath(child.name.clone()),Query(serde_json::from_value(serde_json::json!({"scope":"transcripts","query":"untimedcanonical"})).unwrap())).await.unwrap_or_else(|e|panic!("{}", e.message));
            assert_eq!(untimed.turns.len(), 1);
            assert!(untimed.turns[0].timestamp_ms.is_none());
            assert!(serde_json::to_value(&untimed).unwrap()["turns"][0]["timestamp_ms"].is_null());
            // Source-shaped synthetic Claude blocks, exercised through the
            // canonical selector rather than only direct index queries.
            let native = dir.path().join("agents").join(child.id.as_str()).join("config/sylphx-transcript.jsonl");
            std::fs::OpenOptions::new().append(true).open(&native).unwrap().write_all(
                "{\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"tool_use\",\"id\":\"canon-shell\",\"name\":\"shell\",\"input\":{}},{\"type\":\"tool_use\",\"id\":\"canon-files\",\"name\":\"files\",\"input\":{}}]}}\n"
            .as_bytes()).unwrap();
            let Json(multi)=conversation_route(principal(),State(state.clone()),AxumPath(child.name.clone()),Query(serde_json::from_value(serde_json::json!({"scope":"transcripts","tool_name":"files","include_tools":false})).unwrap())).await.unwrap_or_else(|e|panic!("{}", e.message));
            assert_eq!(multi.turns.len(), 1);
            assert_eq!(multi.turns[0].tool_names, ["files", "shell"]);
            assert_eq!(serde_json::to_value(&multi).unwrap()["turns"][0]["tool_names"], serde_json::json!(["files", "shell"]));
            std::fs::OpenOptions::new().append(true).open(native).unwrap().write_all(b"{\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"canon-shell\",\"content\":\"canonicaltoolresult\"}]}}\n").unwrap();
            let Json(result)=conversation_route(principal(),State(state.clone()),AxumPath(child.name.clone()),Query(serde_json::from_value(serde_json::json!({"scope":"transcripts","tool_name":"shell","query":"canonicaltoolresult"})).unwrap())).await.unwrap_or_else(|e|panic!("{}", e.message));
            assert_eq!(result.turns.len(), 1);
            assert_eq!(result.turns[0].tool_names, ["shell"]);
            let denied=conversation_route(principal(),State(state.clone()),AxumPath(peer.name.clone()),Query(serde_json::from_value(serde_json::json!({"scope":"memory"})).unwrap())).await.unwrap_err();assert_eq!(denied.status,StatusCode::FORBIDDEN);
            let query=serde_json::from_value(serde_json::json!({"scope":"all","only_tools":true,"tool_name":"browser","from_seq":1,"to_seq":1})).unwrap();
            let Json(response)=conversation_route(principal(),State(state),AxumPath("all".into()),Query(query)).await.unwrap_or_else(|e|panic!("{}",e.message));assert_eq!(response.turns.len(),2);assert!(response.turns.iter().all(|t|t.tool_name.as_deref()==Some("browser")));
        });
    }

    /// Answers one sign-in with two lines and records where it ran.
    struct Scripted {
        dir: Arc<Mutex<Option<PathBuf>>>,
    }

    impl CommandRunner for Scripted {
        fn run(
            &mut self,
            _program: &str,
            _args: &[String],
            dir: &Path,
            env: &[(&str, &str)],
            on_line: &mut dyn LineSink,
        ) -> Result<(), InstallError> {
            *self.dir.lock().unwrap() = Some(dir.to_path_buf());
            assert!(env.iter().any(|(key, _)| *key == "CODEX_HOME"));
            on_line.line("LINK https://auth.example/device");
            on_line.line("SIGNED_IN ada@example");
            Ok(())
        }
    }

    /// These tests start a server in this process, so the environment they set
    /// is read under this lock and put back before the next one runs.
    static ENV: Mutex<()> = Mutex::new(());

    /// One lock for every test that sets this process's environment, so a
    /// server started by one never reads another's settings.
    pub(crate) fn env_lock() -> MutexGuard<'static, ()> {
        ENV.lock().unwrap_or_else(|err| err.into_inner())
    }

    /// Process-local environment locks do not isolate forked children or
    /// process-global PATH. Keep one-tick lifecycle assertions in one worker.
    fn single_test_worker(name: &str) -> bool {
        let name = format!("server::tests::{name}");
        if std::env::var("ANYAGENTS_SINGLE_TEST_WORKER").as_deref() == Ok(name.as_str()) {
            return false;
        }
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &name, "--nocapture"])
            .env("ANYAGENTS_SINGLE_TEST_WORKER", &name)
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(
            out.status.success() && stdout.contains("1 passed; 0 failed;"),
            "{name} worker failed or did not execute: {stdout}{}",
            String::from_utf8_lossy(&out.stderr)
        );
        true
    }

    #[test]
    fn describe_preserves_scope_and_audits_authorized_policy_updates() {
        let _env = env_lock();
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        store
            .hold_capability_key("test-capability-key-0123456789abcdef")
            .unwrap();
        let parent = store
            .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
            .unwrap();
        let mut new = anyagents_core::NewAgent::new("mind", "codex");
        new.parent_id = Some(parent.id.clone());
        new.role = Some("Mind".into());
        let mind = store.create_agent(new).unwrap();
        let state = AppState {
            store: Arc::clone(&store),
            health: HealthBoard::default(),
            sign_in: Arc::new(Mutex::new(ProcessRunner)),
            identity: Arc::new(ServingIdentity::capture()),
            profiles: Default::default(),
            auth: auth::Mode::Trusted {
                data_dir: dir.path().to_path_buf(),
            },
        };
        let caller = |agent: &Agent| Principal::Agent {
            agent_id: agent.id.clone(),
            name: agent.name.clone(),
        };
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let mut expected_audit = Vec::new();
            for (body, before, after, source) in [
                (serde_json::json!({"scope":"siblings"}), "private", "siblings", "scope"),
                (serde_json::json!({"description":"Public charter", "public":false}), "siblings", "siblings", "public"),
                (serde_json::json!({"instructions":"Private instructions"}), "siblings", "siblings", ""),
                (serde_json::json!({"role":"Updated Mind"}), "siblings", "siblings", ""),
                (serde_json::json!({"public":true}), "siblings", "public", "public"),
                (serde_json::json!({"public":false}), "public", "private", "public"),
                (serde_json::json!({"scope":"isolated", "public":true}), "private", "isolated", "scope"),
                (serde_json::json!({"public":false}), "isolated", "isolated", "public"),
                (serde_json::json!({"scope":"private"}), "isolated", "private", "scope"),
                (serde_json::json!({"public":false}), "private", "private", "public"),
                (serde_json::json!({"scope":"siblings"}), "private", "siblings", "scope"),
            ] {
                let Json(updated) = set_description_route(
                    caller(&parent), State(state.clone()), AxumPath("mind".into()),
                    Json(serde_json::from_value(body).unwrap()),
                ).await.unwrap_or_else(|e| panic!("{}: {}", e.status, e.message));
                assert_eq!(updated["scope"], after);
                if !source.is_empty() {
                    expected_audit.push(("agent:parent".to_string(), before.to_string(), after.to_string(), source.to_string()));
                }
            }
            let before = store.agent(&mind.id).unwrap();
            let outsider = store.create_agent(anyagents_core::NewAgent::new("outsider", "codex")).unwrap();
            let mut sibling_new = anyagents_core::NewAgent::new("sibling", "codex");
            sibling_new.parent_id = Some(parent.id.clone());
            let sibling = store.create_agent(sibling_new).unwrap();
            for agent in [&outsider, &sibling] {
                for body in [serde_json::json!({"scope":"public"}), serde_json::json!({"description":"Overwrite", "instructions":"Overwrite"})] {
                    let err = set_description_route(caller(agent), State(state.clone()), AxumPath("mind".into()), Json(serde_json::from_value(body).unwrap()))
                        .await.expect_err("unrelated caller must be refused");
                    assert_eq!(err.status, StatusCode::FORBIDDEN);
                    assert_eq!(store.agent(&mind.id).unwrap(), before);
                }
            }
            for self_caller in [&mind, &parent] {
                for body in [
                    serde_json::json!({"scope":"public"}),
                    serde_json::json!({"description":"Self-modified description"}),
                    serde_json::json!({"instructions":"Self-modified instructions"}),
                ] {
                    let err = set_description_route(caller(self_caller), State(state.clone()), AxumPath("self".into()), Json(serde_json::from_value(body).unwrap()))
                        .await.expect_err("non-admin self cannot mutate description, instructions, or scope");
                    assert_eq!(err.status, StatusCode::FORBIDDEN);
                }
                let Json(read_self) = get_description_route(caller(self_caller), State(state.clone()), AxumPath("self".into()))
                    .await.expect("self can read own description");
                assert_eq!(read_self["name"], self_caller.name);
            }
            assert_eq!(store.agent(&mind.id).unwrap(), before);

            // An admin's self update is allowed and has the same atomic audit.
            store.set_agent_admin(&parent.id, true).unwrap();
            let _ = set_description_route(caller(&parent), State(state.clone()), AxumPath("self".into()), Json(serde_json::from_value(serde_json::json!({"scope":"isolated", "description":"Admin self charter"})).unwrap()))
                .await.unwrap_or_else(|e| panic!("{}", e.message));
            let conn = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
            let mut stmt = conn.prepare("SELECT actor,before_scope,after_scope,source FROM admin_audit WHERE target='mind' AND source IS NOT NULL ORDER BY id").unwrap();
            let rows: Vec<(String,String,String,String)> = stmt.query_map([], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap().map(Result::unwrap).collect();
            assert_eq!(rows, expected_audit);
            let self_audit: (String,String,String,String) = conn.query_row("SELECT actor,before_scope,after_scope,source FROM admin_audit WHERE target='parent' AND source IS NOT NULL", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
            assert_eq!(self_audit, ("agent:parent".into(),"public".into(),"isolated".into(),"scope".into()));
            assert_eq!(store.agent(&mind.id).unwrap().description.as_deref(), Some("Public charter"));
            // Every runtime-config pass reads the store's policy; none owns it.
            for _ in 0..2 {
                let current = store.agent(&mind.id).unwrap();
                let config = anyagents_core::agent_config_dir(dir.path(), &mind.id);
                let work = anyagents_core::agent_work_dir(dir.path(), &mind.id);
                create::reconcile_agent_runtime_config(&store, dir.path(), &current, &config, &work).unwrap();
                let reloaded = Store::open(dir.path().join("state.db")).unwrap().agent(&mind.id).unwrap();
                assert_eq!(reloaded.scope(), "siblings");
                assert_eq!(reloaded.description, current.description);
            }
        });
    }

    #[test]
    fn spawn_defaults_to_private_regardless_of_role() {
        let _env = env_lock();
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        store
            .hold_capability_key("test-capability-key-0123456789abcdef")
            .unwrap();
        let parent = store
            .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
            .unwrap();
        let state = AppState {
            store: Arc::clone(&store),
            health: HealthBoard::default(),
            sign_in: Arc::new(Mutex::new(ProcessRunner)),
            identity: Arc::new(ServingIdentity::capture()),
            profiles: Default::default(),
            auth: auth::Mode::Trusted {
                data_dir: dir.path().into(),
            },
        };
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            for (name, role, scope) in [("labelled", Some("Researcher"), "private"), ("task", None, "private")] {
                let body = serde_json::from_value(serde_json::json!({"name":name,"role":role,"harness":"codex","model":"test-model","workdir":"private"})).unwrap();
                let (_, Json(created)) = create_agent_route(Principal::Agent { agent_id: parent.id.clone(), name: parent.name.clone() }, State(state.clone()), axum::http::HeaderMap::new(), Json(body)).await.unwrap_or_else(|e| panic!("{}: {}",e.status,e.message));
                assert_eq!(created.scope, scope);
                let _ = set_description_route(Principal::Agent { agent_id: parent.id.clone(), name: parent.name.clone() }, State(state.clone()), AxumPath(name.into()), Json(serde_json::from_value(serde_json::json!({"public":true})).unwrap())).await.unwrap_or_else(|e| panic!("{}",e.message));
                let _ = set_description_route(Principal::Agent { agent_id: parent.id.clone(), name: parent.name.clone() }, State(state.clone()), AxumPath(name.into()), Json(serde_json::from_value(serde_json::json!({"public":false})).unwrap())).await.unwrap_or_else(|e| panic!("{}",e.message));
                assert_eq!(store.agent_by_name(name).unwrap().scope(),scope);
            }
        });
    }

    #[test]
    fn local_scheduler_keeps_ticking_while_external_probe_is_blocked() {
        use anyagents_core::{NewAgent, NewSchedule};
        use std::time::{Duration, Instant};
        if single_test_worker("local_scheduler_keeps_ticking_while_external_probe_is_blocked") {
            return;
        }
        let _env = env_lock();
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let external = store
            .create_agent(NewAgent::new("external", "codex"))
            .unwrap();
        let watch = store
            .create_schedule(NewSchedule {
                agent_id: external.id.clone(),
                kind: "watch".into(),
                spec: "github:pr:example-org/project#1".into(),
                prompt: "external".into(),
                interval_ms: None,
                next_fire_at_ms: None,
                watch_baseline: Some("pending".into()),
            })
            .unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let previous = std::env::var_os("ANYAGENTS_GITHUB_API_BASE");
        struct RestoreApi(Option<std::ffi::OsString>);
        impl Drop for RestoreApi {
            fn drop(&mut self) {
                // SAFETY: the server fixture and its external worker have stopped.
                unsafe {
                    if let Some(value) = &self.0 {
                        std::env::set_var("ANYAGENTS_GITHUB_API_BASE", value);
                    } else {
                        std::env::remove_var("ANYAGENTS_GITHUB_API_BASE");
                    }
                }
            }
        }
        let _api = RestoreApi(previous);
        // SAFETY: process-global API overrides are serialized by env_lock.
        unsafe {
            std::env::set_var("ANYAGENTS_GITHUB_API_BASE", format!("http://{address}"));
        }
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (entered, mut received) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let peer = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut request = [0; 4096];
            assert!(stream.read(&mut request).unwrap() > 0);
            entered.send(()).unwrap();
            // Keep the real external HTTP request in flight across later local ticks.
            let _ = blocked.recv_timeout(Duration::from_secs(15));
            stream
                .write_all(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                )
                .unwrap();
        });
        struct Release(Option<std::sync::mpsc::Sender<()>>);
        impl Drop for Release {
            fn drop(&mut self) {
                let _ = self.0.take().unwrap().send(());
            }
        }
        let release = Release(Some(release));
        runtime.block_on(async {
            let (local, network) = start_schedule_loops(Arc::clone(&store), HealthBoard::default());
            let deadline = Instant::now() + Duration::from_secs(8);
            loop {
                match received.try_recv() {
                    Ok(()) => break,
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
                    Err(err) => panic!("external probe did not start: {err}"),
                }
                assert!(
                    Instant::now() < deadline,
                    "external loop did not start on its first tick"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            // These schedules did not exist when the first local pass ran.
            let target = store
                .create_agent(NewAgent::new("idle-target", "codex"))
                .unwrap();
            let mut child = std::process::Command::new("/bin/sh")
                .args(["-c", "exit 0"])
                .spawn()
                .unwrap();
            let pid = child.id();
            assert!(child.wait().unwrap().success());
            let file = dir.path().join("completion");
            let due = now_ms() + 100;
            let mut owners = Vec::new();
            for (name, kind, spec) in [
                ("at-owner", "at", "due".into()),
                ("every-owner", "every", "1s".into()),
                ("file-owner", "watch", format!("file:{}", file.display())),
                ("pid-owner", "watch", format!("pid:{pid}")),
                ("idle-owner", "watch", format!("agent:idle:{}", target.name)),
            ] {
                let owner = store.create_agent(NewAgent::new(name, "codex")).unwrap();
                store
                    .create_schedule(NewSchedule {
                        agent_id: owner.id.clone(),
                        kind: kind.into(),
                        spec,
                        prompt: name.into(),
                        interval_ms: (kind == "every").then_some(1000),
                        next_fire_at_ms: (kind != "watch").then_some(due),
                        watch_baseline: (name == "file-owner").then(|| "missing".into()),
                    })
                    .unwrap();
                owners.push(owner.id);
            }
            std::fs::write(file, "complete").unwrap();
            let future = store
                .create_agent(NewAgent::new("future-owner", "codex"))
                .unwrap();
            store
                .create_schedule(NewSchedule {
                    agent_id: future.id.clone(),
                    kind: "at".into(),
                    spec: "future".into(),
                    prompt: "not due".into(),
                    interval_ms: None,
                    next_fire_at_ms: Some(due + 60_000),
                    watch_baseline: None,
                })
                .unwrap();
            crate::schedule::schedules_changed(store.data_dir());
            loop {
                if owners
                    .iter()
                    .all(|id| store.inbox(id, None).unwrap().len() == 1)
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "local schedules waited for external probe"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(store.inbox(&future.id, None).unwrap().is_empty());
            assert!(store.inbox(&external.id, None).unwrap().is_empty());
            let active = store.list_schedules(Some(&external.id), true).unwrap();
            assert_eq!(active[0].id, watch.id);
            assert_eq!(active[0].last_observed, watch.last_observed);
            // All five admissions are proven before releasing the external response.
            local.abort();
            network.abort();
        });
        drop(release);
        peer.join().unwrap();
        runtime.shutdown_timeout(Duration::from_secs(5));
    }

    struct SingleUidEnvironment(Vec<(&'static str, Option<std::ffi::OsString>)>);

    impl SingleUidEnvironment {
        /// Call only while env_lock is held, before starting the fixture server.
        fn new() -> Self {
            let keys = [
                "ANYAGENTS_PRINCIPALS",
                "ANYAGENTS_PRINCIPAL_REGISTRY",
                "ANYAGENTS_AGENTS",
                "ANYAGENTS_AGENT_UID",
                "ANYAGENTS_AGENT_GID",
                "ANYAGENTS_SOCKET_GID",
            ];
            let saved = keys
                .iter()
                .map(|key| (*key, std::env::var_os(key)))
                .collect();
            for key in keys {
                // SAFETY: these server fixtures hold env_lock through shutdown.
                unsafe { std::env::remove_var(key) };
            }
            assert!(!crate::sandbox::seats_configured());
            assert!(crate::sandbox::launch_boundary(&[]));
            Self(saved)
        }
    }

    impl Drop for SingleUidEnvironment {
        fn drop(&mut self) {
            for (key, value) in &self.0 {
                // SAFETY: the fixture server has stopped and env_lock is held.
                unsafe {
                    if let Some(value) = value {
                        std::env::set_var(key, value);
                    } else {
                        std::env::remove_var(key);
                    }
                }
            }
        }
    }

    #[test]
    fn readiness_answers_while_the_once_only_image_hash_is_blocked() {
        if single_test_worker("readiness_answers_while_the_once_only_image_hash_is_blocked") {
            return;
        }
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::{Duration, Instant};
        let _env = env_lock();
        let _single_uid = SingleUidEnvironment::new();
        let data = tempfile::tempdir().unwrap();
        let token = data.path().join("operator-token");
        std::fs::write(&token, "an-operator-token-0123456789abcdef\n").unwrap();
        let settings = [
            (
                "ANYAGENTS_OPERATOR_TOKEN_FILE",
                token.to_string_lossy().into_owned(),
            ),
            ("ANYAGENTS_FAKE_MODE", "1".into()),
        ];
        struct Restore(Vec<(&'static str, Option<std::ffi::OsString>)>);
        impl Drop for Restore {
            fn drop(&mut self) {
                for (key, value) in &self.0 {
                    // SAFETY: this fixture holds env_lock until its server stops.
                    unsafe {
                        if let Some(value) = value {
                            std::env::set_var(key, value);
                        } else {
                            std::env::remove_var(key);
                        }
                    }
                }
            }
        }
        let _restore = Restore(
            settings
                .iter()
                .map(|(key, _)| (*key, std::env::var_os(key)))
                .collect(),
        );
        for (key, value) in settings {
            // SAFETY: every server fixture uses env_lock.
            unsafe {
                std::env::set_var(key, value);
            }
        }
        let image = data.path().join("image");
        std::fs::write(&image, b"abc").unwrap();
        let (release, blocked) = std::sync::mpsc::channel();
        let (entered, running) = std::sync::mpsc::channel();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let identity = Arc::new(ServingIdentity::with_hasher(move || {
            use sha2::{Digest, Sha256};
            observed.fetch_add(1, Ordering::SeqCst);
            entered.send(()).unwrap();
            blocked.recv().unwrap();
            Ok(format!("{:x}", Sha256::digest(std::fs::read(image)?)))
        }));
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let path = data.path().to_path_buf();
        let serving = Arc::clone(&identity);
        let server = std::thread::spawn(move || {
            run_with_identity(
                &path,
                Arc::new(Mutex::new(ProcessRunner)),
                stopped,
                serving,
                HarnessHost::InProcess,
            )
        });
        struct Stop(
            Option<tokio::sync::oneshot::Sender<()>>,
            Option<std::thread::JoinHandle<Result<(), Error>>>,
        );
        impl Drop for Stop {
            fn drop(&mut self) {
                let _ = self.0.take().unwrap().send(());
                self.1.take().unwrap().join().unwrap().unwrap();
            }
        }
        let _server = Stop(Some(stop), Some(server));
        let deadline = Instant::now() + Duration::from_secs(5);
        let socket = socket_path(data.path());
        loop {
            if std::os::unix::net::UnixStream::connect(&socket).is_ok() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "readiness waited for the image hash"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        running.recv_timeout(Duration::from_secs(5)).unwrap();
        let health = || {
            let mut stream = std::os::unix::net::UnixStream::connect(&socket).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            write!(
                stream,
                "GET /v1/webhooks/healthz HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            assert!(response.starts_with("HTTP/1.1 200"), "{response}");
            serde_json::from_str::<serde_json::Value>(response.split_once("\r\n\r\n").unwrap().1)
                .unwrap()
        };
        let computing = health();
        assert_eq!(computing["binary_sha256_state"], "computing");
        assert!(computing["binary_sha256"].is_null());
        assert_eq!(computing["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(computing["source_sha"], env!("ANYAGENTS_SOURCE_SHA"));
        identity.start_hash();
        identity.clone().start_hash();
        release.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let ready = health();
            if ready["binary_sha256_state"] == "ready" {
                // SHA-256 of the exact bytes written to the fixture image.
                assert_eq!(
                    ready["binary_sha256"],
                    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
                );
                break;
            }
            assert!(Instant::now() < deadline, "{ready}");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_sign_in_streams_the_link_before_the_outcome() {
        if single_test_worker("a_sign_in_streams_the_link_before_the_outcome") {
            return;
        }
        let _env = env_lock();
        let _single_uid = SingleUidEnvironment::new();
        let data = tempfile::tempdir().unwrap();
        let socket = socket_path(data.path());
        let dir = Arc::new(Mutex::new(None));
        let runner = Arc::new(Mutex::new(Scripted {
            dir: Arc::clone(&dir),
        }));
        let data_path = data.path().to_path_buf();
        // This deployment has an operator, so the sign-in presents one.
        let token_file = data.path().join("operator-token");
        std::fs::write(&token_file, "an-operator-token-0123456789abcdef\n").unwrap();
        // SAFETY: the test process sets these before the server thread reads
        // them, and sets them for its own duration only.
        unsafe {
            std::env::set_var("ANYAGENTS_OPERATOR_TOKEN_FILE", &token_file);
            // Authentication is independent of optional multi-UID seats.
            std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
        }
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let started = Arc::new(Mutex::new(None));
        let outcome = Arc::clone(&started);
        let server = std::thread::spawn(move || {
            let result = run_with(&data_path, runner, stop_rx);
            if let Err(err) = &result {
                *outcome.lock().unwrap_or_else(|e| e.into_inner()) = Some(err.to_string());
            }
            result
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !socket.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(
            socket.exists(),
            "the server did not start: {:?}",
            started.lock().unwrap_or_else(|e| e.into_inner())
        );
        let mut stream = std::os::unix::net::UnixStream::connect(&socket).unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        stream
            .write_all(
                format!(
                    "POST /v1/harnesses/codex/sign-in HTTP/1.1\r\nHost: local\r\n{}: an-operator-token-0123456789abcdef\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    crate::operator::HEADER
                )
                .as_bytes(),
            )
            .unwrap();
        let mut buf = String::new();
        let mut chunk = [0u8; 256];
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline && !buf.contains("\"kind\":\"done\"") {
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => buf.push_str(&String::from_utf8_lossy(&chunk[..n])),
                Err(_) => break,
            }
        }
        let link = buf.find("\"kind\":\"link\"").expect(&buf);
        let done = buf.find("\"kind\":\"done\"").expect(&buf);
        assert!(link < done, "{buf}");
        assert!(buf.contains("https://auth.example/device"), "{buf}");
        assert!(buf.contains("ada@example"), "{buf}");
        let recorded = dir.lock().unwrap().clone().unwrap();
        assert!(recorded.ends_with("config/codex/default"), "{recorded:?}");
        drop(stream);
        let _ = stop_tx.send(());
        let _ = server.join();
        // SAFETY: as above, for this test only.
        unsafe {
            std::env::remove_var("ANYAGENTS_OPERATOR_TOKEN_FILE");
            std::env::remove_var("ANYAGENTS_AGENT_UID");
            std::env::remove_var("ANYAGENTS_AGENT_GID");
            std::env::remove_var("ANYAGENTS_SOCKET_GID");
            std::env::remove_var("ANYAGENTS_FAKE_MODE");
            std::env::remove_var("ANYAGENTS_LAUNCHER");
        }
    }

    /// A sign-in call from an agent's own capability is refused, and no vendor
    /// command runs.
    #[test]
    fn an_agent_capability_cannot_sign_a_harness_in() {
        if single_test_worker("an_agent_capability_cannot_sign_a_harness_in") {
            return;
        }
        let _env = env_lock();
        let _single_uid = SingleUidEnvironment::new();
        let data = tempfile::tempdir().unwrap();
        let socket = socket_path(data.path());
        let dir = Arc::new(Mutex::new(None));
        let runner = Arc::new(Mutex::new(Scripted {
            dir: Arc::clone(&dir),
        }));
        let data_path = data.path().to_path_buf();
        let token_file = data.path().join("operator-token");
        std::fs::write(&token_file, "an-operator-token-0123456789abcdef\n").unwrap();
        // SAFETY: the test process sets these before the server thread reads
        // them, and sets them for its own duration only.
        unsafe {
            std::env::set_var("ANYAGENTS_OPERATOR_TOKEN_FILE", &token_file);
            std::env::set_var(
                "ANYAGENTS_CAPABILITY_KEY",
                "0123456789abcdef0123456789abcdef",
            );
        }
        // SAFETY: as above, for this test's own duration under the lock.
        unsafe {
            std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
        }
        let store = Store::open(data.path().join("state.db")).unwrap();
        let configured = "0123456789abcdef0123456789abcdef";
        store.hold_capability_key(configured).unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("ada", "codex"))
            .unwrap();
        drop(store);
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let started = Arc::new(Mutex::new(None));
        let outcome = Arc::clone(&started);
        let server = std::thread::spawn(move || {
            let result = run_with(&data_path, runner, stop_rx);
            if let Err(err) = &result {
                *outcome.lock().unwrap_or_else(|e| e.into_inner()) = Some(err.to_string());
            }
            result
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !socket.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(
            socket.exists(),
            "the server did not start: {:?}",
            started.lock().unwrap_or_else(|e| e.into_inner())
        );
        // The server mints each agent's capability when it starts, so this test
        // reads the one it left and the server recognises.
        let capability = std::fs::read_to_string(
            data.path()
                .join("agents")
                .join(agent.id.as_str())
                .join("config")
                .join(crate::operator::CAPABILITY_FILE),
        )
        .unwrap()
        .trim()
        .to_string();
        let mut stream = std::os::unix::net::UnixStream::connect(&socket).unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        stream
            .write_all(
                format!(
                    "POST /v1/harnesses/codex/sign-in HTTP/1.1\r\nHost: local\r\n{}: {capability}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    crate::operator::AGENT_HEADER
                )
                .as_bytes(),
            )
            .unwrap();
        let mut buf = String::new();
        let _ = stream.read_to_string(&mut buf);
        assert!(buf.starts_with("HTTP/1.1 403"), "{buf}");
        assert!(buf.contains("cannot administer"), "{buf}");
        assert!(dir.lock().unwrap().is_none(), "the vendor command ran");
        drop(stream);
        let _ = stop_tx.send(());
        let _ = server.join();
        // SAFETY: as above, under `ENV`.
        unsafe {
            std::env::remove_var("ANYAGENTS_OPERATOR_TOKEN_FILE");
            std::env::remove_var("ANYAGENTS_CAPABILITY_KEY");
            std::env::remove_var("ANYAGENTS_AGENT_UID");
            std::env::remove_var("ANYAGENTS_AGENT_GID");
            std::env::remove_var("ANYAGENTS_SOCKET_GID");
            std::env::remove_var("ANYAGENTS_FAKE_MODE");
            std::env::remove_var("ANYAGENTS_LAUNCHER");
        }
    }

    fn archived_reap_fixture(store: &Store, name: &str, parent: &Agent) -> Agent {
        let mut new = anyagents_core::NewAgent::new(name, "opencode");
        new.parent_id = Some(parent.id.clone());
        let child = store.create_agent(new).unwrap();
        let _lifecycle = store.lock_agent_lifecycle(&child.id).unwrap();
        let child = store.set_agent_state(&child.id, "archived").unwrap();
        mark_archive_generation_locked(store, store.data_dir(), &child);
        child
    }

    #[test]
    fn archive_reap_marker_survives_restart_and_is_invalidated_by_wake_and_rearchive() {
        if single_test_worker(
            "archive_reap_marker_survives_restart_and_is_invalidated_by_wake_and_rearchive",
        ) {
            return;
        }
        let _env = env_lock();
        unsafe {
            std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
        }
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");
        let store = Store::open(&db).unwrap();
        let parent = store
            .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
            .unwrap();
        let child = archived_reap_fixture(&store, "worker", &parent);
        let cfg = anyagents_core::agent_config_dir(dir.path(), &child.id);
        let generation = std::fs::read(cfg.join(".anyagents-archived")).unwrap();
        std::fs::write(cfg.join(".anyagents-reaped"), b"foreign generation").unwrap();
        std::fs::create_dir_all(cfg.join(".claude")).unwrap();
        reap_archived_agents(&store, dir.path(), &HealthBoard::default());
        assert!(!cfg.join(".claude").exists());
        assert_eq!(
            std::fs::read(cfg.join(".anyagents-reaped")).unwrap(),
            generation
        );
        // A fresh process must use durable completion, not rescan retained dirs.
        std::fs::create_dir_all(cfg.join("shell-snapshots")).unwrap();
        let restarted = Store::open(&db).unwrap();
        reap_archived_agents(&restarted, dir.path(), &HealthBoard::default());
        assert!(cfg.join("shell-snapshots").exists());
        let stale_child = restarted.agent(&child.id).unwrap();
        authored_send(
            &store,
            &Actor::Agent(parent.name.clone()),
            "worker",
            "continue",
            "wake-reaped",
        );
        assert!(!cfg.join(".anyagents-reaped").exists());
        assert!(!cfg.join(".anyagents-archived").exists());
        {
            let _lifecycle = restarted.lock_agent_lifecycle(&stale_child.id).unwrap();
            reap_archived_agent_locked(&restarted, dir.path(), &stale_child.id);
        }
        assert!(
            cfg.join("shell-snapshots").exists(),
            "stale cleanup deleted running config"
        );
        assert_eq!(store.agent(&child.id).unwrap().state, "running");
        mark_explicitly_archived(&store, dir.path(), &child);
        assert_ne!(
            std::fs::read(cfg.join(".anyagents-archived")).unwrap(),
            generation
        );
        assert!(!cfg.join(".anyagents-reaped").exists());
        reap_archived_agents(&store, dir.path(), &HealthBoard::default());
        assert!(!cfg.join("shell-snapshots").exists());
        assert_eq!(
            std::fs::read(cfg.join(".anyagents-reaped")).unwrap(),
            std::fs::read(cfg.join(".anyagents-archived")).unwrap()
        );
        unsafe {
            std::env::remove_var("ANYAGENTS_FAKE_MODE");
        }
    }

    #[test]
    fn archive_reap_repairs_empty_legacy_marker_and_retries_partial_failure() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let parent = store
            .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
            .unwrap();
        let child = archived_reap_fixture(&store, "worker", &parent);
        let other = archived_reap_fixture(&store, "other", &parent);
        let cfg = anyagents_core::agent_config_dir(dir.path(), &child.id);
        let other_cfg = anyagents_core::agent_config_dir(dir.path(), &other.id);
        std::fs::write(cfg.join(".anyagents-archived"), b"").unwrap();
        let blocked = cfg.join(".cache");
        std::fs::create_dir_all(&blocked).unwrap();
        std::fs::write(blocked.join("keep"), b"partial cleanup").unwrap();
        std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o000)).unwrap();
        let failed_at = now_ms();
        reap_archived_agents_at(&store, dir.path(), &HealthBoard::default(), failed_at);
        // Restore access before asserting so failed tests don't leak fixtures.
        std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(
            !std::fs::read(cfg.join(".anyagents-archived"))
                .unwrap()
                .is_empty()
        );
        assert!(
            !cfg.join(".anyagents-reaped").exists(),
            "failed cleanup was marked complete"
        );
        assert!(
            other_cfg.join(".anyagents-reaped").exists(),
            "failure starved an independent archive"
        );
        // A failed cleanup waits out its backoff instead of the next minute.
        reap_archived_agents_at(
            &store,
            dir.path(),
            &HealthBoard::default(),
            failed_at + 30_000,
        );
        assert!(blocked.exists(), "retried before its backoff");
        assert!(!cfg.join(".anyagents-reaped").exists());
        reap_archived_agents_at(
            &store,
            dir.path(),
            &HealthBoard::default(),
            now_ms() + 60_000,
        );
        assert!(!blocked.exists());
        assert_eq!(
            std::fs::read(cfg.join(".anyagents-reaped")).unwrap(),
            std::fs::read(cfg.join(".anyagents-archived")).unwrap()
        );
    }

    #[test]
    fn reap_queue_backfills_once_and_idle_maintenance_visits_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let parent = store
            .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
            .unwrap();
        // Finished under an earlier binary: its marker matches its archive.
        let finished = archived_reap_fixture(&store, "finished", &parent);
        let finished_cfg = anyagents_core::agent_config_dir(dir.path(), &finished.id);
        std::fs::copy(
            finished_cfg.join(".anyagents-archived"),
            finished_cfg.join(".anyagents-reaped"),
        )
        .unwrap();
        // Archived by an earlier binary that wrote no marker at all.
        let mut new = anyagents_core::NewAgent::new("legacy", "opencode");
        new.parent_id = Some(parent.id.clone());
        let legacy = store.create_agent(new).unwrap();
        store.set_agent_state(&legacy.id, "archived").unwrap();
        let legacy_cfg = anyagents_core::agent_config_dir(dir.path(), &legacy.id);
        // Forget what this binary queued, as if the archives predated the queue.
        std::fs::remove_dir_all(dir.path().join("reap-pending")).unwrap();
        for cfg in [&finished_cfg, &legacy_cfg] {
            std::fs::create_dir_all(cfg.join(".claude")).unwrap();
        }

        reap_archived_agents(&store, dir.path(), &HealthBoard::default());
        assert!(
            finished_cfg.join(".claude").exists(),
            "finished archive revisited"
        );
        assert!(
            !legacy_cfg.join(".claude").exists(),
            "legacy archive not backfilled"
        );
        assert_eq!(
            std::fs::read(legacy_cfg.join(".anyagents-reaped")).unwrap(),
            std::fs::read(legacy_cfg.join(".anyagents-archived")).unwrap()
        );

        // Nothing is owed now, so maintenance visits no agent at all.
        std::fs::create_dir_all(legacy_cfg.join(".claude")).unwrap();
        std::fs::remove_file(finished_cfg.join(".anyagents-reaped")).unwrap();
        reap_archived_agents(&store, dir.path(), &HealthBoard::default());
        assert!(legacy_cfg.join(".claude").exists());
        assert!(finished_cfg.join(".claude").exists());
        assert!(reap_queue::due(dir.path(), i64::MAX).is_empty());
    }

    #[test]
    fn roots_and_role_bearing_children_can_self_archive_without_losing_browser_profiles() {
        let _env = env_lock();
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let parent = store
            .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
            .unwrap();
        let state = AppState {
            store: Arc::clone(&store),
            health: HealthBoard::default(),
            sign_in: Arc::new(Mutex::new(ProcessRunner)),
            identity: Arc::new(ServingIdentity::capture()),
            profiles: Default::default(),
            auth: auth::Mode::Trusted {
                data_dir: dir.path().into(),
            },
        };
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            for (name, parent_id, role) in [
                ("root", None, Some("Researcher")),
                (
                    "labelled-child",
                    Some(parent.id.clone()),
                    Some("Researcher"),
                ),
                ("plain-child", Some(parent.id.clone()), None),
            ] {
                let mut new = anyagents_core::NewAgent::new(name, "codex");
                new.parent_id = parent_id;
                new.role = role.map(str::to_string);
                let agent = store.create_agent(new).unwrap();
                let profile = dir.path().join("browser-profiles").join(name);
                std::fs::create_dir_all(&profile).unwrap();
                std::fs::write(profile.join("login"), b"preserved session").unwrap();
                let caller = Principal::Agent {
                    agent_id: agent.id.clone(),
                    name: name.into(),
                };
                let Json(archived) =
                    archive_agent_route(caller, State(state.clone()), AxumPath("self".into()))
                        .await
                        .unwrap_or_else(|e| panic!("{}: {}", e.status, e.message));
                assert_eq!(archived.state, "archived");
                assert_eq!(store.agent(&agent.id).unwrap().role, agent.role);
                assert_eq!(
                    std::fs::read(profile.join("login")).unwrap(),
                    b"preserved session"
                );
                let _ = delete_agent_route(
                    Principal::Operator,
                    State(state.clone()),
                    AxumPath(name.into()),
                )
                .await
                .unwrap_or_else(|e| panic!("{}: {}", e.status, e.message));
                assert!(!profile.exists(), "delete must remove the profile");
            }
        });
    }

    #[test]
    fn archive_reap_waits_for_self_archived_process_to_stop() {
        if single_test_worker("archive_reap_waits_for_self_archived_process_to_stop") {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let parent = store
            .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
            .unwrap();
        let child = archived_reap_fixture(&store, "worker", &parent);
        let cfg = anyagents_core::agent_config_dir(dir.path(), &child.id);
        std::fs::create_dir_all(cfg.join(".claude")).unwrap();
        let health = HealthBoard::default();
        let mut live = anyagents_supervisor::LiveState {
            process_alive: true,
            turn_state: anyagents_harness::TurnState::Idle,
            delivery: Some(crate::vendor::declared_delivery(&child.harness)),
            quota_reset_at_ms: None,
            restart_at_ms: None,
            pid: None,
            live_harness: Some(child.harness.clone()),
            live_model: None,
            process_started_at_ms: None,
            last_error: None,
        };
        health.publish(&child.id, live.clone());
        reap_archived_agents(&store, dir.path(), &health);
        assert!(cfg.join(".claude").exists());
        assert!(!cfg.join(".anyagents-reaped").exists());
        live.process_alive = false;
        health.publish(&child.id, live);
        reap_archived_agents(&store, dir.path(), &health);
        assert!(!cfg.join(".claude").exists());
        assert!(cfg.join(".anyagents-reaped").exists());
    }

    #[test]
    fn archive_reap_failed_protection_read_preserves_work_and_delete_still_cleans() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let parent = store
            .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
            .unwrap();
        let child = archived_reap_fixture(&store, "worker", &parent);
        let work = anyagents_core::agent_work_dir(dir.path(), &child.id);
        std::fs::create_dir_all(&work).unwrap();
        std::fs::write(work.join("keep"), b"protected").unwrap();
        // Simulate an unreadable/corrupt protection database, not an empty list.
        let failed = tempfile::tempdir().unwrap();
        let failed_work = anyagents_core::agent_work_dir(failed.path(), &child.id);
        std::fs::create_dir_all(&failed_work).unwrap();
        std::fs::write(failed_work.join("keep"), b"unknown references").unwrap();
        std::fs::write(failed.path().join("state.db"), b"not a database").unwrap();
        assert!(!reap_subagent_ephemeral_dirs(failed.path(), &child.id));
        assert!(failed_work.join("keep").exists());
        // A live sibling sharing this workdir protects it. Deleting the target
        // row still invokes raw cleanup, but must retain surviving references.
        let sibling = store
            .create_agent(anyagents_core::NewAgent::new("sibling", "opencode"))
            .unwrap();
        store
            .set_agent_running(&sibling.id, work.to_str().unwrap())
            .unwrap();
        store.delete_agent(&child.id).unwrap();
        assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
        assert!(work.join("keep").exists());
        store.delete_agent(&sibling.id).unwrap();
        assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
        assert!(!work.exists());
    }

    #[test]
    fn recorded_cleanup_preserves_unpushed_head_and_other_branch_then_removes_pushed() {
        use crate::worktree::tests::git;
        fn snapshot(root: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
            fn walk(
                root: &Path,
                path: &Path,
                out: &mut std::collections::BTreeMap<PathBuf, Vec<u8>>,
            ) {
                for entry in std::fs::read_dir(path).unwrap() {
                    let path = entry.unwrap().path();
                    let meta = std::fs::symlink_metadata(&path).unwrap();
                    if meta.is_dir() {
                        walk(root, &path, out);
                    } else if meta.is_file() {
                        out.insert(
                            path.strip_prefix(root).unwrap().to_path_buf(),
                            std::fs::read(&path).unwrap(),
                        );
                    } else {
                        panic!("unexpected fixture object: {}", path.display());
                    }
                }
            }
            let mut out = std::collections::BTreeMap::new();
            walk(root, root, &mut out);
            out
        }
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let parent = store
            .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
            .unwrap();
        let child = archived_reap_fixture(&store, "worker", &parent);
        let work = anyagents_core::agent_work_dir(dir.path(), &child.id);
        let fixture = crate::worktree::tests::Fixture::new(&dir.path().join("fixture"));
        fixture
            .consumer
            .remote("SylphxAI/demo", &work, "worker", None)
            .unwrap();
        crate::worktree_access::set_worktree_identity(&work).unwrap();
        crate::worktree_access::share(
            &work,
            crate::sandbox::server_uid(),
            crate::sandbox::server_gid(),
            &crate::worktree_access::grant_path(dir.path(), &child.id),
        )
        .unwrap();
        let recorded =
            crate::worktree_access::RecordedWorktree::load(dir.path(), &child.id, &work).unwrap();
        assert!(
            is_git_worktree_safe_to_reap(dir.path(), &child.id, &work),
            "published baseline must be eligible"
        );
        std::fs::write(work.join("src/unique"), b"unique committed source\n").unwrap();
        git(&work, &["add", "src/unique"]);
        git(&work, &["commit", "-m", "Unpushed source"]);
        // A child-controlled marker must not turn an unpushed HEAD into a GC proof.
        let cfg = anyagents_core::agent_config_dir(dir.path(), &child.id);
        std::fs::write(cfg.join(".anyagents-managed-worktree.json"), serde_json::to_vec(&serde_json::json!({"path": work, "initial_head": git(&work, &["rev-parse", "HEAD"])})).unwrap()).unwrap();
        let before_work = snapshot(&work);
        let before_private = snapshot(recorded.private());
        assert!(!is_git_worktree_safe_to_reap(dir.path(), &child.id, &work));
        assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
        assert_eq!(snapshot(&work), before_work);
        assert_eq!(snapshot(recorded.private()), before_private);
        let dotgit = std::fs::read(work.join(".git")).unwrap();
        for pointer in [None, Some(b"gitdir: /nonexistent/broken\n".as_slice())] {
            match pointer {
                None => std::fs::remove_file(work.join(".git")).unwrap(),
                Some(bytes) => std::fs::write(work.join(".git"), bytes).unwrap(),
            }
            std::fs::create_dir_all(work.join("target")).unwrap();
            std::fs::write(work.join("target/uncertain-report"), b"retain every byte\n").unwrap();
            let missing_work = snapshot(&work);
            assert!(!is_git_worktree_safe_to_reap(dir.path(), &child.id, &work));
            assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
            assert_eq!(snapshot(&work), missing_work, "lost pointer deleted source");
            assert_eq!(snapshot(recorded.private()), before_private);
            std::fs::remove_dir_all(work.join("target")).unwrap();
            std::fs::write(work.join(".git"), &dotgit).unwrap();
        }
        git(&work, &["push", "origin", "HEAD:refs/heads/worker"]);
        assert!(is_git_worktree_safe_to_reap(dir.path(), &child.id, &work));
        let pushed_head = git(&work, &["rev-parse", "HEAD"]);
        git(&work, &["checkout", "-b", "unpublished-side"]);
        git(
            &work,
            &["commit", "--allow-empty", "-m", "Side branch only"],
        );
        git(&work, &["checkout", "--detach", &pushed_head]);
        let before_work = snapshot(&work);
        let before_private = snapshot(recorded.private());
        assert!(
            !is_git_worktree_safe_to_reap(dir.path(), &child.id, &work),
            "published HEAD does not cover another unpushed local branch"
        );
        assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
        assert_eq!(snapshot(&work), before_work);
        assert_eq!(snapshot(recorded.private()), before_private);
        git(&work, &["push", "origin", "refs/heads/unpublished-side"]);
        assert!(is_git_worktree_safe_to_reap(dir.path(), &child.id, &work));
        assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
        assert!(!work.exists());
        assert!(!recorded.private().exists());
    }

    #[test]
    fn archive_reap_preserves_unpushed_and_external_worktrees() {
        let _env = env_lock();
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let parent = store
            .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
            .unwrap();
        let child = archived_reap_fixture(&store, "worker", &parent);
        let work = anyagents_core::agent_work_dir(dir.path(), &child.id);
        std::fs::create_dir_all(&work).unwrap();
        let git = |args: &[&str]| {
            let output = std::process::Command::new("git")
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .args([
                    "-c",
                    "user.name=Example Author",
                    "-c",
                    "user.email=author@example.org",
                    "-C",
                ])
                .arg(&work)
                .args(args)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "git: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        };
        git(&["init", "-q"]);
        std::fs::write(work.join("source.rs"), b"unique committed source").unwrap();
        std::fs::write(work.join(".gitignore"), b"/target/\n").unwrap();
        git(&["add", "source.rs", ".gitignore"]);
        git(&["commit", "-qm", "Initial source"]);
        std::fs::create_dir_all(work.join("target/debug")).unwrap();
        // A clean commit with no remote is not safe to delete.
        assert!(!is_git_worktree_safe_to_reap(dir.path(), &child.id, &work,));
        reap_archived_agents(&store, dir.path(), &HealthBoard::default());
        assert!(work.join("source.rs").exists());
        assert!(work.join(".git").exists());
        assert!(!work.join("target").exists());
        let cfg = anyagents_core::agent_config_dir(dir.path(), &child.id);
        assert!(
            cfg.join(".anyagents-reaped").exists(),
            "retained source is not pending cleanup"
        );
        let external = dir.path().join("borrowed-worktree");
        std::fs::create_dir_all(external.join("target/debug")).unwrap();
        std::fs::write(external.join("source.rs"), b"borrowed source").unwrap();
        std::fs::write(
            cfg.join(".anyagents-managed-worktree.json"),
            serde_json::to_vec(&serde_json::json!({"path": external})).unwrap(),
        )
        .unwrap();
        assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
        assert!(external.join("source.rs").exists());
        assert!(external.join("target/debug").exists());
    }

    #[test]
    fn archive_reap_blocked_in_git_does_not_delay_other_mail() {
        if single_test_worker("archive_reap_blocked_in_git_does_not_delay_other_mail") {
            return;
        }
        let _env = env_lock();
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");
        let store = Store::open(&db).unwrap();
        let parent = store
            .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
            .unwrap();
        let child = archived_reap_fixture(&store, "slow-worker", &parent);
        let work = anyagents_core::agent_work_dir(dir.path(), &child.id);
        let fixture = crate::worktree::tests::Fixture::new(&dir.path().join("fixture"));
        fixture
            .consumer
            .remote("SylphxAI/demo", &work, "worker", None)
            .unwrap();
        crate::worktree_access::set_worktree_identity(&work).unwrap();
        crate::worktree_access::share(
            &work,
            crate::sandbox::server_uid(),
            crate::sandbox::server_gid(),
            &crate::worktree_access::grant_path(dir.path(), &child.id),
        )
        .unwrap();
        let recipient = store
            .create_agent(anyagents_core::NewAgent::new("recipient", "opencode"))
            .unwrap();
        store
            .receive(&recipient.id, anyagents_core::Incoming::new("pending mail"))
            .unwrap();
        store.set_agent_state(&recipient.id, "running").unwrap();
        let entered = dir.path().join("git-entered");
        let release = dir.path().join("git-release");
        let bin = dir.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        // Delegate to the same Git that prepared the fixture (Homebrew on
        // macOS), preserving all arguments and stdin for validation probes.
        let real_git = std::process::Command::new("/bin/sh")
            .args(["-c", "command -v git"])
            .output()
            .unwrap();
        assert!(real_git.status.success());
        let real_git = String::from_utf8(real_git.stdout).unwrap();
        let real_git = real_git.trim();
        assert!(Path::new(real_git).is_absolute());
        let git = bin.join("git");
        crate::worktree::tests::write_executable(
            &git,
            &format!(
                r#"#!/bin/sh
# Parse in a subshell so non-barrier calls retain the original argv.
if (
    while [ "$#" -gt 0 ]; do
        case "$1" in
            -c|--git-dir) shift 2 ;;
            --no-optional-locks) shift ;;
            --work-tree|-C) cwd="$2"; shift 2 ;;
            *) break ;;
        esac
    done
    [ "$cwd" = '{}' ] && [ "$1" = status ]
); then
    touch '{}'
    n=0
    while [ ! -e '{}' ] && [ "$n" -lt 1000 ]; do sleep 0.01; n=$((n+1)); done
    exit 1
fi
exec '{}' "$@"
"#,
                work.display(),
                entered.display(),
                release.display(),
                real_git
            ),
        );
        let previous_path = std::env::var_os("PATH");
        let path =
            std::env::join_paths([bin, PathBuf::from("/usr/bin"), PathBuf::from("/bin")]).unwrap();
        unsafe {
            std::env::set_var("PATH", path);
        }
        // Calibration must reach the production validator through the fake
        // Git, not silently refuse before the status barrier under test.
        crate::worktree_access::RecordedWorktree::load(dir.path(), &child.id, &work)
            .unwrap()
            .validate()
            .expect("barrier Git cannot validate the prepared checkout");
        let worker_store = Store::open(&db).unwrap();
        let worker_data = dir.path().to_path_buf();
        let worker = std::thread::spawn(move || {
            reap_archived_agents(&worker_store, &worker_data, &HealthBoard::default())
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !entered.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let was_entered = entered.exists();
        let owns_lifecycle = matches!(
            store.try_lock_agent_lifecycle(&child.id),
            Err(Error::AgentBusy)
        );
        let rec_store = Store::open(&db).unwrap();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let scanner = std::thread::spawn(move || {
            authored_send(
                &rec_store,
                &Actor::Operator("operator".into()),
                "recipient",
                "unrelated mail",
                "during-reap",
            );
            done_tx.send(()).unwrap();
        });
        let progressed_while_blocked = done_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .is_ok();
        let recipient_running = store.agent(&recipient.id).unwrap().state == "running";
        std::fs::write(&release, b"release").unwrap();
        worker.join().unwrap();
        scanner.join().unwrap();
        unsafe {
            if let Some(path) = previous_path {
                std::env::set_var("PATH", path);
            } else {
                std::env::remove_var("PATH");
            }
        }
        assert!(was_entered, "reaper never reached the git barrier");
        assert!(
            owns_lifecycle,
            "barrier did not observe reaper lifecycle ownership"
        );
        assert!(
            progressed_while_blocked && recipient_running,
            "unrelated mail delivery waited for blocked cleanup"
        );
    }

    #[test]
    fn reap_subagent_ephemeral_dirs_removes_gemini_tmp_and_dirty_worktree_build_outputs() {
        let data = tempfile::tempdir().unwrap();
        let agent_id = anyagents_core::AgentId::generate();
        let agent_root = data.path().join("agents").join(agent_id.as_str());
        let config_dir = agent_root.join("config");
        let work_dir = agent_root.join("work");
        std::fs::create_dir_all(config_dir.join(".gemini/antigravity-cli/brain")).unwrap();
        std::fs::write(
            config_dir.join(".gemini/antigravity-cli/brain/transcript.jsonl"),
            b"large transcript",
        )
        .unwrap();
        std::fs::create_dir_all(config_dir.join(".cache/sub")).unwrap();
        std::fs::create_dir_all(agent_root.join("tmp/sub")).unwrap();
        std::fs::create_dir_all(work_dir.join(".git")).unwrap();
        std::fs::create_dir_all(work_dir.join("target/debug")).unwrap();
        std::fs::create_dir_all(work_dir.join("apps/web/node_modules/pkg")).unwrap();
        std::fs::write(work_dir.join("dirty.rs"), b"fn main() {}").unwrap();

        reap_subagent_ephemeral_dirs(data.path(), &agent_id);

        assert!(!config_dir.join(".gemini").exists());
        assert!(!config_dir.join(".cache").exists());
        assert!(!agent_root.join("tmp").exists());
        // Dirty worktree is preserved, while heavy build outputs inside it are reaped:
        assert!(work_dir.join("dirty.rs").exists());
        assert!(!work_dir.join("target").exists());
        assert!(!work_dir.join("apps/web/node_modules").exists());
    }
    fn mark_explicitly_archived(store: &Store, data_dir: &Path, agent: &Agent) {
        let Ok(_lifecycle) = store.lock_agent_lifecycle(&agent.id) else {
            return;
        };
        let _ = store.set_agent_state(&agent.id, "archived");
        mark_explicitly_archived_locked(store, data_dir, agent).unwrap();
    }

    fn authored_send(
        store: &Store,
        actor: &Actor,
        to: &str,
        text: &str,
        key: &str,
    ) -> SendResponse {
        let body = SendRequest {
            sender: Some(actor.as_address()),
            to: vec![to.to_string()],
            text: text.to_string(),
            idempotency_key: Some(key.to_string()),
            ..Default::default()
        };
        let key = send_key(&body);
        let plan = SendPlan::parse(store, actor, &body).unwrap_or_else(|e| panic!("{}", e.message));
        send_sync(store, actor, body, plan, key)
            .unwrap_or_else(|e| panic!("{}", e.message))
            .0
    }

    /// A send that cannot wake an archived recipient (through an alias, or to
    /// several recipients) is refused for it with who answers for it, and
    /// nothing is stored unread. A send with no live recipient fails.
    #[test]
    fn a_send_an_archived_agent_would_never_read_is_refused_with_its_parent() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let root = store
            .create_agent(anyagents_core::NewAgent::new("root", "opencode"))
            .unwrap();
        // Running, as an agent that answers for its archived child.
        store.set_agent_state(&root.id, "running").unwrap();
        let mut new = anyagents_core::NewAgent::new("lead", "opencode");
        new.parent_id = Some(root.id.clone());
        let lead = store.create_agent(new).unwrap();
        let mut new = anyagents_core::NewAgent::new("worker", "opencode");
        new.parent_id = Some(lead.id.clone());
        let worker = store.create_agent(new).unwrap();
        let peer = store
            .create_agent(anyagents_core::NewAgent::new("peer", "opencode"))
            .unwrap();
        // A public sub-agent may also write to a root agent.
        store.set_agent_scope(&worker.id, "public").unwrap();
        mark_explicitly_archived(&store, dir.path(), &lead);
        let actor = Actor::Agent(worker.name.clone());
        let send = |to: Vec<&str>| {
            let body = SendRequest {
                sender: Some(actor.as_address()),
                to: to.into_iter().map(str::to_string).collect(),
                text: "status report".to_string(),
                ..Default::default()
            };
            let plan = SendPlan::parse(&store, &actor, &body)?;
            send_sync(&store, &actor, body, plan, None).map(|response| response.0)
        };

        let refused = send(vec!["parent"]).expect_err("no live recipient");
        assert_eq!(refused.status, StatusCode::CONFLICT);
        assert!(
            refused
                .message
                .contains("agent `lead` is archived; its parent is `root`"),
            "{}",
            refused.message
        );
        assert!(
            store.inbox(&lead.id, None).unwrap().is_empty(),
            "nothing stored unread"
        );
        assert_eq!(store.agent(&lead.id).unwrap().state, "archived");

        let mixed =
            send(vec!["parent", "agent:peer"]).unwrap_or_else(|error| panic!("{}", error.message));
        assert_eq!(mixed.deliveries.len(), 1);
        assert_eq!(mixed.deliveries[0].to, "agent:peer");
        assert_eq!(mixed.skipped.len(), 1);
        assert_eq!(mixed.skipped[0].to, "agent:lead");
        assert_eq!(
            mixed.skipped[0].reason,
            "agent `lead` is archived; its parent is `root`"
        );
        assert!(store.inbox(&lead.id, None).unwrap().is_empty());
        assert_eq!(store.inbox(&peer.id, None).unwrap().len(), 1);
    }

    /// A parked agent lists as `faulted`, with its errored turns and why it
    /// is blocked; a resume lists it as it was.
    #[test]
    fn a_parked_agent_is_shown_faulted_until_it_is_resumed() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("worker", "spiron"))
            .unwrap();
        store.set_agent_state(&agent.id, "running").unwrap();
        for _ in 0..anyagents_core::MAX_ERRORED_TURNS {
            store
                .note_turn_errored(&agent.id, "context_length_exceeded", true)
                .unwrap();
        }
        assert!(store.park_faulted(&agent.id).unwrap());
        let health = HealthBoard::default();
        let agent = store.agent(&agent.id).unwrap();
        let view = agent_view(&store, &health, &agent).unwrap();
        assert_eq!(view.state, "paused");
        assert_eq!(view.display_state(), "faulted");
        assert_eq!(view.turn_state, "faulted");
        assert_eq!(view.block_reason.as_deref(), Some("faulted"));
        assert_eq!(view.last_error.as_deref(), Some("context_length_exceeded"));
        let errors = view.turn_errors.expect("errored turns are shown");
        assert_eq!(errors.streak, anyagents_core::MAX_ERRORED_TURNS);
        assert_eq!(errors.limit, anyagents_core::MAX_ERRORED_TURNS);
        assert!(errors.faulted_at_ms.is_some());
        let (roster, _) = store.roster_agents(None).unwrap();
        let listed = roster_views(
            &store,
            &health,
            &roster.iter().collect::<Vec<_>>(),
            0,
            &ListAgentsQuery::default(),
        )
        .unwrap_or_else(|error| panic!("{}", error.message));
        assert_eq!(listed.agents[0].display_state(), "faulted");

        store.set_agent_state(&agent.id, "running").unwrap();
        let agent = store.agent(&agent.id).unwrap();
        let view = agent_view(&store, &health, &agent).unwrap();
        assert_eq!(view.display_state(), "running");
        assert!(view.turn_errors.is_none());
    }

    #[test]
    fn closing_sends_do_not_store_messages_receipts_or_wake_archived_targets() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let sender = store
            .create_agent(anyagents_core::NewAgent::new("sender", "opencode"))
            .unwrap();
        let mut new = anyagents_core::NewAgent::new("target", "opencode");
        new.parent_id = Some(sender.id.clone());
        let target = store.create_agent(new).unwrap();
        store.set_agent_state(&target.id, "archived").unwrap();
        let actor = Actor::Agent(sender.name.clone());
        for text in [
            "NO_REPLY",
            "End.",
            "停止。",
            "No more.",
            "。",
            "Done.",
            "終了。",
            "Final!",
        ] {
            let response = authored_send(&store, &actor, "agent:target", text, "closing-only");
            assert!(response.deliveries.is_empty());
            assert!(response.skipped.is_empty());
            assert!(store.inbox(&target.id, None).unwrap().is_empty());
            let key = send_key(&SendRequest {
                sender: Some(actor.as_address()),
                to: vec!["agent:target".into()],
                text: text.into(),
                idempotency_key: Some("closing-only".into()),
                ..Default::default()
            })
            .unwrap();
            assert!(store.send_receipt(&key).unwrap().is_none());
            assert_eq!(store.agent(&target.id).unwrap().state, "archived");
        }
        // A real message containing a closing word must still be delivered.
        store.set_agent_state(&target.id, "running").unwrap();
        let response = authored_send(
            &store,
            &actor,
            "agent:target",
            "Done. Build passed.",
            "real-message",
        );
        assert_eq!(response.deliveries.len(), 1);
        assert_eq!(store.inbox(&target.id, None).unwrap().len(), 1);
    }

    #[test]
    fn direct_authored_sends_wake_archived_agents_once_in_mailbox_order_and_survive_reopen() {
        let _env = env_lock();
        unsafe {
            std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
        }
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");
        let store = Store::open(&db).unwrap();
        let parent = store
            .create_agent(anyagents_core::NewAgent::new("parent-agent", "opencode"))
            .unwrap();
        let mut new = anyagents_core::NewAgent::new("worker", "opencode");
        new.parent_id = Some(parent.id.clone());
        let child = store.create_agent(new).unwrap();
        let actor = Actor::Agent(parent.name.clone());
        store
            .receive(&child.id, Incoming::new("queued before archive"))
            .unwrap();
        store.set_agent_state(&child.id, "archived").unwrap();
        mark_explicitly_archived(&store, dir.path(), &child);
        let cfg = anyagents_core::agent_config_dir(dir.path(), &child.id);
        for file in [
            ".anyagents-last-error.json",
            ".anyagents-notified-parent-error.json",
        ] {
            std::fs::write(cfg.join(file), "old error").unwrap();
        }
        // An authored quotation of a lifecycle prefix is still a direct send:
        // bodies are not the wake discriminator.
        let sent = authored_send(
            &store,
            &actor,
            "agent:worker",
            "[Sub-Agent Completed] quoted in a follow-up",
            "direct-1",
        );
        assert_eq!(sent.deliveries[0].seq, 2);
        assert_eq!(store.agent(&child.id).unwrap().state, "running");
        assert!(!has_explicit_archive_marker(dir.path(), &child.id));
        assert!(!cfg.join(".anyagents-last-error.json").exists());
        assert!(!cfg.join(".anyagents-notified-parent-error.json").exists());
        assert!(anyagents_core::agent_work_dir(dir.path(), &child.id).exists());
        let retried = authored_send(
            &store,
            &actor,
            "agent:worker",
            "[Sub-Agent Completed] quoted in a follow-up",
            "direct-1",
        );
        assert_eq!(retried.deliveries[0].id, sent.deliveries[0].id);
        assert_eq!(store.inbox(&child.id, None).unwrap().len(), 2);
        // Replay after a new archive is not a fresh wake instruction.
        store.set_agent_state(&child.id, "archived").unwrap();
        mark_explicitly_archived(&store, dir.path(), &child);
        authored_send(
            &store,
            &actor,
            "agent:worker",
            "[Sub-Agent Completed] quoted in a follow-up",
            "direct-1",
        );
        assert_eq!(store.agent(&child.id).unwrap().state, "archived");
        assert!(has_explicit_archive_marker(dir.path(), &child.id));
        // Operator-authored direct sends also work by immutable id.
        authored_send(
            &store,
            &Actor::Operator("operator".into()),
            child.id.as_str(),
            "operator follow-up",
            "direct-2",
        );
        drop(store);
        let store = Store::open(&db).unwrap();
        assert_eq!(store.agent(&child.id).unwrap().state, "running");
        assert!(!has_explicit_archive_marker(dir.path(), &child.id));
        let inbox = store.inbox(&child.id, None).unwrap();
        assert_eq!(
            inbox.iter().map(|m| m.seq).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(inbox[2].text, "operator follow-up");
        mark_explicitly_archived(&store, dir.path(), &child);
        let replayed = authored_send(
            &store,
            &Actor::Operator("operator".into()),
            child.id.as_str(),
            "operator follow-up",
            "direct-2",
        );
        assert_eq!(replayed.deliveries[0].seq, 3);
        assert_eq!(store.agent(&child.id).unwrap().state, "archived");
        assert!(has_explicit_archive_marker(dir.path(), &child.id));
        // Simulate a stop after mailbox commit but before whole-send receipt.
        rusqlite::Connection::open(&db)
            .unwrap()
            .execute("DELETE FROM sends", [])
            .unwrap();
        std::fs::write(
            cfg.join(".anyagents-last-error.json"),
            "new error after archive",
        )
        .unwrap();
        let replayed = authored_send(
            &store,
            &Actor::Operator("operator".into()),
            child.id.as_str(),
            "operator follow-up",
            "direct-2",
        );
        assert!(replayed.deliveries[0].duplicate);
        assert_eq!(store.agent(&child.id).unwrap().state, "archived");
        assert!(has_explicit_archive_marker(dir.path(), &child.id));
        assert!(cfg.join(".anyagents-last-error.json").exists());
        assert_eq!(store.inbox(&child.id, None).unwrap().len(), 3);
        unsafe {
            std::env::remove_var("ANYAGENTS_FAKE_MODE");
        }
    }

    #[test]
    fn archived_agents_stay_retired_on_channel_senders_aliases_groups_and_deleted_targets() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let parent = store
            .create_agent(anyagents_core::NewAgent::new("parent-agent", "opencode"))
            .unwrap();
        store.set_agent_state(&parent.id, "running").unwrap();
        let mut new = anyagents_core::NewAgent::new("worker", "opencode");
        new.parent_id = Some(parent.id.clone());
        let child = store.create_agent(new).unwrap();
        store.set_agent_state(&child.id, "archived").unwrap();
        mark_explicitly_archived(&store, dir.path(), &child);
        store.join_group("crew", &parent.id).unwrap();
        store.join_group("crew", &child.id).unwrap();
        let operator = Actor::Operator("operator".into());
        for to in ["all", "group:all", "group:crew"] {
            authored_send(&store, &operator, to, "broadcast", to);
            assert_eq!(store.agent(&child.id).unwrap().state, "archived");
        }
        let actor = Actor::Agent(parent.name.clone());
        for to in ["children", "group:children", "siblings"] {
            let body = SendRequest {
                to: vec![to.into()],
                ..Default::default()
            };
            assert!(SendPlan::parse(&store, &actor, &body).is_err());
        }
        for from in ["telegram:user:123", "telegram:chat:-456"] {
            // A channel sender cannot wake it, so the send is refused with
            // the reason instead of stored unread.
            let channel = Actor::parse(from).unwrap_or_else(|e| panic!("{}", e.message));
            let body = SendRequest {
                sender: Some(channel.as_address()),
                to: vec!["worker".into()],
                text: "channel update".into(),
                idempotency_key: Some(from.into()),
                ..Default::default()
            };
            let key = send_key(&body);
            let plan = SendPlan::parse(&store, &channel, &body)
                .unwrap_or_else(|e| panic!("{}", e.message));
            let refused = send_sync(&store, &channel, body, plan, key).expect_err("archived");
            assert_eq!(refused.status, StatusCode::CONFLICT);
            assert!(
                refused
                    .message
                    .contains("agent `worker` is archived; its parent is `parent-agent`"),
                "{}",
                refused.message
            );
            assert_eq!(store.agent(&child.id).unwrap().state, "archived");
            assert!(has_explicit_archive_marker(dir.path(), &child.id));
        }
        assert!(
            store
                .inbox(&child.id, None)
                .unwrap()
                .iter()
                .all(|message| message.text != "channel update"),
            "nothing is stored unread"
        );
        let deleted_id = child.id.clone();
        store.delete_agent(&deleted_id).unwrap();
        let body = SendRequest {
            to: vec![deleted_id.to_string()],
            ..Default::default()
        };
        let plan =
            SendPlan::parse(&store, &actor, &body).unwrap_or_else(|e| panic!("{}", e.message));
        let error = send_sync(&store, &actor, body, plan, None).expect_err("deleted target");
        assert_eq!(error.status, StatusCode::NOT_FOUND);
        assert!(store.agent(&deleted_id).is_err());
    }
    #[test]
    fn direct_send_and_archive_serialize_across_store_connections() {
        let _env = env_lock();
        unsafe {
            std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
        }
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");
        let store = Store::open(&db).unwrap();
        let recipient = store
            .create_agent(anyagents_core::NewAgent::new("recipient", "opencode"))
            .unwrap();
        store.set_agent_state(&recipient.id, "running").unwrap();
        let guard = store.lock_agent_lifecycle(&recipient.id).unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let other = Store::open(&db).unwrap();
        let sender = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let receipt = authored_send(
                &other,
                &Actor::Operator("operator".into()),
                "recipient",
                "follow-up",
                "interleave",
            );
            done_tx.send(receipt).unwrap();
        });
        started_rx.recv().unwrap();
        assert!(
            done_rx
                .recv_timeout(std::time::Duration::from_millis(30))
                .is_err()
        );
        store.set_agent_state(&recipient.id, "archived").unwrap();
        mark_explicitly_archived_locked(&store, dir.path(), &recipient).unwrap();
        drop(guard);
        let receipt = done_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        sender.join().unwrap();
        assert_eq!(receipt.deliveries[0].seq, 1);
        assert_eq!(store.agent(&recipient.id).unwrap().state, "running");
        assert!(!has_explicit_archive_marker(dir.path(), &recipient.id));
        // The converse ordering leaves a coherent archive, not running+marker.
        let guard = store.lock_agent_lifecycle(&recipient.id).unwrap();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let other = Store::open(&db).unwrap();
        let archived_id = recipient.id.clone();
        let archive = std::thread::spawn(move || {
            let recipient = other.agent(&archived_id).unwrap();
            mark_explicitly_archived(&other, other.data_dir(), &recipient);
            done_tx.send(()).unwrap();
        });
        assert!(
            done_rx
                .recv_timeout(std::time::Duration::from_millis(30))
                .is_err()
        );
        drop(guard);
        done_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        archive.join().unwrap();
        assert_eq!(store.agent(&recipient.id).unwrap().state, "archived");
        assert!(has_explicit_archive_marker(dir.path(), &recipient.id));
        unsafe {
            std::env::remove_var("ANYAGENTS_FAKE_MODE");
        }
    }
    #[test]
    fn archetype_templates_apply_frontmatter_defaults_and_prepend_instructions() {
        let _env = env_lock();
        unsafe {
            std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
        }
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        store
            .hold_capability_key("test-capability-key-0123456789abcdef")
            .unwrap();
        let parent_work = dir.path().join("parent-work");
        let agents_dir = parent_work.join(".agents").join("agents");
        std::fs::create_dir_all(&agents_dir).unwrap();
        std::fs::write(
            agents_dir.join("writer.md"),
            "---\ndescription: Ephemeral isolated-worktree sole writer\nscope: isolated\nworkdir: private\nharness: opencode\nmodel: gpt-5.4-mini\n---\nAlways run cargo test before opening a PR.",
        )
        .unwrap();
        std::fs::write(
            agents_dir.join("code-reviewer.md"),
            "---\ndescription: Read-only code reviewer\nscope: siblings\nharness: claude-code\n---\nVerify all invariants before approving.",
        )
        .unwrap();

        let data_templates = dir.path().join("agents-templates");
        std::fs::create_dir_all(&data_templates).unwrap();
        std::fs::write(
            data_templates.join("auditor.md"),
            "---\ndescription: Global template auditor\n---\nAudit template body.",
        )
        .unwrap();

        let mut new_parent = anyagents_core::NewAgent::new("calliope", "claude-code");
        new_parent.role = Some("Mind".into());
        new_parent.workdir = Some(parent_work.to_string_lossy().into_owned());
        new_parent.description = Some("Parent mind".into());
        let parent = store.create_agent(new_parent).unwrap();
        let state = AppState {
            store: Arc::clone(&store),
            health: HealthBoard::default(),
            sign_in: Arc::new(Mutex::new(ProcessRunner)),
            identity: Arc::new(ServingIdentity::capture()),
            profiles: Default::default(),
            auth: auth::Mode::Trusted {
                data_dir: dir.path().into(),
            },
        };

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let (status, Json(created)) = create_agent_route(
                Principal::Agent {
                    agent_id: parent.id.clone(),
                    name: parent.name.clone(),
                },
                State(state.clone()),
                axum::http::HeaderMap::new(),
                Json(
                    serde_json::from_value(serde_json::json!({
                        "name": "writer-sub-1",
                        "type": "writer",
                        "instructions": "Fix issue #42."
                    }))
                    .unwrap(),
                ),
            )
            .await
            .unwrap_or_else(|e| panic!("{}: {}", e.status, e.message));

            assert_eq!(status, StatusCode::CREATED);
            assert_eq!(
                created.description.as_deref(),
                Some("Ephemeral isolated-worktree sole writer")
            );
            assert_eq!(created.scope, "isolated");
            assert_eq!(created.harness, "opencode");
            assert_eq!(created.model.as_deref(), Some("gpt-5.4-mini"));
            let stored = store.agent_by_name("writer-sub-1").unwrap();
            assert_eq!(
                stored.instructions.as_deref(),
                Some("Always run cargo test before opening a PR.\n\nFix issue #42.")
            );

            // Role matching a template slug (`role: "code-reviewer"`) applies template defaults
            // when `archetype` is omitted.
            let (_, Json(reviewer)) = create_agent_route(
                Principal::Agent {
                    agent_id: parent.id.clone(),
                    name: parent.name.clone(),
                },
                State(state.clone()),
                axum::http::HeaderMap::new(),
                Json(
                    serde_json::from_value(serde_json::json!({
                        "name": "reviewer-sub-1",
                        "role": "code-reviewer"
                    }))
                    .unwrap(),
                ),
            )
            .await
            .unwrap_or_else(|e| panic!("{}: {}", e.status, e.message));
            assert_eq!(
                reviewer.description.as_deref(),
                Some("Read-only code reviewer")
            );
            assert_eq!(reviewer.scope, "siblings");
            let reviewer_stored = store.agent_by_name("reviewer-sub-1").unwrap();
            assert_eq!(
                reviewer_stored.instructions.as_deref(),
                Some("Verify all invariants before approving.")
            );

            // `<data_dir>/agents-templates/<name>.md` is also searched.
            let (_, Json(auditor)) = create_agent_route(
                Principal::Agent {
                    agent_id: parent.id.clone(),
                    name: parent.name.clone(),
                },
                State(state),
                axum::http::HeaderMap::new(),
                Json(
                    serde_json::from_value(serde_json::json!({
                        "name": "auditor-sub-1",
                        "template": "auditor"
                    }))
                    .unwrap(),
                ),
            )
            .await
            .unwrap_or_else(|e| panic!("{}: {}", e.status, e.message));
            assert_eq!(
                auditor.description.as_deref(),
                Some("Global template auditor")
            );
        });
        unsafe {
            std::env::remove_var("ANYAGENTS_FAKE_MODE");
        }
    }

    #[test]
    fn archetype_absence_preserves_http_harness_defaults_and_explicit_inheritance() {
        let _env = env_lock();
        unsafe {
            std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
        }
        let dir = tempfile::tempdir().unwrap();
        let state = archetype_validation_state(dir.path());
        let parent = state
            .store
            .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
            .unwrap();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            for (name, parent_ref, harness, role, expected) in [
                ("root-default", "root", None, None, "claude-code"),
                ("child-default", parent.id.as_str(), None, None, "claude-code"),
                ("unmatched-label", parent.id.as_str(), None, Some("nonexistent-display-label-134"), "claude-code"),
                ("child-empty", parent.id.as_str(), Some(""), None, "opencode"),
                ("child-inherit", parent.id.as_str(), Some("inherit"), None, "opencode"),
                ("child-explicit", parent.id.as_str(), Some("codex"), None, "codex"),
            ] {
                let mut body = serde_json::json!({"name": name, "parent": parent_ref, "workdir": dir.path().join(name), "scope": "team"});
                if let Some(harness) = harness { body["harness"] = serde_json::json!(harness); }
                if let Some(role) = role { body["role"] = serde_json::json!(role); }
                let (status, Json(created)) = create_agent_route(
                    Principal::Operator, State(state.clone()), axum::http::HeaderMap::new(), Json(serde_json::from_value(body).unwrap()),
                ).await.unwrap_or_else(|e| panic!("{}", e.message));
                assert_eq!(status, StatusCode::CREATED);
                assert_eq!(created.harness, expected, "{name}");
                assert_eq!(created.scope, "siblings");
            }
        });
        unsafe {
            std::env::remove_var("ANYAGENTS_FAKE_MODE");
        }
    }

    fn archetype_validation_state(data: &Path) -> AppState {
        let store = Arc::new(Store::open(data.join("state.db")).unwrap());
        store
            .hold_capability_key("test-capability-key-0123456789abcdef")
            .unwrap();
        AppState {
            store,
            health: HealthBoard::default(),
            sign_in: Arc::new(Mutex::new(ProcessRunner)),
            identity: Arc::new(ServingIdentity::capture()),
            profiles: Default::default(),
            auth: auth::Mode::Trusted {
                data_dir: data.into(),
            },
        }
    }

    async fn assert_archetype_create_refused(
        state: &AppState,
        body: serde_json::Value,
        expected_error: &str,
    ) {
        let result = create_agent_route(
            Principal::Operator,
            State(state.clone()),
            axum::http::HeaderMap::new(),
            Json(serde_json::from_value(body).unwrap()),
        )
        .await;
        let error = match result {
            Ok(_) => panic!("invalid archetype unexpectedly created an agent"),
            Err(error) => error,
        };
        assert_eq!(error.status, StatusCode::BAD_REQUEST, "{}", error.message);
        assert!(error.message.contains(expected_error), "{}", error.message);
        assert!(!error.message.contains("OUTSIDE_TEMPLATE_SENTINEL"));
        assert!(state.store.list_agents().unwrap().is_empty());
        let Json(listed) = list_agents(
            Principal::Operator,
            State(state.clone()),
            Query(ListAgentsQuery::default()),
        )
        .await
        .unwrap_or_else(|error| panic!("{}", error.message));
        assert!(listed.agents.is_empty());
        assert!(
            !serde_json::to_string(&listed)
                .unwrap()
                .contains("OUTSIDE_TEMPLATE_SENTINEL")
        );
        let description = get_description_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath("rejected-template-agent".into()),
        )
        .await;
        match description {
            Ok(_) => panic!("rejected template must not have a description"),
            Err(error) => assert_eq!(error.status, StatusCode::NOT_FOUND),
        }
        // Validation runs before creation, including config/SYSTEM_PROMPT.md writes.
        let agents_dir = state.store.data_dir().join("agents");
        assert!(
            !agents_dir.exists() || std::fs::read_dir(agents_dir).unwrap().next().is_none(),
            "a rejected template must not leave a SYSTEM_PROMPT.md or agent directory"
        );
    }

    #[cfg(unix)]
    #[test]
    fn archetype_templates_reject_file_and_directory_symlinks_without_leaking() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let _env = env_lock();
        unsafe {
            std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
        }
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            for variant in [
                "file",
                "agents-dir",
                "dot-agents-dir",
                "nested-dir",
                "data-dir",
            ] {
                let dir = tempfile::tempdir().unwrap();
                let state = archetype_validation_state(dir.path());
                let work = dir.path().join("work");
                let outside = dir.path().join("outside");
                std::fs::create_dir_all(&outside).unwrap();
                let secret = outside.join("escape.md");
                std::fs::write(&secret, "OUTSIDE_TEMPLATE_SENTINEL").unwrap();
                std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600)).unwrap();
                std::fs::write(outside.join("agent.md"), "OUTSIDE_TEMPLATE_SENTINEL").unwrap();
                match variant {
                    "file" => {
                        std::fs::create_dir_all(work.join(".agents/agents")).unwrap();
                        symlink(&secret, work.join(".agents/agents/escape.md")).unwrap();
                    }
                    "agents-dir" => {
                        std::fs::create_dir_all(work.join(".agents")).unwrap();
                        symlink(&outside, work.join(".agents/agents")).unwrap();
                    }
                    "dot-agents-dir" => {
                        std::fs::create_dir_all(&work).unwrap();
                        std::fs::create_dir_all(outside.join("agents")).unwrap();
                        std::fs::write(
                            outside.join("agents/escape.md"),
                            "OUTSIDE_TEMPLATE_SENTINEL",
                        )
                        .unwrap();
                        symlink(&outside, work.join(".agents")).unwrap();
                    }
                    "nested-dir" => {
                        std::fs::create_dir_all(work.join(".agents/agents")).unwrap();
                        symlink(&outside, work.join(".agents/agents/escape")).unwrap();
                    }
                    "data-dir" => {
                        std::fs::create_dir_all(&work).unwrap();
                        symlink(&outside, dir.path().join("agents-templates")).unwrap();
                    }
                    _ => unreachable!(),
                }
                // A lower-priority valid template must never mask a hostile path.
                std::fs::create_dir_all(dir.path().join("archetypes")).unwrap();
                std::fs::write(dir.path().join("archetypes/escape.md"), "Safe fallback.").unwrap();
                for selector in ["archetype", "role"] {
                    let mut body = serde_json::json!({
                        "name": "rejected-template-agent",
                        "workdir": work,
                        "harness": "codex",
                    });
                    body[selector] = serde_json::json!("escape");
                    assert_archetype_create_refused(&state, body, "symlink").await;
                }
            }
        });
        unsafe {
            std::env::remove_var("ANYAGENTS_FAKE_MODE");
        }
    }

    #[test]
    fn archetype_templates_reject_unknown_explicit_names_and_malformed_frontmatter() {
        let _env = env_lock();
        unsafe {
            std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
        }
        let dir = tempfile::tempdir().unwrap();
        let state = archetype_validation_state(dir.path());
        let work = dir.path().join("work");
        let templates = work.join(".agents/agents");
        std::fs::create_dir_all(&templates).unwrap();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            for selector in ["archetype", "template", "type", "preset"] {
                let mut body =
                    serde_json::json!({"name": "rejected-template-agent", "workdir": work});
                body[selector] = serde_json::json!("nonexistent-role");
                assert_archetype_create_refused(
                    &state,
                    body,
                    "unknown archetype template \"nonexistent-role\"",
                )
                .await;
            }
            assert_archetype_create_refused(
                &state,
                serde_json::json!({"name": "rejected-template-agent", "scope": "invalid-request-scope", "workdir": work}),
                "invalid scope `invalid-request-scope`",
            ).await;
            for name in [
                ".",
                "..",
                "../escape",
                "escape.md",
                "escape/agent",
                "escape\\agent",
            ] {
                assert_archetype_create_refused(
                    &state,
                    serde_json::json!({
                        "name": "rejected-template-agent", "archetype": name, "workdir": work,
                    }),
                    "invalid archetype template name",
                )
                .await;
            }
            for (raw, expected) in [
                ("---\ndescription: unclosed", "unclosed YAML frontmatter"),
                (
                    "---\nscpoe: isolated\n---\nUNKNOWN_KEY_BODY",
                    "unknown archetype template frontmatter key \"scpoe\"",
                ),
                (
                    "---\nscope: invalid-review-scope\n---\nINVALID_SCOPE_BODY",
                    "invalid scope `invalid-review-scope`",
                ),
                (
                    "---\ndescription broken\n---\nBody.",
                    "malformed YAML frontmatter",
                ),
                (
                    "---\ndescription: [broken\n---\nBody.",
                    "malformed YAML frontmatter",
                ),
                (
                    "---\ndescription: broken\n---not-a-delimiter\nBody.",
                    "unclosed YAML frontmatter",
                ),
            ] {
                std::fs::write(templates.join("broken.md"), raw).unwrap();
                for selector in ["archetype", "role"] {
                    let mut body =
                        serde_json::json!({"name": "rejected-template-agent", "workdir": work});
                    body[selector] = serde_json::json!("broken");
                    assert_archetype_create_refused(&state, body, expected).await;
                }
            }
        });
        unsafe {
            std::env::remove_var("ANYAGENTS_FAKE_MODE");
        }
    }

    #[test]
    fn memory_topic_upsert_syncs_index_and_supports_direct_o1_topic_read() {
        let _env = env_lock();
        let dir = crate::test_tempdir();
        let state = archetype_validation_state(dir.path());
        let vulcan = state
            .store
            .create_agent(anyagents_core::NewAgent::new("vulcan", "agy"))
            .unwrap();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            // 1. Seed initial index so the 60s refresh cache is populated before upsert.
            let _ = state.store.sync_agent_memory_index(&vulcan);

            // 2. Upsert a topic via conversation_route.
            let Json(upserted) = conversation_route(
                Principal::Agent {
                    agent_id: vulcan.id.clone(),
                    name: vulcan.name.clone(),
                },
                State(state.clone()),
                AxumPath(vulcan.name.clone()),
                Query(ConversationQuery {
                    scope: anyagents_core::host_index::MemoryScope::Memory,
                    topic: Some("test-invariants".into()),
                    description: Some("Vulcan system invariants and verified receipts".into()),
                    content: Some(
                        "# Test Invariants\n\nVerified invariant alpha-beta-gamma.\n".into(),
                    ),
                    ..Default::default()
                }),
            )
            .await
            .unwrap_or_else(|e| panic!("{}: {}", e.status, e.message));

            assert_eq!(upserted.turns.len(), 1);
            let turn = &upserted.turns[0];
            assert!(
                turn.raw_bytes.is_none(),
                "upsert response must not bloat payload with raw_bytes"
            );
            let raw = turn.raw.as_ref().expect("raw coordinate");
            assert_eq!(raw.seq, 1);

            // 3. Immediate raw_id + raw_sha256 readback (Read-Your-Own-Writes within 60s window).
            let Json(raw_read) = conversation_route(
                Principal::Agent {
                    agent_id: vulcan.id.clone(),
                    name: vulcan.name.clone(),
                },
                State(state.clone()),
                AxumPath(vulcan.name.clone()),
                Query(ConversationQuery {
                    scope: anyagents_core::host_index::MemoryScope::Memory,
                    raw_id: Some(turn.id.clone()),
                    raw_sha256: Some(raw.sha256.clone()),
                    raw_offset: Some(0),
                    raw_length: Some(32),
                    ..Default::default()
                }),
            )
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "immediate raw_id readback failed: {}: {}",
                    e.status, e.message
                )
            });
            assert_eq!(raw_read.turns.len(), 1);
            assert!(
                raw_read.turns[0].raw_bytes.is_some(),
                "explicit raw_id slice read must include raw_bytes"
            );

            // 4. Direct O(1) read by topic (without content or delete).
            let Json(topic_read) = conversation_route(
                Principal::Agent {
                    agent_id: vulcan.id.clone(),
                    name: vulcan.name.clone(),
                },
                State(state.clone()),
                AxumPath(vulcan.name.clone()),
                Query(ConversationQuery {
                    scope: anyagents_core::host_index::MemoryScope::Memory,
                    topic: Some("test-invariants".into()),
                    ..Default::default()
                }),
            )
            .await
            .unwrap_or_else(|e| panic!("direct topic read failed: {}: {}", e.status, e.message));
            assert_eq!(topic_read.turns.len(), 1);
            assert!(
                topic_read.turns[0]
                    .text
                    .contains("Verified invariant alpha-beta-gamma.")
            );
            assert!(topic_read.turns[0].raw_bytes.is_none());
            assert_eq!(
                topic_read.turns[0].reply_to.as_deref(),
                Some("test-invariants.md")
            );

            // 5. Lexical search in scope="memory" returns the topic file with reply_to filename
            // and does not return duplicate MEMORY.md index hits.
            let Json(searched) = conversation_route(
                Principal::Agent {
                    agent_id: vulcan.id.clone(),
                    name: vulcan.name.clone(),
                },
                State(state.clone()),
                AxumPath(vulcan.name.clone()),
                Query(ConversationQuery {
                    scope: anyagents_core::host_index::MemoryScope::Memory,
                    query: Some("invariants".into()),
                    ..Default::default()
                }),
            )
            .await
            .unwrap_or_else(|e| panic!("memory search failed: {}: {}", e.status, e.message));
            assert_eq!(searched.turns.len(), 1);
            assert_eq!(
                searched.turns[0].reply_to.as_deref(),
                Some("test-invariants.md")
            );
        });
    }

    #[test]
    fn active_seat_ids_excludes_archived_agents_from_startup_boundary() {
        let data = crate::test_tempdir();
        let store = Store::open(data.path().join("state.db")).unwrap();
        let active = store
            .create_agent(anyagents_core::NewAgent::new("active-root", "codex"))
            .unwrap();
        let archived = store
            .create_agent(anyagents_core::NewAgent::new("archived-root", "codex"))
            .unwrap();
        store.set_agent_state(&archived.id, "archived").unwrap();
        let seats = active_seat_ids(&store).unwrap();
        assert_eq!(seats, vec![active.id.to_string()]);
    }

    #[test]
    fn streamable_http_mcp_enforces_bearer_auth_and_three_mode_profiles() {
        use http_body_util::BodyExt as _;

        let _env = env_lock();
        let dir = crate::test_tempdir();
        let state = archetype_validation_state(dir.path());
        std::fs::write(
            dir.path().join(crate::remote::CONFIG_FILE),
            "[server]\npublic_url = \"https://agents.example.com\"\n",
        )
        .unwrap();
        let cloud_agent = state
            .store
            .create_agent(anyagents_core::NewAgent::new(
                "cloud-worker",
                "claude-cloud",
            ))
            .unwrap();
        let spiron_agent = state
            .store
            .create_agent(anyagents_core::NewAgent::new(
                "spiron-worker",
                "spiron-cloud",
            ))
            .unwrap();
        // HTTP MCP takes remote session tokens only; a local capability is
        // refused below.
        let remote_token = |agent: &Agent| {
            crate::remote::begin_session(&state.store, agent).unwrap();
            crate::remote::session_token(&anyagents_core::agent_config_dir(
                state.store.data_dir(),
                &agent.id,
            ))
            .unwrap()
        };
        let cloud_cap = remote_token(&cloud_agent);
        let spiron_cap = remote_token(&spiron_agent);
        let local_cap = state.store.issue_capability(&cloud_agent.id).unwrap();

        // Seed a skill inside the agent's config dir so `skills` list/read works deterministically.
        let skill_dir = anyagents_core::agent_config_dir(state.store.data_dir(), &cloud_agent.id)
            .join(".agents/skills/test-skill");
        std::fs::create_dir_all(&skill_dir).unwrap();
        std::fs::write(
            skill_dir.join("SKILL.md"),
            "---\nname: test-skill\ndescription: Synthetic test skill for MCP verification\n---\n# Test Skill\nBody content.\n",
        )
        .unwrap();

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            // Verify both `build_router` and `build_webhook_router` compile and bind `/mcp`.
            let _main_app = build_router(state.clone());
            let _webhook_app = build_webhook_router(state.clone());

            // 1. Missing or invalid bearer token returns 401 Unauthorized.
            let unauth_err = mcp_post_route(
                State(state.clone()),
                axum::http::HeaderMap::new(),
                Query(McpRouteQuery::default()),
                Json(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {}
                })),
            )
            .await
            .expect_err("missing auth must fail with 401");
            assert_eq!(unauth_err.status, StatusCode::UNAUTHORIZED);

            let mut bad_headers = axum::http::HeaderMap::new();
            bad_headers.insert(
                axum::http::header::AUTHORIZATION,
                "Bearer invalid-capability-token".parse().unwrap(),
            );
            let bad_err = mcp_post_route(
                State(state.clone()),
                bad_headers,
                Query(McpRouteQuery::default()),
                Json(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {}
                })),
            )
            .await
            .expect_err("invalid token must fail with 401");
            assert_eq!(bad_err.status, StatusCode::UNAUTHORIZED);

            // A valid local capability is refused with 403: local agents use
            // stdio MCP, which runs as the agent, never HTTP inside the server.
            for header in [
                axum::http::header::AUTHORIZATION.as_str(),
                crate::operator::AGENT_HEADER,
            ] {
                let mut local_headers = axum::http::HeaderMap::new();
                let value = if header == "authorization" {
                    format!("Bearer {local_cap}")
                } else {
                    local_cap.clone()
                };
                local_headers.insert(
                    axum::http::HeaderName::from_static(header),
                    value.parse().unwrap(),
                );
                let refused = mcp_post_route(
                    State(state.clone()),
                    local_headers,
                    Query(McpRouteQuery::default()),
                    Json(serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/list",
                        "params": {}
                    })),
                )
                .await
                .expect_err("a local capability must be refused over HTTP");
                assert_eq!(refused.status, StatusCode::FORBIDDEN, "{header}");
                assert!(
                    refused.message.contains("only remote session tokens"),
                    "{}",
                    refused.message
                );
            }

            // Helper to call `mcp_post_route` with a Bearer token and optional profile query.
            let post_mcp = |token: &str, profile: Option<&str>, body: serde_json::Value| {
                let state = state.clone();
                let mut headers = axum::http::HeaderMap::new();
                headers.insert(
                    axum::http::header::AUTHORIZATION,
                    format!("Bearer {token}").parse().unwrap(),
                );
                let query = McpRouteQuery {
                    profile: profile.map(str::to_string),
                };
                async move {
                    let resp = mcp_post_route(State(state), headers, Query(query), Json(body))
                        .await
                        .unwrap();
                    let status = resp.status();
                    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
                    let json: serde_json::Value = if bytes.is_empty() {
                        serde_json::Value::Null
                    } else {
                        serde_json::from_slice(&bytes).unwrap()
                    };
                    (status, json)
                }
            };

            // 2. GET /mcp returns 200 OK text/event-stream by default and JSON when Accept: application/json.
            let mut get_headers = axum::http::HeaderMap::new();
            get_headers.insert(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {cloud_cap}").parse().unwrap(),
            );
            let sse_resp = mcp_get_route(
                State(state.clone()),
                get_headers,
                Query(McpRouteQuery::default()),
            )
            .await
            .unwrap();
            assert_eq!(sse_resp.status(), StatusCode::OK);
            assert!(
                sse_resp
                    .headers()
                    .get("content-type")
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with("text/event-stream")
            );

            // 3. POST /mcp with valid Bearer capability handles `initialize` and `tools/list` under default `Remote` profile.
            let (status, init_json) = post_mcp(
                &cloud_cap,
                None,
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {}
                }),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(init_json["result"]["serverInfo"]["name"], "anyagents");

            let (status, list_json) = post_mcp(
                &cloud_cap,
                None,
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/list",
                    "params": {}
                }),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            let remote_tools: Vec<String> = list_json["result"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t["name"].as_str().unwrap().to_string())
                .collect();
            let expected_remote: Vec<String> = crate::mcp::REMOTE_MCP_TOOLS
                .iter()
                .map(|s| (*s).to_string())
                .collect();
            assert_eq!(remote_tools, expected_remote);
            assert!(!remote_tools.contains(&"code".to_string()));

            // Calling `code` under `Remote` profile is refused with JSON-RPC error.
            let (status, code_err_json) = post_mcp(
                &cloud_cap,
                None,
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "tools/call",
                    "params": {
                        "name": "code",
                        "arguments": { "code": "return 1;" }
                    }
                }),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            assert!(
                code_err_json["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("disabled by active MCP profile `remote`"),
                "unexpected error response: {code_err_json}"
            );

            // Calling `send`, `inbox`, `agents`, `memory`, `skills` succeeds under `Remote` profile.
            let (_, send_res) = post_mcp(
                &cloud_cap,
                None,
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "tools/call",
                    "params": {
                        "name": "send",
                        "arguments": {
                            "to": ["spiron-worker"],
                            "text": "hello from remote mcp"
                        }
                    }
                }),
            )
            .await;
            assert_ne!(
                send_res["result"]["isError"],
                serde_json::json!(true),
                "{send_res}"
            );

            let (_, inbox_res) = post_mcp(
                &spiron_cap,
                None,
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 5,
                    "method": "tools/call",
                    "params": {
                        "name": "inbox",
                        "arguments": { "limit": 10 }
                    }
                }),
            )
            .await;
            assert_ne!(
                inbox_res["result"]["isError"],
                serde_json::json!(true),
                "{inbox_res}"
            );
            assert_eq!(
                inbox_res["result"]["structuredContent"]["messages"][0]["text"],
                "hello from remote mcp"
            );

            let (_, agents_res) = post_mcp(
                &cloud_cap,
                None,
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 6,
                    "method": "tools/call",
                    "params": {
                        "name": "agents",
                        "arguments": { "action": "list" }
                    }
                }),
            )
            .await;
            assert_ne!(
                agents_res["result"]["isError"],
                serde_json::json!(true),
                "{agents_res}"
            );

            let (_, mem_res) = post_mcp(
                &cloud_cap,
                None,
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 7,
                    "method": "tools/call",
                    "params": {
                        "name": "memory",
                        "arguments": {
                            "action": "write",
                            "topic": "remote-note",
                            "description": "Note written over remote MCP",
                            "content": "# Remote Note\nVerified.\n"
                        }
                    }
                }),
            )
            .await;
            assert_ne!(
                mem_res["result"]["isError"],
                serde_json::json!(true),
                "{mem_res}"
            );

            let (_, skills_res) = post_mcp(
                &cloud_cap,
                None,
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 8,
                    "method": "tools/call",
                    "params": {
                        "name": "skills",
                        "arguments": { "action": "list" }
                    }
                }),
            )
            .await;
            assert_ne!(
                skills_res["result"]["isError"],
                serde_json::json!(true),
                "{skills_res}"
            );
            assert!(
                skills_res["result"]["structuredContent"]["skills"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|s| s["name"] == "test-skill"),
                "{skills_res}"
            );

            // 4. `spiron` agents are treated uniformly (defaulting to `McpProfile::Remote` over HTTP `/mcp`),
            // while explicit `?profile=coordination` exposes pure coordination tools only.
            let (_, spiron_default_json) = post_mcp(
                &spiron_cap,
                None,
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 9,
                    "method": "tools/list",
                    "params": {}
                }),
            )
            .await;
            let spiron_default_tools: Vec<String> = spiron_default_json["result"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t["name"].as_str().unwrap().to_string())
                .collect();
            assert_eq!(spiron_default_tools, expected_remote);

            let (_, spiron_list_json) = post_mcp(
                &spiron_cap,
                Some("coordination"),
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 91,
                    "method": "tools/list",
                    "params": {}
                }),
            )
            .await;
            let spiron_tools: Vec<String> = spiron_list_json["result"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t["name"].as_str().unwrap().to_string())
                .collect();
            let expected_coord: Vec<String> = crate::mcp::COORDINATION_MCP_TOOLS
                .iter()
                .map(|s| (*s).to_string())
                .collect();
            assert_eq!(spiron_tools, expected_coord);
            for omitted in [
                "code",
                "web_search",
                "web_fetch",
                "memory",
                "skills",
                "browser",
                "computer",
            ] {
                assert!(!spiron_tools.contains(&omitted.to_string()));
                let (_, err_json) = post_mcp(
                    &spiron_cap,
                    Some("coordination"),
                    serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": 10,
                        "method": "tools/call",
                        "params": {
                            "name": omitted,
                            "arguments": {}
                        }
                    }),
                )
                .await;
                assert!(
                    err_json["error"]["message"]
                        .as_str()
                        .unwrap()
                        .contains("disabled by active MCP profile `coordination`"),
                    "expected `{omitted}` to be blocked in coordination profile, got {err_json}"
                );
            }
        });
    }

    /// One MCP POST with a bearer token and an optional `?profile=`.
    async fn post_mcp_as(
        state: &AppState,
        token: &str,
        profile: Option<&str>,
        body: serde_json::Value,
    ) -> Result<(StatusCode, serde_json::Value), ApiError> {
        use http_body_util::BodyExt as _;
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        let query = McpRouteQuery {
            profile: profile.map(str::to_string),
        };
        let resp = mcp_post_route(State(state.clone()), headers, Query(query), Json(body)).await?;
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        };
        Ok((status, json))
    }

    fn listed_tools(list: &serde_json::Value) -> Vec<String> {
        list["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_string())
            .collect()
    }

    fn tools_list() -> serde_json::Value {
        serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {} })
    }

    fn tool_call(name: &str, arguments: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": { "name": name, "arguments": arguments }
        })
    }

    #[test]
    fn remote_session_tokens_get_only_the_remote_profile() {
        use http_body_util::BodyExt as _;
        let _env = env_lock();
        let dir = crate::test_tempdir();
        let state = archetype_validation_state(dir.path());
        std::fs::write(
            dir.path().join(crate::remote::CONFIG_FILE),
            "[server]\npublic_url = \"https://agents.example.com\"\n",
        )
        .unwrap();
        let cloud = state
            .store
            .create_agent(anyagents_core::NewAgent::new(
                "cloud-remote",
                "claude-cloud",
            ))
            .unwrap();
        let peer = state
            .store
            .create_agent(anyagents_core::NewAgent::new("local-peer", "claude-code"))
            .unwrap();
        let peer_cap = state.store.issue_capability(&peer.id).unwrap();
        crate::remote::begin_session(&state.store, &cloud).unwrap();
        let cloud_cfg = anyagents_core::agent_config_dir(state.store.data_dir(), &cloud.id);
        let remote = crate::remote::session_token(&cloud_cfg).unwrap();
        let expected_remote: Vec<String> = crate::mcp::REMOTE_MCP_TOOLS
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        assert_eq!(
            expected_remote,
            [
                "send",
                "inbox",
                "agents",
                "groups",
                "schedules",
                "conversation",
                "memory",
                "skills"
            ]
        );

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            // The default, and an explicit `remote`, list exactly the remote tools.
            for asked in [None, Some("remote"), Some("cloud")] {
                let (status, list) = post_mcp_as(&state, &remote, asked, tools_list())
                    .await
                    .unwrap();
                assert_eq!(status, StatusCode::OK);
                assert_eq!(listed_tools(&list), expected_remote, "{asked:?}");
            }
            // `coordination` narrows; `full` is refused, and so is nonsense.
            let (_, list) = post_mcp_as(&state, &remote, Some("coordination"), tools_list())
                .await
                .unwrap();
            assert_eq!(listed_tools(&list), crate::mcp::COORDINATION_MCP_TOOLS);
            for (asked, expected) in [
                ("full", StatusCode::FORBIDDEN),
                ("local", StatusCode::FORBIDDEN),
                ("everything", StatusCode::BAD_REQUEST),
            ] {
                let err = post_mcp_as(&state, &remote, Some(asked), tools_list())
                    .await
                    .unwrap_err();
                assert_eq!(err.status, expected, "{asked}: {}", err.message);
            }
            // Nothing that runs on or from the host can be called, even by an
            // alias or the vendor-prefixed name.
            for host_tool in [
                "browser",
                "computer",
                "device",
                "media",
                "code",
                "files",
                "web",
                "web_search",
                "web_fetch",
                "mcp__anyagents__browser",
                "mcp__anyagents__computer",
            ] {
                let (_, refused) = post_mcp_as(
                    &state,
                    &remote,
                    None,
                    tool_call(host_tool, serde_json::json!({})),
                )
                .await
                .unwrap();
                assert!(
                    refused["error"]["message"]
                        .as_str()
                        .unwrap_or_default()
                        .contains("disabled by active MCP profile `remote`"),
                    "{host_tool}: {refused}"
                );
            }
            // Coordination works, as the agent the token names.
            let (_, sent) = post_mcp_as(
                &state,
                &remote,
                None,
                tool_call(
                    "send",
                    serde_json::json!({ "to": ["local-peer"], "text": "from the cloud" }),
                ),
            )
            .await
            .unwrap();
            assert_ne!(sent["result"]["isError"], serde_json::json!(true), "{sent}");
            let inbox = state.store.inbox(&peer.id, None).unwrap();
            assert_eq!(inbox.len(), 1);
            assert_eq!(inbox[0].text, "from the cloud");
            assert!(
                inbox[0]
                    .sender
                    .as_deref()
                    .is_some_and(|sender| sender.contains(cloud.id.as_str())),
                "{:?}",
                inbox[0].sender
            );

            // The GET route names the session and its expiry, and refuses `full`.
            let get = |profile: Option<&str>| {
                let mut headers = axum::http::HeaderMap::new();
                headers.insert(
                    axum::http::header::AUTHORIZATION,
                    format!("Bearer {remote}").parse().unwrap(),
                );
                headers.insert(
                    axum::http::header::ACCEPT,
                    "application/json".parse().unwrap(),
                );
                mcp_get_route(
                    State(state.clone()),
                    headers,
                    Query(McpRouteQuery {
                        profile: profile.map(str::to_string),
                    }),
                )
            };
            let described = get(None).await.unwrap();
            let bytes = described.into_body().collect().await.unwrap().to_bytes();
            let described: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(described["profile"], "remote");
            assert_eq!(described["agent"], "cloud-remote");
            let grant = state.store.authenticate_remote_session(&remote).unwrap();
            assert_eq!(described["remote_session"]["session_id"], grant.session_id);
            assert_eq!(
                described["remote_session"]["expires_at_ms"],
                grant.expires_at_ms
            );
            assert_eq!(
                get(Some("full")).await.unwrap_err().status,
                StatusCode::FORBIDDEN
            );

            // Over HTTP a local capability is refused outright.
            for asked in [None, Some("full")] {
                let refused = post_mcp_as(&state, &peer_cap, asked, tools_list())
                    .await
                    .unwrap_err();
                assert_eq!(refused.status, StatusCode::FORBIDDEN, "{asked:?}");
            }
        });
    }

    #[test]
    fn remote_session_tokens_stop_at_expiry_revocation_relaunch_and_stop() {
        let _env = env_lock();
        let dir = crate::test_tempdir();
        let state = archetype_validation_state(dir.path());
        std::fs::write(
            dir.path().join(crate::remote::CONFIG_FILE),
            "[server]\npublic_url = \"https://agents.example.com\"\n",
        )
        .unwrap();
        let cloud = state
            .store
            .create_agent(anyagents_core::NewAgent::new(
                "cloud-expiring",
                "claude-cloud",
            ))
            .unwrap();
        let cloud_cfg = anyagents_core::agent_config_dir(state.store.data_dir(), &cloud.id);
        let begin = || {
            crate::remote::begin_session(&state.store, &cloud).unwrap();
            crate::remote::session_token(&cloud_cfg).unwrap()
        };
        let set_expiry = |at_ms: i64| {
            rusqlite::Connection::open(dir.path().join("state.db"))
                .unwrap()
                .execute(
                    "UPDATE remote_session_tokens SET expires_at_ms = ?1",
                    [at_ms],
                )
                .unwrap();
        };

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let accepted = |token: String| {
                let state = state.clone();
                async move {
                    post_mcp_as(&state, &token, None, tools_list())
                        .await
                        .map(|(status, _)| status)
                        .map_err(|err| (err.status, err.message))
                }
            };
            let refused = |result: Result<StatusCode, (StatusCode, String)>| {
                let (status, message) = result.unwrap_err();
                assert_eq!(status, StatusCode::UNAUTHORIZED);
                assert!(message.contains("remote session token"), "{message}");
            };

            // Expiry.
            let first = begin();
            assert_eq!(accepted(first.clone()).await.unwrap(), StatusCode::OK);
            set_expiry(anyagents_core::now_ms() - 1);
            refused(accepted(first.clone()).await);

            // On demand, through the operator route.
            let second = begin();
            assert_eq!(accepted(second.clone()).await.unwrap(), StatusCode::OK);
            let revoked = revoke_remote_sessions_route(
                Principal::Operator,
                State(state.clone()),
                AxumPath("cloud-expiring".into()),
                Json(RevokeRemoteSessionsRequest::default()),
            )
            .await
            .unwrap();
            assert_eq!(revoked.0["revoked"], 1);
            refused(accepted(second.clone()).await);
            assert!(!cloud_cfg.join(crate::remote::TOKEN_FILE).exists());

            // A relaunch supersedes the previous session.
            let third = begin();
            let fourth = begin();
            refused(accepted(third).await);
            assert_eq!(accepted(fourth.clone()).await.unwrap(), StatusCode::OK);

            // Pausing (stop) and archiving revoke in the store commit itself.
            state.store.set_agent_state(&cloud.id, "paused").unwrap();
            refused(accepted(fourth.clone()).await);
            state.store.set_agent_state(&cloud.id, "running").unwrap();
            refused(accepted(fourth).await);
            let fifth = begin();
            assert_eq!(accepted(fifth.clone()).await.unwrap(), StatusCode::OK);
            state.store.set_agent_state(&cloud.id, "archived").unwrap();
            refused(accepted(fifth).await);

            // A remote token is never a credential on the internal API.
            state.store.set_agent_state(&cloud.id, "running").unwrap();
            let sixth = begin();
            let principal = state.auth.principal(&state.store, &{
                let request = axum::http::Request::builder()
                    .header(crate::operator::AGENT_HEADER, sixth.as_str())
                    .body(())
                    .unwrap();
                let (parts, ()) = request.into_parts();
                crate::auth::Presented::of(&parts)
            });
            assert!(principal.is_err(), "{principal:?}");
        });
    }

    #[test]
    fn the_public_listener_answers_readiness_only_and_the_socket_keeps_the_identity() {
        let _env = env_lock();
        let dir = crate::test_tempdir();
        let state = archetype_validation_state(dir.path());
        let socket_identity = state.identity.status();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let public = build_webhook_router(state.clone());
            let served = tokio::spawn(async move {
                let _ = axum::serve(listener, public).await;
            });
            for path in ["/", "/healthz", "/v1/webhooks/healthz"] {
                let raw = tokio::task::spawn_blocking(move || {
                    use std::io::{Read as _, Write as _};
                    let mut stream = std::net::TcpStream::connect(addr).unwrap();
                    write!(
                        stream,
                        "GET {path} HTTP/1.1\r\nHost: public\r\nConnection: close\r\n\r\n"
                    )
                    .unwrap();
                    let mut raw = String::new();
                    stream.read_to_string(&mut raw).unwrap();
                    raw
                })
                .await
                .unwrap();
                assert!(raw.starts_with("HTTP/1.1 200"), "{path}: {raw}");
                assert!(
                    raw.to_ascii_lowercase().contains("cache-control: no-store"),
                    "{path}: {raw}"
                );
                let body: serde_json::Value =
                    serde_json::from_str(raw.split_once("\r\n\r\n").unwrap().1).unwrap();
                assert_eq!(body, serde_json::json!({ "ok": true }), "{path}");
            }
            served.abort();
        });
        // The operator's socket still serves the full serving identity.
        for field in [
            "version",
            "source_sha",
            "binary_sha256",
            "binary_sha256_state",
        ] {
            assert!(socket_identity.get(field).is_some(), "{field}");
        }
    }

    #[test]
    fn remote_agents_cannot_be_created_without_a_public_url() {
        let _env = env_lock();
        let dir = crate::test_tempdir();
        let state = archetype_validation_state(dir.path());
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let refused = create_agent_route(
                Principal::Operator,
                State(state.clone()),
                axum::http::HeaderMap::new(),
                Json(
                    serde_json::from_value(serde_json::json!({
                        "name": "cloud-unconfigured",
                        "harness": "claude-cloud",
                        "parent": "root",
                    }))
                    .unwrap(),
                ),
            )
            .await
            .unwrap_err();
            assert_eq!(refused.status, StatusCode::BAD_REQUEST);
            assert!(
                refused.message.contains("public_url"),
                "{}",
                refused.message
            );
            assert!(
                refused.message.contains(crate::remote::CONFIG_FILE),
                "{}",
                refused.message
            );
            assert!(state.store.agent_by_name("cloud-unconfigured").is_err());

            // Moving an existing agent onto a remote harness is refused too.
            state
                .store
                .create_agent(anyagents_core::NewAgent::new("local-first", "claude-code"))
                .unwrap();
            let moved = harness_route(
                Principal::Operator,
                State(state.clone()),
                AxumPath("local-first".into()),
                axum::http::HeaderMap::new(),
                Json(HarnessRequest {
                    harness: "openai-cloud".into(),
                    model: None,
                    profile: None,
                }),
            )
            .await
            .unwrap_err();
            assert_eq!(moved.status, StatusCode::BAD_REQUEST);
            assert!(moved.message.contains("public_url"), "{}", moved.message);
            assert_eq!(
                state.store.agent_by_name("local-first").unwrap().harness,
                "claude-code"
            );
        });
    }

    #[test]
    fn http_mcp_never_serves_more_than_the_remote_profile_to_any_credential() {
        let _env = env_lock();
        let dir = crate::test_tempdir();
        let state = archetype_validation_state(dir.path());
        std::fs::write(
            dir.path().join(crate::remote::CONFIG_FILE),
            "[server]\npublic_url = \"https://agents.example.com\"\n",
        )
        .unwrap();
        let local = state
            .store
            .create_agent(anyagents_core::NewAgent::new("seat-agent", "claude-cloud"))
            .unwrap();
        crate::remote::begin_session(&state.store, &local).unwrap();
        let cap =
            crate::remote::session_token(&anyagents_core::agent_config_dir(dir.path(), &local.id))
                .unwrap();
        // Neither a sandbox binding nor disabled built-in tools upgrades HTTP
        // any more: over HTTP the tools would run as the server.
        let cfg = anyagents_core::agent_config_dir(dir.path(), &local.id);
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::write(cfg.join("cloud.json"), r#"{"disable_builtin_tools":true}"#).unwrap();
        anyagents_tools::SandboxConfig {
            endpoint: "https://sandbox.example.com".into(),
            sandbox_id: "sbx-1".into(),
            token: None,
            workdir: "/workspace".into(),
            remote_spawn: false,
        }
        .write_to_config_dir(&cfg)
        .unwrap();
        let marker = dir.path().join("exec-ran");

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            for asked in [None, Some("remote"), Some("http")] {
                let (status, list) = post_mcp_as(&state, &cap, asked, tools_list())
                    .await
                    .unwrap();
                assert_eq!(status, StatusCode::OK);
                assert_eq!(
                    listed_tools(&list),
                    crate::mcp::REMOTE_MCP_TOOLS,
                    "{asked:?}"
                );
            }
            let (_, list) = post_mcp_as(&state, &cap, Some("coordination"), tools_list())
                .await
                .unwrap();
            assert_eq!(listed_tools(&list), crate::mcp::COORDINATION_MCP_TOOLS);
            for asked in ["full", "FULL", "local", "stdio"] {
                let refused = post_mcp_as(&state, &cap, Some(asked), tools_list())
                    .await
                    .unwrap_err();
                assert_eq!(refused.status, StatusCode::FORBIDDEN, "{asked}");
                assert!(
                    refused.message.contains("not served over HTTP"),
                    "{}",
                    refused.message
                );
            }
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {cap}").parse().unwrap(),
            );
            let refused = mcp_get_route(
                State(state.clone()),
                headers,
                Query(McpRouteQuery {
                    profile: Some("full".into()),
                }),
            )
            .await
            .unwrap_err();
            assert_eq!(refused.status, StatusCode::FORBIDDEN);

            // Nothing that executes or reads on the host is callable.
            let exec = serde_json::json!({
                "action": "exec",
                "command": format!("touch {}", marker.display()),
            });
            for (tool, args) in [
                ("files", exec.clone()),
                ("mcp__anyagents__files", exec.clone()),
                ("code", serde_json::json!({ "code": "return 1" })),
                ("browser", serde_json::json!({ "action": "status" })),
                ("computer", serde_json::json!({ "action": "screenshot" })),
                ("device", serde_json::json!({ "action": "screenshot" })),
                ("media", serde_json::json!({ "action": "speech" })),
                ("web", serde_json::json!({ "url": "https://example.com" })),
                ("web_search", serde_json::json!({ "query": "x" })),
                (
                    "web_fetch",
                    serde_json::json!({ "url": "https://example.com" }),
                ),
            ] {
                let (_, refused) = post_mcp_as(&state, &cap, None, tool_call(tool, args))
                    .await
                    .unwrap();
                assert!(
                    refused["error"]["message"]
                        .as_str()
                        .unwrap_or_default()
                        .contains("disabled by active MCP profile"),
                    "{tool}: {refused}"
                );
            }
            assert!(!marker.exists(), "an HTTP call ran a host command");
        });
    }

    #[test]
    fn calls_over_http_mcp_cannot_start_or_read_on_the_host() {
        let _env = env_lock();
        let dir = crate::test_tempdir();
        let state = archetype_validation_state(dir.path());
        std::fs::write(
            dir.path().join(crate::remote::CONFIG_FILE),
            "[server]\npublic_url = \"https://agents.example.com\"\n",
        )
        .unwrap();
        let local = state
            .store
            .create_agent(anyagents_core::NewAgent::new("http-local", "claude-code"))
            .unwrap();
        let local_cap = state.store.issue_capability(&local.id).unwrap();
        let cloud = state
            .store
            .create_agent(anyagents_core::NewAgent::new("http-cloud", "claude-cloud"))
            .unwrap();
        crate::remote::begin_session(&state.store, &cloud).unwrap();
        let remote =
            crate::remote::session_token(&anyagents_core::agent_config_dir(dir.path(), &cloud.id))
                .unwrap();
        let mut children = Vec::new();
        {
            let parent = &cloud;
            let child = state
                .store
                .create_agent(anyagents_core::NewAgent {
                    parent_id: Some(parent.id.clone()),
                    ..anyagents_core::NewAgent::new(
                        format!("{}-child", parent.name),
                        "claude-cloud",
                    )
                })
                .unwrap();
            let sleeper = state
                .store
                .create_agent(anyagents_core::NewAgent {
                    parent_id: Some(parent.id.clone()),
                    ..anyagents_core::NewAgent::new(
                        format!("{}-sleeper", parent.name),
                        "claude-code",
                    )
                })
                .unwrap();
            state
                .store
                .set_agent_state(&sleeper.id, "archived")
                .unwrap();
            children.push((child, sleeper));
        }
        let secret = dir.path().join("host-secret");
        std::fs::write(&secret, "not for a remote caller").unwrap();

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            // A local capability never reaches these tools over HTTP.
            let refused_local = post_mcp_as(
                &state,
                &local_cap,
                None,
                tool_call("agents", serde_json::json!({ "action": "list" })),
            )
            .await
            .unwrap_err();
            assert_eq!(refused_local.status, StatusCode::FORBIDDEN);
            {
                let token = &remote;
                let (child, sleeper) = &children[0];
                let call = |tool: &str, args: serde_json::Value| {
                    let state = state.clone();
                    let token = token.to_string();
                    let request = tool_call(tool, args);
                    async move {
                        post_mcp_as(&state, &token, None, request)
                            .await
                            .unwrap()
                            .1
                    }
                };
                let refused = |result: &serde_json::Value, why: &str| {
                    assert_eq!(result["result"]["isError"], true, "{result}");
                    assert!(result.to_string().contains(why), "{why}: {result}");
                };

                // Spawning a host-executed agent, through either tool.
                let prefix = token.chars().take(6).collect::<String>();
                let named = |n: &str| format!("host-{prefix}-{n}");
                let attempts = vec![
                    (
                        "agents",
                        serde_json::json!({ "action": "spawn", "name": named("a"), "harness": "claude-code" }),
                        named("a"),
                    ),
                    (
                        "spawn",
                        serde_json::json!({ "name": named("c"), "harness": "codex" }),
                        named("c"),
                    ),
                ];
                for (tool, args, name) in attempts {
                    refused(&call(tool, args).await, "only on a cloud harness");
                    assert!(state.store.agent_by_name(&name).is_err(), "{name}");
                }
                // Moving a child onto a host harness.
                refused(
                    &call(
                        "agents",
                        serde_json::json!({ "action": "harness", "name": child.name, "harness": "claude-code" }),
                    )
                    .await,
                    "only on a cloud harness",
                );
                assert_eq!(state.store.agent(&child.id).unwrap().harness, "claude-cloud");
                // Waking an archived host agent, explicitly or by a direct send.
                refused(
                    &call("wake", serde_json::json!({ "agent": sleeper.name })).await,
                    "cannot wake it",
                );
                let _ = call(
                    "send",
                    serde_json::json!({ "to": [sleeper.name], "text": "wake up" }),
                )
                .await;
                assert_eq!(state.store.agent(&sleeper.id).unwrap().state, "archived");
                // Attaching a host file as media, by path or by bare name.
                for source in [secret.display().to_string(), "operator-token".to_string()] {
                    refused(
                        &call(
                            "send",
                            serde_json::json!({
                                "to": [child.name],
                                "text": "here",
                                "media": { "kind": "document", "source": source },
                            }),
                        )
                        .await,
                        "by `https://` URL only",
                    );
                }
                // A URL attachment and an ordinary send still work.
                let sent = call(
                    "send",
                    serde_json::json!({
                        "to": [child.name],
                        "text": "with a link",
                        "media": { "kind": "photo", "source": "https://example.com/a.png" },
                    }),
                )
                .await;
                assert_ne!(sent["result"]["isError"], true, "{sent}");
                assert!(
                    state
                        .store
                        .inbox(&child.id, None)
                        .unwrap()
                        .iter()
                        .any(|message| message.text.contains("with a link"))
                );
            }
        });
    }

    /// The review's probe, kept as a regression: a seat plants links in the
    /// memory and skill directories it can write, then asks over HTTP MCP,
    /// where the tools run as the server. A local capability is refused
    /// outright; a remote session token's reads never follow the links.
    #[test]
    fn http_mcp_never_reads_a_link_a_seat_planted() {
        let _env = env_lock();
        let dir = crate::test_tempdir();
        let state = archetype_validation_state(dir.path());
        std::fs::write(
            dir.path().join(crate::remote::CONFIG_FILE),
            "[server]\npublic_url = \"https://agents.example.com\"\n",
        )
        .unwrap();
        let canary = "OPERATOR-SECRET-CANARY-7731";
        let secrets = dir.path().join("secrets");
        std::fs::create_dir_all(&secrets).unwrap();
        let secret = secrets.join("operator-token");
        std::fs::write(&secret, canary).unwrap();
        // Makes the linked directory look like a skill to discovery.
        std::fs::write(secrets.join("SKILL.md"), "---\nname: x\n---\n").unwrap();

        let seat = state
            .store
            .create_agent(anyagents_core::NewAgent::new("mem-seat", "claude-code"))
            .unwrap();
        let seat_cap = state.store.issue_capability(&seat.id).unwrap();
        let cloud = state
            .store
            .create_agent(anyagents_core::NewAgent::new("mem-cloud", "claude-cloud"))
            .unwrap();
        for agent in [&seat, &cloud] {
            let config = anyagents_core::agent_config_dir(dir.path(), &agent.id);
            std::fs::create_dir_all(&config).unwrap();
            let root = config.parent().unwrap().to_path_buf();
            let memory = root.join("memory");
            std::fs::create_dir_all(&memory).unwrap();
            std::os::unix::fs::symlink(&secret, memory.join("MEMORY.md")).unwrap();
            std::os::unix::fs::symlink(&secret, memory.join("leak.md")).unwrap();
            let overlay = root.join("skills").join("leaky");
            std::fs::create_dir_all(&overlay).unwrap();
            std::os::unix::fs::symlink(&secret, overlay.join("SKILL.md")).unwrap();
            let view = root.join("skills-view");
            std::fs::create_dir_all(&view).unwrap();
            std::os::unix::fs::symlink(&secrets, view.join("borrowed")).unwrap();
        }
        crate::remote::begin_session(&state.store, &cloud).unwrap();
        let remote =
            crate::remote::session_token(&anyagents_core::agent_config_dir(dir.path(), &cloud.id))
                .unwrap();

        let probes = [
            ("memory", serde_json::json!({ "action": "list" })),
            (
                "memory",
                serde_json::json!({ "action": "read", "topic": "leak" }),
            ),
            (
                "memory",
                serde_json::json!({ "action": "read", "topic": "MEMORY" }),
            ),
            ("memory", serde_json::json!({ "action": "status" })),
            ("skills", serde_json::json!({ "action": "list" })),
            (
                "skills",
                serde_json::json!({ "action": "read", "name": "leaky" }),
            ),
            (
                "skills",
                serde_json::json!({ "action": "read", "name": "borrowed", "file": "operator-token" }),
            ),
            (
                "skills",
                serde_json::json!({ "action": "search", "query": "CANARY" }),
            ),
        ];
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            for (tool, args) in &probes {
                let refused = post_mcp_as(&state, &seat_cap, None, tool_call(tool, args.clone()))
                    .await
                    .unwrap_err();
                assert_eq!(refused.status, StatusCode::FORBIDDEN, "{tool} {args}");
                assert!(!refused.message.contains(canary));

                let (status, body) =
                    post_mcp_as(&state, &remote, None, tool_call(tool, args.clone()))
                        .await
                        .unwrap();
                assert_eq!(status, StatusCode::OK, "{tool} {args}");
                assert!(
                    !body.to_string().contains(canary),
                    "{tool} {args} leaked: {body}"
                );
            }
        });
    }

    #[test]
    fn spiron_and_sylphx_share_compatible_harnesses_and_seed_auth_json_paths() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        let store = Store::open(data.join("state.db")).unwrap();
        let sylphx_profile = data.join("profiles/sylphx/default/.config/sylphx");
        std::fs::create_dir_all(&sylphx_profile).unwrap();
        std::fs::write(
            sylphx_profile.join("auth.json"),
            r#"{"access_token":"spiron-shared-token-123","org_id":"org_shared"}"#,
        )
        .unwrap();

        assert_eq!(
            crate::create::compatible_harnesses_for("spiron"),
            &["spiron", "spiron-cloud", "sylphx"]
        );
        assert_eq!(
            crate::create::compatible_harnesses_for("spiron-cloud"),
            &["spiron", "spiron-cloud", "sylphx"]
        );
        assert_eq!(
            crate::create::compatible_harnesses_for("sylphx"),
            &["spiron", "spiron-cloud", "sylphx"]
        );

        let agent = store
            .create_agent(anyagents_core::NewAgent::new("spiron-seat", "spiron"))
            .unwrap();
        let target_cfg = data.join("agents").join(agent.id.as_str());
        std::fs::create_dir_all(&target_cfg).unwrap();
        crate::create::seed_harness_credentials(&store, data, &agent, &target_cfg).unwrap();
        crate::spiron::write_spiron_config(
            &target_cfg,
            agent.model.as_deref(),
            Path::new("/usr/bin/anyagents"),
            &["mcp".to_string()],
        )
        .unwrap();

        for rel in ["auth.json", ".config/sylphx/auth.json", ".spiron/auth.json"] {
            let raw = std::fs::read_to_string(target_cfg.join(rel))
                .unwrap_or_else(|e| panic!("missing {rel}: {e}"));
            let val: serde_json::Value = serde_json::from_str(&raw).unwrap();
            assert_eq!(val["access_token"], "spiron-shared-token-123", "in {rel}");
        }
    }

    fn usage_fixture() -> (
        tempfile::TempDir,
        AppState,
        Agent,
        Agent,
        Agent,
        Agent,
        Agent,
    ) {
        let dir = crate::test_tempdir();
        let state = archetype_validation_state(dir.path());
        let root = state
            .store
            .create_agent(anyagents_core::NewAgent::new("usage-root", "opencode"))
            .unwrap();
        let mut cn = anyagents_core::NewAgent::new("usage-child", "opencode");
        cn.parent_id = Some(root.id.clone());
        let child = state.store.create_agent(cn).unwrap();
        let mut gn = anyagents_core::NewAgent::new("usage-grandchild", "opencode");
        gn.parent_id = Some(child.id.clone());
        let grandchild = state.store.create_agent(gn).unwrap();
        let mut sn = anyagents_core::NewAgent::new("usage-sibling", "opencode");
        sn.parent_id = Some(root.id.clone());
        let sibling = state.store.create_agent(sn).unwrap();
        let other_root = state
            .store
            .create_agent(anyagents_core::NewAgent::new(
                "usage-other-root",
                "opencode",
            ))
            .unwrap();
        for (agent, tokens) in [
            (&root, 100),
            (&child, 200),
            (&grandchild, 400),
            (&sibling, 800),
            (&other_root, 1600),
        ] {
            state
                .store
                .record_usage(anyagents_core::NewUsage {
                    agent_id: agent.id.clone(),
                    harness: "opencode".into(),
                    model: Some("m1".into()),
                    input_tokens: tokens,
                    output_tokens: tokens / 2,
                    cache_read_tokens: 1,
                    cache_write_tokens: 2,
                    reported_cost_micros: Some(tokens),
                    estimated_cost_micros: None,
                    billing_mode: anyagents_core::BillingMode::Subscription,
                    quota_state: None,
                    day: "2026-11-01".into(),
                })
                .unwrap();
        }
        (dir, state, root, child, grandchild, sibling, other_root)
    }

    async fn usage_q(
        state: AppState,
        principal: Principal,
        q: UsageQuery,
    ) -> Result<anyagents_core::UsageReport, ApiError> {
        usage_report(principal, State(state), Query(q))
            .await
            .map(|Json(r)| r)
    }

    #[tokio::test]
    async fn agents_report_to_the_operator_and_only_the_operator_reads_it() {
        let dir = crate::test_tempdir();
        let state = archetype_validation_state(dir.path());
        let root = state
            .store
            .create_agent(anyagents_core::NewAgent::new("report-root", "opencode"))
            .unwrap();
        state.store.set_agent_admin(&root.id, true).unwrap();
        let mut new = anyagents_core::NewAgent::new("report-child", "opencode");
        new.parent_id = Some(root.id.clone());
        let child = state.store.create_agent(new).unwrap();
        let as_agent = |agent: &Agent| Principal::Agent {
            agent_id: agent.id.clone(),
            name: agent.name.clone(),
        };
        let report = |principal: Principal, to: &[&str], text: &str| {
            send(
                principal,
                State(state.clone()),
                axum::http::HeaderMap::new(),
                Json(SendRequest {
                    to: to.iter().map(|to| to.to_string()).collect(),
                    text: text.to_string(),
                    ..SendRequest::default()
                }),
            )
        };
        let read = |principal: Principal, since: Option<i64>| {
            operator_reports(
                principal,
                State(state.clone()),
                Query(Since {
                    since,
                    limit: None,
                    unread: None,
                    sender: None,
                }),
            )
        };

        // Delivery: the report lands in the operator's inbox, keyed to the
        // sender's immutable id, and in the sender's own history.
        let text = "worktree add failed: EACCES on .git/worktrees; tried a fresh clone";
        let Json(sent) = report(as_agent(&child), &["operator"], text).await.unwrap();
        assert_eq!(sent.deliveries.len(), 1);
        let delivery = &sent.deliveries[0];
        assert_eq!(delivery.to, "operator");
        assert!(!delivery.duplicate);
        let Json(inbox_view) = read(Principal::Operator, None).await.unwrap();
        assert_eq!(inbox_view.messages.len(), 1);
        let stored = &inbox_view.messages[0];
        assert_eq!(stored.id, delivery.id);
        assert_eq!(stored.text, text);
        assert_eq!(stored.sender, format!("agent:{}", child.id));
        assert_eq!(stored.sender_name.as_deref(), Some("report-child"));
        let history = state
            .store
            .conversation(&child.id, Some("operator"), None, None)
            .unwrap();
        assert!(
            history
                .iter()
                .any(|turn| turn.direction == "out" && turn.text == text),
            "{history:?}"
        );
        assert!(
            read(Principal::Operator, Some(stored.seq))
                .await
                .unwrap()
                .0
                .messages
                .is_empty()
        );

        // An agent may only write: neither the operator route nor an agent
        // mailbox route reaches it, for a root admin too.
        for principal in [as_agent(&child), as_agent(&root)] {
            let err = read(principal.clone(), None).await.unwrap_err();
            assert_eq!(err.status, StatusCode::FORBIDDEN, "{}", err.message);
            let err = inbox(
                principal.clone(),
                State(state.clone()),
                AxumPath("operator".into()),
                Query(Since {
                    since: None,
                    limit: None,
                    unread: None,
                    sender: None,
                }),
            )
            .await
            .unwrap_err();
            assert_eq!(err.status, StatusCode::FORBIDDEN, "{}", err.message);
            let err = operator_inbox(
                principal,
                State(state.clone()),
                AxumPath("operator".into()),
                Query(Since {
                    since: None,
                    limit: None,
                    unread: None,
                    sender: None,
                }),
            )
            .await
            .unwrap_err();
            assert_ne!(err.status, StatusCode::OK, "{}", err.message);
        }

        // Deduplication: the same report again is the stored one.
        let Json(again) = report(as_agent(&child), &["operator"], text).await.unwrap();
        assert!(again.deliveries[0].duplicate);
        assert_eq!(again.deliveries[0].id, delivery.id);
        let Json(counted) = read(Principal::Operator, None).await.unwrap();
        assert_eq!(counted.messages[0].repeats, 1);

        // Rate limit: after the burst the sender is refused with 429, before
        // any other address of the same send is written.
        for i in 1..anyagents_core::OPERATOR_BURST {
            let Json(sent) = report(as_agent(&child), &["operator"], &format!("defect {i}"))
                .await
                .unwrap();
            assert!(!sent.deliveries[0].duplicate);
        }
        let parent_mail = state.store.inbox(&root.id, None).unwrap().len();
        let err = report(as_agent(&child), &["parent", "operator"], "one more")
            .await
            .unwrap_err();
        assert_eq!(err.status, StatusCode::TOO_MANY_REQUESTS, "{}", err.message);
        assert!(err.message.contains("Retry in"), "{}", err.message);
        assert_eq!(
            state.store.inbox(&root.id, None).unwrap().len(),
            parent_mail
        );
        // Another sender has its own limit.
        let Json(other) = report(as_agent(&root), &["operator"], "one more")
            .await
            .unwrap();
        assert!(!other.deliveries[0].duplicate);
        let Json(all) = read(Principal::Operator, None).await.unwrap();
        assert_eq!(
            all.messages.len(),
            usize::try_from(anyagents_core::OPERATOR_BURST).unwrap() + 1
        );
    }

    #[test]
    fn concurrent_reports_never_leave_a_mixed_send_half_delivered() {
        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let parent = store
            .create_agent(anyagents_core::NewAgent::new("race-parent", "opencode"))
            .unwrap();
        let mut new = anyagents_core::NewAgent::new("race-child", "opencode");
        new.parent_id = Some(parent.id.clone());
        store.create_agent(new).unwrap();
        let senders = 8;
        let barrier = Arc::new(std::sync::Barrier::new(senders));
        let workers: Vec<_> = (0..senders)
            .map(|i| {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let actor = Actor::Agent("race-child".into());
                    let body = SendRequest {
                        sender: Some(actor.as_address()),
                        to: vec!["parent".into(), "operator".into()],
                        text: format!("defect {i}"),
                        ..SendRequest::default()
                    };
                    let plan = SendPlan::parse(&store, &actor, &body).unwrap();
                    barrier.wait();
                    send_sync(&store, &actor, body, plan, None)
                        .map(|_| ())
                        .map_err(|err| err.status)
                })
            })
            .collect();
        let results: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
        assert!(
            results
                .iter()
                .all(|r| matches!(r, Ok(()) | Err(StatusCode::TOO_MANY_REQUESTS))),
            "{results:?}"
        );
        let sent = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(
            sent,
            usize::try_from(anyagents_core::OPERATOR_BURST).unwrap()
        );
        // Every refused send wrote nothing: the parent got exactly the sends
        // the operator inbox took.
        assert_eq!(store.operator_inbox(None, None).unwrap().len(), sent);
        assert_eq!(store.inbox(&parent.id, None).unwrap().len(), sent);
    }

    async fn http(
        socket: &Path,
        method: &str,
        path: &str,
        body: Option<serde_json::Value>,
        capability: Option<&str>,
        operator: Option<&str>,
    ) -> (hyper::StatusCode, serde_json::Value) {
        let body = body.map(|body| serde_json::to_vec(&body).unwrap());
        let (status, bytes) = crate::mcp::request(socket, method, path, body, capability, operator)
            .await
            .unwrap();
        let value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, value)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn over_http_a_report_is_capped_at_16_kib_and_only_the_operator_token_reads_it() {
        let dir = crate::test_tempdir();
        let mut state = archetype_validation_state(dir.path());
        let operator_token = "operator-token-0123456789abcdef0123456789abcdef";
        state.auth = auth::Mode::Confined {
            operator_digest: Some(auth::operator_digest(operator_token)),
        };
        let parent = state
            .store
            .create_agent(anyagents_core::NewAgent::new("http-parent", "opencode"))
            .unwrap();
        let mut new = anyagents_core::NewAgent::new("http-child", "opencode");
        new.parent_id = Some(parent.id.clone());
        let child = state.store.create_agent(new).unwrap();
        let capability = state.store.issue_capability(&child.id).unwrap();
        let socket = dir.path().join("operator-inbox.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let app = build_router(state.clone());
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let report =
            |text: String| serde_json::json!({ "to": ["parent", "operator"], "text": text });

        // One byte over the cap is refused before any target is written.
        let max = 16 * 1024;
        let (status, refused) = http(
            &socket,
            "POST",
            "/v1/send",
            Some(report("x".repeat(max + 1))),
            Some(&capability),
            None,
        )
        .await;
        assert_eq!(status, hyper::StatusCode::BAD_REQUEST, "{refused}");
        assert!(
            refused["error"].as_str().unwrap().contains("16 KiB"),
            "{refused}"
        );
        assert!(state.store.inbox(&parent.id, None).unwrap().is_empty());
        assert!(state.store.operator_inbox(None, None).unwrap().is_empty());
        // Exactly the cap is delivered to both.
        let (status, sent) = http(
            &socket,
            "POST",
            "/v1/send",
            Some(report("y".repeat(max))),
            Some(&capability),
            None,
        )
        .await;
        assert_eq!(status, hyper::StatusCode::OK, "{sent}");
        assert_eq!(sent["deliveries"].as_array().unwrap().len(), 2, "{sent}");

        // The agent's capability, no credential and a wrong token cannot read.
        for (capability, operator, expected) in [
            (
                Some(capability.as_str()),
                None,
                hyper::StatusCode::FORBIDDEN,
            ),
            (None, None, hyper::StatusCode::UNAUTHORIZED),
            (None, Some("not-the-token"), hyper::StatusCode::UNAUTHORIZED),
        ] {
            let (status, body) = http(
                &socket,
                "GET",
                "/v1/operator/inbox",
                None,
                capability,
                operator,
            )
            .await;
            assert_eq!(status, expected, "{body}");
        }
        let (status, read) = http(
            &socket,
            "GET",
            "/v1/operator/inbox?limit=5",
            None,
            None,
            Some(operator_token),
        )
        .await;
        assert_eq!(status, hyper::StatusCode::OK, "{read}");
        let messages = read["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 1, "{read}");
        assert_eq!(messages[0]["text"].as_str().unwrap().len(), max);
        assert_eq!(messages[0]["sender_name"], "http-child");
        server.abort();
    }

    #[tokio::test]
    async fn verdicts_need_an_ancestor_and_outcomes_follow_the_usage_boundary() {
        let (_dir, state, root, child, _grandchild, sibling, other_root) = usage_fixture();
        let as_agent = |agent: &Agent| Principal::Agent {
            agent_id: agent.id.clone(),
            name: agent.name.clone(),
        };
        let judge = |principal: Principal, target: &str, verdict: &str| {
            verdict_route(
                principal,
                State(state.clone()),
                AxumPath(target.to_string()),
                Json(VerdictRequest {
                    verdict: verdict.to_string(),
                    reason: Some("checked against the brief".to_string()),
                }),
            )
        };
        // An agent never judges its own work; peers and other roots cannot.
        for (principal, target) in [
            (as_agent(&child), "usage-child"),
            (as_agent(&sibling), "usage-child"),
            (as_agent(&other_root), "usage-child"),
        ] {
            let err = judge(principal, target, "accepted").await.unwrap_err();
            assert_eq!(err.status, StatusCode::FORBIDDEN, "{}", err.message);
        }
        let err = judge(Principal::Operator, "usage-child", "pr_merged")
            .await
            .unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        // Any strict ancestor may judge; the operator may judge anyone.
        let Json(rework) = judge(as_agent(&root), "usage-grandchild", "rework")
            .await
            .unwrap();
        assert_eq!(rework.kind, anyagents_core::OutcomeKind::Rework);
        assert_eq!(rework.actor, "agent:usage-root");
        assert_eq!(rework.note.as_deref(), Some("checked against the brief"));
        let Json(accepted) = judge(as_agent(&child), "usage-grandchild", "Accepted")
            .await
            .unwrap();
        assert_eq!(accepted.kind, anyagents_core::OutcomeKind::Accepted);
        let Json(rejected) = judge(Principal::Operator, "usage-sibling", "rejected")
            .await
            .unwrap();
        assert_eq!(rejected.actor, "operator");

        let report = |principal: Principal, query: UsageQuery| {
            outcome_report(principal, State(state.clone()), Query(query))
        };
        let by_agent = || UsageQuery {
            group_by: Some("agent".to_string()),
            ..UsageQuery::default()
        };
        let Json(all) = report(Principal::Operator, by_agent()).await.unwrap();
        let rows: Vec<(&str, u64, u64, u64)> = all
            .groups
            .iter()
            .map(|group| {
                (
                    group.agent_name.as_deref().unwrap(),
                    group.succeeded,
                    group.failed,
                    group.recovered,
                )
            })
            .collect();
        assert_eq!(
            rows,
            vec![("usage-grandchild", 1, 0, 1), ("usage-sibling", 0, 1, 0)]
        );
        // Each judged agent's own usage is its cost; the rest is unattributed.
        assert_eq!(
            all.groups
                .iter()
                .map(|group| group.cost.reported_cost_micros)
                .sum::<i64>(),
            400 + 800
        );
        assert_eq!(all.unattributed.reported_cost_micros, 100 + 200 + 1600);
        // A child sees only its subtree, and cannot widen it.
        let Json(mine) = report(as_agent(&child), by_agent()).await.unwrap();
        assert_eq!(mine.groups.len(), 1);
        assert_eq!(
            mine.groups[0].agent_name.as_deref(),
            Some("usage-grandchild")
        );
        assert_eq!(mine.unattributed.reported_cost_micros, 200);
        let err = report(
            as_agent(&child),
            UsageQuery {
                agent: Some("usage-sibling".to_string()),
                ..UsageQuery::default()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::FORBIDDEN);
        let err = report(
            Principal::Operator,
            UsageQuery {
                group_by: Some("day".to_string()),
                ..UsageQuery::default()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn skill_reads_are_recorded_only_for_the_calling_agent() {
        let (_dir, state, root, ..) = usage_fixture();
        let read = |principal: Principal, file: &str| {
            skill_read_route(
                principal,
                State(state.clone()),
                Json(SkillReadRequest {
                    skill: "prove-it".to_string(),
                    file: file.to_string(),
                    sha256: "d".repeat(64),
                }),
            )
        };
        let as_root = || Principal::Agent {
            agent_id: root.id.clone(),
            name: root.name.clone(),
        };
        let Json(recorded) = read(as_root(), "SKILL.md").await.unwrap();
        assert_eq!(recorded["recorded"], true);
        let err = read(as_root(), "../escape").await.unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        let err = read(Principal::Operator, "SKILL.md").await.unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn usage_rpc_enforces_causal_authorization_boundary() {
        let (_dir, state, root, child, grandchild, sibling, _other) = usage_fixture();
        let op = Principal::Operator;
        // Operator: whole workspace, explicit sibling, `all` all allowed.
        let r = usage_q(state.clone(), op.clone(), UsageQuery::default())
            .await
            .unwrap();
        assert_eq!(r.totals.turns, 5);
        assert_eq!(r.totals.input_tokens, 3100);
        let r = usage_q(
            state.clone(),
            op.clone(),
            UsageQuery {
                agent: Some("usage-sibling".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(r.totals.input_tokens, 800);
        let r = usage_q(
            state.clone(),
            op.clone(),
            UsageQuery {
                agent: Some("all".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(r.totals.turns, 5);
        // Operator + subtree flag on root child widens to its subtree.
        let r = usage_q(
            state.clone(),
            op.clone(),
            UsageQuery {
                agent: Some("usage-child".into()),
                include_descendants: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(r.totals.input_tokens, 600);
        // Root caller (parent: null) is privileged like the operator.
        let root_p = Principal::Agent {
            agent_id: root.id.clone(),
            name: root.name.clone(),
        };
        let r = usage_q(
            state.clone(),
            root_p,
            UsageQuery {
                agent: Some("usage-sibling".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(r.totals.input_tokens, 800);
        // Private child: omitted target = self subtree (child + grandchild).
        let child_p = Principal::Agent {
            agent_id: child.id.clone(),
            name: child.name.clone(),
        };
        let r = usage_q(state.clone(), child_p.clone(), UsageQuery::default())
            .await
            .unwrap();
        assert_eq!(r.totals.input_tokens, 600);
        assert_eq!(r.totals.turns, 2);
        // Explicit descendant allowed; subtree flag widens to self subtree.
        let r = usage_q(
            state.clone(),
            child_p.clone(),
            UsageQuery {
                agent: Some("usage-grandchild".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(r.totals.input_tokens, 400);
        // Parent, sibling, `all` and unrelated roots are fail-closed.
        for target in ["usage-root", "usage-sibling", "all", "usage-other-root"] {
            let err = usage_q(
                state.clone(),
                child_p.clone(),
                UsageQuery {
                    agent: Some(target.into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
            assert_eq!(err.status, StatusCode::FORBIDDEN, "target {target}");
        }
        // Unknown target is a client error, never a leak.
        let err = usage_q(
            state.clone(),
            child_p.clone(),
            UsageQuery {
                agent: Some("nope".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert!(err.status.is_client_error());
        // Public-scope caller regains the whole workspace.
        state.store.set_agent_scope(&child.id, "public").unwrap();
        let r = usage_q(
            state.clone(),
            child_p.clone(),
            UsageQuery {
                agent: Some("usage-sibling".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(r.totals.input_tokens, 800);
        let r = usage_q(
            state.clone(),
            child_p.clone(),
            UsageQuery {
                agent: Some("all".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(r.totals.turns, 5);
        let _ = (grandchild, sibling);
    }

    #[tokio::test]
    async fn usage_rpc_grouping_and_window_accuracy() {
        let (_dir, state, _root, _child, _gc, _sib, _other) = usage_fixture();
        let op = Principal::Operator;
        for group_by in ["agent", "day", "model", "harness"] {
            let r = usage_q(
                state.clone(),
                op.clone(),
                UsageQuery {
                    group_by: Some(group_by.into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            assert!(!r.buckets.is_empty(), "group_by {group_by}");
            assert_eq!(
                r.buckets.iter().map(|b| b.totals.turns).sum::<i64>(),
                5,
                "bucket turns sum for {group_by}"
            );
            assert_eq!(r.totals.turns, 5);
            assert_eq!(r.totals.input_tokens, 3100);
            assert_eq!(r.totals.output_tokens, 1550);
            assert_eq!(r.totals.cache_read_tokens, 5);
            assert_eq!(r.totals.cache_write_tokens, 10);
            assert_eq!(r.totals.reported_cost_micros, Some(3100));
        }
        // Time windows: far-future lower bound excludes everything, days=36500 keeps all.
        let r = usage_q(
            state.clone(),
            op.clone(),
            UsageQuery {
                since_ms: Some(anyagents_core::now_ms() + 86_400_000),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(r.totals.turns, 0);
        let r = usage_q(
            state.clone(),
            op.clone(),
            UsageQuery {
                days: Some(36500),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(r.totals.turns, 5);
        let r = usage_q(
            state.clone(),
            op.clone(),
            UsageQuery {
                until_ms: Some(0),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(r.totals.turns, 0);
        // Invalid grouping / inverted window are client errors.
        let err = usage_q(
            state.clone(),
            op.clone(),
            UsageQuery {
                group_by: Some("bogus".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        let err = usage_q(
            state.clone(),
            op.clone(),
            UsageQuery {
                since_ms: Some(2),
                until_ms: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn mcp_usage_action_forwards_to_daemon_usage_route() {
        // The MCP tool serializes the same query the daemon route accepts:
        // build the query string the way the dispatch does and confirm the
        // route parses it (group_by + window + subtree).
        let (_dir, state, _root, _child, _gc, _sib, _other) = usage_fixture();
        let r = usage_q(
            state.clone(),
            Principal::Operator,
            UsageQuery {
                agent: Some("usage-child".into()),
                include_descendants: Some(true),
                group_by: Some("day".into()),
                days: Some(36500),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(r.totals.input_tokens, 600);
        assert_eq!(r.buckets.len(), 2);
    }
}

#[cfg(all(test, target_os = "linux"))]
#[path = "server_history_benchmark.rs"]
mod history_benchmark;
