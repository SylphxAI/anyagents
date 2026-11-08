//! HTTP/JSON mailbox API on a user-only unix socket.
//!
//! Every call is authenticated before it is routed: an agent instance presents
//! the mailbox capability the server minted for it, and the operator presents
//! its own token. A sender, an inbox, a bot or an agent name in a body is a
//! destination, never an identity.

use std::io::{BufRead, BufReader};
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use anyagents_core::{Address, Agent, Error, Incoming, ReceiveOutcome, Store, now_ms};
use anyagents_gateway::MemoryRoutes;
use anyagents_installer::{CommandRunner, InstallError, LineSink, builtin_catalog, sign_in};
use anyagents_supervisor::{HealthBoard, Supervisor, SystemClock, declared, mode_name, steer_name};
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
    InteractResponse, MessageView, SendRequest, SendResponse,
};
use crate::auth::{self, Principal};
use crate::create::{self, CreateRequest};
use crate::identity::ServingIdentity;
use crate::operator;
use crate::vendor::{self, VendorHarness};

mod outbound_history;

const SOCKET_NAME: &str = "anyagents.sock";
/// `sockaddr_un.sun_path` holds 108 bytes, including the trailing NUL.
const MAX_SOCKET_PATH: usize = 107;

pub fn socket_path(data: &Path) -> PathBuf {
    data.join(SOCKET_NAME)
}

pub fn run(data: &Path) -> Result<(), Error> {
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    run_with(data, Arc::new(Mutex::new(ProcessRunner)), stop_rx)
}

/// Run the server with a sign-in runner the caller supplies. Tests use this so
/// no vendor command is started. `stop` ends the server when the caller drops it.
pub fn run_with(
    data: &Path,
    runner: Arc<Mutex<dyn CommandRunner + Send>>,
    stop: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), Error> {
    run_with_identity(data, runner, stop, Arc::new(ServingIdentity::capture()))
}

fn run_with_identity(
    data: &Path,
    runner: Arc<Mutex<dyn CommandRunner + Send>>,
    mut stop: tokio::sync::oneshot::Receiver<()>,
    identity: Arc<ServingIdentity>,
) -> Result<(), Error> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        tokio::select! {
            result = serve(data, runner, identity) => result,
            _ = &mut stop => Ok(()),
        }
    })
}

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) store: Arc<Store>,
    pub(crate) health: HealthBoard,
    pub(crate) sign_in: Arc<Mutex<dyn CommandRunner + Send>>,
    pub(crate) auth: auth::Mode,
    pub(crate) identity: Arc<ServingIdentity>,
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

async fn serve(
    data: &Path,
    sign_in: Arc<Mutex<dyn CommandRunner + Send>>,
    identity: Arc<ServingIdentity>,
) -> Result<(), Error> {
    let _server_lock = acquire_server_lock(data)?;
    crate::worktree_access::recover_inspections(data)?;
    let store = Arc::new(Store::open(data.join("state.db"))?);
    crate::sandbox::reconcile_principals(&store)?;
    crate::host_memory::on_start(&store);
    let key = capability_key()?;
    store.hold_capability_key(&key)?;
    let agents = store.list_agents()?;
    let mut mint_failures = Vec::new();
    for agent in &agents {
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
    let seats: Vec<String> = store
        .list_agents()?
        .into_iter()
        .map(|agent| agent.id.to_string())
        .collect();
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
    crate::gateway::start(data)?;
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
    let supervisor = Supervisor::<VendorHarness>::start(
        Arc::clone(&store),
        Arc::new(SystemClock::new()),
        Arc::new(move |request| vendor::vendor_launch_with_store(request, &launch_store)),
        vendor::child_env(),
    )
    .map_err(Error::Message)?;
    if auth.is_confined() {
        crate::sandbox::share_socket_with_seats(&data.join("wake.sock"));
    }
    let state = AppState {
        store: Arc::clone(&store),
        health: supervisor.board(),
        sign_in,
        auth,
        identity,
    };
    let _ = boundary;
    let listener = api_listener;
    let poller = crate::telegram::Poller::start(Arc::clone(&store), data.to_path_buf())
        .map_err(Error::Message)?;
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_none() && !crate::sandbox::is_isolated_test_env()
    {
        let reaper_root = data.to_path_buf();
        let reaper_store = Arc::clone(&store);
        let reaper_health = state.health.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            interval.tick().await;
            let mut tick_count = 0u64;
            loop {
                interval.tick().await;
                tick_count = tick_count.wrapping_add(1);
                let run_verify = tick_count.is_multiple_of(15);
                let root = reaper_root.clone();
                let store = Arc::clone(&reaper_store);
                let health = reaper_health.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    reap_archived_agents(&store, &root, &health);
                    consolidate_idle_agent_memory(&store, &root, &health);
                    if run_verify
                        && let Err(error) =
                            store.verify_one_host_history_source(anyagents_core::now_ms())
                    {
                        eprintln!("anyagents-host-history: deferred full verification: {error}");
                    }
                    crate::browser::reap_idle_browsers(&root);
                    crate::create::sync_fresher_bound_agent_credentials(&root);
                    #[cfg(unix)]
                    crate::create::sync_canonical_skills(&root);
                })
                .await;
            }
        });
        let updater_root = data.to_path_buf();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(6 * 3600));
            loop {
                interval.tick().await;
                let root = updater_root.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    for entry in anyagents_installer::builtin_catalog() {
                        if let Ok((before, after, true)) =
                            crate::install::update_harness(&root, &entry.id)
                        {
                            eprintln!(
                                "anyagents-updater: upgraded harness `{}` {} -> {}",
                                entry.id, before, after
                            );
                        }
                    }
                })
                .await;
            }
        });
        // The primary serves scheduling only when no administrative scheduler owns it.
        start_schedule_loops(Arc::clone(&store), state.health.clone(), true);
    }
    if let Some(addr) = webhook_listen_addr() {
        match tokio::net::TcpListener::bind(&addr).await {
            Ok(tcp_listener) => {
                if let Ok(local) = tcp_listener.local_addr() {
                    let _ = std::fs::write(data.join("webhook.addr"), format!("{local}\n"));
                }
                let webhook_app = Router::new()
                    .route("/", get(webhook_healthz))
                    .route("/healthz", get(webhook_healthz))
                    .route("/v1/webhooks/healthz", get(webhook_healthz))
                    .route("/v1/webhooks/telegram/{channel_id}", post(telegram_webhook))
                    .with_state(state.clone());
                tokio::spawn(async move {
                    let _ = axum::serve(tcp_listener, webhook_app).await;
                });
            }
            Err(err) => {
                eprintln!("warning: webhook TCP listener ({addr}) did not bind: {err}");
            }
        }
    }
    let app = Router::new()
        .route("/v1/agents", get(list_agents).post(create_agent_route))
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
        .route("/v1/whoami", get(whoami))
        .route("/v1/webhooks/healthz", get(webhook_healthz))
        .route("/v1/webhooks/telegram/{channel_id}", post(telegram_webhook))
        .with_state(state);
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

/// Serve the authenticated HTTP API on `<data>/admin.sock` without starting a
/// second `Supervisor` or `Poller`. Allows newly shipped API routes (such as
/// `POST /v1/agents/{name}/harness`) to answer authenticated callers across the
/// OS seat boundary (`anyagents-sock`) while an existing `anyagents serve`
/// daemon keeps running uninterrupted on `anyagents.sock`.
#[derive(Default)]
struct AdminReconcileState {
    initialized: bool,
    watermark_seq: std::collections::HashMap<String, i64>,
    last_backoff_reset_ms: std::collections::HashMap<String, i64>,
    flushing_sylphx: std::collections::HashSet<String>,
}

fn is_transient_sylphx_turn_error(err: &str) -> bool {
    let lower = err.to_ascii_lowercase();
    if lower.contains("\"retryable\":false") || lower.contains("\"retryable\": false") {
        return false;
    }
    if lower.contains("insufficient_quota")
        || lower.contains("credit balance is too low")
        || lower.contains("out of credits")
        || lower.contains("quota exceeded")
    {
        return false;
    }
    lower.contains("model_unavailable")
        || lower.contains("included_capacity_unavailable")
        || lower.contains("temporarily unavailable")
        || lower.contains("overloaded")
        || lower.contains("server_error")
        || lower.contains("api_error")
        || lower.contains("rate_limit")
        || lower.contains("\"retryable\":true")
        || lower.contains("\"retryable\": true")
        || lower.contains("http 408")
        || lower.contains("http 409")
        || lower.contains("http 425")
        || lower.contains("http 429")
        || lower.contains("http 500")
        || lower.contains("http 502")
        || lower.contains("http 503")
        || lower.contains("http 504")
        || lower.contains("invalid sylphx responses api json")
        || lower.contains("eof while parsing")
        || lower.contains("bad gateway")
        || lower.contains("service unavailable")
        || lower.contains("gateway timeout")
        || lower.contains("connection reset")
        || lower.contains("timed out")
}

fn spawn_admin_sylphx_delivery(
    data: PathBuf,
    agent: Agent,
    health: HealthBoard,
    reconcile: Arc<std::sync::Mutex<AdminReconcileState>>,
) {
    std::thread::spawn(move || {
        let store = match Store::open(data.join("state.db")) {
            Ok(s) => s,
            Err(_) => {
                let mut guard = match reconcile.lock() {
                    Ok(g) => g,
                    Err(poisoned) => poisoned.into_inner(),
                };
                guard.flushing_sylphx.remove(agent.id.as_str());
                return;
            }
        };
        let _agent_lock = match store.try_lock_agent(&agent.id) {
            Ok(lock) => lock,
            Err(_) => {
                let mut guard = match reconcile.lock() {
                    Ok(g) => g,
                    Err(poisoned) => poisoned.into_inner(),
                };
                guard.flushing_sylphx.remove(agent.id.as_str());
                return;
            }
        };
        let (delivery_mode, steer_support) = declared(&agent.harness);
        let cfg = anyagents_core::agent_config_dir(&data, &agent.id);
        let work_dir = agent
            .workdir
            .as_deref()
            .map(PathBuf::from)
            .unwrap_or_else(|| anyagents_core::agent_work_dir(&data, &agent.id));
        health.publish(
            &agent.id,
            anyagents_supervisor::LiveState {
                process_alive: true,
                turn_state: anyagents_harness::TurnState::InTurn,
                delivery_mode,
                steer_support,
                quota_reset_at_ms: None,
                restart_at_ms: None,
                pid: Some(std::process::id()),
                live_harness: Some(agent.harness.clone()),
                live_model: agent.model.clone(),
                process_started_at_ms: Some(now_ms()),
                last_error: None,
            },
        );
        let delivery_result = (|| -> Result<(), Error> {
            std::fs::create_dir_all(&work_dir)?;
            // Creation and explicit wakes own grants; never follow a child's
            // editable .git pointer to expand them during a harness launch.
            let req = anyagents_supervisor::LaunchRequest {
                agent: agent.clone(),
                data_dir: data.clone(),
                config_dir: cfg.clone(),
                work_dir,
                extra_env: Vec::new(),
            };
            match crate::vendor::vendor_launch(req) {
                Ok(mut harness) => {
                    let mut had_err = false;
                    let mut completed_any = false;
                    let mut recorded_usage = anyagents_harness::TurnUsage::default();
                    let flush_harness_usage = |harness: &crate::vendor::VendorHarness,
                                               recorded: &mut anyagents_harness::TurnUsage|
                     -> Result<(), Error> {
                        let served = anyagents_harness::Harness::last_usage(harness);
                        let diff = served.difference(recorded.clone());
                        if diff.input_tokens > 0 || diff.output_tokens > 0 {
                            store.record_usage(anyagents_core::NewUsage {
                                agent_id: agent.id.clone(),
                                harness: agent.harness.clone(),
                                model: agent.model.clone(),
                                input_tokens: i64::try_from(diff.input_tokens).unwrap_or(i64::MAX),
                                output_tokens: i64::try_from(diff.output_tokens)
                                    .unwrap_or(i64::MAX),
                                cache_read_tokens: i64::try_from(diff.cache_read_tokens)
                                    .unwrap_or(i64::MAX),
                                cache_write_tokens: i64::try_from(diff.cache_write_tokens)
                                    .unwrap_or(i64::MAX),
                                reported_cost_micros: diff.reported_cost_micros,
                                estimated_cost_micros: None,
                                billing_mode: anyagents_core::BillingMode::Subscription,
                                quota_state: None,
                                day: anyagents_core::utc_day(now_ms()),
                            })?;
                            *recorded = served;
                        }
                        Ok(())
                    };
                    while store.has_actionable_undelivered(&agent.id).unwrap_or(false) {
                        let Ok(Some(message)) = store.next_undelivered(&agent.id) else {
                            break;
                        };
                        let outbound = anyagents_harness::OutboundMessage {
                            id: message.id.as_str().to_string(),
                            seq: message.seq,
                            text: message.text.clone(),
                            sender: message.sender.clone(),
                            reply_to: message.reply_to.clone(),
                            source_time_ms: message.source_time_ms,
                            context: message.context.clone(),
                        };
                        match anyagents_harness::Harness::deliver(&mut harness, &outbound) {
                            Ok(anyagents_harness::DeliverOutcome::Accepted { turn_finished }) => {
                                store.note_delivered(&agent.id, message.seq)?;
                                if !turn_finished {
                                    while anyagents_harness::Harness::health(&harness).turn_state
                                        == anyagents_harness::TurnState::InTurn
                                    {
                                        if let Ok(Some(steered)) = store.next_undelivered(&agent.id)
                                        {
                                            let steered_out = anyagents_harness::OutboundMessage {
                                                id: steered.id.as_str().to_string(),
                                                seq: steered.seq,
                                                text: steered.text.clone(),
                                                sender: steered.sender.clone(),
                                                reply_to: steered.reply_to.clone(),
                                                source_time_ms: steered.source_time_ms,
                                                context: steered.context.clone(),
                                            };
                                            if anyagents_harness::Harness::deliver(
                                                &mut harness,
                                                &steered_out,
                                            )
                                            .is_ok()
                                            {
                                                store.note_delivered(&agent.id, steered.seq)?;
                                            }
                                        }
                                        std::thread::sleep(std::time::Duration::from_millis(100));
                                    }
                                }
                                flush_harness_usage(&harness, &mut recorded_usage)?;
                                let turn_err = anyagents_harness::Harness::health(&harness)
                                    .sign_in_fault
                                    .or_else(|| {
                                        crate::vendor::read_persisted_last_error(&cfg)
                                            .and_then(|(e, _)| e)
                                    });
                                if let Some(err_msg) = turn_err {
                                    had_err = true;
                                    let can_auto_recover = is_transient_sylphx_turn_error(&err_msg)
                                        && store.open_turn(&agent.id).ok().flatten().is_some();
                                    if can_auto_recover {
                                        crate::vendor::persist_last_error(&cfg, None, None);
                                    } else {
                                        crate::vendor::persist_last_error(
                                            &cfg,
                                            Some(&err_msg),
                                            None,
                                        );
                                        create::maybe_notify_parent_on_error(
                                            &store, &agent, &cfg, &err_msg,
                                        );
                                    }
                                    break;
                                }
                                store.note_turn_completed(&agent.id)?;
                                completed_any = true;
                                crate::vendor::persist_last_error(&cfg, None, None);
                            }
                            Ok(_) => {
                                flush_harness_usage(&harness, &mut recorded_usage)?;
                                store.note_turn_completed(&agent.id)?;
                                break;
                            }
                            Err(err) => {
                                had_err = true;
                                let err_msg = err.to_string();
                                let can_auto_recover = is_transient_sylphx_turn_error(&err_msg)
                                    && store.open_turn(&agent.id).ok().flatten().is_some();
                                if can_auto_recover {
                                    crate::vendor::persist_last_error(&cfg, None, None);
                                } else {
                                    crate::vendor::persist_last_error(&cfg, Some(&err_msg), None);
                                    create::maybe_notify_parent_on_error(
                                        &store, &agent, &cfg, &err_msg,
                                    );
                                }
                                break;
                            }
                        }
                    }
                    if !had_err
                        && !completed_any
                        && let Ok(Some(open)) = store.open_turn(&agent.id)
                    {
                        let outbound = anyagents_supervisor::recovery_outbound(&open);
                        match anyagents_harness::Harness::deliver(&mut harness, &outbound) {
                            Ok(anyagents_harness::DeliverOutcome::Accepted { turn_finished }) => {
                                store.note_turn_recovered(&agent.id, open.opened_seq)?;
                                if !turn_finished {
                                    while anyagents_harness::Harness::health(&harness).turn_state
                                        == anyagents_harness::TurnState::InTurn
                                    {
                                        if let Ok(Some(steered)) = store.next_undelivered(&agent.id)
                                        {
                                            let steered_out = anyagents_harness::OutboundMessage {
                                                id: steered.id.as_str().to_string(),
                                                seq: steered.seq,
                                                text: steered.text.clone(),
                                                sender: steered.sender.clone(),
                                                reply_to: steered.reply_to.clone(),
                                                source_time_ms: steered.source_time_ms,
                                                context: steered.context.clone(),
                                            };
                                            if anyagents_harness::Harness::deliver(
                                                &mut harness,
                                                &steered_out,
                                            )
                                            .is_ok()
                                            {
                                                store.note_delivered(&agent.id, steered.seq)?;
                                            }
                                        }
                                        std::thread::sleep(std::time::Duration::from_millis(100));
                                    }
                                }
                                flush_harness_usage(&harness, &mut recorded_usage)?;
                                let turn_err = anyagents_harness::Harness::health(&harness)
                                    .sign_in_fault
                                    .or_else(|| {
                                        crate::vendor::read_persisted_last_error(&cfg)
                                            .and_then(|(e, _)| e)
                                    });
                                if let Some(err_msg) = turn_err {
                                    had_err = true;
                                    let can_auto_recover = is_transient_sylphx_turn_error(&err_msg)
                                        && store.open_turn(&agent.id).ok().flatten().is_some();
                                    if can_auto_recover {
                                        crate::vendor::persist_last_error(&cfg, None, None);
                                    } else {
                                        crate::vendor::persist_last_error(
                                            &cfg,
                                            Some(&err_msg),
                                            None,
                                        );
                                        create::maybe_notify_parent_on_error(
                                            &store, &agent, &cfg, &err_msg,
                                        );
                                    }
                                } else {
                                    store.note_turn_completed(&agent.id)?;
                                    completed_any = true;
                                    crate::vendor::persist_last_error(&cfg, None, None);
                                }
                            }
                            Ok(_) => {
                                flush_harness_usage(&harness, &mut recorded_usage)?;
                            }
                            Err(err) => {
                                had_err = true;
                                let err_msg = err.to_string();
                                let can_auto_recover = is_transient_sylphx_turn_error(&err_msg)
                                    && store.open_turn(&agent.id).ok().flatten().is_some();
                                if can_auto_recover {
                                    crate::vendor::persist_last_error(&cfg, None, None);
                                } else {
                                    crate::vendor::persist_last_error(&cfg, Some(&err_msg), None);
                                    create::maybe_notify_parent_on_error(
                                        &store, &agent, &cfg, &err_msg,
                                    );
                                }
                            }
                        }
                    }
                    flush_harness_usage(&harness, &mut recorded_usage)?;
                    if completed_any && !had_err {
                        let _ = store.complete_idle_agent(&agent.id);
                    }
                }
                Err(reason) => {
                    crate::vendor::persist_last_error(&cfg, Some(&reason), None);
                    create::maybe_notify_parent_on_error(&store, &agent, &cfg, &reason);
                }
            }
            Ok(())
        })();
        if let Err(error) = delivery_result {
            let message = format!("durable delivery transition failed: {error}");
            eprintln!("{message}");
            crate::vendor::persist_last_error(&cfg, Some(&message), None);
        }
        let persisted_err = crate::vendor::read_persisted_last_error(&cfg).and_then(|(e, _)| e);
        let (still_running, has_pending_work) = Store::open(data.join("state.db"))
            .ok()
            .map(|s| {
                let running = s
                    .agent(&agent.id)
                    .ok()
                    .is_some_and(|a| a.state == "running");
                let pending = s.next_undelivered(&agent.id).ok().flatten().is_some()
                    || s.open_turn(&agent.id).ok().flatten().is_some();
                (running, pending)
            })
            .unwrap_or((false, false));
        health.publish(
            &agent.id,
            anyagents_supervisor::LiveState {
                process_alive: still_running,
                turn_state: if still_running && has_pending_work && persisted_err.is_none() {
                    anyagents_harness::TurnState::InTurn
                } else {
                    anyagents_harness::TurnState::Idle
                },
                delivery_mode,
                steer_support,
                quota_reset_at_ms: None,
                restart_at_ms: None,
                pid: None,
                live_harness: Some(agent.harness.clone()),
                live_model: agent.model.clone(),
                process_started_at_ms: None,
                last_error: persisted_err,
            },
        );
        let mut guard = match reconcile.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.flushing_sylphx.remove(agent.id.as_str());
    });
}

fn sync_agy_cli_port_hint(store: &Store, cfg: &Path, agent: &Agent) -> bool {
    #[cfg(target_os = "linux")]
    {
        use std::io::{Read as _, Write as _};
        use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};
        let Some((_, seat_uid, seat_gid, seat_spec)) =
            create::resolved_agent_seat_info(store, agent)
                .or_else(|| crate::sandbox::existing_config_seat(cfg))
        else {
            return false;
        };
        let uid_str = seat_uid.to_string();
        let Ok(proc_entries) = std::fs::read_dir("/proc") else {
            return false;
        };
        let mut found_pid_csrf: Option<(u32, String)> = None;
        for entry in proc_entries.flatten() {
            let name = entry.file_name();
            let Some(pid_str) = name.to_str() else {
                continue;
            };
            let Ok(pid) = pid_str.parse::<u32>() else {
                continue;
            };
            let Ok(status_text) = std::fs::read_to_string(entry.path().join("status")) else {
                continue;
            };
            let matches_uid = status_text
                .lines()
                .find(|l| l.starts_with("Uid:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .is_some_and(|u| u == uid_str);
            if !matches_uid {
                continue;
            }
            let Ok(cmd_bytes) = std::fs::read(entry.path().join("cmdline")) else {
                continue;
            };
            let args: Vec<&str> = cmd_bytes
                .split(|b| *b == 0)
                .filter_map(|s| std::str::from_utf8(s).ok())
                .filter(|s| !s.is_empty())
                .collect();
            if args
                .first()
                .is_some_and(|p| p.ends_with("/agy") || *p == "agy")
                && let Some(idx) = args.iter().position(|a| *a == "--csrf_token")
                && let Some(csrf) = args.get(idx + 1)
            {
                found_pid_csrf = Some((pid, (*csrf).to_string()));
                break;
            }
        }
        let Some((pid, csrf)) = found_pid_csrf else {
            return false;
        };
        let Ok(tcp_text) = std::fs::read_to_string(format!("/proc/{pid}/net/tcp")) else {
            return false;
        };
        let mut found_port: Option<u16> = None;
        for line in tcp_text.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() > 9
                && parts[3] == "0A"
                && parts[7] == uid_str
                && let Some((ip_hex, port_hex)) = parts[1].split_once(':')
                && ip_hex == "0100007F"
                && let Ok(port) = u16::from_str_radix(port_hex, 16)
            {
                let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
                if let Ok(mut stream) =
                    TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(500))
                {
                    let _ = stream.set_read_timeout(Some(std::time::Duration::from_millis(500)));
                    let _ = stream.set_write_timeout(Some(std::time::Duration::from_millis(500)));
                    let req = format!(
                        "POST /exa.language_server_pb.LanguageServerService/GetAllCascadeTrajectories HTTP/1.0\r\n\
                         Host: 127.0.0.1:{port}\r\n\
                         Content-Type: application/json\r\n\
                         x-codeium-csrf-token: {csrf}\r\n\
                         Content-Length: 2\r\n\
                         Connection: close\r\n\r\n{{}}"
                    );
                    let mut resp = String::new();
                    if stream.write_all(req.as_bytes()).is_ok()
                        && stream.read_to_string(&mut resp).is_ok()
                        && (resp.starts_with("HTTP/1.1 200") || resp.starts_with("HTTP/1.0 200"))
                    {
                        found_port = Some(port);
                        break;
                    }
                }
            }
        }
        let Some(port) = found_port else {
            return false;
        };
        let log_path = cfg.join(".gemini/antigravity-cli/cli.log");
        let expected = format!(
            "Starting language server process with pid {pid}\nLanguage server listening on random port at {port} for HTTP\n"
        );
        if std::fs::read_to_string(&log_path)
            .ok()
            .is_some_and(|cur| cur.contains(&expected))
        {
            return true;
        }
        let _ = std::fs::remove_file(&log_path);
        if std::fs::write(&log_path, &expected).is_ok() {
            return true;
        }
        if let Some(spec) = seat_spec {
            let parts: Vec<&str> = spec.split('\t').collect();
            if let [launcher, u, g, grps] = parts.as_slice() {
                let script = format!(
                    "/bin/rm -f '{}' && printf '%s' '{}' > '{}' && /bin/chmod 664 '{}'",
                    log_path.display(),
                    expected,
                    log_path.display(),
                    log_path.display()
                );
                let _ = std::process::Command::new(launcher)
                    .args([
                        "--reuid",
                        u,
                        "--regid",
                        g,
                        "--groups",
                        grps,
                        "--no-new-privs",
                        "--",
                        "/bin/sh",
                        "-c",
                        &script,
                    ])
                    .status();
            } else {
                let _ = (seat_uid, seat_gid);
            }
        }
        std::fs::read_to_string(&log_path)
            .ok()
            .is_some_and(|cur| cur.contains(&expected))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (store, cfg, agent);
        false
    }
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
    if let Ok(all_agents) = store.list_agents() {
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(agent.id.clone());
        let mut seen = std::collections::HashSet::new();
        seen.insert(agent.id.clone());
        while let Some(pid) = queue.pop_front() {
            for child in all_agents
                .iter()
                .filter(|a| a.parent_id.as_ref() == Some(&pid))
            {
                if seen.insert(child.id.clone()) {
                    queue.push_back(child.id.clone());
                    let Ok(_child_lifecycle) = store.lock_agent_lifecycle(&child.id) else {
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
    }
    Ok(archived)
}

/// Reconciliation marks only this row: no descendant locks or filesystem reap
/// may run while the global mail-reconciliation mutex is held.
fn mark_archive_generation_locked(store: &Store, data_dir: &Path, agent: &Agent) {
    let cfg = anyagents_core::agent_config_dir(data_dir, &agent.id);
    let _ = std::fs::create_dir_all(&cfg);
    let _ = std::fs::remove_file(cfg.join(".anyagents-reaped"));
    let _ = std::fs::write(
        cfg.join(".anyagents-archived"),
        format!("{}:{}\n", now_ms(), anyagents_core::AgentId::generate()),
    );
    if let Ok(schedules) = store.list_schedules(Some(&agent.id), true) {
        for sched in schedules {
            let _ = store.cancel_schedule(Some(&agent.id), &sched.id);
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
    let Ok(agent) = store.agent(agent_id) else {
        return;
    };
    if agent.state != "archived" || agent.parent_id.is_none() {
        return;
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
    let Ok(generation) = std::fs::read(&archived) else {
        return;
    };
    if generation.is_empty() {
        return;
    }
    let reaped = cfg.join(".anyagents-reaped");
    if std::fs::read(&reaped).is_ok_and(|done| done == generation) {
        return;
    }
    if reap_subagent_ephemeral_dirs(data_dir, agent_id) {
        let _ = std::fs::write(reaped, generation);
    }
}

/// Existing independent maintenance workers run this at startup and each minute.
/// Never take the global reconciliation mutex, and never wait for an agent lock.
fn reap_archived_agents(store: &Store, data_dir: &Path, health: &HealthBoard) {
    let Ok(agents) = store.list_agents() else {
        return;
    };
    let large_pop = agents.len() > 32 && std::env::var_os("ANYAGENTS_TESTS").is_none();
    for agent in agents {
        if agent.state != "archived" || agent.parent_id.is_none() {
            continue;
        }
        if large_pop {
            let cfg = anyagents_core::agent_config_dir(data_dir, &agent.id);
            if cfg.join(".anyagents-reaped").is_file() {
                continue;
            }
        }
        let Ok(_lifecycle) = store.try_lock_agent_lifecycle(&agent.id) else {
            continue;
        };
        // A wake followed by a new self-archive may have started a process since
        // the population read. Check current health only after taking the lock.
        if health.get(&agent.id).is_some_and(|live| live.process_alive) {
            continue;
        }
        // Re-read under the lock: this population may predate a direct wake.
        reap_archived_agent_locked(store, data_dir, &agent.id);
    }
}

/// Consolidate idle agents' topic files without changing the active session's
/// immutable snapshot. Updated files are pinned only at the next explicit session.
fn consolidate_idle_agent_memory(store: &Store, data_dir: &Path, health: &HealthBoard) {
    let Ok(agents) = store.list_agents() else {
        return;
    };
    let mut llm_consolidated_count = 0usize;
    for agent in agents {
        if agent.state == "archived" {
            continue;
        }
        let agent_root = data_dir.join("agents").join(agent.id.as_str());
        if !agent_root.join("memory").is_dir() {
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
        if let Ok((_, true)) =
            store.consolidate_agent_sleep_memory_with_options(&agent.id, allow_llm)
        {
            llm_consolidated_count += 1;
        }
    }
}

pub(crate) fn clear_explicitly_archived(data_dir: &Path, agent_id: &anyagents_core::AgentId) {
    let cfg = anyagents_core::agent_config_dir(data_dir, agent_id);
    let _ = std::fs::remove_file(cfg.join(".anyagents-reaped"));
    let _ = std::fs::remove_file(cfg.join(".anyagents-archived"));
}

fn reconcile_stopped_and_backed_off_agents(
    store: &Store,
    health: &HealthBoard,
    data: &Path,
    reconcile: &Arc<std::sync::Mutex<AdminReconcileState>>,
    now_ms: i64,
) {
    if let Err(error) = outbound_history::reconcile(store) {
        eprintln!("outbound history settlement remains pending: {error}");
    }
    let Ok(agents) = store.list_agents() else {
        return;
    };
    reconcile_agent_snapshot(store, health, data, reconcile, now_ms, agents);
}

fn reconcile_agent_snapshot(
    store: &Store,
    health: &HealthBoard,
    data: &Path,
    reconcile: &Arc<std::sync::Mutex<AdminReconcileState>>,
    now_ms: i64,
    agents: Vec<Agent>,
) {
    let mut guard = match reconcile.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    if !guard.initialized {
        for agent in &agents {
            let delivered = store.delivered_cursor(&agent.id).unwrap_or(0);
            guard.watermark_seq.insert(agent.id.to_string(), delivered);
        }
        guard.initialized = true;
    }

    let mut ordered_indices: Vec<usize> = (0..agents.len()).collect();
    ordered_indices.sort_by_key(|&idx| {
        let a = &agents[idx];
        let last_try = guard
            .last_backoff_reset_ms
            .get(a.id.as_str())
            .copied()
            .unwrap_or(0);
        (last_try, idx)
    });

    for idx in ordered_indices {
        let Ok(current) = store.agent(&agents[idx].id) else {
            continue;
        };
        // Match cascade ordering: parent before child. This keeps the parent's
        // archive/wake decision stable while applying it to this child.
        let _parent_lifecycle = if let Some(ref parent_id) = current.parent_id {
            let Ok(lock) = store.try_lock_agent_lifecycle(parent_id) else {
                continue;
            };
            Some(lock)
        } else {
            None
        };
        let Ok(_lifecycle) = store.try_lock_agent_lifecycle(&agents[idx].id) else {
            continue;
        };
        // A send/wake may have changed this row since the population scan.
        let Ok(agent) = store.agent(&agents[idx].id) else {
            continue;
        };
        let agent = &agent;
        if agent.state == "archived" && !health.get(&agent.id).is_some_and(|l| l.process_alive) {
            continue;
        }
        let delivered = store.delivered_cursor(&agent.id).unwrap_or(0);
        let parent_archived = agent.parent_id.as_ref().is_some_and(|pid| {
            store
                .agent(pid)
                .is_ok_and(|parent| is_explicitly_archived(data, &parent))
        });
        if parent_archived && agent.state != "archived" {
            let _ = store.set_agent_state(&agent.id, "archived");
            mark_archive_generation_locked(store, data, agent);
        }
        if is_explicitly_archived(data, agent) || parent_archived {
            if has_explicit_archive_marker(data, &agent.id) && agent.state != "archived" {
                let _ = store.set_agent_state(&agent.id, "archived");
            }
            if agent.harness == "sylphx"
                && !guard.flushing_sylphx.contains(agent.id.as_str())
                && health.get(&agent.id).is_some_and(|l| l.process_alive)
            {
                let (delivery_mode, steer_support) = declared(&agent.harness);
                health.publish(
                    &agent.id,
                    anyagents_supervisor::LiveState {
                        process_alive: false,
                        turn_state: anyagents_harness::TurnState::Idle,
                        delivery_mode,
                        steer_support,
                        quota_reset_at_ms: None,
                        restart_at_ms: None,
                        pid: None,
                        live_harness: Some(agent.harness.clone()),
                        live_model: agent.model.clone(),
                        process_started_at_ms: None,
                        last_error: None,
                    },
                );
            }
            let undelivered = store.inbox(&agent.id, Some(delivered)).unwrap_or_default();
            let max_seq = undelivered.last().map(|m| m.seq).unwrap_or(delivered);
            guard.watermark_seq.insert(agent.id.to_string(), max_seq);
            continue;
        }
        let oldest_age_ms = store
            .lag(&agent.id, now_ms)
            .ok()
            .and_then(|l| l.oldest_undelivered_age_ms);
        if agent.state == "idle" || agent.state == "created" {
            if oldest_age_ms.is_some() {
                let undelivered = store.inbox(&agent.id, Some(delivered)).unwrap_or_default();
                let max_seq = undelivered.last().map(|m| m.seq).unwrap_or(delivered);
                let prev_wm = guard
                    .watermark_seq
                    .get(agent.id.as_str())
                    .copied()
                    .unwrap_or(delivered);
                let has_new_mail =
                    max_seq > delivered && (agent.state == "created" || max_seq > prev_wm);
                if has_new_mail {
                    let check_from = if agent.state == "created" {
                        delivered
                    } else {
                        prev_wm
                    };
                    let should_wake = undelivered.iter().filter(|m| m.seq > check_from).any(|m| {
                        if m.context.event.as_deref() == Some("reaction_passive") {
                            return false;
                        }
                        let trimmed = m.text.trim_start();
                        !(agent.parent_id.is_none()
                            && (trimmed.starts_with("[Sub-Agent Completed]")
                                || trimmed.starts_with("[Sub-Agent Error]")))
                    });
                    if should_wake {
                        if store.set_agent_state(&agent.id, "running").is_ok() {
                            if agent.harness == "sylphx" {
                                if !guard.flushing_sylphx.contains(agent.id.as_str()) {
                                    let mut running_agent = agent.clone();
                                    running_agent.state = "running".to_string();
                                    guard.watermark_seq.insert(agent.id.to_string(), max_seq);
                                    guard
                                        .last_backoff_reset_ms
                                        .insert(agent.id.to_string(), now_ms);
                                    guard.flushing_sylphx.insert(agent.id.to_string());
                                    spawn_admin_sylphx_delivery(
                                        data.to_path_buf(),
                                        running_agent,
                                        health.clone(),
                                        Arc::clone(reconcile),
                                    );
                                }
                            } else {
                                guard.watermark_seq.insert(agent.id.to_string(), max_seq);
                                create::poke_agent_wake_socket(data, &agent.id);
                                create::poke_agent_wake_socket_mail(data, &agent.id);
                            }
                        }
                    } else {
                        guard.watermark_seq.insert(agent.id.to_string(), max_seq);
                    }
                }
            } else {
                guard.watermark_seq.insert(agent.id.to_string(), delivered);
            }
        } else if agent.state == "running" {
            if agent.harness == "sylphx" {
                let cfg = anyagents_core::agent_config_dir(data, &agent.id);
                let persisted_err =
                    crate::vendor::read_persisted_last_error(&cfg).and_then(|(e, _)| e);
                let has_open_recovery = store.open_turn(&agent.id).ok().flatten().is_some();
                let has_pending_work = oldest_age_ms.is_some() || has_open_recovery;
                if !guard.flushing_sylphx.contains(agent.id.as_str()) {
                    if !has_pending_work
                        && persisted_err.is_none()
                        && store.complete_idle_agent(&agent.id).unwrap_or(false)
                    {
                        let (delivery_mode, steer_support) = declared(&agent.harness);
                        health.publish(
                            &agent.id,
                            anyagents_supervisor::LiveState {
                                process_alive: false,
                                turn_state: anyagents_harness::TurnState::Idle,
                                delivery_mode,
                                steer_support,
                                quota_reset_at_ms: None,
                                restart_at_ms: None,
                                pid: None,
                                live_harness: Some(agent.harness.clone()),
                                live_model: agent.model.clone(),
                                process_started_at_ms: None,
                                last_error: None,
                            },
                        );
                        guard.watermark_seq.insert(agent.id.to_string(), delivered);
                        continue;
                    }
                    let (delivery_mode, steer_support) = declared(&agent.harness);
                    health.publish(
                        &agent.id,
                        anyagents_supervisor::LiveState {
                            process_alive: true,
                            turn_state: anyagents_harness::TurnState::Idle,
                            delivery_mode,
                            steer_support,
                            quota_reset_at_ms: None,
                            restart_at_ms: None,
                            pid: None,
                            live_harness: Some(agent.harness.clone()),
                            live_model: agent.model.clone(),
                            process_started_at_ms: None,
                            last_error: persisted_err.clone(),
                        },
                    );
                }
                if has_pending_work && !guard.flushing_sylphx.contains(agent.id.as_str()) {
                    let undelivered = store.inbox(&agent.id, Some(delivered)).unwrap_or_default();
                    let max_seq = undelivered.last().map(|m| m.seq).unwrap_or(delivered);
                    let prev_wm = guard
                        .watermark_seq
                        .get(agent.id.as_str())
                        .copied()
                        .unwrap_or(delivered);
                    let last_try = guard
                        .last_backoff_reset_ms
                        .get(agent.id.as_str())
                        .copied()
                        .unwrap_or(0);
                    let ready_to_spawn = if has_open_recovery && oldest_age_ms.is_none() {
                        last_try == 0 || now_ms.saturating_sub(last_try) >= 10_000
                    } else {
                        (persisted_err.is_none()
                            && (last_try == 0
                                || max_seq > prev_wm
                                || now_ms.saturating_sub(last_try) >= 5_000))
                            || max_seq > prev_wm
                            || now_ms.saturating_sub(last_try) >= 15_000
                    };
                    if ready_to_spawn {
                        guard.watermark_seq.insert(agent.id.to_string(), max_seq);
                        guard.flushing_sylphx.insert(agent.id.to_string());
                        spawn_admin_sylphx_delivery(
                            data.to_path_buf(),
                            agent.clone(),
                            health.clone(),
                            Arc::clone(reconcile),
                        );
                    }
                } else if oldest_age_ms.is_none() {
                    guard.watermark_seq.insert(agent.id.to_string(), delivered);
                }
                continue;
            }
            if agent.harness == "codex" && store.has_open_turn(&agent.id).unwrap_or(false) {
                let cfg = anyagents_core::agent_config_dir(data, &agent.id);
                let rotated_ms = std::fs::read_to_string(cfg.join(".anyagents-profile-rotated-ms"))
                    .ok()
                    .and_then(|s| s.trim().parse::<i64>().ok())
                    .unwrap_or(0);
                if crate::vendor::detect_codex_session_turn_error(&cfg, rotated_ms).is_some() {
                    let last_reset = guard
                        .last_backoff_reset_ms
                        .get(agent.id.as_str())
                        .copied()
                        .unwrap_or(0);
                    if now_ms.saturating_sub(last_reset) >= 60_000 {
                        let work_dir = agent
                            .workdir
                            .as_deref()
                            .map(PathBuf::from)
                            .unwrap_or_else(|| anyagents_core::agent_work_dir(data, &agent.id));
                        let reconciled = create::reconcile_agent_runtime_config(
                            store, data, agent, &cfg, &work_dir,
                        )
                        .and_then(|_| {
                            std::fs::write(
                                cfg.join(".anyagents-profile-rotated-ms"),
                                now_ms.to_string(),
                            )
                            .map_err(Error::from)
                        });
                        if let Err(error) = reconciled {
                            eprintln!("runtime reconciliation failed for {}: {error}", agent.id);
                            continue;
                        }
                        guard
                            .last_backoff_reset_ms
                            .insert(agent.id.to_string(), now_ms);
                        create::poke_agent_wake_socket(data, &agent.id);
                        create::poke_agent_wake_socket_mail(data, &agent.id);
                        continue;
                    }
                }
            }
            if oldest_age_ms.is_none() {
                guard.watermark_seq.insert(agent.id.to_string(), delivered);
                continue;
            }
            if agent.harness == "agy" && oldest_age_ms.is_some_and(|age| age >= 3_000) {
                let cfg = anyagents_core::agent_config_dir(data, &agent.id);
                if sync_agy_cli_port_hint(store, &cfg, agent) {
                    create::poke_agent_wake_socket_mail(data, &agent.id);
                }
            }
            let is_down_without_quota = match health.get(&agent.id) {
                Some(live) => {
                    !live.process_alive
                        && live.quota_reset_at_ms.is_none()
                        && (live.restart_at_ms.is_none()
                            || live.restart_at_ms.is_some_and(|r| r >= 15_000))
                }
                None => true,
            };
            if is_down_without_quota
                && oldest_age_ms.is_some_and(|age| age >= 15_000)
                && crate::schedule::agent_wake_block_reason(store, health, agent, now_ms)
                    != Some("quota_blocked")
            {
                let last_reset = guard
                    .last_backoff_reset_ms
                    .get(agent.id.as_str())
                    .copied()
                    .unwrap_or(0);
                if now_ms.saturating_sub(last_reset) >= 60_000 {
                    guard
                        .last_backoff_reset_ms
                        .insert(agent.id.to_string(), now_ms);
                    let cfg = anyagents_core::agent_config_dir(data, &agent.id);
                    let _ = std::fs::write(
                        cfg.join(".anyagents-profile-rotated-ms"),
                        now_ms.to_string(),
                    );
                    create::poke_agent_wake_socket(data, &agent.id);
                    create::poke_agent_wake_socket_mail(data, &agent.id);
                }
            }
        }
    }
}

fn acquire_admin_lock(data: &Path) -> Result<std::fs::File, Error> {
    std::fs::create_dir_all(data)?;
    let lock_path = data.join("admin.lock");
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
                "anyagents serve-admin already running on {}",
                data.display()
            )));
        }
    }
    Ok(file)
}

/// Both intervals tick immediately at startup. Each loop owns its blocking
/// pass, so an in-flight external request cannot hold the next local tick.
fn start_schedule_loops(
    store: Arc<Store>,
    health: HealthBoard,
    defer_to_admin: bool,
) -> (tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>) {
    let local = spawn_schedule_loop(
        Arc::clone(&store),
        health.clone(),
        defer_to_admin,
        std::time::Duration::from_secs(1),
        crate::schedule::tick_local_schedules_once,
    );
    let external = spawn_schedule_loop(
        store,
        health,
        defer_to_admin,
        std::time::Duration::from_secs(3),
        crate::schedule::tick_external_watches_once,
    );
    (local, external)
}

fn spawn_schedule_loop(
    store: Arc<Store>,
    health: HealthBoard,
    defer_to_admin: bool,
    period: std::time::Duration,
    tick: fn(&Store, &HealthBoard, i64) -> usize,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let st = Arc::clone(&store);
            let hb = health.clone();
            if let Err(err) = tokio::task::spawn_blocking(move || {
                let data = st.data_dir();
                if defer_to_admin
                    && (data.join("admin.sock").exists() || data.join("admin.lock").exists())
                {
                    return 0;
                }
                tick(&st, &hb, now_ms())
            })
            .await
            {
                eprintln!("Schedule pass failed: {err}");
            }
        }
    })
}

pub fn run_admin(data: &Path) -> Result<(), Error> {
    let identity = Arc::new(ServingIdentity::capture());
    let _admin_lock = acquire_admin_lock(data)?;
    crate::worktree_access::recover_inspections(data)?;
    let runtime = tokio::runtime::Runtime::new()?;
    let store = Arc::new(Store::open(data.join("state.db"))?);
    if let Ok(key) = capability_key() {
        let _ = store.hold_capability_key(&key);
    }
    crate::host_memory::on_start(&store);
    let res = runtime.block_on(async move {
        let auth = deployment_mode(data);
        let path = data.join("admin.sock");
        if path.as_os_str().len() > MAX_SOCKET_PATH {
            return Err(Error::Message(format!(
                "socket path is longer than {MAX_SOCKET_PATH} bytes"
            )));
        }
        prepare_socket(&path)?;
        let listener = tokio::net::UnixListener::bind(&path)?;
        set_socket_mode(&path)?;
        identity.start_hash();
        if auth.is_confined() {
            crate::sandbox::share_socket_with_seats(&path);
        }
        if let Ok(agents) = store.list_agents() {
            for agent in agents {
                if agent.harness == "sylphx" {
                    create::poke_agent_wake_socket(data, &agent.id);
                }
            }
        }
        let health = HealthBoard::default();
        sync_health_from_primary_socket(data, &health);
        let rec_store = Arc::clone(&store);
        let rec_health = health.clone();
        let rec_data = data.to_path_buf();
        let reconcile_state = Arc::new(std::sync::Mutex::new(AdminReconcileState::default()));
        let reconcile_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            let mut ticks: u64 = 0;
            loop {
                interval.tick().await;
                ticks = ticks.wrapping_add(1);
                let sync_health = ticks.is_multiple_of(5);
                let st = Arc::clone(&rec_store);
                let hb = rec_health.clone();
                let dd = rec_data.clone();
                let rec = Arc::clone(&reconcile_state);
                let _ = tokio::task::spawn_blocking(move || {
                    if sync_health {
                        sync_health_from_primary_socket(&dd, &hb);
                    }
                    let now = now_ms();
                    reconcile_stopped_and_backed_off_agents(&st, &hb, &dd, &rec, now);
                })
                .await;
            }
        });
        let (local_sched_handle, external_sched_handle) =
            start_schedule_loops(Arc::clone(&store), health.clone(), false);
        let reaper_handle = if std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
            && !crate::sandbox::is_isolated_test_env()
        {
            let reaper_root = data.to_path_buf();
            let reaper_store = Arc::clone(&store);
            let reaper_health = health.clone();
            Some(tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
                loop {
                    interval.tick().await;
                    let root = reaper_root.clone();
                    let store = Arc::clone(&reaper_store);
                    let health = reaper_health.clone();
                    let _ = tokio::task::spawn_blocking(move || {
                        reap_archived_agents(&store, &root, &health);
                        consolidate_idle_agent_memory(&store, &root, &health);
                        crate::browser::reap_idle_browsers(&root);
                        crate::create::sync_fresher_bound_agent_credentials(&root);
                        #[cfg(unix)]
                        crate::create::sync_canonical_skills(&root);
                    })
                    .await;
                }
            }))
        } else {
            None
        };
        let state = AppState {
            store: Arc::clone(&store),
            health,
            sign_in: Arc::new(std::sync::Mutex::new(ProcessRunner)),
            auth,
            identity,
        };
        let app = Router::new()
            .route("/v1/agents", get(list_agents).post(create_agent_route))
            .route("/v1/agents/{name}/admin", post(set_admin_route))
            .route("/v1/agents/{name}/rename", post(rename_agent_route))
            .route("/v1/agents/{name}/role", post(role_agent_route))
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
            .route(
                "/v1/operator/agents/{name}/capability",
                post(mint_capability),
            )
            .route("/v1/operator/agents/{name}/inbox", get(operator_inbox))
            .route("/v1/whoami", get(whoami))
            .route("/v1/webhooks/healthz", get(admin_healthz))
            .with_state(state);
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let result = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = terminate.recv() => {}
                }
            })
            .await;
        reconcile_handle.abort();
        local_sched_handle.abort();
        external_sched_handle.abort();
        if let Some(h) = reaper_handle {
            h.abort();
        }
        result.map_err(Error::from)
    });
    runtime.shutdown_timeout(std::time::Duration::from_millis(100));
    res
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

/// `POST /v1/operator/agents/{name}/capability`: the operator gives one agent a
/// fresh capability, retiring the last one. The value is written into that
/// agent's own config directory and never returned.
async fn mint_capability(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_operator(&principal)?;
    let agent = state
        .store
        .agent_by_name(&name)
        .map_err(ApiError::from_store)?;
    operator::mint(&state.store, &agent.id).map_err(ApiError::from_store)?;
    Ok(Json(
        serde_json::json!({ "agent": agent.name, "minted": true }),
    ))
}

/// `GET /v1/webhooks/healthz`: unauthenticated readiness probe for the public
/// webhook listener (`https://webhooks.example.com/v1/webhooks/healthz`).
async fn webhook_healthz(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        Json(state.identity.status(false)),
    )
}

/// Process-local details are available only on the admin Unix socket, never
/// through the internet-reachable webhook listener or its primary API aliases.
async fn admin_healthz(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        Json(state.identity.status(true)),
    )
        .into_response()
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

/// `GET /v1/whoami`: the agent this call acts as. A tool server asks before it
/// speaks for anyone, and gets the id the server resolved from the capability,
/// never the name a caller wrote.
async fn whoami(
    State(state): State<AppState>,
    ctx: Whoami,
) -> Result<Json<serde_json::Value>, ApiError> {
    match ctx.principal {
        Principal::Agent { agent_id, name } => {
            let workdir = state.store.agent(&agent_id).ok().and_then(|a| a.workdir);
            Ok(Json(serde_json::json!({
            "agent_id": agent_id.as_str(),
            "name": name,
                "workdir": workdir,
            })))
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
            let principal =
                auth::agent_by_name(&state.store, id.trim()).map_err(auth::AuthRejection)?;
            return Ok(Self { principal });
        }
        use axum::extract::FromRequestParts;
        let principal =
            <Principal as FromRequestParts<AppState>>::from_request_parts(parts, state).await?;
        Ok(Self { principal })
    }
}

/// `GET /v1/operator/agents/{name}/inbox`: any agent's mailbox, for the
/// operator's own reading. A different path from the agent's own route, so a
/// name can never be taken for an identity.
async fn operator_inbox(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Query(query): Query<Since>,
) -> Result<Json<InboxResponse>, ApiError> {
    require_operator(&principal)?;
    let agent = state
        .store
        .agent_by_name(&name)
        .map_err(ApiError::from_store)?;
    spawn_store(move || inbox_of(&state.store, &agent.id, query.since, query.limit)).await
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
            Principal::Agent { agent_id, .. } if found.parent_id.as_ref() == Some(agent_id) => (),
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
    Json(body): Json<HarnessRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        let found = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        let actor = match &principal {
            Principal::Agent {
                agent_id,
                name: caller_name,
            } if found.parent_id.as_ref() == Some(agent_id) => {
                format!("agent:{caller_name}")
            }
            _ => {
                require_admin_or_operator(&state.store, &principal, "agents.harness", &found.name)?
            }
        };
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
        Ok(Json(out))
    })
    .await
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
        let actor = match &principal {
            Principal::Agent {
                agent_id,
                name: caller_name,
            } if found.parent_id.as_ref() == Some(agent_id) => {
                format!("agent:{caller_name}")
            }
            _ => require_admin_or_operator(&state.store, &principal, "agents.transfer", &found.name)?,
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
                let target = state
                    .store
                    .agent_by_name(clean)
                    .map_err(ApiError::from_store)?;
                if target.state == "archived" {
                    return Err(ApiError::new(
                        StatusCode::BAD_REQUEST,
                        format!("cannot transfer to archived parent `{}`", target.name),
                    ));
                }
                Some(target)
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

fn sync_health_from_primary_socket(data: &Path, health: &HealthBoard) {
    #[cfg(unix)]
    {
        use std::io::{Read as _, Write as _};
        use std::os::unix::net::UnixStream;
        let sock = data.join("anyagents.sock");
        if !sock.exists() {
            return;
        }
        let Ok(mut stream) = UnixStream::connect(&sock) else {
            return;
        };
        let _ = stream.set_read_timeout(Some(std::time::Duration::from_millis(3000)));
        let _ = stream.set_write_timeout(Some(std::time::Duration::from_millis(3000)));
        let auth_hdr = match crate::operator::operator_token(data) {
            Some(tok) => format!("{}: {tok}\r\n", crate::operator::HEADER),
            None => String::new(),
        };
        let req = format!(
            "GET /v1/agents HTTP/1.0\r\nHost: localhost\r\n{auth_hdr}Connection: close\r\n\r\n"
        );
        if stream.write_all(req.as_bytes()).is_err() {
            return;
        }
        let mut resp = String::new();
        if stream.read_to_string(&mut resp).is_err() {
            return;
        }
        let Some((head, body)) = resp.split_once("\r\n\r\n") else {
            return;
        };
        if !head.starts_with("HTTP/1.1 200") && !head.starts_with("HTTP/1.0 200") {
            return;
        }
        let Some(start) = body.find('{') else {
            return;
        };
        let Some(end) = body.rfind('}') else {
            return;
        };
        let Ok(val) = serde_json::from_str::<serde_json::Value>(&body[start..=end]) else {
            return;
        };
        let Some(arr) = val.get("agents").and_then(serde_json::Value::as_array) else {
            return;
        };
        for item in arr {
            let Some(id_str) = item.get("id").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let Ok(id) = anyagents_core::AgentId::parse(id_str) else {
                continue;
            };
            let alive = item
                .get("process")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|p| p == "alive");
            let working = item
                .get("turn_state")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|t| t == "working");
            let harness_str = item
                .get("harness")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("codex");
            let (delivery_mode, steer_support) = declared(harness_str);
            let raw_last_error = item
                .get("last_error")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string);
            if harness_str == "sylphx" {
                continue;
            }
            health.publish(
                &id,
                anyagents_supervisor::LiveState {
                    process_alive: alive,
                    turn_state: if working {
                        anyagents_harness::TurnState::InTurn
                    } else {
                        anyagents_harness::TurnState::Idle
                    },
                    delivery_mode,
                    steer_support,
                    quota_reset_at_ms: item
                        .get("quota_reset_at_ms")
                        .and_then(serde_json::Value::as_i64),
                    restart_at_ms: item
                        .get("restart_at_ms")
                        .and_then(serde_json::Value::as_i64),
                    pid: None,
                    live_harness: item
                        .get("live_harness")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string),
                    live_model: item
                        .get("live_model")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string),
                    process_started_at_ms: item
                        .get("process_started_at_ms")
                        .and_then(serde_json::Value::as_i64),
                    last_error: raw_last_error,
                },
            );
        }
    }
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
            if &found.id == agent_id || found.parent_id.as_ref() == Some(agent_id) {
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
        return spawn_store(move || {
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
            Ok(Json(out))
        })
        .await;
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
        require_admin_or_operator(&state.store, &principal, "agents.rename", &old.name)?;
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
        require_admin_or_operator(&state.store, &principal, "agents.role", &found.name)?;
        let new_role = if body.clear.unwrap_or(false) {
            None
        } else {
            body.role.as_deref()
        };
        let updated = state
            .store
            .set_agent_role(&found.id, new_role)
            .map_err(ApiError::from_store)?;
        let data_dir = state.store.data_dir();
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
    require_admin_or_operator(&state.store, &principal, "profiles.models", "profiles")?;
    let data = state.store.data_dir().to_path_buf();
    let prof = query.profile;
    spawn_store(move || {
        Ok(Json(create::probe_profile_models_catalog(
            &data,
            prof.as_deref(),
        )))
    })
    .await
}

async fn profiles_route(
    principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<Vec<create::ProviderProfileEntry>>, ApiError> {
    require_admin_or_operator(&state.store, &principal, "profiles.list", "profiles")?;
    Ok(Json(create::list_provider_profiles_fast(
        &state.store,
        state.store.data_dir(),
    )))
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
    require_admin_or_operator(&state.store, &principal, "profiles.set", &body.profile)?;
    let (provider, profile) =
        create::set_provider_profile(state.store.data_dir(), &body.profile, &body.key)
            .map_err(ApiError::from_store)?;
    Ok(Json(serde_json::json!({
        "provider": provider,
        "profile": profile,
    })))
}

async fn get_subagent_defaults_route(
    _principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<create::SubagentDefaultsConfig>, ApiError> {
    Ok(Json(create::load_subagent_defaults(state.store.data_dir())))
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
            harness: body.harness.filter(|s| !s.trim().is_empty()),
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
    create::save_subagent_defaults(state.store.data_dir(), &cfg).map_err(ApiError::from_store)?;
    Ok(Json(cfg))
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
        let is_self_or_parent = match &principal {
            Principal::Agent { agent_id, .. } => {
                is_self_or_ancestor(&state.store, agent_id, &target)
            }
            Principal::Operator => true,
        };
        if !is_self_or_parent {
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
        let is_ancestor = match &principal {
            Principal::Agent { agent_id, .. } => {
                *agent_id != rec.id && is_self_or_ancestor(&state.store, agent_id, &rec)
            }
            Principal::Operator => true,
        };
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
        if let Some(ref role_str) = body.role {
            if !is_ancestor {
                require_admin_or_operator(&state.store, &principal, "agents.role", &rec.name)?;
            }
            rec = state
                .store
                .set_agent_role(&rec.id, Some(role_str.as_str()))
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

            let agents = store.list_agents().map_err(ApiError::from_store)?;
            let mut visible_ids = std::collections::HashSet::from([caller.id.clone()]);
            let mut changed = true;
            while changed {
                changed = false;
                for agent in &agents {
                    if let Some(parent) = &agent.parent_id
                        && visible_ids.contains(parent)
                        && visible_ids.insert(agent.id.clone())
                    {
                        changed = true;
                    }
                }
            }
            let visible_agent_ids: Vec<String> =
                visible_ids.iter().map(ToString::to_string).collect();

            if requested.eq_ignore_ascii_case("self") {
                return Ok((caller.name, Some(vec![caller.id.to_string()])));
            }
            if requested.eq_ignore_ascii_case("all") || requested == "*" {
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
pub fn listen_on_loopback() -> Result<(u16, tokio::net::TcpListener), Error> {
    // A non-blocking listener before it joins the runtime; the runtime makes it
    // blocking-polled itself.
    let address = std::net::TcpListener::bind(("127.0.0.1", 0))?;
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

async fn list_agents(
    principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<AgentsResponse>, ApiError> {
    let operator = principal.is_operator();
    spawn_store(move || {
        let mut listed = list_agents_sync_for_principal(&state, &principal)?;
        if !operator {
            listed.0.gateway = None;
        }
        Ok(listed)
    })
    .await
}

fn compute_caller_relationship(store: &Store, caller: &Agent, target: &Agent) -> String {
    if caller.id == target.id {
        "self".to_string()
    } else if caller.parent_id.as_ref() == Some(&target.id) {
        "parent".to_string()
    } else if target.parent_id.as_ref() == Some(&caller.id) {
        "child".to_string()
    } else if caller.parent_id.is_some() && caller.parent_id == target.parent_id {
        "sibling".to_string()
    } else if is_self_or_ancestor(store, &target.id, caller) {
        "ancestor".to_string()
    } else if is_self_or_ancestor(store, &caller.id, target) {
        "descendant".to_string()
    } else if caller.parent_id.is_none() && target.parent_id.is_none() {
        "peer".to_string()
    } else {
        "other".to_string()
    }
}

fn list_agents_sync_for_principal(
    state: &AppState,
    principal: &Principal,
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
    let all_agents = state.store.list_agents().map_err(ApiError::from_store)?;
    let fast_archived = all_agents.len() > 32;
    let mut agents = Vec::with_capacity(all_agents.len());
    for agent in all_agents {
        if let Some(ref caller) = caller_agent
            && caller.is_isolated()
            && !is_self_or_ancestor(&state.store, &caller.id, &agent)
            && caller.parent_id.as_ref() != Some(&agent.id)
            && !state.store.share_any_group_or_chat(&caller.id, &agent.id)
        {
            continue;
        }
        let mut view = if fast_archived && agent.state == "archived" {
            archived_agent_view_fast(&state.store, &agent)
        } else {
            agent_view(&state.store, &state.health, &agent)?
        };
        if let Some(ref caller) = caller_agent {
            view.relationship = Some(compute_caller_relationship(&state.store, caller, &agent));
            view.can_message = Some(
                can_actor_message_agent(&state.store, &Actor::Agent(caller.name.clone()), &agent)
                    .is_ok(),
            );
            if !can_actor_read_agent_private(&state.store, principal, &agent) {
                view.instructions = None;
            }
        }
        agents.push(view);
    }
    Ok(Json(AgentsResponse {
        agents,
        gateway: crate::gateway::origin(),
    }))
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

fn archived_agent_view_fast(store: &Store, agent: &Agent) -> AgentView {
    let (mode, steer) = declared(&agent.harness);
    AgentView {
        id: agent.id.to_string(),
        name: agent.name.clone(),
        role: agent.role.clone(),
        aliases: Vec::new(),
        parent: agent.parent_id.as_ref().map(ToString::to_string),
        scope: agent.scope().to_string(),
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
        delivery_mode: mode_name(mode).to_string(),
        steer_support: steer_name(steer).to_string(),
        quota_reset_at_ms: None,
        block_reason: None,
        last_error: None,
        oldest_undelivered_age_ms: None,
        install: install_state(store, agent),
        description: agent.public_description(),
        instructions: agent.instructions.clone(),
        seat_user: None,
        seat_uid: None,
        seat_gid: None,
        schedules: Vec::new(),
    }
}

fn agent_view(store: &Store, health: &HealthBoard, agent: &Agent) -> Result<AgentView, ApiError> {
    let now = now_ms();
    let lag = store.lag(&agent.id, now).map_err(ApiError::from_store)?;
    let has_open_turn = store
        .has_open_turn(&agent.id)
        .map_err(ApiError::from_store)?;
    let cfg = anyagents_core::agent_config_dir(store.data_dir(), &agent.id);
    let rotated_ms = std::fs::read_to_string(cfg.join(".anyagents-profile-rotated-ms"))
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let (session_quota, mut session_error) = if agent.state == "archived" {
        (None, None)
    } else if agent.harness == "claude-code" {
        let (_, persisted_err) =
            anyagents_harness_agy::detect_agy_session_quota_and_error(&cfg, now);
        (
            anyagents_harness_claude::detect_session_quota_reset_ms(&cfg, now),
            persisted_err,
        )
    } else {
        anyagents_harness_agy::detect_agy_session_quota_and_error(&cfg, now)
    };
    if agent.state != "archived" && session_error.is_none() && agent.harness == "codex" {
        session_error = crate::vendor::detect_codex_session_turn_error(&cfg, rotated_ms);
    }
    let (
        process,
        mut turn_state,
        mode,
        steer,
        mut quota,
        live_harness,
        live_model,
        process_started_at_ms,
        pending_switch,
        mut last_error,
    ) = match health.get(&agent.id) {
        Some(live) if agent.harness != "sylphx" || live.process_alive => {
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
                mode_name(live.delivery_mode),
                steer_name(live.steer_support),
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
            let (mode, steer) = declared(&agent.harness);
            let lock_busy = agent.harness == "sylphx"
                && matches!(
                    store.try_lock_agent(&agent.id),
                    Err(anyagents_core::Error::AgentBusy)
                );
            let admin_alive = agent.harness == "sylphx"
                && agent.state == "running"
                && (store.data_dir().join("admin.sock").exists()
                    || store.data_dir().join("admin.lock").exists());
            let proc_str = if lock_busy || admin_alive {
                "alive"
            } else {
                "down"
            };
            let turn_str = if lock_busy { "working" } else { "idle" };
            (
                proc_str,
                turn_str,
                mode_name(mode),
                steer_name(steer),
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
    if let Some(reset_ms) = quota.filter(|&ms| ms > now) {
        create::record_profile_quota_reset(store.data_dir(), agent, reset_ms);
        if create::try_rotate_exhausted_profile(store, store.data_dir(), agent, true).is_some() {
            quota = None;
            last_error = None;
        }
    }
    if (agent.state == "running" || agent.state == "idle") && quota.is_none_or(|ms| ms <= now) {
        if let Some(ref err_msg) = last_error {
            turn_state = if crate::host_memory::is_migration_wait(err_msg) {
                "idle"
            } else {
                "errored"
            };
            create::maybe_notify_parent_on_error(store, agent, &cfg, err_msg);
        } else {
            let _ = std::fs::remove_file(cfg.join(".anyagents-notified-parent-error.json"));
        }
    }
    let block_reason = compute_block_reason(quota, last_error.as_deref(), now);
    let oldest_undelivered_age_ms = match lag.oldest_undelivered_age_ms {
        Some(age) => Some(age),
        None if has_open_turn
            && (quota.is_some() || last_error.is_some() || turn_state != "working") =>
        {
            store.open_turn(&agent.id).ok().flatten().map(|open| {
                open.messages
                    .first()
                    .map(|m| now.saturating_sub(m.received_at_ms))
                    .unwrap_or_else(|| now.saturating_sub(open.opened_at_ms))
            })
        }
        None => None,
    };
    let aliases = store.agent_aliases(&agent.id).unwrap_or_default();
    let seat_info = create::resolved_agent_seat_info(store, agent);
    let schedules = store
        .list_schedules(Some(&agent.id), true)
        .unwrap_or_default()
        .into_iter()
        .map(crate::api::ScheduleView::from)
        .collect();
    Ok(AgentView {
        id: agent.id.to_string(),
        name: agent.name.clone(),
        role: agent.role.clone(),
        aliases,
        parent: agent.parent_id.as_ref().map(ToString::to_string),
        scope: agent.scope().to_string(),
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
        delivery_mode: mode.to_string(),
        steer_support: steer.to_string(),
        quota_reset_at_ms: quota,
        block_reason,
        last_error,
        oldest_undelivered_age_ms,
        install: install_state(store, agent),
        description: agent.public_description(),
        instructions: agent.instructions.clone(),
        seat_user: seat_info.as_ref().map(|(u, _, _, _)| u.clone()),
        seat_uid: seat_info.as_ref().map(|(_, uid, _, _)| *uid),
        seat_gid: seat_info.as_ref().map(|(_, _, gid, _)| *gid),
        schedules,
    })
}

/// `installed` once this harness is on the machine, `needed` when the reason it
/// is not, `pending` when nothing has been recorded yet.
fn install_state(store: &Store, agent: &Agent) -> String {
    match crate::install::recorded(&agent.harness) {
        Some(Ok(())) => "installed".to_string(),
        Some(Err(reason)) => reason,
        None => {
            if crate::install::program(store.data_dir(), &agent.harness).is_file() {
                "installed".to_string()
            } else {
                "pending".to_string()
            }
        }
    }
}

async fn create_agent_route(
    principal: Principal,
    State(state): State<AppState>,
    Json(mut body): Json<CreateAgentRequest>,
) -> Result<(StatusCode, Json<AgentView>), ApiError> {
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
        create::apply_archetype_defaults(state.store.data_dir(), parent_agent.as_ref(), &mut body)
            .map_err(ApiError::from_store)?;
        if let Some(scope) = body.scope.as_deref() {
            anyagents_core::parse_agent_scope(scope).map_err(ApiError::from_store)?;
        }
        let parent_id = parent_agent.as_ref().map(|p| p.id.clone());
        let (harness, model, profile) = create::resolve_subagent_defaults(
            state.store.data_dir(),
            parent_agent.as_ref(),
            body.harness.as_deref().unwrap_or("claude-code"),
            body.model,
            body.profile,
        );
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
                if agent_id != &found.id && found.parent_id.as_ref() != Some(agent_id)
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
        if !is_self {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            while std::time::Instant::now() < deadline {
                if state
                    .health
                    .get(&found.id)
                    .is_none_or(|live| !live.process_alive)
                {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        }
        if state
            .health
            .get(&found.id)
            .is_none_or(|live| !live.process_alive)
        {
            reap_archived_agent_locked(&state.store, state.store.data_dir(), &found.id);
        }
        Ok(Json(agent_view(&state.store, &state.health, &agent)?))
    })
    .await
}

pub(crate) fn reap_subagent_ephemeral_dirs(
    data_dir: &Path,
    agent_id: &anyagents_core::AgentId,
) -> bool {
    let Ok(store) = Store::open(data_dir.join("state.db")) else {
        return false;
    };
    let Ok(agents) = store.list_agents() else {
        return false;
    };
    // Delete calls this after removing the row; all surviving references still
    // protect their workdirs. A failed read must never mean no shared workdirs.
    let agent = agents.iter().find(|agent| &agent.id == agent_id);
    let parent_id = agent.and_then(|agent| agent.parent_id.as_ref());
    let mut complete = true;
    let agent_root = data_dir.join("agents").join(agent_id.as_str());
    let config_dir = agent_root.join("config");
    let seat_ids =
        detect_seat_ids(&config_dir).or_else(|| detect_seat_ids(&agent_root.join("work")));
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
        ".keel",
        "logs_2.sqlite",
        "logs_2.sqlite-wal",
        "logs_2.sqlite-shm",
        "state_5.sqlite-wal",
        "state_5.sqlite-shm",
        "thread_history_1.sqlite",
        "thread_history_1.sqlite-wal",
        "thread_history_1.sqlite-shm",
        "local-evidence",
        ".android",
        ".sylphx-remote-models.json",
        "models_cache.json",
        "model-catalog-direct.json",
    ] {
        let target = config_dir.join(rel);
        complete &= remove_path_with_seat(&target, seat_ids);
    }
    complete &= remove_path_with_seat(&agent_root.join("tmp"), seat_ids);
    complete &= remove_path_with_seat(&agent_root.join("state-import"), seat_ids);
    for dir in [&agent_root, &agent_root.join("work")] {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                if entry.file_name().to_string_lossy().starts_with(".desk-worktree.") {
                    complete &= remove_path_with_seat(&entry.path(), seat_ids);
                }
            }
        }
    }
    let _ = std::fs::remove_file(
        data_dir
            .join("locks")
            .join(format!("{}.lock", agent_id.as_str())),
    );
    let mut shared_workdirs = std::collections::HashSet::new();
    if let Some(agent) = agent {
        let _ = crate::browser::stop_profile(data_dir, &agent.name);
    }
    for other in &agents {
        let keep_protected =
            &other.id != agent_id && (other.state != "archived" || parent_id == Some(&other.id));
        if keep_protected && let Some(wd) = &other.workdir {
            let wd_path = PathBuf::from(&wd);
            if let Ok(abs) = std::path::absolute(&wd_path) {
                shared_workdirs.insert(abs);
            }
            shared_workdirs.insert(wd_path);
        }
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

    let work_dir =
        std::path::absolute(agent_root.join("work")).unwrap_or_else(|_| agent_root.join("work"));
    // Never reap an external `explicit-worktree` (`wt_path != &work_dir`) such as `/scratch/wt/...`
    // that a parent or operator created and lent to a child agent. Only reap the agent's own
    // ephemeral `agent_root/work` directory when not shared with another agent.
    if !shared_workdirs.contains(&work_dir) {
        let journaled = crate::worktree_access::grant_path(data_dir, agent_id)
            .symlink_metadata()
            .is_ok();
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
    if let Ok(seat_name) = std::fs::read_to_string(config_dir.join(".anyagents-seat")) {
        let seat_name = seat_name.trim();
        if !seat_name.is_empty()
            && let Some((_, uid, seat_gid, seat_sock_gid)) =
                crate::sandbox::seat_for_agent(seat_name)
            && uid != me
            && uid != 0
        {
            return Some((uid, seat_gid, seat_sock_gid));
        }
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
                if agent_id != &found.id && found.parent_id.as_ref() != Some(agent_id)
        ) {
            require_admin_or_operator(&state.store, &principal, "agents.pause", &found.name)?;
        }
        let agent = state
            .store
            .set_agent_state(&found.id, "paused")
            .map_err(ApiError::from_store)?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            if state
                .health
                .get(&found.id)
                .is_none_or(|live| !live.process_alive)
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
                if agent_id != &found.id && found.parent_id.as_ref() != Some(agent_id)
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
        reap_subagent_ephemeral_dirs(state.store.data_dir(), &deleted.id);
        remove_path_with_seat(&agent_dir.join("config"), seat_ids);
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
) -> Result<Json<AgentView>, ApiError> {
    spawn_store(move || {
        let found = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        if let Principal::Agent {
            name: ref caller_name,
            ..
        } = principal
        {
            let actor = Actor::Agent(caller_name.clone());
            if can_actor_message_agent(&state.store, &actor, &found).is_err() {
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
    let _ = std::fs::write(
        cfg.join(".anyagents-profile-rotated-ms"),
        format!("{}\n", now_ms()),
    );
    crate::worktree_access::restore_recorded(store.data_dir(), &found.id)
        .map_err(ApiError::from_store)?;
    clear_explicitly_archived(store.data_dir(), &found.id);
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
}

#[derive(Default, Deserialize)]
struct ConversationQuery {
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
            let matches_self = name == agent_id.as_str()
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
    spawn_store(move || inbox_of(&state.store, &agent_id, query.since, query.limit)).await
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
    Query(query): Query<ConversationQuery>,
) -> Result<Json<ConversationResponse>, ApiError> {
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
                None => state
                    .store
                    .list_agents()
                    .map_err(ApiError::from_store)?
                    .into_iter()
                    .map(|a| a.id.to_string())
                    .collect(),
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
                .clamp(1, 200),
            visible_agent_ids: visible_agent_ids.clone(),
        };
        let authorized = match visible_agent_ids.as_ref() {
            Some(ids) => ids.clone(),
            None => state
                .store
                .list_agents()
                .map_err(ApiError::from_store)?
                .into_iter()
                .map(|a| a.id.to_string())
                .collect(),
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
                    limit: query.limit.unwrap_or(20),
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
        if file_request.radius == 0 && ranked.len() > file_request.limit {
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
) -> Result<Json<InboxResponse>, ApiError> {
    let mut rows = store.inbox(agent_id, since).map_err(ApiError::from_store)?;
    if let Some(limit) = limit {
        if since.is_none() && rows.len() > limit {
            rows.drain(..rows.len() - limit);
        } else if rows.len() > limit {
            rows.truncate(limit);
        }
    }
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
    Json(body): Json<SendRequest>,
) -> Result<Json<SendResponse>, ApiError> {
    // The authenticated principal decides who the message is from; a body
    // `sender` is only a check, and a disagreement is refused with no write.
    // On a machine without the boundary, its own agent route names the sender;
    // that is the one place a body chooses the author, and an inbox is still
    // only ever its own.
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
    let store = Arc::clone(&state.store);
    let principal_for_plan = principal.clone();
    let actor_for_plan = actor.clone();
    let body_for_plan = body.clone();
    let plan = spawn_store(move || {
        plan_send(&store, &principal_for_plan, &actor_for_plan, &body_for_plan)
    })
    .await?;
    let key_body = SendRequest {
        sender: plan
            .sender_id
            .as_ref()
            .map(|id| format!("agent:{id}"))
            .or_else(|| body.sender.clone()),
        ..body.clone()
    };
    let key = send_key(&key_body);
    let store = Arc::clone(&state.store);
    spawn_store(move || send_sync(&store, &actor, body, plan, key)).await
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
}

#[derive(Debug)]
enum Target {
    Agent(String),
    Room(String),
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
    if store.share_any_group_or_chat(&sender.id, &recipient.id)
        || store.has_messaged(recipient, sender)
    {
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
    let mut deliveries = Vec::with_capacity(plan.targets.len());
    let mut skipped = Vec::new();
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
                        if !matches!(name.as_str(), "all" | "peers" | "siblings" | "team" | "children" | "parent"));
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
        .list_agents()
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
            .list_agents()
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
    store
        .list_agents()
        .unwrap_or_default()
        .into_iter()
        .filter(|agent| !body_author_is(body, agent))
        .filter(|agent| members.iter().any(|m| m == &agent.name))
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
struct ApiError {
    status: StatusCode,
    message: String,
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
            consolidate_idle_agent_memory(&store, dir.path(), &HealthBoard::default());
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
    use std::io::{Read, Write};
    use std::sync::MutexGuard;

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
        let (delivery_mode, steer_support) = declared(&agent.harness);
        let mut live = anyagents_supervisor::LiveState {
            process_alive: true,
            turn_state: anyagents_harness::TurnState::Idle,
            delivery_mode,
            steer_support,
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
        let visible = list_agents_sync_for_principal(&state, &principal)
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
        let denied = list_agents_sync_for_principal(&state, &principal)
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
        let operator = list_agents_sync_for_principal(&state, &Principal::Operator)
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
            .create_agent(anyagents_core::NewAgent::new("range-parent", "sylphx"))
            .unwrap();
        let mut new = anyagents_core::NewAgent::new("range-child", "sylphx");
        new.parent_id = Some(parent.id.clone());
        let child = store.create_agent(new).unwrap();
        let peer = store
            .create_agent(anyagents_core::NewAgent::new("range-peer", "sylphx"))
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
            .create_agent(anyagents_core::NewAgent::new("history-parent", "sylphx"))
            .unwrap();
        let mut new = anyagents_core::NewAgent::new("history-child", "sylphx");
        new.parent_id = Some(parent.id.clone());
        let child = store.create_agent(new).unwrap();
        let peer = store
            .create_agent(anyagents_core::NewAgent::new("history-peer", "sylphx"))
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
            let err = set_description_route(caller(&mind), State(state.clone()), AxumPath("self".into()), Json(serde_json::from_value(serde_json::json!({"scope":"public"})).unwrap()))
                .await.expect_err("self cannot widen scope");
            assert_eq!(err.status, StatusCode::FORBIDDEN);

            // An admin's self update is allowed and has the same atomic audit.
            store.set_agent_admin(&parent.id, true).unwrap();
            let _ = set_description_route(caller(&parent), State(state.clone()), AxumPath("self".into()), Json(serde_json::from_value(serde_json::json!({"scope":"isolated"})).unwrap()))
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
            auth: auth::Mode::Trusted {
                data_dir: dir.path().into(),
            },
        };
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            for (name, role, scope) in [("labelled", Some("Researcher"), "private"), ("task", None, "private")] {
                let body = serde_json::from_value(serde_json::json!({"name":name,"role":role,"harness":"codex","model":"test-model","workdir":"private"})).unwrap();
                let (_, Json(created)) = create_agent_route(Principal::Agent { agent_id: parent.id.clone(), name: parent.name.clone() }, State(state.clone()), Json(body)).await.unwrap_or_else(|e| panic!("{}: {}",e.status,e.message));
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
            let (local, network) =
                start_schedule_loops(Arc::clone(&store), HealthBoard::default(), false);
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
            run_with_identity(&path, Arc::new(Mutex::new(ProcessRunner)), stopped, serving)
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
        reap_archived_agents(&store, dir.path(), &HealthBoard::default());
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
        reap_archived_agents(&store, dir.path(), &HealthBoard::default());
        assert!(!blocked.exists());
        assert_eq!(
            std::fs::read(cfg.join(".anyagents-reaped")).unwrap(),
            std::fs::read(cfg.join(".anyagents-archived")).unwrap()
        );
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
        let (delivery_mode, steer_support) = declared(&child.harness);
        let mut live = anyagents_supervisor::LiveState {
            process_alive: true,
            turn_state: anyagents_harness::TurnState::Idle,
            delivery_mode,
            steer_support,
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
    fn archive_reap_parent_reconciliation_marks_one_row_without_waiting_for_descendants() {
        if single_test_worker(
            "archive_reap_parent_reconciliation_marks_one_row_without_waiting_for_descendants",
        ) {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");
        let store = Store::open(&db).unwrap();
        let parent = store
            .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
            .unwrap();
        let mut new = anyagents_core::NewAgent::new("worker", "opencode");
        new.parent_id = Some(parent.id.clone());
        let child = store.create_agent(new).unwrap();
        let mut new = anyagents_core::NewAgent::new("grandchild", "opencode");
        new.parent_id = Some(child.id.clone());
        let grandchild = store.create_agent(new).unwrap();
        store.set_agent_state(&parent.id, "archived").unwrap();
        let cfg = anyagents_core::agent_config_dir(dir.path(), &child.id);
        std::fs::create_dir_all(cfg.join(".claude")).unwrap();
        let descendant = store.lock_agent_lifecycle(&grandchild.id).unwrap();
        let scanner_store = Store::open(&db).unwrap();
        let data = dir.path().to_path_buf();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let scanner = std::thread::spawn(move || {
            reconcile_stopped_and_backed_off_agents(
                &scanner_store,
                &HealthBoard::default(),
                &data,
                &Arc::new(Mutex::new(AdminReconcileState::default())),
                now_ms(),
            );
            done_tx.send(()).unwrap();
        });
        let progressed = done_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .is_ok();
        drop(descendant);
        scanner.join().unwrap();
        assert!(
            progressed,
            "reconciliation cascaded into a contended descendant"
        );
        assert_eq!(store.agent(&child.id).unwrap().state, "archived");
        assert!(
            cfg.join(".claude").exists(),
            "reconciliation performed cleanup"
        );
        assert!(!cfg.join(".anyagents-reaped").exists());
        reconcile_stopped_and_backed_off_agents(
            &store,
            &HealthBoard::default(),
            dir.path(),
            &Arc::new(Mutex::new(AdminReconcileState::default())),
            now_ms(),
        );
        assert_eq!(store.agent(&grandchild.id).unwrap().state, "archived");
    }

    #[test]
    fn archive_reap_blocked_in_git_does_not_hold_reconcile_or_delay_other_mail() {
        if single_test_worker(
            "archive_reap_blocked_in_git_does_not_hold_reconcile_or_delay_other_mail",
        ) {
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
        store.set_agent_state(&recipient.id, "created").unwrap();
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
        let rec_data = dir.path().to_path_buf();
        let reconcile = Arc::new(Mutex::new(AdminReconcileState::default()));
        let rec = Arc::clone(&reconcile);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let scanner = std::thread::spawn(move || {
            reconcile_stopped_and_backed_off_agents(
                &rec_store,
                &HealthBoard::default(),
                &rec_data,
                &rec,
                now_ms(),
            );
            done_tx.send(()).unwrap();
        });
        let progressed_while_blocked = done_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .is_ok();
        let recipient_running = store.agent(&recipient.id).unwrap().state == "running";
        let mutex_free = reconcile.try_lock().is_ok();
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
            progressed_while_blocked && recipient_running && mutex_free,
            "unrelated mail reconciliation waited for blocked cleanup"
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
            authored_send(
                &store,
                &Actor::parse(from).unwrap_or_else(|e| panic!("{}", e.message)),
                "worker",
                "channel update",
                from,
            );
            assert_eq!(store.agent(&child.id).unwrap().state, "archived");
            assert!(has_explicit_archive_marker(dir.path(), &child.id));
        }
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
    fn stale_reconcile_population_cannot_rearchive_a_woken_parent_and_child() {
        let _env = env_lock();
        unsafe {
            std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
        }
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let parent = store
            .create_agent(anyagents_core::NewAgent::new("parent-agent", "opencode"))
            .unwrap();
        let mut new = anyagents_core::NewAgent::new("worker", "opencode");
        new.parent_id = Some(parent.id.clone());
        let child = store.create_agent(new).unwrap();
        mark_explicitly_archived(&store, dir.path(), &parent);
        let stale_population = store.list_agents().unwrap();
        // Both wake operations happen after the population scan and before
        // reconciliation takes their locks. The old parent row is not authority.
        authored_send(
            &store,
            &Actor::Operator("operator".into()),
            "parent-agent",
            "continue",
            "parent-wake",
        );
        authored_send(
            &store,
            &Actor::Agent(parent.name.clone()),
            "worker",
            "continue",
            "child-wake",
        );
        reconcile_agent_snapshot(
            &store,
            &HealthBoard::default(),
            dir.path(),
            &Arc::new(Mutex::new(AdminReconcileState::default())),
            now_ms(),
            stale_population,
        );
        assert_eq!(store.agent(&parent.id).unwrap().state, "running");
        assert_eq!(store.agent(&child.id).unwrap().state, "running");
        assert!(!has_explicit_archive_marker(dir.path(), &parent.id));
        assert!(!has_explicit_archive_marker(dir.path(), &child.id));
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
                    Principal::Operator, State(state.clone()), Json(serde_json::from_value(body).unwrap()),
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
        let Json(listed) = list_agents(Principal::Operator, State(state.clone()))
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
}

#[cfg(all(test, target_os = "linux"))]
#[path = "server_history_benchmark.rs"]
mod history_benchmark;
