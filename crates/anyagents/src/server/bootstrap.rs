//! Server startup, listeners, PostgreSQL blob hydration, lockfile, and shutdown.

use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyagents_core::{Error, Store, now_ms};
use anyagents_gateway::MemoryRoutes;
use anyagents_installer::CommandRunner;
use anyagents_supervisor::{HealthBoard, Supervisor, SystemClock};

use crate::auth;
use crate::create;
use crate::identity::ServingIdentity;
use crate::operator;
use crate::vendor::{self, HarnessHost, VendorHarness};

use super::agents::finish_interrupted_spawns;
use super::agents_lifecycle::reap_archived_agents;
use super::types::{AppState, ProcessRunner};
use super::{
    admin_auth, build_public_router, build_router, build_webhook_router, in_flight, memory_backlog,
    outbound_history, quota_rotation, serverless,
};

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
    run_with_port(data, None)
}

/// Run the server with an optional explicit `--port` override for the HTTP
/// webhook/public TCP listener.
pub fn run_with_port(data: &Path, port: Option<u16>) -> Result<(), Error> {
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let host = crate::keeper::host().map_err(Error::Message)?;
    run_with_identity_and_port(
        data,
        Arc::new(Mutex::new(ProcessRunner)),
        stop_rx,
        Arc::new(ServingIdentity::capture()),
        host,
        port,
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

#[cfg(test)]
pub(super) fn run_with_identity(
    data: &Path,
    runner: Arc<Mutex<dyn CommandRunner + Send>>,
    stop: tokio::sync::oneshot::Receiver<()>,
    identity: Arc<ServingIdentity>,
    host: HarnessHost,
) -> Result<(), Error> {
    run_with_identity_and_port(data, runner, stop, identity, host, None)
}

fn run_with_identity_and_port(
    data: &Path,
    runner: Arc<Mutex<dyn CommandRunner + Send>>,
    mut stop: tokio::sync::oneshot::Receiver<()>,
    identity: Arc<ServingIdentity>,
    host: HarnessHost,
    port_override: Option<u16>,
) -> Result<(), Error> {
    let runtime = tokio::runtime::Runtime::new()?;
    let result = runtime.block_on(async {
        tokio::select! {
            result = serve(data, runner, identity, host, port_override) => result,
            _ = &mut stop => Ok(()),
        }
    });
    // Dropping a runtime waits for every blocking task without end: a call
    // dropped at the end of the drain may still be in one (a store write
    // waiting for the writer). Those are abandoned with the process.
    runtime.shutdown_timeout(RUNTIME_STOP_WITHIN);
    result
}

/// What a stopped server's runtime waits for its blocking tasks.
const RUNTIME_STOP_WITHIN: std::time::Duration = std::time::Duration::from_secs(2);

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

/// Materialize deployment-level config, secret, profile, skill, and role blobs from
/// PostgreSQL (`public.deployment_blobs`) into a stateless/serverless replica's
/// local ephemeral data directory so `FileCredentialStore`, `Settings::load`,
/// profile lookups, skills, and custom role templates work identically across cold starts.
fn hydrate_deployment_blobs_on_start(store: &Store, data: &Path) -> Result<usize, Error> {
    let master_key = anyagents_core::MasterKey::from_env()?;
    let blobs = store.blob_store_with_key(master_key);
    let mut hydrated = 0usize;
    let secrets_dir = data.join("secrets");
    std::fs::create_dir_all(&secrets_dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(&secrets_dir, std::fs::Permissions::from_mode(0o700));
    }
    for ns in ["config", "secrets", "profiles", "skills", "roles"] {
        let entries = blobs.get_deployment_blobs_all(ns)?;
        for entry in entries {
            if anyagents_core::stateless::validate_blob_path(&entry.path).is_err() {
                continue;
            }
            let dest = match ns {
                "config" => data.join(&entry.path),
                "secrets" => data.join("secrets").join(&entry.path),
                "profiles" => data.join("profiles").join(&entry.path),
                "skills" => data.join("skills").join(&entry.path),
                "roles" => data.join("roles").join(&entry.path),
                _ => continue,
            };
            let mode = if entry.encrypted || ns == "secrets" {
                0o600
            } else {
                0o644
            };
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
                #[cfg(unix)]
                if ns == "secrets" {
                    use std::os::unix::fs::PermissionsExt as _;
                    let _ =
                        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
                }
            }
            if entry.content.is_empty() {
                std::fs::write(&dest, [])?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt as _;
                    let _ = std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(mode));
                }
            } else {
                anyagents_harness::atomic_write_nonempty_file(&dest, &entry.content, mode)?;
            }
            hydrated += 1;
        }
    }
    if hydrated > 0 {
        eprintln!("anyagents-blobs: hydrated {hydrated} deployment blob(s) from postgres");
    }
    Ok(hydrated)
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

/// Index host history in the background: what searches left for later,
/// sources due for full verification, and a bounded pre-warm pass over live agents
/// after the daemon starts. It runs on its own thread at the lowest CPU and
/// I/O priority, so it reads only when nothing else wants the machine.
fn start_host_history_indexer(store: Arc<Store>) {
    let spawned = std::thread::Builder::new()
        .name("host-history".into())
        .spawn(move || {
            lower_this_thread_priority();
            // Let the daemon finish starting before the pre-warm pass reads.
            std::thread::sleep(std::time::Duration::from_secs(60));
            store.run_host_history_indexer();
        });
    if let Err(error) = spawned {
        eprintln!("anyagents-host-history: background indexer not started: {error}");
    }
}

#[cfg(target_os = "linux")]
fn lower_this_thread_priority() {
    const IOPRIO_WHO_PROCESS: libc::c_long = 1;
    const IOPRIO_CLASS_IDLE: libc::c_long = 3;
    // SAFETY: plain syscalls that lower only the calling thread's priority.
    unsafe {
        let tid = libc::syscall(libc::SYS_gettid);
        libc::setpriority(libc::PRIO_PROCESS, tid as libc::id_t, 19);
        libc::syscall(
            libc::SYS_ioprio_set,
            IOPRIO_WHO_PROCESS,
            tid,
            IOPRIO_CLASS_IDLE << 13,
        );
    }
}

#[cfg(not(target_os = "linux"))]
fn lower_this_thread_priority() {}

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

pub(super) fn active_seat_ids(store: &Store) -> Result<Vec<String>, Error> {
    Ok(store
        .list_live_agents()?
        .into_iter()
        .filter(|agent| agent.state != "archived")
        .map(|agent| agent.id.to_string())
        .collect())
}

/// Minimum age (5 minutes) before a PostgreSQL replica's startup repair
/// releases an unadmitted agent name reservation, preventing a booting peer
/// pod from clobbering another replica's in-flight `create_agent`.
const UNADMITTED_STALE_AGE_MS: i64 = 5 * 60 * 1000;

/// Startup repair of failed agent creation reservations.
///
/// On single-host SQLite, `server.lock` excludes concurrent creation so every
/// unadmitted row is a failed creation. On multi-replica PostgreSQL, gate
/// behind a distributed `coordinator_leases` lock and only release rows older
/// than `stale_age_ms` without a fresh runtime heartbeat.
pub(crate) fn release_stale_unadmitted_names_on_start(
    store: &Store,
    stale_age_ms: i64,
) -> Result<Vec<(anyagents_core::AgentId, String)>, Error> {
    if store.is_postgres() {
        let Some(_lease) = store.try_acquire_coordinator_lease(
            "maint:unadmitted_names",
            std::time::Duration::from_secs(60),
        )?
        else {
            return Ok(Vec::new());
        };
        return store.release_unadmitted_names_older_than(stale_age_ms);
    }
    store.release_unadmitted_names()
}

async fn serve(
    data: &Path,
    sign_in: Arc<Mutex<dyn CommandRunner + Send>>,
    identity: Arc<ServingIdentity>,
    host: HarnessHost,
    port_override: Option<u16>,
) -> Result<(), Error> {
    let serverless = serverless::is_serverless_mode();
    std::fs::create_dir_all(data)?;
    let _server_lock = if serverless {
        None
    } else {
        Some(acquire_server_lock(data)?)
    };
    anyagents_toolchain::init_server(data, crate::sandbox::host_home_dir());
    let store = Arc::new(Store::open(data.join("state.db"))?);
    let remote_required = crate::sandbox_providers::SandboxMode::from_env().is_remote_required();
    let stateless_pg = store.is_postgres() && remote_required;
    if store.is_postgres()
        && (serverless || remote_required)
        && let Err(err) = hydrate_deployment_blobs_on_start(&store, data)
    {
        eprintln!("anyagents-blobs: startup hydration warning: {err}");
    }
    for (id, name) in release_stale_unadmitted_names_on_start(&store, UNADMITTED_STALE_AGE_MS)? {
        eprintln!("released name `{name}` held by failed creation {id}");
    }
    if !stateless_pg {
        crate::sandbox::reconcile_principals(&store)?;
        crate::host_memory::on_start(&store);
    }
    let key = capability_key()?;
    store.hold_capability_key(&key)?;
    // In stateless multi-replica PG mode, HTTP MCP injects `Principal::Agent`
    // directly after authenticating `remote_session_tokens`, so booting pods
    // must NEVER blindly call `operator::ensure()` when pod-local
    // `mcp-capability` is absent (which would revoke peer pods' live
    // `agent_capabilities` rows in PostgreSQL).
    if !stateless_pg {
        let agents = store.list_agents()?;
        // Keep small legacy deployments compatible without walking every archived
        // seat tree in a large registry. Wake/admission mints a capability on demand.
        let skip_archived_mint = agents.len() > 32;
        let mut mint_failures = Vec::new();
        for agent in &agents {
            if skip_archived_mint && agent.state == "archived" {
                continue;
            }
            if store.is_postgres()
                && store.has_active_capability(&agent.id).unwrap_or(false)
                && !anyagents_core::agent_config_dir(store.data_dir(), &agent.id)
                    .join(crate::operator::CAPABILITY_FILE)
                    .is_file()
            {
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
    if !crate::sandbox_providers::SandboxMode::from_env().is_remote_required()
        && operator::operator_token(data).is_some()
        && !crate::sandbox::launch_boundary(&seats)
    {
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
    if let Err(err) = crate::gateway::ensure_routes_loaded(data) {
        eprintln!("anyagents-gateway: deferred route hydration: {err}");
    }
    // The API is bound before any vendor starts: an agent that is slow, silent
    // or wedged must never stop the server from answering.
    prepare_socket(&path)?;
    let api_listener = tokio::net::UnixListener::bind(&path)?;
    set_socket_mode(&path)?;
    identity.start_hash();
    if auth.is_confined() {
        crate::sandbox::share_socket_with_seats(&path);
    }
    let kept = !serverless && matches!(host, HarnessHost::Keeper(_));
    let (supervisor, health) = if serverless {
        // Pure serverless mode (`ANYAGENTS_SERVERLESS=true`): zero long-running
        // keeper/supervisor loops, zero coordinator-side `envd` pipes or SSE
        // streams across HTTP requests.
        (None, HealthBoard::default())
    } else {
        let launch_store = Arc::clone(&store);
        // A harness kept from the last server on a gateway origin that moved
        // would send its token wherever that port now leads: it is replaced.
        let refused = if gateway.moved {
            gateway.agents
        } else {
            Vec::new()
        };
        let custody = crate::keeper::custody(data, &host, refused);
        let sup = Supervisor::<VendorHarness>::start_with_custody(
            Arc::clone(&store),
            Arc::new(SystemClock::new()),
            Arc::new(move |request| vendor::vendor_launch_hosted(request, &launch_store, &host)),
            vendor::child_env(),
            custody,
        )
        .map_err(Error::Message)?;
        let idle_release_store = Arc::clone(&store);
        sup.set_on_idle_released(Arc::new(move |agent_id, cfg| {
            crate::sandbox_exec::handle_agent_idle_release(&idle_release_store, agent_id, cfg);
        }));
        let board = sup.board();
        (Some(Retaining(Some(sup), kept)), board)
    };
    if !serverless && auth.is_confined() {
        crate::sandbox::share_socket_with_seats(&data.join("wake.sock"));
    }
    let timer_backend = crate::timer::build_timer_backend(
        Arc::clone(&store),
        health.clone(),
        tokio::runtime::Handle::current(),
    );
    let state = AppState {
        store: Arc::clone(&store),
        health,
        sign_in,
        auth,
        identity,
        profiles: Default::default(),
        timer_backend,
    };
    if serverless || crate::server::timer_fire::configured_timer_scheduler_url().is_some() {
        let rec_store = Arc::clone(&store);
        let rec_timer = Arc::clone(&state.timer_backend);
        tokio::spawn(async move {
            if let Err(err) =
                super::serverless_maintenance::startup_reconcile_serverless(rec_store, rec_timer)
                    .await
            {
                eprintln!("anyagents-serverless: startup reconciliation warning: {err}");
            }
        });
    }
    let _ = boundary;
    let listener = api_listener;
    let poller = if serverless {
        None
    } else {
        Some(
            crate::telegram::Poller::start(Arc::clone(&store), data.to_path_buf())
                .map_err(Error::Message)?,
        )
    };
    // Set once a stop is requested: every listener (the socket and both TCP
    // listeners) stops taking connections at once, calls running on any of
    // them drain together, and every background loop ends at once.
    let (stopping, _) = tokio::sync::watch::channel(false);
    if !serverless
        && std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
        && !crate::sandbox::is_isolated_test_env()
    {
        quota_rotation::start(Arc::clone(&store));
        start_host_history_indexer(Arc::clone(&store));
        let memory_backlog = memory_backlog::start(&store);
        let reaper_root = data.to_path_buf();
        let reaper_store = Arc::clone(&store);
        let reaper_health = state.health.clone();
        let mut reaper_stop = stopping.subscribe();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            let mut tick_count = 0u64;
            loop {
                tokio::select! {
                    biased;
                    _ = reaper_stop.wait_for(|stop| *stop) => break,
                    _ = interval.tick() => {}
                }
                tick_count = tick_count.wrapping_add(1);
                let run_verify = tick_count.is_multiple_of(15);
                let first_pass = tick_count == 1;
                let root = reaper_root.clone();
                let store = Arc::clone(&reaper_store);
                let health = reaper_health.clone();
                let memory_backlog = Arc::clone(&memory_backlog);
                let _ = tokio::task::spawn_blocking(move || {
                    // In multi-replica PostgreSQL mode, elect one singleton
                    // maintenance leader per tick via `public.coordinator_leases`
                    // so N replicas never race on reaping, rotation, or renewals.
                    let _singleton_lease = if store.is_postgres() {
                        match store.try_acquire_coordinator_lease(
                            "maint:singleton",
                            std::time::Duration::from_secs(90),
                        ) {
                            Ok(Some(guard)) => Some(guard),
                            Ok(None) => return,
                            Err(err) => {
                                eprintln!(
                                    "anyagents-maint: deferred singleton lease acquisition: {err}"
                                );
                                return;
                            }
                        }
                    } else {
                        None
                    };
                    // The daemon is the long-lived writer: keep the WAL bounded
                    // from here so it never grows while readers overlap.
                    if !store.is_postgres() {
                        match store.checkpoint_wal() {
                            Ok(c)
                                if c.passive_busy
                                    || c.checkpointed_frames < c.log_frames
                                    || c.log_frames > WAL_REPORT_FRAMES => eprintln!(
                                "anyagents-store: wal checkpoint log={} checkpointed={} busy={} truncated={}",
                                c.log_frames, c.checkpointed_frames, c.passive_busy, c.truncated
                            ),
                            Ok(_) => {}
                            Err(error) => {
                                eprintln!("anyagents-store: wal checkpoint failed: {error}")
                            }
                        }
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
                    if let Err(error) = crate::remote::rotate_due(&store) {
                        eprintln!("anyagents-remote: session rotation deferred: {error}");
                    }
                    record_live_quota_resets(&store, &root, &health);
                    crate::create::sync_fresher_bound_agent_credentials_in(&store, &root);
                    #[cfg(unix)]
                    crate::create::sync_canonical_skills(&root);
                    // 2-tier idle-TTL sandbox sweeper under singleton maintenance lease:
                    // releases compute ONLY when idle_since_ms + idle_ttl_ms <= now AND
                    // inflight_children == 0 AND no active turn (preserving persistent volumes).
                    if let Err(err) = crate::sandbox_exec::sweep_idle_sandboxes(
                        &store,
                        &root,
                        anyagents_core::now_ms(),
                    ) {
                        eprintln!("anyagents-sandbox: idle sweep deferred: {err}");
                    }
                    if tick_count.is_multiple_of(5)
                        && let Ok(live) = store.list_live_agents()
                    {
                        for a in live {
                            let cfg = anyagents_core::agent_config_dir(&root, &a.id);
                            if let Err(err) = crate::sandbox_providers::renew_lease_for_config_dir(&cfg) {
                                eprintln!(
                                    "anyagents-sandbox: lease renew failed for `{}`: {err}",
                                    a.name
                                );
                            }
                        }
                    }
                })
                .await;
            }
        });
        if !crate::sandbox_providers::SandboxMode::from_env().is_remote_required() {
            let updater_root = data.to_path_buf();
            let updater_store = Arc::clone(&store);
            let mut updater_stop = stopping.subscribe();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval_at(
                    tokio::time::Instant::now() + std::time::Duration::from_secs(60),
                    std::time::Duration::from_secs(6 * 3600),
                );
                loop {
                    tokio::select! {
                        biased;
                        _ = updater_stop.wait_for(|stop| *stop) => break,
                        _ = interval.tick() => {}
                    }
                    let root = updater_root.clone();
                    let store_for_update = Arc::clone(&updater_store);
                    let _ = tokio::task::spawn_blocking(move || {
                        let catalog = match anyagents_installer::Catalog::load(&root) {
                            Ok(catalog) => catalog,
                            Err(err) => {
                                eprintln!(
                                    "anyagents-updater: failed to load catalog overrides: {err}"
                                );
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
        }
        crate::schedule::start_watch_loops(
            Arc::clone(&store),
            state.health.clone(),
            stopping.subscribe(),
        );
    } else if serverless
        && std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
        && !crate::sandbox::is_isolated_test_env()
    {
        spawn_serverless_startup_backlog_drain(Arc::clone(&store), stopping.subscribe());
    }
    // `public_listen` decides whether the local TCP listener is also the
    // public one. A configuration file that cannot be read fails closed: no
    // public listener, and the local listener defaults to loopback.
    let (public_listen, public_configured) = match crate::remote::Settings::load(data) {
        Ok(settings) => (settings.public_listen, settings.public_listen.is_some()),
        Err(why) => {
            eprintln!(
                "warning: {why}; the public listener is off and the local listener defaults to loopback"
            );
            (None, true)
        }
    };
    if public_listen.is_none() {
        let _ = std::fs::remove_file(data.join("public.addr"));
    }
    let mut public_listener = None;
    if let Some(addr) = public_listen {
        match tokio::net::TcpListener::bind(addr).await {
            Ok(tcp_listener) => {
                if let Ok(local) = tcp_listener.local_addr() {
                    let addr_path = data.join("public.addr");
                    if let Err(err) = anyagents_harness::atomic_write_nonempty_file(
                        &addr_path,
                        format!("{local}\n").as_bytes(),
                        0o644,
                    ) {
                        eprintln!("cannot persist {}: {err}", addr_path.display());
                    }
                }
                public_listener = Some(tcp_listener);
            }
            Err(err) => {
                eprintln!("warning: public TCP listener ({addr}) did not bind: {err}");
            }
        }
    }
    let mut webhook_listener = None;
    if let Some(addr) = webhook_listen_addr(public_configured, port_override) {
        if public_configured && !serverless && !binds_loopback(&addr) {
            eprintln!(
                "warning: the local TCP listener ({addr}) is not loopback while `public_listen` is set; \
                 it accepts every local credential, so bind it to 127.0.0.1"
            );
        }
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
                webhook_listener = Some(tcp_listener);
            }
            Err(err) => {
                eprintln!("warning: webhook TCP listener ({addr}) did not bind: {err}");
            }
        }
    }
    if !serverless {
        // A spawn the last server stopped part way gets the rest of it (most
        // often its prompt) before any call is answered on any listener: a call
        // to list, inspect or delete that agent then sees it whole. Every
        // listener is already bound, so a client connecting meanwhile waits in
        // the kernel listen backlog rather than finding no server.
        let store = Arc::clone(&store);
        let data = data.to_path_buf();
        let _ = tokio::task::spawn_blocking(move || {
            if let Err(err) = create::backfill_historical_agent_roles(&store, &data) {
                eprintln!("anyagents: historical role backfill warning: {err}");
            }
            finish_interrupted_spawns(&store, &data);
        })
        .await;
    }
    let public_task = public_listener.map(|tcp_listener| {
        let public_app = build_public_router(state.clone());
        let stop = stopped(&stopping);
        tokio::spawn(async move {
            let _ = axum::serve(tcp_listener, public_app)
                .with_graceful_shutdown(stop)
                .await;
        })
    });
    let webhook_task = webhook_listener.map(|tcp_listener| {
        let webhook_app = build_webhook_router(state.clone());
        let stop = stopped(&stopping);
        tokio::spawn(async move {
            let _ = axum::serve(tcp_listener, webhook_app)
                .with_graceful_shutdown(stop)
                .await;
        })
    });
    let app = build_router(state);
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let serving = axum::serve(listener, app)
        .with_graceful_shutdown(stopped(&stopping))
        .into_future();
    tokio::pin!(serving);
    let mut releasing = None;
    let result = tokio::select! {
        result = &mut serving => result,
        () = stop_requested(&mut terminate) => {
            // From here no new connection is taken: a call made now waits on
            // the agent's side for the next server (`mcp::RESTART_WAIT`).
            // Calls already running finish first: dropping a call's future
            // does not stop its blocking work, and what does stop it is the
            // process exiting, which can cut a mutation between its steps.
            eprintln!(
                "anyagents: stopping; no new calls are taken; waiting up to {}s for {} mutating \
                 call(s) still running to finish (reads end with the process){}",
                MUTATION_DRAIN_WITHIN.as_secs(),
                in_flight::MUTATIONS.running(),
                if kept {
                    format!("; this process exits within {}s", STOP_WITHIN.as_secs())
                } else {
                    String::new()
                }
            );
            if kept {
                // A keeper still starting is the next server's to take over:
                // a call waiting for its `ready` (up to three minutes) returns
                // now instead of holding the stop.
                anyagents_keeper::abandon_starts();
                exit_after(STOP_WITHIN);
            }

            let preserve_webhook = preserve_webhooks_on_shutdown(data);
            if !preserve_webhook && let Some(ref poller) = poller {
                // A long poll ends at Telegram, not here: signal the poller to
                // stop so released long polls exit instead of re-entering a 30s
                // wait, and release every bot while the calls drain, so each
                // bot's last offset is written before shutdown.
                poller.signal_stop();
                let release_store = Arc::clone(&store);
                releasing = Some(tokio::task::spawn_blocking(move || {
                    crate::telegram::poller::release_every_bot(
                        &release_store,
                        release_store.data_dir(),
                        TELEGRAM_RELEASE_WITHIN,
                    )
                }));
            }
            let _ = stopping.send(true);
            // `serving` is polled first so the Unix listener closes at once,
            // while `settling` keeps waiting even after `serving` has no open
            // socket connections (for TCP mutations and detached blocking work).
            let settling = in_flight::MUTATIONS.settle(MUTATION_DRAIN_WITHIN);
            tokio::pin!(settling);
            let mut serving_done = false;
            let mut serve_result = Ok(());
            let left = loop {
                tokio::select! {
                    biased;
                    result = &mut serving, if !serving_done => {
                        serving_done = true;
                        serve_result = result;
                    }
                    left = &mut settling => break left,
                }
            };
            if kept {
                // Nothing left to wait for once the mutations are done: the
                // answers, the Telegram release and the joins after it get a
                // few seconds, then the process exits whatever still runs.
                exit_after(STOP_AFTER_DRAIN_WITHIN);
            }
            if left > 0 {
                eprintln!(
                    "anyagents: {left} mutating call(s) still running after {}s end with \
                     the process; each caller is told its call may or may not have \
                     applied",
                    MUTATION_DRAIN_WITHIN.as_secs()
                );
            }
            // Answers of calls that just finished are written out across every
            // listener; reads still running end with the process.
            let flush = async {
                if !serving_done {
                    serve_result = (&mut serving).await;
                }
                if let Some(task) = public_task {
                    let _ = task.await;
                }
                if let Some(task) = webhook_task {
                    let _ = task.await;
                }
            };
            let _ = tokio::time::timeout(RESPONSE_FLUSH_WITHIN, flush).await;
            serve_result
        }
    };
    let preserve_webhook = preserve_webhooks_on_shutdown(data);
    let released = if preserve_webhook {
        Ok(())
    } else {
        match releasing {
            Some(task) => task.await.unwrap_or_else(|error| Err(error.to_string())),
            None => {
                if let Some(ref poller) = poller {
                    poller.signal_stop();
                    crate::telegram::poller::release_every_bot(
                        &store,
                        store.data_dir(),
                        TELEGRAM_RELEASE_WITHIN,
                    )
                } else {
                    Ok(())
                }
            }
        }
    };
    if let Err(reason) = released {
        eprintln!("anyagents-telegram: shutdown did not release every bot: {reason}");
    }
    if let Some(poller) = poller {
        if preserve_webhook {
            // Stateless webhook pods keep Telegram webhooks active on pod
            // SIGTERM so rolling replacements never drop incoming updates.
            std::mem::forget(poller);
        } else {
            poller.stop();
        }
    }
    drop(supervisor);
    let _ = std::fs::remove_file(&path);
    result.map_err(Error::from)
}

fn preserve_webhooks_on_shutdown(data: &Path) -> bool {
    serverless::is_serverless_mode()
        || crate::sandbox_providers::SandboxMode::from_env().is_remote_required()
        || std::env::var_os("KUBERNETES_SERVICE_HOST").is_some()
        || std::env::var("ANYAGENTS_PUBLIC_URL")
            .ok()
            .is_some_and(|v| !v.trim().is_empty())
        || crate::remote::require_public_url(data).is_ok()
}

/// How long a stopping server waits for the mutating calls it is running
/// (spawn, delete, send, schedules, any call but a read) to finish before it
/// exits. No connection is taken meanwhile; the agents' tool servers wait for
/// the next server (`mcp::RESTART_WAIT`). A mutation still running after it
/// ends with the process, and its caller is told the call may or may not have
/// applied.
///
/// A spawn, the longest common mutation, typically takes a few seconds; a
/// minute covers one that provisions a large checkout, and keeps a restart
/// well inside the shortest long tool deadline a harness sets (three minutes).
const MUTATION_DRAIN_WITHIN: std::time::Duration = std::time::Duration::from_secs(60);

/// Once the mutations are done, how long their answers get to be written out
/// before the server stops serving; reads still running end with the process.
const RESPONSE_FLUSH_WITHIN: std::time::Duration = std::time::Duration::from_secs(2);

/// The longest a server whose harnesses live in keepers takes to exit once
/// told to stop. Past it the process exits at once: its harnesses stay with
/// their keepers for the next server, exactly as after a crash, and its store
/// writes are transactions, so a write cut short is not half made.
pub(crate) const STOP_WITHIN: std::time::Duration = std::time::Duration::from_secs(
    MUTATION_DRAIN_WITHIN.as_secs() + STOP_AFTER_DRAIN_WITHIN.as_secs(),
);

/// Once the mutations are done (or their wait is over), the longest a server
/// whose harnesses live in keepers takes to exit: the answers are written out
/// ([`RESPONSE_FLUSH_WITHIN`]), the Telegram release and the joins after it
/// finish, and the runtime abandons its blocking work. Background loops (the
/// schedule timers and their external probes among them) already ended at the
/// stop. The budget includes response flushing, nine-second desktop session
/// shutdown, and final cleanup.
const STOP_AFTER_DRAIN_WITHIN: std::time::Duration = std::time::Duration::from_secs(15);

/// The wait for Telegram to hand back every bot's long poll at shutdown. It
/// runs beside the drain. A bot not released in time is polled again by the
/// next server from its last written offset; the store deduplicates updates
/// by their source key.
const TELEGRAM_RELEASE_WITHIN: std::time::Duration = std::time::Duration::from_secs(10);

/// Resolves once the server is told to stop (`stopping` set), or when the
/// server is gone: every listener stops taking connections then.
pub(super) fn stopped(
    stopping: &tokio::sync::watch::Sender<bool>,
) -> impl std::future::Future<Output = ()> + Send + 'static {
    let mut told = stopping.subscribe();
    async move {
        let _ = told.wait_for(|stop| *stop).await;
    }
}

/// SIGINT or SIGTERM.
async fn stop_requested(terminate: &mut tokio::signal::unix::Signal) {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}

/// End the process after `within`, however its stop is going: a worker in a
/// call to a keeper that does not answer, a store write waiting for the
/// writer, or a poller join cannot keep it alive. Only for a server whose
/// harnesses live in keepers, which outlive it by design.
fn exit_after(within: std::time::Duration) {
    let _ = std::thread::Builder::new()
        .name("anyagents-stop-deadline".into())
        .spawn(move || {
            std::thread::sleep(within);
            eprintln!(
                "anyagents: stop still running after {}s; exiting now (harnesses stay with \
                 their keepers for the next server)",
                within.as_secs()
            );
            std::process::exit(0);
        });
}

/// Consolidate idle agents' topic files without changing the active session's
/// immutable snapshot. Updated files are pinned only at the next explicit session.
/// Only agents in the backlog are visited; one leaves once its consolidation
/// has caught up with its mailbox.
pub(super) fn consolidate_idle_agent_memory(
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

/// How this server authenticates. A machine started without an operator token
/// serves the agents on its own machine, and names them by the route it serves:
/// its data directory, as a start script exports it to every harness. With an
/// operator token the server is the identity boundary and reads nothing from a
/// name.
fn deployment_mode(data: &Path) -> auth::Mode {
    if serverless::is_serverless_mode() || admin_auth::is_oidc_configured() {
        return auth::Mode::from_env(data);
    }
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

/// The TCP listener for the local API surface: readiness, Telegram webhooks
/// and MCP for every credential kind. Until a deployment configures
/// `public_listen` it is also the public one and keeps the `0.0.0.0:8788`
/// default; once the public listener exists it defaults to loopback.
fn parse_port_env(name: &str) -> Option<u16> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .and_then(|v| v.parse::<u16>().ok())
        .filter(|&p| p > 0)
}

fn webhook_listen_addr(public_configured: bool, port_override: Option<u16>) -> Option<String> {
    local_listen_addr_with_port(
        std::env::var("ANYAGENTS_WEBHOOK_LISTEN").ok().as_deref(),
        port_override,
        parse_port_env("ANYAGENTS_PORT"),
        parse_port_env("PORT"),
        serverless::is_serverless_mode(),
        std::env::var_os("ANYAGENTS_FAKE_MODE").is_some()
            || std::env::var_os("ANYAGENTS_TELEGRAM_API").is_some(),
        public_configured,
    )
}

#[cfg(test)]
pub(super) fn local_listen_addr(
    explicit: Option<&str>,
    test_double: bool,
    public_configured: bool,
) -> Option<String> {
    local_listen_addr_with_port(
        explicit,
        None,
        None,
        None,
        false,
        test_double,
        public_configured,
    )
}

pub(super) fn local_listen_addr_with_port(
    explicit: Option<&str>,
    cli_port: Option<u16>,
    anyagents_port: Option<u16>,
    std_port: Option<u16>,
    serverless: bool,
    test_double: bool,
    public_configured: bool,
) -> Option<String> {
    if let Some(addr) = explicit {
        let trimmed = addr.trim();
        if trimmed.is_empty() || trimmed == "0" || trimmed.eq_ignore_ascii_case("off") {
            return None;
        }
        return Some(trimmed.to_string());
    }
    if test_double && cli_port.is_none() {
        return None;
    }
    if let Some(port) = cli_port.or(anyagents_port).or(std_port) {
        let host = if serverless || std_port.is_some() || !public_configured {
            "0.0.0.0"
        } else {
            "127.0.0.1"
        };
        return Some(format!("{host}:{port}"));
    }
    Some(if public_configured && !serverless {
        "127.0.0.1:8788".to_string()
    } else {
        "0.0.0.0:8788".to_string()
    })
}

/// Whether `addr` binds only the loopback interface.
pub(super) fn binds_loopback(addr: &str) -> bool {
    addr.parse::<std::net::SocketAddr>()
        .map(|addr| addr.ip().is_loopback())
        .unwrap_or_else(|_| addr.starts_with("localhost:"))
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
    let app = crate::gateway::http_router_with(routes, usage);
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

/// Spawn a lightweight one-shot startup backlog drain in serverless mode so
/// undelivered messages queued across coordinator restarts are dispatched promptly
/// without waiting for the next external tick. Cancels cleanly on shutdown.
fn spawn_serverless_startup_backlog_drain(
    store: Arc<Store>,
    stop: tokio::sync::watch::Receiver<bool>,
) {
    tokio::spawn(run_serverless_startup_backlog_drain(
        store,
        stop,
        std::time::Duration::from_secs(2),
    ));
}

async fn run_serverless_startup_backlog_drain(
    store: Arc<Store>,
    mut stop: tokio::sync::watch::Receiver<bool>,
    delay: std::time::Duration,
) {
    tokio::select! {
        biased;
        _ = stop.wait_for(|stopped| *stopped) => return,
        _ = tokio::time::sleep(delay) => {}
    }
    let store_scan = Arc::clone(&store);
    let pending = tokio::task::spawn_blocking(move || {
        if let Err(err) = store_scan.reconcile_all_stale_serverless_open_turns()
            && !err.is_missing_schema_object()
        {
            eprintln!("anyagents-serverless: startup open-turn reconcile failed: {err}");
        }
        match store_scan.list_running_agents_with_undelivered(24) {
            Ok(agents) => agents,
            Err(err) => {
                eprintln!("anyagents-serverless: startup backlog scan failed: {err}");
                Vec::new()
            }
        }
    })
    .await
    .unwrap_or_default();

    for aid in pending {
        if *stop.borrow() {
            break;
        }
        let store_disp = Arc::clone(&store);
        let call = in_flight::carry();
        tokio::spawn(async move {
            let _call = call;
            if let Err(err) =
                serverless::dispatch_next_mailbox_message_if_serverless(store_disp, &aid).await
            {
                eprintln!(
                    "anyagents-serverless: startup backlog dispatch failed for `{aid}`: {err}"
                );
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn serverless_startup_backlog_drain_cancels_cleanly_on_shutdown() {
        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(run_serverless_startup_backlog_drain(
            store,
            stop_rx,
            std::time::Duration::from_secs(60),
        ));
        let _ = stop_tx.send(true);
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .expect("startup drain must exit immediately on shutdown")
            .unwrap();
    }
}
