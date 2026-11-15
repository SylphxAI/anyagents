//! The server's desktop sessions, one per profile directory, each held by
//! one agent at a time.
//!
//! A session starts on the first browser or session-computer call for a
//! profile; the caller acquires its lease, and every other agent's call for
//! that profile is refused with `DESKTOP_IN_USE` until the session stops. It
//! stops on `close`, after `ANYAGENTS_BROWSER_IDLE_SECS` without calls
//! (unless a person holds control), when the agent holding it is deleted or
//! archived, and on server shutdown. The profile directory is the key: the
//! browser refuses to share one profile, so neither does this registry.
//!
//! Binaries come from the toolchain (ADR 0005): `chromium` (browser),
//! `sway` (browser) and `wayvnc` (browser.takeover).
//!
//! Environment (all optional):
//!
//! | Variable | Meaning | Default |
//! | --- | --- | --- |
//! | `ANYAGENTS_BROWSER_SEED_FLAG` | flag that takes the profile's fingerprint seed, e.g. `--fingerprint` | none |
//! | `ANYAGENTS_BROWSER_ARGS` | extra browser arguments, whitespace separated | [`DEFAULT_BROWSER_ARGS`] |
//! | `ANYAGENTS_BROWSER_PROXY` | proxy for sessions started without `proxy` | none |
//! | `ANYAGENTS_BROWSER_LANGUAGES` / `ANYAGENTS_BROWSER_TIMEZONE` | identity of newly created profiles | `en-US,en` / `UTC` |
//! | `ANYAGENTS_BROWSER_IDLE_SECS` | idle seconds before a session stops | 900 |
//!
//! Where people reach the takeover page is decided in [`super::takeover`].

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use agent_workstation::{
    BrowserConfig, DesktopSession, Holder, Profile, Proxy, SessionConfig, TakeoverConfig,
};
use anyagents_core::now_ms;

use super::takeover::{self, Tunnel};
use super::{ToolError, server_profile_dir};

const DEFAULT_IDLE_SECS: i64 = 15 * 60;
// One total caller budget, including a slot held by startup or an in-flight call.
const SHUTDOWN_WAIT: std::time::Duration = std::time::Duration::from_secs(9);
const HEALTH_WAIT: std::time::Duration = std::time::Duration::from_secs(2);
/// Browser arguments when `ANYAGENTS_BROWSER_ARGS` is unset. No sandbox, as
/// before: containers without user namespaces cannot start Chromium's. No
/// crash-restore prompt: profiles the previous implementation stopped with a
/// signal are marked crashed, and the prompt would cover every one of their
/// desktops until someone dismissed it.
pub const DEFAULT_BROWSER_ARGS: [&str; 3] = [
    "--no-sandbox",
    "--hide-crash-restore-bubble",
    "--disable-dev-shm-usage",
];

/// A running session and the caller state that belongs to it.
#[derive(Debug)]
pub struct Live {
    /// The desktop.
    pub session: Arc<DesktopSession>,
    /// Profile name.
    pub profile: String,
    /// Proxy the session was started with; `Some("")` is an explicit direct
    /// connection.
    pub proxy: Option<String>,
    last_used_ms: AtomicI64,
    started_at_ms: i64,
    /// The takeover page's mount key on the public listener, when mounted.
    mount: Option<String>,
    /// A quick tunnel started for a takeover on a host without a public entry.
    tunnel: tokio::sync::Mutex<Option<Tunnel>>,
    watcher: tokio::task::JoinHandle<()>,
    /// Hand-backs from a person so far.
    hand_backs: tokio::sync::watch::Receiver<u64>,
    /// Hand-backs counted when the latest takeover was requested.
    awaited: AtomicU64,
    runtime_dir: PathBuf,
}

impl Live {
    /// Record a call (idle reaping counts from the last one).
    pub fn touch(&self) {
        self.last_used_ms.store(now_ms(), Ordering::Relaxed);
    }

    /// Last call time.
    pub fn last_used_ms(&self) -> i64 {
        self.last_used_ms.load(Ordering::Relaxed)
    }

    /// Start time.
    pub fn started_at_ms(&self) -> i64 {
        self.started_at_ms
    }

    /// `"agent"` or `"human"`.
    pub fn holder(&self) -> &'static str {
        match self.session.gate().holder() {
            Holder::Agent => "agent",
            Holder::Human { .. } => "human",
        }
    }

    /// The agent holding the session.
    pub fn leaseholder(&self) -> Option<String> {
        self.session.leaseholder().map(|l| l.principal)
    }

    /// Takeover page as people reach it now.
    pub fn takeover_url(&self) -> Option<String> {
        self.session.takeover_url().map(|u| u.to_string())
    }

    /// A person is asked to take over: the next hand-back completes it.
    pub fn expect_hand_back(&self) {
        self.awaited
            .store(*self.hand_backs.borrow(), Ordering::Release);
    }

    /// Wait up to `wait` for a hand-back after the latest takeover request;
    /// `true` once one happened, even before this call.
    pub async fn wait_for_hand_back(&self, wait: std::time::Duration) -> bool {
        let awaited = self.awaited.load(Ordering::Acquire);
        let mut rx = self.hand_backs.clone();
        let done = tokio::time::timeout(wait, rx.wait_for(|n| *n > awaited)).await;
        self.touch();
        matches!(done, Ok(Ok(_)))
    }

    /// Publish the takeover page for a person: on a host without a public
    /// entry, start a quick tunnel to it the first time (P11: the page
    /// accepts the tunnel's origin once it is set, so no tunnel starts with
    /// every session).
    pub async fn publish_takeover(&self) {
        if self.mount.is_some() {
            return;
        }
        let mut tunnel = self.tunnel.lock().await;
        if tunnel.is_some() {
            return;
        }
        let Some(addr) = self.session.takeover_addr() else {
            return;
        };
        if let Some((url, started)) = takeover::start_tunnel(addr).await
            && self.session.set_takeover_public_url(url).is_ok()
        {
            *tunnel = Some(started);
        }
    }
}

type Slot = Arc<tokio::sync::Mutex<Option<Arc<Live>>>>;

static SESSIONS: LazyLock<std::sync::Mutex<HashMap<PathBuf, Slot>>> =
    LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

fn slot(key: &Path) -> Slot {
    let mut map = SESSIONS.lock().unwrap_or_else(|p| p.into_inner());
    map.entry(key.to_path_buf()).or_default().clone()
}

/// The running session for `profile`, without starting one. `None` also while
/// another call is starting it.
pub fn running(data: &Path, profile: &str) -> Option<Arc<Live>> {
    let key = server_profile_dir(data, profile, false).ok()??;
    let slot = SESSIONS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&key)
        .cloned()?;
    slot.try_lock().ok().and_then(|g| g.clone())
}

/// The session for `profile`, started on first use, with its lease held by
/// `principal`. Another agent's lease is `DESKTOP_IN_USE`.
///
/// `proxy` is the request's proxy (`None` keeps a running session's, or uses
/// `ANYAGENTS_BROWSER_PROXY` for a new one). A different proxy than the
/// running session's is refused: changing routing would discard open tabs or
/// interrupt a person.
pub async fn acquire(
    data: &Path,
    profile: &str,
    principal: &str,
    proxy: Option<&str>,
) -> Result<Arc<Live>, ToolError> {
    let key = server_profile_dir(data, profile, true)
        .map_err(|e| ToolError::invalid(e.to_string()))?
        .ok_or_else(|| ToolError::invalid("browser profile is missing"))?;
    let slot = slot(&key);
    let mut guard = tokio::time::timeout(SHUTDOWN_WAIT, slot.clone().lock_owned())
        .await
        .map_err(|_| session_busy())?;
    let mut replacement_proxy = None;
    if let Some(live) = guard.as_ref() {
        // Authorize even a closed session before probing or replacing it. The
        // engine's lease() checks closed first; its gate preserves the owner.
        live.session
            .gate()
            .acquire(principal)
            .map_err(|e| in_use(profile, e))?;
        if let Some(requested) = proxy
            && live.proxy.as_deref().unwrap_or("") != requested
        {
            return Err(ToolError::invalid(
                "browser proxy change requires closing the profile first; call close, then reopen with the desired proxy (or an empty string for direct access)",
            ));
        }
        if cached_healthy(live, &key).await? {
            live.touch();
            return Ok(live.clone());
        }
        // Never restart underneath a person who took control during the probe.
        live.session
            .gate()
            .check(principal)
            .map_err(ToolError::from)?;
        replacement_proxy = Some(live.proxy.clone());
        // Keep the SAME slot locked through graceful close. Cancelling an
        // acquire or hitting its deadline must not permit a second writer.
        let principal = principal.to_owned();
        let closing = tokio::spawn(async move {
            let live = guard.as_ref().expect("live session").clone();
            live.session
                .gate()
                .check(&principal)
                .map_err(ToolError::from)?;
            shutdown(live).await;
            *guard = None;
            Ok::<_, ToolError>(guard)
        });
        guard = wait_for_shutdown(closing, SHUTDOWN_WAIT).await??;
    }
    let proxy = super::proxy::resolve(
        proxy,
        replacement_proxy.as_ref().map(|p| p.as_deref()),
        environment_proxy,
    )?;
    let live = Arc::new(start(data, &key, profile, principal, proxy).await?);
    *guard = Some(live.clone());
    Ok(live)
}

fn session_busy() -> ToolError {
    ToolError {
        code: "STARTING",
        message: "desktop is still starting or closing; retry after it finishes".into(),
        hint: "Inspect browser status or retry after startup or cleanup finishes.",
        retryable: true,
    }
}

/// Only read-only checks run here, before dispatch. Mutating calls are never
/// retried: losing their reply still means their outcome is uncertain.
async fn cached_healthy(live: &Live, dir: &Path) -> Result<bool, ToolError> {
    if matches!(live.session.gate().holder(), Holder::Human { .. }) {
        // Takeover/status/release must remain usable; observations are gated.
        return Ok(true);
    }
    if live.session.is_closed() {
        return Ok(false);
    }
    transport_healthy(&live.runtime_dir, dir).await
}

async fn transport_healthy(runtime_dir: &Path, dir: &Path) -> Result<bool, ToolError> {
    // Pinned engine b740702 binds this exact private IPC path. A missing or
    // refused socket proves compositor loss; resource/permission errors do not.
    match tokio::time::timeout(
        HEALTH_WAIT,
        tokio::net::UnixStream::connect(runtime_dir.join("sway.sock")),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(e))
            if matches!(
                e.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            return Ok(false);
        }
        Ok(Err(e)) => return Err(agent_workstation::Error::Io(e).into()),
        Err(_) => return Err(session_busy()),
    }
    // Read only the engine's validated loopback endpoint. Do not touch CDP:
    // timing out a command must not poison the shared connection or replay work.
    let text = match tokio::time::timeout(
        HEALTH_WAIT,
        tokio::fs::read_to_string(dir.join("user-data/DevToolsActivePort")),
    )
    .await
    .map_err(|_| session_busy())?
    {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(agent_workstation::Error::Io(e).into()),
    };
    let mut lines = text.lines();
    let port = lines
        .next()
        .and_then(|line| line.trim().parse::<u16>().ok())
        .filter(|port| *port != 0);
    let valid_path = lines
        .next()
        .is_some_and(|line| line.trim().starts_with("/devtools/browser/"));
    let Some(port) = port.filter(|_| valid_path) else {
        return Err(ToolError::invalid(
            "invalid browser debugging endpoint file",
        ));
    };
    match tokio::time::timeout(
        HEALTH_WAIT,
        tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port)),
    )
    .await
    {
        Ok(Ok(_)) => Ok(true),
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::ConnectionRefused => Ok(false),
        Ok(Err(e)) => Err(agent_workstation::Error::Io(e).into()),
        Err(_) => Err(session_busy()),
    }
}

/// A clear refusal for an agent whose profile another agent holds.
fn in_use(profile: &str, e: agent_workstation::Error) -> ToolError {
    match e {
        agent_workstation::Error::Leased { principal, since } => ToolError {
            code: "DESKTOP_IN_USE",
            message: format!(
                "browser profile `{profile}` is in use by agent `{principal}` since {since}; only \
                 one agent drives a profile's desktop at a time"
            ),
            hint: "Use another `profile`, or ask that agent to `close` it (logins stay on disk); \
                   it also frees after the idle timeout or when that agent is deleted.",
            retryable: false,
        },
        other => other.into(),
    }
}

fn environment_proxy() -> Result<Option<String>, ToolError> {
    match std::env::var("ANYAGENTS_BROWSER_PROXY") {
        Ok(v) => Ok(Some(v)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(_) => Err(ToolError::invalid(
            "ANYAGENTS_BROWSER_PROXY must be valid Unicode",
        )),
    }
}

fn env_list(name: &str) -> Option<Vec<String>> {
    std::env::var(name)
        .ok()
        .map(|v| v.split_whitespace().map(String::from).collect())
}

/// A declared tool's path, or a typed refusal naming it.
pub(crate) fn tool(name: &str, needed_by: &str) -> Result<PathBuf, ToolError> {
    anyagents_toolchain::tool(name, needed_by)
        .map(|t| t.path)
        .map_err(|e| ToolError {
            code: "TOOL_UNAVAILABLE",
            message: e.to_string(),
            hint: "Declare the tool in the toolchain manifest (toolchain.toml) or install it.",
            retryable: false,
        })
}

const FILE_ACCESS_FLAGS: [&str; 4] = [
    "--allow-file-access-from-files",
    "--allow-file-access",
    "--disable-web-security",
    "--enable-local-file-accesses",
];

fn refuse_file_access_flags(args: &[String]) -> Result<(), ToolError> {
    for arg in args {
        let flag = arg.split('=').next().unwrap_or(arg);
        if FILE_ACCESS_FLAGS.contains(&flag) {
            return Err(ToolError::invalid(format!(
                "refusing to start Chromium with `{flag}`"
            )));
        }
    }
    Ok(())
}

/// Chromium never inherits the server's credentials or configuration.
fn chromium_environment(home: &Path, tmp: &Path) -> Result<Vec<(String, String)>, ToolError> {
    let text = |p: PathBuf| {
        p.into_os_string()
            .into_string()
            .map_err(|_| ToolError::invalid("browser environment paths must be valid Unicode"))
    };
    let mut env = vec![
        ("HOME".into(), text(home.to_path_buf())?),
        ("XDG_CONFIG_HOME".into(), text(home.join(".config"))?),
        ("XDG_CACHE_HOME".into(), text(home.join(".cache"))?),
        ("XDG_DATA_HOME".into(), text(home.join(".local/share"))?),
        ("TMPDIR".into(), text(tmp.to_path_buf())?),
        ("PATH".into(), "/usr/local/bin:/usr/bin:/bin".into()),
    ];
    for key in [
        "LANG",
        "LANGUAGE",
        "LC_ALL",
        "LC_CTYPE",
        "FONTCONFIG_FILE",
        "FONTCONFIG_PATH",
    ] {
        if let Some(value) = std::env::var_os(key) {
            env.push((
                key.into(),
                value
                    .into_string()
                    .map_err(|_| ToolError::invalid(format!("{key} must be valid Unicode")))?,
            ));
        }
    }
    Ok(env)
}

fn session_config(
    profile: &str,
    dir: &Path,
    runtime_dir: &Path,
    proxy: Option<Proxy>,
    proxy_argument: Option<String>,
) -> Result<SessionConfig, ToolError> {
    let mut browser = BrowserConfig::new(tool("chromium", "browser")?);
    browser.seed_flag = std::env::var("ANYAGENTS_BROWSER_SEED_FLAG")
        .ok()
        .map(|f| f.trim().to_string())
        .filter(|f| !f.is_empty());
    browser.extra_args = env_list("ANYAGENTS_BROWSER_ARGS")
        .unwrap_or_else(|| DEFAULT_BROWSER_ARGS.iter().map(|a| a.to_string()).collect());
    if let Some(argument) = proxy_argument {
        browser.extra_args.push(argument);
    }
    refuse_file_access_flags(&browser.extra_args)?;
    if !browser
        .extra_args
        .iter()
        .any(|a| a == "--disable-dev-shm-usage")
    {
        browser.extra_args.push("--disable-dev-shm-usage".into());
    }
    super::private_dir(runtime_dir).map_err(|e| ToolError::invalid(e.to_string()))?;
    let tmp = runtime_dir.join("chromium-tmp");
    super::private_dir(&tmp).map_err(|e| ToolError::invalid(e.to_string()))?;
    let home = dir.join("home");
    super::private_dir(&home).map_err(|e| ToolError::invalid(e.to_string()))?;
    let writable = [
        std::path::absolute(dir),
        std::path::absolute(&tmp),
        std::path::absolute(runtime_dir),
    ]
    .into_iter()
    .collect::<std::io::Result<Vec<_>>>()
    .map_err(|e| ToolError::invalid(e.to_string()))?;
    let read = std::env::var_os("ANYAGENTS_BROWSER_JAIL_READ")
        .map(|v| std::env::split_paths(&v).collect::<Vec<_>>())
        .unwrap_or_default();
    let (helper, mut args) = super::jail::wrap(
        &writable.iter().map(PathBuf::as_path).collect::<Vec<_>>(),
        &read,
        &[],
        crate::sandbox::seats_configured(),
        &browser.binary,
    )
    .map_err(|e| ToolError::invalid(e.to_string()))?;
    browser.binary = args
        .pop()
        .map(PathBuf::from)
        .ok_or_else(|| ToolError::invalid("browser jail executable is missing"))?;
    browser.wrapper = std::iter::once(helper.into_os_string())
        .chain(args)
        .map(|v| {
            v.into_string()
                .map_err(|_| ToolError::invalid("browser jail paths must be valid Unicode"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    browser.clear_env = true;
    browser.extra_env = chromium_environment(&home, &tmp)?;
    let mut cfg = SessionConfig::new(Profile::new(profile, dir), browser, runtime_dir);
    if let Some(languages) = std::env::var("ANYAGENTS_BROWSER_LANGUAGES")
        .ok()
        .map(|v| {
            v.split(',')
                .map(|l| l.trim().to_string())
                .filter(|l| !l.is_empty())
                .collect::<Vec<_>>()
        })
        .filter(|l| !l.is_empty())
    {
        cfg.fingerprint_defaults.languages = languages;
    }
    if let Ok(tz) = std::env::var("ANYAGENTS_BROWSER_TIMEZONE")
        && !tz.trim().is_empty()
    {
        cfg.fingerprint_defaults.timezone = tz.trim().to_string();
    }
    cfg.compositor = tool("sway", "browser")?;
    cfg.vnc = tool("wayvnc", "browser.takeover")?;
    cfg.proxy = proxy;
    cfg.takeover = Some(TakeoverConfig::default());
    Ok(cfg)
}

fn new_runtime_dir() -> std::io::Result<PathBuf> {
    use std::os::unix::ffi::OsStringExt as _;
    // Keep Unix-domain sockets well below sockaddr_un's pathname limit.
    let mut template = b"/tmp/anyagents-desktop-XXXXXX\0".to_vec();
    // SAFETY: mutable, NUL-terminated template with six trailing X bytes.
    if unsafe { libc::mkdtemp(template.as_mut_ptr().cast()) }.is_null() {
        return Err(std::io::Error::last_os_error());
    }
    template.pop();
    Ok(PathBuf::from(std::ffi::OsString::from_vec(template)))
}

async fn start(
    data: &Path,
    dir: &Path,
    profile: &str,
    principal: &str,
    proxy: Option<String>,
) -> Result<Live, ToolError> {
    let (parsed, proxy_argument) = super::proxy::configure(proxy.as_deref())?;
    super::legacy::retire(dir)
        .await
        .map_err(|e| ToolError::invalid(format!("cannot retire legacy browser: {e}")))?;
    let runtime_dir = new_runtime_dir().map_err(|e| ToolError::invalid(e.to_string()))?;
    let cfg = match session_config(profile, dir, &runtime_dir, parsed, proxy_argument) {
        Ok(cfg) => cfg,
        Err(e) => {
            let _ = std::fs::remove_dir_all(&runtime_dir);
            return Err(e);
        }
    };
    let session = match DesktopSession::start(cfg).await {
        Ok(s) => Arc::new(s),
        Err(e) => {
            let _ = std::fs::remove_dir_all(&runtime_dir);
            return Err(e.into());
        }
    };
    // A fresh session: the lease cannot be held by anyone else yet.
    session.lease(principal).map_err(|e| in_use(profile, e))?;
    let mount = takeover::mount(&session);
    let (watcher, hand_backs) = takeover::watch_hand_backs(data, profile, session.clone());
    let now = now_ms();
    Ok(Live {
        session,
        profile: profile.to_string(),
        proxy,
        last_used_ms: AtomicI64::new(now),
        started_at_ms: now,
        mount,
        tunnel: tokio::sync::Mutex::new(None),
        watcher,
        hand_backs,
        awaited: AtomicU64::new(0),
        runtime_dir,
    })
}

/// Stop `profile`'s session; `true` when one was running. The profile
/// directory (logins included) stays on disk.
pub async fn stop(data: &Path, profile: &str) -> bool {
    let Ok(Some(key)) = server_profile_dir(data, profile, false) else {
        return false;
    };
    let slot = SESSIONS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&key)
        .cloned();
    let Some(slot) = slot else {
        return false;
    };
    // Detach, do not cancel, on timeout: close may be flushing the profile.
    // Retain the registry slot until it finishes, excluding every new writer.
    let closing = tokio::spawn(async move { close_slot(slot.lock_owned().await).await });
    match wait_for_shutdown(closing, SHUTDOWN_WAIT).await {
        Ok(was_running) => was_running,
        _ => true, // shutdown requested; completion can outlive the caller budget
    }
}

// Dropping a JoinHandle detaches the cleanup; it never releases the writer
// lock while the original session is still closing.
async fn wait_for_shutdown<T>(
    task: tokio::task::JoinHandle<T>,
    budget: std::time::Duration,
) -> Result<T, ToolError> {
    tokio::time::timeout(budget, task)
        .await
        .map_err(|_| session_busy())?
        .map_err(|_| session_busy())
}

async fn close_slot(mut guard: tokio::sync::OwnedMutexGuard<Option<Arc<Live>>>) -> bool {
    let Some(live) = guard.as_ref().cloned() else {
        return false;
    };
    shutdown(live).await;
    *guard = None;
    true
}

/// Close a session gracefully: calls in flight finish, the browser flushes
/// its profile, and nothing keeps it reachable afterwards.
async fn shutdown(live: Arc<Live>) {
    live.watcher.abort();
    if let Some(key) = &live.mount {
        takeover::unmount(key);
    }
    live.session.close().await;
    drop(live.tunnel.lock().await.take());
    let runtime_dir = live.runtime_dir.clone();
    let _ = tokio::task::spawn_blocking(move || std::fs::remove_dir_all(runtime_dir)).await;
}

/// Stop every session `agent` holds, and the one of its own profile, from
/// synchronous code (agent deletion, archive reaping).
pub fn stop_agent_in_background(data: &Path, agent: &str) {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        let (data, agent) = (data.to_path_buf(), agent.to_string());
        handle.spawn(async move {
            stop_agent(&data, &agent).await;
        });
    }
}

/// Stop every session `agent` holds, and the one of its own profile, and
/// wait for them, from a blocking worker thread (never from async code).
pub fn stop_agent_blocking(data: &Path, agent: &str) {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.block_on(stop_agent(data, agent));
    }
}

async fn stop_agent(data: &Path, agent: &str) {
    let mut profiles: Vec<String> = all_running()
        .into_iter()
        .filter(|l| l.leaseholder().as_deref() == Some(agent))
        .map(|l| l.profile.clone())
        .collect();
    profiles.push(agent.to_string());
    profiles.sort();
    profiles.dedup();
    futures::future::join_all(profiles.iter().map(|profile| stop(data, profile))).await;
}

/// Stop every session (server shutdown), concurrently.
pub async fn stop_all() {
    let slots: Vec<Slot> = SESSIONS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .values()
        .cloned()
        .collect();
    let closing = tokio::spawn(async move {
        futures::future::join_all(slots.into_iter().map(|slot| async move {
            close_slot(slot.lock_owned().await).await;
        }))
        .await;
    });
    // A single total deadline covers all slots, startup contention, web calls,
    // tunnel publication and filesystem cleanup, not a per-session timeout.
    let _ = wait_for_shutdown(closing, SHUTDOWN_WAIT).await;
}

/// Drop every session without a graceful close; their processes are killed
/// as the last handles go. For paths that cannot await (a panicking test).
#[cfg(test)]
pub fn abandon_all() {
    let slots: Vec<Slot> = SESSIONS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .drain()
        .map(|(_, s)| s)
        .collect();
    for slot in slots {
        if let Ok(mut g) = slot.try_lock()
            && let Some(live) = g.take()
        {
            live.watcher.abort();
            if let Some(key) = &live.mount {
                takeover::unmount(key);
            }
        }
    }
}

/// Every running session.
pub fn all_running() -> Vec<Arc<Live>> {
    let slots: Vec<Slot> = SESSIONS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .values()
        .cloned()
        .collect();
    slots
        .iter()
        .filter_map(|s| s.try_lock().ok().and_then(|g| g.clone()))
        .collect()
}

/// Stop sessions idle past `ANYAGENTS_BROWSER_IDLE_SECS`; a session a person
/// controls is never reaped.
pub fn reap_idle(data: &Path) {
    let idle_ms = std::env::var("ANYAGENTS_BROWSER_IDLE_SECS")
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(DEFAULT_IDLE_SECS)
        .saturating_mul(1000);
    let now = now_ms();
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    for live in all_running() {
        if live.holder() == "agent" && now.saturating_sub(live.last_used_ms()) > idle_ms {
            let (data, profile) = (data.to_path_buf(), live.profile.clone());
            handle.spawn(async move {
                stop(&data, &profile).await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_access_disables_system_proxies() {
        let dir = tempfile::tempdir().unwrap();
        // Unavailable tools are a typed refusal, never a guessed path.
        let cfg = match session_config(
            "p",
            dir.path(),
            &dir.path().join("rt"),
            None,
            Some("--no-proxy-server".into()),
        ) {
            Ok(cfg) => cfg,
            Err(e) => {
                assert_eq!(e.code, "TOOL_UNAVAILABLE");
                return;
            }
        };
        assert!(
            cfg.browser
                .extra_args
                .iter()
                .any(|a| a == "--no-proxy-server")
        );
        if std::env::var_os("ANYAGENTS_BROWSER_ARGS").is_none() {
            for arg in DEFAULT_BROWSER_ARGS {
                assert!(cfg.browser.extra_args.iter().any(|a| a == arg), "{arg}");
            }
        }
        assert_eq!(cfg.profile.user_data_dir(), dir.path().join("user-data"));
    }

    #[test]
    fn a_second_agent_gets_a_clear_refusal() {
        let err = in_use(
            "shopping",
            agent_workstation::Error::Leased {
                principal: "alice".into(),
                since: chrono::Utc::now(),
            },
        );
        assert_eq!(err.code, "DESKTOP_IN_USE");
        assert!(err.message.contains("`shopping`") && err.message.contains("`alice`"));
        assert!(!err.retryable);
    }
    #[tokio::test]
    async fn transport_checks_detect_helper_loss_without_cdp() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("user-data")).unwrap();
        let sway = tokio::net::UnixListener::bind(dir.path().join("sway.sock")).unwrap();
        let browser = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let endpoint = dir.path().join("user-data/DevToolsActivePort");
        std::fs::write(
            &endpoint,
            format!(
                "{}\n/devtools/browser/test\n",
                browser.local_addr().unwrap().port()
            ),
        )
        .unwrap();
        assert!(transport_healthy(dir.path(), dir.path()).await.unwrap());
        let valid_endpoint = std::fs::read_to_string(&endpoint).unwrap();
        std::fs::remove_file(&endpoint).unwrap();
        assert!(!transport_healthy(dir.path(), dir.path()).await.unwrap());
        std::fs::write(&endpoint, valid_endpoint).unwrap();
        drop(browser);
        assert!(!transport_healthy(dir.path(), dir.path()).await.unwrap());
        std::fs::write(&endpoint, "0\n/devtools/browser/test\n").unwrap();
        assert!(transport_healthy(dir.path(), dir.path()).await.is_err());
        drop(sway);
        assert!(!transport_healthy(dir.path(), dir.path()).await.unwrap());
    }

    #[tokio::test]
    async fn shutdown_deadline_detaches_but_preserves_writer_lock() {
        let slot: Slot = Arc::new(tokio::sync::Mutex::new(None));
        let guard = slot.clone().lock_owned().await;
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        let (finished, done) = tokio::sync::oneshot::channel::<()>();
        let cleanup = tokio::spawn(async move {
            let _guard = guard;
            let _ = wait.await;
            drop(_guard);
            let _ = finished.send(());
        });
        assert!(
            wait_for_shutdown(cleanup, std::time::Duration::from_millis(20))
                .await
                .is_err()
        );
        assert!(
            slot.try_lock().is_err(),
            "cleanup must retain the profile writer lock"
        );
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), done)
            .await
            .unwrap()
            .unwrap();
        assert!(slot.try_lock().is_ok());
        assert!(SHUTDOWN_WAIT <= std::time::Duration::from_secs(10));
    }
}
