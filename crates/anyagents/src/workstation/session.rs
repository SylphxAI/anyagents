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

use std::collections::{HashMap, HashSet};
use std::os::fd::AsRawFd as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Once};

use agent_workstation::{
    BrowserConfig, DesktopSession, Holder, Profile, Proxy, SessionConfig, TakeoverConfig,
};
use anyagents_core::now_ms;

use super::takeover::{self, Tunnel};
use super::{ToolError, server_profile_dir};

const DEFAULT_IDLE_SECS: i64 = 15 * 60;
// One total caller budget, including a slot held by startup or an in-flight call.
const SHUTDOWN_WAIT: std::time::Duration = std::time::Duration::from_secs(10);
const HEALTH_WAIT: std::time::Duration = std::time::Duration::from_secs(2);
const ORPHAN_RUNTIME_AGE: std::time::Duration = std::time::Duration::from_secs(300);
const RUNTIME_LOCK_FILE: &str = ".desktop.lock";
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
    /// Caller work (including clipboard I/O) must finish before teardown.
    admission: tokio::sync::RwLock<()>,
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
    hand_back_pending: std::sync::Mutex<Option<u64>>,
    /// Whether the latest takeover started on a challenged page requiring solve proof.
    watch_challenge: Arc<AtomicBool>,
    runtime_dir: PathBuf,
    runtime_guard: Option<RuntimeDirGuard>,
}

#[derive(Debug)]
struct RuntimeDirGuard {
    dir: PathBuf,
    lock: std::sync::Mutex<Option<std::fs::File>>,
}

impl RuntimeDirGuard {
    fn new(dir: PathBuf, lock: Option<std::fs::File>) -> Self {
        register_starting_runtime_dir(&dir);
        Self {
            dir,
            lock: std::sync::Mutex::new(lock),
        }
    }

    fn release_lock(&self) {
        if let Ok(mut lock) = self.lock.lock() {
            drop(lock.take());
        }
        unregister_starting_runtime_dir(&self.dir);
    }
}

impl Drop for RuntimeDirGuard {
    fn drop(&mut self) {
        self.release_lock();
        if self.dir.exists() {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

impl Drop for Live {
    fn drop(&mut self) {
        self.watcher.abort();
        if let Some(key) = &self.mount {
            takeover::unmount(key);
        }
        drop(self.runtime_guard.take());
    }
}

/// Touch on every completion, error and cancellation, before releasing admission.
pub struct Operation<'a> {
    live: &'a Live,
    _guard: tokio::sync::RwLockReadGuard<'a, ()>,
}

impl Drop for Operation<'_> {
    fn drop(&mut self) {
        self.live.touch();
    }
}

impl Live {
    /// Admit caller work or refuse an Arc retired while the caller was waiting.
    pub async fn operation(&self) -> Result<Operation<'_>, ToolError> {
        let guard = self.admission.read().await;
        if self.session.is_closed() {
            return Err(agent_workstation::Error::Closed.into());
        }
        Ok(Operation {
            live: self,
            _guard: guard,
        })
    }

    fn pending_hand_back(&self) -> bool {
        let mut pending = self
            .hand_back_pending
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let Some(baseline) = *pending else {
            return false;
        };
        if *self.hand_backs.borrow() <= baseline {
            return true;
        }
        // Hand-back is activity too. Give the agent its normal idle window
        // to resume; don't reap immediately after a long human interaction.
        *pending = None;
        self.touch();
        false
    }

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
    pub fn expect_hand_back(&self, challenge_detected: bool) {
        let mut pending = self
            .hand_back_pending
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        self.watch_challenge
            .store(challenge_detected, Ordering::Release);
        // The watcher may lag a previous release; never let that old
        // completion satisfy a newly requested takeover.
        let baseline = self.session.gate().hand_backs();
        self.awaited.store(baseline, Ordering::Release);
        *pending = Some(baseline);
    }

    /// Release human custody or cancel an invitation that was never claimed.
    pub fn release_takeover(&self, note: String) -> bool {
        let mut pending = self
            .hand_back_pending
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let gate = self.session.gate();
        let released = gate.release(Some(note));
        // The watcher may not have observed a real hand-back yet. Don't
        // report a second release or manufacture a hand-back notification.
        let cancelled = pending.take().is_some_and(|n| gate.hand_backs() == n);
        if cancelled {
            self.watch_challenge.store(false, Ordering::Release);
        }
        self.touch();
        released || cancelled
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
static STARTING_RUNTIME_DIRS: LazyLock<std::sync::Mutex<HashSet<PathBuf>>> =
    LazyLock::new(|| std::sync::Mutex::new(HashSet::new()));

fn register_starting_runtime_dir(dir: &Path) {
    STARTING_RUNTIME_DIRS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(dir.to_path_buf());
}

fn unregister_starting_runtime_dir(dir: &Path) {
    STARTING_RUNTIME_DIRS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(dir);
}

fn active_runtime_dirs() -> HashSet<PathBuf> {
    let mut dirs = STARTING_RUNTIME_DIRS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone();
    for live in all_running() {
        dirs.insert(live.runtime_dir.clone());
    }
    dirs
}

fn lock_runtime_dir(dir: &Path) -> std::io::Result<std::fs::File> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(dir.join(RUNTIME_LOCK_FILE))?;
    // SAFETY: valid open file descriptor; LOCK_EX | LOCK_NB never blocks.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(file)
}

fn current_uid() -> u32 {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() }
}

/// Remove orphaned `/tmp/anyagents-desktop-*` runtime directories left behind by
/// crashed processes once their kernel `flock` is released (or stale legacy dirs).
pub fn reap_stale_runtime_dirs() {
    reap_stale_runtime_dirs_in(Path::new("/tmp"), ORPHAN_RUNTIME_AGE);
}

fn reap_stale_runtime_dirs_once() {
    static INIT_REAP: Once = Once::new();
    INIT_REAP.call_once(reap_stale_runtime_dirs);
}

fn reap_stale_runtime_dirs_in(base: &Path, legacy_max_age: std::time::Duration) {
    use std::os::unix::fs::MetadataExt as _;
    let Ok(entries) = std::fs::read_dir(base) else {
        return;
    };
    let me = current_uid();
    let active = active_runtime_dirs();
    let now = std::time::SystemTime::now();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else {
            continue;
        };
        if !name_str.starts_with("anyagents-desktop-") {
            continue;
        }
        let path = entry.path();
        if active.contains(&path) {
            continue;
        }
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if !meta.is_dir() || meta.file_type().is_symlink() || meta.uid() != me {
            continue;
        }
        let lock_path = path.join(RUNTIME_LOCK_FILE);
        if lock_path.exists() {
            match lock_runtime_dir(&path) {
                Ok(lock_file) => {
                    drop(lock_file);
                    let _ = std::fs::remove_dir_all(&path);
                }
                Err(_) => continue,
            }
        } else if let Ok(modified) = meta.modified()
            && now
                .duration_since(modified)
                .is_ok_and(|age| age >= legacy_max_age)
        {
            let _ = std::fs::remove_dir_all(&path);
        }
    }
}

pub async fn acquire(
    data: &Path,
    profile: &str,
    principal: &str,
    proxy: Option<&str>,
) -> Result<Arc<Live>, ToolError> {
    reap_stale_runtime_dirs_once();
    let key = server_profile_dir(data, profile, true)
        .map_err(|e| ToolError::invalid(e.to_string()))?
        .ok_or_else(|| ToolError::invalid("browser profile is missing"))?;
    let slot = slot(&key);
    let mut guard = tokio::time::timeout(SHUTDOWN_WAIT, slot.clone().lock_owned())
        .await
        .map_err(|_| session_busy())?;
    let mut replacement_proxy = None;
    if let Some(live) = guard.as_ref() {
        // Authorize before probing or replacing a running session. Closing
        // ends the engine lease; the profile directory remains protected.
        authorize(live, principal)?;
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
        replacement_proxy = Some(live.proxy.clone());
        // Keep the SAME slot and caller admission locked through revalidation
        // and close. A stale health probe must not replace a recovered session.
        let principal = principal.to_owned();
        let health_key = key.clone();
        let closing = tokio::spawn(async move {
            let live = guard.as_ref().expect("live session").clone();
            let admission = live.admission.write().await;
            authorize(&live, &principal)?;
            if !cached_healthy(&live, &health_key).await? {
                if !live.session.is_closed() {
                    live.session
                        .gate()
                        .check(&principal)
                        .map_err(ToolError::from)?;
                }
                if !shutdown_if_agent(&live).await {
                    return Err(session_busy());
                }
                *guard = None;
            }
            drop(admission);
            Ok::<_, ToolError>(guard)
        });
        guard = wait_for_shutdown(closing, SHUTDOWN_WAIT).await??;
        if let Some(live) = guard.as_ref() {
            live.touch();
            return Ok(live.clone());
        }
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

// Engine close ends its lease. The already-validated profile may be reopened,
// but a running session still requires its exact current principal.
fn authorize(live: &Live, principal: &str) -> Result<(), ToolError> {
    if live.session.is_closed() {
        return Ok(());
    }
    live.session
        .gate()
        .acquire(principal)
        .map(|_| ())
        .map_err(|e| in_use(&live.profile, e))
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
    if live.pending_hand_back() {
        return Ok(true);
    }
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
pub(super) fn in_use(profile: &str, e: agent_workstation::Error) -> ToolError {
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

/// Mesa llvmpipe's rasterizer thread count. On a virtual X display with GLX
/// (Xvfb) Chromium's GPU process renders through llvmpipe, which otherwise
/// starts one thread per core up to 32; a page that animates without end
/// then holds every one of them busy at 60 fps.
const LLVMPIPE_THREADS_ENV: &str = "LP_NUM_THREADS";

/// Default rasterizer thread count: a software-rendered browser costs about
/// two cores at most, while WebGL, screenshots and takeover keep rendering.
/// The server's own `LP_NUM_THREADS` overrides it only when it is a whole
/// number: Mesa reads anything else as unset, one thread per core.
const LLVMPIPE_THREADS: &str = "2";

/// The rasterizer thread count Chromium is given: the server's own when it
/// is a whole number, else [`LLVMPIPE_THREADS`].
fn llvmpipe_threads() -> String {
    std::env::var(LLVMPIPE_THREADS_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .map_or_else(|| LLVMPIPE_THREADS.to_string(), |count| count.to_string())
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
    env.push((LLVMPIPE_THREADS_ENV.into(), llvmpipe_threads()));
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
    super::private_dir(&dir.join("user-data")).map_err(|e| ToolError::invalid(e.to_string()))?;
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
    let extra_exec = std::env::var_os("ANYAGENTS_BROWSER_JAIL_READ")
        .map(|v| std::env::split_paths(&v).collect::<Vec<_>>())
        .unwrap_or_default();
    let (helper, mut args) = super::jail::wrap(
        &writable.iter().map(PathBuf::as_path).collect::<Vec<_>>(),
        &[],
        &extra_exec,
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
    // A takeover is a custody transfer, not an idle lease. Only explicit
    // hand-back may resume the agent, including after a browser failure.
    cfg.human_idle_timeout = None;
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
    reap_stale_runtime_dirs_once();
    let (parsed, proxy_argument) = super::proxy::configure(proxy.as_deref())?;
    super::legacy::retire(dir)
        .await
        .map_err(|e| ToolError::invalid(format!("cannot retire legacy browser: {e}")))?;
    let runtime_dir = new_runtime_dir().map_err(|e| ToolError::invalid(e.to_string()))?;
    let runtime_lock = lock_runtime_dir(&runtime_dir).ok();
    let runtime_guard = RuntimeDirGuard::new(runtime_dir.clone(), runtime_lock);
    let cfg = session_config(profile, dir, &runtime_dir, parsed, proxy_argument)?;
    let session = Arc::new(DesktopSession::start(cfg).await?);
    // A fresh session: the lease cannot be held by anyone else yet.
    session.lease(principal).map_err(|e| in_use(profile, e))?;
    let mount = takeover::mount(data, &session);
    let watch_challenge = Arc::new(AtomicBool::new(false));
    let (watcher, hand_backs) =
        takeover::watch_hand_backs(data, profile, session.clone(), watch_challenge.clone());
    let now = now_ms();
    Ok(Live {
        session,
        profile: profile.to_string(),
        proxy,
        admission: tokio::sync::RwLock::new(()),
        last_used_ms: AtomicI64::new(now),
        started_at_ms: now,
        mount,
        tunnel: tokio::sync::Mutex::new(None),
        watcher,
        hand_backs,
        awaited: AtomicU64::new(0),
        hand_back_pending: std::sync::Mutex::new(None),
        watch_challenge,
        runtime_dir,
        runtime_guard: Some(runtime_guard),
    })
}

/// Check the lease and begin shutdown under one profile-slot lock.
pub async fn stop_authorized(
    data: &Path,
    profile: &str,
    principal: &str,
) -> Result<bool, ToolError> {
    let Some(key) =
        server_profile_dir(data, profile, false).map_err(|e| ToolError::invalid(e.to_string()))?
    else {
        return Ok(false);
    };
    let slot = SESSIONS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&key)
        .cloned();
    let Some(slot) = slot else {
        return Ok(false);
    };
    // Slot contention and graceful close share one caller deadline.
    let deadline = tokio::time::Instant::now() + SHUTDOWN_WAIT;
    let guard = tokio::time::timeout_at(deadline, slot.lock_owned())
        .await
        .map_err(|_| session_busy())?;
    let principal = principal.to_owned();
    let closing = tokio::spawn(async move {
        let mut guard = guard;
        let Some(live) = guard.as_ref().cloned() else {
            return Ok(false);
        };
        let _admission = live.admission.write().await;
        authorize(&live, &principal)?;
        if !live.session.is_closed() {
            live.session
                .gate()
                .check(&principal)
                .map_err(ToolError::from)?;
        }
        if !shutdown_if_agent(&live).await {
            live.session
                .gate()
                .check(&principal)
                .map_err(ToolError::from)?;
            return Err(session_busy());
        }
        *guard = None;
        Ok(true)
    });
    wait_for_shutdown(
        closing,
        deadline.saturating_duration_since(tokio::time::Instant::now()),
    )
    .await?
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
    let _admission = live.admission.write().await;
    live.session.close().await;
    cleanup(&live).await;
    *guard = None;
    true
}

// Caller admission is held exclusively. The engine's conditional close is
// atomic with human takeover; no mounts, watchers or tunnels change on refusal.
async fn shutdown_if_agent(live: &Live) -> bool {
    if live.session.is_closed() {
        live.session.close().await;
    } else if !live.session.close_if_agent().await {
        return false;
    }
    // A published invitation is not custody. Clear it only after the
    // engine wins the close-vs-Take control race.
    *live
        .hand_back_pending
        .lock()
        .unwrap_or_else(|p| p.into_inner()) = None;
    cleanup(live).await;
    true
}

/// Only remove caller resources after engine close has succeeded.
async fn cleanup(live: &Live) {
    live.watcher.abort();
    if let Some(key) = &live.mount {
        takeover::unmount(key);
    }
    drop(live.tunnel.lock().await.take());
    if let Some(guard) = &live.runtime_guard {
        guard.release_lock();
    }
    unregister_starting_runtime_dir(&live.runtime_dir);
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
    // Revalidate the principal under the close lock, not only in the snapshot.
    futures::future::join_all(
        profiles
            .iter()
            .map(|profile| stop_authorized(data, profile, agent)),
    )
    .await;
}

/// Stop every session (server shutdown), concurrently across per-slot tasks.
pub async fn stop_all() {
    let slots: Vec<Slot> = SESSIONS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .values()
        .cloned()
        .collect();
    let closing = tokio::spawn(async move {
        let tasks: Vec<_> = slots
            .into_iter()
            .map(|slot| tokio::spawn(async move { close_slot(slot.lock_owned().await).await }))
            .collect();
        futures::future::join_all(tasks).await;
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
        if live.holder() == "agent"
            && !live.pending_hand_back()
            && now.saturating_sub(live.last_used_ms()) > idle_ms
        {
            let data = data.to_path_buf();
            handle.spawn(async move {
                reap_candidate(&data, live, idle_ms).await;
            });
        }
    }
}

// A snapshot only nominates a candidate. Identity and current activity must
// still match after waiting for both registry and caller work to drain.
async fn reap_candidate(data: &Path, expected: Arc<Live>, idle_ms: i64) -> bool {
    let Ok(Some(key)) = server_profile_dir(data, &expected.profile, false) else {
        return false;
    };
    let slot = slot(&key);
    let mut guard = slot.lock_owned().await;
    let Some(live) = guard.as_ref().cloned() else {
        return false;
    };
    if !Arc::ptr_eq(&live, &expected) {
        return false;
    }
    let _admission = live.admission.write().await;
    if live.pending_hand_back()
        || now_ms().saturating_sub(live.last_used_ms()) <= idle_ms
        || !shutdown_if_agent(&live).await
    {
        return false;
    }
    *guard = None;
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn headful_teardown_revalidates_activity_custody_and_identity() {
        if std::env::var("ANYAGENTS_DESKTOP_HEADFUL").as_deref() != Ok("1") {
            eprintln!("skipped: set ANYAGENTS_DESKTOP_HEADFUL=1");
            return;
        }
        let data = tempfile::tempdir().unwrap();
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(data.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let live = acquire(data.path(), "old-owner", "old-owner", None)
            .await
            .unwrap();
        assert!(
            !live.pending_hand_back(),
            "fresh sessions must still expire"
        );
        assert_eq!(live.session.gate().human_idle_timeout(), None);

        // An old nomination waits for in-flight caller work (clipboard included),
        // then sees the completion timestamp rather than killing the browser.
        let operation = live.operation().await.unwrap();
        live.last_used_ms.store(0, Ordering::Relaxed);
        let reaper = {
            let data = data.path().to_path_buf();
            let live = live.clone();
            tokio::spawn(async move { reap_candidate(&data, live, 60_000).await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        assert!(!reaper.is_finished());
        drop(operation);
        assert!(!reaper.await.unwrap());
        assert!(!live.session.is_closed());

        // Cancellation also records activity before giving up admission.
        let (ready, waiting) = tokio::sync::oneshot::channel();
        let task = {
            let live = live.clone();
            tokio::spawn(async move {
                let _operation = live.operation().await.unwrap();
                live.last_used_ms.store(0, Ordering::Relaxed);
                ready.send(()).unwrap();
                std::future::pending::<()>().await;
            })
        };
        waiting.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(live.last_used_ms() > 0);

        // A takeover can win after nomination. Reaping must leave the watcher,
        // runtime and public page reachable, not just avoid browser shutdown.
        live.last_used_ms.store(0, Ordering::Relaxed);
        let admission = live.admission.write().await;
        let reaper = {
            let data = data.path().to_path_buf();
            let live = live.clone();
            tokio::spawn(async move { reap_candidate(&data, live, 60_000).await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        live.session.gate().take(Some("fixture".into()));
        drop(admission);
        assert!(!reaper.await.unwrap());
        assert_eq!(live.holder(), "human");
        assert!(!live.watcher.is_finished());
        assert!(live.runtime_dir.exists());
        assert_eq!(
            stop_authorized(data.path(), "old-owner", "old-owner")
                .await
                .unwrap_err()
                .code,
            "HUMAN_IN_CONTROL"
        );
        assert!(live.session.gate().release(Some("fixture complete".into())));

        // A requested but unopened takeover is protected, including a timed-out
        // wait and failed transport probe. No implicit replacement/replay.
        live.expect_hand_back(false);
        assert!(
            !live
                .wait_for_hand_back(std::time::Duration::from_millis(40))
                .await
        );
        live.last_used_ms.store(0, Ordering::Relaxed);
        assert!(live.pending_hand_back());
        assert!(!reap_candidate(data.path(), live.clone(), 60_000).await);
        assert!(
            cached_healthy(&live, &data.path().join("absent"))
                .await
                .unwrap()
        );
        assert!(Arc::ptr_eq(
            &acquire(data.path(), "old-owner", "old-owner", None)
                .await
                .unwrap(),
            &live
        ));
        // Only implicit retirement honors a pending invitation; explicit
        // close is covered separately through the browser tool surface.
        live.session.gate().take(Some("fixture".into()));
        assert!(
            live.session
                .gate()
                .release(Some("explicit hand-back".into()))
        );
        assert!(
            live.wait_for_hand_back(std::time::Duration::from_secs(2))
                .await
        );
        live.last_used_ms.store(0, Ordering::Relaxed);
        assert!(!live.pending_hand_back());
        assert!(
            live.last_used_ms() > 0,
            "hand-back starts a fresh idle window"
        );

        // An actual idle, agent-held session still closes. A previously acquired
        // Arc then refuses work, and its stale nomination cannot close a successor.
        live.last_used_ms.store(0, Ordering::Relaxed);
        assert!(reap_candidate(data.path(), live.clone(), 60_000).await);
        assert_eq!(live.operation().await.err().unwrap().code, "CLOSED");
        let successor = acquire(data.path(), "old-owner", "new-owner", None)
            .await
            .unwrap();
        successor.last_used_ms.store(0, Ordering::Relaxed);
        assert!(!reap_candidate(data.path(), live, 60_000).await);
        stop_agent(data.path(), "old-owner").await;
        assert!(
            !successor.session.is_closed(),
            "archive must revalidate the current lease"
        );
        assert_eq!(successor.leaseholder().as_deref(), Some("new-owner"));
        // Engine close ends the lease, so this protected profile can reopen.
        successor.session.close().await;
        let recovered = acquire(data.path(), "old-owner", "recovered-owner", None)
            .await
            .unwrap();
        assert!(!Arc::ptr_eq(&successor, &recovered));
        assert!(!recovered.session.is_closed());
        assert!(
            stop_authorized(data.path(), "old-owner", "recovered-owner")
                .await
                .unwrap()
        );
    }

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
        assert_eq!(
            cfg.human_idle_timeout, None,
            "takeover requires explicit hand-back"
        );
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

    #[test]
    fn chromium_software_rendering_is_bounded_to_a_few_threads() {
        let _env = crate::server::tests::env_lock();
        struct Reset(Option<std::ffi::OsString>);
        impl Drop for Reset {
            fn drop(&mut self) {
                match &self.0 {
                    Some(value) => unsafe { std::env::set_var(LLVMPIPE_THREADS_ENV, value) },
                    None => unsafe { std::env::remove_var(LLVMPIPE_THREADS_ENV) },
                }
            }
        }
        let _reset = Reset(std::env::var_os(LLVMPIPE_THREADS_ENV));
        let counts = |server: Option<&str>| {
            match server {
                Some(value) => unsafe { std::env::set_var(LLVMPIPE_THREADS_ENV, value) },
                None => unsafe { std::env::remove_var(LLVMPIPE_THREADS_ENV) },
            }
            chromium_environment(Path::new("/profile/home"), Path::new("/profile/tmp"))
                .unwrap()
                .into_iter()
                .filter(|(name, _)| name == LLVMPIPE_THREADS_ENV)
                .map(|(_, value)| value)
                .collect::<Vec<_>>()
        };
        assert_eq!(counts(None), vec!["2"]);
        assert_eq!(counts(Some("4")), vec!["4"]);
        assert_eq!(counts(Some(" 0 ")), vec!["0"]);
        for unreadable in ["", "-1", "abc", "2.5"] {
            assert_eq!(counts(Some(unreadable)), vec!["2"], "{unreadable:?}");
        }
    }

    #[test]
    fn orphan_runtime_dir_reaper_removes_unlocked_and_stale_dirs_but_preserves_locked_or_fresh() {
        let base = tempfile::tempdir().unwrap();
        let unlocked_dead = base.path().join("anyagents-desktop-dead01");
        std::fs::create_dir(&unlocked_dead).unwrap();
        std::fs::write(unlocked_dead.join(RUNTIME_LOCK_FILE), b"").unwrap();
        std::fs::write(unlocked_dead.join("stale.sock"), b"junk").unwrap();

        let locked_live = base.path().join("anyagents-desktop-live02");
        std::fs::create_dir(&locked_live).unwrap();
        let held_lock = lock_runtime_dir(&locked_live).unwrap();

        let fresh_legacy = base.path().join("anyagents-desktop-fresh3");
        std::fs::create_dir(&fresh_legacy).unwrap();

        let unrelated = base.path().join("unrelated-dir");
        std::fs::create_dir(&unrelated).unwrap();

        // With 5m threshold: unlocked_dead (unlocked .desktop.lock) is reaped immediately;
        // locked_live (flock held), fresh_legacy (younger than 5m), and unrelated survive.
        reap_stale_runtime_dirs_in(base.path(), ORPHAN_RUNTIME_AGE);
        assert!(!unlocked_dead.exists());
        assert!(locked_live.exists());
        assert!(fresh_legacy.exists());
        assert!(unrelated.exists());

        // Dropping held lock + zero legacy threshold reaps both unlocked dirs, leaving unrelated.
        drop(held_lock);
        reap_stale_runtime_dirs_in(base.path(), std::time::Duration::ZERO);
        assert!(!locked_live.exists());
        assert!(!fresh_legacy.exists());
        assert!(unrelated.exists());
    }

    #[test]
    fn runtime_dir_guard_drop_removes_runtime_dir_and_unregisters_starting_set() {
        let base = tempfile::tempdir().unwrap();
        let runtime_dir = base.path().join("anyagents-desktop-drop01");
        std::fs::create_dir(&runtime_dir).unwrap();
        std::fs::write(runtime_dir.join("sway.sock"), b"sock").unwrap();
        let lock = lock_runtime_dir(&runtime_dir).unwrap();
        let guard = RuntimeDirGuard::new(runtime_dir.clone(), Some(lock));
        assert!(active_runtime_dirs().contains(&runtime_dir));
        assert!(runtime_dir.exists());

        // While guard is alive, orphan reaper must preserve the locked directory.
        reap_stale_runtime_dirs_in(base.path(), std::time::Duration::ZERO);
        assert!(runtime_dir.exists());

        drop(guard);
        assert!(!runtime_dir.exists());
        assert!(!active_runtime_dirs().contains(&runtime_dir));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stop_all_runs_concurrent_slot_teardowns_within_budget() {
        assert!(SHUTDOWN_WAIT <= std::time::Duration::from_secs(10));
        // Verify two locked profile slots run their close futures concurrently under stop_all.
        let s1: Slot = Arc::new(tokio::sync::Mutex::new(None));
        let s2: Slot = Arc::new(tokio::sync::Mutex::new(None));
        let g1 = s1.clone().lock_owned().await;
        let g2 = s2.clone().lock_owned().await;
        let t1 = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(60)).await;
            close_slot(g1).await
        });
        let t2 = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(60)).await;
            close_slot(g2).await
        });
        let started = std::time::Instant::now();
        let (r1, r2) = tokio::join!(t1, t2);
        assert!(!r1.unwrap() && !r2.unwrap());
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    }
}
