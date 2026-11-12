//! The server's desktop sessions, one per profile directory.
//!
//! A session starts on the first browser or session-computer call for a
//! profile and stops on `close`, after `ANYAGENTS_BROWSER_IDLE_SECS` without
//! calls (unless a person holds control), or when its agent is deleted. The
//! profile directory is the key: the browser refuses to share one profile,
//! so neither does this registry.
//!
//! Environment (all optional):
//!
//! | Variable | Meaning | Default |
//! | --- | --- | --- |
//! | `ANYAGENTS_CHROMIUM_BIN` | Chromium-family browser binary | first installed Chrome/Chromium |
//! | `ANYAGENTS_BROWSER_SEED_FLAG` | flag that takes the profile's fingerprint seed, e.g. `--fingerprint` | none |
//! | `ANYAGENTS_BROWSER_ARGS` | extra browser arguments, whitespace separated | `--no-sandbox` |
//! | `ANYAGENTS_BROWSER_PROXY` | proxy for sessions started without `proxy` | none |
//! | `ANYAGENTS_BROWSER_LANGUAGES` / `ANYAGENTS_BROWSER_TIMEZONE` | identity of newly created profiles | `en-US,en` / `UTC` |
//! | `ANYAGENTS_BROWSER_IDLE_SECS` | idle seconds before a session stops | 900 |
//! | `ANYAGENTS_DESKTOP_COMPOSITOR` / `ANYAGENTS_DESKTOP_VNC` | compositor / VNC server binaries | `sway` / `wayvnc` |
//! | `ANYAGENTS_TAKEOVER_TUNNEL` | `0` disables the public takeover tunnel | on when `cloudflared` is installed |

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use agent_workstation::{
    BrowserConfig, DesktopSession, Holder, Profile, Proxy, SessionConfig, TakeoverConfig,
};
use anyagents_core::now_ms;

use super::takeover::{self, Tunnel};
use super::{ToolError, profile_dir};

const DEFAULT_IDLE_SECS: i64 = 15 * 60;
/// How long `stop` waits for in-flight calls before dropping the session.
const STOP_DRAIN: Duration = Duration::from_secs(30);

/// The latest snapshot: its id and the page URL it was taken on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotMark {
    /// Snapshot id issued to the agent.
    pub id: String,
    /// URL of the page the refs point into.
    pub url: String,
}

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
    /// Takeover page as people reach it (the tunnel URL when one runs).
    pub takeover_url: Option<String>,
    snapshot: std::sync::Mutex<Option<SnapshotMark>>,
    last_used_ms: AtomicI64,
    started_at_ms: i64,
    tunnel: Option<Tunnel>,
    watcher: tokio::task::JoinHandle<()>,
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

    /// The latest snapshot, if its refs have not been invalidated.
    pub fn snapshot(&self) -> Option<SnapshotMark> {
        self.snapshot.lock().ok().and_then(|s| s.clone())
    }

    /// A new snapshot replaced every ref (`None`: refs are gone).
    pub fn set_snapshot(&self, mark: Option<SnapshotMark>) {
        if let Ok(mut s) = self.snapshot.lock() {
            *s = mark;
        }
    }

    /// `"agent"` or `"human"`.
    pub fn holder(&self) -> &'static str {
        match self.session.gate().holder() {
            Holder::Agent => "agent",
            Holder::Human { .. } => "human",
        }
    }
}

type Slot = Arc<tokio::sync::Mutex<Option<Arc<Live>>>>;

static SESSIONS: LazyLock<std::sync::Mutex<HashMap<PathBuf, Slot>>> =
    LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));
static RUNTIME_SEQ: AtomicU64 = AtomicU64::new(0);

fn slot(key: &Path) -> Slot {
    let mut map = SESSIONS.lock().unwrap_or_else(|p| p.into_inner());
    map.entry(key.to_path_buf()).or_default().clone()
}

/// The running session for `profile`, without starting one. `None` also while
/// another call is starting it.
pub fn running(data: &Path, profile: &str) -> Option<Arc<Live>> {
    let key = profile_dir(data, profile);
    let slot = SESSIONS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&key)
        .cloned()?;
    slot.try_lock().ok().and_then(|g| g.clone())
}

/// The session for `profile`, started on first use.
///
/// `proxy` is the request's proxy (`None` keeps a running session's, or uses
/// `ANYAGENTS_BROWSER_PROXY` for a new one). A different proxy than the
/// running session's is refused: changing routing would discard open tabs or
/// interrupt a person.
pub async fn acquire(
    data: &Path,
    profile: &str,
    owner: &str,
    proxy: Option<&str>,
) -> Result<Arc<Live>, ToolError> {
    let key = profile_dir(data, profile);
    let slot = slot(&key);
    let mut guard = slot.lock().await;
    if let Some(live) = guard.as_ref() {
        if let Some(requested) = proxy
            && live.proxy.as_deref().unwrap_or("") != requested
        {
            return Err(ToolError::invalid(
                "browser proxy change requires closing the profile first; call close, then reopen \
                 with the desired proxy (or an empty string for direct access)",
            ));
        }
        live.touch();
        return Ok(live.clone());
    }
    let proxy = match proxy {
        Some(p) => Some(p.to_string()),
        None => environment_proxy()?,
    };
    let live = Arc::new(start(data, &key, profile, owner, proxy).await?);
    *guard = Some(live.clone());
    Ok(live)
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

/// Parse a request proxy: empty is a direct connection, a bare `host:port`
/// is HTTP. The input is never echoed (it may carry credentials).
fn parse_proxy(value: &str) -> Result<Option<Proxy>, ToolError> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    let url = if value.contains("://") {
        value.to_string()
    } else {
        format!("http://{value}")
    };
    Proxy::parse(&url)
        .map(Some)
        .map_err(|e| ToolError::invalid(format!("invalid browser proxy: {}", e.code())))
}

fn env_list(name: &str) -> Option<Vec<String>> {
    std::env::var(name)
        .ok()
        .map(|v| v.split_whitespace().map(String::from).collect())
}

/// The browser binary: `ANYAGENTS_CHROMIUM_BIN`, else the first installed
/// Chrome/Chromium, else `chromium` on `PATH`.
pub fn browser_binary() -> PathBuf {
    if let Ok(custom) = std::env::var("ANYAGENTS_CHROMIUM_BIN")
        && !custom.trim().is_empty()
    {
        return PathBuf::from(custom.trim());
    }
    [
        "/usr/bin/google-chrome-stable",
        "/usr/bin/google-chrome",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
    ]
    .into_iter()
    .map(PathBuf::from)
    .find(|p| p.is_file())
    .unwrap_or_else(|| PathBuf::from("chromium"))
}

fn session_config(
    profile: &str,
    dir: &Path,
    runtime_dir: &Path,
    proxy: Option<Proxy>,
    direct: bool,
    takeover: TakeoverConfig,
) -> SessionConfig {
    let mut browser = BrowserConfig::new(browser_binary());
    browser.seed_flag = std::env::var("ANYAGENTS_BROWSER_SEED_FLAG")
        .ok()
        .map(|f| f.trim().to_string())
        .filter(|f| !f.is_empty());
    browser.extra_args =
        env_list("ANYAGENTS_BROWSER_ARGS").unwrap_or_else(|| vec!["--no-sandbox".into()]);
    if direct {
        // An explicit empty proxy overrides environment and system proxies.
        browser.extra_args.push("--no-proxy-server".into());
    }
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
    if let Some(bin) = std::env::var_os("ANYAGENTS_DESKTOP_COMPOSITOR") {
        cfg.compositor = bin.into();
    }
    if let Some(bin) = std::env::var_os("ANYAGENTS_DESKTOP_VNC") {
        cfg.vnc = bin.into();
    }
    cfg.proxy = proxy;
    cfg.takeover = Some(takeover);
    cfg
}

async fn start(
    data: &Path,
    dir: &Path,
    profile: &str,
    owner: &str,
    proxy: Option<String>,
) -> Result<Live, ToolError> {
    let parsed = match proxy.as_deref() {
        Some(p) => parse_proxy(p)?,
        None => None,
    };
    let direct = proxy.as_deref().is_some_and(|p| p.trim().is_empty());
    super::legacy::retire(dir);
    let runtime_dir = std::env::temp_dir().join(format!(
        "anyagents-desktop-{}-{}",
        std::process::id(),
        RUNTIME_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let (takeover_cfg, tunnel) = takeover::config().await;
    let cfg = session_config(profile, dir, &runtime_dir, parsed, direct, takeover_cfg);
    let session = match DesktopSession::start(cfg).await {
        Ok(s) => Arc::new(s),
        Err(e) => {
            drop(tunnel);
            let _ = std::fs::remove_dir_all(&runtime_dir);
            return Err(e.into());
        }
    };
    let takeover_url = session.takeover_url().map(|u| u.to_string());
    let watcher = takeover::watch_hand_backs(data, profile, owner, session.clone());
    let now = now_ms();
    Ok(Live {
        session,
        profile: profile.to_string(),
        proxy,
        takeover_url,
        snapshot: std::sync::Mutex::new(None),
        last_used_ms: AtomicI64::new(now),
        started_at_ms: now,
        tunnel,
        watcher,
        runtime_dir,
    })
}

/// Stop `profile`'s session; `true` when one was running. The profile
/// directory (logins included) stays on disk.
pub async fn stop(data: &Path, profile: &str) -> bool {
    let key = profile_dir(data, profile);
    let slot = {
        let mut map = SESSIONS.lock().unwrap_or_else(|p| p.into_inner());
        map.remove(&key)
    };
    let Some(slot) = slot else {
        return false;
    };
    let Some(live) = slot.lock().await.take() else {
        return false;
    };
    shutdown(live).await;
    true
}

async fn shutdown(live: Arc<Live>) {
    // In-flight calls hold clones; let them finish so the browser closes
    // gracefully and flushes the profile.
    let deadline = tokio::time::Instant::now() + STOP_DRAIN;
    let mut live = live;
    let owned = loop {
        match Arc::try_unwrap(live) {
            Ok(l) => break Some(l),
            Err(shared) if tokio::time::Instant::now() < deadline => {
                live = shared;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(_) => break None,
        }
    };
    let Some(owned) = owned else {
        // Still shared: dropping the last clone kills the processes.
        return;
    };
    owned.watcher.abort();
    drop(owned.tunnel);
    let runtime_dir = owned.runtime_dir.clone();
    match Arc::try_unwrap(owned.session) {
        Ok(session) => session.shutdown().await,
        Err(shared) => drop(shared),
    }
    let _ = std::fs::remove_dir_all(runtime_dir);
}

/// Stop a session from synchronous code (agent deletion, archive reaping).
pub fn stop_in_background(data: &Path, profile: &str) {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        let (data, profile) = (data.to_path_buf(), profile.to_string());
        handle.spawn(async move {
            stop(&data, &profile).await;
        });
    }
}

/// Stop a session and wait for it, from a blocking worker thread (never
/// from async code).
pub fn stop_blocking(data: &Path, profile: &str) {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.block_on(stop(data, profile));
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
    for live in all_running() {
        if live.holder() == "agent" && now.saturating_sub(live.last_used_ms()) > idle_ms {
            stop_in_background(data, &live.profile);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxies_parse_without_echoing_input() {
        assert!(parse_proxy("").unwrap().is_none());
        assert!(parse_proxy("  ").unwrap().is_none());
        assert!(parse_proxy("proxy.example.com:3128").unwrap().is_some());
        assert!(
            parse_proxy("socks5://u:p@proxy.example.com:1080")
                .unwrap()
                .is_some()
        );
        let err = parse_proxy("ftp://user:hunter2@proxy.example.com:21").unwrap_err();
        assert_eq!(err.code, "INVALID_INPUT");
        assert!(!err.message.contains("hunter2"), "{}", err.message);
    }

    #[test]
    fn direct_access_disables_system_proxies() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = session_config(
            "p",
            dir.path(),
            &dir.path().join("rt"),
            None,
            true,
            TakeoverConfig::default(),
        );
        assert!(
            cfg.browser
                .extra_args
                .iter()
                .any(|a| a == "--no-proxy-server")
        );
        assert_eq!(cfg.profile.user_data_dir(), dir.path().join("user-data"));
    }
}
