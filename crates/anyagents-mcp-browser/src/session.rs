//! Browser process lifecycle (direct headless CDP child or headful browser in a
//! private `sway` Wayland session) and CDP `Web` session management.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use agent_workstation::browser::Web;
use agent_workstation::cdp::Cdp;
use agent_workstation::{DesktopSession, Holder, PageImage, Secret, SecretResolver, Snapshot, Tab};
use serde_json::{Value, json};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, MutexGuard};

use crate::chromium::{discover_chromium, is_headless_shell_binary};
use crate::config::{BrowserMode, BrowserServerConfig};
use crate::desktop::{
    LaunchPlan, default_bin_dirs, find_desktop_tools_in, launch_desktop_instance, plan_launch,
};
use crate::profile_sync::{
    RemoteProfileSync, resolve_sync_config, sync_down_profile, sync_up_profile,
    validate_profile_name,
};
use crate::proxy::{BrowserProxy, ProxyHandle, ProxyOverride, resolve_browser_proxy};
use crate::takeover::{TakeoverMode, TakeoverState};
use crate::tunnel::TakeoverTunnelHandle;

const LAUNCH_TIMEOUT: Duration = Duration::from_secs(45);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(4);

/// Typed error returned by browser session operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrowserError {
    pub code: &'static str,
    pub message: String,
}

impl BrowserError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::new("INVALID_INPUT", message)
    }

    pub fn stale_target(message: impl Into<String>) -> Self {
        Self::new("STALE_TARGET", message)
    }

    pub fn browser_not_found(message: impl Into<String>) -> Self {
        Self::new("BROWSER_NOT_FOUND", message)
    }

    pub fn to_json(&self) -> Value {
        json!({
            "code": self.code,
            "error": self.message,
        })
    }
}

impl std::fmt::Display for BrowserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for BrowserError {}

impl From<agent_workstation::Error> for BrowserError {
    fn from(err: agent_workstation::Error) -> Self {
        let code = match &err {
            agent_workstation::Error::HumanInControl { .. } => "HUMAN_IN_CONTROL",
            agent_workstation::Error::Leased { .. } => "DESKTOP_IN_USE",
            agent_workstation::Error::StaleRef(_) => "STALE_REF",
            agent_workstation::Error::NotFound(_) => "NOT_FOUND",
            agent_workstation::Error::InvalidInput(_) => "INVALID_INPUT",
            agent_workstation::Error::Timeout(_) => "TIMEOUT",
            agent_workstation::Error::Script(_) => "SCRIPT_ERROR",
            agent_workstation::Error::Unsupported(_) => "UNSUPPORTED",
            agent_workstation::Error::Refused(_) => "REFUSED",
            agent_workstation::Error::Process(_)
            | agent_workstation::Error::Profile(_)
            | agent_workstation::Error::Closed => "BROWSER_PROCESS_ERROR",
            _ => "PROTOCOL_ERROR",
        };
        Self {
            code,
            message: err.to_string(),
        }
    }
}

pub(crate) const DESKTOP_PRINCIPAL: &str = "anyagents-mcp-browser";

#[derive(Debug)]
struct PreResolvedSecret(std::sync::Mutex<Option<Secret>>);

impl SecretResolver for PreResolvedSecret {
    fn resolve<'a, 'b, 'c>(
        &'a self,
        _: &'b str,
    ) -> Pin<Box<dyn Future<Output = agent_workstation::Result<Secret>> + Send + 'c>>
    where
        'a: 'c,
        'b: 'c,
        Self: 'c,
    {
        let taken = self.0.lock().unwrap_or_else(|p| p.into_inner()).take();
        Box::pin(async move {
            taken.ok_or_else(|| agent_workstation::Error::Refused("secret consumed".into()))
        })
    }
}

/// Active browser process backend: a directly launched CDP child (headless, or
/// `--headful` on the host display) or a headful browser in a private `sway` session.
pub enum BrowserBackend {
    Headless {
        child: Child,
        ws_endpoint: String,
        web: Box<Web>,
    },
    Desktop {
        session: Arc<DesktopSession>,
        _runtime_dir: tempfile::TempDir,
    },
}

/// Live Chromium or desktop takeover instance + profile metadata.
pub struct BrowserInstance {
    pub backend: BrowserBackend,
    pub persistent_profile: bool,
    pub active_profile: Option<String>,
    pub active_proxy: Option<BrowserProxy>,
    pub user_data_dir: PathBuf,
    pub remote_sync: Option<RemoteProfileSync>,
    pub takeover: TakeoverState,
    pub(crate) takeover_tunnel: Option<TakeoverTunnelHandle>,
    pub(crate) _proxy_handle: Option<ProxyHandle>,
    pub(crate) _ephemeral_dir: Option<tempfile::TempDir>,
}

impl BrowserInstance {
    pub(crate) fn is_alive(&mut self) -> bool {
        match &mut self.backend {
            BrowserBackend::Headless { child, .. } => matches!(child.try_wait(), Ok(None)),
            BrowserBackend::Desktop { session, .. } => {
                !session.is_closed()
                    && (session.takeover_status().browser_running
                        || matches!(session.gate().holder(), Holder::Human { .. }))
            }
        }
    }
}

/// Result of closing a live browser session, including remote profile sync status.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloseSessionOutcome {
    pub was_running: bool,
    pub profile_preserved: bool,
    pub profile_synced: bool,
    pub sync_error: Option<String>,
}

/// Exclusive lease over a live Chromium CDP controller (headless or desktop takeover).
pub struct LiveWebGuard<'a> {
    guard: MutexGuard<'a, Option<BrowserInstance>>,
}

macro_rules! web_methods {
    ($(pub async fn $name:ident(&mut self $(, $arg:ident : $ty:ty)*) -> Result<$ret:ty, BrowserError>;)*) => {
        $(pub async fn $name(&mut self $(, $arg: $ty)*) -> Result<$ret, BrowserError> {
            match &mut self.instance()?.backend {
                BrowserBackend::Headless { web, .. } => Ok(web.$name($($arg),*).await?),
                BrowserBackend::Desktop { session, .. } => Ok(session.lease(DESKTOP_PRINCIPAL)?.$name($($arg),*).await?),
            }
        })*
    };
}

impl LiveWebGuard<'_> {
    /// Return a mutable handle implementing all CDP web operations.
    pub fn web(&mut self) -> &mut Self {
        self
    }

    fn instance(&mut self) -> Result<&mut BrowserInstance, BrowserError> {
        let inst = self
            .guard
            .as_mut()
            .expect("LiveWebGuard holds live instance");
        if inst.takeover.mode == TakeoverMode::VisibleWindow && inst.takeover.active {
            return Err(BrowserError::new(
                "HUMAN_IN_CONTROL",
                "human holds takeover window; release or stop to resume",
            ));
        }
        Ok(inst)
    }

    web_methods! {
        pub async fn navigate(&mut self, url: &str) -> Result<String, BrowserError>;
        pub async fn reload(&mut self) -> Result<String, BrowserError>;
        pub async fn snapshot(&mut self) -> Result<Snapshot, BrowserError>;
        pub async fn read(&mut self) -> Result<String, BrowserError>;
        pub async fn html(&mut self, selector: Option<&str>) -> Result<String, BrowserError>;
        pub async fn click(&mut self, selector: &str) -> Result<(), BrowserError>;
        pub async fn fill(&mut self, selector: &str, text: &str) -> Result<(), BrowserError>;
        pub async fn press(&mut self, key: &str) -> Result<(), BrowserError>;
        pub async fn page_screenshot(&mut self, full_page: bool) -> Result<PageImage, BrowserError>;
        pub async fn eval(&mut self, script: &str) -> Result<Value, BrowserError>;
        pub async fn tabs(&mut self) -> Result<Vec<Tab>, BrowserError>;
        pub async fn new_tab(&mut self, url: &str) -> Result<String, BrowserError>;
        pub async fn select_tab(&mut self, id: &str) -> Result<(), BrowserError>;
        pub async fn close_tab(&mut self, id: &str) -> Result<(), BrowserError>;
        pub async fn current_url(&mut self) -> Result<String, BrowserError>;
    }

    pub async fn history(&mut self, delta: i64) -> Result<String, BrowserError> {
        match &mut self.instance()?.backend {
            BrowserBackend::Headless { web, .. } => Ok(web.history(delta).await?),
            BrowserBackend::Desktop { session, .. } => {
                let a = session.lease(DESKTOP_PRINCIPAL)?;
                Ok(if delta < 0 {
                    a.back().await?
                } else {
                    a.forward().await?
                })
            }
        }
    }

    pub async fn fill_secret(
        &mut self,
        selector: &str,
        secret_ref: &str,
        secret: Secret,
        expect_origin: Option<&str>,
    ) -> Result<(), BrowserError> {
        match &mut self.instance()?.backend {
            BrowserBackend::Headless { web, .. } => {
                Ok(web.fill_secret(selector, &secret, expect_origin).await?)
            }
            BrowserBackend::Desktop { session, .. } => {
                let resolver = PreResolvedSecret(std::sync::Mutex::new(Some(secret)));
                let a = session.lease(DESKTOP_PRINCIPAL)?;
                Ok(
                    a.fill_secret(selector, secret_ref, &resolver, expect_origin)
                        .await?,
                )
            }
        }
    }
}

/// Thread-safe lazily-initialized Chromium session handle.
#[derive(Clone)]
pub struct BrowserSession {
    pub(crate) config: Arc<BrowserServerConfig>,
    pub(crate) inner: Arc<Mutex<Option<BrowserInstance>>>,
}

impl BrowserSession {
    /// Create a new browser session manager (Chromium is started lazily on first tool call).
    pub fn new(config: BrowserServerConfig) -> Self {
        Self {
            config: Arc::new(config),
            inner: Arc::new(Mutex::new(None)),
        }
    }

    /// Borrow the underlying server configuration.
    pub fn config(&self) -> &BrowserServerConfig {
        &self.config
    }

    /// Acquire exclusive access to the live `Web` CDP client, launching Chromium
    /// lazily if not yet running (or if the previous process exited).
    pub async fn acquire(&self) -> Result<LiveWebGuard<'_>, BrowserError> {
        self.acquire_with_profile(None).await
    }

    /// Acquire exclusive access to a live Chromium session, optionally selecting
    /// a named persistent browser profile synced via the AnyAgents coordinator.
    ///
    /// If a live session is running under a different named profile, it is closed
    /// and synced up first before syncing down and launching the requested profile.
    pub async fn acquire_with_profile(
        &self,
        requested_profile: Option<&str>,
    ) -> Result<LiveWebGuard<'_>, BrowserError> {
        self.acquire_with_options(requested_profile, None).await
    }

    /// Acquire exclusive access to a live Chromium session, optionally selecting
    /// a named persistent browser profile and/or browser-only proxy override.
    pub async fn acquire_with_options(
        &self,
        requested_profile: Option<&str>,
        proxy_override: Option<&ProxyOverride>,
    ) -> Result<LiveWebGuard<'_>, BrowserError> {
        let normalized_req = requested_profile
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(validate_profile_name)
            .transpose()
            .map_err(BrowserError::invalid_input)?;

        let mut guard = self.inner.lock().await;
        let alive = guard.as_mut().is_some_and(BrowserInstance::is_alive);
        let cur_profile = guard.as_ref().and_then(|i| i.active_profile.clone());
        let profile_mismatch = alive
            && normalized_req.is_some()
            && cur_profile.as_deref() != normalized_req.as_deref();

        let effective_profile = normalized_req
            .as_deref()
            .or(cur_profile.as_deref())
            .or(self.config.profile.as_deref())
            .map(str::to_string);
        let desired_proxy = if !alive || profile_mismatch || proxy_override.is_some() {
            drop(guard);
            let p =
                resolve_session_proxy(&self.config, effective_profile.as_deref(), proxy_override)
                    .await?;
            guard = self.inner.lock().await;
            Some(p)
        } else {
            None
        };
        let alive = guard.as_mut().is_some_and(BrowserInstance::is_alive);
        let proxy_mismatch = alive
            && desired_proxy
                .as_ref()
                .is_some_and(|want| guard.as_ref().map(|i| &i.active_proxy) != Some(want));

        if !alive || profile_mismatch || proxy_mismatch {
            if let Some(mut old) = guard.take() {
                drop(guard);
                finalize_instance(&mut old).await;
                guard = self.inner.lock().await;
            }
            let proxy = match desired_proxy {
                Some(p) => p,
                None => {
                    drop(guard);
                    let p = resolve_session_proxy(
                        &self.config,
                        effective_profile.as_deref(),
                        proxy_override,
                    )
                    .await?;
                    guard = self.inner.lock().await;
                    p
                }
            };
            let launched = launch_instance(&self.config, normalized_req.as_deref(), proxy).await?;
            *guard = Some(launched);
        }
        Ok(LiveWebGuard { guard })
    }

    /// Gracefully stop the running Chromium instance, flushing profile data to
    /// disk and syncing any active named profile archive back to the coordinator.
    pub async fn close_with_sync(&self) -> CloseSessionOutcome {
        let persistent_configured =
            self.config.profile_dir.is_some() || self.config.profile.is_some();
        let taken = {
            let mut guard = self.inner.lock().await;
            guard.take()
        };
        let Some(mut inst) = taken else {
            return CloseSessionOutcome {
                was_running: false,
                profile_preserved: persistent_configured,
                profile_synced: false,
                sync_error: None,
            };
        };
        let (profile_synced, sync_error) = finalize_instance(&mut inst).await;
        CloseSessionOutcome {
            was_running: true,
            profile_preserved: inst.persistent_profile,
            profile_synced,
            sync_error,
        }
    }

    /// Gracefully stop the running Chromium instance, flushing profile data to disk.
    ///
    /// Returns `(was_running, profile_preserved)`.
    pub async fn close(&self) -> (bool, bool) {
        let outcome = self.close_with_sync().await;
        (outcome.was_running, outcome.profile_preserved)
    }
}

/// Resolve an optional explicit `target_id` against live tabs, failing closed
/// with `STALE_TARGET` if the requested tab does not exist.
pub async fn select_explicit_target(
    web: &mut LiveWebGuard<'_>,
    target_id: Option<&str>,
) -> Result<Option<String>, BrowserError> {
    let Some(raw) = target_id else {
        return Ok(None);
    };
    let id = raw.trim();
    if id.is_empty() {
        return Err(BrowserError::invalid_input(
            "browser `target_id` cannot be empty",
        ));
    }
    let tabs = web.tabs().await?;
    let Some(tab) = tabs.iter().find(|t| t.id == id) else {
        let live = if tabs.is_empty() {
            "none".to_string()
        } else {
            tabs.iter()
                .take(8)
                .map(|t| {
                    let url = if t.url.is_empty() {
                        "about:blank"
                    } else {
                        t.url.as_str()
                    };
                    format!("{} ({url})", t.id)
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        return Err(BrowserError::stale_target(format!(
            "browser target `{id}` does not exist (live targets: {live})"
        )));
    };
    if !tab.active {
        web.select_tab(&tab.id).await?;
    }
    Ok(Some(tab.id.clone()))
}

/// Build Chromium launch flags for standalone headless (or optional headful) operation.
pub fn build_launch_args(
    binary: &Path,
    user_data_dir: &Path,
    config: &BrowserServerConfig,
) -> Vec<String> {
    let direct = config
        .proxy
        .as_deref()
        .and_then(|raw| BrowserProxy::parse(raw).ok())
        .filter(|p| !p.is_authenticated())
        .map(|p| p.direct_chromium_args());
    build_launch_args_with_proxy(
        binary,
        user_data_dir,
        config,
        direct.as_ref().map(AsRef::as_ref),
    )
}

/// Build Chromium launch flags using pre-resolved, credential-free proxy flags.
pub fn build_launch_args_with_proxy(
    binary: &Path,
    user_data_dir: &Path,
    config: &BrowserServerConfig,
    proxy_args: Option<&[String]>,
) -> Vec<String> {
    let mut args = Vec::new();
    if !config.headful && !is_headless_shell_binary(binary) {
        args.push("--headless=new".to_string());
    }
    args.push(format!("--user-data-dir={}", user_data_dir.display()));
    args.push("--remote-debugging-port=0".to_string());
    args.push("--remote-debugging-address=127.0.0.1".to_string());
    args.push("--no-sandbox".to_string());
    args.push("--disable-dev-shm-usage".to_string());
    if !config.headful {
        args.push("--disable-gpu".to_string());
    }
    args.push("--hide-crash-restore-bubble".to_string());
    args.push("--no-first-run".to_string());
    args.push("--no-default-browser-check".to_string());
    args.push("--password-store=basic".to_string());
    args.push("--disable-blink-features=AutomationControlled".to_string());
    let languages = config.languages();
    if let Some(first) = languages.first() {
        args.push(format!("--lang={first}"));
        args.push(format!("--accept-lang={}", languages.join(",")));
    }
    args.push(format!(
        "--window-size={},{}",
        config.viewport_width.max(320),
        config.viewport_height.max(240)
    ));
    if let Some(p_args) = proxy_args {
        args.extend(p_args.iter().cloned());
    }
    args.extend(config.parsed_extra_args());
    args.push("about:blank".to_string());
    args
}

pub(crate) async fn resolve_session_proxy(
    config: &BrowserServerConfig,
    profile: Option<&str>,
    proxy_override: Option<&ProxyOverride>,
) -> Result<Option<BrowserProxy>, BrowserError> {
    let cfg_proxy = config.proxy.clone();
    let prof = profile.map(str::to_string);
    let ov = proxy_override.cloned();
    tokio::task::spawn_blocking(move || {
        resolve_browser_proxy(cfg_proxy.as_deref(), prof.as_deref(), ov.as_ref())
    })
    .await
    .map_err(|e| BrowserError::new("PROXY_CONFIG_ERROR", format!("proxy resolve task: {e}")))?
}

/// Parse Chromium's `<user-data-dir>/DevToolsActivePort` into a `ws://127.0.0.1:<port><path>` URL.
pub fn parse_devtools_active_port(contents: &str) -> Option<(u16, String)> {
    let mut lines = contents.lines();
    let port: u16 = lines.next()?.trim().parse().ok()?;
    let path = lines.next()?.trim();
    if port == 0 || !path.starts_with("/devtools/browser/") {
        return None;
    }
    Some((port, format!("ws://127.0.0.1:{port}{path}")))
}

/// Parse a `DevTools listening on ws://127.0.0.1:<port>/devtools/browser/...` line from Chromium logs.
pub fn parse_devtools_stderr_url(log_text: &str) -> Option<(u16, String)> {
    for line in log_text.lines() {
        let Some((_, rest)) = line.split_once("DevTools listening on ") else {
            continue;
        };
        let candidate = rest.trim();
        let Ok(parsed) = url::Url::parse(candidate) else {
            continue;
        };
        if matches!(parsed.scheme(), "ws" | "wss")
            && let Some(port) = parsed.port()
            && parsed.path().starts_with("/devtools/browser/")
        {
            return Some((port, candidate.to_string()));
        }
    }
    None
}

pub(crate) struct PreparedWorkspace {
    pub active_profile: Option<String>,
    pub user_data_dir: PathBuf,
    pub persistent_profile: bool,
    pub remote_sync: Option<RemoteProfileSync>,
    pub ephemeral_dir: Option<tempfile::TempDir>,
}

pub(crate) async fn prepare_profile_workspace(
    config: &BrowserServerConfig,
    requested_profile: Option<&str>,
) -> Result<PreparedWorkspace, BrowserError> {
    let remote_sync = resolve_sync_config(config, requested_profile)?;
    let active_profile = remote_sync
        .as_ref()
        .map(|s| s.profile_name.clone())
        .or_else(|| {
            requested_profile
                .or(config.profile.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        });
    if let Some(ref name) = active_profile {
        validate_profile_name(name).map_err(BrowserError::invalid_input)?;
    }

    let (user_data_dir, persistent_profile, ephemeral_dir): (
        PathBuf,
        bool,
        Option<tempfile::TempDir>,
    ) = match (&config.profile_dir, &active_profile) {
        (Some(dir), Some(name)) => (dir.join(name).join("user-data"), true, None),
        (Some(dir), None) => (dir.join("user-data"), true, None),
        (None, _) => {
            let tmp = tempfile::Builder::new()
                .prefix("anyagents-mcp-browser-")
                .tempdir()
                .map_err(|e| {
                    BrowserError::new(
                        "BROWSER_PROCESS_ERROR",
                        format!("failed creating ephemeral browser profile directory: {e}"),
                    )
                })?;
            let ud = tmp.path().join("user-data");
            (ud, remote_sync.is_some(), Some(tmp))
        }
    };

    if let Some(ref sync) = remote_sync {
        let sync_c = sync.clone();
        let ud_c = user_data_dir.clone();
        tokio::task::spawn_blocking(move || sync_down_profile(&sync_c, &ud_c))
            .await
            .map_err(|e| {
                BrowserError::new(
                    "PROFILE_SYNC_ERROR",
                    format!("profile sync-down task failed: {e}"),
                )
            })??;
    }

    std::fs::create_dir_all(&user_data_dir).map_err(|e| {
        BrowserError::new(
            "BROWSER_PROCESS_ERROR",
            format!(
                "failed creating profile directory `{}`: {e}",
                user_data_dir.display()
            ),
        )
    })?;

    #[cfg(target_os = "linux")]
    agent_workstation::profile::clear_stale_singleton(&user_data_dir)?;

    Ok(PreparedWorkspace {
        active_profile,
        user_data_dir,
        persistent_profile,
        remote_sync,
        ephemeral_dir,
    })
}

pub(crate) async fn launch_instance(
    config: &BrowserServerConfig,
    requested_profile: Option<&str>,
    proxy: Option<BrowserProxy>,
) -> Result<BrowserInstance, BrowserError> {
    let binary = discover_chromium(config.chromium_bin.as_deref())
        .map_err(BrowserError::browser_not_found)?;
    let tools = match config.browser_mode {
        BrowserMode::Headless => None,
        BrowserMode::Auto | BrowserMode::Desktop => find_desktop_tools_in(&default_bin_dirs()),
    };
    match plan_launch(config.browser_mode, config.headful, &binary, tools)? {
        LaunchPlan::Desktop(tools) => {
            let launched = launch_desktop_instance(
                config,
                requested_profile,
                proxy.clone(),
                binary.clone(),
                &tools,
                None,
            )
            .await;
            match launched {
                Ok(inst) => return Ok(inst),
                Err(err) if config.browser_mode == BrowserMode::Auto => eprintln!(
                    "anyagents-mcp-browser: headful desktop browser failed to start ({err}); falling back to headless"
                ),
                Err(err) => return Err(err),
            }
        }
        LaunchPlan::Direct {
            fallback_reason: Some(reason),
        } => eprintln!("anyagents-mcp-browser: using headless browser: {reason}"),
        LaunchPlan::Direct { .. } => {}
    }
    launch_direct_instance(config, requested_profile, proxy, binary).await
}

async fn launch_direct_instance(
    config: &BrowserServerConfig,
    requested_profile: Option<&str>,
    proxy: Option<BrowserProxy>,
    binary: PathBuf,
) -> Result<BrowserInstance, BrowserError> {
    let ws = prepare_profile_workspace(config, requested_profile).await?;
    let proxy_handle = match &proxy {
        Some(p) => Some(ProxyHandle::start(p).await?),
        None => None,
    };
    let port_file = ws.user_data_dir.join("DevToolsActivePort");
    if let Err(err) = std::fs::remove_file(&port_file)
        && err.kind() != std::io::ErrorKind::NotFound
    {
        return Err(BrowserError::new(
            "BROWSER_PROCESS_ERROR",
            format!("failed clearing stale DevToolsActivePort: {err}"),
        ));
    }

    let log_path = ws.user_data_dir.join("devtools.log");
    let log_file = std::fs::File::create(&log_path).map_err(|e| {
        BrowserError::new(
            "BROWSER_PROCESS_ERROR",
            format!("failed creating Chromium log `{}`: {e}", log_path.display()),
        )
    })?;
    let log_stderr = log_file.try_clone().map_err(|e| {
        BrowserError::new(
            "BROWSER_PROCESS_ERROR",
            format!("failed cloning Chromium log handle: {e}"),
        )
    })?;

    let proxy_args = proxy_handle.as_ref().map(ProxyHandle::chromium_args);
    let args = build_launch_args_with_proxy(
        &binary,
        &ws.user_data_dir,
        config,
        proxy_args.as_ref().map(AsRef::as_ref),
    );
    let mut cmd = Command::new(&binary);
    cmd.args(&args)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_stderr))
        .kill_on_drop(true);
    if let Some(tz) = config.resolved_timezone() {
        cmd.env("TZ", tz);
    }
    #[cfg(unix)]
    cmd.process_group(0);

    let mut child = cmd.spawn().map_err(|e| {
        BrowserError::new(
            "BROWSER_PROCESS_ERROR",
            format!("failed launching `{}`: {e}", binary.display()),
        )
    })?;

    let ws_endpoint = wait_for_devtools(&mut child, &port_file, &log_path).await?;
    let web = match Web::connect(&ws_endpoint).await {
        Ok(web) => web,
        Err(err) => {
            let _ = child.kill().await;
            return Err(err.into());
        }
    };

    Ok(BrowserInstance {
        backend: BrowserBackend::Headless {
            child,
            ws_endpoint,
            web: Box::new(web),
        },
        persistent_profile: ws.persistent_profile,
        active_profile: ws.active_profile,
        active_proxy: proxy,
        user_data_dir: ws.user_data_dir,
        remote_sync: ws.remote_sync,
        takeover: TakeoverState::default(),
        takeover_tunnel: None,
        _proxy_handle: proxy_handle,
        _ephemeral_dir: ws.ephemeral_dir,
    })
}

pub(crate) async fn finalize_instance(inst: &mut BrowserInstance) -> (bool, Option<String>) {
    if let Some(tunnel) = inst.takeover_tunnel.take() {
        tunnel.stop().await;
    }
    shutdown_backend(&mut inst.backend).await;
    // Flush cookie stores (incl. SQLite WAL/shm sidecars) and `Local State`
    // to disk before any sync-up: the browser may have been killed mid-write,
    // leaving WAL frames or a racing `Local State` (incl. DBSC keys) that must
    // not be archived into the cross-container S3 profile.
    if let Err(err) = crate::profile_sync::fsync_profile_files(&inst.user_data_dir) {
        eprintln!("anyagents-mcp-browser: failed flushing profile files after shutdown: {err}");
    }
    inst._proxy_handle = None;
    let Some(sync) = inst.remote_sync.take() else {
        return (false, None);
    };
    let ud = inst.user_data_dir.clone();
    match tokio::task::spawn_blocking(move || sync_up_profile(&sync, &ud)).await {
        Ok(Ok(_)) => (true, None),
        Ok(Err(err)) => {
            eprintln!("anyagents-mcp-browser profile sync-up failed: {err}");
            (false, Some(err.to_string()))
        }
        Err(err) => {
            let msg = format!("profile sync-up task failed: {err}");
            eprintln!("anyagents-mcp-browser {msg}");
            (false, Some(msg))
        }
    }
}

async fn wait_for_devtools(
    child: &mut Child,
    port_file: &Path,
    log_path: &Path,
) -> Result<String, BrowserError> {
    let deadline = Instant::now() + LAUNCH_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait().map_err(|e| {
            BrowserError::new(
                "BROWSER_PROCESS_ERROR",
                format!("failed checking Chromium process status: {e}"),
            )
        })? {
            let snippet = std::fs::read_to_string(log_path)
                .ok()
                .map(|s| s.chars().take(600).collect::<String>())
                .unwrap_or_default();
            return Err(BrowserError::new(
                "BROWSER_PROCESS_ERROR",
                format!("Chromium exited during startup ({status}): {snippet}"),
            ));
        }

        let discovered = std::fs::read_to_string(port_file)
            .ok()
            .and_then(|txt| parse_devtools_active_port(&txt))
            .or_else(|| {
                std::fs::read_to_string(log_path)
                    .ok()
                    .and_then(|txt| parse_devtools_stderr_url(&txt))
            });

        if let Some((port, ws_url)) = discovered
            && tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_ok()
        {
            return Ok(ws_url);
        }

        if Instant::now() >= deadline {
            let _ = child.kill().await;
            return Err(BrowserError::new(
                "TIMEOUT",
                "timed out waiting for Chromium DevTools endpoint",
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub(crate) async fn shutdown_backend(backend: &mut BrowserBackend) {
    let BrowserBackend::Headless {
        child, ws_endpoint, ..
    } = backend
    else {
        if let BrowserBackend::Desktop { session, .. } = backend {
            session.close().await;
        }
        return;
    };
    if let Ok(mut cdp) = Cdp::connect(ws_endpoint).await {
        for page in cdp.pages().await.unwrap_or_default() {
            if let Some(id) = page["targetId"].as_str() {
                let _ = cdp
                    .call_in(
                        None,
                        "Target.closeTarget",
                        json!({ "targetId": id }),
                        Duration::from_secs(2),
                    )
                    .await;
            }
        }
        if tokio::time::timeout(SHUTDOWN_GRACE, child.wait())
            .await
            .is_ok()
        {
            return;
        }
        let _ = tokio::time::timeout(Duration::from_secs(2), cdp.call("Browser.close", json!({})))
            .await;
    }
    if tokio::time::timeout(SHUTDOWN_GRACE, child.wait())
        .await
        .is_err()
    {
        let _ = child.kill().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn launch_args_default_to_headless_new_with_sandbox_safe_flags() {
        let cfg = BrowserServerConfig {
            proxy: Some("http://127.0.0.1:8888".into()),
            extra_args: Some("--mute-audio".into()),
            ..Default::default()
        };
        let args = build_launch_args(Path::new("/usr/bin/chromium"), Path::new("/tmp/ud"), &cfg);
        assert!(args.contains(&"--headless=new".to_string()));
        assert!(args.contains(&"--no-sandbox".to_string()));
        assert!(args.contains(&"--disable-dev-shm-usage".to_string()));
        assert!(args.contains(&"--remote-debugging-port=0".to_string()));
        assert!(args.contains(&"--remote-debugging-address=127.0.0.1".to_string()));
        assert!(args.contains(&"--user-data-dir=/tmp/ud".to_string()));
        assert!(args.contains(&"--proxy-server=http://127.0.0.1:8888".to_string()));
        assert!(args.contains(&format!(
            "--proxy-bypass-list={}",
            crate::proxy::PROXY_BYPASS_LIST
        )));
        assert!(args.contains(&"--mute-audio".to_string()));
        assert_eq!(args.last().map(String::as_str), Some("about:blank"));

        // Direct build_launch_args must never leak authenticated proxy credentials into argv.
        let authed = BrowserServerConfig {
            proxy: Some("http://user:topsecret@proxy.example:3128".into()),
            ..Default::default()
        };
        let authed_args = build_launch_args(
            Path::new("/usr/bin/chromium"),
            Path::new("/tmp/ud"),
            &authed,
        );
        assert!(
            authed_args
                .iter()
                .all(|a| !a.contains("topsecret") && !a.contains("user:"))
        );
    }

    #[test]
    fn launch_args_omit_headless_flag_for_headful_or_headless_shell() {
        let headful = BrowserServerConfig {
            headful: true,
            ..Default::default()
        };
        let args = build_launch_args(
            Path::new("/usr/bin/chromium"),
            Path::new("/tmp/ud"),
            &headful,
        );
        assert!(!args.iter().any(|a| a.starts_with("--headless")));
        assert!(!args.contains(&"--disable-gpu".to_string()));
        assert!(!args.iter().any(|a| a.starts_with("--lang=")));

        let localized = BrowserServerConfig {
            locale: Some("en-GB,en".into()),
            ..Default::default()
        };
        let args = build_launch_args(
            Path::new("/usr/bin/chromium"),
            Path::new("/tmp/ud"),
            &localized,
        );
        assert!(args.contains(&"--lang=en-GB".to_string()));
        assert!(args.contains(&"--accept-lang=en-GB,en".to_string()));

        let headless_shell_args = build_launch_args(
            Path::new("/opt/playwright/chromium_headless_shell-1180/chrome-linux/headless_shell"),
            Path::new("/tmp/ud"),
            &BrowserServerConfig::default(),
        );
        assert!(
            !headless_shell_args
                .iter()
                .any(|a| a.starts_with("--headless"))
        );
    }

    #[test]
    fn parses_devtools_active_port_and_stderr_lines() {
        let active = "42319\n/devtools/browser/abc-123\n";
        assert_eq!(
            parse_devtools_active_port(active),
            Some((
                42319,
                "ws://127.0.0.1:42319/devtools/browser/abc-123".into()
            ))
        );
        assert_eq!(parse_devtools_active_port("0\n/devtools/browser/abc"), None);

        let stderr = "some log\nDevTools listening on ws://127.0.0.1:9222/devtools/browser/xyz-9\n";
        assert_eq!(
            parse_devtools_stderr_url(stderr),
            Some((9222, "ws://127.0.0.1:9222/devtools/browser/xyz-9".into()))
        );
    }
}
