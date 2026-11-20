//! Interactive human takeover (`sway` + `wayvnc` + embedded noVNC via
//! `agent-workstation`, or visible headful window handoff).
//!
//! External binaries (`sway`, `wayvnc`) are resolved from explicit system paths
//! without ambient `$PATH` lookup (ADR 0005).

use std::path::PathBuf;
use std::sync::Arc;

use agent_workstation::{
    BrowserConfig, DesktopSession, HandBack, Holder, Profile, SessionConfig, TakeoverConfig,
    TakeoverStatus,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use url::Url;

use crate::chromium::{discover_chromium, is_executable_file};
use crate::profile_sync::validate_profile_name;
use crate::proxy::{ProxyHandle, ProxyOverride};
use crate::session::{
    BrowserBackend, BrowserError, BrowserInstance, BrowserSession, DESKTOP_PRINCIPAL,
    finalize_instance, prepare_profile_workspace, resolve_session_proxy,
};
use crate::url_guard::navigable_url;

const BIN_DIRS: &str = "/usr/local/bin /usr/bin /opt/homebrew/bin /bin /snap/bin";

#[derive(Clone, Debug, Default, Deserialize, JsonSchema)]
pub struct BrowserTakeoverArgs {
    /// Optional URL to open before handing control to a person.
    pub url: Option<String>,
    /// Optional named persistent browser profile to open for takeover.
    pub profile: Option<String>,
    /// Short reason (e.g. `"solve CAPTCHA"`, `"complete 2FA login"`).
    pub reason: Option<String>,
    /// Optional public base URL override for the noVNC takeover page.
    pub public_url: Option<String>,
    /// Optional browser-only proxy URL (`http://`, `https://`, `socks5://`, `socks5h://`) or `op://` secret_ref.
    #[serde(default)]
    pub proxy: Option<String>,
    /// Optional env var name (e.g. from Named Encrypted Secrets) holding a browser-only proxy URL.
    #[serde(default)]
    pub proxy_env: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema)]
pub struct BrowserTakeoverStatusArgs {
    /// Action: `"status"` (default), `"release"`, or `"stop"`.
    pub action: Option<String>,
    /// Optional note when releasing or stopping takeover.
    pub reason: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TakeoverMode {
    #[default]
    None,
    WayvncNovnc,
    VisibleWindow,
}

#[derive(Clone, Debug, Default)]
pub struct TakeoverState {
    pub active: bool,
    pub mode: TakeoverMode,
    pub reason: Option<String>,
    pub awaited_hand_backs: u64,
}

impl TakeoverState {
    pub fn mark(&mut self, mode: TakeoverMode, awaited: u64, reason: Option<String>) {
        self.active = true;
        self.mode = mode;
        self.awaited_hand_backs = awaited;
        self.reason = trimmed(reason.as_deref()).map(str::to_string);
    }
}

fn trimmed(opt: Option<&str>) -> Option<&str> {
    opt.map(str::trim).filter(|s| !s.is_empty())
}

fn inv(m: impl Into<String>) -> BrowserError {
    BrowserError::invalid_input(m)
}

pub fn extract_takeover_token(raw: &str) -> Option<String> {
    let url = Url::parse(raw).ok()?;
    let (_, val) = url.query_pairs().find(|(k, _)| k == "token")?;
    (!val.is_empty()).then(|| val.into_owned())
}

pub fn parse_public_url(raw: Option<&str>) -> Result<Option<Url>, BrowserError> {
    let Some(s) = trimmed(raw) else {
        return Ok(None);
    };
    let mut u = Url::parse(s).map_err(|e| inv(format!("invalid URL: {e}")))?;
    if !matches!(u.scheme(), "http" | "https") || u.host_str().is_none() {
        return Err(inv("public URL must use http/https"));
    }
    if !u.path().ends_with('/') {
        u.set_path(&format!("{}/", u.path()));
    }
    Ok(Some(u))
}

pub fn find_binary_in(name: &str, dirs: &[PathBuf]) -> Option<PathBuf> {
    dirs.iter()
        .map(|d| d.join(name))
        .find(|p| is_executable_file(p))
}

pub fn desktop_payload(st: &TakeoverStatus, meta: &TakeoverState, done: bool) -> Value {
    let human = st.holder == "human";
    let status = match (human, done && meta.mode != TakeoverMode::None, meta.active) {
        (true, _, _) => "human_in_control",
        (false, true, _) => "takeover_completed",
        (false, false, true) => "takeover_ready",
        _ => "inactive",
    };
    let tok = st.takeover_url.as_deref().and_then(extract_takeover_token);
    json!({
        "status": status, "active": human || (meta.active && !done), "mode": "wayvnc_novnc",
        "holder": st.holder, "takeover_url": st.takeover_url, "novnc_url": st.takeover_url,
        "token": tok, "browser_running": st.browser_running, "completed": done,
        "reason": meta.reason,
    })
}

pub fn simple_payload(meta: Option<&TakeoverState>, running: bool) -> Value {
    let vis = meta.filter(|m| m.mode == TakeoverMode::VisibleWindow);
    let active = vis.is_some_and(|m| m.active) && running;
    let (status, mode, holder) = match (vis.is_some(), active) {
        (true, true) => ("human_in_control", "visible_window", "human"),
        (true, false) => ("takeover_completed", "visible_window", "agent"),
        _ => ("inactive", "none", "agent"),
    };
    json!({
        "status": status, "active": active, "mode": mode, "holder": holder,
        "takeover_url": null, "novnc_url": null, "token": null, "browser_running": running,
        "completed": !active, "reason": vis.and_then(|m| m.reason.as_deref()),
    })
}

impl BrowserSession {
    pub async fn start_takeover(&self, req: BrowserTakeoverArgs) -> Result<Value, BrowserError> {
        let url = trimmed(req.url.as_deref())
            .map(navigable_url)
            .transpose()
            .map_err(inv)?;
        let prof = trimmed(req.profile.as_deref())
            .map(validate_profile_name)
            .transpose()
            .map_err(inv)?;
        let proxy_ov = ProxyOverride::from_opts(req.proxy, req.proxy_env);
        let env_pub = std::env::var("ANYAGENTS_TAKEOVER_PUBLIC_URL").ok();
        let pub_url = parse_public_url(req.public_url.as_deref().or(env_pub.as_deref()))?;
        let dirs: Vec<PathBuf> = BIN_DIRS.split_whitespace().map(PathBuf::from).collect();
        if cfg!(target_os = "linux")
            && let (Some(sway), Some(vnc)) = (
                find_binary_in("sway", &dirs),
                find_binary_in("wayvnc", &dirs),
            )
        {
            let mut g = self.inner.lock().await;
            let alive = g.as_mut().is_some_and(BrowserInstance::is_alive);
            let cur_prof = g.as_ref().and_then(|i| i.active_profile.clone());
            let same_p = prof.is_none() || cur_prof == prof;
            let target_p = prof
                .clone()
                .or_else(|| same_p.then_some(cur_prof.clone()).flatten());
            let effective_prof = target_p.as_deref().or(self.config.profile.as_deref());
            drop(g);
            let desired_proxy =
                resolve_session_proxy(&self.config, effective_prof, proxy_ov.as_ref()).await?;
            g = self.inner.lock().await;
            let alive = g.as_mut().is_some_and(BrowserInstance::is_alive);
            let same_proxy =
                proxy_ov.is_none() || g.as_ref().map(|i| &i.active_proxy) == Some(&desired_proxy);
            let reuse = alive
                && same_p
                && same_proxy
                && g.as_ref()
                    .is_some_and(|i| matches!(i.backend, BrowserBackend::Desktop { .. }));
            if !reuse {
                let old = g.take();
                drop(g);
                if let Some(mut old) = old {
                    finalize_instance(&mut old).await;
                }
                let bin = discover_chromium(self.config.chromium_bin.as_deref())
                    .map_err(BrowserError::browser_not_found)?;
                let ws = prepare_profile_workspace(&self.config, target_p.as_deref()).await?;
                let proxy_handle = match &desired_proxy {
                    Some(p) => Some(ProxyHandle::start(p).await?),
                    None => None,
                };
                let rt = tempfile::Builder::new()
                    .prefix("aa-desk-")
                    .tempdir()
                    .map_err(|e| {
                        BrowserError::new("BROWSER_PROCESS_ERROR", format!("runtime: {e}"))
                    })?;
                let mut browser = BrowserConfig::new(bin);
                browser.extra_args =
                    "--no-sandbox --disable-dev-shm-usage --hide-crash-restore-bubble"
                        .split_whitespace()
                        .map(str::to_string)
                        .collect();
                if let Some(ref ph) = proxy_handle {
                    browser.extra_args.extend(ph.chromium_args());
                }
                browser.extra_args.extend(self.config.parsed_extra_args());
                let root = ws.user_data_dir.parent().unwrap_or(&ws.user_data_dir);
                let p_cfg = Profile::new(ws.active_profile.as_deref().unwrap_or("default"), root);
                let mut cfg = SessionConfig::new(p_cfg, browser, rt.path());
                let bind = std::env::var("ANYAGENTS_TAKEOVER_BIND")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .or_else(|| TakeoverConfig::default().bind);
                cfg.human_idle_timeout = None;
                (cfg.compositor, cfg.vnc) = (sway, vnc);
                cfg.takeover = Some(TakeoverConfig {
                    bind,
                    public_url: pub_url.clone(),
                    token: None,
                });
                let session = Arc::new(DesktopSession::start(cfg).await?);
                let _ = session.lease(DESKTOP_PRINCIPAL)?;
                g = self.inner.lock().await;
                *g = Some(BrowserInstance {
                    backend: BrowserBackend::Desktop {
                        session,
                        _runtime_dir: rt,
                    },
                    persistent_profile: ws.persistent_profile,
                    active_profile: ws.active_profile,
                    active_proxy: desired_proxy,
                    user_data_dir: ws.user_data_dir,
                    remote_sync: ws.remote_sync,
                    takeover: TakeoverState::default(),
                    _proxy_handle: proxy_handle,
                    _ephemeral_dir: ws.ephemeral_dir,
                });
            }
            let inst = g.as_mut().expect("desktop ready");
            let BrowserBackend::Desktop { ref session, .. } = inst.backend else {
                unreachable!()
            };
            if let Some(u) = pub_url {
                let _ = session.set_takeover_public_url(u);
            }
            if session.takeover_url().is_none() {
                return Err(BrowserError::new(
                    "TAKEOVER_UNAVAILABLE",
                    "missing takeover URL",
                ));
            }
            let mut cur = None;
            if matches!(session.gate().holder(), Holder::Agent) {
                let a = session.lease(DESKTOP_PRINCIPAL)?;
                if let Some(u) = url.as_deref() {
                    a.navigate(u).await?;
                }
                cur = a.current_url().await.ok();
            }
            inst.takeover.mark(
                TakeoverMode::WayvncNovnc,
                session.gate().hand_backs(),
                req.reason,
            );
            let mut p = desktop_payload(&session.takeover_status(), &inst.takeover, false);
            p["profile"] = json!(inst.active_profile);
            p["url"] = json!(cur);
            return Ok(p);
        }
        if self.config.headful {
            let mut live = self
                .acquire_with_options(prof.as_deref(), proxy_ov.as_ref())
                .await?;
            if let Some(u) = url.as_deref() {
                live.navigate(u).await?;
            }
            let cur = live.current_url().await.ok();
            drop(live);
            let mut g = self.inner.lock().await;
            let inst = g.as_mut().expect("headful running");
            inst.takeover
                .mark(TakeoverMode::VisibleWindow, 0, req.reason);
            let mut p = simple_payload(Some(&inst.takeover), true);
            p["status"] = json!("takeover_ready");
            p["url"] = json!(cur);
            p["profile"] = json!(inst.active_profile);
            return Ok(p);
        }
        Err(BrowserError::new(
            "TAKEOVER_UNAVAILABLE",
            "interactive takeover requires Linux Wayland (`sway` + `wayvnc`) or `--headful`",
        ))
    }

    pub async fn takeover_status(
        &self,
        req: BrowserTakeoverStatusArgs,
    ) -> Result<Value, BrowserError> {
        let act = trimmed(req.action.as_deref()).map(str::to_ascii_lowercase);
        let kind = match act.as_deref().unwrap_or("status") {
            "stop" | "end" | "cancel" | "end_takeover" => "stop",
            "release" | "resume" | "resume_agent" => "release",
            "status" => "status",
            other => return Err(inv(format!("unknown takeover action `{other}`"))),
        };
        let mut g = self.inner.lock().await;
        let Some(inst) = g.as_mut() else {
            let mut p = simple_payload(None, false);
            p["profile"] = json!(self.config.profile);
            if kind == "stop" {
                p["status"] = json!("stopped");
                p["stopped"] = json!(false);
            } else if kind == "release" {
                p["status"] = json!("released");
                p["released"] = json!(false);
            }
            return Ok(p);
        };
        let alive = inst.is_alive();
        if kind == "stop" || kind == "release" {
            let note = req.reason.unwrap_or_else(|| kind.into());
            let g_rel = match &inst.backend {
                BrowserBackend::Desktop { session, .. } => session.gate().release(Some(note)),
                _ => false,
            };
            if kind == "stop" && matches!(inst.backend, BrowserBackend::Desktop { .. }) {
                drop(g);
                let o = self.close_with_sync().await;
                return Ok(json!({
                    "status": "stopped", "active": false, "stopped": o.was_running, "holder": "agent",
                    "browser_running": false, "profile_preserved": o.profile_preserved,
                    "profile_synced": o.profile_synced, "sync_error": o.sync_error,
                }));
            }
            let changed = if kind == "stop" {
                std::mem::take(&mut inst.takeover).mode != TakeoverMode::None
            } else {
                g_rel || std::mem::replace(&mut inst.takeover.active, false)
            };
            let status = if kind == "stop" {
                "stopped"
            } else {
                "released"
            };
            let mut p = json!({"status": status, "active": false, "holder": "agent", "browser_running": alive});
            p[status] = json!(changed);
            return Ok(p);
        }
        match &mut inst.backend {
            BrowserBackend::Desktop { session, .. } => {
                let gate = session.gate();
                let done = gate.hand_backs() > inst.takeover.awaited_hand_backs
                    || (!inst.takeover.active && matches!(gate.holder(), Holder::Agent));
                inst.takeover.active &= !done;
                let st = session.takeover_status();
                let mut p = desktop_payload(&st, &inst.takeover, done);
                p["profile"] = json!(inst.active_profile);
                if st.holder == "agent"
                    && let Ok(a) = session.lease(DESKTOP_PRINCIPAL)
                {
                    p["url"] = json!(a.current_url().await.ok());
                }
                if let Some(HandBack::Released { note: Some(n) }) = gate.take_hand_back() {
                    p["hand_back_note"] = json!(n);
                }
                Ok(p)
            }
            BrowserBackend::Headless { web, .. } => {
                let mut p = simple_payload(Some(&inst.takeover), alive);
                p["profile"] = json!(inst.active_profile);
                p["url"] = json!(if alive {
                    web.current_url().await.ok()
                } else {
                    None
                });
                Ok(p)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_workstation::ControlGate;

    #[test]
    fn takeover_helpers_visible_state_and_control_gate_transitions() {
        let tok = extract_takeover_token("http://127.0.0.1:6080/?token=abc");
        assert!(tok.as_deref() == Some("abc") && extract_takeover_token("http://x/").is_none());
        let pu = parse_public_url(Some("https://t.example.com/s"))
            .unwrap()
            .unwrap();
        assert!(
            pu.as_str() == "https://t.example.com/s/" && parse_public_url(Some("ftp://x")).is_err()
        );

        let mut st = TakeoverState::default();
        st.mark(TakeoverMode::VisibleWindow, 0, Some("2FA".into()));
        assert_eq!(simple_payload(Some(&st), true)["holder"], "human");
        st.active = false;
        assert_eq!(simple_payload(Some(&st), true)["completed"], true);

        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("wayvnc");
        std::fs::write(&bin, b"#!/bin/sh\n").unwrap();
        let dirs = [dir.path().to_path_buf()];
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let p = |m| std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(m)).unwrap();
            p(0o644);
            assert_eq!(find_binary_in("wayvnc", &dirs), None);
            p(0o755);
        }
        assert_eq!(find_binary_in("wayvnc", &dirs), Some(bin));

        let gate = ControlGate::with_human_idle_timeout(None);
        st.mark(
            TakeoverMode::WayvncNovnc,
            gate.hand_backs(),
            Some("login".into()),
        );
        let u = Some("http://127.0.0.1:6080/?token=tok-123".to_string());
        let ts = |g: &ControlGate| TakeoverStatus::from_gate(g, u.clone(), true);
        let r = desktop_payload(&ts(&gate), &st, false);
        assert!(r["status"] == "takeover_ready" && r["token"] == "tok-123");
        let _ = gate.take(Some("human".into()));
        assert_eq!(
            desktop_payload(&ts(&gate), &st, false)["status"],
            "human_in_control"
        );
        assert!(gate.release(Some("signed in".into())));
        assert_eq!(
            desktop_payload(&ts(&gate), &st, true)["status"],
            "takeover_completed"
        );
    }
}
