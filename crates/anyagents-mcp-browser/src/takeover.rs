//! Interactive human takeover (`sway` + `wayvnc` + embedded noVNC via
//! `agent-workstation`, or visible headful window handoff).
//!
//! External binaries (`sway`, `wayvnc`) are resolved from explicit system paths
//! without ambient `$PATH` lookup (ADR 0005).

use agent_workstation::{HandBack, Holder, TakeoverConfig, TakeoverStatus};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use std::path::PathBuf;
use url::Url;

use crate::chromium::{discover_chromium, is_executable_file};
use crate::desktop::{DesktopTools, default_bin_dirs, launch_desktop_instance};
use crate::profile_sync::{resolve_api_url, resolve_coordinator_auth, validate_profile_name};
use crate::proxy::ProxyOverride;
use crate::session::{
    BrowserBackend, BrowserError, BrowserInstance, BrowserSession, DESKTOP_PRINCIPAL,
    finalize_instance, resolve_session_proxy,
};
use crate::tunnel::{TakeoverTunnelHandle, TunnelConfig};
use crate::url_guard::navigable_url;

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

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum DetachedDesktop {
    None,
    Viewing,
    Reclaimable,
}

impl BrowserInstance {
    fn human_handed_back(&self) -> bool {
        self.takeover.active
            && matches!(&self.backend, BrowserBackend::Desktop { session, .. }
                if session.gate().hand_backs() > self.takeover.awaited_hand_backs)
    }

    fn tunnel_ended(&self) -> bool {
        self.takeover_tunnel
            .as_ref()
            .is_some_and(TakeoverTunnelHandle::is_finished)
    }

    async fn reap_viewer(&mut self) {
        if self.human_handed_back() {
            self.detach_takeover(None).await;
        }
    }

    async fn detach_takeover(&mut self, note: Option<String>) -> bool {
        if let Some(tunnel) = &self.takeover_tunnel {
            tunnel.quiesce();
        }
        let had_takeover = std::mem::replace(&mut self.takeover.active, false);
        let detached = match &self.backend {
            BrowserBackend::Desktop { session, .. } => session.detach_takeover(note).await,
            BrowserBackend::Headless { .. } => false,
        };
        if let Some(tunnel) = self.takeover_tunnel.take() {
            tunnel.stop().await;
        }
        detached || had_takeover
    }
}

impl BrowserSession {
    #[cfg(target_os = "linux")]
    pub(crate) async fn detached_desktop(&self) -> DetachedDesktop {
        self.reap_takeover().await;
        match self.inner.lock().await.as_ref() {
            Some(inst) if inst.takeover.mode == TakeoverMode::WayvncNovnc => {
                if !inst.takeover.active {
                    DetachedDesktop::Reclaimable
                } else if inst.takeover_tunnel.is_some() {
                    DetachedDesktop::Viewing
                } else {
                    // A local-only viewer has no reverse tunnel lease to
                    // bound its lifetime after the MCP client disconnects.
                    DetachedDesktop::None
                }
            }
            _ => DetachedDesktop::None,
        }
    }

    // Revoke a finished viewer before admitting another tool without replacing
    // the desktop. A dead browser, unlike a completed hand-back, is finalized.
    pub(crate) async fn reap_takeover(&self) {
        let mut inner = self.inner.lock().await;
        let Some(inst) = inner.as_mut() else { return };
        // Coordinator can terminate the old relay immediately after /release.
        // A gate hand-back takes precedence, but a tunnel that ends without
        // one (TTL, terminal transport error) cannot retain a private desktop.
        let tunnel_ended = inst.tunnel_ended();
        inst.reap_viewer().await;
        if (!inst.is_alive() || (tunnel_ended && inst.takeover.active))
            && let Some(mut dead) = inner.take()
        {
            finalize_instance(&mut dead).await;
        }
    }

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
        let dirs = default_bin_dirs();
        if cfg!(target_os = "linux")
            && let (Some(sway), Some(vnc)) = (
                find_binary_in("sway", &dirs),
                find_binary_in("wayvnc", &dirs),
            )
        {
            let mut g = self.inner.lock().await;
            let current_profile = g.as_ref().and_then(|i| i.active_profile.as_deref());
            let target_profile = prof.as_deref().or(current_profile).map(str::to_owned);
            let effective_profile = target_profile.as_deref().or(self.config.profile.as_deref());
            let desired_proxy =
                resolve_session_proxy(&self.config, effective_profile, proxy_ov.as_ref()).await?;
            let same_profile = prof.is_none() || prof.as_deref() == current_profile;
            let reuse = same_profile
                && g.as_mut().is_some_and(|inst| {
                    inst.is_alive()
                        && (proxy_ov.is_none() || inst.active_proxy == desired_proxy)
                        && matches!(inst.backend, BrowserBackend::Desktop { .. })
                });
            if !reuse {
                if let Some(mut old) = g.take() {
                    finalize_instance(&mut old).await;
                }
                let bin = discover_chromium(self.config.chromium_bin.as_deref())
                    .map_err(BrowserError::browser_not_found)?;
                let tools = DesktopTools {
                    compositor: sway,
                    vnc: Some(vnc),
                };
                *g = Some(
                    launch_desktop_instance(
                        &self.config,
                        target_profile.as_deref(),
                        desired_proxy,
                        bin,
                        &tools,
                    )
                    .await?,
                );
            }
            let inst = g.as_mut().expect("desktop ready");
            inst.reap_viewer().await;
            let BrowserBackend::Desktop { session, .. } = &inst.backend else {
                unreachable!()
            };
            // Reusing an active viewer must not swallow a concurrent hand-back
            // by advancing the baseline. Only a fresh viewer gets a new one.
            let awaited = if inst.takeover.active {
                inst.takeover.awaited_hand_backs
            } else {
                session.gate().hand_backs()
            };
            let mut cur = None;
            if matches!(session.gate().holder(), Holder::Agent) {
                let a = session.lease(DESKTOP_PRINCIPAL)?;
                if let Some(u) = url.as_deref() {
                    a.navigate(u).await?;
                }
                cur = a.current_url().await.ok();
            }
            session
                .start_takeover(TakeoverConfig {
                    bind: std::env::var("ANYAGENTS_TAKEOVER_BIND")
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .or_else(|| TakeoverConfig::default().bind),
                    public_url: pub_url.clone(),
                    token: None,
                })
                .await?;

            let mut tunnel_error = None;
            if let Some(base) = pub_url {
                session.set_takeover_public_url(base)?;
                if let Some(old) = inst.takeover_tunnel.take() {
                    old.stop().await;
                }
            } else if inst.takeover_tunnel.is_none()
                && let Ok(Some(api_url)) = resolve_api_url(&self.config)
                && let Some(auth) = resolve_coordinator_auth(self.config.agent_token.as_deref())
                && let Some(addr) = session.takeover_addr()
                && let Some(token) = session
                    .takeover_url()
                    .and_then(|u| extract_takeover_token(u.as_str()))
            {
                match TakeoverTunnelHandle::start(TunnelConfig::new(api_url, auth, addr, token))
                    .await
                {
                    Ok(handle) => {
                        if let Ok(base) = Url::parse(handle.public_base_url()) {
                            let _ = session.set_takeover_public_url(base);
                        }
                        inst.takeover_tunnel = Some(handle);
                    }
                    Err(err) => {
                        eprintln!(
                            "browser takeover reverse tunnel unavailable ({err}); falling back to local URL"
                        );
                        tunnel_error = Some(err);
                    }
                }
            }
            inst.takeover
                .mark(TakeoverMode::WayvncNovnc, awaited, req.reason);
            let mut p = desktop_payload(&session.takeover_status(), &inst.takeover, false);
            p["profile"] = json!(inst.active_profile);
            p["url"] = json!(cur);
            p["tunnel_active"] = json!(inst.takeover_tunnel.is_some());
            if let Some(ref tunnel) = inst.takeover_tunnel {
                p["tunnel_session_id"] = json!(tunnel.session_id());
            }
            if let Some(err) = tunnel_error {
                p["tunnel_error"] = json!(err);
            }
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
                inst.detach_takeover(req.reason.or_else(|| Some("release".into())))
                    .await
            };
            let status = if kind == "stop" {
                "stopped"
            } else {
                "released"
            };
            let mut p = json!({"status": status, "active": false, "holder": "agent", "browser_running": inst.is_alive()});
            p[status] = json!(changed);
            return Ok(p);
        }
        match &mut inst.backend {
            BrowserBackend::Desktop { session, .. } => {
                let gate = session.gate();
                let done = gate.hand_backs() > inst.takeover.awaited_hand_backs
                    || (!inst.takeover.active && matches!(gate.holder(), Holder::Agent));
                // Status is observation, not cleanup. If the human released
                // after the pre-tool reap, leave it armed for the next reap.
                let mut st = session.takeover_status();
                if done {
                    st.takeover_url = None;
                }
                let mut p = desktop_payload(&st, &inst.takeover, done);
                p["profile"] = json!(inst.active_profile);
                p["tunnel_active"] = json!(inst.takeover_tunnel.is_some());
                if let Some(ref tunnel) = inst.takeover_tunnel {
                    p["tunnel_session_id"] = json!(tunnel.session_id());
                }
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
