//! `tools.browser`: the session desktop's browser.
//!
//! Every action maps onto one `agent-workstation` operation; this module adds
//! only caller policy: profile selection, snapshot ids for stale-ref checks,
//! secret references, screenshot files, and the response shape.

use std::path::Path;
#[cfg(target_os = "linux")]
use std::path::PathBuf;

use anyagents_core::Error;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[cfg(target_os = "linux")]
use super::ToolError;

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct BrowserRequest {
    /// Browser action: `"open"` / `"goto"` (start the profile's desktop if needed and navigate to `url`),
    /// `"back"` / `"forward"` / `"reload"`,
    /// `"snapshot"` (accessibility outline with `@e1`, `@e2` refs to interactive elements),
    /// `"read"` (visible page text),
    /// `"click"` (click a ref),
    /// `"fill"` / `"type"` (replace a field's value with `text` or a secret reference, optional `submit`),
    /// `"press"` (press `key`, such as `"Escape"` or `"ctrl+l"`, on the desktop keyboard: it reaches
    /// whatever has keyboard focus on the desktop, which after `open` is the address bar),
    /// `"screenshot"` (PNG of the whole desktop, saved to `path` or the profile's screenshots directory),
    /// `"tabs"` (list tabs, or open/switch/close via `tab_action` and `target_id`),
    /// `"takeover"` (return the URL where a person takes over the whole desktop, e.g. to solve a CAPTCHA or 2FA challenge),
    /// `"takeover_status"` / `"wait_for_user"` (inspect, or wait for, a person's hand-back),
    /// `"release_takeover"` (reclaim control from a person),
    /// `"status"` (profile status and fingerprint),
    /// `"profiles"` / `"list"` (all persistent profiles),
    /// or `"close"` / `"stop"` (stop the desktop; logins stay on disk).
    /// `"eval"`, `"cookies"`, `"hold"`, `"key_down"`, `"key_up"`, `"release_keys"` and `full_page`
    /// screenshots return `UNSUPPORTED` in this release.
    #[serde(default)]
    pub action: String,
    /// Persistent browser profile name (e.g. `"default"`, `"research"`, `"shopping"`); defaults to
    /// the calling agent's own profile. Cookies, storage and the fingerprint seed persist per profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    /// URL for `"open"`, `"takeover"` or a new tab (`https://` is assumed without a scheme).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Element ref for `"click"` and `"fill"`: snapshot-qualified (`"@<snapshot_id>:e1"`, rejected
    /// with `STALE_REF` once a newer snapshot or navigation replaced it) or short (`"@e1"`).
    #[serde(
        default,
        alias = "ref",
        alias = "target",
        skip_serializing_if = "Option::is_none"
    )]
    pub selector: Option<String>,
    /// Text for `"fill"` / `"type"`. A value that is itself a secret reference is treated as
    /// `secret_ref`:
    /// - `"op://<vault>/<item>/<field>"` (1Password secret reference)
    /// - `"op-otp://<vault>/<item>"` or `"op://<vault>/<item>/otp?attribute=otp"` (live 1Password TOTP)
    /// - `"secret://<ref>"` (AnyAgents local credential store)
    ///
    /// Secret values are inserted at the moment of the fill and never returned or logged.
    #[serde(default, alias = "value", skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Explicit secret reference (`"op://..."`, `"op-otp://..."`, or `"secret://..."`) for `"fill"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<String>,
    /// Key or chord for `"press"` (e.g. `"Enter"`, `"Tab"`, `"Escape"`, `"ArrowUp"`, `"ctrl+l"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Keys pressed together as one chord, modifiers first (e.g. `["ctrl", "shift", "t"]`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keys: Option<Vec<String>>,
    /// Hold duration in milliseconds (key holding returns `UNSUPPORTED` in this release).
    #[serde(default, alias = "hold", skip_serializing_if = "Option::is_none")]
    pub hold_ms: Option<u64>,
    /// Alias of `hold_ms`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// JavaScript for `"eval"` (returns `UNSUPPORTED` in this release).
    #[serde(
        default,
        alias = "expression",
        alias = "js",
        skip_serializing_if = "Option::is_none"
    )]
    pub script: Option<String>,
    /// Output file for `"screenshot"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Press Enter inside the page after `"fill"` (returns `UNSUPPORTED` in this release; click
    /// the form's submit button ref instead).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submit: Option<bool>,
    /// Capture the full scrollable page (returns `UNSUPPORTED` in this release; screenshots
    /// show the whole desktop).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full_page: Option<bool>,
    /// Extra wait in milliseconds after the action (at most 10 s; for `"wait_for_user"`, the wait
    /// for a hand-back, default 60 s, at most 1 h).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_ms: Option<u64>,
    /// Egress proxy for the profile's desktop: `http://host[:port]`, `socks5://host[:port]`
    /// (credentials allowed: `http://user:pass@host:port`), or `host[:port]` (HTTP).
    /// Omitted: keep a running profile's proxy, otherwise use `ANYAGENTS_BROWSER_PROXY`.
    /// Empty string: direct connection, overriding environment/system proxies.
    /// Close a running profile before changing its proxy; logins stay on disk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy: Option<String>,
    /// Tab sub-action for `"tabs"`: `"list"` (default), `"new"`, `"activate"`, or `"close"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tab_action: Option<String>,
    /// Tab id for `"tabs"` `activate` / `close`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_id: Option<String>,
    /// Cookies to set for `"cookies"` (returns `UNSUPPORTED` in this release).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cookies: Option<Vec<serde_json::Value>>,
    /// Why a person is needed (`"takeover"`; returned for the message you send them), or the
    /// note for `"release_takeover"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl BrowserRequest {
    /// Whether this call carries a secret reference (its errors are redacted).
    pub fn carries_secret(&self) -> bool {
        self.secret_ref.is_some()
            || self
                .text
                .as_deref()
                .is_some_and(super::secrets::is_secret_reference)
    }
}

/// Execute `req` for the calling agent (`default_profile` is its profile).
#[cfg(target_os = "linux")]
pub async fn execute(
    data: &Path,
    default_profile: &str,
    req: BrowserRequest,
) -> Result<serde_json::Value, Error> {
    let redact = req.carries_secret();
    run(data, default_profile, req)
        .await
        .map_err(|e| e.into_error(redact))
}

/// Non-Linux hosts have no session desktop.
#[cfg(not(target_os = "linux"))]
pub async fn execute(
    _data: &Path,
    _default_profile: &str,
    _req: BrowserRequest,
) -> Result<serde_json::Value, Error> {
    Err(super::unsupported_host().into())
}

#[cfg(target_os = "linux")]
fn normalize_url(raw: &str) -> String {
    let raw = raw.trim();
    if raw.contains("://") || raw.starts_with("about:") || raw.starts_with("data:") {
        raw.to_string()
    } else {
        format!("https://{raw}")
    }
}

#[cfg(target_os = "linux")]
fn unsupported_until(what: &str) -> ToolError {
    ToolError::unsupported(format!("{what} is not available in this release"))
}

#[cfg(target_os = "linux")]
async fn pause(ms: Option<u64>, default: u64) {
    let ms = ms.unwrap_or(default).min(10_000);
    if ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }
}

/// Translate browser key names (`ArrowUp`, `PageDown`, `Control+a`) into the
/// desktop keyboard's chord names; anything else passes through.
#[cfg(target_os = "linux")]
pub(crate) fn chord(raw: &str) -> String {
    raw.split('+')
        .map(|part| {
            let p = part.trim();
            match p.to_ascii_lowercase().as_str() {
                "arrowup" => "up".into(),
                "arrowdown" => "down".into(),
                "arrowleft" => "left".into(),
                "arrowright" => "right".into(),
                "pageup" | "prior" => "pageup".into(),
                "pagedown" | "next" => "pagedown".into(),
                "control" | "ctl" => "ctrl".into(),
                "option" => "alt".into(),
                "command" | "win" | "windows" => "super".into(),
                "spacebar" => "space".into(),
                "" if raw.trim() == "+" => "+".into(),
                _ => p.to_string(),
            }
        })
        .collect::<Vec<_>>()
        .join("+")
}

#[cfg(target_os = "linux")]
fn is_modifier(key: &str) -> bool {
    matches!(
        key.trim().to_ascii_lowercase().as_str(),
        "shift" | "ctrl" | "control" | "alt" | "option" | "super" | "meta" | "cmd" | "command"
    )
}

/// The chord a `press` request names.
#[cfg(target_os = "linux")]
fn requested_chord(req: &BrowserRequest) -> Result<String, ToolError> {
    match (req.key.as_deref(), req.keys.as_deref()) {
        (_, Some(keys)) if !keys.is_empty() => {
            let (last, mods) = keys.split_last().expect("non-empty");
            if mods.iter().all(|k| is_modifier(k)) {
                let mut parts: Vec<&str> = mods.iter().map(String::as_str).collect();
                parts.push(last);
                Ok(chord(&parts.join("+")))
            } else {
                Err(unsupported_until(
                    "holding several non-modifier keys together",
                ))
            }
        }
        (Some(k), _) if !k.trim().is_empty() => Ok(chord(k)),
        _ => Err(ToolError::invalid("browser press requires `key`")),
    }
}

/// Resolve a request ref against the session's latest snapshot.
///
/// `@<snapshot_id>:eN` must name the latest snapshot (otherwise `STALE_REF`);
/// `@eN` / `eN` name the latest snapshot implicitly. CSS selectors are not
/// refs.
#[cfg(target_os = "linux")]
fn resolve_ref(raw: &str, current: Option<&str>) -> Result<String, ToolError> {
    let r = raw.trim();
    let body = r.strip_prefix('@').unwrap_or(r);
    let (snapshot, short) = match body.split_once(':') {
        Some((s, e)) => (Some(s), e),
        None => (None, body),
    };
    let is_ref =
        short.len() > 1 && short.starts_with('e') && short[1..].bytes().all(|b| b.is_ascii_digit());
    if !is_ref {
        // P3 pending: DesktopSession::query for CSS selectors.
        return Err(ToolError::unsupported(format!(
            "`{r}` is not a snapshot ref; CSS selectors are not available in this release, so take \
             a snapshot and use its @eN refs"
        )));
    }
    if let Some(id) = snapshot
        && current != Some(id)
    {
        return Err(ToolError::stale_ref(format!(
            "STALE_REF: `{r}` belongs to snapshot `{id}`, which a newer snapshot or navigation replaced"
        )));
    }
    Ok(short.to_string())
}

/// B7 interim: the crate will invalidate refs on every main-frame navigation.
///
/// Resolve `raw` against the latest snapshot, which must still describe the
/// page the tab shows: a navigation the tool did not make (a link, a form
/// submission, a script) leaves the old refs pointing into another document.
#[cfg(target_os = "linux")]
async fn current_ref(live: &super::session::Live, raw: &str) -> Result<String, ToolError> {
    let mark = live.snapshot();
    let short = resolve_ref(raw, mark.as_ref().map(|m| m.id.as_str()))?;
    let Some(mark) = mark else {
        return Err(ToolError::stale_ref(format!(
            "STALE_REF: `{raw}` has no current snapshot; take one first"
        )));
    };
    let url = live.session.current_url().await?;
    if url != mark.url {
        live.set_snapshot(None);
        return Err(ToolError::stale_ref(format!(
            "STALE_REF: the page changed since snapshot `{}` (now {url})",
            mark.id
        )));
    }
    Ok(short)
}

#[cfg(target_os = "linux")]
async fn snapshot_json(
    live: &super::session::Live,
) -> Result<serde_json::Map<String, serde_json::Value>, ToolError> {
    let snap = live.session.snapshot().await?;
    let id = crate::computer::generate_snapshot_id();
    live.set_snapshot(Some(super::session::SnapshotMark {
        id: id.clone(),
        url: snap.url.clone(),
    }));
    let title = active_title(live).await;
    let mut m = serde_json::Map::new();
    m.insert("url".into(), snap.url.into());
    m.insert("title".into(), title.into());
    m.insert("snapshot".into(), snap.text.into());
    m.insert("snapshot_id".into(), id.into());
    m.insert("refs".into(), snap.refs.into());
    Ok(m)
}

#[cfg(target_os = "linux")]
async fn active_title(live: &super::session::Live) -> String {
    live.session
        .tabs()
        .await
        .ok()
        .and_then(|tabs| tabs.into_iter().find(|t| t.active))
        .map(|t| t.title)
        .unwrap_or_default()
}

#[cfg(target_os = "linux")]
fn with_profile(
    profile: &str,
    live: &super::session::Live,
    mut m: serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    m.insert("profile".into(), profile.into());
    m.insert("holder".into(), live.holder().into());
    if let Some(url) = &live.takeover_url {
        m.insert("takeover_url".into(), url.clone().into());
    }
    serde_json::Value::Object(m)
}

#[cfg(target_os = "linux")]
async fn run(
    data: &Path,
    default_profile: &str,
    req: BrowserRequest,
) -> Result<serde_json::Value, ToolError> {
    use super::session;
    use serde_json::json;

    let action = if req.action.trim().is_empty() {
        if req.url.is_some() { "open" } else { "status" }
    } else {
        req.action.trim()
    }
    .to_ascii_lowercase();
    let profile = super::sanitize_profile_name(req.profile.as_deref(), default_profile);
    if req.proxy.is_some()
        && matches!(
            action.as_str(),
            "profiles" | "list" | "close" | "stop" | "shutdown" | "status" | "info" | "fingerprint"
        )
    {
        return Err(ToolError::invalid(
            "proxy applies only to actions that start or use the browser; omit it for status/list/close",
        ));
    }

    match action.as_str() {
        "profiles" | "list" => return Ok(json!({ "profiles": list_profiles(data)? })),
        "close" | "stop" | "shutdown" => {
            let was_running = session::stop(data, &profile).await;
            return Ok(json!({
                "profile": profile,
                "status": "stopped",
                "was_running": was_running,
                "user_data_preserved": true,
            }));
        }
        "status" | "info" | "fingerprint" => return status(data, &profile).await,
        // B2 pending: DesktopSession::evaluate.
        "eval" | "evaluate" | "js" => return Err(unsupported_until("evaluating page scripts")),
        "html" => return Err(unsupported_until("reading page HTML")),
        // B3 pending: DesktopSession::cookies / set_cookies.
        "cookies" => return Err(unsupported_until("reading and setting cookies")),
        // B4 pending: Input::KeyDown / KeyUp.
        "hold" | "key_down" | "keydown" | "key_up" | "keyup" | "release" | "release_keys" => {
            return Err(unsupported_until("holding keys"));
        }
        // B6 interim: page-level Enter needs the crate's in-page key press.
        "fill" | "type" | "input" if req.submit.unwrap_or(false) => {
            return Err(ToolError::unsupported(
                "`submit` (pressing Enter inside the page) is not available in this release; fill, \
                 then click the form's submit button ref",
            ));
        }
        _ => {}
    }

    // A stale qualified ref fails before anything starts or a secret resolves.
    if let Some(sel) = req.selector.as_deref().filter(|s| !s.trim().is_empty()) {
        let current = session::running(data, &profile)
            .and_then(|l| l.snapshot())
            .map(|m| m.id);
        resolve_ref(sel, current.as_deref())?;
    }

    let live = session::acquire(data, &profile, default_profile, req.proxy.as_deref()).await?;
    let s = &live.session;
    let out = match action.as_str() {
        "open" | "goto" | "navigate" => {
            let url = req
                .url
                .as_deref()
                .filter(|u| !u.trim().is_empty())
                .ok_or_else(|| ToolError::invalid("browser open requires `url`"))?;
            live.set_snapshot(None);
            s.navigate(&normalize_url(url)).await?;
            pause(req.wait_ms, 0).await;
            let mut m = snapshot_json(&live).await?;
            m.insert("status".into(), "running".into());
            with_profile(&profile, &live, m)
        }
        "back" | "forward" | "reload" => {
            live.set_snapshot(None);
            let url = match action.as_str() {
                "back" => s.back().await?,
                "forward" => s.forward().await?,
                _ => s.reload().await?,
            };
            pause(req.wait_ms, 0).await;
            with_profile(&profile, &live, json_map(json!({ "url": url })))
        }
        "snapshot" | "dom" => {
            let m = snapshot_json(&live).await?;
            with_profile(&profile, &live, m)
        }
        "read" | "text" | "content" => {
            let text = s.read().await?;
            let url = s.current_url().await?;
            with_profile(
                &profile,
                &live,
                json_map(json!({ "url": url, "text": text })),
            )
        }
        "click" => {
            let raw = req
                .selector
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| {
                    ToolError::invalid("browser click requires `selector` (an @eN ref)")
                })?;
            let r = current_ref(&live, raw).await?;
            s.click(&r).await?;
            pause(req.wait_ms, 300).await;
            let url = s.current_url().await?;
            let title = active_title(&live).await;
            with_profile(
                &profile,
                &live,
                json_map(json!({ "clicked": raw, "url": url, "title": title })),
            )
        }
        "fill" | "type" | "input" => {
            let raw = req
                .selector
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| {
                    ToolError::invalid("browser fill requires `selector` (an @eN ref)")
                })?;
            let r = current_ref(&live, raw).await?;
            let secret = req
                .secret_ref
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .or_else(|| {
                    req.text
                        .as_deref()
                        .filter(|t| super::secrets::is_secret_reference(t))
                        .map(str::trim)
                });
            match secret {
                Some(secret_ref) => {
                    let resolver = super::secrets::SecretRefs::new(data);
                    s.fill_secret(&r, secret_ref, &resolver, None).await?;
                }
                None => s.fill(&r, req.text.as_deref().unwrap_or("")).await?,
            }
            pause(req.wait_ms, 0).await;
            let mut m = json_map(json!({ "filled": raw }));
            if let Some(src) = secret {
                m.insert("secret_source".into(), src.into());
            }
            with_profile(&profile, &live, m)
        }
        "press" | "key" => {
            if req.hold_ms.or(req.duration_ms).unwrap_or(0) > 0 {
                return Err(unsupported_until("holding keys"));
            }
            if req.selector.is_some() {
                return Err(ToolError::unsupported(
                    "pressing a key on a ref is not available in this release; `click` or `fill` \
                     the element first, then `press`",
                ));
            }
            let chord = requested_chord(&req)?;
            s.key(&chord).await?;
            pause(req.wait_ms, 0).await;
            with_profile(&profile, &live, json_map(json!({ "pressed": chord })))
        }
        "screenshot" | "shot" => {
            // P2 pending: DesktopSession::page_screenshot.
            if req.full_page.unwrap_or(false) {
                return Err(unsupported_until("full-page screenshots"));
            }
            let png = s.screenshot().await?.to_png().map_err(ToolError::from)?;
            let path = save_screenshot(data, &profile, req.path.as_deref(), &png)?;
            with_profile(
                &profile,
                &live,
                json_map(json!({
                    "path": path.display().to_string(),
                    "bytes": png.len(),
                    "mime_type": "image/png",
                    "image_base64": crate::computer::base64_encode(&png),
                })),
            )
        }
        "tabs" => {
            let sub = req
                .tab_action
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("list")
                .to_ascii_lowercase();
            let m = match sub.as_str() {
                "list" => json_map(json!({})),
                "new" | "open" => {
                    let url = normalize_url(req.url.as_deref().unwrap_or("about:blank"));
                    live.set_snapshot(None);
                    let id = s.new_tab(&url).await?;
                    json_map(json!({ "target_id": id, "url": url }))
                }
                "activate" | "switch" => {
                    let id = req
                        .target_id
                        .as_deref()
                        .ok_or_else(|| ToolError::invalid("tabs activate requires target_id"))?;
                    live.set_snapshot(None);
                    s.select_tab(id).await?;
                    json_map(json!({ "active_target_id": id }))
                }
                "close" => {
                    let tabs = s.tabs().await?;
                    let id = match req.target_id.as_deref() {
                        Some(id) => id.to_string(),
                        None => tabs
                            .iter()
                            .find(|t| t.active)
                            .map(|t| t.id.clone())
                            .ok_or_else(|| ToolError::invalid("tabs close requires target_id"))?,
                    };
                    live.set_snapshot(None);
                    // B8 interim: activate the next tab first, so closing never leaves the
                    // session choosing among tabs that are still going away.
                    if tabs.iter().any(|t| t.id == id && t.active)
                        && let Some(next) = tabs.iter().find(|t| t.id != id)
                    {
                        s.select_tab(&next.id).await?;
                    }
                    s.close_tab(&id).await?;
                    // B8 interim: the browser lists a closed tab briefly after closing it.
                    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
                    while s.tabs().await?.iter().any(|t| t.id == id)
                        && tokio::time::Instant::now() < deadline
                    {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                    json_map(json!({ "closed_target_id": id }))
                }
                other => {
                    return Err(ToolError::invalid(format!("unknown tab_action `{other}`")));
                }
            };
            let mut m = m;
            m.insert("tabs".into(), json!(s.tabs().await?));
            with_profile(&profile, &live, m)
        }
        "takeover" | "human_takeover" | "user_takeover" => {
            if let Some(url) = req.url.as_deref().filter(|u| !u.trim().is_empty()) {
                live.set_snapshot(None);
                s.navigate(&normalize_url(url)).await?;
            }
            let shot = match s.screenshot().await.and_then(|f| f.to_png()) {
                Ok(png) => save_screenshot(data, &profile, None, &png)
                    .ok()
                    .map(|p| p.display().to_string()),
                Err(_) => None,
            };
            let url = s.current_url().await.unwrap_or_default();
            if live.takeover_url.is_none() {
                return Err(ToolError::unsupported(
                    "this desktop session has no takeover page",
                ));
            }
            let reason = req.reason.as_deref().or(req.text.as_deref());
            let mut m = json_map(json!({
                "status": "takeover_ready",
                "url": url,
                "reason": reason,
                "screenshot_path": shot,
                "instructions": "Send `takeover_url` (and `screenshot_path` via `send` media) to the user. They see this whole desktop, press Take control, work in the browser, and press Hand back. You receive `[Browser Takeover Completed]` when they hand back, or call `browser({ \"action\": \"wait_for_user\", \"wait_ms\": 120000 })`.",
            }));
            if let Some(ms) = req.wait_ms.filter(|ms| *ms > 0) {
                let completed = wait_for_hand_back(&live, ms).await;
                m.insert("completed".into(), completed.into());
                m.insert(
                    "status".into(),
                    if completed {
                        "takeover_completed"
                    } else {
                        "takeover_ready"
                    }
                    .into(),
                );
            }
            with_profile(&profile, &live, m)
        }
        "takeover_status" | "wait_for_user" | "wait_takeover" => {
            let wait = matches!(action.as_str(), "wait_for_user" | "wait_takeover")
                || req.wait_ms.is_some_and(|w| w > 0);
            let completed = if wait {
                wait_for_hand_back(&live, req.wait_ms.unwrap_or(60_000)).await
            } else {
                live.holder() == "agent"
            };
            let mut m = json_map(json!({ "completed": completed }));
            if live.holder() == "agent" {
                m.insert("url".into(), s.current_url().await?.into());
            }
            with_profile(&profile, &live, m)
        }
        "release_takeover" | "end_takeover" | "resume_agent" => {
            let note = req
                .reason
                .as_deref()
                .or(req.text.as_deref())
                .unwrap_or("released by agent");
            let released = s.gate().release(Some(note.to_string()));
            let url = s.current_url().await?;
            with_profile(
                &profile,
                &live,
                json_map(json!({ "released_takeover": released, "url": url })),
            )
        }
        other => {
            return Err(ToolError::invalid(format!(
                "unknown browser action `{other}`; expected `open`, `back`, `forward`, `reload`, \
                 `snapshot`, `read`, `click`, `fill`, `press`, `screenshot`, `tabs`, `takeover`, \
                 `takeover_status`, `wait_for_user`, `release_takeover`, `status`, `profiles`, or `close`"
            )));
        }
    };
    live.touch();
    Ok(out)
}

#[cfg(target_os = "linux")]
fn json_map(v: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    match v {
        serde_json::Value::Object(m) => m,
        _ => serde_json::Map::new(),
    }
}

/// Wait until a person who holds (or takes) control hands back, up to
/// `wait_ms` (at most one hour). `true` once the agent holds control after a
/// person did.
#[cfg(target_os = "linux")]
async fn wait_for_hand_back(live: &super::session::Live, wait_ms: u64) -> bool {
    use agent_workstation::Holder;
    let mut rx = live.session.gate().subscribe();
    let wait = std::time::Duration::from_millis(wait_ms.min(3_600_000));
    let mut saw_human = matches!(*rx.borrow(), Holder::Human { .. });
    let done = tokio::time::timeout(wait, async {
        loop {
            if rx.changed().await.is_err() {
                return false;
            }
            match *rx.borrow_and_update() {
                Holder::Human { .. } => saw_human = true,
                Holder::Agent if saw_human => return true,
                Holder::Agent => {}
            }
        }
    })
    .await;
    live.touch();
    done.unwrap_or(false)
}

#[cfg(target_os = "linux")]
async fn status(data: &Path, profile: &str) -> Result<serde_json::Value, ToolError> {
    use serde_json::json;
    let p = agent_workstation::Profile::new(profile, super::profile_dir(data, profile));
    let fingerprint = agent_workstation::Fingerprint::read(&p.fingerprint_path())?;
    match super::session::running(data, profile) {
        Some(live) => {
            let tabs = live.session.tabs().await.unwrap_or_default();
            Ok(with_profile(
                profile,
                &live,
                json_map(json!({
                    "status": "running",
                    "started_at_ms": live.started_at_ms(),
                    "last_used_at_ms": live.last_used_ms(),
                    "proxy_configured": live.proxy.as_deref().is_some_and(|p| !p.is_empty()),
                    "desktop": { "width": live.session.size().0, "height": live.session.size().1 },
                    "fingerprint": fingerprint,
                    "tabs": tabs,
                })),
            ))
        }
        None => Ok(json!({
            "profile": profile,
            "status": "stopped",
            "fingerprint": fingerprint,
        })),
    }
}

#[cfg(target_os = "linux")]
fn list_profiles(data: &Path) -> Result<Vec<serde_json::Value>, ToolError> {
    use serde_json::json;
    let mut names = std::collections::BTreeSet::new();
    if let Ok(entries) = std::fs::read_dir(super::profiles_root(data)) {
        for entry in entries.flatten() {
            if entry.path().is_dir()
                && let Some(name) = entry.file_name().to_str()
            {
                names.insert(name.to_string());
            }
        }
    }
    Ok(names
        .into_iter()
        .map(|name| match super::session::running(data, &name) {
            Some(live) => json!({
                "profile": name,
                "status": "running",
                "holder": live.holder(),
                "last_used_at_ms": live.last_used_ms(),
            }),
            None => json!({ "profile": name, "status": "stopped" }),
        })
        .collect())
}

/// Write a screenshot to `requested` (or the profile's `screenshots/`). When
/// the requested directory belongs to another uid (an agent seat), the file
/// is copied in as that uid.
#[cfg(target_os = "linux")]
fn save_screenshot(
    data: &Path,
    profile: &str,
    requested: Option<&str>,
    png: &[u8],
) -> Result<PathBuf, ToolError> {
    use std::process::Command;
    let io = |e: std::io::Error| ToolError {
        code: "IO",
        message: format!("cannot save the screenshot: {e}"),
        hint: "Choose a writable `path`.",
        retryable: false,
    };
    let shots_dir = super::profile_dir(data, profile).join("screenshots");
    std::fs::create_dir_all(&shots_dir).map_err(io)?;
    let fallback = shots_dir.join(format!("shot-{}.png", anyagents_core::now_ms()));
    let Some(target) = requested
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
    else {
        std::fs::write(&fallback, png).map_err(io)?;
        return Ok(fallback);
    };
    if let Some(parent) = target.parent()
        && !parent.as_os_str().is_empty()
    {
        let _ = std::fs::create_dir_all(parent);
    }
    if std::fs::write(&target, png).is_ok() {
        return Ok(target);
    }
    std::fs::write(&fallback, png).map_err(io)?;
    let copied_to_seat = {
        use std::os::unix::fs::MetadataExt as _;
        if let Some(parent) = target.parent()
            && let Ok(meta) = std::fs::metadata(parent)
            // SAFETY: geteuid has no preconditions.
            && meta.uid() != unsafe { libc::geteuid() }
            && Path::new("/usr/local/bin/anyagents-setpriv").is_file()
        {
            let uid_s = meta.uid().to_string();
            let gid_s = meta.gid().to_string();
            Command::new("/usr/local/bin/anyagents-setpriv")
                .args([
                    "--reuid",
                    &uid_s,
                    "--regid",
                    &gid_s,
                    "--groups",
                    &gid_s,
                    "--no-new-privs",
                    "--",
                    "/bin/cp",
                    "-f",
                ])
                .arg(&fallback)
                .arg(&target)
                .status()
                .is_ok_and(|s| s.success())
        } else {
            false
        }
    };
    Ok(if copied_to_seat { target } else { fallback })
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn refs_resolve_against_the_latest_snapshot_only() {
        assert_eq!(resolve_ref("@e3", None).unwrap(), "e3");
        assert_eq!(resolve_ref("e12", Some("s1")).unwrap(), "e12");
        assert_eq!(resolve_ref("@s1:e2", Some("s1")).unwrap(), "e2");
        assert_eq!(
            resolve_ref("@s0:e2", Some("s1")).unwrap_err().code,
            "STALE_REF"
        );
        assert_eq!(resolve_ref("@s0:e2", None).unwrap_err().code, "STALE_REF");
        assert_eq!(
            resolve_ref("#email", Some("s1")).unwrap_err().code,
            "UNSUPPORTED"
        );
        assert_eq!(resolve_ref("@e", None).unwrap_err().code, "UNSUPPORTED");
    }

    #[test]
    fn browser_key_names_become_desktop_chords() {
        assert_eq!(chord("ArrowUp"), "up");
        assert_eq!(chord("Control+a"), "ctrl+a");
        assert_eq!(chord("Enter"), "Enter");
        assert_eq!(chord("PageDown"), "pagedown");
        let req = |keys: &[&str]| BrowserRequest {
            keys: Some(keys.iter().map(|k| k.to_string()).collect()),
            ..Default::default()
        };
        assert_eq!(
            requested_chord(&req(&["ctrl", "shift", "t"])).unwrap(),
            "ctrl+shift+t"
        );
        assert_eq!(
            requested_chord(&req(&["w", "d"])).unwrap_err().code,
            "UNSUPPORTED"
        );
    }

    #[test]
    fn secret_calls_are_detected_from_text_or_secret_ref() {
        let mut req = BrowserRequest {
            text: Some(" op://vault/item/password".into()),
            ..Default::default()
        };
        assert!(req.carries_secret());
        req.text = Some("hello".into());
        assert!(!req.carries_secret());
        req.secret_ref = Some("secret://k".into());
        assert!(req.carries_secret());
    }

    #[tokio::test]
    async fn pending_capabilities_are_typed_unsupported_without_starting_a_desktop() {
        let dir = tempfile::tempdir().unwrap();
        for action in [
            "eval",
            "cookies",
            "hold",
            "key_down",
            "release_keys",
            "html",
        ] {
            let req = BrowserRequest {
                action: action.into(),
                ..Default::default()
            };
            let err = execute(dir.path(), "agent-a", req)
                .await
                .unwrap_err()
                .to_string();
            assert!(err.contains("\"code\":\"UNSUPPORTED\""), "{action}: {err}");
        }
        assert!(super::super::session::running(dir.path(), "agent-a").is_none());
        let err = execute(
            dir.path(),
            "agent-a",
            BrowserRequest {
                action: "click".into(),
                selector: Some("@s0:e1".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("STALE_REF"), "{err}");
    }
}

#[cfg(all(test, not(target_os = "linux")))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn non_linux_hosts_answer_unsupported_naming_the_routing_setting() {
        let dir = tempfile::tempdir().unwrap();
        let err = execute(
            dir.path(),
            "agent-a",
            BrowserRequest {
                action: "open".into(),
                url: Some("https://example.com".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("\"code\":\"UNSUPPORTED\""), "{err}");
        assert!(err.contains(super::super::REMOTE_WORKSTATION_ENV), "{err}");
    }
}
