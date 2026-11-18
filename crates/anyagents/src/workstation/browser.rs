//! `tools.browser`: the session desktop's browser.
//!
//! Every action maps onto `agent-workstation` operations on the calling
//! agent's lease of the profile's desktop; this module adds only caller
//! policy: profile selection, secret references, screenshot files, response
//! size limits, and the response shape.

use std::path::Path;
#[cfg(target_os = "linux")]
use std::path::PathBuf;

use anyagents_core::Error;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::Caller;
#[cfg(target_os = "linux")]
use super::ToolError;

/// Largest script result or HTML returned to the model, in bytes.
#[cfg(target_os = "linux")]
const MAX_TEXT_RESULT: usize = 256 * 1024;

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct BrowserRequest {
    /// Browser action: `"open"` / `"goto"` (start the profile's desktop if needed and navigate to `url`),
    /// `"back"` / `"forward"` / `"reload"`,
    /// `"snapshot"` (accessibility outline with `@e1`, `@e2` refs to interactive elements),
    /// `"read"` (visible page text), `"html"` (outer HTML of `selector`, or of the page),
    /// `"click"` (click a ref or CSS selector),
    /// `"fill"` / `"type"` (replace a field's value with `text` or a secret reference, optional `submit`),
    /// `"press"` (press `key`, such as `"Enter"` or `"ctrl+a"`, inside the page: it reaches the page's
    /// focused element, or `selector` focused first; optional `hold_ms`),
    /// `"hold"` / `"key_down"` / `"key_up"` / `"release_keys"` (hold page keys; held modifiers apply to clicks),
    /// `"eval"` (evaluate `script` in the page and return its JSON value),
    /// `"cookies"` (set `cookies`, then list the page's cookies),
    /// `"screenshot"` (PNG of the whole desktop, or of the page with `full_page`, saved to `path` or the
    /// profile's screenshots directory),
    /// `"tabs"` (list tabs, or open/switch/close via `tab_action` and `target_id`),
    /// `"takeover"` (return the URL where a person takes over the whole desktop, e.g. to solve a CAPTCHA or 2FA challenge),
    /// `"takeover_status"` / `"wait_for_user"` (inspect, or wait for, a person's hand-back),
    /// `"release_takeover"` (reclaim control from a person),
    /// `"status"` (profile status and fingerprint),
    /// `"profiles"` / `"list"` (all persistent profiles),
    /// or `"close"` / `"stop"` (stop the desktop; logins stay on disk).
    /// One agent at a time holds a profile's desktop; another agent gets `DESKTOP_IN_USE`.
    #[serde(default)]
    pub action: String,
    /// Persistent browser profile name (e.g. `"default"`, `"research"`, `"shopping"`); defaults to
    /// the calling agent's own profile. Cookies, storage and the fingerprint seed persist per profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    /// URL for `"open"`, `"takeover"` or a new tab (`https://` is assumed without a scheme).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Element for `"click"`, `"fill"`, `"press"` and `"html"`: a snapshot ref (`"@e1"`, or
    /// `"@<snapshot_id>:e1"`) or a CSS selector (`"#email"`). Refs fail with `STALE_REF` once a
    /// newer snapshot or any navigation of their page replaced them.
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
    /// Key or chord for `"press"`, `"hold"`, `"key_down"` and `"key_up"` (e.g. `"Enter"`, `"Tab"`,
    /// `"ArrowUp"`, `"shift"`, `"ctrl+a"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Keys held together, modifiers first (e.g. `["ctrl", "shift", "t"]`, or `["w", "d"]` with
    /// `hold_ms`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keys: Option<Vec<String>>,
    /// Hold duration in milliseconds for `"press"` / `"hold"` (at most 10 s).
    #[serde(default, alias = "hold", skip_serializing_if = "Option::is_none")]
    pub hold_ms: Option<u64>,
    /// Alias of `hold_ms`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// JavaScript expression for `"eval"` (a promise is awaited).
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
    /// Press Enter inside the field after `"fill"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submit: Option<bool>,
    /// Capture the full scrollable page for `"screenshot"` (the page only, without browser chrome).
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
    /// Append `,direct://` to a proxy URL to fall back to a direct connection if it fails.
    /// Close a running profile before changing its proxy; logins stay on disk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy: Option<String>,
    /// Tab sub-action for `"tabs"`: `"list"` (default), `"new"`, `"activate"`, or `"close"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tab_action: Option<String>,
    /// Target tab id. When supplied, the action operates only on that live
    /// CDP target and fails closed with `STALE_TARGET` if the tab does not
    /// exist (never falling back to `about:blank` or another tab).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_id: Option<String>,
    /// Cookies to set for `"cookies"`, in DevTools fields: `name`, `value`, and `url` or `domain`
    /// (optional `path`, `expires`, `httpOnly`, `secure`, `sameSite`).
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

fn request_mutates(req: &BrowserRequest) -> bool {
    let action = req.action.trim().to_ascii_lowercase();
    match action.as_str() {
        "" => req.url.is_some(),
        "snapshot" | "dom" | "read" | "text" | "content" | "html" | "screenshot" | "shot"
        | "status" | "info" | "fingerprint" | "profiles" | "list" | "takeover_status"
        | "wait_for_user" => false,
        "cookies" => req.cookies.is_some(),
        "tabs" => req
            .tab_action
            .as_deref()
            .is_some_and(|a| !matches!(a.trim(), "" | "list")),
        _ => true,
    }
}

/// Execute `req` for `caller`.
#[cfg(target_os = "linux")]
pub async fn execute(
    data: &Path,
    caller: &Caller,
    req: BrowserRequest,
    files: &crate::agent_file::FileAuthority,
) -> Result<serde_json::Value, Error> {
    let redact = req.carries_secret();
    let mutating = request_mutates(&req);
    run(data, caller, req, files)
        .await
        .map_err(|e| e.into_action_error(redact, mutating))
}

/// Non-Linux hosts have no session desktop.
#[cfg(not(target_os = "linux"))]
pub async fn execute(
    _data: &Path,
    _caller: &Caller,
    _req: BrowserRequest,
    _files: &crate::agent_file::FileAuthority,
) -> Result<serde_json::Value, Error> {
    Err(super::unsupported_host().into())
}

#[cfg(target_os = "linux")]
fn navigable_url(raw: &str) -> Result<String, ToolError> {
    // What Chromium's URL parser drops before it reads a scheme: leading and
    // trailing C0 controls and spaces, and every tab or newline anywhere.
    let url: String = raw
        .trim_matches(|c: char| c <= ' ')
        .chars()
        .filter(|c| !matches!(c, '\t' | '\n' | '\r'))
        .collect();
    if url.is_empty() {
        return Err(ToolError::invalid("browser navigation requires a URL"));
    }
    if url.eq_ignore_ascii_case("about:blank") {
        return Ok("about:blank".to_string());
    }
    match url_scheme(&url) {
        // `example.com` or `localhost:8080/x`: a host, over https.
        None => Ok(format!("https://{url}")),
        Some(scheme) if scheme == "http" || scheme == "https" => Ok(url),
        Some(scheme) => Err(ToolError::invalid(format!(
            "browser navigation allows only http://, https:// and about:blank, not `{scheme}:`"
        ))),
    }
}

/// The lowercased scheme `url` names, if it names one: the letters before
/// its first `:` when they form a scheme and what follows is not a port.
#[cfg(target_os = "linux")]
fn url_scheme(url: &str) -> Option<String> {
    let (scheme, rest) = url.split_once(':')?;
    let is_scheme = scheme.starts_with(|c: char| c.is_ascii_alphabetic())
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'));
    let after_port = rest.trim_start_matches(|c: char| c.is_ascii_digit());
    let is_port = (scheme.eq_ignore_ascii_case("localhost") || scheme.contains('.'))
        && after_port.len() < rest.len()
        && after_port
            .chars()
            .next()
            .is_none_or(|c| matches!(c, '/' | '?' | '#'));
    (is_scheme && !is_port).then(|| scheme.to_ascii_lowercase())
}

/// Whether a page now showing `url` may be read or driven for a caller:
/// what [`navigable_url`] admits, plus Chromium's own error page for a
/// navigation that failed.
#[cfg(target_os = "linux")]
fn readable_page_url(url: &str) -> bool {
    let url = url.trim();
    url.is_empty()
        || url.eq_ignore_ascii_case("about:blank")
        || url
            .to_ascii_lowercase()
            .starts_with("chrome-error://chromewebdata/")
        || matches!(url_scheme(url).as_deref(), Some("http" | "https"))
}

/// An explicit wait the agent asked for (never a default delay).
#[cfg(target_os = "linux")]
async fn pause(ms: Option<u64>) {
    let ms = ms.unwrap_or(0).min(10_000);
    if ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }
}

#[cfg(target_os = "linux")]
fn is_modifier(key: &str) -> bool {
    matches!(
        key.trim().to_ascii_lowercase().as_str(),
        "shift"
            | "ctrl"
            | "control"
            | "alt"
            | "option"
            | "super"
            | "meta"
            | "cmd"
            | "command"
            | "win"
            | "windows"
    )
}

/// The keys a key request names: one chord (`key`, or `keys` that are
/// modifiers plus one key), or several keys held together (`keys` with more
/// than one non-modifier).
#[cfg(target_os = "linux")]
fn requested_keys(req: &BrowserRequest) -> Result<Vec<String>, ToolError> {
    match (req.key.as_deref(), req.keys.as_deref()) {
        (_, Some(keys)) if !keys.is_empty() => {
            let (last, mods) = keys.split_last().expect("non-empty");
            if mods.iter().all(|k| is_modifier(k)) {
                let mut parts: Vec<&str> = mods.iter().map(String::as_str).collect();
                parts.push(last);
                Ok(vec![parts.join("+")])
            } else {
                Ok(keys.to_vec())
            }
        }
        (Some(k), _) if !k.trim().is_empty() => Ok(vec![k.trim().to_string()]),
        _ => Err(ToolError::invalid("this browser key action requires `key`")),
    }
}

/// Cut text returned to the model at `MAX_TEXT_RESULT` bytes.
#[cfg(target_os = "linux")]
fn bounded(mut text: String) -> (String, bool) {
    if text.len() <= MAX_TEXT_RESULT {
        return (text, false);
    }
    let mut end = MAX_TEXT_RESULT;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    (text, true)
}

/// The smallest box, in CSS pixels per side, a human can see and act on as a
/// challenge. A background widget mounted at 0x0 or 1x1 never reaches it; the
/// smallest real challenge checkbox does.
#[cfg(target_os = "linux")]
const CAPTCHA_MIN_SIDE_PX: u32 = 40;

/// `(el, min, box?) => bool`: whether `el` is rendered to a human with at
/// least `min` CSS pixels per side. `box` replaces `el`'s own rectangle (a
/// text node's range). Nothing on the way to the screen may hide it:
/// `display: none`, `visibility`, transparency, `content-visibility`
/// (`hidden="until-found"`, a closed `<details>`) on it or any ancestor, in
/// its document and in every same-origin frame around it. Its rectangle,
/// clipped by every box that clips it (`overflow`, `clip`, each frame), by
/// the scrollable page, and by the viewport where the page cannot scroll or
/// the content is fixed, must keep the size.
#[cfg(target_os = "linux")]
const CAPTCHA_VISIBLE_JS: &str = r#"(el, min, box) => {
  if (!el || !el.isConnected) return false;
  const r = box || el.getBoundingClientRect();
  let left = r.left, top = r.top, right = r.right, bottom = r.bottom;
  const clipTo = (l, t, rr, b) => {
    left = Math.max(left, l); top = Math.max(top, t);
    right = Math.min(right, rr); bottom = Math.min(bottom, b);
  };
  const parentOf = (n) => n.parentElement || (n.parentNode && n.parentNode.host) || null;
  const hides = (overflow) => overflow === 'hidden' || overflow === 'clip';
  const tokens = (v) => String(v || '').split(/[\s,]+/);
  // Whether a box is the containing block of fixed descendants.
  const holdsFixed = (s) =>
    s.transform !== 'none' || s.filter !== 'none' || (s.backdropFilter || 'none') !== 'none' ||
    s.perspective !== 'none' ||
    tokens(s.contain).some(v => ['paint', 'layout', 'strict', 'content'].includes(v)) ||
    tokens(s.willChange).some(v => ['transform', 'filter', 'perspective'].includes(v));
  for (let start = el; ; ) {
    const doc = start.ownerDocument;
    const view = doc.defaultView;
    if (typeof start.checkVisibility === 'function' &&
        !start.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true })) return false;
    const html = doc.documentElement;
    const rootStyle = view.getComputedStyle(html);
    // The body's overflow belongs to the viewport when the root's is visible.
    const viewportBody = doc.body && rootStyle.overflowX === 'visible' && rootStyle.overflowY === 'visible' ? doc.body : null;
    const viewportStyle = viewportBody ? view.getComputedStyle(viewportBody) : rootStyle;
    let pos = null;
    for (let n = start; n; n = parentOf(n)) {
      const s = view.getComputedStyle(n);
      if (s.display === 'none') return false;
      if (n === start && (s.visibility === 'hidden' || s.visibility === 'collapse')) return false;
      if (parseFloat(s.opacity) <= 0.01) return false;
      if (n !== start) {
        const clips = pos === 'fixed' ? holdsFixed(s)
          : pos === 'absolute' ? (s.position !== 'static' || holdsFixed(s))
          : true;
        if (!clips) continue;
      }
      pos = s.position;
      if (n === html || n === viewportBody) continue;
      const c = n.getBoundingClientRect();
      if (n !== start && (s.overflowX !== 'visible' || s.overflowY !== 'visible')) {
        const l = c.left + n.clientLeft, t = c.top + n.clientTop;
        clipTo(
          s.overflowX !== 'visible' ? l : -Infinity,
          s.overflowY !== 'visible' ? t : -Infinity,
          s.overflowX !== 'visible' ? l + n.clientWidth : Infinity,
          s.overflowY !== 'visible' ? t + n.clientHeight : Infinity,
        );
      }
      if ((s.position === 'absolute' || s.position === 'fixed') && s.clip.startsWith('rect(')) {
        const v = s.clip.slice(5, -1).split(/[\s,]+/);
        const at = (i, base, edge) => v[i] === 'auto' ? edge : base + parseFloat(v[i]);
        clipTo(at(3, c.left, c.left), at(0, c.top, c.top), at(1, c.left, c.right), at(2, c.top, c.bottom));
      }
    }
    if (pos === 'fixed') {
      clipTo(0, 0, view.innerWidth, view.innerHeight);
    } else {
      const lockX = hides(viewportStyle.overflowX), lockY = hides(viewportStyle.overflowY);
      clipTo(
        lockX ? 0 : -view.scrollX,
        lockY ? 0 : -view.scrollY,
        lockX ? view.innerWidth : Math.max(view.innerWidth, html.scrollWidth) - view.scrollX,
        lockY ? view.innerHeight : Math.max(view.innerHeight, html.scrollHeight) - view.scrollY,
      );
    }
    // A cross-origin parent hides its frame element; the caller checks it.
    let frame = null;
    try { frame = view.frameElement; } catch (_) { frame = null; }
    if (!frame) break;
    const fr = frame.getBoundingClientRect();
    const fs = frame.ownerDocument.defaultView.getComputedStyle(frame);
    // A transformed frame scales everything inside it.
    const sx = frame.offsetWidth ? fr.width / frame.offsetWidth : 0;
    const sy = frame.offsetHeight ? fr.height / frame.offsetHeight : 0;
    const ox = fr.left + (frame.clientLeft + parseFloat(fs.paddingLeft)) * sx;
    const oy = fr.top + (frame.clientTop + parseFloat(fs.paddingTop)) * sy;
    left = ox + left * sx; right = ox + right * sx;
    top = oy + top * sy; bottom = oy + bottom * sy;
    clipTo(ox, oy, ox + view.innerWidth * sx, oy + view.innerHeight * sy);
    start = frame;
  }
  return right - left >= min && bottom - top >= min;
}"#;

/// `(visible, min) => detection`, applied to [`CAPTCHA_VISIBLE_JS`]: a challenge
/// counts only when a human can see it. A widget, container or challenge
/// iframe matches when it is visible at [`CAPTCHA_MIN_SIDE_PX`]; challenge
/// wording matches only in rendered text.
#[cfg(target_os = "linux")]
const DETECT_CAPTCHA_JS: &str = r#"((visible, MIN) => {
  const shown = (el) => !el.closest('.grecaptcha-badge') && visible(el, MIN);
  const hasSel = (s) => {
    try { return Array.from(document.querySelectorAll(s)).some(shown); } catch (_) { return false; }
  };
  const iframes = Array.from(document.querySelectorAll('iframe'));
  const hasFrame = (pred) => iframes.some(f => pred((f.src || f.getAttribute('src') || '').toLowerCase()) && shown(f));
  const renderedText = () => {
    if (!document.body) return '';
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    const range = document.createRange();
    const parts = [];
    let len = 0;
    for (let n = walker.nextNode(), seen = 0; n && seen < 5000 && len < 20000; n = walker.nextNode(), seen++) {
      const t = n.nodeValue.trim();
      if (!t || !n.parentElement) continue;
      range.selectNodeContents(n);
      if (!visible(n.parentElement, 1, range.getBoundingClientRect())) continue;
      parts.push(t);
      len += t.length + 1;
    }
    return parts.join(' ').toLowerCase();
  };
  const title = (document.title || '').toLowerCase();
  let text = null;
  const bodyText = () => (text === null ? (text = renderedText()) : text);
  if (
    hasSel('#cf-chl-widget, .cf-turnstile, #challenge-running, #challenge-stage') ||
    hasFrame(u => u.includes('challenges.cloudflare.com') || u.includes('turnstile')) ||
    (title.includes('just a moment') && (bodyText().includes('verify you are human') || bodyText().includes('cloudflare'))) ||
    title.includes('attention required! | cloudflare')
  ) {
    return { detected: true, kind: 'cloudflare_turnstile', summary: 'Cloudflare Turnstile / human verification challenge' };
  }
  if (
    hasSel('.g-recaptcha, #g-recaptcha') ||
    hasFrame(u => u.includes('google.com/recaptcha') || u.includes('recaptcha/api2') || u.includes('recaptcha/enterprise'))
  ) {
    return { detected: true, kind: 'recaptcha', summary: 'Google reCAPTCHA verification challenge' };
  }
  if (hasSel('.h-captcha') || hasFrame(u => u.includes('hcaptcha.com'))) {
    return { detected: true, kind: 'hcaptcha', summary: 'hCaptcha verification challenge' };
  }
  if (hasSel('#arkose, [data-arkose]') || hasFrame(u => u.includes('arkoselabs.com') || u.includes('funcaptcha.com'))) {
    return { detected: true, kind: 'arkose_funcaptcha', summary: 'Arkose Labs / FunCaptcha challenge' };
  }
  if (hasSel('.geetest_panel, .geetest_holder, .nc_scale, #nc_1_n1z')) {
    return { detected: true, kind: 'slider_captcha', summary: 'Slider / Geetest verification challenge' };
  }
  if (
    hasSel('[data-captcha], #captcha-box') ||
    title === 'robot check' ||
    title === 'security check' ||
    (bodyText().includes('verify you are human') && bodyText().length < 1500)
  ) {
    return { detected: true, kind: 'generic_captcha', summary: 'Human verification / CAPTCHA challenge' };
  }
  return { detected: false };
})"#;

/// The page expression that runs [`DETECT_CAPTCHA_JS`].
#[cfg(target_os = "linux")]
fn detect_captcha_expression() -> String {
    format!("{DETECT_CAPTCHA_JS}({CAPTCHA_VISIBLE_JS}, {CAPTCHA_MIN_SIDE_PX})")
}

/// Project a successful detection into the caller response. An unavailable or
/// malformed evaluation is an observation failure, not a failed snapshot.
#[cfg(target_os = "linux")]
fn captcha_detected_flag(detection: Option<&serde_json::Value>) -> bool {
    detection
        .and_then(|v| v.get("detected"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

#[cfg(target_os = "linux")]
fn insert_captcha_fields(
    m: &mut serde_json::Map<String, serde_json::Value>,
    detection: Option<&serde_json::Value>,
) {
    let detected = captcha_detected_flag(detection);
    m.insert("captcha_detected".into(), detected.into());
    if detected {
        for (source, target) in [("kind", "captcha_kind"), ("summary", "captcha_summary")] {
            if let Some(value) = detection
                .and_then(|v| v.get(source))
                .and_then(serde_json::Value::as_str)
            {
                m.insert(target.into(), value.into());
            }
        }
    }
}

#[cfg(target_os = "linux")]
trait BrowserClient {
    async fn current_url(&self) -> Result<String, ToolError>;
    async fn tabs(&self) -> Result<Vec<agent_workstation::Tab>, ToolError>;
    async fn new_tab(&self, url: &str) -> Result<String, ToolError>;
    async fn select_tab(&self, id: &str) -> Result<(), ToolError>;
    async fn close_tab(&self, id: &str) -> Result<(), ToolError>;
    async fn navigate(&self, url: &str) -> Result<String, ToolError>;
    async fn back(&self) -> Result<String, ToolError>;
    async fn forward(&self) -> Result<String, ToolError>;
    async fn reload(&self) -> Result<String, ToolError>;
    async fn snapshot(&self) -> Result<agent_workstation::Snapshot, ToolError>;
    async fn read(&self) -> Result<String, ToolError>;
    async fn html(&self, target: Option<&str>) -> Result<String, ToolError>;
    async fn eval(&self, script: &str) -> Result<serde_json::Value, ToolError>;
    async fn cookies(&self, urls: &[String]) -> Result<Vec<agent_workstation::Cookie>, ToolError>;
    async fn set_cookies(&self, cookies: &[agent_workstation::Cookie]) -> Result<(), ToolError>;
    async fn click(&self, target: &str) -> Result<(), ToolError>;
    async fn fill(&self, target: &str, text: &str) -> Result<(), ToolError>;
    async fn fill_secret(&self, target: &str, secret_ref: &str) -> Result<(), ToolError>;
    async fn focus(&self, target: &str) -> Result<(), ToolError>;
    async fn press(&self, chord: &str) -> Result<(), ToolError>;
    async fn press_and_hold(&self, chord: &str, hold: std::time::Duration)
    -> Result<(), ToolError>;
    async fn key_down(&self, chord: &str) -> Result<(), ToolError>;
    async fn key_up(&self, chord: &str) -> Result<(), ToolError>;
    async fn release_keys(&self) -> Result<(), ToolError>;
    async fn held_keys(&self) -> Result<Vec<String>, ToolError>;
    async fn page_screenshot(
        &self,
        full_page: bool,
    ) -> Result<(Vec<u8>, u32, u32, bool), ToolError>;
    fn holder(&self) -> &'static str;
    fn takeover_url(&self) -> Option<String>;
    async fn publish_takeover(&self);
    fn expect_hand_back(&self, challenge_detected: bool);
    async fn wait_for_hand_back(&self, wait: std::time::Duration) -> bool;
    fn release_takeover(&self, note: String) -> bool;
}

#[cfg(target_os = "linux")]
struct LiveBrowserClient<'a> {
    agent: agent_workstation::Agent<'a>,
    live: &'a super::session::Live,
    data: &'a Path,
}

#[cfg(target_os = "linux")]
impl BrowserClient for LiveBrowserClient<'_> {
    async fn current_url(&self) -> Result<String, ToolError> {
        Ok(self.agent.current_url().await?)
    }
    async fn tabs(&self) -> Result<Vec<agent_workstation::Tab>, ToolError> {
        Ok(self.agent.tabs().await?)
    }
    async fn new_tab(&self, url: &str) -> Result<String, ToolError> {
        Ok(self.agent.new_tab(url).await?)
    }
    async fn select_tab(&self, id: &str) -> Result<(), ToolError> {
        Ok(self.agent.select_tab(id).await?)
    }
    async fn close_tab(&self, id: &str) -> Result<(), ToolError> {
        Ok(self.agent.close_tab(id).await?)
    }
    async fn navigate(&self, url: &str) -> Result<String, ToolError> {
        Ok(self.agent.navigate(url).await?)
    }
    async fn back(&self) -> Result<String, ToolError> {
        Ok(self.agent.back().await?)
    }
    async fn forward(&self) -> Result<String, ToolError> {
        Ok(self.agent.forward().await?)
    }
    async fn reload(&self) -> Result<String, ToolError> {
        Ok(self.agent.reload().await?)
    }
    async fn snapshot(&self) -> Result<agent_workstation::Snapshot, ToolError> {
        Ok(self.agent.snapshot().await?)
    }
    async fn read(&self) -> Result<String, ToolError> {
        Ok(self.agent.read().await?)
    }
    async fn html(&self, target: Option<&str>) -> Result<String, ToolError> {
        Ok(self.agent.html(target).await?)
    }
    async fn eval(&self, script: &str) -> Result<serde_json::Value, ToolError> {
        Ok(self.agent.eval(script).await?)
    }
    async fn cookies(&self, urls: &[String]) -> Result<Vec<agent_workstation::Cookie>, ToolError> {
        Ok(self.agent.cookies(urls).await?)
    }
    async fn set_cookies(&self, cookies: &[agent_workstation::Cookie]) -> Result<(), ToolError> {
        Ok(self.agent.set_cookies(cookies).await?)
    }
    async fn click(&self, target: &str) -> Result<(), ToolError> {
        Ok(self.agent.click(target).await?)
    }
    async fn fill(&self, target: &str, text: &str) -> Result<(), ToolError> {
        Ok(self.agent.fill(target, text).await?)
    }
    async fn fill_secret(&self, target: &str, secret_ref: &str) -> Result<(), ToolError> {
        let resolver = super::secrets::SecretRefs::new(self.data);
        Ok(self
            .agent
            .fill_secret(target, secret_ref, &resolver, None)
            .await?)
    }
    async fn focus(&self, target: &str) -> Result<(), ToolError> {
        Ok(self.agent.focus(target).await?)
    }
    async fn press(&self, chord: &str) -> Result<(), ToolError> {
        Ok(self.agent.press(chord).await?)
    }
    async fn press_and_hold(
        &self,
        chord: &str,
        hold: std::time::Duration,
    ) -> Result<(), ToolError> {
        Ok(self.agent.press_and_hold(chord, hold).await?)
    }
    async fn key_down(&self, chord: &str) -> Result<(), ToolError> {
        Ok(self.agent.key_down(chord).await?)
    }
    async fn key_up(&self, chord: &str) -> Result<(), ToolError> {
        Ok(self.agent.key_up(chord).await?)
    }
    async fn release_keys(&self) -> Result<(), ToolError> {
        Ok(self.agent.release_keys().await?)
    }
    async fn held_keys(&self) -> Result<Vec<String>, ToolError> {
        Ok(self.agent.held_keys().await?)
    }
    async fn page_screenshot(
        &self,
        full_page: bool,
    ) -> Result<(Vec<u8>, u32, u32, bool), ToolError> {
        if full_page {
            let page = self.agent.page_screenshot(true).await?;
            Ok((page.png, page.width, page.height, page.truncated))
        } else {
            let frame = self.agent.screenshot().await?;
            Ok((frame.to_png()?, frame.width, frame.height, false))
        }
    }
    fn holder(&self) -> &'static str {
        self.live.holder()
    }
    fn takeover_url(&self) -> Option<String> {
        self.live.takeover_url()
    }
    async fn publish_takeover(&self) {
        self.live.publish_takeover().await;
    }
    fn expect_hand_back(&self, challenge_detected: bool) {
        self.live.expect_hand_back(challenge_detected);
    }
    async fn wait_for_hand_back(&self, wait: std::time::Duration) -> bool {
        self.live.wait_for_hand_back(wait).await
    }
    fn release_takeover(&self, note: String) -> bool {
        self.live.session.gate().release(Some(note))
    }
}

#[cfg(target_os = "linux")]
fn live_target_summary(tabs: &[agent_workstation::Tab]) -> String {
    if tabs.is_empty() {
        return "none".into();
    }
    tabs.iter()
        .take(5)
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
}

/// Fail closed with `STALE_TARGET` whenever an explicit `target_id` is absent
/// from the live CDP target list; never silently fall back to another tab.
#[cfg(target_os = "linux")]
async fn resolve_explicit_target<C: BrowserClient>(
    client: &C,
    profile: &str,
    target_id: Option<&str>,
    activate: bool,
) -> Result<Option<String>, ToolError> {
    let Some(id) = target_id else {
        return Ok(None);
    };
    let tabs = client.tabs().await?;
    let Some(tab) = tabs.iter().find(|t| t.id == id) else {
        return Err(ToolError::stale_target(format!(
            "browser target `{id}` does not exist in profile `{profile}` (live targets: {})",
            live_target_summary(&tabs),
        )));
    };
    if activate
        && !tab.active
        && let Err(err) = client.select_tab(&tab.id).await
    {
        if let Ok(after) = client.tabs().await
            && !after.iter().any(|t| t.id == id)
        {
            return Err(ToolError::stale_target(format!(
                "browser target `{id}` disappeared in profile `{profile}` (live targets: {})",
                live_target_summary(&after),
            )));
        }
        return Err(err);
    }
    Ok(Some(tab.id.clone()))
}

#[cfg(target_os = "linux")]
async fn require_readable_page<C: BrowserClient>(s: &C) -> Result<(), ToolError> {
    let url = s.current_url().await?;
    if !readable_page_url(&url) {
        return Err(ToolError::invalid(
            "browser page access allows only http://, https:// and about:blank; the active page was left unchanged",
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
async fn snapshot_json<C: BrowserClient>(
    s: &C,
) -> Result<serde_json::Map<String, serde_json::Value>, ToolError> {
    require_readable_page(s).await?;
    let snap = s.snapshot().await?;
    let (target_id, title) = active_tab_info(s).await;
    let mut m = serde_json::Map::new();
    m.insert("url".into(), snap.url.into());
    m.insert("title".into(), title.into());
    if let Some(id) = target_id {
        m.insert("target_id".into(), id.into());
    }
    m.insert("snapshot".into(), snap.text.into());
    m.insert("snapshot_id".into(), snap.id.into());
    m.insert("refs".into(), snap.refs.into());
    let captcha = s.eval(&detect_captcha_expression()).await.ok();
    insert_captcha_fields(&mut m, captcha.as_ref());
    Ok(m)
}

#[cfg(target_os = "linux")]
async fn active_tab_info<C: BrowserClient>(s: &C) -> (Option<String>, String) {
    s.tabs()
        .await
        .ok()
        .and_then(|tabs| tabs.into_iter().find(|t| t.active))
        .map(|t| ((!t.id.is_empty()).then_some(t.id), t.title))
        .unwrap_or((None, String::new()))
}

#[cfg(target_os = "linux")]
fn with_profile(
    profile: &str,
    holder: &str,
    action: &str,
    takeover_url: impl FnOnce() -> Option<String>,
    mut m: serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    m.insert("profile".into(), profile.into());
    m.insert("holder".into(), holder.into());
    let captcha_detected = m
        .get("captcha_detected")
        .and_then(serde_json::Value::as_bool)
        == Some(true);
    if captcha_detected {
        let request = serde_json::json!({ "action": "takeover", "profile": profile, "reason": "solve CAPTCHA" });
        m.insert("takeover_hint".into(), format!(
            "Human verification / CAPTCHA detected. Call `browser({request})` and send `takeover_url` (and `screenshot_path`) to the user."
        ).into());
    }
    // The takeover link is a credential, not ordinary session metadata.
    m.remove("takeover_url");
    if (matches!(action, "takeover" | "human_takeover" | "user_takeover") || captcha_detected)
        && let Some(url) = takeover_url()
    {
        m.insert("takeover_url".into(), url.into());
    }
    serde_json::Value::Object(m)
}

/// A cookie as the model sees it: an `HttpOnly` cookie's value is a
/// credential the page itself cannot read, so it is withheld.
#[cfg(target_os = "linux")]
fn cookie_json(c: agent_workstation::Cookie) -> serde_json::Value {
    let http_only = c.http_only == Some(true);
    let mut v = serde_json::to_value(&c).unwrap_or_default();
    if http_only && let Some(m) = v.as_object_mut() {
        m.insert("value".into(), serde_json::Value::Null);
        m.insert("value_withheld".into(), true.into());
    }
    v
}

#[cfg(target_os = "linux")]
fn required_selector<'r>(req: &'r BrowserRequest, action: &str) -> Result<&'r str, ToolError> {
    req.selector
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            ToolError::invalid(format!(
                "browser {action} requires `selector` (an @eN ref or a CSS selector)"
            ))
        })
}

#[cfg(target_os = "linux")]
async fn run(
    data: &Path,
    caller: &Caller,
    req: BrowserRequest,
    files: &crate::agent_file::FileAuthority,
) -> Result<serde_json::Value, ToolError> {
    use super::session;
    use serde_json::json;

    let action = if req.action.trim().is_empty() {
        if req.url.is_some() { "open" } else { "status" }
    } else {
        req.action.trim()
    }
    .to_ascii_lowercase();
    let profile = super::sanitize_profile_name(req.profile.as_deref(), &caller.profile);
    let explicit_target = match req.target_id.as_deref() {
        Some(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Err(ToolError::invalid("browser `target_id` cannot be empty"));
            }
            Some(trimmed.to_string())
        }
        None => None,
    };
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
            let was_running = session::stop_authorized(data, &profile, &caller.principal).await?;
            return Ok(json!({
                "profile": profile,
                "status": "stopped",
                "was_running": was_running,
                "user_data_preserved": true,
            }));
        }
        "status" | "info" | "fingerprint" => {
            return status(
                data,
                &profile,
                &caller.principal,
                explicit_target.as_deref(),
            )
            .await;
        }
        _ => {}
    }

    let live = session::acquire(data, &profile, &caller.principal, req.proxy.as_deref()).await?;
    let _operation = live.operation().await?;
    let agent = live
        .session
        .lease(&caller.principal)
        .map_err(ToolError::from)?;
    let client = LiveBrowserClient {
        agent,
        live: &live,
        data,
    };
    let out = dispatch_action(
        &client,
        data,
        caller,
        &profile,
        &action,
        explicit_target.as_deref(),
        req,
        files,
    )
    .await?;
    Ok(out)
}

#[cfg(target_os = "linux")]
#[allow(clippy::too_many_arguments)]
async fn dispatch_action<C: BrowserClient>(
    s: &C,
    data: &Path,
    caller: &Caller,
    profile: &str,
    action: &str,
    explicit_target: Option<&str>,
    req: BrowserRequest,
    files: &crate::agent_file::FileAuthority,
) -> Result<serde_json::Value, ToolError> {
    use serde_json::json;
    use std::time::Duration;

    let response = match action {
        "open" | "goto" | "navigate" => {
            let url = req
                .url
                .as_deref()
                .filter(|u| !u.trim().is_empty())
                .ok_or_else(|| ToolError::invalid("browser open requires `url`"))?;
            let nav_url = navigable_url(url)?;
            resolve_explicit_target(s, profile, explicit_target, true).await?;
            s.navigate(&nav_url).await?;
            pause(req.wait_ms).await;
            let mut m = snapshot_json(s).await?;
            m.insert("status".into(), "running".into());
            m
        }
        "back" | "forward" | "reload" => {
            resolve_explicit_target(s, profile, explicit_target, true).await?;
            let url = match action {
                "back" => s.back().await?,
                "forward" => s.forward().await?,
                _ => s.reload().await?,
            };
            pause(req.wait_ms).await;
            json_map(json!({ "url": url }))
        }
        "snapshot" | "dom" => {
            resolve_explicit_target(s, profile, explicit_target, true).await?;
            snapshot_json(s).await?
        }
        "read" | "text" | "content" => {
            resolve_explicit_target(s, profile, explicit_target, true).await?;
            require_readable_page(s).await?;
            let text = s.read().await?;
            let url = s.current_url().await?;
            json_map(json!({ "url": url, "text": text }))
        }
        "html" => {
            let target = req
                .selector
                .as_deref()
                .map(str::trim)
                .filter(|t| !t.is_empty());
            resolve_explicit_target(s, profile, explicit_target, true).await?;
            require_readable_page(s).await?;
            let (html, truncated) = bounded(s.html(target).await?);
            json_map(json!({ "html": html, "truncated": truncated }))
        }
        "eval" | "evaluate" | "js" => {
            let script = req
                .script
                .as_deref()
                .filter(|t| !t.trim().is_empty())
                .ok_or_else(|| ToolError::invalid("browser eval requires `script`"))?;
            resolve_explicit_target(s, profile, explicit_target, true).await?;
            require_readable_page(s).await?;
            let value = s.eval(script).await?;
            let text = value.to_string();
            let m = if text.len() > MAX_TEXT_RESULT {
                let (cut, _) = bounded(text);
                json!({ "result_json_truncated": cut, "truncated": true })
            } else {
                json!({ "result": value })
            };
            json_map(m)
        }
        "cookies" => {
            let parsed_cookies = if let Some(raw) = &req.cookies {
                Some(
                    serde_json::from_value::<Vec<agent_workstation::Cookie>>(
                        serde_json::Value::Array(raw.clone()),
                    )
                    .map_err(|e| ToolError::invalid(format!("invalid `cookies`: {e}")))?,
                )
            } else {
                None
            };
            resolve_explicit_target(s, profile, explicit_target, true).await?;
            require_readable_page(s).await?;
            if let Some(cookies) = parsed_cookies {
                s.set_cookies(&cookies).await?;
            }
            let cookies: Vec<serde_json::Value> =
                s.cookies(&[]).await?.into_iter().map(cookie_json).collect();
            json_map(json!({ "count": cookies.len(), "cookies": cookies }))
        }
        "click" => {
            let target = required_selector(&req, "click")?;
            resolve_explicit_target(s, profile, explicit_target, true).await?;
            require_readable_page(s).await?;
            s.click(target).await?;
            pause(req.wait_ms).await;
            let url = s.current_url().await?;
            let (_, title) = active_tab_info(s).await;
            json_map(json!({ "clicked": target, "url": url, "title": title }))
        }
        "fill" | "type" | "input" => {
            let target = required_selector(&req, "fill")?;
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
            resolve_explicit_target(s, profile, explicit_target, true).await?;
            require_readable_page(s).await?;
            match secret {
                Some(secret_ref) => s.fill_secret(target, secret_ref).await?,
                None => s.fill(target, req.text.as_deref().unwrap_or("")).await?,
            }
            if req.submit.unwrap_or(false) {
                s.focus(target).await?;
                s.press("Enter").await?;
            }
            pause(req.wait_ms).await;
            let mut m = json_map(json!({ "filled": target }));
            if let Some(src) = secret {
                m.insert("secret_source".into(), src.into());
            }
            if req.submit.unwrap_or(false) {
                m.insert("submitted".into(), true.into());
            }
            m
        }
        "press" | "key" | "hold" => {
            let keys = requested_keys(&req)?;
            let hold = req
                .hold_ms
                .or(req.duration_ms)
                .unwrap_or(if action == "hold" { 500 } else { 0 });
            let hold = Duration::from_millis(hold);
            if hold > agent_workstation::MAX_HOLD {
                return Err(ToolError::invalid("a key is held for at most 10 s"));
            }
            resolve_explicit_target(s, profile, explicit_target, true).await?;
            require_readable_page(s).await?;
            if let Some(target) = req
                .selector
                .as_deref()
                .map(str::trim)
                .filter(|t| !t.is_empty())
            {
                s.focus(target).await?;
            }
            match keys.as_slice() {
                [chord] if hold.is_zero() => s.press(chord).await?,
                [chord] => s.press_and_hold(chord, hold).await?,
                several => {
                    for k in several {
                        s.key_down(k).await?;
                    }
                    tokio::time::sleep(hold).await;
                    for k in several.iter().rev() {
                        s.key_up(k).await?;
                    }
                }
            }
            pause(req.wait_ms).await;
            json_map(json!({ "pressed": keys, "held_ms": hold.as_millis() as u64 }))
        }
        "key_down" | "keydown" | "key_up" | "keyup" => {
            let keys = requested_keys(&req)?;
            resolve_explicit_target(s, profile, explicit_target, true).await?;
            require_readable_page(s).await?;
            for k in keys {
                if action.starts_with("key_d") || action == "keydown" {
                    s.key_down(&k).await?;
                } else {
                    s.key_up(&k).await?;
                }
            }
            json_map(json!({ "held_keys": s.held_keys().await? }))
        }
        "release" | "release_keys" => {
            resolve_explicit_target(s, profile, explicit_target, true).await?;
            require_readable_page(s).await?;
            s.release_keys().await?;
            json_map(json!({ "held_keys": [] }))
        }
        "screenshot" | "shot" => {
            resolve_explicit_target(s, profile, explicit_target, true).await?;
            require_readable_page(s).await?;
            let (png, width, height, truncated) =
                s.page_screenshot(req.full_page.unwrap_or(false)).await?;
            let (path, path_refused) =
                save_screenshot(data, caller, profile, req.path.as_deref(), &png, files)?;
            let mut m = json_map(json!({
                "path": path.display().to_string(),
                "bytes": png.len(),
                "width": width,
                "height": height,
                "mime_type": "image/png",
                "image_base64": crate::computer::base64_encode(&png),
            }));
            if let Some(why) = path_refused {
                m.insert("path_refused".into(), why.into());
            }
            if truncated {
                m.insert("truncated".into(), true.into());
            }
            m
        }
        "tabs" => {
            let sub = req
                .tab_action
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("list")
                .to_ascii_lowercase();
            let mut m = match sub.as_str() {
                "list" => {
                    resolve_explicit_target(s, profile, explicit_target, false).await?;
                    json_map(json!({}))
                }
                "new" | "open" => {
                    if explicit_target.is_some() {
                        return Err(ToolError::invalid(
                            "tabs new creates a new tab and does not accept `target_id`",
                        ));
                    }
                    let url = navigable_url(req.url.as_deref().unwrap_or("about:blank"))?;
                    let id = s.new_tab(&url).await?;
                    json_map(json!({ "target_id": id, "url": url }))
                }
                "activate" | "switch" => {
                    let id = explicit_target
                        .ok_or_else(|| ToolError::invalid("tabs activate requires target_id"))?;
                    resolve_explicit_target(s, profile, Some(id), true).await?;
                    json_map(json!({ "active_target_id": id }))
                }
                "close" => {
                    let id = match explicit_target {
                        Some(id) => {
                            resolve_explicit_target(s, profile, Some(id), false).await?;
                            id.to_string()
                        }
                        None => s
                            .tabs()
                            .await?
                            .into_iter()
                            .find(|t| t.active)
                            .map(|t| t.id)
                            .ok_or_else(|| ToolError::invalid("tabs close requires target_id"))?,
                    };
                    s.close_tab(&id).await?;
                    json_map(json!({ "closed_target_id": id }))
                }
                other => {
                    return Err(ToolError::invalid(format!("unknown tab_action `{other}`")));
                }
            };
            let tabs = s.tabs().await?;
            if let Some(active) = tabs.iter().find(|t| t.active)
                && !active.id.is_empty()
            {
                m.insert("active_target_id".into(), active.id.clone().into());
            }
            m.insert("tabs".into(), json!(tabs));
            m
        }
        "takeover" | "human_takeover" | "user_takeover" => {
            let nav_url = req
                .url
                .as_deref()
                .filter(|u| !u.trim().is_empty())
                .map(navigable_url)
                .transpose()?;
            resolve_explicit_target(s, profile, explicit_target, true).await?;
            if let Some(url) = nav_url {
                s.navigate(&url).await?;
            }
            require_readable_page(s).await?;
            let shot = match s.page_screenshot(false).await {
                Ok((png, _, _, _)) => save_screenshot(data, caller, profile, None, &png, files)
                    .ok()
                    .map(|(p, _)| p.display().to_string()),
                Err(_) => None,
            };
            let url = s.current_url().await.unwrap_or_default();
            // Observe before handing control to the person; evaluation must not
            // turn an otherwise available takeover into an error.
            let captcha = s.eval(&detect_captcha_expression()).await.ok();
            s.publish_takeover().await;
            s.expect_hand_back(captcha_detected_flag(captcha.as_ref()));
            if s.takeover_url().is_none() {
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
            insert_captcha_fields(&mut m, captcha.as_ref());
            if let Some(ms) = req.wait_ms.filter(|ms| *ms > 0) {
                let completed = s.wait_for_hand_back(hand_back_wait(ms)).await;
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
            m
        }
        "takeover_status" | "wait_for_user" | "wait_takeover" => {
            let wait = matches!(action, "wait_for_user" | "wait_takeover")
                || req.wait_ms.is_some_and(|w| w > 0);
            let completed = if wait {
                s.wait_for_hand_back(hand_back_wait(req.wait_ms.unwrap_or(60_000)))
                    .await
            } else {
                s.holder() == "agent"
            };
            let mut m = json_map(json!({ "completed": completed }));
            if s.holder() == "agent" {
                resolve_explicit_target(s, profile, explicit_target, true).await?;
                m.insert("url".into(), s.current_url().await?.into());
            }
            m
        }
        "release_takeover" | "end_takeover" | "resume_agent" => {
            let note = req
                .reason
                .as_deref()
                .or(req.text.as_deref())
                .unwrap_or("released by agent");
            let released = s.release_takeover(note.to_string());
            resolve_explicit_target(s, profile, explicit_target, true).await?;
            let url = s.current_url().await?;
            json_map(json!({ "released_takeover": released, "url": url }))
        }
        other => {
            return Err(ToolError::invalid(format!(
                "unknown browser action `{other}`; expected `open`, `back`, `forward`, `reload`, \
             `snapshot`, `read`, `html`, `click`, `fill`, `press`, `hold`, `key_down`, `key_up`, \
             `release_keys`, `eval`, `cookies`, `screenshot`, `tabs`, `takeover`, \
             `takeover_status`, `wait_for_user`, `release_takeover`, `status`, `profiles`, or `close`"
            )));
        }
    };
    Ok(with_profile(
        profile,
        s.holder(),
        action,
        || s.takeover_url(),
        response,
    ))
}

#[cfg(target_os = "linux")]
fn json_map(v: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    match v {
        serde_json::Value::Object(m) => m,
        _ => serde_json::Map::new(),
    }
}

/// How long `wait_for_user` waits: `wait_ms`, at most one hour.
#[cfg(target_os = "linux")]
fn hand_back_wait(wait_ms: u64) -> std::time::Duration {
    std::time::Duration::from_millis(wait_ms.min(3_600_000))
}

#[cfg(target_os = "linux")]
async fn status(
    data: &Path,
    profile: &str,
    principal: &str,
    explicit_target: Option<&str>,
) -> Result<serde_json::Value, ToolError> {
    use serde_json::json;
    let Some(dir) = super::server_profile_dir(data, profile, false)
        .map_err(|e| ToolError::invalid(e.to_string()))?
    else {
        if let Some(id) = explicit_target {
            return Err(ToolError::stale_target(format!(
                "browser target `{id}` does not exist in stopped profile `{profile}`",
            )));
        }
        return Ok(json!({"profile": profile, "status": "stopped", "fingerprint": null}));
    };
    let p = agent_workstation::Profile::new(profile, dir);
    let fingerprint = read_fingerprint(&p.fingerprint_path())?;
    match super::session::running(data, profile) {
        Some(live) => {
            let _operation = live.operation().await?;
            // Never impersonate or acquire another caller's session to read tabs.
            let is_holder = live.leaseholder().as_deref() == Some(principal);
            let agent = is_holder
                .then(|| live.session.lease(principal).ok())
                .flatten();
            let tabs = match agent.as_ref() {
                Some(agent) => agent.tabs().await.unwrap_or_default(),
                None => Vec::new(),
            };
            if is_holder
                && let Some(id) = explicit_target
                && !tabs.iter().any(|t| t.id == id)
            {
                return Err(ToolError::stale_target(format!(
                    "browser target `{id}` does not exist in profile `{profile}` (live targets: {})",
                    live_target_summary(&tabs),
                )));
            }
            Ok(json!({
                "profile": profile,
                "holder": live.holder(),
                "status": "running",
                "started_at_ms": live.started_at_ms(),
                "last_used_at_ms": live.last_used_ms(),
                "proxy_configured": live.proxy.as_deref().is_some_and(|p| !p.is_empty()),
                "held_by": live.leaseholder(),
                "desktop": { "width": live.session.size().0, "height": live.session.size().1 },
                "fingerprint": fingerprint,
                "tabs": tabs,
            }))
        }
        None => {
            if let Some(id) = explicit_target {
                return Err(ToolError::stale_target(format!(
                    "browser target `{id}` does not exist in stopped profile `{profile}`",
                )));
            }
            Ok(json!({
                "profile": profile,
                "status": "stopped",
                "fingerprint": fingerprint,
            }))
        }
    }
}

#[cfg(target_os = "linux")]
fn profile_entry_metadata(
    root: &std::fs::File,
    c_name: &std::ffi::CStr,
) -> std::io::Result<Option<libc::stat>> {
    use std::os::fd::AsRawFd as _;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: root is an open directory, c_name is NUL-terminated,
    // and stat points to storage for the metadata written on success.
    if unsafe {
        libc::fstatat(
            root.as_raw_fd(),
            c_name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        let error = std::io::Error::last_os_error();
        // Unlink after readdir is harmless; other metadata failures are not.
        if error.kind() == std::io::ErrorKind::NotFound {
            return Ok(None);
        }
        return Err(error);
    }
    // SAFETY: fstatat succeeded and initialized stat.
    Ok(Some(unsafe { stat.assume_init() }))
}

#[cfg(target_os = "linux")]
fn read_fingerprint(path: &Path) -> Result<Option<agent_workstation::Fingerprint>, ToolError> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() && meta.len() == 0 => return Ok(None),
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(ToolError::invalid(e.to_string())),
    }
    Ok(agent_workstation::Fingerprint::read(path)?)
}

#[cfg(target_os = "linux")]
fn list_profiles(data: &Path) -> Result<Vec<serde_json::Value>, ToolError> {
    use serde_json::json;
    use std::os::fd::IntoRawFd as _;
    let names = (|| -> std::io::Result<std::collections::BTreeSet<String>> {
        let (data_dir, _) = super::trusted_data_dir(data)?;
        let mut names = std::collections::BTreeSet::new();
        let Some(root) =
            super::owned_root_dir(&data_dir, "browser-profiles".as_ref(), 0o755, false)?
        else {
            return Ok(names);
        };
        names.insert("default".into());
        struct Directory(*mut libc::DIR);
        impl Drop for Directory {
            fn drop(&mut self) {
                unsafe {
                    libc::closedir(self.0);
                }
            }
        }
        let fd = root.try_clone()?.into_raw_fd();
        // SAFETY: fd is a live directory; fdopendir owns it only on success.
        let stream = unsafe { libc::fdopendir(fd) };
        if stream.is_null() {
            let error = std::io::Error::last_os_error();
            unsafe {
                libc::close(fd);
            }
            return Err(error);
        }
        let directory = Directory(stream);
        loop {
            // SAFETY: only this thread uses this directory stream and errno slot.
            unsafe {
                *libc::__errno_location() = 0;
            }
            let entry = unsafe { libc::readdir(directory.0) };
            if entry.is_null() {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() != Some(0) {
                    return Err(error);
                }
                break;
            }
            // SAFETY: readdir supplies a NUL-terminated name valid until its next call.
            let c_name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) };
            if matches!(c_name.to_bytes(), b"." | b"..") {
                continue;
            }
            let Ok(name) = c_name.to_str() else { continue };
            let Some(stat) = profile_entry_metadata(&root, c_name)? else {
                continue;
            };
            if stat.st_mode & libc::S_IFMT == libc::S_IFDIR
                && stat.st_uid == crate::sandbox::server_uid()
            {
                names.insert(name.to_string());
            }
        }
        Ok(names)
    })()
    .map_err(|e| ToolError::invalid(e.to_string()))?;
    Ok(names.into_iter().map(|name| {
        let summary = (|| -> Result<serde_json::Value, ToolError> {
            if let Some(dir) = super::server_profile_dir(data, &name, false).map_err(|e| ToolError::invalid(e.to_string()))? {
                let profile = agent_workstation::Profile::new(&name, dir);
                read_fingerprint(&profile.fingerprint_path())?;
            }
            Ok(match super::session::running(data, &name) {
                Some(live) => json!({"profile":name,"status":"running","holder":live.holder(),"held_by":live.leaseholder(),"last_used_at_ms":live.last_used_ms()}),
                None => json!({"profile":name,"status":"stopped"}),
            })
        })();
        summary.unwrap_or_else(|error| json!({"profile":name,"status":"error","error":error.message}))
    }).collect())
}

#[cfg(target_os = "linux")]
fn save_screenshot(
    data: &Path,
    caller: &Caller,
    profile: &str,
    requested: Option<&str>,
    png: &[u8],
    files: &crate::agent_file::FileAuthority,
) -> Result<(PathBuf, Option<String>), ToolError> {
    let requested = requested.map(str::trim).filter(|s| !s.is_empty());
    let refused = if let Some(target) = requested {
        match files.write(Path::new(target), png) {
            Ok(()) => return Ok((PathBuf::from(target), None)),
            Err(why) => Some(why),
        }
    } else {
        None
    };
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let name = format!(
        "shot-{}-{}.png",
        anyagents_core::now_ms(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    if matches!(files, crate::agent_file::FileAuthority::Seat { .. })
        && let Some(agent_id) = caller.agent_id.as_deref()
        && matches!(
            Path::new(agent_id)
                .components()
                .collect::<Vec<_>>()
                .as_slice(),
            [std::path::Component::Normal(_)]
        )
    {
        let target = data
            .join("agents")
            .join(agent_id)
            .join("sessions/screenshots")
            .join(&name);
        if files.write(&target, png).is_ok() {
            return Ok((target, refused));
        }
    }
    // Only the validated server-owned profile receives the fallback copy.
    // Never infer the caller's identity from a target's filesystem owner.
    let dir = super::server_profile_dir(data, profile, true)
        .map_err(|e| ToolError::invalid(e.to_string()))?
        .ok_or_else(|| ToolError::invalid("browser profile is missing"))?;
    let save = || -> std::io::Result<PathBuf> {
        use std::io::Write as _;
        use std::os::fd::{AsRawFd as _, FromRawFd as _};
        // Keep the complete profile chain open; path validation followed by
        // a separate open would permit an intervening directory replacement.
        let (root, _) = super::trusted_data_dir(data)?;
        let profiles = super::owned_root_dir(
            &root,
            std::ffi::OsStr::new("browser-profiles"),
            0o755,
            false,
        )?
        .ok_or_else(|| std::io::Error::other("browser profiles disappeared"))?;
        let profile_fd =
            super::owned_child_dir(&profiles, std::ffi::OsStr::new(profile), 0o755, false)?
                .ok_or_else(|| std::io::Error::other("browser profile disappeared"))?;
        let parent = super::owned_child_dir(
            &profile_fd,
            std::ffi::OsStr::new("screenshots"),
            0o755,
            true,
        )?
        .ok_or_else(|| std::io::Error::other("screenshots directory disappeared"))?;
        let shots = dir.join("screenshots");
        let c_name = std::ffi::CString::new(name.as_str()).expect("generated filename");
        // The descriptor fixes the directory across renames; O_EXCL refuses
        // existing entries, including symlinks. No server write follows a link.
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                c_name.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0o600,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
        file.write_all(png)?;
        file.sync_all()?;
        Ok(shots.join(name))
    };
    let path = save().map_err(|e| ToolError {
        code: "IO",
        message: format!("cannot save the screenshot: {e}"),
        hint: "Choose a writable path.",
        retryable: false,
    })?;
    Ok((path, refused))
}

#[cfg(all(test, target_os = "linux"))]
fn fixture_chromium() -> Option<PathBuf> {
    match super::session::tool("chromium", "browser") {
        Ok(path) => Some(path),
        Err(err) => {
            if std::env::var_os("CI").is_some() {
                panic!("CI needs Chromium for this test: {}", err.message);
            }
            eprintln!("skipped: {}", err.message);
            None
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    #[test]
    fn web_navigation_and_page_reads_reject_privileged_schemes() {
        for url in [
            "https://example.com",
            "HTTP://example.com",
            "example.com",
            "localhost:8080/x",
            "127.0.0.1:9",
            " about:blank ",
            "\nhttps://example.com\t",
        ] {
            assert!(navigable_url(url).is_ok(), "{url:?}");
        }
        for url in [
            "file:///etc/passwd",
            "FILE:/etc/passwd",
            "fi\tle:///etc/passwd",
            "fi\nle:///etc/passwd",
            "\u{0001}file:///etc/passwd",
            "view-source:https://x",
            "chrome://settings",
            "chrome-untrusted://x",
            "devtools://x",
            "data:text/plain,x",
            "javascript:123",
            "blob:https://x/id",
            "filesystem:https://x/file",
            "about:config",
            "ftp://x",
        ] {
            assert!(navigable_url(url).is_err(), "{url:?}");
            assert!(!readable_page_url(url), "{url:?}");
        }
        assert!(navigable_url("").is_err());
        for url in [
            "",
            "about:blank",
            "chrome-error://chromewebdata/",
            "https://example.com",
            "http:123",
        ] {
            assert!(readable_page_url(url), "{url:?}");
        }
        assert_eq!(url_scheme("javascript:123").as_deref(), Some("javascript"));
        assert_eq!(url_scheme("http:123").as_deref(), Some("http"));
    }
    #[test]
    fn profile_listing_isolates_corruption_and_skips_links() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let data = tempfile::tempdir().unwrap();
        assert!(list_profiles(data.path()).unwrap().is_empty());
        let root = data.path().join("browser-profiles");
        std::fs::create_dir(&root).unwrap();
        for name in ["healthy", "empty-fp", "bad-json", "bad-schema", "insecure"] {
            std::fs::create_dir(root.join(name)).unwrap();
        }
        std::fs::write(root.join("empty-fp/fingerprint.json"), b"").unwrap();
        std::fs::write(root.join("bad-json/fingerprint.json"), b"{").unwrap();
        std::fs::write(root.join("bad-schema/fingerprint.json"), b"{}").unwrap();
        std::fs::set_permissions(
            root.join("insecure"),
            std::fs::Permissions::from_mode(0o777),
        )
        .unwrap();
        let recovery = root.join("bad-json/recovery");
        std::fs::write(&recovery, b"preserve me").unwrap();
        symlink(root.join("healthy"), root.join("linked")).unwrap();
        std::fs::write(root.join("not-a-profile"), b"x").unwrap();
        let profiles = list_profiles(data.path()).unwrap();
        let get = |name: &str| profiles.iter().find(|p| p["profile"] == name).unwrap();
        assert_eq!(get("healthy")["status"], "stopped");
        assert_eq!(get("empty-fp")["status"], "stopped");
        for name in ["bad-json", "bad-schema", "insecure"] {
            assert_eq!(get(name)["status"], "error", "{name}");
            assert!(get(name)["error"].is_string());
        }
        assert!(
            !profiles
                .iter()
                .any(|p| p["profile"] == "linked" || p["profile"] == "not-a-profile")
        );
        assert_eq!(std::fs::read(recovery).unwrap(), b"preserve me");
    }
    #[test]
    fn profile_listing_rejects_invalid_containers() {
        use std::os::unix::fs::symlink;
        let data = tempfile::tempdir().unwrap();
        let root = data.path().join("browser-profiles");
        std::fs::write(&root, b"x").unwrap();
        assert!(list_profiles(data.path()).is_err());
        std::fs::remove_file(&root).unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        symlink(elsewhere.path(), &root).unwrap();
        assert!(list_profiles(data.path()).is_err());
    }
    #[test]
    fn vanished_profile_entry_is_not_a_listing_failure() {
        let data = tempfile::tempdir().unwrap();
        let root = std::fs::File::open(data.path()).unwrap();
        assert!(
            profile_entry_metadata(&root, c"vanished")
                .unwrap()
                .is_none()
        );
    }

    /// Render every reference fixture in real Chromium: only visible challenges count.
    #[test]
    fn captcha_detection_requires_a_rendered_challenge() {
        use std::process::{Command, Stdio};
        let Some(chromium) = fixture_chromium() else {
            return;
        };
        const TS: &str = "https://challenges.cloudflare.com/cdn-cgi/challenge-platform/h/b/turnstile/if/ov2/av0/rcv0/0/abc/light/normal";
        const RC: &str = "https://www.google.com/recaptcha/api2/anchor?k=site";
        const RC_IMG: &str = "https://www.google.com/recaptcha/api2/bframe?k=site";
        const HC: &str =
            "https://newassets.hcaptcha.com/captcha/v1/abc/static/hcaptcha.html#frame=checkbox";
        const HC_IMG: &str =
            "https://newassets.hcaptcha.com/captcha/v1/abc/static/hcaptcha.html#frame=challenge";
        let frame = |src: &str, w: u32, h: u32, style: &str| {
            format!(
                r#"<iframe src="{src}" width="{w}" height="{h}" style="border:0;{style}"></iframe>"#
            )
        };
        let login =
            r#"<form><label>Email <input name="email"></label><button>Continue</button></form>"#;
        let cases: Vec<(String, &str, String, Option<&str>)> = vec![
            ("turnstile 0x0".into(), "Sign in", frame(TS, 0, 0, ""), None),
            ("turnstile 1x1".into(), "Sign in", frame(TS, 1, 1, ""), None),
            ("hcaptcha 1x1".into(), "Sign in", frame(HC, 1, 1, ""), None),
            ("recaptcha 0x0".into(), "Sign in", frame(RC, 0, 0, ""), None),
            ("recaptcha 1x1".into(), "Sign in", frame(RC, 1, 1, ""), None),
            ("turnstile display none".into(), "Sign in", frame(TS, 300, 65, "display:none"), None),
            ("turnstile visibility hidden".into(), "Sign in", frame(TS, 300, 65, "visibility:hidden"), None),
            ("turnstile opacity 0".into(), "Sign in", frame(TS, 300, 65, "opacity:0"), None),
            ("turnstile offscreen".into(), "Sign in", frame(TS, 300, 65, "position:absolute;left:-9999px;top:0"), None),
            ("turnstile fixed offscreen".into(), "Sign in", frame(TS, 300, 65, "position:fixed;top:-9999px;left:0"), None),
            ("turnstile scale 0".into(), "Sign in", frame(TS, 300, 65, "transform:scale(0)"), None),
            (
                "turnstile in 0x0 overflow wrapper".into(),
                "Sign in",
                format!(r#"<div style="width:0;height:0;overflow:hidden">{}</div>"#, frame(TS, 300, 65, "")),
                None,
            ),
            (
                "turnstile in display none wrapper".into(),
                "Sign in",
                format!(r#"<div style="display:none">{}</div>"#, frame(TS, 300, 65, "")),
                None,
            ),
            (
                "turnstile in hidden attribute wrapper".into(),
                "Sign in",
                format!(r#"<div hidden aria-hidden="true">{}</div>"#, frame(TS, 300, 65, "")),
                None,
            ),
            (
                "turnstile in visibility hidden wrapper".into(),
                "Sign in",
                format!(r#"<div style="visibility:hidden">{}</div>"#, frame(TS, 300, 65, "")),
                None,
            ),
            (
                "turnstile in transparent wrapper".into(),
                "Sign in",
                format!(r#"<div style="opacity:0">{}</div>"#, frame(TS, 300, 65, "")),
                None,
            ),
            (
                "turnstile in clip rect wrapper".into(),
                "Sign in",
                format!(
                    r#"<div style="position:absolute;clip:rect(0,0,0,0)">{}</div>"#,
                    frame(TS, 300, 65, "")
                ),
                None,
            ),
            (
                "turnstile in offscreen wrapper".into(),
                "Sign in",
                format!(
                    r#"<div style="position:absolute;left:-9999px">{}</div>"#,
                    frame(TS, 300, 65, "")
                ),
                None,
            ),
            (
                "empty managed turnstile container".into(),
                "Sign in",
                format!(r#"{login}<div class="cf-turnstile" data-sitekey="x"></div><input type="hidden" name="cf-turnstile-response">"#),
                None,
            ),
            (
                "parked recaptcha image challenge".into(),
                "Sign in",
                format!(
                    r#"<div style="visibility:hidden;position:absolute;top:-10000px">{}</div>"#,
                    frame(RC_IMG, 400, 580, "")
                ),
                None,
            ),
            (
                "invisible recaptcha badge".into(),
                "Sign in",
                format!(
                    r#"<div class="grecaptcha-badge" style="position:fixed;right:0;bottom:14px;width:256px;height:60px">{}</div>"#,
                    frame(RC, 256, 60, "")
                ),
                None,
            ),
            (
                "hidden interstitial wording".into(),
                "Just a moment...",
                format!(r#"{login}<div style="display:none">Verify you are human. Cloudflare</div>"#),
                None,
            ),
            (
                "transparent and offscreen challenge wording".into(),
                "Sign in",
                format!(
                    r#"{login}<p style="opacity:0">Verify you are human</p><p style="position:absolute;left:-9999px">Verify you are human</p>"#
                ),
                None,
            ),
            (
                "visible turnstile widget".into(),
                "Sign in",
                format!(r#"{login}<div class="cf-turnstile" style="width:300px;height:65px">{}</div>"#, frame(TS, 300, 65, "")),
                Some("cloudflare_turnstile"),
            ),
            (
                "visible bare turnstile iframe".into(),
                "Sign in",
                format!("{login}{}", frame(TS, 300, 65, "")),
                Some("cloudflare_turnstile"),
            ),
            (
                "visible turnstile below the fold".into(),
                "Sign in",
                format!(r#"<div style="height:2000px"></div>{}"#, frame(TS, 300, 65, "")),
                Some("cloudflare_turnstile"),
            ),
            (
                "visible turnstile in scroll container".into(),
                "Sign in",
                format!(
                    r#"<div style="width:400px;height:200px;overflow:auto">{}</div>"#,
                    frame(TS, 300, 65, "")
                ),
                Some("cloudflare_turnstile"),
            ),
            (
                "visible recaptcha checkbox".into(),
                "Sign in",
                format!(r#"{login}<div class="g-recaptcha">{}</div>"#, frame(RC, 304, 78, "")),
                Some("recaptcha"),
            ),
            (
                "visible recaptcha image challenge".into(),
                "Sign in",
                format!(
                    r#"<div style="position:absolute;top:10px;left:10px">{}</div>"#,
                    frame(RC_IMG, 400, 580, "")
                ),
                Some("recaptcha"),
            ),
            (
                "visible hcaptcha checkbox".into(),
                "Sign in",
                format!("{login}{}", frame(HC, 303, 78, "")),
                Some("hcaptcha"),
            ),
            (
                "visible hcaptcha image challenge".into(),
                "Sign in",
                format!(
                    r#"<div style="position:fixed;top:50px;left:100px">{}</div>"#,
                    frame(HC_IMG, 400, 600, "")
                ),
                Some("hcaptcha"),
            ),
            (
                "visible full-page interstitial".into(),
                "Just a moment...",
                r#"<h1>www.example.com</h1><p>Verify you are human by completing the action below.</p><p>www.example.com needs to review the security of your connection before proceeding.</p>"#.into(),
                Some("cloudflare_turnstile"),
            ),
            (
                "turnstile right of a page that cannot scroll sideways".into(),
                "Sign in",
                format!(
                    "<style>html{{overflow-x:hidden}}</style>{}",
                    frame(TS, 300, 65, "position:absolute;left:9999px;top:0")
                ),
                None,
            ),
            (
                "turnstile below a page that cannot scroll".into(),
                "Sign in",
                format!(
                    "<style>body{{overflow:hidden}}</style>{}",
                    frame(TS, 300, 65, "position:absolute;left:0;top:10000px")
                ),
                None,
            ),
            (
                "turnstile right of a scrollable page".into(),
                "Sign in",
                frame(TS, 300, 65, "position:absolute;left:2000px;top:0"),
                Some("cloudflare_turnstile"),
            ),
            (
                "turnstile in content-visibility hidden".into(),
                "Sign in",
                format!(r#"<div style="content-visibility:hidden">{}</div>"#, frame(TS, 300, 65, "")),
                None,
            ),
            (
                "turnstile in hidden until found".into(),
                "Sign in",
                format!(r#"<div hidden="until-found">{}</div>"#, frame(TS, 300, 65, "")),
                None,
            ),
            (
                "turnstile in closed details".into(),
                "Sign in",
                format!("<details><summary>More</summary>{}</details>", frame(TS, 300, 65, "")),
                None,
            ),
            (
                "fixed turnstile escaping a 0x0 overflow wrapper".into(),
                "Sign in",
                format!(
                    r#"<div style="width:0;height:0;overflow:hidden"><div style="position:fixed;top:0;left:0">{}</div></div>"#,
                    frame(TS, 300, 65, "")
                ),
                Some("cloudflare_turnstile"),
            ),
            (
                "fixed turnstile held by a filtered 0x0 wrapper".into(),
                "Sign in",
                format!(
                    r#"<div style="filter:blur(0);width:0;height:0;overflow:hidden"><div style="position:fixed;top:0;left:0">{}</div></div>"#,
                    frame(TS, 300, 65, "")
                ),
                None,
            ),
            (
                "fixed turnstile held by a paint-contained 0x0 wrapper".into(),
                "Sign in",
                format!(
                    r#"<div style="contain:paint;width:0;height:0;overflow:hidden"><div style="position:fixed;top:0;left:0">{}</div></div>"#,
                    frame(TS, 300, 65, "")
                ),
                None,
            ),
            (
                "visible turnstile under a hidden parent".into(),
                "Sign in",
                format!(
                    r#"<div style="visibility:hidden">{}</div>"#,
                    frame(TS, 300, 65, "visibility:visible")
                ),
                Some("cloudflare_turnstile"),
            ),
        ];
        let fixtures: Vec<serde_json::Value> = cases
            .iter()
            .map(|(name, title, html, _)| serde_json::json!({ "name": name, "title": title, "html": html }))
            .collect();
        let script = format!(
            "const cases = {};\nconst results = {{}};\nfor (const c of cases) {{\n  document.title = c.title;\n  document.body.innerHTML = c.html;\n  window.scrollTo(0, 0);\n  const r = {};\n  results[c.name] = r.detected ? r.kind : null;\n}}\ndocument.title = 'done';\ndocument.body.innerHTML = '';\nconst out = document.createElement('pre');\nout.id = 'results';\nout.textContent = JSON.stringify(results);\ndocument.body.appendChild(out);",
            serde_json::Value::Array(fixtures),
            detect_captcha_expression()
        )
        .replace("</", "<\\/");
        let dir = tempfile::tempdir().unwrap();
        let page = dir.path().join("captcha.html");
        std::fs::write(
            &page,
            format!(
                "<!doctype html><html><head><title>t</title></head><body style=\"margin:0\"><script>{script}</script></body></html>"
            ),
        )
        .unwrap();
        let out = Command::new(chromium)
            .arg("--headless=new")
            .arg("--no-sandbox")
            .arg("--disable-gpu")
            .arg("--no-first-run")
            .arg("--hide-scrollbars")
            .arg("--window-size=1280,800")
            .arg("--host-resolver-rules=MAP * ~NOTFOUND")
            .arg("--timeout=20000")
            .arg(format!(
                "--user-data-dir={}",
                dir.path().join("profile").display()
            ))
            .arg("--dump-dom")
            .arg(format!("file://{}", page.display()))
            .stdin(Stdio::null())
            .output()
            .expect("run Chromium");
        let dom = String::from_utf8_lossy(&out.stdout);
        let json = dom
            .split_once("<pre id=\"results\">")
            .and_then(|(_, rest)| rest.split_once("</pre>"))
            .map(|(json, _)| json)
            .unwrap_or_else(|| {
                panic!(
                    "no results in the dumped page: {dom}\n{}",
                    String::from_utf8_lossy(&out.stderr)
                )
            });
        let results: serde_json::Value = serde_json::from_str(json).expect("results JSON");
        let wrong: Vec<String> = cases
            .iter()
            .filter(|(name, _, _, want)| {
                results.get(name.as_str()).and_then(|v| v.as_str()) != *want
            })
            .map(|(name, _, _, want)| {
                format!("{name}: want {want:?}, got {}", results[name.as_str()])
            })
            .collect();
        assert!(wrong.is_empty(), "wrong detections:\n{}", wrong.join("\n"));
    }

    #[test]
    fn captcha_expression_applies_the_visibility_gate_and_minimum_size() {
        let expression = detect_captcha_expression();
        assert_eq!(CAPTCHA_MIN_SIDE_PX, 40);
        assert_eq!(
            expression,
            format!("{DETECT_CAPTCHA_JS}({CAPTCHA_VISIBLE_JS}, 40)")
        );
        assert!(expression.contains("visible(el, MIN)"));
        assert!(expression.contains("visible(n.parentElement, 1, range.getBoundingClientRect())"));
        assert!(expression.contains("!el.closest('.grecaptcha-badge')"));
        assert!(expression.contains("right - left >= min && bottom - top >= min"));
        assert!(!expression.contains("document.body.innerText"));
    }

    #[test]
    fn captcha_fields_serialize_detection_without_replacing_snapshot_data() {
        let mut m = json_map(json!({ "snapshot": "page", "refs": 3 }));
        insert_captcha_fields(
            &mut m,
            Some(&json!({
                "detected": true,
                "kind": "recaptcha",
                "summary": "Google reCAPTCHA verification challenge"
            })),
        );
        let serialized = serde_json::to_string(&m).unwrap();
        let value: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        assert_eq!(
            value,
            json!({
                "snapshot": "page", "refs": 3, "captcha_detected": true,
                "captcha_kind": "recaptcha",
                "captcha_summary": "Google reCAPTCHA verification challenge"
            })
        );
    }

    #[test]
    fn captcha_fields_default_false_and_omit_absent_or_invalid_metadata() {
        for detection in [
            None,
            Some(json!(null)),
            Some(json!({})),
            Some(json!({ "detected": "true" })),
            Some(json!({ "detected": false, "kind": "recaptcha", "summary": "hidden" })),
        ] {
            let mut m = serde_json::Map::new();
            insert_captcha_fields(&mut m, detection.as_ref());
            assert_eq!(
                serde_json::to_value(m).unwrap(),
                json!({ "captcha_detected": false })
            );
        }
        let mut m = serde_json::Map::new();
        insert_captcha_fields(
            &mut m,
            Some(&json!({ "detected": true, "kind": 7, "summary": null })),
        );
        assert_eq!(
            serde_json::to_value(m).unwrap(),
            json!({ "captcha_detected": true })
        );
    }

    fn caller() -> Caller {
        Caller {
            principal: "agent-a".into(),
            agent_id: None,
            profile: "agent-a".into(),
        }
    }

    #[test]
    fn key_requests_name_one_chord_or_keys_held_together() {
        let req = |keys: &[&str]| BrowserRequest {
            keys: Some(keys.iter().map(|k| k.to_string()).collect()),
            ..Default::default()
        };
        assert_eq!(
            requested_keys(&req(&["ctrl", "shift", "t"])).unwrap(),
            ["ctrl+shift+t"]
        );
        assert_eq!(requested_keys(&req(&["w", "d"])).unwrap(), ["w", "d"]);
        let key = BrowserRequest {
            key: Some(" Enter ".into()),
            ..Default::default()
        };
        assert_eq!(requested_keys(&key).unwrap(), ["Enter"]);
        assert_eq!(
            requested_keys(&BrowserRequest::default()).unwrap_err().code,
            "INVALID_INPUT"
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

    #[test]
    fn http_only_cookie_values_are_withheld_from_the_model() {
        let visible = cookie_json(agent_workstation::Cookie {
            name: "theme".into(),
            value: "dark".into(),
            ..Default::default()
        });
        assert_eq!(visible["value"], "dark");
        let session = cookie_json(agent_workstation::Cookie {
            name: "sid".into(),
            value: "s3cret".into(),
            http_only: Some(true),
            ..Default::default()
        });
        assert!(session["value"].is_null() && session["value_withheld"] == true);
        assert!(!session.to_string().contains("s3cret"));
    }

    #[test]
    fn long_results_are_cut_on_a_character_boundary() {
        let (short, cut) = bounded("é".repeat(10));
        assert!(!cut && short.len() == 20);
        let (long, cut) = bounded("é".repeat(MAX_TEXT_RESULT));
        assert!(cut && long.len() <= MAX_TEXT_RESULT && long.chars().all(|c| c == 'é'));
    }

    #[tokio::test]
    async fn requests_are_checked_before_a_desktop_starts() {
        let dir = tempfile::tempdir().unwrap();
        for (req, code) in [
            (json!({"action": "close", "proxy": "x:1"}), "INVALID_INPUT"),
            (json!({"action": "status", "proxy": ""}), "INVALID_INPUT"),
            (
                json!({"action": "snapshot", "target_id": "   "}),
                "INVALID_INPUT",
            ),
        ] {
            let req: BrowserRequest = serde_json::from_value(req).unwrap();
            let err = execute(
                dir.path(),
                &caller(),
                req,
                &crate::agent_file::FileAuthority::Own,
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(err.contains(&format!("\"code\":\"{code}\"")), "{err}");
        }
        assert!(super::super::session::running(dir.path(), "agent-a").is_none());
    }

    #[derive(Clone, Debug)]
    struct FakeTab {
        id: String,
        url: String,
        title: String,
        active: bool,
        text: String,
    }

    struct FakeBrowserState {
        tabs: Vec<FakeTab>,
        actions_on_tab: Vec<(String, String)>,
        gate: std::sync::Arc<agent_workstation::ControlGate>,
        takeover_url: Option<String>,
        captcha_detected: bool,
    }

    impl FakeBrowserState {
        fn new(tabs: Vec<FakeTab>) -> Self {
            Self {
                tabs,
                actions_on_tab: Vec::new(),
                gate: agent_workstation::ControlGate::new(),
                takeover_url: Some("https://takeover.test/?token=secret".into()),
                captcha_detected: false,
            }
        }

        fn active_id(&self) -> String {
            self.tabs
                .iter()
                .find(|t| t.active)
                .map(|t| t.id.clone())
                .unwrap_or_default()
        }
    }

    struct FakeBrowserClient {
        principal: String,
        state: std::sync::Arc<std::sync::Mutex<FakeBrowserState>>,
    }

    impl FakeBrowserClient {
        fn check_lease(&self) -> Result<(), ToolError> {
            self.state
                .lock()
                .unwrap()
                .gate
                .check(&self.principal)
                .map_err(ToolError::from)
        }

        fn record(&self, op: &str) -> String {
            let mut st = self.state.lock().unwrap();
            let id = st.active_id();
            st.actions_on_tab.push((op.to_string(), id.clone()));
            id
        }
    }

    impl BrowserClient for FakeBrowserClient {
        async fn current_url(&self) -> Result<String, ToolError> {
            self.check_lease()?;
            let st = self.state.lock().unwrap();
            Ok(st
                .tabs
                .iter()
                .find(|t| t.active)
                .map(|t| t.url.clone())
                .unwrap_or_default())
        }
        async fn tabs(&self) -> Result<Vec<agent_workstation::Tab>, ToolError> {
            self.check_lease()?;
            let st = self.state.lock().unwrap();
            Ok(st
                .tabs
                .iter()
                .map(|t| agent_workstation::Tab {
                    id: t.id.clone(),
                    url: t.url.clone(),
                    title: t.title.clone(),
                    active: t.active,
                })
                .collect())
        }
        async fn new_tab(&self, url: &str) -> Result<String, ToolError> {
            self.check_lease()?;
            let mut st = self.state.lock().unwrap();
            for t in &mut st.tabs {
                t.active = false;
            }
            let id = format!("T{}", st.tabs.len() + 1);
            st.tabs.push(FakeTab {
                id: id.clone(),
                url: url.to_string(),
                title: id.clone(),
                active: true,
                text: format!("body:{url}"),
            });
            Ok(id)
        }
        async fn select_tab(&self, id: &str) -> Result<(), ToolError> {
            self.check_lease()?;
            let mut st = self.state.lock().unwrap();
            if !st.tabs.iter().any(|t| t.id == id) {
                return Err(ToolError::from(agent_workstation::Error::Protocol(
                    format!("unknown target {id}"),
                )));
            }
            for t in &mut st.tabs {
                t.active = t.id == id;
            }
            st.actions_on_tab.push(("select_tab".into(), id.into()));
            Ok(())
        }
        async fn close_tab(&self, id: &str) -> Result<(), ToolError> {
            self.check_lease()?;
            let mut st = self.state.lock().unwrap();
            let pos =
                st.tabs.iter().position(|t| t.id == id).ok_or_else(|| {
                    ToolError::from(agent_workstation::Error::NotFound(id.into()))
                })?;
            let was_active = st.tabs[pos].active;
            st.tabs.remove(pos);
            if was_active && let Some(first) = st.tabs.first_mut() {
                first.active = true;
            }
            st.actions_on_tab.push(("close_tab".into(), id.into()));
            Ok(())
        }
        async fn navigate(&self, url: &str) -> Result<String, ToolError> {
            self.check_lease()?;
            let mut st = self.state.lock().unwrap();
            let id = st.active_id();
            if let Some(tab) = st.tabs.iter_mut().find(|t| t.active) {
                tab.url = url.to_string();
            }
            st.actions_on_tab.push(("navigate".into(), id));
            Ok(url.to_string())
        }
        async fn back(&self) -> Result<String, ToolError> {
            self.check_lease()?;
            self.record("back");
            self.current_url().await
        }
        async fn forward(&self) -> Result<String, ToolError> {
            self.check_lease()?;
            self.record("forward");
            self.current_url().await
        }
        async fn reload(&self) -> Result<String, ToolError> {
            self.check_lease()?;
            self.record("reload");
            self.current_url().await
        }
        async fn snapshot(&self) -> Result<agent_workstation::Snapshot, ToolError> {
            self.check_lease()?;
            let id = self.record("snapshot");
            let st = self.state.lock().unwrap();
            let tab = st.tabs.iter().find(|t| t.id == id).unwrap();
            Ok(agent_workstation::Snapshot {
                id: format!("snap-{id}"),
                url: tab.url.clone(),
                text: format!("- heading \"{}\"", tab.title),
                refs: 1,
            })
        }
        async fn read(&self) -> Result<String, ToolError> {
            self.check_lease()?;
            let id = self.record("read");
            let st = self.state.lock().unwrap();
            Ok(st.tabs.iter().find(|t| t.id == id).unwrap().text.clone())
        }
        async fn html(&self, _target: Option<&str>) -> Result<String, ToolError> {
            self.check_lease()?;
            let id = self.record("html");
            Ok(format!("<main data-tab=\"{id}\"></main>"))
        }
        async fn eval(&self, _script: &str) -> Result<serde_json::Value, ToolError> {
            self.check_lease()?;
            let id = self.record("eval");
            let detected = self.state.lock().unwrap().captcha_detected;
            Ok(serde_json::json!({ "detected": detected, "tab": id }))
        }
        async fn cookies(
            &self,
            _urls: &[String],
        ) -> Result<Vec<agent_workstation::Cookie>, ToolError> {
            self.check_lease()?;
            self.record("cookies");
            Ok(Vec::new())
        }
        async fn set_cookies(
            &self,
            _cookies: &[agent_workstation::Cookie],
        ) -> Result<(), ToolError> {
            self.check_lease()?;
            self.record("set_cookies");
            Ok(())
        }
        async fn click(&self, _target: &str) -> Result<(), ToolError> {
            self.check_lease()?;
            self.record("click");
            Ok(())
        }
        async fn fill(&self, _target: &str, _text: &str) -> Result<(), ToolError> {
            self.check_lease()?;
            self.record("fill");
            Ok(())
        }
        async fn fill_secret(&self, _target: &str, _secret_ref: &str) -> Result<(), ToolError> {
            self.check_lease()?;
            self.record("fill_secret");
            Ok(())
        }
        async fn focus(&self, _target: &str) -> Result<(), ToolError> {
            self.check_lease()?;
            self.record("focus");
            Ok(())
        }
        async fn press(&self, _chord: &str) -> Result<(), ToolError> {
            self.check_lease()?;
            self.record("press");
            Ok(())
        }
        async fn press_and_hold(
            &self,
            _chord: &str,
            _hold: std::time::Duration,
        ) -> Result<(), ToolError> {
            self.check_lease()?;
            self.record("press_and_hold");
            Ok(())
        }
        async fn key_down(&self, _chord: &str) -> Result<(), ToolError> {
            self.check_lease()?;
            self.record("key_down");
            Ok(())
        }
        async fn key_up(&self, _chord: &str) -> Result<(), ToolError> {
            self.check_lease()?;
            self.record("key_up");
            Ok(())
        }
        async fn release_keys(&self) -> Result<(), ToolError> {
            self.check_lease()?;
            self.record("release_keys");
            Ok(())
        }
        async fn held_keys(&self) -> Result<Vec<String>, ToolError> {
            self.check_lease()?;
            Ok(Vec::new())
        }
        async fn page_screenshot(
            &self,
            _full_page: bool,
        ) -> Result<(Vec<u8>, u32, u32, bool), ToolError> {
            self.check_lease()?;
            self.record("screenshot");
            Ok((vec![137, 80, 78, 71], 100, 100, false))
        }
        fn holder(&self) -> &'static str {
            match self.state.lock().unwrap().gate.holder() {
                agent_workstation::Holder::Agent => "agent",
                agent_workstation::Holder::Human { .. } => "human",
            }
        }
        fn takeover_url(&self) -> Option<String> {
            self.state.lock().unwrap().takeover_url.clone()
        }
        async fn publish_takeover(&self) {}
        fn expect_hand_back(&self, _challenge_detected: bool) {}
        async fn wait_for_hand_back(&self, _wait: std::time::Duration) -> bool {
            false
        }
        fn release_takeover(&self, note: String) -> bool {
            self.state.lock().unwrap().gate.release(Some(note))
        }
    }

    async fn dispatch_fake(
        client: &FakeBrowserClient,
        data: &Path,
        caller: &Caller,
        profile: &str,
        req: Value,
    ) -> Result<Value, Error> {
        let req: BrowserRequest = serde_json::from_value(req).unwrap();
        let redact = req.carries_secret();
        let mutating = request_mutates(&req);
        let action = req.action.trim().to_ascii_lowercase();
        let target_id = req.target_id.clone();
        dispatch_action(
            client,
            data,
            caller,
            profile,
            &action,
            target_id.as_deref().map(str::trim),
            req,
            &crate::agent_file::FileAuthority::Own,
        )
        .await
        .map_err(|e| e.into_action_error(redact, mutating))
    }

    #[tokio::test]
    async fn takeover_url_requires_explicit_takeover_or_detected_captcha() {
        let dir = tempfile::tempdir().unwrap();
        let caller = caller();
        let state = std::sync::Arc::new(std::sync::Mutex::new(FakeBrowserState::new(vec![
            FakeTab {
                id: "fixture-tab".into(),
                url: "https://example.test/".into(),
                title: "Fixture".into(),
                active: true,
                text: "Fixture".into(),
            },
        ])));
        state
            .lock()
            .unwrap()
            .gate
            .acquire(&caller.principal)
            .unwrap();
        let client = FakeBrowserClient {
            principal: caller.principal.clone(),
            state: state.clone(),
        };
        let expected_url = client.takeover_url().unwrap();
        let dispatch = async |action: &str| {
            dispatch_fake(
                &client,
                dir.path(),
                &caller,
                &caller.profile,
                json!({"action": action}),
            )
            .await
            .unwrap()
        };

        let ordinary = dispatch("snapshot").await;
        assert_eq!(ordinary["captcha_detected"], false);
        assert!(ordinary.get("takeover_url").is_none());

        let status = dispatch("takeover_status").await;
        assert_eq!(status["completed"], true);
        assert!(status.get("takeover_url").is_none());

        for action in ["takeover", "human_takeover", "user_takeover"] {
            let takeover = dispatch(action).await;
            assert_eq!(takeover["captcha_detected"], false);
            assert_eq!(takeover["takeover_url"], expected_url);
        }

        state.lock().unwrap().captcha_detected = true;
        let challenge = dispatch("snapshot").await;
        assert_eq!(challenge["captcha_detected"], true);
        assert_eq!(challenge["takeover_url"], expected_url);
    }

    #[tokio::test]
    async fn unreadable_page_refusal_preserves_active_tab() {
        for url in [
            "file:///tmp/private.txt",
            "chrome://settings/",
            "data:text/html,keep",
        ] {
            let state = std::sync::Arc::new(std::sync::Mutex::new(FakeBrowserState::new(vec![
                FakeTab {
                    id: "kept-target".into(),
                    url: url.into(),
                    title: "Keep this page".into(),
                    active: true,
                    text: "unsaved state".into(),
                },
            ])));
            state.lock().unwrap().gate.acquire("test-agent").unwrap();
            let client = FakeBrowserClient {
                principal: "test-agent".into(),
                state: state.clone(),
            };
            assert!(require_readable_page(&client).await.is_err());
            let state = state.lock().unwrap();
            assert_eq!(state.tabs[0].url, url);
            assert_eq!(state.active_id(), "kept-target");
            assert!(state.actions_on_tab.is_empty());
        }
    }

    #[tokio::test]
    async fn explicit_target_id_selects_matching_tab_and_preserves_other_tabs() {
        let dir = tempfile::tempdir().unwrap();
        let state = std::sync::Arc::new(std::sync::Mutex::new(FakeBrowserState::new(vec![
            FakeTab {
                id: "7B4E44C4".into(),
                url: "https://a.example/".into(),
                title: "Tab A".into(),
                active: true,
                text: "alpha".into(),
            },
            FakeTab {
                id: "2689D04D".into(),
                url: "https://b.example/".into(),
                title: "Tab B".into(),
                active: false,
                text: "beta".into(),
            },
        ])));
        state.lock().unwrap().gate.acquire("fortuna").unwrap();
        let fortuna = Caller {
            agent_id: Some("agt_fortuna".into()),
            principal: "fortuna".into(),
            profile: "sylphx-ga4-agent".into(),
        };
        let client = FakeBrowserClient {
            principal: "fortuna".into(),
            state: state.clone(),
        };

        let snap = dispatch_fake(
            &client,
            dir.path(),
            &fortuna,
            "sylphx-ga4-agent",
            json!({"action": "snapshot", "target_id": "2689D04D"}),
        )
        .await
        .unwrap();
        assert_eq!(snap["target_id"], "2689D04D");
        assert_eq!(snap["url"], "https://b.example/");
        assert_eq!(snap["title"], "Tab B");

        let closed = dispatch_fake(
            &client,
            dir.path(),
            &fortuna,
            "sylphx-ga4-agent",
            json!({"action": "tabs", "tab_action": "close", "target_id": "2689D04D"}),
        )
        .await
        .unwrap();
        assert_eq!(closed["closed_target_id"], "2689D04D");
        assert_eq!(closed["active_target_id"], "7B4E44C4");
        assert_eq!(closed["tabs"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn explicit_target_id_fails_closed_with_stale_target_never_falling_back_to_about_blank() {
        let dir = tempfile::tempdir().unwrap();
        let state = std::sync::Arc::new(std::sync::Mutex::new(FakeBrowserState::new(vec![
            FakeTab {
                id: "C2D59D53".into(),
                url: "about:blank".into(),
                title: String::new(),
                active: true,
                text: String::new(),
            },
        ])));
        state.lock().unwrap().gate.acquire("fortuna").unwrap();
        let fortuna = Caller {
            agent_id: Some("agt_fortuna".into()),
            principal: "fortuna".into(),
            profile: "sylphx-ga4-agent".into(),
        };
        let client = FakeBrowserClient {
            principal: "fortuna".into(),
            state: state.clone(),
        };

        let stale = "7B4E44C4";
        let requests = vec![
            json!({"action": "snapshot", "target_id": stale}),
            json!({"action": "open", "url": "https://analytics.google.com", "target_id": stale}),
            json!({"action": "navigate", "url": "https://analytics.google.com", "target_id": stale}),
            json!({"action": "back", "target_id": stale}),
            json!({"action": "forward", "target_id": stale}),
            json!({"action": "reload", "target_id": stale}),
            json!({"action": "read", "target_id": stale}),
            json!({"action": "html", "target_id": stale}),
            json!({"action": "eval", "script": "location.href", "target_id": stale}),
            json!({"action": "cookies", "target_id": stale}),
            json!({"action": "click", "selector": "#btn", "target_id": stale}),
            json!({"action": "fill", "selector": "#q", "text": "x", "target_id": stale}),
            json!({"action": "type", "selector": "#q", "text": "x", "target_id": stale}),
            json!({"action": "press", "key": "Enter", "target_id": stale}),
            json!({"action": "hold", "key": "Shift", "hold_ms": 10, "target_id": stale}),
            json!({"action": "key_down", "key": "Shift", "target_id": stale}),
            json!({"action": "key_up", "key": "Shift", "target_id": stale}),
            json!({"action": "release_keys", "target_id": stale}),
            json!({"action": "screenshot", "target_id": stale}),
            json!({"action": "takeover", "reason": "captcha", "target_id": stale}),
            json!({"action": "release_takeover", "target_id": stale}),
            json!({"action": "tabs", "tab_action": "list", "target_id": stale}),
            json!({"action": "tabs", "tab_action": "activate", "target_id": stale}),
            json!({"action": "tabs", "tab_action": "close", "target_id": stale}),
        ];

        for req in requests {
            let action = req["action"].as_str().unwrap().to_string();
            let err = dispatch_fake(&client, dir.path(), &fortuna, "sylphx-ga4-agent", req)
                .await
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("\"code\":\"STALE_TARGET\"")
                    && err.contains("\"retry\":\"never\"")
                    && err.contains(stale),
                "action {action} must fail closed with STALE_TARGET: {err}"
            );
        }

        let st = state.lock().unwrap();
        assert!(
            st.actions_on_tab.is_empty(),
            "stale target_id must never touch fallback tab: {:?}",
            st.actions_on_tab
        );
        assert_eq!(st.tabs.len(), 1);
        assert_eq!(st.tabs[0].id, "C2D59D53");
        assert_eq!(st.tabs[0].url, "about:blank");
    }

    #[tokio::test]
    async fn shared_profile_isolates_concurrent_callers_with_desktop_in_use() {
        let dir = tempfile::tempdir().unwrap();
        let state = std::sync::Arc::new(std::sync::Mutex::new(FakeBrowserState::new(vec![
            FakeTab {
                id: "T_FORTUNA".into(),
                url: "https://analytics.google.com/".into(),
                title: "GA4".into(),
                active: true,
                text: "ga4-dashboard".into(),
            },
        ])));
        state.lock().unwrap().gate.acquire("fortuna").unwrap();
        let fortuna = Caller {
            agent_id: Some("agt_fortuna".into()),
            principal: "fortuna".into(),
            profile: "sylphx-ga4-agent".into(),
        };
        let argus = Caller {
            agent_id: Some("agt_argus".into()),
            principal: "argus".into(),
            profile: "sylphx-ga4-agent".into(),
        };

        let fortuna_client = FakeBrowserClient {
            principal: "fortuna".into(),
            state: state.clone(),
        };
        let snap = dispatch_fake(
            &fortuna_client,
            dir.path(),
            &fortuna,
            "sylphx-ga4-agent",
            json!({"action": "snapshot", "target_id": "T_FORTUNA"}),
        )
        .await
        .unwrap();
        assert_eq!(snap["target_id"], "T_FORTUNA");
        let recorded_before = state.lock().unwrap().actions_on_tab.len();

        for action in [
            json!({"action": "open", "url": "https://other.test", "profile": "sylphx-ga4-agent"}),
            json!({"action": "snapshot", "profile": "sylphx-ga4-agent"}),
            json!({"action": "tabs", "tab_action": "close", "target_id": "T_FORTUNA", "profile": "sylphx-ga4-agent"}),
            json!({"action": "takeover", "profile": "sylphx-ga4-agent"}),
        ] {
            let err = {
                let gate_err = state
                    .lock()
                    .unwrap()
                    .gate
                    .acquire(&argus.principal)
                    .unwrap_err();
                super::super::session::in_use("sylphx-ga4-agent", gate_err)
                    .into_action_error(false, true)
                    .to_string()
            };
            assert!(
                err.contains("\"code\":\"DESKTOP_IN_USE\"")
                    && err.contains("sylphx-ga4-agent")
                    && err.contains("fortuna"),
                "unexpected error for {action}: {err}"
            );
            let argus_client = FakeBrowserClient {
                principal: argus.principal.clone(),
                state: state.clone(),
            };
            let direct_err = dispatch_fake(
                &argus_client,
                dir.path(),
                &argus,
                "sylphx-ga4-agent",
                action,
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(
                direct_err.contains("\"code\":\"DESKTOP_IN_USE\""),
                "{direct_err}"
            );
        }

        {
            let st = state.lock().unwrap();
            assert_eq!(st.actions_on_tab.len(), recorded_before);
            assert_eq!(st.tabs[0].url, "https://analytics.google.com/");
        }

        assert!(state.lock().unwrap().gate.release_lease("fortuna"));
        state.lock().unwrap().gate.acquire("argus").unwrap();
        let argus_client = FakeBrowserClient {
            principal: "argus".into(),
            state: state.clone(),
        };
        let argus_snap = dispatch_fake(
            &argus_client,
            dir.path(),
            &argus,
            "sylphx-ga4-agent",
            json!({"action": "snapshot"}),
        )
        .await
        .unwrap();
        assert_eq!(argus_snap["target_id"], "T_FORTUNA");
    }

    use serde_json::{Value, json};
}

#[cfg(all(test, not(target_os = "linux")))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn non_linux_hosts_answer_unsupported_naming_the_routing_setting() {
        let dir = tempfile::tempdir().unwrap();
        let caller = Caller {
            principal: "agent-a".into(),
            agent_id: None,
            profile: "agent-a".into(),
        };
        let err = execute(
            dir.path(),
            &caller,
            BrowserRequest {
                action: "open".into(),
                url: Some("https://example.com".into()),
                ..Default::default()
            },
            &crate::agent_file::FileAuthority::Own,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("\"code\":\"UNSUPPORTED\""), "{err}");
        assert!(err.contains(super::super::REMOTE_WORKSTATION_ENV), "{err}");
    }
}
