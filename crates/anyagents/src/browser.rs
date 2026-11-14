//! Native SOTA anti-detect agent browser with persistent profiles and scale-to-zero lifecycle.
//!
//! Each browser profile lives under `<data>/browser-profiles/<profile>/`:
//! - `user-data/`: persistent Chromium `--user-data-dir` (cookies, localStorage, IndexedDB, sessions).
//! - `fingerprint.json`: deterministic per-profile hardware, WebGL, Client Hints, and canvas/audio seed.
//! - `runtime.json`: live process metadata (`pid`, `cdp_port`, `started_at_ms`, `last_used_at_ms`).
//! - `refs.json`: semantic interactive element reference map (`@e1`, `@e2`, ...) from the latest `snapshot`.
//! - `screenshots/`: captured PNG screenshots ready to view or send via `send` media.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use anyagents_core::{Error, now_ms};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub(crate) mod jail;
mod proxy;

const DEFAULT_IDLE_TIMEOUT_MS: i64 = 15 * 60 * 1000;

fn deterministic_profile_seed(profile: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for byte in profile.as_bytes() {
        h ^= *byte as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58476d1ce4e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d049bb133111eb);
    h ^ (h >> 31)
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct BrowserFingerprint {
    pub profile: String,
    pub seed: u64,
    pub user_agent: String,
    pub chrome_major: String,
    pub chrome_full_version: String,
    pub platform: String,
    pub oscpu: String,
    pub hardware_concurrency: u32,
    pub device_memory: u32,
    pub screen_width: u32,
    pub screen_height: u32,
    pub webgl_vendor: String,
    pub webgl_renderer: String,
    pub languages: Vec<String>,
    pub timezone: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct BrowserRuntimeState {
    pub profile: String,
    pub pid: u32,
    pub cdp_port: u16,
    pub headless: bool,
    pub display_mode: String,
    /// Effective launch proxy; an empty string explicitly disables system proxies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_target_id: Option<String>,
    pub started_at_ms: i64,
    pub last_used_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct InteractiveRef {
    /// Short reference handle such as `"@e1"`, `"@e2"`.
    pub r#ref: String,
    pub tag: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_type: Option<String>,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub href: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    pub selector: String,
    pub x: f64,
    pub y: f64,
    pub width: f64,
    pub height: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frame_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frame_url: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct StoredRefs {
    #[serde(default)]
    pub snapshot_id: String,
    pub url: String,
    pub captured_at_ms: i64,
    pub elements: Vec<InteractiveRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct BrowserProfileSummary {
    pub profile: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cdp_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_at_ms: Option<i64>,
    pub webgl_renderer: String,
    pub user_agent: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct BrowserRequest {
    /// Browser action: `"open"` / `"goto"` (launch if stopped and navigate to `url`),
    /// `"snapshot"` (extract visible text + `@e1`, `@e2` interactive element refs),
    /// `"click"` (click `@e1` ref or CSS `selector`),
    /// `"fill"` / `"type"` (type `text` into `@e1` ref or CSS `selector`, optional `submit`),
    /// `"press"` / `"hold"` (press or hold keyboard `key` / `keys` such as `"Enter"`, `"Escape"`, `"a"`, `"w"`, `"ArrowUp"`, with optional `hold_ms` to hold across rendered frames),
    /// `"key_down"` / `"key_up"` / `"release_keys"` (explicitly hold down or release `key` / `keys` on this profile),
    /// `"eval"` (evaluate JavaScript `script` / `expression` in the page),
    /// `"screenshot"` (save PNG screenshot to `path` or profile screenshots dir),
    /// `"cookies"` (list cookies, or set cookies when `cookies` JSON array is provided),
    /// `"tabs"` (list tabs, or open/switch/close via `tab_action` and `target_id`),
    /// `"takeover"` / `"human_takeover"` (open an interactive live remote browser takeover session so the user can solve a CAPTCHA or 2FA challenge directly in this persistent profile),
    /// `"takeover_status"` / `"wait_for_user"` (inspect or wait for an active human takeover session to complete),
    /// `"release_takeover"` (reclaim control from a human takeover session and shut down its ephemeral takeover endpoint),
    /// `"status"` (inspect profile status and anti-detect fingerprint),
    /// `"profiles"` / `"list"` (list all persistent browser profiles),
    /// or `"close"` / `"stop"` (shut down the browser process to scale to zero while keeping profile login state on disk).
    #[serde(default)]
    pub action: String,
    /// Persistent browser profile name (e.g. `"default"`, `"research"`, `"shopping"`).
    /// Login cookies, localStorage, and deterministic anti-detect fingerprint persist per profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    /// URL to open when `action` is `"open"` or `"goto"` (or when opening a new tab).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Target snapshot-qualified reference (`"@<snapshot_id>:e1"`), active short ref (`"@e1"`),
    /// or CSS selector for `"click"`, `"fill"`, `"type"`. Old snapshot IDs return `STALE_REF`.
    #[serde(
        default,
        alias = "ref",
        alias = "target",
        skip_serializing_if = "Option::is_none"
    )]
    pub selector: Option<String>,
    /// Text to enter for `"fill"` / `"type"`. Supports zero-knowledge secret references:
    /// - `"op://<vault>/<item>/<field>"` (1Password secret reference, resolved in-memory by the daemon)
    /// - `"op-otp://<vault>/<item>"` or `"op://<vault>/<item>/otp?attribute=otp"` (live 1Password 2FA TOTP code)
    /// - `"secret://<ref>"` (AnyAgents local credential store secret)
    ///
    /// Plaintext resolved from a secret reference is typed directly via CDP and never returned in tool output or logs.
    #[serde(default, alias = "value", skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Optional explicit secret reference (`"op://..."`, `"op-otp://..."`, or `"secret://..."`) for `"fill"` / `"type"`,
    /// so credentials can be injected into password or 2FA inputs without ever passing plaintext through LLM context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<String>,
    /// Keyboard key name for `"press"`, `"hold"`, `"key_down"`, or `"key_up"` (e.g. `"Enter"`, `"Tab"`, `"Escape"`, `"a"`, `"w"`, `"d"`, `"ArrowUp"`, `"Space"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Optional list of keys to press, hold, or release simultaneously (e.g. `["w", "d"]`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keys: Option<Vec<String>>,
    /// Duration in milliseconds to hold `key` / `keys` down across rendered frames before releasing (`keyUp`), e.g. `hold_ms: 500` for continuous game movement.
    #[serde(default, alias = "hold", skip_serializing_if = "Option::is_none")]
    pub hold_ms: Option<u64>,
    /// Alias of `hold_ms`: duration in milliseconds to hold `key` / `keys` down across rendered frames before releasing (`keyUp`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// Whether to emit auto-repeat `keyDown` events (`autoRepeat: true`) while holding a key for `hold_ms`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repeat: Option<bool>,
    /// JavaScript code to evaluate when `action = "eval"`.
    #[serde(
        default,
        alias = "expression",
        alias = "js",
        skip_serializing_if = "Option::is_none"
    )]
    pub script: Option<String>,
    /// Optional output file path for `"screenshot"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Whether to press Enter after `"fill"` / `"type"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submit: Option<bool>,
    /// Whether `"screenshot"` should capture the full scrollable page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full_page: Option<bool>,
    /// Optional wait time in milliseconds after navigation or click (default 800ms for `open`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_ms: Option<u64>,
    /// Optional headless override (`false` by default: runs real headed Chromium inside Xvfb virtual display for maximum anti-detect stealth).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headless: Option<bool>,
    /// Launch proxy endpoint: `http://host[:port]`, `https://host[:port]`,
    /// `socks4://host[:port]`, `socks5://host[:port]`, or `host[:port]` (HTTP).
    /// Omitted: keep a running profile's proxy, otherwise use `ANYAGENTS_BROWSER_PROXY`.
    /// Empty string: direct connection, overriding environment/system proxies.
    /// Credentials, whitespace, paths, and proxy maps are rejected. Close a running
    /// profile before changing its proxy; cookies and login state stay on disk.
    /// Applies to actions that start/use the browser, not status/list/close.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy: Option<String>,
    /// Optional tab sub-action when `action = "tabs"`: `"list"` (default), `"new"`, `"activate"`, or `"close"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tab_action: Option<String>,
    /// Optional CDP page `target_id` when switching or closing a tab.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_id: Option<String>,
    /// Optional cookies array to set when `action = "cookies"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cookies: Option<Vec<serde_json::Value>>,
    /// Optional reason / instructions displayed to the user in the takeover UI banner when `action = "takeover"` (e.g. `"Solve Cloudflare Turnstile CAPTCHA"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Optional time-to-live in milliseconds for a human takeover session (default `600_000` = 10 minutes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    /// Whether `action = "takeover"` should open a temporary public HTTPS tunnel (`cloudflared`) when available so the user can open `takeover_url` from a phone or remote browser.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tunnel: Option<bool>,
}

fn sanitize_profile_name(raw: Option<&str>, fallback: &str) -> String {
    let chosen = raw
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(fallback);
    let cleaned: String = chosen
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    let trimmed = cleaned.trim_matches('-').trim_matches('.');
    if trimmed.is_empty() {
        "default".to_string()
    } else {
        trimmed.to_string()
    }
}

pub fn profiles_root(data: &Path) -> PathBuf {
    data.join("browser-profiles")
}

pub fn profile_dir(data: &Path, profile: &str) -> PathBuf {
    profiles_root(data).join(profile)
}

fn detect_chromium_version() -> (String, String) {
    if let Ok(bin) = chromium_binary()
        && let Ok(out) = Command::new(&bin).arg("--version").output()
    {
        let text = String::from_utf8_lossy(&out.stdout);
        for token in text.split_whitespace() {
            if token.chars().next().is_some_and(|c| c.is_ascii_digit()) && token.contains('.') {
                let major = token.split('.').next().unwrap_or("136").to_string();
                return (major, token.to_string());
            }
        }
    }
    ("136".to_string(), "136.0.7103.92".to_string())
}

fn chromium_binary() -> Result<PathBuf, Error> {
    anyagents_toolchain::tool("chromium", "browser")
        .map(|tool| tool.path)
        .map_err(|err| Error::Message(err.to_string()))
}

fn read_profile_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>, Error> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(Error::Message(format!(
                "cannot read {}: {err}",
                path.display()
            )));
        }
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|err| Error::Message(format!("invalid {}: {err}", path.display())))
}

fn write_profile_json<T: Serialize + ?Sized>(path: &Path, value: &T) -> Result<(), Error> {
    static NEXT_WRITE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dir = path
        .parent()
        .ok_or_else(|| Error::Message("profile path has no parent".into()))?;
    std::fs::create_dir_all(dir)?;
    let seq = NEXT_WRITE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp = dir.join(format!(".profile.{}.{seq}.json", std::process::id()));
    let publish = || -> Result<(), Error> {
        let bytes =
            serde_json::to_vec_pretty(value).map_err(|err| Error::Message(err.to_string()))?;
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&tmp, path)?;
        #[cfg(unix)]
        std::fs::File::open(dir)?.sync_all()?;
        Ok(())
    };
    let result = publish();
    let _ = std::fs::remove_file(tmp);
    result
}

fn remove_profile_file(path: &Path) -> Result<(), Error> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

pub fn ensure_fingerprint(data: &Path, profile: &str) -> Result<BrowserFingerprint, Error> {
    let dir = profile_dir(data, profile);
    std::fs::create_dir_all(&dir)?;
    let fp_path = dir.join("fingerprint.json");
    if let Some(existing) = read_profile_json::<BrowserFingerprint>(&fp_path)? {
        return Ok(existing);
    }
    let seed = deterministic_profile_seed(profile);

    let (major, full) = detect_chromium_version();
    let gpu_pool = [
        (
            "Google Inc. (Intel)",
            "ANGLE (Intel, Mesa Intel(R) UHD Graphics 630 (CML GT2), OpenGL 4.6)",
        ),
        (
            "Google Inc. (Intel)",
            "ANGLE (Intel, Mesa Intel(R) Iris(R) Xe Graphics (TGL GT2), OpenGL 4.6)",
        ),
        (
            "Google Inc. (AMD)",
            "ANGLE (AMD, AMD Radeon RX 6700 XT (navi22, LLVM 17.0.6, DRM 3.54, 6.8.0), OpenGL 4.6)",
        ),
        (
            "Google Inc. (NVIDIA Corporation)",
            "ANGLE (NVIDIA Corporation, NVIDIA GeForce RTX 3060/PCIe/SSE2, OpenGL 4.6.0 NVIDIA 550.54.14)",
        ),
    ];
    let (vendor, renderer) = gpu_pool[(seed as usize) % gpu_pool.len()];
    let cores_pool = [8u32, 12, 16];
    let mem_pool = [8u32, 16, 32];
    let hardware_concurrency = cores_pool[((seed >> 8) as usize) % cores_pool.len()];
    let device_memory = mem_pool[((seed >> 16) as usize) % mem_pool.len()];

    let user_agent = format!(
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{full} Safari/537.36"
    );
    let fp = BrowserFingerprint {
        profile: profile.to_string(),
        seed,
        user_agent,
        chrome_major: major,
        chrome_full_version: full,
        platform: "Linux x86_64".to_string(),
        oscpu: "Linux x86_64".to_string(),
        hardware_concurrency,
        device_memory,
        screen_width: 1920,
        screen_height: 1080,
        webgl_vendor: vendor.to_string(),
        webgl_renderer: renderer.to_string(),
        languages: vec![
            "zh-HK".to_string(),
            "zh".to_string(),
            "en-US".to_string(),
            "en".to_string(),
        ],
        timezone: "Asia/Hong_Kong".to_string(),
    };
    write_profile_json(&fp_path, &fp)?;
    Ok(fp)
}

fn stealth_init_script(fp: &BrowserFingerprint) -> String {
    let vendor =
        serde_json::to_string(&fp.webgl_vendor).unwrap_or_else(|_| "\"Google Inc.\"".into());
    let renderer = serde_json::to_string(&fp.webgl_renderer).unwrap_or_else(|_| "\"ANGLE\"".into());
    let langs =
        serde_json::to_string(&fp.languages).unwrap_or_else(|_| "[\"en-US\",\"en\"]".into());
    let noise_seed = (fp.seed % 9973) as u32 + 1;
    format!(
        r#"(() => {{
  'use strict';
    Object.defineProperty(navigator, 'webdriver', {{ get: () => false, configurable: true }});
    Object.defineProperty(navigator, 'languages', {{ get: () => {langs}, configurable: true }});
    Object.defineProperty(navigator, 'language', {{ get: () => 'zh-HK', configurable: true }});
    Object.defineProperty(navigator, 'platform', {{ get: () => 'Linux x86_64', configurable: true }});
    Object.defineProperty(navigator, 'hardwareConcurrency', {{ get: () => {cores}, configurable: true }});
    Object.defineProperty(navigator, 'deviceMemory', {{ get: () => {mem}, configurable: true }});
    Object.defineProperty(navigator, 'pdfViewerEnabled', {{ get: () => true, configurable: true }});
    if (!window.chrome) {{ window.chrome = {{}}; }}
    if (!window.chrome.runtime) {{
      window.chrome.runtime = {{
        OnInstalledReason: {{ CHROME_UPDATE: 'chrome_update', INSTALL: 'install', SHARED_MODULE_UPDATE: 'shared_module_update', UPDATE: 'update' }},
        OnRestartRequiredReason: {{ APP_UPDATE: 'app_update', OS_UPDATE: 'os_update', PERIODIC: 'periodic' }},
        PlatformArch: {{ ARM: 'arm', ARM64: 'arm64', MIPS: 'mips', MIPS64: 'mips64', X86_32: 'x86-32', X86_64: 'x86-64' }},
        PlatformOs: {{ ANDROID: 'android', CROS: 'cros', LINUX: 'linux', MAC: 'mac', OPENBSD: 'openbsd', WIN: 'win' }},
        RequestUpdateCheckStatus: {{ NO_UPDATE: 'no_update', THROTTLED: 'throttled', UPDATE_AVAILABLE: 'update_available' }}
      }};
    }}
    window.chrome.app = window.chrome.app || {{ isInstalled: false, InstallState: {{ DISABLED: 'disabled', INSTALLED: 'installed', NOT_INSTALLED: 'not_installed' }}, RunningState: {{ CANNOT_RUN: 'cannot_run', READY_TO_RUN: 'ready_to_run', RUNNING: 'running' }} }};
    const patchWebGL = (proto) => {{
      if (!proto || !proto.getParameter) return;
      const origParam = proto.getParameter;
      proto.getParameter = function(param) {{
        if (param === 37445) return {vendor};
        if (param === 37446) return {renderer};
        return origParam.apply(this, arguments);
      }};
      const origExt = proto.getExtension;
      if (origExt) {{
        proto.getExtension = function(name) {{
          const res = origExt.apply(this, arguments);
          if (name === 'WEBGL_debug_renderer_info' && !res) {{
            return {{ UNMASKED_VENDOR_WEBGL: 37445, UNMASKED_RENDERER_WEBGL: 37446 }};
          }}
          return res;
        }};
      }}
    }};
    if (typeof WebGLRenderingContext !== 'undefined') patchWebGL(WebGLRenderingContext.prototype);
    if (typeof WebGL2RenderingContext !== 'undefined') patchWebGL(WebGL2RenderingContext.prototype);
    if (typeof HTMLCanvasElement !== 'undefined') {{
      const origGetContext = HTMLCanvasElement.prototype.getContext;
      HTMLCanvasElement.prototype.getContext = function(type, attrs) {{
        const ctx = origGetContext.apply(this, arguments);
        if (!ctx && (type === 'webgl' || type === 'experimental-webgl' || type === 'webgl2')) {{
          return {{
            UNMASKED_VENDOR_WEBGL: 37445,
            UNMASKED_RENDERER_WEBGL: 37446,
            getExtension: (n) => n === 'WEBGL_debug_renderer_info' ? {{ UNMASKED_VENDOR_WEBGL: 37445, UNMASKED_RENDERER_WEBGL: 37446 }} : null,
            getParameter: (p) => p === 37445 ? {vendor} : (p === 37446 ? {renderer} : 'WebGL 1.0'),
            getSupportedExtensions: () => ['WEBGL_debug_renderer_info', 'OES_texture_float', 'OES_element_index_uint'],
          }};
        }}
        return ctx;
      }};
    }}
    if (navigator.permissions && navigator.permissions.query) {{
      const origQuery = navigator.permissions.query.bind(navigator.permissions);
      navigator.permissions.query = (params) => (
        params && params.name === 'notifications'
          ? Promise.resolve({{ state: Notification.permission, onchange: null }})
          : origQuery(params)
      );
    }}
    if (typeof HTMLCanvasElement !== 'undefined') {{
      const origGetImageData = CanvasRenderingContext2D.prototype.getImageData;
      CanvasRenderingContext2D.prototype.getImageData = function() {{
        const imageData = origGetImageData.apply(this, arguments);
        if (imageData && imageData.data && imageData.data.length >= 16) {{
          imageData.data[0] = (imageData.data[0] ^ ({noise_seed} & 1));
        }}
        return imageData;
      }};
    }}
  return true;
}})();"#,
        langs = langs,
        cores = fp.hardware_concurrency,
        mem = fp.device_memory,
        vendor = vendor,
        renderer = renderer,
        noise_seed = noise_seed,
    )
}

fn process_alive(pid: u32) -> bool {
    if pid == 0 || pid > i32::MAX as u32 {
        return false;
    }
    #[cfg(unix)]
    {
        // /proc is not mounted on macOS. Signal 0 checks existence without
        // changing the process; lack of permission still proves it is alive.
        let exists = unsafe { libc::kill(pid as i32, 0) == 0 };
        exists || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }
    #[cfg(not(unix))]
    Path::new(&format!("/proc/{pid}")).exists()
}

fn cdp_port_alive(port: u16) -> bool {
    TcpStream::connect_timeout(
        &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
        Duration::from_millis(250),
    )
    .is_ok()
}

fn load_runtime(data: &Path, profile: &str) -> Result<Option<BrowserRuntimeState>, Error> {
    let path = profile_dir(data, profile).join("runtime.json");
    let Some(state) = read_profile_json::<BrowserRuntimeState>(&path)? else {
        return Ok(None);
    };
    if process_alive(state.pid) {
        if !cdp_port_alive(state.cdp_port) {
            return Err(Error::Message(format!(
                "browser process {} is alive but CDP is unavailable",
                state.pid
            )));
        }
        Ok(Some(state))
    } else {
        remove_profile_file(&path)?;
        Ok(None)
    }
}

fn save_runtime(data: &Path, state: &BrowserRuntimeState) -> Result<(), Error> {
    note_active_profile(data, &state.profile);
    write_profile_json(
        &profile_dir(data, &state.profile).join("runtime.json"),
        state,
    )
}

/// Profiles that may hold a running browser or an open takeover, per profiles
/// root, so the idle reaper never lists every profile ever created. A root is
/// read from disk once, since a browser can outlive the daemon that started it;
/// launches and takeovers in this process add to it, and the reaper drops a
/// profile once neither remains.
#[derive(Default)]
struct ActiveProfiles {
    seeded: bool,
    names: std::collections::BTreeSet<String>,
}

static ACTIVE_PROFILES: std::sync::LazyLock<
    Mutex<std::collections::HashMap<PathBuf, ActiveProfiles>>,
> = std::sync::LazyLock::new(Default::default);

fn active_profiles()
-> std::sync::MutexGuard<'static, std::collections::HashMap<PathBuf, ActiveProfiles>> {
    ACTIVE_PROFILES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn note_active_profile(data: &Path, profile: &str) {
    active_profiles()
        .entry(profiles_root(data))
        .or_default()
        .names
        .insert(profile.to_string());
}

fn forget_active_profile(data: &Path, profile: &str) {
    if let Some(active) = active_profiles().get_mut(&profiles_root(data)) {
        active.names.remove(profile);
    }
}

/// The profiles the idle reaper must look at.
fn candidate_profiles(data: &Path) -> Vec<String> {
    let root = profiles_root(data);
    let mut all = active_profiles();
    let active = all.entry(root.clone()).or_default();
    if !active.seeded {
        if let Ok(entries) = std::fs::read_dir(&root) {
            for entry in entries.flatten() {
                let path = entry.path();
                if (path.join("runtime.json").is_file() || path.join("takeover.json").is_file())
                    && let Some(name) = path.file_name().and_then(|n| n.to_str())
                {
                    active.names.insert(name.to_string());
                }
            }
        }
        active.seeded = true;
    }
    active.names.iter().cloned().collect()
}

fn touch_runtime(data: &Path, state: &mut BrowserRuntimeState) -> Result<(), Error> {
    state.last_used_at_ms = now_ms();
    save_runtime(data, state)
}

fn allocate_free_port() -> Result<u16, Error> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn cleanup_singleton_locks(user_data_dir: &Path) {
    for lock_name in ["SingletonLock", "SingletonCookie", "SingletonSocket"] {
        let p = user_data_dir.join(lock_name);
        if p.symlink_metadata().is_ok() {
            let _ = std::fs::remove_file(&p);
        }
    }
}

/// Flags that would let a page read host files, or lift the same-origin
/// policy that keeps one from another. Chromium runs as the server's user, so
/// none is ever passed (ADR 0010 D3).
const FILE_ACCESS_FLAGS: &[&str] = &[
    "--allow-file-access-from-files",
    "--allow-file-access",
    "--disable-web-security",
    "--enable-local-file-accesses",
];

fn refuse_file_access_flags(args: &[String]) -> Result<(), Error> {
    match args.iter().find(|arg| {
        let name = arg.split('=').next().unwrap_or(arg);
        FILE_ACCESS_FLAGS.contains(&name)
    }) {
        Some(flag) => Err(Error::Message(format!(
            "refusing to start Chromium with `{flag}`"
        ))),
        None => Ok(()),
    }
}

/// `<profile>/chromium-tmp`: names the private temporary directory the
/// profile's running Chromium was given.
fn chromium_tmp_record(profile_dir: &Path) -> PathBuf {
    profile_dir.join("chromium-tmp")
}

const CHROMIUM_TMP_PREFIX: &str = "/tmp/anyagents-chromium-";

/// Remove the private temporary directory recorded for a profile, when it is
/// still this user's own directory.
fn remove_chromium_tmp(profile_dir: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        let record = chromium_tmp_record(profile_dir);
        if let Ok(text) = std::fs::read_to_string(&record) {
            let path = Path::new(text.trim());
            if text.trim().starts_with(CHROMIUM_TMP_PREFIX)
                && path.parent() == Some(Path::new("/tmp"))
                && let Ok(meta) = std::fs::symlink_metadata(path)
                && meta.is_dir()
                && meta.uid() == crate::sandbox::server_uid()
            {
                let _ = std::fs::remove_dir_all(path);
            }
        }
        let _ = std::fs::remove_file(record);
    }
}

/// A new private (0700) temporary directory for a profile's Chromium, with a
/// short name: Chromium's singleton socket lives there and must fit a Unix
/// socket address (108 bytes), which a path inside the profile does not.
fn new_chromium_tmp(profile_dir: &Path) -> Result<PathBuf, Error> {
    use std::os::unix::ffi::OsStringExt as _;
    remove_chromium_tmp(profile_dir);
    let mut template = format!("{CHROMIUM_TMP_PREFIX}XXXXXX\0").into_bytes();
    // SAFETY: a writable NUL-terminated template ending in XXXXXX.
    if unsafe { libc::mkdtemp(template.as_mut_ptr().cast()) }.is_null() {
        return Err(std::io::Error::last_os_error().into());
    }
    template.pop();
    let path = PathBuf::from(std::ffi::OsString::from_vec(template));
    std::fs::write(
        chromium_tmp_record(profile_dir),
        path.as_os_str().as_encoded_bytes(),
    )?;
    Ok(path)
}

/// `<profile>/home`: the `HOME` Chromium runs with.
fn chromium_home(profile_dir: &Path) -> PathBuf {
    profile_dir.join("home")
}

/// The environment that keeps Chromium's per-user files under `home`.
fn chromium_env(home: &Path) -> [(&'static str, PathBuf); 4] {
    [
        ("HOME", home.to_path_buf()),
        ("XDG_CONFIG_HOME", home.join(".config")),
        ("XDG_CACHE_HOME", home.join(".cache")),
        ("XDG_DATA_HOME", home.join(".local/share")),
    ]
}

/// The Chromium command line for one profile, before display and proxy.
fn chromium_args(user_data_dir: &Path, port: u16, fp: &BrowserFingerprint) -> Vec<String> {
    vec![
        format!("--user-data-dir={}", user_data_dir.display()),
        format!("--remote-debugging-port={port}"),
        "--remote-debugging-address=127.0.0.1".to_string(),
        "--remote-allow-origins=*".to_string(),
        "--disable-blink-features=AutomationControlled".to_string(),
        "--disable-infobars".to_string(),
        "--no-first-run".to_string(),
        "--no-default-browser-check".to_string(),
        "--password-store=basic".to_string(),
        "--use-mock-keychain".to_string(),
        "--disable-background-timer-throttling".to_string(),
        "--disable-backgrounding-occluded-windows".to_string(),
        "--disable-renderer-backgrounding".to_string(),
        "--disable-dev-shm-usage".to_string(),
        "--no-sandbox".to_string(),
        "--enable-webgl".to_string(),
        "--ignore-gpu-blocklist".to_string(),
        "--enable-unsafe-swiftshader".to_string(),
        "--force-webrtc-ip-handling-policy=disable_non_proxied_udp".to_string(),
        format!("--user-agent={}", fp.user_agent),
        format!("--window-size={},{}", fp.screen_width, fp.screen_height),
        "--lang=zh-HK".to_string(),
        "about:blank".to_string(),
    ]
}

fn ensure_browser_running(
    data: &Path,
    profile: &str,
    headless_override: Option<bool>,
    proxy_override: Option<&str>,
) -> Result<(BrowserRuntimeState, BrowserFingerprint), Error> {
    let running = load_runtime(data, profile)?;
    let proxy = proxy::resolve(proxy_override, running.as_ref(), proxy::environment_default)?;
    // Do not discard open tabs or interrupt human input to change routing.
    if running.as_ref().is_some_and(|state| state.proxy != proxy) {
        return Err(Error::Message(
            "browser proxy change requires closing the profile first; call close, then reopen with the desired proxy (or an empty string for direct access)".into(),
        ));
    }
    let fp = ensure_fingerprint(data, profile)?;
    if let Some(mut running) = running {
        touch_runtime(data, &mut running)?;
        return Ok((running, fp));
    }

    let dir = profile_dir(data, profile);
    let user_data_dir = dir.join("user-data");
    let screenshots_dir = dir.join("screenshots");
    std::fs::create_dir_all(&user_data_dir)?;
    std::fs::create_dir_all(&screenshots_dir)?;
    cleanup_singleton_locks(&user_data_dir);

    let port = allocate_free_port()?;
    let want_headless = headless_override.unwrap_or(false);
    let has_xvfb = Path::new("/usr/bin/xvfb-run").is_file();
    let has_display = std::env::var_os("DISPLAY").is_some_and(|v| !v.is_empty());
    let bin = chromium_binary()?;

    let mut chrome_args = chromium_args(&user_data_dir, port, &fp);

    if let Some(arg) = proxy::launch_argument(proxy.as_deref()) {
        chrome_args.insert(0, arg);
    }
    refuse_file_access_flags(&chrome_args)?;

    // On Linux Chromium starts inside a Landlock jail that opens only the
    // system and this profile to it; required whenever seats share the host.
    let private_tmp = if cfg!(target_os = "linux") {
        Some(new_chromium_tmp(&dir)?)
    } else {
        None
    };
    let (program, program_args) = match &private_tmp {
        Some(tmp) => jail::wrap(
            &[&std::path::absolute(&dir)?, tmp],
            crate::sandbox::seats_configured(),
            &bin,
        )?,
        None => (bin.clone(), Vec::new()),
    };
    let (mut cmd, display_mode, actual_headless) = if !want_headless && has_display {
        let mut c = Command::new(&program);
        c.args(&program_args).args(&chrome_args);
        (c, "x11-display".to_string(), false)
    } else if !want_headless && has_xvfb {
        let mut c = Command::new("/usr/bin/xvfb-run");
        let xvfb_server_args = "-screen 0 1920x1080x24 -ac +extension GLX +render -noreset";
        c.arg("-a")
            .arg("-s")
            .arg(xvfb_server_args)
            .arg(&program)
            .args(&program_args)
            .args(&chrome_args);
        (c, "xvfb-headed".to_string(), false)
    } else {
        chrome_args.insert(0, "--headless=new".to_string());
        chrome_args.insert(1, "--hide-scrollbars".to_string());
        chrome_args.insert(2, "--disable-gpu".to_string());
        chrome_args.insert(3, "--no-zygote".to_string());
        let mut c = Command::new(&program);
        c.args(&program_args).args(&chrome_args);
        (c, "headless-new".to_string(), true)
    };

    let log_file = std::fs::File::create(dir.join("browser.log"))?;
    let err_file = log_file.try_clone()?;
    cmd.stdin(Stdio::null())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(err_file))
        .env("TZ", &fp.timezone);
    // Chromium's home is inside its profile, so a download, a cache or a
    // certificate store lands there and never in the server user's home; the
    // distribution wrapper takes extra flags only from root's /etc.
    let home = chromium_home(&std::path::absolute(&dir)?);
    std::fs::create_dir_all(home.join("Downloads"))?;
    for (name, value) in chromium_env(&home) {
        cmd.env(name, value);
    }
    // xvfb-run's X authority file and Chromium's own temporary files, its
    // singleton socket among them: in a private directory the jail opens.
    if let Some(tmp) = &private_tmp {
        cmd.env("TMPDIR", tmp);
    }
    for name in ["CHROMIUM_FLAGS", "CHROMIUM_USER_FLAGS", "XDG_DOWNLOAD_DIR"] {
        cmd.env_remove(name);
    }

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        unsafe {
            cmd.pre_exec(|| {
                libc::setsid();
                Ok(())
            });
        }
    }

    let mut child = cmd
        .spawn()
        .map_err(|err| Error::Message(format!("failed to launch chromium: {err}")))?;
    let pid = child.id();

    wait_for_chromium_cdp(
        &mut child,
        port,
        &dir.join("browser.log"),
        Duration::from_secs(30),
    )?;

    let now = now_ms();
    let state = BrowserRuntimeState {
        profile: profile.to_string(),
        pid,
        cdp_port: port,
        headless: actual_headless,
        display_mode,
        proxy,
        active_target_id: None,
        started_at_ms: now,
        last_used_at_ms: now,
    };
    if let Err(err) = save_runtime(data, &state) {
        stop_process_group(pid);
        return Err(err);
    }
    Ok((state, fp))
}

/// A TCP listener alone can precede DevTools initialization. Probe the actual
/// endpoint under the remaining startup budget, never an unbounded HTTP read.
fn chromium_cdp_ready(port: u16, timeout: Duration) -> Result<(), String> {
    let agent = ureq::Agent::config_builder()
        .proxy(None)
        .timeout_global(Some(timeout))
        .max_redirects(0)
        .build()
        .new_agent();
    let mut response = agent
        .get(format!("http://127.0.0.1:{port}/json/version"))
        .call()
        .map_err(|err| err.to_string())?;
    if response.status().as_u16() != 200 {
        return Err(format!(
            "DevTools version HTTP status {}",
            response.status()
        ));
    }
    let text = response
        .body_mut()
        .with_config()
        .limit(16_384)
        .read_to_string()
        .map_err(|err| err.to_string())?;
    let version: serde_json::Value =
        serde_json::from_str(&text).map_err(|_| "invalid DevTools version JSON".to_string())?;
    if version["Browser"].as_str().is_some_and(|v| !v.is_empty())
        && version["webSocketDebuggerUrl"]
            .as_str()
            .is_some_and(|url| url.starts_with(&format!("ws://127.0.0.1:{port}/devtools/browser/")))
    {
        Ok(())
    } else {
        Err("DevTools version response is not ready".to_string())
    }
}

fn browser_startup_log_tail(path: &Path) -> String {
    use std::io::{Seek, SeekFrom};
    let Ok(mut file) = std::fs::File::open(path) else {
        return "<browser log unavailable>".to_string();
    };
    let length = file.metadata().map(|metadata| metadata.len()).unwrap_or(0);
    let _ = file.seek(SeekFrom::Start(length.saturating_sub(8192)));
    let mut bytes = Vec::new();
    match file.take(8192).read_to_end(&mut bytes) {
        Ok(_) => String::from_utf8_lossy(&bytes).into_owned(),
        Err(err) => format!("<browser log unreadable: {err}>"),
    }
}

fn wait_for_chromium_cdp(
    child: &mut std::process::Child,
    port: u16,
    log_path: &Path,
    timeout: Duration,
) -> Result<(), Error> {
    let started = std::time::Instant::now();
    let deadline = started + timeout;
    let mut last_probe = "not attempted".to_string();
    let reason = loop {
        match child.try_wait() {
            Ok(Some(status)) => break format!("Chromium exited before CDP readiness: {status}"),
            Err(err) => break format!("could not inspect Chromium exit status: {err}"),
            Ok(None) => {}
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            break format!(
                "Chromium CDP startup timed out after {}ms",
                timeout.as_millis()
            );
        }
        match chromium_cdp_ready(port, remaining.min(Duration::from_millis(250))) {
            Ok(()) => return Ok(()),
            Err(err) => last_probe = err,
        }
        std::thread::sleep(
            deadline
                .saturating_duration_since(std::time::Instant::now())
                .min(Duration::from_millis(80)),
        );
    };
    // Read the exit before cleanup so our termination signal cannot be mistaken
    // for the cause of a startup failure. Reap the launched child after cleanup.
    let before_cleanup = format!("{:?}", child.try_wait());
    stop_process_group(child.id());
    // Also terminate the direct child if group cleanup could not observe it.
    // Never wait for a TERM-ignoring launcher indefinitely on startup failure.
    let _ = child.kill();
    let after_cleanup = format!("{:?}", child.wait());
    Err(Error::Message(format!(
        "{reason}; port={port}; elapsed={}ms; before_cleanup={before_cleanup}; \
         after_cleanup={after_cleanup}; last_probe={last_probe}; browser.log tail:\n{}",
        started.elapsed().as_millis(),
        browser_startup_log_tail(log_path)
    )))
}

fn stop_process_group(pid: u32) {
    if pid == 0 {
        return;
    }
    #[cfg(unix)]
    unsafe {
        let pgid = -(pid as i32);
        libc::kill(pgid, libc::SIGTERM);
        libc::kill(pid as i32, libc::SIGTERM);
    }
    for _ in 0..20 {
        if !process_alive(pid) {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    #[cfg(unix)]
    unsafe {
        let pgid = -(pid as i32);
        libc::kill(pgid, libc::SIGKILL);
        libc::kill(pid as i32, libc::SIGKILL);
    }
}

pub fn stop_profile(data: &Path, profile: &str) -> Result<serde_json::Value, Error> {
    let dir = profile_dir(data, profile);
    let rt_path = dir.join("runtime.json");
    let mut stopped_pid = None;
    let mut released_keys = Vec::new();
    if let Some(mut rt) = read_profile_json::<BrowserRuntimeState>(&rt_path)? {
        if cdp_port_alive(rt.cdp_port)
            && let Ok(page) = select_page_target(data, &mut rt, None)
            && let Some(ws_url) = page.ws_url
            && let Ok(mut cdp) = CdpSession::connect(rt.cdp_port, &ws_url)
        {
            released_keys = release_recorded_held_keys(data, profile, &mut cdp)?;
        }
        stopped_pid = Some(rt.pid);
        stop_process_group(rt.pid);
    }
    clear_held_keys(data, profile)?;
    cleanup_takeover_session(data, profile)?;
    remove_profile_file(&rt_path)?;
    cleanup_singleton_locks(&dir.join("user-data"));
    remove_chromium_tmp(&dir);
    Ok(serde_json::json!({
        "profile": profile,
        "status": "stopped",
        "stopped_pid": stopped_pid,
        "released_keys": released_keys,
        "user_data_preserved": true,
    }))
}

/// Automatically stop any running browser profile that has been idle past the timeout.
pub fn reap_idle_browsers(data: &Path) {
    let idle_ms = std::env::var("ANYAGENTS_BROWSER_IDLE_SECS")
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .map(|s| s * 1000)
        .unwrap_or(DEFAULT_IDLE_TIMEOUT_MS);
    let now = now_ms();
    for name in candidate_profiles(data) {
        let name = name.as_str();
        if !profile_dir(data, name).is_dir() {
            forget_active_profile(data, name);
            continue;
        }
        if reap_expired_takeover(data, name, now).is_err() {
            continue;
        }
        if is_human_takeover_active(data, name, now).unwrap_or(true) {
            continue;
        }
        match load_runtime(data, name) {
            Ok(Some(rt)) if now.saturating_sub(rt.last_used_at_ms) > idle_ms => {
                if stop_profile(data, name).is_ok() {
                    forget_active_profile(data, name);
                }
            }
            Ok(Some(_)) | Err(_) => {}
            Ok(None) => forget_active_profile(data, name),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CdpTarget {
    pub id: String,
    #[serde(rename = "type")]
    pub target_type: String,
    pub title: String,
    pub url: String,
    #[serde(default, rename = "webSocketDebuggerUrl")]
    pub ws_url: Option<String>,
}

fn retry_interrupted<T>(mut op: impl FnMut() -> std::io::Result<T>) -> std::io::Result<T> {
    loop {
        match op() {
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

fn cdp_http_get(port: u16, path: &str) -> Result<Vec<u8>, Error> {
    let mut stream = retry_interrupted(|| {
        TcpStream::connect_timeout(
            &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
            Duration::from_secs(3),
        )
    })?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let req = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes())?;
    let mut buf = Vec::with_capacity(4096);
    let mut temp = [0u8; 4096];
    let mut header_end: Option<usize> = None;
    let mut content_length: Option<usize> = None;
    loop {
        match retry_interrupted(|| stream.read(&mut temp)) {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&temp[..n]);
                if header_end.is_none()
                    && let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n")
                {
                    let hend = pos + 4;
                    header_end = Some(hend);
                    let headers_str = String::from_utf8_lossy(&buf[..pos]);
                    let status = headers_str
                        .lines()
                        .next()
                        .and_then(|s| s.split_whitespace().nth(1))
                        .and_then(|s| s.parse::<u16>().ok())
                        .ok_or_else(|| Error::Message("invalid CDP HTTP status".into()))?;
                    if !(200..300).contains(&status) {
                        return Err(Error::Message(format!("CDP HTTP {path}: status {status}")));
                    }
                    for line in headers_str.lines() {
                        if let Some((k, v)) = line.split_once(':')
                            && k.trim().eq_ignore_ascii_case("content-length")
                        {
                            content_length = v.trim().parse::<usize>().ok();
                        }
                    }
                }
                if let (Some(hend), Some(clen)) = (header_end, content_length)
                    && buf.len() >= hend + clen
                {
                    return Ok(buf[hend..hend + clen].to_vec());
                }
            }
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) && header_end.is_some() =>
            {
                break;
            }
            Err(err) => return Err(err.into()),
        }
    }
    if let Some(hend) = header_end {
        Ok(buf[hend..].to_vec())
    } else {
        Err(Error::Message("CDP HTTP response has no headers".into()))
    }
}

fn list_page_targets(port: u16) -> Result<Vec<CdpTarget>, Error> {
    let body = cdp_http_get(port, "/json/list")?;
    let all: Vec<CdpTarget> =
        serde_json::from_slice(&body).map_err(|err| Error::Message(err.to_string()))?;
    Ok(all
        .into_iter()
        .filter(|t| t.target_type == "page")
        .collect())
}

fn select_page_target(
    data: &Path,
    rt: &mut BrowserRuntimeState,
    explicit_target: Option<&str>,
) -> Result<CdpTarget, Error> {
    let mut pages = list_page_targets(rt.cdp_port)?;
    if pages.is_empty() {
        let _ = cdp_http_get(rt.cdp_port, "/json/new?about:blank");
        pages = list_page_targets(rt.cdp_port)?;
    }
    if let Some(tid) = explicit_target.or(rt.active_target_id.as_deref())
        && let Some(found) = pages.iter().find(|p| p.id == tid).cloned()
    {
        if rt.active_target_id.as_deref() != Some(&found.id) {
            rt.active_target_id = Some(found.id.clone());
            save_runtime(data, rt)?;
        }
        return Ok(found);
    }
    let chosen = pages
        .into_iter()
        .next()
        .ok_or_else(|| Error::Message("no browser page target available".into()))?;
    rt.active_target_id = Some(chosen.id.clone());
    save_runtime(data, rt)?;
    Ok(chosen)
}

/// Pure-Rust local RFC 6455 WebSocket client for Chrome DevTools Protocol (`127.0.0.1:<port>`).
struct CdpSession {
    port: u16,
    stream: TcpStream,
    read_buf: Vec<u8>,
    next_id: u64,
    takeover_mouse_owner: Option<(PathBuf, String)>,
    takeover_cancel_registered: bool,
    // New-document scripts belong to their CDP connection. Keep OOPIF
    // registrations alive for as long as the takeover input connection.
    takeover_cancel_children: std::collections::HashMap<String, CdpSession>,
}

impl CdpSession {
    fn connect(port: u16, ws_url: &str) -> Result<Self, Error> {
        let path = ws_url
            .find("/devtools/")
            .map(|idx| &ws_url[idx..])
            .unwrap_or("/devtools/browser");
        let mut stream = retry_interrupted(|| {
            TcpStream::connect_timeout(
                &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
                Duration::from_secs(5),
            )
        })?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(Duration::from_secs(20)))?;
        stream.set_write_timeout(Some(Duration::from_secs(10)))?;

        let ws_hdr = ["Sec-WebSocket", "Key"].join("-");
        let ws_val = "AAAAAAAAAAAAAAAAAAAAAA==";
        let handshake = format!(
            "GET {path} HTTP/1.1\r\n\
             Host: 127.0.0.1:{port}\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             {ws_hdr}: {ws_val}\r\n\
             Sec-WebSocket-Version: 13\r\n\r\n"
        );
        stream.write_all(handshake.as_bytes())?;

        let mut buf = Vec::with_capacity(1024);
        let mut temp = [0u8; 512];
        let header_end = loop {
            let n = retry_interrupted(|| stream.read(&mut temp))?;
            if n == 0 {
                return Err(Error::Message(
                    "CDP WebSocket closed during handshake".into(),
                ));
            }
            buf.extend_from_slice(&temp[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
        };
        let header_str = String::from_utf8_lossy(&buf[..header_end]);
        if !header_str.starts_with("HTTP/1.1 101") {
            return Err(Error::Message(format!(
                "CDP WebSocket upgrade failed: {}",
                header_str.lines().next().unwrap_or("unknown")
            )));
        }
        let leftover = buf[header_end..].to_vec();
        Ok(Self {
            port,
            stream,
            read_buf: leftover,
            next_id: 1,
            takeover_mouse_owner: None,
            takeover_cancel_registered: false,
            takeover_cancel_children: Default::default(),
        })
    }

    fn call(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        self.call_in_session(None, method, params)
    }

    fn call_in_session(
        &mut self,
        session_id: Option<&str>,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        let id = self.next_id;
        self.next_id += 1;
        let mut msg = serde_json::json!({
            "id": id,
            "method": method,
            "params": params,
        });
        if let Some(sid) = session_id.filter(|s| !s.is_empty()) {
            msg["sessionId"] = serde_json::Value::String(sid.to_string());
        }
        let payload = msg.to_string();
        self.send_text_frame(payload.as_bytes())?;
        loop {
            let msg_text = self.read_text_message()?;
            let Ok(val) = serde_json::from_str::<serde_json::Value>(&msg_text) else {
                continue;
            };
            if val.get("id").and_then(|v| v.as_u64()) == Some(id) {
                if let Some(err) = val.get("error") {
                    let msg = err
                        .get("message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("CDP call error");
                    return Err(Error::Message(format!("{method}: {msg}")));
                }
                return Ok(val
                    .get("result")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({})));
            }
        }
    }

    fn send_text_frame(&mut self, data: &[u8]) -> Result<(), Error> {
        self.send_frame(0x81, data)
    }

    fn send_frame(&mut self, first_byte: u8, data: &[u8]) -> Result<(), Error> {
        let mask = [0x37u8, 0xfa, 0x21, 0x3d];
        let len = data.len();
        let mut frame = Vec::with_capacity(len + 14);
        frame.push(first_byte);
        if len < 126 {
            frame.push(0x80 | (len as u8));
        } else if len <= 65535 {
            frame.push(0x80 | 126);
            frame.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            frame.push(0x80 | 127);
            frame.extend_from_slice(&(len as u64).to_be_bytes());
        }
        frame.extend_from_slice(&mask);
        for (i, byte) in data.iter().enumerate() {
            frame.push(byte ^ mask[i & 3]);
        }
        self.stream.write_all(&frame)?;
        Ok(())
    }

    fn read_exact_bytes(&mut self, n: usize) -> Result<Vec<u8>, Error> {
        while self.read_buf.len() < n {
            let mut temp = [0u8; 8192];
            let read_n = retry_interrupted(|| self.stream.read(&mut temp))?;
            if read_n == 0 {
                return Err(Error::Message("CDP WebSocket closed unexpectedly".into()));
            }
            self.read_buf.extend_from_slice(&temp[..read_n]);
        }
        let out: Vec<u8> = self.read_buf.drain(..n).collect();
        Ok(out)
    }

    fn read_text_message(&mut self) -> Result<String, Error> {
        let mut message = Vec::new();
        loop {
            let header = self.read_exact_bytes(2)?;
            let fin = (header[0] & 0x80) != 0;
            let opcode = header[0] & 0x0f;
            let masked = (header[1] & 0x80) != 0;
            let mut len = (header[1] & 0x7f) as usize;
            if len == 126 {
                let b = self.read_exact_bytes(2)?;
                len = u16::from_be_bytes([b[0], b[1]]) as usize;
            } else if len == 127 {
                let b = self.read_exact_bytes(8)?;
                len = u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]) as usize;
            }
            let mask_key = if masked {
                let m = self.read_exact_bytes(4)?;
                Some([m[0], m[1], m[2], m[3]])
            } else {
                None
            };
            let mut payload = self.read_exact_bytes(len)?;
            if let Some(mask) = mask_key {
                for (i, b) in payload.iter_mut().enumerate() {
                    *b ^= mask[i & 3];
                }
            }
            match opcode {
                0x8 => return Err(Error::Message("CDP WebSocket closed by browser".into())),
                0x9 => {
                    // Ping -> reply Pong
                    let _ = self.send_frame(0x8A, &payload);
                    continue;
                }
                0xA => continue,
                0x0..=0x2 => {
                    message.extend_from_slice(&payload);
                    if fin {
                        return Ok(String::from_utf8_lossy(&message).into_owned());
                    }
                }
                _ => {}
            }
        }
    }
}

fn apply_stealth(cdp: &mut CdpSession, fp: &BrowserFingerprint) -> Result<(), Error> {
    cdp.call("Page.enable", serde_json::json!({}))?;
    cdp.call("Runtime.enable", serde_json::json!({}))?;
    cdp.call("DOM.enable", serde_json::json!({}))?;
    cdp.call("Network.enable", serde_json::json!({}))?;
    cdp.call(
        "Emulation.setUserAgentOverride",
        serde_json::json!({
            "userAgent": fp.user_agent,
            "acceptLanguage": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "platform": fp.platform,
            "userAgentMetadata": {
                "brands": [
                    { "brand": "Not(A:Brand", "version": "99" },
                    { "brand": "Google Chrome", "version": fp.chrome_major },
                    { "brand": "Chromium", "version": fp.chrome_major }
                ],
                "fullVersionList": [
                    { "brand": "Not(A:Brand", "version": "99.0.0.0" },
                    { "brand": "Google Chrome", "version": fp.chrome_full_version },
                    { "brand": "Chromium", "version": fp.chrome_full_version }
                ],
                "platform": "Linux",
                "platformVersion": "6.8.0",
                "architecture": "x86",
                "model": "",
                "mobile": false,
                "bitness": "64",
                "wow64": false
            }
        }),
    )?;
    cdp.call(
        "Emulation.setTimezoneOverride",
        serde_json::json!({ "timezoneId": fp.timezone }),
    )?;
    let script = stealth_init_script(fp);
    cdp.call(
        "Page.addScriptToEvaluateOnNewDocument",
        serde_json::json!({ "source": script }),
    )?;
    if eval_js(cdp, &script)?.as_bool() != Some(true) {
        return Err(Error::Message(
            "stealth initialization did not complete".into(),
        ));
    }
    Ok(())
}

fn eval_js(cdp: &mut CdpSession, expression: &str) -> Result<serde_json::Value, Error> {
    eval_js_with_session(cdp, None, None, expression)
}

fn eval_js_with_session(
    cdp: &mut CdpSession,
    session_id: Option<&str>,
    context_id: Option<u64>,
    expression: &str,
) -> Result<serde_json::Value, Error> {
    let mut params = serde_json::json!({
        "expression": expression,
        "awaitPromise": true,
        "returnByValue": true,
    });
    if let Some(cid) = context_id {
        params["contextId"] = serde_json::json!(cid);
    }
    let res = cdp.call_in_session(session_id, "Runtime.evaluate", params)?;
    if let Some(exc) = res.get("exceptionDetails") {
        let text = exc
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("JavaScript exception");
        let desc = exc
            .get("exception")
            .and_then(|e| e.get("description"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        return Err(Error::Message(format!("{text}: {desc}")));
    }
    Ok(res
        .get("result")
        .and_then(|r| r.get("value"))
        .cloned()
        .unwrap_or(serde_json::Value::Null))
}

#[derive(Clone, Debug)]
struct ChildFrameInfo {
    pub frame_id: String,
    pub url: String,
    pub offset_x: f64,
    pub offset_y: f64,
}

fn collect_frame_tree_nodes(
    node: &serde_json::Value,
    main_id: &str,
    out: &mut Vec<(String, String, String)>,
) {
    if let Some(frame) = node.get("frame") {
        let id = frame.get("id").and_then(|v| v.as_str()).unwrap_or_default();
        let url = frame
            .get("url")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let name = frame
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if !id.is_empty() && id != main_id {
            out.push((id.to_string(), url.to_string(), name.to_string()));
        }
    }
    if let Some(children) = node.get("childFrames").and_then(|v| v.as_array()) {
        for child in children {
            collect_frame_tree_nodes(child, main_id, out);
        }
    }
}

const IFRAME_RECTS_JS: &str = r#"(() => {
  return Array.from(document.querySelectorAll('iframe')).map(f => {
    const r = f.getBoundingClientRect();
    return {
      src: f.src || f.getAttribute('src') || '',
      name: f.name || '',
      id: f.id || '',
      x: Math.round(r.left),
      y: Math.round(r.top),
      width: Math.round(r.width),
      height: Math.round(r.height),
    };
  });
})()"#;

fn discover_child_frames(cdp: &mut CdpSession) -> Vec<ChildFrameInfo> {
    let iframe_rects = eval_js(cdp, IFRAME_RECTS_JS)
        .ok()
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default();

    let mut raw_frames: Vec<(String, String, String)> = Vec::new();
    if let Ok(tree_res) = cdp.call("Page.getFrameTree", serde_json::json!({}))
        && let Some(root) = tree_res.get("frameTree")
    {
        let main_id = root
            .pointer("/frame/id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        collect_frame_tree_nodes(root, &main_id, &mut raw_frames);
    }
    if let Ok(body) = cdp_http_get(cdp.port, "/json/list")
        && let Ok(all) = serde_json::from_slice::<Vec<CdpTarget>>(&body)
    {
        for t in all {
            if t.target_type == "iframe"
                && !t.id.is_empty()
                && !raw_frames.iter().any(|(id, _, _)| id == &t.id)
            {
                raw_frames.push((t.id, t.url, String::new()));
            }
        }
    }
    if let Ok(targets_res) = cdp.call("Target.getTargets", serde_json::json!({}))
        && let Some(infos) = targets_res.get("targetInfos").and_then(|v| v.as_array())
    {
        for info in infos {
            if info.get("type").and_then(|v| v.as_str()) == Some("iframe") {
                let tid = info
                    .get("targetId")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let url = info.get("url").and_then(|v| v.as_str()).unwrap_or_default();
                if !tid.is_empty() && !raw_frames.iter().any(|(id, _, _)| id == tid) {
                    raw_frames.push((tid.to_string(), url.to_string(), String::new()));
                }
            }
        }
    }

    let mut out = Vec::new();
    for (idx, (fid, furl, fname)) in raw_frames.into_iter().enumerate() {
        let mut offset_x = 0.0;
        let mut offset_y = 0.0;
        let matched = iframe_rects
            .iter()
            .find(|r| {
                let r_src = r.get("src").and_then(|v| v.as_str()).unwrap_or_default();
                let r_name = r.get("name").and_then(|v| v.as_str()).unwrap_or_default();
                let r_id = r.get("id").and_then(|v| v.as_str()).unwrap_or_default();
                (!furl.is_empty()
                    && !r_src.is_empty()
                    && (r_src == furl || furl.starts_with(r_src) || r_src.starts_with(&furl)))
                    || (!fname.is_empty() && (r_name == fname || r_id == fname))
            })
            .or_else(|| iframe_rects.get(idx));
        if let Some(m) = matched {
            offset_x = m.get("x").and_then(|v| v.as_f64()).unwrap_or(0.0);
            offset_y = m.get("y").and_then(|v| v.as_f64()).unwrap_or(0.0);
        }
        out.push(ChildFrameInfo {
            frame_id: fid,
            url: furl,
            offset_x,
            offset_y,
        });
    }
    out
}

fn eval_js_in_frame(
    cdp: &mut CdpSession,
    frame_id: Option<&str>,
    expression: &str,
) -> Result<serde_json::Value, Error> {
    let Some(fid) = frame_id.filter(|s| !s.is_empty()) else {
        return eval_js(cdp, expression);
    };
    // 1. Out-Of-Process Iframe (OOPIF): connect directly to its target WebSocket `/devtools/page/<fid>`.
    if let Ok(mut sub_cdp) = CdpSession::connect(cdp.port, &format!("/devtools/page/{fid}")) {
        let _ = sub_cdp.call("Runtime.enable", serde_json::json!({}));
        if let Ok(val) = eval_js(&mut sub_cdp, expression) {
            return Ok(val);
        }
    }
    // 2. Fallback: in-process child frame via Page.createIsolatedWorld.
    let world = cdp.call(
        "Page.createIsolatedWorld",
        serde_json::json!({
            "frameId": fid,
            "worldName": "anyagents",
            "grantUniveralAccess": true,
        }),
    )?;
    let ctx_id = world
        .get("executionContextId")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| Error::Message(format!("missing executionContextId for frame {fid}")))?;
    eval_js_with_session(cdp, None, Some(ctx_id), expression)
}

fn snapshot_js_with_offset(start_idx: usize) -> String {
    format!(
        r#"(() => {{
  const isVisible = (el) => {{
    const rect = el.getBoundingClientRect();
    if (rect.width < 2 || rect.height < 2) return false;
    const style = window.getComputedStyle(el);
    if (style.visibility === 'hidden' || style.display === 'none' || parseFloat(style.opacity || '1') === 0) return false;
    return true;
  }};
  const cssPath = (el) => {{
    if (el.id && /^[a-zA-Z][\w-]*$/.test(el.id)) return '#' + el.id;
    const parts = [];
    let cur = el;
    while (cur && cur.nodeType === 1 && parts.length < 5) {{
      let part = cur.nodeName.toLowerCase();
      if (cur.id && /^[a-zA-Z][\w-]*$/.test(cur.id)) {{
        parts.unshift('#' + cur.id);
        break;
      }}
      let sib = cur, nth = 1;
      while ((sib = sib.previousElementSibling)) {{
        if (sib.nodeName === cur.nodeName) nth++;
      }}
      if (nth > 1) part += ':nth-of-type(' + nth + ')';
      parts.unshift(part);
      cur = cur.parentElement;
    }}
    return parts.join(' > ');
  }};
  const candidates = Array.from(document.querySelectorAll(
    'a[href], button, input, textarea, select, summary, [role="button"], [role="link"], [role="tab"], [role="menuitem"], [role="checkbox"], [role="switch"], [contenteditable="true"]'
  ));
  const elements = [];
  let idx = {start_idx};
  for (const el of candidates) {{
    if (!isVisible(el)) continue;
    if (elements.length >= 120) break;
    const rect = el.getBoundingClientRect();
    const refId = '@e' + idx++;
    el.setAttribute('data-anyagents-ref', refId);
    const tag = el.tagName.toLowerCase();
    const role = el.getAttribute('role') || null;
    const inputType = el.getAttribute('type') || null;
    const rawText = (
      el.innerText ||
      el.getAttribute('aria-label') ||
      el.getAttribute('placeholder') ||
      el.getAttribute('title') ||
      el.getAttribute('name') ||
      el.value ||
      ''
    ).replace(/\s+/g, ' ').trim().slice(0, 120);
    elements.push({{
      ref: refId,
      tag,
      role,
      input_type: inputType,
      text: rawText,
      href: el.href || null,
      value: typeof el.value === 'string' && el.type !== 'password' ? el.value.slice(0, 120) : null,
      selector: '[data-anyagents-ref="' + refId + '"]',
      fallback_selector: cssPath(el),
      x: Math.round(rect.left + rect.width / 2),
      y: Math.round(rect.top + rect.height / 2),
      width: Math.round(rect.width),
      height: Math.round(rect.height),
    }});
  }}
  const bodyText = (document.body ? document.body.innerText : '')
    .replace(/\n{{3,}}/g, '\n\n')
    .trim()
    .slice(0, 12000);
  return {{
    url: location.href,
    title: document.title || '',
    text: bodyText,
    elements: elements.map(e => ({{
      ref: e.ref,
      tag: e.tag,
      role: e.role,
      input_type: e.input_type,
      text: e.text,
      href: e.href,
      value: e.value,
      selector: e.fallback_selector || e.selector,
      x: e.x,
      y: e.y,
      width: e.width,
      height: e.height,
    }})),
  }};
}})()"#
    )
}

const DEFAULT_TAKEOVER_TTL_MS: u64 = 10 * 60 * 1000;

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct CaptchaDetection {
    pub detected: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

/// Original page metrics are kept with the target, not whichever tab is active at release.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct TakeoverViewport {
    cdp_port: u16,
    window_id: i64,
    window_width: u32,
    window_height: u32,
    viewport_width: u32,
    viewport_height: u32,
    window_state: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct BrowserTakeoverState {
    pub profile: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_agent: Option<String>,
    pub token: String,
    pub port: u16,
    pub local_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_url: Option<String>,
    pub takeover_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// `"human"` while the user holds control; `"agent"` once completed or released.
    pub holder: String,
    pub completed: bool,
    /// Completion was persisted but the owner's continuation still needs delivery.
    #[serde(default)]
    pub notification_pending: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_note: Option<String>,
    pub started_at_ms: i64,
    pub expires_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tunnel_pid: Option<u32>,
    #[serde(default)]
    pub initial_captcha_detected: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_captcha_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_viewport: Option<TakeoverViewport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub viewport_heartbeat_ms: Option<i64>,
}

fn takeover_path(data: &Path, profile: &str) -> PathBuf {
    profile_dir(data, profile).join("takeover.json")
}

// Shared requests and exclusive ownership transitions use the same profile
// fence across workers and processes. Revalidate the token after acquiring it.
fn takeover_ownership_lock(
    data: &Path,
    profile: &str,
    exclusive: bool,
) -> Result<std::fs::File, Error> {
    let dir = profile_dir(data, profile);
    std::fs::create_dir_all(&dir)?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.join("takeover.lock"))?;
    if exclusive {
        retry_interrupted(|| file.lock())?;
    } else {
        retry_interrupted(|| file.lock_shared())?;
    }
    Ok(file)
}

fn load_takeover_state(data: &Path, profile: &str) -> Result<Option<BrowserTakeoverState>, Error> {
    read_profile_json(&takeover_path(data, profile))
}

fn save_takeover_state(data: &Path, state: &BrowserTakeoverState) -> Result<(), Error> {
    if !state.completed {
        note_active_profile(data, &state.profile);
    }
    write_profile_json(&takeover_path(data, &state.profile), state)
}

fn is_human_takeover_active(data: &Path, profile: &str, now: i64) -> Result<bool, Error> {
    Ok(load_takeover_state(data, profile)?.is_some_and(|st| {
        st.holder == "human" && !st.completed && now < st.expires_at_ms && cdp_port_alive(st.port)
    }))
}

fn reap_expired_takeover(data: &Path, profile: &str, now: i64) -> Result<(), Error> {
    let _ownership = takeover_ownership_lock(data, profile, true)?;
    if let Some(mut st) = load_takeover_state(data, profile)?
        && st.holder == "human"
        && !st.completed
        && (now >= st.expires_at_ms || !cdp_port_alive(st.port))
    {
        drain_takeover_mouse(data, profile)?;
        restore_takeover_viewport(&mut st)?;
        if let Some(tpid) = st.tunnel_pid.take() {
            stop_process_group(tpid);
        }
        st.holder = "agent".to_string();
        st.completed = true;
        st.completed_at_ms = Some(now);
        st.completion_note = Some("takeover session expired".to_string());
        save_takeover_state(data, &st)?;
    }
    Ok(())
}

fn cleanup_takeover_session(data: &Path, profile: &str) -> Result<(), Error> {
    let _ownership = takeover_ownership_lock(data, profile, true)?;
    cleanup_takeover_session_owned(data, profile)
}

fn cleanup_takeover_session_owned(data: &Path, profile: &str) -> Result<(), Error> {
    drain_takeover_mouse(data, profile)?;
    if let Some(mut st) = load_takeover_state(data, profile)? {
        if st.notification_pending {
            return Err(Error::Message(
                "takeover owner notification pending; retry completion before cleanup".into(),
            ));
        }
        restore_takeover_viewport(&mut st)?;
        if let Some(tpid) = st.tunnel_pid {
            stop_process_group(tpid);
        }
    }
    remove_profile_file(&takeover_path(data, profile))?;
    Ok(())
}

const TAKEOVER_METRICS_JS: &str = "({width:innerWidth,height:innerHeight,device_scale_factor:devicePixelRatio,screen_width:screen.width,screen_height:screen.height})";

fn restore_takeover_viewport(st: &mut BrowserTakeoverState) -> Result<(), Error> {
    if let Some(original) = st.original_viewport.as_ref() {
        // Bounds are window-global and outlive CDP sessions. Use any surviving
        // page, so closing the initial tab does not strand the resized window.
        if cdp_port_alive(original.cdp_port) {
            let pages = list_page_targets(original.cdp_port)?;
            if let Some(ws) = pages.iter().find_map(|p| p.ws_url.as_deref()) {
                let mut cdp = CdpSession::connect(original.cdp_port, ws)?;
                let mut exists = false;
                for page in &pages {
                    let window = cdp.call(
                        "Browser.getWindowForTarget",
                        serde_json::json!({"targetId": page.id}),
                    )?;
                    if window["windowId"].as_i64() == Some(original.window_id) {
                        exists = true;
                        break;
                    }
                }
                if exists {
                    cdp.call("Browser.setWindowBounds", serde_json::json!({
                        "windowId": original.window_id,
                        "bounds": { "width": original.window_width, "height": original.window_height },
                    }))?;
                    if original.window_state != "normal" {
                        cdp.call("Browser.setWindowBounds", serde_json::json!({
                            "windowId": original.window_id, "bounds": { "windowState": original.window_state },
                        }))?;
                    }
                    // CDP acknowledges the window operation before the renderer
                    // receives its resize. Hand back only after CSS metrics settle.
                    let deadline = std::time::Instant::now() + Duration::from_secs(2);
                    loop {
                        let metrics = eval_js(&mut cdp, TAKEOVER_METRICS_JS)?;
                        if metrics["width"].as_u64() == Some(original.viewport_width as u64)
                            && metrics["height"].as_u64() == Some(original.viewport_height as u64)
                        {
                            break;
                        }
                        if std::time::Instant::now() >= deadline {
                            return Err(Error::Message(
                                "browser viewport restoration did not settle".into(),
                            ));
                        }
                        std::thread::sleep(Duration::from_millis(20));
                    }
                }
            }
        }
        st.original_viewport = None;
        st.viewport_heartbeat_ms = None;
    }
    Ok(())
}

/// Phone layout is a short lease, renewed by /state. A vanished client must not
/// leave the agent with an emulated viewport, even before the takeover expires.
fn reap_disconnected_takeover_viewport(data: &Path, profile: &str, now: i64) -> Result<(), Error> {
    if let Some(mut st) = load_takeover_state(data, profile)?
        && st.original_viewport.is_some()
        && st
            .viewport_heartbeat_ms
            .is_none_or(|last| now - last > 10_000)
    {
        restore_takeover_viewport(&mut st)?;
        save_takeover_state(data, &st)?;
    }
    Ok(())
}

fn set_takeover_viewport(
    data: &Path,
    profile: &str,
    payload: &serde_json::Value,
) -> Result<serde_json::Value, Error> {
    let enabled = payload
        .get("enabled")
        .and_then(|v| v.as_bool())
        .ok_or_else(|| Error::Message("viewport requires boolean enabled".into()))?;
    for field in ["width", "height"] {
        if payload.get(field).is_some_and(|v| v.as_u64().is_none()) {
            return Err(Error::Message(format!(
                "viewport {field} must be an unsigned integer"
            )));
        }
    }
    let mut st = load_takeover_state(data, profile)?
        .ok_or_else(|| Error::Message("takeover session is not active".into()))?;
    if st.completed || st.holder != "human" || now_ms() >= st.expires_at_ms {
        return Err(Error::Message("takeover session has ended".into()));
    }
    if !enabled {
        restore_takeover_viewport(&mut st)?;
    } else {
        let mut rt = load_runtime(data, profile)?
            .ok_or_else(|| Error::Message("browser is not running".into()))?;
        let target = select_page_target(data, &mut rt, None)?;
        let ws_url = target
            .ws_url
            .ok_or_else(|| Error::Message("no page target".into()))?;
        let mut cdp = CdpSession::connect(rt.cdp_port, &ws_url)?;
        let window = cdp.call(
            "Browser.getWindowForTarget",
            serde_json::json!({"targetId": target.id}),
        )?;
        let window_id = window["windowId"]
            .as_i64()
            .ok_or_else(|| Error::Message("browser target has no window".into()))?;
        let bounds = &window["bounds"];
        if st
            .original_viewport
            .as_ref()
            .is_some_and(|v| v.window_id != window_id)
        {
            restore_takeover_viewport(&mut st)?;
        }
        if st.original_viewport.is_none() {
            let metrics = eval_js(&mut cdp, TAKEOVER_METRICS_JS)?;
            st.original_viewport = Some(TakeoverViewport {
                cdp_port: rt.cdp_port,
                window_id,
                window_width: bounds["width"].as_u64().unwrap_or(1280) as u32,
                window_height: bounds["height"].as_u64().unwrap_or(800) as u32,
                viewport_width: metrics["width"].as_u64().unwrap_or(1280) as u32,
                viewport_height: metrics["height"].as_u64().unwrap_or(800) as u32,
                window_state: bounds["windowState"]
                    .as_str()
                    .unwrap_or("normal")
                    .to_string(),
            });
            st.viewport_heartbeat_ms = Some(now_ms());
            save_takeover_state(data, &st)?;
        }
        // Resize only the window: no Emulation/UA/Client Hints/DPR changes.
        // Metrics overrides disappear with their short-lived CDP session; window
        // bounds are shared by the screenshot, input and focused-page sessions.
        cdp.call(
            "Browser.setWindowBounds",
            serde_json::json!({
                "windowId": window_id, "bounds": { "windowState": "normal" },
            }),
        )?;
        cdp.call(
            "Browser.setWindowBounds",
            serde_json::json!({
                "windowId": window_id,
                "bounds": { "width": payload["width"].as_u64().unwrap_or(390).clamp(390, 430),
                            "height": payload["height"].as_u64().unwrap_or(844).clamp(600, 1400) },
            }),
        )?;
        st.viewport_heartbeat_ms = Some(now_ms());
    }
    save_takeover_state(data, &st)?;
    Ok(serde_json::json!({ "ok": true, "phone_layout": st.original_viewport.is_some() }))
}

const TAKEOVER_FOCUS_JS: &str = r#"(() => {
    let el = document.activeElement, x = 0, y = 0;
    // Traverse same-origin frames here; cross-origin focus is inspected via CDP below.
    try {
        while (el && el.tagName === 'IFRAME' && el.contentDocument) {
            const r = el.getBoundingClientRect(); x += r.x; y += r.y;
            el = el.contentDocument.activeElement;
        }
    } catch (_) {}
    const editable = !!el && !el.disabled && !el.readOnly && (el.isContentEditable || el.tagName === 'TEXTAREA' ||
        (el.tagName === 'INPUT' && !['button','submit','reset','checkbox','radio','range','color','file','hidden','image'].includes(el.type)));
    if (!editable) return {editable:false,frame_focused:el?.tagName === 'IFRAME'};
    const r = el.getBoundingClientRect();
    return {editable:true,focused:document.hasFocus(),input_type:el.type || 'text',input_mode:el.inputMode || (el.type === 'email' ? 'email' : el.type === 'number' || el.type === 'tel' ? 'decimal' : 'text'),rect:{x:r.x+x,y:r.y+y,width:r.width,height:r.height}};
})()"#;

fn takeover_focused_field(cdp: &mut CdpSession) -> serde_json::Value {
    let focus = eval_js(cdp, TAKEOVER_FOCUS_JS).unwrap_or(serde_json::Value::Null);
    if focus["frame_focused"].as_bool() == Some(true) {
        for frame in discover_child_frames(cdp) {
            if let Ok(mut child) = eval_js_in_frame(cdp, Some(&frame.frame_id), TAKEOVER_FOCUS_JS)
                && child["editable"].as_bool() == Some(true)
                && child["focused"].as_bool() == Some(true)
            {
                let x = child["rect"]["x"].as_f64().unwrap_or(0.0) + frame.offset_x;
                let y = child["rect"]["y"].as_f64().unwrap_or(0.0) + frame.offset_y;
                child["rect"]["x"] = serde_json::json!(x);
                child["rect"]["y"] = serde_json::json!(y);
                return child;
            }
        }
    }
    focus
}

// Clipboard extraction is explicit and never exposes a password's value or selection.
const TAKEOVER_SELECTION_JS: &str = r#"(() => {
    let el = document.activeElement;
    try { while (el?.tagName === 'IFRAME' && el.contentDocument) el = el.contentDocument.activeElement; } catch (_) {}
    if (el?.tagName === 'IFRAME') return {frame_focused:true};
    if (!document.hasFocus()) return {focused:false};
    if (el?.type === 'password') return {focused:true,blocked:true};
    const doc = el?.ownerDocument || document;
    const identity = doc.__anyagentsTakeoverSelectionIdentity ||= {
        nonce:Array.from(crypto.getRandomValues(new Uint32Array(4))).join('-'), nodes:new WeakMap(), next:0
    };
    const nodeId = node => {
        if (!node) return null;
        if (!identity.nodes.has(node)) identity.nodes.set(node, ++identity.next);
        return identity.nodes.get(node);
    };
    if (el && typeof el.selectionStart === 'number') {
        return {focused:true,text:el.value.slice(el.selectionStart,el.selectionEnd),
            selection_key:JSON.stringify([identity.nonce,nodeId(el),el.selectionStart,el.selectionEnd,el.selectionDirection])};
    }
    const selected = doc.getSelection();
    return {focused:true,text:selected?.toString() || '',
        selection_key:JSON.stringify([identity.nonce,nodeId(el),nodeId(selected?.anchorNode),selected?.anchorOffset,nodeId(selected?.focusNode),selected?.focusOffset])};
})()"#;

fn takeover_selection(cdp: &mut CdpSession) -> Result<serde_json::Value, Error> {
    takeover_selection_expression(cdp, TAKEOVER_SELECTION_JS)
}

fn takeover_selection_expression(
    cdp: &mut CdpSession,
    expression: &str,
) -> Result<serde_json::Value, Error> {
    let selection = eval_js(cdp, expression)?;
    if selection["frame_focused"].as_bool() == Some(true) {
        for frame in discover_child_frames(cdp) {
            if let Ok(child) = eval_js_in_frame(cdp, Some(&frame.frame_id), expression)
                && child["focused"].as_bool() == Some(true)
            {
                return Ok(child);
            }
        }
        return Err(Error::Message("focused selection is unavailable".into()));
    }
    Ok(selection)
}

fn dispatch_takeover_key(cdp: &mut CdpSession, payload: &serde_json::Value) -> Result<(), Error> {
    let key = payload["key"].as_str().unwrap_or("Enter");
    let modifiers = payload["modifiers"].as_u64().unwrap_or(0) & 15;
    // The viewer's OS need not match the remote host. Linux/Windows Chromium
    // treats Control, not a Mac viewer's Meta, as the editing accelerator.
    #[cfg(not(target_os = "macos"))]
    let modifiers = if modifiers & 4 != 0 {
        (modifiers & !4) | 2
    } else {
        modifiers
    };
    let shortcut = modifiers & (2 | 4) != 0;
    let lower = key.to_ascii_lowercase();
    if shortcut && matches!(lower.as_str(), "w" | "t" | "l") {
        return Err(Error::Message("browser-tab shortcuts stay local".into()));
    }
    let commands: Vec<&str> = payload["commands"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str())
        .filter(|v| matches!(*v, "selectAll" | "copy" | "cut" | "paste" | "undo" | "redo"))
        .collect();
    let inferred = if shortcut {
        match lower.as_str() {
            "a" => Some("selectAll"),
            "c" => Some("copy"),
            "x" => Some("cut"),
            "v" => Some("paste"),
            "z" => Some(if modifiers & 8 != 0 { "redo" } else { "undo" }),
            "y" => Some("redo"),
            _ => None,
        }
    } else {
        None
    };
    let commands = if commands.is_empty() {
        inferred.into_iter().collect()
    } else {
        commands
    };
    if commands.iter().any(|c| matches!(*c, "copy" | "cut"))
        && takeover_selection(cdp)?["blocked"].as_bool() == Some(true)
    {
        return Err(Error::Message(
            "password fields cannot be copied or cut".into(),
        ));
    }
    let spec = resolve_key_spec(key);
    let mut down = serde_json::json!({
        "type":"keyDown", "key":spec.key, "code":spec.code,
        "windowsVirtualKeyCode":spec.vk, "nativeVirtualKeyCode":spec.vk,
        "modifiers":modifiers, "commands":commands,
    });
    // A modified printable key is a chord, never a literal character insertion.
    if spec.key.chars().count() == 1 && modifiers & (1 | 2 | 4) == 0 && commands.is_empty() {
        down["text"] = serde_json::json!(spec.key);
        down["unmodifiedText"] = serde_json::json!(spec.key);
    }
    cdp.call("Input.dispatchKeyEvent", down)?;
    cdp.call(
        "Input.dispatchKeyEvent",
        serde_json::json!({
            "type":"keyUp", "key":spec.key, "code":spec.code,
            "windowsVirtualKeyCode":spec.vk, "nativeVirtualKeyCode":spec.vk, "modifiers":modifiers,
        }),
    )?;
    Ok(())
}

fn generate_takeover_token() -> Result<String, Error> {
    takeover_token_from_entropy(std::fs::File::open("/dev/urandom")?)
}

fn takeover_token_from_entropy(mut entropy: impl Read) -> Result<String, Error> {
    let mut bytes = [0u8; 16];
    entropy.read_exact(&mut bytes)?;
    let mut out = String::with_capacity(36);
    out.push_str("tok_");
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    Ok(out)
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.bytes().zip(b.bytes()) {
        diff |= x ^ y;
    }
    diff == 0
}

const DETECT_CAPTCHA_JS: &str = r#"(() => {
  const title = (document.title || '').toLowerCase();
  const bodyText = ((document.body && document.body.innerText) || '').toLowerCase();
  const iframes = Array.from(document.querySelectorAll('iframe')).map(f => (f.src || f.getAttribute('src') || '').toLowerCase());
  const hasSel = (s) => {
    try { return Boolean(document.querySelector(s)); } catch (_) { return false; }
  };
  if (
    hasSel('#cf-chl-widget') ||
    hasSel('.cf-turnstile') ||
    hasSel('[data-sitekey].cf-turnstile') ||
    hasSel('#challenge-running') ||
    hasSel('#challenge-stage') ||
    hasSel('input[name="cf-turnstile-response"]') ||
    iframes.some(u => u.includes('challenges.cloudflare.com') || u.includes('turnstile')) ||
    (title.includes('just a moment') && (bodyText.includes('verify you are human') || bodyText.includes('cloudflare'))) ||
    title.includes('attention required! | cloudflare')
  ) {
    return { detected: true, kind: 'cloudflare_turnstile', summary: 'Cloudflare Turnstile / human verification challenge' };
  }
  if (
    hasSel('.g-recaptcha') ||
    hasSel('#g-recaptcha') ||
    hasSel('textarea[name="g-recaptcha-response"]') ||
    iframes.some(u => u.includes('google.com/recaptcha') || u.includes('recaptcha/api2') || u.includes('recaptcha/enterprise'))
  ) {
    return { detected: true, kind: 'recaptcha', summary: 'Google reCAPTCHA verification challenge' };
  }
  if (
    hasSel('.h-captcha') ||
    hasSel('textarea[name="h-captcha-response"]') ||
    iframes.some(u => u.includes('hcaptcha.com'))
  ) {
    return { detected: true, kind: 'hcaptcha', summary: 'hCaptcha verification challenge' };
  }
  if (
    hasSel('#arkose') ||
    hasSel('[data-arkose]') ||
    iframes.some(u => u.includes('arkoselabs.com') || u.includes('funcaptcha.com'))
  ) {
    return { detected: true, kind: 'arkose_funcaptcha', summary: 'Arkose Labs / FunCaptcha challenge' };
  }
  if (
    hasSel('.geetest_panel') ||
    hasSel('.geetest_holder') ||
    hasSel('.nc_scale') ||
    hasSel('#nc_1_n1z')
  ) {
    return { detected: true, kind: 'slider_captcha', summary: 'Slider / Geetest verification challenge' };
  }
  if (
    hasSel('[data-captcha]') ||
    hasSel('#captcha-box') ||
    title === 'robot check' ||
    title === 'security check' ||
    (bodyText.includes('verify you are human') && bodyText.length < 1500)
  ) {
    return { detected: true, kind: 'generic_captcha', summary: 'Human verification / CAPTCHA challenge' };
  }
  return { detected: false };
})()"#;

fn detect_captcha(cdp: &mut CdpSession) -> CaptchaDetection {
    if let Ok(val) = eval_js(cdp, DETECT_CAPTCHA_JS)
        && val.get("detected").and_then(|v| v.as_bool()) == Some(true)
    {
        return CaptchaDetection {
            detected: true,
            kind: val.get("kind").and_then(|v| v.as_str()).map(str::to_string),
            summary: val
                .get("summary")
                .and_then(|v| v.as_str())
                .map(str::to_string),
        };
    }
    for cf in discover_child_frames(cdp) {
        let lower_url = cf.url.to_ascii_lowercase();
        if lower_url.contains("challenges.cloudflare.com") || lower_url.contains("turnstile") {
            return CaptchaDetection {
                detected: true,
                kind: Some("cloudflare_turnstile".to_string()),
                summary: Some("Cloudflare Turnstile iframe challenge".to_string()),
            };
        }
        if lower_url.contains("google.com/recaptcha") || lower_url.contains("recaptcha/api2") {
            return CaptchaDetection {
                detected: true,
                kind: Some("recaptcha".to_string()),
                summary: Some("Google reCAPTCHA iframe challenge".to_string()),
            };
        }
        if lower_url.contains("hcaptcha.com") {
            return CaptchaDetection {
                detected: true,
                kind: Some("hcaptcha".to_string()),
                summary: Some("hCaptcha iframe challenge".to_string()),
            };
        }
        if lower_url.contains("arkoselabs.com") || lower_url.contains("funcaptcha.com") {
            return CaptchaDetection {
                detected: true,
                kind: Some("arkose_funcaptcha".to_string()),
                summary: Some("Arkose / FunCaptcha iframe challenge".to_string()),
            };
        }
    }
    CaptchaDetection::default()
}

fn notify_agent_takeover_completed(
    data: &Path,
    owner_agent: Option<&str>,
    profile: &str,
    url: &str,
    note: Option<&str>,
    completion_id: i64,
) -> Result<(), Error> {
    let Some(agent_name) = owner_agent
        .map(str::trim)
        .filter(|s| !s.is_empty() && *s != "default" && *s != "operator")
    else {
        return Ok(());
    };
    let db_path = data.join("state.db");
    if !db_path.try_exists()? {
        return Err(Error::Message(
            "takeover completion owner store is missing".into(),
        ));
    }
    let store = anyagents_core::Store::open(db_path)?;
    let agent = store.agent_by_name(agent_name)?;
    let now = now_ms();
    let msg_id = format!("msg_takeover_{profile}_{completion_id}");
    let note_suffix = note
        .filter(|s| !s.trim().is_empty())
        .map(|n| format!(" ({n})"))
        .unwrap_or_default();
    let text = format!(
        "[Browser Takeover Completed] User finished browser takeover on profile `{profile}`{note_suffix} (current url: {url}). Control is now back with you; call `browser({{ \"action\": \"snapshot\", \"profile\": \"{profile}\" }})` to inspect the updated page and continue."
    );
    let outcome = store.receive(
        &agent.id,
        anyagents_core::Incoming {
            source_key: Some(msg_id),
            text,
            sender: Some("system:browser-takeover".to_string()),
            reply_to: None,
            quote: None,
            source_time_ms: Some(now),
            received_at_ms: Some(now),
            origin: None,
        },
    )?;
    // Receipt replay must not replay lifecycle effects after a later archive.
    if matches!(outcome, anyagents_core::ReceiveOutcome::Duplicate(_)) {
        return Ok(());
    }
    if agent.state == "idle" || agent.state == "archived" {
        store.set_agent_state(&agent.id, "running")?;
    }
    crate::create::poke_agent_wake_socket(data, &agent.id);
    crate::create::poke_agent_wake_socket_mail(data, &agent.id);
    Ok(())
}

fn spawn_cloudflared_quick_tunnel(local_port: u16, token: &str) -> (Option<String>, Option<u32>) {
    if let Ok(explicit_base) = std::env::var("ANYAGENTS_TAKEOVER_URL") {
        let trimmed = explicit_base.trim().trim_end_matches('/');
        if !trimmed.is_empty() {
            return (Some(format!("{trimmed}/takeover/{token}")), None);
        }
    }
    let Ok(bin) = anyagents_toolchain::tool("cloudflared", "browser.takeover") else {
        return (None, None);
    };

    let mut cmd = bin.command();
    cmd.args([
        "tunnel",
        "--no-autoupdate",
        "--url",
        &format!("http://127.0.0.1:{local_port}"),
    ])
    .stdin(Stdio::null())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped());

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        unsafe {
            cmd.pre_exec(|| {
                libc::setsid();
                Ok(())
            });
        }
    }

    let Ok(mut child) = cmd.spawn() else {
        return (None, None);
    };
    let pid = child.id();
    let (tx, rx) = std::sync::mpsc::channel::<String>();

    if let Some(stderr) = child.stderr.take() {
        let tx_err = tx.clone();
        std::thread::spawn(move || {
            use std::io::{BufRead, BufReader};
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if let Some(url) = extract_trycloudflare_url(&line) {
                    let _ = tx_err.send(url);
                }
            }
        });
    }
    if let Some(stdout) = child.stdout.take() {
        std::thread::spawn(move || {
            use std::io::{BufRead, BufReader};
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Some(url) = extract_trycloudflare_url(&line) {
                    let _ = tx.send(url);
                }
            }
        });
    }

    if let Ok(base_url) = rx.recv_timeout(Duration::from_secs(7)) {
        let clean = base_url.trim_end_matches('/');
        (Some(format!("{clean}/takeover/{token}")), Some(pid))
    } else {
        stop_process_group(pid);
        (None, None)
    }
}

fn extract_trycloudflare_url(line: &str) -> Option<String> {
    let idx = line.find("https://")?;
    let rest = &line[idx..];
    let end = rest
        .find(|c: char| c.is_whitespace() || c == '|' || c == '"' || c == '\'')
        .unwrap_or(rest.len());
    let candidate = &rest[..end];
    if candidate.contains(".trycloudflare.com") {
        Some(candidate.to_string())
    } else {
        None
    }
}

fn render_takeover_html(profile: &str, token: &str, reason: Option<&str>) -> String {
    let escape = |s: &str| {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
    };
    let config = serde_json::json!({ "token": token })
        .to_string()
        .replace('<', "\\u003c");
    include_str!("browser_takeover.html")
        .replace("{{PROFILE}}", &escape(profile))
        .replace(
            "{{REASON}}",
            &escape(reason.unwrap_or("Finish verification, then tap Done.")),
        )
        .replace("{{CONFIG}}", &config)
        .replace("{{SCRIPT}}", include_str!("browser_takeover.js"))
}

/// The URL a page may be sent to for a caller: `http(s)://` or
/// `about:blank`, as Chromium itself will read it. A URL without a scheme is
/// `https://`. Chromium runs as the server's user, so `file:`, `view-source:`,
/// `chrome:`, `devtools:`, `data:`, `javascript:` and every other scheme would
/// reach host files or browser internals the caller could not (ADR 0010 D3).
fn navigable_url(raw: &str) -> Result<String, Error> {
    // What Chromium's URL parser drops before it reads a scheme: leading and
    // trailing C0 controls and spaces, and every tab or newline anywhere.
    let url: String = raw
        .trim_matches(|c: char| c <= ' ')
        .chars()
        .filter(|c| !matches!(c, '\t' | '\n' | '\r'))
        .collect();
    if url.is_empty() {
        return Err(Error::Message("browser navigation requires a URL".into()));
    }
    if url.eq_ignore_ascii_case("about:blank") {
        return Ok("about:blank".to_string());
    }
    match url_scheme(&url) {
        // `example.com` or `localhost:8080/x`: a host, over https.
        None => Ok(format!("https://{url}")),
        Some(scheme) if scheme == "http" || scheme == "https" => Ok(url),
        Some(scheme) => Err(Error::Message(format!(
            "browser navigation allows only http://, https:// and about:blank, not `{scheme}:`"
        ))),
    }
}

/// The lowercased scheme `url` names, if it names one: the letters before
/// its first `:` when they form a scheme and what follows is not a port.
fn url_scheme(url: &str) -> Option<String> {
    let (scheme, rest) = url.split_once(':')?;
    let is_scheme = scheme.starts_with(|c: char| c.is_ascii_alphabetic())
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'));
    let after_port = rest.trim_start_matches(|c: char| c.is_ascii_digit());
    let is_port = after_port.len() < rest.len()
        && after_port
            .chars()
            .next()
            .is_none_or(|c| matches!(c, '/' | '?' | '#'));
    (is_scheme && !is_port).then(|| scheme.to_ascii_lowercase())
}

/// Whether a page now showing `url` may be read or driven for a caller:
/// what [`navigable_url`] admits, plus Chromium's own error page for a
/// navigation that failed.
fn readable_page_url(url: &str) -> bool {
    let url = url.trim();
    url.is_empty()
        || url.eq_ignore_ascii_case("about:blank")
        || url
            .to_ascii_lowercase()
            .starts_with("chrome-error://chromewebdata/")
        || matches!(url_scheme(url).as_deref(), Some("http" | "https"))
}

/// A CDP target id as Chromium writes it, so one is never a path or query in
/// a DevTools HTTP request.
fn checked_target_id(tid: &str) -> Result<&str, Error> {
    if !tid.is_empty() && tid.len() <= 64 && tid.bytes().all(|b| b.is_ascii_alphanumeric()) {
        Ok(tid)
    } else {
        Err(Error::Message(format!(
            "`{tid}` is not a browser target id"
        )))
    }
}

fn navigate_page(cdp: &mut CdpSession, url: &str) -> Result<(), Error> {
    let url = navigable_url(url)?;
    let result = cdp.call("Page.navigate", serde_json::json!({ "url": url }))?;
    if let Some(error) = result["errorText"].as_str().filter(|s| !s.is_empty()) {
        return Err(Error::Message(format!("Page.navigate: {error}")));
    }
    Ok(())
}

struct TakeoverPageSession {
    target_id: String,
    cdp: CdpSession,
}

#[cfg(test)]
fn dispatch_takeover_input(
    data: &Path,
    profile: &str,
    payload: &serde_json::Value,
) -> Result<serde_json::Value, Error> {
    dispatch_takeover_input_session(data, profile, payload, &mut None)
}

fn dispatch_takeover_input_session(
    data: &Path,
    profile: &str,
    payload: &serde_json::Value,
    session: &mut Option<TakeoverPageSession>,
) -> Result<serde_json::Value, Error> {
    validate_takeover_input(payload)?;
    let mut rt = load_runtime(data, profile)?
        .ok_or_else(|| Error::Message(format!("browser profile `{profile}` is not running")))?;
    touch_runtime(data, &mut rt)?;
    if session.as_ref().is_some_and(|page| {
        page.cdp.port != rt.cdp_port || rt.active_target_id.as_deref() != Some(&page.target_id)
    }) {
        drain_takeover_mouse(data, profile)?;
    }
    takeover_page_session(data, &mut rt, session)?;
    let page = session.as_mut().unwrap();
    page.cdp.takeover_mouse_owner =
        Some((takeover_mouse_path(data, profile), page.target_id.clone()));
    let cdp = &mut page.cdp;
    let result = dispatch_takeover_cdp_input(cdp, payload);
    // Reconnect for the next action, never replay a possibly committed input.
    if result.is_err() {
        *session = None;
    }
    result
}

// Frame capture and input each keep their own page connection. A slow capture
// must not hold the input lock, and a target/port change replaces either cache.
fn takeover_page_session<'a>(
    data: &Path,
    rt: &mut BrowserRuntimeState,
    session: &'a mut Option<TakeoverPageSession>,
) -> Result<&'a mut CdpSession, Error> {
    if session.as_ref().is_none_or(|s| {
        s.cdp.port != rt.cdp_port || rt.active_target_id.as_deref() != Some(&s.target_id)
    }) {
        let target = select_page_target(data, rt, None)?;
        let ws_url = target
            .ws_url
            .ok_or_else(|| Error::Message("browser page has no WebSocket debugger URL".into()))?;
        *session = Some(TakeoverPageSession {
            target_id: target.id,
            cdp: CdpSession::connect(rt.cdp_port, &ws_url)?,
        });
    }
    Ok(&mut session.as_mut().unwrap().cdp)
}

// Register before admitting input, not after a press. A persistent CDP
// registration installs the guard before scripts in replacement documents too.
fn register_takeover_cancel(cdp: &mut CdpSession) -> Result<(), Error> {
    if !cdp.takeover_cancel_registered {
        // Chromium acknowledges registration while Page is disabled, but does
        // not run it in replacement documents until this session enables Page.
        cdp.call("Page.enable", serde_json::json!({}))?;
        let result = cdp.call(
            "Page.addScriptToEvaluateOnNewDocument",
            serde_json::json!({
                "source": include_str!("browser_takeover_cancel.js"),
                "worldName": "anyagents-takeover-cancel",
                "runImmediately": true,
            }),
        )?;
        if result["identifier"].as_str().is_none_or(str::is_empty) {
            return Err(Error::Message(
                "cancellation registration not acknowledged".into(),
            ));
        }
        cdp.takeover_cancel_registered = true;
    }
    Ok(())
}

// Cancellation code stays in a separate world, including in cross-origin frames.
// Its capture listener shares DOM event propagation but no page globals.
fn eval_takeover_cancel_js(
    cdp: &mut CdpSession,
    frame_id: &str,
    expression: &str,
) -> Result<serde_json::Value, Error> {
    fn evaluate(
        cdp: &mut CdpSession,
        frame_id: &str,
        expression: &str,
    ) -> Result<serde_json::Value, Error> {
        let world = cdp.call(
            "Page.createIsolatedWorld",
            serde_json::json!({"frameId": frame_id, "worldName": "anyagents-takeover-cancel"}),
        )?;
        let context = world["executionContextId"]
            .as_u64()
            .ok_or_else(|| Error::Message("missing cancellation world".into()))?;
        eval_js_with_session(cdp, None, Some(context), expression)
    }
    if !cdp.takeover_cancel_children.contains_key(frame_id) {
        match evaluate(cdp, frame_id, expression) {
            Ok(value) => return Ok(value),
            Err(_) => {
                let mut child =
                    CdpSession::connect(cdp.port, &format!("/devtools/page/{frame_id}"))?;
                register_takeover_cancel(&mut child)?;
                cdp.takeover_cancel_children
                    .insert(frame_id.to_string(), child);
            }
        }
    }
    evaluate(
        cdp.takeover_cancel_children.get_mut(frame_id).unwrap(),
        frame_id,
        expression,
    )
}

// CDP has no mouse-cancel event. Admit a press only after every live frame
// acknowledges a persistent isolated guard. A later cancellation must never
// depend on frame discovery, script installation or an expiring listener.
fn prepare_takeover_mouse(cdp: &mut CdpSession) -> Result<(), Error> {
    register_takeover_cancel(cdp)?;
    // Snapshot both the frame tree and out-of-process iframe targets. Unlike
    // geometry discovery, cancellation cannot treat a failed census as empty.
    fn collect(node: &serde_json::Value, frames: &mut Vec<String>) -> Result<(), Error> {
        let id = node
            .pointer("/frame/id")
            .and_then(|v| v.as_str())
            .filter(|id| !id.is_empty())
            .ok_or_else(|| Error::Message("missing cancellation frame".into()))?;
        if !frames.iter().any(|frame| frame == id) {
            frames.push(id.to_string());
        }
        if let Some(children) = node.get("childFrames") {
            let children = children
                .as_array()
                .ok_or_else(|| Error::Message("invalid cancellation frame tree".into()))?;
            for child in children {
                collect(child, frames)?;
            }
        }
        Ok(())
    }
    let tree = cdp.call("Page.getFrameTree", serde_json::json!({}))?;
    let mut frames = Vec::new();
    collect(&tree["frameTree"], &mut frames)?;
    let targets = cdp.call("Target.getTargets", serde_json::json!({}))?;
    let targets = targets["targetInfos"]
        .as_array()
        .ok_or_else(|| Error::Message("missing cancellation target census".into()))?;
    for target in targets {
        if target["type"] == "iframe" {
            let id = target["targetId"]
                .as_str()
                .filter(|id| !id.is_empty())
                .ok_or_else(|| Error::Message("missing cancellation iframe target".into()))?;
            if !frames.iter().any(|frame| frame == id) {
                frames.push(id.to_string());
            }
            if !cdp.takeover_cancel_children.contains_key(id) {
                let mut child = CdpSession::connect(cdp.port, &format!("/devtools/page/{id}"))?;
                register_takeover_cancel(&mut child)?;
                cdp.takeover_cancel_children.insert(id.to_string(), child);
            }
        }
    }
    cdp.takeover_cancel_children
        .retain(|id, _| frames.contains(id));
    for frame in frames {
        if eval_takeover_cancel_js(cdp, &frame, include_str!("browser_takeover_cancel.js"))?
            != serde_json::json!(true)
        {
            return Err(Error::Message("cancellation listener not installed".into()));
        }
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct TakeoverHeldMouse {
    port: u16,
    target_id: String,
    payload: serde_json::Value,
}

fn takeover_mouse_path(data: &Path, profile: &str) -> PathBuf {
    profile_dir(data, profile).join("takeover-mouse.json")
}

fn read_takeover_mouse(path: &Path) -> Result<Option<TakeoverHeldMouse>, Error> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| Error::Message(e.to_string())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

// Written before native down under the profile ownership fence. A completion
// in another worker/process must drain this exact target before publishing it.
fn retain_takeover_mouse(cdp: &CdpSession, payload: &serde_json::Value) -> Result<(), Error> {
    if let Some((path, target_id)) = &cdp.takeover_mouse_owner {
        let held = TakeoverHeldMouse {
            port: cdp.port,
            target_id: target_id.clone(),
            payload: payload.clone(),
        };
        write_profile_json(path, &held)?;
    }
    Ok(())
}

fn drain_takeover_mouse(data: &Path, profile: &str) -> Result<(), Error> {
    let path = takeover_mouse_path(data, profile);
    if let Some(held) = read_takeover_mouse(&path)? {
        // A terminated browser has no surviving native press to release.
        if cdp_port_alive(held.port) {
            let mut cdp =
                CdpSession::connect(held.port, &format!("/devtools/page/{}", held.target_id))?;
            cancel_takeover_mouse(&mut cdp, &held.payload)?;
        }
        std::fs::remove_file(path)?;
    }
    Ok(())
}

fn cancel_takeover_mouse(
    cdp: &mut CdpSession,
    payload: &serde_json::Value,
) -> Result<serde_json::Value, Error> {
    let held = match &cdp.takeover_mouse_owner {
        Some((path, _)) => match read_takeover_mouse(path)? {
            Some(held) => Some(held),
            None => return Ok(serde_json::json!({"ok":true,"type":"mouseCancel"})),
        },
        None => None,
    };
    let payload = held.as_ref().map_or(payload, |held| &held.payload);
    // Viewer mouse events carry no keyboard modifiers. All four modifier bits
    // are reserved for this terminal release, which the prearmed guard observes
    // without consuming pointerup/mouseup or depending on clickCount heuristics.
    cdp.call(
        "Input.dispatchMouseEvent",
        serde_json::json!({
            "type": "mouseReleased",
            "x": payload["x"].as_f64().unwrap_or(0.0),
            "y": payload["y"].as_f64().unwrap_or(0.0),
            "button": payload["button"].as_str().unwrap_or("left"),
            "buttons": 0,
            "clickCount": 1,
            "modifiers": 15,
        }),
    )?;
    if let Some((path, _)) = &cdp.takeover_mouse_owner {
        std::fs::remove_file(path)?;
    }
    Ok(serde_json::json!({"ok": true, "type": "mouseCancel"}))
}

fn dispatch_takeover_cdp_input(
    cdp: &mut CdpSession,
    payload: &serde_json::Value,
) -> Result<serde_json::Value, Error> {
    let kind = validate_takeover_input(payload)?;
    match kind {
        "mouseCancel" => cancel_takeover_mouse(cdp, payload),
        "click" => {
            let x = payload.get("x").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let y = payload.get("y").and_then(|v| v.as_f64()).unwrap_or(0.0);
            dispatch_trusted_click(cdp, x, y)?;
            let focus = takeover_focused_field(cdp);
            Ok(serde_json::json!({ "ok": true, "type": "click", "x": x, "y": y, "focus": focus }))
        }
        "mouse" => {
            let event_type = payload
                .get("event_type")
                .and_then(|v| v.as_str())
                .unwrap_or("mouseMoved");
            let held = cdp
                .takeover_mouse_owner
                .as_ref()
                .map(|(path, _)| read_takeover_mouse(path))
                .transpose()?
                .flatten();
            if event_type == "mouseReleased" && cdp.takeover_mouse_owner.is_some() && held.is_none()
            {
                return Ok(serde_json::json!({"ok":true,"focus":takeover_focused_field(cdp)}));
            }
            let x = payload
                .get("x")
                .and_then(|v| v.as_f64())
                .filter(|v| v.is_finite())
                .or_else(|| held.as_ref().and_then(|h| h.payload["x"].as_f64()))
                .unwrap_or(0.0);
            let y = payload
                .get("y")
                .and_then(|v| v.as_f64())
                .filter(|v| v.is_finite())
                .or_else(|| held.as_ref().and_then(|h| h.payload["y"].as_f64()))
                .unwrap_or(0.0);
            let button = payload
                .get("button")
                .and_then(|v| v.as_str())
                .unwrap_or("left");
            let buttons = payload
                .get("buttons")
                .and_then(|v| v.as_u64())
                .unwrap_or(if event_type == "mouseReleased" { 0 } else { 1 });
            if event_type == "mousePressed" {
                prepare_takeover_mouse(cdp)?;
                // The viewer can receive its first down before any hover event.
                // Establish the pointer's target widget as a native move would,
                // without releasing other buttons already held during a chord.
                let pressed_button = match button {
                    "right" => 2,
                    "middle" => 4,
                    _ => 1,
                };
                cdp.call(
                    "Input.dispatchMouseEvent",
                    serde_json::json!({
                        "type": "mouseMoved", "x": x, "y": y,
                        "button": "none", "buttons": buttons & !pressed_button,
                    }),
                )?;
            }
            if event_type == "mousePressed" || (event_type == "mouseMoved" && held.is_some()) {
                let held_button = held
                    .as_ref()
                    .and_then(|h| h.payload["button"].as_str())
                    .unwrap_or(button);
                retain_takeover_mouse(cdp, &serde_json::json!({"x":x,"y":y,"button":held_button}))?;
            }
            cdp.call(
                "Input.dispatchMouseEvent",
                serde_json::json!({
                    "type": event_type,
                    "x": x,
                    "y": y,
                    "button": button,
                    "buttons": buttons,
                    "clickCount": 1,
                }),
            )?;
            if event_type == "mouseReleased"
                && let Some((path, _)) = &cdp.takeover_mouse_owner
            {
                std::fs::remove_file(path)?;
            }
            let focus = if event_type == "mouseReleased" {
                Some(takeover_focused_field(cdp))
            } else {
                None
            };
            Ok(
                serde_json::json!({ "ok": true, "type": "mouse", "event_type": event_type, "focus": focus }),
            )
        }
        "wheel" => {
            let x = payload.get("x").and_then(|v| v.as_f64()).unwrap_or(400.0);
            let y = payload.get("y").and_then(|v| v.as_f64()).unwrap_or(300.0);
            let delta_x = payload
                .get("delta_x")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let delta_y = payload
                .get("delta_y")
                .and_then(|v| v.as_f64())
                .unwrap_or(120.0);
            cdp.call(
                "Input.dispatchMouseEvent",
                serde_json::json!({
                    "type": "mouseWheel",
                    "x": x,
                    "y": y,
                    "deltaX": delta_x,
                    "deltaY": delta_y,
                }),
            )?;
            Ok(serde_json::json!({ "ok": true, "type": "wheel" }))
        }
        "key" => {
            dispatch_takeover_key(cdp, payload)?;
            Ok(serde_json::json!({ "ok":true, "type":"key", "focus":takeover_focused_field(cdp) }))
        }
        "clipboard" => {
            let selection = takeover_selection(cdp)?;
            if selection["blocked"].as_bool() == Some(true) {
                return Err(Error::Message(
                    "password fields cannot be copied or cut".into(),
                ));
            }
            let text = selection["text"].as_str().unwrap_or("");
            match payload["action"].as_str().unwrap_or("copy") {
                "copy" => Ok(serde_json::json!({
                    "ok":true, "text":text, "selection_key":selection["selection_key"]
                })),
                "cut" => {
                    // The asynchronous local clipboard write must not authorize deleting
                    // another field with identical text. Check node/range identity and
                    // delete in one JavaScript task, with no intervening CDP key event.
                    let expected = payload.to_string();
                    let expression = format!(
                        r#"(() => {{
                            const selection = {TAKEOVER_SELECTION_JS};
                            if (selection.frame_focused || !selection.focused) return selection;
                            const expected = {expected};
                            if (selection.blocked || !expected.selection_key ||
                                selection.selection_key !== expected.selection_key ||
                                selection.text !== expected.expected_text)
                                return {{focused:true,ok:false,error:'Focus or selection changed. Cut cancelled; select the text again.'}};
                            let el = document.activeElement;
                            try {{ while (el?.tagName === 'IFRAME' && el.contentDocument) el = el.contentDocument.activeElement; }} catch (_) {{}}
                            const doc = el?.ownerDocument || document;
                            if (selection.text && !doc.execCommand('delete'))
                                return {{focused:true,ok:false,error:'Cut cancelled; the selected text could not be deleted.'}};
                            return {{focused:true,ok:true,type:'cut'}};
                        }})()"#
                    );
                    takeover_selection_expression(cdp, &expression)
                }
                _ => Err(Error::Message("unsupported clipboard action".into())),
            }
        }
        "text" | "paste" => {
            let text = payload["text"].as_str().unwrap_or("");
            if !text.is_empty() {
                cdp.call("Input.insertText", serde_json::json!({ "text":text }))?;
            }
            let submit = payload["submit"].as_bool().unwrap_or(false);
            if submit {
                dispatch_takeover_key(cdp, &serde_json::json!({"key":"Enter"}))?;
            }
            // Never echo typed or pasted values, including password text.
            Ok(
                serde_json::json!({ "ok":true, "type":kind, "submitted":submit, "focus":takeover_focused_field(cdp) }),
            )
        }
        "navigate" => {
            if let Some(u) = payload
                .get("url")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
            {
                navigate_page(cdp, u)?;
            }
            Ok(serde_json::json!({ "ok": true, "type": "navigate" }))
        }
        "reload" => {
            cdp.call("Page.reload", serde_json::json!({}))?;
            Ok(serde_json::json!({ "ok": true, "type": "reload" }))
        }
        other => Err(Error::Message(format!(
            "unsupported takeover input type `{other}`"
        ))),
    }
}

fn write_http_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
) -> std::io::Result<()> {
    write_http_response_with_headers(stream, status, content_type, body, "")
}

fn write_http_response_with_headers(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
    extra_headers: &str,
) -> std::io::Result<()> {
    let header = format!(
        "HTTP/1.1 {status}\r\n{extra_headers}Content-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-store, no-cache, must-revalidate\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()
}

#[derive(Clone, Copy)]
struct TakeoverFrameSize {
    width: u16,
    height: u16,
}

fn jpeg_size(bytes: &[u8]) -> Option<TakeoverFrameSize> {
    if !bytes.starts_with(&[0xff, 0xd8]) {
        return None;
    }
    let mut offset = 2;
    while offset + 4 <= bytes.len() {
        if bytes[offset] != 0xff {
            return None;
        }
        let marker = bytes[offset + 1];
        let len = u16::from_be_bytes([bytes[offset + 2], bytes[offset + 3]]) as usize;
        if len < 2 || offset + 2 + len > bytes.len() {
            return None;
        }
        if matches!(marker, 0xc0..=0xc3 | 0xc5..=0xc7 | 0xc9..=0xcb | 0xcd..=0xcf) && len >= 7 {
            return Some(TakeoverFrameSize {
                height: u16::from_be_bytes([bytes[offset + 5], bytes[offset + 6]]),
                width: u16::from_be_bytes([bytes[offset + 7], bytes[offset + 8]]),
            });
        }
        offset += 2 + len;
    }
    None
}

fn decode_takeover_payload(bytes: &[u8]) -> Result<serde_json::Value, Error> {
    let value: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|err| Error::Message(format!("invalid takeover JSON: {err}")))?;
    if !value.is_object() {
        return Err(Error::Message("takeover payload must be an object".into()));
    }
    Ok(value)
}

fn validate_takeover_input(payload: &serde_json::Value) -> Result<&str, Error> {
    let kind = payload
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Message("takeover input requires type".into()))?;
    if matches!(kind, "click" | "mouse") && (!payload["x"].is_number() || !payload["y"].is_number())
    {
        return Err(Error::Message(
            "pointer input requires numeric x and y".into(),
        ));
    }
    if kind == "navigate" && payload["url"].as_str().is_none_or(|s| s.is_empty()) {
        return Err(Error::Message("navigation requires url".into()));
    }
    Ok(kind)
}

#[derive(Default)]
struct TakeoverServerShared {
    last_frame: Mutex<Option<TakeoverFrameSize>>,
    input: Mutex<Option<TakeoverPageSession>>,
    frame: Mutex<Option<TakeoverPageSession>>,
    // Readers (frame/state/input) remain concurrent, but completion drains
    // admitted requests before revoking control. Slow HTTP readers hold no lock.
    dispatch: RwLock<()>,
    // Serialize read-modify-publish of viewport/heartbeat/completion only.
    control: Mutex<()>,
}

// Bounded parallel clients keep screenshot capture, state inspection and slow
// HTTP readers off the input path. A persistent, independently locked CDP input
// session preserves a press/move/release trajectory across HTTP requests.
fn spawn_takeover_client(
    stream: TcpStream,
    data: PathBuf,
    profile: String,
    token: String,
    shared: Arc<TakeoverServerShared>,
    active: Arc<std::sync::atomic::AtomicUsize>,
) {
    use std::sync::atomic::Ordering;
    if active.fetch_add(1, Ordering::AcqRel) >= 32 {
        active.fetch_sub(1, Ordering::AcqRel);
        return;
    }
    let counter = active.clone();
    let result = std::thread::Builder::new()
        .name(format!("takeover-client-{profile}"))
        .spawn(move || {
            struct ActiveClient(Arc<std::sync::atomic::AtomicUsize>);
            impl Drop for ActiveClient {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::AcqRel);
                }
            }
            let _active = ActiveClient(counter);
            let _ = stream.set_nonblocking(false);
            handle_takeover_http_client(stream, &data, &profile, &token, &shared);
        });
    if result.is_err() {
        active.fetch_sub(1, Ordering::AcqRel);
    }
}

fn takeover_request_active(data: &Path, profile: &str, token: &str) -> Result<bool, Error> {
    Ok(load_takeover_state(data, profile)?.is_some_and(|state| {
        constant_time_eq(&state.token, token)
            && state.holder == "human"
            && !state.completed
            && now_ms() < state.expires_at_ms
    }))
}

fn handle_takeover_http_client(
    mut stream: TcpStream,
    data: &Path,
    profile: &str,
    expected_token: &str,
    shared: &TakeoverServerShared,
) -> bool {
    match handle_takeover_http_client_inner(&mut stream, data, profile, expected_token, shared) {
        Ok(done) => done,
        Err(err) => {
            let body = serde_json::json!({ "ok":false, "error":err.to_string() }).to_string();
            let _ = write_http_response(
                &mut stream,
                "400 Bad Request",
                "application/json",
                body.as_bytes(),
            );
            false
        }
    }
}

fn handle_takeover_http_client_inner(
    stream: &mut TcpStream,
    data: &Path,
    profile: &str,
    expected_token: &str,
    shared: &TakeoverServerShared,
) -> Result<bool, Error> {
    load_takeover_state(data, profile)?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(3)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let mut buf = Vec::with_capacity(2048);
    let mut chunk = [0u8; 1024];
    let header_end = loop {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return Ok(false),
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    break pos + 4;
                }
                if buf.len() > 65536 {
                    return Ok(false);
                }
            }
        }
    };
    let header_str = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let first_line = header_str.lines().next().unwrap_or("");
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or("GET");
    let raw_path = parts.next().unwrap_or("/");
    let path_only = raw_path.split('?').next().unwrap_or("/");

    let content_len = match header_str.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.trim()
            .eq_ignore_ascii_case("content-length")
            .then_some(value.trim())
    }) {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| Error::Message("invalid Content-Length".into()))?,
        None => 0,
    };
    if content_len > 131_072 {
        return Err(Error::Message("takeover request body is too large".into()));
    }
    while buf.len() < header_end + content_len {
        match stream.read(&mut chunk) {
            Ok(0) => return Err(Error::Message("incomplete takeover request body".into())),
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(err) => return Err(err.into()),
        }
    }
    let body_bytes = if buf.len() >= header_end {
        &buf[header_end..buf.len().min(header_end + content_len)]
    } else {
        &[][..]
    };

    if path_only == "/healthz" {
        let _ = write_http_response(stream, "200 OK", "application/json", b"{\"ok\":true}");
        return Ok(false);
    }

    let prefix = format!("/takeover/{expected_token}");
    let subpath = match path_only.strip_prefix(&prefix) {
        Some(s) if s.is_empty() || s.starts_with('/') => s,
        _ => {
            let matched = path_only
                .strip_prefix("/takeover/")
                .map(|rest| rest.split('/').next().unwrap_or(""))
                .is_some_and(|tok| constant_time_eq(tok, expected_token));
            if !matched {
                let _ = write_http_response(
                    stream,
                    "403 Forbidden",
                    "application/json",
                    b"{\"error\":\"invalid takeover token\"}",
                );
                return Ok(false);
            }
            ""
        }
    };

    // Accepting TCP or recognizing a captured URL is not admission. A worker
    // may have waited on headers while this takeover completed or was replaced.
    let completing = method == "POST" && subpath == "/complete";
    let _ownership = takeover_ownership_lock(data, profile, completing)?;
    let _completion = completing.then(|| shared.dispatch.write().unwrap());
    let _dispatch = (!completing).then(|| shared.dispatch.read().unwrap());
    if !takeover_request_active(data, profile, expected_token)? {
        let _ = write_http_response(
            stream,
            "403 Forbidden",
            "application/json",
            b"{\"error\":\"takeover session is no longer active\"}",
        );
        return Ok(false);
    }

    Ok(match (method, subpath) {
        ("GET", "" | "/") => {
            let reason = load_takeover_state(data, profile)?.and_then(|s| s.reason);
            let html = render_takeover_html(profile, expected_token, reason.as_deref());
            let _ = write_http_response(
                stream,
                "200 OK",
                "text/html; charset=utf-8",
                html.as_bytes(),
            );
            false
        }
        ("GET", "/frame.jpg") => {
            let Some(mut rt) = load_runtime(data, profile)? else {
                let _ = write_http_response(
                    stream,
                    "503 Service Unavailable",
                    "application/json",
                    b"{\"error\":\"browser not running\"}",
                );
                return Ok(false);
            };
            let mut session = shared.frame.lock().unwrap();
            let Ok(cdp) = takeover_page_session(data, &mut rt, &mut session) else {
                let _ = write_http_response(
                    stream,
                    "503 Service Unavailable",
                    "application/json",
                    b"{\"error\":\"no page target\"}",
                );
                return Ok(false);
            };
            let metrics = eval_js(cdp, TAKEOVER_METRICS_JS).unwrap_or(serde_json::Value::Null);
            if let Ok(shot) = cdp.call(
                "Page.captureScreenshot",
                serde_json::json!({
                    "format": "jpeg",
                    "quality": 75,
                    "optimizeForSpeed": true,
                    "captureBeyondViewport": false,
                }),
            ) && let Some(b64) = shot.get("data").and_then(|v| v.as_str())
                && let Ok(jpeg_bytes) = decode_base64(b64)
            {
                *shared.last_frame.lock().unwrap() = jpeg_size(&jpeg_bytes);
                let dpr = metrics["device_scale_factor"].as_f64().unwrap_or(1.0);
                let width = metrics["width"].as_u64().unwrap_or(1280);
                let height = metrics["height"].as_u64().unwrap_or(800);
                let _ = write_http_response_with_headers(
                    stream,
                    "200 OK",
                    "image/jpeg",
                    &jpeg_bytes,
                    &format!(
                        "X-Remote-Dpr: {dpr}\r\nX-Remote-Width: {width}\r\nX-Remote-Height: {height}\r\n"
                    ),
                );
            } else {
                *session = None;
                let _ = write_http_response(
                    stream,
                    "500 Internal Server Error",
                    "application/json",
                    b"{\"error\":\"capture failed\"}",
                );
            }
            false
        }
        ("GET", "/state") => {
            let _control = shared.control.lock().unwrap();
            let last_frame = *shared.last_frame.lock().unwrap();
            let mut url = String::new();
            let mut title = String::new();
            let mut cap = CaptchaDetection::default();
            let mut metrics = serde_json::Value::Null;
            if let Some(mut rt) = load_runtime(data, profile)?
                && let Ok(target) = select_page_target(data, &mut rt, None)
            {
                url = target.url.clone();
                title = target.title.clone();
                if let Some(ws_url) = target.ws_url
                    && let Ok(mut cdp) = CdpSession::connect(rt.cdp_port, &ws_url)
                {
                    cap = detect_captcha(&mut cdp);
                    metrics =
                        eval_js(&mut cdp, TAKEOVER_METRICS_JS).unwrap_or(serde_json::Value::Null);
                    if let Ok(u) = eval_js(&mut cdp, "location.href")
                        && let Some(s) = u.as_str()
                    {
                        url = s.to_string();
                    }
                    if let Ok(t) = eval_js(&mut cdp, "document.title || ''")
                        && let Some(s) = t.as_str()
                    {
                        title = s.to_string();
                    }
                }
            }
            let mut st = load_takeover_state(data, profile)?;
            if let Some(st) = st.as_mut()
                && st.original_viewport.is_some()
            {
                st.viewport_heartbeat_ms = Some(now_ms());
                save_takeover_state(data, st)?;
            }
            let dpr = metrics["device_scale_factor"].as_f64().unwrap_or(1.0);
            let body = serde_json::json!({
                "profile": profile,
                "url": url,
                "title": title,
                "captcha_detected": cap.detected,
                "captcha_kind": cap.kind,
                "captcha_summary": cap.summary,
                "holder": st.as_ref().map(|s| s.holder.as_str()).unwrap_or("agent"),
                "completed": st.as_ref().is_none_or(|s| s.completed),
                "expires_at_ms": st.as_ref().map(|s| s.expires_at_ms),
                "phone_layout": st.as_ref().is_some_and(|s| s.original_viewport.is_some()),
                "viewport": metrics,
                "device_scale_factor": dpr,
                "natural_width": last_frame.map(|f| f.width),
                "natural_height": last_frame.map(|f| f.height),
            });
            let bytes = serde_json::to_vec(&body).unwrap_or_default();
            let _ = write_http_response(stream, "200 OK", "application/json", &bytes);
            false
        }
        ("POST", "/viewport") => {
            let _control = shared.control.lock().unwrap();
            let result = decode_takeover_payload(body_bytes)
                .and_then(|payload| set_takeover_viewport(data, profile, &payload));
            let (status, body) = match result {
                Ok(value) => ("200 OK", value),
                Err(err) => (
                    "400 Bad Request",
                    serde_json::json!({ "ok": false, "error": err.to_string() }),
                ),
            };
            let _ = write_http_response(
                stream,
                status,
                "application/json",
                &serde_json::to_vec(&body).unwrap_or_default(),
            );
            false
        }
        ("POST", "/input") => {
            let mut session = shared.input.lock().unwrap();
            if !takeover_request_active(data, profile, expected_token)? {
                let _ = write_http_response(
                    stream,
                    "403 Forbidden",
                    "application/json",
                    b"{\"error\":\"takeover session is no longer active\"}",
                );
                return Ok(false);
            }
            match decode_takeover_payload(body_bytes).and_then(|payload| {
                dispatch_takeover_input_session(data, profile, &payload, &mut session)
            }) {
                Ok(res) => {
                    let bytes = serde_json::to_vec(&res).unwrap_or_default();
                    let _ = write_http_response(stream, "200 OK", "application/json", &bytes);
                }
                Err(err) => {
                    let bytes = serde_json::to_vec(&serde_json::json!({
                        "ok": false,
                        "error": err.to_string(),
                    }))
                    .unwrap_or_default();
                    let _ =
                        write_http_response(stream, "400 Bad Request", "application/json", &bytes);
                }
            }
            false
        }
        ("POST", "/complete") => {
            let _control = shared.control.lock().unwrap();
            let payload = decode_takeover_payload(body_bytes)?;
            let note = payload
                .get("note")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let done = complete_takeover_session_owned(data, profile, note.as_deref())?;
            let bytes = serde_json::to_vec(&done).unwrap_or_default();
            let _ = write_http_response(stream, "200 OK", "application/json", &bytes);
            done["completed"].as_bool().unwrap_or(false)
        }
        _ => {
            let _ = write_http_response(
                stream,
                "404 Not Found",
                "application/json",
                b"{\"error\":\"not found\"}",
            );
            false
        }
    })
}

fn complete_takeover_session(
    data: &Path,
    profile: &str,
    note: Option<&str>,
) -> Result<serde_json::Value, Error> {
    let _ownership = takeover_ownership_lock(data, profile, true)?;
    complete_takeover_session_owned(data, profile, note)
}

fn complete_takeover_session_owned(
    data: &Path,
    profile: &str,
    note: Option<&str>,
) -> Result<serde_json::Value, Error> {
    drain_takeover_mouse(data, profile)?;
    let now = now_ms();
    let mut current_url = String::new();
    if let Some(mut rt) = load_runtime(data, profile)?
        && let Ok(target) = select_page_target(data, &mut rt, None)
    {
        current_url = target.url.clone();
        if let Some(ws_url) = target.ws_url
            && let Ok(mut cdp) = CdpSession::connect(rt.cdp_port, &ws_url)
            && let Ok(u) = eval_js(&mut cdp, "location.href")
            && let Some(s) = u.as_str()
        {
            current_url = s.to_string();
        }
    }
    if let Some(mut st) = load_takeover_state(data, profile)? {
        if !st.completed {
            restore_takeover_viewport(&mut st)?;
            if let Some(tpid) = st.tunnel_pid.take() {
                stop_process_group(tpid);
            }
            st.holder = "agent".to_string();
            st.completed = true;
            st.notification_pending = true;
            st.completed_at_ms = Some(now);
            st.completion_note = note
                .map(str::to_string)
                .or_else(|| Some("completed by user".to_string()));
            save_takeover_state(data, &st)?;
        }
        if st.notification_pending {
            notify_agent_takeover_completed(
                data,
                st.owner_agent.as_deref(),
                profile,
                &current_url,
                st.completion_note.as_deref(),
                st.started_at_ms,
            )
            .map_err(|err| {
                Error::Message(format!(
                    "takeover completed; owner notification pending: {err}"
                ))
            })?;
            st.notification_pending = false;
            save_takeover_state(data, &st)?;
        }
        Ok(serde_json::json!({
            "ok": true,
            "profile": profile,
            "holder": "agent",
            "completed": true,
            "completed_at_ms": st.completed_at_ms,
            "url": current_url,
        }))
    } else {
        Ok(serde_json::json!({
            "ok": true,
            "profile": profile,
            "holder": "agent",
            "completed": true,
            "url": current_url,
        }))
    }
}

fn start_or_reuse_takeover_session(
    data: &Path,
    profile: &str,
    owner_agent: Option<&str>,
    reason: Option<&str>,
    ttl_ms: Option<u64>,
    want_tunnel: Option<bool>,
    initial_captcha: &CaptchaDetection,
) -> Result<BrowserTakeoverState, Error> {
    let _ownership = takeover_ownership_lock(data, profile, true)?;
    let now = now_ms();
    let duration_ms = ttl_ms
        .unwrap_or(DEFAULT_TAKEOVER_TTL_MS)
        .clamp(5_000, 3_600_000) as i64;
    if let Some(mut existing) = load_takeover_state(data, profile)?
        && existing.holder == "human"
        && !existing.completed
        && now < existing.expires_at_ms
        && cdp_port_alive(existing.port)
    {
        existing.expires_at_ms = now + duration_ms;
        if let Some(r) = reason.filter(|s| !s.trim().is_empty()) {
            existing.reason = Some(r.to_string());
        }
        save_takeover_state(data, &existing)?;
        return Ok(existing);
    }

    if load_takeover_state(data, profile)?.is_some_and(|s| s.notification_pending) {
        complete_takeover_session_owned(data, profile, None)?;
    }
    cleanup_takeover_session_owned(data, profile)?;

    let listener = TcpListener::bind("0.0.0.0:0").or_else(|_| TcpListener::bind("127.0.0.1:0"))?;
    let port = listener.local_addr()?.port();
    let _ = listener.set_nonblocking(true);
    let token = generate_takeover_token()?;
    let local_url = format!("http://127.0.0.1:{port}/takeover/{token}");

    let allow_tunnel = want_tunnel.unwrap_or_else(|| {
        std::env::var_os("ANYAGENTS_FAKE_MODE").is_none() && !crate::sandbox::is_isolated_test_env()
    });
    let (public_url, tunnel_pid) = if allow_tunnel {
        spawn_cloudflared_quick_tunnel(port, &token)
    } else {
        (None, None)
    };
    let takeover_url = public_url.clone().unwrap_or_else(|| local_url.clone());

    let state = BrowserTakeoverState {
        profile: profile.to_string(),
        owner_agent: owner_agent.map(str::to_string),
        token: token.clone(),
        port,
        local_url,
        public_url,
        takeover_url,
        reason: reason.map(str::to_string),
        holder: "human".to_string(),
        completed: false,
        notification_pending: false,
        completed_at_ms: None,
        completion_note: None,
        started_at_ms: now,
        expires_at_ms: now + duration_ms,
        tunnel_pid,
        initial_captcha_detected: initial_captcha.detected,
        initial_captcha_kind: initial_captcha.kind.clone(),
        original_viewport: None,
        viewport_heartbeat_ms: None,
    };
    if let Err(err) = save_takeover_state(data, &state) {
        if let Some(pid) = state.tunnel_pid {
            stop_process_group(pid);
        }
        return Err(err);
    }

    let data_dir = data.to_path_buf();
    let prof = profile.to_string();
    let tok = token;
    std::thread::Builder::new()
        .name(format!("anyagents-takeover-{prof}"))
        .spawn(move || {
            let shared = Arc::new(TakeoverServerShared::default());
            let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            loop {
                let now = now_ms();
                // Never wait for state inspection in the accept loop.
                if let Ok(_control) = shared.control.try_lock() {
                    let _ = reap_disconnected_takeover_viewport(&data_dir, &prof, now);
                }
                let cur = load_takeover_state(&data_dir, &prof);
                if matches!(cur, Ok(None)) { break; }
                if let Ok(Some(cur)) = cur
                    && (cur.token != tok || cur.completed || now >= cur.expires_at_ms) {
                    if cur.token == tok && !cur.completed && now >= cur.expires_at_ms {
                        let _ = reap_expired_takeover(&data_dir, &prof, now);
                        if !matches!(load_takeover_state(&data_dir, &prof), Ok(Some(s)) if s.completed) {
                            std::thread::sleep(Duration::from_millis(100));
                            continue;
                        }
                    }
                    break;
                }
                match listener.accept() {
                    Ok((stream, _)) => {
                        spawn_takeover_client(
                            stream,
                            data_dir.clone(),
                            prof.clone(),
                            tok.clone(),
                            shared.clone(),
                            active.clone(),
                        );
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        wait_acceptable(&listener, TAKEOVER_STATE_CHECK);
                    }
                    Err(_) => break,
                }
            }
        })
        .map_err(|e| Error::Message(format!("failed to spawn takeover server thread: {e}")))?;

    Ok(state)
}

/// How often an idle takeover accept loop rechecks its session state:
/// completion, expiry and a viewer's lost heartbeat. A connection ends the wait
/// at once.
const TAKEOVER_STATE_CHECK: Duration = Duration::from_secs(1);

/// Block until `listener` has a connection to accept or `timeout` passes.
fn wait_acceptable(listener: &TcpListener, timeout: Duration) {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        let mut fd = libc::pollfd {
            fd: listener.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let timeout_ms = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);
        // SAFETY: one valid pollfd that outlives the call.
        unsafe { libc::poll(&mut fd, 1, timeout_ms) };
    }
    #[cfg(not(unix))]
    {
        let _ = listener;
        std::thread::sleep(timeout);
    }
}

fn wait_for_takeover_completion(
    data: &Path,
    profile: &str,
    wait_ms: u64,
) -> Result<(Option<BrowserTakeoverState>, CaptchaDetection), Error> {
    let deadline = std::time::Instant::now() + Duration::from_millis(wait_ms.clamp(100, 600_000));
    let mut consecutive_cleared = 0u32;
    loop {
        let now = now_ms();
        reap_expired_takeover(data, profile, now)?;
        let st = load_takeover_state(data, profile)?;
        let mut cap = CaptchaDetection::default();
        if let Some(mut rt) = load_runtime(data, profile)?
            && let Ok(target) = select_page_target(data, &mut rt, None)
            && let Some(ws_url) = target.ws_url
            && let Ok(mut cdp) = CdpSession::connect(rt.cdp_port, &ws_url)
        {
            cap = detect_captcha(&mut cdp);
        }
        let Some(ref cur) = st else {
            return Ok((None, cap));
        };
        if cur.notification_pending {
            return Err(Error::Message(
                "takeover completed; owner notification pending; retry completion".into(),
            ));
        }
        if cur.completed || cur.holder != "human" {
            return Ok((st, cap));
        }
        if cur.initial_captcha_detected {
            if !cap.detected {
                consecutive_cleared += 1;
                if consecutive_cleared >= 2 {
                    complete_takeover_session(
                        data,
                        profile,
                        Some("CAPTCHA challenge automatically detected as solved"),
                    )?;
                    return Ok((load_takeover_state(data, profile)?, cap));
                }
            } else {
                consecutive_cleared = 0;
            }
        }
        if std::time::Instant::now() >= deadline {
            return Ok((st, cap));
        }
        std::thread::sleep(Duration::from_millis(400));
    }
}

fn check_human_takeover_fence(data: &Path, profile: &str, action: &str) -> Result<(), Error> {
    let is_mutating = matches!(
        action,
        "open"
            | "goto"
            | "navigate"
            | "click"
            | "fill"
            | "type"
            | "input"
            | "press"
            | "key"
            | "hold"
            | "key_down"
            | "keydown"
            | "key_up"
            | "keyup"
            | "release"
            | "release_keys"
            | "eval"
            | "evaluate"
            | "js"
    );
    if !is_mutating {
        return Ok(());
    }
    let now = now_ms();
    reap_expired_takeover(data, profile, now)?;
    if let Some(st) = load_takeover_state(data, profile)?
        && st.holder == "human"
        && !st.completed
        && now < st.expires_at_ms
        && cdp_port_alive(st.port)
    {
        return Err(Error::Message(format!(
            "CONTROL_HELD_BY_HUMAN: browser profile `{profile}` is currently held by a human takeover session ({}). Wait for the user to finish, call `browser({{ \"action\": \"wait_for_user\", \"profile\": \"{profile}\" }})`, or call `browser({{ \"action\": \"release_takeover\", \"profile\": \"{profile}\" }})` to reclaim control.",
            st.takeover_url
        )));
    }
    Ok(())
}

fn capture_snapshot(
    data: &Path,
    profile: &str,
    cdp: &mut CdpSession,
) -> Result<serde_json::Value, Error> {
    let mut val = eval_js(cdp, &snapshot_js_with_offset(1))?;
    let mut all_elements: Vec<InteractiveRef> =
        serde_json::from_value(val.get("elements").cloned().unwrap_or_default())
            .unwrap_or_default();
    let mut combined_text = val
        .get("text")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();

    for cf in discover_child_frames(cdp) {
        let next_idx = all_elements.len() + 1;
        if next_idx > 160 {
            break;
        }
        let Ok(fval) =
            eval_js_in_frame(cdp, Some(&cf.frame_id), &snapshot_js_with_offset(next_idx))
        else {
            continue;
        };
        let furl = fval
            .get("url")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or(&cf.url)
            .to_string();
        if let Some(ftext) = fval
            .get("text")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            if !combined_text.is_empty() {
                combined_text.push_str("\n\n");
            }
            combined_text.push_str(&format!("[iframe: {furl}]\n{ftext}"));
        }
        if let Ok(felems) = serde_json::from_value::<Vec<InteractiveRef>>(
            fval.get("elements").cloned().unwrap_or_default(),
        ) {
            for mut el in felems {
                el.x += cf.offset_x;
                el.y += cf.offset_y;
                el.frame_id = Some(cf.frame_id.clone());
                el.frame_url = Some(furl.clone());
                all_elements.push(el);
            }
        }
    }

    let captcha = detect_captcha(cdp);
    val["text"] = serde_json::Value::String(combined_text);
    let snapshot_id = crate::computer::generate_snapshot_id();
    val["snapshot_id"] = serde_json::json!(snapshot_id);
    val["elements"] = qualified_elements(&snapshot_id, &all_elements);
    val["captcha_detected"] = serde_json::Value::Bool(captcha.detected);
    if let Some(kind) = captcha.kind {
        val["captcha_kind"] = serde_json::Value::String(kind);
    }
    if let Some(summary) = captcha.summary {
        val["captcha_summary"] = serde_json::Value::String(summary);
    }
    if captcha.detected {
        val["takeover_hint"] = serde_json::Value::String(format!(
            "Human verification / CAPTCHA detected on profile `{profile}`. Call `browser({{ \"action\": \"takeover\", \"profile\": \"{profile}\", \"reason\": \"solve CAPTCHA\" }})` and send `takeover_url` (and `screenshot_path`) to the user."
        ));
    }

    let stored = StoredRefs {
        snapshot_id,
        url: val
            .get("url")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        captured_at_ms: now_ms(),
        elements: all_elements,
    };
    write_profile_json(&profile_dir(data, profile).join("refs.json"), &stored)?;
    Ok(val)
}

fn qualified_elements(snapshot_id: &str, elements: &[InteractiveRef]) -> serde_json::Value {
    let mut value = serde_json::to_value(elements).unwrap_or_default();
    if let Some(elements) = value.as_array_mut() {
        for el in elements {
            let short_ref = el["ref"]
                .as_str()
                .unwrap_or_default()
                .trim_start_matches('@');
            el["qualified_ref"] = serde_json::json!(format!("@{snapshot_id}:{short_ref}"));
        }
    }
    value
}

#[derive(Debug)]
struct ResolvedTarget {
    css_selector: String,
    fallback_xy: Option<(f64, f64)>,
    frame_id: Option<String>,
}

fn resolve_selector(data: &Path, profile: &str, raw_target: &str) -> Result<ResolvedTarget, Error> {
    let trimmed = raw_target.trim();
    if let Some(raw_ref) = trimmed.strip_prefix('@') {
        let stale = |reason: &str| {
            Error::Message(format!(
                "STALE_REF: {reason}; call tools.browser({{action: 'snapshot'}}) for fresh @<snapshot_id>:e<N> refs"
            ))
        };
        let refs_path = profile_dir(data, profile).join("refs.json");
        let stored = read_profile_json::<StoredRefs>(&refs_path)?
            .ok_or_else(|| stale("no active browser snapshot"))?;
        let element = if let Some((snapshot_id, element)) = raw_ref.split_once(':') {
            if snapshot_id.is_empty() || snapshot_id != stored.snapshot_id {
                return Err(stale("reference belongs to an older or different snapshot"));
            }
            element
        } else {
            raw_ref
        };
        if !element
            .strip_prefix('e')
            .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
        {
            return Err(stale("malformed element reference"));
        }
        let short_ref = format!("@{element}");
        let found = stored
            .elements
            .iter()
            .find(|e| e.r#ref == short_ref)
            .ok_or_else(|| stale("element is absent from the active snapshot"))?;
        return Ok(ResolvedTarget {
            css_selector: format!("[data-anyagents-ref=\"{}\"], {}", short_ref, found.selector),
            fallback_xy: Some((found.x, found.y)),
            frame_id: found.frame_id.clone(),
        });
    }
    Ok(ResolvedTarget {
        css_selector: trimmed.to_string(),
        fallback_xy: None,
        frame_id: None,
    })
}

/// Inherit the provider's documented HOME, OP_CONFIG_DIR and
/// OP_SERVICE_ACCOUNT_TOKEN settings while marking the child as an in-memory
/// daemon secret read (`DESK_SECRET_READ=1`) with piped stdio so output-guard
/// shims permit consumed pipe capture without exposing plaintext.
fn configure_op_command(cmd: &mut Command) {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("DESK_SECRET_READ", "1");
}

fn build_op_secret_command(op_program: &std::ffi::OsStr, args: &[&str]) -> Command {
    let mut cmd = Command::new(op_program);
    cmd.args(args);
    configure_op_command(&mut cmd);
    cmd
}

/// Resolve a secret reference (`op://...`, `op-otp://...`, `secret://...`) in memory
/// without ever echoing the plaintext secret in errors, logs, or responses.
fn resolve_fill_value(
    data: &Path,
    raw_text: Option<&str>,
    secret_ref: Option<&str>,
) -> Result<(String, Option<String>), Error> {
    resolve_fill_value_with_op(data, raw_text, secret_ref, None)
}

fn resolve_op_program(op_program: Option<&std::ffi::OsStr>) -> Result<std::ffi::OsString, Error> {
    match op_program {
        Some(bin) => Ok(bin.to_os_string()),
        None => anyagents_toolchain::tool("op", "browser.secret_ref")
            .map(|tool| tool.path.into_os_string())
            .map_err(|err| Error::Message(err.to_string())),
    }
}

fn resolve_fill_value_with_op(
    data: &Path,
    raw_text: Option<&str>,
    secret_ref: Option<&str>,
    op_program: Option<&std::ffi::OsStr>,
) -> Result<(String, Option<String>), Error> {
    let candidate = secret_ref
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or_else(|| raw_text.map(str::trim).filter(|s| !s.is_empty()))
        .unwrap_or("");

    if candidate.starts_with("op://") {
        let parts: Vec<&str> = candidate
            .trim_start_matches("op://")
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();
        if parts.len() < 3 {
            return Err(Error::Message(
                "invalid 1Password secret reference format (expected op://<vault>/<item>/<field>)"
                    .into(),
            ));
        }
        let op_bin = resolve_op_program(op_program)?;
        let mut cmd = build_op_secret_command(&op_bin, &["read", "--no-newline", candidate]);
        let out = cmd
            .output()
            .map_err(|_| Error::Message("failed to invoke `op` CLI for secret reference".into()))?;
        if !out.status.success() {
            return Err(Error::Message(format!(
                "1Password secret resolution failed (exit code {})",
                out.status.code().unwrap_or(-1)
            )));
        }
        let secret = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if secret.is_empty() {
            return Err(Error::Message(
                "1Password secret resolution returned an empty value".into(),
            ));
        }
        return Ok((secret, Some(candidate.to_string())));
    }

    if let Some(rest) = candidate.strip_prefix("op-otp://") {
        let trimmed = rest.trim_matches('/');
        if trimmed.is_empty() {
            return Err(Error::Message(
                "invalid 1Password OTP reference format (expected op-otp://<vault>/<item>)".into(),
            ));
        }
        let mut cmd = if let Some((vault, item)) = trimmed.split_once('/') {
            let vault = vault.trim().trim_matches('/');
            let item = item.trim().trim_matches('/');
            if vault.is_empty() || item.is_empty() {
                return Err(Error::Message(
                    "invalid 1Password OTP reference format (expected op-otp://<vault>/<item>)"
                        .into(),
                ));
            }
            let op_bin = resolve_op_program(op_program)?;
            build_op_secret_command(&op_bin, &["item", "get", item, "--vault", vault, "--otp"])
        } else {
            let op_bin = resolve_op_program(op_program)?;
            build_op_secret_command(&op_bin, &["item", "get", trimmed, "--otp"])
        };
        let out = cmd
            .output()
            .map_err(|_| Error::Message("failed to invoke `op` CLI for OTP reference".into()))?;
        if !out.status.success() {
            return Err(Error::Message(format!(
                "1Password OTP resolution failed (exit code {})",
                out.status.code().unwrap_or(-1)
            )));
        }
        let otp = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if otp.is_empty() {
            return Err(Error::Message(
                "1Password OTP resolution returned an empty value".into(),
            ));
        }
        return Ok((otp, Some(candidate.to_string())));
    }

    if let Some(secret_key) = candidate.strip_prefix("secret://") {
        use anyagents_core::{CredentialStore, FileCredentialStore};
        let store = FileCredentialStore::new(data);
        let secret = store.read(secret_key.trim())?;
        let val = secret
            .as_str()
            .map(|s| s.trim().to_string())
            .map_err(|_| Error::Message("stored secret is not valid UTF-8 text".into()))?;
        return Ok((val, Some(candidate.to_string())));
    }

    if secret_ref.is_some() {
        return Err(Error::Message(
            "secret_ref must start with `op://`, `op-otp://`, or `secret://`".into(),
        ));
    }

    Ok((raw_text.unwrap_or("").to_string(), None))
}

fn dispatch_trusted_click(cdp: &mut CdpSession, x: f64, y: f64) -> Result<(), Error> {
    cdp.call(
        "Input.dispatchMouseEvent",
        serde_json::json!({
            "type": "mouseMoved",
            "x": x,
            "y": y,
            "button": "none",
            "buttons": 0,
        }),
    )?;
    cdp.call(
        "Input.dispatchMouseEvent",
        serde_json::json!({
            "type": "mousePressed",
            "x": x,
            "y": y,
            "button": "left",
            "buttons": 1,
            "clickCount": 1,
        }),
    )?;
    cdp.call(
        "Input.dispatchMouseEvent",
        serde_json::json!({
            "type": "mouseReleased",
            "x": x,
            "y": y,
            "button": "left",
            "buttons": 0,
            "clickCount": 1,
        }),
    )?;
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct KeySpec {
    pub key: String,
    pub code: String,
    pub vk: u32,
}

fn resolve_key_spec(raw: &str) -> KeySpec {
    let trimmed = raw.trim();
    if trimmed.is_empty() || raw == " " {
        return KeySpec {
            key: " ".to_string(),
            code: "Space".to_string(),
            vk: 32,
        };
    }
    if trimmed.len() == 1 {
        let ch = trimmed.chars().next().unwrap();
        if ch.is_ascii_alphabetic() {
            let upper = ch.to_ascii_uppercase();
            return KeySpec {
                key: ch.to_string(),
                code: format!("Key{upper}"),
                vk: upper as u32,
            };
        }
        if ch.is_ascii_digit() {
            return KeySpec {
                key: ch.to_string(),
                code: format!("Digit{ch}"),
                vk: ch as u32,
            };
        }
    }
    if trimmed.len() == 4
        && trimmed.starts_with("Key")
        && trimmed.as_bytes()[3].is_ascii_alphabetic()
    {
        let upper = (trimmed.as_bytes()[3] as char).to_ascii_uppercase();
        let lower = upper.to_ascii_lowercase();
        return KeySpec {
            key: lower.to_string(),
            code: format!("Key{upper}"),
            vk: upper as u32,
        };
    }
    if trimmed.len() == 6 && trimmed.starts_with("Digit") && trimmed.as_bytes()[5].is_ascii_digit()
    {
        let digit = trimmed.as_bytes()[5] as char;
        return KeySpec {
            key: digit.to_string(),
            code: format!("Digit{digit}"),
            vk: digit as u32,
        };
    }
    match trimmed {
        "Enter" => KeySpec {
            key: "Enter".to_string(),
            code: "Enter".to_string(),
            vk: 13,
        },
        "NumpadEnter" => KeySpec {
            key: "Enter".to_string(),
            code: "NumpadEnter".to_string(),
            vk: 13,
        },
        "Tab" => KeySpec {
            key: "Tab".to_string(),
            code: "Tab".to_string(),
            vk: 9,
        },
        "Escape" | "Esc" => KeySpec {
            key: "Escape".to_string(),
            code: "Escape".to_string(),
            vk: 27,
        },
        "Backspace" => KeySpec {
            key: "Backspace".to_string(),
            code: "Backspace".to_string(),
            vk: 8,
        },
        "Delete" => KeySpec {
            key: "Delete".to_string(),
            code: "Delete".to_string(),
            vk: 46,
        },
        "Space" => KeySpec {
            key: " ".to_string(),
            code: "Space".to_string(),
            vk: 32,
        },
        "ArrowDown" => KeySpec {
            key: "ArrowDown".to_string(),
            code: "ArrowDown".to_string(),
            vk: 40,
        },
        "ArrowUp" => KeySpec {
            key: "ArrowUp".to_string(),
            code: "ArrowUp".to_string(),
            vk: 38,
        },
        "ArrowLeft" => KeySpec {
            key: "ArrowLeft".to_string(),
            code: "ArrowLeft".to_string(),
            vk: 37,
        },
        "ArrowRight" => KeySpec {
            key: "ArrowRight".to_string(),
            code: "ArrowRight".to_string(),
            vk: 39,
        },
        "PageDown" => KeySpec {
            key: "PageDown".to_string(),
            code: "PageDown".to_string(),
            vk: 34,
        },
        "PageUp" => KeySpec {
            key: "PageUp".to_string(),
            code: "PageUp".to_string(),
            vk: 33,
        },
        "Home" => KeySpec {
            key: "Home".to_string(),
            code: "Home".to_string(),
            vk: 36,
        },
        "End" => KeySpec {
            key: "End".to_string(),
            code: "End".to_string(),
            vk: 35,
        },
        "[" | "BracketLeft" => KeySpec {
            key: "[".to_string(),
            code: "BracketLeft".to_string(),
            vk: 219,
        },
        "]" | "BracketRight" => KeySpec {
            key: "]".to_string(),
            code: "BracketRight".to_string(),
            vk: 221,
        },
        "/" | "?" | "Slash" => KeySpec {
            key: if trimmed == "?" { "?" } else { "/" }.to_string(),
            code: "Slash".to_string(),
            vk: 191,
        },
        other => KeySpec {
            key: other.to_string(),
            code: other.to_string(),
            vk: 0,
        },
    }
}

fn held_keys_path(data: &Path, profile: &str) -> PathBuf {
    profile_dir(data, profile).join("held_keys.json")
}

fn load_held_keys(data: &Path, profile: &str) -> Result<Vec<KeySpec>, Error> {
    Ok(read_profile_json(&held_keys_path(data, profile))?.unwrap_or_default())
}

fn save_held_keys(data: &Path, profile: &str, keys: &[KeySpec]) -> Result<(), Error> {
    if keys.is_empty() {
        return clear_held_keys(data, profile);
    }
    write_profile_json(&held_keys_path(data, profile), keys)
}

fn clear_held_keys(data: &Path, profile: &str) -> Result<(), Error> {
    remove_profile_file(&held_keys_path(data, profile))
}

fn dispatch_cdp_key_down(
    cdp: &mut CdpSession,
    spec: &KeySpec,
    auto_repeat: bool,
    target_frame_id: Option<&str>,
) -> Result<(), Error> {
    cdp.call(
        "Input.dispatchKeyEvent",
        serde_json::json!({
            "type": "keyDown",
            "key": spec.key,
            "code": spec.code,
            "windowsVirtualKeyCode": spec.vk,
            "nativeVirtualKeyCode": spec.vk,
            "autoRepeat": auto_repeat,
        }),
    )?;
    if let Some(fid) = target_frame_id {
        let key_json = serde_json::to_string(&spec.key).unwrap_or_else(|_| "\"Enter\"".into());
        let code_json = serde_json::to_string(&spec.code).unwrap_or_else(|_| "\"Enter\"".into());
        let vk = spec.vk;
        let js = format!(
            r#"(() => {{
              const el = document.activeElement || document.body;
              if (!el) return;
              el.dispatchEvent(new KeyboardEvent('keydown', {{ key: {key_json}, code: {code_json}, keyCode: {vk}, which: {vk}, repeat: {auto_repeat}, bubbles: true }}));
            }})()"#
        );
        eval_js_in_frame(cdp, Some(fid), &js)?;
    }
    Ok(())
}

fn dispatch_cdp_key_up(
    cdp: &mut CdpSession,
    spec: &KeySpec,
    target_frame_id: Option<&str>,
) -> Result<(), Error> {
    cdp.call(
        "Input.dispatchKeyEvent",
        serde_json::json!({
            "type": "keyUp",
            "key": spec.key,
            "code": spec.code,
            "windowsVirtualKeyCode": spec.vk,
            "nativeVirtualKeyCode": spec.vk,
        }),
    )?;
    if let Some(fid) = target_frame_id {
        let key_json = serde_json::to_string(&spec.key).unwrap_or_else(|_| "\"Enter\"".into());
        let code_json = serde_json::to_string(&spec.code).unwrap_or_else(|_| "\"Enter\"".into());
        let vk = spec.vk;
        let js = format!(
            r#"(() => {{
              const el = document.activeElement || document.body;
              if (!el) return;
              el.dispatchEvent(new KeyboardEvent('keyup', {{ key: {key_json}, code: {code_json}, keyCode: {vk}, which: {vk}, bubbles: true }}));
            }})()"#
        );
        eval_js_in_frame(cdp, Some(fid), &js)?;
    }
    Ok(())
}

fn release_key_ledger(
    data: &Path,
    profile: &str,
    cdp: &mut CdpSession,
    frame: Option<&str>,
) -> Result<Vec<String>, Error> {
    let held = load_held_keys(data, profile)?;
    let mut remaining = held.clone();
    let mut released = Vec::new();
    let mut first_err = None;
    for spec in held.iter().rev() {
        match dispatch_cdp_key_up(cdp, spec, frame) {
            Ok(()) => {
                remaining.retain(|s| s.code != spec.code || s.key != spec.key);
                released.push(spec.key.clone());
            }
            Err(err) => {
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
        }
    }
    save_held_keys(data, profile, &remaining)?;
    if let Some(err) = first_err {
        return Err(err);
    }
    Ok(released)
}

fn release_recorded_held_keys(
    data: &Path,
    profile: &str,
    cdp: &mut CdpSession,
) -> Result<Vec<String>, Error> {
    release_key_ledger(data, profile, cdp, None)
}

struct HeldKeyGuard<'a> {
    data: &'a Path,
    profile: &'a str,
    cdp: &'a mut CdpSession,
    target_frame_id: Option<String>,
    specs: Vec<KeySpec>,
    released: bool,
}

impl HeldKeyGuard<'_> {
    fn release(&mut self) -> Result<(), Error> {
        if self.released {
            return Ok(());
        }
        self.released = true;
        release_key_ledger(
            self.data,
            self.profile,
            self.cdp,
            self.target_frame_id.as_deref(),
        )?;
        Ok(())
    }
}

impl Drop for HeldKeyGuard<'_> {
    fn drop(&mut self) {
        // Drop cannot acknowledge failure, but must preserve failed releases in
        // the durable ledger so the next explicit release can recover them.
        if !self.released {
            let _ = self.release();
        }
    }
}

fn resolve_request_key_specs(req: &BrowserRequest) -> Result<Vec<KeySpec>, Error> {
    let mut out = Vec::new();
    if let Some(ref list) = req.keys {
        for item in list {
            if !item.is_empty() {
                out.push(resolve_key_spec(item));
            }
        }
    }
    if out.is_empty()
        && let Some(raw) = req
            .key
            .as_deref()
            .or(req.text.as_deref())
            .filter(|s| !s.is_empty())
    {
        out.push(resolve_key_spec(raw));
    }
    if out.is_empty() {
        return Err(Error::Message(
            "browser press/hold requires `key` or `keys` (e.g. `Enter`, `Escape`, `a`, `w`, `ArrowUp`)".into(),
        ));
    }
    Ok(out)
}

pub fn list_profiles(data: &Path) -> Result<Vec<BrowserProfileSummary>, Error> {
    let root = profiles_root(data);
    let mut names = std::collections::BTreeSet::new();
    names.insert("default".to_string());
    if let Ok(entries) = std::fs::read_dir(&root) {
        for entry in entries.flatten() {
            if entry.path().is_dir()
                && let Some(name) = entry.file_name().to_str()
            {
                names.insert(name.to_string());
            }
        }
    }
    let mut out = Vec::new();
    for name in names {
        let fp = ensure_fingerprint(data, &name)?;
        if let Some(rt) = load_runtime(data, &name)? {
            let pages = list_page_targets(rt.cdp_port).unwrap_or_default();
            let current = pages.first();
            out.push(BrowserProfileSummary {
                profile: name,
                status: "running".to_string(),
                pid: Some(rt.pid),
                cdp_port: Some(rt.cdp_port),
                display_mode: Some(rt.display_mode),
                current_url: current.map(|p| p.url.clone()),
                current_title: current.map(|p| p.title.clone()),
                last_used_at_ms: Some(rt.last_used_at_ms),
                webgl_renderer: fp.webgl_renderer,
                user_agent: fp.user_agent,
            });
        } else {
            out.push(BrowserProfileSummary {
                profile: name,
                status: "stopped".to_string(),
                pid: None,
                cdp_port: None,
                display_mode: None,
                current_url: None,
                current_title: None,
                last_used_at_ms: None,
                webgl_renderer: fp.webgl_renderer,
                user_agent: fp.user_agent,
            });
        }
    }
    Ok(out)
}

/// Run one browser request. `files` is whose access opens any host file the
/// request names (a screenshot `path`): the caller's, never the server's.
pub fn execute(
    data: &Path,
    default_profile: &str,
    req: BrowserRequest,
    files: &crate::agent_file::FileAuthority,
) -> Result<serde_json::Value, Error> {
    let secret = req.secret_ref.is_some()
        || req.text.as_deref().is_some_and(|s| {
            let s = s.trim();
            s.starts_with("op://") || s.starts_with("op-otp://") || s.starts_with("secret://")
        });
    let mutating = !browser_action_reads(&req);
    execute_inner(data, default_profile, req, files)
        .map_err(|err| browser_error(err, secret, mutating))
}

/// Whether a browser request only reads. Anything else, including an action
/// this list does not name, may have changed the page before it failed.
fn browser_action_reads(req: &BrowserRequest) -> bool {
    let action = req.action.trim().to_ascii_lowercase();
    match action.as_str() {
        "" => req.url.is_none(),
        "profiles" | "list" | "status" | "info" | "fingerprint" | "snapshot" | "read" | "dom"
        | "text" | "content" | "html" | "screenshot" | "shot" | "takeover_status"
        | "wait_for_user" | "wait_takeover" => true,
        "tabs" => req
            .tab_action
            .as_deref()
            .map(str::trim)
            .is_none_or(|sub| sub.is_empty() || sub.eq_ignore_ascii_case("list")),
        "cookies" => req.cookies.is_none(),
        _ => false,
    }
}

/// `mutating`: whether the request can change the page; see
/// [`crate::computer::transient_disposition`].
fn browser_error(err: Error, secret: bool, mutating: bool) -> Error {
    use crate::computer::{ErrorDisposition, format_computer_error, transient_disposition};
    let message = err.to_string();
    let lower = message.to_ascii_lowercase();
    let disposition = if lower.contains("stale_ref") {
        ErrorDisposition::stale_ref("Call tools.browser({action: 'snapshot'}) to refresh refs.")
    } else if lower.contains("not found") || lower.contains("no such") {
        ErrorDisposition::settled_absence(
            "Inspect the page with a new browser snapshot before acting.",
        )
    } else if let Some(disposition) = transient_disposition(
        &lower,
        mutating,
        "Transient browser failure; retry once or inspect browser status.",
        "The action may already have been delivered; take a new browser snapshot and verify \
         the page before repeating it.",
    ) {
        disposition
    } else {
        ErrorDisposition::terminal("Check browser input, environment, and human takeover status.")
    };
    // Keep actionable disposition metadata without exposing provider/page details for secrets.
    Error::Message(format_computer_error(
        if secret {
            "browser secret input failed (provider/page details withheld)"
        } else {
            &message
        },
        disposition,
    ))
}

fn execute_inner(
    data: &Path,
    default_profile: &str,
    req: BrowserRequest,
    files: &crate::agent_file::FileAuthority,
) -> Result<serde_json::Value, Error> {
    reap_idle_browsers(data);
    let action = if req.action.trim().is_empty() {
        if req.url.is_some() { "open" } else { "status" }
    } else {
        req.action.trim()
    }
    .to_ascii_lowercase();

    let profile = sanitize_profile_name(req.profile.as_deref(), default_profile);
    if req.proxy.is_some()
        && matches!(
            action.as_str(),
            "profiles" | "list" | "close" | "stop" | "shutdown" | "status" | "info" | "fingerprint"
        )
    {
        return Err(Error::Message("proxy applies only to actions that start or use the browser; omit it for status/list/close".into()));
    }

    match action.as_str() {
        "profiles" | "list" => {
            let list = list_profiles(data)?;
            return Ok(serde_json::json!({
                "profiles": list,
            }));
        }
        "close" | "stop" | "shutdown" => {
            return stop_profile(data, &profile);
        }
        "status" | "info" | "fingerprint" => {
            let fp = ensure_fingerprint(data, &profile)?;
            let held: Vec<String> = load_held_keys(data, &profile)?
                .into_iter()
                .map(|k| k.key)
                .collect();
            let takeover = load_takeover_state(data, &profile)?;
            if let Some(rt) = load_runtime(data, &profile)? {
                let pages = list_page_targets(rt.cdp_port).unwrap_or_default();
                return Ok(serde_json::json!({
                    "profile": profile,
                    "status": "running",
                    "runtime": rt,
                    "fingerprint": fp,
                    "tabs": pages,
                    "held_keys": held,
                    "takeover": takeover,
                }));
            }
            return Ok(serde_json::json!({
                "profile": profile,
                "status": "stopped",
                "fingerprint": fp,
                "held_keys": held,
                "takeover": takeover,
            }));
        }
        _ => {}
    }

    // Any URL a request carries is checked before a browser starts or a page
    // is touched.
    if let Some(url) = req.url.as_deref().map(str::trim).filter(|u| !u.is_empty()) {
        navigable_url(url)?;
    }
    check_human_takeover_fence(data, &profile, &action)?;

    // Reject stale refs before launching a browser, connecting CDP, or resolving secrets.
    if let Some(selector) = req
        .selector
        .as_deref()
        .filter(|s| s.trim().starts_with('@'))
    {
        resolve_selector(data, &profile, selector)?;
    }

    let (mut rt, fp) = ensure_browser_running(data, &profile, req.headless, req.proxy.as_deref())?;

    if action == "tabs" {
        let sub = req
            .tab_action
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("list")
            .to_ascii_lowercase();
        match sub.as_str() {
            "list" => {
                let pages = list_page_targets(rt.cdp_port)?;
                return Ok(serde_json::json!({
                    "profile": profile,
                    "active_target_id": rt.active_target_id,
                    "tabs": pages,
                }));
            }
            "new" | "open" => {
                let u = navigable_url(req.url.as_deref().unwrap_or("about:blank"))?;
                let u = u.as_str();
                let page = select_page_target(data, &mut rt, None)?;
                let ws_url = page
                    .ws_url
                    .ok_or_else(|| Error::Message("target missing ws_url".into()))?;
                let mut cdp = CdpSession::connect(rt.cdp_port, &ws_url)?;
                let created = cdp.call("Target.createTarget", serde_json::json!({ "url": u }))?;
                let tid = created
                    .get("targetId")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                if !tid.is_empty() {
                    rt.active_target_id = Some(tid.clone());
                    save_runtime(data, &rt)?;
                }
                return Ok(serde_json::json!({
                    "profile": profile,
                    "target_id": tid,
                    "url": u,
                }));
            }
            "activate" | "switch" => {
                let tid = req
                    .target_id
                    .as_deref()
                    .ok_or_else(|| Error::Message("tabs activate requires target_id".into()))?;
                let tid = checked_target_id(tid)?;
                cdp_http_get(rt.cdp_port, &format!("/json/activate/{tid}"))?;
                rt.active_target_id = Some(tid.to_string());
                save_runtime(data, &rt)?;
                return Ok(serde_json::json!({
                    "profile": profile,
                    "active_target_id": tid,
                }));
            }
            "close" => {
                let tid = req
                    .target_id
                    .as_deref()
                    .or(rt.active_target_id.as_deref())
                    .ok_or_else(|| Error::Message("tabs close requires target_id".into()))?
                    .to_string();
                checked_target_id(&tid)?;
                cdp_http_get(rt.cdp_port, &format!("/json/close/{tid}"))?;
                if rt.active_target_id.as_deref() == Some(&tid) {
                    rt.active_target_id = None;
                    save_runtime(data, &rt)?;
                }
                return Ok(serde_json::json!({
                    "profile": profile,
                    "closed_target_id": tid,
                }));
            }
            other => {
                return Err(Error::Message(format!("unknown tab_action `{other}`")));
            }
        }
    }

    let target = select_page_target(data, &mut rt, req.target_id.as_deref())?;
    let ws_url = target
        .ws_url
        .ok_or_else(|| Error::Message("browser page has no WebSocket debugger URL".into()))?;
    let mut cdp = CdpSession::connect(rt.cdp_port, &ws_url)?;
    // A page showing a host file or a browser-internal page is never read or
    // driven for a caller, however it got there; it goes back to blank.
    if !readable_page_url(&target.url) {
        let _ = cdp.call("Page.navigate", serde_json::json!({ "url": "about:blank" }));
        return Err(Error::Message(
            "the browser page was showing a URL other than http(s) or about:blank; it was reset to about:blank".into(),
        ));
    }
    apply_stealth(&mut cdp, &fp)?;

    match action.as_str() {
        "open" | "goto" | "navigate" => {
            let raw_url = req
                .url
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| Error::Message("browser open requires `url`".into()))?;
            let url = navigable_url(raw_url)?;
            release_recorded_held_keys(data, &profile, &mut cdp)?;
            navigate_page(&mut cdp, &url)?;
            let wait_ms = req.wait_ms.unwrap_or(1000);
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while std::time::Instant::now() < deadline {
                if let Ok(state) = eval_js(&mut cdp, "document.readyState")
                    && matches!(state.as_str(), Some("interactive" | "complete"))
                {
                    break;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            if wait_ms > 0 {
                std::thread::sleep(Duration::from_millis(wait_ms.min(10_000)));
            }
            apply_stealth(&mut cdp, &fp)?;
            let snap = capture_snapshot(data, &profile, &mut cdp)?;
            let mut out = serde_json::json!({
                "profile": profile,
                "status": "running",
                "display_mode": rt.display_mode,
                "url": snap.get("url").cloned().unwrap_or_else(|| serde_json::json!(url)),
                "title": snap.get("title").cloned().unwrap_or_default(),
                "text_preview": snap.get("text").and_then(|v| v.as_str()).map(|s| s.chars().take(1500).collect::<String>()).unwrap_or_default(),
                "interactive_elements": snap.get("elements").cloned().unwrap_or_default(),
                "snapshot_id": snap.get("snapshot_id").cloned().unwrap_or_default(),
                "captcha_detected": snap.get("captcha_detected").and_then(|v| v.as_bool()).unwrap_or(false),
            });
            if let Some(kind) = snap.get("captcha_kind").cloned() {
                out["captcha_kind"] = kind;
            }
            if let Some(hint) = snap.get("takeover_hint").cloned() {
                out["takeover_hint"] = hint;
            }
            Ok(out)
        }
        "snapshot" | "read" | "dom" | "text" | "content" | "html" => {
            let snap = capture_snapshot(data, &profile, &mut cdp)?;
            let mut out = serde_json::json!({
                "profile": profile,
                "url": snap.get("url").cloned().unwrap_or_default(),
                "title": snap.get("title").cloned().unwrap_or_default(),
                "text": snap.get("text").cloned().unwrap_or_default(),
                "elements": snap.get("elements").cloned().unwrap_or_default(),
                "snapshot_id": snap.get("snapshot_id").cloned().unwrap_or_default(),
                "captcha_detected": snap.get("captcha_detected").and_then(|v| v.as_bool()).unwrap_or(false),
            });
            if action == "html"
                && let Ok(html_val) = eval_js(
                    &mut cdp,
                    "document.documentElement ? document.documentElement.outerHTML : ''",
                )
            {
                out["html"] = html_val;
            }
            if let Some(kind) = snap.get("captcha_kind").cloned() {
                out["captcha_kind"] = kind;
            }
            if let Some(hint) = snap.get("takeover_hint").cloned() {
                out["takeover_hint"] = hint;
            }
            if let Some(st) = load_takeover_state(data, &profile)? {
                out["takeover"] = serde_json::to_value(st).unwrap_or_default();
            }
            Ok(out)
        }
        "click" => {
            let raw_sel = req
                .selector
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    Error::Message(
                        "browser click requires `selector` (e.g. `@e1` or CSS selector)".into(),
                    )
                })?;
            let resolved = resolve_selector(data, &profile, raw_sel)?;
            let sel_json =
                serde_json::to_string(&resolved.css_selector).unwrap_or_else(|_| "\"body\"".into());
            let prep_js = format!(
                r#"(() => {{
                  const el = document.querySelector({sel_json});
                  if (!el) return {{ found: false }};
                  el.scrollIntoView({{ block: 'center', inline: 'center' }});
                  const r = el.getBoundingClientRect();
                  window.__anyagents_click_hit = false;
                  el.addEventListener('click', () => {{ window.__anyagents_click_hit = true; }}, {{ once: true, capture: true }});
                  return {{
                    found: true,
                    tag: el.tagName.toLowerCase(),
                    text: (el.innerText || el.value || '').trim().slice(0, 80),
                    x: Math.round(r.left + r.width / 2),
                    y: Math.round(r.top + r.height / 2)
                  }};
                }})()"#
            );
            let child_frames = discover_child_frames(&mut cdp);
            let mut active_frame_id = resolved.frame_id.clone();
            let mut target_info = eval_js_in_frame(&mut cdp, active_frame_id.as_deref(), &prep_js)
                .unwrap_or_else(|_| serde_json::json!({ "found": false }));
            if target_info.get("found").and_then(|v| v.as_bool()) != Some(true) {
                for cf in &child_frames {
                    if let Ok(info) = eval_js_in_frame(&mut cdp, Some(&cf.frame_id), &prep_js)
                        && info.get("found").and_then(|v| v.as_bool()) == Some(true)
                    {
                        active_frame_id = Some(cf.frame_id.clone());
                        target_info = info;
                        break;
                    }
                }
            }
            if target_info.get("found").and_then(|v| v.as_bool()) == Some(true) {
                let (off_x, off_y) = active_frame_id
                    .as_deref()
                    .and_then(|fid| child_frames.iter().find(|cf| cf.frame_id == fid))
                    .map(|cf| (cf.offset_x, cf.offset_y))
                    .unwrap_or((0.0, 0.0));
                let x = target_info.get("x").and_then(|v| v.as_f64()).unwrap_or(0.0) + off_x;
                let y = target_info.get("y").and_then(|v| v.as_f64()).unwrap_or(0.0) + off_y;
                dispatch_trusted_click(&mut cdp, x, y)?;
            } else if let Some((x, y)) = resolved.fallback_xy {
                dispatch_trusted_click(&mut cdp, x, y)?;
            } else {
                return Err(Error::Message(format!(
                    "element `{raw_sel}` not found on page; run `snapshot` first to refresh @e refs"
                )));
            }
            if let Some(wait_ms) = req.wait_ms
                && wait_ms > 0
            {
                std::thread::sleep(Duration::from_millis(wait_ms.min(10_000)));
            } else {
                std::thread::sleep(Duration::from_millis(300));
            }
            let snap = capture_snapshot(data, &profile, &mut cdp)?;
            Ok(serde_json::json!({
                "profile": profile,
                "clicked": raw_sel,
                "detail": target_info,
                "url": snap.get("url").cloned().unwrap_or_default(),
                "title": snap.get("title").cloned().unwrap_or_default(),
            }))
        }
        "fill" | "type" | "input" => {
            let raw_sel = req
                .selector
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    Error::Message(
                        "browser fill requires `selector` (e.g. `@e1` or CSS selector)".into(),
                    )
                })?;
            let resolved = resolve_selector(data, &profile, raw_sel)?;
            let (text_val, secret_source) =
                resolve_fill_value(data, req.text.as_deref(), req.secret_ref.as_deref())?;
            let sel_json = serde_json::to_string(&resolved.css_selector)
                .unwrap_or_else(|_| "\"input\"".into());
            let val_json = serde_json::to_string(&text_val).unwrap_or_else(|_| "\"\"".into());
            let submit = req.submit.unwrap_or(false);

            // Step 1: Scroll into view, focus, handle <select> directly, or prepare <input>/<textarea>/[contenteditable]
            // for hardware-trusted CDP keystroke injection (`event.isTrusted === true`).
            let prep_js = format!(
                r#"(() => {{
                  const el = document.querySelector({sel_json});
                  if (!el) return {{ found: false }};
                  el.scrollIntoView({{ block: 'center', inline: 'center' }});
                  const r = el.getBoundingClientRect();
                  const tag = el.tagName.toLowerCase();
                  if (tag === 'select') {{
                    const targetVal = {val_json};
                    let selectedValue = null;
                    for (const opt of Array.from(el.options || [])) {{
                      if (opt.value === targetVal || (opt.text && opt.text.trim() === targetVal)) {{
                        el.value = opt.value;
                        selectedValue = opt.value;
                        break;
                      }}
                    }}
                    if (selectedValue === null) el.value = targetVal;
                    el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                    el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                    return {{ found: true, kind: 'select', tag, matched: el.value === (selectedValue === null ? targetVal : selectedValue) }};
                  }}
                  try {{ window.focus && window.focus(); }} catch (_) {{}}
                  el.focus();
                  let hadContent = false;
                  if (typeof el.value === 'string') {{
                    hadContent = el.value.length > 0;
                    if (typeof el.select === 'function') {{
                      try {{ el.select(); }} catch (_) {{}}
                    }}
                    if (typeof el.setSelectionRange === 'function') {{
                      try {{ el.setSelectionRange(0, el.value.length); }} catch (_) {{}}
                    }}
                  }} else if (el.isContentEditable) {{
                    hadContent = (el.innerText || '').length > 0;
                    try {{
                      const range = document.createRange();
                      range.selectNodeContents(el);
                      const sel = window.getSelection();
                      sel.removeAllRanges();
                      sel.addRange(range);
                    }} catch (_) {{}}
                  }}
                  return {{
                    found: true,
                    kind: el.isContentEditable ? 'contenteditable' : 'input',
                    tag,
                    had_content: hadContent,
                    x: Math.round(r.left + r.width / 2),
                    y: Math.round(r.top + r.height / 2)
                  }};
                }})()"#
            );
            let child_frames = discover_child_frames(&mut cdp);
            let mut active_frame_id = resolved.frame_id.clone();
            let mut prep = eval_js_in_frame(&mut cdp, active_frame_id.as_deref(), &prep_js)
                .unwrap_or_else(|_| serde_json::json!({ "found": false }));
            if prep.get("found").and_then(|v| v.as_bool()) != Some(true) {
                for cf in &child_frames {
                    if let Ok(info) = eval_js_in_frame(&mut cdp, Some(&cf.frame_id), &prep_js)
                        && info.get("found").and_then(|v| v.as_bool()) == Some(true)
                    {
                        active_frame_id = Some(cf.frame_id.clone());
                        prep = info;
                        break;
                    }
                }
            }
            if prep.get("found").and_then(|v| v.as_bool()) != Some(true) {
                return Err(Error::Message(format!(
                    "input element `{raw_sel}` not found on page; run `snapshot` first"
                )));
            }

            if active_frame_id.is_some() {
                let (off_x, off_y) = active_frame_id
                    .as_deref()
                    .and_then(|fid| child_frames.iter().find(|cf| cf.frame_id == fid))
                    .map(|cf| (cf.offset_x, cf.offset_y))
                    .unwrap_or((0.0, 0.0));
                let click_x = prep.get("x").and_then(|v| v.as_f64()).unwrap_or(0.0) + off_x;
                let click_y = prep.get("y").and_then(|v| v.as_f64()).unwrap_or(0.0) + off_y;
                if click_x > 0.0 || click_y > 0.0 {
                    dispatch_trusted_click(&mut cdp, click_x, click_y)?;
                    eval_js_in_frame(&mut cdp, active_frame_id.as_deref(), &prep_js)?;
                }
            }

            if prep["kind"] == "select" && prep["matched"] != true {
                return Err(Error::Message(
                    "requested select option was not accepted".into(),
                ));
            }
            if prep.get("kind").and_then(|v| v.as_str()) != Some("select") {
                if prep.get("had_content").and_then(|v| v.as_bool()) == Some(true) {
                    cdp.call(
                        "Input.dispatchKeyEvent",
                        serde_json::json!({
                            "type": "keyDown",
                            "key": "Backspace",
                            "code": "Backspace",
                            "windowsVirtualKeyCode": 8,
                            "nativeVirtualKeyCode": 8,
                        }),
                    )?;
                    cdp.call(
                        "Input.dispatchKeyEvent",
                        serde_json::json!({
                            "type": "keyUp",
                            "key": "Backspace",
                            "code": "Backspace",
                            "windowsVirtualKeyCode": 8,
                            "nativeVirtualKeyCode": 8,
                        }),
                    )?;
                }

                // Use per-character hardware `Input.dispatchKeyEvent` for short single-line values
                // (passwords, usernames, 6-digit OTP boxes) so `keydown`, `beforeinput`, `input`, and `keyup`
                // all fire with `event.isTrusted === true` and split OTP boxes auto-advance cleanly.
                if text_val.chars().count() <= 128 && !text_val.contains('\n') {
                    for ch in text_val.chars() {
                        let s = ch.to_string();
                        cdp.call(
                            "Input.dispatchKeyEvent",
                            serde_json::json!({
                                "type": "keyDown",
                                "key": s,
                                "text": s,
                                "unmodifiedText": s,
                            }),
                        )?;
                        cdp.call(
                            "Input.dispatchKeyEvent",
                            serde_json::json!({
                                "type": "keyUp",
                                "key": s,
                            }),
                        )?;
                    }
                } else if !text_val.is_empty() {
                    cdp.call(
                        "Input.insertText",
                        serde_json::json!({
                            "text": text_val,
                        }),
                    )?;
                }

                // Verify and fire `change` event (or apply native setter fallback if a custom control or cross-origin frame missed key events).
                let finalize_js = format!(
                    r#"(() => {{
                      const el = document.querySelector({sel_json});
                      if (!el) return false;
                      // Auto-advancing single-cell codes deliberately spread the
                      // trusted keystrokes across controls; verify the whole group,
                      // never replace or approve only its first cell.
                      if (el.maxLength === 1 && Array.from({val_json}).length > 1) {{
                        const root = el.form || el.parentElement;
                        if (!root) return false;
                        const cells = Array.from(root.querySelectorAll('input')).filter(cell =>
                          cell.maxLength === 1 && !cell.disabled && !cell.readOnly && cell.type !== 'hidden');
                        const start = cells.indexOf(el);
                        const group = cells.slice(start, start + Array.from({val_json}).length);
                        if (start < 0 || group.length !== Array.from({val_json}).length || group.map(cell => cell.value).join('') !== {val_json}) return false;
                        for (const cell of group) cell.dispatchEvent(new Event('change', {{ bubbles: true }}));
                        return group.map(cell => cell.value).join('') === {val_json};
                      }}
                      if (typeof el.value === 'string' && el.value !== {val_json} && (!el.maxLength || el.maxLength < 0 || {val_json}.length <= el.maxLength)) {{
                        const proto = Object.getPrototypeOf(el);
                        const desc = Object.getOwnPropertyDescriptor(proto, 'value')
                          || Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')
                          || Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value');
                        if (desc && desc.set) {{
                          desc.set.call(el, {val_json});
                        }} else {{
                          el.value = {val_json};
                        }}
                        el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                      }}
                      el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                      return el.isContentEditable ? el.innerText === {val_json} : (typeof el.value === 'string' && el.value === {val_json});
                    }})()"#
                );
                if eval_js_in_frame(&mut cdp, active_frame_id.as_deref(), &finalize_js)?.as_bool()
                    != Some(true)
                {
                    return Err(Error::Message("input value verification failed".into()));
                }
            }

            if submit {
                cdp.call(
                    "Input.dispatchKeyEvent",
                    serde_json::json!({
                        "type": "keyDown",
                        "key": "Enter",
                        "code": "Enter",
                        "windowsVirtualKeyCode": 13,
                        "nativeVirtualKeyCode": 13,
                    }),
                )?;
                cdp.call(
                    "Input.dispatchKeyEvent",
                    serde_json::json!({
                        "type": "keyUp",
                        "key": "Enter",
                        "code": "Enter",
                        "windowsVirtualKeyCode": 13,
                        "nativeVirtualKeyCode": 13,
                    }),
                )?;
                if active_frame_id.is_some() {
                    let submit_js = format!(
                        r#"(() => {{
                          const el = document.querySelector({sel_json});
                          if (!el) return;
                          el.dispatchEvent(new KeyboardEvent('keydown', {{ key: 'Enter', code: 'Enter', keyCode: 13, which: 13, bubbles: true }}));
                          el.dispatchEvent(new KeyboardEvent('keyup', {{ key: 'Enter', code: 'Enter', keyCode: 13, which: 13, bubbles: true }}));
                        }})()"#
                    );
                    eval_js_in_frame(&mut cdp, active_frame_id.as_deref(), &submit_js)?;
                }
            }
            let mut resp = serde_json::json!({
                "profile": profile,
                "filled": raw_sel,
                "submitted": submit,
                "trusted_input": true,
            });
            if let Some(fid) = active_frame_id {
                resp["frame_id"] = serde_json::Value::String(fid);
            }
            if let Some(src) = secret_source {
                resp["secret_injected"] = serde_json::Value::Bool(true);
                resp["secret_source"] = serde_json::Value::String(src);
            }
            Ok(resp)
        }
        "press" | "key" | "hold" | "key_down" | "keydown" | "key_up" | "keyup" | "release"
        | "release_keys" => {
            let mut target_frame_id = None;
            if let Some(raw_sel) = req
                .selector
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                let resolved = resolve_selector(data, &profile, raw_sel)?;
                target_frame_id = resolved.frame_id.clone();
                let sel_json = serde_json::to_string(&resolved.css_selector)
                    .unwrap_or_else(|_| "\"body\"".into());
                let focus_js = format!(
                    r#"(() => {{
                      const el = document.querySelector({sel_json});
                      if (!el) return false;
                      try {{ window.focus && window.focus(); }} catch (_) {{}}
                      el.focus && el.focus();
                      return document.activeElement === el;
                    }})()"#
                );
                if eval_js_in_frame(&mut cdp, target_frame_id.as_deref(), &focus_js)?.as_bool()
                    != Some(true)
                {
                    return Err(Error::Message(format!(
                        "keyboard target `{raw_sel}` could not be focused"
                    )));
                }
            } else {
                let _ = eval_js(
                    &mut cdp,
                    "try { window.focus && window.focus(); } catch (_) {}",
                );
            }

            if matches!(
                action.as_str(),
                "key_up" | "keyup" | "release" | "release_keys"
            ) {
                let has_explicit_key = req
                    .keys
                    .as_ref()
                    .is_some_and(|v| v.iter().any(|s| !s.trim().is_empty()))
                    || req
                        .key
                        .as_deref()
                        .or(req.text.as_deref())
                        .is_some_and(|s| !s.trim().is_empty());
                if has_explicit_key {
                    let specs = resolve_request_key_specs(&req)?;
                    for spec in specs.iter().rev() {
                        dispatch_cdp_key_up(&mut cdp, spec, target_frame_id.as_deref())?;
                    }
                    let mut remaining = load_held_keys(data, &profile)?;
                    remaining.retain(|held| {
                        !specs
                            .iter()
                            .any(|s| s.code == held.code || s.key == held.key)
                    });
                    save_held_keys(data, &profile, &remaining)?;
                    let released_names: Vec<String> = specs.iter().map(|s| s.key.clone()).collect();
                    return Ok(serde_json::json!({
                        "profile": profile,
                        "released": if released_names.len() == 1 {
                            serde_json::json!(released_names[0])
                        } else {
                            serde_json::json!(released_names)
                        },
                        "released_keys": released_names,
                        "held_keys": remaining.iter().map(|s| s.key.clone()).collect::<Vec<_>>(),
                    }));
                } else {
                    let released = release_recorded_held_keys(data, &profile, &mut cdp)?;
                    return Ok(serde_json::json!({
                        "profile": profile,
                        "released": released,
                        "released_keys": released,
                        "held_keys": Vec::<String>::new(),
                    }));
                }
            }

            let specs = resolve_request_key_specs(&req)?;
            let key_names: Vec<String> = specs.iter().map(|s| s.key.clone()).collect();
            let code_names: Vec<String> = specs.iter().map(|s| s.code.clone()).collect();

            if matches!(action.as_str(), "key_down" | "keydown") {
                let mut current_held = load_held_keys(data, &profile)?;
                for spec in &specs {
                    if !current_held
                        .iter()
                        .any(|h| h.code == spec.code && h.key == spec.key)
                    {
                        current_held.push(spec.clone());
                    }
                }
                save_held_keys(data, &profile, &current_held)?;
                for spec in &specs {
                    dispatch_cdp_key_down(&mut cdp, spec, false, target_frame_id.as_deref())?;
                }
                return Ok(serde_json::json!({
                    "profile": profile,
                    "key_down": if key_names.len() == 1 {
                        serde_json::json!(key_names[0])
                    } else {
                        serde_json::json!(key_names)
                    },
                    "keys": key_names,
                    "codes": code_names,
                    "held_keys": current_held.iter().map(|s| s.key.clone()).collect::<Vec<_>>(),
                }));
            }

            // Release any prior unclosed held keys on this profile before starting a new press/hold.
            release_recorded_held_keys(data, &profile, &mut cdp)?;

            let requested_hold = req.hold_ms.or(req.duration_ms);
            let hold_ms = if action == "hold" {
                requested_hold.or(req.wait_ms).unwrap_or(500)
            } else {
                requested_hold.unwrap_or(0)
            }
            .min(30_000);
            let repeat = req.repeat.unwrap_or(false);

            if hold_ms > 0 {
                save_held_keys(data, &profile, &specs)?;
                let mut guard = HeldKeyGuard {
                    data,
                    profile: &profile,
                    cdp: &mut cdp,
                    specs: specs.clone(),
                    target_frame_id: target_frame_id.clone(),
                    released: false,
                };
                for spec in &guard.specs {
                    dispatch_cdp_key_down(
                        guard.cdp,
                        spec,
                        false,
                        guard.target_frame_id.as_deref(),
                    )?;
                }
                let start = std::time::Instant::now();
                let total = Duration::from_millis(hold_ms);
                let mut next_repeat = Duration::from_millis(50);
                while start.elapsed() < total {
                    let rem = total.saturating_sub(start.elapsed());
                    let step = rem.min(Duration::from_millis(25));
                    if !step.is_zero() {
                        std::thread::sleep(step);
                    }
                    if repeat && start.elapsed() >= next_repeat && start.elapsed() < total {
                        for spec in &guard.specs {
                            dispatch_cdp_key_down(
                                guard.cdp,
                                spec,
                                true,
                                guard.target_frame_id.as_deref(),
                            )?;
                        }
                        next_repeat += Duration::from_millis(50);
                    }
                }
                let elapsed_ms = start.elapsed().as_millis() as u64;
                guard.release()?;

                if let Some(wait_ms) = req.wait_ms
                    && action != "hold"
                    && wait_ms > 0
                {
                    std::thread::sleep(Duration::from_millis(wait_ms.min(10_000)));
                }

                Ok(serde_json::json!({
                    "profile": profile,
                    "pressed": if key_names.len() == 1 {
                        serde_json::json!(key_names[0])
                    } else {
                        serde_json::json!(key_names)
                    },
                    "keys": key_names,
                    "codes": code_names,
                    "hold_ms": hold_ms,
                    "elapsed_ms": elapsed_ms,
                    "released": true,
                    "trusted_input": true,
                }))
            } else {
                save_held_keys(data, &profile, &specs)?;
                let mut guard = HeldKeyGuard {
                    data,
                    profile: &profile,
                    cdp: &mut cdp,
                    target_frame_id: target_frame_id.clone(),
                    specs: specs.clone(),
                    released: false,
                };
                for spec in &specs {
                    dispatch_cdp_key_down(guard.cdp, spec, false, target_frame_id.as_deref())?;
                }
                guard.release()?;
                if let Some(wait_ms) = req.wait_ms
                    && wait_ms > 0
                {
                    std::thread::sleep(Duration::from_millis(wait_ms.min(10_000)));
                }
                Ok(serde_json::json!({
                    "profile": profile,
                    "pressed": if key_names.len() == 1 {
                        serde_json::json!(key_names[0])
                    } else {
                        serde_json::json!(key_names)
                    },
                    "keys": key_names,
                    "codes": code_names,
                    "hold_ms": 0,
                    "released": true,
                    "trusted_input": true,
                }))
            }
        }
        "eval" | "evaluate" | "js" => {
            let script = req
                .script
                .as_deref()
                .or(req.text.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| Error::Message("browser eval requires `script`".into()))?;
            let value = eval_js(&mut cdp, script)?;
            Ok(serde_json::json!({
                "profile": profile,
                "result": value,
            }))
        }
        "screenshot" | "shot" => {
            let full = req.full_page.unwrap_or(false);
            let shot = cdp.call(
                "Page.captureScreenshot",
                serde_json::json!({
                    "format": "png",
                    "captureBeyondViewport": full,
                }),
            )?;
            let b64 = shot
                .get("data")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Error::Message("screenshot returned no image data".into()))?;
            let png_bytes = decode_base64(b64)?;
            let shots_dir = profile_dir(data, &profile).join("screenshots");
            std::fs::create_dir_all(&shots_dir)?;
            let fallback_path = shots_dir.join(format!("shot-{}.png", now_ms()));
            let requested_path = req
                .path
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(PathBuf::from);
            // A requested path is written with the caller's own access, never
            // the server's: a seat writes it as itself (ADR 0010 D3). One it
            // cannot write still gets the profile's copy, and says why.
            let mut path_refused = None;
            let final_path = match requested_path {
                Some(target) => match files.write(&target, &png_bytes) {
                    Ok(()) => target,
                    Err(why) => {
                        path_refused = Some(why);
                        std::fs::write(&fallback_path, &png_bytes)?;
                        fallback_path
                    }
                },
                None => {
                    std::fs::write(&fallback_path, &png_bytes)?;
                    fallback_path
                }
            };
            let mut out = serde_json::json!({
                "profile": profile,
                "path": final_path.display().to_string(),
                "bytes": png_bytes.len(),
                "mime_type": "image/png",
                "image_base64": b64,
            });
            if let Some(why) = path_refused {
                out["path_refused"] = serde_json::json!(why);
            }
            Ok(out)
        }
        "cookies" => {
            if let Some(new_cookies) = req.cookies {
                cdp.call(
                    "Network.setCookies",
                    serde_json::json!({ "cookies": new_cookies }),
                )?;
            }
            let all = cdp.call("Network.getAllCookies", serde_json::json!({}))?;
            let list = all
                .get("cookies")
                .cloned()
                .unwrap_or_else(|| serde_json::json!([]));
            let count = list.as_array().map(|a| a.len()).unwrap_or(0);
            Ok(serde_json::json!({
                "profile": profile,
                "count": count,
                "cookies": list,
            }))
        }
        "takeover" | "human_takeover" | "user_takeover" => {
            if let Some(raw_url) = req.url.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
                navigate_page(&mut cdp, &navigable_url(raw_url)?)?;
                std::thread::sleep(Duration::from_millis(800));
            }
            let cap = detect_captcha(&mut cdp);
            let reason = req
                .reason
                .as_deref()
                .or(req.text.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .or_else(|| cap.summary.clone());

            let shots_dir = profile_dir(data, &profile).join("screenshots");
            let _ = std::fs::create_dir_all(&shots_dir);
            let shot_path = shots_dir.join(format!("takeover-{}.png", now_ms()));
            let screenshot_path = if let Ok(shot) = cdp.call(
                "Page.captureScreenshot",
                serde_json::json!({
                    "format": "png",
                    "captureBeyondViewport": false,
                }),
            ) && let Some(b64) = shot.get("data").and_then(|v| v.as_str())
                && let Ok(bytes) = decode_base64(b64)
                && std::fs::write(&shot_path, &bytes).is_ok()
            {
                Some(shot_path.display().to_string())
            } else {
                None
            };

            let state = start_or_reuse_takeover_session(
                data,
                &profile,
                Some(default_profile),
                reason.as_deref(),
                req.ttl_ms,
                req.tunnel,
                &cap,
            )?;

            if let Some(wait_ms) = req.wait_ms
                && wait_ms > 0
            {
                let (after_st, after_cap) = wait_for_takeover_completion(data, &profile, wait_ms)?;
                let completed = after_st.as_ref().is_some_and(|s| s.completed);
                let snap = capture_snapshot(data, &profile, &mut cdp)?;
                return Ok(serde_json::json!({
                    "profile": profile,
                    "status": if completed { "takeover_completed" } else { "takeover_active" },
                    "holder": after_st.as_ref().map(|s| s.holder.as_str()).unwrap_or("agent"),
                    "completed": completed,
                    "takeover_url": state.takeover_url,
                    "local_url": state.local_url,
                    "public_url": state.public_url,
                    "token": state.token,
                    "screenshot_path": screenshot_path,
                    "url": snap.get("url").cloned().unwrap_or_default(),
                    "title": snap.get("title").cloned().unwrap_or_default(),
                    "text_preview": snap.get("text").and_then(|v| v.as_str()).map(|s| s.chars().take(1500).collect::<String>()).unwrap_or_default(),
                    "interactive_elements": snap.get("elements").cloned().unwrap_or_default(),
                    "captcha_detected": after_cap.detected,
                    "captcha_kind": after_cap.kind,
                    "expires_at_ms": state.expires_at_ms,
                }));
            }

            let cur_url = eval_js(&mut cdp, "location.href")
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_default();
            let cur_title = eval_js(&mut cdp, "document.title || ''")
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_default();

            Ok(serde_json::json!({
                "profile": profile,
                "status": "takeover_active",
                "holder": state.holder,
                "completed": state.completed,
                "takeover_url": state.takeover_url,
                "local_url": state.local_url,
                "public_url": state.public_url,
                "token": state.token,
                "port": state.port,
                "reason": state.reason,
                "screenshot_path": screenshot_path,
                "url": cur_url,
                "title": cur_title,
                "captcha_detected": cap.detected,
                "captcha_kind": cap.kind,
                "expires_at_ms": state.expires_at_ms,
                "instructions": "Send `takeover_url` (and `screenshot_path` via `send` media) to the user so they can solve the CAPTCHA or verification challenge in the live browser session. When the user clicks 'Done — Hand Back to Agent', you will automatically receive `[Browser Takeover Completed]` (or call `browser({ \"action\": \"wait_for_user\", \"profile\": \"...\", \"wait_ms\": 120000 })`).",
            }))
        }
        "takeover_status" | "wait_for_user" | "wait_takeover" => {
            let should_wait = matches!(action.as_str(), "wait_for_user" | "wait_takeover")
                || req.wait_ms.is_some_and(|w| w > 0);
            let (st, cap) = if should_wait {
                let wait_ms = req.wait_ms.unwrap_or(60_000);
                wait_for_takeover_completion(data, &profile, wait_ms)?
            } else {
                reap_expired_takeover(data, &profile, now_ms())?;
                (
                    load_takeover_state(data, &profile)?,
                    detect_captcha(&mut cdp),
                )
            };
            let snap = capture_snapshot(data, &profile, &mut cdp)?;
            let active = st
                .as_ref()
                .is_some_and(|s| s.holder == "human" && !s.completed);
            Ok(serde_json::json!({
                "profile": profile,
                "status": if active { "takeover_active" } else { "agent_control" },
                "holder": st.as_ref().map(|s| s.holder.as_str()).unwrap_or("agent"),
                "completed": st.as_ref().is_none_or(|s| s.completed),
                "takeover": st,
                "url": snap.get("url").cloned().unwrap_or_default(),
                "title": snap.get("title").cloned().unwrap_or_default(),
                "text_preview": snap.get("text").and_then(|v| v.as_str()).map(|s| s.chars().take(1500).collect::<String>()).unwrap_or_default(),
                "interactive_elements": snap.get("elements").cloned().unwrap_or_default(),
                "captcha_detected": cap.detected,
                "captcha_kind": cap.kind,
            }))
        }
        "release_takeover" | "end_takeover" | "resume_agent" => {
            let note = req
                .reason
                .as_deref()
                .or(req.text.as_deref())
                .unwrap_or("released by agent");
            {
                let _ownership = takeover_ownership_lock(data, &profile, true)?;
                complete_takeover_session_owned(data, &profile, Some(note))?;
                cleanup_takeover_session_owned(data, &profile)?;
            }
            let snap = capture_snapshot(data, &profile, &mut cdp)?;
            Ok(serde_json::json!({
                "profile": profile,
                "status": "agent_control",
                "holder": "agent",
                "released_takeover": true,
                "url": snap.get("url").cloned().unwrap_or_default(),
                "title": snap.get("title").cloned().unwrap_or_default(),
                "text_preview": snap.get("text").and_then(|v| v.as_str()).map(|s| s.chars().take(1500).collect::<String>()).unwrap_or_default(),
                "interactive_elements": snap.get("elements").cloned().unwrap_or_default(),
                "captcha_detected": snap.get("captcha_detected").and_then(|v| v.as_bool()).unwrap_or(false),
            }))
        }
        other => Err(Error::Message(format!(
            "unknown browser action `{other}`; expected `open`, `snapshot`, `click`, `fill`, `press`, `hold`, `key_down`, `key_up`, `eval`, `screenshot`, `cookies`, `tabs`, `takeover`, `takeover_status`, `wait_for_user`, `release_takeover`, `status`, `profiles`, or `close`"
        ))),
    }
}

fn decode_base64(input: &str) -> Result<Vec<u8>, Error> {
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    let mut buf = 0u32;
    let mut bits = 0u8;
    for byte in input.bytes() {
        let val = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' | b'\r' | b'\n' | b' ' => continue,
            _ => return Err(Error::Message("invalid base64 in screenshot".into())),
        };
        buf = (buf << 6) | (val as u32);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(((buf >> bits) & 0xff) as u8);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    extern "C" fn ignore_eintr_test_signal(_signal: libc::c_int) {}

    #[cfg(unix)]
    fn install_eintr_test_handler_once() {
        use std::sync::Once;
        static ONCE: Once = Once::new();
        ONCE.call_once(|| unsafe {
            let mut action: libc::sigaction = std::mem::zeroed();
            action.sa_sigaction = ignore_eintr_test_signal as *const () as usize;
            action.sa_flags = 0;
            libc::sigemptyset(&mut action.sa_mask);
            libc::sigaction(libc::SIGUSR1, &action, std::ptr::null_mut());
        });
    }

    #[cfg(unix)]
    fn current_thread_pthread_addr() -> usize {
        unsafe {
            let pthread = libc::pthread_self();
            std::mem::transmute_copy::<libc::pthread_t, usize>(&pthread)
        }
    }

    #[cfg(unix)]
    fn pthread_from_addr(addr: usize) -> libc::pthread_t {
        unsafe { std::mem::transmute_copy::<usize, libc::pthread_t>(&addr) }
    }

    #[cfg(unix)]
    #[test]
    fn takeover_ownership_lock_retries_after_eintr() {
        use super::*;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::thread;
        install_eintr_test_handler_once();
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().to_path_buf();
        let held = takeover_ownership_lock(&data, "default", true).unwrap();
        let waiter_tid = Arc::new(AtomicUsize::new(usize::MAX));
        let ready_data = data.clone();
        let ready_tid = Arc::clone(&waiter_tid);
        let waiter = thread::spawn(move || {
            ready_tid.store(current_thread_pthread_addr(), Ordering::SeqCst);
            takeover_ownership_lock(&ready_data, "default", true)
        });
        for _ in 0..200 {
            if waiter_tid.load(Ordering::SeqCst) != usize::MAX {
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
        // The waiter is now blocked in flock; interrupt it with SIGUSR1.
        thread::sleep(Duration::from_millis(100));
        let pthread = pthread_from_addr(waiter_tid.load(Ordering::SeqCst));
        for _ in 0..10 {
            unsafe { libc::pthread_kill(pthread, libc::SIGUSR1) };
            thread::sleep(Duration::from_millis(10));
        }
        drop(held);
        let lock = waiter
            .join()
            .unwrap()
            .expect("ownership lock must be acquired after EINTR is retried");
        assert!(lock.metadata().is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn cdp_http_get_retries_after_eintr() {
        use super::*;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::thread;
        install_eintr_test_handler_once();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            conn.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            let mut discard = [0u8; 4096];
            while !discard.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = conn.read(&mut discard).unwrap_or(0);
                if n == 0 {
                    break;
                }
            }
            // Stall so the client blocks in read while EINTR signals arrive.
            thread::sleep(Duration::from_millis(500));
            let body = r#"{"Browser":"Chrome/EINTR-test"}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            conn.write_all(response.as_bytes()).unwrap();
        });
        let client_tid = Arc::new(AtomicUsize::new(usize::MAX));
        let ready_tid = Arc::clone(&client_tid);
        let client = thread::spawn(move || {
            ready_tid.store(current_thread_pthread_addr(), Ordering::SeqCst);
            cdp_http_get(port, "/json/version")
        });
        let client_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let signal_done = Arc::clone(&client_done);
        let signal_tid = Arc::clone(&client_tid);
        let signaler = thread::spawn(move || {
            for _ in 0..5000 {
                if signal_done.load(Ordering::SeqCst) {
                    break;
                }
                let addr = signal_tid.load(Ordering::SeqCst);
                if addr != usize::MAX {
                    let pthread = pthread_from_addr(addr);
                    unsafe { libc::pthread_kill(pthread, libc::SIGUSR1) };
                }
                thread::sleep(Duration::from_millis(5));
            }
        });
        let body = client
            .join()
            .unwrap()
            .expect("CDP HTTP response must be read after EINTR is retried");
        client_done.store(true, Ordering::SeqCst);
        let _ = signaler.join();
        assert!(
            String::from_utf8_lossy(&body).contains("Chrome/EINTR-test"),
            "unexpected CDP body: {:?}",
            String::from_utf8_lossy(&body)
        );
        server.join().unwrap();
    }

    #[test]
    fn snapshot_refs_are_qualified_scoped_and_backward_compatible() {
        use super::*;
        let dir = tempfile::tempdir().unwrap();
        let element: InteractiveRef = serde_json::from_value(serde_json::json!({
            "ref":"@e1", "tag":"button", "text":"Save", "selector":"#save", "x":30.0, "y":40.0, "width":50.0, "height":20.0
        })).unwrap();
        let refs = StoredRefs {
            snapshot_id: "sfirst".into(),
            elements: vec![element.clone()],
            ..Default::default()
        };
        let path = profile_dir(dir.path(), "default").join("refs.json");
        write_profile_json(&path, &refs).unwrap();
        assert_eq!(
            qualified_elements("sfirst", &[element])[0]["qualified_ref"],
            "@sfirst:e1"
        );
        for target in ["@e1", "@sfirst:e1"] {
            let resolved = resolve_selector(dir.path(), "default", target).unwrap();
            assert!(resolved.css_selector.contains("@e1"));
            assert_eq!(resolved.fallback_xy, Some((30.0, 40.0)));
        }
        assert_eq!(
            resolve_selector(dir.path(), "default", "#save")
                .unwrap()
                .css_selector,
            "#save"
        );
        for target in ["@old:e1", "@sfirst:e99", "@sfirst:evil", "@e1\"]"] {
            assert!(
                resolve_selector(dir.path(), "default", target)
                    .unwrap_err()
                    .to_string()
                    .contains("STALE_REF")
            );
        }
        assert!(resolve_selector(dir.path(), "other", "@sfirst:e1").is_err());
        write_profile_json(
            &path,
            &StoredRefs {
                snapshot_id: "ssecond".into(),
                ..refs
            },
        )
        .unwrap();
        assert!(resolve_selector(dir.path(), "default", "@sfirst:e1").is_err());
        assert!(resolve_selector(dir.path(), "default", "@e1").is_ok());
    }

    #[test]
    fn browser_errors_have_four_dispositions_and_preserve_secret_redaction() {
        use super::*;
        for (message, kind) in [
            ("STALE_REF old snapshot", "stale_ref"),
            ("element not found", "settled_absence"),
            ("CDP timed out", "retryable_failure"),
            ("invalid selector", "terminal_failure"),
        ] {
            let err = browser_error(Error::Message(message.into()), false, false).to_string();
            assert!(err.contains(kind), "{err}");
            let secret = browser_error(
                Error::Message(format!("{message}: sensitive-value")),
                true,
                false,
            )
            .to_string();
            assert!(secret.contains(kind));
            assert!(!secret.contains("sensitive-value"));
        }
        let dir = tempfile::tempdir().unwrap();
        let err = execute(
            dir.path(),
            "default",
            BrowserRequest {
                action: "fill".into(),
                selector: Some("@old:e1".into()),
                secret_ref: Some("op://vault/item/password".into()),
                ..Default::default()
            },
            &crate::agent_file::FileAuthority::Own,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("STALE_REF"));
        assert!(!err.contains("vault"));
    }

    #[test]
    fn a_mutating_browser_action_that_timed_out_is_not_safe_to_retry() {
        use super::*;
        let request = |action: &str| BrowserRequest {
            action: action.into(),
            ..Default::default()
        };
        for action in [
            "click",
            "fill",
            "type",
            "press",
            "key_down",
            "open",
            "navigate",
            "eval",
            "takeover",
            "close",
            "frobnicate",
        ] {
            assert!(!browser_action_reads(&request(action)), "{action}");
        }
        for action in [
            "snapshot",
            "text",
            "screenshot",
            "status",
            "profiles",
            "wait_for_user",
        ] {
            assert!(browser_action_reads(&request(action)), "{action}");
        }
        assert!(browser_action_reads(&request("")));
        assert!(!browser_action_reads(&BrowserRequest {
            url: Some("https://example.com".into()),
            ..Default::default()
        }));
        assert!(browser_action_reads(&request("tabs")));
        assert!(!browser_action_reads(&BrowserRequest {
            tab_action: Some("new".into()),
            ..request("tabs")
        }));
        assert!(browser_action_reads(&request("cookies")));
        assert!(!browser_action_reads(&BrowserRequest {
            cookies: Some(Vec::new()),
            ..request("cookies")
        }));

        for (message, read, mutating) in [
            (
                "Input.dispatchMouseEvent: CDP timed out",
                "RETRYABLE_FAILURE",
                "DELIVERY_UNCERTAIN",
            ),
            (
                "Resource temporarily unavailable (os error 11)",
                "RETRYABLE_FAILURE",
                "DELIVERY_UNCERTAIN",
            ),
            // The browser never started, so nothing was dispatched.
            (
                "Chromium CDP startup timed out after 100ms",
                "RETRYABLE_FAILURE",
                "RETRYABLE_FAILURE",
            ),
            (
                "DevTools version HTTP status 503",
                "RETRYABLE_FAILURE",
                "RETRYABLE_FAILURE",
            ),
        ] {
            let as_read = browser_error(Error::Message(message.into()), false, false).to_string();
            assert!(as_read.contains(read), "{as_read}");
            let as_write = browser_error(Error::Message(message.into()), false, true).to_string();
            assert!(as_write.contains(mutating), "{as_write}");
        }
        let err =
            browser_error(Error::Message("CDP timed out: secret".into()), true, true).to_string();
        assert!(err.contains("\"retry\":\"verify_first\""), "{err}");
        assert!(err.contains("browser snapshot"), "{err}");
        assert!(!err.contains("CDP timed out: secret"), "{err}");
    }

    #[test]
    fn takeover_entropy_requires_a_complete_read() {
        let error = super::takeover_token_from_entropy(&[1u8; 15][..]).unwrap_err();
        assert!(matches!(error, anyagents_core::Error::Io(ref e)
            if e.kind() == std::io::ErrorKind::UnexpectedEof));
        let denied =
            std::io::Error::new(std::io::ErrorKind::PermissionDenied, "entropy unavailable");
        struct FailedEntropy(std::io::Error);
        impl std::io::Read for FailedEntropy {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(self.0.kind(), "entropy unavailable"))
            }
        }
        let error = super::takeover_token_from_entropy(FailedEntropy(denied)).unwrap_err();
        assert!(matches!(error, anyagents_core::Error::Io(ref e)
            if e.kind() == std::io::ErrorKind::PermissionDenied));
        assert_eq!(
            super::takeover_token_from_entropy(&[0xabu8; 16][..]).unwrap(),
            "tok_abababababababababababababababab"
        );
        assert_eq!(super::generate_takeover_token().unwrap().len(), 36);
    }

    use super::*;
    use anyagents_core::{CredentialStore, FileCredentialStore};
    use std::io::{Read, Write};
    use std::net::TcpListener;

    /// The tests' requests come from this process's own user, as the CLI's
    /// do; shadows the glob-imported [`super::execute`].
    fn execute(
        data: &Path,
        default_profile: &str,
        req: BrowserRequest,
    ) -> Result<serde_json::Value, Error> {
        super::execute(
            data,
            default_profile,
            req,
            &crate::agent_file::FileAuthority::Own,
        )
    }

    // A real local HTTP/WebSocket peer exercises the production CDP transport,
    // including server refusals. No Chromium or process-global env is needed.
    struct ScriptedBrowser {
        data: tempfile::TempDir,
        port: u16,
        calls: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
        stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
        worker: Option<std::thread::JoinHandle<()>>,
    }

    fn cdp_ok_response(request: &serde_json::Value) -> serde_json::Value {
        use serde_json::json;
        let result = match request["method"].as_str().unwrap() {
            "Runtime.evaluate" => {
                let expression = request["params"]["expression"].as_str().unwrap();
                let value = if expression.contains("const isVisible") {
                    json!({"url":"https://example.test/", "title":"Fixture", "text":"hello", "elements":[]})
                } else if expression.contains("found: false") {
                    json!({"found":true, "kind":"input", "had_content":true, "x":30, "y":40})
                } else {
                    json!(true)
                };
                json!({"result":{"value":value}})
            }
            "Page.getFrameTree" => json!({"frameTree":{"frame":{"id":"main"}}}),
            "Page.navigate" => json!({"frameId":"main"}),
            _ => json!({}),
        };
        json!({"result":result})
    }

    impl ScriptedBrowser {
        fn new(
            respond: impl Fn(&serde_json::Value) -> serde_json::Value + Send + Sync + 'static,
        ) -> Self {
            use std::sync::{
                Arc, Mutex,
                atomic::{AtomicBool, Ordering},
            };
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            listener.set_nonblocking(true).unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let calls = Arc::new(Mutex::new(Vec::new()));
            let responder = Arc::new(respond);
            let worker = {
                let stop = stop.clone();
                let calls = calls.clone();
                std::thread::spawn(move || {
                    let mut clients = Vec::new();
                    while !stop.load(Ordering::SeqCst) {
                        let (mut stream, _) = match listener.accept() {
                            Ok(pair) => pair,
                            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                                std::thread::sleep(Duration::from_millis(2));
                                continue;
                            }
                            Err(err) => panic!("fixture accept: {err}"),
                        };
                        // macOS inherits O_NONBLOCK from the listener; Linux
                        // does not. CDP framing below expects blocking reads.
                        stream.set_nonblocking(false).unwrap();
                        let calls = calls.clone();
                        let respond = responder.clone();
                        clients.push(std::thread::spawn(move || {
                            stream.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
                            let mut header = Vec::new();
                            let mut byte = [0];
                            while !header.ends_with(b"\r\n\r\n") {
                                if stream.read_exact(&mut byte).is_err() { return; }
                                header.push(byte[0]);
                            }
                            let header = String::from_utf8(header).unwrap();
                            if header.contains("Upgrade: websocket") {
                                stream.write_all(b"HTTP/1.1 101 Switching Protocols\r\n\r\n").unwrap();
                                let mut peer = CdpSession { port, stream, read_buf:Vec::new(), next_id:1, takeover_mouse_owner:None, takeover_cancel_registered:false, takeover_cancel_children:Default::default() };
                                while let Ok(message) = peer.read_text_message() {
                                    let request: serde_json::Value = serde_json::from_str(&message).unwrap();
                                    calls.lock().unwrap().push(request.clone());
                                    let mut response = respond(&request);
                                    response["id"] = request["id"].clone();
                                    if peer.send_text_frame(response.to_string().as_bytes()).is_err() { break; }
                                }
                            } else {
                                let path = header.split_whitespace().nth(1).unwrap();
                                let (status, body) = if path == "/json/list" {
                                    let listed = respond(&serde_json::json!({"method":"list_url"}));
                                    let url = listed["url"].as_str().unwrap_or("https://example.test/").to_string();
                                    ("200 OK", serde_json::json!([{
                                        "id":"page", "type":"page", "url":url, "title":"Fixture",
                                        "webSocketDebuggerUrl":format!("ws://127.0.0.1:{port}/devtools/page/page")
                                    }]).to_string())
                                } else {
                                    let request = serde_json::json!({"method":path});
                                    calls.lock().unwrap().push(request.clone());
                                    if respond(&request).get("error").is_some() {
                                        ("404 Not Found", "missing target".into())
                                    } else { ("200 OK", "acknowledged".into()) }
                                };
                                write_http_response(&mut stream, status, "application/json", body.as_bytes()).unwrap();
                            }
                        }));
                    }
                    for client in clients {
                        client.join().unwrap();
                    }
                })
            };
            let data = tempfile::tempdir().unwrap();
            ensure_fingerprint(data.path(), "fixture").unwrap();
            save_runtime(
                data.path(),
                &BrowserRuntimeState {
                    profile: "fixture".into(),
                    pid: std::process::id(),
                    cdp_port: port,
                    headless: true,
                    display_mode: "test".into(),
                    proxy: None,
                    active_target_id: Some("page".into()),
                    started_at_ms: now_ms(),
                    last_used_at_ms: now_ms(),
                },
            )
            .unwrap();
            Self {
                data,
                port,
                calls,
                stop,
                worker: Some(worker),
            }
        }
        fn session(&self) -> CdpSession {
            CdpSession::connect(
                self.port,
                &format!("ws://127.0.0.1:{}/devtools/page/page", self.port),
            )
            .unwrap()
        }
        fn execute(&self, req: BrowserRequest) -> Result<serde_json::Value, Error> {
            execute(self.data.path(), "fixture", req)
        }
        fn input_calls(&self) -> Vec<serde_json::Value> {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|c| c["method"].as_str().unwrap().starts_with("Input."))
                .cloned()
                .collect()
        }
    }
    impl Drop for ScriptedBrowser {
        fn drop(&mut self) {
            self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
            self.worker.take().unwrap().join().unwrap();
        }
    }

    #[test]
    fn scripted_cdp_peer_waits_for_a_delayed_command() {
        let fixture = ScriptedBrowser::new(cdp_ok_response);
        let mut session = fixture.session();
        // A nonblocking accepted socket closes before this command arrives.
        std::thread::sleep(Duration::from_millis(20));
        session.call("Page.enable", serde_json::json!({})).unwrap();
        assert_eq!(fixture.calls.lock().unwrap().len(), 1);
    }

    #[test]
    #[cfg(unix)]
    fn process_liveness_tracks_owned_child_without_procfs() {
        assert!(process_alive(std::process::id()));
        assert!(!process_alive(0));
        assert!(!process_alive(u32::MAX));
        let dir = tempfile::tempdir().unwrap();
        let mut child = startup_test_child(&dir.path().join("child.log"), "exec sleep 30");
        let pid = child.id();
        assert!(process_alive(pid));
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(!process_alive(pid));
    }

    #[test]
    fn takeover_mouse_publication_failure_prevents_native_press() {
        let fixture = ScriptedBrowser::new(|request| {
            let result = match request["method"].as_str().unwrap() {
                "Page.addScriptToEvaluateOnNewDocument" => {
                    serde_json::json!({"identifier":"guard"})
                }
                "Page.createIsolatedWorld" => serde_json::json!({"executionContextId":1}),
                "Target.getTargets" => serde_json::json!({"targetInfos":[]}),
                _ => return cdp_ok_response(request),
            };
            serde_json::json!({"result":result})
        });
        let path = takeover_mouse_path(fixture.data.path(), "fixture");
        std::fs::create_dir(&path).unwrap();
        let mut cdp = fixture.session();
        cdp.takeover_mouse_owner = Some((path.clone(), "page".into()));
        let payload = serde_json::json!({"type":"mouse","event_type":"mousePressed","x":30,"y":40});
        let error = dispatch_takeover_cdp_input(&mut cdp, &payload).unwrap_err();
        assert!(
            error.to_string().to_ascii_lowercase().contains("directory"),
            "{error}"
        );
        assert!(
            fixture
                .input_calls()
                .iter()
                .all(|call| call["params"]["type"] != "mousePressed")
        );
        std::fs::remove_dir(&path).unwrap();
        dispatch_takeover_cdp_input(&mut cdp, &payload).unwrap();
        let retained = read_takeover_mouse(&path).unwrap().unwrap();
        assert_eq!(retained.port, fixture.port);
        assert_eq!(retained.target_id, "page");
        assert_eq!(retained.payload["x"], 30.0);
        assert_eq!(
            fixture
                .input_calls()
                .iter()
                .filter(|call| call["params"]["type"] == "mousePressed")
                .count(),
            1
        );
        cancel_takeover_mouse(&mut cdp, &payload).unwrap();
        assert!(read_takeover_mouse(&path).unwrap().is_none());
        assert_eq!(
            fixture
                .input_calls()
                .iter()
                .filter(|call| call["params"]["type"] == "mouseReleased")
                .count(),
            1
        );
    }

    #[test]
    fn persistence_distinguishes_missing_corruption_and_io_errors() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load_runtime(dir.path(), "profile").unwrap().is_none());
        assert!(
            load_takeover_state(dir.path(), "profile")
                .unwrap()
                .is_none()
        );
        assert!(load_held_keys(dir.path(), "profile").unwrap().is_empty());
        let profile = profile_dir(dir.path(), "profile");
        std::fs::create_dir_all(profile.join("user-data")).unwrap();
        let lock = profile.join("user-data/SingletonLock");
        std::fs::write(&lock, b"retained").unwrap();
        for file in [
            "runtime.json",
            "takeover.json",
            "held_keys.json",
            "fingerprint.json",
        ] {
            let path = profile.join(file);
            std::fs::write(&path, b"{corrupt").unwrap();
            let error = match file {
                "runtime.json" => {
                    ensure_browser_running(dir.path(), "profile", Some(true), None).unwrap_err()
                }
                "takeover.json" => {
                    check_human_takeover_fence(dir.path(), "profile", "click").unwrap_err()
                }
                "held_keys.json" => load_held_keys(dir.path(), "profile").unwrap_err(),
                _ => ensure_fingerprint(dir.path(), "profile").unwrap_err(),
            };
            assert!(error.to_string().contains(file), "{error}");
            assert_eq!(std::fs::read(&path).unwrap(), b"{corrupt");
            std::fs::remove_file(&path).unwrap();
            std::fs::create_dir(&path).unwrap();
            let error = match file {
                "runtime.json" => load_runtime(dir.path(), "profile").unwrap_err(),
                "takeover.json" => load_takeover_state(dir.path(), "profile").unwrap_err(),
                "held_keys.json" => load_held_keys(dir.path(), "profile").unwrap_err(),
                _ => ensure_fingerprint(dir.path(), "profile").unwrap_err(),
            };
            assert!(error.to_string().contains(file), "{error}");
            std::fs::remove_dir(&path).unwrap();
        }
        assert_eq!(std::fs::read(lock).unwrap(), b"retained");
        let fp = ensure_fingerprint(dir.path(), "profile").unwrap();
        assert_eq!(
            ensure_fingerprint(dir.path(), "profile").unwrap().seed,
            fp.seed
        );
    }

    #[test]
    fn live_process_without_cdp_is_not_replaced() {
        let fixture = ScriptedBrowser::new(cdp_ok_response);
        let mut runtime = load_runtime(fixture.data.path(), "fixture")
            .unwrap()
            .unwrap();
        let port = TcpListener::bind("127.0.0.1:0").unwrap();
        runtime.cdp_port = port.local_addr().unwrap().port();
        drop(port);
        save_runtime(fixture.data.path(), &runtime).unwrap();
        let err = load_runtime(fixture.data.path(), "fixture").unwrap_err();
        assert!(
            err.to_string().contains("alive but CDP is unavailable"),
            "{err}"
        );
        assert!(
            profile_dir(fixture.data.path(), "fixture")
                .join("runtime.json")
                .is_file()
        );
    }

    #[test]
    fn required_profile_publications_propagate_failures() {
        let fixture = ScriptedBrowser::new(cdp_ok_response);
        let data = fixture.data.path();
        let dir = profile_dir(data, "fixture");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let new_profile = profile_dir(data, "cannot-publish-fingerprint");
            std::fs::create_dir_all(&new_profile).unwrap();
            std::fs::set_permissions(&new_profile, std::fs::Permissions::from_mode(0o500)).unwrap();
            let result = ensure_fingerprint(data, "cannot-publish-fingerprint");
            std::fs::set_permissions(&new_profile, std::fs::Permissions::from_mode(0o700)).unwrap();
            if unsafe { libc::geteuid() } != 0 {
                assert!(
                    result.is_err(),
                    "unwritable fingerprint must not initialize successfully"
                );
                assert!(!new_profile.join("fingerprint.json").exists());
            }
        }
        let runtime = load_runtime(data, "fixture").unwrap().unwrap();
        std::fs::remove_file(dir.join("runtime.json")).unwrap();
        std::fs::create_dir(dir.join("runtime.json")).unwrap();
        assert!(save_runtime(data, &runtime).is_err());
        std::fs::remove_dir(dir.join("runtime.json")).unwrap();
        save_runtime(data, &runtime).unwrap();
        std::fs::create_dir(dir.join("held_keys.json")).unwrap();
        let err = fixture
            .execute(BrowserRequest {
                action: "key_down".into(),
                key: Some("a".into()),
                ..Default::default()
            })
            .unwrap_err();
        assert!(err.to_string().contains("held_keys.json"), "{err}");
        assert!(
            fixture.input_calls().is_empty(),
            "failed ledger admission must precede keyDown"
        );
        assert!(save_held_keys(data, "fixture", &[resolve_key_spec("a")]).is_err());
        std::fs::remove_dir(dir.join("held_keys.json")).unwrap();
        save_held_keys(data, "fixture", &[resolve_key_spec("a")]).unwrap();
        assert_eq!(load_held_keys(data, "fixture").unwrap()[0].key, "a");
        clear_held_keys(data, "fixture").unwrap();
        std::fs::create_dir(dir.join("refs.json")).unwrap();
        let err = capture_snapshot(data, "fixture", &mut fixture.session()).unwrap_err();
        assert!(
            err.to_string().contains("directory") || err.to_string().contains("Directory"),
            "{err}"
        );
        std::fs::remove_dir(dir.join("refs.json")).unwrap();
        let snap = capture_snapshot(data, "fixture", &mut fixture.session()).unwrap();
        assert_eq!(snap["title"], "Fixture");
        assert_eq!(
            read_profile_json::<StoredRefs>(&dir.join("refs.json"))
                .unwrap()
                .unwrap()
                .url,
            "https://example.test/"
        );
        let state = start_or_reuse_takeover_session(
            data,
            "fixture",
            None,
            None,
            None,
            Some(false),
            &CaptchaDetection::default(),
        )
        .unwrap();
        std::fs::remove_file(dir.join("takeover.json")).unwrap();
        std::fs::create_dir(dir.join("takeover.json")).unwrap();
        assert!(save_takeover_state(data, &state).is_err());
        assert!(
            start_or_reuse_takeover_session(
                data,
                "fixture",
                None,
                None,
                None,
                Some(false),
                &CaptchaDetection::default()
            )
            .is_err()
        );
        std::fs::remove_dir(dir.join("takeover.json")).unwrap();
        // End the fixture's listener through its normal persisted state.
        let mut state = state;
        state.completed = true;
        save_takeover_state(data, &state).unwrap();
    }

    #[test]
    fn takeover_http_rejects_malformed_actions_without_mutation() {
        let fixture = ScriptedBrowser::new(cdp_ok_response);
        let state = start_or_reuse_takeover_session(
            fixture.data.path(),
            "fixture",
            None,
            None,
            None,
            Some(false),
            &CaptchaDetection::default(),
        )
        .unwrap();
        let before = std::fs::read(takeover_path(fixture.data.path(), "fixture")).unwrap();
        for endpoint in ["input", "viewport"] {
            for body in ["{", "null", "[]", "{}"] {
                let (status, response) = raw_http_call(
                    state.port,
                    "POST",
                    &format!("/takeover/{}/{endpoint}", state.token),
                    Some(body),
                );
                assert_eq!(
                    status,
                    400,
                    "{endpoint} {body}: {}",
                    String::from_utf8_lossy(&response)
                );
                assert_eq!(
                    std::fs::read(takeover_path(fixture.data.path(), "fixture")).unwrap(),
                    before
                );
                assert!(fixture.input_calls().is_empty());
            }
        }
        for body in [
            r#"{"enabled":"false"}"#,
            r#"{"enabled":true,"width":"bad"}"#,
        ] {
            let (status, _) = raw_http_call(
                state.port,
                "POST",
                &format!("/takeover/{}/viewport", state.token),
                Some(body),
            );
            assert_eq!(status, 400);
            assert_eq!(
                std::fs::read(takeover_path(fixture.data.path(), "fixture")).unwrap(),
                before
            );
        }
        let (status, _) = raw_http_call(
            state.port,
            "POST",
            &format!("/takeover/{}/viewport", state.token),
            Some(r#"{"enabled":false}"#),
        );
        assert_eq!(status, 200);
        let (status, _) = raw_http_call(
            state.port,
            "POST",
            &format!("/takeover/{}/input", state.token),
            Some(r#"{"type":"click","x":1,"y":2}"#),
        );
        assert_eq!(status, 200);
        assert_eq!(fixture.input_calls().len(), 3);
        complete_takeover_session(fixture.data.path(), "fixture", None).unwrap();
    }

    #[test]
    fn stealth_reports_each_command_and_script_failure() {
        for method in [
            "Page.enable",
            "Runtime.enable",
            "DOM.enable",
            "Network.enable",
            "Emulation.setUserAgentOverride",
            "Emulation.setTimezoneOverride",
            "Page.addScriptToEvaluateOnNewDocument",
            "Runtime.evaluate",
        ] {
            let fixture = ScriptedBrowser::new({
                move |request| {
                    if request["method"] == method {
                        serde_json::json!({"error":{"message":"stealth refused"}})
                    } else {
                        cdp_ok_response(request)
                    }
                }
            });
            let fp = ensure_fingerprint(fixture.data.path(), "fixture").unwrap();
            let result = apply_stealth(&mut fixture.session(), &fp);
            // CDP calls this method addScriptToEvaluateOnNewDocument.
            assert!(
                result.unwrap_err().to_string().contains("stealth refused"),
                "{method}"
            );
        }
        let fixture = ScriptedBrowser::new(|request| {
            if request["method"] == "Runtime.evaluate" {
                serde_json::json!({"result":{"exceptionDetails":{"text":"override rejected"}}})
            } else {
                cdp_ok_response(request)
            }
        });
        let fp = ensure_fingerprint(fixture.data.path(), "fixture").unwrap();
        assert!(
            apply_stealth(&mut fixture.session(), &fp)
                .unwrap_err()
                .to_string()
                .contains("override rejected")
        );
        let fixture = ScriptedBrowser::new(cdp_ok_response);
        let fp = ensure_fingerprint(fixture.data.path(), "fixture").unwrap();
        apply_stealth(&mut fixture.session(), &fp).unwrap();
    }

    #[test]
    #[ignore = "launches real Chromium; CDP I/O fails intermittently with EINTR in CI; deleted with browser.rs, tracking: https://github.com/SylphxAI/anyagents/issues/200"]
    fn chromium_stealth_override_exception_is_observable() {
        let _browser = browser_fixture_guard();
        if !has_chromium() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let (mut runtime, fp) =
            ensure_browser_running(dir.path(), "override", Some(true), None).unwrap();
        let page = select_page_target(dir.path(), &mut runtime, None).unwrap();
        let mut session =
            CdpSession::connect(runtime.cdp_port, page.ws_url.as_deref().unwrap()).unwrap();
        apply_stealth(&mut session, &fp).unwrap();
        eval_js(&mut session, "Object.defineProperty(navigator, 'hardwareConcurrency', { value:1, configurable:false }); true").unwrap();
        let result = apply_stealth(&mut session, &fp);
        drop(session);
        stop_profile(dir.path(), "override").unwrap();
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("hardwareConcurrency")
        );
    }

    #[test]
    #[ignore = "launches real Chromium; CDP I/O fails intermittently with EINTR in CI; deleted with browser.rs, tracking: https://github.com/SylphxAI/anyagents/issues/200"]
    fn chromium_fill_requires_the_requested_value_or_option() {
        let _browser = browser_fixture_guard();
        if !has_chromium() {
            return;
        }
        let data = tempfile::tempdir().unwrap();
        let (mut runtime, _) =
            ensure_browser_running(data.path(), "input-value", Some(true), None).unwrap();
        let target = select_page_target(data.path(), &mut runtime, None).unwrap();
        let mut session =
            CdpSession::connect(runtime.cdp_port, target.ws_url.as_deref().unwrap()).unwrap();
        eval_js(&mut session, r#"document.body.innerHTML = '<input id="field" maxlength="1"><select id="choice"><option value="a">First option</option><option value="b">Second option</option></select>'; true"#).unwrap();
        let fill = |selector: &str, value: &str| {
            execute(
                data.path(),
                "input-value",
                BrowserRequest {
                    action: "fill".into(),
                    selector: Some(selector.into()),
                    text: Some(value.into()),
                    ..Default::default()
                },
            )
        };
        assert!(
            fill("#field", "ab")
                .unwrap_err()
                .to_string()
                .contains("input value verification failed")
        );
        assert_eq!(
            eval_js(&mut session, "document.querySelector('#field').value").unwrap(),
            "a"
        );
        assert!(
            fill("#choice", "missing option")
                .unwrap_err()
                .to_string()
                .contains("select option was not accepted")
        );
        fill("#field", "z").unwrap();
        fill("#choice", "Second option").unwrap();
        assert_eq!(
            eval_js(&mut session, "document.querySelector('#field').value").unwrap(),
            "z"
        );
        assert_eq!(
            eval_js(&mut session, "document.querySelector('#choice').value").unwrap(),
            "b"
        );
        drop(session);
        stop_profile(data.path(), "input-value").unwrap();
    }

    #[test]
    #[ignore = "launches real Chromium; CDP I/O fails intermittently with EINTR in CI; deleted with browser.rs, tracking: https://github.com/SylphxAI/anyagents/issues/200"]
    fn chromium_split_otp_fill_verifies_all_cells_before_submit() {
        let _browser = browser_fixture_guard();
        // The default suite permits hosts without Chromium. An explicitly
        // configured browser remains required, including a broken path.
        match chromium_binary() {
            Ok(bin) if bin.exists() => {}
            Ok(_) => panic!("Chromium is required for split OTP input verification"),
            Err(_) => return,
        }
        let data = tempfile::tempdir().unwrap();
        let (mut runtime, _) =
            ensure_browser_running(data.path(), "split-code", Some(true), None).unwrap();
        let target = select_page_target(data.path(), &mut runtime, None).unwrap();
        let mut session =
            CdpSession::connect(runtime.cdp_port, target.ws_url.as_deref().unwrap()).unwrap();
        let mut outcomes = Vec::new();
        for (cells, corrupt) in [(6, false), (6, true), (5, false)] {
            eval_js(
                &mut session,
                &format!(
                    r#"(() => {{
              document.body.innerHTML = '<form id="code"></form>';
              const form = document.querySelector('form');
              window.codeSubmissions = 0;
              window.trustedCodeInputs = 0;
              for (let i = 0; i < {cells}; i++) {{
                const field = document.createElement('input');
                field.id = 'digit-' + i;
                field.maxLength = 1;
                field.inputMode = 'numeric';
                field.addEventListener('input', event => {{
                  if (event.isTrusted) window.trustedCodeInputs++;
                  if ({corrupt} && i === 2) field.value = 'x';
                  if (field.value && field.nextElementSibling) field.nextElementSibling.focus();
                }});
                form.appendChild(field);
              }}
              form.addEventListener('keydown', event => {{
                if (event.key === 'Enter') {{ event.preventDefault(); window.codeSubmissions++; }}
              }});
              return true;
            }})()"#
                ),
            )
            .unwrap();
            let result = execute(
                data.path(),
                "split-code",
                BrowserRequest {
                    action: "fill".into(),
                    selector: Some("#digit-0".into()),
                    text: Some("123456".into()),
                    submit: Some(true),
                    ..Default::default()
                },
            );
            let observed = eval_js(&mut session, r#"({ value: Array.from(document.querySelectorAll('input')).map(el => el.value).join(''), submissions: window.codeSubmissions, trusted: window.trustedCodeInputs })"#).unwrap();
            outcomes.push((result, observed));
        }
        drop(session);
        stop_profile(data.path(), "split-code").unwrap();
        let (result, observed) = &outcomes[0];
        assert!(result.is_ok(), "split code must be accepted: {result:?}");
        assert_eq!(observed["value"], "123456");
        assert_eq!(observed["submissions"], 1);
        assert_eq!(observed["trusted"], 6);
        for (result, observed) in &outcomes[1..] {
            assert!(
                result
                    .as_ref()
                    .unwrap_err()
                    .to_string()
                    .contains("input value verification failed")
            );
            assert_ne!(observed["value"], "123456");
            assert_eq!(
                observed["submissions"], 0,
                "an incomplete or wrong split code cannot submit"
            );
        }
    }

    #[test]
    fn navigation_and_tab_refusals_do_not_acknowledge_or_change_target() {
        for action in ["navigate", "reload"] {
            let fixture = ScriptedBrowser::new(move |request| {
                if request["method"] == "Page.navigate" || request["method"] == "Page.reload" {
                    serde_json::json!({"error":{"message":"navigation refused"}})
                } else {
                    cdp_ok_response(request)
                }
            });
            let result = dispatch_takeover_input(
                fixture.data.path(),
                "fixture",
                &serde_json::json!({"type":action,"url":"https://example.test/new"}),
            );
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("navigation refused")
            );
            if action == "navigate" {
                let err = fixture
                    .execute(BrowserRequest {
                        action: "takeover".into(),
                        url: Some("https://example.test/new".into()),
                        ..Default::default()
                    })
                    .unwrap_err();
                assert!(err.to_string().contains("navigation refused"));
                assert!(
                    load_takeover_state(fixture.data.path(), "fixture")
                        .unwrap()
                        .is_none()
                );
            }
        }
        let fixture = ScriptedBrowser::new(|request| {
            if request["method"] == "Page.navigate" {
                serde_json::json!({"result":{"errorText":"net::ERR_BLOCKED_BY_CLIENT"}})
            } else if request["method"].as_str().unwrap().starts_with("/json/") {
                serde_json::json!({"error":true})
            } else {
                cdp_ok_response(request)
            }
        });
        assert!(
            navigate_page(&mut fixture.session(), "https://example.test")
                .unwrap_err()
                .to_string()
                .contains("ERR_BLOCKED_BY_CLIENT")
        );
        for action in ["activate", "close"] {
            let err = fixture
                .execute(BrowserRequest {
                    action: "tabs".into(),
                    tab_action: Some(action.into()),
                    target_id: Some("page".into()),
                    ..Default::default()
                })
                .unwrap_err();
            assert!(err.to_string().contains("status 404"), "{err}");
            assert_eq!(
                load_runtime(fixture.data.path(), "fixture")
                    .unwrap()
                    .unwrap()
                    .active_target_id
                    .as_deref(),
                Some("page")
            );
        }
        let fixture = ScriptedBrowser::new(cdp_ok_response);
        dispatch_takeover_input(
            fixture.data.path(),
            "fixture",
            &serde_json::json!({"type":"navigate","url":"https://example.test/new"}),
        )
        .unwrap();
        for action in ["activate", "close"] {
            fixture
                .execute(BrowserRequest {
                    action: "tabs".into(),
                    tab_action: Some(action.into()),
                    target_id: Some("page".into()),
                    ..Default::default()
                })
                .unwrap();
        }
        assert!(
            load_runtime(fixture.data.path(), "fixture")
                .unwrap()
                .unwrap()
                .active_target_id
                .is_none()
        );
    }

    #[test]
    fn failed_click_fill_submit_and_focus_do_not_report_success() {
        let fixture = ScriptedBrowser::new(|request| {
            if request["method"] == "Input.dispatchMouseEvent" {
                serde_json::json!({"error":{"message":"pointer refused"}})
            } else {
                cdp_ok_response(request)
            }
        });
        assert!(
            fixture
                .execute(BrowserRequest {
                    action: "click".into(),
                    selector: Some("#button".into()),
                    ..Default::default()
                })
                .unwrap_err()
                .to_string()
                .contains("pointer refused")
        );
        for failure in [
            "delete-down",
            "delete-up",
            "text-down",
            "text-up",
            "insert",
            "finalize",
            "submit-down",
            "submit-up",
        ] {
            let fixture = ScriptedBrowser::new(move |request| {
                let p = &request["params"];
                let reject = match failure {
                    "delete-down" => p["key"] == "Backspace" && p["type"] == "keyDown",
                    "delete-up" => p["key"] == "Backspace" && p["type"] == "keyUp",
                    "text-down" => p["key"] == "x" && p["type"] == "keyDown",
                    "text-up" => p["key"] == "x" && p["type"] == "keyUp",
                    "insert" => request["method"] == "Input.insertText",
                    "finalize" => p["expression"]
                        .as_str()
                        .is_some_and(|s| s.contains("Object.getPrototypeOf(el)")),
                    "submit-down" => p["key"] == "Enter" && p["type"] == "keyDown",
                    "submit-up" => p["key"] == "Enter" && p["type"] == "keyUp",
                    _ => unreachable!(),
                };
                if reject {
                    serde_json::json!({"error":{"message":"input refused"}})
                } else {
                    cdp_ok_response(request)
                }
            });
            let err = fixture
                .execute(BrowserRequest {
                    action: "fill".into(),
                    selector: Some("#field".into()),
                    text: Some(if failure == "insert" {
                        "x".repeat(129)
                    } else {
                        "x".into()
                    }),
                    submit: Some(true),
                    ..Default::default()
                })
                .unwrap_err();
            assert!(
                err.to_string().contains("input refused"),
                "{failure}: {err}"
            );
        }
        let fixture = ScriptedBrowser::new(|request| {
            if request["params"]["expression"]
                .as_str()
                .is_some_and(|s| s.contains("document.activeElement === el"))
            {
                serde_json::json!({"result":{"result":{"value":false}}})
            } else {
                cdp_ok_response(request)
            }
        });
        let err = fixture
            .execute(BrowserRequest {
                action: "press".into(),
                selector: Some("#missing".into()),
                key: Some("Enter".into()),
                ..Default::default()
            })
            .unwrap_err();
        assert!(err.to_string().contains("could not be focused"));
        assert!(fixture.input_calls().is_empty());
        let secret_fixture = ScriptedBrowser::new(|request| {
            if request["method"] == "Input.dispatchKeyEvent" {
                serde_json::json!({"error":{"message":"synthetic-private-value was refused"}})
            } else {
                cdp_ok_response(request)
            }
        });
        FileCredentialStore::new(secret_fixture.data.path())
            .write("test/value", b"synthetic-private-value")
            .unwrap();
        let error = secret_fixture
            .execute(BrowserRequest {
                action: "fill".into(),
                selector: Some("#field".into()),
                secret_ref: Some("secret://test/value".into()),
                ..Default::default()
            })
            .unwrap_err();
        assert!(error.to_string().contains("details withheld"));
        assert!(!error.to_string().contains("synthetic-private-value"));
        let error = secret_fixture
            .execute(BrowserRequest {
                action: "fill".into(),
                selector: Some("#field".into()),
                text: Some(" secret://test/value ".into()),
                ..Default::default()
            })
            .unwrap_err();
        assert!(error.to_string().contains("details withheld"));
        assert!(!error.to_string().contains("synthetic-private-value"));
        let fixture = ScriptedBrowser::new(cdp_ok_response);
        fixture
            .execute(BrowserRequest {
                action: "fill".into(),
                selector: Some("#field".into()),
                text: Some("x".into()),
                submit: Some(true),
                ..Default::default()
            })
            .unwrap();
        fixture
            .execute(BrowserRequest {
                action: "click".into(),
                selector: Some("#button".into()),
                wait_ms: Some(1),
                ..Default::default()
            })
            .unwrap();
        fixture
            .execute(BrowserRequest {
                action: "press".into(),
                selector: Some("#field".into()),
                key: Some("Enter".into()),
                ..Default::default()
            })
            .unwrap();
    }

    #[test]
    fn failed_key_releases_remain_recoverable_including_guard_drop() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        let fail = Arc::new(AtomicBool::new(true));
        let fixture = ScriptedBrowser::new({
            let fail = fail.clone();
            move |request| {
                if fail.load(Ordering::SeqCst)
                    && request["params"]["type"] == "keyUp"
                    && request["params"]["key"] == "b"
                {
                    serde_json::json!({"error":{"message":"key release refused"}})
                } else {
                    cdp_ok_response(request)
                }
            }
        });
        save_held_keys(
            fixture.data.path(),
            "fixture",
            &[resolve_key_spec("a"), resolve_key_spec("b")],
        )
        .unwrap();
        let err =
            release_recorded_held_keys(fixture.data.path(), "fixture", &mut fixture.session())
                .unwrap_err();
        assert!(err.to_string().contains("key release refused"));
        let remaining = load_held_keys(fixture.data.path(), "fixture").unwrap();
        assert_eq!(
            remaining.iter().map(|s| s.key.as_str()).collect::<Vec<_>>(),
            ["b"]
        );
        {
            let mut session = fixture.session();
            let _guard = HeldKeyGuard {
                data: fixture.data.path(),
                profile: "fixture",
                cdp: &mut session,
                target_frame_id: None,
                specs: remaining,
                released: false,
            };
        }
        assert_eq!(
            load_held_keys(fixture.data.path(), "fixture")
                .unwrap()
                .len(),
            1
        );
        fail.store(false, Ordering::SeqCst);
        assert_eq!(
            release_recorded_held_keys(fixture.data.path(), "fixture", &mut fixture.session())
                .unwrap(),
            ["b"]
        );
        assert!(
            load_held_keys(fixture.data.path(), "fixture")
                .unwrap()
                .is_empty()
        );
        fixture
            .execute(BrowserRequest {
                action: "key_down".into(),
                key: Some("a".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            load_held_keys(fixture.data.path(), "fixture").unwrap()[0].key,
            "a"
        );
        fixture
            .execute(BrowserRequest {
                action: "release_keys".into(),
                ..Default::default()
            })
            .unwrap();
        assert!(
            load_held_keys(fixture.data.path(), "fixture")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn completion_notification_failure_is_visible_and_retry_is_deduplicated() {
        let fixture = ScriptedBrowser::new(cdp_ok_response);
        let store = anyagents_core::Store::open(fixture.data.path().join("state.db")).unwrap();
        let state = start_or_reuse_takeover_session(
            fixture.data.path(),
            "fixture",
            Some("reader"),
            None,
            None,
            Some(false),
            &CaptchaDetection::default(),
        )
        .unwrap();
        let err =
            complete_takeover_session(fixture.data.path(), "fixture", Some("done")).unwrap_err();
        assert!(
            err.to_string().contains("owner notification pending"),
            "{err}"
        );
        let pending = load_takeover_state(fixture.data.path(), "fixture")
            .unwrap()
            .unwrap();
        assert!(pending.completed && pending.notification_pending);
        assert!(
            wait_for_takeover_completion(fixture.data.path(), "fixture", 100)
                .unwrap_err()
                .to_string()
                .contains("notification pending")
        );
        assert!(
            cleanup_takeover_session(fixture.data.path(), "fixture")
                .unwrap_err()
                .to_string()
                .contains("notification pending")
        );
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("reader", "codex"))
            .unwrap();
        let connection = rusqlite::Connection::open(fixture.data.path().join("state.db")).unwrap();
        connection.execute_batch("CREATE TRIGGER refuse_completion BEFORE INSERT ON messages BEGIN SELECT RAISE(FAIL, 'completion queue refused'); END;").unwrap();
        let error =
            complete_takeover_session(fixture.data.path(), "fixture", Some("done")).unwrap_err();
        assert!(error.to_string().contains("completion queue refused"));
        assert!(store.inbox(&agent.id, None).unwrap().is_empty());
        assert!(
            load_takeover_state(fixture.data.path(), "fixture")
                .unwrap()
                .unwrap()
                .notification_pending
        );
        connection
            .execute_batch("DROP TRIGGER refuse_completion;")
            .unwrap();
        for _ in 0..2 {
            complete_takeover_session(fixture.data.path(), "fixture", Some("done")).unwrap();
        }
        assert!(
            !load_takeover_state(fixture.data.path(), "fixture")
                .unwrap()
                .unwrap()
                .notification_pending
        );
        let inbox = store.inbox(&agent.id, None).unwrap();
        assert_eq!(inbox.len(), 1);
        assert!(inbox[0].text.contains("Browser Takeover Completed"));
        assert_eq!(
            load_takeover_state(fixture.data.path(), "fixture")
                .unwrap()
                .unwrap()
                .started_at_ms,
            state.started_at_ms
        );
    }

    #[test]
    fn completed_takeover_retries_preserve_owner_lifecycle_and_receipt() {
        let fixture = ScriptedBrowser::new(cdp_ok_response);
        let store = anyagents_core::Store::open(fixture.data.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("reader", "codex"))
            .unwrap();
        store.set_agent_state(&agent.id, "idle").unwrap();
        start_or_reuse_takeover_session(
            fixture.data.path(),
            "fixture",
            Some("reader"),
            None,
            None,
            Some(false),
            &CaptchaDetection::default(),
        )
        .unwrap();
        let initial =
            complete_takeover_session(fixture.data.path(), "fixture", Some("first completion"))
                .unwrap();
        assert_eq!(store.agent_by_name("reader").unwrap().state, "running");
        store.set_agent_state(&agent.id, "archived").unwrap();
        let before = std::fs::read(takeover_path(fixture.data.path(), "fixture")).unwrap();
        let replay =
            complete_takeover_session(fixture.data.path(), "fixture", Some("repeat completion"))
                .unwrap();
        assert_eq!(
            store.agent_by_name("reader").unwrap().state,
            "archived",
            "replayed completion must not wake an archived owner"
        );
        assert_eq!(
            std::fs::read(takeover_path(fixture.data.path(), "fixture")).unwrap(),
            before
        );
        assert_eq!(replay["completed_at_ms"], initial["completed_at_ms"]);
        // The message can be queued before clearing the pending flag fails. Its
        // recovery must observe a duplicate receipt without replaying wake effects.
        let mut pending = load_takeover_state(fixture.data.path(), "fixture")
            .unwrap()
            .unwrap();
        pending.notification_pending = true;
        save_takeover_state(fixture.data.path(), &pending).unwrap();
        complete_takeover_session(fixture.data.path(), "fixture", Some("pending recovery"))
            .unwrap();
        assert_eq!(store.agent_by_name("reader").unwrap().state, "archived");
        let recovered = load_takeover_state(fixture.data.path(), "fixture")
            .unwrap()
            .unwrap();
        assert!(!recovered.notification_pending);
        assert_eq!(recovered.completed_at_ms, pending.completed_at_ms);
        assert_eq!(recovered.completion_note, pending.completion_note);
        assert_eq!(store.inbox(&agent.id, None).unwrap().len(), 1);
    }

    #[test]
    fn provider_configuration_preserves_documented_operator_settings() {
        let mut command = Command::new("env");
        command
            .env_clear()
            .env("HOME", "/synthetic/home")
            .env("OP_CONFIG_DIR", "/synthetic/config")
            .env("OP_SERVICE_ACCOUNT_TOKEN", "synthetic-not-a-credential");
        configure_op_command(&mut command);
        let envs: Vec<(&std::ffi::OsStr, Option<&std::ffi::OsStr>)> = command.get_envs().collect();
        assert!(envs.contains(&(
            std::ffi::OsStr::new("DESK_SECRET_READ"),
            Some(std::ffi::OsStr::new("1"))
        )));
        let output = command.output().unwrap();
        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout).unwrap();
        let mut settings = stdout.lines().collect::<Vec<_>>();
        settings.sort();
        assert_eq!(
            settings,
            [
                "DESK_SECRET_READ=1",
                "HOME=/synthetic/home",
                "OP_CONFIG_DIR=/synthetic/config",
                "OP_SERVICE_ACCOUNT_TOKEN=synthetic-not-a-credential",
            ]
        );
    }

    #[test]
    fn op_secret_and_otp_commands_include_desk_secret_read_env() {
        let read_cmd = build_op_secret_command(
            std::ffi::OsStr::new("op"),
            &["read", "--no-newline", "op://vault/item/password"],
        );
        let read_args: Vec<&std::ffi::OsStr> = read_cmd.get_args().collect();
        assert_eq!(
            read_args,
            [
                std::ffi::OsStr::new("read"),
                std::ffi::OsStr::new("--no-newline"),
                std::ffi::OsStr::new("op://vault/item/password"),
            ]
        );
        let read_envs: Vec<(&std::ffi::OsStr, Option<&std::ffi::OsStr>)> =
            read_cmd.get_envs().collect();
        assert!(read_envs.contains(&(
            std::ffi::OsStr::new("DESK_SECRET_READ"),
            Some(std::ffi::OsStr::new("1"))
        )));

        let otp_cmd = build_op_secret_command(
            std::ffi::OsStr::new("op"),
            &["item", "get", "item", "--vault", "vault", "--otp"],
        );
        let otp_args: Vec<&std::ffi::OsStr> = otp_cmd.get_args().collect();
        assert_eq!(
            otp_args,
            [
                std::ffi::OsStr::new("item"),
                std::ffi::OsStr::new("get"),
                std::ffi::OsStr::new("item"),
                std::ffi::OsStr::new("--vault"),
                std::ffi::OsStr::new("vault"),
                std::ffi::OsStr::new("--otp"),
            ]
        );
        let otp_envs: Vec<(&std::ffi::OsStr, Option<&std::ffi::OsStr>)> =
            otp_cmd.get_envs().collect();
        assert!(otp_envs.contains(&(
            std::ffi::OsStr::new("DESK_SECRET_READ"),
            Some(std::ffi::OsStr::new("1"))
        )));
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "launches real Chromium; CDP I/O fails intermittently with EINTR in CI; deleted with browser.rs, tracking: https://github.com/SylphxAI/anyagents/issues/200"]
    fn op_secret_and_otp_resolution_sets_desk_secret_read_and_never_leaks_values() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let shim = dir.path().join("fake-op");
        std::fs::write(
            &shim,
            r#"#!/bin/sh
if [ "${DESK_SECRET_READ:-}" != "1" ]; then
  echo "desk-tool-guard: refusing to print a 1Password secret value" >&2
  exit 2
fi
case "$*" in
  "read --no-newline op://vault-alpha/item-beta/password")
    printf 'in-memory-secret-value
'
    exit 0
    ;;
  "read --no-newline op://vault-alpha/item-beta/otp?attribute=otp")
    printf '987654
'
    exit 0
    ;;
  "item get item-beta --vault vault-alpha --otp")
    printf '654321
'
    exit 0
    ;;
  "item get standalone-item --otp")
    printf '112233
'
    exit 0
    ;;
  "read --no-newline op://vault-alpha/partial-leak/password" | "item get partial-leak --vault vault-alpha --otp")
    printf 'partial-stdout-secret-bytes'
    printf 'partial-stderr-secret-bytes op://vault-alpha/partial-leak/password
' >&2
    exit 2
    ;;
  "read --no-newline op://vault-alpha/empty-item/password" | "item get empty-item --vault vault-alpha --otp")
    printf '   
'
    exit 0
    ;;
  *)
    printf 'unexpected-args-secret-fragment
' >&2
    exit 1
    ;;
esac
"#,
        )
        .unwrap();
        std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o700)).unwrap();

        // Counterfactual check: without DESK_SECRET_READ=1, the guard shim refuses with exit code 2.
        let unguarded = Command::new(&shim)
            .env_remove("DESK_SECRET_READ")
            .args([
                "read",
                "--no-newline",
                "op://vault-alpha/item-beta/password",
            ])
            .output()
            .unwrap();
        assert_eq!(unguarded.status.code(), Some(2));

        // Positive checks: op:// and op-otp:// pass DESK_SECRET_READ=1 and resolve in memory.
        let (secret, src) = resolve_fill_value_with_op(
            dir.path(),
            None,
            Some("op://vault-alpha/item-beta/password"),
            Some(shim.as_os_str()),
        )
        .unwrap();
        assert_eq!(secret, "in-memory-secret-value");
        assert_eq!(src.as_deref(), Some("op://vault-alpha/item-beta/password"));

        let (attr_otp, attr_src) = resolve_fill_value_with_op(
            dir.path(),
            Some("op://vault-alpha/item-beta/otp?attribute=otp"),
            None,
            Some(shim.as_os_str()),
        )
        .unwrap();
        assert_eq!(attr_otp, "987654");
        assert_eq!(
            attr_src.as_deref(),
            Some("op://vault-alpha/item-beta/otp?attribute=otp")
        );

        let (otp, otp_src) = resolve_fill_value_with_op(
            dir.path(),
            None,
            Some("op-otp://vault-alpha/item-beta"),
            Some(shim.as_os_str()),
        )
        .unwrap();
        assert_eq!(otp, "654321");
        assert_eq!(otp_src.as_deref(), Some("op-otp://vault-alpha/item-beta"));

        let (single_otp, single_src) = resolve_fill_value_with_op(
            dir.path(),
            None,
            Some("op-otp://standalone-item"),
            Some(shim.as_os_str()),
        )
        .unwrap();
        assert_eq!(single_otp, "112233");
        assert_eq!(single_src.as_deref(), Some("op-otp://standalone-item"));

        // Negative checks: partial stdout/stderr on non-zero exit must never appear in errors.
        for failing_ref in [
            "op://vault-alpha/partial-leak/password",
            "op-otp://vault-alpha/partial-leak",
        ] {
            let err = resolve_fill_value_with_op(
                dir.path(),
                None,
                Some(failing_ref),
                Some(shim.as_os_str()),
            )
            .unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("exit code 2"), "{msg}");
            for forbidden in [
                "partial-stdout-secret-bytes",
                "partial-stderr-secret-bytes",
                "vault-alpha",
                "partial-leak",
                "password",
            ] {
                assert!(!msg.contains(forbidden), "leaked {forbidden} in {msg}");
            }

            let public_err = browser_error(err, true, true).to_string();
            assert!(public_err.contains("TERMINAL_FAILURE"), "{public_err}");
            assert!(
                public_err.contains("browser secret input failed (provider/page details withheld)"),
                "{public_err}"
            );
            for forbidden in [
                "partial-stdout-secret-bytes",
                "partial-stderr-secret-bytes",
                "vault-alpha",
                "partial-leak",
                "password",
            ] {
                assert!(
                    !public_err.contains(forbidden),
                    "leaked {forbidden} in {public_err}"
                );
            }
        }

        // Empty stdout, missing binary, and malformed references never echo secret or ref fragments.
        for empty_ref in [
            "op://vault-alpha/empty-item/password",
            "op-otp://vault-alpha/empty-item",
        ] {
            let msg = resolve_fill_value_with_op(
                dir.path(),
                None,
                Some(empty_ref),
                Some(shim.as_os_str()),
            )
            .unwrap_err()
            .to_string();
            assert!(msg.contains("empty value"), "{msg}");
            assert!(!msg.contains("vault-alpha"), "{msg}");
            assert!(!msg.contains("empty-item"), "{msg}");
        }

        let missing_bin = dir.path().join("nonexistent-op");
        for valid_ref in [
            "op://vault-alpha/item-beta/password",
            "op-otp://vault-alpha/item-beta",
        ] {
            let msg = resolve_fill_value_with_op(
                dir.path(),
                None,
                Some(valid_ref),
                Some(missing_bin.as_os_str()),
            )
            .unwrap_err()
            .to_string();
            assert!(msg.contains("failed to invoke `op` CLI"), "{msg}");
            assert!(!msg.contains("vault-alpha"), "{msg}");
            assert!(!msg.contains("item-beta"), "{msg}");
        }

        for malformed_ref in [
            "op://vault-alpha/only-two",
            "op-otp:///",
            "op-otp://vault-alpha/   ",
            "unsupported://vault-alpha/item-beta",
        ] {
            let msg = resolve_fill_value_with_op(
                dir.path(),
                None,
                Some(malformed_ref),
                Some(shim.as_os_str()),
            )
            .unwrap_err()
            .to_string();
            assert!(!msg.contains("vault-alpha"), "{msg}");
            assert!(!msg.contains("item-beta"), "{msg}");
            assert!(!msg.contains("only-two"), "{msg}");
        }
    }

    // Each real-browser scenario owns its Chromium processes until teardown.
    // Starting every fixture together can starve CDP startup on a small runner;
    // keep the production 12s readiness ceiling and all interaction assertions.
    fn has_chromium() -> bool {
        chromium_binary().is_ok_and(|path| path.exists())
    }

    static BROWSER_FIXTURE: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn browser_fixture_guard() -> std::sync::MutexGuard<'static, ()> {
        BROWSER_FIXTURE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(unix)]
    fn startup_test_child(log: &Path, command: &str) -> std::process::Child {
        use std::os::unix::process::CommandExt;
        Command::new("sh")
            .args(["-c", command])
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(std::fs::File::create(log).unwrap()))
            .spawn()
            .unwrap()
    }

    #[test]
    fn takeover_mouse_press_requires_complete_frame_census() {
        for mode in [
            "ready",
            "targets-failed",
            "targets-malformed",
            "tree-failed",
            "tree-malformed",
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let stream = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            stream.set_nodelay(true).unwrap();
            let endpoint = std::thread::spawn(move || {
                let (stream, _) = listener.accept().unwrap();
                stream.set_nodelay(true).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut server = CdpSession {
                    port: 0,
                    stream,
                    read_buf: Vec::new(),
                    next_id: 1,
                    takeover_mouse_owner: None,
                    takeover_cancel_registered: false,
                    takeover_cancel_children: Default::default(),
                };
                let mut calls = Vec::new();
                while let Ok(text) = server.read_text_message() {
                    let call: serde_json::Value = serde_json::from_str(&text).unwrap();
                    let method = call["method"].as_str().unwrap();
                    let mut response = serde_json::json!({"id": call["id"], "result": {}});
                    if (method == "Page.getFrameTree" && mode == "tree-failed")
                        || (method == "Target.getTargets" && mode == "targets-failed")
                    {
                        response = serde_json::json!({"id": call["id"], "error": {"message": "discovery refused"}});
                    } else if method == "Page.getFrameTree" {
                        response["result"] = if mode == "tree-malformed" {
                            serde_json::json!({"frameTree":{"frame":{"id":"main"},"childFrames":[{}]}})
                        } else {
                            serde_json::json!({"frameTree":{"frame":{"id":"main"},"childFrames":[{"frame":{"id":"child"}}]}})
                        };
                    } else if method == "Target.getTargets" {
                        response["result"] = if mode == "ready" {
                            serde_json::json!({"targetInfos":[]})
                        } else {
                            serde_json::json!({"targetInfos":[{"type":"iframe"}]})
                        };
                    } else if method == "Page.addScriptToEvaluateOnNewDocument" {
                        response["result"] = serde_json::json!({"identifier":"guard"});
                    } else if method == "Page.createIsolatedWorld" {
                        response["result"] = serde_json::json!({"executionContextId":1});
                    } else if method == "Runtime.evaluate" {
                        response["result"] =
                            serde_json::json!({"result":{"value":mode != "unacknowledged"}});
                    }
                    calls.push(call);
                    server
                        .send_text_frame(response.to_string().as_bytes())
                        .unwrap();
                }
                calls
            });
            let mut cdp = CdpSession {
                port: 0,
                stream,
                read_buf: Vec::new(),
                next_id: 1,
                takeover_mouse_owner: None,
                takeover_cancel_registered: false,
                takeover_cancel_children: Default::default(),
            };
            let result = dispatch_takeover_cdp_input(
                &mut cdp,
                &serde_json::json!({"type":"mouse","event_type":"mousePressed","x":100,"y":400}),
            );
            drop(cdp);
            let calls = endpoint.join().unwrap();
            if mode == "ready" {
                result.unwrap();
                let release = calls
                    .iter()
                    .position(|c| c["method"] == "Input.dispatchMouseEvent")
                    .unwrap();
                let installed: Vec<_> = calls[..release]
                    .iter()
                    .filter(|c| c["method"] == "Page.createIsolatedWorld")
                    .map(|c| c["params"]["frameId"].as_str().unwrap())
                    .collect();
                assert_eq!(
                    installed,
                    ["main", "child"],
                    "all live in-process frames are acknowledged"
                );
                assert_eq!(
                    release, 8,
                    "Page enable, registration and both installs acknowledged before any native mouse event"
                );
            } else {
                assert!(result.is_err(), "{mode}: incomplete frame census must fail");
                assert!(
                    !calls
                        .iter()
                        .any(|c| c["method"] == "Input.dispatchMouseEvent"),
                    "{mode}: no press before complete admission"
                );
            }
        }
    }

    #[test]
    fn takeover_mouse_press_requires_every_isolated_listener_acknowledgement() {
        for mode in ["ready", "failed", "unacknowledged", "slow"] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let stream = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            stream.set_nodelay(true).unwrap();
            let endpoint = std::thread::spawn(move || {
                let (stream, _) = listener.accept().unwrap();
                stream.set_nodelay(true).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut server = CdpSession {
                    port: 0,
                    stream,
                    read_buf: Vec::new(),
                    next_id: 1,
                    takeover_mouse_owner: None,
                    takeover_cancel_registered: false,
                    takeover_cancel_children: Default::default(),
                };
                let mut calls = Vec::new();
                let mut installs = 0;
                while let Ok(text) = server.read_text_message() {
                    let call: serde_json::Value = serde_json::from_str(&text).unwrap();
                    let method = call["method"].as_str().unwrap();
                    let mut response = serde_json::json!({"id": call["id"], "result": {}});
                    if method == "Page.getFrameTree" {
                        response["result"] = serde_json::json!({"frameTree":{"frame":{"id":"main"},"childFrames":[{"frame":{"id":"child"}}]}});
                    } else if method == "Target.getTargets" {
                        response["result"] = serde_json::json!({"targetInfos":[]});
                    } else if method == "Page.addScriptToEvaluateOnNewDocument" {
                        response["result"] = serde_json::json!({"identifier":"guard"});
                    } else if method == "Page.createIsolatedWorld" {
                        assert_eq!(call["params"]["worldName"], "anyagents-takeover-cancel");
                        response["result"] = serde_json::json!({"executionContextId": if call["params"]["frameId"] == "main" {1} else {2}});
                        if mode == "failed" && call["params"]["frameId"] == "child" {
                            response = serde_json::json!({"id":call["id"],"error":{"message":"installation refused"}});
                        }
                    } else if method == "Runtime.evaluate" {
                        assert!(call["params"]["contextId"].as_u64().is_some());
                        installs += 1;
                        if mode == "slow" && installs == 1 {
                            std::thread::sleep(Duration::from_millis(210));
                        }
                        response["result"] =
                            serde_json::json!({"result":{"value":mode != "unacknowledged"}});
                    }
                    calls.push(call);
                    server
                        .send_text_frame(response.to_string().as_bytes())
                        .unwrap();
                }
                calls
            });
            let mut cdp = CdpSession {
                port: 0,
                stream,
                read_buf: Vec::new(),
                next_id: 1,
                takeover_mouse_owner: None,
                takeover_cancel_registered: false,
                takeover_cancel_children: Default::default(),
            };
            let result = dispatch_takeover_cdp_input(
                &mut cdp,
                &serde_json::json!({"type":"mouse","event_type":"mousePressed","x":100,"y":400,"button":"left"}),
            );
            assert_eq!(result.is_ok(), matches!(mode, "ready" | "slow"));
            drop(cdp);
            let calls = endpoint.join().unwrap();
            let releases: Vec<_> = calls
                .iter()
                .enumerate()
                .filter(|(_, c)| c["method"] == "Input.dispatchMouseEvent")
                .collect();
            if matches!(mode, "ready" | "slow") {
                assert_eq!(releases.len(), 2);
                assert_eq!(
                    releases[0].0, 8,
                    "both isolated installs are acknowledged before press"
                );
                assert_eq!(releases[0].1["params"]["type"], "mouseMoved");
                assert_eq!(releases[1].1["params"]["type"], "mousePressed");
            } else {
                assert!(
                    releases.is_empty(),
                    "failed installation must withhold the uncommitted press"
                );
            }
        }
    }

    #[test]
    fn cancelled_mouse_always_dispatches_terminal_release_without_discovery() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let stream = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        stream.set_nodelay(true).unwrap();
        let endpoint = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream.set_nodelay(true).unwrap();
            let mut server = CdpSession {
                port: 0,
                stream,
                read_buf: Vec::new(),
                next_id: 1,
                takeover_mouse_owner: None,
                takeover_cancel_registered: false,
                takeover_cancel_children: Default::default(),
            };
            let mut calls = Vec::new();
            while let Ok(text) = server.read_text_message() {
                let call: serde_json::Value = serde_json::from_str(&text).unwrap();
                // The page and native input remain alive, but every discovery
                // or script operation fails slowly, as in the reviewed fault.
                let response = if call["method"] == "Input.dispatchMouseEvent" {
                    serde_json::json!({"id":call["id"],"result":{}})
                } else {
                    std::thread::sleep(Duration::from_millis(210));
                    serde_json::json!({"id":call["id"],"error":{"message":"discovery unavailable"}})
                };
                calls.push(call);
                server
                    .send_text_frame(response.to_string().as_bytes())
                    .unwrap();
            }
            calls
        });
        let mut cdp = CdpSession {
            port: 0,
            stream,
            read_buf: Vec::new(),
            next_id: 1,
            takeover_mouse_owner: None,
            takeover_cancel_registered: false,
            takeover_cancel_children: Default::default(),
        };
        let result = cancel_takeover_mouse(
            &mut cdp,
            &serde_json::json!({"x":100,"y":400,"button":"left"}),
        );
        drop(cdp);
        let calls = endpoint.join().unwrap();
        assert_eq!(
            calls.len(),
            1,
            "terminal release has no discovery/install dependency"
        );
        assert_eq!(calls[0]["method"], "Input.dispatchMouseEvent");
        assert_eq!(
            calls[0]["params"],
            serde_json::json!({"type":"mouseReleased","x":100.0,"y":400.0,"button":"left","buttons":0,"clickCount":1,"modifiers":15})
        );
        result.unwrap();
    }

    #[test]
    #[ignore = "launches real Chromium; CDP I/O fails intermittently with EINTR in CI; deleted with browser.rs, tracking: https://github.com/SylphxAI/anyagents/issues/200"]
    fn takeover_prearmed_guard_survives_navigation_before_native_press() {
        let _browser = browser_fixture_guard();
        if !has_chromium() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let html = r#"<!doctype html><button id="target" style="position:fixed;left:50px;top:50px;width:200px;height:100px">Hold</button>
        <script>window.clicks=0;window.held=false;window.pointer=0;const target=document.querySelector('#target');
        target.onpointerdown=e=>{window.pointer=e.pointerId;window.held=true;target.setPointerCapture(e.pointerId);e.preventDefault()};
        target.onpointerup=()=>window.held=false;target.onclick=()=>window.clicks++;</script>"#.to_string();
        let port = spawn_html_server("127.0.0.1", html);
        let url = format!("http://127.0.0.1:{port}/");
        execute(
            dir.path(),
            "guard-navigation",
            BrowserRequest {
                action: "open".into(),
                headless: Some(true),
                url: Some(url.clone()),
                ..Default::default()
            },
        )
        .unwrap();
        let result = (|| -> Result<(), Error> {
            let mut rt = load_runtime(dir.path(), "guard-navigation")?.unwrap();
            let ws = select_page_target(dir.path(), &mut rt, None)?
                .ws_url
                .unwrap();
            let mut cdp = CdpSession::connect(rt.cdp_port, &ws)?;
            prepare_takeover_mouse(&mut cdp)?;
            // Deliberately replace the acknowledged document at the exact gap
            // between admission and native press; do not prepare again.
            cdp.call(
                "Page.navigate",
                serde_json::json!({"url": format!("{url}?replacement")}),
            )?;
            let ready = std::time::Instant::now();
            loop {
                if eval_js(
                    &mut cdp,
                    "location.search==='?replacement' && window.clicks===0 && !!document.querySelector('#target')",
                )
                .ok()
                    == Some(serde_json::json!(true))
                {
                    break;
                }
                assert!(
                    ready.elapsed() < Duration::from_secs(5),
                    "replacement document did not become ready"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
            cdp.call(
                "Input.dispatchMouseEvent",
                serde_json::json!({"type":"mouseMoved","x":100,"y":100,"buttons":0}),
            )?;
            cdp.call("Input.dispatchMouseEvent", serde_json::json!({"type":"mousePressed","x":100,"y":100,"button":"left","buttons":1,"clickCount":1}))?;
            cdp.call("Input.dispatchMouseEvent", serde_json::json!({"type":"mouseMoved","x":101,"y":100,"button":"left","buttons":1}))?;
            assert_eq!(
                eval_js(
                    &mut cdp,
                    "window.held && document.querySelector('#target').hasPointerCapture(window.pointer)"
                )?,
                serde_json::json!(true)
            );
            cancel_takeover_mouse(
                &mut cdp,
                &serde_json::json!({"x":101,"y":100,"button":"left"}),
            )?;
            assert_eq!(
                eval_js(
                    &mut cdp,
                    "({clicks:window.clicks,held:window.held,captured:document.querySelector('#target').hasPointerCapture(window.pointer)})"
                )?,
                serde_json::json!({"clicks":0,"held":false,"captured":false})
            );
            dispatch_trusted_click(&mut cdp, 100.0, 100.0)?;
            assert_eq!(eval_js(&mut cdp, "window.clicks")?, serde_json::json!(1));
            Ok(())
        })();
        let _ = execute(
            dir.path(),
            "guard-navigation",
            BrowserRequest {
                action: "close".into(),
                ..Default::default()
            },
        );
        result.unwrap();
    }

    #[test]
    #[cfg(unix)]
    fn chromium_startup_waits_for_version_endpoint_and_preserves_live_child() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("browser.log");
        let mut child = startup_test_child(&log, "exec sleep 30");
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let endpoint = std::thread::spawn(move || {
            for attempt in 0..3 {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                let mut request = [0; 1024];
                let size = stream.read(&mut request).unwrap();
                assert!(
                    String::from_utf8_lossy(&request[..size]).starts_with("GET /json/version ")
                );
                // TCP is already open; neither a valid but incomplete response
                // nor an HTTP error with otherwise valid JSON means ready.
                let body = if attempt == 0 {
                    "{}".to_string()
                } else {
                    format!(
                        r#"{{"Browser":"Chrome/test","webSocketDebuggerUrl":"ws://127.0.0.1:{port}/devtools/browser/test"}}"#
                    )
                };
                let status = if attempt == 1 {
                    "503 Not Ready"
                } else {
                    "200 OK"
                };
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        let result = wait_for_chromium_cdp(&mut child, port, &log, Duration::from_secs(3));
        let running = child.try_wait().unwrap().is_none();
        stop_process_group(child.id());
        child.wait().unwrap();
        result.unwrap();
        assert!(running, "successful readiness must not terminate Chromium");
        endpoint.join().unwrap();
    }

    #[test]
    #[cfg(unix)]
    fn chromium_startup_reports_original_exit_and_stderr_without_waiting_for_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("browser.log");
        let mut child = startup_test_child(&log, "echo startup-crash-sentinel >&2; exit 23");
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let started = std::time::Instant::now();
        let err = wait_for_chromium_cdp(&mut child, port, &log, Duration::from_secs(10))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("exited before CDP readiness: exit status: 23"),
            "{err}"
        );
        assert!(err.contains("startup-crash-sentinel"), "{err}");
        assert!(err.contains("before_cleanup=Ok(Some("), "{err}");
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    #[cfg(unix)]
    fn chromium_startup_bounds_stalled_http_and_distinguishes_our_cleanup_signal() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("browser.log");
        let mut child = startup_test_child(&log, "echo startup-stall-sentinel >&2; exec sleep 30");
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let endpoint = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            std::thread::sleep(Duration::from_millis(500));
            drop(stream);
        });
        let started = std::time::Instant::now();
        let err = wait_for_chromium_cdp(&mut child, port, &log, Duration::from_millis(100))
            .unwrap_err()
            .to_string();
        assert!(err.contains("startup timed out after 100ms"), "{err}");
        assert!(err.contains("before_cleanup=Ok(None)"), "{err}");
        assert!(err.contains("startup-stall-sentinel"), "{err}");
        assert!(
            child.try_wait().unwrap().is_some(),
            "timed-out child must be reaped"
        );
        assert!(started.elapsed() < Duration::from_secs(5));
        endpoint.join().unwrap();
    }

    #[test]
    fn chromium_startup_log_tail_is_bounded_and_handles_missing_or_non_utf8_logs() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("browser.log");
        assert_eq!(browser_startup_log_tail(&log), "<browser log unavailable>");
        let mut bytes = vec![b'x'; 20_000];
        bytes.extend_from_slice(b"tail-sentinel\xff");
        std::fs::write(&log, bytes).unwrap();
        let tail = browser_startup_log_tail(&log);
        assert!(tail.ends_with("tail-sentinel\u{fffd}"));
        assert!(tail.len() <= 8194);
    }

    fn spawn_html_server(bind_ip: &str, html: String) -> u16 {
        let listener = TcpListener::bind(format!("{bind_ip}:0")).expect("bind local test server");
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let html = html.clone();
                std::thread::spawn(move || {
                    let mut stream = stream;
                    let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
                    let mut req_buf = Vec::with_capacity(1024);
                    let mut chunk = [0u8; 1024];
                    while let Ok(n) = stream.read(&mut chunk) {
                        if n == 0 {
                            break;
                        }
                        req_buf.extend_from_slice(&chunk[..n]);
                        if req_buf.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        html.len(),
                        html
                    );
                    let _ = stream.write_all(resp.as_bytes());
                    let _ = stream.flush();
                    let _ = stream.shutdown(std::net::Shutdown::Write);
                });
            }
        });
        port
    }

    #[test]
    #[ignore = "browser open intermittently fails with EINTR in CI; deleted with browser.rs, tracking: https://github.com/SylphxAI/anyagents/issues/200"]
    fn cross_origin_iframe_snapshot_fill_and_click_with_secret_ref() {
        let _browser = browser_fixture_guard();
        if !has_chromium() {
            return;
        }
        let iframe_html = r#"<!doctype html>
<html>
<head><title>Auth Frame</title></head>
<body>
  <h2>Sign in to Apple Account</h2>
  <input id="account_name_text_field" type="text" placeholder="Email or Phone Number" />
  <input id="password_text_field" type="password" placeholder="Password" />
  <button id="sign_in" onclick="document.getElementById('result').innerText = 'OK:' + document.getElementById('account_name_text_field').value + ':' + document.getElementById('password_text_field').value">Continue</button>
  <div id="result">pending</div>
</body>
</html>"#
            .to_string();
        let iframe_port = spawn_html_server("127.0.0.2", iframe_html);
        // Use `127.0.0.2:<iframe_port>` for the iframe while using `127.0.0.1:<parent_port>` for the parent page so Chromium treats it as cross-origin/cross-site without IPv6 `::1` races.
        let parent_html = format!(
            r#"<!doctype html>
<html>
<head><title>App Store Connect</title></head>
<body>
  <h1>Parent Shell</h1>
  <iframe id="aid-auth-widget-iFrame" src="http://127.0.0.2:{iframe_port}/auth" width="600" height="400"></iframe>
</body>
</html>"#
        );
        let parent_port = spawn_html_server("127.0.0.1", parent_html);
        let dir = tempfile::tempdir().unwrap();
        let store = FileCredentialStore::new(dir.path());
        store.write("apple/password", b"s3cr3t-iframe-pw").unwrap();

        let open_res = match execute(
            dir.path(),
            "test-iframe",
            BrowserRequest {
                action: "open".into(),
                profile: Some("test-iframe".into()),
                url: Some(format!("http://127.0.0.1:{parent_port}/")),
                headless: Some(true),
                wait_ms: Some(1200),
                ..Default::default()
            },
        ) {
            Ok(res) => res,
            Err(Error::Message(msg))
                if std::env::var_os("CI").is_some()
                    && (msg.contains("did not open CDP port")
                        || msg.contains("failed to launch chromium")) =>
            {
                return;
            }
            Err(err) => panic!("browser open: {err:?}"),
        };

        let elems = open_res["interactive_elements"]
            .as_array()
            .expect("interactive_elements array");
        assert!(
            elems.len() >= 3,
            "expected cross-origin iframe elements in snapshot, got: {open_res}"
        );

        let email_ref = elems
            .iter()
            .find(|e| e["selector"].as_str() == Some("#account_name_text_field"))
            .and_then(|e| e["ref"].as_str())
            .expect("email input @e ref")
            .to_string();
        let pw_ref = elems
            .iter()
            .find(|e| e["selector"].as_str() == Some("#password_text_field"))
            .and_then(|e| e["ref"].as_str())
            .expect("password input @e ref")
            .to_string();
        let btn_ref = elems
            .iter()
            .find(|e| e["selector"].as_str() == Some("#sign_in"))
            .and_then(|e| e["ref"].as_str())
            .expect("sign_in button @e ref")
            .to_string();

        execute(
            dir.path(),
            "test-iframe",
            BrowserRequest {
                action: "fill".into(),
                profile: Some("test-iframe".into()),
                selector: Some(email_ref),
                text: Some("hi@kylet.se".into()),
                ..Default::default()
            },
        )
        .expect("fill email in cross-origin iframe");

        let pw_fill = execute(
            dir.path(),
            "test-iframe",
            BrowserRequest {
                action: "fill".into(),
                profile: Some("test-iframe".into()),
                selector: Some(pw_ref),
                secret_ref: Some("secret://apple/password".into()),
                ..Default::default()
            },
        )
        .expect("fill secret_ref in cross-origin iframe");
        assert_eq!(pw_fill["secret_injected"], true);
        assert!(
            !pw_fill.to_string().contains("s3cr3t-iframe-pw"),
            "secret plaintext must never appear in fill response"
        );

        execute(
            dir.path(),
            "test-iframe",
            BrowserRequest {
                action: "click".into(),
                profile: Some("test-iframe".into()),
                selector: Some(btn_ref),
                wait_ms: Some(300),
                ..Default::default()
            },
        )
        .expect("click button in cross-origin iframe");

        let snap = execute(
            dir.path(),
            "test-iframe",
            BrowserRequest {
                action: "snapshot".into(),
                profile: Some("test-iframe".into()),
                ..Default::default()
            },
        )
        .expect("snapshot after click");
        let text = snap["text"].as_str().unwrap_or("");
        assert!(
            text.contains("OK:hi@kylet.se:s3cr3t-iframe-pw"),
            "expected iframe DOM to receive email + secret password, got text: {text}"
        );

        let _ = execute(
            dir.path(),
            "test-iframe",
            BrowserRequest {
                action: "close".into(),
                profile: Some("test-iframe".into()),
                ..Default::default()
            },
        );
    }

    #[test]
    #[ignore = "launches real Chromium; CDP I/O fails intermittently with EINTR in CI; deleted with browser.rs, tracking: https://github.com/SylphxAI/anyagents/issues/200"]
    fn held_key_moves_raf_fixture_stops_on_release_and_isolates_profiles() {
        let _browser = browser_fixture_guard();
        if !has_chromium() {
            return;
        }
        let fixture_html = r#"<!doctype html>
<html>
<head><title>Held Key Fixture</title></head>
<body>
  <div id="status">ready</div>
  <script>
    window.__state = {
      x: 0,
      y: 0,
      frames: 0,
      heldFrames: 0,
      downCount: 0,
      upCount: 0,
      trustedDown: false,
      trustedUp: false,
      lastKey: "",
      lastCode: "",
      pressed: {}
    };
    window.addEventListener("keydown", (e) => {
      window.__state.downCount += 1;
      window.__state.trustedDown = e.isTrusted === true;
      window.__state.lastKey = e.key;
      window.__state.lastCode = e.code;
      window.__state.pressed[e.key.toLowerCase()] = true;
      window.__state.pressed[e.code] = true;
    });
    window.addEventListener("keyup", (e) => {
      window.__state.upCount += 1;
      window.__state.trustedUp = e.isTrusted === true;
      delete window.__state.pressed[e.key.toLowerCase()];
      delete window.__state.pressed[e.code];
    });
    function step() {
      window.__state.frames += 1;
      let moving = false;
      if (window.__state.pressed["d"] || window.__state.pressed["KeyD"] || window.__state.pressed["ArrowRight"]) {
        window.__state.x += 4;
        moving = true;
      }
      if (window.__state.pressed["w"] || window.__state.pressed["KeyW"] || window.__state.pressed["ArrowUp"]) {
        window.__state.y -= 4;
        moving = true;
      }
      if (moving) {
        window.__state.heldFrames += 1;
      }
      document.getElementById("status").innerText =
        `x=${window.__state.x} y=${window.__state.y} heldFrames=${window.__state.heldFrames} pressed=${Object.keys(window.__state.pressed).join(",")}`;
      requestAnimationFrame(step);
    }
    requestAnimationFrame(step);
  </script>
</body>
</html>"#
            .to_string();
        let port = spawn_html_server("127.0.0.1", fixture_html);
        let dir = tempfile::tempdir().unwrap();

        if let Err(err) = execute(
            dir.path(),
            "profile-a",
            BrowserRequest {
                action: "open".into(),
                profile: Some("profile-a".into()),
                url: Some(format!("http://127.0.0.1:{port}/")),
                headless: Some(true),
                wait_ms: Some(300),
                ..Default::default()
            },
        ) {
            if let Error::Message(ref msg) = err
                && std::env::var_os("CI").is_some()
                && (msg.contains("did not open CDP port")
                    || msg.contains("failed to launch chromium"))
            {
                return;
            }
            panic!("open profile-a: {err:?}");
        }

        execute(
            dir.path(),
            "profile-b",
            BrowserRequest {
                action: "open".into(),
                profile: Some("profile-b".into()),
                url: Some(format!("http://127.0.0.1:{port}/")),
                headless: Some(true),
                wait_ms: Some(300),
                ..Default::default()
            },
        )
        .expect("open profile-b");

        // Hold `d` for 350ms on profile-a.
        let hold_res = execute(
            dir.path(),
            "profile-a",
            BrowserRequest {
                action: "press".into(),
                profile: Some("profile-a".into()),
                key: Some("d".into()),
                hold_ms: Some(350),
                ..Default::default()
            },
        )
        .expect("hold d on profile-a");
        assert_eq!(hold_res["pressed"], "d");
        assert_eq!(hold_res["codes"][0], "KeyD");
        assert_eq!(hold_res["released"], true);
        assert_eq!(hold_res["trusted_input"], true);

        let after_hold = execute(
            dir.path(),
            "profile-a",
            BrowserRequest {
                action: "eval".into(),
                profile: Some("profile-a".into()),
                script: Some("window.__state".into()),
                ..Default::default()
            },
        )
        .expect("eval state after hold")["result"]
            .clone();

        let x_after = after_hold["x"].as_i64().unwrap_or(0);
        let held_frames = after_hold["heldFrames"].as_i64().unwrap_or(0);
        assert!(
            x_after > 0 && held_frames >= 2,
            "expected multi-frame movement while held, got: {after_hold}"
        );
        assert_eq!(after_hold["trustedDown"], true);
        assert_eq!(after_hold["trustedUp"], true);
        assert_eq!(after_hold["lastKey"], "d");
        assert_eq!(after_hold["lastCode"], "KeyD");
        assert_eq!(
            after_hold["pressed"]
                .as_object()
                .map(|m| m.len())
                .unwrap_or(99),
            0,
            "expected no stuck keys after hold completes, got: {after_hold}"
        );

        // Wait 200ms after release and verify movement stopped completely.
        std::thread::sleep(Duration::from_millis(200));
        let after_stop = execute(
            dir.path(),
            "profile-a",
            BrowserRequest {
                action: "eval".into(),
                profile: Some("profile-a".into()),
                script: Some("window.__state".into()),
                ..Default::default()
            },
        )
        .expect("eval state after stop")["result"]
            .clone();
        assert_eq!(
            after_stop["x"].as_i64().unwrap_or(-1),
            x_after,
            "expected x to remain stationary after keyUp, got {after_stop} vs {x_after}"
        );

        // Verify profile-b was completely isolated and never moved.
        let state_b = execute(
            dir.path(),
            "profile-b",
            BrowserRequest {
                action: "eval".into(),
                profile: Some("profile-b".into()),
                script: Some("window.__state".into()),
                ..Default::default()
            },
        )
        .expect("eval profile-b")["result"]
            .clone();
        assert_eq!(state_b["x"].as_i64().unwrap_or(-1), 0);
        assert_eq!(state_b["downCount"].as_i64().unwrap_or(-1), 0);

        // Verify explicit key_down + close releases held keys and clears held_keys.json.
        execute(
            dir.path(),
            "profile-a",
            BrowserRequest {
                action: "key_down".into(),
                profile: Some("profile-a".into()),
                key: Some("w".into()),
                ..Default::default()
            },
        )
        .expect("key_down w");
        assert_eq!(load_held_keys(dir.path(), "profile-a").unwrap().len(), 1);

        let _ = execute(
            dir.path(),
            "profile-a",
            BrowserRequest {
                action: "close".into(),
                profile: Some("profile-a".into()),
                ..Default::default()
            },
        );
        assert!(
            load_held_keys(dir.path(), "profile-a").unwrap().is_empty(),
            "closing profile must clear held_keys state"
        );
        let _ = execute(
            dir.path(),
            "profile-b",
            BrowserRequest {
                action: "close".into(),
                profile: Some("profile-b".into()),
                ..Default::default()
            },
        );
    }

    fn raw_http_call(port: u16, method: &str, path: &str, body: Option<&str>) -> (u16, Vec<u8>) {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect takeover server");
        let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
        let body_bytes = body.unwrap_or("").as_bytes();
        let req = if body.is_some() {
            format!(
                "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body_bytes.len(),
                body.unwrap_or("")
            )
        } else {
            format!(
                "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
            )
        };
        stream.write_all(req.as_bytes()).expect("write request");
        let mut resp = Vec::new();
        let _ = stream.read_to_end(&mut resp);
        let sep = resp
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .expect("http header separator");
        let header = String::from_utf8_lossy(&resp[..sep]);
        let code = header
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(500);
        (code, resp[sep + 4..].to_vec())
    }

    #[test]
    #[ignore = "launches real Chromium; CDP I/O fails intermittently with EINTR in CI; deleted with browser.rs, tracking: https://github.com/SylphxAI/anyagents/issues/200"]
    fn human_takeover_captcha_detection_fencing_and_remote_solve() {
        let _browser = browser_fixture_guard();
        if !has_chromium() {
            return;
        }
        let html = r#"<!doctype html>
<html>
<head><title>Just a moment...</title></head>
<body style="margin:0;padding:40px;background:#111;color:#fff;">
  <div id="cf-chl-widget" class="cf-turnstile" style="width:300px;height:80px;background:#2563eb;display:flex;align-items:center;justify-content:center;cursor:pointer;">
    Verify you are human (Click to solve)
  </div>
  <input id="post_captcha_input" type="text" value="" style="margin-top:20px;" />
  <button id="continue_btn" type="button">Continue</button>
  <script>
    window.__captchaSolved = false;
    window.__trustedSolveClick = false;
    const widget = document.getElementById('cf-chl-widget');
    widget.addEventListener('click', (e) => {
      window.__captchaSolved = true;
      window.__trustedSolveClick = Boolean(e.isTrusted);
      document.title = 'Dashboard Unlocked';
      widget.remove();
      document.getElementById('post_captcha_input').focus();
    });
  </script>
</body>
</html>"#
        .to_string();

        let port = spawn_html_server("127.0.0.1", html);
        let dir = tempfile::tempdir().expect("tempdir");
        let store = anyagents_core::Store::open(dir.path().join("state.db")).unwrap();
        store
            .create_agent(anyagents_core::NewAgent::new("captcha-prof", "codex"))
            .unwrap();

        let open_res = match execute(
            dir.path(),
            "captcha-prof",
            BrowserRequest {
                action: "open".into(),
                profile: Some("captcha-prof".into()),
                url: Some(format!("http://127.0.0.1:{port}/")),
                headless: Some(true),
                wait_ms: Some(300),
                ..Default::default()
            },
        ) {
            Ok(v) => v,
            Err(err) => {
                let msg = format!("{err:?}");
                if msg.contains("did not open CDP port")
                    || msg.contains("Operation not permitted")
                    || msg.contains("Permission denied")
                {
                    return;
                }
                panic!("open captcha page: {err:?}");
            }
        };

        assert_eq!(open_res["captcha_detected"], true);
        assert_eq!(open_res["captcha_kind"], "cloudflare_turnstile");
        assert!(
            open_res["takeover_hint"]
                .as_str()
                .unwrap_or_default()
                .contains("takeover")
        );

        let takeover_res = execute(
            dir.path(),
            "captcha-prof",
            BrowserRequest {
                action: "takeover".into(),
                profile: Some("captcha-prof".into()),
                reason: Some("Solve Cloudflare Turnstile".into()),
                tunnel: Some(false),
                ..Default::default()
            },
        )
        .expect("start takeover session");

        assert_eq!(takeover_res["status"], "takeover_active");
        assert_eq!(takeover_res["holder"], "human");
        assert_eq!(takeover_res["captcha_detected"], true);
        let token = takeover_res["token"]
            .as_str()
            .expect("takeover token")
            .to_string();
        let takeover_port = takeover_res["port"].as_u64().expect("takeover port") as u16;
        let shot_path = takeover_res["screenshot_path"]
            .as_str()
            .expect("screenshot_path");
        assert!(Path::new(shot_path).is_file());

        // 1. Mutating agent actions must be fenced while human holds control.
        let fenced_err = execute(
            dir.path(),
            "captcha-prof",
            BrowserRequest {
                action: "click".into(),
                profile: Some("captcha-prof".into()),
                selector: Some("#continue_btn".into()),
                ..Default::default()
            },
        )
        .expect_err("expected CONTROL_HELD_BY_HUMAN fence");
        assert!(
            fenced_err.to_string().contains("CONTROL_HELD_BY_HUMAN"),
            "unexpected error: {fenced_err}"
        );

        // 2. Wrong takeover token must be rejected with 403 Forbidden.
        let (bad_code, _) = raw_http_call(takeover_port, "GET", "/takeover/tok_wrong/state", None);
        assert_eq!(bad_code, 403);

        // 3. Valid token serves the HTML UI, JPEG frame, and live state.
        let (html_code, html_bytes) =
            raw_http_call(takeover_port, "GET", &format!("/takeover/{token}"), None);
        assert_eq!(html_code, 200);
        assert!(
            String::from_utf8_lossy(&html_bytes).contains("id=\"done-btn\""),
            "expected takeover UI HTML"
        );

        let (jpg_code, jpg_bytes) = raw_http_call(
            takeover_port,
            "GET",
            &format!("/takeover/{token}/frame.jpg"),
            None,
        );
        assert_eq!(jpg_code, 200);
        assert!(
            jpg_bytes.len() > 100 && jpg_bytes.starts_with(&[0xff, 0xd8, 0xff]),
            "expected valid JPEG screenshot frame"
        );

        let (state_code, state_bytes) = raw_http_call(
            takeover_port,
            "GET",
            &format!("/takeover/{token}/state"),
            None,
        );
        assert_eq!(state_code, 200);
        let state_json: serde_json::Value =
            serde_json::from_slice(&state_bytes).expect("parse state json");
        assert_eq!(state_json["captcha_detected"], true);
        assert_eq!(state_json["holder"], "human");

        // 4. User clicks the Turnstile widget (centered around x=150, y=80) and types into the input via the remote takeover API.
        let (click_code, _) = raw_http_call(
            takeover_port,
            "POST",
            &format!("/takeover/{token}/input"),
            Some(r#"{"type":"click","x":150,"y":80}"#),
        );
        assert_eq!(click_code, 200);
        std::thread::sleep(Duration::from_millis(150));

        let (text_code, _) = raw_http_call(
            takeover_port,
            "POST",
            &format!("/takeover/{token}/input"),
            Some(r#"{"type":"text","text":"SOLVED-BY-USER","submit":false}"#),
        );
        assert_eq!(text_code, 200);

        // 5. User clicks "Done — Hand Back to Agent".
        let (done_code, done_bytes) = raw_http_call(
            takeover_port,
            "POST",
            &format!("/takeover/{token}/complete"),
            Some(r#"{"note":"CAPTCHA solved by user"}"#),
        );
        assert_eq!(done_code, 200);
        let done_json: serde_json::Value =
            serde_json::from_slice(&done_bytes).expect("parse complete json");
        assert_eq!(done_json["completed"], true);
        assert_eq!(done_json["holder"], "agent");

        // 6. Agent now re-inspects and can perform mutating actions again!
        let snap_after = execute(
            dir.path(),
            "captcha-prof",
            BrowserRequest {
                action: "snapshot".into(),
                profile: Some("captcha-prof".into()),
                ..Default::default()
            },
        )
        .expect("snapshot after takeover");
        assert_eq!(snap_after["captcha_detected"], false);
        assert_eq!(snap_after["title"], "Dashboard Unlocked");

        let solved_eval = execute(
            dir.path(),
            "captcha-prof",
            BrowserRequest {
                action: "eval".into(),
                profile: Some("captcha-prof".into()),
                script: Some(
                    "({ solved: window.__captchaSolved, trusted: window.__trustedSolveClick, val: document.getElementById('post_captcha_input').value })"
                        .into(),
                ),
                ..Default::default()
            },
        )
        .expect("eval solved state")["result"]
            .clone();
        assert_eq!(solved_eval["solved"], true);
        assert_eq!(solved_eval["trusted"], true);
        assert_eq!(solved_eval["val"], "SOLVED-BY-USER");

        let _ = execute(
            dir.path(),
            "captcha-prof",
            BrowserRequest {
                action: "close".into(),
                profile: Some("captcha-prof".into()),
                ..Default::default()
            },
        );
    }
    #[test]
    #[ignore = "Chromium CDP startup exceeds 12s under CI load; tracking: https://github.com/SylphxAI/anyagents/pull/115"]
    fn takeover_clipboard_ui_acceptance() {
        if !has_chromium() {
            return;
        }
        run_takeover_ui_acceptance(true);
    }

    #[test]
    #[ignore = "requires Chromium and Node >=22; run explicitly for takeover UI changes"]
    fn mobile_takeover_ui_acceptance() {
        run_takeover_ui_acceptance(false);
    }

    #[test]
    #[ignore = "requires real Chromium; takeover terminal ownership"]
    fn takeover_http_completion_releases_held_press() {
        assert_takeover_termination_releases("http");
    }

    #[test]
    #[ignore = "requires real Chromium; takeover terminal ownership"]
    fn takeover_direct_completion_releases_held_press() {
        assert_takeover_termination_releases("direct");
    }

    #[test]
    #[ignore = "requires real Chromium; takeover terminal ownership"]
    fn takeover_agent_reclaim_releases_held_press() {
        assert_takeover_termination_releases("reclaim");
    }

    #[test]
    #[ignore = "requires real Chromium; takeover terminal ownership"]
    fn takeover_captcha_completion_releases_held_press() {
        assert_takeover_termination_releases("captcha");
    }

    #[test]
    #[ignore = "requires real Chromium; takeover terminal ownership"]
    fn takeover_timeout_releases_held_press() {
        assert_takeover_termination_releases("timeout");
    }

    fn assert_takeover_termination_releases(path: &str) {
        let _browser = browser_fixture_guard();
        assert!(has_chromium(), "Chromium is required");
        let dir = tempfile::tempdir().unwrap();
        let profile = "terminal-owner";
        let html = "<button style='position:absolute;left:20px;top:20px;width:150px;height:100px'>Hold</button><script>window.downs=0;window.ups=0;window.clicks=0;document.addEventListener('mousedown',()=>downs++);document.addEventListener('mouseup',()=>ups++);document.addEventListener('click',()=>clicks++);</script>".to_string();
        let port = spawn_html_server("127.0.0.1", html);
        execute(
            dir.path(),
            profile,
            BrowserRequest {
                action: "open".into(),
                headless: Some(true),
                url: Some(format!("http://127.0.0.1:{port}/")),
                wait_ms: Some(200),
                ..Default::default()
            },
        )
        .unwrap();
        let result = (|| -> Result<(), Error> {
            let mut rt = load_runtime(dir.path(), profile)?.unwrap();
            let ws = select_page_target(dir.path(), &mut rt, None)?
                .ws_url
                .unwrap();
            let mut cdp = CdpSession::connect(rt.cdp_port, &ws)?;
            let cap = CaptchaDetection {
                detected: path == "captcha",
                ..Default::default()
            };
            let st = start_or_reuse_takeover_session(
                dir.path(),
                profile,
                None,
                None,
                Some(60_000),
                Some(false),
                &cap,
            )?;
            let (code, body) = raw_http_call(
                st.port,
                "POST",
                &format!("/takeover/{}/input", st.token),
                Some(
                    r#"{"type":"mouse","event_type":"mousePressed","x":60,"y":60,"button":"left","buttons":1}"#,
                ),
            );
            assert_eq!(
                code,
                200,
                "press admitted: {}",
                String::from_utf8_lossy(&body)
            );
            assert_eq!(eval_js(&mut cdp, "window.downs")?, 1);
            match path {
                "http" => {
                    let (code, body) = raw_http_call(
                        st.port,
                        "POST",
                        &format!("/takeover/{}/complete", st.token),
                        Some("{}"),
                    );
                    assert_eq!(code, 200);
                    assert_eq!(
                        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["completed"],
                        true
                    );
                }
                "direct" => assert_eq!(
                    complete_takeover_session(dir.path(), profile, None)?["completed"],
                    true
                ),
                "reclaim" => {
                    let done = execute(
                        dir.path(),
                        profile,
                        BrowserRequest {
                            action: "release_takeover".into(),
                            ..Default::default()
                        },
                    )?;
                    assert_eq!(done["released_takeover"], true);
                }
                "captcha" => {
                    let (done, cap) = wait_for_takeover_completion(dir.path(), profile, 2_000)?;
                    assert!(!cap.detected);
                    assert!(done.unwrap().completed);
                }
                "timeout" => {
                    reap_expired_takeover(dir.path(), profile, st.expires_at_ms)?;
                    assert!(load_takeover_state(dir.path(), profile)?.unwrap().completed);
                }
                _ => unreachable!(),
            }
            let observed = eval_js(&mut cdp, "({downs,ups,clicks})")?;
            println!("termination={path} observed={observed}");
            assert_eq!(
                observed,
                serde_json::json!({"downs":1,"ups":1,"clicks":0}),
                "{path} must terminate the press before handing back ownership"
            );
            // All terminal entrypoints converge without a second native release.
            complete_takeover_session(dir.path(), profile, None)?;
            cleanup_takeover_session(dir.path(), profile)?;
            assert_eq!(eval_js(&mut cdp, "window.ups")?, 1);
            Ok(())
        })();
        let _ = execute(
            dir.path(),
            profile,
            BrowserRequest {
                action: "close".into(),
                ..Default::default()
            },
        );
        result.unwrap();
    }

    #[test]
    #[ignore = "requires real Chromium; takeover native cancellation"]
    fn takeover_cancel_survives_earlier_stopped_pointerdown() {
        let _browser = browser_fixture_guard();
        assert!(has_chromium(), "Chromium is required");
        let dir = tempfile::tempdir().unwrap();
        let profile = "cancel-earlier-listener";
        let html = r#"<button id="target" style="position:fixed;left:50px;top:50px;width:200px;height:100px">Action</button><script>
            window.clicks=0;window.ups=0;window.blockDown=false;
            window.addEventListener('pointerdown',e=>{if(window.blockDown)e.stopImmediatePropagation()},true);
            window.addEventListener('pointerup',()=>window.ups++);
            target.onclick=()=>window.clicks++;</script>"#.to_string();
        let port = spawn_html_server("127.0.0.1", html);
        execute(
            dir.path(),
            profile,
            BrowserRequest {
                action: "open".into(),
                headless: Some(true),
                url: Some(format!("http://127.0.0.1:{port}/")),
                wait_ms: Some(200),
                ..Default::default()
            },
        )
        .unwrap();
        let result = (|| -> Result<(), Error> {
            let mut rt = load_runtime(dir.path(), profile)?.unwrap();
            let ws = select_page_target(dir.path(), &mut rt, None)?
                .ws_url
                .unwrap();
            let mut cdp = CdpSession::connect(rt.cdp_port, &ws)?;
            let point = serde_json::json!({"x":100,"y":100,"button":"left"});
            let down = serde_json::json!({"type":"mouse","event_type":"mousePressed","x":100,"y":100,"buttons":1});
            dispatch_takeover_cdp_input(&mut cdp, &down)?;
            cancel_takeover_mouse(&mut cdp, &point)?;
            assert_eq!(
                eval_js(&mut cdp, "({clicks,ups})")?,
                serde_json::json!({"clicks":0,"ups":1})
            );
            dispatch_trusted_click(&mut cdp, 100.0, 100.0)?;
            assert_eq!(eval_js(&mut cdp, "window.clicks")?, 1);
            eval_js(&mut cdp, "window.blockDown=true;window.ups=0")?;
            dispatch_takeover_cdp_input(&mut cdp, &down)?;
            cancel_takeover_mouse(&mut cdp, &point)?;
            let observed = eval_js(&mut cdp, "({clicks,ups})")?;
            println!("earlier stopImmediatePropagation observed={observed}");
            assert_eq!(
                observed,
                serde_json::json!({"clicks":1,"ups":1}),
                "cancel must emit exactly one release and no additional click"
            );
            Ok(())
        })();
        let _ = execute(
            dir.path(),
            profile,
            BrowserRequest {
                action: "close".into(),
                ..Default::default()
            },
        );
        result.unwrap();
    }

    fn run_takeover_ui_acceptance(clipboard_only: bool) {
        let _browser = browser_fixture_guard();
        assert!(has_chromium(), "Chromium is required");
        let html = r#"<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1">
        <title>Remote browser input test</title>
        <style>body{margin:0;background:#edf3fb;font:20px system-ui;color:#172a43}header{padding:28px;background:#fff;border-bottom:1px solid #ccd9e8}input{display:block;font:20px system-ui;width:280px;height:48px;margin-top:12px}#target{position:fixed;left:50%;top:50%;transform:translate(-50%,-50%);background:#086bce;color:white;border:0;border-radius:16px;padding:24px;font:20px system-ui}p{margin:28px;max-width:460px;line-height:1.6}</style>
        <header>Remote browser<input id="entry" placeholder="Type here"><input id="entry2" placeholder="Next field"><input id="password" type="password" placeholder="Password"></header>
        <p>Pinch to inspect the page. Tap the blue target to verify precise input. The remote browser keeps its original identity.</p>
        <button id="target">Click target</button>
        <script>window.clicks=[];window.keys=[];document.addEventListener('click',e=>window.clicks.push({x:e.clientX,y:e.clientY,target:e.target.id,trusted:e.isTrusted}));document.addEventListener('keydown',e=>window.keys.push(e.key));</script>"#.to_string();
        let port = spawn_html_server("127.0.0.1", html);
        let dir = tempfile::tempdir().unwrap();
        execute(
            dir.path(),
            "mobile-ui",
            BrowserRequest {
                action: "open".into(),
                headless: Some(true),
                url: Some(format!("http://127.0.0.1:{port}/")),
                wait_ms: Some(200),
                ..Default::default()
            },
        )
        .unwrap();
        let mut rt = load_runtime(dir.path(), "mobile-ui").unwrap().unwrap();
        let ws = select_page_target(dir.path(), &mut rt, None)
            .unwrap()
            .ws_url
            .unwrap();
        let st = start_or_reuse_takeover_session(
            dir.path(),
            "mobile-ui",
            None,
            Some("Verify the remote browser controls."),
            Some(300_000),
            Some(false),
            &CaptchaDetection::default(),
        )
        .unwrap();
        let output = std::env::var("TAKEOVER_EVIDENCE_DIR")
            .unwrap_or_else(|_| dir.path().join("captures").display().to_string());
        let mut command = Command::new("node");
        if clipboard_only {
            command.env("TAKEOVER_CLIPBOARD_ONLY", "1");
        }
        let result = command
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../tests/browser-takeover.mjs"
            ))
            .args([&st.local_url, &ws, &output])
            .env("ANYAGENTS_CHROMIUM", chromium_binary().unwrap())
            .status();
        let _ = execute(
            dir.path(),
            "mobile-ui",
            BrowserRequest {
                action: "close".into(),
                ..Default::default()
            },
        );
        assert!(
            result.unwrap().success(),
            "mobile takeover acceptance failed; captures: {output}"
        );
    }
    #[test]
    fn idle_reaper_visits_only_profiles_with_a_browser_or_takeover() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path();
        let crashed_state = |profile: &str| BrowserRuntimeState {
            profile: profile.into(),
            // Above any pid: the browser is gone.
            pid: u32::MAX,
            cdp_port: 9,
            headless: true,
            display_mode: "headless".into(),
            proxy: None,
            active_target_id: None,
            started_at_ms: 0,
            last_used_at_ms: 0,
        };
        // A profile kept only for its logins, and one whose browser died while
        // no daemon was watching.
        std::fs::create_dir_all(profile_dir(data, "dormant").join("user-data")).unwrap();
        write_profile_json(
            &profile_dir(data, "crashed").join("runtime.json"),
            &crashed_state("crashed"),
        )
        .unwrap();

        reap_idle_browsers(data);
        assert!(!profile_dir(data, "crashed").join("runtime.json").exists());
        assert!(
            !profile_dir(data, "dormant").join("takeover.lock").exists(),
            "a profile with no browser was visited"
        );
        assert!(candidate_profiles(data).is_empty());

        // Once seeded, only what this process starts is visited.
        write_profile_json(
            &profile_dir(data, "dormant").join("runtime.json"),
            &crashed_state("dormant"),
        )
        .unwrap();
        reap_idle_browsers(data);
        assert!(profile_dir(data, "dormant").join("runtime.json").exists());
        save_runtime(data, &crashed_state("dormant")).unwrap();
        assert_eq!(candidate_profiles(data), ["dormant"]);
        reap_idle_browsers(data);
        assert!(!profile_dir(data, "dormant").join("runtime.json").exists());
        assert!(candidate_profiles(data).is_empty());
    }

    #[test]
    fn accepted_takeover_request_is_fenced_after_completion_or_replacement() {
        for revoked in ["completed", "replaced", "expired", "agent", "missing"] {
            let dir = tempfile::tempdir().unwrap();
            let mut state = start_or_reuse_takeover_session(
                dir.path(),
                "revocation",
                None,
                None,
                Some(30_000),
                Some(false),
                &CaptchaDetection::default(),
            )
            .unwrap();
            assert!(
                !load_takeover_state(dir.path(), "revocation")
                    .unwrap()
                    .unwrap()
                    .completed
            );
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            client
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let (accepted, ready) = std::sync::mpsc::channel();
            let data = dir.path().to_path_buf();
            let token = state.token.clone();
            let worker = std::thread::spawn(move || {
                let (stream, _) = listener.accept().unwrap();
                accepted.send(()).unwrap();
                handle_takeover_http_client(
                    stream,
                    &data,
                    "revocation",
                    &token,
                    &TakeoverServerShared::default(),
                )
            });
            let body = r#"{"type":"clipboard","action":"copy"}"#;
            write!(
                client,
                "POST /takeover/{}/input HTTP/1.1\r\nContent-Length: {}\r\n",
                state.token,
                body.len()
            )
            .unwrap();
            ready.recv_timeout(Duration::from_secs(2)).unwrap();
            if revoked == "completed" {
                let mut complete = TcpStream::connect(("127.0.0.1", state.port)).unwrap();
                complete
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                write!(
                    complete,
                    "POST /takeover/{}/complete HTTP/1.1\r\nContent-Length: 2\r\n\r\n{{}}",
                    state.token
                )
                .unwrap();
                let mut response = String::new();
                complete.read_to_string(&mut response).unwrap();
                assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
                assert!(
                    load_takeover_state(dir.path(), "revocation")
                        .unwrap()
                        .unwrap()
                        .completed
                );
            } else if revoked == "missing" {
                cleanup_takeover_session(dir.path(), "revocation").unwrap();
            } else {
                match revoked {
                    "replaced" => state.token = "replacement-token".into(),
                    "expired" => state.expires_at_ms = now_ms() - 1,
                    "agent" => state.holder = "agent".into(),
                    _ => unreachable!(),
                }
                save_takeover_state(dir.path(), &state).unwrap();
            }
            write!(client, "\r\n{body}").unwrap();
            let mut response = String::new();
            client.read_to_string(&mut response).unwrap();
            assert!(
                response.starts_with("HTTP/1.1 403 Forbidden"),
                "{revoked}: {response}"
            );
            assert!(
                response.contains("takeover session is no longer active"),
                "{response}"
            );
            assert!(!worker.join().unwrap());
            cleanup_takeover_session(dir.path(), "revocation").unwrap();
        }
    }

    #[test]
    fn takeover_input_is_not_blocked_by_a_slow_frame_client() {
        let dir = tempfile::tempdir().unwrap();
        let st = start_or_reuse_takeover_session(
            dir.path(),
            "parallel-input",
            None,
            None,
            Some(30_000),
            Some(false),
            &CaptchaDetection::default(),
        )
        .unwrap();
        let mut slow = TcpStream::connect(("127.0.0.1", st.port)).unwrap();
        // This request cannot finish until the test drops it. The old single
        // synchronous listener waits its full 3s read deadline before /input.
        write!(
            slow,
            "GET /takeover/{}/frame.jpg HTTP/1.1\r\nHost: localhost\r\n",
            st.token
        )
        .unwrap();
        let begin = std::time::Instant::now();
        let (code, body) = raw_http_call(
            st.port,
            "POST",
            &format!("/takeover/{}/input", st.token),
            Some(r#"{"type":"key","key":"Escape"}"#),
        );
        assert_eq!(code, 400);
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(body["error"].as_str().unwrap().contains("is not running"));
        assert!(
            begin.elapsed() < Duration::from_secs(1),
            "input was blocked by an unrelated HTTP reader"
        );
        drop(slow);
        cleanup_takeover_session(dir.path(), "parallel-input").unwrap();
    }

    #[test]
    fn takeover_state_publication_preserves_in_flight_readers() {
        let dir = tempfile::tempdir().unwrap();
        let first = start_or_reuse_takeover_session(
            dir.path(),
            "publication",
            None,
            Some("first"),
            Some(60_000),
            Some(false),
            &CaptchaDetection::default(),
        )
        .unwrap();
        // Opening a reader before publication deterministically exercises the
        // same inode a concurrent listener read would hold during a save.
        let mut reader = std::fs::File::open(takeover_path(dir.path(), "publication")).unwrap();
        let mut next = first.clone();
        next.reason = Some("next".repeat(1024));
        save_takeover_state(dir.path(), &next).unwrap();
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).unwrap();
        let previous: BrowserTakeoverState = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            previous.reason, first.reason,
            "an in-flight reader must retain its complete snapshot"
        );
        assert_eq!(
            load_takeover_state(dir.path(), "publication")
                .unwrap()
                .unwrap()
                .reason,
            next.reason
        );
        assert!(is_human_takeover_active(dir.path(), "publication", now_ms()).unwrap());
        let (code, _) = raw_http_call(
            first.port,
            "GET",
            &format!("/takeover/{}/state", first.token),
            None,
        );
        assert_eq!(code, 200, "publication must not stop the active listener");
        cleanup_takeover_session(dir.path(), "publication").unwrap();
    }

    #[test]
    #[ignore = "launches real Chromium; CDP I/O fails intermittently with EINTR in CI; deleted with browser.rs, tracking: https://github.com/SylphxAI/anyagents/issues/200"]
    fn takeover_viewport_preserves_identity_and_restores_every_exit() {
        let _browser = browser_fixture_guard();
        if !has_chromium() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        execute(
            dir.path(),
            "viewport",
            BrowserRequest {
                action: "open".into(),
                headless: Some(true),
                url: Some("about:blank".into()),
                ..Default::default()
            },
        )
        .unwrap();
        let mut rt = load_runtime(dir.path(), "viewport").unwrap().unwrap();
        let ws = select_page_target(dir.path(), &mut rt, None)
            .unwrap()
            .ws_url
            .unwrap();
        let mut cdp = CdpSession::connect(rt.cdp_port, &ws).unwrap();
        let identity_js = "({ua:navigator.userAgent,hints:JSON.stringify(navigator.userAgentData?.toJSON()),screen:[screen.width,screen.height]})";
        let identity = eval_js(&mut cdp, identity_js).unwrap();
        let baseline = eval_js(&mut cdp, TAKEOVER_METRICS_JS).unwrap();
        let begin = || {
            start_or_reuse_takeover_session(
                dir.path(),
                "viewport",
                None,
                None,
                Some(60_000),
                Some(false),
                &CaptchaDetection::default(),
            )
            .unwrap()
        };
        let enable = || {
            set_takeover_viewport(
                dir.path(),
                "viewport",
                &serde_json::json!({"enabled":true,"width":390,"height":844}),
            )
            .unwrap()
        };
        let assert_restored = |cdp: &mut CdpSession| {
            assert_eq!(eval_js(cdp, TAKEOVER_METRICS_JS).unwrap(), baseline);
            assert_eq!(eval_js(cdp, identity_js).unwrap(), identity);
        };
        assert!(
            begin().original_viewport.is_none(),
            "phone mode must default off"
        );
        enable();
        let metrics = eval_js(&mut cdp, TAKEOVER_METRICS_JS).unwrap();
        assert_eq!(metrics["width"], 390);
        assert!(metrics["height"].as_u64().unwrap() >= 600);
        assert_eq!(
            metrics["device_scale_factor"],
            baseline["device_scale_factor"]
        );
        assert_eq!(
            eval_js(&mut cdp, identity_js).unwrap(),
            identity,
            "UA and Client Hints must remain byte-identical"
        );
        set_takeover_viewport(
            dir.path(),
            "viewport",
            &serde_json::json!({"enabled":false}),
        )
        .unwrap();
        assert_restored(&mut cdp);
        enable();
        complete_takeover_session(dir.path(), "viewport", None).unwrap();
        assert_restored(&mut cdp);
        assert!(
            load_takeover_state(dir.path(), "viewport")
                .unwrap()
                .unwrap()
                .completed
        );

        begin();
        enable();
        execute(
            dir.path(),
            "viewport",
            BrowserRequest {
                action: "release_takeover".into(),
                ..Default::default()
            },
        )
        .unwrap();
        assert_restored(&mut cdp);

        begin();
        enable();
        let st = load_takeover_state(dir.path(), "viewport")
            .unwrap()
            .unwrap();
        // Advance only the explicit reaper clock, not the stored deadline. The
        // listener's real-time reaper must not race this forced expiry.
        reap_expired_takeover(dir.path(), "viewport", st.expires_at_ms).unwrap();
        assert_restored(&mut cdp);
        assert!(
            set_takeover_viewport(dir.path(), "viewport", &serde_json::json!({"enabled":true}))
                .is_err(),
            "late toggle after reap must be refused"
        );
        complete_takeover_session(dir.path(), "viewport", None).unwrap();
        assert_restored(&mut cdp);

        begin();
        enable();
        let st = load_takeover_state(dir.path(), "viewport")
            .unwrap()
            .unwrap();
        let heartbeat = st.viewport_heartbeat_ms.unwrap();
        reap_disconnected_takeover_viewport(dir.path(), "viewport", heartbeat + 10_000).unwrap();
        assert!(
            load_takeover_state(dir.path(), "viewport")
                .unwrap()
                .unwrap()
                .original_viewport
                .is_some(),
            "a live heartbeat must retain phone layout through the lease boundary"
        );
        reap_expired_takeover(dir.path(), "viewport", heartbeat + 10_001).unwrap();
        assert!(is_human_takeover_active(dir.path(), "viewport", heartbeat + 10_001).unwrap());
        reap_disconnected_takeover_viewport(dir.path(), "viewport", heartbeat + 10_001).unwrap();
        assert_restored(&mut cdp);
        assert!(
            !load_takeover_state(dir.path(), "viewport")
                .unwrap()
                .unwrap()
                .completed,
            "disconnect restores metrics, not control"
        );
        enable();
        cleanup_takeover_session(dir.path(), "viewport").unwrap();
        assert_restored(&mut cdp);
        let _ = execute(
            dir.path(),
            "viewport",
            BrowserRequest {
                action: "close".into(),
                ..Default::default()
            },
        );
    }
    #[test]
    #[ignore = "Chromium startup and OOPIF click/focus races; see docs/verification.md"]
    fn takeover_focus_finds_cross_origin_input() {
        let _browser = browser_fixture_guard();
        if !has_chromium() {
            return;
        }
        let child = spawn_html_server(
            "127.0.0.2",
            "<style>body{margin:0}input{width:240px;height:44px}</style><input id='entry'>".into(),
        );
        let parent = spawn_html_server(
            "127.0.0.1",
            format!(
                "<style>body{{margin:0}}iframe{{position:absolute;left:60px;top:80px;border:0;width:400px;height:200px}}</style><iframe src='http://127.0.0.2:{child}'></iframe>"
            ),
        );
        let dir = tempfile::tempdir().unwrap();
        execute(
            dir.path(),
            "frame-focus",
            BrowserRequest {
                action: "open".into(),
                url: Some(format!("http://127.0.0.1:{parent}")),
                headless: Some(true),
                wait_ms: Some(400),
                ..Default::default()
            },
        )
        .unwrap();
        let response = dispatch_takeover_input(
            dir.path(),
            "frame-focus",
            &serde_json::json!({"type":"click","x":100,"y":100}),
        )
        .unwrap();
        let _ = stop_profile(dir.path(), "frame-focus");
        assert_eq!(response["focus"]["editable"], true, "{response}");
        assert_eq!(response["focus"]["rect"]["x"], 60.0);
        assert_eq!(response["focus"]["rect"]["y"], 80.0);
    }

    /// Chromium runs as the server's user, so a caller's navigation reaches
    /// http(s) and about:blank only, however the URL is spelled.
    #[test]
    fn navigation_allows_only_http_https_and_blank() {
        for (raw, want) in [
            ("https://example.test/a", "https://example.test/a"),
            ("HTTP://example.test", "HTTP://example.test"),
            ("example.test/path", "https://example.test/path"),
            ("localhost:8080/x", "https://localhost:8080/x"),
            ("127.0.0.1:9", "https://127.0.0.1:9"),
            ("about:blank", "about:blank"),
            (" ABOUT:BLANK ", "about:blank"),
            ("  https://example.test  ", "https://example.test"),
        ] {
            assert_eq!(navigable_url(raw).unwrap(), want, "{raw}");
        }
        for raw in [
            "file:///scratch/coordinator/anyagents-data/secrets/operator-token",
            "FILE:///etc/passwd",
            "file:/etc/passwd",
            "fi\tle:///etc/passwd",
            "\u{1}file:///etc/passwd",
            "fi\nle:///etc/passwd",
            "view-source:https://example.test",
            "view-source:file:///etc/passwd",
            "chrome://settings",
            "chrome-untrusted://x",
            "devtools://devtools/bundled/inspector.html",
            "data:text/html,<script>1</script>",
            "javascript:alert(1)",
            "blob:https://example.test/x",
            "filesystem:https://example.test/temporary/x",
            "about:config",
            "ftp://example.test/x",
            "",
            "   ",
        ] {
            assert!(navigable_url(raw).is_err(), "{raw:?} must be refused");
        }
        for (url, readable) in [
            ("https://example.test/", true),
            ("about:blank", true),
            ("chrome-error://chromewebdata/", true),
            ("file:///etc/passwd", false),
            ("view-source:https://example.test/", false),
            ("chrome://version/", false),
            ("data:text/html,x", false),
        ] {
            assert_eq!(readable_page_url(url), readable, "{url}");
        }
        assert!(checked_target_id("8F2A0B").is_ok());
        for bad in ["", "../new?file:///etc/passwd", "a/b", "a b"] {
            assert!(checked_target_id(bad).is_err(), "{bad}");
        }
    }

    /// A refused URL never reaches Chromium: no navigation, no new tab,
    /// whatever action carries it; an http(s) one is navigated as before.
    #[test]
    fn a_file_url_is_refused_before_any_cdp_call() {
        let fixture = ScriptedBrowser::new(cdp_ok_response);
        let methods = || -> Vec<String> {
            fixture
                .calls
                .lock()
                .unwrap()
                .iter()
                .map(|c| c["method"].as_str().unwrap_or_default().to_string())
                .collect()
        };
        for (action, tab_action) in [
            ("open", None),
            ("goto", None),
            ("takeover", None),
            ("tabs", Some("new")),
            ("snapshot", None),
        ] {
            let error = fixture
                .execute(BrowserRequest {
                    action: action.into(),
                    tab_action: tab_action.map(Into::into),
                    url: Some("file:///etc/passwd".into()),
                    ..Default::default()
                })
                .unwrap_err()
                .to_string();
            assert!(error.contains("allows only http"), "{action}: {error}");
        }
        assert!(methods().is_empty(), "{:?}", methods());
        fixture
            .execute(BrowserRequest {
                action: "open".into(),
                url: Some("https://example.test/ok".into()),
                wait_ms: Some(0),
                ..Default::default()
            })
            .unwrap();
        let navigated: Vec<_> = fixture
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c["method"] == "Page.navigate")
            .map(|c| c["params"]["url"].clone())
            .collect();
        assert_eq!(navigated, [serde_json::json!("https://example.test/ok")]);
    }

    /// A page already on a host file is neither read nor driven: it goes
    /// back to about:blank and the request fails.
    #[test]
    fn a_page_showing_a_file_is_reset_not_read() {
        let fixture = ScriptedBrowser::new(|request| {
            if request["method"] == "list_url" {
                return serde_json::json!({"url": "file:///etc/passwd"});
            }
            cdp_ok_response(request)
        });
        for action in ["snapshot", "eval", "screenshot"] {
            let error = fixture
                .execute(BrowserRequest {
                    action: action.into(),
                    script: Some("document.body.innerText".into()),
                    ..Default::default()
                })
                .unwrap_err()
                .to_string();
            assert!(error.contains("reset to about:blank"), "{action}: {error}");
        }
        let calls = fixture.calls.lock().unwrap().clone();
        assert!(
            calls
                .iter()
                .all(|c| c["method"] == "Page.navigate" && c["params"]["url"] == "about:blank"),
            "{calls:?}"
        );
    }

    /// A screenshot `path` is written with the caller's access: one the
    /// caller may not write is never written by the server, and the shot
    /// stays in the profile.
    #[test]
    fn a_screenshot_path_is_written_only_with_the_callers_access() {
        let fixture = ScriptedBrowser::new(|request| {
            if request["method"] == "Page.captureScreenshot" {
                return serde_json::json!({"result": {"data": "iVBORw0KGgo="}});
            }
            cdp_ok_response(request)
        });
        let outside = tempfile::tempdir().unwrap();
        let target = outside.path().join("victim.txt");
        std::fs::write(&target, b"keep").unwrap();
        let shot = |files: &crate::agent_file::FileAuthority| {
            super::execute(
                fixture.data.path(),
                "fixture",
                BrowserRequest {
                    action: "screenshot".into(),
                    path: Some(target.display().to_string()),
                    ..Default::default()
                },
                files,
            )
            .unwrap()
        };
        let refused = shot(&crate::agent_file::FileAuthority::Refused("no seat".into()));
        assert_eq!(std::fs::read(&target).unwrap(), b"keep");
        assert!(
            refused["path_refused"]
                .as_str()
                .unwrap()
                .contains("no seat")
        );
        let kept = PathBuf::from(refused["path"].as_str().unwrap());
        assert!(kept.starts_with(fixture.data.path()), "{}", kept.display());
        assert_eq!(std::fs::read(&kept).unwrap(), b"\x89PNG\r\n\x1a\n");
        let own = shot(&crate::agent_file::FileAuthority::Own);
        assert_eq!(own["path"], target.display().to_string());
        assert!(own.get("path_refused").is_none());
        assert_eq!(std::fs::read(&target).unwrap(), b"\x89PNG\r\n\x1a\n");
    }

    /// No flag that opens host files to pages is ever passed, and Chromium's
    /// home, where downloads land, is inside its profile.
    #[test]
    fn chromium_starts_without_file_access_and_downloads_into_its_profile() {
        let data = tempfile::tempdir().unwrap();
        let fp = ensure_fingerprint(data.path(), "p").unwrap();
        let args = chromium_args(&profile_dir(data.path(), "p").join("user-data"), 9222, &fp);
        refuse_file_access_flags(&args).unwrap();
        for flag in FILE_ACCESS_FLAGS {
            let mut with = args.clone();
            with.push(flag.to_string());
            assert!(refuse_file_access_flags(&with).is_err(), "{flag}");
        }
        let home = chromium_home(&profile_dir(data.path(), "p"));
        for (_, value) in chromium_env(&home) {
            assert!(value.starts_with(profile_dir(data.path(), "p")));
        }
    }

    /// Real Chromium, headless and on Xvfb as the server starts it: a page
    /// still loads over http, and even a raw DevTools client (any process on
    /// the host can reach the port) cannot make it read a file outside its
    /// profile, because the jail, not the URL check, closes the host to it.
    #[test]
    #[ignore = "launches real Chromium; run on a Linux host with Landlock"]
    fn chromium_in_its_jail_reads_no_host_file_even_over_raw_cdp() {
        let _browser = browser_fixture_guard();
        if !has_chromium() || jail::landlock_abi() < 1 {
            return;
        }
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let body = "<html><body>hello from the jail test</body></html>";
                let _ = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        let modes: &[Option<bool>] = if Path::new("/usr/bin/xvfb-run").is_file() {
            &[Some(true), Some(false)]
        } else {
            &[Some(true)]
        };
        for headless in modes {
            // As long as a live data directory's profile path, which no Unix
            // socket name can hold.
            let data = tempfile::Builder::new()
                .prefix(&"long-data-directory-".repeat(4))
                .tempdir()
                .unwrap();
            let secrets = data.path().join("secrets");
            std::fs::create_dir_all(&secrets).unwrap();
            let token = secrets.join("operator-token");
            std::fs::write(&token, "operator-secret").unwrap();
            let (mut runtime, _) =
                ensure_browser_running(data.path(), "jail", *headless, None).unwrap();
            let target = select_page_target(data.path(), &mut runtime, None).unwrap();
            let mut session =
                CdpSession::connect(runtime.cdp_port, target.ws_url.as_deref().unwrap()).unwrap();
            let text = |session: &mut CdpSession| {
                eval_js(session, "document.body ? document.body.innerText : ''")
                    .unwrap()
                    .as_str()
                    .unwrap_or_default()
                    .to_string()
            };
            session
                .call(
                    "Page.navigate",
                    serde_json::json!({ "url": format!("http://127.0.0.1:{port}/") }),
                )
                .unwrap();
            std::thread::sleep(Duration::from_millis(1500));
            assert!(
                text(&mut session).contains("hello from the jail test"),
                "{headless:?}"
            );
            // What a raw DevTools client would do, past every server check.
            let _ = session.call(
                "Page.navigate",
                serde_json::json!({ "url": format!("file://{}", token.display()) }),
            );
            std::thread::sleep(Duration::from_millis(1500));
            let shown = text(&mut session);
            assert!(!shown.contains("operator-secret"), "{headless:?}: {shown}");
            let downloads = chromium_home(&profile_dir(data.path(), "jail")).join("Downloads");
            drop(session);
            let tmp =
                std::fs::read_to_string(chromium_tmp_record(&profile_dir(data.path(), "jail")))
                    .unwrap();
            assert!(Path::new(tmp.trim()).is_dir());
            stop_profile(data.path(), "jail").unwrap();
            assert!(!Path::new(tmp.trim()).exists());
            assert!(downloads.is_dir());
            assert_eq!(std::fs::read_to_string(&token).unwrap(), "operator-secret");
        }
    }
}
