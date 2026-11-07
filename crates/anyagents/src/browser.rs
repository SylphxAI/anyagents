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
use std::time::Duration;

use anyagents_core::{Error, now_ms};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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
    /// Persistent browser profile name (e.g. `"default"`, `"xenon"`, `"github"`, `"x"`).
    /// Login cookies, localStorage, and deterministic anti-detect fingerprint persist per profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    /// URL to open when `action` is `"open"` or `"goto"` (or when opening a new tab).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Target element reference (`"@e1"`, `"@e2"` from `snapshot`) or CSS selector for `"click"`, `"fill"`, `"type"`.
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
    let bin = chromium_binary();
    if let Ok(out) = Command::new(&bin).arg("--version").output() {
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

fn chromium_binary() -> String {
    if let Ok(custom) = std::env::var("ANYAGENTS_CHROMIUM_BIN")
        && !custom.trim().is_empty()
    {
        return custom.trim().to_string();
    }
    static RESOLVED: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    RESOLVED
        .get_or_init(|| {
            for candidate in [
                "/usr/bin/google-chrome-stable",
                "/usr/bin/google-chrome",
                "/usr/bin/chromium",
                "/usr/bin/chromium-browser",
            ] {
                if Path::new(candidate).is_file()
                    && Command::new(candidate)
                        .arg("--version")
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status()
                        .is_ok_and(|s| s.success())
                {
                    return candidate.to_string();
                }
            }
            "chromium".to_string()
        })
        .clone()
}

pub fn ensure_fingerprint(data: &Path, profile: &str) -> Result<BrowserFingerprint, Error> {
    let dir = profile_dir(data, profile);
    std::fs::create_dir_all(&dir)?;
    let fp_path = dir.join("fingerprint.json");
    if let Ok(bytes) = std::fs::read(&fp_path)
        && let Ok(existing) = serde_json::from_slice::<BrowserFingerprint>(&bytes)
    {
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
    if let Ok(bytes) = serde_json::to_vec_pretty(&fp) {
        let _ = std::fs::write(&fp_path, bytes);
    }
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
  try {{
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
  }} catch (_) {{}}
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
    if pid == 0 {
        return false;
    }
    Path::new(&format!("/proc/{pid}")).exists()
}

fn cdp_port_alive(port: u16) -> bool {
    TcpStream::connect_timeout(
        &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
        Duration::from_millis(250),
    )
    .is_ok()
}

fn load_runtime(data: &Path, profile: &str) -> Option<BrowserRuntimeState> {
    let path = profile_dir(data, profile).join("runtime.json");
    let bytes = std::fs::read(&path).ok()?;
    let state: BrowserRuntimeState = serde_json::from_slice(&bytes).ok()?;
    if process_alive(state.pid) && cdp_port_alive(state.cdp_port) {
        Some(state)
    } else {
        let _ = std::fs::remove_file(&path);
        None
    }
}

fn save_runtime(data: &Path, state: &BrowserRuntimeState) {
    let dir = profile_dir(data, &state.profile);
    let _ = std::fs::create_dir_all(&dir);
    if let Ok(bytes) = serde_json::to_vec_pretty(state) {
        let _ = std::fs::write(dir.join("runtime.json"), bytes);
    }
}

fn touch_runtime(data: &Path, state: &mut BrowserRuntimeState) {
    state.last_used_at_ms = now_ms();
    save_runtime(data, state);
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

fn ensure_browser_running(
    data: &Path,
    profile: &str,
    headless_override: Option<bool>,
) -> Result<(BrowserRuntimeState, BrowserFingerprint), Error> {
    let fp = ensure_fingerprint(data, profile)?;
    if let Some(mut running) = load_runtime(data, profile) {
        touch_runtime(data, &mut running);
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
    let bin = chromium_binary();

    let mut chrome_args = vec![
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
    ];

    let (mut cmd, display_mode, actual_headless) = if !want_headless && has_display {
        let mut c = Command::new(&bin);
        c.args(&chrome_args);
        (c, "x11-display".to_string(), false)
    } else if !want_headless && has_xvfb {
        let mut c = Command::new("/usr/bin/xvfb-run");
        let xvfb_server_args = "-screen 0 1920x1080x24 -ac +extension GLX +render -noreset";
        c.arg("-a")
            .arg("-s")
            .arg(xvfb_server_args)
            .arg(&bin)
            .args(&chrome_args);
        (c, "xvfb-headed".to_string(), false)
    } else {
        chrome_args.insert(0, "--headless=new".to_string());
        chrome_args.insert(1, "--hide-scrollbars".to_string());
        chrome_args.insert(2, "--disable-gpu".to_string());
        chrome_args.insert(3, "--no-zygote".to_string());
        let mut c = Command::new(&bin);
        c.args(&chrome_args);
        (c, "headless-new".to_string(), true)
    };

    let log_file = std::fs::File::create(dir.join("browser.log"))?;
    let err_file = log_file.try_clone()?;
    cmd.stdin(Stdio::null())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(err_file))
        .env("TZ", &fp.timezone);

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

    let child = cmd
        .spawn()
        .map_err(|err| Error::Message(format!("failed to launch chromium: {err}")))?;
    let pid = child.id();

    let deadline = std::time::Instant::now() + Duration::from_secs(12);
    while std::time::Instant::now() < deadline {
        if cdp_port_alive(port) {
            break;
        }
        std::thread::sleep(Duration::from_millis(80));
    }
    if !cdp_port_alive(port) {
        stop_process_group(pid);
        return Err(Error::Message(format!(
            "chromium did not open CDP port {port} within 12s"
        )));
    }

    let now = now_ms();
    let state = BrowserRuntimeState {
        profile: profile.to_string(),
        pid,
        cdp_port: port,
        headless: actual_headless,
        display_mode,
        active_target_id: None,
        started_at_ms: now,
        last_used_at_ms: now,
    };
    save_runtime(data, &state);
    Ok((state, fp))
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
    if let Ok(bytes) = std::fs::read(&rt_path)
        && let Ok(mut rt) = serde_json::from_slice::<BrowserRuntimeState>(&bytes)
    {
        if cdp_port_alive(rt.cdp_port)
            && let Ok(page) = select_page_target(data, &mut rt, None)
            && let Some(ws_url) = page.ws_url
            && let Ok(mut cdp) = CdpSession::connect(rt.cdp_port, &ws_url)
        {
            released_keys = release_recorded_held_keys(data, profile, &mut cdp);
        }
        stopped_pid = Some(rt.pid);
        stop_process_group(rt.pid);
    }
    clear_held_keys(data, profile);
    let _ = cleanup_takeover_session(data, profile);
    let _ = std::fs::remove_file(&rt_path);
    cleanup_singleton_locks(&dir.join("user-data"));
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
    let root = profiles_root(data);
    let Ok(entries) = std::fs::read_dir(&root) else {
        return;
    };
    let now = now_ms();
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        reap_expired_takeover(data, name, now);
        if is_human_takeover_active(data, name, now) {
            continue;
        }
        if let Some(rt) = load_runtime(data, name)
            && now.saturating_sub(rt.last_used_at_ms) > idle_ms
        {
            let _ = stop_profile(data, name);
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

fn cdp_http_get(port: u16, path: &str) -> Result<Vec<u8>, Error> {
    let mut stream = TcpStream::connect_timeout(
        &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
        Duration::from_secs(3),
    )?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let req = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes())?;
    let mut buf = Vec::with_capacity(4096);
    let mut temp = [0u8; 4096];
    let mut header_end: Option<usize> = None;
    let mut content_length: Option<usize> = None;
    loop {
        match stream.read(&mut temp) {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&temp[..n]);
                if header_end.is_none()
                    && let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n")
                {
                    let hend = pos + 4;
                    header_end = Some(hend);
                    let headers_str = String::from_utf8_lossy(&buf[..pos]);
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
        Ok(buf)
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
            save_runtime(data, rt);
        }
        return Ok(found);
    }
    let chosen = pages
        .into_iter()
        .next()
        .ok_or_else(|| Error::Message("no browser page target available".into()))?;
    rt.active_target_id = Some(chosen.id.clone());
    save_runtime(data, rt);
    Ok(chosen)
}

/// Pure-Rust local RFC 6455 WebSocket client for Chrome DevTools Protocol (`127.0.0.1:<port>`).
struct CdpSession {
    port: u16,
    stream: TcpStream,
    read_buf: Vec<u8>,
    next_id: u64,
}

impl CdpSession {
    fn connect(port: u16, ws_url: &str) -> Result<Self, Error> {
        let path = ws_url
            .find("/devtools/")
            .map(|idx| &ws_url[idx..])
            .unwrap_or("/devtools/browser");
        let mut stream = TcpStream::connect_timeout(
            &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
            Duration::from_secs(5),
        )?;
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
            let n = stream.read(&mut temp)?;
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
            let read_n = self.stream.read(&mut temp)?;
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
    let _ = cdp.call("Page.enable", serde_json::json!({}));
    let _ = cdp.call("Runtime.enable", serde_json::json!({}));
    let _ = cdp.call("DOM.enable", serde_json::json!({}));
    let _ = cdp.call("Network.enable", serde_json::json!({}));
    let _ = cdp.call(
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
    );
    let _ = cdp.call(
        "Emulation.setTimezoneOverride",
        serde_json::json!({ "timezoneId": fp.timezone }),
    );
    let script = stealth_init_script(fp);
    let _ = cdp.call(
        "Page.addScriptToEvaluateOnNewDocument",
        serde_json::json!({ "source": script }),
    );
    let _ = cdp.call(
        "Runtime.evaluate",
        serde_json::json!({
            "expression": script,
            "returnByValue": true,
        }),
    );
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

fn load_takeover_state(data: &Path, profile: &str) -> Option<BrowserTakeoverState> {
    let bytes = std::fs::read(takeover_path(data, profile)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn save_takeover_state(data: &Path, state: &BrowserTakeoverState) {
    static NEXT_WRITE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dir = profile_dir(data, &state.profile);
    let seq = NEXT_WRITE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp = dir.join(format!(".takeover.{}.{seq}.json", std::process::id()));
    let publish = || -> std::io::Result<()> {
        std::fs::create_dir_all(&dir)?;
        let bytes = serde_json::to_vec_pretty(state).map_err(std::io::Error::other)?;
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&tmp)?;
        file.write_all(&bytes)?;
        // Readers include the listener's liveness loop. Never truncate the live
        // file: a partial JSON read looks like a missing session and stops it.
        std::fs::rename(&tmp, dir.join("takeover.json"))?;
        Ok(())
    };
    let _ = publish();
    let _ = std::fs::remove_file(tmp);
}

fn is_human_takeover_active(data: &Path, profile: &str, now: i64) -> bool {
    load_takeover_state(data, profile).is_some_and(|st| {
        st.holder == "human" && !st.completed && now < st.expires_at_ms && cdp_port_alive(st.port)
    })
}

fn reap_expired_takeover(data: &Path, profile: &str, now: i64) {
    if let Some(mut st) = load_takeover_state(data, profile)
        && st.holder == "human"
        && !st.completed
        && (now >= st.expires_at_ms || !cdp_port_alive(st.port))
    {
        if restore_takeover_viewport(&mut st).is_err() {
            return;
        }
        if let Some(tpid) = st.tunnel_pid.take() {
            stop_process_group(tpid);
        }
        st.holder = "agent".to_string();
        st.completed = true;
        st.completed_at_ms = Some(now);
        st.completion_note = Some("takeover session expired".to_string());
        save_takeover_state(data, &st);
    }
}

fn cleanup_takeover_session(data: &Path, profile: &str) -> Result<(), Error> {
    if let Some(mut st) = load_takeover_state(data, profile) {
        restore_takeover_viewport(&mut st)?;
        if let Some(tpid) = st.tunnel_pid {
            stop_process_group(tpid);
        }
    }
    let _ = std::fs::remove_file(takeover_path(data, profile));
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
fn reap_disconnected_takeover_viewport(data: &Path, profile: &str, now: i64) {
    if let Some(mut st) = load_takeover_state(data, profile)
        && st.original_viewport.is_some()
        && st
            .viewport_heartbeat_ms
            .is_none_or(|last| now - last > 10_000)
        && restore_takeover_viewport(&mut st).is_ok()
    {
        save_takeover_state(data, &st);
    }
}

fn set_takeover_viewport(
    data: &Path,
    profile: &str,
    payload: &serde_json::Value,
) -> Result<serde_json::Value, Error> {
    let mut st = load_takeover_state(data, profile)
        .ok_or_else(|| Error::Message("takeover session is not active".into()))?;
    if st.completed || st.holder != "human" || now_ms() >= st.expires_at_ms {
        return Err(Error::Message("takeover session has ended".into()));
    }
    let enabled = payload
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !enabled {
        restore_takeover_viewport(&mut st)?;
    } else {
        let mut rt = load_runtime(data, profile)
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
            save_takeover_state(data, &st);
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
    save_takeover_state(data, &st);
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

fn generate_takeover_token() -> String {
    let mut bytes = [0u8; 16];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        let _ = f.read_exact(&mut bytes);
    } else {
        let seed = deterministic_profile_seed(&format!("takeover-{}", now_ms()));
        bytes[..8].copy_from_slice(&seed.to_le_bytes());
        bytes[8..].copy_from_slice(&seed.rotate_left(19).to_le_bytes());
    }
    let mut out = String::with_capacity(36);
    out.push_str("tok_");
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
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
) {
    let Some(agent_name) = owner_agent
        .map(str::trim)
        .filter(|s| !s.is_empty() && *s != "default" && *s != "operator")
    else {
        return;
    };
    let db_path = data.join("state.db");
    if !db_path.exists() {
        return;
    }
    let Ok(store) = anyagents_core::Store::open(db_path) else {
        return;
    };
    let Ok(agent) = store.agent_by_name(agent_name) else {
        return;
    };
    let now = now_ms();
    let msg_id = format!("msg_takeover_{profile}_{now}");
    let note_suffix = note
        .filter(|s| !s.trim().is_empty())
        .map(|n| format!(" ({n})"))
        .unwrap_or_default();
    let text = format!(
        "[Browser Takeover Completed] User finished browser takeover on profile `{profile}`{note_suffix} (current url: {url}). Control is now back with you; call `browser({{ \"action\": \"snapshot\", \"profile\": \"{profile}\" }})` to inspect the updated page and continue."
    );
    let _ = store.receive(
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
    );
    if agent.state == "idle" || agent.state == "archived" {
        let _ = store.set_agent_state(&agent.id, "running");
    }
    crate::create::poke_agent_wake_socket(data, &agent.id);
    crate::create::poke_agent_wake_socket_mail(data, &agent.id);
}

fn spawn_cloudflared_quick_tunnel(local_port: u16, token: &str) -> (Option<String>, Option<u32>) {
    if let Ok(explicit_base) = std::env::var("ANYAGENTS_TAKEOVER_URL") {
        let trimmed = explicit_base.trim().trim_end_matches('/');
        if !trimmed.is_empty() {
            return (Some(format!("{trimmed}/takeover/{token}")), None);
        }
    }
    let mut candidates = Vec::new();
    if let Ok(home) = std::env::var("HOME") {
        candidates.push(format!(
            "{}/.local/bin/cloudflared",
            home.trim_end_matches('/')
        ));
    }
    candidates.extend([
        "/usr/local/bin/cloudflared".to_string(),
        "/usr/bin/cloudflared".to_string(),
        "cloudflared".to_string(),
    ]);
    let Some(bin) = candidates.into_iter().find(|p| {
        if p.starts_with('/') {
            Path::new(p).is_file()
        } else {
            Command::new("which")
                .arg(p)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|s| s.success())
        }
    }) else {
        return (None, None);
    };

    let mut cmd = Command::new(bin);
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

fn dispatch_takeover_input(
    data: &Path,
    profile: &str,
    payload: &serde_json::Value,
) -> Result<serde_json::Value, Error> {
    let mut rt = load_runtime(data, profile)
        .ok_or_else(|| Error::Message(format!("browser profile `{profile}` is not running")))?;
    touch_runtime(data, &mut rt);
    let target = select_page_target(data, &mut rt, None)?;
    let ws_url = target
        .ws_url
        .ok_or_else(|| Error::Message("browser page has no WebSocket debugger URL".into()))?;
    let mut cdp = CdpSession::connect(rt.cdp_port, &ws_url)?;

    let kind = payload
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("click");
    match kind {
        "click" => {
            let x = payload.get("x").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let y = payload.get("y").and_then(|v| v.as_f64()).unwrap_or(0.0);
            dispatch_trusted_click(&mut cdp, x, y)?;
            let focus = takeover_focused_field(&mut cdp);
            Ok(serde_json::json!({ "ok": true, "type": "click", "x": x, "y": y, "focus": focus }))
        }
        "mouse" => {
            let event_type = payload
                .get("event_type")
                .and_then(|v| v.as_str())
                .unwrap_or("mouseMoved");
            let x = payload.get("x").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let y = payload.get("y").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let button = payload
                .get("button")
                .and_then(|v| v.as_str())
                .unwrap_or("left");
            let buttons = payload
                .get("buttons")
                .and_then(|v| v.as_u64())
                .unwrap_or(if event_type == "mouseReleased" { 0 } else { 1 });
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
            let focus = if event_type == "mouseReleased" {
                Some(takeover_focused_field(&mut cdp))
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
            dispatch_takeover_key(&mut cdp, payload)?;
            Ok(
                serde_json::json!({ "ok":true, "type":"key", "focus":takeover_focused_field(&mut cdp) }),
            )
        }
        "clipboard" => {
            let selection = takeover_selection(&mut cdp)?;
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
                    takeover_selection_expression(&mut cdp, &expression)
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
                dispatch_takeover_key(&mut cdp, &serde_json::json!({"key":"Enter"}))?;
            }
            // Never echo typed or pasted values, including password text.
            Ok(
                serde_json::json!({ "ok":true, "type":kind, "submitted":submit, "focus":takeover_focused_field(&mut cdp) }),
            )
        }
        "navigate" => {
            if let Some(u) = payload
                .get("url")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
            {
                let _ = cdp.call("Page.navigate", serde_json::json!({ "url": u }));
            }
            Ok(serde_json::json!({ "ok": true, "type": "navigate" }))
        }
        "reload" => {
            let _ = cdp.call("Page.reload", serde_json::json!({}));
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

fn handle_takeover_http_client(
    mut stream: TcpStream,
    data: &Path,
    profile: &str,
    expected_token: &str,
    last_frame: &mut Option<TakeoverFrameSize>,
) -> bool {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(3)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let mut buf = Vec::with_capacity(2048);
    let mut chunk = [0u8; 1024];
    let header_end = loop {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return false,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    break pos + 4;
                }
                if buf.len() > 65536 {
                    return false;
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

    let content_len: usize = header_str
        .lines()
        .find_map(|line| {
            let (k, v) = line.split_once(':')?;
            if k.trim().eq_ignore_ascii_case("content-length") {
                v.trim().parse::<usize>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0);
    while buf.len() < header_end + content_len && buf.len() < 131_072 {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    let body_bytes = if buf.len() >= header_end {
        &buf[header_end..buf.len().min(header_end + content_len)]
    } else {
        &[][..]
    };

    if path_only == "/healthz" {
        let _ = write_http_response(&mut stream, "200 OK", "application/json", b"{\"ok\":true}");
        return false;
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
                    &mut stream,
                    "403 Forbidden",
                    "application/json",
                    b"{\"error\":\"invalid takeover token\"}",
                );
                return false;
            }
            ""
        }
    };

    match (method, subpath) {
        ("GET", "" | "/") => {
            let reason = load_takeover_state(data, profile).and_then(|s| s.reason);
            let html = render_takeover_html(profile, expected_token, reason.as_deref());
            let _ = write_http_response(
                &mut stream,
                "200 OK",
                "text/html; charset=utf-8",
                html.as_bytes(),
            );
            false
        }
        ("GET", "/frame.jpg") => {
            let Some(mut rt) = load_runtime(data, profile) else {
                let _ = write_http_response(
                    &mut stream,
                    "503 Service Unavailable",
                    "application/json",
                    b"{\"error\":\"browser not running\"}",
                );
                return false;
            };
            let Ok(target) = select_page_target(data, &mut rt, None) else {
                let _ = write_http_response(
                    &mut stream,
                    "503 Service Unavailable",
                    "application/json",
                    b"{\"error\":\"no page target\"}",
                );
                return false;
            };
            let Some(ws_url) = target.ws_url else {
                return false;
            };
            let Ok(mut cdp) = CdpSession::connect(rt.cdp_port, &ws_url) else {
                return false;
            };
            let metrics = eval_js(&mut cdp, TAKEOVER_METRICS_JS).unwrap_or(serde_json::Value::Null);
            if let Ok(shot) = cdp.call(
                "Page.captureScreenshot",
                serde_json::json!({
                    "format": "jpeg",
                    "quality": 75,
                    "captureBeyondViewport": false,
                }),
            ) && let Some(b64) = shot.get("data").and_then(|v| v.as_str())
                && let Ok(jpeg_bytes) = decode_base64(b64)
            {
                *last_frame = jpeg_size(&jpeg_bytes);
                let dpr = metrics["device_scale_factor"].as_f64().unwrap_or(1.0);
                let width = metrics["width"].as_u64().unwrap_or(1280);
                let height = metrics["height"].as_u64().unwrap_or(800);
                let _ = write_http_response_with_headers(
                    &mut stream,
                    "200 OK",
                    "image/jpeg",
                    &jpeg_bytes,
                    &format!(
                        "X-Remote-Dpr: {dpr}\r\nX-Remote-Width: {width}\r\nX-Remote-Height: {height}\r\n"
                    ),
                );
            } else {
                let _ = write_http_response(
                    &mut stream,
                    "500 Internal Server Error",
                    "application/json",
                    b"{\"error\":\"capture failed\"}",
                );
            }
            false
        }
        ("GET", "/state") => {
            let mut url = String::new();
            let mut title = String::new();
            let mut cap = CaptchaDetection::default();
            let mut metrics = serde_json::Value::Null;
            if let Some(mut rt) = load_runtime(data, profile)
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
            let mut st = load_takeover_state(data, profile);
            if let Some(st) = st.as_mut()
                && st.original_viewport.is_some()
            {
                st.viewport_heartbeat_ms = Some(now_ms());
                save_takeover_state(data, st);
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
            let _ = write_http_response(&mut stream, "200 OK", "application/json", &bytes);
            false
        }
        ("POST", "/viewport") => {
            let payload = serde_json::from_slice(body_bytes).unwrap_or(serde_json::Value::Null);
            let result = set_takeover_viewport(data, profile, &payload);
            let (status, body) = match result {
                Ok(value) => ("200 OK", value),
                Err(err) => (
                    "400 Bad Request",
                    serde_json::json!({ "ok": false, "error": err.to_string() }),
                ),
            };
            let _ = write_http_response(
                &mut stream,
                status,
                "application/json",
                &serde_json::to_vec(&body).unwrap_or_default(),
            );
            false
        }
        ("POST", "/input") => {
            let payload: serde_json::Value =
                serde_json::from_slice(body_bytes).unwrap_or_else(|_| serde_json::json!({}));
            match dispatch_takeover_input(data, profile, &payload) {
                Ok(res) => {
                    let bytes = serde_json::to_vec(&res).unwrap_or_default();
                    let _ = write_http_response(&mut stream, "200 OK", "application/json", &bytes);
                }
                Err(err) => {
                    let bytes = serde_json::to_vec(&serde_json::json!({
                        "ok": false,
                        "error": err.to_string(),
                    }))
                    .unwrap_or_default();
                    let _ = write_http_response(
                        &mut stream,
                        "400 Bad Request",
                        "application/json",
                        &bytes,
                    );
                }
            }
            false
        }
        ("POST", "/complete") => {
            let payload: serde_json::Value =
                serde_json::from_slice(body_bytes).unwrap_or_else(|_| serde_json::json!({}));
            let note = payload
                .get("note")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let done = complete_takeover_session(data, profile, note.as_deref());
            let bytes = serde_json::to_vec(&done).unwrap_or_default();
            let _ = write_http_response(&mut stream, "200 OK", "application/json", &bytes);
            done["completed"].as_bool().unwrap_or(false)
        }
        _ => {
            let _ = write_http_response(
                &mut stream,
                "404 Not Found",
                "application/json",
                b"{\"error\":\"not found\"}",
            );
            false
        }
    }
}

fn complete_takeover_session(data: &Path, profile: &str, note: Option<&str>) -> serde_json::Value {
    let now = now_ms();
    let mut current_url = String::new();
    if let Some(mut rt) = load_runtime(data, profile)
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
    if let Some(mut st) = load_takeover_state(data, profile) {
        let was_completed = st.completed;
        if let Err(err) = restore_takeover_viewport(&mut st) {
            return serde_json::json!({ "ok": false, "error": err.to_string(), "completed": false });
        }
        if let Some(tpid) = st.tunnel_pid.take() {
            stop_process_group(tpid);
        }
        st.holder = "agent".to_string();
        st.completed = true;
        st.completed_at_ms = Some(now);
        st.completion_note = note
            .map(str::to_string)
            .or_else(|| Some("completed by user".to_string()));
        save_takeover_state(data, &st);
        if !was_completed {
            notify_agent_takeover_completed(
                data,
                st.owner_agent.as_deref(),
                profile,
                &current_url,
                st.completion_note.as_deref(),
            );
        }
        serde_json::json!({
            "ok": true,
            "profile": profile,
            "holder": "agent",
            "completed": true,
            "completed_at_ms": now,
            "url": current_url,
        })
    } else {
        serde_json::json!({
            "ok": true,
            "profile": profile,
            "holder": "agent",
            "completed": true,
            "url": current_url,
        })
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
    let now = now_ms();
    let duration_ms = ttl_ms
        .unwrap_or(DEFAULT_TAKEOVER_TTL_MS)
        .clamp(5_000, 3_600_000) as i64;
    if let Some(mut existing) = load_takeover_state(data, profile)
        && existing.holder == "human"
        && !existing.completed
        && now < existing.expires_at_ms
        && cdp_port_alive(existing.port)
    {
        existing.expires_at_ms = now + duration_ms;
        if let Some(r) = reason.filter(|s| !s.trim().is_empty()) {
            existing.reason = Some(r.to_string());
        }
        save_takeover_state(data, &existing);
        return Ok(existing);
    }

    cleanup_takeover_session(data, profile)?;

    let listener = TcpListener::bind("0.0.0.0:0").or_else(|_| TcpListener::bind("127.0.0.1:0"))?;
    let port = listener.local_addr()?.port();
    let _ = listener.set_nonblocking(true);
    let token = generate_takeover_token();
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
    save_takeover_state(data, &state);

    let data_dir = data.to_path_buf();
    let prof = profile.to_string();
    let tok = token;
    std::thread::Builder::new()
        .name(format!("anyagents-takeover-{prof}"))
        .spawn(move || {
            let mut last_frame = None;
            loop {
                let now = now_ms();
                reap_disconnected_takeover_viewport(&data_dir, &prof, now);
                let Some(cur) = load_takeover_state(&data_dir, &prof) else {
                    break;
                };
                if cur.token != tok || cur.completed || now >= cur.expires_at_ms {
                    if cur.token == tok && !cur.completed && now >= cur.expires_at_ms {
                        reap_expired_takeover(&data_dir, &prof, now);
                        if load_takeover_state(&data_dir, &prof).is_some_and(|s| !s.completed) {
                            std::thread::sleep(Duration::from_millis(100));
                            continue;
                        }
                    }
                    break;
                }
                match listener.accept() {
                    Ok((stream, _)) => {
                        let _ = stream.set_nonblocking(false);
                        let finished = handle_takeover_http_client(
                            stream,
                            &data_dir,
                            &prof,
                            &tok,
                            &mut last_frame,
                        );
                        if finished {
                            std::thread::sleep(Duration::from_millis(150));
                            break;
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(40));
                    }
                    Err(_) => break,
                }
            }
        })
        .map_err(|e| Error::Message(format!("failed to spawn takeover server thread: {e}")))?;

    Ok(state)
}

fn wait_for_takeover_completion(
    data: &Path,
    profile: &str,
    wait_ms: u64,
) -> (Option<BrowserTakeoverState>, CaptchaDetection) {
    let deadline = std::time::Instant::now() + Duration::from_millis(wait_ms.clamp(100, 600_000));
    let mut consecutive_cleared = 0u32;
    loop {
        let now = now_ms();
        reap_expired_takeover(data, profile, now);
        let st = load_takeover_state(data, profile);
        let mut cap = CaptchaDetection::default();
        if let Some(mut rt) = load_runtime(data, profile)
            && let Ok(target) = select_page_target(data, &mut rt, None)
            && let Some(ws_url) = target.ws_url
            && let Ok(mut cdp) = CdpSession::connect(rt.cdp_port, &ws_url)
        {
            cap = detect_captcha(&mut cdp);
        }
        let Some(ref cur) = st else {
            return (None, cap);
        };
        if cur.completed || cur.holder != "human" {
            return (st, cap);
        }
        if cur.initial_captcha_detected {
            if !cap.detected {
                consecutive_cleared += 1;
                if consecutive_cleared >= 2 {
                    let _ = complete_takeover_session(
                        data,
                        profile,
                        Some("CAPTCHA challenge automatically detected as solved"),
                    );
                    return (load_takeover_state(data, profile), cap);
                }
            } else {
                consecutive_cleared = 0;
            }
        }
        if std::time::Instant::now() >= deadline {
            return (st, cap);
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
    reap_expired_takeover(data, profile, now);
    if let Some(st) = load_takeover_state(data, profile)
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
    val["elements"] = serde_json::to_value(&all_elements).unwrap_or_default();
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
        url: val
            .get("url")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        captured_at_ms: now_ms(),
        elements: all_elements,
    };
    if let Ok(bytes) = serde_json::to_vec_pretty(&stored) {
        let _ = std::fs::write(profile_dir(data, profile).join("refs.json"), bytes);
    }
    Ok(val)
}

struct ResolvedTarget {
    css_selector: String,
    fallback_xy: Option<(f64, f64)>,
    frame_id: Option<String>,
}

fn resolve_selector(data: &Path, profile: &str, raw_target: &str) -> ResolvedTarget {
    let trimmed = raw_target.trim();
    if trimmed.starts_with("@e") {
        let refs_path = profile_dir(data, profile).join("refs.json");
        if let Ok(bytes) = std::fs::read(&refs_path)
            && let Ok(stored) = serde_json::from_slice::<StoredRefs>(&bytes)
            && let Some(found) = stored.elements.iter().find(|e| e.r#ref == trimmed)
        {
            return ResolvedTarget {
                css_selector: format!("[data-anyagents-ref=\"{}\"], {}", trimmed, found.selector),
                fallback_xy: Some((found.x, found.y)),
                frame_id: found.frame_id.clone(),
            };
        }
        return ResolvedTarget {
            css_selector: format!("[data-anyagents-ref=\"{trimmed}\"]"),
            fallback_xy: None,
            frame_id: None,
        };
    }
    ResolvedTarget {
        css_selector: trimmed.to_string(),
        fallback_xy: None,
        frame_id: None,
    }
}

fn load_op_service_account_token() -> Option<String> {
    if let Ok(tok) = std::env::var("OP_SERVICE_ACCOUNT_TOKEN")
        && !tok.trim().is_empty()
    {
        return Some(tok.trim().to_string());
    }
    if let Some(home) = crate::sandbox::host_home_dir() {
        for rel in [
            ".config/op-seat/service-account-token",
            ".config/op/service-account-token",
        ] {
            if let Ok(raw) = std::fs::read_to_string(home.join(rel)) {
                let trimmed = raw.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
        }
    }
    None
}

fn ensure_caller_op_home_and_config_dir() -> Option<(PathBuf, PathBuf)> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        let uid = unsafe { libc::geteuid() };
        if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
            let candidate = home.join(".config/op");
            if let Ok(meta) = std::fs::metadata(&candidate)
                && meta.uid() == uid
                && (meta.permissions().mode() & 0o777) == 0o700
            {
                return Some((home, candidate));
            }
        }
        let home_dir = std::env::temp_dir().join(format!("anyagents-op-home-{uid}"));
        let cfg_root = home_dir.join(".config");
        let op_dir = cfg_root.join("op");
        let _ = std::fs::create_dir_all(&op_dir);
        let _ = std::fs::set_permissions(&home_dir, std::fs::Permissions::from_mode(0o700));
        let _ = std::fs::set_permissions(&cfg_root, std::fs::Permissions::from_mode(0o700));
        let _ = std::fs::set_permissions(&op_dir, std::fs::Permissions::from_mode(0o700));
        if op_dir.is_dir() {
            return Some((home_dir, op_dir));
        }
        None
    }
    #[cfg(not(unix))]
    {
        None
    }
}

fn configure_op_command(cmd: &mut Command) {
    cmd.stdin(Stdio::null());
    cmd.env("DESK_SECRET_READ", "1");
    if let Some(host_home) = crate::sandbox::host_home_dir() {
        cmd.env("DESK_OP_HOME", host_home);
    }
    if let Some((op_home, cfg_dir)) = ensure_caller_op_home_and_config_dir() {
        cmd.env("HOME", &op_home);
        cmd.env("XDG_CONFIG_HOME", op_home.join(".config"));
        cmd.env("OP_CONFIG_DIR", cfg_dir);
    }
    if let Some(token) = load_op_service_account_token() {
        cmd.env("OP_SERVICE_ACCOUNT_TOKEN", token);
    }
}

/// Resolve a secret reference (`op://...`, `op-otp://...`, `secret://...`) in memory
/// without ever echoing the plaintext secret in errors, logs, or responses.
fn resolve_fill_value(
    data: &Path,
    raw_text: Option<&str>,
    secret_ref: Option<&str>,
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
        let mut cmd = Command::new("op");
        cmd.args(["read", "--no-newline", candidate]);
        configure_op_command(&mut cmd);
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
        let mut cmd = Command::new("op");
        let trimmed = rest.trim_matches('/');
        if trimmed.is_empty() {
            return Err(Error::Message(
                "invalid 1Password OTP reference format (expected op-otp://<vault>/<item>)".into(),
            ));
        }
        if let Some((vault, item)) = trimmed.split_once('/') {
            cmd.args(["item", "get", item, "--vault", vault, "--otp"]);
        } else {
            cmd.args(["item", "get", trimmed, "--otp"]);
        }
        configure_op_command(&mut cmd);
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

fn load_held_keys(data: &Path, profile: &str) -> Vec<KeySpec> {
    let path = held_keys_path(data, profile);
    std::fs::read(&path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Vec<KeySpec>>(&bytes).ok())
        .unwrap_or_default()
}

fn save_held_keys(data: &Path, profile: &str, keys: &[KeySpec]) {
    let path = held_keys_path(data, profile);
    if keys.is_empty() {
        let _ = std::fs::remove_file(&path);
        return;
    }
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(bytes) = serde_json::to_vec_pretty(keys) {
        let _ = std::fs::write(&path, bytes);
    }
}

fn clear_held_keys(data: &Path, profile: &str) {
    let _ = std::fs::remove_file(held_keys_path(data, profile));
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
        let _ = eval_js_in_frame(cdp, Some(fid), &js);
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
        let _ = eval_js_in_frame(cdp, Some(fid), &js);
    }
    Ok(())
}

fn release_recorded_held_keys(data: &Path, profile: &str, cdp: &mut CdpSession) -> Vec<String> {
    let held = load_held_keys(data, profile);
    if held.is_empty() {
        return Vec::new();
    }
    let mut released = Vec::with_capacity(held.len());
    for spec in held.iter().rev() {
        let _ = dispatch_cdp_key_up(cdp, spec, None);
        released.push(spec.key.clone());
    }
    clear_held_keys(data, profile);
    released
}

struct HeldKeyGuard<'a> {
    data: &'a Path,
    profile: &'a str,
    cdp: &'a mut CdpSession,
    target_frame_id: Option<String>,
    specs: Vec<KeySpec>,
    released: bool,
}

impl<'a> HeldKeyGuard<'a> {
    fn release(&mut self) -> Result<(), Error> {
        if self.released {
            return Ok(());
        }
        self.released = true;
        let mut first_err = None;
        for spec in self.specs.iter().rev() {
            if let Err(err) = dispatch_cdp_key_up(self.cdp, spec, self.target_frame_id.as_deref())
                && first_err.is_none()
            {
                first_err = Some(err);
            }
        }
        clear_held_keys(self.data, self.profile);
        if let Some(err) = first_err {
            Err(err)
        } else {
            Ok(())
        }
    }
}

impl Drop for HeldKeyGuard<'_> {
    fn drop(&mut self) {
        if !self.released {
            self.released = true;
            for spec in self.specs.iter().rev() {
                let _ = dispatch_cdp_key_up(self.cdp, spec, self.target_frame_id.as_deref());
            }
            clear_held_keys(self.data, self.profile);
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
        if let Some(rt) = load_runtime(data, &name) {
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

pub fn execute(
    data: &Path,
    default_profile: &str,
    req: BrowserRequest,
) -> Result<serde_json::Value, Error> {
    reap_idle_browsers(data);
    let action = if req.action.trim().is_empty() {
        if req.url.is_some() { "open" } else { "status" }
    } else {
        req.action.trim()
    }
    .to_ascii_lowercase();

    let profile = sanitize_profile_name(req.profile.as_deref(), default_profile);

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
            let held: Vec<String> = load_held_keys(data, &profile)
                .into_iter()
                .map(|k| k.key)
                .collect();
            let takeover = load_takeover_state(data, &profile);
            if let Some(rt) = load_runtime(data, &profile) {
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

    check_human_takeover_fence(data, &profile, &action)?;

    let (mut rt, fp) = ensure_browser_running(data, &profile, req.headless)?;

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
                let u = req.url.as_deref().unwrap_or("about:blank");
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
                    save_runtime(data, &rt);
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
                let _ = cdp_http_get(rt.cdp_port, &format!("/json/activate/{tid}"));
                rt.active_target_id = Some(tid.to_string());
                save_runtime(data, &rt);
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
                let _ = cdp_http_get(rt.cdp_port, &format!("/json/close/{tid}"));
                if rt.active_target_id.as_deref() == Some(&tid) {
                    rt.active_target_id = None;
                    save_runtime(data, &rt);
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
    apply_stealth(&mut cdp, &fp)?;

    match action.as_str() {
        "open" | "goto" | "navigate" => {
            let raw_url = req
                .url
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| Error::Message("browser open requires `url`".into()))?;
            let url = if raw_url.contains("://") || raw_url.starts_with("about:") {
                raw_url.to_string()
            } else {
                format!("https://{raw_url}")
            };
            clear_held_keys(data, &profile);
            cdp.call("Page.navigate", serde_json::json!({ "url": url }))?;
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
            if let Some(st) = load_takeover_state(data, &profile) {
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
            let resolved = resolve_selector(data, &profile, raw_sel);
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
                let _ = dispatch_trusted_click(&mut cdp, x, y);
                let check_js = format!(
                    r#"(() => {{
                      if (window.__anyagents_click_hit) return true;
                      const el = document.querySelector({sel_json});
                      if (el && el.click) {{ el.focus && el.focus(); el.click(); return true; }}
                      return false;
                    }})()"#
                );
                let _ = eval_js_in_frame(&mut cdp, active_frame_id.as_deref(), &check_js);
            } else if let Some((x, y)) = resolved.fallback_xy {
                let _ = dispatch_trusted_click(&mut cdp, x, y);
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
            let (text_val, secret_source) =
                resolve_fill_value(data, req.text.as_deref(), req.secret_ref.as_deref())?;
            let resolved = resolve_selector(data, &profile, raw_sel);
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
                    let matched = false;
                    for (const opt of Array.from(el.options || [])) {{
                      if (opt.value === targetVal || (opt.text && opt.text.trim() === targetVal)) {{
                        el.value = opt.value;
                        matched = true;
                        break;
                      }}
                    }}
                    if (!matched) el.value = targetVal;
                    el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                    el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                    return {{ found: true, kind: 'select', tag }};
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
                    let _ = dispatch_trusted_click(&mut cdp, click_x, click_y);
                    let _ = eval_js_in_frame(&mut cdp, active_frame_id.as_deref(), &prep_js);
                }
            }

            if prep.get("kind").and_then(|v| v.as_str()) != Some("select") {
                if prep.get("had_content").and_then(|v| v.as_bool()) == Some(true) {
                    let _ = cdp.call(
                        "Input.dispatchKeyEvent",
                        serde_json::json!({
                            "type": "keyDown",
                            "key": "Backspace",
                            "code": "Backspace",
                            "windowsVirtualKeyCode": 8,
                            "nativeVirtualKeyCode": 8,
                        }),
                    );
                    let _ = cdp.call(
                        "Input.dispatchKeyEvent",
                        serde_json::json!({
                            "type": "keyUp",
                            "key": "Backspace",
                            "code": "Backspace",
                            "windowsVirtualKeyCode": 8,
                            "nativeVirtualKeyCode": 8,
                        }),
                    );
                }

                // Use per-character hardware `Input.dispatchKeyEvent` for short single-line values
                // (passwords, usernames, 6-digit OTP boxes) so `keydown`, `beforeinput`, `input`, and `keyup`
                // all fire with `event.isTrusted === true` and split OTP boxes auto-advance cleanly.
                if text_val.chars().count() <= 128 && !text_val.contains('\n') {
                    for ch in text_val.chars() {
                        let s = ch.to_string();
                        let _ = cdp.call(
                            "Input.dispatchKeyEvent",
                            serde_json::json!({
                                "type": "keyDown",
                                "key": s,
                                "text": s,
                                "unmodifiedText": s,
                            }),
                        );
                        let _ = cdp.call(
                            "Input.dispatchKeyEvent",
                            serde_json::json!({
                                "type": "keyUp",
                                "key": s,
                            }),
                        );
                    }
                } else if !text_val.is_empty() {
                    let _ = cdp.call(
                        "Input.insertText",
                        serde_json::json!({
                            "text": text_val,
                        }),
                    );
                }

                // Verify and fire `change` event (or apply native setter fallback if a custom control or cross-origin frame missed key events).
                let finalize_js = format!(
                    r#"(() => {{
                      const el = document.querySelector({sel_json});
                      if (!el) return;
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
                    }})()"#
                );
                let _ = eval_js_in_frame(&mut cdp, active_frame_id.as_deref(), &finalize_js);
            }

            if submit {
                let _ = cdp.call(
                    "Input.dispatchKeyEvent",
                    serde_json::json!({
                        "type": "keyDown",
                        "key": "Enter",
                        "code": "Enter",
                        "windowsVirtualKeyCode": 13,
                        "nativeVirtualKeyCode": 13,
                    }),
                );
                let _ = cdp.call(
                    "Input.dispatchKeyEvent",
                    serde_json::json!({
                        "type": "keyUp",
                        "key": "Enter",
                        "code": "Enter",
                        "windowsVirtualKeyCode": 13,
                        "nativeVirtualKeyCode": 13,
                    }),
                );
                if active_frame_id.is_some() {
                    let submit_js = format!(
                        r#"(() => {{
                          const el = document.querySelector({sel_json});
                          if (!el) return;
                          el.dispatchEvent(new KeyboardEvent('keydown', {{ key: 'Enter', code: 'Enter', keyCode: 13, which: 13, bubbles: true }}));
                          el.dispatchEvent(new KeyboardEvent('keyup', {{ key: 'Enter', code: 'Enter', keyCode: 13, which: 13, bubbles: true }}));
                        }})()"#
                    );
                    let _ = eval_js_in_frame(&mut cdp, active_frame_id.as_deref(), &submit_js);
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
                let resolved = resolve_selector(data, &profile, raw_sel);
                target_frame_id = resolved.frame_id.clone();
                let sel_json = serde_json::to_string(&resolved.css_selector)
                    .unwrap_or_else(|_| "\"body\"".into());
                let focus_js = format!(
                    r#"(() => {{
                      const el = document.querySelector({sel_json});
                      if (!el) return false;
                      try {{ window.focus && window.focus(); }} catch (_) {{}}
                      el.focus && el.focus();
                      return true;
                    }})()"#
                );
                let _ = eval_js_in_frame(&mut cdp, target_frame_id.as_deref(), &focus_js);
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
                    let mut remaining = load_held_keys(data, &profile);
                    remaining.retain(|held| {
                        !specs
                            .iter()
                            .any(|s| s.code == held.code || s.key == held.key)
                    });
                    save_held_keys(data, &profile, &remaining);
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
                    let released = release_recorded_held_keys(data, &profile, &mut cdp);
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
                let mut current_held = load_held_keys(data, &profile);
                for spec in &specs {
                    dispatch_cdp_key_down(&mut cdp, spec, false, target_frame_id.as_deref())?;
                    if !current_held
                        .iter()
                        .any(|h| h.code == spec.code && h.key == spec.key)
                    {
                        current_held.push(spec.clone());
                    }
                }
                save_held_keys(data, &profile, &current_held);
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
            let _ = release_recorded_held_keys(data, &profile, &mut cdp);

            let requested_hold = req.hold_ms.or(req.duration_ms);
            let hold_ms = if action == "hold" {
                requested_hold.or(req.wait_ms).unwrap_or(500)
            } else {
                requested_hold.unwrap_or(0)
            }
            .min(30_000);
            let repeat = req.repeat.unwrap_or(false);

            if hold_ms > 0 {
                save_held_keys(data, &profile, &specs);
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
                for spec in &specs {
                    dispatch_cdp_key_down(&mut cdp, spec, false, target_frame_id.as_deref())?;
                }
                for spec in specs.iter().rev() {
                    dispatch_cdp_key_up(&mut cdp, spec, target_frame_id.as_deref())?;
                }
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
            let final_path = if let Some(ref target) = requested_path {
                if let Some(parent) = target.parent()
                    && !parent.as_os_str().is_empty()
                {
                    let _ = std::fs::create_dir_all(parent);
                }
                if std::fs::write(target, &png_bytes).is_ok() {
                    target.clone()
                } else {
                    std::fs::write(&fallback_path, &png_bytes)?;
                    #[cfg(unix)]
                    let copied_to_seat = {
                        use std::os::unix::fs::MetadataExt as _;
                        if let Some(parent) = target.parent()
                            && let Ok(meta) = std::fs::metadata(parent)
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
                                .arg(&fallback_path)
                                .arg(target)
                                .status()
                                .is_ok_and(|s| s.success())
                        } else {
                            false
                        }
                    };
                    #[cfg(not(unix))]
                    let copied_to_seat = false;
                    if copied_to_seat {
                        target.clone()
                    } else {
                        fallback_path
                    }
                }
            } else {
                std::fs::write(&fallback_path, &png_bytes)?;
                fallback_path
            };
            Ok(serde_json::json!({
                "profile": profile,
                "path": final_path.display().to_string(),
                "bytes": png_bytes.len(),
            }))
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
                let url = if raw_url.contains("://") || raw_url.starts_with("about:") {
                    raw_url.to_string()
                } else {
                    format!("https://{raw_url}")
                };
                let _ = cdp.call("Page.navigate", serde_json::json!({ "url": url }));
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
                let (after_st, after_cap) = wait_for_takeover_completion(data, &profile, wait_ms);
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
                wait_for_takeover_completion(data, &profile, wait_ms)
            } else {
                reap_expired_takeover(data, &profile, now_ms());
                (
                    load_takeover_state(data, &profile),
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
            let _ = complete_takeover_session(data, &profile, Some(note));
            cleanup_takeover_session(data, &profile)?;
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
    use super::*;
    use anyagents_core::{CredentialStore, FileCredentialStore};
    use std::io::{Read, Write};
    use std::net::TcpListener;

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
    fn cross_origin_iframe_snapshot_fill_and_click_with_secret_ref() {
        if !Path::new(&chromium_binary()).exists() {
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
    fn held_key_moves_raf_fixture_stops_on_release_and_isolates_profiles() {
        if !Path::new(&chromium_binary()).exists() {
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
        assert_eq!(load_held_keys(dir.path(), "profile-a").len(), 1);

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
            load_held_keys(dir.path(), "profile-a").is_empty(),
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
    fn human_takeover_captcha_detection_fencing_and_remote_solve() {
        if !Path::new(&chromium_binary()).exists() {
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
    fn takeover_clipboard_ui_acceptance() {
        if !Path::new(&chromium_binary()).exists() {
            return;
        }
        run_takeover_ui_acceptance(true);
    }

    #[test]
    #[ignore = "requires Chromium and Node >=22; run explicitly for takeover UI changes"]
    fn mobile_takeover_ui_acceptance() {
        run_takeover_ui_acceptance(false);
    }

    fn run_takeover_ui_acceptance(clipboard_only: bool) {
        assert!(
            Path::new(&chromium_binary()).exists(),
            "Chromium is required"
        );
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
        let mut rt = load_runtime(dir.path(), "mobile-ui").unwrap();
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
            .env("ANYAGENTS_CHROMIUM", chromium_binary())
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
        save_takeover_state(dir.path(), &next);
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
                .reason,
            next.reason
        );
        assert!(is_human_takeover_active(
            dir.path(),
            "publication",
            now_ms()
        ));
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
    fn takeover_viewport_preserves_identity_and_restores_every_exit() {
        if !Path::new(&chromium_binary()).exists() {
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
        let mut rt = load_runtime(dir.path(), "viewport").unwrap();
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
        complete_takeover_session(dir.path(), "viewport", None);
        assert_restored(&mut cdp);
        assert!(
            load_takeover_state(dir.path(), "viewport")
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
        let st = load_takeover_state(dir.path(), "viewport").unwrap();
        // Advance only the explicit reaper clock, not the stored deadline. The
        // listener's real-time reaper must not race this forced expiry.
        reap_expired_takeover(dir.path(), "viewport", st.expires_at_ms);
        assert_restored(&mut cdp);
        assert!(
            set_takeover_viewport(dir.path(), "viewport", &serde_json::json!({"enabled":true}))
                .is_err(),
            "late toggle after reap must be refused"
        );
        complete_takeover_session(dir.path(), "viewport", None);
        assert_restored(&mut cdp);

        begin();
        enable();
        let st = load_takeover_state(dir.path(), "viewport").unwrap();
        let heartbeat = st.viewport_heartbeat_ms.unwrap();
        reap_disconnected_takeover_viewport(dir.path(), "viewport", heartbeat + 10_000);
        assert!(
            load_takeover_state(dir.path(), "viewport")
                .unwrap()
                .original_viewport
                .is_some(),
            "a live heartbeat must retain phone layout through the lease boundary"
        );
        reap_expired_takeover(dir.path(), "viewport", heartbeat + 10_001);
        assert!(is_human_takeover_active(
            dir.path(),
            "viewport",
            heartbeat + 10_001
        ));
        reap_disconnected_takeover_viewport(dir.path(), "viewport", heartbeat + 10_001);
        assert_restored(&mut cdp);
        assert!(
            !load_takeover_state(dir.path(), "viewport")
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
    #[ignore = "Chromium startup and OOPIF click/focus races; see docs/verification.md; fix owner: lyra-anyagents-takeover-test-race after #91"]
    fn takeover_focus_finds_cross_origin_input() {
        if !Path::new(&chromium_binary()).exists() {
            return;
        }
        let events = r#"<script>
            window.events=[];
            for (const type of ['pointerdown','mousedown','mouseup','click','focus','blur','focusout'])
                document.addEventListener(type,e=>{
                    window.events.push({type,target:e.target.id||e.target.tagName,
                        related:e.relatedTarget?.outerHTML||null,active:document.activeElement?.outerHTML,
                        focused:document.hasFocus(),stack:new Error('event '+type).stack,
                        trusted:e.isTrusted,x:e.clientX,y:e.clientY,at:performance.now()});
                    if(type==='blur') debugger;
                },true);
        </script>"#;
        let child = spawn_html_server(
            "127.0.0.2",
            format!(
                "<style>body{{margin:0}}input{{width:240px;height:44px}}</style><input id='entry'>{events}"
            ),
        );
        let parent = spawn_html_server(
            "127.0.0.1",
            format!(
                "<style>body{{margin:0}}iframe{{position:absolute;left:60px;top:80px;border:0;width:400px;height:200px}}</style><iframe src='http://127.0.0.2:{child}'></iframe>{events}"
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
        let mut rt = load_runtime(dir.path(), "frame-focus").unwrap();
        let target = select_page_target(dir.path(), &mut rt, None).unwrap();
        let parent_ws = target.ws_url.unwrap();
        let mut parent_cdp = CdpSession::connect(rt.cdp_port, &parent_ws).unwrap();
        let frame = discover_child_frames(&mut parent_cdp).remove(0);
        let mut child_cdp = CdpSession::connect(rt.cdp_port, &format!("/devtools/page/{}",frame.frame_id)).unwrap();
        child_cdp.call("Debugger.enable", serde_json::json!({})).unwrap();
        child_cdp.stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let observer = std::thread::spawn(move || {
            while let Ok(text) = child_cdp.read_text_message() {
                let value:serde_json::Value = serde_json::from_str(&text).unwrap();
                if value["method"] == "Debugger.paused" {
                    eprintln!("BLUR PAUSED: {}",value);
                    let mut parent = CdpSession::connect(rt.cdp_port, &parent_ws).unwrap();
                    eprintln!("PARENT AT BLUR: {:?}",eval_js(&mut parent,"({active:document.activeElement?.outerHTML,focused:document.hasFocus()})"));
                    eprintln!("CHILD AT BLUR: {:?}",eval_js(&mut child_cdp,"window.events"));
                    child_cdp.call("Debugger.resume",serde_json::json!({})).unwrap();
                    break;
                }
            }
        });
        let response = dispatch_takeover_input(
            dir.path(),
            "frame-focus",
            &serde_json::json!({"type":"click","x":100,"y":100}),
        )
        .unwrap();
        observer.join().unwrap();
        if response["focus"]["editable"] != true {
            eprintln!("FIRST CLICK: {response}");
            let mut rt = load_runtime(dir.path(), "frame-focus").unwrap();
            let target = select_page_target(dir.path(), &mut rt, None).unwrap();
            let mut cdp = CdpSession::connect(rt.cdp_port, &target.ws_url.unwrap()).unwrap();
            let diag = "({url:location.href,ready:document.readyState,focus:document.hasFocus(),active:document.activeElement?.outerHTML,hit:document.elementFromPoint(40,20)?.outerHTML,events:window.events})";
            eprintln!("PARENT: {:?}", eval_js(&mut cdp, diag));
            for frame in discover_child_frames(&mut cdp) {
                eprintln!(
                    "CHILD {frame:?}: {:?}",
                    eval_js_in_frame(&mut cdp, Some(&frame.frame_id), diag)
                );
            }
        }
        let _ = stop_profile(dir.path(), "frame-focus");
        assert_eq!(response["focus"]["editable"], true, "{response}");
        assert_eq!(response["focus"]["rect"]["x"], 60.0);
        assert_eq!(response["focus"]["rect"]["y"], 80.0);
    }
}
