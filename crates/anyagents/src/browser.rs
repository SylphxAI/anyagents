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
    /// `"press"` (press keyboard `key` such as `"Enter"`, `"Tab"`, `"Escape"`),
    /// `"eval"` (evaluate JavaScript `script` / `expression` in the page),
    /// `"screenshot"` (save PNG screenshot to `path` or profile screenshots dir),
    /// `"cookies"` (list cookies, or set cookies when `cookies` JSON array is provided),
    /// `"tabs"` (list tabs, or open/switch/close via `tab_action` and `target_id`),
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
    /// Text to enter for `"fill"` / `"type"`.
    #[serde(default, alias = "value", skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Keyboard key name for `"press"` (e.g. `"Enter"`, `"Tab"`, `"Escape"`, `"PageDown"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
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
    for candidate in [
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/google-chrome",
    ] {
        if Path::new(candidate).is_file() {
            return candidate.to_string();
        }
    }
    "chromium".to_string()
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
    if let Ok(bytes) = std::fs::read(&rt_path)
        && let Ok(rt) = serde_json::from_slice::<BrowserRuntimeState>(&bytes)
    {
        stopped_pid = Some(rt.pid);
        stop_process_group(rt.pid);
    }
    let _ = std::fs::remove_file(&rt_path);
    cleanup_singleton_locks(&dir.join("user-data"));
    Ok(serde_json::json!({
        "profile": profile,
        "status": "stopped",
        "stopped_pid": stopped_pid,
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
        let id = self.next_id;
        self.next_id += 1;
        let payload = serde_json::json!({
            "id": id,
            "method": method,
            "params": params,
        })
        .to_string();
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
    let res = cdp.call(
        "Runtime.evaluate",
        serde_json::json!({
            "expression": expression,
            "awaitPromise": true,
            "returnByValue": true,
        }),
    )?;
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

const SNAPSHOT_JS: &str = r#"(() => {
  const isVisible = (el) => {
    const rect = el.getBoundingClientRect();
    if (rect.width < 2 || rect.height < 2) return false;
    const style = window.getComputedStyle(el);
    if (style.visibility === 'hidden' || style.display === 'none' || parseFloat(style.opacity || '1') === 0) return false;
    return true;
  };
  const cssPath = (el) => {
    if (el.id && /^[a-zA-Z][\w-]*$/.test(el.id)) return '#' + el.id;
    const parts = [];
    let cur = el;
    while (cur && cur.nodeType === 1 && parts.length < 5) {
      let part = cur.nodeName.toLowerCase();
      if (cur.id && /^[a-zA-Z][\w-]*$/.test(cur.id)) {
        parts.unshift('#' + cur.id);
        break;
      }
      let sib = cur, nth = 1;
      while ((sib = sib.previousElementSibling)) {
        if (sib.nodeName === cur.nodeName) nth++;
      }
      if (nth > 1) part += ':nth-of-type(' + nth + ')';
      parts.unshift(part);
      cur = cur.parentElement;
    }
    return parts.join(' > ');
  };
  const candidates = Array.from(document.querySelectorAll(
    'a[href], button, input, textarea, select, summary, [role="button"], [role="link"], [role="tab"], [role="menuitem"], [role="checkbox"], [role="switch"], [contenteditable="true"]'
  ));
  const elements = [];
  let idx = 1;
  for (const el of candidates) {
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
    elements.push({
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
    });
  }
  const bodyText = (document.body ? document.body.innerText : '')
    .replace(/\n{3,}/g, '\n\n')
    .trim()
    .slice(0, 12000);
  return {
    url: location.href,
    title: document.title || '',
    text: bodyText,
    elements: elements.map(e => ({
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
    })),
  };
})()"#;

fn capture_snapshot(
    data: &Path,
    profile: &str,
    cdp: &mut CdpSession,
) -> Result<serde_json::Value, Error> {
    let val = eval_js(cdp, SNAPSHOT_JS)?;
    if let Ok(elements) = serde_json::from_value::<Vec<InteractiveRef>>(
        val.get("elements").cloned().unwrap_or_default(),
    ) {
        let stored = StoredRefs {
            url: val
                .get("url")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            captured_at_ms: now_ms(),
            elements,
        };
        if let Ok(bytes) = serde_json::to_vec_pretty(&stored) {
            let _ = std::fs::write(profile_dir(data, profile).join("refs.json"), bytes);
        }
    }
    Ok(val)
}

fn resolve_selector(data: &Path, profile: &str, raw_target: &str) -> (String, Option<(f64, f64)>) {
    let trimmed = raw_target.trim();
    if trimmed.starts_with("@e") {
        let refs_path = profile_dir(data, profile).join("refs.json");
        if let Ok(bytes) = std::fs::read(&refs_path)
            && let Ok(stored) = serde_json::from_slice::<StoredRefs>(&bytes)
            && let Some(found) = stored.elements.iter().find(|e| e.r#ref == trimmed)
        {
            return (
                format!("[data-anyagents-ref=\"{}\"], {}", trimmed, found.selector),
                Some((found.x, found.y)),
            );
        }
        return (format!("[data-anyagents-ref=\"{trimmed}\"]"), None);
    }
    (trimmed.to_string(), None)
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
            if let Some(rt) = load_runtime(data, &profile) {
                let pages = list_page_targets(rt.cdp_port).unwrap_or_default();
                return Ok(serde_json::json!({
                    "profile": profile,
                    "status": "running",
                    "runtime": rt,
                    "fingerprint": fp,
                    "tabs": pages,
                }));
            }
            return Ok(serde_json::json!({
                "profile": profile,
                "status": "stopped",
                "fingerprint": fp,
            }));
        }
        _ => {}
    }

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
            Ok(serde_json::json!({
                "profile": profile,
                "status": "running",
                "display_mode": rt.display_mode,
                "url": snap.get("url").cloned().unwrap_or_else(|| serde_json::json!(url)),
                "title": snap.get("title").cloned().unwrap_or_default(),
                "text_preview": snap.get("text").and_then(|v| v.as_str()).map(|s| s.chars().take(1500).collect::<String>()).unwrap_or_default(),
                "interactive_elements": snap.get("elements").cloned().unwrap_or_default(),
            }))
        }
        "snapshot" | "read" | "dom" => {
            let snap = capture_snapshot(data, &profile, &mut cdp)?;
            Ok(serde_json::json!({
                "profile": profile,
                "url": snap.get("url").cloned().unwrap_or_default(),
                "title": snap.get("title").cloned().unwrap_or_default(),
                "text": snap.get("text").cloned().unwrap_or_default(),
                "elements": snap.get("elements").cloned().unwrap_or_default(),
            }))
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
            let (css_sel, fallback_xy) = resolve_selector(data, &profile, raw_sel);
            let sel_json = serde_json::to_string(&css_sel).unwrap_or_else(|_| "\"body\"".into());
            let js = format!(
                r#"(() => {{
                  const el = document.querySelector({sel_json});
                  if (!el) return {{ found: false }};
                  el.scrollIntoView({{ block: 'center', inline: 'center' }});
                  const r = el.getBoundingClientRect();
                  el.focus && el.focus();
                  el.click && el.click();
                  return {{
                    found: true,
                    tag: el.tagName.toLowerCase(),
                    text: (el.innerText || el.value || '').trim().slice(0, 80),
                    x: Math.round(r.left + r.width / 2),
                    y: Math.round(r.top + r.height / 2)
                  }};
                }})()"#
            );
            let clicked = eval_js(&mut cdp, &js)?;
            if clicked.get("found").and_then(|v| v.as_bool()) != Some(true) {
                if let Some((x, y)) = fallback_xy {
                    for ev_type in ["mouseMoved", "mousePressed", "mouseReleased"] {
                        let _ = cdp.call(
                            "Input.dispatchMouseEvent",
                            serde_json::json!({
                                "type": ev_type,
                                "x": x,
                                "y": y,
                                "button": "left",
                                "clickCount": 1,
                            }),
                        );
                    }
                } else {
                    return Err(Error::Message(format!(
                        "element `{raw_sel}` not found on page; run `snapshot` first to refresh @e refs"
                    )));
                }
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
                "detail": clicked,
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
            let text_val = req.text.as_deref().unwrap_or("");
            let (css_sel, _) = resolve_selector(data, &profile, raw_sel);
            let sel_json = serde_json::to_string(&css_sel).unwrap_or_else(|_| "\"input\"".into());
            let val_json = serde_json::to_string(text_val).unwrap_or_else(|_| "\"\"".into());
            let submit = req.submit.unwrap_or(false);
            let js = format!(
                r#"(() => {{
                  const el = document.querySelector({sel_json});
                  if (!el) return {{ found: false }};
                  el.scrollIntoView({{ block: 'center', inline: 'center' }});
                  el.focus();
                  const proto = Object.getPrototypeOf(el);
                  const desc = Object.getOwnPropertyDescriptor(proto, 'value') || Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value');
                  if (desc && desc.set) {{
                    desc.set.call(el, {val_json});
                  }} else {{
                    el.value = {val_json};
                  }}
                  el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                  el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                  if ({submit} && el.form) {{
                    el.form.requestSubmit ? el.form.requestSubmit() : el.form.submit();
                  }}
                  return {{ found: true, tag: el.tagName.toLowerCase() }};
                }})()"#
            );
            let filled = eval_js(&mut cdp, &js)?;
            if filled.get("found").and_then(|v| v.as_bool()) != Some(true) {
                return Err(Error::Message(format!(
                    "input element `{raw_sel}` not found on page; run `snapshot` first"
                )));
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
            }
            Ok(serde_json::json!({
                "profile": profile,
                "filled": raw_sel,
                "submitted": submit,
            }))
        }
        "press" | "key" => {
            let key_name = req
                .key
                .as_deref()
                .or(req.text.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    Error::Message(
                        "browser press requires `key` (e.g. `Enter`, `Tab`, `Escape`)".into(),
                    )
                })?;
            let vk = match key_name {
                "Enter" => 13,
                "Tab" => 9,
                "Escape" => 27,
                "Backspace" => 8,
                "ArrowDown" => 40,
                "ArrowUp" => 38,
                "ArrowLeft" => 37,
                "ArrowRight" => 39,
                "PageDown" => 34,
                "PageUp" => 33,
                "Space" | " " => 32,
                _ => 0,
            };
            cdp.call(
                "Input.dispatchKeyEvent",
                serde_json::json!({
                    "type": "keyDown",
                    "key": key_name,
                    "code": key_name,
                    "windowsVirtualKeyCode": vk,
                    "nativeVirtualKeyCode": vk,
                }),
            )?;
            cdp.call(
                "Input.dispatchKeyEvent",
                serde_json::json!({
                    "type": "keyUp",
                    "key": key_name,
                    "code": key_name,
                    "windowsVirtualKeyCode": vk,
                    "nativeVirtualKeyCode": vk,
                }),
            )?;
            Ok(serde_json::json!({
                "profile": profile,
                "pressed": key_name,
            }))
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
            let out_path = match req.path.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
                Some(p) => PathBuf::from(p),
                None => {
                    let shots_dir = profile_dir(data, &profile).join("screenshots");
                    std::fs::create_dir_all(&shots_dir)?;
                    shots_dir.join(format!("shot-{}.png", now_ms()))
                }
            };
            if let Some(parent) = out_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&out_path, &png_bytes)?;
            Ok(serde_json::json!({
                "profile": profile,
                "path": out_path.display().to_string(),
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
        other => Err(Error::Message(format!(
            "unknown browser action `{other}`; expected `open`, `snapshot`, `click`, `fill`, `press`, `eval`, `screenshot`, `cookies`, `tabs`, `status`, `profiles`, or `close`"
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
