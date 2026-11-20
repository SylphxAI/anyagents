//! Configuration for the standalone `anyagents-mcp-browser` stdio MCP server.

use std::path::PathBuf;

use clap::{Parser, ValueEnum};

/// Default browser viewport width in CSS pixels.
pub const DEFAULT_VIEWPORT_WIDTH: u32 = 1280;
/// Default browser viewport height in CSS pixels.
pub const DEFAULT_VIEWPORT_HEIGHT: u32 = 800;

/// How the browser process is hosted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum BrowserMode {
    /// Real headful browser inside a private `sway` Wayland session when `sway`
    /// and a full (non `headless_shell`) browser are available; headless otherwise.
    #[default]
    Auto,
    /// Always use the private `sway` Wayland session; fail when it is unavailable.
    Desktop,
    /// Always launch the browser directly (`--headless=new` unless `--headful`).
    Headless,
}

impl BrowserMode {
    /// Stable lowercase name used in logs and tool payloads.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Desktop => "desktop",
            Self::Headless => "headless",
        }
    }
}

/// CLI arguments and environment configuration for `anyagents-mcp-browser`.
#[derive(Clone, Parser)]
#[command(
    name = "anyagents-mcp-browser",
    version,
    about = "Standalone stdio MCP server providing headless Chromium CDP browser automation"
)]
pub struct BrowserServerConfig {
    /// Explicit path to a Chromium, Google Chrome, or Playwright Chromium binary.
    #[arg(long, env = "ANYAGENTS_CHROMIUM_BIN")]
    pub chromium_bin: Option<PathBuf>,

    /// Optional persistent profile directory (cookies and storage survive restarts).
    /// When omitted, an ephemeral temporary directory is created per browser session.
    #[arg(long, env = "ANYAGENTS_BROWSER_PROFILE_DIR")]
    pub profile_dir: Option<PathBuf>,

    /// Optional named browser profile synced with the AnyAgents coordinator
    /// (`GET/PUT <ANYAGENTS_API_URL>/v1/browser-profiles/<profile>/archive`).
    #[arg(long, env = "ANYAGENTS_BROWSER_PROFILE")]
    pub profile: Option<String>,

    /// Optional coordinator HTTP API base URL used for remote persistent browser
    /// profile sync-down on open and sync-up on close.
    #[arg(long, env = "ANYAGENTS_API_URL")]
    pub api_url: Option<String>,

    /// Optional agent capability token presented as `x-anyagents-agent` when
    /// syncing named profiles with the coordinator (also checks `ANYAGENTS_CAPABILITY`
    /// and `ANYAGENTS_MCP_TOKEN_FILE`).
    #[arg(long, env = "ANYAGENTS_AGENT_TOKEN")]
    pub agent_token: Option<String>,

    /// Optional directory where `browser_screenshot` writes PNG files when no
    /// explicit absolute `path` is provided (defaults to `$ANYAGENTS_AGENT_DATA_DIR/tmp/screenshots`
    /// or `$ANYAGENTS_SANDBOX_DATA_DIR/tmp/screenshots`).
    #[arg(long, env = "ANYAGENTS_BROWSER_SCREENSHOT_DIR")]
    pub screenshot_dir: Option<PathBuf>,

    /// Optional proxy URL (e.g. `http://127.0.0.1:8080` or `socks5://127.0.0.1:1080`).
    #[arg(long, env = "ANYAGENTS_BROWSER_PROXY")]
    pub proxy: Option<String>,

    /// Run Chromium with a visible window instead of `--headless=new`.
    #[arg(long, env = "ANYAGENTS_BROWSER_HEADFUL")]
    pub headful: bool,

    /// Browser hosting mode: `auto` (default), `desktop`, or `headless`.
    #[arg(long, env = "ANYAGENTS_BROWSER_MODE", value_enum, default_value_t = BrowserMode::Auto)]
    pub browser_mode: BrowserMode,

    /// IANA timezone for the browser (e.g. `Europe/London`); defaults to `$TZ`, then `UTC`.
    #[arg(long, env = "ANYAGENTS_BROWSER_TIMEZONE")]
    pub timezone: Option<String>,

    /// Comma-separated BCP 47 browser languages, most preferred first (e.g. `en-GB,en`).
    #[arg(long, env = "ANYAGENTS_BROWSER_LOCALE")]
    pub locale: Option<String>,

    /// Extra whitespace-separated Chromium command-line flags.
    #[arg(long, env = "ANYAGENTS_BROWSER_ARGS")]
    pub extra_args: Option<String>,

    /// Initial viewport width in CSS pixels.
    #[arg(long, default_value_t = DEFAULT_VIEWPORT_WIDTH)]
    pub viewport_width: u32,

    /// Initial viewport height in CSS pixels.
    #[arg(long, default_value_t = DEFAULT_VIEWPORT_HEIGHT)]
    pub viewport_height: u32,
}

impl std::fmt::Debug for BrowserServerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrowserServerConfig")
            .field("chromium_bin", &self.chromium_bin)
            .field("profile_dir", &self.profile_dir)
            .field("profile", &self.profile)
            .field("api_url", &self.api_url)
            .field(
                "agent_token",
                &self.agent_token.as_ref().map(|_| "[redacted]"),
            )
            .field("screenshot_dir", &self.screenshot_dir)
            .field(
                "proxy",
                &self.proxy.as_deref().map(crate::proxy::redact_proxy_url),
            )
            .field("headful", &self.headful)
            .field("browser_mode", &self.browser_mode)
            .field("timezone", &self.timezone)
            .field("locale", &self.locale)
            .field("extra_args", &self.extra_args)
            .field("viewport_width", &self.viewport_width)
            .field("viewport_height", &self.viewport_height)
            .finish()
    }
}

impl Default for BrowserServerConfig {
    fn default() -> Self {
        Self {
            chromium_bin: None,
            profile_dir: None,
            profile: None,
            api_url: None,
            agent_token: None,
            screenshot_dir: None,
            proxy: None,
            headful: false,
            browser_mode: BrowserMode::Auto,
            timezone: None,
            locale: None,
            extra_args: None,
            viewport_width: DEFAULT_VIEWPORT_WIDTH,
            viewport_height: DEFAULT_VIEWPORT_HEIGHT,
        }
    }
}

impl BrowserServerConfig {
    /// Parse whitespace-separated extra Chromium flags.
    pub fn parsed_extra_args(&self) -> Vec<String> {
        self.extra_args
            .as_deref()
            .map(|raw| raw.split_whitespace().map(str::to_owned).collect())
            .unwrap_or_default()
    }

    /// Browser languages from `--locale`, ignoring empty entries.
    pub fn languages(&self) -> Vec<String> {
        self.locale
            .as_deref()
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Browser timezone from `--timezone`, else an IANA-looking `$TZ`.
    pub fn resolved_timezone(&self) -> Option<String> {
        resolve_timezone(
            self.timezone.as_deref(),
            std::env::var("TZ").ok().as_deref(),
        )
    }
}

/// Pure timezone resolution: an explicit value wins verbatim (validated later by
/// the browser launcher); `$TZ` is used only when it looks like an IANA name
/// (POSIX rule strings such as `EST5EDT,M3.2.0` or file paths are ignored).
pub fn resolve_timezone(explicit: Option<&str>, tz_env: Option<&str>) -> Option<String> {
    if let Some(tz) = explicit.map(str::trim).filter(|s| !s.is_empty()) {
        return Some(tz.to_string());
    }
    let tz = tz_env?.trim().trim_start_matches(':');
    let looks_iana = !tz.is_empty()
        && !tz.starts_with('/')
        && !tz.contains("..")
        && tz
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '_' | '-' | '+'))
        && (tz.contains('/') || tz == "UTC" || tz == "GMT");
    looks_iana.then(|| tz.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_uses_headless_standard_viewport() {
        let cfg = BrowserServerConfig::default();
        assert!(!cfg.headful);
        assert_eq!(cfg.viewport_width, 1280);
        assert_eq!(cfg.viewport_height, 800);
        assert!(cfg.chromium_bin.is_none());
        assert!(cfg.profile_dir.is_none());
        assert!(cfg.parsed_extra_args().is_empty());
        assert_eq!(cfg.browser_mode, BrowserMode::Auto);
    }

    #[test]
    fn browser_mode_timezone_and_locale_parse() {
        let cfg = BrowserServerConfig::try_parse_from([
            "anyagents-mcp-browser",
            "--browser-mode",
            "desktop",
            "--timezone",
            "Europe/London",
            "--locale",
            "en-GB, en ,",
        ])
        .unwrap();
        assert_eq!(cfg.browser_mode, BrowserMode::Desktop);
        assert_eq!(cfg.resolved_timezone().as_deref(), Some("Europe/London"));
        assert_eq!(cfg.languages(), vec!["en-GB", "en"]);
        assert!(BrowserServerConfig::try_parse_from(["x", "--browser-mode", "bogus"]).is_err());
        let headless =
            BrowserServerConfig::try_parse_from(["x", "--browser-mode", "headless"]).unwrap();
        assert_eq!(headless.browser_mode.as_str(), "headless");
    }

    #[test]
    fn timezone_falls_back_to_iana_tz_env_only() {
        assert_eq!(
            resolve_timezone(None, Some(":Europe/London")).as_deref(),
            Some("Europe/London")
        );
        assert_eq!(resolve_timezone(None, Some("UTC")).as_deref(), Some("UTC"));
        assert_eq!(resolve_timezone(None, Some("EST5EDT,M3.2.0,M11.1.0")), None);
        assert_eq!(
            resolve_timezone(None, Some("/usr/share/zoneinfo/UTC")),
            None
        );
        assert_eq!(resolve_timezone(None, None), None);
        assert_eq!(
            resolve_timezone(Some(" Asia/Tokyo "), Some("Europe/London")).as_deref(),
            Some("Asia/Tokyo")
        );
    }

    #[test]
    fn extra_args_split_on_whitespace() {
        let cfg = BrowserServerConfig {
            extra_args: Some("--disable-extensions   --mute-audio".into()),
            ..Default::default()
        };
        assert_eq!(
            cfg.parsed_extra_args(),
            vec!["--disable-extensions", "--mute-audio"]
        );
    }

    #[test]
    fn cli_parser_accepts_profile_dir_chromium_bin_and_headful() {
        let cfg = BrowserServerConfig::try_parse_from([
            "anyagents-mcp-browser",
            "--chromium-bin",
            "/usr/bin/chromium",
            "--profile-dir",
            "/tmp/profile",
            "--headful",
        ])
        .unwrap();
        assert_eq!(cfg.chromium_bin, Some(PathBuf::from("/usr/bin/chromium")));
        assert_eq!(cfg.profile_dir, Some(PathBuf::from("/tmp/profile")));
        assert!(cfg.headful);
    }

    #[test]
    fn debug_redacts_proxy_and_agent_token_credentials() {
        let cfg = BrowserServerConfig {
            agent_token: Some("tok-secret".into()),
            proxy: Some("http://user:pw-secret@proxy.example:8080".into()),
            ..Default::default()
        };
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("tok-secret"));
        assert!(!dbg.contains("pw-secret"));
        assert!(!dbg.contains("user:"));
        assert!(dbg.contains("proxy.example:8080"));
    }
}
