//! Configuration for the standalone `anyagents-mcp-browser` stdio MCP server.

use std::path::PathBuf;

use clap::Parser;

/// Default browser viewport width in CSS pixels.
pub const DEFAULT_VIEWPORT_WIDTH: u32 = 1280;
/// Default browser viewport height in CSS pixels.
pub const DEFAULT_VIEWPORT_HEIGHT: u32 = 800;

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
