//! Real headful browser hosted in a private `sway` Wayland session.
//!
//! `--browser-mode desktop` (and `auto` when the host supports it) runs the
//! browser headfully inside its own headless-backend `sway` compositor through
//! `agent-workstation`: no `--headless` and no `--disable-gpu`. The same session
//! serves interactive takeover (`wayvnc` + noVNC), so `browser_takeover` hands
//! the live window to a person without restarting the browser.
//!
//! External binaries are resolved from explicit system directories without
//! ambient `$PATH` lookup (ADR 0005).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use agent_workstation::{
    BrowserConfig, DesktopSession, FingerprintDefaults, Profile, SessionConfig,
};

use crate::chromium::is_headless_shell_binary;
use crate::config::{BrowserMode, BrowserServerConfig};
use crate::proxy::{BrowserProxy, ProxyHandle};
use crate::session::{
    BrowserBackend, BrowserError, BrowserInstance, DESKTOP_PRINCIPAL, prepare_profile_workspace,
};
use crate::takeover::{TakeoverState, find_binary_in};

const BIN_DIRS: &[&str] = &[
    "/usr/local/bin",
    "/usr/bin",
    "/opt/homebrew/bin",
    "/bin",
    "/snap/bin",
];

/// System directories searched for `sway` / `wayvnc`.
pub fn default_bin_dirs() -> Vec<PathBuf> {
    BIN_DIRS.iter().map(PathBuf::from).collect()
}

/// Wayland desktop binaries available on this host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DesktopTools {
    /// `sway` compositor (required).
    pub compositor: PathBuf,
    /// `wayvnc` server; when present the session also serves takeover.
    pub vnc: Option<PathBuf>,
}

/// Find `sway` (required) and `wayvnc` (optional) in `dirs`. Always `None`
/// off Linux, where the private Wayland session is unsupported.
pub fn find_desktop_tools_in(dirs: &[PathBuf]) -> Option<DesktopTools> {
    if !cfg!(target_os = "linux") {
        return None;
    }
    Some(DesktopTools {
        compositor: find_binary_in("sway", dirs)?,
        vnc: find_binary_in("wayvnc", dirs),
    })
}

/// How the next browser process will be hosted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LaunchPlan {
    /// Headful browser inside a private `sway` session.
    Desktop(DesktopTools),
    /// Browser launched directly; `fallback_reason` explains why `auto` did not
    /// pick the desktop session.
    Direct { fallback_reason: Option<String> },
}

/// Pure launch decision for `mode` given the discovered browser and desktop tools.
pub fn plan_launch(
    mode: BrowserMode,
    headful: bool,
    binary: &Path,
    tools: Option<DesktopTools>,
) -> Result<LaunchPlan, BrowserError> {
    let headless_shell = is_headless_shell_binary(binary);
    match mode {
        BrowserMode::Headless => Ok(LaunchPlan::Direct {
            fallback_reason: None,
        }),
        BrowserMode::Desktop => {
            if headless_shell {
                return Err(BrowserError::new(
                    "DESKTOP_UNAVAILABLE",
                    format!(
                        "`--browser-mode desktop` needs a full browser, but `{}` is a headless shell",
                        binary.display()
                    ),
                ));
            }
            tools.map(LaunchPlan::Desktop).ok_or_else(|| {
                BrowserError::new(
                    "DESKTOP_UNAVAILABLE",
                    format!(
                        "`--browser-mode desktop` requires Linux with `sway` installed in one of: {}",
                        BIN_DIRS.join(" ")
                    ),
                )
            })
        }
        BrowserMode::Auto => {
            let reason = if headful {
                Some("`--headful` requested a window on the host display".to_string())
            } else if headless_shell {
                Some(format!("`{}` is a headless shell", binary.display()))
            } else if tools.is_none() {
                Some("`sway` is not installed (Linux only)".to_string())
            } else {
                None
            };
            Ok(match (reason, tools) {
                (None, Some(t)) => LaunchPlan::Desktop(t),
                (reason, _) => LaunchPlan::Direct {
                    fallback_reason: reason,
                },
            })
        }
    }
}

/// Browser flags appended to `agent-workstation`'s Wayland launch flags (which
/// already include `--password-store=basic`, `--lang` and `--accept-lang`).
pub fn desktop_browser_args(
    config: &BrowserServerConfig,
    proxy_args: Option<&[String]>,
) -> Vec<String> {
    let mut args: Vec<String> = [
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--hide-crash-restore-bubble",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    if let Some(p) = proxy_args {
        args.extend(p.iter().cloned());
    }
    args.extend(config.parsed_extra_args());
    args
}

/// Apply the configured timezone and languages to a new profile's identity.
pub fn apply_locale_defaults(defaults: &mut FingerprintDefaults, config: &BrowserServerConfig) {
    if let Some(tz) = config.resolved_timezone() {
        defaults.timezone = tz;
    }
    let languages = config.languages();
    if !languages.is_empty() {
        defaults.languages = languages;
    }
}

/// Start a private `sway` session with a headful browser on the requested
/// profile (synced down first) and browser-only proxy.
pub(crate) async fn launch_desktop_instance(
    config: &BrowserServerConfig,
    requested_profile: Option<&str>,
    proxy: Option<BrowserProxy>,
    binary: PathBuf,
    tools: &DesktopTools,
) -> Result<BrowserInstance, BrowserError> {
    let ws = prepare_profile_workspace(config, requested_profile).await?;
    let proxy_handle = match &proxy {
        Some(p) => Some(ProxyHandle::start(p).await?),
        None => None,
    };
    let runtime_dir = tempfile::Builder::new()
        .prefix("aa-desk-")
        .tempdir()
        .map_err(|e| BrowserError::new("BROWSER_PROCESS_ERROR", format!("runtime: {e}")))?;
    let mut browser = BrowserConfig::new(binary);
    let proxy_args = proxy_handle.as_ref().map(ProxyHandle::chromium_args);
    browser.extra_args = desktop_browser_args(config, proxy_args.as_ref().map(AsRef::as_ref));
    let root = ws.user_data_dir.parent().unwrap_or(&ws.user_data_dir);
    let profile = Profile::new(ws.active_profile.as_deref().unwrap_or("default"), root);
    let mut cfg = SessionConfig::new(profile, browser, runtime_dir.path());
    apply_locale_defaults(&mut cfg.fingerprint_defaults, config);
    cfg.human_idle_timeout = None;
    cfg.compositor = tools.compositor.clone();
    cfg.takeover = None;
    if let Some(vnc) = &tools.vnc {
        cfg.vnc = vnc.clone();
    }
    let session = Arc::new(DesktopSession::start(cfg).await?);
    let _ = session.lease(DESKTOP_PRINCIPAL)?;
    Ok(BrowserInstance {
        backend: BrowserBackend::Desktop {
            session,
            _runtime_dir: runtime_dir,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn tools() -> DesktopTools {
        DesktopTools {
            compositor: PathBuf::from("/usr/bin/sway"),
            vnc: Some(PathBuf::from("/usr/bin/wayvnc")),
        }
    }

    #[test]
    fn auto_prefers_desktop_only_with_sway_and_a_full_browser() {
        let chrome = Path::new("/usr/bin/google-chrome-stable");
        let shell = Path::new("/opt/pw/chromium_headless_shell-1/chrome-linux/headless_shell");
        let auto = |headful, bin, t| plan_launch(BrowserMode::Auto, headful, bin, t).unwrap();

        assert_eq!(
            auto(false, chrome, Some(tools())),
            LaunchPlan::Desktop(tools())
        );
        for (plan, why) in [
            (auto(false, chrome, None), "`sway` is not installed"),
            (auto(false, shell, Some(tools())), "headless shell"),
            (auto(true, chrome, Some(tools())), "--headful"),
        ] {
            let LaunchPlan::Direct {
                fallback_reason: Some(reason),
            } = plan
            else {
                panic!("expected direct fallback for {why}");
            };
            assert!(reason.contains(why), "{reason}");
        }
    }

    #[test]
    fn explicit_modes_never_silently_switch_backends() {
        let chrome = Path::new("/usr/bin/google-chrome-stable");
        assert_eq!(
            plan_launch(BrowserMode::Headless, false, chrome, Some(tools())).unwrap(),
            LaunchPlan::Direct {
                fallback_reason: None
            }
        );
        assert_eq!(
            plan_launch(BrowserMode::Desktop, false, chrome, Some(tools())).unwrap(),
            LaunchPlan::Desktop(tools())
        );
        let missing = plan_launch(BrowserMode::Desktop, false, chrome, None).unwrap_err();
        assert_eq!(missing.code, "DESKTOP_UNAVAILABLE");
        assert!(missing.message.contains("sway"));
        let shell = Path::new("/opt/pw/chrome-headless-shell-linux64/chrome-headless-shell");
        let err = plan_launch(BrowserMode::Desktop, false, shell, Some(tools())).unwrap_err();
        assert_eq!(err.code, "DESKTOP_UNAVAILABLE");
    }

    #[test]
    fn desktop_args_are_headful_and_carry_proxy_and_extra_flags() {
        let cfg = BrowserServerConfig {
            extra_args: Some("--mute-audio".into()),
            ..Default::default()
        };
        let proxy = vec!["--proxy-server=http://127.0.0.1:9".to_string()];
        let args = desktop_browser_args(&cfg, Some(&proxy));
        assert!(!args.iter().any(|a| a.starts_with("--headless")));
        assert!(!args.iter().any(|a| a == "--disable-gpu"));
        assert!(args.contains(&"--proxy-server=http://127.0.0.1:9".to_string()));
        assert!(args.contains(&"--mute-audio".to_string()));
    }

    #[test]
    fn locale_defaults_follow_config() {
        let mut defaults = FingerprintDefaults::default();
        let cfg = BrowserServerConfig {
            timezone: Some("Europe/London".into()),
            locale: Some("en-GB,en".into()),
            ..Default::default()
        };
        apply_locale_defaults(&mut defaults, &cfg);
        assert_eq!(defaults.timezone, "Europe/London");
        assert_eq!(defaults.languages, vec!["en-GB", "en"]);
    }

    #[test]
    fn desktop_tools_require_sway_and_treat_wayvnc_as_optional() {
        let dir = tempfile::tempdir().unwrap();
        let dirs = [dir.path().to_path_buf()];
        assert_eq!(find_desktop_tools_in(&dirs), None);
        let exec = |name: &str| {
            let p = dir.path().join(name);
            std::fs::write(&p, b"#!/bin/sh\n").unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
            }
            p
        };
        let sway = exec("sway");
        if cfg!(target_os = "linux") {
            assert_eq!(
                find_desktop_tools_in(&dirs),
                Some(DesktopTools {
                    compositor: sway.clone(),
                    vnc: None
                })
            );
            let vnc = exec("wayvnc");
            assert_eq!(find_desktop_tools_in(&dirs).unwrap().vnc, Some(vnc));
        } else {
            assert_eq!(find_desktop_tools_in(&dirs), None);
        }
    }
}
