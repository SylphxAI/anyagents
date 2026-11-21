//! Unprivileged background daemon installer for `anyagents node install/uninstall`.
//!
//! Generates and installs user-level daemon services WITHOUT root:
//! - macOS: `~/Library/LaunchAgents/dev.anyagents.node.<name>.plist`
//! - Linux: `~/.config/systemd/user/anyagents-node-<name>.service`

use std::path::{Path, PathBuf};

use anyagents_core::Error;

/// Sanitize a node name for use in service identifiers (alphanumeric + hyphens).
fn sanitize_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_ascii_lowercase()
}

/// Generate macOS launchd plist XML for a user-level daemon.
pub(crate) fn render_launchd_plist(
    name: &str,
    binary_path: &Path,
    config_path: &Path,
    log_dir: &Path,
) -> String {
    let safe_name = sanitize_name(name);
    let label = format!("dev.anyagents.node.{safe_name}");
    let bin = escape_xml(&binary_path.to_string_lossy());
    let cfg = escape_xml(&config_path.to_string_lossy());
    let stdout_log = escape_xml(
        &log_dir
            .join(format!("{safe_name}.out.log"))
            .to_string_lossy(),
    );
    let stderr_log = escape_xml(
        &log_dir
            .join(format!("{safe_name}.err.log"))
            .to_string_lossy(),
    );

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{label}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{bin}</string>
        <string>node</string>
        <string>--config</string>
        <string>{cfg}</string>
        <string>run</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{stdout_log}</string>
    <key>StandardErrorPath</key>
    <string>{stderr_log}</string>
    <key>ThrottleInterval</key>
    <integer>5</integer>
</dict>
</plist>
"#
    )
}

/// Generate Linux systemd user unit file content.
pub(crate) fn render_systemd_unit(name: &str, binary_path: &Path, config_path: &Path) -> String {
    let safe_name = sanitize_name(name);
    let bin = binary_path.to_string_lossy();
    let cfg = config_path.to_string_lossy();

    format!(
        r#"[Unit]
Description=anyagents node daemon ({safe_name})
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart={bin} node --config {cfg} run
Restart=on-failure
RestartSec=5
Environment=RUST_LOG=info

[Install]
WantedBy=default.target
"#
    )
}

fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn resolve_binary_path() -> Result<PathBuf, Error> {
    std::env::current_exe()
        .map_err(|err| Error::Message(format!("cannot determine binary path: {err}")))
}

fn resolve_log_dir() -> Result<PathBuf, Error> {
    if let Ok(state) = std::env::var("XDG_STATE_HOME")
        && !state.trim().is_empty()
    {
        return Ok(PathBuf::from(state.trim()).join("anyagents").join("logs"));
    }
    if let Ok(home) = std::env::var("HOME")
        && !home.trim().is_empty()
    {
        return Ok(PathBuf::from(home.trim())
            .join(".local")
            .join("state")
            .join("anyagents")
            .join("logs"));
    }
    Err(Error::Message(
        "cannot determine log directory: set HOME or XDG_STATE_HOME".into(),
    ))
}

/// Install a user-level daemon service. Returns the installed unit/plist path.
pub(crate) fn install_daemon(
    name: &str,
    config_path: &Path,
    dry_run: bool,
) -> Result<PathBuf, Error> {
    let safe_name = sanitize_name(name);
    let binary_path = resolve_binary_path()?;

    if cfg!(target_os = "macos") {
        install_launchd(&safe_name, &binary_path, config_path, dry_run)
    } else {
        install_systemd(&safe_name, &binary_path, config_path, dry_run)
    }
}

fn install_launchd(
    safe_name: &str,
    binary_path: &Path,
    config_path: &Path,
    dry_run: bool,
) -> Result<PathBuf, Error> {
    let home = std::env::var("HOME").map_err(|_| Error::Message("HOME not set".into()))?;
    let agents_dir = PathBuf::from(&home).join("Library").join("LaunchAgents");
    let plist_name = format!("dev.anyagents.node.{safe_name}.plist");
    let plist_path = agents_dir.join(&plist_name);
    let log_dir = resolve_log_dir()?;
    let content = render_launchd_plist(safe_name, binary_path, config_path, &log_dir);

    if dry_run {
        println!("# Would write to {}", plist_path.display());
        println!("{content}");
        return Ok(plist_path);
    }

    std::fs::create_dir_all(&agents_dir)?;
    std::fs::create_dir_all(&log_dir)?;
    std::fs::write(&plist_path, &content)?;

    let label = format!("dev.anyagents.node.{safe_name}");
    // Try to unload first (ignore errors if not loaded).
    let _ = std::process::Command::new("launchctl")
        .args([
            "bootout",
            &format!("gui/{}", unsafe { libc::getuid() }),
            &label,
        ])
        .output();
    let status = std::process::Command::new("launchctl")
        .args([
            "bootstrap",
            &format!("gui/{}", unsafe { libc::getuid() }),
            &plist_path.to_string_lossy(),
        ])
        .status()
        .map_err(|err| Error::Message(format!("launchctl bootstrap failed: {err}")))?;
    if !status.success() {
        return Err(Error::Message(format!(
            "launchctl bootstrap exited with status {status}"
        )));
    }
    Ok(plist_path)
}

fn install_systemd(
    safe_name: &str,
    binary_path: &Path,
    config_path: &Path,
    dry_run: bool,
) -> Result<PathBuf, Error> {
    let config_dir = if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME")
        && !xdg.trim().is_empty()
    {
        PathBuf::from(xdg.trim())
    } else if let Ok(home) = std::env::var("HOME")
        && !home.trim().is_empty()
    {
        PathBuf::from(home.trim()).join(".config")
    } else {
        return Err(Error::Message("HOME not set".into()));
    };

    let unit_dir = config_dir.join("systemd").join("user");
    let unit_name = format!("anyagents-node-{safe_name}.service");
    let unit_path = unit_dir.join(&unit_name);
    let content = render_systemd_unit(safe_name, binary_path, config_path);

    if dry_run {
        println!("# Would write to {}", unit_path.display());
        println!("{content}");
        return Ok(unit_path);
    }

    std::fs::create_dir_all(&unit_dir)?;
    std::fs::write(&unit_path, &content)?;

    let _ = std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .status();
    let _ = std::process::Command::new("systemctl")
        .args(["--user", "enable", "--now", &unit_name])
        .status();
    Ok(unit_path)
}

/// Uninstall a user-level daemon service.
pub(crate) fn uninstall_daemon(name: &str) -> Result<(), Error> {
    let safe_name = sanitize_name(name);
    if cfg!(target_os = "macos") {
        uninstall_launchd(&safe_name)
    } else {
        uninstall_systemd(&safe_name)
    }
}

fn uninstall_launchd(safe_name: &str) -> Result<(), Error> {
    let label = format!("dev.anyagents.node.{safe_name}");
    let _ = std::process::Command::new("launchctl")
        .args([
            "bootout",
            &format!("gui/{}", unsafe { libc::getuid() }),
            &label,
        ])
        .output();
    let home = std::env::var("HOME").map_err(|_| Error::Message("HOME not set".into()))?;
    let plist_path = PathBuf::from(&home)
        .join("Library")
        .join("LaunchAgents")
        .join(format!("dev.anyagents.node.{safe_name}.plist"));
    if plist_path.exists() {
        std::fs::remove_file(&plist_path)?;
    }
    Ok(())
}

fn uninstall_systemd(safe_name: &str) -> Result<(), Error> {
    let unit_name = format!("anyagents-node-{safe_name}.service");
    let _ = std::process::Command::new("systemctl")
        .args(["--user", "stop", &unit_name])
        .status();
    let _ = std::process::Command::new("systemctl")
        .args(["--user", "disable", &unit_name])
        .status();

    let config_dir = if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME")
        && !xdg.trim().is_empty()
    {
        PathBuf::from(xdg.trim())
    } else if let Ok(home) = std::env::var("HOME")
        && !home.trim().is_empty()
    {
        PathBuf::from(home.trim()).join(".config")
    } else {
        return Ok(());
    };

    let unit_path = config_dir
        .join("systemd")
        .join("user")
        .join(format!("anyagents-node-{safe_name}.service"));
    if unit_path.exists() {
        std::fs::remove_file(&unit_path)?;
    }
    let _ = std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .status();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plist_xml_is_valid_and_deterministic() {
        let plist = render_launchd_plist(
            "my-laptop",
            Path::new("/usr/local/bin/anyagents"),
            Path::new("/Users/test/.config/anyagents/node.json"),
            Path::new("/Users/test/.local/state/anyagents/logs"),
        );
        assert!(plist.contains("<!DOCTYPE plist"));
        assert!(plist.contains("<key>Label</key>"));
        assert!(plist.contains("dev.anyagents.node.my-laptop"));
        assert!(plist.contains("<key>RunAtLoad</key>"));
        assert!(plist.contains("<true/>"));
        assert!(plist.contains("<key>KeepAlive</key>"));
        assert!(plist.contains("/usr/local/bin/anyagents"));
        assert!(plist.contains("node.json"));
        assert!(plist.contains("<key>ThrottleInterval</key>"));
        assert!(plist.contains("<integer>5</integer>"));
        // Verify no unescaped special characters in paths.
        let plist2 = render_launchd_plist(
            "test & <node>",
            Path::new("/path/with spaces/bin"),
            Path::new("/cfg"),
            Path::new("/logs"),
        );
        assert!(plist2.contains("dev.anyagents.node.test---node-"));
        assert!(!plist2.contains("& "));
        assert!(!plist2.contains(" <"));
    }

    #[test]
    fn systemd_unit_is_valid_and_deterministic() {
        let unit = render_systemd_unit(
            "edge-box",
            Path::new("/usr/local/bin/anyagents"),
            Path::new("/home/user/.config/anyagents/node.json"),
        );
        assert!(unit.contains("[Unit]"));
        assert!(unit.contains("[Service]"));
        assert!(unit.contains("[Install]"));
        assert!(unit.contains("anyagents node daemon (edge-box)"));
        assert!(unit.contains("Restart=on-failure"));
        assert!(unit.contains("RestartSec=5"));
        assert!(unit.contains("WantedBy=default.target"));
        assert!(unit.contains("/usr/local/bin/anyagents node --config"));
        assert!(unit.contains("node.json run"));
    }

    #[test]
    fn sanitize_name_handles_edge_cases() {
        assert_eq!(sanitize_name("my-laptop"), "my-laptop");
        assert_eq!(sanitize_name("My Laptop!"), "my-laptop-");
        assert_eq!(sanitize_name("test.node_1"), "test-node-1");
        assert_eq!(sanitize_name("---"), "");
    }

    #[test]
    fn plist_on_linux_is_testable_deterministically() {
        // Verify the render path works on Linux even though launchd won't run.
        let plist = render_launchd_plist(
            "linux-test",
            Path::new("/usr/bin/anyagents"),
            Path::new("/etc/anyagents/node.json"),
            Path::new("/var/log/anyagents"),
        );
        assert!(plist.contains("dev.anyagents.node.linux-test"));
        assert!(plist.contains("/usr/bin/anyagents"));
    }
}
