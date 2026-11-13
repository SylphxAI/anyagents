//! The agent's desktop session: one headless Wayland desktop per browser
//! profile, running one headful browser, from the `agent-workstation` crate.
//!
//! The server owns every session ([`session`]); `tools.browser` drives the
//! browser over the crate's web operations ([`browser`]) and session
//! `tools.computer` requests drive the same desktop's pixels ([`desktop`]).
//! AnyAgents supplies only caller policy: profile names and directories,
//! secret references ([`secrets`]), proxy and browser settings from the
//! environment, snapshot ids, idle reaping, and takeover delivery.
//!
//! Sessions need Linux. Other hosts answer every call with a typed
//! `UNSUPPORTED` error that names [`REMOTE_WORKSTATION_ENV`]; there the
//! session helpers below are compiled but unused.
#![cfg_attr(not(target_os = "linux"), allow(dead_code, unused_imports))]

use std::path::{Path, PathBuf};

use anyagents_core::Error;

use crate::computer::{ErrorDisposition, format_computer_error};

pub mod browser;
#[cfg(target_os = "linux")]
pub mod desktop;
#[cfg(target_os = "linux")]
mod legacy;
pub mod secrets;
#[cfg(target_os = "linux")]
pub mod session;
#[cfg(target_os = "linux")]
mod takeover;

#[cfg(all(test, target_os = "linux"))]
mod headful;

pub use browser::BrowserRequest;

#[cfg(any(test, not(target_os = "linux")))]
/// Setting that remote routing reads: the base URL of a Linux workstation
/// that serves desktop sessions for a host without one.
pub const REMOTE_WORKSTATION_ENV: &str = "ANYAGENTS_WORKSTATION_URL";

/// Root of every browser profile.
pub fn profiles_root(data: &Path) -> PathBuf {
    data.join("browser-profiles")
}

/// Directory of `profile` (`user-data/` and `fingerprint.json` inside).
pub fn profile_dir(data: &Path, profile: &str) -> PathBuf {
    profiles_root(data).join(profile)
}

/// A filesystem-safe profile name; `fallback` when `raw` is empty.
pub fn sanitize_profile_name(raw: Option<&str>, fallback: &str) -> String {
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

/// A tool failure with a stable machine-readable code.
///
/// Rendered through [`format_computer_error`], so agents see the same
/// `code` + `disposition` shape as every other desktop tool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolError {
    /// Stable code such as `UNSUPPORTED`, `STALE_REF`, `HUMAN_IN_CONTROL`.
    pub code: &'static str,
    /// Human-readable message (never a secret value).
    pub message: String,
    /// What the agent should do next.
    pub hint: &'static str,
    /// Whether the same call can succeed later without changes.
    pub retryable: bool,
}

impl ToolError {
    /// The capability does not exist on this host or engine.
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self {
            code: "UNSUPPORTED",
            message: message.into(),
            hint: "This capability is not available here; do not retry.",
            retryable: false,
        }
    }

    /// The caller passed an argument the tool refuses.
    pub fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: "INVALID_INPUT",
            message: message.into(),
            hint: "Fix the request arguments.",
            retryable: false,
        }
    }

    /// A snapshot-qualified ref names an older snapshot.
    pub fn stale_ref(message: impl Into<String>) -> Self {
        Self {
            code: "STALE_REF",
            message: message.into(),
            hint: "Call browser({action: 'snapshot'}) to refresh refs.",
            retryable: false,
        }
    }

    fn disposition(&self) -> ErrorDisposition {
        let mut d = match (self.code, self.retryable) {
            ("STALE_REF", _) => ErrorDisposition::stale_ref(self.hint),
            ("NOT_FOUND", _) => ErrorDisposition::settled_absence(self.hint),
            (_, true) => ErrorDisposition::retryable(self.hint),
            (_, false) => ErrorDisposition::terminal(self.hint),
        };
        d.code = Some(self.code.to_string());
        d
    }

    /// Render for the tool surface; `redact` replaces the message for calls
    /// that carried a secret reference.
    pub fn into_error(self, redact: bool) -> Error {
        let message = if redact {
            "browser secret input failed (provider/page details withheld)".to_string()
        } else {
            self.message.clone()
        };
        Error::Message(format_computer_error(message, self.disposition()))
    }
}

impl From<ToolError> for Error {
    fn from(e: ToolError) -> Self {
        e.into_error(false)
    }
}

#[cfg(target_os = "linux")]
impl From<agent_workstation::Error> for ToolError {
    fn from(e: agent_workstation::Error) -> Self {
        use agent_workstation::Error as W;
        let (code, hint) = match &e {
            W::HumanInControl { .. } => (
                "HUMAN_IN_CONTROL",
                "A person controls this desktop; call browser({action: 'wait_for_user'}) or release_takeover.",
            ),
            W::NotFound(_) => (
                "NOT_FOUND",
                "Inspect the page with a new browser snapshot before acting.",
            ),
            W::InvalidInput(_) => ("INVALID_INPUT", "Fix the request arguments."),
            W::Unsupported(_) => (
                "UNSUPPORTED",
                "This capability is not available here; do not retry.",
            ),
            W::Refused(_) => (
                "REFUSED",
                "A precondition refused the action; inspect the page first.",
            ),
            W::Timeout(_) | W::Starting { .. } => (
                "TIMEOUT",
                "Transient browser failure; retry once or inspect browser status.",
            ),
            W::Profile(_) => (
                "PROFILE",
                "The profile directory is unusable or held by another browser.",
            ),
            W::Closed => (
                "CLOSED",
                "The desktop session closed; the next call starts a new one.",
            ),
            _ => (
                "DESKTOP_FAILURE",
                "Check browser status, the desktop runtime, and takeover state.",
            ),
        };
        Self {
            code,
            message: e.to_string(),
            hint,
            retryable: e.is_transient() || matches!(e, W::Closed),
        }
    }
}

/// The error every session call returns on a host without session desktops.
#[cfg(any(test, not(target_os = "linux")))]
pub fn unsupported_host() -> ToolError {
    ToolError::unsupported(format!(
        "desktop sessions (tools.browser and session tools.computer) need a Linux host, and this \
         AnyAgents host runs {os}. Point this host at a Linux workstation with \
         {REMOTE_WORKSTATION_ENV}, for example \
         {REMOTE_WORKSTATION_ENV}=https://linux-workstation.example:7600; that workstation runs the \
         agent-workstation daemon (sway, wayvnc and a Chromium-family browser installed, started \
         with AGENT_WORKSTATION_TOKEN). This release does not route through \
         {REMOTE_WORKSTATION_ENV} yet, so until it does, run browser work on a Linux AnyAgents \
         host. Host desktop control (tools.computer with `platform`) is unaffected.",
        os = std::env::consts::OS,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_names_are_sanitized() {
        assert_eq!(sanitize_profile_name(Some(" Work Bot "), "x"), "work-bot");
        assert_eq!(sanitize_profile_name(Some(""), "agent-a"), "agent-a");
        let climbing = sanitize_profile_name(Some("../.."), "x");
        assert!(
            !climbing.contains('/') && climbing != ".." && climbing != ".",
            "{climbing}"
        );
        assert_eq!(sanitize_profile_name(Some("--"), "x"), "default");
    }

    #[test]
    fn unsupported_host_error_is_typed_and_names_the_routing_setting() {
        let rendered = unsupported_host().into_error(false).to_string();
        let json: serde_json::Value =
            serde_json::from_str(rendered.split_once(" | ").unwrap().1).unwrap();
        assert_eq!(json["code"], "UNSUPPORTED");
        assert_eq!(json["disposition"]["retry"], "never");
        let message = json["error"].as_str().unwrap();
        assert!(message.contains(REMOTE_WORKSTATION_ENV), "{message}");
        assert!(
            message.contains("ANYAGENTS_WORKSTATION_URL=https://"),
            "names an example value: {message}"
        );
        assert!(message.contains(std::env::consts::OS), "{message}");
    }

    #[test]
    fn secret_calls_redact_messages_but_keep_codes() {
        let rendered = ToolError::invalid("op://vault/item/password is wrong")
            .into_error(true)
            .to_string();
        assert!(rendered.contains("INVALID_INPUT"), "{rendered}");
        assert!(!rendered.contains("vault"), "{rendered}");
    }
}
