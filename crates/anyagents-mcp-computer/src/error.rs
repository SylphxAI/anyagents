//! Typed desktop automation errors and structured retry dispositions.

use agent_workstation::AdapterError;
use serde_json::{Value, json};

/// Typed error returned by `anyagents-mcp-computer` operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComputerError {
    pub code: &'static str,
    pub message: String,
    pub retry: &'static str,
    pub hint: String,
}

impl ComputerError {
    /// Capability or display is unavailable on this host (e.g. headless container without display).
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self {
            code: "UNSUPPORTED",
            message: message.into(),
            retry: "never",
            hint: "Desktop GUI automation requires an active display session (X11/Wayland on Linux, macOS Accessibility, or Windows UIA).".to_string(),
        }
    }

    /// Invalid tool arguments or coordinates.
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self {
            code: "INVALID_INPUT",
            message: message.into(),
            retry: "never",
            hint: "Fix the tool arguments and call again.".to_string(),
        }
    }

    /// Accessibility ref belongs to a superseded snapshot.
    pub fn stale_ref(message: impl Into<String>) -> Self {
        Self {
            code: "STALE_REF",
            message: message.into(),
            retry: "never",
            hint: "Call `computer_snapshot` to obtain fresh `@<snapshot_id>:e<N>` refs."
                .to_string(),
        }
    }

    /// Requested node or element was not found.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            code: "NOT_FOUND",
            message: message.into(),
            retry: "never",
            hint: "Call `computer_snapshot` or `computer_screenshot` to inspect current desktop state.".to_string(),
        }
    }

    /// Host OS refused accessibility or input injection permissions.
    pub fn permission_denied(message: impl Into<String>) -> Self {
        Self {
            code: "PERMISSION_DENIED",
            message: message.into(),
            retry: "never",
            hint: "Grant desktop accessibility / screen-recording permission to this process on the host OS.".to_string(),
        }
    }

    /// Transient or backend execution failure.
    pub fn backend(message: impl Into<String>) -> Self {
        Self {
            code: "BACKEND_ERROR",
            message: message.into(),
            retry: "verify_first",
            hint: "Inspect the current desktop state before retrying.".to_string(),
        }
    }

    /// Serialize into a structured MCP error JSON payload.
    pub fn to_json(&self) -> Value {
        json!({
            "code": self.code,
            "error": self.message,
            "disposition": {
                "retry": self.retry,
                "hint": self.hint,
            }
        })
    }
}

impl std::fmt::Display for ComputerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ComputerError {}

impl From<AdapterError> for ComputerError {
    fn from(err: AdapterError) -> Self {
        match &err {
            AdapterError::StaleRef(_) => Self::stale_ref(err.to_string()),
            AdapterError::NodeNotFound(_) => Self::not_found(err.to_string()),
            AdapterError::PermissionDenied(_) => Self::permission_denied(err.to_string()),
            AdapterError::UnsupportedPlatform(_) | AdapterError::UnsupportedAction(_) => {
                Self::unsupported(err.to_string())
            }
            AdapterError::InvalidRef(_) | AdapterError::InvalidInput(_) => {
                Self::invalid_input(err.to_string())
            }
            AdapterError::Backend(_) => Self::backend(err.to_string()),
            _ => Self::backend(err.to_string()),
        }
    }
}
