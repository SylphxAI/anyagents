//! Shared WebSocket reverse-tunnel protocol frames for `anyagents node`.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Transport WebSocket Ping & Heartbeat interval (< 60s CDN/proxy idle cutoffs).
pub(crate) const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(25);
pub(crate) const MAX_TUNNEL_FRAME_BYTES: usize = 2 * 1024 * 1024;
pub(crate) const MAX_EXEC_OUTPUT_BYTES: usize = 128 * 1024;
pub(crate) const DEFAULT_EXEC_TIMEOUT_SECS: u64 = 30;
pub(crate) const MAX_EXEC_TIMEOUT_SECS: u64 = 300;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OutputStream {
    Stdout,
    Stderr,
}

/// Frames sent from Coordinator -> Node daemon over `/v1/nodes/connect`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum NodeServerFrame {
    Welcome {
        node_id: String,
        status: String,
    },
    ExecStart {
        exec_id: String,
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        cwd: Option<String>,
        timeout_secs: u64,
    },
    ExecCancel {
        exec_id: String,
        reason: String,
    },
    Control {
        action: String,
        #[serde(default)]
        reason: Option<String>,
    },
}

/// Frames sent from Node daemon -> Coordinator over `/v1/nodes/connect`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum NodeClientFrame {
    Hello {
        hostname: String,
        os: String,
        arch: String,
        capabilities: Vec<String>,
    },
    Heartbeat {
        active_execs: usize,
    },
    ExecOutput {
        exec_id: String,
        stream: OutputStream,
        data_b64: String,
    },
    ExecDone {
        exec_id: String,
        exit_code: Option<i32>,
        timed_out: bool,
        cancelled: bool,
        #[serde(default)]
        error: Option<String>,
        duration_ms: u64,
    },
}
