//! Shared WebSocket reverse-tunnel protocol frames for `anyagents node`.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Transport WebSocket Ping & Heartbeat interval (< 60s CDN/proxy idle cutoffs).
pub(crate) const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(25);
pub(crate) const MAX_TUNNEL_FRAME_BYTES: usize = 2 * 1024 * 1024;
pub(crate) const MAX_EXEC_OUTPUT_BYTES: usize = 128 * 1024;
pub(crate) const DEFAULT_EXEC_TIMEOUT_SECS: u64 = 30;
pub(crate) const MAX_EXEC_TIMEOUT_SECS: u64 = 300;

/// Maximum concurrent multiplexed TCP egress streams per node.
pub(crate) const MAX_STREAM_CONCURRENCY: usize = 64;
/// Per-stream window size (bytes) before backpressure feedback.
pub(crate) const STREAM_WINDOW_BYTES: usize = 256 * 1024;
/// Idle timeout for a multiplexed stream with no data in either direction.
pub(crate) const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
/// Maximum file size for path-jailed file read/write (default 10 MiB).
pub(crate) const MAX_FILE_SIZE_BYTES: u64 = 10 * 1024 * 1024;

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

    // ── Multiplexed TCP egress streams (exit capability) ─────────────
    StreamOpen {
        stream_id: String,
        host: String,
        port: u16,
    },
    StreamData {
        stream_id: String,
        data_b64: String,
    },
    StreamClose {
        stream_id: String,
    },

    // ── Scoped files capability ──────────────────────────────────────
    FilesList {
        req_id: String,
        path: String,
    },
    FilesRead {
        req_id: String,
        path: String,
    },
    FilesWrite {
        req_id: String,
        path: String,
        data_b64: String,
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
        #[serde(default)]
        active_streams: usize,
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

    // ── Multiplexed TCP egress stream responses ──────────────────────
    StreamOpened {
        stream_id: String,
    },
    StreamData {
        stream_id: String,
        data_b64: String,
    },
    StreamError {
        stream_id: String,
        error: String,
    },
    StreamClosed {
        stream_id: String,
        bytes_tx: u64,
        bytes_rx: u64,
    },

    // ── Scoped files capability responses ────────────────────────────
    FilesResult {
        req_id: String,
        #[serde(default)]
        entries: Option<Vec<FilesEntry>>,
        #[serde(default)]
        data_b64: Option<String>,
        #[serde(default)]
        error: Option<String>,
    },
}

/// A single directory entry returned by `files_list`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct FilesEntry {
    pub(crate) name: String,
    pub(crate) is_dir: bool,
    pub(crate) size: u64,
}
