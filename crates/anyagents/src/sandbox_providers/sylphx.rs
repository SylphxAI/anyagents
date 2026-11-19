//! Native Sylphx Sandboxes v1 REST/Connect-RPC client, persistent volume/lease
//! lifecycle, and [`SandboxMode`] isolation policy.

use std::collections::BTreeMap;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::{DEFAULT_EXEC_TIMEOUT_MS, url_encode_query_component};
use agent_tools::{
    DockerWorkspace, ExecOutput, LocalWorkspace, REMOTE_EXEC_MAX_TIMEOUT_MS,
    RemoteSandboxWorkspace, SandboxConfig, SandboxProviderConfig, WorkspaceBackend, base64_decode,
    base64_encode, sha256_hex,
};

/// Environment variable selecting the sandbox isolation mode.
pub const ENV_SANDBOX_MODE: &str = "ANYAGENTS_SANDBOX_MODE";
/// Environment variable enabling pure serverless scale-to-zero mode (`true` | `false`).
pub const ENV_SERVERLESS: &str = "ANYAGENTS_SERVERLESS";
/// Environment variable selecting the sandbox provider driver (`"sylphx"` | `"legacy_http"`).
pub const ENV_SANDBOX_DRIVER: &str = "ANYAGENTS_SANDBOX_DRIVER";
/// Environment variable naming the Sylphx resource parent (`orgs/{o}/projects/{p}/envs/{e}`).
pub const ENV_SYLPHX_PARENT: &str = "ANYAGENTS_SYLPHX_PARENT";
/// Environment variable selecting the Sylphx sandbox template (default `"agent"`).
pub const ENV_SANDBOX_TEMPLATE: &str = "ANYAGENTS_SANDBOX_TEMPLATE";
/// Environment variable selecting the Sylphx sandbox compute shape (default `"linux-m"`).
pub const ENV_SANDBOX_SHAPE: &str = "ANYAGENTS_SANDBOX_SHAPE";
/// Environment variable configuring persistent agent volume size in GiB.
pub const ENV_SANDBOX_VOLUME_SIZE_GIB: &str = "ANYAGENTS_SANDBOX_VOLUME_SIZE_GIB";
/// Single persistent volume mount root inside sandboxes (`/home/user/agent-data`).
///
/// Contains BOTH `state/` and `workspace/` so agent state, git checkouts, and
/// uncommitted edits all survive compute lease `:release` / microVM recycling.
pub const DEFAULT_MOUNT_PATH: &str = "/home/user/agent-data";
/// Default persistent agent state directory inside the mounted volume (`/home/user/agent-data/state`).
pub const DEFAULT_STATE_DIR: &str = "/home/user/agent-data/state";
/// Default persistent working directory inside sandboxes (`/home/user/agent-data/workspace`).
pub const DEFAULT_SANDBOX_WORKDIR: &str = "/home/user/agent-data/workspace";
/// Default Sylphx template name.
pub const DEFAULT_TEMPLATE: &str = "agent";
/// Default Sylphx machine shape.
pub const DEFAULT_SHAPE: &str = "linux-m";
/// Default safety lease TTL passed to `POST /v1/{parent}/leases` and `:renew` (15 minutes).
pub const DEFAULT_LEASE_TTL: &str = "900s";
/// Default persistent volume size in GiB when `ANYAGENTS_SANDBOX_VOLUME_SIZE_GIB` is unset.
pub const DEFAULT_VOLUME_SIZE_GIB: u64 = 10;
/// Per-agent Sylphx lease and persistent volume state file inside `<config_dir>`.
pub const SYLPHX_STATE_FILE: &str = "sandbox-sylphx.json";

static EXEC_IDEMPOTENCY_SEQ: AtomicU64 = AtomicU64::new(1);

const DEFAULT_SYLPHX_ENDPOINT: &str = "https://api.sylphx.com";
const LEASE_READY_POLL_INITIAL: Duration = Duration::from_millis(25);
const LEASE_READY_POLL_INTERVAL: Duration = Duration::from_millis(200);
const LEASE_READY_TIMEOUT: Duration = Duration::from_secs(120);
const VOLUME_READY_TIMEOUT: Duration = Duration::from_secs(60);
const EXEC_IN_PROGRESS_POLL_DELAY: Duration = Duration::from_millis(1_000);

/// Return `true` when `ANYAGENTS_SERVERLESS` is explicitly enabled (`1` | `true` | `yes` | `on`).
pub fn serverless_enabled_from_env() -> bool {
    std::env::var(ENV_SERVERLESS).ok().is_some_and(|raw| {
        matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

/// Orchestrator sandbox isolation policy (`ANYAGENTS_SANDBOX_MODE`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxMode {
    /// Default for open-source local deployments: run on host unless sandbox config exists.
    #[default]
    Off,
    /// Provision remote sandboxes when a sandbox provider is configured; otherwise run locally.
    Auto,
    /// Thin coordinator mode: fail closed whenever no remote sandbox backend is available.
    RemoteRequired,
}

impl SandboxMode {
    /// Parse a [`SandboxMode`] policy value.
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "off" | "local" | "disabled" | "false" | "0" => Ok(Self::Off),
            "auto" => Ok(Self::Auto),
            "remote_required" | "remote-required" | "required" | "remote" => {
                Ok(Self::RemoteRequired)
            }
            other => Err(format!(
                "invalid {ENV_SANDBOX_MODE} value `{other}` (expected `off`, `auto`, or `remote_required`)"
            )),
        }
    }

    /// Parse [`SandboxMode`] from `ANYAGENTS_SANDBOX_MODE`, validating unrecognized values.
    /// Automatically enables `RemoteRequired` when `ANYAGENTS_SERVERLESS=true`.
    pub fn checked_from_env() -> Result<Self, String> {
        if serverless_enabled_from_env() {
            if let Ok(val) = std::env::var(ENV_SANDBOX_MODE)
                && !val.trim().is_empty()
            {
                let _ = Self::parse(&val)?;
            }
            return Ok(Self::RemoteRequired);
        }
        match std::env::var(ENV_SANDBOX_MODE) {
            Ok(val) => Self::parse(&val),
            Err(_) => Ok(Self::Off),
        }
    }

    /// Read [`SandboxMode`] from `ANYAGENTS_SANDBOX_MODE`, failing closed to `RemoteRequired`
    /// if a non-empty value cannot be parsed or if `ANYAGENTS_SERVERLESS=true`.
    pub fn from_env() -> Self {
        if serverless_enabled_from_env() {
            return Self::RemoteRequired;
        }
        match std::env::var(ENV_SANDBOX_MODE) {
            Ok(val) if !val.trim().is_empty() => Self::parse(&val).unwrap_or(Self::RemoteRequired),
            _ => Self::Off,
        }
    }

    /// Whether local host fallback is forbidden.
    pub fn is_remote_required(self) -> bool {
        self == Self::RemoteRequired
    }

    /// Whether execution may fall back to the coordinator host filesystem.
    pub fn allows_local_fallback(self) -> bool {
        self != Self::RemoteRequired
    }
}

/// Sandbox provisioning/transport driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxDriver {
    /// Legacy E2B / HTTP `/v1/sandboxes` driver.
    #[default]
    LegacyHttp,
    /// Native Sylphx Sandboxes v1 Volumes + Leases + `envd` Connect-RPC driver.
    Sylphx,
}

impl SandboxDriver {
    /// Parse a sandbox driver string (`"sylphx"` or `"legacy_http"`).
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "sylphx" | "sylphx-v1" | "sylphx_v1" => Ok(Self::Sylphx),
            "" | "legacy_http" | "legacy-http" | "legacy" | "e2b" | "http" => Ok(Self::LegacyHttp),
            other => Err(format!(
                "invalid {ENV_SANDBOX_DRIVER} value `{other}` (expected `sylphx` or `legacy_http`)"
            )),
        }
    }
}

/// Combined provider file model allowing `driver: "sylphx" | "legacy_http"` and
/// Sylphx-specific settings alongside [`SandboxProviderConfig`] fields.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct RawProviderFile {
    #[serde(default)]
    driver: Option<String>,
    #[serde(default)]
    endpoint: Option<String>,
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    parent: Option<String>,
    #[serde(default)]
    template: Option<String>,
    #[serde(default)]
    shape: Option<String>,
    #[serde(default)]
    volume_size_gib: Option<u64>,
    #[serde(default)]
    default_workdir: Option<String>,
    #[serde(default)]
    mount_path: Option<String>,
    #[serde(default)]
    remote_spawn: Option<bool>,
}

/// Resolved Sylphx Sandboxes v1 provider configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SylphxProviderConfig {
    pub driver: SandboxDriver,
    pub endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    pub parent: String,
    pub template: String,
    pub shape: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_size_gib: Option<u64>,
    pub default_workdir: String,
    pub mount_path: String,
}

impl SylphxProviderConfig {
    /// Resolve Sylphx provider settings from `<data_dir>/sandbox-provider.json`
    /// (or `ANYAGENTS_SANDBOX_PROVIDER_CONFIG` / `$HOME/.config/anyagents/sandbox-provider.json`)
    /// overlaid with environment variables.
    ///
    /// Returns `Some` only when `driver == SandboxDriver::Sylphx` is explicitly requested or
    /// `ANYAGENTS_SYLPHX_PARENT` / `parent` in `sandbox-provider.json` is set.
    pub fn resolve(data_dir: Option<&Path>) -> Result<Option<Self>, String> {
        let file_cfg = load_raw_provider_file(data_dir);
        let env_driver = env_non_empty(ENV_SANDBOX_DRIVER);
        let env_parent = env_non_empty(ENV_SYLPHX_PARENT);
        let file_parent = file_cfg
            .as_ref()
            .and_then(|c| c.parent.as_deref())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);

        let explicit_driver = if let Some(ref d) = env_driver {
            Some(SandboxDriver::parse(d)?)
        } else if let Some(ref d) = file_cfg.as_ref().and_then(|c| c.driver.clone()) {
            Some(SandboxDriver::parse(d)?)
        } else {
            None
        };

        let parent = env_parent.or(file_parent);
        let driver = match explicit_driver {
            Some(SandboxDriver::LegacyHttp) => return Ok(None),
            Some(SandboxDriver::Sylphx) => SandboxDriver::Sylphx,
            None if parent.is_some() => SandboxDriver::Sylphx,
            None => return Ok(None),
        };

        let parent = parent
            .map(|p| normalize_parent(&p))
            .filter(|p| !p.is_empty())
            .ok_or_else(|| {
                format!(
                    "Sylphx sandbox driver selected, but {ENV_SYLPHX_PARENT} (orgs/{{o}}/projects/{{p}}/envs/{{e}}) is not configured"
                )
            })?;

        let endpoint = env_non_empty("ANYAGENTS_SANDBOX_ENDPOINT")
            .or_else(|| env_non_empty("SYLPHX_SANDBOX_URL"))
            .or_else(|| env_non_empty("ANYAGENTS_SYLPHX_ENDPOINT"))
            .or_else(|| {
                file_cfg
                    .as_ref()
                    .and_then(|c| c.endpoint.as_deref())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
            })
            .unwrap_or_else(|| DEFAULT_SYLPHX_ENDPOINT.to_string());

        let token = env_non_empty("ANYAGENTS_SANDBOX_TOKEN")
            .or_else(|| env_non_empty("SYLPHX_API_KEY"))
            .or_else(|| env_non_empty("ANYAGENTS_SYLPHX_TOKEN"))
            .or_else(|| {
                file_cfg
                    .as_ref()
                    .and_then(|c| c.token.as_deref())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
            });

        let template = env_non_empty(ENV_SANDBOX_TEMPLATE)
            .or_else(|| {
                file_cfg
                    .as_ref()
                    .and_then(|c| c.template.as_deref())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
            })
            .unwrap_or_else(|| DEFAULT_TEMPLATE.to_string());

        let shape = env_non_empty(ENV_SANDBOX_SHAPE)
            .or_else(|| {
                file_cfg
                    .as_ref()
                    .and_then(|c| c.shape.as_deref())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
            })
            .unwrap_or_else(|| DEFAULT_SHAPE.to_string());

        let volume_size_gib = match env_non_empty(ENV_SANDBOX_VOLUME_SIZE_GIB) {
            Some(raw) => Some(raw.parse::<u64>().map_err(|err| {
                format!("invalid {ENV_SANDBOX_VOLUME_SIZE_GIB} value `{raw}`: {err}")
            })?),
            None => file_cfg.as_ref().and_then(|c| c.volume_size_gib),
        };

        let default_workdir = env_non_empty("ANYAGENTS_SANDBOX_WORKDIR")
            .or_else(|| {
                file_cfg
                    .as_ref()
                    .and_then(|c| c.default_workdir.as_deref())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
            })
            .unwrap_or_else(|| DEFAULT_SANDBOX_WORKDIR.to_string());

        let mount_path = env_non_empty("ANYAGENTS_SANDBOX_MOUNT_PATH")
            .or_else(|| {
                file_cfg
                    .as_ref()
                    .and_then(|c| c.mount_path.as_deref())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
            })
            .unwrap_or_else(|| DEFAULT_MOUNT_PATH.to_string());

        Ok(Some(Self {
            driver,
            endpoint: normalize_api_base(&endpoint),
            token,
            parent,
            template,
            shape,
            volume_size_gib,
            default_workdir: normalize_unix_dir(&default_workdir, DEFAULT_SANDBOX_WORKDIR),
            mount_path: normalize_unix_dir(&mount_path, DEFAULT_MOUNT_PATH),
        }))
    }
}

/// Persistent Sylphx Sandboxes v1 volume metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SylphxVolume {
    pub resource_name: String,
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_gib: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
}

/// Sylphx Sandboxes v1 compute lease metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SylphxLease {
    pub resource_name: String,
    pub id: String,
    #[serde(default)]
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_plane_resource: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envd_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envd_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

impl SylphxLease {
    /// Whether this lease attaches `volume_resource` (by full resource name or volume id suffix).
    pub fn attaches_volume(&self, volume_resource: &str) -> bool {
        let target = volume_resource.trim().trim_matches('/');
        if target.is_empty() {
            return false;
        }
        let target_short = target.rsplit('/').next().unwrap_or(target);
        self.volumes.iter().any(|v| {
            let candidate = v.trim().trim_matches('/');
            candidate == target || candidate.rsplit('/').next() == Some(target_short)
        })
    }

    /// Whether this lease is in a runnable state (`READY` / `RUNNING` / `ACTIVE`).
    pub fn is_ready(&self) -> bool {
        let s = self.state.trim().to_ascii_uppercase();
        matches!(
            s.as_str(),
            "READY" | "STATE_READY" | "LEASE_STATE_READY" | "RUNNING" | "ACTIVE"
        )
    }

    /// Whether this lease has permanently ended and a fresh lease is required.
    pub fn is_terminal(&self) -> bool {
        let s = self.state.trim().to_ascii_uppercase();
        matches!(
            s.as_str(),
            "ENDED"
                | "STATE_ENDED"
                | "LEASE_STATE_ENDED"
                | "ENDING"
                | "STATE_ENDING"
                | "LEASE_STATE_ENDING"
                | "REFUSED"
                | "STATE_REFUSED"
                | "LEASE_STATE_REFUSED"
                | "PAUSED"
                | "STATE_PAUSED"
                | "LEASE_STATE_PAUSED"
                | "RELEASED"
                | "STATE_RELEASED"
                | "LEASE_STATE_RELEASED"
                | "EXPIRED"
                | "STATE_EXPIRED"
                | "LEASE_STATE_EXPIRED"
                | "FAILED"
                | "STATE_FAILED"
                | "LEASE_STATE_FAILED"
                | "TERMINATED"
                | "STATE_TERMINATED"
                | "DELETED"
        )
    }
}

/// Persisted per-agent Sylphx Sandboxes v1 lease + persistent volume record (`sandbox-sylphx.json`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SylphxLeaseConfig {
    pub endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    pub parent: String,
    pub template: String,
    pub shape: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_size_gib: Option<u64>,
    pub workdir: String,
    pub mount_path: String,
    pub volume_id: String,
    pub volume_resource: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_resource: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_plane_resource: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envd_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envd_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
}

impl SylphxLeaseConfig {
    /// Load `<config_dir>/sandbox-sylphx.json` if present.
    pub fn from_config_dir(config_dir: &Path) -> Option<Self> {
        let path = config_dir.join(SYLPHX_STATE_FILE);
        let raw = std::fs::read_to_string(path).ok()?;
        let mut parsed: Self = serde_json::from_str(&raw).ok()?;
        parsed.endpoint = normalize_api_base(&parsed.endpoint);
        parsed.parent = normalize_parent(&parsed.parent);
        parsed.workdir = normalize_unix_dir(&parsed.workdir, DEFAULT_SANDBOX_WORKDIR);
        parsed.mount_path = normalize_unix_dir(&parsed.mount_path, DEFAULT_MOUNT_PATH);
        if parsed.token.is_none() {
            parsed.token = env_non_empty("ANYAGENTS_SANDBOX_TOKEN")
                .or_else(|| env_non_empty("SYLPHX_API_KEY"))
                .or_else(|| env_non_empty("ANYAGENTS_SYLPHX_TOKEN"));
        }
        Some(parsed)
    }

    /// Atomically persist `<config_dir>/sandbox-sylphx.json` and sync `<config_dir>/sandbox.json`.
    pub fn save_to_config_dir(&self, config_dir: &Path) -> Result<(), String> {
        std::fs::create_dir_all(config_dir)
            .map_err(|err| format!("cannot create {}: {err}", config_dir.display()))?;
        let state_path = config_dir.join(SYLPHX_STATE_FILE);
        let json = serde_json::to_string_pretty(self).map_err(|err| err.to_string())?;
        write_private_file(&state_path, format!("{json}\n").as_bytes())?;

        // Keep `<config_dir>/sandbox.json` synchronized so `remote::runs_off_host` and
        // generic `SandboxConfig::resolve` recognize this agent as sandbox-backed.
        let legacy = SandboxConfig {
            endpoint: self.endpoint.clone(),
            sandbox_id: self
                .lease_id
                .clone()
                .unwrap_or_else(|| self.volume_id.clone()),
            token: self.token.clone(),
            workdir: self.workdir.clone(),
            remote_spawn: true,
        };
        let legacy_json = serde_json::to_string_pretty(&legacy).map_err(|err| err.to_string())?;
        write_private_file(
            &config_dir.join("sandbox.json"),
            format!("{legacy_json}\n").as_bytes(),
        )
    }

    /// Active data-plane resource path (e.g. `orgs/.../leases/{id}`).
    pub fn active_data_resource(&self) -> Result<&str, String> {
        self.data_plane_resource
            .as_deref()
            .or(self.lease_resource.as_deref())
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| "no active Sylphx sandbox lease".to_string())
    }

    /// Resolved `envd` proxy base URL for streaming Connect-RPC process control.
    pub fn resolved_envd_url(&self) -> Result<String, String> {
        if let Some(url) = self
            .envd_url
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            return Ok(url.trim_end_matches('/').to_string());
        }
        let resource = self.active_data_resource()?.trim_matches('/');
        Ok(format!(
            "{}/v1/{resource}/envd",
            normalize_api_base(&self.endpoint)
        ))
    }

    /// Build a client configured for this lease's endpoint and bearer token.
    pub fn client(&self) -> SylphxSandboxClient {
        SylphxSandboxClient::new(self.endpoint.clone(), self.token.clone())
    }

    /// Persistent agent state directory inside the mounted volume.
    ///
    /// Under the unified `/home/user/agent-data` mount this resolves to
    /// `/home/user/agent-data/state`; legacy `/home/user/agent-state` configs keep
    /// `/home/user/agent-state` directly.
    pub fn state_dir(&self) -> String {
        let mp = normalize_unix_dir(&self.mount_path, DEFAULT_MOUNT_PATH);
        if mp == "/home/user/agent-state" {
            mp
        } else {
            format!("{mp}/state")
        }
    }

    /// Persistent workspace directory inside the mounted volume.
    pub fn workspace_dir(&self) -> String {
        normalize_unix_dir(&self.workdir, DEFAULT_SANDBOX_WORKDIR)
    }
}

/// Directory entry returned by `:listFiles`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SylphxFileEntry {
    pub name: String,
    pub path: String,
    pub is_dir: bool,
    #[serde(default)]
    pub size_bytes: Option<u64>,
}

/// Streaming process event decoded from Connect-RPC `process.Process/Start`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvdProcessEvent {
    Start {
        pid: u32,
    },
    Data {
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        pty: Vec<u8>,
    },
    End {
        exit_code: Option<i32>,
        error: Option<String>,
        status: Option<String>,
    },
    Keepalive,
}

/// POSIX signal sent via `process.Process/SendSignal`.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvdSignal {
    SigTerm,
    SigKill,
    SigInt,
}

impl EnvdSignal {
    fn as_proto_str(self) -> &'static str {
        match self {
            Self::SigTerm => "SIGNAL_SIGTERM",
            Self::SigKill => "SIGNAL_SIGKILL",
            Self::SigInt => "SIGNAL_SIGINT",
        }
    }
}

/// Native HTTP/Connect-RPC client for Sylphx Sandboxes v1.
#[derive(Debug, Clone)]
pub struct SylphxSandboxClient {
    pub endpoint: String,
    pub token: Option<String>,
}

impl SylphxSandboxClient {
    /// Create a client targeting `endpoint` (e.g. `https://api.sylphx.com`) with optional bearer token.
    pub fn new(endpoint: impl Into<String>, token: Option<String>) -> Self {
        Self {
            endpoint: normalize_api_base(&endpoint.into()),
            token: token
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty()),
        }
    }

    fn rest_agent(&self, timeout_ms: u64) -> ureq::Agent {
        ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_millis(timeout_ms.max(1_000))))
            .http_status_as_error(false)
            .build()
            .new_agent()
    }

    fn stream_agent(&self) -> ureq::Agent {
        // Long-lived turns via envd streaming must never be aborted at 120s/300s.
        ureq::Agent::config_builder()
            .timeout_connect(Some(Duration::from_secs(15)))
            .timeout_global(None)
            .timeout_recv_body(None)
            .timeout_send_body(None)
            .http_status_as_error(false)
            .build()
            .new_agent()
    }

    fn v1_resource_url(&self, resource: &str) -> String {
        let trimmed = resource.trim_matches('/');
        format!("{}/v1/{trimmed}", self.endpoint)
    }

    fn collection_url(&self, parent: &str, collection: &str) -> String {
        let p = normalize_parent(parent);
        format!("{}/v1/{p}/{collection}", self.endpoint)
    }

    fn resolve_resource(&self, parent: &str, collection: &str, id_or_name: &str) -> String {
        let trimmed = id_or_name.trim_matches('/');
        if trimmed.contains('/') {
            trimmed.to_string()
        } else {
            let p = normalize_parent(parent);
            format!("{p}/{collection}/{trimmed}")
        }
    }

    fn request_json(
        &self,
        method: &str,
        url: &str,
        body: Option<&serde_json::Value>,
        timeout_ms: u64,
    ) -> Result<(u16, serde_json::Value), String> {
        let agent = self.rest_agent(timeout_ms);
        let resp = match method {
            "GET" => {
                let mut req = agent.get(url);
                if let Some(tok) = &self.token {
                    req = req.header("authorization", &format!("Bearer {tok}"));
                }
                req.call()
            }
            "DELETE" => {
                let mut req = agent.delete(url);
                if let Some(tok) = &self.token {
                    req = req.header("authorization", &format!("Bearer {tok}"));
                }
                req.call()
            }
            "POST" => {
                let payload = serde_json::to_vec(
                    body.unwrap_or(&serde_json::Value::Object(Default::default())),
                )
                .map_err(|e| e.to_string())?;
                let mut req = agent.post(url).header("content-type", "application/json");
                if let Some(tok) = &self.token {
                    req = req.header("authorization", &format!("Bearer {tok}"));
                }
                req.send(payload.as_slice())
            }
            other => return Err(format!("unsupported HTTP method {other}")),
        }
        .map_err(|err| format!("Sylphx sandbox request `{method} {url}` failed: {err}"))?;

        let status = resp.status().as_u16();
        let raw = resp
            .into_body()
            .read_to_string()
            .map_err(|err| format!("Sylphx sandbox response read failed ({url}): {err}"))?;
        let val = if raw.trim().is_empty() {
            serde_json::json!({})
        } else {
            serde_json::from_str(&raw).unwrap_or_else(|_| serde_json::json!({ "raw": raw }))
        };
        Ok((status, val))
    }

    /// Create a persistent volume (`POST /v1/{parent}/volumes?volume_id={id}`).
    pub fn create_volume(
        &self,
        parent: &str,
        name: &str,
        size_gib: Option<u64>,
    ) -> Result<SylphxVolume, String> {
        let volume_id = normalize_resource_id(name);
        let url = format!(
            "{}?volume_id={}",
            self.collection_url(parent, "volumes"),
            url_encode_query_component(&volume_id)
        );
        let effective_size = size_gib.unwrap_or(DEFAULT_VOLUME_SIZE_GIB).clamp(1, 1024);
        let body = serde_json::json!({
            "spec": {
                "size_gib": effective_size,
            }
        });
        let (status, val) = self.request_json("POST", &url, Some(&body), 30_000)?;
        if !(200..300).contains(&status) {
            return Err(format!(
                "Sylphx POST {url} returned HTTP {status}: {}",
                compact_error_body(&val)
            ));
        }
        parse_volume_response(&val, parent, &volume_id, Some(effective_size))
    }

    /// Fetch volume state (`GET /v1/{parent}/volumes/{id}`). Returns `Ok(None)` on HTTP 404.
    pub fn get_volume(
        &self,
        parent: &str,
        volume_id_or_name: &str,
    ) -> Result<Option<SylphxVolume>, String> {
        let resource = self.resolve_resource(parent, "volumes", volume_id_or_name);
        let url = self.v1_resource_url(&resource);
        let (status, val) = self.request_json("GET", &url, None, 20_000)?;
        if status == 404 {
            return Ok(None);
        }
        if !(200..300).contains(&status) {
            return Err(format!(
                "Sylphx GET {url} returned HTTP {status}: {}",
                compact_error_body(&val)
            ));
        }
        Ok(Some(parse_volume_response(
            &val,
            parent,
            volume_id_or_name,
            None,
        )?))
    }

    /// Delete a persistent volume (`DELETE /v1/{parent}/volumes/{id}`). Idempotent on 404.
    pub fn delete_volume(&self, parent: &str, volume_id_or_name: &str) -> Result<(), String> {
        let resource = self.resolve_resource(parent, "volumes", volume_id_or_name);
        let url = self.v1_resource_url(&resource);
        let (status, val) = self.request_json("DELETE", &url, None, 30_000)?;
        if status == 404 || (200..300).contains(&status) {
            return Ok(());
        }
        Err(format!(
            "Sylphx DELETE {url} returned HTTP {status}: {}",
            compact_error_body(&val)
        ))
    }

    /// Poll `GET /v1/{volume_resource}` until a newly created volume leaves `creating`.
    pub fn wait_volume_available(
        &self,
        parent: &str,
        volume: SylphxVolume,
        timeout: Duration,
    ) -> Result<SylphxVolume, String> {
        let is_creating = |v: &SylphxVolume| {
            v.state
                .as_deref()
                .is_some_and(|s| s.trim().eq_ignore_ascii_case("creating"))
        };
        let is_failed = |v: &SylphxVolume| {
            v.state.as_deref().is_some_and(|s| {
                let u = s.trim().to_ascii_uppercase();
                matches!(u.as_str(), "FAILED" | "DELETING" | "DELETED")
            })
        };
        if !is_creating(&volume) {
            if is_failed(&volume) {
                return Err(format!(
                    "Sylphx volume {} entered unusable state `{}`",
                    volume.resource_name,
                    volume.state.as_deref().unwrap_or("")
                ));
            }
            return Ok(volume);
        }
        let deadline = Instant::now() + timeout;
        let resource = volume.resource_name.clone();
        let mut poll_delay = LEASE_READY_POLL_INITIAL;
        loop {
            if Instant::now() >= deadline {
                return Err(format!(
                    "timed out waiting for Sylphx volume {resource} to become available"
                ));
            }
            std::thread::sleep(poll_delay);
            poll_delay = (poll_delay * 2).min(LEASE_READY_POLL_INTERVAL);
            let current = self.get_volume(parent, &resource)?.ok_or_else(|| {
                format!("Sylphx volume {resource} disappeared while waiting for available")
            })?;
            if !is_creating(&current) {
                if is_failed(&current) {
                    return Err(format!(
                        "Sylphx volume {resource} entered unusable state `{}`",
                        current.state.as_deref().unwrap_or("")
                    ));
                }
                return Ok(current);
            }
        }
    }

    /// Ensure a persistent volume exists for an agent and is no longer `creating`.
    /// Reuses `existing_volume` if found via `GET`.
    pub fn ensure_volume(
        &self,
        parent: &str,
        existing_volume: Option<&str>,
        preferred_name: &str,
        size_gib: Option<u64>,
    ) -> Result<SylphxVolume, String> {
        if let Some(existing) = existing_volume.map(str::trim).filter(|s| !s.is_empty())
            && let Some(found) = self.get_volume(parent, existing)?
        {
            return self.wait_volume_available(parent, found, VOLUME_READY_TIMEOUT);
        }
        let volume_id = normalize_resource_id(preferred_name);
        if let Some(by_name) = self.get_volume(parent, &volume_id)? {
            return self.wait_volume_available(parent, by_name, VOLUME_READY_TIMEOUT);
        }
        let vol = match self.create_volume(parent, &volume_id, size_gib) {
            Ok(vol) => vol,
            Err(err) if err.contains("HTTP 409") || err.contains("RESOURCE_ALREADY_EXISTS") => {
                if let Some(by_name) = self.get_volume(parent, &volume_id)? {
                    by_name
                } else {
                    return Err(err);
                }
            }
            Err(err) => return Err(err),
        };
        self.wait_volume_available(parent, vol, VOLUME_READY_TIMEOUT)
    }

    /// Create a sandbox lease attaching `volume_resource` (`POST /v1/{parent}/leases`).
    #[allow(clippy::too_many_arguments)]
    pub fn create_lease(
        &self,
        parent: &str,
        volume_resource: &str,
        mount_path: &str,
        template: &str,
        shape: &str,
        env: &BTreeMap<String, String>,
        ttl: Option<&str>,
    ) -> Result<SylphxLease, String> {
        self.create_lease_with_options(
            parent,
            volume_resource,
            mount_path,
            None,
            Some(template),
            shape,
            env,
            ttl,
            None,
        )
    }

    /// Create a sandbox lease supporting BYO digest-pinned OCI `image`, `template`,
    /// persistent volume mount, and per-lease [`super::providers::NetworkPolicySpec`].
    #[allow(clippy::too_many_arguments)]
    pub fn create_lease_with_options(
        &self,
        parent: &str,
        volume_resource: &str,
        mount_path: &str,
        image: Option<&str>,
        template: Option<&str>,
        shape: &str,
        env: &BTreeMap<String, String>,
        ttl: Option<&str>,
        network: Option<&super::providers::NetworkPolicySpec>,
    ) -> Result<SylphxLease, String> {
        let url = self.collection_url(parent, "leases");
        let ttl_val = ttl
            .filter(|s| !s.trim().is_empty())
            .unwrap_or(DEFAULT_LEASE_TTL);
        let mut spec = serde_json::Map::new();
        let image_ref = if let Some(img) = image.map(str::trim).filter(|s| !s.is_empty()) {
            img.to_string()
        } else {
            let tpl = template
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(DEFAULT_TEMPLATE);
            if tpl.starts_with("template:") {
                tpl.to_string()
            } else {
                format!("template:{tpl}")
            }
        };
        spec.insert("image".into(), serde_json::json!(image_ref));
        if !shape.trim().is_empty() {
            spec.insert("shape".into(), serde_json::json!(shape.trim()));
        }
        spec.insert("ttl".into(), serde_json::json!(ttl_val));
        if !env.is_empty() {
            spec.insert("env".into(), serde_json::json!(env));
        }
        spec.insert(
            "volumes".into(),
            serde_json::json!([
                {
                    "volume": volume_resource,
                    "mount_path": mount_path,
                }
            ]),
        );
        if let Some(policy) = network {
            spec.insert("network".into(), sylphx_network_json(policy));
        }
        let body = serde_json::json!({ "spec": spec });
        const MAX_CAPACITY_RETRIES: usize = 3;
        let mut attempt = 0usize;
        let (status, val) = loop {
            // `POST /v1/{parent}/leases` waits server-side up to 60s for cold BYO-OCI
            // boot unless skipped; give the client a 120s global timeout so we never
            // prematurely abort a healthy in-flight grant and orphan its volume lock.
            let (status, val) = self.request_json("POST", &url, Some(&body), 120_000)?;
            if (200..300).contains(&status) {
                break (status, val);
            }
            let retryable_capacity = matches!(status, 429 | 503)
                && (val
                    .get("retryable")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false)
                    || val
                        .get("code")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|c| c.eq_ignore_ascii_case("NO_CAPACITY")));
            if retryable_capacity && attempt < MAX_CAPACITY_RETRIES {
                attempt += 1;
                let retry_ms = val
                    .get("retry_after_ms")
                    .and_then(|v| {
                        v.as_u64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
                    })
                    .unwrap_or(2_000)
                    .clamp(1_000, 5_000);
                std::thread::sleep(Duration::from_millis(retry_ms));
                continue;
            }
            break (status, val);
        };
        if !(200..300).contains(&status) {
            return Err(format!(
                "Sylphx POST {url} returned HTTP {status}: {}",
                compact_error_body(&val)
            ));
        }
        parse_lease_response(&val, parent)
    }

    /// Update outbound egress policy on a live lease (`POST /v1/{lease_resource}:setNetwork`).
    pub fn set_network(
        &self,
        parent: &str,
        lease_id_or_name: &str,
        policy: &super::providers::NetworkPolicySpec,
    ) -> Result<(), String> {
        let resource = self.resolve_resource(parent, "leases", lease_id_or_name);
        let url = format!("{}:setNetwork", self.v1_resource_url(&resource));
        let body = serde_json::json!({
            "network": sylphx_network_json(policy),
        });
        let (status, val) = self.request_json("POST", &url, Some(&body), 20_000)?;
        if !(200..300).contains(&status) {
            return Err(format!(
                "Sylphx POST {url} returned HTTP {status}: {}",
                compact_error_body(&val)
            ));
        }
        Ok(())
    }

    /// Initialize `/home/user/agent-data/{state,workspace}`, backwards-compatible
    /// symlinks (`/home/user/agent-state`, `/home/user/workspace`), and `user:user`
    /// ownership on freshly formatted root-owned persistent volumes.
    pub fn init_persistent_layout(
        &self,
        resource: &str,
        mount_path: &str,
        state_dir: &str,
        workdir: &str,
    ) -> Result<(), String> {
        let base_script =
            super::providers::persistent_layout_init_script(mount_path, state_dir, workdir);
        let mp = super::providers::shell_single_quote(mount_path);
        let sd = super::providers::shell_single_quote(state_dir);
        let wd = super::providers::shell_single_quote(workdir);
        let script = format!(
            "{base_script}; \
             if id user >/dev/null 2>&1; then \
               chown -R user:user {mp} {sd} {wd} 2>/dev/null || true; \
               chown -h user:user /home/user/agent-state /home/user/workspace 2>/dev/null || true; \
             fi"
        );
        let out = self.exec_as_user(
            resource,
            &["/bin/sh".to_string(), "-lc".to_string(), script],
            Some("/"),
            &BTreeMap::new(),
            Some(20_000),
            Some("root"),
        )?;
        if out.exit_code != 0 {
            return Err(format!(
                "Sylphx persistent layout init failed (exit {}): {}",
                out.exit_code,
                out.stderr.trim()
            ));
        }
        Ok(())
    }

    /// Fetch lease state (`GET /v1/{lease_resource}`). Returns `Ok(None)` on HTTP 404.
    pub fn get_lease(
        &self,
        parent: &str,
        lease_id_or_name: &str,
    ) -> Result<Option<SylphxLease>, String> {
        let resource = self.resolve_resource(parent, "leases", lease_id_or_name);
        let url = self.v1_resource_url(&resource);
        let (status, val) = self.request_json("GET", &url, None, 20_000)?;
        if status == 404 {
            return Ok(None);
        }
        if !(200..300).contains(&status) {
            return Err(format!(
                "Sylphx GET {url} returned HTTP {status}: {}",
                compact_error_body(&val)
            ));
        }
        Ok(Some(parse_lease_response(&val, parent)?))
    }

    /// List sandbox leases under `parent` (`GET /v1/{parent}/leases`), following pagination
    /// up to 10 pages so callers can recover active leases holding a persistent volume.
    pub fn list_leases(&self, parent: &str) -> Result<Vec<SylphxLease>, String> {
        let base_url = self.collection_url(parent, "leases");
        let mut leases = Vec::new();
        let mut page_token: Option<String> = None;
        for _ in 0..10 {
            let url = match page_token.as_deref().filter(|t| !t.is_empty()) {
                Some(tok) => format!("{base_url}?page_token={}", url_encode_query(tok)),
                None => base_url.clone(),
            };
            let (status, val) = self.request_json("GET", &url, None, 20_000)?;
            if !(200..300).contains(&status) {
                return Err(format!(
                    "Sylphx GET {url} returned HTTP {status}: {}",
                    compact_error_body(&val)
                ));
            }
            let items = val
                .get("leases")
                .or_else(|| val.get("items"))
                .or_else(|| val.get("data"))
                .and_then(serde_json::Value::as_array)
                .or_else(|| val.as_array());
            if let Some(arr) = items {
                for item in arr {
                    if let Ok(parsed) = parse_lease_response(item, parent) {
                        leases.push(parsed);
                    }
                }
            }
            let next = val
                .get("next_page_token")
                .or_else(|| val.get("nextPageToken"))
                .or_else(|| val.get("next_cursor"))
                .or_else(|| val.get("nextCursor"))
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            if next.is_none() || next == page_token {
                break;
            }
            page_token = next;
        }
        Ok(leases)
    }

    /// Find an existing non-terminal lease already attaching `volume_resource` (preferring
    /// `READY` leases over still-booting ones).
    pub fn find_active_lease_for_volume(
        &self,
        parent: &str,
        volume_resource: &str,
    ) -> Result<Option<SylphxLease>, String> {
        let leases = self.list_leases(parent)?;
        let mut matching: Vec<SylphxLease> = leases
            .into_iter()
            .filter(|l| !l.is_terminal() && l.attaches_volume(volume_resource))
            .collect();
        if matching.is_empty() {
            return Ok(None);
        }
        // Prefer ready leases first; break ties by latest expires_at.
        matching.sort_by(|a, b| {
            b.is_ready()
                .cmp(&a.is_ready())
                .then_with(|| b.expires_at.cmp(&a.expires_at))
        });
        Ok(matching.into_iter().next())
    }

    /// Poll `GET /v1/{lease_resource}` until the lease transitions to `READY`.
    pub fn wait_lease_ready(
        &self,
        parent: &str,
        lease: SylphxLease,
        timeout: Duration,
    ) -> Result<SylphxLease, String> {
        if lease.is_ready() {
            return Ok(lease);
        }
        if lease.is_terminal() {
            return Err(format!(
                "Sylphx lease {} entered terminal state `{}`",
                lease.resource_name, lease.state
            ));
        }
        let deadline = Instant::now() + timeout;
        let resource = lease.resource_name.clone();
        let mut poll_delay = LEASE_READY_POLL_INITIAL;
        loop {
            if Instant::now() >= deadline {
                return Err(format!(
                    "timed out waiting for Sylphx lease {resource} to become READY"
                ));
            }
            std::thread::sleep(poll_delay);
            poll_delay = (poll_delay * 2).min(LEASE_READY_POLL_INTERVAL);
            let current = self.get_lease(parent, &resource)?.ok_or_else(|| {
                format!("Sylphx lease {resource} disappeared while waiting for READY")
            })?;
            if current.is_ready() {
                return Ok(current);
            }
            if current.is_terminal() {
                return Err(format!(
                    "Sylphx lease {resource} entered terminal state `{}`",
                    current.state
                ));
            }
        }
    }

    /// Renew an active lease TTL (`POST /v1/{lease_resource}:renew`).
    pub fn renew_lease(
        &self,
        parent: &str,
        lease_id_or_name: &str,
        ttl: Option<&str>,
    ) -> Result<SylphxLease, String> {
        let resource = self.resolve_resource(parent, "leases", lease_id_or_name);
        let url = format!("{}:renew", self.v1_resource_url(&resource));
        let ttl_val = ttl
            .filter(|s| !s.trim().is_empty())
            .unwrap_or(DEFAULT_LEASE_TTL);
        let body = serde_json::json!({ "ttl": ttl_val });
        let (status, val) = self.request_json("POST", &url, Some(&body), 20_000)?;
        if !(200..300).contains(&status) {
            return Err(format!(
                "Sylphx POST {url} returned HTTP {status}: {}",
                compact_error_body(&val)
            ));
        }
        parse_lease_response(&val, parent)
    }

    /// Release an active lease on idle / pause / archive (`POST /v1/{lease_resource}:release`).
    /// Preserves the attached persistent volume.
    pub fn release_lease(&self, parent: &str, lease_id_or_name: &str) -> Result<(), String> {
        let resource = self.resolve_resource(parent, "leases", lease_id_or_name);
        let url = format!("{}:release", self.v1_resource_url(&resource));
        let (status, val) =
            self.request_json("POST", &url, Some(&serde_json::json!({})), 30_000)?;
        if status == 404 || status == 409 || status == 410 || (200..300).contains(&status) {
            return Ok(());
        }
        Err(format!(
            "Sylphx POST {url} returned HTTP {status}: {}",
            compact_error_body(&val)
        ))
    }

    /// Ensure an active, `READY` lease exists for an agent (creating its persistent volume if
    /// absent, reusing/polling an active lease, or creating a fresh lease attaching the
    /// preserved volume if previously released/expired).
    pub fn ensure_leased(
        &self,
        provider: &SylphxProviderConfig,
        config_dir: &Path,
        agent_id: &str,
        workdir: Option<&str>,
        env: &BTreeMap<String, String>,
    ) -> Result<SylphxLeaseConfig, String> {
        let existing = SylphxLeaseConfig::from_config_dir(config_dir);
        let preferred_vol_name = format!("anyagents-{}", sanitize_resource_segment(agent_id));
        let vol = self.ensure_volume(
            &provider.parent,
            existing.as_ref().map(|c| c.volume_resource.as_str()),
            &preferred_vol_name,
            provider.volume_size_gib,
        )?;

        let mut reused_lease = None;
        if let Some(prev_lease) = existing
            .as_ref()
            .and_then(|c| c.lease_resource.as_deref().or(c.lease_id.as_deref()))
            && let Some(fetched) = self.get_lease(&provider.parent, prev_lease)?
            && !fetched.is_terminal()
        {
            reused_lease =
                Some(self.wait_lease_ready(&provider.parent, fetched, LEASE_READY_TIMEOUT)?);
        }

        let (lease, freshly_created) = match reused_lease {
            Some(active) => {
                let _ = self.renew_lease(
                    &provider.parent,
                    &active.resource_name,
                    Some(DEFAULT_LEASE_TTL),
                );
                (active, false)
            }
            None => {
                let created = self.create_lease(
                    &provider.parent,
                    &vol.resource_name,
                    &provider.mount_path,
                    &provider.template,
                    &provider.shape,
                    env,
                    Some(DEFAULT_LEASE_TTL),
                )?;
                (
                    self.wait_lease_ready(&provider.parent, created, LEASE_READY_TIMEOUT)?,
                    true,
                )
            }
        };

        let resolved_workdir = workdir
            .map(|w| normalize_unix_dir(w, &provider.default_workdir))
            .unwrap_or_else(|| provider.default_workdir.clone());
        let data_plane_resource = lease
            .data_plane_resource
            .clone()
            .unwrap_or_else(|| lease.resource_name.clone());

        let state = SylphxLeaseConfig {
            endpoint: self.endpoint.clone(),
            token: self.token.clone().or_else(|| provider.token.clone()),
            parent: provider.parent.clone(),
            template: provider.template.clone(),
            shape: provider.shape.clone(),
            volume_size_gib: provider.volume_size_gib.or(vol.size_gib),
            workdir: resolved_workdir,
            mount_path: provider.mount_path.clone(),
            volume_id: vol.id.clone(),
            volume_resource: vol.resource_name.clone(),
            lease_id: Some(lease.id.clone()),
            lease_resource: Some(lease.resource_name.clone()),
            data_plane_resource: Some(data_plane_resource.clone()),
            envd_url: lease.envd_url.clone(),
            envd_token: lease.envd_token.clone(),
            expires_at: lease.expires_at.clone(),
        };
        if freshly_created {
            let _ = self.init_persistent_layout(
                &data_plane_resource,
                &state.mount_path,
                &state.state_dir(),
                &state.workdir,
            );
        }
        state.save_to_config_dir(config_dir)?;
        Ok(state)
    }

    /// Execute a command via `:exec` (`POST /v1/{resource}:exec`) as the default guest user.
    pub fn exec(
        &self,
        resource: &str,
        command: &[String],
        working_directory: Option<&str>,
        env: &BTreeMap<String, String>,
        timeout_ms: Option<u64>,
    ) -> Result<ExecOutput, String> {
        self.exec_as_user(resource, command, working_directory, env, timeout_ms, None)
    }

    /// Execute a command via `:exec` (`POST /v1/{resource}:exec`), optionally targeting a specific
    /// guest user (e.g. `Some("root")` for volume mount initialization).
    #[allow(clippy::too_many_arguments)]
    pub fn exec_as_user(
        &self,
        resource: &str,
        command: &[String],
        working_directory: Option<&str>,
        env: &BTreeMap<String, String>,
        timeout_ms: Option<u64>,
        user: Option<&str>,
    ) -> Result<ExecOutput, String> {
        if command.is_empty() {
            return Err("Sylphx `:exec` requires a non-empty command vector".to_string());
        }
        let url = format!("{}:exec", self.v1_resource_url(resource));
        let effective_timeout = timeout_ms
            .unwrap_or(DEFAULT_EXEC_TIMEOUT_MS)
            .clamp(100, REMOTE_EXEC_MAX_TIMEOUT_MS);
        let timeout_secs = effective_timeout.div_ceil(1_000).clamp(1, 1_800);
        let idempotency_key = next_exec_idempotency_key();
        let mut body = serde_json::Map::new();
        body.insert("command".into(), serde_json::json!(command));
        if let Some(wd) = working_directory.filter(|s| !s.trim().is_empty()) {
            body.insert("working_directory".into(), serde_json::json!(wd));
        }
        if !env.is_empty() {
            body.insert("env".into(), serde_json::json!(env));
        }
        if let Some(u) = user.map(str::trim).filter(|s| !s.is_empty()) {
            body.insert("user".into(), serde_json::json!(u));
        }
        body.insert(
            "timeout".into(),
            serde_json::json!(format!("{timeout_secs}s")),
        );
        body.insert("idempotency_key".into(), serde_json::json!(idempotency_key));
        let payload = serde_json::Value::Object(body);

        let deadline = Instant::now() + Duration::from_millis(effective_timeout + 15_000);
        let val = loop {
            let per_call_timeout = effective_timeout.min(35_000).saturating_add(10_000);
            let (status, val) =
                self.request_json("POST", &url, Some(&payload), per_call_timeout)?;
            if (200..300).contains(&status) {
                break val;
            }
            let code = val.get("code").and_then(serde_json::Value::as_str);
            if code == Some("IDEMPOTENCY_IN_PROGRESS") && Instant::now() < deadline {
                std::thread::sleep(EXEC_IN_PROGRESS_POLL_DELAY);
                continue;
            }
            return Err(format!(
                "Sylphx POST {url} returned HTTP {status}: {}",
                compact_error_body(&val)
            ));
        };
        let stdout = decode_output_stream(&val, &["stdout_base64", "stdoutBase64"], "stdout")?;
        let stderr = decode_output_stream(&val, &["stderr_base64", "stderrBase64"], "stderr")?;
        let exit_code = val
            .get("exit_code")
            .or_else(|| val.get("exitCode"))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0) as i32;
        Ok(ExecOutput {
            stdout,
            stderr,
            exit_code,
        })
    }

    /// Read a file via `:readFile` (`POST /v1/{resource}:readFile`).
    pub fn read_file(&self, resource: &str, path: &str) -> Result<Vec<u8>, String> {
        let url = format!("{}:readFile", self.v1_resource_url(resource));
        let body = serde_json::json!({ "path": path });
        let (status, val) = self.request_json("POST", &url, Some(&body), 30_000)?;
        if !(200..300).contains(&status) {
            return Err(format!(
                "Sylphx `:readFile` {path} returned HTTP {status}: {}",
                compact_error_body(&val)
            ));
        }
        if let Some(b64) = val
            .get("content_base64")
            .or_else(|| val.get("contentBase64"))
            .or_else(|| val.get("content"))
            .and_then(serde_json::Value::as_str)
        {
            return base64_decode(b64);
        }
        if let Some(text) = val.get("data").and_then(serde_json::Value::as_str) {
            return Ok(text.as_bytes().to_vec());
        }
        if val.is_object() {
            return Ok(Vec::new());
        }
        Err(format!(
            "Sylphx `:readFile` {path} returned unexpected payload: {val}"
        ))
    }

    /// Write a file via `:writeFile` (`POST /v1/{resource}:writeFile`).
    pub fn write_file(&self, resource: &str, path: &str, content: &[u8]) -> Result<(), String> {
        let url = format!("{}:writeFile", self.v1_resource_url(resource));
        let b64 = base64_encode(content);
        let body = serde_json::json!({
            "path": path,
            "content": b64,
        });
        let (status, val) = self.request_json("POST", &url, Some(&body), 45_000)?;
        if !(200..300).contains(&status) {
            return Err(format!(
                "Sylphx `:writeFile` {path} returned HTTP {status}: {}",
                compact_error_body(&val)
            ));
        }
        Ok(())
    }

    /// List directory entries via `:listFiles` (`POST /v1/{resource}:listFiles`).
    pub fn list_files(&self, resource: &str, path: &str) -> Result<Vec<SylphxFileEntry>, String> {
        let url = format!("{}:listFiles", self.v1_resource_url(resource));
        let body = serde_json::json!({ "path": path });
        let (status, val) = self.request_json("POST", &url, Some(&body), 20_000)?;
        if !(200..300).contains(&status) {
            return Err(format!(
                "Sylphx `:listFiles` {path} returned HTTP {status}: {}",
                compact_error_body(&val)
            ));
        }
        let empty_arr = Vec::new();
        let arr: &[serde_json::Value] =
            if let Some(raw_entries) = val.get("entries").or_else(|| val.get("files")) {
                if raw_entries.is_null() {
                    &empty_arr
                } else {
                    raw_entries.as_array().ok_or_else(|| {
                        format!("Sylphx `:listFiles` {path} `entries` is not an array")
                    })?
                }
            } else if val.is_object() {
                // Proto3 JSON omits empty repeated fields (returning `{}` for an empty directory).
                &empty_arr
            } else {
                return Err(format!(
                    "Sylphx `:listFiles` {path} missing `entries` array"
                ));
            };
        let mut entries = Vec::with_capacity(arr.len());
        for item in arr {
            let name = item
                .get("name")
                .and_then(serde_json::Value::as_str)
                .or_else(|| item.get("path").and_then(serde_json::Value::as_str))
                .unwrap_or("")
                .trim_matches('/')
                .rsplit('/')
                .next()
                .unwrap_or("")
                .to_string();
            if name.is_empty() {
                continue;
            }
            let entry_path = item
                .get("path")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| {
                    if path == "/" {
                        format!("/{name}")
                    } else {
                        format!("{}/{}", path.trim_end_matches('/'), name)
                    }
                });
            let is_dir = item
                .get("directory")
                .or_else(|| item.get("is_dir"))
                .or_else(|| item.get("isDir"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or_else(|| {
                    item.get("type")
                        .or_else(|| item.get("kind"))
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|k| {
                            k.eq_ignore_ascii_case("dir") || k.eq_ignore_ascii_case("directory")
                        })
                        || item
                            .get("mode")
                            .and_then(serde_json::Value::as_u64)
                            .is_some_and(|m| (m & 0o170000) == 0o040000)
                });
            let size_bytes = item
                .get("size_bytes")
                .or_else(|| item.get("sizeBytes"))
                .or_else(|| item.get("size"))
                .or_else(|| item.get("bytes"))
                .and_then(|v| {
                    v.as_u64()
                        .or_else(|| v.as_str().and_then(|s| s.trim().parse::<u64>().ok()))
                });
            entries.push(SylphxFileEntry {
                name,
                path: entry_path,
                is_dir,
                size_bytes,
            });
        }
        Ok(entries)
    }

    /// Start a bidirectional streaming process via `envd` Connect-RPC (`process.Process/Start`).
    ///
    /// Reads Connect-RPC 5-byte server-stream frames incrementally without a global timeout
    /// (preventing 120s/300s truncation) and invokes `on_event` for each frame as it arrives.
    #[allow(clippy::too_many_arguments)]
    pub fn start_envd_stream<F>(
        &self,
        envd_base: &str,
        envd_token: Option<&str>,
        cmd: &str,
        args: &[String],
        cwd: Option<&str>,
        env: &BTreeMap<String, String>,
        stdin_enabled: bool,
        mut on_event: F,
    ) -> Result<Option<i32>, String>
    where
        F: FnMut(EnvdProcessEvent),
    {
        let url = format!("{}/process.Process/Start", envd_base.trim_end_matches('/'));
        let req_json = serde_json::json!({
            "process": {
                "cmd": cmd,
                "args": args,
                "envs": env,
                "cwd": cwd.unwrap_or(DEFAULT_SANDBOX_WORKDIR),
            },
            "pty": null,
            "stdin": stdin_enabled,
        });
        let framed = encode_connect_frame(
            0x00,
            &serde_json::to_vec(&req_json).map_err(|e| e.to_string())?,
        )?;
        let agent = self.stream_agent();
        let mut req = agent
            .post(&url)
            .header("content-type", "application/connect+json")
            .header("connect-protocol-version", "1");
        if let Some(tok) = envd_token.or(self.token.as_deref()) {
            req = req
                .header("authorization", &format!("Bearer {tok}"))
                .header("x-access-token", tok);
        }
        let resp = req
            .send(framed.as_slice())
            .map_err(|err| format!("envd `process.Process/Start` request failed ({url}): {err}"))?;
        let status = resp.status().as_u16();
        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_ascii_lowercase();
        let mut reader = resp.into_body().into_reader();
        if !(200..300).contains(&status) {
            let mut err_buf = String::new();
            let _ = reader.read_to_string(&mut err_buf);
            return Err(format!(
                "envd `process.Process/Start` returned HTTP {status}: {err_buf}"
            ));
        }
        if content_type.contains("application/json") && !content_type.contains("connect+json") {
            let mut body = String::new();
            reader
                .read_to_string(&mut body)
                .map_err(|err| format!("envd stream JSON read error: {err}"))?;
            let mut last_exit = None;
            for line in body.lines().filter(|l| !l.trim().is_empty()) {
                let val: serde_json::Value = serde_json::from_str(line)
                    .map_err(|err| format!("invalid envd JSON line: {err}"))?;
                if let Some(ev) = parse_envd_event(&val) {
                    if let EnvdProcessEvent::End { exit_code, .. } = &ev {
                        last_exit = *exit_code;
                    }
                    on_event(ev);
                }
            }
            return Ok(last_exit);
        }

        let mut exit_code_seen = None;
        while let Some((flags, payload)) = read_connect_frame(&mut reader)? {
            if flags & 0x01 != 0 {
                return Err("compressed Connect-RPC envd frames are not supported".to_string());
            }
            let val: serde_json::Value = serde_json::from_slice(&payload)
                .map_err(|err| format!("invalid Connect-RPC frame JSON: {err}"))?;
            if flags & 0x02 != 0 {
                if let Some(err_obj) = val.get("error").filter(|v| !v.is_null()) {
                    let msg = err_obj
                        .get("message")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown envd stream error");
                    return Err(format!("envd Connect-RPC trailer error: {msg}"));
                }
                break;
            }
            if let Some(ev) = parse_envd_event(&val) {
                if let EnvdProcessEvent::End {
                    exit_code, error, ..
                } = &ev
                {
                    exit_code_seen = *exit_code;
                    if let Some(err_msg) = error.as_ref().filter(|s| !s.is_empty()).cloned()
                        && exit_code_seen.is_none_or(|c| c == 0)
                    {
                        on_event(ev);
                        return Err(format!("envd process failed: {err_msg}"));
                    }
                }
                on_event(ev);
            }
        }
        Ok(exit_code_seen)
    }

    /// Deliver stdin bytes to a live process via `envd` Connect-RPC (`process.Process/SendInput`).
    pub fn send_envd_input(
        &self,
        envd_base: &str,
        envd_token: Option<&str>,
        pid: u32,
        data: &[u8],
    ) -> Result<(), String> {
        let url = format!(
            "{}/process.Process/SendInput",
            envd_base.trim_end_matches('/')
        );
        let body = serde_json::json!({
            "process": { "pid": pid },
            "input": { "data": base64_encode(data) },
        });
        self.post_envd_unary(&url, envd_token, &body, "SendInput")
    }

    /// Send a POSIX signal to a live process via `envd` Connect-RPC (`process.Process/SendSignal`).
    pub fn send_envd_signal(
        &self,
        envd_base: &str,
        envd_token: Option<&str>,
        pid: u32,
        signal: EnvdSignal,
    ) -> Result<(), String> {
        let url = format!(
            "{}/process.Process/SendSignal",
            envd_base.trim_end_matches('/')
        );
        let body = serde_json::json!({
            "process": { "pid": pid },
            "signal": signal.as_proto_str(),
        });
        self.post_envd_unary(&url, envd_token, &body, "SendSignal")
    }

    fn post_envd_unary(
        &self,
        url: &str,
        envd_token: Option<&str>,
        body: &serde_json::Value,
        op_name: &str,
    ) -> Result<(), String> {
        let agent = self.rest_agent(20_000);
        let payload = serde_json::to_vec(body).map_err(|e| e.to_string())?;
        let mut req = agent
            .post(url)
            .header("content-type", "application/json")
            .header("connect-protocol-version", "1");
        if let Some(tok) = envd_token.or(self.token.as_deref()) {
            req = req
                .header("authorization", &format!("Bearer {tok}"))
                .header("x-access-token", tok);
        }
        let resp = req
            .send(payload.as_slice())
            .map_err(|err| format!("envd `process.Process/{op_name}` failed: {err}"))?;
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            let raw = resp.into_body().read_to_string().unwrap_or_default();
            return Err(format!(
                "envd `process.Process/{op_name}` returned HTTP {status}: {raw}"
            ));
        }
        Ok(())
    }
}

/// Remote [`WorkspaceBackend`] backed by a native Sylphx Sandboxes v1 lease & persistent volume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SylphxSandboxWorkspace {
    pub config: SylphxLeaseConfig,
}

impl SylphxSandboxWorkspace {
    pub fn new(config: SylphxLeaseConfig) -> Self {
        Self { config }
    }
}

impl WorkspaceBackend for SylphxSandboxWorkspace {
    fn kind(&self) -> &'static str {
        "sylphx-sandbox"
    }

    fn root_display(&self) -> String {
        self.config.workdir.clone()
    }

    fn base_path(&self) -> &Path {
        Path::new(&self.config.workdir)
    }

    fn exec(
        &self,
        command: &str,
        cwd: Option<&str>,
        timeout_ms: Option<u64>,
    ) -> Result<ExecOutput, String> {
        let cmd_trimmed = command.trim();
        if cmd_trimmed.is_empty() {
            return Err("`workspace.exec` requires a non-empty `cmd`".to_string());
        }
        let resource = self.config.active_data_resource()?;
        let resolved_cwd = self.resolve_path(cwd, false)?;
        let argv = vec![
            "/bin/sh".to_string(),
            "-lc".to_string(),
            cmd_trimmed.to_string(),
        ];
        self.config.client().exec(
            resource,
            &argv,
            Some(&resolved_cwd),
            &BTreeMap::new(),
            timeout_ms,
        )
    }

    fn read_file_bytes(&self, path: Option<&str>) -> Result<(String, Vec<u8>), String> {
        let resolved = self.resolve_path(path, true)?;
        let resource = self.config.active_data_resource()?;
        let bytes = self.config.client().read_file(resource, &resolved)?;
        Ok((resolved, bytes))
    }

    fn write_file_bytes(
        &self,
        path: Option<&str>,
        content: &[u8],
        append: bool,
        overwrite: bool,
    ) -> Result<(String, usize, String), String> {
        let resolved = self.resolve_path(path, true)?;
        let resource = self.config.active_data_resource()?;
        let client = self.config.client();

        let final_bytes = if append {
            let mut merged = client.read_file(resource, &resolved).unwrap_or_default();
            merged.extend_from_slice(content);
            merged
        } else {
            if !overwrite && client.read_file(resource, &resolved).is_ok() {
                return Err(format!(
                    "refusing to overwrite existing remote file {resolved} (pass overwrite: true)"
                ));
            }
            content.to_vec()
        };

        if let Some(parent) = Path::new(&resolved).parent().and_then(|p| p.to_str())
            && !parent.is_empty()
            && parent != "/"
        {
            let _ = client.exec(
                resource,
                &["mkdir".to_string(), "-p".to_string(), parent.to_string()],
                Some(&self.config.workdir),
                &BTreeMap::new(),
                Some(15_000),
            );
        }

        client.write_file(resource, &resolved, &final_bytes)?;
        let sha = sha256_hex(&final_bytes);
        Ok((resolved, final_bytes.len(), sha))
    }

    fn list_dir(
        &self,
        path: Option<&str>,
        include_hidden: bool,
        max_results: usize,
    ) -> Result<serde_json::Value, String> {
        let resolved = self.resolve_path(path, false)?;
        let resource = self.config.active_data_resource()?;
        let mut entries = self.config.client().list_files(resource, &resolved)?;
        if !include_hidden {
            entries.retain(|e| !e.name.starts_with('.'));
        }
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        let truncated = entries.len() > max_results;
        entries.truncate(max_results);
        let json_entries: Vec<serde_json::Value> = entries
            .iter()
            .map(|entry| {
                serde_json::json!({
                    "name": entry.name,
                    "path": entry.path,
                    "kind": if entry.is_dir { "dir" } else { "file" },
                    "bytes": entry.size_bytes.unwrap_or(0),
                })
            })
            .collect();
        let count = json_entries.len();
        Ok(serde_json::json!({
            "action": "list",
            "path": resolved,
            "entries": json_entries,
            "count": count,
            "truncated": truncated,
        }))
    }
}

/// Resolve a [`WorkspaceBackend`] enforcing [`SandboxMode`]: fails closed under
/// [`SandboxMode::RemoteRequired`] when no remote sandbox lease/config exists.
pub fn resolve_workspace_backend_for_mode(
    config_dir: Option<&Path>,
    local_root: PathBuf,
    mode: SandboxMode,
) -> Result<Box<dyn WorkspaceBackend>, String> {
    if let Some(dir) = config_dir
        && let Some(sylphx_cfg) = SylphxLeaseConfig::from_config_dir(dir)
    {
        return Ok(Box::new(SylphxSandboxWorkspace::new(sylphx_cfg)));
    }
    if let Some(cfg) = super::resolve_sandbox_config(config_dir) {
        if let Some(rest) = cfg.endpoint.strip_prefix("docker://") {
            let docker_host = (!rest.is_empty() && rest != "local").then(|| rest.to_string());
            return Ok(Box::new(DockerWorkspace {
                container: cfg.sandbox_id,
                workdir: PathBuf::from(cfg.workdir),
                docker_host,
                docker_bin: anyagents_toolchain::tool("docker", "sandbox.docker")
                    .ok()
                    .map(|t| t.path().to_path_buf()),
            }));
        }
        return Ok(Box::new(RemoteSandboxWorkspace::new(cfg)));
    }
    if mode.is_remote_required() {
        return Err(
            "remote sandbox required (`ANYAGENTS_SANDBOX_MODE=remote_required`), \
             but agent has no active sandbox lease or configuration; refusing host fallback"
                .to_string(),
        );
    }
    Ok(Box::new(LocalWorkspace::new(local_root)))
}

/// Ensure an active Sylphx sandbox lease (with persistent volume attached) for `<config_dir>`,
/// returning `Ok(Some(lease_config))` when Sylphx driver/state applies, or `Ok(None)` if not Sylphx.
pub fn ensure_leased_for_config_dir(
    data_dir: Option<&Path>,
    config_dir: &Path,
    agent_id: &str,
    workdir: Option<&str>,
    env: &BTreeMap<String, String>,
) -> Result<Option<SylphxLeaseConfig>, String> {
    let provider = match SylphxProviderConfig::resolve(data_dir)? {
        Some(p) => p,
        None => match SylphxLeaseConfig::from_config_dir(config_dir) {
            Some(prev) => SylphxProviderConfig {
                driver: SandboxDriver::Sylphx,
                endpoint: prev.endpoint.clone(),
                token: prev.token.clone(),
                parent: prev.parent.clone(),
                template: prev.template.clone(),
                shape: prev.shape.clone(),
                volume_size_gib: prev.volume_size_gib,
                default_workdir: prev.workdir.clone(),
                mount_path: prev.mount_path.clone(),
            },
            None => return Ok(None),
        },
    };
    let client = SylphxSandboxClient::new(provider.endpoint.clone(), provider.token.clone());
    let leased = client.ensure_leased(&provider, config_dir, agent_id, workdir, env)?;
    Ok(Some(leased))
}

/// Renew an active Sylphx sandbox lease stored under `<config_dir>`.
/// Returns `Ok(true)` if renewed, `Ok(false)` if not a Sylphx sandbox or no lease active.
pub fn renew_lease_for_config_dir(config_dir: &Path) -> Result<bool, String> {
    let Some(mut cfg) = SylphxLeaseConfig::from_config_dir(config_dir) else {
        return Ok(false);
    };
    let Some(lease_ref) = cfg
        .lease_resource
        .as_deref()
        .or(cfg.lease_id.as_deref())
        .map(str::to_string)
    else {
        return Ok(false);
    };
    let renewed = cfg
        .client()
        .renew_lease(&cfg.parent, &lease_ref, Some(DEFAULT_LEASE_TTL))?;
    cfg.expires_at = renewed.expires_at;
    if let Some(url) = renewed.envd_url {
        cfg.envd_url = Some(url);
    }
    cfg.save_to_config_dir(config_dir)?;
    Ok(true)
}

/// Release an agent's Sylphx compute lease (`:release`) while preserving its persistent volume,
/// OR call legacy `SandboxProvider::pause` if using `legacy_http`.
/// Never calls unimplemented `:pause`/`:resume` on `sylphx` sandboxes.
pub fn release_sandbox_for_config_dir(config_dir: &Path) -> Result<(), String> {
    if let Some(mut cfg) = SylphxLeaseConfig::from_config_dir(config_dir) {
        if let Some(lease_ref) = cfg
            .lease_resource
            .as_deref()
            .or(cfg.lease_id.as_deref())
            .map(str::to_string)
        {
            cfg.client().release_lease(&cfg.parent, &lease_ref)?;
        }
        cfg.lease_id = None;
        cfg.lease_resource = None;
        cfg.data_plane_resource = None;
        cfg.envd_url = None;
        cfg.envd_token = None;
        cfg.expires_at = None;
        // Persist cleared lease metadata while retaining volume_id + volume_resource
        // and keep `sandbox.json` marker indicating `remote_spawn: true`.
        cfg.save_to_config_dir(config_dir)?;
        return Ok(());
    }
    if let Some(sbx) = super::sandbox_config_from_config_dir(config_dir) {
        return agent_tools::SandboxProvider::pause(&sbx);
    }
    Ok(())
}

/// Permanently release any active lease and delete the persistent volume for `<config_dir>`.
pub fn destroy_sandbox_for_config_dir(config_dir: &Path) -> Result<(), String> {
    if let Some(cfg) = SylphxLeaseConfig::from_config_dir(config_dir) {
        let client = cfg.client();
        if let Some(lease_ref) = cfg.lease_resource.as_deref().or(cfg.lease_id.as_deref()) {
            let _ = client.release_lease(&cfg.parent, lease_ref);
        }
        if !cfg.volume_resource.trim().is_empty() {
            client.delete_volume(&cfg.parent, &cfg.volume_resource)?;
        }
        let _ = std::fs::remove_file(config_dir.join(SYLPHX_STATE_FILE));
        let _ = std::fs::remove_file(config_dir.join("sandbox.json"));
        return Ok(());
    }
    if let Some(sbx) = super::sandbox_config_from_config_dir(config_dir) {
        return agent_tools::SandboxProvider::destroy(&sbx);
    }
    Ok(())
}

fn load_raw_provider_file(data_dir: Option<&Path>) -> Option<RawProviderFile> {
    let mut candidates = Vec::new();
    if let Some(p) = env_non_empty("ANYAGENTS_SANDBOX_PROVIDER_CONFIG") {
        candidates.push(PathBuf::from(p));
    }
    if let Some(dir) = data_dir {
        candidates.push(dir.join("sandbox-provider.json"));
    }
    if let Some(home) = env_non_empty("HOME") {
        candidates.push(PathBuf::from(home).join(".config/anyagents/sandbox-provider.json"));
    }
    for path in candidates {
        if let Ok(raw) = std::fs::read_to_string(&path)
            && let Ok(parsed) = serde_json::from_str::<RawProviderFile>(&raw)
        {
            return Some(parsed);
        }
    }
    // Fallback: check standard SandboxProviderConfig so `token`/`endpoint`/`default_workdir`
    // align even if tests set `ANYAGENTS_SANDBOX_DRIVER=sylphx` with an existing provider JSON.
    let _legacy: Option<SandboxProviderConfig> = None;
    None
}

fn env_non_empty(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn normalize_api_base(raw: &str) -> String {
    let trimmed = raw.trim().trim_end_matches('/');
    trimmed.strip_suffix("/v1").unwrap_or(trimmed).to_string()
}

fn normalize_parent(raw: &str) -> String {
    raw.trim().trim_matches('/').to_string()
}

fn normalize_unix_dir(raw: &str, default: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return default.to_string();
    }
    let no_trail = trimmed.trim_end_matches('/');
    if no_trail.is_empty() {
        "/".to_string()
    } else {
        no_trail.to_string()
    }
}

fn sanitize_resource_segment(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('-');
        }
    }
    let trimmed = out.trim_matches('-');
    if trimmed.is_empty() {
        "agent".to_string()
    } else {
        trimmed.chars().take(48).collect()
    }
}

/// Normalize a caller-supplied volume/lease ID to match `^[a-z]([a-z0-9_-]{0,61}[a-z0-9])?$`.
fn normalize_resource_id(raw: &str) -> String {
    let seg = sanitize_resource_segment(raw);
    let prefixed = if seg.as_bytes().first().is_some_and(u8::is_ascii_lowercase) {
        seg
    } else {
        format!("v-{seg}")
    };
    let clipped: String = prefixed.chars().take(63).collect();
    let trimmed = clipped.trim_end_matches(['-', '_']);
    if trimmed.is_empty() {
        "vol-agent".to_string()
    } else {
        trimmed.to_string()
    }
}

fn next_exec_idempotency_key() -> String {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let seq = EXEC_IDEMPOTENCY_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("aa-exec-{}-{now_ms}-{seq}", std::process::id())
}

fn write_private_file(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let tmp = path.with_extension(format!("tmp-{}", std::process::id()));
    std::fs::write(&tmp, bytes).map_err(|err| format!("write {}: {err}", tmp.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600));
    }
    std::fs::rename(&tmp, path).map_err(|err| format!("rename {}: {err}", path.display()))
}

fn sylphx_network_json(policy: &super::providers::NetworkPolicySpec) -> serde_json::Value {
    match policy {
        super::providers::NetworkPolicySpec::Allow => serde_json::json!({
            "egress": "allow",
        }),
        super::providers::NetworkPolicySpec::Deny => serde_json::json!({
            "egress": "deny",
        }),
        super::providers::NetworkPolicySpec::Allowlist(entries) => {
            let (domains, cidrs) = super::providers::split_allowlist_domains_and_cidrs(entries);
            let mut obj = serde_json::Map::new();
            obj.insert("egress".into(), serde_json::json!("allowlist"));
            if !domains.is_empty() {
                obj.insert("allowed_domains".into(), serde_json::json!(domains));
            }
            if !cidrs.is_empty() {
                obj.insert("allowed_cidrs".into(), serde_json::json!(cidrs));
            }
            serde_json::Value::Object(obj)
        }
    }
}

fn compact_error_body(val: &serde_json::Value) -> String {
    val.get("error")
        .or_else(|| val.get("message"))
        .or_else(|| val.get("raw"))
        .and_then(|v| {
            v.as_str()
                .map(str::to_string)
                .or_else(|| Some(v.to_string()))
        })
        .unwrap_or_else(|| val.to_string())
}

fn parse_volume_response(
    val: &serde_json::Value,
    parent: &str,
    fallback_name: &str,
    fallback_size_gib: Option<u64>,
) -> Result<SylphxVolume, String> {
    let root = val.get("volume").unwrap_or(val);
    let raw_name = root
        .get("name")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let raw_id = root
        .get("volume_id")
        .or_else(|| root.get("volumeId"))
        .or_else(|| root.get("id"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let resource_name = match (raw_name, raw_id) {
        (Some(n), _) if n.contains('/') => n.to_string(),
        (_, Some(id)) if id.contains('/') => id.to_string(),
        (_, Some(id)) => format!("{}/volumes/{id}", normalize_parent(parent)),
        (Some(n), None) => format!("{}/volumes/{n}", normalize_parent(parent)),
        (None, None) => {
            return Err(format!("Sylphx volume response missing name/id: {val}"));
        }
    };
    let id = raw_id.map(str::to_string).unwrap_or_else(|| {
        resource_name
            .rsplit('/')
            .next()
            .unwrap_or(fallback_name)
            .to_string()
    });
    let display_name = raw_name.unwrap_or(fallback_name).to_string();
    let size_gib = root
        .pointer("/spec/size_gib")
        .or_else(|| root.pointer("/spec/sizeGib"))
        .or_else(|| root.get("sizeGib"))
        .or_else(|| root.get("size_gib"))
        .and_then(serde_json::Value::as_u64)
        .or(fallback_size_gib);
    let state = root
        .pointer("/status/state")
        .or_else(|| root.get("state"))
        .or_else(|| root.get("status"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    Ok(SylphxVolume {
        resource_name,
        id,
        name: display_name,
        size_gib,
        state,
    })
}

fn parse_lease_response(val: &serde_json::Value, parent: &str) -> Result<SylphxLease, String> {
    let root = val.get("lease").unwrap_or(val);
    let raw_name = root
        .get("name")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let raw_id = root
        .get("lease_id")
        .or_else(|| root.get("leaseId"))
        .or_else(|| root.get("id"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let resource_name = match (raw_name, raw_id) {
        (Some(n), _) if n.contains('/') => n.to_string(),
        (_, Some(id)) if id.contains('/') => id.to_string(),
        (_, Some(id)) => format!("{}/leases/{id}", normalize_parent(parent)),
        (Some(n), None) => format!("{}/leases/{n}", normalize_parent(parent)),
        (None, None) => return Err(format!("Sylphx lease response missing name/id: {val}")),
    };
    let id = raw_id
        .map(str::to_string)
        .unwrap_or_else(|| resource_name.rsplit('/').next().unwrap_or("").to_string());
    let state = root
        .pointer("/status/state")
        .or_else(|| root.get("state"))
        .or_else(|| root.get("status"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("READY")
        .to_string();
    let sandbox_id = root
        .pointer("/status/endpoints/e2b_sandbox_id")
        .or_else(|| root.pointer("/status/endpoints/e2bSandboxId"))
        .or_else(|| root.get("sandbox_id"))
        .or_else(|| root.get("sandboxId"))
        .or_else(|| root.get("sandbox"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let data_plane_resource = root
        .get("data_plane_resource")
        .or_else(|| root.get("dataPlaneResource"))
        .or_else(|| root.get("sandboxResource"))
        .or_else(|| root.get("sandbox_resource"))
        .and_then(serde_json::Value::as_str)
        .map(|s| s.trim_matches('/').to_string())
        .or_else(|| {
            sandbox_id
                .as_deref()
                .filter(|s| s.contains('/'))
                .map(|s| s.trim_matches('/').to_string())
        });
    let envd_url = root
        .pointer("/status/endpoints/guest_uri")
        .or_else(|| root.pointer("/status/endpoints/guestUri"))
        .or_else(|| root.get("envd_url"))
        .or_else(|| root.get("envdUrl"))
        .or_else(|| root.get("envd_proxy_url"))
        .or_else(|| root.get("envdProxyUrl"))
        .or_else(|| root.pointer("/urls/envd"))
        .or_else(|| root.pointer("/endpoints/envd"))
        .and_then(serde_json::Value::as_str)
        .map(|s| s.trim_end_matches('/').to_string());
    let envd_token = root
        .get("envd_token")
        .or_else(|| root.get("envdToken"))
        .or_else(|| root.get("envd_access_token"))
        .or_else(|| root.get("envdAccessToken"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let expires_at = root
        .pointer("/status/expire_time")
        .or_else(|| root.pointer("/status/expireTime"))
        .or_else(|| root.get("expires_at"))
        .or_else(|| root.get("expiresAt"))
        .or_else(|| root.get("expireTime"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let mut volumes = Vec::new();
    if let Some(arr) = root
        .pointer("/spec/volumes")
        .or_else(|| root.get("volumes"))
        .and_then(serde_json::Value::as_array)
    {
        for entry in arr {
            if let Some(vol) = entry
                .get("volume")
                .or_else(|| entry.get("name"))
                .or_else(|| entry.as_str().map(|_| entry))
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                volumes.push(vol.to_string());
            }
        }
    }
    let mut labels = BTreeMap::new();
    if let Some(obj) = root
        .pointer("/meta/labels")
        .or_else(|| root.get("labels"))
        .and_then(serde_json::Value::as_object)
    {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                labels.insert(k.clone(), s.to_string());
            }
        }
    }
    Ok(SylphxLease {
        resource_name,
        id,
        state,
        sandbox_id,
        data_plane_resource,
        envd_url,
        envd_token,
        expires_at,
        volumes,
        labels,
    })
}

fn url_encode_query(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for b in raw.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn decode_output_stream(
    val: &serde_json::Value,
    b64_keys: &[&str],
    plain_key: &str,
) -> Result<String, String> {
    for key in b64_keys {
        if let Some(b64) = val.get(*key).and_then(serde_json::Value::as_str) {
            let bytes = base64_decode(b64)?;
            return Ok(String::from_utf8_lossy(&bytes).into_owned());
        }
    }
    let Some(raw) = val.get(plain_key).and_then(serde_json::Value::as_str) else {
        return Ok(String::new());
    };
    if raw.is_empty() {
        return Ok(String::new());
    }
    let bytes = base64_decode(raw)?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

/// Encode a single Connect-RPC streaming envelope `[flags: u8][be_len: u32][payload]`.
pub fn encode_connect_frame(flags: u8, payload: &[u8]) -> Result<Vec<u8>, String> {
    let len = u32::try_from(payload.len())
        .map_err(|_| "Connect-RPC payload exceeds 4GiB limit".to_string())?;
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(flags);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

/// Decode the next Connect-RPC streaming envelope from `reader`. Returns `Ok(None)` at clean EOF.
pub fn read_connect_frame<R: std::io::Read>(
    reader: &mut R,
) -> Result<Option<(u8, Vec<u8>)>, String> {
    let mut header = [0u8; 5];
    let mut read_so_far = 0usize;
    while read_so_far < 5 {
        match reader.read(&mut header[read_so_far..]) {
            Ok(0) if read_so_far == 0 => return Ok(None),
            Ok(0) => {
                return Err("truncated Connect-RPC frame header from envd stream".to_string());
            }
            Ok(n) => read_so_far += n,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(format!("Connect-RPC header read error: {err}")),
        }
    }
    let flags = header[0];
    let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
    let mut payload = vec![0u8; len];
    reader
        .read_exact(&mut payload)
        .map_err(|err| format!("Connect-RPC payload read error (len={len}): {err}"))?;
    Ok(Some((flags, payload)))
}

fn decode_maybe_bytes_field(val: &serde_json::Value) -> Vec<u8> {
    let Some(s) = val.as_str() else {
        return Vec::new();
    };
    if s.is_empty() {
        return Vec::new();
    }
    base64_decode(s).unwrap_or_else(|_| s.as_bytes().to_vec())
}

fn parse_envd_event(val: &serde_json::Value) -> Option<EnvdProcessEvent> {
    let ev = val
        .get("event")
        .or_else(|| val.pointer("/result/event"))
        .unwrap_or(val);
    if let Some(start) = ev.get("start") {
        let pid = start
            .get("pid")
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
            })
            .unwrap_or(0) as u32;
        return Some(EnvdProcessEvent::Start { pid });
    }
    if let Some(data) = ev.get("data") {
        let stdout = data
            .get("stdout")
            .map(decode_maybe_bytes_field)
            .unwrap_or_default();
        let stderr = data
            .get("stderr")
            .map(decode_maybe_bytes_field)
            .unwrap_or_default();
        let pty = data
            .get("pty")
            .map(decode_maybe_bytes_field)
            .unwrap_or_default();
        return Some(EnvdProcessEvent::Data {
            stdout,
            stderr,
            pty,
        });
    }
    if let Some(end) = ev.get("end") {
        let exit_code = end
            .get("exitCode")
            .or_else(|| end.get("exit_code"))
            .and_then(serde_json::Value::as_i64)
            .map(|c| c as i32);
        let error = end
            .get("error")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        let status = end
            .get("status")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        return Some(EnvdProcessEvent::End {
            exit_code,
            error,
            status,
        });
    }
    if ev.get("keepalive").is_some() {
        return Some(EnvdProcessEvent::Keepalive);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn spawn_mock_http<F>(handler: F) -> (String, std::thread::JoinHandle<()>)
    where
        F: Fn(&str, &str, &[u8]) -> (u16, Vec<(String, String)>, Vec<u8>) + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handler = Arc::new(handler);
        let join = std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let h = Arc::clone(&handler);
                std::thread::spawn(move || {
                    let mut reader = BufReader::new(stream);
                    let mut request_line = String::new();
                    if reader.read_line(&mut request_line).is_err() || request_line.is_empty() {
                        return;
                    }
                    let mut parts = request_line.split_whitespace();
                    let method = parts.next().unwrap_or("GET").to_string();
                    let path = parts.next().unwrap_or("/").to_string();
                    let mut content_len = 0usize;
                    loop {
                        let mut hdr = String::new();
                        if reader.read_line(&mut hdr).is_err() {
                            return;
                        }
                        if hdr == "\r\n" || hdr == "\n" || hdr.is_empty() {
                            break;
                        }
                        if let Some((k, v)) = hdr.split_once(':')
                            && k.trim().eq_ignore_ascii_case("content-length")
                        {
                            content_len = v.trim().parse().unwrap_or(0);
                        }
                    }
                    let mut body = vec![0u8; content_len];
                    if content_len > 0 {
                        let _ = reader.read_exact(&mut body);
                    }
                    let (status, headers, resp_body) = h(&method, &path, &body);
                    let reason = match status {
                        200 => "OK",
                        201 => "Created",
                        404 => "Not Found",
                        _ => "Error",
                    };
                    let mut stream = reader.into_inner();
                    let mut head = format!(
                        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n",
                        resp_body.len()
                    );
                    for (k, v) in headers {
                        head.push_str(&format!("{k}: {v}\r\n"));
                    }
                    head.push_str("\r\n");
                    let _ = stream.write_all(head.as_bytes());
                    let _ = stream.write_all(&resp_body);
                    let _ = stream.flush();
                });
            }
        });
        (format!("http://{addr}"), join)
    }

    #[test]
    fn sandbox_mode_and_remote_required_fail_closed() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var(ENV_SANDBOX_MODE);
        }
        assert_eq!(SandboxMode::from_env(), SandboxMode::Off);
        assert!(SandboxMode::parse("auto").unwrap() == SandboxMode::Auto);
        assert!(SandboxMode::parse("remote_required").unwrap() == SandboxMode::RemoteRequired);

        let dir = tempfile::tempdir().unwrap();
        let err = resolve_workspace_backend_for_mode(
            Some(dir.path()),
            dir.path().to_path_buf(),
            SandboxMode::RemoteRequired,
        )
        .err()
        .expect("RemoteRequired must fail closed without sandbox config");
        assert!(err.contains("remote_required"), "err: {err}");
    }

    #[test]
    fn sylphx_v1_volume_lease_renew_release_and_data_plane_roundtrip() {
        let calls: Arc<Mutex<Vec<(String, serde_json::Value)>>> = Arc::new(Mutex::new(Vec::new()));
        let calls_ref = Arc::clone(&calls);
        let files: Arc<Mutex<BTreeMap<String, Vec<u8>>>> = Arc::new(Mutex::new(BTreeMap::new()));
        let files_ref = Arc::clone(&files);
        let lease_polls = Arc::new(Mutex::new(0u32));
        let lease_polls_ref = Arc::clone(&lease_polls);

        let (base_url, _srv) = spawn_mock_http(move |method, path, body| {
            let parsed: serde_json::Value =
                serde_json::from_slice(body).unwrap_or_else(|_| serde_json::json!({}));
            calls_ref
                .lock()
                .unwrap()
                .push((format!("{method} {path}"), parsed.clone()));

            let json_hdr = vec![("Content-Type".into(), "application/json".into())];
            match (method, path) {
                ("GET", "/v1/orgs/o1/projects/p1/envs/e1/volumes/anyagents-agt-1") => {
                    (404, json_hdr, br#"{"error":"not found"}"#.to_vec())
                }
                (
                    "POST",
                    "/v1/orgs/o1/projects/p1/envs/e1/volumes?volume_id=anyagents-agt-1",
                ) => {
                    assert_eq!(parsed["spec"]["size_gib"], 20);
                    (
                        201,
                        json_hdr,
                        serde_json::to_vec(&serde_json::json!({
                            "name": "orgs/o1/projects/p1/envs/e1/volumes/vol_123",
                            "spec": { "size_gib": 20 },
                            "status": { "state": "creating" }
                        }))
                        .unwrap(),
                    )
                }
                ("GET", "/v1/orgs/o1/projects/p1/envs/e1/volumes/vol_123") => (
                    200,
                    json_hdr,
                    serde_json::to_vec(&serde_json::json!({
                        "name": "orgs/o1/projects/p1/envs/e1/volumes/vol_123",
                        "spec": { "size_gib": 20 },
                        "status": { "state": "available" }
                    }))
                    .unwrap(),
                ),
                ("POST", "/v1/orgs/o1/projects/p1/envs/e1/leases") => {
                    assert_eq!(parsed["spec"]["shape"], "linux-m");
                    assert_eq!(parsed["spec"]["ttl"], "900s");
                    assert_eq!(
                        parsed["spec"]["volumes"][0]["volume"],
                        "orgs/o1/projects/p1/envs/e1/volumes/vol_123"
                    );
                    (
                        201,
                        json_hdr,
                        serde_json::to_vec(&serde_json::json!({
                            "name": "orgs/o1/projects/p1/envs/e1/leases/lease_1",
                            "status": { "state": "granted" }
                        }))
                        .unwrap(),
                    )
                }
                ("GET", "/v1/orgs/o1/projects/p1/envs/e1/leases/lease_1") => {
                    let mut count = lease_polls_ref.lock().unwrap();
                    *count += 1;
                    let st = if *count >= 1 { "ready" } else { "granted" };
                    (
                        200,
                        json_hdr,
                        serde_json::to_vec(&serde_json::json!({
                            "name": "orgs/o1/projects/p1/envs/e1/leases/lease_1",
                            "status": {
                                "state": st,
                                "expire_time": "2026-11-19T20:00:00Z"
                            }
                        }))
                        .unwrap(),
                    )
                }
                ("POST", "/v1/orgs/o1/projects/p1/envs/e1/leases/lease_1:renew") => (
                    200,
                    json_hdr,
                    serde_json::to_vec(&serde_json::json!({
                        "name": "orgs/o1/projects/p1/envs/e1/leases/lease_1",
                        "status": {
                            "state": "ready",
                            "expire_time": "2026-11-20T20:00:00Z"
                        }
                    }))
                    .unwrap(),
                ),
                ("POST", "/v1/orgs/o1/projects/p1/envs/e1/leases/lease_1:exec") => {
                    assert!(
                        parsed["idempotency_key"]
                            .as_str()
                            .is_some_and(|s| !s.is_empty())
                    );
                    assert!(parsed["timeout"].as_str().is_some_and(|s| s.ends_with('s')));
                    (
                        200,
                        json_hdr,
                        serde_json::to_vec(&serde_json::json!({
                            "stdout": base64_encode(b"hello sylphx\n"),
                            "stderr": base64_encode(b""),
                            "exit_code": 0
                        }))
                        .unwrap(),
                    )
                }
                ("POST", "/v1/orgs/o1/projects/p1/envs/e1/leases/lease_1:writeFile") => {
                    let p = parsed["path"].as_str().unwrap().to_string();
                    let b64 = parsed["content"].as_str().unwrap();
                    let bytes = base64_decode(b64).unwrap();
                    files_ref.lock().unwrap().insert(p, bytes);
                    (200, json_hdr, br#"{}"#.to_vec())
                }
                ("POST", "/v1/orgs/o1/projects/p1/envs/e1/leases/lease_1:readFile") => {
                    let p = parsed["path"].as_str().unwrap();
                    if let Some(bytes) = files_ref.lock().unwrap().get(p).cloned() {
                        (
                            200,
                            json_hdr,
                            serde_json::to_vec(&serde_json::json!({
                                "content": base64_encode(&bytes),
                            }))
                            .unwrap(),
                        )
                    } else {
                        (404, json_hdr, br#"{"error":"enoent"}"#.to_vec())
                    }
                }
                ("POST", "/v1/orgs/o1/projects/p1/envs/e1/leases/lease_1:listFiles") => (
                    200,
                    json_hdr,
                    serde_json::to_vec(&serde_json::json!({
                        "entries": [
                            { "path": "/home/user/workspace/src", "directory": true },
                            { "path": "/home/user/workspace/note.txt", "directory": false, "size_bytes": 5 }
                        ]
                    }))
                    .unwrap(),
                ),
                ("POST", "/v1/orgs/o1/projects/p1/envs/e1/leases/lease_1:release") => (
                    200,
                    json_hdr,
                    serde_json::to_vec(&serde_json::json!({
                        "name": "orgs/o1/projects/p1/envs/e1/leases/lease_1",
                        "status": { "state": "ended" }
                    }))
                    .unwrap(),
                ),
                ("DELETE", "/v1/orgs/o1/projects/p1/envs/e1/volumes/vol_123") => {
                    (200, json_hdr, br#"{"deleted":true}"#.to_vec())
                }
                _ => (404, json_hdr, br#"{"error":"unmocked route"}"#.to_vec()),
            }
        });

        let provider = SylphxProviderConfig {
            driver: SandboxDriver::Sylphx,
            endpoint: base_url.clone(),
            token: Some("sbx-secret".into()),
            parent: "orgs/o1/projects/p1/envs/e1".into(),
            template: "agent".into(),
            shape: "linux-m".into(),
            volume_size_gib: Some(20),
            default_workdir: "/home/user/workspace".into(),
            mount_path: "/home/user/agent-state".into(),
        };
        let dir = tempfile::tempdir().unwrap();
        let mut env = BTreeMap::new();
        env.insert("FOO".into(), "bar".into());

        let client = SylphxSandboxClient::new(base_url.clone(), Some("sbx-secret".into()));
        let leased = client
            .ensure_leased(&provider, dir.path(), "agt_1", None, &env)
            .unwrap();
        assert_eq!(leased.volume_id, "vol_123");
        assert_eq!(leased.lease_id.as_deref(), Some("lease_1"));
        assert!(dir.path().join("sandbox.json").exists());

        // Test WorkspaceBackend trait over SylphxSandboxWorkspace.
        let ws = resolve_workspace_backend_for_mode(
            Some(dir.path()),
            dir.path().to_path_buf(),
            SandboxMode::RemoteRequired,
        )
        .unwrap();
        assert_eq!(ws.kind(), "sylphx-sandbox");
        assert!(ws.is_remote());
        ws.write_file_bytes(Some("note.txt"), b"hello", false, true)
            .unwrap();
        assert_eq!(ws.read_file_bytes(Some("note.txt")).unwrap().1, b"hello");
        let listed = ws.list_dir(None, false, 50).unwrap();
        assert_eq!(listed["count"], 2);
        let out = ws.exec("echo hi", None, None).unwrap();
        assert_eq!(out.stdout, "hello sylphx\n");
        assert_eq!(out.exit_code, 0);

        // Renew, release (never :pause!), and destroy.
        assert!(renew_lease_for_config_dir(dir.path()).unwrap());
        release_sandbox_for_config_dir(dir.path()).unwrap();
        let after_release = SylphxLeaseConfig::from_config_dir(dir.path()).unwrap();
        assert!(after_release.lease_id.is_none(), "lease cleared on release");
        assert_eq!(
            after_release.volume_id, "vol_123",
            "volume preserved across release"
        );

        destroy_sandbox_for_config_dir(dir.path()).unwrap();
        let recorded = calls.lock().unwrap();
        assert!(recorded.iter().all(|(route, _)| !route.contains(":pause")));
        assert!(recorded.iter().all(|(route, _)| !route.contains(":resume")));
        assert!(
            recorded
                .iter()
                .any(|(route, _)| route.ends_with(":release"))
        );
        assert!(
            recorded
                .iter()
                .any(|(route, _)| route.starts_with("DELETE "))
        );
    }

    #[test]
    fn envd_connect_rpc_streaming_and_unary_control_work() {
        let events: Arc<Mutex<Vec<(String, serde_json::Value)>>> = Arc::new(Mutex::new(Vec::new()));
        let events_ref = Arc::clone(&events);

        let (base_url, _srv) = spawn_mock_http(move |method, path, body| {
            if path.ends_with("/process.Process/Start") {
                let (flags, frame) = read_connect_frame(&mut std::io::Cursor::new(body))
                    .unwrap()
                    .unwrap();
                assert_eq!(flags, 0x00);
                let req: serde_json::Value = serde_json::from_slice(&frame).unwrap();
                events_ref.lock().unwrap().push((path.to_string(), req));

                let mut resp_bytes = Vec::new();
                resp_bytes.extend(
                    encode_connect_frame(
                        0x00,
                        &serde_json::to_vec(&serde_json::json!({
                            "event": { "start": { "pid": 42 } }
                        }))
                        .unwrap(),
                    )
                    .unwrap(),
                );
                resp_bytes.extend(
                    encode_connect_frame(
                        0x00,
                        &serde_json::to_vec(&serde_json::json!({
                            "event": {
                                "data": {
                                    "stdout": base64_encode(b"chunk-1\n"),
                                }
                            }
                        }))
                        .unwrap(),
                    )
                    .unwrap(),
                );
                resp_bytes.extend(
                    encode_connect_frame(
                        0x00,
                        &serde_json::to_vec(&serde_json::json!({
                            "event": { "end": { "exitCode": 0 } }
                        }))
                        .unwrap(),
                    )
                    .unwrap(),
                );
                resp_bytes.extend(encode_connect_frame(0x02, b"{}").unwrap());
                (
                    200,
                    vec![("Content-Type".into(), "application/connect+json".into())],
                    resp_bytes,
                )
            } else if path.ends_with("/process.Process/SendInput")
                || path.ends_with("/process.Process/SendSignal")
            {
                let req: serde_json::Value = serde_json::from_slice(body).unwrap();
                events_ref
                    .lock()
                    .unwrap()
                    .push((format!("{method} {path}"), req));
                (
                    200,
                    vec![("Content-Type".into(), "application/json".into())],
                    br#"{}"#.to_vec(),
                )
            } else {
                (404, vec![], b"{}".to_vec())
            }
        });

        let client = SylphxSandboxClient::new(base_url.clone(), Some("tok".into()));
        let mut seen = Vec::new();
        let exit = client
            .start_envd_stream(
                &format!("{base_url}/envd"),
                Some("envd-tok"),
                "anyagents-guest",
                &["serve".into()],
                Some("/home/user/workspace"),
                &BTreeMap::new(),
                true,
                |ev| seen.push(ev),
            )
            .unwrap();
        assert_eq!(exit, Some(0));
        assert_eq!(
            seen,
            vec![
                EnvdProcessEvent::Start { pid: 42 },
                EnvdProcessEvent::Data {
                    stdout: b"chunk-1\n".to_vec(),
                    stderr: Vec::new(),
                    pty: Vec::new()
                },
                EnvdProcessEvent::End {
                    exit_code: Some(0),
                    error: None,
                    status: None
                },
            ]
        );
        client
            .send_envd_input(
                &format!("{base_url}/envd"),
                Some("envd-tok"),
                42,
                b"steer\n",
            )
            .unwrap();
        client
            .send_envd_signal(
                &format!("{base_url}/envd"),
                Some("envd-tok"),
                42,
                EnvdSignal::SigTerm,
            )
            .unwrap();

        let calls = events.lock().unwrap();
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[1].1["process"]["pid"], 42);
        assert_eq!(calls[2].1["signal"], "SIGNAL_SIGTERM");
    }

    #[test]
    fn init_persistent_layout_runs_as_root_and_chowns_mount_for_user() {
        let seen_bodies: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
        let seen_ref = Arc::clone(&seen_bodies);
        let (base_url, _server) = spawn_mock_http(move |method, path, body| {
            let parsed: serde_json::Value =
                serde_json::from_slice(body).unwrap_or_else(|_| serde_json::json!({}));
            if method == "POST" && path == "/v1/orgs/o1/projects/p1/envs/e1/leases/lease_1:exec" {
                seen_ref.lock().unwrap().push(parsed);
                return (
                    200,
                    vec![("Content-Type".into(), "application/json".into())],
                    serde_json::to_vec(&serde_json::json!({
                        "stdout": base64_encode(b""),
                        "stderr": base64_encode(b""),
                        "exit_code": 0
                    }))
                    .unwrap(),
                );
            }
            (404, vec![], br#"{"error":"not found"}"#.to_vec())
        });

        let client = SylphxSandboxClient::new(base_url, Some("tok".into()));
        client
            .init_persistent_layout(
                "orgs/o1/projects/p1/envs/e1/leases/lease_1",
                "/home/user/agent-data",
                "/home/user/agent-data/state",
                "/home/user/agent-data/workspace",
            )
            .unwrap();

        let calls = seen_bodies.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["user"], "root");
        let cmd = calls[0]["command"].as_array().unwrap();
        assert_eq!(cmd[0], "/bin/sh");
        assert_eq!(cmd[1], "-lc");
        let script = cmd[2].as_str().unwrap();
        assert!(script.contains("chown -R user:user"));
        assert!(script.contains("/home/user/agent-data"));
    }

    #[test]
    fn create_lease_retries_transient_no_capacity_503() {
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let attempts_ref = Arc::clone(&attempts);
        let (base_url, _server) = spawn_mock_http(move |method, path, _body| {
            if method == "POST" && path == "/v1/orgs/o1/projects/p1/envs/e1/leases" {
                let n = attempts_ref.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    return (
                        503,
                        vec![("Content-Type".into(), "application/json".into())],
                        serde_json::to_vec(&serde_json::json!({
                            "code": "NO_CAPACITY",
                            "retryable": true,
                            "retry_after_ms": "1000"
                        }))
                        .unwrap(),
                    );
                }
                return (
                    200,
                    vec![("Content-Type".into(), "application/json".into())],
                    serde_json::to_vec(&serde_json::json!({
                        "name": "orgs/o1/projects/p1/envs/e1/leases/lease_ok",
                        "status": { "state": "ready" }
                    }))
                    .unwrap(),
                );
            }
            (404, vec![], br#"{"error":"not found"}"#.to_vec())
        });

        let client = SylphxSandboxClient::new(base_url, Some("tok".into()));
        let lease = client
            .create_lease_with_options(
                "orgs/o1/projects/p1/envs/e1",
                "orgs/o1/projects/p1/envs/e1/volumes/vol_1",
                "/home/user/agent-data",
                Some("registry.example.com/agent@sha256:abc"),
                None,
                "linux-m",
                &BTreeMap::new(),
                Some("900s"),
                None,
            )
            .unwrap();
        assert_eq!(lease.id, "lease_ok");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn find_active_lease_for_volume_matches_ready_lease_and_ignores_terminal() {
        let (base_url, _server) = spawn_mock_http(move |method, path, _body| {
            if method == "GET" && path == "/v1/orgs/o1/projects/p1/envs/e1/leases" {
                return (
                    200,
                    vec![("Content-Type".into(), "application/json".into())],
                    serde_json::to_vec(&serde_json::json!({
                        "leases": [
                            {
                                "name": "orgs/o1/projects/p1/envs/e1/leases/lease_ended",
                                "spec": {
                                    "volumes": [{
                                        "volume": "orgs/o1/projects/p1/envs/e1/volumes/anyagents-agt-1",
                                        "mount_path": "/home/user/agent-data"
                                    }]
                                },
                                "status": { "state": "ended" }
                            },
                            {
                                "name": "orgs/o1/projects/p1/envs/e1/leases/lease_live",
                                "spec": {
                                    "volumes": [{
                                        "volume": "orgs/o1/projects/p1/envs/e1/volumes/anyagents-agt-1",
                                        "mount_path": "/home/user/agent-data"
                                    }]
                                },
                                "status": {
                                    "state": "ready",
                                    "expire_time": "2026-11-19T18:00:00Z"
                                }
                            }
                        ]
                    }))
                    .unwrap(),
                );
            }
            (404, vec![], br#"{"error":"not found"}"#.to_vec())
        });

        let client = SylphxSandboxClient::new(base_url, Some("tok".into()));
        let found = client
            .find_active_lease_for_volume(
                "orgs/o1/projects/p1/envs/e1",
                "orgs/o1/projects/p1/envs/e1/volumes/anyagents-agt-1",
            )
            .unwrap()
            .expect("expected active lease for volume");
        assert_eq!(found.id, "lease_live");
        assert!(found.attaches_volume("anyagents-agt-1"));
    }

    #[test]
    fn provider_ensure_recovers_from_409_resource_in_use_by_reusing_active_lease() {
        use super::super::providers::sylphx::{SylphxConfig, SylphxProvider};
        use super::super::providers::{
            EnsureSandboxRequest, ImageSpec, LeasePolicy, SandboxProvider, VolumeSpec,
        };

        let list_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let post_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let list_ref = Arc::clone(&list_calls);
        let post_ref = Arc::clone(&post_calls);
        let (base_url, _server) = spawn_mock_http(move |method, path, _body| {
            match (method, path) {
                ("GET", "/v1/orgs/o1/projects/p1/envs/e1/volumes/anyagents-agt-race") => (
                    200,
                    vec![("Content-Type".into(), "application/json".into())],
                    serde_json::to_vec(&serde_json::json!({
                        "name": "orgs/o1/projects/p1/envs/e1/volumes/anyagents-agt-race",
                        "status": { "state": "available" }
                    }))
                    .unwrap(),
                ),
                ("GET", "/v1/orgs/o1/projects/p1/envs/e1/leases") => {
                    let n = list_ref.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        // First check before POST sees no lease yet (simulates race with another replica).
                        (
                            200,
                            vec![("Content-Type".into(), "application/json".into())],
                            serde_json::to_vec(&serde_json::json!({ "leases": [] })).unwrap(),
                        )
                    } else {
                        // Recovery check after 409 sees the winning replica's active lease!
                        (
                            200,
                            vec![("Content-Type".into(), "application/json".into())],
                            serde_json::to_vec(&serde_json::json!({
                                "leases": [{
                                    "name": "orgs/o1/projects/p1/envs/e1/leases/lease_winner",
                                    "spec": {
                                        "volumes": [{
                                            "volume": "orgs/o1/projects/p1/envs/e1/volumes/anyagents-agt-race",
                                            "mount_path": "/home/user/agent-data"
                                        }]
                                    },
                                    "status": { "state": "ready" }
                                }]
                            }))
                            .unwrap(),
                        )
                    }
                }
                ("POST", "/v1/orgs/o1/projects/p1/envs/e1/leases") => {
                    post_ref.fetch_add(1, Ordering::SeqCst);
                    (
                        409,
                        vec![("Content-Type".into(), "application/json".into())],
                        serde_json::to_vec(&serde_json::json!({
                            "code": "RESOURCE_IN_USE",
                            "detail": "orgs/o1/projects/p1/envs/e1/volumes/anyagents-agt-race is attached to another lease."
                        }))
                        .unwrap(),
                    )
                }
                ("POST", "/v1/orgs/o1/projects/p1/envs/e1/leases/lease_winner:renew") => (
                    200,
                    vec![("Content-Type".into(), "application/json".into())],
                    serde_json::to_vec(&serde_json::json!({
                        "name": "orgs/o1/projects/p1/envs/e1/leases/lease_winner",
                        "status": { "state": "ready" }
                    }))
                    .unwrap(),
                ),
                _ => (404, vec![], br#"{"error":"not found"}"#.to_vec()),
            }
        });

        let mut cfg = SylphxConfig::new("orgs/o1/projects/p1/envs/e1");
        cfg.endpoint = base_url;
        cfg.token = Some("tok".into());
        let provider = SylphxProvider::new(cfg).unwrap();
        let req = EnsureSandboxRequest {
            agent_id: "agt_race".into(),
            agent_name: Some("race".into()),
            image: ImageSpec::default(),
            volume: Some(VolumeSpec::for_agent("agt_race")),
            shape: None,
            network: super::super::providers::NetworkPolicySpec::Allow,
            lease: LeasePolicy::default(),
            existing: None,
            workdir: None,
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let handle = rt.block_on(provider.ensure(&req)).unwrap();
        assert_eq!(handle.sandbox_id, "lease_winner");
        assert_eq!(post_calls.load(Ordering::SeqCst), 1);
        assert!(list_calls.load(Ordering::SeqCst) >= 2);
    }

    #[test]
    fn list_files_accepts_proto3_empty_object_and_string_size_bytes() {
        let base_url = spawn_mock_http(|method, path, _body| match (method, path) {
            ("POST", "/v1/orgs/o1/projects/p1/envs/e1/leases/sbx_1:listFiles") => (
                200,
                vec![("Content-Type".into(), "application/json".into())],
                b"{}".to_vec(),
            ),
            ("POST", "/v1/orgs/o1/projects/p1/envs/e1/leases/sbx_2:listFiles") => (
                200,
                vec![("Content-Type".into(), "application/json".into())],
                serde_json::to_vec(&serde_json::json!({
                    "entries": [
                        {
                            "path": "/home/user/agent-data/workspace/hello.txt",
                            "size_bytes": "27",
                            "mode": 420
                        },
                        {
                            "path": "/home/user/agent-data/workspace/subdir",
                            "size_bytes": "4096",
                            "mode": 16877
                        }
                    ]
                }))
                .unwrap(),
            ),
            _ => (404, vec![], br#"{"error":"not found"}"#.to_vec()),
        });

        let client = SylphxSandboxClient::new(base_url, Some("tok".into()));
        let empty = client
            .list_files(
                "orgs/o1/projects/p1/envs/e1/leases/sbx_1",
                "/home/user/agent-data/workspace",
            )
            .unwrap();
        assert!(empty.is_empty());

        let populated = client
            .list_files(
                "orgs/o1/projects/p1/envs/e1/leases/sbx_2",
                "/home/user/agent-data/workspace",
            )
            .unwrap();
        assert_eq!(populated.len(), 2);
        assert_eq!(populated[0].name, "hello.txt");
        assert_eq!(populated[0].size_bytes, Some(27));
        assert!(!populated[0].is_dir);
        assert_eq!(populated[1].name, "subdir");
        assert_eq!(populated[1].size_bytes, Some(4096));
        assert!(populated[1].is_dir);
    }
}
