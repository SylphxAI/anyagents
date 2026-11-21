//! Pluggable, vendor-neutral multi-provider sandbox runtime (`SandboxProvider` trait).
//!
//! Decouples persistent agent storage (`/home/user/agent-data/{state,workspace}`) from
//! ephemeral compute lifecycles across 8 first-class backends:
//! 1. [`LocalHostProvider`] (`local`, zero-config local default)
//! 2. [`SylphxProvider`] (`sylphx`, Sylphx Sandboxes v1 Volumes + Leases + `envd` + `:setNetwork`)
//! 3. [`E2bProvider`] (`e2b`, E2B Sandboxes REST API + `envd` Connect-RPC)
//! 4. [`ModalProvider`] (`modal`, Modal Sandboxes HTTP bridge with OCI images + Volumes)
//! 5. [`DaytonaProvider`] (`daytona`, Daytona Sandboxes REST API + Toolbox)
//! 6. [`DockerProvider`] (`docker`, Docker Engine CLI with named volumes & streaming tar sync)
//! 7. [`CloudflareProvider`] (`cloudflare`, Cloudflare Sandbox Worker HTTP contract)
//! 8. [`GenericHttpOciProvider`] (`http`, documented vendor-neutral HTTP JSON contract)

pub mod cloudflare;
pub mod daytona;
pub mod docker;
pub mod e2b;
pub mod http_oci;
pub mod local;
pub mod modal;
pub mod registry;
pub mod sylphx;

use std::collections::BTreeMap;
use std::fmt;
use std::io::{Cursor, Read as _, Write as _};
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde::{Deserialize, Serialize};

pub use agent_tools::ExecOutput;
#[cfg(test)]
pub use cloudflare::{CloudflareConfig, CloudflareProvider};
#[cfg(test)]
pub use daytona::{DaytonaConfig, DaytonaProvider};
#[cfg(test)]
pub use docker::{DockerConfig, DockerProvider};
#[cfg(test)]
pub use e2b::{E2bConfig, E2bProvider};
#[cfg(test)]
pub use http_oci::{GenericHttpOciConfig, GenericHttpOciProvider};
#[cfg(test)]
pub use local::LocalHostProvider;
#[cfg(test)]
pub use modal::{ModalConfig, ModalProvider};
pub use registry::{ProviderRegistry, SandboxRegistryConfig};
#[cfg(test)]
pub use sylphx::{SylphxConfig, SylphxProvider};

/// Default single persistent volume mount root inside all sandboxes.
///
/// Houses BOTH `state/` and `workspace/` so git repositories, uncommitted edits,
/// and agent state all survive compute lease release and microVM recycling.
pub const DEFAULT_MOUNT_PATH: &str = "/home/user/agent-data";
/// Default persistent agent state directory (`<mount_path>/state`).
pub const DEFAULT_STATE_DIR: &str = "/home/user/agent-data/state";
/// Default persistent working directory (`<mount_path>/workspace`).
pub const DEFAULT_WORKDIR: &str = "/home/user/agent-data/workspace";
/// Legacy state symlink path preserved for backwards compatibility.
pub const LEGACY_STATE_SYMLINK: &str = "/home/user/agent-state";
/// Legacy workspace symlink path preserved for backwards compatibility.
pub const LEGACY_WORKDIR_SYMLINK: &str = "/home/user/workspace";
/// Default warm-hold idle grace period before releasing compute (5 minutes).
pub const DEFAULT_IDLE_TTL_MS: u64 = 300_000;
/// Default safety maximum lease TTL (15 minutes; bounds orphan burn on coordinator crash).
pub const DEFAULT_MAX_LEASE_TTL_MS: u64 = 900_000;

fn default_mount_path() -> String {
    DEFAULT_MOUNT_PATH.to_string()
}

/// Supported sandbox provider identifiers (`ANYAGENTS_SANDBOX_PROVIDER`).
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    #[default]
    Local,
    Sylphx,
    E2b,
    Modal,
    Daytona,
    Docker,
    Cloudflare,
    Http,
}

impl ProviderKind {
    /// Canonical lowercase identifier for this provider.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Sylphx => "sylphx",
            Self::E2b => "e2b",
            Self::Modal => "modal",
            Self::Daytona => "daytona",
            Self::Docker => "docker",
            Self::Cloudflare => "cloudflare",
            Self::Http => "http",
        }
    }

    /// All built-in provider kinds in canonical order.
    pub const fn all() -> &'static [Self] {
        &[
            Self::Local,
            Self::Sylphx,
            Self::E2b,
            Self::Modal,
            Self::Daytona,
            Self::Docker,
            Self::Cloudflare,
            Self::Http,
        ]
    }
}

impl fmt::Display for ProviderKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ProviderKind {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "local" | "localhost" | "local_host" | "host" => Ok(Self::Local),
            "sylphx" | "sylphx-v1" | "sylphx_v1" => Ok(Self::Sylphx),
            "e2b" => Ok(Self::E2b),
            "modal" => Ok(Self::Modal),
            "daytona" => Ok(Self::Daytona),
            "docker" | "oci-docker" | "container" => Ok(Self::Docker),
            "cloudflare" | "cf" | "cloudflare_sandbox" | "cloudflare-sandbox" => {
                Ok(Self::Cloudflare)
            }
            "http" | "http_oci" | "http-oci" | "generic_http" | "generic-http"
            | "generic_http_oci" | "legacy_http" | "legacy-http" => Ok(Self::Http),
            other => Err(format!(
                "unknown sandbox provider `{other}` (expected one of: local, sylphx, e2b, modal, daytona, docker, cloudflare, http)"
            )),
        }
    }
}

/// Container image or template specification for a sandbox.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageSpec {
    /// Standard OCI image reference (preferably digest-pinned, e.g. `ghcr.io/org/sandbox@sha256:...`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// Provider-native template/snapshot alias (e.g. `"agent"`, `"base"`, or E2B template ID).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    /// Environment variables injected into the sandbox compute environment.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

impl ImageSpec {
    /// Construct an [`ImageSpec`] targeting a standard OCI image reference.
    pub fn with_image(image: impl Into<String>) -> Self {
        Self {
            image: Some(image.into()),
            template: None,
            env: BTreeMap::new(),
        }
    }

    /// Construct an [`ImageSpec`] targeting a provider template alias.
    pub fn with_template(template: impl Into<String>) -> Self {
        Self {
            image: None,
            template: Some(template.into()),
            env: BTreeMap::new(),
        }
    }

    /// Return `true` if `image` is pinned by an immutable `@sha256:<64-hex>` digest.
    pub fn is_digest_pinned(&self) -> bool {
        let Some(img) = self.image.as_deref().map(str::trim) else {
            return false;
        };
        let Some((_, digest)) = img.rsplit_once("@sha256:") else {
            return false;
        };
        digest.len() == 64
            && digest
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    }
}

/// Persistent volume specification attached to an agent's sandbox.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeSpec {
    /// Existing provider volume ID/resource or preferred deterministic volume name.
    pub id_or_name: String,
    /// Mount path inside the sandbox (defaults to `/home/user/agent-data`).
    #[serde(default = "default_mount_path")]
    pub mount_path: String,
    /// Optional persistent volume capacity in GiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_gib: Option<u32>,
}

impl VolumeSpec {
    /// Build a deterministic persistent volume spec for `agent_id` mounted at `/home/user/agent-data`.
    pub fn for_agent(agent_id: &str) -> Self {
        Self {
            id_or_name: format!("anyagents-{}", sanitize_name(agent_id)),
            mount_path: DEFAULT_MOUNT_PATH.to_string(),
            size_gib: None,
        }
    }

    /// Persistent agent state directory under this volume (`<mount_path>/state`).
    pub fn state_dir(&self) -> String {
        format!(
            "{}/state",
            normalize_unix_dir(&self.mount_path, DEFAULT_MOUNT_PATH)
        )
    }

    /// Persistent workspace directory under this volume (`<mount_path>/workspace`).
    pub fn workdir(&self) -> String {
        format!(
            "{}/workspace",
            normalize_unix_dir(&self.mount_path, DEFAULT_MOUNT_PATH)
        )
    }
}

/// Outbound network egress policy for a sandbox.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", content = "allowlist", rename_all = "snake_case")]
pub enum NetworkPolicySpec {
    /// Permit outbound internet egress (subject to provider private/metadata CIDR guards).
    #[default]
    Allow,
    /// Block all outbound network egress.
    Deny,
    /// Allow outbound egress only to the listed domains (`api.github.com`, `*.npmjs.org`) or IPv4 CIDRs.
    Allowlist(Vec<String>),
}

/// Two-tier compute lease lifecycle policy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeasePolicy {
    /// Warm-hold window in milliseconds after turn completion before releasing compute.
    ///
    /// Default is `300_000` (5 minutes) for interactive responsiveness; set `0` for batch
    /// sub-agents that should release compute immediately on idle.
    pub idle_ttl_ms: u64,
    /// Safety upper-bound TTL in milliseconds for a single compute lease.
    ///
    /// Default is `900_000` (15 minutes) so crashed coordinators never leak 24-hour VMs.
    pub max_lease_ttl_ms: u64,
}

impl Default for LeasePolicy {
    fn default() -> Self {
        Self {
            idle_ttl_ms: DEFAULT_IDLE_TTL_MS,
            max_lease_ttl_ms: DEFAULT_MAX_LEASE_TTL_MS,
        }
    }
}

impl LeasePolicy {
    /// Batch-oriented scale-to-zero policy (`idle_ttl_ms = 0`, 15-minute safety lease TTL).
    pub const fn batch() -> Self {
        Self {
            idle_ttl_ms: 0,
            max_lease_ttl_ms: DEFAULT_MAX_LEASE_TTL_MS,
        }
    }

    /// Return `max_lease_ttl_ms` rounded up to whole seconds (minimum 60 seconds).
    pub fn max_lease_ttl_secs(&self) -> u64 {
        self.max_lease_ttl_ms.div_ceil(1_000).max(60)
    }

    /// Return `max_lease_ttl_ms` formatted as a protobuf duration string (e.g. `"900s"`).
    pub fn ttl_duration_string(&self) -> String {
        format!("{}s", self.max_lease_ttl_secs())
    }
}

/// Request payload for [`SandboxProvider::ensure`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnsureSandboxRequest {
    pub agent_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub image: ImageSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume: Option<VolumeSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shape: Option<String>,
    #[serde(default)]
    pub network: NetworkPolicySpec,
    #[serde(default)]
    pub lease: LeasePolicy,
    /// Existing handle to reuse or whose preserved volume should be re-attached.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub existing: Option<SandboxHandle>,
    /// Optional working directory override (defaults to `<mount_path>/workspace`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
}

impl EnsureSandboxRequest {
    /// Create a baseline request for `agent_id` with default `/home/user/agent-data` layout and 5m/15m lease policy.
    pub fn for_agent(agent_id: impl Into<String>) -> Self {
        let agent_id = agent_id.into();
        let volume = VolumeSpec::for_agent(&agent_id);
        Self {
            agent_id,
            agent_name: None,
            image: ImageSpec::default(),
            volume: Some(volume),
            shape: None,
            network: NetworkPolicySpec::Allow,
            lease: LeasePolicy::default(),
            existing: None,
            workdir: None,
        }
    }
}

/// Active sandbox handle returned by [`SandboxProvider::ensure`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxHandle {
    pub provider: ProviderKind,
    /// Active compute lease / sandbox / container identifier.
    pub sandbox_id: String,
    /// Preserved persistent volume ID or resource name (survives `release()`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_id: Option<String>,
    /// Root mount path of the persistent volume (default `/home/user/agent-data`).
    pub mount_path: String,
    /// Persistent agent state directory (`<mount_path>/state`).
    pub state_dir: String,
    /// Persistent working directory (`<mount_path>/workspace`).
    pub workdir: String,
    /// Optional provider data-plane / `envd` / exec endpoint URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport_url: Option<String>,
    /// Optional data-plane bearer / access token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport_token: Option<String>,
    /// Optional ISO-8601 expiration timestamp for the active compute lease.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    /// Provider-specific opaque metadata (e.g. `lease_resource`, `volume_resource`, `container_name`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

/// Command execution request passed to [`SandboxProvider::exec`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecRequest {
    /// Command vector (`argv`), e.g. `["/bin/sh", "-lc", "git status"]`.
    pub command: Vec<String>,
    /// Working directory inside the sandbox (defaults to `handle.workdir`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Extra environment variables for this command.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    /// Timeout in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    /// Optional raw bytes piped to stdin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdin: Option<Vec<u8>>,
}

impl ExecRequest {
    /// Build a command request from an explicit `argv` vector.
    pub fn argv<I, S>(args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            command: args.into_iter().map(Into::into).collect(),
            cwd: None,
            env: BTreeMap::new(),
            timeout_ms: None,
            stdin: None,
        }
    }

    /// Build a `/bin/sh -lc <script>` command request.
    pub fn shell(script: impl Into<String>) -> Self {
        Self::argv(["/bin/sh".to_string(), "-lc".to_string(), script.into()])
    }

    pub fn with_cwd(mut self, cwd: impl Into<String>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }

    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }

    pub fn with_stdin(mut self, stdin: impl Into<Vec<u8>>) -> Self {
        self.stdin = Some(stdin.into());
        self
    }
}

/// Static capability flags advertised by a [`SandboxProvider`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxCapabilities {
    /// Supports booting arbitrary OCI images (`ImageSpec.image`).
    pub oci_image: bool,
    /// Supports provider-native templates or snapshots (`ImageSpec.template`).
    pub templates: bool,
    /// Supports persistent volumes attached across sessions.
    pub persistent_volumes: bool,
    /// Preserves persistent volumes (state + workspace) when `release()` frees compute.
    pub volume_survives_release: bool,
    /// Supports per-sandbox outbound network policies (`Allow` / `Deny` / `Allowlist`).
    pub network_policy: bool,
    /// Supports mutating egress policy on a running sandbox (`set_network()`).
    pub live_network_mutation: bool,
    /// Supports single-tarball `.tar.gz` bundle synchronization.
    pub tar_bundle_sync: bool,
    /// Supports bidirectional streaming exec (e.g. `envd` Connect-RPC or stdio pipes).
    pub streaming_exec: bool,
    /// Supports pause/resume or stop/start compute suspension.
    pub pause_resume: bool,
}

/// Provider readiness/health report returned by [`SandboxProvider::health`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderHealth {
    pub provider: ProviderKind,
    pub healthy: bool,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
}

/// Core async multi-provider sandbox lifecycle and execution trait.
#[async_trait]
pub trait SandboxProvider: Send + Sync {
    /// Non-secret identity of an explicitly scoped target, for lease fencing.
    fn credential_target(&self) -> Option<serde_json::Value> {
        None
    }

    fn scoped_lease_client(&self) -> Option<super::sylphx::SylphxSandboxClient> {
        None
    }

    /// Canonical provider kind.
    fn kind(&self) -> ProviderKind;

    /// Capability matrix supported by this provider.
    fn capabilities(&self) -> SandboxCapabilities;

    /// Ensure a persistent volume and running compute instance exist for the agent.
    async fn ensure(&self, req: &EnsureSandboxRequest) -> Result<SandboxHandle, String>;

    /// Execute a command inside the sandbox.
    async fn exec(&self, handle: &SandboxHandle, req: &ExecRequest) -> Result<ExecOutput, String>;

    /// Upload and extract a `.tar.gz` bundle archive into `dest_dir` in a single round-trip batch.
    async fn sync_bundle_tar(
        &self,
        handle: &SandboxHandle,
        dest_dir: &str,
        tar_gz: &[u8],
    ) -> Result<(), String>;

    /// Release compute resources on idle/pause while preserving the persistent volume.
    async fn release(&self, handle: &SandboxHandle) -> Result<(), String>;

    /// Permanently release compute and delete the agent's persistent volume.
    async fn destroy(&self, handle: &SandboxHandle) -> Result<(), String>;

    /// Probe provider control-plane / engine health.
    async fn health(&self) -> Result<ProviderHealth, String>;

    /// Mutate outbound network policy on an already-running sandbox (when supported).
    async fn set_network(
        &self,
        _handle: &SandboxHandle,
        _policy: &NetworkPolicySpec,
    ) -> Result<(), String> {
        Err(format!(
            "sandbox provider `{}` does not support live network policy mutation",
            self.kind()
        ))
    }
}

/// Pack relative `(path, bytes)` entries into a single deterministic `.tar.gz` archive.
///
/// Rejects absolute paths and `..` path-traversal segments before packing.
pub fn build_bundle_tar_gz(entries: &[(String, Vec<u8>)]) -> Result<Vec<u8>, String> {
    let mut sorted: Vec<(&str, &[u8])> = Vec::with_capacity(entries.len());
    for (rel, bytes) in entries {
        let clean = validate_relative_bundle_path(rel)?;
        sorted.push((clean, bytes.as_slice()));
    }
    sorted.sort_by(|a, b| a.0.cmp(b.0));

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(1);
    let encoder = GzEncoder::new(Vec::new(), Compression::fast());
    let mut builder = tar::Builder::new(encoder);
    for (rel, bytes) in sorted {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        let mode = if is_private_bundle_file(rel) {
            0o600
        } else {
            0o644
        };
        header.set_mode(mode);
        header.set_mtime(now_secs);
        header.set_cksum();
        builder
            .append_data(&mut header, rel, bytes)
            .map_err(|err| format!("cannot append `{rel}` to bundle tar: {err}"))?;
    }
    let encoder = builder
        .into_inner()
        .map_err(|err| format!("cannot finalize bundle tar: {err}"))?;
    encoder
        .finish()
        .map_err(|err| format!("cannot finish bundle gzip stream: {err}"))
}

/// Safely unpack a `.tar.gz` bundle archive into `dest_dir`, rejecting path traversal and links.
pub fn extract_bundle_tar_gz(tar_gz: &[u8], dest_dir: &Path) -> Result<usize, String> {
    std::fs::create_dir_all(dest_dir).map_err(|err| {
        format!(
            "cannot create bundle destination {}: {err}",
            dest_dir.display()
        )
    })?;
    let decoder = GzDecoder::new(Cursor::new(tar_gz));
    let mut archive = tar::Archive::new(decoder);
    let mut count = 0usize;
    let entries = archive
        .entries()
        .map_err(|err| format!("cannot read tar.gz bundle entries: {err}"))?;
    for entry_res in entries {
        let mut entry = entry_res.map_err(|err| format!("corrupt tar.gz bundle entry: {err}"))?;
        let entry_type = entry.header().entry_type();
        if entry_type.is_symlink() || entry_type.is_hard_link() {
            return Err("refusing symlink/hardlink inside bundle archive".to_string());
        }
        let rel_path = entry
            .path()
            .map_err(|err| format!("invalid bundle entry path: {err}"))?
            .into_owned();
        let safe_rel = sanitize_archive_rel_path(&rel_path)?;
        let target = dest_dir.join(&safe_rel);
        if entry_type.is_dir() {
            std::fs::create_dir_all(&target)
                .map_err(|err| format!("cannot create {}: {err}", target.display()))?;
            continue;
        }
        if !entry_type.is_file() {
            continue;
        }
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| format!("cannot create {}: {err}", parent.display()))?;
        }
        let mut data = Vec::new();
        entry
            .read_to_end(&mut data)
            .map_err(|err| format!("cannot read `{}` from bundle: {err}", safe_rel.display()))?;
        let mut file = std::fs::File::create(&target)
            .map_err(|err| format!("cannot write {}: {err}", target.display()))?;
        file.write_all(&data)
            .map_err(|err| format!("cannot flush {}: {err}", target.display()))?;
        #[cfg(unix)]
        if let Ok(mode) = entry.header().mode() {
            use std::os::unix::fs::PermissionsExt as _;
            let perms = mode & 0o777;
            if perms != 0 {
                let _ = std::fs::set_permissions(&target, std::fs::Permissions::from_mode(perms));
            }
        }
        count += 1;
    }
    Ok(count)
}

/// Generate an idempotent POSIX shell script creating `/home/user/agent-data/{state,workspace}`
/// and backwards-compatible symlinks `/home/user/agent-state` and `/home/user/workspace`.
pub fn persistent_layout_init_script(mount_path: &str, state_dir: &str, workdir: &str) -> String {
    let mp = shell_single_quote(mount_path);
    let sd = shell_single_quote(state_dir);
    let wd = shell_single_quote(workdir);
    let legacy_state = shell_single_quote(LEGACY_STATE_SYMLINK);
    let legacy_work = shell_single_quote(LEGACY_WORKDIR_SYMLINK);
    format!(
        "set -e; mkdir -p {mp} {sd} {wd}; \
         if [ -d /home/user ]; then \
           if [ {sd} != {legacy_state} ]; then \
             if [ -L {legacy_state} ]; then ln -sfn {sd} {legacy_state}; \
             elif [ ! -e {legacy_state} ]; then ln -s {sd} {legacy_state}; \
             elif [ -d {legacy_state} ] && [ -z \"$(ls -A {legacy_state} 2>/dev/null)\" ]; then rmdir {legacy_state} && ln -s {sd} {legacy_state}; \
             fi; \
           fi; \
           if [ {wd} != {legacy_work} ]; then \
             if [ -L {legacy_work} ]; then ln -sfn {wd} {legacy_work}; \
             elif [ ! -e {legacy_work} ]; then ln -s {wd} {legacy_work}; \
             elif [ -d {legacy_work} ] && [ -z \"$(ls -A {legacy_work} 2>/dev/null)\" ]; then rmdir {legacy_work} && ln -s {wd} {legacy_work}; \
             fi; \
           fi; \
         fi"
    )
}

/// Resolve `(mount_path, state_dir, workdir)` for an [`EnsureSandboxRequest`].
pub(crate) fn resolve_layout(req: &EnsureSandboxRequest) -> (String, String, String) {
    let mount_path = req
        .volume
        .as_ref()
        .map(|v| normalize_unix_dir(&v.mount_path, DEFAULT_MOUNT_PATH))
        .or_else(|| {
            req.existing
                .as_ref()
                .map(|h| normalize_unix_dir(&h.mount_path, DEFAULT_MOUNT_PATH))
        })
        .unwrap_or_else(|| DEFAULT_MOUNT_PATH.to_string());
    let state_dir = if mount_path == LEGACY_STATE_SYMLINK {
        LEGACY_STATE_SYMLINK.to_string()
    } else {
        format!("{mount_path}/state")
    };
    let default_wd = format!("{mount_path}/workspace");
    let workdir = req
        .workdir
        .as_deref()
        .map(|w| normalize_unix_dir(w, &default_wd))
        .or_else(|| {
            req.existing
                .as_ref()
                .map(|h| normalize_unix_dir(&h.workdir, &default_wd))
        })
        .unwrap_or(default_wd);
    (mount_path, state_dir, workdir)
}

/// Partition allowlist entries into `(allowed_domains, allowed_cidrs)`.
pub(crate) fn split_allowlist_domains_and_cidrs(entries: &[String]) -> (Vec<String>, Vec<String>) {
    let mut domains = Vec::new();
    let mut cidrs = Vec::new();
    for raw in entries {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        if is_cidr_or_ip(trimmed) {
            cidrs.push(trimmed.to_string());
        } else {
            domains.push(trimmed.to_string());
        }
    }
    (domains, cidrs)
}

fn is_cidr_or_ip(raw: &str) -> bool {
    if let Some((ip_part, prefix_part)) = raw.split_once('/') {
        ip_part.parse::<std::net::IpAddr>().is_ok() && prefix_part.parse::<u8>().is_ok()
    } else {
        raw.parse::<std::net::IpAddr>().is_ok()
    }
}

pub(crate) fn sanitize_name(raw: &str) -> String {
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

pub(crate) fn normalize_unix_dir(raw: &str, default: &str) -> String {
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

pub(crate) fn shell_single_quote(raw: &str) -> String {
    format!("'{}'", raw.replace('\'', "'\"'\"'"))
}

pub(crate) fn argv_to_shell_command(argv: &[String], env: &BTreeMap<String, String>) -> String {
    let mut parts = Vec::with_capacity(env.len() + argv.len());
    for (k, v) in env {
        if !k.is_empty() && k.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            parts.push(format!("{k}={}", shell_single_quote(v)));
        }
    }
    for arg in argv {
        parts.push(shell_single_quote(arg));
    }
    parts.join(" ")
}

pub(crate) fn build_ureq_agent(timeout_ms: u64) -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_millis(timeout_ms.max(1_000))))
        .http_status_as_error(false)
        .build()
        .new_agent()
}

pub(crate) fn http_call_bytes(
    method: &str,
    url: &str,
    headers: &[(String, String)],
    body: Option<&[u8]>,
    timeout_ms: u64,
) -> Result<(u16, Vec<u8>), String> {
    let agent = build_ureq_agent(timeout_ms);
    let resp = match method {
        "GET" => {
            let mut req = agent.get(url);
            for (k, v) in headers {
                req = req.header(k.as_str(), v.as_str());
            }
            req.call()
        }
        "DELETE" => {
            let mut req = agent.delete(url);
            for (k, v) in headers {
                req = req.header(k.as_str(), v.as_str());
            }
            req.call()
        }
        "POST" => {
            let mut req = agent.post(url);
            for (k, v) in headers {
                req = req.header(k.as_str(), v.as_str());
            }
            req.send(body.unwrap_or(&[]))
        }
        "PUT" => {
            let mut req = agent.put(url);
            for (k, v) in headers {
                req = req.header(k.as_str(), v.as_str());
            }
            req.send(body.unwrap_or(&[]))
        }
        other => return Err(format!("unsupported HTTP method `{other}`")),
    }
    .map_err(|err| format!("HTTP `{method} {url}` failed: {err}"))?;

    let status = resp.status().as_u16();
    let mut out = Vec::new();
    resp.into_body()
        .into_reader()
        .read_to_end(&mut out)
        .map_err(|err| format!("HTTP `{method} {url}` body read failed: {err}"))?;
    Ok((status, out))
}

pub(crate) fn http_call_json(
    method: &str,
    url: &str,
    headers: &[(String, String)],
    body: Option<&serde_json::Value>,
    timeout_ms: u64,
) -> Result<(u16, serde_json::Value), String> {
    let mut all_headers = headers.to_vec();
    let encoded = if let Some(val) = body {
        if !all_headers
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("content-type"))
        {
            all_headers.push(("content-type".to_string(), "application/json".to_string()));
        }
        Some(serde_json::to_vec(val).map_err(|e| e.to_string())?)
    } else {
        None
    };
    let (status, raw) = http_call_bytes(method, url, &all_headers, encoded.as_deref(), timeout_ms)?;
    if raw.is_empty() {
        return Ok((status, serde_json::json!({})));
    }
    let val = serde_json::from_slice(&raw).unwrap_or_else(
        |_| serde_json::json!({ "raw": String::from_utf8_lossy(&raw).into_owned() }),
    );
    Ok((status, val))
}

pub(crate) async fn run_blocking<F, T>(f: F) -> Result<T, String>
where
    F: FnOnce() -> Result<T, String> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|err| format!("sandbox provider worker task panicked: {err}"))?
}

fn validate_relative_bundle_path(raw: &str) -> Result<&str, String> {
    let trimmed = raw.trim().trim_start_matches("./");
    if trimmed.is_empty() {
        return Err("bundle entry path must not be empty".to_string());
    }
    let p = Path::new(trimmed);
    if p.is_absolute() {
        return Err(format!("bundle entry path `{raw}` must be relative"));
    }
    for comp in p.components() {
        match comp {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(format!(
                    "bundle entry path `{raw}` escapes destination root"
                ));
            }
        }
    }
    Ok(trimmed)
}

fn sanitize_archive_rel_path(path: &Path) -> Result<PathBuf, String> {
    if path.is_absolute() {
        return Err(format!(
            "bundle archive entry `{}` must not be absolute",
            path.display()
        ));
    }
    let mut clean = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::CurDir => {}
            Component::Normal(seg) => clean.push(seg),
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(format!(
                    "bundle archive entry `{}` escapes destination directory",
                    path.display()
                ));
            }
        }
    }
    if clean.as_os_str().is_empty() {
        return Err("bundle archive entry resolves to empty path".to_string());
    }
    Ok(clean)
}

fn is_private_bundle_file(rel: &str) -> bool {
    let lower = rel.to_ascii_lowercase();
    let basename = lower.rsplit('/').next().unwrap_or(lower.as_str());
    matches!(
        basename,
        "guest-config.json"
            | "credentials.json"
            | ".credentials.json"
            | "auth.json"
            | "key"
            | ".mcp-token"
    ) || lower.contains(".config/opencode/")
        || lower.starts_with("opencode/")
        || basename.ends_with(".key")
        || basename.ends_with("-session-id")
        || basename.contains("token")
        || basename.contains("secret")
        || basename.contains("credential")
}
