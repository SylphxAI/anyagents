//! Native Remote User-Device / Edge Node CLI (`anyagents node pair|run|status|pause|resume|revoke`).

use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyagents_core::{Error, NodeRecord, is_node_token, validate_node_capability};
use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::auth_device::{self, DeviceLoginOptions};
use crate::{client, operator, server, ui};

pub(crate) mod runner;

const NODE_CONFIG_ENV: &str = "ANYAGENTS_NODE_CONFIG";
const HTTP_TIMEOUT_SECS: u64 = 30;
const MAX_HTTP_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Args)]
pub(crate) struct NodeCliArgs {
    /// Path to node pairing config (defaults to `~/.config/anyagents/node.json`).
    #[arg(long, global = true)]
    pub(crate) config: Option<PathBuf>,
    #[command(subcommand)]
    pub(crate) action: NodeCommand,
}

#[derive(Subcommand)]
pub(crate) enum NodeCommand {
    /// Pair this device with a coordinator using OIDC Device Flow (`--scope node:connect`) or operator auth.
    Pair {
        /// Coordinator base URL (e.g. `https://agents.example.com`).
        #[arg(long)]
        url: Option<String>,
        /// Friendly name for this node (defaults to system hostname).
        #[arg(long)]
        name: Option<String>,
        /// Capabilities advertised by this node (default `exec`; extensible: `exit`, `files`, `browser`, `screen`, `gpu`).
        #[arg(long = "capability", value_name = "CAP")]
        capabilities: Vec<String>,
        /// Explicit OIDC issuer URL (auto-discovered from `--url` when omitted).
        #[arg(long)]
        issuer: Option<String>,
        /// Explicit OAuth2 client ID (auto-discovered from `--url` when omitted).
        #[arg(long = "client-id")]
        client_id: Option<String>,
        /// Explicit RFC 8707 `resource` audience URI (defaults to `--url`).
        #[arg(long)]
        resource: Option<String>,
        /// Output machine-readable JSON (never prints the secret token).
        #[arg(long)]
        json: bool,
    },
    /// Connect outbound over TLS 1.3 WebSocket (`/v1/nodes/connect`) and serve granted `exec` requests.
    Run,
    /// Show node status (local runner + coordinator state).
    Status {
        /// Coordinator base URL override.
        #[arg(long)]
        url: Option<String>,
        /// Specific node ID (`nd_...`) when querying remotely without local `node.json`.
        #[arg(long = "node-id")]
        node_id: Option<String>,
        /// Output machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Instant kill-switch: pause the node locally (killing active child process groups) and on the coordinator.
    Pause {
        /// Coordinator base URL override.
        #[arg(long)]
        url: Option<String>,
        /// Target node ID (`nd_...`, defaults to `node_id` from `node.json`).
        #[arg(long = "node-id")]
        node_id: Option<String>,
        /// Output machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Resume a paused node locally and on the coordinator.
    Resume {
        /// Coordinator base URL override.
        #[arg(long)]
        url: Option<String>,
        /// Target node ID (`nd_...`, defaults to `node_id` from `node.json`).
        #[arg(long = "node-id")]
        node_id: Option<String>,
        /// Output machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Permanently revoke a node's token and kill any active local processes.
    Revoke {
        /// Coordinator base URL override.
        #[arg(long)]
        url: Option<String>,
        /// Target node ID (`nd_...`, defaults to `node_id` from `node.json`).
        #[arg(long = "node-id")]
        node_id: Option<String>,
        /// Output machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
}

/// Device-bound credential stored strictly at `0600` in `~/.config/anyagents/node.json`.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredNodeConfig {
    pub(crate) coordinator_url: String,
    pub(crate) node_id: String,
    pub(crate) node_token: String,
    pub(crate) name: String,
    #[serde(default = "default_capabilities")]
    pub(crate) capabilities: Vec<String>,
}

fn default_capabilities() -> Vec<String> {
    vec!["exec".to_string()]
}

impl std::fmt::Debug for StoredNodeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoredNodeConfig")
            .field("coordinator_url", &self.coordinator_url)
            .field("node_id", &self.node_id)
            .field("node_token", &"<redacted>")
            .field("name", &self.name)
            .field("capabilities", &self.capabilities)
            .finish()
    }
}

pub(crate) fn default_node_config_path() -> Result<PathBuf, Error> {
    if let Ok(explicit) = std::env::var(NODE_CONFIG_ENV)
        && !explicit.trim().is_empty()
    {
        return Ok(PathBuf::from(explicit.trim()));
    }
    if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME")
        && !xdg.trim().is_empty()
    {
        return Ok(PathBuf::from(xdg.trim())
            .join("anyagents")
            .join("node.json"));
    }
    if let Ok(home) = std::env::var("HOME")
        && !home.trim().is_empty()
    {
        return Ok(PathBuf::from(home.trim())
            .join(".config")
            .join("anyagents")
            .join("node.json"));
    }
    Err(Error::Message(
        "cannot determine config directory: pass `--config <path>` or set `HOME`/`XDG_CONFIG_HOME`"
            .into(),
    ))
}

pub(crate) fn resolve_node_config_path(explicit: Option<&Path>) -> Result<PathBuf, Error> {
    match explicit {
        Some(path) => Ok(path.to_path_buf()),
        None => default_node_config_path(),
    }
}

pub(crate) fn control_socket_path(config_path: &Path) -> PathBuf {
    let stem = config_path
        .file_stem()
        .and_then(|s| s.to_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("node");
    let parent = config_path.parent().unwrap_or_else(|| Path::new("."));
    parent.join(format!("{stem}.control.sock"))
}

pub(crate) fn save_node_config_at(path: &Path, cfg: &StoredNodeConfig) -> Result<(), Error> {
    let parent = path.parent().ok_or_else(|| {
        Error::Message(format!(
            "invalid node config path (no parent): {}",
            path.display()
        ))
    })?;
    std::fs::create_dir_all(parent)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
    }
    let bytes = serde_json::to_vec_pretty(cfg)
        .map_err(|err| Error::Message(format!("failed to serialize node config: {err}")))?;
    let tmp = parent.join(format!(".node.json.tmp.{}", std::process::id()));

    #[cfg(unix)]
    {
        use std::io::Write as _;
        use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(&tmp)?;
        f.write_all(&bytes)?;
        f.write_all(b"\n")?;
        f.sync_all()?;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    {
        let mut payload = bytes;
        payload.push(b'\n');
        std::fs::write(&tmp, payload)?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

pub(crate) fn load_node_config_at(path: &Path) -> Result<Option<StoredNodeConfig>, Error> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let parsed: StoredNodeConfig = serde_json::from_slice(&bytes).map_err(|err| {
                Error::Message(format!("corrupt node config {}: {err}", path.display()))
            })?;
            if !is_node_token(&parsed.node_token) {
                return Err(Error::Message(format!(
                    "node config {} contains an invalid node token",
                    path.display()
                )));
            }
            Ok(Some(parsed))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

pub(crate) fn detect_hostname() -> String {
    if let Ok(h) = std::env::var("HOSTNAME")
        && !h.trim().is_empty()
    {
        return h.trim().to_string();
    }
    #[cfg(unix)]
    {
        let mut buf = [0u8; 256];
        let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
        if rc == 0 {
            let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
            if let Ok(s) = std::str::from_utf8(&buf[..len]) {
                let trimmed = s.trim();
                if !trimmed.is_empty() {
                    return trimmed.to_string();
                }
            }
        }
    }
    "unknown-host".to_string()
}

pub(crate) fn run(data: &Path, args: NodeCliArgs) -> Result<(), Error> {
    let config_path = resolve_node_config_path(args.config.as_deref())?;
    match args.action {
        NodeCommand::Pair {
            url,
            name,
            capabilities,
            issuer,
            client_id,
            resource,
            json: json_out,
        } => pair_cmd(
            data,
            &config_path,
            PairCmdOpts {
                url,
                name,
                capabilities,
                issuer,
                client_id,
                resource,
                json_out,
            },
        ),
        NodeCommand::Run => runner::run_daemon(&config_path),
        NodeCommand::Status {
            url,
            node_id,
            json: json_out,
        } => status_cmd(
            data,
            &config_path,
            url.as_deref(),
            node_id.as_deref(),
            json_out,
        ),
        NodeCommand::Pause {
            url,
            node_id,
            json: json_out,
        } => lifecycle_cmd(
            data,
            &config_path,
            url.as_deref(),
            node_id.as_deref(),
            "pause",
            json_out,
        ),
        NodeCommand::Resume {
            url,
            node_id,
            json: json_out,
        } => lifecycle_cmd(
            data,
            &config_path,
            url.as_deref(),
            node_id.as_deref(),
            "resume",
            json_out,
        ),
        NodeCommand::Revoke {
            url,
            node_id,
            json: json_out,
        } => lifecycle_cmd(
            data,
            &config_path,
            url.as_deref(),
            node_id.as_deref(),
            "revoke",
            json_out,
        ),
    }
}

struct PairCmdOpts {
    url: Option<String>,
    name: Option<String>,
    capabilities: Vec<String>,
    issuer: Option<String>,
    client_id: Option<String>,
    resource: Option<String>,
    json_out: bool,
}

fn parse_capabilities_flags(raw: &[String]) -> Result<Vec<String>, Error> {
    if raw.is_empty() {
        return Ok(vec!["exec".to_string()]);
    }
    let mut caps = Vec::new();
    for item in raw {
        for part in item.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let valid = validate_node_capability(part)?;
            if !caps.contains(&valid) {
                caps.push(valid);
            }
        }
    }
    if caps.is_empty() {
        return Err(Error::Message("at least one capability is required".into()));
    }
    Ok(caps)
}

fn http_agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(HTTP_TIMEOUT_SECS)))
        .http_status_as_error(false)
        .build()
        .into()
}

fn oidc_metadata_available(base_url: &str) -> bool {
    let url = format!("{base_url}/.well-known/oauth-protected-resource");
    http_agent()
        .get(&url)
        .header("accept", "application/json")
        .call()
        .is_ok_and(|resp| resp.status().as_u16() == 200)
}

fn has_node_pair_scope(scope: Option<&str>) -> bool {
    let Some(raw) = scope else {
        return false;
    };
    let Some(scopes) = crate::oauth_scope::parse(raw) else {
        return false;
    };
    scopes.contains(crate::oauth_scope::NODE_CONNECT) || scopes.contains(crate::oauth_scope::ADMIN)
}

fn resolve_local_coordinator_url(data: &Path) -> Option<String> {
    if let Some(pub_url) = crate::server::serverless::resolve_public_url(data) {
        return Some(pub_url);
    }
    if let Ok(addr) = std::fs::read_to_string(data.join("webhook.addr")) {
        let trimmed = addr.trim();
        if !trimmed.is_empty() {
            return Some(format!("http://{trimmed}"));
        }
    }
    None
}

fn pair_cmd(data: &Path, config_path: &Path, opts: PairCmdOpts) -> Result<(), Error> {
    let capabilities = parse_capabilities_flags(&opts.capabilities)?;
    let hostname = detect_hostname();
    let name = opts
        .name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| hostname.clone());
    let payload = json!({
        "name": name,
        "hostname": hostname,
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "capabilities": capabilities,
    });

    let explicit_url = opts
        .url
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.trim_end_matches('/').to_string())
        .or_else(|| {
            std::env::var("ANYAGENTS_CONTROL_URL")
                .ok()
                .map(|v| v.trim().trim_end_matches('/').to_string())
                .filter(|v| !v.is_empty())
        });

    let now_unix = jiff::Timestamp::now().as_second();
    let saved_auth = auth_device::load_control_auth()?;

    let (coordinator_url, resp_val): (String, Value) = if let Some(base_url) = explicit_url {
        let matching_saved = saved_auth.as_ref().filter(|a| {
            a.coordinator_url.trim_end_matches('/') == base_url
                && !a.is_expired_at(now_unix)
                && has_node_pair_scope(a.scope.as_deref())
                && a.bearer_token().is_some()
        });
        let op_token = operator::operator_token(data);
        let force_device_flow =
            opts.issuer.is_some() || opts.client_id.is_some() || opts.resource.is_some();

        let (bearer, use_op) = if !force_device_flow && let Some(auth) = matching_saved {
            (auth.bearer_token().map(str::to_string), None)
        } else if force_device_flow || (op_token.is_none() && oidc_metadata_available(&base_url)) {
            auth_device::login_cmd(&DeviceLoginOptions {
                url: Some(base_url.clone()),
                issuer: opts.issuer,
                client_id: opts.client_id,
                resource: opts.resource,
                scope: vec![crate::oauth_scope::NODE_CONNECT.to_string()],
            })?;
            let fresh = auth_device::load_control_auth()?.ok_or_else(|| {
                Error::Message("device flow succeeded but control auth was not saved".into())
            })?;
            (fresh.bearer_token().map(str::to_string), None)
        } else {
            (None, op_token)
        };

        let val = http_call_json(
            &base_url,
            "POST",
            "/v1/nodes/pair",
            Some(&payload),
            bearer.as_deref(),
            use_op.as_deref(),
        )?;
        (base_url, val)
    } else if server::socket_path(data).exists() {
        let coord = resolve_local_coordinator_url(data).ok_or_else(|| {
            Error::Message(
                "cannot determine coordinator URL for local socket pairing; pass `--url <coordinator_url>`"
                    .into(),
            )
        })?;
        let raw = serde_json::to_vec(&payload).map_err(|e| Error::Message(e.to_string()))?;
        let auth = client::authority(data);
        let val: Value = client::call(data, "POST", "/v1/nodes/pair", Some(raw), &auth)
            .map_err(Error::Message)?
            .ok_or_else(|| Error::Message("empty response from /v1/nodes/pair".into()))?;
        (coord.trim_end_matches('/').to_string(), val)
    } else if let Some(auth) = saved_auth.filter(|a| {
        !a.is_expired_at(now_unix)
            && has_node_pair_scope(a.scope.as_deref())
            && a.bearer_token().is_some()
    }) {
        let base_url = auth.coordinator_url.trim_end_matches('/').to_string();
        let val = http_call_json(
            &base_url,
            "POST",
            "/v1/nodes/pair",
            Some(&payload),
            auth.bearer_token(),
            None,
        )?;
        (base_url, val)
    } else {
        return Err(Error::Message(
            "missing coordinator URL: pass `--url <coordinator_url>` or start a local coordinator"
                .into(),
        ));
    };

    let node_id = resp_val
        .get("node_id")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Message("coordinator pair response missing `node_id`".into()))?
        .to_string();
    let node_token = resp_val
        .get("node_token")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Message("coordinator pair response missing `node_token`".into()))?
        .to_string();

    let stored = StoredNodeConfig {
        coordinator_url: coordinator_url.clone(),
        node_id: node_id.clone(),
        node_token,
        name: name.clone(),
        capabilities: capabilities.clone(),
    };
    save_node_config_at(config_path, &stored)?;

    if opts.json_out {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "node_id": node_id,
                "name": name,
                "coordinator_url": coordinator_url,
                "capabilities": capabilities,
                "config_path": config_path.display().to_string(),
            }))
            .unwrap_or_default()
        );
    } else if ui::is_pretty() {
        ui::success(&format!("Paired node `{name}` ({node_id})"));
        ui::note(&format!(
            "Saved credentials (0600) to {}",
            config_path.display()
        ));
        ui::note("Start tunnel: anyagents node run");
    } else {
        println!(
            "paired\t{}\t{}\t{}\t{}",
            node_id,
            name,
            coordinator_url,
            config_path.display()
        );
    }
    Ok(())
}

struct ResolvedNodeTarget {
    cfg: Option<StoredNodeConfig>,
    coord_url: Option<String>,
    node_id: Option<String>,
}

fn resolve_target_for_control(
    data: &Path,
    config_path: &Path,
    explicit_url: Option<&str>,
    explicit_node_id: Option<&str>,
) -> Result<ResolvedNodeTarget, Error> {
    let cfg = load_node_config_at(config_path)?;
    let node_id = explicit_node_id
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| cfg.as_ref().map(|c| c.node_id.clone()));
    let coord_url = explicit_url
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.trim_end_matches('/').to_string())
        .or_else(|| {
            std::env::var("ANYAGENTS_CONTROL_URL")
                .ok()
                .map(|v| v.trim().trim_end_matches('/').to_string())
                .filter(|v| !v.is_empty())
        })
        .or_else(|| cfg.as_ref().map(|c| c.coordinator_url.clone()))
        .or_else(|| resolve_local_coordinator_url(data));
    Ok(ResolvedNodeTarget {
        cfg,
        coord_url,
        node_id,
    })
}

fn status_cmd(
    data: &Path,
    config_path: &Path,
    explicit_url: Option<&str>,
    explicit_node_id: Option<&str>,
    json_out: bool,
) -> Result<(), Error> {
    let local_status = runner::query_local_control(config_path, "status");
    let ResolvedNodeTarget {
        cfg,
        coord_url,
        node_id,
    } = resolve_target_for_control(data, config_path, explicit_url, explicit_node_id)?;

    let remote_val: Option<Value> = if let Some(ref nid) = node_id {
        let path = format!("/v1/nodes/{nid}");
        call_node_control(data, coord_url.as_deref(), cfg.as_ref(), "GET", &path, None).ok()
    } else if coord_url.is_some() || server::socket_path(data).exists() {
        call_node_control(
            data,
            coord_url.as_deref(),
            cfg.as_ref(),
            "GET",
            "/v1/nodes",
            None,
        )
        .ok()
    } else {
        None
    };

    if json_out {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "config_path": config_path.display().to_string(),
                "local": local_status,
                "remote": remote_val,
            }))
            .unwrap_or_default()
        );
        return Ok(());
    }

    if let Some(ref local) = local_status {
        println!(
            "local\tnode={}\tconnected={}\tpaused={}\tactive_execs={}",
            local.node_id, local.connected, local.paused, local.active_execs
        );
    }
    if let Some(ref remote) = remote_val {
        if let Some(node) = remote
            .get("node")
            .and_then(|v| serde_json::from_value::<NodeRecord>(v.clone()).ok())
        {
            println!(
                "remote\t{}\t{}\tstatus={}\tos={}/{}\tcaps={}",
                node.id,
                node.name,
                node.status.as_str(),
                node.os,
                node.arch,
                node.capabilities.join(",")
            );
        } else if let Some(nodes) = remote.get("nodes").and_then(Value::as_array) {
            for item in nodes {
                if let Ok(node) = serde_json::from_value::<NodeRecord>(item.clone()) {
                    println!(
                        "{}\t{}\tstatus={}\tos={}/{}\tcaps={}",
                        node.id,
                        node.name,
                        node.status.as_str(),
                        node.os,
                        node.arch,
                        node.capabilities.join(",")
                    );
                }
            }
        }
    }
    if local_status.is_none() && remote_val.is_none() {
        return Err(Error::Message(
            "no running local node daemon and unable to reach coordinator (pair first or pass `--url`)"
                .into(),
        ));
    }
    Ok(())
}

fn lifecycle_cmd(
    data: &Path,
    config_path: &Path,
    explicit_url: Option<&str>,
    explicit_node_id: Option<&str>,
    action: &str,
    json_out: bool,
) -> Result<(), Error> {
    // Trigger instant local kill-switch BEFORE coordinator call for pause/revoke.
    let local = if matches!(action, "pause" | "revoke") {
        runner::query_local_control(config_path, action)
    } else {
        None
    };

    let ResolvedNodeTarget {
        cfg,
        coord_url,
        node_id,
    } = resolve_target_for_control(data, config_path, explicit_url, explicit_node_id)?;
    let nid = node_id.ok_or_else(|| {
        Error::Message(format!(
            "missing node ID: pass `--node-id <nd_...>` or pair this device ({})",
            config_path.display()
        ))
    })?;
    let path = format!("/v1/nodes/{nid}/{action}");
    let remote = call_node_control(
        data,
        coord_url.as_deref(),
        cfg.as_ref(),
        "POST",
        &path,
        Some(&json!({})),
    )?;

    if action == "resume" {
        let _ = runner::query_local_control(config_path, "resume");
    }
    if action == "revoke" && cfg.as_ref().is_some_and(|c| c.node_id == nid) {
        let _ = std::fs::remove_file(config_path);
    }

    if json_out {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "action": action,
                "node_id": nid,
                "local": local,
                "remote": remote,
            }))
            .unwrap_or_default()
        );
    } else {
        let status = remote
            .pointer("/node/status")
            .and_then(Value::as_str)
            .unwrap_or(action);
        println!("{action}\t{nid}\tstatus={status}");
    }
    Ok(())
}

fn call_node_control(
    data: &Path,
    coord_url: Option<&str>,
    cfg: Option<&StoredNodeConfig>,
    method: &str,
    path: &str,
    body: Option<&Value>,
) -> Result<Value, Error> {
    let saved_auth = auth_device::load_control_auth().ok().flatten();
    let now_unix = jiff::Timestamp::now().as_second();
    let op_token = operator::operator_token(data);

    if let Some(base_url) = coord_url {
        let bearer = saved_auth
            .as_ref()
            .filter(|a| {
                a.coordinator_url.trim_end_matches('/') == base_url.trim_end_matches('/')
                    && !a.is_expired_at(now_unix)
            })
            .and_then(|a| a.bearer_token())
            .or_else(|| cfg.map(|c| c.node_token.as_str()));
        let use_op = if bearer.is_none() {
            op_token.as_deref()
        } else {
            None
        };
        return http_call_json(base_url, method, path, body, bearer, use_op);
    }

    if server::socket_path(data).exists() {
        let raw = body
            .map(serde_json::to_vec)
            .transpose()
            .map_err(|e| Error::Message(e.to_string()))?;
        let auth = client::authority(data);
        return client::call(data, method, path, raw, &auth)
            .map_err(Error::Message)?
            .ok_or_else(|| Error::Message(format!("empty response from {path}")));
    }

    Err(Error::Message(
        "coordinator URL is unavailable and local socket does not exist".into(),
    ))
}

fn http_call_json(
    base_url: &str,
    method: &str,
    path: &str,
    body: Option<&Value>,
    bearer: Option<&str>,
    op_token: Option<&str>,
) -> Result<Value, Error> {
    let url = format!("{}{path}", base_url.trim_end_matches('/'));
    let agent = http_agent();
    let body_bytes = body
        .map(serde_json::to_vec)
        .transpose()
        .map_err(|e| Error::Message(e.to_string()))?;
    let mut resp = match method {
        "GET" => {
            let mut req = agent.get(&url).header("accept", "application/json");
            if let Some(b) = bearer {
                req = req.header("authorization", format!("Bearer {b}"));
            } else if let Some(op) = op_token {
                req = req.header(operator::HEADER, op);
            }
            req.call()
        }
        "POST" => {
            let mut req = agent
                .post(&url)
                .header("accept", "application/json")
                .header("content-type", "application/json");
            if let Some(b) = bearer {
                req = req.header("authorization", format!("Bearer {b}"));
            } else if let Some(op) = op_token {
                req = req.header(operator::HEADER, op);
            }
            req.send(body_bytes.as_deref().unwrap_or(b"{}"))
        }
        other => return Err(Error::Message(format!("unsupported method `{other}`"))),
    }
    .map_err(|err| Error::Message(format!("request failed ({url}): {err}")))?;

    let status = resp.status().as_u16();
    let mut raw = Vec::new();
    let _ = resp
        .body_mut()
        .as_reader()
        .take(MAX_HTTP_BYTES)
        .read_to_end(&mut raw);
    if !(200..300).contains(&status) {
        let msg = serde_json::from_slice::<Value>(&raw)
            .ok()
            .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_string))
            .unwrap_or_else(|| {
                let txt = String::from_utf8_lossy(&raw).trim().to_string();
                if txt.is_empty() {
                    format!("HTTP {status}")
                } else {
                    format!("HTTP {status}: {txt}")
                }
            });
        return Err(Error::Message(msg));
    }
    serde_json::from_slice(&raw)
        .map_err(|err| Error::Message(format!("invalid JSON from {path}: {err}")))
}
