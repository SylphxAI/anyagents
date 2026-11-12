use std::io::Read as _;
use std::path::Path;
use std::time::Duration;

use crate::encoding::url_encode_query_component;
use serde::{Deserialize, Serialize};

pub(crate) const DEFAULT_REMOTE_WORKDIR: &str = "/workspace";
pub(crate) const DEFAULT_EXEC_TIMEOUT_MS: u64 = 30_000;

pub(crate) fn default_remote_workdir() -> String {
    DEFAULT_REMOTE_WORKDIR.to_string()
}

pub(crate) fn env_remote_spawn_flag() -> bool {
    std::env::var("ANYAGENTS_SANDBOX_REMOTE_SPAWN")
        .ok()
        .is_some_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true"))
}

/// Configuration for a remote container sandbox bound to an agent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]

pub struct SandboxConfig {
    /// Base URL of the sandbox API (e.g. `https://sandbox.example.com` or `http://127.0.0.1:9090`).
    pub endpoint: String,
    /// Unique identifier of the remote sandbox instance.
    pub sandbox_id: String,
    /// Optional bearer token used to authenticate requests to the sandbox API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// Default working directory inside the remote sandbox container.
    #[serde(default = "default_remote_workdir")]
    pub workdir: String,
    /// Whether agent harness processes should be spawned inside the remote sandbox via `sandbox-exec`.
    #[serde(default)]
    pub remote_spawn: bool,
}

impl SandboxConfig {
    /// Load [`SandboxConfig`] from `<config_dir>/sandbox.json` if present and valid.
    pub fn from_config_dir(config_dir: &Path) -> Option<Self> {
        let raw = std::fs::read_to_string(config_dir.join("sandbox.json")).ok()?;
        let mut cfg: Self = serde_json::from_str(&raw).ok()?;
        cfg.endpoint = cfg.endpoint.trim().trim_end_matches('/').to_string();
        cfg.sandbox_id = cfg.sandbox_id.trim().to_string();
        cfg.token = cfg
            .token
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty());
        let wd = cfg.workdir.trim().trim_end_matches('/');
        cfg.workdir = if wd.is_empty() {
            DEFAULT_REMOTE_WORKDIR.to_string()
        } else {
            wd.to_string()
        };
        if env_remote_spawn_flag() {
            cfg.remote_spawn = true;
        }
        if cfg.endpoint.is_empty() || cfg.sandbox_id.is_empty() {
            return None;
        }
        Some(cfg)
    }

    /// Load [`SandboxConfig`] from `ANYAGENTS_SANDBOX_API_URL` and `ANYAGENTS_SANDBOX_ID`.
    pub fn from_env() -> Option<Self> {
        let endpoint = std::env::var("ANYAGENTS_SANDBOX_API_URL")
            .ok()?
            .trim()
            .trim_end_matches('/')
            .to_string();
        let sandbox_id = std::env::var("ANYAGENTS_SANDBOX_ID")
            .ok()?
            .trim()
            .to_string();
        if endpoint.is_empty() || sandbox_id.is_empty() {
            return None;
        }
        let token = std::env::var("ANYAGENTS_SANDBOX_TOKEN")
            .ok()
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty());
        let workdir = std::env::var("ANYAGENTS_SANDBOX_WORKDIR")
            .ok()
            .map(|w| w.trim().to_string())
            .filter(|w| !w.is_empty())
            .unwrap_or_else(default_remote_workdir);
        let remote_spawn = env_remote_spawn_flag();
        Some(Self {
            endpoint,
            sandbox_id,
            token,
            workdir,
            remote_spawn,
        })
    }

    /// Resolve [`SandboxConfig`] from `<config_dir>/sandbox.json` first, then environment variables.
    pub fn resolve(config_dir: Option<&Path>) -> Option<Self> {
        config_dir
            .and_then(Self::from_config_dir)
            .or_else(Self::from_env)
    }

    /// Persist [`SandboxConfig`] to `<config_dir>/sandbox.json`.
    pub fn write_to_config_dir(&self, config_dir: &Path) -> Result<(), String> {
        std::fs::create_dir_all(config_dir)
            .map_err(|e| format!("cannot create config dir {}: {e}", config_dir.display()))?;
        let mut bytes = serde_json::to_vec_pretty(self)
            .map_err(|e| format!("cannot serialize sandbox.json: {e}"))?;
        bytes.push(b'\n');
        let path = config_dir.join("sandbox.json");
        std::fs::write(&path, &bytes).map_err(|e| format!("cannot write {}: {e}", path.display()))
    }
}

/// Configuration for a sandbox fleet provider capable of provisioning, pausing,
/// resuming, and destroying remote sandboxes on agent lifecycle transitions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxProviderConfig {
    pub endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(default = "default_remote_workdir")]
    pub default_workdir: String,
    #[serde(default)]
    pub remote_spawn: bool,
}

impl SandboxProviderConfig {
    /// Load [`SandboxProviderConfig`] from `<data_dir>/sandbox-provider.json`.
    pub fn from_data_dir(data_dir: &Path) -> Option<Self> {
        let raw = std::fs::read_to_string(data_dir.join("sandbox-provider.json")).ok()?;
        let mut cfg: Self = serde_json::from_str(&raw).ok()?;
        cfg.endpoint = cfg.endpoint.trim().trim_end_matches('/').to_string();
        cfg.token = cfg
            .token
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty());
        let wd = cfg.default_workdir.trim().trim_end_matches('/');
        cfg.default_workdir = if wd.is_empty() {
            DEFAULT_REMOTE_WORKDIR.to_string()
        } else {
            wd.to_string()
        };
        if env_remote_spawn_flag() {
            cfg.remote_spawn = true;
        }
        if cfg.endpoint.is_empty() {
            return None;
        }
        Some(cfg)
    }

    /// Load [`SandboxProviderConfig`] from `ANYAGENTS_SANDBOX_PROVIDER_URL` and companion env vars.
    pub fn from_env() -> Option<Self> {
        let endpoint = std::env::var("ANYAGENTS_SANDBOX_PROVIDER_URL")
            .ok()?
            .trim()
            .trim_end_matches('/')
            .to_string();
        if endpoint.is_empty() {
            return None;
        }
        let token = std::env::var("ANYAGENTS_SANDBOX_PROVIDER_TOKEN")
            .or_else(|_| std::env::var("ANYAGENTS_SANDBOX_TOKEN"))
            .ok()
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty());
        let default_workdir = std::env::var("ANYAGENTS_SANDBOX_WORKDIR")
            .ok()
            .map(|w| w.trim().trim_end_matches('/').to_string())
            .filter(|w| !w.is_empty())
            .unwrap_or_else(default_remote_workdir);
        let remote_spawn = env_remote_spawn_flag();
        Some(Self {
            endpoint,
            token,
            default_workdir,
            remote_spawn,
        })
    }

    /// Resolve [`SandboxProviderConfig`] from `<data_dir>/sandbox-provider.json` first, then env vars.
    pub fn resolve(data_dir: Option<&Path>) -> Option<Self> {
        data_dir
            .and_then(Self::from_data_dir)
            .or_else(Self::from_env)
    }
}

/// Client for provisioning and managing remote sandbox lifecycles (`provision`, `pause`, `resume`, `destroy`).
pub struct SandboxProvider;

impl SandboxProvider {
    /// Provision a new remote sandbox for `(agent_id, agent_name, repo_url)`.
    pub fn provision(
        provider: &SandboxProviderConfig,
        agent_id: &str,
        agent_name: &str,
        repo_url: Option<&str>,
    ) -> Result<SandboxConfig, String> {
        let url = format!("{}/v1/sandboxes", provider.endpoint.trim_end_matches('/'));
        let agent = ureq_agent(30_000);
        let mut req = agent.post(&url).header("content-type", "application/json");
        if let Some(ref token) = provider.token {
            req = req.header("authorization", &format!("Bearer {token}"));
        }
        let payload = serde_json::json!({
            "agent_id": agent_id,
            "name": agent_name,
            "workdir": provider.default_workdir,
            "repo": repo_url,
        });
        let mut resp = req
            .send_json(&payload)
            .map_err(|e| format!("sandbox provision request failed ({url}): {e}"))?;
        let status = resp.status().as_u16();
        let mut body = Vec::new();
        let _ = resp.body_mut().as_reader().read_to_end(&mut body);
        if !(200..300).contains(&status) {
            return Err(format!(
                "sandbox provision returned HTTP {status}: {}",
                String::from_utf8_lossy(&body)
            ));
        }
        let parsed: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| format!("invalid JSON from sandbox provision: {e}"))?;
        let sandbox_id = parsed
            .get("sandbox_id")
            .or_else(|| parsed.get("id"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| "sandbox provision response missing `sandbox_id`".to_string())?
            .to_string();
        let endpoint = parsed
            .get("endpoint")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| provider.endpoint.clone());
        let token = parsed
            .get("token")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .or_else(|| provider.token.clone());
        let workdir = parsed
            .get("workdir")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| provider.default_workdir.clone());
        let remote_spawn = parsed
            .get("remote_spawn")
            .and_then(|v| v.as_bool())
            .unwrap_or(provider.remote_spawn);
        Ok(SandboxConfig {
            endpoint,
            sandbox_id,
            token,
            workdir,
            remote_spawn,
        })
    }

    /// Pause a running remote sandbox (`POST /v1/sandboxes/{sandbox_id}/pause`).
    pub fn pause(cfg: &SandboxConfig) -> Result<(), String> {
        Self::lifecycle_post(cfg, "pause")
    }

    /// Resume a paused or archived remote sandbox (`POST /v1/sandboxes/{sandbox_id}/resume`).
    pub fn resume(cfg: &SandboxConfig) -> Result<(), String> {
        Self::lifecycle_post(cfg, "resume")
    }

    /// Destroy a remote sandbox (`DELETE /v1/sandboxes/{sandbox_id}`, falling back to `POST .../destroy`).
    pub fn destroy(cfg: &SandboxConfig) -> Result<(), String> {
        let url = format!(
            "{}/v1/sandboxes/{}",
            cfg.endpoint.trim_end_matches('/'),
            url_encode_query_component(&cfg.sandbox_id)
        );
        let agent = ureq_agent(15_000);
        let mut req = agent.delete(&url);
        if let Some(ref token) = cfg.token {
            req = req.header("authorization", &format!("Bearer {token}"));
        }
        if let Ok(mut resp) = req.call() {
            let status = resp.status().as_u16();
            if (200..300).contains(&status) {
                return Ok(());
            }
            if status != 404 && status != 405 {
                let mut body = Vec::new();
                let _ = resp.body_mut().as_reader().read_to_end(&mut body);
                return Err(format!(
                    "sandbox destroy returned HTTP {status}: {}",
                    String::from_utf8_lossy(&body)
                ));
            }
        }
        Self::lifecycle_post(cfg, "destroy")
    }

    fn lifecycle_post(cfg: &SandboxConfig, action: &str) -> Result<(), String> {
        let url = format!(
            "{}/v1/sandboxes/{}/{action}",
            cfg.endpoint.trim_end_matches('/'),
            url_encode_query_component(&cfg.sandbox_id)
        );
        let agent = ureq_agent(15_000);
        let mut req = agent.post(&url).header("content-type", "application/json");
        if let Some(ref token) = cfg.token {
            req = req.header("authorization", &format!("Bearer {token}"));
        }
        let payload = serde_json::json!({ "action": action });
        let mut resp = req
            .send_json(&payload)
            .map_err(|e| format!("sandbox {action} request failed ({url}): {e}"))?;
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            let mut body = Vec::new();
            let _ = resp.body_mut().as_reader().read_to_end(&mut body);
            return Err(format!(
                "sandbox {action} returned HTTP {status}: {}",
                String::from_utf8_lossy(&body)
            ));
        }
        Ok(())
    }
}

pub(crate) fn ureq_agent(timeout_ms: u64) -> ureq::Agent {
    let config = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_millis(
            timeout_ms.saturating_add(5_000),
        )))
        .http_status_as_error(false)
        .build();
    config.into()
}
