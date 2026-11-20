//! Thin Control-Plane CLI subcommands (`auth`, `profiles`, `secrets`, `usage`)
//! targeting the single unified Control-Plane REST API via either:
//! - Remote HTTP(S) authenticated with caller-private RFC 8628 OIDC Bearer JWT
//!   (`Authorization: Bearer <token>` from `$XDG_STATE_HOME/anyagents/control-auth.json`), or
//! - Local Unix-socket coordinator (`crate::client`).

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Read as _};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyagents_core::{Error, UsageGroupBy, UsageReport};
use clap::{Args, Subcommand};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::api::{
    AgentSecretsResponse, DeleteSecretResponse, SecretMetadataView, SecretsListResponse,
    SetAgentSecretsRequest, UpsertSecretRequest,
};
use crate::auth_device::{self, DeviceLoginOptions};
use crate::create::{self, ProviderProfileEntry};
use crate::server::ServedProfile;
use crate::{agent, client, operator, server, ui};

const HTTP_TIMEOUT_SECS: u64 = 180;
const MAX_RESPONSE_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Subcommand)]
pub(crate) enum AuthCommand {
    /// Sign in to a remote coordinator via RFC 8628 OAuth2 Device Flow, or sign in a local harness/profile.
    Login {
        /// Optional local agent name, `<provider>/<profile>`, or harness ID. Omit (or pass `--url`/`--issuer`) for OIDC device login.
        target: Option<String>,
        /// Optional local profile name when signing in a local harness/agent.
        #[arg(long)]
        profile: Option<String>,
        /// Coordinator base URL (e.g. `https://coordinator.example.com`) for OIDC device login.
        #[arg(long)]
        url: Option<String>,
        /// Explicit OIDC issuer URL (auto-discovered from `--url` when omitted).
        #[arg(long)]
        issuer: Option<String>,
        /// Explicit OAuth2 public client ID (auto-discovered from `--url` when omitted).
        #[arg(long = "client-id")]
        client_id: Option<String>,
        /// Explicit RFC 8707 `resource` audience URI (defaults to `--url`).
        #[arg(long)]
        resource: Option<String>,
    },
    /// Show caller's control-plane OIDC login status (never prints token values).
    Status {
        /// Output machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Remove caller's saved control-plane OIDC token.
    Logout,
    /// List provider accounts and quota.
    List {
        filter: Option<String>,
        /// Probe fresh live quotas (bypassing caches).
        #[arg(long)]
        fresh: bool,
        /// Remote coordinator URL (defaults to saved OIDC login when no local socket exists).
        #[arg(long)]
        url: Option<String>,
        /// Output machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Set the API key for a provider profile.
    Set {
        target: String,
        #[arg(long = "key-file")]
        key_file: Option<PathBuf>,
        #[arg(long)]
        url: Option<String>,
    },
    /// List supported harnesses.
    Harnesses,
}

#[derive(Args)]
pub(crate) struct ProfilesCliArgs {
    #[command(subcommand)]
    pub(crate) action: Option<ProfilesCommand>,
}

#[derive(Subcommand)]
pub(crate) enum ProfilesCommand {
    /// List upstream provider profiles and 5h + Weekly quotas (`GET /v1/profiles`).
    List {
        /// Optional provider or harness filter.
        filter: Option<String>,
        /// Bypass stale caches and probe live 5h + Weekly quotas (`GET /v1/profiles?fresh=1`).
        #[arg(long)]
        fresh: bool,
        /// Remote coordinator URL.
        #[arg(long)]
        url: Option<String>,
        /// Output machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Run a native provider sign-in on the coordinator (`POST /v1/profiles/native-signin`).
    #[command(name = "native-signin")]
    NativeSignin {
        /// Target harness or provider (`claude-code`, `codex`, `agy`, `anthropic`, `openai`, `google-antigravity`).
        harness: String,
        /// Target profile name (defaults to `default`).
        #[arg(long)]
        profile: Option<String>,
        /// Remote coordinator URL.
        #[arg(long)]
        url: Option<String>,
    },
    /// Force-refresh profile credentials and probe fresh 5h + Weekly quotas (`GET /v1/profiles?fresh=1`).
    Refresh {
        /// Optional provider or profile filter.
        filter: Option<String>,
        /// Remote coordinator URL.
        #[arg(long)]
        url: Option<String>,
        /// Output machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Store an API key / coding-plan key for `<provider>/<profile>` (`POST /v1/profiles`).
    Set {
        /// Target `<provider>/<profile>`.
        target: String,
        /// File holding the API key on its own line (reads from stdin when omitted).
        #[arg(long = "key-file")]
        key_file: Option<PathBuf>,
        /// Remote coordinator URL.
        #[arg(long)]
        url: Option<String>,
    },
}

#[derive(Args)]
pub(crate) struct SecretsCliArgs {
    #[command(subcommand)]
    pub(crate) action: SecretsCommand,
}

#[derive(Subcommand)]
pub(crate) enum SecretsCommand {
    /// List write-only secret bundle metadata (`GET /v1/secrets`; never returns values).
    List {
        /// Remote coordinator URL.
        #[arg(long)]
        url: Option<String>,
        /// Output machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Upsert a named encrypted deployment secret bundle (`PUT /v1/secrets/{name}`; write-only).
    Set {
        /// Secret bundle name (e.g. `ci-github`).
        name: String,
        /// File containing `KEY=VALUE` lines to encrypt into this secret bundle.
        #[arg(long = "env-file")]
        env_file: Option<PathBuf>,
        /// Environment variable name(s) to read from the current process environment.
        #[arg(long = "from-env")]
        from_env: Vec<String>,
        /// Read a JSON object `{"KEY":"VALUE"}` from standard input.
        #[arg(long = "stdin-json")]
        stdin_json: bool,
        /// Remote coordinator URL.
        #[arg(long)]
        url: Option<String>,
        /// Output machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Grant one or more deployment secret bundles to an agent (`PUT /v1/agents/{agent}/secrets`).
    Grant {
        /// Target agent name or ID.
        agent: String,
        /// One or more secret bundle names to grant.
        #[arg(required = true)]
        secrets: Vec<String>,
        /// Remote coordinator URL.
        #[arg(long)]
        url: Option<String>,
        /// Output machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Revoke one or more deployment secret bundles from an agent (`PUT /v1/agents/{agent}/secrets`).
    Revoke {
        /// Target agent name or ID.
        agent: String,
        /// One or more secret bundle names to revoke.
        #[arg(required = true)]
        secrets: Vec<String>,
        /// Remote coordinator URL.
        #[arg(long)]
        url: Option<String>,
        /// Output machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Delete a deployment secret bundle and cascade its grants (`DELETE /v1/secrets/{name}`).
    Delete {
        /// Secret bundle name.
        name: String,
        /// Remote coordinator URL.
        #[arg(long)]
        url: Option<String>,
        /// Output machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Args)]
pub(crate) struct UsageCliArgs {
    #[arg(long)]
    pub(crate) agent: Option<String>,
    /// Group sums (`agent`, `day`, `model`, `harness`, `profile`). Legacy alias for `--group-by`.
    #[arg(long)]
    pub(crate) by: Option<String>,
    /// Group sums (`agent`, `day`, `model`, `harness`, `profile`).
    #[arg(long = "group-by")]
    pub(crate) group_by: Option<String>,
    /// Include causal descendants of `--agent`.
    #[arg(long)]
    pub(crate) include_descendants: bool,
    /// Window start, inclusive (unix ms).
    #[arg(long = "since-ms")]
    pub(crate) since_ms: Option<i64>,
    /// Window end, inclusive (unix ms).
    #[arg(long = "until-ms")]
    pub(crate) until_ms: Option<i64>,
    /// Trailing window in days.
    #[arg(long)]
    pub(crate) days: Option<i64>,
    /// Remote coordinator URL.
    #[arg(long)]
    pub(crate) url: Option<String>,
    /// Output machine-readable JSON.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Clone, Debug)]
enum ControlEndpoint {
    Remote {
        base_url: String,
        bearer: Option<String>,
        operator_token: Option<String>,
    },
    LocalSocket,
}

fn resolve_control_endpoint(
    data: &Path,
    explicit_url: Option<&str>,
) -> Result<ControlEndpoint, Error> {
    let explicit = explicit_url
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            std::env::var("ANYAGENTS_CONTROL_URL")
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        });

    let saved = auth_device::load_control_auth()?;

    if let Some(url) = explicit {
        let normalized = url.trim_end_matches('/').to_string();
        let bearer = saved
            .as_ref()
            .filter(|a| a.coordinator_url.trim_end_matches('/') == normalized)
            .and_then(|a| a.bearer_token().map(str::to_string))
            .or_else(|| {
                saved
                    .as_ref()
                    .and_then(|a| a.bearer_token().map(str::to_string))
            });
        let op_tok = if bearer.is_none() {
            operator::operator_token(data)
        } else {
            None
        };
        return Ok(ControlEndpoint::Remote {
            base_url: normalized,
            bearer,
            operator_token: op_tok,
        });
    }

    if server::socket_path(data).exists() {
        return Ok(ControlEndpoint::LocalSocket);
    }

    if let Some(auth) = saved
        && !auth.coordinator_url.trim().is_empty()
    {
        return Ok(ControlEndpoint::Remote {
            base_url: auth.coordinator_url.trim_end_matches('/').to_string(),
            bearer: auth.bearer_token().map(str::to_string),
            operator_token: None,
        });
    }

    Ok(ControlEndpoint::LocalSocket)
}

fn build_http_agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(HTTP_TIMEOUT_SECS)))
        .http_status_as_error(false)
        .build()
        .into()
}

fn extract_error_message(status: u16, body: &[u8]) -> String {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_string))
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            let text = String::from_utf8_lossy(body).trim().to_string();
            if text.is_empty() {
                format!("HTTP {status}")
            } else {
                format!("HTTP {status}: {text}")
            }
        })
}

fn rest_call_json<T: DeserializeOwned>(
    data: &Path,
    explicit_url: Option<&str>,
    method: &str,
    path: &str,
    body: Option<&Value>,
) -> Result<T, Error> {
    let endpoint = resolve_control_endpoint(data, explicit_url)?;
    match endpoint {
        ControlEndpoint::Remote {
            base_url,
            bearer,
            operator_token,
        } => {
            let url = format!("{base_url}{path}");
            let agent = build_http_agent();
            let body_bytes = body
                .map(serde_json::to_vec)
                .transpose()
                .map_err(|err| Error::Message(err.to_string()))?;

            let mut resp = match method {
                "GET" => {
                    let mut req = agent.get(&url).header("accept", "application/json");
                    if let Some(ref b) = bearer {
                        req = req.header("authorization", format!("Bearer {b}"));
                    } else if let Some(ref op) = operator_token {
                        req = req.header(operator::HEADER, op);
                    }
                    req.call()
                }
                "DELETE" => {
                    let mut req = agent.delete(&url).header("accept", "application/json");
                    if let Some(ref b) = bearer {
                        req = req.header("authorization", format!("Bearer {b}"));
                    } else if let Some(ref op) = operator_token {
                        req = req.header(operator::HEADER, op);
                    }
                    req.call()
                }
                "POST" => {
                    let mut req = agent
                        .post(&url)
                        .header("accept", "application/json")
                        .header("content-type", "application/json");
                    if let Some(ref b) = bearer {
                        req = req.header("authorization", format!("Bearer {b}"));
                    } else if let Some(ref op) = operator_token {
                        req = req.header(operator::HEADER, op);
                    }
                    req.send(body_bytes.as_deref().unwrap_or(b"{}"))
                }
                "PUT" => {
                    let mut req = agent
                        .put(&url)
                        .header("accept", "application/json")
                        .header("content-type", "application/json");
                    if let Some(ref b) = bearer {
                        req = req.header("authorization", format!("Bearer {b}"));
                    } else if let Some(ref op) = operator_token {
                        req = req.header(operator::HEADER, op);
                    }
                    req.send(body_bytes.as_deref().unwrap_or(b"{}"))
                }
                other => {
                    return Err(Error::Message(format!("unsupported HTTP method `{other}`")));
                }
            }
            .map_err(|err| {
                Error::Message(format!("control-plane request failed ({url}): {err}"))
            })?;

            let status = resp.status().as_u16();
            let mut raw = Vec::new();
            let _ = resp
                .body_mut()
                .as_reader()
                .take(MAX_RESPONSE_BYTES)
                .read_to_end(&mut raw);
            if !(200..300).contains(&status) {
                return Err(Error::Message(extract_error_message(status, &raw)));
            }
            serde_json::from_slice(&raw)
                .map_err(|err| Error::Message(format!("invalid JSON from {path}: {err}")))
        }
        ControlEndpoint::LocalSocket => {
            if !server::socket_path(data).exists() {
                return Err(Error::Message(
                    "coordinator is not running locally and no remote login is configured (start `anyagents serve` or run `anyagents auth login --url <url>`)"
                        .into(),
                ));
            }
            let raw_body = body
                .map(serde_json::to_vec)
                .transpose()
                .map_err(|err| Error::Message(err.to_string()))?;
            let auth = client::authority(data);
            client::call::<T>(data, method, path, raw_body, &auth)
                .map_err(Error::Message)?
                .ok_or_else(|| Error::Message("coordinator returned an empty response".into()))
        }
    }
}

/// Dispatch `anyagents auth <subcommand>`.
pub(crate) fn run_auth_cmd(
    data: &Path,
    action: Option<AuthCommand>,
    local_signin: impl FnOnce(&Path, &str, Option<&str>) -> Result<(), Error>,
    list_harnesses: impl FnOnce(&Path),
) -> Result<(), Error> {
    let cmd = action.unwrap_or(AuthCommand::List {
        filter: None,
        fresh: false,
        url: None,
        json: false,
    });
    match cmd {
        AuthCommand::Login {
            target,
            profile,
            url,
            issuer,
            client_id,
            resource,
        } => {
            let wants_device_flow = target.is_none()
                || url.is_some()
                || issuer.is_some()
                || client_id.is_some()
                || resource.is_some();
            if wants_device_flow {
                auth_device::login_cmd(&DeviceLoginOptions {
                    url: url.or(target),
                    issuer,
                    client_id,
                    resource,
                })
            } else {
                let target_str = target.unwrap_or_default();
                local_signin(data, &target_str, profile.as_deref())
            }
        }
        AuthCommand::Status { json } => auth_device::status_cmd(json),
        AuthCommand::Logout => auth_device::logout_cmd(),
        AuthCommand::List {
            filter,
            fresh,
            url,
            json,
        } => profiles_list_cmd(data, filter.as_deref(), fresh, url.as_deref(), json),
        AuthCommand::Set {
            target,
            key_file,
            url,
        } => profiles_set_cmd(data, &target, key_file.as_deref(), url.as_deref()),
        AuthCommand::Harnesses => {
            list_harnesses(data);
            Ok(())
        }
    }
}

/// Dispatch `anyagents profiles [subcommand]`.
pub(crate) fn run_profiles_cmd(data: &Path, args: ProfilesCliArgs) -> Result<(), Error> {
    match args.action.unwrap_or(ProfilesCommand::List {
        filter: None,
        fresh: false,
        url: None,
        json: false,
    }) {
        ProfilesCommand::List {
            filter,
            fresh,
            url,
            json,
        } => profiles_list_cmd(data, filter.as_deref(), fresh, url.as_deref(), json),
        ProfilesCommand::NativeSignin {
            harness,
            profile,
            url,
        } => profiles_native_signin_cmd(data, &harness, profile.as_deref(), url.as_deref()),
        ProfilesCommand::Refresh { filter, url, json } => {
            if url.is_none() && !server::socket_path(data).exists() {
                let _ = create::force_refresh_provider_profiles(data, filter.as_deref());
            }
            profiles_list_cmd(data, filter.as_deref(), true, url.as_deref(), json)
        }
        ProfilesCommand::Set {
            target,
            key_file,
            url,
        } => profiles_set_cmd(data, &target, key_file.as_deref(), url.as_deref()),
    }
}

/// Fetch and print provider profiles from `GET /v1/profiles` (with `?fresh=1` when requested),
/// falling back to local store listing only when no remote URL/login and no local daemon exist.
pub(crate) fn profiles_list_cmd(
    data: &Path,
    filter: Option<&str>,
    fresh: bool,
    explicit_url: Option<&str>,
    json_out: bool,
) -> Result<(), Error> {
    let endpoint = resolve_control_endpoint(data, explicit_url)?;
    let path = if fresh {
        "/v1/profiles?fresh=1"
    } else {
        "/v1/profiles"
    };
    let mut entries: Vec<ServedProfile> = match endpoint {
        ControlEndpoint::Remote { .. } => rest_call_json(data, explicit_url, "GET", path, None)?,
        ControlEndpoint::LocalSocket if server::socket_path(data).exists() => {
            rest_call_json(data, None, "GET", path, None)?
        }
        ControlEndpoint::LocalSocket => {
            let store = anyagents_core::Store::open(data.join("state.db"))?;
            let listed = if fresh {
                create::list_provider_profiles_fresh(&store, data)
            } else {
                create::list_provider_profiles_fast(&store, data)
            };
            listed
                .into_iter()
                .map(|entry| ServedProfile {
                    entry,
                    usage_age_ms: None,
                })
                .collect()
        }
    };

    if let Some(f) = filter.map(str::trim).filter(|s| !s.is_empty()) {
        let norm = anyagents_harness::normalize_harness_name(f);
        entries.retain(|sp| {
            let e = &sp.entry;
            e.provider.eq_ignore_ascii_case(f)
                || e.provider.eq_ignore_ascii_case(norm)
                || e.compatible_harnesses
                    .iter()
                    .any(|h| h.eq_ignore_ascii_case(f) || h.eq_ignore_ascii_case(norm))
                || format!("{}/{}", e.provider, e.profile).eq_ignore_ascii_case(f)
        });
    }

    if json_out {
        println!(
            "{}",
            serde_json::to_string_pretty(&entries).unwrap_or_else(|_| "[]".into())
        );
        return Ok(());
    }

    render_profiles(&entries);
    Ok(())
}

fn format_quota_window(win: Option<&create::QuotaWindow>) -> String {
    match win {
        Some(q) => match q.reset_eta.as_deref() {
            Some(eta) => format!("{}% ↻{eta}", q.remaining_pct),
            None => format!("{}%", q.remaining_pct),
        },
        None => "-".to_string(),
    }
}

fn render_profiles(entries: &[ServedProfile]) {
    if ui::is_pretty() {
        let raw: Vec<ProviderProfileEntry> = entries.iter().map(|s| s.entry.clone()).collect();
        ui::print_profiles_pretty(&raw);
        return;
    }

    for served in entries {
        let e: &ProviderProfileEntry = &served.entry;
        let q5 = format_quota_window(e.quota_5h.as_ref());
        let q7 = format_quota_window(e.quota_weekly.as_ref());
        println!(
            "{}/{}\t{}\t{}\t{}\t{}\t{}",
            e.provider,
            e.profile,
            e.account.as_deref().unwrap_or("-"),
            q5,
            q7,
            e.usage.as_deref().unwrap_or("-"),
            if e.assigned_agents.is_empty() {
                "-".to_string()
            } else {
                e.assigned_agents.join(",")
            }
        );
    }
}

fn read_secret_line(key_file: Option<&Path>) -> Result<String, Error> {
    let raw = match key_file {
        Some(path) => std::fs::read_to_string(path)?,
        None if ui::is_pretty() && std::io::IsTerminal::is_terminal(&std::io::stdin()) => {
            crate::signin::read_secret_line("API key", Some(Duration::from_secs(300)))?
        }
        None => {
            let mut buf = String::new();
            std::io::stdin().read_to_string(&mut buf)?;
            buf
        }
    };
    let key = raw.trim().to_string();
    if key.is_empty() {
        return Err(Error::Message(
            "empty key; pass --key-file <path> or pipe the key on stdin".into(),
        ));
    }
    Ok(key)
}

/// `anyagents profiles set <provider>/<profile>`: calls `POST /v1/profiles`.
pub(crate) fn profiles_set_cmd(
    data: &Path,
    target: &str,
    key_file: Option<&Path>,
    explicit_url: Option<&str>,
) -> Result<(), Error> {
    let key = read_secret_line(key_file)?;
    let endpoint = resolve_control_endpoint(data, explicit_url)?;
    let body = json!({ "profile": target, "key": key });
    let resp: Value = match endpoint {
        ControlEndpoint::Remote { .. } => {
            rest_call_json(data, explicit_url, "POST", "/v1/profiles", Some(&body))?
        }
        ControlEndpoint::LocalSocket if server::socket_path(data).exists() => {
            rest_call_json(data, None, "POST", "/v1/profiles", Some(&body))?
        }
        ControlEndpoint::LocalSocket => {
            let (provider, profile) = create::set_provider_profile(data, target, &key)?;
            json!({ "provider": provider, "profile": profile })
        }
    };
    let provider = resp.get("provider").and_then(Value::as_str).unwrap_or("");
    let profile = resp.get("profile").and_then(Value::as_str).unwrap_or("");
    if ui::is_pretty() {
        ui::success(&format!("Saved profile {provider}/{profile}"));
    } else {
        println!("saved\t{provider}/{profile}");
    }
    Ok(())
}

/// `anyagents profiles native-signin <harness> [--profile <name>]`: streams NDJSON
/// from `POST /v1/profiles/native-signin`.
pub(crate) fn profiles_native_signin_cmd(
    data: &Path,
    harness: &str,
    profile: Option<&str>,
    explicit_url: Option<&str>,
) -> Result<(), Error> {
    let body = json!({
        "harness": harness,
        "profile": profile,
    });
    let endpoint = resolve_control_endpoint(data, explicit_url)?;
    match endpoint {
        ControlEndpoint::Remote {
            base_url,
            bearer,
            operator_token,
        } => {
            let url = format!("{base_url}/v1/profiles/native-signin");
            let agent = build_http_agent();
            let mut req = agent
                .post(&url)
                .header("accept", "application/x-ndjson")
                .header("content-type", "application/json");
            if let Some(ref b) = bearer {
                req = req.header("authorization", format!("Bearer {b}"));
            } else if let Some(ref op) = operator_token {
                req = req.header(operator::HEADER, op);
            }
            let payload =
                serde_json::to_vec(&body).map_err(|err| Error::Message(err.to_string()))?;
            let mut resp = req
                .send(payload.as_slice())
                .map_err(|err| Error::Message(format!("native sign-in request failed: {err}")))?;
            let status = resp.status().as_u16();
            if !(200..300).contains(&status) {
                let mut err_buf = Vec::new();
                let _ = resp
                    .body_mut()
                    .as_reader()
                    .take(64 * 1024)
                    .read_to_end(&mut err_buf);
                return Err(Error::Message(extract_error_message(status, &err_buf)));
            }
            let reader = BufReader::new(resp.body_mut().as_reader());
            consume_signin_ndjson(reader)
        }
        ControlEndpoint::LocalSocket => {
            let socket = server::socket_path(data);
            if !socket.exists() {
                return Err(Error::Message(
                    "coordinator is not running locally; start `anyagents serve` or pass `--url <coordinator_url>`"
                        .into(),
                ));
            }
            let op_token = operator::operator_token(data);
            let payload =
                serde_json::to_vec(&body).map_err(|err| Error::Message(err.to_string()))?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|err| Error::Message(err.to_string()))?;
            let (status, bytes) = runtime
                .block_on(crate::mcp::request_waiting(
                    &socket,
                    "POST",
                    "/v1/profiles/native-signin",
                    Some(payload),
                    None,
                    op_token.as_deref(),
                    Duration::ZERO,
                    Duration::from_secs(300),
                ))
                .map_err(Error::Message)?;
            if !status.is_success() {
                return Err(Error::Message(extract_error_message(
                    status.as_u16(),
                    &bytes,
                )));
            }
            consume_signin_ndjson(BufReader::new(bytes.as_slice()))
        }
    }
}

fn consume_signin_ndjson<R: BufRead>(reader: R) -> Result<(), Error> {
    let mut final_error: Option<String> = None;
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(val) = serde_json::from_str::<Value>(trimmed) else {
            println!("{trimmed}");
            continue;
        };
        let kind = val.get("kind").and_then(Value::as_str).unwrap_or("");
        let text = val.get("text").and_then(Value::as_str).unwrap_or("");
        match kind {
            "link" => eprintln!("  Open sign-in link: {text}"),
            "code" => eprintln!("  Device code: {text}"),
            "prompt" => eprintln!("  {text}"),
            "done" => {
                if ui::is_pretty() {
                    if text.is_empty() {
                        ui::success("Native sign-in completed.");
                    } else {
                        ui::success(&format!("Native sign-in completed ({text})."));
                    }
                } else {
                    println!("done\t{}", if text.is_empty() { "-" } else { text });
                }
            }
            "error" => {
                final_error = Some(text.to_string());
            }
            _ => println!("{trimmed}"),
        }
    }
    if let Some(err) = final_error {
        return Err(Error::Message(err));
    }
    Ok(())
}

fn parse_env_file_pairs(raw: &str) -> Result<BTreeMap<String, String>, Error> {
    let mut map = BTreeMap::new();
    for (idx, line) in raw.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let cleaned = trimmed.strip_prefix("export ").unwrap_or(trimmed).trim();
        let Some((k, v)) = cleaned.split_once('=') else {
            return Err(Error::Message(format!(
                "invalid env-file line {}: expected KEY=VALUE",
                idx + 1
            )));
        };
        let key = k.trim();
        let mut val = v.trim();
        if val.len() >= 2
            && ((val.starts_with('"') && val.ends_with('"'))
                || (val.starts_with('\'') && val.ends_with('\'')))
        {
            val = &val[1..val.len() - 1];
        }
        map.insert(key.to_string(), val.to_string());
    }
    Ok(map)
}

/// Dispatch `anyagents secrets <subcommand>` against the strictly write-only `/v1/secrets*` API.
pub(crate) fn run_secrets_cmd(data: &Path, args: SecretsCliArgs) -> Result<(), Error> {
    match args.action {
        SecretsCommand::List { url, json } => {
            let resp: SecretsListResponse =
                rest_call_json(data, url.as_deref(), "GET", "/v1/secrets", None)?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&resp).unwrap_or_else(|_| "{}".into())
                );
                return Ok(());
            }
            render_secrets_list(&resp.secrets);
            Ok(())
        }
        SecretsCommand::Set {
            name,
            env_file,
            from_env,
            stdin_json,
            url,
            json,
        } => {
            let mut env_map = BTreeMap::new();
            if let Some(ref path) = env_file {
                let raw = std::fs::read_to_string(path)?;
                env_map.extend(parse_env_file_pairs(&raw)?);
            }
            for key in &from_env {
                let val = std::env::var(key).map_err(|_| {
                    Error::Message(format!("environment variable `{key}` is not set"))
                })?;
                env_map.insert(key.clone(), val);
            }
            if stdin_json || (env_file.is_none() && from_env.is_empty()) {
                let mut raw = String::new();
                std::io::stdin().read_to_string(&mut raw)?;
                let trimmed = raw.trim();
                if !trimmed.is_empty() {
                    if trimmed.starts_with('{') {
                        let parsed: BTreeMap<String, String> = serde_json::from_str(trimmed)
                            .map_err(|err| {
                                Error::Message(format!(
                                    "invalid stdin JSON (expected {{\"KEY\":\"VALUE\"}}): {err}"
                                ))
                            })?;
                        env_map.extend(parsed);
                    } else {
                        env_map.extend(parse_env_file_pairs(trimmed)?);
                    }
                }
            }
            if env_map.is_empty() {
                return Err(Error::Message(
                    "no secret key/value pairs supplied; use --env-file, --from-env, or pipe KEY=VALUE / JSON on stdin"
                        .into(),
                ));
            }
            let req = UpsertSecretRequest { env: env_map };
            let body = serde_json::to_value(&req).map_err(|err| Error::Message(err.to_string()))?;
            let path = format!("/v1/secrets/{}", crate::mcp::urlencoding_simple_pub(&name));
            let meta: SecretMetadataView =
                rest_call_json(data, url.as_deref(), "PUT", &path, Some(&body))?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&meta).unwrap_or_else(|_| "{}".into())
                );
            } else if ui::is_pretty() {
                ui::success(&format!(
                    "Stored write-only secret `{}` (keys: {})",
                    meta.name,
                    meta.env_keys.join(", ")
                ));
            } else {
                println!("stored\t{}\t{}", meta.name, meta.env_keys.join(","));
            }
            Ok(())
        }
        SecretsCommand::Grant {
            agent,
            secrets,
            url,
            json,
        } => mutate_agent_secret_grants(data, &agent, &secrets, true, url.as_deref(), json),
        SecretsCommand::Revoke {
            agent,
            secrets,
            url,
            json,
        } => mutate_agent_secret_grants(data, &agent, &secrets, false, url.as_deref(), json),
        SecretsCommand::Delete { name, url, json } => {
            let path = format!("/v1/secrets/{}", crate::mcp::urlencoding_simple_pub(&name));
            let resp: DeleteSecretResponse =
                rest_call_json(data, url.as_deref(), "DELETE", &path, None)?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&resp).unwrap_or_else(|_| "{}".into())
                );
            } else if ui::is_pretty() {
                ui::success(&format!("Deleted secret `{}`", resp.name));
            } else {
                println!("deleted\t{}", resp.name);
            }
            Ok(())
        }
    }
}

fn mutate_agent_secret_grants(
    data: &Path,
    agent_name: &str,
    delta: &[String],
    grant: bool,
    url: Option<&str>,
    json_out: bool,
) -> Result<(), Error> {
    let path = format!(
        "/v1/agents/{}/secrets",
        crate::mcp::urlencoding_simple_pub(agent_name)
    );
    let current: AgentSecretsResponse = rest_call_json(data, url, "GET", &path, None)?;
    let mut desired: BTreeSet<String> = current.secrets.into_iter().collect();
    for item in delta {
        let clean = item.trim();
        if clean.is_empty() {
            continue;
        }
        if grant {
            desired.insert(clean.to_string());
        } else {
            desired.remove(clean);
        }
    }
    let req = SetAgentSecretsRequest {
        secrets: desired.into_iter().collect(),
    };
    let body = serde_json::to_value(&req).map_err(|err| Error::Message(err.to_string()))?;
    let updated: AgentSecretsResponse = rest_call_json(data, url, "PUT", &path, Some(&body))?;
    if json_out {
        println!(
            "{}",
            serde_json::to_string_pretty(&updated).unwrap_or_else(|_| "{}".into())
        );
    } else if ui::is_pretty() {
        ui::success(&format!(
            "Updated secret grants for `{}`: [{}]",
            updated.agent,
            updated.secrets.join(", ")
        ));
    } else {
        println!(
            "{}\t{}",
            updated.agent,
            if updated.secrets.is_empty() {
                "-".to_string()
            } else {
                updated.secrets.join(",")
            }
        );
    }
    Ok(())
}

fn render_secrets_list(secrets: &[SecretMetadataView]) {
    if ui::is_pretty() {
        if secrets.is_empty() {
            println!("  {}", ui::dim("No deployment secrets stored."));
            return;
        }
        println!("\n  {}\n", ui::bold_cyan("Deployment Secrets (write-only)"));
        let mut table = ui::Table::new(&[
            ("NAME", false),
            ("ENV KEYS", false),
            ("GRANTED AGENTS", false),
        ]);
        for s in secrets {
            table.add_row(vec![
                ui::bold(&s.name),
                s.env_keys.join(", "),
                if s.granted_agents.is_empty() {
                    ui::dim("-")
                } else {
                    s.granted_agents.join(", ")
                },
            ]);
        }
        table.print();
        println!();
        return;
    }
    for s in secrets {
        println!(
            "{}\t{}\t{}",
            s.name,
            s.env_keys.join(","),
            if s.granted_agents.is_empty() {
                "-".to_string()
            } else {
                s.granted_agents.join(",")
            }
        );
    }
}

/// Dispatch `anyagents usage [...]` against `GET /v1/usage` (remote Bearer HTTP or local socket/store).
pub(crate) fn run_usage_cmd(data: &Path, args: UsageCliArgs) -> Result<(), Error> {
    let group_by_raw = args.group_by.or(args.by);
    if let Some(ref raw) = group_by_raw {
        UsageGroupBy::parse(raw).map_err(Error::Message)?;
    }

    let endpoint = resolve_control_endpoint(data, args.url.as_deref())?;
    let is_remote = matches!(endpoint, ControlEndpoint::Remote { .. });

    if is_remote || args.json {
        let path = build_usage_query_path(
            args.agent.as_deref(),
            group_by_raw.as_deref(),
            args.include_descendants.then_some(true),
            args.since_ms,
            args.until_ms,
            args.days,
        );
        let report: UsageReport = rest_call_json(data, args.url.as_deref(), "GET", &path, None)?;
        if args.json {
            println!(
                "{}",
                serde_json::to_string_pretty(&report).unwrap_or_else(|_| "{}".into())
            );
        } else {
            print_usage_report(&report);
        }
        return Ok(());
    }

    agent::usage(
        data,
        &agent::UsageParams {
            agent: args.agent,
            group_by: group_by_raw,
            since_ms: args.since_ms,
            until_ms: args.until_ms,
            days: args.days,
            include_descendants: args.include_descendants.then_some(true),
        },
    )
    .map_err(|err| match err {
        agent::UsageError::Store(err) => err,
        agent::UsageError::Refused(text) => Error::Message(text),
    })
}

fn build_usage_query_path(
    agent_name: Option<&str>,
    group_by: Option<&str>,
    include_descendants: Option<bool>,
    since_ms: Option<i64>,
    until_ms: Option<i64>,
    days: Option<i64>,
) -> String {
    let mut qs: Vec<(&str, String)> = Vec::new();
    if let Some(a) = agent_name.map(str::trim).filter(|s| !s.is_empty()) {
        qs.push(("agent", a.to_string()));
    }
    if let Some(v) = include_descendants {
        qs.push(("include_descendants", v.to_string()));
    }
    if let Some(v) = since_ms {
        qs.push(("since_ms", v.to_string()));
    }
    if let Some(v) = until_ms {
        qs.push(("until_ms", v.to_string()));
    }
    if let Some(v) = days {
        qs.push(("days", v.to_string()));
    }
    if let Some(g) = group_by.map(str::trim).filter(|s| !s.is_empty()) {
        qs.push(("group_by", g.to_string()));
    }
    if qs.is_empty() {
        "/v1/usage".to_string()
    } else {
        let q = qs
            .iter()
            .map(|(k, v)| format!("{k}={}", crate::mcp::urlencoding_simple_pub(v)))
            .collect::<Vec<_>>()
            .join("&");
        format!("/v1/usage?{q}")
    }
}

/// Render a `UsageReport` (handling `group_by=profile` per-profile rollups cleanly).
pub(crate) fn print_usage_report(report: &UsageReport) {
    if matches!(report.group_by, UsageGroupBy::Profile) {
        if ui::is_pretty() {
            if report.profiles.is_empty() {
                println!("  {}", ui::dim("No usage records found."));
                return;
            }
            println!("\n  {}\n", ui::bold_cyan("Token Usage by Profile"));
            let mut table = ui::Table::new(&[
                ("PROFILE", false),
                ("AGENTS", false),
                ("TURNS", true),
                ("INPUT", true),
                ("OUTPUT", true),
                ("CACHE READ", true),
                ("CACHE WRITE", true),
                ("COST", true),
            ]);
            for p in &report.profiles {
                let cost = match p.totals.reported_cost_micros {
                    Some(micros) => ui::green(&format!("${:.4}", micros as f64 / 1_000_000.0)),
                    None => ui::dim("-"),
                };
                table.add_row(vec![
                    ui::bold(&p.profile),
                    ui::dim(&p.agent_names.join(", ")),
                    p.totals.turns.to_string(),
                    ui::format_tokens(p.totals.input_tokens),
                    ui::format_tokens(p.totals.output_tokens),
                    ui::format_tokens(p.totals.cache_read_tokens),
                    ui::format_tokens(p.totals.cache_write_tokens),
                    cost,
                ]);
            }
            table.print();
            println!();
            return;
        }
        for p in &report.profiles {
            let cost = match p.totals.reported_cost_micros {
                Some(micros) => micros.to_string(),
                None => "-".to_string(),
            };
            println!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{cost}",
                p.profile,
                p.agent_names.join(","),
                p.totals.input_tokens,
                p.totals.output_tokens,
                p.totals.cache_read_tokens,
                p.totals.cache_write_tokens,
            );
        }
        return;
    }

    agent::print_report(report);
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::auth_device::{StoredControlAuth, save_control_auth_at};

    fn spawn_capture_server(
        handler: impl Fn(String, String, Option<String>, String) -> (u16, String, String)
        + Send
        + Sync
        + 'static,
    ) -> (String, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let base = format!("http://{addr}");
        let handler = Arc::new(handler);
        let handle = std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut req_line = String::new();
                if reader.read_line(&mut req_line).unwrap_or(0) == 0 {
                    continue;
                }
                let mut parts = req_line.split_whitespace();
                let method = parts.next().unwrap_or("").to_string();
                let path = parts.next().unwrap_or("").to_string();
                let mut content_len = 0usize;
                let mut auth_hdr = None;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 {
                        break;
                    }
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        break;
                    }
                    if let Some((k, v)) = trimmed.split_once(':') {
                        if k.trim().eq_ignore_ascii_case("content-length") {
                            content_len = v.trim().parse().unwrap_or(0);
                        } else if k.trim().eq_ignore_ascii_case("authorization") {
                            auth_hdr = Some(v.trim().to_string());
                        }
                    }
                }
                let mut body_buf = vec![0u8; content_len];
                if content_len > 0 {
                    let _ = reader.read_exact(&mut body_buf);
                }
                let body_str = String::from_utf8_lossy(&body_buf).to_string();
                let (status, content_type, resp_body) = handler(method, path, auth_hdr, body_str);
                let mut stream = stream;
                let resp = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{resp_body}",
                    resp_body.len()
                );
                let _ = stream.write_all(resp.as_bytes());
            }
        });
        (base, handle)
    }

    #[test]
    fn control_cli_subcommands_call_unified_rest_endpoints_with_bearer_token() {
        let _env = crate::server::tests::env_lock();
        let prev_auth_file = std::env::var_os("ANYAGENTS_CONTROL_AUTH_FILE");
        let prev_ctrl_url = std::env::var_os("ANYAGENTS_CONTROL_URL");

        let seen = Arc::new(Mutex::new(
            Vec::<(String, String, Option<String>, String)>::new(),
        ));
        let seen_srv = Arc::clone(&seen);
        let (base, _handle) = spawn_capture_server(move |method, path, auth, body| {
            seen_srv
                .lock()
                .unwrap()
                .push((method.clone(), path.clone(), auth, body));
            match (method.as_str(), path.as_str()) {
                ("GET", "/v1/profiles?fresh=1") => (
                    200,
                    "application/json".into(),
                    json!([{
                        "provider": "openai",
                        "profile": "default",
                        "kind": "subscription",
                        "compatible_harnesses": ["codex"],
                        "assigned_agents": ["alpha"],
                        "subagent_count": 0,
                        "usage": "5h: 82% left · 7d: 64% left (plus)",
                        "quota_5h": { "remaining_pct": 82 },
                        "quota_weekly": { "remaining_pct": 64 }
                    }])
                    .to_string(),
                ),
                ("POST", "/v1/profiles/native-signin") => (
                    200,
                    "application/x-ndjson".into(),
                    "{\"kind\":\"link\",\"text\":\"https://auth.example.com/device\"}\n{\"kind\":\"code\",\"text\":\"ABCD-1234\"}\n{\"kind\":\"done\",\"text\":\"user@example.com\"}\n".into(),
                ),
                ("GET", "/v1/secrets") => (
                    200,
                    "application/json".into(),
                    json!({ "secrets": [{
                        "name": "ci-token",
                        "env_keys": ["CI_KEY"],
                        "updated_at_ms": 1700000000000i64,
                        "granted_agent_ids": [],
                        "granted_agents": []
                    }]})
                    .to_string(),
                ),
                ("PUT", "/v1/secrets/ci-token") => (
                    200,
                    "application/json".into(),
                    json!({
                        "name": "ci-token",
                        "env_keys": ["CI_KEY"],
                        "updated_at_ms": 1700000000000i64,
                        "granted_agent_ids": [],
                        "granted_agents": []
                    })
                    .to_string(),
                ),
                ("GET", "/v1/agents/alpha/secrets") => (
                    200,
                    "application/json".into(),
                    json!({ "agent": "alpha", "agent_id": "agt_1", "secrets": ["existing"] })
                        .to_string(),
                ),
                ("PUT", "/v1/agents/alpha/secrets") => (
                    200,
                    "application/json".into(),
                    json!({ "agent": "alpha", "agent_id": "agt_1", "secrets": ["ci-token", "existing"] })
                        .to_string(),
                ),
                ("DELETE", "/v1/secrets/ci-token") => (
                    200,
                    "application/json".into(),
                    json!({ "deleted": true, "name": "ci-token" }).to_string(),
                ),
                ("GET", "/v1/usage?group_by=profile") => (
                    200,
                    "application/json".into(),
                    json!({
                        "group_by": "profile",
                        "agent_ids": [],
                        "totals": {
                            "turns": 3,
                            "input_tokens": 1000,
                            "output_tokens": 500,
                            "cache_read_tokens": 0,
                            "cache_write_tokens": 0,
                            "reported_cost_micros": 120000
                        },
                        "buckets": [],
                        "profiles": [{
                            "profile": "anthropic/acct-1",
                            "agent_ids": [],
                            "agent_names": ["alpha", "beta"],
                            "turns": 3,
                            "input_tokens": 1000,
                            "output_tokens": 500,
                            "cache_read_tokens": 0,
                            "cache_write_tokens": 0,
                            "reported_cost_micros": 120000
                        }]
                    })
                    .to_string(),
                ),
                _ => (404, "application/json".into(), r#"{"error":"not found"}"#.into()),
            }
        });

        let dir = tempfile::tempdir().unwrap();
        let auth_file = dir.path().join("state").join("control-auth.json");
        save_control_auth_at(
            &auth_file,
            &StoredControlAuth {
                coordinator_url: base.clone(),
                resource: base.clone(),
                issuer: "https://idp.example.com".into(),
                client_id: "cli".into(),
                access_token: "test-bearer-jwt-token".into(),
                id_token: None,
                refresh_token: None,
                token_type: Some("Bearer".into()),
                scope: Some("openid email profile".into()),
                expires_at_unix: Some(4_000_000_000),
                email: Some("ops@example.com".into()),
                obtained_at_unix: 1_750_000_000,
            },
        )
        .unwrap();

        unsafe {
            std::env::set_var("ANYAGENTS_CONTROL_AUTH_FILE", &auth_file);
            std::env::remove_var("ANYAGENTS_CONTROL_URL");
        }

        let env_file = dir.path().join("secret.env");
        std::fs::write(&env_file, "CI_KEY=top-secret-value\n").unwrap();

        let data = dir.path().join("data");
        std::fs::create_dir_all(&data).unwrap();

        profiles_list_cmd(&data, None, true, Some(&base), false).unwrap();
        profiles_native_signin_cmd(&data, "codex", Some("acct-1"), Some(&base)).unwrap();
        run_secrets_cmd(
            &data,
            SecretsCliArgs {
                action: SecretsCommand::List {
                    url: Some(base.clone()),
                    json: false,
                },
            },
        )
        .unwrap();
        run_secrets_cmd(
            &data,
            SecretsCliArgs {
                action: SecretsCommand::Set {
                    name: "ci-token".into(),
                    env_file: Some(env_file),
                    from_env: vec![],
                    stdin_json: false,
                    url: Some(base.clone()),
                    json: false,
                },
            },
        )
        .unwrap();
        run_secrets_cmd(
            &data,
            SecretsCliArgs {
                action: SecretsCommand::Grant {
                    agent: "alpha".into(),
                    secrets: vec!["ci-token".into()],
                    url: Some(base.clone()),
                    json: false,
                },
            },
        )
        .unwrap();
        run_secrets_cmd(
            &data,
            SecretsCliArgs {
                action: SecretsCommand::Delete {
                    name: "ci-token".into(),
                    url: Some(base.clone()),
                    json: false,
                },
            },
        )
        .unwrap();
        run_usage_cmd(
            &data,
            UsageCliArgs {
                agent: None,
                by: None,
                group_by: Some("profile".into()),
                include_descendants: false,
                since_ms: None,
                until_ms: None,
                days: None,
                url: Some(base.clone()),
                json: false,
            },
        )
        .unwrap();

        match prev_auth_file {
            Some(v) => unsafe { std::env::set_var("ANYAGENTS_CONTROL_AUTH_FILE", v) },
            None => unsafe { std::env::remove_var("ANYAGENTS_CONTROL_AUTH_FILE") },
        }
        match prev_ctrl_url {
            Some(v) => unsafe { std::env::set_var("ANYAGENTS_CONTROL_URL", v) },
            None => unsafe { std::env::remove_var("ANYAGENTS_CONTROL_URL") },
        }

        let calls = seen.lock().unwrap();
        assert_eq!(calls.len(), 8);
        assert_eq!(calls[0].0, "GET");
        assert_eq!(calls[0].1, "/v1/profiles?fresh=1");
        assert_eq!(calls[0].2.as_deref(), Some("Bearer test-bearer-jwt-token"));

        assert_eq!(calls[1].0, "POST");
        assert_eq!(calls[1].1, "/v1/profiles/native-signin");
        assert!(calls[1].3.contains("\"codex\""));
        assert!(calls[1].3.contains("\"acct-1\""));

        assert_eq!(calls[2].1, "/v1/secrets");
        assert_eq!(calls[3].0, "PUT");
        assert_eq!(calls[3].1, "/v1/secrets/ci-token");
        assert_eq!(calls[4].0, "GET");
        assert_eq!(calls[4].1, "/v1/agents/alpha/secrets");
        assert_eq!(calls[5].0, "PUT");
        assert_eq!(calls[5].1, "/v1/agents/alpha/secrets");
        assert!(calls[5].3.contains("\"ci-token\"") && calls[5].3.contains("\"existing\""));
        assert_eq!(calls[6].0, "DELETE");
        assert_eq!(calls[6].1, "/v1/secrets/ci-token");
        assert_eq!(calls[7].0, "GET");
        assert_eq!(calls[7].1, "/v1/usage?group_by=profile");
    }
}
