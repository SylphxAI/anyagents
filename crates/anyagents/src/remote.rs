//! Remote (cloud) sessions: how a harness that runs off this host calls back
//! into the control plane.
//!
//! * `public_url` in the `[server]` table of `<data>/anyagents.toml` names the
//!   origin this server's public listener is reachable at. Every callback URL
//!   handed to a remote session derives from it. Without it no remote agent is
//!   created or started: a loopback default would hand a remote vendor an
//!   address it cannot reach.
//! * Each launch of a remote harness begins a remote session. The server issues
//!   a short-lived token bound to that agent and that session
//!   ([`anyagents_core::Store::begin_remote_session`]) and writes it, with the
//!   MCP URL, into the agent's config directory, where the adapter reads both
//!   before each turn. The maintenance pass rotates the token before it
//!   expires. The long-lived local mailbox capability never leaves this host.
//! * Every remote session receives the exact same universal MCP surface
//!   ([`crate::mcp::UNIVERSAL_MCP_TOOLS`]) as local sessions.
//! * `public_listen` in the same table binds the public listener: its own port,
//!   serving readiness, Telegram webhooks and MCP for remote session tokens
//!   only. Every local credential stays on the socket and the loopback API
//!   (ADR 0010 D2). The separation is by listener, never by source address.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use anyagents_core::{Agent, AgentId, Error, Store};
use serde::Deserialize;

/// The deployment's configuration file, in the data directory.
pub const CONFIG_FILE: &str = "anyagents.toml";
/// The file in an agent's config directory that holds its current remote
/// session token. Written only by the server.
pub const TOKEN_FILE: &str = "remote-session-token";
/// The file in an agent's config directory that holds the MCP URL its remote
/// session calls back to. Written by the server from `public_url`.
pub const MCP_URL_FILE: &str = ".anyagents-mcp-url";
/// Where the public listener serves MCP, below `public_url`.
pub const MCP_PATH: &str = "/v1/webhooks/mcp";

/// Remote session token lifetime when `remote_session_ttl_secs` is unset.
pub const DEFAULT_TTL: Duration = Duration::from_secs(60 * 60);
/// How often the maintenance pass looks for tokens to rotate.
pub const ROTATION_INTERVAL: Duration = Duration::from_secs(60);
/// The longest supported remote turn. The Spiron REST adapter uses this
/// same budget instead of maintaining a separate, potentially larger limit.
pub const LONGEST_TURN: Duration = Duration::from_secs(30 * 60);
/// A blocking REST request can finish this long after a turn deadline.
pub const REQUEST_MARGIN: Duration = Duration::from_secs(30);
/// Bounds on the configured lifetime. Existing 1800-second configurations
/// remain valid; issuance applies `MIN_SAFE_TTL` without changing the config.
pub const MIN_TTL: Duration = Duration::from_secs(30 * 60);
pub const MAX_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// A token read just before the next rotation pass has at least half its
/// issued lifetime minus one maintenance interval left. Include a blocking
/// request margin, and keep that remainder strictly above the turn budget.
pub const MIN_SAFE_TTL: Duration = Duration::from_secs(
    2 * (LONGEST_TURN.as_secs() + ROTATION_INTERVAL.as_secs() + REQUEST_MARGIN.as_secs() + 1),
);
const _: () = assert!(
    MIN_SAFE_TTL.as_secs() / 2
        > ROTATION_INTERVAL.as_secs() + LONGEST_TURN.as_secs() + REQUEST_MARGIN.as_secs(),
    "a token read at turn start must outlive the longest turn and request margin"
);
/// Revoked and expired token rows are forgotten after this long.
const PRUNE_AFTER_MS: i64 = 7 * 24 * 60 * 60 * 1000;

/// One writer at a time for an agent's session files: a launch beginning a new
/// session and the rotation pass must never leave an older token in the file.
static SESSION_FILES: Mutex<()> = Mutex::new(());

#[derive(Debug, Default, Deserialize)]
struct ConfigFile {
    #[serde(default)]
    server: ServerTable,
}

/// `[server]`. Unknown keys are refused so a misspelt key fails loudly
/// instead of silently leaving remote agents unconfigured.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerTable {
    public_url: Option<String>,
    public_listen: Option<String>,
    remote_session_ttl_secs: Option<u64>,
    webhook_secret: Option<String>,
}

/// The server settings remote sessions depend on.
#[derive(Clone, PartialEq, Eq)]
pub struct Settings {
    /// The normalized public origin (and optional base path), no trailing `/`.
    pub public_url: Option<String>,
    /// Where the public listener binds, when it is configured.
    pub public_listen: Option<SocketAddr>,
    pub remote_session_ttl: Duration,
    webhook_secret: Option<String>,
}

impl std::fmt::Debug for Settings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Settings")
            .field("public_url", &self.public_url)
            .field("public_listen", &self.public_listen)
            .field("remote_session_ttl", &self.remote_session_ttl)
            .field(
                "webhook_secret",
                &self.webhook_secret.as_ref().map(|_| "[redacted]"),
            )
            .finish()
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            public_url: None,
            public_listen: None,
            remote_session_ttl: DEFAULT_TTL,
            webhook_secret: None,
        }
    }
}

impl Settings {
    /// Read `<data>/anyagents.toml` with optional environment overrides
    /// (`ANYAGENTS_PUBLIC_URL`, `ANYAGENTS_PUBLIC_LISTEN`,
    /// `ANYAGENTS_REMOTE_SESSION_TTL_SECS`) when host storage env is active.
    /// A missing file uses defaults plus env; a file or env value that cannot
    /// be parsed is an error, never a silent default.
    pub fn load(data_dir: &Path) -> Result<Self, String> {
        let path = data_dir.join(CONFIG_FILE);
        let mut settings = match std::fs::read_to_string(&path) {
            Ok(text) => Self::parse(&text, &path)?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Self::default(),
            Err(err) => return Err(format!("cannot read {}: {err}", path.display())),
        };
        if anyagents_core::storage_env_enabled() {
            if settings.public_url.is_none()
                && let Some((env_name, raw)) = std::env::var("ANYAGENTS_PUBLIC_URL")
                    .ok()
                    .filter(|v| !v.trim().is_empty())
                    .map(|v| ("ANYAGENTS_PUBLIC_URL", v))
                    .or_else(|| {
                        std::env::var("ANYAGENTS_WEBHOOK_URL")
                            .ok()
                            .filter(|v| !v.trim().is_empty())
                            .map(|v| ("ANYAGENTS_WEBHOOK_URL", v))
                    })
            {
                settings.public_url =
                    Some(normalize_public_url(&raw).map_err(|err| format!("{env_name} {err}"))?);
            }
            if settings.public_listen.is_none()
                && let Ok(raw) = std::env::var("ANYAGENTS_PUBLIC_LISTEN")
                && !raw.trim().is_empty()
            {
                settings.public_listen = Some(raw.trim().parse::<SocketAddr>().map_err(|_| {
                    format!(
                        "ANYAGENTS_PUBLIC_LISTEN `{raw}` must be an `address:port` such as `0.0.0.0:8789`"
                    )
                })?);
            }
            if settings.remote_session_ttl == DEFAULT_TTL
                && let Ok(raw) = std::env::var("ANYAGENTS_REMOTE_SESSION_TTL_SECS")
                && !raw.trim().is_empty()
            {
                let secs = raw.trim().parse::<u64>().map_err(|_| {
                    format!("ANYAGENTS_REMOTE_SESSION_TTL_SECS `{raw}` must be an integer")
                })?;
                let ttl = Duration::from_secs(secs);
                if ttl < MIN_TTL || ttl > MAX_TTL {
                    return Err(format!(
                        "ANYAGENTS_REMOTE_SESSION_TTL_SECS must be between {} and {}",
                        MIN_TTL.as_secs(),
                        MAX_TTL.as_secs()
                    ));
                }
                settings.remote_session_ttl = ttl;
            }
        }
        Ok(settings)
    }

    pub fn parse(text: &str, origin: &Path) -> Result<Self, String> {
        let file: ConfigFile =
            toml::from_str(text).map_err(|err| format!("{}: {err}", origin.display()))?;
        let public_url = file
            .server
            .public_url
            .as_deref()
            .map(normalize_public_url)
            .transpose()
            .map_err(|err| format!("{}: [server] public_url {err}", origin.display()))?;
        let public_listen = file
            .server
            .public_listen
            .as_deref()
            .map(|raw| {
                raw.trim().parse::<SocketAddr>().map_err(|_| {
                    format!(
                        "{}: [server] public_listen `{raw}` must be an `address:port` such as `0.0.0.0:8789`",
                        origin.display()
                    )
                })
            })
            .transpose()?;
        let remote_session_ttl = match file.server.remote_session_ttl_secs {
            None => DEFAULT_TTL,
            Some(secs) => {
                let ttl = Duration::from_secs(secs);
                if ttl < MIN_TTL || ttl > MAX_TTL {
                    return Err(format!(
                        "{}: [server] remote_session_ttl_secs must be between {} and {}",
                        origin.display(),
                        MIN_TTL.as_secs(),
                        MAX_TTL.as_secs()
                    ));
                }
                ttl
            }
        };
        Ok(Self {
            public_url,
            public_listen,
            remote_session_ttl,
            webhook_secret: file.server.webhook_secret,
        })
    }

    /// Explicit config (including empty = disabled) overrides the environment.
    pub fn webhook_secret(&self) -> Option<String> {
        self.resolve_webhook_secret(std::env::var("ANYAGENTS_WEBHOOK_SECRET").ok())
    }

    fn resolve_webhook_secret(&self, environment: Option<String>) -> Option<String> {
        self.webhook_secret
            .clone()
            .or(environment)
            .filter(|secret| !secret.is_empty())
    }

    /// The MCP URL a remote session calls back to, when `public_url` is set.
    pub fn mcp_url(&self) -> Option<String> {
        self.public_url
            .as_deref()
            .map(|base| format!("{base}{MCP_PATH}"))
    }

    fn ttl_ms(&self) -> i64 {
        i64::try_from(self.remote_session_ttl.max(MIN_SAFE_TTL).as_millis()).unwrap_or(i64::MAX)
    }
}

/// Check and normalize a `public_url`: `https://host[:port][/base]`, or
/// `http://` on a loopback host for a local test origin. No user info, query or
/// fragment; a trailing `/` is dropped.
pub fn normalize_public_url(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err("is empty".into());
    }
    if trimmed
        .chars()
        .any(|c| c.is_whitespace() || c.is_control() || matches!(c, '?' | '#' | '@' | '\\'))
    {
        return Err(format!(
            "`{trimmed}` must be a plain origin and path, without spaces, user info, query or fragment"
        ));
    }
    let (scheme, rest) = trimmed
        .split_once("://")
        .ok_or_else(|| format!("`{trimmed}` must start with `https://`"))?;
    let authority = rest.split('/').next().unwrap_or_default();
    let host = if let Some(bracketed) = authority.strip_prefix('[') {
        bracketed.split(']').next().unwrap_or_default()
    } else {
        authority.split(':').next().unwrap_or_default()
    };
    if host.is_empty() {
        return Err(format!("`{trimmed}` names no host"));
    }
    let loopback = matches!(
        host.to_ascii_lowercase().as_str(),
        "localhost" | "127.0.0.1" | "::1"
    );
    match scheme.to_ascii_lowercase().as_str() {
        "https" => {}
        "http" if loopback => {}
        "http" => {
            return Err(format!(
                "`{trimmed}` must use `https://`; plain `http://` is accepted only on a loopback host"
            ));
        }
        _ => return Err(format!("`{trimmed}` must start with `https://`")),
    }
    Ok(format!("{}://{rest}", scheme.to_ascii_lowercase()))
}

/// Whether this agent executes via a vendor-hosted cloud runtime (`cloud`).
///
/// Governed authoritatively by the agent's first-class `runtime` column (with
/// a fallback for legacy `*-cloud` harness values if a row bypassed
/// canonicalization): `local` and `sandbox[:provider]` agents never silently
/// hijack into cloud execution.
pub(crate) fn is_agent_cloud(
    agent: &Agent,
    _data_dir: Option<&Path>,
    _config_dir: Option<&Path>,
) -> bool {
    agent.runtime.is_cloud() || crate::vendor::is_cloud_harness(&agent.harness)
}

/// Declared delivery guarantee for an agent, respecting its first-class `runtime`
/// (`cloud` uses remote cloud delivery regardless of canonical harness name).
pub(crate) fn declared_delivery_for_agent(agent: &Agent) -> anyagents_harness::Delivery {
    if agent.runtime.is_cloud() {
        anyagents_harness_claude::CLOUD_DELIVERY
    } else {
        crate::vendor::declared_delivery(&agent.harness)
    }
}

/// Legacy `*-cloud` adapter selector for an agent routed to cloud execution
/// (`(spiron, Cloud) -> "spiron-cloud"`, `(claude-code, Cloud) -> "claude-cloud"`, etc.).
pub(crate) fn agent_cloud_selector(agent: &Agent) -> String {
    anyagents_harness::ResolvedExecution {
        harness: agent.canonical_harness(),
        runtime: anyagents_core::ExecutionRuntime::Cloud,
    }
    .legacy_harness_selector()
}

/// Resolve the public MCP callback URL for an off-host (cloud or sandbox) agent.
/// Prefers `<config>/.anyagents-mcp-url` written by the server from `public_url`,
/// falling back to `ANYAGENTS_PUBLIC_MCP_URL` or `<config>/cloud.json` `mcp_url`.
pub(crate) fn resolve_public_mcp_url(config: &Path) -> Result<String, String> {
    if let Ok(file_url) = std::fs::read_to_string(config.join(MCP_URL_FILE)) {
        let trimmed = file_url.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }
    if let Ok(url) = std::env::var("ANYAGENTS_PUBLIC_MCP_URL") {
        let trimmed = url.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }
    if let Ok(raw) = std::fs::read_to_string(config.join("cloud.json"))
        && let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw)
        && let Some(url) = val.get("mcp_url").and_then(serde_json::Value::as_str)
        && !url.trim().is_empty()
    {
        return Ok(url.trim().to_string());
    }
    Err(format!(
        "no MCP callback URL for this remote agent in {}: set `public_url` under `[server]` in \
         the server's `{CONFIG_FILE}` and start the agent again",
        config.display()
    ))
}

/// Whether this agent's harness runs off this host: a cloud harness, or a
/// harness spawned into a remote sandbox. These are the agents that hold
/// remote session tokens.
pub fn runs_off_host(agent: &Agent, config_dir: &Path) -> bool {
    let data_dir = config_dir
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent);
    agent.runtime.is_off_host()
        || crate::vendor::is_cloud_harness(&agent.harness)
        || crate::sandbox_exec::has_remote_sandbox_provider(data_dir)
        || crate::sandbox_providers::sylphx::SylphxLeaseConfig::from_config_dir(config_dir)
            .is_some()
        || crate::sandbox_providers::resolve_sandbox_config(Some(config_dir))
            .is_some_and(|sandbox| sandbox.remote_spawn)
}

/// Store-aware check for whether this agent runs off-host, consulting
/// PostgreSQL `agent_sandbox_bindings`, active `ProviderRegistry` providers,
/// and `remote_required` mode so peer replicas without pod-local `sandbox*.json`
/// files admit HTTP MCP properly.
pub fn runs_off_host_in(store: &Store, agent: &Agent, config_dir: &Path) -> bool {
    runs_off_host(agent, config_dir)
        || crate::sandbox_exec::has_remote_sandbox_provider(Some(store.data_dir()))
        || crate::sandbox_providers::SandboxMode::from_env().is_remote_required()
        || store.sandbox_binding(&agent.id).ok().flatten().is_some()
}

/// Why a remote agent cannot be created or started here, when it cannot.
pub fn require_public_url(data_dir: &Path) -> Result<Settings, String> {
    let settings = Settings::load(data_dir)?;
    if settings.public_url.is_none() {
        return Err(format!(
            "remote agents need this server's public URL: set `public_url` under `[server]` in {} \
             (for example `public_url = \"https://agents.example.com\"`) so a remote session can call \
             back to `<public_url>{MCP_PATH}`",
            data_dir.join(CONFIG_FILE).display()
        ));
    }
    Ok(settings)
}

/// Begin a remote session for an agent whose harness runs off this host:
/// revoke its earlier sessions, issue a fresh token, and write the token and
/// the MCP URL where its adapter reads them. Fails, and the launch with it,
/// when `public_url` is not configured.
pub fn begin_session(store: &Store, agent: &Agent) -> Result<(), String> {
    let settings = require_public_url(store.data_dir())
        .map_err(|why| format!("remote agent `{}` cannot start: {why}", agent.name))?;
    let mcp_url = settings.mcp_url().unwrap_or_default();
    let _files = SESSION_FILES.lock().unwrap_or_else(|err| err.into_inner());
    let issued = store
        .begin_remote_session(&agent.id, settings.ttl_ms())
        .map_err(|err| err.to_string())?;
    let config = anyagents_core::agent_config_dir(store.data_dir(), &agent.id);
    let written = std::fs::create_dir_all(&config)
        .map_err(Error::from)
        .and_then(|()| {
            anyagents_harness::atomic_write_nonempty_file(
                &config.join(MCP_URL_FILE),
                format!("{mcp_url}\n").as_bytes(),
                0o644,
            )
            .map_err(Error::from)
        })
        .and_then(|()| {
            crate::operator::write_agent_secret(store, &agent.id, TOKEN_FILE, &issued.token)
        });
    if let Err(err) = written {
        // A token its session cannot read is withdrawn, never left live.
        let _ = store.revoke_remote_session_token(&issued.token);
        return Err(format!(
            "remote agent `{}` cannot start: its session files were not written: {err}",
            agent.name
        ));
    }
    Ok(())
}

/// Rotate every live remote session whose newest token has less than half its
/// lifetime left, and forget long-dead token rows. The new token is written
/// where the adapter reads it before its next turn; the previous token keeps
/// its own expiry, so a turn already in flight finishes.
pub fn rotate_due(store: &Store) -> Result<usize, Error> {
    let settings = Settings::load(store.data_dir()).unwrap_or_else(|why| {
        eprintln!("anyagents-remote: {why}; rotating with the default lifetime");
        Settings::default()
    });
    let ttl_ms = settings.ttl_ms();
    let now = anyagents_core::now_ms();
    let mut rotated = 0;
    for session in store.live_remote_sessions(now)? {
        if session.expires_at_ms.saturating_sub(now) >= ttl_ms / 2 {
            continue;
        }
        let _files = SESSION_FILES.lock().unwrap_or_else(|err| err.into_inner());
        // A launch may have begun a newer session since the list was read;
        // rotating a revoked session fails and leaves its file alone.
        let Ok(issued) =
            store.rotate_remote_session(&session.agent_id, &session.session_id, ttl_ms)
        else {
            continue;
        };
        // A token that did not reach the file is withdrawn, so the session
        // stays due and the next pass tries again while the last token lives.
        if let Err(err) =
            crate::operator::write_agent_secret(store, &session.agent_id, TOKEN_FILE, &issued.token)
        {
            let _ = store.revoke_remote_session_token(&issued.token);
            eprintln!(
                "anyagents-remote: session {} of {} not rotated: {err}",
                session.session_id, session.agent_id
            );
            continue;
        }
        rotated += 1;
    }
    store.prune_remote_session_tokens(now.saturating_sub(PRUNE_AFTER_MS))?;
    Ok(rotated)
}

/// Revoke an agent's remote sessions on demand: one session when `session_id`
/// names it, else all. Returns how many tokens stopped authenticating. Once
/// none is live, the token file is removed.
pub fn revoke(store: &Store, agent_id: &AgentId, session_id: Option<&str>) -> Result<usize, Error> {
    let _files = SESSION_FILES.lock().unwrap_or_else(|err| err.into_inner());
    let revoked = store.revoke_remote_sessions(agent_id, session_id)?;
    let still_live = store
        .live_remote_sessions(anyagents_core::now_ms())?
        .iter()
        .any(|session| &session.agent_id == agent_id);
    if !still_live {
        let file = anyagents_core::agent_config_dir(store.data_dir(), agent_id).join(TOKEN_FILE);
        match std::fs::remove_file(&file) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
    }
    Ok(revoked)
}

/// The remote session token an adapter presents for this turn. Only a token
/// the server wrote is used: the adapter never falls back to the agent's local
/// capability, which must not leave this host.
pub fn session_token(config: &Path) -> Result<String, String> {
    let path = config.join(TOKEN_FILE);
    let raw = std::fs::read_to_string(&path).map_err(|err| {
        format!(
            "no remote session token at {} ({err}); the server writes one when it starts a remote agent",
            path.display()
        )
    })?;
    let token = raw.trim();
    if !anyagents_core::is_remote_session_token(token) {
        return Err(format!(
            "{} does not hold a remote session token",
            path.display()
        ));
    }
    Ok(token.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Result<Settings, String> {
        Settings::parse(text, Path::new("anyagents.toml"))
    }

    #[test]
    fn settings_default_without_a_file_and_read_the_server_table() {
        let dir = crate::test_tempdir();
        assert_eq!(Settings::load(dir.path()).unwrap(), Settings::default());
        assert_eq!(Settings::default().remote_session_ttl, DEFAULT_TTL);

        std::fs::write(
            dir.path().join(CONFIG_FILE),
            "[server]\npublic_url = \"https://Agents.Example.com/base/\"\npublic_listen = \"0.0.0.0:8789\"\nremote_session_ttl_secs = 1800\n",
        )
        .unwrap();
        let settings = Settings::load(dir.path()).unwrap();
        assert_eq!(
            settings.public_url.as_deref(),
            Some("https://Agents.Example.com/base")
        );
        assert_eq!(settings.remote_session_ttl, Duration::from_secs(1800));
        assert_eq!(
            settings.public_listen,
            Some("0.0.0.0:8789".parse().unwrap())
        );
        assert_eq!(Settings::default().public_listen, None);
        assert_eq!(
            settings.mcp_url().as_deref(),
            Some("https://Agents.Example.com/base/v1/webhooks/mcp")
        );
        // Other tables are left to their own readers.
        assert!(parse("[agents]\nanything = 1\n").is_ok());
    }

    #[test]
    fn configured_lifetimes_remain_valid_and_issuance_covers_the_turn_budget() {
        for secs in [1800, 3600, 3782, 7200, 86400] {
            let settings = parse(&format!(
                "[server]
remote_session_ttl_secs = {secs}
"
            ))
            .unwrap();
            assert_eq!(settings.remote_session_ttl, Duration::from_secs(secs));
            let issued_secs = u64::try_from(settings.ttl_ms()).unwrap() / 1000;
            assert_eq!(issued_secs, secs.max(3782));
            assert!(
                issued_secs / 2 - ROTATION_INTERVAL.as_secs()
                    > LONGEST_TURN.as_secs() + REQUEST_MARGIN.as_secs()
            );
        }
        assert_eq!(Settings::default().ttl_ms(), 3_782_000);
    }

    #[test]
    fn newly_issued_token_authenticates_through_the_latest_start_and_whole_turn() {
        let dir = crate::test_tempdir();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            "[server]
public_url = 'https://agents.example.com'
remote_session_ttl_secs = 1800
",
        )
        .unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("cloud", "spiron-cloud"))
            .unwrap();
        begin_session(&store, &agent).unwrap();
        let config = anyagents_core::agent_config_dir(dir.path(), &agent.id);
        let token = session_token(&config).unwrap();
        let grant = store.authenticate_remote_session(&token).unwrap();
        let issued_ms = 3_782_000;
        let created = grant.expires_at_ms - issued_ms;
        // A maintenance pass can lag the half-life boundary by one interval.
        let latest_read = created + issued_ms / 2 + 60_000;
        let turn_end = latest_read + 1_800_000 + 30_000;
        assert_eq!(grant.expires_at_ms - turn_end, 1000);
        assert!(
            store
                .authenticate_remote_session_at(&token, turn_end)
                .is_ok()
        );
        assert!(
            store
                .authenticate_remote_session_at(&token, grant.expires_at_ms)
                .is_err()
        );
        revoke(&store, &agent.id, None).unwrap();
        assert!(
            store
                .authenticate_remote_session_at(&token, turn_end)
                .is_err()
        );
    }

    #[test]
    fn rotation_upgrades_legacy_short_tokens_without_extending_or_reviving_them() {
        let dir = crate::test_tempdir();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            "[server]
public_url = 'https://agents.example.com'
remote_session_ttl_secs = 1800
",
        )
        .unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("cloud", "spiron-cloud"))
            .unwrap();
        // A token issued by the old daemon keeps its original expiry.
        let legacy = store.begin_remote_session(&agent.id, 1_800_000).unwrap();
        assert_eq!(rotate_due(&store).unwrap(), 1);
        let config = anyagents_core::agent_config_dir(dir.path(), &agent.id);
        let fresh = session_token(&config).unwrap();
        assert_ne!(fresh, legacy.token);
        let old_grant = store.authenticate_remote_session(&legacy.token).unwrap();
        let new_grant = store.authenticate_remote_session(&fresh).unwrap();
        assert_eq!(old_grant.expires_at_ms, legacy.expires_at_ms);
        assert_eq!(old_grant.session_id, new_grant.session_id);
        assert!(new_grant.expires_at_ms >= legacy.expires_at_ms + 1_982_000);
        assert_eq!(rotate_due(&store).unwrap(), 0);
        assert!(
            store
                .authenticate_remote_session_at(&legacy.token, legacy.expires_at_ms)
                .is_err()
        );
        assert!(
            store
                .authenticate_remote_session_at(&fresh, legacy.expires_at_ms)
                .is_ok()
        );
        assert_eq!(revoke(&store, &agent.id, None).unwrap(), 2);
        assert!(store.authenticate_remote_session(&legacy.token).is_err());
        assert!(store.authenticate_remote_session(&fresh).is_err());
        assert_eq!(rotate_due(&store).unwrap(), 0);
        assert!(
            store
                .rotate_remote_session(&agent.id, &legacy.session_id, 3_782_000)
                .is_err()
        );
        assert!(!config.join(TOKEN_FILE).exists());
    }

    #[test]
    fn settings_refuse_unsafe_urls_bad_lifetimes_and_misspelt_keys() {
        for bad in [
            "http://agents.example.com",
            "ftp://agents.example.com",
            "agents.example.com",
            "https://",
            "https://user:pass@agents.example.com",
            "https://agents.example.com/?x=1",
            "https://agents.example.com/#frag",
            "https://agents example.com",
        ] {
            let err = parse(&format!("[server]\npublic_url = \"{bad}\"\n")).unwrap_err();
            assert!(err.contains("public_url"), "{bad}: {err}");
        }
        for ok in [
            "http://127.0.0.1:8788",
            "http://localhost",
            "http://[::1]:8788",
        ] {
            assert!(
                parse(&format!("[server]\npublic_url = \"{ok}\"\n")).is_ok(),
                "{ok}"
            );
        }
        for secs in [0, 299, 1_799, 86_401] {
            let err = parse(&format!("[server]\nremote_session_ttl_secs = {secs}\n")).unwrap_err();
            assert!(err.contains("remote_session_ttl_secs"), "{err}");
        }
        assert!(parse("[server]\npublic-url = \"https://a.example.com\"\n").is_err());
        for bad in ["8789", "agents.example.com:8789", "0.0.0.0", ""] {
            let err = parse(&format!("[server]\npublic_listen = \"{bad}\"\n")).unwrap_err();
            assert!(err.contains("public_listen"), "{bad}: {err}");
        }
    }

    #[test]
    fn a_remote_agent_cannot_start_without_a_public_url() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("cloud", "claude-cloud"))
            .unwrap();
        let err = begin_session(&store, &agent).unwrap_err();
        assert!(err.contains("public_url"), "{err}");
        assert!(err.contains(CONFIG_FILE), "{err}");
        let config = anyagents_core::agent_config_dir(dir.path(), &agent.id);
        assert!(!config.join(TOKEN_FILE).exists());
        assert!(store.live_remote_sessions(0).unwrap().is_empty());
    }

    #[test]
    fn a_session_whose_files_cannot_be_written_leaves_no_live_token() {
        let dir = crate::test_tempdir();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            "[server]\npublic_url = \"https://agents.example.com\"\n",
        )
        .unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("cloud", "claude-cloud"))
            .unwrap();
        let config = anyagents_core::agent_config_dir(dir.path(), &agent.id);
        std::fs::create_dir_all(config.parent().unwrap()).unwrap();
        std::fs::write(&config, "not a directory").unwrap();
        let err = begin_session(&store, &agent).unwrap_err();
        assert!(err.contains("session files were not written"), "{err}");
        assert!(store.live_remote_sessions(0).unwrap().is_empty());
    }

    #[test]
    fn a_session_writes_its_files_rotates_before_expiry_and_revokes_on_demand() {
        let dir = crate::test_tempdir();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            "[server]\npublic_url = \"https://agents.example.com\"\n",
        )
        .unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("cloud", "claude-cloud"))
            .unwrap();
        begin_session(&store, &agent).unwrap();
        let config = anyagents_core::agent_config_dir(dir.path(), &agent.id);
        assert_eq!(
            std::fs::read_to_string(config.join(MCP_URL_FILE)).unwrap(),
            "https://agents.example.com/v1/webhooks/mcp\n"
        );
        let first = session_token(&config).unwrap();
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(config.join(TOKEN_FILE))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o077, 0, "the token file is private: {mode:o}");
        }
        let grant = store.authenticate_remote_session(&first).unwrap();
        assert_eq!(grant.agent_id, agent.id);

        // A fresh token has its whole lifetime left: nothing to rotate.
        assert_eq!(rotate_due(&store).unwrap(), 0);
        assert_eq!(session_token(&config).unwrap(), first);

        // Age the session past half its lifetime: the pass rotates it, and the
        // old token still completes a turn already in flight.
        let soon = anyagents_core::now_ms() + 60_000;
        rusqlite::Connection::open(dir.path().join("state.db"))
            .unwrap()
            .execute(
                "UPDATE remote_session_tokens SET expires_at_ms = ?1",
                [soon],
            )
            .unwrap();
        assert_eq!(rotate_due(&store).unwrap(), 1);
        let second = session_token(&config).unwrap();
        assert_ne!(second, first);
        assert_eq!(
            store
                .authenticate_remote_session(&second)
                .unwrap()
                .session_id,
            grant.session_id
        );
        assert!(store.authenticate_remote_session(&first).is_ok());

        // On demand: every token of the session stops at once, and the file goes.
        assert_eq!(revoke(&store, &agent.id, None).unwrap(), 2);
        assert!(store.authenticate_remote_session(&first).is_err());
        assert!(store.authenticate_remote_session(&second).is_err());
        assert!(!config.join(TOKEN_FILE).exists());
        assert!(session_token(&config).is_err());

        // A relaunch begins a new session and supersedes the old one.
        begin_session(&store, &agent).unwrap();
        let third = session_token(&config).unwrap();
        assert_ne!(
            store
                .authenticate_remote_session(&third)
                .unwrap()
                .session_id,
            grant.session_id
        );
    }
}

#[cfg(test)]
mod webhook_secret_tests {
    use super::*;
    #[test]
    fn config_overrides_environment_and_empty_disables() {
        let settings = Settings::parse(
            "[server]
webhook_secret = 'configured-secret'",
            Path::new("config"),
        )
        .unwrap();
        assert_eq!(
            settings
                .resolve_webhook_secret(Some("environment".into()))
                .as_deref(),
            Some("configured-secret")
        );
        assert!(!format!("{settings:?}").contains("configured-secret"));
        let disabled = Settings::parse(
            "[server]
webhook_secret = ''",
            Path::new("config"),
        )
        .unwrap();
        assert_eq!(
            disabled.resolve_webhook_secret(Some("environment".into())),
            None
        );
        assert_eq!(
            Settings::default()
                .resolve_webhook_secret(Some("environment".into()))
                .as_deref(),
            Some("environment")
        );
        assert_eq!(Settings::default().resolve_webhook_secret(None), None);
    }
}
