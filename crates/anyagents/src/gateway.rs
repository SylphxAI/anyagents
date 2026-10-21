//! The protocol gateway as the server runs it: one loopback port for the life
//! of the server, one route and one token per agent that has a provider key.
//!
//! An agent's provider key is stored once, in a 0600 file under the agent's own
//! directory (`provider.json`), and reaches only the gateway. The harness
//! process is handed the gateway's origin and that agent's own token, so the
//! key never appears in a harness configuration, an environment or a log. A
//! vendor subscription never comes near this: the gateway is for API keys only.
//!
//! The route table is in memory. A restart rebuilds it from the stored images,
//! before the supervisor starts any agent, and a token is kept on disk so an
//! agent that resumes keeps the same one.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock, PoisonError};

use anyagents_core::{Agent, AgentId, Error, Store};
use anyagents_gateway::{MemoryRoutes, Protocol, ProviderRoute};

/// The running gateway: where it listens, and what it resolves.
pub struct Gateway {
    pub port: u16,
    /// What each agent's token resolves to. The router reads it on every request.
    pub routes: MemoryRoutes,
}

static GATEWAY: OnceLock<Gateway> = OnceLock::new();
static START: Mutex<()> = Mutex::new(());

/// The running gateway, if `start` was called.
pub fn running() -> Option<&'static Gateway> {
    GATEWAY.get()
}

/// Start the gateway once, as part of the server starting. It is bound to the
/// server's lifetime: a server that cannot serve harnesses reports why.
pub fn start(data: &Path) -> Result<(), Error> {
    let guard = START.lock().unwrap_or_else(PoisonError::into_inner);
    if GATEWAY.get().is_some() {
        return Ok(());
    }
    let (port, listener) = crate::server::listen_on_loopback()?;
    let routes = MemoryRoutes::new();
    for agent in Store::open(data.join("state.db"))?.list_agents()? {
        if let Some(image) = ProviderImage::load(data, &agent.id)? {
            routes.insert(token(data, &agent.id)?, image.route(&agent.name));
        }
    }
    let usage = anyagents_gateway::MemoryUsage::new();
    crate::server::serve_gateway(listener, routes.clone(), usage)?;
    let _ = GATEWAY.set(Gateway { port, routes });
    // The port is this process's, so every agent pointed at the gateway is
    // repointed now: a recorded origin from an earlier start no longer answers.
    let origin = format!("http://127.0.0.1:{port}");
    let store = Store::open(data.join("state.db"))?;
    for agent in store.list_agents()? {
        if ProviderImage::load(data, &agent.id)?.is_none() {
            continue;
        }
        let env = env_for(data, &agent.id, &origin);
        if !env.is_empty() {
            store.set_agent_launch_env(&agent.id, &env)?;
        }
    }
    drop(guard);
    Ok(())
}

/// The origin a harness sends its requests to.
pub fn origin() -> Option<String> {
    running().map(|gateway| format!("http://127.0.0.1:{}", gateway.port))
}

/// Point one agent at the gateway. Called when the agent's key is stored, with
/// the server running or not: the next start rebuilds it from the same file.
pub fn add_route(data: &Path, agent: &Agent) -> Result<Option<String>, Error> {
    let Some(image) = ProviderImage::load(data, &agent.id)? else {
        return Ok(None);
    };
    let token = token(data, &agent.id)?;
    // The agent is told the origin and its token from the next start on. A
    // client process reads the origin the running server published; a server
    // process knows it directly.
    let origin = origin().or_else(|| published_origin(data));
    let Some(origin) = origin else {
        return Ok(Some(token));
    };
    store_launch_env(data, &agent.id, &origin)?;
    let Some(gateway) = running() else {
        return Ok(Some(token));
    };
    gateway
        .routes
        .insert(token.clone(), image.route(&agent.name));
    Ok(Some(token))
}

/// Store the provider key for one agent and point its route at the gateway.
/// The file is 0600 and nothing prints the key. Called by `anyagents model`.
pub fn store_key(
    data: &Path,
    agent: &Agent,
    base_url: &str,
    model: &str,
    api_key: &str,
) -> Result<(), Error> {
    ProviderImage::store(data, agent, base_url, model, api_key)?;
    // With the gateway running the route answers at once; either way the agent
    // is told the gateway origin and its own token from its next start.
    add_route(data, agent)?;
    Ok(())
}

/// The origin the running server published on its socket, for a client process
/// that has no gateway of its own.
fn published_origin(data: &Path) -> Option<String> {
    let data = std::path::absolute(data).unwrap_or_else(|_| data.to_path_buf());
    crate::client::call::<crate::api::AgentsResponse>(
        &data,
        "GET",
        "/v1/agents",
        None,
        &crate::client::authority(),
    )
    .ok()
    .flatten()
    .and_then(|listed| listed.gateway)
}

/// Record an agent's launch environment from an origin a caller already has.
fn store_launch_env(data: &Path, id: &AgentId, origin: &str) -> Result<(), Error> {
    let env = env_for(data, id, origin);
    if env.is_empty() {
        return Ok(());
    }
    Store::open(data.join("state.db"))?.set_agent_launch_env(id, &env)?;
    Ok(())
}

/// The environment that points an agent's harness at the gateway: the origin
/// for the protocol that harness speaks, and that agent's own token. It is the
/// key the harness would otherwise have asked the user for, so nothing else is
/// inherited from the gateway's side.
pub fn agent_env(data: &Path, id: &AgentId) -> Vec<(String, String)> {
    // Before the gateway listens the agent keeps the key's vendor sign-in, and
    // picks the gateway up at the next start.
    match origin() {
        Some(origin) => env_for(data, id, &origin),
        None => Vec::new(),
    }
}

/// `agent_env` against one known origin, so a client that read the origin from
/// the running server records the same environment the server would.
fn env_for(data: &Path, id: &AgentId, origin: &str) -> Vec<(String, String)> {
    let Ok(Some(image)) = ProviderImage::load(data, id) else {
        return Vec::new();
    };
    let Ok(token) = token(data, id) else {
        return Vec::new();
    };
    let origin = origin.to_string();
    let mut env = match protocol_of(&image.harness) {
        Protocol::AnthropicMessages => vec![
            ("ANTHROPIC_BASE_URL".to_string(), origin),
            ("ANTHROPIC_AUTH_TOKEN".to_string(), token),
        ],
        Protocol::OpenAiChat | Protocol::OpenAiResponses => vec![
            ("OPENAI_BASE_URL".to_string(), origin),
            ("OPENAI_API_KEY".to_string(), token),
        ],
    };
    if !image.model.is_empty() && image.harness == "claude-code" {
        env.push(("ANTHROPIC_MODEL".to_string(), image.model.clone()));
    }
    env
}

/// The gateway token of one agent. It is generated once, kept 0600 beside the
/// agent's other state, and never printed.
pub fn token(data: &Path, id: &AgentId) -> Result<String, Error> {
    let file = data
        .join("agents")
        .join(id.to_string())
        .join("gateway-token.txt");
    match std::fs::read_to_string(&file) {
        Ok(text) if !text.trim().is_empty() => Ok(text.trim().to_string()),
        _ => {
            let token = random_token()?;
            write_private(&file, &token)?;
            Ok(token)
        }
    }
}

fn random_token() -> Result<String, Error> {
    use std::io::Read;
    let mut bytes = [0u8; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn write_private(file: &Path, text: &str) -> Result<(), Error> {
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    if let Some(parent) = file.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut handle = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(file)?;
    handle.write_all(text.as_bytes())?;
    std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

/// Where an agent's provider key is kept.
fn image_file(data: &Path, id: &AgentId) -> PathBuf {
    data.join("agents")
        .join(id.to_string())
        .join("provider.json")
}

/// The provider an agent's model runs on, with the key the user stored. The key
/// is read here and handed to the gateway; nothing prints it.
pub struct ProviderImage {
    pub harness: String,
    pub model: String,
    pub base_url: String,
    api_key: String,
}

impl ProviderImage {
    /// Store the key the user typed. `anyagents model <agent> --base-url ...`
    /// is the only caller; the file is 0600 and the command prints no key.
    pub fn store(
        data: &Path,
        agent: &Agent,
        base_url: &str,
        model: &str,
        api_key: &str,
    ) -> Result<Self, Error> {
        let file = image_file(data, &agent.id);
        let value = serde_json::json!({
            "harness": agent.harness,
            "model": model,
            "baseUrl": base_url,
            "apiKey": api_key,
        });
        let text =
            serde_json::to_string_pretty(&value).map_err(|err| Error::Message(err.to_string()))?;
        write_private(&file, &text)?;
        Ok(Self {
            harness: agent.harness.clone(),
            model: model.to_string(),
            base_url: base_url.to_string(),
            api_key: api_key.to_string(),
        })
    }

    /// The image stored for this agent, or `None` when it runs on a subscription.
    pub fn load(data: &Path, id: &AgentId) -> Result<Option<Self>, Error> {
        let raw = match std::fs::read(image_file(data, id)) {
            Ok(raw) => raw,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let value: serde_json::Value =
            serde_json::from_slice(&raw).map_err(|err| Error::Message(err.to_string()))?;
        let text = |key: &str| {
            value
                .get(key)
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string()
        };
        Ok(Some(Self {
            harness: text("harness"),
            model: text("model"),
            base_url: text("baseUrl"),
            api_key: text("apiKey"),
        }))
    }

    /// The route an agent's token resolves to. The inbound protocol is the one
    /// the harness speaks; the provider is the image's own origin.
    pub fn route(&self, agent: &str) -> ProviderRoute {
        ProviderRoute {
            agent: agent.to_string(),
            provider: provider_of(&self.base_url).to_string(),
            protocol: protocol_of(&self.harness),
            base_url: self.base_url.trim_end_matches('/').to_string(),
            model: self.model.clone(),
            api_key: self.api_key.clone(),
        }
    }
}

/// Which harness API a harness calls the gateway with.
pub fn protocol_of(harness: &str) -> Protocol {
    match harness {
        "codex" => Protocol::OpenAiResponses,
        "opencode" => Protocol::OpenAiChat,
        _ => Protocol::AnthropicMessages,
    }
}

/// The provider name recorded on usage events: the first host label that is not
/// `api`, so `https://api.deepseek.com` is `deepseek`.
fn provider_of(base_url: &str) -> &str {
    let host = base_url
        .split("://")
        .nth(1)
        .unwrap_or(base_url)
        .split(['/', ':'])
        .next()
        .unwrap_or(base_url);
    let labels: Vec<&str> = host.split('.').filter(|l| !l.is_empty()).collect();
    match labels.as_slice() {
        [first, _, ..] if *first != "api" => first,
        [_, second, ..] => second,
        [only] => only,
        [] => base_url,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_provider_name_is_the_first_label_after_api() {
        assert_eq!(provider_of("https://api.deepseek.com"), "deepseek");
        assert_eq!(provider_of("https://api.openai.com/v1"), "openai");
        assert_eq!(provider_of("https://api.anthropic.com"), "anthropic");
        assert_eq!(provider_of("http://127.0.0.1:8080"), "127");
        assert_eq!(provider_of("deepseek"), "deepseek");
    }

    #[test]
    fn each_harness_calls_the_api_it_speaks() {
        assert_eq!(protocol_of("claude-code"), Protocol::AnthropicMessages);
        assert_eq!(protocol_of("codex"), Protocol::OpenAiResponses);
        assert_eq!(protocol_of("opencode"), Protocol::OpenAiChat);
    }
}
