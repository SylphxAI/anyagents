//! The operator's authority, separate from every agent's.
//!
//! An operator token is a file in the company or user secret store; the CLI
//! reads it and presents it. The server keeps only a hash of it and accepts the
//! presented token from a process whose real user is the server's own user, so
//! an agent inside its own OS boundary can neither hold the token nor become
//! the server. This is the authority for administrative calls: creating agents,
//! signing a harness in, setting a channel secret.

use std::path::Path;

use anyagents_core::{Error, Store};

/// The header an operator call presents its token in.
pub const HEADER: &str = "x-anyagents-operator";
/// The header an agent call presents its mailbox capability in.
pub const AGENT_HEADER: &str = "x-anyagents-agent";

/// The file an agent's harness reads its capability from, in the agent's own
/// config directory.
pub const CAPABILITY_FILE: &str = "mcp-capability";

/// The operator token, when this deployment has one: the environment names
/// the file (`ANYAGENTS_OPERATOR_TOKEN_FILE`), else the data directory holds
/// the one file a start script wrote before the server started.
pub fn operator_token(data: &std::path::Path) -> Option<String> {
    let path = match std::env::var_os("ANYAGENTS_OPERATOR_TOKEN_FILE") {
        Some(path) if !path.is_empty() => std::path::PathBuf::from(path),
        _ => {
            let direct = data.join(TOKEN_FILE);
            if direct.is_file() {
                direct
            } else {
                data.join("secrets").join(TOKEN_FILE)
            }
        }
    };
    let text = std::fs::read_to_string(path).ok()?;
    let token = text.trim();
    (!token.is_empty()).then(|| token.to_string())
}

/// The operator's token file in a data directory.
pub const TOKEN_FILE: &str = "operator-token";

/// The agent's config directory: `agents/<id>/config`.
fn config_dir(data_dir: &Path, agent_id: &str) -> std::path::PathBuf {
    data_dir.join("agents").join(agent_id).join("config")
}

/// Ensure this agent has a valid capability file in its config directory that
/// authenticates against `store` right now. If the existing file already
/// authenticates as `agent_id`, keep it so running MCP tool processes do not
/// lose their session across a server restart; otherwise mint a fresh one.
pub fn ensure(store: &Store, agent_id: &anyagents_core::AgentId) -> Result<(), Error> {
    let dir = config_dir(store.data_dir(), agent_id.as_str());
    let path = dir.join(CAPABILITY_FILE);
    if let Ok(raw) = std::fs::read_to_string(&path) {
        let token = raw.trim();
        if !token.is_empty()
            && store
                .authenticate_capability(token)
                .is_ok_and(|id| &id == agent_id)
        {
            share_capability_if_needed(store, agent_id, &path)?;
            return Ok(());
        }
    }
    mint(store, agent_id)
}

/// Mint a capability for one agent and leave it where that agent's harness
/// reads it. The value is never printed, never an argument and never logged.
pub fn mint(store: &Store, agent_id: &anyagents_core::AgentId) -> Result<(), Error> {
    let token = store.issue_capability(agent_id)?;
    let dir = config_dir(store.data_dir(), agent_id.as_str());
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(CAPABILITY_FILE);
    let temp = dir.join(format!("{CAPABILITY_FILE}.new"));
    {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&temp)?;
        file.write_all(token.as_bytes())?;
        file.write_all(b"\n")?;
    }
    std::fs::rename(&temp, &path)?;
    share_capability_if_needed(store, agent_id, &path)?;
    Ok(())
}

fn share_capability_if_needed(
    store: &Store,
    agent_id: &anyagents_core::AgentId,
    path: &Path,
) -> Result<(), Error> {
    // The capability belongs to this agent's own seat group, 0640, and is read
    // back: no other seat can read it. A seat group that is the server's own
    // group adds nothing, so that file stays 0600.
    if let Ok(agent) = store.agent(agent_id) {
        let parent_name = agent
            .parent_id
            .as_ref()
            .and_then(|pid| store.agent(pid).ok())
            .map(|p| p.name);
        if let Some((_, seat_uid, gid, _)) =
            crate::sandbox::seat_for_agent_with_hint(&agent.name, parent_name.as_deref())
            && seat_uid != crate::sandbox::server_uid()
            && gid != crate::sandbox::server_gid()
        {
            crate::sandbox::share_capability_with_seat(path, gid).map_err(|err| {
                anyagents_core::Error::Message(format!(
                    "the capability for {} is not shared with its seat: {err}",
                    agent.name
                ))
            })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_core::{NewAgent, Store};
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn a_minted_capability_is_a_private_file_and_replaces_the_last_one() {
        let _env = crate::server::tests::env_lock();
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let agent = store.create_agent(NewAgent::new("ada", "codex")).unwrap();
        mint(&store, &agent.id).unwrap();
        let path = config_dir(dir.path(), agent.id.as_str()).join(CAPABILITY_FILE);
        let first = std::fs::read_to_string(&path).unwrap();
        let first = first.trim().to_string();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        assert_eq!(store.authenticate_capability(&first).unwrap(), agent.id);
        mint(&store, &agent.id).unwrap();
        let second = std::fs::read_to_string(&path).unwrap();
        let second = second.trim().to_string();
        assert_ne!(first, second);
        assert!(store.authenticate_capability(&first).is_err());
        assert_eq!(store.authenticate_capability(&second).unwrap(), agent.id);
    }
}
