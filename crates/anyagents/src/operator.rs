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

/// Verify through held daemon authority, or ask the primary daemon when this
/// process owns only a reopened store (for example a separate launch worker).
/// A file's presence is never evidence that it authenticates the intended ID.
pub(crate) fn verify_capability(
    store: &Store,
    agent_id: &anyagents_core::AgentId,
    token: &str,
) -> Result<(), Error> {
    match store.authenticate_capability(token) {
        Ok(id) if &id == agent_id => return Ok(()),
        Ok(_) => {
            return Err(Error::Message(
                "mailbox capability belongs to another agent".into(),
            ));
        }
        Err(local_error) => {
            use std::io::{Read, Write};
            // Raw capability values enter an HTTP header only after validation.
            if token.is_empty() || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(local_error);
            }
            let mut stream = match std::os::unix::net::UnixStream::connect(
                store.data_dir().join("anyagents.sock"),
            ) {
                Ok(stream) => stream,
                Err(_) => return Err(local_error),
            };
            stream.set_read_timeout(Some(std::time::Duration::from_secs(2)))?;
            stream.set_write_timeout(Some(std::time::Duration::from_secs(2)))?;
            write!(
                stream,
                "GET /v1/whoami HTTP/1.1\r\nHost: local\r\n{AGENT_HEADER}: {token}\r\nConnection: close\r\n\r\n"
            )?;
            let mut response = String::new();
            stream.take(65_537).read_to_string(&mut response)?;
            if response.len() > 65_536 || !response.starts_with("HTTP/1.1 200 ") {
                return Err(Error::Message(
                    "primary daemon refused mailbox capability verification".into(),
                ));
            }
            let body = response
                .split_once("\r\n\r\n")
                .map(|(_, body)| body)
                .ok_or_else(|| Error::Message("invalid capability verification response".into()))?;
            let value: serde_json::Value = serde_json::from_str(body)
                .map_err(|_| Error::Message("invalid capability verification response".into()))?;
            if value.get("agent_id").and_then(|value| value.as_str()) != Some(agent_id.as_str()) {
                return Err(Error::Message(
                    "mailbox capability belongs to another agent".into(),
                ));
            }
        }
    }
    Ok(())
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
    if let Err(err) = share_capability_if_needed(store, agent_id, &temp) {
        let _ = std::fs::remove_file(&temp);
        return Err(err);
    }
    std::fs::rename(&temp, &path)?;
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
    if let Ok(agent) = store.agent(agent_id)
        && let Some((_, seat_uid, gid, _)) = crate::create::resolved_agent_seat_info(store, &agent)
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_core::{NewAgent, Store};
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn reopened_store_verification_requires_exact_primary_daemon_identity() {
        use std::io::{Read, Write};
        use std::os::unix::net::UnixListener;
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        store
            .hold_capability_key("test-capability-key-0123456789abcdef")
            .unwrap();
        let agent = store
            .create_agent(NewAgent::new("worker", "codex"))
            .unwrap();
        let peer = store.create_agent(NewAgent::new("peer", "codex")).unwrap();
        let token = store.issue_capability(&agent.id).unwrap();
        assert!(verify_capability(&store, &agent.id, &token).is_ok());
        assert!(verify_capability(&store, &peer.id, &token).is_err());
        let reopened = Store::open(dir.path().join("state.db")).unwrap();
        assert!(verify_capability(&reopened, &agent.id, &token).is_err());
        let socket = dir.path().join("anyagents.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let id = agent.id.to_string();
        let peer_id = peer.id.to_string();
        let worker = std::thread::spawn(move || {
            for body in [
                serde_json::json!({"agent_id":id}).to_string(),
                serde_json::json!({"agent_id":peer_id}).to_string(),
                "{}".into(),
                "corrupt".into(),
            ] {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0; 4096];
                let size = stream.read(&mut request).unwrap();
                let request = std::str::from_utf8(&request[..size]).unwrap();
                assert!(request.starts_with("GET /v1/whoami "));
                assert!(!request.contains(HEADER));
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        assert!(verify_capability(&reopened, &agent.id, &token).is_ok());
        assert!(verify_capability(&reopened, &agent.id, &token).is_err());
        assert!(verify_capability(&reopened, &agent.id, &token).is_err());
        assert!(verify_capability(&reopened, &agent.id, &token).is_err());
        worker.join().unwrap();
        assert!(verify_capability(&reopened, &agent.id, "header\r\ninjection").is_err());
    }

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
