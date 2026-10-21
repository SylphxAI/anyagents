//! The identity boundary a running server enforces, written as one file in the
//! data directory.
//!
//! A server started with an operator token *is* the identity boundary: it
//! serves agents that do not share its user, so its API socket accepts a
//! mailbox capability and nothing else, and start scripts must exit non-zero
//! rather than run an agent in the server's own files.
//!
//! The file records which data directory carries the boundary, so a client on
//! the machine can tell what it is talking to. The server reads it back after
//! writing it, and refuses to serve when it does not.

use std::path::Path;

use crate::error::Error;

/// The boundary contract file, inside the data directory.
pub const BOUNDARY_FILE: &str = "identity-boundary.json";

/// The contract, as it is stored.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Boundary {
    /// Always 1 for this contract.
    pub version: u32,
    /// The absolute data directory the server serves.
    pub data_dir: String,
    /// The one presentation an unauthenticated API call may use.
    pub agent_route: String,
    /// What an agent started on the machine needs before it can be isolated.
    pub agents: AgentRequirement,
    /// What the server itself needs.
    pub server: ServerRequirement,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AgentRequirement {
    /// The agent shell must be admitted by this OS mechanism before it starts.
    pub mechanism: String,
    /// Every path of the identity boundary, which no agent shell may read or
    /// write, as a glob.
    pub denied: Vec<String>,
    /// The agent's own writable workspace and config, under the work root.
    pub writable: Vec<String>,
    /// The shared binaries and guidance an agent reads and cannot change.
    pub readable: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ServerRequirement {
    /// The uid the server keeps for itself.
    pub user: String,
    /// The group an admitted agent shell shares with the server for its socket
    /// only; the socket is 0660 in it, and nothing else is.
    pub socket_group: String,
    /// The API socket, 0660 in `socket_group`.
    pub socket: String,
    /// The mode an admitted agent shell needs on that socket: its group may
    /// connect, no one else may.
    pub socket_mode: String,
    /// `--agent` must name this value, and nothing else.
    pub agent_presentation: String,
}

/// Write the contract for `data_dir`, then read it back. A file that does not
/// read back as written stops the server.
pub fn write(data_dir: &Path) -> Result<Boundary, Error> {
    let data = std::path::absolute(data_dir)?;
    let data_text = data.display().to_string();
    let server_user = whoami();
    let boundary = Boundary {
        version: 1,
        data_dir: data_text.clone(),
        agent_route: data_text.clone(),
        agents: AgentRequirement {
            mechanism: "userns".to_string(),
            denied: vec![
                format!("{data_text}/**"),
                "/proc/*/environ".to_string(),
                "/proc/*/cmdline".to_string(),
                "/proc/*/fd/**".to_string(),
            ],
            writable: vec!["/work/**".to_string()],
            readable: vec!["/usr/**".to_string(), "/lib/**".to_string(), "/lib64/**".to_string()],
        },
        server: ServerRequirement {
            user: server_user,
            socket_group: String::new(),
            socket_mode: "0660".to_string(),
            socket: format!("{data_text}/anyagents.sock"),
            agent_presentation: data_text,
        },
    };
    let text = serde_json::to_string_pretty(&boundary)
        .map_err(|err| Error::Message(err.to_string()))?;
    let path = data.join(BOUNDARY_FILE);
    std::fs::write(&path, text)?;
    let back = std::fs::read_to_string(&path)?;
    let read: Boundary = serde_json::from_str(&back).map_err(|err| Error::Message(err.to_string()))?;
    if read != boundary {
        return Err(Error::Message(format!(
            "{} does not read back as it was written",
            path.display()
        )));
    }
    Ok(boundary)
}

/// The server's own user name, for the record. Never a credential.
fn whoami() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("LOGNAME"))
        .unwrap_or_default()
}
