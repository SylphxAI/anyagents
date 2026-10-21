//! Where this deployment keeps its data.
//!
//! The server exports `ANYAGENTS_DATA` to every harness it starts, so an agent's
//! tools find the one data directory their deployment serves and the config
//! directory that holds their capability. No path is guessed from a working
//! directory, which an agent can change.

use std::path::PathBuf;

/// The data directory this process belongs to, when its deployment named one.
pub fn data_dir() -> Option<PathBuf> {
    match std::env::var_os("ANYAGENTS_DATA") {
        Some(value) if !value.is_empty() => Some(PathBuf::from(value)),
        _ => None,
    }
}
