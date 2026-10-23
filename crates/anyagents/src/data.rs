//! Where this deployment keeps its data.
//!
//! The server exports `ANYAGENTS_DATA` to every harness it starts, so an agent's
//! tools find the one data directory their deployment serves and the config
//! directory that holds their capability. No path is guessed from a working
//! directory, which an agent can change.

use std::path::PathBuf;

/// The data directory this process belongs to, when its deployment named one
/// via `ANYAGENTS_DATA` or a `<exe_dir>/anyagents-data.path` pointer file.
pub fn data_dir() -> Option<PathBuf> {
    if let Some(value) = std::env::var_os("ANYAGENTS_DATA")
        && !value.is_empty()
    {
        return Some(PathBuf::from(value));
    }
    if !crate::sandbox::is_isolated_test_env()
        && let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent()
    {
        let pointer = dir.join("anyagents-data.path");
        if let Ok(raw) = std::fs::read_to_string(pointer) {
            let trimmed = raw.trim();
            if !trimmed.is_empty() {
                let candidate = PathBuf::from(trimmed);
                if candidate.join("anyagents.sock").exists() || candidate.join("state.db").is_file()
                {
                    return Some(candidate);
                }
            }
        }
    }
    None
}
