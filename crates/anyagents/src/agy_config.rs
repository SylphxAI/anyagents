//! Where the Antigravity steering hook lives in an agent's own config directory.
//!
//! The adapter registers the hook itself (`anyagents_harness_agy::write_settings`),
//! because it is the code that serves that hook's queue; this module owns only
//! the path, so the server and the adapter agree on one file name.

use std::path::Path;

/// The steering hook program's file name in the agent's config directory.
pub const HOOK_FILE: &str = "agy-steer-hook";

/// The steering hook at the path the adapter registers.
pub fn hook_path(dir: &Path) -> std::path::PathBuf {
    dir.join(HOOK_FILE)
}
