//! OpenCode config for one agent. It lives in the agent's config directory,
//! which the server hands the child as `XDG_CONFIG_HOME`. It names the model and
//! registers the AnyAgents MCP server. Credentials stay in the user's OpenCode
//! state and are inherited; nothing here copies a provider or a secret.

use std::path::Path;

use anyagents_core::Error;

/// Write `opencode.json` in `dir`. A model already set by the caller is kept
/// and this file adds nothing to it.
pub fn write(
    dir: &Path,
    model: Option<&str>,
    command: &Path,
    args: &[String],
) -> Result<(), Error> {
    let mut root = serde_json::json!({});
    if let Some(model) = model.filter(|model| !model.is_empty()) {
        root["model"] = serde_json::Value::String(model.to_string());
    }
    root["mcp"] = serde_json::json!({
        "anyagents": {
            "type": "local",
            "command": command_args(command, args),
            "enabled": true,
        }
    });
    let bytes = serde_json::to_vec_pretty(&root).map_err(|err| Error::Message(err.to_string()))?;
    std::fs::write(dir.join("opencode.json"), bytes)?;
    Ok(())
}

fn command_args(command: &Path, args: &[String]) -> Vec<String> {
    let mut all = Vec::with_capacity(args.len() + 1);
    all.push(command.display().to_string());
    all.extend(args.iter().cloned());
    all
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_model_and_the_mcp_server_are_written() {
        let dir = tempfile::tempdir().unwrap();
        let args = vec!["--data".to_string(), "/tmp/d".to_string(), "mcp".to_string()];
        write(dir.path(), Some("sylphx/deepseek"), Path::new("/bin/anyagents"), &args).unwrap();
        let parsed: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("opencode.json")).unwrap())
                .unwrap();
        assert_eq!(parsed["model"], "sylphx/deepseek");
        let server = &parsed["mcp"]["anyagents"];
        assert_eq!(server["type"], "local");
        assert_eq!(server["command"][0], "/bin/anyagents");
        assert_eq!(server["command"][3], "mcp");
    }

    #[test]
    fn no_model_leaves_the_model_out() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), None, Path::new("/bin/anyagents"), &[]).unwrap();
        let parsed: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("opencode.json")).unwrap())
                .unwrap();
        assert!(parsed.get("model").is_none(), "{parsed}");
    }
}
