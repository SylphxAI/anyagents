//! Fail-closed sandbox environment filtering and `ANYAGENTS_SANDBOX_ENV_<KEY>`
//! deployment-wide default forwarding.

use std::collections::BTreeMap;

/// Coordinator environment prefix forwarded as deployment-wide defaults into
/// remote sandbox leases, `guest-config.json`, and detached turns.
pub(crate) const SANDBOX_ENV_PREFIX: &str = "ANYAGENTS_SANDBOX_ENV_";

/// Return `true` when `key` is malformed or matches the fail-closed blocklist
/// of coordinator secrets and protected sandbox runtime/gateway paths.
pub(crate) fn is_blocked_sandbox_env_key(key: &str) -> bool {
    let trimmed = key.trim();
    if trimmed.is_empty() || trimmed.contains('=') || trimmed.contains('\0') {
        return true;
    }
    let upper = trimmed.to_ascii_uppercase();
    matches!(
        upper.as_str(),
        "HOME"
            | "ANYAGENTS_OPERATOR_TOKEN"
            | "ANYAGENTS_OPERATOR_TOKEN_FILE"
            | "ANYAGENTS_DATABASE_URL"
            | "ANYAGENTS_DIRECT_DATABASE_URL"
            | "ANYAGENTS_MASTER_KEY"
            | "ANYAGENTS_CAPABILITY_KEY"
            | "ANYAGENTS_TICK_TOKEN"
            | "ANYAGENTS_TICK_AUD"
            | "ANYAGENTS_TICK_ISS"
            | "ANYAGENTS_TICK_JWKS_URL"
            | "ANYAGENTS_TICK_SIGNATURE_HEADER"
            | "ANYAGENTS_SANDBOX_TOKEN"
            | "ANYAGENTS_SYLPHX_TOKEN"
            | "ANYAGENTS_CLOUD_WEBHOOK_SECRET"
            | "ANYAGENTS_SPIRON_WEBHOOK_SECRET"
            | "ANYAGENTS_GUEST_STATE_DIR"
            | "ANYAGENTS_GUEST_WORKDIR"
            | "ANYAGENTS_MCP_URL"
            | "ANYAGENTS_MCP_TOKEN_FILE"
            | "ANYAGENTS_GATEWAY_URL"
            | "ANYAGENTS_GATEWAY_TOKEN"
    ) || upper.starts_with("ANYAGENTS_OPERATOR_")
        || upper.starts_with("ANYAGENTS_TICK_")
        || upper.starts_with("ANYAGENTS_S3_")
        || upper.starts_with(SANDBOX_ENV_PREFIX)
}

/// Pure implementation of [`filter_sandbox_launch_env`] taking an explicit
/// coordinator environment snapshot for deterministic testing.
///
/// 1. Extracts every `ANYAGENTS_SANDBOX_ENV_<KEY>=<VALUE>` pair from
///    `process_env`, strips the prefix to `<KEY>=<VALUE>`, and filters it
///    through [`is_blocked_sandbox_env_key`].
/// 2. Applies per-agent `raw` (`launch_env`) pairs through the exact same
///    blocklist, overriding any deployment-wide default with the same key.
pub(crate) fn filter_sandbox_launch_env_with_defaults<I>(
    raw: &[(String, String)],
    process_env: I,
) -> Vec<(String, String)>
where
    I: IntoIterator<Item = (String, String)>,
{
    let mut merged: BTreeMap<String, String> = BTreeMap::new();
    for (k, v) in process_env {
        let Some(stripped) = k.strip_prefix(SANDBOX_ENV_PREFIX) else {
            continue;
        };
        let key = stripped.trim();
        if is_blocked_sandbox_env_key(key) {
            continue;
        }
        merged.insert(key.to_string(), v);
    }
    for (k, v) in raw {
        let key = k.trim();
        if is_blocked_sandbox_env_key(key) {
            continue;
        }
        merged.insert(key.to_string(), v.clone());
    }
    merged.into_iter().collect()
}

/// Filter deployment defaults (`ANYAGENTS_SANDBOX_ENV_<KEY>`) and per-agent
/// `launch_env` pairs before forwarding them into a remote sandbox lease,
/// `guest-config.json`, or detached turn exec.
pub(crate) fn filter_sandbox_launch_env(raw: &[(String, String)]) -> Vec<(String, String)> {
    filter_sandbox_launch_env_with_defaults(raw, std::env::vars())
}

/// Derive the unified sandbox data root (`/home/user/agent-data`) from
/// `state_root` (`/home/user/agent-data/state` -> `/home/user/agent-data`).
pub(crate) fn sandbox_data_dir_from_state_root(state_root: &str) -> String {
    let trimmed = state_root.trim_end_matches('/');
    trimmed
        .strip_suffix("/state")
        .filter(|prefix| !prefix.is_empty())
        .unwrap_or(trimmed)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deployment_prefix_forwards_stripped_defaults_and_ignores_other_host_vars() {
        let process_env = vec![
            (
                "ANYAGENTS_SANDBOX_ENV_GIT_AUTHOR_NAME".into(),
                "Bot Author".into(),
            ),
            (
                "ANYAGENTS_SANDBOX_ENV_GIT_AUTHOR_EMAIL".into(),
                "bot@example.invalid".into(),
            ),
            (
                "ANYAGENTS_SANDBOX_ENV_GIT_COMMITTER_NAME".into(),
                "Bot Committer".into(),
            ),
            (
                "ANYAGENTS_SANDBOX_ENV_GIT_COMMITTER_EMAIL".into(),
                "bot@example.invalid".into(),
            ),
            ("ANYAGENTS_SANDBOX_ENV_".into(), "empty-suffix".into()),
            ("UNRELATED_HOST_VAR".into(), "must-not-leak".into()),
            ("ANYAGENTS_OPERATOR_TOKEN".into(), "host-op-secret".into()),
        ];
        let out = filter_sandbox_launch_env_with_defaults(&[], process_env);
        let map: BTreeMap<String, String> = out.into_iter().collect();
        assert_eq!(
            map.get("GIT_AUTHOR_NAME").map(String::as_str),
            Some("Bot Author")
        );
        assert_eq!(
            map.get("GIT_AUTHOR_EMAIL").map(String::as_str),
            Some("bot@example.invalid")
        );
        assert_eq!(
            map.get("GIT_COMMITTER_NAME").map(String::as_str),
            Some("Bot Committer")
        );
        assert_eq!(
            map.get("GIT_COMMITTER_EMAIL").map(String::as_str),
            Some("bot@example.invalid")
        );
        assert!(!map.contains_key(""));
        assert!(!map.contains_key("UNRELATED_HOST_VAR"));
        assert!(!map.contains_key("ANYAGENTS_OPERATOR_TOKEN"));
    }

    #[test]
    fn per_agent_launch_env_overrides_deployment_defaults() {
        let process_env = vec![
            (
                "ANYAGENTS_SANDBOX_ENV_GIT_AUTHOR_NAME".into(),
                "Default Author".into(),
            ),
            (
                "ANYAGENTS_SANDBOX_ENV_GIT_AUTHOR_EMAIL".into(),
                "default@example.invalid".into(),
            ),
        ];
        let raw = vec![
            ("GIT_AUTHOR_NAME".into(), "Agent Specific".into()),
            ("CUSTOM_FLAG".into(), "1".into()),
        ];
        let map: BTreeMap<String, String> =
            filter_sandbox_launch_env_with_defaults(&raw, process_env)
                .into_iter()
                .collect();
        assert_eq!(
            map.get("GIT_AUTHOR_NAME").map(String::as_str),
            Some("Agent Specific")
        );
        assert_eq!(
            map.get("GIT_AUTHOR_EMAIL").map(String::as_str),
            Some("default@example.invalid")
        );
        assert_eq!(map.get("CUSTOM_FLAG").map(String::as_str), Some("1"));
    }

    #[test]
    fn deployment_defaults_enforce_exact_same_fail_closed_secret_blocklist() {
        let process_env = vec![
            ("ANYAGENTS_SANDBOX_ENV_HOME".into(), "/evil".into()),
            (
                "ANYAGENTS_SANDBOX_ENV_ANYAGENTS_OPERATOR_TOKEN".into(),
                "op-secret".into(),
            ),
            (
                "ANYAGENTS_SANDBOX_ENV_ANYAGENTS_OPERATOR_CUSTOM".into(),
                "op-custom".into(),
            ),
            (
                "ANYAGENTS_SANDBOX_ENV_ANYAGENTS_DATABASE_URL".into(),
                "postgres://secret".into(),
            ),
            (
                "ANYAGENTS_SANDBOX_ENV_ANYAGENTS_MASTER_KEY".into(),
                "master-secret".into(),
            ),
            (
                "ANYAGENTS_SANDBOX_ENV_ANYAGENTS_CAPABILITY_KEY".into(),
                "cap-secret".into(),
            ),
            (
                "ANYAGENTS_SANDBOX_ENV_ANYAGENTS_TICK_TOKEN".into(),
                "tick-secret".into(),
            ),
            (
                "ANYAGENTS_SANDBOX_ENV_ANYAGENTS_SANDBOX_TOKEN".into(),
                "sbx-secret".into(),
            ),
            (
                "ANYAGENTS_SANDBOX_ENV_ANYAGENTS_MCP_URL".into(),
                "https://evil.invalid".into(),
            ),
            (
                "ANYAGENTS_SANDBOX_ENV_ANYAGENTS_GATEWAY_TOKEN".into(),
                "gw-secret".into(),
            ),
            (
                "ANYAGENTS_SANDBOX_ENV_ANYAGENTS_S3_SECRET_KEY".into(),
                "s3-secret".into(),
            ),
            (
                "ANYAGENTS_SANDBOX_ENV_ANYAGENTS_SANDBOX_ENV_NESTED".into(),
                "nested".into(),
            ),
            ("ANYAGENTS_SANDBOX_ENV_BAD=KEY".into(), "bad".into()),
            ("ANYAGENTS_SANDBOX_ENV_SAFE_KEY".into(), "allowed".into()),
        ];
        let map: BTreeMap<String, String> =
            filter_sandbox_launch_env_with_defaults(&[], process_env)
                .into_iter()
                .collect();
        assert_eq!(map.len(), 1, "only SAFE_KEY should pass: {map:?}");
        assert_eq!(map.get("SAFE_KEY").map(String::as_str), Some("allowed"));
    }

    #[test]
    fn sandbox_data_dir_strips_state_suffix() {
        assert_eq!(
            sandbox_data_dir_from_state_root("/home/user/agent-data/state"),
            "/home/user/agent-data"
        );
        assert_eq!(
            sandbox_data_dir_from_state_root("/home/user/agent-data/state/"),
            "/home/user/agent-data"
        );
        assert_eq!(
            sandbox_data_dir_from_state_root("/home/user/agent-state"),
            "/home/user/agent-state"
        );
    }

    #[test]
    fn dockerfile_sandbox_pins_opencode_v2_oss_dev_tools_and_user_ownership() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let dockerfile = std::fs::read_to_string(root.join("Dockerfile.sandbox")).unwrap();
        for required in [
            "fonts-noto-cjk",
            "fc-cache -f",
            "ANYAGENTS_AGENT_DATA_DIR=/home/user/agent-data",
            "/home/user/agent-data/tmp",
            "ARG OPENCODE_VERSION=",
            "@opencode/cli@${OPENCODE_VERSION}",
            "pnpm",
            "uv-${rust_arch}-unknown-linux-gnu.tar.gz",
            "go${GO_VERSION}.linux-${arch}.tar.gz",
            "helm-v${HELM_VERSION}-linux-${arch}.tar.gz",
            "ffmpeg",
            "python3-pil",
            "atlas-community-linux-${arch}-latest",
            "protobuf-compiler",
            "buf-Linux-${rust_arch}",
            "target add wasm32-unknown-unknown",
            "wasm-bindgen-${WASM_BINDGEN_VERSION}-${rust_arch}-unknown-linux-musl.tar.gz",
            "binaryen",
            "/home/user/.config/opencode",
            "chown -R \"${USER_UID}:${USER_GID}\" /home/user",
        ] {
            assert!(
                dockerfile.contains(required),
                "Dockerfile.sandbox missing required Layer 2 entry: {required}"
            );
        }
        assert!(
            !dockerfile.contains("npm_pkgs=\"$npm_pkgs opencode-ai\""),
            "Dockerfile.sandbox must not install legacy 1.x opencode-ai"
        );
        // Ensure every stage finishes with `/home/user` owned by `1000:1000` (`${USER_UID}:${USER_GID}`).
        assert!(
            dockerfile
                .matches("chown -R \"${USER_UID}:${USER_GID}\" /home/user")
                .count()
                >= 3,
            "all three sandbox stages must chown /home/user at stage end"
        );
    }
}
