//! Fail-closed sandbox environment filtering and `ANYAGENTS_SANDBOX_ENV_<KEY>`
//! deployment-wide default forwarding.

use std::collections::BTreeMap;
use std::path::Path;

use serde_json::{Value, json};

/// Optional deployment-wide resource tier overrides for repo-backed coding sandboxes.
pub(crate) const ENV_SANDBOX_CODE_SHAPE: &str = "ANYAGENTS_SANDBOX_CODE_SHAPE";
pub(crate) const ENV_SANDBOX_CODE_VOLUME_SIZE_GIB: &str = "ANYAGENTS_SANDBOX_CODE_VOLUME_SIZE_GIB";

/// Optional deployment-wide compute tier for sandboxes with browser profile grants.
const ENV_SANDBOX_BROWSER_SHAPE: &str = "ANYAGENTS_SANDBOX_BROWSER_SHAPE";

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
            | "ANYAGENTS_TIMER_TOKEN"
            | "ANYAGENTS_TIMER_AUD"
            | "ANYAGENTS_TIMER_ISS"
            | "ANYAGENTS_TIMER_SUB"
            | "ANYAGENTS_TIMER_JWKS_URL"
            | "ANYAGENTS_TIMER_SIGNATURE_HEADER"
            | "ANYAGENTS_TIMER_SCHEDULER_URL"
            | "ANYAGENTS_TIMER_SCHEDULER_TOKEN"
            | "ANYAGENTS_TIMER_SCHEDULER_MODE"
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
    ) || anyagents_harness_spiron::inference_key::is_env_key(&upper)
        || upper.starts_with("ANYAGENTS_OPERATOR_")
        || upper.starts_with("ANYAGENTS_ADMIN_")
        || upper.starts_with("ANYAGENTS_OIDC_")
        || upper.starts_with("ANYAGENTS_CONTROL_")
        || upper.starts_with("ANYAGENTS_AUTH_")
        || upper == "ANYAGENTS_BEARER_TOKEN"
        || upper == "XDG_STATE_HOME"
        || upper.starts_with("ANYAGENTS_TICK_")
        || upper.starts_with("ANYAGENTS_TIMER_")
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

/// Resolve `(shape, volume_size_gib)` for a remote sandbox lease.
///
/// Browser agents prefer the browser tier, then the code tier, over a shape
/// recorded by an earlier lease. Other agents keep the binding-first policy,
/// with the code tier as the fallback for repo agents. Browser grants do not
/// affect volume sizing.
pub(crate) fn resolve_sandbox_shape_and_volume_with_env<I>(
    has_repo: bool,
    has_browser_profiles: bool,
    binding_shape: Option<&str>,
    explicit_volume_gib: Option<u32>,
    default_shape: Option<String>,
    default_volume_gib: Option<u32>,
    process_env: I,
) -> (Option<String>, Option<u32>)
where
    I: IntoIterator<Item = (String, String)>,
{
    let env_map: BTreeMap<String, String> = process_env.into_iter().collect();
    let env_nonempty = |k: &str| {
        env_map
            .get(k)
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    };
    let shape = if has_browser_profiles {
        // The binding records a previously requested shape, possibly from
        // before the agent had browser grants. Recompute the browser tier.
        env_nonempty(ENV_SANDBOX_BROWSER_SHAPE).or_else(|| env_nonempty(ENV_SANDBOX_CODE_SHAPE))
    } else {
        binding_shape
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or_else(|| {
                has_repo
                    .then(|| env_nonempty(ENV_SANDBOX_CODE_SHAPE))
                    .flatten()
            })
    }
    .or(default_shape);
    let volume_gib = explicit_volume_gib
        .filter(|&v| v > 0)
        .or_else(|| {
            has_repo
                .then(|| {
                    env_nonempty(ENV_SANDBOX_CODE_VOLUME_SIZE_GIB)
                        .and_then(|v| v.parse::<u32>().ok())
                        .filter(|&v| v > 0)
                })
                .flatten()
        })
        .or(default_volume_gib);
    (shape, volume_gib)
}

pub(crate) fn resolve_sandbox_shape_and_volume_for_agent(
    store: &anyagents_core::Store,
    agent_id: &anyagents_core::AgentId,
    has_local_repo: bool,
    default_shape: Option<String>,
    default_volume_gib: Option<u32>,
    process_env: impl IntoIterator<Item = (String, String)>,
) -> Result<(Option<String>, Option<u32>), anyagents_core::Error> {
    let has_browser_profiles = !store.list_agent_browser_profiles(agent_id)?.is_empty();
    let existing = store.sandbox_binding(agent_id).ok().flatten();
    let spec = existing
        .as_ref()
        .and_then(|b| serde_json::from_str::<Value>(&b.spec_json).ok());
    let binding_shape = spec.as_ref().and_then(|v| v.get("shape")?.as_str());
    let explicit_vol = existing
        .as_ref()
        .and_then(|b| b.volume_size_gib)
        .and_then(|v| u32::try_from(v).ok());
    let has_repo = has_local_repo
        || (store.is_postgres()
            && store
                .blob_store()
                .get_agent_file(agent_id, "config", "repo-spec.json")
                .ok()
                .flatten()
                .is_some());
    Ok(resolve_sandbox_shape_and_volume_with_env(
        has_repo,
        has_browser_profiles,
        binding_shape,
        explicit_vol,
        default_shape,
        default_volume_gib,
        process_env,
    ))
}

/// Merge `mcpServers.anyagents` into existing `.claude.json` (or `.claude/.claude.json`)
/// while preserving Claude Code auth/onboarding fields (`oauthAccount`, `hasCompletedOnboarding`).
pub(crate) fn merge_sandbox_claude_json(
    config_dir: &Path,
    guest_bin: &str,
    proxy_args: &[String],
) -> Result<Vec<u8>, String> {
    let mut root = [".claude.json", ".claude/.claude.json"]
        .into_iter()
        .find_map(|rel| {
            std::fs::read_to_string(config_dir.join(rel))
                .ok()
                .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
                .filter(Value::is_object)
        })
        .unwrap_or_else(|| json!({}));
    if let Some(obj) = root.as_object_mut() {
        obj.entry("hasCompletedOnboarding")
            .or_insert(Value::Bool(true));
        let servers = obj.entry("mcpServers").or_insert_with(|| json!({}));
        if !servers.is_object() {
            *servers = json!({});
        }
        if let Some(map) = servers.as_object_mut() {
            map.insert(
                "anyagents".into(),
                json!({ "command": guest_bin, "args": proxy_args }),
            );
        }
    }
    let mut bytes = serde_json::to_vec_pretty(&root).map_err(|e| e.to_string())?;
    bytes.push(b'\n');
    Ok(bytes)
}

/// Preserve existing Codex/Spiron `config.toml` settings (`model`, `model_provider`,
/// `approval_policy`, `sandbox_mode`, `[projects."/"]`) while updating `[mcp_servers.anyagents]`.
pub(crate) fn merge_sandbox_toml_config(
    config_dir: &Path,
    harness: &str,
    guest_bin: &str,
    mcp_url: &str,
    token_path: &str,
) -> String {
    let existing = std::fs::read_to_string(config_dir.join("config.toml")).unwrap_or_default();
    let cmd = serde_json::to_string(guest_bin).unwrap_or_else(|_| "\"anyagents-guest\"".into());
    let url = serde_json::to_string(mcp_url).unwrap_or_default();
    let tok = serde_json::to_string(token_path).unwrap_or_default();
    let mcp_block = format!(
        "[mcp_servers.anyagents]\ncommand = {cmd}\nargs = [\"mcp-proxy\", \"--url\", {url}, \"--token-file\", {tok}]\n"
    );
    let mut out_lines = Vec::new();
    let mut skipping = false;
    for line in existing.lines() {
        let t = line.trim();
        if t.starts_with('[') && t.ends_with(']') {
            skipping = t == "[mcp_servers.anyagents]";
        }
        if !skipping {
            out_lines.push(line);
        }
    }
    let mut text = out_lines.join("\n");
    if harness != "codex" && !text.contains("always_approve") {
        text = if text.is_empty() {
            "always_approve = true\n".to_string()
        } else {
            format!("always_approve = true\n{text}\n")
        };
    }
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
    if !text.is_empty() {
        text.push('\n');
    }
    text.push_str(&mcp_block);
    text
}

/// Strip MCP/webhook endpoint suffixes so `ANYAGENTS_API_URL` always points at
/// the coordinator's HTTP origin root.
pub(crate) fn normalize_api_base_url(raw: &str) -> String {
    let mut url = raw.trim().trim_end_matches('/');
    for suffix in ["/v1/webhooks/mcp", "/v1/mcp", "/mcp", "/v1/webhooks"] {
        if let Some(stripped) = url.strip_suffix(suffix) {
            url = stripped.trim_end_matches('/');
            break;
        }
    }
    url.to_string()
}

/// Populate unified sandbox data/media/screenshot/API environment variables.
pub(crate) fn enrich_sandbox_guest_env(
    env_map: &mut BTreeMap<String, String>,
    state_root: &str,
    agent_id_str: &str,
    mcp_url: &str,
) -> String {
    let data_dir = sandbox_data_dir_from_state_root(state_root);
    env_map.insert("ANYAGENTS_AGENT_DATA_DIR".into(), data_dir.clone());
    env_map.insert("ANYAGENTS_SANDBOX_DATA_DIR".into(), data_dir.clone());
    env_map.insert("ANYAGENTS_MEDIA_DIR".into(), format!("{data_dir}/media"));
    env_map.insert(
        "ANYAGENTS_BROWSER_SCREENSHOT_DIR".into(),
        format!("{data_dir}/tmp/screenshots"),
    );
    let api_url = normalize_api_base_url(mcp_url);
    if !api_url.is_empty() {
        env_map.insert("ANYAGENTS_API_URL".into(), api_url);
    }
    if !agent_id_str.trim().is_empty() {
        env_map.insert("ANYAGENTS_AGENT_ID".into(), agent_id_str.to_string());
    }
    data_dir
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn append_sandbox_mcp_bundle(
    state_root: &str,
    config_dir: &Path,
    harness: &str,
    guest_bin: &str,
    mcp_url: &str,
    token_path_in_vm: &str,
    uploads: &mut Vec<(String, Vec<u8>)>,
    dirs_to_create: &mut std::collections::BTreeSet<String>,
) -> Result<(), String> {
    let proxy_args = vec![
        "mcp-proxy".to_string(),
        "--url".to_string(),
        mcp_url.to_string(),
        "--token-file".to_string(),
        token_path_in_vm.to_string(),
    ];
    let mut stdio_mcp_bytes = serde_json::to_vec_pretty(&json!({
        "mcpServers": {
            "anyagents": { "command": guest_bin, "args": proxy_args }
        }
    }))
    .map_err(|e| e.to_string())?;
    stdio_mcp_bytes.push(b'\n');
    for target in [".mcp.json", "mcp.json", "spiron-mcp.json"] {
        uploads.push((target.to_string(), stdio_mcp_bytes.clone()));
    }
    uploads.push((
        ".claude.json".to_string(),
        merge_sandbox_claude_json(config_dir, guest_bin, &proxy_args)?,
    ));
    dirs_to_create.insert(format!("{state_root}/.gemini/config"));
    uploads.push((
        ".gemini/config/mcp_config.json".to_string(),
        stdio_mcp_bytes.clone(),
    ));
    dirs_to_create.insert(format!("{state_root}/.omp/agent"));
    uploads.push((".omp/agent/mcp.json".to_string(), stdio_mcp_bytes));
    let toml_cfg =
        merge_sandbox_toml_config(config_dir, harness, guest_bin, mcp_url, token_path_in_vm);
    uploads.push(("config.toml".to_string(), toml_cfg.into_bytes()));
    Ok(())
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
                "ANYAGENTS_SANDBOX_ENV_ANYAGENTS_TIMER_TOKEN".into(),
                "timer-secret".into(),
            ),
            (
                "ANYAGENTS_SANDBOX_ENV_ANYAGENTS_TIMER_SCHEDULER_TOKEN".into(),
                "timer-sched-secret".into(),
            ),
            (
                "ANYAGENTS_SANDBOX_ENV_ANYAGENTS_TIMER_JWKS_URL".into(),
                "https://jwks.example.test/.well-known/jwks.json".into(),
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
            "/home/user/agent-data/media",
            "/home/user/agent-data/tmp/screenshots",
            "default-mysql-client",
            "redis-tools",
            "python3-jwt",
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

    #[test]
    fn repo_backed_sandbox_selects_code_tier_while_non_repo_stays_lean() {
        let env = vec![
            (ENV_SANDBOX_BROWSER_SHAPE.into(), "c8m16".into()),
            (ENV_SANDBOX_CODE_SHAPE.into(), "c4m8".into()),
            (ENV_SANDBOX_CODE_VOLUME_SIZE_GIB.into(), "40".into()),
        ];
        // Non-repo sandbox stays on lean default tier.
        assert_eq!(
            resolve_sandbox_shape_and_volume_with_env(
                false,
                false,
                None,
                None,
                Some("c1m2".into()),
                Some(10),
                env.clone(),
            ),
            (Some("c1m2".into()), Some(10))
        );
        // Repo-backed sandbox uses code tier when no explicit override is set.
        assert_eq!(
            resolve_sandbox_shape_and_volume_with_env(
                true,
                false,
                None,
                None,
                Some("c1m2".into()),
                Some(10),
                env.clone(),
            ),
            (Some("c4m8".into()), Some(40))
        );
        // Explicit per-agent override takes precedence over code tier.
        assert_eq!(
            resolve_sandbox_shape_and_volume_with_env(
                true,
                false,
                Some("c8m16"),
                Some(80),
                Some("c1m2".into()),
                Some(10),
                env,
            ),
            (Some("c8m16".into()), Some(80))
        );
        // Fallback to lean defaults when code tier env vars are unset.
        assert_eq!(
            resolve_sandbox_shape_and_volume_with_env(
                true,
                false,
                None,
                None,
                Some("c1m2".into()),
                Some(10),
                Vec::<(String, String)>::new(),
            ),
            (Some("c1m2".into()), Some(10))
        );
    }

    #[test]
    fn browser_grant_recomputes_shape_from_current_tiers_without_changing_volume() {
        use anyagents_core::{BrowserProfileActor, NewAgent, SandboxBinding, Store};

        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("browser-agent", "codex"))
            .unwrap();
        let mut binding = SandboxBinding::new(agent.id.clone(), "http");
        binding.spec_json = json!({"shape": "old-lean"}).to_string();
        binding.volume_size_gib = Some(24);
        store.sandbox_binding_store().upsert(&binding).unwrap();

        let resolve = |has_repo, browser_shape, code_shape| {
            resolve_sandbox_shape_and_volume_for_agent(
                &store,
                &agent.id,
                has_repo,
                Some("lean".into()),
                Some(10),
                [
                    (ENV_SANDBOX_BROWSER_SHAPE, browser_shape),
                    (ENV_SANDBOX_CODE_SHAPE, code_shape),
                    (ENV_SANDBOX_CODE_VOLUME_SIZE_GIB, Some("40")),
                ]
                .into_iter()
                .filter_map(|(key, value)| Some((key.into(), String::from(value?)))),
            )
            .unwrap()
        };
        assert_eq!(
            resolve(false, Some("browser"), Some("code")),
            (Some("old-lean".into()), Some(24))
        );
        store
            .set_agent_browser_profiles_as(
                &BrowserProfileActor::Operator,
                &agent.id,
                &["test-profile".into()],
            )
            .unwrap();

        for has_repo in [false, true] {
            assert_eq!(
                resolve(has_repo, Some(" browser "), Some("code")),
                (Some("browser".into()), Some(24))
            );
            assert_eq!(
                resolve(has_repo, None, Some("code")),
                (Some("code".into()), Some(24))
            );
            assert_eq!(
                resolve(has_repo, Some(" "), Some("code")),
                (Some("code".into()), Some(24))
            );
            assert_eq!(
                resolve(has_repo, None, None),
                (Some("lean".into()), Some(24))
            );
        }
    }

    #[test]
    fn sandbox_env_strips_control_plane_and_oidc_auth_vars() {
        for forbidden in [
            "ANYAGENTS_ADMIN_OIDC_ISSUER",
            "ANYAGENTS_ADMIN_OIDC_CLIENT_SECRET",
            "ANYAGENTS_ADMIN_SESSION_SECRET",
            "ANYAGENTS_ADMIN_EMAILS",
            "ANYAGENTS_OIDC_TOKEN",
            "ANYAGENTS_CONTROL_AUTH_FILE",
            "ANYAGENTS_CONTROL_URL",
            "ANYAGENTS_AUTH_TOKEN",
            "ANYAGENTS_BEARER_TOKEN",
            "XDG_STATE_HOME",
        ] {
            assert!(
                is_blocked_sandbox_env_key(forbidden),
                "sandbox must strip `{forbidden}` so child sandboxes inherit zero admin rights"
            );
        }
        let filtered = filter_sandbox_launch_env_with_defaults(
            &[
                (
                    "ANYAGENTS_CONTROL_AUTH_FILE".into(),
                    "/secret/auth.json".into(),
                ),
                ("ANYAGENTS_ADMIN_OIDC_CLIENT_SECRET".into(), "shh".into()),
                ("XDG_STATE_HOME".into(), "/home/op/.local/state".into()),
                ("SAFE_APP_ENV".into(), "ok".into()),
            ],
            vec![
                (
                    "ANYAGENTS_SANDBOX_ENV_ANYAGENTS_BEARER_TOKEN".into(),
                    "leaked".into(),
                ),
                ("ANYAGENTS_SANDBOX_ENV_GOOD_DEFAULT".into(), "1".into()),
            ],
        );
        assert_eq!(
            filtered,
            vec![
                ("GOOD_DEFAULT".into(), "1".into()),
                ("SAFE_APP_ENV".into(), "ok".into()),
            ]
        );
    }

    #[test]
    fn api_url_strips_webhook_and_mcp_suffixes_to_origin_root() {
        for raw in [
            "https://coord.example/v1/webhooks/mcp",
            "https://coord.example/v1/webhooks/mcp/",
            "https://coord.example/v1/webhooks",
            "https://coord.example/v1/mcp",
            "https://coord.example/mcp",
            "https://coord.example/",
        ] {
            assert_eq!(
                normalize_api_base_url(raw),
                "https://coord.example",
                "{raw}"
            );
            let mut env = BTreeMap::new();
            enrich_sandbox_guest_env(&mut env, "/home/user/agent-data/state", "agent_01", raw);
            assert_eq!(
                env.get("ANYAGENTS_API_URL").map(String::as_str),
                Some("https://coord.example"),
                "{raw}"
            );
        }
    }
}
