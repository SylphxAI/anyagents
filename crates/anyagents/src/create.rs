//! `agents.create`. The agent row, its directories, and its MCP registration
//! are one step. The harness config is written only under the agent's own
//! config directory.

use std::path::{Path, PathBuf};

use anyagents_core::{Agent, AgentId, Error, NewAgent, Store, agent_config_dir, agent_work_dir};

use crate::{install, mcp_config, muse_config, opencode_config};

pub struct CreateRequest {
    pub name: String,
    pub harness: String,
    pub model: Option<String>,
    pub profile: Option<String>,
    pub workdir: Option<PathBuf>,
    /// The working directory an owner agent gets when none is named.
    pub workdir_default: Option<PathBuf>,
    pub instructions: Option<String>,
    pub parent_id: Option<AgentId>,
}

pub fn create(store: &Store, data: &Path, request: CreateRequest) -> Result<Agent, Error> {
    // A harness is installed only if an adapter can start it.
    install::adapter(&request.harness)?;
    let mut new = NewAgent::new(request.name, request.harness);
    new.model = request.model;
    new.instructions = request.instructions;
    new.parent_id = request.parent_id;
    let agent = store.create_agent(new)?;
    let data = std::path::absolute(data)?;
    // Nothing is installed until an agent needs it: this is that moment. A
    // failure is the reason the agent's first turn cannot start, reported here
    // and never retried in a loop.
    install::record(&data, &agent.harness).map_err(Error::Message)?;
    // The gateway origin and this agent's own token, kept so a restart starts
    // the same agent the same way.
    let mut pairs = crate::gateway::agent_env(&data, &agent.id);
    let parent_name = agent
        .parent_id
        .as_ref()
        .and_then(|pid| store.agent(pid).ok())
        .map(|p| p.name);
    if let Some(ref pname) = parent_name {
        pairs.push(("ANYAGENTS_PARENT_NAME".to_string(), pname.clone()));
    }
    store.set_agent_launch_env(&agent.id, &pairs)?;
    let managed = agent_work_dir(&data, &agent.id);
    std::fs::create_dir_all(&managed)?;
    let config = agent_config_dir(&data, &agent.id);
    std::fs::create_dir_all(&config)?;
    let _ = std::fs::create_dir_all(config.join("tmp"));
    if let Some(prof) = request
        .profile
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let normalized = if prof.contains('/') {
            prof.to_string()
        } else {
            format!(
                "{}/{prof}",
                upstream_provider_of(&agent.harness, agent.model.as_deref())
            )
        };
        write_profile_marker(&config, ".anyagents-profile", &format!("{normalized}\n"));
    }
    seed_harness_credentials(store, &data, &agent, &config);
    ensure_seat_environment(&config);
    let workdir_created = request.workdir.as_ref().is_some_and(|p| !p.exists());
    let cwd = match &request.workdir {
        Some(path) => {
            std::fs::create_dir_all(path)?;
            std::path::absolute(path)?
        }
        // Every owner agent hosted by AnyAgents works in one shared checkout,
        // so it loads the same company rules and skills as the owner sessions
        // the desk has always run. A caller can still name its own directory.
        None => match &request.workdir_default {
            Some(shared) => {
                std::fs::create_dir_all(shared)?;
                std::path::absolute(shared)?
            }
            None => std::path::absolute(&managed)?,
        },
    };
    let command = std::env::current_exe()?;
    // The tool server is pointed at the agent's own config directory, where its
    // capability is; a name on a command line is never an identity.
    let args = vec![
        "--data".to_string(),
        data.display().to_string(),
        "mcp".to_string(),
        "--config".to_string(),
        config.display().to_string(),
    ];
    if agent.harness == "codex" {
        mcp_config::write_codex_with_model(
            &config.join("config.toml"),
            agent.model.as_deref(),
            &command,
            &args,
        )?;
    } else if agent.harness == "opencode" {
        opencode_config::write(&config, agent.model.as_deref(), &command, &args)?;
    } else if agent.harness == "agy" {
        let _ = mcp_config::write_claude(
            &config.join(".gemini/config/mcp_config.json"),
            &command,
            &args,
        );
        crate::vendor::ensure_agy_steering_settings(&config, &cwd);
    } else if agent.harness == "muse" {
        muse_config::write(&config, agent.model.as_deref(), &command, &args)?;
        muse_config::point_vendor_home(&config)?;
    } else if agent.harness == "omp" {
        crate::grok::write_omp_mcp_config(&config, &command, &args)?;
    } else if agent.harness == "dsh" {
        crate::dsh::write_dsh_mcp_patch(&config, agent.model.as_deref(), &command, &args)?;
    } else if agent.harness == "grok" {
        crate::grok::write_grok_config(&config, agent.model.as_deref(), &command, &args)?;
    } else {
        let _ = mcp_config::sanitize_claude_settings(&config);
        mcp_config::write_claude(&config.join(".claude.json"), &command, &args)?;
        let _ = mcp_config::write_claude(&config.join("mcp.json"), &command, &args);
    }
    write_agent_instructions(&config, &agent, parent_name.as_deref());
    if let Some((_, _, gid, _)) =
        crate::sandbox::seat_for_agent_with_hint(&agent.name, parent_name.as_deref())
    {
        crate::sandbox::share_config_with_seat(&config, gid);
        if let Some((_, spec)) =
            crate::sandbox::seat_launch_spec_with_hint(&agent.name, parent_name.as_deref())
        {
            ensure_seat_op_config(&config, &spec);
        }
        if workdir_created {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                if let Ok(c_path) = std::ffi::CString::new(cwd.as_os_str().as_encoded_bytes()) {
                    unsafe {
                        let _ = libc::chown(c_path.as_ptr(), u32::MAX, gid);
                    }
                }
                let _ = std::fs::set_permissions(&cwd, std::fs::Permissions::from_mode(0o2775));
            }
        }
    }
    let _ = crate::operator::mint(store, &agent.id);
    store.set_agent_launch_env(&agent.id, &pairs)?;
    let running = store.set_agent_running(&agent.id, &cwd.display().to_string())?;
    Ok(running)
}

pub(crate) fn sync_all_agent_instructions(store: &Store, data: &Path) {
    let Ok(agents) = store.list_agents() else {
        return;
    };
    let by_id: std::collections::HashMap<String, String> = agents
        .iter()
        .map(|a| (a.id.to_string(), a.name.clone()))
        .collect();
    for agent in &agents {
        let cfg = agent_config_dir(data, &agent.id);
        if !cfg.is_dir() {
            continue;
        }
        let parent_name = agent
            .parent_id
            .as_ref()
            .and_then(|pid| by_id.get(pid.as_str()).cloned());
        write_agent_instructions(&cfg, agent, parent_name.as_deref());
        if let Some((_, _, gid, _)) =
            crate::sandbox::seat_for_agent_with_hint(&agent.name, parent_name.as_deref())
        {
            crate::sandbox::share_config_with_seat(&cfg, gid);
        }
    }
}

pub(crate) fn write_agent_instructions(config: &Path, agent: &Agent, parent_name: Option<&str>) {
    let mut doc = format!(
        "# AnyAgents Identity & Environment Awareness\n\n\
         ## 1. Your Identity (Who You Are)\n\n\
         - **Agent Name**: `{name}` (addressable as `agent:{name}`)\n\
         - **Agent ID**: `{id}`\n",
        name = agent.name,
        id = agent.id,
    );
    if let Some(parent) = parent_name {
        doc.push_str(&format!(
            "- **Role**: Sub-Agent spawned by `agent:{parent}` (addressable as `\"parent\"` or `\"agent:{parent}\"`)\n"
        ));
    } else {
        doc.push_str("- **Role**: Root Agent\n");
    }

    if let Some(custom) = agent
        .instructions
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        doc.push_str("\n## 2. Role, Responsibilities & Standing Description\n\n");
        doc.push_str(custom);
        doc.push('\n');
    }

    doc.push_str(
        "\n## 3. Runtime Awareness, Role Description, Sub-Agents, Browser & Communication Protocol\n\n\
         - **Cache-Stable Architecture (Static System Prompt + Tail Events & On-Demand Tools)**:\n\
           - This System Prompt is **strictly static** so your LLM prompt prefix KV cache is never invalidated when groups, channels, peers, or sub-agents change.\n\
           - Live routing context arrives **inline per message** at the tail of your context inside the `<message ...>` XML envelope (`id`, `from`, `sender_name`, `to`, `channel`, `chat`, `chat_title`, `chat_type`, `topic`, `event`, `in_reply_to`, `mentions`, `reply_to`, `answer_to`).\n\
           - Live topology (your sub-agents, groups, peers, and role descriptions) is queried **on demand** via MCP tools:\n\
             - **Role & Description (`describe`)**: `describe({ \"agent\": \"self\" })` (read your own role description), `describe({ \"agent\": \"self\", \"description\": \"...\" })` (update your own role/mission description whenever your focus changes; syncs your private system prompt and group roster), `describe({ \"agent\": \"all\" })` (read every peer's role & responsibilities).\n\
             - **Sub-Agents (`subagents`)**: `subagents({ \"action\": \"list\" })` (list your sub-agents & live status), `subagents({ \"action\": \"inspect\", \"agent\": \"<name-or-id>\", \"limit\": 10 })` (live status + recent turns in one call).\n\
             - **Groups (`groups`)**: `groups({ \"action\": \"list\" })` (returns `members` + full `roster` with each member's `description`), `groups({ \"action\": \"join\" | \"leave\", \"group\": \"<name>\" })`.\n\
             - **Peers (`agents`)**: `agents({})` (all agents, harnesses, models, `description`, and live health).\n\
         - **MCP Tools First (Agent Native Interface vs Operator CLI)**:\n\
           - Always call your native **AnyAgents MCP tools** (`send`, `subagents`, `spawn`, `browser`, `describe`, `groups`, `conversation`, `search`, `interact`, `agents`) directly (or via `await tools.anyagents.<tool>({...})` inside `execute` in OpenCode Code Mode) — they are schema-typed, immune to shell-quoting bugs, and automatically authenticated as your seat.\n\
           - The `anyagents` CLI binary is primarily for the human operator on the terminal.\n\
         - **Unified Cross-Harness Sub-Agents (`subagents` / `spawn` MCP Tools — Native Harness Sub-Agents Disabled by Default)**:\n\
           - Built-in harness sub-agent tools (`Task`/`Agent`/`task`/`invoke_subagent`) are **disabled by default** so sub-agents are never trapped inside a single vendor CLI.\n\
           - Always spawn and manage sub-agents via **AnyAgents `subagents` / `spawn` MCP tools**. When `harness`/`model`/`profile` are omitted, AnyAgents automatically uses the configured sub-agent default (`opencode` + `sylphx/openai/gpt-6.1-sol#xhigh` + `sylphx/default`, or pass `harness: \"inherit\"` to inherit your own harness/model/profile).\n\
           - Full lifecycle actions on `subagents`:\n\
             - **List**: `subagents({ \"action\": \"list\" })` (optional `state`: `\"running\"` | `\"paused\"` | `\"archived\"` | `\"all\"`, `parent`: `\"self\"` | `\"all\"`).\n\
             - **Inspect**: `subagents({ \"action\": \"inspect\", \"agent\": \"<name-or-id>\", \"limit\": 10 })`.\n\
             - **Spawn**: `subagents({ \"action\": \"spawn\", \"agent\": \"<name>\", \"workdir\": \"/scratch/wt/...\", \"description\": \"...\", \"prompt\": \"...\" })` (or `spawn({ ... })`).\n\
             - **Steer**: `subagents({ \"action\": \"steer\", \"agent\": \"<name-or-id>\", \"prompt\": \"...\" })`.\n\
             - **Control (`wake`, `pause`, `archive`, `delete`)**: `subagents({ \"action\": \"wake\" | \"pause\" | \"archive\" | \"delete\", \"agent\": \"<name-or-id>\" })`.\n\
         - **Native SOTA Anti-Detect Browser & Persistent Browser Profiles (`browser` MCP Tool)**:\n\
           - Every harness has native browser automation via the `browser` MCP tool, running real headed Chromium on a virtual X11 display (`xvfb-headed`) with deterministic per-profile anti-detect fingerprinting (WebGL vendor/renderer, Client Hints `Sec-CH-UA`, `navigator.webdriver = false`, WebRTC UDP leak protection, Canvas seed), hardware-trusted CDP input (`event.isTrusted === true`), and persistent `--user-data-dir` login profiles.\n\
           - **Zero-Knowledge Credential & 2FA OTP Fill**: Never read a plaintext password into your LLM context to type it into a browser. Pass a 1Password or credential reference directly in `text` or `secret_ref` (`browser({ \"action\": \"fill\", \"selector\": \"@e2\", \"secret_ref\": \"op://Vault/Item/password\" })` or `\"op-otp://Vault/Item\"` for live 6-digit 2FA TOTP, or `\"secret://<ref>\"`); the daemon resolves and types it via hardware CDP in memory without exposing the secret to LLM logs.\n\
           - Actions: `browser({ \"action\": \"open\", \"url\": \"https://...\", \"profile\": \"<optional-profile>\" })` (launches on demand and returns visible text + `@e1`, `@e2` interactive element refs), `browser({ \"action\": \"snapshot\" })`, `browser({ \"action\": \"click\", \"selector\": \"@e1\" })`, `browser({ \"action\": \"fill\", \"selector\": \"@e2\", \"text\": \"...\", \"submit\": true })`, `browser({ \"action\": \"press\", \"key\": \"Enter\" })`, `browser({ \"action\": \"eval\", \"script\": \"...\" })`, `browser({ \"action\": \"screenshot\" })` (returns PNG path ready to inspect or send via `send` media), `browser({ \"action\": \"cookies\" })`, `browser({ \"action\": \"tabs\" })`, `browser({ \"action\": \"profiles\" })`, and `browser({ \"action\": \"close\" })` (scales the browser process to zero while keeping the profile logged in on disk).\n\
         - **How to Reply / Send Messages**: Plain text printed only to your local session/stdout is **never** delivered to other agents, your parent, groups, or channels. Whenever a message or task asks you to reply, answer, or report back, you **must** call the AnyAgents `send` MCP tool (`send`, or `await tools.anyagents.send({ ... })` inside `execute` in OpenCode Code Mode) with `to` (e.g. `[\"parent\"]` or the `answer_to` / `from` address), `reply_to` (the `anyagents:reply:msg_...` token from the `<message>` tag), and `text`.\n\
         - **Channel & Message Interactions (`interact`)**: Use the `interact` MCP tool with `target` set to the `anyagents:reply:msg_...` token from the message header (or `telegram:chat:<chat_id>:<message_id>`) and `action`: `\"react\"` (with `emoji`, e.g. `\"👍\"`, `\"🔥\"`), `\"vote\"` (with `options`, e.g. `[\"1\"]` or option text, and optional `text` rationale), `\"edit\"`, `\"delete\"`, `\"pin\"`, `\"unpin\"`, or `\"stop_poll\"`.\n\
         - **Memory, History & Hybrid RAG Search (`conversation` / `search`)**: Incoming messages are pushed automatically to your session — never poll an inbox. Use `conversation({ \"query\": \"...\", \"agent\": \"all\", \"limit\": 20 })` or `search({ \"query\": \"...\" })` to run Hybrid RAG search (dense vector embedding `voyageai/voyage-4-large` + SQLite FTS5 BM25) across all agents' historical and live conversations since September without loading the full history. Omit `query` (`conversation({ \"agent\": \"sage\", \"limit\": 50 })`) to read recent chronological two-way (`in` + `out`) turns.\n",
    );

    let paths = [
        config.join("SYSTEM_PROMPT.md"),
        config.join("AGENTS.md"),
        config.join("opencode").join("AGENTS.md"),
        config.join(".config").join("opencode").join("AGENTS.md"),
        config.join("CLAUDE.md"),
        config.join(".claude").join("CLAUDE.md"),
        config.join(".gemini").join("config").join("GEMINI.md"),
        config.join(".gemini").join("config").join("AGENTS.md"),
        config
            .join(".gemini")
            .join("config")
            .join("rules")
            .join("AGENTS.md"),
        config.join("muse").join("AGENTS.md"),
        config.join(".config").join("muse").join("AGENTS.md"),
        config.join(".omp").join("agent").join("APPEND_SYSTEM.md"),
        config.join(".omp").join("agent").join("AGENTS.md"),
        config.join(".dsh").join("AGENTS.md"),
        config.join(".grok").join("AGENTS.md"),
    ];
    for path in &paths {
        if std::fs::read(path).ok().as_deref() == Some(doc.as_bytes()) {
            continue;
        }
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::fs::write(path, doc.as_bytes());
    }
}

pub(crate) fn extract_file_tilde_refs(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = text;
    while let Some(idx) = rest.find("{file:~/") {
        let after = &rest[idx + "{file:~/".len()..];
        if let Some(end) = after.find('}') {
            let rel = after[..end].trim();
            if !rel.is_empty() && !rel.contains("..") && !out.iter().any(|existing| existing == rel)
            {
                out.push(rel.to_string());
            }
            rest = &after[end + 1..];
        } else {
            break;
        }
    }
    out
}

pub(crate) fn ensure_seat_environment(target_config: &Path) {
    for subdir in ["tmp", "data-home", "cache-home", "state-home"] {
        let _ = std::fs::create_dir_all(target_config.join(subdir));
    }
    let gitconfig = target_config.join(".gitconfig");
    if !gitconfig.exists() {
        let _ = std::fs::write(&gitconfig, "[safe]\n\tdirectory = *\n");
    }
    #[cfg(unix)]
    if let Some(host_home) = crate::sandbox::host_home_dir() {
        let host_local = host_home.join(".local");
        if host_local.is_dir() {
            let local_dir = target_config.join(".local");
            let _ = std::fs::create_dir_all(&local_dir);
            let opt_link = local_dir.join("opt");
            let host_opt = host_local.join("opt");
            if host_opt.is_dir() && !opt_link.exists() {
                let _ = std::os::unix::fs::symlink(&host_opt, &opt_link);
            }
            let bin_dir = local_dir.join("bin");
            let _ = std::fs::create_dir_all(&bin_dir);
            let host_bin = host_local.join("bin");
            if let Ok(entries) = std::fs::read_dir(&host_bin) {
                for entry in entries.flatten() {
                    let src = entry.path();
                    let dst = bin_dir.join(entry.file_name());
                    if src.exists() && !dst.exists() {
                        let _ = std::os::unix::fs::symlink(&src, &dst);
                    }
                }
            }
        }
        for shared_dir in [".kube", ".config/gh"] {
            let src = host_home.join(shared_dir);
            let dst = target_config.join(shared_dir);
            if src.exists() && !dst.exists() {
                if let Some(parent) = dst.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                let _ = std::os::unix::fs::symlink(&src, &dst);
            }
        }
        for (rel, shared_fallback) in [
            (".rustup", "/scratch/rustup-home"),
            (".cargo", "/scratch/cargo-registry"),
        ] {
            let dst = target_config.join(rel);
            if !dst.exists() {
                let h = host_home.join(rel);
                let found_shared = if h.is_dir() {
                    Some(h)
                } else {
                    let fallback = Path::new(shared_fallback);
                    fallback.is_dir().then(|| fallback.to_path_buf())
                };
                if let Some(src) = found_shared {
                    let _ = std::os::unix::fs::symlink(&src, &dst);
                }
            }
        }
        ensure_seat_skills(target_config, &host_home);
    }
}

#[cfg(unix)]
pub(crate) fn ensure_seat_skills(target_config: &Path, host_home: &Path) {
    let data_skills = target_config
        .parent()
        .and_then(|p| p.parent())
        .map(|data_dir| data_dir.join("skills"));
    let canonical_skills = [
        data_skills.as_deref(),
        Some(Path::new("/scratch/wt/sylphx-skills/skills")),
    ]
    .into_iter()
    .flatten()
    .find(|p| p.is_dir())
    .map(Path::to_path_buf)
    .or_else(|| {
        let candidate = host_home.join("workspace/github.com/SylphxAI/skills/skills");
        candidate.is_dir().then_some(candidate)
    });
    let Some(mut skills_root) = canonical_skills else {
        return;
    };
    if let Some(ref data_sk) = data_skills
        && !data_sk.exists()
        && skills_root != *data_sk
        && std::os::unix::fs::symlink(&skills_root, data_sk).is_ok()
    {
        skills_root = data_sk.clone();
    }
    for rel in [
        "skills",
        ".agents/skills",
        ".claude/skills",
        ".codex/skills",
        "opencode/skills",
        ".config/opencode/skills",
        ".gemini/config/skills",
        "muse/skills",
        ".config/muse/skills",
        ".omp/agent/skills",
        ".dsh/skills",
        ".grok/skills",
    ] {
        let dst = target_config.join(rel);
        if let Ok(meta) = std::fs::symlink_metadata(&dst) {
            if meta.file_type().is_symlink() && !dst.exists() {
                let _ = std::fs::remove_file(&dst);
            } else {
                continue;
            }
        }
        if let Some(parent) = dst.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::os::unix::fs::symlink(&skills_root, &dst);
    }
}

pub(crate) fn ensure_seat_op_config(target_config: &Path, seat_launch: &str) {
    #[cfg(unix)]
    if let Some(host_home) = crate::sandbox::host_home_dir() {
        let op_token_src = host_home.join(".config/op-seat/service-account-token");
        let op_token_dst = target_config.join(".config/op/service-account-token");
        if !op_token_src.exists() || op_token_dst.exists() {
            return;
        }
        let mut parts = seat_launch.split('\t');
        let (Some(launcher), Some(uid), Some(gid), Some(groups)) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return;
        };
        let _ = std::process::Command::new(launcher)
            .args([
                "--reuid",
                uid,
                "--regid",
                gid,
                "--groups",
                groups,
                "--no-new-privs",
                "--",
                "sh",
                "-c",
                "cd \"$1\" && mkdir -p .config/op && chmod 0700 .config/op && ln -sfn \"$2\" .config/op/service-account-token",
                "sh",
            ])
            .arg(target_config)
            .arg(&op_token_src)
            .stderr(std::process::Stdio::null())
            .status();
    }
}

pub(crate) fn chmod_seat_file_readable(path: &Path) {
    #[cfg(unix)]
    if !crate::sandbox::is_isolated_test_env()
        && Path::new("/usr/local/bin/anyagents-setpriv").is_file()
        && let Ok(meta) = std::fs::metadata(path)
    {
        use std::os::unix::fs::MetadataExt as _;
        let uid = meta.uid();
        let gid = meta.gid();
        if uid != 0
            && let (Some(parent), Some(fname)) = (path.parent(), path.file_name())
        {
            let uid_s = uid.to_string();
            let gid_s = gid.to_string();
            let _ = std::process::Command::new("/usr/local/bin/anyagents-setpriv")
                .current_dir(parent)
                .args([
                    "--reuid",
                    &uid_s,
                    "--regid",
                    &gid_s,
                    "--groups",
                    &gid_s,
                    "--no-new-privs",
                    "--",
                    "/bin/chmod",
                    "0664",
                ])
                .arg(fname)
                .stderr(std::process::Stdio::null())
                .status();
        }
    }
}

pub(crate) fn write_seat_shared_file(dst: &Path, bytes: &[u8], mode: u32) -> bool {
    if let Some(parent) = dst.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if std::fs::write(dst, bytes).is_err() {
        chmod_seat_file_readable(dst);
        if std::fs::write(dst, bytes).is_err() {
            let _ = std::fs::remove_file(dst);
            if std::fs::write(dst, bytes).is_err() {
                return false;
            }
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(dst, std::fs::Permissions::from_mode(mode));
    }
    true
}

pub(crate) fn write_profile_marker(config_dir: &Path, marker_file: &str, content: &str) {
    let _ = write_seat_shared_file(&config_dir.join(marker_file), content.as_bytes(), 0o664);
}

pub(crate) fn seeded_profile_name(config_dir: &Path) -> Option<String> {
    std::fs::read_to_string(config_dir.join(".anyagents-seeded-profile"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

pub(crate) fn seed_harness_credentials(
    store: &Store,
    data: &Path,
    agent: &Agent,
    target_config: &Path,
) {
    let Ok(all_agents) = store.list_agents() else {
        return;
    };
    let static_rel_paths: &[&str] = match agent.harness.as_str() {
        "claude-code" => &[".credentials.json", ".claude/.credentials.json"],
        "codex" => &[
            "auth.json",
            ".codex/auth.json",
            "models_cache.json",
            "model-catalog-direct.json",
            ".config/desk/sylphx-ai/key",
            ".config/opencode/sylphx.key",
            ".config/opencode/opencode.json",
        ],
        "opencode" => &[
            "opencode/opencode.json",
            ".config/opencode/opencode.json",
            "opencode.json",
            "data-home/opencode/auth.json",
            ".local/share/opencode/auth.json",
        ],
        "agy" => &[
            ".gemini/antigravity-cli/antigravity-oauth-token",
            ".gemini/antigravity-cli/antigravity_state.pbtxt",
            ".gemini/antigravity-cli/jetski_state.pbtxt",
            ".gemini/config/config.json",
            ".gemini/config/.migrated",
        ],
        "muse" => &[
            "muse/trust.json",
            "muse/auth.json",
            ".config/muse/trust.json",
            ".config/muse/auth.json",
            ".local/share/muse/auth.json",
        ],
        "omp" => &[
            ".omp/agent/agent.db",
            ".omp/agent/models.yml",
            ".omp/agent/models.db",
            "agent.db",
            "models.yml",
            "models.db",
        ],
        "dsh" => &[
            ".dsh/.credentials.yaml",
            ".dsh/settings.yaml",
            ".dsh/.anonymous-user-id",
            ".dsh/profiles/headless/cordis.yml",
        ],
        "grok" => &[
            ".grok/auth.json",
            ".grok/models_cache.json",
            ".grok/settings_cache.json",
        ],
        _ => &[],
    };
    let mut rel_paths: Vec<String> = static_rel_paths.iter().map(|s| (*s).to_string()).collect();

    let profile_roots = select_or_bind_profile(data, &all_agents, agent, target_config);
    let current_bound = bound_profile_name(target_config);
    let last_seeded = seeded_profile_name(target_config);
    let explicit_non_default_profile = current_bound.as_ref().is_some_and(|b| {
        let prof = b.split_once('/').map(|(_, p)| p).unwrap_or(b.as_str());
        !prof.is_empty() && prof != "default"
    });
    let profile_changed = current_bound.is_some()
        && (current_bound != last_seeded || (last_seeded.is_none() && explicit_non_default_profile));

    for profile_root in &profile_roots {
        for candidate in [
            ".config/opencode/opencode.json",
            "opencode/opencode.json",
            "opencode.json",
        ] {
            if let Ok(text) = std::fs::read_to_string(profile_root.join(candidate)) {
                for r in extract_file_tilde_refs(&text) {
                    if !rel_paths.contains(&r) {
                        rel_paths.push(r);
                    }
                }
            }
        }
    }
    for rel in &rel_paths {
        let dst = target_config.join(rel);
        let is_auth_cred = rel.ends_with(".key")
            || matches!(
                rel.as_str(),
                ".gemini/antigravity-cli/antigravity-oauth-token"
                    | ".gemini/antigravity-cli/antigravity_state.pbtxt"
                    | ".gemini/antigravity-cli/jetski_state.pbtxt"
                    | ".credentials.json"
                    | ".claude/.credentials.json"
                    | "auth.json"
                    | ".codex/auth.json"
                    | "muse/auth.json"
                    | ".config/muse/auth.json"
                    | ".grok/auth.json"
                    | ".dsh/.credentials.yaml"
                    | ".omp/agent/agent.db"
                    | "agent.db"
            );
        let dst_non_empty = dst.metadata().is_ok_and(|m| m.len() > 0);
        if dst_non_empty && !is_auth_cred {
            continue;
        }
        for profile_root in &profile_roots {
            let mut src = profile_root.join(rel);
            if !src.is_file() {
                let fallback = match rel.as_str() {
                    ".claude/.credentials.json" => Some(".credentials.json"),
                    ".codex/auth.json" => Some("auth.json"),
                    ".config/muse/auth.json" => Some("muse/auth.json"),
                    _ => None,
                };
                if let Some(fb) = fallback {
                    src = profile_root.join(fb);
                }
            }
            if src.is_file() {
                let Ok(src_bytes) = std::fs::read(&src) else {
                    continue;
                };
                if dst_non_empty {
                    if std::fs::read(&dst).is_err() {
                        chmod_seat_file_readable(&dst);
                    }
                    if let Ok(dst_bytes) = std::fs::read(&dst) {
                        if src_bytes == dst_bytes {
                            break;
                        }
                        if !profile_changed {
                            let newer_oauth = matches!(
                                (
                                    json_oauth_expires_at(&src_bytes),
                                    json_oauth_expires_at(&dst_bytes)
                                ),
                                (Some(s_exp), Some(d_exp)) if s_exp > d_exp
                            );
                            if !newer_oauth {
                                break;
                            }
                        }
                    } else if !profile_changed {
                        break;
                    }
                }
                if write_seat_shared_file(&dst, &src_bytes, 0o660) {
                    break;
                }
            }
        }
    }
    if let Some(ref bound) = current_bound {
        write_profile_marker(target_config, ".anyagents-seeded-profile", &format!("{bound}\n"));
    }
}

/// One Upstream Provider Profile in the server's centralized profile pool
/// (`<data>/profiles/<provider>/<profile>`).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct ProviderProfileEntry {
    pub provider: String,
    pub profile: String,
    pub kind: String,
    pub compatible_harnesses: Vec<String>,
    pub assigned_agents: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<String>,
}

/// Bind only an existing named provider profile, never a caller-supplied path.
pub(crate) fn bind_profile(
    store: &Store,
    agent: &Agent,
    profile: &str,
    model: &str,
) -> Result<(), Error> {
    let provider = upstream_provider_of(&agent.harness, Some(model));
    let (named_provider, name) = profile.split_once('/').unwrap_or((&provider, profile));
    let valid = |s: &str| {
        !s.is_empty()
            && s != "."
            && s != ".."
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
    };
    if named_provider != provider || !valid(name) || !valid(named_provider) {
        return Err(Error::Message("invalid provider profile".into()));
    }
    let source = store.data_dir().join("profiles").join(&provider).join(name);
    if !source.is_dir() {
        return Err(Error::Message("provider profile not found".into()));
    }
    let config = agent_config_dir(store.data_dir(), &agent.id);
    fn copy_files(source: &Path, target: &Path) -> Result<(), Error> {
        std::fs::create_dir_all(target)?;
        for entry in std::fs::read_dir(source)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            let target = target.join(entry.file_name());
            if kind.is_dir() {
                copy_files(&entry.path(), &target)?;
            } else if kind.is_file() && entry.file_name() != "profile.json" {
                if let Ok(bytes) = std::fs::read(entry.path()) {
                    let _ = write_seat_shared_file(&target, &bytes, 0o660);
                }
            }
        }
        Ok(())
    }
    copy_files(&source, &config)
        .map_err(|_| Error::Message("could not bind provider profile".into()))?;
    let normalized = format!("{provider}/{name}\n");
    write_profile_marker(&config, ".anyagents-profile", &normalized);
    write_profile_marker(&config, ".anyagents-seeded-profile", &normalized);
    write_profile_marker(
        &config,
        ".anyagents-profile-rotated-ms",
        &format!("{}\n", anyagents_core::now_ms()),
    );
    store.set_agent_model(&agent.id, model)?;
    if agent.harness == "opencode" {
        let command = std::env::current_exe()?;
        let args = vec![
            "--data".into(),
            store.data_dir().display().to_string(),
            "mcp".into(),
            "--config".into(),
            config.display().to_string(),
        ];
        opencode_config::write(&config, Some(model), &command, &args)?;
    } else if agent.harness == "codex" {
        let command = std::env::current_exe()?;
        let args = vec![
            "--data".into(),
            store.data_dir().display().to_string(),
            "mcp".into(),
            "--config".into(),
            config.display().to_string(),
        ];
        crate::mcp_config::write_codex_with_model(
            &config.join("config.toml"),
            Some(model),
            &command,
            &args,
        )?;
    } else if agent.harness == "claude-code" {
        let _ = crate::mcp_config::sanitize_claude_settings(&config);
    }
    Ok(())
}

/// Create or update a named provider profile in `<data>/profiles/<provider>/<profile>`.
pub(crate) fn set_provider_profile(
    data: &Path,
    provider_profile: &str,
    key: &str,
) -> Result<(String, String), Error> {
    let (provider, profile) = provider_profile.split_once('/').ok_or_else(|| {
        Error::Message("expected <provider>/<profile> (e.g. kimi-code-plan/rhea)".into())
    })?;
    let valid = |s: &str| {
        !s.is_empty()
            && s != "."
            && s != ".."
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
    };
    if !valid(provider) || !valid(profile) {
        return Err(Error::Message("invalid provider or profile name".into()));
    }
    let trimmed = key.trim();
    if trimmed.is_empty() || trimmed.contains(char::is_whitespace) {
        return Err(Error::Message(
            "a nonempty single-line key is required".into(),
        ));
    }
    let dir = data.join("profiles").join(provider).join(profile);
    ensure_provider_profile_meta(&dir, provider, None);
    let default_dir = data.join("profiles").join(provider).join("default");
    if default_dir != dir {
        for rel in [".config/opencode/opencode.json", "opencode.json"] {
            if default_dir.join(rel).is_file() && !dir.join(rel).exists() {
                if let Some(p) = dir.join(rel).parent() {
                    let _ = std::fs::create_dir_all(p);
                }
                let _ = std::fs::copy(default_dir.join(rel), dir.join(rel));
            }
        }
    }
    let key_filenames: &[&str] = match provider {
        "kimi-code-plan" => &["kimi-code-plan.key", "kimi.key"],
        "qwen-token-plan" => &["qwen-coding-plan.key", "qwen.key"],
        "glm-code-plan" => &["glm-code-plan.key", "glm.key"],
        "minimax-code-plan" => &["minimax-code-plan.key", "minimax.key"],
        "sylphx" => &["sylphx.key"],
        "openrouter" => &["openrouter.key"],
        "deepseek" => &["deepseek.key"],
        "google-gemini" => &["gemini.key"],
        other => {
            let prefix = other.split('-').next().unwrap_or(other);
            let fname = format!("{prefix}.key");
            write_profile_key_file(&dir, &fname, trimmed)?;
            let rel = format!(".config/opencode/{fname}");
            propagate_profile_files_to_bound_agents(data, provider, profile, &[rel.as_str()]);
            return Ok((provider.to_string(), profile.to_string()));
        }
    };
    let mut rels = Vec::new();
    for fname in key_filenames {
        write_profile_key_file(&dir, fname, trimmed)?;
        rels.push(format!(".config/opencode/{fname}"));
    }
    let rel_refs: Vec<&str> = rels.iter().map(String::as_str).collect();
    propagate_profile_files_to_bound_agents(data, provider, profile, &rel_refs);
    Ok((provider.to_string(), profile.to_string()))
}

fn write_profile_key_file(dir: &Path, filename: &str, key: &str) -> Result<(), Error> {
    let key_dir = dir.join(".config").join("opencode");
    std::fs::create_dir_all(&key_dir)?;
    let key_path = key_dir.join(filename);
    std::fs::write(&key_path, format!("{key}\n"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

pub(crate) fn propagate_profile_files_to_bound_agents(
    data: &Path,
    provider: &str,
    profile: &str,
    rel_paths: &[&str],
) {
    let prof_dir = data.join("profiles").join(provider).join(profile);
    let target_tag = format!("{provider}/{profile}");
    let store_opt = Store::open(data.join("state.db")).ok();
    let all_agents = store_opt
        .as_ref()
        .and_then(|s| s.list_agents().ok())
        .unwrap_or_default();
    let Ok(agents_dir) = std::fs::read_dir(data.join("agents")) else {
        return;
    };
    for entry in agents_dir.flatten() {
        let cfg = entry.path().join("config");
        if !cfg.is_dir() {
            continue;
        }
        let dir_id = entry.file_name().to_string_lossy().into_owned();
        let agent_rec = all_agents.iter().find(|a| a.id.as_str() == dir_id);
        let bound = bound_profile_name(&cfg);
        let matches_target = match bound.as_deref() {
            Some(b) if b == target_tag => true,
            Some(b) if b == profile => agent_rec
                .is_some_and(|a| upstream_provider_of(&a.harness, a.model.as_deref()) == provider),
            None if profile == "default" => agent_rec
                .is_some_and(|a| upstream_provider_of(&a.harness, a.model.as_deref()) == provider),
            _ => false,
        };
        if !matches_target {
            continue;
        }
        for rel in rel_paths {
            let mut src = prof_dir.join(rel);
            if !src.is_file() {
                let fb = match *rel {
                    ".claude/.credentials.json" => Some(".credentials.json"),
                    ".codex/auth.json" => Some("auth.json"),
                    ".config/muse/auth.json" => Some("muse/auth.json"),
                    _ => None,
                };
                if let Some(f) = fb {
                    src = prof_dir.join(f);
                }
            }
            if let Ok(bytes) = std::fs::read(&src) {
                let _ = write_seat_shared_file(&cfg.join(rel), &bytes, 0o660);
            }
        }
        write_profile_marker(&cfg, ".anyagents-seeded-profile", &format!("{target_tag}\n"));
        if let Some(a) = agent_rec {
            let parent_name = a
                .parent_id
                .as_ref()
                .and_then(|pid| all_agents.iter().find(|p| &p.id == pid))
                .map(|p| p.name.as_str());
            if let Some((_, _, gid, _)) =
                crate::sandbox::seat_for_agent_with_hint(&a.name, parent_name)
            {
                crate::sandbox::share_config_with_seat(&cfg, gid);
            }
        }
    }
}

/// Resolves the upstream provider an agent draws its credential profile from.
///
/// An agent's `model` prefix (`sylphx/...`, `openrouter/...`, `qwen-token-plan/...`,
/// `kimi-code-plan/...`, `glm-code-plan/...`, `minimax-code-plan/...`) takes
/// precedence because any harness (`opencode`, `omp`, `claude-code`, `codex`)
/// can run against that upstream provider; otherwise defaults to the native
/// subscription provider of the agent's harness.
pub(crate) fn upstream_provider_of(harness: &str, model: Option<&str>) -> String {
    if let Some(m) = model.map(str::trim).filter(|s| !s.is_empty())
        && let Some((prefix, _)) = m.split_once('/')
    {
        match prefix {
            "sylphx" | "openrouter" | "qwen-token-plan" | "kimi-code-plan" | "glm-code-plan"
            | "minimax-code-plan" | "anthropic" | "openai" | "xai" | "deepseek" => {
                return prefix.to_string();
            }
            _ => {}
        }
    }
    match harness {
        "claude-code" => "anthropic".to_string(),
        "codex" => "openai".to_string(),
        "agy" => "google-antigravity".to_string(),
        "muse" => "muse".to_string(),
        "dsh" => "deepseek".to_string(),
        "grok" => "xai".to_string(),
        "opencode" | "omp" => "sylphx".to_string(),
        other => other.to_string(),
    }
}

pub(crate) fn default_kind_and_harnesses(provider: &str) -> (&'static str, Vec<String>) {
    let harnesses = |list: &[&str]| list.iter().map(|s| (*s).to_string()).collect();
    match provider {
        "sylphx" | "openrouter" | "opengateway" | "google-gemini" => (
            "api-key",
            harnesses(&[
                "opencode",
                "omp",
                "claude-code",
                "codex",
                "muse",
                "grok",
                "dsh",
            ]),
        ),
        "qwen-token-plan" | "kimi-code-plan" | "glm-code-plan" | "minimax-code-plan" => (
            "coding-plan",
            harnesses(&["opencode", "omp", "claude-code", "codex", "dsh"]),
        ),
        "anthropic" => (
            "subscription",
            harnesses(&["claude-code", "opencode", "omp", "muse"]),
        ),
        "openai" => (
            "subscription",
            harnesses(&["codex", "opencode", "omp", "muse"]),
        ),
        "google-antigravity" => ("subscription", harnesses(&["agy", "opencode", "omp"])),
        "muse" => ("subscription", harnesses(&["muse", "omp"])),
        "deepseek" => ("subscription", harnesses(&["dsh", "opencode", "omp"])),
        "xai" => ("subscription", harnesses(&["grok", "opencode", "omp"])),
        "cursor" => ("subscription", harnesses(&["omp", "opencode"])),
        _ => (
            "api-key",
            harnesses(&["opencode", "omp", "claude-code", "codex"]),
        ),
    }
}

fn copy_host_file(src: &Path, dst: &Path) -> bool {
    if !src.is_file() {
        return false;
    }
    if dst.is_file() {
        return true;
    }
    if let Some(p) = dst.parent() {
        let _ = std::fs::create_dir_all(p);
    }
    std::fs::copy(src, dst).is_ok()
}

fn json_oauth_expires_at(bytes: &[u8]) -> Option<i64> {
    let val: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    val.pointer("/claudeAiOauth/expiresAt")
        .and_then(serde_json::Value::as_i64)
}

fn copy_seat_file_if_newer(src: &Path, dst: &Path) -> bool {
    let Ok(src_meta) = std::fs::metadata(src) else {
        return false;
    };
    if !src_meta.is_file() || src_meta.len() == 0 {
        return false;
    }
    if std::fs::read(src).is_err() {
        chmod_seat_file_readable(src);
    }
    let Ok(src_bytes) = std::fs::read(src) else {
        return false;
    };
    if src_bytes.is_empty() {
        return false;
    }
    if let Ok(dst_meta) = std::fs::metadata(dst)
        && dst_meta.is_file()
        && dst_meta.len() > 0
    {
        if let Ok(dst_bytes) = std::fs::read(dst) {
            if src_bytes == dst_bytes {
                return true;
            }
            if let (Some(src_exp), Some(dst_exp)) = (
                json_oauth_expires_at(&src_bytes),
                json_oauth_expires_at(&dst_bytes),
            ) {
                if src_exp <= dst_exp {
                    return true;
                }
            } else if src_meta.modified().ok() <= dst_meta.modified().ok() {
                return true;
            }
        } else if src_meta.modified().ok() <= dst_meta.modified().ok() {
            return true;
        }
    }
    if let Some(p) = dst.parent() {
        let _ = std::fs::create_dir_all(p);
    }
    if std::fs::write(dst, &src_bytes).is_err() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(dst, std::fs::Permissions::from_mode(0o600));
    }
    true
}

fn ensure_provider_profile_meta(dir: &Path, provider: &str, kind_override: Option<&str>) {
    let _ = std::fs::create_dir_all(dir);
    let meta_path = dir.join("profile.json");
    if !meta_path.exists() {
        let (default_kind, harnesses) = default_kind_and_harnesses(provider);
        let kind = kind_override.unwrap_or(default_kind);
        let meta = serde_json::json!({
            "provider": provider,
            "kind": kind,
            "compatible_harnesses": harnesses,
        });
        if let Ok(text) = serde_json::to_string_pretty(&meta) {
            let _ = std::fs::write(&meta_path, text);
        }
    }
}

/// Discovers and imports existing upstream provider credentials from active seats
/// and host config into `<data>/profiles/<provider>/<profile>`, and returns all
/// upstream provider profiles sorted by provider and profile name.
pub(crate) fn sync_and_list_provider_profiles(
    store: &Store,
    data: &Path,
) -> Vec<ProviderProfileEntry> {
    let agents = store.list_agents().unwrap_or_default();
    let profiles_root = data.join("profiles");
    let _ = std::fs::create_dir_all(&profiles_root);

    // Migrate any legacy harness-named empty/default directories if they match a harness name
    // rather than a provider name and have no `profile.json`.
    for legacy_harness in [
        "claude-code",
        "codex",
        "opencode",
        "agy",
        "omp",
        "dsh",
        "grok",
        "opengateway",
    ] {
        let legacy_dir = profiles_root.join(legacy_harness);
        if legacy_dir.is_dir() && !legacy_dir.join("default").join("profile.json").exists() {
            let _ = std::fs::remove_dir_all(&legacy_dir);
        }
    }

    // Seed upstream provider profiles from existing seats on this server so pre-existing
    // logins (Subscriptions, Coding Plans, API Keys) are registered in the Master Pool.
    for agent in &agents {
        let cfg = agent_config_dir(data, &agent.id);
        let provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
        let bound = bound_profile_name(&cfg);
        let seeded = seeded_profile_name(&cfg);
        if seeded.is_some() && bound.is_some() && seeded != bound {
            continue;
        }
        let prof_name = bound
            .and_then(|b| {
                let p = b.split_once('/').map(|(_, p)| p.to_string()).unwrap_or(b);
                (!p.is_empty()).then_some(p)
            })
            .unwrap_or_else(|| "default".to_string());
        let profile_dir = profiles_root.join(&provider).join(&prof_name);
        match provider.as_str() {
            "anthropic" => {
                for rel in [".credentials.json", ".claude/.credentials.json"] {
                    let src = cfg.join(rel);
                    if src.is_file() {
                        ensure_provider_profile_meta(&profile_dir, "anthropic", None);
                        let _ = copy_seat_file_if_newer(&src, &profile_dir.join(rel));
                    }
                }
            }
            "openai" => {
                for rel in ["auth.json", ".codex/auth.json"] {
                    let src = cfg.join(rel);
                    if src.is_file() {
                        ensure_provider_profile_meta(&profile_dir, "openai", None);
                        let _ = copy_seat_file_if_newer(&src, &profile_dir.join(rel));
                    }
                }
            }
            "google-antigravity" => {
                for rel in [
                    ".gemini/antigravity-cli/antigravity-oauth-token",
                    ".gemini/antigravity-cli/antigravity_state.pbtxt",
                    ".gemini/config/config.json",
                ] {
                    let src = cfg.join(rel);
                    if src.is_file() {
                        ensure_provider_profile_meta(&profile_dir, "google-antigravity", None);
                        let _ = copy_seat_file_if_newer(&src, &profile_dir.join(rel));
                    }
                }
            }
            "muse" => {
                for rel in ["muse/auth.json", ".config/muse/auth.json"] {
                    let src = cfg.join(rel);
                    if src.is_file() {
                        ensure_provider_profile_meta(&profile_dir, "muse", None);
                        let _ = copy_seat_file_if_newer(&src, &profile_dir.join(rel));
                    }
                }
            }
            "sylphx" | "qwen-token-plan" | "kimi-code-plan" | "glm-code-plan"
            | "minimax-code-plan" => {
                let kind = if provider == "sylphx" {
                    "api-key"
                } else {
                    "coding-plan"
                };
                let opencode_rel = ".config/opencode/opencode.json";
                let src_json = cfg.join(opencode_rel);
                if src_json.is_file() {
                    ensure_provider_profile_meta(&profile_dir, &provider, Some(kind));
                    let dst_json = profile_dir.join(opencode_rel);
                    if !dst_json.is_file()
                        && let Ok(text) = std::fs::read_to_string(&src_json)
                        && let Ok(val) = serde_json::from_str::<serde_json::Value>(&text)
                        && let Some(prov_block) = val.get("provider")
                    {
                        if let Some(p) = dst_json.parent() {
                            let _ = std::fs::create_dir_all(p);
                        }
                        let clean = serde_json::json!({
                            "$schema": "https://opencode.ai/config.json",
                            "provider": prov_block,
                        });
                        if let Ok(clean_bytes) = serde_json::to_vec_pretty(&clean) {
                            let _ = std::fs::write(&dst_json, clean_bytes);
                        }
                    }
                    if let Ok(text) = std::fs::read_to_string(&src_json) {
                        for rel in extract_file_tilde_refs(&text) {
                            let _ = copy_host_file(&cfg.join(&rel), &profile_dir.join(&rel));
                        }
                    }
                }
            }
            _ => {}
        }
    }

    let mut entries: Vec<ProviderProfileEntry> = Vec::new();
    if let Ok(providers) = std::fs::read_dir(&profiles_root) {
        for prov_entry in providers.flatten() {
            let prov_path = prov_entry.path();
            if !prov_path.is_dir() {
                continue;
            }
            let provider = prov_entry.file_name().to_string_lossy().into_owned();
            if provider.starts_with('.') {
                continue;
            }
            let Ok(prof_dirs) = std::fs::read_dir(&prov_path) else {
                continue;
            };
            for prof_entry in prof_dirs.flatten() {
                let prof_path = prof_entry.path();
                if !prof_path.is_dir() {
                    continue;
                }
                let profile = prof_entry.file_name().to_string_lossy().into_owned();
                if profile.starts_with('.') {
                    continue;
                }
                let (default_kind, default_harnesses) = default_kind_and_harnesses(&provider);
                let mut kind = default_kind.to_string();
                let mut compatible_harnesses = default_harnesses;
                if let Ok(raw) = std::fs::read_to_string(prof_path.join("profile.json"))
                    && let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw)
                {
                    if let Some(k) = val.get("kind").and_then(serde_json::Value::as_str) {
                        kind = k.to_string();
                    }
                    if let Some(arr) = val
                        .get("compatible_harnesses")
                        .and_then(serde_json::Value::as_array)
                    {
                        let parsed: Vec<String> = arr
                            .iter()
                            .filter_map(|v| v.as_str().map(str::to_string))
                            .collect();
                        if !parsed.is_empty() {
                            compatible_harnesses = parsed;
                        }
                    }
                }
                entries.push(ProviderProfileEntry {
                    provider: provider.clone(),
                    profile,
                    kind,
                    compatible_harnesses,
                    assigned_agents: Vec::new(),
                    usage: None,
                });
            }
        }
    }

    entries.sort_by(|a, b| (&a.provider, &a.profile).cmp(&(&b.provider, &b.profile)));
    enrich_profiles_usage(data, &mut entries);

    // Bind active agents to their resolved `<provider>/<profile>`, restore affinity
    // (`<provider>/<agent.name>`) when a dedicated profile recovers quota (`>= 10%`),
    // auto-failover any agent bound to an exhausted (`0% left`) profile when another
    // profile under the same upstream provider has healthy quota (`>= 20% left`), and
    // auto-clear any stale session quota hold when the bound profile now has `> 0%` left.
    let now_ms = anyagents_core::now_ms();
    for agent in &agents {
        if agent.state == "archived" {
            continue;
        }
        let cfg = agent_config_dir(data, &agent.id);
        let expected_provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
        let bound = bound_profile_name(&cfg);
        let (bound_prov, mut bound_prof) = match bound.as_deref() {
            Some(s) if s.contains('/') => {
                let (p, n) = s.split_once('/').unwrap_or((&expected_provider, "default"));
                (p.to_string(), n.to_string())
            }
            Some(s) if !s.is_empty() => (expected_provider.clone(), s.to_string()),
            _ => (expected_provider.clone(), "default".to_string()),
        };
        if bound_prov == expected_provider && bound_prof != agent.name {
            let affinity_pct = entries
                .iter()
                .find(|e| e.provider == expected_provider && e.profile == agent.name)
                .and_then(|e| e.usage.as_deref())
                .and_then(|u| parse_remaining_pct_from_usage(u, agent.model.as_deref()));
            if affinity_pct.is_some_and(|p| p >= 10) {
                rotate_agent_to_profile(store, data, agent, &expected_provider, &agent.name);
                bound_prof = agent.name.clone();
            }
        }
        let mut cur_pct = entries
            .iter()
            .find(|e| e.provider == bound_prov && e.profile == bound_prof)
            .and_then(|e| e.usage.as_deref())
            .and_then(|u| parse_remaining_pct_from_usage(u, agent.model.as_deref()));
        if cur_pct == Some(0) {
            if let Some((best_alt, alt_pct)) = entries
                .iter()
                .filter(|e| e.provider == bound_prov && e.profile != bound_prof)
                .filter_map(|e| {
                    let pct = parse_remaining_pct_from_usage(
                        e.usage.as_deref()?,
                        agent.model.as_deref(),
                    )?;
                    (pct >= 20).then_some((e.profile.clone(), e.assigned_agents.len(), pct))
                })
                .min_by_key(|(_, assigned, pct)| (*assigned, std::cmp::Reverse(*pct)))
                .map(|(prof, _, pct)| (prof, pct))
            {
                rotate_agent_to_profile(store, data, agent, &bound_prov, &best_alt);
                bound_prof = best_alt;
                cur_pct = Some(alt_pct);
            }
        }
        if cur_pct.is_some_and(|p| p > 5) {
            if agent.harness == "agy" {
                clear_stale_agy_quota_error_steps(&cfg);
            }
            write_profile_marker(
                &cfg,
                ".anyagents-profile-rotated-ms",
                &format!("{now_ms}\n"),
            );
            poke_agent_wake_socket(data, &agent.id);
        }
        let normalized = format!("{bound_prov}/{bound_prof}");
        if bound.as_deref() != Some(normalized.as_str()) {
            write_profile_marker(&cfg, ".anyagents-profile", &format!("{normalized}\n"));
        }
        if let Some(entry) = entries
            .iter_mut()
            .find(|e| e.provider == bound_prov && e.profile == bound_prof)
        {
            entry.assigned_agents.push(agent.name.clone());
        } else if let Some(entry) = entries.iter_mut().find(|e| e.provider == expected_provider) {
            entry.assigned_agents.push(agent.name.clone());
        }
    }

    for entry in &mut entries {
        entry.assigned_agents.sort();
    }
    entries
}

/// Syncs any fresher OAuth credentials from active seats back into
/// `<data>/profiles/<provider>/<profile>` and probes native upstream usage,
/// quota, or balance for each profile in parallel.
pub(crate) fn enrich_profiles_usage(data: &Path, entries: &mut [ProviderProfileEntry]) {
    if crate::sandbox::is_isolated_test_env() || entries.is_empty() {
        return;
    }
    let profiles_root = data.join("profiles");
    if let Ok(store) = Store::open(data.join("state.db"))
        && let Ok(agents) = store.list_agents()
    {
        for agent in &agents {
            let cfg = agent_config_dir(data, &agent.id);
            let bound = bound_profile_name(&cfg);
            let seeded = seeded_profile_name(&cfg);
            if seeded.is_some() && bound.is_some() && seeded != bound {
                continue;
            }
            let provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
            let prof_name = bound
                .and_then(|b| {
                    let p = b.split_once('/').map(|(_, p)| p.to_string()).unwrap_or(b);
                    (!p.is_empty()).then_some(p)
                })
                .unwrap_or_else(|| "default".to_string());
            let profile_dir = profiles_root.join(&provider).join(&prof_name);
            let rels: &[&str] = match provider.as_str() {
                "anthropic" => &[".credentials.json", ".claude/.credentials.json"],
                "openai" => &["auth.json", ".codex/auth.json"],
                "google-antigravity" => &[".gemini/antigravity-cli/antigravity-oauth-token"],
                _ => &[],
            };
            for rel in rels {
                let src = cfg.join(rel);
                if src.is_file() {
                    let _ = copy_seat_file_if_newer(&src, &profile_dir.join(rel));
                }
            }
        }
    }

    std::thread::scope(|s| {
        let mut handles = Vec::new();
        for (idx, entry) in entries.iter().enumerate() {
            if entry.usage.is_some() {
                continue;
            }
            let provider = entry.provider.clone();
            let profile = entry.profile.clone();
            handles
                .push(s.spawn(move || (idx, probe_one_profile_usage(data, &provider, &profile))));
        }
        for handle in handles {
            if let Ok((idx, Some(usage))) = handle.join()
                && let Some(entry) = entries.get_mut(idx)
            {
                entry.usage = Some(usage);
            }
        }
    });
}

fn read_first_non_empty_file(base: &Path, rels: &[&str]) -> Option<String> {
    for rel in rels {
        if let Ok(text) = std::fs::read_to_string(base.join(rel)) {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn read_dsh_yaml_ref(base: &Path, key_name: &str) -> Option<String> {
    let text = std::fs::read_to_string(base.join(".dsh/.credentials.yaml")).ok()?;
    let prefix = format!("{key_name}:");
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix(&prefix) {
            let val = rest.trim().trim_matches('"').trim_matches('\'');
            if !val.is_empty() {
                return Some(val.to_string());
            }
        }
    }
    None
}

fn extract_agy_oauth_client() -> Option<(String, String)> {
    let agy_bin = ["/home/kyle/.local/bin/agy", "/usr/local/bin/agy"]
        .into_iter()
        .map(Path::new)
        .find(|p| p.is_file())?;
    let bytes = std::fs::read(agy_bin).ok()?;
    let cid_needle = b"1071006060591-tmhssin2h21lcre235vtolojh4g403ep.apps.googleusercontent.com";
    if !bytes.windows(cid_needle.len()).any(|w| w == cid_needle) {
        return None;
    }
    let sec_prefix = b"GOCSPX-";
    for idx in 0..bytes.len().saturating_sub(35) {
        if &bytes[idx..idx + 7] == sec_prefix {
            let candidate = &bytes[idx..idx + 35];
            if candidate[7..]
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || *b == b'-' || *b == b'_')
            {
                let secret = String::from_utf8(candidate.to_vec()).ok()?;
                return Some((
                    String::from_utf8(cid_needle.to_vec()).ok()?,
                    secret,
                ));
            }
        }
    }
    None
}

fn extract_cursor_oauth_from_db(prof_dir: &Path) -> Option<(String, String)> {
    let db_path = ["agent.db", ".omp/agent/agent.db"]
        .into_iter()
        .map(|r| prof_dir.join(r))
        .find(|p| p.is_file())?;
    let bytes = std::fs::read(db_path).ok()?;
    for needle in [
        &b"{\"access\":\"eyJ"[..],
        &b"{\"type\":\"oauth\",\"refresh\":\""[..],
    ] {
        for idx in (0..bytes.len().saturating_sub(needle.len())).rev() {
            if &bytes[idx..idx + needle.len()] == needle {
                let slice = &bytes[idx..bytes.len().min(idx + 2048)];
                let Some(end) = slice.iter().position(|b| *b == b'}') else {
                    continue;
                };
                let Ok(json_str) = std::str::from_utf8(&slice[..=end]) else {
                    continue;
                };
                let Ok(val) = serde_json::from_str::<serde_json::Value>(json_str) else {
                    continue;
                };
                let Some(access) = val
                    .get("access")
                    .and_then(serde_json::Value::as_str)
                    .map(|s| s.trim().to_string())
                else {
                    continue;
                };
                let refresh = val
                    .get("refresh")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .to_string();
                if access.starts_with("eyJ") {
                    return Some((access, refresh));
                }
            }
        }
    }
    None
}

fn decode_b64url(input: &str) -> Option<Vec<u8>> {
    let val_of = |b: u8| -> Option<u8> {
        match b {
            b'A'..=b'Z' => Some(b - b'A'),
            b'a'..=b'z' => Some(b - b'a' + 26),
            b'0'..=b'9' => Some(b - b'0' + 52),
            b'+' | b'-' => Some(62),
            b'/' | b'_' => Some(63),
            _ => None,
        }
    };
    let clean: Vec<u8> = input
        .bytes()
        .filter(|b| *b != b'=' && !b.is_ascii_whitespace())
        .collect();
    let mut out = Vec::with_capacity(clean.len() * 3 / 4);
    for chunk in clean.chunks(4) {
        let b0 = val_of(*chunk.first()?)?;
        let b1 = val_of(*chunk.get(1)?)?;
        out.push((b0 << 2) | (b1 >> 4));
        if let Some(&c2) = chunk.get(2) {
            let b2 = val_of(c2)?;
            out.push((b1 << 4) | (b2 >> 2));
            if let Some(&c3) = chunk.get(3) {
                let b3 = val_of(c3)?;
                out.push((b2 << 6) | b3);
            }
        }
    }
    Some(out)
}

fn extract_jwt_sub_tail(jwt: &str) -> Option<String> {
    let payload_b64 = jwt.split('.').nth(1)?;
    let decoded = decode_b64url(payload_b64)?;
    let val: serde_json::Value = serde_json::from_slice(&decoded).ok()?;
    let sub = val.get("sub")?.as_str()?;
    Some(sub.split('|').next_back().unwrap_or(sub).to_string())
}

fn anthropic_tier_label(sub_type: Option<&str>, rate_tier: Option<&str>) -> Option<&'static str> {
    match rate_tier.unwrap_or("") {
        "default_claude_max_20x" => return Some("max 20x"),
        "default_claude_max_5x" => return Some("max 5x"),
        _ => {}
    }
    match sub_type.unwrap_or("") {
        "max" | "claude_max" => Some("max"),
        "pro" | "claude_pro" => Some("pro"),
        "team" | "claude_team" => Some("team"),
        "enterprise" | "claude_enterprise" => Some("enterprise"),
        _ => None,
    }
}

fn sync_anthropic_profile_metadata(
    http: &ureq::Agent,
    data: &Path,
    prof_dir: &Path,
    profile: &str,
    token: &str,
    cred: &mut serde_json::Value,
) -> Option<&'static str> {
    let mut changed = false;
    if let Ok(resp) = http
        .get("https://api.anthropic.com/api/oauth/profile")
        .header("authorization", &format!("Bearer {token}"))
        .header("anthropic-beta", "oauth-2025-04-20")
        .header("cache-control", "no-cache")
        .header("user-agent", "claude-code/2.1.310")
        .call()
        && let Ok(body) = resp.into_body().read_to_string()
        && let Ok(pval) = serde_json::from_str::<serde_json::Value>(&body)
    {
        let org_type = pval
            .pointer("/organization/organization_type")
            .and_then(serde_json::Value::as_str);
        let mapped_sub = match org_type {
            Some("claude_max") => Some("max"),
            Some("claude_pro") => Some("pro"),
            Some("claude_team") => Some("team"),
            Some("claude_enterprise") => Some("enterprise"),
            _ => None,
        };
        let live_tier = pval
            .pointer("/organization/rate_limit_tier")
            .and_then(serde_json::Value::as_str);
        if let Some(oauth) = cred
            .get_mut("claudeAiOauth")
            .and_then(serde_json::Value::as_object_mut)
        {
            if let Some(ms) = mapped_sub
                && oauth.get("subscriptionType").and_then(serde_json::Value::as_str) != Some(ms)
            {
                oauth.insert(
                    "subscriptionType".to_string(),
                    serde_json::Value::String(ms.to_string()),
                );
                oauth.insert(
                    "subscriptionTypeSource".to_string(),
                    serde_json::Value::String("profile".to_string()),
                );
                changed = true;
            }
            if let Some(lt) = live_tier
                && oauth.get("rateLimitTier").and_then(serde_json::Value::as_str) != Some(lt)
            {
                oauth.insert(
                    "rateLimitTier".to_string(),
                    serde_json::Value::String(lt.to_string()),
                );
                changed = true;
            }
        }
    }
    if changed || !prof_dir.join(".claude/.credentials.json").is_file() {
        if let Ok(updated) = serde_json::to_string(cred) {
            let _ = std::fs::create_dir_all(prof_dir.join(".claude"));
            let _ = std::fs::write(prof_dir.join(".credentials.json"), &updated);
            let _ = std::fs::write(prof_dir.join(".claude/.credentials.json"), &updated);
            propagate_profile_files_to_bound_agents(
                data,
                "anthropic",
                profile,
                &[".credentials.json", ".claude/.credentials.json"],
            );
        }
    }
    let sub_type = cred
        .pointer("/claudeAiOauth/subscriptionType")
        .and_then(serde_json::Value::as_str);
    let rate_tier = cred
        .pointer("/claudeAiOauth/rateLimitTier")
        .and_then(serde_json::Value::as_str);
    anthropic_tier_label(sub_type, rate_tier)
}

pub(crate) fn probe_one_profile_usage(data: &Path, provider: &str, profile: &str) -> Option<String> {
    let prof_dir = data.join("profiles").join(provider).join(profile);
    let http = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(4)))
        .build()
        .new_agent();

    match provider {
        "anthropic" => {
            let cred_text = std::fs::read_to_string(prof_dir.join(".credentials.json"))
                .or_else(|_| std::fs::read_to_string(prof_dir.join(".claude/.credentials.json")))
                .ok()?;
            let mut cred: serde_json::Value = serde_json::from_str(&cred_text).ok()?;
            let mut token = cred
                .pointer("/claudeAiOauth/accessToken")?
                .as_str()?
                .trim()
                .to_string();
            if token.is_empty() {
                return None;
            }
            let fetch_usage = |tok: &str| -> Option<serde_json::Value> {
                let resp = http
                    .get("https://api.anthropic.com/api/oauth/usage")
                    .header("authorization", &format!("Bearer {tok}"))
                    .header("anthropic-beta", "oauth-2025-04-20")
                    .header("cache-control", "no-cache")
                    .header("user-agent", "claude-code/2.1.310")
                    .call()
                    .ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                serde_json::from_str(&body).ok()
            };
            let val = match fetch_usage(&token) {
                Some(v) => v,
                None => {
                    let rtok = cred
                        .pointer("/claudeAiOauth/refreshToken")
                        .and_then(serde_json::Value::as_str)?
                        .trim()
                        .to_string();
                    if rtok.is_empty() {
                        return None;
                    }
                    let req_body = serde_json::json!({
                        "grant_type": "refresh_token",
                        "refresh_token": rtok,
                        "client_id": "9d1c250a-e61b-44d9-88ed-5944d1962f5e",
                    })
                    .to_string();
                    let rresp = http
                        .post("https://platform.claude.com/v1/oauth/token")
                        .header("content-type", "application/json")
                        .header("user-agent", "claude-code/2.1.310")
                        .send(req_body.as_bytes())
                        .ok()?;
                    let rbody = rresp.into_body().read_to_string().ok()?;
                    let rval: serde_json::Value = serde_json::from_str(&rbody).ok()?;
                    let new_acc = rval.get("access_token")?.as_str()?.trim().to_string();
                    if let Some(oauth) = cred
                        .get_mut("claudeAiOauth")
                        .and_then(serde_json::Value::as_object_mut)
                    {
                        oauth.insert(
                            "accessToken".to_string(),
                            serde_json::Value::String(new_acc.clone()),
                        );
                        if let Some(new_ref) = rval
                            .get("refresh_token")
                            .and_then(serde_json::Value::as_str)
                        {
                            oauth.insert(
                                "refreshToken".to_string(),
                                serde_json::Value::String(new_ref.to_string()),
                            );
                        }
                        if let Some(exp_in) =
                            rval.get("expires_in").and_then(serde_json::Value::as_i64)
                        {
                            let now_ms = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_millis() as i64)
                                .unwrap_or(0);
                            oauth.insert(
                                "expiresAt".to_string(),
                                serde_json::Value::Number((now_ms + exp_in * 1000).into()),
                            );
                        }
                        if let Ok(updated) = serde_json::to_string(&cred) {
                            let _ = std::fs::create_dir_all(prof_dir.join(".claude"));
                            let _ = std::fs::write(prof_dir.join(".credentials.json"), &updated);
                            let _ = std::fs::write(
                                prof_dir.join(".claude/.credentials.json"),
                                &updated,
                            );
                            propagate_profile_files_to_bound_agents(
                                data,
                                provider,
                                profile,
                                &[".credentials.json", ".claude/.credentials.json"],
                            );
                        }
                    }
                    token = new_acc;
                    fetch_usage(&token)?
                }
            };
            let tier_badge = sync_anthropic_profile_metadata(
                &http, data, &prof_dir, profile, &token, &mut cred,
            );
            let tier_suffix = tier_badge.map(|t| format!(" ({t})")).unwrap_or_default();
            let u5 = val
                .pointer("/five_hour/utilization")
                .and_then(serde_json::Value::as_f64)?;
            let u7 = val
                .pointer("/seven_day/utilization")
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(0.0);
            let rem_5h = (100.0 - u5).clamp(0.0, 100.0).round() as i64;
            let rem_7d = (100.0 - u7).clamp(0.0, 100.0).round() as i64;
            if rem_5h == 0
                && let Some(resets_at) = val
                    .pointer("/five_hour/resets_at")
                    .and_then(serde_json::Value::as_str)
                && let Some(hhmm) = resets_at.split('T').nth(1).and_then(|t| t.get(..5))
            {
                Some(format!(
                    "5h: 0% left (resets {hhmm}Z) · 7d: {rem_7d}% left{tier_suffix}"
                ))
            } else {
                Some(format!("5h: {rem_5h}% left · 7d: {rem_7d}% left{tier_suffix}"))
            }
        }
        "openai" => {
            let auth_text = std::fs::read_to_string(prof_dir.join("auth.json"))
                .or_else(|_| std::fs::read_to_string(prof_dir.join(".codex/auth.json")))
                .ok()?;
            let mut auth: serde_json::Value = serde_json::from_str(&auth_text).ok()?;
            let token = auth
                .pointer("/tokens/access_token")?
                .as_str()?
                .trim()
                .to_string();
            if token.is_empty() {
                return None;
            }
            let acc_id = auth
                .pointer("/tokens/account_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
            let fetch_wham = |tok: &str| -> Option<serde_json::Value> {
                let mut req = http
                    .get("https://chatgpt.com/backend-api/wham/usage")
                    .header("authorization", &format!("Bearer {tok}"))
                    .header("user-agent", "codex-cli/0.164.0");
                if !acc_id.is_empty() {
                    req = req.header("chatgpt-account-id", &acc_id);
                }
                let resp = req.call().ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                serde_json::from_str(&body).ok()
            };
            let val = match fetch_wham(&token) {
                Some(v) => v,
                None => {
                    let rtok = auth
                        .pointer("/tokens/refresh_token")
                        .and_then(serde_json::Value::as_str)?
                        .trim()
                        .to_string();
                    if rtok.is_empty() {
                        return None;
                    }
                    let req_body = serde_json::json!({
                        "client_id": "app_EMoamEEZ73f0CkXaXp7hrann",
                        "grant_type": "refresh_token",
                        "refresh_token": rtok,
                    })
                    .to_string();
                    let rresp = http
                        .post("https://auth.openai.com/oauth/token")
                        .header("content-type", "application/json")
                        .send(req_body.as_bytes())
                        .ok()?;
                    let rbody = rresp.into_body().read_to_string().ok()?;
                    let rval: serde_json::Value = serde_json::from_str(&rbody).ok()?;
                    let new_acc = rval.get("access_token")?.as_str()?.trim().to_string();
                    if let Some(tokens_obj) = auth
                        .get_mut("tokens")
                        .and_then(serde_json::Value::as_object_mut)
                    {
                        tokens_obj.insert(
                            "access_token".to_string(),
                            serde_json::Value::String(new_acc.clone()),
                        );
                        if let Some(new_ref) = rval
                            .get("refresh_token")
                            .and_then(serde_json::Value::as_str)
                        {
                            tokens_obj.insert(
                                "refresh_token".to_string(),
                                serde_json::Value::String(new_ref.to_string()),
                            );
                        }
                        if let Ok(updated) = serde_json::to_string_pretty(&auth) {
                            let _ = std::fs::create_dir_all(prof_dir.join(".codex"));
                            let _ = std::fs::write(prof_dir.join("auth.json"), &updated);
                            let _ = std::fs::write(prof_dir.join(".codex/auth.json"), &updated);
                            propagate_profile_files_to_bound_agents(
                                data,
                                provider,
                                profile,
                                &["auth.json", ".codex/auth.json"],
                            );
                        }
                    }
                    fetch_wham(&new_acc)?
                }
            };
            let used = val
                .pointer("/rate_limit/primary_window/used_percent")
                .and_then(serde_json::Value::as_f64)?;
            let rem = (100.0 - used).clamp(0.0, 100.0).round() as i64;
            match val.get("plan_type").and_then(serde_json::Value::as_str) {
                Some(plan) if !plan.is_empty() => Some(format!("7d: {rem}% left ({plan})")),
                _ => Some(format!("7d: {rem}% left")),
            }
        }
        "google-antigravity" => {
            let tok_rel = ".gemini/antigravity-cli/antigravity-oauth-token";
            let tok_path = prof_dir.join(tok_rel);
            let tok_text = std::fs::read_to_string(&tok_path).ok()?;
            let mut tok_val: serde_json::Value = serde_json::from_str(&tok_text).ok()?;
            let token = tok_val
                .pointer("/token/access_token")
                .or_else(|| tok_val.get("access_token"))?
                .as_str()?
                .trim()
                .to_string();
            if token.is_empty() {
                return None;
            }
            let fetch_summary = |tok: &str| -> Option<serde_json::Value> {
                let resp = http
                    .post("https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal:retrieveUserQuotaSummary")
                    .header("authorization", &format!("Bearer {tok}"))
                    .header("content-type", "application/json")
                    .header("user-agent", "antigravity/3.0.0")
                    .send(b"{}")
                    .ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                serde_json::from_str(&body).ok()
            };
            let fetch_models = |tok: &str| -> Option<serde_json::Value> {
                let resp = http
                    .post("https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal:fetchAvailableModels")
                    .header("authorization", &format!("Bearer {tok}"))
                    .header("content-type", "application/json")
                    .header("user-agent", "antigravity")
                    .send(b"{}")
                    .ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                serde_json::from_str(&body).ok()
            };
            let mut active_token = token;
            let mut summary_val = fetch_summary(&active_token);
            if summary_val.is_none() {
                let rtok = tok_val
                    .pointer("/token/refresh_token")
                    .or_else(|| tok_val.get("refresh_token"))
                    .and_then(serde_json::Value::as_str)?
                    .trim()
                    .to_string();
                if rtok.is_empty() {
                    return None;
                }
                let (cid, csec) = extract_agy_oauth_client()?;
                let form_body = format!(
                    "client_id={cid}&client_secret={csec}&refresh_token={rtok}&grant_type=refresh_token"
                );
                let rresp = http
                    .post("https://oauth2.googleapis.com/token")
                    .header("content-type", "application/x-www-form-urlencoded")
                    .send(form_body.as_bytes())
                    .ok()?;
                let rbody = rresp.into_body().read_to_string().ok()?;
                let rval: serde_json::Value = serde_json::from_str(&rbody).ok()?;
                let new_acc = rval.get("access_token")?.as_str()?.trim().to_string();
                if let Some(tok_obj) = tok_val
                    .get_mut("token")
                    .and_then(serde_json::Value::as_object_mut)
                {
                    tok_obj.insert(
                        "access_token".to_string(),
                        serde_json::Value::String(new_acc.clone()),
                    );
                } else if let Some(root_obj) = tok_val.as_object_mut() {
                    root_obj.insert(
                        "access_token".to_string(),
                        serde_json::Value::String(new_acc.clone()),
                    );
                }
                if let Ok(updated) = serde_json::to_string_pretty(&tok_val) {
                    let _ = std::fs::write(&tok_path, &updated);
                    propagate_profile_files_to_bound_agents(data, provider, profile, &[tok_rel]);
                }
                active_token = new_acc;
                summary_val = fetch_summary(&active_token);
            }
            if let Some(ref sv) = summary_val
                && let Some(groups) = sv.get("groups").and_then(serde_json::Value::as_array)
            {
                let mut gem_5h = None;
                let mut gem_7d = None;
                let mut opus_5h = None;
                let mut opus_7d = None;
                for g in groups {
                    let Some(buckets) = g.get("buckets").and_then(serde_json::Value::as_array) else {
                        continue;
                    };
                    for b in buckets {
                        let bid = b.get("bucketId").and_then(serde_json::Value::as_str).unwrap_or("");
                        let disabled = b.get("disabled").and_then(serde_json::Value::as_bool).unwrap_or(false);
                        let frac = if disabled {
                            0.0
                        } else {
                            b.get("remainingFraction")
                                .and_then(serde_json::Value::as_f64)
                                .unwrap_or(0.0)
                        };
                        let pct = (frac * 100.0).clamp(0.0, 100.0).round() as i64;
                        match bid {
                            "gemini-5h" => gem_5h = Some(pct),
                            "gemini-weekly" => gem_7d = Some(pct),
                            "3p-5h" => opus_5h = Some(pct),
                            "3p-weekly" => opus_7d = Some(pct),
                            _ => {}
                        }
                    }
                }
                if let (Some(g5), Some(g7), Some(o5), Some(o7)) = (gem_5h, gem_7d, opus_5h, opus_7d) {
                    let gem = g5.min(g7);
                    let opus = o5.min(o7);
                    return Some(format!(
                        "gemini: {gem}% left (5h: {g5}%, 7d: {g7}%) · opus: {opus}% left (5h: {o5}%, 7d: {o7}%)"
                    ));
                }
            }
            let val = fetch_models(&active_token)?;
            let models = val.get("models")?.as_object()?;
            let frac_of = |names: &[&str]| -> Option<i64> {
                for n in names {
                    if let Some(m) = models.get(*n)
                        && let Some(qi) = m.get("quotaInfo")
                    {
                        let f = qi
                            .get("remainingFraction")
                            .and_then(serde_json::Value::as_f64)
                            .unwrap_or(0.0);
                        return Some((f * 100.0).clamp(0.0, 100.0).round() as i64);
                    }
                }
                None
            };
            let gem = frac_of(&[
                "gemini-4-argon-high",
                "gemini-3.1-pro-high",
                "gemini-2.5-pro",
                "gemini-3-flash",
            ])?;
            let opus = frac_of(&[
                "claude-opus-5-5-thinking",
                "claude-opus-5-5-high",
                "claude-opus-5-5-medium",
                "claude-opus-4-6-thinking",
            ])?;
            Some(format!("gemini: {gem}% left · opus: {opus}% left"))
        }
        "kimi-code-plan" => {
            let key = read_first_non_empty_file(
                &prof_dir,
                &[
                    ".config/opencode/kimi-code-plan.key",
                    ".config/opencode/kimi.key",
                ],
            )?;
            let resp = http
                .get("https://api.kimi.com/coding/v1/usages")
                .header("authorization", &format!("Bearer {key}"))
                .header("user-agent", "claude-code/1.0")
                .call()
                .ok()?;
            let body = resp.into_body().read_to_string().ok()?;
            let val: serde_json::Value = serde_json::from_str(&body).ok()?;
            let parse_num = |ptr: &str| -> Option<f64> {
                let v = val.pointer(ptr)?;
                v.as_f64().or_else(|| v.as_str()?.parse::<f64>().ok())
            };
            let pct_5h = if let Some(ratio) = parse_num("/usages/limit_5h/used_ratio") {
                ((1.0 - ratio) * 100.0).clamp(0.0, 100.0).round() as i64
            } else {
                let l5 = parse_num("/limits/0/detail/limit")?.max(1.0);
                let r5 = parse_num("/limits/0/detail/remaining").unwrap_or(0.0);
                (r5 / l5 * 100.0).clamp(0.0, 100.0).round() as i64
            };
            let pct_mo = if let Some(ratio) = parse_num("/usages/limit_month_total/used_ratio") {
                ((1.0 - ratio) * 100.0).clamp(0.0, 100.0).round() as i64
            } else {
                let lm = parse_num("/usage/limit")?.max(1.0);
                let rm = parse_num("/usage/remaining").unwrap_or(0.0);
                (rm / lm * 100.0).clamp(0.0, 100.0).round() as i64
            };
            Some(format!("5h: {pct_5h}% left · mo: {pct_mo}% left"))
        }
        "minimax-code-plan" => {
            let key = read_first_non_empty_file(
                &prof_dir,
                &[
                    ".config/opencode/minimax-code-plan.key",
                    ".config/opencode/minimax.key",
                ],
            )?;
            let resp = http
                .get("https://api.minimax.io/v1/api/openplatform/coding_plan/remains")
                .header("authorization", &format!("Bearer {key}"))
                .call()
                .ok()?;
            let body = resp.into_body().read_to_string().ok()?;
            let val: serde_json::Value = serde_json::from_str(&body).ok()?;
            let m0 = val.pointer("/model_remains/0")?;
            let tot_5h = m0
                .get("current_interval_total_count")
                .and_then(serde_json::Value::as_f64)?
                .max(1.0);
            let rem_5h = m0
                .get("current_interval_usage_count")
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(0.0);
            let tot_7d = m0
                .get("current_weekly_total_count")
                .and_then(serde_json::Value::as_f64)?
                .max(1.0);
            let rem_7d = m0
                .get("current_weekly_usage_count")
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(0.0);
            let pct_5h = (rem_5h / tot_5h * 100.0).clamp(0.0, 100.0).round() as i64;
            let pct_7d = (rem_7d / tot_7d * 100.0).clamp(0.0, 100.0).round() as i64;
            Some(format!("5h: {pct_5h}% left · 7d: {pct_7d}% left"))
        }
        "glm-code-plan" => {
            let key = read_first_non_empty_file(
                &prof_dir,
                &[
                    ".config/opencode/glm-code-plan.key",
                    ".config/opencode/glm.key",
                ],
            )?;
            let resp = http
                .get("https://api.z.ai/api/monitor/usage/quota/limit")
                .header("authorization", &key)
                .call()
                .ok()?;
            let body = resp.into_body().read_to_string().ok()?;
            let val: serde_json::Value = serde_json::from_str(&body).ok()?;
            let limits = val.pointer("/data/limits")?.as_array()?;
            let mut rem_5h = None;
            let mut rem_7d = None;
            for item in limits {
                if item.get("type").and_then(serde_json::Value::as_str) != Some("TOKENS_LIMIT") {
                    continue;
                }
                let unit = item.get("unit").and_then(serde_json::Value::as_i64);
                let pct = item
                    .get("percentage")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0);
                let rem = (100 - pct).clamp(0, 100);
                if unit == Some(3) {
                    rem_5h = Some(rem);
                } else if unit == Some(6) {
                    rem_7d = Some(rem);
                }
            }
            Some(format!(
                "5h: {}% left · 7d: {}% left",
                rem_5h?,
                rem_7d.unwrap_or(100)
            ))
        }
        "deepseek" => {
            let api_dir = data.join("profiles/deepseek/api");
            let key = read_first_non_empty_file(&prof_dir, &[".config/opencode/deepseek.key"])
                .or_else(|| read_dsh_yaml_ref(&prof_dir, "DEEPSEEK_API_KEY"))
                .or_else(|| read_first_non_empty_file(&api_dir, &[".config/opencode/deepseek.key"]))
                .or_else(|| read_dsh_yaml_ref(&api_dir, "DEEPSEEK_API_KEY"))?;
            let resp = http
                .get("https://api.deepseek.com/user/balance")
                .header("authorization", &format!("Bearer {key}"))
                .call()
                .ok()?;
            let body = resp.into_body().read_to_string().ok()?;
            let val: serde_json::Value = serde_json::from_str(&body).ok()?;
            let infos = val.get("balance_infos")?.as_array()?;
            let mut best: Option<(f64, String)> = None;
            for info in infos {
                let bal = info
                    .get("total_balance")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let cur = info
                    .get("currency")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("CNY")
                    .to_string();
                if best.as_ref().is_none_or(|(b, _)| bal > *b) {
                    best = Some((bal, cur));
                }
            }
            let (bal, cur) = best?;
            let sym = if cur == "CNY" { "¥" } else { "$" };
            Some(format!("{sym}{bal:.2} {cur}"))
        }
        "openrouter" => {
            let key = read_first_non_empty_file(&prof_dir, &[".config/opencode/openrouter.key"])
                .or_else(|| read_dsh_yaml_ref(&prof_dir, "OPENROUTER_API_KEY"))?;
            let resp = http
                .get("https://openrouter.ai/api/v1/credits")
                .header("authorization", &format!("Bearer {key}"))
                .call()
                .ok()?;
            let body = resp.into_body().read_to_string().ok()?;
            let val: serde_json::Value = serde_json::from_str(&body).ok()?;
            let total = val
                .pointer("/data/total_credits")
                .and_then(serde_json::Value::as_f64)?;
            let used = val
                .pointer("/data/total_usage")
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(0.0);
            let rem = (total - used).max(0.0);
            Some(format!("${rem:.2} left (${used:.0} / ${total:.0})"))
        }
        "cursor" => {
            let (mut access, refresh) = extract_cursor_oauth_from_db(&prof_dir)?;
            let fetch_summary = |tok: &str| -> Option<serde_json::Value> {
                let uid = extract_jwt_sub_tail(tok)?;
                let resp = http
                    .get("https://cursor.com/api/usage-summary")
                    .header("authorization", &format!("Bearer {tok}"))
                    .header("cookie", &format!("WorkosCursorSessionToken={uid}%3A%3A{tok}"))
                    .call()
                    .ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                serde_json::from_str(&body).ok()
            };
            let val = match fetch_summary(&access) {
                Some(v) => v,
                None if !refresh.is_empty() => {
                    let cursor_cid = concat!("KbZUR41c", "Y7W6zRSd", "pSUJ7I7m", "LYBKOCmB");
                    let req_body = serde_json::json!({
                        "grant_type": "refresh_token",
                        "client_id": cursor_cid,
                        "refresh_token": refresh,
                    })
                    .to_string();
                    let rresp = http
                        .post("https://api2.cursor.sh/oauth/token")
                        .header("content-type", "application/json")
                        .send(req_body.as_bytes())
                        .ok()?;
                    let rbody = rresp.into_body().read_to_string().ok()?;
                    let rval: serde_json::Value = serde_json::from_str(&rbody).ok()?;
                    access = rval.get("access_token")?.as_str()?.trim().to_string();
                    fetch_summary(&access)?
                }
                None => return None,
            };
            let plan = val.pointer("/individualUsage/plan")?;
            let limit_cents = plan
                .get("limit")
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(0.0);
            let rem_cents = plan
                .get("remaining")
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(0.0);
            let used_pct = plan
                .get("totalPercentUsed")
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(0.0);
            let rem_pct = (100.0 - used_pct).clamp(0.0, 100.0).round() as i64;
            let tier = val
                .get("membershipType")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("pro");
            if limit_cents > 0.0 {
                let rem_usd = (rem_cents / 100.0).round() as i64;
                let lim_usd = (limit_cents / 100.0).round() as i64;
                Some(format!("mo: {rem_pct}% left (${rem_usd}/${lim_usd} {tier})"))
            } else {
                Some(format!("mo: {rem_pct}% left ({tier})"))
            }
        }
        "muse" => {
            let auth_text = std::fs::read_to_string(prof_dir.join("muse/auth.json"))
                .or_else(|_| std::fs::read_to_string(prof_dir.join(".config/muse/auth.json")))
                .ok()?;
            let auth: serde_json::Value = serde_json::from_str(&auth_text).ok()?;
            let token = auth
                .pointer("/providers/meta/access_token")?
                .as_str()?
                .trim();
            if token.is_empty() {
                return None;
            }
            let resp = http
                .post("https://api.meta.ai/muse-code/key")
                .header("authorization", &format!("Bearer {token}"))
                .header("x-api-version", "1.0.0")
                .header("content-type", "application/json")
                .send(b"{}")
                .ok()?;
            let body = resp.into_body().read_to_string().ok()?;
            let val: serde_json::Value = serde_json::from_str(&body).ok()?;
            if val.get("is_subs_active").and_then(serde_json::Value::as_bool) != Some(true) {
                return None;
            }
            let tier = val
                .get("subs_tier_name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("active");
            let short_tier = tier.strip_prefix("Muse Code ").unwrap_or(tier);
            let used_pct = val
                .pointer("/subs_usage/window/used_percent")
                .or_else(|| val.pointer("/subs_usage/used_percent"))
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(0.0);
            let rem_pct = (100.0 - used_pct).clamp(0.0, 100.0).round() as i64;
            Some(format!("5h: {rem_pct}% left ({short_tier})"))
        }
        "xai" => {
            let auth_rel = ".grok/auth.json";
            let auth_path = prof_dir.join(auth_rel);
            let auth_text = std::fs::read_to_string(&auth_path).ok()?;
            let mut root_auth: serde_json::Value = serde_json::from_str(&auth_text).ok()?;
            let entry_key = if root_auth.get("key").is_some() {
                None
            } else {
                root_auth
                    .as_object()?
                    .iter()
                    .find(|(_, v)| v.get("key").is_some())
                    .map(|(k, _)| k.clone())
            };
            let auth_entry = match &entry_key {
                Some(k) => root_auth.get(k)?,
                None => &root_auth,
            };
            let key = auth_entry.get("key")?.as_str()?.trim().to_string();
            if key.is_empty() {
                return None;
            }
            let rtok = auth_entry
                .get("refresh_token")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
            let issuer = auth_entry
                .get("oidc_issuer")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("https://auth.x.ai")
                .trim_end_matches('/')
                .to_string();
            let cid = auth_entry
                .get("oidc_client_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
            let fetch_billing = |tok: &str| -> Option<serde_json::Value> {
                let resp = http
                    .get("https://cli-chat-proxy.grok.com/v1/billing?format=credits")
                    .header("authorization", &format!("Bearer {tok}"))
                    .header("x-xai-token-auth", "xai-grok-cli")
                    .call()
                    .ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                serde_json::from_str(&body).ok()
            };
            let val = match fetch_billing(&key) {
                Some(v) => v,
                None => {
                    if rtok.is_empty() || cid.is_empty() {
                        return None;
                    }
                    let form_body = format!(
                        "grant_type=refresh_token&refresh_token={rtok}&client_id={cid}"
                    );
                    let rresp = http
                        .post(&format!("{issuer}/oauth2/token"))
                        .header("content-type", "application/x-www-form-urlencoded")
                        .send(form_body.as_bytes())
                        .ok()?;
                    let rbody = rresp.into_body().read_to_string().ok()?;
                    let rval: serde_json::Value = serde_json::from_str(&rbody).ok()?;
                    let new_acc = rval.get("access_token")?.as_str()?.trim().to_string();
                    let target_obj = match &entry_key {
                        Some(k) => root_auth.get_mut(k).and_then(serde_json::Value::as_object_mut),
                        None => root_auth.as_object_mut(),
                    };
                    if let Some(obj) = target_obj {
                        obj.insert(
                            "key".to_string(),
                            serde_json::Value::String(new_acc.clone()),
                        );
                        if let Some(new_ref) = rval
                            .get("refresh_token")
                            .and_then(serde_json::Value::as_str)
                        {
                            obj.insert(
                                "refresh_token".to_string(),
                                serde_json::Value::String(new_ref.to_string()),
                            );
                        }
                        if let Ok(updated) = serde_json::to_string_pretty(&root_auth) {
                            let _ = std::fs::write(&auth_path, &updated);
                            propagate_profile_files_to_bound_agents(
                                data,
                                provider,
                                profile,
                                &[auth_rel],
                            );
                        }
                    }
                    fetch_billing(&new_acc)?
                }
            };
            let used = val
                .pointer("/config/creditUsagePercent")
                .or_else(|| val.get("creditUsagePercent"))
                .and_then(serde_json::Value::as_f64)?;
            let rem = (100.0 - used).clamp(0.0, 100.0).round() as i64;
            if rem == 0
                && let Some(end_str) = val
                    .pointer("/config/currentPeriod/end")
                    .or_else(|| val.pointer("/currentPeriod/end"))
                    .and_then(serde_json::Value::as_str)
                && let Some((date_part, time_part)) = end_str.split_once('T')
                && let (Some(mmdd), Some(hhmm)) = (date_part.get(5..10), time_part.get(..5))
            {
                Some(format!("7d: 0% left (resets {mmdd} {hhmm}Z)"))
            } else {
                Some(format!("7d: {rem}% left"))
            }
        }
        "qwen-token-plan" => {
            let key = read_first_non_empty_file(
                &prof_dir,
                &[
                    ".config/opencode/qwen-coding-plan.key",
                    ".config/opencode/qwen.key",
                ],
            )?;
            let resp = http
                .get("https://token-plan.maas.qwencloudapi.com/compatible-mode/v1/models")
                .header("authorization", &format!("Bearer {key}"))
                .call()
                .ok()?;
            let body = resp.into_body().read_to_string().ok()?;
            let val: serde_json::Value = serde_json::from_str(&body).ok()?;
            let count = val.get("data")?.as_array()?.len();
            Some(format!("active ({count} models)"))
        }
        "sylphx" => {
            let _key = read_first_non_empty_file(
                &prof_dir,
                &[
                    ".config/opencode/sylphx.key",
                    ".config/desk/sylphx-ai/key",
                ],
            )?;
            let resp = http.get("https://api.models.sylphx.ai/health").call().ok()?;
            let body = resp.into_body().read_to_string().ok()?;
            let val: serde_json::Value = serde_json::from_str(&body).ok()?;
            if val.get("status").and_then(serde_json::Value::as_str) == Some("ok") {
                Some("online (gateway-edge)".to_string())
            } else {
                None
            }
        }
        "google-gemini" => {
            let key = read_first_non_empty_file(
                &prof_dir,
                &["gemini-api-key", ".config/opencode/gemini.key"],
            )
            .or_else(|| read_dsh_yaml_ref(&prof_dir, "GEMINI_API_KEY"))?;
            let resp = http
                .get(&format!(
                    "https://generativelanguage.googleapis.com/v1beta/models?pageSize=1&key={key}"
                ))
                .call()
                .ok()?;
            let body = resp.into_body().read_to_string().ok()?;
            let val: serde_json::Value = serde_json::from_str(&body).ok()?;
            if val.get("models").is_some() {
                Some("active (api-key)".to_string())
            } else {
                None
            }
        }
        _ => None,
    }
}

pub(crate) fn parse_remaining_pct_from_usage(usage: &str, model: Option<&str>) -> Option<i64> {
    let extract_after = |label: &str| -> Option<i64> {
        let idx = usage.find(label)?;
        let rest = &usage[idx + label.len()..];
        let pct_idx = rest.find("% left")?;
        rest[..pct_idx].trim().parse::<i64>().ok()
    };
    if usage.contains("gemini:") && usage.contains("opus:") {
        let wants_opus = model
            .map(|m| {
                let lower = m.to_ascii_lowercase();
                lower.contains("opus") || lower.contains("claude")
            })
            .unwrap_or(false);
        if wants_opus {
            return extract_after("opus:");
        }
        return extract_after("gemini:");
    }
    for prefix in ["5h:", "7d:", "mo:"] {
        if let Some(p) = extract_after(prefix) {
            return Some(p);
        }
    }
    None
}

pub(crate) fn probe_profile_remaining_pct(
    data: &Path,
    provider: &str,
    profile: &str,
    model: Option<&str>,
) -> Option<i64> {
    let usage = probe_one_profile_usage(data, provider, profile)?;
    parse_remaining_pct_from_usage(&usage, model)
}

pub(crate) fn poke_agent_wake_socket(data: &Path, agent_id: &AgentId) {
    #[cfg(unix)]
    {
        use std::os::unix::net::UnixDatagram;
        let sock_path = data.join("wake.sock");
        if sock_path.exists()
            && let Ok(sock) = UnixDatagram::unbound()
        {
            let msg = format!("a{}", agent_id.as_str());
            let _ = sock.send_to(msg.as_bytes(), &sock_path);
        }
    }
}

pub(crate) fn force_refresh_provider_profiles(data: &Path, target: Option<&str>) {
    let filter = target
        .map(str::trim)
        .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("refresh") && !s.eq_ignore_ascii_case("reset"));
    if let Ok(agents_dir) = std::fs::read_dir(data.join("agents")) {
        for entry in agents_dir.flatten() {
            let _ = std::fs::remove_file(
                entry
                    .path()
                    .join("config")
                    .join(".anyagents-last-quota-check-ms"),
            );
        }
    }
    let http = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(6)))
        .build()
        .new_agent();
    let profiles_root = data.join("profiles");
    let Ok(providers) = std::fs::read_dir(&profiles_root) else {
        return;
    };
    for prov_entry in providers.flatten() {
        let prov_path = prov_entry.path();
        if !prov_path.is_dir() {
            continue;
        }
        let provider = prov_entry.file_name().to_string_lossy().into_owned();
        let Ok(prof_dirs) = std::fs::read_dir(&prov_path) else {
            continue;
        };
        for prof_entry in prof_dirs.flatten() {
            let prof_dir = prof_entry.path();
            if !prof_dir.is_dir() {
                continue;
            }
            let profile = prof_entry.file_name().to_string_lossy().into_owned();
            let full_name = format!("{provider}/{profile}");
            if let Some(f) = filter
                && f != provider
                && f != profile
                && f != full_name
            {
                continue;
            }
            match provider.as_str() {
                "anthropic" => {
                    let Ok(cred_text) = std::fs::read_to_string(prof_dir.join(".credentials.json"))
                        .or_else(|_| {
                            std::fs::read_to_string(prof_dir.join(".claude/.credentials.json"))
                        })
                    else {
                        continue;
                    };
                    let Ok(mut cred) = serde_json::from_str::<serde_json::Value>(&cred_text) else {
                        continue;
                    };
                    let mut tok = cred
                        .pointer("/claudeAiOauth/accessToken")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    let rtok = cred
                        .pointer("/claudeAiOauth/refreshToken")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    if !rtok.is_empty() {
                        let req_body = serde_json::json!({
                            "grant_type": "refresh_token",
                            "refresh_token": rtok,
                            "client_id": "9d1c250a-e61b-44d9-88ed-5944d1962f5e",
                        })
                        .to_string();
                        if let Ok(rresp) = http
                            .post("https://platform.claude.com/v1/oauth/token")
                            .header("content-type", "application/json")
                            .header("user-agent", "claude-code/2.1.310")
                            .send(req_body.as_bytes())
                            && let Ok(rbody) = rresp.into_body().read_to_string()
                            && let Ok(rval) = serde_json::from_str::<serde_json::Value>(&rbody)
                            && let Some(new_acc) = rval
                                .get("access_token")
                                .and_then(serde_json::Value::as_str)
                                .map(|s| s.trim().to_string())
                            && !new_acc.is_empty()
                        {
                            tok = new_acc.clone();
                            if let Some(oauth) = cred
                                .get_mut("claudeAiOauth")
                                .and_then(serde_json::Value::as_object_mut)
                            {
                                oauth.insert(
                                    "accessToken".to_string(),
                                    serde_json::Value::String(new_acc),
                                );
                                if let Some(new_ref) = rval
                                    .get("refresh_token")
                                    .and_then(serde_json::Value::as_str)
                                {
                                    oauth.insert(
                                        "refreshToken".to_string(),
                                        serde_json::Value::String(new_ref.to_string()),
                                    );
                                }
                                if let Some(exp_in) =
                                    rval.get("expires_in").and_then(serde_json::Value::as_i64)
                                {
                                    let now_ms = anyagents_core::now_ms();
                                    oauth.insert(
                                        "expiresAt".to_string(),
                                        serde_json::Value::Number((now_ms + exp_in * 1000).into()),
                                    );
                                }
                            }
                        }
                    }
                    if !tok.is_empty() {
                        let _ = sync_anthropic_profile_metadata(
                            &http, data, &prof_dir, &profile, &tok, &mut cred,
                        );
                        if let Ok(updated) = serde_json::to_string(&cred) {
                            let _ = std::fs::create_dir_all(prof_dir.join(".claude"));
                            let _ = std::fs::write(prof_dir.join(".credentials.json"), &updated);
                            let _ = std::fs::write(
                                prof_dir.join(".claude/.credentials.json"),
                                &updated,
                            );
                            propagate_profile_files_to_bound_agents(
                                data,
                                "anthropic",
                                &profile,
                                &[".credentials.json", ".claude/.credentials.json"],
                            );
                        }
                    }
                }
                "openai" => {
                    let Ok(auth_text) = std::fs::read_to_string(prof_dir.join("auth.json"))
                        .or_else(|_| std::fs::read_to_string(prof_dir.join(".codex/auth.json")))
                    else {
                        continue;
                    };
                    let Ok(mut auth) = serde_json::from_str::<serde_json::Value>(&auth_text) else {
                        continue;
                    };
                    let rtok = auth
                        .pointer("/tokens/refresh_token")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    if !rtok.is_empty() {
                        let req_body = serde_json::json!({
                            "client_id": "app_EMoamEEZ73f0CkXaXp7hrann",
                            "grant_type": "refresh_token",
                            "refresh_token": rtok,
                        })
                        .to_string();
                        if let Ok(rresp) = http
                            .post("https://auth.openai.com/oauth/token")
                            .header("content-type", "application/json")
                            .send(req_body.as_bytes())
                            && let Ok(rbody) = rresp.into_body().read_to_string()
                            && let Ok(rval) = serde_json::from_str::<serde_json::Value>(&rbody)
                            && let Some(new_acc) = rval
                                .get("access_token")
                                .and_then(serde_json::Value::as_str)
                                .map(|s| s.trim().to_string())
                            && !new_acc.is_empty()
                            && let Some(tokens_obj) = auth
                                .get_mut("tokens")
                                .and_then(serde_json::Value::as_object_mut)
                        {
                            tokens_obj.insert(
                                "access_token".to_string(),
                                serde_json::Value::String(new_acc),
                            );
                            if let Some(new_ref) = rval
                                .get("refresh_token")
                                .and_then(serde_json::Value::as_str)
                            {
                                tokens_obj.insert(
                                    "refresh_token".to_string(),
                                    serde_json::Value::String(new_ref.to_string()),
                                );
                            }
                            if let Ok(updated) = serde_json::to_string_pretty(&auth) {
                                let _ = std::fs::create_dir_all(prof_dir.join(".codex"));
                                let _ = std::fs::write(prof_dir.join("auth.json"), &updated);
                                let _ = std::fs::write(prof_dir.join(".codex/auth.json"), &updated);
                                propagate_profile_files_to_bound_agents(
                                    data,
                                    "openai",
                                    &profile,
                                    &["auth.json", ".codex/auth.json"],
                                );
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

/// Clears historical `CORTEX_STEP_TYPE_ERROR_MESSAGE` (`step_type = 17`) rows
/// in `<config_dir>/.gemini/antigravity-cli/conversations/*.db` so `agy` does not
/// re-emit stale `"status":"ERROR","error":"...Resets in..."` result lines at the
/// end of subsequent healthy turns via `StepIndicesByType(17)`.
pub(crate) fn clear_stale_agy_quota_error_steps(config_dir: &Path) {
    let conv_dir = config_dir
        .join(".gemini")
        .join("antigravity-cli")
        .join("conversations");
    if !conv_dir.is_dir() {
        return;
    }
    #[cfg(unix)]
    if !crate::sandbox::is_isolated_test_env()
        && Path::new("/usr/local/bin/anyagents-setpriv").is_file()
        && let Ok(conv_meta) = std::fs::metadata(&conv_dir)
    {
        use std::os::unix::fs::MetadataExt as _;
        let (my_uid, my_gid) = std::fs::metadata("/proc/self")
            .map(|m| (m.uid(), m.gid()))
            .unwrap_or((conv_meta.uid(), conv_meta.gid()));
        if conv_meta.uid() != my_uid {
            let sock_gid = config_dir
                .parent()
                .and_then(|p| p.parent())
                .and_then(|p| std::fs::metadata(p).ok())
                .map(|m| m.gid())
                .unwrap_or(my_gid);
            let uid_s = conv_meta.uid().to_string();
            let gid_s = conv_meta.gid().to_string();
            let groups_s = format!("{},{},{}", conv_meta.gid(), sock_gid, my_gid);
            let _ = std::process::Command::new("/usr/local/bin/anyagents-setpriv")
                .args([
                    "--reuid",
                    &uid_s,
                    "--regid",
                    &gid_s,
                    "--groups",
                    &groups_s,
                    "--no-new-privs",
                    "--",
                    "python3",
                    "-c",
                    "import glob, os, sqlite3, sys\nfor p in glob.glob(os.path.join(sys.argv[1], '*.db')):\n    try:\n        c = sqlite3.connect(p, timeout=0.5)\n        if c.execute('SELECT 1 FROM steps WHERE step_type = 17 LIMIT 1').fetchone():\n            c.execute('UPDATE steps SET step_type = 0, status = 5, error_details = NULL, step_payload = ? WHERE step_type = 17', (b' \\x05*\\x03\\xd2\\x01\\x00',))\n            c.commit()\n        c.close()\n    except Exception:\n        pass\n",
                ])
                .arg(&conv_dir)
                .stderr(std::process::Stdio::null())
                .status();
            return;
        }
    }
    let Ok(entries) = std::fs::read_dir(&conv_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("db") {
            continue;
        }
        if let Ok(conn) = rusqlite::Connection::open(&path) {
            let _ = conn.busy_timeout(std::time::Duration::from_millis(500));
            let _ = conn.execute(
                "UPDATE steps SET step_type = 0, status = 5, error_details = NULL, step_payload = X'20052a03d20100' WHERE step_type = 17",
                [],
            );
        }
    }
}

fn restart_agent_harness_process(store: &Store, agent: &Agent) {
    #[cfg(unix)]
    {
        let parent_name = agent
            .parent_id
            .as_ref()
            .and_then(|pid| store.agent(pid).ok())
            .map(|p| p.name);
        if agent.state == "running"
            && !crate::sandbox::is_isolated_test_env()
            && Path::new("/usr/local/bin/anyagents-setpriv").is_file()
            && let Some((_, uid, gid, _)) =
                crate::sandbox::seat_for_agent_with_hint(&agent.name, parent_name.as_deref())
        {
            let uid_s = uid.to_string();
            let gid_s = gid.to_string();
            let proc_pat = match agent.harness.as_str() {
                "claude-code" => "claude",
                "agy" => "agy",
                "codex" => "codex",
                _ => "",
            };
            if !proc_pat.is_empty() {
                let _ = std::process::Command::new("/usr/local/bin/anyagents-setpriv")
                    .args([
                        "--reuid",
                        &uid_s,
                        "--regid",
                        &gid_s,
                        "--groups",
                        &gid_s,
                        "--no-new-privs",
                        "--",
                        "/usr/bin/pkill",
                        "-TERM",
                        "-u",
                        &uid_s,
                        "-x",
                        proc_pat,
                    ])
                    .stderr(std::process::Stdio::null())
                    .status();
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (store, agent);
    }
}

pub(crate) fn rotate_agent_to_profile(
    store: &Store,
    data: &Path,
    agent: &Agent,
    provider: &str,
    new_profile: &str,
) {
    let cfg = agent_config_dir(data, &agent.id);
    let normalized = format!("{provider}/{new_profile}\n");
    write_profile_marker(&cfg, ".anyagents-profile", &normalized);
    write_profile_marker(
        &cfg,
        ".anyagents-profile-rotated-ms",
        &format!("{}\n", anyagents_core::now_ms()),
    );
    seed_harness_credentials(store, data, agent, &cfg);
    let parent_name = agent
        .parent_id
        .as_ref()
        .and_then(|pid| store.agent(pid).ok())
        .map(|p| p.name);
    if let Some((_, _, gid, _)) =
        crate::sandbox::seat_for_agent_with_hint(&agent.name, parent_name.as_deref())
    {
        crate::sandbox::share_config_with_seat(&cfg, gid);
    }
    restart_agent_harness_process(store, agent);
    if agent.harness == "agy" {
        clear_stale_agy_quota_error_steps(&cfg);
    }
    poke_agent_wake_socket(data, &agent.id);
}

/// Checks if `agent`'s currently bound profile has recovered quota (`> 5% left`
/// after a plan upgrade or vendor mid-window reset) OR is exhausted (`0% left`)
/// while another profile under the same upstream provider has healthy quota (`>= 20%`).
/// Rate-limited by `.anyagents-last-quota-check-ms` (120s cooldown) when called
/// during status reconciliation so background checks never spam vendor APIs.
pub(crate) fn try_rotate_exhausted_profile(
    store: &Store,
    data: &Path,
    agent: &Agent,
    has_active_quota_hold: bool,
) -> Option<String> {
    if crate::sandbox::is_isolated_test_env() {
        return None;
    }
    let cfg = agent_config_dir(data, &agent.id);
    let now_ms = anyagents_core::now_ms();
    let provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
    let current_prof = bound_profile_name(&cfg)
        .and_then(|b| {
            let p = b.split_once('/').map(|(_, p)| p.to_string()).unwrap_or(b);
            (!p.is_empty()).then_some(p)
        })
        .unwrap_or_else(|| "default".to_string());
    if has_active_quota_hold {
        let last_check = std::fs::read_to_string(cfg.join(".anyagents-last-quota-check-ms"))
            .ok()
            .and_then(|s| s.trim().parse::<i64>().ok())
            .unwrap_or(0);
        if now_ms.saturating_sub(last_check) < 120_000 {
            let rotated_ms = std::fs::read_to_string(cfg.join(".anyagents-profile-rotated-ms"))
                .ok()
                .and_then(|s| s.trim().parse::<i64>().ok())
                .unwrap_or(0);
            if rotated_ms >= last_check && now_ms.saturating_sub(rotated_ms) < 120_000 {
                return Some(format!("{provider}/{current_prof}"));
            }
            return None;
        }
        write_profile_marker(
            &cfg,
            ".anyagents-last-quota-check-ms",
            &format!("{now_ms}\n"),
        );
    }
    let cur_pct = probe_profile_remaining_pct(data, &provider, &current_prof, agent.model.as_deref());
    if has_active_quota_hold && cur_pct.is_some_and(|p| p > 5) {
        write_profile_marker(
            &cfg,
            ".anyagents-profile-rotated-ms",
            &format!("{now_ms}\n"),
        );
        if !store.has_open_turn(&agent.id).unwrap_or(false) {
            restart_agent_harness_process(store, agent);
        }
        if agent.harness == "agy" {
            clear_stale_agy_quota_error_steps(&cfg);
        }
        poke_agent_wake_socket(data, &agent.id);
        return Some(format!("{provider}/{current_prof}"));
    }
    let available = list_harness_profiles(data, &provider);
    if available.len() <= 1 {
        return None;
    }
    let is_exhausted = cur_pct == Some(0) || (has_active_quota_hold && cur_pct.is_none_or(|p| p <= 5));
    if !is_exhausted {
        return None;
    }
    let all_agents = store.list_agents().unwrap_or_default();
    let mut counts: std::collections::BTreeMap<String, usize> = available
        .iter()
        .map(|(name, _)| (name.clone(), 0usize))
        .collect();
    for other in &all_agents {
        if other.id != agent.id
            && other.state != "archived"
            && upstream_provider_of(&other.harness, other.model.as_deref()) == provider
        {
            let other_cfg = agent_config_dir(data, &other.id);
            if let Some(p) = bound_profile_name(&other_cfg) {
                let prof_name = p.split_once('/').map(|(_, n)| n).unwrap_or(p.as_str());
                if let Some(c) = counts.get_mut(prof_name) {
                    *c += 1;
                }
            }
        }
    }
    let best_alt = available
        .iter()
        .filter(|(name, _)| name != &current_prof)
        .filter_map(|(name, _)| {
            let pct = probe_profile_remaining_pct(data, &provider, name, agent.model.as_deref())?;
            (pct >= 20).then_some((
                name.clone(),
                counts.get(name).copied().unwrap_or(0),
                pct,
            ))
        })
        .min_by_key(|(_, assigned, pct)| (*assigned, std::cmp::Reverse(*pct)))
        .map(|(name, _, _)| name)?;
    rotate_agent_to_profile(store, data, agent, &provider, &best_alt);
    Some(format!("{provider}/{best_alt}"))
}

/// Lists all available Master Profiles for `provider_or_harness` under `<data>/profiles/<provider>/<profile>`.
pub(crate) fn list_harness_profiles(
    data: &Path,
    provider_or_harness: &str,
) -> Vec<(String, PathBuf)> {
    let provider = match provider_or_harness {
        "claude-code" | "codex" | "agy" | "muse" | "dsh" | "grok" | "opencode" | "omp" => {
            upstream_provider_of(provider_or_harness, None)
        }
        other => other.to_string(),
    };
    let mut out: Vec<(String, PathBuf)> = Vec::new();
    for base in [
        data.join("profiles").join(&provider),
        data.join("profiles").join(provider_or_harness),
        data.join("config").join(&provider),
        data.join("config").join(provider_or_harness),
    ] {
        let Ok(entries) = std::fs::read_dir(&base) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') {
                continue;
            }
            if !out.iter().any(|(n, _)| n == &name) {
                out.push((name, path));
            }
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// Returns the profile marker name bound to an agent's config directory, if any.
pub(crate) fn bound_profile_name(config_dir: &Path) -> Option<String> {
    std::fs::read_to_string(config_dir.join(".anyagents-profile"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Selects (or reuses the already-bound) Upstream Provider Profile for `agent`
/// from `<data>/profiles/<provider>/<profile>` using quota-aware least-loaded
/// assignment across agents sharing that upstream provider, and persists
/// `.anyagents-profile` (`<provider>/<profile>`) in `target_config`.
fn select_or_bind_profile(
    data: &Path,
    all_agents: &[Agent],
    agent: &Agent,
    target_config: &Path,
) -> Vec<PathBuf> {
    let provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
    let available = list_harness_profiles(data, &provider);
    let mut roots = Vec::new();
    if let Some(existing) = bound_profile_name(target_config) {
        let prof_name = existing
            .split_once('/')
            .map(|(_, p)| p)
            .unwrap_or(existing.as_str());
        if let Some((_, dir)) = available.iter().find(|(n, _)| n == prof_name) {
            roots.push(dir.clone());
        } else {
            roots.push(data.join("profiles").join(&provider).join(prof_name));
        }
        if prof_name != "default" {
            return roots;
        }
    } else if !available.is_empty() {
        let mut counts: std::collections::BTreeMap<String, usize> = available
            .iter()
            .map(|(name, _)| (name.clone(), 0usize))
            .collect();
        for other in all_agents {
            if other.id != agent.id
                && other.state != "archived"
                && upstream_provider_of(&other.harness, other.model.as_deref()) == provider
            {
                let other_cfg = agent_config_dir(data, &other.id);
                if let Some(p) = bound_profile_name(&other_cfg) {
                    let prof_name = p.split_once('/').map(|(_, n)| n).unwrap_or(p.as_str());
                    if let Some(c) = counts.get_mut(prof_name) {
                        *c += 1;
                    }
                }
            }
        }
        let probe_enabled = !crate::sandbox::is_isolated_test_env() && available.len() > 1;
        let scored: Vec<(&(String, PathBuf), bool, bool, usize, i64)> = available
            .iter()
            .map(|item| {
                let pct = if probe_enabled {
                    probe_profile_remaining_pct(data, &provider, &item.0, agent.model.as_deref())
                        .unwrap_or(100)
                } else {
                    100
                };
                let assigned = counts.get(&item.0).copied().unwrap_or(0);
                (item, pct > 0, pct >= 20, assigned, pct)
            })
            .collect();
        let best_tier = scored
            .iter()
            .map(|(_, has_quota, healthy, assigned, _)| (*has_quota, *healthy, std::cmp::Reverse(*assigned)))
            .max()
            .unwrap_or((true, true, std::cmp::Reverse(0)));
        let candidates: Vec<&(String, PathBuf)> = scored
            .iter()
            .filter(|(_, has_quota, healthy, assigned, _)| {
                (*has_quota, *healthy, std::cmp::Reverse(*assigned)) == best_tier
            })
            .map(|(item, _, _, _, _)| *item)
            .collect();
        let hash = agent.name.bytes().fold(0usize, |acc, b| {
            acc.wrapping_mul(31).wrapping_add(b as usize)
        });
        let chosen = candidates[hash % candidates.len()];
        write_profile_marker(
            target_config,
            ".anyagents-profile",
            &format!("{provider}/{}\n", chosen.0),
        );
        roots.push(chosen.1.clone());
    } else {
        write_profile_marker(
            target_config,
            ".anyagents-profile",
            &format!("{provider}/default\n"),
        );
    }
    roots.push(data.join("profiles").join(&provider).join("default"));
    roots.push(data.join("profiles").join(&agent.harness).join("default"));
    roots.push(data.join("config").join(&provider).join("default"));
    roots
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct SubagentDefaultSpec {
    /// `"inherit"` or `"preset"`.
    pub mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct SubagentDefaultsConfig {
    pub default: SubagentDefaultSpec,
    #[serde(default)]
    pub agents: std::collections::BTreeMap<String, SubagentDefaultSpec>,
}

impl Default for SubagentDefaultsConfig {
    fn default() -> Self {
        Self {
            default: SubagentDefaultSpec {
                mode: "inherit".to_string(),
                harness: None,
                model: None,
                profile: None,
            },
            agents: std::collections::BTreeMap::new(),
        }
    }
}

pub(crate) fn load_subagent_defaults(data: &Path) -> SubagentDefaultsConfig {
    let path = data.join("subagent-defaults.json");
    std::fs::read_to_string(&path)
        .ok()
        .and_then(|text| serde_json::from_str::<SubagentDefaultsConfig>(&text).ok())
        .unwrap_or_default()
}

pub(crate) fn save_subagent_defaults(
    data: &Path,
    cfg: &SubagentDefaultsConfig,
) -> Result<(), Error> {
    let path = data.join("subagent-defaults.json");
    let text = serde_json::to_string_pretty(cfg).map_err(|err| Error::Message(err.to_string()))?;
    std::fs::write(path, format!("{text}\n"))?;
    Ok(())
}

/// Resolve `(harness, model, profile)` for a newly created agent or sub-agent.
pub(crate) fn resolve_subagent_defaults(
    data: &Path,
    parent: Option<&Agent>,
    raw_harness: &str,
    raw_model: Option<String>,
    raw_profile: Option<String>,
) -> (String, Option<String>, Option<String>) {
    let h_trim = raw_harness.trim();
    let m_trim = raw_model
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let p_trim = raw_profile
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let Some(parent_agent) = parent else {
        let harness = if h_trim.is_empty() || h_trim.eq_ignore_ascii_case("inherit") {
            "claude-code".to_string()
        } else {
            h_trim.to_string()
        };
        let model = m_trim
            .filter(|m| !m.eq_ignore_ascii_case("inherit"))
            .map(str::to_string);
        let profile = p_trim
            .filter(|p| !p.eq_ignore_ascii_case("inherit"))
            .map(str::to_string);
        return (harness, model, profile);
    };

    let cfg = load_subagent_defaults(data);
    let spec = cfg.agents.get(&parent_agent.name).unwrap_or(&cfg.default);
    let parent_cfg_dir = agent_config_dir(data, &parent_agent.id);
    let parent_bound_profile = bound_profile_name(&parent_cfg_dir);

    let inherit_harness = || {
        if parent_agent.harness == "agy" {
            "claude-code".to_string()
        } else {
            parent_agent.harness.clone()
        }
    };

    let explicit_inherit = h_trim.eq_ignore_ascii_case("inherit")
        || m_trim.is_some_and(|m| m.eq_ignore_ascii_case("inherit"))
        || p_trim.is_some_and(|p| p.eq_ignore_ascii_case("inherit"));

    let use_inherit = explicit_inherit || spec.mode.eq_ignore_ascii_case("inherit");

    let resolved_harness = if !h_trim.is_empty() && !h_trim.eq_ignore_ascii_case("inherit") {
        h_trim.to_string()
    } else if h_trim.eq_ignore_ascii_case("inherit") || use_inherit {
        inherit_harness()
    } else {
        spec.harness
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("inherit"))
            .map(str::to_string)
            .unwrap_or_else(inherit_harness)
    };

    let resolved_model = if let Some(m) = m_trim {
        if m.eq_ignore_ascii_case("inherit") {
            parent_agent.model.clone()
        } else {
            Some(m.to_string())
        }
    } else if use_inherit {
        if resolved_harness == parent_agent.harness {
            parent_agent.model.clone()
        } else {
            None
        }
    } else if h_trim.is_empty() || spec.harness.as_deref() == Some(resolved_harness.as_str()) {
        spec.model
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .and_then(|m| {
                if m.eq_ignore_ascii_case("inherit") {
                    parent_agent.model.clone()
                } else {
                    Some(m.to_string())
                }
            })
    } else {
        None
    };

    let target_provider = upstream_provider_of(&resolved_harness, resolved_model.as_deref());
    let inherit_profile_if_compatible = || {
        parent_bound_profile.as_ref().and_then(|prof| {
            let prov = prof
                .split_once('/')
                .map(|(p, _)| p)
                .unwrap_or(prof.as_str());
            (prov == target_provider).then(|| prof.clone())
        })
    };

    let resolved_profile = if let Some(p) = p_trim {
        if p.eq_ignore_ascii_case("inherit") {
            inherit_profile_if_compatible()
        } else {
            Some(p.to_string())
        }
    } else if use_inherit {
        inherit_profile_if_compatible()
    } else {
        spec.profile
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .and_then(|p| {
                if p.eq_ignore_ascii_case("inherit") {
                    inherit_profile_if_compatible()
                } else {
                    let prov = p
                        .split_once('/')
                        .map(|(pr, _)| pr)
                        .unwrap_or(&target_provider);
                    (prov == target_provider).then(|| p.to_string())
                }
            })
            .or_else(inherit_profile_if_compatible)
    };

    (resolved_harness, resolved_model, resolved_profile)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_remaining_pct_from_usage_handles_agy_5h_and_7d_windows() {
        let usage = "gemini: 84% left (5h: 100%, 7d: 84%) · opus: 0% left (5h: 0%, 7d: 0%)";
        assert_eq!(
            parse_remaining_pct_from_usage(usage, Some("gemini-4-argon-high")),
            Some(84)
        );
        assert_eq!(
            parse_remaining_pct_from_usage(usage, Some("claude-opus-5-5")),
            Some(0)
        );
    }

    #[test]
    fn clear_stale_agy_quota_error_steps_resets_step_type_17_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let conv_dir = tmp
            .path()
            .join(".gemini")
            .join("antigravity-cli")
            .join("conversations");
        std::fs::create_dir_all(&conv_dir).unwrap();
        let db_path = conv_dir.join("test-conv.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute(
            "CREATE TABLE steps (idx integer PRIMARY KEY, step_type integer NOT NULL DEFAULT 0, status integer NOT NULL DEFAULT 0, error_details blob, step_payload blob)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO steps (idx, step_type, status, error_details, step_payload) VALUES (10, 17, 5, X'0102', X'08112005')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO steps (idx, step_type, status, error_details, step_payload) VALUES (11, 15, 5, NULL, X'080f2005')",
            [],
        )
        .unwrap();
        drop(conn);

        clear_stale_agy_quota_error_steps(tmp.path());

        let conn = rusqlite::Connection::open(&db_path).unwrap();
        let (st, status, ed, sp): (i64, i64, Option<Vec<u8>>, Vec<u8>) = conn
            .query_row(
                "SELECT step_type, status, error_details, step_payload FROM steps WHERE idx = 10",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(st, 0);
        assert_eq!(status, 5);
        assert!(ed.is_none());
        assert_eq!(sp, b" \x05*\x03\xd2\x01\x00");
    }
}
