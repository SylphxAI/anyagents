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
        let _ = std::fs::write(config.join(".anyagents-profile"), format!("{normalized}\n"));
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
        mcp_config::write_claude(&config.join(".claude.json"), &command, &args)?;
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
         - **Unified Cross-Harness Sub-Agents (`subagents` / `spawn` MCP Tools — Native Harness Sub-Agents Disabled by Default)**:\n\
           - Built-in harness sub-agent tools (`Task`/`Agent`/`task`/`invoke_subagent`) are **disabled by default** so sub-agents are never trapped inside a single vendor CLI.\n\
           - Always spawn and manage sub-agents via **AnyAgents `subagents` / `spawn` MCP tools** (or `anyagents spawn` CLI). When `harness`/`model`/`profile` are omitted, AnyAgents automatically uses the configured sub-agent default (`opencode` + `sylphx/openai/gpt-6.1-sol#medium` + `sylphx/default`, or pass `harness: \"inherit\"` to inherit your own harness/model/profile).\n\
           - Full lifecycle actions on `subagents`:\n\
             - **List**: `subagents({ \"action\": \"list\" })` (optional `state`: `\"running\"` | `\"paused\"` | `\"archived\"` | `\"all\"`, `parent`: `\"self\"` | `\"all\"`).\n\
             - **Inspect**: `subagents({ \"action\": \"inspect\", \"agent\": \"<name-or-id>\", \"limit\": 10 })`.\n\
             - **Spawn**: `subagents({ \"action\": \"spawn\", \"agent\": \"<name>\", \"workdir\": \"/scratch/wt/...\", \"description\": \"...\", \"prompt\": \"...\" })` (or `spawn({ ... })`).\n\
             - **Steer**: `subagents({ \"action\": \"steer\", \"agent\": \"<name-or-id>\", \"prompt\": \"...\" })`.\n\
             - **Control (`wake`, `pause`, `archive`, `delete`)**: `subagents({ \"action\": \"wake\" | \"pause\" | \"archive\" | \"delete\", \"agent\": \"<name-or-id>\" })`.\n\
         - **Native SOTA Anti-Detect Browser & Persistent Browser Profiles (`browser` MCP Tool & `anyagents browser` CLI)**:\n\
           - Every harness has native browser automation via the `browser` MCP tool (`anyagents browser` CLI), running real headed Chromium on a virtual X11 display (`xvfb-headed`) with deterministic per-profile anti-detect fingerprinting (WebGL vendor/renderer, Client Hints `Sec-CH-UA`, `navigator.webdriver = false`, WebRTC UDP leak protection, Canvas seed) and persistent `--user-data-dir` login profiles.\n\
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
        for (rel, shared_candidates) in [
            (
                ".rustup",
                ["/home/kyle/.rustup", "/scratch/rustup-home"],
            ),
            (
                ".cargo",
                ["/home/kyle/.cargo", "/scratch/cargo-registry"],
            ),
        ] {
            let dst = target_config.join(rel);
            if !dst.exists() {
                let found_shared = shared_candidates
                    .iter()
                    .map(Path::new)
                    .find(|p| p.is_dir())
                    .map(Path::to_path_buf)
                    .or_else(|| {
                        let h = host_home.join(rel);
                        h.is_dir().then_some(h)
                    });
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
    {
        if std::os::unix::fs::symlink(&skills_root, data_sk).is_ok() {
            skills_root = data_sk.clone();
        }
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
                "--",
                "sh",
                "-c",
                "mkdir -p \"$1/.config/op\" && chmod 0700 \"$1/.config/op\" && ln -sfn \"$2\" \"$1/.config/op/service-account-token\"",
                "sh",
            ])
            .arg(target_config)
            .arg(&op_token_src)
            .status();
    }
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
    let mut donors: Vec<Agent> = Vec::new();
    if let Some(ref pid) = agent.parent_id
        && let Ok(parent) = store.agent(pid)
        && parent.harness == agent.harness
    {
        donors.push(parent);
    }
    // Prefer root owner seats (`parent_id.is_none()`) before ephemeral children
    // so full provider/credential configurations are always found first.
    for other in &all_agents {
        if other.id != agent.id
            && other.parent_id.is_none()
            && other.harness == agent.harness
            && !donors.iter().any(|d| d.id == other.id)
        {
            donors.push(other.clone());
        }
    }
    for other in &all_agents {
        if other.id != agent.id
            && other.harness == agent.harness
            && !donors.iter().any(|d| d.id == other.id)
        {
            donors.push(other.clone());
        }
    }
    let static_rel_paths: &[&str] = match agent.harness.as_str() {
        "claude-code" => &[
            ".credentials.json",
            ".claude/.credentials.json",
            "settings.json",
            ".claude.json",
        ],
        "codex" => &[
            "auth.json",
            ".codex/auth.json",
            "config.toml",
            "models_cache.json",
            "model-catalog-direct.json",
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
            "settings.json",
            "muse/settings.json",
            "muse/trust.json",
            "muse/auth.json",
            ".config/muse/settings.json",
            ".config/muse/trust.json",
            ".config/muse/auth.json",
            ".local/share/muse/auth.json",
        ],
        "omp" => &[
            ".omp/agent/agent.db",
            ".omp/agent/config.yml",
            ".omp/agent/models.yml",
            ".omp/agent/models.db",
            "agent.db",
            "config.yml",
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
            ".grok/config.toml",
            ".grok/models_cache.json",
            ".grok/settings_cache.json",
        ],
        _ => &[],
    };
    let host_home = crate::sandbox::host_home_dir();
    let mut rel_paths: Vec<String> = static_rel_paths.iter().map(|s| (*s).to_string()).collect();
    // Discover any `{file:~/<rel>}` key paths referenced by opencode configs across
    // donors or host home so custom provider key files are seeded without hardcoding.
    for other in &all_agents {
        let cfg = agent_config_dir(data, &other.id);
        for candidate in [
            ".config/opencode/opencode.json",
            "opencode/opencode.json",
            "opencode.json",
        ] {
            if let Ok(text) = std::fs::read_to_string(cfg.join(candidate)) {
                for r in extract_file_tilde_refs(&text) {
                    if !rel_paths.contains(&r) {
                        rel_paths.push(r);
                    }
                }
            }
        }
    }
    if let Some(ref home) = host_home
        && let Ok(text) = std::fs::read_to_string(home.join(".config/opencode/opencode.json"))
    {
        for r in extract_file_tilde_refs(&text) {
            if !rel_paths.contains(&r) {
                rel_paths.push(r);
            }
        }
    }

    let explicit_non_default_profile = bound_profile_name(target_config).is_some_and(|b| {
        let prof = b.split_once('/').map(|(_, p)| p).unwrap_or(b.as_str());
        !prof.is_empty() && prof != "default"
    });
    let profile_roots = select_or_bind_profile(data, &all_agents, agent, target_config);
    for rel in &rel_paths {
        let dst = target_config.join(rel);
        let is_auth_cred = matches!(
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
        );
        let dst_non_empty = dst.metadata().is_ok_and(|m| m.len() > 0);
        if dst_non_empty && !(explicit_non_default_profile && is_auth_cred && !dst.is_file()) {
            continue;
        }
        let mut copied = false;
        for profile_root in &profile_roots {
            let src = profile_root.join(rel);
            if src.is_file() {
                if dst.is_file()
                    && let (Ok(a), Ok(b)) = (std::fs::read(&src), std::fs::read(&dst))
                    && a == b
                {
                    copied = true;
                    break;
                }
                if let Some(parent_dir) = dst.parent() {
                    let _ = std::fs::create_dir_all(parent_dir);
                }
                if std::fs::copy(&src, &dst).is_ok() {
                    copied = true;
                    break;
                }
            }
        }
        if copied || dst.exists() {
            continue;
        }
        if explicit_non_default_profile
            && matches!(
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
            )
        {
            continue;
        }
        for donor in &donors {
            let donor_config = agent_config_dir(data, &donor.id);
            let src = donor_config.join(rel);
            if !src.is_file() {
                continue;
            }
            if (rel == "opencode.json"
                || rel == "opencode/opencode.json"
                || rel == ".config/opencode/opencode.json")
                && let Ok(text) = std::fs::read_to_string(&src)
                && !text.contains("\"provider\"")
            {
                continue;
            }
            if let Some(parent_dir) = dst.parent() {
                let _ = std::fs::create_dir_all(parent_dir);
            }
            if std::fs::copy(&src, &dst).is_ok() {
                copied = true;
                break;
            }
        }
        if !copied && let Some(ref home) = host_home {
            let host_src = home.join(rel);
            if host_src.is_file() {
                if let Some(parent_dir) = dst.parent() {
                    let _ = std::fs::create_dir_all(parent_dir);
                }
                if std::fs::copy(&host_src, &dst).is_ok() {
                    // Persist into the centralized AnyAgents upstream provider profile store as well.
                    let provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
                    let default_profile = data.join("profiles").join(&provider).join("default");
                    ensure_provider_profile_meta(&default_profile, &provider, None);
                    let profile_dst = default_profile.join(rel);
                    if !profile_dst.exists() {
                        if let Some(p) = profile_dst.parent() {
                            let _ = std::fs::create_dir_all(p);
                        }
                        let _ = std::fs::copy(&dst, &profile_dst);
                    }
                }
            }
        }
    }
    #[cfg(unix)]
    if agent.harness == "dsh"
        && let Some(ref home) = host_home
    {
        let host_dsh = home.join(".dsh");
        let target_dsh = target_config.join(".dsh");
        let _ = std::fs::create_dir_all(&target_dsh);
        if let Ok(entries) = std::fs::read_dir(host_dsh) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if name_str.starts_with("runtime-") || name_str == "profiles" {
                    let dst = target_dsh.join(&name);
                    if !dst.exists() {
                        let _ = std::os::unix::fs::symlink(entry.path(), &dst);
                    }
                }
            }
        }
    }
    if agent.harness == "opencode" {
        for donor in &donors {
            let donor_key_dir = agent_config_dir(data, &donor.id)
                .join(".config")
                .join("opencode");
            if let Ok(entries) = std::fs::read_dir(&donor_key_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_file()
                        && let Some(name) = path.file_name()
                    {
                        let dst = target_config.join(".config").join("opencode").join(name);
                        if !dst.exists() {
                            if let Some(parent_dir) = dst.parent() {
                                let _ = std::fs::create_dir_all(parent_dir);
                            }
                            let _ = std::fs::copy(&path, &dst);
                        }
                    }
                }
            }
        }
    }
    // Ensure shared config files (`.kube/config`, `.gitconfig`, plus any `{file:~/...}` keys)
    // are also seeded from any existing seat if the same-harness donor did not have them.
    let mut shared_rels: Vec<String> = vec![".kube/config".to_string(), ".gitconfig".to_string()];
    for r in &rel_paths {
        if r.starts_with(".config/") && !shared_rels.contains(r) {
            shared_rels.push(r.clone());
        }
    }
    for shared_rel in &shared_rels {
        let dst = target_config.join(shared_rel);
        if dst.exists() {
            continue;
        }
        for other in &all_agents {
            if other.id == agent.id {
                continue;
            }
            let src = agent_config_dir(data, &other.id).join(shared_rel);
            if src.is_file() {
                if let Some(parent_dir) = dst.parent() {
                    let _ = std::fs::create_dir_all(parent_dir);
                }
                if std::fs::copy(&src, &dst).is_ok() {
                    break;
                }
            }
        }
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
                std::fs::copy(entry.path(), &target)?;
            }
        }
        Ok(())
    }
    copy_files(&source, &config)
        .map_err(|_| Error::Message("could not bind provider profile".into()))?;
    std::fs::write(
        config.join(".anyagents-profile"),
        format!("{provider}/{name}\n"),
    )?;
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
    }
    Ok(())
}

/// Create or update a named provider profile in `<data>/profiles/<provider>/<profile>`.
pub(crate) fn set_provider_profile(
    data: &Path,
    provider_profile: &str,
    key: &str,
) -> Result<(String, String), Error> {
    let (provider, profile) = provider_profile
        .split_once('/')
        .ok_or_else(|| Error::Message("expected <provider>/<profile> (e.g. kimi-code-plan/rhea)".into()))?;
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
        return Err(Error::Message("a nonempty single-line key is required".into()));
    }
    let dir = data.join("profiles").join(provider).join(profile);
    ensure_provider_profile_meta(&dir, provider, None);
    let default_dir = data.join("profiles").join(provider).join("default");
    if default_dir != dir && default_dir.join("opencode.json").is_file() && !dir.join("opencode.json").exists() {
        let _ = std::fs::copy(default_dir.join("opencode.json"), dir.join("opencode.json"));
    }
    let key_filename = match provider {
        "kimi-code-plan" => "kimi.key",
        "qwen-token-plan" => "qwen.key",
        "glm-code-plan" => "glm.key",
        "minimax-code-plan" => "minimax.key",
        "sylphx" => "sylphx.key",
        "openrouter" => "openrouter.key",
        other => {
            let prefix = other.split('-').next().unwrap_or(other);
            return write_profile_key_file(&dir, &format!("{prefix}.key"), trimmed)
                .map(|()| (provider.to_string(), profile.to_string()));
        }
    };
    write_profile_key_file(&dir, key_filename, trimmed)?;
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
        let prof_name = bound_profile_name(&cfg)
            .and_then(|b| {
                let p = b
                    .split_once('/')
                    .map(|(_, p)| p.to_string())
                    .unwrap_or(b);
                (!p.is_empty()).then_some(p)
            })
            .unwrap_or_else(|| "default".to_string());
        let profile_dir = profiles_root.join(&provider).join(&prof_name);
        match provider.as_str() {
            "anthropic" => {
                for rel in [
                    ".credentials.json",
                    ".claude/.credentials.json",
                    "settings.json",
                ] {
                    let src = cfg.join(rel);
                    if src.is_file() {
                        ensure_provider_profile_meta(&profile_dir, "anthropic", None);
                        let dst = profile_dir.join(rel);
                        if !dst.exists() {
                            if let Some(p) = dst.parent() {
                                let _ = std::fs::create_dir_all(p);
                            }
                            let _ = std::fs::copy(&src, &dst);
                        }
                    }
                }
            }
            "openai" => {
                for rel in ["auth.json", "config.toml"] {
                    let src = cfg.join(rel);
                    if src.is_file() {
                        ensure_provider_profile_meta(&profile_dir, "openai", None);
                        let dst = profile_dir.join(rel);
                        if !dst.exists() {
                            let _ = std::fs::copy(&src, &dst);
                        }
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
                        let dst = profile_dir.join(rel);
                        if !dst.exists() {
                            if let Some(p) = dst.parent() {
                                let _ = std::fs::create_dir_all(p);
                            }
                            let _ = std::fs::copy(&src, &dst);
                        }
                    }
                }
            }
            "muse" => {
                for rel in ["settings.json", "muse/settings.json", "muse/auth.json"] {
                    let src = cfg.join(rel);
                    if src.is_file() {
                        ensure_provider_profile_meta(&profile_dir, "muse", None);
                        let dst = profile_dir.join(rel);
                        if !dst.exists() {
                            if let Some(p) = dst.parent() {
                                let _ = std::fs::create_dir_all(p);
                            }
                            let _ = std::fs::copy(&src, &dst);
                        }
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
                    let _ = copy_host_file(&src_json, &profile_dir.join(opencode_rel));
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

    if let Some(home) = crate::sandbox::host_home_dir() {
        // 1. Anthropic subscription (`~/.claude`)
        if home.join(".claude/.credentials.json").is_file() {
            let dir = profiles_root.join("anthropic").join("default");
            ensure_provider_profile_meta(&dir, "anthropic", Some("subscription"));
            copy_host_file(
                &home.join(".claude/.credentials.json"),
                &dir.join(".claude/.credentials.json"),
            );
            copy_host_file(
                &home.join(".claude/.credentials.json"),
                &dir.join(".credentials.json"),
            );
            copy_host_file(
                &home.join(".claude/settings.json"),
                &dir.join("settings.json"),
            );
            copy_host_file(&home.join(".claude.json"), &dir.join(".claude.json"));
        }

        // 2. OpenAI / Codex subscription (`~/.codex`)
        if home.join(".codex/auth.json").is_file() {
            let dir = profiles_root.join("openai").join("default");
            ensure_provider_profile_meta(&dir, "openai", Some("subscription"));
            copy_host_file(&home.join(".codex/auth.json"), &dir.join("auth.json"));
            copy_host_file(
                &home.join(".codex/auth.json"),
                &dir.join(".codex/auth.json"),
            );
            copy_host_file(&home.join(".codex/config.toml"), &dir.join("config.toml"));
            copy_host_file(
                &home.join(".codex/models_cache.json"),
                &dir.join("models_cache.json"),
            );
        }

        // 3. Google Antigravity subscription (`~/.gemini`)
        if home
            .join(".gemini/antigravity-cli/antigravity-oauth-token")
            .is_file()
        {
            let dir = profiles_root.join("google-antigravity").join("default");
            ensure_provider_profile_meta(&dir, "google-antigravity", Some("subscription"));
            for rel in [
                ".gemini/antigravity-cli/antigravity-oauth-token",
                ".gemini/antigravity-cli/antigravity_state.pbtxt",
                ".gemini/antigravity-cli/jetski_state.pbtxt",
                ".gemini/config/config.json",
                ".gemini/config/.migrated",
            ] {
                copy_host_file(&home.join(rel), &dir.join(rel));
            }
        }

        // 4. Google Gemini API Key (`~/.codex/gemini-api-key`)
        if home.join(".codex/gemini-api-key").is_file() {
            let dir = profiles_root.join("google-gemini").join("api");
            ensure_provider_profile_meta(&dir, "google-gemini", Some("api-key"));
            copy_host_file(
                &home.join(".codex/gemini-api-key"),
                &dir.join("gemini-api-key"),
            );
        }

        // 5. Meta Muse Code subscriptions (`~/.config/muse` + `.omp/agent/agent.db`)
        if home.join(".config/muse/auth.json").is_file() {
            let dir = profiles_root.join("muse").join("default");
            ensure_provider_profile_meta(&dir, "muse", Some("subscription"));
            for rel in ["auth.json", "settings.json", "trust.json"] {
                copy_host_file(
                    &home.join(".config/muse").join(rel),
                    &dir.join(".config/muse").join(rel),
                );
                copy_host_file(
                    &home.join(".config/muse").join(rel),
                    &dir.join("muse").join(rel),
                );
            }
            copy_host_file(
                &home.join(".config/muse/settings.json"),
                &dir.join("settings.json"),
            );
        }

        // 6. DeepSeek Harness (`dsh` web session + API key in `~/.dsh`)
        if home.join(".dsh/.credentials.yaml").is_file() {
            let web_dir = profiles_root.join("deepseek").join("web");
            ensure_provider_profile_meta(&web_dir, "deepseek", Some("subscription"));
            for rel in [
                ".dsh/.credentials.yaml",
                ".dsh/settings.yaml",
                ".dsh/.anonymous-user-id",
                ".dsh/profiles/headless/cordis.yml",
            ] {
                copy_host_file(&home.join(rel), &web_dir.join(rel));
            }
            let api_dir = profiles_root.join("deepseek").join("api");
            ensure_provider_profile_meta(&api_dir, "deepseek", Some("api-key"));
            copy_host_file(
                &home.join(".dsh/.credentials.yaml"),
                &api_dir.join(".dsh/.credentials.yaml"),
            );
            copy_host_file(
                &home.join(".omp/agent/agent.db"),
                &api_dir.join(".omp/agent/agent.db"),
            );
        }

        // 7. xAI Grok CLI (`~/.grok`)
        if home.join(".grok/auth.json").is_file() {
            let dir = profiles_root.join("xai").join("default");
            ensure_provider_profile_meta(&dir, "xai", Some("subscription"));
            for rel in [
                ".grok/auth.json",
                ".grok/config.toml",
                ".grok/models_cache.json",
                ".grok/settings_cache.json",
            ] {
                copy_host_file(&home.join(rel), &dir.join(rel));
            }
        }

        // 8. OMP (`agent.db` + `models.yml` containing Cursor OAuth & OpenRouter API key)
        if home.join(".omp/agent/agent.db").is_file() {
            let cursor_dir = profiles_root.join("cursor").join("default");
            ensure_provider_profile_meta(&cursor_dir, "cursor", Some("subscription"));
            for rel in [
                ".omp/agent/agent.db",
                ".omp/agent/config.yml",
                ".omp/agent/models.yml",
                ".omp/agent/models.db",
            ] {
                copy_host_file(&home.join(rel), &cursor_dir.join(rel));
                if let Some(fname) = Path::new(rel).file_name() {
                    copy_host_file(&home.join(rel), &cursor_dir.join(fname));
                }
            }

            let or_dir = profiles_root.join("openrouter").join("default");
            ensure_provider_profile_meta(&or_dir, "openrouter", Some("api-key"));
            copy_host_file(
                &home.join(".omp/agent/agent.db"),
                &or_dir.join(".omp/agent/agent.db"),
            );
            copy_host_file(
                &home.join(".dsh/.credentials.yaml"),
                &or_dir.join(".dsh/.credentials.yaml"),
            );
        }

        // 9. Extra Qwen Cloud API key (`~/.codex/qwencloud-api-key`)
        if home.join(".codex/qwencloud-api-key").is_file() {
            let qc = profiles_root.join("qwen-token-plan").join("qwencloud");
            ensure_provider_profile_meta(&qc, "qwen-token-plan", Some("coding-plan"));
            copy_host_file(
                &home.join(".codex/qwencloud-api-key"),
                &qc.join("qwencloud-api-key"),
            );
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
                });
            }
        }
    }

    // Bind active agents to their resolved `<provider>/<profile>`.
    for agent in &agents {
        if agent.state == "archived" {
            continue;
        }
        let cfg = agent_config_dir(data, &agent.id);
        let expected_provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
        let bound = bound_profile_name(&cfg);
        let (bound_prov, bound_prof) = match bound.as_deref() {
            Some(s) if s.contains('/') => {
                let (p, n) = s.split_once('/').unwrap_or((&expected_provider, "default"));
                (p.to_string(), n.to_string())
            }
            Some(s) if !s.is_empty() => (expected_provider.clone(), s.to_string()),
            _ => (expected_provider.clone(), "default".to_string()),
        };
        let normalized = format!("{bound_prov}/{bound_prof}");
        if bound.as_deref() != Some(normalized.as_str()) {
            let _ = std::fs::write(cfg.join(".anyagents-profile"), format!("{normalized}\n"));
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
    entries.sort_by(|a, b| (&a.provider, &a.profile).cmp(&(&b.provider, &b.profile)));
    entries
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
/// from `<data>/profiles/<provider>/<profile>` using least-loaded assignment
/// across agents sharing that upstream provider, and persists
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
        let min_count = counts.values().copied().min().unwrap_or(0);
        let candidates: Vec<&(String, PathBuf)> = available
            .iter()
            .filter(|(name, _)| counts.get(name).copied().unwrap_or(0) == min_count)
            .collect();
        let hash = agent.name.bytes().fold(0usize, |acc, b| {
            acc.wrapping_mul(31).wrapping_add(b as usize)
        });
        let chosen = candidates[hash % candidates.len()];
        let _ = std::fs::write(
            target_config.join(".anyagents-profile"),
            format!("{provider}/{}\n", chosen.0),
        );
        roots.push(chosen.1.clone());
    } else {
        let _ = std::fs::write(
            target_config.join(".anyagents-profile"),
            format!("{provider}/default\n"),
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
    let text =
        serde_json::to_string_pretty(cfg).map_err(|err| Error::Message(err.to_string()))?;
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
            let prov = prof.split_once('/').map(|(p, _)| p).unwrap_or(prof.as_str());
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
                    let prov = p.split_once('/').map(|(pr, _)| pr).unwrap_or(&target_provider);
                    (prov == target_provider).then(|| p.to_string())
                }
            })
            .or_else(inherit_profile_if_compatible)
    };

    (resolved_harness, resolved_model, resolved_profile)
}
