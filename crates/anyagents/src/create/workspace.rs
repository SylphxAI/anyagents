//! Agent instruction & description mirrors, seat environment setup, skills projection, and shared file helpers.

use std::path::{Path, PathBuf};

use anyagents_core::{Agent, Error, Store, agent_config_dir};

use crate::mcp_config;

use super::spawn::create_private_config_dir;
use super::{
    DEFAULT_AGENT_ROLE, backfill_historical_agent_roles, compute_live_session_continuity,
    discover_available_archetype_slugs, effective_agent_instructions, harness_session_filename,
    mark_instructions_rotated_if_changed,
};

pub(crate) fn sync_all_agent_instructions(store: &Store, data: &Path) -> Result<(), Error> {
    backfill_historical_agent_roles(store, data)?;
    let agents = store.list_agents()?;
    for agent in &agents {
        if agent.state == "archived" {
            continue;
        }
        let cfg = agent_config_dir(data, &agent.id);
        if !cfg.is_dir() {
            continue;
        }
        sync_agent_description_files(store, agent, &cfg)?;
    }
    Ok(())
}

pub(crate) fn write_agent_instructions(
    config: &Path,
    agent: &Agent,
    parent_name: Option<&str>,
) -> Result<(), Error> {
    let cached_bridge = std::fs::read_to_string(config.join(".anyagents-session-bridge.json"))
        .ok()
        .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
        .and_then(|v| {
            let h = v.get("harness").and_then(|x| x.as_str())?;
            if h != agent.harness {
                return None;
            }
            v.get("summary")
                .and_then(|x| x.as_str())
                .map(str::to_string)
        });
    write_agent_instructions_with_continuity(config, agent, parent_name, cached_bridge.as_deref())
}

fn lock_instruction_mirrors(config: &Path) -> std::io::Result<std::fs::File> {
    let root = config
        .parent()
        .ok_or_else(|| std::io::Error::other("agent root missing"))?;
    let sessions = root.join("sessions");
    create_private_config_dir(&sessions)?;
    let mut options = std::fs::OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let file = options.open(sessions.join("instructions.lock"))?;
    file.lock()?;
    Ok(file)
}

pub(crate) fn write_agent_instructions_with_continuity(
    config: &Path,
    agent: &Agent,
    parent_name: Option<&str>,
    continuity: Option<&str>,
) -> Result<(), Error> {
    write_agent_instructions_with_continuity_and_store(config, agent, parent_name, continuity, None)
}

pub(crate) fn write_agent_instructions_with_continuity_and_store(
    config: &Path,
    agent: &Agent,
    parent_name: Option<&str>,
    continuity: Option<&str>,
    store: Option<&Store>,
) -> Result<(), Error> {
    // Launch preparation and description/config updates can render concurrently,
    // including from different server processes. Hold a host-owned lock through
    // all mirrors and their readbacks so writers cannot replace each other's
    // verified output or leave mirrors from different snapshots.
    let _instructions_lock = lock_instruction_mirrors(config)?;
    let _ = std::fs::remove_file(config.join(".anyagents-harness-continuity.md"));
    // Minimal, persona-neutral identity & routing envelope (arXiv:2611.05306 / arXiv:2611.03931):
    // inject only causal addressing facts and non-obvious runtime rules; tool schemas carry API details.
    let mut doc = format!(
        "# AnyAgents Identity & Environment Awareness\n\n         ## 1. Identity\n\n         - **Agent Name**: `{name}` (`agent:{name}`)\n         - **Agent ID**: `{id}`\n",
        name = agent.name,
        id = agent.id,
    );
    if let Some(role) = agent
        .role
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        doc.push_str(&format!("- **Role Label**: `{role}`\n"));
    }
    if let Some(pub_desc) = agent.public_description() {
        doc.push_str(&format!("- **Public Description**: {pub_desc}\n"));
    }
    if let Some(parent) = parent_name {
        doc.push_str(&format!("- **Parent**: `agent:{parent}` (`\"parent\"`)\n"));
    } else {
        doc.push_str("- **Parent**: none (root)\n");
    }

    doc.push_str("- **Lifecycle**: Role templates may declare `lifecycle: standing | task`. Sub-agents never archive themselves; the spawning parent closes/archives a task sub-agent (`action: \"archive\"`) once its work is complete. Idle processes release after mailbox/child/background work drains; identity and session persist.\n");

    let data_dir_guess = config
        .parent()
        .and_then(|agent_dir| {
            let parent = agent_dir.parent()?;
            if parent.file_name().and_then(|s| s.to_str()) == Some("agents") {
                parent.parent()
            } else {
                Some(agent_dir)
            }
        })
        .unwrap_or(config);
    let data_dir = store.map(Store::data_dir).unwrap_or(data_dir_guess);
    if let Some(effective) = effective_agent_instructions(store, data_dir, agent) {
        doc.push_str("\n## 2. Instructions\n\n");
        doc.push_str(&effective);
        doc.push('\n');
    }

    doc.push_str(
        "\n## 3. Hierarchy, Topology & Communication Rules\n\n         - `description` is public; `instructions` are private. Supervision is downward-only; role labels impose no preset persona or lifecycle policy.\n         - Stdout never reaches peers or channels. Reply via `send` (`to`, `reply_to`, `text`) using `<message ... answer_to=\"...\">`; emit `NO_REPLY` in non-matching group turns.\n         - Never poll: `<message>`, child completions, and `schedules` watches wake you reactively.\n         - Any agent may call the 7 AnyAgents MCP tools (`agents`, `send`, `inbox`, `conversation`, `memory`, `schedules`, `groups`) instead of vendor sub-agent/timer tools.\n",
    );
    if let Some(parent) = parent_name {
        doc.push_str(&format!(
            "- Direct parent: `agent:{parent}`; report via `send` to `parent`.\n"
        ));
    }
    let available_roles = discover_available_archetype_slugs(
        agent.workdir.as_deref().map(Path::new),
        config.parent(),
        data_dir,
    );
    if !available_roles.is_empty() {
        let formatted_roles = available_roles
            .iter()
            .map(|slug| format!("`{slug}`"))
            .collect::<Vec<_>>()
            .join(", ");
        doc.push_str(&format!(
            "- Available sub-agent roles (`role` in `agents`/`spawn`; defaults to `{DEFAULT_AGENT_ROLE}`): {formatted_roles} (see `spawn` tool `role` schema for details).\n"
        ));
    }

    // A configured harness switch rotates before rendering. Ordinary updates
    // consume the persisted pin without opening live learned-memory files.
    if let Some(root) = config.parent() {
        if let Some((_, policy)) = agent
            .launch_env
            .iter()
            .find(|(key, _)| key == "ANYAGENTS_MEMORY_GUIDANCE")
            && !policy.trim().is_empty()
        {
            doc.push_str("\n## Memory policy\n\n");
            doc.push_str(policy);
            doc.push('\n');
        }
        match anyagents_core::host_session::prepare_harness_with_source(
            root,
            &agent.harness,
            harness_session_filename(&agent.harness),
            crate::vendor::memory_source(agent),
        ) {
            Ok(Some(session)) => {
                // Never inject a duplicate pinned memory block if caller instructions
                // already carry `<learned_memory>` or `<memory>`.
                if !doc.contains("<learned_memory>") && !doc.contains("<memory>") {
                    doc.push('\n');
                    doc.push_str(&session.memory.section);
                    doc.push('\n');
                }
            }
            Ok(None) => (), // First dispatch pins and rerenders before launch.
            Err(error) => {
                return Err(Error::Message(format!(
                    "invalid host memory pin for {}: {error}",
                    agent.id
                )));
            }
        }
    }

    // Spiron assembles the shared layer independently. Other harnesses load
    // their global AGENTS.md / CLAUDE.md / GEMINI.md instead, so those files
    // must retain the shared layer even though SYSTEM_PROMPT.md stays compact.
    let mut native_doc = doc.clone();
    if let Some(shared) = read_shared_agents_md(config) {
        native_doc.push_str("\n## 4. Shared Instructions (AGENTS.md)\n\n");
        native_doc.push_str(&shared);
        native_doc.push('\n');
    }
    if let Some(cont) = continuity.map(str::trim).filter(|s| !s.is_empty()) {
        // Reserve each file's own shared-layer budget before adding continuity.
        // Never truncate company instructions to make room for history.
        const MAX_DOC_BYTES: usize = 23_200;
        for rendered in [&mut doc, &mut native_doc] {
            let remaining = MAX_DOC_BYTES.saturating_sub(rendered.len() + 2);
            if remaining >= 256 {
                rendered.push('\n');
                if cont.len() <= remaining {
                    rendered.push_str(cont);
                } else {
                    let mut cut = remaining;
                    while cut > 0 && !cont.is_char_boundary(cut) {
                        cut -= 1;
                    }
                    if let Some(nl) = cont[..cut].rfind('\n')
                        && nl > 0
                    {
                        cut = nl;
                    }
                    rendered.push_str(&cont[..cut]);
                }
                rendered.push('\n');
            }
        }
    }

    let sys_prompt_path = config.join("SYSTEM_PROMPT.md");
    let prompt_changed = std::fs::read(&sys_prompt_path).ok().as_deref() != Some(doc.as_bytes())
        || std::fs::read(config.join("AGENTS.md")).ok().as_deref() != Some(native_doc.as_bytes());
    if prompt_changed {
        for (sess_file, fp_file) in [
            (
                "spiron-cloud-session.json",
                ".spiron-cloud-instructions-fingerprint",
            ),
            (
                "sylphx-cloud-session.json",
                ".sylphx-cloud-instructions-fingerprint",
            ),
        ] {
            if config.join(sess_file).is_file() {
                let _ = write_seat_shared_file(&config.join(fp_file), b"stale\n", 0o664);
            }
        }
    }

    let paths = [
        (sys_prompt_path, &doc),
        (config.join("AGENTS.md"), &native_doc),
        (config.join("opencode/AGENTS.md"), &native_doc),
        (config.join(".config/opencode/AGENTS.md"), &native_doc),
        (config.join("CLAUDE.md"), &native_doc),
        (config.join(".claude/CLAUDE.md"), &native_doc),
        (config.join(".gemini/config/GEMINI.md"), &native_doc),
        (config.join("muse/AGENTS.md"), &native_doc),
        (config.join(".config/muse/AGENTS.md"), &native_doc),
        (config.join(".omp/agent/APPEND_SYSTEM.md"), &native_doc),
        (config.join(".omp/agent/AGENTS.md"), &native_doc),
    ];
    for (path, contents) in &paths {
        if std::fs::read(path).ok().as_deref() == Some(contents.as_bytes()) {
            continue;
        }
        write_required_file(path, contents.as_bytes(), 0o664)?;
    }
    // agy identifies GEMINI.md as "user_global", but rules by file path: even
    // symlinks inject the prompt twice. Keep only the canonical global rule and
    // remove both legacy aliases on every refresh of existing configurations.
    let gemini_cfg = config.join(".gemini").join("config");
    let _ = std::fs::remove_file(gemini_cfg.join("AGENTS.md"));
    let _ = std::fs::remove_file(gemini_cfg.join("rules").join("AGENTS.md"));
    Ok(())
}

/// Fast, non-intrusive instruction sync used by `describe()`: updates the
/// agent's system prompt files (`AGENTS.md`, `CLAUDE.md`, `SYSTEM_PROMPT.md`,
/// `config.toml` developer instructions) without walking or re-chowning the
/// `config/` or `work/` directory trees, and guarantees `config`'s existing
/// group and execute bits remain intact.
pub(crate) fn sync_agent_description_files(
    store: &Store,
    agent: &Agent,
    config: &Path,
) -> Result<(), Error> {
    sync_agent_description_files_for_live(store, agent, config, None)
}

pub(crate) fn sync_agent_description_files_for_live(
    store: &Store,
    agent: &Agent,
    config: &Path,
    min_started_ms: Option<i64>,
) -> Result<(), Error> {
    #[cfg(unix)]
    let pre_existing_mode = {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::metadata(config)
            .ok()
            .map(|m| m.permissions().mode() & 0o7777)
    };
    create_private_config_dir(config)?;
    #[cfg(unix)]
    if let Some(base_mode) = pre_existing_mode {
        use std::os::unix::fs::PermissionsExt as _;
        let group_exec = if (base_mode & 0o0060) != 0 { 0o0010 } else { 0 };
        let other_exec = if (base_mode & 0o0006) != 0 { 0o0001 } else { 0 };
        let safe_mode = (base_mode | 0o0700 | group_exec | other_exec) & 0o7777;
        if safe_mode != base_mode {
            std::fs::set_permissions(config, std::fs::Permissions::from_mode(safe_mode))?;
        }
    }
    let parent_name = agent
        .parent_id
        .as_ref()
        .and_then(|pid| store.agent(pid).ok())
        .map(|p| p.name)
        .or_else(|| {
            agent
                .launch_env
                .iter()
                .find(|(k, _)| k == "ANYAGENTS_PARENT_NAME")
                .map(|(_, v)| v.clone())
        });
    let before_prompt = std::fs::read(config.join("SYSTEM_PROMPT.md")).ok();
    let continuity = compute_live_session_continuity(store, agent, config);
    write_agent_instructions_with_continuity_and_store(
        config,
        agent,
        parent_name.as_deref(),
        continuity.as_deref(),
        Some(store),
    )?;
    mark_instructions_rotated_if_changed(
        store,
        config,
        agent,
        before_prompt.as_deref(),
        min_started_ms,
    )?;
    if agent.harness == "codex" {
        let codex_toml = config.join("config.toml");
        if codex_toml.is_file() {
            mcp_config::sync_codex_developer_instructions(&codex_toml, continuity.as_deref())?;
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let base_mode = pre_existing_mode.unwrap_or(0o0700);
        let group_exec = if (base_mode & 0o0060) != 0 { 0o0010 } else { 0 };
        let other_exec = if (base_mode & 0o0006) != 0 { 0o0001 } else { 0 };
        let safe_mode = (base_mode | 0o0700 | group_exec | other_exec) & 0o7777;
        if let Ok(after_meta) = std::fs::metadata(config)
            && (after_meta.permissions().mode() & 0o7777) != safe_mode
        {
            std::fs::set_permissions(config, std::fs::Permissions::from_mode(safe_mode))?;
        }
    }
    Ok(())
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

pub(crate) fn ensure_seat_environment(target_config: &Path, workdir: &Path) -> Result<(), Error> {
    for subdir in ["tmp", "data-home", "cache-home", "state-home"] {
        let dir = target_config.join(subdir);
        std::fs::create_dir_all(&dir)?;
        // Shared runtime trees must not permit peers to unlink each other's
        // entries. One the agent's seat created only that seat may chmod.
        #[cfg(unix)]
        crate::seat_tree::set_runtime_dir_mode(&dir, 0o1777)?;
    }
    if !crate::sandbox_providers::SandboxMode::from_env().is_remote_required() {
        crate::repo::configure_seat_git(target_config, workdir)?;
    }
    #[cfg(unix)]
    {
        let shared: Vec<String> = std::env::var("ANYAGENTS_SHARED_HOME_DIRS")
            .ok()
            .map(|raw| {
                serde_json::from_str(&raw)
                    .map_err(|err| Error::Message(format!("ANYAGENTS_SHARED_HOME_DIRS: {err}")))
            })
            .transpose()?
            .unwrap_or_default();
        let host_home = crate::sandbox::host_home_dir();
        for shared_dir in shared {
            let rel = Path::new(&shared_dir);
            if rel.is_absolute()
                || rel
                    .components()
                    .any(|c| matches!(c, std::path::Component::ParentDir))
            {
                return Err(Error::Message(
                    "shared home paths must be relative and cannot escape the home".into(),
                ));
            }
            let home = host_home.as_ref().ok_or_else(|| {
                Error::Message("configured shared home paths require an available host home".into())
            })?;
            let src = home.join(rel);
            let dst = target_config.join(rel);
            if !dst.exists() {
                if let Some(parent) = dst.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::os::unix::fs::symlink(&src, &dst)?;
            }
        }
        for (rel, key) in [
            (".rustup", "ANYAGENTS_RUSTUP_HOME"),
            (".cargo", "ANYAGENTS_CARGO_HOME"),
        ] {
            if let Some(src) = std::env::var_os(key)
                && !target_config.join(rel).exists()
            {
                std::os::unix::fs::symlink(PathBuf::from(src), target_config.join(rel))?;
            }
        }
        if let Some(host_home) = host_home {
            let host_local_opt = host_home.join(".local/opt");
            let dst_local_opt = target_config.join(".local/opt");
            if host_local_opt.exists() && !dst_local_opt.exists() {
                if let Some(parent) = dst_local_opt.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                let _ = std::os::unix::fs::symlink(&host_local_opt, &dst_local_opt);
            }
            ensure_seat_skills(target_config, &host_home);
        }
    }
    Ok(())
}

pub(crate) fn data_dir_from_config(config: &Path) -> Option<PathBuf> {
    let parent1 = config.parent()?;
    let parent2 = parent1.parent()?;
    if parent2.file_name().and_then(|s| s.to_str()) == Some("agents") {
        parent2.parent().map(Path::to_path_buf)
    } else {
        Some(parent1.to_path_buf())
    }
}

pub(crate) fn read_shared_agents_md(config: &Path) -> Option<String> {
    let data_dir = data_dir_from_config(config)?;
    std::fs::read_to_string(data_dir.join("AGENTS.md"))
        .ok()
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
}

/// Fast-forward the checkout that holds the shared `skills` and `AGENTS.md`.
/// One `ls-remote` names origin's `main`; while it is the commit this process
/// last pulled, the checkout is neither scanned nor pulled.
#[cfg(unix)]
pub(crate) fn sync_canonical_skills(data_dir: &Path) {
    use std::collections::BTreeMap;
    use std::sync::{Mutex, PoisonError};
    static PULLED: Mutex<BTreeMap<PathBuf, String>> = Mutex::new(BTreeMap::new());
    let git = |repo_root: &Path| {
        let mut command = std::process::Command::new("git");
        command
            .args([
                "-c",
                "core.fsmonitor=false",
                "-c",
                "core.hooksPath=/dev/null",
            ])
            .arg("-C")
            .arg(repo_root);
        command
    };
    let repo_roots: std::collections::BTreeSet<PathBuf> = ["skills", "AGENTS.md"]
        .into_iter()
        .filter_map(|rel| std::fs::canonicalize(data_dir.join(rel)).ok())
        .filter_map(|resolved| resolved.parent().map(Path::to_path_buf))
        .filter(|repo_root| repo_root.join(".git").exists())
        .collect();
    for repo_root in repo_roots {
        let remote_main = git(&repo_root)
            .args(["ls-remote", "--quiet", "origin", "refs/heads/main"])
            .stderr(std::process::Stdio::null())
            .output()
            .ok()
            .filter(|out| out.status.success())
            .and_then(|out| {
                String::from_utf8_lossy(&out.stdout)
                    .split_whitespace()
                    .next()
                    .map(str::to_string)
            });
        let pulled = || PULLED.lock().unwrap_or_else(PoisonError::into_inner);
        if remote_main.is_some() && pulled().get(&repo_root) == remote_main.as_ref() {
            continue;
        }
        let is_clean = git(&repo_root)
            .args(["status", "--porcelain"])
            .output()
            .ok()
            .is_some_and(|o| o.status.success() && o.stdout.is_empty());
        if !is_clean {
            continue;
        }
        let fast_forwarded = git(&repo_root)
            .args(["pull", "--ff-only", "--quiet", "origin", "main"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .is_ok_and(|status| status.success());
        if fast_forwarded && let Some(commit) = remote_main {
            pulled().insert(repo_root, commit);
        }
    }
}

#[cfg(unix)]
pub(crate) fn ensure_seat_skills(target_config: &Path, _host_home: &Path) {
    // An absent shared library must not disable the agent's own overlay.
    let Some(data) = data_dir_from_config(target_config) else {
        return;
    };
    let skills_root = data.join("skills");
    if let Some(root) = target_config.parent()
        && let Err(error) = anyagents_core::host_skills::mount_with_fallback(
            root,
            &skills_root,
            target_config,
            |target, view, global, overlay| {
                install_skill_projection_as_owner(target_config, target, view, global, overlay)
            },
        )
    {
        eprintln!(
            "skill overlay mount refused for {}: {error}",
            target_config.display()
        );
    }
}

/// Only a refused mirror is delegated, and all effects run as the existing
/// parent owner. No daemon chmod, broad home grant or foreign-link replacement.
#[cfg(unix)]
pub(super) const SKILL_PROJECTION_OWNER_SCRIPT: &str = r#"set -eu
config=$1; relative=$2; view=$3; global=$4; overlay=$5
  managed_source() {
    for src_root in "$global" "$overlay"; do
      case $1 in
        "$src_root"/*)
          rest=${1#"$src_root"/}
          case "/$rest/" in *"/./"*|*"/../"*|*"//"*) continue;; esac
          case $rest in
            synced/*/*) rest=${rest#synced/}; rest=${rest#*/}; case $rest in */*) ;; *) return 0;; esac;;
            */*) ;;
            *) return 0;;
          esac;;
      esac
    done
    return 1
  }
if [ "$relative" = "skills-view" ]; then
  if [ -L "$overlay" ] || [ -L "$view" ]; then exit 1; fi
  mkdir -p -m 02770 "$overlay"
  chmod 02770 "$overlay"
  mkdir -p "$view"
  chmod 0755 "$view"
  # Same bounded layout and precedence as host_skills::discover. Shell glob
  # order is deterministic under C; the first synced bucket wins.
  LC_ALL=C; export LC_ALL
  desired_for() {
    name=$1; desired=""
    for src_root in "$global" "$overlay"; do
      for bucket in "$src_root"/synced/*; do
        [ -d "$bucket" ] || continue
        case ${bucket##*/} in .*) continue;; esac
        if [ -d "$bucket/$name" ] && [ -f "$bucket/$name/SKILL.md" ]; then
          desired=$bucket/$name; break
        fi
      done
    done
    # Direct skills override synced skills, irrespective of source root.
    for src_root in "$global" "$overlay"; do
      if [ -d "$src_root/$name" ] && [ -f "$src_root/$name/SKILL.md" ]; then desired=$src_root/$name; fi
    done
    # Keep visible legacy containers, without letting them mask a skill.
    if [ -z "$desired" ]; then
      for src_root in "$global" "$overlay"; do
        if [ -d "$src_root/$name" ]; then desired=$src_root/$name; fi
      done
    fi
  }
  for item in "$view"/*; do
    [ -L "$item" ] || continue
    old=$(readlink "$item")
    if managed_source "$old"; then
      desired_for "${item##*/}"
      if [ "$old" != "$desired" ]; then rm "$item"; fi
    fi
  done
  for item in "$global"/* "$overlay"/* "$global"/synced/*/* "$overlay"/synced/*/*; do
    [ -d "$item" ] || continue
    name=${item##*/}
    case $name in .*) continue;; esac
    desired_for "$name"
    [ -n "$desired" ] || continue
    link=$view/$name
    if [ -L "$link" ] || [ -e "$link" ]; then continue; fi
    ln -s "$desired" "$link"
  done
  exit 0
fi
# Every existing component is inspected by its actual owner, not the daemon
# which was denied traversal. Foreign parent links are preserved untouched.
parent=$config
old_ifs=$IFS; IFS=/; set -- $relative; IFS=$old_ifs
while [ "$#" -gt 1 ]; do
  parent=$parent/$1; shift
  if [ -L "$parent" ]; then exit 0; fi
  mkdir -p "$parent"
done
target=$parent/$1
if [ -L "$target" ]; then
  old=$(readlink "$target")
  if [ "$old" = "$view" ]; then exit 0; fi
  if [ "$old" != "$global" ]; then exit 0; fi
  temp=$parent/.skills-owner-$$
  trap 'rm -f "$temp"' EXIT HUP INT TERM
  ln -s "$view" "$temp"
  mv -fT "$temp" "$target"
elif [ -d "$target" ]; then
  for item in "$target"/*; do
    [ -L "$item" ] || continue
    old=$(readlink "$item")
    if managed_source "$old" && [ ! -e "$view/${item##*/}" ]; then rm "$item"; fi
  done
  for item in "$view"/*; do
    [ -L "$item" ] || continue
    link=$target/${item##*/}; source=$(readlink "$item")
    if [ -L "$link" ]; then
      old=$(readlink "$link")
      if ! managed_source "$old"; then continue; fi
      if [ "$old" = "$source" ]; then continue; fi
      rm "$link"
    elif [ -e "$link" ]; then continue; fi
    ln -s "$source" "$link"
  done
elif [ ! -e "$target" ]; then ln -s "$view" "$target"; fi
"#;

#[cfg(unix)]
fn install_skill_projection_as_owner(
    config: &Path,
    target: &Path,
    view: &Path,
    global: &Path,
    overlay: &Path,
) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt as _;
    let root = view.parent().unwrap_or(config);
    let relative = if target == view {
        Path::new("skills-view")
    } else {
        let rel = target.strip_prefix(config).map_err(std::io::Error::other)?;
        if rel
            .components()
            .any(|c| !matches!(c, std::path::Component::Normal(_)))
        {
            return Err(std::io::Error::other("invalid skill projection path"));
        }
        rel
    };
    let srv_uid = crate::sandbox::server_uid();
    let candidates = std::iter::once(target)
        .chain(std::iter::once(overlay))
        .chain(
            target
                .parent()
                .into_iter()
                .flat_map(Path::ancestors)
                .take_while(|p| p.starts_with(root)),
        )
        .chain(std::iter::once(config));
    let mut any_dir = false;
    let mut seat_owner = None;
    for path in candidates {
        if let Ok(meta) = std::fs::symlink_metadata(path)
            && meta.is_dir()
        {
            any_dir = true;
            if meta.uid() != srv_uid {
                seat_owner = Some((meta.uid(), meta.gid()));
                break;
            }
        }
    }
    if !any_dir {
        return Err(std::io::Error::other("skill projection owner missing"));
    }
    // UID and primary GID are independent OS identities.
    let (eff_uid, eff_gid) = seat_owner
        .ok_or_else(|| std::io::Error::other("skill projection is not owned by another seat"))?;
    let launcher = crate::sandbox::descriptor_launcher()
        .ok_or_else(|| std::io::Error::other("skill projection launcher missing"))?;
    let groups = match crate::sandbox::sock_gid_for() {
        Some(sg) if sg != eff_gid && sg != 0 => format!("{eff_gid},{sg}"),
        _ => eff_gid.to_string(),
    };
    let output = std::process::Command::new(launcher)
        .args([
            "--reuid",
            &eff_uid.to_string(),
            "--regid",
            &eff_gid.to_string(),
            "--groups",
            &groups,
            "--no-new-privs",
            "--",
            "/bin/sh",
            "-c",
            SKILL_PROJECTION_OWNER_SCRIPT,
            "skill-projection-owner",
        ])
        .args([
            config.as_os_str(),
            relative.as_os_str(),
            view.as_os_str(),
            global.as_os_str(),
            overlay.as_os_str(),
        ])
        .stdin(std::process::Stdio::null())
        .output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "owner skill projection refused: {}",
            output.status
        )))
    }
}

pub(crate) fn ensure_seat_op_config(target_config: &Path, seat_launch: &str) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        let mut parts = seat_launch.split('\t');
        let (Some(launcher), Some(uid), Some(gid), Some(groups)) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return;
        };
        let Ok(target_uid) = uid.parse::<u32>() else {
            return;
        };
        let Ok(target_gid) = gid.parse::<u32>() else {
            return;
        };
        let dot_config = target_config.join(".config");
        let _ = std::fs::create_dir_all(&dot_config);
        let _ = std::fs::set_permissions(&dot_config, std::fs::Permissions::from_mode(0o2770));
        let op_dir = dot_config.join("op");
        if let Ok(meta) = std::fs::symlink_metadata(&op_dir)
            && (meta.uid() != target_uid || meta.gid() != target_gid)
        {
            let me = crate::sandbox::server_uid();
            if meta.uid() == me {
                let _ = std::fs::remove_dir_all(&op_dir);
            } else {
                let old_uid = meta.uid().to_string();
                let old_gid = meta.gid().to_string();
                let _ = std::process::Command::new(launcher)
                    .args([
                        "--reuid",
                        &old_uid,
                        "--regid",
                        &old_gid,
                        "--groups",
                        &old_gid,
                        "--no-new-privs",
                        "--",
                        "rm",
                        "-rf",
                    ])
                    .arg(&op_dir)
                    .stderr(std::process::Stdio::null())
                    .status();
                let _ = std::fs::remove_dir_all(&op_dir);
            }
        }
        let op_token_src = crate::sandbox::host_home_dir()
            .map(|h| h.join(".config/op-seat/service-account-token"))
            .filter(|p| p.exists())
            .unwrap_or_default();
        let connect_token_src = crate::sandbox::host_home_dir()
            .map(|h| h.join(".config/op/connect-token"))
            .filter(|p| p.exists())
            .unwrap_or_default();
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
                "cd \"$1\" && mkdir -p .config/op && chmod 00700 .config/op && if [ -n \"$2\" ]; then ln -sfn \"$2\" .config/op/service-account-token; fi && if [ -n \"$3\" ]; then ln -sfn \"$3\" .config/op/connect-token; fi",
                "sh",
            ])
            .arg(target_config)
            .arg(op_token_src.as_os_str())
            .arg(connect_token_src.as_os_str())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

pub(crate) fn chmod_seat_file_readable(path: &Path) {
    #[cfg(unix)]
    if !crate::sandbox::is_isolated_test_env()
        && Path::new("/usr/local/bin/anyagents-setpriv").is_file()
        && let Ok(meta) = std::fs::symlink_metadata(path)
        && !meta.file_type().is_symlink()
        && meta.file_type().is_file()
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
                    "0666",
                ])
                .arg(fname)
                .stderr(std::process::Stdio::null())
                .status();
        }
    }
}

/// Atomically replace `dst` only after writing a sibling temp file, `sync_all()`,
/// non-empty + full read-back verification, `rename`, and parent-directory sync.
/// A failed write (`ENOSPC`, `EROFS`, `EACCES`) never truncates or removes `dst`.
pub(crate) fn write_required_file(dst: &Path, bytes: &[u8], mode: u32) -> Result<(), Error> {
    anyagents_harness::atomic_write_nonempty_file(dst, bytes, mode).map_err(Error::from)
}

pub(crate) fn write_seat_shared_file(dst: &Path, bytes: &[u8], mode: u32) -> bool {
    match write_required_file(dst, bytes, mode) {
        Ok(()) => true,
        Err(err) => {
            eprintln!("cannot persist {}: {err}", dst.display());
            false
        }
    }
}
