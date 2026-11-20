//! Single-source-of-truth `<learned_memory>` refresh for hydrated prompt files in
//! PostgreSQL serverless mode.
//!
//! When a stateless replica hydrates an agent's config files from `agent_files(config)`
//! and its learned memory from `agent_files(memory)`, pre-migration prompt files
//! (`SYSTEM_PROMPT.md`, `AGENTS.md`, `CLAUDE.md`, `.gemini/config/GEMINI.md`, etc.)
//! may still carry an older `<learned_memory>...</learned_memory>` block. This module
//! rewrites that block in-place from the freshly hydrated PostgreSQL `MEMORY.md`
//! (rebuilding `MEMORY.md` from topic files if needed) before the bundle tar is synced
//! into the sandbox.

use std::path::Path;

use anyagents_core::{Agent, Store};

const PROMPT_FILES: &[&str] = &[
    "SYSTEM_PROMPT.md",
    "AGENTS.md",
    "opencode/AGENTS.md",
    ".config/opencode/AGENTS.md",
    "CLAUDE.md",
    ".claude/CLAUDE.md",
    ".gemini/config/GEMINI.md",
    "muse/AGENTS.md",
    ".config/muse/AGENTS.md",
    ".omp/agent/APPEND_SYSTEM.md",
    ".omp/agent/AGENTS.md",
];

/// Refresh the `<learned_memory>...</learned_memory>` section inside every hydrated
/// prompt file under `config_dir` using the freshly hydrated `memory/MEMORY.md`
/// under `<data_dir>/agents/<agent_id>/memory/`.
pub(crate) fn refresh_hydrated_prompt_learned_memory(
    store: &Store,
    agent: &Agent,
    config_dir: &Path,
) -> usize {
    if !store.is_postgres() {
        return 0;
    }
    let agent_root = store.data_dir().join("agents").join(agent.id.as_str());
    refresh_prompt_files_from_agent_root(
        &agent_root,
        config_dir,
        &agent.name,
        agent.role.as_deref(),
    )
}

pub(crate) fn refresh_prompt_files_from_agent_root(
    agent_root: &Path,
    config_dir: &Path,
    agent_name: &str,
    role: Option<&str>,
) -> usize {
    let mem_dir = agent_root.join("memory");
    if !mem_dir.is_dir() {
        return 0;
    }
    let mem_md_path = mem_dir.join("MEMORY.md");
    let mut memory_text = std::fs::read_to_string(&mem_md_path).unwrap_or_default();
    if memory_text.trim().is_empty()
        && let Ok(rebuilt) =
            anyagents_core::learned_memory::rebuild_memory_index(agent_root, agent_name, role)
    {
        memory_text = rebuilt;
    }
    if memory_text.trim().is_empty() {
        return 0;
    }
    let fresh_section = anyagents_core::learned_memory::bounded_section(&memory_text);
    let mut updated = 0usize;
    for rel in PROMPT_FILES {
        let path = config_dir.join(rel);
        let Ok(existing) = std::fs::read_to_string(&path) else {
            continue;
        };
        let next = splice_learned_memory_section(&existing, &fresh_section);
        if next != existing && std::fs::write(&path, next.as_bytes()).is_ok() {
            updated += 1;
        }
    }
    updated
}

/// Replace an existing `<learned_memory>...</learned_memory>` block (case-insensitive
/// tag match) with `fresh_section`, or append `fresh_section` cleanly when absent.
pub(crate) fn splice_learned_memory_section(doc: &str, fresh_section: &str) -> String {
    let lower = doc.to_ascii_lowercase();
    if let Some(start) = lower.find("<learned_memory>")
        && let Some(rel_end) = lower[start..].find("</learned_memory>")
    {
        let mut end = start + rel_end + "</learned_memory>".len();
        if doc[end..].starts_with('\n') {
            end += 1;
        }
        let mut out = String::with_capacity(doc.len() + fresh_section.len());
        out.push_str(&doc[..start]);
        out.push_str(fresh_section);
        out.push_str(&doc[end..]);
        return out;
    }
    let mut out = String::with_capacity(doc.len() + fresh_section.len() + 2);
    out.push_str(doc);
    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
    if !out.is_empty() {
        out.push('\n');
    }
    out.push_str(fresh_section);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replaces_stale_learned_memory_in_hydrated_prompt_files_from_fresh_memory_md() {
        let tmp = tempfile::tempdir().unwrap();
        let agent_root = tmp.path().join("agents/agt_test");
        let cfg_dir = agent_root.join("config");
        let mem_dir = agent_root.join("memory");
        std::fs::create_dir_all(cfg_dir.join(".gemini/config")).unwrap();
        std::fs::create_dir_all(&mem_dir).unwrap();

        let stale_prompt = concat!(
            "# Agent System Prompt\n\n",
            "<learned_memory>\n",
            "# Learned memory (session snapshot)\n",
            "- old pre-migration fact\n",
            "</learned_memory>\n\n",
            "## Tail Section\n",
        );
        std::fs::write(cfg_dir.join("SYSTEM_PROMPT.md"), stale_prompt).unwrap();
        std::fs::write(cfg_dir.join("CLAUDE.md"), stale_prompt).unwrap();
        std::fs::write(cfg_dir.join(".gemini/config/GEMINI.md"), stale_prompt).unwrap();

        std::fs::write(
            mem_dir.join("MEMORY.md"),
            "- [pg-topic](pg-topic.md): fresh PostgreSQL fact\n",
        )
        .unwrap();

        let updated =
            refresh_prompt_files_from_agent_root(&agent_root, &cfg_dir, "test-agent", None);
        assert_eq!(updated, 3);

        for rel in ["SYSTEM_PROMPT.md", "CLAUDE.md", ".gemini/config/GEMINI.md"] {
            let content = std::fs::read_to_string(cfg_dir.join(rel)).unwrap();
            assert!(
                content.contains("fresh PostgreSQL fact"),
                "{rel} should contain fresh PG memory"
            );
            assert!(
                !content.contains("old pre-migration fact"),
                "{rel} should not retain stale pre-migration memory"
            );
            assert!(
                content.contains("## Tail Section"),
                "{rel} should preserve trailing prompt sections"
            );
            assert_eq!(content.matches("<learned_memory>").count(), 1);
            assert_eq!(content.matches("</learned_memory>").count(), 1);
        }
    }
}
