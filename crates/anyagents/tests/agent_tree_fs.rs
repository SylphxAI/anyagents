//! Keeps raw filesystem calls off the trees an agent's seat can write
//! (`agents/<id>/{config,memory,skills,sessions,work}`, `browser-profiles`).
//! Server code reaches them through `anyagents_core::agent_tree::AgentTree`,
//! which never follows a link below the agent root (ADR 0010 D3).
//!
//! Two rules, both on source text, both failing CI:
//!
//! 1. Strict modules, whose whole job is an agent tree, make no raw
//!    `std::fs` path call at all, unless the line (or the one above) says
//!    `fs-ok:` and why the path is not in an agent tree.
//! 2. Everywhere else in the server, a statement that makes a raw path call
//!    and names an agent tree path (`config_dir`, `.join("memory")`, …) is
//!    counted per file against `agent_tree_fs.counts`. A new one fails; route
//!    it through `AgentTree`. When a file's count drops, record the lower
//!    number, so the ratchet only ever tightens. This is a name heuristic: it
//!    sees a path built where it is used, not one passed in from elsewhere.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Modules that work on agent trees only.
const STRICT: &[&str] = &[
    "crates/anyagents-core/src/agent_tree.rs",
    "crates/anyagents-core/src/beneath.rs",
    "crates/anyagents-core/src/host_session.rs",
    "crates/anyagents-core/src/learned_memory.rs",
    "crates/anyagents/src/mcp_config.rs",
];

/// Raw path calls: each resolves a name, following links, as the server.
const RAW: &[&str] = &[
    "fs::read(",
    "fs::read_to_string(",
    "fs::write(",
    "fs::create_dir(",
    "fs::create_dir_all(",
    "fs::remove_file(",
    "fs::remove_dir(",
    "fs::remove_dir_all(",
    "fs::rename(",
    "fs::copy(",
    "fs::set_permissions(",
    "fs::read_dir(",
    "fs::read_link(",
    "fs::hard_link(",
    "fs::metadata(",
    "File::open(",
    "File::create(",
    "OpenOptions::new(",
    "DirBuilder::new(",
    "atomic_write_nonempty_file(",
    "atomic_write_file(",
    "atomic_write_in_existing_dir(",
    "learned_memory::atomic_write(",
    "write_required_file(",
    "unix::fs::symlink(",
];

/// Names that build a path into an agent tree.
const TREE: &[&str] = &[
    "agent_root",
    "config_dir",
    "agent_config_dir(",
    "agent_work_dir(",
    "target_config",
    ".join(\"config\")",
    ".join(\"memory\")",
    ".join(\"skills\")",
    ".join(\"sessions\")",
    ".join(\"work\")",
    "\"config/",
    "\"memory/",
    "\"sessions/",
    "\"skills/",
    "CAPABILITY_FILE",
    "mcp-capability",
    "SYSTEM_PROMPT",
    "registry.json",
    "profile_dir(",
    "browser-profiles",
];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

fn sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if path.is_dir() {
            if name != "tests" {
                sources(&path, out);
            }
        } else if name.ends_with(".rs") && !name.contains("test") && !name.contains("benchmark") {
            out.push(path);
        }
    }
}

/// The non-test part of a source file: every inline `#[cfg(test)] mod … { … }`
/// removed (braces counted naively, which source formatting keeps balanced).
fn production(text: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let mut out = String::new();
    let mut index = 0;
    while index < lines.len() {
        let next = lines.get(index + 1).map(|l| l.trim_start()).unwrap_or("");
        let inline_test_mod = lines[index].trim() == "#[cfg(test)]"
            && (next.starts_with("mod ")
                || next.starts_with("pub mod ")
                || next.starts_with("pub(crate) mod "))
            && next.trim_end().ends_with('{');
        if inline_test_mod {
            let mut depth = 0i64;
            index += 1;
            while index < lines.len() {
                let code = lines[index].split("//").next().unwrap_or("");
                depth += code.matches('{').count() as i64 - code.matches('}').count() as i64;
                index += 1;
                if depth <= 0 {
                    break;
                }
            }
            continue;
        }
        out.push_str(lines[index]);
        out.push('\n');
        index += 1;
    }
    out
}

/// Code statements, comments removed, split at `;`, `{` and `}`.
fn statements(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for line in text.lines() {
        let code = line.split("//").next().unwrap_or("");
        for ch in code.chars() {
            current.push(ch);
            if matches!(ch, ';' | '{' | '}') {
                out.push(std::mem::take(&mut current));
            }
        }
        current.push(' ');
    }
    out.push(current);
    out
}

fn raw(text: &str) -> bool {
    RAW.iter().any(|call| text.contains(call))
}

#[test]
fn strict_agent_tree_modules_make_no_raw_path_calls() {
    let root = repo_root();
    let mut found = Vec::new();
    for file in STRICT {
        let text = std::fs::read_to_string(root.join(file)).unwrap();
        let production = production(&text);
        let lines: Vec<&str> = production.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            let code = line.split("//").next().unwrap_or("");
            let excused = line.contains("fs-ok:")
                || index
                    .checked_sub(1)
                    .is_some_and(|above| lines[above].contains("fs-ok:"));
            if raw(code) && !excused {
                found.push(format!("{file}:{}: {}", index + 1, line.trim()));
            }
        }
    }
    assert!(
        found.is_empty(),
        "raw path calls in agent-tree modules; use AgentTree/Dir, or say `// fs-ok: <why>` \
         when the path is not in an agent tree:\n{}",
        found.join("\n")
    );
}

#[test]
fn raw_path_calls_on_agent_trees_only_ever_decrease() {
    let root = repo_root();
    let mut files = Vec::new();
    sources(&root.join("crates/anyagents/src"), &mut files);
    sources(&root.join("crates/anyagents-core/src"), &mut files);
    let mut counts = BTreeMap::new();
    for file in files {
        let relative = file
            .strip_prefix(&root)
            .unwrap()
            .to_string_lossy()
            .into_owned();
        if STRICT.contains(&relative.as_str()) {
            continue;
        }
        let text = std::fs::read_to_string(&file).unwrap();
        let count = statements(&production(&text))
            .iter()
            .filter(|statement| raw(statement) && TREE.iter().any(|name| statement.contains(name)))
            .count();
        if count > 0 {
            counts.insert(relative, count);
        }
    }
    let recorded_path = root.join("crates/anyagents/tests/agent_tree_fs.counts");
    let recorded: BTreeMap<String, usize> = std::fs::read_to_string(&recorded_path)
        .unwrap()
        .lines()
        .filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
        .map(|line| {
            let (path, count) = line.rsplit_once(' ').unwrap();
            (path.trim().to_string(), count.trim().parse().unwrap())
        })
        .collect();
    let mut grown = Vec::new();
    let mut shrunk = Vec::new();
    for (file, count) in &counts {
        let before = recorded.get(file).copied().unwrap_or(0);
        if *count > before {
            grown.push(format!("{file}: {before} -> {count}"));
        }
    }
    for (file, before) in &recorded {
        let now = counts.get(file).copied().unwrap_or(0);
        if now < *before {
            shrunk.push(format!("{file} {now}"));
        }
    }
    assert!(
        grown.is_empty(),
        "new raw path calls on agent trees; route them through AgentTree:\n{}",
        grown.join("\n")
    );
    assert!(
        shrunk.is_empty(),
        "fewer raw path calls on agent trees; record the new counts in {}:\n{}",
        recorded_path.display(),
        shrunk.join("\n")
    );
}
