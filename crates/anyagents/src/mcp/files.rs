//! Workspace file operations, atomic edits, V4A patching, globbing, and grep.

use super::*;

pub(super) const DEFAULT_READ_MAX_BYTES: usize = 46_080;
pub(super) const HARD_READ_MAX_BYTES: usize = 262_144;
pub(super) const DEFAULT_GLOB_MAX_RESULTS: usize = 200;
pub(super) const HARD_GLOB_MAX_RESULTS: usize = 5_000;
pub(super) const DEFAULT_GREP_MAX_RESULTS: usize = 100;
pub(super) const HARD_GREP_MAX_RESULTS: usize = 2_000;
pub(super) const DEFAULT_LIST_MAX_RESULTS: usize = 500;
pub(super) const HARD_LIST_MAX_RESULTS: usize = 5_000;

pub(super) const DEFAULT_IGNORED_DIRS: &[&str] = &[
    ".git",
    "target",
    "node_modules",
    ".next",
    "dist",
    ".venv",
    "__pycache__",
];

pub(super) fn normalize_lexical_path(path: &Path) -> PathBuf {
    use std::path::Component;
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::Prefix(p) => out.push(p.as_os_str()),
            Component::RootDir => out.push(comp.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if matches!(out.components().next_back(), Some(Component::Normal(_))) {
                    out.pop();
                } else if !out.is_absolute() {
                    out.push("..");
                }
            }
            Component::Normal(c) => out.push(c),
        }
    }
    if out.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        out
    }
}

pub(super) fn resolve_workspace_path(
    base: &Path,
    raw: Option<&str>,
    required: bool,
) -> Result<PathBuf, String> {
    let Some(raw_str) = raw else {
        if required {
            return Err("`path` is required for this `files` action".to_string());
        }
        return Ok(normalize_lexical_path(base));
    };
    if raw_str.contains('\0') {
        return Err("`path` must not contain null bytes".to_string());
    }
    let trimmed = raw_str.trim();
    if trimmed.is_empty() {
        if required {
            return Err("`path` must not be empty".to_string());
        }
        return Ok(normalize_lexical_path(base));
    }
    let candidate = Path::new(trimmed);
    let joined = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        base.join(candidate)
    };
    Ok(normalize_lexical_path(&joined))
}

pub(super) fn execute_files_tool_with_media_backend(
    backend: &dyn agent_tools::WorkspaceBackend,
    base_dir: &Path,
    args: FilesArgs,
    limits: CodeLimits,
) -> Result<(serde_json::Value, Vec<rmcp::model::ContentBlock>), String> {
    if backend.is_remote() {
        return execute_remote_files_tool_with_media(backend, args, limits);
    }
    execute_files_tool_with_media(base_dir, args, limits)
}

pub(super) fn execute_remote_files_tool_with_media(
    backend: &dyn agent_tools::WorkspaceBackend,
    args: FilesArgs,
    limits: CodeLimits,
) -> Result<(serde_json::Value, Vec<rmcp::model::ContentBlock>), String> {
    let action = args
        .action
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_ascii_lowercase)
        .unwrap_or_else(|| {
            if args.cmd.is_some() {
                "exec".to_string()
            } else if args.patch.is_some() {
                "patch".to_string()
            } else if !args.edits.is_empty() {
                "multi_edit".to_string()
            } else if args.target.is_some() {
                "edit".to_string()
            } else if args.content.is_some() {
                "write".to_string()
            } else if args.query.is_some() {
                "grep".to_string()
            } else if args.pattern.is_some() || args.glob.is_some() {
                "glob".to_string()
            } else {
                "read".to_string()
            }
        });

    match action.as_str() {
        "read" | "cat" | "view" => {
            let (resolved_str, full_bytes) = backend.read_file_bytes(args.path.as_deref())?;
            let resolved_path = Path::new(&resolved_str);
            if let Some(mime_type) = detect_multimedia_mime(resolved_path, &full_bytes) {
                let sha256 = sha256_hex(&full_bytes);
                return read_multimedia_file(resolved_path, &full_bytes, mime_type, sha256, &args);
            }
            let sha256 = sha256_hex(&full_bytes);
            let total_bytes = full_bytes.len();
            let text = String::from_utf8_lossy(&full_bytes);
            let all_lines: Vec<&str> = text.lines().collect();
            let total_lines = all_lines.len();
            let max_bytes = args
                .max_bytes
                .unwrap_or(DEFAULT_READ_MAX_BYTES)
                .clamp(1, HARD_READ_MAX_BYTES);
            let byte_offset = args.byte_offset.unwrap_or(0);
            if total_lines == 0 {
                return Ok((
                    serde_json::json!({
                        "action": "read",
                        "path": resolved_str,
                        "content": "",
                        "numbered": "",
                        "start_line": 0,
                        "end_line": 0,
                        "total_lines": 0,
                        "bytes_read": 0,
                        "total_bytes": total_bytes,
                        "byte_offset": byte_offset,
                        "truncated": false,
                        "sha256": sha256,
                    }),
                    Vec::new(),
                ));
            }
            let start_line = args.start_line.unwrap_or(1).max(1);
            if start_line > total_lines {
                return Ok((
                    serde_json::json!({
                        "action": "read",
                        "path": resolved_str,
                        "content": "",
                        "numbered": "",
                        "start_line": start_line,
                        "end_line": total_lines,
                        "total_lines": total_lines,
                        "bytes_read": 0,
                        "total_bytes": total_bytes,
                        "byte_offset": byte_offset,
                        "truncated": false,
                        "sha256": sha256,
                    }),
                    Vec::new(),
                ));
            }
            let requested_end = args
                .end_line
                .unwrap_or(total_lines)
                .min(total_lines)
                .max(start_line);
            let selected_lines = &all_lines[start_line - 1..requested_end];
            let joined = selected_lines.join("\n");
            let start_byte = floor_char_boundary_str(&joined, byte_offset.min(joined.len()));
            let after_offset = &joined[start_byte..];
            let end_byte = floor_char_boundary_str(after_offset, max_bytes);
            let sliced = &after_offset[..end_byte];
            let truncated = end_byte < after_offset.len() || requested_end < total_lines;
            let offset_lines = joined[..start_byte].bytes().filter(|&b| b == b'\n').count();
            let first_line_no = start_line + offset_lines;
            let sliced_lines: Vec<&str> = sliced.lines().collect();
            let actual_end_line = if sliced_lines.is_empty() {
                first_line_no
            } else {
                first_line_no + sliced_lines.len() - 1
            };
            let mut numbered = String::with_capacity(sliced.len() + sliced_lines.len() * 8);
            for (idx, line) in sliced_lines.iter().enumerate() {
                if idx > 0 {
                    numbered.push('\n');
                }
                numbered.push_str(&format!("{}: {line}", first_line_no + idx));
            }
            Ok((
                serde_json::json!({
                    "action": "read",
                    "path": resolved_str,
                    "content": sliced,
                    "numbered": numbered,
                    "start_line": first_line_no,
                    "end_line": actual_end_line,
                    "total_lines": total_lines,
                    "bytes_read": sliced.len(),
                    "total_bytes": total_bytes,
                    "byte_offset": start_byte,
                    "truncated": truncated,
                    "sha256": sha256,
                }),
                Vec::new(),
            ))
        }
        "write" | "create" => {
            let content = args.content.unwrap_or_default();
            let append = args.append.unwrap_or(false);
            let overwrite = args.overwrite.unwrap_or(true);
            let (resolved_str, total_bytes, sha256) = backend.write_file_bytes(
                args.path.as_deref(),
                content.as_bytes(),
                append,
                overwrite,
            )?;
            Ok((
                serde_json::json!({
                    "action": "write",
                    "path": resolved_str,
                    "bytes_written": content.len(),
                    "total_bytes": total_bytes,
                    "append": append,
                    "sha256": sha256,
                }),
                Vec::new(),
            ))
        }
        "edit" | "multi_edit" | "replace" => {
            let mut specs = Vec::new();
            if let Some(target) = args.target {
                specs.push(FileEditSpec {
                    target,
                    replacement: args.replacement.unwrap_or_default(),
                    replace_all: args.replace_all,
                });
            }
            specs.extend(args.edits);
            if specs.is_empty() {
                return Err(
                    "`files` edit requires `target` (and optional `replacement`) or non-empty `edits`"
                        .to_string(),
                );
            }
            let (resolved_str, existing_bytes) = backend.read_file_bytes(args.path.as_deref())?;
            let existing = String::from_utf8(existing_bytes)
                .map_err(|e| format!("cannot decode {resolved_str} as UTF-8 for edit: {e}"))?;
            let (updated, replacements_made, lines_added, lines_removed) = apply_atomic_edits(
                &existing,
                &specs,
                args.replace_all.unwrap_or(true),
                Path::new(&resolved_str),
            )?;
            backend.write_file_bytes(args.path.as_deref(), updated.as_bytes(), false, true)?;
            let sha256 = sha256_hex(updated.as_bytes());
            Ok((
                serde_json::json!({
                    "action": if action == "multi_edit" { "multi_edit" } else { "edit" },
                    "path": resolved_str,
                    "replacements_made": replacements_made,
                    "lines_added": lines_added,
                    "lines_removed": lines_removed,
                    "sha256": sha256,
                }),
                Vec::new(),
            ))
        }
        "list" | "ls" | "list_dir" => {
            let include_hidden = args.include_hidden.unwrap_or(true);
            let max_results = args
                .max_results
                .unwrap_or(DEFAULT_LIST_MAX_RESULTS)
                .clamp(1, HARD_LIST_MAX_RESULTS);
            let val = backend.list_dir(args.path.as_deref(), include_hidden, max_results)?;
            Ok((val, Vec::new()))
        }
        "exec" | "bash" | "sh" | "command" => {
            if std::env::var("ANYAGENTS_DISABLE_WORKSPACE_EXEC")
                .ok()
                .is_some_and(|v| {
                    matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes")
                })
            {
                return Err(
                    "workspace.exec is disabled for this agent (ANYAGENTS_DISABLE_WORKSPACE_EXEC=1); delegate command execution to a subagent".to_string(),
                );
            }
            let cmd_str = args
                .cmd
                .as_deref()
                .or(args.content.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "`workspace.exec` requires a non-empty `cmd`".to_string())?;
            let raw_cwd = args.cwd.as_deref().or(args.path.as_deref());
            let resolved_cwd = backend.resolve_path(raw_cwd, false)?;
            let timeout_ms = limits.resolve(args.timeout_ms);
            let started = std::time::Instant::now();
            let out = backend.exec(cmd_str, Some(&resolved_cwd), Some(timeout_ms))?;
            let duration_ms = started.elapsed().as_millis() as u64;
            let mut result = serde_json::json!({
                "action": "exec",
                "cmd": cmd_str,
                "cwd": resolved_cwd,
                "exit_code": out.exit_code,
                "stdout": out.stdout,
                "stderr": out.stderr,
                "duration_ms": duration_ms,
            });
            let asked = args.requested_timeout_ms.or(args.timeout_ms);
            let note = match backend.kind() {
                "docker" => None,
                _ => code_contract::exec_clamp_note(
                    asked,
                    timeout_ms.min(agent_tools::REMOTE_EXEC_MAX_TIMEOUT_MS),
                    &format!(
                        "your remote sandbox stops a command at {}ms",
                        agent_tools::REMOTE_EXEC_MAX_TIMEOUT_MS
                    ),
                ),
            }
            .or_else(|| {
                code_contract::exec_clamp_note(asked, timeout_ms, &limits.ceiling_reason())
            });
            if let Some(note) = note {
                result["note"] = note.into();
            }
            Ok((result, Vec::new()))
        }
        "patch" | "apply_patch" => {
            let patch_text = args
                .patch
                .or(args.content)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "`files` patch requires non-empty `patch`".to_string())?;
            let val = apply_workspace_patch_with(&RemotePatchFs { backend }, &patch_text)?;
            Ok((val, Vec::new()))
        }
        "glob" => {
            let val = execute_remote_glob(backend, args)?;
            Ok((val, Vec::new()))
        }
        "grep" | "search" => {
            let val = execute_remote_grep(backend, args)?;
            Ok((val, Vec::new()))
        }
        other => Err(format!(
            "remote sandbox `files` action `{other}` is not supported directly; use `read`, `write`, `edit`, `multi_edit`, `patch`, `list`, `glob`, `grep`, or `exec`"
        )),
    }
}

pub(super) fn execute_remote_glob(
    backend: &dyn agent_tools::WorkspaceBackend,
    args: FilesArgs,
) -> Result<serde_json::Value, String> {
    let mut raw_pattern = args
        .pattern
        .or(args.glob)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let mut raw_path = args
        .path
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    if raw_pattern.is_none()
        && let Some(ref p) = raw_path
        && (p.contains('*') || p.contains('?') || p.contains('[') || p.contains('{'))
    {
        raw_pattern = Some(p.clone());
        raw_path = None;
    }
    let search_root = backend.resolve_path(raw_path.as_deref(), false)?;
    let pattern = raw_pattern.unwrap_or_else(|| "**/*".to_string());
    let clean_pat = pattern.strip_prefix("./").unwrap_or(&pattern);
    let max_results = args
        .max_results
        .unwrap_or(DEFAULT_GLOB_MAX_RESULTS)
        .clamp(1, HARD_GLOB_MAX_RESULTS);
    let include_hidden = args
        .include_hidden
        .unwrap_or_else(|| clean_pat.starts_with('.') || clean_pat.contains("/."));
    let include_ignored = args.include_ignored.unwrap_or(false)
        || DEFAULT_IGNORED_DIRS.iter().any(|d| clean_pat.contains(d));

    let candidate_files =
        remote_collect_workspace_files(backend, &search_root, include_hidden, include_ignored)?;
    let mut matched = Vec::new();
    let mut truncated = false;
    let pat_has_slash = clean_pat.contains('/');
    for rel in candidate_files {
        let file_name = Path::new(&rel)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(&rel);
        let is_match =
            glob_matches(clean_pat, &rel) || (!pat_has_slash && glob_matches(clean_pat, file_name));
        if is_match {
            if matched.len() >= max_results {
                truncated = true;
                break;
            }
            matched.push(rel);
        }
    }
    let count = matched.len();
    Ok(serde_json::json!({
        "action": "glob",
        "base_dir": search_root,
        "pattern": pattern,
        "files": matched,
        "matches": matched,
        "count": count,
        "truncated": truncated,
    }))
}

pub(super) fn execute_remote_grep(
    backend: &dyn agent_tools::WorkspaceBackend,
    args: FilesArgs,
) -> Result<serde_json::Value, String> {
    let query = args
        .query
        .or(args.pattern)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "`files` grep requires a non-empty `query`".to_string())?;
    let target_path = backend.resolve_path(args.path.as_deref(), false)?;
    let max_results = args
        .max_results
        .unwrap_or(DEFAULT_GREP_MAX_RESULTS)
        .clamp(1, HARD_GREP_MAX_RESULTS);
    let context_lines = args.context_lines.unwrap_or(0).min(10);
    let matcher = GrepMatcher::compile(
        &query,
        args.case_insensitive.unwrap_or(false),
        args.literal.unwrap_or(false),
    );
    let file_glob = args
        .glob
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.strip_prefix("./").unwrap_or(s));

    let base_root = backend.root_display();
    let files_to_search: Vec<(String, String)> = match backend.list_dir(Some(&target_path), true, 1)
    {
        Err(_) if backend.read_file_bytes(Some(&target_path)).is_ok() => {
            let disp = remote_rel_display(&base_root, &target_path);
            vec![(target_path, disp)]
        }
        Err(err) => {
            if backend.read_file_bytes(Some(&target_path)).is_ok() {
                let disp = remote_rel_display(&base_root, &target_path);
                vec![(target_path, disp)]
            } else {
                return Err(format!(
                    "grep target path `{target_path}` does not exist: {err}"
                ));
            }
        }
        Ok(_) => {
            let include_hidden = args.include_hidden.unwrap_or(false);
            let include_ignored = args.include_ignored.unwrap_or(false);
            let rels = remote_collect_workspace_files(
                backend,
                &target_path,
                include_hidden,
                include_ignored,
            )?;
            rels.into_iter()
                .filter(|rel| {
                    if let Some(g) = file_glob {
                        let fname = Path::new(rel)
                            .file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or(rel);
                        glob_matches(g, rel) || (!g.contains('/') && glob_matches(g, fname))
                    } else {
                        true
                    }
                })
                .map(|rel| {
                    let abs = remote_join_path(&target_path, &rel);
                    let disp = remote_rel_display(&base_root, &abs);
                    (abs, disp)
                })
                .collect()
        }
    };

    let mut matches = Vec::new();
    let mut truncated = false;
    for (abs_path, disp_path) in files_to_search {
        let Ok((_, bytes)) = backend.read_file_bytes(Some(&abs_path)) else {
            continue;
        };
        if collect_grep_hits_from_bytes(
            &bytes,
            &disp_path,
            &matcher,
            context_lines,
            max_results,
            &mut matches,
        ) {
            truncated = true;
            break;
        }
    }
    let count = matches.len();
    Ok(serde_json::json!({
        "action": "grep",
        "query": query,
        "matches": matches,
        "count": count,
        "truncated": truncated,
    }))
}

pub(super) fn collect_grep_hits_from_bytes(
    bytes: &[u8],
    disp_path: &str,
    matcher: &GrepMatcher,
    context_lines: usize,
    max_results: usize,
    matches: &mut Vec<serde_json::Value>,
) -> bool {
    let probe_len = bytes.len().min(8192);
    if bytes[..probe_len].contains(&0) {
        return false;
    }
    let text = String::from_utf8_lossy(bytes);
    let lines: Vec<&str> = text.lines().collect();
    for (idx, line) in lines.iter().enumerate() {
        if matcher.is_match(line) {
            if matches.len() >= max_results {
                return true;
            }
            let mut hit = serde_json::json!({
                "path": disp_path,
                "line": idx + 1,
                "text": line,
            });
            if context_lines > 0
                && let Some(obj) = hit.as_object_mut()
            {
                let before_start = idx.saturating_sub(context_lines);
                let after_end = (idx + 1 + context_lines).min(lines.len());
                let before: Vec<&str> = lines[before_start..idx].to_vec();
                let after: Vec<&str> = lines[idx + 1..after_end].to_vec();
                obj.insert("context_before".to_string(), serde_json::json!(before));
                obj.insert("context_after".to_string(), serde_json::json!(after));
            }
            matches.push(hit);
        }
    }
    false
}

pub(super) fn remote_collect_workspace_files(
    backend: &dyn agent_tools::WorkspaceBackend,
    search_root: &str,
    include_hidden: bool,
    include_ignored: bool,
) -> Result<Vec<String>, String> {
    let gitignore_patterns = if include_ignored {
        Vec::new()
    } else {
        let gi_path = remote_join_path(search_root, ".gitignore");
        backend
            .read_file_bytes(Some(&gi_path))
            .ok()
            .map(|(_, bytes)| parse_gitignore_patterns(&String::from_utf8_lossy(&bytes)))
            .unwrap_or_default()
    };

    // Fast path: run `find . -type f` inside the remote sandbox to collect all files
    // in one round-trip, falling back to recursive `list_dir` walk when `exec` does not
    // return valid `find` output (e.g., restricted mock backends).
    let find_cmd = "find . -maxdepth 16 -type f";
    let mut candidates = if let Ok(out) = backend.exec(find_cmd, Some(search_root), Some(20_000))
        && out.exit_code == 0
        && out
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .any(|l| l.trim().starts_with("./"))
        && out
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .all(|l| l.trim().starts_with("./"))
    {
        out.stdout
            .lines()
            .filter_map(|l| l.trim().strip_prefix("./"))
            .filter(|rel| !rel.is_empty())
            .filter(|rel| {
                let parts: Vec<&str> = rel.split('/').collect();
                if !include_hidden && parts.iter().any(|p| p.starts_with('.')) {
                    return false;
                }
                if !include_ignored
                    && parts
                        .iter()
                        .take(parts.len().saturating_sub(1))
                        .any(|d| DEFAULT_IGNORED_DIRS.contains(d))
                {
                    return false;
                }
                let fname = parts.last().copied().unwrap_or("");
                include_ignored || !is_gitignored(rel, fname, &gitignore_patterns)
            })
            .map(str::to_string)
            .collect()
    } else {
        let mut collected = Vec::new();
        remote_walk_dir_recursive(
            backend,
            search_root,
            "",
            include_hidden,
            include_ignored,
            &gitignore_patterns,
            &mut collected,
            0,
        )?;
        collected
    };

    candidates.sort();
    Ok(candidates)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn remote_walk_dir_recursive(
    backend: &dyn agent_tools::WorkspaceBackend,
    abs_dir: &str,
    rel_prefix: &str,
    include_hidden: bool,
    include_ignored: bool,
    gitignore_patterns: &[String],
    out: &mut Vec<String>,
    depth: usize,
) -> Result<(), String> {
    if depth > 16 {
        return Ok(());
    }
    let val = backend.list_dir(Some(abs_dir), include_hidden, HARD_GLOB_MAX_RESULTS)?;
    let Some(entries) = val.get("entries").and_then(serde_json::Value::as_array) else {
        return Ok(());
    };
    for entry in entries {
        let Some(name) = entry.get("name").and_then(serde_json::Value::as_str) else {
            continue;
        };
        if name == "." || name == ".." || name.is_empty() {
            continue;
        }
        if !include_hidden && name.starts_with('.') {
            continue;
        }
        let kind = entry
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("file");
        let rel = if rel_prefix.is_empty() {
            name.to_string()
        } else {
            format!("{rel_prefix}/{name}")
        };
        if kind == "dir" {
            if !include_ignored && DEFAULT_IGNORED_DIRS.contains(&name) {
                continue;
            }
            if !include_ignored && is_gitignored(&rel, name, gitignore_patterns) {
                continue;
            }
            let child_abs = remote_join_path(abs_dir, name);
            remote_walk_dir_recursive(
                backend,
                &child_abs,
                &rel,
                include_hidden,
                include_ignored,
                gitignore_patterns,
                out,
                depth + 1,
            )?;
        } else {
            if !include_ignored && is_gitignored(&rel, name, gitignore_patterns) {
                continue;
            }
            out.push(rel);
        }
    }
    Ok(())
}

pub(super) fn remote_join_path(base: &str, rel: &str) -> String {
    let base_clean = base.trim_end_matches('/');
    let rel_clean = rel.trim_start_matches('/');
    if base_clean.is_empty() {
        format!("/{rel_clean}")
    } else if rel_clean.is_empty() {
        base_clean.to_string()
    } else {
        format!("{base_clean}/{rel_clean}")
    }
}

pub(super) fn remote_rel_display(base_root: &str, abs_path: &str) -> String {
    let root_clean = base_root.trim_end_matches('/');
    if root_clean.is_empty() {
        abs_path.to_string()
    } else if let Some(stripped) = abs_path
        .strip_prefix(root_clean)
        .and_then(|s| s.strip_prefix('/'))
        .filter(|s| !s.is_empty())
    {
        stripped.to_string()
    } else {
        abs_path.to_string()
    }
}

pub(super) fn shell_quote_remote(raw: &str) -> String {
    format!("'{}'", raw.replace('\'', "'\"'\"'"))
}

pub(super) fn execute_files_tool_with_media(
    base_dir: &Path,
    args: FilesArgs,
    limits: CodeLimits,
) -> Result<(serde_json::Value, Vec<rmcp::model::ContentBlock>), String> {
    let is_read_action = matches!(
        args.action
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("read" | "cat" | "view")
    ) || (args.action.as_deref().is_none_or(|s| s.trim().is_empty())
        && args.cmd.is_none()
        && args.patch.is_none()
        && args.edits.is_empty()
        && args.target.is_none()
        && args.content.is_none()
        && args.query.is_none()
        && args.pattern.is_none()
        && args.glob.is_none());

    if is_read_action {
        let resolved = resolve_workspace_path(base_dir, args.path.as_deref(), true)?;
        let meta = std::fs::metadata(&resolved)
            .map_err(|e| format!("cannot read {}: {e}", resolved.display()))?;
        if meta.is_dir() {
            return Err(format!(
                "{} is a directory; use `action: \"list\"` or `action: \"glob\"` to list files",
                resolved.display()
            ));
        }
        let full_bytes = std::fs::read(&resolved)
            .map_err(|e| format!("cannot read {}: {e}", resolved.display()))?;
        if let Some(mime_type) = detect_multimedia_mime(&resolved, &full_bytes) {
            let sha256 = sha256_hex(&full_bytes);
            return read_multimedia_file(&resolved, &full_bytes, mime_type, sha256, &args);
        }
    }

    let val = execute_files_tool(base_dir, args, limits)?;
    Ok((val, Vec::new()))
}

pub(super) fn execute_files_tool(
    base_dir: &Path,
    args: FilesArgs,
    limits: CodeLimits,
) -> Result<serde_json::Value, String> {
    let action = args
        .action
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_ascii_lowercase)
        .unwrap_or_else(|| {
            if args.cmd.is_some() {
                "exec".to_string()
            } else if args.patch.is_some() {
                "patch".to_string()
            } else if !args.edits.is_empty() {
                "multi_edit".to_string()
            } else if args.target.is_some() {
                "edit".to_string()
            } else if args.content.is_some() {
                "write".to_string()
            } else if args.query.is_some() {
                "grep".to_string()
            } else if args.pattern.is_some() || args.glob.is_some() {
                "glob".to_string()
            } else {
                "read".to_string()
            }
        });

    match action.as_str() {
        "read" | "cat" | "view" => {
            let resolved = resolve_workspace_path(base_dir, args.path.as_deref(), true)?;
            let meta = std::fs::metadata(&resolved)
                .map_err(|e| format!("cannot read {}: {e}", resolved.display()))?;
            if meta.is_dir() {
                return Err(format!(
                    "{} is a directory; use `action: \"list\"` or `action: \"glob\"` to list files",
                    resolved.display()
                ));
            }
            let full_bytes = std::fs::read(&resolved)
                .map_err(|e| format!("cannot read {}: {e}", resolved.display()))?;
            let sha256 = sha256_hex(&full_bytes);
            let total_bytes = full_bytes.len();
            let text = String::from_utf8_lossy(&full_bytes);
            let all_lines: Vec<&str> = text.lines().collect();
            let total_lines = all_lines.len();

            let max_bytes = args
                .max_bytes
                .unwrap_or(DEFAULT_READ_MAX_BYTES)
                .clamp(1, HARD_READ_MAX_BYTES);
            let byte_offset = args.byte_offset.unwrap_or(0);

            if total_lines == 0 {
                return Ok(serde_json::json!({
                    "action": "read",
                    "path": resolved.display().to_string(),
                    "content": "",
                    "numbered": "",
                    "start_line": 0,
                    "end_line": 0,
                    "total_lines": 0,
                    "bytes_read": 0,
                    "total_bytes": total_bytes,
                    "byte_offset": byte_offset,
                    "truncated": false,
                    "sha256": sha256,
                }));
            }

            let start_line = args.start_line.unwrap_or(1).max(1);
            if start_line > total_lines {
                return Ok(serde_json::json!({
                    "action": "read",
                    "path": resolved.display().to_string(),
                    "content": "",
                    "numbered": "",
                    "start_line": start_line,
                    "end_line": total_lines,
                    "total_lines": total_lines,
                    "bytes_read": 0,
                    "total_bytes": total_bytes,
                    "byte_offset": byte_offset,
                    "truncated": false,
                    "sha256": sha256,
                }));
            }
            let requested_end = args
                .end_line
                .unwrap_or(total_lines)
                .min(total_lines)
                .max(start_line);
            let selected_lines = &all_lines[start_line - 1..requested_end];
            let joined = selected_lines.join("\n");

            let start_byte = floor_char_boundary_str(&joined, byte_offset.min(joined.len()));
            let after_offset = &joined[start_byte..];
            let end_byte = floor_char_boundary_str(after_offset, max_bytes);
            let sliced = &after_offset[..end_byte];
            let truncated = end_byte < after_offset.len() || requested_end < total_lines;

            let offset_lines = joined[..start_byte].bytes().filter(|&b| b == b'\n').count();
            let first_line_no = start_line + offset_lines;
            let sliced_lines: Vec<&str> = sliced.lines().collect();
            let actual_end_line = if sliced_lines.is_empty() {
                first_line_no
            } else {
                first_line_no + sliced_lines.len() - 1
            };
            let mut numbered = String::with_capacity(sliced.len() + sliced_lines.len() * 8);
            for (idx, line) in sliced_lines.iter().enumerate() {
                if idx > 0 {
                    numbered.push('\n');
                }
                numbered.push_str(&format!("{}: {line}", first_line_no + idx));
            }

            Ok(serde_json::json!({
                "action": "read",
                "path": resolved.display().to_string(),
                "content": sliced,
                "numbered": numbered,
                "start_line": first_line_no,
                "end_line": actual_end_line,
                "total_lines": total_lines,
                "bytes_read": sliced.len(),
                "total_bytes": total_bytes,
                "byte_offset": start_byte,
                "truncated": truncated,
                "sha256": sha256,
            }))
        }
        "write" | "create" => {
            let resolved = resolve_workspace_path(base_dir, args.path.as_deref(), true)?;
            let content = args.content.unwrap_or_default();
            let append = args.append.unwrap_or(false);
            let overwrite = args.overwrite.unwrap_or(true);
            if !append && !overwrite && resolved.exists() {
                return Err(format!(
                    "file {} already exists and `overwrite` is false",
                    resolved.display()
                ));
            }
            if let Some(parent) = resolved.parent()
                && !parent.as_os_str().is_empty()
            {
                std::fs::create_dir_all(parent).map_err(|e| {
                    format!("cannot create parent directory {}: {e}", parent.display())
                })?;
            }
            if append {
                use std::io::Write as _;
                let mut f = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&resolved)
                    .map_err(|e| format!("cannot open {} for append: {e}", resolved.display()))?;
                f.write_all(content.as_bytes())
                    .map_err(|e| format!("cannot append to {}: {e}", resolved.display()))?;
            } else {
                std::fs::write(&resolved, content.as_bytes())
                    .map_err(|e| format!("cannot write {}: {e}", resolved.display()))?;
            }
            let final_bytes = std::fs::read(&resolved)
                .map_err(|e| format!("cannot read back {}: {e}", resolved.display()))?;
            let sha256 = sha256_hex(&final_bytes);
            Ok(serde_json::json!({
                "action": "write",
                "path": resolved.display().to_string(),
                "bytes_written": content.len(),
                "total_bytes": final_bytes.len(),
                "append": append,
                "sha256": sha256,
            }))
        }
        "edit" | "multi_edit" | "replace" => {
            let resolved = resolve_workspace_path(base_dir, args.path.as_deref(), true)?;
            let mut specs = Vec::new();
            if let Some(target) = args.target {
                specs.push(FileEditSpec {
                    target,
                    replacement: args.replacement.unwrap_or_default(),
                    replace_all: args.replace_all,
                });
            }
            specs.extend(args.edits);
            if specs.is_empty() {
                return Err(
                    "`files` edit requires `target` (and optional `replacement`) or non-empty `edits`"
                        .to_string(),
                );
            }
            let existing = std::fs::read_to_string(&resolved)
                .map_err(|e| format!("cannot read {} for edit: {e}", resolved.display()))?;
            let (updated, replacements_made, lines_added, lines_removed) = apply_atomic_edits(
                &existing,
                &specs,
                args.replace_all.unwrap_or(true),
                &resolved,
            )?;
            std::fs::write(&resolved, updated.as_bytes())
                .map_err(|e| format!("cannot write edited file {}: {e}", resolved.display()))?;
            let sha256 = sha256_hex(updated.as_bytes());
            Ok(serde_json::json!({
                "action": if action == "multi_edit" { "multi_edit" } else { "edit" },
                "path": resolved.display().to_string(),
                "replacements_made": replacements_made,
                "lines_added": lines_added,
                "lines_removed": lines_removed,
                "sha256": sha256,
            }))
        }
        "patch" | "apply_patch" => {
            let patch_text = args
                .patch
                .or(args.content)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "`files` patch requires non-empty `patch`".to_string())?;
            apply_workspace_patch(base_dir, &patch_text)
        }
        "list" | "ls" | "list_dir" => {
            let dir_path = resolve_workspace_path(base_dir, args.path.as_deref(), false)?;
            let rd = std::fs::read_dir(&dir_path)
                .map_err(|e| format!("cannot list directory {}: {e}", dir_path.display()))?;
            let include_hidden = args.include_hidden.unwrap_or(true);
            let max_results = args
                .max_results
                .unwrap_or(DEFAULT_LIST_MAX_RESULTS)
                .clamp(1, HARD_LIST_MAX_RESULTS);

            let mut raw_entries: Vec<(String, serde_json::Value)> = Vec::new();
            for entry_res in rd {
                let Ok(entry) = entry_res else {
                    continue;
                };
                let name = entry.file_name().to_string_lossy().into_owned();
                if !include_hidden && name.starts_with('.') {
                    continue;
                }
                let entry_path = entry.path();
                let Ok(sym_meta) = std::fs::symlink_metadata(&entry_path) else {
                    continue;
                };
                let ft = sym_meta.file_type();
                let (kind, bytes) = if ft.is_symlink() {
                    let target_len = std::fs::metadata(&entry_path)
                        .ok()
                        .filter(|m| m.is_file())
                        .map_or(0, |m| m.len());
                    ("symlink", target_len)
                } else if ft.is_dir() {
                    ("dir", 0)
                } else {
                    ("file", sym_meta.len())
                };
                let rel_path = entry_path
                    .strip_prefix(base_dir)
                    .ok()
                    .filter(|p| !p.as_os_str().is_empty())
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| entry_path.display().to_string());
                raw_entries.push((
                    name.clone(),
                    serde_json::json!({
                        "name": name,
                        "path": rel_path,
                        "kind": kind,
                        "bytes": bytes,
                    }),
                ));
            }
            raw_entries.sort_by(|a, b| a.0.cmp(&b.0));
            let truncated = raw_entries.len() > max_results;
            let entries: Vec<serde_json::Value> = raw_entries
                .into_iter()
                .take(max_results)
                .map(|(_, v)| v)
                .collect();
            let count = entries.len();
            Ok(serde_json::json!({
                "action": "list",
                "path": dir_path.display().to_string(),
                "entries": entries,
                "count": count,
                "truncated": truncated,
            }))
        }
        "glob" => {
            let mut raw_pattern = args
                .pattern
                .or(args.glob)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            let mut raw_path = args
                .path
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            if raw_pattern.is_none()
                && let Some(ref p) = raw_path
                && (p.contains('*') || p.contains('?') || p.contains('[') || p.contains('{'))
            {
                raw_pattern = Some(p.clone());
                raw_path = None;
            }
            let search_root = resolve_workspace_path(base_dir, raw_path.as_deref(), false)?;
            if !search_root.exists() {
                return Err(format!(
                    "glob base directory {} does not exist",
                    search_root.display()
                ));
            }
            let pattern = raw_pattern.unwrap_or_else(|| "**/*".to_string());
            let clean_pat = pattern.strip_prefix("./").unwrap_or(&pattern);
            let max_results = args
                .max_results
                .unwrap_or(DEFAULT_GLOB_MAX_RESULTS)
                .clamp(1, HARD_GLOB_MAX_RESULTS);
            let include_hidden = args
                .include_hidden
                .unwrap_or_else(|| clean_pat.starts_with('.') || clean_pat.contains("/."));
            let include_ignored = args.include_ignored.unwrap_or(false)
                || DEFAULT_IGNORED_DIRS.iter().any(|d| clean_pat.contains(d));

            let mut candidate_files = Vec::new();
            let gitignore_patterns = if include_ignored {
                Vec::new()
            } else {
                read_gitignore_patterns(&search_root)
            };
            collect_workspace_files(
                &search_root,
                &search_root,
                include_hidden,
                include_ignored,
                &gitignore_patterns,
                &mut candidate_files,
            );
            candidate_files.sort();

            let mut matched = Vec::new();
            let mut truncated = false;
            let pat_has_slash = clean_pat.contains('/');
            for rel in candidate_files {
                let file_name = Path::new(&rel)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(&rel);
                let is_match = glob_matches(clean_pat, &rel)
                    || (!pat_has_slash && glob_matches(clean_pat, file_name));
                if is_match {
                    if matched.len() >= max_results {
                        truncated = true;
                        break;
                    }
                    matched.push(rel);
                }
            }
            let count = matched.len();
            Ok(serde_json::json!({
                "action": "glob",
                "base_dir": search_root.display().to_string(),
                "pattern": pattern,
                "files": matched,
                "matches": matched,
                "count": count,
                "truncated": truncated,
            }))
        }
        "grep" | "search" => {
            let query = args
                .query
                .or(args.pattern)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "`files` grep requires a non-empty `query`".to_string())?;
            let target_path = resolve_workspace_path(base_dir, args.path.as_deref(), false)?;
            if !target_path.exists() {
                return Err(format!(
                    "grep target path {} does not exist",
                    target_path.display()
                ));
            }
            let max_results = args
                .max_results
                .unwrap_or(DEFAULT_GREP_MAX_RESULTS)
                .clamp(1, HARD_GREP_MAX_RESULTS);
            let context_lines = args.context_lines.unwrap_or(0).min(10);
            let matcher = GrepMatcher::compile(
                &query,
                args.case_insensitive.unwrap_or(false),
                args.literal.unwrap_or(false),
            );
            let file_glob = args
                .glob
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| s.strip_prefix("./").unwrap_or(s));

            let files_to_search: Vec<(PathBuf, String)> = if target_path.is_file() {
                let display = target_path
                    .strip_prefix(base_dir)
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|_| target_path.display().to_string());
                vec![(target_path, display)]
            } else {
                let include_hidden = args.include_hidden.unwrap_or(false);
                let include_ignored = args.include_ignored.unwrap_or(false);
                let gitignore_patterns = if include_ignored {
                    Vec::new()
                } else {
                    read_gitignore_patterns(&target_path)
                };
                let mut rels = Vec::new();
                collect_workspace_files(
                    &target_path,
                    &target_path,
                    include_hidden,
                    include_ignored,
                    &gitignore_patterns,
                    &mut rels,
                );
                rels.sort();
                rels.into_iter()
                    .filter(|rel| {
                        if let Some(g) = file_glob {
                            let fname = Path::new(rel)
                                .file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or(rel);
                            glob_matches(g, rel) || (!g.contains('/') && glob_matches(g, fname))
                        } else {
                            true
                        }
                    })
                    .map(|rel| {
                        let abs = target_path.join(&rel);
                        let disp = abs
                            .strip_prefix(base_dir)
                            .map(|p| p.display().to_string())
                            .unwrap_or(rel);
                        (abs, disp)
                    })
                    .collect()
            };

            let mut matches = Vec::new();
            let mut truncated = false;
            for (abs_path, disp_path) in files_to_search {
                let Ok(bytes) = std::fs::read(&abs_path) else {
                    continue;
                };
                if collect_grep_hits_from_bytes(
                    &bytes,
                    &disp_path,
                    &matcher,
                    context_lines,
                    max_results,
                    &mut matches,
                ) {
                    truncated = true;
                    break;
                }
            }
            let count = matches.len();
            Ok(serde_json::json!({
                "action": "grep",
                "query": query,
                "matches": matches,
                "count": count,
                "truncated": truncated,
            }))
        }
        "exec" | "bash" | "sh" | "command" => {
            if std::env::var("ANYAGENTS_DISABLE_WORKSPACE_EXEC")
                .ok()
                .is_some_and(|v| {
                    matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes")
                })
            {
                return Err(
                    "workspace.exec is disabled for this agent (ANYAGENTS_DISABLE_WORKSPACE_EXEC=1); delegate command execution to a subagent".to_string(),
                );
            }
            let cmd_str = args
                .cmd
                .as_deref()
                .or(args.content.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "`workspace.exec` requires a non-empty `cmd`".to_string())?;
            let raw_cwd = args.cwd.as_deref().or(args.path.as_deref());
            let resolved_cwd = resolve_workspace_path(base_dir, raw_cwd, false)?;
            if !resolved_cwd.is_dir() {
                return Err(format!(
                    "workspace.exec cwd {} does not exist or is not a directory",
                    resolved_cwd.display()
                ));
            }
            let timeout_ms = limits.resolve(args.timeout_ms);
            let started = std::time::Instant::now();
            let mut cmd = std::process::Command::new("/bin/bash");
            cmd.arg("-c")
                .arg(cmd_str)
                .current_dir(&resolved_cwd)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            #[cfg(unix)]
            {
                use std::os::unix::process::CommandExt as _;
                cmd.process_group(0);
            }
            let mut child = cmd
                .spawn()
                .map_err(|e| format!("failed to spawn bash for workspace.exec: {e}"))?;
            let stdout_pipe = child
                .stdout
                .take()
                .ok_or_else(|| "workspace.exec stdout missing".to_string())?;
            let stderr_pipe = child
                .stderr
                .take()
                .ok_or_else(|| "workspace.exec stderr missing".to_string())?;
            let stdout_handle = std::thread::spawn(move || {
                use std::io::Read as _;
                let mut buf = Vec::new();
                let mut take = stdout_pipe.take(64 * 1024);
                let _ = take.read_to_end(&mut buf);
                String::from_utf8_lossy(&buf).into_owned()
            });
            let stderr_handle = std::thread::spawn(move || {
                use std::io::Read as _;
                let mut buf = Vec::new();
                let mut take = stderr_pipe.take(64 * 1024);
                let _ = take.read_to_end(&mut buf);
                String::from_utf8_lossy(&buf).into_owned()
            });
            let mut guard = CodeChildGuard { child: Some(child) };
            let deadline = started + std::time::Duration::from_millis(timeout_ms);
            // A command a `code` call started ends with that call, and one
            // whose tool call the harness cancelled ends then.
            let scope = code_children::current();
            let registration = scope
                .children
                .as_ref()
                .map(code_children::CodeChildren::start);
            let mut stopped_early = false;
            let status_opt = loop {
                if let Some(ref mut c) = guard.child {
                    match c.try_wait() {
                        Ok(Some(st)) => break Some(st),
                        Ok(None) => {
                            if std::time::Instant::now() >= deadline {
                                break None;
                            }
                            if scope.should_stop(registration.as_ref()) {
                                stopped_early = true;
                                break None;
                            }
                            std::thread::sleep(std::time::Duration::from_millis(10));
                        }
                        Err(e) => return Err(format!("workspace.exec wait failed: {e}")),
                    }
                } else {
                    break None;
                }
            };
            if status_opt.is_none()
                && let Some(mut child) = guard.child.take()
            {
                // The whole group, gently first: git removes its lock files
                // on SIGTERM and leaves them behind on SIGKILL.
                code_children::stop_group(&mut child, code_children::TERM_GRACE);
                if stopped_early && let Some(registration) = &registration {
                    registration.stopped(cmd_str, started.elapsed());
                }
            }
            drop(guard);
            let stdout = stdout_handle.join().unwrap_or_default();
            let stderr = stderr_handle.join().unwrap_or_default();
            let duration_ms = started.elapsed().as_millis() as u64;
            let note = code_contract::exec_clamp_note(
                args.requested_timeout_ms.or(args.timeout_ms),
                timeout_ms,
                &limits.ceiling_reason(),
            );
            let Some(status) = status_opt else {
                if stopped_early {
                    return Err(format!(
                        "workspace.exec was stopped after {}ms because the call that started it ended; \
                         its whole process group was ended (SIGTERM, then SIGKILL), so a write it was making may be partial \
                         (stdout: {}, stderr: {})",
                        started.elapsed().as_millis(),
                        stdout.trim(),
                        stderr.trim()
                    ));
                }
                return Err(format!(
                    "workspace.exec timed out after {timeout_ms}ms and was killed{}; {} (stdout: {}, stderr: {})",
                    note.as_deref()
                        .map(|n| format!(" ({n})"))
                        .unwrap_or_default(),
                    code_contract::exec_timeout_hint(limits.max_ms),
                    stdout.trim(),
                    stderr.trim()
                ));
            };
            let exit_code = status.code().unwrap_or(-1);
            let mut result = serde_json::json!({
                "action": "exec",
                "cmd": cmd_str,
                "cwd": resolved_cwd.display().to_string(),
                "exit_code": exit_code,
                "stdout": stdout,
                "stderr": stderr,
                "duration_ms": duration_ms,
            });
            if let Some(note) = note {
                result["note"] = note.into();
            }
            Ok(result)
        }
        other => Err(format!(
            "unknown `files` action `{other}`; expected `read`, `write`, `edit`, `multi_edit`, `patch`, `list`, `glob`, `grep`, or `exec`"
        )),
    }
}

pub(super) fn apply_atomic_edits(
    existing: &str,
    specs: &[FileEditSpec],
    default_replace_all: bool,
    path: &Path,
) -> Result<(String, usize, usize, usize), String> {
    let mut working = existing.to_string();
    let mut total_replacements = 0usize;
    let mut total_lines_added = 0usize;
    let mut total_lines_removed = 0usize;

    for (idx, spec) in specs.iter().enumerate() {
        if spec.target.is_empty() {
            return Err(format!(
                "edit #{} in {}: `target` must not be empty",
                idx + 1,
                path.display()
            ));
        }
        let replace_all = spec.replace_all.unwrap_or(default_replace_all);
        let tgt_lines = spec
            .target
            .lines()
            .count()
            .max(usize::from(!spec.target.is_empty()));
        let rep_lines = spec
            .replacement
            .lines()
            .count()
            .max(usize::from(!spec.replacement.is_empty()));

        match apply_single_edit_cascade(&working, &spec.target, &spec.replacement, replace_all) {
            Ok((next, count)) => {
                working = next;
                total_replacements += count;
                total_lines_removed += count * tgt_lines;
                total_lines_added += count * rep_lines;
            }
            Err(reason) => {
                let hint = find_closest_lines(&working, &spec.target);
                return Err(format!(
                    "edit #{} in {}: {reason}{hint}",
                    idx + 1,
                    path.display()
                ));
            }
        }
    }

    Ok((
        working,
        total_replacements,
        total_lines_added,
        total_lines_removed,
    ))
}

pub(super) fn apply_single_edit_cascade(
    content: &str,
    target: &str,
    replacement: &str,
    replace_all: bool,
) -> Result<(String, usize), String> {
    // Stage 1: Exact match
    let exact_matches: Vec<usize> = content.match_indices(target).map(|(pos, _)| pos).collect();
    if exact_matches.len() == 1 || (exact_matches.len() > 1 && replace_all) {
        let count = if replace_all { exact_matches.len() } else { 1 };
        let updated = if replace_all {
            content.replace(target, replacement)
        } else {
            content.replacen(target, replacement, 1)
        };
        return Ok((updated, count));
    } else if exact_matches.len() > 1 {
        let lines: Vec<String> = exact_matches
            .iter()
            .take(8)
            .map(|&pos| (content[..pos].bytes().filter(|&b| b == b'\n').count() + 1).to_string())
            .collect();
        return Err(format!(
            "`target` matched {} times (at lines {}); include more context or set `replace_all: true`",
            exact_matches.len(),
            lines.join(", ")
        ));
    }

    // Stage 2: CRLF/LF normalized match
    let uses_crlf = content.contains("\r\n");
    let norm_content = content.replace("\r\n", "\n");
    let norm_target = target.replace("\r\n", "\n");
    let norm_replacement = replacement.replace("\r\n", "\n");
    if norm_content != content || norm_target != target {
        let crlf_matches: Vec<usize> = norm_content
            .match_indices(&norm_target)
            .map(|(pos, _)| pos)
            .collect();
        if crlf_matches.len() == 1 || (crlf_matches.len() > 1 && replace_all) {
            let count = if replace_all { crlf_matches.len() } else { 1 };
            let updated_lf = if replace_all {
                norm_content.replace(&norm_target, &norm_replacement)
            } else {
                norm_content.replacen(&norm_target, &norm_replacement, 1)
            };
            let final_str = if uses_crlf {
                updated_lf.replace('\n', "\r\n")
            } else {
                updated_lf
            };
            return Ok((final_str, count));
        } else if crlf_matches.len() > 1 {
            let lines: Vec<String> = crlf_matches
                .iter()
                .take(8)
                .map(|&pos| {
                    (norm_content[..pos].bytes().filter(|&b| b == b'\n').count() + 1).to_string()
                })
                .collect();
            return Err(format!(
                "`target` matched {} times (at lines {}); include more context or set `replace_all: true`",
                crlf_matches.len(),
                lines.join(", ")
            ));
        }
    }

    let doc_lines: Vec<&str> = norm_content.split('\n').collect();
    let target_lines: Vec<&str> = norm_target.split('\n').collect();
    if target_lines.is_empty() || target_lines.len() > doc_lines.len() {
        return Err("`target` not found.".to_string());
    }

    // Stage 3: Line-end whitespace-trimmed (`trim_end()`) match
    let mut trim_end_hits = Vec::new();
    for i in 0..=(doc_lines.len() - target_lines.len()) {
        if (0..target_lines.len())
            .all(|j| doc_lines[i + j].trim_end() == target_lines[j].trim_end())
        {
            trim_end_hits.push(i);
        }
    }
    if trim_end_hits.len() == 1 {
        let i = trim_end_hits[0];
        let repl_lines: Vec<&str> = norm_replacement.split('\n').collect();
        let mut new_doc: Vec<String> = Vec::with_capacity(doc_lines.len() + repl_lines.len());
        new_doc.extend(doc_lines[..i].iter().map(|s| (*s).to_string()));
        new_doc.extend(repl_lines.iter().map(|s| (*s).to_string()));
        new_doc.extend(
            doc_lines[i + target_lines.len()..]
                .iter()
                .map(|s| (*s).to_string()),
        );
        let joined = if uses_crlf {
            new_doc.join("\r\n")
        } else {
            new_doc.join("\n")
        };
        return Ok((joined, 1));
    } else if trim_end_hits.len() > 1 {
        let lines: Vec<String> = trim_end_hits
            .iter()
            .take(8)
            .map(|&i| (i + 1).to_string())
            .collect();
        return Err(format!(
            "`target` matched {} times (at lines {}); include more surrounding context lines",
            trim_end_hits.len(),
            lines.join(", ")
        ));
    }

    // Stage 4: Indentation-tolerant (`trim()`) match preserving relative indentation
    let has_substantive_line = target_lines.iter().any(|l| l.trim().len() >= 3);
    if has_substantive_line {
        let mut trim_hits = Vec::new();
        for i in 0..=(doc_lines.len() - target_lines.len()) {
            if (0..target_lines.len()).all(|j| doc_lines[i + j].trim() == target_lines[j].trim()) {
                trim_hits.push(i);
            }
        }
        if trim_hits.len() == 1 {
            let i = trim_hits[0];
            let mut indent_delta: isize = 0;
            for j in 0..target_lines.len() {
                if !target_lines[j].trim().is_empty() && !doc_lines[i + j].trim().is_empty() {
                    let doc_indent = doc_lines[i + j].len() - doc_lines[i + j].trim_start().len();
                    let tgt_indent = target_lines[j].len() - target_lines[j].trim_start().len();
                    indent_delta = doc_indent as isize - tgt_indent as isize;
                    break;
                }
            }
            let repl_lines: Vec<String> = norm_replacement
                .split('\n')
                .map(|line| {
                    if line.trim().is_empty() || indent_delta == 0 {
                        line.to_string()
                    } else if indent_delta > 0 {
                        format!("{}{}", " ".repeat(indent_delta as usize), line)
                    } else {
                        let remove = (-indent_delta) as usize;
                        let leading = line.len() - line.trim_start_matches(' ').len();
                        line[remove.min(leading)..].to_string()
                    }
                })
                .collect();
            let mut new_doc: Vec<String> = Vec::with_capacity(doc_lines.len() + repl_lines.len());
            new_doc.extend(doc_lines[..i].iter().map(|s| (*s).to_string()));
            new_doc.extend(repl_lines);
            new_doc.extend(
                doc_lines[i + target_lines.len()..]
                    .iter()
                    .map(|s| (*s).to_string()),
            );
            let joined = if uses_crlf {
                new_doc.join("\r\n")
            } else {
                new_doc.join("\n")
            };
            return Ok((joined, 1));
        } else if trim_hits.len() > 1 {
            let lines: Vec<String> = trim_hits
                .iter()
                .take(8)
                .map(|&i| (i + 1).to_string())
                .collect();
            return Err(format!(
                "`target` matched {} times by indentation-tolerant comparison (at lines {}); include more context lines",
                trim_hits.len(),
                lines.join(", ")
            ));
        }
    }

    Err("`target` not found.".to_string())
}

pub(super) fn is_v4a_patch(text: &str) -> bool {
    text.lines().any(|l| {
        let t = l.trim();
        t == "*** Begin Patch"
            || t.starts_with("*** Update File:")
            || t.starts_with("*** Add File:")
            || t.starts_with("*** Delete File:")
    })
}

pub(super) fn clean_patch_rel(raw: &str) -> &str {
    let t = raw.trim();
    t.strip_prefix("a/")
        .or_else(|| t.strip_prefix("b/"))
        .unwrap_or(t)
}

pub(super) fn resolve_patch_rel_path(base_dir: &Path, raw: &str) -> Result<PathBuf, String> {
    resolve_workspace_path(base_dir, Some(clean_patch_rel(raw)), true)
}

pub(super) trait PatchFs {
    fn resolve(&self, raw: &str) -> Result<String, String>;
    fn read_text(&self, resolved: &str, rel_label: &str) -> Result<String, String>;
    fn write_text(&self, resolved: &str, rel_label: &str, content: &str) -> Result<String, String>;
    fn delete_if_exists(&self, resolved: &str, rel_label: &str) -> Result<(), String>;
}

pub(super) struct LocalPatchFs<'a> {
    base_dir: &'a Path,
}

impl PatchFs for LocalPatchFs<'_> {
    fn resolve(&self, raw: &str) -> Result<String, String> {
        Ok(resolve_patch_rel_path(self.base_dir, raw)?
            .display()
            .to_string())
    }

    fn read_text(&self, resolved: &str, rel_label: &str) -> Result<String, String> {
        std::fs::read_to_string(resolved).map_err(|e| format!("failed reading `{rel_label}`: {e}"))
    }

    fn write_text(&self, resolved: &str, rel_label: &str, content: &str) -> Result<String, String> {
        let p = Path::new(resolved);
        if let Some(parent) = p.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|e| {
                format!("failed creating parent directory {}: {e}", parent.display())
            })?;
        }
        std::fs::write(p, content.as_bytes())
            .map_err(|e| format!("failed writing `{rel_label}`: {e}"))?;
        Ok(sha256_hex(content.as_bytes()))
    }

    fn delete_if_exists(&self, resolved: &str, rel_label: &str) -> Result<(), String> {
        let p = Path::new(resolved);
        if p.exists() {
            std::fs::remove_file(p).map_err(|e| format!("failed deleting `{rel_label}`: {e}"))?;
        }
        Ok(())
    }
}

pub(super) struct RemotePatchFs<'a> {
    backend: &'a dyn agent_tools::WorkspaceBackend,
}

impl PatchFs for RemotePatchFs<'_> {
    fn resolve(&self, raw: &str) -> Result<String, String> {
        self.backend.resolve_path(Some(clean_patch_rel(raw)), true)
    }

    fn read_text(&self, resolved: &str, rel_label: &str) -> Result<String, String> {
        let (_, bytes) = self
            .backend
            .read_file_bytes(Some(resolved))
            .map_err(|e| format!("failed reading `{rel_label}`: {e}"))?;
        String::from_utf8(bytes).map_err(|e| format!("`{rel_label}` is not valid UTF-8: {e}"))
    }

    fn write_text(&self, resolved: &str, rel_label: &str, content: &str) -> Result<String, String> {
        let (_, _, sha256) = self
            .backend
            .write_file_bytes(Some(resolved), content.as_bytes(), false, true)
            .map_err(|e| format!("failed writing `{rel_label}`: {e}"))?;
        Ok(sha256)
    }

    fn delete_if_exists(&self, resolved: &str, rel_label: &str) -> Result<(), String> {
        let cmd = format!("rm -f -- {}", shell_quote_remote(resolved));
        let out = self
            .backend
            .exec(&cmd, None, Some(15_000))
            .map_err(|e| format!("failed deleting `{rel_label}`: {e}"))?;
        if out.exit_code != 0 {
            return Err(format!(
                "failed deleting `{rel_label}`: {}",
                out.stderr.trim()
            ));
        }
        Ok(())
    }
}

pub(super) fn apply_workspace_patch(
    base_dir: &Path,
    patch_text: &str,
) -> Result<serde_json::Value, String> {
    apply_workspace_patch_with(&LocalPatchFs { base_dir }, patch_text)
}

pub(super) fn apply_workspace_patch_with(
    fs: &dyn PatchFs,
    patch_text: &str,
) -> Result<serde_json::Value, String> {
    let normalized = patch_text.replace("\r\n", "\n");
    let lines: Vec<&str> = normalized.lines().collect();
    let mut operations = Vec::new();
    let mut files = Vec::new();

    if is_v4a_patch(&normalized) {
        let mut i = 0usize;
        while i < lines.len() {
            let line = lines[i].trim();
            if line.is_empty()
                || line == "*** Begin Patch"
                || line == "*** End Patch"
                || line.starts_with("```")
            {
                i += 1;
                continue;
            }
            if let Some(rel) = line.strip_prefix("*** Add File:").map(str::trim) {
                i += 1;
                let mut body_lines = Vec::new();
                while i < lines.len() && !lines[i].trim().starts_with("*** ") {
                    let raw_l = lines[i];
                    if let Some(stripped) = raw_l.strip_prefix('+') {
                        body_lines.push(stripped);
                    } else {
                        body_lines.push(raw_l);
                    }
                    i += 1;
                }
                let target = fs.resolve(rel)?;
                let mut content = body_lines.join("\n");
                if !content.is_empty() && !content.ends_with('\n') {
                    content.push('\n');
                }
                let sha256 = fs.write_text(&target, rel, &content)?;
                files.push(rel.to_string());
                operations.push(serde_json::json!({
                    "op": "add",
                    "path": rel,
                    "resolved_path": target,
                    "sha256": sha256,
                }));
                continue;
            }
            if let Some(rel) = line.strip_prefix("*** Delete File:").map(str::trim) {
                i += 1;
                while i < lines.len() && !lines[i].trim().starts_with("*** ") {
                    i += 1;
                }
                let target = fs.resolve(rel)?;
                fs.delete_if_exists(&target, rel)?;
                files.push(rel.to_string());
                operations.push(serde_json::json!({
                    "op": "delete",
                    "path": rel,
                    "resolved_path": target,
                }));
                continue;
            }
            if let Some(rel) = line.strip_prefix("*** Update File:").map(str::trim) {
                i += 1;
                let mut move_to: Option<String> = None;
                if i < lines.len()
                    && let Some(mv) = lines[i].trim().strip_prefix("*** Move to:").map(str::trim)
                {
                    move_to = Some(mv.to_string());
                    i += 1;
                }
                let mut hunk_lines = Vec::new();
                while i < lines.len() && !lines[i].trim().starts_with("*** ") {
                    if !lines[i].trim().starts_with("```") {
                        hunk_lines.push(lines[i]);
                    }
                    i += 1;
                }
                let target = fs.resolve(rel)?;
                let existing = fs.read_text(&target, rel)?;
                let updated = apply_patch_context_hunks(&existing, &hunk_lines)
                    .map_err(|e| format!("failed patching `{rel}`: {e}"))?;
                if let Some(dest_rel) = move_to {
                    let dest = fs.resolve(&dest_rel)?;
                    let sha256 = fs.write_text(&dest, &dest_rel, &updated)?;
                    if dest != target {
                        let _ = fs.delete_if_exists(&target, rel);
                    }
                    files.push(dest_rel.clone());
                    operations.push(serde_json::json!({
                        "op": "update",
                        "path": rel,
                        "moved_to": dest_rel,
                        "resolved_path": dest,
                        "sha256": sha256,
                    }));
                } else {
                    let sha256 = fs.write_text(&target, rel, &updated)?;
                    files.push(rel.to_string());
                    operations.push(serde_json::json!({
                        "op": "update",
                        "path": rel,
                        "resolved_path": target,
                        "sha256": sha256,
                    }));
                }
                continue;
            }
            i += 1;
        }
    } else {
        let mut i = 0usize;
        while i < lines.len() {
            let line = lines[i];
            if line.starts_with("--- ") && i + 1 < lines.len() && lines[i + 1].starts_with("+++ ") {
                let old_header = line
                    .strip_prefix("--- ")
                    .unwrap_or("")
                    .split('\t')
                    .next()
                    .unwrap_or("")
                    .trim();
                let new_header = lines[i + 1]
                    .strip_prefix("+++ ")
                    .unwrap_or("")
                    .split('\t')
                    .next()
                    .unwrap_or("")
                    .trim();
                i += 2;
                let mut hunk_lines = Vec::new();
                while i < lines.len()
                    && !lines[i].starts_with("diff --git ")
                    && !(lines[i].starts_with("--- ")
                        && i + 1 < lines.len()
                        && lines[i + 1].starts_with("+++ "))
                {
                    hunk_lines.push(lines[i]);
                    i += 1;
                }
                if new_header == "/dev/null" {
                    let target = fs.resolve(old_header)?;
                    fs.delete_if_exists(&target, old_header)?;
                    let clean_rel = old_header.strip_prefix("a/").unwrap_or(old_header);
                    files.push(clean_rel.to_string());
                    operations.push(serde_json::json!({
                        "op": "delete",
                        "path": clean_rel,
                        "resolved_path": target,
                    }));
                } else if old_header == "/dev/null" {
                    let target = fs.resolve(new_header)?;
                    let mut added = Vec::new();
                    for hl in &hunk_lines {
                        if hl.starts_with("@@") || hl.starts_with("\\ No newline") {
                            continue;
                        }
                        if let Some(rest) = hl.strip_prefix('+') {
                            added.push(rest);
                        } else if let Some(rest) = hl.strip_prefix(' ') {
                            added.push(rest);
                        }
                    }
                    let mut content = added.join("\n");
                    if !content.is_empty() {
                        content.push('\n');
                    }
                    let sha256 = fs.write_text(&target, new_header, &content)?;
                    let clean_rel = new_header.strip_prefix("b/").unwrap_or(new_header);
                    files.push(clean_rel.to_string());
                    operations.push(serde_json::json!({
                        "op": "add",
                        "path": clean_rel,
                        "resolved_path": target,
                        "sha256": sha256,
                    }));
                } else {
                    let rel = if !new_header.is_empty() {
                        new_header
                    } else {
                        old_header
                    };
                    let target = fs.resolve(rel)?;
                    let existing = fs.read_text(&target, rel)?;
                    let updated = apply_patch_context_hunks(&existing, &hunk_lines)
                        .map_err(|e| format!("failed applying hunk to `{rel}`: {e}"))?;
                    let sha256 = fs.write_text(&target, rel, &updated)?;
                    let clean_rel = rel
                        .strip_prefix("b/")
                        .or_else(|| rel.strip_prefix("a/"))
                        .unwrap_or(rel);
                    files.push(clean_rel.to_string());
                    operations.push(serde_json::json!({
                        "op": "update",
                        "path": clean_rel,
                        "resolved_path": target,
                        "sha256": sha256,
                    }));
                }
                continue;
            }
            i += 1;
        }
    }

    if operations.is_empty() {
        return Err(
            "patch failed: no valid V4A (`*** Begin Patch` / `*** Update File:`) or unified diff (`--- a/...` / `+++ b/...`) hunks found"
                .to_string(),
        );
    }
    let count = operations.len();
    Ok(serde_json::json!({
        "action": "patch",
        "files": files,
        "operations": operations,
        "count": count,
    }))
}

pub(super) fn apply_patch_context_hunks(
    existing: &str,
    hunk_lines: &[&str],
) -> Result<String, String> {
    let had_trailing_newline = existing.ends_with('\n');
    let mut doc_lines: Vec<String> = existing
        .replace("\r\n", "\n")
        .lines()
        .map(String::from)
        .collect();

    let mut hunks: Vec<Vec<&str>> = Vec::new();
    let mut current_hunk: Vec<&str> = Vec::new();
    for &line in hunk_lines {
        if line.starts_with("@@") {
            if !current_hunk.is_empty() {
                hunks.push(std::mem::take(&mut current_hunk));
            }
            continue;
        }
        if line.starts_with("\\ No newline") {
            continue;
        }
        current_hunk.push(line);
    }
    if !current_hunk.is_empty() {
        hunks.push(current_hunk);
    }

    let mut search_cursor = 0usize;
    for (hunk_idx, hunk) in hunks.iter().enumerate() {
        let mut old_block: Vec<&str> = Vec::new();
        let mut new_block: Vec<String> = Vec::new();
        for &hl in hunk {
            if let Some(rem) = hl.strip_prefix('-') {
                old_block.push(rem);
            } else if let Some(add) = hl.strip_prefix('+') {
                new_block.push(add.to_string());
            } else if let Some(ctx) = hl.strip_prefix(' ') {
                old_block.push(ctx);
                new_block.push(ctx.to_string());
            } else {
                old_block.push(hl);
                new_block.push(hl.to_string());
            }
        }

        if old_block.is_empty() {
            let pos = search_cursor.min(doc_lines.len());
            doc_lines.splice(pos..pos, new_block);
            continue;
        }

        if let Some(pos) = find_patch_subslice_lines(&doc_lines, &old_block, search_cursor) {
            let new_len = new_block.len();
            doc_lines.splice(pos..pos + old_block.len(), new_block);
            search_cursor = pos + new_len;
        } else {
            return Err(format!(
                "hunk #{} context did not match target file around line `{}`",
                hunk_idx + 1,
                old_block.first().unwrap_or(&"")
            ));
        }
    }

    let mut out = doc_lines.join("\n");
    if had_trailing_newline && !out.ends_with('\n') {
        out.push('\n');
    }
    Ok(out)
}

pub(super) fn find_patch_subslice_lines(
    doc: &[String],
    needle: &[&str],
    cursor: usize,
) -> Option<usize> {
    if needle.len() > doc.len() {
        return None;
    }
    let max_start = doc.len() - needle.len();
    let start_pos = cursor.min(max_start);

    for idx in (start_pos..=max_start).chain(0..start_pos) {
        if (0..needle.len()).all(|j| doc[idx + j] == needle[j]) {
            return Some(idx);
        }
    }
    for idx in (start_pos..=max_start).chain(0..start_pos) {
        if (0..needle.len()).all(|j| doc[idx + j].trim_end() == needle[j].trim_end()) {
            return Some(idx);
        }
    }
    for idx in (start_pos..=max_start).chain(0..start_pos) {
        if (0..needle.len()).all(|j| doc[idx + j].trim() == needle[j].trim()) {
            return Some(idx);
        }
    }
    None
}

pub(super) fn find_closest_lines(content: &str, target: &str) -> String {
    let first_non_empty = target
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("");
    if first_non_empty.is_empty() {
        return String::new();
    }
    let needle_lower = first_non_empty.to_ascii_lowercase();
    let tokens: Vec<&str> = needle_lower
        .split_whitespace()
        .filter(|t| t.len() >= 3)
        .take(4)
        .collect();

    let mut candidates = Vec::new();
    for (idx, line) in content.lines().enumerate() {
        let lower = line.to_ascii_lowercase();
        let hit = lower.contains(&needle_lower)
            || (!tokens.is_empty() && tokens.iter().all(|t| lower.contains(t)));
        if hit {
            candidates.push(format!("  {}: {}", idx + 1, line));
            if candidates.len() >= 5 {
                break;
            }
        }
    }
    if candidates.is_empty() {
        let preview: Vec<String> = content
            .lines()
            .take(5)
            .enumerate()
            .map(|(i, l)| format!("  {}: {l}", i + 1))
            .collect();
        if preview.is_empty() {
            return " (file is empty)".to_string();
        }
        return format!("\nFirst lines of file:\n{}", preview.join("\n"));
    }
    format!("\nClosest lines in file:\n{}", candidates.join("\n"))
}

pub(super) fn read_gitignore_patterns(root: &Path) -> Vec<String> {
    let path = root.join(".gitignore");
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    parse_gitignore_patterns(&text)
}

pub(super) fn parse_gitignore_patterns(text: &str) -> Vec<String> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#') && !l.starts_with('!'))
        .map(|l| l.trim_matches('/').to_string())
        .filter(|l| !l.is_empty())
        .collect()
}

pub(super) fn is_gitignored(rel_path: &str, file_name: &str, patterns: &[String]) -> bool {
    for pat in patterns {
        if pat == file_name || pat == rel_path {
            return true;
        }
        if glob_matches(pat, rel_path) || (!pat.contains('/') && glob_matches(pat, file_name)) {
            return true;
        }
    }
    false
}

pub(super) fn collect_workspace_files(
    root: &Path,
    current: &Path,
    include_hidden: bool,
    include_ignored: bool,
    gitignore_patterns: &[String],
    out: &mut Vec<String>,
) {
    let Ok(rd) = std::fs::read_dir(current) else {
        return;
    };
    let mut entries: Vec<std::fs::DirEntry> = rd.flatten().collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let name_os = entry.file_name();
        let Some(name) = name_os.to_str() else {
            continue;
        };
        if !include_hidden && name.starts_with('.') {
            continue;
        }
        if !include_ignored && DEFAULT_IGNORED_DIRS.contains(&name) {
            continue;
        }
        let path = entry.path();
        let Ok(rel) = path.strip_prefix(root) else {
            continue;
        };
        let rel_str = rel.to_string_lossy().replace('\\', "/");
        if !include_ignored && is_gitignored(&rel_str, name, gitignore_patterns) {
            continue;
        }
        let Ok(ft) = entry.file_type() else {
            continue;
        };
        if ft.is_dir() {
            collect_workspace_files(
                root,
                &path,
                include_hidden,
                include_ignored,
                gitignore_patterns,
                out,
            );
        } else if ft.is_file() {
            out.push(rel_str);
        }
    }
}

pub(super) fn glob_matches(pattern: &str, text: &str) -> bool {
    if let Some(open) = pattern.find('{')
        && let Some(close_rel) = pattern[open + 1..].find('}')
    {
        let close = open + 1 + close_rel;
        let prefix = &pattern[..open];
        let suffix = &pattern[close + 1..];
        for alt in pattern[open + 1..close].split(',') {
            let expanded = format!("{prefix}{alt}{suffix}");
            if glob_matches(&expanded, text) {
                return true;
            }
        }
        return false;
    }
    let pat_chars: Vec<char> = pattern.chars().collect();
    let txt_chars: Vec<char> = text.chars().collect();
    glob_match_chars(&pat_chars, 0, &txt_chars, 0)
}

pub(super) fn glob_match_chars(pat: &[char], mut pi: usize, txt: &[char], mut ti: usize) -> bool {
    while pi < pat.len() {
        if pi + 1 < pat.len() && pat[pi] == '*' && pat[pi + 1] == '*' {
            pi += 2;
            if pi < pat.len() && pat[pi] == '/' {
                pi += 1;
                if glob_match_chars(pat, pi, txt, ti) {
                    return true;
                }
                for idx in ti..txt.len() {
                    if txt[idx] == '/' && glob_match_chars(pat, pi, txt, idx + 1) {
                        return true;
                    }
                }
                return false;
            }
            if pi == pat.len() {
                return true;
            }
            for idx in ti..=txt.len() {
                if glob_match_chars(pat, pi, txt, idx) {
                    return true;
                }
            }
            return false;
        }
        match pat[pi] {
            '*' => {
                pi += 1;
                for idx in ti..=txt.len() {
                    if idx > ti && txt[idx - 1] == '/' {
                        break;
                    }
                    if glob_match_chars(pat, pi, txt, idx) {
                        return true;
                    }
                }
                return false;
            }
            '?' => {
                if ti >= txt.len() || txt[ti] == '/' {
                    return false;
                }
                pi += 1;
                ti += 1;
            }
            '[' => {
                if ti >= txt.len() || txt[ti] == '/' {
                    return false;
                }
                if let Some((matched, next_pi)) = match_char_class(pat, pi + 1, txt[ti], false) {
                    if !matched {
                        return false;
                    }
                    pi = next_pi;
                    ti += 1;
                } else {
                    if pat[pi] != txt[ti] {
                        return false;
                    }
                    pi += 1;
                    ti += 1;
                }
            }
            c => {
                if ti >= txt.len() || txt[ti] != c {
                    return false;
                }
                pi += 1;
                ti += 1;
            }
        }
    }
    ti == txt.len()
}

pub(super) fn match_char_class(
    pat: &[char],
    mut idx: usize,
    ch: char,
    case_insensitive: bool,
) -> Option<(bool, usize)> {
    if idx >= pat.len() {
        return None;
    }
    let negated = if pat[idx] == '!' || pat[idx] == '^' {
        idx += 1;
        true
    } else {
        false
    };
    let target = if case_insensitive {
        ch.to_ascii_lowercase()
    } else {
        ch
    };
    let mut found = false;
    let mut closed = false;
    while idx < pat.len() {
        if pat[idx] == ']' {
            closed = true;
            idx += 1;
            break;
        }
        let mut start = pat[idx];
        if start == '\\' && idx + 1 < pat.len() {
            idx += 1;
            start = pat[idx];
        }
        if idx + 2 < pat.len() && pat[idx + 1] == '-' && pat[idx + 2] != ']' {
            let end = pat[idx + 2];
            let (s, e) = if case_insensitive {
                (start.to_ascii_lowercase(), end.to_ascii_lowercase())
            } else {
                (start, end)
            };
            if target >= s && target <= e {
                found = true;
            }
            idx += 3;
        } else {
            let s = if case_insensitive {
                start.to_ascii_lowercase()
            } else {
                start
            };
            if target == s {
                found = true;
            }
            idx += 1;
        }
    }
    if !closed {
        return None;
    }
    Some((if negated { !found } else { found }, idx))
}

pub(super) enum GrepMatcher {
    Literal {
        needle: String,
        case_insensitive: bool,
    },
    Regex {
        node: RegexNode,
        case_insensitive: bool,
    },
}

#[derive(Clone)]
pub(super) enum RegexNode {
    Alt(Vec<RegexSeq>),
}

#[derive(Clone)]
pub(super) struct RegexSeq {
    pieces: Vec<RegexPiece>,
}

#[derive(Clone)]
pub(super) struct RegexPiece {
    atom: RegexAtom,
    min: usize,
    max: Option<usize>,
}

#[derive(Clone)]
pub(super) enum RegexAtom {
    AnchorStart,
    AnchorEnd,
    WordBoundary,
    AnyChar,
    Literal(char),
    Digit(bool),
    Word(bool),
    Space(bool),
    Class {
        negated: bool,
        ranges: Vec<(char, char)>,
    },
    Group(Box<RegexNode>),
}

impl GrepMatcher {
    fn compile(query: &str, mut case_insensitive: bool, force_literal: bool) -> Self {
        let mut pat = query;
        if let Some(stripped) = pat.strip_prefix("(?i)") {
            case_insensitive = true;
            pat = stripped;
        }
        if force_literal {
            return Self::Literal {
                needle: if case_insensitive {
                    pat.to_ascii_lowercase()
                } else {
                    pat.to_string()
                },
                case_insensitive,
            };
        }
        let chars: Vec<char> = pat.chars().collect();
        let mut pos = 0;
        if let Some(node) = parse_regex_alt(&chars, &mut pos, false, case_insensitive)
            && pos == chars.len()
        {
            return Self::Regex {
                node,
                case_insensitive,
            };
        }
        Self::Literal {
            needle: if case_insensitive {
                pat.to_ascii_lowercase()
            } else {
                pat.to_string()
            },
            case_insensitive,
        }
    }

    fn is_match(&self, line: &str) -> bool {
        match self {
            Self::Literal {
                needle,
                case_insensitive,
            } => {
                if *case_insensitive {
                    line.to_ascii_lowercase().contains(needle)
                } else {
                    line.contains(needle)
                }
            }
            Self::Regex {
                node,
                case_insensitive,
            } => {
                let chars: Vec<char> = line.chars().collect();
                let mut steps = 0usize;
                for start in 0..=chars.len() {
                    if eval_regex_node(node, &chars, start, *case_insensitive, &mut steps).is_some()
                    {
                        return true;
                    }
                    if steps > 50_000 {
                        break;
                    }
                }
                false
            }
        }
    }
}

pub(super) fn parse_regex_alt(
    chars: &[char],
    pos: &mut usize,
    in_group: bool,
    case_insensitive: bool,
) -> Option<RegexNode> {
    let mut branches = Vec::new();
    loop {
        branches.push(parse_regex_seq(chars, pos, in_group, case_insensitive)?);
        if *pos < chars.len() && chars[*pos] == '|' {
            *pos += 1;
        } else {
            break;
        }
    }
    Some(RegexNode::Alt(branches))
}

pub(super) fn parse_regex_seq(
    chars: &[char],
    pos: &mut usize,
    in_group: bool,
    case_insensitive: bool,
) -> Option<RegexSeq> {
    let mut pieces = Vec::new();
    while *pos < chars.len() {
        let ch = chars[*pos];
        if ch == '|' || (in_group && ch == ')') {
            break;
        }
        if ch == ')' && !in_group {
            return None;
        }
        let atom = match ch {
            '^' => {
                *pos += 1;
                RegexAtom::AnchorStart
            }
            '$' => {
                *pos += 1;
                RegexAtom::AnchorEnd
            }
            '.' => {
                *pos += 1;
                RegexAtom::AnyChar
            }
            '(' => {
                *pos += 1;
                if *pos + 1 < chars.len() && chars[*pos] == '?' && chars[*pos + 1] == ':' {
                    *pos += 2;
                }
                let inner = parse_regex_alt(chars, pos, true, case_insensitive)?;
                if *pos >= chars.len() || chars[*pos] != ')' {
                    return None;
                }
                *pos += 1;
                RegexAtom::Group(Box::new(inner))
            }
            '[' => {
                *pos += 1;
                parse_regex_class(chars, pos, case_insensitive)?
            }
            '\\' => {
                *pos += 1;
                if *pos >= chars.len() {
                    return None;
                }
                let esc = chars[*pos];
                *pos += 1;
                match esc {
                    'd' => RegexAtom::Digit(true),
                    'D' => RegexAtom::Digit(false),
                    'w' => RegexAtom::Word(true),
                    'W' => RegexAtom::Word(false),
                    's' => RegexAtom::Space(true),
                    'S' => RegexAtom::Space(false),
                    'b' => RegexAtom::WordBoundary,
                    'n' => RegexAtom::Literal('\n'),
                    'r' => RegexAtom::Literal('\r'),
                    't' => RegexAtom::Literal('\t'),
                    other => RegexAtom::Literal(if case_insensitive {
                        other.to_ascii_lowercase()
                    } else {
                        other
                    }),
                }
            }
            '*' | '+' | '?' => return None,
            other => {
                *pos += 1;
                RegexAtom::Literal(if case_insensitive {
                    other.to_ascii_lowercase()
                } else {
                    other
                })
            }
        };
        let (min, max) = if *pos < chars.len() {
            match chars[*pos] {
                '*' => {
                    *pos += 1;
                    if *pos < chars.len() && chars[*pos] == '?' {
                        *pos += 1;
                    }
                    (0, None)
                }
                '+' => {
                    *pos += 1;
                    if *pos < chars.len() && chars[*pos] == '?' {
                        *pos += 1;
                    }
                    (1, None)
                }
                '?' => {
                    *pos += 1;
                    if *pos < chars.len() && chars[*pos] == '?' {
                        *pos += 1;
                    }
                    (0, Some(1))
                }
                '{' => {
                    if let Some((m, mx, next_p)) = parse_bounded_rep(chars, *pos + 1) {
                        *pos = next_p;
                        (m, mx)
                    } else {
                        (1, Some(1))
                    }
                }
                _ => (1, Some(1)),
            }
        } else {
            (1, Some(1))
        };
        pieces.push(RegexPiece { atom, min, max });
    }
    Some(RegexSeq { pieces })
}

pub(super) fn parse_bounded_rep(
    chars: &[char],
    mut idx: usize,
) -> Option<(usize, Option<usize>, usize)> {
    let start = idx;
    while idx < chars.len() && chars[idx].is_ascii_digit() {
        idx += 1;
    }
    if idx == start || idx >= chars.len() {
        return None;
    }
    let min: usize = chars[start..idx].iter().collect::<String>().parse().ok()?;
    if chars[idx] == '}' {
        return Some((min, Some(min), idx + 1));
    }
    if chars[idx] != ',' {
        return None;
    }
    idx += 1;
    let max_start = idx;
    while idx < chars.len() && chars[idx].is_ascii_digit() {
        idx += 1;
    }
    if idx >= chars.len() || chars[idx] != '}' {
        return None;
    }
    let max = if idx == max_start {
        None
    } else {
        Some(
            chars[max_start..idx]
                .iter()
                .collect::<String>()
                .parse::<usize>()
                .ok()?,
        )
    };
    Some((min, max, idx + 1))
}

pub(super) fn parse_regex_class(
    chars: &[char],
    pos: &mut usize,
    case_insensitive: bool,
) -> Option<RegexAtom> {
    if *pos >= chars.len() {
        return None;
    }
    let negated = if chars[*pos] == '^' {
        *pos += 1;
        true
    } else {
        false
    };
    let mut ranges = Vec::new();
    let mut closed = false;
    while *pos < chars.len() {
        if chars[*pos] == ']' && !ranges.is_empty() {
            closed = true;
            *pos += 1;
            break;
        }
        let mut c = chars[*pos];
        *pos += 1;
        if c == '\\' {
            if *pos >= chars.len() {
                return None;
            }
            let esc = chars[*pos];
            *pos += 1;
            match esc {
                'd' => {
                    ranges.push(('0', '9'));
                    continue;
                }
                's' => {
                    for sp in [' ', '\t', '\n', '\r'] {
                        ranges.push((sp, sp));
                    }
                    continue;
                }
                'w' => {
                    ranges.push(('a', 'z'));
                    ranges.push(('A', 'Z'));
                    ranges.push(('0', '9'));
                    ranges.push(('_', '_'));
                    continue;
                }
                other => c = other,
            }
        }
        if *pos + 1 < chars.len() && chars[*pos] == '-' && chars[*pos + 1] != ']' {
            *pos += 1;
            let mut end = chars[*pos];
            *pos += 1;
            if end == '\\' && *pos < chars.len() {
                end = chars[*pos];
                *pos += 1;
            }
            let (s, e) = if case_insensitive {
                (c.to_ascii_lowercase(), end.to_ascii_lowercase())
            } else {
                (c, end)
            };
            ranges.push((s, e));
        } else {
            let s = if case_insensitive {
                c.to_ascii_lowercase()
            } else {
                c
            };
            ranges.push((s, s));
        }
    }
    if !closed {
        return None;
    }
    Some(RegexAtom::Class { negated, ranges })
}

pub(super) fn is_word_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

pub(super) fn eval_regex_node(
    node: &RegexNode,
    chars: &[char],
    idx: usize,
    case_insensitive: bool,
    steps: &mut usize,
) -> Option<usize> {
    let RegexNode::Alt(branches) = node;
    for seq in branches {
        if let Some(end) = eval_regex_seq(&seq.pieces, chars, idx, case_insensitive, steps) {
            return Some(end);
        }
    }
    None
}

pub(super) fn eval_regex_seq(
    pieces: &[RegexPiece],
    chars: &[char],
    idx: usize,
    case_insensitive: bool,
    steps: &mut usize,
) -> Option<usize> {
    *steps += 1;
    if *steps > 50_000 {
        return None;
    }
    let Some((first, rest)) = pieces.split_first() else {
        return Some(idx);
    };
    let max_rep = first.max.unwrap_or(chars.len().saturating_sub(idx) + 1);
    let mut positions = vec![idx];
    let mut cur = idx;
    for _ in 0..max_rep {
        if let Some(next_idx) = eval_regex_atom(&first.atom, chars, cur, case_insensitive, steps) {
            if next_idx == cur && positions.len() > first.min {
                break;
            }
            positions.push(next_idx);
            cur = next_idx;
        } else {
            break;
        }
    }
    if positions.len() <= first.min {
        return None;
    }
    for &pos in positions[first.min..].iter().rev() {
        if let Some(end) = eval_regex_seq(rest, chars, pos, case_insensitive, steps) {
            return Some(end);
        }
    }
    None
}

pub(super) fn eval_regex_atom(
    atom: &RegexAtom,
    chars: &[char],
    idx: usize,
    case_insensitive: bool,
    steps: &mut usize,
) -> Option<usize> {
    match atom {
        RegexAtom::AnchorStart => (idx == 0).then_some(idx),
        RegexAtom::AnchorEnd => (idx == chars.len()).then_some(idx),
        RegexAtom::WordBoundary => {
            let prev_w = idx > 0 && is_word_char(chars[idx - 1]);
            let next_w = idx < chars.len() && is_word_char(chars[idx]);
            (prev_w != next_w).then_some(idx)
        }
        RegexAtom::AnyChar => (idx < chars.len() && chars[idx] != '\n').then_some(idx + 1),
        RegexAtom::Literal(expected) => {
            if idx >= chars.len() {
                return None;
            }
            let actual = if case_insensitive {
                chars[idx].to_ascii_lowercase()
            } else {
                chars[idx]
            };
            (actual == *expected).then_some(idx + 1)
        }
        RegexAtom::Digit(want) => {
            if idx >= chars.len() {
                return None;
            }
            (chars[idx].is_ascii_digit() == *want).then_some(idx + 1)
        }
        RegexAtom::Word(want) => {
            if idx >= chars.len() {
                return None;
            }
            (is_word_char(chars[idx]) == *want).then_some(idx + 1)
        }
        RegexAtom::Space(want) => {
            if idx >= chars.len() {
                return None;
            }
            (chars[idx].is_ascii_whitespace() == *want).then_some(idx + 1)
        }
        RegexAtom::Class { negated, ranges } => {
            if idx >= chars.len() {
                return None;
            }
            let c = if case_insensitive {
                chars[idx].to_ascii_lowercase()
            } else {
                chars[idx]
            };
            let hit = ranges.iter().any(|&(s, e)| c >= s && c <= e);
            let ok = if *negated { !hit } else { hit };
            ok.then_some(idx + 1)
        }
        RegexAtom::Group(inner) => eval_regex_node(inner, chars, idx, case_insensitive, steps),
    }
}
