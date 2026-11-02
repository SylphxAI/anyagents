//! SOTA Agent-Computer Interface (ACI) primitives for `SylphxHarness`:
//! 1. Process-group isolated `bash` execution with enforced `timeout_ms` and non-blocking pipe readers.
//! 2. UTF-8-safe Head + Tail middle-out observation truncation (`arXiv:2508.21433`).
//! 3. Line-numbered, bounded `read_file` with automatic line-number prefix stripping in `edit_file`.
//! 4. Multi-edit (`edits: [...]`) and cascade fuzzy matching (exact -> CRLF -> trim_end -> indentation-tolerant) in `edit_file`.
//! 5. Universal `apply_patch` engine supporting OpenAI V4A patches (`*** Begin Patch`), `git apply --recount -C1`, `patch --fuzz=3`, and pure-Rust context-addressed unified diff fallback.
//! 6. Dependency-aware parallel tool batch classifier (`can_parallelize_tool_batch`).

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use super::workspace;
use std::time::{Duration, Instant};

use serde_json::Value;

use super::auth::truncate_tool_output_middle_utf8;

/// Determines whether a batch of `function_call` items emitted in a single
/// `/v1/responses` step can be safely executed in parallel via `std::thread::scope`.
pub(super) fn can_parallelize_tool_batch(calls: &[(String, String, Value)]) -> bool {
    if calls.len() <= 1 {
        return false;
    }
    let mut seen_write_paths: Vec<String> = Vec::new();
    let mut has_file_mutation = false;
    let mut has_bash = false;

    for (_, fn_name, args) in calls {
        let norm = fn_name
            .strip_prefix("mcp__anyagents__")
            .unwrap_or(fn_name.as_str());
        match norm {
            // Lifecycle and compaction transitions must run sequentially.
            "archive" | "context_window_open" | "context_edit" | "apply_patch" | "sandbox" => {
                return false;
            }
            "agents" | "subagents" => {
                if args
                    .get("action")
                    .and_then(Value::as_str)
                    .is_some_and(|a| a.eq_ignore_ascii_case("archive"))
                {
                    return false;
                }
            }
            "write_file" | "edit_file" => {
                has_file_mutation = true;
                let Some(p) = args
                    .get("path")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                else {
                    return false;
                };
                if seen_write_paths.iter().any(|existing| existing == p) {
                    return false;
                }
                seen_write_paths.push(p.to_string());
            }
            "bash" | "shell" => {
                has_bash = true;
            }
            _ => {}
        }
    }
    // Avoid racing concurrent bash scripts against in-flight file writes/edits.
    if has_file_mutation && has_bash {
        return false;
    }
    true
}

/// Executes a local `bash -c` command in its own process group with enforced
/// `timeout_ms` (default 120,000 ms) and UTF-8-safe head+tail middle truncation.
pub(super) fn execute_bash_with_timeout(
    cmd_str: &str,
    work_dir: &Path,
    env: &[(String, String)],
    timeout_ms: u64,
) -> String {
    let bounded_timeout_ms = timeout_ms.clamp(500, 600_000);
    let mut cmd = match workspace::command(Path::new("bash"), env) {
        Ok(cmd) => cmd,
        Err(e) => return format!("error executing bash: {e}"),
    };
    cmd.arg("-c")
        .arg(cmd_str)
        .current_dir(work_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        cmd.process_group(0);
    }

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return format!("error executing bash: {e}"),
    };

    let stdout_pipe = child.stdout.take();
    let stderr_pipe = child.stderr.take();
    let out_handle = std::thread::spawn(move || {
        let mut buf = Vec::new();
        if let Some(mut r) = stdout_pipe {
            let _ = r.read_to_end(&mut buf);
        }
        buf
    });
    let err_handle = std::thread::spawn(move || {
        let mut buf = Vec::new();
        if let Some(mut r) = stderr_pipe {
            let _ = r.read_to_end(&mut buf);
        }
        buf
    });

    let deadline = Instant::now() + Duration::from_millis(bounded_timeout_ms);
    let mut timed_out = false;
    let exit_code = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status.code().unwrap_or(-1),
            Ok(None) => {
                if Instant::now() >= deadline {
                    timed_out = true;
                    #[cfg(unix)]
                    unsafe {
                        let pgid = child.id() as i32;
                        if pgid > 0 {
                            let _ = libc::kill(-pgid, libc::SIGKILL);
                        }
                    }
                    let _ = child.kill();
                    let _ = child.wait();
                    break -1;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                break -1;
            }
        }
    };

    let stdout_bytes = out_handle.join().unwrap_or_default();
    let stderr_bytes = err_handle.join().unwrap_or_default();
    let stdout = String::from_utf8_lossy(&stdout_bytes);
    let stderr = String::from_utf8_lossy(&stderr_bytes);

    let mut combined = format!("exit_code: {exit_code}\n");
    if !stdout.is_empty() {
        combined.push_str(&stdout);
    }
    if !stderr.is_empty() {
        if !combined.ends_with('\n') {
            combined.push('\n');
        }
        combined.push_str("stderr:\n");
        combined.push_str(&stderr);
    }
    if timed_out {
        if !combined.ends_with('\n') {
            combined.push('\n');
        }
        combined.push_str(&format!(
            "[command timed out after {bounded_timeout_ms}ms and was terminated]"
        ));
    }
    truncate_tool_output_middle_utf8(combined, 30_000, 14_000, 14_000)
}

/// Formats `read_file` output with a metadata header, 1-based line numbers, and
/// safe default bounds on un-ranged reads.
pub(super) fn format_read_file_output(
    display_path: &str,
    content: &str,
    start_line: Option<u64>,
    end_line: Option<u64>,
) -> String {
    let lines: Vec<&str> = content.lines().collect();
    let total = lines.len();
    if total == 0 {
        return format!("[file: {display_path} (0 lines, empty)]");
    }
    let start = start_line.unwrap_or(1).max(1) as usize;
    if start > total {
        return format!(
            "[file: {display_path} (requested start_line {start} > total {total} lines)]"
        );
    }
    let max_default_lines = 2_000usize;
    let end = match end_line {
        Some(e) => (e as usize).min(total).max(start),
        None => (start + max_default_lines - 1).min(total),
    };
    let width = end.to_string().len().max(4);
    let mut out = String::with_capacity((end - start + 2) * 48);
    out.push_str(&format!(
        "[file: {display_path} (lines {start}..{end} of {total})]\n"
    ));
    for (idx, line) in lines[start - 1..end].iter().enumerate() {
        let line_no = start + idx;
        out.push_str(&format!("{line_no:>width$}: {line}\n"));
    }
    if end < total && end_line.is_none() {
        out.push_str(&format!(
            "... [{} more lines not shown; pass start_line={} to continue reading]\n",
            total - end,
            end + 1
        ));
    }
    truncate_tool_output_middle_utf8(out, 64_000, 30_000, 30_000)
}

/// Native directory listing for local workspaces without spawning a `bash` subprocess.
pub(super) fn list_dir_native(work_dir: &Path, raw_path: &str) -> String {
    let target = if Path::new(raw_path).is_absolute() {
        PathBuf::from(raw_path)
    } else {
        work_dir.join(raw_path)
    };
    let entries = match std::fs::read_dir(&target) {
        Ok(rd) => rd,
        Err(e) => return format!("error listing {}: {e}", target.display()),
    };
    let mut dirs = Vec::new();
    let mut files = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Ok(ft) = entry.file_type() {
            if ft.is_dir() {
                dirs.push(format!("{name}/"));
            } else if let Ok(meta) = entry.metadata() {
                files.push(format!("{name} ({} B)", meta.len()));
            } else {
                files.push(name);
            }
        } else {
            files.push(name);
        }
    }
    dirs.sort();
    files.sort();
    let total = dirs.len() + files.len();
    let mut out = format!("[dir: {raw_path} ({total} entries)]\n");
    for d in dirs.into_iter().chain(files).take(200) {
        out.push_str(&d);
        out.push('\n');
    }
    out
}

/// Strips accidental `<line_no>: ` or `<line_no> | ` prefixes when a model copies
/// numbered lines from `read_file` into `edit_file`.
fn strip_copied_line_numbers(text: &str) -> Option<String> {
    let lines: Vec<&str> = text.split('\n').collect();
    if lines.is_empty() {
        return None;
    }
    let mut stripped = Vec::with_capacity(lines.len());
    let mut any_stripped = false;
    for line in &lines {
        let trimmed_start = line.trim_start_matches(' ');
        let digits_len = trimmed_start
            .bytes()
            .take_while(|b| b.is_ascii_digit())
            .count();
        if digits_len > 0 && digits_len <= 7 {
            let rest = &trimmed_start[digits_len..];
            if let Some(after) = rest
                .strip_prefix(": ")
                .or_else(|| rest.strip_prefix(" | "))
                .or_else(|| rest.strip_prefix(':'))
            {
                stripped.push(after);
                any_stripped = true;
                continue;
            }
        }
        if line.trim().is_empty() {
            stripped.push(*line);
        } else {
            return None;
        }
    }
    any_stripped.then(|| stripped.join("\n"))
}

fn byte_offset_to_line(text: &str, byte_offset: usize) -> usize {
    text[..byte_offset.min(text.len())]
        .bytes()
        .filter(|&b| b == b'\n')
        .count()
        + 1
}

/// Applies one or more `(target, replacement)` edits to `existing` content using
/// a 4-stage cascade matcher:
/// 1. Exact substring match (with uniqueness check unless `replace_all` is true).
/// 2. CRLF-normalized exact match.
/// 3. Line-by-line `trim_end()` match (ignoring trailing spaces/tabs).
/// 4. Line-by-line `trim()` indentation-tolerant block match (for multi-line targets).
///
/// Also automatically retries after stripping accidental `12: ` line-number prefixes.
pub(super) fn apply_edits_to_content(
    existing: &str,
    edits: &[(String, String)],
    replace_all: bool,
) -> Result<(String, Vec<(usize, usize)>), String> {
    if edits.is_empty() {
        return Err("missing `target`/`replacement` or `edits` array".to_string());
    }
    let mut current = existing.to_string();
    let mut edited_ranges = Vec::with_capacity(edits.len());

    for (idx, (raw_target, raw_replacement)) in edits.iter().enumerate() {
        if raw_target.is_empty() {
            return Err(format!("edit #{}: `target` cannot be empty", idx + 1));
        }
        let mut candidates = vec![(raw_target.clone(), raw_replacement.clone())];
        if let Some(stripped_t) = strip_copied_line_numbers(raw_target) {
            let stripped_r = strip_copied_line_numbers(raw_replacement)
                .unwrap_or_else(|| raw_replacement.clone());
            candidates.push((stripped_t, stripped_r));
        }

        let mut last_err = String::new();
        let mut applied = false;
        for (target, replacement) in candidates {
            match apply_single_edit_cascade(&current, &target, &replacement, replace_all) {
                Ok((updated, range)) => {
                    current = updated;
                    edited_ranges.push(range);
                    applied = true;
                    break;
                }
                Err(e) => {
                    if last_err.is_empty() {
                        last_err = e;
                    }
                }
            }
        }
        if !applied {
            return Err(last_err);
        }
    }
    Ok((current, edited_ranges))
}

fn apply_single_edit_cascade(
    content: &str,
    target: &str,
    replacement: &str,
    replace_all: bool,
) -> Result<(String, (usize, usize)), String> {
    // Pass 1: Exact match
    let matches: Vec<usize> = content.match_indices(target).map(|(i, _)| i).collect();
    if matches.len() == 1 {
        let pos = matches[0];
        let start_line = byte_offset_to_line(content, pos);
        let new_lines = replacement.lines().count().max(1);
        let end_line = start_line + new_lines - 1;
        let mut updated = String::with_capacity(content.len() - target.len() + replacement.len());
        updated.push_str(&content[..pos]);
        updated.push_str(replacement);
        updated.push_str(&content[pos + target.len()..]);
        return Ok((updated, (start_line, end_line)));
    } else if matches.len() > 1 {
        if replace_all {
            let start_line = byte_offset_to_line(content, matches[0]);
            let updated = content.replace(target, replacement);
            let end_line = byte_offset_to_line(&updated, updated.len());
            return Ok((updated, (start_line, end_line)));
        }
        let line_nums: Vec<String> = matches
            .iter()
            .take(8)
            .map(|&pos| byte_offset_to_line(content, pos).to_string())
            .collect();
        return Err(format!(
            "target string appears {} times (at lines {}); include more surrounding context lines to make `target` unique, or set `replace_all: true`",
            matches.len(),
            line_nums.join(", ")
        ));
    }

    // Pass 2: CRLF-normalized match
    let uses_crlf = content.contains("\r\n");
    let norm_content = content.replace("\r\n", "\n");
    let norm_target = target.replace("\r\n", "\n");
    let norm_replacement = replacement.replace("\r\n", "\n");
    if norm_content != content || norm_target != target {
        let crlf_matches: Vec<usize> = norm_content
            .match_indices(&norm_target)
            .map(|(i, _)| i)
            .collect();
        if crlf_matches.len() == 1 || (crlf_matches.len() > 1 && replace_all) {
            let pos = crlf_matches[0];
            let start_line = byte_offset_to_line(&norm_content, pos);
            let new_lines = norm_replacement.lines().count().max(1);
            let updated_lf = if replace_all {
                norm_content.replace(&norm_target, &norm_replacement)
            } else {
                let mut s = String::new();
                s.push_str(&norm_content[..pos]);
                s.push_str(&norm_replacement);
                s.push_str(&norm_content[pos + norm_target.len()..]);
                s
            };
            let final_str = if uses_crlf {
                updated_lf.replace('\n', "\r\n")
            } else {
                updated_lf
            };
            return Ok((final_str, (start_line, start_line + new_lines - 1)));
        } else if crlf_matches.len() > 1 {
            let line_nums: Vec<String> = crlf_matches
                .iter()
                .take(8)
                .map(|&pos| byte_offset_to_line(&norm_content, pos).to_string())
                .collect();
            return Err(format!(
                "target string appears {} times (at lines {}); include more context lines or set `replace_all: true`",
                crlf_matches.len(),
                line_nums.join(", ")
            ));
        }
    }

    // Pass 3 & 4: Line-by-line trailing-whitespace-tolerant and indentation-tolerant matching
    let doc_lines: Vec<&str> = norm_content.split('\n').collect();
    let target_lines: Vec<&str> = norm_target.split('\n').collect();
    if target_lines.is_empty() || target_lines.len() > doc_lines.len() {
        return Err("target string not found in file".to_string());
    }

    // Pass 3: `trim_end()` match
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
        return Ok((joined, (i + 1, i + repl_lines.len().max(1))));
    } else if trim_end_hits.len() > 1 {
        let line_nums: Vec<String> = trim_end_hits
            .iter()
            .take(8)
            .map(|&i| (i + 1).to_string())
            .collect();
        return Err(format!(
            "target string appears {} times (at lines {}); include more surrounding context lines",
            trim_end_hits.len(),
            line_nums.join(", ")
        ));
    }

    // Pass 4: `trim()` indentation-tolerant match (when target has at least one non-trivial line)
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
            // Compute leading indentation offset from the first non-empty line in the matched block
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
            new_doc.extend(repl_lines.iter().cloned());
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
            return Ok((joined, (i + 1, i + repl_lines.len().max(1))));
        }
    }

    Err("target string not found in file".to_string())
}

/// Universal `apply_patch` engine for local workspaces:
/// 1. Native OpenAI V4A Patch format (`*** Begin Patch` / `*** Update File:` / `*** Add File:` / `*** Delete File:`).
/// 2. `git apply --recount -C1` and `patch -p1 --fuzz=3` for unified diffs.
/// 3. Pure-Rust context-addressed unified diff fallback when hunk line numbers/counts are malformed.
pub(super) fn apply_patch_universal(
    work_dir: &Path,
    patch_text: &str,
    env: &[(String, String)],
) -> Result<String, String> {
    let trimmed = patch_text.trim();
    if trimmed.is_empty() {
        return Err("empty patch".to_string());
    }

    if is_v4a_patch(trimmed) {
        return apply_v4a_patch_local(work_dir, trimmed, env);
    }

    // Every external attempt uses the same trusted workspace principal. Feed
    // patches over stdin; a server-owned temp file may be unreadable by a seat.
    let mut patch_with_nl = patch_text.to_string();
    if !patch_with_nl.ends_with('\n') {
        patch_with_nl.push('\n');
    }
    let git_attempts: &[&[&str]] = &[
        &["apply", "--recount", "-C1", "--whitespace=nowarn"],
        &["apply", "--recount", "-p0", "-C1", "--whitespace=nowarn"],
        &["apply", "--recount", "--whitespace=nowarn"],
    ];
    for args in git_attempts {
        let mut cmd = workspace::command(Path::new("git"), env)?;
        cmd.args(*args).current_dir(work_dir);
        if let Ok(out) = workspace::output(cmd, patch_with_nl.as_bytes())
            && out.status.success()
        {
            return Ok("exit_code: 0\npatched successfully (git apply)".to_string());
        }
    }
    for p_level in ["-p1", "-p0"] {
        let mut cmd = workspace::command(Path::new("patch"), env)?;
        cmd.args([
            p_level,
            "--fuzz=3",
            "--no-backup-if-mismatch",
            "--batch",
            "--silent",
        ])
        .current_dir(work_dir);
        if let Ok(out) = workspace::output(cmd, patch_with_nl.as_bytes())
            && out.status.success()
        {
            return Ok("exit_code: 0\npatched successfully (patch --fuzz=3)".to_string());
        }
    }

    // Pure-Rust context-addressed unified diff fallback (ignores broken @@ line numbers/counts)
    apply_unified_diff_context_fallback(work_dir, &patch_with_nl, env)
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

fn resolve_rel_path(work_dir: &Path, raw: &str) -> PathBuf {
    let clean = raw
        .trim()
        .strip_prefix("a/")
        .or_else(|| raw.trim().strip_prefix("b/"))
        .unwrap_or(raw.trim());
    let p = Path::new(clean);
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        work_dir.join(p)
    }
}

fn apply_v4a_patch_local(
    work_dir: &Path,
    patch_text: &str,
    env: &[(String, String)],
) -> Result<String, String> {
    let normalized = patch_text.replace("\r\n", "\n");
    let lines: Vec<&str> = normalized.lines().collect();
    let mut i = 0usize;
    let mut summary = Vec::new();

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
            let target = resolve_rel_path(work_dir, rel);
            if let Some(p) = target.parent() {
                workspace::create_dir_all(p, env)?;
            }
            let mut content = body_lines.join("\n");
            if !content.is_empty() && !content.ends_with('\n') {
                content.push('\n');
            }
            workspace::write(&target, content, env)
                .map_err(|e| format!("failed writing `{rel}`: {e}"))?;
            summary.push(format!("added {rel}"));
            continue;
        }
        if let Some(rel) = line.strip_prefix("*** Delete File:").map(str::trim) {
            i += 1;
            while i < lines.len() && !lines[i].trim().starts_with("*** ") {
                i += 1;
            }
            let target = resolve_rel_path(work_dir, rel);
            workspace::remove_if_exists(&target, env)
                .map_err(|e| format!("failed deleting `{rel}`: {e}"))?;
            summary.push(format!("deleted {rel}"));
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
            let target = resolve_rel_path(work_dir, rel);
            let existing = workspace::read_to_string(&target, env)
                .map_err(|e| format!("failed reading `{rel}`: {e}"))?;
            let updated = apply_context_hunks_to_content(&existing, &hunk_lines)
                .map_err(|e| format!("failed patching `{rel}`: {e}"))?;
            if let Some(dest_rel) = move_to {
                let dest = resolve_rel_path(work_dir, &dest_rel);
                if let Some(p) = dest.parent() {
                    workspace::create_dir_all(p, env)?;
                }
                workspace::write(&dest, &updated, env)
                    .map_err(|e| format!("failed writing `{dest_rel}`: {e}"))?;
                if dest != target {
                    workspace::remove_if_exists(&target, env)?;
                }
                summary.push(format!("updated {rel} -> {dest_rel}"));
            } else {
                workspace::write(&target, &updated, env)
                    .map_err(|e| format!("failed writing `{rel}`: {e}"))?;
                summary.push(format!("updated {rel}"));
            }
            continue;
        }
        i += 1;
    }

    if summary.is_empty() {
        return Err("no valid V4A file operations found in patch".to_string());
    }
    Ok(format!("exit_code: 0\n{}", summary.join("\n")))
}

fn apply_unified_diff_context_fallback(
    work_dir: &Path,
    patch_text: &str,
    env: &[(String, String)],
) -> Result<String, String> {
    let normalized = patch_text.replace("\r\n", "\n");
    let lines: Vec<&str> = normalized.lines().collect();
    let mut i = 0usize;
    let mut summary = Vec::new();

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
                let target = resolve_rel_path(work_dir, old_header);
                workspace::remove_if_exists(&target, env)
                    .map_err(|e| format!("failed deleting `{old_header}`: {e}"))?;
                summary.push(format!("deleted {old_header}"));
            } else if old_header == "/dev/null" {
                let target = resolve_rel_path(work_dir, new_header);
                if let Some(p) = target.parent() {
                    workspace::create_dir_all(p, env)?;
                }
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
                workspace::write(&target, content, env)
                    .map_err(|e| format!("failed writing `{new_header}`: {e}"))?;
                summary.push(format!("added {new_header}"));
            } else {
                let rel = if !new_header.is_empty() {
                    new_header
                } else {
                    old_header
                };
                let target = resolve_rel_path(work_dir, rel);
                let existing = workspace::read_to_string(&target, env)
                    .map_err(|e| format!("failed reading `{rel}`: {e}"))?;
                let updated = apply_context_hunks_to_content(&existing, &hunk_lines)
                    .map_err(|e| format!("failed applying hunk to `{rel}`: {e}"))?;
                workspace::write(&target, updated, env)
                    .map_err(|e| format!("failed writing `{rel}`: {e}"))?;
                summary.push(format!("patched {rel}"));
            }
            continue;
        }
        i += 1;
    }

    if summary.is_empty() {
        return Err("patch failed: could not parse unified diff or V4A hunks".to_string());
    }
    Ok(format!("exit_code: 0\n{}", summary.join("\n")))
}

/// Applies a sequence of `@@ ... @@` hunks (from either V4A or unified diff) by matching
/// context (` `) and removed (`-`) lines against `existing` content, completely independent
/// of brittle line numbers in the `@@` header.
fn apply_context_hunks_to_content(existing: &str, hunk_lines: &[&str]) -> Result<String, String> {
    let had_trailing_newline = existing.ends_with('\n');
    let mut doc_lines: Vec<String> = existing
        .replace("\r\n", "\n")
        .lines()
        .map(String::from)
        .collect();

    // Split `hunk_lines` into individual hunks at `@@` boundaries
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
            // Pure insertion at cursor
            let pos = search_cursor.min(doc_lines.len());
            doc_lines.splice(pos..pos, new_block);
            continue;
        }

        if let Some(pos) = find_subslice_lines(&doc_lines, &old_block, search_cursor) {
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

fn find_subslice_lines(doc: &[String], needle: &[&str], cursor: usize) -> Option<usize> {
    if needle.len() > doc.len() {
        return None;
    }
    let max_start = doc.len() - needle.len();
    let start_pos = cursor.min(max_start);

    // 1. Exact match from cursor forward, then from 0..cursor
    for idx in (start_pos..=max_start).chain(0..start_pos) {
        if (0..needle.len()).all(|j| doc[idx + j] == needle[j]) {
            return Some(idx);
        }
    }
    // 2. Trailing-whitespace-tolerant (`trim_end`) match
    for idx in (start_pos..=max_start).chain(0..start_pos) {
        if (0..needle.len()).all(|j| doc[idx + j].trim_end() == needle[j].trim_end()) {
            return Some(idx);
        }
    }
    // 3. Indentation-tolerant (`trim`) match
    for idx in (start_pos..=max_start).chain(0..start_pos) {
        if (0..needle.len()).all(|j| doc[idx + j].trim() == needle[j].trim()) {
            return Some(idx);
        }
    }
    None
}

/// Builds a self-contained `python3` shell command for `code_mode` (Programmatic Tool
/// Calling / PTC, Cloudflare Code Mode + Anthropic CE-MCP + `arXiv:2605.18747`),
/// injecting a zero-import SDK (`read`, `write`, `edit`, `files`, `grep`, `sh`, `mcp`,
/// `agents`, `send`, `conversation`, `schedule`, `recall`) so intermediate data stays
/// in Python RAM and only printed output enters the LLM context window.
pub(super) fn build_code_mode_command(
    user_code: &str,
    config_dir: &Path,
    mcp_command: &Path,
    mcp_args: &[String],
) -> String {
    let mut mcp_cmd_vec = vec![mcp_command.display().to_string()];
    mcp_cmd_vec.extend(mcp_args.iter().cloned());
    let mcp_cmd_json = serde_json::to_string(&mcp_cmd_vec).unwrap_or_else(|_| "[]".to_string());
    let transcript_path = config_dir.join("sylphx-transcript.jsonl");
    let transcript_json =
        serde_json::to_string(&transcript_path.display().to_string()).unwrap_or_default();

    let prelude = format!(
        r#"import json, os, pathlib, re, subprocess, sys

_MCP_CMD = {mcp_cmd_json}
_TRANSCRIPT_PATH = {transcript_json}

def read(path, start_line=None, end_line=None):
    p = pathlib.Path(path)
    text = p.read_text(encoding="utf-8")
    if start_line is None and end_line is None:
        return text
    lines = text.splitlines()
    s = max(1, int(start_line or 1))
    e = min(len(lines), int(end_line or len(lines)))
    return "\n".join(lines[s - 1:e])

read_file = read

def write(path, content):
    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(str(content), encoding="utf-8")
    return f"wrote {{p}} ({{len(str(content))}} bytes)"

write_file = write

def edit(path, target, replacement, replace_all=False):
    p = pathlib.Path(path)
    src = p.read_text(encoding="utf-8")
    if target not in src:
        raise ValueError(f"target not found in {{path}}")
    updated = src.replace(target, replacement) if replace_all else src.replace(target, replacement, 1)
    p.write_text(updated, encoding="utf-8")
    return f"edited {{p}}"

edit_file = edit

def files(pattern="**/*", path="."):
    proc = subprocess.run(["rg", "--files", "-g", pattern, path], capture_output=True, text=True)
    if proc.returncode == 0 or proc.stdout:
        return [l for l in proc.stdout.splitlines() if l.strip()]
    base = pathlib.Path(path)
    return [str(p) for p in base.glob(pattern) if p.is_file()]

glob = files

def grep(pattern, path=".", glob=None):
    cmd = ["rg", "-n", "--no-heading", "--color=never"]
    if glob:
        cmd.extend(["-g", glob])
    cmd.extend(["--", pattern, path])
    proc = subprocess.run(cmd, capture_output=True, text=True)
    hits = []
    for line in proc.stdout.splitlines():
        parts = line.split(":", 2)
        if len(parts) == 3 and parts[1].isdigit():
            hits.append({{"path": parts[0], "line": int(parts[1]), "text": parts[2]}})
        elif len(parts) >= 2 and parts[0].isdigit():
            hits.append({{"path": path, "line": int(parts[0]), "text": ":".join(parts[1:])}})
    return hits

def sh(command, timeout=120):
    proc = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=timeout)
    return {{"code": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}}

def mcp(tool_name, **kwargs):
    if not _MCP_CMD:
        raise RuntimeError("MCP command not configured")
    reqs = [
        {{"jsonrpc": "2.0", "id": 0, "method": "initialize", "params": {{"protocolVersion": "2024-11-05", "capabilities": {{}}, "clientInfo": {{"name": "sylphx-code-mode", "version": "0.1.0"}}}}}},
        {{"jsonrpc": "2.0", "method": "notifications/initialized"}},
        {{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {{"name": tool_name, "arguments": kwargs}}}},
    ]
    payload = "\n".join(json.dumps(r) for r in reqs) + "\n"
    proc = subprocess.run(_MCP_CMD, input=payload, capture_output=True, text=True)
    for line in reversed(proc.stdout.splitlines()):
        try:
            msg = json.loads(line)
        except Exception:
            continue
        if msg.get("id") == 1:
            if "error" in msg:
                raise RuntimeError(msg["error"].get("message", str(msg["error"])))
            text = (((msg.get("result") or {{}}).get("content") or [{{}}])[0]).get("text", "")
            try:
                return json.loads(text)
            except Exception:
                return text
    return proc.stdout.strip()

def agents(**kwargs):
    return mcp("agents", **kwargs)

def send(target, message, **kwargs):
    return mcp("send", target=target, message=message, **kwargs)

def conversation(**kwargs):
    return mcp("conversation", **kwargs)

def schedule(**kwargs):
    return mcp("schedule", **kwargs)

def recall(query=None, limit=10):
    p = pathlib.Path(_TRANSCRIPT_PATH)
    if not p.is_file():
        return []
    q = (query or "").lower()
    out = []
    for idx, line in enumerate(p.read_text(encoding="utf-8", errors="replace").splitlines()):
        if not q or q in line.lower():
            out.append({{"index": idx, "raw": line}})
    return out[-limit:]
"#
    );

    let full_script = format!("{prelude}\n{user_code}\n");
    let b64 = super::auth::base64_encode(full_script.as_bytes());
    format!("python3 -c \"import base64; exec(base64.b64decode('{b64}').decode('utf-8'))\"")
}
