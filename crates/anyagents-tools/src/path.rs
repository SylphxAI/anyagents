use std::path::{Component, Path, PathBuf};

/// Resolve a user-supplied relative or absolute path against a workspace root directory.
pub fn resolve_workspace_path(
    base: &Path,
    raw: Option<&str>,
    required: bool,
) -> Result<String, String> {
    let Some(raw_str) = raw else {
        if required {
            return Err("`path` is required for this `files` action".to_string());
        }
        return Ok(normalize_lexical_path(base).display().to_string());
    };
    if raw_str.contains('\0') {
        return Err("`path` must not contain null bytes".to_string());
    }
    let trimmed = raw_str.trim();
    if trimmed.is_empty() {
        if required {
            return Err("`path` must not be empty".to_string());
        }
        return Ok(normalize_lexical_path(base).display().to_string());
    }
    let candidate = Path::new(trimmed);
    let joined = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        base.join(candidate)
    };
    Ok(normalize_lexical_path(&joined).display().to_string())
}

pub fn normalize_lexical_path(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::Prefix(p) => out.push(p.as_os_str()),
            Component::RootDir => out.push(comp.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() && !out.is_absolute() {
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
