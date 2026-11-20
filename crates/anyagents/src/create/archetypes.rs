//! Subagent defaults config, archetype markdown templates, role resolution, effective instructions, and role backfill.

use std::path::{Path, PathBuf};

use anyagents_core::{Agent, Error, Store, agent_config_dir};
use anyagents_harness::normalize_harness_name;

use super::{
    bound_profile_name, ensure_harness_runs_profile, poke_agent_wake_socket,
    sync_agent_description_files, upstream_provider_of, write_profile_marker, write_required_file,
};

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
    let mut cfg = std::fs::read_to_string(&path)
        .ok()
        .and_then(|text| serde_json::from_str::<SubagentDefaultsConfig>(&text).ok())
        .unwrap_or_default();
    normalize_subagent_harnesses(&mut cfg);
    cfg
}

fn normalize_subagent_harnesses(cfg: &mut SubagentDefaultsConfig) {
    for spec in std::iter::once(&mut cfg.default).chain(cfg.agents.values_mut()) {
        if let Some(harness) = spec.harness.as_mut() {
            *harness = normalize_harness_name(harness).to_string();
        }
    }
}

pub(crate) fn save_subagent_defaults(
    data: &Path,
    cfg: &SubagentDefaultsConfig,
) -> Result<(), Error> {
    let path = data.join("subagent-defaults.json");
    let mut cfg = cfg.clone();
    normalize_subagent_harnesses(&mut cfg);
    let text = serde_json::to_string_pretty(&cfg).map_err(|err| Error::Message(err.to_string()))?;
    write_required_file(&path, format!("{text}\n").as_bytes(), 0o644)
}

/// Default built-in role assigned when `role` is omitted or empty in `spawn` / `create`.
pub(crate) const DEFAULT_AGENT_ROLE: &str = "general";

/// Standing execution instructions for the built-in `general` default role.
pub(crate) const BUILTIN_GENERAL_ROLE_BODY: &str = "\
You are a cohesive end-to-end execution worker. Own the assigned outcome from first inspection through live verification rather than stopping at partial steps or delegating your core deliverable sideways.\n\n\
Work from primary evidence: read the relevant files, state, and runtime behavior before changing anything, implement the simplest complete solution, and verify the result against real execution before reporting back.\n\n\
Split work into sub-agents only along genuine causal or independent-scope boundaries when parallel execution or distinct specialization is warranted. Keep changes minimal, clean, and honest about every verified outcome and any remaining gaps.";

fn builtin_role_template(slug: &str) -> Result<Option<ArchetypeTemplate>, Error> {
    if slug == DEFAULT_AGENT_ROLE {
        let raw = format!(
            "---\nname: general\nrole: general\nscope: private\ndescription: Default general-purpose end-to-end execution worker\n---\n{BUILTIN_GENERAL_ROLE_BODY}\n"
        );
        let mut tpl = parse_archetype_markdown(&raw)?;
        if tpl
            .role
            .as_deref()
            .map(str::trim)
            .filter(|r| valid_template_stem(r))
            .is_none()
        {
            tpl.role = Some(DEFAULT_AGENT_ROLE.to_string());
        }
        return Ok(Some(tpl));
    }
    Ok(None)
}

/// Parsed archetype template loaded from `<archetype>.md` or `<archetype>/agent.md`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ArchetypeTemplate {
    pub description: Option<String>,
    pub scope: Option<String>,
    pub workdir: Option<String>,
    pub harness: Option<String>,
    pub model: Option<String>,
    pub role: Option<String>,
    pub profile: Option<String>,
    pub body: String,
}

/// Parse optional YAML frontmatter (`description`, `scope`, `workdir`, `harness`, `model`,
/// `role`, `profile`) and markdown body from an archetype `.md` file.
///
/// Templates are configuration, rather than best-effort prose.  In particular, once a
/// document starts with a frontmatter delimiter, an incomplete or malformed delimiter must
/// not turn the rest of the file into an instruction body by accident.
pub(crate) fn parse_archetype_markdown(raw: &str) -> Result<ArchetypeTemplate, Error> {
    let mut tpl = ArchetypeTemplate::default();
    let normalized = raw.replace("\r\n", "\n");
    let trimmed_start = normalized.trim_start_matches('\u{feff}');
    let body = if let Some(rest) = trimmed_start.strip_prefix("---\n") {
        let mut offset = 0;
        let mut closing = None;
        for line in rest.split_inclusive('\n') {
            if line.trim_end_matches('\n') == "---" {
                closing = Some((offset, offset + line.len()));
                break;
            }
            offset += line.len();
        }
        let (start, end) = closing.ok_or_else(|| {
            Error::Message("archetype template has unclosed YAML frontmatter".to_string())
        })?;
        let frontmatter = &rest[..start];
        let invalid_yaml = |err: serde_yaml_ng::Error| {
            // YAML errors can quote the input. Do not reflect private template
            // contents into a validation response.
            Error::Message(format!(
                "archetype template has malformed YAML frontmatter{}",
                err.location()
                    .map(|loc| format!(" at line {}", loc.line()))
                    .unwrap_or_default()
            ))
        };
        let fields: std::collections::BTreeMap<String, String> = if frontmatter
            .lines()
            .all(|line| line.trim().is_empty() || line.trim().starts_with('#'))
        {
            std::collections::BTreeMap::new()
        } else {
            // Value's mapping parser also rejects duplicate keys; a direct
            // BTreeMap deserializer would silently overwrite them.
            let mapping: serde_yaml_ng::Value =
                serde_yaml_ng::from_str(frontmatter).map_err(invalid_yaml)?;
            serde_yaml_ng::from_value(mapping).map_err(invalid_yaml)?
        };
        if fields.is_empty() && rest[end..].trim().is_empty() {
            return Err(Error::Message(
                "archetype template frontmatter requires fields or a markdown body".to_string(),
            ));
        }
        let mut frontmatter_stem: Option<String> = None;
        for (key, value) in fields {
            let value = value.trim();
            if key.trim().is_empty() || value.is_empty() {
                return Err(Error::Message(
                    "archetype template has an empty YAML key or value".to_string(),
                ));
            }
            match key.to_ascii_lowercase().as_str() {
                "description" | "summary" => tpl.description = Some(value.to_string()),
                "scope" => tpl.scope = Some(anyagents_core::parse_agent_scope(value)?.to_string()),
                "workdir" | "workspace" => tpl.workdir = Some(value.to_string()),
                "harness" => tpl.harness = Some(normalize_harness_name(value).to_string()),
                "model" => tpl.model = Some(value.to_string()),
                "role" => tpl.role = Some(value.to_string()),
                "profile" => tpl.profile = Some(value.to_string()),
                "name" | "archetype" => {
                    if frontmatter_stem.is_none() && valid_template_stem(value) {
                        frontmatter_stem = Some(value.to_string());
                    }
                }
                _ => {
                    return Err(Error::Message(format!(
                        "unknown archetype template frontmatter key {key:?}"
                    )));
                }
            }
        }
        if tpl.role.is_none() {
            tpl.role = frontmatter_stem;
        }
        &rest[end..]
    } else {
        trimmed_start
    };
    tpl.body = body.trim().to_string();
    Ok(tpl)
}

/// Resolve an archetype template by name across:
/// 1. `<explicit_workdir>/.agents/agents/<archetype>.md` (or `<archetype>/agent.md`)
/// 2. `<parent_workdir>/.agents/agents/<archetype>.md` (or `<archetype>/agent.md`)
/// 3. `<data_dir>/.agents/agents/<archetype>.md` (then `<data_dir>/agents-templates/`, `<data_dir>/archetypes/`)
/// 4. `<HOME>/.agents/agents/<archetype>.md` (and `owner_workdir()/.agents/agents/`)
fn is_archetype_workdir_keyword(path: &Path) -> bool {
    matches!(
        path.to_string_lossy().trim(),
        "" | "worktree" | "branch" | "private" | "inherit"
    )
}

fn push_unique_search_root(
    search_roots: &mut Vec<(PathBuf, PathBuf)>,
    base: PathBuf,
    rel_root: PathBuf,
) {
    if !search_roots
        .iter()
        .any(|(b, r)| b == &base && r == &rel_root)
    {
        search_roots.push((base, rel_root));
    }
}

pub(crate) fn ancestor_archetype_workdirs(
    store: Option<&Store>,
    data_dir: &Path,
    parent: Option<&Agent>,
) -> Result<Vec<PathBuf>, Error> {
    const MAX_ANCESTOR_DEPTH: usize = 64;
    let mut workdirs = Vec::new();
    let Some(first) = parent else {
        return Ok(workdirs);
    };
    let mut visited = std::collections::HashSet::new();
    visited.insert(first.id.clone());
    let push_agent_wd = |agent: &Agent, out: &mut Vec<PathBuf>| {
        if let Some(wd) = agent.workdir.as_deref() {
            let p = Path::new(wd);
            if !is_archetype_workdir_keyword(p) {
                out.push(p.to_path_buf());
                return;
            }
        }
        out.push(anyagents_core::agent_work_dir(data_dir, &agent.id));
    };
    push_agent_wd(first, &mut workdirs);
    let mut next_parent_id = first.parent_id.clone();
    while let Some(pid) = next_parent_id {
        if visited.len() >= MAX_ANCESTOR_DEPTH || !visited.insert(pid.clone()) {
            break;
        }
        let Some(st) = store else {
            break;
        };
        match st.agent(&pid) {
            Ok(anc) => {
                push_agent_wd(&anc, &mut workdirs);
                next_parent_id = anc.parent_id;
            }
            Err(Error::AgentNotFound(_)) => break,
            Err(err) => return Err(err),
        }
    }
    Ok(workdirs)
}

pub(crate) fn archetype_search_roots_with_ancestors(
    explicit_workdir: Option<&Path>,
    ancestor_workdirs: &[PathBuf],
    data_dir: &Path,
) -> Vec<(PathBuf, PathBuf)> {
    archetype_search_roots_for_owner(
        explicit_workdir,
        ancestor_workdirs,
        data_dir,
        configured_owner_archetype_workdir().as_deref(),
    )
}

fn configured_owner_archetype_workdir() -> Option<PathBuf> {
    #[cfg(not(test))]
    {
        crate::server::owner_workdir()
    }
    #[cfg(test)]
    {
        None
    }
}

pub(super) fn archetype_search_roots_for_owner(
    explicit_workdir: Option<&Path>,
    ancestor_workdirs: &[PathBuf],
    data_dir: &Path,
    owner_workdir: Option<&Path>,
) -> Vec<(PathBuf, PathBuf)> {
    // Configuration is authoritative even when its catalog is missing or empty.
    // Do not resurrect removed templates from a workspace, data directory or HOME.
    if let Some(owner) = owner_workdir {
        return vec![(owner.to_path_buf(), PathBuf::from(".agents/agents"))];
    }
    let mut search_roots: Vec<(PathBuf, PathBuf)> = Vec::new();
    if let Some(wd) = explicit_workdir
        && !is_archetype_workdir_keyword(wd)
    {
        push_unique_search_root(
            &mut search_roots,
            wd.to_path_buf(),
            PathBuf::from(".agents/agents"),
        );
    }
    for pwd in ancestor_workdirs {
        if !is_archetype_workdir_keyword(pwd) {
            push_unique_search_root(
                &mut search_roots,
                pwd.clone(),
                PathBuf::from(".agents/agents"),
            );
        }
    }
    let has_workspace_agents_dir = search_roots
        .iter()
        .any(|(base, rel)| base.join(rel).is_dir());
    if !has_workspace_agents_dir {
        push_unique_search_root(
            &mut search_roots,
            data_dir.to_path_buf(),
            PathBuf::from(".agents/agents"),
        );
        // The data directory's `AGENTS.md` may link into the shared rules
        // repository; that repository's catalog sits beside the file it names.
        if let Some(rules_root) = std::fs::canonicalize(data_dir.join("AGENTS.md"))
            .ok()
            .and_then(|path| path.parent().map(Path::to_path_buf))
        {
            push_unique_search_root(
                &mut search_roots,
                rules_root,
                PathBuf::from(".agents/agents"),
            );
        }
    }
    push_unique_search_root(
        &mut search_roots,
        data_dir.to_path_buf(),
        PathBuf::from("agents-templates"),
    );
    push_unique_search_root(
        &mut search_roots,
        data_dir.to_path_buf(),
        PathBuf::from("archetypes"),
    );
    #[cfg(not(test))]
    if !has_workspace_agents_dir && let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        push_unique_search_root(&mut search_roots, home, PathBuf::from(".agents/agents"));
    }
    search_roots
}

#[cfg(test)]
pub(crate) fn archetype_search_roots(
    explicit_workdir: Option<&Path>,
    parent_workdir: Option<&Path>,
    data_dir: &Path,
) -> Vec<(PathBuf, PathBuf)> {
    let ancestors: Vec<PathBuf> = parent_workdir.map(Path::to_path_buf).into_iter().collect();
    archetype_search_roots_with_ancestors(explicit_workdir, &ancestors, data_dir)
}

fn discover_disk_archetype_slugs_with_ancestors(
    explicit_workdir: Option<&Path>,
    ancestor_workdirs: &[PathBuf],
    data_dir: &Path,
) -> std::collections::BTreeSet<String> {
    let mut slugs = std::collections::BTreeSet::new();
    for (base, rel_root) in
        archetype_search_roots_with_ancestors(explicit_workdir, ancestor_workdirs, data_dir)
    {
        let dir = base.join(rel_root);
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(ft) = entry.file_type() else {
                continue;
            };
            if ft.is_symlink() {
                continue;
            }
            let path = entry.path();
            if ft.is_file() {
                if path.extension().and_then(|e| e.to_str()) == Some("md")
                    && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                    && valid_template_stem(stem)
                {
                    slugs.insert(stem.to_string());
                }
            } else if ft.is_dir()
                && let Some(stem) = path.file_name().and_then(|s| s.to_str())
                && valid_template_stem(stem)
                && (path.join("agent.md").is_file() || path.join("AGENT.md").is_file())
            {
                slugs.insert(stem.to_string());
            }
        }
    }
    slugs
}

pub(crate) fn discover_available_archetype_slugs_with_ancestors(
    explicit_workdir: Option<&Path>,
    ancestor_workdirs: &[PathBuf],
    data_dir: &Path,
) -> Vec<String> {
    let mut disk =
        discover_disk_archetype_slugs_with_ancestors(explicit_workdir, ancestor_workdirs, data_dir);
    disk.remove(DEFAULT_AGENT_ROLE);
    let mut slugs = Vec::with_capacity(disk.len() + 1);
    slugs.push(DEFAULT_AGENT_ROLE.to_string());
    slugs.extend(disk);
    slugs
}

pub(crate) fn discover_available_archetype_slugs(
    explicit_workdir: Option<&Path>,
    parent_workdir: Option<&Path>,
    data_dir: &Path,
) -> Vec<String> {
    let ancestors: Vec<PathBuf> = parent_workdir.map(Path::to_path_buf).into_iter().collect();
    discover_available_archetype_slugs_with_ancestors(explicit_workdir, &ancestors, data_dir)
}

pub(crate) fn has_configured_archetype_roots_with_ancestors(
    explicit_workdir: Option<&Path>,
    ancestor_workdirs: &[PathBuf],
    data_dir: &Path,
) -> bool {
    configured_owner_archetype_workdir().is_some()
        || archetype_search_roots_with_ancestors(explicit_workdir, ancestor_workdirs, data_dir)
            .into_iter()
            .any(|(base, rel_root)| base.join(rel_root).exists())
}

pub(crate) fn discover_available_archetype_templates(
    explicit_workdir: Option<&Path>,
    parent_workdir: Option<&Path>,
    data_dir: &Path,
) -> Vec<(String, Option<String>)> {
    let slugs = discover_available_archetype_slugs(explicit_workdir, parent_workdir, data_dir);
    let mut out = Vec::with_capacity(slugs.len());
    for slug in slugs {
        let desc = resolve_archetype_template(&slug, explicit_workdir, parent_workdir, data_dir)
            .ok()
            .flatten()
            .and_then(|tpl| tpl.description)
            .map(|d| d.trim().to_string())
            .filter(|d| !d.is_empty());
        out.push((slug, desc));
    }
    out
}

pub(crate) fn resolve_archetype_template(
    archetype: &str,
    explicit_workdir: Option<&Path>,
    parent_workdir: Option<&Path>,
    data_dir: &Path,
) -> Result<Option<ArchetypeTemplate>, Error> {
    let ancestors: Vec<PathBuf> = parent_workdir.map(Path::to_path_buf).into_iter().collect();
    resolve_archetype_template_with_ancestors(archetype, explicit_workdir, &ancestors, data_dir)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ArchetypeSource {
    Disk,
    Builtin,
}

pub(crate) fn resolve_archetype_template_with_ancestors(
    archetype: &str,
    explicit_workdir: Option<&Path>,
    ancestor_workdirs: &[PathBuf],
    data_dir: &Path,
) -> Result<Option<ArchetypeTemplate>, Error> {
    Ok(resolve_archetype_template_with_source(
        archetype,
        explicit_workdir,
        ancestor_workdirs,
        data_dir,
    )?
    .map(|(tpl, _)| tpl))
}

fn resolve_archetype_template_with_source(
    archetype: &str,
    explicit_workdir: Option<&Path>,
    ancestor_workdirs: &[PathBuf],
    data_dir: &Path,
) -> Result<Option<(ArchetypeTemplate, ArchetypeSource)>, Error> {
    let slug = archetype.trim();
    if !valid_template_stem(slug) {
        return Err(Error::Message(format!(
            "invalid archetype template name {archetype:?}; expected a single filename stem"
        )));
    }

    let search_roots =
        archetype_search_roots_with_ancestors(explicit_workdir, ancestor_workdirs, data_dir);
    let candidates = [
        format!("{slug}.md"),
        format!("{}/agent.md", slug),
        format!("{}/AGENT.md", slug),
    ];

    for (base, root) in search_roots {
        for rel in &candidates {
            if let Some(raw) = read_archetype_file(&base, &root.join(rel))? {
                let mut tpl = parse_archetype_markdown(&raw)?;
                if tpl
                    .role
                    .as_deref()
                    .map(str::trim)
                    .filter(|r| valid_template_stem(r))
                    .is_none()
                {
                    tpl.role = Some(slug.to_string());
                }
                return Ok(Some((tpl, ArchetypeSource::Disk)));
            }
        }
    }
    Ok(builtin_role_template(slug)?.map(|tpl| (tpl, ArchetypeSource::Builtin)))
}

/// Read a template under a trusted search root `base` while refusing any symlink
/// or traversal inside `relative`.
///
/// Trusted `base` roots themselves may be symlinks or sit beneath symlinked mount
/// points (e.g. `/home/user/workspace -> /workspace-data/workspace` or macOS
/// `/var -> /private/var`), so `base` is canonicalized first; every component of
/// `relative` beneath `canonical_base` is then verified symlink-free and opened
/// with `O_NOFOLLOW` descriptor pinning.
pub(super) fn read_archetype_file(base: &Path, relative: &Path) -> Result<Option<String>, Error> {
    use std::path::Component;

    let mut normal_parts = Vec::new();
    for part in relative.components() {
        match part {
            Component::Normal(seg) => normal_parts.push(seg),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(Error::Message(format!(
                    "archetype template relative path escapes base: {}",
                    relative.display()
                )));
            }
        }
    }
    if normal_parts.is_empty() {
        return Err(Error::Message(
            "archetype template relative path is empty".to_string(),
        ));
    }

    let canonical_base = match std::fs::canonicalize(base) {
        Ok(resolved) => resolved,
        Err(err)
            if matches!(
                err.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) =>
        {
            return Ok(None);
        }
        Err(err) => {
            return Err(Error::Message(format!(
                "cannot resolve archetype base {}: {err}",
                base.display()
            )));
        }
    };
    if !canonical_base.is_dir() {
        return Ok(None);
    }

    let inspect_relative_component = |component: &Path| -> Result<bool, Error> {
        let metadata = match std::fs::symlink_metadata(component) {
            Ok(metadata) => metadata,
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                ) =>
            {
                return Ok(false);
            }
            Err(err) => {
                return Err(Error::Message(format!(
                    "cannot inspect archetype template path {}: {err}",
                    component.display()
                )));
            }
        };
        if metadata.file_type().is_symlink() {
            return Err(Error::Message(format!(
                "archetype template path contains a symlink: {}",
                component.display()
            )));
        }
        Ok(true)
    };

    let mut component = canonical_base.clone();
    for seg in &normal_parts {
        component.push(seg);
        if !inspect_relative_component(&component)? {
            return Ok(None);
        }
    }
    let path = component.as_path();

    let template_dir = path.parent().ok_or_else(|| {
        Error::Message(format!(
            "archetype template has no containing directory: {}",
            path.display()
        ))
    })?;
    let canonical_dir = std::fs::canonicalize(template_dir).map_err(|err| {
        Error::Message(format!(
            "cannot resolve archetype template directory {}: {err}",
            template_dir.display()
        ))
    })?;
    let canonical_file = std::fs::canonicalize(path).map_err(|err| {
        Error::Message(format!(
            "cannot resolve archetype template {}: {err}",
            path.display()
        ))
    })?;
    if !canonical_file.starts_with(&canonical_base)
        || canonical_file.parent() != Some(canonical_dir.as_path())
    {
        return Err(Error::Message(format!(
            "archetype template escapes its authorized directory: {}",
            path.display()
        )));
    }

    let file = open_archetype_nofollow(path).map_err(|err| {
        Error::Message(format!(
            "cannot open archetype template {} without following symlinks: {err}",
            path.display()
        ))
    })?;
    if !file.metadata()?.is_file() {
        return Err(Error::Message(format!(
            "archetype template is not a regular file: {}",
            path.display()
        )));
    }
    let mut raw = String::new();
    use std::io::Read;
    std::io::BufReader::new(file)
        .read_to_string(&mut raw)
        .map_err(|err| {
            Error::Message(format!(
                "cannot read archetype template {}: {err}",
                path.display()
            ))
        })?;
    Ok(Some(raw))
}

// Pin each parent directory by descriptor. O_NOFOLLOW on the final file alone
// would still allow an intermediate directory to be swapped for a symlink after
// symlink_metadata has checked it.
#[cfg(unix)]
pub(super) fn open_archetype_nofollow(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    use std::path::Component;

    let mut directory = std::fs::File::open(if path.is_absolute() { "/" } else { "." })?;
    let mut parts = path
        .components()
        .filter(|part| !matches!(part, Component::RootDir))
        .peekable();
    while let Some(part) = parts.next() {
        let name = std::ffi::CString::new(part.as_os_str().as_bytes())?;
        let mut flags = libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK;
        if parts.peek().is_some() {
            flags |= libc::O_DIRECTORY;
        }
        // SAFETY: name is a NUL-terminated path component and the borrowed
        // directory descriptor is live. A successful descriptor is owned once.
        let fd = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: openat returned a new descriptor and ownership is transferred.
        let file = unsafe { std::fs::File::from_raw_fd(fd) };
        if parts.peek().is_none() {
            return Ok(file);
        }
        directory = file;
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "empty template path",
    ))
}

#[cfg(not(unix))]
pub(super) fn open_archetype_nofollow(path: &Path) -> std::io::Result<std::fs::File> {
    std::fs::File::open(path)
}

/// Apply archetype template defaults (`.agents/agents/<archetype>.md`) to a `CreateAgentRequest`
/// in-place before agent creation. Unifies `role` and `archetype`: resolving a template sets
/// `body.role` to the canonical template stem `<slug>`, and when templates are configured in
/// `.agents/agents/`, unknown `role` names are rejected before persistence.
pub(crate) const INSTRUCTIONS_ROTATED_MARKER: &str = ".anyagents-instructions-rotated-ms";

pub(crate) fn mark_instructions_rotated_if_changed(
    store: &Store,
    config: &Path,
    agent: &Agent,
    before_prompt: Option<&[u8]>,
    min_started_ms: Option<i64>,
) -> Result<(), Error> {
    if agent.state != "running" || !crate::vendor::is_spiron_harness(&agent.harness) {
        return Ok(());
    }
    let after_prompt = std::fs::read(config.join("SYSTEM_PROMPT.md")).ok();
    if before_prompt == after_prompt.as_deref() {
        return Ok(());
    }
    let marker_path = config.join(INSTRUCTIONS_ROTATED_MARKER);
    let prev = std::fs::read_to_string(&marker_path)
        .ok()
        .and_then(|raw| raw.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let mut ts = anyagents_core::now_ms().max(prev.saturating_add(1));
    if let Some(started) = min_started_ms {
        ts = ts.max(started.saturating_add(1));
    }
    write_profile_marker(config, INSTRUCTIONS_ROTATED_MARKER, &format!("{ts}\n"))?;
    store.poke(anyagents_core::Wake::Agent {
        agent_id: agent.id.clone(),
    });
    poke_agent_wake_socket(store.data_dir(), &agent.id);
    Ok(())
}

pub(super) fn resolve_role_template_with_ancestors(
    raw_role: &str,
    agent_workdir: Option<&Path>,
    ancestor_wds: &[PathBuf],
    data_dir: &Path,
) -> Result<Option<(String, ArchetypeTemplate, ArchetypeSource)>, Error> {
    let role = raw_role.trim();
    if role.is_empty() {
        return Ok(None);
    }
    let slugified = role.to_ascii_lowercase().replace(' ', "-");
    if !valid_template_stem(role) && !valid_template_stem(&slugified) {
        resolve_archetype_template_with_source(role, agent_workdir, ancestor_wds, data_dir)?;
        return Ok(None);
    }
    if valid_template_stem(role)
        && let Some((tpl, src)) =
            resolve_archetype_template_with_source(role, agent_workdir, ancestor_wds, data_dir)?
    {
        return Ok(Some((role.to_string(), tpl, src)));
    }
    if slugified != role
        && valid_template_stem(&slugified)
        && let Some((tpl, src)) = resolve_archetype_template_with_source(
            &slugified,
            agent_workdir,
            ancestor_wds,
            data_dir,
        )?
    {
        return Ok(Some((slugified, tpl, src)));
    }
    Ok(None)
}

fn canonical_template_role(slug: &str, tpl: &ArchetypeTemplate) -> String {
    tpl.role
        .as_deref()
        .map(str::trim)
        .filter(|r| valid_template_stem(r))
        .unwrap_or(slug)
        .to_string()
}

pub(crate) fn resolve_role_template_for_agent(
    store: Option<&Store>,
    data_dir: &Path,
    agent: &Agent,
) -> Option<(String, ArchetypeTemplate)> {
    let raw_role = agent
        .role
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())?;
    let parent = store.and_then(|s| agent.parent_id.as_ref().and_then(|pid| s.agent(pid).ok()));
    let ancestor_wds =
        ancestor_archetype_workdirs(store, data_dir, parent.as_ref()).unwrap_or_default();
    let agent_wd = agent.workdir.as_deref().map(Path::new);
    resolve_role_template_with_ancestors(raw_role, agent_wd, &ancestor_wds, data_dir)
        .ok()
        .flatten()
        .map(|(slug, tpl, _)| (canonical_template_role(&slug, &tpl), tpl))
}

fn strip_role_prefix_match(instructions: &str, prefix: &str) -> Option<Option<String>> {
    let inst = instructions.trim();
    let pref = prefix.trim();
    if inst.is_empty() || pref.is_empty() {
        return None;
    }
    if inst == pref {
        return Some(None);
    }
    if let Some(rest) = inst.strip_prefix(pref)
        && rest.starts_with(['\n', '\r'])
    {
        let cleaned = rest.trim();
        return Some((!cleaned.is_empty()).then(|| cleaned.to_string()));
    }
    None
}

fn strip_known_baked_role_prefixes<'a>(
    raw_instructions: Option<&str>,
    candidate_bodies: impl IntoIterator<Item = &'a str>,
) -> Option<String> {
    let trimmed = raw_instructions.map(str::trim).filter(|s| !s.is_empty())?;
    for body in candidate_bodies {
        if let Some(stripped) = strip_role_prefix_match(trimmed, body) {
            return stripped;
        }
    }
    if let Some(stripped) = strip_role_prefix_match(trimmed, BUILTIN_GENERAL_ROLE_BODY) {
        return stripped;
    }
    Some(trimmed.to_string())
}

pub(crate) fn effective_agent_instructions(
    store: Option<&Store>,
    data_dir: &Path,
    agent: &Agent,
) -> Option<String> {
    let resolved = resolve_role_template_for_agent(store, data_dir, agent);
    let role_body = resolved
        .as_ref()
        .map(|(_, tpl)| tpl.body.trim())
        .filter(|s| !s.is_empty());
    let mut custom = agent.instructions.clone();
    loop {
        let next = strip_known_baked_role_prefixes(custom.as_deref(), role_body);
        if next == custom {
            break;
        }
        custom = next;
    }
    // Execution discipline is a runtime baseline, not a role-name policy.
    // Role templates stay caller-owned and are reloaded from their source.
    let mut sections = vec![BUILTIN_GENERAL_ROLE_BODY.to_string()];
    for section in [role_body, custom.as_deref()].into_iter().flatten() {
        let section = strip_role_prefix_match(section, BUILTIN_GENERAL_ROLE_BODY)
            .unwrap_or_else(|| Some(section.to_string()));
        if let Some(section) = section {
            sections.push(section);
        }
    }
    Some(sections.join("\n\n"))
}

pub(crate) fn backfill_historical_agent_roles(
    store: &Store,
    data_dir: &Path,
) -> Result<usize, Error> {
    let agents = store.list_agents()?;
    let mut updated_count = 0usize;
    for agent in agents {
        let parent = agent
            .parent_id
            .as_ref()
            .and_then(|pid| store.agent(pid).ok());
        let ancestor_wds =
            ancestor_archetype_workdirs(store_opt(store), data_dir, parent.as_ref())?;
        let agent_wd = agent.workdir.as_deref().map(Path::new);
        let raw_role = agent
            .role
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let mut candidate_bodies: Vec<String> = Vec::new();
        let target_role = match raw_role {
            None => DEFAULT_AGENT_ROLE.to_string(),
            Some(raw) => {
                let underscore_slug = raw.to_ascii_lowercase().replace([' ', '_'], "-");
                let matched =
                    resolve_role_template_with_ancestors(raw, agent_wd, &ancestor_wds, data_dir)?
                        .or_else(|| {
                            valid_template_stem(&underscore_slug)
                                .then(|| {
                                    resolve_role_template_with_ancestors(
                                        &underscore_slug,
                                        agent_wd,
                                        &ancestor_wds,
                                        data_dir,
                                    )
                                    .ok()
                                    .flatten()
                                })
                                .flatten()
                        });
                if let Some((slug, tpl, _)) = matched {
                    if !tpl.body.trim().is_empty() {
                        candidate_bodies.push(tpl.body.clone());
                    }
                    canonical_template_role(&slug, &tpl)
                } else {
                    let disk_slugs = discover_disk_archetype_slugs_with_ancestors(
                        agent_wd,
                        &ancestor_wds,
                        data_dir,
                    );
                    if !disk_slugs.is_empty()
                        || has_configured_archetype_roots_with_ancestors(
                            agent_wd,
                            &ancestor_wds,
                            data_dir,
                        )
                    {
                        DEFAULT_AGENT_ROLE.to_string()
                    } else {
                        raw.to_string()
                    }
                }
            }
        };
        if target_role != DEFAULT_AGENT_ROLE
            && let Some((_, tpl, _)) = resolve_role_template_with_ancestors(
                &target_role,
                agent_wd,
                &ancestor_wds,
                data_dir,
            )?
            && !tpl.body.trim().is_empty()
        {
            candidate_bodies.push(tpl.body);
        }

        let mut current = agent;
        let mut row_changed = false;
        if current.role.as_deref() != Some(target_role.as_str()) {
            current = store.set_agent_role(&current.id, Some(&target_role))?;
            row_changed = true;
        }
        let stripped = strip_known_baked_role_prefixes(
            current.instructions.as_deref(),
            candidate_bodies.iter().map(String::as_str),
        );
        if stripped != current.instructions {
            current = store.set_agent_instructions(&current.id, stripped.as_deref())?;
            row_changed = true;
        }
        if row_changed {
            updated_count += 1;
            if current.state != "archived" {
                let cfg = agent_config_dir(data_dir, &current.id);
                if cfg.is_dir() {
                    sync_agent_description_files(store, &current, &cfg)?;
                }
            }
        }
    }
    Ok(updated_count)
}

#[inline]
fn store_opt(store: &Store) -> Option<&Store> {
    Some(store)
}

pub(crate) fn apply_archetype_defaults(
    store: Option<&Store>,
    data_dir: &Path,
    parent: Option<&Agent>,
    body: &mut crate::api::CreateAgentRequest,
) -> Result<(), Error> {
    let explicit_wd = body
        .workdir
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(PathBuf::from);
    let ancestor_wds = ancestor_archetype_workdirs(store, data_dir, parent)?;
    let role = body
        .role
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_AGENT_ROLE);
    let resolved = match resolve_role_template_with_ancestors(
        role,
        explicit_wd.as_deref(),
        &ancestor_wds,
        data_dir,
    )? {
        Some(matched) => Some(matched),
        None => {
            let disk_slugs = discover_disk_archetype_slugs_with_ancestors(
                explicit_wd.as_deref(),
                &ancestor_wds,
                data_dir,
            );
            if !disk_slugs.is_empty()
                || has_configured_archetype_roots_with_ancestors(
                    explicit_wd.as_deref(),
                    &ancestor_wds,
                    data_dir,
                )
            {
                let available = discover_available_archetype_slugs_with_ancestors(
                    explicit_wd.as_deref(),
                    &ancestor_wds,
                    data_dir,
                );
                let suffix = if available.is_empty() {
                    String::new()
                } else {
                    format!(" (available: {})", available.join(", "))
                };
                return Err(Error::Message(format!(
                    "unknown archetype template {role:?}{suffix}"
                )));
            }
            None
        }
    };
    let Some((resolved_slug, tpl, source)) = resolved else {
        return Ok(());
    };

    let caller_description = body
        .description
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string);
    if caller_description.is_none()
        && let Some(desc) = tpl.description
    {
        body.description = Some(desc);
    }
    // Disk templates apply their declared `scope` unconditionally; built-in
    // `general` defaults sub-agents (`parent.is_some()`) to `private` while
    // letting root agents keep their default `public` scope unless overridden.
    if body
        .scope
        .as_deref()
        .map(str::trim)
        .is_none_or(str::is_empty)
        && body.public.is_none()
        && (source == ArchetypeSource::Disk || parent.is_some())
        && let Some(scope) = tpl.scope
    {
        body.scope = Some(scope);
    }
    if body
        .workdir
        .as_deref()
        .map(str::trim)
        .is_none_or(str::is_empty)
        && body
            .repo
            .as_deref()
            .map(str::trim)
            .is_none_or(str::is_empty)
        && let Some(wd) = tpl.workdir
    {
        body.workdir = Some(wd);
    }
    let caller_harness = body
        .harness
        .as_deref()
        .map(str::trim)
        .filter(|h| !h.is_empty())
        .map(|h| {
            if h.eq_ignore_ascii_case("inherit") {
                resolve_subagent_defaults(data_dir, parent, "inherit", None, None).0
            } else {
                normalize_harness_name(h).to_string()
            }
        });
    let tpl_harness = tpl
        .harness
        .as_deref()
        .map(str::trim)
        .filter(|h| !h.is_empty())
        .map(|h| normalize_harness_name(h).to_string());
    let harness_overridden = caller_harness
        .as_deref()
        .is_some_and(|ch| tpl_harness.as_deref().is_some_and(|th| ch != th));
    if caller_harness.is_none()
        && let Some(h) = tpl.harness
    {
        body.harness = Some(h);
    }
    if body
        .model
        .as_deref()
        .map(str::trim)
        .is_none_or(str::is_empty)
        && !harness_overridden
        && let Some(m) = tpl.model
    {
        body.model = Some(m);
    }
    if body
        .profile
        .as_deref()
        .map(str::trim)
        .is_none_or(str::is_empty)
        && !harness_overridden
        && let Some(p) = tpl.profile
    {
        let (effective_harness, effective_model, _) = resolve_subagent_defaults(
            data_dir,
            parent,
            body.harness.as_deref().unwrap_or("claude-code"),
            body.model.clone(),
            None,
        );
        let expected_provider =
            upstream_provider_of(&effective_harness, effective_model.as_deref());
        let tpl_provider = p
            .split_once('/')
            .map(|(prov, _)| prov.trim())
            .unwrap_or(expected_provider.as_str());
        if tpl_provider == expected_provider
            && ensure_harness_runs_profile(
                data_dir,
                &effective_harness,
                Some(&p),
                effective_model.as_deref(),
            )
            .is_ok()
        {
            body.profile = Some(p);
        }
    }
    let canonical_role = tpl
        .role
        .as_deref()
        .map(str::trim)
        .filter(|r| valid_template_stem(r))
        .unwrap_or(&resolved_slug)
        .to_string();
    body.role = Some(canonical_role);
    body.instructions = body
        .instructions
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .or(caller_description);
    Ok(())
}

pub(crate) fn normalize_or_validate_agent_role(
    raw_role: &str,
    agent_workdir: Option<&Path>,
    parent_workdir: Option<&Path>,
    data_dir: &Path,
) -> Result<String, Error> {
    let role = raw_role.trim();
    let parent_ancestors: Vec<PathBuf> =
        parent_workdir.map(Path::to_path_buf).into_iter().collect();
    if let Some((slug, tpl, _)) =
        resolve_role_template_with_ancestors(role, agent_workdir, &parent_ancestors, data_dir)?
    {
        return Ok(canonical_template_role(&slug, &tpl));
    }
    let disk_slugs =
        discover_disk_archetype_slugs_with_ancestors(agent_workdir, &parent_ancestors, data_dir);
    if !disk_slugs.is_empty() || configured_owner_archetype_workdir().is_some() {
        let available = discover_available_archetype_slugs(agent_workdir, parent_workdir, data_dir);
        return Err(Error::Message(format!(
            "unknown role template {role:?} (available: {})",
            available.join(", ")
        )));
    }
    Ok(role.to_string())
}

fn valid_template_stem(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
}

/// Resolve `(harness, model, profile)` for a newly created agent or sub-agent.
pub(crate) fn resolve_subagent_defaults(
    data: &Path,
    parent: Option<&Agent>,
    raw_harness: &str,
    raw_model: Option<String>,
    raw_profile: Option<String>,
) -> (String, Option<String>, Option<String>) {
    let h_trim = normalize_harness_name(raw_harness);
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
            .map(str::to_string)
            .or_else(|| {
                (harness == "spiron" || harness == "spiron-local")
                    .then(|| crate::spiron::DEFAULT_SPIRON_MODEL.to_string())
            });
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
            "spiron".to_string()
        } else {
            normalize_harness_name(&parent_agent.harness).to_string()
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
    }
    .or_else(|| {
        (resolved_harness == "spiron" || resolved_harness == "spiron-local").then(|| {
            spec.model
                .as_deref()
                .map(str::trim)
                .filter(|m| m.starts_with("sylphx/"))
                .map(str::to_string)
                .unwrap_or_else(|| crate::spiron::DEFAULT_SPIRON_MODEL.to_string())
        })
    });

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
