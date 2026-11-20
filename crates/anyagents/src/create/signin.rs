//! Staged sign-in directories, crash reaping, same-account profile deduplication, validation, and commit.

use std::path::{Path, PathBuf};

use anyagents_core::{Agent, Error, Store, agent_config_dir};

use super::credentials::{OAUTH_REFRESH_ATTEMPT_FILE, OAUTH_REFRESH_RESERVATION_FILE};
use super::profile_identity::{extract_profile_dedup_key, sanitize_profile_slug};
use super::profiles::{
    copy_seat_file_if_newer, ensure_provider_profile_meta, sync_and_list_provider_profiles_inner,
};
use super::quota::sync_anthropic_profile_metadata;
use super::{
    chmod_seat_file_readable, extract_profile_account_identity, list_harness_profiles,
    propagate_profile_files_to_bound_agents, update_profile_json_identity, upstream_provider_of,
    write_profile_marker, write_required_file,
};

const SIGNIN_STAGING_PREFIX: &str = ".signin-staging-";

const MERGED_PROFILE_PREFIX: &str = ".merged-";

/// A sign-in's staging directory, `profiles/<provider>/.signin-staging-<suffix>`.
/// An exclusive `flock` on the directory marks it in flight for as long as this
/// value lives. Successful commits remove it; failed sign-ins retain nonempty
/// staging for recovery rather than discarding potentially unique credentials.
pub(crate) struct SigninStaging {
    path: PathBuf,
    #[cfg(unix)]
    _lock: std::fs::File,
}

impl SigninStaging {
    /// Create a fresh staging directory, first removing the ones abandoned by
    /// sign-ins that no longer run.
    pub(crate) fn create(data: &Path, provider: &str, suffix: &str) -> Result<Self, Error> {
        let parent = data.join("profiles").join(provider);
        std::fs::create_dir_all(&parent)?;
        reap_abandoned_signin_staging(&parent);
        let path = parent.join(format!(
            "{SIGNIN_STAGING_PREFIX}{}-{suffix}",
            std::process::id()
        ));
        std::fs::create_dir(&path)?;
        #[cfg(unix)]
        let _lock = match lock_signin_staging(&path) {
            Ok(Some(lock)) => lock,
            Ok(None) => {
                return Err(Error::Message(format!(
                    "sign-in staging {} was taken by another process",
                    path.display()
                )));
            }
            Err(err) => {
                let _ = std::fs::remove_dir_all(&path);
                return Err(err.into());
            }
        };
        Ok(Self {
            path,
            #[cfg(unix)]
            _lock,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for SigninStaging {
    fn drop(&mut self) {
        match std::fs::remove_dir(&self.path) {
            Err(err)
                if !matches!(
                    err.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
                ) =>
            {
                eprintln!(
                    "sign-in staging {} was not removed: {err}",
                    self.path.display()
                )
            }
            _ => {}
        }
    }
}

/// Take the staging lock on `path` without waiting. `None` when a live sign-in
/// holds it, or when `path` no longer names the locked directory.
#[cfg(unix)]
fn lock_signin_staging(path: &Path) -> std::io::Result<Option<std::fs::File>> {
    use std::os::unix::fs::MetadataExt as _;
    use std::os::unix::io::AsRawFd as _;
    let dir = std::fs::File::open(path)?;
    // SAFETY: `dir` is an open descriptor for the duration of the call.
    if unsafe { libc::flock(dir.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        let err = std::io::Error::last_os_error();
        return if err.kind() == std::io::ErrorKind::WouldBlock {
            Ok(None)
        } else {
            Err(err)
        };
    }
    let (held, named) = (dir.metadata()?, std::fs::symlink_metadata(path)?);
    Ok((named.is_dir() && held.dev() == named.dev() && held.ino() == named.ino()).then_some(dir))
}

/// Legacy interactive staging names also carry a PID. Unknown formats and any
/// process whose absence cannot be proved are retained, including older agent
/// sign-ins that did not take a directory lock. A live PID also closes the
/// creation-before-flock window for new staging directories.
pub(super) fn signin_staging_owner_exited(name: &str) -> bool {
    #[cfg(unix)]
    {
        let Some(pid) = name
            .strip_prefix(SIGNIN_STAGING_PREFIX)
            .and_then(|suffix| suffix.split('-').next())
            .and_then(|pid| pid.parse::<libc::pid_t>().ok())
            .filter(|pid| *pid > 0)
        else {
            return false;
        };
        // SAFETY: signal zero probes existence without sending a signal.
        (unsafe { libc::kill(pid, 0) }) == -1
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
    }
    #[cfg(not(unix))]
    {
        let _ = name;
        false
    }
}

/// Reap only empty, provably abandoned staging. Nonempty copies are considered
/// below, where deletion also requires verified recovery in a live profile.
fn reap_abandoned_signin_staging(provider_dir: &Path) {
    #[cfg(unix)]
    if let Ok(entries) = std::fs::read_dir(provider_dir) {
        for entry in entries.flatten() {
            if !signin_staging_owner_exited(&entry.file_name().to_string_lossy()) {
                continue;
            }
            let path = entry.path();
            if let Ok(Some(_held)) = lock_signin_staging(&path) {
                let _ = std::fs::remove_dir(&path);
            }
        }
    }
    #[cfg(not(unix))]
    let _ = provider_dir;
}

/// The identities a profile directory holds, keyed as the deduplication keys them.
fn profile_identity_keys(dir: &Path, provider: &str) -> Vec<String> {
    let (email, account_id) = extract_profile_account_identity(dir, provider);
    email
        .map(|e| format!("email:{}", e.to_ascii_lowercase()))
        .into_iter()
        .chain(account_id.map(|id| format!("id:{id}")))
        .chain(extract_profile_dedup_key(dir, provider))
        .collect()
}

/// Remove `.merged-*` recovery copies that hold nothing a live profile lacks: a
/// live profile with credentials holds the same account and has every declared
/// sign-in file the copy has, byte for byte. Unknown files, symlinks, unreadable
/// contents and differing credentials are retained. Abandoned staging is subject
/// to the same recovery check in addition to its PID and lock checks.
fn reap_superseded_merged_profiles(provider_dir: &Path, provider: &str) {
    let Ok((primary, companion)) = staged_signin_manifest(provider) else {
        return;
    };
    let Ok(entries) = std::fs::read_dir(provider_dir) else {
        return;
    };
    let is_file = |path: &Path| std::fs::symlink_metadata(path).is_ok_and(|m| m.is_file());
    let mut live = Vec::new();
    let mut copies: Vec<(PathBuf, Option<std::fs::File>)> = Vec::new();
    for entry in entries.flatten() {
        if !entry.file_type().is_ok_and(|t| t.is_dir()) {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(MERGED_PROFILE_PREFIX) {
            copies.push((entry.path(), None));
        } else if signin_staging_owner_exited(&name) {
            #[cfg(unix)]
            if let Ok(Some(lock)) = lock_signin_staging(&entry.path()) {
                copies.push((entry.path(), Some(lock)));
            }
        } else if !name.starts_with('.')
            && primary.iter().any(|rel| is_file(&entry.path().join(rel)))
        {
            live.push((profile_identity_keys(&entry.path(), provider), entry.path()));
        }
    }
    for (copy, _held) in copies {
        let keys = profile_identity_keys(&copy, provider);
        let Some((_, holder)) = live
            .iter()
            .find(|(held, _)| held.iter().any(|k| keys.contains(k)))
        else {
            continue;
        };
        let mut superseded = true;
        let mut pending = vec![copy.clone()];
        while superseded && let Some(dir) = pending.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                superseded = false;
                break;
            };
            for entry in entries {
                let Ok(entry) = entry else {
                    superseded = false;
                    break;
                };
                let path = entry.path();
                let Ok(rel) = path.strip_prefix(&copy) else {
                    superseded = false;
                    break;
                };
                let rel_str = rel.to_string_lossy();
                superseded = match entry.file_type() {
                    Ok(t) if t.is_dir() => {
                        pending.push(path.clone());
                        true
                    }
                    Ok(t) if t.is_symlink() => false,
                    Ok(_)
                        if primary.contains(&rel_str.as_ref())
                            || companion.contains(&rel_str.as_ref()) =>
                    {
                        is_file(&holder.join(rel))
                            && std::fs::read(&path)
                                .ok()
                                .zip(std::fs::read(holder.join(rel)).ok())
                                .is_some_and(|(source, live)| source == live)
                    }
                    Ok(t) if t.is_file() => {
                        matches!(
                            rel_str.as_ref(),
                            "profile.json" | ".anyagents-last-usage.txt"
                        )
                    }
                    Ok(_) => false,
                    Err(_) => false,
                };
                if !superseded {
                    break;
                }
            }
        }
        if superseded && let Err(err) = std::fs::remove_dir_all(&copy) {
            eprintln!(
                "merged profile copy {} was not removed: {err}",
                copy.display()
            );
        }
    }
}

/// Enforces the `1 Email / Account = 1 Profile` invariant across all providers under
/// `<data>/profiles/<provider>`. If two or more profiles under the same provider share
/// the exact same email, account ID, or API key, merges them into a single canonical
/// profile (preferring `default` when present), updates any bound agents' `.anyagents-profile`
/// and `.anyagents-seeded-profile` markers, and removes the duplicate profile directory.
pub(crate) fn deduplicate_same_account_profiles(data: &Path) {
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
        if provider.starts_with('.') {
            continue;
        }
        let Ok(prof_dirs) = std::fs::read_dir(&prov_path) else {
            continue;
        };
        let mut profs: Vec<(String, PathBuf)> = prof_dirs
            .flatten()
            .filter_map(|e| {
                let p = e.path();
                if !p.is_dir() {
                    return None;
                }
                let name = e.file_name().to_string_lossy().into_owned();
                if name.starts_with('.') {
                    return None;
                }
                Some((name, p))
            })
            .collect();
        if profs.len() <= 1 {
            continue;
        }
        profs.sort_by(|a, b| {
            (a.0 != "default")
                .cmp(&(b.0 != "default"))
                .then_with(|| a.0.cmp(&b.0))
        });

        let mut canonical_by_key: std::collections::BTreeMap<String, (String, PathBuf)> =
            std::collections::BTreeMap::new();
        let mut merged_into: std::collections::BTreeMap<String, String> =
            std::collections::BTreeMap::new();

        for (prof_name, prof_path) in profs {
            let (em, aid) = extract_profile_account_identity(&prof_path, &provider);
            let dedup_key = extract_profile_dedup_key(&prof_path, &provider);
            let mut matched_canon: Option<(String, PathBuf)> = None;
            if let Some(ref e) = em {
                let k = format!("email:{}", e.to_ascii_lowercase());
                if let Some(c) = canonical_by_key.get(&k) {
                    matched_canon = Some(c.clone());
                }
            }
            if matched_canon.is_none()
                && let Some(ref id) = aid
            {
                let k = format!("id:{id}");
                if let Some(c) = canonical_by_key.get(&k) {
                    matched_canon = Some(c.clone());
                }
            }
            if matched_canon.is_none()
                && let Some(ref dk) = dedup_key
                && let Some(c) = canonical_by_key.get(dk)
            {
                matched_canon = Some(c.clone());
            }

            if let Some((canon_name, canon_path)) = matched_canon {
                let rels: &[&str] = match provider.as_str() {
                    "anthropic" => &[".credentials.json", ".claude/.credentials.json"],
                    "openai" => &["auth.json", ".codex/auth.json"],
                    "google-antigravity" => &[
                        ".gemini/antigravity-cli/antigravity-oauth-token",
                        ".gemini/antigravity-cli/antigravity_state.pbtxt",
                        ".gemini/antigravity-cli/jetski_state.pbtxt",
                        ".gemini/config/config.json",
                    ],
                    "muse" => &["muse/auth.json", ".config/muse/auth.json"],
                    "xai" => &[".grok/auth.json"],
                    _ => &[],
                };
                let mut complete = !rels.is_empty();
                for rel in rels {
                    let src = prof_path.join(rel);
                    if src.is_file() {
                        let dst = canon_path.join(rel);
                        let copied = copy_seat_file_if_newer(&src, &dst);
                        complete &= copied
                            || std::fs::read(&src)
                                .ok()
                                .zip(std::fs::read(&dst).ok())
                                .is_some_and(|(a, b)| a == b);
                    }
                }
                if !complete {
                    continue;
                }
                // Retain every source byte in an excluded recovery directory before rebinding.
                let recovery = prof_path
                    .with_file_name(format!(".merged-{prof_name}-{}", anyagents_core::now_ms()));
                if recovery.exists() || std::fs::rename(&prof_path, &recovery).is_err() {
                    continue;
                }
                merged_into.insert(prof_name, canon_name);
            } else {
                if let Some(e) = em {
                    canonical_by_key.insert(
                        format!("email:{}", e.to_ascii_lowercase()),
                        (prof_name.clone(), prof_path.clone()),
                    );
                }
                if let Some(id) = aid {
                    canonical_by_key
                        .insert(format!("id:{id}"), (prof_name.clone(), prof_path.clone()));
                }
                if let Some(dk) = dedup_key {
                    canonical_by_key.insert(dk, (prof_name, prof_path));
                }
            }
        }

        if !merged_into.is_empty()
            && let Ok(agents_dir) = std::fs::read_dir(data.join("agents"))
        {
            for entry in agents_dir.flatten() {
                let cfg = entry.path().join("config");
                if !cfg.is_dir() {
                    continue;
                }
                for marker in [".anyagents-profile", ".anyagents-seeded-profile"] {
                    if let Ok(raw) = std::fs::read_to_string(cfg.join(marker)) {
                        let trimmed = raw.trim();
                        let (m_prov, m_prof) = trimmed
                            .split_once('/')
                            .unwrap_or((provider.as_str(), trimmed));
                        if m_prov == provider
                            && let Some(canon_prof) = merged_into.get(m_prof)
                        {
                            write_profile_marker(
                                &cfg,
                                marker,
                                &format!("{provider}/{canon_prof}\n"),
                            )
                            .unwrap_or_else(|err| eprintln!("runtime persistence failed: {err}"));
                        }
                    }
                }
            }
        }
    }
    reap_signin_leftovers(&profiles_root);
}

/// Remove what finished sign-ins and merges leave beside the profiles of every
/// provider: abandoned staging directories and superseded `.merged-*` copies.
fn reap_signin_leftovers(profiles_root: &Path) {
    let Ok(providers) = std::fs::read_dir(profiles_root) else {
        return;
    };
    for prov_entry in providers.flatten() {
        let provider = prov_entry.file_name().to_string_lossy().into_owned();
        if provider.starts_with('.') || !prov_entry.file_type().is_ok_and(|t| t.is_dir()) {
            continue;
        }
        reap_abandoned_signin_staging(&prov_entry.path());
        reap_superseded_merged_profiles(&prov_entry.path(), &provider);
    }
}

/// Resolves the target profile for an OAuth sign-in staged at `staging_dir` under
/// strict `1 Email / Account = 1 Profile` and `1 Agent = 1 Assigned Profile` rules:
/// 1. If an existing profile under `provider` already holds the same `account_id` or
///    `email`, updates that single profile in-place (never creating a duplicate profile
///    for the same account).
/// 2. If the signed-in account is new to `provider`:
///    - Uses `explicit_profile` if specified and not already occupied by a different account;
///    - Otherwise uses `default` if `default` does not yet hold a different account;
///    - Otherwise creates a new profile named after the account's email handle.
fn has_credential_material(value: &serde_json::Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    object.iter().any(|(key, value)| {
        (matches!(
            key.as_str(),
            "access_token"
                | "accessToken"
                | "refresh_token"
                | "refreshToken"
                | "api_key"
                | "apiKey"
                | "OPENAI_API_KEY"
                | "key"
                | "token"
        ) && value.as_str().is_some_and(|text| !text.trim().is_empty()))
            || has_credential_material(value)
    })
}

/// Declared `(primary_credentials, companion_state_files)` for a sign-in provider.
///
/// Only files in this manifest are ever read or committed into `profiles/<provider>/<profile>`;
/// vendor CLI runtime artifacts created inside `.signin-staging-<pid>` (such as `cli.log`
/// symlinks, `cache/`, `crashes/`, `brain/`, `builtin/`, `log/`, sockets, lockfiles, and
/// SQLite conversation caches) are ignored and discarded with the staging directory.
fn staged_signin_manifest(
    provider: &str,
) -> Result<(&'static [&'static str], &'static [&'static str]), Error> {
    match provider {
        "anthropic" => Ok((
            &[".credentials.json", ".claude/.credentials.json"],
            &[".claude.json", ".claude/.claude.json"],
        )),
        "openai" => Ok((&["auth.json", ".codex/auth.json"], &[])),
        "google-antigravity" => Ok((
            &[".gemini/antigravity-cli/antigravity-oauth-token"],
            &[
                ".gemini/antigravity-cli/antigravity_state.pbtxt",
                ".gemini/antigravity-cli/jetski_state.pbtxt",
                ".gemini/config/config.json",
                ".gemini/config/.migrated",
            ],
        )),
        "muse" => Ok((
            &[
                "muse/auth.json",
                ".config/muse/auth.json",
                ".local/share/muse/auth.json",
            ],
            &[
                "settings.json",
                "muse/settings.json",
                "muse/trust.json",
                ".config/muse/settings.json",
                ".config/muse/trust.json",
            ],
        )),
        "xai" => Ok((&[".grok/auth.json"], &[])),
        "spiron" | "sylphx" => Ok((
            &[
                "auth.json",
                "credentials.json",
                ".config/sylphx/auth.json",
                ".spiron/auth.json",
                ".spiron/credentials.json",
                ".config/sylphx/credentials.json",
                ".spiron/key",
                ".config/sylphx/key",
                ".config/opencode/sylphx.key",
            ],
            &[
                "config.toml",
                ".spiron/context.json",
                ".config/sylphx/context.json",
                ".config/opencode/opencode.json",
            ],
        )),
        "opencode" => Ok((
            &[
                "data-home/opencode/auth.json",
                ".local/share/opencode/auth.json",
            ],
            &[
                "opencode/opencode.json",
                ".config/opencode/opencode.json",
                "opencode.json",
            ],
        )),
        "deepseek" => Ok((
            &[".dsh/.credentials.yaml", ".config/opencode/deepseek.key"],
            &[
                ".dsh/settings.yaml",
                ".dsh/.anonymous-user-id",
                ".dsh/profiles/headless/cordis.yml",
            ],
        )),
        "omp" | "cursor" => Ok((
            &[
                "agent.db",
                ".omp/agent/agent.db",
                "cursor-auth.json",
                "auth.json",
                "credentials.json",
            ],
            &["models.yml", ".omp/agent/models.yml"],
        )),
        _ => Err(Error::Message(format!(
            "unsupported sign-in provider: {provider}"
        ))),
    }
}

/// Fail-closed regular-file reader under `root`:
/// - Returns `Ok(None)` ONLY when `rel` or an intermediate directory does not exist.
/// - Refuses (`Err`) if any intermediate segment is a symlink/non-directory or if the
///   target file itself is a symlink, directory, FIFO, socket, or device, opening the
///   leaf with `O_NOFOLLOW` on Unix to defeat TOCTOU symlink swaps.
fn read_staged_regular_file(root: &Path, rel: &str) -> Result<Option<Vec<u8>>, Error> {
    use std::io::Read as _;
    use std::path::Component;

    let rel_path = Path::new(rel);
    let comps: Vec<&std::ffi::OsStr> = rel_path
        .components()
        .map(|c| match c {
            Component::Normal(s) => Ok(s),
            _ => Err(Error::Message(format!(
                "invalid staged credential relative path: {rel}"
            ))),
        })
        .collect::<Result<_, _>>()?;
    if comps.is_empty() {
        return Err(Error::Message("empty staged credential path".into()));
    }

    let mut cur = root.to_path_buf();
    for parent_seg in &comps[..comps.len() - 1] {
        cur.push(parent_seg);
        let meta = match std::fs::symlink_metadata(&cur) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
            return Err(Error::Message(format!(
                "staged credential path traverses symlink or non-directory: {rel}"
            )));
        }
    }

    cur.push(comps[comps.len() - 1]);
    let leaf_meta = match std::fs::symlink_metadata(&cur) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    if leaf_meta.file_type().is_symlink() || !leaf_meta.file_type().is_file() {
        return Err(Error::Message(format!(
            "staged credential must be a regular file (not symlink or directory): {rel}"
        )));
    }

    let mut opts = std::fs::OpenOptions::new();
    opts.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        opts.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = match opts.open(&cur) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    if !file.metadata()?.file_type().is_file() {
        return Err(Error::Message(format!(
            "staged credential must be a regular file: {rel}"
        )));
    }
    let mut bytes = Vec::with_capacity(leaf_meta.len() as usize);
    file.read_to_end(&mut bytes)?;
    Ok(Some(bytes))
}

fn validate_staged_primary_bytes(provider: &str, rel: &str, bytes: &[u8]) -> Result<(), Error> {
    if bytes.is_empty() {
        return Err(Error::Message(format!("empty staged credential: {rel}")));
    }
    if rel.ends_with(".key") || rel.ends_with("/key") {
        let valid_key = std::str::from_utf8(bytes).is_ok_and(|text| {
            let token = text.trim();
            !token.is_empty() && !token.contains(char::is_whitespace)
        });
        if !valid_key {
            return Err(Error::Message(format!("invalid staged API key: {rel}")));
        }
        return Ok(());
    }
    if rel.ends_with(".yaml") || rel.ends_with(".yml") {
        let valid_yaml = std::str::from_utf8(bytes).is_ok_and(|text| !text.trim().is_empty());
        if !valid_yaml {
            return Err(Error::Message(format!(
                "invalid staged YAML credential: {rel}"
            )));
        }
        return Ok(());
    }
    if rel.ends_with(".db") {
        if !bytes.starts_with(b"SQLite format 3\0") {
            return Err(Error::Message(format!(
                "invalid staged SQLite credential store: {rel}"
            )));
        }
        return Ok(());
    }
    // AGY also accepts an operator-provided plaintext refresh token. Other
    // providers use JSON credential stores; metadata alone is not a login.
    let plaintext_agy_token = provider == "google-antigravity"
        && std::str::from_utf8(bytes).is_ok_and(|text| {
            let token = text.trim();
            !token.is_empty()
                && !token.contains(char::is_whitespace)
                && !token.starts_with(['{', '[', '"'])
        });
    if !plaintext_agy_token {
        let value: serde_json::Value = serde_json::from_slice(bytes)
            .map_err(|err| Error::Message(format!("invalid staged credential {rel}: {err}")))?;
        if !has_credential_material(&value) {
            return Err(Error::Message(format!(
                "no usable credential material in staged file: {rel}"
            )));
        }
    }
    Ok(())
}

pub(super) fn validate_staged_credentials(stage: &Path, provider: &str) -> Result<(), Error> {
    let (primary_rels, companion_rels) = staged_signin_manifest(provider)?;
    let mut found = false;
    for rel in primary_rels {
        let Some(bytes) = read_staged_regular_file(stage, rel)? else {
            continue;
        };
        validate_staged_primary_bytes(provider, rel, &bytes)?;
        found = true;
    }
    for rel in companion_rels {
        let _ = read_staged_regular_file(stage, rel)?;
    }
    if !found {
        return Err(Error::Message("no staged sign-in credentials".into()));
    }
    Ok(())
}

fn commit_staged_signin_files(
    staging_dir: &Path,
    final_dir: &Path,
    provider: &str,
) -> Result<(), Error> {
    let (primary_rels, companion_rels) = staged_signin_manifest(provider)?;
    let mut to_commit: Vec<(&'static str, Vec<u8>)> = Vec::new();
    let mut primary_count = 0usize;
    for &rel in primary_rels {
        let Some(bytes) = read_staged_regular_file(staging_dir, rel)? else {
            continue;
        };
        validate_staged_primary_bytes(provider, rel, &bytes)?;
        to_commit.push((rel, bytes));
        primary_count += 1;
    }
    if primary_count == 0 {
        return Err(Error::Message("no staged sign-in credentials".into()));
    }
    for &rel in companion_rels {
        if let Some(bytes) = read_staged_regular_file(staging_dir, rel)?
            && !bytes.is_empty()
        {
            to_commit.push((rel, bytes));
        }
    }
    std::fs::create_dir_all(final_dir)?;
    for (rel, bytes) in to_commit {
        write_required_file(&final_dir.join(rel), &bytes, 0o600)?;
    }
    Ok(())
}

pub(crate) fn resolve_and_commit_staged_signin(
    data: &Path,
    provider: &str,
    staging_dir: &Path,
    explicit_profile: Option<&str>,
) -> Result<(String, PathBuf, Option<String>), Error> {
    validate_staged_credentials(staging_dir, provider)?;
    deduplicate_same_account_profiles(data);
    if provider == "anthropic" {
        let root_cred = staging_dir.join(".credentials.json");
        let sub_cred = staging_dir.join(".claude/.credentials.json");
        let root_mt = std::fs::metadata(&root_cred)
            .and_then(|m| m.modified())
            .ok();
        let sub_mt = std::fs::metadata(&sub_cred).and_then(|m| m.modified()).ok();
        if sub_mt > root_mt
            && let Ok(bytes) = std::fs::read(&sub_cred)
        {
            write_required_file(&root_cred, &bytes, 0o600)?;
        } else if root_mt.is_some()
            && let Ok(bytes) = std::fs::read(&root_cred)
        {
            let _ = std::fs::create_dir_all(staging_dir.join(".claude"));
            write_required_file(&sub_cred, &bytes, 0o600)?;
        }
        let (claude_email, claude_uuid) = extract_profile_account_identity(staging_dir, provider);
        if let Ok(cred_text) = std::fs::read_to_string(&root_cred)
            && let Ok(mut cred) = serde_json::from_str::<serde_json::Value>(&cred_text)
        {
            if let Some(oauth) = cred
                .get_mut("claudeAiOauth")
                .and_then(serde_json::Value::as_object_mut)
            {
                if let Some(ref em) = claude_email {
                    oauth.insert("email".to_string(), serde_json::Value::String(em.clone()));
                }
                if let Some(ref u) = claude_uuid {
                    oauth.insert(
                        "accountUuid".to_string(),
                        serde_json::Value::String(u.clone()),
                    );
                }
            }
            if let Some(tok) = cred
                .pointer("/claudeAiOauth/accessToken")
                .and_then(serde_json::Value::as_str)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
            {
                let http = ureq::Agent::config_builder()
                    .timeout_global(Some(std::time::Duration::from_secs(6)))
                    .build()
                    .new_agent();
                let _ = sync_anthropic_profile_metadata(
                    &http,
                    data,
                    staging_dir,
                    ".staging",
                    &tok,
                    &mut cred,
                    true,
                );
            }
            if let Ok(updated) = serde_json::to_string(&cred) {
                let _ = std::fs::create_dir_all(staging_dir.join(".claude"));
                write_required_file(&root_cred, updated.as_bytes(), 0o600)?;
                write_required_file(&sub_cred, updated.as_bytes(), 0o600)?;
            }
        }
    } else if provider == "google-antigravity"
        && std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
        && std::env::var_os("ANYAGENTS_FAKE_VENDOR_MODE").is_none()
    {
        let tok_rel = ".gemini/antigravity-cli/antigravity-oauth-token";
        let tok_path = staging_dir.join(tok_rel);
        let Some(tok_bytes) = read_staged_regular_file(staging_dir, tok_rel)? else {
            return Err(Error::Message(
                "agy sign-in did not complete: no OAuth token was written".into(),
            ));
        };
        if let Ok(text) = std::str::from_utf8(&tok_bytes)
            && let Ok(mut val) = serde_json::from_str::<serde_json::Value>(text)
        {
            let has_email = val
                .get("email")
                .or_else(|| val.get("id_token_email"))
                .and_then(serde_json::Value::as_str)
                .is_some_and(|s| !s.trim().is_empty());
            if !has_email
                && let Some(access_tok) = val
                    .pointer("/token/access_token")
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
            {
                let http = ureq::Agent::config_builder()
                    .timeout_global(Some(std::time::Duration::from_secs(6)))
                    .build()
                    .new_agent();
                if let Ok(mut resp) = http
                    .get("https://www.googleapis.com/oauth2/v2/userinfo")
                    .header("Authorization", &format!("Bearer {access_tok}"))
                    .call()
                    && let Ok(body) = resp.body_mut().read_to_string()
                    && let Ok(info) = serde_json::from_str::<serde_json::Value>(&body)
                    && let Some(obj) = val.as_object_mut()
                {
                    if let Some(em) = info
                        .get("email")
                        .and_then(serde_json::Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                    {
                        obj.insert(
                            "id_token_email".to_string(),
                            serde_json::Value::String(em.to_string()),
                        );
                        obj.insert(
                            "email".to_string(),
                            serde_json::Value::String(em.to_string()),
                        );
                    }
                    if let Some(id) = info
                        .get("id")
                        .and_then(serde_json::Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                    {
                        obj.insert(
                            "account_id".to_string(),
                            serde_json::Value::String(id.to_string()),
                        );
                    }
                    if let Ok(updated) = serde_json::to_string_pretty(&val) {
                        write_required_file(&tok_path, updated.as_bytes(), 0o600)?;
                    }
                }
            }
        }
    }

    let (staged_email, staged_id) = extract_profile_account_identity(staging_dir, provider);
    let existing = list_harness_profiles(data, provider);
    struct ExistingProf {
        name: String,
        email: Option<String>,
        acct_id: Option<String>,
        has_creds: bool,
    }
    let mut profs = Vec::new();
    for (name, dir) in &existing {
        let (em, aid) = extract_profile_account_identity(dir, provider);
        let has_creds = em.is_some()
            || aid.is_some()
            || dir.join(".credentials.json").is_file()
            || dir.join("auth.json").is_file()
            || dir
                .join(".gemini/antigravity-cli/antigravity-oauth-token")
                .is_file();
        profs.push(ExistingProf {
            name: name.clone(),
            email: em,
            acct_id: aid,
            has_creds,
        });
    }

    let matches_staged = |p: &ExistingProf| -> bool {
        if let (Some(s_id), Some(p_id)) = (staged_id.as_deref(), p.acct_id.as_deref())
            && s_id == p_id
        {
            return true;
        }
        if let (Some(s_em), Some(p_em)) = (staged_email.as_deref(), p.email.as_deref())
            && s_em.eq_ignore_ascii_case(p_em)
        {
            return true;
        }
        false
    };

    let chosen_profile = if let Some(existing_match) = profs.iter().find(|p| matches_staged(p)) {
        // 1 Email / Account = 1 Profile: if this account already exists under `provider`,
        // always update its existing profile rather than creating a second profile with the same email.
        existing_match.name.clone()
    } else if let Some(exp) = explicit_profile.map(str::trim).filter(|s| !s.is_empty()) {
        exp.to_string()
    } else if profs.is_empty()
        || profs
            .iter()
            .find(|p| p.name == "default")
            .is_none_or(|p| !p.has_creds)
    {
        "default".to_string()
    } else if let Some(empty) = profs.iter().find(|p| !p.has_creds) {
        empty.name.clone()
    } else {
        let base_slug = staged_email
            .as_deref()
            .map(sanitize_profile_slug)
            .or_else(|| staged_id.as_deref().map(sanitize_profile_slug))
            .unwrap_or_else(|| "acct".to_string());
        let mut candidate = base_slug.clone();
        let mut seq = 2usize;
        while profs.iter().any(|p| p.name == candidate) {
            candidate = format!("{base_slug}-{seq}");
            seq += 1;
        }
        candidate
    };

    let final_dir = data.join("profiles").join(provider).join(&chosen_profile);
    if staging_dir != final_dir {
        commit_staged_signin_files(staging_dir, &final_dir, provider)?;
        std::fs::remove_dir_all(staging_dir)?;
    }
    finalize_profile_signin(data, provider, &chosen_profile)?;
    deduplicate_same_account_profiles(data);
    let (final_email, final_id) = extract_profile_account_identity(&final_dir, provider);
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
        && std::env::var_os("ANYAGENTS_FAKE_VENDOR_MODE").is_none()
        && (data.join("state.db").is_file()
            || anyagents_core::db::configured_database_url_for_data_dir(data).is_some())
        && let Ok(store) = Store::open(data.join("state.db"))
    {
        // Moving agents onto the new profile is the server's rebalance; a
        // sign-in never reconfigures other agents' seats from this process.
        let _ = sync_and_list_provider_profiles_inner(&store, data, true, false);
    }
    Ok((chosen_profile, final_dir, final_email.or(final_id)))
}

pub(crate) fn finalize_profile_signin(
    data: &Path,
    provider: &str,
    profile: &str,
) -> Result<(), Error> {
    let prof_dir = data.join("profiles").join(provider).join(profile);
    ensure_provider_profile_meta(&prof_dir, provider, None);
    let _ = std::fs::remove_file(prof_dir.join(OAUTH_REFRESH_ATTEMPT_FILE));
    let _ = std::fs::remove_file(prof_dir.join(OAUTH_REFRESH_RESERVATION_FILE));
    let _ = std::fs::remove_file(prof_dir.join(".anyagents-profile-meta-sync-ms"));
    let _ = std::fs::remove_file(prof_dir.join(".anyagents-usage-backoff-until-ms"));
    let _ = std::fs::remove_file(prof_dir.join(".anyagents-quota-reset-ms"));
    let (pre_email, pre_id) = extract_profile_account_identity(&prof_dir, provider);
    update_profile_json_identity(&prof_dir, provider, pre_email.as_deref(), pre_id.as_deref());
    match provider {
        "anthropic" => {
            let root_cred = prof_dir.join(".credentials.json");
            let sub_cred = prof_dir.join(".claude/.credentials.json");
            let root_mt = std::fs::metadata(&root_cred)
                .and_then(|m| m.modified())
                .ok();
            let sub_mt = std::fs::metadata(&sub_cred).and_then(|m| m.modified()).ok();
            if sub_mt > root_mt
                && let Ok(bytes) = std::fs::read(&sub_cred)
            {
                write_required_file(&root_cred, &bytes, 0o600)?;
            } else if root_mt.is_some()
                && let Ok(bytes) = std::fs::read(&root_cred)
            {
                let _ = std::fs::create_dir_all(prof_dir.join(".claude"));
                write_required_file(&sub_cred, &bytes, 0o600)?;
            }
            if let Ok(cred_text) = std::fs::read_to_string(&root_cred)
                && let Ok(mut cred) = serde_json::from_str::<serde_json::Value>(&cred_text)
            {
                if let Some(oauth) = cred
                    .get_mut("claudeAiOauth")
                    .and_then(serde_json::Value::as_object_mut)
                {
                    if let Some(ref em) = pre_email {
                        oauth.insert("email".to_string(), serde_json::Value::String(em.clone()));
                    }
                    if let Some(ref u) = pre_id {
                        oauth.insert(
                            "accountUuid".to_string(),
                            serde_json::Value::String(u.clone()),
                        );
                    }
                }
                if let Ok(updated) = serde_json::to_string(&cred) {
                    let _ = std::fs::create_dir_all(prof_dir.join(".claude"));
                    write_required_file(&root_cred, updated.as_bytes(), 0o600)?;
                    write_required_file(&sub_cred, updated.as_bytes(), 0o600)?;
                }
                if let Some(tok) = cred
                    .pointer("/claudeAiOauth/accessToken")
                    .and_then(serde_json::Value::as_str)
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                {
                    let http = ureq::Agent::config_builder()
                        .timeout_global(Some(std::time::Duration::from_secs(6)))
                        .build()
                        .new_agent();
                    let _ = sync_anthropic_profile_metadata(
                        &http, data, &prof_dir, profile, &tok, &mut cred, true,
                    );
                }
            }
            propagate_profile_files_to_bound_agents(
                data,
                "anthropic",
                profile,
                &[".credentials.json", ".claude/.credentials.json"],
            )?;
        }
        "openai" => {
            let root_auth = prof_dir.join("auth.json");
            let sub_auth = prof_dir.join(".codex/auth.json");
            let root_mt = std::fs::metadata(&root_auth)
                .and_then(|m| m.modified())
                .ok();
            let sub_mt = std::fs::metadata(&sub_auth).and_then(|m| m.modified()).ok();
            if sub_mt > root_mt
                && let Ok(bytes) = std::fs::read(&sub_auth)
            {
                write_required_file(&root_auth, &bytes, 0o600)?;
            } else if root_mt.is_some()
                && let Ok(bytes) = std::fs::read(&root_auth)
            {
                let _ = std::fs::create_dir_all(prof_dir.join(".codex"));
                write_required_file(&sub_auth, &bytes, 0o600)?;
            }
            propagate_profile_files_to_bound_agents(
                data,
                "openai",
                profile,
                &["auth.json", ".codex/auth.json"],
            )?;
        }
        "google-antigravity" => {
            propagate_profile_files_to_bound_agents(
                data,
                "google-antigravity",
                profile,
                &[
                    ".gemini/antigravity-cli/antigravity-oauth-token",
                    ".gemini/antigravity-cli/antigravity_state.pbtxt",
                    ".gemini/antigravity-cli/jetski_state.pbtxt",
                    ".gemini/config/config.json",
                ],
            )?;
        }
        "muse" => {
            propagate_profile_files_to_bound_agents(
                data,
                "muse",
                profile,
                &["muse/auth.json", ".config/muse/auth.json"],
            )?;
        }
        "xai" => {
            propagate_profile_files_to_bound_agents(data, "xai", profile, &[".grok/auth.json"])?;
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn finalize_agent_signin(data: &Path, agent: &Agent) -> Result<(), Error> {
    let cfg = agent_config_dir(data, &agent.id);
    let provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
    let (primary_rels, companion_rels) = staged_signin_manifest(&provider)?;
    let staging = SigninStaging::create(
        data,
        &provider,
        &format!("agent-{}-{}", agent.id.as_str(), anyagents_core::now_ms()),
    )?;
    let stage = staging.path();
    let read_seat_regular = |rel: &str| -> Result<Option<Vec<u8>>, Error> {
        match read_staged_regular_file(&cfg, rel) {
            Ok(v) => Ok(v),
            Err(Error::Io(ref io_err)) if io_err.kind() == std::io::ErrorKind::PermissionDenied => {
                chmod_seat_file_readable(&cfg.join(rel));
                read_staged_regular_file(&cfg, rel)
            }
            Err(err) => Err(err),
        }
    };
    let stage_res = (|| -> Result<(), Error> {
        let mut best_src: Option<(std::time::SystemTime, Vec<u8>)> = None;
        for &rel in primary_rels {
            let Some(bytes) = read_seat_regular(rel)? else {
                continue;
            };
            if bytes.is_empty() {
                continue;
            }
            let mt = std::fs::symlink_metadata(cfg.join(rel))
                .ok()
                .and_then(|m| m.modified().ok())
                .unwrap_or(std::time::UNIX_EPOCH);
            if matches!(provider.as_str(), "anthropic" | "openai") {
                if best_src.as_ref().is_none_or(|(bmt, _)| mt > *bmt) {
                    best_src = Some((mt, bytes));
                }
            } else {
                write_required_file(&stage.join(rel), &bytes, 0o600)?;
            }
        }
        if let Some((_, bytes)) = best_src {
            for &rel in primary_rels {
                write_required_file(&stage.join(rel), &bytes, 0o600)?;
            }
        }
        for &rel in companion_rels {
            if let Some(bytes) = read_seat_regular(rel)?
                && !bytes.is_empty()
            {
                write_required_file(&stage.join(rel), &bytes, 0o600)?;
            }
        }
        if !primary_rels.iter().any(|rel| stage.join(rel).is_file()) {
            return Err(Error::Message("no staged sign-in credentials".into()));
        }
        Ok(())
    })();
    stage_res?;
    let (resolved_prof, _, _) = resolve_and_commit_staged_signin(data, &provider, stage, None)?;
    let normalized = format!("{provider}/{resolved_prof}\n");
    write_profile_marker(&cfg, ".anyagents-profile", &normalized)?;
    write_profile_marker(&cfg, ".anyagents-seeded-profile", &normalized)?;
    finalize_profile_signin(data, &provider, &resolved_prof)?;
    Ok(())
}
