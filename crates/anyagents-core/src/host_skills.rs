//! Non-destructive global + per-agent skill views. Existing seat-local skill
//! directories win collisions and are never removed. Only symlinks created by
//! this host (and the previous global mount) may be replaced.
#[cfg(unix)]
use std::{
    collections::BTreeMap,
    fs, io,
    path::{Path, PathBuf},
};

#[cfg(unix)]
pub fn mount(agent_root: &Path, global: &Path, config: &Path) -> io::Result<()> {
    mount_with_fallback(agent_root, global, config, |_, _, _, _| {
        Err(io::Error::from(io::ErrorKind::PermissionDenied))
    })
}

/// Build the host view once; an inaccessible seat-owned vendor parent can be
/// installed by its owner without widening that parent or changing authority.
#[cfg(unix)]
pub fn mount_with_fallback(
    agent_root: &Path,
    global: &Path,
    config: &Path,
    fallback: impl Fn(&Path, &Path, &Path, &Path) -> io::Result<()>,
) -> io::Result<()> {
    let overlay = agent_root.join("skills");
    let view = agent_root.join("skills-view");
    fs::create_dir_all(agent_root)?;
    {
        use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, PermissionsExt as _};
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true).mode(0o700);
        builder.create(&overlay)?;
        let meta = fs::symlink_metadata(&overlay)?;
        if !meta.file_type().is_symlink() && (meta.mode() & 0o007) != 0 {
            let safe_mode = if meta.uid() == meta.gid() {
                0o700
            } else {
                meta.mode() & 0o7770
            };
            fs::set_permissions(&overlay, fs::Permissions::from_mode(safe_mode))?;
        }
    }
    fs::create_dir_all(&view)?;
    // The host publishes this read-only projection, not a seat-private overlay.
    // Explicit mode is required even when the daemon starts with umask 077.
    use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
    let directory = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
        .open(&view)?;
    directory.set_permissions(fs::Permissions::from_mode(0o755))?;
    // Refuse a foreign view/overlay link rather than writing into another tree.
    if fs::symlink_metadata(&overlay)?.file_type().is_symlink()
        || fs::symlink_metadata(&view)?.file_type().is_symlink()
    {
        return Err(io::Error::other(
            "skill overlay/view must be host directories",
        ));
    }
    let mut entries = BTreeMap::new();
    for source in [global, overlay.as_path()] {
        match fs::read_dir(source) {
            Ok(children) => {
                for child in children {
                    let child = child?;
                    let path = child.path();
                    if path.is_dir() {
                        entries.insert(child.file_name(), path);
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => (),
            Err(e) => return Err(e),
        }
    }
    sync_entries(&view, &entries, &[global, &overlay])?;
    for rel in [
        "skills",
        ".agents/skills",
        ".claude/skills",
        ".agy/skills",
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
        let target = config.join(rel);
        let parent = target
            .parent()
            .ok_or_else(|| io::Error::other("skill parent missing"))?;
        // A vendor config parent may be a user-managed symlink. Do not follow
        // it to install/clean entries in an unrelated home or global namespace.
        if parent
            .ancestors()
            .take_while(|p| *p != config)
            .any(|p| fs::symlink_metadata(p).is_ok_and(|m| m.file_type().is_symlink()))
        {
            continue;
        }
        let install = || -> io::Result<()> {
            fs::create_dir_all(parent)?;
            match fs::symlink_metadata(&target) {
                Ok(meta) if meta.file_type().is_dir() => {
                    sync_entries(&target, &entries, &[global, &overlay])?
                }
                Ok(meta) if meta.file_type().is_symlink() => {
                    let current = fs::read_link(&target)?;
                    if current == view {
                        return Ok(());
                    }
                    if current == global {
                        replace_link(&target, &view)?;
                    }
                    // Foreign mounts remain untouched, not recursively cleared.
                }
                Ok(_) => (),
                Err(e) if e.kind() == io::ErrorKind::NotFound => replace_link(&target, &view)?,
                Err(e) => return Err(e),
            }
            Ok(())
        };
        match install() {
            Ok(()) => (),
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                fallback(&target, &view, global, &overlay)?
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
#[cfg(unix)]
fn sync_entries(
    target: &Path,
    entries: &BTreeMap<std::ffi::OsString, PathBuf>,
    managed: &[&Path],
) -> io::Result<()> {
    for child in fs::read_dir(target)? {
        let child = child?;
        let path = child.path();
        if child.file_type()?.is_symlink() {
            let old = fs::read_link(&path)?;
            if managed.iter().any(|root| old.parent() == Some(*root))
                && !entries.contains_key(&child.file_name())
            {
                fs::remove_file(path)?;
            }
        }
    }
    for (name, source) in entries {
        let path = target.join(name);
        match fs::symlink_metadata(&path) {
            Ok(meta) if meta.file_type().is_symlink() => {
                let old = fs::read_link(&path)?;
                if old != *source && managed.iter().any(|root| old.parent() == Some(*root)) {
                    replace_link(&path, source)?;
                }
            }
            Ok(_) => (), // Never overwrite independent local content.
            Err(e) if e.kind() == io::ErrorKind::NotFound => replace_link(&path, source)?,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
#[cfg(unix)]
fn replace_link(path: &Path, source: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("skill link parent missing"))?;
    let temp = parent.join(format!(".skills-{}", uuid::Uuid::now_v7()));
    std::os::unix::fs::symlink(source, &temp)?;
    if let Err(e) = fs::rename(&temp, path) {
        let _ = fs::remove_file(temp);
        return Err(e);
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    fn skill(root: &Path, name: &str, text: &[u8]) {
        fs::create_dir_all(root.join(name)).unwrap();
        fs::write(root.join(name).join("SKILL.md"), text).unwrap();
    }
    #[test]
    fn overlay_preserves_global_bytes_and_local_skills_across_all_mirrors() {
        let dir = crate::test_tempdir();
        let root = dir.path().join("agents/a");
        let config = root.join("config");
        let global = dir.path().join("skills");
        let bytes = b"---\nname: global\n---\nexact \xff\n";
        skill(&global, "global", bytes);
        skill(&global, "collision", b"global original");
        skill(&root.join("skills"), "collision", b"agent override");
        skill(&config.join(".claude/skills"), "local", b"unique local");
        fs::create_dir_all(&config).unwrap();
        std::os::unix::fs::symlink(&global, config.join("skills")).unwrap();
        mount(&root, &global, &config).unwrap();
        mount(&root, &global, &config).unwrap();
        for rel in [
            "skills",
            ".agents/skills",
            ".claude/skills",
            ".codex/skills",
            ".config/opencode/skills",
        ] {
            assert_eq!(
                fs::read(config.join(rel).join("global/SKILL.md")).unwrap(),
                bytes
            );
            assert_eq!(
                fs::read(config.join(rel).join("collision/SKILL.md")).unwrap(),
                b"agent override"
            );
        }
        assert_eq!(
            fs::read(global.join("collision/SKILL.md")).unwrap(),
            b"global original"
        );
        assert_eq!(
            fs::read(config.join(".claude/skills/local/SKILL.md")).unwrap(),
            b"unique local"
        );
        fs::remove_dir_all(root.join("skills/collision")).unwrap();
        mount(&root, &global, &config).unwrap();
        assert_eq!(
            fs::read(config.join("skills/collision/SKILL.md")).unwrap(),
            b"global original"
        );
    }
    #[test]
    fn foreign_mount_and_symlink_parent_are_not_mutated() {
        let dir = crate::test_tempdir();
        let root = dir.path().join("agents/a");
        let config = root.join("config");
        let global = dir.path().join("skills");
        let personal = dir.path().join("personal");
        skill(&global, "shared", b"shared");
        skill(&personal, "private", b"private");
        fs::create_dir_all(&config).unwrap();
        std::os::unix::fs::symlink(&personal, config.join("skills")).unwrap();
        std::os::unix::fs::symlink(&personal, config.join(".claude")).unwrap();
        mount(&root, &global, &config).unwrap();
        assert_eq!(fs::read_link(config.join("skills")).unwrap(), personal);
        assert!(!personal.join("shared").exists());
        assert!(!personal.join("skills").exists());
        assert_eq!(
            fs::read(personal.join("private/SKILL.md")).unwrap(),
            b"private"
        );
    }
}
