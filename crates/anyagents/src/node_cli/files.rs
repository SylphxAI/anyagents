//! Path-jailed file operations for the `files` capability on the node daemon.
//!
//! All file operations are confined to explicit `--allow-dir` roots.  Symlink
//! escape, `..` traversal, and accesses outside allowed roots are denied
//! fail-closed.

use std::path::{Component, Path, PathBuf};

use base64::Engine as _;

use crate::node_protocol::{FilesEntry, MAX_FILE_SIZE_BYTES};

/// Compiled set of allowed directory roots for the files capability.
#[derive(Clone, Debug, Default)]
pub(crate) struct FilesJail {
    roots: Vec<PathBuf>,
}

impl FilesJail {
    pub(crate) fn new(roots: Vec<PathBuf>) -> Self {
        Self { roots }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        !self.roots.is_empty()
    }

    /// Validate and canonicalize `path`, ensuring it is strictly contained
    /// within one of the allowed roots.  Returns the canonical absolute path
    /// or an error message.
    pub(crate) fn resolve(&self, raw: &str) -> Result<PathBuf, String> {
        if self.roots.is_empty() {
            return Err("files capability is not enabled (no --allow-dir configured)".into());
        }

        let raw_trimmed = raw.trim();
        if raw_trimmed.is_empty() {
            return Err("empty file path".into());
        }

        // Reject any path containing NUL bytes.
        if raw_trimmed.contains('\0') {
            return Err("path contains NUL byte".into());
        }

        // Reject raw `..` components before canonicalization to catch
        // traversal attempts even when the intermediate path doesn't exist.
        let raw_path = Path::new(raw_trimmed);
        for component in raw_path.components() {
            if matches!(component, Component::ParentDir) {
                return Err("path traversal (`..`) is not allowed".into());
            }
        }

        // For read operations, the target must already exist so we can
        // canonicalize it and check containment.  For write operations the
        // parent must exist.
        let canonical = if raw_path.exists() {
            raw_path
                .canonicalize()
                .map_err(|err| format!("cannot resolve path: {err}"))?
        } else {
            // Parent must exist and be inside an allowed root.
            let parent = raw_path
                .parent()
                .ok_or_else(|| "path has no parent directory".to_string())?;
            if !parent.exists() {
                return Err(format!(
                    "parent directory does not exist: {}",
                    parent.display()
                ));
            }
            let canonical_parent = parent
                .canonicalize()
                .map_err(|err| format!("cannot resolve parent: {err}"))?;
            let file_name = raw_path
                .file_name()
                .ok_or_else(|| "path has no file name".to_string())?;
            canonical_parent.join(file_name)
        };

        // Verify the canonical path is strictly within one of the allowed roots.
        for root in &self.roots {
            let canonical_root = match root.canonicalize() {
                Ok(r) => r,
                Err(_) => continue,
            };
            if canonical.starts_with(&canonical_root) {
                return Ok(canonical);
            }
        }

        Err(format!(
            "path is outside all allowed directories: {}",
            canonical.display()
        ))
    }

    /// List entries at a directory path within the jail.
    pub(crate) fn list(&self, raw: &str) -> Result<Vec<FilesEntry>, String> {
        let canonical = self.resolve(raw)?;
        if !canonical.is_dir() {
            return Err(format!("{} is not a directory", canonical.display()));
        }
        let mut entries = Vec::new();
        let rd =
            std::fs::read_dir(&canonical).map_err(|err| format!("cannot read directory: {err}"))?;
        for item in rd {
            let item = item.map_err(|err| format!("directory entry error: {err}"))?;
            let meta = item
                .metadata()
                .map_err(|err| format!("metadata error: {err}"))?;
            entries.push(FilesEntry {
                name: item.file_name().to_string_lossy().into_owned(),
                is_dir: meta.is_dir(),
                size: meta.len(),
            });
        }
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(entries)
    }

    /// Read a file within the jail, returning base64-encoded content.
    pub(crate) fn read(&self, raw: &str) -> Result<String, String> {
        let canonical = self.resolve(raw)?;
        if canonical.is_dir() {
            return Err(format!(
                "{} is a directory, not a file",
                canonical.display()
            ));
        }
        let meta = std::fs::metadata(&canonical)
            .map_err(|err| format!("cannot read file metadata: {err}"))?;
        if meta.len() > MAX_FILE_SIZE_BYTES {
            return Err(format!(
                "file size {} exceeds maximum {} bytes",
                meta.len(),
                MAX_FILE_SIZE_BYTES
            ));
        }
        let content =
            std::fs::read(&canonical).map_err(|err| format!("cannot read file: {err}"))?;
        Ok(base64::engine::general_purpose::STANDARD.encode(&content))
    }

    /// Write base64-encoded data to a file within the jail.
    pub(crate) fn write(&self, raw: &str, data_b64: &str) -> Result<(), String> {
        let canonical = self.resolve(raw)?;
        if canonical.is_dir() {
            return Err(format!("{} is an existing directory", canonical.display()));
        }
        let data = base64::engine::general_purpose::STANDARD
            .decode(data_b64)
            .map_err(|err| format!("invalid base64 data: {err}"))?;
        if data.len() as u64 > MAX_FILE_SIZE_BYTES {
            return Err(format!(
                "write size {} exceeds maximum {} bytes",
                data.len(),
                MAX_FILE_SIZE_BYTES
            ));
        }
        std::fs::write(&canonical, &data).map_err(|err| format!("cannot write file: {err}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jail_denies_traversal_and_allows_contained_paths() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("allowed");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("file.txt"), b"hello").unwrap();
        let jail = FilesJail::new(vec![root.clone()]);

        // Contained file is allowed.
        assert!(
            jail.resolve(&root.join("file.txt").to_string_lossy())
                .is_ok()
        );

        // Parent traversal is denied.
        let escape = format!("{}/../../etc/passwd", root.display());
        assert!(jail.resolve(&escape).unwrap_err().contains(".."));

        // Path outside root is denied.
        let outside = dir.path().join("outside.txt");
        std::fs::write(&outside, b"nope").unwrap();
        assert!(
            jail.resolve(&outside.to_string_lossy())
                .unwrap_err()
                .contains("outside")
        );

        // Empty jail denies everything.
        let empty = FilesJail::default();
        assert!(
            empty
                .resolve(&root.join("file.txt").to_string_lossy())
                .unwrap_err()
                .contains("not enabled")
        );
    }

    #[test]
    fn list_read_write_within_jail() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("readme.txt"), b"test content").unwrap();
        let jail = FilesJail::new(vec![root.clone()]);

        let entries = jail.list(&root.to_string_lossy()).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "readme.txt");
        assert!(!entries[0].is_dir);

        let content = jail
            .read(&root.join("readme.txt").to_string_lossy())
            .unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&content)
            .unwrap();
        assert_eq!(decoded, b"test content");

        let new_data = base64::engine::general_purpose::STANDARD.encode(b"new content");
        jail.write(&root.join("new.txt").to_string_lossy(), &new_data)
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(root.join("new.txt")).unwrap(),
            "new content"
        );
    }

    #[test]
    fn symlink_escape_denied() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("jail");
        std::fs::create_dir_all(&root).unwrap();
        let secret = dir.path().join("secret.txt");
        std::fs::write(&secret, b"top-secret").unwrap();

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&secret, root.join("link")).unwrap();
            let jail = FilesJail::new(vec![root]);
            let result = jail.resolve("link");
            // The canonical path of the symlink target is outside the jail root.
            assert!(
                result.is_err(),
                "symlink escape should be denied: {result:?}"
            );
        }
    }
}
