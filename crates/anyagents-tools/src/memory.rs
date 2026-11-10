//! Backend-agnostic 3-tier file memory store (`MEMORY.md` index + `topics/<topic>.md`)
//! operating over [`WorkspaceBackend`] (`LocalHost` or `RemoteSandbox`).

use crate::WorkspaceBackend;

/// File-backed memory store operating transparently over `LocalHost` or `RemoteSandbox`.
#[derive(Clone, Debug)]
pub struct MemoryStore {
    backend: WorkspaceBackend,
    memory_dir: String,
}

impl MemoryStore {
    /// Create a [`MemoryStore`] rooted at `memory_dir` (relative to the backend root or absolute).
    pub fn new(backend: WorkspaceBackend, memory_dir: impl Into<String>) -> Self {
        let raw = memory_dir.into();
        let trimmed = raw.trim().trim_end_matches('/').to_string();
        Self {
            backend,
            memory_dir: if trimmed.is_empty() {
                "memory".to_string()
            } else {
                trimmed
            },
        }
    }

    /// Create a [`MemoryStore`] using the default `"memory"` subdirectory.
    pub fn with_default_dir(backend: WorkspaceBackend) -> Self {
        Self::new(backend, "memory")
    }

    pub fn backend(&self) -> &WorkspaceBackend {
        &self.backend
    }

    pub fn memory_dir(&self) -> &str {
        &self.memory_dir
    }

    fn index_path(&self) -> String {
        format!("{}/MEMORY.md", self.memory_dir)
    }

    fn topics_dir(&self) -> String {
        format!("{}/topics", self.memory_dir)
    }

    fn topic_path(&self, topic: &str) -> Result<String, String> {
        let cleaned = normalize_topic_slug(topic)?;
        Ok(format!("{}/topics/{cleaned}.md", self.memory_dir))
    }

    /// Read `<memory_dir>/MEMORY.md`.
    pub fn read_index(&self) -> Result<String, String> {
        let (_, bytes) = self.backend.read_file_bytes(Some(&self.index_path()))?;
        String::from_utf8(bytes).map_err(|e| format!("MEMORY.md is not valid UTF-8: {e}"))
    }

    /// Write `<memory_dir>/MEMORY.md` (creating parent directories if needed).
    pub fn write_index(&self, content: &str) -> Result<(), String> {
        self.backend
            .write_file_bytes(Some(&self.index_path()), content.as_bytes(), false, true)
            .map(|_| ())
    }

    /// Read `<memory_dir>/topics/<topic>.md`.
    pub fn read_topic(&self, topic: &str) -> Result<String, String> {
        let rel = self.topic_path(topic)?;
        let (_, bytes) = self.backend.read_file_bytes(Some(&rel))?;
        String::from_utf8(bytes).map_err(|e| format!("topic `{topic}` is not valid UTF-8: {e}"))
    }

    /// Write `<memory_dir>/topics/<topic>.md` (creating parent directories if needed).
    pub fn write_topic(&self, topic: &str, content: &str) -> Result<(), String> {
        let rel = self.topic_path(topic)?;
        self.backend
            .write_file_bytes(Some(&rel), content.as_bytes(), false, true)
            .map(|_| ())
    }

    /// List all topic names (without the `.md` suffix) under `<memory_dir>/topics`.
    pub fn list_topics(&self) -> Result<Vec<String>, String> {
        let listing = match self.backend.list_dir(Some(&self.topics_dir()), false, 500) {
            Ok(val) => val,
            Err(_) => return Ok(Vec::new()),
        };
        let Some(entries) = listing.get("entries").and_then(|v| v.as_array()) else {
            return Ok(Vec::new());
        };
        let mut topics = Vec::new();
        for item in entries {
            let kind = item.get("kind").and_then(|v| v.as_str()).unwrap_or("file");
            if kind != "file" {
                continue;
            }
            if let Some(name) = item.get("name").and_then(|v| v.as_str())
                && let Some(stem) = name.strip_suffix(".md")
                && !stem.is_empty()
            {
                topics.push(stem.to_string());
            }
        }
        topics.sort();
        Ok(topics)
    }
}

fn normalize_topic_slug(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    let without_ext = trimmed.strip_suffix(".md").unwrap_or(trimmed).trim();
    if without_ext.is_empty() {
        return Err("memory topic name must not be empty".to_string());
    }
    if without_ext.contains('/')
        || without_ext.contains('\\')
        || without_ext.contains("..")
        || without_ext.contains('\0')
    {
        return Err(format!("invalid memory topic name `{raw}`"));
    }
    Ok(without_ext.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_store_reads_and_writes_index_and_topics_on_local_host() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoryStore::with_default_dir(WorkspaceBackend::LocalHost {
            root: dir.path().to_path_buf(),
        });

        assert!(store.list_topics().unwrap().is_empty());
        store
            .write_index("# Memory Index\n- architecture\n")
            .unwrap();
        assert_eq!(
            store.read_index().unwrap(),
            "# Memory Index\n- architecture\n"
        );

        store
            .write_topic("architecture", "## Architecture Notes\nClean core.\n")
            .unwrap();
        store
            .write_topic("testing.md", "## Testing Notes\nDeterministic.\n")
            .unwrap();

        let topics = store.list_topics().unwrap();
        assert_eq!(
            topics,
            vec!["architecture".to_string(), "testing".to_string()]
        );
        assert_eq!(
            store.read_topic("architecture").unwrap(),
            "## Architecture Notes\nClean core.\n"
        );
        assert_eq!(
            store.read_topic("testing").unwrap(),
            "## Testing Notes\nDeterministic.\n"
        );
    }
}
