//! PostgreSQL config persistence for newly spawned agents.

use std::path::Path;

use anyagents_core::{AgentId, Store};

const PROFILE_MARKER_RELS: &[&str] = &[
    ".anyagents-profile",
    ".anyagents-seeded-profile",
    ".anyagents-profile-rotated-ms",
    ".anyagents-quota-cleared-ms",
    ".anyagents-last-quota-check-ms",
];

/// Persist agent config, profile markers, and seeded auth credentials into
/// PostgreSQL `agent_files` so any stateless replica immediately sees both
/// newly spawned agents and runtime profile/model/quota-rotation updates.
pub(crate) fn persist_spawned_agent_config_to_store(
    store: &Store,
    agent_id: &AgentId,
    config_dir: &Path,
) {
    if !store.is_postgres() {
        return;
    }
    let blobs = store.blob_store();
    persist_agent_config_to_blobs(blobs.as_ref(), agent_id, config_dir);
}

fn persist_agent_config_to_blobs(
    blobs: &dyn anyagents_core::stateless::StateBlobStore,
    agent_id: &AgentId,
    config_dir: &Path,
) {
    let config_rels = [
        "SYSTEM_PROMPT.md",
        "AGENTS.md",
        "CLAUDE.md",
        ".claude/CLAUDE.md",
        ".claude.json",
        "settings.json",
        "config.toml",
        "spiron-mcp.json",
        ".gemini/config/GEMINI.md",
        ".gemini/antigravity-cli/settings.json",
        "opencode/AGENTS.md",
        ".config/opencode/AGENTS.md",
        "opencode/opencode.json",
        ".config/opencode/opencode.json",
        ".anyagents-managed-worktree.json",
        "repo-spec.json",
    ];
    for rel in config_rels {
        let path = config_dir.join(rel);
        if let Ok(bytes) = std::fs::read(&path)
            && !bytes.is_empty()
            && let Err(err) = blobs.put_agent_file(agent_id, "config", rel, &bytes, false)
        {
            eprintln!("anyagents-store: persist config/{rel} for `{agent_id}` failed: {err}");
        }
    }
    for rel in PROFILE_MARKER_RELS {
        let path = config_dir.join(rel);
        if let Ok(bytes) = std::fs::read(&path)
            && !bytes.is_empty()
        {
            for ns in ["markers", "config"] {
                if let Err(err) = blobs.put_agent_file(agent_id, ns, rel, &bytes, false) {
                    eprintln!("anyagents-store: persist {ns}/{rel} for `{agent_id}` failed: {err}");
                }
            }
        }
    }
    if !config_dir.join(".anyagents-last-error.json").exists() {
        for ns in ["markers", "config"] {
            let _ = blobs.delete_agent_file(agent_id, ns, ".anyagents-last-error.json");
        }
    }
    let secret_rels = collect_spawned_secret_rels(config_dir);
    for rel in secret_rels {
        let path = config_dir.join(&rel);
        if let Ok(bytes) = std::fs::read(&path)
            && !bytes.is_empty()
            && let Err(err) = blobs.put_agent_file(agent_id, "secrets", &rel, &bytes, true)
        {
            eprintln!("anyagents-store: persist secrets/{rel} for `{agent_id}` failed: {err}");
        }
    }
}

/// Collect relative secret file paths to persist into PostgreSQL `agent_files(namespace = 'secrets')`
/// when a new agent is spawned or its profile is switched, including any `{file:~/...}` key files
/// referenced by `opencode.json`.
fn collect_spawned_secret_rels(config_dir: &Path) -> Vec<String> {
    let mut rels: Vec<String> = [
        ".gemini/antigravity-cli/antigravity-oauth-token",
        ".gemini/oauth_creds.json",
        ".claude/.credentials.json",
        ".claude/.claude.json",
        ".credentials.json",
        ".codex/auth.json",
        ".grok/auth.json",
        "auth.json",
        "credentials.json",
        ".spiron/auth.json",
        ".config/sylphx/auth.json",
        ".config/sylphx/credentials.json",
        ".config/sylphx/key",
        ".config/opencode/sylphx.key",
        "data-home/opencode/auth.json",
        ".local/share/opencode/auth.json",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    for candidate in [
        ".config/opencode/opencode.json",
        "opencode/opencode.json",
        "opencode.json",
    ] {
        if let Ok(text) = std::fs::read_to_string(config_dir.join(candidate)) {
            for r in super::workspace::extract_file_tilde_refs(&text) {
                if !rels.contains(&r) {
                    rels.push(r);
                }
            }
        }
    }
    rels
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn collect_spawned_secret_rels_includes_opencode_file_tilde_key_refs() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path();
        std::fs::create_dir_all(cfg.join(".config/opencode")).unwrap();
        std::fs::write(
            cfg.join(".config/opencode/opencode.json"),
            r#"{"provider":{"custom":{"options":{"apiKey":"{file:~/.config/custom/provider.key}"}}}}"#,
        )
        .unwrap();
        let rels = collect_spawned_secret_rels(cfg);
        assert!(rels.iter().any(|r| r == ".config/custom/provider.key"));
        assert!(rels.iter().any(|r| r == ".config/opencode/sylphx.key"));
        assert!(rels.iter().any(|r| r == ".spiron/auth.json"));
    }

    #[test]
    fn persist_agent_config_updates_markers_config_and_secrets_and_clears_stale_error() {
        use anyagents_core::Error;
        use anyagents_core::stateless::{BlobEntry, BlobMeta, StateBlobStore};
        use std::collections::BTreeMap;
        use std::sync::Mutex;

        #[derive(Default)]
        struct MemBlobs {
            agent_files: Mutex<BTreeMap<(String, String, String), BlobEntry>>,
        }

        impl StateBlobStore for MemBlobs {
            fn get_agent_file(
                &self,
                agent_id: &AgentId,
                namespace: &str,
                path: &str,
            ) -> Result<Option<BlobEntry>, Error> {
                Ok(self
                    .agent_files
                    .lock()
                    .unwrap()
                    .get(&(
                        agent_id.as_str().to_string(),
                        namespace.to_string(),
                        path.to_string(),
                    ))
                    .cloned())
            }
            fn put_agent_file(
                &self,
                agent_id: &AgentId,
                namespace: &str,
                path: &str,
                content: &[u8],
                encrypted: bool,
            ) -> Result<BlobMeta, Error> {
                let entry = BlobEntry {
                    namespace: namespace.to_string(),
                    path: path.to_string(),
                    content: content.to_vec(),
                    sha256: "test".to_string(),
                    encrypted,
                    version: 1,
                    updated_at_ms: 1,
                };
                self.agent_files.lock().unwrap().insert(
                    (
                        agent_id.as_str().to_string(),
                        namespace.to_string(),
                        path.to_string(),
                    ),
                    entry,
                );
                Ok(BlobMeta {
                    namespace: namespace.to_string(),
                    path: path.to_string(),
                    sha256: "test".to_string(),
                    encrypted,
                    version: 1,
                    updated_at_ms: 1,
                    size_bytes: content.len(),
                })
            }
            fn delete_agent_file(
                &self,
                agent_id: &AgentId,
                namespace: &str,
                path: &str,
            ) -> Result<bool, Error> {
                Ok(self
                    .agent_files
                    .lock()
                    .unwrap()
                    .remove(&(
                        agent_id.as_str().to_string(),
                        namespace.to_string(),
                        path.to_string(),
                    ))
                    .is_some())
            }
            fn list_agent_files(
                &self,
                agent_id: &AgentId,
                namespace: &str,
            ) -> Result<Vec<BlobMeta>, Error> {
                Ok(self
                    .agent_files
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|((a, n, _), _)| a == agent_id.as_str() && n == namespace)
                    .map(|(_, e)| BlobMeta {
                        namespace: e.namespace.clone(),
                        path: e.path.clone(),
                        sha256: e.sha256.clone(),
                        encrypted: e.encrypted,
                        version: e.version,
                        updated_at_ms: e.updated_at_ms,
                        size_bytes: e.content.len(),
                    })
                    .collect())
            }
            fn get_deployment_blob(&self, _: &str, _: &str) -> Result<Option<BlobEntry>, Error> {
                Ok(None)
            }
            fn put_deployment_blob(
                &self,
                ns: &str,
                path: &str,
                content: &[u8],
                encrypted: bool,
            ) -> Result<BlobMeta, Error> {
                Ok(BlobMeta {
                    namespace: ns.to_string(),
                    path: path.to_string(),
                    sha256: "test".to_string(),
                    encrypted,
                    version: 1,
                    updated_at_ms: 1,
                    size_bytes: content.len(),
                })
            }
            fn delete_deployment_blob(&self, _: &str, _: &str) -> Result<bool, Error> {
                Ok(false)
            }
            fn list_deployment_blobs(&self, _: &str) -> Result<Vec<BlobMeta>, Error> {
                Ok(Vec::new())
            }
        }

        let blobs = MemBlobs::default();
        let aid = AgentId::generate();

        // Seed stale prior profile markers & error in both `markers` and `config`.
        for ns in ["markers", "config"] {
            blobs
                .put_agent_file(&aid, ns, ".anyagents-profile", b"provider/old\n", false)
                .unwrap();
            blobs
                .put_agent_file(
                    &aid,
                    ns,
                    ".anyagents-seeded-profile",
                    b"provider/old\n",
                    false,
                )
                .unwrap();
            blobs
                .put_agent_file(&aid, ns, ".anyagents-last-error.json", b"{}", false)
                .unwrap();
        }
        blobs
            .put_agent_file(
                &aid,
                "secrets",
                ".gemini/antigravity-cli/antigravity-oauth-token",
                b"old-oauth-token",
                true,
            )
            .unwrap();

        // Simulate runtime profile switch on local disk + removal of last error.
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path();
        std::fs::create_dir_all(cfg.join(".gemini/antigravity-cli")).unwrap();
        std::fs::write(cfg.join(".anyagents-profile"), "provider/new\n").unwrap();
        std::fs::write(cfg.join(".anyagents-seeded-profile"), "provider/new\n").unwrap();
        std::fs::write(cfg.join(".anyagents-profile-rotated-ms"), "1795000000000\n").unwrap();
        std::fs::write(
            cfg.join(".gemini/antigravity-cli/antigravity-oauth-token"),
            "new-oauth-token",
        )
        .unwrap();

        persist_agent_config_to_blobs(&blobs, &aid, cfg);

        // Both `markers` and `config` must reflect the new profile so hydration never reverts.
        for ns in ["markers", "config"] {
            assert_eq!(
                blobs
                    .get_agent_file(&aid, ns, ".anyagents-profile")
                    .unwrap()
                    .unwrap()
                    .content,
                b"provider/new\n"
            );
            assert_eq!(
                blobs
                    .get_agent_file(&aid, ns, ".anyagents-seeded-profile")
                    .unwrap()
                    .unwrap()
                    .content,
                b"provider/new\n"
            );
            assert!(
                blobs
                    .get_agent_file(&aid, ns, ".anyagents-last-error.json")
                    .unwrap()
                    .is_none(),
                "cleared error marker must be deleted from {ns}"
            );
        }
        let sec = blobs
            .get_agent_file(
                &aid,
                "secrets",
                ".gemini/antigravity-cli/antigravity-oauth-token",
            )
            .unwrap()
            .unwrap();
        assert_eq!(sec.content, b"new-oauth-token");
        assert!(sec.encrypted);
    }
}
