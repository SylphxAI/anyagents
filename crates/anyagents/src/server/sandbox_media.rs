//! Materialize channel media on the destination volume before advertising local paths.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyagents_core::{AgentId, MessageId, Store};

use crate::sandbox_providers::providers::{
    ExecRequest, SandboxHandle, SandboxProvider, build_bundle_tar_gz,
};

const SCOPED_PREFIX: &str = "/v1/media/";
const LEGACY_PREFIX: &str = "/var/lib/anyagents/media/";
const EXEC_TIMEOUT_MS: u64 = 10_000;

// Probe the actual destination, not the Coordinator's bundle manifest. A deleted
// file or a replaced volume must be fetched again. Never use symlinks as hits.
const CACHE_PROBE: &str = r#"
set -eu
dir=$1
shift
[ ! -L "$dir" ] || exit 1
for name do
    file="$dir/$name"
    if [ ! -L "$file" ] && [ -f "$file" ] && [ -s "$file" ]; then
        printf '%s\n' "$name"
    fi
done
"#;

const PUBLISH: &str = r#"
set -eu
dir=$1
stage=$2
name=$3
[ ! -L "$dir" ] && [ ! -L "$stage" ]
file="$dir/$name"
if [ ! -L "$file" ] && [ -f "$file" ] && [ -s "$file" ]; then
    exit 0
fi
[ ! -L "$file" ] && [ ! -d "$file" ]
source="$stage/$name"
[ ! -L "$source" ] && [ -f "$source" ] && [ -s "$source" ]
mv -f "$source" "$file"
[ ! -L "$file" ] && [ -f "$file" ] && [ -s "$file" ]
"#;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum MediaRef<'a> {
    Scoped(&'a str),
    Legacy(&'a str),
}

impl<'a> MediaRef<'a> {
    fn name(self) -> &'a str {
        match self {
            Self::Scoped(name) | Self::Legacy(name) => name,
        }
    }
}

fn media_refs<'a>(
    prompt: &'a str,
    owner: &AgentId,
    local_prefix: &str,
) -> Vec<(std::ops::Range<usize>, MediaRef<'a>)> {
    let delimiter = |c: char| {
        c.is_whitespace()
            || matches!(
                c,
                '[' | ']' | '(' | ')' | '"' | '\'' | '<' | '>' | ',' | ';'
            )
    };
    let mut refs = Vec::new();
    let mut cursor = 0;
    for token in prompt.split(delimiter) {
        if token.is_empty() {
            continue;
        }
        let start = cursor + prompt[cursor..].find(token).expect("token in prompt");
        cursor = start + token.len();
        let Some(reference) = token
            .strip_prefix(SCOPED_PREFIX)
            .and_then(|tail| tail.strip_prefix(owner.as_str())?.strip_prefix('/'))
            .map(MediaRef::Scoped)
            .or_else(|| {
                token
                    .strip_prefix(local_prefix)
                    .or_else(|| token.strip_prefix(LEGACY_PREFIX))
                    .map(MediaRef::Legacy)
            })
        else {
            continue;
        };
        let name = reference.name();
        if !name.is_empty()
            && name.len() <= 200
            && !name.starts_with('.')
            && !name.contains("..")
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
        {
            refs.push((start..cursor, reference));
        }
    }
    refs
}

async fn exec_media<'a>(
    provider: &dyn SandboxProvider,
    handle: &SandboxHandle,
    script: &'a str,
    args: impl IntoIterator<Item = &'a str>,
) -> Result<String, String> {
    let req = ExecRequest::argv(["/bin/sh", "-ec", script, "media"].into_iter().chain(args))
        .with_timeout_ms(EXEC_TIMEOUT_MS);
    let out = provider.exec(handle, &req).await?;
    if out.exit_code != 0 {
        return Err(format!("sandbox media command exited {}", out.exit_code));
    }
    Ok(out.stdout)
}

async fn upload_media(
    provider: &dyn SandboxProvider,
    handle: &SandboxHandle,
    dir: &str,
    name: &str,
    bytes: Vec<u8>,
) -> Result<(), String> {
    // Upload to a unique sibling inside the destination directory so promotion is
    // a same-filesystem rename. Providers do not need a new guest binary.
    let stage = format!("{dir}/.incoming-{}", MessageId::generate());
    let tar = build_bundle_tar_gz(&[(name.to_string(), bytes)])?;
    exec_media(
        provider,
        handle,
        r#"[ ! -L "$1" ]; mkdir -p "$1"; (umask 077; mkdir "$2")"#,
        [dir, &stage],
    )
    .await?;
    let result = async {
        provider.sync_bundle_tar(handle, &stage, &tar).await?;
        exec_media(provider, handle, PUBLISH, [dir, &stage, name]).await?;
        Ok(())
    }
    .await;
    let _ = exec_media(
        provider,
        handle,
        r#"rm -f "$1/$2"; rmdir "$1""#,
        [stage.as_str(), name],
    )
    .await;
    result
}

/// Used by both the opener and the mid-turn steer spool. On failure, keep the
/// original reference and annotate it; never inject a nonexistent local path.
/// Subsequent deliveries retry misses from the durable source.
pub(super) async fn hydrate_prompt(
    store: &Arc<Store>,
    owner: &AgentId,
    provider: &dyn SandboxProvider,
    handle: &SandboxHandle,
    prompt: &str,
) -> String {
    let media_dir = store.data_dir().join(anyagents_harness::media::MEDIA_DIR);
    let local_prefix = format!("{}/", media_dir.display());
    let refs = media_refs(prompt, owner, &local_prefix);
    if refs.is_empty() {
        return prompt.to_owned();
    }

    let dir = format!("{}/media", handle.state_dir.trim_end_matches('/'));
    let mut ready: BTreeMap<_, bool> = refs.iter().map(|(_, r)| (*r, false)).collect();
    // If a legacy basename collides with a scoped attachment, do not let it use
    // that cache entry or replace the scoped file.
    let scoped_names: BTreeSet<_> = ready
        .keys()
        .filter_map(|r| match r {
            MediaRef::Scoped(name) => Some(*name),
            MediaRef::Legacy(_) => None,
        })
        .collect();
    for chunk in ready.keys().copied().collect::<Vec<_>>().chunks(64) {
        let probe = exec_media(
            provider,
            handle,
            CACHE_PROBE,
            std::iter::once(dir.as_str()).chain(chunk.iter().map(|r| r.name())),
        )
        .await;
        if let Err(err) = &probe {
            eprintln!("anyagents-media: sandbox cache probe failed: {err}");
            continue;
        }
        let probe = probe.unwrap_or_default();
        for &reference in chunk {
            let name = reference.name();
            if matches!(reference, MediaRef::Legacy(_)) && scoped_names.contains(name) {
                continue;
            }
            let cached = probe.lines().any(|line| line == name);
            if !cached {
                let store = Arc::clone(store);
                let owner = owner.clone();
                let name_owned = name.to_owned();
                let kind = matches!(reference, MediaRef::Scoped(_));
                let result = async {
                    let bytes = tokio::task::spawn_blocking(move || {
                        if kind {
                            super::s3_media_profiles::load_scoped_media_bytes(
                                &store,
                                &owner,
                                &name_owned,
                            )
                            .map_err(|err| err.status.to_string())
                        } else {
                            load_legacy_media(&store, &name_owned)
                        }
                    })
                    .await
                    .map_err(|err| err.to_string())??
                    .filter(|bytes| !bytes.is_empty())
                    .ok_or("media is missing or empty")?;
                    upload_media(provider, handle, &dir, name, bytes).await
                }
                .await;
                if let Err(err) = result {
                    eprintln!(
                        "anyagents-media: could not hydrate inbound attachment {name}: {err}"
                    );
                    continue;
                }
            }
            ready.insert(reference, true);
        }
    }

    let mut output = prompt.to_owned();
    for (span, r) in refs.into_iter().rev() {
        if ready[&r] {
            output.replace_range(span, &format!("{dir}/{}", r.name()));
        } else {
            output.insert_str(span.end, " (attachment unavailable in sandbox)");
        }
    }
    output
}

fn load_legacy_media(store: &Store, name: &str) -> Result<Option<Vec<u8>>, String> {
    let key = anyagents_core::MasterKey::from_env().map_err(|err| err.to_string())?;
    if let Ok(Some(blob)) = store
        .blob_store_with_key(key)
        .get_deployment_blob("media", name)
    {
        return Ok(Some(blob.content));
    }
    let path = store
        .data_dir()
        .join(anyagents_harness::media::MEDIA_DIR)
        .join(name);
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.to_string()),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::sandbox_providers::providers::{
        EnsureSandboxRequest, ExecOutput, LocalHostProvider, ProviderHealth, ProviderKind,
        SandboxCapabilities,
    };

    // The Coordinator supplies a guest workdir. This fixture executes on the
    // test host, so keep that workdir inside the disposable provider volume.
    struct FixtureVolume(LocalHostProvider);

    #[async_trait::async_trait]
    impl SandboxProvider for FixtureVolume {
        fn kind(&self) -> ProviderKind {
            self.0.kind()
        }

        fn capabilities(&self) -> SandboxCapabilities {
            self.0.capabilities()
        }

        async fn ensure(&self, req: &EnsureSandboxRequest) -> Result<SandboxHandle, String> {
            let mut req = req.clone();
            req.workdir = None;
            self.0.ensure(&req).await
        }

        async fn exec(
            &self,
            handle: &SandboxHandle,
            req: &ExecRequest,
        ) -> Result<ExecOutput, String> {
            self.0.exec(handle, req).await
        }

        async fn sync_bundle_tar(
            &self,
            handle: &SandboxHandle,
            dest_dir: &str,
            tar_gz: &[u8],
        ) -> Result<(), String> {
            self.0.sync_bundle_tar(handle, dest_dir, tar_gz).await
        }

        async fn release(&self, handle: &SandboxHandle) -> Result<(), String> {
            self.0.release(handle).await
        }

        async fn destroy(&self, handle: &SandboxHandle) -> Result<(), String> {
            self.0.destroy(handle).await
        }

        async fn health(&self) -> Result<ProviderHealth, String> {
            self.0.health().await
        }
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn warm_turn_hydrates_new_media_reuses_cache_and_preserves_misses() {
        let _env = super::super::tests::env_lock();
        let data = crate::test_tempdir();
        let volume = crate::test_tempdir();
        let store = Arc::new(Store::open(data.path().join("state.db")).unwrap());
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("worker", "claude-code"))
            .unwrap();
        let provider: Arc<dyn SandboxProvider> =
            Arc::new(FixtureVolume(LocalHostProvider::with_root(volume.path())));
        let warm = provider
            .ensure(&EnsureSandboxRequest::for_agent(agent.id.as_str()))
            .await
            .unwrap();
        let source = data
            .path()
            .join(anyagents_harness::media::MEDIA_DIR)
            .join(agent.id.as_str());
        std::fs::create_dir_all(&source).unwrap();
        std::fs::write(source.join("photo.jpg"), b"photo").unwrap();
        std::fs::write(source.join("blocked.jpg"), b"blocked").unwrap();
        let cached = std::path::Path::new(&warm.state_dir).join("media");
        std::fs::create_dir_all(cached.join("blocked.jpg")).unwrap();

        let photo = format!("/v1/media/{}/photo.jpg", agent.id);
        let missing = format!("/v1/media/{}/missing.pdf", agent.id);
        let blocked = format!("/v1/media/{}/blocked.jpg", agent.id);
        let prompt = format!("[Media (photo): {photo}]\n[{missing}] [{blocked}]");
        let (ensured, rendered) =
            super::super::serverless_envelope::ensure_and_prepare_sandbox_for_turn(
                Arc::clone(&store),
                Arc::clone(&provider),
                agent.clone(),
                prompt,
                "http://127.0.0.1:9".into(),
                Vec::new(),
            )
            .await
            .unwrap();
        assert_eq!(ensured.handle.state_dir, warm.state_dir);
        assert_eq!(ensured.handle.workdir, warm.workdir);
        assert!(std::path::Path::new(&ensured.handle.workdir).starts_with(volume.path()));
        assert_eq!(std::fs::read(cached.join("photo.jpg")).unwrap(), b"photo");
        let local = cached.join("photo.jpg");
        assert_eq!(
            rendered,
            format!(
                "[Media (photo): {}]\n[{missing} (attachment unavailable in sandbox)] [{blocked} (attachment unavailable in sandbox)]",
                local.display()
            )
        );
        assert!(cached.join("blocked.jpg").is_dir());
        assert_eq!(std::fs::read_dir(&cached).unwrap().count(), 2);

        std::fs::remove_file(source.join("photo.jpg")).unwrap();
        // The source is gone: a cache hit must be sufficient for the identical ref.
        assert_eq!(
            hydrate_prompt(&store, &agent.id, provider.as_ref(), &warm, &photo).await,
            local.display().to_string()
        );
        std::fs::remove_file(&local).unwrap();
        assert_eq!(
            hydrate_prompt(&store, &agent.id, provider.as_ref(), &warm, &photo).await,
            format!("{photo} (attachment unavailable in sandbox)")
        );
    }
}
