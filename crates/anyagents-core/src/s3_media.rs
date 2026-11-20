//! Portable inbound channel media persistence across standard S3-compatible
//! object storage (`ANYAGENTS_S3_*`), PostgreSQL `deployment_blobs`, and local
//! filesystem fallback in zero-config SQLite mode.

use std::fmt::Write as _;
use std::io::Read as _;
use std::path::Path;
use std::time::Duration;

use sha2::{Digest as _, Sha256};

use crate::{AgentId, MasterKey, S3Config, S3Store, Store};

const MAX_TELEGRAM_MEDIA_BYTES: usize = 20 * 1024 * 1024;

/// Build the canonical S3 object key for a per-agent inbound media attachment.
pub fn media_s3_key(agent_id: &str, media_id: &str) -> String {
    format!("media/{agent_id}/{media_id}")
}

/// Infer a safe HTTP `Content-Type` header from a sanitized media filename.
pub fn media_content_type(file_name: &str) -> &'static str {
    let ext = Path::new(file_name)
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase);
    match ext.as_deref() {
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("png") => "image/png",
        Some("webp") => "image/webp",
        Some("gif") => "image/gif",
        Some("mp4") => "video/mp4",
        Some("ogg" | "oga") => "audio/ogg",
        Some("mp3") => "audio/mpeg",
        Some("wav") => "audio/wav",
        Some("pdf") => "application/pdf",
        Some("txt" | "log" | "md") => "text/plain; charset=utf-8",
        Some("json") => "application/json",
        _ => "application/octet-stream",
    }
}

fn sanitize_media_filename(raw: &str) -> String {
    let cleaned: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .take(64)
        .collect();
    let trimmed = cleaned.trim_matches('.');
    if trimmed.is_empty() || trimmed.contains("..") {
        "attachment.bin".to_string()
    } else {
        trimmed.to_string()
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for b in digest {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Persist an inbound channel media attachment:
/// - When `ANYAGENTS_S3_*` is configured: stores in S3 under
///   `media/<agent_id>/<sha256>_<sanitized_name>` (encrypted at rest when
///   `ANYAGENTS_MASTER_KEY` is configured) and returns a portable ref
///   `[Media (<kind>): /v1/media/<agent_id>/<media_id>]`.
/// - Otherwise when PostgreSQL is active: stores in `deployment_blobs(namespace='media')`
///   under `<agent_id>/<sha256>_<sanitized_name>` and returns the same portable ref.
/// - Otherwise (zero-config local SQLite mode): writes into `<data_dir>/media/complete_<name>`
///   and returns `anyagents_harness::media::local_media_line`.
pub fn persist_inbound_media_bytes(
    store: &Store,
    agent_id: &AgentId,
    kind: &str,
    sanitized_name: &str,
    bytes: &[u8],
) -> Result<String, String> {
    let clean_name = sanitize_media_filename(sanitized_name);
    let digest = sha256_hex(bytes);
    let media_id = format!("{digest}_{clean_name}");
    let ct = media_content_type(&clean_name);

    if let Some(s3) = S3Store::from_env().map_err(|e| e.to_string())? {
        let key = media_s3_key(agent_id.as_str(), &media_id);
        match MasterKey::from_env().map_err(|e| e.to_string())? {
            Some(master_key) => s3
                .put_encrypted_object(&key, bytes, ct, Some(&master_key))
                .map_err(|e| e.to_string())?,
            None => s3.put_object(&key, bytes, ct).map_err(|e| e.to_string())?,
        }
        return Ok(format!(
            "[Media ({kind}): /v1/media/{}/{media_id}]",
            agent_id.as_str()
        ));
    }

    if store.is_postgres() {
        let blob_path = format!("{}/{media_id}", agent_id.as_str());
        let master_key = MasterKey::from_env().map_err(|e| e.to_string())?;
        let encrypted = master_key.is_some();
        let blobs = store.blob_store_with_key(master_key);
        blobs
            .put_deployment_blob("media", &blob_path, bytes, encrypted)
            .map_err(|e| e.to_string())?;
        return Ok(format!(
            "[Media ({kind}): /v1/media/{}/{media_id}]",
            agent_id.as_str()
        ));
    }

    let media_dir = store.data_dir().join(anyagents_harness::media::MEDIA_DIR);
    let _ = std::fs::create_dir_all(&media_dir);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(&media_dir, std::fs::Permissions::from_mode(0o711));
    }
    let dest = media_dir.join(format!("complete_{clean_name}"));
    if !dest.is_file() {
        anyagents_harness::atomic_write_file(&dest, bytes, 0o644)
            .map_err(|e| format!("failed persisting local media: {e}"))?;
    }
    Ok(anyagents_harness::media::local_media_line(kind, &dest))
}

/// Read a bounded inbound channel media stream (verifying optional `expected`
/// byte length) and persist it via [`persist_inbound_media_bytes`].
pub fn persist_inbound_media_stream(
    store: &Store,
    agent_id: &AgentId,
    kind: &str,
    sanitized_name: &str,
    reader: &mut dyn std::io::Read,
    expected: Option<i64>,
) -> Result<String, String> {
    let mut bytes = Vec::new();
    reader
        .take((MAX_TELEGRAM_MEDIA_BYTES as u64) + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| format!("media stream read failed: {e}"))?;
    if bytes.len() > MAX_TELEGRAM_MEDIA_BYTES {
        return Err("media exceeds 20 MiB limit".into());
    }
    if expected.is_some_and(|size| size < 0 || bytes.len() as u64 != size as u64) {
        return Err("media length mismatch".into());
    }
    persist_inbound_media_bytes(store, agent_id, kind, sanitized_name, &bytes)
}

/// Build a sanitized filename for a Telegram attachment, download its bytes from
/// `download_url`, and persist via S3 (`ANYAGENTS_S3_*`), PostgreSQL
/// `deployment_blobs(namespace='media')`, or the caller's local-disk `persist_local`
/// fallback in zero-config SQLite mode.
#[allow(clippy::too_many_arguments)]
pub fn download_and_persist_telegram_media(
    store: &Store,
    agent_id: &AgentId,
    kind: &str,
    file_id: &str,
    file_unique_id: Option<&str>,
    orig_file_name: Option<&str>,
    file_size: Option<i64>,
    remote_path: &str,
    download_url: &str,
    timeout_secs: u64,
    persist_local: impl FnOnce(&mut dyn std::io::Read, &Path, Option<i64>) -> std::io::Result<()>,
) -> Option<String> {
    let ext = Path::new(remote_path)
        .extension()
        .and_then(|e| e.to_str())
        .filter(|e| !e.is_empty())
        .unwrap_or(match kind {
            "photo" => "jpg",
            "video" | "video_note" | "animation" => "mp4",
            "voice" => "ogg",
            "audio" => "mp3",
            _ => "bin",
        });
    let base_id: String = file_unique_id
        .unwrap_or(file_id)
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
        .take(32)
        .collect();
    let file_name = match orig_file_name {
        Some(orig) => {
            let sanitized: String = orig
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                        c
                    } else {
                        '_'
                    }
                })
                .take(64)
                .collect();
            format!("{base_id}_{sanitized}")
        }
        None => format!("{kind}_{base_id}.{ext}"),
    };
    let http = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(timeout_secs)))
        .max_redirects(0)
        .build()
        .new_agent();

    if S3Config::from_env().is_some() || store.is_postgres() {
        let Ok(response) = http.get(download_url).call() else {
            return None;
        };
        if response.status().as_u16() != 200 {
            return None;
        }
        let mut reader = response.into_body().into_reader();
        return match persist_inbound_media_stream(
            store,
            agent_id,
            kind,
            &file_name,
            &mut reader,
            file_size,
        ) {
            Ok(line) => Some(line),
            Err(err) => {
                eprintln!("Telegram media persistence failed: {err}");
                None
            }
        };
    }

    let media_dir = store.data_dir().join(anyagents_harness::media::MEDIA_DIR);
    let _ = std::fs::create_dir_all(&media_dir);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(&media_dir, std::fs::Permissions::from_mode(0o711));
    }
    let dest = media_dir.join(format!("complete_{file_name}"));
    if !dest.is_file()
        && let Ok(response) = http.get(download_url).call()
        && response.status().as_u16() == 200
    {
        let mut reader = response.into_body().into_reader();
        if persist_local(&mut reader, &dest, file_size).is_err() {
            eprintln!("Telegram media transfer incomplete; attachment not cached");
        }
    }
    dest.is_file()
        .then(|| anyagents_harness::media::local_media_line(kind, &dest))
}
