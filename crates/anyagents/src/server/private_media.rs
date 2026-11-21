//! Private attachment admission. Uploads never enter the legacy shared inbound
//! media namespace or the coordinator's host-path resolution pipeline.

use std::ffi::OsStr;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::path::Path;

use anyagents_core::{AgentId, MasterKey, MessageId, S3Store, Store, s3_media};
use axum::body::{Body, to_bytes};
use axum::extract::{
    DefaultBodyLimit, FromRequest, FromRequestParts, Multipart, Path as AxumPath, Query, Request,
    State,
};
use axum::http::{StatusCode, header, request::Parts};
use axum::response::IntoResponse;
use axum::{
    Json, Router,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};

use crate::auth::{Mode, Principal};
use crate::seat_tree::{open_child, open_root, private_media_dir};
use crate::telegram::MediaUpload;

use super::{ApiError, AppState, spawn_store};

use anyagents_harness::media::PRIVATE_MEDIA_MAX_BYTES as MAX_BYTES;
const MULTIPART_OVERHEAD: usize = 64 * 1024;
const URI_PREFIX: &str = "media://";
const STORAGE_NAMESPACE: &str = "private-media";
pub(super) const UPLOAD_HELP: &str = "Upload the file to $ANYAGENTS_API_URL/v1/media with the remote MCP token as Bearer auth (read $ANYAGENTS_MCP_TOKEN_FILE if set), then use the returned media://<id> in media.source.";

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/media",
            post(upload_route).layer(DefaultBodyLimit::max(MAX_BYTES + MULTIPART_OVERHEAD)),
        )
        .route(
            "/v1/media/{agent_id}/uploads/{media_id}",
            get(download_route),
        )
}

/// The network listener may share a state with a trusted single-user Unix
/// socket. A caller-supplied agent header alone is never upload/download auth.
struct MediaAgent(AgentId);

impl FromRequestParts<AppState> for MediaAgent {
    type Rejection = axum::response::Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let mut state = state.clone();
        if state.auth.is_trusted() {
            state.auth = Mode::Confined {
                operator_digest: None,
            };
        }
        match Principal::from_request_parts(parts, &state)
            .await
            .map_err(IntoResponse::into_response)?
        {
            Principal::Agent { agent_id, .. } => Ok(Self(agent_id)),
            Principal::Operator => Err(ApiError::forbidden(
                "private media requires an authenticated agent",
            )
            .into_response()),
        }
    }
}

#[derive(Default, Deserialize)]
struct UploadQuery {
    filename: Option<String>,
}

#[derive(Serialize)]
struct UploadReceipt {
    media_id: String,
    source: String,
    content_type: &'static str,
    filename: String,
    bytes: usize,
}

async fn upload_route(
    MediaAgent(agent_id): MediaAgent,
    State(state): State<AppState>,
    Query(query): Query<UploadQuery>,
    request: Request,
) -> Result<impl IntoResponse, ApiError> {
    let (parts, body) = request.into_parts();
    let declared_type = parts
        .headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());
    let multipart = declared_type
        .and_then(|ct| ct.split(';').next())
        .is_some_and(|ct| ct.trim().eq_ignore_ascii_case("multipart/form-data"));
    let limit = if multipart {
        MAX_BYTES + MULTIPART_OVERHEAD
    } else {
        MAX_BYTES
    };
    let bytes = to_bytes(body, limit).await.map_err(|e| {
        if std::error::Error::source(&e)
            .is_some_and(|source| source.is::<http_body_util::LengthLimitError>())
        {
            too_large()
        } else {
            bad_request("could not read media body")
        }
    })?;
    let (filename, bytes) = if multipart {
        let mut form =
            Multipart::from_request(Request::from_parts(parts, Body::from(bytes)), &state)
                .await
                .map_err(|_| bad_request("invalid multipart media body"))?;
        let file = form
            .next_field()
            .await
            .map_err(|_| bad_request("invalid multipart media body"))?
            .filter(|field| field.name() == Some("file"))
            .ok_or_else(|| bad_request("multipart media requires one `file` field"))?;
        let filename = choose_filename(
            file.file_name().or(query.filename.as_deref()),
            file.content_type(),
        );
        let bytes = file
            .bytes()
            .await
            .map_err(|_| bad_request("invalid multipart file"))?;
        if form
            .next_field()
            .await
            .map_err(|_| bad_request("invalid multipart media body"))?
            .is_some()
        {
            return Err(bad_request("multipart media accepts only one `file` field"));
        }
        (filename, bytes)
    } else {
        let header_name = parts
            .headers
            .get("x-filename")
            .and_then(|v| v.to_str().ok());
        (
            choose_filename(header_name.or(query.filename.as_deref()), declared_type),
            bytes,
        )
    };
    if bytes.is_empty() {
        return Err(bad_request("media body is empty"));
    }
    if bytes.len() > MAX_BYTES {
        return Err(too_large());
    }
    let content_type = detect_type(&filename, &bytes);
    let receipt = spawn_store(move || {
        state.store.agent(&agent_id).map_err(ApiError::from_store)?;
        let media_id = persist(&state.store, &agent_id, &filename, content_type, &bytes)
            .map_err(storage_error)?;
        Ok(UploadReceipt {
            source: format!("{URI_PREFIX}{media_id}"),
            media_id,
            filename,
            content_type,
            bytes: bytes.len(),
        })
    })
    .await?;
    Ok((
        StatusCode::CREATED,
        [(header::CACHE_CONTROL, "private, no-store")],
        Json(receipt),
    ))
}

async fn download_route(
    MediaAgent(caller): MediaAgent,
    State(state): State<AppState>,
    AxumPath((agent, media_id)): AxumPath<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    if caller.as_str() != agent {
        return Err(not_found());
    }
    let upload = spawn_store(move || load(&state.store, &caller, &media_id)).await?;
    Ok((
        [
            (header::CONTENT_TYPE, upload.content_type),
            (header::CONTENT_DISPOSITION, "attachment"),
            (header::CACHE_CONTROL, "private, no-store"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        upload.bytes,
    ))
}

fn bad_request(message: &'static str) -> ApiError {
    ApiError::new(StatusCode::BAD_REQUEST, message)
}

fn too_large() -> ApiError {
    ApiError::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        "media file must be at most 25 MiB",
    )
}

fn not_found() -> ApiError {
    ApiError::new(StatusCode::NOT_FOUND, "private media not found")
}

fn storage_error(error: anyagents_core::Error) -> ApiError {
    eprintln!("anyagents-media: private media store operation failed: {error}");
    ApiError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "private media storage unavailable",
    )
}

fn valid_filename(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && !name.starts_with('.')
        && !name.ends_with('.')
        && !name.contains("..")
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

fn choose_filename(filename: Option<&str>, declared_type: Option<&str>) -> String {
    if let Some(name) = filename {
        let clean: String = name
            .rsplit(['/', '\\'])
            .next()
            .unwrap_or_default()
            .trim()
            .chars()
            .take(64)
            .map(|c| {
                if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        if valid_filename(&clean) {
            return clean;
        }
    }
    // Use known types only; never reflect a caller-controlled MIME header.
    match declared_type
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("image/png") => "attachment.png",
        Some("image/jpeg") => "attachment.jpg",
        Some("image/gif") => "attachment.gif",
        Some("image/webp") => "attachment.webp",
        Some("application/pdf") => "attachment.pdf",
        Some("video/mp4") => "attachment.mp4",
        Some("audio/mpeg") => "attachment.mp3",
        Some("audio/ogg") => "attachment.ogg",
        Some("audio/wav") => "attachment.wav",
        _ => "attachment.bin",
    }
    .into()
}

fn detect_type(filename: &str, bytes: &[u8]) -> &'static str {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        "image/png"
    } else if bytes.starts_with(b"\xff\xd8\xff") {
        "image/jpeg"
    } else if bytes.starts_with(b"%PDF-") {
        "application/pdf"
    } else {
        s3_media::media_content_type(filename)
    }
}

fn filename_from_id(id: &str) -> Result<&str, ApiError> {
    let (unique, filename) = id
        .split_once('~')
        .ok_or_else(|| bad_request("invalid media identifier"))?;
    if MessageId::parse(unique).is_err() || !valid_filename(filename) {
        return Err(bad_request("invalid media identifier"));
    }
    Ok(filename)
}

pub(super) fn source_id(source: &str) -> Result<Option<&str>, ApiError> {
    let Some(id) = source.trim().strip_prefix(URI_PREFIX) else {
        return Ok(None);
    };
    filename_from_id(id)?;
    Ok(Some(id))
}

pub(super) fn load(store: &Store, caller: &AgentId, id: &str) -> Result<MediaUpload, ApiError> {
    let filename = filename_from_id(id)?;
    let bytes = read(store, caller, id)
        .map_err(storage_error)?
        .ok_or_else(not_found)?;
    if bytes.is_empty() || bytes.len() > MAX_BYTES {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "invalid stored media size",
        ));
    }
    Ok(MediaUpload {
        file_name: filename.to_string(),
        content_type: detect_type(filename, &bytes),
        bytes,
    })
}

pub(super) fn copy_for_recipient(
    store: &Store,
    recipient: &AgentId,
    upload: &MediaUpload,
) -> Result<String, anyagents_core::Error> {
    let id = persist(
        store,
        recipient,
        &upload.file_name,
        upload.content_type,
        &upload.bytes,
    )?;
    Ok(format!("/v1/media/{recipient}/uploads/{id}"))
}

fn persist(
    store: &Store,
    agent: &AgentId,
    filename: &str,
    mime: &str,
    bytes: &[u8],
) -> Result<String, anyagents_core::Error> {
    let id = format!("{}~{filename}", MessageId::generate());
    let s3 = S3Store::from_env()?;
    let master = if s3.is_some() || store.is_postgres() {
        MasterKey::from_env()?
    } else {
        None
    };
    // Keep storage separate from the legacy inbound namespace.
    let relative = format!("{agent}/{id}");
    if let Some(s3) = s3 {
        let key = format!("{STORAGE_NAMESPACE}/{relative}");
        if let Some(key_ref) = &master {
            s3.put_encrypted_object(&key, bytes, mime, Some(key_ref))?;
        } else {
            s3.put_object(&key, bytes, mime)?;
        }
    } else if store.is_postgres() {
        store
            .blob_store_with_key(master.clone())
            .put_deployment_blob(STORAGE_NAMESPACE, &relative, bytes, master.is_some())?;
    } else {
        write_local(store.data_dir(), agent, &id, bytes)?;
    }
    Ok(id)
}

fn read(
    store: &Store,
    agent: &AgentId,
    id: &str,
) -> Result<Option<Vec<u8>>, anyagents_core::Error> {
    let relative = format!("{agent}/{id}");
    if let Some(s3) = S3Store::from_env()? {
        let key = format!("{STORAGE_NAMESPACE}/{relative}");
        let s3 = s3.with_max_object_bytes(MAX_BYTES + 4096);
        return s3
            .get_object(&key)?
            .map(|raw| {
                if S3Store::is_encrypted_envelope(&raw) {
                    s3.decrypt_object_bytes(&key, &raw, None)
                } else {
                    Ok(raw)
                }
            })
            .transpose();
    }
    if store.is_postgres() {
        return Ok(store
            .blob_store_with_key(MasterKey::from_env()?)
            .get_deployment_blob(STORAGE_NAMESPACE, &relative)?
            .map(|blob| blob.content));
    }
    match read_local(store.data_dir(), agent, id) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn write_local(data: &Path, agent: &AgentId, id: &str, bytes: &[u8]) -> io::Result<()> {
    let data = open_root(data)?;
    let root = private_media_dir(&data, OsStr::new(STORAGE_NAMESPACE), None, false)?;
    let dir = private_media_dir(&root, OsStr::new(agent.as_str()), None, false)?;
    let name = std::ffi::CString::new(id)?;
    let fd = unsafe {
        libc::openat(
            dir.as_raw_fd(),
            name.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o600,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
    let result = file
        .write_all(bytes)
        .and_then(|()| file.sync_all())
        .and_then(|()| dir.sync_all());
    if result.is_err() {
        unsafe {
            libc::unlinkat(dir.as_raw_fd(), name.as_ptr(), 0);
        }
    }
    result
}

fn read_local(data: &Path, agent: &AgentId, id: &str) -> io::Result<Vec<u8>> {
    use std::os::unix::fs::MetadataExt;
    let data = open_root(data)?;
    let root = open_child(&data, OsStr::new(STORAGE_NAMESPACE), true)?;
    let dir = open_child(&root, OsStr::new(agent.as_str()), true)?;
    let file = open_child(&dir, OsStr::new(id), false)?;
    for entry in [&root, &dir, &file] {
        let meta = entry.metadata()?;
        if meta.uid() != crate::sandbox::server_uid() || meta.mode() & 0o077 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "invalid private media permissions",
            ));
        }
    }
    let meta = file.metadata()?;
    if !meta.is_file() || meta.nlink() != 1 || meta.len() > MAX_BYTES as u64 {
        return Err(io::Error::other("invalid private media file"));
    }
    let mut bytes = Vec::with_capacity(meta.len() as usize);
    file.take(MAX_BYTES as u64 + 1).read_to_end(&mut bytes)?;
    Ok(bytes)
}
