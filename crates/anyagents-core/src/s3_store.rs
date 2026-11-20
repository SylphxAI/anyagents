//! Pure open-source S3-compatible object storage client using AWS Signature V4
//! (`AWS4-HMAC-SHA256`) and optional AEAD encryption at rest via [`MasterKey`].
//!
//! Configured exclusively through standard environment variables:
//! - `ANYAGENTS_S3_ENDPOINT` (optional custom endpoint, e.g. `https://s3.example.com`)
//! - `ANYAGENTS_S3_BUCKET`
//! - `ANYAGENTS_S3_ACCESS_KEY_ID`
//! - `ANYAGENTS_S3_SECRET_ACCESS_KEY`
//! - `ANYAGENTS_S3_REGION` (default `"us-east-1"`)
//! - `ANYAGENTS_S3_PREFIX` (optional key prefix, trimmed of leading/trailing `/`)
//! - `ANYAGENTS_S3_FORCE_PATH_STYLE` (default `true` when `ANYAGENTS_S3_ENDPOINT` is set)

use std::fmt::{self, Write as _};
use std::io::Read as _;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Datelike as _, Timelike as _, Utc};
use sha2::{Digest as _, Sha256};
use url::Url;

use crate::error::Error;
use crate::stateless::crypto::MasterKey;

pub const S3_ENDPOINT_ENV: &str = "ANYAGENTS_S3_ENDPOINT";
pub const S3_BUCKET_ENV: &str = "ANYAGENTS_S3_BUCKET";
pub const S3_ACCESS_KEY_ID_ENV: &str = "ANYAGENTS_S3_ACCESS_KEY_ID";
pub const S3_SECRET_ACCESS_KEY_ENV: &str = "ANYAGENTS_S3_SECRET_ACCESS_KEY";
pub const S3_REGION_ENV: &str = "ANYAGENTS_S3_REGION";
pub const S3_PREFIX_ENV: &str = "ANYAGENTS_S3_PREFIX";
pub const S3_FORCE_PATH_STYLE_ENV: &str = "ANYAGENTS_S3_FORCE_PATH_STYLE";

/// Default upper bound on object payloads downloaded from S3 (256 MiB + 1 MiB AEAD headroom).
pub const DEFAULT_MAX_S3_OBJECT_BYTES: usize = 257 * 1024 * 1024;
const DEFAULT_S3_CONNECT_TIMEOUT_SECS: u64 = 15;
const DEFAULT_S3_TIMEOUT_SECS: u64 = 120;
const MAX_LIST_PAGES: usize = 1_000;
const MAX_LIST_XML_BYTES: u64 = 16 * 1024 * 1024;

/// Configuration for an S3-compatible object store.
///
/// Credentials are redacted in `Debug` output so keys never leak into logs.
#[derive(Clone, PartialEq, Eq)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
    pub prefix: Option<String>,
    pub force_path_style: bool,
}

impl fmt::Debug for S3Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("access_key_id", &"[redacted]")
            .field("secret_access_key", &"[redacted]")
            .field("region", &self.region)
            .field("prefix", &self.prefix)
            .field("force_path_style", &self.force_path_style)
            .finish()
    }
}

fn non_empty_env(name: &str) -> Option<String> {
    let raw = std::env::var(name).ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn parse_bool_env(name: &str) -> Option<bool> {
    let raw = non_empty_env(name)?;
    match raw.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

impl S3Config {
    /// Load standard S3-compatible storage configuration from `ANYAGENTS_S3_*`
    /// environment variables. Returns `None` when bucket or credentials are not set.
    pub fn from_env() -> Option<Self> {
        let bucket = non_empty_env(S3_BUCKET_ENV)?;
        let access_key_id = non_empty_env(S3_ACCESS_KEY_ID_ENV)?;
        let secret_access_key = non_empty_env(S3_SECRET_ACCESS_KEY_ENV)?;
        let region = non_empty_env(S3_REGION_ENV).unwrap_or_else(|| "us-east-1".to_string());
        let custom_endpoint = non_empty_env(S3_ENDPOINT_ENV);
        let has_custom_endpoint = custom_endpoint.is_some();
        let endpoint = custom_endpoint
            .unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com"))
            .trim_end_matches('/')
            .to_string();
        let prefix = non_empty_env(S3_PREFIX_ENV)
            .map(|p| p.trim_matches('/').to_string())
            .filter(|p| !p.is_empty());
        let force_path_style =
            parse_bool_env(S3_FORCE_PATH_STYLE_ENV).unwrap_or(has_custom_endpoint);

        Some(Self {
            endpoint,
            bucket,
            access_key_id,
            secret_access_key,
            region,
            prefix,
            force_path_style,
        })
    }

    /// Validate that the endpoint URL and bucket name are well-formed.
    pub fn validate(&self) -> Result<(), Error> {
        let parsed = Url::parse(&self.endpoint)
            .map_err(|err| Error::Message(format!("invalid S3 endpoint URL: {err}")))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(Error::Message(
                "S3 endpoint URL must use http:// or https://".into(),
            ));
        }
        if parsed.host_str().is_none() {
            return Err(Error::Message("S3 endpoint URL must include a host".into()));
        }
        let bucket = self.bucket.trim();
        if bucket.is_empty()
            || bucket.contains('/')
            || bucket.contains('\\')
            || bucket.contains("..")
            || bucket.contains('\0')
        {
            return Err(Error::Message(format!("invalid S3 bucket name `{bucket}`")));
        }
        if self.access_key_id.trim().is_empty() || self.secret_access_key.trim().is_empty() {
            return Err(Error::Message("S3 credentials must be non-empty".into()));
        }
        if self.region.trim().is_empty() {
            return Err(Error::Message("S3 region must be non-empty".into()));
        }
        Ok(())
    }
}

/// Object metadata returned by [`S3Store::head_object`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectMeta {
    pub key: String,
    pub size_bytes: u64,
    pub etag: Option<String>,
    pub content_type: Option<String>,
    pub last_modified: Option<String>,
}

/// Lightweight S3-compatible object storage client signed with AWS Signature V4.
#[derive(Clone)]
pub struct S3Store {
    config: S3Config,
    agent: ureq::Agent,
    max_object_bytes: usize,
}

impl fmt::Debug for S3Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Store")
            .field("config", &self.config)
            .field("max_object_bytes", &self.max_object_bytes)
            .finish()
    }
}

impl S3Store {
    /// Construct a new [`S3Store`] from explicit configuration.
    pub fn new(config: S3Config) -> Result<Self, Error> {
        config.validate()?;
        let agent = ureq::Agent::config_builder()
            .timeout_connect(Some(Duration::from_secs(DEFAULT_S3_CONNECT_TIMEOUT_SECS)))
            .timeout_global(Some(Duration::from_secs(DEFAULT_S3_TIMEOUT_SECS)))
            .http_status_as_error(false)
            .build()
            .new_agent();
        Ok(Self {
            config,
            agent,
            max_object_bytes: DEFAULT_MAX_S3_OBJECT_BYTES,
        })
    }

    /// Construct an [`S3Store`] from `ANYAGENTS_S3_*` environment variables, or
    /// return `Ok(None)` when S3 object storage is not configured.
    pub fn from_env() -> Result<Option<Self>, Error> {
        let Some(cfg) = S3Config::from_env() else {
            return Ok(None);
        };
        Self::new(cfg).map(Some)
    }

    /// Override the maximum response size accepted by [`Self::get_object`].
    pub fn with_max_object_bytes(mut self, max_bytes: usize) -> Self {
        self.max_object_bytes = max_bytes;
        self
    }

    /// Borrow the underlying [`S3Config`].
    pub fn config(&self) -> &S3Config {
        &self.config
    }

    /// Resolve a logical key to its full bucket key (applying optional prefix).
    pub fn full_key(&self, key: &str) -> Result<String, Error> {
        let clean = normalize_object_key(key)?;
        Ok(match self.config.prefix.as_deref() {
            Some(prefix) if !prefix.is_empty() => format!("{prefix}/{clean}"),
            _ => clean,
        })
    }

    /// Associated data binding AEAD ciphertexts to this bucket and full object key.
    pub fn encryption_aad(&self, key: &str) -> Result<Vec<u8>, Error> {
        let full = self.full_key(key)?;
        Ok(format!("anyagents-s3-v1:{}:{full}", self.config.bucket).into_bytes())
    }

    /// Return `true` when `bytes` begins with AnyAgents' AEAD ciphertext envelope magic.
    pub fn is_encrypted_envelope(bytes: &[u8]) -> bool {
        bytes.starts_with(b"AAENC1")
    }

    /// Decrypt an encrypted S3 object payload bound to `(bucket, full_key)`.
    pub fn decrypt_object_bytes(
        &self,
        key: &str,
        ciphertext: &[u8],
        master_key: Option<&MasterKey>,
    ) -> Result<Vec<u8>, Error> {
        let owned;
        let resolved_key = match master_key {
            Some(k) => k,
            None => {
                owned = MasterKey::require_from_env()?;
                &owned
            }
        };
        let aad = self.encryption_aad(key)?;
        resolved_key.decrypt(&aad, ciphertext)
    }

    /// Upload `bytes` to `key` with `content_type`.
    pub fn put_object(&self, key: &str, bytes: &[u8], content_type: &str) -> Result<(), Error> {
        let full = self.full_key(key)?;
        let req = self.build_signed_request(
            "PUT",
            Some(&full),
            &[],
            bytes,
            Some(content_type),
            current_utc_timestamp(),
        )?;
        let mut call = self.agent.put(&req.url);
        for (name, value) in &req.headers {
            call = call.header(name.as_str(), value.as_str());
        }
        let mut resp = call
            .send(bytes)
            .map_err(|err| Error::Message(format!("S3 PUT `{key}` failed: {err}")))?;
        let status = resp.status().as_u16();
        if (200..300).contains(&status) {
            return Ok(());
        }
        let detail = read_error_snippet(&mut resp);
        Err(Error::Message(format!(
            "S3 PUT `{key}` returned HTTP {status}{detail}"
        )))
    }

    /// Download `key`, returning `Ok(None)` when the object does not exist (`404`).
    pub fn get_object(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let full = self.full_key(key)?;
        let req =
            self.build_signed_request("GET", Some(&full), &[], &[], None, current_utc_timestamp())?;
        let mut call = self.agent.get(&req.url);
        for (name, value) in &req.headers {
            call = call.header(name.as_str(), value.as_str());
        }
        let mut resp = call
            .call()
            .map_err(|err| Error::Message(format!("S3 GET `{key}` failed: {err}")))?;
        let status = resp.status().as_u16();
        if status == 404 {
            return Ok(None);
        }
        if !(200..300).contains(&status) {
            let detail = read_error_snippet(&mut resp);
            return Err(Error::Message(format!(
                "S3 GET `{key}` returned HTTP {status}{detail}"
            )));
        }
        let limit = self.max_object_bytes as u64;
        let mut out = Vec::new();
        resp.body_mut()
            .as_reader()
            .take(limit.saturating_add(1))
            .read_to_end(&mut out)
            .map_err(|err| Error::Message(format!("S3 GET `{key}` body read failed: {err}")))?;
        if out.len() > self.max_object_bytes {
            return Err(Error::Message(format!(
                "S3 object `{key}` exceeds maximum allowed size ({} bytes)",
                self.max_object_bytes
            )));
        }
        Ok(Some(out))
    }

    /// Fetch metadata for `key` via `HEAD`, returning `Ok(None)` on `404`.
    pub fn head_object(&self, key: &str) -> Result<Option<ObjectMeta>, Error> {
        let full = self.full_key(key)?;
        let req = self.build_signed_request(
            "HEAD",
            Some(&full),
            &[],
            &[],
            None,
            current_utc_timestamp(),
        )?;
        let mut call = self.agent.head(&req.url);
        for (name, value) in &req.headers {
            call = call.header(name.as_str(), value.as_str());
        }
        let resp = call
            .call()
            .map_err(|err| Error::Message(format!("S3 HEAD `{key}` failed: {err}")))?;
        let status = resp.status().as_u16();
        if status == 404 {
            return Ok(None);
        }
        if !(200..300).contains(&status) {
            return Err(Error::Message(format!(
                "S3 HEAD `{key}` returned HTTP {status}"
            )));
        }
        let header_str = |name: &str| -> Option<String> {
            resp.headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        };
        let size_bytes = header_str("content-length")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        Ok(Some(ObjectMeta {
            key: normalize_object_key(key)?,
            size_bytes,
            etag: header_str("etag").map(|v| v.trim_matches('"').to_string()),
            content_type: header_str("content-type"),
            last_modified: header_str("last-modified"),
        }))
    }

    /// Delete `key`. Returns `Ok(true)` on `200`/`204`, `Ok(false)` on `404`.
    pub fn delete_object(&self, key: &str) -> Result<bool, Error> {
        let full = self.full_key(key)?;
        let req = self.build_signed_request(
            "DELETE",
            Some(&full),
            &[],
            &[],
            None,
            current_utc_timestamp(),
        )?;
        let mut call = self.agent.delete(&req.url);
        for (name, value) in &req.headers {
            call = call.header(name.as_str(), value.as_str());
        }
        let mut resp = call
            .call()
            .map_err(|err| Error::Message(format!("S3 DELETE `{key}` failed: {err}")))?;
        let status = resp.status().as_u16();
        if status == 404 {
            return Ok(false);
        }
        if (200..300).contains(&status) {
            return Ok(true);
        }
        let detail = read_error_snippet(&mut resp);
        Err(Error::Message(format!(
            "S3 DELETE `{key}` returned HTTP {status}{detail}"
        )))
    }

    /// List all logical keys matching `prefix` using S3 `ListObjectsV2` pagination.
    pub fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, Error> {
        let trimmed = prefix.trim_start_matches('/');
        if trimmed.contains('\0') || trimmed.contains('\\') || trimmed.contains("..") {
            return Err(Error::Message(format!("invalid S3 list prefix `{prefix}`")));
        }
        let full_prefix = match self.config.prefix.as_deref() {
            Some(base) if !base.is_empty() && !trimmed.is_empty() => format!("{base}/{trimmed}"),
            Some(base) if !base.is_empty() => format!("{base}/"),
            _ => trimmed.to_string(),
        };
        let strip_prefix = self
            .config
            .prefix
            .as_deref()
            .filter(|p| !p.is_empty())
            .map(|p| format!("{p}/"));

        let mut keys = Vec::new();
        let mut continuation: Option<String> = None;
        for _ in 0..MAX_LIST_PAGES {
            let mut query: Vec<(&str, &str)> = vec![("list-type", "2")];
            if !full_prefix.is_empty() {
                query.push(("prefix", &full_prefix));
            }
            if let Some(ref token) = continuation {
                query.push(("continuation-token", token.as_str()));
            }
            let req =
                self.build_signed_request("GET", None, &query, &[], None, current_utc_timestamp())?;
            let mut call = self.agent.get(&req.url);
            for (name, value) in &req.headers {
                call = call.header(name.as_str(), value.as_str());
            }
            let mut resp = call
                .call()
                .map_err(|err| Error::Message(format!("S3 ListObjectsV2 failed: {err}")))?;
            let status = resp.status().as_u16();
            if !(200..300).contains(&status) {
                let detail = read_error_snippet(&mut resp);
                return Err(Error::Message(format!(
                    "S3 ListObjectsV2 returned HTTP {status}{detail}"
                )));
            }
            let mut raw = Vec::new();
            resp.body_mut()
                .as_reader()
                .take(MAX_LIST_XML_BYTES)
                .read_to_end(&mut raw)
                .map_err(|err| {
                    Error::Message(format!("S3 ListObjectsV2 body read failed: {err}"))
                })?;
            let xml = String::from_utf8_lossy(&raw);
            let page = parse_list_objects_v2_xml(&xml);
            for raw_key in page.keys {
                let logical = match strip_prefix.as_deref() {
                    Some(sp) => raw_key.strip_prefix(sp).unwrap_or(&raw_key).to_string(),
                    None => raw_key,
                };
                keys.push(logical);
            }
            if !page.is_truncated {
                break;
            }
            match page.next_continuation_token {
                Some(next) if Some(&next) != continuation.as_ref() => continuation = Some(next),
                _ => break,
            }
        }
        Ok(keys)
    }

    /// Encrypt `plaintext` at rest with [`MasterKey`] (`ANYAGENTS_MASTER_KEY`)
    /// bound to `(bucket, full_key)` and store the ciphertext in S3.
    ///
    /// Fails closed when no master key is provided or configured in env.
    pub fn put_encrypted_object(
        &self,
        key: &str,
        plaintext: &[u8],
        content_type: &str,
        master_key: Option<&MasterKey>,
    ) -> Result<(), Error> {
        let owned;
        let resolved_key = match master_key {
            Some(k) => k,
            None => {
                owned = MasterKey::require_from_env()?;
                &owned
            }
        };
        let aad = self.encryption_aad(key)?;
        let ciphertext = resolved_key.encrypt(&aad, plaintext)?;
        let wire_ct = if content_type.trim().is_empty() {
            "application/octet-stream"
        } else {
            content_type
        };
        self.put_object(key, &ciphertext, wire_ct)
    }

    /// Fetch and authenticate-decrypt an encrypted object from S3 using
    /// [`MasterKey`] (`ANYAGENTS_MASTER_KEY`).
    ///
    /// Fails closed when no master key is provided or configured in env, or if
    /// authentication fails (wrong key, wrong key path, or tampered bytes).
    pub fn get_encrypted_object(
        &self,
        key: &str,
        master_key: Option<&MasterKey>,
    ) -> Result<Option<Vec<u8>>, Error> {
        let owned;
        let resolved_key = match master_key {
            Some(k) => k,
            None => {
                owned = MasterKey::require_from_env()?;
                &owned
            }
        };
        let Some(ciphertext) = self.get_object(key)? else {
            return Ok(None);
        };
        let aad = self.encryption_aad(key)?;
        resolved_key.decrypt(&aad, &ciphertext).map(Some)
    }

    fn build_signed_request(
        &self,
        method: &str,
        full_key: Option<&str>,
        query: &[(&str, &str)],
        payload: &[u8],
        content_type: Option<&str>,
        timestamp: UtcTimestamp,
    ) -> Result<SignedRequest, Error> {
        sign_s3_request(
            &self.config,
            method,
            full_key,
            query,
            payload,
            content_type,
            timestamp,
        )
    }
}

#[derive(Clone, Debug)]
struct UtcTimestamp {
    amz_date: String,
    date_stamp: String,
}

fn current_utc_timestamp() -> UtcTimestamp {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let dt = DateTime::<Utc>::from_timestamp(secs, 0).unwrap_or(DateTime::UNIX_EPOCH);
    UtcTimestamp {
        amz_date: format!(
            "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
            dt.year(),
            dt.month(),
            dt.day(),
            dt.hour(),
            dt.minute(),
            dt.second()
        ),
        date_stamp: format!("{:04}{:02}{:02}", dt.year(), dt.month(), dt.day()),
    }
}

#[derive(Clone, Debug)]
struct SignedRequest {
    url: String,
    headers: Vec<(String, String)>,
    #[cfg(test)]
    canonical_request: String,
    #[cfg(test)]
    signature: String,
}

fn normalize_object_key(key: &str) -> Result<String, Error> {
    let trimmed = key.trim().trim_start_matches('/');
    if trimmed.is_empty()
        || trimmed.ends_with('/')
        || trimmed.contains('\0')
        || trimmed.contains('\\')
    {
        return Err(Error::Message(format!("invalid S3 object key `{key}`")));
    }
    for segment in trimmed.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err(Error::Message(format!(
                "invalid S3 object key segment in `{key}`"
            )));
        }
    }
    Ok(trimmed.to_string())
}

fn uri_encode(input: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        let unreserved = byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'_' | b'.' | b'~')
            || (!encode_slash && byte == b'/');
        if unreserved {
            out.push(byte as char);
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn sha256_hex(data: &[u8]) -> String {
    hex_lower(&Sha256::digest(data))
}

/// Standard RFC 2104 HMAC-SHA256 supporting arbitrary key lengths.
fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut block_key = [0u8; 64];
    if key.len() > 64 {
        let hashed = Sha256::digest(key);
        block_key[..32].copy_from_slice(&hashed);
    } else {
        block_key[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for i in 0..64 {
        ipad[i] ^= block_key[i];
        opad[i] ^= block_key[i];
    }
    let mut inner = Sha256::new();
    inner.update(ipad);
    inner.update(msg);
    let inner_digest = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(opad);
    outer.update(inner_digest);
    outer.finalize().into()
}

fn normalize_header_value(raw: &str) -> String {
    raw.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn sign_s3_request(
    config: &S3Config,
    method: &str,
    full_key: Option<&str>,
    query: &[(&str, &str)],
    payload: &[u8],
    content_type: Option<&str>,
    timestamp: UtcTimestamp,
) -> Result<SignedRequest, Error> {
    let parsed = Url::parse(&config.endpoint)
        .map_err(|err| Error::Message(format!("invalid S3 endpoint URL: {err}")))?;
    let endpoint_host = parsed
        .host_str()
        .ok_or_else(|| Error::Message("S3 endpoint URL missing host".into()))?;
    let base_path = parsed.path().trim_end_matches('/');

    let (host_only, canonical_uri) = if config.force_path_style {
        let uri = match full_key {
            Some(key) => format!(
                "{base_path}/{}/{}",
                uri_encode(&config.bucket, true),
                uri_encode(key, false)
            ),
            None => format!("{base_path}/{}", uri_encode(&config.bucket, true)),
        };
        (endpoint_host.to_string(), uri)
    } else {
        let uri = match full_key {
            Some(key) => format!("{base_path}/{}", uri_encode(key, false)),
            None if base_path.is_empty() => "/".to_string(),
            None => base_path.to_string(),
        };
        (format!("{}.{endpoint_host}", config.bucket), uri)
    };
    let canonical_uri = if canonical_uri.starts_with('/') {
        canonical_uri
    } else {
        format!("/{canonical_uri}")
    };
    let host_header = match parsed.port() {
        Some(port) => format!("{host_only}:{port}"),
        None => host_only,
    };

    let mut encoded_query: Vec<(String, String)> = query
        .iter()
        .map(|(k, v)| (uri_encode(k, true), uri_encode(v, true)))
        .collect();
    encoded_query.sort();
    let canonical_query = encoded_query
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");

    let payload_hash = sha256_hex(payload);

    let mut signing_headers: Vec<(String, String)> = vec![
        ("host".to_string(), normalize_header_value(&host_header)),
        ("x-amz-content-sha256".to_string(), payload_hash.clone()),
        ("x-amz-date".to_string(), timestamp.amz_date.clone()),
    ];
    if let Some(ct) = content_type.map(str::trim).filter(|s| !s.is_empty()) {
        signing_headers.push(("content-type".to_string(), normalize_header_value(ct)));
    }
    signing_headers.sort_by(|a, b| a.0.cmp(&b.0));

    let mut canonical_headers = String::new();
    let mut signed_header_names = Vec::with_capacity(signing_headers.len());
    for (name, value) in &signing_headers {
        let _ = writeln!(canonical_headers, "{name}:{value}");
        signed_header_names.push(name.as_str());
    }
    let signed_headers = signed_header_names.join(";");

    let canonical_request = format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );
    let credential_scope = format!("{}/{}/s3/aws4_request", timestamp.date_stamp, config.region);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        timestamp.amz_date,
        credential_scope,
        sha256_hex(canonical_request.as_bytes())
    );

    let mut k_secret = Vec::with_capacity(4 + config.secret_access_key.len());
    k_secret.extend_from_slice(b"AWS4");
    k_secret.extend_from_slice(config.secret_access_key.as_bytes());
    let k_date = hmac_sha256(&k_secret, timestamp.date_stamp.as_bytes());
    let k_region = hmac_sha256(&k_date, config.region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex_lower(&hmac_sha256(&k_signing, string_to_sign.as_bytes()));

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        config.access_key_id, credential_scope, signed_headers, signature
    );

    let mut wire_headers = Vec::with_capacity(signing_headers.len() + 1);
    for (name, value) in signing_headers {
        // `ureq` derives `Host` directly from the request URL; omit duplicate wire header.
        if name != "host" {
            wire_headers.push((name, value));
        }
    }
    wire_headers.push(("authorization".to_string(), authorization));

    let url = if canonical_query.is_empty() {
        format!("{}://{host_header}{canonical_uri}", parsed.scheme())
    } else {
        format!(
            "{}://{host_header}{canonical_uri}?{canonical_query}",
            parsed.scheme()
        )
    };

    Ok(SignedRequest {
        url,
        headers: wire_headers,
        #[cfg(test)]
        canonical_request,
        #[cfg(test)]
        signature,
    })
}

fn read_error_snippet(resp: &mut ureq::http::Response<ureq::Body>) -> String {
    let mut buf = Vec::new();
    let _ = resp.body_mut().as_reader().take(512).read_to_end(&mut buf);
    let text = String::from_utf8_lossy(&buf).trim().to_string();
    if text.is_empty() {
        String::new()
    } else {
        format!(": {text}")
    }
}

struct ListObjectsV2Page {
    keys: Vec<String>,
    is_truncated: bool,
    next_continuation_token: Option<String>,
}

fn extract_xml_tag<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)?;
    Some(&xml[start..start + end])
}

fn unescape_xml(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(amp) = rest.find('&') {
        out.push_str(&rest[..amp]);
        let after = &rest[amp + 1..];
        if let Some(semi) = after.find(';') {
            let entity = &after[..semi];
            match entity {
                "amp" => out.push('&'),
                "lt" => out.push('<'),
                "gt" => out.push('>'),
                "quot" => out.push('"'),
                "apos" => out.push('\''),
                _ if entity.starts_with("#x") || entity.starts_with("#X") => {
                    if let Ok(code) = u32::from_str_radix(&entity[2..], 16)
                        && let Some(ch) = char::from_u32(code)
                    {
                        out.push(ch);
                    }
                }
                _ if entity.starts_with('#') => {
                    if let Ok(code) = entity[1..].parse::<u32>()
                        && let Some(ch) = char::from_u32(code)
                    {
                        out.push(ch);
                    }
                }
                _ => {
                    out.push('&');
                    out.push_str(entity);
                    out.push(';');
                }
            }
            rest = &after[semi + 1..];
        } else {
            out.push('&');
            rest = after;
        }
    }
    out.push_str(rest);
    out
}

fn parse_list_objects_v2_xml(xml: &str) -> ListObjectsV2Page {
    let mut keys = Vec::new();
    let mut cursor = xml;
    while let Some(start) = cursor.find("<Contents>") {
        let after_open = &cursor[start + "<Contents>".len()..];
        let Some(end) = after_open.find("</Contents>") else {
            break;
        };
        let block = &after_open[..end];
        if let Some(raw_key) = extract_xml_tag(block, "Key") {
            keys.push(unescape_xml(raw_key));
        }
        cursor = &after_open[end + "</Contents>".len()..];
    }
    let is_truncated =
        extract_xml_tag(xml, "IsTruncated").is_some_and(|v| v.trim().eq_ignore_ascii_case("true"));
    let next_continuation_token = extract_xml_tag(xml, "NextContinuationToken")
        .map(unescape_xml)
        .filter(|s| !s.is_empty());
    ListObjectsV2Page {
        keys,
        is_truncated,
        next_continuation_token,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::io::{BufRead, BufReader, Write as _};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard(Vec<(&'static str, Option<String>)>);

    impl EnvGuard {
        fn capture(keys: &[&'static str]) -> Self {
            Self(keys.iter().map(|&k| (k, std::env::var(k).ok())).collect())
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (k, v) in &self.0 {
                match v {
                    Some(val) => unsafe { std::env::set_var(k, val) },
                    None => unsafe { std::env::remove_var(k) },
                }
            }
        }
    }

    #[test]
    fn sigv4_canonical_request_and_signatures_match_reference_vectors() {
        let cfg = S3Config {
            endpoint: "http://s3.example.test:9000".into(),
            bucket: "my-bucket".into(),
            access_key_id: "TESTKEY0000000000001".into(),
            secret_access_key: "test-secret-key-for-sigv4-verification-only".into(),
            region: "us-east-1".into(),
            prefix: None,
            force_path_style: true,
        };
        let ts = UtcTimestamp {
            amz_date: "20260115T123045Z".into(),
            date_stamp: "20260115".into(),
        };
        let req = sign_s3_request(
            &cfg,
            "GET",
            Some("prefix/browser_profiles/demo one.tar.zst"),
            &[("prefix", "a/b"), ("list-type", "2")],
            b"",
            None,
            ts.clone(),
        )
        .unwrap();
        assert_eq!(
            sha256_hex(req.canonical_request.as_bytes()),
            "f0f3bea61ae8d22f4139abea415a80700999ad6ee4fa6cd50c477f2cd20dfd74"
        );
        assert_eq!(
            req.signature,
            "96c3200c6fdc9088fd2ab65b211908c5a4c4d674cab026c95d0f1fa9e76ee2ac"
        );

        let mut cfg_eu = cfg;
        cfg_eu.region = "eu-west-1".into();
        let put_req = sign_s3_request(
            &cfg_eu,
            "PUT",
            Some("media/agt_1/file.png"),
            &[],
            b"hello encrypted browser profile",
            Some("application/octet-stream"),
            ts,
        )
        .unwrap();
        assert_eq!(
            put_req.signature,
            "11ab5be86c16aabdb82082df2dca334777f03c5a9c9e7a3d3b4ae4da3cbbe052"
        );

        // Debug redacts secrets.
        let dbg = format!("{cfg_eu:?}");
        assert!(dbg.contains("[redacted]"));
        assert!(!dbg.contains("TESTKEY0000000000001"));
        assert!(!dbg.contains("test-secret-key"));
    }

    #[test]
    fn from_env_applies_defaults_and_trims_prefix() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = EnvGuard::capture(&[
            S3_ENDPOINT_ENV,
            S3_BUCKET_ENV,
            S3_ACCESS_KEY_ID_ENV,
            S3_SECRET_ACCESS_KEY_ENV,
            S3_REGION_ENV,
            S3_PREFIX_ENV,
            S3_FORCE_PATH_STYLE_ENV,
        ]);
        unsafe {
            std::env::remove_var(S3_BUCKET_ENV);
        }
        assert!(S3Config::from_env().is_none());

        unsafe {
            std::env::set_var(S3_ENDPOINT_ENV, "https://storage.example.com/");
            std::env::set_var(S3_BUCKET_ENV, "profiles-bucket");
            std::env::set_var(S3_ACCESS_KEY_ID_ENV, "key-1");
            std::env::set_var(S3_SECRET_ACCESS_KEY_ENV, "secret-1");
            std::env::remove_var(S3_REGION_ENV);
            std::env::set_var(S3_PREFIX_ENV, "/prod/tenant/");
            std::env::remove_var(S3_FORCE_PATH_STYLE_ENV);
        }
        let cfg = S3Config::from_env().expect("S3Config loaded");
        assert_eq!(cfg.endpoint, "https://storage.example.com");
        assert_eq!(cfg.region, "us-east-1");
        assert_eq!(cfg.prefix.as_deref(), Some("prod/tenant"));
        assert!(cfg.force_path_style);
    }

    #[derive(Default)]
    struct MockS3State {
        objects: BTreeMap<String, (Vec<u8>, String)>,
        auth_headers: Vec<String>,
    }

    fn spawn_mock_s3_server(bucket: &str) -> (String, Arc<Mutex<MockS3State>>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock S3");
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(MockS3State::default()));
        let server_state = Arc::clone(&state);
        let bucket_prefix = format!("/{bucket}/");
        let bucket_root = format!("/{bucket}");

        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut req_line = String::new();
                if reader.read_line(&mut req_line).unwrap_or(0) == 0 {
                    continue;
                }
                let mut parts = req_line.split_whitespace();
                let method = parts.next().unwrap_or("").to_string();
                let target = parts.next().unwrap_or("/").to_string();
                let mut content_length = 0usize;
                let mut content_type = "application/octet-stream".to_string();
                let mut auth_header = String::new();
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0
                        || line == "\r\n"
                        || line == "\n"
                    {
                        break;
                    }
                    if let Some((k, v)) = line.split_once(':') {
                        let k_low = k.trim().to_ascii_lowercase();
                        let val = v.trim().to_string();
                        match k_low.as_str() {
                            "content-length" => content_length = val.parse().unwrap_or(0),
                            "content-type" => content_type = val,
                            "authorization" => auth_header = val,
                            _ => {}
                        }
                    }
                }
                let mut body = vec![0u8; content_length];
                if content_length > 0 {
                    let _ = reader.read_exact(&mut body);
                }

                let (path, query) = target
                    .split_once('?')
                    .map(|(p, q)| (p, Some(q)))
                    .unwrap_or((target.as_str(), None));
                let mut st = server_state.lock().unwrap();
                st.auth_headers.push(auth_header);

                let mut stream = stream;
                if method == "GET" && path == bucket_root && query.is_some() {
                    let mut prefix = String::new();
                    let mut token_idx = 0usize;
                    for pair in query.unwrap_or("").split('&') {
                        if let Some((k, v)) = pair.split_once('=') {
                            let decoded = percent_decode_simple(v);
                            if k == "prefix" {
                                prefix = decoded;
                            } else if k == "continuation-token" {
                                token_idx = decoded.parse().unwrap_or(0);
                            }
                        }
                    }
                    let matching: Vec<String> = st
                        .objects
                        .keys()
                        .filter(|k| k.starts_with(&prefix))
                        .cloned()
                        .collect();
                    // Paginate 1 key per page to exercise continuation tokens.
                    let page_keys = matching.get(token_idx..token_idx + 1).unwrap_or(&[]);
                    let truncated = token_idx + 1 < matching.len();
                    let mut xml = String::from(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult>",
                    );
                    let _ = write!(
                        xml,
                        "<IsTruncated>{}</IsTruncated>",
                        if truncated { "true" } else { "false" }
                    );
                    if truncated {
                        let _ = write!(
                            xml,
                            "<NextContinuationToken>{}</NextContinuationToken>",
                            token_idx + 1
                        );
                    }
                    for k in page_keys {
                        let _ = write!(xml, "<Contents><Key>{k}</Key></Contents>");
                    }
                    xml.push_str("</ListBucketResult>");
                    let _ = write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        xml.len(),
                        xml
                    );
                    continue;
                }

                let key = path
                    .strip_prefix(&bucket_prefix)
                    .map(percent_decode_simple)
                    .unwrap_or_default();
                match method.as_str() {
                    "PUT" => {
                        st.objects.insert(key, (body, content_type));
                        let _ = write!(
                            stream,
                            "HTTP/1.1 200 OK\r\nETag: \"mock-etag\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        );
                    }
                    "GET" => {
                        if let Some((data, ct)) = st.objects.get(&key) {
                            let _ = write!(
                                stream,
                                "HTTP/1.1 200 OK\r\nContent-Type: {ct}\r\nETag: \"mock-etag\"\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                data.len()
                            );
                            let _ = stream.write_all(data);
                        } else {
                            let _ = write!(
                                stream,
                                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            );
                        }
                    }
                    "HEAD" => {
                        if let Some((data, ct)) = st.objects.get(&key) {
                            let _ = write!(
                                stream,
                                "HTTP/1.1 200 OK\r\nContent-Type: {ct}\r\nETag: \"mock-etag\"\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                data.len()
                            );
                        } else {
                            let _ = write!(
                                stream,
                                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            );
                        }
                    }
                    "DELETE" => {
                        let status = if st.objects.remove(&key).is_some() {
                            "204 No Content"
                        } else {
                            "404 Not Found"
                        };
                        let _ = write!(
                            stream,
                            "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        );
                    }
                    _ => {
                        let _ = write!(
                            stream,
                            "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        );
                    }
                }
            }
        });
        (format!("http://{addr}"), state)
    }

    fn percent_decode_simple(s: &str) -> String {
        let bytes = s.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%'
                && i + 2 < bytes.len()
                && let Ok(hex_str) = std::str::from_utf8(&bytes[i + 1..i + 3])
                && let Ok(val) = u8::from_str_radix(hex_str, 16)
            {
                out.push(val);
                i += 3;
                continue;
            }
            out.push(bytes[i]);
            i += 1;
        }
        String::from_utf8_lossy(&out).into_owned()
    }

    #[test]
    fn mock_s3_encrypted_roundtrip_hides_plaintext_and_fails_closed_without_master_key() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = EnvGuard::capture(&[
            crate::stateless::MASTER_KEY_ENV,
            crate::stateless::CAPABILITY_KEY_ENV,
        ]);
        unsafe {
            std::env::remove_var(crate::stateless::MASTER_KEY_ENV);
            std::env::remove_var(crate::stateless::CAPABILITY_KEY_ENV);
        }

        let (endpoint, state) = spawn_mock_s3_server("test-bucket");
        let store = S3Store::new(S3Config {
            endpoint,
            bucket: "test-bucket".into(),
            access_key_id: "MOCKKEY000001".into(),
            secret_access_key: "mock-secret-key-value".into(),
            region: "us-east-1".into(),
            prefix: Some("env-a".into()),
            force_path_style: true,
        })
        .unwrap();

        // Fail closed when ANYAGENTS_MASTER_KEY is missing.
        let plaintext = b"SENSITIVE_COOKIE_SESSION_TOKEN_987654321";
        assert!(
            store
                .put_encrypted_object(
                    "browser_profiles/alpha.tar.zst",
                    plaintext,
                    "application/zstd",
                    None,
                )
                .is_err()
        );
        assert!(
            store
                .get_encrypted_object("browser_profiles/alpha.tar.zst", None)
                .is_err()
        );

        let key = MasterKey::from_secret_str("master-key-for-s3-roundtrip-unit-test").unwrap();
        store
            .put_encrypted_object(
                "browser_profiles/alpha.tar.zst",
                plaintext,
                "application/zstd",
                Some(&key),
            )
            .unwrap();
        store
            .put_encrypted_object(
                "browser_profiles/beta.tar.zst",
                b"second-profile-payload",
                "application/zstd",
                Some(&key),
            )
            .unwrap();

        // Stored S3 bytes MUST NOT contain plaintext.
        {
            let st = state.lock().unwrap();
            let (raw_stored, _) = st
                .objects
                .get("env-a/browser_profiles/alpha.tar.zst")
                .expect("stored under prefix");
            assert!(raw_stored.starts_with(b"AAENC1"));
            assert!(
                !raw_stored.windows(plaintext.len()).any(|w| w == plaintext),
                "S3 object must be encrypted at rest"
            );
            assert!(
                st.auth_headers
                    .iter()
                    .all(|h| h.starts_with("AWS4-HMAC-SHA256 Credential=MOCKKEY000001/"))
            );
        }

        // Encrypted roundtrip decrypts cleanly; wrong key fails closed.
        let decrypted = store
            .get_encrypted_object("browser_profiles/alpha.tar.zst", Some(&key))
            .unwrap()
            .expect("exists");
        assert_eq!(decrypted, plaintext);
        let wrong_key = MasterKey::from_secret_str("different-master-key-fails-closed").unwrap();
        assert!(
            store
                .get_encrypted_object("browser_profiles/alpha.tar.zst", Some(&wrong_key))
                .is_err()
        );

        // HEAD, paginated LIST (stripping configured prefix), DELETE, and 404.
        let meta = store
            .head_object("browser_profiles/alpha.tar.zst")
            .unwrap()
            .expect("head exists");
        assert!(meta.size_bytes > plaintext.len() as u64);
        assert_eq!(meta.etag.as_deref(), Some("mock-etag"));

        let listed = store.list_prefix("browser_profiles/").unwrap();
        assert_eq!(
            listed,
            vec![
                "browser_profiles/alpha.tar.zst".to_string(),
                "browser_profiles/beta.tar.zst".to_string(),
            ]
        );

        assert!(
            store
                .delete_object("browser_profiles/alpha.tar.zst")
                .unwrap()
        );
        assert!(
            !store
                .delete_object("browser_profiles/alpha.tar.zst")
                .unwrap()
        );
        assert!(
            store
                .get_encrypted_object("browser_profiles/alpha.tar.zst", Some(&key))
                .unwrap()
                .is_none()
        );
    }
}
