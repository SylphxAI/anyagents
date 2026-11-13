//! The one way bytes come in from a registry: an HTTPS (or loopback, in tests)
//! `GET`. Metadata is read into memory under a size cap; artifacts are streamed
//! to disk so a large release never sits in memory.

use std::io::{Read, Write};
use std::time::Duration;

use crate::InstallError;

/// Metadata documents (registry manifests, checksum files) larger than this are refused.
pub const METADATA_LIMIT: u64 = 32 * 1024 * 1024;

/// Reads from a registry. Tests use [`HttpFetch`] against a loopback server,
/// so the production path is the tested path.
pub trait Fetch: Send + Sync {
    /// The whole body of `url`, refused if larger than `limit` bytes.
    fn get(&self, url: &str, limit: u64) -> Result<Vec<u8>, InstallError>;
    /// Stream the body of `url` into `out`, returning the number of bytes.
    fn download(&self, url: &str, out: &mut dyn Write) -> Result<u64, InstallError>;
}

/// Plain HTTP(S) through `ureq`, with a neutral user agent and bounded timeouts.
pub struct HttpFetch {
    agent: ureq::Agent,
}

impl Default for HttpFetch {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpFetch {
    pub fn new() -> Self {
        let config = ureq::Agent::config_builder()
            .timeout_connect(Some(Duration::from_secs(30)))
            .timeout_recv_response(Some(Duration::from_secs(60)))
            .timeout_global(Some(Duration::from_secs(30 * 60)))
            .user_agent("anyagents-installer")
            .build();
        Self {
            agent: config.into(),
        }
    }
}

fn fetch_error(url: &str, err: impl std::fmt::Display) -> InstallError {
    InstallError::Fetch {
        url: url.to_string(),
        reason: err.to_string(),
    }
}

impl Fetch for HttpFetch {
    fn get(&self, url: &str, limit: u64) -> Result<Vec<u8>, InstallError> {
        let mut response = self
            .agent
            .get(url)
            .header("Accept", "application/json, text/plain, */*")
            .call()
            .map_err(|err| fetch_error(url, err))?;
        response
            .body_mut()
            .with_config()
            .limit(limit)
            .read_to_vec()
            .map_err(|err| fetch_error(url, err))
    }

    fn download(&self, url: &str, out: &mut dyn Write) -> Result<u64, InstallError> {
        let mut response = self
            .agent
            .get(url)
            .call()
            .map_err(|err| fetch_error(url, err))?;
        let mut reader = response.body_mut().with_config().limit(u64::MAX).reader();
        let mut buf = vec![0u8; 256 * 1024];
        let mut total = 0u64;
        loop {
            let n = reader.read(&mut buf).map_err(|err| fetch_error(url, err))?;
            if n == 0 {
                break;
            }
            out.write_all(&buf[..n])
                .map_err(|err| InstallError::Io(err.to_string()))?;
            total += n as u64;
        }
        Ok(total)
    }
}
