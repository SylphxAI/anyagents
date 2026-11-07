//! Immutable identity of the image serving this process, not the installed path.

use std::fs::File;
use std::io;
use std::sync::OnceLock;

use serde::Serialize;
use sha2::{Digest, Sha256};

static PROCESS_STARTED_AT_MS: OnceLock<i64> = OnceLock::new();

/// Called before command parsing, without hashing every short-lived CLI call.
pub(crate) fn mark_process_start() -> i64 {
    *PROCESS_STARTED_AT_MS.get_or_init(anyagents_core::now_ms)
}

#[derive(Clone, Serialize)]
pub(crate) struct ServingIdentity {
    pub(crate) version: &'static str,
    pub(crate) source_sha: &'static str,
    pub(crate) binary_sha256: String,
    pub(crate) process_started_at_ms: i64,
    pub(crate) pid: u32,
}

impl ServingIdentity {
    /// Capture once, before binding either server's listener. On Linux,
    /// /proc/self/exe keeps referring to this image after an atomic install.
    /// A hash read failure prevents readiness rather than inventing identity.
    pub(crate) fn capture() -> io::Result<Self> {
        static IDENTITY: OnceLock<Result<ServingIdentity, (io::ErrorKind, String)>> =
            OnceLock::new();
        match IDENTITY
            .get_or_init(|| Self::read_image().map_err(|error| (error.kind(), error.to_string())))
        {
            Ok(identity) => Ok(identity.clone()),
            Err((kind, message)) => Err(io::Error::new(*kind, message.clone())),
        }
    }

    fn read_image() -> io::Result<Self> {
        let process_started_at_ms = mark_process_start();
        #[cfg(target_os = "linux")]
        let mut image = File::open("/proc/self/exe")?;
        #[cfg(not(target_os = "linux"))]
        let mut image = File::open(std::env::current_exe()?)?;
        let mut hash = Sha256::new();
        io::copy(&mut image, &mut hash)?;
        Ok(Self {
            version: env!("CARGO_PKG_VERSION"),
            source_sha: env!("ANYAGENTS_SOURCE_SHA"),
            binary_sha256: format!("{:x}", hash.finalize()),
            process_started_at_ms,
            pid: std::process::id(),
        })
    }
}
