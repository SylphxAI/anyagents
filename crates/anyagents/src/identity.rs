//! Immutable image provenance with a once-only hash off the readiness path.

use std::fs::File;
use std::io;
use std::sync::{Arc, Mutex, OnceLock};

use sha2::{Digest, Sha256};

type ImageHasher = Box<dyn FnOnce() -> io::Result<String> + Send>;

struct ImageHash {
    result: OnceLock<Result<String, String>>,
    pending: Mutex<Option<ImageHasher>>,
}

#[derive(Clone)]
pub(crate) struct ServingIdentity {
    version: &'static str,
    source_sha: &'static str,
    hash: Arc<ImageHash>,
}

impl ServingIdentity {
    /// Metadata is immediate. Neither filesystem reads nor hashing delay bind.
    pub(crate) fn capture() -> Self {
        static IDENTITY: OnceLock<ServingIdentity> = OnceLock::new();
        IDENTITY
            .get_or_init(|| Self::with_hasher(Self::read_image))
            .clone()
    }

    pub(crate) fn with_hasher(
        hasher: impl FnOnce() -> io::Result<String> + Send + 'static,
    ) -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION"),
            source_sha: env!("ANYAGENTS_SOURCE_SHA"),
            hash: Arc::new(ImageHash {
                result: OnceLock::new(),
                pending: Mutex::new(Some(Box::new(hasher))),
            }),
        }
    }

    /// Called only after binding. Clones and repeated calls share one worker;
    /// a slow disk or hash failure never prevents the server from answering.
    pub(crate) fn start_hash(&self) {
        let Some(hasher) = self
            .hash
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        else {
            return;
        };
        let hash = Arc::clone(&self.hash);
        if let Err(error) = std::thread::Builder::new()
            .name("serving-image-hash".into())
            .spawn(move || {
                let result = hasher().map_err(|error| error.to_string());
                if let Err(error) = &result {
                    eprintln!("warning: serving image hash failed: {error}");
                }
                let _ = hash.result.set(result);
            })
        {
            let _ = self.hash.result.set(Err(error.to_string()));
        }
    }

    pub(crate) fn status(&self) -> serde_json::Value {
        let (value, state) = match self.hash.result.get() {
            None => (None, "computing"),
            Some(Ok(value)) => (Some(value.as_str()), "ready"),
            Some(Err(_)) => (None, "failed"),
        };
        serde_json::json!({
            "ok": true,
            "version": self.version,
            "source_sha": self.source_sha,
            "binary_sha256": value,
            "binary_sha256_state": state,
        })
    }

    fn read_image() -> io::Result<String> {
        // Linux keeps this reference to the running image after atomic install.
        #[cfg(target_os = "linux")]
        let mut image = File::open("/proc/self/exe")?;
        #[cfg(not(target_os = "linux"))]
        let mut image = File::open(std::env::current_exe()?)?;
        let mut hash = Sha256::new();
        io::copy(&mut image, &mut hash)?;
        Ok(format!("{:x}", hash.finalize()))
    }
}
