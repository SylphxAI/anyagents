//! Pinned model files and the ONNX Runtime library: where each comes from,
//! what it must hash to, and the local copy the service loads. Nothing is
//! loaded that was not verified against its pinned SHA-256; a file that does
//! not match is refused and removed, never used.
use anyagents_installer::fetch::{Fetch, HttpFetch};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

/// One file of a pinned model revision.
#[derive(Clone, Copy, Debug)]
pub struct PinnedFile {
    /// Path inside the repository revision.
    pub path: &'static str,
    pub size: u64,
    pub sha256: &'static str,
}

/// How an encoder's output becomes one vector or one score.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Head {
    /// Embedder: the first token's hidden state, L2-normalised.
    Cls,
    /// Embedder: the attention-masked mean of the hidden states, L2-normalised.
    Mean,
    /// Cross-encoder: the single relevance logit.
    Logit,
}

/// A model revision pinned by repository commit and per-file digest.
#[derive(Clone, Copy, Debug)]
pub struct ModelPin {
    /// Stable name recorded with every vector built by this model.
    pub name: &'static str,
    pub repo: &'static str,
    pub revision: &'static str,
    pub onnx: PinnedFile,
    pub tokenizer: PinnedFile,
    pub head: Head,
    /// Tokens per input, special tokens included.
    pub max_tokens: usize,
    pub query_prefix: &'static str,
    pub passage_prefix: &'static str,
    pub dims: usize,
}

impl ModelPin {
    fn files(&self) -> [PinnedFile; 2] {
        [self.onnx, self.tokenizer]
    }
    pub fn url(&self, file: &PinnedFile) -> String {
        format!(
            "https://huggingface.co/{}/resolve/{}/{}",
            self.repo, self.revision, file.path
        )
    }
    /// The local directory of this revision under `models`.
    pub fn dir(&self, models: &Path) -> PathBuf {
        models.join(self.name).join(self.revision)
    }
    pub fn onnx_path(&self, models: &Path) -> PathBuf {
        self.dir(models).join(self.onnx.path)
    }
    pub fn tokenizer_path(&self, models: &Path) -> PathBuf {
        self.dir(models).join(self.tokenizer.path)
    }
}

/// The ONNX Runtime shared library for one platform, inside a pinned
/// official release archive.
#[derive(Clone, Copy, Debug)]
pub struct RuntimePin {
    pub version: &'static str,
    pub url: &'static str,
    pub sha256: &'static str,
    /// Path of the shared library inside the archive.
    pub library: &'static str,
}

/// The platform's pinned runtime, if one is published for it.
pub fn runtime_pin() -> Option<RuntimePin> {
    crate::pins::RUNTIMES
        .iter()
        .find(|(os, arch, _)| *os == std::env::consts::OS && *arch == std::env::consts::ARCH)
        .map(|(_, _, pin)| *pin)
}

impl RuntimePin {
    pub fn dir(&self, models: &Path) -> PathBuf {
        models.join("onnxruntime").join(self.version)
    }
    pub fn library_path(&self, models: &Path) -> PathBuf {
        self.dir(models).join(
            Path::new(self.library)
                .file_name()
                .unwrap_or_else(|| self.library.as_ref()),
        )
    }
}

fn sha256_file(path: &Path) -> io::Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hash.update(&buf[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

/// Whether `path` holds exactly the pinned bytes.
pub fn verified(path: &Path, size: u64, sha256: &str) -> io::Result<bool> {
    match fs::metadata(path) {
        Ok(meta) if meta.len() == size => Ok(sha256_file(path)? == sha256),
        Ok(_) => Ok(false),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// Download `url` to `dest` through a temporary file, publishing it only when
/// it has the pinned size and digest.
fn fetch_verified(
    fetch: &dyn Fetch,
    url: &str,
    dest: &Path,
    size: Option<u64>,
    sha256: &str,
) -> io::Result<()> {
    let parent = dest.parent().ok_or_else(|| io::Error::other("no parent"))?;
    fs::create_dir_all(parent)?;
    let temp = parent.join(format!(
        ".{}.part",
        dest.file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("download")
    ));
    let result = (|| {
        let mut out = fs::File::create(&temp)?;
        let written = fetch
            .download(url, &mut out)
            .map_err(|e| io::Error::other(e.to_string()))?;
        out.flush()?;
        out.sync_all()?;
        if size.is_some_and(|size| size != written) {
            return Err(io::Error::other(format!(
                "{url}: {written} bytes, pinned {}",
                size.unwrap_or_default()
            )));
        }
        let actual = sha256_file(&temp)?;
        if actual != sha256 {
            return Err(io::Error::other(format!(
                "{url}: sha256 {actual}, pinned {sha256}"
            )));
        }
        fs::rename(&temp, dest)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

/// Make every pinned file present and verified under `models`, downloading
/// what is missing. Returns what was downloaded.
pub fn fetch(models: &Path, pins: &[ModelPin]) -> io::Result<Vec<String>> {
    let fetch = HttpFetch::new();
    let mut done = Vec::new();
    for pin in pins {
        for file in pin.files() {
            let dest = pin.dir(models).join(file.path);
            if !verified(&dest, file.size, file.sha256)? {
                fetch_verified(&fetch, &pin.url(&file), &dest, Some(file.size), file.sha256)?;
                done.push(pin.url(&file));
            }
        }
    }
    let runtime = runtime_pin().ok_or_else(|| {
        io::Error::other(format!(
            "no pinned ONNX Runtime for {}-{}",
            std::env::consts::OS,
            std::env::consts::ARCH
        ))
    })?;
    let library = runtime.library_path(models);
    let receipt = runtime.dir(models).join("release.sha256");
    let archive_ok =
        fs::read_to_string(&receipt).is_ok_and(|r| r.trim() == runtime.sha256) && library.is_file();
    if !archive_ok {
        let dir = runtime.dir(models);
        fs::create_dir_all(&dir)?;
        let archive = dir.join("release.tgz");
        fetch_verified(&fetch, runtime.url, &archive, None, runtime.sha256)?;
        extract_library(&archive, runtime.library, &library)?;
        fs::remove_file(&archive)?;
        fs::write(&receipt, runtime.sha256)?;
        done.push(runtime.url.to_owned());
    }
    Ok(done)
}

/// Copy one member of a verified release archive out, without following or
/// creating links and without writing anywhere else.
fn extract_library(archive: &Path, member: &str, dest: &Path) -> io::Result<()> {
    let file = fs::File::open(archive)?;
    let mut tar = tar::Archive::new(flate2::read::GzDecoder::new(file));
    for entry in tar.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        // Some release archives name members `./<dir>/...`.
        if path.strip_prefix(".").unwrap_or(&path) == Path::new(member) {
            if !entry.header().entry_type().is_file() {
                return Err(io::Error::other(format!(
                    "{member} is not a regular file in the archive"
                )));
            }
            let temp = dest.with_extension("part");
            let mut out = fs::File::create(&temp)?;
            io::copy(&mut entry, &mut out)?;
            out.sync_all()?;
            return fs::rename(temp, dest);
        }
    }
    Err(io::Error::other(format!(
        "{member} is not in the pinned archive"
    )))
}

/// Every pinned file verified in place, or the first that is not.
pub fn check(models: &Path, pins: &[ModelPin]) -> io::Result<()> {
    for pin in pins {
        for file in pin.files() {
            let path = pin.dir(models).join(file.path);
            if !verified(&path, file.size, file.sha256)? {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "{} is missing or does not match its pin; run `anyagents memory-service fetch`",
                        path.display()
                    ),
                ));
            }
        }
    }
    let runtime = runtime_pin()
        .ok_or_else(|| io::Error::other("no pinned ONNX Runtime for this platform"))?;
    let receipt = runtime.dir(models).join("release.sha256");
    if fs::read_to_string(&receipt).is_ok_and(|r| r.trim() == runtime.sha256)
        && runtime.library_path(models).is_file()
    {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "ONNX Runtime is missing; run `anyagents memory-service fetch`",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_file_is_used_only_with_its_pinned_size_and_digest() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        fs::write(&path, b"abc").unwrap();
        let sha = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        assert!(verified(&path, 3, sha).unwrap());
        assert!(!verified(&path, 4, sha).unwrap());
        assert!(!verified(&path, 3, &sha.replace('b', "c")).unwrap());
        assert!(!verified(&dir.path().join("missing"), 3, sha).unwrap());
    }

    #[test]
    fn every_pin_is_complete() {
        for pin in crate::pins::EMBEDDERS.iter().chain(crate::pins::RERANKERS) {
            assert!(
                pin.revision.len() == 40,
                "{}: commit, not a branch",
                pin.name
            );
            for file in pin.files() {
                assert_eq!(file.sha256.len(), 64, "{}", file.path);
                assert!(file.size > 0, "{}", file.path);
            }
        }
        for (_, _, runtime) in crate::pins::RUNTIMES {
            assert_eq!(runtime.sha256.len(), 64);
            assert!(
                runtime
                    .url
                    .starts_with("https://github.com/microsoft/onnxruntime/releases/download/")
            );
        }
    }
}
