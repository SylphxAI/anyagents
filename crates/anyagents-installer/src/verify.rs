//! What a download must hash to, and the hashing itself.

use std::io::Write;
use std::path::Path;

use base64::Engine as _;
use sha2::{Digest as _, Sha256, Sha512};

use crate::InstallError;

/// A publisher-supplied digest an artifact is checked against.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Digest {
    /// Lowercase hex SHA-256.
    Sha256(String),
    /// Raw SHA-512 bytes, as npm's `dist.integrity` carries them (base64).
    Sha512(Vec<u8>),
}

impl Digest {
    /// A SHA-256 from 64 hex characters, or `None` if it is not one.
    pub fn sha256_hex(hex: &str) -> Option<Self> {
        let hex = hex.trim().to_ascii_lowercase();
        (hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit())).then_some(Self::Sha256(hex))
    }

    /// The strongest digest in an npm Subresource Integrity string. Only
    /// `sha512-` is accepted: a `sha1` shasum alone is not a verifiable source.
    pub fn from_integrity(integrity: &str) -> Option<Self> {
        integrity
            .split_whitespace()
            .filter_map(|part| part.strip_prefix("sha512-"))
            .find_map(|b64| {
                base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .ok()
                    .filter(|bytes| bytes.len() == 64)
            })
            .map(Self::Sha512)
    }

    /// The text recorded in a receipt: `sha256:<hex>` or `sha512-<base64>`.
    pub fn label(&self) -> String {
        match self {
            Self::Sha256(hex) => format!("sha256:{hex}"),
            Self::Sha512(bytes) => format!(
                "sha512-{}",
                base64::engine::general_purpose::STANDARD.encode(bytes)
            ),
        }
    }

    fn hasher(&self) -> Hasher {
        match self {
            Self::Sha256(_) => Hasher::Sha256(Sha256::new()),
            Self::Sha512(_) => Hasher::Sha512(Sha512::new()),
        }
    }
}

enum Hasher {
    Sha256(Sha256),
    Sha512(Sha512),
}

impl Hasher {
    fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Sha256(h) => h.update(bytes),
            Self::Sha512(h) => h.update(bytes),
        }
    }

    fn finish(self) -> Digest {
        match self {
            Self::Sha256(h) => Digest::Sha256(to_hex(&h.finalize())),
            Self::Sha512(h) => Digest::Sha512(h.finalize().to_vec()),
        }
    }
}

/// Writes through to `inner` and hashes everything written, with the algorithm
/// of the digest it will be checked against.
pub struct HashingWriter<W> {
    inner: W,
    hasher: Hasher,
}

impl<W: Write> HashingWriter<W> {
    pub fn new(inner: W, expected: &Digest) -> Self {
        Self {
            inner,
            hasher: expected.hasher(),
        }
    }

    /// Flush and return the digest of everything written.
    pub fn finish(mut self) -> std::io::Result<Digest> {
        self.inner.flush()?;
        Ok(self.hasher.finish())
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Refuse anything but an exact match.
pub fn check(url: &str, expected: &Digest, actual: &Digest) -> Result<(), InstallError> {
    if expected == actual {
        Ok(())
    } else {
        Err(InstallError::ChecksumMismatch {
            url: url.to_string(),
            expected: expected.label(),
            actual: actual.label(),
        })
    }
}

/// The SHA-256 for `name` in a `sha256sum`-format checksum file
/// (`<hex>  <name>` or `<hex> *<name>`, one per line).
pub fn checksum_for(file: &str, name: &str) -> Option<Digest> {
    file.lines().find_map(|line| {
        let (hex, rest) = line.trim().split_once(char::is_whitespace)?;
        let listed = rest.trim_start().trim_start_matches('*');
        (listed == name).then(|| Digest::sha256_hex(hex)).flatten()
    })
}

pub(crate) fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The SHA-256 of one file, as lowercase hex.
pub fn sha256_file(path: &Path) -> std::io::Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    std::io::copy(&mut file, &mut hasher)?;
    Ok(to_hex(&hasher.finalize()))
}

/// A digest of a whole directory tree: every entry's relative path, kind and
/// content (or link target), in sorted order. Used to give an install that
/// predates receipts a digest it can be checked against later.
pub fn sha256_tree(root: &Path) -> std::io::Result<String> {
    let mut entries = Vec::new();
    collect(root, Path::new(""), &mut entries)?;
    entries.sort();
    let mut hasher = Sha256::new();
    for relative in entries {
        let path = root.join(&relative);
        let meta = std::fs::symlink_metadata(&path)?;
        hasher.update(relative.to_string_lossy().as_bytes());
        hasher.update([0]);
        if meta.file_type().is_symlink() {
            hasher.update(b"L");
            hasher.update(std::fs::read_link(&path)?.to_string_lossy().as_bytes());
        } else if meta.is_dir() {
            hasher.update(b"D");
        } else {
            hasher.update(b"F");
            let mut file = std::fs::File::open(&path)?;
            std::io::copy(&mut file, &mut hasher)?;
        }
        hasher.update([0]);
    }
    Ok(to_hex(&hasher.finalize()))
}

fn collect(root: &Path, relative: &Path, out: &mut Vec<std::path::PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(root.join(relative))? {
        let entry = entry?;
        let child = relative.join(entry.file_name());
        let kind = entry.file_type()?;
        out.push(child.clone());
        if kind.is_dir() {
            collect(root, &child, out)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn npm_integrity_takes_sha512_and_refuses_sha1_alone() {
        let bytes = Sha512::digest(b"x");
        let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
        let integrity = format!("sha1-AAAA sha512-{b64}");
        assert_eq!(
            Digest::from_integrity(&integrity),
            Some(Digest::Sha512(bytes.to_vec()))
        );
        assert_eq!(Digest::from_integrity("sha1-AAAA"), None);
    }

    #[test]
    fn a_checksum_file_names_each_asset_once() {
        let hex = "a".repeat(64);
        let other = "b".repeat(64);
        let file = format!("{other}  tool-linux.zip\n{hex} *tool-mac.zip\n");
        assert_eq!(
            checksum_for(&file, "tool-mac.zip"),
            Some(Digest::Sha256(hex))
        );
        assert_eq!(checksum_for(&file, "tool-mac"), None);
    }

    #[test]
    fn hashing_while_writing_matches_hashing_after() {
        let expected = Digest::Sha256(to_hex(&Sha256::digest(b"payload")));
        let mut out = Vec::new();
        let mut writer = HashingWriter::new(&mut out, &expected);
        writer.write_all(b"payload").unwrap();
        assert_eq!(writer.finish().unwrap(), expected);
        assert!(check("u", &expected, &Digest::Sha256("0".repeat(64))).is_err());
    }

    #[test]
    fn a_tree_digest_changes_when_any_file_changes() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("a/b")).unwrap();
        std::fs::write(dir.path().join("a/b/f"), "1").unwrap();
        let first = sha256_tree(dir.path()).unwrap();
        assert_eq!(first, sha256_tree(dir.path()).unwrap());
        std::fs::write(dir.path().join("a/b/f"), "2").unwrap();
        assert_ne!(first, sha256_tree(dir.path()).unwrap());
    }
}
