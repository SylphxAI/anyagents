//! Rebuildable source cursors. Native append reuse checks a bounded boundary,
//! while host maintenance periodically rebuilds the entire source. Returned
//! coordinates always retain independent raw/window SHA verification.
use crate::raw_segment::{JsonlLine, RawCoordinate, RawSource, read_jsonl_line};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::{self, BufRead},
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct FileStamp {
    pub len: u64,
    device: u64,
    inode: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}
impl FileStamp {
    pub fn read(file: &File) -> io::Result<Self> {
        Ok(Self::from_metadata(&file.metadata()?))
    }
    pub fn from_metadata(m: &fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;
        Self {
            len: m.len(),
            device: m.dev(),
            inode: m.ino(),
            modified: (m.mtime(), m.mtime_nsec()),
            changed: (m.ctime(), m.ctime_nsec()),
        }
    }
    pub fn same_file(&self, other: &Self) -> bool {
        self.device == other.device && self.inode == other.inode
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SourceCursor {
    pub stamp: FileStamp,
    pub policy: String,
    pub end: u64,
    pub next_seq: u64,
    #[serde(default)]
    pub boundary_sha256: String,
    #[serde(default)]
    pub full_verified_ms: i64,
    #[serde(default)]
    pub verification_attempt_ms: i64,
}
pub(crate) struct JsonlRefresh {
    pub records: Vec<RawCoordinate>,
    pub end: u64,
    pub next_seq: u64,
    pub boundary_sha256: String,
    pub prefix_verified_bytes: u64,
    pub boundary_capture_bytes: u64,
    pub suffix_scanned_bytes: u64,
    pub replace: bool,
}
/// Read one stable-length snapshot. Incomplete tail bytes never enter the
/// committed frontier. Replacement, shrink or a changed 64KiB boundary forces
/// a full rebuild; older edits are found by periodic full verification.
/// Every record is hashed and validated from the one buffered pass over it.
pub(crate) fn scan_jsonl(
    source: &mut RawSource,
    previous: Option<&SourceCursor>,
    force_full: bool,
) -> io::Result<JsonlRefresh> {
    let stamp = source.stamp().clone();
    let mut start = 0;
    let mut seq = 1;
    let mut verified = 0;
    let mut replace = true;
    if !force_full
        && let Some(old) = previous
        && old.stamp.same_file(&stamp)
        && stamp.len >= old.stamp.len
        && old.end <= stamp.len
    {
        let (boundary, length) = boundary_hash(source, old.end)?;
        verified = length;
        if boundary == old.boundary_sha256 {
            start = old.end;
            seq = old.next_seq;
            replace = false;
        }
    }
    let segment = source.path().to_owned();
    let mut reader = source.range(start, stamp.len - start);
    let mut records = Vec::new();
    let mut at = start;
    let mut scanned = 0;
    while let Some(line) = read_jsonl_line(&mut reader)? {
        match line {
            JsonlLine::Complete { length, sha256 } => {
                records.push(RawCoordinate {
                    segment: segment.clone(),
                    offset: at,
                    length,
                    sha256,
                    seq,
                });
                at += length;
                scanned += length;
                seq += 1;
            }
            JsonlLine::Partial { length } => scanned += length,
        }
    }
    let (boundary_sha256, boundary_capture_bytes) = boundary_hash(source, at)?;
    Ok(JsonlRefresh {
        records,
        end: at,
        next_seq: seq,
        boundary_sha256,
        boundary_capture_bytes,
        prefix_verified_bytes: verified,
        suffix_scanned_bytes: scanned,
        replace,
    })
}

/// Two boundary reads per incremental refresh (old and new frontier), never
/// proportional to prior history. A tail fragment remains outside the frontier.
pub(crate) const BOUNDARY_BYTES: u64 = 64 * 1024;
fn boundary_hash(source: &mut RawSource, end: u64) -> io::Result<(String, u64)> {
    let length = end.min(BOUNDARY_BYTES);
    let mut reader = source.range(end - length, length);
    let mut hash = Sha256::new();
    let mut read = 0;
    while read < length {
        let bytes = reader.fill_buf()?;
        if bytes.is_empty() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        hash.update(bytes);
        read += bytes.len() as u64;
        let n = bytes.len();
        reader.consume(n);
    }
    Ok((format!("{:x}", hash.finalize()), length))
}
