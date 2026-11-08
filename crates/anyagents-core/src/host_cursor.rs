//! Rebuildable source cursors. Native append reuse checks a bounded boundary,
//! while host maintenance periodically rebuilds the entire source. Returned
//! coordinates always retain independent raw/window SHA verification.
use crate::raw_segment::{RawCoordinate, open_readonly};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom},
    path::Path,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct FileStamp {
    pub len: u64,
    device: u64,
    inode: u64,
    modified: (i64, i64),
}
impl FileStamp {
    pub fn read(file: &File) -> io::Result<Self> {
        use std::os::unix::fs::MetadataExt;
        let m = file.metadata()?;
        Ok(Self {
            len: m.len(),
            device: m.dev(),
            inode: m.ino(),
            modified: (m.mtime(), m.mtime_nsec()),
        })
    }
    pub fn same_file(&self, other: &Self) -> bool {
        self.device == other.device && self.inode == other.inode
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn permission_changes_preserve_stamp_but_content_and_inode_changes_do_not() {
        let dir = crate::test_tempdir();
        let path = dir.path().join("transcript.jsonl");
        std::fs::write(&path, b"{\"content\":\"initial\"}\n").unwrap();
        let before = FileStamp::read(&File::open(&path).unwrap()).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        let after_mode = FileStamp::read(&File::open(&path).unwrap()).unwrap();
        assert_eq!(before, after_mode);
        // Old serialized cursors remain readable; their obsolete ctime is ignored.
        let mut legacy = serde_json::to_value(&before).unwrap();
        legacy["changed"] = serde_json::json!([1, 2]);
        assert_eq!(
            serde_json::from_value::<FileStamp>(legacy).unwrap(),
            after_mode
        );
        std::fs::write(&path, b"{\"content\":\"new larger input\"}\n").unwrap();
        assert_ne!(
            before,
            FileStamp::read(&File::open(&path).unwrap()).unwrap()
        );
        let replacement = dir.path().join("replacement");
        std::fs::write(&replacement, b"{\"content\":\"initial\"}\n").unwrap();
        std::fs::rename(replacement, &path).unwrap();
        assert!(!before.same_file(&FileStamp::read(&File::open(&path).unwrap()).unwrap()));
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
pub(crate) fn scan_jsonl(
    path: &Path,
    stamp: &FileStamp,
    previous: Option<&SourceCursor>,
    force_full: bool,
) -> io::Result<JsonlRefresh> {
    let mut file = open_readonly(path)?;
    let mut start = 0;
    let mut seq = 1;
    let mut verified = 0;
    let mut replace = true;
    if !force_full
        && let Some(old) = previous
        && old.stamp.same_file(stamp)
        && stamp.len >= old.stamp.len
        && old.end <= stamp.len
    {
        let (boundary, length) = boundary_hash(&mut file, old.end)?;
        verified = length;
        if boundary == old.boundary_sha256 {
            start = old.end;
            seq = old.next_seq;
            replace = false;
        }
    }
    file.seek(SeekFrom::Start(start))?;
    let mut reader = BufReader::with_capacity(65536, file.take(stamp.len - start));
    let mut records = Vec::new();
    let mut at = start;
    let mut length = 0;
    let mut record_hash = Sha256::new();
    let mut scanned = 0;
    loop {
        let bytes = reader.fill_buf()?;
        if bytes.is_empty() {
            break;
        }
        let newline = bytes.iter().position(|b| *b == b'\n');
        let consumed = newline.map_or(bytes.len(), |n| n + 1);
        record_hash.update(&bytes[..consumed]);
        length += consumed as u64;
        scanned += consumed as u64;
        reader.consume(consumed);
        if newline.is_none() {
            continue;
        }
        let mut validation = open_readonly(path)?;
        validation.seek(SeekFrom::Start(at))?;
        serde_json::from_reader::<_, serde::de::IgnoredAny>(validation.take(length))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        records.push(RawCoordinate {
            segment: path.into(),
            offset: at,
            length,
            sha256: format!("{:x}", record_hash.finalize_reset()),
            seq,
        });
        at += length;
        length = 0;
        seq += 1;
    }
    let mut file = open_readonly(path)?;
    let (boundary_sha256, boundary_capture_bytes) = boundary_hash(&mut file, at)?;
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
fn boundary_hash(file: &mut File, end: u64) -> io::Result<(String, u64)> {
    let length = end.min(BOUNDARY_BYTES);
    file.seek(SeekFrom::Start(end - length))?;
    let mut bytes = vec![0; length as usize];
    file.read_exact(&mut bytes)?;
    Ok((format!("{:x}", Sha256::digest(&bytes)), length))
}
