//! Rebuildable source cursors. Metadata equality skips unchanged sources only;
//! native appends still authenticate the complete prior prefix before reuse.
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
    changed: (i64, i64),
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
            changed: (m.ctime(), m.ctime_nsec()),
        })
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
    pub prefix_sha256: String,
}
pub(crate) struct JsonlRefresh {
    pub records: Vec<RawCoordinate>,
    pub end: u64,
    pub next_seq: u64,
    pub prefix_sha256: String,
    pub prefix_verified_bytes: u64,
    pub suffix_scanned_bytes: u64,
    pub replace: bool,
}
/// Read one stable-length snapshot. Incomplete tail bytes never enter the
/// committed frontier. A replaced/truncated/edited prefix forces a full rebuild.
pub(crate) fn scan_jsonl(
    path: &Path,
    stamp: &FileStamp,
    previous: Option<&SourceCursor>,
) -> io::Result<JsonlRefresh> {
    let mut file = open_readonly(path)?;
    let mut hash = Sha256::new();
    let mut start = 0;
    let mut seq = 1;
    let mut verified = 0;
    let mut replace = true;
    if let Some(old) = previous
        && old.stamp.same_file(stamp)
        && old.end <= stamp.len
    {
        let mut buffer = [0u8; 65536];
        while verified < old.end {
            let n = (old.end - verified).min(buffer.len() as u64) as usize;
            file.read_exact(&mut buffer[..n])?;
            hash.update(&buffer[..n]);
            verified += n as u64;
        }
        if format!("{:x}", hash.clone().finalize()) == old.prefix_sha256 {
            start = old.end;
            seq = old.next_seq;
            replace = false;
        } else {
            hash = Sha256::new();
        }
    }
    file.seek(SeekFrom::Start(start))?;
    let mut reader = BufReader::with_capacity(65536, file.take(stamp.len - start));
    let mut records = Vec::new();
    let mut at = start;
    let mut length = 0;
    let mut record_hash = Sha256::new();
    let mut committed_hash = hash.clone();
    let mut scanned = 0;
    loop {
        let bytes = reader.fill_buf()?;
        if bytes.is_empty() {
            break;
        }
        let newline = bytes.iter().position(|b| *b == b'\n');
        let consumed = newline.map_or(bytes.len(), |n| n + 1);
        hash.update(&bytes[..consumed]);
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
        committed_hash = hash.clone();
    }
    Ok(JsonlRefresh {
        records,
        end: at,
        next_seq: seq,
        prefix_sha256: format!("{:x}", committed_hash.finalize()),
        prefix_verified_bytes: verified,
        suffix_scanned_bytes: scanned,
        replace,
    })
}
