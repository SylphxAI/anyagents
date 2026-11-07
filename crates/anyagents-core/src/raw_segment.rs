//! Append-only host adapter port. SQLite stores coordinates, never raw events.
//! A frame is synced before its coordinate is acknowledged. Native JSONL remains
//! in place until its writer is stopped; this module does not rename it.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const MAGIC: &[u8; 8] = b"AARAW001";
const HEADER: u64 = 56;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawCoordinate {
    pub segment: PathBuf,
    pub offset: u64,
    pub length: u64,
    pub sha256: String,
    pub seq: u64,
}

pub struct RawSegment {
    path: PathBuf,
}
struct LockedFile(File);
impl Drop for LockedFile {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            unsafe {
                libc::flock(self.0.as_raw_fd(), libc::LOCK_UN);
            }
        }
    }
}
impl RawSegment {
    pub fn open(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let segment = Self {
            path: path.to_owned(),
        };
        let mut file = segment.lock()?;
        // Recover only a torn final frame. Hash corruption of a complete frame
        // is not repaired by deleting acknowledged history.
        scan(&mut file.0, &segment.path, true)?;
        Ok(segment)
    }
    fn lock(&self) -> io::Result<LockedFile> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&self.path)?;
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(LockedFile(file))
    }
    pub fn append(&self, payload: &[u8]) -> io::Result<RawCoordinate> {
        let mut file = self.lock()?;
        let records = scan(&mut file.0, &self.path, true)?;
        let seq = records.last().map_or(1, |r| r.seq + 1);
        let start = file.0.seek(SeekFrom::End(0))?;
        let hash = Sha256::digest(payload);
        file.0.write_all(MAGIC)?;
        file.0.write_all(&seq.to_le_bytes())?;
        file.0.write_all(&(payload.len() as u64).to_le_bytes())?;
        file.0.write_all(&hash)?;
        file.0.write_all(payload)?;
        file.0.sync_all()?;
        Ok(RawCoordinate {
            segment: self.path.clone(),
            offset: start + HEADER,
            length: payload.len() as u64,
            sha256: format!("{hash:x}"),
            seq,
        })
    }
    pub fn records(&self) -> io::Result<Vec<RawCoordinate>> {
        scan(&mut self.lock()?.0, &self.path, false)
    }
}
fn scan(file: &mut File, path: &Path, recover_tail: bool) -> io::Result<Vec<RawCoordinate>> {
    file.seek(SeekFrom::Start(0))?;
    let size = file.metadata()?.len();
    let mut at = 0;
    let mut records = Vec::new();
    while at < size {
        let mut header = [0u8; HEADER as usize];
        if size - at < HEADER {
            if recover_tail {
                file.set_len(at)?;
                file.sync_all()?;
                break;
            }
            return Err(io::Error::other("incomplete raw frame header"));
        }
        file.read_exact(&mut header)?;
        if &header[..8] != MAGIC {
            return Err(io::Error::other("invalid raw segment magic"));
        }
        let seq = u64::from_le_bytes(header[8..16].try_into().unwrap());
        let len = u64::from_le_bytes(header[16..24].try_into().unwrap());
        if seq != records.last().map_or(1, |r: &RawCoordinate| r.seq + 1) {
            return Err(io::Error::other("noncontiguous raw sequence"));
        }
        if len > size - at - HEADER {
            if recover_tail {
                file.set_len(at)?;
                file.sync_all()?;
                break;
            }
            return Err(io::Error::other("incomplete raw frame payload"));
        }
        let mut hasher = Sha256::new();
        let mut remaining = len;
        let mut buf = [0u8; 65536];
        while remaining > 0 {
            let n = remaining.min(buf.len() as u64) as usize;
            file.read_exact(&mut buf[..n])?;
            hasher.update(&buf[..n]);
            remaining -= n as u64;
        }
        let hash = hasher.finalize();
        if hash[..] != header[24..] {
            return Err(io::Error::other("raw frame SHA mismatch"));
        }
        records.push(RawCoordinate {
            segment: path.to_owned(),
            offset: at + HEADER,
            length: len,
            sha256: format!("{hash:x}"),
            seq,
        });
        at += HEADER + len;
    }
    Ok(records)
}

/// Exact raw bytes; callers must authorize the owning agent before using a
/// coordinate. Coordinates come from the trusted host index, not tool arguments.
pub fn read_raw(coordinate: &RawCoordinate) -> io::Result<Vec<u8>> {
    let mut file = File::open(&coordinate.segment)?;
    file.seek(SeekFrom::Start(coordinate.offset))?;
    let mut bytes = Vec::new();
    file.take(coordinate.length).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != coordinate.length
        || crate::learned_memory::sha256(&bytes) != coordinate.sha256
    {
        return Err(io::Error::other("raw coordinate readback mismatch"));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn append_restart_and_exact_range_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("segments/first.raw");
        let segment = RawSegment::open(&path).unwrap();
        let first = segment.append(b"{\"tool\":\"browser\"}\n\x00\xff").unwrap();
        let second = segment.append("你好\nreasoning".as_bytes()).unwrap();
        assert_eq!(first.seq, 1);
        assert_eq!(second.seq, 2);
        assert_eq!(first.offset, HEADER);
        assert_eq!(second.offset, HEADER * 2 + first.length);
        drop(segment);
        let restarted = RawSegment::open(&path).unwrap();
        assert_eq!(
            restarted.records().unwrap(),
            vec![first.clone(), second.clone()]
        );
        assert_eq!(
            read_raw(&first).unwrap(),
            b"{\"tool\":\"browser\"}\n\x00\xff"
        );
        assert_eq!(read_raw(&second).unwrap(), "你好\nreasoning".as_bytes());
        assert_eq!(restarted.append(b"next").unwrap().seq, 3);
    }
    #[test]
    fn torn_tail_recovers_but_complete_corruption_does_not() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raw");
        let first = RawSegment::open(&path).unwrap().append(b"safe").unwrap();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"AAR")
            .unwrap();
        let restarted = RawSegment::open(&path).unwrap();
        assert_eq!(restarted.records().unwrap(), vec![first.clone()]);
        assert_eq!(read_raw(&first).unwrap(), b"safe");
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(first.offset)).unwrap();
        file.write_all(b"bad!").unwrap();
        let len = fs::metadata(&path).unwrap().len();
        assert_eq!(
            RawSegment::open(&path).err().unwrap().to_string(),
            "raw frame SHA mismatch"
        );
        assert_eq!(fs::metadata(&path).unwrap().len(), len);
    }
}
