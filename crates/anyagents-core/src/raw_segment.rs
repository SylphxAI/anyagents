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

const MAGIC: &[u8; 8] = b"AARAW002";
const HEADER: u64 = 88;

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
        segment.recover(&mut file.0)?;
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
        let records = self.recover(&mut file.0)?;
        let seq = records.last().map_or(1, |r| r.seq + 1);
        let start = file.0.seek(SeekFrom::End(0))?;
        let hash = Sha256::digest(payload);
        let mut header = Vec::with_capacity(HEADER as usize);
        header.extend_from_slice(MAGIC);
        header.extend_from_slice(&seq.to_le_bytes());
        header.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        header.extend_from_slice(&hash);
        let header_hash = Sha256::digest(&header);
        header.extend_from_slice(&header_hash);
        file.0.write_all(&header)?;
        file.0.write_all(payload)?;
        file.0.sync_all()?;
        // Publish durable acknowledgement frontier before returning a coordinate.
        // Bytes past it can only belong to an append that was not acknowledged.
        crate::learned_memory::atomic_write(
            &self.frontier_path(),
            &(start + HEADER + payload.len() as u64).to_le_bytes(),
        )?;
        Ok(RawCoordinate {
            segment: self.path.clone(),
            offset: start + HEADER,
            length: payload.len() as u64,
            sha256: format!("{hash:x}"),
            seq,
        })
    }
    fn frontier_path(&self) -> PathBuf {
        self.path.with_extension("ack")
    }
    fn recover(&self, file: &mut File) -> io::Result<Vec<RawCoordinate>> {
        let size = file.metadata()?.len();
        let frontier = match fs::read(self.frontier_path()) {
            Ok(bytes) => u64::from_le_bytes(
                bytes
                    .try_into()
                    .map_err(|_| io::Error::other("invalid raw acknowledgement frontier"))?,
            ),
            Err(e) if e.kind() == io::ErrorKind::NotFound && size == 0 => {
                crate::learned_memory::atomic_write(&self.frontier_path(), &0u64.to_le_bytes())?;
                0
            }
            Err(e) => return Err(e),
        };
        if frontier > size {
            return Err(io::Error::other("acknowledged raw segment shortened"));
        }
        // Validate every acknowledged byte BEFORE considering tail recovery.
        let records = scan(file, &self.path, frontier)?;
        if size > frontier {
            file.set_len(frontier)?;
            file.sync_all()?;
        }
        Ok(records)
    }
    pub fn records(&self) -> io::Result<Vec<RawCoordinate>> {
        self.recover(&mut self.lock()?.0)
    }
}
fn scan(file: &mut File, path: &Path, size: u64) -> io::Result<Vec<RawCoordinate>> {
    file.seek(SeekFrom::Start(0))?;
    let mut at = 0;
    let mut records = Vec::new();
    while at < size {
        let mut header = [0u8; HEADER as usize];
        if size - at < HEADER {
            return Err(io::Error::other("incomplete raw frame header"));
        }
        file.read_exact(&mut header)?;
        if &header[..8] != MAGIC {
            return Err(io::Error::other("invalid raw segment magic"));
        }
        if Sha256::digest(&header[..56])[..] != header[56..] {
            return Err(io::Error::other("raw header SHA mismatch"));
        }
        let seq = u64::from_le_bytes(header[8..16].try_into().unwrap());
        let len = u64::from_le_bytes(header[16..24].try_into().unwrap());
        if seq != records.last().map_or(1, |r: &RawCoordinate| r.seq + 1) {
            return Err(io::Error::other("noncontiguous raw sequence"));
        }
        if len > size - at - HEADER {
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
        if hash[..] != header[24..56] {
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
    fn acknowledged_length_mutation_refuses_without_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raw");
        RawSegment::open(&path)
            .unwrap()
            .append(b"acknowledged payload")
            .unwrap();
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(16)).unwrap();
        file.write_all(&u64::MAX.to_le_bytes()).unwrap();
        let before = fs::read(&path).unwrap();
        assert_eq!(
            RawSegment::open(&path).err().unwrap().to_string(),
            "raw header SHA mismatch"
        );
        assert_eq!(fs::read(&path).unwrap(), before);
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
