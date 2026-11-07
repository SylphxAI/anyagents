//! Append-only host adapter port. SQLite stores coordinates, never raw events.
//! A frame is synced before its coordinate is acknowledged. Native JSONL remains
//! in place until its writer is stopped; once handed off, this module can index
//! and append that same JSONL file without copying or reframing it.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const MAGIC_V1: &[u8; 8] = b"AARAW001";
const MAGIC_V2: &[u8; 8] = b"AARAW002";
const HEADER_V1: u64 = 56;
// v2 keeps the v1 fields and adds a SHA-256 checksum of all header fields.
const HEADER_V2: u64 = 88;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawCoordinate {
    pub segment: PathBuf,
    pub offset: u64,
    pub length: u64,
    pub sha256: String,
    pub seq: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SegmentFormat {
    FramedV1,
    FramedV2,
    Jsonl,
}

pub struct RawSegment {
    path: PathBuf,
    format: SegmentFormat,
}

struct LockedFile(File);
impl Drop for LockedFile {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            // Unlocking on drop is best-effort; the file close also releases it.
            unsafe {
                libc::flock(self.0.as_raw_fd(), libc::LOCK_UN);
            }
        }
    }
}

impl RawSegment {
    /// Open a segment and infer its on-disk format.
    ///
    /// Empty paths start in the integrity-protected framed format. A renamed
    /// native JSONL file is detected in place, so handoff does not copy or
    /// reframe an existing history. Call [`Self::open_jsonl`] for an empty
    /// renamed JSONL file, whose format cannot be inferred from bytes.
    pub fn open(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let format = detect_format(path)?;
        let segment = Self {
            path: path.to_owned(),
            format,
        };
        let mut file = segment.lock()?;
        // Recover only a provably unacknowledged final frame. A complete v2
        // header has an integrity MAC; a changed length is metadata corruption
        // and is never repaired by deleting acknowledged history.
        scan(&mut file.0, &segment.path, segment.format, true)?;
        Ok(segment)
    }

    /// Open a path known by the host migration manifest to be legacy JSONL.
    /// This is needed only for an empty file; non-empty files are also accepted
    /// by [`Self::open`] through byte-format detection.
    pub fn open_jsonl(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let segment = Self {
            path: path.to_owned(),
            format: SegmentFormat::Jsonl,
        };
        let mut file = segment.lock()?;
        scan(&mut file.0, &segment.path, segment.format, false)?;
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
        match self.format {
            SegmentFormat::Jsonl => append_jsonl(&mut file.0, &self.path, payload),
            SegmentFormat::FramedV1 | SegmentFormat::FramedV2 => {
                append_framed(&mut file.0, &self.path, self.format, payload)
            }
        }
    }

    pub fn records(&self) -> io::Result<Vec<RawCoordinate>> {
        scan(&mut self.lock()?.0, &self.path, self.format, false)
    }
}

fn detect_format(path: &Path) -> io::Result<SegmentFormat> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    let mut magic = [0u8; 8];
    let n = file.read(&mut magic)?;
    if n == 0 {
        return Ok(SegmentFormat::FramedV2);
    }
    if n == 8 && &magic == MAGIC_V1 {
        Ok(SegmentFormat::FramedV1)
    } else if n == 8 && &magic == MAGIC_V2 {
        Ok(SegmentFormat::FramedV2)
    } else {
        Ok(SegmentFormat::Jsonl)
    }
}

fn append_framed(
    file: &mut File,
    path: &Path,
    format: SegmentFormat,
    payload: &[u8],
) -> io::Result<RawCoordinate> {
    // Scan with recovery enabled before calculating the next sequence. For v2,
    // only a checksum-valid torn payload can be truncated; metadata corruption is an
    // error and leaves the acknowledged prefix untouched.
    let records = scan(file, path, format, true)?;
    let seq = records.last().map_or(1, |r| r.seq + 1);
    let start = file.seek(SeekFrom::End(0))?;
    let hash = Sha256::digest(payload);
    let mut header = [0u8; HEADER_V2 as usize];
    header[..8].copy_from_slice(match format {
        SegmentFormat::FramedV1 => MAGIC_V1,
        SegmentFormat::FramedV2 => MAGIC_V2,
        SegmentFormat::Jsonl => unreachable!(),
    });
    header[8..16].copy_from_slice(&seq.to_le_bytes());
    header[16..24].copy_from_slice(&(payload.len() as u64).to_le_bytes());
    header[24..56].copy_from_slice(&hash);
    if format == SegmentFormat::FramedV2 {
        let header_hash = Sha256::digest(&header[..56]);
        header[56..88].copy_from_slice(&header_hash);
        file.write_all(&header)?;
    } else {
        file.write_all(&header[..HEADER_V1 as usize])?;
    }
    file.write_all(payload)?;
    file.sync_all()?;
    Ok(RawCoordinate {
        segment: path.to_owned(),
        offset: start
            + match format {
                SegmentFormat::FramedV1 => HEADER_V1,
                SegmentFormat::FramedV2 => HEADER_V2,
                SegmentFormat::Jsonl => unreachable!(),
            },
        length: payload.len() as u64,
        sha256: format!("{hash:x}"),
        seq,
    })
}

fn append_jsonl(file: &mut File, path: &Path, payload: &[u8]) -> io::Result<RawCoordinate> {
    let records = scan_jsonl(file, path, false)?;
    // A handoff may append only after the native writer has committed a whole
    // line. Never turn an unacknowledged partial line into a different event.
    let end = file.seek(SeekFrom::End(0))?;
    if end > 0 {
        file.seek(SeekFrom::End(-1))?;
        let mut last = [0u8; 1];
        file.read_exact(&mut last)?;
        if last[0] != b'\n' {
            return Err(io::Error::other("incomplete legacy JSONL final line"));
        }
    }
    let json = payload.strip_suffix(b"\n").unwrap_or(payload);
    if json.contains(&b'\n') || serde_json::from_slice::<serde_json::Value>(json).is_err() {
        return Err(io::Error::other(
            "legacy JSONL append requires one valid JSON value",
        ));
    }
    let start = file.seek(SeekFrom::End(0))?;
    let line = if payload.ends_with(b"\n") {
        payload.to_vec()
    } else {
        let mut line = Vec::with_capacity(payload.len() + 1);
        line.extend_from_slice(payload);
        line.push(b'\n');
        line
    };
    let hash = Sha256::digest(&line);
    file.write_all(&line)?;
    file.sync_all()?;
    Ok(RawCoordinate {
        segment: path.to_owned(),
        offset: start,
        length: line.len() as u64,
        sha256: format!("{hash:x}"),
        seq: records.last().map_or(1, |r| r.seq + 1),
    })
}

fn scan(
    file: &mut File,
    path: &Path,
    format: SegmentFormat,
    recover_tail: bool,
) -> io::Result<Vec<RawCoordinate>> {
    match format {
        SegmentFormat::Jsonl => scan_jsonl(file, path, recover_tail),
        SegmentFormat::FramedV1 | SegmentFormat::FramedV2 => {
            scan_framed(file, path, format, recover_tail)
        }
    }
}

fn scan_framed(
    file: &mut File,
    path: &Path,
    format: SegmentFormat,
    recover_tail: bool,
) -> io::Result<Vec<RawCoordinate>> {
    file.seek(SeekFrom::Start(0))?;
    let size = file.metadata()?.len();
    let header_len = match format {
        SegmentFormat::FramedV1 => HEADER_V1,
        SegmentFormat::FramedV2 => HEADER_V2,
        SegmentFormat::Jsonl => unreachable!(),
    };
    let mut at = 0;
    let mut records = Vec::new();
    while at < size {
        let remaining = size - at;
        if remaining < header_len {
            if recover_tail {
                // A short header cannot be an acknowledged frame: append writes
                // the complete header before any coordinate is returned.
                file.set_len(at)?;
                file.sync_all()?;
                break;
            }
            return Err(io::Error::other("incomplete raw frame header"));
        }
        let mut header = [0u8; HEADER_V2 as usize];
        file.read_exact(&mut header[..header_len as usize])?;
        let expected_magic = match format {
            SegmentFormat::FramedV1 => MAGIC_V1,
            SegmentFormat::FramedV2 => MAGIC_V2,
            SegmentFormat::Jsonl => unreachable!(),
        };
        if &header[..8] != expected_magic {
            return Err(io::Error::other("invalid raw segment magic"));
        }
        if format == SegmentFormat::FramedV2 {
            let expected = Sha256::digest(&header[..56]);
            if header[56..88] != expected[..] {
                return Err(io::Error::other("raw frame header integrity mismatch"));
            }
        }
        let seq = u64::from_le_bytes(header[8..16].try_into().unwrap());
        let len = u64::from_le_bytes(header[16..24].try_into().unwrap());
        if seq != records.last().map_or(1, |r: &RawCoordinate| r.seq + 1) {
            return Err(io::Error::other("noncontiguous raw sequence"));
        }
        if len > size - at - header_len {
            if recover_tail && format == SegmentFormat::FramedV2 {
                // A valid header checksum proves this is a payload torn after the
                // header, not a flipped length field. It is unacknowledged and
                // may be removed; acknowledged coordinates are never touched.
                file.set_len(at)?;
                file.sync_all()?;
                break;
            }
            return Err(io::Error::other("incomplete or corrupt raw frame payload"));
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
            offset: at + header_len,
            length: len,
            sha256: format!("{hash:x}"),
            seq,
        });
        at += header_len + len;
    }
    Ok(records)
}

fn scan_jsonl(file: &mut File, path: &Path, _recover_tail: bool) -> io::Result<Vec<RawCoordinate>> {
    file.seek(SeekFrom::Start(0))?;
    let mut records = Vec::new();
    let mut at = 0u64;
    let mut line = Vec::new();
    let mut reader = BufReader::with_capacity(64 * 1024, file);
    loop {
        line.clear();
        let n = reader.read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        if !line.ends_with(b"\n") {
            // A JSONL handoff must not acknowledge an incomplete native write.
            return Err(io::Error::other("incomplete legacy JSONL final line"));
        }
        let hash = Sha256::digest(&line);
        records.push(RawCoordinate {
            segment: path.to_owned(),
            offset: at,
            length: line.len() as u64,
            sha256: format!("{hash:x}"),
            seq: records.len() as u64 + 1,
        });
        at += line.len() as u64;
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
        assert_eq!(first.offset, HEADER_V2);
        assert_eq!(second.offset, HEADER_V2 * 2 + first.length);
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
        file.seek(SeekFrom::Start(first.offset - (HEADER_V2 - 16)))
            .unwrap(); // v2 length field
        file.write_all(&u64::MAX.to_le_bytes()).unwrap();
        let len = fs::metadata(&path).unwrap().len();
        assert_eq!(
            RawSegment::open(&path).err().unwrap().to_string(),
            "raw frame header integrity mismatch"
        );
        assert_eq!(fs::metadata(&path).unwrap().len(), len);
    }

    #[test]
    fn complete_frame_length_mutation_preserves_all_bytes_and_refuses() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raw");
        let first = RawSegment::open(&path)
            .unwrap()
            .append(b"acknowledged")
            .unwrap();
        let before = fs::read(&path).unwrap();
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(first.offset - (HEADER_V2 - 16)))
            .unwrap();
        file.write_all(&((first.length + 1).to_le_bytes())).unwrap();
        drop(file);
        assert!(RawSegment::open(&path).is_err());
        assert_eq!(fs::read(&path).unwrap().len(), before.len());
        // Restoring the original bytes is the only repair; no acknowledged
        // payload was truncated by open().
        fs::write(&path, before).unwrap();
        assert_eq!(
            RawSegment::open(&path).unwrap().records().unwrap(),
            vec![first]
        );
    }

    #[test]
    fn renamed_jsonl_is_indexed_and_appended_without_reframing() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("native.jsonl");
        fs::write(&source, b"{\"seq\":1}\n{\"seq\":2}\n").unwrap();
        let destination = dir.path().join("host/segments/history.jsonl");
        fs::create_dir_all(destination.parent().unwrap()).unwrap();
        fs::rename(&source, &destination).unwrap();
        let segment = RawSegment::open(&destination).unwrap();
        let existing = segment.records().unwrap();
        assert_eq!(existing.len(), 2);
        let appended = segment.append(br#"{"seq":3}"#).unwrap();
        assert_eq!(appended.seq, 3);
        assert_eq!(read_raw(&existing[0]).unwrap(), b"{\"seq\":1}\n");
        assert_eq!(read_raw(&appended).unwrap(), b"{\"seq\":3}\n");
        assert_eq!(
            fs::read(&destination).unwrap(),
            b"{\"seq\":1}\n{\"seq\":2}\n{\"seq\":3}\n"
        );
    }

    #[test]
    fn jsonl_partial_final_line_is_preserved_and_not_split() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("native.jsonl");
        fs::write(&path, b"{\"complete\":true}\npartial").unwrap();
        let before = fs::read(&path).unwrap();
        assert!(RawSegment::open(&path).is_err());
        assert_eq!(fs::read(&path).unwrap(), before);
        let known_empty = dir.path().join("empty.jsonl");
        let segment = RawSegment::open_jsonl(&known_empty).unwrap();
        assert_eq!(segment.append(b"{\"ok\":true}").unwrap().seq, 1);
    }
}
