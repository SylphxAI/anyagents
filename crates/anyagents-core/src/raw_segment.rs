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
    sync::Mutex,
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
    append_cursor: Mutex<Option<AppendCursor>>,
    #[cfg(test)]
    full_validations: std::sync::atomic::AtomicUsize,
}
struct AppendCursor {
    stamp: crate::host_cursor::FileStamp,
    next_seq: u64,
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
            append_cursor: Mutex::new(None),
            #[cfg(test)]
            full_validations: std::sync::atomic::AtomicUsize::new(0),
        };
        let mut file = segment.lock()?;
        // Validate the acknowledged prefix, recover complete synced events,
        // and quarantine/read back an uncertain suffix before any truncation.
        let records = segment.recover(&mut file.0)?;
        segment.cache_append_cursor(&file.0, records.last().map_or(1, |r| r.seq + 1))?;
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
            append_cursor: Mutex::new(None),
            #[cfg(test)]
            full_validations: std::sync::atomic::AtomicUsize::new(0),
        };
        let mut file = segment.lock()?;
        let records = scan(&mut file.0, &segment.path, segment.format, false)?;
        segment.cache_append_cursor(&file.0, records.last().map_or(1, |r| r.seq + 1))?;
        Ok(segment)
    }

    fn recover(&self, file: &mut File) -> io::Result<Vec<RawCoordinate>> {
        #[cfg(test)]
        self.full_validations
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.format == SegmentFormat::Jsonl {
            return scan_jsonl(file, &self.path, false);
        }
        let size = file.metadata()?.len();
        let frontier = read_frontier(&self.path, size)?;
        if frontier > size {
            return Err(io::Error::other("acknowledged raw segment shortened"));
        }
        // Validate the acknowledged prefix before touching any tail. Its header
        // and payload integrity are independent of the acknowledgement marker.
        let records = scan_framed(file, &self.path, self.format, false, Some(frontier))?;
        if size == frontier {
            return Ok(records);
        }
        match scan_framed(file, &self.path, self.format, false, None) {
            Ok(complete) => {
                // A crash between segment fsync and frontier publication must
                // not discard the only captured result. Adopt complete events.
                write_frontier(&self.path, size)?;
                Ok(complete)
            }
            Err(_) => {
                // Preserve uncertain bytes with a hash/readback receipt before
                // removing them from the active append path.
                quarantine_tail(file, &self.path, frontier, size)?;
                file.set_len(frontier)?;
                file.sync_all()?;
                Ok(records)
            }
        }
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

    fn cache_append_cursor(&self, file: &File, next_seq: u64) -> io::Result<()> {
        *self
            .append_cursor
            .lock()
            .map_err(|_| io::Error::other("raw append cursor poisoned"))? = Some(AppendCursor {
            stamp: crate::host_cursor::FileStamp::read(file)?,
            next_seq,
        });
        Ok(())
    }
    pub fn append(&self, payload: &[u8]) -> io::Result<RawCoordinate> {
        let mut file = self.lock()?;
        let stamp = crate::host_cursor::FileStamp::read(&file.0)?;
        let cached = self
            .append_cursor
            .lock()
            .map_err(|_| io::Error::other("raw append cursor poisoned"))?
            .take();
        // A frontier is authenticated on every framed append, even on a genuine
        // eligible cache hit. A corrupt acknowledgement never enters recovery.
        let frontier = if self.format == SegmentFormat::Jsonl {
            stamp.len
        } else {
            read_frontier(&self.path, stamp.len)?
        };
        let next_seq = if let Some(cursor) = cached
            && cursor.stamp == stamp
            && frontier == stamp.len
        {
            cursor.next_seq
        } else {
            self.recover(&mut file.0)?.last().map_or(1, |r| r.seq + 1)
        };
        let coordinate = match self.format {
            SegmentFormat::Jsonl => append_jsonl(&mut file.0, &self.path, payload, next_seq)?,
            SegmentFormat::FramedV1 | SegmentFormat::FramedV2 => {
                let coordinate =
                    append_framed(&mut file.0, &self.path, self.format, payload, next_seq)?;
                write_frontier(&self.path, file.0.metadata()?.len())?;
                coordinate
            }
        };
        self.cache_append_cursor(&file.0, coordinate.seq + 1)?;
        Ok(coordinate)
    }

    pub fn records(&self) -> io::Result<Vec<RawCoordinate>> {
        let mut file = self.lock()?;
        // Explicit enumeration validates all acknowledged history, not a cache.
        *self
            .append_cursor
            .lock()
            .map_err(|_| io::Error::other("raw append cursor poisoned"))? = None;
        let records = self.recover(&mut file.0)?;
        self.cache_append_cursor(&file.0, records.last().map_or(1, |r| r.seq + 1))?;
        Ok(records)
    }
}

/// Open retained bytes without following a symlink at any path component.
/// Descriptor-relative traversal closes the check/open race with a live seat.
pub fn open_readonly(path: &Path) -> io::Result<File> {
    open_readonly_with_metadata(path).map(|(file, _)| file)
}

fn open_readonly_with_metadata(path: &Path) -> io::Result<(File, fs::Metadata)> {
    #[cfg(test)]
    counters::OPENS.with(|n| n.set(n.get() + 1));
    #[cfg(unix)]
    {
        use std::{
            ffi::CString,
            os::fd::{AsRawFd, FromRawFd},
            os::unix::{ffi::OsStrExt, fs::MetadataExt},
        };
        let absolute = if path.is_absolute() {
            path.to_owned()
        } else {
            std::env::current_dir()?.join(path)
        };
        let parts: Vec<_> = absolute
            .components()
            .filter_map(|c| match c {
                std::path::Component::RootDir => None,
                std::path::Component::Normal(s) => Some(Ok(s)),
                _ => Some(Err(io::Error::other("unsupported raw path component"))),
            })
            .collect::<io::Result<_>>()?;
        let mut directory = File::open("/")?;
        for (i, part) in parts.iter().enumerate() {
            let name = CString::new(part.as_bytes()).map_err(io::Error::other)?;
            let final_part = i + 1 == parts.len();
            let flags = libc::O_RDONLY
                | libc::O_NOFOLLOW
                | libc::O_CLOEXEC
                | if final_part { 0 } else { libc::O_DIRECTORY };
            let fd = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags) };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            let file = unsafe { File::from_raw_fd(fd) };
            if final_part {
                let meta = file.metadata()?;
                if !meta.is_file() || meta.nlink() != 1 {
                    return Err(io::Error::other(
                        "raw source must be a single-link regular file",
                    ));
                }
                return Ok((file, meta));
            }
            directory = file;
        }
        Err(io::Error::other("raw source needs a file path"))
    }
    #[cfg(not(unix))]
    {
        let file = File::open(path)?;
        let meta = file.metadata()?;
        Ok((file, meta))
    }
}

/// One retained file opened once through [`open_readonly`]. Every read of
/// one refresh (scan, validation, metadata, projection, hashing) is served by
/// positioned reads on this descriptor through one read-ahead window, so a
/// source costs one path walk and O(bytes / window) reads, not a walk or a
/// syscall per record or per byte. Positioned reads share no file offset.
pub(crate) struct RawSource {
    path: PathBuf,
    file: File,
    stamp: crate::host_cursor::FileStamp,
    window: Vec<u8>,
    window_at: u64,
    window_len: usize,
}
const MIN_WINDOW: u64 = 4 * 1024;
const READ_AHEAD: u64 = 64 * 1024;
const MAX_WINDOW: u64 = 1024 * 1024;
impl RawSource {
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        let (file, metadata) = open_readonly_with_metadata(path)?;
        Ok(Self {
            path: path.to_owned(),
            file,
            stamp: crate::host_cursor::FileStamp::from_metadata(&metadata),
            window: Vec::new(),
            window_at: 0,
            window_len: 0,
        })
    }
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
    /// The stamp observed when the descriptor was opened.
    pub(crate) fn stamp(&self) -> &crate::host_cursor::FileStamp {
        &self.stamp
    }
    /// `[offset, offset + length)`, cut short at the end of the file.
    pub(crate) fn range(&mut self, offset: u64, length: u64) -> SourceRange<'_> {
        SourceRange {
            source: self,
            offset,
            end: offset.saturating_add(length),
        }
    }
    /// Refuse a coordinate whose bytes no longer have its length and SHA-256.
    pub(crate) fn verify(&mut self, raw: &RawCoordinate) -> io::Result<()> {
        let (length, sha256) = hash_all(self.range(raw.offset, raw.length))?;
        if length != raw.length || sha256 != raw.sha256 {
            return Err(io::Error::other("index coordinate integrity mismatch"));
        }
        Ok(())
    }
    /// The whole file, to its current end, as one document coordinate.
    pub(crate) fn whole(&mut self) -> io::Result<RawCoordinate> {
        let (length, sha256) = hash_all(self.range(0, u64::MAX))?;
        Ok(RawCoordinate {
            segment: self.path.clone(),
            offset: 0,
            length,
            sha256,
            seq: 1,
        })
    }
    /// Bytes from `offset`, refilling the window there when it does not
    /// already hold them. A refill reads ahead at least 64 KiB, so consecutive
    /// records share one read, and at most 1 MiB or the file's size.
    fn fill(&mut self, offset: u64, wanted: u64) -> io::Result<&[u8]> {
        let held = self.window_at..self.window_at + self.window_len as u64;
        if !held.contains(&offset) {
            let size = wanted
                .clamp(READ_AHEAD, MAX_WINDOW)
                .min(self.stamp.len.max(MIN_WINDOW)) as usize;
            if self.window.len() < size {
                self.window.resize(size, 0);
            }
            #[cfg(test)]
            counters::READS.with(|n| n.set(n.get() + 1));
            self.window_at = offset;
            self.window_len = 0;
            let mut reader = ReadAt {
                file: &self.file,
                offset,
            };
            while self.window_len < size {
                match reader.read(&mut self.window[self.window_len..size]) {
                    Ok(0) => break,
                    Ok(n) => self.window_len += n,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(e) => {
                        self.window_len = 0;
                        return Err(e);
                    }
                }
            }
        }
        let start = (offset - self.window_at) as usize;
        Ok(&self.window[start..self.window_len])
    }
}

/// Buffered reader over one byte range of a [`RawSource`].
pub(crate) struct SourceRange<'a> {
    source: &'a mut RawSource,
    offset: u64,
    end: u64,
}
impl Read for SourceRange<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let available = self.fill_buf()?;
        let n = available.len().min(output.len());
        output[..n].copy_from_slice(&available[..n]);
        self.consume(n);
        Ok(n)
    }
}
impl BufRead for SourceRange<'_> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.offset >= self.end {
            return Ok(&[]);
        }
        let wanted = self.end - self.offset;
        let bytes = self.source.fill(self.offset, wanted)?;
        Ok(&bytes[..bytes
            .len()
            .min(usize::try_from(wanted).unwrap_or(usize::MAX))])
    }
    fn consume(&mut self, amount: usize) {
        self.offset += amount as u64;
    }
}

/// Positioned reads from an offset; the descriptor's own offset is untouched.
struct ReadAt<'a> {
    file: &'a File,
    offset: u64,
}
impl Read for ReadAt<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let n = std::os::unix::fs::FileExt::read_at(self.file, output, self.offset)?;
        self.offset += n as u64;
        Ok(n)
    }
}

fn hash_all(mut reader: impl BufRead) -> io::Result<(u64, String)> {
    let mut hash = Sha256::new();
    let mut length = 0u64;
    loop {
        let bytes = reader.fill_buf()?;
        if bytes.is_empty() {
            return Ok((length, format!("{:x}", hash.finalize())));
        }
        hash.update(bytes);
        length += bytes.len() as u64;
        let n = bytes.len();
        reader.consume(n);
    }
}

/// One JSONL line read from a buffered stream.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum JsonlLine {
    /// A newline-terminated line holding an indexed raw coordinate.
    Complete { length: u64, sha256: String },
    /// Final bytes with no newline yet: never validated or committed.
    Partial { length: u64 },
}

/// Read, hash and validate one line as its bytes pass. Validation streams
/// through `IgnoredAny`, so neither a huge tool output nor the line itself is
/// ever resident, and no byte is read twice. `None` means end of input.
pub(crate) fn read_jsonl_line(reader: &mut impl BufRead) -> io::Result<Option<JsonlLine>> {
    read_jsonl_line_with_policy(reader, false)
}

pub(crate) fn read_jsonl_line_with_policy(
    reader: &mut impl BufRead,
    tolerate_malformed: bool,
) -> io::Result<Option<JsonlLine>> {
    let mut line = LineState {
        hash: Sha256::new(),
        length: 0,
        complete: false,
    };
    // serde reads byte by byte; the BufReader serves those bytes from memory.
    let parsed = serde_json::from_reader::<_, serde::de::IgnoredAny>(BufReader::with_capacity(
        8 * 1024,
        Line {
            reader: &mut *reader,
            state: &mut line,
        },
    ));
    // Whatever the parser did not consume still belongs to this line.
    io::copy(
        &mut Line {
            reader: &mut *reader,
            state: &mut line,
        },
        &mut io::sink(),
    )?;
    if line.length == 0 {
        return Ok(None);
    }
    if !line.complete {
        return Ok(Some(JsonlLine::Partial {
            length: line.length,
        }));
    }
    if let Err(e) = parsed {
        if e.classify() == serde_json::error::Category::Io {
            return Err(io::Error::new(
                e.io_error_kind().unwrap_or(io::ErrorKind::Other),
                e,
            ));
        }
        if !tolerate_malformed {
            return Err(io::Error::new(io::ErrorKind::InvalidData, e));
        }
    }
    Ok(Some(JsonlLine::Complete {
        length: line.length,
        sha256: format!("{:x}", line.hash.finalize()),
    }))
}
struct LineState {
    hash: Sha256,
    length: u64,
    complete: bool,
}
/// Yields bytes up to and including the first newline, hashing them.
struct Line<'a, R> {
    reader: &'a mut R,
    state: &'a mut LineState,
}
impl<R: BufRead> Read for Line<'_, R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.state.complete || output.is_empty() {
            return Ok(0);
        }
        let bytes = self.reader.fill_buf()?;
        let bytes = &bytes[..bytes.len().min(output.len())];
        let n = match bytes.iter().position(|b| *b == b'\n') {
            Some(newline) => {
                self.state.complete = true;
                newline + 1
            }
            None => bytes.len(),
        };
        output[..n].copy_from_slice(&bytes[..n]);
        self.state.hash.update(&bytes[..n]);
        self.state.length += n as u64;
        self.reader.consume(n);
        Ok(n)
    }
}

#[cfg(test)]
pub(crate) mod counters {
    use std::cell::Cell;
    thread_local! {
        /// Path walks through `open_readonly` on this thread.
        pub(crate) static OPENS: Cell<usize> = const { Cell::new(0) };
        /// Positioned window reads by `RawSource` on this thread.
        pub(crate) static READS: Cell<usize> = const { Cell::new(0) };
    }
    pub(crate) fn snapshot() -> (usize, usize) {
        (OPENS.with(Cell::get), READS.with(Cell::get))
    }
}

/// Read-only indexing entry point for a still-active native JSONL writer.
/// Incomplete final bytes are left untouched and retried on the next scan.
pub fn retained_jsonl_records(path: &Path) -> io::Result<Vec<RawCoordinate>> {
    let mut file = open_readonly(path)?;
    scan_jsonl(&mut file, path, true)
}

fn quarantine_tail(file: &mut File, path: &Path, start: u64, end: u64) -> io::Result<()> {
    let quarantine = path.with_file_name(format!(
        "{}.uncertain-{}",
        path.file_name().unwrap_or_default().to_string_lossy(),
        uuid::Uuid::now_v7()
    ));
    let mut out = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&quarantine)?;
    file.seek(SeekFrom::Start(start))?;
    let mut source_hash = Sha256::new();
    let mut remaining = end - start;
    let mut buffer = [0u8; 65536];
    while remaining > 0 {
        let n = remaining.min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..n])?;
        out.write_all(&buffer[..n])?;
        source_hash.update(&buffer[..n]);
        remaining -= n as u64;
    }
    out.sync_all()?;
    let source_hash = format!("{:x}", source_hash.finalize());
    let mut readback = File::open(&quarantine)?;
    let mut hash = Sha256::new();
    let mut count = 0u64;
    loop {
        let n = readback.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
        count += n as u64;
    }
    if count != end - start || format!("{:x}", hash.finalize()) != source_hash {
        return Err(io::Error::other("uncertain raw tail readback mismatch"));
    }
    let receipt = serde_json::json!({"source":path, "offset":start, "bytes":count,
        "destination":quarantine, "sha256":source_hash, "readback_verified":true});
    crate::learned_memory::atomic_write(
        &PathBuf::from(format!("{}.receipt.json", quarantine.display())),
        &serde_json::to_vec(&receipt).map_err(io::Error::other)?,
    )?;
    Ok(())
}

fn frontier_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".ack");
    PathBuf::from(name)
}
fn write_frontier(path: &Path, end: u64) -> io::Result<()> {
    let mut bytes = end.to_le_bytes().to_vec();
    let hash = Sha256::digest(&bytes);
    bytes.extend_from_slice(&hash);
    crate::learned_memory::atomic_write(&frontier_path(path), &bytes)
}
fn read_frontier(path: &Path, size: u64) -> io::Result<u64> {
    let bytes = match fs::read(frontier_path(path)) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == io::ErrorKind::NotFound && size == 0 => {
            write_frontier(path, 0)?;
            return Ok(0);
        }
        Err(e) => return Err(e),
    };
    if bytes.len() != 40 || Sha256::digest(&bytes[..8])[..] != bytes[8..] {
        return Err(io::Error::other(
            "raw acknowledgement frontier integrity mismatch",
        ));
    }
    Ok(u64::from_le_bytes(bytes[..8].try_into().unwrap()))
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
    } else if frontier_path(path).try_exists()? {
        // The host acknowledgement marker is authoritative: damaged framed
        // magic cannot select the weaker legacy JSONL path.
        Ok(SegmentFormat::FramedV2)
    } else {
        scan_jsonl(&mut file, path, false)?;
        Ok(SegmentFormat::Jsonl)
    }
}

fn append_framed(
    file: &mut File,
    path: &Path,
    format: SegmentFormat,
    payload: &[u8],
    seq: u64,
) -> io::Result<RawCoordinate> {
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

fn append_jsonl(
    file: &mut File,
    path: &Path,
    payload: &[u8],
    seq: u64,
) -> io::Result<RawCoordinate> {
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
        seq,
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
            scan_framed(file, path, format, recover_tail, None)
        }
    }
}

fn scan_framed(
    file: &mut File,
    path: &Path,
    format: SegmentFormat,
    _recover_tail: bool,
    limit: Option<u64>,
) -> io::Result<Vec<RawCoordinate>> {
    file.seek(SeekFrom::Start(0))?;
    let size = limit.unwrap_or(file.metadata()?.len());
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

fn scan_jsonl(
    file: &mut File,
    path: &Path,
    allow_partial_tail: bool,
) -> io::Result<Vec<RawCoordinate>> {
    file.seek(SeekFrom::Start(0))?;
    let mut records = Vec::new();
    let mut at = 0u64;
    let mut reader = BufReader::with_capacity(64 * 1024, file);
    // Each line is validated from the bytes already being read and hashed.
    while let Some(line) = read_jsonl_line(&mut reader)? {
        match line {
            JsonlLine::Complete { length, sha256 } => {
                records.push(RawCoordinate {
                    segment: path.to_owned(),
                    offset: at,
                    length,
                    sha256,
                    seq: records.len() as u64 + 1,
                });
                at += length;
            }
            // Read-only indexing leaves an active writer's tail untouched.
            JsonlLine::Partial { .. } if allow_partial_tail => break,
            JsonlLine::Partial { .. } => {
                return Err(io::Error::other("incomplete legacy JSONL final line"));
            }
        }
    }
    Ok(records)
}

/// Exact raw bytes; callers must authorize the owning agent before using a
/// coordinate. Coordinates come from the trusted host index, not tool arguments.
pub fn read_raw(coordinate: &RawCoordinate) -> io::Result<Vec<u8>> {
    let mut file = open_readonly(&coordinate.segment)?;
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

/// Bounded exact-byte readback. The complete indexed event is authenticated in
/// 64KiB chunks before returning at most 64KiB; no whole-event allocation.
pub const MAX_RAW_RANGE_BYTES: u64 = 64 * 1024;
pub fn read_raw_range(
    coordinate: &RawCoordinate,
    offset: u64,
    length: u64,
) -> io::Result<(RawCoordinate, Vec<u8>)> {
    if length == 0
        || length > MAX_RAW_RANGE_BYTES
        || offset > coordinate.length
        || offset.checked_add(length).is_none()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "raw range requires offset within event and length 1..=65536",
        ));
    }
    let end = offset.saturating_add(length).min(coordinate.length);
    let mut file = open_readonly(&coordinate.segment)?;
    let stamp = crate::host_cursor::FileStamp::read(&file)?;
    file.seek(SeekFrom::Start(coordinate.offset))?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 65536];
    let mut at = 0;
    let mut bytes = Vec::with_capacity((end - offset) as usize);
    while at < coordinate.length {
        let length = (coordinate.length - at).min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..length])?;
        hash.update(&buffer[..length]);
        let from = offset.max(at);
        let to = end.min(at + length as u64);
        if from < to {
            bytes.extend_from_slice(&buffer[(from - at) as usize..(to - at) as usize]);
        }
        at += length as u64;
    }
    if format!("{:x}", hash.finalize()) != coordinate.sha256 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "raw coordinate readback mismatch",
        ));
    }
    if crate::host_cursor::FileStamp::read(&open_readonly(&coordinate.segment)?)? != stamp {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "raw source changed during range read; retry",
        ));
    }
    let window = RawCoordinate {
        segment: coordinate.segment.clone(),
        offset: coordinate.offset.checked_add(offset).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "raw range offset overflow")
        })?,
        length: bytes.len() as u64,
        sha256: crate::learned_memory::sha256(&bytes),
        seq: coordinate.seq,
    };
    Ok((window, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_range_authenticates_whole_event_and_bounds_exact_binary_output() {
        let dir = crate::test_tempdir();
        let path = dir.path().join("binary");
        let bytes: Vec<u8> = (0..200_000).map(|n| (n % 256) as u8).collect();
        fs::write(&path, &bytes).unwrap();
        let coordinate = RawCoordinate {
            segment: path.clone(),
            offset: 13,
            length: 180_000,
            sha256: crate::learned_memory::sha256(&bytes[13..180_013]),
            seq: 7,
        };
        let (window, slice) = read_raw_range(&coordinate, 65_530, MAX_RAW_RANGE_BYTES).unwrap();
        assert_eq!(slice, bytes[65_543..131_079]);
        assert_eq!(
            (window.offset, window.length, window.seq),
            (65_543, MAX_RAW_RANGE_BYTES, 7)
        );
        assert_eq!(window.sha256, crate::learned_memory::sha256(&slice));
        assert_eq!(
            read_raw_range(&coordinate, 179_990, 32).unwrap().1,
            bytes[180_003..180_013]
        );
        assert!(
            read_raw_range(&coordinate, 180_000, 1)
                .unwrap()
                .1
                .is_empty()
        );
        for (offset, length) in [
            (0, 0),
            (0, MAX_RAW_RANGE_BYTES + 1),
            (180_001, 1),
            (u64::MAX, 1),
        ] {
            assert_eq!(
                read_raw_range(&coordinate, offset, length)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
        }
        // Corruption outside the returned range must still refuse the response.
        let mut modified = bytes;
        modified[100] ^= 1;
        fs::write(&path, modified).unwrap();
        assert_eq!(
            read_raw_range(&coordinate, 65_530, 32).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[cfg(unix)]
    #[test]
    fn readonly_descriptor_walk_refuses_linked_files_and_ancestors() {
        let dir = crate::test_tempdir();
        let plain = dir.path().join("plain");
        fs::create_dir(&plain).unwrap();
        fs::write(plain.join("event"), b"exact").unwrap();
        let mut bytes = Vec::new();
        open_readonly(&plain.join("event"))
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        assert_eq!(bytes, b"exact");
        std::os::unix::fs::symlink(&plain, dir.path().join("alias")).unwrap();
        assert!(open_readonly(&dir.path().join("alias/event")).is_err());
        fs::hard_link(plain.join("event"), dir.path().join("linked")).unwrap();
        let error = open_readonly(&dir.path().join("linked")).unwrap_err();
        assert!(error.to_string().contains("single-link"));
    }

    #[test]
    fn append_cursor_skips_unchanged_history_but_revalidates_other_writers_and_ack() {
        use std::sync::atomic::Ordering;
        let dir = crate::test_tempdir();
        let path = dir.path().join("raw");
        let segment = RawSegment::open(&path).unwrap();
        assert_eq!(segment.full_validations.load(Ordering::Relaxed), 1);
        for seq in 1..=20 {
            assert_eq!(segment.append(b"stable").unwrap().seq, seq);
        }
        assert_eq!(segment.full_validations.load(Ordering::Relaxed), 1);
        let other = RawSegment::open(&path).unwrap();
        assert_eq!(other.append(b"other writer").unwrap().seq, 21);
        assert_eq!(segment.append(b"after other").unwrap().seq, 22);
        assert_eq!(segment.full_validations.load(Ordering::Relaxed), 2);
        let before = fs::read(&path).unwrap();
        // This is a genuine eligible runtime cache hit; corrupt only the ack.
        fs::write(frontier_path(&path), b"corrupt acknowledgement").unwrap();
        assert!(
            segment
                .append(b"must not append")
                .unwrap_err()
                .to_string()
                .contains("frontier integrity mismatch")
        );
        assert_eq!(fs::read(&path).unwrap(), before);
        write_frontier(&path, before.len() as u64).unwrap();
        assert_eq!(segment.append(b"after ack repair").unwrap().seq, 23);
        // Rewriting acknowledged bytes invalidates metadata even at same size.
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(HEADER_V2)).unwrap();
        file.write_all(b"broken").unwrap();
        let damaged = fs::read(&path).unwrap();
        assert!(segment.append(b"must preserve corruption").is_err());
        assert_eq!(fs::read(&path).unwrap(), damaged);
    }

    #[test]
    fn append_restart_and_exact_range_read() {
        let dir = crate::test_tempdir();
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
        let dir = crate::test_tempdir();
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
        let uncertain: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("raw.uncertain-")
                    && !p.to_string_lossy().ends_with("receipt.json")
            })
            .collect();
        assert_eq!(uncertain.len(), 1);
        assert_eq!(fs::read(&uncertain[0]).unwrap(), b"AAR");
        let receipt: serde_json::Value = serde_json::from_slice(
            &fs::read(PathBuf::from(format!(
                "{}.receipt.json",
                uncertain[0].display()
            )))
            .unwrap(),
        )
        .unwrap();
        assert_eq!(receipt["sha256"], crate::learned_memory::sha256(b"AAR"));
        assert_eq!(receipt["bytes"], 3);
        assert_eq!(receipt["readback_verified"], true);
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
        let dir = crate::test_tempdir();
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
        let dir = crate::test_tempdir();
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
        let dir = crate::test_tempdir();
        let path = dir.path().join("native.jsonl");
        fs::write(&path, b"{\"complete\":true}\npartial").unwrap();
        let before = fs::read(&path).unwrap();
        assert!(RawSegment::open(&path).is_err());
        assert_eq!(fs::read(&path).unwrap(), before);
        let known_empty = dir.path().join("empty.jsonl");
        let segment = RawSegment::open_jsonl(&known_empty).unwrap();
        assert_eq!(segment.append(b"{\"ok\":true}").unwrap().seq, 1);
    }
    #[test]
    fn synced_event_before_frontier_crash_is_recovered_not_discarded() {
        let dir = crate::test_tempdir();
        let path = dir.path().join("raw");
        let segment = RawSegment::open(&path).unwrap();
        let first = segment.append(b"acknowledged").unwrap();
        let mut file = segment.lock().unwrap();
        let captured = append_framed(
            &mut file.0,
            &path,
            SegmentFormat::FramedV2,
            b"only captured tool result",
            first.seq + 1,
        )
        .unwrap();
        let before = fs::read(&path).unwrap();
        drop(file);
        drop(segment); // simulate death before write_frontier
        let reopened = RawSegment::open(&path).unwrap();
        assert_eq!(reopened.records().unwrap(), vec![first, captured.clone()]);
        assert_eq!(read_raw(&captured).unwrap(), b"only captured tool result");
        assert_eq!(fs::read(&path).unwrap(), before);
        assert_eq!(reopened.append(b"next").unwrap().seq, 3);
    }
    #[test]
    fn damaged_magic_cannot_downgrade_an_acknowledged_frame_to_jsonl() {
        let dir = crate::test_tempdir();
        let path = dir.path().join("raw");
        RawSegment::open(&path)
            .unwrap()
            .append(b"ends in newline\n")
            .unwrap();
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all(b"B").unwrap();
        drop(file);
        let before = fs::read(&path).unwrap();
        assert_eq!(
            RawSegment::open(&path).err().unwrap().to_string(),
            "invalid raw segment magic"
        );
        assert_eq!(fs::read(&path).unwrap(), before);
    }
    #[test]
    fn read_only_native_indexing_keeps_partial_tail_and_never_creates_files() {
        let dir = crate::test_tempdir();
        let path = dir.path().join("native.jsonl");
        fs::write(&path, b"{\"event\":1}\n{\"event\":").unwrap();
        let before = fs::read(&path).unwrap();
        let records = retained_jsonl_records(&path).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(read_raw(&records[0]).unwrap(), b"{\"event\":1}\n");
        assert_eq!(fs::read(&path).unwrap(), before);
        let absent = dir.path().join("absent");
        assert!(retained_jsonl_records(&absent).is_err());
        assert!(!absent.exists());
    }
    /// The single buffered pass accepts and refuses exactly what validating
    /// each newline-terminated line on its own did, and never commits a tail.
    #[test]
    fn streamed_line_validation_matches_per_line_json_validation() {
        let huge = format!(
            "{{\"output\":{}}}\n",
            serde_json::json!("x\\\"☃".repeat(700_000))
        );
        let lines: Vec<&[u8]> = vec![
            b"{\"a\":1}\n",
            b"[1,2,{\"b\":null}]\r\n",
            b"  \"padded\"  \n",
            huge.as_bytes(),
            b"\n",
            b"   \n",
            b"{\"a\":1} {\"b\":2}\n",
            b"{\"a\":\n",
            b"1}\n",
            b"{\"bad\":tru}\n",
            b"\"\\ud83d\\ude00\"\n",
        ];
        for line in &lines {
            let expected = serde_json::from_slice::<serde::de::IgnoredAny>(line).is_ok();
            let mut reader = BufReader::with_capacity(4096, *line);
            let got = read_jsonl_line(&mut reader);
            match got {
                Ok(Some(JsonlLine::Complete { length, sha256 })) => {
                    assert!(
                        expected,
                        "{:?}",
                        String::from_utf8_lossy(&line[..40.min(line.len())])
                    );
                    assert_eq!(length, line.len() as u64);
                    assert_eq!(sha256, crate::learned_memory::sha256(line));
                }
                Err(error) => {
                    assert!(!expected);
                    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
                }
                other => panic!("unexpected {other:?}"),
            }
            assert!(read_jsonl_line(&mut reader).unwrap().is_none());
        }
        // Lines are delimited by the first newline only, then by the stream.
        let stream = [lines[0], lines[1], huge.as_bytes(), b"{\"tail\":"].concat();
        let mut reader = BufReader::with_capacity(4096, stream.as_slice());
        let mut lengths = Vec::new();
        while let Some(line) = read_jsonl_line(&mut reader).unwrap() {
            lengths.push(line);
        }
        assert_eq!(lengths.len(), 4);
        assert_eq!(lengths[3], JsonlLine::Partial { length: 8 });
        // A complete value is still a tail until its newline is written.
        let mut reader = BufReader::new(b"{\"done\":true}".as_slice());
        assert_eq!(
            read_jsonl_line(&mut reader).unwrap(),
            Some(JsonlLine::Partial { length: 13 })
        );
    }

    #[test]
    fn acknowledged_payload_corruption_is_never_quarantined_or_truncated() {
        let dir = crate::test_tempdir();
        let path = dir.path().join("raw");
        let coordinate = RawSegment::open(&path)
            .unwrap()
            .append(b"original")
            .unwrap();
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(coordinate.offset)).unwrap();
        file.write_all(b"modified").unwrap();
        drop(file);
        let before = fs::read(&path).unwrap();
        assert_eq!(
            RawSegment::open(&path).err().unwrap().to_string(),
            "raw frame SHA mismatch"
        );
        assert_eq!(fs::read(&path).unwrap(), before);
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 2); // segment + frontier only
    }
}
