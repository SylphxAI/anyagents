//! Host-owned learned memory. File edits are live for retrieval, never for an
//! already pinned session. Callers supply the authoritative host session ID.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{self, Read, Write},
    path::Path,
};

pub const MAX_BYTES: usize = 4096;
pub const MAX_LINES: usize = 150;
const MARKER: &str = "[Memory truncated; use conversation scope=memory for full text.]\n";
const FOOTER: &str = "</learned_memory>\n";

pub fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryPin {
    pub session_id: String,
    /// Digest of the exact bounded section, not of a subsequently edited file.
    pub sha256: String,
    pub section: String,
}

/// Complete section including its delimiters and visible truncation marker.
/// The digest lives alongside it, avoiding a self-referential section hash.
pub fn bounded_section(input: &str) -> String {
    const HEADER: &str = "<learned_memory>\n# Learned memory (session snapshot)\n";
    let base = HEADER.len() + FOOTER.len();
    let base_lines = HEADER.lines().count() + FOOTER.lines().count();
    let normalized = if input.is_empty() || input.ends_with('\n') {
        input.to_owned()
    } else {
        format!("{input}\n")
    };
    if base + normalized.len() <= MAX_BYTES && base_lines + normalized.lines().count() <= MAX_LINES
    {
        return format!("{HEADER}{normalized}{FOOTER}");
    }
    let available = MAX_BYTES - base - MARKER.len();
    // Reserve one line each for the marker and a cut body line.
    let lines = MAX_LINES - base_lines - 1;
    let mut end = normalized.len().min(available);
    while !normalized.is_char_boundary(end) {
        end -= 1;
    }
    let mut count = 0;
    for (i, b) in normalized.bytes().enumerate().take(end) {
        if b == b'\n' {
            count += 1;
            if count == lines {
                end = i + 1;
                break;
            }
        }
    }
    // A partial line needs its own newline, within the byte budget.
    if end > 0 && !normalized[..end].ends_with('\n') {
        while end + 1 > available {
            end -= 1;
            while !normalized.is_char_boundary(end) {
                end -= 1;
            }
        }
    }
    let body = &normalized[..end];
    let separator = if body.is_empty() || body.ends_with('\n') {
        ""
    } else {
        "\n"
    };
    format!("{HEADER}{body}{separator}{MARKER}{FOOTER}")
}

pub fn atomic_write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("missing parent"))?;
    fs::create_dir_all(parent)?;
    let temp = parent.join(format!(".memory-{}.tmp", uuid::Uuid::now_v7()));
    let result = (|| {
        let mut out = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)?;
        out.write_all(bytes)?;
        out.sync_all()?;
        fs::rename(&temp, path)?;
        fs::File::open(parent)?.sync_all()
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result
}

/// Read an existing pin without ever opening learned-memory source files.
/// A known session with a missing pin must fail, not silently repin live facts.
pub fn read_pin(agent_root: &Path, session_id: &str) -> io::Result<MemoryPin> {
    let path = agent_root
        .join("sessions")
        .join(sha256(session_id.as_bytes()))
        .join("memory-pin.json");
    let bytes = fs::read(&path)?;
    let pin: MemoryPin = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
    if pin.session_id != session_id
        || pin.sha256 != sha256(pin.section.as_bytes())
        || pin.section.len() > MAX_BYTES
        || pin.section.lines().count() > MAX_LINES
    {
        return Err(io::Error::other("invalid persisted memory pin"));
    }
    Ok(pin)
}

/// Must run serialized with host session creation. Repeated calls/restarts for
/// the same ID read only the persisted pin; they do not open MEMORY.md at all.
pub fn pin_session(agent_root: &Path, session_id: &str) -> io::Result<MemoryPin> {
    if session_id.trim().is_empty() {
        return Err(io::Error::other("empty host session identity"));
    }
    match read_pin(agent_root, session_id) {
        Ok(pin) => return Ok(pin),
        Err(e) if e.kind() == io::ErrorKind::NotFound => (),
        Err(e) => return Err(e),
    }
    let path = agent_root
        .join("sessions")
        .join(sha256(session_id.as_bytes()))
        .join("memory-pin.json");
    fs::create_dir_all(agent_root.join("memory"))?;
    // Only enough input for the bounded prompt. Full raw bytes remain on disk.
    let input = match fs::File::open(agent_root.join("memory/MEMORY.md")) {
        Ok(file) => {
            let mut bytes = Vec::new();
            file.take((MAX_BYTES + 4) as u64).read_to_end(&mut bytes)?;
            match std::str::from_utf8(&bytes) {
                Ok(text) => text.to_owned(),
                Err(error) if error.error_len().is_none() => {
                    std::str::from_utf8(&bytes[..error.valid_up_to()])
                        .unwrap()
                        .to_owned()
                }
                Err(error) => return Err(io::Error::new(io::ErrorKind::InvalidData, error)),
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e),
    };
    let section = bounded_section(&input);
    let pin = MemoryPin {
        session_id: session_id.into(),
        sha256: sha256(section.as_bytes()),
        section,
    };
    atomic_write(&path, &serde_json::to_vec(&pin).map_err(io::Error::other)?)?;
    Ok(pin)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cap_includes_wrappers_and_marker() {
        for input in [
            "x".repeat(20000),
            "字".repeat(20000),
            "abc\n".repeat(1000),
            format!("{}字", "x".repeat(4090)),
        ] {
            let section = bounded_section(&input);
            assert!(section.len() <= MAX_BYTES, "{}", section.len());
            assert!(section.lines().count() <= MAX_LINES);
            assert!(section.contains(MARKER));
            assert!(section.ends_with(FOOTER));
        }
    }
    #[test]
    fn exact_byte_boundary_is_not_truncated() {
        let overhead = bounded_section("").len();
        let text = format!("{}\n", "x".repeat(MAX_BYTES - overhead - 1));
        let section = bounded_section(&text);
        assert_eq!(section.len(), MAX_BYTES);
        assert!(!section.contains(MARKER));
        assert!(bounded_section(&(text + "x")).contains(MARKER));
    }
    #[test]
    fn exact_line_boundary_is_not_truncated() {
        let text = "x\n".repeat(MAX_LINES - bounded_section("").lines().count());
        assert_eq!(bounded_section(&text).lines().count(), MAX_LINES);
        assert!(!bounded_section(&text).contains(MARKER));
        assert!(bounded_section(&(text + "x\n")).contains(MARKER));
    }
    #[test]
    fn pin_survives_edits_restart_and_missing_source() {
        let root = crate::test_tempdir();
        fs::create_dir(root.path().join("memory")).unwrap();
        let source = root.path().join("memory/MEMORY.md");
        fs::write(&source, "old fact\n").unwrap();
        let original = pin_session(root.path(), "session-1").unwrap();
        fs::write(&source, "new fact\n").unwrap();
        assert_eq!(pin_session(root.path(), "session-1").unwrap(), original);
        let next = pin_session(root.path(), "session-2").unwrap();
        assert!(next.section.contains("new fact"));
        fs::remove_file(source).unwrap();
        assert_eq!(pin_session(root.path(), "session-1").unwrap(), original);
        assert_eq!(original.sha256, sha256(original.section.as_bytes()));
    }
    #[test]
    fn corrupt_pin_fails_instead_of_refreshing() {
        let root = crate::test_tempdir();
        pin_session(root.path(), "one").unwrap();
        let path = root
            .path()
            .join("sessions")
            .join(sha256(b"one"))
            .join("memory-pin.json");
        fs::write(path, b"{}").unwrap();
        assert!(pin_session(root.path(), "one").is_err());
    }
}
