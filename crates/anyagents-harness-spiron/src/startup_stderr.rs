//! Bounded diagnostics for a listening process that dies before attach.
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;
use std::process::ExitStatus;

use anyagents_harness::masking::mask_with_secrets;

use crate::ListenError;

const MAX_LINES: usize = 20;
const MAX_BYTES: usize = 2048;
// Read some context before the excerpt so a credential's label or an ANSI
// sequence isn't cut off at the display limit before we can sanitize it.
const READ_BYTES: usize = 16 * 1024;

pub(super) fn open_log(path: &Path, read: bool) -> Result<File, String> {
    OpenOptions::new()
        .read(read)
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .map_err(|err| format!("cannot open {}: {err}", path.display()))
}

pub(super) fn exit_before_listen(
    status: ExitStatus,
    stderr: &File,
    environment: &[(String, String)],
) -> ListenError {
    let mut error = format!("spiron exited with {status} before it listened");
    if let Ok(tail) = read_tail(stderr, environment)
        && !tail.is_empty()
    {
        error.push_str(" (stderr tail: ");
        error.push_str(&tail);
        error.push(')');
    }
    ListenError::Failed(error)
}

fn read_tail(file: &File, environment: &[(String, String)]) -> io::Result<String> {
    let len = file.metadata()?.len();
    // Read one extra byte to tell if the window starts at a line boundary.
    let start = len.saturating_sub(READ_BYTES as u64 + 1);
    let mut bytes = [0; READ_BYTES + 1];
    let bytes = &mut bytes[..(len - start) as usize];
    file.read_exact_at(bytes, start)?;
    let bytes = if start > 0 {
        // Never emit the suffix of a cut line: its credential label or token
        // prefix may be outside the window.
        bytes
            .iter()
            .position(|&b| b == b'\n')
            .map_or(&[][..], |end| &bytes[end + 1..])
    } else {
        &bytes[..]
    };
    let text = String::from_utf8_lossy(bytes);
    let secrets: Vec<_> = environment
        .iter()
        .map(|(_, value)| value.as_str())
        .collect();
    // Mask the original as well as the displayed text: normalization can
    // either change an exact secret or expose a colorized credential label.
    let masked = mask_with_secrets(&text, &secrets);
    let cleaned = sanitize(&masked);
    let masked = mask_with_secrets(&cleaned, &secrets);
    Ok(bounded_excerpt(&masked))
}

fn sanitize(text: &str) -> String {
    let mut cleaned = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' && chars.next_if_eq(&'[').is_some() {
            // CSI (e.g. colored panic output): consume through the final byte.
            while chars.next_if(|c| (' '..='?').contains(c)).is_some() {}
            let _ = chars.next_if(|c| ('@'..='~').contains(c));
        } else if ch == '\t' {
            cleaned.push(' ');
        } else if ch == '\n' || !ch.is_control() {
            cleaned.push(ch);
        }
    }
    cleaned
}

fn bounded_excerpt(text: &str) -> String {
    let mut lines = Vec::new();
    let mut remaining = MAX_BYTES;
    for line in text.lines().rev().map(str::trim).filter(|s| !s.is_empty()) {
        if lines.len() == MAX_LINES || remaining == 0 {
            break;
        }
        let start = line.ceil_char_boundary(line.len().saturating_sub(remaining));
        if start < line.len() {
            lines.push(&line[start..]);
        }
        if start > 0 {
            break;
        }
        remaining = remaining.saturating_sub(line.len() + 1);
    }
    lines.reverse();
    lines.join("\n")
}
