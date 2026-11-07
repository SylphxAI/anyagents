//! Bounded stderr evidence with a closed, secret-safe public projection.
//! Vendor stderr may contain credentials from files, not merely environment
//! values. Never publish its text, paths, argv, or an inferred authentication
//! state. Safe hints are observations about fixed byte strings, not causes.
use std::io::Read;
use std::process::{ChildStderr, ExitStatus};
use std::sync::{Arc, Condvar, Mutex};

const LIMIT: usize = 4096;

#[derive(Default)]
pub(super) struct Evidence {
    tail: Vec<u8>,
    bytes: u64,
    complete: bool,
    hints: u8,
    pub status: Option<ExitStatus>,
}

impl Evidence {
    fn accept(&mut self, bytes: &[u8]) {
        self.bytes = self.bytes.saturating_add(bytes.len() as u64);
        // A complete logical diagnostic can span arbitrary pipe reads. Keep an
        // overlap larger than every recognized phrase; never truncate before
        // classification. Neither this bounded buffer nor raw text is exposed.
        let mut scan = self.tail[self.tail.len().saturating_sub(64)..].to_vec();
        scan.extend_from_slice(bytes);
        scan.make_ascii_lowercase();
        for (bit, phrase) in [
            (1, b"no such file or directory".as_slice()),
            (2, b"permission denied".as_slice()),
            (4, b"exec format error".as_slice()),
            (8, b"not logged in".as_slice()),
            (16, b"invalid api key".as_slice()),
        ] {
            if scan.windows(phrase.len()).any(|window| window == phrase) {
                self.hints |= bit;
            }
        }
        self.tail.extend_from_slice(bytes);
        if self.tail.len() > LIMIT {
            self.tail.drain(..self.tail.len() - LIMIT);
        }
    }

    pub fn projection(&self) -> String {
        #[cfg(unix)]
        use std::os::unix::process::ExitStatusExt as _;
        let code = self.status.as_ref().and_then(ExitStatus::code);
        #[cfg(unix)]
        let signal = self.status.as_ref().and_then(ExitStatus::signal);
        #[cfg(not(unix))]
        let signal: Option<i32> = None;
        let hints = [
            (1, "mentions_missing_file"),
            (2, "mentions_permission_denied"),
            (4, "mentions_exec_format"),
            (8, "mentions_not_logged_in"),
            (16, "mentions_invalid_api_key"),
        ]
        .into_iter()
        .filter_map(|(bit, name)| (self.hints & bit != 0).then_some(name))
        .collect::<Vec<_>>()
        .join(",");
        format!(
            "Claude child stopped: exit_code={}; signal={}; stderr_bytes={}; stderr_complete={}; stderr_hints={}; raw_stderr=withheld",
            code.map_or_else(|| "unknown".into(), |n| n.to_string()),
            signal.map_or_else(|| "none".into(), |n| n.to_string()),
            self.bytes,
            self.complete,
            if hints.is_empty() {
                "unclassified"
            } else {
                &hints
            },
        )
    }
}

pub(super) fn drain(mut stderr: ChildStderr, shared: Arc<Mutex<super::State>>, wake: Arc<Condvar>) {
    let mut buffer = [0; LIMIT];
    loop {
        match stderr.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => super::lock(&shared).stderr.accept(&buffer[..count]),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return, // Preserve incomplete evidence, never replace the process outcome.
        }
    }
    {
        let mut state = super::lock(&shared);
        state.stderr.complete = true;
        state.epoch = state.epoch.wrapping_add(1);
    }
    wake.notify_all();
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn split_and_oversized_secret_output_has_only_a_closed_bounded_projection() {
        let mut evidence = Evidence::default();
        evidence.accept(b"sentinel-password=never-publish\0\xff Permis");
        evidence.accept(b"sion denied\n");
        for _ in 0..100 {
            evidence.accept(&[b'x'; 4096]);
        }
        let text = evidence.projection();
        assert!(text.contains("mentions_permission_denied"));
        assert!(text.contains("raw_stderr=withheld"));
        assert!(
            !text.contains("sentinel") && !text.contains("never-publish") && !text.contains("xxx")
        );
        assert_eq!(evidence.tail.len(), LIMIT);
        assert!(text.len() < 300);
    }
}
