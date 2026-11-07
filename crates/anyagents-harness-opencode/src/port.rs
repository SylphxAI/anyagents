//! Ports belong to an agent config, not OpenCode's machine-wide default.

use std::collections::HashSet;
use std::io;
use std::net::{Ipv4Addr, TcpListener};
use std::path::Path;
use std::sync::Mutex;

use anyagents_harness::HarnessError;

const FIRST: u16 = 20_000;
const COUNT: u16 = 10_000;
const FILENAME: &str = "opencode-service-port";
static LEASED: Mutex<Option<HashSet<u16>>> = Mutex::new(None);

/// A socket reserves the port until spawn; the process-local lease prevents
/// another agent selecting it while the child is binding or still starting.
pub(super) struct PortLease {
    pub port: u16,
    reservation: Option<TcpListener>,
}

impl PortLease {
    pub fn acquire(config: Option<&Path>, avoid: Option<u16>) -> Result<Self, HarnessError> {
        let stored = config.and_then(|dir| {
            std::fs::read_to_string(dir.join(FILENAME))
                .ok()?
                .trim()
                .parse::<u16>()
                .ok()
                .filter(|port| (FIRST..FIRST + COUNT).contains(port))
        });
        // Stable FNV-1a seed, independent of randomized HashMap state or PID.
        let seed = config
            .map(|dir| dir.as_os_str().as_encoded_bytes())
            .unwrap_or_default()
            .iter()
            .fold(2_166_136_261u32, |hash, byte| {
                (hash ^ u32::from(*byte)).wrapping_mul(16_777_619)
            });
        let first = stored.unwrap_or(FIRST + (seed % u32::from(COUNT)) as u16);
        let mut leased = LEASED.lock().unwrap_or_else(|err| err.into_inner());
        let ports = leased.get_or_insert_with(HashSet::new);
        for offset in 0..COUNT {
            let port = FIRST + (first - FIRST + offset) % COUNT;
            if Some(port) == avoid || ports.contains(&port) {
                continue;
            }
            let reservation = match TcpListener::bind((Ipv4Addr::LOCALHOST, port)) {
                Ok(listener) => listener,
                Err(err) if err.kind() == io::ErrorKind::AddrInUse => continue,
                Err(err) => {
                    return Err(HarnessError::Failed(format!("OpenCode port {port}: {err}")));
                }
            };
            if let Some(dir) = config {
                std::fs::create_dir_all(dir).map_err(failed)?;
                // One lifecycle writer owns each agent config. Rename avoids a
                // partial number becoming the next restart's allocation.
                let path = dir.join(FILENAME);
                let temp = dir.join(format!(".{FILENAME}.{}.new", std::process::id()));
                std::fs::write(&temp, format!("{port}\n")).map_err(failed)?;
                std::fs::rename(&temp, &path).map_err(failed)?;
                if let Some(previous) = stored.or(avoid).filter(|previous| *previous != port) {
                    eprintln!(
                        "anyagents-opencode: port {previous} is occupied; persisted port {port} in {}",
                        path.display()
                    );
                }
            }
            ports.insert(port);
            return Ok(Self {
                port,
                reservation: Some(reservation),
            });
        }
        Err(HarnessError::Failed(format!(
            "no free OpenCode port in {FIRST}..{}",
            FIRST + COUNT
        )))
    }

    pub fn handoff(&mut self) {
        self.reservation.take();
    }
}

impl Drop for PortLease {
    fn drop(&mut self) {
        if let Some(ports) = LEASED
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .as_mut()
        {
            ports.remove(&self.port);
        }
    }
}

fn failed(err: io::Error) -> HarnessError {
    HarnessError::Failed(format!("persist OpenCode port: {err}"))
}

/// Drain stderr without retaining or publishing vendor credentials. Keep only a
/// bounded overlap so a diagnostic split across reads is still recognized.
pub(super) fn watch_collision(
    mut stderr: impl io::Read + Send + 'static,
) -> std::sync::mpsc::Receiver<bool> {
    let (events, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut buffer = [0; 1024];
        let mut tail = Vec::new();
        let mut reported = false;
        loop {
            let count = match stderr.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => count,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            };
            tail.extend_from_slice(&buffer[..count]);
            let text = String::from_utf8_lossy(&tail).to_ascii_lowercase();
            if !reported
                && ["address already in use", "eaddrinuse", "addrinuse"]
                    .iter()
                    .any(|marker| text.contains(marker))
            {
                let _ = events.send(true);
                reported = true;
            }
            if tail.len() > 128 {
                tail.drain(..tail.len() - 128);
            }
        }
        if !reported {
            let _ = events.send(false);
        }
    });
    receiver
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::time::Duration;

    #[test]
    fn collision_markers_survive_read_boundaries_without_forwarding_stderr() {
        for marker in ["Address already in use", "EADDRINUSE", "AddrInUse"] {
            let bytes = format!("{}{}{}", "x".repeat(1020), marker, "y".repeat(4096));
            let events = watch_collision(Cursor::new(bytes));
            assert_eq!(events.recv_timeout(Duration::from_secs(5)), Ok(true));
            assert!(events.recv_timeout(Duration::from_secs(5)).is_err());
        }
    }

    #[test]
    fn unrelated_startup_errors_do_not_report_a_collision() {
        let events = watch_collision(Cursor::new("configuration rejected\n".repeat(1024)));
        assert_eq!(events.recv_timeout(Duration::from_secs(5)), Ok(false));
    }
}
