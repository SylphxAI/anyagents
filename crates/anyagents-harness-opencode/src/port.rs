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
        Self::acquire_with(config, avoid, FIRST..FIRST + COUNT, |port| {
            TcpListener::bind((Ipv4Addr::LOCALHOST, port))
        })
    }

    // Keep allocation decisions testable with fixture-owned reservations and
    // an ephemeral range, without adding a public runtime/test setting.
    fn acquire_with(
        config: Option<&Path>,
        avoid: Option<u16>,
        range: std::ops::Range<u16>,
        mut reserve: impl FnMut(u16) -> io::Result<TcpListener>,
    ) -> Result<Self, HarnessError> {
        let count = range.end - range.start;
        let stored = config.and_then(|dir| {
            std::fs::read_to_string(dir.join(FILENAME))
                .ok()?
                .trim()
                .parse::<u16>()
                .ok()
                .filter(|port| range.contains(port))
        });
        // Stable FNV-1a seed, independent of randomized HashMap state or PID.
        let seed = config
            .map(|dir| dir.as_os_str().as_encoded_bytes())
            .unwrap_or_default()
            .iter()
            .fold(2_166_136_261u32, |hash, byte| {
                (hash ^ u32::from(*byte)).wrapping_mul(16_777_619)
            });
        let first = stored.unwrap_or(range.start + (seed % u32::from(count)) as u16);
        let mut leased = LEASED.lock().unwrap_or_else(|err| err.into_inner());
        let ports = leased.get_or_insert_with(HashSet::new);
        for offset in 0..count {
            let port = range.start
                + ((u32::from(first - range.start) + u32::from(offset)) % u32::from(count)) as u16;
            if Some(port) == avoid || ports.contains(&port) {
                continue;
            }
            let reservation = match reserve(port) {
                Ok(listener) => listener,
                Err(err) if err.kind() == io::ErrorKind::AddrInUse => continue,
                Err(err) => {
                    return Err(HarnessError::Failed(format!("OpenCode port {port}: {err}")));
                }
            };
            if let Some(dir) = config {
                // One lifecycle writer owns each agent config. Atomic persistence avoids a
                // partial or empty number becoming the next restart's allocation.
                let path = dir.join(FILENAME);
                anyagents_harness::atomic_write_nonempty_file(
                    &path,
                    format!("{port}\n").as_bytes(),
                    0o664,
                )
                .map_err(failed)?;
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
            "no free OpenCode port in {}..{}",
            range.start, range.end
        )))
    }

    pub fn handoff(&mut self) {
        self.reservation.take();
    }

    pub fn release_stored(config: Option<&Path>) {
        let Some(port) = config.and_then(|dir| {
            std::fs::read_to_string(dir.join(FILENAME))
                .ok()?
                .trim()
                .parse::<u16>()
                .ok()
        }) else {
            return;
        };
        if let Some(ports) = LEASED
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .as_mut()
        {
            ports.remove(&port);
        }
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

    /// Provision three adjacent ports before exercising allocation. Every
    /// candidate stays owned throughout the test; successful reservations are
    /// handed directly to the allocator instead of racing a release/rebind.
    fn fixture_ports() -> [TcpListener; 3] {
        for _ in 0..100 {
            let first = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
            let port = first.local_addr().unwrap().port();
            if port > u16::MAX - 3 {
                continue;
            }
            let Ok(second) = TcpListener::bind((Ipv4Addr::LOCALHOST, port + 1)) else {
                continue;
            };
            let Ok(third) = TcpListener::bind((Ipv4Addr::LOCALHOST, port + 2)) else {
                continue;
            };
            return [first, second, third];
        }
        panic!("could not provision three adjacent ephemeral fixture ports");
    }

    fn persisted(config: &Path) -> u16 {
        std::fs::read_to_string(config.join(FILENAME))
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    }

    #[test]
    fn an_available_stored_port_is_reused_and_remains_reserved() {
        let dir = tempfile::tempdir().unwrap();
        let [first, _second, _third] = fixture_ports();
        let port = first.local_addr().unwrap().port();
        std::fs::write(dir.path().join(FILENAME), port.to_string()).unwrap();
        let mut available = Some(first);
        let lease = PortLease::acquire_with(Some(dir.path()), None, port..port + 3, |candidate| {
            assert_eq!(
                candidate, port,
                "the stored available candidate must be tried first"
            );
            Ok(available.take().unwrap())
        })
        .unwrap();
        assert_eq!(lease.port, port);
        assert_eq!(persisted(dir.path()), port);
        assert!(TcpListener::bind((Ipv4Addr::LOCALHOST, port)).is_err());
    }

    #[test]
    fn an_occupied_replacement_is_skipped_and_the_new_free_choice_is_persisted() {
        let dir = tempfile::tempdir().unwrap();
        let [occupied, next, third] = fixture_ports();
        let stored = occupied.local_addr().unwrap().port();
        let range = stored..stored + 3;
        std::fs::write(dir.path().join(FILENAME), stored.to_string()).unwrap();
        let mut available = Some(next);
        let mut tried = Vec::new();
        let mut lease =
            PortLease::acquire_with(Some(dir.path()), None, range.clone(), |candidate| {
                tried.push(candidate);
                if candidate == stored + 1 {
                    return Ok(available.take().unwrap());
                }
                TcpListener::bind((Ipv4Addr::LOCALHOST, candidate))
            })
            .unwrap();
        assert_eq!(tried, [stored, stored + 1]);
        let replacement = lease.port;
        assert_ne!(replacement, stored);
        assert_eq!(persisted(dir.path()), replacement);
        // Model the precise CI failure: after the service ends, an external
        // process owns its replacement. Keep the socket; release only the
        // allocator's process-local lease, then offer the third reservation.
        let _external_holder = lease.reservation.take().unwrap();
        drop(lease);
        let mut available = Some(third);
        let mut tried = Vec::new();
        let restarted = PortLease::acquire_with(Some(dir.path()), None, range, |candidate| {
            tried.push(candidate);
            if candidate == stored + 2 {
                return Ok(available.take().unwrap());
            }
            TcpListener::bind((Ipv4Addr::LOCALHOST, candidate))
        })
        .unwrap();
        assert_eq!(tried, [replacement, stored + 2]);
        assert_ne!(restarted.port, replacement);
        assert_eq!(persisted(dir.path()), restarted.port);
        assert!(TcpListener::bind((Ipv4Addr::LOCALHOST, restarted.port)).is_err());
    }

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
