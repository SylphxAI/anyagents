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
