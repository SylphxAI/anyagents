//! Exercise the real child bind failure, retry and persistence without racing
//! to rebind a port that the allocator has already released to other processes.

use super::*;
use std::net::{Ipv4Addr, TcpListener};
use tempfile::TempDir;

fn scripted_opencode() -> PathBuf {
    let exe = std::env::current_exe().expect("the test binary path");
    let deps = exe.parent().expect("a directory");
    deps.parent()
        .map(|dir| dir.join("fake_opencode"))
        .filter(|path| path.is_file())
        .expect("the scripted opencode is built with these tests")
}

fn persisted(config: &Path) -> u16 {
    std::fs::read_to_string(config.join("opencode-service-port"))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

fn wait_for(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !path.exists() {
        assert!(Instant::now() < deadline, "barrier {}", path.display());
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn collision_is_retried(mode: &str, release_before_exit: bool) {
    let dir = TempDir::new().unwrap();
    let config = dir.path().join("config");
    let log = dir.path().join("log");
    let reservation = loop {
        let socket = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        if socket.local_addr().unwrap().port() < u16::MAX {
            break socket;
        }
    };
    let first_port = reservation.local_addr().unwrap().port();
    // Both handles refer to one already-owned socket. The allocator receives
    // its reservation, while the fixture remains the competing holder across
    // handoff. There is no release/rebind window in fixture setup.
    let occupied = reservation.try_clone().unwrap();
    let mut available = Some(reservation);
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(config.join("opencode-service-port"), first_port.to_string()).unwrap();
    std::thread::scope(|scope| {
        let launch = scope.spawn(|| {
            let mut allocations = 0;
            OpenCodeHarness::start_with(
                scripted_opencode(),
                Some("ses_fake"),
                &[
                    ("ANYAGENTS_FAKE_MODE", mode),
                    ("ANYAGENTS_FAKE_LOG", log.to_str().unwrap()),
                    ("ANYAGENTS_FAKE_STATE", dir.path().to_str().unwrap()),
                ],
                None,
                Some(&config),
                |config, avoid| {
                    allocations += 1;
                    if allocations == 1 {
                        assert_eq!(avoid, None);
                        return port::PortLease::acquire_with(
                            config,
                            avoid,
                            first_port..first_port + 1,
                            |candidate| {
                                assert_eq!(candidate, first_port);
                                Ok(available.take().unwrap())
                            },
                        );
                    }
                    if allocations == 2 {
                        assert_eq!(avoid, Some(first_port), "retry must avoid the failed port");
                    }
                    port::PortLease::acquire(config, avoid)
                },
            )
            .unwrap_or_else(|error| {
                panic!(
                    "{error}: {}",
                    std::fs::read_to_string(&log).unwrap_or_default()
                )
            })
        });
        wait_for(&dir.path().join("first-serve-port"));
        assert_eq!(
            std::fs::read_to_string(dir.path().join("first-serve-port"))
                .unwrap()
                .parse::<u16>()
                .unwrap(),
            first_port
        );
        std::fs::write(dir.path().join("collision-bound"), "bound").unwrap();
        let holder = if release_before_exit {
            wait_for(&dir.path().join("collision-reported"));
            drop(occupied);
            // The child cannot exit before the holder is gone. Diagnosis must
            // retain the child's actual collision, not rely on a later probe.
            std::fs::write(dir.path().join("collision-released"), "released").unwrap();
            None
        } else {
            Some(occupied)
        };
        let harness = launch.join().unwrap();
        let replacement = persisted(&config);
        let text = std::fs::read_to_string(&log).unwrap();
        assert!(
            text.contains(&format!("bind failed:{first_port}:")),
            "{text}"
        );
        assert_ne!(replacement, first_port);
        assert!(text.matches("serve:").count() >= 2, "{text}");
        assert!(
            text.contains(&format!("server:http://127.0.0.1:{replacement}")),
            "{text}"
        );
        assert!(TcpListener::bind((Ipv4Addr::LOCALHOST, replacement)).is_err());
        drop(harness);
        drop(holder);
    });
}

#[test]
fn a_collision_during_socket_handoff_retries_and_persists_the_new_port() {
    collision_is_retried("bind-race", false);
}

#[test]
fn a_transient_bind_collision_is_retried_even_after_the_holder_releases_it() {
    collision_is_retried("transient-bind-race", true);
}
