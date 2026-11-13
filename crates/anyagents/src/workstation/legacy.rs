//! Retire state the previous browser implementation left in a profile.
//!
//! It ran each profile's browser as a detached process recorded in
//! `runtime.json`, which outlives a server restart and would hold the
//! profile's lock. Before a session opens a profile, that process is stopped
//! when, and only when, it is still alive and its command line names this
//! profile's `user-data` directory; then the old bookkeeping files go.
//! `user-data/`, `fingerprint.json` and `screenshots/` are never touched.

use std::path::Path;
use std::time::Duration;

const STATE_FILES: [&str; 6] = [
    "runtime.json",
    "refs.json",
    "held_keys.json",
    "takeover.json",
    "takeover-mouse.json",
    "takeover.lock",
];

#[derive(serde::Deserialize)]
struct Runtime {
    pid: u32,
}

/// Stop the old browser for `dir` (if it still runs) and remove the old
/// bookkeeping files. Unreadable state is left in place.
pub fn retire(dir: &Path) {
    let runtime = dir.join("runtime.json");
    if let Ok(bytes) = std::fs::read(&runtime)
        && let Ok(rt) = serde_json::from_slice::<Runtime>(&bytes)
        && owns_profile(rt.pid, &dir.join("user-data"))
    {
        stop_group(rt.pid);
    }
    for name in STATE_FILES {
        let path = dir.join(name);
        if path.is_file() {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Whether `pid` is alive and was started for `user_data_dir`.
fn owns_profile(pid: u32, user_data_dir: &Path) -> bool {
    if pid == 0 {
        return false;
    }
    let Ok(cmdline) = std::fs::read(format!("/proc/{pid}/cmdline")) else {
        return false;
    };
    let want = format!("--user-data-dir={}", user_data_dir.display());
    cmdline.split(|b| *b == 0).any(|arg| arg == want.as_bytes())
}

fn stop_group(pid: u32) {
    let Ok(pid) = i32::try_from(pid) else {
        return;
    };
    // The old browser was its own session and process-group leader.
    // SAFETY: kill(2) with a negative pid signals that process group only.
    unsafe {
        libc::kill(-pid, libc::SIGTERM);
    }
    for _ in 0..50 {
        if !Path::new(&format!("/proc/{pid}")).exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    // SAFETY: as above.
    unsafe {
        libc::kill(-pid, libc::SIGKILL);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retires_only_the_process_started_for_this_profile() {
        let dir = tempfile::tempdir().unwrap();
        let user_data = dir.path().join("user-data");
        std::fs::create_dir_all(&user_data).unwrap();
        std::fs::write(user_data.join("Cookies"), b"keep").unwrap();
        std::fs::write(dir.path().join("fingerprint.json"), b"{}").unwrap();

        // A stand-in for the old detached browser: its own process group,
        // with this profile's user-data flag on its command line.
        let mut ours = {
            use std::os::unix::process::CommandExt as _;
            std::process::Command::new("/bin/sh")
                .args(["-c", "sleep 30; :", "sh"])
                .arg(format!("--user-data-dir={}", user_data.display()))
                .process_group(0)
                .spawn()
                .unwrap()
        };
        // Not ours: a live process whose pid the file might name after reuse.
        let mut other = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap();

        std::fs::write(
            dir.path().join("runtime.json"),
            format!(r#"{{"pid": {}}}"#, other.id()),
        )
        .unwrap();
        retire(dir.path());
        assert!(
            other.try_wait().unwrap().is_none(),
            "unrelated process untouched"
        );
        assert!(!dir.path().join("runtime.json").exists());

        std::fs::write(
            dir.path().join("runtime.json"),
            format!(r#"{{"pid": {}}}"#, ours.id()),
        )
        .unwrap();
        std::fs::write(dir.path().join("held_keys.json"), b"[]").unwrap();
        retire(dir.path());
        assert!(ours.wait().is_ok());
        for name in STATE_FILES {
            assert!(!dir.path().join(name).exists(), "{name}");
        }
        assert_eq!(std::fs::read(user_data.join("Cookies")).unwrap(), b"keep");
        assert!(dir.path().join("fingerprint.json").exists());
        let _ = other.kill();
        let _ = other.wait();
    }
}
