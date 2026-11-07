//! Production-path archive cleanup with a synthetic host home, never host credentials.

use std::path::Path;
use std::process::Command;

fn command(data: &Path, home: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_anyagents"));
    command
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", home)
        .env("ANYAGENTS_HOST_HOME", home)
        .arg("--data")
        .arg(data);
    command
}

#[test]
fn production_archive_reap_does_not_recreate_seat_directories() {
    let root = tempfile::tempdir().unwrap();
    let data = root.path().join("data");
    let home = root.path().join("home");
    std::fs::create_dir_all(data.join("skills")).unwrap();
    std::fs::create_dir_all(&home).unwrap();
    let store = anyagents_core::Store::open(data.join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
        .unwrap();
    let mut new = anyagents_core::NewAgent::new("worker", "opencode");
    new.parent_id = Some(parent.id);
    let child = store.create_agent(new).unwrap();
    let cfg = anyagents_core::agent_config_dir(&data, &child.id);
    std::fs::create_dir_all(cfg.join(".claude")).unwrap();
    std::fs::create_dir_all(cfg.join(".gemini")).unwrap();
    let archived = command(&data, &home)
        .args(["agents", "archive", "worker"])
        .output()
        .unwrap();
    assert!(
        archived.status.success(),
        "archive: {}",
        String::from_utf8_lossy(&archived.stderr)
    );
    assert_eq!(store.agent(&child.id).unwrap().state, "archived");
    for rel in [".claude", ".gemini", "cache-home", "tmp"] {
        assert!(!cfg.join(rel).exists(), "archive reap recreated {rel}");
    }
    let archive_generation = std::fs::read(cfg.join(".anyagents-archived")).unwrap();
    let reaped_generation = std::fs::read(cfg.join(".anyagents-reaped")).unwrap();
    assert_eq!(reaped_generation, archive_generation);
    let work = anyagents_core::agent_work_dir(&data, &child.id);
    assert!(!work.exists(), "archive recreated an empty workdir");
    let woken = command(&data, &home)
        .args(["agents", "wake", "worker"])
        .output()
        .unwrap();
    assert!(
        woken.status.success(),
        "wake: {}",
        String::from_utf8_lossy(&woken.stderr)
    );
    assert_eq!(store.agent(&child.id).unwrap().state, "running");
    assert!(work.is_dir(), "wake did not prepare the reaped workdir");
    assert!(cfg.join(".claude/skills").is_symlink());
    assert!(cfg.join(".gemini/config/skills").is_symlink());
    assert!(!cfg.join(".anyagents-archived").exists());
    assert!(!cfg.join(".anyagents-reaped").exists());
}

struct Admin(std::process::Child);

impl Drop for Admin {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn admin(data: &Path, home: &Path, stderr: &Path) -> Admin {
    let mut admin = Admin(
        command(data, home)
            .arg("serve-admin")
            .stdout(std::process::Stdio::null())
            .stderr(std::fs::File::create(stderr).unwrap())
            .spawn()
            .unwrap(),
    );
    // Hashing the cold debug executable is startup work, not maintenance.
    // Give it its own readiness deadline; keep the five-second first-pass
    // maintenance assertion below unchanged.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while std::os::unix::net::UnixStream::connect(data.join("admin.sock")).is_err() {
        assert!(
            admin.0.try_wait().unwrap().is_none(),
            "admin exited: {}",
            std::fs::read_to_string(stderr).unwrap()
        );
        assert!(
            std::time::Instant::now() < deadline,
            "admin did not bind: {}",
            std::fs::read_to_string(stderr).unwrap()
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    admin
}

fn wait_for_reap(admin: &mut Admin, cfg: &Path, stderr: &Path, stage: &str) {
    use std::time::{Duration, Instant};
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let completed = std::fs::read(cfg.join(".anyagents-archived"))
            .ok()
            .filter(|generation| !generation.is_empty())
            .is_some_and(|generation| {
                std::fs::read(cfg.join(".anyagents-reaped")).is_ok_and(|done| done == generation)
            });
        if completed {
            return;
        }
        assert!(
            admin.0.try_wait().unwrap().is_none(),
            "{stage} admin exited: {}",
            std::fs::read_to_string(stderr).unwrap()
        );
        assert!(
            Instant::now() < deadline,
            "{stage} maintenance did not complete: {}",
            std::fs::read_to_string(stderr).unwrap()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn production_maintenance_reaps_legacy_archive_once_across_restart() {
    let root = tempfile::tempdir().unwrap();
    let data = root.path().join("data");
    let home = root.path().join("home");
    std::fs::create_dir_all(data.join("skills")).unwrap();
    std::fs::create_dir_all(&home).unwrap();
    let store = anyagents_core::Store::open(data.join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
        .unwrap();
    let mut new = anyagents_core::NewAgent::new("worker", "opencode");
    new.parent_id = Some(parent.id);
    let child = store.create_agent(new).unwrap();
    // Legacy row: no explicit archive marker. Start the real production worker,
    // not the unit-test version where host-home/maintenance paths are disabled.
    store.set_agent_state(&child.id, "archived").unwrap();
    let cfg = anyagents_core::agent_config_dir(&data, &child.id);
    std::fs::create_dir_all(cfg.join(".claude")).unwrap();
    std::fs::create_dir_all(cfg.join(".gemini")).unwrap();
    // Both workers tick at startup. Reconciliation takes parent then child,
    // while maintenance takes only the child. Exclude the unrelated reconciler
    // so a valid busy-child skip (next maintenance pass: 60s) cannot race this
    // fixture's 5s first-pass assertion, on either initial start or restart.
    let _reconcile_parent = store
        .lock_agent_lifecycle(child.parent_id.as_ref().unwrap())
        .unwrap();
    let stderr = root.path().join("admin.stderr");
    let mut first = admin(&data, &home, &stderr);
    wait_for_reap(&mut first, &cfg, &stderr, "initial");
    assert!(!cfg.join(".claude").exists());
    assert!(!cfg.join(".gemini").exists());
    let generation = std::fs::read(cfg.join(".anyagents-archived")).unwrap();
    assert!(!generation.is_empty());
    assert_eq!(
        std::fs::read(cfg.join(".anyagents-reaped")).unwrap(),
        generation
    );
    drop(first);
    // A second pending legacy row proves the restarted worker has completed a
    // real pass. A sentinel left under the completed row must not be reaped.
    std::fs::create_dir_all(cfg.join("shell-snapshots")).unwrap();
    let mut new = anyagents_core::NewAgent::new("second-worker", "opencode");
    new.parent_id = child.parent_id.clone();
    let second = store.create_agent(new).unwrap();
    store.set_agent_state(&second.id, "archived").unwrap();
    let second_cfg = anyagents_core::agent_config_dir(&data, &second.id);
    let mut restarted = admin(&data, &home, &stderr);
    wait_for_reap(&mut restarted, &second_cfg, &stderr, "restarted");
    assert!(
        cfg.join("shell-snapshots").exists(),
        "completed archive was reaped again"
    );
    assert_eq!(
        std::fs::read(cfg.join(".anyagents-reaped")).unwrap(),
        generation
    );
}

#[test]
fn production_maintenance_skips_busy_archive_then_reaps_it_on_restart() {
    let root = tempfile::tempdir().unwrap();
    let data = root.path().join("data");
    let home = root.path().join("home");
    std::fs::create_dir_all(data.join("skills")).unwrap();
    std::fs::create_dir_all(&home).unwrap();
    let store = anyagents_core::Store::open(data.join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
        .unwrap();
    let mut new = anyagents_core::NewAgent::new("busy-worker", "opencode");
    new.parent_id = Some(parent.id.clone());
    let busy = store.create_agent(new).unwrap();
    let mut new = anyagents_core::NewAgent::new("canary", "opencode");
    new.parent_id = Some(parent.id.clone());
    let canary = store.create_agent(new).unwrap();
    for child in [&busy, &canary] {
        store.set_agent_state(&child.id, "archived").unwrap();
    }
    // Keep reconciliation from competing for either child, then force only
    // the busy child's maintenance lock to fail on the initial startup pass.
    let _parent = store.lock_agent_lifecycle(&parent.id).unwrap();
    let busy_lock = store.lock_agent_lifecycle(&busy.id).unwrap();
    let cfg = anyagents_core::agent_config_dir(&data, &busy.id);
    std::fs::create_dir_all(cfg.join(".claude")).unwrap();
    let canary_cfg = anyagents_core::agent_config_dir(&data, &canary.id);
    let stderr = root.path().join("admin.stderr");
    let mut first = admin(&data, &home, &stderr);
    wait_for_reap(&mut first, &canary_cfg, &stderr, "canary");
    // Completion of the independent canary proves maintenance actually ran;
    // the busy archive may validly remain pending until the next 60s pass.
    assert!(!cfg.join(".anyagents-reaped").exists());
    assert!(cfg.join(".claude").exists());
    drop(first);
    drop(busy_lock);
    let mut restarted = admin(&data, &home, &stderr);
    wait_for_reap(&mut restarted, &cfg, &stderr, "unblocked restart");
    assert!(!cfg.join(".claude").exists());
}
