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
    Admin(
        command(data, home)
            .arg("serve-admin")
            .stdout(std::process::Stdio::null())
            .stderr(std::fs::File::create(stderr).unwrap())
            .spawn()
            .unwrap(),
    )
}

#[test]
fn production_maintenance_reaps_legacy_archive_once_across_restart() {
    use std::time::{Duration, Instant};
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
    let stderr = root.path().join("admin.stderr");
    let mut first = admin(&data, &home, &stderr);
    let deadline = Instant::now() + Duration::from_secs(5);
    while !cfg.join(".anyagents-reaped").exists() {
        assert!(
            first.0.try_wait().unwrap().is_none(),
            "fixture admin exited: {}",
            std::fs::read_to_string(&stderr).unwrap()
        );
        assert!(
            Instant::now() < deadline,
            "archive maintenance did not complete: {}",
            std::fs::read_to_string(&stderr).unwrap()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
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
    let deadline = Instant::now() + Duration::from_secs(5);
    while !second_cfg.join(".anyagents-reaped").exists() {
        assert!(
            restarted.0.try_wait().unwrap().is_none(),
            "restarted admin exited"
        );
        assert!(
            Instant::now() < deadline,
            "restarted maintenance did not run"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        cfg.join("shell-snapshots").exists(),
        "completed archive was reaped again"
    );
    assert_eq!(
        std::fs::read(cfg.join(".anyagents-reaped")).unwrap(),
        generation
    );
}
