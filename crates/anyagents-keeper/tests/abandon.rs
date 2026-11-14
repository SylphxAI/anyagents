//! A kernel that stops while a keeper is still starting does not wait for
//! it: the keeper finishes its start on its own, for the next kernel. Its own
//! test binary, because abandoning starts is for the whole process.
#![cfg(target_os = "linux")]

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyagents_keeper::{Program, Spec};
use serde_json::json;

#[test]
fn a_stopping_kernel_leaves_a_keeper_start_to_finish_on_its_own() {
    let base = if std::env::temp_dir().as_os_str().len() <= 40 {
        std::env::temp_dir()
    } else {
        PathBuf::from("/tmp")
    };
    let tmp = tempfile::Builder::new()
        .prefix("ka")
        .tempdir_in(base)
        .unwrap();
    let root = tmp.path().join("keepers");
    let program = Program {
        ready: Duration::from_secs(600),
        ..Program::new(PathBuf::from(env!("CARGO_BIN_EXE_kept_line")), Vec::new())
    };
    let spec = Spec {
        agent: "agt_test".into(),
        harness: "line".into(),
        model: None,
        seat: None,
        labels: Default::default(),
        lock_path: None,
        launch: json!({"hang_start": true}),
    };
    let starting = {
        let root = root.clone();
        std::thread::spawn(move || {
            let started = Instant::now();
            let outcome = anyagents_keeper::spawn(&program, &root, &spec, None).map(|_| ());
            (outcome, started.elapsed())
        })
    };
    // The keeper is started and its start is under way.
    let deadline = Instant::now() + Duration::from_secs(20);
    let dir = loop {
        let found = std::fs::read_dir(&root)
            .ok()
            .and_then(|mut entries| entries.next())
            .and_then(Result::ok)
            .map(|entry| entry.path())
            .filter(|dir| dir.join("spawned").exists());
        if let Some(dir) = found {
            break dir;
        }
        assert!(Instant::now() < deadline, "the keeper never started");
        std::thread::sleep(Duration::from_millis(20));
    };
    anyagents_keeper::abandon_starts();
    let (outcome, waited) = starting.join().unwrap();
    let refused = outcome.expect_err("a start went on after the kernel stopped");
    assert!(refused.contains("stopping"), "{refused}");
    assert!(waited < Duration::from_secs(20), "waited {waited:?}");
    // Nothing was ended or cleared: the keeper goes on starting.
    let spawned: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.join("spawned")).unwrap()).unwrap();
    let pid = spawned["keeper"]["pid"].as_u64().unwrap() as i32;
    // SAFETY: signal 0 only probes for the process.
    assert_eq!(unsafe { libc::kill(pid, 0) }, 0, "the keeper was ended");
    assert!(dir.exists());
    anyagents_keeper::stop_agent_with(&root, "agt_test", true).unwrap();
}
