//! A process started while spawns are reported runs nothing until the
//! recorder answers that its group is recorded. Its own test binary: the
//! reporting is process-wide.

#![cfg(target_os = "linux")]

use std::os::fd::IntoRawFd as _;
use std::path::Path;
use std::time::Duration;

#[test]
fn a_reported_process_runs_nothing_until_its_group_is_recorded() {
    let (mut reports, writer) = std::io::pipe().unwrap();
    anyagents_harness::report_spawns_to(writer.into_raw_fd());
    let dir = tempfile::tempdir().unwrap();
    let start = |marker: &Path| {
        let mut command = anyagents_harness::spawn_command(Path::new("/bin/sh"), &[]).unwrap();
        command.args(["-c", &format!("touch '{}'", marker.display())]);
        std::thread::spawn(move || command.spawn().and_then(|mut child| child.wait()))
    };

    let ran = dir.path().join("ran");
    let spawning = start(&ran);
    let (pid, token) = anyagents_harness::read_spawn(&mut reports).unwrap();
    assert!(pid > 0);
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        !ran.exists(),
        "the process ran before its group was recorded"
    );
    assert!(!spawning.is_finished());
    anyagents_harness::answer_spawn(token, true);
    assert!(spawning.join().unwrap().unwrap().success());
    assert!(ran.exists());

    // Refused (its group could not be recorded): it never runs.
    let refused = dir.path().join("refused");
    let spawning = start(&refused);
    let (_, token) = anyagents_harness::read_spawn(&mut reports).unwrap();
    anyagents_harness::answer_spawn(token, false);
    assert!(spawning.join().unwrap().is_err());
    assert!(!refused.exists());
}
