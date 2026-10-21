//! A restart keeps the conversation on Codex.
//!
//! A new server builds the adapter again and resumes the thread id kept beside
//! the agent's config. Nothing the first adapter had in memory survives the
//! restart, so the second start has only the stored id to pass back to the
//! vendor. These tests take the step a new server takes at startup, against the
//! vendor double, which writes down every thread it was asked to resume and
//! every one it refused.

use std::path::PathBuf;

use anyagents_harness_codex::CodexHarness;
use tempfile::TempDir;

/// The vendor double's files: its log and its refusal list.
struct Files {
    log: String,
    refuse: String,
}

impl Files {
    fn new(dir: &TempDir) -> Self {
        Self {
            log: dir.path().join("log").display().to_string(),
            refuse: dir.path().join("refuse").display().to_string(),
        }
    }

    fn pairs(&self) -> Vec<(&'static str, &str)> {
        vec![
            ("ANYAGENTS_FAKE_LOG", self.log.as_str()),
            ("ANYAGENTS_VENDOR_REFUSE", self.refuse.as_str()),
        ]
    }

    /// One of the files the double writes beside its log.
    fn side(&self, name: &str) -> Vec<String> {
        let path = PathBuf::from(&self.log);
        std::fs::read_to_string(path.with_file_name(name))
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }
}

#[test]
fn a_codex_restart_resumes_the_thread_the_vendor_reported() {
    let dir = TempDir::new().unwrap();
    let files = Files::new(&dir);
    let first = CodexHarness::start_after_restart(
        env!("CARGO_BIN_EXE_restart-codex"),
        None,
        &files.pairs(),
        None,
    )
    .expect("first start");
    let saved = first.session_id().expect("the vendor named a thread");
    assert_eq!(saved, "sess-vendor-double");
    drop(first);

    let second = CodexHarness::start_after_restart(
        env!("CARGO_BIN_EXE_restart-codex"),
        Some(&saved),
        &files.pairs(),
        None,
    )
    .expect("restart");
    assert!(
        files.side("resumed").contains(&saved),
        "the restart resumes {saved}: the vendor saw {:?}",
        files.side("resumed")
    );
    assert_eq!(second.session_id().as_deref(), Some(saved.as_str()));
    drop(second);
}

#[test]
fn a_codex_restart_whose_thread_the_vendor_refuses_starts_a_new_one() {
    let dir = TempDir::new().unwrap();
    let files = Files::new(&dir);
    let gone = "gone-thread";
    std::fs::write(&files.refuse, format!("{gone}\n")).unwrap();
    let resumed = CodexHarness::start_after_restart(
        env!("CARGO_BIN_EXE_restart-codex"),
        Some(gone),
        &files.pairs(),
        None,
    )
    .expect("restart after a refused resume");
    assert!(
        files.side("refused").contains(&gone.to_string()),
        "the vendor was asked for {gone} and refused it: {:?}",
        files.side("refused")
    );
    assert_eq!(
        resumed.session_id().as_deref(),
        Some("sess-vendor-double"),
        "the new thread's id is the one this adapter now holds"
    );
    // A new conversation answers from here on.
    let fresh = CodexHarness::start_after_restart(
        env!("CARGO_BIN_EXE_restart-codex"),
        resumed.session_id().as_deref(),
        &files.pairs(),
        None,
    )
    .expect("the start after the refusal");
    assert_eq!(fresh.session_id().as_deref(), Some("sess-vendor-double"));
    drop(fresh);
}
