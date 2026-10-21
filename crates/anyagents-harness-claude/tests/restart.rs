//! A restart keeps the conversation on Claude Code.
//!
//! A new server builds the adapter again and resumes the session id kept beside
//! the agent's config. Nothing the first adapter had in memory survives the
//! restart, so the second start has only the stored id to pass back to the
//! vendor. These tests take the step a new server takes at startup, against the
//! vendor double, which writes down every session it was asked to resume and
//! every one it refused.

use std::path::PathBuf;

use std::time::Duration;

use anyagents_harness::{DeliverOutcome, Harness, OutboundMessage};
use anyagents_harness_claude::ClaudeHarness;
use tempfile::TempDir;

fn message() -> OutboundMessage {
    OutboundMessage {
        id: "m1".to_string(),
        seq: 1,
        text: "hello".to_string(),
        sender: None,
        reply_to: None,
        source_time_ms: None,
    }
}

/// The vendor double's files: its scripts, its log, its refusal list.
struct Files {
    mode: String,
    log: String,
    refuse: String,
}

impl Files {
    fn new(dir: &TempDir, mode: &str) -> Self {
        Self {
            mode: mode.to_string(),
            log: dir.path().join("log").display().to_string(),
            refuse: dir.path().join("refuse").display().to_string(),
        }
    }

    fn pairs(&self) -> Vec<(&'static str, &str)> {
        vec![
            ("ANYAGENTS_FAKE_MODE", self.mode.as_str()),
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

/// Send `m1` as soon as the child will take it, and wait for its result.
fn ask(harness: &mut ClaudeHarness) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match harness.deliver(&message()) {
            Ok(DeliverOutcome::Accepted { .. }) => break,
            other if std::time::Instant::now() < deadline => {
                let _ = other;
                std::thread::sleep(Duration::from_millis(20));
            }
            other => panic!("the child would not take the message: {other:?}"),
        }
    }
    assert!(harness.wait_finished("m1", Duration::from_secs(10)));
}

#[test]
fn a_claude_code_restart_resumes_the_session_the_vendor_reported() {
    let dir = TempDir::new().unwrap();
    let finish = Files::new(&dir, "finish");
    let mut first = ClaudeHarness::start_after_restart(
        env!("CARGO_BIN_EXE_restart-claude"),
        None,
        &finish.pairs(),
        None,
    )
    .expect("first start");
    ask(&mut first);
    let saved = first.session_id().expect("the vendor reported a session");
    assert_eq!(saved, "sess-vendor-double");
    first.wait_dead(Duration::from_secs(5));
    drop(first);

    let mut second = ClaudeHarness::start_after_restart(
        env!("CARGO_BIN_EXE_restart-claude"),
        Some(&saved),
        &finish.pairs(),
        None,
    )
    .expect("restart");
    assert!(
        finish.side("resumed").contains(&saved),
        "the restart asks the vendor for {saved}: the vendor saw {:?}",
        finish.side("resumed")
    );
    ask(&mut second);
    assert_eq!(second.session_id().as_deref(), Some(saved.as_str()));
    drop(second);
}

#[test]
fn a_claude_code_restart_whose_session_the_vendor_refuses_starts_a_new_one() {
    let dir = TempDir::new().unwrap();
    let finish = Files::new(&dir, "finish");
    let gone = "gone-session";
    std::fs::write(&finish.refuse, format!("{gone}\n")).unwrap();
    let mut restart = ClaudeHarness::start_after_restart(
        env!("CARGO_BIN_EXE_restart-claude"),
        Some(gone),
        &finish.pairs(),
        None,
    )
    .expect("restart after a refused resume");
    assert!(
        finish.side("refused").contains(&gone.to_string()),
        "the vendor was asked for {gone} and refused it: {:?}",
        finish.side("refused")
    );
    ask(&mut restart);
    assert_eq!(
        restart.session_id().as_deref(),
        Some("sess-vendor-double"),
        "the new session's id is the one this adapter now holds"
    );
    drop(restart);
}
