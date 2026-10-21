//! A restart keeps the conversation on Antigravity.
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
use anyagents_harness_agy::AgyHarness;
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

/// The vendor double's script, its inbox and the files it writes.
struct Files {
    mode: String,
    log: String,
    inbox: String,
    refuse: String,
}

impl Files {
    fn new(dir: &TempDir, mode: &str) -> Self {
        let inbox = dir.path().join("inbox");
        std::fs::write(&inbox, "").unwrap();
        Self {
            mode: mode.to_string(),
            log: dir.path().join("log").display().to_string(),
            inbox: inbox.display().to_string(),
            refuse: dir.path().join("refuse").display().to_string(),
        }
    }

    fn pairs(&self) -> Vec<(&'static str, &str)> {
        vec![
            ("ANYAGENTS_FAKE_MODE", self.mode.as_str()),
            ("ANYAGENTS_FAKE_LOG", self.log.as_str()),
            ("ANYAGENTS_INBOX", self.inbox.as_str()),
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

/// Send `m1` as soon as the vendor will take it, and wait for its turn.
fn ask(harness: &mut AgyHarness) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match harness.deliver(&message()) {
            Ok(DeliverOutcome::Accepted { .. }) => break,
            other if std::time::Instant::now() < deadline => {
                let _ = other;
                std::thread::sleep(Duration::from_millis(20));
            }
            other => panic!("the vendor would not take the message: {other:?}"),
        }
    }
    assert!(harness.wait_finished("m1", Duration::from_secs(10)));
}

#[test]
fn an_agy_restart_resumes_the_session_the_vendor_reported() {
    let dir = TempDir::new().unwrap();
    let finish = Files::new(&dir, "finish");
    let mut first = AgyHarness::start(env!("CARGO_BIN_EXE_restart-agy"), None, &finish.pairs())
        .expect("first start");
    ask(&mut first);
    let saved = first.session_id().expect("the vendor reported a session");
    assert_eq!(saved, "sess-vendor-double");
    // The server goes down: the launch ends with the vendor.
    first.end_launch();
    drop(first);

    let mut second = AgyHarness::start_after_restart(
        env!("CARGO_BIN_EXE_restart-agy"),
        Some(&saved),
        &finish.pairs(),
    )
    .expect("restart");
    assert!(
        finish.side("resumed").contains(&saved),
        "the restart asks the vendor for {saved}: the vendor saw {:?}",
        finish.side("resumed")
    );
    // The resumed conversation is the one that answers, and its id is the same.
    ask(&mut second);
    assert_eq!(second.session_id().as_deref(), Some(saved.as_str()));
    drop(second);
}

#[test]
fn an_agy_restart_whose_session_the_vendor_refuses_starts_a_new_one() {
    let dir = TempDir::new().unwrap();
    let finish = Files::new(&dir, "finish");
    // The stored session is one the vendor does not have: it answers the line
    // with an error and stays up, as the CLI does before a conversation of its
    // own.
    let gone = "gone-session";
    std::fs::write(&finish.refuse, format!("{gone}\n")).unwrap();
    let mut restart = AgyHarness::start_after_restart(
        env!("CARGO_BIN_EXE_restart-agy"),
        Some(gone),
        &finish.pairs(),
    )
    .expect("restart after a refused resume");
    assert!(
        finish.side("refused").contains(&gone.to_string()),
        "the vendor was asked for {gone} and refused it: {:?}",
        finish.side("refused")
    );
    // A new conversation answers, and its id replaces the one that was gone.
    ask(&mut restart);
    assert_eq!(
        restart.session_id().as_deref(),
        Some("sess-vendor-double"),
        "the new session's id is the one this adapter now holds"
    );
    assert!(
        !finish.side("resumed").contains(&gone.to_string()),
        "the refused session is never kept as resumed"
    );
    drop(restart);
}
