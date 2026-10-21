//! A restart keeps the conversation on Muse.
//!
//! A new server builds the adapter again and resumes the session id kept beside
//! the agent's config. Nothing the first adapter had in memory survives the
//! restart, so the second start has only the stored id to pass back to the
//! vendor. These tests take the step a new server takes at startup, against the
//! vendor double, which writes down every session it was asked to resume and
//! every one it refused.

use std::path::PathBuf;

use anyagents_harness_muse::MuseHarness;
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
fn a_muse_restart_resumes_the_session_the_vendor_reported() {
    let dir = TempDir::new().unwrap();
    let files = Files::new(&dir);
    // The first start opens a session.
    let first = MuseHarness::start(env!("CARGO_BIN_EXE_restart-muse"), None, &files.pairs())
        .expect("first start");
    // The vendor records the resume it is asked for when it starts, so the
    // first start's own record has to settle before the second is read.
    let saved = first.session_id().expect("the vendor named a session");
    assert_eq!(saved, "sess-vendor-double");
    drop(first);

    let second = MuseHarness::start_after_restart(
        env!("CARGO_BIN_EXE_restart-muse"),
        Some(&saved),
        &files.pairs(),
        None,
    )
    .expect("restart");
    // The vendor's resume call is made before the adapter answers anything, so
    // its own record is the proof that the stored id went back.
    // The adapter ran the vendor's own `session/resume` for the stored id, so
    // the vendor was asked for exactly that conversation.
    // The adapter ran the vendor's own `session/resume` for the stored id, so
    // the vendor was asked for exactly that conversation.
    let asked = files.side("resumed");
    assert!(
        asked.contains(&saved),
        "the restart asks the vendor for {saved}: the vendor saw {asked:?}"
    );
    assert_eq!(second.session_id().as_deref(), Some(saved.as_str()));
    drop(second);
}

#[test]
fn a_muse_restart_whose_session_the_vendor_refuses_starts_a_new_one() {
    let dir = TempDir::new().unwrap();
    let files = Files::new(&dir);
    let gone = "gone-session";
    std::fs::write(&files.refuse, format!("{gone}\n")).unwrap();
    let fresh = MuseHarness::start_after_restart(
        env!("CARGO_BIN_EXE_restart-muse"),
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
        fresh.session_id().as_deref(),
        Some("sess-vendor-double"),
        "the new session's id is the one this adapter now holds"
    );
    drop(fresh);
}
