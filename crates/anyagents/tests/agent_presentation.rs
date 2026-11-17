//! Human receipts and the unchanged machine-output boundary of agent commands.
mod support;
use std::path::Path;
use std::process::Command;

use anyagents_core::{Incoming, NewAgent, ReceiveOutcome, Store};

fn command(data: &Path, pretty: bool) -> Command {
    let home = data.join("home");
    std::fs::create_dir_all(&home).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_anyagents"));
    command
        .env_clear()
        .env("PATH", support::machine_path(data))
        .env("HOME", &home)
        .env("ANYAGENTS_HOST_HOME", &home)
        .env("ANYAGENTS_TESTS", "1")
        .env("ANYAGENTS_CLAUDE_CODE_BIN", env!("CARGO_BIN_EXE_anyagents"))
        .env("ANYAGENTS_PRETTY", "1")
        .env("NO_COLOR", "1")
        .arg("--data")
        .arg(data);
    if !pretty {
        // Raw must win even when pretty was explicitly requested.
        command.env("ANYAGENTS_RAW", "1");
    }
    command
}

fn run(data: &Path, pretty: bool, args: &[&str]) -> String {
    let output = command(data, pretty).args(args).output().unwrap();
    assert!(
        output.status.success(),
        "{args:?} status={} stdout={:?} stderr={:?}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn human(text: &str, expected: &str, data: &Path) {
    assert!(
        text.contains(expected),
        "expected {expected:?}, got stdout {text:?}"
    );
    assert!(!text.contains('\t'), "raw tab delimiter: {text}");
    assert!(
        !text.contains(data.to_str().unwrap()),
        "internal path: {text}"
    );
    assert!(!text.contains('\x1b'), "NO_COLOR ignored: {text}");
}

#[test]
fn offline_lifecycle_receipts_preserve_state_and_raw_columns() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    let mut new = NewAgent::new("worker", "claude-code");
    new.workdir = Some(data.join("private-workspace").display().to_string());
    let agent = store.create_agent(new).unwrap();
    let id = &agent.id;
    assert_eq!(
        run(data, false, &["agents", "pause", "worker"]),
        format!("{id}\tworker\tpaused\n")
    );
    human(
        &run(data, true, &["agents", "wake", "worker"]),
        "Woken worker",
        data,
    );
    assert_eq!(store.agent(id).unwrap().state, "running");
    human(
        &run(data, true, &["pause", "worker"]),
        "Paused worker",
        data,
    );
    assert_eq!(store.agent(id).unwrap().state, "paused");
    let verbose = run(data, true, &["--verbose", "agents", "pause", "worker"]);
    assert!(
        verbose.contains(data.join("private-workspace").to_str().unwrap()),
        "{verbose}"
    );
    human(
        &run(data, true, &["agents", "archive", "worker"]),
        "Archived worker",
        data,
    );
    assert_eq!(store.agent(id).unwrap().state, "archived");
    human(
        &run(data, true, &["agents", "delete", "worker"]),
        "Deleted worker",
        data,
    );
    assert!(store.agent(id).is_err());
}

#[test]
fn offline_create_rename_role_and_descriptions_have_semantic_receipts() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    human(
        &run(
            data,
            true,
            &["agents", "create", "worker", "--harness", "claude-code"],
        ),
        "Created worker",
        data,
    );
    let store = Store::open(data.join("state.db")).unwrap();
    let agent = store.agent_by_name("worker").unwrap();
    human(
        &run(data, true, &["agents", "rename", "worker", "builder"]),
        "Renamed worker → builder",
        data,
    );
    assert_eq!(store.agent(&agent.id).unwrap().name, "builder");
    human(
        &run(data, true, &["agents", "role", "builder", "--clear"]),
        "Role updated: builder · no role",
        data,
    );
    assert_eq!(store.agent(&agent.id).unwrap().role, None);
    human(
        &run(
            data,
            true,
            &["agents", "describe", "builder", "Useful worker"],
        ),
        "Updated builder",
        data,
    );
    human(
        &run(data, true, &["agents", "describe", "builder"]),
        "Useful worker",
        data,
    );
    assert_eq!(
        run(data, false, &["agents", "describe", "builder"]),
        "builder · general\tUseful worker\n"
    );
    let raw = run(
        data,
        false,
        &["agents", "create", "raw-worker", "--harness", "claude-code"],
    );
    let raw_agent = store.agent_by_name("raw-worker").unwrap();
    assert_eq!(raw, format!("{}\traw-worker\n", raw_agent.id));
}

#[test]
fn parked_messages_hide_internal_reasons_except_in_verbose_output() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    let agent = store
        .create_agent(NewAgent::new("worker", "claude-code"))
        .unwrap();
    human(
        &run(data, true, &["requeue", "worker"]),
        "No parked messages.",
        data,
    );
    let message = store
        .receive(
            &agent.id,
            Incoming {
                text: "retry me".into(),
                sender: Some("operator".into()),
                source_time_ms: None,
                source_key: None,
                reply_to: None,
                quote: None,
                origin: None,
                received_at_ms: None,
            },
        )
        .unwrap();
    let message = match message {
        ReceiveOutcome::New(message) => message,
        ReceiveOutcome::Duplicate(_) => panic!("new fixture was deduplicated"),
    };
    let reason = format!("cannot read {}/private/config", data.display());
    store
        .reject_message(&agent.id, message.seq, &reason)
        .unwrap();
    let pretty = run(data, true, &["requeue", "worker"]);
    human(&pretty, "Parked messages", data);
    assert!(pretty.contains("Rejected"), "{pretty}");
    assert_eq!(
        run(data, false, &["requeue", "worker"]),
        format!("{}\t{}\toperator\t{reason}\n", message.id, message.seq)
    );
    assert!(run(data, true, &["--verbose", "requeue", "worker"]).contains(&reason));
    human(
        &run(data, true, &["requeue", "worker", message.id.as_ref()]),
        "Requeued message for worker",
        data,
    );
    assert!(store.rejected_messages(&agent.id).unwrap().is_empty());
}

#[test]
fn flush_busy_diagnostic_and_empty_reports_remain_readable() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    let agent = store
        .create_agent(NewAgent::new("worker", "claude-code"))
        .unwrap();
    store.set_agent_state(&agent.id, "running").unwrap();
    let _lock = store.try_lock_agent(&agent.id).unwrap();
    human(
        &run(data, true, &["flush", "worker"]),
        "The harness is already running.",
        data,
    );
    assert_eq!(
        run(data, false, &["flush", "worker"]),
        "worker\tskipped\tits harness process is live\n"
    );
    human(
        &run(data, true, &["usage"]),
        "No usage records found.",
        data,
    );
    human(
        &run(data, true, &["outcomes"]),
        "No outcomes recorded.",
        data,
    );
    human(&run(data, true, &["agents", "list"]), "worker", data);
    let json = run(data, true, &["agents", "inspect", "worker", "--json"]);
    assert!(
        serde_json::from_str::<serde_json::Value>(&json).is_ok(),
        "{json}"
    );
}
