use std::time::Duration;

use anyagents_harness_spiron::{ListenError, SpironHarness, SpironLaunch};

fn launch_failure(script: &str) -> String {
    let root = tempfile::tempdir_in("/tmp").unwrap();
    let dir = root.path();
    let result = SpironHarness::launch_listening_with_stderr_tail(
        SpironLaunch {
            program: "/bin/sh".into(),
            args: vec!["-c".into(), script.into(), "fake-spiron".into()],
            environment: vec![("TEST_VALUE".into(), "mock deployment	value".into())],
            work_dir: dir.into(),
            session: None,
            state_dir: dir.into(),
        },
        &dir.join("s.sock"),
        &dir.join("spiron.log"),
        Duration::from_secs(5),
        |_| Ok(()),
    );
    let Err(ListenError::Failed(error)) = result else {
        panic!("test process never listened");
    };
    error
}

const EXIT: &str = "spiron exited with exit status: 101 before it listened";

#[test]
fn startup_error_keeps_panic_lines_masks_secrets_and_ignores_stdout() {
    let error = launch_failure(
        r#"
        printf 'stdout is not a diagnostic\n'
        printf '\033[31mthread panicked\033[0m\007\r\n\n' >&2
        printf '\tinvalid argument --example\n' >&2
        printf 'pass\033[1mword\033[0m=not-a-real-password\n' >&2
        printf 'environment: %s\n' "$TEST_VALUE" >&2
        exit 101
        "#,
    );
    assert_eq!(
        error,
        format!(
            "{EXIT} (stderr tail: thread panicked\ninvalid argument --example\npassword=[redacted]\nenvironment: [redacted])"
        )
    );
}

#[test]
fn startup_error_keeps_the_last_twenty_nonempty_lines() {
    let error = launch_failure(
        r#"
        i=1
        while [ "$i" -le 800 ]; do
            printf 'old noise before the startup failure: %s\n' "$i" >&2
            i=$((i+1))
        done
        i=1
        while [ "$i" -le 25 ]; do
            printf 'error line %s\n\n' "$i" >&2
            i=$((i+1))
        done
        exit 101
        "#,
    );
    let expected = (6..=25)
        .map(|i| format!("error line {i}"))
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(error, format!("{EXIT} (stderr tail: {expected})"));
}

#[test]
fn startup_error_bounds_bytes_without_leaking_a_cut_credential() {
    let error = launch_failure(
        r#"
        # Long credential line cut by the read window; its suffix must not leak.
        printf 'password=%20000sclipped-secret\n' '' | tr ' ' x >&2
        i=0
        while [ "$i" -lt 1000 ]; do
            printf '界' >&2
            i=$((i+1))
        done
        printf ' missing shared library' >&2
        exit 101
        "#,
    );
    let marker = " missing shared library";
    let tail = format!("{}{marker}", "界".repeat((2048 - marker.len()) / 3));
    assert_eq!(error, format!("{EXIT} (stderr tail: {tail})"));
    let error = launch_failure(
        "printf 'password=%20000sclipped-secret\\nlast-error\\n' '' | tr ' ' x >&2; exit 101",
    );
    assert_eq!(error, format!("{EXIT} (stderr tail: last-error)"));
}

#[test]
fn startup_error_without_stderr_has_no_empty_suffix() {
    assert_eq!(launch_failure("echo stdout; exit 101"), EXIT);
    assert_eq!(launch_failure("printf '\\r\\n \\t\\n' >&2; exit 101"), EXIT);
}
