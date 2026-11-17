//! Real terminal regressions using only synthetic input. Each reader runs in a
//! fresh subprocess/PTY, so tests neither share nor modify the test runner's TTY.
#![cfg(unix)]

#[path = "../src/signin/input.rs"]
mod input;
#[path = "../src/signin/vendor.rs"]
mod vendor;

use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Pty {
    scenario: String,
    master: File,
    slave: File,
    saved: libc::termios,
    child: Child,
    capture: Vec<u8>,
}

fn attrs(fd: &File) -> libc::termios {
    let mut attrs = unsafe { std::mem::zeroed() };
    assert_eq!(unsafe { libc::tcgetattr(fd.as_raw_fd(), &mut attrs) }, 0);
    attrs
}

impl Pty {
    fn start(scenario: &str) -> Self {
        let (mut master, mut slave) = (0, 0);
        #[cfg(target_vendor = "apple")]
        let mut size = libc::winsize {
            ws_row: 24,
            ws_col: 100,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        #[cfg(not(target_vendor = "apple"))]
        let size = libc::winsize {
            ws_row: 24,
            ws_col: 100,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        #[cfg(target_vendor = "apple")]
        let opened = unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &raw mut size,
            )
        };
        #[cfg(not(target_vendor = "apple"))]
        let opened = unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null(),
                &raw const size,
            )
        };
        assert_eq!(opened, 0);
        let master = unsafe { File::from_raw_fd(master) };
        let slave = unsafe { File::from_raw_fd(slave) };
        let saved = attrs(&slave);
        // A fresh process is essential: libc stdin must belong to this terminal.
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "pty_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("SIGNIN_INPUT_SCENARIO", scenario)
            .env_remove("ANYAGENTS_RAW")
            .stdin(slave.try_clone().unwrap())
            .stdout(slave.try_clone().unwrap())
            .stderr(slave.try_clone().unwrap())
            .spawn()
            .unwrap();
        Self {
            scenario: scenario.to_owned(),
            master,
            slave,
            saved,
            child,
            capture: Vec::new(),
        }
    }

    fn send(&mut self, bytes: &[u8]) {
        self.master.write_all(bytes).unwrap();
    }

    fn until(&mut self, needle: &str) {
        let until = Instant::now() + Duration::from_secs(5);
        while !String::from_utf8_lossy(&self.capture).contains(needle) {
            assert!(
                Instant::now() < until,
                "missing {needle:?} in {:?}",
                String::from_utf8_lossy(&self.capture)
            );
            let mut fd = libc::pollfd {
                fd: self.master.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            if unsafe { libc::poll(&mut fd, 1, 50) } <= 0 {
                continue;
            }
            let mut bytes = [0; 4096];
            let n = self.master.read(&mut bytes).unwrap();
            assert!(n > 0, "PTY closed before {needle:?}");
            self.capture.extend_from_slice(&bytes[..n]);
        }
    }

    fn done(mut self) {
        self.until("RESULT_OK");
        let end = Instant::now() + Duration::from_secs(5);
        let status = loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                break status;
            }
            assert!(Instant::now() < end, "reader did not exit");
            std::thread::sleep(Duration::from_millis(5));
        };
        assert!(
            status.success(),
            "{:?}",
            String::from_utf8_lossy(&self.capture)
        );
        let now = attrs(&self.slave);
        assert_eq!(
            now.c_lflag, self.saved.c_lflag,
            "local terminal flags restored"
        );
        assert_eq!(
            now.c_iflag, self.saved.c_iflag,
            "input terminal flags restored"
        );
        assert_eq!(now.c_cc, self.saved.c_cc, "control characters restored");
        let text = String::from_utf8_lossy(&self.capture);
        assert!(text.contains("\x1b[?2004l"), "bracketed paste disabled");
        assert!(
            !text.contains("synthetic-secret-code"),
            "secret was printed"
        );
        assert!(!text.contains("a中文🔒"), "typed secret was printed");
        // Optional synthetic evidence for review; never used in production.
        if let Some(dir) = std::env::var_os("ANYAGENTS_SIGNIN_CAPTURE_DIR") {
            let dir = std::path::PathBuf::from(dir);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join(format!("{}.ansi", self.scenario)), &self.capture).unwrap();
            std::fs::write(
                dir.join(format!("{}.txt", self.scenario)),
                format!(
                    "Scenario: {}\nTerminal attributes restored; no plaintext input.\n{}",
                    self.scenario,
                    text.replace('\x1b', "\\x1b").replace('\r', "\\r")
                ),
            )
            .unwrap();
        }
    }
}

impl Drop for Pty {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn paste_feedback_is_visible_before_enter_and_unicode_editing_is_exact() {
    let mut pty = Pty::start("edit");
    pty.until("0 chars");
    pty.send(b"\x1b[200~synthetic-secret-code\r\n\x03\x1b[201~");
    pty.until("21 chars"); // No Enter yet: closing the paste must redraw too.
    pty.send(&[21]);
    pty.send("a中文🔒".as_bytes());
    pty.until("4 chars");
    pty.send(&[127]);
    pty.until("3 chars");
    pty.send(b"\r");
    pty.done();
}

#[test]
fn cancel_restores_terminal() {
    let mut pty = Pty::start("cancel");
    pty.until("0 chars");
    pty.send(b"hidden\x03");
    pty.done();
}

#[test]
fn eof_restores_terminal() {
    let mut pty = Pty::start("eof");
    pty.until("0 chars");
    pty.send(&[4]);
    pty.done();
}

#[test]
fn timeout_restores_terminal_without_a_stdin_thread() {
    let mut pty = Pty::start("timeout");
    pty.until("0 chars");
    pty.done();
}

#[test]
fn vendor_partial_prompts_forward_only_the_submitted_line_and_redact_echoes() {
    let mut pty = Pty::start("vendor");
    pty.until("Manual code: ");
    pty.send(b"\x1b[200~synthetic-secret-code\x1b[201~");
    pty.until("21 chars");
    pty.send(b"\r");
    pty.until("[hidden]");
    pty.done();
}

#[test]
fn raw_pipe_keeps_line_contract_without_prompt_or_escape_sequences() {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "pty_child", "--nocapture"])
        .env("SIGNIN_INPUT_SCENARIO", "plain")
        .env("ANYAGENTS_RAW", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"first\r\nsecond\n")
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "{out:?}");
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("RESULT_OK"));
    assert!(!text.contains("ignored-prompt"));
    assert!(!out.stdout.contains(&0x1b));
    assert!(out.stderr.is_empty());
}

#[test]
#[ignore = "subprocess entry point; invoked only by the PTY tests"]
fn pty_child() {
    let Ok(scenario) = std::env::var("SIGNIN_INPUT_SCENARIO") else {
        return;
    };
    if scenario == "plain" {
        assert_eq!(
            input::read_secret_line("ignored-prompt", None).unwrap(),
            "first"
        );
        assert_eq!(
            input::read_secret_line("ignored-prompt", None).unwrap(),
            "second"
        );
    } else if scenario == "vendor" {
        struct Sink;
        impl anyagents_installer::LineSink for Sink {
            fn line(&mut self, line: &str) {
                println!("{line}");
            }
        }
        let terminal = input::Terminal::new().unwrap();
        let child = Command::new("sh")
            .args([
                "-c",
                r#"
            printf 'Manual code: ' >&2
            IFS= read -r code
            [ "$code" = 'synthetic-secret-code' ] || exit 9
            printf '%s' "$code" | cut -c1-7 | tr -d '\n'
            sleep 0.05
            printf '%s\n' "$code" | cut -c8-
        "#,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        vendor::run(child, terminal, &mut Sink).unwrap();
    } else {
        let timeout = if scenario == "timeout" {
            Duration::from_millis(150)
        } else {
            Duration::from_secs(5)
        };
        let result = input::read_secret_line("Private entry", Some(timeout));
        match scenario.as_str() {
            "edit" => assert_eq!(result.unwrap(), "a中文"),
            "cancel" => assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Interrupted),
            "eof" => assert_eq!(result.unwrap_err().kind(), io::ErrorKind::UnexpectedEof),
            "timeout" => assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut),
            _ => panic!("unknown synthetic scenario"),
        }
    }
    println!("RESULT_OK");
}
