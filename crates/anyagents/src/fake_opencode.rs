//! Test double for a streaming vendor, run as `anyagents fake-opencode`. It
//! prints text lines and never a result line, which is what the tests need to
//! see a turn end on quiet. With `ANYAGENTS_FAKE_LOG` set it records its argv
//! and every stdin line there.

use std::io::{self, BufRead, Write};

use anyagents_core::Error;

pub fn run() -> Result<(), Error> {
    let log = std::env::var_os("ANYAGENTS_FAKE_LOG");
    let argv = std::env::args().skip(1).collect::<Vec<_>>().join(" ");
    record(&log, format!("argv:{argv}"));
    let mut stdout = io::stdout();
    for line in io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        record(&log, line.clone());
        if line.contains("\"type\":\"prompt\"") {
            let _ = writeln!(
                stdout,
                r#"{{"type":"text","session_id":"sess-1","text":"pong from opencode"}}"#
            );
            let _ = stdout.flush();
        }
    }
    Ok(())
}

fn record(log: &Option<std::ffi::OsString>, line: String) {
    let Some(path) = log else { return };
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let _ = writeln!(file, "{line}");
    }
}
