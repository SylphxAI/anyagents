//! A keeper hosting a small line adapter, for the keeper's process tests.
//!
//! The launch is `{"script": "<sh>", "args": [...]}`: a shell harness that
//! reads one message id per line and answers `took <id>` once it has taken
//! it. The adapter writes an id once, reports it in flight until the harness
//! answers, and answers a repeat from what it remembers, as a vendor adapter
//! with consumption proof does. `--protocol <min>-<max>` before the keeper's
//! own arguments changes the protocol versions this keeper advertises.
//!
//! Two faults on purpose: a launch with `"hang_start": true` never finishes
//! starting, and the message id `wedge` is never answered (its call holds the
//! adapter for good), as a blocking write into a stuck vendor would. The
//! message id `refuse` is refused for good, as a vendor refuses a malformed
//! request.
//!
//! Two probes: `"slow_health": "<path>"` makes the first health read after
//! `<path>` appears take a while (it removes `<path>` and writes
//! `<path>.reading` as it starts), as a vendor's can under load; and
//! `"dropped": "<path>"` is written once the adapter's drop has ended the
//! harness.

use std::collections::HashSet;
use std::io::{BufRead as _, BufReader, Write as _};
use std::process::{Child, ChildStdin, Stdio};
use std::sync::{Arc, Mutex};

use anyagents_harness::{
    DeliverOutcome, Delivery, DeliveryLevel, Harness, HarnessError, HarnessHealth, OutboundMessage,
    Proof, TurnState,
};
use anyagents_keeper::{Hosted, KeeperOptions, Spec};

type Notify = Arc<dyn Fn() + Send + Sync>;

#[derive(Default)]
struct State {
    written: Vec<String>,
    taken: HashSet<String>,
    alive: bool,
    notify: Option<Notify>,
}

impl State {
    fn changed(&self) {
        if let Some(notify) = &self.notify {
            notify();
        }
    }
}

struct LineAdapter {
    child: Child,
    stdin: Option<ChildStdin>,
    state: Arc<Mutex<State>>,
    slow_health: Option<std::path::PathBuf>,
    dropped: Option<std::path::PathBuf>,
}

impl LineAdapter {
    fn start(spec: &Spec) -> Result<Self, String> {
        if spec.launch["hang_start"].as_bool() == Some(true) {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(3600));
            }
        }
        let script = spec.launch["script"]
            .as_str()
            .ok_or("launch names no script")?;
        let args: Vec<String> = spec.launch["args"]
            .as_array()
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let mut command = anyagents_harness::spawn_command(std::path::Path::new("/bin/sh"), &[])
            .map_err(|error| error.to_string())?;
        command
            .arg("-c")
            .arg(script)
            .arg("line-harness")
            .args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut child = command.spawn().map_err(|error| error.to_string())?;
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().ok_or("stdout missing")?;
        let state = Arc::new(Mutex::new(State {
            alive: true,
            ..State::default()
        }));
        let reading = Arc::clone(&state);
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if let Some(id) = line.strip_prefix("took ") {
                    let mut state = reading.lock().unwrap();
                    state.taken.insert(id.to_string());
                    state.changed();
                }
            }
            let mut state = reading.lock().unwrap();
            state.alive = false;
            state.changed();
        });
        let path = |key: &str| spec.launch[key].as_str().map(std::path::PathBuf::from);
        Ok(Self {
            child,
            stdin,
            state,
            slow_health: path("slow_health"),
            dropped: path("dropped"),
        })
    }
}

impl Harness for LineAdapter {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        if message.id == "wedge" {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(3600));
            }
        }
        if message.id == "refuse" {
            return Ok(DeliverOutcome::rejected("refused for good", true));
        }
        let mut state = self.state.lock().unwrap();
        if state.taken.contains(&message.id) {
            return Ok(DeliverOutcome::Accepted {
                turn_finished: true,
            });
        }
        if state.written.contains(&message.id) {
            return Ok(DeliverOutcome::InFlight);
        }
        if !state.alive {
            return Err(HarnessError::Dead);
        }
        let stdin = self.stdin.as_mut().ok_or(HarnessError::Dead)?;
        writeln!(stdin, "{}", message.id).map_err(|_| HarnessError::Dead)?;
        stdin.flush().map_err(|_| HarnessError::Dead)?;
        state.written.push(message.id.clone());
        Ok(DeliverOutcome::InFlight)
    }

    fn proven(&self, message_id: &str) -> Option<Proof> {
        self.state
            .lock()
            .unwrap()
            .taken
            .contains(message_id)
            .then_some(Proof::Taken {
                turn_finished: true,
            })
    }

    /// The harness ends; what it answered before stays proven.
    fn stop(&mut self) {
        self.stdin = None;
        anyagents_harness::kill_child(&mut self.child, &[]);
        let _ = self.child.wait();
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        Ok(())
    }

    fn resume(&mut self, _session: &str) -> Result<(), HarnessError> {
        Ok(())
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        Ok(())
    }

    fn health(&self) -> HarnessHealth {
        if let Some(trigger) = &self.slow_health
            && std::fs::remove_file(trigger).is_ok()
        {
            let _ = std::fs::write(trigger.with_extension("reading"), b"");
            std::thread::sleep(std::time::Duration::from_millis(300));
        }
        let state = self.state.lock().unwrap();
        let open = state.written.iter().any(|id| !state.taken.contains(id));
        HarnessHealth {
            process_alive: state.alive,
            turn_state: if open && state.alive {
                TurnState::InTurn
            } else {
                TurnState::Idle
            },
            delivery: Delivery {
                level: DeliveryLevel::NextStep,
                idempotent: true,
                proof: "took <id>".into(),
                peer: None,
            },
            quota_reset_at_ms: None,
            sign_in_fault: None,
        }
    }

    fn turns_served(&self) -> u32 {
        self.state.lock().unwrap().taken.len() as u32
    }

    fn process_id(&self) -> Option<u32> {
        Some(self.child.id())
    }

    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        let mut state = self.state.lock().unwrap();
        state.notify = Some(Arc::clone(&notify));
        drop(state);
        notify();
    }
}

impl Hosted for LineAdapter {
    fn session_id(&self) -> Option<String> {
        Some("line-session".into())
    }
}

impl Drop for LineAdapter {
    fn drop(&mut self) {
        self.stdin = None;
        anyagents_harness::kill_child(&mut self.child, &[]);
        let _ = self.child.wait();
        if let Some(dropped) = &self.dropped {
            let _ = std::fs::write(dropped, b"");
        }
    }
}

fn main() {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let mut options = KeeperOptions {
        binary: "kept_line".into(),
        ..KeeperOptions::default()
    };
    if args.first().map(String::as_str) == Some("--protocol") && args.len() > 1 {
        let range = args[1].clone();
        args.drain(..2);
        if let Some((min, max)) = range.split_once('-')
            && let (Ok(min), Ok(max)) = (min.parse(), max.parse())
        {
            options.protocol = (min, max);
        }
    }
    let code = anyagents_keeper::keeper_main(&args, options, LineAdapter::start);
    std::process::exit(code);
}
