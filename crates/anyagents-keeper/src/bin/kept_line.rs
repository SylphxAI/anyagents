//! A keeper hosting a small line adapter, for the keeper's process tests.
//!
//! The launch is `{"script": "<sh>", "args": [...]}`: a shell harness that
//! reads one message id per line and answers `took <id>` once it has taken
//! it. The adapter writes an id once, reports it in flight until the harness
//! answers, and answers a repeat from what it remembers, as a vendor adapter
//! with consumption proof does. `--protocol <min>-<max>` before the keeper's
//! own arguments changes the protocol versions this keeper advertises.

use std::collections::HashSet;
use std::io::{BufRead as _, BufReader, Write as _};
use std::process::{Child, ChildStdin, Stdio};
use std::sync::{Arc, Mutex};

use anyagents_harness::{
    DeliverOutcome, DeliveryMode, Harness, HarnessError, HarnessHealth, OutboundMessage,
    SteerSupport, TurnState,
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
}

impl LineAdapter {
    fn start(spec: &Spec) -> Result<Self, String> {
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
        Ok(Self {
            child,
            stdin,
            state,
        })
    }
}

impl Harness for LineAdapter {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
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
        let state = self.state.lock().unwrap();
        let open = state.written.iter().any(|id| !state.taken.contains(id));
        HarnessHealth {
            process_alive: state.alive,
            turn_state: if open && state.alive {
                TurnState::InTurn
            } else {
                TurnState::Idle
            },
            delivery_mode: DeliveryMode::Steer,
            steer_support: SteerSupport::Yes,
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
