//! `anyagents agent-run --config <dir> -- <command…>`: start one agent
//! instance inside the deployment's OS boundary.
//!
//! A server started with an operator token serves agents that do not share its
//! user, so this launcher is the only door into it. It checks the boundary and
//! starts the command (a harness, or a shell for the agent's own bash) only when
//! every check holds. When one does not, it prints the one OS admission the
//! deployment needs and exits non-zero: it never falls back to running the agent
//! in the server's own files.
//!
//! What "admitted" means is read from the boundary contract, not assumed: the
//! mechanism is a user namespace (`setpriv --reuid …`, `bwrap`, `unshare`) that
//! places the new process in a different uid from the server, with the data
//! directory absent and its own config directory writable. The mechanism must
//! start: a probe runs `/bin/true` inside it before anything else is launched.

use std::path::Path;

use anyagents_core::Error;
use anyagents_core::boundary::{self, Boundary};

/// Read the boundary contract of `data`, or fail with what is missing.
pub fn boundary_of(data: &Path) -> Result<Boundary, Error> {
    let path = data.join(boundary::BOUNDARY_FILE);
    let text = std::fs::read_to_string(&path).map_err(|err| {
        Error::Message(format!(
            "no identity boundary at {}: {err}; this deployment runs on the single-user route",
            path.display()
        ))
    })?;
    serde_json::from_str(&text).map_err(|err| Error::Message(err.to_string()))
}

/// The programs each mechanism starts. The first one present is used.
fn launcher(mechanism: &str) -> Option<&'static str> {
    let candidates: &[&str] = match mechanism {
        "userns" | "bwrap" => &["bwrap", "setpriv", "unshare"],
        _ => &[],
    };
    candidates.iter().copied().find(|program| on_path(program))
}

fn on_path(program: &str) -> bool {
    std::env::var_os("PATH")
        .map(|paths| {
            std::env::split_paths(&paths).any(|dir| {
                let candidate = dir.join(program);
                candidate.is_file()
            })
        })
        .unwrap_or(false)
}

/// The argv prefix that runs `command` inside the boundary.
fn prefix(boundary: &Boundary, program: &str, agent_uid: u32) -> Vec<String> {
    let mut argv = vec![program.to_string()];
    match program {
        "bwrap" => {
            for denied in &boundary.agents.denied {
                // `denied` is the data directory subtree and the process files
                // of other agents; bwrap takes paths, so a glob's directory is
                // bound empty.
                let path = denied.split('*').next().unwrap_or(denied);
                argv.push("--tmpfs".to_string());
                argv.push(path.to_string());
            }
            for readable in &boundary.agents.readable {
                argv.push("--ro-bind-try".to_string());
                argv.push(readable.to_string());
                argv.push(readable.to_string());
            }
            for writable in &boundary.agents.writable {
                let dir = writable.split('*').next().unwrap_or(writable);
                argv.push("--bind".to_string());
                argv.push(dir.to_string());
                argv.push(dir.to_string());
            }
            argv.push("--dev".to_string());
            argv.push("/dev".to_string());
            argv.push("--proc".to_string());
            argv.push("/proc".to_string());
            argv.push("--die-with-parent".to_string());
        }
        "setpriv" => {
            argv.push("--reuid".to_string());
            argv.push(agent_uid.to_string());
            argv.push("--regid".to_string());
            argv.push(agent_uid.to_string());
            argv.push("--clear-groups".to_string());
        }
        _ => {
            argv.push("--user".to_string());
            argv.push("--map-root-user".to_string());
        }
    }
    argv
}

/// Start one admitted agent command, or say what admission is missing.
pub fn run(data: &Path, config: Option<&Path>, command: &[String]) -> Result<(), Error> {
    if command.is_empty() {
        return Err(Error::Message("agent-run needs a command".into()));
    }
    let boundary = boundary_of(data)?;
    if let Some(config) = config
        && !config.join(crate::operator::CAPABILITY_FILE).is_file()
    {
        return Err(Error::Message(format!(
            "{} holds no mailbox capability",
            config.display()
        )));
    }
    // The agent's own uid: an operator names it, and nothing defaults to the
    // server's, which is the one uid an agent must never have.
    let Some(agent_uid) = std::env::var("ANYAGENTS_AGENT_UID")
        .ok()
        .and_then(|raw| raw.parse::<u32>().ok())
    else {
        return Err(Error::Message(unadmitted(
            &boundary,
            "ANYAGENTS_AGENT_UID names no agent uid",
        )));
    };
    if agent_uid == current_uid() {
        return Err(Error::Message(unadmitted(
            &boundary,
            "the agent uid is the server's own uid",
        )));
    }
    let Some(program) = launcher(&boundary.agents.mechanism) else {
        return Err(Error::Message(unadmitted(
            &boundary,
            &format!("no {} launcher is installed", boundary.agents.mechanism),
        )));
    };
    let mut argv = prefix(&boundary, program, agent_uid);
    // The probe runs before the agent's own command: if the mechanism cannot
    // start here, nothing else is started either.
    let probe: Vec<String> = argv
        .iter()
        .cloned()
        .chain(["/bin/true".to_string()])
        .collect();
    let status = std::process::Command::new(&probe[0])
        .args(&probe[1..])
        .status()
        .map_err(|err| Error::Message(unadmitted(&boundary, &err.to_string())))?;
    if !status.success() {
        return Err(Error::Message(unadmitted(
            &boundary,
            &format!("the {program} probe exited {status}"),
        )));
    }
    argv.extend(command.iter().cloned());
    let status = std::process::Command::new(&argv[0])
        .args(&argv[1..])
        .status()
        .map_err(Error::Io)?;
    if status.success() {
        Ok(())
    } else {
        Err(Error::Message(format!("the agent command exited {status}")))
    }
}

/// The exact admission an operator must grant, so a deployment never silently
/// runs an agent where the server's own files are readable.
fn unadmitted(boundary: &Boundary, why: &str) -> String {
    let data = &boundary.data_dir;
    format!(
        "agent not started: {why}; admit this host with user namespaces for the server's own user \
         (for example Ops sets kernel.unprivileged_userns_clone=1 and a max_user_namespaces above \
         zero, and installs bwrap or setpriv), then start the agent with this server, its own uid \
         (ANYAGENTS_AGENT_UID) and anyagents agent-run on {data}"
    )
}

fn current_uid() -> u32 {
    // SAFETY: getuid takes no arguments and cannot fail.
    unsafe { libc::getuid() }
}
