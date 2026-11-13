//! Running a managed program once, bounded: doctor checks and the managed
//! `npm` that installs a dependency tree. Each run gets exactly the
//! environment it is given and its own process group.

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Runs one doctor command. Production uses [`ProcessExec`]; it is a trait
/// so a test can make a version fail doctor without building a broken binary.
pub trait Exec: Send + Sync {
    /// Run `program args` with exactly `env`, in `dir`. Returns stdout and
    /// stderr together on a zero exit, else the reason.
    fn run(
        &self,
        program: &Path,
        args: &[String],
        env: &[(String, String)],
        dir: &Path,
        timeout: Duration,
    ) -> Result<String, String>;
}

/// Runs the doctor command as a real child in its own process group, killed
/// as a whole on timeout.
pub struct ProcessExec;

impl Exec for ProcessExec {
    fn run(
        &self,
        program: &Path,
        args: &[String],
        env: &[(String, String)],
        dir: &Path,
        timeout: Duration,
    ) -> Result<String, String> {
        let out_path = dir.join(".doctor-out");
        let out = std::fs::File::create(&out_path).map_err(|e| e.to_string())?;
        let err = out.try_clone().map_err(|e| e.to_string())?;
        let mut command = Command::new(program);
        command
            .args(args)
            .env_clear()
            .envs(env.iter().map(|(k, v)| (k, v)))
            .current_dir(dir)
            .stdin(Stdio::null())
            .stdout(out)
            .stderr(err);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt as _;
            command.process_group(0);
        }
        let mut child = command
            .spawn()
            .map_err(|e| format!("{}: {e}", program.display()))?;
        let started = Instant::now();
        let status = loop {
            match child.try_wait().map_err(|e| e.to_string())? {
                Some(status) => break status,
                None if started.elapsed() >= timeout => {
                    kill_group(&mut child);
                    return Err(format!(
                        "{} did not exit within {}s",
                        program.display(),
                        timeout.as_secs()
                    ));
                }
                None => std::thread::sleep(Duration::from_millis(20)),
            }
        };
        let output = std::fs::read(&out_path).map_err(|e| e.to_string())?;
        let _ = std::fs::remove_file(&out_path);
        let output = String::from_utf8_lossy(&output).into_owned();
        if status.success() {
            Ok(output)
        } else {
            let tail: String = output
                .chars()
                .rev()
                .take(400)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect();
            Err(format!("{} exited {status}: {tail}", program.display()))
        }
    }
}

fn kill_group(child: &mut std::process::Child) {
    #[cfg(unix)]
    if let Ok(pid) = i32::try_from(child.id()) {
        // SAFETY: signalling the process group this child leads; the group was
        // created for it by `process_group(0)` and holds nothing else.
        unsafe {
            libc::kill(-pid, libc::SIGKILL);
        }
    }
    let _ = child.kill();
    let _ = child.wait();
}
