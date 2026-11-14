//! The commands a `code` call starts end with it.
//!
//! A snippet's `workspace.exec` runs on a blocking thread. When the call ends
//! first (its time ran out, the snippet returned without awaiting the command,
//! or the harness cancelled the call) the task that awaited the command is
//! aborted, but the blocking thread and the command it started do not notice:
//! before this module the command ran on until its own `timeout_ms`, holding
//! whatever it was writing (a `git commit` holding `index.lock`, say) after the
//! model had been told the call was over.
//!
//! Each `code` call now owns a [`CodeChildren`]. Every synchronous exec the
//! call starts registers in it, polls it, and when the call ends stops its
//! whole process group: SIGTERM first, so a command like git can remove its
//! lock files, then SIGKILL for whatever is left after [`TERM_GRACE`]. The
//! call reports each command it stopped, so the model knows a write may be
//! partial and checks before retrying.

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

/// How long a stopped command's group has after SIGTERM before SIGKILL.
pub(crate) const TERM_GRACE: Duration = Duration::from_secs(2);

/// How long an ending call waits for its commands to stop before answering:
/// the grace plus time for the polling thread to notice.
pub(crate) const SETTLE: Duration = Duration::from_millis(2_500);

/// How long a command line is when the model is told it was stopped.
const CMD_PREVIEW_CHARS: usize = 160;

/// The commands one `code` call started, and whether the call has ended.
#[derive(Default)]
pub(crate) struct CodeChildren {
    ended: AtomicBool,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    next: u64,
    running: Vec<u64>,
    stopped: Vec<Stopped>,
}

/// A command a call stopped because the call ended before it did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Stopped {
    pub cmd: String,
    pub ran_ms: u64,
}

impl CodeChildren {
    /// The call is over: every command it started stops.
    pub(crate) fn end(&self) {
        self.ended.store(true, Ordering::SeqCst);
    }

    pub(crate) fn ended(&self) -> bool {
        self.ended.load(Ordering::SeqCst)
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Record a command starting; it stays running until its registration
    /// is dropped.
    pub(crate) fn start(self: &Arc<Self>) -> Registration {
        let mut state = self.state();
        state.next += 1;
        let id = state.next;
        state.running.push(id);
        Registration {
            owner: Arc::clone(self),
            id,
        }
    }

    /// Wait up to `within` for every running command to stop, then return
    /// the commands this call stopped.
    pub(crate) async fn settle(&self, within: Duration) -> Vec<Stopped> {
        let until = Instant::now() + within;
        while !self.state().running.is_empty() && Instant::now() < until {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        std::mem::take(&mut self.state().stopped)
    }
}

/// One command's place in its call.
pub(crate) struct Registration {
    owner: Arc<CodeChildren>,
    id: u64,
}

impl Registration {
    /// The call that started the command has ended.
    pub(crate) fn call_ended(&self) -> bool {
        self.owner.ended()
    }

    /// The command was stopped because its call ended.
    pub(crate) fn stopped(&self, cmd: &str, ran: Duration) {
        let mut preview: String = cmd.chars().take(CMD_PREVIEW_CHARS).collect();
        if cmd.chars().count() > CMD_PREVIEW_CHARS {
            preview.push('…');
        }
        self.owner.state().stopped.push(Stopped {
            cmd: preview,
            ran_ms: u64::try_from(ran.as_millis()).unwrap_or(u64::MAX),
        });
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.owner.state().running.retain(|id| *id != self.id);
    }
}

/// Ends the call's commands when the call's future is dropped (the harness
/// cancelled it, or it returned early).
pub(crate) struct EndOnDrop(pub Arc<CodeChildren>);

impl Drop for EndOnDrop {
    fn drop(&mut self) {
        self.0.end();
    }
}

/// What a blocking tool thread knows about the call it serves.
#[derive(Clone, Default)]
pub(crate) struct Scope {
    /// The `code` call's commands, when the tool runs inside one.
    pub children: Option<Arc<CodeChildren>>,
    /// Whether the harness cancelled the tool call.
    pub cancelled: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
}

impl Scope {
    /// The call is over or cancelled: a command it started must stop.
    pub(crate) fn should_stop(&self, registration: Option<&Registration>) -> bool {
        registration.is_some_and(Registration::call_ended)
            || self.cancelled.as_ref().is_some_and(|cancelled| cancelled())
    }
}

thread_local! {
    static CURRENT: RefCell<Scope> = RefCell::new(Scope::default());
}

/// Restores the thread's previous scope when dropped.
pub(crate) struct Entered(Option<Scope>);

impl Drop for Entered {
    fn drop(&mut self) {
        if let Some(previous) = self.0.take() {
            CURRENT.with(|current| *current.borrow_mut() = previous);
        }
    }
}

/// Serve `scope` on this thread until the guard is dropped.
pub(crate) fn enter(scope: Scope) -> Entered {
    Entered(Some(CURRENT.with(|current| current.replace(scope))))
}

/// The scope this thread serves.
pub(crate) fn current() -> Scope {
    CURRENT.with(|current| current.borrow().clone())
}

/// Stop `child`'s whole process group (the child leads it): SIGTERM, up to
/// `grace` for the leader to exit, then SIGKILL for whatever remains, and
/// reap the leader. The leader is reaped only after the SIGKILL, so its
/// group id cannot have been reused by then.
pub(crate) fn stop_group(child: &mut std::process::Child, grace: Duration) {
    #[cfg(unix)]
    {
        let Ok(pgid) = i32::try_from(child.id()) else {
            let _ = child.kill();
            let _ = child.wait();
            return;
        };
        if pgid > 0 {
            // SAFETY: kill only sends a signal, to the group this process
            // created for the child and has not reaped.
            unsafe {
                libc::kill(-pgid, libc::SIGTERM);
            }
            let until = Instant::now() + grace;
            while Instant::now() < until && !exited_unreaped(pgid) {
                std::thread::sleep(Duration::from_millis(20));
            }
            // SAFETY: as above; the unreaped leader keeps the group id ours.
            unsafe {
                libc::kill(-pgid, libc::SIGKILL);
            }
        }
    }
    let _ = child.kill();
    let _ = child.wait();
}

/// Whether `pid` (a child of this process) has exited, without reaping it.
#[cfg(unix)]
fn exited_unreaped(pid: i32) -> bool {
    // SAFETY: waitid writes only into `info`; WNOWAIT leaves the child
    // waitable, WNOHANG returns at once.
    unsafe {
        let mut info: libc::siginfo_t = std::mem::zeroed();
        let rc = libc::waitid(
            libc::P_PID,
            pid as libc::id_t,
            &mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        );
        rc == 0 && info.si_pid() != 0
    }
}

/// The sentence a call adds for the commands it stopped.
pub(crate) fn stopped_note(stopped: &[Stopped]) -> Option<String> {
    if stopped.is_empty() {
        return None;
    }
    let list = stopped
        .iter()
        .map(|s| format!("`{}` (ran {}ms)", s.cmd, s.ran_ms))
        .collect::<Vec<_>>()
        .join("; ");
    Some(format!(
        "Stopped {} command{} this call started, whole process group (SIGTERM, then SIGKILL after {}s): {list}. \
         A write such a command was making may be partial: check its effect (for git, `git status` and `git log -1`) before running it again.",
        stopped.len(),
        if stopped.len() == 1 { "" } else { "s" },
        TERM_GRACE.as_secs()
    ))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn alive(pid: i32) -> bool {
        // SAFETY: signal 0 only checks that the process exists.
        unsafe { libc::kill(pid, 0) == 0 }
    }

    /// A shell that leads its own group and starts a grandchild that ignores
    /// SIGTERM, recording the grandchild's pid in `pidfile`.
    fn group_with_stubborn_grandchild(pidfile: &std::path::Path) -> std::process::Child {
        use std::os::unix::process::CommandExt as _;
        let script = format!(
            "sh -c 'trap \"\" TERM; echo $$ > {}; exec sleep 300' & wait",
            pidfile.display()
        );
        let mut cmd = std::process::Command::new("/bin/sh");
        cmd.arg("-c").arg(script).process_group(0);
        let child = cmd.spawn().unwrap();
        let until = Instant::now() + Duration::from_secs(10);
        while std::fs::read_to_string(pidfile)
            .ok()
            .and_then(|s| s.trim().parse::<i32>().ok())
            .is_none()
        {
            assert!(Instant::now() < until, "grandchild never started");
            std::thread::sleep(Duration::from_millis(10));
        }
        child
    }

    #[test]
    fn stop_group_ends_a_grandchild_that_ignores_sigterm() {
        let dir = tempfile::tempdir().unwrap();
        let pidfile = dir.path().join("grandchild.pid");
        let mut child = group_with_stubborn_grandchild(&pidfile);
        let grandchild: i32 = std::fs::read_to_string(&pidfile)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert!(alive(grandchild));
        stop_group(&mut child, Duration::from_millis(300));
        let until = Instant::now() + Duration::from_secs(5);
        while alive(grandchild) && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            !alive(grandchild),
            "grandchild {grandchild} outlived its group"
        );
    }

    #[tokio::test]
    async fn an_ended_call_stops_its_command_and_reports_it() {
        let children = Arc::new(CodeChildren::default());
        let registration = children.start();
        assert!(!registration.call_ended());
        let thread = {
            let scope = Scope {
                children: Some(Arc::clone(&children)),
                cancelled: None,
            };
            std::thread::spawn(move || {
                let _entered = enter(scope);
                let scope = current();
                let started = Instant::now();
                while !scope.should_stop(Some(&registration)) {
                    std::thread::sleep(Duration::from_millis(5));
                }
                registration.stopped("git commit -m x", started.elapsed());
            })
        };
        children.end();
        let stopped = children.settle(SETTLE).await;
        thread.join().unwrap();
        assert_eq!(stopped.len(), 1);
        assert_eq!(stopped[0].cmd, "git commit -m x");
        let note = stopped_note(&stopped).unwrap();
        assert!(note.contains("git commit -m x") && note.contains("git status"));
        assert!(stopped_note(&[]).is_none());
    }

    #[test]
    fn a_thread_outside_any_call_has_an_empty_scope() {
        let scope = current();
        assert!(scope.children.is_none() && scope.cancelled.is_none());
        assert!(!scope.should_stop(None));
    }
}
