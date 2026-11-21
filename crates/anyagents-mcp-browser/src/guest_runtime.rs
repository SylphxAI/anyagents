//! Linux guest-only stdio relay. One private, lock-owned runtime per guest.

use std::ffi::{OsStr, OsString};
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{Command, Stdio};
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use std::time::Duration;

use rmcp::ServiceExt;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

use crate::{BrowserMcpServer, signals::terminate_signal, takeover::DetachedDesktop};

pub const DIR_ARG: &str = "--guest-runtime-dir";
pub const WORKER_ARG: &str = "--guest-runtime-worker";
const STARTUP: Duration = Duration::from_secs(10);
/// Leave time for the Coordinator to wake an idle agent after hand-back,
/// without indefinitely keeping an unused browser in the guest.
const RECLAIM: Duration = Duration::from_secs(120);
const READY: u8 = 1;

struct RuntimeDir {
    dir: PathBuf,
    socket: PathBuf,
    uid: u32,
}

impl RuntimeDir {
    fn open(dir: &Path) -> io::Result<Self> {
        if !dir.is_absolute() {
            return Err(io::Error::other(
                "guest browser runtime directory must be absolute",
            ));
        }
        let socket = dir.join("mcp.sock");
        if socket.as_os_str().as_bytes().len() >= 108 {
            return Err(io::Error::other(
                "guest browser runtime socket path is too long",
            ));
        }
        // SAFETY: geteuid has no preconditions.
        let uid = unsafe { libc::geteuid() };
        match DirBuilder::new().mode(0o700).create(dir) {
            Err(e) if e.kind() != io::ErrorKind::AlreadyExists => return Err(e),
            _ => {}
        }
        let meta = fs::symlink_metadata(dir)?;
        if !meta.is_dir() || meta.uid() != uid || meta.mode() & 0o777 != 0o700 {
            return Err(io::Error::other("unsafe guest browser runtime directory"));
        }
        Ok(Self {
            dir: dir.to_owned(),
            socket,
            uid,
        })
    }

    fn socket_exists(&self) -> io::Result<bool> {
        match fs::symlink_metadata(&self.socket) {
            Ok(meta) if meta.file_type().is_socket() && meta.uid() == self.uid => Ok(true),
            Ok(_) => Err(io::Error::other("unsafe guest browser socket path")),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }

    fn lock(&self) -> io::Result<File> {
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .mode(0o600)
            .open(self.dir.join("owner.lock"))?;
        let meta = lock.metadata()?;
        if !meta.is_file() || meta.uid() != self.uid || meta.mode() & 0o777 != 0o600 {
            return Err(io::Error::other("unsafe guest browser lock file"));
        }
        lock.try_lock()?;
        Ok(lock)
    }

    fn check_peer(&self, stream: &UnixStream) -> io::Result<()> {
        if stream.peer_cred()?.uid() != self.uid {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "unexpected guest browser peer uid",
            ));
        }
        Ok(())
    }
}

struct SocketPath(PathBuf);
impl Drop for SocketPath {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

pub async fn relay(dir: &Path, args: &[OsString]) -> io::Result<()> {
    let dir = RuntimeDir::open(dir)?;
    let stream = tokio::time::timeout(STARTUP, connect_or_start(&dir, args))
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "guest browser runtime busy or unavailable",
            )
        })??;
    let (mut rx, mut tx) = stream.into_split();
    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let input = async {
        tokio::io::copy(&mut stdin, &mut tx).await?;
        tx.shutdown().await
    };
    let output = async {
        tokio::io::copy(&mut rx, &mut stdout).await?;
        stdout.flush().await
    };
    tokio::pin!(output);
    tokio::select! {
        result = &mut output => result,
        result = input => { result?; output.await },
        _ = terminate_signal() => Ok(()),
    }
}

async fn connect_or_start(dir: &RuntimeDir, args: &[OsString]) -> io::Result<UnixStream> {
    let mut child: Option<std::process::Child> = None;
    loop {
        if dir.socket_exists()? {
            match UnixStream::connect(&dir.socket).await {
                Ok(mut stream) => {
                    dir.check_peer(&stream)?;
                    match stream.read_u8().await {
                        Ok(READY) => return Ok(stream),
                        Ok(_) => {
                            return Err(io::Error::other("invalid guest browser relay handshake"));
                        }
                        Err(e)
                            if matches!(
                                e.kind(),
                                io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset
                            ) => {}
                        Err(e) => return Err(e),
                    }
                }
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                    ) => {}
                Err(e) => return Err(e),
            }
        }
        if let Some(process) = &mut child
            && process.try_wait()?.is_some()
        {
            child = None;
        }
        if child.is_none() {
            match dir.lock() {
                Ok(probe) => {
                    drop(probe);
                    child = Some(spawn_worker(args)?);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn spawn_worker(args: &[OsString]) -> io::Result<std::process::Child> {
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args(args)
        .arg(WORKER_ARG)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // SAFETY: the hook only makes async-signal-safe Linux syscalls and returns errno.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() < 0
                || libc::syscall(
                    libc::SYS_close_range,
                    3u32,
                    u32::MAX,
                    libc::CLOSE_RANGE_CLOEXEC,
                ) < 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    command.spawn()
}

/// Reserve the private guest arguments outside the public browser configuration.
pub fn extract_args(args: &mut Vec<OsString>) -> io::Result<Option<(PathBuf, bool)>> {
    let mut dir = None;
    let mut worker = false;
    let mut i = 0;
    while i < args.len() {
        if args[i] == OsStr::new(DIR_ARG) && dir.is_none() && i + 1 < args.len() {
            args.remove(i);
            dir = Some(PathBuf::from(args.remove(i)));
        } else if args[i] == OsStr::new(WORKER_ARG) && !worker {
            args.remove(i);
            worker = true;
        } else {
            i += 1;
        }
    }
    if worker && dir.is_none() {
        return Err(io::Error::other(
            "guest browser worker needs a runtime directory",
        ));
    }
    Ok(dir.map(|dir| (dir, worker)))
}

pub async fn worker(dir: &Path, server: BrowserMcpServer) -> io::Result<()> {
    let dir = RuntimeDir::open(dir)?;
    let _owner = dir.lock()?;
    if dir.socket_exists()? {
        fs::remove_file(&dir.socket)?;
    }
    let signal = terminate_signal();
    tokio::pin!(signal);
    // Register signal handlers before publishing the listening socket.
    let _ = futures_util::poll!(&mut signal);
    let listener = UnixListener::bind(&dir.socket)?;
    let path = SocketPath(dir.socket.clone());
    fs::set_permissions(&dir.socket, fs::Permissions::from_mode(0o600))?;
    let result = drive(&dir, &listener, &server, signal).await;
    drop(listener);
    drop(path);
    server.shutdown().await;
    result
}

async fn drive(
    dir: &RuntimeDir,
    listener: &UnixListener,
    server: &BrowserMcpServer,
    signal: impl Future<Output = &'static str>,
) -> io::Result<()> {
    tokio::pin!(signal);
    let startup = tokio::time::sleep(STARTUP);
    tokio::pin!(startup);
    let mut first = true;
    let mut reclaim = None;
    let mut reap = tokio::time::interval(Duration::from_secs(1));
    reap.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let (mut peer, _) = tokio::select! {
            biased;
            _ = &mut signal => return Ok(()),
            _ = &mut startup, if first => return Ok(()),
            _ = reap.tick(), if !first => {
                if !retain_detached(server.detached_desktop().await, &mut reclaim, tokio::time::Instant::now()) {
                    return Ok(());
                }
                continue;
            }
            _ = async {
                if let Some(deadline) = reclaim {
                    tokio::time::sleep_until(deadline).await;
                }
            }, if reclaim.is_some() => return Ok(()),
            peer = listener.accept() => peer?,
        };
        if dir.check_peer(&peer).is_err() || peer.write_u8(READY).await.is_err() {
            continue;
        }
        first = false;
        reclaim = None;
        let (client, attached) = server.attach_client();
        server.start_client_monitor(attached.clone());
        // Keep the relay alive through detach/finalization even after rmcp
        // closes its framed transport. This duplicate is never used for I/O.
        let std_peer = peer.into_std()?;
        let guard = std_peer.try_clone()?;
        let (rx, tx) = tokio::io::split(UnixStream::from_std(std_peer)?);
        let serve = async {
            if let Ok(running) = client.serve((rx, FlushOnly(tx))).await {
                let _ = running.waiting().await;
            }
        };
        let stop = tokio::select! {
            _ = &mut signal => true,
            _ = serve => false,
        };
        attached.store(false, Ordering::Release);
        if stop {
            server.shutdown().await;
            return Ok(());
        }
        if !retain_detached(
            server.detached_desktop().await,
            &mut reclaim,
            tokio::time::Instant::now(),
        ) {
            server.shutdown().await;
            return Ok(());
        }
        drop(guard);
    }
}

fn retain_detached(
    desktop: DetachedDesktop,
    reclaim: &mut Option<tokio::time::Instant>,
    now: tokio::time::Instant,
) -> bool {
    match desktop {
        DetachedDesktop::None => false,
        DetachedDesktop::Viewing => true,
        DetachedDesktop::Reclaimable => now < *reclaim.get_or_insert(now + RECLAIM),
    }
}

struct FlushOnly<W>(W);
impl<W: AsyncWrite + Unpin> AsyncWrite for FlushOnly<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, data)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detached_reclaim_window_starts_once_and_expires() {
        let start = tokio::time::Instant::now();
        let mut deadline = None;
        assert!(retain_detached(
            DetachedDesktop::Viewing,
            &mut deadline,
            start
        ));
        assert!(deadline.is_none());
        assert!(retain_detached(
            DetachedDesktop::Reclaimable,
            &mut deadline,
            start
        ));
        let before = start + RECLAIM - Duration::from_millis(1);
        assert!(retain_detached(
            DetachedDesktop::Reclaimable,
            &mut deadline,
            before
        ));
        assert_eq!(deadline, Some(start + RECLAIM));
        assert!(!retain_detached(
            DetachedDesktop::Reclaimable,
            &mut deadline,
            start + RECLAIM
        ));
        assert!(!retain_detached(
            DetachedDesktop::None,
            &mut deadline,
            start
        ));
    }
}
