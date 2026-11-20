//! Process termination signals that must trigger a graceful browser shutdown.
//!
//! Sandboxes and MCP hosts stop servers with `SIGTERM` (or `SIGHUP`/`SIGINT`)
//! rather than closing stdin. Without handling, the process dies before the
//! browser flushes its cookie store and before the named profile is synced
//! back, while the browser (in its own process group) is orphaned.

/// Resolve with the signal name once the process is asked to terminate.
#[cfg(unix)]
pub async fn terminate_signal() -> &'static str {
    use tokio::signal::unix::{SignalKind, signal};
    let (Ok(mut term), Ok(mut int), Ok(mut hup)) = (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
        signal(SignalKind::hangup()),
    ) else {
        std::future::pending::<()>().await;
        unreachable!()
    };
    tokio::select! {
        _ = term.recv() => "SIGTERM",
        _ = int.recv() => "SIGINT",
        _ = hup.recv() => "SIGHUP",
    }
}

/// Resolve with the signal name once the process is asked to terminate.
#[cfg(not(unix))]
pub async fn terminate_signal() -> &'static str {
    if tokio::signal::ctrl_c().await.is_err() {
        std::future::pending::<()>().await;
    }
    "Ctrl-C"
}
