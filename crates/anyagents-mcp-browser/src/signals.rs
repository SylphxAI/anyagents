//! Process termination signals that must trigger a graceful browser shutdown.
//!
//! Sandboxes and MCP hosts stop servers with `SIGTERM` (or `SIGHUP`/`SIGINT`)
//! rather than closing stdin. Without handling, the process dies before the
//! browser flushes its cookie store and before the named profile is synced
//! back, while the browser (in its own process group) is orphaned.

/// Resolve with the signal name once the process is asked to terminate.
///
/// Every signal whose stream registers successfully is watched; the future
/// resolves with the first delivered signal name.
#[cfg(unix)]
pub async fn terminate_signal() -> &'static str {
    use tokio::signal::unix::{SignalKind, signal};

    let (tx, mut rx) = tokio::sync::mpsc::channel::<&'static str>(1);
    for (kind, name) in [
        (SignalKind::terminate(), "SIGTERM"),
        (SignalKind::interrupt(), "SIGINT"),
        (SignalKind::hangup(), "SIGHUP"),
    ] {
        match signal(kind) {
            Ok(mut stream) => {
                let tx = tx.clone();
                tokio::spawn(async move {
                    while stream.recv().await.is_some() {
                        if tx.send(name).await.is_err() {
                            return;
                        }
                    }
                });
            }
            Err(err) => {
                eprintln!("anyagents-mcp-browser: cannot watch {name}: {err}");
            }
        }
    }
    rx.recv().await.unwrap_or("unknown")
}

/// Resolve with the signal name once the process is asked to terminate.
#[cfg(not(unix))]
pub async fn terminate_signal() -> &'static str {
    if tokio::signal::ctrl_c().await.is_err() {
        std::future::pending::<()>().await;
    }
    "Ctrl-C"
}
