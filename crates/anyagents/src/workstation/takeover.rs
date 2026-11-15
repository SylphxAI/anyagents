//! Takeover delivery: where people reach a session's takeover page, and the
//! mailbox message the agent holding the session receives when they hand
//! control back.
//!
//! Where the page is reached, in order:
//!
//! 1. **The server's public entry.** When the public listener runs
//!    (`ANYAGENTS_WEBHOOK_LISTEN`) and `ANYAGENTS_WEBHOOK_URL` names where
//!    people reach it, every session's page is mounted on that listener at
//!    `/v1/webhooks/takeover/<key>/` (the prefix the public entry already
//!    forwards). No tunnel or extra port is involved.
//! 2. **A quick tunnel,** started on the first `takeover` of a session when
//!    the toolchain has `cloudflared` (`browser.takeover`); the page accepts
//!    the tunnel's origin from then on.
//! 3. **Loopback,** otherwise.

use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use agent_workstation::{DesktopSession, HandBack};
use anyagents_core::{Error, now_ms};
use tokio::io::AsyncBufReadExt as _;
use tokio::process::{Child, Command};

const TUNNEL_START: Duration = Duration::from_secs(15);
/// Path prefix of takeover pages on the public listener.
pub const MOUNT_PREFIX: &str = "/v1/webhooks/takeover/";

/// A public tunnel to the takeover page. Dropping it stops the tunnel.
#[derive(Debug)]
pub struct Tunnel {
    _child: Child,
}

/// Takeover pages mounted on the public listener, by key.
static MOUNTS: LazyLock<std::sync::Mutex<HashMap<String, axum::Router>>> =
    LazyLock::new(Default::default);

/// The public base people reach the server's public listener at, when both
/// are configured.
fn public_entry(data: &Path) -> Option<url::Url> {
    if !crate::server::public_entry_listening(data) {
        return None;
    }
    let base = std::env::var("ANYAGENTS_WEBHOOK_URL").ok()?;
    let base = base.trim().trim_end_matches('/');
    url::Url::parse(&format!("{base}/")).ok()
}

fn random_key() -> Option<String> {
    use std::io::Read as _;
    let mut b = [0u8; 16];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut b))
        .ok()?;
    Some(b.iter().map(|x| format!("{x:02x}")).collect())
}

/// Mount `session`'s takeover page on the public listener when the server
/// has a public entry; returns its key.
pub fn mount(data: &Path, session: &DesktopSession) -> Option<String> {
    let base = public_entry(data)?;
    let router = session.takeover_router()?;
    let key = random_key()?;
    let url = base
        .join(&format!("{}{key}/", MOUNT_PREFIX.trim_start_matches('/')))
        .ok()?;
    session.set_takeover_public_url(url).ok()?;
    MOUNTS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(key.clone(), router);
    Some(key)
}

/// Mount `router` as a page under `key` (tests).
#[cfg(test)]
pub fn mount_router(key: &str, router: axum::Router) {
    MOUNTS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(key.to_string(), router);
}

/// Remove a mounted page.
pub fn unmount(key: &str) {
    MOUNTS.lock().unwrap_or_else(|p| p.into_inner()).remove(key);
}

/// The mounted page for `key`.
pub fn mounted(key: &str) -> Option<axum::Router> {
    MOUNTS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(key)
        .cloned()
}

/// Start a quick tunnel to the takeover listener at `addr`. `None` when
/// tunnels are off, `cloudflared` is not in the toolchain, or it gave no URL.
pub async fn start_tunnel(addr: std::net::SocketAddr) -> Option<(url::Url, Tunnel)> {
    if std::env::var("ANYAGENTS_TAKEOVER_TUNNEL").as_deref() == Ok("0")
        || std::env::var_os("ANYAGENTS_FAKE_MODE").is_some()
        || crate::sandbox::is_isolated_test_env()
    {
        return None;
    }
    let bin = anyagents_toolchain::tool("cloudflared", "browser.takeover").ok()?;
    let mut child = Command::from(bin.command())
        .args(["tunnel", "--no-autoupdate", "--url"])
        .arg(format!("http://{addr}"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .process_group(0)
        .spawn()
        .ok()?;
    let mut lines = tokio::io::BufReader::new(child.stderr.take()?).lines();
    let found = tokio::time::timeout(TUNNEL_START, async {
        while let Ok(Some(line)) = lines.next_line().await {
            if let Some(url) = trycloudflare_url(&line) {
                return Some(url);
            }
        }
        None
    })
    .await
    .ok()
    .flatten()?;
    // Keep draining so cloudflared never blocks on a full pipe.
    tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
    let url = url::Url::parse(&format!("{}/", found.trim_end_matches('/'))).ok()?;
    Some((url, Tunnel { _child: child }))
}

fn trycloudflare_url(line: &str) -> Option<String> {
    let idx = line.find("https://")?;
    let rest = &line[idx..];
    let end = rest
        .find(|c: char| c.is_whitespace() || c == '|' || c == '"' || c == '\'')
        .unwrap_or(rest.len());
    let candidate = &rest[..end];
    candidate
        .contains(".trycloudflare.com")
        .then(|| candidate.to_string())
}

/// Count every hand-back from a person (the returned receiver), and deliver
/// a mailbox message to the agent holding the session for each, so it
/// resumes without polling. Hand-backs are read from the gate's counter, so
/// one between two wake-ups is never missed.
pub fn watch_hand_backs(
    data: &Path,
    profile: &str,
    session: Arc<DesktopSession>,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::watch::Receiver<u64>,
) {
    let (data, profile) = (data.to_path_buf(), profile.to_string());
    let mut holder = session.gate().subscribe();
    let mut seen = session.gate().hand_backs();
    let (count, hand_backs) = tokio::sync::watch::channel(seen);
    let task = tokio::spawn(async move {
        while holder.changed().await.is_ok() {
            holder.borrow_and_update();
            let now = session.gate().hand_backs();
            if now == seen {
                continue;
            }
            seen = now;
            count.send_replace(now);
            let note = match session.gate().take_hand_back() {
                Some(HandBack::Released { note }) => note,
                Some(HandBack::Idle) => Some("ended after five idle minutes".into()),
                None => None,
            };
            let Some(owner) = session.leaseholder().map(|l| l.principal) else {
                continue;
            };
            let url = match session.lease(&owner) {
                Ok(agent) => agent.current_url().await.unwrap_or_default(),
                Err(_) => String::new(),
            };
            let (d, p) = (data.clone(), profile.clone());
            let delivered = tokio::task::spawn_blocking(move || {
                notify_agent_takeover_completed(
                    &d,
                    Some(&owner),
                    &p,
                    &url,
                    note.as_deref(),
                    now_ms(),
                )
            })
            .await;
            if let Ok(Err(e)) = delivered {
                eprintln!("browser takeover completion for `{profile}` not delivered: {e}");
            }
        }
    });
    (task, hand_backs)
}

fn notify_agent_takeover_completed(
    data: &Path,
    owner_agent: Option<&str>,
    profile: &str,
    url: &str,
    note: Option<&str>,
    completion_id: i64,
) -> Result<(), Error> {
    let Some(agent_name) = owner_agent
        .map(str::trim)
        .filter(|s| !s.is_empty() && *s != "default" && *s != super::OPERATOR)
    else {
        return Ok(());
    };
    let db_path = data.join("state.db");
    if !db_path.try_exists()? {
        return Err(Error::Message(
            "takeover completion owner store is missing".into(),
        ));
    }
    let store = anyagents_core::Store::open(db_path)?;
    let agent = store.agent_by_name(agent_name)?;
    let now = now_ms();
    let msg_id = format!("msg_takeover_{profile}_{completion_id}");
    let note_suffix = note
        .filter(|s| !s.trim().is_empty())
        .map(|n| format!(" ({n})"))
        .unwrap_or_default();
    let text = format!(
        "[Browser Takeover Completed] User finished browser takeover on profile `{profile}`{note_suffix} (current url: {url}). Control is now back with you; call `browser({{ \"action\": \"snapshot\", \"profile\": \"{profile}\" }})` to inspect the updated page and continue."
    );
    let outcome = store.receive(
        &agent.id,
        anyagents_core::Incoming {
            source_key: Some(msg_id),
            text,
            sender: Some("system:browser-takeover".to_string()),
            reply_to: None,
            quote: None,
            source_time_ms: Some(now),
            received_at_ms: Some(now),
            origin: None,
        },
    )?;
    // Receipt replay must not replay lifecycle effects after a later archive.
    if matches!(outcome, anyagents_core::ReceiveOutcome::Duplicate(_)) {
        return Ok(());
    }
    if agent.state == "idle" || agent.state == "archived" {
        store.set_agent_state(&agent.id, "running")?;
    }
    crate::create::poke_agent_wake_socket(data, &agent.id);
    crate::create::poke_agent_wake_socket_mail(data, &agent.id);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tunnel_url_is_read_from_cloudflared_output() {
        assert_eq!(
            trycloudflare_url("INF |  https://quiet-river.trycloudflare.com  |").as_deref(),
            Some("https://quiet-river.trycloudflare.com")
        );
        assert_eq!(trycloudflare_url("INF https://example.com"), None);
    }

    #[test]
    fn hand_back_notification_wakes_the_owner_once() {
        let dir = crate::test_tempdir();
        let store = anyagents_core::Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("reader", "codex"))
            .unwrap();
        store.set_agent_state(&agent.id, "idle").unwrap();
        notify_agent_takeover_completed(
            dir.path(),
            Some("reader"),
            "p",
            "https://x/",
            Some("done"),
            7,
        )
        .unwrap();
        assert_eq!(store.agent_by_name("reader").unwrap().state, "running");
        // A replayed receipt neither duplicates the message nor wakes an archived owner.
        store.set_agent_state(&agent.id, "archived").unwrap();
        notify_agent_takeover_completed(
            dir.path(),
            Some("reader"),
            "p",
            "https://x/",
            Some("done"),
            7,
        )
        .unwrap();
        assert_eq!(store.agent_by_name("reader").unwrap().state, "archived");
        let inbox = store.inbox(&agent.id, None).unwrap();
        assert_eq!(inbox.len(), 1);
        assert!(
            inbox[0].text.contains("[Browser Takeover Completed]")
                && inbox[0].text.contains("(done)")
        );
        // The operator and shared default profile have no owner to notify.
        notify_agent_takeover_completed(dir.path(), Some("default"), "p", "u", None, 8).unwrap();
        notify_agent_takeover_completed(
            dir.path(),
            Some(super::super::OPERATOR),
            "p",
            "u",
            None,
            9,
        )
        .unwrap();
    }
}
