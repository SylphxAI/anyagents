//! Takeover delivery: where people reach a session's takeover page, and the
//! mailbox message its agent receives when they hand control back.

use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use agent_workstation::{DesktopSession, HandBack, Holder, TakeoverConfig};
use anyagents_core::{Error, now_ms};
use tokio::io::AsyncBufReadExt as _;
use tokio::process::{Child, Command};

const TUNNEL_START: Duration = Duration::from_secs(15);

/// A public tunnel to the takeover page. Dropping it stops the tunnel.
#[derive(Debug)]
pub struct Tunnel {
    _child: Child,
}

fn tunnel_enabled() -> bool {
    std::env::var("ANYAGENTS_TAKEOVER_TUNNEL").as_deref() != Ok("0")
        && std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
        && !crate::sandbox::is_isolated_test_env()
}

fn cloudflared() -> Option<std::path::PathBuf> {
    let mut candidates = Vec::new();
    if let Ok(home) = std::env::var("HOME") {
        candidates.push(Path::new(&home).join(".local/bin/cloudflared"));
    }
    candidates.push("/usr/local/bin/cloudflared".into());
    candidates.push("/usr/bin/cloudflared".into());
    candidates.into_iter().find(|p| p.is_file())
}

/// The takeover listener for a new session. When `cloudflared` is installed
/// (and tunnels are enabled), the page is published through a quick tunnel
/// so a person can open it from any device; otherwise it stays on loopback.
///
/// P11 interim: the tunnel starts before every session because the takeover
/// page only accepts the origin published at start; with a settable public
/// URL it starts on the first `takeover` instead.
pub async fn config() -> (TakeoverConfig, Option<Tunnel>) {
    let mut cfg = TakeoverConfig::default();
    if !tunnel_enabled() {
        return (cfg, None);
    }
    let Some(bin) = cloudflared() else {
        return (cfg, None);
    };
    let Ok(port) = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
    else {
        return (cfg, None);
    };
    match start_tunnel(&bin, port).await {
        Some((url, tunnel)) => {
            cfg.bind = ([127, 0, 0, 1], port).into();
            cfg.public_url = Some(url);
            (cfg, Some(tunnel))
        }
        None => (cfg, None),
    }
}

async fn start_tunnel(bin: &Path, port: u16) -> Option<(url::Url, Tunnel)> {
    let mut child = Command::new(bin)
        .args(["tunnel", "--no-autoupdate", "--url"])
        .arg(format!("http://127.0.0.1:{port}"))
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

/// Deliver a mailbox message to `owner` each time a person hands control
/// back, so the agent resumes without polling.
pub fn watch_hand_backs(
    data: &Path,
    profile: &str,
    owner: &str,
    session: Arc<DesktopSession>,
) -> tokio::task::JoinHandle<()> {
    let (data, profile, owner) = (data.to_path_buf(), profile.to_string(), owner.to_string());
    let mut holder = session.gate().subscribe();
    tokio::spawn(async move {
        let mut was_human = matches!(*holder.borrow(), Holder::Human { .. });
        while holder.changed().await.is_ok() {
            let is_human = matches!(*holder.borrow_and_update(), Holder::Human { .. });
            if was_human && !is_human {
                let note = match session.take_hand_back() {
                    Some(HandBack::Released { note }) => note,
                    Some(HandBack::Idle) => Some("ended after five idle minutes".into()),
                    None => None,
                };
                let url = session.current_url().await.unwrap_or_default();
                let (d, p, o) = (data.clone(), profile.clone(), owner.clone());
                let delivered = tokio::task::spawn_blocking(move || {
                    notify_agent_takeover_completed(
                        &d,
                        Some(&o),
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
            was_human = is_human;
        }
    })
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
        .filter(|s| !s.is_empty() && *s != "default" && *s != "operator")
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
    }
}
