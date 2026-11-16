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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use agent_workstation::{DesktopSession, HandBack, Holder, PageEvidence};
use anyagents_core::{Error, now_ms};
use tokio::io::AsyncBufReadExt as _;
use tokio::process::{Child, Command};

const TUNNEL_START: Duration = Duration::from_secs(15);
const SOLVE_POLL_INTERVAL: Duration = Duration::from_millis(1500);
const SOLVE_REQUIRED_READS: u32 = 2;

/// Neutral light-DOM challenge widget/container selectors matching
/// `DETECT_CAPTCHA_JS`, within `Handoff::observe`'s 32-selector budget.
const CHALLENGE_SELECTORS: &[&str] = &[
    "#cf-chl-widget",
    ".cf-turnstile",
    "#challenge-running",
    "#challenge-stage",
    r#"iframe[src*="challenges.cloudflare.com"]"#,
    r#"iframe[src*="turnstile"]"#,
    ".g-recaptcha:not(.grecaptcha-badge)",
    "#g-recaptcha",
    r#"iframe[src*="google.com/recaptcha"]"#,
    r#"iframe[src*="recaptcha/api2"]"#,
    r#"iframe[src*="recaptcha/enterprise"]"#,
    ".h-captcha",
    r#"iframe[src*="hcaptcha.com"]"#,
    "#arkose",
    "[data-arkose]",
    r#"iframe[src*="arkoselabs.com"]"#,
    r#"iframe[src*="funcaptcha.com"]"#,
    ".geetest_panel",
    ".geetest_holder",
    ".nc_scale",
    "#nc_1_n1z",
    "[data-captcha]",
    "#captcha-box",
];
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

fn challenge_visible(ev: &PageEvidence) -> bool {
    !ev.truncated && ev.selectors.iter().any(|s| s.visible)
}

fn has_solved_token(ev: &PageEvidence) -> bool {
    !ev.truncated
        && ev.forms.iter().any(|f| {
            f.value_nonempty
                && (f.name.starts_with("g-recaptcha-response")
                    || f.name == "h-captcha-response"
                    || f.name == "cf-turnstile-response")
        })
}

/// Positive proof that a challenge seen on `initial` was solved: a newly filled
/// response token appeared, or the page navigated to another `http(s)` address
/// (ignoring `#fragment`) where no challenge selector is visible. Same-page
/// disappearance or truncated evidence never resolves as solved.
fn challenge_solved(initial: &PageEvidence, now: &PageEvidence) -> bool {
    if initial.truncated || now.truncated {
        return false;
    }
    fn address(url: &str) -> &str {
        url.split_once('#').map_or(url, |(a, _)| a)
    }
    (has_solved_token(now) && !has_solved_token(initial))
        || (!challenge_visible(now)
            && (now.url.starts_with("https://") || now.url.starts_with("http://"))
            && address(&now.url) != address(&initial.url))
}

fn hand_back_note(reason: Option<HandBack>) -> Option<String> {
    match reason {
        Some(HandBack::Released { note }) => note,
        Some(HandBack::Idle) => Some("ended after five idle minutes".into()),
        Some(HandBack::Confirmed) => {
            Some("CAPTCHA challenge automatically detected as solved".into())
        }
        None => None,
    }
}

/// Observe one cycle-fenced snapshot during human takeover. Returns `true` if
/// auto-completion triggered, or `false` to continue watching.
async fn observe_challenge_step(
    session: &DesktopSession,
    watch_challenge: &AtomicBool,
    initial: &mut Option<PageEvidence>,
    consecutive_solved: &mut u32,
    skip_non_challenge_cycle: &mut bool,
) -> bool {
    let Some(owner) = session.leaseholder().map(|l| l.principal) else {
        *consecutive_solved = 0;
        return false;
    };
    let Ok(handoff) = session.handoff(&owner) else {
        *consecutive_solved = 0;
        return false;
    };
    let Ok(obs) = handoff.observe(CHALLENGE_SELECTORS).await else {
        *consecutive_solved = 0;
        return false;
    };
    let ev = obs.evidence();
    if ev.truncated {
        *consecutive_solved = 0;
        return false;
    }
    if let Some(init) = initial.as_ref() {
        if challenge_solved(init, ev) {
            *consecutive_solved += 1;
            if *consecutive_solved >= SOLVE_REQUIRED_READS {
                return obs.complete().is_ok();
            }
        } else {
            *consecutive_solved = 0;
        }
        return false;
    }
    if watch_challenge.load(Ordering::Acquire) || challenge_visible(ev) {
        *initial = Some(ev.clone());
    } else {
        *skip_non_challenge_cycle = true;
    }
    false
}

/// Count every hand-back from a person (the returned receiver), and deliver
/// a mailbox message to the agent holding the session for each, so it
/// resumes without polling. Hand-backs are read from the gate's counter, so
/// one between two wake-ups is never missed. While a person controls a
/// challenged page, positive solve proof (`Handoff::observe`) can complete the
/// cycle without relaxing `ControlGate` isolation.
pub fn watch_hand_backs(
    data: &Path,
    profile: &str,
    session: Arc<DesktopSession>,
    watch_challenge: Arc<AtomicBool>,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::watch::Receiver<u64>,
) {
    let (data, profile) = (data.to_path_buf(), profile.to_string());
    let mut holder = session.gate().subscribe();
    let mut seen = session.gate().hand_backs();
    let (count, hand_backs) = tokio::sync::watch::channel(seen);
    let task = tokio::spawn(async move {
        let mut initial: Option<PageEvidence> = None;
        let mut consecutive_solved = 0u32;
        let mut skip_non_challenge_cycle = false;
        loop {
            holder.borrow_and_update();
            let now = session.gate().hand_backs();
            if now != seen {
                seen = now;
                initial = None;
                consecutive_solved = 0;
                skip_non_challenge_cycle = false;
                watch_challenge.store(false, Ordering::Release);
                count.send_replace(now);
                let note = hand_back_note(session.gate().take_hand_back());
                if let Some(owner) = session.leaseholder().map(|l| l.principal) {
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
            }
            if session.is_closed() {
                break;
            }
            let in_human = matches!(*holder.borrow(), Holder::Human { .. });
            if !in_human {
                initial = None;
                consecutive_solved = 0;
                skip_non_challenge_cycle = false;
                if holder.changed().await.is_err() {
                    break;
                }
                continue;
            }
            if skip_non_challenge_cycle {
                if holder.changed().await.is_err() {
                    break;
                }
                continue;
            }
            // Only sleep between polls once the initial baseline is captured;
            // capture immediately on transition into human control.
            if initial.is_some() {
                tokio::select! {
                    changed = holder.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        continue;
                    }
                    () = tokio::time::sleep(SOLVE_POLL_INTERVAL) => {}
                }
            }
            if !observe_challenge_step(
                &session,
                &watch_challenge,
                &mut initial,
                &mut consecutive_solved,
                &mut skip_non_challenge_cycle,
            )
            .await
                && initial.is_none()
                && !skip_non_challenge_cycle
            {
                // Transient observe error before baseline; wait before retrying.
                tokio::select! {
                    changed = holder.changed() => {
                        if changed.is_err() {
                            break;
                        }
                    }
                    () = tokio::time::sleep(SOLVE_POLL_INTERVAL) => {}
                }
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

    fn evidence(
        url: &str,
        visible_challenge: bool,
        solved_token: bool,
        truncated: bool,
    ) -> PageEvidence {
        PageEvidence {
            target_id: "tab-1".into(),
            url: url.into(),
            forms: vec![agent_workstation::FormState {
                tag: "textarea".into(),
                id: "cf-token".into(),
                name: "cf-turnstile-response".into(),
                value_nonempty: solved_token,
            }],
            selectors: vec![agent_workstation::SelectorState {
                matched: visible_challenge,
                visible: visible_challenge,
            }],
            truncated,
        }
    }

    #[test]
    fn challenge_solved_requires_token_or_clean_http_navigation() {
        let initial = evidence("https://example.com/login#start", true, false, false);
        // Positive: newly filled challenge token on same page.
        assert!(challenge_solved(
            &initial,
            &evidence("https://example.com/login#start", true, true, false)
        ));
        // Positive: clean navigation to another http(s) address without challenge.
        assert!(challenge_solved(
            &initial,
            &evidence("https://example.com/dashboard#done", false, false, false)
        ));

        // Negative (#310): challenge disappears on same page with no token.
        assert!(!challenge_solved(
            &initial,
            &evidence("https://example.com/login#start", false, false, false)
        ));
        // Negative (#310): only #fragment changed with no token.
        assert!(!challenge_solved(
            &initial,
            &evidence("https://example.com/login#reset", false, false, false)
        ));
        // Negative (#310): navigated to another challenged page.
        assert!(!challenge_solved(
            &initial,
            &evidence("https://example.com/step2", true, false, false)
        ));
        // Negative (#310): navigated to non-http(s) URL (about:blank).
        assert!(!challenge_solved(
            &initial,
            &evidence("about:blank", false, false, false)
        ));
        // Negative (#310): truncated evidence never solves.
        assert!(!challenge_solved(
            &initial,
            &evidence("https://example.com/dashboard", false, true, true)
        ));
        assert!(!challenge_solved(
            &evidence("https://example.com/login", true, false, true),
            &evidence("https://example.com/dashboard", false, true, false)
        ));
        // Negative: pre-existing filled token without new solve on same URL.
        assert!(!challenge_solved(
            &evidence("https://example.com/login", true, true, false),
            &evidence("https://example.com/login", false, true, false)
        ));
    }

    #[test]
    fn hand_back_confirmed_reason_formats_auto_solve_note() {
        assert_eq!(
            hand_back_note(Some(HandBack::Confirmed)).as_deref(),
            Some("CAPTCHA challenge automatically detected as solved")
        );
        assert_eq!(
            hand_back_note(Some(HandBack::Idle)).as_deref(),
            Some("ended after five idle minutes")
        );
        assert_eq!(
            hand_back_note(Some(HandBack::Released {
                note: Some("done".into())
            }))
            .as_deref(),
            Some("done")
        );
    }
}
