//! A request that waits on the disk or the store holds only itself.
//!
//! The server below runs on a runtime with exactly one async worker. If any
//! step of a request (the caller's authentication or the handler) blocked that
//! worker, the cheap health route could not answer until the blocked request
//! finished; each test holds a request inside a blocking read and asks the
//! health route meanwhile.

use std::io::{Read as _, Write as _};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyagents_core::Store;
use anyagents_supervisor::HealthBoard;

use super::{AppState, ProcessRunner, build_router};
use crate::auth;
use crate::identity::ServingIdentity;

const OPERATOR: &str = "0123456789abcdef0123456789abcdef";

/// The router on its own one-worker runtime, until the returned sender drops.
fn one_worker_server(state: AppState, socket: PathBuf) -> tokio::sync::oneshot::Sender<()> {
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let (bound, ready) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let listener = tokio::net::UnixListener::bind(&socket).unwrap();
            bound.send(()).unwrap();
            let _ = axum::serve(listener, build_router(state))
                .with_graceful_shutdown(async move {
                    let _ = stopped.await;
                })
                .await;
        });
    });
    ready.recv().unwrap();
    stop
}

/// One HTTP/1.1 exchange; the status line, or the error when nothing came back
/// within `wait`.
fn call(
    socket: &Path,
    path: &str,
    headers: &[(&str, &str)],
    wait: Duration,
) -> Result<String, String> {
    let mut stream = UnixStream::connect(socket).map_err(|err| err.to_string())?;
    stream
        .set_read_timeout(Some(wait))
        .map_err(|err| err.to_string())?;
    let mut request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|err| err.to_string())?;
    let mut answer = String::new();
    stream
        .read_to_string(&mut answer)
        .map_err(|err| format!("no answer within {wait:?}: {err}"))?;
    Ok(answer)
}

fn state(dir: &Path) -> AppState {
    AppState {
        store: Arc::new(Store::open(dir.join("state.db")).unwrap()),
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        auth: auth::Mode::Confined {
            operator_digest: Some(auth::operator_digest(OPERATOR)),
        },
        identity: Arc::new(ServingIdentity::capture()),
    }
}

fn healthz_answers(socket: &Path) {
    let answer = call(socket, "/v1/webhooks/healthz", &[], Duration::from_secs(2))
        .expect("the health route answers while another request waits");
    assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
}

/// Authentication that waits on the store holds its own request only. A server
/// whose store does not hold the capability key asks the primary's socket; that
/// primary accepts and never answers, so the check waits in a blocking read.
#[test]
fn an_authentication_that_waits_never_holds_the_async_worker() {
    let dir = crate::test_tempdir();
    let primary = UnixListener::bind(dir.path().join("anyagents.sock")).unwrap();
    let api = dir.path().join("api.sock");
    let stop = one_worker_server(state(dir.path()), api.clone());

    let waiting = {
        let api = api.clone();
        std::thread::spawn(move || {
            call(
                &api,
                "/v1/agents",
                &[("x-anyagents-agent", "a-capability-the-primary-must-check")],
                Duration::from_secs(10),
            )
        })
    };
    // The check is now inside its blocking read, with nobody answering.
    let (held, _) = primary.accept().unwrap();
    healthz_answers(&api);
    drop(held);
    let refused = waiting.join().unwrap().unwrap();
    assert!(refused.starts_with("HTTP/1.1 401"), "{refused}");
    drop(stop);
}

/// A handler whose file read waits on the disk holds its own request only. The
/// subagent defaults are a FIFO with no writer, so the read waits until the
/// test writes them.
#[test]
fn a_handler_whose_read_waits_never_holds_the_async_worker() {
    use std::os::unix::ffi::OsStrExt as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    let dir = crate::test_tempdir();
    let defaults = dir.path().join("subagent-defaults.json");
    let fifo = std::ffi::CString::new(defaults.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    let api = dir.path().join("api.sock");
    let stop = one_worker_server(state(dir.path()), api.clone());

    let waiting = {
        let api = api.clone();
        std::thread::spawn(move || {
            call(
                &api,
                "/v1/subagent-defaults",
                &[("x-anyagents-operator", OPERATOR)],
                Duration::from_secs(10),
            )
        })
    };
    // A writer can open the FIFO only once the handler has it open for reading;
    // the handler then waits in its read until the writer speaks.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut writer = loop {
        match std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&defaults)
        {
            Ok(writer) => break writer,
            Err(err) if err.raw_os_error() == Some(libc::ENXIO) && Instant::now() < deadline => {
                std::thread::yield_now();
            }
            Err(err) => panic!("the handler never opened its defaults: {err}"),
        }
    };
    healthz_answers(&api);
    writer
        .write_all(br#"{"default":{"mode":"preset","harness":"codex"}}"#)
        .unwrap();
    drop(writer);
    let answered = waiting.join().unwrap().unwrap();
    assert!(answered.starts_with("HTTP/1.1 200"), "{answered}");
    assert!(answered.contains(r#""harness":"codex""#), "{answered}");
    drop(stop);
}

mod views {
    use anyagents_core::{Agent, NewAgent, Store, agent_config_dir, now_ms};
    use anyagents_supervisor::HealthBoard;
    use serde_json::{Value, json};

    #[cfg(target_os = "linux")]
    use super::super::file_cache::FileCache;
    #[cfg(target_os = "linux")]
    use super::super::{BOUND_PROFILE, PROFILE_ROTATED, QUOTA_CLEARED};
    use super::super::{LAST_ERROR, ListAgentsQuery, NOTIFIED_PARENT, agent_view, roster_views};

    fn roster(store: &Store, health: &HealthBoard) -> Value {
        let (agents, archived) = store.roster_agents(None).unwrap();
        let rows: Vec<&Agent> = agents.iter().collect();
        let listed =
            roster_views(store, health, &rows, archived, &ListAgentsQuery::default()).unwrap();
        serde_json::to_value(listed).unwrap()
    }

    #[cfg(target_os = "linux")]
    fn named(roster: &Value, name: &str) -> Value {
        roster["agents"]
            .as_array()
            .unwrap()
            .iter()
            .find(|view| view["name"] == name)
            .unwrap()
            .clone()
    }

    fn running(store: &Store, new: NewAgent) -> Agent {
        let agent = store.create_agent(new).unwrap();
        store.set_agent_state(&agent.id, "running").unwrap()
    }

    fn write(store: &Store, agent: &Agent, name: &str, contents: impl AsRef<[u8]>) {
        let cfg = agent_config_dir(store.data_dir(), &agent.id);
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::write(cfg.join(name), contents).unwrap();
    }

    /// Once its markers are cached a roster, and a single view, read no file,
    /// and they show what a fresh read shows; a write by any hand is seen on
    /// the next call. (Without inotify every view reads its files.)
    #[cfg(target_os = "linux")]
    #[test]
    fn a_steady_roster_reads_no_file_and_shows_what_the_files_say() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let now = now_ms();
        let root = running(&store, NewAgent::new("root-a", "codex"));
        let mut new_sub = NewAgent::new("sub-b", "codex");
        new_sub.parent_id = Some(root.id.clone());
        let sub = running(&store, new_sub);
        let agy = running(&store, NewAgent::new("agy-c", "agy"));
        let plain = running(&store, NewAgent::new("plain-d", "claude-code"));
        let held_until = now + 3_600_000;
        let fault = |error: &str, reset: Option<i64>| {
            json!({ "timestamp_ms": now - 1_000, "error": error, "quota_reset_at_ms": reset })
                .to_string()
        };
        write(
            &store,
            &root,
            LAST_ERROR,
            fault("rate_limit hit", Some(held_until)),
        );
        write(&store, &root, PROFILE_ROTATED, format!("{}\n", now - 5_000));
        write(&store, &root, BOUND_PROFILE, "openai/second\n");
        write(&store, &sub, LAST_ERROR, fault("tool crashed", None));
        // The parent was already told of this episode.
        let episode = format!("subagent-error:{}:0:tool crashed", sub.id.as_str());
        write(
            &store,
            &sub,
            NOTIFIED_PARENT,
            json!({ "source_key": episode }).to_string(),
        );
        // AGY's fault predates its quota clear, so it no longer counts.
        write(&store, &agy, LAST_ERROR, fault("quota", Some(held_until)));
        write(&store, &agy, QUOTA_CLEARED, format!("{now}\n"));
        write(&store, &plain, BOUND_PROFILE, "anthropic/default\n");

        let first = roster(&store, &health);
        let cold_reads = FileCache::loads_under(dir.path());
        assert!(cold_reads > 0);
        let second = roster(&store, &health);
        for agent in [&root, &sub, &agy, &plain] {
            let single = serde_json::to_value(agent_view(&store, &health, agent).unwrap()).unwrap();
            assert_eq!(single, named(&second, &agent.name));
        }
        assert_eq!(
            FileCache::loads_under(dir.path()),
            cold_reads,
            "a steady roster and view read no file"
        );
        assert_eq!(first, second);

        let root_view = named(&first, "root-a");
        assert_eq!(root_view["last_error"], "rate_limit hit");
        assert_eq!(root_view["quota_reset_at_ms"], held_until);
        assert_eq!(root_view["block_reason"], "quota_exhausted");
        assert_eq!(root_view["turn_state"], "idle");
        assert_eq!(root_view["profile"], "openai/second");
        let sub_view = named(&first, "sub-b");
        assert_eq!(sub_view["last_error"], "tool crashed");
        assert_eq!(sub_view["turn_state"], "errored");
        assert_eq!(sub_view["block_reason"], "delivery_error");
        assert!(store.inbox(&root.id, None).unwrap().is_empty());
        let agy_view = named(&first, "agy-c");
        assert_eq!(agy_view["last_error"], Value::Null);
        assert_eq!(agy_view["quota_reset_at_ms"], Value::Null);
        let plain_view = named(&first, "plain-d");
        assert_eq!(plain_view["profile"], "anthropic/default");
        assert_eq!(plain_view["last_error"], Value::Null);

        // Another hand records a fault; the next roster shows it.
        write(
            &store,
            &plain,
            LAST_ERROR,
            json!({ "timestamp_ms": now, "error": "not logged in" }).to_string(),
        );
        let third = roster(&store, &health);
        let plain_view = named(&third, "plain-d");
        assert_eq!(plain_view["last_error"], "not logged in");
        assert_eq!(plain_view["block_reason"], "sign_in_required");
        assert_eq!(named(&third, "root-a"), named(&first, "root-a"));
    }

    /// A parent hears of a subagent's fault once per episode, and the marker
    /// is cleared once the fault is gone, as a view always did.
    #[test]
    fn a_parent_hears_of_a_subagent_fault_once() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let parent = running(&store, NewAgent::new("parent-e", "codex"));
        let mut new_child = NewAgent::new("child-f", "codex");
        new_child.parent_id = Some(parent.id.clone());
        let child = running(&store, new_child);
        write(
            &store,
            &child,
            LAST_ERROR,
            json!({ "timestamp_ms": now_ms(), "error": "tool crashed" }).to_string(),
        );
        for _ in 0..3 {
            roster(&store, &health);
            agent_view(&store, &health, &child).unwrap();
        }
        let told = store.inbox(&parent.id, None).unwrap();
        assert_eq!(told.len(), 1);
        assert!(told[0].text.starts_with("[Sub-Agent Error]"));
        let notice = agent_config_dir(store.data_dir(), &child.id).join(NOTIFIED_PARENT);
        assert!(notice.exists());

        std::fs::remove_file(agent_config_dir(store.data_dir(), &child.id).join(LAST_ERROR))
            .unwrap();
        roster(&store, &health);
        assert!(!notice.exists());
        assert_eq!(store.inbox(&parent.id, None).unwrap().len(), 1);
    }
}
