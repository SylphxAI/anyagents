//! A kernel restart in the middle of a remote session: the stopping kernel
//! leaves the session running and attached; the next kernel's supervisor takes
//! it over from its record, replays only what it missed, and completes the
//! open turn without a `[Recovery]` note, a second write or a second usage
//! row. Driven through the real supervisor and delivery loop.

mod support;

use std::sync::Arc;

use anyagents_core::{Agent, AgentId, Incoming, Store};
use anyagents_remote::Executor;
use anyagents_supervisor::{Attached, Custody, LaunchRequest, Supervisor, SystemClock};
use support::{Vendor, config, store_with_agent, transcript_lines, wait_until};

/// Remote sessions as the server's custody holds them: they always outlive
/// the kernel, are taken over from their record, and are settled by the
/// agent's state when no running agent takes them.
struct RemoteCustody {
    store: Arc<Store>,
    vendor: Vendor,
}

impl RemoteCustody {
    fn config(&self, agent: &Agent) -> anyagents_remote::Config {
        config(&self.vendor, &self.store, agent, 2500)
    }
}

impl Custody<Executor> for RemoteCustody {
    fn outlives_kernel(&self) -> bool {
        true
    }

    fn retained(&self) -> Vec<AgentId> {
        anyagents_remote::attached(&self.store)
    }

    fn attach(&self, agent: &AgentId) -> Result<Option<Attached<Executor>>, String> {
        let agent = self.store.agent(agent).map_err(|e| e.to_string())?;
        let Some((executor, lock)) = anyagents_remote::take_over(self.config(&agent))? else {
            return Ok(None);
        };
        Ok(Some(Attached {
            harness_name: executor.harness().to_string(),
            model: executor.model().map(str::to_string),
            started_at_ms: Some(executor.started_at_ms()),
            note: executor.note(),
            harness: executor,
            lock,
            older: Vec::new(),
        }))
    }

    fn release(&self, agent: &AgentId) {
        let adapter = self.config(&self.store.agent(agent).unwrap()).adapter;
        anyagents_remote::settle(&self.store, agent, adapter.as_ref());
    }

    fn retain(&self, harness: Executor) {
        harness.detach();
    }

    fn note(&self, harness: &Executor, note: &str) {
        harness.set_note(note);
    }
}

fn start(store: &Arc<Store>, vendor: &Vendor) -> Supervisor<Executor> {
    let launch_store = Arc::clone(store);
    let launch_vendor = vendor.clone();
    Supervisor::start_with_custody(
        Arc::clone(store),
        Arc::new(SystemClock::new()),
        Arc::new(move |request: LaunchRequest| {
            Executor::launch(config(&launch_vendor, &launch_store, &request.agent, 2500))
        }),
        Vec::new(),
        Arc::new(RemoteCustody {
            store: Arc::clone(store),
            vendor: vendor.clone(),
        }),
    )
    .unwrap()
}

fn within(what: &str, stop: impl FnOnce() + Send + 'static) {
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        stop();
        let _ = done.send(());
    });
    assert!(
        finished
            .recv_timeout(std::time::Duration::from_secs(30))
            .is_ok(),
        "never: {what}"
    );
}

#[test]
fn a_kernel_restart_mid_turn_takes_the_session_over_without_recovery_or_duplicates() {
    let vendor = Vendor::start();
    vendor.set_hold(true);
    let (_dir, store, agent) = store_with_agent("running");
    store.receive(&agent.id, Incoming::new("hello")).unwrap();

    // The first kernel creates the session and delivers the mail; the turn
    // runs and is still open when the kernel stops.
    let first = start(&store, &vendor);
    wait_until("the turn runs at the vendor", || {
        vendor.lock().sessions.len() == 1 && vendor.status(&vendor.only_session()) == "running"
    });
    let session = vendor.only_session();
    wait_until("the mail is committed", || {
        store.next_undelivered(&agent.id).unwrap().is_none()
    });
    assert!(store.has_open_turn(&agent.id).unwrap());
    // A turn's usage is reported when it ends: none yet.
    assert!(store.list_usage(None).unwrap().is_empty());
    within("the first kernel stops", move || first.retain_processes());

    // Nothing ended the session or withdrew its token.
    assert!(vendor.sent("user.interrupt").is_empty());
    assert_eq!(vendor.status(&session), "running");
    let token = vendor.token_of_session(&session).unwrap();
    assert!(store.authenticate_remote_session(&token).is_ok());
    let row = store.remote_execution(&agent.id).unwrap().unwrap();
    assert!(row.is_attached());
    assert!(
        row.note.is_some(),
        "the kernel kept its accounting with the session"
    );

    // While no kernel runs, the vendor finishes the turn.
    vendor.finish(&session);
    assert_eq!(vendor.status(&session), "idle");

    let second = start(&store, &vendor);
    wait_until("the turn is completed, not recovered", || {
        !store.has_open_turn(&agent.id).unwrap()
    });
    // One write of the mail, no recovery note, one session.
    let sent = vendor.sent("user.message");
    assert_eq!(sent.len(), 1, "{sent:?}");
    assert!(
        !sent[0]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("[Recovery]")
    );
    assert_eq!(
        vendor
            .requests("POST", "/v1/sessions")
            .iter()
            .filter(|r| r.path == "/v1/sessions")
            .count(),
        1
    );
    // The turn's usage and cost are recorded once, by the kernel that saw
    // it end.
    let recorded = |store: &Store| -> (i64, i64) {
        store
            .list_usage(None)
            .unwrap()
            .iter()
            .fold((0, 0), |(tokens, cost), row| {
                (
                    tokens + row.input_tokens,
                    cost + row.reported_cost_micros.unwrap_or(0),
                )
            })
    };
    wait_until("the turn's usage is recorded", || recorded(&store).0 == 100);
    std::thread::sleep(std::time::Duration::from_millis(300));
    assert_eq!(recorded(&store), (100, 70_000));
    let lines = transcript_lines(&store, &agent.id);
    let ids: Vec<&str> = lines.iter().map(|l| l["id"].as_str().unwrap()).collect();
    let unique: std::collections::HashSet<&&str> = ids.iter().collect();
    assert_eq!(unique.len(), ids.len(), "no event applied twice: {ids:?}");
    assert!(
        lines.iter().any(
            |l| l["role"] == "assistant" && l["content"].as_str().unwrap().starts_with("done:")
        )
    );

    // Mail after the restart reaches the same session.
    vendor.set_hold(false);
    store.receive(&agent.id, Incoming::new("again")).unwrap();
    wait_until("the second mail is answered", || {
        vendor.sent("user.message").len() == 2 && !store.has_open_turn(&agent.id).unwrap()
    });
    assert_eq!(vendor.lock().sessions.len(), 1);
    wait_until("both turns' usage is recorded once", || {
        recorded(&store).0 == 200
    });
    within("the second kernel stops", move || drop(second));
}
