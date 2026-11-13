//! An idle fleet costs nothing (ADR 0006 invariant I1): on a store with a
//! large delivered history, idle agents run no supervisor pass at all, and so
//! no store read and no `/proc` read, even while a neighbour agent is busy
//! receiving mail.
//!
//! CPU time is read per agent thread (`/proc/self/task/<tid>/stat`): the
//! launch callback runs on the agent's own supervisor thread and records its
//! tid, so the idle fleet's cost is measured apart from the busy neighbour.

#![cfg(target_os = "linux")]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyagents_core::{AgentId, Incoming, NewAgent, Store};
use anyagents_harness::{
    DeliverOutcome, DeliveryMode, Harness, HarnessError, HarnessHealth, OutboundMessage,
    SteerSupport, TurnState,
};
use anyagents_supervisor::{Clock, LaunchRequest, Supervisor, SystemClock};

const IDLE_AGENTS: usize = 24;
const HISTORY_PER_AGENT: i64 = 20_000;
const WARMUP: Duration = Duration::from_secs(5);
const WINDOW: Duration = Duration::from_secs(6);
const NEIGHBOUR_MAIL_EVERY: Duration = Duration::from_millis(50);

/// Counts every return from `wait`: one per supervisor loop pass that slept.
struct CountingClock {
    inner: SystemClock,
    waits: AtomicU64,
}

impl Clock for CountingClock {
    fn now_ms(&self) -> i64 {
        self.inner.now_ms()
    }
    fn wait(&self, deadline: Option<i64>, woken: &Arc<AtomicBool>) -> bool {
        let flagged = self.inner.wait(deadline, woken);
        self.waits.fetch_add(1, Ordering::SeqCst);
        flagged
    }
    fn poke(&self) {
        self.inner.poke();
    }
}

/// A live, idle vendor process: it accepts every line and finishes the turn at
/// once. It reports the test process as its pid so the idle-release check
/// inspects a real process tree, as it does for a live vendor.
///
/// `reads` counts every `health` and `process_id` call: each supervisor pass
/// reads the harness's health, and every `/proc` read of the in-flight check
/// starts from `process_id`, so a pass, a store read or a `/proc` read for
/// this agent cannot happen without moving it.
struct IdleHarness {
    served: u32,
    reads: Arc<AtomicU64>,
}

impl Harness for IdleHarness {
    fn deliver(&mut self, _message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        self.served += 1;
        Ok(DeliverOutcome::Accepted {
            turn_finished: true,
        })
    }
    fn background(&mut self) -> Result<(), HarnessError> {
        Ok(())
    }
    fn resume(&mut self, _session: &str) -> Result<(), HarnessError> {
        Ok(())
    }
    fn cancel(&mut self) -> Result<(), HarnessError> {
        Ok(())
    }
    fn health(&self) -> HarnessHealth {
        self.reads.fetch_add(1, Ordering::SeqCst);
        HarnessHealth {
            process_alive: true,
            turn_state: TurnState::Idle,
            delivery_mode: DeliveryMode::Followup,
            steer_support: SteerSupport::No,
            quota_reset_at_ms: None,
            sign_in_fault: None,
        }
    }
    fn turns_served(&self) -> u32 {
        self.served
    }
    fn process_id(&self) -> Option<u32> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Some(std::process::id())
    }
}

/// User + system CPU seconds of one `stat` file (process or thread).
fn cpu_seconds(path: &str) -> f64 {
    let Ok(stat) = std::fs::read_to_string(path) else {
        return 0.0;
    };
    // Fields after the parenthesised command name; utime and stime are the
    // 14th and 15th fields of the full line.
    let rest = &stat[stat.rfind(')').unwrap() + 2..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    let utime: f64 = fields[11].parse().unwrap();
    let stime: f64 = fields[12].parse().unwrap();
    (utime + stime) / 100.0
}

fn process_cpu_seconds() -> f64 {
    cpu_seconds("/proc/self/stat")
}

fn threads_cpu_seconds(tids: &[u32]) -> f64 {
    tids.iter()
        .map(|tid| cpu_seconds(&format!("/proc/self/task/{tid}/stat")))
        .sum()
}

/// Kernel id of the calling thread.
fn current_tid() -> u32 {
    let link = std::fs::read_link("/proc/thread-self").unwrap();
    link.file_name().unwrap().to_str().unwrap().parse().unwrap()
}

/// Give `agent` a long, fully delivered history so every per-agent mailbox
/// query runs against a large table, as on a long-lived host.
fn seed_history(db: &std::path::Path, agent: &AgentId) {
    let conn = rusqlite::Connection::open(db).unwrap();
    conn.busy_timeout(Duration::from_secs(30)).unwrap();
    let columns: Vec<String> = {
        let mut stmt = conn
            .prepare("SELECT name FROM pragma_table_info('messages')")
            .unwrap();
        stmt.query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    };
    let select: Vec<String> = columns
        .iter()
        .map(|c| match c.as_str() {
            "id" => "m.id || '-' || n.i".to_string(),
            "seq" => "n.i".to_string(),
            "source_key" => "NULL".to_string(),
            "received_at_ms" => "m.received_at_ms - (?2 - n.i)".to_string(),
            other => format!("m.{other}"),
        })
        .collect();
    let sql = format!(
        "WITH RECURSIVE n(i) AS (SELECT 2 UNION ALL SELECT i + 1 FROM n WHERE i < ?2)
         INSERT INTO messages ({cols})
         SELECT {select} FROM n, messages m WHERE m.agent_id = ?1 AND m.seq = 1",
        cols = columns.join(", "),
        select = select.join(", "),
    );
    conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    conn.execute(&sql, rusqlite::params![agent.as_str(), HISTORY_PER_AGENT])
        .unwrap();
    conn.execute(
        "UPDATE mailbox_cursors SET delivered_seq = ?2 WHERE agent_id = ?1",
        rusqlite::params![agent.as_str(), HISTORY_PER_AGENT],
    )
    .unwrap();
    conn.execute_batch("COMMIT").unwrap();
}

fn wait_for(mut ready: impl FnMut() -> bool, ceiling: Duration) -> bool {
    let started = Instant::now();
    while started.elapsed() < ceiling {
        if ready() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    ready()
}

#[test]
fn an_idle_fleet_on_a_large_store_sleeps_while_a_neighbour_is_busy() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("state.db");
    let store = Arc::new(Store::open(&db).unwrap());
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();

    let mut idle = Vec::new();
    for i in 0..IDLE_AGENTS {
        let agent = store
            .create_agent(NewAgent::new(format!("idle-{i}"), "contract"))
            .unwrap();
        store
            .receive(&agent.id, Incoming::new("first line of a long history"))
            .unwrap();
        seed_history(&db, &agent.id);
        assert_eq!(
            store.delivered_cursor(&agent.id).unwrap(),
            HISTORY_PER_AGENT
        );
        idle.push(agent);
    }
    let neighbour = store
        .create_agent(NewAgent::new("busy-neighbour", "contract"))
        .unwrap();
    for agent in idle.iter().chain(std::iter::once(&neighbour)) {
        store
            .set_agent_running(&agent.id, &work.display().to_string())
            .unwrap();
    }

    let clock = Arc::new(CountingClock {
        inner: SystemClock::new(),
        waits: AtomicU64::new(0),
    });
    let threads: Arc<Mutex<HashMap<String, u32>>> = Arc::default();
    let launched = Arc::clone(&threads);
    let reads: Arc<Mutex<HashMap<String, Arc<AtomicU64>>>> = Arc::default();
    let counted = Arc::clone(&reads);
    let supervisor = Supervisor::<IdleHarness>::start(
        Arc::clone(&store),
        Arc::clone(&clock) as Arc<dyn Clock>,
        Arc::new(move |request: LaunchRequest| {
            launched
                .lock()
                .unwrap()
                .insert(request.agent.name.clone(), current_tid());
            let harness_reads = Arc::new(AtomicU64::new(0));
            counted
                .lock()
                .unwrap()
                .insert(request.agent.name.clone(), Arc::clone(&harness_reads));
            Ok(IdleHarness {
                served: 0,
                reads: harness_reads,
            })
        }),
        Vec::new(),
    )
    .unwrap();
    assert!(
        wait_for(
            || idle
                .iter()
                .all(|a| supervisor.live(&a.id).is_some_and(|s| s.process_alive)),
            Duration::from_secs(30),
        ),
        "every idle agent must start its process"
    );
    std::thread::sleep(WARMUP);
    let idle_tids: Vec<u32> = {
        let threads = threads.lock().unwrap();
        idle.iter().map(|a| threads[&a.name]).collect()
    };

    // Steady state: one neighbour receives a line every 50 ms while the rest
    // of the fleet has nothing to do.
    let stop_mail = Arc::new(AtomicBool::new(false));
    let mailer = {
        let store = Arc::clone(&store);
        let stop_mail = Arc::clone(&stop_mail);
        let id = neighbour.id.clone();
        std::thread::spawn(move || {
            let mut sent = 0u64;
            while !stop_mail.load(Ordering::SeqCst) {
                let mut line = Incoming::new(format!("neighbour line {sent}"));
                line.sender = Some("agent:peer".to_string());
                store.receive(&id, line).unwrap();
                sent += 1;
                std::thread::sleep(NEIGHBOUR_MAIL_EVERY);
            }
            sent
        })
    };
    let idle_reads = |reads: &Mutex<HashMap<String, Arc<AtomicU64>>>| -> u64 {
        let reads = reads.lock().unwrap();
        idle.iter()
            .map(|a| reads[&a.name].load(Ordering::SeqCst))
            .sum()
    };
    let reads_before = idle_reads(&reads);
    let cpu_before = process_cpu_seconds();
    let idle_cpu_before = threads_cpu_seconds(&idle_tids);
    let waits_before = clock.waits.load(Ordering::SeqCst);
    let started = Instant::now();
    std::thread::sleep(WINDOW);
    let elapsed = started.elapsed().as_secs_f64();
    let waits = clock.waits.load(Ordering::SeqCst) - waits_before;
    let cpu = process_cpu_seconds() - cpu_before;
    let idle_cpu = threads_cpu_seconds(&idle_tids) - idle_cpu_before;
    let harness_reads = idle_reads(&reads) - reads_before;
    stop_mail.store(true, Ordering::SeqCst);
    let sent = mailer.join().unwrap();

    // The neighbour alone accounts for about one pass per line it receives.
    let idle_passes = waits.saturating_sub(sent * 2);
    eprintln!(
        "idle_cost: agents={IDLE_AGENTS} history_rows={} window_s={elapsed:.2} \
         total_waits={waits} neighbour_lines={sent} idle_passes~={idle_passes} \
         passes_per_idle_agent_per_s={:.3} idle_harness_reads={harness_reads} \
         idle_threads_cpu_s={idle_cpu:.3} idle_threads_pct_one_core={:.2} \
         process_cpu_s={cpu:.3}",
        IDLE_AGENTS as i64 * HISTORY_PER_AGENT,
        idle_passes as f64 / IDLE_AGENTS as f64 / elapsed,
        idle_cpu / elapsed * 100.0,
    );

    // A fixed 250 ms review would be 4 passes per agent per second (576 in
    // this window), and a backed-off review a few. An idle agent waits for an
    // event, and none arrives for it: no pass reads its harness, so none
    // reads the store or `/proc` for it either.
    assert_eq!(
        harness_reads, 0,
        "idle agents must not run supervisor passes without an event"
    );
    assert!(
        idle_passes <= (IDLE_AGENTS as u64) * 3,
        "idle agents must not review on a timer: {idle_passes} passes for \
         {IDLE_AGENTS} agents in {elapsed:.1}s"
    );
    // The idle agents' own threads, together, stay near zero while the
    // neighbour is busy: no per-review process-table scan and no wake of the
    // whole fleet on each neighbour line. A fixed 250 ms review with a
    // process-table scan and a fleet-wide wake per line costs these threads
    // well over one core here; the residue is the one backed-off review per
    // agent that falls inside the window.
    assert!(
        idle_cpu / elapsed < 0.10,
        "{IDLE_AGENTS} idle agents used {:.1}% of one core",
        idle_cpu / elapsed * 100.0
    );

    // Backoff never delays mail: a new line reaches an idle agent at once.
    let target = &idle[IDLE_AGENTS / 2];
    let mut line = Incoming::new("wake up");
    line.sender = Some("agent:peer".to_string());
    store.receive(&target.id, line).unwrap();
    let sent_at = Instant::now();
    assert!(
        wait_for(
            || store.delivered_cursor(&target.id).unwrap() > HISTORY_PER_AGENT,
            Duration::from_secs(2),
        ),
        "mail to a backed-off idle agent must be delivered promptly"
    );
    eprintln!(
        "idle_cost: wake latency {} ms",
        sent_at.elapsed().as_millis()
    );
    drop(supervisor);
}
