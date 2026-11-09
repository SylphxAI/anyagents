//! Opt-in synthetic canonical-route measurements; never a recall benchmark.
use super::*;
use std::io::Write;
use std::time::Instant;

fn usage() -> (f64, f64, i64) {
    let mut value = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the provided correctly-sized object.
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, value.as_mut_ptr()) },
        0
    );
    // SAFETY: the successful call initialized value.
    let value = unsafe { value.assume_init() };
    let millis = |t: libc::timeval| t.tv_sec as f64 * 1000.0 + t.tv_usec as f64 / 1000.0;
    (
        millis(value.ru_utime),
        millis(value.ru_stime),
        value.ru_maxrss,
    )
}

fn state(store: Arc<Store>, data: &Path) -> AppState {
    AppState {
        store,
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        auth: auth::Mode::Trusted {
            data_dir: data.into(),
        },
        identity: Arc::new(ServingIdentity::capture()),
    }
}

#[test]
#[ignore = "synthetic latency/CPU/RSS measurement; explicit opt-in and owned artifact directory"]
fn canonical_history_latency() {
    assert_eq!(
        std::env::var("ANYAGENTS_HISTORY_BENCHMARK").as_deref(),
        Ok("1")
    );
    let source = std::env::var("ANYAGENTS_BENCH_SOURCE_SHA").expect("exact tested source SHA");
    let base =
        std::env::var("ANYAGENTS_HISTORY_BENCHMARK_DIR").expect("existing owned scratch directory");
    let dir = tempfile::tempdir_in(base).unwrap();
    let db = dir.path().join("state.db");
    let store = Arc::new(Store::open(&db).unwrap());
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("bench-parent", "spiron"))
        .unwrap();
    let mut new = anyagents_core::NewAgent::new("bench-child", "spiron");
    new.parent_id = Some(parent.id.clone());
    let child = store.create_agent(new).unwrap();
    let hidden = store
        .create_agent(anyagents_core::NewAgent::new("bench-hidden", "spiron"))
        .unwrap();
    let mut conn = rusqlite::Connection::open(&db).unwrap();
    let tx = conn.transaction().unwrap();
    let mut manifest = Vec::new();
    let mut mailbox_bytes = 0;
    let mut native_bytes = 0;
    let mut memory_bytes = 0;
    for a in [&parent, &child, &hidden] {
        let root = dir.path().join("agents").join(a.id.as_str());
        std::fs::create_dir_all(root.join("config")).unwrap();
        std::fs::create_dir_all(root.join("memory")).unwrap();
        let body = format!(
            "{} benchneedle0000 {}",
            "memorypadding ".repeat(5000),
            a.name
        );
        memory_bytes += body.len();
        std::fs::write(root.join("memory/MEMORY.md"), &body).unwrap();
        manifest.push(format!(
            "{}/memory:{}",
            a.name,
            anyagents_core::learned_memory::sha256(body.as_bytes())
        ));
        let mut native =
            std::fs::File::create(root.join("config/sylphx-transcript.jsonl")).unwrap();
        for n in 0..128 {
            let body = format!(
                "{} benchneedle{n:04} {}",
                "transcriptpadding ".repeat(4000),
                a.name
            );
            let event = serde_json::to_vec(
                &serde_json::json!({"role":"user","timestamp_ms":n,"content":body}),
            )
            .unwrap();
            native.write_all(&event).unwrap();
            native.write_all(b"\n").unwrap();
            native_bytes += event.len() + 1;
            manifest.push(format!(
                "{}/native/{n}:{}",
                a.name,
                anyagents_core::learned_memory::sha256(&event)
            ));
            if n >= 32 {
                continue;
            }
            let body = format!(
                "{} benchneedle{n:04} {}",
                "mailboxpadding ".repeat(4500),
                a.name
            );
            mailbox_bytes += body.len();
            manifest.push(format!(
                "{}/mailbox/{n}:{}",
                a.name,
                anyagents_core::learned_memory::sha256(body.as_bytes())
            ));
            let id = format!("bench-{}-{n}", a.name);
            tx.execute("INSERT INTO messages(id,agent_id,seq,sender,body,received_at_ms) VALUES(?1,?2,?3,'agent:synthetic',?4,?3)",rusqlite::params![id,a.id.as_str(),n+1,body]).unwrap();
            // Deliberately reproduce pre-upgrade derived prefixes. Authoritative
            // mailbox bodies are complete; only production backfill repairs FTS.
            tx.execute("INSERT INTO rag_chunks(chunk_id,agent_id,agent_name,direction,sender,destination,timestamp_ms,content_hash,text,kind,tool_name,seq,text_format) VALUES(?1,?2,?3,'in','agent:synthetic',?3,?4,?1,?5,'message','',?4,1)",rusqlite::params![id,a.id.as_str(),a.name,n+1,&body[..2400]]).unwrap();
        }
    }
    tx.execute("INSERT INTO rag_chunks_fts(chunk_id,agent_name,sender,destination,kind,tool_name,text,search_tokens) SELECT chunk_id,agent_name,sender,destination,kind,tool_name,text,text FROM rag_chunks",[]).unwrap();
    tx.commit().unwrap();
    let corpus_sha = anyagents_core::learned_memory::sha256(manifest.join("\n").as_bytes());
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let mut app = state(Arc::clone(&store), dir.path());
    drop(store);
    let mut measurements = Vec::new();
    for (phase, n) in std::iter::once(("first-upgrade", 0))
        .chain((0..20).map(|n| ("steady", n)))
        .chain(std::iter::once(("reopened", 3)))
    {
        if phase == "reopened" {
            drop(app);
            app = state(Arc::new(Store::open(&db).unwrap()), dir.path());
        }
        let query = serde_json::from_value(
            serde_json::json!({"scope":"all","query":format!("benchneedle{n:04}"),"limit":20}),
        )
        .unwrap();
        let before = usage();
        let start = Instant::now();
        let result = runtime
            .block_on(conversation_route(
                Principal::Agent {
                    agent_id: parent.id.clone(),
                    name: parent.name.clone(),
                },
                State(app.clone()),
                AxumPath("all".into()),
                Query(query),
            ))
            .unwrap_or_else(|e| panic!("{}", e.message));
        // Include JSON response encoding, not only helper execution.
        let encoded = serde_json::to_vec(&result.0).unwrap();
        let wall_ms = start.elapsed().as_secs_f64() * 1000.0;
        let after = usage();
        assert_eq!(
            result.0.turns.len(),
            if n == 0 { 6 } else { 4 },
            "{phase} {n}: {:?}",
            result
                .0
                .turns
                .iter()
                .map(|t| (&t.id, &t.source, &t.raw, t.text.len()))
                .collect::<Vec<_>>()
        );
        assert!(
            result
                .0
                .turns
                .iter()
                .all(|t| !t.text.contains("bench-hidden"))
        );
        let mailbox_hits = result.0.turns.iter().filter(|t| t.raw.is_none()).count();
        assert_eq!(
            mailbox_hits, 2,
            "full mailbox tail must be upgraded through canonical route"
        );
        measurements.push(serde_json::json!({"phase":phase,"query":format!("benchneedle{n:04}"),"wall_ms":wall_ms,"user_ms":after.0-before.0,"system_ms":after.1-before.1,"peak_rss_kib":after.2,"result_count":result.0.turns.len(),"response_bytes":encoded.len()}));
    }
    let hidden_old: i64 = conn
        .query_row(
            "SELECT count(*) FROM rag_chunks WHERE agent_id=?1 AND text_format=1",
            [hidden.id.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(hidden_old, 32, "unrelated owner's body not upgraded");
    let visible_old: i64 = conn
        .query_row(
            "SELECT count(*) FROM rag_chunks WHERE agent_id<>?1 AND text_format=1",
            [hidden.id.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(visible_old, 0);
    let mut warm: Vec<f64> = measurements
        .iter()
        .filter(|m| m["phase"] == "steady")
        .map(|m| m["wall_ms"].as_f64().unwrap())
        .collect();
    warm.sort_by(f64::total_cmp);
    let cpu = std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find(|line| line.starts_with("Cpus_allowed_list:"))
        .unwrap_or("unknown")
        .to_string();
    let sizes = |path: &Path| std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    println!(
        "HISTORY_BENCHMARK {}",
        serde_json::json!({
            "source_sha":source,"corpus_sha256":corpus_sha,"corpus":"synthetic-v1:3owners/96mailbox/384native/3memory;parent+child visible,peer hidden",
            "mailbox_bytes":mailbox_bytes,"native_bytes":native_bytes,"memory_bytes":memory_bytes,
            "cold_definition":"first canonical call: pre-upgrade mailbox prefixes and absent host index; OS page cache uncontrolled",
            "warm_definition":"20 canonical calls on unchanged corpus with 20 distinct query strings, persisted index; no result cache",
            "entrypoint":"conversation_route including ACL, host refresh, IMMEDIATE mailbox backfill, retrieval and JSON encoding; no HTTP/socket overhead",
            "profile":if cfg!(debug_assertions) {"debug"} else {"release"},"os":std::env::consts::OS,"arch":std::env::consts::ARCH,
            "cpu_affinity":cpu,"cpu_max":std::fs::read_to_string("/sys/fs/cgroup/cpu.max").unwrap_or_default().trim(),
            "state_db_bytes":sizes(&db),"host_index_bytes":sizes(&dir.path().join("host-history-index-v1.db")),
            "steady_p50_ms":warm[9],"steady_p95_ms":warm[18],"steady_max_ms":warm[19],"measurements":measurements,
            "relevance_claim":"none; finite synthetic result/ACL/tail assertions only"
        })
    );
}
