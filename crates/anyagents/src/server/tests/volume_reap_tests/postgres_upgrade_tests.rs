use super::*;
use std::{
    io::Write as _,
    process::{Command, Stdio},
};

// The test server URL must allow the test to create disposable databases. The
// configured database itself is never seeded, altered, or used for agent state.
struct TestDatabase<'a> {
    admin_url: &'a str,
    url: url::Url,
    name: String,
}

impl<'a> TestDatabase<'a> {
    fn new(admin_url: &'a str) -> Self {
        let name = format!("archive_upgrade_{}", anyagents_core::AgentId::generate());
        let mut url = url::Url::parse(admin_url).unwrap();
        url.set_path(&name);
        psql(admin_url, &format!("CREATE DATABASE {name};"));
        Self {
            admin_url,
            url,
            name,
        }
    }
}

impl Drop for TestDatabase<'_> {
    fn drop(&mut self) {
        psql(
            self.admin_url,
            &format!("DROP DATABASE {} WITH (FORCE);", self.name),
        );
    }
}

fn psql(url: &str, sql: &str) -> String {
    let mut child = Command::new("psql")
        .args(["-XAtq", "-v", "ON_ERROR_STOP=1", "--dbname", url])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("psql required when the test PostgreSQL URL is configured");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(sql.as_bytes())
        .unwrap();
    let result = child.wait_with_output().unwrap();
    assert!(result.status.success(), "psql failed: {}", result.status);
    String::from_utf8(result.stdout).unwrap().trim().to_owned()
}

fn seed(url: &str, baseline: i64) {
    let root =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../anyagents-core/migrations_pg");
    let mut files: Vec<_> = root
        .read_dir()
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    files.sort();
    for file in files {
        let name = file.file_name().unwrap().to_str().unwrap();
        let version: i64 = name.split_once('_').unwrap().0.parse().unwrap();
        if version <= baseline {
            let sql = std::fs::read_to_string(file).unwrap();
            psql(
                url,
                &format!(
                    "BEGIN;\n{sql}\nINSERT INTO public.pg_schema_migrations VALUES ({version}, 0);\nCOMMIT;"
                ),
            );
        }
    }
    if baseline != 0 {
        assert_eq!(
            psql(url, "SELECT max(version) FROM public.pg_schema_migrations;"),
            baseline.to_string()
        );
        assert_eq!(
            psql(
                url,
                "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'agents' AND column_name = 'terminal_archive_verdict';"
            ),
            if baseline == 14 { "0" } else { "1" }
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_startup_repairs_archive_verdict_on_fresh_and_upgraded_databases() {
    let Ok(admin_url) = std::env::var("ANYAGENTS_ARCHIVE_UPGRADE_TEST_POSTGRES_URL") else {
        eprintln!("skipping PostgreSQL archive upgrade: dedicated test URL unset");
        return;
    };
    for (baseline, missing_column) in [(14, true), (15, false), (15, true), (0, false)] {
        let db = TestDatabase::new(&admin_url);
        let url = db.url.as_str();
        seed(url, baseline);
        if baseline == 15 && missing_column {
            psql(
                url,
                "DROP TRIGGER agents_clear_terminal_archive_verdict ON public.agents;
                       ALTER TABLE public.agents DROP COLUMN terminal_archive_verdict;",
            );
        }
        if baseline != 0 {
            let id = anyagents_core::AgentId::generate();
            psql(
                url,
                &format!(
                    "INSERT INTO public.agents (id, name, harness, state, created_at_ms)
                               VALUES ('{id}', 'legacy', 'opencode', 'running', 1);"
                ),
            );
        }
        let f = Fixture::with_store(
            "task",
            false,
            probe(1, ""),
            StatusCode::NOT_FOUND,
            Some(url),
        )
        .await;
        assert_eq!(
            psql(
                url,
                "SELECT count(*) FROM public.pg_schema_migrations WHERE version = 16;"
            ),
            "1"
        );
        if baseline != 0 {
            assert_eq!(
                f.state.store.agent_by_name("legacy").unwrap().state,
                "running"
            );
            assert_eq!(
                psql(
                    url,
                    "SELECT count(*) FROM public.pg_schema_migrations WHERE version <= 14 AND applied_at_ms = 0;"
                ),
                "14"
            );
        }
        let verdict = f.verdict("accepted").await;
        assert_eq!(verdict["kind"], "accepted");
        assert_eq!(
            verdict["remote_volume_reaped"], true,
            "baseline {baseline}: {verdict}"
        );
        assert_eq!(f.archive().await["remote_volume_reaped"], true);
        f.state
            .store
            .set_agent_state(&f.agent.id, "running")
            .unwrap();
        let archived = f.archive().await;
        assert_eq!(archived["state"], "archived");
        assert_eq!(
            archived["remote_volume_reaped"], false,
            "reopening must clear the verdict"
        );
        f.calls(&["delete", "delete"]);
    }
}
