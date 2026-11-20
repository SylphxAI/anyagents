use super::*;

fn assert_dedup(store: &Store) {
    let first = RuntimeIssueReport {
        component: RuntimeComponent::Sandbox,
        summary: "Guest boot refused",
        exact_error: "guest handshake: error 52",
        repro_steps: "Start a guest fixture.",
        source: RuntimeIssueSource::AgentReport,
        sample_agent_id: None,
    };
    let id = store.record_runtime_issue(first).unwrap();
    let duplicate = RuntimeIssueReport {
        exact_error: "  GUEST Handshake:\n  error 52\t",
        source: RuntimeIssueSource::AutoCapture,
        ..first
    };
    std::thread::scope(|scope| {
        let attempts: Vec<_> = (0..4)
            .map(|_| scope.spawn(|| store.record_runtime_issue(duplicate).unwrap()))
            .collect();
        for result in attempts {
            assert_eq!(result.join().unwrap(), id);
        }
    });
    assert_ne!(
        store
            .record_runtime_issue(RuntimeIssueReport {
                component: RuntimeComponent::Harness,
                ..first
            })
            .unwrap(),
        id
    );
    assert_ne!(
        store
            .record_runtime_issue(RuntimeIssueReport {
                exact_error: "guest handshake: error 53",
                ..first
            })
            .unwrap(),
        id
    );
    assert!(
        store
            .record_runtime_issue(RuntimeIssueReport {
                exact_error: "\t",
                ..first
            })
            .is_err()
    );
    let all = store.runtime_issues(None, None).unwrap();
    assert_eq!(all.len(), 3);
    assert!(
        all.windows(2)
            .all(|pair| (pair[0].last_seen_at, &pair[0].id) >= (pair[1].last_seen_at, &pair[1].id))
    );
    let issue = all.iter().find(|row| row.id == id).unwrap();
    assert_eq!(issue.occurrence_count, 5);
    assert_eq!(issue.exact_error, first.exact_error);
    assert_eq!(issue.source, RuntimeIssueSource::AgentReport);
    assert_eq!(issue.error_fingerprint.len(), 64);
    assert!(issue.first_seen_at <= issue.last_seen_at);
    assert_eq!(
        store
            .runtime_issues(
                Some(RuntimeIssueStatus::Open),
                Some(RuntimeComponent::Sandbox)
            )
            .unwrap()
            .len(),
        2
    );
    assert!(
        store
            .runtime_issues(Some(RuntimeIssueStatus::Resolved), None)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn sqlite_runtime_issue_upgrade_and_dedup() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    let mut conn = rusqlite::Connection::open(&path).unwrap();
    // Simulate a deployment that had applied every migration before telemetry.
    conn.execute_batch("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL);").unwrap();
    let migrations = crate::migrate::MIGRATIONS;
    let (latest, _) = migrations.last().unwrap();
    for (version, sql) in &migrations[..migrations.len() - 1] {
        let tx = conn.transaction().unwrap();
        tx.execute_batch(sql).unwrap();
        tx.execute("INSERT INTO schema_migrations VALUES (?1, 0)", [version])
            .unwrap();
        tx.commit().unwrap();
    }
    drop(conn);
    let store = Store::open(&path).unwrap();
    assert_dedup(&store);
    assert_eq!(
        crate::migrate::current_version(&rusqlite::Connection::open(path).unwrap()).unwrap(),
        *latest
    );
}

#[test]
fn postgres_runtime_issue_migration_and_dedup() {
    let Ok(url) = std::env::var(crate::db::TEST_POSTGRES_URL_ENV) else {
        eprintln!(
            "skipping Postgres runtime issues: set {}",
            crate::db::TEST_POSTGRES_URL_ENV
        );
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open_postgres_isolated(&url, dir.path(), "runtime-issues-test").unwrap();
    assert_dedup(&store);
}
