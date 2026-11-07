//! Shared credential masking for transport ingestion and durable history.
pub use anyagents_harness::masking::{mask_json_strings, mask_known_secrets};

#[cfg(test)]
mod persistence_tests {
    use crate::{HistorySearchQuery, NewAgent, Store};

    #[test]
    fn tool_history_and_search_persist_only_masked_output() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("mask-test", "sylphx"))
            .unwrap();
        let raw = "password=fixturecanarypersistence";
        store.record_tool_event(&agent.id, "bash", raw).unwrap();
        let rows = store
            .search_history(&HistorySearchQuery {
                agent: Some(agent.name.clone()),
                only_tools: true,
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].text, "[tool:bash] password=[redacted]");
        let hits = store
            .search_history(&HistorySearchQuery {
                query: Some("fixturecanarypersistence".into()),
                agent: Some(agent.name),
                only_tools: true,
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert!(hits.is_empty());
    }
    #[test]
    fn all_nine_external_forms_are_masked_before_history_and_search() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("external-history-mask", "spiron"))
            .unwrap();
        let canary = "fixturecanaryexternalcredential0123456789";
        let raw = format!(
            "Bearer {canary}\nbearer {canary}\napi_key={canary}\nsk-{canary}\nxoxb-{canary}\nhttps://fixture:{canary}@example.invalid/path\nhttps://example.invalid/path?access_token={canary}&page=2\nOPENAI_API_KEY={canary}\nX-Api-Key: {canary}\nfailed to parse config.toml; 測試🙂"
        );
        store
            .record_tool_event(&agent.id, "external", &raw)
            .unwrap();
        let query = HistorySearchQuery {
            agent: Some(agent.name),
            only_tools: true,
            limit: 10,
            ..Default::default()
        };
        let rows = store.search_history(&query).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].text.matches("[redacted]").count(), 9);
        assert!(rows[0].text.contains("&page=2"));
        assert!(
            rows[0]
                .text
                .ends_with("failed to parse config.toml; 測試🙂")
        );
        assert!(!rows[0].text.contains(canary));
        assert_eq!(
            store
                .search_history(&HistorySearchQuery {
                    query: Some("failed".into()),
                    ..query.clone()
                })
                .unwrap()
                .len(),
            1
        );
        assert!(
            store
                .search_history(&HistorySearchQuery {
                    query: Some(canary.to_string()),
                    ..query
                })
                .unwrap()
                .is_empty()
        );
    }
}
