use super::super::*;

pub(super) fn setup() -> (tempfile::TempDir, Store) {
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    (dir, store)
}

pub(super) fn agent(store: &Store, name: &str) -> Agent {
    store
        .create_agent(NewAgent::new(name, "claude-code"))
        .unwrap()
}
