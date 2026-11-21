//! Shared CLI archive receipt and offline operator path.

use super::{
    ArchiveResponse, Error, Path, Store, client, offline_agent_view, open_with_key, server,
};

pub(super) fn archive(data: &Path, name: &str) -> Result<ArchiveResponse, Error> {
    if client::can_call_server(data) {
        return client::call(
            data,
            "POST",
            &format!("/v1/agents/{name}/archive"),
            Some(b"{}".to_vec()),
            &client::authority(data),
        )
        .map_err(Error::Message)?
        .ok_or_else(|| Error::Message("the server answered nothing".into()));
    }
    archive_offline(&open_with_key(data)?, data, name)
}

pub(crate) fn archive_offline(
    store: &Store,
    data: &Path,
    name: &str,
) -> Result<ArchiveResponse, Error> {
    let found = store.agent_by_name(name)?;
    let _lifecycle = store.lock_agent_lifecycle(&found.id)?;
    let found = store.agent(&found.id)?;
    let agent = store.set_agent_state(&found.id, "archived")?;
    let process = store.try_lock_agent(&agent.id).ok();
    let can_probe = found.state != "archived"
        && process.is_some()
        && store
            .remote_turn_store()
            .list_active_for_agent(&agent.id)
            .is_ok_and(|turns| turns.is_empty())
        && matches!(store.has_open_turn(&agent.id), Ok(false));
    let volume = crate::sandbox_providers::lifecycle::archive_volume(store, &agent, can_probe);
    drop(process);
    let archived = server::mark_explicitly_archived_locked(store, data, &agent)?;
    // Offline there is no maintenance worker. Keep the parent lock and take
    // descendant locks only downwards.
    for id in archived {
        if id == agent.id {
            server::reap_archived_agent_locked(store, data, &id);
        } else if let Ok(_child_lifecycle) = store.try_lock_agent_lifecycle(&id) {
            server::reap_archived_agent_locked(store, data, &id);
        }
    }
    Ok(ArchiveResponse {
        agent: offline_agent_view(store, data, agent)?,
        volume,
    })
}
