//! Durable, immutable-ID principal grants, with a bounded legacy import.
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;

use anyagents_core::{Agent, AgentId, Error, Store};
use serde_json::{Map, Value};

const REGISTRY: &str = "execution-principals.json";

/// Select the deployment's registry before runtime threads start. Child API
/// clients need not read the operator file; only a launch resolves its grants.
pub(crate) fn load_principal_registry(data: &Path) -> Result<(), Error> {
    if std::env::var_os("ANYAGENTS_PRINCIPALS").is_none() {
        let path = std::path::absolute(data)?.join(REGISTRY);
        // SAFETY: main calls this before starting service or vendor threads.
        unsafe {
            std::env::set_var("ANYAGENTS_PRINCIPAL_REGISTRY", path);
        }
    }
    Ok(())
}

pub(super) fn read_principal_registry(data: &Path) -> Result<Option<String>, Error> {
    let path = data.join(REGISTRY);
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&path)
    {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return match std::fs::symlink_metadata(&path) {
                Err(missing) if missing.kind() == std::io::ErrorKind::NotFound => Ok(None),
                _ => Err(Error::Message(
                    "execution principal registry path cannot be resolved".into(),
                )),
            };
        }
        Err(err) => {
            return Err(Error::Message(format!(
                "cannot read {}: {err}",
                path.display()
            )));
        }
    };
    trusted_file(&file)?;
    let mut raw = String::new();
    file.take(1_048_577).read_to_string(&mut raw)?;
    if raw.len() > 1_048_576 {
        return Err(Error::Message(
            "execution principal registry exceeds 1 MiB".into(),
        ));
    }
    let value: Value = serde_json::from_str(&raw)
        .map_err(|_| Error::Message("invalid execution principal registry JSON".into()))?;
    let entries = value
        .as_object()
        .ok_or_else(|| Error::Message("execution principal registry must be an object".into()))?;
    for (id, entry) in entries {
        if AgentId::parse(id).is_err() || super::admitted_principal_entry(entry).is_none() {
            return Err(Error::Message(
                "execution principal registry contains an invalid identity or grant".into(),
            ));
        }
    }
    Ok(Some(raw))
}

fn trusted_file(file: &File) -> Result<(), Error> {
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.uid() != super::current_uid()
        || meta.mode() & 0o022 != 0
        || meta.nlink() != 1
    {
        return Err(Error::Message("execution principal registry must be a server-owned regular file without shared write access or links".into()));
    }
    Ok(())
}

/// Only the first daemon startup can import exact legacy OS account names.
/// Thereafter the persisted registry, not a mutable name, is authoritative.
pub(crate) fn reconcile_principals(store: &Store) -> Result<(), Error> {
    if std::env::var_os("ANYAGENTS_PRINCIPALS").is_some() {
        return Ok(());
    }
    update(store.data_dir(), &store.list_agents()?, true)
}

/// New children inherit the parent's bound principal. New roots require an
/// explicit grant or an exact per-ID OS account; their names confer no grant.
pub(crate) fn bind_principal(store: &Store, agent: &Agent) -> Result<(), Error> {
    if std::env::var_os("ANYAGENTS_PRINCIPALS").is_some() {
        return Ok(());
    }
    update(store.data_dir(), std::slice::from_ref(agent), false)
}

fn update(data: &Path, agents: &[Agent], import_legacy: bool) -> Result<(), Error> {
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(data.join(".execution-principals.lock"))?;
    trusted_file(&lock)?;
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let old = read_principal_registry(data)?;
    let mut grants: Map<String, Value> = match &old {
        Some(raw) => serde_json::from_str(raw)
            .map_err(|_| Error::Message("invalid principal registry".into()))?,
        None => Map::new(),
    };
    let prefix = std::env::var("ANYAGENTS_SEAT_USER_PREFIX").unwrap_or_else(|_| "seat-".into());
    let Some(socket_gid) = super::sock_gid_for().or_else(|| {
        grants.values().find_map(|entry| {
            entry
                .get("socket_gid")?
                .as_u64()
                .and_then(|n| u32::try_from(n).ok())
        })
    }) else {
        // Existing registries remain in force, even when automatic provisioning
        // is unavailable; this is not permission to choose another principal.
        return Ok(());
    };
    let changed = extend(
        &mut grants,
        agents,
        &prefix,
        socket_gid,
        import_legacy && old.is_none(),
        lookup_account,
    );
    if !changed && (old.is_some() || grants.is_empty()) {
        return Ok(());
    }
    let temp = data.join(format!(
        ".execution-principals-{}.tmp",
        anyagents_core::MessageId::generate()
    ));
    let result = (|| -> Result<(), Error> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)?;
        serde_json::to_writer(&mut file, &grants).map_err(|err| Error::Message(err.to_string()))?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        std::fs::rename(&temp, data.join(REGISTRY))?;
        File::open(data)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

fn lookup_account(user: &str) -> Option<(u32, u32)> {
    if super::is_isolated_test_env() {
        return None;
    }
    // Parse the operator-owned account database, avoiding getpwnam's shared
    // static buffer while concurrent launches may resolve other accounts.
    let text = std::fs::read_to_string("/etc/passwd").ok()?;
    text.lines().find_map(|line| {
        let mut fields = line.split(':');
        if fields.next()? != user {
            return None;
        }
        fields.next()?;
        Some((fields.next()?.parse().ok()?, fields.next()?.parse().ok()?))
    })
}

fn extend(
    grants: &mut Map<String, Value>,
    agents: &[Agent],
    prefix: &str,
    socket_gid: u32,
    legacy: bool,
    lookup: impl Fn(&str) -> Option<(u32, u32)>,
) -> bool {
    let mut changed = false;
    // Parents can appear later than their children in a database census.
    loop {
        let before = grants.len();
        for agent in agents {
            if grants.contains_key(agent.id.as_str()) {
                continue;
            }
            let id_user = format!("{prefix}{}", agent.id);
            let principal = lookup(&id_user)
                .map(|(uid, gid)| (id_user, uid, gid))
                .or_else(|| {
                    if !legacy {
                        return None;
                    }
                    let user = format!("{prefix}{}", agent.name);
                    lookup(&user).map(|(uid, gid)| (user, uid, gid))
                });
            let value = if let Some((user, uid, gid)) = principal {
                if uid == 0 || uid == super::current_uid() {
                    continue;
                }
                serde_json::json!({"user":user,"uid":uid,"gid":gid,"socket_gid":socket_gid})
            } else if let Some(parent) = agent
                .parent_id
                .as_ref()
                .and_then(|id| grants.get(id.as_str()))
            {
                parent.clone()
            } else {
                continue;
            };
            grants.insert(agent.id.to_string(), value);
        }
        if before == grants.len() {
            break;
        }
        changed = true;
    }
    changed
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_core::NewAgent;

    #[test]
    fn empty_initial_census_does_not_select_managed_execution() {
        let _env = crate::server::tests::env_lock();
        let previous = std::env::var_os("ANYAGENTS_SOCKET_GID");
        unsafe {
            std::env::set_var("ANYAGENTS_SOCKET_GID", "42003");
        }
        let dir = crate::test_tempdir();
        update(dir.path(), &[], true).unwrap();
        assert!(!dir.path().join(REGISTRY).exists());
        assert!(read_principal_registry(dir.path()).unwrap().is_none());
        unsafe {
            match previous {
                Some(value) => std::env::set_var("ANYAGENTS_SOCKET_GID", value),
                None => std::env::remove_var("ANYAGENTS_SOCKET_GID"),
            }
        }
    }

    #[test]
    fn legacy_import_is_once_and_children_inherit_immutable_parent_grants() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let root = store
            .create_agent(NewAgent::new("coordinator", "codex"))
            .unwrap();
        let mut new = NewAgent::new("child", "codex");
        new.parent_id = Some(root.id.clone());
        let child = store.create_agent(new).unwrap();
        let mut grants = Map::new();
        let lookup = |name: &str| (name == "seat-coordinator").then_some((42001, 42002));
        assert!(extend(
            &mut grants,
            &[child.clone(), root.clone()],
            "seat-",
            42003,
            true,
            lookup
        ));
        assert_eq!(grants[root.id.as_str()], grants[child.id.as_str()]);
        let retained = grants.clone();
        let mut renamed = root.clone();
        renamed.name = "reviewer".into();
        assert!(!extend(
            &mut grants,
            &[renamed],
            "seat-",
            42003,
            false,
            |_| Some((42004, 42005))
        ));
        assert_eq!(grants, retained);
        store.rename_agent(&root.id, "reviewer").unwrap();
        let impostor = store
            .create_agent(NewAgent::new("coordinator", "codex"))
            .unwrap();
        assert!(!extend(
            &mut grants,
            &[impostor],
            "seat-",
            42003,
            false,
            lookup
        ));
        let foreign = store
            .create_agent(NewAgent::new("unbound", "codex"))
            .unwrap();
        assert!(!extend(
            &mut grants,
            &[foreign],
            "seat-",
            42003,
            false,
            lookup
        ));
    }

    #[test]
    fn child_binding_persists_once_without_rebinding_after_rename() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let _env = crate::server::tests::env_lock();
        let previous = std::env::var_os("ANYAGENTS_PRINCIPALS");
        unsafe {
            std::env::remove_var("ANYAGENTS_PRINCIPALS");
        }
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let root = store
            .create_agent(NewAgent::new("planner", "codex"))
            .unwrap();
        let uid = if super::super::current_uid() == 42001 {
            42002
        } else {
            42001
        };
        let raw = serde_json::json!({root.id.as_str(): {"user":"worker","uid":uid,"gid":42003,"socket_gid":42004}}).to_string();
        let path = dir.path().join(REGISTRY);
        std::fs::write(&path, raw).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let mut new = NewAgent::new("child", "codex");
        new.parent_id = Some(root.id.clone());
        let child = store.create_agent(new).unwrap();
        bind_principal(&store, &child).unwrap();
        let retained = read_principal_registry(dir.path()).unwrap().unwrap();
        let grants: Value = serde_json::from_str(&retained).unwrap();
        assert_eq!(grants[child.id.as_str()], grants[root.id.as_str()]);
        assert_eq!(std::fs::metadata(&path).unwrap().mode() & 0o777, 0o600);
        store.rename_agent(&child.id, "renamed").unwrap();
        bind_principal(&store, &store.agent(&child.id).unwrap()).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), retained);
        std::fs::write(&path, "corrupt").unwrap();
        assert!(bind_principal(&store, &child).is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "corrupt");
        unsafe {
            match previous {
                Some(value) => std::env::set_var("ANYAGENTS_PRINCIPALS", value),
                None => std::env::remove_var("ANYAGENTS_PRINCIPALS"),
            }
        }
    }

    #[test]
    fn new_roots_require_exact_id_accounts_and_refuse_server_or_root_ids() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("worker", "codex"))
            .unwrap();
        let mut grants = Map::new();
        assert!(!extend(
            &mut grants,
            std::slice::from_ref(&agent),
            "seat-",
            42003,
            false,
            |user| (user == "seat-worker").then_some((42001, 42002))
        ));
        for uid in [0, super::super::current_uid()] {
            assert!(!extend(
                &mut grants,
                std::slice::from_ref(&agent),
                "seat-",
                42003,
                false,
                |_| Some((uid, 42002))
            ));
        }
        assert!(extend(
            &mut grants,
            std::slice::from_ref(&agent),
            "seat-",
            42003,
            false,
            |user| (user == format!("seat-{}", agent.id)).then_some((42001, 42002))
        ));
        assert_eq!(grants[agent.id.as_str()]["uid"], 42001);
    }
}
