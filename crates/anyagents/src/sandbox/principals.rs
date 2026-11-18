//! Durable, immutable-ID principal grants, with a bounded legacy import.
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;
use std::sync::{Mutex, OnceLock};

use anyagents_core::{Agent, AgentId, Error, Store};
use serde_json::{Map, Value};

const REGISTRY: &str = "execution-principals.json";

type PrincipalTuple = (String, u32, u32, u32);
type ParsedRegistry = (String, HashMap<String, PrincipalTuple>);
type FileStamp = (u64, u64, u64, i64, i64);
type CachedRegistry = (u32, FileStamp, String, HashMap<String, PrincipalTuple>);

fn registry_cache() -> &'static Mutex<Option<CachedRegistry>> {
    static CACHE: OnceLock<Mutex<Option<CachedRegistry>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(None))
}

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

/// A trusted display snapshot. Launch authorization still reads fresh grants.
pub(super) fn read_admitted_principals(
    data: &Path,
) -> Result<HashMap<String, PrincipalTuple>, Error> {
    Ok(read_principal_registry_parsed(data)?
        .map(|(_, map)| map)
        .unwrap_or_default())
}

pub(super) fn read_admitted_principal(
    data: &Path,
    agent_id: &str,
) -> Result<Option<PrincipalTuple>, Error> {
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
    let stamp = trusted_file(&file)?;
    let me = super::current_uid();
    if !super::is_isolated_test_env()
        && let Ok(guard) = registry_cache().lock()
        && let Some((cached_uid, cached_stamp, _, cached_map)) = guard.as_ref()
        && *cached_uid == me
        && *cached_stamp == stamp
    {
        return Ok(cached_map.get(agent_id).cloned());
    }
    let Some((_, parsed)) = read_principal_registry_from_file(file, stamp)? else {
        return Ok(None);
    };
    Ok(parsed.get(agent_id).cloned())
}

pub(super) fn read_principal_registry(data: &Path) -> Result<Option<String>, Error> {
    Ok(read_principal_registry_parsed(data)?.map(|(raw, _)| raw))
}

fn read_principal_registry_parsed(data: &Path) -> Result<Option<ParsedRegistry>, Error> {
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
    let stamp = trusted_file(&file)?;
    read_principal_registry_from_file(file, stamp)
}

fn read_principal_registry_from_file(
    file: File,
    stamp: FileStamp,
) -> Result<Option<ParsedRegistry>, Error> {
    let mut raw = String::new();
    file.take(1_048_577).read_to_string(&mut raw)?;
    if raw.len() > 1_048_576 {
        return Err(Error::Message(
            "execution principal registry exceeds 1 MiB".into(),
        ));
    }
    let me = super::current_uid();
    if let Ok(guard) = registry_cache().lock()
        && let Some((cached_uid, _, cached_raw, cached_map)) = guard.as_ref()
        && *cached_uid == me
        && cached_raw == &raw
    {
        return Ok(Some((raw, cached_map.clone())));
    }
    let value: Value = serde_json::from_str(&raw)
        .map_err(|_| Error::Message("invalid execution principal registry JSON".into()))?;
    let entries = value
        .as_object()
        .ok_or_else(|| Error::Message("execution principal registry must be an object".into()))?;
    let mut parsed = HashMap::with_capacity(entries.len());
    for (id, entry) in entries {
        let Some(principal) = super::admitted_principal_entry(entry) else {
            return Err(Error::Message(
                "execution principal registry contains an invalid identity or grant".into(),
            ));
        };
        if AgentId::parse(id).is_err() {
            return Err(Error::Message(
                "execution principal registry contains an invalid identity or grant".into(),
            ));
        }
        parsed.insert(id.clone(), principal);
    }
    if let Ok(mut guard) = registry_cache().lock() {
        *guard = Some((me, stamp, raw.clone(), parsed.clone()));
    }
    Ok(Some((raw, parsed)))
}

fn trusted_file(file: &File) -> Result<FileStamp, Error> {
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.uid() != super::current_uid()
        || meta.mode() & 0o022 != 0
        || meta.nlink() != 1
    {
        return Err(Error::Message("execution principal registry must be a server-owned regular file without shared write access or links".into()));
    }
    Ok((
        meta.dev(),
        meta.ino(),
        meta.len(),
        meta.mtime(),
        meta.mtime_nsec(),
    ))
}

/// Only the first daemon startup can import exact legacy OS account names.
/// Thereafter the persisted registry, not a mutable name, is authoritative.
pub(crate) fn reconcile_principals(store: &Store) -> Result<(), Error> {
    if std::env::var_os("ANYAGENTS_PRINCIPALS").is_some() {
        return Ok(());
    }
    update(store.data_dir(), &store.list_agents()?, true)
}

/// Optional account selection and UID assertion from an authenticated operator.
#[derive(Default)]
pub(crate) struct RootSeatRequest {
    pub user: Option<String>,
    pub uid: Option<u32>,
}

impl RootSeatRequest {
    pub fn explicit(&self) -> bool {
        self.user.is_some() || self.uid.is_some()
    }
}

/// Returns whether this call installed a new operator seat grant.
pub(crate) fn bind_principal(
    store: &Store,
    agent: &Agent,
    root: Option<&RootSeatRequest>,
) -> Result<bool, Error> {
    let explicit = root.is_some_and(RootSeatRequest::explicit);
    if root.is_some() && agent.parent_id.is_some() {
        return Err(seat_denied(
            "seat admission requires an operator-created root",
        ));
    }
    if std::env::var_os("ANYAGENTS_PRINCIPALS").is_some() {
        if explicit {
            return Err(seat_denied(
                "cannot select a seat with an operator principal map",
            ));
        }
        return Ok(false);
    }
    let data = store.data_dir();
    let Some(root) = root else {
        update(data, std::slice::from_ref(agent), false)?;
        return Ok(false);
    };
    let prefix = seat_prefix();
    let mut claimed = false;
    edit_registry(data, |grants, _| {
        let socket_gid = registry_socket_gid(grants);
        claimed = claim_root_seat(grants, agent, root, &prefix, socket_gid, |user| {
            lookup_root_account(data, user)
        })?;
        if claimed || grants.contains_key(agent.id.as_str()) {
            return Ok(claimed);
        }
        // No named seat was requested or found; keep ordinary per-ID behavior.
        Ok(socket_gid.is_some_and(|gid| {
            extend(
                grants,
                std::slice::from_ref(agent),
                &prefix,
                gid,
                false,
                lookup_account,
            )
        }))
    })?;
    Ok(claimed)
}

pub(crate) fn rollback_root_seat(store: &Store, id: &AgentId) -> Result<(), Error> {
    edit_registry(store.data_dir(), |grants, _| {
        let Some(grant) = grants.get(id.as_str()) else {
            return Ok(false);
        };
        // A live child can inherit a grant during creation. Keep the original
        // reservation if anyone else uses these credentials.
        let (user, uid) = (grant["user"].as_str(), grant["uid"].as_u64());
        if grants.iter().any(|(other, value)| {
            other != id.as_str() && (value["user"].as_str() == user || value["uid"].as_u64() == uid)
        }) {
            return Ok(false);
        }
        grants.remove(id.as_str());
        Ok(true)
    })
}

fn seat_prefix() -> String {
    std::env::var("ANYAGENTS_SEAT_USER_PREFIX").unwrap_or_else(|_| "seat-".into())
}

fn registry_socket_gid(grants: &Map<String, Value>) -> Option<u32> {
    super::sock_gid_for().or_else(|| {
        grants
            .values()
            .find_map(|entry| u32::try_from(entry.get("socket_gid")?.as_u64()?).ok())
    })
}

fn update(data: &Path, agents: &[Agent], import_legacy: bool) -> Result<(), Error> {
    edit_registry(data, |grants, new| {
        Ok(registry_socket_gid(grants).is_some_and(|gid| {
            extend(
                grants,
                agents,
                &seat_prefix(),
                gid,
                import_legacy && new,
                lookup_account,
            )
        }))
    })
}

fn edit_registry(
    data: &Path,
    edit: impl FnOnce(&mut Map<String, Value>, bool) -> Result<bool, Error>,
) -> Result<(), Error> {
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
    if !edit(&mut grants, old.is_none())? {
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
        super::invalidate_peers_cache();
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

fn seat_denied(message: &str) -> Error {
    Error::PermissionDenied(message.into())
}

fn seat_claimed(grants: &Map<String, Value>, user: &str, uid: u32) -> bool {
    grants.values().any(|grant| {
        grant["user"].as_str() == Some(user) || grant["uid"].as_u64() == Some(uid.into())
    })
}

fn claim_root_seat(
    grants: &mut Map<String, Value>,
    agent: &Agent,
    request: &RootSeatRequest,
    prefix: &str,
    socket_gid: Option<u32>,
    lookup: impl FnOnce(&str) -> Result<Option<(u32, u32)>, Error>,
) -> Result<bool, Error> {
    if let Some(grant) = grants.get(agent.id.as_str()) {
        if request
            .user
            .as_deref()
            .is_some_and(|user| grant["user"].as_str() != Some(user))
            || request
                .uid
                .is_some_and(|uid| grant["uid"].as_u64() != Some(uid.into()))
        {
            return Err(seat_denied("execution principal grant is immutable"));
        }
        return Ok(false);
    }
    let default_user = format!("{prefix}{}", agent.name);
    let user = request.user.as_deref().unwrap_or(&default_user);
    // Named admission cannot consume the reserved per-ID account namespace.
    if prefix.is_empty()
        || user
            .strip_prefix(prefix)
            .is_none_or(|slug| slug.is_empty() || AgentId::parse(slug).is_ok())
    {
        return if request.explicit() {
            Err(seat_denied("expected a named seat account"))
        } else {
            Ok(false)
        };
    }
    let Some((uid, gid)) = lookup(user)? else {
        return if request.explicit() {
            Err(seat_denied("named seat account does not exist"))
        } else {
            Ok(false)
        };
    };
    if uid == 0
        || uid == super::current_uid()
        || request.uid.is_some_and(|expected| uid != expected)
    {
        return Err(seat_denied(
            "named seat UID is not eligible or does not match",
        ));
    }
    if seat_claimed(grants, user, uid) {
        return Err(seat_denied("named seat account is already claimed"));
    }
    let socket_gid = socket_gid.ok_or_else(|| seat_denied("no socket group for named seat"))?;
    grants.insert(
        agent.id.to_string(),
        serde_json::json!({"user":user,"uid":uid,"gid":gid,"socket_gid":socket_gid}),
    );
    Ok(true)
}

fn lookup_root_account(data: &Path, user: &str) -> Result<Option<(u32, u32)>, Error> {
    #[cfg(test)]
    match std::fs::read_to_string(data.join("test-passwd")) {
        Ok(text) => return Ok(account_in(&text, user)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err.into()),
    }
    let _ = data;
    if super::is_isolated_test_env() {
        return Ok(None);
    }
    Ok(account_in(&std::fs::read_to_string("/etc/passwd")?, user))
}

fn lookup_account(user: &str) -> Option<(u32, u32)> {
    if super::is_isolated_test_env() {
        return None;
    }
    // Parse the operator-owned account database, avoiding getpwnam's shared
    // static buffer while concurrent launches may resolve other accounts.
    let text = std::fs::read_to_string("/etc/passwd").ok()?;
    account_in(&text, user)
}

fn account_in(text: &str, user: &str) -> Option<(u32, u32)> {
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
                if uid == 0 || uid == super::current_uid() || seat_claimed(grants, &user, uid) {
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
    fn operator_admits_one_immutable_root_per_user_or_uid() {
        let _env = crate::server::tests::env_lock();
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let root = store
            .create_agent(NewAgent::new("receiver", "codex"))
            .unwrap();
        let next = store
            .create_agent(NewAgent::new("reviewer", "codex"))
            .unwrap();
        let uid = (42001..42010)
            .find(|&uid| uid != super::super::current_uid())
            .unwrap();
        std::fs::write(dir.path().join("test-passwd"), format!(
            "seat-receiver:x:{uid}:42010::/:/bin/false\nseat-reviewer:x:{uid}:42010::/:/bin/false\n"
        )).unwrap();
        // Use an unrelated pre-existing grant to supply the socket GID from disk.
        let other = store.create_agent(NewAgent::new("other", "codex")).unwrap();
        edit_registry(dir.path(), |grants, _| {
            grants.insert(
                other.id.to_string(),
                serde_json::json!({
                    "user":"seat-other", "uid":uid + 100, "gid":42010, "socket_gid":42011
                }),
            );
            Ok(true)
        })
        .unwrap();
        assert!(!bind_principal(&store, &root, None).unwrap());
        let implicit = RootSeatRequest::default();
        assert!(bind_principal(&store, &root, Some(&implicit)).unwrap());
        let (user, bound_uid, _, _) = read_admitted_principal(dir.path(), root.id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!((user.as_str(), bound_uid), ("seat-receiver", uid));
        let explicit = RootSeatRequest {
            user: Some(user),
            uid: Some(uid),
        };
        let registry = dir.path().join(REGISTRY);
        let saved = std::fs::read(&registry).unwrap();
        assert!(!bind_principal(&store, &root, Some(&explicit)).unwrap());
        // Same username, or different username with the same OS UID.
        for request in [&explicit, &implicit] {
            assert!(matches!(
                bind_principal(&store, &next, Some(request)),
                Err(Error::PermissionDenied(_))
            ));
        }
        let mismatch = RootSeatRequest {
            user: None,
            uid: Some(uid + 1),
        };
        assert!(matches!(
            bind_principal(&store, &root, Some(&mismatch)),
            Err(Error::PermissionDenied(_))
        ));
        assert_eq!(std::fs::read(&registry).unwrap(), saved);
        assert_eq!(std::fs::metadata(&registry).unwrap().mode() & 0o777, 0o600);
        rollback_root_seat(&store, &root.id).unwrap();
        assert!(
            read_admitted_principal(dir.path(), root.id.as_str())
                .unwrap()
                .is_none()
        );
        assert!(bind_principal(&store, &next, Some(&explicit)).unwrap());
    }

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
        bind_principal(&store, &child, None).unwrap();
        let retained = read_principal_registry(dir.path()).unwrap().unwrap();
        let grants: Value = serde_json::from_str(&retained).unwrap();
        assert_eq!(grants[child.id.as_str()], grants[root.id.as_str()]);
        assert_eq!(std::fs::metadata(&path).unwrap().mode() & 0o777, 0o600);
        store.rename_agent(&child.id, "renamed").unwrap();
        bind_principal(&store, &store.agent(&child.id).unwrap(), None).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), retained);
        std::fs::write(&path, "corrupt").unwrap();
        assert!(bind_principal(&store, &child, None).is_err());
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
