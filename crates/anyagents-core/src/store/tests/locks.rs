use super::super::*;
use super::helpers::{agent, setup};

#[test]
fn second_lock_is_busy_until_the_first_drops() {
    let (dir, store) = setup();
    let held_agent = agent(&store, "alpha");
    let other = agent(&store, "beta");
    let held = store.try_lock_agent(&held_agent.id).unwrap();
    let again = Store::open(dir.path().join("state.db")).unwrap();
    assert!(matches!(
        again.try_lock_agent(&held_agent.id).unwrap_err(),
        Error::AgentBusy
    ));
    let _other_lock = store.try_lock_agent(&other.id).unwrap();
    drop(held);
    again.try_lock_agent(&held_agent.id).unwrap();
}

#[test]
fn dropping_agent_lock_releases_it_with_an_inherited_descriptor_open() {
    let (_dir, store) = setup();
    let owner = agent(&store, "owner");
    let lock = store.try_lock_agent(&owner.id).unwrap();
    // A concurrent fork can inherit this open file description until exec.
    // A clone deterministically models that lifetime without timing a fork.
    let inherited = lock.flock_file().try_clone().unwrap();
    drop(lock);
    let _next_turn = store.try_lock_agent(&owner.id).unwrap();
    drop(inherited);
}

#[test]
fn a_shared_copy_holds_the_lock_only_until_the_owner_releases_it() {
    let (_dir, store) = setup();
    let owner = agent(&store, "owner");
    let lock = store.try_lock_agent(&owner.id).unwrap();
    let keeper = lock.share().unwrap();
    // A copy never unlocks: dropping it leaves the owner holding the lock.
    drop(keeper);
    assert!(matches!(
        store.try_lock_agent(&owner.id),
        Err(Error::AgentBusy)
    ));
    let keeper = lock.share().unwrap();
    drop(lock);
    // The owner released the open file description for every copy.
    let _next = store.try_lock_agent(&owner.id).unwrap();
    drop(keeper);
}

#[test]
fn a_retained_lock_stays_held_by_the_copy_a_keeper_keeps() {
    let (_dir, store) = setup();
    let owner = agent(&store, "owner");
    let lock = store.try_lock_agent(&owner.id).unwrap();
    let keeper = lock.share().unwrap();
    let kernel_copy = lock.share().unwrap();
    kernel_copy.retain();
    drop(kernel_copy);
    drop(lock);
    // The keeper's descriptor still holds it after the kernel let go.
    assert!(matches!(
        store.try_lock_agent(&owner.id),
        Err(Error::AgentBusy)
    ));
    // The next kernel adopts the keeper's descriptor and owns release.
    let adopted = AgentLock::adopt(std::os::fd::OwnedFd::from(
        keeper.flock_file().try_clone().unwrap(),
    ));
    drop(keeper);
    assert!(matches!(
        store.try_lock_agent(&owner.id),
        Err(Error::AgentBusy)
    ));
    drop(adopted);
    let _next = store.try_lock_agent(&owner.id).unwrap();
}

#[test]
fn dropping_lifecycle_lock_releases_it_with_an_inherited_descriptor_open() {
    let (_dir, store) = setup();
    let owner = agent(&store, "owner");
    for nonblocking in [false, true] {
        let lock = if nonblocking {
            store.try_lock_agent_lifecycle(&owner.id).unwrap()
        } else {
            store.lock_agent_lifecycle(&owner.id).unwrap()
        };
        // A duplicate shares the open file description inherited by fork,
        // even when the descriptor is close-on-exec. No fork timing needed.
        let inherited = lock.flock_file().try_clone().unwrap();
        assert!(matches!(
            store.try_lock_agent_lifecycle(&owner.id),
            Err(Error::AgentBusy)
        ));
        drop(lock);
        let _next_lifecycle = store.try_lock_agent_lifecycle(&owner.id).unwrap();
        drop(inherited);
    }
}

/// A Telegram reply, reaction or edit makes ingress look the referenced
/// message up in the stored history by text pattern, which can read every
/// stored message, and a group reaction also counts the chat's agents the
/// same way. Those reads ran inside the ingress transaction, so the
/// daemon's single writer, and every other write, waited for them (a reply
/// to apollo on 2026-11-13 held it for 100 s). Here another thread writes
/// while each lookup runs; it must finish while the lookup is still going.
/// Lock order: file locks that can wait (an agent's lifecycle, the
/// outbound barrier and key locks) come before the Store writer. A thread
/// holding the writer that waits for one is refused in debug builds; a
/// non-blocking attempt never waits and stays allowed.
#[cfg(debug_assertions)]
#[test]
fn waiting_for_a_lifecycle_while_holding_the_writer_is_refused() {
    let (_dir, store) = setup();
    let held = agent(&store, "lock_order");
    assert!(!holds_store_writer());
    let writer = store.conn();
    assert!(holds_store_writer());
    drop(store.try_lock_agent_lifecycle(&held.id).unwrap());
    let waited = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store.lock_agent_lifecycle(&held.id)
    }));
    assert!(
        waited.is_err(),
        "a lifecycle wait under the writer was allowed"
    );
    drop(writer);
    assert!(!holds_store_writer());
    drop(store.lock_agent_lifecycle(&held.id).unwrap());
}
