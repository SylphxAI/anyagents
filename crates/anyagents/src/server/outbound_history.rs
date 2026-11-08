//! Durable local settlement for effects whose external acknowledgement cannot
//! participate in the store transaction. Recovery writes history, never sends.

use super::{ApiError, Delivery};
use anyagents_core::{AgentId, Error, MessageId, Store};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize)]
struct Intent {
    effect_key: String,
    agent_id: AgentId,
    destination: String,
    reply_to: Option<String>,
    body: Option<String>,
    receipt: Option<Vec<Delivery>>,
}

fn directory(store: &Store) -> PathBuf {
    store.data_dir().join("outbound-history")
}

fn write(path: &Path, intent: &Intent) -> Result<(), Error> {
    let bytes = serde_json::to_vec(intent)
        .map_err(|error| Error::Message(format!("serialize outbound obligation: {error}")))?;
    let parent = path.parent().expect("owned journal path");
    let temporary = parent.join(format!(".{}.tmp", MessageId::generate()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    std::fs::rename(&temporary, path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn load(path: &Path) -> Result<Option<Intent>, Error> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(|error| {
            Error::Message(format!(
                "invalid outbound obligation {}: {error}",
                path.display()
            ))
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn settle(store: &Store, path: &Path, intent: &Intent) -> Result<(), Error> {
    let receipt = intent.receipt.as_ref().ok_or_else(|| {
        Error::Message(
            "outbound effect has no terminal receipt; automatic resend is refused".into(),
        )
    })?;
    if !receipt.is_empty() {
        store.record_outbound_once(
            &intent.effect_key,
            &intent.agent_id,
            &intent.destination,
            intent.reply_to.as_deref(),
            intent
                .body
                .as_deref()
                .ok_or_else(|| Error::Message("pending outbound obligation has no body".into()))?,
        )?;
    }
    // Completed receipts retain no message body or digest. An erasure cannot
    // leave a second raw-body copy in this local settlement journal.
    let done = Intent {
        effect_key: intent.effect_key.clone(),
        agent_id: intent.agent_id.clone(),
        destination: intent.destination.clone(),
        reply_to: intent.reply_to.clone(),
        body: None,
        receipt: intent.receipt.clone(),
    };
    write(&path.with_extension("done"), &done)?;
    std::fs::remove_file(path)?;
    File::open(path.parent().expect("owned journal path"))?.sync_all()?;
    Ok(())
}

/// The obligation exists before the effect. After acknowledgement its receipt
/// survives a failed local projection; a keyed retry reuses that receipt.
pub(super) fn deliver<F>(
    store: &Store,
    agent_id: &AgentId,
    destination: &str,
    reply_to: Option<&str>,
    body: &str,
    key: Option<&str>,
    effect: F,
) -> Result<Vec<Delivery>, ApiError>
where
    F: FnOnce() -> Result<Vec<Delivery>, ApiError>,
{
    let directory = directory(store);
    std::fs::create_dir_all(&directory)
        .map_err(Error::from)
        .map_err(ApiError::from_store)?;
    let effect_key = key
        .map(str::to_owned)
        .unwrap_or_else(|| MessageId::generate().to_string());
    // Internal keys are short; hexadecimal encoding is reversible and avoids
    // introducing a second collision or pathname interpretation boundary.
    let filename: String = effect_key
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let path = directory.join(format!("{filename}.pending"));
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(path.with_extension("lock"))
        .map_err(Error::from)
        .map_err(ApiError::from_store)?;
    lock.lock()
        .map_err(Error::from)
        .map_err(ApiError::from_store)?;
    let existing = load(&path.with_extension("done"))
        .map_err(ApiError::from_store)?
        .or(load(&path).map_err(ApiError::from_store)?);
    if let Some(intent) = existing {
        if intent.effect_key != effect_key
            || &intent.agent_id != agent_id
            || intent.destination != destination
            || intent.reply_to.as_deref() != reply_to
            || intent
                .body
                .as_deref()
                .is_some_and(|retained| retained != body)
        {
            return Err(ApiError::from_store(Error::Message(
                "outbound obligation payload conflict".into(),
            )));
        }
        let mut receipt = intent.receipt.clone().ok_or_else(|| ApiError::from_store(Error::Message(
            "outbound effect outcome is unknown; retained obligation prevents an automatic resend".into())))?;
        if path.exists() {
            let pending = load(&path).map_err(ApiError::from_store)?.ok_or_else(|| {
                ApiError::from_store(Error::Message("outbound obligation disappeared".into()))
            })?;
            settle(store, &path, &pending).map_err(ApiError::from_store)?;
        } else if !receipt.is_empty() {
            // Check the canonical body, including redaction, rather than
            // retaining a replay copy or restoring an erased body.
            store
                .record_outbound_once(&intent.effect_key, agent_id, destination, reply_to, body)
                .map_err(ApiError::from_store)?;
        }
        for delivery in &mut receipt {
            delivery.duplicate = true;
        }
        return Ok(receipt);
    }
    let mut intent = Intent {
        effect_key,
        agent_id: agent_id.clone(),
        destination: destination.into(),
        reply_to: reply_to.map(str::to_owned),
        body: Some(body.into()),
        receipt: None,
    };
    write(&path, &intent).map_err(ApiError::from_store)?;
    let receipt = effect()?;
    intent.receipt = Some(receipt.clone());
    write(&path, &intent).map_err(ApiError::from_store)?;
    settle(store, &path, &intent).map_err(ApiError::from_store)?;
    Ok(receipt)
}

/// Only receipts of already acknowledged effects are replayed. Prepared or
/// malformed obligations remain untouched and produce an explicit diagnostic.
pub(super) fn reconcile(store: &Store) -> Result<(), Error> {
    let directory = directory(store);
    let entries = match std::fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let mut first_error = None;
    for entry in entries {
        let result = (|| -> Result<(), Error> {
            let path = entry?.path();
            if path
                .extension()
                .is_none_or(|extension| extension != "pending")
            {
                return Ok(());
            }
            let lock = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .mode(0o600)
                .open(path.with_extension("lock"))?;
            match lock.try_lock() {
                Ok(()) => {}
                Err(std::fs::TryLockError::WouldBlock) => return Ok(()),
                Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
            }
            if let Some(intent) = load(&path)? {
                settle(store, &path, &intent)?;
            }
            Ok(())
        })();
        if let Err(error) = result {
            first_error.get_or_insert(error);
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn delivery() -> Vec<Delivery> {
        vec![Delivery {
            to: "agent:recipient".into(),
            id: "provider-receipt".into(),
            seq: 1,
            duplicate: false,
        }]
    }

    #[test]
    fn failed_history_reuses_acknowledgement_without_repeating_effect() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let sender = store
            .create_agent(anyagents_core::NewAgent::new("sender", "opencode"))
            .unwrap();
        let db = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
        db.execute_batch("CREATE TRIGGER fail_history BEFORE INSERT ON outbound_messages BEGIN SELECT RAISE(ABORT, 'injected history failure'); END;").unwrap();
        let effects = Cell::new(0);
        let first = deliver(
            &store,
            &sender.id,
            "agent:recipient",
            None,
            "hello",
            Some("same-call"),
            || {
                effects.set(effects.get() + 1);
                Ok(delivery())
            },
        );
        assert!(first.is_err());
        assert_eq!(effects.get(), 1);
        db.execute_batch("DROP TRIGGER fail_history;").unwrap();
        // Restart-style recovery settles history from the durable receipt only.
        reconcile(&store).unwrap();
        let retry = deliver(
            &store,
            &sender.id,
            "agent:recipient",
            None,
            "hello",
            Some("same-call"),
            || {
                effects.set(effects.get() + 1);
                Ok(delivery())
            },
        )
        .unwrap_or_else(|error| panic!("{}", error.message));
        assert!(retry[0].duplicate);
        assert_eq!(effects.get(), 1);
        for entry in std::fs::read_dir(directory(&store))
            .unwrap()
            .filter_map(Result::ok)
        {
            if entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "done")
            {
                let retained: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(entry.path()).unwrap()).unwrap();
                assert!(
                    retained["body"].is_null(),
                    "completed receipts retain no body copy"
                );
            }
        }
        assert_eq!(
            db.query_row("SELECT COUNT(*) FROM outbound_messages", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert!(
            deliver(
                &store,
                &sender.id,
                "agent:recipient",
                None,
                "changed",
                Some("same-call"),
                || panic!("conflict must not dispatch")
            )
            .is_err()
        );
    }

    #[test]
    fn unknown_effect_retains_intent_and_refuses_automatic_resend() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let sender = store
            .create_agent(anyagents_core::NewAgent::new("sender", "opencode"))
            .unwrap();
        assert!(
            deliver(
                &store,
                &sender.id,
                "agent:recipient",
                None,
                "hello",
                Some("unknown-call"),
                || {
                    Err(ApiError::from_store(Error::Message(
                        "lost remote acknowledgement".into(),
                    )))
                }
            )
            .is_err()
        );
        assert!(reconcile(&store).is_err());
        assert!(
            deliver(
                &store,
                &sender.id,
                "agent:recipient",
                None,
                "hello",
                Some("unknown-call"),
                || panic!("unknown must not dispatch")
            )
            .is_err()
        );
        assert_eq!(
            std::fs::read_dir(directory(&store))
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "pending"))
                .count(),
            1
        );
    }

    #[test]
    fn journal_failure_prevents_effect() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let sender = store
            .create_agent(anyagents_core::NewAgent::new("sender", "opencode"))
            .unwrap();
        std::fs::write(directory(&store), b"blocked directory sentinel").unwrap();
        assert!(
            deliver(
                &store,
                &sender.id,
                "agent:recipient",
                None,
                "hello",
                None,
                || panic!("no durable obligation means no effect")
            )
            .is_err()
        );
        assert_eq!(
            std::fs::read(directory(&store)).unwrap(),
            b"blocked directory sentinel"
        );
    }
}
