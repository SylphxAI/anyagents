//! Durable local settlement for effects whose external acknowledgement cannot
//! participate in the store transaction. Recovery writes history, never sends.
//!
//! `<data>/outbound-history/` holds:
//! - `<key>.pending`: an open obligation, written before its effect runs and
//!   rewritten with the effect's receipt. Core redaction inspects these, and
//!   every `*.tmp`, before an erasure.
//! - `<key>.lock`: the key's flock. It exists only while held.
//! - `<key>.pending.tmp`, `<key>.done.tmp`: a write in flight. A failed write
//!   removes its own; one a dead process left is recovered under the key lock.
//! - `receipts/<key>.done`: a settled receipt, without the body. A keyed retry
//!   reuses it. Receipts settled before this layout stay at the top level as
//!   `<key>.done`, are read there, and move into `receipts/` on the first scan.
//!
//! An effect that fails is never left as an obligation when its error proves
//! it did not happen (a Telegram API refusal of the whole post) or when its
//! destination dedupes a replay (a mailbox): the obligation is removed and the
//! caller has the error. Any other failure leaves the outcome unknown: the
//! obligation stays, with the error, and is never resent automatically. The
//! sender is told once through its mailbox, `agents inspect` and `status` list
//! it, and the operator retries or discards it (`anyagents outbound`).
//!
//! `<key>` is the hexadecimal effect key. Only this daemon opens obligations,
//! so it knows each one: the first reconcile lists the directory once,
//! registering the obligations a previous process left, and every obligation
//! this process opens stays registered until it settles. What a previous
//! process left to tidy (unheld lock files, staged writes, flat-layout
//! receipts) is worked off a bounded batch per reconcile. Reconciles then
//! visit registered obligations only and touch no file when none is open.

use super::{ApiError, Delivery};
use anyagents_core::{AgentId, Error, MessageId, Store};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock, PoisonError};

#[derive(Serialize, Deserialize)]
struct Intent {
    effect_key: String,
    agent_id: AgentId,
    destination: String,
    reply_to: Option<String>,
    body: Option<String>,
    receipt: Option<Vec<Delivery>>,
    /// When the obligation was opened. Absent in obligations from before it
    /// was recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    created_at_ms: Option<i64>,
    /// The error that left the effect's outcome unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    uncertain: Option<String>,
    /// How an operator settled it without its own effect's receipt:
    /// `discarded`, or `retried` (the receipt is then the new send's).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    outcome: Option<String>,
}

/// An obligation whose effect's outcome is unknown and that nothing resends
/// automatically: its sender was told, and the operator retries or discards
/// it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Unsettled {
    pub effect_key: String,
    pub agent_id: AgentId,
    pub destination: String,
    pub reply_to: Option<String>,
    pub since_ms: Option<i64>,
    pub error: Option<String>,
    pub body: String,
}

impl Unsettled {
    fn of(intent: &Intent, since_ms: Option<i64>) -> Self {
        Unsettled {
            effect_key: intent.effect_key.clone(),
            agent_id: intent.agent_id.clone(),
            destination: intent.destination.clone(),
            reply_to: intent.reply_to.clone(),
            since_ms: intent.created_at_ms.or(since_ms),
            error: intent.uncertain.clone(),
            body: intent.body.clone().unwrap_or_default(),
        }
    }

    /// The first characters of the body, for a listing.
    pub fn excerpt(&self) -> String {
        let mut chars = self.body.chars();
        let head: String = chars.by_ref().take(160).collect();
        let head = head.replace('\n', " ");
        if chars.next().is_some() {
            format!("{head}…")
        } else {
            head
        }
    }
}

/// Telegram error classes that do not prove the API posted nothing.
const UNCERTAIN_TELEGRAM_CLASSES: &[&str] = &[
    "timeout",
    "network",
    "transport",
    "response_read",
    "invalid_response",
    "partial_delivery",
];

/// Whether an effect's error proves the effect did not happen: the Telegram
/// API answered the whole post with a refusal (a 4xx status), so nothing was
/// posted. Any other error (a timeout, a dropped connection, a later part of
/// a post, a local failure after the post) proves nothing.
fn proves_not_sent(message: &str) -> bool {
    let Some(detail) = message
        .strip_prefix("telegram ")
        .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
    else {
        return false;
    };
    let class = detail
        .get("class")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    !UNCERTAIN_TELEGRAM_CLASSES.contains(&class)
        && detail
            .get("status")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|status| (400..500).contains(&status))
}

impl Intent {
    fn same_obligation(&self, other: &Intent) -> bool {
        self.effect_key == other.effect_key
            && self.agent_id == other.agent_id
            && self.destination == other.destination
            && self.reply_to == other.reply_to
            && self.body == other.body
    }
}

fn directory(store: &Store) -> PathBuf {
    store.data_dir().join("outbound-history")
}

/// Every file of one effect key.
struct Files {
    pending: PathBuf,
    lock: PathBuf,
    staged_pending: PathBuf,
    staged_done: PathBuf,
    done: PathBuf,
    legacy_done: PathBuf,
}

impl Files {
    fn new(directory: &Path, name: &str) -> Self {
        Files {
            pending: directory.join(format!("{name}.pending")),
            lock: directory.join(format!("{name}.lock")),
            staged_pending: directory.join(format!("{name}.pending.tmp")),
            staged_done: directory.join(format!("{name}.done.tmp")),
            done: directory.join("receipts").join(format!("{name}.done")),
            legacy_done: directory.join(format!("{name}.done")),
        }
    }
}

/// Internal keys are short; hexadecimal encoding is reversible and avoids
/// introducing a second collision or pathname interpretation boundary.
fn encode(key: &str) -> String {
    key.as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Whether `name` is a key encoding this module wrote.
fn is_encoded(name: &str) -> bool {
    !name.is_empty()
        && name.len().is_multiple_of(2)
        && name.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Open obligations of each journal directory this process knows of.
#[derive(Default)]
struct Open {
    /// The directory was listed for obligations a previous process left.
    scanned: bool,
    /// Encoded keys whose `.pending` file may exist.
    keys: BTreeSet<String>,
    /// Files the listing found that a previous process left to tidy, worked
    /// off at most [`TIDY_BATCH`] per reconcile.
    leftovers: Vec<String>,
    /// Encoded keys of obligations whose outcome is unknown, moved out of
    /// `keys` so that a reconcile no longer visits them, with whether their
    /// sender was told.
    stuck: std::collections::BTreeMap<String, (Unsettled, bool)>,
}

/// Leftover files one reconcile tidies. A flat-layout journal held 11k
/// receipts and as many lock files; moving them all in one maintenance pass
/// kept that pass (and the publication barrier) busy for the whole migration.
const TIDY_BATCH: usize = 256;

static OPEN: OnceLock<Mutex<HashMap<PathBuf, Open>>> = OnceLock::new();

fn with_open<R>(directory: &Path, f: impl FnOnce(&mut Open) -> R) -> R {
    let mut journals = OPEN
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    f(journals.entry(directory.to_path_buf()).or_default())
}

/// Releases the open-file-description flock on drop even when a concurrent
/// `fork()` temporarily holds an inherited descriptor before `execve()`.
struct FileLock(File);

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

/// Shared with core redaction; acquire before entering any Store transaction.
fn publication_barrier(store: &Store) -> Result<FileLock, Error> {
    debug_assert!(
        !anyagents_core::holds_store_writer(),
        "the publication barrier is waited for while this thread holds the Store writer"
    );
    let directory = directory(store);
    std::fs::create_dir_all(&directory)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(directory.join(".publication.lock"))?;
    file.lock_shared()?;
    Ok(FileLock(file))
}

/// One key's exclusive flock, on a file that exists only while it is held.
/// The holder unlinks the file before it unlocks, and an acquirer keeps its
/// lock only while the path still names the inode it locked; a waiter that
/// locked an unlinked inode retries on the current one. So two holders never
/// coexist, and no lock file outlives its holder.
struct KeyLock {
    file: File,
    path: PathBuf,
}

impl KeyLock {
    /// `wait` blocks for a holder; otherwise a held key yields `None`.
    fn acquire(path: &Path, wait: bool) -> Result<Option<KeyLock>, Error> {
        debug_assert!(
            !wait || !anyagents_core::holds_store_writer(),
            "an outbound key lock is waited for while this thread holds the Store writer"
        );
        loop {
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .mode(0o600)
                .open(path)?;
            if wait {
                file.lock()?;
            } else {
                match file.try_lock() {
                    Ok(()) => {}
                    Err(std::fs::TryLockError::WouldBlock) => return Ok(None),
                    Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
                }
            }
            let held = file.metadata()?;
            match std::fs::symlink_metadata(path) {
                Ok(current) if current.dev() == held.dev() && current.ino() == held.ino() => {
                    return Ok(Some(KeyLock {
                        file,
                        path: path.to_path_buf(),
                    }));
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
}

impl Drop for KeyLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = self.file.unlock();
    }
}

fn sync_directory(path: &Path) -> Result<(), Error> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn create_receipts(directory: &Path) -> Result<PathBuf, Error> {
    let receipts = directory.join("receipts");
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&receipts)?;
    Ok(receipts)
}

/// Atomically replace `target` with `intent`, staged at `staged`, a top-level
/// `*.tmp` that the key's lock holder alone writes. A failed write removes the
/// staged copy it made, so no failed write leaves a temporary file behind.
fn write(staged: &Path, target: &Path, intent: &Intent) -> Result<(), Error> {
    let bytes = serde_json::to_vec(intent)
        .map_err(|error| Error::Message(format!("serialize outbound obligation: {error}")))?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(staged)?;
    let result = (|| -> Result<(), Error> {
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(staged, target)?;
        let target_dir = target.parent().expect("owned journal path");
        sync_directory(target_dir)?;
        let staged_dir = staged.parent().expect("owned journal path");
        if staged_dir != target_dir {
            sync_directory(staged_dir)?;
        }
        Ok(())
    })();
    if result.is_err()
        && let Err(error) = std::fs::remove_file(staged)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        eprintln!(
            "outbound history: cannot remove failed write {}: {error}",
            staged.display()
        );
    }
    result
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

fn remove_if_present(path: &Path) -> Result<(), Error> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Finish what a dead lock holder of this key staged; call with the key's lock
/// held. A complete staged obligation that carries an acknowledged effect's
/// receipt is the newer state of a committed obligation lacking one, so it is
/// rolled forward. Every other staged copy is superseded by the committed
/// files and removed.
fn recover_staged(directory: &Path, name: &str) -> Result<(), Error> {
    let files = Files::new(directory, name);
    match std::fs::read(&files.staged_pending) {
        Ok(bytes) => roll_forward_or_remove(&bytes, &files.staged_pending, &files.pending)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    remove_if_present(&files.staged_done)
}

fn roll_forward_or_remove(bytes: &[u8], staged: &Path, pending: &Path) -> Result<(), Error> {
    if let Ok(staged_intent) = serde_json::from_slice::<Intent>(bytes)
        && staged_intent.body.is_some()
        && staged_intent.receipt.is_some()
        && let Some(committed) = load(pending)?
        && committed.receipt.is_none()
        && committed.same_obligation(&staged_intent)
    {
        std::fs::rename(staged, pending)?;
        return sync_directory(pending.parent().expect("owned journal path"));
    }
    remove_if_present(staged)
}

fn settle(store: &Store, directory: &Path, name: &str, intent: &Intent) -> Result<(), Error> {
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
        created_at_ms: intent.created_at_ms,
        uncertain: intent.uncertain.clone(),
        outcome: intent.outcome.clone(),
    };
    let files = Files::new(directory, name);
    create_receipts(directory)?;
    write(&files.staged_done, &files.done, &done)?;
    std::fs::remove_file(&files.pending)?;
    sync_directory(directory)?;
    with_open(directory, |open| {
        open.keys.remove(name);
        open.stuck.remove(name);
    });
    Ok(())
}

/// The obligation exists before the effect. After acknowledgement its receipt
/// survives a failed local projection; a keyed retry reuses that receipt.
#[allow(clippy::too_many_arguments)]
pub(super) fn deliver<F>(
    store: &Store,
    agent_id: &AgentId,
    destination: &str,
    reply_to: Option<&str>,
    body: &str,
    key: Option<&str>,
    replay_safe: bool,
    effect: F,
) -> Result<Vec<Delivery>, ApiError>
where
    F: FnOnce() -> Result<Vec<Delivery>, ApiError>,
{
    let _barrier = publication_barrier(store).map_err(ApiError::from_store)?;
    let directory = directory(store);
    let effect_key = key
        .map(str::to_owned)
        .unwrap_or_else(|| MessageId::generate().to_string());
    let name = encode(&effect_key);
    let files = Files::new(&directory, &name);
    let _lock = KeyLock::acquire(&files.lock, true)
        .map_err(ApiError::from_store)?
        .expect("a waiting acquire holds the lock");
    recover_staged(&directory, &name).map_err(ApiError::from_store)?;
    // A legacy receipt only ever moves into `receipts/`, so reading the
    // legacy place first cannot miss one that moves between the two reads.
    let existing = match load(&files.legacy_done).map_err(ApiError::from_store)? {
        Some(intent) => Some(intent),
        None => match load(&files.done).map_err(ApiError::from_store)? {
            Some(intent) => Some(intent),
            None => load(&files.pending).map_err(ApiError::from_store)?,
        },
    };
    let mut intent = if let Some(intent) = existing {
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
        if let Some(mut receipt) = intent.receipt.clone() {
            if files.pending.exists() {
                let pending = load(&files.pending)
                    .map_err(ApiError::from_store)?
                    .ok_or_else(|| {
                        ApiError::from_store(Error::Message(
                            "outbound obligation disappeared".into(),
                        ))
                    })?;
                with_open(&directory, |open| open.keys.insert(name.clone()));
                settle(store, &directory, &name, &pending).map_err(ApiError::from_store)?;
            } else if !receipt.is_empty() {
                store
                    .record_outbound_once(&intent.effect_key, agent_id, destination, reply_to, body)
                    .map_err(ApiError::from_store)?;
            }
            for delivery in &mut receipt {
                delivery.duplicate = true;
            }
            return Ok(receipt);
        }
        if !replay_safe || key.is_none() {
            return Err(ApiError::from_store(Error::Message(
                "outbound effect outcome is unknown; retained obligation prevents an automatic resend".into())));
        }
        // The caller explicitly supplies a durable per-recipient source key.
        // A partial local fan-out can finish without repeating accepted rows.
        intent
    } else {
        Intent {
            effect_key,
            agent_id: agent_id.clone(),
            destination: destination.into(),
            reply_to: reply_to.map(str::to_owned),
            body: Some(body.into()),
            receipt: None,
            created_at_ms: Some(anyagents_core::now_ms()),
            uncertain: None,
            outcome: None,
        }
    };
    // Registered before it can exist, so no outcome leaves it unreconciled.
    with_open(&directory, |open| open.keys.insert(name.clone()));
    write(&files.staged_pending, &files.pending, &intent).map_err(ApiError::from_store)?;
    let receipt = match effect() {
        Ok(receipt) => receipt,
        Err(error) => {
            if replay_safe || proves_not_sent(&error.message) {
                // Nothing took effect, or a replay is deduped by its
                // destination: no obligation is left to settle, and a
                // later send (keyed or not) runs the effect afresh.
                if let Err(cleanup) =
                    remove_if_present(&files.pending).and_then(|()| sync_directory(&directory))
                {
                    eprintln!(
                        "outbound history: cannot drop the obligation of a send that did not happen: {cleanup}"
                    );
                } else {
                    with_open(&directory, |open| {
                        open.keys.remove(&name);
                        open.stuck.remove(&name);
                    });
                }
            } else {
                // The outcome is unknown: the obligation stays, with why,
                // and is never resent automatically (see `reconcile`).
                intent.uncertain = Some(error.message.clone());
                if let Err(record) = write(&files.staged_pending, &files.pending, &intent) {
                    eprintln!("outbound history: cannot record why a send is uncertain: {record}");
                }
            }
            return Err(error);
        }
    };
    intent.receipt = Some(receipt.clone());
    write(&files.staged_pending, &files.pending, &intent).map_err(ApiError::from_store)?;
    settle(store, &directory, &name, &intent).map_err(ApiError::from_store)?;
    Ok(receipt)
}

/// Only receipts of already acknowledged effects are replayed. Prepared or
/// malformed obligations remain untouched and produce an explicit diagnostic.
/// The cost is one visit per open obligation plus at most [`TIDY_BATCH`]
/// leftovers of a previous process; with neither, no file is touched.
pub(super) fn reconcile(store: &Store) -> Result<(), Error> {
    let directory = directory(store);
    if with_open(&directory, |open| {
        open.scanned
            && open.keys.is_empty()
            && open.leftovers.is_empty()
            && open.stuck.values().all(|(_, told)| *told)
    }) {
        return Ok(());
    }
    let _barrier = publication_barrier(store)?;
    let mut first_error = None;
    if !with_open(&directory, |open| open.scanned) {
        match list(&directory) {
            Ok((pending, leftovers)) => with_open(&directory, |open| {
                open.keys.extend(pending);
                open.leftovers = leftovers;
                open.scanned = true;
            }),
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }
    let batch: Vec<String> = with_open(&directory, |open| {
        let keep = open.leftovers.len().saturating_sub(TIDY_BATCH);
        open.leftovers.split_off(keep)
    });
    if let Some(error) = tidy(&directory, &batch) {
        first_error.get_or_insert(error);
    }
    let keys: Vec<String> = with_open(&directory, |open| open.keys.iter().cloned().collect());
    for name in keys {
        let result = (|| -> Result<(), Error> {
            let files = Files::new(&directory, &name);
            // A delivery holding the key settles it itself.
            let Some(_lock) = KeyLock::acquire(&files.lock, false)? else {
                return Ok(());
            };
            recover_staged(&directory, &name)?;
            match load(&files.pending)? {
                // Its effect's outcome is unknown: it is never resent, and
                // no longer visited; its sender is told below.
                Some(intent) if intent.receipt.is_none() => {
                    hold_unsettled(&directory, &name, &intent);
                    Ok(())
                }
                Some(intent) => settle(store, &directory, &name, &intent),
                None => {
                    with_open(&directory, |open| open.keys.remove(&name));
                    Ok(())
                }
            }
        })();
        if let Err(error) = result {
            first_error.get_or_insert(error);
        }
    }
    tell_senders(store, &directory);
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Move an obligation whose effect's outcome is unknown out of the keys a
/// reconcile visits, into the held list `unsettled` reports. Called with the
/// key's lock held.
fn hold_unsettled(directory: &Path, name: &str, intent: &Intent) {
    let since = std::fs::metadata(Files::new(directory, name).pending)
        .and_then(|meta| meta.modified())
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .and_then(|age| i64::try_from(age.as_millis()).ok());
    let unsettled = Unsettled::of(intent, since);
    with_open(directory, |open| {
        open.keys.remove(name);
        open.stuck
            .entry(name.to_string())
            .or_insert((unsettled, false));
    });
}

/// Tell each held obligation's sender, once, through its own mailbox. The
/// notice is keyed by the effect key, so a restart does not repeat it.
fn tell_senders(store: &Store, directory: &Path) {
    let untold: Vec<(String, Unsettled)> = with_open(directory, |open| {
        open.stuck
            .iter()
            .filter(|(_, (_, told))| !told)
            .map(|(name, (unsettled, _))| (name.clone(), unsettled.clone()))
            .collect()
    });
    for (name, unsettled) in untold {
        let since = unsettled.since_ms.map_or_else(String::new, |ms| {
            format!(" at {} UTC", crate::ui::format_timestamp_ms(ms))
        });
        let why = unsettled
            .error
            .as_deref()
            .map_or_else(String::new, |error| {
                format!(" The send failed with: {error}.")
            });
        let text = format!(
            "[Delivery Uncertain] Your message to {} (sent{since}) could not be confirmed delivered, and it is not resent automatically, so that a delivered message is never sent twice.{why}\n\nCheck whether the recipient has it. If not, send it again yourself. The operator can also retry or discard it: `anyagents outbound retry {}` / `anyagents outbound discard {}`.\n\nThe message began: {}",
            unsettled.destination,
            unsettled.effect_key,
            unsettled.effect_key,
            unsettled.excerpt(),
        );
        let told = match store.post_notice(
            &unsettled.agent_id,
            format!("anyagents:outbound-unsettled:{}", unsettled.effect_key),
            text,
        ) {
            Ok(_) => true,
            // A sender that is gone cannot be told; the operator still sees it.
            Err(Error::AgentNotFound(_)) => true,
            Err(error) => {
                eprintln!(
                    "outbound history: cannot tell {} that {} is unconfirmed: {error}",
                    unsettled.agent_id, unsettled.effect_key
                );
                false
            }
        };
        if told {
            // Reported once per obligation, when its sender is told.
            eprintln!(
                "outbound history: {} from {} to {} could not be confirmed and is held, not resent; `anyagents outbound` retries or discards it{}",
                unsettled.effect_key,
                unsettled.agent_id,
                unsettled.destination,
                unsettled
                    .error
                    .as_deref()
                    .map_or_else(String::new, |error| format!(": {error}"))
            );
            with_open(directory, |open| {
                if let Some((_, flag)) = open.stuck.get_mut(&name) {
                    *flag = true;
                }
            });
        }
    }
}

/// The obligations whose effect's outcome is unknown, oldest first. Reads
/// only: the directory is listed once per process, as a reconcile would, and
/// a key some send still holds is in flight, not unsettled.
pub(crate) fn unsettled(store: &Store) -> Vec<Unsettled> {
    let directory = directory(store);
    if !with_open(&directory, |open| open.scanned)
        && let Ok((pending, leftovers)) = list(&directory)
    {
        with_open(&directory, |open| {
            if !open.scanned {
                open.keys.extend(pending);
                open.leftovers = leftovers;
                open.scanned = true;
            }
        });
    }
    let keys: Vec<String> = with_open(&directory, |open| open.keys.iter().cloned().collect());
    for name in keys {
        let files = Files::new(&directory, &name);
        let Ok(Some(_lock)) = KeyLock::acquire(&files.lock, false) else {
            continue;
        };
        if let Ok(Some(intent)) = load(&files.pending)
            && intent.receipt.is_none()
        {
            hold_unsettled(&directory, &name, &intent);
        }
    }
    let mut held: Vec<Unsettled> = with_open(&directory, |open| {
        open.stuck.values().map(|(held, _)| held.clone()).collect()
    });
    held.sort_by(|a, b| {
        a.since_ms
            .cmp(&b.since_ms)
            .then_with(|| a.effect_key.cmp(&b.effect_key))
    });
    held
}

/// One held obligation, under its key's lock, for an operator's decision.
fn held_obligation(
    store: &Store,
    effect_key: &str,
) -> Result<(PathBuf, String, KeyLock, Intent), Error> {
    let directory = directory(store);
    let name = encode(effect_key);
    let files = Files::new(&directory, &name);
    let lock = KeyLock::acquire(&files.lock, true)?.expect("a waiting acquire holds the lock");
    recover_staged(&directory, &name)?;
    match load(&files.pending)? {
        Some(intent) if intent.receipt.is_none() && intent.effect_key == effect_key => {
            Ok((directory, name, lock, intent))
        }
        Some(_) => Err(Error::Message(format!(
            "{effect_key} was delivered; it is being settled, not held"
        ))),
        None => Err(Error::Message(format!(
            "{effect_key} is not an unsettled outbound message"
        ))),
    }
}

/// Close a held obligation with the operator's `outcome` and the receipt it
/// ends with, keeping no body. Called with the key's lock held.
fn close_held(
    directory: &Path,
    name: &str,
    intent: &Intent,
    outcome: &str,
    receipt: Vec<Delivery>,
) -> Result<(), Error> {
    let done = Intent {
        effect_key: intent.effect_key.clone(),
        agent_id: intent.agent_id.clone(),
        destination: intent.destination.clone(),
        reply_to: intent.reply_to.clone(),
        body: None,
        receipt: Some(receipt),
        created_at_ms: intent.created_at_ms,
        uncertain: intent.uncertain.clone(),
        outcome: Some(outcome.to_string()),
    };
    let files = Files::new(directory, name);
    create_receipts(directory)?;
    write(&files.staged_done, &files.done, &done)?;
    std::fs::remove_file(&files.pending)?;
    sync_directory(directory)?;
    with_open(directory, |open| {
        open.keys.remove(name);
        open.stuck.remove(name);
    });
    Ok(())
}

/// The operator settles a held obligation without sending it: the message is
/// not sent again, and a keyed retry of the same send gets an empty receipt.
pub(crate) fn discard(store: &Store, effect_key: &str) -> Result<Unsettled, Error> {
    let _barrier = publication_barrier(store)?;
    let (directory, name, _lock, intent) = held_obligation(store, effect_key)?;
    close_held(&directory, &name, &intent, "discarded", Vec::new())?;
    Ok(Unsettled::of(&intent, None))
}

/// The operator sends a held obligation's message again, as its agent and to
/// its destination, through `resend`, which opens an obligation of its own.
/// The held one is closed with the new receipt only when that send succeeds;
/// otherwise it stays held, unchanged.
pub(crate) fn retry<F>(
    store: &Store,
    effect_key: &str,
    resend: F,
) -> Result<Vec<Delivery>, ApiError>
where
    F: FnOnce(&Unsettled) -> Result<Vec<Delivery>, ApiError>,
{
    let (directory, name, _lock, intent) =
        held_obligation(store, effect_key).map_err(ApiError::from_store)?;
    let held = Unsettled::of(&intent, None);
    let receipt = resend(&held)?;
    let _barrier = publication_barrier(store).map_err(ApiError::from_store)?;
    close_held(&directory, &name, &intent, "retried", receipt.clone())
        .map_err(ApiError::from_store)?;
    Ok(receipt)
}

/// The one directory listing a process makes. It returns the obligations a
/// previous process left open, which are registered at once, and the files it
/// left behind to tidy: lock files no one holds, staged writes no one
/// finishes, and receipts in the legacy flat layout. Listing reads names
/// only; [`tidy`] works the leftovers off in batches.
///
/// `Err` means the directory could not be read in full, so an open obligation
/// may be unregistered and the listing must run again.
fn list(directory: &Path) -> Result<(Vec<String>, Vec<String>), Error> {
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((Vec::new(), Vec::new()));
        }
        Err(error) => return Err(error.into()),
    };
    let mut pending = Vec::new();
    let mut leftovers = Vec::new();
    for entry in entries {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        if !entry.file_type()?.is_file() || file_name == ".publication.lock" {
            continue;
        }
        let keyed = |suffix: &str| file_name.strip_suffix(suffix).filter(|n| is_encoded(n));
        if let Some(name) = keyed(".pending") {
            pending.push(name.to_owned());
        } else if keyed(".lock").is_some()
            || keyed(".pending.tmp").is_some()
            || keyed(".done.tmp").is_some()
            || keyed(".done").is_some()
            || (file_name.starts_with('.') && file_name.ends_with(".tmp"))
        {
            leftovers.push(file_name.to_owned());
        }
    }
    Ok((pending, leftovers))
}

/// Tidy one batch of leftover files: unlink lock files no one holds, recover
/// staged writes, and move legacy receipts into `receipts/`. A key some
/// delivery holds is left to that delivery; files this module did not write
/// are untouched. A file that could not be tidied stays as it is and is
/// reported; it is not retried.
fn tidy(directory: &Path, batch: &[String]) -> Option<Error> {
    if batch.is_empty() {
        return None;
    }
    let receipts = match create_receipts(directory) {
        Ok(receipts) => receipts,
        Err(error) => return Some(error),
    };
    let mut first_error = None;
    let mut moved = false;
    for file_name in batch {
        let result = (|| -> Result<(), Error> {
            let keyed = |suffix: &str| file_name.strip_suffix(suffix).filter(|n| is_encoded(n));
            if let Some(name) = keyed(".lock") {
                // Acquiring and releasing an unheld lock unlinks its file.
                drop(KeyLock::acquire(&Files::new(directory, name).lock, false)?);
            } else if let Some(name) = keyed(".pending.tmp").or_else(|| keyed(".done.tmp")) {
                if let Some(_lock) = KeyLock::acquire(&Files::new(directory, name).lock, false)? {
                    recover_staged(directory, name)?;
                }
            } else if let Some(name) = keyed(".done") {
                if let Some(_lock) = KeyLock::acquire(&Files::new(directory, name).lock, false)? {
                    moved |= migrate_receipt(directory, name)?;
                }
            } else if file_name.starts_with('.') && file_name.ends_with(".tmp") {
                match recover_legacy_staged(directory, &directory.join(file_name)) {
                    Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
                    other => other?,
                }
            }
            Ok(())
        })();
        if let Err(error) = result {
            first_error.get_or_insert(error);
        }
    }
    if moved && let Err(error) = sync_directory(&receipts).and_then(|()| sync_directory(directory))
    {
        first_error.get_or_insert(error);
    }
    first_error
}

/// Move one flat-layout receipt into `receipts/`. A receipt already there for
/// the same key that differs keeps both; the flat one is read first.
fn migrate_receipt(directory: &Path, name: &str) -> Result<bool, Error> {
    let files = Files::new(directory, name);
    let read = |path: &Path| -> Result<Option<serde_json::Value>, Error> {
        match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(|error| {
                Error::Message(format!(
                    "invalid outbound receipt {}: {error}",
                    path.display()
                ))
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    };
    let Some(legacy) = read(&files.legacy_done)? else {
        return Ok(false);
    };
    match read(&files.done)? {
        None => std::fs::rename(&files.legacy_done, &files.done)?,
        Some(current) if current == legacy => std::fs::remove_file(&files.legacy_done)?,
        Some(_) => return Ok(false),
    }
    Ok(true)
}

/// A staged write of the flat layout's writer, named `.<random id>.tmp`. Only
/// a previous process wrote these. A complete one names its key and is
/// recovered under that key's lock; an empty or torn one was never committed.
fn recover_legacy_staged(directory: &Path, path: &Path) -> Result<(), Error> {
    let bytes = std::fs::read(path)?;
    let Ok(intent) = serde_json::from_slice::<Intent>(&bytes) else {
        return remove_if_present(path);
    };
    let name = encode(&intent.effect_key);
    let files = Files::new(directory, &name);
    match KeyLock::acquire(&files.lock, false)? {
        Some(_lock) => roll_forward_or_remove(&bytes, path, &files.pending),
        None => Ok(()),
    }
}

#[cfg(test)]
fn forget(store: &Store) {
    with_open(&directory(store), |open| *open = Open::default());
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

    fn names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    fn fixture() -> (tempfile::TempDir, Store, AgentId) {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let sender = store
            .create_agent(anyagents_core::NewAgent::new("sender", "opencode"))
            .unwrap();
        (dir, store, sender.id)
    }

    fn intent(sender: &AgentId, key: &str, receipt: Option<Vec<Delivery>>) -> Intent {
        Intent {
            effect_key: key.into(),
            agent_id: sender.clone(),
            destination: "agent:recipient".into(),
            reply_to: None,
            body: Some("hello".into()),
            receipt,
            created_at_ms: None,
            uncertain: None,
            outcome: None,
        }
    }

    fn notices(store: &Store, agent: &AgentId) -> Vec<String> {
        store
            .inbox(agent, None)
            .unwrap()
            .into_iter()
            .filter(|m| m.sender.as_deref() == Some(anyagents_core::SYSTEM_SENDER))
            .map(|m| m.text)
            .collect()
    }

    fn telegram_refusal(status: u16, class: &str) -> ApiError {
        ApiError::from_store(Error::Message(format!(
            "telegram {}",
            serde_json::json!({
                "method": "sendMessage", "channel_id": null, "class": class,
                "status": status, "error_code": status,
                "description": "Bad Request: message thread not found", "retry_after": null,
            })
        )))
    }

    fn put(path: &Path, intent: &Intent) {
        std::fs::write(path, serde_json::to_vec(intent).unwrap()).unwrap();
    }

    fn outbound_rows(store: &Store) -> i64 {
        rusqlite::Connection::open(store.data_dir().join("state.db"))
            .unwrap()
            .query_row("SELECT COUNT(*) FROM outbound_messages", [], |row| {
                row.get(0)
            })
            .unwrap()
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
            false,
            || {
                effects.set(effects.get() + 1);
                Ok(delivery())
            },
        );
        assert!(first.is_err());
        assert_eq!(effects.get(), 1);
        db.execute_batch("DROP TRIGGER fail_history;").unwrap();
        // Restart-style recovery settles history from the durable receipt only.
        forget(&store);
        reconcile(&store).unwrap();
        let retry = deliver(
            &store,
            &sender.id,
            "agent:recipient",
            None,
            "hello",
            Some("same-call"),
            false,
            || {
                effects.set(effects.get() + 1);
                Ok(delivery())
            },
        )
        .unwrap_or_else(|error| panic!("{}", error.message));
        assert!(retry[0].duplicate);
        assert_eq!(effects.get(), 1);
        let receipts = directory(&store).join("receipts");
        assert_eq!(names(&receipts).len(), 1);
        for entry in std::fs::read_dir(&receipts).unwrap().filter_map(Result::ok) {
            let retained: serde_json::Value =
                serde_json::from_slice(&std::fs::read(entry.path()).unwrap()).unwrap();
            assert!(
                retained["body"].is_null(),
                "completed receipts retain no body copy"
            );
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
                false,
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
                false,
                || {
                    Err(ApiError::from_store(Error::Message(
                        "lost remote acknowledgement".into(),
                    )))
                }
            )
            .is_err()
        );
        // Held, not an error on every pass: its sender is told once instead.
        reconcile(&store).unwrap();
        let held = unsettled(&store);
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].effect_key, "unknown-call");
        assert_eq!(
            held[0].error.as_deref(),
            Some("lost remote acknowledgement")
        );
        let told = notices(&store, &sender.id);
        assert_eq!(told.len(), 1, "{told:?}");
        assert!(told[0].starts_with("[Delivery Uncertain] Your message to agent:recipient"));
        assert!(told[0].contains("anyagents outbound retry unknown-call"));
        assert!(
            deliver(
                &store,
                &sender.id,
                "agent:recipient",
                None,
                "hello",
                Some("unknown-call"),
                false,
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
        // The retained obligation stays open across a restart, too, and
        // its sender is not told again.
        forget(&store);
        reconcile(&store).unwrap();
        assert_eq!(unsettled(&store).len(), 1);
        assert_eq!(notices(&store, &sender.id).len(), 1);
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
                false,
                || panic!("no durable obligation means no effect")
            )
            .is_err()
        );
        assert_eq!(
            std::fs::read(directory(&store)).unwrap(),
            b"blocked directory sentinel"
        );
    }

    /// Settled obligations leave a receipt and nothing else: no lock file and
    /// no temporary file. Once the startup scan ran and nothing is open, the
    /// per-minute reconcile touches no file at all.
    #[test]
    fn settled_obligations_leave_only_receipts_and_idle_reconcile_touches_nothing() {
        let (dir, store, sender) = fixture();
        for key in [Some("keyed-call"), None] {
            deliver(
                &store,
                &sender,
                "agent:recipient",
                None,
                "hello",
                key,
                false,
                || Ok(delivery()),
            )
            .unwrap_or_else(|error| panic!("{}", error.message));
        }
        let journal = directory(&store);
        assert_eq!(names(&journal), [".publication.lock", "receipts"]);
        assert_eq!(names(&journal.join("receipts")).len(), 2);
        reconcile(&store).unwrap();
        assert_eq!(names(&journal), [".publication.lock", "receipts"]);
        // Any file access would now fail: the journal path is a regular file.
        let parked = dir.path().join("parked");
        std::fs::rename(&journal, &parked).unwrap();
        std::fs::write(&journal, b"no directory here").unwrap();
        reconcile(&store).unwrap();
        std::fs::remove_file(&journal).unwrap();
        std::fs::rename(&parked, &journal).unwrap();
        // A keyed retry still reuses its receipt.
        let retry = deliver(
            &store,
            &sender,
            "agent:recipient",
            None,
            "hello",
            Some("keyed-call"),
            false,
            || panic!("a settled keyed effect is never repeated"),
        )
        .unwrap_or_else(|error| panic!("{}", error.message));
        assert!(retry[0].duplicate);
        assert_eq!(outbound_rows(&store), 2);
    }

    /// The first reconcile after a restart moves the flat layout's receipts
    /// into `receipts/`, unlinks lock files no one holds, removes staged writes
    /// that were never committed, and settles acknowledged obligations. Every
    /// receipt stays readable and keyed retries still reuse them.
    #[test]
    fn first_scan_migrates_the_flat_layout_without_losing_a_receipt() {
        let (_dir, store, sender) = fixture();
        let journal = directory(&store);
        std::fs::create_dir_all(journal.join("abandoned-20261113")).unwrap();
        std::fs::write(journal.join("abandoned-20261113/note.txt"), "kept").unwrap();
        std::fs::write(journal.join("README"), "kept").unwrap();
        std::fs::write(journal.join(".publication.lock"), b"").unwrap();
        let settled = encode("send:old:0");
        let mut done = intent(&sender, "send:old:0", Some(delivery()));
        done.body = None;
        put(&journal.join(format!("{settled}.done")), &done);
        std::fs::write(journal.join(format!("{settled}.lock")), b"").unwrap();
        let acknowledged = encode("send:acked:0");
        put(
            &journal.join(format!("{acknowledged}.pending")),
            &intent(&sender, "send:acked:0", Some(delivery())),
        );
        std::fs::write(journal.join(format!("{acknowledged}.lock")), b"").unwrap();
        for torn in [&b""[..], b"{\"effect_key\":\"send:"] {
            std::fs::write(
                journal.join(format!(".{}.tmp", MessageId::generate())),
                torn,
            )
            .unwrap();
        }
        forget(&store);
        reconcile(&store).unwrap();
        assert_eq!(
            names(&journal),
            [
                ".publication.lock",
                "README",
                "abandoned-20261113",
                "receipts"
            ]
        );
        assert_eq!(
            names(&journal.join("receipts")),
            [format!("{acknowledged}.done"), format!("{settled}.done")]
        );
        assert_eq!(names(&journal.join("abandoned-20261113")), ["note.txt"]);
        assert_eq!(outbound_rows(&store), 1, "the acknowledged effect settled");
        for key in ["send:old:0", "send:acked:0"] {
            let retry = deliver(
                &store,
                &sender,
                "agent:recipient",
                None,
                "hello",
                Some(key),
                false,
                || panic!("a settled keyed effect is never repeated"),
            )
            .unwrap_or_else(|error| panic!("{}", error.message));
            assert!(retry[0].duplicate);
        }
        assert_eq!(
            names(&journal),
            [
                ".publication.lock",
                "README",
                "abandoned-20261113",
                "receipts"
            ]
        );
    }

    /// The listing registers every open obligation at once, but a reconcile
    /// tidies at most `TIDY_BATCH` leftovers, so migrating a large flat
    /// journal never holds one maintenance pass (and the publication barrier)
    /// for the whole directory. A receipt still waiting in the flat layout is
    /// read there, so a keyed retry reuses it mid-migration.
    #[test]
    fn the_first_scan_tidies_leftovers_in_bounded_batches() {
        let (_dir, store, sender) = fixture();
        let journal = directory(&store);
        std::fs::create_dir_all(&journal).unwrap();
        let total = 2 * TIDY_BATCH + 5;
        let key = |n: usize| format!("send:old-{n}:0");
        for n in 0..total {
            let mut done = intent(&sender, &key(n), Some(delivery()));
            done.body = None;
            put(&journal.join(format!("{}.done", encode(&key(n)))), &done);
        }
        let acknowledged = encode("send:acked:0");
        put(
            &journal.join(format!("{acknowledged}.pending")),
            &intent(&sender, "send:acked:0", Some(delivery())),
        );
        let flat = |journal: &Path| {
            names(journal)
                .into_iter()
                .filter(|name| name.ends_with(".done"))
                .count()
        };
        forget(&store);
        reconcile(&store).unwrap();
        assert_eq!(
            outbound_rows(&store),
            1,
            "the open obligation settled at once"
        );
        assert_eq!(flat(&journal), total - TIDY_BATCH);
        assert_eq!(names(&journal.join("receipts")).len(), TIDY_BATCH + 1);
        let waiting = (0..total)
            .find(|n| journal.join(format!("{}.done", encode(&key(*n)))).exists())
            .unwrap();
        let retry = deliver(
            &store,
            &sender,
            "agent:recipient",
            None,
            "hello",
            Some(&key(waiting)),
            false,
            || panic!("a settled keyed effect is never repeated"),
        )
        .unwrap_or_else(|error| panic!("{}", error.message));
        assert!(retry[0].duplicate);
        reconcile(&store).unwrap();
        assert_eq!(flat(&journal), total - 2 * TIDY_BATCH);
        reconcile(&store).unwrap();
        assert_eq!(flat(&journal), 0);
        assert_eq!(names(&journal.join("receipts")).len(), total + 1);
        assert_eq!(names(&journal), [".publication.lock", "receipts"]);
    }

    /// A file the first scan cannot tidy stays in place and is reported once;
    /// the scan is complete and later reconciles do not repeat it.
    #[test]
    fn a_file_the_scan_cannot_tidy_is_reported_once() {
        let (dir, store, _sender) = fixture();
        let journal = directory(&store);
        std::fs::create_dir_all(&journal).unwrap();
        let malformed = journal.join(format!("{}.done", encode("send:torn:0")));
        std::fs::write(&malformed, b"{\"effect_key\"").unwrap();
        forget(&store);
        assert!(reconcile(&store).is_err());
        assert_eq!(std::fs::read(&malformed).unwrap(), b"{\"effect_key\"");
        let parked = dir.path().join("parked");
        std::fs::rename(&journal, &parked).unwrap();
        std::fs::write(&journal, b"no directory here").unwrap();
        reconcile(&store).unwrap();
    }

    /// A process that died after staging an acknowledged receipt but before
    /// committing it leaves the receipt in the staged copy: recovery rolls it
    /// forward instead of leaving the outcome unknown, and never repeats the
    /// effect. A staged copy without a receipt is simply removed.
    #[test]
    fn staged_receipt_of_a_dead_writer_is_rolled_forward() {
        let (_dir, store, sender) = fixture();
        let journal = directory(&store);
        std::fs::create_dir_all(&journal).unwrap();
        let acked = encode("send:acked:0");
        put(
            &journal.join(format!("{acked}.pending")),
            &intent(&sender, "send:acked:0", None),
        );
        put(
            &journal.join(format!("{acked}.pending.tmp")),
            &intent(&sender, "send:acked:0", Some(delivery())),
        );
        let unknown = encode("send:unknown:0");
        put(
            &journal.join(format!("{unknown}.pending")),
            &intent(&sender, "send:unknown:0", None),
        );
        put(
            &journal.join(format!("{unknown}.pending.tmp")),
            &intent(&sender, "send:unknown:0", None),
        );
        forget(&store);
        reconcile(&store).unwrap();
        assert_eq!(
            unsettled(&store)
                .into_iter()
                .map(|held| held.effect_key)
                .collect::<Vec<_>>(),
            ["send:unknown:0"],
            "the obligation without a receipt stays open, held"
        );
        assert_eq!(
            names(&journal),
            [
                ".publication.lock".to_string(),
                format!("{unknown}.pending"),
                "receipts".to_string()
            ]
        );
        assert_eq!(outbound_rows(&store), 1);
        let retry = deliver(
            &store,
            &sender,
            "agent:recipient",
            None,
            "hello",
            Some("send:acked:0"),
            false,
            || panic!("an acknowledged effect is never repeated"),
        )
        .unwrap_or_else(|error| panic!("{}", error.message));
        assert!(retry[0].duplicate);
    }

    /// A write that fails after staging removes its staged copy, and only its
    /// own. When settlement fails after the effect, the receipt survives in
    /// the obligation, nothing temporary is left, and reconcile settles it.
    #[test]
    fn a_failed_write_leaves_no_temporary_file() {
        let (dir, store, sender) = fixture();
        let staged = dir.path().join("key.pending.tmp");
        let unreachable = dir.path().join("missing/key.done");
        let record = intent(&sender, "key", Some(delivery()));
        assert!(write(&staged, &unreachable, &record).is_err());
        assert!(!staged.exists(), "the failed write removed its staged copy");
        std::fs::write(&staged, b"not this write's").unwrap();
        assert!(write(&staged, &dir.path().join("key.pending"), &record).is_err());
        assert_eq!(std::fs::read(&staged).unwrap(), b"not this write's");

        let journal = directory(&store);
        std::fs::create_dir_all(&journal).unwrap();
        // Settlement cannot make `receipts/` through a dangling link.
        std::os::unix::fs::symlink(dir.path().join("nowhere"), journal.join("receipts")).unwrap();
        let effects = Cell::new(0);
        assert!(
            deliver(
                &store,
                &sender,
                "agent:recipient",
                None,
                "hello",
                Some("blocked-call"),
                false,
                || {
                    effects.set(effects.get() + 1);
                    Ok(delivery())
                },
            )
            .is_err()
        );
        let pending = encode("blocked-call");
        assert_eq!(
            names(&journal),
            [
                ".publication.lock".to_string(),
                format!("{pending}.pending"),
                "receipts".to_string()
            ]
        );
        std::fs::remove_file(journal.join("receipts")).unwrap();
        reconcile(&store).unwrap();
        assert_eq!(names(&journal), [".publication.lock", "receipts"]);
        assert_eq!(effects.get(), 1);
        assert_eq!(outbound_rows(&store), 1);
        let retry = deliver(
            &store,
            &sender,
            "agent:recipient",
            None,
            "hello",
            Some("blocked-call"),
            false,
            || panic!("an acknowledged effect is never repeated"),
        )
        .unwrap_or_else(|error| panic!("{}", error.message));
        assert!(retry[0].duplicate);
    }

    /// A waiter that locked an inode its holder unlinked retries on the
    /// current file, so it never holds the key beside a later acquirer.
    #[test]
    fn a_key_lock_is_exclusive_although_its_file_is_unlinked_on_release() {
        let dir = crate::test_tempdir();
        let path = dir.path().join("key.lock");
        let holder = KeyLock::acquire(&path, true).unwrap().unwrap();
        let (acquired, observed) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel::<()>();
        let waiter_path = path.clone();
        let waiter = std::thread::spawn(move || {
            let lock = KeyLock::acquire(&waiter_path, true).unwrap().unwrap();
            acquired.send(()).unwrap();
            released.recv().unwrap();
            drop(lock);
        });
        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(holder);
        observed.recv().unwrap();
        assert!(
            KeyLock::acquire(&path, false).unwrap().is_none(),
            "the waiter holds the lock on the current file"
        );
        release.send(()).unwrap();
        waiter.join().unwrap();
        assert!(!path.exists(), "no lock file outlives its holder");
        assert!(KeyLock::acquire(&path, false).unwrap().is_some());
    }

    /// The Telegram API refused the whole post: nothing was posted, so no
    /// obligation is left, and the same call later runs its effect again.
    #[test]
    fn a_refused_post_leaves_no_obligation() {
        let (_dir, store, sender) = fixture();
        let effects = Cell::new(0);
        let send = || {
            deliver(
                &store,
                &sender,
                "telegram:chat:1951959493:3662",
                None,
                "a reply",
                Some("refused-call"),
                false,
                || {
                    effects.set(effects.get() + 1);
                    Err(telegram_refusal(400, "provider"))
                },
            )
        };
        assert!(send().is_err());
        assert!(send().is_err(), "a refusal is never 'outcome unknown'");
        assert_eq!(effects.get(), 2);
        reconcile(&store).unwrap();
        assert!(unsettled(&store).is_empty());
        assert!(notices(&store, &sender).is_empty());
        assert_eq!(names(&directory(&store)), [".publication.lock"]);
    }

    /// A mailbox write dedupes a replay by its source key, so a failed one
    /// leaves nothing to settle either.
    #[test]
    fn a_failed_replay_safe_send_leaves_no_obligation() {
        let (_dir, store, sender) = fixture();
        assert!(
            deliver(
                &store,
                &sender,
                "agent:recipient",
                None,
                "hello",
                None,
                true,
                || Err(ApiError::from_store(Error::AgentNotFound(
                    "recipient".into()
                ))),
            )
            .is_err()
        );
        reconcile(&store).unwrap();
        assert!(unsettled(&store).is_empty());
        assert_eq!(names(&directory(&store)), [".publication.lock"]);
    }

    /// A timeout, or a later chunk of a post refused after earlier ones were
    /// posted, proves nothing: held, never resent, its sender told once.
    #[test]
    fn an_unconfirmed_post_is_held_and_its_sender_told_once() {
        let (_dir, store, sender) = fixture();
        for error in [
            telegram_refusal(0, "timeout"),
            telegram_refusal(400, "partial_delivery"),
            telegram_refusal(502, "provider"),
        ] {
            assert!(
                deliver(
                    &store,
                    &sender,
                    "telegram:chat:1951959493",
                    None,
                    "a reply",
                    None,
                    false,
                    || Err(error),
                )
                .is_err()
            );
        }
        reconcile(&store).unwrap();
        let held = unsettled(&store);
        assert_eq!(held.len(), 3, "{held:?}");
        assert!(
            held.iter()
                .all(|h| h.destination == "telegram:chat:1951959493")
        );
        assert!(held.iter().all(|h| h.since_ms.is_some()));
        assert!(held.iter().all(|h| {
            h.error
                .as_deref()
                .is_some_and(|e| e.starts_with("telegram "))
        }));
        assert_eq!(notices(&store, &sender).len(), 3);
        reconcile(&store).unwrap();
        forget(&store);
        reconcile(&store).unwrap();
        assert_eq!(notices(&store, &sender).len(), 3, "told once each");
    }

    /// An obligation a binary from before this layout left (no time, no
    /// error) is held all the same, aged by its file.
    #[test]
    fn an_old_obligation_without_a_receipt_is_held() {
        let (_dir, store, sender) = fixture();
        let journal = directory(&store);
        std::fs::create_dir_all(&journal).unwrap();
        let name = encode("msg_01m7b8rp7ve8n9s9cns8z8fafp");
        std::fs::write(
            journal.join(format!("{name}.pending")),
            serde_json::json!({
                "effect_key": "msg_01m7b8rp7ve8n9s9cns8z8fafp",
                "agent_id": sender.to_string(),
                "destination": "telegram:chat:1951959493:3662",
                "reply_to": "telegram:chat:1951959493:3662",
                "body": "a reply the human never got",
                "receipt": null,
            })
            .to_string(),
        )
        .unwrap();
        forget(&store);
        let held = unsettled(&store);
        assert_eq!(held.len(), 1);
        assert!(held[0].since_ms.is_some());
        assert!(held[0].error.is_none());
        assert!(notices(&store, &sender).is_empty(), "a read tells no one");
        reconcile(&store).unwrap();
        let told = notices(&store, &sender);
        assert_eq!(told.len(), 1);
        assert!(told[0].contains("telegram:chat:1951959493:3662"));
        assert!(told[0].contains("a reply the human never got"));
    }

    /// The operator discards a held obligation: it is closed without being
    /// sent, and nothing about it is held or retried any more.
    #[test]
    fn the_operator_discards_a_held_obligation() {
        let (_dir, store, sender) = fixture();
        assert!(
            deliver(
                &store,
                &sender,
                "telegram:chat:1",
                None,
                "hello",
                Some("held-call"),
                false,
                || Err(telegram_refusal(0, "timeout")),
            )
            .is_err()
        );
        reconcile(&store).unwrap();
        let discarded = discard(&store, "held-call").unwrap();
        assert_eq!(discarded.destination, "telegram:chat:1");
        assert!(unsettled(&store).is_empty());
        assert!(discard(&store, "held-call").is_err(), "settled once");
        let receipts = directory(&store).join("receipts");
        let done: serde_json::Value = serde_json::from_slice(
            &std::fs::read(receipts.join(format!("{}.done", encode("held-call")))).unwrap(),
        )
        .unwrap();
        assert_eq!(done["outcome"], "discarded");
        assert!(done["body"].is_null());
        assert!(
            !directory(&store)
                .join(format!("{}.pending", encode("held-call")))
                .exists()
        );
    }

    /// The operator retries a held obligation through a new send; it is
    /// closed with that send's receipt only when the send succeeds.
    #[test]
    fn the_operator_retries_a_held_obligation() {
        let (_dir, store, sender) = fixture();
        assert!(
            deliver(
                &store,
                &sender,
                "telegram:chat:1",
                None,
                "hello",
                Some("held-call"),
                false,
                || Err(telegram_refusal(0, "timeout")),
            )
            .is_err()
        );
        let failed = retry(&store, "held-call", |held| {
            assert_eq!(held.body, "hello");
            Err(ApiError::from_store(Error::Message("still down".into())))
        });
        assert!(failed.is_err());
        assert_eq!(unsettled(&store).len(), 1, "a failed retry keeps it held");
        let receipt = retry(&store, "held-call", |held| {
            assert_eq!(held.agent_id, sender);
            Ok(delivery())
        })
        .unwrap_or_else(|error| panic!("{}", error.message));
        assert_eq!(receipt.len(), 1);
        assert_eq!(receipt[0].id, "provider-receipt");
        assert!(unsettled(&store).is_empty());
        let done: serde_json::Value = serde_json::from_slice(
            &std::fs::read(
                directory(&store)
                    .join("receipts")
                    .join(format!("{}.done", encode("held-call"))),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(done["outcome"], "retried");
        assert!(retry(&store, "held-call", |_| Ok(delivery())).is_err());
    }
}
