//! Host-only file retrieval. All paths are derived from authorized registry IDs;
//! active native JSONL is read in place and never handed off or rewritten here.
use crate::{
    Agent, AgentId, HistorySearchQuery, Store,
    host_cursor::{FileStamp, SourceCursor},
    host_index::{HostIndex, IndexError, IndexHit, IndexMetadata, MemoryScope},
    host_native_metadata::{Envelope, EventHeader, MetadataReader},
    host_sqlite::{RowRef, StoreKind},
    learned_memory::sha256,
    raw_segment::{RawSource, open_readonly, read_raw},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs, io,
    path::{Path, PathBuf},
};

pub const INDEX_FILE: &str = "host-history-index-v1.db";

/// A raw byte range another retriever cites inside an indexed document:
/// absolute offsets in the document's segment and the range's SHA-256.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CitedRange {
    pub document_id: String,
    pub offset: u64,
    pub length: u64,
    pub sha256: String,
}

/// ID and expected digest come from a search result; offsets are event-relative.
pub struct HostRawRead<'a> {
    pub id: &'a str,
    pub sha256: &'a str,
    pub offset: u64,
    pub length: u64,
}

/// The registry facts history indexing reads from an agent row.
struct Owner {
    id: AgentId,
    name: String,
}
impl From<&Agent> for Owner {
    fn from(agent: &Agent) -> Self {
        Self {
            id: agent.id.clone(),
            name: agent.name.clone(),
        }
    }
}

fn store_error(error: crate::Error) -> IndexError {
    io::Error::other(error.to_string()).into()
}

impl Store {
    pub fn is_host_path_redacted(&self, owner: &str, path: &Path) -> Result<bool, IndexError> {
        let root = self.data_dir().join("agents").join(owner);
        if !safe_components(&mut Walk::new(&root), self.data_dir(), &root)? {
            return Ok(true);
        }
        Ok(self
            .unresolved_host_paths(owner)?
            .iter()
            .any(|p| path.starts_with(p)))
    }

    fn unresolved_host_paths(&self, owner: &str) -> Result<Vec<PathBuf>, IndexError> {
        let mut paths = Vec::new();
        for (_, receipt) in self.redaction_receipts(Some(owner)).map_err(store_error)? {
            paths.extend(unscrubbed_paths(&receipt)?);
        }
        Ok(paths)
    }

    /// Pending redaction exclusions of the listed agents, read once per search.
    fn unresolved_host_paths_of(
        &self,
        owners: &[Owner],
    ) -> Result<HashMap<String, Vec<PathBuf>>, IndexError> {
        let wanted: HashSet<&str> = owners.iter().map(|o| o.id.as_str()).collect();
        let mut paths: HashMap<String, Vec<PathBuf>> = HashMap::new();
        for (owner, receipt) in self.redaction_receipts(None).map_err(store_error)? {
            if wanted.contains(owner.as_str()) {
                let unscrubbed = unscrubbed_paths(&receipt)?;
                paths.entry(owner).or_default().extend(unscrubbed);
            }
        }
        Ok(paths)
    }

    /// Low-priority daemon maintenance: at most one due native source per call.
    /// Failures advance only attempt ordering, so one bad source cannot starve
    /// others; frontier/documents remain the previous atomic publication.
    pub fn verify_one_host_history_source(&self, now_ms: i64) -> Result<bool, IndexError> {
        let index_path = self.data_dir().join(INDEX_FILE);
        if !index_path.exists() {
            return Ok(false);
        }
        let index = HostIndex::shared(&index_path)?;
        let Some((owner, path)) = index.verification_candidate(now_ms)? else {
            return Ok(false);
        };
        let Some((id, name)) = self
            .agent_names([owner.as_str()])
            .map_err(store_error)?
            .pop()
        else {
            index.clear_source(&owner, &path)?;
            index.checkpoint()?;
            return Ok(true);
        };
        let agent = Owner { id, name };
        let Some(mut cursor) = index.source_cursor(&owner, &path)? else {
            return Ok(false);
        };
        let root = self.data_dir().join("agents").join(agent.id.as_str());
        let mut walk = Walk::new(&root);
        let found = safe_components(&mut walk, self.data_dir(), &root)?
            && native_sources(&mut walk, &root)?
                .iter()
                .any(|s| s.path == path)
            && !self
                .unresolved_host_paths(&owner)?
                .iter()
                .any(|p| path.starts_with(p));
        if !found {
            index.clear_source(&owner, &path)?;
            index.checkpoint()?;
            return Ok(true);
        }
        // Maintenance neither reads the source nor waits for a lifecycle: the
        // same worker runs the WAL checkpoint and outbound settlement. It
        // records the attempt, so the rotation moves on, and hands the source
        // to the background indexer, which reads it in bounded batches with
        // no lock held and records the verification when it has read it all.
        cursor.verification_attempt_ms = now_ms;
        index.save_source(&owner, &path, &cursor)?;
        enqueue_job(
            self.data_dir(),
            Job::Verify(agent.id.to_string(), path, now_ms),
        );
        Ok(true)
    }

    /// Read a bounded range from an already-indexed document, never a supplied
    /// path/coordinate. Resolve ACL before file access, then repeat lookup under
    /// its owner lifecycle lock so redaction cannot race authorization/readback.
    pub fn read_host_file_range(
        &self,
        authorized: &[String],
        scope: MemoryScope,
        selected: Option<&str>,
        request: HostRawRead<'_>,
    ) -> Result<Option<(IndexHit, Vec<u8>)>, IndexError> {
        let selected = selected
            .map(str::trim)
            .filter(|s| *s != "*" && !s.eq_ignore_ascii_case("all"))
            .map(|s| self.agent_by_name(s).map(|a| a.id.to_string()))
            .transpose()
            .map_err(store_error)?;
        let visible: Vec<String> = authorized
            .iter()
            .filter(|id| selected.as_ref().is_none_or(|s| s == *id))
            .cloned()
            .collect();
        if visible.is_empty() {
            return Ok(None);
        }
        let index_path = self.data_dir().join(INDEX_FILE);
        if !index_path.exists() {
            return Ok(None);
        }
        let index = HostIndex::shared(&index_path)?;
        let Some(hit) = index.document(&visible, scope, request.id)? else {
            return Ok(None);
        };
        let agent = self
            .agent_by_name(&hit.metadata.agent_id)
            .map_err(store_error)?;
        let _guard = self.lock_agent_lifecycle(&agent.id).map_err(store_error)?;
        let Some(mut hit) = index.document(
            std::slice::from_ref(&agent.id.to_string()),
            scope,
            request.id,
        )?
        else {
            return Ok(None);
        };
        if hit.raw.sha256 != request.sha256 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "raw document changed since search",
            )
            .into());
        }
        let root = self.data_dir().join("agents").join(agent.id.as_str());
        let mut walk = Walk::new(&root);
        if !safe_components(&mut walk, self.data_dir(), &root)?
            || self
                .unresolved_host_paths(agent.id.as_str())?
                .iter()
                .any(|p| hit.raw.segment.starts_with(p))
        {
            return Ok(None);
        }
        let paths = match hit.metadata.source {
            MemoryScope::Memory => regular_files(&mut walk, &root.join("memory"), true)?
                .into_iter()
                .filter(|p| !is_hidden_memory_file(p))
                .collect(),
            MemoryScope::Transcripts => {
                let mut paths: Vec<PathBuf> = native_sources(&mut walk, &root)?
                    .into_iter()
                    .map(|s| s.path)
                    .collect();
                paths.extend(history_archives(&mut walk, &root)?);
                paths
            }
            MemoryScope::All => return Ok(None),
        };
        // A row is readable while its store is one the walk still finds.
        if !paths.contains(&crate::host_sqlite::source_of(&hit.raw.segment)) {
            return Ok(None);
        }
        let (window, bytes) =
            crate::raw_segment::read_raw_range(&hit.raw, request.offset, request.length)?;
        hit.window = window;
        Ok(Some((hit, bytes)))
    }

    /// Immediately synchronize an agent's `memory/` directory into `HostIndex`
    /// so newly upserted, read, or
    /// deleted topics are immediately readable via `raw_id` and `search_host_files`.
    /// The caller holds the agent's lifecycle lock, so publishing needs no
    /// other; learned-memory topics are small whole files.
    pub fn sync_agent_memory_index(&self, agent: &Agent) -> Result<(), IndexError> {
        let mut index = HostIndex::shared(&self.data_dir().join(INDEX_FILE))?;
        let unresolved = self.unresolved_host_paths(agent.id.as_str())?;
        let mut budget = u64::MAX;
        let mut held =
            || -> Result<Option<Box<dyn std::any::Any>>, IndexError> { Ok(Some(Box::new(()))) };
        self.refresh_class(
            &mut index,
            &Owner::from(agent),
            MemoryScope::Memory,
            &unresolved,
            agents_identity(self.data_dir())?,
            crate::now_ms(),
            &mut budget,
            &mut held,
        )?;
        Ok(())
    }

    /// The caller has already resolved the authorized immutable ID set.
    /// Indexing reads with no lock held; publishing each batch and reading
    /// back each hit hold the owner's lifecycle lock, which serializes them
    /// against redaction, rename and lifecycle changes. At most
    /// [`REQUEST_INDEX_BYTES`] of new history is indexed before answering.
    pub fn search_host_files(
        &self,
        authorized: &[String],
        scope: MemoryScope,
        query: &HistorySearchQuery,
    ) -> Result<Vec<(IndexHit, Vec<u8>)>, IndexError> {
        self.search_host_files_at(
            authorized,
            scope,
            query,
            crate::now_ms(),
            REQUEST_INDEX_BYTES,
        )
        .map(|(hits, _)| hits)
    }

    /// [`Store::search_host_files`] that also says what history it could not
    /// index before answering: at most [`REQUEST_INDEX_BYTES`] of new history
    /// is indexed on the request path; classes with more are queued for the
    /// background indexer, and their unindexed history is missing from these
    /// hits.
    pub fn search_host_files_reporting(
        &self,
        authorized: &[String],
        scope: MemoryScope,
        query: &HistorySearchQuery,
    ) -> Result<(HostHits, PendingHistory), IndexError> {
        self.search_host_files_at(
            authorized,
            scope,
            query,
            crate::now_ms(),
            REQUEST_INDEX_BYTES,
        )
    }

    fn search_host_files_at(
        &self,
        authorized: &[String],
        scope: MemoryScope,
        query: &HistorySearchQuery,
        now_ms: i64,
        budget: u64,
    ) -> Result<(HostHits, PendingHistory), IndexError> {
        let selected = query
            .agent
            .as_deref()
            .map(str::trim)
            .filter(|s| *s != "*" && !s.eq_ignore_ascii_case("all"))
            .map(|s| self.agent_by_name(s).map(|a| a.id))
            .transpose()
            .map_err(store_error)?;
        let authorized: HashSet<&str> = authorized.iter().map(String::as_str).collect();
        let mut owners: Vec<Owner> = self
            .agent_names(
                authorized
                    .into_iter()
                    .filter(|id| selected.as_ref().is_none_or(|s| s.as_str() == *id)),
            )
            .map_err(store_error)?
            .into_iter()
            .map(|(id, name)| Owner { id, name })
            .collect();
        owners.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str()));
        let mut index = HostIndex::shared(&self.data_dir().join(INDEX_FILE))?;
        let unresolved = self.unresolved_host_paths_of(&owners)?;
        let agents = agents_identity(self.data_dir())?;
        let mut remaining = budget;
        let mut pending = PendingHistory::default();
        for owner in &owners {
            let unresolved = unresolved
                .get(owner.id.as_str())
                .map_or(&[][..], Vec::as_slice);
            for &class in classes(scope) {
                let refresh = self.refresh_class_if_changed(
                    &mut index,
                    owner,
                    class,
                    unresolved,
                    agents,
                    now_ms,
                    &mut remaining,
                )?;
                if !refresh.complete {
                    pending.sources += refresh.pending_sources.max(1);
                    pending.bytes += refresh.pending_bytes;
                    enqueue(self.data_dir(), owner.id.as_str(), class);
                }
            }
        }
        let visible: Vec<_> = owners.iter().map(|a| a.id.to_string()).collect();
        let mut request = query.clone();
        request.agent = None;
        let hits = index.search(&visible, scope, &request)?;
        let mut out = Vec::with_capacity(hits.len());
        for hit in hits {
            let owner_id = crate::AgentId::parse(&hit.metadata.agent_id).map_err(store_error)?;
            let _guard = self.lock_agent_lifecycle(&owner_id).map_err(store_error)?;
            let Some(current) =
                index.document(std::slice::from_ref(&hit.metadata.agent_id), scope, &hit.id)?
            else {
                continue;
            };
            if current.raw.sha256 != hit.raw.sha256
                || self.is_host_path_redacted(&hit.metadata.agent_id, &hit.window.segment)?
            {
                continue;
            }
            // Return the matching bounded window, not an unbounded tool payload.
            // The whole-event coordinate remains available for explicit readback.
            let bytes = read_raw(&hit.window)?;
            out.push((hit, bytes));
        }
        Ok((out, pending))
    }

    /// Raw ranges another retriever cited (a dense index, for example),
    /// resolved exactly as lexical hits are: only documents of the authorized
    /// owners and scope that still match every structured filter of `query`,
    /// with the cited range inside its document, re-checked under the owner's
    /// lifecycle lock and redaction exclusions, and read back against the
    /// range's own digest. A candidate failing any check is dropped. The
    /// result keeps the cited order and returns the cited range as `window`.
    pub fn resolve_host_ranges(
        &self,
        authorized: &[String],
        scope: MemoryScope,
        query: &HistorySearchQuery,
        cited: &[CitedRange],
    ) -> Result<Vec<(IndexHit, Vec<u8>)>, IndexError> {
        let index_path = self.data_dir().join(INDEX_FILE);
        if cited.is_empty() || !index_path.exists() {
            return Ok(Vec::new());
        }
        let selected = query
            .agent
            .as_deref()
            .map(str::trim)
            .filter(|s| *s != "*" && !s.eq_ignore_ascii_case("all"))
            .map(|s| self.agent_by_name(s).map(|a| a.id.to_string()))
            .transpose()
            .map_err(store_error)?;
        let visible: Vec<String> = authorized
            .iter()
            .filter(|id| selected.as_ref().is_none_or(|s| s == *id))
            .cloned()
            .collect();
        let index = HostIndex::shared(&index_path)?;
        let ids: Vec<String> = cited.iter().map(|c| c.document_id.clone()).collect();
        let mut request = query.clone();
        request.agent = None;
        let allowed: HashMap<String, IndexHit> = index
            .filter_documents(&visible, scope, &request, &ids)?
            .into_iter()
            .map(|hit| (hit.id.clone(), hit))
            .collect();
        let mut out = Vec::new();
        for range in cited {
            let Some(hit) = allowed.get(&range.document_id) else {
                continue;
            };
            let end = range.offset.checked_add(range.length);
            if range.length == 0
                || range.offset < hit.raw.offset
                || end.is_none_or(|end| end > hit.raw.offset + hit.raw.length)
            {
                continue;
            }
            let owner = crate::AgentId::parse(&hit.metadata.agent_id).map_err(store_error)?;
            let _guard = self.lock_agent_lifecycle(&owner).map_err(store_error)?;
            let Some(current) =
                index.document(std::slice::from_ref(&hit.metadata.agent_id), scope, &hit.id)?
            else {
                continue;
            };
            if current.raw.sha256 != hit.raw.sha256
                || self.is_host_path_redacted(&hit.metadata.agent_id, &hit.raw.segment)?
            {
                continue;
            }
            let window = crate::raw_segment::RawCoordinate {
                segment: hit.raw.segment.clone(),
                offset: range.offset,
                length: range.length,
                sha256: range.sha256.clone(),
                seq: hit.raw.seq,
            };
            let Ok(bytes) = read_raw(&window) else {
                continue;
            };
            let mut hit = hit.clone();
            hit.window = window;
            hit.lexical_score = 0.0;
            out.push((hit, bytes));
        }
        Ok(out)
    }

    /// A digest of the owner's redaction receipts: a refresh that read its
    /// history at one value publishes only while it still holds.
    fn redaction_epoch(&self, owner: &str) -> Result<String, IndexError> {
        let mut receipts: Vec<String> = self
            .redaction_receipts(Some(owner))
            .map_err(store_error)?
            .into_iter()
            .map(|(_, receipt)| receipt)
            .collect();
        receipts.sort();
        Ok(sha256(receipts.join("\n").as_bytes()))
    }

    /// Skip a class whose recorded walk still matches the filesystem and the
    /// policy: it has nothing new to index, so it costs a stat per recorded
    /// entry, with no lock, no directory listing, no file open and no query
    /// besides its own record. Otherwise refresh it within `budget` bytes of
    /// new history. Walking, reading and projecting hold no lock; each
    /// published batch holds the owner's lifecycle lock only while it
    /// publishes, and is dropped if a redaction happened since it was read.
    /// The walk is recorded only once every source is complete.
    #[allow(clippy::too_many_arguments)]
    fn refresh_class_if_changed(
        &self,
        index: &mut HostIndex,
        owner: &Owner,
        class: MemoryScope,
        unresolved: &[PathBuf],
        agents: Option<(u64, u64)>,
        now_ms: i64,
        budget: &mut u64,
    ) -> Result<ClassRefresh, IndexError> {
        let class_name = class
            .source()
            .ok_or_else(|| io::Error::other("a history class is memory or transcripts"))?;
        let root = self.data_dir().join("agents").join(owner.id.as_str());
        if let Some((recorded, tree)) = index.agent_tree(owner.id.as_str(), class_name)?
            && recorded == tree_policy(owner, unresolved)?
            && serde_json::from_str::<Tree>(&tree).is_ok_and(|t| t.is_current(&root, agents))
        {
            return Ok(ClassRefresh::complete());
        }
        #[cfg(test)]
        tests::CLASS_WALKS.with(|n| n.set(n.get() + 1));
        let epoch = self.redaction_epoch(owner.id.as_str())?;
        let unresolved = self.unresolved_host_paths(owner.id.as_str())?;
        let policy = tree_policy(owner, &unresolved)?;
        let mut gate = || -> Result<Option<Box<dyn std::any::Any>>, IndexError> {
            let guard = self.lock_agent_lifecycle(&owner.id).map_err(store_error)?;
            if self.redaction_epoch(owner.id.as_str())? != epoch {
                return Ok(None);
            }
            Ok(Some(Box::new(guard)))
        };
        let (walk, refresh) = self.refresh_class(
            index,
            owner,
            class,
            &unresolved,
            agents,
            now_ms,
            budget,
            &mut gate,
        )?;
        if refresh.complete {
            let tree = Tree {
                started_ms: now_ms,
                agents,
                entries: walk.entries,
            };
            // A path that cannot be recorded (not UTF-8) only costs a walk later.
            if let Ok(tree) = serde_json::to_string(&tree)
                && let Some(_guard) = gate()?
            {
                index.save_agent_tree(owner.id.as_str(), class_name, &policy, &tree)?;
            }
        }
        Ok(refresh)
    }

    /// Index what one class's walk finds, within `budget`, and forget the
    /// class's sources it no longer finds. The other class and its cursors
    /// are never touched.
    #[allow(clippy::too_many_arguments)]
    fn refresh_class(
        &self,
        index: &mut HostIndex,
        owner: &Owner,
        class: MemoryScope,
        unresolved: &[PathBuf],
        agents: Option<(u64, u64)>,
        now_ms: i64,
        budget: &mut u64,
        gate: &mut Gate<'_>,
    ) -> Result<(Walk, ClassRefresh), IndexError> {
        let root = self.data_dir().join("agents").join(owner.id.as_str());
        let mut walk = Walk::new(&root);
        let mut keep = HashSet::new();
        let mut refresh = ClassRefresh::complete();
        // The durable redactor is the only text mutation authority. A
        // pending/failed copy excludes precisely that path and descendants;
        // an unrelated unsupported store does not hide scrubbed local data.
        let blocked = |path: &Path| unresolved.iter().any(|b| path.starts_with(b));
        if agent_root_safe(&mut walk, agents)? {
            match class {
                MemoryScope::Memory => {
                    for path in memory_indexable_files(&mut walk, &root.join("memory"))? {
                        if blocked(&path) {
                            continue;
                        }
                        let id = format!(
                            "memory:{}:{}",
                            owner.id,
                            sha256(path.as_os_str().as_encoded_bytes())
                        );
                        let metadata = memory_metadata(owner, Some(modified_ms(&path)?));
                        let stats = retry_later(
                            refresh_document_ctx(
                                index,
                                &id,
                                &metadata,
                                &path,
                                budget,
                                gate,
                                || Ok(()),
                            ),
                            &path,
                        )?;
                        walk.note(&path, Some(stats.stamp.clone()));
                        refresh.add(&stats);
                        keep.insert(path.to_string_lossy().into_owned());
                    }
                }
                MemoryScope::Transcripts => {
                    for source in native_sources(&mut walk, &root)? {
                        if blocked(&source.path) {
                            continue;
                        }
                        let path = source.path;
                        if let NativeFormat::Store(_) = source.format {
                            // New rows land in the write-ahead log before a
                            // checkpoint touches the database file: both stamps,
                            // taken before the read, describe the store.
                            let wal = crate::host_sqlite::wal_path(&path);
                            walk.note(&wal, FileStamp::of_path(&wal)?);
                        }
                        let stats = retry_later(
                            refresh_source_ctx(
                                index,
                                owner,
                                &path,
                                source.format,
                                is_external_harness_transcript(&root, &path),
                                now_ms,
                                budget,
                                gate,
                            ),
                            &path,
                        )?;
                        walk.note(&path, Some(stats.stamp.clone()));
                        refresh.add(&stats);
                        if stats.indexed {
                            keep.insert(path.to_string_lossy().into_owned());
                        }
                    }
                    // Reconciled archive pages are lossless historical documents,
                    // not necessarily JSONL. Their entire text remains searchable.
                    for path in history_archives(&mut walk, &root)? {
                        if blocked(&path) {
                            continue;
                        }
                        let id = format!(
                            "history:{}:{}",
                            owner.id,
                            sha256(path.as_os_str().as_encoded_bytes())
                        );
                        let stats = retry_later(
                            refresh_document_ctx(
                                index,
                                &id,
                                &history_metadata(owner),
                                &path,
                                budget,
                                gate,
                                || Ok(()),
                            ),
                            &path,
                        )?;
                        walk.note(&path, Some(stats.stamp.clone()));
                        refresh.add(&stats);
                        keep.insert(path.to_string_lossy().into_owned());
                    }
                }
                MemoryScope::All => {
                    return Err(io::Error::other("refresh one history class at a time").into());
                }
            }
        }
        let stale: Vec<String> = index
            .agent_sources(owner.id.as_str())?
            .into_iter()
            .filter(|(segment, cursor)| {
                !keep.contains(segment) && source_class(cursor).is_none_or(|c| c == class)
            })
            .map(|(segment, _)| segment)
            .collect();
        if !stale.is_empty() {
            match gate()? {
                Some(_guard) => index.forget_sources(owner.id.as_str(), &stale)?,
                None => refresh.complete = false,
            }
        }
        Ok((walk, refresh))
    }
}

/// A source that changed under its read is not an error: it is left for a
/// later refresh, which reads it again.
fn retry_later(
    result: Result<RefreshStats, IndexError>,
    path: &Path,
) -> Result<RefreshStats, IndexError> {
    match result {
        Err(IndexError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock => {
            let stamp = FileStamp::read(&open_readonly(path)?)?;
            let len = stamp.len;
            Ok(RefreshStats::new(stamp).pending(len))
        }
        other => other,
    }
}

/// What refreshing one class left: everything published, or bytes still to
/// index in the background.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ClassRefresh {
    complete: bool,
    pending_sources: usize,
    pending_bytes: u64,
}
impl ClassRefresh {
    fn complete() -> Self {
        Self {
            complete: true,
            ..Self::default()
        }
    }
    fn add(&mut self, stats: &RefreshStats) {
        if !stats.complete {
            self.complete = false;
            self.pending_sources += 1;
            self.pending_bytes += stats.pending_bytes;
        }
    }
}

/// Hits with the raw bytes each window read back.
pub type HostHits = Vec<(IndexHit, Vec<u8>)>;

/// History a search could not index before answering: it is being indexed
/// in the background, so results from it may be missing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PendingHistory {
    pub sources: usize,
    pub bytes: u64,
}

/// Background work: a class a search left incomplete, or a source due for
/// full verification.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Job {
    Class(String, MemoryScope),
    /// Owner, source, and when the verifier scheduled it: the time recorded.
    Verify(String, PathBuf, i64),
}

/// Jobs by data directory, for the background indexer; and its pre-warm
/// pass over every agent after the process starts.
struct Backlog {
    queue: std::collections::VecDeque<Job>,
    prewarm: Option<Vec<String>>,
    prewarm_next: usize,
}
static BACKLOG: std::sync::LazyLock<(
    std::sync::Mutex<HashMap<PathBuf, Backlog>>,
    std::sync::Condvar,
)> = std::sync::LazyLock::new(Default::default);

fn backlog() -> std::sync::MutexGuard<'static, HashMap<PathBuf, Backlog>> {
    BACKLOG
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
fn backlog_entry<'a>(map: &'a mut HashMap<PathBuf, Backlog>, data: &Path) -> &'a mut Backlog {
    map.entry(data.to_owned()).or_insert_with(|| Backlog {
        queue: std::collections::VecDeque::new(),
        prewarm: None,
        prewarm_next: 0,
    })
}

fn enqueue_job(data: &Path, job: Job) {
    let mut map = backlog();
    let entry = backlog_entry(&mut map, data);
    if !entry.queue.contains(&job) {
        entry.queue.push_back(job);
    }
    BACKLOG.1.notify_all();
}
fn enqueue(data: &Path, owner: &str, class: MemoryScope) {
    enqueue_job(data, Job::Class(owner.to_owned(), class));
}

impl Store {
    /// History classes waiting for the background indexer in this process.
    pub fn host_history_backlog(&self) -> usize {
        backlog().get(self.data_dir()).map_or(0, |b| b.queue.len())
    }

    /// One bounded step of background indexing: a queued job first (a class
    /// a search left incomplete, or a source due for verification),
    /// otherwise the next agent of the pre-warm pass that covers every agent
    /// once per process, live agents first. A class still incomplete after
    /// `budget` bytes is queued again behind the others. Returns whether
    /// there was work.
    pub fn index_host_history_step(&self, budget: u64) -> Result<bool, IndexError> {
        let data = self.data_dir().to_owned();
        let next = backlog_entry(&mut backlog(), &data).queue.pop_front();
        let jobs: Vec<Job> = match next {
            Some(job) => vec![job],
            None => {
                let listed = backlog().get(&data).and_then(|b| b.prewarm.clone());
                let list = match listed {
                    Some(list) => list,
                    None => {
                        let mut live = Vec::new();
                        let mut archived = Vec::new();
                        for agent in self.list_agents().map_err(store_error)? {
                            if agent.state == "archived" {
                                archived.push(agent.id.to_string());
                            } else {
                                live.push(agent.id.to_string());
                            }
                        }
                        live.extend(archived);
                        backlog_entry(&mut backlog(), &data).prewarm = Some(live.clone());
                        live
                    }
                };
                let at = {
                    let mut map = backlog();
                    let entry = backlog_entry(&mut map, &data);
                    entry.prewarm_next += 1;
                    entry.prewarm_next - 1
                };
                match list.get(at) {
                    Some(owner) => CLASSES
                        .iter()
                        .map(|c| Job::Class(owner.clone(), *c))
                        .collect(),
                    None => {
                        // Once: what a dense rebuild should wait for.
                        if at == list.len() {
                            eprintln!(
                                "anyagents-host-history: pre-warm done: {} agents' history indexed",
                                list.len()
                            );
                        }
                        return Ok(false);
                    }
                }
            }
        };
        let index_path = data.join(INDEX_FILE);
        let mut index = HostIndex::shared(&index_path)?;
        let agents = agents_identity(&data)?;
        for job in jobs {
            let owner = match &job {
                Job::Class(owner, _) | Job::Verify(owner, _, _) => owner.as_str(),
            };
            let Some((id, name)) = self.agent_names([owner]).map_err(store_error)?.pop() else {
                continue;
            };
            let owner = Owner { id, name };
            match job {
                Job::Class(_, class) => {
                    let unresolved = self.unresolved_host_paths(owner.id.as_str())?;
                    let mut remaining = budget;
                    let refresh = self.refresh_class_if_changed(
                        &mut index,
                        &owner,
                        class,
                        &unresolved,
                        agents,
                        crate::now_ms(),
                        &mut remaining,
                    )?;
                    if !refresh.complete {
                        enqueue(&data, owner.id.as_str(), class);
                    }
                }
                Job::Verify(_, path, at) => {
                    self.verify_host_history_source(&mut index, &owner, &path, at)?;
                }
            }
        }
        Ok(true)
    }

    /// Fully verify one source the verifier scheduled, if it is still one of
    /// the owner's sources and not withheld by a pending redaction.
    fn verify_host_history_source(
        &self,
        index: &mut HostIndex,
        owner: &Owner,
        path: &Path,
        now_ms: i64,
    ) -> Result<bool, IndexError> {
        let root = self.data_dir().join("agents").join(owner.id.as_str());
        let mut walk = Walk::new(&root);
        let found = if safe_components(&mut walk, self.data_dir(), &root)? {
            native_sources(&mut walk, &root)?
                .into_iter()
                .find(|s| s.path == path)
        } else {
            None
        };
        let Some(found) = found else {
            return Ok(false);
        };
        let epoch = self.redaction_epoch(owner.id.as_str())?;
        if self
            .unresolved_host_paths(owner.id.as_str())?
            .iter()
            .any(|p| path.starts_with(p))
        {
            return Ok(false);
        }
        let mut gate = || -> Result<Option<Box<dyn std::any::Any>>, IndexError> {
            let guard = self.lock_agent_lifecycle(&owner.id).map_err(store_error)?;
            if self.redaction_epoch(owner.id.as_str())? != epoch {
                return Ok(None);
            }
            Ok(Some(Box::new(guard)))
        };
        verify_source_ctx(
            index,
            owner,
            path,
            found.format,
            is_external_harness_transcript(&root, path),
            now_ms,
            &mut gate,
        )
    }

    /// Run the background indexer on the calling thread for as long as the
    /// process lives: classes searches left incomplete, then a pre-warm pass
    /// over every agent, then nothing until a search queues more. The caller
    /// runs it at the lowest CPU priority. A failing class is reported and
    /// left for the next search to queue again.
    pub fn run_host_history_indexer(&self) {
        loop {
            match self.index_host_history_step(BACKGROUND_STEP_BYTES) {
                Ok(true) => continue,
                Ok(false) => {}
                Err(error) => {
                    eprintln!("anyagents-host-history: background indexing: {error}");
                    std::thread::sleep(std::time::Duration::from_secs(10));
                    continue;
                }
            }
            let map = backlog();
            let _idle = BACKLOG
                .1
                .wait_while(map, |map| {
                    map.get(self.data_dir()).is_none_or(|b| b.queue.is_empty())
                })
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

fn unscrubbed_paths(receipt: &str) -> Result<Vec<PathBuf>, IndexError> {
    let receipt: crate::redaction::Receipt = serde_json::from_str(receipt)?;
    Ok(receipt
        .unscrubbed
        .into_iter()
        .map(|f| PathBuf::from(f.path))
        .collect())
}

const CLASSES: [MemoryScope; 2] = [MemoryScope::Memory, MemoryScope::Transcripts];
fn classes(scope: MemoryScope) -> &'static [MemoryScope] {
    match scope {
        MemoryScope::All => &CLASSES,
        MemoryScope::Memory => &CLASSES[..1],
        MemoryScope::Transcripts => &CLASSES[1..],
    }
}

/// The class a source cursor was published for. Unknown cursors are treated
/// as belonging to whichever class is being refreshed.
fn source_class(cursor: &str) -> Option<MemoryScope> {
    let cursor: SourceCursor = serde_json::from_str(cursor).ok()?;
    if cursor.policy.starts_with("native-") {
        return Some(MemoryScope::Transcripts);
    }
    serde_json::from_str::<IndexMetadata>(&cursor.policy)
        .ok()
        .map(|m| m.source)
}

fn native_policy(owner: &Owner) -> String {
    NativeFormat::Jsonl.policy(owner)
}

/// How one native history source is laid out, and so how its events are
/// framed and which fields carry their metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NativeFormat {
    /// One event per JSONL line, metadata in top-level fields: AnyAgents
    /// transcripts, legacy Spiron `sylphx-transcript.jsonl`, Claude Code
    /// sessions and subagents, OMP, Grok and Muse.
    Jsonl,
    /// Codex rollouts: `{timestamp, type, payload}` per line.
    CodexRollout,
    /// AGY transcripts: `{created_at, type, source, content}` per line.
    AgyTranscript,
    /// A harness's own SQLite store, one event per row.
    Store(StoreKind),
}
impl NativeFormat {
    /// The parser policy a published cursor records. A changed policy
    /// rebuilds the source.
    fn policy(self, owner: &Owner) -> String {
        let family = match self {
            Self::Jsonl => "native-jsonl-v4",
            Self::CodexRollout => "native-jsonl-v4+codex",
            Self::AgyTranscript => "native-jsonl-v4+agy",
            Self::Store(StoreKind::SpironSessions) => "native-sqlite-v1+spiron",
            Self::Store(StoreKind::OpenCodeMessages) => "native-sqlite-v1+opencode",
        };
        format!("{family}:{}:{}", owner.id, owner.name)
    }
    fn envelope(self) -> Envelope {
        match self {
            Self::Jsonl | Self::AgyTranscript => Envelope::None,
            Self::CodexRollout => Envelope::Codex,
            Self::Store(StoreKind::SpironSessions) => Envelope::Spiron,
            Self::Store(StoreKind::OpenCodeMessages) => Envelope::OpenCode,
        }
    }
}
struct NativeSource {
    path: PathBuf,
    format: NativeFormat,
}
fn memory_metadata(owner: &Owner, timestamp_ms: Option<i64>) -> IndexMetadata {
    IndexMetadata {
        agent_id: owner.id.to_string(),
        source: MemoryScope::Memory,
        timestamp_ms,
        direction: "in".into(),
        sender: format!("agent:{}", owner.name),
        destination: format!("agent:{}", owner.name),
        tool_name: None,
        tool_names: Vec::new(),
        role: Some("memory".into()),
    }
}
fn history_metadata(owner: &Owner) -> IndexMetadata {
    IndexMetadata {
        agent_id: owner.id.to_string(),
        source: MemoryScope::Transcripts,
        timestamp_ms: None,
        direction: "in".into(),
        sender: format!("agent:{}", owner.name),
        destination: format!("agent:{}", owner.name),
        tool_name: None,
        tool_names: Vec::new(),
        role: Some("history".into()),
    }
}

/// Everything besides file stamps that decides what a class indexes: every
/// policy and metadata shape that names the agent, and its redaction
/// exclusions. Bump the version when indexing changes in any other way.
fn tree_policy(owner: &Owner, unresolved: &[PathBuf]) -> Result<String, IndexError> {
    let mut unresolved: Vec<_> = unresolved.iter().map(|p| p.to_string_lossy()).collect();
    unresolved.sort();
    Ok(serde_json::to_string(&(
        // v2: every harness's native history location is walked.
        "host-tree-v2",
        native_policy(owner),
        memory_metadata(owner, None),
        history_metadata(owner),
        unresolved,
    ))?)
}

/// What one refresh of a history class inspected: the agent root, every
/// directory it listed and every file it indexed, each stamped before it was
/// read. A change to any entry changes its stamp, and an added, removed or
/// renamed entry changes its directory's stamp, so while all of them still
/// match, the class has nothing new to index.
#[derive(Serialize, Deserialize)]
struct Tree {
    started_ms: i64,
    /// Identity of `agents/`; its own stamp changes with every new agent.
    agents: Option<(u64, u64)>,
    /// Paths relative to the agent root; the empty path is the root itself.
    entries: Vec<(PathBuf, Option<FileStamp>)>,
}
impl Tree {
    fn is_current(&self, root: &Path, agents: Option<(u64, u64)>) -> bool {
        self.agents == agents
            && self.entries.iter().all(|(relative, recorded)| {
                let path = if relative.as_os_str().is_empty() {
                    root.to_owned()
                } else {
                    root.join(relative)
                };
                // A stamp taken within the timestamp granularity of the walk
                // could be reused by a later change; such a tree is not trusted.
                recorded
                    .as_ref()
                    .is_none_or(|s| s.settled_before(self.started_ms))
                    && FileStamp::of_path(&path).is_ok_and(|now| now == *recorded)
            })
    }
}

/// Records what a refresh inspects, relative to the agent root.
struct Walk {
    root: PathBuf,
    entries: Vec<(PathBuf, Option<FileStamp>)>,
}
impl Walk {
    fn new(root: &Path) -> Self {
        Self {
            root: root.to_owned(),
            entries: Vec::new(),
        }
    }
    /// The first observation of a path is kept: any later change differs from it.
    fn note(&mut self, path: &Path, stamp: Option<FileStamp>) {
        let relative = path.strip_prefix(&self.root).unwrap_or(path).to_owned();
        if !self.entries.iter().any(|(seen, _)| *seen == relative) {
            self.entries.push((relative, stamp));
        }
    }
    fn note_dir(&mut self, path: &Path, metadata: &fs::Metadata) {
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            self.note(path, Some(FileStamp::from_metadata(metadata)));
        }
    }
}

/// `agents/` must be a directory reached without a symlink.
fn agents_identity(data_dir: &Path) -> io::Result<Option<(u64, u64)>> {
    use std::os::unix::fs::MetadataExt;
    match fs::symlink_metadata(data_dir.join("agents")) {
        Ok(m) if m.file_type().is_symlink() => Ok(None),
        Ok(m) => Ok(Some((m.dev(), m.ino()))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}
/// The same check as `safe_components(data_dir, root)`, with `agents/`
/// checked once per search. The root is recorded even when absent.
fn agent_root_safe(walk: &mut Walk, agents: Option<(u64, u64)>) -> io::Result<bool> {
    let root = walk.root.clone();
    let metadata = match fs::symlink_metadata(&root) {
        Ok(m) => Some(m),
        Err(e) if e.kind() == io::ErrorKind::NotFound => None,
        Err(e) => return Err(e),
    };
    walk.note(&root, metadata.as_ref().map(FileStamp::from_metadata));
    Ok(agents.is_some() && metadata.is_some_and(|m| !m.file_type().is_symlink()))
}

/// Raw bytes one batch reads before it publishes. Every read, hash and token
/// projection of a batch happens with no lock and no index write held; only
/// publishing a batch holds the owner's lifecycle lock and the index write,
/// so this bounds how long either is held.
const BATCH_BYTES: u64 = 2 * 1024 * 1024;
fn batch_bytes() -> u64 {
    #[cfg(test)]
    if let Some(bytes) = tests::BATCH_BYTES_OVERRIDE.with(std::cell::Cell::get) {
        return bytes;
    }
    BATCH_BYTES
}
/// Store rows one batch reads.
const BATCH_ROWS: usize = 512;
/// New history one search may index before it answers. A class with more is
/// handed to the background indexer and the answer says it is partial.
pub const REQUEST_INDEX_BYTES: u64 = 4 * 1024 * 1024;
/// What one background step may index before it yields to the next class.
pub const BACKGROUND_STEP_BYTES: u64 = 64 * 1024 * 1024;

/// May a batch publish now? `Some(guard)` is what must be held while it does
/// (the owner's lifecycle lock, unless the caller already holds it); `None`
/// means the owner's history changed under the read (a redaction), so the
/// batch is dropped and the class is left for a later refresh.
pub(crate) type Gate<'a> = dyn FnMut() -> Result<Option<Box<dyn std::any::Any>>, IndexError> + 'a;

/// A gate for a caller that already holds what publishing needs.
#[cfg(test)]
fn open_gate() -> Result<Option<Box<dyn std::any::Any>>, IndexError> {
    Ok(Some(Box::new(())))
}

#[derive(Clone, Debug)]
pub struct RefreshStats {
    /// The stamp of the file the published frontier describes.
    stamp: FileStamp,
    pub prefix_verified_bytes: u64,
    pub boundary_capture_bytes: u64,
    pub suffix_scanned_bytes: u64,
    pub indexed_records: usize,
    /// Everything the source held when read is published.
    pub complete: bool,
    /// Bytes still to index when not complete (an estimate for a store).
    pub pending_bytes: u64,
    /// A source this reader understands; a store of another layout is not.
    indexed: bool,
}
impl RefreshStats {
    fn new(stamp: FileStamp) -> Self {
        Self {
            stamp,
            prefix_verified_bytes: 0,
            boundary_capture_bytes: 0,
            suffix_scanned_bytes: 0,
            indexed_records: 0,
            complete: false,
            pending_bytes: 0,
            indexed: true,
        }
    }
    fn done(mut self) -> Self {
        self.complete = true;
        self.pending_bytes = 0;
        self
    }
    fn pending(mut self, bytes: u64) -> Self {
        self.complete = false;
        self.pending_bytes = bytes.max(1);
        self
    }
}

/// The published cursor a batch was planned from must still be the
/// published one when it publishes; anything else means another refresh
/// moved the frontier first, and this batch is planned again.
fn same_cursor(a: &Option<SourceCursor>, b: &Option<SourceCursor>) -> bool {
    let key = |c: &Option<SourceCursor>| c.as_ref().and_then(|c| serde_json::to_string(c).ok());
    key(a) == key(b)
}

fn changed_during_refresh(what: &str) -> IndexError {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        format!("{what} changed during refresh; retry"),
    )
    .into()
}

#[cfg(test)]
fn refresh_document(
    index: &mut HostIndex,
    id: &str,
    metadata: &IndexMetadata,
    path: &Path,
) -> Result<FileStamp, IndexError> {
    refresh_document_before_publish(index, id, metadata, path, || Ok(()))
}
#[cfg(test)]
fn refresh_document_before_publish(
    index: &mut HostIndex,
    id: &str,
    metadata: &IndexMetadata,
    path: &Path,
    before_publish: impl FnOnce() -> io::Result<()>,
) -> Result<FileStamp, IndexError> {
    let mut budget = u64::MAX;
    refresh_document_ctx(
        index,
        id,
        metadata,
        path,
        &mut budget,
        &mut open_gate,
        before_publish,
    )
    .map(|stats| stats.stamp)
}

/// Index one whole-file document (a learned-memory topic or an archive
/// page). It is read and projected with no lock held; publishing checks
/// that the file still has the stamp it was read at.
fn refresh_document_ctx(
    index: &mut HostIndex,
    id: &str,
    metadata: &IndexMetadata,
    path: &Path,
    budget: &mut u64,
    gate: &mut Gate<'_>,
    before_publish: impl FnOnce() -> io::Result<()>,
) -> Result<RefreshStats, IndexError> {
    // One path walk; the stamp, hash and projection share this descriptor.
    let mut source = RawSource::open(path)?;
    let stamp = source.stamp().clone();
    let stats = RefreshStats::new(stamp.clone());
    let policy = serde_json::to_string(metadata)?;
    if index
        .source_cursor(&metadata.agent_id, path)?
        .is_some_and(|old| old.stamp == stamp && old.policy == policy)
    {
        return Ok(stats.done());
    }
    // A whole document is one batch. One larger than what is left of the
    // budget waits, unless no smaller budget would ever take it.
    if stamp.len > *budget && *budget < BACKGROUND_STEP_BYTES {
        return Ok(stats.pending(stamp.len));
    }
    let raw = source.whole()?;
    let prepared = if index.is_current(id, metadata, &raw)? {
        None
    } else {
        Some(crate::host_index::prepare_document(
            &mut source,
            id,
            metadata,
            &raw,
        )?)
    };
    drop(source);
    let Some(_guard) = gate()? else {
        return Ok(stats.pending(stamp.len));
    };
    index.refresh_source(|index| {
        if let Some(doc) = &prepared {
            index.publish_document(doc)?;
        }
        before_publish()?;
        if FileStamp::read(&open_readonly(path)?)? != stamp {
            return Err(changed_during_refresh("memory source"));
        }
        index.save_source(
            &metadata.agent_id,
            path,
            &SourceCursor {
                stamp: stamp.clone(),
                policy,
                end: raw.length,
                next_seq: 2,
                boundary_sha256: String::new(),
                full_verified_ms: 0,
                verification_attempt_ms: 0,
                complete: true,
                rebuilding: false,
            },
        )?;
        Ok(())
    })?;
    *budget = budget.saturating_sub(raw.length);
    Ok(stats.done())
}

/// A history file a harness writes, not AnyAgents. Its writer may crash
/// mid-line or concatenate records, so a malformed complete line is indexed
/// as text with unknown metadata instead of failing the agent's search.
fn is_external_harness_transcript(root: &Path, path: &Path) -> bool {
    path.starts_with(root.join("config"))
}
#[cfg(test)]
fn refresh_native(
    index: &mut HostIndex,
    owner: &Owner,
    path: &Path,
) -> Result<RefreshStats, IndexError> {
    refresh_native_at(
        index,
        owner,
        path,
        NativeFormat::Jsonl,
        false,
        false,
        crate::now_ms(),
    )
}
#[cfg(test)]
fn refresh_native_at(
    index: &mut HostIndex,
    owner: &Owner,
    path: &Path,
    format: NativeFormat,
    tolerate_malformed: bool,
    force_full: bool,
    now_ms: i64,
) -> Result<RefreshStats, IndexError> {
    let mut budget = u64::MAX;
    refresh_native_ctx(
        index,
        owner,
        path,
        format,
        tolerate_malformed,
        force_full,
        now_ms,
        &mut budget,
        &mut open_gate,
    )
}
#[cfg(test)]
fn refresh_store_at(
    index: &mut HostIndex,
    owner: &Owner,
    db: &Path,
    kind: StoreKind,
    force_full: bool,
    now_ms: i64,
) -> Result<RefreshStats, IndexError> {
    let mut budget = u64::MAX;
    refresh_store_ctx(
        index,
        owner,
        db,
        kind,
        force_full,
        now_ms,
        &mut budget,
        &mut open_gate,
    )
}

#[allow(clippy::too_many_arguments)]
fn refresh_source_ctx(
    index: &mut HostIndex,
    owner: &Owner,
    path: &Path,
    format: NativeFormat,
    tolerate_malformed: bool,
    now_ms: i64,
    budget: &mut u64,
    gate: &mut Gate<'_>,
) -> Result<RefreshStats, IndexError> {
    match format {
        NativeFormat::Store(kind) => {
            refresh_store_ctx(index, owner, path, kind, false, now_ms, budget, gate)
        }
        _ => refresh_native_ctx(
            index,
            owner,
            path,
            format,
            tolerate_malformed,
            false,
            now_ms,
            budget,
            gate,
        ),
    }
}

/// Metadata of one native event and the tool calls it defines. Calls are
/// correlated within one source: a file, or one native store.
fn native_metadata(
    index: &HostIndex,
    owner: &Owner,
    source: &Path,
    header: &EventHeader,
    format: NativeFormat,
    batch: &Prepared,
) -> Result<(IndexMetadata, Vec<(String, String)>), IndexError> {
    let mut metadata = event_metadata(owner, header, format);
    let records = header.tool_records(format.envelope());
    let mut names = Vec::new();
    if let Some(name) = &metadata.tool_name {
        names.push(name.clone());
    }
    for (call_id, name) in &records {
        let name = name.map(str::to_owned).or_else(|| {
            call_id
                .and_then(|id| {
                    records
                        .iter()
                        .find_map(|(key, name)| (*key == Some(id)).then_some(*name).flatten())
                })
                .map(str::to_owned)
        });
        let name = match (name, call_id) {
            (Some(name), _) => name,
            (None, Some(id)) => batch
                .call(index, owner.id.as_str(), source, id)?
                .unwrap_or_else(|| "unknown".into()),
            (None, None) => "unknown".into(),
        };
        if !names.contains(&name) {
            names.push(name);
        }
    }
    metadata.tool_name = names.first().cloned();
    metadata.tool_names = names;
    let defined = records
        .into_iter()
        .filter_map(|(call, name)| Some((call?.to_owned(), name?.to_owned())))
        .collect();
    Ok((metadata, defined))
}

/// A batch read and projected with no lock held, waiting to publish.
struct Prepared {
    docs: Vec<crate::host_index::PreparedDocument>,
    calls: Vec<(String, String, String)>,
    /// The batch starts a rebuild: publishing it first drops every document
    /// of the source, so nothing published may be reused.
    clearing: bool,
}
impl Prepared {
    fn new(clearing: bool) -> Self {
        Self {
            docs: Vec::new(),
            calls: Vec::new(),
            clearing,
        }
    }
    /// The tool a call defined earlier in this batch or, unless the batch
    /// starts a rebuild, by a published document.
    fn call(
        &self,
        index: &HostIndex,
        owner: &str,
        source: &Path,
        id: &str,
    ) -> Result<Option<String>, IndexError> {
        if let Some(name) = self
            .calls
            .iter()
            .rev()
            .find_map(|(call, name, _)| (call == id).then_some(name))
        {
            return Ok(Some(name.clone()));
        }
        if self.clearing {
            return Ok(None);
        }
        index.call_tool(owner, source, id)
    }
    /// Project one event unless the index already holds it exactly, and
    /// record the calls it defines.
    fn add(
        &mut self,
        index: &HostIndex,
        source: &mut RawSource,
        id: String,
        metadata: IndexMetadata,
        raw: &crate::raw_segment::RawCoordinate,
        calls: Vec<(String, String)>,
    ) -> Result<(), IndexError> {
        if self.clearing || !index.is_current(&id, &metadata, raw)? {
            self.docs.push(crate::host_index::prepare_document(
                source, &id, &metadata, raw,
            )?);
        }
        for (call, name) in calls {
            self.calls.push((call, name, id.clone()));
        }
        Ok(())
    }
    /// Write the batch. The caller holds the gate and the write scope.
    fn publish(&self, index: &mut HostIndex, owner: &str, source: &Path) -> Result<(), IndexError> {
        if self.clearing {
            index.clear_source(owner, source)?;
        }
        for doc in &self.docs {
            index.publish_document(doc)?;
        }
        for (call, name, id) in &self.calls {
            index.save_call(owner, source, call, name, id)?;
        }
        Ok(())
    }
}

/// One store row as a document ready to publish.
fn prepare_row(
    index: &HostIndex,
    owner: &Owner,
    db: &Path,
    kind: StoreKind,
    row: crate::host_sqlite::StoreRow,
    prepared: &mut Prepared,
) -> Result<(), IndexError> {
    let reference = RowRef::new(db, kind, row.rowid);
    let raw = crate::raw_segment::RawCoordinate {
        segment: reference.segment(),
        offset: 0,
        length: row.bytes.len() as u64,
        sha256: sha256(&row.bytes),
        seq: row.rowid.max(0) as u64,
    };
    // A row the store accepted but this reader cannot frame still indexes
    // its text, with unknown metadata.
    let header: EventHeader =
        serde_json::from_reader(MetadataReader::new(&row.bytes[..])).unwrap_or_default();
    let (mut metadata, calls) = native_metadata(
        index,
        owner,
        db,
        &header,
        NativeFormat::Store(kind),
        prepared,
    )?;
    if let Some(role) = row.role.as_deref().filter(|r| !r.is_empty()) {
        metadata.direction = if role == "user" { "in" } else { "out" }.into();
        metadata.role = Some(role.into());
    }
    if row.timestamp_ms.is_some() {
        metadata.timestamp_ms = row.timestamp_ms;
    }
    let id = format!(
        "transcript:{}:{}:{}",
        owner.id,
        sha256(db.as_os_str().as_encoded_bytes()),
        row.rowid
    );
    let mut source = RawSource::from_row(&reference, row.bytes)?;
    prepared.add(index, &mut source, id, metadata, &raw, calls)
}

/// The digest of one store row, or empty for no row.
fn row_digest(store: &crate::host_sqlite::Store, rowid: i64) -> io::Result<String> {
    Ok(match rowid {
        rowid if rowid > 0 => store.row(rowid)?.map(|b| sha256(&b)).unwrap_or_default(),
        _ => String::new(),
    })
}

/// The rows a batch read must still be the store's when it publishes: the
/// same database file, with the batch's last row unchanged.
fn check_store(
    db: &Path,
    kind: StoreKind,
    stamp: &FileStamp,
    last: i64,
    digest: &str,
) -> Result<(), IndexError> {
    if !FileStamp::read(&open_readonly(db)?)?.same_file(stamp) {
        return Err(changed_during_refresh("native store"));
    }
    if last > 0 {
        let check = crate::host_sqlite::Store::open(db, kind)?
            .ok_or_else(|| changed_during_refresh("native store"))?;
        if row_digest(&check, last)? != digest {
            return Err(changed_during_refresh("native store"));
        }
    }
    Ok(())
}

/// Index what is new in one native SQLite store, in batches. Rows past the
/// published rowid frontier are read in rowid order, a bounded batch at a
/// time from one snapshot of the store each, projected with no lock held,
/// and published under the gate after checking that the store is the same
/// database and the batch's last row is unchanged. Once the frontier reaches
/// the store's last row, rows the store rewrote after the published change
/// mark are read the same way. A replaced database, a lower frontier or a
/// changed frontier row rebuilds the store: its first batch drops the old
/// documents. Deleted rows otherwise stay published until full verification
/// and are refused at readback by their digest.
#[allow(clippy::too_many_arguments)]
fn refresh_store_ctx(
    index: &mut HostIndex,
    owner: &Owner,
    db: &Path,
    kind: StoreKind,
    force_full: bool,
    now_ms: i64,
    budget: &mut u64,
    gate: &mut Gate<'_>,
) -> Result<RefreshStats, IndexError> {
    let policy = NativeFormat::Store(kind).policy(owner);
    let mut force = force_full;
    let mut indexed_records = 0;
    loop {
        let stamp = FileStamp::read(&open_readonly(db)?)?;
        let mut stats = RefreshStats::new(stamp.clone());
        stats.indexed_records = indexed_records;
        let Some(store) = crate::host_sqlite::Store::open(db, kind)? else {
            // Not this harness's layout: nothing in it is history this reader
            // understands. Whatever an earlier layout published is forgotten
            // by the caller's walk.
            stats.indexed = false;
            return Ok(stats.done());
        };
        let old = index
            .source_cursor(owner.id.as_str(), db)?
            .filter(|old| old.policy == policy);
        store.snapshot()?;
        let (top, changed) = store.frontier()?;
        let top = top.max(0);
        let resume = match &old {
            Some(old)
                if !force
                    && old.stamp.same_file(&stamp)
                    && old.end <= top as u64
                    && row_digest(&store, old.end as i64)? == old.boundary_sha256 =>
            {
                Some(old.clone())
            }
            _ => None,
        };
        // Nothing past the frontier and nothing rewritten since its mark.
        if let Some(r) = &resume
            && r.complete
            && r.end == top as u64
            && r.next_seq == changed.max(0) as u64
        {
            return Ok(stats.done());
        }
        let clearing = resume.is_none();
        let rebuilding = clearing || resume.as_ref().is_some_and(|r| r.rebuilding);
        let (after, mark) = match &resume {
            Some(r) => (r.end as i64, r.next_seq as i64),
            // A rebuild remembers the change mark it started at, so a row
            // rewritten while it runs is read again afterwards.
            None => (0, changed.max(0)),
        };
        let average = if top > 0 { stamp.len / top as u64 } else { 0 }.max(1);
        let rewrites = after >= top;
        if !rewrites && *budget == 0 {
            return Ok(stats.pending((top - after) as u64 * average));
        }
        let rows = if rewrites {
            store.rows_changed(top, mark, BATCH_ROWS, batch_bytes().min(*budget))?
        } else {
            store.rows_after(after, top, BATCH_ROWS, batch_bytes().min(*budget))?
        };
        if rewrites && rows.is_empty() && resume.as_ref().is_some_and(|r| r.complete) {
            return Ok(stats.done());
        }
        if rewrites && !rows.is_empty() && *budget == 0 {
            return Ok(stats.pending(average * rows.len() as u64));
        }
        let read: u64 = rows.iter().map(|r| r.bytes.len() as u64).sum();
        let end = if rewrites {
            after
        } else {
            rows.last().map_or(after, |r| r.rowid)
        };
        // The rewrites of a full batch continue from its last change mark;
        // a short batch reads every rewrite there is.
        let (new_mark, caught_up) = if !rewrites {
            (mark, false)
        } else if rows.len() < BATCH_ROWS && read < batch_bytes().min(*budget) {
            (rows.iter().map(|r| r.changed).fold(mark, i64::max), true)
        } else {
            // Rows sharing the last mark past this batch are read again
            // from the mark below it; reading one twice costs no write.
            let last = rows.last().map_or(mark, |r| r.changed);
            let below = rows
                .iter()
                .map(|r| r.changed)
                .filter(|c| *c < last)
                .max()
                .unwrap_or(last);
            (below.max(mark), false)
        };
        let boundary_sha256 = row_digest(&store, end)?;
        drop(store);
        // Project every row with no lock held.
        let mut prepared = Prepared::new(clearing);
        for row in rows {
            prepare_row(index, owner, db, kind, row, &mut prepared)?;
            stats.indexed_records += 1;
        }
        indexed_records = stats.indexed_records;
        let Some(_guard) = gate()? else {
            return Ok(stats.pending(read.max(average)));
        };
        let published = index.refresh_source(|index| {
            let current = index
                .source_cursor(owner.id.as_str(), db)?
                .filter(|old| old.policy == policy);
            if !same_cursor(&current, &old) {
                return Ok(false);
            }
            check_store(db, kind, &stamp, end, &boundary_sha256)?;
            prepared.publish(index, owner.id.as_str(), db)?;
            let completed = rebuilding && caught_up;
            index.save_source(
                owner.id.as_str(),
                db,
                &SourceCursor {
                    stamp: stamp.clone(),
                    policy: policy.clone(),
                    end: end.max(0) as u64,
                    next_seq: new_mark.max(0) as u64,
                    boundary_sha256: boundary_sha256.clone(),
                    full_verified_ms: match (&old, completed) {
                        (_, true) => now_ms,
                        (Some(old), false) => old.full_verified_ms,
                        (None, false) => 0,
                    },
                    verification_attempt_ms: match (&old, completed) {
                        (_, true) => now_ms,
                        (Some(old), false) => old.verification_attempt_ms,
                        (None, false) => 0,
                    },
                    complete: caught_up,
                    rebuilding: rebuilding && !caught_up,
                },
            )?;
            Ok(true)
        })?;
        *budget = budget.saturating_sub(read.max(1));
        force = false;
        if published && caught_up {
            return Ok(stats.done());
        }
    }
}

/// The records a batch read must still be the file's when it publishes: the
/// same file, unchanged, or grown by appends with the batch's boundary (the
/// 64 KiB before its end) unchanged.
fn check_jsonl(path: &Path, stamp: &FileStamp, end: u64, boundary: &str) -> Result<(), IndexError> {
    let mut check = RawSource::open(path)?;
    let now = check.stamp().clone();
    if now == *stamp {
        return Ok(());
    }
    if !now.same_file(stamp)
        || now.len <= stamp.len
        || crate::host_cursor::boundary_hash(&mut check, end)?.0 != boundary
    {
        return Err(changed_during_refresh("native source"));
    }
    Ok(())
}

/// Read and project one scanned batch of JSONL records with no lock held.
#[allow(clippy::too_many_arguments)]
fn prepare_records(
    index: &HostIndex,
    owner: &Owner,
    path: &Path,
    format: NativeFormat,
    tolerate_malformed: bool,
    source: &mut RawSource,
    records: &[crate::raw_segment::RawCoordinate],
    prepared: &mut Prepared,
) -> Result<(), IndexError> {
    for raw in records {
        // EventHeader is derivative metadata; malformed/torn external JSONL
        // records must not break lexical indexing or fail whole-agent search.
        let header: EventHeader = match serde_json::from_reader(MetadataReader::new(
            source.range(raw.offset, raw.length),
        )) {
            Ok(header) => header,
            Err(error)
                if tolerate_malformed
                    && matches!(
                        error.classify(),
                        serde_json::error::Category::Syntax | serde_json::error::Category::Eof
                    ) =>
            {
                EventHeader::default()
            }
            Err(error) => return Err(error.into()),
        };
        let (metadata, calls) = native_metadata(index, owner, path, &header, format, prepared)?;
        let id = format!(
            "transcript:{}:{}:{}",
            owner.id,
            sha256(path.as_os_str().as_encoded_bytes()),
            raw.seq
        );
        prepared.add(index, source, id, metadata, raw, calls)?;
    }
    Ok(())
}

/// Index what is new in one native JSONL file, in batches. Each batch scans
/// a bounded run of complete records from the published frontier (checking
/// the 64 KiB boundary before it, as every incremental refresh does), reads
/// and projects them with no lock held, then publishes under the gate after
/// checking the file still holds them ([`check_jsonl`]). A changed boundary,
/// a replaced file or a forced refresh rebuilds the source: its first batch
/// drops the old documents.
#[allow(clippy::too_many_arguments)]
fn refresh_native_ctx(
    index: &mut HostIndex,
    owner: &Owner,
    path: &Path,
    format: NativeFormat,
    tolerate_malformed: bool,
    force_full: bool,
    now_ms: i64,
    budget: &mut u64,
    gate: &mut Gate<'_>,
) -> Result<RefreshStats, IndexError> {
    let policy = format.policy(owner);
    let mut force = force_full;
    let mut total: Option<RefreshStats> = None;
    loop {
        // One path walk; scan, metadata and projection share this descriptor.
        let mut source = RawSource::open(path)?;
        let stamp = source.stamp().clone();
        let mut stats = total
            .take()
            .unwrap_or_else(|| RefreshStats::new(stamp.clone()));
        stats.stamp = stamp.clone();
        let old = index
            .source_cursor(owner.id.as_str(), path)?
            .filter(|old| old.policy == policy);
        if !force
            && old
                .as_ref()
                .is_some_and(|old| old.complete && old.stamp == stamp)
        {
            return Ok(stats.done());
        }
        if *budget == 0 {
            let from = old
                .as_ref()
                .filter(|o| o.stamp.same_file(&stamp))
                .map_or(0, |o| o.end);
            return Ok(stats.pending(stamp.len.saturating_sub(from)));
        }
        let scan = crate::host_cursor::scan_jsonl_bounded(
            &mut source,
            old.as_ref(),
            force,
            tolerate_malformed,
            batch_bytes().min(*budget),
        )?;
        stats.prefix_verified_bytes += scan.prefix_verified_bytes;
        stats.boundary_capture_bytes += scan.boundary_capture_bytes;
        stats.suffix_scanned_bytes += scan.suffix_scanned_bytes;
        stats.indexed_records += scan.records.len();
        let rebuilding = scan.replace || old.as_ref().is_some_and(|o| o.rebuilding);
        let mut prepared = Prepared::new(scan.replace);
        prepare_records(
            index,
            owner,
            path,
            format,
            tolerate_malformed,
            &mut source,
            &scan.records,
            &mut prepared,
        )?;
        drop(source);
        let Some(_guard) = gate()? else {
            return Ok(stats.pending(stamp.len.saturating_sub(scan.start)));
        };
        let published = index.refresh_source(|index| {
            let current = index
                .source_cursor(owner.id.as_str(), path)?
                .filter(|old| old.policy == policy);
            if !same_cursor(&current, &old) {
                return Ok(false);
            }
            check_jsonl(path, &stamp, scan.end, &scan.boundary_sha256)?;
            prepared.publish(index, owner.id.as_str(), path)?;
            let completed = rebuilding && scan.reached_end;
            index.save_source(
                owner.id.as_str(),
                path,
                &SourceCursor {
                    stamp: stamp.clone(),
                    policy: policy.clone(),
                    end: scan.end,
                    next_seq: scan.next_seq,
                    boundary_sha256: scan.boundary_sha256.clone(),
                    full_verified_ms: match (&old, completed) {
                        (_, true) => now_ms,
                        (Some(old), false) => old.full_verified_ms,
                        (None, false) => 0,
                    },
                    verification_attempt_ms: match (&old, completed) {
                        (_, true) => now_ms,
                        (Some(old), false) => old.verification_attempt_ms,
                        (None, false) => 0,
                    },
                    complete: scan.reached_end,
                    rebuilding: rebuilding && !scan.reached_end,
                },
            )?;
            Ok(true)
        })?;
        *budget = budget.saturating_sub(scan.suffix_scanned_bytes.max(1));
        force = false;
        if published && scan.reached_end {
            return Ok(stats.done());
        }
        total = Some(stats);
    }
}

/// Fully verify one published source, in the background: read it from the
/// first record in bounded batches with no lock held, publish under the gate
/// only the documents whose bytes no longer match what is indexed, and when
/// the whole source has been read, drop documents past its last record and
/// record the verification. The published frontier is not touched until
/// then, so a source that fails to read keeps its frontier and documents.
/// Returns whether the verification was recorded.
fn verify_source_ctx(
    index: &mut HostIndex,
    owner: &Owner,
    path: &Path,
    format: NativeFormat,
    tolerate_malformed: bool,
    now_ms: i64,
    gate: &mut Gate<'_>,
) -> Result<bool, IndexError> {
    let policy = format.policy(owner);
    match format {
        NativeFormat::Store(kind) => {
            let mut after = 0i64;
            let mut first: Option<FileStamp> = None;
            loop {
                let stamp = FileStamp::read(&open_readonly(path)?)?;
                if !first.get_or_insert_with(|| stamp.clone()).same_file(&stamp) {
                    return Ok(false);
                }
                let Some(store) = crate::host_sqlite::Store::open(path, kind)? else {
                    return Ok(false);
                };
                store.snapshot()?;
                let (top, _) = store.frontier()?;
                let rows = store.rows_after(after, top.max(0), BATCH_ROWS, batch_bytes())?;
                let last = rows.last().map_or(after, |r| r.rowid);
                let digest = row_digest(&store, last)?;
                let present = if last >= top {
                    Some(store.rowids(top.max(0))?)
                } else {
                    None
                };
                drop(store);
                let mut prepared = Prepared::new(false);
                for row in rows {
                    prepare_row(index, owner, path, kind, row, &mut prepared)?;
                }
                let Some(_guard) = gate()? else {
                    return Ok(false);
                };
                let recorded = index.refresh_source(|index| {
                    check_store(path, kind, &stamp, last, &digest)?;
                    prepared.publish(index, owner.id.as_str(), path)?;
                    let Some(present) = &present else {
                        return Ok(false);
                    };
                    let Some(mut cursor) = index
                        .source_cursor(owner.id.as_str(), path)?
                        .filter(|c| c.policy == policy)
                    else {
                        return Ok(false);
                    };
                    let present: HashSet<u64> = present.iter().map(|r| *r as u64).collect();
                    let stale: Vec<u64> = index
                        .source_sequences(owner.id.as_str(), path)?
                        .into_iter()
                        .filter(|seq| *seq <= last.max(0) as u64 && !present.contains(seq))
                        .collect();
                    index.forget_sequences(owner.id.as_str(), path, &stale)?;
                    cursor.full_verified_ms = now_ms;
                    index.save_source(owner.id.as_str(), path, &cursor)?;
                    Ok(true)
                })?;
                if present.is_some() {
                    return Ok(recorded);
                }
                after = last;
            }
        }
        _ => {
            let mut at: Option<SourceCursor> = None;
            loop {
                let mut source = RawSource::open(path)?;
                let stamp = source.stamp().clone();
                if let Some(at) = &mut at {
                    if !at.stamp.same_file(&stamp) {
                        return Ok(false);
                    }
                    at.stamp = stamp.clone();
                }
                let scan = crate::host_cursor::scan_jsonl_bounded(
                    &mut source,
                    at.as_ref(),
                    at.is_none(),
                    tolerate_malformed,
                    batch_bytes(),
                )?;
                if at.is_some() && scan.replace {
                    // Rewritten under the read: the next refresh rebuilds it.
                    return Ok(false);
                }
                let mut prepared = Prepared::new(false);
                prepare_records(
                    index,
                    owner,
                    path,
                    format,
                    tolerate_malformed,
                    &mut source,
                    &scan.records,
                    &mut prepared,
                )?;
                drop(source);
                let Some(_guard) = gate()? else {
                    return Ok(false);
                };
                let recorded = index.refresh_source(|index| {
                    check_jsonl(path, &stamp, scan.end, &scan.boundary_sha256)?;
                    prepared.publish(index, owner.id.as_str(), path)?;
                    if !scan.reached_end {
                        return Ok(false);
                    }
                    let Some(current) = index
                        .source_cursor(owner.id.as_str(), path)?
                        .filter(|c| c.policy == policy)
                    else {
                        return Ok(false);
                    };
                    // A refresh that published past this read keeps its
                    // frontier and documents; this read verified all before.
                    let cursor = if current.end > scan.end {
                        SourceCursor {
                            full_verified_ms: now_ms,
                            ..current
                        }
                    } else {
                        index.forget_documents_from(owner.id.as_str(), path, scan.next_seq)?;
                        SourceCursor {
                            stamp: stamp.clone(),
                            policy: policy.clone(),
                            end: scan.end,
                            next_seq: scan.next_seq,
                            boundary_sha256: scan.boundary_sha256.clone(),
                            full_verified_ms: now_ms,
                            verification_attempt_ms: current.verification_attempt_ms,
                            complete: true,
                            rebuilding: false,
                        }
                    };
                    index.save_source(owner.id.as_str(), path, &cursor)?;
                    Ok(true)
                })?;
                if scan.reached_end {
                    return Ok(recorded);
                }
                at = Some(SourceCursor {
                    stamp,
                    policy: policy.clone(),
                    end: scan.end,
                    next_seq: scan.next_seq,
                    boundary_sha256: scan.boundary_sha256,
                    full_verified_ms: 0,
                    verification_attempt_ms: 0,
                    complete: false,
                    rebuilding: false,
                });
            }
        }
    }
}

/// No component below `root` may be a symlink or missing. Directories on the
/// way are recorded.
fn safe_components(walk: &mut Walk, root: &Path, target: &Path) -> io::Result<bool> {
    let relative = target.strip_prefix(root).map_err(io::Error::other)?;
    let mut path = root.to_owned();
    for component in relative.components() {
        path.push(component);
        match fs::symlink_metadata(&path) {
            Ok(m) if m.file_type().is_symlink() => return Ok(false),
            Ok(m) => walk.note_dir(&path, &m),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}
fn modified_ms(path: &Path) -> io::Result<i64> {
    Ok(fs::metadata(path)?
        .modified()?
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_millis()
        .min(i64::MAX as u128) as i64)
}
/// Read both the generic archive and the historical directory spelling so an
/// upgrade never makes already-migrated documents disappear. New migrations
/// only write the generic location.
fn history_archives(walk: &mut Walk, root: &Path) -> io::Result<Vec<PathBuf>> {
    let history = root.join("history");
    if let Ok(metadata) = fs::symlink_metadata(&history) {
        walk.note_dir(&history, &metadata);
    }
    let mut paths = regular_files(walk, &history.join("archive"), true)?;
    paths.extend(regular_files(walk, &history.join("owners"), true)?);
    Ok(paths)
}
fn is_hidden_memory_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|s| s.to_str())
        .is_some_and(|name| name.starts_with('.'))
}

fn memory_indexable_files(walk: &mut Walk, mem_dir: &Path) -> io::Result<Vec<PathBuf>> {
    let files: Vec<PathBuf> = regular_files(walk, mem_dir, true)?
        .into_iter()
        .filter(|p| !is_hidden_memory_file(p))
        .collect();
    let has_topic_files = files.iter().any(|p| {
        p.file_name()
            .and_then(|s| s.to_str())
            .is_some_and(|name| !name.eq_ignore_ascii_case("MEMORY.md"))
    });
    if !has_topic_files {
        return Ok(files);
    }
    Ok(files
        .into_iter()
        .filter(|p| {
            !p.file_name()
                .and_then(|s| s.to_str())
                .is_some_and(|name| name.eq_ignore_ascii_case("MEMORY.md"))
        })
        .collect())
}

/// Regular files under `root`, recording each directory before listing it.
fn regular_files(walk: &mut Walk, root: &Path, recursive: bool) -> io::Result<Vec<PathBuf>> {
    let meta = match fs::symlink_metadata(root) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
        Err(e) => return Err(e),
    };
    if !meta.is_dir() || meta.file_type().is_symlink() {
        return Ok(vec![]);
    }
    walk.note_dir(root, &meta);
    let mut result = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_file() {
            result.push(entry.path());
        } else if recursive && kind.is_dir() {
            result.extend(regular_files(walk, &entry.path(), true)?);
        }
    }
    result.sort();
    Ok(result)
}
/// Every native history source of one agent, found only below its own root
/// and never through a symlink. Each harness keeps its history where
/// AnyAgents points its configuration:
///
/// | Harness | Source |
/// | --- | --- |
/// | any | `transcripts/**` |
/// | Spiron | `config/sessions.sqlite3` (`session_entries`); legacy `config/sylphx-transcript.jsonl` |
/// | Claude Code | `config/{,.claude/}projects/*/*.jsonl` and `projects/*/*/subagents/*.jsonl` |
/// | Codex | `config/sessions/**/*.jsonl` (`CODEX_HOME`) |
/// | OpenCode | `config/data-home/opencode/opencode.db`, `config/.local/share/opencode/opencode.db` (`session_message`) |
/// | AGY | `config/.gemini/antigravity-cli/brain/*/.system_generated/logs/transcript_full.jsonl` |
/// | Muse | `config/data-home/muse/sessions/**/session.jsonl`, subagents included |
/// | OMP | `config/.omp/agent/sessions/*/*.jsonl` (`PI_CODING_AGENT_DIR`) |
/// | Grok | `config/.grok/sessions/*/*/chat_history.jsonl` |
///
/// dsh keeps zstd-compressed `session.jsonl.zstd`; it is not read until a
/// raw coordinate can address bytes inside a compressed frame.
fn native_sources(walk: &mut Walk, root: &Path) -> io::Result<Vec<NativeSource>> {
    let jsonl = |path: PathBuf| NativeSource {
        path,
        format: NativeFormat::Jsonl,
    };
    let mut result: Vec<NativeSource> = regular_files(walk, &root.join("transcripts"), true)?
        .into_iter()
        .map(jsonl)
        .collect();
    let config = root.join("config");
    if !safe_components(walk, root, &config)? {
        return Ok(result);
    }
    let file = |walk: &mut Walk, path: &Path| -> io::Result<bool> {
        Ok(safe_components(walk, root, path.parent().unwrap_or(root))?
            && fs::symlink_metadata(path).is_ok_and(|m| m.is_file() && !m.file_type().is_symlink()))
    };
    // One harness location; a directory the server may not read is empty.
    let mut add = |found: io::Result<Vec<NativeSource>>| -> io::Result<()> {
        match found {
            Ok(found) => {
                result.extend(found);
                Ok(())
            }
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => Ok(()),
            Err(e) => Err(e),
        }
    };
    let sylphx = config.join("sylphx-transcript.jsonl");
    add(file(walk, &sylphx).map(|found| found.then(|| jsonl(sylphx)).into_iter().collect()))?;
    for (db, kind) in [
        (config.join("sessions.sqlite3"), StoreKind::SpironSessions),
        (
            config.join("data-home/opencode/opencode.db"),
            StoreKind::OpenCodeMessages,
        ),
        (
            config.join(".local/share/opencode/opencode.db"),
            StoreKind::OpenCodeMessages,
        ),
    ] {
        add(file(walk, &db).map(|found| {
            found
                .then_some(NativeSource {
                    path: db,
                    format: NativeFormat::Store(kind),
                })
                .into_iter()
                .collect()
        }))?;
    }
    // Claude Code: one JSONL per session in each project directory, and each
    // session's subagents below it. No guessed personal home is read.
    for projects in [config.join("projects"), config.join(".claude/projects")] {
        let found = subdirectories(walk, root, &projects);
        let Some(projects) = tolerate(found)? else {
            continue;
        };
        for project in projects {
            add(jsonl_files(walk, &project, false).map(|p| p.into_iter().map(jsonl).collect()))?;
            let Some(sessions) = tolerate(subdirectories(walk, root, &project))? else {
                continue;
            };
            for session in sessions {
                let subagents = session.join("subagents");
                add(jsonl_files(walk, &subagents, false)
                    .map(|p| p.into_iter().map(jsonl).collect()))?;
            }
        }
    }
    let codex = config.join("sessions");
    add(safe_components(walk, root, &codex).and_then(|safe| {
        if !safe {
            return Ok(Vec::new());
        }
        Ok(jsonl_files(walk, &codex, true)?
            .into_iter()
            .map(|path| NativeSource {
                path,
                format: NativeFormat::CodexRollout,
            })
            .collect())
    }))?;
    let brain = config.join(".gemini/antigravity-cli/brain");
    if let Some(conversations) = tolerate(subdirectories(walk, root, &brain))? {
        for conversation in conversations {
            let transcript = conversation.join(".system_generated/logs/transcript_full.jsonl");
            add(file(walk, &transcript).map(|found| {
                found
                    .then_some(NativeSource {
                        path: transcript,
                        format: NativeFormat::AgyTranscript,
                    })
                    .into_iter()
                    .collect()
            }))?;
        }
    }
    let muse = config.join("data-home/muse/sessions");
    add(safe_components(walk, root, &muse).and_then(|safe| {
        if !safe {
            return Ok(Vec::new());
        }
        // Hidden directories hold Muse's view snapshots, not the session log.
        Ok(regular_files(walk, &muse, true)?
            .into_iter()
            .filter(|p| {
                p.file_name().is_some_and(|n| n == "session.jsonl")
                    && p.strip_prefix(&muse).is_ok_and(|rel| {
                        !rel.components()
                            .any(|c| c.as_os_str().to_string_lossy().starts_with('.'))
                    })
            })
            .map(jsonl)
            .collect())
    }))?;
    if let Some(cwds) = tolerate(subdirectories(
        walk,
        root,
        &config.join(".omp/agent/sessions"),
    ))? {
        for cwd in cwds {
            add(jsonl_files(walk, &cwd, false).map(|p| p.into_iter().map(jsonl).collect()))?;
        }
    }
    if let Some(cwds) = tolerate(subdirectories(walk, root, &config.join(".grok/sessions")))? {
        for cwd in cwds {
            let Some(sessions) = tolerate(subdirectories(walk, root, &cwd))? else {
                continue;
            };
            for session in sessions {
                let history = session.join("chat_history.jsonl");
                add(file(walk, &history)
                    .map(|found| found.then(|| jsonl(history)).into_iter().collect()))?;
            }
        }
    }
    result.sort_by(|a, b| a.path.cmp(&b.path));
    result.dedup_by(|a, b| a.path == b.path);
    Ok(result)
}

/// `Some(found)`, or `None` when a harness directory may not be read.
fn tolerate<T>(found: io::Result<T>) -> io::Result<Option<T>> {
    match found {
        Ok(found) => Ok(Some(found)),
        Err(e) if e.kind() == io::ErrorKind::PermissionDenied => Ok(None),
        Err(e) => Err(e),
    }
}

/// Directories directly below `dir`, reached without a symlink. Hidden
/// directories (a harness's own snapshots and caches) are not history.
fn subdirectories(walk: &mut Walk, root: &Path, dir: &Path) -> io::Result<Vec<PathBuf>> {
    if !safe_components(walk, root, dir)? {
        return Ok(Vec::new());
    }
    let meta = fs::symlink_metadata(dir)?;
    if !meta.is_dir() {
        return Ok(Vec::new());
    }
    let mut result = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() && !entry.file_name().to_string_lossy().starts_with('.') {
            let path = entry.path();
            if let Ok(meta) = fs::symlink_metadata(&path) {
                walk.note_dir(&path, &meta);
            }
            result.push(path);
        }
    }
    result.sort();
    Ok(result)
}

/// `*.jsonl` regular files in `dir`, optionally below it too.
fn jsonl_files(walk: &mut Walk, dir: &Path, recursive: bool) -> io::Result<Vec<PathBuf>> {
    Ok(regular_files(walk, dir, recursive)?
        .into_iter()
        .filter(|p| p.extension().is_some_and(|e| e == "jsonl"))
        .collect())
}

fn event_metadata(owner: &Owner, value: &EventHeader, format: NativeFormat) -> IndexMetadata {
    // Only an enveloped format's body is read; elsewhere `data` is payload.
    let body =
        matches!(format.envelope(), Envelope::Codex | Envelope::Spiron).then_some(&value.data);
    let role = value
        .role
        .as_deref()
        .or_else(|| value.message.as_ref().and_then(|m| m.role.as_deref()))
        .or_else(|| body.and_then(|b| b.role.as_deref()))
        .or(match (format, value.kind.as_deref()) {
            (NativeFormat::Store(StoreKind::SpironSessions), Some("tool_call")) => {
                Some("assistant")
            }
            (NativeFormat::Store(StoreKind::SpironSessions), Some("tool_result")) => Some("tool"),
            (NativeFormat::AgyTranscript, Some("USER_INPUT")) => Some("user"),
            _ => None,
        })
        .unwrap_or("system");
    let tool = value
        .tool_name
        .clone()
        .or_else(|| value.tool.clone())
        .or_else(|| body.and_then(|b| b.tool_name.clone()))
        .or_else(|| {
            (role == "tool").then(|| value.name.clone().unwrap_or_else(|| "unknown".into()))
        });
    let direction = value
        .direction
        .as_deref()
        .unwrap_or(if role == "user" { "in" } else { "out" });
    IndexMetadata {
        agent_id: owner.id.to_string(),
        source: MemoryScope::Transcripts,
        timestamp_ms: value.timestamp_ms.or_else(|| {
            value
                .timestamp
                .as_deref()
                .or(match format {
                    NativeFormat::AgyTranscript => value.created_at.as_deref(),
                    _ => None,
                })
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|time| time.timestamp_millis())
        }),
        direction: direction.into(),
        sender: value.sender.clone().unwrap_or_else(|| role.into()),
        destination: value
            .destination
            .clone()
            .unwrap_or_else(|| format!("agent:{}", owner.name)),
        tool_names: tool.iter().cloned().collect(),
        tool_name: tool,
        role: Some(role.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NewAgent;
    use std::cell::Cell;

    thread_local! {
        /// Class refreshes that walked the tree.
        pub(super) static CLASS_WALKS: Cell<usize> = const { Cell::new(0) };
        /// A smaller batch, so a small fixture spans several batches.
        pub(super) static BATCH_BYTES_OVERRIDE: Cell<Option<u64>> = const { Cell::new(None) };
    }

    /// A search touches an unchanged agent with a stat per recorded entry:
    /// no lock, no directory listing, no file open. Any change it could miss
    /// (a file, a directory entry, the name, the exclusions, the documents)
    /// walks only the class that changed, and results stay the same.
    #[test]
    fn unchanged_agent_history_is_searched_without_rewalking() {
        use std::io::Write;
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("settled-archive", "spiron"))
            .unwrap();
        let absent = store
            .create_agent(NewAgent::new("never-ran", "spiron"))
            .unwrap();
        for id in [&agent.id, &absent.id] {
            store.set_agent_state(id, "archived").unwrap();
        }
        let root = dir.path().join("agents").join(agent.id.as_str());
        fs::create_dir_all(root.join("memory")).unwrap();
        fs::create_dir_all(root.join("config/projects/p")).unwrap();
        fs::write(root.join("memory/MEMORY.md"), "settledneedle memory").unwrap();
        let native = root.join("config/projects/p/session.jsonl");
        fs::write(
            &native,
            "{\"role\":\"user\",\"content\":\"settledneedle transcript\"}\n",
        )
        .unwrap();
        let ids = [agent.id.to_string(), absent.id.to_string()];
        let walks = || CLASS_WALKS.with(Cell::get);
        // Searches run "later" so the stamps written here have settled.
        let mut at = crate::now_ms() + 10_000;
        let mut search = |term: &str| {
            at += 1_000;
            store
                .search_host_files_at(
                    &ids,
                    MemoryScope::All,
                    &HistorySearchQuery {
                        query: Some(term.into()),
                        sort: crate::HistorySort::Oldest,
                        ..Default::default()
                    },
                    at,
                    u64::MAX,
                )
                .unwrap()
                .0
                .into_iter()
                .map(|(hit, bytes)| (hit.id, hit.metadata.sender, bytes))
                .collect::<Vec<_>>()
        };
        let start = walks();
        let first = search("settledneedle");
        assert_eq!(first.len(), 2);
        assert_eq!(walks() - start, 4, "two classes of two agents");
        let start = walks();
        assert_eq!(search("settledneedle"), first);
        assert_eq!(walks(), start, "nothing changed, nothing walked");

        // An append changes the transcript's stamp: only that class walks.
        fs::OpenOptions::new()
            .append(true)
            .open(&native)
            .unwrap()
            .write_all(b"{\"role\":\"user\",\"content\":\"appendedneedle\"}\n")
            .unwrap();
        assert_eq!(search("appendedneedle").len(), 1);
        assert_eq!(walks() - start, 1);
        // A new file in a nested directory changes that directory's stamp.
        let start = walks();
        fs::create_dir_all(root.join("memory/topics")).unwrap();
        fs::write(root.join("memory/topics/first.md"), "nestedneedle one").unwrap();
        assert_eq!(search("nestedneedle").len(), 1);
        fs::write(root.join("memory/topics/second.md"), "nestedneedle two").unwrap();
        assert_eq!(search("nestedneedle").len(), 2);
        assert_eq!(walks() - start, 2);
        assert_eq!(search("nestedneedle").len(), 2);
        assert_eq!(walks() - start, 2);
        // A rename changes every policy that names the agent.
        let start = walks();
        store.rename_agent(&agent.id, "renamed-archive").unwrap();
        let renamed = search("appendedneedle");
        assert_eq!(renamed[0].1, "user");
        assert_eq!(search("nestedneedle")[0].1, "agent:renamed-archive");
        assert_eq!(walks() - start, 2);
        // Dropping the agent's documents (as a redaction does) forgets its walks.
        let start = walks();
        HostIndex::open(&dir.path().join(INDEX_FILE))
            .unwrap()
            .retain_agent_documents(agent.id.as_str(), &HashSet::new())
            .unwrap();
        assert_eq!(search("nestedneedle").len(), 2);
        assert_eq!(walks() - start, 2);
        // A walk right after a change cannot trust stamps the filesystem may
        // still reuse, so the next search walks again until they settle.
        let start = walks();
        fs::write(root.join("memory/topics/third.md"), "nestedneedle three").unwrap();
        let now = crate::now_ms();
        for _ in 0..2 {
            let hits = store
                .search_host_files_at(
                    &ids,
                    MemoryScope::Memory,
                    &HistorySearchQuery {
                        query: Some("nestedneedle".into()),
                        ..Default::default()
                    },
                    now,
                    u64::MAX,
                )
                .unwrap()
                .0;
            assert_eq!(hits.len(), 3);
        }
        assert_eq!(walks() - start, 2);
    }
    #[test]
    fn native_tool_correlation_survives_reopen_and_is_scoped_atomic_and_rebuildable() {
        use std::io::Write;
        let dir = crate::test_tempdir();
        let db = dir.path().join("state.db");
        let store = Store::open(&db).unwrap();
        let own = store
            .create_agent(NewAgent::new("tool-owner", "spiron"))
            .unwrap();
        let peer = store
            .create_agent(NewAgent::new("tool-peer", "spiron"))
            .unwrap();
        let path = |agent: &Agent| {
            dir.path()
                .join("agents")
                .join(agent.id.as_str())
                .join("config/sylphx-transcript.jsonl")
        };
        let native = path(&own);
        fs::create_dir_all(native.parent().unwrap()).unwrap();
        // Source-shaped synthetic Responses/Claude formats; not captured runtime data.
        fs::write(&native, concat!(
            "{\"type\":\"function_call\",\"call_id\":\"same\",\"name\":\"browser\",\"arguments\":\"{}\"}\n",
            "{\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"ordinary\"},{\"type\":\"tool_use\",\"id\":\"claude1\",\"name\":\"shell\",\"input\":{}},{\"type\":\"tool_use\",\"id\":\"claude2\",\"name\":\"files\",\"input\":{}}]}}\n"
        )).unwrap();
        let foreign = path(&peer);
        fs::create_dir_all(foreign.parent().unwrap()).unwrap();
        fs::write(
            &foreign,
            "{\"type\":\"function_call\",\"call_id\":\"same\",\"name\":\"foreign\"}\n",
        )
        .unwrap();
        let ids = [own.id.to_string(), peer.id.to_string()];
        store
            .search_host_files(
                &ids,
                MemoryScope::Transcripts,
                &HistorySearchQuery::default(),
            )
            .unwrap();
        drop(store);
        let store = Store::open(&db).unwrap();
        let huge = r#"escaped \"☃\\"#.repeat(20_000);
        let output = format!(
            "{{\"output\":{},\"type\":\"function_call_output\",\"call_id\":\"same\"}}\n",
            serde_json::to_string(&huge).unwrap()
        );
        let claude = "{\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"mixed\"},{\"type\":\"tool_result\",\"tool_use_id\":\"claude1\",\"content\":\"result\"}]}}\n";
        fs::OpenOptions::new()
            .append(true)
            .open(&native)
            .unwrap()
            .write_all(format!("{output}{claude}").as_bytes())
            .unwrap();
        let other = native
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("transcripts/other.jsonl");
        fs::create_dir_all(other.parent().unwrap()).unwrap();
        fs::write(
            &other,
            "{\"type\":\"function_call_output\",\"call_id\":\"same\",\"output\":\"unresolved\"}\n",
        )
        .unwrap();
        let search = |name: Option<&str>, include_tools, only_tools| {
            store
                .search_host_files(
                    &[own.id.to_string()],
                    MemoryScope::Transcripts,
                    &HistorySearchQuery {
                        tool_name: name.map(str::to_owned),
                        include_tools,
                        only_tools,
                        ..Default::default()
                    },
                )
                .unwrap()
        };
        let browser = search(Some("BROWSER"), false, false);
        assert_eq!(browser.len(), 2);
        let result = browser.iter().find(|(h, _)| h.raw.seq == 3).unwrap();
        assert_eq!(read_raw(&result.0.raw).unwrap(), output.as_bytes());
        assert_eq!(result.0.metadata.timestamp_ms, None);
        let shell = search(Some("shell"), true, false);
        assert_eq!(shell.len(), 2);
        let multi = shell.iter().find(|(h, _)| h.raw.seq == 2).unwrap();
        assert_eq!(multi.0.metadata.tool_names, vec!["files", "shell"]);
        assert_eq!(search(Some("files"), true, false).len(), 1);
        assert_eq!(search(Some("foreign"), true, false).len(), 0);
        assert_eq!(search(Some("unknown"), true, false).len(), 1);
        assert_eq!(search(None, true, true).len(), 5);
        assert!(search(None, false, false).is_empty());
        let mut index = HostIndex::open(&dir.path().join(INDEX_FILE)).unwrap();
        let cursor = index.source_cursor(own.id.as_str(), &native).unwrap();
        let doc = browser[0].0.id.clone();
        let failure = index
            .refresh_source(|index| -> Result<(), IndexError> {
                index.save_call(own.id.as_str(), &native, "injected", "wrong", &doc)?;
                index.clear_source(own.id.as_str(), &native)?;
                Err(io::Error::other("injected metadata rollback").into())
            })
            .unwrap_err();
        assert!(failure.to_string().contains("injected metadata rollback"));
        assert!(
            index
                .call_tool(own.id.as_str(), &native, "injected")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            index
                .call_tool(own.id.as_str(), &native, "same")
                .unwrap()
                .as_deref(),
            Some("browser")
        );
        assert_eq!(
            serde_json::to_string(&cursor).unwrap(),
            serde_json::to_string(&index.source_cursor(own.id.as_str(), &native).unwrap()).unwrap()
        );
        assert_eq!(
            index
                .call_tool(peer.id.as_str(), &foreign, "same")
                .unwrap()
                .as_deref(),
            Some("foreign")
        );
        // A valid earlier append plus excessive later metadata must publish
        // neither the new correlation nor an advanced source cursor.
        let excessive = serde_json::json!({"content": (0..1025).map(|_| serde_json::json!({"type":"tool_use","id":"too-many","name":"wrong"})).collect::<Vec<_>>()});
        fs::OpenOptions::new().append(true).open(&native).unwrap().write_all(format!("{{\"type\":\"function_call\",\"call_id\":\"partial\",\"name\":\"not-published\"}}\n{excessive}\n").as_bytes()).unwrap();
        let failure = refresh_native(&mut index, &Owner::from(&own), &native)
            .expect_err("metadata cap must refuse refresh");
        assert!(
            failure
                .to_string()
                .contains("native event exceeds tool metadata limit")
        );
        assert!(
            index
                .call_tool(own.id.as_str(), &native, "partial")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            serde_json::to_string(&cursor).unwrap(),
            serde_json::to_string(&index.source_cursor(own.id.as_str(), &native).unwrap()).unwrap()
        );
        // Replacement must discard old call correlations, not attach stale names.
        fs::write(&native, &output).unwrap();
        refresh_native(&mut index, &Owner::from(&own), &native).unwrap();
        assert!(
            index
                .call_tool(own.id.as_str(), &native, "same")
                .unwrap()
                .is_none()
        );
        assert_eq!(search(Some("unknown"), true, false).len(), 2);
        index
            .retain_agent_documents(own.id.as_str(), &HashSet::new())
            .unwrap();
        assert!(
            index
                .source_cursor(own.id.as_str(), &native)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            index
                .call_tool(peer.id.as_str(), &foreign, "same")
                .unwrap()
                .as_deref(),
            Some("foreign")
        );
    }

    /// A refresh walks the path once for all its reads (plus one replacement
    /// check) and reads O(bytes / window), never once per record or per byte.
    #[test]
    fn refresh_reads_each_source_through_one_descriptor_and_buffer() {
        use crate::raw_segment::counters::snapshot;
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("buffered-reader", "spiron"))
            .unwrap();
        let path = dir.path().join("native.jsonl");
        let mut text = String::new();
        for i in 0..4_000 {
            text.push_str(&format!(
                "{{\"role\":\"user\",\"content\":\"record{i} {}\"}}\n",
                "word ".repeat(i % 50)
            ));
        }
        text.push_str(&format!(
            "{{\"role\":\"tool\",\"output\":{}}}\n",
            serde_json::json!("large ".repeat(300_000))
        ));
        fs::write(&path, &text).unwrap();
        let mut index = HostIndex::open(&dir.path().join("index.db")).unwrap();
        let delta = |before: (usize, usize)| {
            let after = snapshot();
            (after.0 - before.0, after.1 - before.1)
        };
        let before = snapshot();
        let stats = refresh_native(&mut index, &Owner::from(&agent), &path).unwrap();
        let (opens, reads) = delta(before);
        assert_eq!(stats.indexed_records, 4_001);
        assert_eq!(opens, 2);
        let windows = text.len() / 65_536 + 1;
        assert!(
            reads <= 4 * windows,
            "{reads} reads for {} bytes in 4001 records",
            text.len()
        );
        let hits = index
            .search(
                &[agent.id.to_string()],
                MemoryScope::Transcripts,
                &HistorySearchQuery {
                    query: Some("record3999".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].raw.seq, 4_000);
        // Unchanged: one walk to stamp the file, no reads.
        let before = snapshot();
        refresh_native(&mut index, &Owner::from(&agent), &path).unwrap();
        assert_eq!(delta(before), (1, 0));
        // A whole-file document hashes and projects from one descriptor too.
        let memory = dir.path().join("MEMORY.md");
        fs::write(&memory, "memoryneedle ".repeat(50_000)).unwrap();
        let metadata = IndexMetadata {
            agent_id: agent.id.to_string(),
            source: MemoryScope::Memory,
            timestamp_ms: Some(0),
            direction: "in".into(),
            sender: "owner".into(),
            destination: "owner".into(),
            tool_name: None,
            tool_names: Vec::new(),
            role: Some("memory".into()),
        };
        let before = snapshot();
        refresh_document(&mut index, "memory:buffered", &metadata, &memory).unwrap();
        let (opens, reads) = delta(before);
        assert_eq!(opens, 2);
        assert!(reads <= 4, "{reads}");
        let before = snapshot();
        refresh_document(&mut index, "memory:buffered", &metadata, &memory).unwrap();
        assert_eq!(delta(before), (1, 0));
    }

    #[test]
    fn native_times_preserve_explicit_values_and_leave_absent_or_invalid_unknown() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("native-time", "spiron"))
            .unwrap();
        let path = dir
            .path()
            .join("agents")
            .join(agent.id.as_str())
            .join("config/sylphx-transcript.jsonl");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        // Source-shaped synthetic fixtures, not captured runtime transcripts.
        fs::write(&path, concat!(
            "{\"role\":\"user\",\"timestamp\":\"2026-11-07T04:00:00.123Z\",\"content\":\"timefixture iso\"}\n",
            "{\"role\":\"user\",\"timestamp_ms\":0,\"content\":\"timefixture epoch\"}\n",
            "{\"role\":\"user\",\"content\":\"timefixture absent\"}\n",
            "{\"role\":\"user\",\"timestamp\":\"invalid\",\"content\":\"timefixture invalid\"}\n"
        )).unwrap();
        let ids = [agent.id.to_string()];
        let query = HistorySearchQuery {
            query: Some("timefixture".into()),
            sort: crate::HistorySort::Oldest,
            ..Default::default()
        };
        let hits = store
            .search_host_files(&ids, MemoryScope::Transcripts, &query)
            .unwrap();
        assert_eq!(hits.len(), 4);
        for (needle, expected) in [
            ("iso", Some(1794024000123)),
            ("epoch", Some(0)),
            ("absent", None),
            ("invalid", None),
        ] {
            let hit = hits
                .iter()
                .find(|(_, b)| String::from_utf8_lossy(b).contains(needle))
                .unwrap();
            assert_eq!(hit.0.metadata.timestamp_ms, expected, "{needle}");
        }
        drop(store);
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let filtered = store
            .search_host_files(
                &ids,
                MemoryScope::Transcripts,
                &HistorySearchQuery {
                    since_ms: Some(-1),
                    until_ms: Some(1),
                    ..query
                },
            )
            .unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].0.metadata.timestamp_ms, Some(0));
        assert_eq!(
            serde_json::to_value(
                &hits
                    .iter()
                    .find(|(h, _)| h.metadata.timestamp_ms.is_none())
                    .unwrap()
                    .0
                    .metadata
            )
            .unwrap()["timestamp_ms"],
            serde_json::Value::Null
        );
    }

    #[test]
    fn reconciled_non_jsonl_history_is_full_text_scoped_and_read_back_losslessly() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let own = store
            .create_agent(NewAgent::new("page-owner", "spiron"))
            .unwrap();
        let hidden = store
            .create_agent(NewAgent::new("page-hidden", "spiron"))
            .unwrap();
        let mut own_path = PathBuf::new();
        for agent in [&own, &hidden] {
            let root = dir
                .path()
                .join("agents")
                .join(agent.id.as_str())
                .join("history/archive/pagehash");
            fs::create_dir_all(&root).unwrap();
            let path = root.join("history.md");
            fs::write(
                &path,
                format!(
                    "# historical page\n{} pagetailneedle",
                    "padding ".repeat(2000)
                ),
            )
            .unwrap();
            if agent.id == own.id {
                own_path = path;
            }
        }
        let ids = [own.id.to_string()];
        let q = HistorySearchQuery {
            query: Some("pagetailneedle".into()),
            ..Default::default()
        };
        assert!(
            store
                .search_host_files(&ids, MemoryScope::Memory, &q)
                .unwrap()
                .is_empty()
        );
        let hits = store
            .search_host_files(&ids, MemoryScope::Transcripts, &q)
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0.metadata.agent_id, own.id.as_str());
        assert_eq!(hits[0].0.metadata.role.as_deref(), Some("history"));
        assert_eq!(
            read_raw(&hits[0].0.raw).unwrap(),
            fs::read(&own_path).unwrap()
        );
        assert!(String::from_utf8_lossy(&hits[0].1).contains("pagetailneedle"));
        assert!(
            store
                .search_host_files(&[], MemoryScope::All, &q)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn boundary_fast_path_reads_bounded_prefix_and_periodic_verifier_recovers_older_edits() {
        use std::io::{Seek, SeekFrom, Write};
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("boundary-owner", "spiron"))
            .unwrap();
        let root = dir.path().join("agents").join(agent.id.as_str());
        fs::create_dir_all(root.join("config")).unwrap();
        let path = root.join("config/sylphx-transcript.jsonl");
        let prefix = format!(
            "{{\"content\":\"oldneedle {}\"}}\n",
            "padding ".repeat(40_000)
        );
        fs::write(&path, &prefix).unwrap();
        let index_path = dir.path().join(INDEX_FILE);
        let mut index = HostIndex::open(&index_path).unwrap();
        let t0 = crate::now_ms();
        refresh_native_at(
            &mut index,
            &Owner::from(&agent),
            &path,
            NativeFormat::Jsonl,
            false,
            false,
            t0,
        )
        .unwrap();
        let suffix = b"{\"content\":\"suffixneedle\"}\n";
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(suffix)
            .unwrap();
        let append = refresh_native_at(
            &mut index,
            &Owner::from(&agent),
            &path,
            NativeFormat::Jsonl,
            false,
            false,
            t0 + 1,
        )
        .unwrap();
        assert_eq!(
            append.prefix_verified_bytes,
            crate::host_cursor::BOUNDARY_BYTES
        );
        assert_eq!(
            append.boundary_capture_bytes,
            crate::host_cursor::BOUNDARY_BYTES
        );
        assert_eq!(append.suffix_scanned_bytes, suffix.len() as u64);
        assert_eq!(append.indexed_records, 1);
        assert_eq!(
            index
                .source_documents(agent.id.as_str(), &path)
                .unwrap()
                .len(),
            2
        );
        // Same-size edit inside the last boundary must trigger full replacement.
        let mut file = fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(prefix.len() as u64 + 12))
            .unwrap();
        file.write_all(b"inside").unwrap();
        let inside = refresh_native_at(
            &mut index,
            &Owner::from(&agent),
            &path,
            NativeFormat::Jsonl,
            false,
            false,
            t0 + 2,
        )
        .unwrap();
        assert_eq!(inside.indexed_records, 2);
        assert_eq!(
            inside.suffix_scanned_bytes,
            (prefix.len() + suffix.len()) as u64
        );
        // An old edit outside the boundary is the documented delayed-detection
        // case, even when an append follows it. Per-hit SHA still refuses stale
        // returned bytes immediately, before the full verifier catches up.
        file.seek(SeekFrom::Start(12)).unwrap();
        file.write_all(b"newneedle").unwrap();
        let mut append_file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        append_file.write_all(suffix).unwrap();
        let outside = refresh_native_at(
            &mut index,
            &Owner::from(&agent),
            &path,
            NativeFormat::Jsonl,
            false,
            false,
            t0 + 3,
        )
        .unwrap();
        assert_eq!(outside.indexed_records, 1);
        assert_eq!(
            outside.prefix_verified_bytes,
            crate::host_cursor::BOUNDARY_BYTES
        );
        let query = |term: &str| HistorySearchQuery {
            query: Some(term.into()),
            ..Default::default()
        };
        let ids = [agent.id.to_string()];
        assert!(
            index
                .search(&ids, MemoryScope::Transcripts, &query("newneedle"))
                .unwrap()
                .is_empty()
        );
        let stale = index
            .search(&ids, MemoryScope::Transcripts, &query("oldneedle"))
            .unwrap();
        assert_eq!(stale.len(), 1);
        assert!(read_raw(&stale[0].window).is_err());
        drop(index);
        assert!(
            !store
                .verify_one_host_history_source(t0 + 3_599_000)
                .unwrap()
        );
        assert!(
            store
                .verify_one_host_history_source(t0 + 3_600_003)
                .unwrap()
        );
        // The maintenance tick only schedules it; the background indexer
        // reads the source and repairs what changed.
        assert!(store.index_host_history_step(u64::MAX).unwrap());
        let mut index = HostIndex::open(&index_path).unwrap();
        assert!(
            index
                .search(&ids, MemoryScope::Transcripts, &query("oldneedle"))
                .unwrap()
                .is_empty()
        );
        let fresh = index
            .search(&ids, MemoryScope::Transcripts, &query("newneedle"))
            .unwrap();
        assert_eq!(fresh.len(), 1);
        assert!(
            String::from_utf8_lossy(&read_raw(&fresh[0].window).unwrap()).contains("newneedle")
        );
        // Shrink and a new inode use the complete-source path, not suffix reuse.
        fs::write(&path, suffix).unwrap();
        let shrink = refresh_native_at(
            &mut index,
            &Owner::from(&agent),
            &path,
            NativeFormat::Jsonl,
            false,
            false,
            t0 + 3_600_004,
        )
        .unwrap();
        assert_eq!(
            (
                shrink.prefix_verified_bytes,
                shrink.suffix_scanned_bytes,
                shrink.indexed_records
            ),
            (0, suffix.len() as u64, 1)
        );
        let replacement = root.join("replacement");
        fs::write(&replacement, &prefix).unwrap();
        fs::rename(&replacement, &path).unwrap();
        let replaced = refresh_native_at(
            &mut index,
            &Owner::from(&agent),
            &path,
            NativeFormat::Jsonl,
            false,
            false,
            t0 + 3_600_005,
        )
        .unwrap();
        assert_eq!(
            (
                replaced.prefix_verified_bytes,
                replaced.suffix_scanned_bytes,
                replaced.indexed_records
            ),
            (0, prefix.len() as u64, 1)
        );
    }

    /// A transcript of `count` records, each naming its number.
    fn numbered_transcript(path: &Path, count: usize) -> Vec<u8> {
        let mut bytes = Vec::new();
        for i in 0..count {
            bytes.extend_from_slice(
                format!(
                    "{{\"role\":\"user\",\"content\":\"record{i:04} {}\"}}\n",
                    "padding ".repeat(24)
                )
                .as_bytes(),
            );
        }
        fs::write(path, &bytes).unwrap();
        bytes
    }

    /// (a) Reading and projecting a source happen with no lifecycle lock and
    /// no index write transaction held: the gate that takes the lock for a
    /// batch is called only once the batch is read, the index accepts
    /// another writer at that moment, and each batch reads a bounded share.
    /// While the gate is held, the only read is the boundary check.
    #[test]
    fn indexing_reads_with_no_lock_or_write_and_publishes_bounded_batches() {
        use std::cell::RefCell;
        use std::rc::Rc;
        BATCH_BYTES_OVERRIDE.with(|b| b.set(Some(4096)));
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("batches", "spiron"))
            .unwrap();
        let root = dir
            .path()
            .join("agents")
            .join(agent.id.as_str())
            .join("transcripts");
        fs::create_dir_all(&root).unwrap();
        let path = root.join("a.jsonl");
        let bytes = numbered_transcript(&path, 60);
        let index_path = dir.path().join(INDEX_FILE);
        let mut index = HostIndex::open(&index_path).unwrap();

        let prepared = || crate::host_index::PREPARED.with(Cell::get);
        struct Held(Rc<RefCell<Vec<(usize, usize)>>>, usize);
        impl Drop for Held {
            fn drop(&mut self) {
                let now = crate::host_index::PREPARED.with(Cell::get);
                self.0.borrow_mut().push((self.1, now));
            }
        }
        let holds = Rc::new(RefCell::new(Vec::new()));
        let mut published_before = Vec::new();
        let mut gate = || -> Result<Option<Box<dyn std::any::Any>>, IndexError> {
            // Nothing holds this owner's lifecycle while a batch is read.
            let lock = store.try_lock_agent_lifecycle(&agent.id).unwrap();
            // Nor the index's write lock: another writer gets it at once.
            let other = rusqlite::Connection::open(&index_path).unwrap();
            other.busy_timeout(std::time::Duration::ZERO).unwrap();
            other.execute_batch("BEGIN IMMEDIATE; ROLLBACK;").unwrap();
            published_before.push(
                HostIndex::open(&index_path)
                    .unwrap()
                    .source_documents(agent.id.as_str(), &path)
                    .unwrap()
                    .len(),
            );
            // This batch was read and projected before the gate.
            let at = prepared();
            let last = holds.borrow().last().map_or(0, |h: &(usize, usize)| h.1);
            assert!(at > last, "a batch is projected before its gate");
            Ok(Some(Box::new((lock, Held(Rc::clone(&holds), at)))))
        };
        let mut budget = u64::MAX;
        let stats = refresh_native_ctx(
            &mut index,
            &Owner::from(&agent),
            &path,
            NativeFormat::Jsonl,
            false,
            false,
            crate::now_ms(),
            &mut budget,
            &mut gate,
        )
        .unwrap();
        assert!(stats.complete);
        assert_eq!(stats.indexed_records, 60);
        let record = bytes.len() as u64 / 60;
        let batches = bytes.len() as u64 / (4096 + record) + 1;
        let holds = holds.borrow();
        assert!(holds.len() as u64 >= batches, "{} batches", holds.len());
        // Every batch read its records before taking the gate, and published
        // after: documents grow batch by batch, never all at once.
        assert_eq!(published_before[0], 0);
        assert!(published_before.windows(2).all(|w| w[0] < w[1]));
        assert!(published_before.last().unwrap() < &60);
        for (taken, released) in holds.iter() {
            assert_eq!(
                taken, released,
                "nothing is read or projected under the gate"
            );
        }
        let cursor = index
            .source_cursor(agent.id.as_str(), &path)
            .unwrap()
            .unwrap();
        assert!(cursor.complete && !cursor.rebuilding);
        assert_eq!(cursor.end, bytes.len() as u64);
        assert_eq!(
            index
                .source_documents(agent.id.as_str(), &path)
                .unwrap()
                .len(),
            60
        );
        BATCH_BYTES_OVERRIDE.with(|b| b.set(None));
    }

    /// (a) A batch is published only if the source still holds what was
    /// read. A batch read before a redaction (the gate refuses it) or before
    /// an in-place rewrite is dropped and its frontier kept; an append by a
    /// live writer is not a change to the records read. The next refresh
    /// resumes from the last published batch.
    #[test]
    fn a_batch_publishes_only_while_its_source_and_owner_are_unchanged() {
        use std::io::Write;
        BATCH_BYTES_OVERRIDE.with(|b| b.set(Some(4096)));
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("gated", "spiron"))
            .unwrap();
        let owner = Owner::from(&agent);
        let root = dir
            .path()
            .join("agents")
            .join(agent.id.as_str())
            .join("transcripts");
        fs::create_dir_all(&root).unwrap();
        let path = root.join("a.jsonl");
        let bytes = numbered_transcript(&path, 60);
        let mut index = HostIndex::open(&dir.path().join(INDEX_FILE)).unwrap();
        let refresh = |index: &mut HostIndex, gate: &mut Gate<'_>| {
            let mut budget = u64::MAX;
            refresh_native_ctx(
                index,
                &owner,
                &path,
                NativeFormat::Jsonl,
                false,
                false,
                crate::now_ms(),
                &mut budget,
                gate,
            )
        };
        // The owner changed under the second batch: it is dropped.
        let mut calls = 0;
        let stats = refresh(&mut index, &mut || {
            calls += 1;
            Ok((calls == 1).then(|| Box::new(()) as Box<dyn std::any::Any>))
        })
        .unwrap();
        assert!(!stats.complete);
        assert!(stats.pending_bytes > 0);
        let first = index
            .source_cursor(agent.id.as_str(), &path)
            .unwrap()
            .unwrap();
        assert!(!first.complete);
        assert!(first.end > 0 && first.end < bytes.len() as u64);
        let published = index
            .source_documents(agent.id.as_str(), &path)
            .unwrap()
            .len();
        assert!(published > 0 && published < 60);

        // A rewrite of the records being read refuses the batch.
        let mut edited = bytes.clone();
        let at = first.end as usize + 20;
        edited[at..at + 6].copy_from_slice(b"EDITED");
        let error = refresh(&mut index, &mut || {
            fs::write(&path, &edited).unwrap();
            Ok(Some(Box::new(())))
        })
        .unwrap_err();
        assert!(matches!(error, IndexError::Io(ref e) if e.kind() == io::ErrorKind::WouldBlock));
        assert_eq!(
            serde_json::to_string(&index.source_cursor(agent.id.as_str(), &path).unwrap()).unwrap(),
            serde_json::to_string(&Some(first.clone())).unwrap()
        );
        assert_eq!(
            index
                .source_documents(agent.id.as_str(), &path)
                .unwrap()
                .len(),
            published
        );
        fs::write(&path, &bytes).unwrap();

        // A live append under the read publishes, and the refresh resumes
        // from the published frontier to the end.
        let mut appended = false;
        let stats = refresh(&mut index, &mut || {
            if !appended {
                appended = true;
                fs::OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .unwrap()
                    .write_all(b"{\"role\":\"user\",\"content\":\"lateneedle\"}\n")
                    .unwrap();
            }
            Ok(Some(Box::new(())))
        })
        .unwrap();
        assert!(stats.complete);
        assert_eq!(
            index
                .source_documents(agent.id.as_str(), &path)
                .unwrap()
                .len(),
            61
        );
        BATCH_BYTES_OVERRIDE.with(|b| b.set(None));
    }

    /// (b) A search indexes at most its budget of new history before it
    /// answers. The rest is queued for the background indexer and the answer
    /// says what is pending; once the indexer has run, the same search finds
    /// everything and nothing is pending. The pre-warm pass covers an agent
    /// no search has touched.
    #[test]
    fn a_search_indexes_a_bounded_delta_and_the_background_indexes_the_rest() {
        BATCH_BYTES_OVERRIDE.with(|b| b.set(Some(4096)));
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("large", "spiron"))
            .unwrap();
        let quiet = store
            .create_agent(NewAgent::new("unsearched", "spiron"))
            .unwrap();
        let root = dir
            .path()
            .join("agents")
            .join(agent.id.as_str())
            .join("transcripts");
        fs::create_dir_all(&root).unwrap();
        let bytes = numbered_transcript(&root.join("a.jsonl"), 200);
        let quiet_root = dir
            .path()
            .join("agents")
            .join(quiet.id.as_str())
            .join("transcripts");
        fs::create_dir_all(&quiet_root).unwrap();
        numbered_transcript(&quiet_root.join("q.jsonl"), 10);
        let ids = [agent.id.to_string()];
        let search = |term: &str, budget: u64| {
            store
                .search_host_files_at(
                    &ids,
                    MemoryScope::Transcripts,
                    &HistorySearchQuery {
                        query: Some(term.into()),
                        ..Default::default()
                    },
                    crate::now_ms() + 10_000,
                    budget,
                )
                .unwrap()
        };
        let budget = 8 * 1024;
        let (hits, pending) = search("record0000", budget);
        assert_eq!(hits.len(), 1, "the first records are indexed");
        assert_eq!(pending.sources, 1);
        assert!(pending.bytes >= bytes.len() as u64 - budget - 4096);
        let index = HostIndex::open(&dir.path().join(INDEX_FILE)).unwrap();
        let cursor = index
            .source_cursor(agent.id.as_str(), &root.join("a.jsonl"))
            .unwrap()
            .unwrap();
        let record = bytes.len() as u64 / 200;
        assert!(cursor.end <= budget + record, "{} > budget", cursor.end);
        assert_eq!(store.host_history_backlog(), 1);
        // A later search does not wait for the rest either.
        let (hits, pending) = search("record0199", budget);
        assert!(hits.is_empty());
        assert_eq!(pending.sources, 1);
        // The background indexer completes it in bounded steps.
        let mut steps = 0;
        while store.host_history_backlog() > 0 {
            assert!(store.index_host_history_step(16 * 1024).unwrap());
            steps += 1;
            assert!(steps < 100);
        }
        assert!(steps > 1, "a step stays within its budget");
        let (hits, pending) = search("record0199", 0);
        assert_eq!(hits.len(), 1);
        assert_eq!(pending, PendingHistory::default());

        // Pre-warm: with nothing queued, steps cover every agent once.
        while store.index_host_history_step(u64::MAX).unwrap() {}
        let quiet_ids = [quiet.id.to_string()];
        let (hits, pending) = store
            .search_host_files_at(
                &quiet_ids,
                MemoryScope::Transcripts,
                &HistorySearchQuery {
                    query: Some("record0009".into()),
                    ..Default::default()
                },
                crate::now_ms() + 10_000,
                0,
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(pending, PendingHistory::default());
        assert!(!store.index_host_history_step(u64::MAX).unwrap());
        BATCH_BYTES_OVERRIDE.with(|b| b.set(None));
    }

    /// The verifier runs on the maintenance worker beside the WAL checkpoint
    /// and outbound settlement. A held lifecycle (a delivery, search or archive
    /// of that owner) must not make it wait: before, it blocked in `flock` for
    /// as long as the holder kept the lock. It now never reads the source or
    /// takes the lock; it schedules the verification for the background
    /// indexer, which publishes once the owner is free.
    #[test]
    fn periodic_verifier_never_waits_for_a_busy_owner() {
        let dir = crate::test_tempdir();
        let store = std::sync::Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let agent = store
            .create_agent(NewAgent::new("busy-owner", "spiron"))
            .unwrap();
        let root = dir
            .path()
            .join("agents")
            .join(agent.id.as_str())
            .join("transcripts");
        fs::create_dir_all(&root).unwrap();
        let path = root.join("a.jsonl");
        fs::write(&path, "{\"content\":\"busyneedle\"}\n").unwrap();
        let index_path = dir.path().join(INDEX_FILE);
        let t0 = crate::now_ms();
        {
            let mut index = HostIndex::open(&index_path).unwrap();
            refresh_native_at(
                &mut index,
                &Owner::from(&agent),
                &path,
                NativeFormat::Jsonl,
                false,
                false,
                t0,
            )
            .unwrap();
        }
        let held = store.lock_agent_lifecycle(&agent.id).unwrap();
        let (done, outcome) = std::sync::mpsc::channel();
        let verifier = std::sync::Arc::clone(&store);
        std::thread::spawn(move || {
            let _ = done.send(verifier.verify_one_host_history_source(t0 + 3_600_001));
        });
        let scheduled = outcome
            .recv_timeout(std::time::Duration::from_secs(20))
            .expect("the verifier waited for a held lifecycle");
        assert!(scheduled.unwrap());
        assert_eq!(store.host_history_backlog(), 1);
        let index = HostIndex::open(&index_path).unwrap();
        let cursor = index
            .source_cursor(agent.id.as_str(), &path)
            .unwrap()
            .unwrap();
        assert_eq!(cursor.verification_attempt_ms, t0 + 3_600_001);
        assert!(cursor.full_verified_ms < t0 + 3_600_001);
        drop(index);
        drop(held);
        assert!(store.index_host_history_step(u64::MAX).unwrap());
        let index = HostIndex::open(&index_path).unwrap();
        let cursor = index
            .source_cursor(agent.id.as_str(), &path)
            .unwrap()
            .unwrap();
        assert_eq!(cursor.full_verified_ms, t0 + 3_600_001);
        assert_eq!(store.host_history_backlog(), 0);
    }

    #[test]
    fn periodic_verifier_rotates_failed_sources_without_advancing_their_frontier() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("verifier-owner", "spiron"))
            .unwrap();
        let root = dir
            .path()
            .join("agents")
            .join(agent.id.as_str())
            .join("transcripts");
        fs::create_dir_all(&root).unwrap();
        let bad = root.join("a.jsonl");
        let good = root.join("b.jsonl");
        fs::write(&bad, "{\"content\":\"badneedle\"}\n").unwrap();
        fs::write(&good, "{\"content\":\"goodneedle\"}\n").unwrap();
        let mut index = HostIndex::open(&dir.path().join(INDEX_FILE)).unwrap();
        let t0 = crate::now_ms();
        refresh_native_at(
            &mut index,
            &Owner::from(&agent),
            &bad,
            NativeFormat::Jsonl,
            false,
            false,
            t0,
        )
        .unwrap();
        refresh_native_at(
            &mut index,
            &Owner::from(&agent),
            &good,
            NativeFormat::Jsonl,
            false,
            false,
            t0,
        )
        .unwrap();
        let before = index
            .source_cursor(agent.id.as_str(), &bad)
            .unwrap()
            .unwrap();
        fs::write(&bad, "malformed complete line\n").unwrap();
        drop(index);
        assert!(
            store
                .verify_one_host_history_source(t0 + 3_600_001)
                .unwrap()
        );
        let error = store.index_host_history_step(u64::MAX).unwrap_err();
        assert!(matches!(error, IndexError::Io(ref e) if e.kind() == io::ErrorKind::InvalidData));
        assert!(
            store
                .verify_one_host_history_source(t0 + 3_600_002)
                .unwrap()
        );
        assert!(store.index_host_history_step(u64::MAX).unwrap());
        let index = HostIndex::open(&dir.path().join(INDEX_FILE)).unwrap();
        let after = index
            .source_cursor(agent.id.as_str(), &bad)
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                after.end,
                after.next_seq,
                after.boundary_sha256,
                after.stamp,
                after.full_verified_ms
            ),
            (
                before.end,
                before.next_seq,
                before.boundary_sha256,
                before.stamp,
                before.full_verified_ms
            )
        );
        assert_eq!(after.verification_attempt_ms, t0 + 3_600_001);
        assert_eq!(
            index
                .source_cursor(agent.id.as_str(), &good)
                .unwrap()
                .unwrap()
                .full_verified_ms,
            t0 + 3_600_002
        );
    }

    #[test]
    fn memory_changed_during_publication_rolls_back_documents_and_cursor() {
        let dir = crate::test_tempdir();
        let path = dir.path().join("MEMORY.md");
        let mut index = HostIndex::open(&dir.path().join("index.db")).unwrap();
        let metadata = IndexMetadata {
            agent_id: "owner".into(),
            source: MemoryScope::Memory,
            timestamp_ms: Some(0),
            direction: "in".into(),
            sender: "owner".into(),
            destination: "owner".into(),
            tool_name: None,
            tool_names: Vec::new(),
            role: Some("memory".into()),
        };
        fs::write(&path, "committedneedle").unwrap();
        refresh_document(&mut index, "memory:owner:one", &metadata, &path).unwrap();
        let before = serde_json::to_string(&index.source_cursor("owner", &path).unwrap()).unwrap();
        fs::write(&path, "uncommittedneedle").unwrap();
        let error = refresh_document_before_publish(
            &mut index,
            "memory:owner:one",
            &metadata,
            &path,
            || fs::write(&path, "racingneedle"),
        )
        .unwrap_err();
        assert!(matches!(error, IndexError::Io(ref e) if e.kind() == io::ErrorKind::WouldBlock));
        assert_eq!(
            serde_json::to_string(&index.source_cursor("owner", &path).unwrap()).unwrap(),
            before
        );
        let query = |term: &str| HistorySearchQuery {
            query: Some(term.into()),
            ..Default::default()
        };
        assert_eq!(
            index
                .search(
                    &["owner".into()],
                    MemoryScope::Memory,
                    &query("committedneedle")
                )
                .unwrap()
                .len(),
            1
        );
        assert!(
            index
                .search(
                    &["owner".into()],
                    MemoryScope::Memory,
                    &query("uncommittedneedle")
                )
                .unwrap()
                .is_empty()
        );
        refresh_document(&mut index, "memory:owner:one", &metadata, &path).unwrap();
        assert_eq!(
            index
                .search(
                    &["owner".into()],
                    MemoryScope::Memory,
                    &query("racingneedle")
                )
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn scoped_refresh_ignores_unrelated_failures_and_preserves_other_class() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("scoped-owner", "spiron"))
            .unwrap();
        let root = dir.path().join("agents").join(agent.id.as_str());
        fs::create_dir_all(root.join("memory")).unwrap();
        fs::create_dir_all(root.join("transcripts")).unwrap();
        let memory = root.join("memory/MEMORY.md");
        // AnyAgents' own transcript is strict; a harness's file tolerates
        // malformed lines (see `malformed_native_jsonl_falls_back...`).
        let native = root.join("transcripts/history.jsonl");
        fs::write(&memory, "sharedneedle").unwrap();
        fs::write(&native, "{\"content\":\"sharedneedle\"}\n").unwrap();
        let ids = [agent.id.to_string()];
        let q = HistorySearchQuery {
            query: Some("sharedneedle".into()),
            ..Default::default()
        };
        assert_eq!(
            store
                .search_host_files(&ids, MemoryScope::All, &q)
                .unwrap()
                .len(),
            2
        );
        let cursor = |path: &Path| {
            serde_json::to_string(
                &HostIndex::open(&dir.path().join(INDEX_FILE))
                    .unwrap()
                    .source_cursor(agent.id.as_str(), path)
                    .unwrap(),
            )
            .unwrap()
        };
        let native_before = cursor(&native);
        fs::write(&native, "invalid complete JSON record\n").unwrap();
        assert_eq!(
            store
                .search_host_files(&ids, MemoryScope::Memory, &q)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(cursor(&native), native_before);
        assert!(store.search_host_files(&ids, MemoryScope::All, &q).is_err());
        fs::write(&native, "{\"content\":\"sharedneedle\"}\n").unwrap();
        let memory_before = cursor(&memory);
        fs::hard_link(&memory, root.join("hardlink")).unwrap();
        assert_eq!(
            store
                .search_host_files(&ids, MemoryScope::Transcripts, &q)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(cursor(&memory), memory_before);
        assert!(store.search_host_files(&ids, MemoryScope::All, &q).is_err());
    }

    #[test]
    fn malformed_native_jsonl_falls_back_to_default_metadata_and_stays_searchable() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("torn-json-owner", "spiron"))
            .unwrap();
        let project_dir = dir
            .path()
            .join("agents")
            .join(agent.id.as_str())
            .join("config/projects/work");
        fs::create_dir_all(&project_dir).unwrap();
        let session = project_dir.join("session.jsonl");
        let torn = concat!(
            "{\"type\":\"api-request\",\"output_config\":{\"effort\":\"medium\"}",
            "{\"parentUuid\":\"382fafc4\",\"message\":{\"content\":\"tornalpha99\"}}\n",
            "invalid complete JSON record\n",
            "{\"role\":\"assistant\",\"content\":\"validbeta88\"}\n",
        );
        // Truncated inside a string used to become an I/O error from
        // MetadataReader, bypassing the malformed-record fallback entirely.
        let torn = format!(
            "{torn}{{\"content\":\"shorttornneedle\n{{\"content\":\"{}longtornneedle\n",
            "x ".repeat(5000)
        );
        fs::write(&session, &torn).unwrap();
        let ids = [agent.id.to_string()];
        for needle in [
            "tornalpha99",
            "validbeta88",
            "shorttornneedle",
            "longtornneedle",
        ] {
            let q = HistorySearchQuery {
                query: Some(needle.into()),
                ..Default::default()
            };
            let hits = store
                .search_host_files(&ids, MemoryScope::Transcripts, &q)
                .unwrap();
            assert_eq!(hits.len(), 1, "expected hit for {needle}");
            let q_all = HistorySearchQuery {
                query: Some(needle.into()),
                ..Default::default()
            };
            assert_eq!(
                store
                    .search_host_files(&ids, MemoryScope::All, &q_all)
                    .unwrap()
                    .len(),
                1
            );
        }
        let index = HostIndex::open(&dir.path().join(INDEX_FILE)).unwrap();
        assert!(
            index
                .source_cursor(agent.id.as_str(), &session)
                .unwrap()
                .is_some()
        );
        drop(index);
        assert!(
            store
                .verify_one_host_history_source(crate::now_ms() + 3_600_001)
                .unwrap()
        );
    }

    #[test]
    fn persistent_cursor_reuses_unchanged_and_authenticates_append_edit_replace_and_tail() {
        use std::io::Write;
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("cursor-owner", "spiron"))
            .unwrap();
        let path = dir.path().join("native.jsonl");
        let prefix = b"{\"role\":\"user\",\"content\":\"originalneedle\"}\n{\"role\":\"assistant\",\"content\":\"secondneedle\"}\n";
        let tail = b"{\"role\":\"tool\",\"content\":\"partial";
        fs::write(&path, [prefix.as_slice(), tail.as_slice()].concat()).unwrap();
        let index_path = dir.path().join("index.db");
        let mut index = HostIndex::open(&index_path).unwrap();
        let first = refresh_native(&mut index, &Owner::from(&agent), &path).unwrap();
        assert_eq!(first.indexed_records, 2);
        assert_eq!(first.prefix_verified_bytes, 0);
        assert_eq!(
            first.suffix_scanned_bytes,
            (prefix.len() + tail.len()) as u64
        );
        assert_eq!(
            index
                .source_cursor(agent.id.as_str(), &path)
                .unwrap()
                .unwrap()
                .end,
            prefix.len() as u64
        );
        drop(index);
        let mut index = HostIndex::open(&index_path).unwrap();
        let warm = refresh_native(&mut index, &Owner::from(&agent), &path).unwrap();
        assert_eq!(
            (
                warm.prefix_verified_bytes,
                warm.suffix_scanned_bytes,
                warm.indexed_records
            ),
            (0, 0, 0)
        );
        let documents =
            |index: &HostIndex| index.source_documents(agent.id.as_str(), &path).unwrap();
        assert_eq!(documents(&index).len(), 2);
        let suffix = b"needle\"}\n{\"role\":\"user\",\"content\":\"newneedle\"}\n";
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(suffix)
            .unwrap();
        let appended = refresh_native(&mut index, &Owner::from(&agent), &path).unwrap();
        assert_eq!(appended.prefix_verified_bytes, prefix.len() as u64);
        assert_eq!(
            appended.suffix_scanned_bytes,
            (tail.len() + suffix.len()) as u64
        );
        assert_eq!(appended.indexed_records, 2);
        assert_eq!(documents(&index).len(), 4);
        // A middle/prefix edit followed by append must not look append-only.
        let edited = String::from_utf8(fs::read(&path).unwrap())
            .unwrap()
            .replace("originalneedle", "rewrittenneedle")
            + "{\"content\":\"fifthneedle\"}\n";
        fs::write(&path, &edited).unwrap();
        let changed = refresh_native(&mut index, &Owner::from(&agent), &path).unwrap();
        assert_eq!(changed.indexed_records, 5);
        assert_eq!(changed.suffix_scanned_bytes, edited.len() as u64);
        let search = |index: &HostIndex, term: &str| {
            index
                .search(
                    &[agent.id.to_string()],
                    MemoryScope::Transcripts,
                    &HistorySearchQuery {
                        query: Some(term.into()),
                        ..Default::default()
                    },
                )
                .unwrap()
        };
        assert!(search(&index, "originalneedle").is_empty());
        assert_eq!(search(&index, "rewrittenneedle").len(), 1);
        let replacement = dir.path().join("replacement");
        fs::write(&replacement, b"{\"content\":\"replacementneedle\"}\n").unwrap();
        fs::rename(replacement, &path).unwrap();
        let replaced = refresh_native(&mut index, &Owner::from(&agent), &path).unwrap();
        assert_eq!(replaced.indexed_records, 1);
        assert_eq!(replaced.prefix_verified_bytes, 0);
        assert_eq!(documents(&index).len(), 1);
        assert!(search(&index, "rewrittenneedle").is_empty());
        assert_eq!(
            read_raw(&search(&index, "replacementneedle")[0].raw).unwrap(),
            fs::read(&path).unwrap()
        );
        // A batch can write both documents and its cursor before an injected
        // interruption; the outer transaction must roll all of them back.
        let before =
            serde_json::to_string(&index.source_cursor(agent.id.as_str(), &path).unwrap()).unwrap();
        let failure = index
            .refresh_source(|index| -> Result<(), IndexError> {
                index.clear_source(agent.id.as_str(), &path)?;
                let raw = RawSource::open(&path)?.whole()?;
                index.index(
                    "uncommitted-batch",
                    &event_metadata(
                        &Owner::from(&agent),
                        &EventHeader::default(),
                        NativeFormat::Jsonl,
                    ),
                    &raw,
                )?;
                let mut advanced: SourceCursor =
                    serde_json::from_str::<Option<SourceCursor>>(&before)?.unwrap();
                advanced.next_seq = 999;
                index.save_source(agent.id.as_str(), &path, &advanced)?;
                Err(io::Error::other("injected before source publication").into())
            })
            .unwrap_err();
        assert!(
            failure
                .to_string()
                .contains("injected before source publication")
        );
        assert_eq!(
            serde_json::to_string(&index.source_cursor(agent.id.as_str(), &path).unwrap()).unwrap(),
            before
        );
        assert_eq!(search(&index, "replacementneedle").len(), 1);
        index
            .retain_agent_documents(agent.id.as_str(), &HashSet::new())
            .unwrap();
        assert!(
            index
                .source_cursor(agent.id.as_str(), &path)
                .unwrap()
                .is_none()
        );
        assert!(search(&index, "replacementneedle").is_empty());
        refresh_native(&mut index, &Owner::from(&agent), &path).unwrap();
        fs::write(&path, b"").unwrap();
        refresh_native(&mut index, &Owner::from(&agent), &path).unwrap();
        assert!(documents(&index).is_empty());
    }

    #[test]
    fn indexed_native_history_cannot_bypass_redaction_or_symlink_privacy() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("redaction-owner", "spiron"))
            .unwrap();
        store.set_agent_state(&agent.id, "archived").unwrap();
        let root = dir.path().join("agents").join(agent.id.as_str());
        fs::create_dir_all(root.join("config")).unwrap();
        fs::write(
            root.join("config/sylphx-transcript.jsonl"),
            concat!(
                "{\"type\":\"function_call\",\"call_id\":\"redaction-call\",\"name\":\"browser\"}\n",
                "{\"type\":\"function_call_output\",\"call_id\":\"redaction-call\",\"output\":\"secrettoken remainingword\"}\n"
            ),
        )
        .unwrap();
        let ids = [agent.id.to_string()];
        let mut q = HistorySearchQuery {
            query: Some("secrettoken".into()),
            ..Default::default()
        };
        assert_eq!(
            store
                .search_host_files(&ids, MemoryScope::Transcripts, &q)
                .unwrap()
                .len(),
            1
        );
        let native = root.join("config/sylphx-transcript.jsonl");
        assert!(
            HostIndex::open(&dir.path().join(INDEX_FILE))
                .unwrap()
                .source_cursor(agent.id.as_str(), &native)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            HostIndex::open(&dir.path().join(INDEX_FILE))
                .unwrap()
                .call_tool(agent.id.as_str(), &native, "redaction-call")
                .unwrap()
                .as_deref(),
            Some("browser")
        );
        // An unrelated unsupported binary copy produces a partial receipt but
        // must not hide the successfully scrubbed local transcript.
        fs::write(root.join("config/unrelated.sqlite"), b"unmodified binary").unwrap();
        let mut receipt = store
            .redact_conversation(
                crate::redaction::Actor::Operator,
                &agent.id,
                &crate::redaction::Request {
                    message_id: Some("transcript:1".into()),
                    seq: None,
                    span: Some(crate::redaction::Span {
                        unit: crate::redaction::SpanUnit::Bytes,
                        start: 0,
                        end: 11,
                    }),
                    reason: "privacy request".into(),
                },
            )
            .unwrap();
        assert!(!receipt.full_scrub, "{receipt:?}");
        assert!(
            HostIndex::open(&dir.path().join(INDEX_FILE))
                .unwrap()
                .source_cursor(agent.id.as_str(), &native)
                .unwrap()
                .is_none()
        );
        assert!(
            HostIndex::open(&dir.path().join(INDEX_FILE))
                .unwrap()
                .call_tool(agent.id.as_str(), &native, "redaction-call")
                .unwrap()
                .is_none()
        );
        assert!(
            receipt
                .unscrubbed
                .iter()
                .any(|f| f.path.ends_with("unrelated.sqlite"))
        );
        assert!(
            store
                .search_host_files(&ids, MemoryScope::Transcripts, &q)
                .unwrap()
                .is_empty()
        );
        q.query = Some("remainingword".into());
        let hits = store
            .search_host_files(&ids, MemoryScope::Transcripts, &q)
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert!(String::from_utf8_lossy(&hits[0].1).contains("[redacted]"));
        let raw_id = hits[0].0.id.clone();
        let raw_sha = hits[0].0.raw.sha256.clone();
        assert!(
            self::Store::read_host_file_range(
                &store,
                &ids,
                MemoryScope::Transcripts,
                None,
                HostRawRead {
                    id: &raw_id,
                    sha256: &raw_sha,
                    offset: 0,
                    length: 65536
                }
            )
            .unwrap()
            .is_some()
        );
        // Persisted pending directory exclusions survive a Store reopen and
        // apply to descendants, not merely exact filenames.
        receipt.unscrubbed.push(crate::redaction::CopyFailure {
            path: root.join("config").display().to_string(),
            error: "scrub pending/interrupted".into(),
        });
        store
            .conn()
            .execute(
                "UPDATE conversation_redactions SET receipt=?1 WHERE id=?2",
                rusqlite::params![serde_json::to_string(&receipt).unwrap(), receipt.audit_id],
            )
            .unwrap();
        drop(store);
        let store = Store::open(dir.path().join("state.db")).unwrap();
        assert!(
            store
                .read_host_file_range(
                    &ids,
                    MemoryScope::Transcripts,
                    None,
                    HostRawRead {
                        id: &raw_id,
                        sha256: &raw_sha,
                        offset: 0,
                        length: 65536
                    }
                )
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .search_host_files(&ids, MemoryScope::Transcripts, &q)
                .unwrap()
                .is_empty()
        );
        #[cfg(unix)]
        {
            let foreign = dir.path().join("foreign");
            fs::create_dir_all(&foreign).unwrap();
            fs::write(foreign.join("MEMORY.md"), b"foreignsecret").unwrap();
            std::os::unix::fs::symlink(&foreign, root.join("memory")).unwrap();
            q.query = Some("foreignsecret".into());
            assert!(
                store
                    .search_host_files(&ids, MemoryScope::All, &q)
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[test]
    fn host_selection_normalizes_wildcards_names_and_historical_aliases() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("old-name", "spiron"))
            .unwrap();
        store.rename_agent(&agent.id, "new-name").unwrap();
        let root = dir
            .path()
            .join("agents")
            .join(agent.id.as_str())
            .join("memory");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("MEMORY.md"), "positivehostneedle").unwrap();
        let ids = [agent.id.to_string()];
        for selector in [
            "*",
            "ALL",
            "all",
            "old-name",
            "new-name",
            "agent:old-name",
            agent.id.as_str(),
        ] {
            let q = HistorySearchQuery {
                query: Some("positivehostneedle".into()),
                agent: Some(selector.into()),
                ..Default::default()
            };
            assert_eq!(
                store
                    .search_host_files(&ids, MemoryScope::Memory, &q)
                    .unwrap()
                    .len(),
                1,
                "{selector}"
            );
            assert!(
                store
                    .search_host_files(&[], MemoryScope::Memory, &q)
                    .unwrap()
                    .is_empty()
            );
        }
    }

    /// One agent root holding every harness's native history where AnyAgents
    /// points it, each event carrying its own needle.
    fn every_harness_history(root: &Path) {
        let line = |v: serde_json::Value| serde_json::to_string(&v).unwrap() + "\n";
        let write = |rel: &str, text: String| {
            let path = root.join(rel);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, text).unwrap();
        };
        crate::host_sqlite::tests::spiron_store(
            &root.join("config/sessions.sqlite3"),
            &[
                r#"{"type":"text","timestamp":"2026-11-12T02:51:45Z","data":{"role":"user","content":"spironneedle 粵語問題"}}"#,
                r#"{"type":"tool_call","timestamp":"2026-11-12T02:51:49Z","data":{"toolName":"mcp__anyagents__send","args":{"text":"spironcallneedle"},"callId":"c1"}}"#,
                r#"{"type":"tool_result","timestamp":"2026-11-12T02:52:42Z","data":{"callId":"c1","toolName":"mcp__anyagents__send","result":"spironresultneedle"}}"#,
            ],
        );
        let opencode = root.join("config/data-home/opencode/opencode.db");
        fs::create_dir_all(opencode.parent().unwrap()).unwrap();
        let conn = rusqlite::Connection::open(&opencode).unwrap();
        conn.execute_batch(
            "CREATE TABLE session_message(id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
                type TEXT NOT NULL, seq INTEGER NOT NULL, time_created INTEGER NOT NULL,
                time_updated INTEGER NOT NULL, data TEXT NOT NULL);
            INSERT INTO session_message VALUES('m1','s','user',1,1794162535171,1794162535171,
                '{\"text\":\"opencodeneedle question\"}');
            INSERT INTO session_message VALUES('m2','s','assistant',2,1794162535193,1794162538640,
                '{\"content\":[{\"type\":\"tool\",\"id\":\"call_1\",\"name\":\"execute\",\"state\":{\"output\":\"opencodetoolneedle\"}}]}');",
        )
        .unwrap();
        drop(conn);
        let project = "config/projects/-work";
        write(
            &format!("{project}/s1.jsonl"),
            line(
                serde_json::json!({"type":"user","timestamp":"2026-11-01T00:00:00Z","message":{"role":"user","content":"claudeneedle"}}),
            ),
        );
        write(
            &format!("{project}/s1/subagents/agent-a1.jsonl"),
            line(
                serde_json::json!({"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"Bash","input":{"command":"claudesubagentneedle"}}]}}),
            ),
        );
        write(
            "config/sessions/2026/11/03/rollout-2026-11-03T09-00-40-x.jsonl",
            line(
                serde_json::json!({"timestamp":"2026-11-03T09:00:41Z","type":"response_item","payload":{"type":"function_call","name":"exec","call_id":"k1","arguments":"codexneedle"}}),
            ) + "{\"torn\":\"codextornneedle\"\n"
                + &line(
                    serde_json::json!({"timestamp":"2026-11-03T09:00:42Z","type":"response_item","payload":{"type":"function_call_output","call_id":"k1","output":"codexoutputneedle"}}),
                ),
        );
        write(
            "config/.gemini/antigravity-cli/brain/b1/.system_generated/logs/transcript_full.jsonl",
            line(
                serde_json::json!({"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","created_at":"2026-11-12T02:12:19Z","content":"agyneedle"}),
            ),
        );
        // The non-full AGY transcript duplicates the full one and is not read.
        write(
            "config/.gemini/antigravity-cli/brain/b1/.system_generated/logs/transcript.jsonl",
            line(serde_json::json!({"type":"USER_INPUT","content":"agyduplicateneedle"})),
        );
        write(
            "config/data-home/muse/sessions/2026/11/12/m1/session.jsonl",
            line(
                serde_json::json!({"retained_frame":"session_permission_transaction","children":[{"record_json":"{\"payload\":\"museneedle\"}"}]}),
            ),
        );
        write(
            "config/data-home/muse/sessions/2026/11/12/m1/subagent/m2/session.jsonl",
            line(
                serde_json::json!({"retained_frame":"x","children":[{"record_json":"musesubagentneedle"}]}),
            ),
        );
        write(
            "config/data-home/muse/sessions/.msp-view-g1/m1/session.jsonl",
            line(serde_json::json!({"snapshot":"musesnapshotneedle"})),
        );
        write(
            "config/.omp/agent/sessions/--work--/2026-10-14T01-34-15-875Z_x.jsonl",
            line(
                serde_json::json!({"type":"message","timestamp":"2026-10-14T01:34:16.234Z","message":{"role":"user","content":[{"type":"text","text":"ompneedle"}]}}),
            ),
        );
        write(
            "config/.grok/sessions/%2Fwork/g1/chat_history.jsonl",
            line(serde_json::json!({"role":"user","content":"grokneedle"})),
        );
    }

    #[test]
    fn every_harness_native_history_is_indexed_with_metadata_and_exact_readback() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("polyglot", "spiron"))
            .unwrap();
        let other = store
            .create_agent(NewAgent::new("other", "spiron"))
            .unwrap();
        let root = dir.path().join("agents").join(agent.id.as_str());
        every_harness_history(&root);
        every_harness_history(&dir.path().join("agents").join(other.id.as_str()));
        let ids = [agent.id.to_string()];
        let find = |needle: &str| {
            store
                .search_host_files(
                    &ids,
                    MemoryScope::Transcripts,
                    &HistorySearchQuery {
                        query: Some(needle.into()),
                        ..Default::default()
                    },
                )
                .unwrap()
        };
        for needle in [
            "spironneedle",
            "spironcallneedle",
            "spironresultneedle",
            "opencodeneedle",
            "opencodetoolneedle",
            "claudeneedle",
            "claudesubagentneedle",
            "codexneedle",
            "codextornneedle",
            "codexoutputneedle",
            "agyneedle",
            "museneedle",
            "musesubagentneedle",
            "ompneedle",
            "grokneedle",
        ] {
            let hits = find(needle);
            assert_eq!(hits.len(), 1, "{needle}: {hits:?}");
            let (hit, window) = &hits[0];
            assert_eq!(hit.metadata.agent_id, agent.id.as_str(), "{needle}");
            assert!(String::from_utf8_lossy(window).contains(needle), "{needle}");
            // Every hit cites a raw coordinate that reads back exactly.
            let (read, bytes) = store
                .read_host_file_range(
                    &ids,
                    MemoryScope::Transcripts,
                    None,
                    HostRawRead {
                        id: &hit.id,
                        sha256: &hit.raw.sha256,
                        offset: 0,
                        length: 65536,
                    },
                )
                .unwrap()
                .unwrap();
            assert_eq!(read.raw, hit.raw, "{needle}");
            assert_eq!(sha256(&bytes), hit.raw.sha256, "{needle}");
        }
        for skipped in ["agyduplicateneedle", "musesnapshotneedle"] {
            assert!(find(skipped).is_empty(), "{skipped}");
        }

        // Metadata comes from each format's own fields.
        let meta = |needle: &str| find(needle).remove(0).0;
        let spiron = meta("spironneedle");
        assert_eq!(spiron.metadata.role.as_deref(), Some("user"));
        assert_eq!(spiron.metadata.direction, "in");
        assert!(spiron.metadata.timestamp_ms.is_some());
        assert!(
            spiron
                .raw
                .segment
                .ends_with("config/sessions.sqlite3/session_entries/1")
        );
        let result = meta("spironresultneedle");
        assert_eq!(result.metadata.role.as_deref(), Some("tool"));
        assert_eq!(
            result.metadata.tool_name.as_deref(),
            Some("mcp__anyagents__send")
        );
        let opencode = meta("opencodetoolneedle");
        assert_eq!(opencode.metadata.role.as_deref(), Some("assistant"));
        assert_eq!(opencode.metadata.timestamp_ms, Some(1794162535193));
        assert_eq!(opencode.metadata.tool_names, vec!["execute".to_string()]);
        let codex = meta("codexoutputneedle");
        assert_eq!(codex.metadata.tool_name.as_deref(), Some("exec"));
        assert!(codex.metadata.timestamp_ms.is_some());
        assert_eq!(meta("agyneedle").metadata.role.as_deref(), Some("user"));
        assert!(meta("agyneedle").metadata.timestamp_ms.is_some());
        assert_eq!(
            meta("claudesubagentneedle").metadata.tool_name.as_deref(),
            Some("Bash")
        );
        // Tool filters reach store rows as they reach files.
        let tools = store
            .search_host_files(
                &ids,
                MemoryScope::Transcripts,
                &HistorySearchQuery {
                    query: Some("opencodetoolneedle".into()),
                    tool_name: Some("execute".into()),
                    only_tools: true,
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(tools.len(), 1);
    }

    /// Under a seat, Claude Code keeps `config/sessions` owner-only, which is
    /// where Codex would keep its rollouts; a harness's projects directory
    /// can be private too. Neither may hide the agent's other history.
    #[test]
    fn a_harness_directory_the_server_cannot_read_hides_no_other_history() {
        use std::os::unix::fs::PermissionsExt;
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("private-dirs", "claude-code"))
            .unwrap();
        let root = dir.path().join("agents").join(agent.id.as_str());
        every_harness_history(&root);
        let private = [
            root.join("config/sessions"),
            root.join("config/.omp/agent/sessions"),
        ];
        for path in &private {
            fs::set_permissions(path, fs::Permissions::from_mode(0o000)).unwrap();
        }
        // A privileged test run reads everything; the case does not arise.
        let denied = fs::read_dir(&private[0]).is_err();
        let ids = [agent.id.to_string()];
        let found = |needle: &str| {
            store
                .search_host_files(
                    &ids,
                    MemoryScope::Transcripts,
                    &HistorySearchQuery {
                        query: Some(needle.into()),
                        ..Default::default()
                    },
                )
                .map(|hits| hits.len())
        };
        let results: Vec<_> = [
            "claudeneedle",
            "spironneedle",
            "agyneedle",
            "museneedle",
            "codexneedle",
        ]
        .iter()
        .map(|needle| (*needle, found(needle)))
        .collect();
        for path in &private {
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        for (needle, hits) in results {
            let hits = hits.unwrap_or_else(|e| panic!("{needle}: {e}"));
            let expected = usize::from(!(denied && needle == "codexneedle"));
            assert_eq!(hits, expected, "{needle}");
        }
    }

    #[test]
    fn store_rows_index_incrementally_follow_rewrites_and_rebuild_a_replaced_store() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store.create_agent(NewAgent::new("rows", "spiron")).unwrap();
        let root = dir.path().join("agents").join(agent.id.as_str());
        every_harness_history(&root);
        let ids = [agent.id.to_string()];
        let count = |needle: &str| {
            store
                .search_host_files(
                    &ids,
                    MemoryScope::Transcripts,
                    &HistorySearchQuery {
                        query: Some(needle.into()),
                        ..Default::default()
                    },
                )
                .unwrap()
                .len()
        };
        assert_eq!(count("spironneedle"), 1);
        let db = root.join("config/sessions.sqlite3");
        let mut index = HostIndex::open(&dir.path().join(INDEX_FILE)).unwrap();
        let owner = Owner::from(&agent);
        let first = index
            .source_cursor(agent.id.as_str(), &db)
            .unwrap()
            .unwrap();
        assert_eq!(first.end, 3);
        assert!(first.policy.starts_with("native-sqlite-v1+spiron:"));

        // A live writer appends through its WAL; the next search sees it
        // without the database file changing.
        let writer = rusqlite::Connection::open(&db).unwrap();
        writer
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
            .unwrap();
        writer
            .execute(
                "INSERT INTO session_entries VALUES('s1',4,'{\"type\":\"text\",\"data\":{\"role\":\"assistant\",\"content\":\"appendedrowneedle\"}}')",
                [],
            )
            .unwrap();
        assert_eq!(count("appendedrowneedle"), 1);
        let appended = index
            .source_cursor(agent.id.as_str(), &db)
            .unwrap()
            .unwrap();
        assert_eq!(
            (appended.end, appended.full_verified_ms),
            (4, first.full_verified_ms)
        );

        // OpenCode rewrites a streaming message in place: the new text is
        // found and the old text is not.
        let opencode = root.join("config/data-home/opencode/opencode.db");
        let oc = rusqlite::Connection::open(&opencode).unwrap();
        oc.execute(
            "UPDATE session_message SET data='{\"text\":\"rewrittenneedle\"}', time_updated=1794162600000 WHERE id='m1'",
            [],
        )
        .unwrap();
        drop(oc);
        assert_eq!(count("rewrittenneedle"), 1);
        assert_eq!(count("opencodeneedle"), 0);

        // A changed frontier row means the store was rebuilt beneath us.
        writer
            .execute(
                "UPDATE session_entries SET event='{\"type\":\"done\"}' WHERE idx=4",
                [],
            )
            .unwrap();
        refresh_store_at(&mut index, &owner, &db, StoreKind::SpironSessions, false, 7).unwrap();
        let rebuilt = index
            .source_cursor(agent.id.as_str(), &db)
            .unwrap()
            .unwrap();
        assert_eq!((rebuilt.end, rebuilt.full_verified_ms), (4, 7));
        assert_eq!(count("appendedrowneedle"), 0);
        assert_eq!(count("spironneedle"), 1);

        // A deleted row is refused at readback by its digest.
        let hit = store
            .search_host_files(
                &ids,
                MemoryScope::Transcripts,
                &HistorySearchQuery {
                    query: Some("spironcallneedle".into()),
                    ..Default::default()
                },
            )
            .unwrap()
            .remove(0)
            .0;
        writer
            .execute("DELETE FROM session_entries WHERE idx=2", [])
            .unwrap();
        assert!(crate::raw_segment::read_raw(&hit.raw).is_err());

        // A store this reader does not know is not history: its earlier rows
        // are forgotten with the walk that no longer finds it.
        drop(writer);
        fs::remove_file(&db).unwrap();
        for suffix in ["-wal", "-shm"] {
            let _ = fs::remove_file(crate::host_sqlite::tests::sibling_for_test(&db, suffix));
        }
        rusqlite::Connection::open(&db)
            .unwrap()
            .execute_batch("CREATE TABLE unrelated(x);")
            .unwrap();
        assert_eq!(count("spironneedle"), 0);
        assert!(
            index
                .source_cursor(agent.id.as_str(), &db)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn a_redacted_or_linked_store_is_never_read() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("linked", "spiron"))
            .unwrap();
        let root = dir.path().join("agents").join(agent.id.as_str());
        let elsewhere = dir.path().join("elsewhere");
        every_harness_history(&elsewhere);
        fs::create_dir_all(root.join("config/data-home/opencode")).unwrap();
        std::os::unix::fs::symlink(
            elsewhere.join("config/sessions.sqlite3"),
            root.join("config/sessions.sqlite3"),
        )
        .unwrap();
        std::os::unix::fs::symlink(
            elsewhere.join("config/data-home/opencode"),
            root.join("config/data-home/opencode/linked"),
        )
        .unwrap();
        let ids = [agent.id.to_string()];
        for needle in ["spironneedle", "opencodeneedle"] {
            assert!(
                store
                    .search_host_files(
                        &ids,
                        MemoryScope::Transcripts,
                        &HistorySearchQuery {
                            query: Some(needle.into()),
                            ..Default::default()
                        },
                    )
                    .unwrap()
                    .is_empty(),
                "{needle}"
            );
        }
    }

    #[test]
    fn authorized_full_text_native_in_place_and_memory_share_filters() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let own = store.create_agent(NewAgent::new("own", "spiron")).unwrap();
        let hidden = store
            .create_agent(NewAgent::new("hidden", "spiron"))
            .unwrap();
        for agent in [&own, &hidden] {
            let root = dir.path().join("agents").join(agent.id.as_str());
            fs::create_dir_all(root.join("memory")).unwrap();
            fs::create_dir_all(root.join("config")).unwrap();
            fs::write(
                root.join("memory/MEMORY.md"),
                format!("{} lateuniqueneedle 繁體中文", "padding ".repeat(1700)),
            )
            .unwrap();
            fs::write(root.join("config/sylphx-transcript.jsonl"),serde_json::to_string(&serde_json::json!({"role":"tool","tool":"browser","timestamp_ms":123,"output":format!("{}\nlateuniqueneedle", "padding ".repeat(1700))})).unwrap()+"\n{\"partial\":").unwrap();
        }
        let source = dir
            .path()
            .join("agents")
            .join(own.id.as_str())
            .join("config/sylphx-transcript.jsonl");
        let before = fs::read(&source).unwrap();
        let mut q = HistorySearchQuery {
            query: Some("lateuniqueneedle".into()),
            ..Default::default()
        };
        let ids = [own.id.to_string()];
        let hits = store.search_host_files(&ids, MemoryScope::All, &q).unwrap();
        assert_eq!(hits.len(), 2);
        assert!(
            hits.iter()
                .all(|(h, b)| h.metadata.agent_id == own.id.as_str()
                    && String::from_utf8_lossy(b).contains("lateuniqueneedle"))
        );
        assert_eq!(fs::read(&source).unwrap(), before);
        assert!(!source.with_extension("jsonl.ack").exists());
        q.only_tools = true;
        q.tool_name = Some("browser".into());
        assert_eq!(
            store
                .search_host_files(&ids, MemoryScope::All, &q)
                .unwrap()
                .len(),
            1
        );
        assert!(
            store
                .search_host_files(&[], MemoryScope::All, &q)
                .unwrap()
                .is_empty()
        );
        q.only_tools = false;
        q.tool_name = None;
        q.exclude = vec!["lateuniqueneedle".into()];
        assert!(
            store
                .search_host_files(&ids, MemoryScope::All, &q)
                .unwrap()
                .is_empty()
        );
        fs::remove_file(
            dir.path()
                .join("agents")
                .join(own.id.as_str())
                .join("memory/MEMORY.md"),
        )
        .unwrap();
        q.exclude.clear();
        assert!(
            store
                .search_host_files(&ids, MemoryScope::Memory, &q)
                .unwrap()
                .is_empty()
        );
    }
    #[test]
    fn generic_and_legacy_archives_remain_scoped_and_raw_readable_after_reopen() {
        let dir = crate::test_tempdir();
        let path = dir.path().join("state.db");
        let store = Store::open(&path).unwrap();
        let own = store
            .create_agent(NewAgent::new("archive-reader", "test"))
            .unwrap();
        let hidden = store
            .create_agent(NewAgent::new("archive-hidden", "test"))
            .unwrap();
        for agent in [&own, &hidden] {
            let root = dir.path().join("agents").join(agent.id.as_str());
            for folder in [
                root.join("history/archive"),
                root.join("history").join("owners"),
            ] {
                fs::create_dir_all(&folder).unwrap();
                fs::write(
                    folder.join("page.md"),
                    format!("archiveneedle {} {:?}", agent.name, folder),
                )
                .unwrap();
            }
        }
        let query = HistorySearchQuery {
            query: Some("archiveneedle".into()),
            ..Default::default()
        };
        let ids = [own.id.to_string()];
        for store in [store, Store::open(&path).unwrap()] {
            let hits = store
                .search_host_files(&ids, MemoryScope::Transcripts, &query)
                .unwrap();
            assert_eq!(hits.len(), 2);
            for (hit, bytes) in hits {
                assert_eq!(hit.metadata.agent_id, own.id.as_str());
                assert_eq!(read_raw(&hit.raw).unwrap(), bytes);
                assert!(String::from_utf8_lossy(&bytes).contains("archive-reader"));
            }
            assert!(
                store
                    .search_host_files(&[], MemoryScope::Transcripts, &query)
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[test]
    fn memory_index_excludes_dotfiles_and_generated_index_when_topics_exist() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("topic-owner", "spiron"))
            .unwrap();
        let root = dir.path().join("agents").join(agent.id.as_str());
        crate::learned_memory::write_sleep_state(
            &root,
            &crate::learned_memory::SleepState {
                last_consolidated_ms: 123,
                last_inbound_seq: 4,
                last_outbound_ms: 5,
                consolidated_topics: 1,
            },
        )
        .unwrap();
        crate::learned_memory::upsert_memory_topic(
            &root,
            &agent.name,
            None,
            "cache-architecture",
            Some("uniqueindexphrase for cache architecture"),
            "Detailed topic body with uniqueindexphrase",
            1700000000000,
        )
        .unwrap();
        fs::write(root.join("memory/.hidden-note.md"), "hiddensecretneedle").unwrap();
        assert!(root.join("memory/MEMORY.md").is_file());
        assert!(root.join("memory/.sleep-state.json").is_file());
        let ids = [agent.id.to_string()];
        let hits = store
            .search_host_files(
                &ids,
                MemoryScope::Memory,
                &HistorySearchQuery {
                    query: Some("uniqueindexphrase".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(
            hits.len(),
            1,
            "MEMORY.md must not duplicate topic search hit"
        );
        assert!(
            hits[0]
                .0
                .raw
                .segment
                .ends_with("memory/cache-architecture.md")
        );
        let dot_hits = store
            .search_host_files(
                &ids,
                MemoryScope::Memory,
                &HistorySearchQuery {
                    query: Some("hiddensecretneedle".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(
            dot_hits.is_empty(),
            "dotfiles in memory/ must never be indexed"
        );
    }

    #[test]
    fn search_host_files_locks_one_agent_at_a_time_across_multi_agent_refresh_and_readback() {
        use std::time::{Duration, Instant};
        let dir = crate::test_tempdir();
        let db = dir.path().join("state.db");
        let store = Store::open(&db).unwrap();
        let a = store
            .create_agent(NewAgent::new("agent-a", "spiron"))
            .unwrap();
        let b = store
            .create_agent(NewAgent::new("agent-b", "spiron"))
            .unwrap();
        let (first, second) = if a.id.as_str() < b.id.as_str() {
            (a, b)
        } else {
            (b, a)
        };
        for agent in [&first, &second] {
            let mem_dir = dir
                .path()
                .join("agents")
                .join(agent.id.as_str())
                .join("memory");
            fs::create_dir_all(&mem_dir).unwrap();
            fs::write(
                mem_dir.join("MEMORY.md"),
                format!("multilockneedle {}", agent.name),
            )
            .unwrap();
        }
        let first_mem = dir
            .path()
            .join("agents")
            .join(first.id.as_str())
            .join("memory/MEMORY.md");
        let second_lock = store.lock_agent_lifecycle(&second.id).unwrap();
        let ids = vec![first.id.to_string(), second.id.to_string()];
        let search_db = db.clone();
        let index_path = dir.path().join(INDEX_FILE);
        let handle = std::thread::spawn(move || {
            let search_store = Store::open(&search_db).unwrap();
            search_store
                .search_host_files(
                    &ids,
                    MemoryScope::Memory,
                    &HistorySearchQuery {
                        query: Some("multilockneedle".into()),
                        ..Default::default()
                    },
                )
                .unwrap()
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut first_refreshed = false;
        let mut first_released = false;
        while Instant::now() < deadline {
            if !first_refreshed
                && index_path.exists()
                && HostIndex::open(&index_path)
                    .ok()
                    .and_then(|idx| idx.source_cursor(first.id.as_str(), &first_mem).ok())
                    .flatten()
                    .is_some()
            {
                first_refreshed = true;
            }
            if first_refreshed && let Ok(probe_first) = store.try_lock_agent_lifecycle(&first.id) {
                drop(probe_first);
                first_released = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            first_refreshed,
            "search_host_files did not refresh first agent before waiting on second"
        );
        // While search_host_files is waiting on `second.id`, `first.id`'s lifecycle lock
        // must already be released so at most one `.lifecycle` lock is held at a time.
        assert!(
            first_released,
            "first agent lifecycle lock remained held while acquiring second"
        );
        drop(second_lock);
        let hits = handle.join().unwrap();
        assert_eq!(hits.len(), 2);
    }
}
