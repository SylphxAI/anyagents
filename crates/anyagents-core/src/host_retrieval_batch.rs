use super::*;

/// Maximum number of memory topic files published under a single lifecycle
/// lock and index transaction so cold serverless hydration of thousands of
/// memory files completes in milliseconds instead of thousands of round-trips.
pub(super) const MEMORY_PUBLISH_BATCH_DOCS: usize = 512;

struct PendingMemoryDoc {
    path: PathBuf,
    stamp: FileStamp,
    policy: String,
    raw_length: u64,
    prepared: Option<crate::host_index::PreparedDocument>,
}

fn flush_memory_batch(
    index: &mut HostIndex,
    owner_id: &str,
    pending_batch: &mut Vec<PendingMemoryDoc>,
    walk: &mut Walk,
    refresh: &mut ClassRefresh,
    budget: &mut u64,
    gate: &mut Gate<'_>,
) -> Result<(), IndexError> {
    if pending_batch.is_empty() {
        return Ok(());
    }
    let batch = std::mem::take(pending_batch);
    let Some(_guard) = gate()? else {
        for doc in batch {
            let len = doc.stamp.len;
            walk.note(&doc.path, Some(doc.stamp.clone()));
            refresh.add(&RefreshStats::new(doc.stamp).pending(len));
        }
        return Ok(());
    };
    let mut published_bytes: u64 = 0;
    let mut done_stamps: Vec<(PathBuf, FileStamp)> = Vec::with_capacity(batch.len());
    let mut skipped: Vec<(PathBuf, Option<FileStamp>, FileStamp)> = Vec::new();
    index.refresh_source(|index| {
        for doc in &batch {
            let current_stamp = match open_readonly(&doc.path).and_then(|f| FileStamp::read(&f)) {
                Ok(s) => s,
                Err(_) => {
                    skipped.push((doc.path.clone(), None, doc.stamp.clone()));
                    continue;
                }
            };
            if current_stamp != doc.stamp {
                skipped.push((doc.path.clone(), Some(current_stamp), doc.stamp.clone()));
                continue;
            }
            if let Some(prepared) = &doc.prepared {
                index.publish_document(prepared)?;
            }
            index.save_source(
                owner_id,
                &doc.path,
                &SourceCursor {
                    stamp: doc.stamp.clone(),
                    policy: doc.policy.clone(),
                    end: doc.raw_length,
                    next_seq: 2,
                    boundary_sha256: String::new(),
                    full_verified_ms: 0,
                    verification_attempt_ms: 0,
                    complete: true,
                    rebuilding: false,
                },
            )?;
            published_bytes = published_bytes.saturating_add(doc.raw_length);
            done_stamps.push((doc.path.clone(), doc.stamp.clone()));
        }
        Ok(())
    })?;
    drop(_guard);
    *budget = budget.saturating_sub(published_bytes);
    for (path, stamp) in done_stamps {
        walk.note(&path, Some(stamp.clone()));
        refresh.add(&RefreshStats::new(stamp).done());
    }
    for (path, fresh, old) in skipped {
        let len = fresh.as_ref().map_or(old.len, |s| s.len);
        walk.note(&path, fresh.clone());
        refresh.add(&RefreshStats::new(fresh.unwrap_or(old)).pending(len));
    }
    Ok(())
}

/// Index local learned-memory markdown files under `root/memory` in bounded batches,
/// publishing up to [`MEMORY_PUBLISH_BATCH_DOCS`] files per lifecycle lock and
/// SQLite index transaction.
#[allow(clippy::too_many_arguments)]
pub(super) fn refresh_local_memory_files(
    index: &mut HostIndex,
    owner: &Owner,
    root: &Path,
    blocked: &dyn Fn(&Path) -> bool,
    walk: &mut Walk,
    keep: &mut HashSet<String>,
    refresh: &mut ClassRefresh,
    budget: &mut u64,
    gate: &mut Gate<'_>,
) -> Result<(), IndexError> {
    let mut pending_batch: Vec<PendingMemoryDoc> = Vec::new();
    let mut batch_bytes: u64 = 0;
    for path in memory_indexable_files(walk, &root.join("memory"))? {
        if blocked(&path) {
            continue;
        }
        keep.insert(path.to_string_lossy().into_owned());
        let id = format!(
            "memory:{}:{}",
            owner.id,
            sha256(path.as_os_str().as_encoded_bytes())
        );
        let metadata = memory_metadata(owner, Some(modified_ms(&path)?));
        let mut source = RawSource::open(&path)?;
        let stamp = source.stamp().clone();
        let policy = serde_json::to_string(&metadata)?;
        if index
            .source_cursor(&metadata.agent_id, &path)?
            .is_some_and(|old| old.stamp == stamp && old.policy == policy)
        {
            walk.note(&path, Some(stamp.clone()));
            refresh.add(&RefreshStats::new(stamp).done());
            continue;
        }
        let remaining_budget = budget.saturating_sub(batch_bytes);
        if stamp.len > remaining_budget && remaining_budget < BACKGROUND_STEP_BYTES {
            walk.note(&path, Some(stamp.clone()));
            refresh.add(&RefreshStats::new(stamp.clone()).pending(stamp.len));
            continue;
        }
        let prepared_res =
            (|| -> Result<(u64, Option<crate::host_index::PreparedDocument>), IndexError> {
                let raw = source.whole()?;
                let prepared = if index.is_current(&id, &metadata, &raw)? {
                    None
                } else {
                    Some(crate::host_index::prepare_document(
                        &mut source,
                        &id,
                        &metadata,
                        &raw,
                    )?)
                };
                Ok((raw.length, prepared))
            })();
        drop(source);
        match prepared_res {
            Ok((raw_length, prepared)) => {
                batch_bytes = batch_bytes.saturating_add(raw_length);
                pending_batch.push(PendingMemoryDoc {
                    path,
                    stamp,
                    policy,
                    raw_length,
                    prepared,
                });
                if pending_batch.len() >= MEMORY_PUBLISH_BATCH_DOCS {
                    flush_memory_batch(
                        index,
                        owner.id.as_str(),
                        &mut pending_batch,
                        walk,
                        refresh,
                        budget,
                        gate,
                    )?;
                    batch_bytes = 0;
                }
            }
            Err(IndexError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock => {
                let fresh = FileStamp::read(&open_readonly(&path)?)?;
                let len = fresh.len;
                walk.note(&path, Some(fresh.clone()));
                refresh.add(&RefreshStats::new(fresh).pending(len));
            }
            Err(other) => return Err(other),
        }
    }
    flush_memory_batch(
        index,
        owner.id.as_str(),
        &mut pending_batch,
        walk,
        refresh,
        budget,
        gate,
    )
}

type PgMemoryPreparedRow = (
    PathBuf,
    FileStamp,
    String,
    u64,
    Option<crate::host_index::PreparedDocument>,
);

fn flush_pg_memory_batch(
    index: &mut HostIndex,
    owner_id: &str,
    pg_batch: &mut Vec<PgMemoryPreparedRow>,
    refresh: &mut ClassRefresh,
    gate: &mut Gate<'_>,
) -> Result<(), IndexError> {
    if pg_batch.is_empty() {
        return Ok(());
    }
    let chunk = std::mem::take(pg_batch);
    if let Some(_guard) = gate()? {
        index.refresh_source(|index| {
            for (seg, stamp, policy, raw_len, prepared) in &chunk {
                if let Some(doc) = prepared {
                    index.publish_document(doc)?;
                }
                index.save_source(
                    owner_id,
                    seg,
                    &SourceCursor {
                        stamp: stamp.clone(),
                        policy: policy.clone(),
                        end: *raw_len,
                        next_seq: 2,
                        boundary_sha256: String::new(),
                        full_verified_ms: 0,
                        verification_attempt_ms: 0,
                        complete: true,
                        rebuilding: false,
                    },
                )?;
            }
            Ok(())
        })?;
    } else {
        refresh.complete = false;
    }
    Ok(())
}

/// Index PostgreSQL `public.agent_files(namespace = 'memory')` blobs in a single
/// batched `get_agent_files_all` fetch and chunked index transactions.
pub(super) fn refresh_pg_memory_blobs(
    store: &Store,
    index: &mut HostIndex,
    owner: &Owner,
    blocked: &dyn Fn(&Path) -> bool,
    keep: &mut HashSet<String>,
    refresh: &mut ClassRefresh,
    gate: &mut Gate<'_>,
) -> Result<(), IndexError> {
    let blob_store = store.blob_store();
    let metas = blob_store
        .list_agent_files(&owner.id, "memory")
        .map_err(store_error)?;
    let visible_metas: Vec<_> = metas
        .into_iter()
        .filter(|m| !m.path.starts_with('.') && !m.path.contains("/."))
        .collect();
    let has_topic_files = visible_metas
        .iter()
        .any(|m| !m.path.eq_ignore_ascii_case("MEMORY.md") && m.path.ends_with(".md"));
    // Load all existing cursors for this agent in ONE query instead of N per-file queries.
    let existing_cursors: HashMap<String, SourceCursor> = index
        .agent_sources(owner.id.as_str())?
        .into_iter()
        .filter_map(|(seg, raw)| {
            serde_json::from_str::<SourceCursor>(&raw)
                .ok()
                .map(|c| (seg, c))
        })
        .collect();
    let mut changed_metas = Vec::new();
    for meta in visible_metas {
        if has_topic_files && meta.path.eq_ignore_ascii_case("MEMORY.md") {
            continue;
        }
        let seg = pg_agent_file_segment(owner.id.as_str(), "memory", &meta.path);
        if blocked(&seg) {
            continue;
        }
        let seg_key = seg.to_string_lossy().into_owned();
        keep.insert(seg_key.clone());
        let metadata = memory_metadata(owner, Some(meta.updated_at_ms));
        let stamp =
            FileStamp::virtual_blob(meta.size_bytes as u64, meta.version, meta.updated_at_ms);
        let policy = serde_json::to_string(&metadata)?;
        let unchanged = existing_cursors
            .get(&seg_key)
            .is_some_and(|old| old.stamp == stamp && old.policy == policy);
        if !unchanged {
            changed_metas.push((meta, seg, metadata, stamp, policy));
        }
    }
    if changed_metas.is_empty() {
        return Ok(());
    }
    let by_path: HashMap<String, crate::stateless::BlobEntry> = blob_store
        .get_agent_files_all(&owner.id, "memory")
        .map_err(store_error)?
        .into_iter()
        .map(|e| (e.path.clone(), e))
        .collect();
    let mut pg_batch: Vec<PgMemoryPreparedRow> =
        Vec::with_capacity(changed_metas.len().min(MEMORY_PUBLISH_BATCH_DOCS));
    for (meta, seg, metadata, stamp, policy) in changed_metas {
        let Some(entry) = by_path.get(&meta.path) else {
            continue;
        };
        let id = format!(
            "memory:{}:{}",
            owner.id,
            sha256(seg.as_os_str().as_encoded_bytes())
        );
        let raw = crate::raw_segment::RawCoordinate {
            segment: seg.clone(),
            offset: 0,
            length: entry.content.len() as u64,
            sha256: entry.sha256.clone(),
            seq: 1,
        };
        let prepared = if index.is_current(&id, &metadata, &raw)? {
            None
        } else {
            Some(crate::host_index::prepare_document_from_bytes(
                &entry.content,
                &id,
                &metadata,
                &raw,
            )?)
        };
        pg_batch.push((seg, stamp, policy, raw.length, prepared));
        if pg_batch.len() >= MEMORY_PUBLISH_BATCH_DOCS {
            flush_pg_memory_batch(index, owner.id.as_str(), &mut pg_batch, refresh, gate)?;
        }
    }
    flush_pg_memory_batch(index, owner.id.as_str(), &mut pg_batch, refresh, gate)
}

/// Check whether a recorded `host_agent_trees` entry is still current for this store.
///
/// On PostgreSQL stateless replicas, container overlayfs `(dev, ino)` values differ
/// across restarts:
/// - For `Memory`, local disk memory files are ignored and `policy` already embeds the
///   SHA-256 fingerprint of every `(path, sha256, version)` row in `agent_files`, so a
///   policy match is authoritative without comparing container inodes.
/// - For `Transcripts`, when the local agent root does not exist on this replica and
///   the recorded tree also observed no local files, skip redundant re-walks and tree
///   rewrites.
pub(super) fn is_tree_current_for_store(
    store: &Store,
    class: MemoryScope,
    root: &Path,
    agents: Option<(u64, u64)>,
    tree_json: &str,
) -> bool {
    if store.is_postgres() {
        if class == MemoryScope::Memory {
            return true;
        }
        if class == MemoryScope::Transcripts
            && fs::symlink_metadata(root).is_err_and(|e| e.kind() == io::ErrorKind::NotFound)
        {
            return serde_json::from_str::<Tree>(tree_json)
                .is_ok_and(|t| t.entries.iter().all(|(_, stamp)| stamp.is_none()));
        }
    }
    serde_json::from_str::<Tree>(tree_json).is_ok_and(|t| t.is_current(root, agents))
}

fn is_pg_blob_segment(owner: &str, segment: &Path) -> bool {
    parse_pg_agent_file_segment(segment).is_some_and(|(seg_agent, _, _)| seg_agent == owner)
        || extract_migrated_agent_blob_ref(owner, segment).is_some()
}

fn slice_verified_window(
    bytes: &[u8],
    coord: &crate::raw_segment::RawCoordinate,
) -> Option<Vec<u8>> {
    let start = usize::try_from(coord.offset).ok()?;
    let len = usize::try_from(coord.length).ok()?;
    let end = start.checked_add(len)?;
    let slice = bytes.get(start..end)?;
    if sha256(slice) != coord.sha256 {
        return None;
    }
    Some(slice.to_vec())
}

/// Verify authorization/redaction/digests and read bounded hit windows.
///
/// For PostgreSQL `agent_files` blob hits, avoids acquiring/releasing per-hit
/// distributed write leases and re-querying redaction receipts: uses the already-loaded
/// per-owner redaction map, caches blob bytes per `(owner, segment)` within the request,
/// and verifies the window SHA-256 directly against the live blob bytes.
pub(super) fn verify_and_read_search_hits(
    store: &Store,
    index: &HostIndex,
    scope: MemoryScope,
    hits: Vec<IndexHit>,
    unresolved: &HashMap<String, Vec<PathBuf>>,
) -> Result<HostHits, IndexError> {
    let mut out = Vec::with_capacity(hits.len());
    let mut blob_cache: HashMap<(String, PathBuf), Option<Vec<u8>>> = HashMap::new();
    for hit in hits {
        if store.is_postgres() && is_pg_blob_segment(&hit.metadata.agent_id, &hit.window.segment) {
            if unresolved
                .get(hit.metadata.agent_id.as_str())
                .is_some_and(|paths| paths.iter().any(|p| hit.window.segment.starts_with(p)))
            {
                continue;
            }
            let key = (hit.metadata.agent_id.clone(), hit.window.segment.clone());
            let cached = match blob_cache.entry(key) {
                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::hash_map::Entry::Vacant(v) => {
                    let loaded = store
                        .read_blob_bytes_for_segment(&hit.metadata.agent_id, &hit.window.segment)?;
                    v.insert(loaded)
                }
            };
            let Some(blob_bytes) = cached.as_deref() else {
                continue;
            };
            let Some(bytes) = slice_verified_window(blob_bytes, &hit.window) else {
                continue;
            };
            out.push((hit, bytes));
            continue;
        }
        let owner_id = crate::AgentId::parse(&hit.metadata.agent_id).map_err(store_error)?;
        let _guard = store.lock_agent_lifecycle(&owner_id).map_err(store_error)?;
        let Some(current) =
            index.document(std::slice::from_ref(&hit.metadata.agent_id), scope, &hit.id)?
        else {
            continue;
        };
        if current.raw.sha256 != hit.raw.sha256
            || store.is_host_path_redacted(&hit.metadata.agent_id, &hit.window.segment)?
        {
            continue;
        }
        // Return the matching bounded window, not an unbounded tool payload.
        // The whole-event coordinate remains available for explicit readback.
        match store.read_coordinate_bytes(&hit.metadata.agent_id, &hit.window) {
            Ok(bytes) => out.push((hit, bytes)),
            Err(IndexError::Io(err))
                if store.is_postgres() && err.kind() == io::ErrorKind::NotFound =>
            {
                // Skip migrated host-filesystem transcript rows absent on stateless pods.
                continue;
            }
            Err(err) => return Err(err),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_blob_segment_detection_enforces_owner_and_verifies_window_digest() {
        let own = pg_agent_file_segment("agt_owner", "memory", "notes.md");
        assert!(is_pg_blob_segment("agt_owner", &own));
        assert!(!is_pg_blob_segment("agt_other", &own));
        let migrated = PathBuf::from("/var/lib/anyagents/agents/agt_owner/memory/notes.md");
        assert!(is_pg_blob_segment("agt_owner", &migrated));
        assert!(!is_pg_blob_segment("agt_other", &migrated));

        let payload = b"prefix HOLD order suffix";
        let window_bytes = &payload[7..17];
        let coord = crate::raw_segment::RawCoordinate {
            segment: own,
            offset: 7,
            length: 10,
            sha256: sha256(window_bytes),
            seq: 1,
        };
        assert_eq!(
            slice_verified_window(payload, &coord).as_deref(),
            Some(window_bytes)
        );
        // Modified blob fails closed on digest mismatch.
        let tampered = b"prefix DONE order suffix";
        assert!(slice_verified_window(tampered, &coord).is_none());
    }
}
