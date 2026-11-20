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
    let mut changed_metas = Vec::new();
    for meta in visible_metas {
        if has_topic_files && meta.path.eq_ignore_ascii_case("MEMORY.md") {
            continue;
        }
        let seg = pg_agent_file_segment(owner.id.as_str(), "memory", &meta.path);
        if blocked(&seg) {
            continue;
        }
        keep.insert(seg.to_string_lossy().into_owned());
        let metadata = memory_metadata(owner, Some(meta.updated_at_ms));
        let stamp =
            FileStamp::virtual_blob(meta.size_bytes as u64, meta.version, meta.updated_at_ms);
        let policy = serde_json::to_string(&metadata)?;
        let unchanged = index
            .source_cursor(&metadata.agent_id, &seg)?
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
