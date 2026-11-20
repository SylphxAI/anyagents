//! Live per-turn mid-turn steering spool watcher & callback emitter for `anyagents-guest turn`.
//!
//! While a detached sandbox turn executes, the serverless coordinator atomically drops
//! JSON [`SteerEnvelope`] files into `<state_dir>/turns/<turn_id>.steer.d/` (via temp+rename).
//! [`SteerPump`] polls this spool directory in strict numeric `seq` order, injects each
//! message immediately into the live harness's native mid-turn steering API (`Harness::deliver`),
//! waits for vendor-verified consumption proof (`Harness::proven`), archives consumed files,
//! and POSTs `steer_accepted` / `steer_consumed` / `steer_refused` callbacks to
//! `/v1/internal/guest/events`.
//!
//! Fail-closed invariant:
//! - On any `DeliveryLevel::TurnEnd` harness (such as one-shot `gemini`), steer files are
//!   never claimed or marked consumed, leaving them safely undelivered in coordinator storage
//!   for turn-end drain.
//! - `steer_consumed` is emitted strictly in contiguous ascending `seq` order and ONLY after
//!   the live harness adapter returns a verified consumption proof (`DeliverOutcome::Accepted`,
//!   `DeliverOutcome::Duplicate`, `Proof::Taken`, or `Proof::Duplicate`).

use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyagents_harness::{
    DeliverOutcome, DeliveryLevel, HarnessError, OutboundMessage, Proof, TurnState,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::guest::{GuestConfig, GuestHarness};
use crate::turn::{TurnWatchdog, post_callback_with_retry};

/// Directory suffix under `<state_dir>/turns/` holding atomic per-turn mid-turn steer files.
pub const STEER_DIR_SUFFIX: &str = ".steer.d";
const CONSUMED_SUBDIR: &str = ".consumed";
const STEER_CALLBACK_TIMEOUT_SECS: u64 = 10;
const STEER_CALLBACK_RETRIES: usize = 3;

/// Atomic JSON envelope written by the coordinator into `<state_dir>/turns/<turn_id>.steer.d/`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SteerEnvelope {
    pub turn_id: String,
    pub message_id: String,
    pub seq: i64,
    pub outbound: OutboundMessage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submitted_at_ms: Option<i64>,
}

/// Verified mid-turn steer consumption record included in `steer_consumed` and `turn_complete`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumedSteer {
    pub message_id: String,
    pub seq: i64,
    pub proof: String,
    pub turn_finished: bool,
}

/// Compute the per-turn steer spool directory `<turns_dir>/<turn_id>.steer.d`.
pub fn steer_spool_dir(turns_dir: &Path, turn_id: &str) -> PathBuf {
    turns_dir.join(format!("{turn_id}{STEER_DIR_SUFFIX}"))
}

/// Build a lexicographically AND numerically ordered safe filename for a steer envelope:
/// `<20-digit-zero-padded-seq>-<sanitized-message-id>.json`.
pub fn steer_filename(seq: i64, message_id: &str) -> String {
    let clamped = seq.max(0) as u64;
    let safe_id: String = message_id
        .trim()
        .chars()
        .take(80)
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let safe_id = if safe_id.is_empty() {
        "msg".to_string()
    } else {
        safe_id
    };
    format!("{clamped:020}-{safe_id}.json")
}

/// Ensure the per-turn steer spool directory exists with owner-only permissions (`0700`).
pub fn ensure_steer_spool_dir(turns_dir: &Path, turn_id: &str) -> Result<PathBuf, String> {
    let dir = steer_spool_dir(turns_dir, turn_id);
    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("cannot create steer spool dir {}: {e}", dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700));
    }
    Ok(dir)
}

#[derive(Clone, Debug)]
struct QueuedSteer {
    file_path: PathBuf,
    message_id: String,
    seq: i64,
    outbound: OutboundMessage,
}

#[derive(Clone, Debug)]
struct InflightSteer {
    file_path: PathBuf,
    message_id: String,
    seq: i64,
    outbound: OutboundMessage,
    proven: Option<ConsumedSteer>,
    archived: bool,
}

/// Stateful per-turn mid-turn steer watcher and native harness injector.
pub(crate) struct SteerPump {
    turn_id: String,
    agent_id: String,
    harness_name: String,
    steer_dir: PathBuf,
    callback_url: String,
    callback_token: String,
    discovered_ids: HashSet<String>,
    pending: VecDeque<QueuedSteer>,
    inflight: VecDeque<InflightSteer>,
    consumed: Vec<ConsumedSteer>,
}

impl SteerPump {
    pub(crate) fn new(
        turns_dir: &Path,
        turn_id: &str,
        agent_id: &str,
        harness_name: &str,
        callback_url: &str,
        callback_token: &str,
    ) -> Self {
        let steer_dir = ensure_steer_spool_dir(turns_dir, turn_id)
            .unwrap_or_else(|_| steer_spool_dir(turns_dir, turn_id));
        Self {
            turn_id: turn_id.to_string(),
            agent_id: agent_id.to_string(),
            harness_name: harness_name.to_string(),
            steer_dir,
            callback_url: callback_url.to_string(),
            callback_token: callback_token.to_string(),
            discovered_ids: HashSet::new(),
            pending: VecDeque::new(),
            inflight: VecDeque::new(),
            consumed: Vec::new(),
        }
    }

    /// Scan `<turn_id>.steer.d`, inject pending steers into the live harness, and
    /// commit contiguous vendor-verified consumption proofs via `steer_consumed` callbacks.
    ///
    /// Returns `true` if any steering progress occurred (injection or consumption proof).
    pub(crate) fn tick(
        &mut self,
        adapter: &mut dyn GuestHarness,
        state_dir: &Path,
        env: &[(String, String)],
        file_cfg: Option<&GuestConfig>,
        watchdog: &TurnWatchdog,
    ) -> bool {
        let delivery = adapter.health().delivery;
        if delivery.level == DeliveryLevel::TurnEnd {
            return false;
        }

        let mut progressed = false;
        self.scan_spool(state_dir, env, file_cfg);

        while let Some(steer) = self.pending.front().cloned() {
            match adapter.deliver(&steer.outbound) {
                Ok(DeliverOutcome::Accepted { turn_finished }) => {
                    self.pending.pop_front();
                    watchdog.touch();
                    progressed = true;
                    let proof = format!("accepted:{}", adapter.health().delivery.proof);
                    let record = ConsumedSteer {
                        message_id: steer.message_id.clone(),
                        seq: steer.seq,
                        proof,
                        turn_finished,
                    };
                    if self.inflight.is_empty() {
                        archive_steer_file(&self.steer_dir, &steer.file_path);
                        self.emit_steer_consumed(&record);
                        self.consumed.push(record);
                    } else {
                        // Preserve strict contiguous seq ordering behind earlier in-flight steers.
                        archive_steer_file(&self.steer_dir, &steer.file_path);
                        self.inflight.push_back(InflightSteer {
                            file_path: steer.file_path,
                            message_id: steer.message_id,
                            seq: steer.seq,
                            outbound: steer.outbound,
                            proven: Some(record),
                            archived: true,
                        });
                    }
                }
                Ok(DeliverOutcome::Duplicate) => {
                    self.pending.pop_front();
                    progressed = true;
                    let record = ConsumedSteer {
                        message_id: steer.message_id.clone(),
                        seq: steer.seq,
                        proof: "duplicate".to_string(),
                        turn_finished: false,
                    };
                    if self.inflight.is_empty() {
                        archive_steer_file(&self.steer_dir, &steer.file_path);
                        self.emit_steer_consumed(&record);
                        self.consumed.push(record);
                    } else {
                        archive_steer_file(&self.steer_dir, &steer.file_path);
                        self.inflight.push_back(InflightSteer {
                            file_path: steer.file_path,
                            message_id: steer.message_id,
                            seq: steer.seq,
                            outbound: steer.outbound,
                            proven: Some(record),
                            archived: true,
                        });
                    }
                }
                Ok(DeliverOutcome::InFlight) => {
                    self.pending.pop_front();
                    watchdog.touch();
                    progressed = true;
                    self.emit_steer_accepted(&steer.message_id, steer.seq);
                    self.inflight.push_back(InflightSteer {
                        file_path: steer.file_path,
                        message_id: steer.message_id,
                        seq: steer.seq,
                        outbound: steer.outbound,
                        proven: None,
                        archived: false,
                    });
                }
                Ok(DeliverOutcome::Deferred) => {
                    // Preserve strict seq ordering; retry on next tick while turn runs.
                    break;
                }
                Ok(DeliverOutcome::Rejected { reason, .. }) => {
                    self.pending.pop_front();
                    archive_steer_file(&self.steer_dir, &steer.file_path);
                    self.emit_steer_refused(&steer.message_id, steer.seq, &reason);
                    progressed = true;
                }
                Ok(DeliverOutcome::Quota { .. }) | Err(HarnessError::Dead) | Err(_) => {
                    // Fail closed: keep file unconsumed so coordinator drains safely at turn end.
                    break;
                }
            }
        }

        if self.poll_inflight_proofs_live(adapter, Some(watchdog)) {
            progressed = true;
        }
        progressed
    }

    /// Re-check all in-flight steers for consumption proofs right before/after `adapter.stop()`.
    pub(crate) fn drain_final_proofs(&mut self, adapter: &dyn GuestHarness) {
        let _ = self.poll_inflight_proofs(adapter, None);
    }

    /// Returns `true` when the live harness is still executing a turn carrying at least
    /// one in-flight or unfinished consumed mid-turn steer.
    pub(crate) fn has_active_work(
        &self,
        adapter: &dyn GuestHarness,
        turn_state: TurnState,
    ) -> bool {
        if turn_state != TurnState::InTurn {
            return false;
        }
        if !self.inflight.is_empty() {
            return true;
        }
        self.consumed.iter().any(|c| {
            !matches!(
                adapter.proven(&c.message_id),
                Some(Proof::Taken {
                    turn_finished: true
                }) | Some(Proof::Duplicate)
            )
        })
    }

    /// Check if any consumed or in-flight steer reported a harness-level failure.
    pub(crate) fn first_failure(&self, adapter: &dyn GuestHarness) -> Option<String> {
        for item in &self.inflight {
            if let Some(err) = adapter.failure_for_id(&item.message_id) {
                return Some(err);
            }
        }
        for item in &self.consumed {
            if let Some(err) = adapter.failure_for_id(&item.message_id) {
                return Some(err);
            }
        }
        None
    }

    /// Look up the latest non-empty answer text across consumed steers (newest first).
    pub(crate) fn latest_answer(
        &self,
        adapter: &dyn GuestHarness,
        state_dir: &Path,
    ) -> Option<String> {
        for item in self.consumed.iter().rev() {
            if let Some(ans) = adapter
                .answer_for(&item.message_id, state_dir)
                .filter(|s| !s.trim().is_empty())
            {
                return Some(ans);
            }
        }
        None
    }

    pub(crate) fn consumed_steers(&self) -> &[ConsumedSteer] {
        &self.consumed
    }

    fn scan_spool(
        &mut self,
        state_dir: &Path,
        env: &[(String, String)],
        cfg: Option<&GuestConfig>,
    ) {
        let Ok(entries) = std::fs::read_dir(&self.steer_dir) else {
            return;
        };
        let mut found: Vec<QueuedSteer> = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            // Ignore hidden temp files (`.tmp*`), `.consumed/`, and non-JSON files.
            if name.starts_with('.') || !name.ends_with(".json") || !path.is_file() {
                continue;
            }
            let Ok(raw) = std::fs::read_to_string(&path) else {
                continue;
            };
            let Some(mut parsed) = parse_steer_file(&raw, &self.turn_id, &path) else {
                continue;
            };
            if self.discovered_ids.contains(&parsed.message_id) {
                continue;
            }
            parsed.outbound.text =
                crate::media::hydrate_turn_media_refs(&parsed.outbound.text, state_dir, env, cfg);
            found.push(parsed);
        }
        if found.is_empty() {
            return;
        }
        found.sort_by(|a, b| {
            a.seq
                .cmp(&b.seq)
                .then_with(|| a.message_id.cmp(&b.message_id))
                .then_with(|| a.file_path.cmp(&b.file_path))
        });
        for item in found {
            if self.discovered_ids.insert(item.message_id.clone()) {
                self.pending.push_back(item);
            }
        }
    }

    fn poll_inflight_proofs_live(
        &mut self,
        adapter: &mut dyn GuestHarness,
        watchdog: Option<&TurnWatchdog>,
    ) -> bool {
        let mut progressed = self.poll_inflight_proofs(adapter, watchdog);
        let health = adapter.health();
        if health.turn_state != TurnState::InTurn || self.inflight.is_empty() {
            return progressed;
        }
        let proof_label = health.delivery.proof.to_string();
        let mut refused_indices: Vec<(usize, String)> = Vec::new();
        for (idx, item) in self.inflight.iter_mut().enumerate() {
            if item.proven.is_some() {
                continue;
            }
            match adapter.deliver(&item.outbound) {
                Ok(DeliverOutcome::Accepted { turn_finished }) => {
                    if let Some(wd) = watchdog {
                        wd.touch();
                    }
                    if !item.archived {
                        archive_steer_file(&self.steer_dir, &item.file_path);
                        item.archived = true;
                    }
                    item.proven = Some(ConsumedSteer {
                        message_id: item.message_id.clone(),
                        seq: item.seq,
                        proof: proof_label.clone(),
                        turn_finished,
                    });
                    progressed = true;
                }
                Ok(DeliverOutcome::Duplicate) => {
                    if !item.archived {
                        archive_steer_file(&self.steer_dir, &item.file_path);
                        item.archived = true;
                    }
                    item.proven = Some(ConsumedSteer {
                        message_id: item.message_id.clone(),
                        seq: item.seq,
                        proof: "duplicate".to_string(),
                        turn_finished: false,
                    });
                    progressed = true;
                }
                Ok(DeliverOutcome::Rejected { reason, .. }) => {
                    refused_indices.push((idx, reason));
                }
                Ok(
                    DeliverOutcome::InFlight
                    | DeliverOutcome::Deferred
                    | DeliverOutcome::Quota { .. },
                )
                | Err(_) => {}
            }
        }
        for (idx, reason) in refused_indices.into_iter().rev() {
            if let Some(item) = self.inflight.remove(idx) {
                archive_steer_file(&self.steer_dir, &item.file_path);
                self.emit_steer_refused(&item.message_id, item.seq, &reason);
                progressed = true;
            }
        }
        while self.inflight.front().is_some_and(|i| i.proven.is_some()) {
            let item = self.inflight.pop_front().expect("checked front");
            let record = item.proven.expect("checked proven");
            self.emit_steer_consumed(&record);
            self.consumed.push(record);
            progressed = true;
        }
        progressed
    }

    fn poll_inflight_proofs(
        &mut self,
        adapter: &dyn GuestHarness,
        watchdog: Option<&TurnWatchdog>,
    ) -> bool {
        let mut progressed = false;
        let proof_label = adapter.health().delivery.proof.to_string();

        for item in &mut self.inflight {
            if item.proven.is_some() {
                continue;
            }
            match adapter.proven(&item.message_id) {
                Some(Proof::Taken { turn_finished }) => {
                    if let Some(wd) = watchdog {
                        wd.touch();
                    }
                    if !item.archived {
                        archive_steer_file(&self.steer_dir, &item.file_path);
                        item.archived = true;
                    }
                    item.proven = Some(ConsumedSteer {
                        message_id: item.message_id.clone(),
                        seq: item.seq,
                        proof: proof_label.clone(),
                        turn_finished,
                    });
                    progressed = true;
                }
                Some(Proof::Duplicate) => {
                    if !item.archived {
                        archive_steer_file(&self.steer_dir, &item.file_path);
                        item.archived = true;
                    }
                    item.proven = Some(ConsumedSteer {
                        message_id: item.message_id.clone(),
                        seq: item.seq,
                        proof: "duplicate".to_string(),
                        turn_finished: false,
                    });
                    progressed = true;
                }
                None => {}
            }
        }

        // Emit `steer_consumed` strictly in contiguous ascending `seq` order.
        while self.inflight.front().is_some_and(|i| i.proven.is_some()) {
            let item = self.inflight.pop_front().expect("checked front");
            let record = item.proven.expect("checked proven");
            self.emit_steer_consumed(&record);
            self.consumed.push(record);
            progressed = true;
        }
        progressed
    }

    fn emit_steer_accepted(&self, message_id: &str, seq: i64) {
        let payload = json!({
            "event_type": "steer_accepted",
            "turn_id": self.turn_id,
            "agent_id": self.agent_id,
            "harness": self.harness_name,
            "message_id": message_id,
            "seq": seq,
            "timestamp_ms": now_unix_ms(),
        });
        let _ = post_callback_with_retry(
            &self.callback_url,
            &self.callback_token,
            &payload,
            1,
            STEER_CALLBACK_TIMEOUT_SECS,
        );
    }

    fn emit_steer_consumed(&self, record: &ConsumedSteer) {
        let payload = json!({
            "event_type": "steer_consumed",
            "turn_id": self.turn_id,
            "agent_id": self.agent_id,
            "harness": self.harness_name,
            "message_id": record.message_id,
            "seq": record.seq,
            "proof": record.proof,
            "turn_finished": record.turn_finished,
            "timestamp_ms": now_unix_ms(),
        });
        let _ = post_callback_with_retry(
            &self.callback_url,
            &self.callback_token,
            &payload,
            STEER_CALLBACK_RETRIES,
            STEER_CALLBACK_TIMEOUT_SECS,
        );
    }

    fn emit_steer_refused(&self, message_id: &str, seq: i64, reason: &str) {
        let payload = json!({
            "event_type": "steer_refused",
            "turn_id": self.turn_id,
            "agent_id": self.agent_id,
            "harness": self.harness_name,
            "message_id": message_id,
            "seq": seq,
            "error_text": reason,
            "timestamp_ms": now_unix_ms(),
        });
        let _ = post_callback_with_retry(
            &self.callback_url,
            &self.callback_token,
            &payload,
            1,
            STEER_CALLBACK_TIMEOUT_SECS,
        );
    }
}

fn parse_steer_file(raw: &str, expected_turn_id: &str, path: &Path) -> Option<QueuedSteer> {
    if let Ok(env) = serde_json::from_str::<SteerEnvelope>(raw) {
        if !env.turn_id.trim().is_empty() && env.turn_id.trim() != expected_turn_id {
            return None;
        }
        let message_id = if env.message_id.trim().is_empty() {
            env.outbound.id.trim().to_string()
        } else {
            env.message_id.trim().to_string()
        };
        if message_id.is_empty() || env.seq <= 0 {
            return None;
        }
        let mut outbound = env.outbound;
        if outbound.id.trim().is_empty() {
            outbound.id = message_id.clone();
        }
        outbound.seq = env.seq;
        return Some(QueuedSteer {
            file_path: path.to_path_buf(),
            message_id,
            seq: env.seq,
            outbound,
        });
    }
    let outbound = serde_json::from_str::<OutboundMessage>(raw).ok()?;
    if outbound.id.trim().is_empty() || outbound.seq <= 0 {
        return None;
    }
    Some(QueuedSteer {
        file_path: path.to_path_buf(),
        message_id: outbound.id.clone(),
        seq: outbound.seq,
        outbound,
    })
}

fn archive_steer_file(steer_dir: &Path, file_path: &Path) {
    let consumed_dir = steer_dir.join(CONSUMED_SUBDIR);
    if std::fs::create_dir_all(&consumed_dir).is_ok()
        && let Some(fname) = file_path.file_name()
        && std::fs::rename(file_path, consumed_dir.join(fname)).is_ok()
    {
        return;
    }
    let _ = std::fs::remove_file(file_path);
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_harness::Context;

    #[test]
    fn steer_filename_orders_numerically_and_sanitizes_message_ids() {
        let f2 = steer_filename(2, "msg_01alpha");
        let f10 = steer_filename(10, "msg_01beta");
        assert!(f2 < f10, "{f2} must sort before {f10}");
        assert_eq!(f2, "00000000000000000002-msg_01alpha.json");
        let dirty = steer_filename(3, "../bad/id");
        assert!(!dirty.contains('/'));
        assert!(!dirty.contains(".."));
    }

    #[test]
    fn parse_steer_file_enforces_turn_id_and_accepts_outbound_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("00000000000000000002-msg_a.json");
        let outbound = OutboundMessage {
            id: "msg_a".into(),
            seq: 2,
            text: "pivot to step 2".into(),
            sender: Some("operator".into()),
            reply_to: None,
            source_time_ms: None,
            context: Context::default(),
        };
        let env = SteerEnvelope {
            turn_id: "turn_1".into(),
            message_id: "msg_a".into(),
            seq: 2,
            outbound: outbound.clone(),
            submitted_at_ms: Some(1_700_000_000_000),
        };
        let raw = serde_json::to_string(&env).unwrap();
        assert!(parse_steer_file(&raw, "turn_1", &path).is_some());
        assert!(
            parse_steer_file(&raw, "turn_other", &path).is_none(),
            "mismatched turn_id must be ignored fail-closed"
        );

        let plain = serde_json::to_string(&outbound).unwrap();
        let parsed = parse_steer_file(&plain, "turn_1", &path).unwrap();
        assert_eq!(parsed.message_id, "msg_a");
        assert_eq!(parsed.seq, 2);
    }
}
