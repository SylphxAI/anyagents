//! Neutral host-level sleep-time memory index synchronization and consolidation.

use super::Store;
use crate::db::DbExecExt;
use crate::db_params;
use crate::error::Error;
use crate::id::AgentId;

impl Store {
    /// Neutral host-level sleep-time memory index and semantic consolidation.
    /// Synchronizes `<agent_root>/memory/MEMORY.md` and `HostIndex` whenever topic
    /// files in `<agent_root>/memory/<topic>.md` are added, modified, or deleted,
    /// and when unconsolidated substantive conversation turns exist and cooldown
    /// has elapsed, runs the 4-phase LLM sleep-time semantic consolidation pass.
    pub fn consolidate_agent_sleep_memory(&self, agent_id: &AgentId) -> Result<bool, Error> {
        self.consolidate_agent_sleep_memory_with_options(agent_id, true)
            .map(|(modified, _)| modified)
    }

    /// Performs deterministic disk topic synchronization and optionally executes
    /// the LLM sleep-time consolidation pass (`allow_llm`). Returns
    /// `(memory_modified, executed_llm_call)`.
    pub fn consolidate_agent_sleep_memory_with_options(
        &self,
        agent_id: &AgentId,
        allow_llm: bool,
    ) -> Result<(bool, bool), Error> {
        let agent = self.agent(agent_id)?;
        if agent.state == "archived" {
            return Ok((false, false));
        }
        let agent_root = self.data_dir().join("agents").join(agent.id.as_str());
        let mem_dir = agent_root.join("memory");
        if !mem_dir.is_dir() {
            return Ok((false, false));
        }
        let mut sleep = crate::learned_memory::read_sleep_state(&agent_root);
        let now = crate::now_ms();

        // Phase 1a: Fast deterministic disk topic check & index sync.
        let updated_topics = crate::learned_memory::list_memory_topics(&agent_root)?;
        let mem_path = mem_dir.join("MEMORY.md");
        let mem_exists = mem_path.is_file();
        let current_mem = std::fs::read_to_string(&mem_path).unwrap_or_default();
        let mem_mtime_ms = std::fs::metadata(&mem_path)
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| i64::try_from(d.as_millis()).unwrap_or(0))
            .unwrap_or(0);
        let any_newer_topic = updated_topics
            .iter()
            .any(|t| t.updated_at_ms > mem_mtime_ms);
        let is_truncated =
            crate::learned_memory::bounded_section(&current_mem).contains("[Memory truncated;");
        let is_legacy_index =
            current_mem.contains("\n## Topics (") && !current_mem.contains("\n## Active Topics");
        let missing_index_with_topics = !mem_exists && !updated_topics.is_empty();
        let mut modified = false;
        if missing_index_with_topics
            || any_newer_topic
            || is_truncated
            || is_legacy_index
            || sleep.consolidated_topics != updated_topics.len()
        {
            crate::learned_memory::rebuild_memory_index(
                &agent_root,
                &agent.name,
                agent.role.as_deref(),
            )?;
            let _ = self.sync_agent_memory_index(&agent);
            modified = true;
        }

        // Phase 1b: Delta turn watermarks & quote/noise filtering.
        let (max_in_seq, latest_in_ms, max_out_ms, substantive_turns) = {
            let conn = self.read_conn();
            let (max_in_seq, latest_in_ms): (i64, i64) = conn
                .db_query_one(
                    "SELECT COALESCE(MAX(seq), 0), COALESCE(MAX(received_at_ms), 0)
                     FROM messages WHERE agent_id = ?1",
                    &db_params![agent.id.as_str()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap_or((0, 0));
            let max_out_ms: i64 = conn
                .db_query_one(
                    "SELECT COALESCE(MAX(sent_at_ms), 0) FROM outbound_messages WHERE agent_id = ?1",
                    &db_params![agent.id.as_str()],
                    |r| r.get(0),
                )
                .unwrap_or(0);

            let has_new_turns =
                max_in_seq > sleep.last_inbound_seq || max_out_ms > sleep.last_outbound_ms;
            if !has_new_turns {
                (max_in_seq, latest_in_ms, max_out_ms, None)
            } else {
                let mut raw_turns: Vec<crate::learned_memory::SleepTurnDelta> = Vec::new();
                if max_in_seq > sleep.last_inbound_seq
                    && let Ok(rows) = conn.db_query_vec(
                        "SELECT received_at_ms, COALESCE(sender, 'unknown'), body, id, seq
                         FROM messages
                         WHERE agent_id = ?1 AND seq > ?2
                         ORDER BY seq DESC
                         LIMIT 30",
                        &db_params![agent.id.as_str(), sleep.last_inbound_seq],
                        |row| {
                            Ok((
                                row.get::<i64>(0).unwrap_or(0),
                                row.get::<String>(1).unwrap_or_default(),
                                row.get::<String>(2).unwrap_or_default(),
                                row.get::<String>(3).unwrap_or_default(),
                                row.get::<i64>(4).unwrap_or_default(),
                            ))
                        },
                    )
                {
                    for (ts, peer, body, id, seq) in rows {
                        if let Some(clean) = crate::learned_memory::clean_sleep_turn_text(&body) {
                            raw_turns.push(crate::learned_memory::SleepTurnDelta {
                                timestamp_ms: ts,
                                direction: "in".to_string(),
                                peer,
                                text: clean,
                                citation: Some(crate::learned_memory::RawCitation::whole(
                                    &id, &body, seq, "in",
                                )),
                            });
                        }
                    }
                }
                if max_out_ms > sleep.last_outbound_ms
                    && let Ok(rows) = conn.db_query_vec(
                        "SELECT sent_at_ms, destination, body, id, seq
                         FROM outbound_messages
                         WHERE agent_id = ?1 AND sent_at_ms > ?2
                         ORDER BY sent_at_ms DESC
                         LIMIT 30",
                        &db_params![agent.id.as_str(), sleep.last_outbound_ms],
                        |row| {
                            Ok((
                                row.get::<i64>(0).unwrap_or(0),
                                row.get::<String>(1).unwrap_or_default(),
                                row.get::<String>(2).unwrap_or_default(),
                                row.get::<String>(3).unwrap_or_default(),
                                row.get::<i64>(4).unwrap_or_default(),
                            ))
                        },
                    )
                {
                    for (ts, peer, body, id, seq) in rows {
                        if let Some(clean) = crate::learned_memory::clean_sleep_turn_text(&body) {
                            raw_turns.push(crate::learned_memory::SleepTurnDelta {
                                timestamp_ms: ts,
                                direction: "out".to_string(),
                                peer,
                                text: clean,
                                citation: Some(crate::learned_memory::RawCitation::whole(
                                    &id, &body, seq, "out",
                                )),
                            });
                        }
                    }
                }
                raw_turns.sort_by_key(|t| t.timestamp_ms);
                let filtered = crate::learned_memory::filter_substantive_sleep_turns(raw_turns);
                let start = filtered.len().saturating_sub(30);
                let bounded_turns = filtered[start..].to_vec();
                (max_in_seq, latest_in_ms, max_out_ms, Some(bounded_turns))
            }
        };

        let Some(substantive_turns) = substantive_turns else {
            if modified {
                sleep.last_consolidated_ms = now;
                sleep.consolidated_topics = updated_topics.len();
                let _ = crate::learned_memory::write_sleep_state(&agent_root, &sleep);
            }
            return Ok((modified, false));
        };

        // If all delta turns were empty, "NO_REPLY", system prompt echoes, or
        // routine schedule ticks without outbound actions, advance watermarks
        // without invoking the LLM.
        if substantive_turns.is_empty() {
            sleep.last_consolidated_ms = now;
            sleep.last_inbound_seq = max_in_seq;
            sleep.last_outbound_ms = max_out_ms;
            sleep.consolidated_topics = updated_topics.len();
            let _ = crate::learned_memory::write_sleep_state(&agent_root, &sleep);
            return Ok((modified, false));
        }

        if !allow_llm {
            if modified {
                sleep.consolidated_topics = updated_topics.len();
                let _ = crate::learned_memory::write_sleep_state(&agent_root, &sleep);
            }
            return Ok((modified, false));
        }

        // Phase 2: Resolve LLM endpoint first; if none is configured (e.g. offline test mode),
        // advance watermarks cleanly without waiting for LLM cooldown.
        let Some(endpoint) =
            crate::learned_memory::resolve_sleep_model_endpoint(self.data_dir(), &agent.id)
        else {
            sleep.last_consolidated_ms = now;
            sleep.last_inbound_seq = max_in_seq;
            sleep.last_outbound_ms = max_out_ms;
            sleep.consolidated_topics = updated_topics.len();
            let _ = crate::learned_memory::write_sleep_state(&agent_root, &sleep);
            return Ok((modified, false));
        };

        // Phase 2b: Quiescence & cooldown gate (2m turn quiescence + 10m consolidation cooldown).
        let force = std::env::var("ANYAGENTS_SLEEP_CONSOLIDATION_FORCE")
            .ok()
            .as_deref()
            == Some("1");
        let custom_url_set = std::env::var("ANYAGENTS_SLEEP_MODEL_URL")
            .ok()
            .is_some_and(|s| !s.trim().is_empty());
        let latest_turn_ms = latest_in_ms.max(max_out_ms);
        let turn_quiescent = custom_url_set || now.saturating_sub(latest_turn_ms) >= 120_000;
        let consolidation_cooled_down = sleep.last_consolidated_ms == 0
            || now.saturating_sub(sleep.last_consolidated_ms) >= 600_000;
        if !force && (!turn_quiescent || !consolidation_cooled_down) {
            if modified {
                sleep.consolidated_topics = updated_topics.len();
                let _ = crate::learned_memory::write_sleep_state(&agent_root, &sleep);
            }
            return Ok((modified, false));
        }

        let Ok(plan) = crate::learned_memory::request_sleep_consolidation_plan(
            &endpoint,
            &agent_root,
            &agent.name,
            agent.role.as_deref(),
            &updated_topics,
            &substantive_turns,
        ) else {
            // Preserve `last_inbound_seq` / `last_outbound_ms` on transient HTTP failure
            // so the unconsolidated turns are retried on a subsequent pass.
            if modified {
                sleep.consolidated_topics = updated_topics.len();
                let _ = crate::learned_memory::write_sleep_state(&agent_root, &sleep);
            }
            return Ok((modified, true));
        };

        // Phase 4: Apply the plan without destroying anything: every topic
        // version a write replaces, and every topic a merge retires, moves to
        // the searchable history archive, and each written topic cites the
        // immutable raw turns it was distilled from (`RawCitation`). Raw
        // history is only read. Then rebuild MEMORY.md, synchronize the host
        // index and advance watermarks.
        for op in &plan.operations {
            match op {
                crate::learned_memory::SleepMemoryOp::Upsert {
                    topic,
                    description,
                    content,
                    sources,
                } => {
                    let cited: Vec<_> = sources
                        .iter()
                        .filter_map(|n| substantive_turns.get(*n))
                        .filter_map(|turn| turn.citation.clone())
                        .collect();
                    // A plan that names no valid turn is cited to the whole
                    // window it was distilled from.
                    let cited = if cited.is_empty() {
                        substantive_turns
                            .iter()
                            .filter_map(|turn| turn.citation.clone())
                            .collect()
                    } else {
                        cited
                    };
                    crate::learned_memory::consolidate_memory_topic(
                        &agent_root,
                        &agent.name,
                        agent.role.as_deref(),
                        topic,
                        description.as_deref(),
                        content,
                        &cited,
                        now,
                    )?;
                    modified = true;
                }
                crate::learned_memory::SleepMemoryOp::Delete { topic } => {
                    let (removed, _) = crate::learned_memory::retire_memory_topic(
                        &agent_root,
                        &agent.name,
                        agent.role.as_deref(),
                        topic,
                        now,
                    )?;
                    if removed {
                        modified = true;
                    }
                }
            }
        }
        if !plan.operations.is_empty() {
            crate::learned_memory::rebuild_memory_index(
                &agent_root,
                &agent.name,
                agent.role.as_deref(),
            )?;
            let _ = self.sync_agent_memory_index(&agent);
        }

        let final_topics = crate::learned_memory::list_memory_topics(&agent_root)?;
        sleep.last_consolidated_ms = now;
        sleep.last_inbound_seq = max_in_seq;
        sleep.last_outbound_ms = max_out_ms;
        sleep.consolidated_topics = final_topics.len();
        let _ = crate::learned_memory::write_sleep_state(&agent_root, &sleep);
        Ok((modified, true))
    }
}
