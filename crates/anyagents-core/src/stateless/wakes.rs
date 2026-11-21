//! Cross-replica PostgreSQL `LISTEN`/`NOTIFY` wake bus (`anyagents_wakes`).
//!
//! - Publishers execute `SELECT pg_notify('anyagents_wakes', $1)` on the regular
//!   pooled connection (fully compatible with both direct PostgreSQL and
//!   transaction-mode PgBouncer).
//! - Subscribers maintain one dedicated `PgListener` on `direct_url`
//!   (`ANYAGENTS_DIRECT_DATABASE_URL`, falling back to primary URL) with
//!   exponential backoff reconnect and clean shutdown when dropped.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgListener;

use crate::db::{acquire_pg, block_on_pg, pg_runtime};
use crate::error::Error;
use crate::id::AgentId;
use crate::store::Wake;

/// PostgreSQL `LISTEN`/`NOTIFY` channel carrying cross-replica mailbox & agent wakes.
pub const PG_WAKE_CHANNEL: &str = "anyagents_wakes";

/// Decoded cross-replica wake notification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PgWakeEvent {
    Wake {
        origin_replica_id: Option<String>,
        wake: Wake,
    },
    Changes {
        origin_replica_id: Option<String>,
    },
}

/// Encode a [`Wake`] or state change into a compact ASCII `pg_notify` payload.
pub fn encode_wake_payload(origin_replica_id: Option<&str>, wake: Option<&Wake>) -> String {
    let body = match wake {
        Some(Wake::Mail { agent_id }) => format!("m:{}", agent_id.as_str()),
        Some(Wake::Agent { agent_id }) => format!("a:{}", agent_id.as_str()),
        None => "s".to_string(),
    };
    match origin_replica_id.map(str::trim).filter(|s| !s.is_empty()) {
        Some(origin) => format!("{origin}|{body}"),
        None => body,
    }
}

/// Decode a `pg_notify` payload received on [`PG_WAKE_CHANNEL`].
pub fn decode_wake_payload(payload: &str) -> Option<PgWakeEvent> {
    let trimmed = payload.trim();
    if trimmed.is_empty() {
        return None;
    }
    let (origin, body) = match trimmed.split_once('|') {
        Some((orig, rest)) if !orig.is_empty() => (Some(orig.to_string()), rest),
        _ => (None, trimmed),
    };
    if body == "s" {
        return Some(PgWakeEvent::Changes {
            origin_replica_id: origin,
        });
    }
    if let Some(id_str) = body.strip_prefix("m:") {
        let agent_id = AgentId::parse(id_str).ok()?;
        return Some(PgWakeEvent::Wake {
            origin_replica_id: origin,
            wake: Wake::Mail { agent_id },
        });
    }
    if let Some(id_str) = body.strip_prefix("a:") {
        let agent_id = AgentId::parse(id_str).ok()?;
        return Some(PgWakeEvent::Wake {
            origin_replica_id: origin,
            wake: Wake::Agent { agent_id },
        });
    }
    None
}

/// Publish one wake or state invalidation onto `PG_WAKE_CHANNEL`.
pub fn publish_pg_wake(
    pool: &PgPool,
    origin_replica_id: Option<&str>,
    wake: Option<&Wake>,
) -> Result<(), Error> {
    let payload = encode_wake_payload(origin_replica_id, wake);
    let pool = pool.clone();
    block_on_pg(async move {
        let mut conn = acquire_pg(&pool).await?;
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(PG_WAKE_CHANNEL)
            .bind(&payload)
            .execute(&mut *conn)
            .await
            .map_err(Error::from)?;
        Ok(())
    })
}

/// Background `LISTEN anyagents_wakes` subscriber tied to a `PgShared` lifecycle.
///
/// Signals stop immediately on `Drop` so no listener connections leak when a
/// `Store` handle closes.
pub struct PgWakeListener {
    stop: tokio::sync::watch::Sender<bool>,
    connected: Arc<AtomicBool>,
}

impl PgWakeListener {
    pub fn start<F>(direct_url: String, on_event: F) -> Self
    where
        F: Fn(PgWakeEvent) + Send + Sync + 'static,
    {
        let (stop, mut stop_rx) = tokio::sync::watch::channel(false);
        let connected = Arc::new(AtomicBool::new(false));
        let connected_task = Arc::clone(&connected);

        pg_runtime().spawn(async move {
            let mut backoff = Duration::from_millis(200);
            loop {
                if *stop_rx.borrow() {
                    break;
                }
                let connect_res = PgListener::connect(&direct_url).await;
                let mut listener = match connect_res {
                    Ok(mut listener) => {
                        if let Err(_err) = listener.listen(PG_WAKE_CHANNEL).await {
                            connected_task.store(false, Ordering::Release);
                            tokio::select! {
                                biased;
                                _ = stop_rx.wait_for(|s| *s) => break,
                                _ = tokio::time::sleep(backoff) => {}
                            }
                            backoff = (backoff * 2).min(Duration::from_secs(2));
                            continue;
                        }
                        backoff = Duration::from_millis(200);
                        connected_task.store(true, Ordering::Release);
                        listener
                    }
                    Err(_) => {
                        connected_task.store(false, Ordering::Release);
                        tokio::select! {
                            biased;
                            _ = stop_rx.wait_for(|s| *s) => break,
                            _ = tokio::time::sleep(backoff) => {}
                        }
                        backoff = (backoff * 2).min(Duration::from_secs(2));
                        continue;
                    }
                };

                loop {
                    tokio::select! {
                        biased;
                        _ = stop_rx.wait_for(|s| *s) => {
                            connected_task.store(false, Ordering::Release);
                            return;
                        }
                        notif = listener.recv() => {
                            match notif {
                                Ok(n) => {
                                    if let Some(event) = decode_wake_payload(n.payload()) {
                                        on_event(event);
                                    }
                                }
                                Err(_) => {
                                    connected_task.store(false, Ordering::Release);
                                    // Notify watchers on reconnect so no transitions are missed.
                                    on_event(PgWakeEvent::Changes { origin_replica_id: None });
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });

        Self { stop, connected }
    }

    /// Whether the background `PgListener` is currently subscribed to `anyagents_wakes`.
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    /// Wait up to `timeout` for the background `PgListener` to finish `LISTEN`.
    pub fn wait_connected(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while !self.is_connected() {
            if std::time::Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        true
    }
}

impl Drop for PgWakeListener {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wake_payload_roundtrips_all_variants() {
        let id = AgentId::generate();
        let mail = encode_wake_payload(
            Some("pod-a"),
            Some(&Wake::Mail {
                agent_id: id.clone(),
            }),
        );
        assert_eq!(
            decode_wake_payload(&mail),
            Some(PgWakeEvent::Wake {
                origin_replica_id: Some("pod-a".into()),
                wake: Wake::Mail {
                    agent_id: id.clone()
                },
            })
        );
        let agent = encode_wake_payload(
            None,
            Some(&Wake::Agent {
                agent_id: id.clone(),
            }),
        );
        assert_eq!(
            decode_wake_payload(&agent),
            Some(PgWakeEvent::Wake {
                origin_replica_id: None,
                wake: Wake::Agent { agent_id: id },
            })
        );
        let changes = encode_wake_payload(Some("pod-b"), None);
        assert_eq!(
            decode_wake_payload(&changes),
            Some(PgWakeEvent::Changes {
                origin_replica_id: Some("pod-b".into()),
            })
        );
        assert_eq!(decode_wake_payload(""), None);
        assert_eq!(decode_wake_payload("bad:payload"), None);
    }
}
