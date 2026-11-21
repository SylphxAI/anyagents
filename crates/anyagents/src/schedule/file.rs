//! File watch observations live in the schedule owner's execution environment.

use std::path::PathBuf;
use std::time::Duration;

use anyagents_core::{AgentId, AgentSchedule, ExecutionRuntime, Store};

use crate::sandbox_exec::{
    block_on_provider, resolve_registry_for_binding, sandbox_handle_from_binding,
};
use crate::sandbox_providers::providers::{ExecRequest, ProviderKind};

use super::WatchEval;

pub(super) const PROBE_INTERVAL_MS: i64 = 5_000;
const TIMEOUT_MS: u64 = 2_000;
const SANDBOX_PREFIX: &str = "sandbox:";
const UNOBSERVED: &str = "sandbox:unobserved";

pub(super) fn sandbox_baseline(sched: &AgentSchedule) -> bool {
    sched
        .watch_baseline
        .as_deref()
        .is_some_and(|b| b.starts_with(SANDBOX_PREFIX))
}

pub(super) fn prepare(
    store: &Store,
    agent_id: &AgentId,
    path: &str,
    now: i64,
) -> Result<(String, Option<String>), String> {
    // Serialize the runtime check and initial probe with runtime transitions.
    let _lifecycle = store
        .lock_agent_lifecycle(agent_id)
        .map_err(|err| err.to_string())?;
    let agent = store.agent(agent_id).map_err(|err| err.to_string())?;
    let (path, baseline) = match agent.runtime {
        ExecutionRuntime::Local => {
            let p = PathBuf::from(path);
            let abs = if p.is_absolute() {
                p
            } else {
                agent.workdir.as_ref().map(PathBuf::from)
                    .unwrap_or_else(|| anyagents_core::agent_work_dir(store.data_dir(), agent_id))
                    .join(p)
            };
            let path = abs.to_string_lossy().into_owned();
            let baseline = local_fingerprint(&path);
            (path, baseline)
        }
        ExecutionRuntime::Sandbox { .. } => {
            let baseline = sandbox_fingerprint(store, agent_id, path, now)
                .map(|value| format!("{SANDBOX_PREFIX}{value}"))
                .unwrap_or_else(|_| UNOBSERVED.into());
            (path.to_string(), baseline)
        }
        ExecutionRuntime::Cloud => return Err(
            "file watches are not supported for runtime cloud (no persistent filesystem); use an event/webhook watch instead".into(),
        ),
    };
    Ok((format!("file:{path}"), Some(baseline)))
}

/// Evaluate the file predicate and report whether to use the local kernel cadence.
pub(super) fn evaluate(
    store: &Store,
    sched: &AgentSchedule,
    path: &str,
    now: i64,
) -> (WatchEval, bool) {
    let pending =
        |observed: &str| WatchEval::pending(sched.watch_baseline.clone(), Some(observed.into()));
    let Ok(_lifecycle) = store.try_lock_agent_lifecycle(&sched.agent_id) else {
        return (
            WatchEval::pending(sched.watch_baseline.clone(), sched.last_observed.clone()),
            false,
        );
    };
    let Ok(agent) = store.agent(&sched.agent_id) else {
        return (pending("owner_unavailable"), false);
    };
    match agent.runtime {
        ExecutionRuntime::Local if !sandbox_baseline(sched) => {
            let current = local_fingerprint(path);
            (compare(sched.watch_baseline.as_deref(), current), true)
        }
        ExecutionRuntime::Sandbox { .. } => {
            let current = match sandbox_fingerprint(store, &agent.id, path, now) {
                Ok(current) => current,
                Err(status) => return (pending(status), false),
            };
            let baseline = sched
                .watch_baseline
                .as_deref()
                .filter(|base| *base != UNOBSERVED)
                .and_then(|base| base.strip_prefix(SANDBOX_PREFIX));
            // Seed from the first real sandbox observation, never from a previous
            // host observation (including watches persisted by older versions).
            let mut eval = match baseline {
                Some(base) => compare(Some(base), current),
                None => WatchEval::pending(Some(current.clone()), Some(current)),
            };
            eval.baseline = eval.baseline.map(|base| format!("{SANDBOX_PREFIX}{base}"));
            (eval, false)
        }
        // A runtime transition must not reinterpret a sandbox path on the host.
        _ => (pending("file_watch_runtime_unsupported"), false),
    }
}

fn compare(base: Option<&str>, current: String) -> WatchEval {
    if current.starts_with("probe_error:") {
        return WatchEval::pending(base.map(str::to_string), Some(current));
    }
    match base {
        Some(base) if current != base => {
            WatchEval::fired(Some(base.into()), Some(format!("{base}->{current}")))
        }
        Some(base) => WatchEval::pending(Some(base.into()), Some(current)),
        None if current != "missing" => WatchEval::fired(Some(current.clone()), Some(current)),
        None => WatchEval::pending(Some(current.clone()), Some(current)),
    }
}

fn local_fingerprint(path: &str) -> String {
    match std::fs::metadata(path) {
        Ok(meta) => {
            let mtime_ms = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis())
                .unwrap_or(0);
            format!("exists:{mtime_ms}:{}", meta.len())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => "missing".to_string(),
        Err(e) => format!("probe_error:stat:{:?}", e.kind()),
    }
}

// The default sandbox image ships Python 3. Use argv, not a shell, and distinguish
// ENOENT from permissions/transport failures. Never read the file's contents.
const STAT: &str = r#"
import os, sys
try:
    info = os.stat(sys.argv[1])
except FileNotFoundError:
    print('missing')
else:
    print(f'exists:{max(0, info.st_mtime_ns // 1000000)}:{info.st_size}')
"#;

fn sandbox_fingerprint(
    store: &Store,
    agent_id: &AgentId,
    path: &str,
    now: i64,
) -> Result<String, &'static str> {
    let unavailable = "sandbox_unavailable";
    let failure = "probe_error:sandbox_stat";
    let binding = store
        .sandbox_binding(agent_id)
        .map_err(|_| failure)?
        .ok_or(unavailable)?;
    if binding.lease_state != "ready"
        || binding
            .lease_expires_at_ms
            .is_some_and(|expires| expires <= now)
    {
        return Err(unavailable);
    }
    let handle = sandbox_handle_from_binding(&binding).ok_or(unavailable)?;
    if handle.provider == ProviderKind::Local || handle.sandbox_id.trim().is_empty() {
        return Err(unavailable);
    }
    let registry = resolve_registry_for_binding(store, &binding).map_err(|_| failure)?;
    if registry.active_kind() != handle.provider {
        return Err(unavailable);
    }
    let provider = registry.active();
    let req = ExecRequest::argv(["python3", "-I", "-c", STAT, path]).with_timeout_ms(TIMEOUT_MS);
    let out = block_on_provider(async {
        tokio::time::timeout(
            Duration::from_millis(TIMEOUT_MS),
            provider.exec(&handle, &req),
        )
        .await
    })
    .map_err(|_| failure)?
    .map_err(|_| failure)?;
    let value = out.stdout.trim();
    if out.exit_code == 0 && (value == "missing" || valid_fingerprint(value)) {
        Ok(value.into())
    } else {
        Err(failure)
    }
}

fn valid_fingerprint(value: &str) -> bool {
    value
        .strip_prefix("exists:")
        .and_then(|value| value.split_once(':'))
        .is_some_and(|(mtime, size)| mtime.parse::<u64>().is_ok() && size.parse::<u64>().is_ok())
}

#[cfg(test)]
mod tests;
