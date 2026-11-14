//! Agents that run as a vendor's remote session (ADR 0010 D6, D7), wired into
//! this kernel: which harnesses they are, the adapter and the API key it
//! calls with, the spend cap, the launch, and the custody that takes their
//! sessions over after a restart.
//!
//! * `claude-cloud` is Claude Managed Agents
//!   ([`anyagents_remote::claude_managed`]). Its API key comes from the
//!   agent's bound API-key profile (`anyagents profiles set anthropic/<name>
//!   --key-file <file>`, then `--profile anthropic/<name>`), and stays in this
//!   process: the session reaches the coordination tools through `public_url`
//!   with a per-session token held in a vendor vault.
//! * Every metered remote agent has a cap: its own (`anyagents agents budget
//!   <agent> <usd>`, kept in `<config>/remote.toml`) or `[remote] budget_usd`
//!   in `<data>/anyagents.toml`. One without either does not start.

use std::path::Path;
use std::sync::Arc;

use anyagents_core::{AgentId, Store};
use anyagents_harness::Delivery;
use anyagents_remote::{Adapter, Executor};
use anyagents_supervisor::{Attached, Custody, LaunchRequest};

use crate::vendor::VendorHarness;

/// An agent's own remote settings, in its config directory.
pub const AGENT_CONFIG: &str = "remote.toml";

/// Whether `harness` runs as a vendor's remote session, with no local process.
pub(crate) fn is_session_harness(harness: &str) -> bool {
    anyagents_harness::normalize_harness_name(harness) == "claude-cloud"
}

/// What the adapter for `harness` guarantees for mail.
pub(crate) fn declared_delivery(_harness: &str) -> Delivery {
    anyagents_remote::claude_managed::DELIVERY
}

/// The adapter's name for `harness`, as listings show it.
pub(crate) fn adapter_name(_harness: &str) -> &'static str {
    anyagents_remote::claude_managed::ADAPTER
}

/// The Anthropic API key of the agent's bound profile. It is read here and
/// sent only to the Anthropic API; no session ever holds it.
fn anthropic_key(data: &Path, config_dir: &Path) -> Result<String, String> {
    let bound = crate::create::bound_profile_name(config_dir).ok_or_else(|| {
        "no Anthropic API key profile is bound: store one with `anyagents profiles set \
         anthropic/<name> --key-file <file>` and give the agent `--profile anthropic/<name>`"
            .to_string()
    })?;
    let (provider, profile) = bound
        .split_once('/')
        .unwrap_or(("anthropic", bound.as_str()));
    if !matches!(provider, "anthropic" | "anthropic-api") {
        return Err(format!(
            "profile `{bound}` is not an Anthropic API key profile; `claude-cloud` calls the \
             Anthropic API with an `anthropic/<name>` key"
        ));
    }
    let path = data
        .join("profiles")
        .join(provider)
        .join(profile)
        .join(".config/opencode/anthropic.key");
    let key = std::fs::read_to_string(&path).map_err(|err| {
        format!(
            "profile `{bound}` holds no API key ({}: {err}); set one with `anyagents profiles set \
             {bound} --key-file <file>`",
            path.display()
        )
    })?;
    Ok(key.trim().to_string())
}

/// The adapter a `harness` session is driven through.
fn adapter(data: &Path, harness: &str, config_dir: &Path) -> Result<Arc<dyn Adapter>, String> {
    if !is_session_harness(harness) {
        return Err(format!("`{harness}` is not a remote-session harness"));
    }
    let settings = crate::remote::Settings::load(data)?;
    let key = anthropic_key(data, config_dir)?;
    let adapter = anyagents_remote::claude_managed::ClaudeManaged::new(
        &key,
        settings.anthropic_base_url.as_deref(),
    )?
    .with_environment(settings.anthropic_environment_id);
    Ok(Arc::new(adapter))
}

#[derive(Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct AgentSettings {
    budget_usd: Option<f64>,
}

fn agent_settings(config_dir: &Path) -> Result<AgentSettings, String> {
    let path = config_dir.join(AGENT_CONFIG);
    match std::fs::read_to_string(&path) {
        Ok(text) => toml::from_str(&text).map_err(|err| format!("{}: {err}", path.display())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(AgentSettings::default()),
        Err(err) => Err(format!("cannot read {}: {err}", path.display())),
    }
}

/// The agent's spend cap in US cents: its own, else the deployment's default.
pub(crate) fn budget_cents(data: &Path, config_dir: &Path) -> Result<Option<i64>, String> {
    if let Some(usd) = agent_settings(config_dir)?.budget_usd {
        return crate::remote::usd_to_cents(usd).map(Some);
    }
    Ok(crate::remote::Settings::load(data)?.remote_budget_cents)
}

/// What an executor for `agent` starts with.
fn config(
    store: &Store,
    agent: &anyagents_core::Agent,
) -> Result<anyagents_remote::Config, String> {
    let data = store.data_dir();
    let config_dir = anyagents_core::agent_config_dir(data, &agent.id);
    let settings = crate::remote::require_public_url(data)?;
    let budget_cents = budget_cents(data, &config_dir)?;
    if budget_cents.is_none() {
        return Err(format!(
            "remote agent `{}` has no spend cap: set one with `anyagents agents budget {} <usd>` \
             or `budget_usd` under `[remote]` in {}",
            agent.name,
            agent.name,
            data.join(crate::remote::CONFIG_FILE).display()
        ));
    }
    let store = Arc::new(Store::open(data.join("state.db")).map_err(|err| err.to_string())?);
    Ok(anyagents_remote::Config {
        adapter: adapter(data, &agent.harness, &config_dir)?,
        agent: agent.clone(),
        system: crate::cloud_harness::build_cloud_system_prompt(&config_dir),
        mcp_url: settings.mcp_url(),
        token_ttl_ms: settings.ttl_ms(),
        budget_cents,
        host_tools: None,
        store,
    })
}

/// Start an agent's remote session: reconnect to the one its record names,
/// or create one.
pub(crate) fn launch(request: &LaunchRequest, store: &Store) -> Result<VendorHarness, String> {
    let config = config(store, &request.agent)
        .map_err(|why| format!("remote agent `{}` cannot start: {why}", request.agent.name))?;
    Executor::launch(config).map(VendorHarness::session)
}

/// Settle the session of an agent no kernel will drive by the agent's state:
/// end it for an archived, deleted or moved agent, interrupt it for a paused
/// one, park it for a sleeping one. The vendor is called on the remote
/// runtime: the caller (the supervisor's router at startup, the maintenance
/// pass) never waits on it.
fn settle(store: &Store, agent: &AgentId) {
    let Ok(Some(row)) = store.remote_execution(agent) else {
        return;
    };
    let config_dir = anyagents_core::agent_config_dir(store.data_dir(), agent);
    let adapter = match adapter(store.data_dir(), &row.harness, &config_dir) {
        Ok(adapter) => adapter,
        Err(why) => {
            eprintln!("anyagents-remote: {agent}: the session was left as it is: {why}");
            return;
        }
    };
    let store = match Store::open(store.data_dir().join("state.db")) {
        Ok(store) => store,
        Err(err) => {
            eprintln!("anyagents-remote: {agent}: the session was left as it is: {err}");
            return;
        }
    };
    let agent = agent.clone();
    anyagents_remote::spawn_blocking(move || {
        anyagents_remote::settle(&store, &agent, adapter.as_ref());
    });
}

/// End the remote sessions of agents that no longer run them while no
/// executor holds them: a parked session whose agent was archived since, or
/// moved to another harness. Run by the maintenance pass.
pub(crate) fn sweep(store: &Store) {
    let Ok(rows) = store.remote_executions() else {
        return;
    };
    for row in rows
        .iter()
        .filter(|row| row.is_live() && !row.is_attached())
    {
        let ends = match store.agent(&row.agent_id) {
            Ok(agent) => agent.state == "archived" || agent.harness != row.harness,
            Err(_) => false,
        };
        if ends {
            settle(store, &row.agent_id);
        }
    }
}

/// Before an agent is deleted, with its record: its remote session is
/// stopped and its token revoked, the vendor called in the background.
pub(crate) fn end_before_delete(store: &Store, agent: &AgentId) {
    let Ok(Some(row)) = store.remote_execution(agent) else {
        return;
    };
    let config_dir = anyagents_core::agent_config_dir(store.data_dir(), agent);
    match adapter(store.data_dir(), &row.harness, &config_dir) {
        Ok(adapter) => anyagents_remote::end_before_delete(store, agent, adapter),
        Err(why) => {
            let _ = store.revoke_remote_sessions(agent, None);
            eprintln!(
                "anyagents-remote: {agent}: the deleted agent's session was not stopped: {why}"
            );
        }
    }
}

/// Change an agent's spend cap: kept with the agent, and handed to its live
/// session at once, which resumes work paused at the old cap.
pub(crate) fn set_budget(
    store: &Store,
    agent: &anyagents_core::Agent,
    usd: f64,
) -> Result<serde_json::Value, String> {
    if !is_session_harness(&agent.harness) {
        return Err(format!(
            "`{}` runs on `{}`, which is not a remote session",
            agent.name, agent.harness
        ));
    }
    let cents = crate::remote::usd_to_cents(usd)?;
    let config_dir = anyagents_core::agent_config_dir(store.data_dir(), &agent.id);
    let text = toml::to_string(&AgentSettings {
        budget_usd: Some(cents as f64 / 100.0),
    })
    .map_err(|err| err.to_string())?;
    std::fs::create_dir_all(&config_dir).map_err(|err| err.to_string())?;
    crate::create::write_required_file(&config_dir.join(AGENT_CONFIG), text.as_bytes(), 0o600)
        .map_err(|err| err.to_string())?;
    let session = match store
        .remote_execution(&agent.id)
        .map_err(|e| e.to_string())?
    {
        Some(row) if row.is_live() => {
            let adapter = adapter(store.data_dir(), &row.harness, &config_dir)?;
            anyagents_remote::change_budget(store, &agent.id, adapter.as_ref(), cents)?;
            row.vendor_session
        }
        _ => None,
    };
    Ok(serde_json::json!({
        "agent": agent.name,
        "budget_usd": cents as f64 / 100.0,
        "session": session,
    }))
}

/// The custody `serve` gives its supervisor: remote sessions always outlive
/// the kernel and are taken over from their record; every other harness is
/// held by `inner`.
pub struct SessionCustody {
    inner: Arc<dyn Custody<VendorHarness>>,
    store: Arc<Store>,
}

impl SessionCustody {
    pub fn new(store: Arc<Store>, inner: Arc<dyn Custody<VendorHarness>>) -> Self {
        Self { inner, store }
    }

    fn take_over(&self, agent: &AgentId) -> Result<Option<Attached<VendorHarness>>, String> {
        let row = match self.store.remote_execution(agent) {
            Ok(Some(row)) if row.is_attached() => row,
            Ok(_) => return Ok(None),
            Err(err) => return Err(err.to_string()),
        };
        let current = self.store.agent(agent).map_err(|err| err.to_string())?;
        if current.harness != row.harness || !is_session_harness(&current.harness) {
            // The agent moved to another harness while no kernel ran.
            settle(&self.store, agent);
            return Ok(None);
        }
        // A session that cannot be configured now (its key or cap is gone)
        // is left to the launch, which says why on the agent.
        let config = match config(&self.store, &current) {
            Ok(config) => config,
            Err(why) => {
                eprintln!("anyagents-remote: {agent}: not taken over: {why}");
                return Ok(None);
            }
        };
        let Some((executor, lock)) = anyagents_remote::take_over(config)? else {
            return Ok(None);
        };
        Ok(Some(Attached {
            harness_name: executor.harness().to_string(),
            model: executor.model().map(str::to_string),
            started_at_ms: Some(executor.started_at_ms()),
            note: executor.note(),
            harness: VendorHarness::session(executor),
            lock,
            older: Vec::new(),
        }))
    }
}

impl Custody<VendorHarness> for SessionCustody {
    fn outlives_kernel(&self) -> bool {
        // A remote session always does.
        true
    }

    fn retained(&self) -> Vec<AgentId> {
        let mut agents = self.inner.retained();
        for agent in anyagents_remote::attached(&self.store) {
            if !agents.contains(&agent) {
                agents.push(agent);
            }
        }
        agents
    }

    fn attach(&self, agent: &AgentId) -> Result<Option<Attached<VendorHarness>>, String> {
        if let Some(attached) = self.take_over(agent)? {
            return Ok(Some(attached));
        }
        self.inner.attach(agent)
    }

    fn release(&self, agent: &AgentId) {
        if self
            .store
            .remote_execution(agent)
            .is_ok_and(|row| row.is_some_and(|row| row.is_live()))
        {
            settle(&self.store, agent);
        }
        self.inner.release(agent);
    }

    fn retain(&self, harness: VendorHarness) {
        if harness.remote_session().is_some() {
            harness.detach();
        } else {
            self.inner.retain(harness);
        }
    }

    fn note(&self, harness: &VendorHarness, note: &str) {
        if harness.remote_session().is_some() {
            harness.set_note(note);
        } else {
            self.inner.note(harness, note);
        }
    }

    fn superseded(&self, harness: &VendorHarness) -> bool {
        harness.remote_session().is_none() && self.inner.superseded(harness)
    }

    fn answers_live(&self, harness: &VendorHarness) -> bool {
        harness.remote_session().is_some() || self.inner.answers_live(harness)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_key_comes_only_from_a_bound_anthropic_profile_and_the_cap_from_config() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        let config = data.join("agents/agt_x/config");
        std::fs::create_dir_all(&config).unwrap();
        let err = anthropic_key(data, &config).unwrap_err();
        assert!(err.contains("profiles set"), "{err}");

        std::fs::write(config.join(".anyagents-profile"), "openai/default\n").unwrap();
        assert!(
            anthropic_key(data, &config)
                .unwrap_err()
                .contains("not an Anthropic")
        );

        std::fs::write(config.join(".anyagents-profile"), "anthropic/api-1\n").unwrap();
        assert!(
            anthropic_key(data, &config)
                .unwrap_err()
                .contains("holds no API key")
        );
        crate::create::set_provider_profile(data, "anthropic/api-1", "sk-ant-api03-test\n")
            .unwrap();
        assert_eq!(anthropic_key(data, &config).unwrap(), "sk-ant-api03-test");

        assert_eq!(budget_cents(data, &config).unwrap(), None);
        std::fs::write(
            data.join(crate::remote::CONFIG_FILE),
            "[remote]\nbudget_usd = 25\n",
        )
        .unwrap();
        assert_eq!(budget_cents(data, &config).unwrap(), Some(2500));
        std::fs::write(config.join(AGENT_CONFIG), "budget_usd = 2.5\n").unwrap();
        assert_eq!(budget_cents(data, &config).unwrap(), Some(250));
        std::fs::write(config.join(AGENT_CONFIG), "budget_usd = -1\n").unwrap();
        assert!(budget_cents(data, &config).is_err());
    }

    /// A remote agent starts only with a public URL to call back to, a cap
    /// and a key, and nothing reaches a vendor before all three are there.
    #[test]
    fn a_remote_agent_without_a_callback_cap_or_key_does_not_start() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        let store = Store::open(data.join("state.db")).unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("cloudy", "claude-cloud"))
            .unwrap();
        let config_dir = anyagents_core::agent_config_dir(data, &agent.id);
        std::fs::create_dir_all(&config_dir).unwrap();
        let request = LaunchRequest {
            agent: agent.clone(),
            data_dir: data.to_path_buf(),
            config_dir: config_dir.clone(),
            work_dir: data.join("work"),
            extra_env: Vec::new(),
            lock: None,
        };
        let why = |store: &Store| launch(&request, store).err().unwrap();
        assert!(why(&store).contains("public_url"), "{}", why(&store));
        std::fs::write(
            data.join(crate::remote::CONFIG_FILE),
            "[server]\npublic_url = \"https://agents.example.com\"\n",
        )
        .unwrap();
        assert!(why(&store).contains("no spend cap"), "{}", why(&store));
        std::fs::write(config_dir.join(AGENT_CONFIG), "budget_usd = 5\n").unwrap();
        assert!(why(&store).contains("profiles set"), "{}", why(&store));
        assert!(store.remote_execution(&agent.id).unwrap().is_none());
        assert!(store.live_remote_sessions(0).unwrap().is_empty());
        assert!(is_session_harness("anthropic-cloud"));
        assert!(!is_session_harness("openai-cloud"));
    }

    /// The custody `serve` runs, around keeper custody: a remote session a
    /// stopping kernel retains is taken over by the next, its note kept in
    /// the record; settling one whose agent no longer runs never holds up
    /// the caller (the supervisor's router at startup).
    #[test]
    fn the_production_custody_takes_remote_sessions_over_around_keeper_custody() {
        use anyagents_harness::{DeliverOutcome, Harness, OutboundMessage};
        use anyagents_remote::mock::{KEY, Vendor, wait_until};

        let vendor = Vendor::start();
        vendor.set_hold(true);
        let dir = crate::test_tempdir();
        let data = dir.path();
        std::fs::write(
            data.join(crate::remote::CONFIG_FILE),
            format!(
                "[server]\npublic_url = \"https://agents.example.com\"\n\
                 [remote]\nbudget_usd = 25\nanthropic_base_url = \"{}\"\n",
                vendor.base
            ),
        )
        .unwrap();
        crate::create::set_provider_profile(data, "anthropic/api-1", KEY).unwrap();
        let store = Arc::new(Store::open(data.join("state.db")).unwrap());
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("cloudy", "claude-cloud"))
            .unwrap();
        let work = data.join("work");
        std::fs::create_dir_all(&work).unwrap();
        store
            .set_agent_running(&agent.id, &work.display().to_string())
            .unwrap();
        let agent = store.agent(&agent.id).unwrap();
        let config_dir = anyagents_core::agent_config_dir(data, &agent.id);
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::write(config_dir.join(".anyagents-profile"), "anthropic/api-1\n").unwrap();
        let custody = SessionCustody::new(
            Arc::clone(&store),
            Arc::new(crate::keeper::KeeperCustody::new(data, Vec::new())),
        );
        assert!(custody.outlives_kernel());

        // The kernel's launch path: a remote session, no local process.
        let request = LaunchRequest {
            agent: agent.clone(),
            data_dir: data.to_path_buf(),
            config_dir: config_dir.clone(),
            work_dir: work.clone(),
            extra_env: Vec::new(),
            lock: None,
        };
        let mut harness = launch(&request, &store).unwrap();
        assert!(harness.remote_session().is_some());
        assert_eq!(harness.process_id(), None);
        let message = OutboundMessage {
            id: "msg_1".into(),
            seq: 1,
            text: "hello".into(),
            sender: None,
            reply_to: None,
            source_time_ms: None,
            context: Default::default(),
        };
        assert_eq!(harness.deliver(&message).unwrap(), DeliverOutcome::InFlight);
        let session = vendor.only_session();
        wait_until("the turn runs", || vendor.status(&session) == "running");

        // A stopping kernel retains it; the next one takes it over.
        custody.retain(harness);
        assert!(custody.retained().contains(&agent.id));
        assert_eq!(vendor.status(&session), "running");
        let attached = custody.attach(&agent.id).unwrap().unwrap();
        assert_eq!(attached.harness_name, "claude-cloud");
        assert!(attached.harness.remote_session().is_some());
        assert!(custody.answers_live(&attached.harness));
        assert!(!custody.superseded(&attached.harness));
        custody.note(&attached.harness, r#"{"turns_recorded":0}"#);
        assert_eq!(
            store
                .remote_execution(&agent.id)
                .unwrap()
                .unwrap()
                .note
                .as_deref(),
            Some(r#"{"turns_recorded":0}"#)
        );
        // The session consumed it under the last kernel: the new one knows,
        // and would never write it again.
        assert_eq!(
            attached.harness.proven("msg_1"),
            Some(anyagents_harness::Proof::Duplicate)
        );
        assert_eq!(vendor.lock().sessions.len(), 1, "no second session");
        drop(attached);

        // The agent was archived while no kernel held it: releasing it
        // returns at once, and the session is ended in the background.
        assert!(
            store
                .remote_execution(&agent.id)
                .unwrap()
                .unwrap()
                .is_attached()
        );
        store.set_agent_state(&agent.id, "archived").unwrap();
        vendor.set_hold(false);
        vendor.finish(&session);
        vendor.lock().archive_delay_ms = 1_500;
        let started = std::time::Instant::now();
        custody.release(&agent.id);
        assert!(
            started.elapsed() < std::time::Duration::from_millis(500),
            "release waited {:?} on the vendor",
            started.elapsed()
        );
        wait_until("the session is ended", || {
            vendor.lock().sessions[&session].archived
        });
        wait_until("and its record says so", || {
            store.remote_execution(&agent.id).unwrap().unwrap().status
                == anyagents_core::remote_execution_status::ENDED
        });
    }

    /// A deployment whose remote agents call the scripted vendor.
    fn deployment(vendor: &anyagents_remote::mock::Vendor) -> (tempfile::TempDir, Arc<Store>) {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path();
        std::fs::write(
            data.join(crate::remote::CONFIG_FILE),
            format!(
                "[server]\npublic_url = \"https://agents.example.com\"\n\
                 [remote]\nbudget_usd = 25\nanthropic_base_url = \"{}\"\n",
                vendor.base
            ),
        )
        .unwrap();
        crate::create::set_provider_profile(data, "anthropic/api-1", anyagents_remote::mock::KEY)
            .unwrap();
        let store = Arc::new(Store::open(data.join("state.db")).unwrap());
        (dir, store)
    }

    /// A running `claude-cloud` agent bound to the API-key profile, and the
    /// launch the supervisor would make for it.
    fn remote_agent(store: &Store, name: &str) -> LaunchRequest {
        let data = store.data_dir();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new(name, "claude-cloud"))
            .unwrap();
        let work = data.join(format!("work-{name}"));
        std::fs::create_dir_all(&work).unwrap();
        store
            .set_agent_running(&agent.id, &work.display().to_string())
            .unwrap();
        let config_dir = anyagents_core::agent_config_dir(data, &agent.id);
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::write(config_dir.join(".anyagents-profile"), "anthropic/api-1\n").unwrap();
        LaunchRequest {
            agent: store.agent(&agent.id).unwrap(),
            data_dir: data.to_path_buf(),
            config_dir,
            work_dir: work,
            extra_env: Vec::new(),
            lock: None,
        }
    }

    /// The minor review item: a session parked while its agent slept is
    /// ended once the agent is archived (the maintenance sweep), and a
    /// deleted agent's session is stopped with it.
    #[test]
    fn parked_sessions_of_archived_or_deleted_agents_are_ended() {
        use anyagents_harness::Harness;
        use anyagents_remote::mock::{Vendor, wait_until};
        let vendor = Vendor::start();
        let (_dir, store) = deployment(&vendor);
        let mut sessions = Vec::new();
        for name in ["archived-later", "deleted-later"] {
            let request = remote_agent(&store, name);
            let mut harness = launch(&request, &store).unwrap();
            let message = anyagents_harness::OutboundMessage {
                id: format!("msg_{name}"),
                seq: 1,
                text: "hello".into(),
                sender: None,
                reply_to: None,
                source_time_ms: None,
                context: Default::default(),
            };
            harness.deliver(&message).unwrap();
            wait_until("the turn ends", || harness.turns_served() == 1);
            // The agent sleeps: its session is parked, not ended.
            store.set_agent_state(&request.agent.id, "idle").unwrap();
            drop(harness);
            let row = store.remote_execution(&request.agent.id).unwrap().unwrap();
            assert_eq!(row.status, anyagents_core::remote_execution_status::PARKED);
            sessions.push((request.agent.id.clone(), row.vendor_session.unwrap()));
        }

        let (archived, archived_session) = &sessions[0];
        store.set_agent_state(archived, "archived").unwrap();
        sweep(&store);
        wait_until("the archived agent's session is ended", || {
            vendor.lock().sessions[archived_session].archived
        });

        let (deleted, deleted_session) = &sessions[1];
        end_before_delete(&store, deleted);
        store.delete_agent(deleted).unwrap();
        wait_until("the deleted agent's session is ended", || {
            vendor.lock().sessions[deleted_session].archived
        });
    }
}
