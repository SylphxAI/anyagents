//! Owner-only organization knowledge grant commands (OIDC Device Flow).

use anyagents_core::Error;
use clap::{Args, Subcommand, ValueEnum};
use serde_json::{Value, json};

use crate::auth_device::{self, StoredControlAuth};
use crate::control_cli::post_device_json;

const PATH: &str = "/v1/knowledge";

#[derive(Args)]
pub(crate) struct KnowledgeCliArgs {
    /// Coordinator URL (must match the saved Device Flow login).
    #[arg(long, global = true)]
    url: Option<String>,
    #[command(subcommand)]
    action: KnowledgeCommand,
}

#[derive(Subcommand)]
enum KnowledgeCommand {
    /// Grant an agent access to a namespace or namespace pattern (e.g. 'org:*').
    Grant {
        /// Stable agent ID (agt_...), not an agent name.
        agent_id: String,
        namespace_pattern: String,
        #[arg(long, default_value = "read")]
        mode: Mode,
    },
    /// List grants, optionally filtered by agent ID.
    #[command(visible_alias = "grants")]
    List {
        #[arg(long)]
        agent_id: Option<String>,
    },
    /// Revoke a grant by its ID as returned by list.
    Revoke { grant_id: String },
}

#[derive(Clone, Copy, ValueEnum)]
enum Mode {
    Read,
    Write,
    Propose,
}

impl KnowledgeCommand {
    fn body(&self) -> Value {
        match self {
            Self::Grant {
                agent_id,
                namespace_pattern,
                mode,
            } => json!({
                "action": "grant",
                "agent_id": agent_id,
                "namespace_pattern": namespace_pattern,
                "mode": match mode {
                    Mode::Read => "read",
                    Mode::Write => "write",
                    Mode::Propose => "propose",
                },
            }),
            Self::List { agent_id: Some(id) } => json!({"action": "grants", "agent_id": id}),
            Self::List { agent_id: None } => json!({"action": "grants"}),
            Self::Revoke { grant_id } => json!({"action": "revoke", "grant_id": grant_id}),
        }
    }
}

pub(crate) fn run(args: KnowledgeCliArgs) -> Result<(), Error> {
    let auth = auth_device::load_control_auth()?;
    let env_url = std::env::var("ANYAGENTS_CONTROL_URL").ok();
    let requested_url = args.url.as_deref().or(env_url.as_deref());
    let response = call(auth.as_ref(), requested_url, &args.action)?;
    println!("{response:#}");
    Ok(())
}

fn call(
    auth: Option<&StoredControlAuth>,
    requested_url: Option<&str>,
    action: &KnowledgeCommand,
) -> Result<Value, Error> {
    let sign_in = || {
        Error::Message(
            "knowledge grants require an owner login; run \x60anyagents auth login --url <url>\x60"
                .into(),
        )
    };
    let auth = auth
        .filter(|a| !a.is_expired_at(jiff::Timestamp::now().as_second()))
        .ok_or_else(sign_in)?;
    let bearer = auth.bearer_token().ok_or_else(sign_in)?;
    let base_url = auth.coordinator_url.trim().trim_end_matches('/');
    let parsed = url::Url::parse(base_url).map_err(|_| sign_in())?;
    if !matches!(parsed.scheme(), "https" | "http")
        || parsed.host().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(sign_in());
    }
    if requested_url
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .is_some_and(|url| url.trim_end_matches('/') != base_url)
    {
        return Err(Error::Message(
            "coordinator URL does not match the saved Device Flow login; sign in to that coordinator first".into(),
        ));
    }
    post_device_json(base_url, bearer, PATH, &action.body())
}
