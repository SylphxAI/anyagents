//! Shared CLI flags and USD budget parser for orthogonal `harness × runtime` commands.

use std::path::{Path, PathBuf};

use anyagents_core::Error;
use clap::Args;

use crate::client;

/// Reusable `--runtime` and `--budget-usd` flags flattened onto `spawn`,
/// `agents create`, and `agents harness`.
#[derive(Args, Debug, Default, Clone)]
pub(crate) struct RuntimeCliArgs {
    /// Execution runtime (`local`, `sandbox`, `sandbox:<provider>`, or `cloud`).
    #[arg(long)]
    pub(crate) runtime: Option<String>,
    /// Optional hard remote spend cap in US dollars (e.g. `2.50` -> `2_500_000` micro-USD).
    #[arg(long = "budget-usd")]
    pub(crate) budget_usd: Option<String>,
}

impl RuntimeCliArgs {
    pub(crate) fn budget_cap_micros(&self) -> Result<Option<u64>, Error> {
        self.budget_usd
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(parse_budget_usd_to_micros)
            .transpose()
    }
}

/// Top-level `anyagents spawn` CLI arguments.
#[derive(Args, Debug)]
pub(crate) struct SpawnCliArgs {
    /// Optional agent name (`[a-z0-9][a-z0-9_-]*`). Auto-generated when omitted.
    pub(crate) name: Option<String>,
    /// Role template slug (for example `general`) that dynamically provides role instructions.
    #[arg(long)]
    pub(crate) role: Option<String>,
    #[arg(long)]
    pub(crate) harness: Option<String>,
    #[command(flatten)]
    pub(crate) runtime_args: RuntimeCliArgs,
    #[arg(long)]
    pub(crate) model: Option<String>,
    /// Optional upstream provider account profile (e.g. `acct-a`, `acct-b`, `anthropic/acct-b`).
    #[arg(long)]
    pub(crate) profile: Option<String>,
    /// Optional repository (`<org>/<repo>`, local repo path, or `inherit`) to provision a Git repository of the agent's own.
    #[arg(long)]
    pub(crate) repo: Option<String>,
    /// Optional Git branch name for the managed checkout when `--repo` is set (defaults to `agent/<name>`).
    #[arg(long)]
    pub(crate) branch: Option<String>,
    /// Optional starting Git ref/branch when `--repo` is set (defaults to `origin/HEAD`).
    #[arg(long)]
    pub(crate) base: Option<String>,
    #[arg(long)]
    pub(crate) workdir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) description: Option<String>,
    #[arg(long, visible_alias = "instruction", visible_alias = "system-prompt")]
    pub(crate) instructions: Option<String>,
    #[arg(long)]
    pub(crate) parent: Option<String>,
    #[arg(long)]
    pub(crate) prompt: Option<String>,
}

/// Parse a non-negative US dollar decimal string into integer micro-USD (`1_000_000` = US$1.00)
/// without floating-point rounding drift.
pub(crate) fn parse_budget_usd_to_micros(raw: &str) -> Result<u64, Error> {
    let trimmed = raw.trim().strip_prefix('$').unwrap_or(raw.trim()).trim();
    if trimmed.is_empty() {
        return Err(Error::Message(
            "invalid --budget-usd: value cannot be empty".into(),
        ));
    }
    if trimmed.starts_with('-') {
        return Err(Error::Message(format!(
            "invalid --budget-usd `{raw}`: must be non-negative"
        )));
    }
    let trimmed = trimmed.strip_prefix('+').unwrap_or(trimmed);
    let (whole_str, frac_str) = match trimmed.split_once('.') {
        Some((w, f)) => {
            if f.contains('.') {
                return Err(Error::Message(format!(
                    "invalid --budget-usd `{raw}`: expected a decimal number"
                )));
            }
            (w, f)
        }
        None => (trimmed, ""),
    };
    if whole_str.is_empty() && frac_str.is_empty() {
        return Err(Error::Message(format!(
            "invalid --budget-usd `{raw}`: expected digits"
        )));
    }
    if !whole_str.bytes().all(|b| b.is_ascii_digit())
        || !frac_str.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(Error::Message(format!(
            "invalid --budget-usd `{raw}`: expected non-negative decimal dollars"
        )));
    }
    let whole: u64 = if whole_str.is_empty() {
        0
    } else {
        whole_str
            .parse::<u64>()
            .map_err(|_| Error::Message(format!("invalid --budget-usd `{raw}`: overflow")))?
    };
    if frac_str.len() > 6 && frac_str[6..].bytes().any(|b| b != b'0') {
        return Err(Error::Message(format!(
            "invalid --budget-usd `{raw}`: at most 6 decimal places (micro-USD) are supported"
        )));
    }
    let frac_padded: String = format!("{:0<6}", &frac_str[..frac_str.len().min(6)]);
    let frac: u64 = frac_padded
        .parse::<u64>()
        .map_err(|_| Error::Message(format!("invalid --budget-usd `{raw}`")))?;
    let micros = whole
        .checked_mul(1_000_000)
        .and_then(|w| w.checked_add(frac))
        .ok_or_else(|| Error::Message(format!("invalid --budget-usd `{raw}`: overflow")))?;
    if i64::try_from(micros).is_err() {
        return Err(Error::Message(format!(
            "invalid --budget-usd `{raw}`: exceeds maximum supported cap"
        )));
    }
    Ok(micros)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn harness_cmd(
    data: &Path,
    agent: &str,
    harness: Option<&str>,
    runtime: Option<&str>,
    budget_cap_micros: Option<u64>,
    model: Option<&str>,
    profile: Option<&str>,
) -> Result<(), Error> {
    let clean_agent = agent.trim().strip_prefix("agent:").unwrap_or(agent.trim());
    let (route_suffix, body) =
        if runtime.is_some() || budget_cap_micros.is_some() || harness.is_none() {
            (
                "runtime",
                serde_json::json!({
                    "harness": harness,
                    "runtime": runtime,
                    "budget_cap_micros": budget_cap_micros,
                    "model": model,
                    "profile": profile,
                }),
            )
        } else {
            (
                "harness",
                serde_json::json!({
                    "harness": harness.unwrap_or_default(),
                    "model": model,
                    "profile": profile,
                }),
            )
        };
    let budget_opt = budget_cap_micros.map(|cap| (cap > 0).then_some(cap));
    if client::can_call_server(data) {
        match client::call::<serde_json::Value>(
            data,
            "POST",
            &format!("/v1/agents/{clean_agent}/{route_suffix}"),
            Some(body.to_string().into_bytes()),
            &client::authority(data),
        ) {
            Ok(result) => {
                if client::agent_identity().is_none()
                    && let Ok(store) = crate::agent::open(data)
                {
                    let _ = crate::create::configure_agent_runtime_with_options(
                        &store,
                        data,
                        "operator",
                        clean_agent,
                        harness,
                        runtime,
                        model,
                        profile,
                        budget_opt,
                    );
                }
                let result =
                    result.ok_or_else(|| Error::Message("the server answered nothing".into()))?;
                if crate::ui::is_pretty() {
                    crate::agent::presentation::harness(&result, clean_agent);
                } else {
                    println!("{}", result);
                }
                return Ok(());
            }
            Err(err) if err.contains("404") && client::agent_identity().is_none() => {}
            Err(err) => return Err(Error::Message(err)),
        }
    }
    if client::agent_identity().is_some() {
        return Err(Error::Message(
            "administrative authority required (agent session cannot bypass server boundary)"
                .into(),
        ));
    }
    let store = crate::agent::open(data)?;
    let out = crate::create::configure_agent_runtime_with_options(
        &store,
        data,
        "operator",
        clean_agent,
        harness,
        runtime,
        model,
        profile,
        budget_opt,
    )?;
    if crate::ui::is_pretty() {
        crate::agent::presentation::harness(&out, clean_agent);
    } else {
        println!("{out}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_usd_budgets_to_exact_micros_and_rejects_invalid() {
        assert_eq!(parse_budget_usd_to_micros("0").unwrap(), 0);
        assert_eq!(parse_budget_usd_to_micros("1").unwrap(), 1_000_000);
        assert_eq!(parse_budget_usd_to_micros("2.50").unwrap(), 2_500_000);
        assert_eq!(parse_budget_usd_to_micros("$0.05").unwrap(), 50_000);
        assert_eq!(parse_budget_usd_to_micros(".000001").unwrap(), 1);
        assert_eq!(
            parse_budget_usd_to_micros("12.34567800").unwrap(),
            12_345_678
        );
        assert!(parse_budget_usd_to_micros("").is_err());
        assert!(parse_budget_usd_to_micros("-1.00").is_err());
        assert!(parse_budget_usd_to_micros("1.0000001").is_err());
        assert!(parse_budget_usd_to_micros("abc").is_err());
        assert!(parse_budget_usd_to_micros("99999999999999").is_err());
    }
}
