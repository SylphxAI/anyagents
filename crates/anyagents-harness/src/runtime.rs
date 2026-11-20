//! Orthogonal `harness × runtime` execution model (ADR 0010 D1 / D3 / D6 / D7).
//!
//! Historically, vendor cloud agents were encoded as synthetic harness names
//! (`claude-cloud`, `cursor-cloud`, `openai-cloud`, `spiron-cloud`). This module
//! separates the two orthogonal axes cleanly:
//!
//! - **Harness:** which external single-agent loop executes turns (`agy`,
//!   `claude-code`, `codex`, `cursor`, `muse`, `omp`, `opencode`, `spiron`).
//! - **Runtime:** where and how that harness runs:
//!   - `local` (aliases: `host`, `os`): local host / OS sandbox process.
//!   - `sandbox` / `sandbox:<provider>` (aliases: `container`, `docker`,
//!     `remote-sandbox`, `<provider>`): isolated remote or container sandbox.
//!   - `cloud` (aliases: `remote`, `managed`, `vendor-cloud`): vendor-hosted
//!     cloud agent API driven directly over an ADR 0010 D7 remote session with
//!     zero local OS child process.
//!
//! Legacy suffixed harness names (`*-cloud`, `spiron-local`) are decomposed
//! losslessly while contradictory combinations fail closed.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::normalize_harness_name;

/// `launch_env` key storing the agent's canonical runtime selector (`local`,
/// `sandbox`, `sandbox:<provider>`, or `cloud`).
pub const RUNTIME_ENV_KEY: &str = "ANYAGENTS_RUNTIME";

/// `launch_env` key storing an optional per-agent hard spend cap in micro-USD
/// (`1_000_000` = US$1.00) enforced by remote session adapters (ADR 0010 D7).
pub const REMOTE_BUDGET_MICROS_ENV_KEY: &str = "ANYAGENTS_REMOTE_BUDGET_MICROS";

/// Known sandbox provider identifiers accepted as `sandbox:<provider>`.
pub const KNOWN_SANDBOX_PROVIDERS: &[&str] = &[
    "cloudflare",
    "daytona",
    "docker",
    "e2b",
    "http_oci",
    "modal",
    "sylphx",
];

/// Canonical execution runtime axis orthogonal to the agent harness.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum ExecutionRuntime {
    /// Local host or OS-sandboxed process (`local`, `host`, `os`).
    #[default]
    Local,
    /// Isolated remote or container sandbox (`sandbox`, `sandbox:<provider>`).
    Sandbox { provider: Option<String> },
    /// Vendor-hosted cloud agent API via an ADR 0010 D7 remote session (`cloud`).
    Cloud,
}

impl ExecutionRuntime {
    /// Parse a runtime string (`local`, `sandbox`, `sandbox:<provider>`, `cloud`,
    /// or documented aliases). Fails closed on empty or unknown values.
    pub fn parse(raw: &str) -> Result<Self, RuntimeResolutionError> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(RuntimeResolutionError::InvalidRuntime(String::new()));
        }
        let lower = trimmed.to_ascii_lowercase();
        match lower.as_str() {
            "local" | "host" | "os" => Ok(Self::Local),
            "cloud" | "remote" | "managed" | "vendor-cloud" | "vendor_cloud" => Ok(Self::Cloud),
            "sandbox" | "container" | "remote-sandbox" | "remote_sandbox" => {
                Ok(Self::Sandbox { provider: None })
            }
            "docker" => Ok(Self::Sandbox {
                provider: Some("docker".to_string()),
            }),
            other => {
                if let Some(provider_raw) = other.strip_prefix("sandbox:") {
                    let provider = normalize_sandbox_provider(provider_raw)?;
                    return Ok(Self::Sandbox {
                        provider: Some(provider),
                    });
                }
                if KNOWN_SANDBOX_PROVIDERS.contains(&other) {
                    return Ok(Self::Sandbox {
                        provider: Some(other.to_string()),
                    });
                }
                Err(RuntimeResolutionError::InvalidRuntime(trimmed.to_string()))
            }
        }
    }

    /// Canonical wire/config representation (`local`, `sandbox`, `sandbox:<provider>`, `cloud`).
    pub fn as_canonical_string(&self) -> String {
        match self {
            Self::Local => "local".to_string(),
            Self::Sandbox { provider: None } => "sandbox".to_string(),
            Self::Sandbox {
                provider: Some(provider),
            } => format!("sandbox:{provider}"),
            Self::Cloud => "cloud".to_string(),
        }
    }

    /// Short runtime family name (`local`, `sandbox`, or `cloud`).
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Sandbox { .. } => "sandbox",
            Self::Cloud => "cloud",
        }
    }

    /// Returns `true` when execution happens off the coordinator host (`sandbox` or `cloud`).
    pub const fn is_off_host(&self) -> bool {
        matches!(self, Self::Sandbox { .. } | Self::Cloud)
    }

    /// Returns `true` when this runtime is a vendor-hosted cloud agent (`cloud`).
    pub const fn is_cloud(&self) -> bool {
        matches!(self, Self::Cloud)
    }

    /// Returns `true` when this runtime is an isolated sandbox (`sandbox`).
    pub const fn is_sandbox(&self) -> bool {
        matches!(self, Self::Sandbox { .. })
    }

    /// Optional explicit sandbox provider hint (`sylphx`, `docker`, `e2b`, ...).
    pub fn sandbox_provider(&self) -> Option<&str> {
        match self {
            Self::Sandbox { provider } => provider.as_deref(),
            _ => None,
        }
    }
}

impl fmt::Display for ExecutionRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.as_canonical_string())
    }
}

impl Serialize for ExecutionRuntime {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.as_canonical_string())
    }
}

impl<'de> Deserialize<'de> for ExecutionRuntime {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

fn normalize_sandbox_provider(raw: &str) -> Result<String, RuntimeResolutionError> {
    let trimmed = raw.trim().to_ascii_lowercase().replace('-', "_");
    if trimmed.is_empty()
        || !trimmed
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        return Err(RuntimeResolutionError::InvalidRuntime(format!(
            "sandbox:{raw}"
        )));
    }
    Ok(trimmed)
}

/// Errors produced when validating or resolving a `(harness, runtime)` pair.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RuntimeResolutionError {
    #[error("invalid runtime `{0}`: expected `local`, `sandbox`, `sandbox:<provider>`, or `cloud`")]
    InvalidRuntime(String),
    #[error(
        "conflicting harness `{harness}` (implies runtime `{implied}`) and explicit runtime `{explicit}`; pass canonical harness `{canonical_harness}` with `--runtime {explicit}`"
    )]
    ConflictingRuntime {
        harness: String,
        canonical_harness: String,
        implied: ExecutionRuntime,
        explicit: ExecutionRuntime,
    },
    #[error(
        "harness `{harness}` does not support runtime `{runtime}` (supported runtimes: {supported})"
    )]
    UnsupportedCombination {
        harness: String,
        runtime: ExecutionRuntime,
        supported: String,
    },
}

/// Canonical `(harness, runtime)` resolution result plus backward-compatible
/// storage/routing selector.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedExecution {
    /// Canonical harness name without `-cloud` / `-local` runtime suffix.
    pub harness: String,
    /// Resolved execution runtime (`local`, `sandbox[:provider]`, `cloud`).
    pub runtime: ExecutionRuntime,
}

impl ResolvedExecution {
    /// Backward-compatible legacy selector for subsystems that still key on
    /// `*-cloud` harness names (`(spiron, Cloud) -> "spiron-cloud"`, etc.).
    pub fn legacy_harness_selector(&self) -> String {
        if self.runtime.is_cloud() {
            match self.harness.as_str() {
                "claude-code" => "claude-cloud".to_string(),
                "codex" => "openai-cloud".to_string(),
                "cursor" => "cursor-cloud".to_string(),
                "spiron" => "spiron-cloud".to_string(),
                other => other.to_string(),
            }
        } else {
            self.harness.clone()
        }
    }
}

/// Decompose a harness selector into `(canonical_harness, implied_runtime)`.
///
/// Supports both:
/// - Clean explicit `harness@runtime` syntax (`spiron@cloud`, `claude@cloud`,
///   `spiron@sandbox:sylphx`, `codex@local`, `agy@sandbox`), and
/// - Deprecated legacy suffixes (`spiron-cloud`, `claude-cloud`, `openai-cloud`,
///   `cursor-cloud`, `spiron-local`).
pub fn decompose_harness_selector(
    raw: &str,
) -> Result<(String, Option<ExecutionRuntime>), RuntimeResolutionError> {
    let trimmed = raw.trim();
    if let Some((raw_harness, raw_runtime)) = trimmed.split_once('@') {
        let rt = ExecutionRuntime::parse(raw_runtime)?;
        let (canonical, legacy_rt) = decompose_legacy_selector(raw_harness);
        if let Some(implied) = legacy_rt
            && implied.kind() != rt.kind()
        {
            return Err(RuntimeResolutionError::ConflictingRuntime {
                harness: trimmed.to_string(),
                canonical_harness: canonical,
                implied,
                explicit: rt,
            });
        }
        return Ok((canonical, Some(rt)));
    }
    Ok(decompose_legacy_selector(trimmed))
}

/// Lossless decomposition of plain or legacy-suffixed selectors (no `@runtime`).
pub fn decompose_legacy_selector(raw: &str) -> (String, Option<ExecutionRuntime>) {
    let normalized = normalize_harness_name(raw);
    match normalized {
        "claude" => ("claude-code".to_string(), None),
        "claude-cloud" => ("claude-code".to_string(), Some(ExecutionRuntime::Cloud)),
        "cursor-cloud" => ("cursor".to_string(), Some(ExecutionRuntime::Cloud)),
        "openai-cloud" => ("codex".to_string(), Some(ExecutionRuntime::Cloud)),
        "spiron-cloud" => ("spiron".to_string(), Some(ExecutionRuntime::Cloud)),
        "spiron-local" | "spiron_local" => ("spiron".to_string(), Some(ExecutionRuntime::Local)),
        other => (other.to_string(), None),
    }
}

/// Supported runtime families (`"local"`, `"sandbox"`, `"cloud"`) for a canonical harness.
///
/// Unknown/custom harnesses permit `local` and `sandbox` (and reject `cloud`
/// unless an explicit cloud adapter exists).
pub fn supported_runtime_kinds(canonical_harness: &str) -> &'static [&'static str] {
    match canonical_harness {
        "claude-code" | "codex" | "spiron" => &["local", "sandbox", "cloud"],
        "cursor" => &["cloud"],
        "agy" | "dsh" | "grok" | "muse" | "omp" | "opencode" => &["local", "sandbox"],
        "http-cloud" | "http_cloud" => &["cloud"],
        _ => &["local", "sandbox"],
    }
}

/// Resolve and validate an orthogonal `(harness, runtime)` pair fail-closed.
pub fn resolve_harness_and_runtime(
    raw_harness: &str,
    explicit_runtime: Option<&str>,
    default_runtime: ExecutionRuntime,
) -> Result<ResolvedExecution, RuntimeResolutionError> {
    let (canonical_harness, implied_runtime) = decompose_harness_selector(raw_harness)?;
    let parsed_explicit = explicit_runtime
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ExecutionRuntime::parse)
        .transpose()?;

    let runtime = match (implied_runtime, parsed_explicit) {
        (Some(implied), Some(explicit)) => {
            if implied.kind() != explicit.kind() {
                return Err(RuntimeResolutionError::ConflictingRuntime {
                    harness: raw_harness.trim().to_string(),
                    canonical_harness,
                    implied,
                    explicit,
                });
            }
            explicit
        }
        (Some(implied), None) => implied,
        (None, Some(explicit)) => explicit,
        (None, None) => {
            let supported = supported_runtime_kinds(&canonical_harness);
            if supported.contains(&default_runtime.kind()) {
                default_runtime
            } else if supported == ["cloud"] {
                ExecutionRuntime::Cloud
            } else {
                default_runtime
            }
        }
    };

    let supported = supported_runtime_kinds(&canonical_harness);
    if !supported.contains(&runtime.kind()) {
        return Err(RuntimeResolutionError::UnsupportedCombination {
            harness: canonical_harness,
            runtime,
            supported: supported.join(", "),
        });
    }

    Ok(ResolvedExecution {
        harness: canonical_harness,
        runtime,
    })
}

/// Extract an agent's resolved runtime from stored `harness` + `launch_env`,
/// preserving backward compatibility with rows written before `ANYAGENTS_RUNTIME`.
pub fn resolve_stored_runtime(
    stored_harness: &str,
    launch_env: &[(String, String)],
) -> ExecutionRuntime {
    if let Some((_, raw)) = launch_env.iter().rev().find(|(k, _)| k == RUNTIME_ENV_KEY)
        && let Ok(parsed) = ExecutionRuntime::parse(raw)
    {
        return parsed;
    }
    decompose_harness_selector(stored_harness)
        .ok()
        .and_then(|(_, implied)| implied)
        .unwrap_or(ExecutionRuntime::Local)
}

/// Extract an optional per-agent remote budget cap in micro-USD from `launch_env`.
pub fn resolve_stored_budget_micros(launch_env: &[(String, String)]) -> Option<u64> {
    launch_env
        .iter()
        .rev()
        .find(|(k, _)| k == REMOTE_BUDGET_MICROS_ENV_KEY)
        .and_then(|(_, raw)| raw.trim().parse::<u64>().ok())
        .filter(|&cap| cap > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_canonical_and_aliased_runtimes() {
        assert_eq!(
            ExecutionRuntime::parse("local").unwrap(),
            ExecutionRuntime::Local
        );
        assert_eq!(
            ExecutionRuntime::parse("host").unwrap(),
            ExecutionRuntime::Local
        );
        assert_eq!(
            ExecutionRuntime::parse("os").unwrap(),
            ExecutionRuntime::Local
        );
        assert_eq!(
            ExecutionRuntime::parse("cloud").unwrap(),
            ExecutionRuntime::Cloud
        );
        assert_eq!(
            ExecutionRuntime::parse("remote").unwrap(),
            ExecutionRuntime::Cloud
        );
        assert_eq!(
            ExecutionRuntime::parse("sandbox").unwrap(),
            ExecutionRuntime::Sandbox { provider: None }
        );
        assert_eq!(
            ExecutionRuntime::parse("sandbox:sylphx").unwrap(),
            ExecutionRuntime::Sandbox {
                provider: Some("sylphx".to_string())
            }
        );
        assert_eq!(
            ExecutionRuntime::parse("docker").unwrap(),
            ExecutionRuntime::Sandbox {
                provider: Some("docker".to_string())
            }
        );
        assert!(ExecutionRuntime::parse("").is_err());
        assert!(ExecutionRuntime::parse("kubernetes-magic").is_err());
    }

    #[test]
    fn decomposes_legacy_cloud_and_local_harness_selectors() {
        let cases = [
            (
                "spiron-cloud",
                "spiron",
                ExecutionRuntime::Cloud,
                "spiron-cloud",
            ),
            (
                "claude-cloud",
                "claude-code",
                ExecutionRuntime::Cloud,
                "claude-cloud",
            ),
            (
                "openai-cloud",
                "codex",
                ExecutionRuntime::Cloud,
                "openai-cloud",
            ),
            (
                "cursor-cloud",
                "cursor",
                ExecutionRuntime::Cloud,
                "cursor-cloud",
            ),
            ("spiron-local", "spiron", ExecutionRuntime::Local, "spiron"),
        ];
        for (raw, want_harness, want_runtime, want_legacy) in cases {
            let resolved = resolve_harness_and_runtime(raw, None, ExecutionRuntime::Local).unwrap();
            assert_eq!(resolved.harness, want_harness, "{raw}");
            assert_eq!(resolved.runtime, want_runtime, "{raw}");
            assert_eq!(resolved.legacy_harness_selector(), want_legacy, "{raw}");
        }
    }

    #[test]
    fn supports_orthogonal_harness_x_runtime_pairs_and_fails_closed_on_invalid() {
        let spiron_cloud =
            resolve_harness_and_runtime("spiron", Some("cloud"), ExecutionRuntime::Local).unwrap();
        assert_eq!(spiron_cloud.harness, "spiron");
        assert_eq!(spiron_cloud.runtime, ExecutionRuntime::Cloud);
        assert_eq!(spiron_cloud.legacy_harness_selector(), "spiron-cloud");

        let claude_sandbox = resolve_harness_and_runtime(
            "claude-code",
            Some("sandbox:sylphx"),
            ExecutionRuntime::Local,
        )
        .unwrap();
        assert_eq!(claude_sandbox.harness, "claude-code");
        assert_eq!(
            claude_sandbox.runtime.as_canonical_string(),
            "sandbox:sylphx"
        );
        assert_eq!(claude_sandbox.legacy_harness_selector(), "claude-code");

        // Contradiction fails closed.
        let conflict =
            resolve_harness_and_runtime("spiron-cloud", Some("local"), ExecutionRuntime::Local);
        assert!(matches!(
            conflict,
            Err(RuntimeResolutionError::ConflictingRuntime { .. })
        ));

        // Unsupported cloud runtime on local-only harness fails closed.
        let unsupported_cloud =
            resolve_harness_and_runtime("agy", Some("cloud"), ExecutionRuntime::Local);
        assert!(matches!(
            unsupported_cloud,
            Err(RuntimeResolutionError::UnsupportedCombination { .. })
        ));

        // Cloud-only harness rejects local runtime fail-closed.
        let unsupported_local =
            resolve_harness_and_runtime("cursor", Some("local"), ExecutionRuntime::Local);
        assert!(matches!(
            unsupported_local,
            Err(RuntimeResolutionError::UnsupportedCombination { .. })
        ));

        // Cloud-only harness defaults to cloud when no explicit runtime given.
        let cursor_default =
            resolve_harness_and_runtime("cursor", None, ExecutionRuntime::Local).unwrap();
        assert_eq!(cursor_default.runtime, ExecutionRuntime::Cloud);
    }

    #[test]
    fn parses_harness_at_runtime_alias_syntax() {
        let spiron_cloud =
            resolve_harness_and_runtime("spiron@cloud", None, ExecutionRuntime::Local).unwrap();
        assert_eq!(spiron_cloud.harness, "spiron");
        assert_eq!(spiron_cloud.runtime, ExecutionRuntime::Cloud);

        let spiron_sbx =
            resolve_harness_and_runtime("spiron@sandbox:sylphx", None, ExecutionRuntime::Local)
                .unwrap();
        assert_eq!(spiron_sbx.harness, "spiron");
        assert_eq!(
            spiron_sbx.runtime,
            ExecutionRuntime::Sandbox {
                provider: Some("sylphx".to_string())
            }
        );

        let claude_cloud =
            resolve_harness_and_runtime("claude@cloud", None, ExecutionRuntime::Local).unwrap();
        assert_eq!(claude_cloud.harness, "claude-code");
        assert_eq!(claude_cloud.runtime, ExecutionRuntime::Cloud);

        let agy_sbx =
            resolve_harness_and_runtime("agy@sandbox", None, ExecutionRuntime::Local).unwrap();
        assert_eq!(agy_sbx.harness, "agy");
        assert_eq!(
            agy_sbx.runtime,
            ExecutionRuntime::Sandbox { provider: None }
        );

        assert!(matches!(
            resolve_harness_and_runtime("agy@cloud", None, ExecutionRuntime::Local),
            Err(RuntimeResolutionError::UnsupportedCombination { .. })
        ));
        assert!(matches!(
            resolve_harness_and_runtime("spiron@cloud", Some("local"), ExecutionRuntime::Local),
            Err(RuntimeResolutionError::ConflictingRuntime { .. })
        ));
        assert!(matches!(
            resolve_harness_and_runtime("spiron@bogus", None, ExecutionRuntime::Local),
            Err(RuntimeResolutionError::InvalidRuntime(_))
        ));
    }

    #[test]
    fn stored_runtime_and_budget_round_trip() {
        let env = vec![
            (RUNTIME_ENV_KEY.to_string(), "sandbox:e2b".to_string()),
            (
                REMOTE_BUDGET_MICROS_ENV_KEY.to_string(),
                "2500000".to_string(),
            ),
        ];
        assert_eq!(
            resolve_stored_runtime("codex", &env),
            ExecutionRuntime::Sandbox {
                provider: Some("e2b".to_string())
            }
        );
        assert_eq!(resolve_stored_budget_micros(&env), Some(2_500_000));
        assert_eq!(
            resolve_stored_runtime("spiron-cloud", &[]),
            ExecutionRuntime::Cloud
        );
        assert_eq!(
            resolve_stored_runtime("spiron", &[]),
            ExecutionRuntime::Local
        );
    }
}
