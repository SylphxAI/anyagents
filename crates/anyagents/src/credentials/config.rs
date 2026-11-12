//! Caller-owned operator configuration: `<data>/credentials.toml`.
//!
//! ```toml
//! [defaults]
//! "model:anthropic" = "record:oauth_subscription/anthropic/acct-a"
//! "tool:github"     = "record:github_user/github/dev"
//!
//! [pools.example]
//! members = ["record:oauth_subscription/anthropic/acct-a", "record:oauth_subscription/anthropic/acct-b"]
//! policy  = "most-remaining"
//!
//! [identities."record:github_user/github/dev"]
//! git_author = { name = "Example Dev", email = "dev@example.invalid" }
//! ```
//!
//! A missing file means no defaults, no pools and no identities. A file that
//! exists but cannot be read or parsed is an error: resolution fails closed
//! rather than proceeding as if no defaults were configured.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyagents_core::Error;
use serde::{Deserialize, Serialize};

use super::binding::{Slot, Target};
use super::store::RecordRef;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum PoolPolicy {
    /// The member with the most remaining quota, ties broken by member order.
    #[default]
    MostRemaining,
    /// The first member with quota left, in declared order.
    Ordered,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolConfig {
    pub members: Vec<String>,
    #[serde(default)]
    pub policy: PoolPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitAuthor {
    pub name: String,
    pub email: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_author: Option<GitAuthor>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawConfig {
    #[serde(default)]
    pub defaults: BTreeMap<String, String>,
    #[serde(default)]
    pub pools: BTreeMap<String, PoolConfig>,
    #[serde(default)]
    pub identities: BTreeMap<String, IdentityConfig>,
}

/// Validated operator configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OperatorConfig {
    pub defaults: BTreeMap<Slot, Target>,
    pub pools: BTreeMap<String, (Vec<RecordRef>, PoolPolicy)>,
    pub identities: BTreeMap<RecordRef, IdentityConfig>,
}

pub fn path(data: &Path) -> PathBuf {
    data.join("credentials.toml")
}

impl OperatorConfig {
    pub fn load(data: &Path) -> Result<Self, Error> {
        let file = path(data);
        let text = match std::fs::read_to_string(&file) {
            Ok(text) => text,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(err) => {
                return Err(Error::Message(format!("{}: {err}", file.display())));
            }
        };
        Self::parse(&text).map_err(|e| Error::Message(format!("{}: {e}", file.display())))
    }

    pub fn parse(text: &str) -> Result<Self, Error> {
        let raw: RawConfig = toml::from_str(text).map_err(|e| Error::Message(e.to_string()))?;
        Self::from_raw(raw)
    }

    pub fn from_raw(raw: RawConfig) -> Result<Self, Error> {
        let mut out = Self::default();
        for (slot, target) in raw.defaults {
            out.defaults.insert(slot.parse()?, target.parse()?);
        }
        for (name, pool) in raw.pools {
            if !super::store::valid_segment(&name) {
                return Err(Error::Message(format!("invalid pool name `{name}`")));
            }
            if pool.members.is_empty() {
                return Err(Error::Message(format!("pool `{name}` has no members")));
            }
            let members = pool
                .members
                .iter()
                .map(|m| match m.parse::<Target>()? {
                    Target::Record(r) => Ok(r),
                    Target::Pool(_) => Err(Error::Message(format!("pool `{name}` nests a pool"))),
                })
                .collect::<Result<Vec<_>, Error>>()?;
            let provider = &members[0].provider;
            if members.iter().any(|m| &m.provider != provider || m.kind != members[0].kind) {
                return Err(Error::Message(format!(
                    "pool `{name}` mixes record kinds or providers"
                )));
            }
            out.pools.insert(name, (members, pool.policy));
        }
        for (key, identity) in raw.identities {
            let r = match key.parse::<Target>()? {
                Target::Record(r) => r,
                Target::Pool(_) => return Err(Error::Message("identities are keyed by record".into())),
            };
            out.identities.insert(r, identity);
        }
        for target in out.defaults.values() {
            if let Target::Pool(p) = target
                && !out.pools.contains_key(p)
            {
                return Err(Error::Message(format!("default names undefined pool `{p}`")));
            }
        }
        Ok(out)
    }

    pub fn to_raw(&self) -> RawConfig {
        RawConfig {
            defaults: self
                .defaults
                .iter()
                .map(|(s, t)| (s.to_string(), t.to_string()))
                .collect(),
            pools: self
                .pools
                .iter()
                .map(|(n, (m, p))| {
                    (
                        n.clone(),
                        PoolConfig {
                            members: m.iter().map(|r| format!("record:{r}")).collect(),
                            policy: *p,
                        },
                    )
                })
                .collect(),
            identities: self
                .identities
                .iter()
                .map(|(r, i)| (format!("record:{r}"), i.clone()))
                .collect(),
        }
    }

    pub fn render(&self) -> Result<String, Error> {
        toml::to_string_pretty(&self.to_raw()).map_err(|e| Error::Message(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE: &str = r#"
[defaults]
"model:anthropic" = "record:oauth_subscription/anthropic/acct-a"
"model:google-antigravity" = "pool:example"

[pools.example]
members = ["record:oauth_subscription/google-antigravity/a", "record:oauth_subscription/google-antigravity/b"]
policy = "ordered"

[identities."record:github_user/github/dev"]
git_author = { name = "Example Dev", email = "dev@example.invalid" }
"#;

    #[test]
    fn parses_round_trips_and_validates() {
        let cfg = OperatorConfig::parse(EXAMPLE).unwrap();
        assert_eq!(cfg.defaults.len(), 2);
        assert_eq!(cfg.pools["example"].1, PoolPolicy::Ordered);
        let again = OperatorConfig::parse(&cfg.render().unwrap()).unwrap();
        assert_eq!(cfg, again);
    }

    #[test]
    fn rejects_unknown_keys_undefined_pools_and_mixed_pools() {
        assert!(OperatorConfig::parse("[defaults]\n\"model:x\" = \"pool:missing\"\n").is_err());
        assert!(OperatorConfig::parse("unknown = 1\n").is_err());
        assert!(
            OperatorConfig::parse(
                "[pools.p]\nmembers = [\"record:api_key/a/x\", \"record:api_key/b/y\"]\n"
            )
            .is_err()
        );
    }

    #[test]
    fn a_missing_file_is_empty_but_an_unreadable_one_fails() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(OperatorConfig::load(dir.path()).unwrap(), OperatorConfig::default());
        std::fs::write(path(dir.path()), "[defaults\n").unwrap();
        assert!(OperatorConfig::load(dir.path()).is_err());
    }
}
