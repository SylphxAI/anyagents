//! The caller-owned declaration of every tool binary: `<data>/toolchain.toml`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::ToolchainError;

/// The only schema this build reads.
pub const SCHEMA: u32 = 1;

/// The manifest file name inside the data directory.
pub const FILE_NAME: &str = "toolchain.toml";

/// One declared tool.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolSpec {
    /// Absolute path the deployment provides.
    pub path: PathBuf,
    /// Exact version; the output of `version_args` must contain it.
    pub version: String,
    /// Lowercase hex sha256 of the canonical target of `path`.
    pub sha256: String,
    /// Arguments that make the tool print its version.
    #[serde(default = "default_version_args")]
    pub version_args: Vec<String>,
    /// The capability that needs this tool, for messages and reports.
    #[serde(default)]
    pub capability: Option<String>,
    /// Link it into the children's `toolchain/bin`.
    #[serde(default)]
    pub expose: bool,
    /// Refuse (instead of report) when verification fails.
    #[serde(default)]
    pub enforce: bool,
}

fn default_version_args() -> Vec<String> {
    vec!["--version".to_string()]
}

/// The parsed and validated manifest.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub schema: u32,
    /// Directories after `toolchain/bin` in every child's `PATH`.
    #[serde(default)]
    pub child_path_tail: Vec<PathBuf>,
    /// Refuse tools that are not declared instead of using the legacy lookup.
    #[serde(default)]
    pub enforce_undeclared: bool,
    #[serde(default)]
    pub tool: BTreeMap<String, ToolSpec>,
}

impl Manifest {
    /// Parse and validate manifest text. `origin` names the file in errors.
    pub fn parse(text: &str, origin: &Path) -> Result<Self, ToolchainError> {
        let unreadable = |reason: String| ToolchainError::ManifestUnreadable {
            path: origin.to_path_buf(),
            reason,
        };
        let manifest: Manifest = toml::from_str(text).map_err(|e| unreadable(e.to_string()))?;
        if manifest.schema != SCHEMA {
            return Err(unreadable(format!(
                "schema {} is not supported (expected {SCHEMA})",
                manifest.schema
            )));
        }
        for dir in &manifest.child_path_tail {
            if !dir.is_absolute() {
                return Err(unreadable(format!(
                    "child_path_tail entry {} is not absolute",
                    dir.display()
                )));
            }
        }
        for (name, spec) in &manifest.tool {
            if name.is_empty() || name.contains(['/', '\0']) {
                return Err(unreadable(format!("tool name {name:?} is not a file name")));
            }
            if !spec.path.is_absolute() {
                return Err(unreadable(format!(
                    "tool {name}: path {} is not absolute",
                    spec.path.display()
                )));
            }
            if spec.version.trim().is_empty() {
                return Err(unreadable(format!("tool {name}: version is empty")));
            }
            if spec.sha256.len() != 64
                || !spec
                    .sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(unreadable(format!(
                    "tool {name}: sha256 must be 64 lowercase hex characters"
                )));
            }
        }
        Ok(manifest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHA: &str = "0000000000000000000000000000000000000000000000000000000000000000";

    fn parse(text: &str) -> Result<Manifest, ToolchainError> {
        Manifest::parse(text, Path::new("toolchain.toml"))
    }

    #[test]
    fn a_complete_manifest_parses_with_defaults() {
        let m = parse(&format!(
            "schema = 1\nchild_path_tail = [\"/usr/bin\"]\n[tool.git]\npath = \"/usr/bin/git\"\nversion = \"2.47.1\"\nsha256 = \"{SHA}\"\n"
        ))
        .unwrap();
        let git = &m.tool["git"];
        assert_eq!(git.version_args, ["--version"]);
        assert!(!git.expose && !git.enforce);
        assert!(!m.enforce_undeclared);
    }

    #[test]
    fn a_relative_tool_path_is_unreadable() {
        let err = parse(&format!(
            "schema = 1\n[tool.git]\npath = \"git\"\nversion = \"1\"\nsha256 = \"{SHA}\"\n"
        ))
        .unwrap_err();
        assert!(
            matches!(err, ToolchainError::ManifestUnreadable { .. }),
            "{err}"
        );
        assert!(err.to_string().contains("not absolute"), "{err}");
    }

    #[test]
    fn a_short_digest_is_unreadable() {
        let err = parse(
            "schema = 1\n[tool.git]\npath = \"/usr/bin/git\"\nversion = \"1\"\nsha256 = \"abc\"\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("sha256"), "{err}");
    }

    #[test]
    fn an_unknown_schema_or_field_is_unreadable() {
        assert!(parse("schema = 2\n").is_err());
        assert!(parse("schema = 1\nsurprise = true\n").is_err());
    }

    #[test]
    fn a_relative_child_path_entry_is_unreadable() {
        assert!(parse("schema = 1\nchild_path_tail = [\"bin\"]\n").is_err());
    }
}
