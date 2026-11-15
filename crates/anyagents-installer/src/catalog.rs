//! The harness registry: what each harness and runtime is, where it comes
//! from and how it is launched, checked and signed in. The catalog ships in
//! the binary; caller configuration (`<data>/harnesses.toml`) overrides any
//! entry by id or adds new ones.

use std::collections::BTreeMap;
use std::path::Path;

use anyagents_harness::normalize_harness_name;
use serde::{Deserialize, Serialize};

use crate::InstallError;
use crate::source::{PinnedArtifact, Source};
use crate::unpack::Format;

/// The caller-owned override file, relative to the data directory.
pub const OVERRIDES: &str = "harnesses.toml";

/// Which models a subscription on this harness may run, and where that was read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionTerms {
    /// The models the vendor's own terms allow under a subscription.
    pub models: Vec<String>,
    /// Where the terms were read. Empty means unread: offer an API key only.
    pub source: String,
}

/// How a freshly installed version proves it runs.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DoctorSpec {
    /// Arguments whose output must contain the exact installed version.
    pub version_args: Vec<String>,
    /// An offline handshake that must exit zero. Empty means none is declared.
    #[serde(default)]
    pub handshake: Vec<String>,
}

/// One vendor tool or runtime, as shipped with the binary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub id: String,
    /// The vendor command that signs the user in: the program name, then its
    /// arguments. A managed sign-in runs the arguments with the managed launcher.
    pub sign_in_command: Vec<String>,
    /// The variable an API key is passed through, when the vendor documents one.
    pub api_key_variable: Option<String>,
    pub subscription: SubscriptionTerms,
    /// Where the binary comes from.
    pub source: Source,
    /// The launcher, relative to the version directory, in
    /// [`crate::Platform::expand`] words.
    pub launcher: String,
    /// The runtime entry (`node`, `bun`) that runs the launcher, if any.
    pub runtime: Option<String>,
    pub doctor: DoctorSpec,
    /// The variable that points the harness at its private config directory.
    pub config_var: Option<String>,
    /// Environment that turns off the vendor's own self-update.
    pub self_update_off: BTreeMap<String, String>,
}

impl CatalogEntry {
    /// The vendor program name a person would type, for messages and the legacy sign-in.
    pub fn command(&self) -> &str {
        self.sign_in_command
            .first()
            .map(String::as_str)
            .unwrap_or(&self.id)
    }
}

/// The variable that points this harness at its private config directory.
pub fn config_dir_variable(harness: &str) -> Option<&'static str> {
    match normalize_harness_name(harness) {
        "claude-code" => Some("CLAUDE_CONFIG_DIR"),
        "codex" => Some("CODEX_HOME"),
        "agy" => Some("AGY_CONFIG_DIR"),
        "muse" => Some("MUSE_CONFIG_HOME"),
        "omp" => Some("PI_CODING_AGENT_DIR"),
        "dsh" | "grok" | "spiron" => Some("HOME"),
        _ => None,
    }
}

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| (*s).to_string()).collect()
}

fn env(items: &[(&str, &str)]) -> BTreeMap<String, String> {
    items
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

struct Spec<'a> {
    id: &'a str,
    sign_in: &'a [&'a str],
    api_key: Option<&'a str>,
    models: &'a [&'a str],
    terms: &'a str,
    source: Source,
    launcher: &'a str,
    runtime: Option<&'a str>,
    self_update_off: &'a [(&'a str, &'a str)],
}

fn harness(spec: Spec<'_>) -> CatalogEntry {
    CatalogEntry {
        id: spec.id.to_string(),
        sign_in_command: strings(spec.sign_in),
        api_key_variable: spec.api_key.map(str::to_string),
        subscription: SubscriptionTerms {
            models: strings(spec.models),
            source: spec.terms.to_string(),
        },
        source: spec.source,
        launcher: spec.launcher.to_string(),
        runtime: spec.runtime.map(str::to_string),
        doctor: DoctorSpec {
            version_args: strings(&["--version"]),
            handshake: Vec::new(),
        },
        config_var: config_dir_variable(spec.id).map(str::to_string),
        self_update_off: env(spec.self_update_off),
    }
}

fn npm(package: &str, platform_package: Option<&str>) -> Source {
    Source::Npm {
        package: package.to_string(),
        platform_package: platform_package.map(str::to_string),
        registry: None,
    }
}

fn npm_tree(package: &str) -> Source {
    Source::NpmTree {
        package: package.to_string(),
        registry: None,
    }
}

/// The harness catalog shipped with the binary.
pub fn builtin_catalog() -> Vec<CatalogEntry> {
    let cloud = |id: &str, sign_in: &[&str], key: &str, models: &[&str], terms: &str| {
        harness(Spec {
            id,
            sign_in,
            api_key: Some(key),
            models,
            terms,
            source: Source::Builtin,
            launcher: "",
            runtime: None,
            self_update_off: &[],
        })
    };
    vec![
        harness(Spec {
            id: "spiron",
            sign_in: &["spiron", "login"],
            api_key: Some("SYLPHX_API_KEY"),
            models: &[],
            terms: "",
            source: Source::Unconfigured,
            launcher: "spiron",
            runtime: None,
            self_update_off: &[],
        }),
        harness(Spec {
            id: "claude-code",
            sign_in: &["claude", "auth", "login"],
            api_key: Some("ANTHROPIC_API_KEY"),
            models: &["claude-opus", "claude-sonnet"],
            terms: "",
            source: npm(
                "@anthropic-ai/claude-code",
                Some("@anthropic-ai/claude-code-{os}-{cpu}"),
            ),
            launcher: "node_modules/@anthropic-ai/claude-code-{os}-{cpu}/claude",
            runtime: None,
            self_update_off: &[("DISABLE_AUTOUPDATER", "1")],
        }),
        harness(Spec {
            id: "codex",
            sign_in: &["codex", "login"],
            api_key: Some("OPENAI_API_KEY"),
            models: &["gpt"],
            terms: "",
            source: npm("@openai/codex", Some("@openai/codex-{os}-{cpu}")),
            launcher: "node_modules/@openai/codex-{os}-{cpu}/vendor/{target}/bin/codex",
            runtime: None,
            self_update_off: &[],
        }),
        harness(Spec {
            id: "opencode",
            sign_in: &["opencode", "auth", "login"],
            api_key: Some("OPENAI_API_KEY"),
            models: &[],
            terms: "",
            source: npm("opencode-ai", Some("opencode-{os}-{cpu}")),
            launcher: "node_modules/opencode-{os}-{cpu}/bin/opencode",
            runtime: None,
            self_update_off: &[("OPENCODE_DISABLE_AUTOUPDATE", "true")],
        }),
        // The Antigravity CLI signs in the first time it runs and keeps the
        // token in its own store under `AGY_CONFIG_DIR`, so running it with
        // that directory is the sign-in.
        harness(Spec {
            id: "agy",
            sign_in: &["agy", "--print", "reply with the word signed-in"],
            api_key: Some("ANTIGRAVITY_API_KEY"),
            models: &["gemini"],
            terms: "",
            source: Source::Unconfigured,
            launcher: "agy",
            runtime: None,
            self_update_off: &[],
        }),
        harness(Spec {
            id: "muse",
            sign_in: &["muse", "login"],
            api_key: Some("META_API_KEY"),
            models: &["muse-wonder-full-1.0"],
            terms: "",
            source: Source::Unconfigured,
            launcher: "muse",
            runtime: None,
            self_update_off: &[],
        }),
        harness(Spec {
            id: "omp",
            sign_in: &["omp", "login"],
            api_key: Some("OPENAI_API_KEY"),
            models: &[],
            terms: "",
            source: npm_tree("@oh-my-pi/pi-coding-agent"),
            launcher: "node_modules/@oh-my-pi/pi-coding-agent/dist/cli.js",
            runtime: Some("bun"),
            self_update_off: &[],
        }),
        harness(Spec {
            id: "dsh",
            sign_in: &["dsh", "--profile", "web"],
            api_key: Some("DEEPSEEK_API_KEY"),
            models: &["deepseek-v3.2-exp-think"],
            terms: "",
            source: npm_tree("@deepseek-ai/dsh"),
            launcher: "node_modules/@deepseek-ai/dsh/lib/bin.js",
            runtime: Some("node"),
            self_update_off: &[],
        }),
        harness(Spec {
            id: "grok",
            sign_in: &["grok", "login"],
            api_key: Some("XAI_API_KEY"),
            models: &["grok-4.20"],
            terms: "",
            source: Source::Unconfigured,
            launcher: "grok",
            runtime: None,
            self_update_off: &[],
        }),
        cloud(
            "spiron-cloud",
            &["spiron", "login"],
            "SYLPHX_API_KEY",
            &["claude-opus-4.6", "claude-sonnet-4.6", "gpt-5.4"],
            "https://sylphx.com/terms",
        ),
        cloud(
            "claude-cloud",
            &["claude", "auth", "login"],
            "ANTHROPIC_API_KEY",
            &["claude-opus-4-6", "claude-sonnet-4-6", "claude-haiku-4-5"],
            "https://www.anthropic.com/legal/consumer-terms",
        ),
        cloud(
            "openai-cloud",
            &["codex", "login"],
            "OPENAI_API_KEY",
            &["gpt-5.4", "gpt-5.4-mini", "gpt-5.3-codex"],
            "https://openai.com/policies/row-terms-of-use/",
        ),
        cloud(
            "cursor-cloud",
            &["cursor", "login"],
            "CURSOR_API_KEY",
            &["claude-4.6-opus", "gpt-5.4"],
            "https://www.cursor.com/terms-of-service",
        ),
    ]
}

fn runtime(id: &str, source: Source, launcher: &str) -> CatalogEntry {
    CatalogEntry {
        id: id.to_string(),
        sign_in_command: Vec::new(),
        api_key_variable: None,
        subscription: SubscriptionTerms {
            models: Vec::new(),
            source: String::new(),
        },
        source,
        launcher: launcher.to_string(),
        runtime: None,
        doctor: DoctorSpec {
            version_args: strings(&["--version"]),
            handshake: Vec::new(),
        },
        config_var: None,
        self_update_off: BTreeMap::new(),
    }
}

/// The language runtimes harness launchers run on.
///
/// Node publishes `SHASUMS256.txt` per release but no machine-readable
/// "latest LTS with checksums" document, so it is pinned here and bumped with
/// the binary (or overridden by caller configuration). The digests were read
/// from `https://nodejs.org/dist/v26.13.1/SHASUMS256.txt` on 2026-11-12.
pub fn builtin_runtimes() -> Vec<CatalogEntry> {
    let node_url = "https://nodejs.org/dist/v{version}/node-v{version}-{os}-{cpu}.tar.gz";
    let node = |sha: &str| PinnedArtifact {
        url: node_url.to_string(),
        sha256: sha.to_string(),
    };
    vec![
        runtime(
            "node",
            Source::Pinned {
                version: "26.13.1".to_string(),
                artifacts: BTreeMap::from([
                    (
                        "linux-x64".to_string(),
                        node("518674de2b621728a5e4486f5fb3b7d2900bb3cc4aa97969edaa463db99f9778"),
                    ),
                    (
                        "linux-arm64".to_string(),
                        node("80a330b35415382878df18d41afbd106ea89bfb3be0087d6608e289a14c5cc7d"),
                    ),
                    (
                        "darwin-x64".to_string(),
                        node("82bb96078a24ca50b9c9a38f2379e6be6d87171fcd400fc14c8934b3bce96e2f"),
                    ),
                    (
                        "darwin-arm64".to_string(),
                        node("aec2514ea1ceb2ba52455e9ec65986f6749a06dd759e5db2a72e6889668c9735"),
                    ),
                ]),
                format: Format::TarGz,
                strip: 1,
            },
            "bin/node",
        ),
        runtime(
            "bun",
            Source::GithubRelease {
                repo: "oven-sh/bun".to_string(),
                tag_prefix: "bun-v".to_string(),
                assets: BTreeMap::from([
                    ("linux-x64".to_string(), "bun-linux-x64.zip".to_string()),
                    (
                        "linux-arm64".to_string(),
                        "bun-linux-aarch64.zip".to_string(),
                    ),
                    ("darwin-x64".to_string(), "bun-darwin-x64.zip".to_string()),
                    (
                        "darwin-arm64".to_string(),
                        "bun-darwin-aarch64.zip".to_string(),
                    ),
                ]),
                checksums: "SHASUMS256.txt".to_string(),
                format: Format::Zip,
                strip: 1,
                api: None,
                download: None,
            },
            "bun",
        ),
    ]
}

/// A caller override for one entry. Every field given replaces the shipped one.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntryOverride {
    pub source: Option<Source>,
    pub launcher: Option<String>,
    pub runtime: Option<String>,
    pub doctor: Option<DoctorSpec>,
    pub config_var: Option<String>,
    pub self_update_off: Option<BTreeMap<String, String>>,
    pub sign_in_command: Option<Vec<String>>,
    pub api_key_variable: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct OverrideFile {
    #[serde(default)]
    harness: BTreeMap<String, EntryOverride>,
    #[serde(default)]
    runtime: BTreeMap<String, EntryOverride>,
}

/// The effective registry: shipped entries with caller overrides applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Catalog {
    harnesses: Vec<CatalogEntry>,
    runtimes: Vec<CatalogEntry>,
}

impl Default for Catalog {
    fn default() -> Self {
        Self::builtin()
    }
}

impl Catalog {
    pub fn builtin() -> Self {
        Self {
            harnesses: builtin_catalog(),
            runtimes: builtin_runtimes(),
        }
    }

    pub fn new(harnesses: Vec<CatalogEntry>, runtimes: Vec<CatalogEntry>) -> Self {
        Self {
            harnesses,
            runtimes,
        }
    }

    /// The shipped catalog with `<data>/harnesses.toml` applied. A missing file
    /// means no overrides; an unreadable one is an error, never "no overrides".
    pub fn load(data: &Path) -> Result<Self, InstallError> {
        let path = data.join(OVERRIDES);
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Self::builtin()),
            Err(err) => return Err(InstallError::Config(format!("{}: {err}", path.display()))),
        };
        let mut catalog = Self::builtin();
        catalog
            .apply(&text)
            .map_err(|err| InstallError::Config(format!("{}: {err}", path.display())))?;
        Ok(catalog)
    }

    /// Apply override TOML text.
    pub fn apply(&mut self, text: &str) -> Result<(), String> {
        let file: OverrideFile = toml::from_str(text).map_err(|err| err.to_string())?;
        for (id, change) in file.harness {
            merge(&mut self.harnesses, normalize_harness_name(&id), change)?;
        }
        for (id, change) in file.runtime {
            merge(&mut self.runtimes, &id, change)?;
        }
        Ok(())
    }

    pub fn harnesses(&self) -> &[CatalogEntry] {
        &self.harnesses
    }

    pub fn runtimes(&self) -> &[CatalogEntry] {
        &self.runtimes
    }

    /// A harness by id (legacy names resolve), or a runtime by id.
    pub fn get(&self, id: &str) -> Option<&CatalogEntry> {
        let harness = normalize_harness_name(id);
        self.harnesses
            .iter()
            .find(|e| e.id == harness)
            .or_else(|| self.runtimes.iter().find(|e| e.id == id))
    }
}

fn merge(entries: &mut Vec<CatalogEntry>, id: &str, change: EntryOverride) -> Result<(), String> {
    let index = match entries.iter().position(|e| e.id == id) {
        Some(index) => index,
        None => {
            if change.source.is_none() || change.launcher.is_none() {
                return Err(format!("new entry {id} needs both source and launcher"));
            }
            let mut fresh = runtime(id, Source::Unconfigured, "");
            fresh.sign_in_command = vec![id.to_string()];
            entries.push(fresh);
            entries.len() - 1
        }
    };
    let entry = &mut entries[index];
    if let Some(v) = change.source {
        entry.source = v;
    }
    if let Some(v) = change.launcher {
        entry.launcher = v;
    }
    if let Some(v) = change.runtime {
        entry.runtime = (!v.is_empty()).then_some(v);
    }
    if let Some(v) = change.doctor {
        entry.doctor = v;
    }
    if let Some(v) = change.config_var {
        entry.config_var = (!v.is_empty()).then_some(v);
    }
    if let Some(v) = change.self_update_off {
        entry.self_update_off = v;
    }
    if let Some(v) = change.sign_in_command {
        entry.sign_in_command = v;
    }
    if let Some(v) = change.api_key_variable {
        entry.api_key_variable = (!v.is_empty()).then_some(v);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_managed_harness_has_a_launcher_and_every_runtime_is_in_the_catalog() {
        let catalog = Catalog::builtin();
        for entry in catalog.harnesses() {
            if entry.source.is_managed() {
                assert!(!entry.launcher.is_empty(), "{}", entry.id);
            }
            if let Some(runtime) = &entry.runtime {
                assert!(catalog.runtimes().iter().any(|r| &r.id == runtime));
            }
        }
    }

    #[test]
    fn caller_configuration_replaces_a_source_and_adds_an_entry() {
        let mut catalog = Catalog::builtin();
        catalog
            .apply(
                r#"
[harness.grok]
source = { kind = "local", version = "1.0.0", path = "/opt/x/grok", sha256 = "aa" }

[harness.newtool]
source = { kind = "pinned", version = "2", artifacts = { linux-x64 = { url = "https://x/{version}", sha256 = "bb" } } }
launcher = "bin/newtool"
"#,
            )
            .unwrap();
        assert!(matches!(
            catalog.get("grok").unwrap().source,
            Source::Local { .. }
        ));
        assert_eq!(catalog.get("grok").unwrap().launcher, "grok");
        assert_eq!(catalog.get("newtool").unwrap().launcher, "bin/newtool");
        assert!(
            catalog
                .apply("[harness.other]\nlauncher = \"x\"\n")
                .is_err()
        );
        assert!(catalog.apply("[harness.grok]\nbogus = 1\n").is_err());
    }

    #[test]
    fn an_unreadable_override_file_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(Catalog::load(dir.path()).unwrap(), Catalog::builtin());
        std::fs::write(dir.path().join(OVERRIDES), "[harness.grok\n").unwrap();
        assert!(matches!(
            Catalog::load(dir.path()),
            Err(InstallError::Config(_))
        ));
    }
}
