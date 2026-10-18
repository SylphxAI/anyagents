//! A harness is installed the first time an agent needs it, into a private
//! prefix, and signed in through the vendor's own command.
//!
//! Nothing here downloads or runs a real vendor tool. The [`CommandRunner`]
//! is the only way a command leaves the process, and tests supply one that
//! records what it was asked and answers without touching the network.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// How a harness is installed on one platform.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InstallMethod {
    /// `npm install` into a private prefix, never the user's global one.
    Npm { package: String },
    /// The vendor's own install script, given a private prefix.
    VendorScript { url: String },
    /// A release binary checked against a published checksum.
    ReleaseBinary { url: String, sha256: String },
}

/// Which models a subscription on this harness may run, and where that was read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionTerms {
    /// The models the vendor's own terms allow under a subscription.
    pub models: Vec<String>,
    /// Where the terms were read. Empty means unread: offer an API key only.
    pub source: String,
}

/// One vendor tool, as shipped with the binary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub id: String,
    pub install: InstallMethod,
    /// The vendor command that signs the user in.
    pub sign_in_command: Vec<String>,
    /// The variable an API key is passed through, when the vendor documents one.
    pub api_key_variable: Option<String>,
    pub subscription: SubscriptionTerms,
}

/// The catalog shipped with the binary. A user override replaces an entry by id.
pub fn builtin_catalog() -> Vec<CatalogEntry> {
    vec![
        entry(
            "claude-code",
            InstallMethod::Npm {
                package: "@anthropic-ai/claude-code".to_string(),
            },
            vec![
                "claude".to_string(),
                "auth".to_string(),
                "login".to_string(),
            ],
            Some("ANTHROPIC_API_KEY".to_string()),
            vec!["claude-opus".to_string(), "claude-sonnet".to_string()],
            "",
        ),
        entry(
            "codex",
            InstallMethod::Npm {
                package: "@openai/codex".to_string(),
            },
            vec!["codex".to_string(), "login".to_string()],
            Some("OPENAI_API_KEY".to_string()),
            vec!["gpt".to_string()],
            "",
        ),
        entry(
            "opencode",
            InstallMethod::Npm {
                package: "opencode-ai".to_string(),
            },
            vec![
                "opencode".to_string(),
                "auth".to_string(),
                "login".to_string(),
            ],
            Some("OPENAI_API_KEY".to_string()),
            vec![],
            "",
        ),
    ]
}

fn entry(
    id: &str,
    install: InstallMethod,
    sign_in_command: Vec<String>,
    api_key_variable: Option<String>,
    models: Vec<String>,
    source: &str,
) -> CatalogEntry {
    CatalogEntry {
        id: id.to_string(),
        install,
        sign_in_command,
        api_key_variable,
        subscription: SubscriptionTerms {
            models,
            source: source.to_string(),
        },
    }
}

/// Whether a (harness, model, auth) triple may be offered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthMode {
    Subscription,
    ApiKey,
}

/// A subscription whose terms have not been read is not offered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Combination {
    /// Valid. Subscription names the vendor models it covers.
    Allowed,
    /// The vendor's terms were not read, so only an API key is offered.
    TermsUnread,
    /// A subscription does not cover this model.
    ModelNotCovered,
}

pub fn combination(entry: &CatalogEntry, model: &str, auth: AuthMode) -> Combination {
    match auth {
        AuthMode::ApiKey => Combination::Allowed,
        AuthMode::Subscription if entry.subscription.source.is_empty() => Combination::TermsUnread,
        AuthMode::Subscription if entry.subscription.models.iter().any(|m| m == model) => {
            Combination::Allowed
        }
        AuthMode::Subscription => Combination::ModelNotCovered,
    }
}

/// Runs one command and returns what it printed. The only seam to a real process.
pub trait CommandRunner {
    fn run(&mut self, program: &str, args: &[String], dir: &Path) -> Result<String, InstallError>;
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum InstallError {
    #[error("no catalog entry for harness {0}")]
    UnknownHarness(String),
    #[error("install failed: {0}")]
    Failed(String),
}

/// Where one harness version lives: `<root>/harnesses/<id>/<version>/`.
pub fn install_dir(root: &Path, id: &str, version: &str) -> PathBuf {
    root.join("harnesses").join(id).join(version)
}

/// Install `version` of `harness` if that exact directory is not already there.
///
/// A second call for the same id and version does nothing. The agent that asked
/// waits; this returns once the install has finished or failed.
pub fn ensure_installed(
    catalog: &[CatalogEntry],
    root: &Path,
    harness: &str,
    version: &str,
    runner: &mut dyn CommandRunner,
) -> Result<PathBuf, InstallError> {
    let entry = catalog
        .iter()
        .find(|e| e.id == harness)
        .ok_or_else(|| InstallError::UnknownHarness(harness.to_string()))?;
    let dir = install_dir(root, harness, version);
    if dir.join(".installed").exists() {
        return Ok(dir);
    }
    std::fs::create_dir_all(&dir).map_err(|e| InstallError::Failed(e.to_string()))?;
    let (program, args) = install_command(&entry.install, version);
    runner.run(&program, &args, &dir)?;
    std::fs::write(dir.join(".installed"), version)
        .map_err(|e| InstallError::Failed(e.to_string()))?;
    Ok(dir)
}

fn install_command(method: &InstallMethod, version: &str) -> (String, Vec<String>) {
    match method {
        InstallMethod::Npm { package } => (
            "npm".to_string(),
            vec![
                "install".to_string(),
                "--prefix".to_string(),
                ".".to_string(),
                format!("{package}@{version}"),
            ],
        ),
        InstallMethod::VendorScript { url } => (
            "sh".to_string(),
            vec!["-c".to_string(), format!("curl -fsSL {url} | sh")],
        ),
        InstallMethod::ReleaseBinary { url, sha256 } => (
            "sh".to_string(),
            vec![
                "-c".to_string(),
                format!("curl -fsSL -o bin {url} && printf '%s  bin\\n' {sha256} | sha256sum -c -"),
            ],
        ),
    }
}

/// What a sign-in is waiting on the user for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SignInPrompt {
    /// A URL the user opens.
    Link(String),
    /// A device code the user enters.
    DeviceCode(String),
    /// The vendor asked a question.
    Prompt(String),
}

/// The recorded outcome. The credential itself stays in the vendor's store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignInOutcome {
    pub signed_in: bool,
    pub account: Option<String>,
    pub prompts: Vec<SignInPrompt>,
}

/// Run the vendor's sign-in command and record only the outcome.
///
/// Lines the command prints are classified into prompts. A line `SIGNED_IN <account>`
/// is the outcome; the vendor writes its own credential.
pub fn sign_in(
    catalog: &[CatalogEntry],
    root: &Path,
    harness: &str,
    version: &str,
    runner: &mut dyn CommandRunner,
) -> Result<SignInOutcome, InstallError> {
    let entry = catalog
        .iter()
        .find(|e| e.id == harness)
        .ok_or_else(|| InstallError::UnknownHarness(harness.to_string()))?;
    let dir = install_dir(root, harness, version);
    let (program, args) = entry
        .sign_in_command
        .split_first()
        .ok_or_else(|| InstallError::Failed("no sign-in command".to_string()))?;
    let output = runner.run(program, args, &dir)?;
    let mut prompts = Vec::new();
    let mut account = None;
    for line in output.lines() {
        if let Some(rest) = line.strip_prefix("LINK ") {
            prompts.push(SignInPrompt::Link(rest.to_string()));
        } else if let Some(rest) = line.strip_prefix("DEVICE ") {
            prompts.push(SignInPrompt::DeviceCode(rest.to_string()));
        } else if let Some(rest) = line.strip_prefix("PROMPT ") {
            prompts.push(SignInPrompt::Prompt(rest.to_string()));
        } else if let Some(rest) = line.strip_prefix("SIGNED_IN ") {
            account = Some(rest.to_string());
        }
    }
    Ok(SignInOutcome {
        signed_in: account.is_some(),
        account,
        prompts,
    })
}

/// A runner that records every command and answers from a script. Tests only.
#[derive(Default)]
pub struct ScriptedRunner {
    pub calls: Vec<(String, Vec<String>, PathBuf)>,
    answers: BTreeMap<String, String>,
    fail: BTreeMap<String, String>,
}

impl ScriptedRunner {
    pub fn answer(&mut self, program: &str, output: &str) {
        self.answers.insert(program.to_string(), output.to_string());
    }
}

impl CommandRunner for ScriptedRunner {
    fn run(&mut self, program: &str, args: &[String], dir: &Path) -> Result<String, InstallError> {
        self.calls
            .push((program.to_string(), args.to_vec(), dir.to_path_buf()));
        if let Some(reason) = self.fail.get(program) {
            return Err(InstallError::Failed(reason.clone()));
        }
        Ok(self.answers.get(program).cloned().unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog() -> Vec<CatalogEntry> {
        builtin_catalog()
    }

    #[test]
    fn an_unread_subscription_is_not_offered() {
        let entry = catalog()
            .into_iter()
            .find(|e| e.id == "claude-code")
            .unwrap();
        assert_eq!(
            combination(&entry, "claude-opus", AuthMode::Subscription),
            Combination::TermsUnread
        );
        assert_eq!(
            combination(&entry, "anything", AuthMode::ApiKey),
            Combination::Allowed
        );
    }

    #[test]
    fn a_read_subscription_covers_only_its_own_models() {
        let mut entry = catalog().into_iter().find(|e| e.id == "codex").unwrap();
        entry.subscription.source = "vendor terms, read".to_string();
        assert_eq!(
            combination(&entry, "gpt", AuthMode::Subscription),
            Combination::Allowed
        );
        assert_eq!(
            combination(&entry, "claude-opus", AuthMode::Subscription),
            Combination::ModelNotCovered
        );
    }

    #[test]
    fn the_second_install_of_the_same_version_does_nothing() {
        let root = tempfile::tempdir().unwrap();
        let mut runner = ScriptedRunner::default();
        ensure_installed(&catalog(), root.path(), "codex", "1.0.0", &mut runner).unwrap();
        ensure_installed(&catalog(), root.path(), "codex", "1.0.0", &mut runner).unwrap();
        assert_eq!(runner.calls.len(), 1);
        assert!(
            install_dir(root.path(), "codex", "1.0.0")
                .join(".installed")
                .exists()
        );
    }

    #[test]
    fn an_install_lands_in_a_private_prefix() {
        let root = tempfile::tempdir().unwrap();
        let mut runner = ScriptedRunner::default();
        ensure_installed(&catalog(), root.path(), "claude-code", "2.0.0", &mut runner).unwrap();
        let (program, args, dir) = &runner.calls[0];
        assert_eq!(program, "npm");
        assert!(
            args.iter()
                .any(|a| a.contains("@anthropic-ai/claude-code@2.0.0"))
        );
        assert!(dir.ends_with("harnesses/claude-code/2.0.0"));
    }

    #[test]
    fn an_unknown_harness_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let mut runner = ScriptedRunner::default();
        let err = ensure_installed(&catalog(), root.path(), "nope", "1", &mut runner).unwrap_err();
        assert_eq!(err, InstallError::UnknownHarness("nope".to_string()));
        assert!(runner.calls.is_empty());
    }

    #[test]
    fn sign_in_records_the_outcome_and_the_prompts() {
        let root = tempfile::tempdir().unwrap();
        let mut runner = ScriptedRunner::default();
        runner.answer(
            "codex",
            "LINK https://auth.example/device\nDEVICE ABCD-1234\nSIGNED_IN ada@example\n",
        );
        let outcome = sign_in(&catalog(), root.path(), "codex", "1.0.0", &mut runner).unwrap();
        assert!(outcome.signed_in);
        assert_eq!(outcome.account.as_deref(), Some("ada@example"));
        assert_eq!(
            outcome.prompts,
            vec![
                SignInPrompt::Link("https://auth.example/device".to_string()),
                SignInPrompt::DeviceCode("ABCD-1234".to_string()),
            ]
        );
    }
}
