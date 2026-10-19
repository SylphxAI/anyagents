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

/// One line a command printed, handed over before the command finishes.
pub trait LineSink {
    fn line(&mut self, line: &str);
}

/// Runs one command. The only seam to a real process.
///
/// `on_line` is called for each line as it is printed, so a sign-in can hand a
/// link or a code to the client while the vendor command is still waiting.
pub trait CommandRunner {
    fn run(
        &mut self,
        program: &str,
        args: &[String],
        dir: &Path,
        env: &[(&str, &str)],
        on_line: &mut dyn LineSink,
    ) -> Result<(), InstallError>;
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
    runner.run(&program, &args, &dir, &[], &mut NopLines)?;
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

/// One line of a sign-in, handed to the client as it arrives.
pub trait SignInSink {
    fn prompt(&mut self, prompt: &SignInPrompt);
    fn done(&mut self, outcome: &SignInOutcome);
}

struct NopLines;

impl LineSink for NopLines {
    fn line(&mut self, _: &str) {}
}

/// Where a harness writes its own sign-in credential.
///
/// `<root>/config/<harness>/<profile>/`. The vendor tool is pointed at this
/// directory by the variable it documents; this crate never reads what it writes.
pub fn config_dir(root: &Path, harness: &str, profile: &str) -> PathBuf {
    root.join("config").join(harness).join(profile)
}

/// The variable that points this harness at its private config directory.
pub fn config_dir_variable(harness: &str) -> Option<&'static str> {
    match harness {
        "claude-code" => Some("CLAUDE_CONFIG_DIR"),
        "codex" => Some("CODEX_HOME"),
        _ => None,
    }
}

struct CollectingSink<'a> {
    prompts: &'a mut Vec<SignInPrompt>,
    account: &'a mut Option<String>,
    client: &'a mut dyn SignInSink,
}

impl LineSink for CollectingSink<'_> {
    fn line(&mut self, line: &str) {
        if let Some(rest) = line.strip_prefix("LINK ") {
            let prompt = SignInPrompt::Link(rest.to_string());
            self.client.prompt(&prompt);
            self.prompts.push(prompt);
        } else if let Some(rest) = line.strip_prefix("DEVICE ") {
            let prompt = SignInPrompt::DeviceCode(rest.to_string());
            self.client.prompt(&prompt);
            self.prompts.push(prompt);
        } else if let Some(rest) = line.strip_prefix("PROMPT ") {
            let prompt = SignInPrompt::Prompt(rest.to_string());
            self.client.prompt(&prompt);
            self.prompts.push(prompt);
        } else if let Some(rest) = line.strip_prefix("SIGNED_IN ") {
            *self.account = Some(rest.to_string());
        }
    }
}

/// Run the vendor's sign-in inside a private config directory.
///
/// Each link, device code or prompt is handed to `sink` as the vendor prints
/// it, before this returns. The vendor writes its own credential into that
/// directory. The returned outcome records only whether sign-in finished and
/// as whom, never the credential.
pub fn sign_in(
    catalog: &[CatalogEntry],
    root: &Path,
    harness: &str,
    profile: &str,
    runner: &mut dyn CommandRunner,
    sink: &mut dyn SignInSink,
) -> Result<SignInOutcome, InstallError> {
    sign_in_at(
        catalog,
        &config_dir(root, harness, profile),
        harness,
        runner,
        sink,
    )
}

/// Run the vendor's sign-in inside `dir`, the exact directory the vendor tool is
/// later started with. An agent needs this: the supervisor launches its harness
/// with the agent's own config directory, so a sign-in under any other path
/// leaves that agent signed out.
pub fn sign_in_at(
    catalog: &[CatalogEntry],
    dir: &Path,
    harness: &str,
    runner: &mut dyn CommandRunner,
    sink: &mut dyn SignInSink,
) -> Result<SignInOutcome, InstallError> {
    let entry = catalog
        .iter()
        .find(|e| e.id == harness)
        .ok_or_else(|| InstallError::UnknownHarness(harness.to_string()))?;
    std::fs::create_dir_all(dir).map_err(|e| InstallError::Failed(e.to_string()))?;
    let (program, args) = entry
        .sign_in_command
        .split_first()
        .ok_or_else(|| InstallError::Failed("no sign-in command".to_string()))?;
    let mut prompts = Vec::new();
    let mut account = None;
    let mut lines = CollectingSink {
        prompts: &mut prompts,
        account: &mut account,
        client: sink,
    };
    let variable = config_dir_variable(harness);
    let value = dir.display().to_string();
    let env: Vec<(&str, &str)> = match variable {
        Some(name) => vec![(name, value.as_str())],
        None => Vec::new(),
    };
    runner.run(program, args, dir, &env, &mut lines)?;
    let outcome = SignInOutcome {
        signed_in: account.is_some(),
        account,
        prompts,
    };
    sink.done(&outcome);
    Ok(outcome)
}

/// One recorded command: program, args, working directory, and the env it was given.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordedCall {
    pub program: String,
    pub args: Vec<String>,
    pub dir: PathBuf,
    pub env: Vec<(String, String)>,
}

/// A runner that records every command and answers from a script. Tests only.
#[derive(Default)]
pub struct ScriptedRunner {
    pub calls: Vec<RecordedCall>,
    answers: BTreeMap<String, String>,
    fail: BTreeMap<String, String>,
}

impl ScriptedRunner {
    pub fn answer(&mut self, program: &str, output: &str) {
        self.answers.insert(program.to_string(), output.to_string());
    }
}

impl CommandRunner for ScriptedRunner {
    fn run(
        &mut self,
        program: &str,
        args: &[String],
        dir: &Path,
        env: &[(&str, &str)],
        on_line: &mut dyn LineSink,
    ) -> Result<(), InstallError> {
        self.calls.push(RecordedCall {
            program: program.to_string(),
            args: args.to_vec(),
            dir: dir.to_path_buf(),
            env: env
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        });
        if let Some(reason) = self.fail.get(program) {
            return Err(InstallError::Failed(reason.clone()));
        }
        if let Some(output) = self.answers.get(program) {
            for line in output.lines() {
                on_line.line(line);
            }
        }
        Ok(())
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
        let call = &runner.calls[0];
        assert_eq!(call.program, "npm");
        assert!(
            call.args
                .iter()
                .any(|a| a.contains("@anthropic-ai/claude-code@2.0.0"))
        );
        assert!(call.dir.ends_with("harnesses/claude-code/2.0.0"));
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
        struct Collect(Vec<SignInPrompt>);
        impl SignInSink for Collect {
            fn prompt(&mut self, prompt: &SignInPrompt) {
                self.0.push(prompt.clone());
            }
            fn done(&mut self, _: &SignInOutcome) {}
        }
        let mut sink = Collect(Vec::new());
        let outcome = sign_in(
            &catalog(),
            root.path(),
            "codex",
            "default",
            &mut runner,
            &mut sink,
        )
        .unwrap();
        assert!(outcome.signed_in);
        assert_eq!(outcome.account.as_deref(), Some("ada@example"));
        assert_eq!(
            outcome.prompts,
            vec![
                SignInPrompt::Link("https://auth.example/device".to_string()),
                SignInPrompt::DeviceCode("ABCD-1234".to_string()),
            ]
        );
        let call = &runner.calls[0];
        assert!(call.dir.ends_with("config/codex/default"));
        assert!(
            call.env
                .iter()
                .any(|(k, v)| k == "CODEX_HOME" && v.ends_with("config/codex/default"))
        );
        assert_eq!(sink.0, outcome.prompts);
    }

    #[test]
    fn sign_in_at_writes_into_the_given_directory() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("agents/agt_1/config");
        let mut runner = ScriptedRunner::default();
        runner.answer("claude", "SIGNED_IN ada@example\n");
        let mut sink = NopSignIn;
        let outcome = sign_in_at(&catalog(), &dir, "claude-code", &mut runner, &mut sink).unwrap();
        assert!(outcome.signed_in);
        let call = &runner.calls[0];
        assert_eq!(
            call.dir, dir,
            "the vendor runs in the agent's own directory"
        );
        assert!(call.args.iter().any(|arg| arg == "login"));
        assert!(
            call.env
                .iter()
                .any(|(key, value)| key == "CLAUDE_CONFIG_DIR" && Path::new(value) == dir)
        );
    }

    struct NopSignIn;

    impl SignInSink for NopSignIn {
        fn prompt(&mut self, _: &SignInPrompt) {}
        fn done(&mut self, _: &SignInOutcome) {}
    }

    #[test]
    fn a_link_reaches_the_client_before_sign_in_finishes() {
        let root = tempfile::tempdir().unwrap();
        let mut runner = ScriptedRunner::default();
        runner.answer("codex", "DEVICE ABCD\nSIGNED_IN ada@example");
        struct Order {
            prompts_before_done: usize,
            done: bool,
        }
        impl SignInSink for Order {
            fn prompt(&mut self, _: &SignInPrompt) {
                assert!(!self.done, "a prompt arrived after sign-in finished");
                self.prompts_before_done += 1;
            }
            fn done(&mut self, _: &SignInOutcome) {
                self.done = true;
            }
        }
        let mut sink = Order {
            prompts_before_done: 0,
            done: false,
        };
        sign_in(
            &catalog(),
            root.path(),
            "codex",
            "default",
            &mut runner,
            &mut sink,
        )
        .unwrap();
        assert!(sink.done);
        assert_eq!(sink.prompts_before_done, 1);
    }
}
