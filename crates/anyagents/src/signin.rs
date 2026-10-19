//! `harnesses sign-in` and `sign-in`. The installer's vendor sign-in is run for
//! the config directory of a recorded agent, so the credential lands in the same
//! store the agent's harness is started with. The credential itself stays in the
//! vendor's store: this module never reads it, prints it or copies it.

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};

use anyagents_core::{Error, Store, agent_config_dir, agent_work_dir};
use anyagents_installer::{
    CommandRunner, InstallError, InstallMethod, LineSink, SignInOutcome, SignInPrompt, SignInSink,
    builtin_catalog, config_dir_variable,
};

/// Which harnesses this client can sign in.
pub fn signable(harness: &str) -> bool {
    builtin_catalog().iter().any(|entry| entry.id == harness)
}

/// The variable that points `harness` at its own config directory.
pub fn config_variable(harness: &str) -> Option<&'static str> {
    config_dir_variable(harness)
}

/// How the vendor tool is installed. Printed so the client knows what is missing
/// when the program is not on the machine.
pub fn install_hint(harness: &str) -> Option<String> {
    builtin_catalog()
        .into_iter()
        .find(|entry| entry.id == harness)
        .map(|entry| match entry.install {
            InstallMethod::Npm { package } => format!("npm install -g {package}"),
            InstallMethod::VendorScript { url } => format!("curl -fsSL {url} | sh"),
            InstallMethod::ReleaseBinary { url, .. } => url,
        })
}

/// Run the vendor sign-in for the agent's own config directory.
pub fn sign_in_agent(
    data: &Path,
    agent_name: &str,
    runner: &mut dyn CommandRunner,
    sink: &mut dyn SignInSink,
) -> Result<SignInOutcome, Error> {
    let store = Store::open(data.join("state.db"))?;
    let agent = store.agent_by_name(agent_name)?;
    let root = store.data_dir().to_path_buf();
    if !signable(&agent.harness) {
        return Err(Error::NotImplemented(format!(
            "sign-in for harness {}",
            agent.harness
        )));
    }
    let dir = agent_config_dir(&root, &agent.id);
    std::fs::create_dir_all(&dir)?;
    // The vendor command needs a working directory; the supervisor would create
    // it before the first launch, and an agent that was never launched has none.
    std::fs::create_dir_all(agent_work_dir(&root, &agent.id))?;
    anyagents_installer::sign_in_at(&builtin_catalog(), &dir, &agent.harness, runner, sink).map_err(
        |err| match err {
            InstallError::UnknownHarness(id) => {
                Error::NotImplemented(format!("sign-in for harness {id}"))
            }
            other => Error::Message(other.to_string()),
        },
    )
}

/// Runs the vendor command for real. Each line it prints reaches the sink before
/// the command returns.
pub struct ProcessRunner;

impl CommandRunner for ProcessRunner {
    fn run(
        &mut self,
        program: &str,
        args: &[String],
        dir: &Path,
        env: &[(&str, &str)],
        on_line: &mut dyn LineSink,
    ) -> Result<(), InstallError> {
        let mut command = Command::new(program);
        command
            .args(args)
            .current_dir(dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        for (key, value) in env {
            command.env(key, value);
        }
        let mut child = command.spawn().map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                InstallError::Failed(format!("{program} is not installed"))
            } else {
                InstallError::Failed(err.to_string())
            }
        })?;
        if let Some(stdout) = child.stdout.take() {
            for line in BufReader::new(stdout).lines() {
                let line = line.map_err(|err| InstallError::Failed(err.to_string()))?;
                on_line.line(&line);
            }
        }
        let status = child
            .wait()
            .map_err(|err| InstallError::Failed(err.to_string()))?;
        if status.success() {
            Ok(())
        } else {
            Err(InstallError::Failed(format!(
                "vendor command exited {status}"
            )))
        }
    }
}

/// Prints each prompt as it arrives. The outcome is printed by the caller so it
/// is the last line, even for a sign-in that never asks for anything.
pub struct TerminalSink;

impl SignInSink for TerminalSink {
    fn prompt(&mut self, prompt: &SignInPrompt) {
        let mut out = std::io::stdout().lock();
        let _ = match prompt {
            SignInPrompt::Link(url) => writeln!(out, "open {url}"),
            SignInPrompt::DeviceCode(code) => writeln!(out, "enter code {code}"),
            SignInPrompt::Prompt(text) => writeln!(out, "{text}"),
        };
        let _ = out.flush();
    }

    fn done(&mut self, _: &SignInOutcome) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_core::NewAgent;
    use anyagents_installer::ScriptedRunner;

    struct Collect(Vec<SignInPrompt>);

    impl SignInSink for Collect {
        fn prompt(&mut self, prompt: &SignInPrompt) {
            self.0.push(prompt.clone());
        }
        fn done(&mut self, _: &SignInOutcome) {}
    }

    fn agent(data: &Path, name: &str, harness: &str) -> anyagents_core::Agent {
        let store = Store::open(data.join("state.db")).unwrap();
        store.create_agent(NewAgent::new(name, harness)).unwrap()
    }

    #[test]
    fn sign_in_uses_the_agents_own_config_directory() {
        let data = tempfile::tempdir().unwrap();
        let agent = agent(data.path(), "ada", "claude-code");
        let mut runner = ScriptedRunner::default();
        runner.answer(
            "claude",
            "LINK https://auth.example/device\nSIGNED_IN ada@example\n",
        );
        let mut sink = Collect(Vec::new());
        let outcome = sign_in_agent(data.path(), "ada", &mut runner, &mut sink).unwrap();
        assert!(outcome.signed_in);
        assert_eq!(outcome.account.as_deref(), Some("ada@example"));
        assert_eq!(sink.0.len(), 1);
        let expected = agent_config_dir(data.path(), &agent.id);
        assert_eq!(runner.calls.len(), 1);
        let call = &runner.calls[0];
        assert_eq!(call.program, "claude");
        assert_eq!(call.args, vec!["auth".to_string(), "login".to_string()]);
        assert_eq!(call.dir, expected);
        assert!(
            call.env
                .iter()
                .any(|(key, value)| key == "CLAUDE_CONFIG_DIR" && Path::new(value) == expected)
        );
        assert_eq!(
            call.env
                .iter()
                .find(|(key, _)| key == "CLAUDE_CONFIG_DIR")
                .map(|(_, value)| Path::new(value)),
            Some(expected.as_path())
        );
    }

    #[test]
    fn sign_in_for_codex_points_it_at_its_own_home() {
        let data = tempfile::tempdir().unwrap();
        let agent = agent(data.path(), "bea", "codex");
        let mut runner = ScriptedRunner::default();
        runner.answer("codex", "SIGNED_IN bea@example\n");
        let mut sink = Collect(Vec::new());
        sign_in_agent(data.path(), "bea", &mut runner, &mut sink).unwrap();
        let call = &runner.calls[0];
        assert!(call.env.iter().any(|(key, value)| key == "CODEX_HOME"
            && Path::new(value) == agent_config_dir(data.path(), &agent.id)));
    }

    #[test]
    fn a_failed_vendor_command_is_an_error_and_keeps_the_prompts() {
        struct Failing;
        impl CommandRunner for Failing {
            fn run(
                &mut self,
                _: &str,
                _: &[String],
                _: &Path,
                _: &[(&str, &str)],
                on_line: &mut dyn LineSink,
            ) -> Result<(), InstallError> {
                on_line.line("LINK https://auth.example/device");
                Err(InstallError::Failed("exit 1".to_string()))
            }
        }
        let data = tempfile::tempdir().unwrap();
        agent(data.path(), "cy", "claude-code");
        let mut sink = Collect(Vec::new());
        let err = sign_in_agent(data.path(), "cy", &mut Failing, &mut sink).unwrap_err();
        assert!(err.to_string().contains("exit 1"), "{err}");
        assert_eq!(sink.0.len(), 1);
    }

    #[test]
    fn an_unknown_agent_is_not_found() {
        let data = tempfile::tempdir().unwrap();
        let mut runner = ScriptedRunner::default();
        let mut sink = Collect(Vec::new());
        let err = sign_in_agent(data.path(), "nobody", &mut runner, &mut sink).unwrap_err();
        assert!(matches!(err, Error::AgentNotFound(_)), "{err}");
        assert!(runner.calls.is_empty());
    }
}
