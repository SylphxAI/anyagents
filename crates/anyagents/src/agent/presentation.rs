//! Human-only agent receipts. Wire output remains at each call site in `agent`.

use serde_json::Value;

use crate::{api::AgentView, ui};

pub(super) fn receipt(action: &str, name: &str) {
    ui::success(&format!("{action} {}", ui::bold(name)));
}

pub(super) fn lifecycle(action: &str, view: &AgentView) {
    receipt(
        action,
        &crate::create::format_agent_display(&view.name, view.role.as_deref()),
    );
    if action == "Created" || action == "Spawned" {
        let model = view.model.as_deref().unwrap_or("default model");
        ui::note(&format!("{} · {model} · {}", view.harness, view.state));
    }
    if ui::is_verbose() {
        ui::note(&format!("ID: {}", view.id));
        if let Some(parent) = &view.parent {
            ui::note(&format!("Parent: {parent}"));
        }
        if let Some(workdir) = &view.workdir {
            ui::note(&format!("Workspace: {workdir}"));
        }
    }
}

pub(super) fn renamed(old: &str, name: &str, aliases: &[String]) {
    ui::success(&format!("Renamed {} → {}", ui::bold(old), ui::bold(name)));
    if !aliases.is_empty() {
        ui::note(&format!("Aliases: {}", aliases.join(", ")));
    }
}

pub(super) fn role(name: &str, role: Option<&str>) {
    ui::success(&format!(
        "Role updated: {} · {}",
        ui::bold(name),
        role.unwrap_or("no role")
    ));
}

fn text<'a>(value: &'a Value, key: &str, fallback: &'a str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or(fallback)
}

// Only known public fields are rendered. Runtime responses also carry internal
// filesystem paths, so dumping the response is never a human-mode fallback.
fn runtime_details(value: &Value, verbose: bool) -> Vec<String> {
    let mut lines = vec![format!(
        "{} · {} · profile {}",
        text(value, "harness", "default harness"),
        text(value, "model", "default model"),
        text(value, "profile", "default"),
    )];
    let cutover = match text(value, "cutover_status", "") {
        "pending_active_turn" => Some("Applies after the active turn finishes."),
        "running" => Some("Active runtime updated."),
        "ready_on_next_wake" => Some("Ready on the next wake."),
        _ => None,
    };
    if let Some(cutover) = cutover {
        lines.push(cutover.into());
    }
    if verbose {
        for (key, label) in [("id", "ID"), ("workdir", "Workspace")] {
            if let Some(value) = value.get(key).and_then(Value::as_str) {
                lines.push(format!("{label}: {value}"));
            }
        }
    }
    lines
}

pub(crate) fn harness(value: &Value, name: &str) {
    receipt("Runtime updated", text(value, "name", name));
    for line in runtime_details(value, ui::is_verbose()) {
        ui::note(&line);
    }
}

pub(super) fn reset(value: &Value, name: &str) {
    receipt("Session reset", text(value, "name", name));
    if let Some(generation) = value.get("generation").and_then(Value::as_u64) {
        ui::note(&format!("Session generation {generation}."));
    }
}

pub(super) fn revoked(value: &Value, name: &str) {
    receipt("Remote sessions revoked", text(value, "agent", name));
    if let Some(count) = value.get("revoked").and_then(Value::as_u64) {
        ui::note(&format!("{count} session(s) revoked."));
    }
}

pub(super) fn description(name: &str, role: Option<&str>, description: &str, updated: bool) {
    let display = crate::create::format_agent_display(name, role);
    if updated {
        receipt("Updated", &display);
    } else {
        ui::heading(&display);
    }
    ui::note(&description_text(description));
}

fn description_text(description: &str) -> String {
    if description == "-" {
        "No description.".into()
    } else {
        description.replace('\t', "  ")
    }
}

pub(super) fn descriptions(agents: &[AgentView]) {
    let mut has_rows = false;
    let mut table = ui::Table::new(&[
        ("AGENT", false),
        ("HARNESS", false),
        ("STATE", false),
        ("DESCRIPTION", false),
    ]);
    for agent in agents {
        if agent.state == "archived" && agent.parent.is_some() {
            continue;
        }
        has_rows = true;
        table.add_row(vec![
            crate::create::format_agent_display(&agent.name, agent.role.as_deref()),
            agent.harness.clone(),
            agent.state.clone(),
            agent
                .description
                .as_deref()
                .unwrap_or("—")
                .replace(['\t', '\n', '\r'], " "),
        ]);
    }
    ui::heading("Agent descriptions");
    if !has_rows {
        ui::note("No agents found.");
    } else {
        table.print();
    }
}

// Diagnostic reasons come from arbitrary harness errors and may embed internal
// paths. Keep the semantic status visible; disclose exact diagnostics only on
// explicit request, without trying to guess whether a string contains a path.
pub(super) fn diagnostic(name: &str, status: &str, detail: &str, reason: Option<&str>) {
    println!("  {} {} · {detail}", ui::bold(name), ui::yellow(status));
    if let Some(reason) = reason {
        if ui::is_verbose() {
            ui::note(&reason.replace('\t', "  "));
        } else {
            ui::note("Use --verbose for diagnostic details.");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptions_keep_prose_without_tab_delimiters() {
        assert_eq!(description_text("-"), "No description.");
        assert_eq!(description_text("one\ttwo\nthree"), "one  two\nthree");
    }

    #[test]
    fn runtime_receipt_has_semantics_without_internal_paths() {
        let response = serde_json::json!({
            "harness": "codex", "model": "model-a", "profile": "default",
            "cutover_status": "pending_active_turn", "id": "agt_123",
            "workdir": "/private/worktrees/agent", "config_path": "/private/config",
        });
        let normal = runtime_details(&response, false).join("\n");
        assert!(normal.contains("codex · model-a · profile default"));
        assert!(normal.contains("after the active turn"));
        assert!(!normal.contains("/private"));
        assert!(!normal.contains('\t'));
        let verbose = runtime_details(&response, true).join("\n");
        assert!(verbose.contains("Workspace: /private/worktrees/agent"));
        assert!(!verbose.contains("/private/config"));
    }

    #[test]
    fn runtime_cutover_statuses_are_readable() {
        for (status, expected) in [
            ("running", "Active runtime updated."),
            ("ready_on_next_wake", "Ready on the next wake."),
        ] {
            let response = serde_json::json!({ "cutover_status": status });
            assert!(
                runtime_details(&response, false)
                    .iter()
                    .any(|line| line == expected)
            );
        }
        let response = serde_json::json!({ "cutover_status": "/private/unknown" });
        assert!(
            !runtime_details(&response, false)
                .join("\n")
                .contains("/private")
        );
    }
}
