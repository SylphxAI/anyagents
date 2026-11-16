//! Shared operator-facing headings, receipts and structured values.
use std::sync::atomic::{AtomicBool, Ordering};

use super::{Table, bold, bold_cyan, dim, green, is_pretty, red};

static VERBOSE: AtomicBool = AtomicBool::new(false);

pub fn set_verbose(verbose: bool) {
    VERBOSE.store(verbose, Ordering::Relaxed);
}

pub fn is_verbose() -> bool {
    VERBOSE.load(Ordering::Relaxed)
}

/// Keep secondary metadata visually subordinate to the task at hand.
pub fn heading(title: &str) {
    println!("\n  {}\n", bold_cyan(title));
}

pub fn success(message: &str) {
    println!("  {} {}", green("✓"), bold(message));
}

pub fn note(message: &str) {
    println!("  {}", dim(message));
}

pub fn error(message: &str) {
    if is_pretty() {
        eprintln!("\n  {} {}\n", red("✗ Error:"), message.replace('\t', "  "));
    } else {
        eprintln!("{message}");
    }
}

/// Structured responses without exposing serialization punctuation to humans.
/// Explicit JSON callers keep their existing serializers and bypass this helper.
pub fn value(title: &str, value: &serde_json::Value) {
    heading(title);
    value_at(value, 2);
    println!();
}

fn value_at(value: &serde_json::Value, indent: usize) {
    match value {
        serde_json::Value::Object(fields) => {
            for (key, value) in fields {
                if !is_verbose()
                    && matches!(
                        key.as_str(),
                        "path" | "workdir" | "config_dir" | "binary" | "socket" | "profile_dir"
                    )
                {
                    continue;
                }
                let label = key.replace('_', " ");
                match value {
                    serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
                        println!("{}{}", " ".repeat(indent), bold(&label));
                        value_at(value, indent + 2);
                    }
                    _ => println!("{}{}  {}", " ".repeat(indent), dim(&label), scalar(value)),
                }
            }
        }
        serde_json::Value::Array(items) => {
            if items.is_empty() {
                println!("{}{}", " ".repeat(indent), dim("None."));
            }
            for item in items {
                value_at(item, indent);
            }
        }
        _ => println!("{}{}", " ".repeat(indent), scalar(value)),
    }
}

fn scalar(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.replace('\t', "  "),
        serde_json::Value::Null => "—".into(),
        _ => value.to_string(),
    }
}

/// Shared schedule renderer for both server and offline responses.
pub fn schedules(value: &serde_json::Value) {
    if let Some(count) = value.get("cancelled") {
        success(&format!("Cancelled {count} schedule(s)"));
    }
    if value.get("schedule").is_none() && value.get("schedules").is_none() {
        return;
    }
    let rows: Vec<&serde_json::Value> = match value.get("schedule") {
        Some(one) => {
            success("Schedule created");
            vec![one]
        }
        None => value
            .get("schedules")
            .and_then(|v| v.as_array())
            .map(|rows| rows.iter().collect())
            .unwrap_or_default(),
    };
    heading("Schedules");
    if rows.is_empty() {
        note("No schedules. Use `anyagents schedule --help` to add one.");
        return;
    }
    let fleet = rows.iter().any(|row| row.get("agent_name").is_some());
    let mut columns = Vec::new();
    if fleet {
        columns.push(("AGENT", false));
    }
    columns.extend([
        ("KIND", false),
        ("WHEN / TARGET", false),
        ("STATE", false),
        ("PROMPT", false),
    ]);
    if is_verbose() {
        columns.push(("ID", false));
    }
    let mut table = Table::new(&columns);
    for row in rows {
        let mut keys = Vec::new();
        if fleet {
            keys.push("agent_name");
        }
        keys.extend(["kind", "spec", "state", "prompt"]);
        if is_verbose() {
            keys.push("id");
        }
        table.add_row(
            keys.iter()
                .map(|key| {
                    row.get(key)
                        .map(scalar)
                        .unwrap_or_else(|| "—".into())
                        .replace(['\n', '\r'], " ")
                })
                .collect(),
        );
    }
    table.print();
    println!();
}
