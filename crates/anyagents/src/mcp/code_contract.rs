//! The `code` tool's contract with the model: how long a call may run, and
//! what an error says when a snippet is not the language or API the sandbox
//! runs.
//!
//! The sandbox compiles TypeScript inside an async function body. Models keep
//! writing Python, ESM modules and Node scripts into it, and guessing API
//! names. Each failure names its fix here, so the next call is right instead
//! of the next ten.

use std::path::Path;

/// Default wall time of one `code` call, and of a `workspace.exec` that names
/// no `timeout_ms`.
pub(super) const DEFAULT_CODE_TIMEOUT_MS: u64 = 30_000;
/// Default ceiling for a per-call `timeout_ms`.
pub(super) const DEFAULT_CODE_MAX_TIMEOUT_MS: u64 = 600_000;
const MIN_TIMEOUT_MS: u64 = 100;
/// No configuration may hold one tool call open longer than a day.
const HARD_MAX_TIMEOUT_MS: u64 = 86_400_000;

/// `<data>/tool-limits.json`, read on every call so an operator's change
/// applies to the next call without a restart.
pub(super) const LIMITS_FILE: &str = "tool-limits.json";
const DEFAULT_ENV: &str = "ANYAGENTS_CODE_TIMEOUT_MS";
const MAX_ENV: &str = "ANYAGENTS_CODE_MAX_TIMEOUT_MS";

/// How long one `code` call or synchronous `workspace.exec` may run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CodeLimits {
    pub default_ms: u64,
    pub max_ms: u64,
}

impl Default for CodeLimits {
    fn default() -> Self {
        Self {
            default_ms: DEFAULT_CODE_TIMEOUT_MS,
            max_ms: DEFAULT_CODE_MAX_TIMEOUT_MS,
        }
    }
}

#[derive(serde::Deserialize, Default)]
struct LimitsFile {
    #[serde(default)]
    code_timeout_ms: Option<u64>,
    #[serde(default)]
    code_max_timeout_ms: Option<u64>,
}

impl CodeLimits {
    /// The environment (`ANYAGENTS_CODE_TIMEOUT_MS`,
    /// `ANYAGENTS_CODE_MAX_TIMEOUT_MS`) over `<data>/tool-limits.json`
    /// (`code_timeout_ms`, `code_max_timeout_ms`) over the defaults.
    pub(super) fn load(data_dir: Option<&Path>) -> Self {
        let file = data_dir
            .and_then(|dir| std::fs::read_to_string(dir.join(LIMITS_FILE)).ok())
            .and_then(|text| serde_json::from_str::<LimitsFile>(&text).ok())
            .unwrap_or_default();
        let env = |name: &str| {
            std::env::var(name)
                .ok()
                .and_then(|raw| raw.trim().parse::<u64>().ok())
        };
        Self::from_parts(
            env(DEFAULT_ENV).or(file.code_timeout_ms),
            env(MAX_ENV).or(file.code_max_timeout_ms),
        )
    }

    fn from_parts(default_ms: Option<u64>, max_ms: Option<u64>) -> Self {
        let max_ms = max_ms
            .unwrap_or(DEFAULT_CODE_MAX_TIMEOUT_MS)
            .clamp(1_000, HARD_MAX_TIMEOUT_MS);
        let default_ms = default_ms
            .unwrap_or(DEFAULT_CODE_TIMEOUT_MS)
            .clamp(MIN_TIMEOUT_MS, max_ms);
        Self { default_ms, max_ms }
    }

    /// The timeout one call runs with: its own `timeout_ms` up to the ceiling.
    pub(super) fn resolve(&self, requested: Option<u64>) -> u64 {
        requested
            .unwrap_or(self.default_ms)
            .clamp(MIN_TIMEOUT_MS, self.max_ms)
    }
}

/// What a `code` call that ran out of time tells the model to do next.
pub(super) fn code_timeout_message(timeout_ms: u64, requested: Option<u64>, max_ms: u64) -> String {
    let clamped = match requested {
        Some(asked) if asked > max_ms => format!(" (asked {asked}ms; the ceiling is {max_ms}ms)"),
        _ => String::new(),
    };
    format!(
        "`code` execution timed out after {timeout_ms}ms{clamped} and was terminated. \
         Pass `timeout_ms` (up to {max_ms}) on the `code` call, or start long commands as jobs: \
         `const j = await workspace.exec({{cmd, background: true}})`, then \
         `await workspace.job({{id: j.job_id, wait_ms: 60000}})` in this or a later call."
    )
}

/// What a synchronous `workspace.exec` that ran out of time tells the model.
pub(super) fn exec_timeout_hint(max_ms: u64) -> String {
    format!(
        "pass `timeout_ms` (up to {max_ms}), or run it as a job with `background: true` \
         and poll it with `workspace.job({{id, wait_ms}})`"
    )
}

/// Every function a snippet can call, by namespace. The model guesses names;
/// a guess that misses is answered with this list.
pub(super) const SANDBOX_API: &[(&str, &[&str])] = &[
    (
        "workspace",
        &[
            "read",
            "write",
            "edit",
            "multi_edit",
            "patch",
            "glob",
            "grep",
            "list",
            "exec",
            "job",
        ],
    ),
    (
        "anyagents",
        &[
            "send",
            "inbox",
            "agents",
            "schedule",
            "schedules",
            "conversation",
            "search",
            "groups",
            "browser",
            "skills",
            "memory",
            "web",
            "computer",
            "device",
            "media",
            "files",
            "subagents",
            "spawn",
            "describe",
            "archive",
            "wake",
            "interact",
        ],
    ),
    (
        "memory",
        &[
            "read",
            "write",
            "upsert",
            "delete",
            "list",
            "search",
            "recall",
            "status",
            "consolidate",
        ],
    ),
    ("web", &["search", "fetch"]),
    (
        "tools",
        &[
            "anyagents",
            "workspace",
            "memory",
            "web",
            "computer",
            "device",
            "media",
        ],
    ),
];

/// A fix for a failed snippet, when the failure is one models keep making.
/// `compile` is true when the snippet never ran.
pub(super) fn diagnose(code: &str, error: &str, compile: bool) -> Option<String> {
    if compile {
        if looks_double_escaped(code) {
            return Some(
                "the `code` string holds literal `\\n` sequences and no line breaks: it was \
                 JSON-escaped twice. Send the snippet with real newlines."
                    .to_string(),
            );
        }
        if looks_python(code) {
            return Some(python_fix());
        }
        if has_module_syntax(code) {
            return Some(
                "`import`/`export` cannot be used: the snippet is the body of an async function \
                 and `workspace`, `anyagents`, `web` and `tools` are already in scope. Delete the \
                 import lines; use `workspace.read`/`write` for files and `workspace.exec` for \
                 commands."
                    .to_string(),
            );
        }
        if error.contains("await is only valid") || error.contains("Unexpected reserved word") {
            return Some(
                "`await` inside a callback needs an `async` callback, e.g. \
                 `await Promise.all(xs.map(async (x) => await f(x)))`."
                    .to_string(),
            );
        }
    }
    if let Some(name) = undefined_global(error) {
        if code.contains(&format!("${{{name}")) {
            return Some(format!(
                "`${{{name}...}}` inside a JS template literal is JavaScript interpolation; to pass \
                 it to the shell escape it as `\\${{{name}...}}` or build the command from plain \
                 strings."
            ));
        }
        return Some(match name {
            "require" | "module" | "exports" | "__dirname" | "__filename" => {
                "`require` and Node modules are unavailable. Use `workspace.read`/`write`/`list` \
                 for files and `workspace.exec({cmd})` for commands (e.g. `node script.js`)."
                    .to_string()
            }
            "process" => "`process` (env, argv, cwd, exit) is unavailable. Read environment \
                          values with `workspace.exec({cmd: 'printenv NAME'})`; just `return` \
                          to finish."
                .to_string(),
            "Buffer" => "`Buffer` is unavailable. Work with strings; for bytes or base64 run a \
                         command with `workspace.exec`."
                .to_string(),
            "fetch" | "XMLHttpRequest" | "WebSocket" => {
                "`fetch` is unavailable. Use `web.fetch(url)` or `web.search(query)`.".to_string()
            }
            "btoa" | "atob" | "TextEncoder" | "TextDecoder" => format!(
                "`{name}` is unavailable. Work with strings; for base64 or bytes run a command \
                 with `workspace.exec` (e.g. `base64 -w0 FILE`)."
            ),
            "print" | "len" | "True" | "False" | "None" | "self" => python_fix(),
            other => format!(
                "`{other}` is not a sandbox global. Globals: `workspace`, `anyagents`, `web`, \
                 `memory`, `computer`, `device`, `media`, `tools`, `console`, `setTimeout`."
            ),
        });
    }
    if let Some(path) = missing_function(error) {
        return Some(unknown_api_fix(&path));
    }
    if error.contains("\"[object Object]\" is not valid JSON") {
        return Some(
            "`JSON.parse` got an object: tool calls return parsed objects already \
             (`workspace.read` returns `{content, truncated, ...}`; parse `res.content`)."
                .to_string(),
        );
    }
    if error.contains("in JSON at position") && code.contains("workspace.read") {
        return Some(
            "`workspace.read` returns at most `max_bytes` (default 46080) per call and sets \
             `truncated: true`; pass `max_bytes` (up to 262144) or read on with `byte_offset`, \
             or extract fields with `workspace.exec({cmd: \"jq ...\"})`."
                .to_string(),
        );
    }
    None
}

fn python_fix() -> String {
    "this tool runs TypeScript/JavaScript, not Python. Rewrite it as TS (`const x = ...`, \
     `for (const x of xs) { ... }`, `//` comments, `console.log`), or run Python through the \
     shell: `await workspace.exec({cmd: \"python3 - <<'EOF'\\n...\\nEOF\"})`."
        .to_string()
}

/// One physical line whose `\n` escapes sit where statements begin: a
/// snippet whose line breaks arrived as two characters each.
fn looks_double_escaped(code: &str) -> bool {
    if code.trim().contains('\n') {
        return false;
    }
    code.split("\\n").skip(1).any(|rest| {
        let rest = rest.trim_start_matches([' ', '\t']);
        [
            "const ", "let ", "var ", "return", "await ", "if ", "if(", "for ", "for(", "}", "//",
        ]
        .iter()
        .any(|start| rest.starts_with(start))
    })
}

fn looks_python(code: &str) -> bool {
    code.lines().map(str::trim).any(|line| {
        let words: Vec<&str> = line.split_whitespace().collect();
        let block_colon = line.ends_with(':') && !line.contains('{') && !line.contains("//");
        match words.as_slice() {
            ["from", module, "import", ..] => !module.starts_with(['\'', '"']),
            ["import", module] => {
                module
                    .trim_end_matches(';')
                    .chars()
                    .all(|c| c.is_alphanumeric() || c == '_' || c == '.')
                    && !line.ends_with(';')
            }
            ["import", _, "as", _] => !line.contains(['\'', '"']),
            ["def", ..]
            | ["class", ..]
            | ["elif", ..]
            | ["for", ..]
            | ["while", ..]
            | ["if", ..]
            | ["with", ..]
            | ["try:"]
            | ["else:"]
            | ["except", ..]
            | ["except:"]
            | ["finally:"] => block_colon,
            _ => false,
        }
    })
}

fn has_module_syntax(code: &str) -> bool {
    code.contains("import(")
        || code.lines().map(str::trim_start).any(|line| {
            line.starts_with("export ")
                || (line.starts_with("import ")
                    && (line.contains(" from ") || line.contains(['\'', '"'])))
        })
}

/// `X is not defined` names an identifier the sandbox does not have.
fn undefined_global(error: &str) -> Option<&str> {
    let (head, _) = error.split_once(" is not defined")?;
    let name = head
        .rsplit(|c: char| !(c.is_alphanumeric() || c == '_' || c == '$'))
        .next()?;
    (!name.is_empty()).then_some(name)
}

/// `a.b.c is not a function` names a call into a namespace that has no such
/// member.
fn missing_function(error: &str) -> Option<String> {
    let (head, _) = error.split_once(" is not a function")?;
    let path = head
        .rsplit(|c: char| !(c.is_alphanumeric() || c == '_' || c == '.' || c == '$'))
        .next()?;
    let root = path.split('.').next()?;
    SANDBOX_API
        .iter()
        .any(|(ns, _)| *ns == root)
        .then(|| path.to_string())
}

fn unknown_api_fix(path: &str) -> String {
    let mut parts: Vec<&str> = path.split('.').collect();
    let missing = parts.pop().unwrap_or_default();
    // `tools.anyagents.x` and `anyagents.x` name the same namespace.
    let namespace = parts.last().copied().unwrap_or_default();
    match SANDBOX_API.iter().find(|(ns, _)| *ns == namespace) {
        Some((ns, names)) => format!(
            "`{path}` does not exist. `{ns}` has: {}. `anyagents.<tool>(args)` takes the same \
             object as the MCP tool of that name.",
            names.join(", ")
        ),
        None => format!(
            "`{path}` is not a function; `{missing}` is not part of the sandbox API. Namespaces: \
             workspace, anyagents, web, memory, computer, device, media, tools."
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_default_to_thirty_seconds_and_a_ten_minute_ceiling() {
        let limits = CodeLimits::from_parts(None, None);
        assert_eq!(limits.default_ms, 30_000);
        assert_eq!(limits.max_ms, 600_000);
        assert_eq!(limits.resolve(None), 30_000);
        assert_eq!(limits.resolve(Some(300_000)), 300_000);
        assert_eq!(limits.resolve(Some(3_600_000)), 600_000);
        assert_eq!(limits.resolve(Some(1)), 100);
    }

    #[test]
    fn limits_come_from_the_data_file_and_stay_consistent() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(LIMITS_FILE),
            r#"{"code_timeout_ms": 45000, "code_max_timeout_ms": 900000}"#,
        )
        .unwrap();
        // The environment is process-wide; the file path is what this
        // asserts, so only run the file assertion when neither is set.
        if std::env::var_os(DEFAULT_ENV).is_none() && std::env::var_os(MAX_ENV).is_none() {
            let limits = CodeLimits::load(Some(dir.path()));
            assert_eq!(limits.default_ms, 45_000);
            assert_eq!(limits.max_ms, 900_000);
        }
        // A default above the ceiling is the ceiling; a ceiling is never
        // below a second or above a day.
        assert_eq!(
            CodeLimits::from_parts(Some(700_000), Some(600_000)),
            CodeLimits {
                default_ms: 600_000,
                max_ms: 600_000
            }
        );
        assert_eq!(CodeLimits::from_parts(None, Some(5)).max_ms, 1_000);
        assert_eq!(
            CodeLimits::from_parts(None, Some(u64::MAX)).max_ms,
            HARD_MAX_TIMEOUT_MS
        );
        // A missing or malformed file is the defaults.
        std::fs::write(dir.path().join(LIMITS_FILE), "not json").unwrap();
        if std::env::var_os(DEFAULT_ENV).is_none() && std::env::var_os(MAX_ENV).is_none() {
            assert_eq!(CodeLimits::load(Some(dir.path())), CodeLimits::default());
        }
    }

    #[test]
    fn timeout_messages_name_the_ceiling_and_the_job_route() {
        let msg = code_timeout_message(30_000, None, 600_000);
        assert!(msg.contains("timed out after 30000ms"), "{msg}");
        assert!(msg.contains("`timeout_ms` (up to 600000)"), "{msg}");
        assert!(msg.contains("background: true"), "{msg}");
        assert!(msg.contains("workspace.job"), "{msg}");
        let clamped = code_timeout_message(600_000, Some(1_800_000), 600_000);
        assert!(clamped.contains("asked 1800000ms"), "{clamped}");
        let hint = exec_timeout_hint(600_000);
        assert!(hint.contains("up to 600000") && hint.contains("background: true"));
    }

    #[test]
    fn python_snippets_get_the_language_fix() {
        for code in [
            "import json\nprint(json.dumps({}))",
            "from pathlib import Path\nprint(Path('.'))",
            "import os.path as p",
            "for a in [1, 2]:\n    print(a)",
            "def main():\n    return 1",
            "if x == 1:\n    pass",
        ] {
            let fix = diagnose(code, "SyntaxError: Expected ';'", true).unwrap();
            assert!(fix.contains("not Python"), "{code}: {fix}");
            assert!(fix.contains("python3 -"), "{code}: {fix}");
        }
        // TypeScript with colons and braces is not Python.
        for code in [
            "const x: number = 1;\nreturn x;",
            "for (const a of [1, 2]) {\n  console.log(a);\n}",
            "if (x) {\n  return 1;\n}",
            "switch (x) {\n  case 1:\n    break;\n}",
        ] {
            assert!(
                diagnose(code, "SyntaxError: Unexpected token", true)
                    .is_none_or(|fix| !fix.contains("not Python")),
                "{code}"
            );
        }
        let fix = diagnose("x = 1", "ReferenceError: print is not defined", false).unwrap();
        assert!(fix.contains("not Python"), "{fix}");
    }

    #[test]
    fn module_syntax_gets_the_in_scope_fix() {
        for code in [
            "import fs from \"fs\";\nreturn fs.readFileSync('x');",
            "import { readFile } from 'node:fs/promises';\nreturn 1;",
            "import \"./setup\";",
            "export const x = 1;",
            "const fs = await import('fs');",
        ] {
            let fix = diagnose(
                code,
                "SyntaxError: 'import', and 'export' cannot be used outside of module code",
                true,
            )
            .unwrap();
            assert!(
                fix.contains("`import`/`export` cannot be used"),
                "{code}: {fix}"
            );
            assert!(fix.contains("already in scope"), "{fix}");
        }
    }

    #[test]
    fn node_globals_name_the_sandbox_replacement() {
        let cases = [
            ("ReferenceError: require is not defined", "workspace.exec"),
            ("ReferenceError: process is not defined", "printenv"),
            ("ReferenceError: Buffer is not defined", "Work with strings"),
            ("ReferenceError: fetch is not defined", "web.fetch(url)"),
            (
                "ReferenceError: foo is not defined",
                "`foo` is not a sandbox global",
            ),
        ];
        for (error, expected) in cases {
            let fix = diagnose("return 1;", error, false).unwrap();
            assert!(fix.contains(expected), "{error}: {fix}");
        }
    }

    #[test]
    fn shell_variables_in_template_literals_are_named() {
        let fix = diagnose(
            "await workspace.exec(`make 2>&1 | tail; echo ${PIPESTATUS[0]}`);",
            "ReferenceError: PIPESTATUS is not defined",
            false,
        )
        .unwrap();
        assert!(fix.contains("`\\${PIPESTATUS...}`"), "{fix}");
    }

    #[test]
    fn missing_functions_list_the_namespace() {
        let fix = diagnose(
            "await anyagents.mailbox()",
            "TypeError: anyagents.mailbox is not a function\n    at __fn (evalmachine.<anonymous>:3:17)",
            false,
        )
        .unwrap();
        assert!(fix.contains("`anyagents.mailbox` does not exist"), "{fix}");
        assert!(fix.contains("inbox") && fix.contains("schedule"), "{fix}");
        let fix = diagnose(
            "await tools.workspace.bash('ls')",
            "TypeError: tools.workspace.bash is not a function",
            false,
        )
        .unwrap();
        assert!(fix.contains("`workspace` has: read"), "{fix}");
        assert!(fix.contains("exec"), "{fix}");
        // A user function that is not a function is the snippet's own bug.
        assert!(diagnose("x()", "TypeError: x is not a function", false).is_none());
    }

    #[test]
    fn double_escaped_snippets_are_named() {
        let fix = diagnose(
            r#"const a = await workspace.read("x");\nreturn a;"#,
            "SyntaxError: Expected unicode escape",
            true,
        )
        .unwrap();
        assert!(fix.contains("JSON-escaped twice"), "{fix}");
        // A one-line snippet without escapes is not.
        assert!(
            diagnose("return 1 +;", "SyntaxError: Unexpected token", true)
                .is_none_or(|fix| !fix.contains("escaped"))
        );
    }

    #[test]
    fn json_parse_mistakes_on_tool_results_are_named() {
        let fix = diagnose(
            "return JSON.parse(await workspace.read('x.json'));",
            "SyntaxError: \"[object Object]\" is not valid JSON",
            false,
        )
        .unwrap();
        assert!(fix.contains("res.content"), "{fix}");
        let fix = diagnose(
            "const r = await workspace.read('big.json'); return JSON.parse(r.content);",
            "SyntaxError: Unterminated string in JSON at position 46080",
            false,
        )
        .unwrap();
        assert!(
            fix.contains("max_bytes") && fix.contains("byte_offset"),
            "{fix}"
        );
    }

    #[test]
    fn await_in_a_plain_callback_gets_the_async_fix() {
        let fix = diagnose(
            "const r = xs.map((x) => await f(x));",
            "SyntaxError: await is only valid in async functions",
            true,
        )
        .unwrap();
        assert!(fix.contains("Promise.all"), "{fix}");
    }
}
