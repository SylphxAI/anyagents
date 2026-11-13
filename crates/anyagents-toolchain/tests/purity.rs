//! ADR 0005 purity invariants: no ambient binary lookup outside `legacy.rs`
//! (and the shrinking cross-stream allow-list), and no credential dependencies
//! inside `anyagents-toolchain`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Remaining ambient lookups in files owned by other streams (ADR 0003).
/// Entries only shrink as each stream lands; W3 removes this list and
/// `legacy.rs`. Toolchain-owned files must never appear here.
const ALLOW_LIST: &[(&str, &str, usize)] = &[
    (
        "crates/anyagents-core/src/learned_memory.rs",
        "bare_command_new",
        1,
    ),
    ("crates/anyagents-harness/src/lib.rs", "bare_command_new", 1),
    ("crates/anyagents-installer/src/lib.rs", "env_path", 1),
    ("crates/anyagents/src/create.rs", "anyagents_bin_env", 1),
    ("crates/anyagents/src/create.rs", "bare_command_new", 9),
    ("crates/anyagents/src/create.rs", "env_path", 1),
    ("crates/anyagents/src/install.rs", "bare_command_new", 2),
    ("crates/anyagents/src/install.rs", "env_path", 1),
    ("crates/anyagents/src/install.rs", "which_call", 3),
    (
        "crates/anyagents/src/muse_config.rs",
        "anyagents_bin_env",
        1,
    ),
    ("crates/anyagents/src/vendor.rs", "anyagents_bin_env", 4),
];

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates dir")
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

fn collect_src_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_src_rs_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

fn crate_source_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let crates_dir = root.join("crates");
    for entry in std::fs::read_dir(&crates_dir).unwrap().flatten() {
        let src = entry.path().join("src");
        if src.is_dir() {
            collect_src_rs_files(&src, &mut files);
        }
    }
    files.sort();
    files
}

fn is_cfg_test_attr(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.starts_with("#[cfg(") && trimmed.ends_with(")]") && trimmed.contains("test")
}

fn is_mod_open(line: &str) -> bool {
    let trimmed = line.trim();
    let rest = trimmed
        .strip_prefix("pub(crate) ")
        .or_else(|| trimmed.strip_prefix("pub "))
        .unwrap_or(trimmed);
    rest.starts_with("mod ") && rest.contains('{')
}

/// Drop `#[cfg(...test...)] mod ... { ... }` blocks and `//` line comments.
fn non_test_source(raw: &str) -> String {
    let lines: Vec<&str> = raw.lines().collect();
    let mut out = String::with_capacity(raw.len());
    let mut i = 0;
    while i < lines.len() {
        if is_cfg_test_attr(lines[i]) && i + 1 < lines.len() && is_mod_open(lines[i + 1]) {
            let mut depth: i32 = 0;
            i += 1;
            while i < lines.len() {
                for ch in lines[i].chars() {
                    match ch {
                        '{' => depth += 1,
                        '}' => depth -= 1,
                        _ => {}
                    }
                }
                i += 1;
                if depth <= 0 {
                    break;
                }
            }
            continue;
        }
        let trimmed = lines[i].trim_start();
        if !trimmed.starts_with("//") {
            out.push_str(lines[i]);
            out.push('\n');
        }
        i += 1;
    }
    out
}

fn skip_ws(bytes: &[u8], mut idx: usize) -> usize {
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    idx
}

fn is_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn scan_var_calls(text: &str) -> (usize, usize) {
    let bytes = text.as_bytes();
    let mut env_path = 0;
    let mut anyagents_bin_env = 0;
    for prefix in ["var_os", "var"] {
        let mut i = 0;
        while let Some(pos) = text[i..].find(prefix) {
            let start = i + pos;
            i = start + prefix.len();
            if start > 0 && is_ident_char(bytes[start - 1]) {
                continue;
            }
            let after_ws = skip_ws(bytes, start + prefix.len());
            if bytes.get(after_ws) != Some(&b'(') {
                continue;
            }
            let arg_start = skip_ws(bytes, after_ws + 1);
            if bytes.get(arg_start) != Some(&b'"') {
                continue;
            }
            let str_start = arg_start + 1;
            let Some(rel_end) = bytes[str_start..].iter().position(|&b| b == b'"') else {
                continue;
            };
            let after_str = skip_ws(bytes, str_start + rel_end + 1);
            if bytes.get(after_str) != Some(&b')') {
                continue;
            }
            let key = &text[str_start..str_start + rel_end];
            if key == "PATH" {
                env_path += 1;
            } else if key.starts_with("ANYAGENTS_") && key.ends_with("_BIN") {
                anyagents_bin_env += 1;
            }
        }
    }
    (env_path, anyagents_bin_env)
}

fn scan_which_calls(text: &str) -> usize {
    let bytes = text.as_bytes();
    let mut count = 0;
    let mut i = 0;
    while let Some(pos) = text[i..].find("which") {
        let start = i + pos;
        i = start + 5;
        if start > 0 && is_ident_char(bytes[start - 1]) {
            continue;
        }
        let after_ws = skip_ws(bytes, start + 5);
        if bytes.get(after_ws) == Some(&b'(') {
            count += 1;
        }
    }
    count
}

fn scan_command_v(text: &str) -> usize {
    let bytes = text.as_bytes();
    let mut count = 0;
    let mut i = 0;
    while let Some(pos) = text[i..].find("command") {
        let start = i + pos;
        i = start + 7;
        if start > 0 && is_ident_char(bytes[start - 1]) {
            continue;
        }
        let after_ws = skip_ws(bytes, start + 7);
        if after_ws == start + 7 {
            continue;
        }
        if bytes[after_ws..].starts_with(b"-v") {
            let after_v = after_ws + 2;
            if bytes
                .get(after_v)
                .is_none_or(|&b| !is_ident_char(b) && b != b'-')
            {
                count += 1;
            }
        }
    }
    count
}

fn scan_bare_command_new(text: &str) -> usize {
    let bytes = text.as_bytes();
    let needle = "Command::new";
    let mut count = 0;
    let mut i = 0;
    while let Some(pos) = text[i..].find(needle) {
        let start = i + pos;
        i = start + needle.len();
        let after_ws = skip_ws(bytes, start + needle.len());
        if bytes.get(after_ws) != Some(&b'(') {
            continue;
        }
        let arg_start = skip_ws(bytes, after_ws + 1);
        if bytes.get(arg_start) != Some(&b'"') {
            continue;
        }
        let str_start = arg_start + 1;
        let Some(rel_end) = bytes[str_start..].iter().position(|&b| b == b'"') else {
            continue;
        };
        let after_str = skip_ws(bytes, str_start + rel_end + 1);
        if bytes.get(after_str) != Some(&b')') {
            continue;
        }
        let prog = &text[str_start..str_start + rel_end];
        if !prog.is_empty() && !prog.contains('/') {
            count += 1;
        }
    }
    count
}

#[test]
fn no_ambient_binary_lookups_in_non_test_source() {
    let root = workspace_root();
    let mut allow: BTreeMap<(&str, &str), usize> = BTreeMap::new();
    for &(file, rule, max) in ALLOW_LIST {
        let prev = allow.insert((file, rule), max);
        assert!(
            prev.is_none(),
            "duplicate allow-list entry: ({file}, {rule})"
        );
    }

    let mut violations = Vec::new();
    for path in crate_source_files(&root) {
        let rel = path
            .strip_prefix(&root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        if rel == "crates/anyagents-toolchain/src/legacy.rs"
            || rel.ends_with("/tests.rs")
            || rel.ends_with("_tests.rs")
        {
            continue;
        }
        let raw = std::fs::read_to_string(&path).unwrap();
        let text = non_test_source(&raw);

        let (env_path, anyagents_bin_env) = scan_var_calls(&text);
        let counts = [
            ("env_path", env_path),
            ("which_call", scan_which_calls(&text)),
            ("command_v", scan_command_v(&text)),
            ("anyagents_bin_env", anyagents_bin_env),
            ("bare_command_new", scan_bare_command_new(&text)),
        ];

        for (rule, count) in counts {
            if count == 0 {
                continue;
            }
            match allow.get(&(rel.as_str(), rule)) {
                Some(&max) if count <= max => {}
                Some(&max) => violations.push(format!(
                    "{rel} [{rule}]: found {count}, allow-list max is {max}"
                )),
                None => {
                    violations.push(format!("{rel} [{rule}]: found {count} (not allow-listed)"))
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "ADR 0005 purity violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn toolchain_crate_holds_no_credentials() {
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let cargo_toml = std::fs::read_to_string(crate_dir.join("Cargo.toml")).unwrap();
    for forbidden in ["anyagents-core", "credentials", "keyring", "secrecy"] {
        assert!(
            !cargo_toml.contains(forbidden),
            "anyagents-toolchain/Cargo.toml must not depend on {forbidden}"
        );
    }
    let mut files = Vec::new();
    collect_src_rs_files(&crate_dir.join("src"), &mut files);
    for path in files {
        let text = std::fs::read_to_string(&path).unwrap();
        for forbidden in ["CredentialStore", "op://", "op-otp://", "secret://"] {
            assert!(
                !text.contains(forbidden),
                "{} must not reference credential token {forbidden}",
                path.display()
            );
        }
    }
}

#[test]
#[ignore = "W3: delete anyagents-toolchain/src/legacy.rs once all streams migrate"]
fn legacy_module_deleted() {
    let legacy = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("legacy.rs");
    assert!(
        !legacy.exists(),
        "{} still exists; W3 must delete the legacy fallback module",
        legacy.display()
    );
}

#[test]
#[ignore = "W3: empty purity allow-list once all streams migrate"]
fn allow_list_empty() {
    assert!(
        ALLOW_LIST.is_empty(),
        "purity ALLOW_LIST still has {} entries: {ALLOW_LIST:?}",
        ALLOW_LIST.len()
    );
}
