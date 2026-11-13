//! ADR 0005 invariants 2, 3, 4 and 5 at the crate boundary.

use std::path::{Path, PathBuf};

use super::*;

/// A fake tool that prints `name version`.
fn fake_tool(dir: &Path, name: &str, version: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, format!("#!/bin/sh\necho \"{name} {version}\"\n")).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    path
}

fn manifest(entries: &[(&str, &Path, &str, String, &str)], extra: &str) -> String {
    let mut text = format!("schema = 1\nchild_path_tail = [\"/usr/bin\", \"/bin\"]\n{extra}\n");
    for (name, path, version, sha, flags) in entries {
        text.push_str(&format!(
            "[tool.{name}]\npath = \"{}\"\nversion = \"{version}\"\nsha256 = \"{sha}\"\n{flags}\n",
            path.display()
        ));
    }
    text
}

fn toolchain(text: &str) -> Toolchain {
    Toolchain::from_manifest_text(text, Path::new("/example.invalid/toolchain.toml"))
}

#[test]
fn a_declared_tool_resolves_to_its_verified_canonical_path() {
    let dir = tempfile::tempdir().unwrap();
    let tool = fake_tool(dir.path(), "faketool", "1.2.3");
    let sha = sha256_file(&tool).unwrap();
    let tc = toolchain(&manifest(
        &[("faketool", &tool, "1.2.3", sha, "enforce = true")],
        "",
    ));
    let resolved = tc.tool("faketool", "test").unwrap();
    assert_eq!(resolved.provenance, Provenance::Declared);
    assert_eq!(resolved.path, std::fs::canonicalize(&tool).unwrap());
    assert!(tc.report().legacy.is_empty());
}

#[test]
fn a_wrong_digest_is_a_checksum_mismatch_naming_the_tool() {
    let dir = tempfile::tempdir().unwrap();
    let tool = fake_tool(dir.path(), "faketool", "1.2.3");
    let tc = toolchain(&manifest(
        &[("faketool", &tool, "1.2.3", "a".repeat(64), "enforce = true")],
        "",
    ));
    let err = tc.tool("faketool", "test").unwrap_err();
    assert!(
        matches!(err, ToolchainError::ChecksumMismatch { ref tool, .. } if tool == "faketool"),
        "{err}"
    );
}

#[test]
fn a_wrong_version_is_a_version_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let tool = fake_tool(dir.path(), "faketool", "1.2.3");
    let sha = sha256_file(&tool).unwrap();
    let tc = toolchain(&manifest(
        &[("faketool", &tool, "9.9.9", sha, "enforce = true")],
        "",
    ));
    let err = tc.tool("faketool", "test").unwrap_err();
    assert!(
        matches!(err, ToolchainError::VersionMismatch { ref actual, .. } if actual.contains("1.2.3")),
        "{err}"
    );
}

#[test]
fn a_missing_declared_tool_is_missing_at_its_path() {
    let tc = toolchain(&manifest(
        &[(
            "adb",
            Path::new("/example.invalid/adb"),
            "35",
            "0".repeat(64),
            "enforce = true",
        )],
        "",
    ));
    let err = tc.tool("adb", "computer.mobile").unwrap_err();
    assert_eq!(err.to_string(), "adb: missing at /example.invalid/adb",);
}

#[test]
fn a_binary_swapped_after_start_is_caught_on_the_next_use() {
    let dir = tempfile::tempdir().unwrap();
    let tool = fake_tool(dir.path(), "faketool", "1.2.3");
    let sha = sha256_file(&tool).unwrap();
    let tc = toolchain(&manifest(
        &[("faketool", &tool, "1.2.3", sha, "enforce = true")],
        "",
    ));
    tc.tool("faketool", "test").unwrap();
    // Replace the file (new inode, new size): the fingerprint changes.
    let replacement = dir.path().join("replacement");
    std::fs::write(
        &replacement,
        "#!/bin/sh\necho \"faketool 1.2.3 tampered\"\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&replacement, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    std::fs::rename(&replacement, &tool).unwrap();
    let err = tc.tool("faketool", "test").unwrap_err();
    assert!(
        matches!(err, ToolchainError::ChecksumMismatch { .. }),
        "{err}"
    );
}

#[test]
fn report_mode_uses_the_declared_path_and_reports_the_failure() {
    let dir = tempfile::tempdir().unwrap();
    let tool = fake_tool(dir.path(), "faketool", "1.2.3");
    let tc = toolchain(&manifest(
        &[("faketool", &tool, "1.2.3", "b".repeat(64), "")],
        "",
    ));
    let resolved = tc.tool("faketool", "test").unwrap();
    assert_eq!(resolved.path, tool);
    assert!(matches!(
        resolved.provenance,
        Provenance::DeclaredUnverified { .. }
    ));
    let report = tc.report();
    assert!(matches!(
        report.tools[0].verified,
        Some(Err(ToolchainError::ChecksumMismatch { .. }))
    ));
}

#[test]
fn an_undeclared_tool_is_refused_when_undeclared_tools_are_enforced() {
    let tc = toolchain(&manifest(&[], "enforce_undeclared = true"));
    let err = tc.tool("adb", "computer.mobile").unwrap_err();
    assert_eq!(
        err,
        ToolchainError::Undeclared {
            tool: "adb".into(),
            needed_by: "computer.mobile".into()
        }
    );
    assert!(tc.report().legacy.is_empty(), "no ambient lookup happened");
}

#[test]
fn an_unreadable_manifest_fails_closed_and_names_the_manifest() {
    let tc = toolchain("schema = 1\n[tool.git]\npath = \"git\"\n");
    let err = tc.tool("git", "checkout").unwrap_err();
    assert!(
        matches!(err, ToolchainError::ManifestUnreadable { ref path, .. } if path.ends_with("toolchain.toml")),
        "{err}"
    );
    assert!(tc.report().manifest_error.is_some());
    assert!(tc.child_path().is_none());
}

#[test]
fn a_missing_manifest_file_is_legacy_and_reported() {
    let dir = tempfile::tempdir().unwrap();
    let tc = Toolchain::load(dir.path());
    let _ = tc.tool("anyagents-toolchain-test-no-such-tool", "test");
    let report = tc.report();
    assert!(report.manifest.is_none());
    assert!(!report.child_path_constructed);
    assert_eq!(report.legacy.len(), 1);
    assert_eq!(
        report.legacy[0].tool,
        "anyagents-toolchain-test-no-such-tool"
    );
    assert!(report.legacy[0].needed_by.contains("test"));
}

#[test]
fn without_a_manifest_the_exec_path_is_a_reported_legacy_use() {
    let tc = Toolchain::empty();
    let path = tc.exec_path("checkout admission");
    assert!(std::env::split_paths(&path).any(|d| d == Path::new("/usr/bin")));
    let report = tc.report();
    assert_eq!(report.legacy[0].tool, "PATH");
    assert!(report.legacy[0].needed_by.contains("checkout admission"));
}

#[test]
fn the_required_set_returns_every_failure() {
    let tc = toolchain(&manifest(&[], "enforce_undeclared = true"));
    let errs = tc
        .check_required(&[("adb", "computer.mobile"), ("gh", "github-watch")])
        .unwrap_err();
    let named: Vec<_> = errs.iter().map(ToString::to_string).collect();
    assert_eq!(named.len(), 2);
    assert!(
        named[0].starts_with("adb:") && named[1].starts_with("gh:"),
        "{named:?}"
    );
}

#[test]
fn the_child_path_is_the_bin_dir_then_the_declared_tail() {
    let tc = toolchain(&manifest(&[], ""))
        .with_bin_dir(PathBuf::from("/example.invalid/data/toolchain/bin"));
    let path = tc.child_path().unwrap();
    assert_eq!(path, "/example.invalid/data/toolchain/bin:/usr/bin:/bin");
}

#[cfg(unix)]
#[test]
fn exposed_tools_are_linked_and_stale_links_removed() {
    let dir = tempfile::tempdir().unwrap();
    let tool = fake_tool(dir.path(), "cargo", "1.91.0");
    let hidden = fake_tool(dir.path(), "hidden", "1");
    let sha = sha256_file(&tool).unwrap();
    let hidden_sha = sha256_file(&hidden).unwrap();
    let bin = dir.path().join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    std::os::unix::fs::symlink("/example.invalid/old", bin.join("stale")).unwrap();
    std::fs::write(bin.join("operator-file"), "kept").unwrap();
    let tc = toolchain(&manifest(
        &[
            ("cargo", &tool, "1.91.0", sha, "expose = true"),
            ("hidden", &hidden, "1", hidden_sha, ""),
        ],
        "",
    ))
    .with_bin_dir(bin.clone());
    assert!(tc.sync_exposed().unwrap().is_empty());
    assert_eq!(
        std::fs::read_link(bin.join("cargo")).unwrap(),
        std::fs::canonicalize(&tool).unwrap()
    );
    assert!(!bin.join("hidden").exists());
    assert!(std::fs::symlink_metadata(bin.join("stale")).is_err());
    assert!(bin.join("operator-file").exists());
    let out = std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg("cargo")
        .env("PATH", tc.child_path().unwrap())
        .output()
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "cargo 1.91.0");
}
