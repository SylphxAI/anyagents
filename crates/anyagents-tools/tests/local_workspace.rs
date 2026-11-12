//! Real behavioral tests for `LocalWorkspace` (the `LocalHost` backend): every
//! assertion runs against a real `tempfile` directory and a real `/bin/sh`.
//! Zero mocks.

use std::path::PathBuf;

use anyagents_tools::{LocalWorkspace, WorkspaceBackend, base64_decode, base64_encode, sha256_hex};

fn workspace() -> (tempfile::TempDir, LocalWorkspace) {
    let dir = tempfile::tempdir().expect("tempdir");
    let ws = LocalWorkspace::new(dir.path().to_path_buf());
    (dir, ws)
}

#[test]
fn write_then_read_roundtrip_through_real_filesystem() {
    let (_dir, ws) = workspace();
    let content = b"hello workspace\nline two\n";
    let (resolved, total, sha) = ws
        .write_file_bytes(Some("nested/dir/greeting.txt"), content, false, false)
        .expect("write nested file");
    assert_eq!(total, content.len());
    assert_eq!(sha, sha256_hex(content));
    // The file really exists on disk, at the resolved location.
    let on_disk: PathBuf = resolved.clone().into();
    assert_eq!(std::fs::read(&on_disk).expect("read from disk"), content);

    let (resolved2, bytes) = ws
        .read_file_bytes(Some("nested/dir/greeting.txt"))
        .expect("read back");
    assert_eq!(resolved, resolved2);
    assert_eq!(bytes, content);
}

#[test]
fn overwrite_guard_append_and_overwrite() {
    let (_dir, ws) = workspace();
    ws.write_file_bytes(Some("f.txt"), b"one", false, false)
        .expect("first write");
    let err = ws
        .write_file_bytes(Some("f.txt"), b"two", false, false)
        .expect_err("second write without overwrite must fail");
    assert!(err.contains("already exists"), "unexpected error: {err}");

    let (_, total, _) = ws
        .write_file_bytes(Some("f.txt"), b"two", true, false)
        .expect("append");
    assert_eq!(total, 6);
    let (_, bytes) = ws
        .read_file_bytes(Some("f.txt"))
        .expect("read after append");
    assert_eq!(bytes, b"onetwo");

    let (_, total, _) = ws
        .write_file_bytes(Some("f.txt"), b"three", false, true)
        .expect("overwrite");
    assert_eq!(total, 5);
    let (_, bytes) = ws
        .read_file_bytes(Some("f.txt"))
        .expect("read after overwrite");
    assert_eq!(bytes, b"three");
}

#[test]
fn read_and_list_negative_edges() {
    let (_dir, ws) = workspace();
    std::fs::write(_dir.path().join("plain.txt"), b"x").expect("seed file");
    assert!(
        ws.read_file_bytes(Some("missing.txt")).is_err(),
        "missing file must error"
    );
    assert!(
        ws.read_file_bytes(Some("")).is_err(),
        "empty required path must error"
    );
    assert!(
        ws.read_file_bytes(None).is_err(),
        "required path None must error"
    );
    let err = ws.read_file_bytes(Some("")).expect_err("empty path");
    assert!(err.contains("must not be empty"), "unexpected error: {err}");

    std::fs::create_dir(_dir.path().join("adir")).expect("seed dir");
    assert!(
        ws.read_file_bytes(Some("adir")).is_err(),
        "reading a directory must error"
    );
    assert!(
        ws.list_dir(Some("no-such-dir"), false, 10).is_err(),
        "listing missing dir must error"
    );

    // Healthy list: sees the seeded file, hides dotfiles unless asked.
    std::fs::write(_dir.path().join(".hidden"), b"h").expect("seed hidden");
    let listing = ws.list_dir(None, false, 10).expect("list root");
    let names: Vec<&str> = listing
        .get("entries")
        .and_then(|e| e.as_array())
        .expect("list returns entries array")
        .iter()
        .filter_map(|e| e.get("name").and_then(|n| n.as_str()))
        .collect();
    assert!(
        names.contains(&"plain.txt"),
        "listing missing plain.txt: {listing}"
    );
    assert!(
        !names.contains(&".hidden"),
        "dotfile leaked with include_hidden=false"
    );
    let listing_with_hidden = ws.list_dir(None, true, 10).expect("list root with hidden");
    let names_h: Vec<&str> = listing_with_hidden
        .get("entries")
        .and_then(|e| e.as_array())
        .expect("list returns entries array")
        .iter()
        .filter_map(|e| e.get("name").and_then(|n| n.as_str()))
        .collect();
    assert!(
        names_h.contains(&".hidden"),
        "hidden file missing with include_hidden=true"
    );
}

#[test]
fn exec_runs_real_shell_and_reports_exit_codes() {
    let (_dir, ws) = workspace();
    let out = ws
        .exec("printf 'hello world'", None, None)
        .expect("exec printf");
    assert_eq!(out.exit_code, 0);
    assert_eq!(out.stdout, "hello world");

    let out = ws
        .exec("printf err >&2; exit 7", None, None)
        .expect("exec failing cmd");
    assert_eq!(out.exit_code, 7);
    assert_eq!(out.stderr, "err");

    assert!(
        ws.exec("   ", None, None).is_err(),
        "empty command must error"
    );
    assert!(
        ws.exec("true", Some("no-such-cwd"), None).is_err(),
        "missing cwd must error"
    );

    // exec in a subdirectory really runs there.
    std::fs::create_dir(_dir.path().join("sub")).expect("seed subdir");
    std::fs::write(_dir.path().join("sub/mark.txt"), b"here").expect("seed marker");
    let out = ws
        .exec("cat mark.txt", Some("sub"), None)
        .expect("exec with cwd");
    assert_eq!(out.stdout, "here");
}

#[test]
fn edit_and_patch_via_exec_roundtrip() {
    let (_dir, ws) = workspace();
    ws.write_file_bytes(Some("note.txt"), b"alpha beta gamma\n", false, false)
        .expect("seed note");
    // Patch: replace beta -> BETA in place with sed through the real shell.
    // `-i.bak` is the GNU/BSD-portable in-place form: macOS BSD sed requires
    // an explicit backup suffix for `-i`. The backup is removed right after.
    let out = ws
        .exec("sed -i.bak 's/beta/BETA/' note.txt && rm -f note.txt.bak", None, None)
        .expect("patch exec");
    assert_eq!(out.exit_code, 0);
    let (_, bytes) = ws.read_file_bytes(Some("note.txt")).expect("read patched");
    assert_eq!(bytes, b"alpha BETA gamma\n");
    // Append a line via shell redirect, then read the real file back.
    let out = ws
        .exec("printf 'delta\\n' >> note.txt", None, None)
        .expect("append exec");
    assert_eq!(out.exit_code, 0);
    let (_, bytes) = ws
        .read_file_bytes(Some("note.txt"))
        .expect("read after shell append");
    assert_eq!(bytes, b"alpha BETA gamma\ndelta\n");
}

#[test]
fn path_resolution_edges() {
    let (_dir, ws) = workspace();
    // Null bytes and empty-required are rejected outright.
    assert!(
        ws.read_file_bytes(Some("a\0b")).is_err(),
        "null byte path must error"
    );
    // `..` is normalized lexically against the root (no prefix clamp trickery).
    let (resolved, _) = {
        std::fs::write(_dir.path().join("up.txt"), b"up").expect("seed up");
        ws.read_file_bytes(Some("sub/../up.txt"))
            .expect("dotdot normalized")
    };
    assert!(
        resolved.ends_with("up.txt"),
        "unexpected resolution: {resolved}"
    );
}

#[test]
fn base64_public_api_roundtrip_and_legacy_cases() {
    // Encode matches the RFC 4648 vectors, decode round-trips all lengths.
    assert_eq!(base64_encode(b""), "");
    assert_eq!(base64_encode(b"f"), "Zg==");
    assert_eq!(base64_encode(b"fo"), "Zm8=");
    assert_eq!(base64_encode(b"foo"), "Zm9v");
    assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    for len in 0..=64usize {
        let data: Vec<u8> = (0..len as u8).collect();
        let enc = base64_encode(&data);
        assert_eq!(base64_decode(&enc).expect("decode own encoding"), data);
    }
    // Legacy leniency preserved: whitespace ignored, URL-safe accepted, mid-chunk
    // padding hole semantics kept ("Zg=Z" -> [0x66, 0x19]).
    assert_eq!(base64_decode("Zm9v\nYmFy").expect("whitespace"), b"foobar");
    assert_eq!(base64_decode("-w==").expect("url-safe"), [0xfb]);
    assert_eq!(base64_decode("Zg=Z").expect("mid-chunk pad"), [0x66, 0x19]);
    assert!(base64_decode("Zm9").is_err(), "bad length must error");
    assert!(
        base64_decode("####").is_err(),
        "invalid character must error"
    );
}
