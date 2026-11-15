//! Privileged acceptance for config sharing; no credentials or host user data.
use std::ffi::CString;
use std::fs::{self, File};
use std::os::fd::AsFd as _;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::Path;
use std::process::Command;

const SEAT: u32 = 24101;
const GROUP: u32 = 24102;
const PEER: u32 = 24103;
const PEER_GROUP: u32 = 24104;

fn owned(path: &Path, uid: u32, gid: u32, mode: u32) {
    let name = CString::new(path.as_os_str().as_bytes()).unwrap();
    assert_eq!(
        unsafe { libc::chown(name.as_ptr(), uid, gid) },
        0,
        "chown {} to {uid}:{gid}: {}",
        path.display(),
        std::io::Error::last_os_error()
    );
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
}

fn seat_command(uid: u32, gid: u32, script: &str, path: &Path) -> std::process::Output {
    Command::new("/usr/bin/setpriv")
        .env("LC_ALL", "C")
        .args([
            "--reuid",
            &uid.to_string(),
            "--regid",
            &gid.to_string(),
            "--clear-groups",
            "--no-new-privs",
            "--",
            "/bin/sh",
            "-c",
            script,
            "fixture",
        ])
        .arg(path)
        .output()
        .unwrap()
}

fn allowed(uid: u32, gid: u32, script: &str, path: &Path) {
    let output = seat_command(uid, gid, script, path);
    assert!(
        output.status.success(),
        "{}: {}",
        path.display(),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn denied(uid: u32, gid: u32, path: &Path) {
    let output = seat_command(uid, gid, r#"cat -- "$1""#, path);
    assert!(
        !output.status.success(),
        "unexpected read of {}",
        path.display()
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("Permission denied"),
        "{:?}",
        output
    );
}

#[test]
#[ignore = "requires the explicit privileged config ACL CI lane"]
fn privileged_config_acl_cross_uid() {
    assert_eq!(
        std::env::var("ANYAGENTS_REQUIRE_CONFIG_ACL_TEST").as_deref(),
        Ok("1")
    );
    assert_eq!(
        crate::sandbox::server_uid(),
        0,
        "root fixture executor required"
    );
    let status = fs::read_to_string("/proc/self/status").unwrap();
    let caps = status
        .lines()
        .find_map(|line| line.strip_prefix("CapEff:\t"))
        .map(|raw| u64::from_str_radix(raw, 16).unwrap())
        .unwrap();
    assert_eq!(
        caps & ((1 << 1) | (1 << 2)),
        0,
        "drop DAC override/read-search; otherwise access checks are vacuous"
    );
    assert_eq!(
        caps & ((1 << 0) | (1 << 6) | (1 << 7)),
        (1 << 0) | (1 << 6) | (1 << 7)
    );
    let _lock = crate::server::tests::env_lock();
    let previous = std::env::var_os("ANYAGENTS_LAUNCHER");
    struct Reset(Option<std::ffi::OsString>);
    impl Drop for Reset {
        fn drop(&mut self) {
            unsafe {
                match self.0.take() {
                    Some(value) => std::env::set_var("ANYAGENTS_LAUNCHER", value),
                    None => std::env::remove_var("ANYAGENTS_LAUNCHER"),
                }
            }
        }
    }
    let _reset = Reset(previous);
    unsafe { std::env::set_var("ANYAGENTS_LAUNCHER", "/usr/bin/setpriv") };
    let fixture = tempfile::Builder::new()
        .prefix("config-acl-")
        .tempdir_in("/var/tmp")
        .unwrap();
    owned(fixture.path(), 0, 0, 0o711);
    let root = fixture.path().join("config");
    // Populate as root before handing ownership to the synthetic seat.
    let dirs = [
        "",
        "profiles",
        "brain",
        "brain/task",
        "brain/task/.system_generated",
        "brain/task/.system_generated/steps",
        "brain/task/.system_generated/logs",
        "data-home",
        "data-home/opencode",
        "state-home",
        "state-home/opencode",
        ".config",
        ".config/op",
        "config-home",
        "config-home/op",
    ];
    for dir in dirs {
        fs::create_dir_all(root.join(dir)).unwrap();
    }
    let files = [
        "profiles/node",
        "brain/task/.system_generated/steps/step.json",
        "brain/task/.system_generated/logs/run.log",
        "data-home/opencode/auth.json",
        "state-home/opencode/auth.json",
        ".config/op/token",
    ];
    for file in files {
        fs::write(root.join(file), b"synthetic").unwrap();
    }
    fs::write(root.join("profiles/node"), b"#!/bin/sh\nexit 0\n").unwrap();
    fs::write(root.join("mcp-capability"), b"synthetic-capability").unwrap();
    owned(&root.join("mcp-capability"), 0, 0, 0o600);
    // Seed an inherited daemon grant on the private op subtree. Sharing must
    // remove BOTH access and default ACLs, not just chmod today's files.
    let op = root.join(".config/op");
    super::config_acl(File::open(&op).unwrap().as_fd(), SEAT, 0, 0o2770, true).unwrap();
    let daemon_op = root.join("config-home/op");
    super::config_acl(
        File::open(&daemon_op).unwrap().as_fd(),
        SEAT,
        0,
        0o2770,
        true,
    )
    .unwrap();
    struct Cleanup(Vec<std::path::PathBuf>);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            for path in &self.0 {
                let name = CString::new(path.as_os_str().as_bytes()).unwrap();
                unsafe {
                    libc::chown(name.as_ptr(), 0, 0);
                    libc::chmod(name.as_ptr(), 0o700);
                }
            }
        }
    }
    let _cleanup = Cleanup(dirs.iter().map(|dir| root.join(dir)).collect());
    for file in files {
        owned(
            &root.join(file),
            SEAT,
            GROUP,
            if file == "profiles/node" {
                0o700
            } else {
                0o600
            },
        );
    }
    // Establish the daemon-private leaf before its ancestors become seat-only.
    // This fixture deliberately lacks DAC override/search capabilities.
    owned(&daemon_op, 0, 0, 0o700);
    for dir in dirs
        .into_iter()
        .rev()
        .filter(|dir| *dir != "config-home/op")
    {
        owned(
            &root.join(dir),
            SEAT,
            GROUP,
            if dir == "brain" { 0o1700 } else { 0o700 },
        );
    }
    assert_eq!(
        File::open(&root).unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied,
        "seat-owned 0700 config must actually deny the daemon before repair"
    );
    super::share_config(&root, SEAT, GROUP).unwrap();
    super::share_config(&root, SEAT, GROUP).unwrap(); // idempotence
    for file in &files[..5] {
        let path = root.join(file);
        assert_eq!(fs::metadata(&path).unwrap().uid(), SEAT);
        assert!(
            File::open(&path).is_ok(),
            "daemon cannot read {}",
            path.display()
        );
        allowed(SEAT, GROUP, r#"test -r "$1" && test -w "$1""#, &path);
        denied(PEER, PEER_GROUP, &path);
    }
    allowed(SEAT, GROUP, r#""$1""#, &root.join("profiles/node"));
    let generated = root.join("brain/task/.system_generated/steps/future.json");
    allowed(
        SEAT,
        GROUP,
        r#"umask 077; printf future > "$1""#,
        &generated,
    );
    assert_eq!(fs::read(&generated).unwrap(), b"future");
    fs::write(&generated, b"daemon-update").unwrap();
    denied(PEER, PEER_GROUP, &generated);
    let later = root.join("data-home/opencode/later.json");
    allowed(
        SEAT,
        GROUP,
        r#"printf later > "$1"; chmod 600 "$1""#,
        &later,
    );
    assert_eq!(
        File::open(&later).unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    super::share_config(&root, SEAT, GROUP).unwrap();
    assert_eq!(fs::read(&later).unwrap(), b"later");
    denied(PEER, PEER_GROUP, &later);
    let capability = root.join("mcp-capability");
    assert_eq!(fs::read(&capability).unwrap(), b"synthetic-capability");
    allowed(
        SEAT,
        GROUP,
        r#"test -r "$1" && test ! -w "$1""#,
        &capability,
    );
    assert!(
        !seat_command(SEAT, GROUP, r#"printf forged >> "$1""#, &capability)
            .status
            .success()
    );
    assert_eq!(fs::read(&capability).unwrap(), b"synthetic-capability");
    denied(PEER, PEER_GROUP, &capability);
    let private = op.join("token");
    denied(0, 0, &private);
    denied(PEER, PEER_GROUP, &private);
    let future = op.join("future-token");
    // Deliberately use a permissive mode: a stale default ACL would expose it.
    allowed(SEAT, GROUP, r#"umask 000; printf private > "$1""#, &future);
    denied(0, 0, &future);
    denied(PEER, PEER_GROUP, &future);
    let daemon_future = daemon_op.join("future-token");
    fs::write(&daemon_future, b"daemon-private").unwrap();
    denied(SEAT, GROUP, &daemon_future);
    denied(PEER, PEER_GROUP, &daemon_future);
    // Inspect ACL metadata too: parent 0700 alone can hide a stale inheritance leak.
    for check in [&op, &daemon_op] {
        let name = CString::new(check.as_os_str().as_bytes()).unwrap();
        let size = unsafe {
            libc::getxattr(
                name.as_ptr(),
                c"system.posix_acl_default".as_ptr(),
                std::ptr::null_mut(),
                0,
            )
        };
        assert_eq!(size, -1, "private directory retained a default ACL");
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ENODATA)
        );
    }
    eprintln!("CONFIG_ACL_PRIVILEGED_ACCEPTANCE_EXECUTED");
}
