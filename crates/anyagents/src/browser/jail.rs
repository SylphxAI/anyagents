//! `anyagents internal browser-jail [--require] <dir>… -- <program> <args…>`:
//! start Chromium inside a Landlock filesystem domain that may write only
//! the given directories (its profile and its private temporary directory).
//!
//! Chromium runs as the server's user with `--no-sandbox` (this host allows
//! no user namespaces), and its DevTools port listens on 127.0.0.1, where
//! every seat on the host can reach it. A URL check in the server therefore
//! cannot keep host files from a page or a DevTools client. This does: the
//! kernel lets Chromium and everything it starts read and run only the
//! system's programs, libraries and configuration, and read and write only
//! its own profile directory. The server's data directory, secrets, state
//! database and the server user's home stay closed to it whatever a page, a
//! renderer exploit or a raw DevTools client asks for (ADR 0010 D3).
//!
//! Without Landlock (another OS, an old kernel) the helper refuses when
//! `--require` is given, which the server passes whenever OS seats are
//! configured; otherwise it starts Chromium as before.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

/// Directories Chromium may read and execute from.
const SYSTEM_READ_EXECUTE: &[&str] = &[
    "/usr", "/lib", "/lib32", "/lib64", "/libx32", "/bin", "/sbin", "/etc", "/opt",
];
/// Directories Chromium may read.
const SYSTEM_READ: &[&str] = &[
    "/sys",
    "/var/cache/fontconfig",
    "/var/lib/dbus",
    "/run/dbus",
];

pub(crate) fn main(args: &[OsString]) -> std::process::ExitCode {
    let usage = || {
        eprintln!("usage: anyagents internal browser-jail [--require] <dir>… -- <program> [args…]");
        std::process::ExitCode::from(2)
    };
    let (require, rest) = match args.first().and_then(|a| a.to_str()) {
        Some("--require") => (true, &args[1..]),
        _ => (false, args),
    };
    let Some(separator) = rest.iter().position(|arg| arg == "--") else {
        return usage();
    };
    let (writable, [program, program_args @ ..]) = (&rest[..separator], &rest[separator + 1..])
    else {
        return usage();
    };
    let writable: Vec<PathBuf> = writable.iter().map(PathBuf::from).collect();
    if writable.is_empty() || !writable.iter().all(|dir| dir.is_absolute()) {
        return usage();
    }
    match confine(&writable, Path::new(program)) {
        Ok(()) => {}
        Err(error) if require || error.kind() != std::io::ErrorKind::Unsupported => {
            eprintln!("browser-jail: refusing to start Chromium unconfined: {error}");
            return std::process::ExitCode::FAILURE;
        }
        Err(error) => eprintln!("browser-jail: Chromium runs unconfined: {error}"),
    }
    use std::os::unix::process::CommandExt as _;
    let error = std::process::Command::new(program)
        .args(program_args)
        .exec();
    eprintln!(
        "browser-jail: cannot start {}: {error}",
        Path::new(program).display()
    );
    std::process::ExitCode::FAILURE
}

/// The paths the jail grants, as `(path, read-write)`; only existing ones
/// are added.
fn grants(writable: &[PathBuf], program: &Path) -> Vec<(PathBuf, Access)> {
    let mut out: Vec<(PathBuf, Access)> = SYSTEM_READ_EXECUTE
        .iter()
        .map(|p| (PathBuf::from(p), Access::ReadExecute))
        .chain(SYSTEM_READ.iter().map(|p| (PathBuf::from(p), Access::Read)))
        .collect();
    // A Chromium installed outside the system directories (a Playwright
    // download, an `ANYAGENTS_CHROMIUM_BIN`): its own directory only.
    for program in [
        Some(program.to_path_buf()),
        std::fs::canonicalize(program).ok(),
    ]
    .into_iter()
    .flatten()
    {
        if let Some(dir) = program.parent() {
            out.push((dir.to_path_buf(), Access::ReadExecute));
        }
    }
    // Host directories an operator adds for this host's own tooling.
    if let Some(extra) = std::env::var_os("ANYAGENTS_BROWSER_JAIL_READ") {
        out.extend(
            std::env::split_paths(&extra)
                .filter(|p| p.is_absolute())
                .map(|p| (p, Access::ReadExecute)),
        );
    }
    out.push((PathBuf::from("/proc"), Access::Proc));
    out.push((PathBuf::from("/dev"), Access::Devices));
    out.push((PathBuf::from("/dev/shm"), Access::ReadWrite));
    out.extend(writable.iter().map(|dir| (dir.clone(), Access::ReadWrite)));
    out
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Access {
    Read,
    ReadExecute,
    /// Read, and write existing files (`oom_score_adj` and the like).
    Proc,
    /// Read and write existing device nodes (`/dev/null`, `/dev/urandom`).
    Devices,
    ReadWrite,
}

#[cfg(target_os = "linux")]
fn confine(writable: &[PathBuf], program: &Path) -> std::io::Result<()> {
    landlock::restrict(&grants(writable, program))
}

#[cfg(not(target_os = "linux"))]
fn confine(_writable: &[PathBuf], _program: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "Landlock is Linux only",
    ))
}

#[cfg(target_os = "linux")]
mod landlock {
    use super::Access;
    use std::io;
    use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
    use std::os::unix::ffi::OsStrExt as _;
    use std::path::PathBuf;

    const EXECUTE: u64 = 1 << 0;
    const WRITE_FILE: u64 = 1 << 1;
    const READ_FILE: u64 = 1 << 2;
    const READ_DIR: u64 = 1 << 3;
    /// Every right of ABI 1: remove, make (char, dir, reg, sock, fifo, block, sym).
    const ABI1_ALL: u64 = (1 << 13) - 1;
    const REFER: u64 = 1 << 13;
    const TRUNCATE: u64 = 1 << 14;
    const CREATE_RULESET_VERSION: u32 = 1;
    const RULE_PATH_BENEATH: libc::c_int = 1;

    #[repr(C)]
    struct RulesetAttr {
        handled_access_fs: u64,
    }

    #[repr(C, packed)]
    struct PathBeneathAttr {
        allowed_access: u64,
        parent_fd: i32,
    }

    fn check(rc: libc::c_long) -> io::Result<libc::c_long> {
        if rc < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(rc)
        }
    }

    /// The rights this kernel's Landlock ABI knows; the ones a rule may
    /// grant and the ruleset handles.
    fn handled() -> io::Result<u64> {
        // SAFETY: a version query takes no attribute.
        let abi = unsafe {
            libc::syscall(
                libc::SYS_landlock_create_ruleset,
                std::ptr::null::<RulesetAttr>(),
                0usize,
                CREATE_RULESET_VERSION,
            )
        };
        if abi < 1 {
            let error = io::Error::last_os_error();
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("Landlock is unavailable: {error}"),
            ));
        }
        let mut rights = ABI1_ALL;
        if abi >= 2 {
            rights |= REFER;
        }
        if abi >= 3 {
            rights |= TRUNCATE;
        }
        Ok(rights)
    }

    pub(super) fn restrict(grants: &[(PathBuf, Access)]) -> io::Result<()> {
        let handled = handled()?;
        let attr = RulesetAttr {
            handled_access_fs: handled,
        };
        // SAFETY: a valid attribute of the size passed.
        let ruleset = check(unsafe {
            libc::syscall(
                libc::SYS_landlock_create_ruleset,
                &attr as *const RulesetAttr,
                std::mem::size_of::<RulesetAttr>(),
                0u32,
            )
        })?;
        // SAFETY: landlock_create_ruleset returned a new descriptor.
        let ruleset = unsafe { OwnedFd::from_raw_fd(ruleset as libc::c_int) };
        for (path, access) in grants {
            let wanted = match access {
                Access::Read => READ_FILE | READ_DIR,
                Access::ReadExecute => EXECUTE | READ_FILE | READ_DIR,
                Access::Proc | Access::Devices => READ_FILE | READ_DIR | WRITE_FILE,
                Access::ReadWrite => handled,
            } & handled;
            let Ok(name) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
                continue;
            };
            // SAFETY: a NUL-terminated path.
            let fd = unsafe { libc::open(name.as_ptr(), libc::O_PATH | libc::O_CLOEXEC) };
            if fd < 0 {
                // Not on this host: nothing to grant.
                continue;
            }
            // SAFETY: open returned a new descriptor.
            let parent = unsafe { OwnedFd::from_raw_fd(fd) };
            let rule = PathBeneathAttr {
                allowed_access: wanted,
                parent_fd: parent.as_raw_fd(),
            };
            // SAFETY: a ruleset descriptor and a valid rule attribute.
            check(unsafe {
                libc::syscall(
                    libc::SYS_landlock_add_rule,
                    ruleset.as_raw_fd(),
                    RULE_PATH_BENEATH,
                    &rule as *const PathBeneathAttr,
                    0u32,
                )
            })
            .map_err(|error| {
                io::Error::new(error.kind(), format!("grant {}: {error}", path.display()))
            })?;
        }
        // SAFETY: prctl with constant arguments.
        if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a ruleset descriptor and no flags.
        check(unsafe {
            libc::syscall(libc::SYS_landlock_restrict_self, ruleset.as_raw_fd(), 0u32)
        })?;
        Ok(())
    }
}

/// The program and arguments that start `program args` inside the jail for
/// `profile`: this binary's `internal browser-jail`, or the test harness's
/// worker under `cargo test`.
pub(crate) fn wrap(
    writable: &[&Path],
    require: bool,
    program: &Path,
) -> std::io::Result<(PathBuf, Vec<OsString>)> {
    let (helper, mut args) = helper()?;
    if require {
        args.push("--require".into());
    }
    args.extend(writable.iter().map(|dir| dir.as_os_str().to_owned()));
    args.push("--".into());
    args.push(real_executable(program).into_os_string());
    Ok((helper, args))
}

/// The Chromium executable behind a distribution's shell launcher, which
/// runs host tools (`grep`, `find`, `findmnt`) that a jail cannot predict:
/// Debian's `/usr/bin/chromium` runs `/usr/lib/chromium/chromium`, Google's
/// `google-chrome` runs the `chrome` beside it. Anything else is started as
/// given.
fn real_executable(program: &Path) -> PathBuf {
    let Ok(resolved) = std::fs::canonicalize(program) else {
        return program.to_path_buf();
    };
    let is_script = std::fs::File::open(&resolved)
        .and_then(|mut file| {
            let mut magic = [0u8; 2];
            std::io::Read::read_exact(&mut file, &mut magic).map(|()| magic == *b"#!")
        })
        .unwrap_or(false);
    if !is_script {
        return resolved;
    }
    let name = resolved.file_name().unwrap_or_default();
    [
        resolved.with_file_name("chrome"),
        Path::new("/usr/lib").join(name).join(name),
    ]
    .into_iter()
    .find(|candidate| {
        std::fs::File::open(candidate)
            .and_then(|mut file| {
                let mut magic = [0u8; 4];
                std::io::Read::read_exact(&mut file, &mut magic).map(|()| magic == *b"\x7fELF")
            })
            .unwrap_or(false)
    })
    .unwrap_or(resolved)
}

#[cfg(not(test))]
fn helper() -> std::io::Result<(PathBuf, Vec<OsString>)> {
    Ok((
        crate::data::current_exe_clean()?,
        vec!["internal".into(), "browser-jail".into()],
    ))
}

#[cfg(test)]
fn helper() -> std::io::Result<(PathBuf, Vec<OsString>)> {
    Ok((
        std::env::current_exe()?,
        [
            "--exact",
            "browser::jail::tests::browser_jail_worker",
            "--nocapture",
            "--",
            "browser-jail",
        ]
        .into_iter()
        .map(OsString::from)
        .collect(),
    ))
}

/// This kernel's Landlock ABI version, 0 when it has none.
#[cfg(test)]
pub(crate) fn landlock_abi() -> i64 {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: a version query takes no attribute.
        unsafe {
            libc::syscall(
                libc::SYS_landlock_create_ruleset,
                std::ptr::null::<u8>(),
                0usize,
                1u32,
            )
        }
        .max(0)
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The helper under `cargo test`: the test harness stands in for this
    /// binary's `internal browser-jail`.
    #[test]
    fn browser_jail_worker() {
        let args: Vec<OsString> = std::env::args_os().collect();
        let Some(start) = args.iter().position(|arg| arg == "browser-jail") else {
            return;
        };
        let code = main(&args[start + 1..]);
        std::process::exit(if code == std::process::ExitCode::SUCCESS {
            0
        } else {
            1
        });
    }

    /// Inside the jail a program reads the system and its own profile, and
    /// nothing else this user owns: not a sibling secret, not through a
    /// link out of the profile.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_jail_opens_the_system_and_the_profile_only() {
        if super::landlock_abi() < 1 {
            eprintln!("Landlock unavailable; jail acceptance not executed");
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(temp.path()).unwrap();
        let profile = root.join("profile");
        std::fs::create_dir_all(&profile).unwrap();
        std::fs::write(profile.join("own"), b"own").unwrap();
        std::fs::write(root.join("operator-token"), b"operator-secret").unwrap();
        std::os::unix::fs::symlink(root.join("operator-token"), profile.join("link")).unwrap();
        let (helper, mut args) = wrap(&[&profile], true, Path::new("/bin/sh")).unwrap();
        args.extend(
            [
                "-c",
                "cat \"$1/own\" && ! cat \"$2\" 2>/dev/null && ! cat \"$1/link\" 2>/dev/null \
                 && echo made > \"$1/made\" && ! echo x > \"$3\" 2>/dev/null && cat /etc/hostname >/dev/null",
                "sh",
            ]
            .map(OsString::from),
        );
        args.push(profile.clone().into());
        args.push(root.join("operator-token").into());
        args.push(root.join("planted").into());
        let out = std::process::Command::new(helper)
            .args(args)
            .stdin(std::process::Stdio::null())
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(out.status.success(), "{stderr}");
        assert!(String::from_utf8_lossy(&out.stdout).contains("own"));
        assert!(!String::from_utf8_lossy(&out.stdout).contains("operator-secret"));
        assert_eq!(std::fs::read(profile.join("made")).unwrap(), b"made\n");
        assert!(!root.join("planted").exists());
    }

    #[test]
    fn a_malformed_jail_call_runs_nothing() {
        for args in [
            vec![],
            vec!["relative", "--", "/bin/true"],
            vec!["/abs", "-x", "/bin/true"],
            vec!["--", "/bin/true"],
            vec!["/abs", "--"],
        ] {
            let args: Vec<OsString> = args.into_iter().map(OsString::from).collect();
            assert_eq!(main(&args), std::process::ExitCode::from(2));
        }
    }
}
