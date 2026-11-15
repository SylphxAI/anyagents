//! The machine a harness is installed for, and the template words that name it.

use crate::InstallError;

/// One operating system and processor pair, named the ways publishers name it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Platform {
    /// `linux` or `darwin`, as npm and Node name it.
    pub os: &'static str,
    /// `x64` or `arm64`, as npm and Node name it.
    pub cpu: &'static str,
    /// `x86_64` or `aarch64`, as the kernel names it.
    pub arch: &'static str,
    /// The Rust target triple a statically linked release is built for.
    pub target: &'static str,
}

impl Platform {
    /// The platform this binary was built for, or a refusal for one no
    /// publisher in the catalog ships to.
    pub fn host() -> Result<Self, InstallError> {
        Self::from_parts(std::env::consts::OS, std::env::consts::ARCH)
    }

    /// The platform for a Rust `target_os` and `target_arch` pair.
    pub fn from_parts(os: &str, arch: &str) -> Result<Self, InstallError> {
        Ok(match (os, arch) {
            ("linux", "x86_64") => Self {
                os: "linux",
                cpu: "x64",
                arch: "x86_64",
                target: "x86_64-unknown-linux-musl",
            },
            ("linux", "aarch64") => Self {
                os: "linux",
                cpu: "arm64",
                arch: "aarch64",
                target: "aarch64-unknown-linux-musl",
            },
            ("macos", "x86_64") => Self {
                os: "darwin",
                cpu: "x64",
                arch: "x86_64",
                target: "x86_64-apple-darwin",
            },
            ("macos", "aarch64") => Self {
                os: "darwin",
                cpu: "arm64",
                arch: "aarch64",
                target: "aarch64-apple-darwin",
            },
            _ => {
                return Err(InstallError::UnsupportedPlatform(format!("{os}-{arch}")));
            }
        })
    }

    /// The key per-platform tables are indexed by: `<os>-<cpu>`, e.g. `linux-x64`.
    pub fn key(&self) -> String {
        format!("{}-{}", self.os, self.cpu)
    }

    /// Replace `{os}`, `{cpu}`, `{arch}`, `{target}` and `{version}` in `template`.
    pub fn expand(&self, template: &str, version: &str) -> String {
        template
            .replace("{os}", self.os)
            .replace("{cpu}", self.cpu)
            .replace("{arch}", self.arch)
            .replace("{target}", self.target)
            .replace("{version}", version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn templates_name_the_platform_the_way_each_publisher_does() {
        let linux = Platform::from_parts("linux", "x86_64").unwrap();
        assert_eq!(linux.key(), "linux-x64");
        assert_eq!(
            linux.expand("pkg-{os}-{cpu}/vendor/{target}/{arch}@{version}", "1.2.3"),
            "pkg-linux-x64/vendor/x86_64-unknown-linux-musl/x86_64@1.2.3"
        );
        let mac = Platform::from_parts("macos", "aarch64").unwrap();
        assert_eq!(mac.key(), "darwin-arm64");
    }

    #[test]
    fn an_unshipped_platform_is_refused() {
        assert_eq!(
            Platform::from_parts("windows", "x86_64").unwrap_err(),
            InstallError::UnsupportedPlatform("windows-x86_64".to_string())
        );
    }
}
