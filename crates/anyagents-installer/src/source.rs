//! Where a managed binary comes from, and how its latest release and the
//! digest that release must hash to are discovered.
//!
//! Every source names a publisher-controlled digest: npm's `dist.integrity`, a
//! checksum file published beside a release, a vendor manifest's per-platform
//! checksum, or a `{version, sha256}` pinned in caller configuration. A source
//! that can name none of these is refused as [`InstallError::UnverifiableSource`].

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::InstallError;
use crate::fetch::{Fetch, METADATA_LIMIT};
use crate::platform::Platform;
use crate::unpack::Format;
use crate::verify::{Digest, checksum_for};

/// The public npm registry, used when an npm source names none.
pub const NPM_REGISTRY: &str = "https://registry.npmjs.org";
/// The public GitHub API and download hosts, used when a release source names none.
pub const GITHUB_API: &str = "https://api.github.com";
pub const GITHUB_DOWNLOAD: &str = "https://github.com";

/// One artifact of a pinned or vendor-manifest source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinnedArtifact {
    pub url: String,
    pub sha256: String,
}

/// Where a managed binary comes from.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Source {
    /// An npm package whose tarball carries no dependencies. With
    /// `platform_package`, the tarball is the per-platform package (a template
    /// in [`Platform::expand`] words) that the main package lists in
    /// `optionalDependencies`, either directly or as an `npm:` alias. The
    /// tarball is unpacked to `node_modules/<package name>/`, the same layout a
    /// private-prefix `npm install` produces.
    Npm {
        package: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        platform_package: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        registry: Option<String>,
    },
    /// An npm package with dependencies. The managed `node` runtime's own
    /// `npm` installs it into the staging prefix with lifecycle scripts off;
    /// npm checks every package against its registry `dist.integrity`.
    NpmTree {
        package: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        registry: Option<String>,
    },
    /// The latest GitHub release of `repo`, whose tag is `tag_prefix` +
    /// version. `assets` maps a platform key to the asset name, and `checksums`
    /// names the `sha256sum`-format file published in the same release.
    GithubRelease {
        repo: String,
        #[serde(default)]
        tag_prefix: String,
        assets: BTreeMap<String, String>,
        checksums: String,
        #[serde(default)]
        format: Format,
        #[serde(default)]
        strip: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        api: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        download: Option<String>,
    },
    /// A JSON document at `url`:
    /// `{"version": "...", "platforms": {"<key>": {"url": "...", "sha256": "..."}}}`.
    VendorManifest {
        url: String,
        #[serde(default)]
        format: Format,
        #[serde(default)]
        strip: usize,
    },
    /// One exact version, with a sha256 per platform. It never tracks latest.
    Pinned {
        version: String,
        artifacts: BTreeMap<String, PinnedArtifact>,
        #[serde(default)]
        format: Format,
        #[serde(default)]
        strip: usize,
    },
    /// A single executable already on this machine, adopted by exact sha256.
    /// For a harness whose publisher offers no verifiable source yet; the
    /// deployment declares the path and digest, and it is copied in.
    Local {
        version: String,
        path: PathBuf,
        sha256: String,
    },
    /// The harness runs inside this binary; there is nothing to install.
    Builtin,
    /// No verifiable source is known. Caller configuration must supply one.
    Unconfigured,
}

/// What one release is: its version and the artifact that carries it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Release {
    pub version: String,
    pub artifact: Artifact,
}

/// How a release's bytes are obtained and checked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Artifact {
    /// Download `url`, check `digest`, unpack into `into` (relative to the version dir).
    Download {
        url: String,
        digest: Digest,
        format: Format,
        strip: usize,
        into: String,
    },
    /// Copy `path`, check `sha256`, place it at the launcher path.
    Local { path: PathBuf, digest: Digest },
    /// Install `spec` with the managed npm; `digest` is the top-level tarball's integrity.
    NpmTree {
        spec: String,
        registry: String,
        digest: Digest,
    },
}

impl Artifact {
    pub fn digest(&self) -> &Digest {
        match self {
            Self::Download { digest, .. }
            | Self::Local { digest, .. }
            | Self::NpmTree { digest, .. } => digest,
        }
    }
}

impl Source {
    /// A short label recorded in receipts.
    pub fn label(&self) -> String {
        match self {
            Self::Npm { package, .. } => format!("npm:{package}"),
            Self::NpmTree { package, .. } => format!("npm-tree:{package}"),
            Self::GithubRelease { repo, .. } => format!("github-release:{repo}"),
            Self::VendorManifest { url, .. } => format!("vendor-manifest:{url}"),
            Self::Pinned { .. } => "pinned".to_string(),
            Self::Local { path, .. } => format!("local:{}", path.display()),
            Self::Builtin => "builtin".to_string(),
            Self::Unconfigured => "unconfigured".to_string(),
        }
    }

    /// Whether this source installs anything at all.
    pub fn is_managed(&self) -> bool {
        !matches!(self, Self::Builtin)
    }
}

fn unverifiable(id: &str, reason: impl Into<String>) -> InstallError {
    InstallError::UnverifiableSource {
        harness: id.to_string(),
        reason: reason.into(),
    }
}

fn json(fetch: &dyn Fetch, url: &str) -> Result<serde_json::Value, InstallError> {
    let bytes = fetch.get(url, METADATA_LIMIT)?;
    serde_json::from_slice(&bytes).map_err(|err| InstallError::Fetch {
        url: url.to_string(),
        reason: format!("not JSON: {err}"),
    })
}

fn text_field<'a>(
    value: &'a serde_json::Value,
    url: &str,
    field: &str,
) -> Result<&'a str, InstallError> {
    value
        .pointer(field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| InstallError::Fetch {
            url: url.to_string(),
            reason: format!("missing {field}"),
        })
}

fn sha256(id: &str, hex: &str) -> Result<Digest, InstallError> {
    Digest::sha256_hex(hex).ok_or_else(|| unverifiable(id, format!("not a sha256: {hex:?}")))
}

/// A version string becomes a directory name, so it must be one plain component.
pub fn check_version(id: &str, version: &str) -> Result<(), InstallError> {
    let ok = !version.is_empty()
        && version != "."
        && version != ".."
        && !version.starts_with('.')
        && version
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '+'));
    if ok {
        Ok(())
    } else {
        Err(unverifiable(
            id,
            format!("unusable version string {version:?}"),
        ))
    }
}

/// Discover the release `source` currently publishes for `platform`.
pub fn resolve(
    id: &str,
    source: &Source,
    platform: &Platform,
    fetch: &dyn Fetch,
) -> Result<Release, InstallError> {
    let release = match source {
        Source::Npm {
            package,
            platform_package,
            registry,
        } => {
            let registry = registry
                .as_deref()
                .unwrap_or(NPM_REGISTRY)
                .trim_end_matches('/');
            let latest_url = format!("{registry}/{package}/latest");
            let latest = json(fetch, &latest_url)?;
            let version = text_field(&latest, &latest_url, "/version")?.to_string();
            let (name, manifest, manifest_url) = match platform_package {
                None => (package.clone(), latest, latest_url),
                Some(template) => {
                    let key = platform.expand(template, &version);
                    let spec = latest
                        .pointer("/optionalDependencies")
                        .and_then(|deps| deps.get(&key))
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| {
                            InstallError::UnsupportedPlatform(format!(
                                "{id}: {package}@{version} lists no {key}"
                            ))
                        })?;
                    let (real_name, real_version) = match spec.strip_prefix("npm:") {
                        Some(alias) => {
                            let at = alias.rfind('@').filter(|&i| i > 0).ok_or_else(|| {
                                unverifiable(id, format!("unreadable npm alias {spec:?}"))
                            })?;
                            (alias[..at].to_string(), alias[at + 1..].to_string())
                        }
                        None => (key.clone(), spec.to_string()),
                    };
                    let url = format!("{registry}/{real_name}/{real_version}");
                    (key, json(fetch, &url)?, url)
                }
            };
            let integrity = manifest
                .pointer("/dist/integrity")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let digest = Digest::from_integrity(integrity).ok_or_else(|| {
                unverifiable(id, format!("{manifest_url} carries no sha512 integrity"))
            })?;
            let url = text_field(&manifest, &manifest_url, "/dist/tarball")?.to_string();
            Release {
                version,
                artifact: Artifact::Download {
                    url,
                    digest,
                    format: Format::TarGz,
                    strip: 1,
                    into: format!("node_modules/{name}"),
                },
            }
        }
        Source::NpmTree { package, registry } => {
            let registry = registry
                .as_deref()
                .unwrap_or(NPM_REGISTRY)
                .trim_end_matches('/')
                .to_string();
            let latest_url = format!("{registry}/{package}/latest");
            let latest = json(fetch, &latest_url)?;
            let version = text_field(&latest, &latest_url, "/version")?.to_string();
            let integrity = latest
                .pointer("/dist/integrity")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let digest = Digest::from_integrity(integrity).ok_or_else(|| {
                unverifiable(id, format!("{latest_url} carries no sha512 integrity"))
            })?;
            Release {
                artifact: Artifact::NpmTree {
                    spec: format!("{package}@{version}"),
                    registry,
                    digest,
                },
                version,
            }
        }
        Source::GithubRelease {
            repo,
            tag_prefix,
            assets,
            checksums,
            format,
            strip,
            api,
            download,
        } => {
            let api = api.as_deref().unwrap_or(GITHUB_API).trim_end_matches('/');
            let download = download
                .as_deref()
                .unwrap_or(GITHUB_DOWNLOAD)
                .trim_end_matches('/');
            let latest_url = format!("{api}/repos/{repo}/releases/latest");
            let latest = json(fetch, &latest_url)?;
            let tag = text_field(&latest, &latest_url, "/tag_name")?.to_string();
            let version = tag
                .strip_prefix(tag_prefix.as_str())
                .ok_or_else(|| {
                    unverifiable(
                        id,
                        format!("release tag {tag:?} lacks prefix {tag_prefix:?}"),
                    )
                })?
                .to_string();
            let asset = assets.get(&platform.key()).ok_or_else(|| {
                InstallError::UnsupportedPlatform(format!("{id}: no asset for {}", platform.key()))
            })?;
            let asset = platform.expand(asset, &version);
            let checksums = platform.expand(checksums, &version);
            let base = format!("{download}/{repo}/releases/download/{tag}");
            let sums_url = format!("{base}/{checksums}");
            let sums = fetch.get(&sums_url, METADATA_LIMIT)?;
            let digest = checksum_for(&String::from_utf8_lossy(&sums), &asset)
                .ok_or_else(|| unverifiable(id, format!("{sums_url} lists no {asset}")))?;
            Release {
                version,
                artifact: Artifact::Download {
                    url: format!("{base}/{asset}"),
                    digest,
                    format: *format,
                    strip: *strip,
                    into: String::new(),
                },
            }
        }
        Source::VendorManifest { url, format, strip } => {
            let manifest = json(fetch, url)?;
            let version = text_field(&manifest, url, "/version")?.to_string();
            let key = platform.key();
            let entry = manifest
                .pointer("/platforms")
                .and_then(|p| p.get(&key))
                .ok_or_else(|| {
                    InstallError::UnsupportedPlatform(format!("{id}: {url} lists no {key}"))
                })?;
            let artifact: PinnedArtifact = serde_json::from_value(entry.clone())
                .map_err(|err| unverifiable(id, format!("{url} {key}: {err}")))?;
            Release {
                version,
                artifact: Artifact::Download {
                    digest: sha256(id, &artifact.sha256)?,
                    url: artifact.url,
                    format: *format,
                    strip: *strip,
                    into: String::new(),
                },
            }
        }
        Source::Pinned {
            version,
            artifacts,
            format,
            strip,
        } => {
            let artifact = artifacts.get(&platform.key()).ok_or_else(|| {
                InstallError::UnsupportedPlatform(format!(
                    "{id}: nothing pinned for {}",
                    platform.key()
                ))
            })?;
            Release {
                version: version.clone(),
                artifact: Artifact::Download {
                    url: platform.expand(&artifact.url, version),
                    digest: sha256(id, &artifact.sha256)?,
                    format: *format,
                    strip: *strip,
                    into: String::new(),
                },
            }
        }
        Source::Local {
            version,
            path,
            sha256: hex,
        } => {
            if !path.is_absolute() {
                return Err(unverifiable(
                    id,
                    format!("{} is not absolute", path.display()),
                ));
            }
            Release {
                version: version.clone(),
                artifact: Artifact::Local {
                    path: path.clone(),
                    digest: sha256(id, hex)?,
                },
            }
        }
        Source::Builtin => {
            return Err(InstallError::Failed(format!(
                "{id} runs inside this binary"
            )));
        }
        Source::Unconfigured => {
            return Err(unverifiable(
                id,
                "no verifiable release source; declare a pinned, local or vendor-manifest source in caller configuration",
            ));
        }
    };
    check_version(id, &release.version)?;
    Ok(release)
}
