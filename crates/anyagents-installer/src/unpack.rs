//! Unpacking a verified artifact into a staging directory. Every entry must
//! land inside the destination: absolute paths, `..` and links that escape are
//! refused rather than skipped, so a hostile archive fails the whole install.

use std::fs::File;
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::InstallError;

/// How an artifact is packed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Format {
    /// A gzip-compressed tar archive (npm tarballs, most release archives).
    #[default]
    TarGz,
    /// A zip archive.
    Zip,
    /// The artifact is the executable itself.
    Raw,
}

fn unpack_error(reason: impl std::fmt::Display) -> InstallError {
    InstallError::Unpack(reason.to_string())
}

/// `relative` with its first `strip` components removed, or `None` when
/// nothing is left. Refuses anything that is not a plain relative path.
fn stripped(relative: &Path, strip: usize) -> Result<Option<PathBuf>, InstallError> {
    let mut out = PathBuf::new();
    for (i, component) in relative.components().enumerate() {
        match component {
            Component::Normal(part) => {
                if i >= strip {
                    out.push(part);
                }
            }
            Component::CurDir => {}
            _ => {
                return Err(unpack_error(format!(
                    "entry escapes the install directory: {}",
                    relative.display()
                )));
            }
        }
    }
    Ok((!out.as_os_str().is_empty()).then_some(out))
}

/// Whether a link at `at` (relative to the root) pointing to `target` stays inside.
fn link_stays_inside(at: &Path, target: &Path) -> bool {
    if target.is_absolute() {
        return false;
    }
    let mut depth: i64 = at.components().count() as i64 - 1;
    for component in target.components() {
        match component {
            Component::ParentDir => depth -= 1,
            Component::Normal(_) => depth += 1,
            Component::CurDir => {}
            _ => return false,
        }
        if depth < 0 {
            return false;
        }
    }
    true
}

fn ensure_parent(path: &Path) -> Result<(), InstallError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(unpack_error)?;
    }
    Ok(())
}

#[cfg(unix)]
fn set_mode(path: &Path, executable: bool) -> Result<(), InstallError> {
    use std::os::unix::fs::PermissionsExt as _;
    let mode = if executable { 0o755 } else { 0o644 };
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).map_err(unpack_error)
}

#[cfg(not(unix))]
fn set_mode(_: &Path, _: bool) -> Result<(), InstallError> {
    Ok(())
}

#[cfg(unix)]
fn symlink(target: &Path, at: &Path) -> Result<(), InstallError> {
    std::os::unix::fs::symlink(target, at).map_err(unpack_error)
}

#[cfg(not(unix))]
fn symlink(_: &Path, _: &Path) -> Result<(), InstallError> {
    Err(unpack_error("symbolic links are not supported here"))
}

/// Unpack `artifact` into `dest`. For [`Format::Raw`], the file is copied to
/// `dest/raw_name` and made executable.
pub fn unpack(
    artifact: &Path,
    format: Format,
    strip: usize,
    dest: &Path,
    raw_name: &str,
) -> Result<(), InstallError> {
    std::fs::create_dir_all(dest).map_err(unpack_error)?;
    match format {
        Format::Raw => {
            let relative = stripped(Path::new(raw_name), 0)?
                .ok_or_else(|| unpack_error("raw artifact has no file name"))?;
            let target = dest.join(relative);
            ensure_parent(&target)?;
            std::fs::copy(artifact, &target).map_err(unpack_error)?;
            set_mode(&target, true)
        }
        Format::TarGz => unpack_tar_gz(artifact, strip, dest),
        Format::Zip => unpack_zip(artifact, strip, dest),
    }
}

fn unpack_tar_gz(artifact: &Path, strip: usize, dest: &Path) -> Result<(), InstallError> {
    let file = File::open(artifact).map_err(unpack_error)?;
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(file));
    for entry in archive.entries().map_err(unpack_error)? {
        let mut entry = entry.map_err(unpack_error)?;
        let path = entry.path().map_err(unpack_error)?.into_owned();
        let Some(relative) = stripped(&path, strip)? else {
            continue;
        };
        let target = dest.join(&relative);
        let kind = entry.header().entry_type();
        if kind.is_dir() {
            std::fs::create_dir_all(&target).map_err(unpack_error)?;
        } else if kind.is_symlink() {
            let link = entry
                .link_name()
                .map_err(unpack_error)?
                .ok_or_else(|| unpack_error("link without a target"))?
                .into_owned();
            if !link_stays_inside(&relative, &link) {
                return Err(unpack_error(format!(
                    "link escapes the install directory: {} -> {}",
                    relative.display(),
                    link.display()
                )));
            }
            ensure_parent(&target)?;
            symlink(&link, &target)?;
        } else if kind.is_file() {
            ensure_parent(&target)?;
            let mode = entry.header().mode().unwrap_or(0o644);
            let mut out = File::create(&target).map_err(unpack_error)?;
            std::io::copy(&mut entry, &mut out).map_err(unpack_error)?;
            drop(out);
            set_mode(&target, mode & 0o111 != 0)?;
        } else if kind.is_hard_link() {
            return Err(unpack_error(format!(
                "hard links are not unpacked: {}",
                relative.display()
            )));
        }
        // PAX headers, GNU long names and the like carry no file.
    }
    Ok(())
}

fn unpack_zip(artifact: &Path, strip: usize, dest: &Path) -> Result<(), InstallError> {
    let file = File::open(artifact).map_err(unpack_error)?;
    let mut archive = zip::ZipArchive::new(file).map_err(unpack_error)?;
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).map_err(unpack_error)?;
        let path = entry
            .enclosed_name()
            .ok_or_else(|| unpack_error(format!("entry escapes: {}", entry.name())))?;
        let Some(relative) = stripped(&path, strip)? else {
            continue;
        };
        let target = dest.join(&relative);
        let mode = entry.unix_mode();
        let is_link = mode.is_some_and(|m| m & 0o170000 == 0o120000);
        if entry.is_dir() {
            std::fs::create_dir_all(&target).map_err(unpack_error)?;
        } else if is_link {
            let mut link = String::new();
            entry.read_to_string(&mut link).map_err(unpack_error)?;
            let link = PathBuf::from(link);
            if !link_stays_inside(&relative, &link) {
                return Err(unpack_error(format!(
                    "link escapes the install directory: {}",
                    relative.display()
                )));
            }
            ensure_parent(&target)?;
            symlink(&link, &target)?;
        } else {
            ensure_parent(&target)?;
            let mut out = File::create(&target).map_err(unpack_error)?;
            std::io::copy(&mut entry, &mut out).map_err(unpack_error)?;
            drop(out);
            set_mode(&target, mode.is_some_and(|m| m & 0o111 != 0))?;
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::io::Write as _;

    /// A `.tar.gz` holding `files` (path, bytes, mode).
    pub(crate) fn tar_gz(files: &[(&str, &[u8], u32)]) -> Vec<u8> {
        let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
            Vec::new(),
            flate2::Compression::fast(),
        ));
        for (path, bytes, mode) in files {
            let mut header = tar::Header::new_gnu();
            header.set_size(bytes.len() as u64);
            header.set_mode(*mode);
            header.set_entry_type(tar::EntryType::Regular);
            header.set_cksum();
            builder.append_data(&mut header, path, *bytes).unwrap();
        }
        builder.into_inner().unwrap().finish().unwrap()
    }

    /// A `.zip` holding `files` (path, bytes, mode).
    pub(crate) fn zip(files: &[(&str, &[u8], u32)]) -> Vec<u8> {
        let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        for (path, bytes, mode) in files {
            let options = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated)
                .unix_permissions(*mode);
            writer.start_file(*path, options).unwrap();
            writer.write_all(bytes).unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    #[test]
    fn a_tarball_unpacks_with_its_root_stripped_and_modes_kept() {
        let dir = tempfile::tempdir().unwrap();
        let artifact = dir.path().join("a.tgz");
        std::fs::write(
            &artifact,
            tar_gz(&[
                ("package/bin/tool", b"#!/bin/sh\n", 0o755),
                ("package/README", b"r", 0o644),
            ]),
        )
        .unwrap();
        let dest = dir.path().join("out");
        unpack(&artifact, Format::TarGz, 1, &dest, "").unwrap();
        assert!(dest.join("bin/tool").is_file());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = |p: &str| {
                std::fs::metadata(dest.join(p))
                    .unwrap()
                    .permissions()
                    .mode()
            };
            assert_eq!(mode("bin/tool") & 0o777, 0o755);
            assert_eq!(mode("README") & 0o777, 0o644);
        }
    }

    #[test]
    fn a_zip_unpacks_with_its_root_stripped() {
        let dir = tempfile::tempdir().unwrap();
        let artifact = dir.path().join("a.zip");
        std::fs::write(&artifact, zip(&[("tool-linux/tool", b"bin", 0o755)])).unwrap();
        let dest = dir.path().join("out");
        unpack(&artifact, Format::Zip, 1, &dest, "").unwrap();
        assert_eq!(std::fs::read(dest.join("tool")).unwrap(), b"bin");
    }

    #[test]
    fn an_escaping_entry_fails_the_whole_unpack() {
        assert!(stripped(Path::new("package/../../etc/passwd"), 1).is_err());
        assert!(stripped(Path::new("/etc/passwd"), 0).is_err());
        assert!(link_stays_inside(
            Path::new("bin/npm"),
            Path::new("../lib/npm.js")
        ));
        assert!(!link_stays_inside(
            Path::new("bin/npm"),
            Path::new("../../x")
        ));
        assert!(!link_stays_inside(Path::new("x"), Path::new("/etc/passwd")));
    }
}
