//! Bounded image reads for Telegram's multipart InputFile.

use std::io::Cursor;
use std::net::IpAddr;
use std::time::Duration;

use anyagents_core::Error;
use image::{ImageFormat, ImageReader, Limits};
use ureq::unversioned::resolver::{DefaultResolver, ResolvedSocketAddrs, Resolver};
use ureq::unversioned::transport::{DefaultConnector, NextTimeout};

use super::super::MediaUpload;

const MAX_PHOTO_DIMENSION: u32 = 4096;

pub(crate) struct Photo {
    pub(super) upload: MediaUpload,
}

impl Photo {
    pub(crate) const MAX_BYTES: u64 = 10 * 1024 * 1024;

    pub(crate) fn from_bytes(bytes: Vec<u8>) -> Result<Self, Error> {
        Self::validate(bytes, None)
    }

    pub(crate) fn from_upload(upload: MediaUpload) -> Result<Self, Error> {
        Self::validate(upload.bytes, Some(upload.file_name))
    }

    fn validate(bytes: Vec<u8>, filename: Option<String>) -> Result<Self, Error> {
        if bytes.len() as u64 > Self::MAX_BYTES {
            return Err(Error::Message("chat photo must be at most 10 MiB".into()));
        }
        let mut reader = ImageReader::new(Cursor::new(&bytes))
            .with_guessed_format()
            .map_err(|_| invalid_image())?;
        let (file_name, mime) = match reader.format() {
            Some(ImageFormat::Png) => ("chat-photo.png", "image/png"),
            Some(ImageFormat::Jpeg) => ("chat-photo.jpg", "image/jpeg"),
            _ => return Err(invalid_image()),
        };
        let mut limits = Limits::default();
        limits.max_image_width = Some(MAX_PHOTO_DIMENSION);
        limits.max_image_height = Some(MAX_PHOTO_DIMENSION);
        limits.max_alloc = Some(80 * 1024 * 1024);
        reader.limits(limits);
        reader.decode().map_err(|_| invalid_image())?;
        Ok(Self {
            upload: MediaUpload {
                file_name: filename.unwrap_or_else(|| file_name.into()),
                content_type: mime,
                bytes,
            },
        })
    }

    pub(crate) fn download(url: &url::Url) -> Result<Self, Error> {
        if !matches!(url.scheme(), "http" | "https")
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(Error::Message(
                "chat photo URL must use HTTP(S) without embedded credentials".into(),
            ));
        }
        let config = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(20)))
            .timeout_resolve(Some(Duration::from_secs(5)))
            .max_redirects(0)
            .proxy(None)
            .build();
        let agent = ureq::Agent::with_parts(config, DefaultConnector::default(), PublicResolver);
        let mut response = agent
            .get(url.as_str())
            .call()
            .map_err(|_| download_failed())?;
        if !response.status().is_success() {
            return Err(download_failed());
        }
        if response
            .body()
            .content_length()
            .is_some_and(|len| len > Self::MAX_BYTES)
        {
            return Err(Error::Message("chat photo must be at most 10 MiB".into()));
        }
        let bytes = response
            .body_mut()
            .with_config()
            .limit(Self::MAX_BYTES)
            .read_to_vec()
            .map_err(|_| Error::Message("could not read chat photo (10 MiB limit)".into()))?;
        Self::from_bytes(bytes)
    }
}

fn invalid_image() -> Error {
    Error::Message("chat photo must be a valid JPEG or PNG (up to 4096 pixels per side)".into())
}

fn download_failed() -> Error {
    Error::Message(
        "could not download chat photo; provide a direct public HTTP(S) image URL (no redirects)"
            .into(),
    )
}

#[derive(Debug)]
struct PublicResolver;

impl Resolver for PublicResolver {
    fn resolve(
        &self,
        uri: &ureq::http::Uri,
        config: &ureq::config::Config,
        timeout: NextTimeout,
    ) -> Result<ResolvedSocketAddrs, ureq::Error> {
        // Vet the exact socket addresses the connector will use, not an
        // earlier DNS answer. Redirects and proxy-side DNS are disabled.
        let addresses = DefaultResolver::default().resolve(uri, config, timeout)?;
        if addresses
            .iter()
            .all(|addr| public_ip(addr.ip()) || mock_loopback(addr.ip()))
        {
            Ok(addresses)
        } else {
            Err(ureq::Error::HostNotFound)
        }
    }
}

fn public_ip(ip: IpAddr) -> bool {
    match ip.to_canonical() {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !(ip.is_loopback()
                || ip.is_private()
                || ip.is_link_local()
                || ip.is_documentation()
                || a == 0
                || a >= 224
                || (a == 100 && (64..128).contains(&b))
                || (a == 192 && b == 0 && c == 0)
                || (a == 198 && (b == 18 || b == 19)))
        }
        IpAddr::V6(ip) => {
            let [a, b, ..] = ip.segments();
            // Global unicast only; exclude special-use, documentation and
            // tunneling ranges (including addresses with embedded IPv4).
            (0x2000..0x4000).contains(&a)
                && !(a == 0x2001 && (b < 0x0200 || b == 0x0db8))
                && a != 0x2002
                && (a & 0xfff0) != 0x3ff0
        }
    }
}

fn mock_loopback(ip: IpAddr) -> bool {
    cfg!(test) && ip.is_loopback()
}
