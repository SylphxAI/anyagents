//! Lease image identity shared by the provider, renewal loop and version view.
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LeaseVersion {
    pub image: Option<String>,
    pub image_digest: Option<String>,
    pub harness_version: Option<String>,
    pub tpl_base_version: Option<String>,
    pub sbx_guest_version: Option<String>,
}

pub(crate) fn desired_ref(image: Option<&str>, template: &str) -> String {
    image
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| {
            if template.starts_with("template:") {
                template.to_owned()
            } else {
                format!("template:{template}")
            }
        })
}

pub(crate) fn digest(image: &str) -> Option<String> {
    let value = image.trim().rsplit('@').next()?.strip_prefix("sha256:")?;
    (value.len() == 64 && value.bytes().all(|ch| ch.is_ascii_hexdigit()))
        .then(|| format!("sha256:{}", value.to_ascii_lowercase()))
}

impl LeaseVersion {
    pub fn digest(&self) -> Option<String> {
        self.image_digest
            .as_deref()
            .and_then(digest)
            .or_else(|| self.image.as_deref().and_then(digest))
    }

    pub fn matches(&self, desired: &str) -> bool {
        if let Some(expected) = digest(desired) {
            return self.digest().as_ref() == Some(&expected);
        }
        // A mutable reference is not proof of a digest. Compare the lease's
        // declared reference only; operators requiring content identity must pin.
        if let Some(spec) = self.image.as_deref() {
            return spec.trim() == desired;
        }
        // Legacy built-in templates omit spec.image from some responses.
        self.image_digest.is_none() && desired.starts_with("template:")
    }

    pub fn from_response(root: &serde_json::Value) -> Self {
        let field = |pointers: &[&str]| {
            pointers
                .iter()
                .filter_map(|key| root.pointer(key)?.as_str())
                .map(str::trim)
                .find(|value| !value.is_empty())
                .map(str::to_owned)
        };
        Self {
            image: field(&["/spec/image", "/image"]),
            image_digest: field(&[
                "/status/imageDigest",
                "/status/image_digest",
                "/imageDigest",
                "/image_digest",
            ]),
            harness_version: field(&["/status/harnessVersion", "/status/harness_version"]),
            tpl_base_version: field(&["/status/tplBaseVersion", "/status/tpl_base_version"]),
            sbx_guest_version: field(&["/status/sbxGuestVersion", "/status/sbx_guest_version"]),
        }
    }

    pub fn inherit_missing(&mut self, prior: Self) {
        self.image = self.image.take().or(prior.image);
        self.image_digest = self.image_digest.take().or(prior.image_digest);
        self.harness_version = self.harness_version.take().or(prior.harness_version);
        self.tpl_base_version = self.tpl_base_version.take().or(prior.tpl_base_version);
        self.sbx_guest_version = self.sbx_guest_version.take().or(prior.sbx_guest_version);
    }
}

pub(crate) fn configured_sylphx_image(
    config_dir: &std::path::Path,
    fallback_template: &str,
) -> Result<(Option<String>, String), String> {
    let root = config_dir.ancestors().nth(3);
    let registry = super::providers::ProviderRegistry::from_env_or_config(root, None)?;
    let spec = registry.default_image_spec();
    let image = spec.image.clone();
    let desired = desired_ref(
        image.as_deref(),
        spec.template.as_deref().unwrap_or(fallback_template),
    );
    Ok((image, desired))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_observed_digest_before_declared_ref_and_does_not_equate_tags_with_digests() {
        let a = format!("sha256:{}", "a".repeat(64));
        let b = format!("sha256:{}", "b".repeat(64));
        let version = LeaseVersion {
            image: Some(format!("registry.example/sandbox@{b}")),
            image_digest: Some(a.clone()),
            ..Default::default()
        };
        assert!(version.matches(&a));
        assert!(version.matches(&format!("mirror.example:5000/sandbox@{a}")));
        assert!(!version.matches(&b));
        assert!(!version.matches("mirror.example:5000/sandbox:latest"));
        assert!(!LeaseVersion::default().matches(&a));
    }
}
