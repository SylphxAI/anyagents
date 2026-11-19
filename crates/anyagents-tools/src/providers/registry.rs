//! Unified configuration and factory registry for all 8 [`SandboxProvider`] backends.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::cloudflare::{CloudflareConfig, CloudflareProvider};
use super::daytona::{DaytonaConfig, DaytonaProvider};
use super::docker::{DockerConfig, DockerProvider};
use super::e2b::{E2bConfig, E2bProvider};
use super::http_oci::{GenericHttpOciConfig, GenericHttpOciProvider};
use super::local::{LocalHostConfig, LocalHostProvider};
use super::modal::{ModalConfig, ModalProvider};
use super::sylphx::{SylphxConfig, SylphxProvider};
use super::{
    DEFAULT_IDLE_TTL_MS, DEFAULT_MAX_LEASE_TTL_MS, DEFAULT_MOUNT_PATH, EnsureSandboxRequest,
    ImageSpec, LeasePolicy, ProviderKind, SandboxProvider, VolumeSpec, normalize_unix_dir,
};

/// Unified provider configuration loaded from `sandbox-provider.json`, environment variables,
/// or explicit programmatic overrides.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxRegistryConfig {
    /// Active provider selector (`local|sylphx|e2b|modal|daytona|docker|cloudflare|http`).
    #[serde(default, alias = "driver", skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shape: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_size_gib: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_ttl_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_lease_ttl_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mount_path: Option<String>,
    #[serde(
        default,
        alias = "default_workdir",
        skip_serializing_if = "Option::is_none"
    )]
    pub workdir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_root: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docker_host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docker_executable: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docker_network: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docker_memory: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docker_cpus: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub e2b_domain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub e2b_envd_base_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modal_app: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modal_token_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modal_token_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub daytona_target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloudflare_account_id: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

/// Registry resolving and holding instantiated [`SandboxProvider`] drivers.
#[derive(Clone)]
pub struct ProviderRegistry {
    active_kind: ProviderKind,
    providers: BTreeMap<ProviderKind, Arc<dyn SandboxProvider>>,
    default_image: ImageSpec,
    default_lease: LeasePolicy,
    default_shape: Option<String>,
    default_mount_path: String,
    default_workdir: String,
    default_volume_size_gib: Option<u32>,
}

impl std::fmt::Debug for ProviderRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderRegistry")
            .field("active_kind", &self.active_kind)
            .field("registered", &self.providers.keys().collect::<Vec<_>>())
            .field("default_image", &self.default_image)
            .field("default_lease", &self.default_lease)
            .field("default_mount_path", &self.default_mount_path)
            .field("default_workdir", &self.default_workdir)
            .finish()
    }
}

impl ProviderRegistry {
    /// Resolve active provider & defaults from `<config_or_data_dir>/sandbox-provider.json`,
    /// standard `ANYAGENTS_SANDBOX_*` environment variables, and optional explicit `overrides`.
    ///
    /// Precedence (highest to lowest): `overrides` > environment variables > config file > defaults.
    pub fn from_env_or_config(
        config_or_data_dir: Option<&Path>,
        overrides: Option<&SandboxRegistryConfig>,
    ) -> Result<Self, String> {
        let file_cfg = load_file_config(config_or_data_dir).unwrap_or_default();
        let env_cfg = load_env_config()?;
        let mut merged = merge_configs(&file_cfg, &env_cfg);
        if let Some(ov) = overrides {
            merged = merge_configs(&merged, ov);
        }
        Self::from_config(&merged)
    }

    /// Build a [`ProviderRegistry`] purely from an explicit [`SandboxRegistryConfig`] without
    /// reading process environment variables.
    pub fn from_config(cfg: &SandboxRegistryConfig) -> Result<Self, String> {
        let active_kind = resolve_active_kind(cfg)?;
        let default_mount_path = cfg
            .mount_path
            .as_deref()
            .map(|p| normalize_unix_dir(p, DEFAULT_MOUNT_PATH))
            .unwrap_or_else(|| DEFAULT_MOUNT_PATH.to_string());
        let fallback_workdir = format!("{default_mount_path}/workspace");
        let default_workdir = cfg
            .workdir
            .as_deref()
            .map(|p| normalize_unix_dir(p, &fallback_workdir))
            .unwrap_or(fallback_workdir);
        let default_lease = LeasePolicy {
            idle_ttl_ms: cfg.idle_ttl_ms.unwrap_or(DEFAULT_IDLE_TTL_MS),
            max_lease_ttl_ms: cfg.max_lease_ttl_ms.unwrap_or(DEFAULT_MAX_LEASE_TTL_MS),
        };
        let default_image = ImageSpec {
            image: clean_opt(&cfg.image),
            template: clean_opt(&cfg.template),
            env: cfg.env.clone(),
        };

        let mut providers: BTreeMap<ProviderKind, Arc<dyn SandboxProvider>> = BTreeMap::new();
        // Always register LocalHostProvider as the zero-config baseline.
        let local_cfg = LocalHostConfig {
            root_dir: cfg
                .local_root
                .clone()
                .unwrap_or_else(|| std::env::temp_dir().join("anyagents-sandboxes")),
        };
        providers.insert(
            ProviderKind::Local,
            Arc::new(LocalHostProvider::new(local_cfg)),
        );

        let active_provider: Arc<dyn SandboxProvider> = match active_kind {
            ProviderKind::Local => Arc::clone(providers.get(&ProviderKind::Local).unwrap()),
            ProviderKind::Sylphx => {
                let parent = clean_opt(&cfg.parent).ok_or_else(|| {
                    "Sylphx provider selected, but `ANYAGENTS_SYLPHX_PARENT` (orgs/{o}/projects/{p}/envs/{e}) is not configured".to_string()
                })?;
                let mut sc = SylphxConfig::new(parent);
                if let Some(ep) = clean_opt(&cfg.endpoint) {
                    sc.endpoint = ep;
                }
                sc.token = clean_opt(&cfg.token);
                sc.default_image = default_image.image.clone();
                if let Some(tpl) = default_image.template.clone() {
                    sc.default_template = tpl;
                }
                if let Some(shape) = clean_opt(&cfg.shape) {
                    sc.default_shape = shape;
                }
                sc.default_volume_size_gib = cfg.volume_size_gib;
                sc.default_lease = default_lease.clone();
                Arc::new(SylphxProvider::new(sc)?)
            }
            ProviderKind::E2b => {
                let mut ec = E2bConfig::default();
                if let Some(ep) = clean_opt(&cfg.endpoint) {
                    ec.api_url = ep;
                }
                if let Some(dom) = clean_opt(&cfg.e2b_domain) {
                    ec.domain = dom;
                }
                ec.api_key = clean_opt(&cfg.token);
                if let Some(tpl) = default_image.template.clone() {
                    ec.default_template = tpl;
                }
                ec.envd_base_url = clean_opt(&cfg.e2b_envd_base_url);
                Arc::new(E2bProvider::new(ec))
            }
            ProviderKind::Modal => {
                let mut mc = ModalConfig::default();
                if let Some(ep) = clean_opt(&cfg.endpoint) {
                    mc.endpoint = ep;
                }
                mc.token_id = clean_opt(&cfg.modal_token_id);
                mc.token_secret = clean_opt(&cfg.modal_token_secret);
                mc.bearer_token = clean_opt(&cfg.token);
                if let Some(app) = clean_opt(&cfg.modal_app) {
                    mc.app_name = app;
                }
                if let Some(img) = default_image.image.clone() {
                    mc.default_image = img;
                }
                Arc::new(ModalProvider::new(mc))
            }
            ProviderKind::Daytona => {
                let mut dc = DaytonaConfig::default();
                if let Some(ep) = clean_opt(&cfg.endpoint) {
                    dc.api_url = ep;
                }
                dc.api_key = clean_opt(&cfg.token);
                dc.target = clean_opt(&cfg.daytona_target);
                dc.default_image = default_image.image.clone();
                dc.default_snapshot = default_image.template.clone();
                Arc::new(DaytonaProvider::new(dc))
            }
            ProviderKind::Docker => {
                let mut dk = DockerConfig {
                    docker_host: clean_opt(&cfg.docker_host),
                    docker_executable: cfg.docker_executable.clone(),
                    default_network: clean_opt(&cfg.docker_network),
                    memory_limit: clean_opt(&cfg.docker_memory),
                    cpus: clean_opt(&cfg.docker_cpus),
                    ..DockerConfig::default()
                };
                if let Some(img) = default_image.image.clone() {
                    dk.default_image = img;
                }
                Arc::new(DockerProvider::new(dk))
            }
            ProviderKind::Cloudflare => {
                let worker_url = clean_opt(&cfg.endpoint).ok_or_else(|| {
                    "Cloudflare provider selected, but `ANYAGENTS_CLOUDFLARE_SANDBOX_URL` is not configured".to_string()
                })?;
                let mut cf = CloudflareConfig::new(worker_url);
                cf.api_token = clean_opt(&cfg.token);
                cf.account_id = clean_opt(&cfg.cloudflare_account_id);
                cf.default_image = default_image.image.clone();
                Arc::new(CloudflareProvider::new(cf)?)
            }
            ProviderKind::Http => {
                let endpoint = clean_opt(&cfg.endpoint).ok_or_else(|| {
                    "Generic HTTP OCI provider selected, but `ANYAGENTS_SANDBOX_HTTP_URL` / `ANYAGENTS_SANDBOX_PROVIDER_URL` is not configured".to_string()
                })?;
                let mut hc = GenericHttpOciConfig::new(endpoint);
                hc.token = clean_opt(&cfg.token);
                hc.default_image = default_image.image.clone();
                hc.default_template = default_image.template.clone();
                Arc::new(GenericHttpOciProvider::new(hc)?)
            }
        };

        providers.insert(active_kind, active_provider);
        Ok(Self {
            active_kind,
            providers,
            default_image,
            default_lease,
            default_shape: clean_opt(&cfg.shape),
            default_mount_path,
            default_workdir,
            default_volume_size_gib: cfg.volume_size_gib,
        })
    }

    /// Return the currently selected [`ProviderKind`].
    pub fn active_kind(&self) -> ProviderKind {
        self.active_kind
    }

    /// Return the active [`SandboxProvider`] instance.
    pub fn active(&self) -> Arc<dyn SandboxProvider> {
        Arc::clone(
            self.providers
                .get(&self.active_kind)
                .expect("active provider always registered"),
        )
    }

    /// Look up any registered provider by [`ProviderKind`].
    pub fn get(&self, kind: ProviderKind) -> Option<Arc<dyn SandboxProvider>> {
        self.providers.get(&kind).cloned()
    }

    /// Register or replace a provider driver in this registry.
    pub fn register(&mut self, provider: Arc<dyn SandboxProvider>) {
        self.providers.insert(provider.kind(), provider);
    }

    /// Default [`ImageSpec`] resolved from environment/config.
    pub fn default_image_spec(&self) -> &ImageSpec {
        &self.default_image
    }

    /// Default [`LeasePolicy`] resolved from environment/config.
    pub fn default_lease_policy(&self) -> &LeasePolicy {
        &self.default_lease
    }

    /// Build a pre-populated [`EnsureSandboxRequest`] for `agent_id` using registry defaults.
    pub fn default_ensure_request(&self, agent_id: &str) -> EnsureSandboxRequest {
        let mut vol = VolumeSpec::for_agent(agent_id);
        vol.mount_path = self.default_mount_path.clone();
        vol.size_gib = self.default_volume_size_gib;

        EnsureSandboxRequest {
            agent_id: agent_id.to_string(),
            agent_name: None,
            image: self.default_image.clone(),
            volume: Some(vol),
            shape: self.default_shape.clone(),
            network: super::NetworkPolicySpec::Allow,
            lease: self.default_lease.clone(),
            existing: None,
            workdir: Some(self.default_workdir.clone()),
        }
    }
}

fn resolve_active_kind(cfg: &SandboxRegistryConfig) -> Result<ProviderKind, String> {
    if let Some(raw) = clean_opt(&cfg.provider) {
        return ProviderKind::from_str(&raw);
    }
    if clean_opt(&cfg.parent).is_some() {
        return Ok(ProviderKind::Sylphx);
    }
    if clean_opt(&cfg.cloudflare_account_id).is_some() && clean_opt(&cfg.endpoint).is_some() {
        return Ok(ProviderKind::Cloudflare);
    }
    if clean_opt(&cfg.modal_token_id).is_some() || clean_opt(&cfg.modal_token_secret).is_some() {
        return Ok(ProviderKind::Modal);
    }
    if clean_opt(&cfg.e2b_domain).is_some() || clean_opt(&cfg.e2b_envd_base_url).is_some() {
        return Ok(ProviderKind::E2b);
    }
    if clean_opt(&cfg.daytona_target).is_some() {
        return Ok(ProviderKind::Daytona);
    }
    if clean_opt(&cfg.endpoint).is_some() {
        return Ok(ProviderKind::Http);
    }
    Ok(ProviderKind::Local)
}

fn load_file_config(config_or_data_dir: Option<&Path>) -> Option<SandboxRegistryConfig> {
    let mut candidates = Vec::new();
    if let Some(p) = env_non_empty("ANYAGENTS_SANDBOX_PROVIDER_CONFIG") {
        candidates.push(PathBuf::from(p));
    }
    if let Some(path) = config_or_data_dir {
        if path.is_file() {
            candidates.push(path.to_path_buf());
        } else {
            candidates.push(path.join("sandbox-provider.json"));
        }
    }
    for path in candidates {
        if let Ok(raw) = std::fs::read_to_string(&path)
            && let Ok(parsed) = serde_json::from_str::<SandboxRegistryConfig>(&raw)
        {
            return Some(parsed);
        }
    }
    None
}

fn load_env_config() -> Result<SandboxRegistryConfig, String> {
    let explicit_provider = env_non_empty("ANYAGENTS_SANDBOX_PROVIDER")
        .or_else(|| env_non_empty("ANYAGENTS_SANDBOX_DRIVER"));
    let parsed_kind = match explicit_provider.as_deref() {
        Some(raw) => Some(ProviderKind::from_str(raw)?),
        None => None,
    };

    let endpoint =
        match parsed_kind {
            Some(ProviderKind::Sylphx) => env_non_empty("ANYAGENTS_SANDBOX_ENDPOINT")
                .or_else(|| env_non_empty("SYLPHX_SANDBOX_URL"))
                .or_else(|| env_non_empty("ANYAGENTS_SYLPHX_ENDPOINT")),
            Some(ProviderKind::E2b) => {
                env_non_empty("ANYAGENTS_E2B_API_URL").or_else(|| env_non_empty("E2B_API_URL"))
            }
            Some(ProviderKind::Modal) => env_non_empty("ANYAGENTS_MODAL_ENDPOINT")
                .or_else(|| env_non_empty("MODAL_SANDBOX_URL")),
            Some(ProviderKind::Daytona) => env_non_empty("ANYAGENTS_DAYTONA_API_URL")
                .or_else(|| env_non_empty("DAYTONA_API_URL")),
            Some(ProviderKind::Cloudflare) => env_non_empty("ANYAGENTS_CLOUDFLARE_SANDBOX_URL")
                .or_else(|| env_non_empty("CLOUDFLARE_SANDBOX_URL")),
            Some(ProviderKind::Http) => env_non_empty("ANYAGENTS_SANDBOX_HTTP_URL")
                .or_else(|| env_non_empty("ANYAGENTS_SANDBOX_PROVIDER_URL"))
                .or_else(|| env_non_empty("ANYAGENTS_SANDBOX_ENDPOINT")),
            _ => env_non_empty("ANYAGENTS_SANDBOX_HTTP_URL")
                .or_else(|| env_non_empty("ANYAGENTS_SANDBOX_PROVIDER_URL"))
                .or_else(|| env_non_empty("ANYAGENTS_SANDBOX_ENDPOINT"))
                .or_else(|| env_non_empty("ANYAGENTS_CLOUDFLARE_SANDBOX_URL"))
                .or_else(|| env_non_empty("ANYAGENTS_MODAL_ENDPOINT"))
                .or_else(|| env_non_empty("DAYTONA_API_URL"))
                .or_else(|| env_non_empty("E2B_API_URL")),
        };

    let token = match parsed_kind {
        Some(ProviderKind::Sylphx) => env_non_empty("ANYAGENTS_SANDBOX_TOKEN")
            .or_else(|| env_non_empty("SYLPHX_API_KEY"))
            .or_else(|| env_non_empty("ANYAGENTS_SYLPHX_TOKEN")),
        Some(ProviderKind::E2b) => env_non_empty("E2B_API_KEY")
            .or_else(|| env_non_empty("ANYAGENTS_E2B_API_KEY"))
            .or_else(|| env_non_empty("ANYAGENTS_SANDBOX_TOKEN")),
        Some(ProviderKind::Daytona) => env_non_empty("DAYTONA_API_KEY")
            .or_else(|| env_non_empty("ANYAGENTS_DAYTONA_API_KEY"))
            .or_else(|| env_non_empty("ANYAGENTS_SANDBOX_TOKEN")),
        Some(ProviderKind::Cloudflare) => env_non_empty("ANYAGENTS_CLOUDFLARE_API_TOKEN")
            .or_else(|| env_non_empty("CLOUDFLARE_API_TOKEN"))
            .or_else(|| env_non_empty("ANYAGENTS_SANDBOX_TOKEN")),
        _ => env_non_empty("ANYAGENTS_SANDBOX_TOKEN")
            .or_else(|| env_non_empty("ANYAGENTS_SANDBOX_PROVIDER_TOKEN"))
            .or_else(|| env_non_empty("SYLPHX_API_KEY"))
            .or_else(|| env_non_empty("E2B_API_KEY"))
            .or_else(|| env_non_empty("DAYTONA_API_KEY"))
            .or_else(|| env_non_empty("CLOUDFLARE_API_TOKEN")),
    };

    // Infer provider from provider-specific env vars if `ANYAGENTS_SANDBOX_PROVIDER` wasn't explicitly set.
    let inferred_provider = explicit_provider.or_else(|| {
        if env_non_empty("ANYAGENTS_SYLPHX_PARENT").is_some() {
            Some("sylphx".to_string())
        } else if env_non_empty("ANYAGENTS_CLOUDFLARE_SANDBOX_URL").is_some()
            || env_non_empty("CLOUDFLARE_SANDBOX_URL").is_some()
        {
            Some("cloudflare".to_string())
        } else if env_non_empty("ANYAGENTS_SANDBOX_HTTP_URL").is_some()
            || env_non_empty("ANYAGENTS_SANDBOX_PROVIDER_URL").is_some()
        {
            Some("http".to_string())
        } else if env_non_empty("E2B_API_KEY").is_some() {
            Some("e2b".to_string())
        } else if env_non_empty("DAYTONA_API_KEY").is_some() {
            Some("daytona".to_string())
        } else if env_non_empty("ANYAGENTS_MODAL_ENDPOINT").is_some()
            || env_non_empty("MODAL_SANDBOX_URL").is_some()
        {
            Some("modal".to_string())
        } else {
            None
        }
    });

    Ok(SandboxRegistryConfig {
        provider: inferred_provider,
        endpoint,
        token,
        image: env_non_empty("ANYAGENTS_SANDBOX_IMAGE"),
        template: env_non_empty("ANYAGENTS_SANDBOX_TEMPLATE")
            .or_else(|| env_non_empty("E2B_TEMPLATE")),
        shape: env_non_empty("ANYAGENTS_SANDBOX_SHAPE"),
        parent: env_non_empty("ANYAGENTS_SYLPHX_PARENT"),
        volume_size_gib: parse_env_u32("ANYAGENTS_SANDBOX_VOLUME_SIZE_GIB")?,
        idle_ttl_ms: parse_env_u64("ANYAGENTS_SANDBOX_IDLE_TTL_MS")?,
        max_lease_ttl_ms: parse_env_u64("ANYAGENTS_SANDBOX_MAX_LEASE_TTL_MS")?,
        mount_path: env_non_empty("ANYAGENTS_SANDBOX_MOUNT_PATH"),
        workdir: env_non_empty("ANYAGENTS_SANDBOX_WORKDIR"),
        local_root: env_non_empty("ANYAGENTS_SANDBOX_LOCAL_ROOT").map(PathBuf::from),
        docker_host: env_non_empty("ANYAGENTS_DOCKER_HOST")
            .or_else(|| env_non_empty("DOCKER_HOST")),
        docker_executable: env_non_empty("ANYAGENTS_DOCKER_EXECUTABLE").map(PathBuf::from),
        docker_network: env_non_empty("ANYAGENTS_DOCKER_NETWORK"),
        docker_memory: env_non_empty("ANYAGENTS_DOCKER_MEMORY"),
        docker_cpus: env_non_empty("ANYAGENTS_DOCKER_CPUS"),
        e2b_domain: env_non_empty("E2B_DOMAIN"),
        e2b_envd_base_url: env_non_empty("ANYAGENTS_E2B_ENVD_URL"),
        modal_app: env_non_empty("ANYAGENTS_MODAL_APP"),
        modal_token_id: env_non_empty("MODAL_TOKEN_ID"),
        modal_token_secret: env_non_empty("MODAL_TOKEN_SECRET"),
        daytona_target: env_non_empty("DAYTONA_TARGET"),
        cloudflare_account_id: env_non_empty("CLOUDFLARE_ACCOUNT_ID"),
        env: BTreeMap::new(),
    })
}

fn merge_configs(
    base: &SandboxRegistryConfig,
    overlay: &SandboxRegistryConfig,
) -> SandboxRegistryConfig {
    let mut env = base.env.clone();
    env.extend(overlay.env.clone());
    SandboxRegistryConfig {
        provider: overlay.provider.clone().or_else(|| base.provider.clone()),
        endpoint: overlay.endpoint.clone().or_else(|| base.endpoint.clone()),
        token: overlay.token.clone().or_else(|| base.token.clone()),
        image: overlay.image.clone().or_else(|| base.image.clone()),
        template: overlay.template.clone().or_else(|| base.template.clone()),
        shape: overlay.shape.clone().or_else(|| base.shape.clone()),
        parent: overlay.parent.clone().or_else(|| base.parent.clone()),
        volume_size_gib: overlay.volume_size_gib.or(base.volume_size_gib),
        idle_ttl_ms: overlay.idle_ttl_ms.or(base.idle_ttl_ms),
        max_lease_ttl_ms: overlay.max_lease_ttl_ms.or(base.max_lease_ttl_ms),
        mount_path: overlay
            .mount_path
            .clone()
            .or_else(|| base.mount_path.clone()),
        workdir: overlay.workdir.clone().or_else(|| base.workdir.clone()),
        local_root: overlay
            .local_root
            .clone()
            .or_else(|| base.local_root.clone()),
        docker_host: overlay
            .docker_host
            .clone()
            .or_else(|| base.docker_host.clone()),
        docker_executable: overlay
            .docker_executable
            .clone()
            .or_else(|| base.docker_executable.clone()),
        docker_network: overlay
            .docker_network
            .clone()
            .or_else(|| base.docker_network.clone()),
        docker_memory: overlay
            .docker_memory
            .clone()
            .or_else(|| base.docker_memory.clone()),
        docker_cpus: overlay
            .docker_cpus
            .clone()
            .or_else(|| base.docker_cpus.clone()),
        e2b_domain: overlay
            .e2b_domain
            .clone()
            .or_else(|| base.e2b_domain.clone()),
        e2b_envd_base_url: overlay
            .e2b_envd_base_url
            .clone()
            .or_else(|| base.e2b_envd_base_url.clone()),
        modal_app: overlay.modal_app.clone().or_else(|| base.modal_app.clone()),
        modal_token_id: overlay
            .modal_token_id
            .clone()
            .or_else(|| base.modal_token_id.clone()),
        modal_token_secret: overlay
            .modal_token_secret
            .clone()
            .or_else(|| base.modal_token_secret.clone()),
        daytona_target: overlay
            .daytona_target
            .clone()
            .or_else(|| base.daytona_target.clone()),
        cloudflare_account_id: overlay
            .cloudflare_account_id
            .clone()
            .or_else(|| base.cloudflare_account_id.clone()),
        env,
    }
}

fn env_non_empty(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn clean_opt(opt: &Option<String>) -> Option<String> {
    opt.as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn parse_env_u64(name: &str) -> Result<Option<u64>, String> {
    match env_non_empty(name) {
        Some(raw) => raw
            .parse::<u64>()
            .map(Some)
            .map_err(|err| format!("invalid {name} value `{raw}`: {err}")),
        None => Ok(None),
    }
}

fn parse_env_u32(name: &str) -> Result<Option<u32>, String> {
    match env_non_empty(name) {
        Some(raw) => raw
            .parse::<u32>()
            .map(Some)
            .map_err(|err| format!("invalid {name} value `{raw}`: {err}")),
        None => Ok(None),
    }
}
