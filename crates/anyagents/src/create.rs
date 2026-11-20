//! `agents.create`. The agent row, its directories, and its MCP registration
//! are one step. The harness config is written only under the agent's own
//! config directory.

mod archetypes;
mod credentials;
mod profile_identity;
mod profile_rotation;
mod profiles;
mod quota;
mod quota_probe;
mod runtime;
mod signin;
mod spawn;
mod workspace;

#[cfg(test)]
mod archetype_tests;
#[cfg(test)]
mod runtime_config_tests;
#[cfg(test)]
mod tests;

pub use spawn::{CreateRequest, create, format_agent_display};

pub(crate) use archetypes::{
    DEFAULT_AGENT_ROLE, SubagentDefaultSpec, SubagentDefaultsConfig, ancestor_archetype_workdirs,
    apply_archetype_defaults, backfill_historical_agent_roles, discover_available_archetype_slugs,
    discover_available_archetype_templates, effective_agent_instructions, load_subagent_defaults,
    mark_instructions_rotated_if_changed, normalize_or_validate_agent_role,
    resolve_subagent_defaults, save_subagent_defaults,
};
pub(crate) use credentials::{
    propagate_profile_files_to_bound_agents, seed_harness_credentials, seeded_profile_name,
    sync_fresher_bound_agent_credentials, sync_fresher_bound_agent_credentials_in,
    write_profile_marker,
};
pub(crate) use profile_identity::{extract_profile_account_identity, update_profile_json_identity};
pub(crate) use profile_rotation::{force_refresh_provider_profiles, try_rotate_exhausted_profile};
pub(crate) use profiles::{
    ProviderProfileEntry, bound_profile_name, bound_profile_of_provider, compatible_harnesses_for,
    ensure_harness_runs_profile, ensure_profile_compatible_and_mapped, list_harness_profiles,
    list_provider_profiles_fast, set_provider_profile, sync_and_list_provider_profiles,
    sync_and_list_provider_profiles_with_options, upstream_provider_of,
};
pub(crate) use quota::{
    clear_stale_agy_quota_error_steps, enrich_profiles_usage, parse_remaining_pct_from_usage,
    probe_profile_models_catalog, probe_profile_remaining_pct, record_profile_quota_reset,
};
pub(crate) use quota_probe::probe_one_profile_usage;
pub(crate) use runtime::{
    compute_live_session_continuity, configure_agent_runtime, ensure_agy_mcp_config,
    harness_session_filename, maybe_notify_parent_on_error, poke_agent_wake_socket,
    poke_agent_wake_socket_mail, reconcile_agent_runtime_config,
    reconcile_agent_runtime_config_for_launch, reconcile_agent_runtime_config_for_live,
    reset_agent_session_locked, resolve_root_seat_hint, resolved_agent_seat_info,
    set_agent_harness, verify_harness_runtime_config,
};
pub(crate) use signin::{
    SigninStaging, deduplicate_same_account_profiles, finalize_agent_signin,
    resolve_and_commit_staged_signin,
};
pub(crate) use spawn::create_operator_root;
#[cfg(unix)]
pub(crate) use workspace::sync_canonical_skills;
pub(crate) use workspace::{
    chmod_seat_file_readable, ensure_seat_environment, ensure_seat_op_config,
    extract_file_tilde_refs, sync_agent_description_files, sync_agent_description_files_for_live,
    sync_all_agent_instructions, write_agent_instructions,
    write_agent_instructions_with_continuity_and_store, write_required_file,
    write_seat_shared_file,
};

#[cfg(test)]
pub(crate) use archetypes::{
    BUILTIN_GENERAL_ROLE_BODY, INSTRUCTIONS_ROTATED_MARKER, parse_archetype_markdown,
    resolve_archetype_template,
};
#[cfg(all(test, unix))]
pub(crate) use archetypes::{archetype_search_roots, resolve_archetype_template_with_ancestors};
#[cfg(test)]
pub(crate) use profiles::default_kind_and_harnesses;
#[cfg(test)]
pub(crate) use runtime::{agy_mcp_registered, mcp_server_args};
#[cfg(all(test, unix))]
pub(crate) use workspace::ensure_seat_skills;
#[cfg(test)]
pub(crate) use workspace::write_agent_instructions_with_continuity;

// Preserved `crate::create::*` API surface for items currently referenced only within
// their owning submodules.
#[allow(unused_imports)]
pub(crate) use archetypes::{
    ArchetypeTemplate, archetype_search_roots_with_ancestors,
    discover_available_archetype_slugs_with_ancestors,
    has_configured_archetype_roots_with_ancestors, resolve_role_template_for_agent,
};
#[allow(unused_imports)]
pub(crate) use credentials::sync_anthropic_agent_credentials_from_profile;
#[allow(unused_imports)]
pub(crate) use profiles::profile_compatible_harnesses;
#[allow(unused_imports)]
pub(crate) use quota::USAGE_LIMITED_PREFIX;
#[allow(unused_imports)]
pub(crate) use runtime::{build_harness_continuity_summary, ensure_agy_mcp_registration};
#[allow(unused_imports)]
pub(crate) use signin::finalize_profile_signin;
#[allow(unused_imports)]
pub(crate) use workspace::{data_dir_from_config, read_shared_agents_md};
