pub(super) fn is_untrusted_launch_env(k: &str) -> bool {
    k.starts_with("ANYAGENTS_S3_")
        || k.starts_with("ANYAGENTS_ADMIN_")
        || k.starts_with("ANYAGENTS_OIDC_")
        || k.starts_with("ANYAGENTS_CONTROL_")
        || k.starts_with("ANYAGENTS_AUTH_")
        || anyagents_harness_spiron::inference_key::is_env_key(k)
        || matches!(
            k,
            anyagents_harness::SEAT_LAUNCH_ENV
                | anyagents_harness::SEAT_PEERS_ENV
                | "ANYAGENTS_DATA"
                | anyagents_core::STATE_DIR_ENV
                | anyagents_core::WORKSPACES_DIR_ENV
                | anyagents_core::SCRATCH_DIR_ENV
                | anyagents_core::GIT_POOL_DIR_ENV
                | "ANYAGENTS_AGENT_CONFIG"
                | "ANYAGENTS_AGENT_UID"
                | "ANYAGENTS_OPERATOR_TOKEN"
                | "ANYAGENTS_OPERATOR_TOKEN_FILE"
                | "ANYAGENTS_CAPABILITY_KEY"
                | "ANYAGENTS_MASTER_KEY"
                | "ANYAGENTS_TICK_TOKEN"
                | "ANYAGENTS_TIMER_TOKEN"
                | "ANYAGENTS_TIMER_SCHEDULER_TOKEN"
                | "ANYAGENTS_DATABASE_URL"
                | "ANYAGENTS_DIRECT_DATABASE_URL"
                | "LD_PRELOAD"
                | "LD_LIBRARY_PATH"
        )
}
