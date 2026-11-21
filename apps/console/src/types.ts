import type { AgentLike } from './lib/status';
import type { ProfileLike } from './lib/providers';

export interface Session {
  authenticated: boolean;
  mode: 'oidc' | 'unconfigured' | 'local_trusted';
  email?: string;
  name?: string;
  serverless: boolean;
}

export interface Agent extends AgentLike {
  parent?: string;
  lifecycle?: string;
  scope?: string;
  profile?: string;
  description?: string;
  process?: string;
  workdir?: string;
  open_turn_age_ms?: number;
  remote_budget_micros?: number;
  secrets?: string[];
  automations?: { total?: number; schedules?: number; watches?: number };
  created_at_ms?: number;
  live_model?: string;
  pending_switch?: boolean;
}

export interface SandboxLease {
  provider: string;
  lease_state: string;
  lease_id?: string;
  lease_expires_at_ms?: number;
  last_heartbeat_ms?: number;
}

export interface AgentMeta { unread_count?: number; sandbox_lease?: SandboxLease | null }

export interface RemoteTurn {
  turn_id: string; agent_id: string; agent_name?: string; harness_kind: string;
  provider_kind?: string; status: string; last_event_at_ms?: number;
}

export interface Overview {
  agents_meta?: Record<string, AgentMeta>;
  serverless?: boolean;
  sandbox_provider?: string;
  active_sandboxes_count?: number;
  timer_token_configured?: boolean;
  timer_jwks_configured?: boolean;
  timer_scheduler_configured?: boolean;
  active_remote_turns?: RemoteTurn[];
}

export interface Profile extends ProfileLike {
  account?: string;
  usage?: string;
  usage_age_ms?: number;
  quota_5h?: { remaining_pct: number; reset_eta?: string };
  quota_weekly?: { remaining_pct: number; reset_eta?: string };
}

export type UpgradeStatus = 'up_to_date' | 'outdated_idle' | 'outdated_draining' | 'cold_uses_latest';
export type UpdatePolicy = 'auto' | 'manual' | 'pinned';

/** `GET /admin/api/versions` (operator-only). Strings may be null on older or partial builds. */
export interface VersionsReport {
  coordinator: {
    version?: string | null; git_sha?: string | null; image_digest?: string | null;
    latest_available?: string | null; auto_update_policy?: UpdatePolicy;
  };
  runtime?: {
    desired_sandbox_image?: string | null; desired_image_digest?: string | null;
    desired_harness_versions?: Record<string, string | null>; tpl_base_version?: string | null; sbx_guest_version?: string | null;
  };
  agents?: {
    id: string; name: string; state?: string; has_open_turn?: boolean; harness?: string;
    running_image_digest?: string | null; desired_image_digest?: string | null; harness_version?: string | null;
    tpl_base_version?: string | null; sbx_guest_version?: string | null;
    upgrade_status: UpgradeStatus; drain_requested?: boolean;
  }[];
  summary?: { total: number; up_to_date: number; outdated: number; draining: number; cold: number };
}

export type DrainAction = 'recycled_now' | 'drain_scheduled' | 'already_up_to_date' | 'cold_next_turn_latest';

export interface Schedule {
  id: string; agent_id: string; agent_name?: string; kind: string; spec: string; state: string;
  prompt: string; next_fire_at_ms?: number | null; last_fired_at_ms?: number;
}

export type Route =
  | { page: 'overview' }
  | { page: 'agents'; id?: string }
  | { page: 'accounts' }
  | { page: 'knowledge' }
  | { page: 'versions' }
  | { page: 'issues' }
  | { page: 'settings'; tab: SettingsTab };
export type SettingsTab = 'channels' | 'ingress' | 'secrets' | 'automations' | 'usage';
export type Page = Route['page'];

export const harnesses = ['agy', 'claude-code', 'codex', 'cursor', 'muse', 'omp', 'opencode'];
export const runtimes = ['sandbox', 'local', 'cloud'];
