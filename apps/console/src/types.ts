export interface Session {
  authenticated: boolean;
  mode: 'oidc' | 'unconfigured' | 'local_trusted';
  email?: string;
  name?: string;
  serverless: boolean;
}

export interface Agent {
  id: string;
  name: string;
  parent?: string;
  state: string;
  role?: string;
  lifecycle?: string;
  scope?: string;
  harness: string;
  runtime?: string;
  model?: string;
  profile?: string;
  description?: string;
  process?: string;
  workdir?: string;
  last_error?: string;
  turn_state?: string;
  turn_errors?: { faulted_at_ms?: number; last_error?: string };
  has_open_turn?: boolean;
  open_turn_age_ms?: number;
  remote_budget_micros?: number;
  secrets?: string[];
  automations?: { total?: number };
}

export interface AgentMeta {
  unread_count?: number;
  sandbox_lease?: { provider: string; lease_state: string; lease_id?: string };
}

export interface Overview {
  agents_meta?: Record<string, AgentMeta>;
  serverless?: boolean;
  sandbox_provider?: string;
  active_sandboxes_count?: number;
  timer_token_configured?: boolean;
  timer_jwks_configured?: boolean;
  timer_scheduler_configured?: boolean;
  active_remote_turns?: { turn_id: string; agent_id: string; agent_name?: string;
    harness_kind: string; provider_kind?: string; status: string; last_event_at_ms?: number }[];
}

export const states = ['all', 'working', 'idle', 'paused', 'errored', 'archived'] as const;
export type AgentState = Exclude<typeof states[number], 'all'>;
export function status(agent: Agent): AgentState {
  if (agent.state === 'archived') return 'archived';
  if (agent.last_error || agent.turn_errors?.faulted_at_ms || ['faulted', 'errored'].includes(agent.turn_state ?? '')) return 'errored';
  if (agent.state === 'paused') return 'paused';
  return agent.has_open_turn || ['working', 'in_turn'].includes(agent.turn_state ?? '') ? 'working' : 'idle';
}

export const harnesses = ['agy', 'claude-code', 'codex', 'cursor', 'muse', 'omp', 'opencode'];
export const runtimes = ['sandbox', 'local', 'cloud'];
