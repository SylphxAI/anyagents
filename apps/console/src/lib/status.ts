// Pure agent-state helpers shared by the tree, overview, and palette.

export interface AgentLike {
  id: string;
  name: string;
  parent?: string | null;
  state: string;
  role?: string | null;
  harness: string;
  runtime?: string | null;
  model?: string | null;
  last_error?: string | null;
  turn_state?: string | null;
  turn_errors?: { faulted_at_ms?: number; last_error?: string } | null;
  has_open_turn?: boolean;
}

export type AgentStatus = 'working' | 'idle' | 'paused' | 'errored' | 'archived';

export function agentStatus(agent: AgentLike): AgentStatus {
  if (agent.state === 'archived') return 'archived';
  if (agent.last_error || agent.turn_errors?.faulted_at_ms || ['faulted', 'errored'].includes(agent.turn_state ?? '')) return 'errored';
  if (agent.state === 'paused') return 'paused';
  return agent.has_open_turn || ['working', 'in_turn'].includes(agent.turn_state ?? '') ? 'working' : 'idle';
}

export const statusLabel: Record<AgentStatus, string> = {
  working: 'Working', idle: 'Idle', paused: 'Paused', errored: 'Errored', archived: 'Archived',
};

export const statusTone: Record<AgentStatus, 'ok' | 'neutral' | 'warn' | 'bad' | 'muted'> = {
  working: 'ok', idle: 'neutral', paused: 'warn', errored: 'bad', archived: 'muted',
};

/** Compact model label: drops vendor prefixes and date stamps. */
export function shortModel(model?: string | null) {
  if (!model) return '';
  const tail = model.split('/').pop() ?? model;
  return tail.replace(/-\d{8}$/, '').replace(/-latest$/, '');
}
