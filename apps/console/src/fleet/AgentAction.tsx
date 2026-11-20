import { ActionForm, Field, Options, formText as text } from '../ui';
import { agentPath, api } from '../api';
import { harnesses, runtimes } from '../types';
import type { Agent } from '../types';

export type FleetAction = { kind: 'spawn'; parent?: Agent } | { kind: 'runtime' | 'pause' | 'wake' | 'archive'; agent: Agent };
export function AgentAction({ action, close, updated, spawned }: {
  action: FleetAction; close: () => void; updated: () => void; spawned: (id: string) => void;
}) {
  if (action.kind === 'spawn') return <ActionForm title={action.parent ? 'Spawn a child agent' : 'Spawn an agent'} action="Spawn agent" close={close} submit={async data => {
    const body: Record<string, string | number> = {};
    for (const field of ['name', 'harness', 'runtime', 'model', 'profile', 'role', 'description', 'instructions', 'prompt']) {
      const value = text(data, field);
      if (value) body[field] = value;
    }
    if (!body.name) throw new Error('Enter an agent name.');
    if (action.parent) body.parent = action.parent.name;
    if (Number(text(data, 'budget')) > 0) body.budget_cap_micros = Math.round(Number(text(data, 'budget')) * 1e6);
    const result = await api<{ id?: string }>('/v1/agents', { method: 'POST', body });
    spawned(result?.id ?? String(body.name)); updated();
  }}>
    {action.parent && <p className="muted">Parent: <strong>{action.parent.name}</strong></p>}
    <Field label="Agent name"><input required name="name" placeholder="e.g. release-verifier" autoFocus /></Field>
    <div className="two-cols"><Field label="Harness"><select name="harness" defaultValue="claude-code"><Options items={harnesses} /></select></Field>
    <Field label="Runtime"><select name="runtime"><Options items={runtimes} /></select></Field></div>
    <div className="two-cols"><Field label="Model"><input name="model" placeholder="Default model" /></Field><Field label="Profile"><input name="profile" placeholder="Automatic" /></Field></div>
    <div className="two-cols"><Field label="Role"><input name="role" placeholder="Optional" /></Field><Field label="Budget cap (USD)"><input name="budget" type="number" min="0" step="0.01" /></Field></div>
    <Field label="Description"><input name="description" /></Field>
    <Field label="Instructions"><textarea name="instructions" rows={2} /></Field>
    <Field label="Initial message"><textarea name="prompt" rows={2} /></Field>
  </ActionForm>;
  const { agent, kind } = action;
  const path = agentPath(agent.name);
  if (kind === 'runtime') {
    const currentRuntime = agent.runtime?.split(':')[0] || 'local';
    return <ActionForm title="Switch runtime" action="Apply changes" close={close} submit={async data => {
      const harness = text(data, 'harness'), runtime = text(data, 'runtime'), cap = text(data, 'budget');
      const micros = cap ? Math.round(Number(cap) * 1e6) : null;
      const model = text(data, 'model') || null, profile = text(data, 'profile') || null;
      const clearBudget = micros === null && agent.remote_budget_micros != null;
      if (harness !== agent.harness || runtime !== currentRuntime || micros !== (agent.remote_budget_micros ?? null)) {
        await api(path + '/runtime', { method: 'POST', body: { harness,
          runtime: runtime === currentRuntime ? agent.runtime || runtime : runtime, model, profile,
          ...(micros === null ? {} : { budget_cap_micros: micros }), ...(clearBudget ? { clear_budget: true } : {}) } });
      } else await api(path + '/model', { method: 'POST', body: { harness, model, profile } });
      updated();
    }}><p className="muted">Update the execution settings for <strong>{agent.name}</strong>.</p>
      <div className="two-cols"><Field label="Harness"><select name="harness" defaultValue={agent.harness}><Options items={[...new Set([...harnesses, agent.harness])]} /></select></Field>
      <Field label="Runtime"><select name="runtime" defaultValue={currentRuntime}><Options items={[...new Set([...runtimes, currentRuntime])]} /></select></Field></div>
      <Field label="Model"><input name="model" defaultValue={agent.model} placeholder="Default model" /></Field>
      <Field label="Profile"><input name="profile" defaultValue={agent.profile} placeholder="Automatic" /></Field>
      <Field label="Budget cap (USD)"><input type="number" name="budget" min="0" step="0.000001" defaultValue={agent.remote_budget_micros != null ? agent.remote_budget_micros / 1e6 : ''} placeholder="Blank clears cap" /></Field>
    </ActionForm>;
  }
  const label = kind[0].toUpperCase() + kind.slice(1);
  const detail = { pause: 'Pause new work for this agent until it is woken.', wake: 'Request that the coordinator wake this agent.', archive: 'The coordinator will release its sandbox compute. The agent remains visible in the archived fleet.' }[kind];
  return <ActionForm title={label + ' ' + agent.name + '?'} action={label + ' agent'} close={close} danger={kind === 'archive'} submit={async () => {
    await api(path + '/' + kind, { method: 'POST' }); updated();
  }}><p className="muted">{detail}</p></ActionForm>;
}
