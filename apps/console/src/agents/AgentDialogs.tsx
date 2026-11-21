import { For, Match, Switch } from 'solid-js';
import { agentPath, api, createPoll } from '../api';
import { agentHref } from '../route';
import { useConsole } from '../state';
import type { AgentAction } from '../state';
import { harnesses, runtimes } from '../types';
import { Empty, Field, FormDialog, Problem, formText as text } from '../ui/primitives';

function Options(props: { items: string[]; selected?: string }) {
  return <For each={props.items}>{item => <option value={item} selected={item === props.selected}>{item}</option>}</For>;
}

export function AgentDialogs(props: { action: AgentAction; close: () => void }) {
  const { refreshAll } = useConsole();
  const action = props.action;
  if (action.kind === 'spawn') return <FormDialog title={action.parent ? 'Spawn sub-agent' : 'Spawn agent'} wide
    description={action.parent ? <>The new agent reports to <span class="strong">{action.parent.name}</span>.</> : 'Create a new top-level agent.'}
    action="Spawn agent" close={props.close} submit={async data => {
      const body: Record<string, string | number> = {};
      for (const field of ['name', 'harness', 'runtime', 'model', 'profile', 'role', 'description', 'instructions', 'prompt']) {
        const value = text(data, field);
        if (value) body[field] = value;
      }
      if (!body.name) throw new Error('Enter an agent name.');
      if (action.parent) body.parent = action.parent.name;
      if (Number(text(data, 'budget')) > 0) body.budget_cap_micros = Math.round(Number(text(data, 'budget')) * 1e6);
      await api('/v1/agents', { method: 'POST', body });
      refreshAll();
      window.location.hash = agentHref(String(body.name));
    }}>
    <Field label="Name"><input class="input" required name="name" placeholder="release-verifier" autofocus autocomplete="off" /></Field>
    <div class="grid-2"><Field label="Harness"><select class="input" name="harness"><Options items={harnesses} selected="claude-code" /></select></Field>
      <Field label="Runtime"><select class="input" name="runtime"><Options items={runtimes} /></select></Field></div>
    <div class="grid-2"><Field label="Model"><input class="input" name="model" placeholder="Harness default" /></Field>
      <Field label="Profile" hint="Leave blank to pick automatically."><input class="input" name="profile" placeholder="Automatic" /></Field></div>
    <div class="grid-2"><Field label="Role"><input class="input" name="role" placeholder="Optional" /></Field>
      <Field label="Budget cap (USD)"><input class="input" name="budget" type="number" min="0" step="0.01" placeholder="No cap" /></Field></div>
    <Field label="Description"><input class="input" name="description" placeholder="Shown to other agents" /></Field>
    <Field label="Instructions"><textarea class="input" name="instructions" rows={2} /></Field>
    <Field label="First message"><textarea class="input" name="prompt" rows={2} /></Field>
  </FormDialog>;

  const { agent, kind } = action;
  const path = agentPath(agent.name);
  if (kind === 'grants') return <GrantDialog agentName={agent.name} close={props.close} updated={refreshAll} />;
  if (kind === 'runtime') {
    const currentRuntime = agent.runtime?.split(':')[0] || 'local';
    return <FormDialog title="Switch runtime or model" description={<>Applies to <span class="strong">{agent.name}</span> from its next turn.</>}
      action="Apply changes" close={props.close} submit={async data => {
        const harness = text(data, 'harness'), runtime = text(data, 'runtime'), cap = text(data, 'budget');
        const micros = cap ? Math.round(Number(cap) * 1e6) : null;
        const model = text(data, 'model') || null, profile = text(data, 'profile') || null;
        const clearBudget = micros === null && agent.remote_budget_micros != null;
        if (harness !== agent.harness || runtime !== currentRuntime || micros !== (agent.remote_budget_micros ?? null)) {
          await api(path + '/runtime', { method: 'POST', body: { harness,
            runtime: runtime === currentRuntime ? agent.runtime || runtime : runtime, model, profile,
            ...(micros === null ? {} : { budget_cap_micros: micros }), ...(clearBudget ? { clear_budget: true } : {}) } });
        } else await api(path + '/model', { method: 'POST', body: { harness, model, profile } });
        refreshAll();
      }}>
      <div class="grid-2"><Field label="Harness"><select class="input" name="harness"><Options items={[...new Set([...harnesses, agent.harness])]} selected={agent.harness} /></select></Field>
        <Field label="Runtime"><select class="input" name="runtime"><Options items={[...new Set([...runtimes, currentRuntime])]} selected={currentRuntime} /></select></Field></div>
      <Field label="Model"><input class="input" name="model" value={agent.model ?? ''} placeholder="Harness default" /></Field>
      <Field label="Profile"><input class="input" name="profile" value={agent.profile ?? ''} placeholder="Automatic" /></Field>
      <Field label="Budget cap (USD)" hint="Leave blank to remove the cap."><input class="input" type="number" name="budget" min="0" step="0.000001"
        value={agent.remote_budget_micros != null ? agent.remote_budget_micros / 1e6 : ''} /></Field>
    </FormDialog>;
  }
  const copy = {
    pause: { title: 'Pause ' + agent.name + '?', body: 'The agent takes no new work until it is woken. Queued messages stay in its mailbox.', action: 'Pause agent' },
    wake: { title: 'Wake ' + agent.name + '?', body: 'The coordinator restarts the agent and delivers any queued messages.', action: 'Wake agent' },
    archive: { title: 'Archive ' + agent.name + '?', body: 'The coordinator releases its sandbox compute. The agent and its history stay visible under archived agents.', action: 'Archive agent' },
  }[kind];
  return <FormDialog title={copy.title} description={copy.body} action={copy.action} danger={kind === 'archive'} close={props.close}
    submit={async () => { await api(path + '/' + kind, { method: 'POST' }); refreshAll(); }} />;
}

type Bundle = { name: string; env_keys?: string[] };
export function GrantDialog(props: { agentName: string; close: () => void; updated: () => void }) {
  const grants = createPoll(signal => Promise.all([
    api<{ secrets: Bundle[] }>('/v1/secrets', { signal }), api<{ secrets: string[] }>(agentPath(props.agentName) + '/secrets', { signal }),
  ]));
  return <FormDialog title="Secret grants" description={<>Choose every bundle <span class="strong">{props.agentName}</span> may read. Unchecked bundles are revoked.</>}
    action="Save grants" close={props.close} submit={async data => {
      if (!grants.data()) throw new Error('Wait for the current grants to load before saving.');
      await api(agentPath(props.agentName) + '/secrets', { method: 'PUT', body: { secrets: data.getAll('bundle') } });
      props.updated();
    }}>
    <Switch>
      <Match when={grants.error()}><Problem retry={grants.refresh}>{grants.error()}</Problem></Match>
      <Match when={!grants.data()}><p class="subtle">Loading grants…</p></Match>
      <Match when={grants.data()?.[0].secrets.length === 0}><Empty compact title="No bundles yet">Seal a bundle in Settings › Secrets first.</Empty></Match>
      <Match when={grants.data()}>{data => <div class="check-list"><For each={data()[0].secrets}>{bundle =>
        <label class="check-row"><input type="checkbox" name="bundle" value={bundle.name} checked={data()[1].secrets.includes(bundle.name)} />
          <span class="mono strong">{bundle.name}</span><span class="subtle mono">{bundle.env_keys?.join(', ')}</span></label>}</For></div>}</Match>
    </Switch>
  </FormDialog>;
}
