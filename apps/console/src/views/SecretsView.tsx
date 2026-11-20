import { useCallback, useState } from 'react';
import { KeyRound, Plus, ShieldCheck } from 'lucide-react';
import { agentPath, api, useResource } from '../api';
import { time } from '../format';
import type { Agent } from '../types';
import { ActionForm, Badge, Empty, Field, Heading, Panel, Problem, Refresh, Table, formText } from '../ui';

type Bundle = { name: string; env_keys?: string[]; granted_agents?: string[]; granted_agent_ids?: string[]; updated_at_ms?: number };
const loadBundles = (signal: AbortSignal) => api<{ secrets: Bundle[] }>('/v1/secrets', { signal });

export function GrantDialog({ agent, close, updated }: { agent: Agent; close: () => void; updated: () => void }) {
  const load = useCallback(async (signal: AbortSignal) => Promise.all([
    loadBundles(signal), api<{ secrets: string[] }>(agentPath(agent.name) + '/secrets', { signal }),
  ]), [agent.name]);
  const grants = useResource(load);
  return <ActionForm title={'Secret grants · ' + agent.name} action="Save grants" close={close} submit={async data => {
    if (!grants.data) throw new Error('Wait for the current grants to load before saving.');
    await api(agentPath(agent.name) + '/secrets', { method: 'PUT', body: { secrets: data.getAll('bundle') } });
    updated();
  }}><p className="muted">Select the full set of bundles this agent may access. Unchecked bundles are revoked.</p>
    {grants.error && <Problem retry={grants.refresh}>{grants.error}</Problem>}
    {!grants.data && !grants.error && <Empty>Loading grants…</Empty>}
    {grants.data?.[0].secrets.length === 0 && <Empty>No bundles to grant.</Empty>}
    {grants.data?.[0].secrets.map(bundle => <label key={bundle.name} className="check-field"><input type="checkbox" name="bundle" value={bundle.name} defaultChecked={grants.data?.[1].secrets.includes(bundle.name)} /><span>{bundle.name}</span></label>)}
  </ActionForm>;
}

export function SecretsView({ agents, openGrants, grantVersion }: { agents: Agent[]; openGrants: (agent: Agent) => void; grantVersion: number }) {
  const load = useCallback((signal: AbortSignal) => { void grantVersion; return loadBundles(signal); }, [grantVersion]);
  const secret = useResource(load);
  const [target, setTarget] = useState('');
  const [edit, setEdit] = useState<Bundle | 'new'>();
  const [remove, setRemove] = useState<Bundle>();
  const close = () => { setEdit(undefined); setRemove(undefined); };
  const active = agents.filter(agent => agent.state !== 'archived');
  const selected = active.find(agent => agent.id === target) ?? active[0];
  return <><Heading eyebrow="CONTROL PLANE / SECURITY" title="Secrets" blurb="Seal deployment credentials and grant access only to the agents that need them.">
      <Refresh onClick={secret.refresh} /><button className="button primary" onClick={() => setEdit('new')}><Plus size={16} /> Seal bundle</button>
    </Heading>
    {secret.error && <Problem retry={secret.refresh}>{secret.error}</Problem>}
    <div className="notice"><ShieldCheck size={18} /><div><strong>Write-only values</strong><p>Names and environment keys are visible here. Encrypted values cannot be read back through the console.</p></div></div>
    <Panel title="Deployment bundles" icon={<KeyRound size={16} />}>
      <Table headers={['Bundle', 'Environment keys', 'Granted agents', 'Updated', 'Actions']} empty={secret.data ? 'No deployment bundles sealed yet.' : 'Loading bundles…'} rows={(secret.data?.secrets ?? []).map(bundle => ({ key: bundle.name, cells: [
        <code>{bundle.name}</code>, <span className="chip-row">{bundle.env_keys?.map(key => <Badge key={key}>{key}</Badge>)}</span>,
        (bundle.granted_agents?.length ? bundle.granted_agents : bundle.granted_agent_ids)?.join(', ') || '—', time(bundle.updated_at_ms),
        <span className="cluster"><button className="button" onClick={() => setEdit(bundle)}>Replace</button><button className="button quiet" onClick={() => setRemove(bundle)}>Delete</button></span>,
      ] }))} />
    </Panel>
    <Panel title="Agent grants"><div className="panel-form"><Field label="Agent"><select value={selected?.id ?? ''} onChange={event => setTarget(event.target.value)}>{!active.length && <option value="">No agents available</option>}{active.map(agent => <option key={agent.id} value={agent.id}>{agent.name}</option>)}</select></Field>
      <button className="button" disabled={!selected} onClick={() => selected && openGrants(selected)}>Manage grants</button></div></Panel>
    {edit && <ActionForm title={edit === 'new' ? 'Seal a deployment bundle' : 'Replace ' + edit.name} action="Seal bundle" close={close} submit={async (data, node) => {
      const name = edit === 'new' ? formText(data, 'name') : edit.name;
      const raw = String(data.get('env') ?? '');
      const input = node.elements.namedItem('env');
      if (input instanceof HTMLTextAreaElement) input.value = '';
      const env = Object.create(null) as Record<string, string>;
      for (const line of raw.split(/\r?\n/)) {
        if (!line.trim() || line.trimStart().startsWith('#')) continue;
        const equals = line.indexOf('=');
        const key = line.slice(0, equals).trim();
        if (equals < 1 || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) throw new Error('Use one valid KEY=VALUE pair per line.');
        env[key] = line.slice(equals + 1);
      }
      if (!Object.keys(env).length) throw new Error('Enter at least one KEY=VALUE pair.');
      await api('/v1/secrets/' + encodeURIComponent(name), { method: 'PUT', body: { env } }); secret.refresh();
    }}>{edit === 'new' && <Field label="Bundle name"><input required name="name" pattern="[a-zA-Z0-9][a-zA-Z0-9._\-]{0,63}" autoFocus /></Field>}
      <Field label="Environment entries (KEY=VALUE)"><textarea required name="env" rows={5} autoComplete="off" spellCheck={false} placeholder="SERVICE_TOKEN=…" /></Field>
    </ActionForm>}
    {remove && <ActionForm title={'Delete ' + remove.name + '?'} action="Delete bundle" danger close={close} submit={async () => {
      await api('/v1/secrets/' + encodeURIComponent(remove.name), { method: 'DELETE' }); secret.refresh();
    }}><p>This removes the encrypted bundle and its grants. You cannot undo this action.</p></ActionForm>}
  </>;
}
