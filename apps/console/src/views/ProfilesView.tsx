import { useCallback, useState } from 'react';
import { CircleAlert, CircleCheck, Gauge, Layers, Plus, RefreshCw, Zap } from 'lucide-react';
import { api, useResource } from '../api';
import { ActionForm, Badge, Empty, Field, Heading, Options, Panel, Problem, Refresh, formText } from '../ui';
import { NativeDialog, nativeProviders } from './NativeDialog';
import { LOW_QUOTA, quotaState } from './profileQuota';

type Profile = { provider: string; profile: string; kind: string; account?: string; usage?: string; usage_age_ms?: number; compatible_harnesses?: string[]; assigned_agents?: string[]; subagent_count?: number };
type DialogState = { kind: 'key' } | { kind: 'native'; initialProvider?: string };
const presets = ['deepseek', 'glm-code-plan', 'google-gemini', 'kimi-code-plan', 'minimax-code-plan', 'openrouter', 'qwen-token-plan', 'custom'];
const metrics = [
  { key: 'all', label: 'Total', icon: Layers },
  { key: 'healthy', label: 'Healthy', icon: CircleCheck },
  { key: 'expired', label: 'OAuth Expired', icon: CircleAlert },
  { key: 'low', label: 'Low / Zero Quota', icon: Gauge },
] as const;

export function ProfilesView() {
  const [live, setLive] = useState(false);
  const load = useCallback((signal: AbortSignal) => api<Profile[]>('/v1/profiles' + (live ? '?fresh=1' : ''), { signal }), [live]);
  const profiles = useResource(load);
  const [dialog, setDialog] = useState<DialogState>();
  const [provider, setProvider] = useState(presets[0]);
  const [filter, setFilter] = useState<typeof metrics[number]['key']>('all');
  const list = (profiles.data ?? []).map(p => ({ ...p, ...quotaState(p.usage) }));
  const groups = new Map<string, typeof list>();
  for (const profile of list) {
    if (filter !== 'all' && !profile[filter]) continue;
    const group = groups.get(profile.provider);
    if (group) group.push(profile);
    else groups.set(profile.provider, [profile]);
  }
  const fresh = () => { setLive(true); profiles.refresh(); };
  return <><Heading eyebrow="CONTROL PLANE / CAPACITY" title="Profiles & quota" blurb="Connect provider accounts and see what capacity is available.">
      <Refresh onClick={() => { setLive(false); profiles.refresh(); }} />
      <button className="button" onClick={fresh}><Zap size={15} /> Live quota</button>
      <button className="button" onClick={() => { setProvider(presets[0]); setDialog({ kind: 'key' }); }}>Add API key</button>
      <button className="button primary" onClick={() => setDialog({ kind: 'native' })}><Plus size={16} /> Connect Account</button>
    </Heading>
    {profiles.error && <Problem retry={profiles.refresh}>{profiles.error}</Problem>}
    <section className="profile-pulse" aria-label="Profile filters">{metrics.map(({ key, label, icon: Icon }) =>
      <button key={key} className={'pulse-card profile-metric ' + key} type="button" aria-pressed={key === filter} onClick={() => setFilter(key)}>
        <span className="pulse-label"><Icon size={15} /> {label}</span>
        <strong>{profiles.data ? key === 'all' ? list.length : list.filter(p => p[key]).length : '—'}</strong>
      </button>)}</section>
    {!groups.size && <Empty>{list.length ? 'No profiles match this filter.' : profiles.data ? 'Connect an account or add an API key to get started.' : profiles.error ? 'Profiles unavailable.' : 'Loading profiles…'}</Empty>}
    {[...groups].sort(([a], [b]) => a.localeCompare(b)).map(([name, group]) => <section key={name} className="profile-group" aria-label={name}>
      <header><h2>{name}</h2><Badge>{group.length} {group.length === 1 ? 'profile' : 'profiles'}</Badge></header>
      <div className="profile-grid">{group.sort((a, b) => Number(b.expired) - Number(a.expired) || Number(b.low) - Number(a.low) || a.profile.localeCompare(b.profile)).map(p => {
        const subscription = p.kind === 'subscription' || p.kind === 'oauth';
        const supported = nativeProviders.some(provider => provider.id === p.provider);
        return <div key={p.profile} className={'profile-card' + (p.expired ? ' expired' : '')}><Panel title={p.profile} actions={<Badge>{p.kind}</Badge>}>
          <div className="profile-content">
            <div className="profile-context"><p>{p.account || 'No account label'}</p>{p.usage_age_ms != null && <small className="muted">Checked {Math.round(p.usage_age_ms / 1000)}s ago</small>}</div>
            {(p.expired || p.low || p.healthy) && <div className="chip-row">
              {p.expired && <Badge tone="danger">OAuth expired</Badge>}
              {p.low && <Badge tone="warning">{p.windows.some(q => q.left === 0) ? 'Quota exhausted' : 'Low quota'}</Badge>}
              {p.healthy && <Badge tone="success">Healthy</Badge>}
            </div>}
            {p.windows.length ? p.windows.map((q, index) => <div key={index} className={'quota' + (q.left <= LOW_QUOTA ? ' low' : '')}>
              <span>{q.label}</span><strong>{q.left}% left</strong><progress max={100} value={q.left} aria-label={q.label + ' remaining quota'} />{q.detail && <small className="muted">{q.detail}</small>}
            </div>) : !p.expired && <p className="muted">{p.usage || 'No quota snapshot available'}</p>}
            <div className="chip-row">{p.compatible_harnesses?.map(h => <Badge key={h}>{h}</Badge>)}</div>
            <div className="muted">Assigned roots: {p.assigned_agents?.join(', ') || 'none'} · Subagents: {p.subagent_count ?? 0}</div>
            {subscription && <div className="profile-card-action"><button className={'button' + (p.expired ? ' danger' : '')} type="button"
              aria-label={'Refresh ' + p.provider + '/' + p.profile} onClick={() => setDialog({ kind: 'native', initialProvider: p.provider })}>
              <RefreshCw size={14} /> Refresh</button>{!supported && <small className="muted">Native sign-in unavailable</small>}</div>}
          </div>
        </Panel></div>;
      })}</div>
    </section>)}
    {dialog?.kind === 'key' && <ActionForm title="Add an API key profile" action="Save profile" close={() => setDialog(undefined)} submit={async (form, node) => {
      const chosen = provider === 'custom' ? formText(form, 'custom') : provider;
      const key = formText(form, 'key'); const input = node.elements.namedItem('key');
      if (input instanceof HTMLInputElement) input.value = '';
      await api('/v1/profiles', { method: 'POST', body: { profile: chosen + '/' + formText(form, 'profile'), key } }); fresh();
    }}><Field label="Provider"><select value={provider} onChange={event => setProvider(event.target.value)}><Options items={presets} /></select></Field>
      {provider === 'custom' && <Field label="Custom provider"><input required name="custom" /></Field>}
      <Field label="Profile name"><input required name="profile" defaultValue="default" /></Field>
      <Field label="API key"><input required name="key" type="password" autoComplete="off" /></Field>
    </ActionForm>}
    {dialog?.kind === 'native' && <NativeDialog initialProvider={dialog.initialProvider} close={() => setDialog(undefined)} updated={fresh} />}
  </>;
}
