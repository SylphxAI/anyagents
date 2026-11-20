import { useCallback, useEffect, useRef, useState } from 'react';
import type { FormEvent } from 'react';
import { ExternalLink, KeyRound, Plus, Zap } from 'lucide-react';
import { api, errorMessage, request, useResource } from '../api';
import { ActionForm, Badge, CopyButton, Dialog, Empty, Field, Heading, Metrics, Options, Panel, Problem, Refresh, formText } from '../ui';

type Profile = { provider: string; profile: string; kind: string; account?: string; usage?: string; usage_age_ms?: number; compatible_harnesses?: string[]; assigned_agents?: string[]; subagent_count?: number };
const presets = ['deepseek', 'glm-code-plan', 'google-gemini', 'kimi-code-plan', 'minimax-code-plan', 'openrouter', 'qwen-token-plan', 'custom'];
function quotas(usage = '') {
  return Array.from(usage.matchAll(/\b(5h|7d|3p|daily|weekly|monthly|quota)\s*:\s*(\d{1,3})%\s*(left|used)?([^,;]*)/gi), ([, label, count, mode, detail]) => {
    const percent = Math.min(100, Number(count));
    return { label, left: mode?.toLowerCase() === 'used' ? 100 - percent : percent, detail: detail.trim() };
  });
}
export function ProfilesView() {
  const [live, setLive] = useState(false);
  const load = useCallback((signal: AbortSignal) => api<Profile[]>('/v1/profiles' + (live ? '?fresh=1' : ''), { signal }), [live]);
  const profiles = useResource(load);
  const [dialog, setDialog] = useState<'key' | 'native'>();
  const [provider, setProvider] = useState(presets[0]);
  const list = profiles.data ?? [];
  const constrained = list.filter(p => quotas(p.usage).some(q => q.left <= 35)).length;
  const fresh = () => { setLive(true); profiles.refresh(); };
  return <><Heading eyebrow="CONTROL PLANE / CAPACITY" title="Profiles & quota" blurb="Manage provider credentials, harness sign-ins, and available quota.">
      <Refresh onClick={() => { setLive(false); profiles.refresh(); }} /><button className="button" onClick={fresh}><Zap size={15} /> Live quota</button>
      <button className="button primary" onClick={() => { setProvider(presets[0]); setDialog('key'); }}><Plus size={16} /> Add profile</button>
      <button className="button" onClick={() => setDialog('native')}>Native sign-in</button>
    </Heading>
    {profiles.error && <Problem retry={profiles.refresh}>{profiles.error}</Problem>}
    <Metrics items={[{ label: 'Configured profiles', value: list.length }, { label: 'Constrained quota', value: constrained },
      { label: 'Subscription seats', value: list.filter(p => p.kind === 'subscription').length }]} />
    {!list.length && <Empty>{profiles.data ? 'Add a profile to connect a provider.' : 'Loading profiles…'}</Empty>}
    <div className="profile-grid">{list.map(p => <Panel key={p.provider + '/' + p.profile} title={p.provider + '/' + p.profile} icon={<KeyRound size={16} />} actions={<Badge>{p.kind}</Badge>}>
      <div className="profile-content"><div className="muted">{p.account || 'No account label'}{p.usage_age_ms != null && ' · checked ' + Math.round(p.usage_age_ms / 1000) + 's ago'}</div>
        {quotas(p.usage).length ? quotas(p.usage).map(q => <div key={q.label} className="quota"><span>{q.label}</span><strong>{q.left}% left</strong><progress max={100} value={q.left} aria-label={q.label + ' remaining quota'} />{q.detail && <small>{q.detail}</small>}</div>) : <p>{p.usage || 'No quota snapshot available'}</p>}
        <div className="chip-row">{p.compatible_harnesses?.map(h => <Badge key={h}>{h}</Badge>)}</div>
        <div className="muted">Assigned roots: {p.assigned_agents?.join(', ') || 'none'} · Subagents: {p.subagent_count ?? 0}</div>
      </div></Panel>)}</div>
    {dialog === 'key' && <ActionForm title="Add an API key profile" action="Save profile" close={() => setDialog(undefined)} submit={async (form, node) => {
      const chosen = provider === 'custom' ? formText(form, 'custom') : provider;
      const key = formText(form, 'key'); const input = node.elements.namedItem('key');
      if (input instanceof HTMLInputElement) input.value = '';
      await api('/v1/profiles', { method: 'POST', body: { profile: chosen + '/' + formText(form, 'profile'), key } }); fresh();
    }}><Field label="Provider"><select value={provider} onChange={event => setProvider(event.target.value)}><Options items={presets} /></select></Field>
      {provider === 'custom' && <Field label="Custom provider"><input required name="custom" /></Field>}
      <Field label="Profile name"><input required name="profile" defaultValue="default" /></Field>
      <Field label="API key"><input required name="key" type="password" autoComplete="off" /></Field>
    </ActionForm>}
    {dialog === 'native' && <NativeDialog close={() => setDialog(undefined)} updated={fresh} />}
  </>;
}

type NativeEvent = { kind: string; text?: string };
function safeLink(value?: string) {
  try { return value && /^https?:$/.test(new URL(value).protocol) ? value : undefined; } catch { return undefined; }
}
function NativeDialog({ close, updated }: { close: () => void; updated: () => void }) {
  const controller = useRef<AbortController | null>(null);
  const [busy, setBusy] = useState(false);
  const [events, setEvents] = useState<NativeEvent[]>([]);
  const [error, setError] = useState('');
  useEffect(() => () => controller.current?.abort(), []);
  async function start(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (controller.current) return;
    const form = new FormData(event.currentTarget);
    const run = new AbortController(); controller.current = run;
    setEvents([]); setError(''); setBusy(true);
    let terminal = false;
    function handle(line: string) {
      if (!line.trim() || run.signal.aborted) return;
      let message: NativeEvent;
      try { message = JSON.parse(line) as NativeEvent; } catch { message = { kind: 'prompt', text: line }; }
      setEvents(previous => [...previous, message].slice(-100));
      if (message.kind === 'done') { terminal = true; updated(); }
      if (message.kind === 'error') { terminal = true; setError(message.text || 'Native sign-in failed.'); }
    }
    try {
      const response = await request('/v1/profiles/native-signin', { method: 'POST', signal: run.signal, accept: 'application/x-ndjson', body: { harness: formText(form, 'harness'), profile: formText(form, 'profile') } });
      if (!response.body) throw new Error('Sign-in stream unavailable.');
      const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
      let tail = '';
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        const lines = (tail + value).split('\n'); tail = lines.pop() ?? '';
        for (const line of lines) handle(line);
      }
      handle(tail);
      if (!terminal && !run.signal.aborted) throw new Error('The sign-in stream ended before confirmation.');
    } catch (cause) { if (!run.signal.aborted) setError(errorMessage(cause)); }
    finally { controller.current = null; setBusy(false); }
  }
  return <Dialog title="Native subscription sign-in" close={close}>
    <form className="dialog-body" onSubmit={start}>
      <p className="muted">Start the harness login, then follow the sign-in link and device instructions here.</p>
      <fieldset className="form-fields" disabled={busy}><div className="two-cols"><Field label="Harness"><select name="harness"><Options items={['agy', 'claude-code', 'codex']} /></select></Field><Field label="Profile name"><input required name="profile" defaultValue="default" /></Field></div></fieldset>
      {events.length > 0 && <div className="native-events" aria-live="polite">{events.map((event, index) => {
        const link = event.kind === 'link' ? safeLink(event.text) : undefined;
        return <div key={index}>{link ? <a className="button" href={link} target="_blank" rel="noopener noreferrer"><ExternalLink size={15} /> Open sign-in page</a> : event.kind === 'code' && event.text ? <span className="cluster"><code>{event.text}</code><CopyButton value={event.text} label="Copy device code" /></span> : event.kind === 'done' ? 'Signed in successfully.' : event.text}</div>;
      })}</div>}
      {error && <Problem>{error}</Problem>}
      <div className="dialog-footer">{busy && <button type="button" className="button" onClick={() => controller.current?.abort()}>Cancel sign-in</button>}<button className="button primary" disabled={busy}>{busy ? 'Waiting for sign-in…' : 'Start native sign-in'}</button></div>
    </form>
  </Dialog>;
}
