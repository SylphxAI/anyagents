import { For, Show, createSignal } from 'solid-js';
import { CalendarClock, Inbox, Play, Plus } from 'lucide-solid';
import { agentPath, api, createPoll, errorMessage } from '../api';
import { dateTime, duration, number } from '../format';
import { agentHref } from '../route';
import { useConsole } from '../state';
import type { Schedule } from '../types';
import { Empty, Field, FormDialog, Pill, Problem, Section, Skeleton, StatStrip, formText } from '../ui/primitives';

interface HeldDelivery { key: string; agent_id: string; destination: string; since_ms?: number; error?: string; excerpt: string }
interface TickSummary { schedules_fired: number; cloud_turns_polled: number; turns_settled: number; stale_open_turns_cleared?: number; backlog_dispatched?: number; leases_renewed: number; sandboxes_swept: number }
type Trigger = 'at' | 'every' | 'watch';

function nextFire(ms?: number | null) {
  if (!ms) return '—';
  const delta = ms - Date.now();
  return delta > 0 && delta < 86_400_000 ? 'in ' + duration(delta) : dateTime(ms);
}

export function AutomationsSettings() {
  const { fleet, overview, refreshAll } = useConsole();
  const [scope, setScope] = createSignal('all');
  const [creating, setCreating] = createSignal(false);
  const [cancel, setCancel] = createSignal<{ owner: string; schedule: Schedule }>();
  const [notice, setNotice] = createSignal('');
  const [tick, setTick] = createSignal<{ pending?: boolean; summary?: TickSummary; error?: string }>({});
  const [delivery, setDelivery] = createSignal<{ pending?: string; error?: string }>({});
  const targets = () => fleet().live.filter(agent => agent.name.toLowerCase() !== 'all');
  const target = () => targets().some(agent => agent.name === scope()) ? scope() : 'all';
  const schedules = createPoll(signal => api<{ agent: string; schedules: Schedule[] }>(agentPath(target()) + '/schedules', { signal }), { interval: 30000, key: target });
  const outbound = createPoll(signal => api<{ unsettled: HeldDelivery[] }>('/v1/outbound', { signal }), { interval: 30000 });
  const status = () => overview.data();
  const list = () => schedules.data()?.schedules ?? [];
  const held = () => outbound.data()?.unsettled ?? [];
  const nameOf = (id: string) => fleet().agents.find(agent => agent.id === id)?.name ?? id;
  const timerAuth = () => [status()?.timer_jwks_configured && 'JWKS', status()?.timer_token_configured && 'timer token'].filter(Boolean).join(' + ');

  async function runTick() {
    if (tick().pending) return;
    setTick(previous => ({ summary: previous.summary, pending: true }));
    try { const summary = await api<TickSummary>('/admin/api/tick', { method: 'POST' }); setTick({ summary }); refreshAll(); schedules.refresh(); outbound.refresh(); }
    catch (cause) { setTick(previous => ({ summary: previous.summary, error: errorMessage(cause) })); }
  }
  async function settle(key: string, action: 'retry' | 'discard') {
    setDelivery({ pending: key });
    try { await api('/v1/outbound/' + encodeURIComponent(key) + '/' + action, { method: 'POST' }); setDelivery({}); outbound.refresh(); }
    catch (cause) { setDelivery({ error: errorMessage(cause) }); }
  }

  return <>
    <StatStrip label="Coordinator" items={[
      { label: 'Mode', value: status() ? (status()!.serverless ? 'Serverless' : 'Local') : '—', detail: 'Sandbox: ' + (status()?.sandbox_provider || 'local') },
      { label: 'Ready sandboxes', value: number(status()?.active_sandboxes_count ?? 0) },
      { label: 'Remote turns', value: number(status()?.active_remote_turns?.length ?? 0), detail: 'In flight on cloud runtimes' },
      { label: 'Timer auth', value: timerAuth() ? 'Configured' : 'Not set', tone: timerAuth() ? 'ok' : undefined, detail: timerAuth() || 'Fire endpoint accepts no tokens' },
    ]} />
    <Section title="Maintenance tick" actions={<button type="button" class="btn sm" disabled={tick().pending} onClick={() => void runTick()}><Play size={13} />{tick().pending ? 'Running…' : 'Run tick now'}</button>}>
      <Show when={tick().error}><Problem>{tick().error}</Problem></Show>
      <Show when={tick().summary} fallback={<p class="subtle">The coordinator runs this pass on its own schedule. Run it now to fire due schedules and settle cloud turns immediately.</p>}>{summary =>
        <p class="tick-summary num">Fired {summary().schedules_fired} schedules · polled {summary().cloud_turns_polled} cloud turns · settled {summary().turns_settled} turns ·
          renewed {summary().leases_renewed} leases · swept {summary().sandboxes_swept} sandboxes · cleared {summary().stale_open_turns_cleared ?? 0} stale turns.</p>}</Show>
    </Section>
    <Section title="Schedules" meta={<Show when={schedules.data()}><span class="num subtle">{list().length}</span></Show>} actions={<>
      <select class="input sm" onChange={event => setScope(event.currentTarget.value)} aria-label="Schedule owner">
        <option value="all" selected={target() === 'all'}>All agents</option><For each={targets()}>{agent => <option value={agent.name} selected={agent.name === target()}>{agent.name}</option>}</For></select>
      <button type="button" class="btn primary sm" disabled={!targets().length} onClick={() => setCreating(true)}><Plus size={13} />New schedule</button></>}>
      <Show when={notice()}><p class="tone-ok small" role="status">{notice()}</p></Show>
      <Show when={schedules.error()}><Problem retry={schedules.refresh}>{schedules.error()}</Problem></Show>
      <Show when={schedules.loading()}><Skeleton rows={4} /></Show>
      <Show when={schedules.data()}><Show when={list().length} fallback={<Empty compact icon={<CalendarClock size={18} />} title="No schedules">One-time timers, recurring prompts, and watches appear here.</Empty>}>
        <div class="table-wrap"><table class="table">
          <thead><tr><th>Agent</th><th>Kind</th><th>Trigger</th><th>Status</th><th>Next fire</th><th>Prompt</th><th class="right"><span class="sr-only">Actions</span></th></tr></thead>
          <tbody><For each={list()}>{schedule => {
            const owner = schedule.agent_name || (target() === 'all' ? nameOf(schedule.agent_id) : target());
            return <tr>
              <td><a class="strong" href={agentHref(owner)}>{owner}</a></td>
              <td><span class="tag">{schedule.kind}</span></td>
              <td class="mono">{schedule.spec}</td>
              <td><Pill tone={schedule.state === 'active' ? 'ok' : 'neutral'} dot>{schedule.state}</Pill></td>
              <td class="num subtle nowrap">{nextFire(schedule.next_fire_at_ms)}</td>
              <td class="cell-wrap subtle">{schedule.prompt}</td>
              <td class="right"><Show when={schedule.state === 'active'}><button type="button" class="btn sm ghost" onClick={() => setCancel({ owner, schedule })}>Cancel</button></Show></td>
            </tr>;
          }}</For></tbody></table></div>
      </Show></Show>
    </Section>
    <Section title="Held outbound deliveries" meta={<Show when={outbound.data()}><Pill tone={held().length ? 'warn' : 'neutral'}>{held().length}</Pill></Show>}>
      <Show when={delivery().error}><Problem>{delivery().error}</Problem></Show>
      <Show when={outbound.error()}><Problem retry={outbound.refresh}>{outbound.error()}</Problem></Show>
      <Show when={outbound.loading()}><Skeleton rows={2} /></Show>
      <Show when={outbound.data()}><Show when={held().length} fallback={<Empty compact icon={<Inbox size={18} />} title="Outbound queue is clear">Messages that fail to reach a channel wait here for retry or discard.</Empty>}>
        <div class="table-wrap"><table class="table">
          <thead><tr><th>Agent</th><th>Destination</th><th>Held since</th><th>Error</th><th>Excerpt</th><th class="right"><span class="sr-only">Actions</span></th></tr></thead>
          <tbody><For each={held()}>{item => <tr>
            <td><a class="strong" href={agentHref(nameOf(item.agent_id))}>{nameOf(item.agent_id)}</a></td>
            <td class="mono">{item.destination}</td>
            <td class="num subtle nowrap">{dateTime(item.since_ms)}</td>
            <td class="tone-bad cell-wrap">{item.error || '—'}</td>
            <td class="cell-wrap subtle">{item.excerpt}</td>
            <td class="right nowrap"><button type="button" class="btn sm" disabled={Boolean(delivery().pending)} onClick={() => void settle(item.key, 'retry')}>Retry</button>
              <button type="button" class="btn sm ghost" disabled={Boolean(delivery().pending)} onClick={() => void settle(item.key, 'discard')}>Discard</button></td>
          </tr>}</For></tbody></table></div>
      </Show></Show>
    </Section>
    <Show when={creating()}><CreateSchedule initial={target() === 'all' ? '' : target()} names={targets().map(agent => agent.name)} close={() => setCreating(false)}
      saved={message => { setNotice(message); schedules.refresh(); }} /></Show>
    <Show when={cancel()} keyed>{value => <FormDialog title="Cancel schedule?" description={<>Stops <span class="mono">{value.schedule.spec}</span> for <span class="strong">{value.owner}</span>.</>}
      action="Cancel schedule" danger close={() => setCancel(undefined)} submit={async () => {
        const result = await api<{ cancelled: number }>(agentPath(value.owner) + '/schedules', { method: 'POST', body: { action: 'cancel', id: value.schedule.id } });
        setNotice(result?.cancelled ? 'Schedule cancelled for ' + value.owner + '.' : 'That schedule was no longer active.'); schedules.refresh();
      }} />}</Show>
  </>;
}

function CreateSchedule(props: { initial: string; names: string[]; close: () => void; saved: (message: string) => void }) {
  const [trigger, setTrigger] = createSignal<Trigger>('at');
  const fields = { at: { label: 'Run at', placeholder: '20m or an ISO timestamp' }, every: { label: 'Repeat every', placeholder: '15m' },
    watch: { label: 'Watch target', placeholder: 'webhook:topic or agent:idle:name' } };
  return <FormDialog title="New schedule" action="Create schedule" close={props.close} submit={async form => {
    const agent = formText(form, 'agent'), value = formText(form, 'trigger'), prompt = formText(form, 'prompt');
    if (!props.names.includes(agent)) throw new Error('Choose an agent.');
    if (!value || !prompt) throw new Error('Enter a trigger and a prompt.');
    await api(agentPath(agent) + '/schedules', { method: 'POST', body: { action: 'create', [trigger()]: value, prompt } });
    props.saved('Schedule saved for ' + agent + '.');
  }}>
    <Field label="Agent"><select class="input" name="agent" required autofocus>
      <option value="" disabled selected={!props.initial}>Choose an agent…</option><For each={props.names}>{name => <option value={name} selected={name === props.initial}>{name}</option>}</For></select></Field>
    <div class="grid-2"><Field label="Trigger"><select class="input" onChange={event => setTrigger(event.currentTarget.value as Trigger)}>
      <option value="at">One time</option><option value="every">Recurring</option><option value="watch">Watch</option></select></Field>
      <Field label={fields[trigger()].label}><input class="input mono" name="trigger" required placeholder={fields[trigger()].placeholder} /></Field></div>
    <Field label="Prompt"><textarea class="input" name="prompt" rows={3} required /></Field>
  </FormDialog>;
}
