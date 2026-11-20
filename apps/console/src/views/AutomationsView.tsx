import { useCallback, useRef, useState } from 'react';
import { agentPath, api, errorMessage, useResource } from '../api';
import { duration, number, time } from '../format';
import type { Agent, Overview } from '../types';
import { ActionForm, Badge, Field, Heading, Metrics, Panel, Problem, Refresh, Table, formText } from '../ui';

interface Schedule {
  id: string;
  agent_id: string;
  agent_name?: string;
  kind: string;
  spec: string;
  state: string;
  prompt: string;
  next_fire_at_ms?: number | null;
  last_fired_at_ms?: number;
}
interface ScheduleList {
  agent: string;
  schedules: Schedule[];
}
interface HeldDelivery {
  key: string;
  agent_id: string;
  destination: string;
  since_ms?: number;
  error?: string;
  excerpt: string;
}
interface TickSummary {
  schedules_fired: number;
  cloud_turns_polled: number;
  turns_settled: number;
  stale_open_turns_cleared: number;
  backlog_dispatched: number;
  leases_renewed: number;
  sandboxes_swept: number;
}

type ScheduleDialog = { kind: 'create'; initialTarget: string } | { kind: 'cancel'; owner: string; schedule: Schedule };
type Trigger = 'at' | 'every' | 'watch';
type Settlement = 'retry' | 'discard';

const schedulesPath = (name: string) => agentPath(name) + '/schedules';
const loadOutbound = (signal: AbortSignal) => api<{ unsettled: HeldDelivery[] }>('/v1/outbound', { signal });
const loadOverview = (signal: AbortSignal) => api<Overview>('/admin/api/overview', { signal });

function nextFire(ms?: number | null) {
  if (!ms) return time(ms);
  const delta = ms - Date.now();
  const day = 86_400_000;
  if (delta <= 0 || delta >= 7 * day) return time(ms);
  const relative = delta < day ? duration(delta) : Math.floor(delta / day) + 'd ' + Math.floor(delta % day / 3_600_000) + 'h';
  return time(ms) + ' (in ' + relative + ')';
}

function CreateSchedule({ agents, initialTarget, close, saved }: {
  agents: Agent[]; initialTarget: string; close: () => void; saved: (message: string) => void;
}) {
  const [trigger, setTrigger] = useState<Trigger>('at');
  const triggerFields = {
    at: { label: 'Run at', placeholder: '20m or an ISO timestamp' },
    every: { label: 'Repeat every', placeholder: '15m' },
    watch: { label: 'Watch target', placeholder: 'webhook:topic or agent:idle:name' },
  };
  const field = triggerFields[trigger];
  return <ActionForm title="Create schedule" action="Create schedule" close={close} submit={async form => {
    const agent = formText(form, 'agent');
    const value = formText(form, 'trigger');
    const prompt = formText(form, 'prompt');
    if (!agent || !agents.some(candidate => candidate.name === agent)) throw new Error('Select a schedule agent.');
    if (!value || !prompt) throw new Error('Enter a trigger and a prompt.');
    await api(schedulesPath(agent), { method: 'POST', body: { action: 'create', [trigger]: value, prompt } });
    saved('Schedule saved for ' + agent + '.');
  }}>
    <Field label="Agent"><select name="agent" required defaultValue={initialTarget} autoFocus>
      <option value="" disabled>Select an agent</option>
      {agents.map(agent => <option key={agent.id} value={agent.name}>{agent.name}</option>)}
    </select></Field>
    <Field label="Trigger"><select value={trigger} onChange={event => setTrigger(event.currentTarget.value as Trigger)}>
      <option value="at">One time</option><option value="every">Recurring</option><option value="watch">Watch</option>
    </select></Field>
    <Field label={field.label}><input key={trigger} name="trigger" required placeholder={field.placeholder} /></Field>
    <Field label="Prompt"><textarea name="prompt" rows={3} required /></Field>
  </ActionForm>;
}

export function AutomationsView({ agents, overview }: { agents: Agent[]; overview?: Overview }) {
  const [selected, setSelected] = useState('all');
  const [dialog, setDialog] = useState<ScheduleDialog | null>(null);
  const [scheduleNotice, setScheduleNotice] = useState('');
  const [tick, setTick] = useState<{ pending?: boolean; summary?: TickSummary; error?: string }>({});
  const tickPending = useRef(false);
  const [delivery, setDelivery] = useState<{ pending?: string; notice?: string; error?: string }>({});
  const deliveryPending = useRef(false);

  const targets = agents.filter(agent => agent.state !== 'archived' && agent.name.toLowerCase() !== 'all')
    .sort((a, b) => a.name.localeCompare(b.name));
  const target = targets.some(agent => agent.name === selected) ? selected : 'all';
  const schedules = useResource(useCallback((signal: AbortSignal) => api<ScheduleList>(schedulesPath(target), { signal }), [target]));
  const outbound = useResource(loadOutbound);
  const coordinator = useResource(loadOverview);
  const status = coordinator.data ?? overview;
  const turns = status?.active_remote_turns ?? [];
  const timerAuth = [status?.timer_jwks_configured ? 'JWKS' : '', status?.timer_token_configured ? 'TIMER_TOKEN' : ''].filter(Boolean);
  const scheduleList = schedules.data?.schedules ?? [];
  const deliveries = outbound.data?.unsettled ?? [];

  function refreshAll() {
    coordinator.refresh();
    schedules.refresh();
    outbound.refresh();
  }

  async function runTick() {
    if (tickPending.current) return;
    tickPending.current = true;
    setTick(previous => ({ summary: previous.summary, pending: true }));
    try {
      const summary = await api<TickSummary>('/admin/api/tick', { method: 'POST' });
      setTick({ summary });
      refreshAll();
    } catch (error) {
      setTick(previous => ({ summary: previous.summary, error: errorMessage(error) }));
    } finally {
      tickPending.current = false;
    }
  }

  async function settle(key: string, action: Settlement) {
    if (deliveryPending.current) return;
    deliveryPending.current = true;
    const label = action === 'retry' ? 'Retry' : 'Discard';
    setDelivery({ pending: key });
    try {
      await api('/v1/outbound/' + encodeURIComponent(key) + '/' + action, { method: 'POST' });
      setDelivery({ notice: label + ' succeeded for ' + key + '.' });
      refreshAll();
    } catch (error) {
      setDelivery({ error: label + ' failed for ' + key + ': ' + errorMessage(error) });
    } finally {
      deliveryPending.current = false;
    }
  }

  function scheduleSaved(message: string) {
    setScheduleNotice(message);
    schedules.refresh();
  }

  return <div className="flow">
    <Heading eyebrow="Operations" title="Automations & Ticks" blurb="Manage schedules, settle held outbound deliveries, and run coordinator maintenance.">
      <Refresh onClick={refreshAll} />
      <button type="button" className="button primary" disabled={tick.pending} onClick={() => void runTick()}>
        {tick.pending ? 'Running tick…' : 'Run Tick Pass'}
      </button>
    </Heading>

    <Panel title="Coordinator Tick & Remote Turn Status" actions={status && <span className="mono muted">{
      timerAuth.length ? '/v1/internal/fire auth: ' + timerAuth.join(' + ') : 'Timer fire auth unconfigured'
    }</span>}>
      <div className="panel-body stack">
        {coordinator.error && <Problem retry={coordinator.refresh}>Could not load coordinator status: {coordinator.error}</Problem>}
        {!status && !coordinator.error && <p className="muted" role="status">Loading coordinator status…</p>}
        {status && <div className="actions">
          <Badge tone={status.serverless ? 'sky' : 'muted'}>{status.serverless ? 'Serverless Mode' : 'Local Mode'}</Badge>
          <Badge>Sandbox: {status.sandbox_provider || 'local'}</Badge>
          <Badge tone="success">Ready sandboxes: {number(status.active_sandboxes_count ?? 0)}</Badge>
          <Badge tone={turns.length ? 'warning' : 'muted'}>Active remote turns: {number(turns.length)}</Badge>
        </div>}
        {tick.error && <Problem retry={() => void runTick()}>Tick pass failed: {tick.error}</Problem>}
        {tick.summary && <div className="stack" role="status">
          <p>Tick pass completed.</p>
          <Metrics items={[
            { label: 'Schedules fired', value: number(tick.summary.schedules_fired) },
            { label: 'Cloud turns polled', value: number(tick.summary.cloud_turns_polled) },
            { label: 'Turns settled', value: number(tick.summary.turns_settled) },
            { label: 'Stale open turns cleared', value: number(tick.summary.stale_open_turns_cleared ?? 0) },
            { label: 'Backlog dispatched', value: number(tick.summary.backlog_dispatched ?? 0) },
            { label: 'Leases renewed', value: number(tick.summary.leases_renewed) },
            { label: 'Sandboxes swept', value: number(tick.summary.sandboxes_swept) },
          ]} />
        </div>}
      </div>
      {turns.length > 0 && <Table headers={['Turn ID', 'Agent', 'Harness', 'Status', 'Last Event']} empty="No active remote turns."
        rows={turns.map(turn => ({ key: turn.turn_id, cells: [
          <span className="mono">{turn.turn_id}</span>,
          <span className="mono">{turn.agent_name || turn.agent_id}</span>,
          <span className="mono">{turn.harness_kind}{turn.provider_kind && (' (' + turn.provider_kind + ')')}</span>,
          <Badge tone="sky">{turn.status}</Badge>,
          <span className="mono">{time(turn.last_event_at_ms)}</span>,
        ] }))} />}
    </Panel>

    <Panel title="Agent Schedules" actions={<>
      {schedules.data && <Badge>{scheduleList.length} schedule{scheduleList.length === 1 ? '' : 's'}</Badge>}
      <Field label="Schedule agent"><select value={target} onChange={event => setSelected(event.currentTarget.value)}>
        <option value="all">All agents</option>
        {targets.map(agent => <option key={agent.id} value={agent.name}>{agent.name}</option>)}
      </select></Field>
      <button type="button" className="button primary" disabled={!targets.length}
        onClick={() => setDialog({ kind: 'create', initialTarget: target === 'all' ? '' : target })}>Create Schedule</button>
    </>}>
      {(schedules.error || scheduleNotice || !schedules.data) && <div className="panel-body stack">
        {schedules.error && <Problem retry={schedules.refresh}>Could not load schedules: {schedules.error}</Problem>}
        {!schedules.data && !schedules.error && <p className="muted" role="status">Loading schedules…</p>}
        {scheduleNotice && <p role="status">{scheduleNotice}</p>}
      </div>}
      {schedules.data && <Table headers={['Agent', 'Kind', 'Spec', 'State', 'Next Fire', 'Last Fired', 'Prompt', 'Actions']}
        empty="No schedules configured for this filter." rows={scheduleList.map(schedule => {
          const owner = schedule.agent_name || (target === 'all' ? agents.find(agent => agent.id === schedule.agent_id)?.name : schedules.data?.agent || target);
          return { key: schedule.id, cells: [
            <span className="mono">{owner || schedule.agent_id}</span>,
            <Badge tone="sky">{schedule.kind}</Badge>,
            <code>{schedule.spec}</code>,
            <Badge tone={schedule.state === 'active' ? 'success' : 'muted'}>{schedule.state}</Badge>,
            <span className="mono">{nextFire(schedule.next_fire_at_ms)}</span>,
            <span className="mono">{time(schedule.last_fired_at_ms)}</span>,
            schedule.prompt,
            owner && owner.toLowerCase() !== 'all' && schedule.state === 'active' ? <button type="button" className="button"
              aria-label={'Cancel schedule for ' + owner + ': ' + schedule.spec}
              onClick={() => setDialog({ kind: 'cancel', owner, schedule })}>Cancel</button> : '—',
          ] };
        })} />}
    </Panel>

    <Panel title="Unsettled Outbound Queue" actions={outbound.data && <Badge tone={deliveries.length ? 'warning' : 'muted'}>{deliveries.length} held</Badge>}>
      {(outbound.error || delivery.error || delivery.notice || !outbound.data) && <div className="panel-body stack">
        {outbound.error && <Problem retry={outbound.refresh}>Could not load outbound queue: {outbound.error}</Problem>}
        {delivery.error && <Problem>{delivery.error}</Problem>}
        {delivery.notice && <p role="status">{delivery.notice}</p>}
        {!outbound.data && !outbound.error && <p className="muted" role="status">Loading outbound queue…</p>}
      </div>}
      {outbound.data && <Table headers={['Key', 'Agent', 'Destination', 'Since', 'Error', 'Excerpt', 'Actions']}
        empty="Outbound delivery queue is clear." rows={deliveries.map(item => ({ key: item.key, cells: [
          <span className="mono">{item.key}</span>,
          <span className="mono">{item.agent_id}</span>,
          <span className="mono">{item.destination}</span>,
          <span className="mono">{time(item.since_ms)}</span>,
          item.error || '—',
          item.excerpt,
          <div className="actions" aria-busy={delivery.pending === item.key}>
            <button type="button" className="button" disabled={!!delivery.pending} aria-label={'Retry delivery ' + item.key}
              onClick={() => void settle(item.key, 'retry')}>Retry</button>
            <button type="button" className="button danger" disabled={!!delivery.pending} aria-label={'Discard delivery ' + item.key}
              onClick={() => void settle(item.key, 'discard')}>Discard</button>
          </div>,
        ] }))} />}
    </Panel>

    {dialog?.kind === 'create' && <CreateSchedule agents={targets} initialTarget={dialog.initialTarget}
      close={() => setDialog(null)} saved={scheduleSaved} />}
    {dialog?.kind === 'cancel' && <ActionForm title="Cancel schedule" action="Cancel schedule" close={() => setDialog(null)} danger
      submit={async () => {
        const result = await api<{ cancelled: number }>(schedulesPath(dialog.owner), {
          method: 'POST', body: { action: 'cancel', id: dialog.schedule.id },
        });
        scheduleSaved(result.cancelled ? 'Schedule cancelled for ' + dialog.owner + '.' : 'This schedule was no longer active.');
      }}>
      <p>Cancel the <code>{dialog.schedule.spec}</code> schedule for <strong>{dialog.owner}</strong>?</p>
      <p className="muted">{dialog.schedule.prompt}</p>
    </ActionForm>}
  </div>;
}
