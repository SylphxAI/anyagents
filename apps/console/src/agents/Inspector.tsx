import { For, Match, Show, Switch, createSignal, onCleanup, onMount } from 'solid-js';
import type { JSX } from 'solid-js';
import { Archive, ArrowLeft, ArrowUpDown, CalendarClock, Ellipsis, GitBranchPlus, KeyRound, MessageSquare, Pause, Play } from 'lucide-solid';
import { agentPath, api, createPoll, errorMessage } from '../api';
import { dateTime, duration, plural, usd } from '../format';
import { shortModel, statusTone } from '../lib/status';
import { agentHref } from '../route';
import { useConsole } from '../state';
import type { Agent, SandboxLease, Schedule } from '../types';
import { CopyButton, Dot, Empty, Pill, Problem, Skeleton, StatusPill, Tabs } from '../ui/primitives';
import { Conversation } from './Conversation';

type Tab = 'conversation' | 'subagents' | 'schedules' | 'details';

export function Inspector(props: { id: string; back: () => void }) {
  const { fleet, overview, revision, act } = useConsole();
  const summary = () => fleet().agents.find(agent => agent.id === props.id)!;
  const name = summary().name;
  const detail = createPoll(signal => api<Agent>(agentPath(name), { signal }), { interval: 8000, key: revision });
  const agent = (): Agent => ({ ...summary(), ...(detail.data() ?? {}) });
  const node = () => fleet().forest.nodes.get(props.id);
  const status = () => node()?.status ?? 'archived';
  const archived = () => status() === 'archived';
  const meta = () => overview.data()?.agents_meta?.[props.id];
  const failure = () => agent().last_error || agent().turn_errors?.last_error;
  const children = () => (node()?.children ?? []).map(id => fleet().forest.nodes.get(id)!);
  const [tab, setTab] = createSignal<Tab>('conversation');
  let composer: HTMLTextAreaElement | undefined;
  const steer = () => { setTab('conversation'); requestAnimationFrame(() => composer?.focus()); };
  const metaItems = () => ([
    agent().role && ['Role', agent().role], ['Harness', agent().harness], agent().runtime && ['Runtime', agent().runtime!.split(':')[0]],
    agent().model && ['Model', shortModel(agent().model)], agent().profile && ['Profile', agent().profile],
  ].filter(Array.isArray) as string[][]);

  return <section class="inspector" aria-label={'Agent ' + name}>
    <header class="inspector-head">
      <button type="button" class="btn ghost sm back" onClick={() => props.back()}><ArrowLeft size={14} />Agents</button>
      <div class="inspector-title">
        <h2>{agent().name}</h2><StatusPill status={status()} />
        <Show when={meta()?.unread_count}><Pill tone="info">{meta()?.unread_count} unread</Pill></Show>
      </div>
      <div class="inspector-meta mono">
        <For each={metaItems()}>{([label, value]) => <span title={label}><span class="meta-key">{label}</span>{value}</span>}</For>
        <Show when={status() === 'working' && agent().open_turn_age_ms != null}><span><span class="meta-key">Turn</span><span class="num">{duration(agent().open_turn_age_ms!)}</span></span></Show>
      </div>
      <Show when={!archived()}><div class="inspector-actions">
        <button type="button" class="btn primary sm" onClick={steer}><MessageSquare size={13} />Steer</button>
        <Show when={status() === 'paused' || status() === 'errored'} fallback={<button type="button" class="btn sm" onClick={() => act({ kind: 'pause', agent: agent() })}><Pause size={13} />Pause</button>}>
          <button type="button" class="btn sm" onClick={() => act({ kind: 'wake', agent: agent() })}><Play size={13} />Wake</button>
        </Show>
        <button type="button" class="btn sm" onClick={() => act({ kind: 'archive', agent: agent() })}><Archive size={13} />Archive</button>
        <Menu label="More actions" items={[
          { label: 'Spawn sub-agent', icon: () => <GitBranchPlus size={14} />, run: () => act({ kind: 'spawn', parent: agent() }) },
          { label: 'Switch runtime or model', icon: () => <ArrowUpDown size={14} />, run: () => act({ kind: 'runtime', agent: agent() }) },
          { label: 'Manage secret grants', icon: () => <KeyRound size={14} />, run: () => act({ kind: 'grants', agent: agent() }) },
        ]} />
      </div></Show>
    </header>
    <Show when={failure()}><div class="inspector-alert"><Problem><span class="strong">Last error</span> — {failure()}</Problem></div></Show>
    <Show when={detail.error() && !detail.data()}><div class="inspector-alert"><Problem retry={detail.refresh}>{detail.error()}</Problem></div></Show>
    <Tabs label="Agent sections" value={tab()} onChange={setTab} tabs={[
      { id: 'conversation', label: 'Conversation' },
      { id: 'subagents', label: 'Sub-agents', count: node()?.rollup.total ?? 0 },
      { id: 'schedules', label: 'Schedules', count: agent().automations?.total },
      { id: 'details', label: 'Details' },
    ]} />
    <div class="inspector-body">
      <Switch>
        <Match when={tab() === 'conversation'}><Conversation name={name} archived={archived()} composerRef={element => { composer = element; }} /></Match>
        <Match when={tab() === 'subagents'}>
          <Show when={children().length} fallback={<Empty compact title="No sub-agents"
            action={!archived() ? <button type="button" class="btn sm" onClick={() => act({ kind: 'spawn', parent: agent() })}><GitBranchPlus size={13} />Spawn sub-agent</button> : undefined}>
            {name} has not delegated work to any sub-agents.</Empty>}>
            <div class="table-wrap"><table class="table">
              <thead><tr><th>Agent</th><th>Status</th><th>Harness · Model</th><th class="right">Descendants</th></tr></thead>
              <tbody><For each={children()}>{child => <tr class="clickable" onClick={() => { window.location.hash = agentHref(child.agent.name); }}>
                <td><span class="cell-agent"><Dot tone={statusTone[child.status]} pulse={child.status === 'working'} /><a class="strong" href={agentHref(child.agent.name)} onClick={event => event.stopPropagation()}>{child.agent.name}</a>
                  <Show when={child.agent.role}><span class="tag">{child.agent.role}</span></Show></span></td>
                <td><StatusPill status={child.status} /></td>
                <td class="mono subtle">{child.agent.harness}{child.agent.model ? ' · ' + shortModel(child.agent.model) : ''}</td>
                <td class="right num">{child.rollup.total || '—'}<Show when={child.rollup.errored > 0}><span class="tone-bad"> · {child.rollup.errored} errored</span></Show></td>
              </tr>}</For></tbody></table></div>
          </Show>
        </Match>
        <Match when={tab() === 'schedules'}><Schedules name={name} archived={archived()} /></Match>
        <Match when={tab() === 'details'}><Details agent={agent()} lease={meta()?.sandbox_lease ?? undefined} /></Match>
      </Switch>
    </div>
  </section>;
}

function Menu(props: { label: string; items: { label: string; icon: () => JSX.Element; run: () => void }[] }) {
  const [open, setOpen] = createSignal(false);
  let root!: HTMLDivElement;
  onMount(() => {
    const onDown = (event: MouseEvent) => { if (!root.contains(event.target as Node)) setOpen(false); };
    const onKey = (event: KeyboardEvent) => { if (event.key === 'Escape') setOpen(false); };
    document.addEventListener('mousedown', onDown); document.addEventListener('keydown', onKey);
    onCleanup(() => { document.removeEventListener('mousedown', onDown); document.removeEventListener('keydown', onKey); });
  });
  return <div class="menu" ref={root}>
    <button type="button" class="btn icon sm" aria-label={props.label} title={props.label} aria-haspopup="menu" aria-expanded={open()} onClick={() => setOpen(value => !value)}><Ellipsis size={14} /></button>
    <Show when={open()}><div class="menu-list" role="menu"><For each={props.items}>{item =>
      <button type="button" role="menuitem" onClick={() => { setOpen(false); item.run(); }}>{item.icon()}{item.label}</button>}</For></div></Show>
  </div>;
}

function Schedules(props: { name: string; archived: boolean }) {
  const schedules = createPoll(signal => api<{ schedules: Schedule[] }>(agentPath(props.name) + '/schedules', { signal }), { interval: 15000 });
  const [error, setError] = createSignal('');
  const list = () => schedules.data()?.schedules ?? [];
  async function cancel(schedule: Schedule) {
    setError('');
    try { await api(agentPath(props.name) + '/schedules', { method: 'POST', body: { action: 'cancel', id: schedule.id } }); schedules.refresh(); }
    catch (cause) { setError(errorMessage(cause)); }
  }
  return <Switch>
    <Match when={schedules.error()}><Problem retry={schedules.refresh}>{schedules.error()}</Problem></Match>
    <Match when={!schedules.data()}><Skeleton rows={3} /></Match>
    <Match when={!list().length}><Empty compact icon={<CalendarClock size={18} />} title="No schedules">Timers, watches, and recurring prompts for {props.name} appear here.</Empty></Match>
    <Match when={list().length}>
      <Show when={error()}><Problem>{error()}</Problem></Show>
      <div class="table-wrap"><table class="table">
        <thead><tr><th>Kind</th><th>Trigger</th><th>Status</th><th>Next fire</th><th>Prompt</th><th class="right"><span class="sr-only">Actions</span></th></tr></thead>
        <tbody><For each={list()}>{schedule => <tr>
          <td><span class="tag">{schedule.kind}</span></td>
          <td class="mono">{schedule.spec}</td>
          <td><Pill tone={schedule.state === 'active' ? 'ok' : 'neutral'} dot>{schedule.state}</Pill></td>
          <td class="num subtle">{dateTime(schedule.next_fire_at_ms)}</td>
          <td class="cell-wrap subtle">{schedule.prompt}</td>
          <td class="right"><Show when={!props.archived && schedule.state === 'active'}><button type="button" class="btn sm ghost" onClick={() => void cancel(schedule)}>Cancel</button></Show></td>
        </tr>}</For></tbody></table></div>
    </Match>
  </Switch>;
}

function Details(props: { agent: Agent; lease?: SandboxLease }) {
  const { act } = useConsole();
  const groups = (): [string, [string, JSX.Element | string | number | undefined][]][] => {
    const agent = props.agent, lease = props.lease;
    return [
      ['Execution', [['Harness', agent.harness], ['Runtime', agent.runtime ?? undefined], ['Model', agent.model ?? undefined], ['Live model', agent.live_model],
        ['Profile', agent.profile || 'Automatic'], ['Process', agent.process], ['Turn state', agent.turn_state ?? undefined], ['Workspace', agent.workdir]]],
      ['Hierarchy', [['Parent', agent.parent ? <a href={agentHref(agent.parent)}>{agent.parent}</a> : 'Top level'], ['Role', agent.role ?? undefined], ['Scope', agent.scope], ['Lifecycle', agent.lifecycle]]],
      ['Sandbox lease', lease ? [['Provider', lease.provider], ['State', lease.lease_state], ['Lease', lease.lease_id], ['Expires', dateTime(lease.lease_expires_at_ms)], ['Heartbeat', dateTime(lease.last_heartbeat_ms)]] : [['Lease', 'No sandbox lease']]],
      ['Budget & access', [['Budget cap', agent.remote_budget_micros != null ? usd(agent.remote_budget_micros) : 'None'],
        ['Secrets', agent.secrets?.length ? <span class="chips"><For each={agent.secrets}>{secret => <span class="tag">{secret}</span>}</For></span> : 'None granted'],
        ['Automations', agent.automations?.total != null ? plural(agent.automations.total, 'schedule') : undefined]]],
      ['Identity', [['Agent ID', <span class="id-cell"><code>{agent.id}</code><CopyButton value={agent.id} label="Copy agent ID" /></span>], ['Created', dateTime(agent.created_at_ms)]]],
    ];
  };
  return <div class="details">
    <Show when={props.agent.description}><p class="details-description">{props.agent.description}</p></Show>
    <For each={groups()}>{([title, rows]) => <section class="details-group">
      <h3>{title}<Show when={title === 'Budget & access' && props.agent.state !== 'archived'}><button type="button" class="btn ghost sm" onClick={() => act({ kind: 'grants', agent: props.agent })}>Manage grants</button></Show></h3>
      <dl><For each={rows.filter(([, value]) => value != null && value !== '')}>{([label, value]) => <div><dt>{label}</dt><dd class={typeof value === 'string' ? 'mono' : undefined}>{value}</dd></div>}</For></dl>
    </section>}</For>
  </div>;
}
