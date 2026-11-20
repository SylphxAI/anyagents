import { useCallback, useMemo, useState } from 'react';
import { Activity, Archive, ArrowDownUp, ArrowLeft, Bot, ChevronRight, CircleAlert, GitFork, KeyRound, Layers, Moon, Pause, Play, Plus, Search, Workflow } from 'lucide-react';
import { agentPath, api, useResource } from '../api';
import { status, states } from '../types';
import type { Agent, Overview } from '../types';
import { Badge, CopyButton, Dot, Empty, Heading, Panel, Problem, Status } from '../ui';
import { Conversation } from './Conversation';
import type { FleetAction } from './AgentAction';

const labels = { all: 'Total fleet', working: 'Working', idle: 'Idle', paused: 'Paused', errored: 'Errors', archived: 'Archived' };
const icons = { all: Layers, working: Activity, idle: Moon, paused: Pause, errored: CircleAlert, archived: Archive };

export function Fleet({ agents, overview, selected, select, open, grants, refreshVersion }: {
  agents: Agent[]; overview?: Overview; selected?: Agent; select: (id?: string) => void;
  open: (action: FleetAction) => void; grants: (agent: Agent) => void; refreshVersion: number;
}) {
  const [filter, setFilter] = useState<typeof states[number]>('all');
  const [search, setSearch] = useState('');
  const [folded, setFolded] = useState(new Set<string>());
  const rows = useMemo(() => {
    const lookup = new Map(agents.flatMap(agent => [[agent.id, agent], [agent.name, agent]] as const));
    const children = new Map<string | undefined, Agent[]>();
    for (const a of [...agents].sort((a, b) => a.name.localeCompare(b.name))) {
      const parent = a.parent ? lookup.get(a.parent)?.id : undefined;
      children.set(parent, [...(children.get(parent) ?? []), a]);
    }
    const flat: { agent: Agent; depth: number; branch: boolean }[] = [];
    const seen = new Set<string>();
    const query = search.trim().toLowerCase();
    function visit(agent: Agent, depth: number, hidden = false) {
      if (seen.has(agent.id)) return;
      seen.add(agent.id);
      const sub = children.get(agent.id) ?? [];
      if (!hidden && (filter === 'all' || status(agent) === filter) &&
        [agent.name, agent.role, agent.harness, agent.runtime, agent.model].some(field => field?.toLowerCase().includes(query))) {
        flat.push({ agent, depth, branch: sub.length > 0 });
      }
      for (const child of sub) visit(child, depth + 1, hidden || (!query && filter === 'all' && folded.has(agent.id)));
    }
    for (const a of children.get(undefined) ?? []) visit(a, 0);
    for (const a of agents) visit(a, 0);
    return flat;
  }, [agents, filter, folded, search]);
  return <div className={selected ? 'fleet-workspace has-agent' : 'fleet-workspace'}>
    <Heading eyebrow="CONTROL PLANE / FLEET" title="Agent fleet" blurb="Monitor your agents, their execution state, and live conversations.">
      <button className="button primary" onClick={() => open({ kind: 'spawn' })}><Plus size={16} /> Spawn agent</button>
    </Heading>
    <section className="pulse" aria-label="Fleet pulse">{states.map(state => {
      const Icon = icons[state];
      const count = state === 'all' ? agents.length : agents.filter(agent => status(agent) === state).length;
      return <button key={state} className={'pulse-card ' + state} aria-pressed={state === filter} onClick={() => setFilter(state === filter ? 'all' : state)}>
        <span className="pulse-label"><Icon size={15} />{labels[state]}</span><strong>{count.toString().padStart(2, '0')}</strong>
      </button>;
    })}</section>
    <div className={'fleet-grid' + (selected ? ' has-selection' : '')}>
      <aside className="fleet-roster panel" aria-label="Fleet tree">
        <div className="panel-head"><h2><GitFork size={16} /> Hierarchy</h2><Badge>{rows.length} agents</Badge></div>
        <label className="tree-search"><Search size={16} /><input aria-label="Search agents" placeholder="Filter by name, role, model…" value={search} onChange={event => setSearch(event.target.value)} /></label>
        <div className="tree-rows">{rows.length === 0 && <Empty>No agents match this filter.</Empty>}{rows.map(({ agent, depth, branch }) => <div key={agent.id} className={'tree-row' + (selected?.id === agent.id ? ' current' : '')} style={{ '--depth': Math.min(depth, 5) } as React.CSSProperties}>
          {branch ? <button className="tree-expand" aria-label={(folded.has(agent.id) ? 'Expand ' : 'Collapse ') + agent.name} aria-expanded={!folded.has(agent.id)} onClick={() => setFolded(before => {
            const next = new Set(before); if (next.has(agent.id)) next.delete(agent.id); else next.add(agent.id); return next;
          })}><ChevronRight size={14} /></button> : <span className="tree-stem" />}
          <button className="tree-select" aria-current={selected?.id === agent.id ? 'true' : undefined} onClick={() => select(agent.id)}>
            <span className="tree-name"><Dot tone={status(agent)} />{agent.name}{Boolean(overview?.agents_meta?.[agent.id]?.unread_count) && <Badge>{overview?.agents_meta?.[agent.id]?.unread_count} unread</Badge>}</span>
            <span className="tree-specs">{[agent.role, agent.harness, agent.runtime, agent.model].filter(Boolean).map((chip, index) => <Badge key={index}>{chip}</Badge>)}</span>
          </button>
        </div>)}</div>
      </aside>
      {selected ? <Inspector key={selected.id} agent={selected} overview={overview} back={() => select()} open={open} grants={grants} revision={refreshVersion} /> : <section className="inspector-placeholder panel"><Bot size={30} /><h2>Select an agent</h2><p>Choose an agent in the hierarchy to see its settings and conversation.</p></section>}
    </div>
  </div>;
}

function Inspector({ agent: summary, overview, back, open, grants, revision }: {
  agent: Agent; overview?: Overview; back: () => void; open: (action: FleetAction) => void; grants: (agent: Agent) => void; revision: number;
}) {
  const load = useCallback((signal: AbortSignal) => api<Agent>(agentPath(summary.name), { signal }), [summary.name, revision]);
  const detail = useResource(load, 6000);
  const agent = detail.data ?? summary;
  const state = status(agent);
  const failure = agent.last_error || agent.turn_errors?.last_error;
  const lease = overview?.agents_meta?.[agent.id]?.sandbox_lease;
  const values = [['Harness', agent.harness], ['Runtime', agent.runtime], ['Model', agent.model], ['Role', agent.role],
    ['Profile', agent.profile], ['Parent', agent.parent], ['Scope', agent.scope], ['Turn', agent.turn_state],
    ['Lifecycle', agent.lifecycle], ['Process', agent.process], ['Workspace', agent.workdir],
    ['Sandbox lease', lease && lease.provider + ' · ' + lease.lease_state],
    ['Budget cap', agent.remote_budget_micros != null ? '$' + (agent.remote_budget_micros / 1e6).toFixed(2) : undefined],
    ['Automations', agent.automations?.total]];
  return <section className="inspector min-w-0" aria-label="Agent inspector">
    <header className="inspector-head panel"><button className="button quiet mobile-back" onClick={back}><ArrowLeft size={15} /> Fleet tree</button>
      <div className="identity"><span className="agent-symbol"><Bot size={23} /></span><div><div className="identity-name"><h2>{agent.name}</h2><Status value={state} /></div>
        <div className="identity-id"><code>{agent.id}</code><CopyButton value={agent.id} label="Copy agent ID" /></div></div></div>
      {agent.description && <p className="agent-description">{agent.description}</p>}
      <div className="toolbar">{state !== 'archived' && <>
        <button className="button" onClick={() => open({ kind: 'spawn', parent: agent })}><Plus size={14} /> Child</button>
        <button className="button" onClick={() => open({ kind: 'runtime', agent })}><ArrowDownUp size={14} /> Switch runtime</button>
        {state === 'paused' || state === 'errored' ? <button className="button" onClick={() => open({ kind: 'wake', agent })}><Play size={14} /> Wake</button> : <button className="button" onClick={() => open({ kind: 'pause', agent })}><Pause size={14} /> Pause</button>}
        <button className="button quiet" onClick={() => open({ kind: 'archive', agent })}><Archive size={14} /> Archive</button>
      </>}</div>
    </header>
    {detail.error && <Problem retry={detail.refresh}>{detail.error}</Problem>}
    {failure && <Problem><strong>Last error</strong><br />{failure}</Problem>}
    <Conversation name={agent.name} archived={state === 'archived'} />
    <Panel title="Execution details" icon={<Workflow size={16} />} actions={state !== 'archived' && <button className="button quiet" onClick={() => grants(agent)}><KeyRound size={14} /> Secret grants</button>}>
      <dl className="metadata">{values.filter(([, value]) => value != null && value !== '').map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{value}</dd></div>)}</dl>
      {Boolean(agent.secrets?.length) && <div className="grant-summary"><span className="muted">Granted secrets</span>{agent.secrets?.map(name => <Badge key={name}>{name}</Badge>)}</div>}
    </Panel>
  </section>;
}
