import { useCallback, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { Activity, Archive, ArrowDownUp, ArrowLeft, Bot, ChevronRight, CircleAlert, GitFork, KeyRound, Layers, MessageSquare, Moon, Pause, Play, Plus, Search, Workflow } from 'lucide-react';
import { agentPath, api, useResource } from '../api';
import { status, states } from '../types';
import type { Agent, Overview } from '../types';
import { Badge, CopyButton, Dot, Empty, Heading, Panel, Problem, Status } from '../ui';
import { Conversation } from './Conversation';
import type { FleetAction } from './AgentAction';
import { buildForest, projectForest } from './tree';
import './fleet.css';

const labels = { all: 'Total fleet', working: 'Working', idle: 'Idle', paused: 'Paused', errored: 'Errors', archived: 'Archived' };
const icons = { all: Layers, working: Activity, idle: Moon, paused: Pause, errored: CircleAlert, archived: Archive };

export function Fleet({ agents, overview, selected, select, open, grants, refreshVersion }: {
  agents: Agent[]; overview?: Overview; selected?: Agent; select: (id?: string) => void;
  open: (action: FleetAction) => void; grants: (agent: Agent) => void; refreshVersion: number;
}) {
  const [filter, setFilter] = useState<typeof states[number]>('all');
  const [search, setSearch] = useState('');
  const [folded, setFolded] = useState(new Set<string>());
  const [filteredFolded, setFilteredFolded] = useState(new Set<string>());
  const filtering = filter !== 'all' || Boolean(search.trim());
  const activeFolds = filtering ? filteredFolded : folded;
  const forest = useMemo(() => buildForest(agents), [agents]);
  const { rows, matchCount } = useMemo(() => projectForest(forest, filter, search, activeFolds), [forest, filter, search, activeFolds]);
  const roster = useRef<HTMLElement>(null);
  const inspectorBack = useRef<HTMLButtonElement>(null);
  const wasSelected = useRef(selected?.id);
  useLayoutEffect(() => {
    const previous = wasSelected.current;
    wasSelected.current = selected?.id;
    if (previous === selected?.id || !matchMedia('(max-width: 760px)').matches) return;
    if (selected) inspectorBack.current?.focus();
    else if (previous) {
      const row = roster.current?.querySelector<HTMLButtonElement>(        '.tree-select[data-agent-id="' + CSS.escape(previous) + '"]');
      (row ?? roster.current)?.focus();
    }
  }, [selected?.id]);

  function toggle(id: string) {
    (filtering ? setFilteredFolded : setFolded)(before => {
      const next = new Set(before);
      if (!next.delete(id)) next.add(id);
      return next;
    });
  }

  return <div className={selected ? 'fleet-workspace has-agent' : 'fleet-workspace'}>
    <Heading eyebrow="CONTROL PLANE / FLEET" title="Agent fleet" blurb="Monitor your agents, their execution state, and live conversations.">
      <button className="button primary" onClick={() => open({ kind: 'spawn' })}><Plus size={16} /> Spawn agent</button>
    </Heading>
    <section className="pulse" aria-label="Fleet pulse">{states.map(state => {
      const Icon = icons[state];
      const count = state === 'all' ? agents.length : agents.filter(agent => status(agent) === state).length;
      return <button key={state} className={'pulse-card ' + state} aria-pressed={state === filter} onClick={() => {
        setFilter(state === filter ? 'all' : state); setFilteredFolded(new Set());
      }}>
        <span className="pulse-label"><Icon size={15} />{labels[state]}</span><strong>{count.toString().padStart(2, '0')}</strong>
      </button>;
    })}</section>
    <div className={'fleet-grid' + (selected ? ' has-selection' : '')}>
      <aside className="fleet-roster panel" aria-label="Fleet tree" ref={roster} tabIndex={-1}>
        <div className="panel-head"><h2><GitFork size={16} /> Hierarchy</h2><Badge>{matchCount} {filtering ? (matchCount === 1 ? 'match' : 'matches') : (matchCount === 1 ? 'agent' : 'agents')}</Badge></div>
        <label className="tree-search"><Search size={16} /><input aria-label="Search agents" placeholder="Filter by name, role, model…" value={search} onChange={event => {
          setSearch(event.target.value); setFilteredFolded(new Set());
        }} /></label>
        {rows.length === 0 ? <Empty>{filtering ? 'No agents match this filter.' : 'No agents in the fleet yet.'}</Empty> :
          <ul className="tree-rows" aria-label="Agent hierarchy" role="list">{rows.map(({ node, depth, rails, last, context, branch, expanded, stem, showParent }) => {
            const { agent, state, activeDescendants, parent } = node;
            const unread = overview?.agents_meta?.[agent.id]?.unread_count;
            const specs = [['Harness', agent.harness], ['Model', agent.model], ['Profile', agent.profile]];
            return <li key={agent.id} className={'tree-row' + (selected?.id === agent.id ? ' current' : '')}>
              <span className="fleet-guides" aria-hidden="true">
                {rails.map((continues, level) => <span key={level} className={continues ? 'fleet-rail continues' : 'fleet-rail'} />)}
                {depth > 0 && !showParent && <span className={last ? 'fleet-elbow last' : 'fleet-elbow'} />}
              </span>
              <span className={'fleet-node-control' + (stem ? ' continues' : '')}>
                {branch && <button type="button" className="tree-expand" aria-label={(expanded ? 'Collapse ' : 'Expand ') + agent.name} aria-expanded={expanded} onClick={() => toggle(agent.id)}><ChevronRight size={16} /></button>}
              </span>
              <button type="button" className="tree-select" data-agent-id={agent.id} aria-current={selected?.id === agent.id ? 'true' : undefined} onClick={() => select(agent.id)}>
                <span className="tree-name">
                  <span className="fleet-agent-label" title={state}><Dot tone={state} /><span>{agent.name}<span className="sr-only">, {state}</span></span></span>
                  {node.children.length > 0 && <span className={'badge fleet-active' + (activeDescendants > 0 ? ' has-active' : '')} title="Non-archived descendants">{activeDescendants} active<span className="sr-only"> {activeDescendants === 1 ? 'descendant' : 'descendants'}</span></span>}
                  {Boolean(unread) && <Badge>{unread} unread</Badge>}
                  {context && <span className="fleet-ancestor">Ancestor</span>}
                </span>
                {parent && <span className={showParent ? 'fleet-parent' : 'sr-only'}>Parent: {parent.agent.name}</span>}
                <span className="tree-specs">{specs.map(([label, value]) => value && <span className="badge" key={label} title={label + ': ' + value}>
                  <span className="sr-only">{label}: </span><span>{value}</span>
                </span>)}</span>
                <span className="sr-only">Open conversation</span>
                <MessageSquare className="fleet-conversation-icon" size={14} aria-hidden="true" />
              </button>
            </li>;
          })}</ul>}
      </aside>
      {selected ? <Inspector key={selected.id} agent={selected} overview={overview} back={() => select()} backRef={inspectorBack} open={open} grants={grants} revision={refreshVersion} /> : <section className="inspector-placeholder panel"><Bot size={30} /><h2>Select an agent</h2><p>Choose an agent in the hierarchy to see its settings and conversation.</p></section>}
    </div>
  </div>;
}

function Inspector({ agent: summary, overview, back, backRef, open, grants, revision }: {
  agent: Agent; overview?: Overview; back: () => void; backRef: React.Ref<HTMLButtonElement>; open: (action: FleetAction) => void; grants: (agent: Agent) => void; revision: number;
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
    <header className="inspector-head panel"><button type="button" className="button quiet mobile-back" ref={backRef} onClick={back}><ArrowLeft size={15} /> Fleet tree</button>
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
