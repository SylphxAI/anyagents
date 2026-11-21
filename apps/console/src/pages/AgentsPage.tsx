import { Index, Show, createEffect, createMemo, createSignal, on, onCleanup, onMount } from 'solid-js';
import { Bot, ChevronRight, ChevronsDownUp, ChevronsUpDown, Plus, SearchX } from 'lucide-solid';
import { shortModel, statusTone } from '../lib/status';
import { ancestorsOf, branchIds, buildForest, visibleRows } from '../lib/tree';
import type { TreeFilter, TreeRow as Row } from '../lib/tree';
import { agentHref } from '../route';
import { useConsole } from '../state';
import type { Agent } from '../types';
import { Dot, Empty, Highlight, Pill, SearchField, Segmented, Skeleton } from '../ui/primitives';
import { Inspector } from '../agents/Inspector';

export function AgentsPage(props: { selectedId?: string }) {
  const { fleet, roster, act } = useConsole();
  const [query, setQuery] = createSignal('');
  const [filter, setFilter] = createSignal<TreeFilter>('all');
  const [expanded, setExpanded] = createSignal<ReadonlySet<string>>(new Set());
  const [showArchived, setShowArchived] = createSignal(false);
  let search: HTMLInputElement | undefined;
  let tree!: HTMLDivElement;
  const forest = createMemo(() => showArchived() ? buildForest(fleet().agents, true) : fleet().forest);
  const selected = createMemo(() => props.selectedId ? fleet().agents.find(agent => agent.id === props.selectedId || agent.name === props.selectedId) : undefined);
  const selectedKey = createMemo(() => selected()?.id);
  const rows = createMemo(() => visibleRows(forest(), { expanded: expanded(), query: query(), filter: filter() }));
  const branches = createMemo(() => branchIds(forest()));
  const allOpen = () => branches().length > 0 && branches().every(id => expanded().has(id));

  // Reveal the selected agent when it is opened from elsewhere (palette, overview, links).
  createEffect(on(selectedKey, id => {
    if (!id) return;
    const chain = ancestorsOf(forest(), id);
    if (chain.some(ancestor => !expanded().has(ancestor))) setExpanded(previous => new Set([...previous, ...chain]));
    requestAnimationFrame(() => tree?.querySelector('[aria-selected="true"]')?.scrollIntoView({ block: 'nearest' }));
  }));

  onMount(() => {
    const onKey = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      if (event.key === 'f' && !event.metaKey && !event.ctrlKey && !(target && /^(INPUT|TEXTAREA|SELECT)$/.test(target.tagName)) && !document.querySelector('dialog[open]')) {
        event.preventDefault(); search?.focus();
      }
    };
    window.addEventListener('keydown', onKey);
    onCleanup(() => window.removeEventListener('keydown', onKey));
  });

  const toggle = (id: string) => setExpanded(previous => { const next = new Set(previous); if (next.has(id)) next.delete(id); else next.add(id); return next; });
  const select = (agent?: Agent) => { window.location.hash = agentHref(agent?.name); };

  function onTreeKey(event: KeyboardEvent) {
    const list = rows();
    const index = list.findIndex(row => row.id === selectedKey());
    const row = list[index];
    const move = (to: number) => { const next = list[Math.max(0, Math.min(list.length - 1, to))]; if (next) { event.preventDefault(); select(forest().nodes.get(next.id)!.agent); } };
    if (event.key === 'ArrowDown') move(index < 0 ? 0 : index + 1);
    else if (event.key === 'ArrowUp') move(index < 0 ? 0 : index - 1);
    else if (event.key === 'Home') move(0);
    else if (event.key === 'End') move(list.length - 1);
    else if (event.key === 'ArrowRight' && row?.hasChildren && !row.expanded) { event.preventDefault(); toggle(row.id); }
    else if (event.key === 'ArrowLeft' && row) {
      event.preventDefault();
      if (row.expanded && !query() && filter() === 'all') toggle(row.id);
      else { const parent = forest().nodes.get(row.id)?.parentId; if (parent) select(forest().nodes.get(parent)!.agent); }
    } else if (event.key === 'Escape' && selectedKey()) { event.preventDefault(); select(); }
  }

  return <div class={'page agents-page' + (selected() ? ' has-selection' : '')}>
    <div class="agents-head">
      <div class="page-heading"><h1>Agents</h1><p>{fleet().live.length} active across {fleet().forest.roots.length} top-level directors.</p></div>
      <div class="page-actions"><button type="button" class="btn primary" aria-label="Spawn agent" onClick={() => act({ kind: 'spawn' })}><Plus size={14} /><span class="btn-label">Spawn agent</span></button></div>
    </div>
    <div class="toolbar">
      <SearchField inputRef={element => { search = element; }} value={query()} onChange={setQuery} placeholder="Filter by name, role, model, harness…" shortcut="F" label="Filter agents" />
      <Segmented label="Agent filter" value={filter()} onChange={setFilter} options={[
        { value: 'all', label: 'All', count: fleet().live.length },
        { value: 'working', label: 'Working', count: fleet().working.length, tone: 'ok' },
        { value: 'attention', label: 'Needs attention', count: fleet().errored.length, tone: 'bad' },
        { value: 'top', label: 'Top-level', count: fleet().forest.roots.length },
      ]} />
      <button type="button" class="btn ghost" disabled={filter() === 'top' || !branches().length}
        onClick={() => setExpanded(allOpen() ? new Set<string>() : new Set(branches()))}>
        <Show when={allOpen()} fallback={<><ChevronsUpDown size={14} />Expand all</>}><ChevronsDownUp size={14} />Collapse all</Show>
      </button>
    </div>
    <div class="split">
      <div class="tree-pane">
        <div class="tree-head" aria-hidden="true"><span>Agent</span><span>Sub-agents</span><span class="tree-head-model">Model</span></div>
        <div class="tree" role="tree" aria-label="Agent hierarchy" ref={tree} onKeyDown={onTreeKey}>
          <Show when={roster.data() || roster.error()} fallback={<Skeleton rows={12} />}>
            <Show when={rows().length === 0}>
              <Empty compact icon={<SearchX size={18} />} title="No matching agents"
                action={<button type="button" class="btn sm" onClick={() => { setQuery(''); setFilter('all'); }}>Clear filters</button>}>
                {query() ? <>Nothing matches “{query()}”.</> : filter() === 'attention' ? 'No agents need attention.' : filter() === 'working' ? 'No agents are working right now.' : 'No agents yet.'}
              </Empty>
            </Show>
          </Show>
          <Index each={rows()}>{(row, position) => <TreeRow row={row()} query={query()} selected={row().id === selectedKey()}
            focusable={selectedKey() ? row().id === selectedKey() : position === 0} forest={forest()} toggle={toggle} select={select} />}</Index>
        </div>
        <div class="tree-foot">
          <span class="num subtle">{rows().length} shown</span>
          <Show when={fleet().archived > 0}><label class="check"><input type="checkbox" checked={showArchived()} onChange={event => setShowArchived(event.currentTarget.checked)} />
            Show {fleet().archived} archived</label></Show>
        </div>
      </div>
      <div class="inspector-pane">
        <Show when={selectedKey()} keyed fallback={<Empty icon={<Bot size={20} />} title="Select an agent">
          Choose an agent to see its conversation, sub-agents, schedules, and configuration.
          <div class="hint-keys"><kbd class="kbd">↑</kbd><kbd class="kbd">↓</kbd> move · <kbd class="kbd">→</kbd> expand · <kbd class="kbd">F</kbd> filter</div>
        </Empty>}>{id => <Inspector id={id} back={() => select()} />}</Show>
      </div>
    </div>
  </div>;
}

function TreeRow(props: {
  row: Row; query: string; selected: boolean; focusable: boolean; forest: ReturnType<typeof buildForest<Agent>>;
  toggle: (id: string) => void; select: (agent: Agent) => void;
}) {
  const node = () => props.forest.nodes.get(props.row.id)!;
  const agent = () => node().agent;
  let element!: HTMLDivElement;
  createEffect(on(() => props.selected, selected => { if (selected && document.activeElement?.closest('.tree')) element.focus({ preventScroll: true }); }, { defer: true }));
  return <div ref={element} role="treeitem" aria-level={props.row.depth + 1} aria-expanded={props.row.hasChildren ? props.row.expanded : undefined} aria-selected={props.selected}
    tabindex={props.focusable ? 0 : -1} class={'tree-row' + (props.selected ? ' selected' : '') + (props.row.match ? ' match' : '') + (node().status === 'archived' ? ' archived' : '')}
    onClick={() => props.select(agent())}>
    <Index each={Array.from({ length: props.row.depth })}>{() => <span class="guide" aria-hidden="true" />}</Index>
    <Show when={props.row.hasChildren} fallback={<span class="chevron-spacer" aria-hidden="true" />}>
      <button type="button" tabindex="-1" class={'chevron' + (props.row.expanded ? ' open' : '')} aria-label={(props.row.expanded ? 'Collapse ' : 'Expand ') + agent().name}
        onClick={event => { event.stopPropagation(); props.toggle(props.row.id); }}><ChevronRight size={14} /></button>
    </Show>
    <Dot tone={statusTone[node().status]} pulse={node().status === 'working'} label={node().status} />
    <span class="tree-name"><Highlight text={agent().name} query={props.query} /></span>
    <Show when={agent().role}><span class="tag">{agent().role}</span></Show>
    <span class="tree-rollup">
      <Show when={node().rollup.errored > 0} fallback={<Show when={node().rollup.working > 0}><Pill tone="ok">{node().rollup.working} active</Pill></Show>}>
        <Pill tone="bad">{node().rollup.errored} errored</Pill>
      </Show>
      <Show when={node().rollup.total > 0}><span class="rollup-count num" title={node().rollup.total + ' sub-agents'}>{node().rollup.total}</span></Show>
    </span>
    <span class="tree-model mono" title={agent().harness + (agent().model ? ' · ' + agent().model : '')}>{shortModel(agent().model) || agent().harness}</span>
  </div>;
}
