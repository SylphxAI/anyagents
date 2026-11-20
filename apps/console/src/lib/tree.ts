// Builds the Director-first agent forest and computes the visible rows for a
// given expansion state, search query, and filter. Pure and unit-tested.
import { agentStatus } from './status.ts';
import type { AgentLike, AgentStatus } from './status.ts';

export interface Rollup { total: number; working: number; errored: number }
export interface TreeNode<A extends AgentLike = AgentLike> {
  agent: A;
  status: AgentStatus;
  parentId?: string;
  children: string[];
  rollup: Rollup;
}
export interface Forest<A extends AgentLike = AgentLike> {
  roots: string[];
  nodes: Map<string, TreeNode<A>>;
}
export type TreeFilter = 'all' | 'working' | 'attention' | 'top';
export interface TreeRow { id: string; depth: number; hasChildren: boolean; expanded: boolean; match: boolean }

const byName = (a: AgentLike, b: AgentLike) => a.name.localeCompare(b.name, undefined, { numeric: true, sensitivity: 'base' });

export function buildForest<A extends AgentLike>(agents: readonly A[], includeArchived = false): Forest<A> {
  const live = agents.filter(agent => includeArchived || agent.state !== 'archived');
  const lookup = new Map<string, A>();
  for (const agent of live) { lookup.set(agent.id, agent); if (!lookup.has(agent.name)) lookup.set(agent.name, agent); }
  const nodes = new Map<string, TreeNode<A>>();
  for (const agent of [...live].sort(byName)) {
    nodes.set(agent.id, { agent, status: agentStatus(agent), children: [], rollup: { total: 0, working: 0, errored: 0 } });
  }
  const roots: string[] = [];
  for (const node of nodes.values()) {
    const parent = node.agent.parent ? lookup.get(node.agent.parent) : undefined;
    if (parent && parent.id !== node.agent.id && nodes.has(parent.id) && !createsCycle(nodes, node.agent.id, parent.id)) {
      node.parentId = parent.id;
      nodes.get(parent.id)!.children.push(node.agent.id);
    } else roots.push(node.agent.id);
  }
  const visit = (id: string): Rollup => {
    const node = nodes.get(id)!;
    const sum: Rollup = { total: 0, working: 0, errored: 0 };
    for (const child of node.children) {
      const sub = visit(child);
      const childStatus = nodes.get(child)!.status;
      sum.total += 1 + sub.total;
      sum.working += (childStatus === 'working' ? 1 : 0) + sub.working;
      sum.errored += (childStatus === 'errored' ? 1 : 0) + sub.errored;
    }
    node.rollup = sum;
    return sum;
  };
  roots.forEach(visit);
  return { roots, nodes };
}

function createsCycle(nodes: Map<string, TreeNode>, child: string, parent: string) {
  let cursor: string | undefined = parent;
  const seen = new Set<string>();
  while (cursor) {
    if (cursor === child || seen.has(cursor)) return true;
    seen.add(cursor);
    cursor = nodes.get(cursor)?.parentId;
  }
  return false;
}

export function agentMatches(agent: AgentLike, query: string) {
  const tokens = query.toLowerCase().trim().split(/\s+/).filter(Boolean);
  if (!tokens.length) return true;
  const haystack = [agent.name, agent.role, agent.harness, agent.runtime, agent.model, agentStatus(agent)]
    .filter(Boolean).join(' ').toLowerCase();
  return tokens.every(token => haystack.includes(token));
}

export function ancestorsOf(forest: Forest, id: string) {
  const out: string[] = [];
  let cursor = forest.nodes.get(id)?.parentId;
  while (cursor && !out.includes(cursor)) { out.push(cursor); cursor = forest.nodes.get(cursor)?.parentId; }
  return out;
}

export function visibleRows(forest: Forest, options: { expanded: ReadonlySet<string>; query?: string; filter?: TreeFilter }): TreeRow[] {
  const { expanded, query = '', filter = 'all' } = options;
  const rows: TreeRow[] = [];
  if (filter === 'top') {
    for (const id of forest.roots) {
      const node = forest.nodes.get(id)!;
      if (!query.trim() || agentMatches(node.agent, query)) rows.push({ id, depth: 0, hasChildren: false, expanded: false, match: Boolean(query.trim()) });
    }
    return rows;
  }
  const filtering = Boolean(query.trim()) || filter !== 'all';
  const matched = new Set<string>();
  const keep = new Set<string>();
  const autoOpen = new Set<string>();
  if (filtering) {
    for (const [id, node] of forest.nodes) {
      const passes = agentMatches(node.agent, query)
        && (filter !== 'working' || node.status === 'working')
        && (filter !== 'attention' || node.status === 'errored');
      if (!passes) continue;
      matched.add(id); keep.add(id);
      for (const ancestor of ancestorsOf(forest, id)) { keep.add(ancestor); autoOpen.add(ancestor); }
    }
  }
  const walk = (id: string, depth: number, revealed: boolean) => {
    if (filtering && !keep.has(id) && !revealed) return;
    const node = forest.nodes.get(id)!;
    const open = node.children.length > 0 && (expanded.has(id) || (filtering && autoOpen.has(id)));
    rows.push({ id, depth, hasChildren: node.children.length > 0, expanded: open, match: matched.has(id) });
    if (!open) return;
    for (const child of node.children) walk(child, depth + 1, filtering && expanded.has(id));
  };
  for (const root of forest.roots) walk(root, 0, false);
  return rows;
}

/** Every node that has children; used by "Expand all". */
export function branchIds(forest: Forest) {
  return [...forest.nodes.values()].filter(node => node.children.length).map(node => node.agent.id);
}
