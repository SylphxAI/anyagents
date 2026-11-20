import { status } from '../types.ts';
import type { Agent, AgentState } from '../types.ts';

// AgentView omits this field when it has no aliases.
type RosterAgent = Agent & { aliases?: readonly string[] };

export function agentLookup(agents: readonly RosterAgent[]) {
  const ids = new Map(agents.map(agent => [agent.id, agent]));
  const names = new Map<string, RosterAgent | undefined>();
  const aliases = new Map<string, RosterAgent | undefined>();
  function index(map: typeof names, key: string, agent: RosterAgent) {
    if (!map.has(key)) map.set(key, agent);
    else if (map.get(key)?.id !== agent.id) map.set(key, undefined);
  }
  for (const agent of ids.values()) {
    index(names, agent.name, agent);
    for (const alias of agent.aliases ?? []) index(aliases, alias, agent);
  }
  return (reference?: string) => {
    if (!reference) return;
    const bare = reference.trim().replace(/^agent:/, '');
    // Canonical IDs take precedence over names and aliases. Don't guess an ambiguous name/alias.
    return ids.get(reference) ?? ids.get(bare) ?? (names.has(bare) ? names.get(bare) : aliases.get(bare));
  };
}

interface Node {
  agent: RosterAgent;
  state: AgentState;
  parent?: Node;
  children: Node[];
  activeDescendants: number;
}

export function buildForest(agents: readonly RosterAgent[]) {
  const sorted = [...agents].sort((a, b) => a.name.localeCompare(b.name) || a.id.localeCompare(b.id));
  const nodes = new Map<string, Node>();
  for (const agent of sorted) {
    if (!nodes.has(agent.id)) nodes.set(agent.id, { agent, state: status(agent), children: [], activeDescendants: 0 });
  }
  const resolve = agentLookup([...nodes.values()].map(node => node.agent));
  for (const node of nodes.values()) {
    const parent = resolve(node.agent.parent);
    if (parent) node.parent = nodes.get(parent.id);
  }

  // Break one edge per cycle before building child lists. Nodes leading into a cycle keep their parents.
  const settled = new Set<Node>();
  const rank = new Map([...nodes.values()].map((node, index) => [node, index]));
  for (const start of nodes.values()) {
    const path: Node[] = [];
    const position = new Map<Node, number>();
    let node: Node | undefined = start;
    while (node && !settled.has(node)) {
      const loop = position.get(node);
      if (loop !== undefined) {
        const root = path.slice(loop).reduce((first, member) => rank.get(member)! < rank.get(first)! ? member : first);
        root.parent = undefined;
        break;
      }
      position.set(node, path.length);
      path.push(node);
      node = node.parent;
    }
    for (const member of path) settled.add(member);
  }

  const roots: Node[] = [];
  for (const node of nodes.values()) (node.parent?.children ?? roots).push(node);
  const preorder: Node[] = [];
  const pending = [...roots].reverse();
  while (pending.length) {
    const node = pending.pop()!;
    preorder.push(node);
    for (let i = node.children.length - 1; i >= 0; i--) pending.push(node.children[i]);
  }
  // Active means non-archived (including idle and paused), not just agents executing a turn.
  for (let i = preorder.length - 1; i >= 0; i--) {
    const node = preorder[i];
    if (node.parent) node.parent.activeDescendants += node.activeDescendants + (node.state === 'archived' ? 0 : 1);
  }
  return { roots, preorder };
}

const maxIndent = 6;
interface TreeRow {
  node: Node;
  depth: number;
  rails: readonly boolean[];
  last: boolean;
  context: boolean;
  branch: boolean;
  expanded: boolean;
  stem: boolean;
  showParent: boolean;
}

export function projectForest(forest: ReturnType<typeof buildForest>, filter: AgentState | 'all', search: string, folded: ReadonlySet<string>) {
  const query = search.trim().toLowerCase();
  const filtering = Boolean(query) || filter !== 'all';
  const matches = new Set<Node>();
  const kept = new Set<Node>();
  if (filtering) {
    for (let i = forest.preorder.length - 1; i >= 0; i--) {
      const node = forest.preorder[i];
      const a = node.agent;
      if ((filter === 'all' || node.state === filter) && (!query ||
        [a.name, a.id, a.role, a.harness, a.runtime, a.model, a.profile, ...(a.aliases ?? [])]
          .some(field => field?.toLowerCase().includes(query)))) {
        matches.add(node);
        kept.add(node);
      }
      if (node.parent && kept.has(node)) kept.add(node.parent);
    }
  }

  const rows: TreeRow[] = [];
  const pending: Pick<TreeRow, 'node' | 'depth' | 'rails' | 'last'>[] = [];
  function pushSiblings(siblings: Node[], depth: number, rails: readonly boolean[]) {
    const visible = filtering ? siblings.filter(node => kept.has(node)) : siblings;
    for (let i = visible.length - 1; i >= 0; i--) pending.push({ node: visible[i], depth, rails, last: i === visible.length - 1 });
  }
  pushSiblings(forest.roots, 0, []);
  while (pending.length) {
    const row = pending.pop()!;
    const { node, depth, rails, last } = row;
    const branch = filtering ? node.children.some(child => kept.has(child)) : node.children.length > 0;
    const expanded = branch && !folded.has(node.agent.id);
    rows.push({ ...row, context: filtering && !matches.has(node), branch, expanded, stem: expanded && depth < maxIndent, showParent: depth > maxIndent });
    if (expanded) pushSiblings(node.children, depth + 1, depth > 0 && depth < maxIndent ? [...rails, !last] : rails);
  }
  return { rows, matchCount: filtering ? matches.size : forest.preorder.length };
}
