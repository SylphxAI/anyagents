import { test } from 'node:test';
import assert from 'node:assert/strict';
import { agentMatches, ancestorsOf, branchIds, buildForest, visibleRows } from './tree.ts';
import type { AgentLike } from './status.ts';

const agent = (name: string, parent?: string, extra: Partial<AgentLike> = {}): AgentLike =>
  ({ id: 'id-' + name, name, parent, state: 'idle', harness: 'codex', model: 'gpt-5', ...extra });

const fleet = [
  agent('ops-director', undefined, { role: 'director' }),
  agent('ops-patrol', 'ops-director', { has_open_turn: true }),
  agent('ops-patrol-child', 'ops-patrol', { last_error: 'boom' }),
  agent('ops-scribe', 'ops-director'),
  agent('rd-director', undefined, { role: 'director', harness: 'claude-code' }),
  agent('rd-builder', 'rd-director', { has_open_turn: true, model: 'claude-opus-4' }),
  agent('solo'),
  agent('old', 'rd-director', { state: 'archived' }),
  agent('orphan', 'missing-parent'),
];
const names = (forest: ReturnType<typeof buildForest>, rows: ReturnType<typeof visibleRows>) =>
  rows.map(row => '  '.repeat(row.depth) + forest.nodes.get(row.id)!.agent.name);

test('forest defaults to top-level directors, alphabetically, without archived agents', () => {
  const forest = buildForest(fleet);
  assert.deepEqual(names(forest, visibleRows(forest, { expanded: new Set() })), ['ops-director', 'orphan', 'rd-director', 'solo']);
  assert.equal(forest.nodes.has('id-old'), false);
});

test('rollups count every descendant with working and errored totals', () => {
  const forest = buildForest(fleet);
  assert.deepEqual(forest.nodes.get('id-ops-director')!.rollup, { total: 3, working: 1, errored: 1 });
  assert.deepEqual(forest.nodes.get('id-rd-director')!.rollup, { total: 1, working: 1, errored: 0 });
  assert.deepEqual(forest.nodes.get('id-solo')!.rollup, { total: 0, working: 0, errored: 0 });
});

test('expanding a director reveals only its immediate children', () => {
  const forest = buildForest(fleet);
  const rows = visibleRows(forest, { expanded: new Set(['id-ops-director']) });
  assert.deepEqual(names(forest, rows), ['ops-director', '  ops-patrol', '  ops-scribe', 'orphan', 'rd-director', 'solo']);
  assert.equal(rows[0].expanded, true);
  assert.equal(rows[1].hasChildren, true);
  assert.equal(rows[1].expanded, false);
});

test('collapse all / expand all', () => {
  const forest = buildForest(fleet);
  const all = visibleRows(forest, { expanded: new Set(branchIds(forest)) });
  assert.equal(all.length, forest.nodes.size);
  assert.equal(visibleRows(forest, { expanded: new Set() }).length, forest.roots.length);
});

test('search auto-expands the branch leading to a deep match and marks it', () => {
  const forest = buildForest(fleet);
  const rows = visibleRows(forest, { expanded: new Set(), query: 'patrol-child' });
  assert.deepEqual(names(forest, rows), ['ops-director', '  ops-patrol', '    ops-patrol-child']);
  assert.deepEqual(rows.map(row => row.match), [false, false, true]);
});

test('search matches role, harness, model, and state tokens', () => {
  const forest = buildForest(fleet);
  assert.deepEqual(names(forest, visibleRows(forest, { expanded: new Set(), query: 'claude opus' })), ['rd-director', '  rd-builder']);
  assert.ok(agentMatches(fleet[0], 'director'));
  assert.ok(agentMatches(fleet[2], 'errored'));
  assert.equal(agentMatches(fleet[0], 'nomatch'), false);
});

test('working and attention filters keep ancestor context', () => {
  const forest = buildForest(fleet);
  assert.deepEqual(names(forest, visibleRows(forest, { expanded: new Set(), filter: 'working' })),
    ['ops-director', '  ops-patrol', 'rd-director', '  rd-builder']);
  assert.deepEqual(names(forest, visibleRows(forest, { expanded: new Set(), filter: 'attention' })),
    ['ops-director', '  ops-patrol', '    ops-patrol-child']);
});

test('top-level filter lists roots only and never expands', () => {
  const forest = buildForest(fleet);
  const rows = visibleRows(forest, { expanded: new Set(branchIds(forest)), filter: 'top' });
  assert.deepEqual(names(forest, rows), ['ops-director', 'orphan', 'rd-director', 'solo']);
  assert.ok(rows.every(row => !row.expanded && !row.hasChildren));
});

test('parent cycles never hang and orphaned agents become roots', () => {
  const forest = buildForest([agent('a', 'b'), agent('b', 'a')]);
  assert.equal(forest.roots.length, 1);
  assert.equal(visibleRows(forest, { expanded: new Set(['id-a', 'id-b']) }).length, 2);
  assert.deepEqual(ancestorsOf(buildForest(fleet), 'id-ops-patrol-child'), ['id-ops-patrol', 'id-ops-director']);
});

test('archived agents can be included on request', () => {
  assert.equal(buildForest(fleet, true).nodes.has('id-old'), true);
});
