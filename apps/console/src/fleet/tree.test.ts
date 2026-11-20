import assert from 'node:assert/strict';
import { test } from 'node:test';
import type { Agent } from '../types.ts';
import { agentLookup, buildForest, projectForest } from './tree.ts';

function agent(id: string, extras: Partial<Agent> & { aliases?: string[] } = {}) {
  return { id, name: id, state: 'idle', harness: 'test', ...extras };
}

const fleet = [
  agent('root', { name: 'Lead', aliases: ['previous-lead'] }),
  agent('a', { name: 'Alpha', parent: 'root' }),
  agent('b', { name: 'Beta', parent: 'agent:previous-lead', state: 'paused' }),
  agent('done', { name: 'Closed', parent: 'a', state: 'archived' }),
  agent('work', { name: 'Worker', parent: 'Closed', turn_state: 'in_turn', profile: 'daily' }),
];
const empty = new Set<string>();
const ids = (rows: ReturnType<typeof projectForest>['rows']) => rows.map(row => row.node.agent.id);

test('opens the full sorted hierarchy with continuous ancestor rails and live descendant counts', () => {
  const { rows, matchCount } = projectForest(buildForest(fleet), 'all', '', empty);
  assert.equal(matchCount, 5);
  assert.deepEqual(rows.map(({ node, depth, rails, last, expanded }) => [node.agent.id, depth, rails, last, expanded, node.activeDescendants]), [
    ['root', 0, [], true, true, 3],
    ['a', 1, [], false, true, 1],
    ['done', 2, [true], true, true, 1],
    ['work', 3, [true, false], true, false, 0],
    ['b', 1, [], true, false, 0],
  ]);
  assert.equal(rows[3].node.state, 'working');

  const folded = projectForest(buildForest(fleet), 'all', '', new Set(['a']));
  assert.deepEqual(ids(folded.rows), ['root', 'a', 'b']);
  assert.equal(folded.rows[1].expanded, false);
  assert.equal(folded.rows[1].node.activeDescendants, 1);
  assert.equal(folded.matchCount, 5);
});

test('search and status filters retain an unbroken, independently collapsible path to a match', () => {
  const forest = buildForest(fleet);
  const { rows, matchCount } = projectForest(forest, 'working', 'DAILY', empty);
  assert.deepEqual(ids(rows), ['root', 'a', 'done', 'work']);
  assert.deepEqual(rows.map(row => row.context), [true, true, true, false]);
  assert.deepEqual(rows[2].rails, [false]);
  assert.equal(rows[0].node.activeDescendants, 3);
  assert.equal(matchCount, 1);
  assert.deepEqual(ids(projectForest(forest, 'working', 'daily', new Set(['a'])).rows), ['root', 'a']);
  assert.deepEqual(ids(projectForest(forest, 'paused', '', empty).rows), ['root', 'b']);
  assert.deepEqual(ids(projectForest(forest, 'working', 'no match', empty).rows), []);
  assert.deepEqual(ids(projectForest(forest, 'all', 'previous-lead', empty).rows), ['root']);
  assert.equal(projectForest(forest, 'all', 'previous-lead', empty).rows[0].branch, false);
});

test('canonical IDs beat names and aliases; missing and ambiguous parents leave accessible roots', () => {
  const roster = [
    agent('canonical', { name: 'Leader', aliases: ['former', 'ambiguous'] }),
    agent('other', { name: 'canonical', aliases: ['former', 'ambiguous'] }),
    agent('named', { name: 'former' }),
    agent('by-id', { parent: 'canonical' }),
    agent('by-name', { parent: 'former' }),
    agent('unknown', { parent: 'not-loaded' }),
    agent('unknown2', { parent: 'ambiguous' }),
  ];
  const resolve = agentLookup(roster);
  assert.equal(resolve('agent:canonical')?.id, 'canonical');
  assert.equal(resolve('former')?.id, 'named');
  assert.equal(resolve('ambiguous'), undefined);
  assert.equal(agentLookup(fleet)('agent:previous-lead')?.id, 'root');
  assert.equal(resolve(), undefined);
  const rows = projectForest(buildForest(roster), 'all', '', empty).rows;
  const byId = new Map(rows.map(row => [row.node.agent.id, row]));
  assert.equal(byId.get('by-id')?.node.parent?.agent.id, 'canonical');
  assert.equal(byId.get('by-name')?.node.parent?.agent.id, 'named');
  assert.equal(byId.get('unknown')?.depth, 0);
  assert.equal(byId.get('unknown2')?.depth, 0);
});

test('self and multi-agent cycles break consistently without dropping or double-counting agents', () => {
  const roster = [agent('a', { parent: 'b' }), agent('b', { parent: 'a' }),
    agent('0-child', { parent: 'b' }), agent('self', { parent: 'self' })];
  const rows = projectForest(buildForest(roster), 'all', '', empty).rows;
  assert.deepEqual(ids(rows), ['a', 'b', '0-child', 'self']);
  assert.deepEqual(rows.map(row => row.node.activeDescendants), [2, 1, 0, 0]);
  const reversed = projectForest(buildForest([...roster].reverse()), 'all', '', empty);
  assert.deepEqual(ids(reversed.rows), ids(rows));
  assert.deepEqual(ids(projectForest(buildForest(roster), 'all', '', new Set(['b'])).rows), ['a', 'b', 'self']);
});

test('deep paths stay complete while bounding connector width', () => {
  const roster = Array.from({ length: 2000 }, (_, i) => agent(String(i), { parent: i ? String(i - 1) : undefined }));
  const { rows } = projectForest(buildForest(roster), 'all', '', empty);
  assert.equal(rows.length, roster.length);
  assert.equal(rows[1999].depth, 1999);
  assert.equal(rows[1999].node.parent?.agent.id, '1998');
  assert.equal(rows[0].node.activeDescendants, 1999);
  assert.ok(rows.every(row => row.rails.length <= 5));
  assert.equal(rows[6].showParent, false);
  assert.equal(rows[7].showParent, true);
});
