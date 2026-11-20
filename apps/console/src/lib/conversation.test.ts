import { test } from 'node:test';
import assert from 'node:assert/strict';
import { groupTurns, recipientLabel } from './conversation.ts';

const turn = (seq: number, to: string, text = 'Ship it', extra = {}) =>
  ({ seq, from: 'rd-director', direction: 'out', to, text, timestamp_ms: 1_000 + seq * 100, ...extra });

test('one send to several recipients becomes one row with every recipient', () => {
  const grouped = groupTurns([turn(1, 'agent:a'), turn(2, 'agent:b'), turn(3, 'agent:c')]);
  assert.equal(grouped.length, 1);
  assert.deepEqual(grouped[0].recipients, ['agent:a', 'agent:b', 'agent:c']);
  assert.equal(grouped[0].seq, 1);
});

test('duplicate recipients are listed once', () => {
  assert.deepEqual(groupTurns([turn(1, 'agent:a'), turn(2, 'agent:a')])[0].recipients, ['agent:a']);
});

test('different text, sender, direction, or a later send stay separate', () => {
  assert.equal(groupTurns([turn(1, 'agent:a'), turn(2, 'agent:b', 'Different')]).length, 2);
  assert.equal(groupTurns([turn(1, 'agent:a'), turn(2, 'agent:b', 'Ship it', { from: 'other' })]).length, 2);
  assert.equal(groupTurns([turn(1, 'agent:a'), turn(2, 'agent:b', 'Ship it', { direction: 'in' })]).length, 2);
  assert.equal(groupTurns([turn(1, 'agent:a'), turn(2, 'agent:b', 'Ship it', { timestamp_ms: 60_000 })]).length, 2);
});

test('only adjacent copies merge, and empty texts never merge', () => {
  assert.equal(groupTurns([turn(1, 'agent:a'), turn(2, 'operator', 'Ack', { direction: 'in' }), turn(3, 'agent:b')]).length, 3);
  assert.equal(groupTurns([turn(1, 'agent:a', ''), turn(2, 'agent:b', '')]).length, 2);
});

test('recipient labels drop the agent scheme only', () => {
  assert.equal(recipientLabel('agent:rd-director'), 'rd-director');
  assert.equal(recipientLabel('telegram:ops-room'), 'telegram:ops-room');
});
