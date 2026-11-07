import { test } from 'node:test';
import assert from 'node:assert/strict';
import { inputDispatcher } from '../crates/anyagents/src/browser_takeover.js';

test('slow input link coalesces 10,000 moves without losing press/release order', async () => {
  let resume;
  const sent = [];
  const queue = inputDispatcher(async payload => {
    sent.push(payload);
    if (sent.length === 1) await new Promise(resolve => { resume = resolve; });
    return { ok: true };
  });
  const press = queue.send({ type: 'mouse', event_type: 'mousePressed', x: 0 });
  for (let x = 1; x <= 10000; x++) queue.send({ type: 'mouse', event_type: 'mouseMoved', x });
  const release = queue.send({ type: 'mouse', event_type: 'mouseReleased', x: 10000 });
  queue.send({ type: 'mouse', event_type: 'mouseMoved', x: 10001 });
  resume(); await press; await release; await queue.idle();
  assert.deepEqual(sent.map(p => [p.event_type, p.x]), [
    ['mousePressed', 0], ['mouseMoved', 10000], ['mouseReleased', 10000], ['mouseMoved', 10001],
  ]);
});
test('wheel coalescing preserves distance but cannot cross a key boundary', async () => {
  let resume;
  const sent = [];
  const queue = inputDispatcher(async payload => {
    sent.push(payload);
    if (sent.length === 1) await new Promise(resolve => { resume = resolve; });
  });
  queue.send({ type: 'key', key: 'a' });
  for (let i = 0; i < 100; i++) queue.send({ type: 'wheel', delta_x: 2, delta_y: -3 });
  queue.send({ type: 'key', key: 'Enter' });
  queue.send({ type: 'wheel', delta_x: 1, delta_y: 1 });
  resume(); await queue.idle();
  assert.deepEqual(sent, [{ type: 'key', key: 'a' }, { type: 'wheel', delta_x: 200, delta_y: -300 }, { type: 'key', key: 'Enter' }, { type: 'wheel', delta_x: 1, delta_y: 1 }]);
});
test('failed possibly committed input is reported, never replayed', async () => {
  const sent = [], errors = [];
  const queue = inputDispatcher(async payload => { sent.push(payload); throw new Error('disconnected'); }, error => errors.push(error.message));
  await assert.rejects(queue.send({ type: 'text', text: 'once' }), /disconnected/);
  await queue.idle();
  assert.equal(sent.length, 1); assert.deepEqual(errors, ['disconnected']);
});
