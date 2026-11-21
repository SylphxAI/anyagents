import assert from 'node:assert/strict';
import { test } from 'node:test';
import { readSignInStream } from './signin-stream.ts';

const bytes = (text: string) => new TextEncoder().encode(text);

test('a split capacity error rejects before an open stream closes', { timeout: 1000 }, async () => {
  let cancelled = false;
  const stream = new ReadableStream<Uint8Array<ArrayBuffer>>({
    start(controller) {
      controller.enqueue(bytes('{"kind":"session","text":"fixture"}\n{"kind":"er'));
      controller.enqueue(bytes('ror","text":"HTTP 503: NO_CAPACITY: sandbox unavailable"}\n'));
    },
    cancel() { cancelled = true; },
  });
  const seen: string[] = [];
  await assert.rejects(readSignInStream(stream, event => seen.push(event.kind)), {
    message: 'HTTP 503: NO_CAPACITY: sandbox unavailable',
  });
  assert.deepEqual(seen, ['session']);
  assert.ok(cancelled);
});

test('a terminal success wins without waiting for EOF', { timeout: 1000 }, async () => {
  const stream = new ReadableStream<Uint8Array<ArrayBuffer>>({
    start(controller) { controller.enqueue(bytes('{"kind":"done","text":"fixture"}\n')); },
  });
  assert.equal(await readSignInStream(stream, () => assert.fail()), 'fixture');
});

test('EOF without a terminal result is not a successful sign-in', async () => {
  const stream = new ReadableStream<Uint8Array<ArrayBuffer>>({ start(controller) { controller.close(); } });
  await assert.rejects(readSignInStream(stream, () => assert.fail()), /ended before/);
});
