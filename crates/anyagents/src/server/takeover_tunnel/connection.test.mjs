import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

const source = readFileSync(new URL('./connection.js', import.meta.url), 'utf8');

function fixture() {
  let time = 0, id = 0, reconnects = 0, suspends = 0;
  const timers = new Map(), navigations = [], requests = [], responses = [], events = [];
  const pill = {style: {}, setAttribute() {}};
  const body = {append(node) { assert.equal(node, pill); }};
  class EventSource extends EventTarget {
    constructor(url) { super(); assert.equal(url.pathname, '/takeover/session/events'); events.push(this); }
    close() { this.closed = true; }
  }
  const transport = vm.runInNewContext(source.replace('export function', 'function') + '\n;desktopTransport', {
    URL, AbortSignal, EventSource,
    location: {href: 'https://example.test/takeover/session/', replace(url) { navigations.push(url.pathname); }},
    document: {body, createElement: () => pill},
    performance: {now: () => time},
    setTimeout(fn, delay) { timers.set(++id, {fn, when: time + delay}); return id; },
    clearTimeout(timer) { timers.delete(timer); },
    async fetch(url, opts) {
      assert.equal(opts.credentials, 'same-origin');
      requests.push(url.pathname);
      return responses.shift() ?? new Response(null, {status: 204});
    },
  })(() => reconnects++);
  transport.events(() => {}, () => suspends++);
  const tick = async (expected) => {
    assert.equal(timers.size, 1);
    const [key, {fn, when}] = timers.entries().next().value;
    assert.equal(when - time, expected);
    time = when;
    timers.delete(key);
    await fn();
  };
  return {transport, pill, events, tick, timers, responses, requests, navigations,
    reconnects: () => reconnects, suspends: () => suspends};
}

test('RFB and SSE recover in the same page, honoring retry delay', async () => {
  const f = fixture();
  assert.equal(f.pill.hidden, true);
  f.transport.lost('rfb');
  f.events[0].dispatchEvent(new Event('error'));
  assert.equal(f.events[0].closed, true);
  assert.equal(f.suspends(), 1);
  assert.equal(f.pill.textContent, 'Reconnecting…');
  assert.equal(f.pill.hidden, false);
  f.responses.push(new Response(null, {status: 503, headers: {'Retry-After': '2'}}));
  await f.tick(500);
  assert.equal(f.reconnects(), 0);
  await f.tick(2000);
  assert.equal(f.reconnects(), 1);
  assert.equal(f.events.length, 2);
  f.transport.ready('rfb');
  assert.equal(f.pill.hidden, false);
  f.events[1].dispatchEvent(new MessageEvent('state', {data: '{"holder":"human"}'}));
  assert.equal(f.pill.hidden, true);
  assert.equal(f.timers.size, 0);
  assert.deepEqual(f.navigations, []);
  f.transport.lost('rfb');
  await f.tick(500); // A successful handshake reset its backoff.
});

for (const status of [401, 404, 410]) {
  test('terminal ' + status + ' stops both retries and navigates to the existing receipt or recovery', async () => {
    const f = fixture();
    const returned = status === 404;
    f.responses.push(new Response(null, {status}), new Response(null, {status: returned ? 200 : 404}));
    f.transport.lost('rfb');
    await f.tick(500);
    f.transport.lost('rfb');
    assert.equal(f.reconnects(), 0);
    assert.deepEqual(f.requests, ['/takeover/session/_connection', '/takeover/session/_returned']);
    assert.deepEqual(f.navigations, [returned ? '/takeover/session/_returned' : '/takeover/session/']);
    assert.equal(f.timers.size, 0);
  });
}
