import { test } from 'node:test';
import assert from 'node:assert/strict';
import { parseUsage, windowLabel } from './quota.ts';

const pick = (usage: string) => parseUsage(usage).windows.map(({ label, left, reset }) => reset ? [label, left, reset] : [label, left]);

test('empty and sentinel readings carry no quota', () => {
  for (const value of [undefined, null, '', '   ', '__NONE__']) {
    const reading = parseUsage(value);
    assert.deepEqual(reading.windows, []);
    assert.deepEqual(reading.modelPools, []);
    assert.equal(reading.expired, false);
  }
});

test('canonical 5h + 7d reading with reset ETAs and plan tier', () => {
  const reading = parseUsage('5h: 72% left ↻2h15m · 7d: 45% left ↻3d04h (pro)');
  assert.deepEqual(pick('5h: 72% left ↻2h15m · 7d: 45% left ↻3d04h (pro)'), [['5h', 72, '2h15m'], ['7d', 45, '3d04h']]);
  assert.equal(reading.tier, 'pro');
});

test('keeps healthy account windows separate from an exhausted model pool', () => {
  const reading = parseUsage('5h: 84% left · 7d: 33% left · Opus: 0% left ↻4d11h (max)');
  assert.deepEqual(reading.windows, [{ label: '5h', left: 84 }, { label: '7d', left: 33 }]);
  assert.deepEqual(reading.modelPools, [{ label: 'opus', left: 0, reset: '4d11h' }]);
  assert.equal(reading.tier, 'max');
});

test('monthly window at zero: "mo: 0% left" with balance tier', () => {
  const reading = parseUsage('mo: 0% left ($0.00/$20 pro)');
  assert.deepEqual(pick('mo: 0% left ($0.00/$20 pro)'), [['mo', 0]]);
  assert.equal(reading.tier, '$0.00/$20 pro');
});

test('5h + mo pair', () => {
  assert.deepEqual(pick('5h: 82% left ↻3h10m · mo: 37% left ↻12d'), [['5h', 82, '3h10m'], ['mo', 37, '12d']]);
});

test('single 5h window with short tier: "5h: 82% left"', () => {
  const reading = parseUsage('5h: 82% left (lite)');
  assert.deepEqual(reading.windows, [{ label: '5h', left: 82 }]);
  assert.equal(reading.tier, 'lite');
});

test('single 7d window with reset', () => {
  assert.deepEqual(pick('7d: 12% left ↻2d03h'), [['7d', 12, '2d03h']]);
});

test('separates other model pools and normalizes account window aliases', () => {
  const usage = 'Sonnet: 77% left · gemini: 0% left · wk: 30% left · day: 95% left · req: 8% left · weekly: 40% left';
  assert.deepEqual(pick(usage), [['wk', 30], ['day', 95], ['req', 8], ['7d', 40]]);
  assert.deepEqual(parseUsage(usage).modelPools, [{ label: 'sonnet', left: 77 }, { label: 'gemini', left: 0 }]);
});

test('used-mode readings invert to remaining', () => {
  assert.deepEqual(pick('5h: 30% used · 7d: 100% used'), [['5h', 70], ['7d', 0]]);
});

test('percentages clamp to 0..100 and round decimals', () => {
  assert.deepEqual(pick('5h: 140% left · 7d: 33.6% left'), [['5h', 100], ['7d', 34]]);
});

test('bare percentage reading', () => {
  assert.deepEqual(pick('73% left'), [['quota', 73]]);
});

test('"oauth expired" alone', () => {
  const reading = parseUsage('oauth expired');
  assert.equal(reading.expired, true);
  assert.deepEqual(reading.windows, []);
});

test('"oauth expired" keeps the last known windows', () => {
  const reading = parseUsage('oauth expired · 5h: 40% left · 7d: 12% left (pro)');
  assert.equal(reading.expired, true);
  assert.deepEqual(reading.windows.map(window => [window.label, window.left]), [['5h', 40], ['7d', 12]]);
  assert.equal(reading.tier, 'pro');
});

test('OAuth expiry is case-insensitive', () => {
  assert.equal(parseUsage('OAuth Expired').expired, true);
});

test('rate-limited prefix with ETA, with and without separator', () => {
  const separated = parseUsage('limited ↻1h20m · 5h: 0% left ↻1h20m · 7d: 61% left');
  assert.equal(separated.limited, true);
  assert.equal(separated.limitedReset, '1h20m');
  assert.deepEqual(separated.windows.map(window => window.left), [0, 61]);
  const spaced = parseUsage('limited ↻45m 5h: 12% left');
  assert.equal(spaced.limited, true);
  assert.deepEqual(spaced.windows.map(window => [window.label, window.left]), [['5h', 12]]);
  const limitedOnly = parseUsage('limited');
  assert.equal(limitedOnly.limited, true);
  assert.deepEqual(limitedOnly.windows, []);
  assert.deepEqual(limitedOnly.modelPools, []);
});

test('prepaid balance and model-count notes are not quota windows', () => {
  const balance = parseUsage('$12.40 left ($88 / $100)');
  assert.equal(balance.balance, '$12.40 left ($88 / $100)');
  assert.deepEqual(balance.windows, []);
  const active = parseUsage('active (12 models)');
  assert.equal(active.note, 'active (12 models)');
  assert.equal(active.tier, undefined);
});

test('window labels read naturally', () => {
  assert.equal(windowLabel('mo'), 'Month');
  assert.equal(windowLabel('opus'), 'Opus');
  assert.equal(windowLabel('5h'), '5h');
  assert.equal(windowLabel('custom'), 'Custom');
});
