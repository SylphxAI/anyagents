import assert from 'node:assert/strict';
import { test } from 'node:test';
import { quotas, quotaState } from './profileQuota.ts';

test('parses arbitrary provider buckets, including zero and adjacent windows', () => {
  assert.deepEqual(quotas('opus: 51% left · 5h: 82% left'), [
    { label: 'opus', left: 51, detail: '' }, { label: '5h', left: 82, detail: '' },
  ]);
  assert.deepEqual(quotas('mo: 0% left'), [{ label: 'mo', left: 0, detail: '' }]);
  assert.deepEqual(quotas('sonnet: 12% used · 7d: 85% left ↻2d · wk: 25% left; day: 50% left, req: 100% left')
    .map(({ label, left }) => [label, left]), [['sonnet', 88], ['7d', 85], ['wk', 25], ['day', 50], ['req', 100]]);
});

test('keeps nested windows and reset details attached to the top-level bucket', () => {
  assert.deepEqual(quotas('gemini: 84% left (5h: 100%, 7d: 84%) · opus: 0% left ↻1h'), [
    { label: 'gemini', left: 84, detail: '(5h: 100%, 7d: 84%)' },
    { label: 'opus', left: 0, detail: '↻1h' },
  ]);
  assert.deepEqual(quotas('75% left'), [{ label: 'Quota', left: 75, detail: '' }]);
});

test('keeps expired, constrained, unknown and healthy signals truthful', () => {
  assert.deepEqual(quotaState('oauth expired'), { windows: [], expired: true, low: false, healthy: false });
  assert.equal(quotaState('oauth expired · mo: 0% left').low, true);
  assert.equal(quotaState('mo: 35% left').healthy, false);
  assert.equal(quotaState('mo: 36% left').healthy, true);
  assert.equal(quotaState().healthy, false);
  assert.deepEqual(quotas('quota: 234% left'), []);
});
