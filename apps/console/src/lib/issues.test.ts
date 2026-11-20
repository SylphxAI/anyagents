import assert from 'node:assert/strict';
import test from 'node:test';
import { runtimeIssues } from './issues.ts';

const row = { id: 'rti_example', component: 'harness', error_fingerprint: 'fixture-launch-failure',
  summary: 'Guest harness launch failed', exact_error: 'Executable was not found', repro_steps: 'Unavailable.',
  source: 'auto_capture', status: 'open', sample_agent_id: null, occurrence_count: 7,
  first_seen_at: 1795214400000, last_seen_at: 1795214460000 };

test('system issues consume the runtime issues envelope and nullable sample agent', () => {
  const { exact_error, repro_steps, first_seen_at, ...rendered } = row;
  assert.deepEqual(runtimeIssues({ issues: [row] }), [{ ...rendered, sample_agent_id: undefined }]);
  assert.deepEqual(runtimeIssues({ issues: [] }), []);
});

test('system issues keep the newest snapshot per component and fingerprint without inflating counts', () => {
  const old = { ...row, occurrence_count: 4, status: 'resolved', last_seen_at: 1795214400000 };
  const distinct = { ...old, id: 'rti_other', component: 'sandbox', sample_agent_id: 'agt_fixture' };
  const issues = runtimeIssues({ issues: [row, old, distinct] });
  assert.deepEqual(issues.map(x => [x.component, x.status, x.occurrence_count, x.sample_agent_id]), [
    ['harness', 'open', 7, undefined], ['sandbox', 'resolved', 4, 'agt_fixture'],
  ]);
});

test('system issues ignore malformed rows and unsafe optional values', () => {
  assert.deepEqual(runtimeIssues(null), []);
  assert.deepEqual(runtimeIssues({ issues: {} }), []);
  const issues = runtimeIssues({ issues: [null, 'bad', { summary: 'missing identity' }, { ...row, last_seen_at: 'later', occurrence_count: -1, source: {} }] });
  assert.equal(issues.length, 1);
  assert.equal(issues[0].last_seen_at, undefined);
  assert.equal(issues[0].occurrence_count, undefined);
  assert.equal(issues[0].source, undefined);
});
