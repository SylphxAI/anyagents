import { test } from 'node:test';
import assert from 'node:assert/strict';
import { accountMatches, classify, groupProfiles, providerName } from './providers.ts';
import { rank, highlight } from './search.ts';
import type { ProfileLike } from './providers.ts';

const profile = (provider: string, name: string, usage?: string, extra: Partial<ProfileLike> = {}): ProfileLike =>
  ({ provider, profile: name, kind: ['anthropic', 'openai', 'google-antigravity'].includes(provider) ? 'subscription' : 'api-key', usage, ...extra });

test('accounts are classified by OAuth state and lowest remaining window', () => {
  assert.equal(classify(profile('anthropic', 'a', 'oauth expired')).status, 'expired');
  assert.equal(classify(profile('anthropic', 'a', 'oauth expired · 5h: 90% left')).status, 'expired');
  assert.equal(classify(profile('openai', 'b', 'mo: 0% left (pro)')).status, 'exhausted');
  assert.equal(classify(profile('openai', 'b', 'limited ↻1h · 5h: 30% left')).status, 'exhausted');
  assert.equal(classify(profile('anthropic', 'c', '5h: 82% left · 7d: 14% left')).status, 'low');
  assert.equal(classify(profile('anthropic', 'd', '5h: 82% left · opus: 51% left')).status, 'healthy');
  assert.equal(classify(profile('kimi-code-plan', 'e', 'active (12 models)')).status, 'healthy');
  assert.equal(classify(profile('kimi-code-plan', 'f')).status, 'connected');
});

test('OAuth identity prefers the provider-reported account over the slot name', () => {
  const account = classify(profile('google-antigravity', 'slot-3', '5h: 50% left', { account: 'dev@example.test' }));
  assert.equal(account.identity, 'dev@example.test');
  assert.equal(account.oauth, true);
  assert.equal(classify(profile('sylphx', 'team-key')).identity, 'team-key');
  assert.equal(classify(profile('sylphx', 'team-key')).oauth, false);
});

test('profiles group by provider with summary counts, sorted by display name', () => {
  const groups = groupProfiles([
    profile('openai', 'one', '5h: 90% left'),
    profile('anthropic', 'two', 'oauth expired'),
    profile('anthropic', 'three', '5h: 10% left'),
    profile('anthropic', 'four', '5h: 60% left · 7d: 40% left'),
    profile('kimi-code-plan', 'five'),
  ]);
  assert.deepEqual(groups.map(group => group.name), ['Anthropic', 'Kimi Code Plan', 'OpenAI']);
  const anthropic = groups[0];
  assert.equal(anthropic.accounts.length, 3);
  assert.equal(anthropic.expired, 1);
  assert.equal(anthropic.low, 1);
  assert.equal(anthropic.healthy, 1);
  assert.equal(anthropic.averageLeft, 25);
  assert.deepEqual(anthropic.accounts.map(account => account.status), ['expired', 'low', 'healthy']);
  assert.equal(groups[1].averageLeft, null);
});

test('provider names fall back to title case', () => {
  assert.equal(providerName('google-antigravity'), 'Google Antigravity');
  assert.equal(providerName('kimi-code-plan'), 'Kimi Code Plan');
  assert.equal(providerName('new-vendor_plan'), 'New Vendor Plan');
});

test('account search spans identity, provider, status, and assigned agents', () => {
  const account = classify(profile('anthropic', 'slot', 'oauth expired', { account: 'ops@example.test', assigned_agents: ['rd-director'] }));
  assert.ok(accountMatches(account, 'anthropic expired'));
  assert.ok(accountMatches(account, 'ops@'));
  assert.ok(accountMatches(account, 'rd-director'));
  assert.equal(accountMatches(account, 'openai'), false);
});

test('palette ranking prefers prefix, then word boundary, then fuzzy', () => {
  const prefix = rank('ops', 'ops-director');
  const boundary = rank('dir', 'ops-director');
  const fuzzy = rank('odr', 'ops-director');
  assert.equal(prefix, 0);
  assert.ok(boundary < fuzzy);
  assert.equal(rank('zzz', 'ops-director'), Infinity);
});

test('highlight marks every token occurrence', () => {
  assert.deepEqual(highlight('ops-patrol-ops', 'ops'), [
    { text: 'ops', hit: true }, { text: '-patrol-', hit: false }, { text: 'ops', hit: true }]);
  assert.deepEqual(highlight('abc', ''), [{ text: 'abc', hit: false }]);
});
