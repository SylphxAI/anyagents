import { test } from 'node:test';
import assert from 'node:assert/strict';
import { accountMatches, classify, groupProfiles, needsQuota, providerName, statusMeta } from './providers.ts';
import { rank, highlight } from './search.ts';
import type { ProfileLike } from './providers.ts';

const profile = (provider: string, name: string, usage?: string, extra: Partial<ProfileLike> = {}): ProfileLike =>
  ({ provider, profile: name, kind: ['anthropic', 'openai', 'google-antigravity'].includes(provider) ? 'subscription' : 'api-key', usage, ...extra });

test('expired credentials, rate limits and account-wide exhaustion retain their own statuses', () => {
  assert.equal(classify(profile('anthropic', 'a', 'oauth expired')).status, 'expired');
  assert.equal(classify(profile('anthropic', 'a', 'oauth expired · 5h: 90% left')).status, 'expired');
  assert.equal(classify(profile('openai', 'b', 'mo: 0% left (pro)')).status, 'exhausted');
  assert.equal(classify(profile('openai', 'b', 'limited ↻1h · 5h: 30% left')).status, 'limited');
  assert.equal(classify(profile('anthropic', 'c', '5h: 82% left · 7d: 14% left')).status, 'low');
  assert.equal(classify(profile('anthropic', 'd', '5h: 82% left · opus: 51% left')).status, 'healthy');
  assert.equal(classify(profile('kimi-code-plan', 'e', 'active (12 models)')).status, 'healthy');
  assert.equal(classify(profile('kimi-code-plan', 'f')).status, 'connected');
});

test('a constrained model pool warns without claiming account-wide exhaustion', () => {
  const partial = classify(profile('anthropic', 'a', '5h: 84% left · 7d: 33% left · opus: 0% left'));
  assert.equal(partial.status, 'partial');
  assert.equal(statusMeta[partial.status].label, 'Partial');
  assert.ok(needsQuota(partial));
  assert.equal(classify(profile('anthropic', 'b', '5h: 84% left · Sonnet: 19% left')).status, 'partial');
  assert.equal(classify(profile('anthropic', 'c', 'opus: 0% left')).status, 'partial');
  assert.equal(classify(profile('anthropic', 'd', '5h: 84% left · 7d: 0% left · opus: 51% left')).status, 'exhausted');
  assert.equal(classify(profile('anthropic', 'e', '5h: 19% left · opus: 0% left')).status, 'low');
  assert.equal(classify(profile('anthropic', 'f', 'oauth expired · 5h: 84% left · opus: 0% left')).status, 'expired');
  assert.equal(classify(profile('anthropic', 'g', 'limited · 5h: 84% left · opus: 0% left')).status, 'limited');
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
  assert.equal(anthropic.quotaWarnings, 1);
  assert.deepEqual(anthropic.averages, [{ label: '5h', left: 35 }, { label: '7d', left: 40 }]);
  assert.deepEqual(anthropic.accounts.map(account => account.status), ['expired', 'low', 'healthy']);
  assert.deepEqual(groups[1].averages, []);
});

test('provider averages use only accounts that report each bucket, including measured zeros', () => {
  const [group] = groupProfiles([
    profile('anthropic', 'one', 'Opus: 0% left · 5h: 84% left · 7d: 33% left'),
    profile('anthropic', 'two', 'mo: 60% left · 5h: 60% left · weekly: 42% left · Sonnet: 54% left'),
    profile('anthropic', 'three', '5h: 0% left · opus: 41% left'),
    profile('anthropic', 'expired', 'oauth expired · 5h: 100% left · 7d: 100% left · opus: 100% left'),
    profile('anthropic', 'limited', 'limited ↻1h'),
    profile('anthropic', 'unreported'),
  ]);
  assert.deepEqual(group.averages, [
    { label: '5h', left: 48 }, { label: '7d', left: 38 }, { label: 'mo', left: 60 },
    { label: 'opus', left: 21 }, { label: 'sonnet', left: 54 },
  ]);
  assert.equal(group.quotaWarnings, 3);
  assert.deepEqual(groupProfiles([profile('anthropic', 'one', 'opus: 0% left')])[0].averages, [{ label: 'opus', left: 0 }]);
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
