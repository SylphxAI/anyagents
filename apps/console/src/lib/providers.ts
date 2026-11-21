// Groups provider profiles into provider sections and classifies each account.
import { compareQuotaLabels, parseUsage } from './quota.ts';
import type { UsageReading } from './quota.ts';

export interface ProfileLike {
  provider: string;
  profile: string;
  kind: string;
  account?: string | null;
  usage?: string | null;
  usage_age_ms?: number | null;
  compatible_harnesses?: string[];
  assigned_agents?: string[];
  subagent_count?: number;
}

export type AccountStatus = 'expired' | 'limited' | 'exhausted' | 'low' | 'partial' | 'healthy' | 'connected';
export interface Account<P extends ProfileLike = ProfileLike> {
  key: string;
  profile: P;
  reading: UsageReading;
  status: AccountStatus;
  oauth: boolean;
  /** Primary display identity: email/account id for OAuth, label for keys. */
  identity: string;
}
export interface ProviderGroup<P extends ProfileLike = ProfileLike> {
  provider: string;
  name: string;
  accounts: Account<P>[];
  expired: number;
  quotaWarnings: number;
  averages: { label: string; left: number }[];
}

const NAMES: Record<string, string> = {
  anthropic: 'Anthropic', cursor: 'Cursor', deepseek: 'DeepSeek', 'glm-code-plan': 'GLM Code Plan',
  'google-antigravity': 'Google Antigravity', 'google-gemini': 'Google Gemini', 'kimi-code-plan': 'Kimi Code Plan',
  'minimax-code-plan': 'MiniMax Code Plan', muse: 'Muse', omp: 'OMP', openai: 'OpenAI', opencode: 'OpenCode',
  openrouter: 'OpenRouter', 'qwen-token-plan': 'Qwen Token Plan', spiron: 'Spiron', sylphx: 'Sylphx', xai: 'xAI',
};

export function providerName(provider: string) {
  return NAMES[provider] ?? provider.split(/[-_]/).filter(Boolean).map(word => word[0].toUpperCase() + word.slice(1)).join(' ');
}

/** Providers whose subscription login runs through the coordinator's native sign-in. */
export const OAUTH_PROVIDERS = [
  { provider: 'anthropic', harness: 'claude-code', name: 'Anthropic Claude', detail: 'Claude Pro or Max subscription' },
  { provider: 'google-antigravity', harness: 'agy', name: 'Google Antigravity', detail: 'Google account with Antigravity access' },
  { provider: 'openai', harness: 'codex', name: 'OpenAI / Codex', detail: 'ChatGPT plan with Codex access' },
] as const;

export const LOW_QUOTA = 20;

export function classify(profile: ProfileLike): Account {
  const reading = parseUsage(profile.usage);
  const oauth = profile.kind === 'subscription';
  let status: AccountStatus;
  if (reading.expired) status = 'expired';
  else if (reading.limited) status = 'limited';
  else if (reading.windows.some(window => window.left === 0)) status = 'exhausted';
  else if (reading.windows.some(window => window.left < LOW_QUOTA)) status = 'low';
  else if (reading.modelPools.some(pool => pool.left < LOW_QUOTA)) status = 'partial';
  else if (reading.windows.length || reading.modelPools.length || reading.balance || reading.note) status = 'healthy';
  else status = 'connected';
  const account = profile.account?.trim();
  return {
    key: profile.provider + '/' + profile.profile,
    profile, reading, status, oauth,
    identity: account || profile.profile,
  };
}

export const statusMeta: Record<AccountStatus, { label: string; tone: 'ok' | 'warn' | 'bad' | 'neutral' }> = {
  expired: { label: 'OAuth expired', tone: 'bad' },
  limited: { label: 'Rate limited', tone: 'warn' },
  exhausted: { label: 'No quota left', tone: 'warn' },
  low: { label: 'Low quota', tone: 'warn' },
  partial: { label: 'Partial', tone: 'warn' },
  healthy: { label: 'Healthy', tone: 'ok' },
  connected: { label: 'Connected', tone: 'neutral' },
};

export const needsQuota = (account: Account) =>
  account.status === 'low' || account.status === 'exhausted' || account.status === 'partial' || account.status === 'limited';

// Each label has its own denominator: a missing or expired reading is not a zero.
function averageQuotas(accounts: readonly Account[]): ProviderGroup['averages'] {
  const totals = new Map<string, { sum: number; count: number }>();
  for (const { reading } of accounts) {
    if (reading.expired) continue;
    const byLabel = new Map([...reading.windows, ...reading.modelPools].map(bucket => [bucket.label, bucket.left]));
    for (const [label, left] of byLabel) {
      const tally = totals.get(label) ?? { sum: 0, count: 0 };
      tally.sum += left;
      tally.count++;
      totals.set(label, tally);
    }
  }
  return [...totals].sort(([a], [b]) => compareQuotaLabels(a, b))
    .map(([label, { sum, count }]) => ({ label, left: Math.round(sum / count) }));
}

export function groupProfiles<P extends ProfileLike>(profiles: readonly P[]): ProviderGroup<P>[] {
  const groups = new Map<string, Account<P>[]>();
  for (const profile of profiles) {
    const account = classify(profile) as Account<P>;
    groups.set(profile.provider, [...(groups.get(profile.provider) ?? []), account]);
  }
  const rank: Record<AccountStatus, number> = { expired: 0, limited: 1, exhausted: 2, low: 3, partial: 4, healthy: 5, connected: 6 };
  return [...groups].map(([provider, accounts]) => {
    accounts.sort((a, b) => rank[a.status] - rank[b.status] || a.identity.localeCompare(b.identity));
    return {
      provider, name: providerName(provider), accounts,
      expired: accounts.filter(account => account.status === 'expired').length,
      quotaWarnings: accounts.filter(needsQuota).length,
      averages: averageQuotas(accounts),
    };
  }).sort((a, b) => a.name.localeCompare(b.name));
}

export function accountMatches(account: Account, query: string) {
  const tokens = query.toLowerCase().trim().split(/\s+/).filter(Boolean);
  const haystack = [account.identity, account.profile.profile, account.profile.provider, providerName(account.profile.provider),
    statusMeta[account.status].label, account.oauth ? 'oauth' : 'api key', ...(account.profile.assigned_agents ?? [])].join(' ').toLowerCase();
  return tokens.every(token => haystack.includes(token));
}
