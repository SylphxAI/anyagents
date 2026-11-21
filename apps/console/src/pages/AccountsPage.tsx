import { For, Show, createMemo, createSignal } from 'solid-js';
import { ChevronRight, Plus, RefreshCw, SearchX } from 'lucide-solid';
import { ago, plural } from '../format';
import { accountMatches, LOW_QUOTA, needsQuota, statusMeta } from '../lib/providers';
import type { Account, ProviderGroup } from '../lib/providers';
import { windowLabel } from '../lib/quota';
import type { QuotaBucket } from '../lib/quota';
import { agentHref } from '../route';
import { useConsole } from '../state';
import type { Profile } from '../types';
import { Dot, Empty, Highlight, Meter, PageHeader, Pill, Problem, SearchField, Segmented, Skeleton } from '../ui/primitives';

type Filter = 'all' | 'expired' | 'quota';

export function AccountsPage() {
  const { groups, accounts, profiles, refreshProfiles, connect } = useConsole();
  const [filter, setFilter] = createSignal<Filter>('all');
  const [query, setQuery] = createSignal('');
  const [collapsed, setCollapsed] = createSignal<ReadonlySet<string>>(new Set());
  const expired = () => accounts().filter(account => account.status === 'expired').length;
  const warnings = () => accounts().filter(needsQuota).length;
  const healthy = () => accounts().length - expired() - warnings();
  const checkedAt = () => {
    const age = profiles.data()?.find(profile => profile.usage_age_ms != null)?.usage_age_ms;
    return age != null && profiles.updated() ? profiles.updated()! - age : undefined;
  };
  const visible = createMemo(() => groups().map(group => ({
    group,
    shown: group.accounts.filter(account => (filter() === 'all' || (filter() === 'expired' ? account.status === 'expired' : needsQuota(account)))
      && accountMatches(account, query())),
  })).filter(entry => entry.shown.length > 0));
  const filtering = () => filter() !== 'all' || Boolean(query().trim());
  const toggle = (provider: string) => setCollapsed(previous => { const next = new Set(previous); if (next.has(provider)) next.delete(provider); else next.add(provider); return next; });

  return <div class="page">
    <PageHeader title="Accounts & Quotas" description="Provider sign-ins and API keys your agents run on, with the quota each one has left."
      actions={<>
        <button type="button" class="btn" onClick={() => refreshProfiles(true)} disabled={profiles.loading()}><RefreshCw size={14} />Refresh quotas</button>
        <button type="button" class="btn primary" onClick={() => connect()}><Plus size={14} />Connect account</button>
      </>} />
    <div class="summary-line" aria-label="Account summary">
      <span><span class="num strong">{accounts().length}</span> accounts</span>
      <span><Dot tone="ok" /><span class="num strong">{healthy()}</span> healthy</span>
      <span class={expired() ? 'tone-bad' : undefined}><Dot tone={expired() ? 'bad' : 'muted'} /><span class="num strong">{expired()}</span> OAuth expired</span>
      <span class={warnings() ? 'tone-warn' : undefined}><Dot tone={warnings() ? 'warn' : 'muted'} /><span class="num strong">{warnings()}</span> quota warnings</span>
      <span class="summary-spacer" />
      <Show when={checkedAt()}><span class="subtle">Quotas checked <span class="num">{ago(checkedAt())}</span></span></Show>
    </div>
    <div class="toolbar">
      <Segmented label="Account filter" value={filter()} onChange={setFilter} options={[
        { value: 'all', label: 'All', count: accounts().length },
        { value: 'expired', label: 'OAuth expired', count: expired(), tone: 'bad' },
        { value: 'quota', label: 'Quota warnings', count: warnings(), tone: 'warn' },
      ]} />
      <SearchField value={query()} onChange={setQuery} placeholder="Search accounts, providers, agents…" label="Search accounts" />
    </div>
    <Show when={profiles.error()}><Problem retry={() => refreshProfiles()}>{profiles.error()}</Problem></Show>
    <Show when={profiles.loading()}><Skeleton rows={8} /></Show>
    <Show when={profiles.data() && accounts().length === 0}><Empty title="No accounts connected" icon={<Plus size={18} />}
      action={<button type="button" class="btn primary" onClick={() => connect()}><Plus size={14} />Connect account</button>}>
      Sign in with a provider subscription or add an API key so agents can run.</Empty></Show>
    <Show when={profiles.data() && accounts().length > 0 && visible().length === 0}><Empty compact icon={<SearchX size={18} />} title="No matching accounts"
      action={<button type="button" class="btn sm" onClick={() => { setFilter('all'); setQuery(''); }}>Clear filters</button>}>
      {filter() === 'expired' ? 'Every OAuth sign-in is current.' : filter() === 'quota' ? 'No accounts have quota warnings.' : <>Nothing matches “{query()}”.</>}</Empty></Show>
    <div class="provider-list">
      <For each={visible()}>{entry => <ProviderSection group={entry.group} shown={entry.shown} query={query()}
        open={filtering() || !collapsed().has(entry.group.provider)} toggle={() => toggle(entry.group.provider)} />}</For>
    </div>
  </div>;
}

function ProviderSection(props: { group: ProviderGroup<Profile>; shown: Account<Profile>[]; query: string; open: boolean; toggle: () => void }) {
  const id = 'provider-' + props.group.provider;
  return <section class="provider">
    <h2 class="provider-head">
      <button type="button" aria-expanded={props.open} aria-controls={id} onClick={() => props.toggle()}>
        <ChevronRight size={14} class={'chevron-icon' + (props.open ? ' open' : '')} aria-hidden="true" />
        <span class="provider-name">{props.group.name}</span>
        <span class="subtle num">{plural(props.group.accounts.length, 'account')}</span>
        <Show when={props.group.expired > 0}><Pill tone="bad">{props.group.expired} expired</Pill></Show>
        <Show when={props.group.quotaWarnings > 0}><Pill tone="warn">{plural(props.group.quotaWarnings, 'quota warning')}</Pill></Show>
        <Show when={props.group.averages.length}><span class="provider-averages subtle">
          <span>Avg. left</span>
          <For each={props.group.averages}>{bucket => <span class="provider-average">
            {windowLabel(bucket.label)} <span class="num strong-muted">{bucket.left}%</span>
          </span>}</For>
        </span></Show>
      </button>
    </h2>
    <Show when={props.open}><div class="table-wrap" id={id}><table class="table accounts-table">
      <thead><tr><th class="col-account">Account</th><th class="col-status">Status</th><th class="col-quota">Remaining quota</th><th class="col-usage">Used by</th><th class="right col-action"><span class="sr-only">Actions</span></th></tr></thead>
      <tbody><For each={props.shown}>{account => <AccountRow account={account} query={props.query} />}</For></tbody>
    </table></div></Show>
  </section>;
}

function AccountRow(props: { account: Account<Profile>; query: string }) {
  const { connect } = useConsole();
  const account = () => props.account;
  const profile = () => account().profile;
  const reading = () => account().reading;
  const expired = () => account().status === 'expired';
  const agents = () => profile().assigned_agents ?? [];
  return <tr class={expired() ? 'row-bad' : undefined}>
    <td class="col-account">
      <div class="account-id"><span class="strong truncate" title={account().identity}><Highlight text={account().identity} query={props.query} /></span></div>
      <div class="account-sub">
        <span class={'auth-badge' + (account().oauth ? ' oauth' : '')}>{account().oauth ? 'OAuth' : 'API key'}</span>
        <Show when={reading().tier}><span class="subtle">{reading().tier}</span></Show>
        <Show when={account().identity !== profile().profile}><span class="subtle mono truncate" title="Profile slot">{profile().profile}</span></Show>
      </div>
    </td>
    <td class="col-status"><Pill tone={statusMeta[account().status].tone} dot>{statusMeta[account().status].label}</Pill>
      <Show when={account().status === 'limited' && reading().limitedReset}><div class="subtle small num">resets in {reading().limitedReset}</div></Show></td>
    <td class="col-quota">
      <Show when={reading().windows.length || reading().modelPools.length} fallback={reading().balance ? <span class="mono">{reading().balance}</span>
        : reading().note ? <span class="subtle">{reading().note}</span>
        : <span class="subtle">{expired() ? 'Sign in again to read quota' : reading().limited ? 'No quota reading reported' : account().oauth ? 'No reading yet' : 'Not reported for API keys'}</span>}>
        <div class={'quota-breakdown' + (expired() ? ' stale' : '')} title={expired() ? 'Last known quota before the sign-in expired' : undefined}>
          <Show when={reading().windows.length}><div class="quotas" role="group" aria-label="Account limits">
            <For each={reading().windows}>{bucket => <QuotaMeter bucket={bucket} />}</For>
          </div></Show>
          <Show when={reading().modelPools.length}><div class="quotas model-pools" role="group" aria-label="Model pools">
            <span class="subtle small" aria-hidden="true">Models</span>
            <For each={reading().modelPools}>{bucket => <QuotaMeter bucket={bucket} />}</For>
          </div></Show>
        </div>
      </Show>
    </td>
    <td class="col-usage"><Show when={agents().length} fallback={<span class="subtle">Unassigned</span>}><span class="usage" title={agents().join(', ')}>
      <a href={agentHref(agents()[0])} class="truncate">{agents()[0]}</a><Show when={agents().length > 1}><span class="subtle num">&nbsp;+{agents().length - 1}</span></Show>
      <Show when={profile().subagent_count}><span class="subtle num">&nbsp;· {plural(profile().subagent_count ?? 0, 'sub-agent')}</span></Show>
    </span></Show></td>
    <td class="right col-action"><Show when={account().oauth}
      fallback={<button type="button" class="btn sm ghost" onClick={() => connect({ provider: profile().provider, tab: 'key', hint: profile().profile })}>Replace key</button>}>
      <button type="button" class={'btn sm' + (expired() ? ' danger-soft' : ' ghost')} onClick={() => connect({ provider: profile().provider, hint: profile().account ?? undefined })}>
        <RefreshCw size={13} />{expired() ? 'Sign in again' : 'Refresh'}</button>
    </Show></td>
  </tr>;
}

function QuotaMeter(props: { bucket: QuotaBucket }) {
  const label = () => windowLabel(props.bucket.label);
  const left = () => props.bucket.left;
  return <span class="quota" title={props.bucket.reset ? label() + ' quota resets in ' + props.bucket.reset : undefined}>
    <span class="quota-label">{label()}</span>
    <span class={'quota-value num' + (left() === 0 ? ' tone-bad' : left() < LOW_QUOTA ? ' tone-warn' : '')}>{left()}%</span>
    <Meter value={left()} label={label() + ' quota remaining'} />
  </span>;
}
