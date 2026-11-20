import { For, Show, createMemo, createSignal } from 'solid-js';
import type { JSX } from 'solid-js';
import { ArrowRight, Bot, CircleCheck, Gauge, KeyRound, PackageCheck, Server, Timer } from 'lucide-solid';
import { duration, plural } from '../format';
import { needsQuota, providerName } from '../lib/providers';
import { shortModel } from '../lib/status';
import { ancestorsOf } from '../lib/tree';
import { agentHref } from '../route';
import { useConsole } from '../state';
import { Dot, Empty, PageHeader, Pill, Section, Skeleton, StatStrip } from '../ui/primitives';
import type { Tone } from '../ui/primitives';

interface ActionItem { key: string; tone: Tone; icon: () => JSX.Element; title: () => JSX.Element; detail: () => JSX.Element; action: () => JSX.Element }

export function OverviewPage() {
  const { fleet, accounts, versions, issues, profiles, roster, connect } = useConsole();
  const [showAll, setShowAll] = createSignal(false);
  const expired = createMemo(() => accounts().filter(account => account.status === 'expired'));
  const low = createMemo(() => accounts().filter(needsQuota));
  const outdated = createMemo(() => (versions.data()?.agents ?? []).filter(agent => agent.upgrade_status === 'outdated_idle'));
  const healthy = () => accounts().length - expired().length - low().length;
  const attention = () => fleet().errored.length + expired().length;
  const loading = () => !roster.data();

  const items = createMemo<ActionItem[]>(() => [
    ...expired().map(account => ({
      key: 'account:' + account.key, tone: 'bad' as const, icon: () => <KeyRound size={15} />,
      title: () => <><span class="strong">{account.identity}</span><span class="subtle"> · {providerName(account.profile.provider)}</span></>,
      detail: () => 'OAuth sign-in expired' + (account.profile.assigned_agents?.length ? ' · ' + plural(account.profile.assigned_agents.length, 'agent') + ' bound to this account' : ''),
      action: () => account.oauth
        ? <button type="button" class="btn sm danger-soft" onClick={() => connect({ provider: account.profile.provider, hint: account.profile.account })}>Sign in again</button>
        : <a class="btn sm" href="#accounts">View</a>,
    })),
    ...fleet().errored.map(agent => ({
      key: 'agent:' + agent.id, tone: 'bad' as const, icon: () => <Bot size={15} />,
      title: () => <span class="strong">{agent.name}</span>,
      detail: () => <span class="truncate">{agent.last_error || agent.turn_errors?.last_error || 'The last turn faulted.'}</span>,
      action: () => <a class="btn sm" href={agentHref(agent.name)}>Inspect</a>,
    })),
    ...(issues.data() ?? []).slice(0, 10).map((issue, index) => {
      const agent = issue.agent_name ?? issue.agent;
      const severe = ['critical', 'error', 'bad', 'high'].includes((issue.severity ?? '').toLowerCase());
      return {
        key: 'issue:' + (issue.id ?? index), tone: (severe ? 'bad' : 'warn') as Tone, icon: () => <Server size={15} />,
        title: () => <span class="strong">{issue.title ?? issue.kind ?? issue.message ?? 'Runtime issue'}</span>,
        detail: () => <span class="truncate">{[agent, issue.detail ?? (issue.title ? issue.message : undefined)].filter(Boolean).join(' · ') || 'Reported by the coordinator runtime.'}</span>,
        action: () => agent ? <a class="btn sm" href={agentHref(agent)}>Inspect</a> : <span />,
      };
    }),
    ...(outdated().length ? [{
      key: 'outdated', tone: 'warn' as const, icon: () => <PackageCheck size={15} />,
      title: () => <span class="strong">{plural(outdated().length, 'agent')} on an outdated sandbox image</span>,
      detail: () => 'Drain at the turn boundary to move them onto the desired image.',
      action: () => <a class="btn sm" href="#versions">Review updates</a>,
    }] : []),
    ...(low().length ? [{
      key: 'low', tone: 'warn' as const, icon: () => <Gauge size={15} />,
      title: () => <span class="strong">{plural(low().length, 'account')} low on quota</span>,
      detail: () => low().slice(0, 3).map(account => account.identity).join(', ') + (low().length > 3 ? ' and ' + (low().length - 3) + ' more' : ''),
      action: () => <a class="btn sm" href="#accounts">View accounts</a>,
    }] : []),
  ]);
  const visible = () => showAll() ? items() : items().slice(0, 7);
  const working = createMemo(() => [...fleet().working].sort((a, b) => (b.open_turn_age_ms ?? 0) - (a.open_turn_age_ms ?? 0)));
  const chain = (id: string) => ancestorsOf(fleet().forest, id).map(ancestor => fleet().forest.nodes.get(ancestor)!.agent.name).reverse();

  return <div class="page">
    <PageHeader title="Overview" description="Fleet health, what needs you, and who is working right now." />
    <StatStrip label="Fleet health" items={[
      { label: 'Active agents', value: loading() ? '—' : fleet().live.length, href: '#agents',
        detail: loading() ? 'Loading…' : plural(fleet().forest.roots.length, 'top-level director') + ' · ' + fleet().archived + ' archived' },
      { label: 'Working now', value: loading() ? '—' : fleet().working.length, tone: fleet().working.length ? 'ok' : undefined, href: '#agents',
        detail: fleet().live.length ? Math.round(fleet().working.length / fleet().live.length * 100) + '% of the fleet' : 'No active agents' },
      { label: 'Needs attention', value: loading() ? '—' : attention(), tone: attention() ? 'bad' : undefined,
        detail: attention() ? [fleet().errored.length && plural(fleet().errored.length, 'errored agent'), expired().length && plural(expired().length, 'expired account')].filter(Boolean).join(' · ') : 'Nothing is blocked' },
      { label: 'Accounts healthy', value: profiles.data() ? <>{healthy()}<span class="stat-of"> / {accounts().length}</span></> : '—', href: '#accounts',
        detail: expired().length ? <span class="tone-bad">{expired().length} OAuth expired</span> : low().length ? <span class="tone-warn">{low().length} low on quota</span> : profiles.data() ? 'All signed in' : 'Loading…' },
    ]} />

    <Section title="Action required" meta={items().length ? <Pill tone={items().some(item => item.tone === 'bad') ? 'bad' : 'warn'}>{items().length}</Pill> : undefined}>
      <Show when={!loading()} fallback={<Skeleton rows={3} />}>
        <Show when={items().length} fallback={<div class="all-clear"><CircleCheck size={16} aria-hidden="true" /><div><div class="strong">All systems operational</div>
          <div class="subtle">No expired accounts, errored agents, runtime issues, or outdated sandboxes.</div></div></div>}>
          <ul class="action-list">
            <For each={visible()}>{item => <li class={'action-row ' + item.tone}>
              <span class={'action-icon ' + item.tone} aria-hidden="true">{item.icon()}</span>
              <div class="action-text"><div class="action-title">{item.title()}</div><div class="action-detail">{item.detail()}</div></div>
              <div class="action-cta">{item.action()}</div>
            </li>}</For>
            <Show when={items().length > 7}><li class="action-more"><button type="button" class="btn ghost sm" onClick={() => setShowAll(value => !value)}>
              {showAll() ? 'Show fewer' : 'Show ' + (items().length - 7) + ' more'}</button></li></Show>
          </ul>
        </Show>
      </Show>
    </Section>

    <Section title="Working now" meta={<span class="num subtle">{fleet().working.length}</span>}
      actions={<a class="btn ghost sm" href="#agents">All agents <ArrowRight size={13} /></a>}>
      <Show when={!loading()} fallback={<Skeleton rows={5} />}>
        <Show when={working().length} fallback={<Empty compact icon={<Timer size={18} />} title="No agents are mid-turn">Agents appear here while they are working on a turn.</Empty>}>
          <div class="table-wrap"><table class="table">
            <thead><tr><th>Agent</th><th>Context</th><th>Harness · Model</th><th class="right">Turn age</th><th class="right"><span class="sr-only">Actions</span></th></tr></thead>
            <tbody><For each={working()}>{agent => {
              const path = chain(agent.id);
              return <tr class="clickable" onClick={() => { window.location.hash = agentHref(agent.name); }}>
                <td><span class="cell-agent"><Dot tone="ok" pulse /><a href={agentHref(agent.name)} onClick={event => event.stopPropagation()} class="strong">{agent.name}</a>
                  <Show when={agent.role}><span class="tag">{agent.role}</span></Show></span></td>
                <td class="subtle">{path.length ? <span class="chain">{path.length > 2 ? [path[0], '…', path[path.length - 1]].join(' › ') : path.join(' › ')}</span> : 'Top level'}</td>
                <td><span class="mono subtle">{agent.harness}{agent.model ? ' · ' + shortModel(agent.model) : ''}</span></td>
                <td class="right num">{agent.open_turn_age_ms != null ? duration(agent.open_turn_age_ms) : '—'}</td>
                <td class="right"><a class="btn sm" href={agentHref(agent.name)} onClick={event => event.stopPropagation()}>Inspect</a></td>
              </tr>;
            }}</For></tbody></table></div>
        </Show>
      </Show>
    </Section>
  </div>;
}
