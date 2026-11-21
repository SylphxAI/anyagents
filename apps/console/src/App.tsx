import { For, Match, Show, Switch, createMemo, createSignal, onCleanup, onMount } from 'solid-js';
import { Dynamic } from 'solid-js/web';
import { Activity, LogOut, Menu, Search, X } from 'lucide-solid';
import { api, createPoll, errorMessage, optional } from './api';
import { AgentDialogs } from './agents/AgentDialogs';
import { ConnectDialog } from './accounts/ConnectDialog';
import { groupProfiles } from './lib/providers';
import { agentStatus } from './lib/status';
import { buildForest } from './lib/tree';
import { NAV } from './nav';
import { pageHref, parseRoute } from './route';
import { ConsoleContext } from './state';
import type { AgentAction, ConnectRequest, ConsoleState, Fleet } from './state';
import { runtimeIssues } from './lib/issues';
import type { Agent, Overview, Page, Profile, Route, Session, VersionsReport } from './types';
import { Palette } from './ui/Palette';
import { Dot, Kbd, Mark, Problem, ThemeToggle } from './ui/primitives';
import { OverviewPage } from './pages/OverviewPage';
import { AgentsPage } from './pages/AgentsPage';
import { NodesPage } from './pages/NodesPage';
import { AccountsPage } from './pages/AccountsPage';
import { KnowledgePage } from './pages/KnowledgePage';
import { VersionsPage } from './pages/VersionsPage';
import { SystemIssuesPage } from './pages/SystemIssuesPage';
import { SettingsPage } from './pages/SettingsPage';

const noAgents: Agent[] = [];

export function App() {
  const [route, setRoute] = createSignal(parseRoute());
  const roster = createPoll(signal => api<{ agents: Agent[] }>('/v1/agents?state=all', { signal }), { interval: 5000 });
  const overview = createPoll(signal => api<Overview>('/admin/api/overview', { signal }), { interval: 5000 });
  const session = createPoll(signal => api<Session>('/admin/auth/me', { signal }), { interval: 30000 });
  const versions = createPoll(signal => optional<VersionsReport>('/admin/api/versions', { signal }), { interval: 30000 });
  const issues = createPoll(async signal => {
    const payload = await optional<unknown>('/admin/api/runtime-issues', { signal });
    return payload === null ? null : runtimeIssues(payload);
  }, { interval: 30000 });
  let wantFresh = false;
  const profiles = createPoll(signal => {
    const fresh = wantFresh; wantFresh = false;
    return api<Profile[]>('/v1/profiles' + (fresh ? '?fresh=1' : ''), { signal });
  }, { interval: 60000 });
  const [palette, setPalette] = createSignal(false);
  const [drawer, setDrawer] = createSignal(false);
  const [action, setAction] = createSignal<AgentAction>();
  const [connect, setConnect] = createSignal<ConnectRequest>();
  const [revision, setRevision] = createSignal(0);
  const [expired, setExpired] = createSignal(false);
  const [signoutError, setSignoutError] = createSignal('');

  onMount(() => {
    const onHash = () => {
      setRoute(parseRoute());
      setDrawer(false);
      setConnect(undefined);
      setAction(undefined);
      setPalette(false);
    };
    const onExpired = () => setExpired(true);
    const onKey = (event: KeyboardEvent) => {
      if (document.querySelector('dialog[open]')) return;
      const target = event.target as HTMLElement | null;
      const typing = target && (target.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(target.tagName));
      if (((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') || (event.key === '/' && !typing)) {
        event.preventDefault(); setPalette(true);
      }
    };
    window.addEventListener('hashchange', onHash); window.addEventListener('session-expired', onExpired); window.addEventListener('keydown', onKey);
    onCleanup(() => { window.removeEventListener('hashchange', onHash); window.removeEventListener('session-expired', onExpired); window.removeEventListener('keydown', onKey); });
  });

  const fleet = createMemo<Fleet>(() => {
    const agents = roster.data()?.agents ?? noAgents;
    const live = agents.filter(agent => agent.state !== 'archived');
    return {
      agents, live, forest: buildForest(agents),
      working: live.filter(agent => agentStatus(agent) === 'working'),
      errored: live.filter(agent => agentStatus(agent) === 'errored'),
      archived: agents.length - live.length,
    };
  });
  const groups = createMemo(() => groupProfiles(profiles.data() ?? []));
  const accounts = createMemo(() => groups().flatMap(group => group.accounts));

  const refreshAll = () => { roster.refresh(); overview.refresh(); versions.refresh(); issues.refresh(); setRevision(value => value + 1); };
  const state: ConsoleState = {
    session, roster, overview, profiles, versions, issues, fleet, groups, accounts, revision, refreshAll,
    refreshProfiles: (fresh = false) => { wantFresh = fresh; profiles.refresh(); },
    act: next => setAction(() => next), connect: request => setConnect(request ?? {}), openPalette: () => setPalette(true),
  };

  const badges = createMemo(() => {
    const expiredAccounts = accounts().filter(account => account.status === 'expired').length;
    const outdated = (versions.data()?.agents ?? []).filter(agent => agent.upgrade_status === 'outdated_idle').length;
    const result: Partial<Record<Page, { count: number; tone: 'bad' | 'warn' | 'muted'; label: string }[]>> = {
      agents: [
        ...(fleet().working.length ? [{ count: fleet().working.length, tone: 'muted' as const, label: ' working' }] : []),
        ...(fleet().errored.length ? [{ count: fleet().errored.length, tone: 'bad' as const, label: ' errored' }] : []),
      ],
      accounts: expiredAccounts ? [{ count: expiredAccounts, tone: 'bad', label: ' expired' }] : [],
      versions: outdated ? [{ count: outdated, tone: 'warn', label: ' outdated' }] : [],
    };
    return result;
  });
  const unhealthy = () => Boolean(roster.error() || overview.error());
  const who = () => session.data()?.name || session.data()?.email || (session.data()?.mode === 'local_trusted' ? 'Local operator' : 'Operator');
  const current = () => NAV.find(item => item.page === route().page)!;

  async function logout() {
    setSignoutError('');
    try { await api('/admin/auth/logout', { method: 'POST' }); window.location.assign('/'); }
    catch (cause) { setSignoutError(errorMessage(cause)); }
  }

  return <ConsoleContext.Provider value={state}>
    <a class="skip-link" href="#main" onClick={event => { event.preventDefault(); document.getElementById('main')?.focus(); }}>Skip to content</a>
    <div class={'app' + (drawer() ? ' drawer-open' : '')}>
      <aside class="sidebar" aria-label="Primary">
        <div class="sidebar-brand">
          <a href="#overview" class="brand"><Mark /><span>AnyAgents</span></a>
          <button type="button" class="btn icon ghost sm drawer-close" aria-label="Close navigation" onClick={() => setDrawer(false)}><X size={15} /></button>
        </div>
        <nav class="nav" aria-label="Console">
          <For each={NAV}>{item => <a href={pageHref(item.page)} class="nav-item" aria-current={route().page === item.page ? 'page' : undefined}>
            <Dynamic component={item.icon} size={15} aria-hidden="true" /><span class="nav-label">{item.label}</span>
            <For each={badges()[item.page] ?? []}>{badge => <span class={'nav-badge num ' + badge.tone} aria-label={badge.count + badge.label}>{badge.count}</span>}</For>
          </a>}</For>
        </nav>
        <div class="sidebar-foot">
          <div class="coordinator-status" title={unhealthy() ? 'The last poll failed' : 'Polling every 5 seconds'}>
            <Dot tone={unhealthy() ? 'bad' : roster.updated() ? 'ok' : 'muted'} pulse={!unhealthy() && Boolean(roster.updated())} />
            <span>{unhealthy() ? 'Connection issue' : roster.updated() ? 'Live' : 'Connecting…'}</span>
            <span class="subtle">{session.data()?.serverless ? 'Serverless' : 'Coordinator'}</span>
          </div>
          <div class="operator">
            <span class="avatar" aria-hidden="true">{who().slice(0, 1).toUpperCase()}</span>
            <span class="operator-name" title={session.data()?.email}>{who()}</span>
            <Show when={session.data()?.mode === 'oidc'}><button type="button" class="btn icon ghost sm" title="Sign out" aria-label="Sign out" onClick={() => void logout()}><LogOut size={14} /></button></Show>
          </div>
        </div>
      </aside>
      <div class="scrim" aria-hidden="true" onClick={() => setDrawer(false)} />
      <div class="main">
        <header class="topbar">
          <button type="button" class="btn icon ghost menu-button" aria-label="Open navigation" onClick={() => setDrawer(true)}><Menu size={16} /></button>
          <div class="crumb"><Dynamic component={current().icon} size={14} aria-hidden="true" /><span>{current().label}</span></div>
          <button type="button" class="search-trigger" onClick={() => setPalette(true)} aria-label="Search agents, accounts, and pages">
            <Search size={14} aria-hidden="true" /><span>Search agents, accounts, pages…</span><Kbd>⌘K</Kbd>
          </button>
          <div class="topbar-tools">
            <span class={'live-chip' + (unhealthy() ? ' bad' : '')}><Activity size={13} aria-hidden="true" />{unhealthy() ? 'Poll failed' : 'Live'}</span>
            <ThemeToggle />
          </div>
        </header>
        <main id="main" class="content" tabindex="-1">
          <Show when={expired() || signoutError() || session.error() || roster.error()}>
            <div class="banner"><Problem retry={expired() ? undefined : () => { refreshAll(); session.refresh(); }}>
              {expired() ? <>Your session ended. <a href="/">Sign in again</a>.</> : signoutError() || roster.error() || session.error()}
            </Problem></div>
          </Show>
          <Switch>
            <Match when={route().page === 'overview'}><OverviewPage /></Match>
            <Match when={route().page === 'agents'}><AgentsPage selectedId={(route() as { id?: string }).id} /></Match>
            <Match when={route().page === 'nodes'}><NodesPage selectedId={(route() as { id?: string }).id} /></Match>
            <Match when={route().page === 'accounts'}><AccountsPage /></Match>
            <Match when={route().page === 'knowledge'}><KnowledgePage /></Match>
            <Match when={route().page === 'versions'}><VersionsPage /></Match>
            <Match when={route().page === 'issues'}><SystemIssuesPage /></Match>
            <Match when={route().page === 'settings'}><SettingsPage tab={(route() as Extract<Route, { page: 'settings' }>).tab} /></Match>
          </Switch>
        </main>
      </div>
    </div>
    <Show when={palette()}><Palette close={() => setPalette(false)} /></Show>
    <Show when={action()} keyed>{value => <AgentDialogs action={value} close={() => setAction(undefined)} />}</Show>
    <Show when={connect()} keyed>{value => <ConnectDialog request={value} close={() => setConnect(undefined)} />}</Show>
  </ConsoleContext.Provider>;
}
