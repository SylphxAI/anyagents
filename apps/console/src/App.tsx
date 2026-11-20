import { useEffect, useMemo, useRef, useState } from 'react';
import { Activity, ArrowRight, Bot, ChartNoAxesCombined, Command, KeyRound, Layers, LogOut, Network, Plus, Search, Timer, WalletCards } from 'lucide-react';
import { api, errorMessage, useResource } from './api';
import { AgentAction } from './fleet/AgentAction';
import type { FleetAction } from './fleet/AgentAction';
import { Fleet } from './fleet/Fleet';
import { agentLookup } from './fleet/tree';
import type { Agent, Overview, Session } from './types';
import { Dialog, Dot, Empty, Mark, Problem, ThemeButton, useTheme } from './ui';
import { AutomationsView } from './views/AutomationsView';
import { ChannelsView } from './views/ChannelsView';
import { ProfilesView } from './views/ProfilesView';
import { GrantDialog, SecretsView } from './views/SecretsView';
import { UsageView } from './views/UsageView';

const tabs = [
  { id: 'fleet', label: 'Fleet', icon: Layers }, { id: 'profiles', label: 'Profiles & Quota', icon: WalletCards },
  { id: 'usage', label: 'Usage', icon: ChartNoAxesCombined }, { id: 'automations', label: 'Automations', icon: Timer },
  { id: 'channels', label: 'Channels & Groups', icon: Network }, { id: 'secrets', label: 'Secrets', icon: KeyRound },
] as const;
type Tab = typeof tabs[number]['id'];
function route() {
  const [section, raw] = window.location.hash.slice(1).split('/');
  const tab: Tab = tabs.find(tab => tab.id === section || (section === 'schedules' && tab.id === 'automations'))?.id ?? 'fleet';
  let id: string | undefined;
  try { id = raw ? decodeURIComponent(raw) : undefined; } catch { /* Ignore a malformed link. */ }
  return { tab, id };
}
const loadRoster = (signal: AbortSignal) => api<{ agents: Agent[] }>('/v1/agents?state=all', { signal });
const loadOverview = (signal: AbortSignal) => api<Overview>('/admin/api/overview', { signal });
const loadSession = (signal: AbortSignal) => api<Session>('/admin/auth/me', { signal });
const noAgents: Agent[] = [];
const fleetLink = (id?: string) => '#fleet' + (id ? '/' + encodeURIComponent(id) : '');

export function App() {
  const [location, setLocation] = useState(route);
  const theme = useTheme();
  const roster = useResource(loadRoster, 5000);
  const overview = useResource(loadOverview, 5000);
  const session = useResource(loadSession, 30000);
  const [palette, setPalette] = useState(false);
  const [action, setAction] = useState<FleetAction>();
  const [grant, setGrant] = useState<Agent>();
  const [grantVersion, setGrantVersion] = useState(0);
  const [revision, setRevision] = useState(0);
  const [signoutError, setSignoutError] = useState('');
  const [expired, setExpired] = useState(false);
  const [signingOut, setSigningOut] = useState(false);
  const agents = roster.data?.agents ?? noAgents;
  const resolveAgent = useMemo(() => agentLookup(agents), [agents]);
  const selected = resolveAgent(location.id);
  useEffect(() => {
    const onHash = () => setLocation(route());
    const onExpired = () => setExpired(true);
    function onKey(event: KeyboardEvent) {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k' && !document.querySelector('dialog[open]')) { event.preventDefault(); setPalette(true); }
    }
    window.addEventListener('hashchange', onHash); window.addEventListener('session-expired', onExpired); window.addEventListener('keydown', onKey);
    return () => { window.removeEventListener('hashchange', onHash); window.removeEventListener('session-expired', onExpired); window.removeEventListener('keydown', onKey); };
  }, []);
  const select = (id?: string) => { window.location.hash = fleetLink(id); };
  const updated = () => { roster.refresh(); overview.refresh(); setRevision(value => value + 1); };
  const grantsUpdated = () => { updated(); setGrantVersion(value => value + 1); };
  async function logout() {
    setSigningOut(true); setSignoutError('');
    try { await api('/admin/auth/logout', { method: 'POST' }); window.location.assign('/'); }
    catch (cause) { setSignoutError(errorMessage(cause)); setSigningOut(false); }
  }
  const unhealthy = Boolean(roster.error || overview.error);
  const who = session.data?.name || session.data?.email || (session.data?.mode === 'local_trusted' ? 'Local operator' : 'Operator');
  return <>
    <a className="skip-link" href="#console-content" onClick={event => { event.preventDefault(); document.querySelector<HTMLElement>('#console-content')?.focus(); }}>Skip to content</a>
    <header className="topbar"><div className="topbar-inner">
      <a href="#fleet" className="brand"><Mark /><strong>AnyAgents</strong></a>
      <nav aria-label="Control plane">{tabs.map(({ id, label, icon: Icon }) => <a key={id} href={'#' + id} aria-current={id === location.tab ? 'page' : undefined}><Icon size={15} />{label}</a>)}</nav>
      <div className="topbar-tools"><button className="command-trigger" aria-label="Command palette" onClick={() => setPalette(true)}><Search size={16} /><span>Search</span><kbd><Command size={11} />K</kbd></button>
        <ThemeButton {...theme} />{session.data?.mode === 'oidc' && <button className="icon-button" title="Sign out" aria-label="Sign out" disabled={signingOut} onClick={() => void logout()}><LogOut size={16} /></button>}
      </div>
    </div></header>
    <main className="workspace" id="console-content" tabIndex={-1}>
      <div className="context-bar"><span><Activity size={14} /> Coordinator <i /> {session.data?.serverless ? 'Serverless' : 'Control plane'}</span><span><Dot tone={unhealthy ? 'errored' : roster.updated ? 'working' : 'idle'} />{unhealthy ? 'Poll failed' : roster.updated ? 'Live · 5s' : 'Connecting'}<i /><span className="operator" title={session.data?.email}>{who}</span></span></div>
      {(expired || signoutError || session.error || roster.error || overview.error) && <Problem retry={!expired ? () => { updated(); session.refresh(); } : undefined}>{expired ? <>Your session ended. <a href="/">Sign in again</a>.</> : (signoutError || session.error || roster.error || overview.error)}</Problem>}
      {location.tab === 'fleet' && <Fleet agents={agents} overview={overview.data} selected={selected} select={select} open={setAction} grants={setGrant} refreshVersion={revision} />}
      {location.tab === 'profiles' && <ProfilesView />}
      {location.tab === 'usage' && <UsageView />}
      {location.tab === 'automations' && <AutomationsView agents={agents} overview={overview.data} />}
      {location.tab === 'channels' && <ChannelsView />}
      {location.tab === 'secrets' && <SecretsView agents={agents} openGrants={setGrant} grantVersion={grantVersion} />}
    </main>
    {palette && <Palette agents={agents} selected={selected} close={() => setPalette(false)} select={select} act={setAction} grants={setGrant} />}
    {action && <AgentAction action={action} close={() => setAction(undefined)} updated={updated} spawned={select} />}
    {grant && <GrantDialog agent={grant} close={() => setGrant(undefined)} updated={grantsUpdated} />}
  </>;
}

type CommandItem = { id: string; title: string; detail: string; icon: React.ElementType; run: () => void };
function fuzzy(query: string, target: string) {
  const text = target.toLowerCase();
  let rank = 0;
  for (const token of query.toLowerCase().trim().split(/\s+/)) {
    const match = text.indexOf(token);
    if (match >= 0) { rank += match; continue; }
    let cursor = 0;
    for (const char of token) { const position = text.indexOf(char, cursor); if (position < 0) return Infinity; cursor = position + 1; }
    rank += 100 + cursor;
  }
  return rank;
}
function Palette({ agents, selected, close, select, act, grants }: { agents: Agent[]; selected?: Agent; close: () => void; select: (id: string) => void; act: (action: FleetAction) => void; grants: (agent: Agent) => void }) {
  const [query, setQuery] = useState('');
  const [position, setPosition] = useState(0);
  const active = useRef<HTMLButtonElement>(null);
  const items = useMemo(() => {
    const commands: CommandItem[] = [...tabs.map(({ id, label, icon }) => ({ id, title: label, detail: 'Navigate', icon, run: () => { window.location.hash = '#' + id; } })),
      { id: 'spawn', title: 'Spawn agent', detail: 'Action', icon: Plus, run: () => act({ kind: 'spawn' }) },
      ...agents.map(agent => ({ id: agent.id, title: agent.name, detail: agent.harness || 'Agent', icon: Bot, run: () => select(agent.id) }))];
    if (selected && selected.state !== 'archived') {
      commands.push({ id: 'child', title: 'Spawn child', detail: selected.name, icon: Plus, run: () => act({ kind: 'spawn', parent: selected }) },
        { id: 'runtime', title: 'Switch runtime', detail: selected.name, icon: Activity, run: () => act({ kind: 'runtime', agent: selected }) },
        { id: 'grants', title: 'Manage secret grants', detail: selected.name, icon: KeyRound, run: () => grants(selected) });
    }
    return commands.map(item => ({ item, rank: fuzzy(query, item.title + ' ' + item.detail) })).filter(({ rank }) => Number.isFinite(rank)).sort((a, b) => a.rank - b.rank).map(({ item }) => item);
  }, [agents, query, selected, select, act, grants]);
  const index = Math.min(position, items.length - 1);
  useEffect(() => { active.current?.scrollIntoView({ block: 'nearest' }); }, [index]);
  function run(item: CommandItem) { close(); item.run(); }
  return <Dialog title="Command palette" close={close} className="palette"><div className="palette-body">
    <label className="tree-search"><Search size={18} /><input autoFocus placeholder="Find an agent or command…" aria-label="Find an agent or command" value={query} onChange={event => { setPosition(0); setQuery(event.target.value); }} onKeyDown={event => {
      if (event.key === 'ArrowDown' || event.key === 'ArrowUp') { event.preventDefault(); setPosition(items.length ? (index + items.length + (event.key === 'ArrowDown' ? 1 : -1)) % items.length : 0); }
      if (event.key === 'Enter' && items[index]) { event.preventDefault(); run(items[index]); }
    }} /></label>
    <div className="command-results">{items.length ? items.map((item, itemIndex) => <button ref={index === itemIndex ? active : undefined} className="command-result" data-active={index === itemIndex} key={item.id} onClick={() => run(item)}><item.icon size={16} /><strong>{item.title}</strong><span>{item.detail}</span><ArrowRight size={15} /></button>) : <Empty>No matching commands.</Empty>}</div>
    <div className="palette-hint"><span><kbd>↑</kbd> <kbd>↓</kbd> Navigate</span><span><kbd>↵</kbd> Open</span><span><kbd>Esc</kbd> Close</span></div>
  </div></Dialog>;
}
