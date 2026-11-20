import { For, Show, createEffect, createMemo, createSignal, on, onCleanup, onMount } from 'solid-js';
import type { JSX } from 'solid-js';
import { Dynamic } from 'solid-js/web';
import { ArrowRight, Bot, CircleAlert, CornerDownLeft, KeyRound, Moon, Plus, RefreshCw, Search, Sun } from 'lucide-solid';
import { NAV } from '../nav';
import { providerName, statusMeta } from '../lib/providers';
import { rank } from '../lib/search';
import { agentStatus, shortModel, statusLabel, statusTone } from '../lib/status';
import { agentHref, pageHref } from '../route';
import { useConsole } from '../state';
import { Dot, Highlight, Kbd, themeState } from './primitives';

type Group = 'Needs attention' | 'Pages' | 'Actions' | 'Agents' | 'Accounts';
interface Item { id: string; group: Group; title: string; detail?: string; keywords: string; icon: () => JSX.Element; hint?: string; run: () => void }
const ORDER: Group[] = ['Needs attention', 'Pages', 'Actions', 'Agents', 'Accounts'];

export function Palette(props: { close: () => void }) {
  const { fleet, accounts, connect, act, refreshProfiles } = useConsole();
  const [query, setQuery] = createSignal('');
  const [position, setPosition] = createSignal(0);
  let dialog!: HTMLDialogElement;
  let list!: HTMLDivElement;
  let input!: HTMLInputElement;
  const previouslyFocused = document.activeElement;
  onMount(() => { dialog.showModal(); input.focus(); });
  onCleanup(() => { dialog.close(); if (previouslyFocused instanceof HTMLElement) previouslyFocused.focus(); });

  const all = createMemo(() => {
    const go = (hash: string) => () => { window.location.hash = hash; };
    const items: Item[] = [
      ...NAV.map(({ page, label, icon }) => ({ id: 'page:' + page, group: 'Pages' as const, title: label, keywords: label + ' page go',
        icon: () => <Dynamic component={icon} size={15} />, hint: 'Page', run: go(pageHref(page)) })),
      ...(['channels', 'secrets', 'automations', 'usage'] as const).map(tab => ({ id: 'settings:' + tab, group: 'Pages' as const,
        title: 'Settings › ' + tab[0].toUpperCase() + tab.slice(1), keywords: 'settings ' + tab, icon: () => <ArrowRight size={15} />, hint: 'Page',
        run: go('#settings/' + tab) })),
      { id: 'act:connect', group: 'Actions', title: 'Connect account', detail: 'Sign in with a provider or add an API key', keywords: '+ connect account oauth sign in add api key',
        icon: () => <Plus size={15} />, hint: 'Action', run: () => connect() },
      { id: 'act:theme', group: 'Actions', title: 'Switch to ' + (themeState.theme() === 'dark' ? 'light' : 'dark') + ' theme', keywords: 'switch theme dark light mode appearance',
        icon: () => themeState.theme() === 'dark' ? <Sun size={15} /> : <Moon size={15} />, hint: 'Action', run: themeState.toggle },
      { id: 'act:spawn', group: 'Actions', title: 'Spawn agent', keywords: 'spawn create new agent', icon: () => <Bot size={15} />, hint: 'Action', run: () => act({ kind: 'spawn' }) },
      { id: 'act:quota', group: 'Actions', title: 'Refresh live quotas', keywords: 'refresh quota usage live accounts', icon: () => <RefreshCw size={15} />, hint: 'Action',
        run: () => { refreshProfiles(true); window.location.hash = '#accounts'; } },
    ];
    for (const agent of fleet().live) {
      const status = agentStatus(agent);
      items.push({ id: 'agent:' + agent.id, group: status === 'errored' ? 'Needs attention' : 'Agents', title: agent.name,
        detail: [agent.role, agent.harness, shortModel(agent.model), statusLabel[status]].filter(Boolean).join(' · '),
        keywords: [agent.name, agent.role, agent.harness, agent.model, agent.runtime, statusLabel[status], 'agent'].filter(Boolean).join(' '),
        icon: () => <Dot tone={statusTone[status]} pulse={status === 'working'} />, hint: 'Agent', run: go(agentHref(agent.name)) });
    }
    for (const account of accounts()) {
      const meta = statusMeta[account.status];
      items.push({ id: 'account:' + account.key, group: account.status === 'expired' ? 'Needs attention' : 'Accounts', title: account.identity,
        detail: providerName(account.profile.provider) + ' · ' + (account.oauth ? 'OAuth' : 'API key') + ' · ' + meta.label,
        keywords: [account.identity, account.profile.profile, account.profile.provider, providerName(account.profile.provider), meta.label, account.oauth ? 'oauth' : 'api key', 'account'].join(' '),
        icon: () => account.status === 'expired' ? <CircleAlert size={15} class="tone-bad" /> : <KeyRound size={15} />, hint: 'Account',
        run: account.status === 'expired' && account.oauth
          ? () => connect({ provider: account.profile.provider, hint: account.profile.account })
          : go('#accounts') });
    }
    return items;
  });

  const results = createMemo(() => {
    const trimmed = query().trim();
    if (!trimmed) {
      const attention = all().filter(item => item.group === 'Needs attention').slice(0, 6);
      const working = all().filter(item => item.group === 'Agents' && item.detail?.endsWith('Working')).slice(0, 6);
      return [...attention, ...all().filter(item => item.id.startsWith('page:')), ...all().filter(item => item.group === 'Actions'), ...working];
    }
    const scored = all().map(item => ({ item, score: Math.min(rank(trimmed, item.title), rank(trimmed, item.keywords) + 2) }))
      .filter(entry => Number.isFinite(entry.score))
      .sort((a, b) => a.score - b.score || a.item.title.localeCompare(b.item.title))
      .slice(0, 60).map(entry => entry.item);
    return ORDER.flatMap(group => scored.filter(item => item.group === group));
  });

  const index = () => Math.min(position(), Math.max(results().length - 1, 0));
  createEffect(on(index, () => list.querySelector('[data-active="true"]')?.scrollIntoView({ block: 'nearest' }), { defer: true }));
  function run(item: Item) { props.close(); item.run(); }

  return <dialog ref={dialog} class="palette" aria-label="Command palette"
    onCancel={event => { event.preventDefault(); props.close(); }}
    onClick={event => { if (event.target === event.currentTarget) props.close(); }}>
    <div class="palette-inner">
      <div class="palette-input"><Search size={16} aria-hidden="true" />
        <input ref={input} value={query()} placeholder="Search agents, accounts, pages, actions…" aria-label="Search" role="combobox"
          aria-expanded="true" aria-controls="palette-results" aria-activedescendant={results()[index()] ? 'cmd-' + index() : undefined} spellcheck={false}
          onInput={event => { setPosition(0); setQuery(event.currentTarget.value); }}
          onKeyDown={event => {
            const count = results().length;
            if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
              event.preventDefault();
              if (count) setPosition((index() + count + (event.key === 'ArrowDown' ? 1 : -1)) % count);
            }
            if (event.key === 'Enter' && results()[index()]) { event.preventDefault(); run(results()[index()]); }
          }} />
        <Kbd>Esc</Kbd>
      </div>
      <div class="palette-results" id="palette-results" role="listbox" ref={list}>
        <Show when={results().length === 0}><div class="palette-empty">No results for “{query()}”</div></Show>
        <For each={results()}>{(item, itemIndex) => <>
          <Show when={itemIndex() === 0 || results()[itemIndex() - 1].group !== item.group}><div class="palette-group" role="presentation">{item.group}</div></Show>
          <div id={'cmd-' + itemIndex()} role="option" aria-selected={index() === itemIndex()} data-active={index() === itemIndex()}
            class="palette-item" onMouseMove={() => setPosition(itemIndex())} onClick={() => run(item)}>
            <span class="palette-icon">{item.icon()}</span>
            <span class="palette-title"><Highlight text={item.title} query={query()} /></span>
            <Show when={item.detail}><span class="palette-detail">{item.detail}</span></Show>
            <Show when={index() === itemIndex()} fallback={<span class="palette-hint">{item.hint}</span>}><CornerDownLeft size={13} class="palette-enter" aria-hidden="true" /></Show>
          </div>
        </>}</For>
      </div>
      <div class="palette-foot"><span><Kbd>↑</Kbd><Kbd>↓</Kbd> Navigate</span><span><Kbd>↵</Kbd> Open</span><span><Kbd>Esc</Kbd> Close</span>
        <span class="num subtle">{results().length} results</span></div>
    </div>
  </dialog>;
}
