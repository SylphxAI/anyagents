import { For, Show, createSignal, createUniqueId, onCleanup, onMount } from 'solid-js';
import type { JSX } from 'solid-js';
import { Check, CircleAlert, Copy, Moon, RotateCw, Search, Sun, X } from 'lucide-solid';
import { errorMessage } from '../api';
import { highlight } from '../lib/search';
import { statusLabel, statusTone } from '../lib/status';
import type { AgentStatus } from '../lib/status';

export type Tone = 'ok' | 'warn' | 'bad' | 'neutral' | 'muted' | 'info';
type Children = { children?: JSX.Element };

export function Mark(props: { size?: number }) {
  return <svg class="mark" width={props.size ?? 20} height={props.size ?? 20} viewBox="0 0 24 24" aria-hidden="true">
    <rect width="24" height="24" rx="6" fill="currentColor" />
    <path d="M6.2 17.5 11 6.5h2l4.8 11h-2.4l-1-2.4H9.6l-1 2.4H6.2Zm4.2-4.4h3.2L12 9.1l-1.6 4Z" fill="var(--mark-ink)" fill-rule="evenodd" />
  </svg>;
}

export function Dot(props: { tone: Tone; pulse?: boolean; label?: string }) {
  return <span class={'dot ' + props.tone + (props.pulse ? ' pulse' : '')} role={props.label ? 'img' : undefined}
    aria-label={props.label} aria-hidden={props.label ? undefined : 'true'} />;
}

export function Pill(props: Children & { tone?: Tone; dot?: boolean; title?: string }) {
  return <span class={'pill ' + (props.tone ?? 'neutral')} title={props.title}><Show when={props.dot}><Dot tone={props.tone ?? 'neutral'} /></Show>{props.children}</span>;
}

export function StatusPill(props: { status: AgentStatus }) {
  return <Pill tone={statusTone[props.status]} dot>{statusLabel[props.status]}</Pill>;
}

export function Kbd(props: Children) { return <kbd class="kbd">{props.children}</kbd>; }

export function Highlight(props: { text: string; query: string }) {
  return <For each={highlight(props.text, props.query)}>{segment => segment.hit ? <mark>{segment.text}</mark> : segment.text}</For>;
}

export function Segmented<T extends string>(props: {
  value: T; options: { value: T; label: string; count?: number; tone?: Tone }[]; onChange: (value: T) => void; label: string;
}) {
  return <div class="seg" role="group" aria-label={props.label}><For each={props.options}>{option =>
    <button type="button" aria-pressed={option.value === props.value} onClick={() => props.onChange(option.value)}>
      {option.label}<Show when={option.count != null}><span class={'seg-count num ' + ((option.count ?? 0) > 0 && option.tone ? option.tone : '')}>{option.count}</span></Show>
    </button>}</For></div>;
}

export function Tabs<T extends string>(props: {
  value: T; tabs: { id: T; label: string; count?: number }[]; onChange: (value: T) => void; label: string;
}) {
  return <div class="tabs" role="tablist" aria-label={props.label}><For each={props.tabs}>{tab =>
    <button type="button" role="tab" aria-selected={tab.id === props.value} onClick={() => props.onChange(tab.id)}>
      {tab.label}<Show when={tab.count != null}><span class="tab-count num">{tab.count}</span></Show>
    </button>}</For></div>;
}

export function SearchField(props: {
  value: string; onChange: (value: string) => void; placeholder: string; inputRef?: (element: HTMLInputElement) => void; shortcut?: string; label?: string;
}) {
  return <label class="search">
    <Search size={14} aria-hidden="true" />
    <input ref={element => props.inputRef?.(element)} type="search" value={props.value} placeholder={props.placeholder} aria-label={props.label ?? props.placeholder} spellcheck={false}
      onInput={event => props.onChange(event.currentTarget.value)}
      onKeyDown={event => { if (event.key === 'Escape' && props.value) { event.stopPropagation(); props.onChange(''); } }} />
    <Show when={props.value} fallback={props.shortcut ? <Kbd>{props.shortcut}</Kbd> : null}>
      <button type="button" class="search-clear" aria-label="Clear search" onClick={() => props.onChange('')}><X size={13} /></button>
    </Show>
  </label>;
}

export function PageHeader(props: Children & { title: string; description?: JSX.Element; actions?: JSX.Element }) {
  return <header class="page-header">
    <div class="page-heading"><h1>{props.title}</h1><Show when={props.description}><p>{props.description}</p></Show></div>
    <Show when={props.actions}><div class="page-actions">{props.actions}</div></Show>
    {props.children}
  </header>;
}

export function Section(props: Children & { title: JSX.Element; meta?: JSX.Element; actions?: JSX.Element }) {
  return <section class="section">
    <div class="section-head"><h2>{props.title}</h2><Show when={props.meta}><span class="section-meta">{props.meta}</span></Show>
      <Show when={props.actions}><div class="section-actions">{props.actions}</div></Show></div>
    {props.children}
  </section>;
}

export interface Stat { label: string; value: JSX.Element; detail?: JSX.Element; tone?: Tone; href?: string }
export function StatStrip(props: { items: Stat[]; label: string }) {
  return <div class="stat-strip" role="list" aria-label={props.label}><For each={props.items}>{item => {
    const body = () => <><span class="stat-label">{item.label}</span>
      <span class={'stat-value num ' + (item.tone ?? '')}>{item.value}</span>
      <Show when={item.detail}><span class="stat-detail">{item.detail}</span></Show></>;
    return <div class="stat" role="listitem"><Show when={item.href} fallback={<div class="stat-body">{body()}</div>}><a class="stat-body" href={item.href}>{body()}</a></Show></div>;
  }}</For></div>;
}

export function Meter(props: { value: number; label: string }) {
  const tone = () => props.value <= 0 ? 'bad' : props.value < 20 ? 'warn' : 'ok';
  return <span class={'meter ' + tone()} role="meter" aria-valuenow={props.value} aria-valuemin={0} aria-valuemax={100} aria-label={props.label}>
    <span style={{ width: Math.max(props.value > 0 ? 3 : 0, props.value) + '%' }} />
  </span>;
}

export function Empty(props: Children & { icon?: JSX.Element; title: string; action?: JSX.Element; compact?: boolean }) {
  return <div class={'empty' + (props.compact ? ' compact' : '')} role="status">
    <Show when={props.icon}><div class="empty-icon" aria-hidden="true">{props.icon}</div></Show>
    <div class="empty-title">{props.title}</div>
    <Show when={props.children}><div class="empty-body">{props.children}</div></Show>
    <Show when={props.action}><div class="empty-action">{props.action}</div></Show>
  </div>;
}

export function Problem(props: Children & { retry?: () => void }) {
  return <div class="problem" role="alert"><CircleAlert size={15} aria-hidden="true" /><div>{props.children}</div>
    <Show when={props.retry}><button type="button" class="btn sm" onClick={() => props.retry?.()}><RotateCw size={13} />Retry</button></Show></div>;
}

export function Skeleton(props: { rows?: number }) {
  return <div class="skeleton" aria-busy="true" aria-label="Loading"><For each={Array.from({ length: props.rows ?? 6 }, (_, index) => index)}>{index =>
    <div class="skeleton-row" style={{ width: 92 - (index % 3) * 14 + '%' }} />}</For></div>;
}

export function Field(props: Children & { label: string; hint?: JSX.Element }) {
  return <label class="field"><span class="field-label">{props.label}</span>{props.children}<Show when={props.hint}><span class="field-hint">{props.hint}</span></Show></label>;
}
export function formText(data: FormData, field: string) { return String(data.get(field) ?? '').trim(); }

export function CopyButton(props: { value: string; label?: string; text?: boolean }) {
  const [result, setResult] = createSignal<'idle' | 'copied' | 'failed'>('idle');
  let timer: number | undefined;
  onCleanup(() => window.clearTimeout(timer));
  const label = () => props.label ?? 'Copy';
  const title = () => result() === 'copied' ? 'Copied' : result() === 'failed' ? 'Clipboard unavailable' : label();
  return <button type="button" class={props.text ? 'btn sm' : 'btn icon ghost sm'} title={title()} aria-label={title()} onClick={async event => {
    event.stopPropagation();
    try { await navigator.clipboard.writeText(props.value); setResult('copied'); } catch { setResult('failed'); }
    window.clearTimeout(timer); timer = window.setTimeout(() => setResult('idle'), 1600);
  }}>{result() === 'copied' ? <Check size={13} /> : result() === 'failed' ? <CircleAlert size={13} /> : <Copy size={13} />}<Show when={props.text}><span>{result() === 'idle' ? label() : title()}</span></Show></button>;
}

export function Dialog(props: Children & { title: string; description?: JSX.Element; close: () => void; busy?: boolean; wide?: boolean }) {
  let node!: HTMLDialogElement;
  const id = createUniqueId();
  const previouslyFocused = document.activeElement;
  onMount(() => node.showModal());
  onCleanup(() => { node.close(); if (previouslyFocused instanceof HTMLElement) previouslyFocused.focus(); });
  return <dialog ref={node} class={'dialog' + (props.wide ? ' wide' : '')} aria-labelledby={id}
    onCancel={event => { event.preventDefault(); if (!props.busy) props.close(); }}
    onClick={event => { if (event.target === event.currentTarget && !props.busy) props.close(); }}>
    <div class="dialog-inner">
      <header class="dialog-head"><div><h2 id={id}>{props.title}</h2><Show when={props.description}><p>{props.description}</p></Show></div>
        <button type="button" class="btn icon ghost sm" aria-label="Close dialog" disabled={props.busy} onClick={() => props.close()}><X size={15} /></button></header>
      {props.children}
    </div>
  </dialog>;
}

export function FormDialog(props: Children & {
  title: string; description?: JSX.Element; action: string; close: () => void;
  submit: (form: FormData, node: HTMLFormElement) => Promise<void>; danger?: boolean; wide?: boolean;
}) {
  const [busy, setBusy] = createSignal(false);
  const [error, setError] = createSignal('');
  async function save(event: SubmitEvent & { currentTarget: HTMLFormElement }) {
    event.preventDefault();
    if (busy()) return;
    const node = event.currentTarget;
    setBusy(true); setError('');
    try { await props.submit(new FormData(node), node); props.close(); } catch (cause) { setError(errorMessage(cause)); } finally { setBusy(false); }
  }
  return <Dialog title={props.title} description={props.description} close={props.close} busy={busy()} wide={props.wide}><form class="dialog-form" onSubmit={save}>
    <Show when={props.children}><fieldset class="dialog-body" disabled={busy()}>{props.children}</fieldset></Show>
    <Show when={error()}><div class="dialog-error"><Problem>{error()}</Problem></div></Show>
    <footer class="dialog-foot"><button class="btn" type="button" disabled={busy()} onClick={() => props.close()}>Cancel</button>
      <button class={'btn ' + (props.danger ? 'danger' : 'primary')} disabled={busy()}>{busy() ? 'Working…' : props.action}</button></footer>
  </form></Dialog>;
}

export type Theme = 'dark' | 'light';
export function readTheme(): Theme {
  try { const saved = localStorage.getItem('anyagents-theme'); if (saved === 'light' || saved === 'dark') return saved; } catch { /* Storage may be disabled. */ }
  return matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
}
function applyTheme(theme: Theme) { document.documentElement.dataset.theme = theme; document.documentElement.style.colorScheme = theme; }
const [theme, setTheme] = createSignal<Theme>(readTheme());
applyTheme(theme());
export const themeState = {
  theme,
  toggle: () => {
    const next = theme() === 'light' ? 'dark' : 'light';
    setTheme(next); applyTheme(next);
    try { localStorage.setItem('anyagents-theme', next); } catch { /* Session-only theme. */ }
  },
};
export function ThemeToggle() {
  const label = () => 'Switch to ' + (theme() === 'light' ? 'dark' : 'light') + ' theme';
  return <button type="button" class="btn icon ghost" title={label()} aria-label={label()} onClick={themeState.toggle}>
    <Show when={theme() === 'light'} fallback={<Sun size={15} />}><Moon size={15} /></Show></button>;
}
