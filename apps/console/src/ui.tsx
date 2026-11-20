import { useEffect, useId, useRef, useState } from 'react';
import type { FormEvent, ReactNode } from 'react';
import { Check, CircleAlert, Copy, Moon, RefreshCw, Sun, X } from 'lucide-react';
import { errorMessage } from './api';

export function Mark({ size = 32 }: { size?: number }) {
  return <svg className="brand-mark" width={size} height={size} viewBox="0 0 32 32" fill="none" aria-hidden="true"><rect width="32" height="32" rx="8" fill="currentColor"/><path d="M7 23.5 14.8 8h2.4L25 23.5h-4.2l-1.4-3h-6.8l-1.5 3H7Z M14 17.5h4L16 13l-2 4.5Z" fillRule="evenodd" fill="var(--mark-ink)"/></svg>;
}
export function useTheme() {
  const [theme, setTheme] = useState(() => {
    try { const saved = localStorage.getItem('anyagents-theme'); if (saved === 'light' || saved === 'dark') return saved; } catch { /* Storage may be disabled. */ }
    return matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
  });
  useEffect(() => { document.documentElement.dataset.theme = theme; }, [theme]);
  return { theme, toggle: () => {
    const next = theme === 'light' ? 'dark' : 'light';
    setTheme(next);
    try { localStorage.setItem('anyagents-theme', next); } catch { /* Session-only theme. */ }
  } };
}
export function ThemeButton({ theme, toggle }: ReturnType<typeof useTheme>) {
  const label = 'Switch to ' + (theme === 'light' ? 'dark' : 'light') + ' theme';
  return <button type="button" className="icon-button" title={label} aria-label={label} onClick={toggle}>{theme === 'light' ? <Moon /> : <Sun />}</button>;
}
export function Badge({ children, tone = 'muted' }: { children: ReactNode; tone?: string }) { return <span className={'badge ' + tone}>{children}</span>; }
export function Dot({ tone }: { tone: string }) { return <span className={'dot ' + tone} aria-hidden="true" />; }
export function Status({ value }: { value: string }) { return <Badge tone={value}><Dot tone={value} />{value}</Badge>; }
export function Empty({ children }: { children: ReactNode }) { return <div className="empty">{children}</div>; }
export function Problem({ children, retry }: { children: ReactNode; retry?: () => void }) {
  return <div className="problem" role="alert"><CircleAlert /><div>{children}</div>{retry && <button type="button" className="button" onClick={retry}>Retry</button>}</div>;
}
export function Refresh({ onClick }: { onClick: () => void }) {
  return <button type="button" className="icon-button" title="Refresh" aria-label="Refresh" onClick={onClick}><RefreshCw /></button>;
}
export function Heading({ eyebrow, title, blurb, children }: { eyebrow: string; title: string; blurb: string; children?: ReactNode }) {
  return <div className="page-heading"><div><div className="eyebrow"><span />{eyebrow}</div><h1>{title}</h1><p>{blurb}</p></div><div className="actions">{children}</div></div>;
}
export function Panel({ title, icon, actions, children }: { title: string; icon?: ReactNode; actions?: ReactNode; children: ReactNode }) {
  return <section className="panel"><header className="panel-heading"><h2>{icon}{title}</h2><div className="actions">{actions}</div></header>{children}</section>;
}
export function Metrics({ items }: { items: { label: string; value: ReactNode; detail?: ReactNode }[] }) {
  return <div className="metrics" style={{ '--metric-count': items.length } as React.CSSProperties}>{items.map(item =>
    <div className="metric" key={item.label}><span>{item.label}</span><strong>{item.value}</strong><small>{item.detail}</small></div>)}</div>;
}
export function Table({ headers, rows, empty }: { headers: string[]; rows: { key: string; cells: ReactNode[] }[]; empty: string }) {
  if (!rows.length) return <Empty>{empty}</Empty>;
  return <div className="table-scroll" role="region" aria-label={headers[0] + ' table'} tabIndex={0}><table><thead><tr>{headers.map(header => <th scope="col" key={header}>{header}</th>)}</tr></thead>
    <tbody>{rows.map(row => <tr key={row.key}>{row.cells.map((cell, index) => <td key={index}>{cell}</td>)}</tr>)}</tbody></table></div>;
}
export function Field({ label, children }: { label: string; children: ReactNode }) { return <label className="field"><span>{label}</span>{children}</label>; }
export function Options({ items }: { items: string[] }) { return <>{items.map(item => <option key={item}>{item}</option>)}</>; }
export function formText(data: FormData, field: string) { return String(data.get(field) ?? '').trim(); }
export function CopyButton({ value, label = 'Copy', text = false }: { value: string; label?: string; text?: boolean }) {
  const [result, setResult] = useState<'idle' | 'copied' | 'failed'>('idle');
  useEffect(() => {
    if (result === 'idle') return;
    const timeout = window.setTimeout(() => setResult('idle'), 1800);
    return () => window.clearTimeout(timeout);
  }, [result]);
  const title = result === 'copied' ? 'Copied' : result === 'failed' ? 'Clipboard unavailable' : label;
  return <button type="button" className={text ? 'button' : 'icon-button'} title={title} aria-label={title} onClick={async () => {
    try { await navigator.clipboard.writeText(value); setResult('copied'); } catch { setResult('failed'); }
  }}>{result === 'copied' ? <Check /> : result === 'failed' ? <CircleAlert /> : <Copy />}{text && title}</button>;
}
export function Dialog({ title, children, close, busy = false, className = '' }: {
  title: string; children: ReactNode; close: () => void; busy?: boolean; className?: string;
}) {
  const ref = useRef<HTMLDialogElement>(null);
  const id = useId();
  useEffect(() => {
    const node = ref.current;
    const previouslyFocused = document.activeElement;
    node?.showModal();
    return () => { node?.close(); if (previouslyFocused instanceof HTMLElement) previouslyFocused.focus(); };
  }, []);
  return <dialog ref={ref} className={'dialog ' + className} aria-labelledby={id} onCancel={event => { event.preventDefault(); if (!busy) close(); }}>
    <header className="panel-heading"><h2 id={id}>{title}</h2><button type="button" className="icon-button" aria-label="Close dialog" disabled={busy} onClick={close}><X /></button></header>{children}
  </dialog>;
}
export function ActionForm({ title, action, close, children, submit, danger }: {
  title: string; action: string; close: () => void; children: ReactNode; submit: (form: FormData, node: HTMLFormElement) => Promise<void>; danger?: boolean;
}) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  async function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (busy) return;
    const node = event.currentTarget;
    setBusy(true); setError('');
    try { await submit(new FormData(node), node); close(); } catch (error) { setError(errorMessage(error)); } finally { setBusy(false); }
  }
  return <Dialog title={title} close={close} busy={busy}><form className="dialog-body" onSubmit={save}>
    <fieldset className="form-fields" disabled={busy}>{children}</fieldset>{error && <Problem>{error}</Problem>}
    <div className="dialog-footer"><button className="button" type="button" disabled={busy} onClick={close}>Cancel</button><button className={'button ' + (danger ? 'danger' : 'primary')} disabled={busy}>{busy ? 'Working…' : action}</button></div>
  </form></Dialog>;
}
