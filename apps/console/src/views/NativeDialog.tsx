import { useEffect, useRef, useState } from 'react';
import type { FormEvent } from 'react';
import { ExternalLink } from 'lucide-react';
import { api, errorMessage, request } from '../api';
import { CopyButton, Dialog, Field, Problem } from '../ui';

export const nativeProviders = [
  { id: 'anthropic', label: 'Claude', harness: 'claude-code' },
  { id: 'google-antigravity', label: 'Google Antigravity', harness: 'agy' },
  { id: 'openai', label: 'OpenAI / Codex', harness: 'codex' },
];
const endpoint = '/v1/profiles/native-signin';
type NativeEvent = { kind: string; text?: string };

function safeLink(value: string) {
  try {
    const url = new URL(value);
    if (url.protocol === 'https:' && !url.username && !url.password) return url.href;
  } catch { /* Untrusted provider output is not necessarily a URL. */ }
}

export function NativeDialog({ close, updated, initialProvider }: { close: () => void; updated: () => void; initialProvider?: string }) {
  const controller = useRef<AbortController | null>(null);
  const onUpdated = useRef(updated);
  useEffect(() => { onUpdated.current = updated; }, [updated]);
  const [provider, setProvider] = useState(initialProvider ?? nativeProviders[0].id);
  const [busy, setBusy] = useState(false);
  const [events, setEvents] = useState<NativeEvent[]>([]);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [session, setSession] = useState('');
  const [expectsInput, setExpectsInput] = useState(false);
  const [callback, setCallback] = useState('');
  const [inputStatus, setInputStatus] = useState('');
  const [inputError, setInputError] = useState('');
  const sending = useRef(false);
  const [submitting, setSubmitting] = useState(false);

  async function start(chosen: string) {
    if (controller.current) return;
    const harness = nativeProviders.find(item => item.id === chosen)?.harness;
    if (!harness) return;
    const run = new AbortController(); controller.current = run;
    setEvents([]); setError(''); setSuccess(''); setSession(''); setExpectsInput(false);
    setCallback(''); setInputStatus(''); setInputError(''); setBusy(true);
    let terminal = false;
    const connectionTimeout = window.setTimeout(() => run.abort('connection-timeout'), 25_000);
    const sessionTimeout = window.setTimeout(() => run.abort('session-timeout'), 320_000);
    function handle(line: string) {
      if (!line.trim() || run.signal.aborted || terminal) return;
      const message = JSON.parse(line) as NativeEvent;
      if (message.kind === 'session') setSession(message.text ?? '');
      else if (message.kind === 'input') setExpectsInput(true);
      else if (message.kind === 'done') {
        terminal = true;
        setSuccess(message.text ? 'Connected ' + message.text + '.' : 'Account connected.');
        onUpdated.current();
      } else if (message.kind === 'error') {
        terminal = true; setError(message.text || 'Could not connect this account. Try again.');
      } else if (['link', 'code', 'prompt'].includes(message.kind)) setEvents(previous => [...previous, message].slice(-40));
    }
    try {
      const response = await request(endpoint, { method: 'POST', signal: run.signal, accept: 'application/x-ndjson', body: { harness } });
      window.clearTimeout(connectionTimeout);
      if (!response.body) throw new Error('Sign-in stream unavailable. Try again.');
      const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
      try {
        let tail = '';
        while (!terminal) {
          const { value, done } = await reader.read();
          if (done) break;
          const lines = (tail + value).split('\n'); tail = lines.pop() ?? '';
          for (const line of lines) handle(line);
        }
        handle(tail);
        if (!terminal && !run.signal.aborted) throw new Error('Sign-in disconnected before confirmation. Start a new sign-in.');
      } finally { await reader.cancel().catch(() => {}); }
    } catch (cause) {
      if (!run.signal.aborted) setError(errorMessage(cause));
      else if (run.signal.reason === 'connection-timeout') setError('Could not reach the sign-in service. Check your connection and try again.');
      else if (run.signal.reason === 'session-timeout') setError('This sign-in expired. Start a new sign-in.');
    } finally {
      window.clearTimeout(connectionTimeout); window.clearTimeout(sessionTimeout);
      controller.current = null; setBusy(false); setCallback('');
    }
  }
  useEffect(() => {
    if (initialProvider) void start(initialProvider);
    return () => controller.current?.abort();
    // Only auto-start the provider selected on the card, once per dialog.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialProvider]);

  async function submitCallback(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const run = controller.current;
    if (!run || sending.current || !callback.trim() || !session) return;
    sending.current = true; setSubmitting(true); setInputError(''); setInputStatus('');
    try {
      await api(`${endpoint}/${encodeURIComponent(session)}/input`, { method: 'POST', signal: run.signal, body: { value: callback.trim() } });
      setCallback(''); setInputStatus('Submitted. Waiting for the provider to confirm…');
    } catch (cause) { if (!run.signal.aborted) setInputError(errorMessage(cause)); }
    finally { sending.current = false; setSubmitting(false); }
  }

  const supported = nativeProviders.some(item => item.id === provider);
  return <Dialog title="Connect Account" close={close}>
    <div className="dialog-body">
      <form className="stack" onSubmit={event => { event.preventDefault(); void start(provider); }}>
        <p className="muted">Sign in with your provider. The authenticated account determines which profile is refreshed or created.</p>
        <Field label="Provider"><select value={provider} disabled={busy} onChange={event => setProvider(event.target.value)}>
          {!supported && <option value={provider}>{provider}</option>}
          {nativeProviders.map(item => <option key={item.id} value={item.id}>{item.label}</option>)}
        </select></Field>
        {!supported && <Problem>This provider does not support native sign-in from the console. Reconnect it using its supported credential setup.</Problem>}
        <div className="dialog-footer">
          {busy ? <button type="button" className="button" onClick={() => controller.current?.abort()}>Cancel sign-in</button> :
            <button className="button primary" disabled={!supported}>{error ? 'Retry sign-in' : 'Start sign-in'}</button>}
        </div>
      </form>
      {busy && <p className="muted" role="status">{events.length ? 'Waiting for sign-in…' : 'Starting sign-in…'}</p>}
      {events.length > 0 && !success && <div className="native-events" aria-live="polite">{events.map(({ kind, text }, index) => {
        if (!text) return null;
        const link = kind === 'link' ? safeLink(text) : undefined;
        if (link) return <div className="cluster" key={index}><a className="button" href={link} target="_blank" rel="noopener noreferrer"><ExternalLink size={15} /> Open sign-in page</a><CopyButton value={link} label="Copy sign-in URL" text /></div>;
        if (kind === 'code') return <div className="cluster native-code" key={index}><span>Device code</span><code>{text}</code><CopyButton value={text} label="Copy device code" /></div>;
        return kind === 'prompt' ? <p key={index}>{text}</p> : null;
      })}</div>}
      {busy && session && expectsInput && <form className="stack" onSubmit={submitCallback}>
        <Field label="Callback URL or authorization code"><input type="password" value={callback} onChange={event => setCallback(event.target.value)}
          autoComplete="off" spellCheck={false} maxLength={8192} required /></Field>
        <p className="muted">Paste the code shown by the provider, or the full callback URL from your browser address bar.</p>
        <div className="actions"><button className="button primary" disabled={submitting || !callback.trim()}>{submitting ? 'Submitting…' : 'Submit callback'}</button></div>
        {inputStatus && <p role="status">{inputStatus}</p>}
        {inputError && <Problem>{inputError}</Problem>}
      </form>}
      {success && <p className="native-success" role="status">{success}</p>}
      {error && <Problem>{error}</Problem>}
    </div>
  </Dialog>;
}
