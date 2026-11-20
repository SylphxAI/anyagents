import { useCallback, useLayoutEffect, useRef, useState } from 'react';
import type { FormEvent } from 'react';
import { ArrowUp, ChevronDown, MessageSquare, Terminal } from 'lucide-react';
import { agentPath, api, errorMessage, useResource } from '../api';
import { time } from '../format';
import { Badge, CopyButton, Empty, Panel, Problem, Refresh } from '../ui';

type Turn = { seq: number; timestamp_ms?: number; direction?: string; role?: string; from?: string; to?: string; tool_name?: string; tool_names?: string[]; text?: string };
function MessageText({ text }: { text: string }) {
  const blocks = text.split(/(\x60{3}[^\n]*\n[\s\S]*?\x60{3})/g);
  return <div className="message-text">{blocks.filter(Boolean).map((block, index) => {
    if (!block.startsWith('\x60\x60\x60')) return <p key={index}>{block}</p>;
    const newline = block.indexOf('\n');
    return <div className="code-block" key={index}><div className="code-label">{block.slice(3, newline).trim() || 'Code'}</div><pre><code>{block.slice(newline + 1, -3).trimEnd()}</code></pre></div>;
  })}</div>;
}
export function Conversation({ name, archived }: { name: string; archived: boolean }) {
  const [live, setLive] = useState(true);
  const [draft, setDraft] = useState('');
  const [busy, setBusy] = useState(false);
  const [sendError, setSendError] = useState('');
  const [sent, setSent] = useState(false);
  const feed = useRef<HTMLDivElement>(null);
  const follow = useRef(true);
  const load = useCallback((signal: AbortSignal) => api<{ turns: Turn[] }>(agentPath(name) + '/conversation?limit=50', { signal }), [name]);
  const { data, error, refresh } = useResource(load, live ? 4000 : 0);
  const turns = data?.turns ?? [];
  useLayoutEffect(() => { const node = feed.current; if (node && follow.current) node.scrollTop = node.scrollHeight; }, [data]);
  async function send(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const text = draft.trim();
    if (!text || busy || archived) return;
    setBusy(true); setSendError(''); setSent(false);
    try {
      await api('/v1/send', { method: 'POST', body: { sender: 'operator', to: ['agent:' + name], text } });
      setDraft(''); setSent(true); follow.current = true; refresh();
    } catch (err) { setSendError(errorMessage(err)); } finally { setBusy(false); }
  }
  return <Panel title="Conversation" icon={<MessageSquare size={16} />} actions={<>
    {data && <Badge>{turns.length} turns</Badge>}<label className="check-label"><input type="checkbox" checked={live} onChange={event => setLive(event.target.checked)} />Live</label><Refresh onClick={refresh} />
  </>}>
    {error && <Problem retry={refresh}>{error}</Problem>}
    <div className="feed" ref={feed} tabIndex={0} aria-label="Conversation history" onScroll={event => {
      const node = event.currentTarget;
      follow.current = node.scrollHeight - node.scrollTop - node.clientHeight < 48;
    }}>
      {!turns.length && <Empty>{data ? 'No conversation turns yet.' : error ? 'Conversation unavailable.' : 'Loading conversation…'}</Empty>}
      {turns.map(turn => {
        const direction = turn.role === 'system' ? 'SYS' : turn.direction === 'out' ? 'OUT' : 'IN';
        const tools = turn.tool_names?.length ? turn.tool_names : turn.tool_name ? [turn.tool_name] : [];
        return <article className={'message turn-' + direction.toLowerCase()} key={turn.seq}>
          <div className="message-heading"><Badge tone={direction === 'OUT' ? 'working' : direction === 'IN' ? 'idle' : 'muted'}>{direction}</Badge><strong>{turn.from || 'System'}{turn.to && <span> → {turn.to}</span>}</strong>
            <time>{time(turn.timestamp_ms)}</time><CopyButton value={turn.text ?? ''} label="Copy message" /></div>
          {turn.text && <MessageText text={turn.text} />}
          {tools.length > 0 && <details className="tools"><summary><Terminal />{tools.length} tool {tools.length === 1 ? 'call' : 'calls'}<ChevronDown /></summary><div className="actions">{tools.map((tool, index) => <Badge key={index}>{tool}</Badge>)}</div></details>}
        </article>;
      })}
    </div>
    {!archived && <form className="composer" onSubmit={send}>
      {sendError && <Problem>{sendError}</Problem>}
      <div className="composer-box"><textarea aria-label="Message" placeholder={'Message ' + name + '…'} value={draft} disabled={busy} rows={2}
        onChange={event => { setDraft(event.target.value); setSent(false); }} onKeyDown={event => {
          if (event.key === 'Enter' && !event.shiftKey && !event.nativeEvent.isComposing) { event.preventDefault(); event.currentTarget.form?.requestSubmit(); }
        }} /><button className="button primary" title="Send message" aria-label="Send message" disabled={busy || !draft.trim()}><ArrowUp /></button></div>
      <div className="composer-hint" role="status">{busy ? 'Sending…' : sent ? 'Message sent' : 'Enter to send · Shift + Enter for a new line'}</div>
    </form>}
  </Panel>;
}
