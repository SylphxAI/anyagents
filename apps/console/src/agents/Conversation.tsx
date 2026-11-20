import { For, Index, Show, createEffect, createSignal, on } from 'solid-js';
import { ArrowUp, MessagesSquare, Wrench } from 'lucide-solid';
import { agentPath, api, createPoll, errorMessage } from '../api';
import { dateTime } from '../format';
import { groupTurns, recipientLabel } from '../lib/conversation';
import { agentHref } from '../route';
import { CopyButton, Empty, Problem, Skeleton } from '../ui/primitives';

type Turn = { seq: number; id?: string; timestamp_ms?: number; direction?: string; role?: string; from?: string; to?: string; tool_name?: string; tool_names?: string[]; text?: string };

function MessageText(props: { text: string }) {
  const blocks = () => props.text.split(/(```[^\n]*\n[\s\S]*?```)/g).filter(Boolean);
  return <div class="message-text"><For each={blocks()}>{block => {
    if (!block.startsWith('```')) return <p>{block.trim()}</p>;
    const newline = block.indexOf('\n');
    return <pre class="code"><code>{block.slice(newline + 1, -3).trimEnd()}</code></pre>;
  }}</For></div>;
}

export function Conversation(props: { name: string; archived: boolean; composerRef?: (element: HTMLTextAreaElement) => void }) {
  const [draft, setDraft] = createSignal('');
  const [busy, setBusy] = createSignal(false);
  const [sendError, setSendError] = createSignal('');
  const [sent, setSent] = createSignal(false);
  let feed!: HTMLDivElement;
  let follow = true;
  const conversation = createPoll(signal => api<{ turns: Turn[] }>(agentPath(props.name) + '/conversation?limit=50', { signal }), { interval: 4000 });
  const turns = () => groupTurns(conversation.data()?.turns ?? []);
  createEffect(on(conversation.data, () => { if (follow) queueMicrotask(() => { feed.scrollTop = feed.scrollHeight; }); }));

  async function send(event: SubmitEvent) {
    event.preventDefault();
    const text = draft().trim();
    if (!text || busy() || props.archived) return;
    setBusy(true); setSendError(''); setSent(false);
    try {
      await api('/v1/send', { method: 'POST', body: { sender: 'operator', to: ['agent:' + props.name], text } });
      setDraft(''); setSent(true); follow = true; conversation.refresh();
    } catch (cause) { setSendError(errorMessage(cause)); } finally { setBusy(false); }
  }

  return <div class="conversation">
    <div class="feed" ref={feed} tabindex="0" aria-label="Conversation history" aria-live="polite"
      onScroll={() => { follow = feed.scrollHeight - feed.scrollTop - feed.clientHeight < 48; }}>
      <Show when={conversation.error()}><Problem retry={conversation.refresh}>{conversation.error()}</Problem></Show>
      <Show when={conversation.loading()}><Skeleton rows={5} /></Show>
      <Show when={conversation.data() && !turns().length}><Empty compact icon={<MessagesSquare size={18} />} title="No messages yet">Send a message below to steer {props.name}.</Empty></Show>
      <Index each={turns()}>{turn => {
        const outbound = () => turn().direction === 'out';
        const system = () => turn().role === 'system';
        const tools = () => turn().tool_names?.length ? turn().tool_names! : turn().tool_name ? [turn().tool_name!] : [];
        const author = () => system() ? 'System' : turn().from || (outbound() ? props.name : 'operator');
        return <article class={'message' + (outbound() ? ' out' : system() ? ' system' : ' in')}>
          <span class="avatar sm" aria-hidden="true">{author().slice(0, 1).toUpperCase()}</span>
          <div class="message-main">
            <header><span class="strong">{author()}</span>
              <Show when={turn().recipients.length}><span class="recipients" aria-label={'To ' + turn().recipients.map(recipientLabel).join(', ')}>
                <span class="subtle" aria-hidden="true">→</span>
                <For each={turn().recipients.slice(0, 4)}>{address => address.startsWith('agent:')
                  ? <a class="tag link" href={agentHref(recipientLabel(address))}>{recipientLabel(address)}</a> : <span class="tag">{recipientLabel(address)}</span>}</For>
                <Show when={turn().recipients.length > 4}><span class="tag" title={turn().recipients.slice(4).map(recipientLabel).join(', ')}>+{turn().recipients.length - 4}</span></Show>
                <Show when={turn().recipients.length > 1}><span class="subtle small num">{turn().recipients.length} recipients</span></Show>
              </span></Show>
              <time class="num subtle">{dateTime(turn().timestamp_ms)}</time>
              <Show when={turn().text}><span class="message-copy"><CopyButton value={turn().text ?? ''} label="Copy message" /></span></Show></header>
            <Show when={turn().text}><MessageText text={turn().text!} /></Show>
            <Show when={tools().length}><div class="tool-calls"><Wrench size={12} aria-hidden="true" /><For each={tools()}>{tool => <span class="tag mono">{tool}</span>}</For></div></Show>
          </div>
        </article>;
      }}</Index>
    </div>
    <Show when={!props.archived}><form class="composer" onSubmit={send}>
      <Show when={sendError()}><Problem>{sendError()}</Problem></Show>
      <div class="composer-box">
        <textarea ref={element => props.composerRef?.(element)} aria-label={'Message ' + props.name} placeholder={'Steer ' + props.name + '…'} value={draft()} disabled={busy()} rows={2}
          onInput={event => { setDraft(event.currentTarget.value); setSent(false); }} onKeyDown={event => {
            if (event.key === 'Enter' && !event.shiftKey && !event.isComposing) { event.preventDefault(); event.currentTarget.form?.requestSubmit(); }
          }} />
        <button class="btn primary icon" title="Send message" aria-label="Send message" disabled={busy() || !draft().trim()}><ArrowUp size={15} /></button>
      </div>
      <div class="composer-hint" role="status">{busy() ? 'Sending…' : sent() ? 'Delivered to the mailbox' : 'Enter to send · Shift+Enter for a new line'}</div>
    </form></Show>
  </div>;
}
