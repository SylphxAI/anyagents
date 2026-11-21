import { For, Index, Match, Show, Switch, createMemo, createSignal, onCleanup, onMount } from 'solid-js';
import { Plus, RotateCw, SearchX, Shield, ShieldCheck } from 'lucide-solid';
import { api, createPoll } from '../api';
import { plural } from '../format';
import { activeBlocks, blockId, blockRequest, channelLabel, filterBlocks, ingressPath, leaveChatNote } from '../lib/ingress';
import type { BlockKind, BlockReceipt, BlockRule } from '../lib/ingress';
import { useConsole } from '../state';
import { Empty, Field, FormDialog, Problem, SearchField, Section, Segmented, Skeleton, formText } from '../ui/primitives';

export function IngressSettings() {
  const { roster } = useConsole();
  const [recipient, setRecipient] = createSignal('');
  const agents = createMemo(() => (roster.data()?.agents ?? []).slice().sort((a, b) => a.name.localeCompare(b.name) || a.id.localeCompare(b.id)));
  const selected = createMemo(() => agents().find(agent => agent.id === recipient()));
  const name = () => selected()?.name ?? '';
  return <>
    <Show when={roster.error()}><Problem retry={roster.refresh}>Could not load receiving agents: {roster.error()}</Problem></Show>
    <Switch>
      <Match when={roster.loading()}><Skeleton rows={3} /></Match>
      <Match when={roster.data() && agents().length === 0}>
        <Empty compact icon={<Shield size={18} />} title="No receiving agents">Create an agent before adding ingress blocks.</Empty>
      </Match>
      <Match when={agents().length > 0}>
        <div class="ingress-recipient">
          <Field label="Receiving agent">
            <select class="input" value={selected()?.id ?? ''} onChange={event => setRecipient(event.currentTarget.value)}>
              <option value="" selected={!selected()}>Choose an agent…</option>
              <Index each={agents()}>{agent => <option value={agent().id} selected={selected()?.id === agent().id}>{agent().name}{agent().state === 'archived' ? ' (archived)' : ''}</option>}</Index>
            </select>
          </Field>
          <p class="subtle">Rules apply only to this agent. Protected senders bypass ingress blocks.</p>
        </div>
        <Show when={selected()?.id} keyed fallback={<Empty compact icon={<Shield size={18} />} title="Choose a receiving agent">
          Select the agent whose incoming messages you want to manage.
        </Empty>}>
          {id => <Blocklist agentId={id} agentName={name()} />}
        </Show>
      </Match>
    </Switch>
  </>;
}

function Blocklist(props: { agentId: string; agentName: string }) {
  const path = ingressPath(props.agentId);
  const [query, setQuery] = createSignal('');
  const [kind, setKind] = createSignal<BlockKind | 'all'>('all');
  const [now, setNow] = createSignal(Date.now());
  const [refreshing, setRefreshing] = createSignal(true);
  const [dialog, setDialog] = createSignal<'add' | BlockRule>();
  const [notice, setNotice] = createSignal<{ text: string; advisory?: string }>();
  const mutations = new AbortController();
  let feedback!: HTMLDivElement;
  const rules = createPoll(async signal => {
    try {
      const data = await api<{ result: 'blocks'; rules: BlockRule[] }>(path, { signal });
      if (!signal.aborted) setNow(Date.now());
      return data;
    } finally { if (!signal.aborted) setRefreshing(false); }
  });
  onMount(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 30_000);
    onCleanup(() => window.clearInterval(timer));
  });
  onCleanup(() => mutations.abort());
  const active = createMemo(() => activeBlocks(rules.data()?.rules ?? [], now()));
  const shown = createMemo(() => filterBlocks(active(), query(), kind()));
  const editable = () => !!rules.data() && !rules.error() && !refreshing();
  const clear = () => { setQuery(''); setKind('all'); };
  const refresh = () => { setRefreshing(true); rules.refresh(); };
  const close = () => setDialog(undefined);
  const reconcile = (text: string, advisory?: string) => {
    if (mutations.signal.aborted) return;
    setNotice({ text, advisory });
    refresh();
    requestAnimationFrame(() => feedback?.focus());
  };

  return <Section title="Active blocks" meta={<Show when={rules.data() && !rules.error()}><span class="num subtle">{refreshing() ? 'Refreshing…' : active().length}</span></Show>}
    actions={<>
      <button type="button" class="btn" disabled={refreshing()} onClick={refresh}><RotateCw size={13} />Refresh</button>
      <button type="button" class="btn primary" disabled={!editable()} onClick={() => setDialog('add')}><Plus size={14} />Add block</button>
    </>}>
    <div class="ingress-feedback" ref={feedback} tabindex={-1} role="status">
      <Show when={notice()}>{entry => <div class="notice ingress-notice"><ShieldCheck size={16} aria-hidden="true" /><div>
        <strong>{entry().text}</strong><Show when={entry().advisory}><p>{entry().advisory}</p></Show>
      </div></div>}</Show>
    </div>
    <Show when={rules.error()}><Problem>Could not load ingress blocks: {rules.error()} Use Refresh to try again.</Problem></Show>
    <Show when={rules.loading()}><Skeleton rows={3} /></Show>
    <Show when={rules.data() && !rules.error()}>
      <Show when={active().length} fallback={<Empty compact icon={<Shield size={18} />} title="No active blocks">
        Blocked messages are dropped before they reach the mailbox. They don’t wake this agent or use model tokens.
      </Empty>}>
        <div class="toolbar ingress-toolbar">
          <SearchField label="Search ingress blocks" placeholder="Search provider, kind, or ID…" value={query()} onChange={setQuery} />
          <Segmented label="Target kind" value={kind()} onChange={setKind} options={[
            { value: 'all', label: 'All' }, { value: 'sender', label: 'Senders' }, { value: 'chat', label: 'Chats' },
          ]} />
        </div>
        <Show when={shown().length} fallback={<Empty compact icon={<SearchX size={18} />} title="No matching blocks"
          action={<button type="button" class="btn sm" onClick={clear}>Clear filters</button>}>
          Try a different provider, kind, or identifier.
        </Empty>}>
          <div class="panel ingress-list">
            <div class="ingress-row ingress-list-head eyebrow" aria-hidden="true"><span>Target</span><span>Reason</span><span>Created</span><span>Expires</span><span /></div>
            <ul aria-label="Active ingress blocks" role="list">
              <For each={shown()}>{rule => <li class="ingress-row">
                <div class="ingress-target">
                  <span class="tag">{channelLabel(rule.target.channel)} · {rule.target.scope.kind === 'sender' ? 'Sender' : 'Chat'}</span>
                  <span class="mono strong">{blockId(rule.target)}</span>
                </div>
                <div class="ingress-reason"><span class="ingress-label eyebrow">Reason</span>{rule.reason || <span class="subtle">No reason</span>}</div>
                <div><span class="ingress-label eyebrow">Created</span><RuleTime ms={rule.created_at_ms} /></div>
                <div><span class="ingress-label eyebrow">Expires</span><Show when={rule.expires_at_ms != null} fallback={<span class="subtle">Never</span>}><RuleTime ms={rule.expires_at_ms!} /></Show></div>
                <div class="ingress-row-action"><button type="button" class="btn sm" disabled={!editable()}
                  aria-label={'Unblock ' + rule.target.scope.kind + ' ' + blockId(rule.target)} onClick={() => setDialog(rule)}>Unblock</button></div>
              </li>}</For>
            </ul>
          </div>
          <p class="section-foot subtle" role="status">{shown().length} of {plural(active().length, 'active block')}</p>
        </Show>
      </Show>
    </Show>
    <Show when={dialog()} keyed>{current => current === 'add'
      ? <AddBlock agentName={props.agentName} close={close} save={async body => {
        const receipt = await api<BlockReceipt>(path, { method: 'POST', body, signal: mutations.signal });
        clear();
        reconcile('Blocked ' + receipt.rule.target.scope.kind + ' ' + blockId(receipt.rule.target) + ' for ' + props.agentName + '.', leaveChatNote(receipt));
      }} />
      : <FormDialog title="Unblock target?" action="Unblock" close={close}
          description={<>{channelLabel(current.target.channel)} {current.target.scope.kind} <span class="mono">{blockId(current.target)}</span> can reach <strong>{props.agentName}</strong> again if allowed by channel policy.{current.target.scope.kind === 'chat' && current.target.channel === 'telegram' ? ' Unblocking does not rejoin a chat the bot left.' : ''}</>}
          submit={async () => {
            const receipt = await api<{ result: 'unblocked'; removed: boolean }>(path, { method: 'DELETE', body: { op: 'unblock_sender', target: current.target }, signal: mutations.signal });
            reconcile((receipt.removed ? 'Unblocked ' : 'Already unblocked: ') + current.target.scope.kind + ' ' + blockId(current.target) + ' for ' + props.agentName + '.');
          }} />}
    </Show>
  </Section>;
}

const ruleDate = new Intl.DateTimeFormat(undefined, { dateStyle: 'medium', timeStyle: 'short' });
function RuleTime(props: { ms: number }) {
  const date = () => new Date(props.ms);
  return <Show when={Number.isFinite(date().getTime())} fallback={<span class="subtle">Date out of range</span>}>
    <time class="num subtle" dateTime={date().toISOString()}>{ruleDate.format(date())}</time>
  </Show>;
}

function AddBlock(props: { agentName: string; close: () => void; save: (body: ReturnType<typeof blockRequest>) => Promise<void> }) {
  const [kind, setKind] = createSignal<BlockKind>('sender');
  return <FormDialog title="Add ingress block" description={<>Block incoming Telegram messages to <strong>{props.agentName}</strong>. Protected senders cannot be blocked.</>}
    action="Add block" close={props.close} submit={form => props.save(blockRequest(kind(), formText(form, 'target'), formText(form, 'reason'), formText(form, 'expiry')))}>
    <Field label="Target kind"><select class="input" value={kind()} onChange={event => setKind(event.currentTarget.value === 'chat' ? 'chat' : 'sender')}>
      <option value="sender">Sender</option><option value="chat">Chat</option>
    </select></Field>
    <Field label={kind() === 'sender' ? 'Sender ID' : 'Chat ID'} hint={kind() === 'sender' ? 'Use the numeric Telegram user ID, not the username.' : 'Use a numeric Telegram chat ID. Groups must already be known to this agent.'}>
      <input class="input mono" name="target" required maxLength={20} spellcheck={false} autocomplete="off" />
    </Field>
    <Field label="Reason (optional)"><textarea class="input" name="reason" rows={2} maxLength={500} placeholder="e.g. Repeated spam" /></Field>
    <Field label="Expires (optional)" hint="Local time. Leave blank for a permanent block."><input class="input" type="datetime-local" name="expiry" /></Field>
    <Show when={kind() === 'chat'}><p class="field-hint">A permanent group block may make the bot leave the chat. Unblocking won’t rejoin it.</p></Show>
  </FormDialog>;
}
