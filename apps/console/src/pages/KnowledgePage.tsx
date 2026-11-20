import { For, Match, Show, Switch, createMemo, createSignal } from 'solid-js';
import { BookOpen, FileText, History, Pin, Plus, ShieldCheck } from 'lucide-solid';
import { HttpError, api, createPoll, errorMessage } from '../api';
import { ago, dateTime } from '../format';
import { Empty, Field, FormDialog, PageHeader, Pill, Problem, SearchField, Section, Skeleton, Tabs, formText } from '../ui/primitives';
import { Markdown } from '../ui/Markdown';
import { GrantAccessDialog } from './GrantAccessDialog';

interface Summary { namespace: string; topic: string; title: string; description: string; pinned: boolean; version: number }
interface Doc extends Summary { id: string; body: string; updated_by: string; created_at_ms: number; updated_at_ms: number }
interface Revision { id: string; version: number; actor_id: string; reason: string; created_at_ms: number; deleted_at_ms?: number | null }
interface Proposal { id: string; namespace: string; topic: string; actor_id: string; expected_version: number; created_at_ms: number; status: string;
  edit: { title?: string; body?: string; reason: string } }
interface Grant { id: string; agent_id: string; namespace_pattern: string; mode: string; granted_by: string; granted_at_ms: number }

type Unavailable = { unavailable: 'missing' | 'forbidden'; message: string };
const knowledge = <T,>(body: object, signal?: AbortSignal) => api<T>('/v1/knowledge', { method: 'POST', body, signal });

async function loadIndex(signal: AbortSignal): Promise<Summary[] | Unavailable> {
  try {
    const items: Summary[] = [];
    let offset: number | null = 0;
    for (let page = 0; page < 20 && offset != null; page++) {
      const result: { items: Summary[]; next_offset: number | null } = await knowledge({ action: 'list', scope: 'all', limit: 50, offset }, signal);
      items.push(...result.items);
      offset = result.next_offset;
    }
    return items;
  } catch (error) {
    if (error instanceof HttpError && [404, 405, 501].includes(error.status)) return { unavailable: 'missing', message: error.message };
    if (error instanceof HttpError && [401, 403].includes(error.status)) return { unavailable: 'forbidden', message: error.message };
    throw error;
  }
}

type Tab = 'documents' | 'proposals' | 'grants';
export function KnowledgePage() {
  const index = createPoll(loadIndex, { interval: 30000 });
  const [tab, setTab] = createSignal<Tab>('documents');
  const unavailable = () => { const data = index.data(); return data && !Array.isArray(data) ? data : undefined; };
  const docs = createMemo(() => { const data = index.data(); return Array.isArray(data) ? data : []; });
  const namespaces = createMemo(() => [...new Set(docs().map(doc => doc.namespace))].sort());

  return <div class="page knowledge-page">
    <PageHeader title="Knowledge" description="The organization brain: versioned documents agents read, with full revision history and review." />
    <Show when={index.error()}><Problem retry={index.refresh}>{index.error()}</Problem></Show>
    <Show when={index.loading()}><Skeleton rows={8} /></Show>
    <Show when={unavailable()}>{state => <Empty icon={state().unavailable === 'forbidden' ? <ShieldCheck size={20} /> : <BookOpen size={20} />}
      title={state().unavailable === 'forbidden' ? 'Sign in as the deployment owner' : 'Knowledge is not enabled on this coordinator'}>
      {state().unavailable === 'forbidden'
        ? 'Knowledge is readable by the OIDC deployment owner and by agents with a namespace grant. Operator tokens do not confer access.'
        : 'This coordinator does not serve the knowledge API yet. It appears here automatically after the coordinator is upgraded.'}
    </Empty>}</Show>
    <Show when={Array.isArray(index.data())}>
      <Tabs label="Knowledge sections" value={tab()} onChange={setTab} tabs={[
        { id: 'documents', label: 'Documents', count: docs().length }, { id: 'proposals', label: 'Proposals' }, { id: 'grants', label: 'Grants' }]} />
      <div class="tab-panel">
        <Switch>
          <Match when={tab() === 'documents'}><Documents docs={docs()} namespaces={namespaces()} refresh={index.refresh} /></Match>
          <Match when={tab() === 'proposals'}><Proposals namespaces={namespaces()} refresh={index.refresh} /></Match>
          <Match when={tab() === 'grants'}><Grants /></Match>
        </Switch>
      </div>
    </Show>
  </div>;
}

function Documents(props: { docs: Summary[]; namespaces: string[]; refresh: () => void }) {
  const [namespace, setNamespace] = createSignal('all');
  const [query, setQuery] = createSignal('');
  const [selected, setSelected] = createSignal<{ namespace: string; topic: string }>();
  const shown = createMemo(() => {
    const tokens = query().toLowerCase().trim().split(/\s+/).filter(Boolean);
    return props.docs.filter(doc => (namespace() === 'all' || doc.namespace === namespace())
      && tokens.every(token => [doc.title, doc.topic, doc.namespace, doc.description].join(' ').toLowerCase().includes(token)));
  });
  const grouped = createMemo(() => props.namespaces.map(ns => ({ ns, list: shown().filter(doc => doc.namespace === ns) })).filter(entry => entry.list.length));
  const current = createMemo(() => selected() ?? (shown()[0] && { namespace: shown()[0].namespace, topic: shown()[0].topic }));
  const currentKey = createMemo(() => current() ? current()!.namespace + '\u0000' + current()!.topic : undefined);
  return <Show when={props.docs.length} fallback={<Empty icon={<FileText size={20} />} title="No documents yet">Agents and owners create documents with the <code>memory</code> tool or the knowledge CLI.</Empty>}>
    <div class="split knowledge-split">
      <div class="list-pane">
        <div class="list-tools">
          <select class="input" onChange={event => setNamespace(event.currentTarget.value)} aria-label="Namespace">
            <option value="all" selected={namespace() === 'all'}>All namespaces</option>
            <For each={props.namespaces}>{ns => <option value={ns} selected={ns === namespace()}>{ns}</option>}</For></select>
          <SearchField value={query()} onChange={setQuery} placeholder="Search documents…" />
        </div>
        <div class="doc-list">
          <Show when={grouped().length === 0}><Empty compact title="No matching documents" /></Show>
          <For each={grouped()}>{entry => <div class="doc-group">
            <div class="doc-group-head mono">{entry.ns}<span class="num subtle">{entry.list.length}</span></div>
            <For each={entry.list}>{doc => {
              const active = () => current()?.namespace === doc.namespace && current()?.topic === doc.topic;
              return <button type="button" class={'doc-item' + (active() ? ' selected' : '')} aria-current={active() || undefined}
                onClick={() => setSelected({ namespace: doc.namespace, topic: doc.topic })}>
                <span class="doc-title"><Show when={doc.pinned}><Pin size={12} aria-label="Pinned" /></Show>{doc.title || doc.topic}</span>
                <span class="doc-meta"><span class="mono truncate">{doc.topic}</span><span class="version num">v{doc.version}</span></span>
              </button>;
            }}</For>
          </div>}</For>
        </div>
      </div>
      <div class="detail-pane"><Show when={currentKey()} keyed fallback={<Empty title="Select a document" />}>{key => {
        const [ns, topic] = key.split('\u0000');
        return <DocView namespace={ns} topic={topic} refresh={props.refresh} />;
      }}</Show></div>
    </div>
  </Show>;
}

function DocView(props: { namespace: string; topic: string; refresh: () => void }) {
  const doc = createPoll(signal => knowledge<Doc>({ action: 'read', namespace: props.namespace, topic: props.topic }, signal));
  const history = createPoll(signal => knowledge<{ revisions: Revision[] }>({ action: 'revisions', namespace: props.namespace, topic: props.topic, limit: 25 }, signal));
  const [tab, setTab] = createSignal<'content' | 'history'>('content');
  const [rollback, setRollback] = createSignal<Revision>();
  return <Switch>
    <Match when={doc.error()}><Problem retry={doc.refresh}>{doc.error()}</Problem></Match>
    <Match when={!doc.data()}><Skeleton rows={10} /></Match>
    <Match when={doc.data()}>{current => <article class="doc">
      <header class="doc-head">
        <div class="doc-path mono subtle">{current().namespace} / {current().topic}</div>
        <h2>{current().title || current().topic}<Show when={current().pinned}><Pill tone="info">Pinned</Pill></Show><Pill>v{current().version}</Pill></h2>
        <Show when={current().description}><p class="subtle">{current().description}</p></Show>
        <div class="doc-byline subtle">Updated <span class="num">{ago(current().updated_at_ms)}</span> by <span class="mono">{current().updated_by}</span></div>
      </header>
      <Tabs label="Document" value={tab()} onChange={setTab} tabs={[{ id: 'content', label: 'Content' }, { id: 'history', label: 'History', count: history.data()?.revisions.length }]} />
      <Show when={tab() === 'content'}><div class="doc-body"><Show when={current().body.trim()} fallback={<p class="subtle">This document is empty.</p>}><Markdown source={current().body} /></Show></div></Show>
      <Show when={tab() === 'history'}><div class="doc-body">
        <Show when={history.error()}><Problem retry={history.refresh}>{history.error()}</Problem></Show>
        <Show when={history.loading()}><Skeleton rows={4} /></Show>
        <Show when={history.data()}>{data => <ol class="timeline"><For each={data().revisions}>{revision => <li class={revision.version === current().version ? 'current' : undefined}>
          <span class="timeline-dot" aria-hidden="true" />
          <div class="timeline-main">
            <div class="timeline-head"><Pill tone={revision.version === current().version ? 'ok' : 'neutral'}>v{revision.version}</Pill>
              <span class="mono">{revision.actor_id}</span><span class="subtle num">{dateTime(revision.created_at_ms)}</span>
              <Show when={revision.deleted_at_ms}><Pill tone="warn">Deleted</Pill></Show></div>
            <div class="timeline-reason">{revision.reason || <span class="subtle">No reason recorded</span>}</div>
          </div>
          <Show when={revision.version !== current().version}><button type="button" class="btn sm ghost" onClick={() => setRollback(revision)}><History size={13} />Roll back</button></Show>
        </li>}</For></ol>}</Show>
      </div></Show>
      <Show when={rollback()} keyed>{revision => <FormDialog title={'Roll back to v' + revision.version + '?'} description="Rollback appends a new revision with the earlier content; history is never rewritten."
        action="Roll back" close={() => setRollback(undefined)} submit={async form => {
          await knowledge({ action: 'rollback', namespace: props.namespace, topic: props.topic, to_version: revision.version, expected_version: current().version, reason: formText(form, 'reason') || 'Rolled back from the console' });
          doc.refresh(); history.refresh(); props.refresh();
        }}><Field label="Reason"><input class="input" name="reason" placeholder="Why roll back?" autofocus /></Field></FormDialog>}</Show>
    </article>}</Match>
  </Switch>;
}

function Proposals(props: { namespaces: string[]; refresh: () => void }) {
  const proposals = createPoll(async signal => {
    const pages = await Promise.all(props.namespaces.map(namespace => knowledge<{ proposals: Proposal[] }>({ action: 'proposals', namespace, limit: 25 }, signal).catch(() => ({ proposals: [] as Proposal[] }))));
    return pages.flatMap(page => page.proposals);
  }, { key: () => props.namespaces.join(',') });
  const [error, setError] = createSignal('');
  const pending = () => (proposals.data() ?? []).filter(proposal => proposal.status === 'pending');
  async function review(proposal: Proposal, promote: boolean) {
    setError('');
    try {
      await knowledge(promote ? { action: 'promote', namespace: proposal.namespace, proposal_id: proposal.id, expected_version: proposal.expected_version }
        : { action: 'reject', namespace: proposal.namespace, proposal_id: proposal.id });
      proposals.refresh(); props.refresh();
    } catch (cause) { setError(errorMessage(cause)); }
  }
  return <Switch>
    <Match when={proposals.error()}><Problem retry={proposals.refresh}>{proposals.error()}</Problem></Match>
    <Match when={!proposals.data()}><Skeleton rows={4} /></Match>
    <Match when={!pending().length}><Empty icon={<FileText size={20} />} title="No pending proposals">Edits agents propose for review appear here.</Empty></Match>
    <Match when={pending().length}>
      <Show when={error()}><Problem>{error()}</Problem></Show>
      <div class="table-wrap"><table class="table">
        <thead><tr><th>Document</th><th>Proposed by</th><th>Reason</th><th>Base</th><th>Submitted</th><th class="right"><span class="sr-only">Actions</span></th></tr></thead>
        <tbody><For each={pending()}>{proposal => <tr>
          <td><div class="strong">{proposal.edit.title || proposal.topic}</div><div class="mono subtle small">{proposal.namespace} / {proposal.topic}</div></td>
          <td class="mono">{proposal.actor_id}</td>
          <td class="cell-wrap">{proposal.edit.reason}</td>
          <td class="num">v{proposal.expected_version}</td>
          <td class="num subtle">{ago(proposal.created_at_ms)}</td>
          <td class="right nowrap"><button type="button" class="btn sm ghost" onClick={() => void review(proposal, false)}>Reject</button>
            <button type="button" class="btn sm primary" onClick={() => void review(proposal, true)}>Promote</button></td>
        </tr>}</For></tbody></table></div>
    </Match>
  </Switch>;
}

function Grants() {
  const grants = createPoll(signal => knowledge<{ grants: Grant[] }>({ action: 'grants', limit: 100 }, signal));
  const [error, setError] = createSignal('');
  const [grantOpen, setGrantOpen] = createSignal(false);
  return <Section title="Namespace grants" actions={<Show when={grants.data() && !grants.error()}>
    <button class="btn sm primary" type="button" onClick={() => setGrantOpen(true)}><Plus size={14} aria-hidden="true" />Grant Access</button>
  </Show>}>
  <Switch>
    <Match when={grants.error()}><Problem retry={grants.refresh}>{grants.error()}</Problem></Match>
    <Match when={!grants.data()}><Skeleton rows={4} /></Match>
    <Match when={!grants.data()!.grants.length}><Empty icon={<ShieldCheck size={20} />} title="No namespace grants">Agents can always read their own namespace. Grant access to share other namespaces.</Empty></Match>
    <Match when={grants.data()}>{data => <>
      <Show when={error()}><Problem>{error()}</Problem></Show>
      <div class="table-wrap"><table class="table">
        <thead><tr><th>Agent</th><th>Namespace pattern</th><th>Mode</th><th>Granted by</th><th>Granted</th><th class="right"><span class="sr-only">Actions</span></th></tr></thead>
        <tbody><For each={data().grants}>{grant => <tr>
          <td class="mono">{grant.agent_id}</td><td class="mono strong">{grant.namespace_pattern}</td>
          <td><Pill tone={grant.mode === 'write' ? 'warn' : 'neutral'}>{grant.mode}</Pill></td>
          <td class="mono subtle">{grant.granted_by}</td><td class="num subtle">{dateTime(grant.granted_at_ms)}</td>
          <td class="right"><button type="button" class="btn sm ghost" onClick={async () => {
            setError('');
            try { await knowledge({ action: 'revoke', grant_id: grant.id }); grants.refresh(); } catch (cause) { setError(errorMessage(cause)); }
          }}>Revoke</button></td>
        </tr>}</For></tbody></table></div>
    </>}</Match>
  </Switch>
  <Show when={grantOpen()}><GrantAccessDialog close={() => setGrantOpen(false)} refresh={grants.refresh}
    grant={(agent_id, namespace_pattern, mode) => knowledge({ action: 'grant', agent_id, namespace_pattern, mode })} /></Show>
  </Section>;
}
