import { For, Match, Show, Switch, createMemo, createSignal } from 'solid-js';
import { ArrowUpRight, FileText, Laptop, Pause, Play, Plus, ShieldCheck, Terminal, Trash2 } from 'lucide-solid';
import { api, createPoll, errorMessage } from '../api';
import { ago, dateTime } from '../format';
import { useConsole } from '../state';
import type { NodeAuditEvent, NodeGrant, NodeRecord, NodeStatus } from '../types';
import {
  CopyButton,
  Dialog,
  Empty,
  Field,
  FormDialog,
  PageHeader,
  Pill,
  Problem,
  SearchField,
  Section,
  Segmented,
  Skeleton,
  StatStrip,
  Tabs,
  formText,
} from '../ui/primitives';
import type { Tone } from '../ui/primitives';

type Filter = 'all' | NodeStatus;
type DetailTab = 'grants' | 'audit';

const statusMeta: Record<NodeStatus, { label: string; tone: Tone }> = {
  online: { label: 'Online', tone: 'ok' },
  offline: { label: 'Offline', tone: 'muted' },
  paused: { label: 'Paused', tone: 'warn' },
  revoked: { label: 'Revoked', tone: 'bad' },
};

const capMeta: Record<string, { label: string; tone: Tone; icon: typeof Terminal }> = {
  exec: { label: 'exec', tone: 'neutral', icon: Terminal },
  exit: { label: 'exit', tone: 'info', icon: ArrowUpRight },
  files: { label: 'files', tone: 'info', icon: FileText },
};

function CapabilityPills(props: { caps: string[] }) {
  return <For each={props.caps}>{cap => {
    const meta = () => capMeta[cap] ?? { label: cap, tone: 'neutral' as Tone, icon: Terminal };
    const Icon = meta().icon;
    return <Pill tone={meta().tone}><Icon size={11} /> {meta().label}</Pill>;
  }}</For>;
}

function outcomeTone(outcome: string): Tone {
  switch (outcome) {
    case 'ok':
      return 'ok';
    case 'denied':
    case 'failed':
      return 'bad';
    case 'timed_out':
    case 'cancelled':
      return 'warn';
    default:
      return 'neutral';
  }
}

function pairingOrigin(): string {
  return typeof window !== 'undefined' && window.location?.origin
    ? window.location.origin
    : 'https://coordinator.example.com';
}

export function NodesPage(props: { selectedId?: string }) {
  const nodes = createPoll(signal => api<{ nodes: NodeRecord[] }>('/v1/nodes', { signal }), { interval: 5000 });
  const [filter, setFilter] = createSignal<Filter>('all');
  const [query, setQuery] = createSignal('');
  const [selected, setSelected] = createSignal<string | undefined>(props.selectedId);
  const [pairOpen, setPairOpen] = createSignal(false);

  const list = createMemo(() => nodes.data()?.nodes ?? []);
  const counts = createMemo(() => {
    const items = list();
    return {
      total: items.length,
      online: items.filter(n => n.status === 'online').length,
      paused: items.filter(n => n.status === 'paused').length,
      offline: items.filter(n => n.status === 'offline').length,
      revoked: items.filter(n => n.status === 'revoked').length,
    };
  });

  const shown = createMemo(() => {
    const tokens = query().toLowerCase().trim().split(/\s+/).filter(Boolean);
    return list().filter(node =>
      (filter() === 'all' || node.status === filter())
      && tokens.every(token =>
        [node.name, node.id, node.hostname, node.os, node.arch, node.owner_id, ...node.capabilities]
          .join(' ')
          .toLowerCase()
          .includes(token)));
  });

  const currentId = createMemo(() => {
    const chosen = selected() ?? props.selectedId;
    if (chosen && shown().some(n => n.id === chosen)) return chosen;
    return shown()[0]?.id;
  });
  const currentNode = createMemo(() => shown().find(n => n.id === currentId()));

  const pairCmd = () => `anyagents node pair --url ${pairingOrigin()}`;

  return <div class="page knowledge-page">
    <PageHeader
      title="Nodes"
      description="Remote user devices and edge nodes connected over outbound TLS WebSocket with explicit per-agent grants."
      actions={<button type="button" class="btn primary" onClick={() => setPairOpen(true)}><Plus size={14} />Pair node</button>}
    />

    <StatStrip label="Node summary" items={[
      { label: 'Total nodes', value: nodes.loading() ? '—' : counts().total },
      { label: 'Online', value: nodes.loading() ? '—' : counts().online, tone: counts().online ? 'ok' : undefined },
      { label: 'Paused', value: nodes.loading() ? '—' : counts().paused, tone: counts().paused ? 'warn' : undefined },
      { label: 'Offline', value: nodes.loading() ? '—' : counts().offline },
    ]} />

    <Show when={nodes.error()}><Problem retry={nodes.refresh}>{nodes.error()}</Problem></Show>
    <Show when={nodes.loading()}><Skeleton rows={6} /></Show>

    <Show when={nodes.data() && list().length === 0}>
      <Empty icon={<Laptop size={20} />} title="No nodes paired yet"
        action={<button type="button" class="btn primary" onClick={() => setPairOpen(true)}><Plus size={14} />Pair a node</button>}>
        <p>Pair a personal workstation or edge machine with one command — no inbound ports or VPN required:</p>
        <pre class="code"><code>{pairCmd()}{'\n'}anyagents node run</code></pre>
      </Empty>
    </Show>

    <Show when={list().length > 0}>
      <div class="toolbar">
        <Segmented label="Node status filter" value={filter()} onChange={setFilter} options={[
          { value: 'all', label: 'All', count: counts().total },
          { value: 'online', label: 'Online', count: counts().online, tone: 'ok' },
          { value: 'paused', label: 'Paused', count: counts().paused, tone: 'warn' },
          { value: 'offline', label: 'Offline', count: counts().offline },
          { value: 'revoked', label: 'Revoked', count: counts().revoked, tone: 'bad' },
        ]} />
        <SearchField value={query()} onChange={setQuery} placeholder="Search nodes, hosts, OS, capabilities…" label="Search nodes" />
      </div>

      <div class="split knowledge-split">
        <div class="list-pane">
          <div class="doc-list">
            <Show when={shown().length === 0}><Empty compact title="No matching nodes" /></Show>
            <For each={shown()}>{node => {
              const active = () => currentId() === node.id;
              const meta = () => statusMeta[node.status] ?? { label: node.status, tone: 'neutral' as Tone };
              return <button type="button" class={'doc-item' + (active() ? ' selected' : '')} aria-current={active() || undefined}
                onClick={() => setSelected(node.id)}>
                <span class="doc-title">
                  <span>{node.name}</span>
                  <Pill tone={meta().tone} dot>{meta().label}</Pill>
                </span>
                <span class="doc-meta">
                  <span class="mono truncate">{node.os}/{node.arch} · {node.hostname}</span>
                  <span><CapabilityPills caps={node.capabilities} /></span>
                  <span class="num">{node.last_heartbeat_at_ms ? ago(node.last_heartbeat_at_ms) : 'never'}</span>
                </span>
              </button>;
            }}</For>
          </div>
        </div>
        <div class="detail-pane">
          <Show when={currentNode()} keyed fallback={<Empty title="Select a node" />}>
            {node => <NodeDetail node={node} refresh={nodes.refresh} />}
          </Show>
        </div>
      </div>
    </Show>

    <Show when={pairOpen()}>
      <Dialog title="Pair a remote node" description="Run these commands on your workstation or edge device to pair via OIDC Device Flow and start the outbound tunnel." close={() => setPairOpen(false)}>
        <div class="dialog-body">
          <Field label="1. Pair device (mints revocable ndt_... token at 0600)">
            <div class="flex-row">
              <pre class="code"><code>{pairCmd()}</code></pre>
              <CopyButton value={pairCmd()} label="Copy pair command" text />
            </div>
          </Field>
          <Field label="2. Connect outbound WebSocket reverse-tunnel">
            <div class="flex-row">
              <pre class="code"><code>anyagents node run</code></pre>
              <CopyButton value="anyagents node run" label="Copy run command" text />
            </div>
          </Field>
          <p class="field-hint">Only devices with explicit per-agent grants are visible to agents. You can pause or revoke any node immediately from this view.</p>
        </div>
        <footer class="dialog-foot">
          <button type="button" class="btn primary" onClick={() => setPairOpen(false)}>Done</button>
        </footer>
      </Dialog>
    </Show>
  </div>;
}

function NodeDetail(props: { node: NodeRecord; refresh: () => void }) {
  const [tab, setTab] = createSignal<DetailTab>('grants');
  const [busy, setBusy] = createSignal<string>();
  const [actionError, setActionError] = createSignal('');
  const [revokeConfirm, setRevokeConfirm] = createSignal(false);

  const meta = () => statusMeta[props.node.status] ?? { label: props.node.status, tone: 'neutral' as Tone };

  async function runControl(action: 'pause' | 'resume' | 'revoke') {
    setBusy(action);
    setActionError('');
    try {
      await api(`/v1/nodes/${encodeURIComponent(props.node.id)}/${action}`, { method: 'POST', body: {} });
      props.refresh();
    } catch (cause) {
      setActionError(errorMessage(cause));
    } finally {
      setBusy(undefined);
    }
  }

  return <article class="doc">
    <header class="doc-head">
      <div class="doc-path mono subtle">{props.node.id} · {props.node.hostname}</div>
      <div class="page-header">
        <div class="page-heading">
          <h2>{props.node.name} <Pill tone={meta().tone} dot>{meta().label}</Pill></h2>
          <p class="subtle">
            <span class="mono">{props.node.os}/{props.node.arch}</span> · Owner <span class="mono">{props.node.owner_id}</span>
            {' · '}<CapabilityPills caps={props.node.capabilities} />
          </p>
          <div class="doc-byline subtle">
            Paired <span class="num">{dateTime(props.node.created_at_ms)}</span>
            {' · '}Last heartbeat <span class="num">{props.node.last_heartbeat_at_ms ? ago(props.node.last_heartbeat_at_ms) : 'never'}</span>
          </div>
        </div>
        <Show when={props.node.status !== 'revoked'}>
          <div class="page-actions">
            <Show when={props.node.status === 'paused'} fallback={
              <button type="button" class="btn sm danger-soft" disabled={Boolean(busy())} onClick={() => void runControl('pause')}>
                <Pause size={13} />{busy() === 'pause' ? 'Pausing…' : 'Pause'}
              </button>
            }>
              <button type="button" class="btn sm primary" disabled={Boolean(busy())} onClick={() => void runControl('resume')}>
                <Play size={13} />{busy() === 'resume' ? 'Resuming…' : 'Resume'}
              </button>
            </Show>
            <button type="button" class="btn sm danger" disabled={Boolean(busy())} onClick={() => setRevokeConfirm(true)}>
              <Trash2 size={13} />Revoke
            </button>
          </div>
        </Show>
      </div>
    </header>

    <Show when={actionError()}><Problem>{actionError()}</Problem></Show>

    <Tabs label="Node sections" value={tab()} onChange={setTab} tabs={[
      { id: 'grants', label: 'Capability Grants' },
      { id: 'audit', label: 'Audit Log' },
    ]} />

    <div class="tab-panel">
      <Switch>
        <Match when={tab() === 'grants'}><NodeGrantsTab node={props.node} /></Match>
        <Match when={tab() === 'audit'}><NodeAuditTab nodeId={props.node.id} /></Match>
      </Switch>
    </div>

    <Show when={revokeConfirm()}>
      <FormDialog
        title={`Revoke node "${props.node.name}"?`}
        description="Revoking permanently invalidates this node's bearer token, closes its tunnel, kills any running processes, and revokes all agent grants."
        action="Revoke node"
        danger
        close={() => setRevokeConfirm(false)}
        submit={async () => { await runControl('revoke'); }}
      />
    </Show>
  </article>;
}

function NodeGrantsTab(props: { node: NodeRecord }) {
  const { fleet } = useConsole();
  const agentNames = createMemo(() => new Map(fleet().agents.map(agent => [agent.id, agent.name])));
  const grants = createPoll(
    signal => api<{ grants: NodeGrant[] }>(`/v1/nodes/${encodeURIComponent(props.node.id)}/grants`, { signal }),
    { interval: 10000, key: () => props.node.id },
  );
  const [grantOpen, setGrantOpen] = createSignal(false);
  const [error, setError] = createSignal('');

  return <Section title="Authorized agents" actions={
    <Show when={props.node.status !== 'revoked'}>
      <button type="button" class="btn sm primary" onClick={() => setGrantOpen(true)}><Plus size={14} />Grant Access</button>
    </Show>
  }>
    <Switch>
      <Match when={grants.error()}><Problem retry={grants.refresh}>{grants.error()}</Problem></Match>
      <Match when={!grants.data()}><Skeleton rows={3} /></Match>
      <Match when={!grants.data()!.grants.length}>
        <Empty compact icon={<ShieldCheck size={18} />} title="No agent grants">
          No agent may access this node until you grant an explicit capability (fail-closed by default).
        </Empty>
      </Match>
      <Match when={grants.data()}>{data => <>
        <Show when={error()}><Problem>{error()}</Problem></Show>
        <div class="table-wrap"><table class="table">
          <thead><tr><th>Agent</th><th>Capability</th><th>Granted by</th><th>Granted</th><th class="right"><span class="sr-only">Actions</span></th></tr></thead>
          <tbody><For each={data().grants}>{grant => <tr>
            <td>
              <div class="strong">{grant.agent_name || agentNames().get(grant.agent_id) || grant.agent_id}</div>
              <div class="mono subtle small">{grant.agent_id}</div>
            </td>
            <td><Pill tone="info">{grant.capability}</Pill></td>
            <td class="mono subtle">{grant.granted_by}</td>
            <td class="num subtle">{dateTime(grant.created_at_ms)}</td>
            <td class="right">
              <button type="button" class="btn sm ghost" onClick={async () => {
                setError('');
                try {
                  await api(`/v1/nodes/${encodeURIComponent(props.node.id)}/grants/${encodeURIComponent(grant.id)}`, { method: 'DELETE' });
                  grants.refresh();
                } catch (cause) { setError(errorMessage(cause)); }
              }}>Revoke</button>
            </td>
          </tr>}</For></tbody>
        </table></div>
      </>}</Match>
    </Switch>

    <Show when={grantOpen()}>
      <GrantNodeAccessDialog
        node={props.node}
        close={() => setGrantOpen(false)}
        refresh={grants.refresh}
      />
    </Show>
  </Section>;
}

function GrantNodeAccessDialog(props: { node: NodeRecord; close: () => void; refresh: () => void }) {
  const { fleet } = useConsole();
  const [agentChoice, setAgentChoice] = createSignal('');
  const live = createMemo(() => [...fleet().live].sort((a, b) => a.name.localeCompare(b.name)));
  const capOptions = createMemo(() => props.node.capabilities.length ? props.node.capabilities : ['exec']);

  return <FormDialog
    title={`Grant capability on "${props.node.name}"`}
    description="Agents require an explicit grant to invoke capabilities on this node."
    action="Grant Access"
    close={props.close}
    submit={async form => {
      const choice = formText(form, 'agent');
      const agent_id = choice === 'manual' ? formText(form, 'agent_id') : choice;
      if (!/^agt_\S+$/.test(agent_id)) throw new Error('Choose an agent or enter a valid agent ID (agt_…).');
      const capability = formText(form, 'capability') || 'exec';
      await api(`/v1/nodes/${encodeURIComponent(props.node.id)}/grants`, {
        method: 'POST',
        body: { agent_id, capability },
      });
      props.refresh();
    }}
  >
    <Field label="Agent">
      <select class="input" name="agent" required value={agentChoice()} onChange={e => setAgentChoice(e.currentTarget.value)}>
        <option value="" disabled>Select an agent</option>
        <For each={live()}>{agent => <option value={agent.id}>{agent.name} · {agent.id}</option>}</For>
        <option value="manual">Enter an agent ID manually</option>
      </select>
    </Field>
    <Show when={agentChoice() === 'manual'}>
      <Field label="Agent ID">
        <input class="input mono" name="agent_id" placeholder="agt_…" required autocomplete="off" spellcheck={false} />
      </Field>
    </Show>
    <Field label="Capability" hint="Available capabilities: exec (remote command), exit (egress proxy), files (scoped file access).">
      <select class="input mono" name="capability" value={capOptions()[0]}>
        <For each={capOptions()}>{cap => <option value={cap}>{cap}</option>}</For>
      </select>
    </Field>
  </FormDialog>;
}

function NodeAuditTab(props: { nodeId: string }) {
  const audit = createPoll(
    signal => api<{ events: NodeAuditEvent[] }>(`/v1/nodes/${encodeURIComponent(props.nodeId)}/audit?limit=50`, { signal }),
    { interval: 10000, key: () => props.nodeId },
  );

  return <Section title="Append-only audit events">
    <Switch>
      <Match when={audit.error()}><Problem retry={audit.refresh}>{audit.error()}</Problem></Match>
      <Match when={!audit.data()}><Skeleton rows={4} /></Match>
      <Match when={!audit.data()!.events.length}>
        <Empty compact icon={<Terminal size={18} />} title="No audit events yet">
          Pairing, pause/resume/revoke, grants, and command executions are recorded immutably here.
        </Empty>
      </Match>
      <Match when={audit.data()}>{data => <div class="table-wrap"><table class="table">
        <thead><tr><th>Time</th><th>Action</th><th>Actor / Agent</th><th>Summary</th><th>Exit</th><th>Outcome</th></tr></thead>
        <tbody><For each={data().events}>{ev => <tr>
          <td class="num subtle nowrap">{dateTime(ev.started_at_ms)}</td>
          <td class="mono">{ev.action} <span class="subtle">({ev.capability})</span></td>
          <td class="mono subtle">{ev.agent_id || 'operator'}</td>
          <td class="mono cell-wrap">{ev.summary}</td>
          <td class="num">{ev.exit_code != null ? ev.exit_code : '—'}</td>
          <td><Pill tone={outcomeTone(ev.outcome)}>{ev.outcome}</Pill></td>
        </tr>}</For></tbody>
      </table></div>}</Match>
    </Switch>
  </Section>;
}
