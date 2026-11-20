import { For, Show, createMemo, createSignal } from 'solid-js';
import { ArrowUpCircle, PackageCheck, RefreshCw } from 'lucide-solid';
import { api, errorMessage } from '../api';
import { shortDigest } from '../format';
import { agentHref } from '../route';
import { useConsole } from '../state';
import type { DrainAction, UpdatePolicy, UpgradeStatus } from '../types';
import { CopyButton, Empty, FormDialog, PageHeader, Pill, Problem, Section, Segmented, Skeleton, StatStrip } from '../ui/primitives';
import type { Tone } from '../ui/primitives';

const STATUS: Record<UpgradeStatus, { label: string; tone: Tone }> = {
  up_to_date: { label: 'Up to date', tone: 'ok' },
  outdated_idle: { label: 'Outdated', tone: 'warn' },
  outdated_draining: { label: 'Draining after turn', tone: 'info' },
  cold_uses_latest: { label: 'Cold → latest', tone: 'neutral' },
};
const ORDER: Record<UpgradeStatus, number> = { outdated_idle: 0, outdated_draining: 1, cold_uses_latest: 2, up_to_date: 3 };
const DRAIN_RESULT: Record<DrainAction, string> = {
  recycled_now: 'was recycled now and wakes on the desired image.',
  drain_scheduled: 'will drain when its current turn ends.',
  already_up_to_date: 'is already up to date.',
  cold_next_turn_latest: 'has no live lease; its next turn starts on the latest image.',
};
const POLICY_NOTE: Record<UpdatePolicy, string> = {
  auto: 'Auto — the coordinator prefers rolling agents onto new images as they become available.',
  manual: 'Manual — agents keep their current runtime until you drain them.',
  pinned: 'Pinned — keep the current versions until the policy changes.',
};
type Filter = 'all' | UpgradeStatus;

export function VersionsPage() {
  const { versions, refreshAll } = useConsole();
  const [filter, setFilter] = createSignal<Filter>('all');
  const [drain, setDrain] = createSignal<string>();
  const [notice, setNotice] = createSignal('');
  const [policyBusy, setPolicyBusy] = createSignal(false);
  const [error, setError] = createSignal('');
  const agents = () => versions.data()?.agents ?? [];
  const count = (status: UpgradeStatus) => agents().filter(agent => agent.upgrade_status === status).length;
  const summary = () => versions.data()?.summary ?? {
    total: agents().length, up_to_date: count('up_to_date'), outdated: count('outdated_idle') + count('outdated_draining'),
    draining: count('outdated_draining'), cold: count('cold_uses_latest'),
  };
  const idle = () => summary().outdated - summary().draining;
  const shown = createMemo(() => agents().filter(agent => filter() === 'all' || agent.upgrade_status === filter())
    .sort((a, b) => (ORDER[a.upgrade_status] ?? 9) - (ORDER[b.upgrade_status] ?? 9) || a.name.localeCompare(b.name)));
  const policy = () => versions.data()?.coordinator.auto_update_policy ?? 'manual';

  async function setPolicy(next: UpdatePolicy) {
    if (next === policy() || policyBusy()) return;
    setPolicyBusy(true); setError('');
    try { await api('/admin/api/versions/policy', { method: 'PATCH', body: { auto_update_policy: next } }); versions.refresh(); }
    catch (cause) { setError(errorMessage(cause)); } finally { setPolicyBusy(false); }
  }

  return <div class="page">
    <PageHeader title="Versions & Updates" description="What the coordinator and each agent sandbox are running, and how upgrades roll out at turn boundaries."
      actions={<button type="button" class="btn" onClick={versions.refresh}><RefreshCw size={14} />Refresh</button>} />
    <Show when={versions.error()}><Problem retry={versions.refresh}>{versions.error()}</Problem></Show>
    <Show when={versions.loading()}><Skeleton rows={8} /></Show>
    <Show when={versions.data() === null}><Empty icon={<PackageCheck size={20} />} title="Version reporting is not available yet">
      This coordinator does not serve <code>/admin/api/versions</code>. Image digests, update policy, and drain controls appear here automatically after it is upgraded.</Empty></Show>
    <Show when={versions.data()}>{report => <>
      <Show when={error()}><Problem>{error()}</Problem></Show>
      <Show when={notice()}><p class="tone-ok small notice-line" role="status">{notice()}</p></Show>
      <section class="panel version-card" aria-label="Coordinator and runtime">
        <div class="version-card-head">
          <div><div class="eyebrow">Coordinator</div>
            <div class="version-title"><span class="num">{report().coordinator.version ?? 'Unknown version'}</span>
              <Show when={report().coordinator.latest_available}>{latest => latest() !== report().coordinator.version
                ? <Pill tone="warn" dot>{latest()} available</Pill> : <Pill tone="ok" dot>Latest</Pill>}</Show></div></div>
          <div class="policy"><span class="field-label">Update policy</span>
            <Segmented label="Update policy" value={policy()} onChange={value => void setPolicy(value)} options={[
              { value: 'auto', label: 'Auto' }, { value: 'manual', label: 'Manual' }, { value: 'pinned', label: 'Pinned' }]} />
          </div>
        </div>
        <dl class="kv-grid">
          <Value label="Git SHA" value={report().coordinator.git_sha} short={report().coordinator.git_sha?.slice(0, 10)} />
          <Value label="Coordinator image" value={report().coordinator.image_digest} short={shortDigest(report().coordinator.image_digest)} />
          <Value label="Desired sandbox image" value={report().runtime?.desired_sandbox_image} />
          <Value label="Desired image digest" value={report().runtime?.desired_image_digest} short={shortDigest(report().runtime?.desired_image_digest)} />
          <Value label="Template base" value={report().runtime?.tpl_base_version} />
          <Value label="Guest supervisor" value={report().runtime?.sbx_guest_version} />
          <For each={Object.entries(report().runtime?.desired_harness_versions ?? {}).sort(([a], [b]) => a.localeCompare(b))}>{([harness, version]) =>
            <Value label={'Harness · ' + harness} value={version} />}</For>
        </dl>
        <p class="policy-note subtle">{POLICY_NOTE[policy()]}</p>
      </section>

      <StatStrip label="Fleet freshness" items={[
        { label: 'Up to date', value: summary().up_to_date, tone: summary().up_to_date ? 'ok' : undefined, detail: 'Running the desired image' },
        { label: 'Outdated', value: idle(), tone: idle() ? 'warn' : undefined, detail: idle() ? 'Waiting for a drain' : 'Nothing waiting' },
        { label: 'Draining after turn', value: summary().draining, detail: 'Upgrades when the turn ends' },
        { label: 'Cold → latest', value: summary().cold, detail: 'Next turn starts on latest' },
      ]} />

      <Section title="Agent sandboxes" meta={<span class="num subtle">{summary().total}</span>} actions={<Segmented label="Freshness filter" value={filter()} onChange={setFilter} options={[
        { value: 'all', label: 'All', count: agents().length }, { value: 'outdated_idle', label: 'Outdated', count: count('outdated_idle'), tone: 'warn' },
        { value: 'outdated_draining', label: 'Draining', count: count('outdated_draining') }, { value: 'cold_uses_latest', label: 'Cold', count: count('cold_uses_latest') },
        { value: 'up_to_date', label: 'Current', count: count('up_to_date') }]} />}>
        <Show when={shown().length} fallback={<Empty compact icon={<PackageCheck size={18} />} title={agents().length ? 'No agents in this state' : 'No sandbox leases to report'} />}>
          <div class="table-wrap"><table class="table">
            <thead><tr><th>Agent</th><th>Status</th><th>Running image</th><th>Desired image</th><th>Harness</th><th class="right"><span class="sr-only">Actions</span></th></tr></thead>
            <tbody><For each={shown()}>{agent => {
              const meta = STATUS[agent.upgrade_status] ?? STATUS.cold_uses_latest;
              const differs = agent.running_image_digest && agent.desired_image_digest && agent.running_image_digest !== agent.desired_image_digest;
              return <tr>
                <td><a class="strong" href={agentHref(agent.name)}>{agent.name}</a><Show when={agent.has_open_turn}><span class="subtle small"> · in turn</span></Show></td>
                <td><Pill tone={meta.tone} dot>{meta.label}</Pill></td>
                <td class={'mono' + (differs ? ' tone-warn' : ' subtle')} title={agent.running_image_digest ?? undefined}>{shortDigest(agent.running_image_digest)}</td>
                <td class="mono subtle" title={agent.desired_image_digest ?? undefined}>{shortDigest(agent.desired_image_digest)}</td>
                <td class="mono subtle">{agent.harness ?? '—'}{agent.harness_version ? ' ' + agent.harness_version : ''}</td>
                <td class="right"><Show when={agent.upgrade_status === 'outdated_idle'}
                  fallback={agent.upgrade_status === 'outdated_draining' ? <span class="subtle small">{agent.drain_requested ? 'Drain requested' : 'After current turn'}</span> : null}>
                  <button type="button" class="btn sm" onClick={() => setDrain(agent.name)}><ArrowUpCircle size={13} />Drain & upgrade</button>
                </Show></td>
              </tr>;
            }}</For></tbody></table></div>
        </Show>
      </Section>
    </>}</Show>
    <Show when={drain()} keyed>{name => <FormDialog title={'Drain & upgrade ' + name + '?'} action="Drain & upgrade" close={() => setDrain(undefined)}
      description="An idle sandbox is recycled now; a busy one finishes its current turn first. The attached volume and queued messages are kept."
      submit={async () => {
        const result = await api<{ agent: string; action: DrainAction }>('/admin/api/agents/' + encodeURIComponent(name) + '/drain-upgrade', { method: 'POST' });
        setNotice((result?.agent ?? name) + ' ' + (DRAIN_RESULT[result?.action] ?? 'was asked to upgrade.'));
        versions.refresh(); refreshAll();
      }} />}</Show>
  </div>;
}

function Value(props: { label: string; value?: string | null; short?: string }) {
  return <div><dt>{props.label}</dt><dd class="mono"><Show when={props.value} fallback={<span class="subtle">—</span>}>
    <span title={props.value!}>{props.short ?? props.value}</span><CopyButton value={props.value!} label={'Copy ' + props.label} /></Show></dd></div>;
}
