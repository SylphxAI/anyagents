import { For, Show, createMemo, createSignal } from 'solid-js';
import { ChartNoAxesColumn } from 'lucide-solid';
import { api, createPoll } from '../api';
import type { Resource } from '../api';
import { compact, number, usd } from '../format';
import { agentHref } from '../route';
import { Empty, Problem, Section, Segmented, Skeleton, StatStrip } from '../ui/primitives';

interface Totals { turns: number; input_tokens: number; output_tokens: number; cache_read_tokens: number; cache_write_tokens: number; reported_cost_micros: number | null }
interface Bucket extends Totals { agent_id: string; agent_name: string | null; model?: string }
interface Report { totals: Totals; buckets: Bucket[] }
const volume = (bucket: Bucket) => bucket.input_tokens + bucket.output_tokens + bucket.cache_read_tokens;

export function UsageSettings() {
  const [days, setDays] = createSignal<'1' | '7' | '30' | '90'>('7');
  const [mode, setMode] = createSignal<'agent' | 'model'>('agent');
  const byAgent = createPoll(signal => api<Report>('/v1/usage?days=' + days() + '&group_by=agent', { signal }), { key: days });
  const byModel = createPoll(signal => api<Report>('/v1/usage?days=' + days() + '&group_by=model', { signal }), { key: days });
  const totals = () => byAgent.data()?.totals ?? byModel.data()?.totals;
  const hit = () => {
    const input = totals()?.input_tokens ?? 0, cacheRead = totals()?.cache_read_tokens ?? 0;
    return input + cacheRead > 0 ? Math.round(cacheRead / (input + cacheRead) * 100) : 0;
  };
  return <>
    <div class="toolbar"><Segmented label="Time window" value={days()} onChange={setDays} options={[
      { value: '1', label: '24 hours' }, { value: '7', label: '7 days' }, { value: '30', label: '30 days' }, { value: '90', label: '90 days' }]} /></div>
    <StatStrip label="Usage totals" items={[
      { label: 'Turns', value: totals() ? number(totals()!.turns) : '—' },
      { label: 'Input tokens', value: totals() ? compact(totals()!.input_tokens) : '—', detail: totals() ? hit() + '% served from cache' : undefined },
      { label: 'Output tokens', value: totals() ? compact(totals()!.output_tokens) : '—', detail: totals() ? 'Cache writes ' + compact(totals()!.cache_write_tokens) : undefined },
      { label: 'Reported cost', value: totals() ? usd(totals()!.reported_cost_micros) : '—', detail: 'API-billed turns only' },
    ]} />
    <Section title="Breakdown" actions={<Segmented label="Group by" value={mode()} onChange={setMode} options={[{ value: 'agent', label: 'By agent' }, { value: 'model', label: 'By model' }]} />}>
      <Breakdown resource={mode() === 'agent' ? byAgent : byModel} byModel={mode() === 'model'} />
    </Section>
  </>;
}

function Breakdown(props: { resource: Resource<Report>; byModel: boolean }) {
  const buckets = createMemo(() => [...(props.resource.data()?.buckets ?? [])].sort((a, b) => volume(b) - volume(a)));
  const max = () => Math.max(1, ...buckets().map(volume));
  return <>
    <Show when={props.resource.error()}><Problem retry={props.resource.refresh}>{props.resource.error()}</Problem></Show>
    <Show when={props.resource.loading()}><Skeleton rows={6} /></Show>
    <Show when={props.resource.data()}><Show when={buckets().length} fallback={<Empty compact icon={<ChartNoAxesColumn size={18} />} title="No usage in this window">Usage appears after agents complete turns.</Empty>}>
      <div class="table-wrap"><table class="table">
        <thead><tr><th>{props.byModel ? 'Agent · Model' : 'Agent'}</th><th class="col-bar">Share</th><th class="right">Turns</th><th class="right">Input</th><th class="right">Output</th><th class="right">Cache read</th><th class="right">Cost</th></tr></thead>
        <tbody><For each={buckets()}>{bucket => {
          const name = bucket.agent_name || bucket.agent_id;
          return <tr>
            <td><a class="strong" href={agentHref(name)}>{name}</a><Show when={props.byModel}><span class="mono subtle"> · {bucket.model || 'default'}</span></Show></td>
            <td class="col-bar"><span class="bar"><span style={{ width: Math.max(2, volume(bucket) / max() * 100) + '%' }} /></span></td>
            <td class="right num">{number(bucket.turns)}</td><td class="right num">{compact(bucket.input_tokens)}</td>
            <td class="right num">{compact(bucket.output_tokens)}</td><td class="right num">{compact(bucket.cache_read_tokens)}</td>
            <td class="right num">{usd(bucket.reported_cost_micros)}</td>
          </tr>;
        }}</For></tbody></table></div>
    </Show></Show>
  </>;
}
