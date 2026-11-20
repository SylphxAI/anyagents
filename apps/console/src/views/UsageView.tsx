import { useCallback, useState } from 'react';
import { api, useResource } from '../api';
import { compact, number, usd } from '../format';
import { Badge, Field, Heading, Metrics, Panel, Problem, Refresh, Table } from '../ui';

interface UsageTotals {
  turns: number;
  input_tokens: number;
  output_tokens: number;
  cache_read_tokens: number;
  cache_write_tokens: number;
  reported_cost_micros: number | null;
}
interface UsageBucket extends UsageTotals {
  agent_id: string;
  agent_name: string | null;
  model?: string;
}
interface UsageReport {
  totals: UsageTotals;
  buckets: UsageBucket[];
}

const volume = (bucket: UsageBucket) => bucket.input_tokens + bucket.output_tokens + bucket.cache_read_tokens;

function UsageBreakdown({ mode, resource }: { mode: 'agent' | 'model'; resource: ReturnType<typeof useResource<UsageReport>> }) {
  const { data, error, refresh } = resource;
  const buckets = data?.buckets ?? [];
  const maxVolume = buckets.reduce((max, bucket) => Math.max(max, volume(bucket)), 1);
  const byModel = mode === 'model';
  const unit = byModel ? 'bucket' : 'agent';
  return <Panel title={byModel ? 'Usage by Model' : 'Usage by Agent'}
    actions={data && <Badge>{buckets.length} {unit}{buckets.length === 1 ? '' : 's'}</Badge>}>
    {error && <div className="panel-body"><Problem retry={refresh}>Could not load usage by {mode}: {error}</Problem></div>}
    {!data && !error && <p className="panel-body muted" role="status">Loading usage by {mode}…</p>}
    {data && <Table headers={[byModel ? 'Agent / Model' : 'Agent', 'Turns', 'Input', 'Output', 'Cache Read', 'Cost']}
      empty="No usage records in this window." rows={buckets.map(bucket => ({
        key: JSON.stringify([bucket.agent_id, bucket.model ?? null]),
        cells: [
          <div className="stack">
            <span className="mono">{bucket.agent_name || bucket.agent_id}{byModel && (' · ' + (bucket.model || 'default'))}</span>
            <meter aria-hidden="true" min={0} max={maxVolume} value={volume(bucket)} />
          </div>,
          <span className="mono">{number(bucket.turns)}</span>,
          <span className="mono">{compact(bucket.input_tokens)}</span>,
          <span className="mono">{compact(bucket.output_tokens)}</span>,
          <span className="mono">{compact(bucket.cache_read_tokens)}</span>,
          <span className="mono">{usd(bucket.reported_cost_micros)}</span>,
        ],
      }))} />}
  </Panel>;
}

export function UsageView() {
  const [days, setDays] = useState('7');
  const byAgent = useResource(useCallback((signal: AbortSignal) => api<UsageReport>(
    '/v1/usage?days=' + days + '&group_by=agent', { signal }), [days]));
  const byModel = useResource(useCallback((signal: AbortSignal) => api<UsageReport>(
    '/v1/usage?days=' + days + '&group_by=model', { signal }), [days]));
  const totals = byAgent.data?.totals ?? byModel.data?.totals;
  const input = totals?.input_tokens ?? 0;
  const cacheRead = totals?.cache_read_tokens ?? 0;
  const cacheHit = input + cacheRead > 0 ? Math.round(cacheRead / (input + cacheRead) * 100) : 0;

  return <div className="flow">
    <Heading eyebrow="Telemetry" title="Usage Telemetry" blurb="Track turns, token usage, and reported costs by agent and model.">
      <Field label="Window"><select value={days} onChange={event => setDays(event.currentTarget.value)}>
        <option value="1">Last 24 hours</option>
        <option value="7">Last 7 days</option>
        <option value="14">Last 14 days</option>
        <option value="30">Last 30 days</option>
        <option value="90">Last 90 days</option>
      </select></Field>
      <Refresh onClick={() => { byAgent.refresh(); byModel.refresh(); }} />
    </Heading>
    {totals && <Metrics items={[
      { label: 'Total Turns', value: number(totals.turns), detail: days === '1' ? 'Last 24 hours' : 'Last ' + days + ' days' },
      { label: 'Input Tokens', value: compact(input), detail: 'Cache read: ' + compact(cacheRead) + ' (' + cacheHit + '% hit)' },
      { label: 'Output Tokens', value: compact(totals.output_tokens), detail: 'Cache write: ' + compact(totals.cache_write_tokens) },
      { label: 'Reported Cost', value: usd(totals.reported_cost_micros), detail: 'API-billed turns' },
    ]} />}
    <div className="two-cols">
      <UsageBreakdown mode="agent" resource={byAgent} />
      <UsageBreakdown mode="model" resource={byModel} />
    </div>
  </div>;
}
