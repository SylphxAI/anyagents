import { For, Match, Show, Switch } from 'solid-js';
import { dateTime, number } from '../format';
import { useConsole } from '../state';
import { PageHeader, Pill, Problem, Skeleton } from '../ui/primitives';

export function SystemIssuesPage() {
  const { issues } = useConsole();
  return <div class="page">
    <PageHeader title="System Issues" description="Recurring issues reported by agents and the coordinator." />
    <Show when={issues.error()}><Problem retry={issues.refresh}>{issues.error()}</Problem></Show>
    <Switch>
      <Match when={issues.loading()}><Skeleton rows={4} /></Match>
      <Match when={issues.data() === null}><p class="muted">Issue reporting isn’t available on this coordinator yet.</p></Match>
      <Match when={issues.data()?.length === 0}><p class="muted">No system issues reported.</p></Match>
      <Match when={issues.data()?.length}>
        <div class="table-wrap" tabindex="0" role="region" aria-label="System issues table">
          <table class="table" aria-label="System issues">
            <thead><tr><th>Component</th><th>Summary</th><th class="right">Occurrences</th><th>Source</th><th>Status</th><th>Last seen</th><th>Sample agent</th></tr></thead>
            <tbody><For each={issues.data()}>{issue => <tr>
              <td class="mono nowrap">{issue.component ?? '—'}</td>
              <td class="strong issue-summary" title={'Fingerprint: ' + issue.error_fingerprint}>{issue.summary}</td>
              <td class="num right">{number(issue.occurrence_count)}</td>
              <td class="muted nowrap">{issue.source === 'auto_capture' ? 'Auto capture' : issue.source === 'agent_report' ? 'Agent report' : issue.source ?? '—'}</td>
              <td class="nowrap"><Show when={issue.status} fallback="—">{status =>
                <Pill tone={['closed', 'resolved'].includes(status().toLowerCase()) ? 'ok' : status().toLowerCase() === 'open' ? 'warn' : 'neutral'}>{status()}</Pill>
              }</Show></td>
              <td class="subtle nowrap"><Show when={issue.last_seen_at !== undefined} fallback="—">
                <time dateTime={new Date(issue.last_seen_at!).toISOString()}>{dateTime(issue.last_seen_at)}</time>
              </Show></td>
              <td class="mono subtle nowrap">{issue.sample_agent_id ?? '—'}</td>
            </tr>}</For></tbody>
          </table>
        </div>
      </Match>
    </Switch>
  </div>;
}
