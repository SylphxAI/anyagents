/** A component/fingerprint pair identifies one issue; occurrence counts are already aggregated by the coordinator. */
export interface RuntimeIssue {
  id: string;
  component: string;
  error_fingerprint: string;
  summary: string;
  occurrence_count?: number;
  source?: string;
  status?: string;
  last_seen_at?: number; // normalized to epoch milliseconds
  sample_agent_id?: string;
}

const text = (value: unknown) => typeof value === 'string' && value.trim() ? value : undefined;
const timestamp = (value: unknown) => typeof value === 'number' && Number.isSafeInteger(value) && Math.abs(value) <= 8.64e15 ? value : undefined;

export const issueKey = (issue: RuntimeIssue) => issue.component + ':' + issue.error_fingerprint;

function latestFirst(a: RuntimeIssue, b: RuntimeIssue) {
  return (b.last_seen_at ?? -1) - (a.last_seen_at ?? -1) || b.id.localeCompare(a.id);
}

/** Read the admin API envelope. Ignore rows without a stable identity. */
export function runtimeIssues(payload: unknown): RuntimeIssue[] {
  const list = (payload as { issues?: unknown } | null)?.issues;
  if (!Array.isArray(list)) return [];
  const rows = new Map<string, RuntimeIssue>();
  for (const value of list) {
    if (!value || typeof value !== 'object') continue;
    const row = value as Record<string, unknown>;
    const id = text(row.id);
    const component = text(row.component);
    const error_fingerprint = text(row.error_fingerprint);
    const summary = text(row.summary);
    if (!id || !component || !error_fingerprint || !summary) continue;
    const count = row.occurrence_count;
    const issue: RuntimeIssue = {
      id, component, error_fingerprint, summary,
      occurrence_count: typeof count === 'number' && Number.isSafeInteger(count) && count >= 0 ? count : undefined,
      source: text(row.source), status: text(row.status), last_seen_at: timestamp(row.last_seen_at),
      sample_agent_id: text(row.sample_agent_id),
    };
    const key = issueKey(issue);
    const previous = rows.get(key);
    if (!previous || latestFirst(issue, previous) < 0) rows.set(key, issue);
  }
  return [...rows.values()].sort((a, b) => latestFirst(a, b) || issueKey(a).localeCompare(issueKey(b)));
}
