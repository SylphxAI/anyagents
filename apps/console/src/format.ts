export const number = (value?: number | null) => value == null ? '—' : value.toLocaleString('en-US');
export const compact = (value?: number | null) => value == null ? '—'
  : Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 }).format(value);
export const usd = (micros?: number | null) => micros == null ? '—'
  : '$' + (micros / 1e6).toFixed(micros > 0 && micros < 10_000 ? 4 : 2);

export function dateTime(ms?: number | null) {
  if (!ms) return '—';
  return new Date(ms).toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false });
}

export function duration(ms: number) {
  const seconds = Math.max(0, Math.round(ms / 1000));
  if (seconds < 60) return seconds + 's';
  if (seconds < 3600) return Math.floor(seconds / 60) + 'm ' + String(seconds % 60).padStart(2, '0') + 's';
  if (seconds < 86_400) return Math.floor(seconds / 3600) + 'h ' + String(Math.floor(seconds / 60) % 60).padStart(2, '0') + 'm';
  return Math.floor(seconds / 86_400) + 'd ' + Math.floor(seconds / 3600) % 24 + 'h';
}

/** "3m ago" style relative time. */
export function ago(ms?: number | null, now = Date.now()) {
  if (!ms) return '—';
  const delta = now - ms;
  if (delta < 0) return 'in ' + duration(-delta).split(' ')[0];
  if (delta < 45_000) return 'just now';
  return duration(delta).split(' ')[0] + ' ago';
}

export function shortDigest(digest?: string | null) {
  if (!digest) return '—';
  const [algo, hash] = digest.includes(':') ? digest.split(':', 2) : ['', digest];
  return (algo ? algo + ':' : '') + hash.slice(0, 12);
}

export function plural(count: number, word: string, many = word + 's') {
  return count.toLocaleString('en-US') + ' ' + (count === 1 ? word : many);
}

export function safeLink(value?: string) {
  try {
    const url = value ? new URL(value) : undefined;
    return url && /^https?:$/.test(url.protocol) && !url.username && !url.password ? url.href : undefined;
  } catch { return undefined; }
}
