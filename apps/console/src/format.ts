export const number = (value?: number) => value == null ? '—' : value.toLocaleString();
export const compact = (value?: number) => value == null ? '—' : Intl.NumberFormat(undefined, { notation: 'compact' }).format(value);
export const usd = (micros?: number | null) => micros == null ? '—' : '$' + (micros / 1e6).toFixed(micros > 0 && micros < 10_000 ? 4 : 2);
export const time = (ms?: number | null) => ms ? new Date(ms).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '—';
export function duration(ms: number) {
  const seconds = Math.max(0, Math.round(ms / 1000));
  if (seconds < 60) return seconds + 's';
  if (seconds < 3600) return Math.floor(seconds / 60) + 'm ' + (seconds % 60) + 's';
  return Math.floor(seconds / 3600) + 'h ' + (Math.floor(seconds / 60) % 60) + 'm';
}

export function safeLoginUrl(value: string) {
  try {
    const url = new URL(value);
    return !url.username && !url.password && (url.protocol === 'https:' ||
      (url.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname))) ? url.href : undefined;
  } catch { return undefined; }
}
