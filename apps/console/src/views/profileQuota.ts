export const LOW_QUOTA = 35;

export function quotas(usage = '') {
  // A bucket may contain a parenthesized breakdown (including commas and more
  // percentages). Parse only top-level buckets; preserve breakdowns as details.
  const buckets: string[] = [];
  let start = 0;
  let depth = 0;
  for (let i = 0; i < usage.length; i++) {
    const char = usage[i];
    if (char === '(') depth++;
    if (char === ')') depth = Math.max(0, depth - 1);
    if (depth === 0 && /[·,;\n]/.test(char)) {
      buckets.push(usage.slice(start, i));
      start = i + 1;
    }
  }
  buckets.push(usage.slice(start));
  return buckets.flatMap(bucket => {
    const match = /^\s*(?:([\w][\w /.-]*?)\s*:\s*)?(\d{1,3}(?:\.\d+)?)\s*%\s*(?:(left|used)\b)?(.*)$/i.exec(bucket);
    if (!match) return [];
    const [, label, count, mode, detail] = match;
    const percent = Number(count);
    if (percent > 100) return [];
    return [{ label: label?.trim() || 'Quota', left: mode?.toLowerCase() === 'used' ? Number((100 - percent).toFixed(2)) : percent, detail: detail.trim() }];
  });
}

export function quotaState(usage = '') {
  const windows = quotas(usage);
  const expired = /^\s*oauth expired\b/i.test(usage);
  const low = windows.some(window => window.left <= LOW_QUOTA);
  return { windows, expired, low, healthy: windows.length > 0 && !expired && !low };
}
