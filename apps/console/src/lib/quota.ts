// Parses the coordinator's human-readable usage strings into structured quota
// readings. Every format the quota probes emit is covered by quota.test.ts:
//   "5h: 72% left ↻2h15m · 7d: 45% left ↻3d04h (pro)"
//   "5h: 90% left · 7d: 64% left · opus: 51% left (max)"
//   "mo: 0% left ($0.00/$20 pro)"         "oauth expired · 5h: 40% left"
//   "limited ↻1h · 5h: 0% left"           "$12.40 left ($88 / $100)"
//   "active (12 models)"                  "73% left"

export interface QuotaWindow {
  /** Window label as reported (`5h`, `7d`, `opus`, `mo`, `wk`, `day`, `req`, ...). */
  label: string;
  /** Remaining capacity, 0..100. */
  left: number;
  /** Reset ETA when reported (e.g. `2h15m`). */
  reset?: string;
}

export interface UsageReading {
  windows: QuotaWindow[];
  /** The provider rejected the stored OAuth credential. */
  expired: boolean;
  /** The provider is rate limiting outside the reported windows. */
  limited: boolean;
  limitedReset?: string;
  /** Plan or tier annotation in trailing parentheses. */
  tier?: string;
  /** Prepaid balance text, e.g. `$12.40 left`. */
  balance?: string;
  /** Any other non-quota note, e.g. `active (12 models)`. */
  note?: string;
  /** Lowest remaining percentage across windows, or null without windows. */
  minLeft: number | null;
}

const EMPTY: UsageReading = { windows: [], expired: false, limited: false, minLeft: null };
const WINDOW = /^([A-Za-z0-9][\w.\- ]{0,23}?)\s*:\s*(\d{1,3}(?:\.\d+)?)\s*%\s*(left|used|remaining)?\s*(?:↻\s*(\S+))?\s*(?:\(([^()]*)\))?\s*$/i;
const BARE = /^(\d{1,3}(?:\.\d+)?)\s*%\s*(left|used|remaining)\s*(?:↻\s*(\S+))?\s*$/i;
const BALANCE = /^\$\s?[\d,]+(?:\.\d+)?\s+left\b/i;

function clampPercent(raw: string, mode?: string) {
  const value = Math.max(0, Math.min(100, Math.round(Number(raw))));
  return mode?.toLowerCase() === 'used' ? 100 - value : value;
}

export function parseUsage(usage?: string | null): UsageReading {
  let text = (usage ?? '').trim();
  if (!text || text === '__NONE__') return { ...EMPTY, windows: [] };
  const reading: UsageReading = { ...EMPTY, windows: [] };

  const expired = /^oauth\s+expired\b\s*(?:[·:-]\s*)?/i.exec(text);
  if (expired) { reading.expired = true; text = text.slice(expired[0].length).trim(); }

  const limited = /^limited\b(?:\s*↻\s*([^\s·]+))?\s*(?:·\s*)?/i.exec(text);
  if (limited) {
    reading.limited = true;
    if (limited[1]) reading.limitedReset = limited[1];
    text = text.slice(limited[0].length).trim();
  }

  // A trailing "(tier)" annotates the whole reading, unless it is part of a balance.
  const tier = /\s\(([^()]*)\)\s*$/.exec(text);
  if (tier && !BALANCE.test(text) && !/^active\b/i.test(text)) {
    reading.tier = tier[1].trim() || undefined;
    text = text.slice(0, tier.index).trim();
  }

  for (const raw of text.split(/\s*·\s*/)) {
    const part = raw.trim();
    if (!part) continue;
    const window = WINDOW.exec(part);
    if (window) {
      const [, label, count, mode, reset, detail] = window;
      reading.windows.push({ label: label.trim().toLowerCase(), left: clampPercent(count, mode), ...(reset ? { reset } : {}) });
      if (detail && !reading.tier) reading.tier = detail.trim();
      continue;
    }
    const bare = BARE.exec(part);
    if (bare) {
      reading.windows.push({ label: 'quota', left: clampPercent(bare[1], bare[2]), ...(bare[3] ? { reset: bare[3] } : {}) });
      continue;
    }
    if (BALANCE.test(part)) { reading.balance = part; continue; }
    reading.note = reading.note ? reading.note + ' · ' + part : part;
  }
  reading.minLeft = reading.windows.length ? Math.min(...reading.windows.map(window => window.left)) : null;
  if (reading.limited && reading.minLeft === null) reading.minLeft = 0;
  return reading;
}

/** Friendly label for a quota window key. */
export function windowLabel(label: string) {
  const known: Record<string, string> = {
    '5h': '5h', '7d': '7d', weekly: '7d', wk: 'Week', mo: 'Month', monthly: 'Month', day: 'Day', daily: 'Day',
    req: 'Requests', quota: 'Quota', opus: 'Opus', sonnet: 'Sonnet',
  };
  return known[label] ?? label.charAt(0).toUpperCase() + label.slice(1);
}
