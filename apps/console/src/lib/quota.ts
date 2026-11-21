// Parses the coordinator's human-readable usage strings into structured quota
// readings. Every format the quota probes emit is covered by quota.test.ts:
//   "5h: 72% left ↻2h15m · 7d: 45% left ↻3d04h (pro)"
//   "5h: 90% left · 7d: 64% left · opus: 51% left (max)"
//   "mo: 0% left ($0.00/$20 pro)"         "oauth expired · 5h: 40% left"
//   "limited ↻1h · 5h: 0% left"           "$12.40 left ($88 / $100)"
//   "active (12 models)"                  "73% left"

export interface QuotaBucket {
  /** Quota key (`5h`, `7d`, `opus`, `mo`, `wk`, `day`, `req`, ...). */
  label: string;
  /** Remaining capacity, 0..100. */
  left: number;
  /** Reset ETA when reported (e.g. `2h15m`). */
  reset?: string;
}

export interface UsageReading {
  /** Account-wide windows and unscoped limits. */
  windows: QuotaBucket[];
  /** Limits that only apply to a model family. */
  modelPools: QuotaBucket[];
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
}

// Known model labels from provider readings. Other labels retain their window semantics.
const MODEL_POOLS = new Set(['opus', 'sonnet', 'gemini']);
const WINDOW_ALIASES = new Map([['weekly', '7d'], ['monthly', 'mo'], ['daily', 'day']]);
const WINDOW_ORDER = ['5h', '7d', 'mo', 'wk', 'day', 'req', 'quota'];
const WINDOW = /^([A-Za-z0-9][\w.\- ]{0,23}?)\s*:\s*(\d{1,3}(?:\.\d+)?)\s*%\s*(left|used|remaining)?\s*(?:↻\s*(\S+))?\s*(?:\(([^()]*)\))?\s*$/i;
const BARE = /^(\d{1,3}(?:\.\d+)?)\s*%\s*(left|used|remaining)\s*(?:↻\s*(\S+))?\s*$/i;
const BALANCE = /^\$\s?[\d,]+(?:\.\d+)?\s+left\b/i;

function clampPercent(raw: string, mode?: string) {
  const value = Math.max(0, Math.min(100, Math.round(Number(raw))));
  return mode?.toLowerCase() === 'used' ? 100 - value : value;
}

export function parseUsage(usage?: string | null): UsageReading {
  let text = (usage ?? '').trim();
  const reading: UsageReading = { windows: [], modelPools: [], expired: false, limited: false };
  if (!text || text === '__NONE__') return reading;

  const expired = /^oauth\s+expired\b\s*(?:[·:-]\s*)?/i.exec(text);
  if (expired) { reading.expired = true; text = text.slice(expired[0].length).trim(); }

  const limited = /^limited\b(?:\s*↻\s*([^\s·]+))?\s*(?:·\s*)?/i.exec(text);
  if (limited) {
    reading.limited = true;
    if (limited[1]) reading.limitedReset = limited[1];
    text = text.slice(limited[0].length).trim();
  }

  // A trailing "(tier)" annotates the whole reading, unless it is part of a balance.
  const tier = /(?:^|\s)\(([^()]*)\)\s*$/.exec(text);
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
      const key = label.trim().toLowerCase();
      const buckets = MODEL_POOLS.has(key) ? reading.modelPools : reading.windows;
      buckets.push({ label: WINDOW_ALIASES.get(key) ?? key, left: clampPercent(count, mode), ...(reset ? { reset } : {}) });
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
  return reading;
}

/** Common account windows first, then other windows, then model pools. */
export function compareQuotaLabels(a: string, b: string) {
  const order = (label: string) => {
    if (MODEL_POOLS.has(label)) return WINDOW_ORDER.length + 1;
    const index = WINDOW_ORDER.indexOf(label);
    return index === -1 ? WINDOW_ORDER.length : index;
  };
  return order(a) - order(b) || a.localeCompare(b);
}

/** Friendly label for a quota bucket key. */
export function windowLabel(label: string) {
  const known: Record<string, string> = {
    '5h': '5h', '7d': '7d', wk: 'Week', mo: 'Month', day: 'Day',
    req: 'Requests', quota: 'Quota', opus: 'Opus', sonnet: 'Sonnet', gemini: 'Gemini',
  };
  return known[label] ?? label.charAt(0).toUpperCase() + label.slice(1);
}
