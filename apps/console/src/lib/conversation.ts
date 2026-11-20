// Collapses the per-recipient copies of one send into a single message row.
// The coordinator stores a multi-recipient send as one turn per recipient
// (same sender, direction and text, written within moments of each other).

export interface TurnLike {
  seq: number;
  id?: string;
  timestamp_ms?: number;
  direction?: string;
  role?: string;
  from?: string;
  to?: string;
  text?: string;
  tool_name?: string;
  tool_names?: string[];
}

export type GroupedTurn<T extends TurnLike> = T & { recipients: string[] };

const WINDOW_MS = 5_000;

export function groupTurns<T extends TurnLike>(turns: readonly T[]): GroupedTurn<T>[] {
  const out: GroupedTurn<T>[] = [];
  for (const turn of turns) {
    const last = out[out.length - 1];
    const sameSend = last
      && last.direction === turn.direction && last.from === turn.from && (last.text ?? '') === (turn.text ?? '')
      && Boolean(turn.text) && Math.abs((turn.timestamp_ms ?? 0) - (last.timestamp_ms ?? 0)) <= WINDOW_MS;
    if (sameSend) {
      if (turn.to && !last.recipients.includes(turn.to)) last.recipients.push(turn.to);
      continue;
    }
    out.push({ ...turn, recipients: turn.to ? [turn.to] : [] });
  }
  return out;
}

/** Display form of an address: `agent:name` → `name`; other schemes keep their prefix. */
export function recipientLabel(address: string) {
  return address.startsWith('agent:') ? address.slice(6) : address;
}
