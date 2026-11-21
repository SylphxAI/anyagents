// Keyboard-palette ranking and match highlighting. Lower rank is better;
// Infinity means no match.

export function rank(query: string, target: string) {
  const text = target.toLowerCase();
  const tokens = query.toLowerCase().trim().split(/\s+/).filter(Boolean);
  let score = 0;
  for (const token of tokens) {
    const index = text.indexOf(token);
    if (index === 0) continue;
    if (index > 0) { score += /[\s/·._-]/.test(text[index - 1]) ? 1 + index / 100 : 5 + index / 10; continue; }
    let cursor = 0, gaps = 0;
    for (const char of token) {
      const position = text.indexOf(char, cursor);
      if (position < 0) return Infinity;
      gaps += position - cursor;
      cursor = position + 1;
    }
    score += 40 + gaps;
  }
  return score;
}

export interface Segment { text: string; hit: boolean }

/** Splits `text` into segments, marking case-insensitive substring hits of each query token. */
export function highlight(text: string, query: string): Segment[] {
  const tokens = query.toLowerCase().trim().split(/\s+/).filter(Boolean);
  if (!tokens.length) return [{ text, hit: false }];
  const marks = new Array<boolean>(text.length).fill(false);
  const lower = text.toLowerCase();
  for (const token of tokens) {
    let from = 0;
    while (from <= lower.length) {
      const index = lower.indexOf(token, from);
      if (index < 0) break;
      for (let i = index; i < index + token.length; i++) marks[i] = true;
      from = index + token.length;
    }
  }
  const segments: Segment[] = [];
  for (let i = 0; i < text.length; i++) {
    const last = segments[segments.length - 1];
    if (last && last.hit === marks[i]) last.text += text[i];
    else segments.push({ text: text[i], hit: marks[i] });
  }
  return segments;
}
