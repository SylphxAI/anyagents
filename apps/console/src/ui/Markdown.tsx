import type { JSX } from 'solid-js';
import { safeLink } from '../format';

// A deliberately small Markdown renderer that builds DOM nodes through JSX
// (never innerHTML): headings, paragraphs, lists, quotes, fenced code, and
// inline code / bold / italic / links.

function inline(text: string): JSX.Element[] {
  const out: JSX.Element[] = [];
  const pattern = /(`[^`]+`)|(\*\*[^*]+\*\*)|(\*[^*\s][^*]*\*)|(\[[^\]]+\]\([^)\s]+\))/g;
  let last = 0, match: RegExpExecArray | null;
  while ((match = pattern.exec(text))) {
    if (match.index > last) out.push(text.slice(last, match.index));
    const token = match[0];
    if (token.startsWith('`')) out.push(<code>{token.slice(1, -1)}</code>);
    else if (token.startsWith('**')) out.push(<strong>{token.slice(2, -2)}</strong>);
    else if (token.startsWith('*')) out.push(<em>{token.slice(1, -1)}</em>);
    else {
      const [, label, url] = /^\[([^\]]+)\]\(([^)\s]+)\)$/.exec(token) ?? [];
      const href = safeLink(url);
      out.push(href ? <a href={href} target="_blank" rel="noopener noreferrer">{label}</a> : label);
    }
    last = match.index + token.length;
  }
  if (last < text.length) out.push(text.slice(last));
  return out;
}

function render(source: string): JSX.Element[] {
  const lines = source.replace(/\r\n/g, '\n').split('\n');
  const blocks: JSX.Element[] = [];
  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    if (/^```/.test(line)) {
      const body: string[] = [];
      i++;
      while (i < lines.length && !/^```/.test(lines[i])) body.push(lines[i++]);
      i++;
      blocks.push(<pre class="code"><code>{body.join('\n')}</code></pre>);
      continue;
    }
    const heading = /^(#{1,4})\s+(.*)$/.exec(line);
    if (heading) {
      const level = heading[1].length;
      const content = inline(heading[2]);
      blocks.push(level === 1 ? <h2>{content}</h2> : level === 2 ? <h3>{content}</h3> : level === 3 ? <h4>{content}</h4> : <h5>{content}</h5>);
      i++; continue;
    }
    if (/^\s*[-*]\s+/.test(line) || /^\s*\d+[.)]\s+/.test(line)) {
      const ordered = /^\s*\d+[.)]\s+/.test(line);
      const items: string[] = [];
      while (i < lines.length && (ordered ? /^\s*\d+[.)]\s+/ : /^\s*[-*]\s+/).test(lines[i])) items.push(lines[i++].replace(/^\s*(?:[-*]|\d+[.)])\s+/, ''));
      const children = items.map(item => <li>{inline(item)}</li>);
      blocks.push(ordered ? <ol>{children}</ol> : <ul>{children}</ul>);
      continue;
    }
    if (/^>\s?/.test(line)) {
      const quote: string[] = [];
      while (i < lines.length && /^>\s?/.test(lines[i])) quote.push(lines[i++].replace(/^>\s?/, ''));
      blocks.push(<blockquote>{inline(quote.join(' '))}</blockquote>);
      continue;
    }
    if (!line.trim()) { i++; continue; }
    const paragraph: string[] = [];
    while (i < lines.length && lines[i].trim() && !/^(```|#{1,4}\s|>|\s*[-*]\s|\s*\d+[.)]\s)/.test(lines[i])) paragraph.push(lines[i++]);
    blocks.push(<p>{inline(paragraph.join(' '))}</p>);
  }
  return blocks;
}

export function Markdown(props: { source: string }) {
  return <div class="prose">{render(props.source)}</div>;
}
