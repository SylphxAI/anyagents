import { render } from 'solid-js/web';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { SettingsPage } from '../pages/SettingsPage';
import { parseRoute } from '../route';
import type { BlockRule } from '../lib/ingress';

vi.mock('../state', () => ({ useConsole: () => ({ roster: {
  loading: () => false, error: () => undefined, refresh: () => {},
  data: () => ({ agents: [{ id: 'first-id', name: 'First' }, { id: 'second-id', name: 'Second' }] }),
} }) }));

const blocks = (rules: BlockRule[] = []) => Response.json({ result: 'blocks', rules });
const sender: BlockRule = { rule_id: '1', target: { channel: 'telegram', scope: { kind: 'sender', sender_id: '123' } }, created_at_ms: 1000, reason: 'Repeated spam' };
const group: BlockRule = { rule_id: '2', target: { channel: 'telegram', scope: { kind: 'chat', chat_id: '-1004' } }, created_at_ms: 2000 };
const firstPath = '/admin/api/agents/first-id/ingress-blocks';
const secondPath = '/admin/api/agents/second-id/ingress-blocks';
const refusal = 'protected relationships cannot be blocked: Admin';

let host: HTMLDivElement;
let dispose: () => void;
const button = (label: string, root: ParentNode = host) => [...root.querySelectorAll('button')].find(node => node.textContent === label)!;
const dialog = () => host.querySelector('dialog')!;
const submit = (label: string) => button(label, dialog()).click();
const expectText = (text: string) => vi.waitFor(() => expect(host.textContent).toContain(text));
function select(element: HTMLSelectElement, value: string) {
  element.value = value;
  element.dispatchEvent(new Event('change', { bubbles: true }));
}
function recipient(id: string) { select(host.querySelector('select')!, id); }
function open(fetch: (path: string, init?: RequestInit) => Promise<Response>) {
  vi.stubGlobal('fetch', fetch);
  host = document.body.appendChild(document.createElement('div'));
  const route = parseRoute('#settings/ingress');
  if (route.page !== 'settings') throw new Error('Expected a settings route');
  dispose = render(() => <SettingsPage tab={route.tab} />, host);
}
afterEach(() => { dispose?.(); host?.remove(); vi.unstubAllGlobals(); });

describe('Ingress Guard', () => {
  it('requires recipient selection, cancels stale reads, and filters active targets', async () => {
    let stale!: (value: Response) => void;
    const fetch = vi.fn((path: string) => path === firstPath
      ? new Promise<Response>(resolve => { stale = resolve; })
      : Promise.resolve(blocks([group, { ...sender, rule_id: 'expired', expires_at_ms: 1000 }])));
    open(fetch);
    await expectText('Choose a receiving agent');
    expect(fetch).not.toHaveBeenCalled();
    recipient('first-id');
    await vi.waitFor(() => expect(fetch.mock.calls[0]?.[0]).toBe(firstPath));
    recipient('second-id');
    await vi.waitFor(() => expect(fetch.mock.calls[1]?.[0]).toBe(secondPath));
    await expectText('-1004');
    stale(blocks([sender]));
    const search = host.querySelector<HTMLInputElement>('input[type=search]')!;
    search.value = 'telegram -1004'; search.dispatchEvent(new Event('input', { bubbles: true }));
    await expectText('1 of 1 active block');
    expect(host.textContent).not.toContain('Repeated spam');
    button('Senders').click(); await expectText('No matching blocks');
    button('Clear filters').click(); await expectText('-1004');
  });

  it('validates before POST, preserves a protected refusal, reloads a saved chat and shows the provider advisory', async () => {
    let rules: BlockRule[] = [];
    const fetch = vi.fn(async (_path: string, init?: RequestInit) => {
      if (init?.method === 'GET') return blocks(rules);
      const body = JSON.parse(init?.body as string);
      if (body.target.scope.kind === 'sender') return Response.json({ error: refusal }, { status: 400 });
      rules = [group];
      return Response.json({ result: 'blocked', rule: group, leave_chat: { status: 'skipped', reason: 'leaving could cut off configured protected senders' } });
    });
    open(fetch); recipient('first-id'); await expectText('No active blocks');
    button('Add block').click();
    const id = dialog().querySelector<HTMLInputElement>('[name=target]')!;
    const reason = dialog().querySelector<HTMLTextAreaElement>('textarea')!;
    id.value = '@nonnumeric'; reason.value = 'Kept on refusal'; submit('Add block');
    await expectText('Sender ID must be');
    expect(fetch.mock.calls).toHaveLength(1);
    id.value = '432'; submit('Add block'); await expectText(refusal);
    expect(reason.value).toBe('Kept on refusal');
    expect(id.value).toBe('432');
    const [path, write] = fetch.mock.calls[1];
    expect(path).toBe(firstPath);
    expect(write?.credentials).toBe('same-origin');
    expect(new Headers(write?.headers).get('x-anyagents-admin-csrf')).toBe('1');
    expect(JSON.parse(write?.body as string)).toEqual({ op: 'block_sender', target: { channel: 'telegram', scope: { kind: 'sender', sender_id: '432' } }, reason: 'Kept on refusal' });
    select(dialog().querySelector('select')!, 'chat');
    id.value = '-1004'; submit('Add block');
    await expectText('Telegram leaveChat skipped: leaving could cut off configured protected senders.');
    await expectText('1 of 1 active block');
    expect(dialog()).toBeNull();
    expect(fetch.mock.calls.map(([, options]) => options?.method)).toEqual(['GET', 'POST', 'POST', 'GET']);
  });

  it('requires confirmation and keeps a row on a failed unblock before retrying and reloading', async () => {
    let attempt = 0;
    const fetch = vi.fn(async (_path: string, init?: RequestInit) => {
      if (init?.method === 'GET') return blocks(attempt < 2 ? [group] : []);
      return ++attempt === 1 ? Response.json({ error: 'block storage is unavailable' }, { status: 500 })
        : Response.json({ result: 'unblocked', removed: true });
    });
    open(fetch); recipient('first-id'); await expectText('-1004');
    button('Unblock').click();
    expect(dialog().textContent).toContain('Unblocking does not rejoin a chat the bot left.');
    submit('Cancel'); expect(fetch.mock.calls).toHaveLength(1);
    button('Unblock').click(); submit('Unblock'); await expectText('block storage is unavailable');
    expect(host.querySelector('li')?.textContent).toContain('-1004');
    submit('Unblock'); await expectText('No active blocks');
    await expectText('Unblocked chat -1004 for First.');
    expect(JSON.parse(fetch.mock.calls[2][1]?.body as string)).toEqual({ op: 'unblock_sender', target: group.target });
  });

  it('does not present an authorization failure as an empty list', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(Response.json({ error: 'operator required' }, { status: 403 })).mockResolvedValueOnce(blocks());
    open(fetch); recipient('first-id');
    await expectText('Could not load ingress blocks: operator required');
    expect(button('Add block').disabled).toBe(true);
    expect(host.textContent).not.toContain('No active blocks');
    button('Refresh').click(); await expectText('No active blocks');
    expect(button('Add block').disabled).toBe(false);
  });
});
