import { render } from 'solid-js/web';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ConnectDialog } from './ConnectDialog';

vi.mock('../state', () => ({ useConsole: () => ({ refreshProfiles: vi.fn() }) }));

let dispose: (() => void) | undefined;
afterEach(() => {
  dispose?.();
  document.body.replaceChildren();
  vi.unstubAllGlobals();
});

function button(text: string) {
  return [...document.querySelectorAll('button')].find(node => node.textContent === text);
}

function open(response: Response) {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response));
  const host = document.body.appendChild(document.createElement('div'));
  dispose = render(() => <ConnectDialog request={{ provider: 'openai' }} close={() => {}} />, host);
  host.querySelector<HTMLButtonElement>('.dialog-foot .primary')!.click();
  expect(host.querySelector('.connect-status')?.textContent).toBe('Preparing an isolated sign-in…');
  return host;
}

describe('native sign-in dialog', () => {
  it('renders live progress while keeping links, codes, prompts and input actionable', async () => {
    let stream!: ReadableStreamDefaultController<Uint8Array>;
    const host = open(new Response(new ReadableStream({ start(controller) { stream = controller; } })));
    const emit = (kind: string, text = '') => stream.enqueue(new TextEncoder().encode(JSON.stringify({ kind, text }) + '\n'));
    const status = () => host.querySelector('.connect-status')?.textContent;

    emit('progress', 'Waiting for the sign-in sandbox…');
    await vi.waitFor(() => expect(status()).toBe('Waiting for the sign-in sandbox…'));
    expect(host.textContent).not.toContain('Preparing an isolated sign-in…');

    const link = 'https://auth.openai.com/codex/device';
    emit('session', 'fixture-session');
    emit('link', link);
    emit('code', 'U81Q-J76WZ');
    emit('prompt', 'Approve access in your browser.');
    emit('input');
    for (let i = 0; i < 66; i++) {
      emit('keepalive');
      emit('progress', `Provider status ${i + 1}`);
    }
    await vi.waitFor(() => expect(status()).toBe('Provider status 66'));
    expect(host.querySelector('a')?.href).toBe(link);
    expect(host.querySelector('.device-code')?.textContent).toContain('U81Q-J76WZ');
    expect(host.querySelector('.connect-log')?.textContent).toContain('Approve access in your browser.');
    expect(host.querySelector('[aria-label="Callback URL or authorization code"]')).not.toBeNull();
    stream.close();
  });

  it.each(['HTTP response', 'stream event'])('shows the exact 503 NO_CAPACITY diagnostic from an %s and unlocks retry', async source => {
    const error = 'Could not prepare sandbox: HTTP 503: NO_CAPACITY: compute unavailable at [endpoint]';
    const response = source === 'HTTP response'
      ? Response.json({ error }, { status: 503 })
      : new Response(new ReadableStream({ start(controller) {
        // Deliberately do not close the transport: an error event is terminal.
        controller.enqueue(new TextEncoder().encode(JSON.stringify({ kind: 'error', text: error }) + '\n'));
      } }));
    const host = open(response);
    await vi.waitFor(() => expect(host.querySelector('[role="alert"]')?.textContent).toBe(error), { timeout: 500 });
    expect(host.querySelector('.connect-status')).toBeNull();
    expect(button('Cancel sign-in')).toBeUndefined();
    expect(button('Try again')?.disabled).toBe(false);
  });
});
