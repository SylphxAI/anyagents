import { render } from 'solid-js/web';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { NodesPage } from './NodesPage';
import type { NodeAuditEvent, NodeGrant, NodeRecord } from '../types';

vi.mock('../state', () => ({
  useConsole: () => ({
    fleet: () => ({
      agents: [{ id: 'agt_alpha', name: 'Alpha' }, { id: 'agt_beta', name: 'Beta' }],
      live: [{ id: 'agt_alpha', name: 'Alpha' }, { id: 'agt_beta', name: 'Beta' }],
    }),
  }),
}));

const sampleNode: NodeRecord = {
  id: 'nd_01h455vb4pex5vsknk084sn02q',
  owner_id: 'ops@example.test',
  name: 'dev-laptop',
  hostname: 'laptop.local',
  os: 'darwin',
  arch: 'aarch64',
  capabilities: ['exec'],
  status: 'online',
  last_heartbeat_at_ms: Date.now() - 5000,
  created_at_ms: Date.now() - 60000,
  revoked_at_ms: null,
};

const sampleGrant: NodeGrant = {
  id: 'ng_1',
  node_id: sampleNode.id,
  agent_id: 'agt_alpha',
  agent_name: 'Alpha',
  capability: 'exec',
  granted_by: 'operator',
  created_at_ms: Date.now() - 30000,
  revoked_at_ms: null,
};

const sampleAudit: NodeAuditEvent = {
  id: 'na_1',
  node_id: sampleNode.id,
  agent_id: 'agt_alpha',
  capability: 'exec',
  action: 'exec',
  summary: 'uname -a',
  exit_code: 0,
  outcome: 'ok',
  started_at_ms: Date.now() - 10000,
  finished_at_ms: Date.now() - 9900,
};

let host: HTMLDivElement;
let dispose: () => void;
const button = (label: string, root: ParentNode = host) =>
  [...root.querySelectorAll('button')].find(node => node.textContent?.trim() === label)!;
const expectText = (text: string) => vi.waitFor(() => expect(host.textContent).toContain(text));

function open(fetchImpl: (path: string, init?: RequestInit) => Promise<Response>) {
  vi.stubGlobal('fetch', fetchImpl);
  host = document.body.appendChild(document.createElement('div'));
  dispose = render(() => <NodesPage />, host);
}

afterEach(() => {
  dispose?.();
  host?.remove();
  vi.unstubAllGlobals();
});

describe('NodesPage', () => {
  it('renders nodes, manages capability grants, and triggers pause/resume kill-switch', async () => {
    let currentStatus: NodeRecord['status'] = 'online';
    let grants: NodeGrant[] = [sampleGrant];

    const fetchMock = vi.fn(async (path: string, init?: RequestInit) => {
      const method = init?.method ?? 'GET';
      if (path === '/v1/nodes' && method === 'GET') {
        return Response.json({ nodes: [{ ...sampleNode, status: currentStatus }] });
      }
      if (path === `/v1/nodes/${sampleNode.id}/grants` && method === 'GET') {
        return Response.json({ grants });
      }
      if (path === `/v1/nodes/${sampleNode.id}/grants` && method === 'POST') {
        const body = JSON.parse(init?.body as string);
        const created: NodeGrant = {
          id: 'ng_2',
          node_id: sampleNode.id,
          agent_id: body.agent_id,
          agent_name: 'Beta',
          capability: body.capability,
          granted_by: 'operator',
          created_at_ms: Date.now(),
          revoked_at_ms: null,
        };
        grants = [created, ...grants];
        return Response.json({ grant: created }, { status: 201 });
      }
      if (path === `/v1/nodes/${sampleNode.id}/grants/ng_1` && method === 'DELETE') {
        grants = grants.filter(g => g.id !== 'ng_1');
        return Response.json({ revoked: true });
      }
      if (path === `/v1/nodes/${sampleNode.id}/audit?limit=50` && method === 'GET') {
        return Response.json({ events: [sampleAudit] });
      }
      if (path === `/v1/nodes/${sampleNode.id}/pause` && method === 'POST') {
        currentStatus = 'paused';
        return Response.json({ node: { ...sampleNode, status: currentStatus } });
      }
      if (path === `/v1/nodes/${sampleNode.id}/resume` && method === 'POST') {
        currentStatus = 'online';
        return Response.json({ node: { ...sampleNode, status: currentStatus } });
      }
      return Response.json({ error: 'not found' }, { status: 404 });
    });

    open(fetchMock);
    await expectText('dev-laptop');
    await expectText('darwin/aarch64');
    await expectText('Alpha');

    // Trigger 1-click Pause kill-switch.
    button('Pause').click();
    await expectText('Resume');
    expect(currentStatus).toBe('paused');

    // Switch to Audit Log tab and verify event rendering.
    button('Audit Log').click();
    await expectText('uname -a');
    await expectText('ok');

    // Switch back to Grants, grant Beta, then verify POST with CSRF header.
    button('Capability Grants').click();
    await expectText('Alpha');
    button('Grant Access').click();
    const dialog = host.querySelector('dialog')!;
    const select = dialog.querySelector<HTMLSelectElement>('select[name="agent"]')!;
    select.value = 'agt_beta';
    select.dispatchEvent(new Event('change', { bubbles: true }));
    button('Grant Access', dialog).click();
    await expectText('Beta');

    const postCall = fetchMock.mock.calls.find(
      ([p, init]) => p === `/v1/nodes/${sampleNode.id}/grants` && init?.method === 'POST',
    );
    expect(postCall).toBeDefined();
    expect(new Headers(postCall?.[1]?.headers).get('x-anyagents-admin-csrf')).toBe('1');
  });

  it('shows pairing instructions when no nodes exist', async () => {
    const fetchMock = vi.fn(async () => Response.json({ nodes: [] }));
    open(fetchMock);
    await expectText('No nodes paired yet');
    await expectText('anyagents node pair --url');
    await expectText('anyagents node run');
  });
});
