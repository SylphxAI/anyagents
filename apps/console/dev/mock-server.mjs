// Local preview of the embedded console against a dense mock API.
// Serves the compiled assets from crates/anyagents/src/server/console with the
// coordinator's exact production CSP. Usage: npm run preview [-- --port 4310]
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import * as fx from './fixtures.mjs';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../../../crates/anyagents/src/server/console');
const CSP = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'";
const portFlag = process.argv.indexOf('--port');
const port = Number(portFlag > 0 ? process.argv[portFlag + 1] : process.env.PORT || 4310);
const flags = new Set(process.argv.slice(2));
const unavailable = new Set([...flags].filter(flag => flag.startsWith('--no-')).map(flag => flag.slice(5)));

const headers = type => ({ 'content-type': type, 'content-security-policy': CSP, 'x-frame-options': 'DENY', 'x-content-type-options': 'nosniff', 'cache-control': 'no-store' });
const json = (res, status, body) => { res.writeHead(status, headers('application/json')); res.end(body === undefined ? '' : JSON.stringify(body)); };
const readBody = req => new Promise(done => { let data = ''; req.on('data', chunk => { data += chunk; }); req.on('end', () => { try { done(data ? JSON.parse(data) : {}); } catch { done({}); } }); });
const sleep = ms => new Promise(done => setTimeout(done, ms));
const signins = new Map();
const grants = flags.has('--empty-grants') ? [] : [...fx.knowledge.grants];
let mockGrantId = 0;

async function asset(res, file, type, replace = {}) {
  let body = await readFile(resolve(root, file), 'utf8');
  for (const [key, value] of Object.entries(replace)) body = body.replaceAll(key, value);
  res.writeHead(200, headers(type)); res.end(body);
}

async function nativeSignin(req, res) {
  const body = await readBody(req);
  const harness = body.harness;
  if (!['agy', 'claude-code', 'codex'].includes(harness)) return json(res, 400, { error: 'Native sign-in supports Google Antigravity, Claude and OpenAI / Codex.' });
  const id = 'sess_' + Math.random().toString(36).slice(2, 12);
  res.writeHead(200, { ...headers('application/x-ndjson'), 'x-accel-buffering': 'no' });
  const send = (kind, text = '') => res.write(JSON.stringify({ kind, text }) + '\n');
  let resolveInput;
  const input = new Promise(done => { resolveInput = done; });
  signins.set(id, resolveInput);
  send('session', id);
  send('prompt', `Preparing an isolated ${harness} sign-in…`);
  await sleep(500);
  const provider = { agy: 'google-antigravity', 'claude-code': 'anthropic', codex: 'openai' }[harness];
  if (harness === 'codex') {
    send('link', 'https://auth.openai.com/codex/device');
    await sleep(200);
    send('code', 'K7QX-M4PD');
    send('prompt', 'Waiting for you to approve the device code…');
    await sleep(Number(process.env.SIGNIN_DONE_MS || 60_000));
  } else {
    const url = harness === 'agy'
      ? 'https://accounts.google.com/o/oauth2/v2/auth?client_id=example.apps.example.test&redirect_uri=http%3A%2F%2Flocalhost%3A51121%2Fcallback&response_type=code&state=st_9f2c&scope=openid%20email'
      : 'https://claude.ai/oauth/authorize?code=true&client_id=example&redirect_uri=https%3A%2F%2Fconsole.example.test%2Foauth%2Fcode%2Fcallback&state=st_41ab';
    send('link', url);
    if (harness === 'claude-code') send('prompt', 'Finish signing in in your browser, then paste the authorization code below.');
    send('input');
    await Promise.race([input, sleep(120_000)]);
    send('prompt', 'Exchanging the authorization code…');
    await sleep(600);
  }
  signins.delete(id);
  send('done', provider + '/' + (body.login_hint || 'new.seat@example.test'));
  res.end();
}

async function knowledge(req, res) {
  if (unavailable.has('knowledge')) return json(res, 404, { error: 'not found' });
  const body = await readBody(req);
  const docs = fx.knowledge.docs;
  const find = () => docs.find(doc => doc.namespace === body.namespace && doc.topic === body.topic);
  switch (body.action) {
    case 'list': return json(res, 200, { items: docs.map(({ namespace, topic, title, description, pinned, version }) => ({ namespace, topic, title, description, pinned, version })), next_offset: null });
    case 'read': return find() ? json(res, 200, find()) : json(res, 404, { error: 'knowledge document not found' });
    case 'revisions': {
      const doc = find();
      if (!doc) return json(res, 404, { error: 'knowledge document not found' });
      const reasons = ['Clarified wording after review', 'Added an example', 'Merged proposal from a sub-agent', 'Fixed a broken link', 'Initial version'];
      const revisions = Array.from({ length: Math.min(doc.version, 6) }, (_, index) => ({ id: doc.id + '_r' + (doc.version - index), version: doc.version - index,
        actor_id: index % 2 ? 'owner' : 'agent:atlas-director', body: doc.body, prior_body: null, reason: doc.version - index === 1 ? 'Initial version' : reasons[index % 4],
        created_at_ms: doc.updated_at_ms - index * 86_400_000, deleted_at_ms: null }));
      return json(res, 200, { revisions, before_version: null });
    }
    case 'rollback': { const doc = find(); doc.version += 1; doc.updated_at_ms = Date.now(); doc.updated_by = 'owner'; return json(res, 200, { namespace: doc.namespace, topic: doc.topic, version: doc.version }); }
    case 'proposals': return json(res, 200, { proposals: fx.knowledge.proposals.filter(proposal => proposal.namespace === body.namespace), after: null });
    case 'promote': case 'reject': {
      const proposal = fx.knowledge.proposals.find(item => item.id === body.proposal_id);
      if (proposal) proposal.status = body.action === 'promote' ? 'promoted' : 'rejected';
      return json(res, 200, { proposal });
    }
    case 'grants': return json(res, 200, { grants, next_after: null });
    case 'grant': {
      const { agent_id, namespace_pattern, mode } = body;
      if (typeof agent_id !== 'string' || !agent_id.startsWith('agt_') || typeof namespace_pattern !== 'string'
        || !namespace_pattern.trim() || !['read', 'write', 'propose'].includes(mode)) return json(res, 400, { error: 'Invalid grant' });
      if (flags.has('--reject-write-grant') && mode === 'write') return json(res, 403, { error: 'Write grants are unavailable in this fixture' });
      const existing = grants.find(grant => grant.agent_id === agent_id && grant.namespace_pattern === namespace_pattern && grant.mode === mode);
      if (existing) return json(res, 200, existing);
      const grant = { id: 'kgrant_mock_' + ++mockGrantId, agent_id, namespace_pattern, mode, granted_by: 'owner', granted_at_ms: Date.now() };
      grants.unshift(grant);
      return json(res, 200, grant);
    }
    case 'revoke': {
      const i = grants.findIndex(grant => grant.id === body.grant_id);
      if (i >= 0) grants.splice(i, 1);
      return json(res, 200, { revoked: i >= 0 });
    }
    default: return json(res, 422, { error: 'unknown action' });
  }
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const path = url.pathname;
  const method = req.method;
  try {
    if (path === '/' || path === '/index.html') return asset(res, 'index.html', 'text/html; charset=utf-8');
    if (path === '/signin.html') return asset(res, 'signin.html', 'text/html; charset=utf-8', { '{{COORDINATOR_URL}}': 'http://localhost:' + port });
    if (path === '/auth_recovery.html') return asset(res, 'auth_recovery.html', 'text/html; charset=utf-8',
      { '{{REASON}}': 'Your sign-in attempt expired before it completed.', '{{LOGIN_URL}}': '/admin/auth/login', '{{HOME_URL}}': '/' });
    if (path === '/assets/console.css') return asset(res, 'console.css', 'text/css; charset=utf-8');
    if (['/assets/console.js', '/assets/secrets.js', '/assets/signin.js', '/assets/profiles.js'].includes(path)) return asset(res, 'console.js', 'application/javascript; charset=utf-8');
    if (path === '/favicon.ico') { res.writeHead(204, headers('image/x-icon')); return res.end(); }

    if (path === '/admin/auth/me') return json(res, 200, { authenticated: true, mode: 'oidc', email: 'operator@example.test', name: 'Operator', serverless: true });
    if (path === '/admin/auth/logout') return json(res, 204);
    if (path === '/admin/api/overview') return json(res, 200, fx.overview());
    if (path === '/admin/api/tick' && method === 'POST') return json(res, 200, { schedules_fired: 3, cloud_turns_polled: 5, turns_settled: 2, stale_open_turns_cleared: 0, backlog_dispatched: 1, leases_renewed: 38, sandboxes_swept: 2 });
    if (path === '/admin/api/runtime-issues' && method === 'GET') {
      if (unavailable.has('issues')) return json(res, 404, { error: 'not found' });
      if (flags.has('--unimplemented-issues')) return json(res, 501, { error: 'not implemented' });
      return json(res, 200, fx.runtimeIssues);
    }
    if (path === '/admin/api/versions') return unavailable.has('versions') ? json(res, 404, { error: 'not found' }) : json(res, 200, fx.versionsReport());
    if (path === '/admin/api/versions/policy' && method === 'PATCH') {
      const body = await readBody(req);
      if (!['auto', 'manual', 'pinned'].includes(body.auto_update_policy)) return json(res, 422, { error: 'invalid policy' });
      fx.versions.coordinator.auto_update_policy = body.auto_update_policy; return json(res, 200, body);
    }
    const drain = /^\/admin\/api\/agents\/([^/]+)\/drain-upgrade$/.exec(path);
    if (drain && method === 'POST') {
      const name = decodeURIComponent(drain[1]);
      const row = fx.versions.agents.find(agent => agent.name === name);
      if (!row) return json(res, 404, { error: 'unknown agent' });
      const action = row.has_open_turn ? 'drain_scheduled' : 'recycled_now';
      row.upgrade_status = row.has_open_turn ? 'outdated_draining' : 'up_to_date'; row.drain_requested = row.has_open_turn;
      if (!row.has_open_turn) row.running_image_digest = row.desired_image_digest;
      return json(res, 200, { agent: name, action });
    }

    if (path === '/v1/agents' && method === 'GET') return json(res, 200, { agents: fx.agents, archived_count: fx.agents.filter(agent => agent.state === 'archived').length });
    if (path === '/v1/agents' && method === 'POST') return json(res, 200, { id: 'agt_new' });
    const agentRoute = /^\/v1\/agents\/([^/]+)(?:\/(.+))?$/.exec(path);
    if (agentRoute) {
      const name = decodeURIComponent(agentRoute[1]);
      const sub = agentRoute[2];
      const agent = fx.byName.get(name);
      if (sub === 'schedules') return method === 'POST' ? json(res, 200, { cancelled: 1 }) : json(res, 200, fx.schedules(name));
      if (!agent) return json(res, 404, { error: 'unknown agent' });
      if (!sub) return json(res, 200, agent);
      if (sub === 'conversation') return json(res, 200, fx.conversation(name));
      if (sub === 'secrets') return method === 'PUT' ? json(res, 200, { ok: true }) : json(res, 200, { secrets: agent.secrets });
      return json(res, 200, { ok: true });
    }
    if (path === '/v1/send') return json(res, 200, { id: 'msg_preview' });
    if (path === '/v1/profiles' && method === 'GET') {
      if (url.searchParams.get('fresh')) await sleep(700);
      return json(res, 200, fx.profiles);
    }
    if (path === '/v1/profiles' && method === 'POST') return json(res, 200, { ok: true });
    if (path === '/v1/profiles/native-signin' && method === 'POST') return nativeSignin(req, res);
    const signinInput = /^\/v1\/profiles\/native-signin\/([^/]+)\/input$/.exec(path);
    if (signinInput) {
      const resolveInput = signins.get(decodeURIComponent(signinInput[1]));
      if (!resolveInput) return json(res, 410, { error: 'This sign-in session is no longer available on this coordinator. Start a new sign-in.' });
      resolveInput(); res.writeHead(204, headers('application/json')); return res.end();
    }
    if (path === '/v1/channels') return json(res, 200, fx.channels);
    if (path === '/v1/groups') return json(res, 200, fx.groups);
    if (path === '/v1/secrets') return json(res, 200, fx.secrets);
    if (path.startsWith('/v1/secrets/')) return json(res, 200, { ok: true });
    if (path === '/v1/outbound') return json(res, 200, fx.outbound);
    if (path.startsWith('/v1/outbound/')) return json(res, 200, { ok: true });
    if (path === '/v1/usage') return json(res, 200, fx.usage(url.searchParams.get('group_by')));
    if (path === '/v1/knowledge' && method === 'POST') return knowledge(req, res);
    return json(res, 404, { error: 'not found' });
  } catch (error) {
    json(res, 500, { error: String(error) });
  }
});
server.listen(port, '127.0.0.1', () => console.log(`Console preview on http://127.0.0.1:${port}  (CSP: ${CSP})${unavailable.size ? '  unavailable: ' + [...unavailable].join(', ') : ''}`));
