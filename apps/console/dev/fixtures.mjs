// Deterministic, dense preview fixtures for the console mock server.
// Every identity here is synthetic (example.test); nothing maps to a real deployment.

const now = Date.now();
const MIN = 60_000, HOUR = 60 * MIN, DAY = 24 * HOUR;

const DIRECTORS = [
  ['atlas-director', 'director', 'claude-code', 'claude-opus-4-1'],
  ['beacon-support', 'support', 'codex', 'gpt-5-codex'],
  ['cobalt-research', 'rd-director', 'claude-code', 'claude-sonnet-4-5'],
  ['delta-release', 'ops-director', 'codex', 'gpt-5-codex'],
  ['ember-design', 'design', 'agy', 'gemini-3-pro'],
  ['fjord-growth', 'growth', 'claude-code', 'claude-sonnet-4-5'],
  ['granite-infra', 'ops-director', 'opencode', 'kimi-k2-turbo'],
  ['harbor-liaison', 'liaison', 'agy', 'gemini-3-flash'],
  ['iris-counsel', 'counsel', 'claude-code', 'claude-opus-4-1'],
  ['juniper-data', 'rd-director', 'codex', 'gpt-5'],
  ['kestrel-qa', 'general', 'opencode', 'kimi-k2-turbo'],
  ['lumen-docs', 'general', 'muse', 'muse-1'],
  ['meridian-ambassador', 'ambassador', 'claude-code', 'claude-sonnet-4-5'],
];

// [name, parent, role, harness, model, state]
const CHILDREN = [
  ['atlas-planner', 'atlas-director', 'general', 'claude-code', 'claude-sonnet-4-5', 'working'],
  ['atlas-reviewer', 'atlas-director', 'general', 'codex', 'gpt-5-codex', 'idle'],
  ['atlas-scribe', 'atlas-director', 'general', 'claude-code', 'claude-haiku-4-5', 'idle'],
  ['atlas-planner-sub1', 'atlas-planner', 'general', 'claude-code', 'claude-sonnet-4-5', 'working'],
  ['atlas-planner-sub2', 'atlas-planner', 'general', 'codex', 'gpt-5-codex', 'errored'],
  ['beacon-triage', 'beacon-support', 'support', 'codex', 'gpt-5-mini', 'working'],
  ['beacon-escalation', 'beacon-support', 'support', 'claude-code', 'claude-sonnet-4-5', 'idle'],
  ['beacon-kb-writer', 'beacon-support', 'general', 'agy', 'gemini-3-flash', 'idle'],
  ['cobalt-benchmarks', 'cobalt-research', 'general', 'codex', 'gpt-5', 'working'],
  ['cobalt-literature', 'cobalt-research', 'general', 'agy', 'gemini-3-pro', 'idle'],
  ['cobalt-prototype', 'cobalt-research', 'general', 'claude-code', 'claude-opus-4-1', 'working'],
  ['cobalt-prototype-ui', 'cobalt-prototype', 'design', 'agy', 'gemini-3-pro', 'idle'],
  ['cobalt-prototype-api', 'cobalt-prototype', 'general', 'codex', 'gpt-5-codex', 'working'],
  ['cobalt-prototype-api-tests', 'cobalt-prototype-api', 'general', 'opencode', 'kimi-k2-turbo', 'idle'],
  ['delta-changelog', 'delta-release', 'general', 'claude-code', 'claude-haiku-4-5', 'idle'],
  ['delta-canary', 'delta-release', 'general', 'codex', 'gpt-5-codex', 'paused'],
  ['delta-rollback-guard', 'delta-release', 'general', 'opencode', 'kimi-k2-turbo', 'idle'],
  ['ember-tokens', 'ember-design', 'design', 'agy', 'gemini-3-pro', 'working'],
  ['ember-illustration', 'ember-design', 'design', 'agy', 'gemini-3-pro', 'idle'],
  ['fjord-seo', 'fjord-growth', 'growth', 'claude-code', 'claude-sonnet-4-5', 'idle'],
  ['fjord-campaigns', 'fjord-growth', 'growth', 'codex', 'gpt-5-mini', 'idle'],
  ['fjord-analytics', 'fjord-growth', 'general', 'codex', 'gpt-5', 'errored'],
  ['fjord-campaigns-email', 'fjord-campaigns', 'growth', 'claude-code', 'claude-haiku-4-5', 'idle'],
  ['granite-patrol', 'granite-infra', 'ops-director', 'opencode', 'kimi-k2-turbo', 'working'],
  ['granite-dns', 'granite-infra', 'general', 'codex', 'gpt-5-mini', 'idle'],
  ['granite-costs', 'granite-infra', 'general', 'opencode', 'kimi-k2-turbo', 'idle'],
  ['granite-patrol-disk', 'granite-patrol', 'general', 'opencode', 'kimi-k2-turbo', 'idle'],
  ['granite-patrol-certs', 'granite-patrol', 'general', 'opencode', 'kimi-k2-turbo', 'errored'],
  ['harbor-partners', 'harbor-liaison', 'liaison', 'agy', 'gemini-3-flash', 'idle'],
  ['harbor-inbox', 'harbor-liaison', 'liaison', 'agy', 'gemini-3-flash', 'working'],
  ['iris-contracts', 'iris-counsel', 'counsel', 'claude-code', 'claude-opus-4-1', 'idle'],
  ['iris-licenses', 'iris-counsel', 'counsel', 'claude-code', 'claude-sonnet-4-5', 'idle'],
  ['juniper-etl', 'juniper-data', 'general', 'codex', 'gpt-5-codex', 'working'],
  ['juniper-dashboards', 'juniper-data', 'general', 'codex', 'gpt-5', 'idle'],
  ['juniper-etl-backfill', 'juniper-etl', 'general', 'opencode', 'kimi-k2-turbo', 'errored'],
  ['juniper-etl-validate', 'juniper-etl', 'general', 'codex', 'gpt-5-mini', 'idle'],
  ['kestrel-e2e', 'kestrel-qa', 'general', 'opencode', 'kimi-k2-turbo', 'idle'],
  ['kestrel-visual', 'kestrel-qa', 'design', 'agy', 'gemini-3-pro', 'idle'],
  ['kestrel-fuzz', 'kestrel-qa', 'general', 'codex', 'gpt-5-codex', 'paused'],
  ['lumen-api-docs', 'lumen-docs', 'general', 'muse', 'muse-1', 'idle'],
  ['lumen-tutorials', 'lumen-docs', 'general', 'claude-code', 'claude-sonnet-4-5', 'idle'],
  ['meridian-events', 'meridian-ambassador', 'ambassador', 'claude-code', 'claude-sonnet-4-5', 'idle'],
  ['meridian-social', 'meridian-ambassador', 'growth', 'agy', 'gemini-3-flash', 'idle'],
  ['meridian-community', 'meridian-ambassador', 'support', 'codex', 'gpt-5-mini', 'idle'],
  ['atlas-reviewer-security', 'atlas-reviewer', 'counsel', 'claude-code', 'claude-opus-4-1', 'idle'],
  ['beacon-triage-locale', 'beacon-triage', 'support', 'agy', 'gemini-3-flash', 'idle'],
];

const ERRORS = {
  'atlas-planner-sub2': 'Harness exited with status 137 during tool call `apply_patch` (out of memory).',
  'fjord-analytics': 'Provider returned 401: OAuth credential for openai/analytics-seat expired.',
  'granite-patrol-certs': 'Sandbox lease lost while renewing; retry budget exhausted after 3 attempts.',
  'juniper-etl-backfill': 'Turn faulted: upstream warehouse rejected the query (statement timeout 300s).',
};

let seq = 0;
const id = name => 'agt_' + name.replace(/[^a-z0-9]/g, '').slice(0, 14).padEnd(14, '0') + String(++seq).padStart(3, '0');

function makeAgent(name, parent, role, harness, model, state) {
  const working = state === 'working';
  return {
    id: id(name), name, parent, role, harness, model, scope: parent ? 'siblings' : 'public', lifecycle: parent ? 'task' : 'standing',
    runtime: harness === 'muse' ? 'local' : 'sandbox:sylphx', profile: undefined,
    state: state === 'paused' ? 'paused' : 'running', process: state === 'paused' ? 'stopped' : 'running',
    turn_state: working ? 'in_turn' : state === 'errored' ? 'faulted' : 'idle', has_open_turn: working,
    open_turn_age_ms: working ? (seq * 37_000) % (42 * MIN) + 20_000 : undefined,
    last_error: ERRORS[name], turn_errors: ERRORS[name] ? { faulted_at_ms: now - 14 * MIN, last_error: ERRORS[name] } : undefined,
    description: parent ? `Delegated by ${parent}.` : `Top-level ${role.replace('-', ' ')} coordinating its own branch of work.`,
    workdir: '/workspace/' + name, created_at_ms: now - ((seq * 7919) % 40) * DAY - DAY,
    automations: { total: (seq % 4 === 0) ? 2 : seq % 3 === 0 ? 1 : 0 }, secrets: seq % 5 === 0 ? ['deploy-tokens'] : [],
    remote_budget_micros: seq % 6 === 0 ? 25_000_000 : undefined,
  };
}

export const agents = [
  ...DIRECTORS.map(([name, role, harness, model], index) => makeAgent(name, undefined, role, harness, model, [0, 2, 3, 6, 9].includes(index) ? 'working' : 'idle')),
  ...CHILDREN.map(([name, parent, role, harness, model, state]) => makeAgent(name, parent, role, harness, model, state)),
  ...['atlas-spike-old', 'cobalt-retired', 'fjord-q3-campaign'].map(name => ({ ...makeAgent(name, name.split('-')[0] === 'atlas' ? 'atlas-director' : undefined, 'general', 'codex', 'gpt-5-mini', 'idle'), state: 'archived', process: 'stopped' })),
];
export const byName = new Map(agents.map(agent => [agent.name, agent]));

const accounts = [
  // provider, slot, account, usage, assigned
  ['google-antigravity', 'acct-01', 'atlas.ops@example.test', '5h: 92% left ↻3h41m · 7d: 71% left ↻5d02h (ultra)', ['ember-design', 'ember-tokens']],
  ['google-antigravity', 'acct-02', 'cobalt.lab@example.test', '5h: 64% left ↻1h12m · 7d: 48% left ↻2d19h (ultra)', ['cobalt-literature']],
  ['google-antigravity', 'acct-03', 'harbor.desk@example.test', 'oauth expired', ['harbor-liaison', 'harbor-inbox']],
  ['google-antigravity', 'acct-04', 'kestrel.visual@example.test', '5h: 88% left · 7d: 80% left (pro)', ['kestrel-visual']],
  ['google-antigravity', 'acct-05', 'meridian.social@example.test', 'oauth expired · 5h: 40% left · 7d: 33% left (pro)', ['meridian-social']],
  ['google-antigravity', 'acct-06', 'beacon.kb@example.test', '5h: 100% left · 7d: 97% left (pro)', ['beacon-kb-writer']],
  ['google-antigravity', 'acct-07', 'spare.seat-07@example.test', 'oauth expired', []],
  ['google-antigravity', 'acct-08', 'locale.team@example.test', '5h: 55% left ↻2h05m · 7d: 61% left (pro)', ['beacon-triage-locale']],
  ['anthropic', 'max-01', 'atlas.director@example.test', '5h: 84% left ↻1h02m · 7d: 33% left ↻4d11h · opus: 0% left ↻4d11h (max)', ['atlas-director', 'atlas-planner']],
  ['anthropic', 'max-02', 'counsel.desk@example.test', '5h: 76% left ↻3h50m · 7d: 58% left · opus: 44% left (max)', ['iris-counsel', 'iris-contracts']],
  ['anthropic', 'max-03', 'research.pod@example.test', '5h: 12% left ↻0h48m · 7d: 35% left · opus: 22% left (max)', ['cobalt-prototype']],
  ['anthropic', 'pro-04', 'growth.writer@example.test', 'oauth expired', ['fjord-growth', 'fjord-seo']],
  ['anthropic', 'pro-05', 'docs.tutorials@example.test', '5h: 81% left · 7d: 70% left (pro)', ['lumen-tutorials']],
  ['anthropic', 'max-06', 'ambassador.hq@example.test', 'oauth expired · 5h: 66% left · 7d: 41% left (max)', ['meridian-ambassador']],
  ['anthropic', 'pro-07', 'escalations@example.test', 'oauth expired', ['beacon-escalation']],
  ['openai', 'plus-01', 'release.captain@example.test', '5h: 82% left ↻3h10m · 7d: 40% left ↻3d08h (pro)', ['delta-release', 'delta-canary']],
  ['openai', 'plus-02', 'triage.bot@example.test', '5h: 95% left · 7d: 88% left (plus)', ['beacon-triage']],
  ['openai', 'plus-03', 'analytics.seat@example.test', 'oauth expired', ['fjord-analytics']],
  ['openai', 'plus-04', 'etl.runner@example.test', '5h: 61% left · 7d: 8% left ↻1d02h (pro)', ['juniper-etl', 'juniper-data']],
  ['openai', 'plus-05', 'benchmarks@example.test', '5h: 70% left ↻2h31m · 7d: 66% left (pro)', ['cobalt-benchmarks', 'cobalt-prototype-api']],
  ['openai', 'plus-06', 'review.seat@example.test', 'oauth expired · 5h: 100% left', ['atlas-reviewer']],
  ['kimi-code-plan', 'patrol-key', null, '5h: 82% left ↻4h02m · mo: 37% left ↻12d', ['granite-infra', 'granite-patrol']],
  ['kimi-code-plan', 'qa-key', null, 'mo: 0% left ($0.00/$19 moderato)', ['kestrel-qa', 'kestrel-e2e']],
  ['kimi-code-plan', 'etl-key', null, '5h: 74% left · mo: 52% left', ['juniper-etl-backfill']],
  ['kimi-code-plan', 'spare-key', null, undefined, []],
  ['sylphx', 'team-key', null, '$12.40 left ($88 / $100)', ['lumen-docs', 'lumen-api-docs']],
  ['sylphx', 'ci-key', null, 'active (12 models)', ['delta-rollback-guard']],
  ['sylphx', 'sandbox-key', null, undefined, []],
];
const HARNESSES = { 'google-antigravity': ['agy'], anthropic: ['claude-code', 'claude-cloud'], openai: ['codex', 'openai-cloud'], 'kimi-code-plan': ['opencode', 'omp'], sylphx: ['opencode', 'omp', 'claude-code', 'codex'] };
export const profiles = accounts.map(([provider, profile, account, usage, assigned]) => ({
  provider, profile, ...(account ? { account } : {}), kind: ['google-antigravity', 'anthropic', 'openai'].includes(provider) ? 'subscription' : 'api-key',
  compatible_harnesses: HARNESSES[provider], assigned_agents: assigned, subagent_count: assigned.length ? (assigned.length * 3) % 7 : 0,
  ...(usage ? { usage } : {}), usage_age_ms: 2 * MIN + 14_000,
}));

export function overview() {
  const agents_meta = {};
  for (const agent of agents) {
    agents_meta[agent.id] = {
      unread_count: ['atlas-director', 'harbor-inbox', 'beacon-triage'].includes(agent.name) ? 3 : 0,
      sandbox_lease: agent.runtime.startsWith('sandbox') && agent.state !== 'archived'
        ? { provider: 'sylphx', lease_state: agent.has_open_turn ? 'busy' : 'ready', lease_id: 'lease_' + agent.id.slice(4, 12), lease_expires_at_ms: now + 20 * MIN, last_heartbeat_ms: now - 25_000 } : null,
    };
  }
  return {
    agents_meta, serverless: true, sandbox_provider: 'sylphx', active_sandboxes_count: 41,
    timer_token_configured: true, timer_jwks_configured: true, timer_scheduler_configured: true,
    active_remote_turns: agents.filter(agent => agent.has_open_turn).slice(0, 4).map((agent, index) => ({
      turn_id: 'turn_' + agent.id.slice(4, 14), agent_id: agent.id, agent_name: agent.name, harness_kind: agent.harness, provider_kind: 'sylphx', status: 'running', last_event_at_ms: now - (index + 1) * 9000 })),
  };
}

export const versions = {
  coordinator: { version: '0.42.0', git_sha: '9c63f727e1d4b2a8c0f5e6d7a8b9c0d1e2f3a4b5', image_digest: 'sha256:5f1c2a9be08d7e44c3a1f09b2d6e7c8a91b0c2d3e4f5a6b7c8d9e0f1a2b3c4d5',
    latest_available: '0.43.0', auto_update_policy: 'manual' },
  runtime: { desired_sandbox_image: 'ghcr.io/example/anyagents-sandbox:0.43.0', desired_image_digest: 'sha256:a3e1f0c97b2d4e58a6c1b0f9e8d7c6b5a4f3e2d1c0b9a8f7e6d5c4b3a2f1e0d9',
    desired_harness_versions: { agy: '1.14.2', 'claude-code': '2.0.31', codex: '0.58.0', opencode: '0.15.8' }, tpl_base_version: '2026.11.3', sbx_guest_version: '0.42.0' },
  agents: agents.filter(agent => agent.state !== 'archived' && agent.runtime.startsWith('sandbox')).map((agent, index) => {
    const status = index % 11 === 3 || index % 13 === 5 ? 'outdated_idle' : index % 14 === 7 ? 'outdated_draining' : index % 9 === 4 ? 'cold_uses_latest' : 'up_to_date';
    const desired = 'sha256:a3e1f0c97b2d4e58a6c1b0f9e8d7c6b5a4f3e2d1c0b9a8f7e6d5c4b3a2f1e0d9';
    return { id: agent.id, name: agent.name, state: agent.state === 'paused' ? 'paused' : 'idle', has_open_turn: agent.has_open_turn, harness: agent.harness,
      running_image_digest: status === 'cold_uses_latest' ? null : status.startsWith('outdated') ? 'sha256:71bd09e4c2a6f3d8e5b1c0a9f8e7d6c5b4a3f2e1d0c9b8a7f6e5d4c3b2a1f0e9' : desired,
      desired_image_digest: desired, harness_version: { agy: '1.14.2', 'claude-code': '2.0.31', codex: '0.58.0', opencode: '0.15.8' }[agent.harness] ?? null,
      tpl_base_version: '2026.11.3', sbx_guest_version: '0.42.0', upgrade_status: status, drain_requested: status === 'outdated_draining' };
  }),
};
export function versionsReport() {
  const list = versions.agents;
  const count = status => list.filter(agent => agent.upgrade_status === status).length;
  return { ...versions, summary: { total: list.length, up_to_date: count('up_to_date'), outdated: count('outdated_idle') + count('outdated_draining'), draining: count('outdated_draining'), cold: count('cold_uses_latest') } };
}

export function conversation(name) {
  const agent = byName.get(name);
  const parent = agent?.parent ?? 'operator';
  const turns = [
    { from: parent, to: name, direction: 'in', text: `Please pick up the next item in the ${name.split('-')[0]} queue and report back with a short status when it is done.` },
    { from: name, to: parent, direction: 'out', text: 'Acknowledged. Reading the queue and the latest notes now.', tool_names: ['memory.read', 'inbox.list'] },
    { from: name, to: parent, direction: 'out', text: 'Found three open items. Starting with the one blocking the release:\n\n```bash\nnpm run test -- --filter=release\n```\nTwo suites fail on a stale snapshot; refreshing it and re-running.', tool_names: ['shell', 'apply_patch', 'shell'] },
    { from: 'operator', to: name, direction: 'in', text: 'Keep the snapshot change in its own commit so it is easy to review.' },
    ...(agent && !agent.parent ? ['planner', 'reviewer', 'scribe'].map(suffix => ({ from: name, to: 'agent:' + name.split('-')[0] + '-' + suffix, direction: 'out', text: 'Heads up: the release branch is frozen at 18:00 UTC. Land or park your open changes before then.', sameTime: true })) : []),
    { from: name, to: 'operator', direction: 'out', text: agent?.last_error ? 'Hit an error while applying the follow-up change; details are in the last error above. Waiting for guidance.' : 'Done — snapshot refresh is isolated in its own commit, and all suites pass locally. Opening the review now.', tool_names: ['git.commit', 'gh.pr_create'] },
  ];
  let stamp = now - turns.length * 7 * MIN;
  return { turns: turns.map((turn, index) => { const { sameTime, ...rest } = turn; if (!sameTime || !turns[index - 1]?.sameTime) stamp += 7 * MIN; return { seq: index + 1, id: 'msg_' + index, timestamp_ms: stamp, ...rest }; }) };
}

export function schedules(name) {
  const make = (agent, index) => [
    { id: 'sch_' + agent.id.slice(4, 12) + 'a', agent_id: agent.id, agent_name: agent.name, kind: 'every', spec: index % 2 ? '30m' : '6h', state: 'active',
      prompt: 'Run the routine health patrol and summarize anything unusual.', next_fire_at_ms: now + (index + 1) * 11 * MIN, last_fired_at_ms: now - 19 * MIN },
    { id: 'sch_' + agent.id.slice(4, 12) + 'b', agent_id: agent.id, agent_name: agent.name, kind: 'watch', spec: 'webhook:deploy.finished', state: 'active',
      prompt: 'A deploy finished — verify the release notes and smoke tests.', next_fire_at_ms: null },
  ].slice(0, agent.automations.total || 0);
  const owners = name === 'all' ? agents.filter(agent => agent.state !== 'archived' && agent.automations.total) : [byName.get(name)].filter(Boolean);
  return { agent: name, schedules: owners.flatMap(make) };
}

export const channels = { channels: [
  { id: 'tg-ops-room', kind: 'telegram', bot: 'ops_helper_bot', agents: ['granite-infra', 'granite-patrol'] },
  { id: 'tg-support', kind: 'telegram', bot: 'support_desk_bot', agents: ['beacon-support', 'beacon-triage', 'beacon-escalation'] },
  { id: 'webhook-releases', kind: 'webhook', bot: null, agents: ['delta-release'] },
  { id: 'tg-partners', kind: 'telegram', bot: 'partner_desk_bot', agents: ['harbor-liaison'] },
] };
const state = name => { const agent = byName.get(name); return agent?.has_open_turn ? 'working' : agent?.last_error ? 'faulted' : agent?.state === 'paused' ? 'paused' : 'idle'; };
export const groups = { groups: [
  { name: 'release-council', public: false, members: ['delta-release', 'atlas-director', 'kestrel-qa', 'iris-counsel'] },
  { name: 'research-sync', public: true, members: ['cobalt-research', 'juniper-data', 'cobalt-benchmarks'] },
  { name: 'support-floor', public: false, members: ['beacon-support', 'beacon-triage', 'beacon-escalation', 'beacon-kb-writer', 'harbor-inbox'] },
].map(group => ({ ...group, roster: group.members.map(name => ({ id: byName.get(name).id, name, harness: byName.get(name).harness, state: state(name) })) })) };

export const secrets = { secrets: [
  { name: 'deploy-tokens', env_keys: ['REGISTRY_TOKEN', 'DEPLOY_HOOK_SECRET'], granted_agents: ['delta-release', 'delta-canary', 'granite-infra', 'atlas-director'], updated_at_ms: now - 3 * DAY },
  { name: 'warehouse-ro', env_keys: ['WAREHOUSE_DSN'], granted_agents: ['juniper-data', 'juniper-etl'], updated_at_ms: now - 9 * DAY },
  { name: 'support-crm', env_keys: ['CRM_API_KEY', 'CRM_WORKSPACE'], granted_agents: ['beacon-support'], updated_at_ms: now - 26 * HOUR },
  { name: 'design-assets', env_keys: ['ASSET_BUCKET', 'ASSET_SIGNING_KEY'], granted_agents: [], updated_at_ms: now - 40 * DAY },
] };

export const outbound = { unsettled: [
  { key: 'out_7f3a91', agent_id: byName.get('harbor-inbox').id, destination: 'telegram:tg-partners', since_ms: now - 42 * MIN, error: 'Bot was blocked by the user (403).', excerpt: 'Thanks for the update — I have shared the revised timeline with the team…' },
  { key: 'out_2c81d4', agent_id: byName.get('beacon-triage').id, destination: 'telegram:tg-support', since_ms: now - 8 * MIN, error: 'Rate limited; retry after 30s.', excerpt: 'Ticket #4821 is resolved. The fix ships in tonight’s release.' },
] };

export function usage(groupBy) {
  const live = agents.filter(agent => agent.state !== 'archived');
  const buckets = live.slice(0, 18).map((agent, index) => {
    const scale = (18 - index) * 1.0;
    return { agent_id: agent.id, agent_name: agent.name, ...(groupBy === 'model' ? { model: agent.model } : {}), turns: Math.round(12 * scale + index),
      input_tokens: Math.round(410_000 * scale), output_tokens: Math.round(52_000 * scale), cache_read_tokens: Math.round(1_900_000 * scale),
      cache_write_tokens: Math.round(160_000 * scale), reported_cost_micros: agent.harness === 'opencode' ? Math.round(1_350_000 * scale) : null };
  });
  const sum = key => buckets.reduce((total, bucket) => total + (bucket[key] ?? 0), 0);
  return { totals: { turns: sum('turns'), input_tokens: sum('input_tokens'), output_tokens: sum('output_tokens'), cache_read_tokens: sum('cache_read_tokens'),
    cache_write_tokens: sum('cache_write_tokens'), reported_cost_micros: sum('reported_cost_micros') }, buckets };
}

const docs = [
  ['org', 'instructions', 'Organization instructions', 'Read on every wake by authorized agents.', true, 7,
    '# How we work\n\nEvery agent reads this page at the start of each turn.\n\n## Principles\n\n- Verify against real execution before reporting **done**.\n- Keep changes small, reviewable, and reversible.\n- Ask your parent when a decision changes scope.\n\n## Escalation\n\n1. Retry once with a narrower step.\n2. Report the failing command and its output.\n3. Wait for guidance instead of guessing.\n\n> Quality beats speed when the two conflict.'],
  ['org', 'glossary', 'Glossary', 'Shared vocabulary for agents and operators.', false, 3, '## Terms\n\n- **Director** — a top-level agent that owns a branch of work.\n- **Lease** — a sandbox reservation bound to one agent.\n- **Drain** — finish the current turn, then recycle the sandbox.'],
  ['org', 'release-checklist', 'Release checklist', 'Steps before tagging a release.', true, 12, '## Before tagging\n\n- [ ] CI is green on `main`\n- [ ] Changelog reviewed\n- [ ] Canary healthy for 30 minutes\n\nRun `anyagents versions` to confirm every sandbox is current.'],
  ['ops', 'on-call', 'On-call runbook', 'What to do when patrol pages.', false, 5, '## First five minutes\n\n1. Check the Overview page for errored agents.\n2. Look at held outbound deliveries.\n3. Drain outdated sandboxes only after the incident.'],
  ['ops', 'cost-guardrails', 'Cost guardrails', 'Budget caps per team.', false, 2, 'Each director has a monthly cap. Agents above **80%** of cap should switch to a smaller model.'],
  ['research', 'benchmark-protocol', 'Benchmark protocol', 'How benchmark runs are recorded.', false, 4, '## Protocol\n\nRecord the model, harness version, and seed for every run. Store raw results under `research/benchmarks`.'],
  ['research', 'reading-list', 'Reading list', 'Papers the research pod is tracking.', false, 9, '- Retrieval-augmented planning\n- Long-horizon tool use\n- Evaluation of agent hierarchies'],
  ['support', 'tone-guide', 'Support tone guide', 'Voice and tone for customer replies.', true, 6, '## Voice\n\nWarm, direct, and specific. Lead with the answer, then the context. Never promise dates we cannot keep.'],
  ['support', 'refund-policy', 'Refund policy', 'When agents may approve refunds.', false, 3, 'Agents may approve refunds up to the plan price within 14 days. Anything larger goes to a human.'],
  ['agent/cobalt-research', 'notes', 'Research notes', 'Private scratchpad for cobalt-research.', false, 21, 'Working notes. Promote stable findings to `research/` via a proposal.'],
];
export const knowledge = {
  docs: docs.map(([namespace, topic, title, description, pinned, version, body], index) => ({
    id: 'kdoc_' + index, namespace, topic, title, description, pinned, version, body, updated_by: index % 3 ? 'agent:atlas-director' : 'owner',
    created_at_ms: now - (30 + index) * DAY, updated_at_ms: now - (index + 1) * 5 * HOUR })),
  proposals: [
    { id: 'kprop_01', namespace: 'org', topic: 'release-checklist', actor_id: 'agent:delta-release', expected_version: 12, created_at_ms: now - 3 * HOUR, status: 'pending',
      edit: { title: 'Release checklist', reason: 'Add the canary soak duration that we agreed on last week.' } },
    { id: 'kprop_02', namespace: 'support', topic: 'tone-guide', actor_id: 'agent:beacon-support', expected_version: 6, created_at_ms: now - 26 * HOUR, status: 'pending',
      edit: { reason: 'Clarify how to respond to outage reports.' } },
  ],
  grants: [
    { id: 'kgr_01', agent_id: byName.get('atlas-director').id, namespace_pattern: 'org*', mode: 'write', granted_by: 'owner', granted_at_ms: now - 20 * DAY },
    { id: 'kgr_02', agent_id: byName.get('beacon-support').id, namespace_pattern: 'support', mode: 'write', granted_by: 'owner', granted_at_ms: now - 12 * DAY },
    { id: 'kgr_03', agent_id: byName.get('cobalt-research').id, namespace_pattern: 'research', mode: 'read', granted_by: 'owner', granted_at_ms: now - 6 * DAY },
  ],
};

const leaseIssue = {
  id: 'rti_fixture_01', error_fingerprint: 'fixture-sandbox-lease-renewals', component: 'sandbox', summary: 'Sandbox lease renewals are flapping',
  occurrence_count: 7, source: 'auto_capture', status: 'open', first_seen_at: now - DAY, last_seen_at: now - 9 * MIN,
  exact_error: 'Sandbox lease renewal failed', repro_steps: 'Automatically captured; reproduction steps unavailable.',
  sample_agent_id: byName.get('granite-patrol-certs').id,
};
export const runtimeIssues = { issues: [
  leaseIssue,
  { ...leaseIssue, occurrence_count: 3, last_seen_at: now - 14 * MIN },
  { id: 'rti_fixture_02', error_fingerprint: 'fixture-knowledge-index-timeout', component: 'coordinator', summary: 'Index refresh exceeded its deadline',
    occurrence_count: 2, source: 'agent_report', status: 'resolved', first_seen_at: now - 2 * DAY, last_seen_at: now - DAY,
    exact_error: 'Index refresh deadline exceeded', repro_steps: 'Refresh the index.',
    sample_agent_id: byName.get('cobalt-research').id },
] };
