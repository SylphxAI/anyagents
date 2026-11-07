import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, rmSync, chmodSync } from 'node:fs';
import { execFileSync, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { increment, isVersionCut, nextVersion, shippedPath, updateManifests } from '../scripts/release-cut.mjs';

test('a cut is recognized only when its subject and committed version change agree', () => {
  assert.equal(isVersionCut('release: v0.1.2 (#123)', '0.1.2', '0.1.1'), true);
  assert.equal(isVersionCut('release: v0.1.2', '0.1.2', '0.1.1'), true);
  assert.equal(isVersionCut('release: v0.1.2', '0.1.1', '0.1.1'), false);
  assert.equal(isVersionCut('release: v0.1.2', '0.1.2', '0.1.2'), false);
  assert.equal(isVersionCut('feat: change source', '0.1.2', '0.1.1'), false);
});

test('patch is the default, and superseded cuts never reuse the committed version', () => {
  assert.equal(nextVersion('0.1.1', '0.1.1'), '0.1.2');
  assert.equal(nextVersion('0.1.2', '0.1.1'), '0.1.3');
  assert.equal(nextVersion('0.1.1', '0.2.0'), '0.2.1');
  assert.equal(nextVersion('0.1.9', '0.1.1', 'minor'), '0.2.0');
  assert.equal(nextVersion('0.1.9', '0.1.1', 'major'), '1.0.0');
  assert.throws(() => nextVersion('0.1.1-beta', '0.1.1'));
  assert.throws(() => nextVersion('0.1.1', 'v0.1.1'));
  assert.throws(() => nextVersion('0.1.1', '0.1.1', 'arbitrary'));
});

test('only merged PRs in the unreleased first-parent population raise the increment', () => {
  const pulls = [
    { merged_at: '2026-11-07T00:00:00Z', merge_commit_sha: 'new', labels: [{ name: 'minor' }] },
    { merged_at: '2026-11-01T00:00:00Z', merge_commit_sha: 'old', labels: [{ name: 'major' }] },
    { merged_at: null, merge_commit_sha: 'pending', labels: [{ name: 'major' }] },
  ];
  assert.equal(increment(pulls, new Set(['new', 'pending'])), 'minor');
  assert.equal(increment(pulls, new Set(['new', 'old'])), 'major');
  assert.equal(increment(pulls, new Set(['other'])), 'patch');
});

test('docs-only and CI-only changes do not create a cut, shipped sources do', () => {
  for (const path of ['README.md', 'docs/architecture.md', '.github/workflows/ci.yml', 'tests/foo.rs', 'scripts/release-cut.mjs', 'LICENSE']) {
    assert.equal(shippedPath(path), false, path);
  }
  for (const path of ['Cargo.toml', 'Cargo.lock', 'crates/anyagents/src/main.rs', 'crates/anyagents/web/app.js',
    'crates/anyagents/build.rs', '.cargo/config.toml', 'rust-toolchain.toml', 'packages/anyagents/bin/anyagents.js', 'packages/npm/linux-x64-gnu/package.json']) {
    assert.equal(shippedPath(path), true, path);
  }
});

test('all five npm manifests and the workspace version change together without other edits', () => {
  const root = mkdtempSync(join(tmpdir(), 'release-cut-'));
  try {
    writeFileSync(join(root, 'Cargo.toml'), '[workspace.package]\nversion = "0.1.1"\nedition = "2024"\n');
    mkdirSync(join(root, 'packages/anyagents'), { recursive: true });
    const natives = ['darwin-arm64', 'darwin-x64', 'linux-arm64-gnu', 'linux-x64-gnu'];
    const optionalDependencies = {};
    for (const platform of natives) {
      mkdirSync(join(root, 'packages/npm', platform), { recursive: true });
      const name = `@sylphx/anyagents-${platform}`;
      optionalDependencies[name] = '0.1.1';
      writeFileSync(join(root, 'packages/npm', platform, 'package.json'), JSON.stringify({ name, version: '0.1.1', os: ['test'] }));
    }
    writeFileSync(join(root, 'packages/anyagents/package.json'), JSON.stringify({ name: '@sylphx/anyagents', version: '0.1.1', optionalDependencies, bin: { anyagents: 'bin/anyagents.js' } }));
    updateManifests('0.1.2', root);
    assert.equal(readFileSync(join(root, 'Cargo.toml'), 'utf8'), '[workspace.package]\nversion = "0.1.2"\nedition = "2024"\n');
    const launcher = JSON.parse(readFileSync(join(root, 'packages/anyagents/package.json')));
    assert.equal(launcher.version, '0.1.2');
    assert.deepEqual(Object.values(launcher.optionalDependencies), ['0.1.2', '0.1.2', '0.1.2', '0.1.2']);
    assert.deepEqual(launcher.bin, { anyagents: 'bin/anyagents.js' });
    for (const platform of natives) {
      const native = JSON.parse(readFileSync(join(root, 'packages/npm', platform, 'package.json')));
      assert.equal(native.version, '0.1.2');
      assert.deepEqual(native.os, ['test']);
    }
    assert.throws(() => updateManifests('broken', root));
  } finally { rmSync(root, { recursive: true, force: true }); }
});

// Real Git history and pushes, isolated provider boundaries. The Cargo double
// regenerates only the tiny fixture lock; resolver behavior is checked separately
// against the real workspace. No test reaches GitHub or npm.
function fixture() {
  const root = mkdtempSync(join(tmpdir(), 'release-cut-git-'));
  const author = join(root, 'author'), work = join(root, 'work'), remote = join(root, 'remote.git'), bin = join(root, 'bin');
  mkdirSync(author); mkdirSync(bin);
  const git = (cwd, ...args) => execFileSync('git', args, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'],
    env: { ...process.env, GIT_AUTHOR_NAME: 'Kyle Tse', GIT_AUTHOR_EMAIL: 'hi@kylet.se', GIT_COMMITTER_NAME: 'Kyle Tse', GIT_COMMITTER_EMAIL: 'hi@kylet.se' } }).trim();
  git(root, 'init', '--bare', remote);
  git(author, 'init', '-b', 'main');
  const write = (path, value) => { mkdirSync(join(author, path, '..'), { recursive: true }); writeFileSync(join(author, path), value); };
  write('Cargo.toml', '[workspace.package]\nversion = "0.1.1"\n');
  write('Cargo.lock', 'version = "0.1.1"\n');
  write('crates/anyagents/src/main.rs', '// released\n');
  const natives = ['darwin-arm64', 'darwin-x64', 'linux-arm64-gnu', 'linux-x64-gnu'];
  write('packages/anyagents/package.json', JSON.stringify({ name: '@sylphx/anyagents', version: '0.1.1', optionalDependencies: Object.fromEntries(natives.map((p) => [`@sylphx/anyagents-${p}`, '0.1.1'])) }));
  for (const platform of natives) write(`packages/npm/${platform}/package.json`, JSON.stringify({ name: `@sylphx/anyagents-${platform}`, version: '0.1.1' }));
  git(author, 'add', '.'); git(author, 'commit', '-m', 'initial'); git(author, 'tag', 'v0.1.1');
  git(author, 'remote', 'add', 'origin', remote); git(author, 'push', 'origin', 'main', '--tags');
  git(root, 'clone', '-b', 'main', remote, work);
  const state = join(root, 'provider.json'); writeFileSync(state, JSON.stringify({ open: null, calls: [] }));
  const gh = `#!/usr/bin/env node
const fs=require('node:fs'), cp=require('node:child_process');
const a=process.argv.slice(2), p=process.env.PROVIDER_STATE, s=JSON.parse(fs.readFileSync(p));
s.calls.push(a);
const g=(...v)=>cp.execFileSync('git',v,{encoding:'utf8'}).trim();
const opt=(key)=>a[a.indexOf(key)+1];
let value;
if(a[0]==='api') {
 const path=a.at(-1);
 if(path.includes('/releases?')) value=[[{tag_name:'v0.1.1',draft:false,prerelease:false}]];
 else if(path.includes('/pulls?')) value=[[]];
 else if(path.endsWith('/git/ref/heads/main')) value={object:{sha:cp.execFileSync('git',['--git-dir',process.env.FIXTURE_REMOTE,'rev-parse','main'],{encoding:'utf8'}).trim()}};
 else throw Error('unexpected api '+path);
} else if(a[0]==='pr' && a[1]==='list') value=s.open ? [s.open] : [];
else if(a[0]==='pr' && (a[1]==='create'||a[1]==='edit')) {
 s.open={number:10,headRefOid:g('rev-parse','HEAD'),title:opt('--title'),autoMergeRequest:s.open?.autoMergeRequest||null,state:'OPEN'};
 value='https://example.invalid/pull/10';
} else if(a[0]==='pr' && a[1]==='view') value=s.open;
else if(a[0]==='pr' && a[1]==='merge') { if(opt('--match-head-commit')!==s.open.headRefOid) throw Error('unpinned merge'); s.open.autoMergeRequest={mergeMethod:'SQUASH'}; }
else throw Error('unexpected gh '+JSON.stringify(a));
fs.writeFileSync(p,JSON.stringify(s));
if(value!==undefined) console.log(typeof value==='string'?value:JSON.stringify(value));
`;
  writeFileSync(join(bin, 'gh'), gh); chmodSync(join(bin, 'gh'), 0o755);
  writeFileSync(join(bin, 'npm'), '#!/usr/bin/env node\nconsole.log(JSON.stringify({error:{code:process.env.NPM_ERROR||"E404"}}));process.exit(1);\n'); chmodSync(join(bin, 'npm'), 0o755);
  writeFileSync(join(bin, 'cargo'), '#!/usr/bin/env node\nconst fs=require("node:fs");fs.writeFileSync("Cargo.lock",fs.readFileSync("Cargo.toml","utf8").match(/version = "[^"]+"/)[0]+"\\n");\n'); chmodSync(join(bin, 'cargo'), 0o755);
  const run = (extra = {}) => spawnSync(process.execPath, [fileURLToPath(new URL('../scripts/release-cut.mjs', import.meta.url))], {
    cwd: work, encoding: 'utf8', env: { ...process.env, PATH: `${bin}:${process.env.PATH}`, GITHUB_REPOSITORY: 'SylphxAI/anyagents', GH_TOKEN: 'fixture-not-a-credential', PROVIDER_STATE: state, FIXTURE_REMOTE: remote, ...extra },
  });
  const advance = (path, contents, subject = 'change shipped source') => {
    write(path, contents); git(author, 'add', '.'); git(author, 'commit', '-m', subject); git(author, 'push', 'origin', 'main');
    return git(author, 'rev-parse', 'HEAD');
  };
  return { root, author, work, remote, git, run, advance, state: () => JSON.parse(readFileSync(state)),
    updateState: (value) => writeFileSync(state, JSON.stringify(value)), cleanup: () => rmSync(root, { recursive: true, force: true }) };
}

test('real Git cut is head-pinned, version-only and updates the same branch without losing new main bytes', () => {
  const f = fixture();
  try {
    const main = f.advance('crates/anyagents/src/main.rs', '// new shipped bytes\n');
    const first = f.run();
    assert.equal(first.status, 0, first.stderr);
    assert.match(first.stdout, /cut v0\.1\.2: PR #10/);
    const firstHead = f.git(f.work, 'rev-parse', 'HEAD');
    const repeated = f.run();
    assert.equal(repeated.status, 0, repeated.stderr);
    assert.match(repeated.stdout, /no-op: rolling cut v0\.1\.2/);
    assert.equal(f.git(f.work, 'rev-parse', 'HEAD'), firstHead, 'reconciliation is idempotent');
    assert.equal(f.git(f.work, 'diff', '--name-only', main, firstHead).split('\n').length, 7);
    assert.equal(f.git(f.work, 'show', 'HEAD:crates/anyagents/src/main.rs'), '// new shipped bytes');
    assert.equal(f.git(f.root, '--git-dir', f.remote, 'rev-parse', 'main'), main, 'main never written');
    const newerMain = f.advance('crates/anyagents/src/main.rs', '// newest shipped bytes\n');
    const second = f.run();
    assert.equal(second.status, 0, second.stderr);
    assert.match(second.stdout, /cut v0\.1\.2: PR #10/);
    assert.equal(f.git(f.work, 'show', 'HEAD:crates/anyagents/src/main.rs'), '// newest shipped bytes');
    assert.equal(f.git(f.work, 'diff', '--name-only', newerMain, 'HEAD').split('\n').length, 7);
    f.git(f.work, 'merge-base', '--is-ancestor', firstHead, 'HEAD');
    assert.equal(f.state().calls.filter((call) => call[0] === 'pr' && call[1] === 'create').length, 1);
    assert.equal(f.state().calls.filter((call) => call[0] === 'pr' && call[1] === 'merge').length, 1, 'correct auto-merge request preserved');
  } finally { f.cleanup(); }
});

test('docs-only current main makes no pull request or branch write', () => {
  const f = fixture();
  try {
    f.advance('README.md', '# Documentation only\n', 'docs: explain setup');
    const result = f.run();
    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout, /no-op: no shipped inputs/);
    assert.equal(f.state().open, null);
    assert.equal(f.git(f.work, 'ls-remote', '--heads', 'origin', 'refs/heads/release/next'), '');
  } finally { f.cleanup(); }
});

test('an npm outage fails closed before any branch or PR publication', () => {
  const f = fixture();
  try {
    f.advance('crates/anyagents/src/main.rs', '// new\n');
    const result = f.run({ NPM_ERROR: 'E503' });
    assert.notEqual(result.status, 0);
    assert.equal(f.state().open, null);
    assert.equal(f.git(f.work, 'ls-remote', '--heads', 'origin', 'refs/heads/release/next'), '');
  } finally { f.cleanup(); }
});

test('unique non-version work on the rolling branch is preserved and refused', () => {
  const f = fixture();
  try {
    f.advance('crates/anyagents/src/main.rs', '// new\n');
    const first = f.run();
    assert.equal(first.status, 0, first.stderr);
    writeFileSync(join(f.work, 'unique-work.txt'), 'must not be overwritten\n');
    f.git(f.work, 'add', 'unique-work.txt');
    f.git(f.work, 'commit', '-m', 'preserve unique branch work');
    f.git(f.work, 'push', 'origin', 'HEAD:release/next');
    const unique = f.git(f.work, 'rev-parse', 'HEAD');
    const state = f.state(); state.open.headRefOid = unique; f.updateState(state);
    const result = f.run();
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /contains non-version changes/);
    assert.equal(f.git(f.root, '--git-dir', f.remote, 'rev-parse', 'release/next'), unique);
    assert.equal(f.git(f.root, '--git-dir', f.remote, 'show', 'release/next:unique-work.txt'), 'must not be overwritten');
  } finally { f.cleanup(); }
});
