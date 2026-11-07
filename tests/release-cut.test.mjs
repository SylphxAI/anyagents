import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { increment, nextVersion, shippedPath, updateManifests } from '../scripts/release-cut.mjs';

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
