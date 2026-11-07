// One rolling, mechanical release PR. Versions are committed and checked by CI;
// publication never builds an uncommitted version overlay.
import { execFileSync } from 'node:child_process';
import { readFileSync, writeFileSync, appendFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

export const BRANCH = 'release/next';
export const VERSION_FILES = ['Cargo.toml', 'Cargo.lock', 'packages/anyagents/package.json',
  ...['darwin-arm64', 'darwin-x64', 'linux-arm64-gnu', 'linux-x64-gnu'].map((key) => `packages/npm/${key}/package.json`)];
const parse = (value) => {
  if (!/^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/.test(value)) throw new Error(`invalid stable version: ${value}`);
  return value.split('.').map(Number);
};
export function nextVersion(current, published, bump = 'patch') {
  const a = parse(current), b = parse(published);
  const parts = a.findIndex((value, i) => value !== b[i]);
  const v = parts < 0 || a[parts] > b[parts] ? a : b;
  if (bump === 'major') return `${v[0] + 1}.0.0`;
  if (bump === 'minor') return `${v[0]}.${v[1] + 1}.0`;
  if (bump !== 'patch') throw new Error('invalid version increment');
  return `${v[0]}.${v[1]}.${v[2] + 1}`;
}
export function shippedPath(path) {
  // Compile inputs and the npm launcher/native manifests. Tests, workflows and
  // documentation do not ship; a conservative crate match includes embedded UI,
  // build scripts and other resources without having to maintain a second map.
  return /^(Cargo\.(toml|lock)|rust-toolchain(\.toml)?|\.cargo\/.*|crates\/.*|packages\/.*)$/.test(path);
}
export function increment(pulls, commits) {
  const labels = new Set(pulls.filter((pr) => pr.merged_at && commits.has(pr.merge_commit_sha))
    .flatMap((pr) => pr.labels.map((label) => label.name)));
  return labels.has('major') ? 'major' : labels.has('minor') ? 'minor' : 'patch';
}
export function updateManifests(version, root = '.') {
  parse(version);
  const read = (path) => readFileSync(`${root}/${path}`, 'utf8');
  const cargo = read('Cargo.toml');
  const pattern = /(\[workspace\.package\][\s\S]*?\nversion\s*=\s*)"[^"]+"/;
  if (!pattern.test(cargo)) throw new Error('workspace package version is absent');
  writeFileSync(`${root}/Cargo.toml`, cargo.replace(pattern, `$1"${version}"`));
  for (const path of VERSION_FILES.filter((path) => path.endsWith('.json'))) {
    const value = JSON.parse(read(path));
    value.version = version;
    if (path === 'packages/anyagents/package.json') {
      for (const name of Object.keys(value.optionalDependencies)) value.optionalDependencies[name] = version;
    }
    writeFileSync(`${root}/${path}`, `${JSON.stringify(value, null, 2)}\n`);
  }
  const platforms = readdirSync(`${root}/packages/npm`).sort();
  if (platforms.join() !== ['darwin-arm64', 'darwin-x64', 'linux-arm64-gnu', 'linux-x64-gnu'].join()) {
    throw new Error('unexpected native package inventory');
  }
}

const run = (command, args, options = {}) => execFileSync(command, args,
  { encoding: 'utf8', stdio: ['ignore', 'pipe', 'inherit'], ...options }).trim();
const git = (...args) => run('git', args);
function gh(...args) { return run('gh', args); }
const api = (path) => JSON.parse(gh('api', path));
function summary(message) {
  console.log(message);
  if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${message}\n`);
}

export function main() {
  const repo = process.env.GITHUB_REPOSITORY;
  if (repo !== 'SylphxAI/anyagents' || !process.env.GH_TOKEN) throw new Error('release writer requires the authorized repository and token');
  if (git('status', '--porcelain')) throw new Error('release checkout must be clean');
  // Never persist the credential or put it in argv/remote URLs. The helper reads
  // the broker token only from this process's inherited environment.
  process.env.GIT_CONFIG_COUNT = '2';
  process.env.GIT_CONFIG_KEY_0 = 'credential.helper';
  process.env.GIT_CONFIG_VALUE_0 = '';
  process.env.GIT_CONFIG_KEY_1 = 'credential.helper';
  process.env.GIT_CONFIG_VALUE_1 = '!f() { printf "username=x-access-token\\npassword=%s\\n" "$GH_TOKEN"; }; f';
  git('fetch', 'origin', '+refs/heads/main:refs/remotes/origin/main', '--tags');
  const head = git('rev-parse', 'origin/main');
  git('checkout', '--detach', head);
  const subject = git('log', '-1', '--format=%s');
  if (/^release: v\d+\.\d+\.\d+(?: \(#\d+\))?$/.test(subject)) {
    summary(`no-op: ${head} is a version cut; successful main CI admits publication`);
    return;
  }
  // Read every release page; draft/partial releases reserve their versions too.
  const releases = JSON.parse(gh('api', '--paginate', '--slurp', `repos/${repo}/releases?per_page=100`)).flat();
  const published = releases.filter((release) => !release.draft && !release.prerelease && /^v\d+\.\d+\.\d+$/.test(release.tag_name))
    .sort((a, b) => {
      const x = parse(a.tag_name.slice(1)), y = parse(b.tag_name.slice(1));
      return y[0] - x[0] || y[1] - x[1] || y[2] - x[2];
    })[0];
  if (!published) throw new Error('an existing stable release is required as the comparison baseline');
  const baseline = git('rev-parse', `${published.tag_name}^{commit}`);
  git('merge-base', '--is-ancestor', baseline, head);
  const paths = git('diff', '--name-only', '--no-renames', baseline, head).split('\n').filter(Boolean);
  // Version-only changes are not new shipped behavior; a superseded cut needs a
  // new version only when there is still other unpublished shipped input.
  const behavior = paths.filter((path) => shippedPath(path) && !VERSION_FILES.includes(path));
  const manifestsChanged = paths.some((path) => VERSION_FILES.includes(path) &&
    git('diff', '--numstat', baseline, head, '--', path));
  if (!behavior.length && !manifestsChanged) {
    summary(`no-op: no shipped inputs changed since ${published.tag_name}`);
    return;
  }
  const commits = new Set(git('rev-list', '--first-parent', `${baseline}..${head}`).split('\n'));
  const pulls = JSON.parse(gh('api', '--paginate', '--slurp', `repos/${repo}/pulls?state=closed&base=main&per_page=100`)).flat();
  const current = JSON.parse(readFileSync('packages/anyagents/package.json', 'utf8')).version;
  const open = JSON.parse(gh('pr', 'list', '-R', repo, '--state', 'open', '--head', BRANCH,
    '--json', 'number,headRefOid,title,autoMergeRequest'));
  if (open.length > 1) throw new Error('more than one rolling release PR');
  const bump = increment(pulls, commits);
  let version = nextVersion(current, published.tag_name.slice(1), bump);
  // Preserve the rolling cut's version while collecting more patch changes;
  // labels can only raise it. A merged/superseded cut advances current instead.
  if (open.length) {
    const match = /^release: v(\d+\.\d+\.\d+)$/.exec(open[0].title);
    if (!match) throw new Error('rolling release PR has an unexpected title');
    const candidate = match[1];
    const a = parse(candidate), b = parse(version);
    const first = a.findIndex((v, i) => v !== b[i]);
    if (first < 0 || a[first] > b[first]) version = candidate;
  }
  if (releases.some((release) => release.tag_name === `v${version}`)) throw new Error('planned version is already reserved by a release');
  // Probe npm before choosing a version: a partially published package cannot be
  // reassigned to new source. Only npm's explicit E404 means unused.
  for (const name of ['@sylphx/anyagents', '@sylphx/anyagents-darwin-arm64', '@sylphx/anyagents-darwin-x64', '@sylphx/anyagents-linux-arm64-gnu', '@sylphx/anyagents-linux-x64-gnu']) {
    try {
      const value = run('npm', ['view', `${name}@${version}`, 'version', '--json']);
      if (value) throw new Error(`planned version is already published: ${name}@${version}`);
      throw new Error('npm returned no authoritative version result');
    } catch (error) {
      // npm normally emits JSON errors to stdout; inspect the structured code,
      // not a message substring that could confuse network failures with absence.
      let payload;
      try { payload = JSON.parse(error.stdout?.toString() || ''); } catch { /* fail closed below */ }
      if (payload?.error?.code !== 'E404') throw error;
    }
  }
  if (open.length) {
    git('fetch', 'origin', `refs/heads/${BRANCH}:refs/remotes/origin/${BRANCH}`);
    if (git('rev-parse', `origin/${BRANCH}`) !== open[0].headRefOid) throw new Error('rolling branch changed during reconciliation');
    const base = git('merge-base', head, `origin/${BRANCH}`);
    const owned = git('diff', '--name-only', base, `origin/${BRANCH}`).split('\n').filter(Boolean);
    if (owned.some((path) => !VERSION_FILES.includes(path))) throw new Error('rolling release branch contains non-version changes; preserve it for review');
    git('checkout', '-B', BRANCH, `origin/${BRANCH}`);
    // Restore generated version files before merging main; conflicts in these
    // files are resolved only by regeneration from the new canonical base.
    git('-c', 'user.name=Kyle Tse', '-c', 'user.email=hi@kylet.se', 'merge', '-s', 'ours', '--no-edit', head);
    // The release branch carries no product changes of its own. Replace the
    // entire tracked tree with current main, retaining ancestry (no force push).
    git('read-tree', '--reset', '-u', head);
  } else {
    git('checkout', '-B', BRANCH, head);
    // A previously merged release branch may remain remote; retain its history.
    const refs = git('ls-remote', '--heads', 'origin', `refs/heads/${BRANCH}`);
    if (refs) {
      git('fetch', 'origin', `refs/heads/${BRANCH}:refs/remotes/origin/${BRANCH}`);
      git('-c', 'user.name=Kyle Tse', '-c', 'user.email=hi@kylet.se', 'merge', '-s', 'ours', '--no-edit', `origin/${BRANCH}`);
    }
  }
  updateManifests(version);
  run('cargo', ['update', '--workspace']);
  const changed = git('diff', '--name-only', head).split('\n').filter(Boolean);
  if (changed.some((path) => !VERSION_FILES.includes(path))) throw new Error('release generation changed an unexpected path');
  git('add', '--', ...VERSION_FILES);
  git('-c', 'user.name=Kyle Tse', '-c', 'user.email=hi@kylet.se', 'commit', '-m', `release: v${version}`);
  const cut = git('rev-parse', 'HEAD');
  // Main may move while Cargo resolves; publish only a cut of the state read.
  if (api(`repos/${repo}/git/ref/heads/main`).object.sha !== head) throw new Error('main moved before release PR publication; the pending reconciler will collect it');
  git('push', 'origin', `HEAD:refs/heads/${BRANCH}`);
  const body = `Bumps Cargo and all five npm packages to ${version}.\n\nCollects shipped changes since ${published.tag_name}; version increment: ${bump}. Publication waits for successful main CI at the exact merged version-cut SHA.\n\nGenerated version files only; normal ci-ok and merge queue remain required.`;
  if (open.length) gh('pr', 'edit', String(open[0].number), '-R', repo, '--title', `release: v${version}`, '--body', body);
  else gh('pr', 'create', '-R', repo, '--base', 'main', '--head', BRANCH, '--title', `release: v${version}`, '--body', body);
  const pr = JSON.parse(gh('pr', 'view', BRANCH, '-R', repo, '--json', 'number,headRefOid,title,autoMergeRequest,state'));
  if (pr.headRefOid !== cut || pr.title !== `release: v${version}` || pr.state !== 'OPEN') throw new Error('release PR read-back mismatch');
  if (!pr.autoMergeRequest) gh('pr', 'merge', String(pr.number), '-R', repo, '--auto', '--squash', '--match-head-commit', cut);
  summary(`cut v${version}: PR #${pr.number}, head ${cut}, base ${head}; normal merge queue requested`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) main();
