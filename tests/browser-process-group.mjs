import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

// Only for a fixture child spawned with detached:true: its PID owns the group.
// Zombies cannot write profiles; kill(0) alone cannot distinguish them from writers.
export async function liveProcessGroupMembers(pgid) {
  const { stdout } = await execFileAsync('ps', ['-axo', 'pid=,pgid=,stat='], { maxBuffer: 4 * 1024 * 1024 });
  return stdout.trim().split('\n').filter(Boolean).map(line => {
    const [pid, group, state] = line.trim().split(/\s+/);
    return { pid: Number(pid), group: Number(group), state };
  }).filter(member => member.group === pgid && !member.state.startsWith('Z'));
}

function signalGroup(pgid, signal) {
  try { process.kill(-pgid, signal); }
  catch (error) { if (error.code !== 'ESRCH') throw error; }
}

async function waitForGroup(pgid, timeoutMs) {
  const deadline = performance.now() + timeoutMs;
  while (true) {
    const live = await liveProcessGroupMembers(pgid);
    if (live.length === 0) return true;
    if (performance.now() >= deadline) return false;
    await delay(40);
  }
}

export async function stopProcessGroup(child, { graceMs = 5000, killMs = 5000 } = {}) {
  const pgid = child.pid;
  if (!Number.isInteger(pgid) || pgid <= 1 || pgid === process.pid) {
    throw new Error('Expected a detached fixture process group');
  }
  const exited = child.exitCode !== null || child.signalCode !== null
    ? Promise.resolve()
    : new Promise(resolve => child.once('exit', resolve));
  signalGroup(pgid, 'SIGTERM');
  if (!await waitForGroup(pgid, graceMs)) {
    signalGroup(pgid, 'SIGKILL');
    if (!await waitForGroup(pgid, killMs)) {
      throw new Error('Fixture process group still has live writers: ' + JSON.stringify(await liveProcessGroupMembers(pgid)));
    }
  }
  await exited;
}
