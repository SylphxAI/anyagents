import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { liveProcessGroupMembers, stopProcessGroup } from './browser-process-group.mjs';

async function fixture(t, ignoreTerm) {
  const dir = await mkdtemp(path.join(tmpdir(), 'process-group-'));
  const script = `
    const {spawn}=require('node:child_process');
    const child=spawn(process.execPath,['-e',${JSON.stringify(`
      const fs=require('node:fs');
      process.on('SIGTERM',()=>{${ignoreTerm ? '' : "fs.writeFileSync(process.argv[1]+'/stopped','yes');process.exit(0);"}});
      process.send('ready');setInterval(()=>{},100);
    `)},process.argv[1]],{stdio:['ignore','ignore','ignore','ipc']});
    process.on('SIGTERM',()=>process.exit(0));
    child.once('message',()=>process.stdout.write(JSON.stringify({pid:child.pid})+'\\n'));
  `;
  const parent = spawn(process.execPath, ['-e', script, dir], { detached: true, stdio: ['ignore', 'pipe', 'inherit'] });
  t.after(async () => { await stopProcessGroup(parent, { graceMs: 100, killMs: 5000 }); await rm(dir, { recursive: true, force: true }); });
  const [data] = await once(parent.stdout, 'data');
  return { parent, grandchild: JSON.parse(data.toString()).pid, dir };
}

test('parent exit leaves a live writer; group cleanup waits for the whole fixture', async t => {
  const { parent, grandchild } = await fixture(t, true);
  const exited = once(parent, 'exit');
  parent.kill('SIGTERM');
  await exited;
  assert.ok((await liveProcessGroupMembers(parent.pid)).some(member => member.pid === grandchild), 'the old parent-only wait must leave the grandchild alive');
  await stopProcessGroup(parent, { graceMs: 100, killMs: 5000 });
  assert.deepEqual(await liveProcessGroupMembers(parent.pid), []);
});

test('graceful group termination reaches descendants before deletion', async t => {
  const { parent, dir } = await fixture(t, false);
  assert.ok((await liveProcessGroupMembers(parent.pid)).length >= 2, 'live-group control');
  await stopProcessGroup(parent);
  assert.equal(await readFile(path.join(dir, 'stopped'), 'utf8'), 'yes');
  assert.deepEqual(await liveProcessGroupMembers(parent.pid), []);
});
