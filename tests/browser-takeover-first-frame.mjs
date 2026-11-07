// Experiment: observe the unmodified viewer's actual first-frame and mouse route.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, readFile, writeFile, mkdir, rm, open } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { stopProcessGroup } from './browser-process-group.mjs';
const [url, output] = process.argv.slice(2);
await mkdir(output, { recursive: true });
const delay = ms => new Promise(r => setTimeout(r, ms));
async function until(fn, name, timeout = 20000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) { const value = await fn(); if (value) return value; await delay(40); }
  throw new Error('Timed out: ' + name);
}
class Cdp {
  next = 0; pending = new Map(); listeners = [];
  static async connect(url) {
    const cdp = new Cdp(); cdp.ws = new WebSocket(url);
    await new Promise((resolve, reject) => { cdp.ws.onopen = resolve; cdp.ws.onerror = reject; });
    cdp.ws.onmessage = e => {
      const m = JSON.parse(e.data);
      if (m.id) { const p = cdp.pending.get(m.id); cdp.pending.delete(m.id); if (m.error) p.reject(new Error(JSON.stringify(m.error))); else p.resolve(m.result); }
      else for (const listener of cdp.listeners) listener(m);
    };
    return cdp;
  }
  call(method, params = {}) { return new Promise((resolve, reject) => { const id = ++this.next; this.pending.set(id, {resolve, reject}); this.ws.send(JSON.stringify({id, method, params})); }); }
  async eval(expression) {
    const result = await this.call('Runtime.evaluate', {expression, returnByValue: true, awaitPromise: true});
    if (result.exceptionDetails) throw new Error(JSON.stringify(result.exceptionDetails));
    return result.result.value;
  }
}
const dir = await mkdtemp(path.join(tmpdir(), 'viewer-first-frame-'));
const stderr = await open(path.join(output, 'viewer-chromium.log'), 'w');
const chrome = spawn(process.env.ANYAGENTS_CHROMIUM || '/usr/bin/chromium', ['--headless=new', '--no-sandbox', '--disable-gpu', '--remote-debugging-port=0', '--user-data-dir=' + dir, 'about:blank'], {stdio: ['ignore', 'ignore', stderr.fd], detached: true});
const result = {route: 'real-viewer-mouse', requests: [], responses: [], frameResponses: [], pointerEvents: []};
let viewer;
try {
  const port = await until(async () => { try { return (await readFile(path.join(dir, 'DevToolsActivePort'), 'utf8')).split('\n')[0]; } catch { return false; } }, 'viewer launch');
  const targets = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json();
  viewer = await Cdp.connect(targets.find(t => t.type === 'page').webSocketDebuggerUrl);
  await viewer.call('Page.enable');
  await viewer.call('Network.enable');
  await viewer.call('Emulation.setDeviceMetricsOverride', {width:1440,height:900,deviceScaleFactor:1,mobile:false});
  const inputIds = new Set();
  const bodies = [];
  viewer.listeners.push(m => {
    if (m.method === 'Network.requestWillBeSent' && m.params.request.url.endsWith('/input')) {
      inputIds.add(m.params.requestId);
      result.requests.push({id:m.params.requestId, timestamp:m.params.timestamp, payload:JSON.parse(m.params.request.postData)});
    }
    if (m.method === 'Network.responseReceived' && m.params.response.url.includes('/frame.jpg')) result.frameResponses.push({id:m.params.requestId,status:m.params.response.status,timestamp:m.params.timestamp});
    if (m.method === 'Network.loadingFinished' && inputIds.has(m.params.requestId)) {
      bodies.push(viewer.call('Network.getResponseBody', {requestId:m.params.requestId}).then(body => {
        result.responses.push({id:m.params.requestId,timestamp:m.params.timestamp,body:JSON.parse(body.body)});
      }));
    }
  });
  await viewer.call('Page.addScriptToEvaluateOnNewDocument', {source:`window.__pointers=[];for(const type of ['pointerdown','pointerup'])document.addEventListener(type,e=>window.__pointers.push({type,trusted:e.isTrusted,pointerType:e.pointerType,x:e.clientX,y:e.clientY,at:performance.now()}),true);`});
  await viewer.call('Page.navigate', {url});
  result.frameReady = await until(() => viewer.eval(`(() => {const c=document.querySelector('#screen');if(!c?.dataset.remoteWidth||!document.querySelector('#connection').hidden)return false;const pixels=c.getContext('2d').getImageData(0,0,1,1).data;return {width:c.width,height:c.height,remoteWidth:Number(c.dataset.remoteWidth),remoteHeight:Number(c.dataset.remoteHeight),pixel:[...pixels],at:performance.now()};})()`), 'first frame fetched, decoded, drawn and connected');
  assert.ok(result.frameReady.pixel[3] > 0, 'first frame is drawn, not an empty transparent canvas');
  result.localPoint = await viewer.eval(`(() => {const c=document.querySelector('#screen'),r=c.getBoundingClientRect();return {x:r.x+100*r.width/Number(c.dataset.remoteWidth),y:r.y+100*r.height/Number(c.dataset.remoteHeight)};})()`);
  for (const [type,buttons] of [['mousePressed',1],['mouseReleased',0]]) await viewer.call('Input.dispatchMouseEvent', {type,button:'left',buttons,clickCount:1,...result.localPoint});
  await until(() => result.responses.length === 2, 'two real /input acknowledgements');
  await Promise.all(bodies);
  result.pointerEvents = await viewer.eval('window.__pointers');
  assert.equal(result.pointerEvents.length, 2);
  assert.ok(result.pointerEvents.every(e => e.trusted && e.pointerType === 'mouse' && e.at > result.frameReady.at));
  assert.deepEqual(result.requests.map(r => [r.payload.type,r.payload.event_type]), [['mouse','mousePressed'],['mouse','mouseReleased']]);
  assert.ok(result.requests.every(r => Math.abs(r.payload.x-100) < 0.01 && Math.abs(r.payload.y-100) < 0.01));
  assert.ok(result.responses.every(r => r.body.ok === true));
  result.focus = result.responses.find(r => r.body.event_type === 'mouseReleased').body.focus;
  if (!result.focus?.editable) {
    const shot = await viewer.call('Page.captureScreenshot', {format:'png',captureBeyondViewport:false});
    await writeFile(path.join(output, 'failure.png'), Buffer.from(shot.data, 'base64'));
  }
} catch (error) { result.error = error.stack; process.exitCode = 1; }
finally {
  if (viewer) viewer.ws.close();
  await writeFile(path.join(output, 'viewer-result.json'), JSON.stringify(result,null,2)+'\n');
  await stopProcessGroup(chrome);
  await stderr.close();
  await rm(dir, {recursive:true});
}
