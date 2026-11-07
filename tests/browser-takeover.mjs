// Dependency-free Chromium acceptance driver. Invoked by the Rust fixture test.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, readFile, writeFile, mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { stopProcessGroup } from './browser-process-group.mjs';
import { interactionAcceptance } from './browser-takeover-interaction.mjs';
const [url, remoteWs, output] = process.argv.slice(2);
assert.ok(url && remoteWs && output, 'usage: node browser-takeover.mjs URL REMOTE_WS OUTPUT');
await mkdir(output, { recursive: true });
const delay = ms => new Promise(r => setTimeout(r, ms));
async function until(fn, name, timeout = 15000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) { const result = await fn(); if (result) return result; await delay(40); }
  throw new Error('Timed out: ' + name);
}
class Cdp {
  next = 0; pending = new Map();
  static async connect(url) {
    const cdp = new Cdp(); cdp.ws = new WebSocket(url);
    await new Promise((resolve, reject) => { cdp.ws.onopen = resolve; cdp.ws.onerror = reject; });
    cdp.ws.onmessage = e => { const m = JSON.parse(e.data); if (m.id) { const p = cdp.pending.get(m.id); cdp.pending.delete(m.id); if (m.error) p.reject(new Error(JSON.stringify(m.error))); else p.resolve(m.result); } };
    return cdp;
  }
  call(method, params = {}) {
    return new Promise((resolve, reject) => { const id = ++this.next; this.pending.set(id, { resolve, reject }); this.ws.send(JSON.stringify({ id, method, params })); });
  }
  async eval(expression) {
    const result = await this.call('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true });
    if (result.exceptionDetails) throw new Error(JSON.stringify(result.exceptionDetails));
    return result.result.value;
  }
  close() { this.ws.close(); }
}
const dir = await mkdtemp(path.join(tmpdir(), 'takeover-viewer-'));
const chrome = spawn(process.env.ANYAGENTS_CHROMIUM || '/usr/bin/chromium', ['--headless=new', '--no-sandbox', '--disable-gpu', '--remote-debugging-port=0', '--user-data-dir=' + dir, 'about:blank'], { stdio: 'ignore', detached: true });
let viewer, remote;
const logs = [];
try {
  const port = await until(async () => { try { return (await readFile(path.join(dir, 'DevToolsActivePort'), 'utf8')).split('\n')[0]; } catch { return false; } }, 'viewer launch');
  const targets = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json();
  viewer = await Cdp.connect(targets.find(t => t.type === 'page').webSocketDebuggerUrl);
  remote = await Cdp.connect(remoteWs);
  await viewer.call('Page.enable');
  const original = await remote.eval('({width:innerWidth,height:innerHeight,dpr:devicePixelRatio,ua:navigator.userAgent,hints:JSON.stringify(navigator.userAgentData?.toJSON()),screen:[screen.width,screen.height]})');
  const base = url.replace(/\/$/, '');
  async function api(route, payload) {
    const res = await fetch(base + route, payload === undefined ? undefined : { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
    return res.json();
  }
  async function screenshot(name) {
    const result = await viewer.call('Page.captureScreenshot', { format: 'png', captureBeyondViewport: false });
    await writeFile(path.join(output, name + '.png'), Buffer.from(result.data, 'base64'));
  }
  async function clickSelector(selector) {
    const p = await viewer.eval(`(() => { const r=document.querySelector(${JSON.stringify(selector)}).getBoundingClientRect(); return {x:r.x+r.width/2,y:r.y+r.height/2}; })()`);
    await viewer.call('Input.dispatchMouseEvent', { type: 'mousePressed', button: 'left', buttons: 1, clickCount: 1, ...p });
    await viewer.call('Input.dispatchMouseEvent', { type: 'mouseReleased', button: 'left', buttons: 0, clickCount: 1, ...p });
  }
  async function touch(type, points) {
    await viewer.call('Input.dispatchTouchEvent', { type, touchPoints: points.map((p, i) => ({ x: p[0], y: p[1], id: i, radiusX: 1, radiusY: 1 })) });
  }
  async function tap(x, y) { await touch('touchStart', [[x, y]]); await touch('touchEnd', []); }
  async function frame(dpr, width = 1920, height = 1080) {
    try {
      await until(() => viewer.eval(`Number(document.querySelector('#screen')?.dataset.remoteWidth) === ${width} && Number(document.querySelector('#screen').dataset.remoteHeight) === ${height} && Number(document.querySelector('#screen').dataset.remoteDpr) === ${dpr} && document.querySelector('#connection').hidden`), 'matching frame ' + dpr);
    } catch (error) {
      console.error('frame diagnostics', await viewer.eval(`({width:document.querySelector('#screen').width,height:document.querySelector('#screen').height,status:document.querySelector('#connection').textContent,hidden:document.querySelector('#connection').hidden})`), await remote.eval('({width:innerWidth,height:innerHeight,dpr:devicePixelRatio})'), await api('/state'));
      throw error;
    }
  }
  async function key(key, code, vk, modifiers = 0) {
    await viewer.call('Input.dispatchKeyEvent', { type:'keyDown', key, code, windowsVirtualKeyCode:vk, modifiers, ...(key.length===1 && !(modifiers&7) ? {text:key,unmodifiedText:key} : {}) });
    await viewer.call('Input.dispatchKeyEvent', { type:'keyUp', key, code, windowsVirtualKeyCode:vk, modifiers });
  }
  async function remoteField(id) {
    const r = await remote.eval(`(() => {const r=document.querySelector('#${id}').getBoundingClientRect();return {x:r.x+r.width/2,y:r.y+r.height/2}})()`);
    const local = await viewer.eval(`(() => {const c=document.querySelector('#screen'),r=c.getBoundingClientRect();return {x:r.x+${r.x}*r.width/Number(c.dataset.remoteWidth),y:r.y+${r.y}*r.height/Number(c.dataset.remoteHeight)}})()`);
    if (await viewer.eval(`matchMedia('(pointer:coarse)').matches`)) await tap(local.x, local.y);
    else {
      await viewer.call('Input.dispatchMouseEvent', {type:'mousePressed',button:'left',buttons:1,clickCount:1,...local});
      await viewer.call('Input.dispatchMouseEvent', {type:'mouseReleased',button:'left',buttons:0,clickCount:1,...local});
    }
    await until(() => remote.eval(`document.activeElement.id === '${id}'`), 'remote field '+id);
  }
  await viewer.call('Browser.grantPermissions', {origin:new URL(url).origin,permissions:['clipboardReadWrite','clipboardSanitizedWrite']});
  await interactionAcceptance({viewer,remote,url,output,until,key,touch,api,screenshot});
  for (const [width,height] of [[1440,900],[390,844]]) {
    await viewer.call('Emulation.setDeviceMetricsOverride', {width,height,deviceScaleFactor:1,mobile:width<700});
    await viewer.call('Emulation.setTouchEmulationEnabled', {enabled:width<700});
    await viewer.call('Page.navigate', {url});
    await until(() => viewer.eval(`!!document.querySelector('#screen') && document.querySelector('#connection').hidden`), 'clipboard viewer connected');
    assert.equal(await viewer.eval(`document.querySelector('#select-all-btn').textContent`),'Select all');
    assert.equal(await viewer.eval(`document.querySelector('#select-all-btn').getAttribute('aria-label')`),'Select all');
    const pillBounds=await viewer.eval(`[...document.querySelectorAll('#hud-bar button')].map(el=>{const r=el.getBoundingClientRect();return {left:r.left,right:r.right,width:r.width,height:r.height}})`);
    assert.ok(pillBounds.every(r=>r.left>=10&&r.right<=width-10&&r.width>=44&&r.height>=44),JSON.stringify(pillBounds));
    await remote.eval(`document.querySelector('#entry').value='';document.querySelector('#entry2').value=''`);
    await remoteField('entry');
    await api('/input',{type:'paste',text:'undo fixture'});
    await key('z','KeyZ',90,2);
    await until(() => remote.eval(`document.querySelector('#entry').value === ''`), 'Ctrl+Z undo');
    await viewer.eval(`navigator.clipboard.writeText('pasted once ✓')`);
    await key('v','KeyV',86,2);
    await until(() => remote.eval(`document.querySelector('#entry').value === 'pasted once ✓'`), 'native paste inserted once');
    // Cmd+A must select, never type a literal a. Cmd+C returns that selection.
    await key('a','KeyA',65,4);
    await key('c','KeyC',67,4);
    await until(() => viewer.eval(`navigator.clipboard.readText().then(v=>v==='pasted once ✓')`), 'copy remote selection');
    await key('x','KeyX',88,4);
    await until(() => remote.eval(`document.querySelector('#entry').value === ''`), 'Cmd+A then Cmd+X cut');
    assert.equal(await viewer.eval(`navigator.clipboard.readText()`), 'pasted once ✓');
    await key('Tab','Tab',9);
    await until(() => remote.eval(`document.activeElement.id === 'entry2'`), 'Tab remote focus');
    await key('Tab','Tab',9,8);
    await until(() => remote.eval(`document.activeElement.id === 'entry'`), 'Shift+Tab remote focus');
    // The clipboard write may yield while another event moves remote focus.
    // Matching text in another field must never authorize cutting that field.
    await remote.eval(`for (const id of ['entry','entry2']) document.querySelector('#'+id).value='same';document.querySelector('#entry').focus();document.querySelector('#entry').select()`);
    const captured = await api('/input',{type:'clipboard',action:'copy'});
    await remote.eval(`document.querySelector('#entry2').focus();document.querySelector('#entry2').select()`);
    const moved = await api('/input',{type:'clipboard',action:'cut',expected_text:captured.text,selection_key:captured.selection_key});
    assert.equal(moved.ok,false,'A-to-B focus race must refuse cut even when selected text matches');
    assert.deepEqual(await remote.eval(`['entry','entry2'].map(id=>document.querySelector('#'+id).value)`),['same','same'],'neither field may be deleted after focus moves');
    // Hold the real viewer's asynchronous clipboard write at the exact yield.
    await remote.eval(`document.querySelector('#entry').focus();document.querySelector('#entry').select()`);
    await viewer.eval(`window.originalClipboardWrite=navigator.clipboard.write.bind(navigator.clipboard);window.clipboardCaptured=false;navigator.clipboard.write=async items=>{await items[0].getType('text/plain');window.clipboardCaptured=true;await new Promise(resolve=>window.releaseClipboardWrite=resolve);await window.originalClipboardWrite(items)}`);
    await key('x','KeyX',88,4);
    await until(() => viewer.eval(`window.clipboardCaptured`),'cut selection captured before asynchronous write');
    await remote.eval(`document.querySelector('#entry2').focus();document.querySelector('#entry2').select()`);
    await viewer.eval(`window.releaseClipboardWrite()`);
    await until(() => viewer.eval(`document.querySelector('#clipboard-status').textContent.includes('Cut cancelled')`),'changed-focus cut has a clear cancellation status');
    assert.deepEqual(await remote.eval(`['entry','entry2'].map(id=>document.querySelector('#'+id).value)`),['same','same'],'viewer async cut cannot delete either field after focus moves');
    await viewer.eval(`navigator.clipboard.write=window.originalClipboardWrite`);
    await remote.eval(`document.querySelector('#entry').value='';document.querySelector('#entry2').value='';document.querySelector('#entry').focus()`);
    await viewer.eval(`navigator.clipboard.writeText('pill paste')`);
    await clickSelector('#paste-btn');
    await until(() => remote.eval(`document.querySelector('#entry').value === 'pill paste'`), 'one-tap paste pill');
    await clickSelector('#select-all-btn');
    await clickSelector('#copy-btn');
    await until(() => viewer.eval(`navigator.clipboard.readText().then(v=>v==='pill paste')`), 'one-tap copy pill');
    if (width<700) {
      const overlay = await viewer.eval(`(() => {const p=document.querySelector('#keyboard-proxy'),c=document.querySelector('#screen'),r=p.getBoundingClientRect(),s=c.getBoundingClientRect();return {x:r.x,y:r.y,width:r.width,height:r.height,screen:{x:s.x,y:s.y,width:s.width},remoteWidth:Number(c.dataset.remoteWidth),pointer:getComputedStyle(p).pointerEvents,active:document.activeElement.id};})()`);
      const field = await remote.eval(`(() => {const r=document.querySelector('#entry').getBoundingClientRect();return {x:r.x,y:r.y,width:r.width,height:r.height}})()`);
      const scale=overlay.screen.width/overlay.remoteWidth;
      assert.ok(Math.abs(overlay.x-(overlay.screen.x+field.x*scale))<1,JSON.stringify(overlay));
      assert.ok(Math.abs(overlay.y-(overlay.screen.y+field.y*scale))<1,JSON.stringify(overlay));
      assert.ok(Math.abs(overlay.width-field.width*scale)<1 && Math.abs(overlay.height-field.height*scale)<1,JSON.stringify(overlay));
      assert.equal(overlay.pointer,'auto'); assert.equal(overlay.active,'keyboard-proxy');
      // Native paste into the aligned input uses the same single paste bridge.
      await viewer.eval(`navigator.clipboard.writeText('mobile paste')`);
      await key('v','KeyV',86,2);
      await until(() => remote.eval(`document.querySelector('#entry').value === 'mobile paste'`), 'aligned phone native paste');
    }
    const beforeDenied = await remote.eval(`document.querySelector('#entry').value`);
    await viewer.eval(`window.originalReadText=navigator.clipboard.readText;navigator.clipboard.readText=async()=>{throw new DOMException('Denied','NotAllowedError')}`);
    await clickSelector('#paste-btn');
    await until(() => viewer.eval(`document.querySelector('#clipboard-status').textContent.includes('Allow clipboard access')`), 'paste permission prompt');
    assert.equal(await remote.eval(`document.querySelector('#entry').value`),beforeDenied,'refused paste must not insert');
    await viewer.eval(`navigator.clipboard.readText=window.originalReadText`);
    // A native phone paste event is also the bridge for an OS long-press menu.
    await api('/input',{type:'key',key:'a',modifiers:2});
    await viewer.eval(`(() => {const data=new DataTransfer();data.setData('text/plain','synthetic paste once');document.activeElement.dispatchEvent(new ClipboardEvent('paste',{clipboardData:data,bubbles:true,cancelable:true}));})()`);
    await until(() => remote.eval(`document.querySelector('#entry').value === 'synthetic paste once'`), 'synthetic paste inserts once');
    if (width<700) {
      await touch('touchStart',[[110,500],[280,500]]); await touch('touchMove',[[70,500],[320,500]]); await touch('touchEnd',[]);
      await touch('touchStart',[[200,500]]); await touch('touchMove',[[170,470]]); await touch('touchEnd',[]);
      const aligned=await viewer.eval(`(() => {const p=document.querySelector('#keyboard-proxy').getBoundingClientRect(),c=document.querySelector('#screen'),r=c.getBoundingClientRect();return {x:p.x,y:p.y,width:p.width,height:p.height,screen:{x:r.x,y:r.y,width:r.width},remoteWidth:Number(c.dataset.remoteWidth)}})()`);
      const field=await remote.eval(`(() => {const r=document.querySelector('#entry').getBoundingClientRect();return {x:r.x,y:r.y,width:r.width,height:r.height}})()`);
      const scale=aligned.screen.width/aligned.remoteWidth;
      assert.ok(Math.abs(aligned.x-(aligned.screen.x+field.x*scale))<1 && Math.abs(aligned.y-(aligned.screen.y+field.y*scale))<1,JSON.stringify(aligned));
      assert.ok(Math.abs(aligned.width-field.width*scale)<1 && Math.abs(aligned.height-field.height*scale)<1,JSON.stringify(aligned));
    }
    await screenshot(`${width}-clipboard`);
    await remoteField('password');
    const typed = await api('/input', {type:'paste',text:'private-fixture-password'});
    assert.ok(!JSON.stringify(typed).includes('private-fixture-password'), 'paste response must not echo secrets');
    const blocked=await api('/input', {type:'clipboard',action:'copy'});
    assert.equal(blocked.ok,false); assert.ok(!JSON.stringify(blocked).includes('private-fixture-password'));
    await viewer.eval(`navigator.clipboard.writeText('keep local clipboard')`);
    await key('a','KeyA',65,4); await key('c','KeyC',67,4);
    assert.equal(await viewer.eval(`navigator.clipboard.readText()`),'keep local clipboard');
    const reserved=await viewer.eval(`['w','t','l'].map(key=>{const e=new KeyboardEvent('keydown',{key,ctrlKey:true,bubbles:true,cancelable:true});document.activeElement.dispatchEvent(e);return e.defaultPrevented})`);
    assert.deepEqual(reserved,[false,false,false], 'browser-tab shortcuts stay local');
    for (const key of ['w','t','l']) assert.equal((await api('/input',{type:'key',key,modifiers:2})).ok,false);
    await remote.eval(`document.querySelector('#password').value='';document.querySelector('#entry').value=''`);
  }
  if (process.env.TAKEOVER_CLIPBOARD_ONLY === '1') {
    await writeFile(path.join(output,'clipboard-result.json'),JSON.stringify({passed:true,viewports:2,nativePaste:true,copy:true,cut:true,tab:true,overlay:true,passwordGuard:true}));
    console.log(JSON.stringify({passed:true,clipboard:true,screenshots:output}));
  } else {
  for (const [width, height] of [[390, 844], [588, 1280], [1280, 800], [1440, 900]]) {
    await viewer.call('Emulation.setDeviceMetricsOverride', { width, height, deviceScaleFactor: 1, mobile: width < 700 });
    await viewer.call('Emulation.setTouchEmulationEnabled', { enabled: width < 700 });
    await viewer.call('Page.navigate', { url });
    await until(() => viewer.eval(`!!document.querySelector('#screen') && document.querySelector('#connection').hidden`), 'UI connected');
    const hud = await viewer.eval(`(() => {const r=document.querySelector('#hud').getBoundingClientRect();return {height:r.height,bottom:r.bottom,stage:document.querySelector('#stage').clientHeight,buttons:[...document.querySelectorAll('#hud-bar button')].map(b=>{const r=b.getBoundingClientRect();return [r.width,r.height]})}})()`);
    assert.ok(hud.height <= 48, JSON.stringify(hud)); assert.equal(hud.stage, height);
    assert.ok(hud.bottom <= height - 10); assert.ok(hud.buttons.every(([w,h])=>w>=44&&h>=44));
    await screenshot(`${width}-collapsed`);
    await clickSelector('#hud-toggle');
    const buttons = await viewer.eval(`[...document.querySelectorAll('#hud button')].filter(b=>b.getClientRects().length).map(b=>{const r=b.getBoundingClientRect();return [r.width,r.height]})`);
    assert.ok(buttons.every(([w,h])=>w>=44&&h>=44));
    await screenshot(`${width}-expanded`);
    assert.equal((await api('/state')).phone_layout, false, 'phone layout is opt-in');
    await clickSelector('#hud-toggle');
    // Each screenshot DPR uses the actual remote CDP target and trusted touch delivery.
    for (const dpr of [1, 2, 3]) {
      await remote.call('Emulation.setDeviceMetricsOverride', { width: 1920, height: 1080, deviceScaleFactor: dpr, mobile: false });
      await frame(dpr);
      if (width < 700) {
        await touch('touchStart', [[width/2-40,height/2],[width/2+40,height/2]]);
        await touch('touchMove', [[width/2-100,height/2],[width/2+100,height/2]]);
        await touch('touchEnd', []);
        await touch('touchStart', [[width/2,height/2]]);
        await touch('touchMove', [[width/2-25,height/2-30]]);
        await touch('touchEnd', []);
      } else {
        // Desktop trackpad zoom retains remote mouse input and wheel scroll.
        await viewer.call('Input.dispatchMouseEvent', { type: 'mouseWheel', x: width/2, y: height/2, deltaX: 0, deltaY: -100, modifiers: 2 });
      }
      const expected = await remote.eval(`(() => {const r=document.querySelector('#target').getBoundingClientRect();return {x:r.x+r.width/2,y:r.y+r.height/2}})()`);
      const local = await viewer.eval(`(() => {const c=document.querySelector('#screen'),r=c.getBoundingClientRect();return {x:r.x+${expected.x}*r.width/Number(c.dataset.remoteWidth),y:r.y+${expected.y}*r.height/Number(c.dataset.remoteHeight)}})()`);
      const before = await remote.eval('window.clicks.length');
      if (width < 700) await tap(local.x, local.y);
      else { await viewer.call('Input.dispatchMouseEvent', { type: 'mousePressed', button: 'left', buttons: 1, clickCount: 1, ...local }); await viewer.call('Input.dispatchMouseEvent', { type: 'mouseReleased', button: 'left', buttons: 0, clickCount: 1, ...local }); }
      await until(async () => (await remote.eval('window.clicks.length')) > before, 'remote click log');
      const actual = await remote.eval('window.clicks.at(-1)');
      assert.equal(actual.target, 'target'); assert.equal(actual.trusted, true);
      assert.ok(Math.abs(actual.x - expected.x) <= 2 && Math.abs(actual.y - expected.y) <= 2, JSON.stringify({actual,expected}));
      const frameState = await api('/state');
      logs.push({width,height,dpr,naturalWidth:frameState.natural_width,naturalHeight:frameState.natural_height,remoteViewport:frameState.viewport,expected,actual});
      if (dpr === 2) assert.equal(frameState.natural_width, 1920, 'fresh capture session raster scale differs from document DPR');
      await screenshot(`${width}-zoomed-dpr${dpr}`);
      await viewer.eval(`document.querySelector('#reset-btn').click()`);
    }
    await remote.call('Emulation.clearDeviceMetricsOverride');
    await frame(original.dpr, original.width, original.height);
    // Phone layout must preserve the whole browser identity, not just UA.
    await clickSelector('#hud-toggle');
    await clickSelector('#viewport-btn');
    await until(async () => (await api('/state')).phone_layout, 'phone layout enabled');
    const mobile = await remote.eval('({width:innerWidth,height:innerHeight,dpr:devicePixelRatio,ua:navigator.userAgent,hints:JSON.stringify(navigator.userAgentData?.toJSON()),screen:[screen.width,screen.height]})');
    assert.equal(mobile.width, Math.min(430, Math.max(390, width)));
    assert.equal(mobile.ua, original.ua); assert.equal(mobile.hints, original.hints); assert.deepEqual(mobile.screen, original.screen); assert.equal(mobile.dpr, original.dpr);
    await until(() => viewer.eval(`Number(document.querySelector('#screen').dataset.remoteWidth) === ${mobile.width}`), 'phone frame');
    await screenshot(`${width}-phone`);
    await clickSelector('#hud-toggle');
    // Focus inspection plus hidden proxy sends text, Backspace and Enter to the remote field.
    const input = await remote.eval(`(() => {const r=document.querySelector('#entry').getBoundingClientRect();return {x:r.x+r.width/2,y:r.y+r.height/2}})()`);
    const localInput = await viewer.eval(`(() => {const c=document.querySelector('#screen'),r=c.getBoundingClientRect();return {x:r.x+${input.x}*r.width/Number(c.dataset.remoteWidth),y:r.y+${input.y}*r.height/Number(c.dataset.remoteHeight)}})()`);
    await tap(localInput.x, localInput.y);
    await until(() => remote.eval(`document.activeElement.id === 'entry'`), 'remote input focused');
    await clickSelector('#hud-toggle');
    await clickSelector('#keyboard-btn');
    assert.equal(await viewer.eval('document.activeElement.id'), 'keyboard-proxy');
    await viewer.call('Input.insertText', { text: 'hello世界' });
    await viewer.call('Input.dispatchKeyEvent', { type: 'keyDown', key: 'Backspace', code: 'Backspace', windowsVirtualKeyCode: 8 });
    await viewer.call('Input.dispatchKeyEvent', { type: 'keyUp', key: 'Backspace', code: 'Backspace', windowsVirtualKeyCode: 8 });
    await viewer.call('Input.dispatchKeyEvent', { type: 'keyDown', key: 'Enter', code: 'Enter', windowsVirtualKeyCode: 13 });
    await viewer.call('Input.dispatchKeyEvent', { type: 'keyUp', key: 'Enter', code: 'Enter', windowsVirtualKeyCode: 13 });
    await until(() => remote.eval(`document.querySelector('#entry').value === 'hello世' && window.keys.includes('Enter')`), 'remote keyboard text/delete/enter');
    await screenshot(`${width}-keyboard`);
    await clickSelector('#hud-toggle');
    await remote.eval(`document.querySelector('#entry').value='';window.keys=[]`);
    await clickSelector('#hud-toggle'); await clickSelector('#viewport-btn');
    await until(async () => !(await api('/state')).phone_layout, 'phone layout disabled');
    const restored = await remote.eval('({width:innerWidth,height:innerHeight,dpr:devicePixelRatio,ua:navigator.userAgent,hints:JSON.stringify(navigator.userAgentData?.toJSON()),screen:[screen.width,screen.height]})');
    assert.deepEqual(restored, original);
    await clickSelector('#hud-toggle');
  }
  // Normal navigation restores promptly without ending the human's takeover.
  await api('/viewport', { enabled: true, width: 390, height: 844 });
  await until(() => viewer.eval(`document.querySelector('#viewport-btn').getAttribute('aria-pressed') === 'true'`), 'viewer knows phone layout before leaving');
  await viewer.call('Page.navigate', { url: 'about:blank' });
  await until(async () => !(await api('/state')).phone_layout, 'pagehide beacon restores viewport');
  assert.equal((await api('/state')).completed, false);
  assert.deepEqual(await remote.eval('({width:innerWidth,height:innerHeight,dpr:devicePixelRatio,ua:navigator.userAgent,hints:JSON.stringify(navigator.userAgentData?.toJSON()),screen:[screen.width,screen.height]})'), original);
  await viewer.call('Emulation.setDeviceMetricsOverride', { width:390, height:844, deviceScaleFactor:1, mobile:true });
  await viewer.call('Emulation.setTouchEmulationEnabled', { enabled:true });
  await viewer.call('Page.navigate', { url });
  await until(() => viewer.eval(`!!document.querySelector('#screen') && document.querySelector('#connection').hidden`), 'UI reconnected after navigation');
  // Drop the real viewer's network requests; stale imagery must not look live.
  await viewer.call('Network.enable');
  await viewer.call('Network.setBlockedURLs', { urls: ['*frame.jpg*', '*takeover/*/state'] });
  await until(() => viewer.eval(`!document.querySelector('#connection').hidden && document.querySelector('#connection').textContent === 'Reconnecting…'`), 'visible reconnect state');
  await screenshot('390-reconnecting');
  await viewer.call('Network.setBlockedURLs', { urls: [] });
  await until(() => viewer.eval(`document.querySelector('#connection').hidden`), 'frame connection recovered');
  if (process.env.TAKEOVER_GOOGLE_ACCEPTANCE === '1') {
    await api('/viewport', { enabled:true, width:390, height:844 });
    await remote.call('Page.navigate', { url:'https://accounts.google.com/' });
    await until(() => remote.eval(`location.hostname === 'accounts.google.com' && !!document.querySelector('#identifierId')`), 'Google sign-in loaded', 30000);
    for (const [width,height] of [[390,844],[588,1280],[1280,800]]) {
      await viewer.call('Emulation.setDeviceMetricsOverride', { width,height,deviceScaleFactor:1,mobile:width<700 });
      await until(() => viewer.eval(`Number(document.querySelector('#screen').dataset.remoteWidth) === 390 && document.querySelector('#connection').hidden`), 'Google phone frame');
      // A new JPEG is required after navigation, not the fixture's prior frame.
      await delay(400);
      const field = await remote.eval(`(() => {const el=document.querySelector('#identifierId'),r=el.getBoundingClientRect(),s=getComputedStyle(el);return {width:r.width,height:r.height,font:parseFloat(s.fontSize),viewport:innerWidth};})()`);
      assert.equal(field.viewport,390); assert.ok(field.width>=200&&field.height>=40&&field.font>=16, JSON.stringify(field));
      await screenshot(`${width}-google-phone`);
    }
  }
  // Explicitly leave phone mode on for Done: restore must happen before handing back.
  await api('/viewport', { enabled: true, width: 390, height: 844 });
  await clickSelector('#done-btn');
  await until(() => viewer.eval(`!document.querySelector('#done-overlay').hidden`), 'Done handed back');
  assert.deepEqual(await remote.eval('({width:innerWidth,height:innerHeight,dpr:devicePixelRatio,ua:navigator.userAgent,hints:JSON.stringify(navigator.userAgentData?.toJSON()),screen:[screen.width,screen.height]})'), original);
  await writeFile(path.join(output, 'remote-click-log.json'), JSON.stringify(logs, null, 2));
  console.log(JSON.stringify({passed:true, viewports:4, clicks:logs.length, screenshots:output}));
  }
} finally {
  viewer?.close(); remote?.close();
  // Parent exit does not establish helper exit. Refuse deletion while any
  // process in this fixture's group can still write the disposable profile.
  await stopProcessGroup(chrome);
  await rm(dir, { recursive: true, force: true });
}
