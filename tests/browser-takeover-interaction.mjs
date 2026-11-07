// Runs only against the real Rust takeover HTTP server and a separate Chromium
// viewer. Trusted CDP input is independent of the product's fetch transport.
import assert from 'node:assert/strict';
import { writeFile } from 'node:fs/promises';
import path from 'node:path';

export async function interactionAcceptance({ viewer, remote, url, output, until, key, touch, api, screenshot }) {
  const results = [], latency = [];
  await remote.eval(`(() => {
    const fixture = document.createElement('section'); fixture.id='interaction-fixture';
    fixture.innerHTML='<button id="signin" style="position:fixed;left:400px;top:30px;width:200px;height:50px">Sign in</button><div id="paint-marker" style="position:fixed;left:650px;top:30px;width:60px;height:60px;background:rgb(240,0,240)"></div><div id="track" style="position:fixed;left:400px;top:200px;width:320px;height:50px;background:#aaa"><div id="slider" style="width:40px;height:50px;background:#06c;position:absolute;left:0"></div></div><div id="scrollbox" style="position:fixed;left:400px;top:320px;width:320px;height:120px;overflow:auto;background:#fff"><div style="height:2000px">Scroll here</div></div>';
    document.body.append(fixture);
    window.signins=0;window.trajectory=[];window.remoteInputs=0;
    document.querySelector('#entry').addEventListener('input',()=>window.remoteInputs++);
    document.querySelector('#signin').onclick=e=>{
      window.signins++; window.lastSigninTrusted=e.isTrusted;
      document.querySelector('#paint-marker').style.background=window.signins%2?'rgb(0,240,0)':'rgb(240,0,240)';
    };
    const slider=document.querySelector('#slider');let start=null;
    slider.addEventListener('mousedown',e=>{start=e.clientX;window.trajectory.push({type:e.type,trusted:e.isTrusted,buttons:e.buttons})});
    document.addEventListener('mousemove',e=>{if(start!==null){slider.style.left=Math.min(280,Math.max(0,e.clientX-start))+'px';window.trajectory.push({type:e.type,trusted:e.isTrusted,buttons:e.buttons})}});
    document.addEventListener('mouseup',e=>{if(start!==null){start=null;window.trajectory.push({type:e.type,trusted:e.isTrusted,buttons:e.buttons})}});
  })()`);
  async function localPoint(x, y) {
    return viewer.eval(`(() => {const c=document.querySelector('#screen'),r=c.getBoundingClientRect();return {x:r.x+${x}*r.width/Number(c.dataset.remoteWidth),y:r.y+${y}*r.height/Number(c.dataset.remoteHeight)}})()`);
  }
  async function mouse(type, p, buttons = 1) {
    await viewer.call('Input.dispatchMouseEvent', { type, button:'left', buttons, clickCount:1, ...p });
  }
  async function clickRemote(id, mobile) {
    const r = await remote.eval(`(() => {const r=document.querySelector('#${id}').getBoundingClientRect();return {x:r.x+r.width/2,y:r.y+r.height/2}})()`);
    const p = await localPoint(r.x, r.y);
    if (mobile) { await touch('touchStart',[[p.x,p.y]]); await touch('touchEnd',[]); }
    else { await mouse('mousePressed',p); await mouse('mouseReleased',p,0); }
    // CDP only acknowledges delivery to the viewer. The remote click and its
    // editable-focus reply still travel over HTTP; IME requires that reply.
    await until(()=>remote.eval(`document.activeElement.id===${JSON.stringify(id)}`),'remote click '+id);
    if (['entry','password'].includes(id)) {
      await until(()=>viewer.eval(`document.activeElement.id==='keyboard-proxy' && document.activeElement.type===${JSON.stringify(id==='password'?'password':'text')}`),'editable proxy '+id);
    }
  }
  for (const [width,height] of [[1440,900],[834,1194],[390,844]]) {
    const mobile=width<700;
    await viewer.call('Emulation.setDeviceMetricsOverride',{width,height,deviceScaleFactor:1,mobile});
    await viewer.call('Emulation.setTouchEmulationEnabled',{enabled:mobile});
    await viewer.call('Page.navigate',{url});
    await until(()=>viewer.eval(`!!document.querySelector('#screen') && document.querySelector('#connection').hidden`),'interaction connected');
    assert.equal(await viewer.eval(`getComputedStyle(document.querySelector('#screen')).cursor`),'default');
    assert.equal(await viewer.eval(`document.activeElement.id`),'screen','viewer auto-focuses on load');
    await remote.eval(`document.querySelector('#entry').value='';document.querySelector('#password').value=''`);
    await clickRemote('entry',mobile);
    await until(()=>remote.eval(`document.activeElement.id==='entry'`),'click input');
    // Every non-editable focus owner must still route keyboard shortcuts.
    const owners=['body','html','#stage','#hud-toggle'];
    for (const owner of owners) {
      await viewer.eval(`(() => {const el=document.querySelector(${JSON.stringify(owner)});el.tabIndex=-1;el.focus()})()`);
      assert.equal(await viewer.eval(`document.activeElement===document.querySelector(${JSON.stringify(owner)})`),true);
      await remote.eval(`document.querySelector('#entry').value='';document.querySelector('#entry').focus();window.remoteInputs=0`);
      await viewer.eval(`navigator.clipboard.writeText('shortcut fixture ✓')`);
      await key('v','KeyV',86,2);
      await until(()=>remote.eval(`document.querySelector('#entry').value==='shortcut fixture ✓'`),'global Ctrl+V '+owner);
      assert.equal(await remote.eval('window.remoteInputs'),1,'native + fallback cannot insert twice');
      // Refocus owner after a remote ack auto-focuses the keyboard proxy.
      await viewer.eval(`document.querySelector(${JSON.stringify(owner)}).focus()`);
      await key('a','KeyA',65,4); await key('c','KeyC',67,4);
      await until(()=>viewer.eval(`navigator.clipboard.readText().then(t=>t==='shortcut fixture ✓')`),'global Cmd+A/C '+owner);
      await key('x','KeyX',88,4);
      await until(()=>remote.eval(`document.querySelector('#entry').value===''`),'global Cmd+X '+owner);
      await key('z','KeyZ',90,4);
      await until(()=>remote.eval(`document.querySelector('#entry').value==='shortcut fixture ✓'`),'Cmd+Z undo cut');
      await key('z','KeyZ',90,12);
      await until(()=>remote.eval(`document.querySelector('#entry').value===''`),'Cmd+Shift+Z redo cut');
      await key('z','KeyZ',90,2);
      await until(()=>remote.eval(`document.querySelector('#entry').value==='shortcut fixture ✓'`),'Ctrl+Z undo');
      await key('y','KeyY',89,2);
      await until(()=>remote.eval(`document.querySelector('#entry').value===''`),'Ctrl+Y redo');
      results.push({width,owner,globalClipboard:true,undoRedo:true});
    }
    // Hold only the real response: the independent CDP input driver and Rust
    // mutation still run. A late editable-focus acknowledgement cannot steal
    // focus after the person switches to the viewer's local text field.
    await viewer.eval(`window.inputReplyHeld=false;window.originalFetch=window.fetch;window.fetch=async(...args)=>{const response=await window.originalFetch(...args);if(String(args[0]).endsWith('/input')&&args[1]?.body&&JSON.parse(args[1].body).text==='fallback once'){window.inputReplyHeld=true;await new Promise(resolve=>window.releaseInputReply=resolve)}return response}`);
    // The key gesture must work even when no native paste reaches the viewer.
    await viewer.eval(`navigator.clipboard.writeText('fallback once');window.suppressPaste=e=>{e.preventDefault();e.stopImmediatePropagation()};document.addEventListener('paste',window.suppressPaste,{capture:true})`);
    await remote.eval(`window.remoteInputs=0`);
    await key('v','KeyV',86,4);
    await until(()=>remote.eval(`document.querySelector('#entry').value==='fallback once'`),'Cmd+V fallback');
    assert.equal(await remote.eval('window.remoteInputs'),1);
    await until(()=>viewer.eval('window.inputReplyHeld'),'real paste response held');
    await viewer.eval(`document.removeEventListener('paste',window.suppressPaste,{capture:true})`);
    // Local HUD text remains local, including select-all and native paste.
    await viewer.eval(`document.querySelector('#hud-panel').hidden=false;document.querySelector('#type-input').focus();navigator.clipboard.writeText('local only');window.releaseInputReply();new Promise(resolve=>setTimeout(resolve,80))`);
    assert.equal(await viewer.eval('document.activeElement.id'),'type-input','late remote reply preserves local editable focus');
    // This is a real viewport resize, not a synthetic visualViewport event.
    await viewer.call('Emulation.setDeviceMetricsOverride',{width,height:height-80,deviceScaleFactor:1,mobile});
    await until(()=>viewer.eval(`visualViewport.height<=${height-80}`),'local keyboard viewport resize');
    assert.equal(await viewer.eval('document.activeElement.id'),'type-input','viewport resize preserves local editable focus');
    await viewer.call('Emulation.setDeviceMetricsOverride',{width,height,deviceScaleFactor:1,mobile});
    await viewer.eval('window.fetch=window.originalFetch');
    await key('v','KeyV',86,2);
    await until(()=>viewer.eval(`document.querySelector('#type-input').value==='local only'`),'local paste remains local');
    assert.equal(await remote.eval(`document.querySelector('#entry').value`),'fallback once');
    await viewer.eval(`document.querySelector('#hud-panel').hidden=true;document.querySelector('#type-input').value=''`);
    await clickRemote('password',mobile);
    await until(()=>viewer.eval(`document.activeElement.id==='keyboard-proxy'`),'password keyboard focused');
    await viewer.eval(`navigator.clipboard.writeText('fixture-not-a-real-password')`);
    await key('v','KeyV',86,4);
    await until(()=>remote.eval(`document.querySelector('#password').value==='fixture-not-a-real-password'`),'Cmd+V password');
    await key('a','KeyA',65,2); await key('v','KeyV',86,2);
    await until(()=>remote.eval(`document.querySelector('#password').value==='fixture-not-a-real-password'`),'Ctrl+V password');
    // IME and printable input use the real editable keyboard proxy.
    await clickRemote('entry',mobile);
    await key('a','KeyA',65,2);
    await viewer.call('Input.imeSetComposition',{text:'中文',selectionStart:2,selectionEnd:2});
    await viewer.call('Input.insertText',{text:'中文'});
    await until(()=>remote.eval(`document.querySelector('#entry').value==='中文'`),'IME commit once');
    await key('q','KeyQ',81);
    // This driver must supply text for the browser's native default action.
    // Calibrated separately below before judging the proxy commit path.
    await viewer.call('Input.insertText',{text:'!'});
    await until(()=>remote.eval(`document.querySelector('#entry').value==='中文q!'`),'printable insertion');
    await viewer.eval(`document.querySelector('#reset-btn').click()`);
    await remote.eval(`document.querySelector('#slider').style.left='0px';window.trajectory=[]`);
    const start=await localPoint(420,225),end=await localPoint(690,225);
    if(mobile) await touch('touchStart',[[start.x,start.y]]); else await mouse('mousePressed',start);
    for(let i=1;i<=18;i++) {
      const p={x:start.x+(end.x-start.x)*i/18,y:start.y};
      if(mobile) await touch('touchMove',[[p.x,p.y]]); else await mouse('mouseMoved',p);
    }
    if(mobile) await touch('touchEnd',[]); else await mouse('mouseReleased',end,0);
    await until(()=>remote.eval(`parseFloat(document.querySelector('#slider').style.left)>=260 && window.trajectory.at(-1)?.type==='mouseup'`),'press-drag slider');
    const trajectory=await remote.eval('window.trajectory');
    assert.equal(trajectory[0].type,'mousedown');assert.equal(trajectory.at(-1).type,'mouseup');
    assert.ok(trajectory.some(p=>p.type==='mousemove'&&p.buttons===1));assert.ok(trajectory.every(p=>p.trusted));
    await remote.eval(`document.querySelector('#scrollbox').scrollTop=0`);
    const scroll=await localPoint(540,380);
    if(mobile) {
      await touch('touchStart',[[scroll.x-20,scroll.y],[scroll.x+20,scroll.y]]);
      await touch('touchMove',[[scroll.x-20,scroll.y-40],[scroll.x+20,scroll.y-40]]);
      await touch('touchEnd',[]);
    } else await viewer.call('Input.dispatchMouseEvent',{type:'mouseWheel',...scroll,deltaX:0,deltaY:240});
    await until(()=>remote.eval(`document.querySelector('#scrollbox').scrollTop>0`),'wheel/two-finger scroll');
    await viewer.eval(`document.querySelector('#reset-btn').click()`);
    const before=await remote.eval('window.signins');
    await clickRemote('signin',mobile);
    await until(()=>remote.eval(`window.signins===${before+1} && window.lastSigninTrusted`),'trusted sign-in click');
    // Deliberately overlap actual screenshot/state requests with input.
    const reads=[api('/state'),fetch(url+'/frame.jpg').then(r=>r.arrayBuffer())];
    const began=performance.now();
    assert.equal((await api('/input',{type:'key',key:'Escape'})).ok,true);
    const concurrentInputMs=performance.now()-began;
    assert.ok(concurrentInputMs<=150,'input behind frame/state: '+concurrentInputMs);
    await Promise.all(reads);
    results.push({width,drag:true,touch:mobile,wheel:true,passwordPaste:true,ime:true,pasteFallback:true,localTextIsolation:true,concurrentInputMs});
    await screenshot(`${width}-interactions`);
    if(width===1440) {
      // Measure input -> changed decoded JPEG pixel, not a POST acknowledgement.
      for(let i=0;i<20;i++) {
        const count=await remote.eval('window.signins');
        const p=await localPoint(500,55);
        await until(()=>viewer.eval(`(() => {const c=document.querySelector('#screen'),p=c.getContext('2d').getImageData(Math.round(680*c.width/Number(c.dataset.remoteWidth)),Math.round(60*c.height/Number(c.dataset.remoteHeight)),1,1).data;return (p[1]>180&&p[0]<60)===${count%2===1}})()`),'prior painted colour');
        await viewer.eval(`window.paintLatency=null;window.paintStart=performance.now();(() => {const c=document.querySelector('#screen');const sample=()=>{const p=c.getContext('2d').getImageData(Math.round(680*c.width/Number(c.dataset.remoteWidth)),Math.round(60*c.height/Number(c.dataset.remoteHeight)),1,1).data;const green=p[1]>180&&p[0]<60;if(green===${count%2===0})window.paintLatency=performance.now()-window.paintStart;else requestAnimationFrame(sample)};requestAnimationFrame(sample)})()`);
        await mouse('mousePressed',p);await mouse('mouseReleased',p,0);
        const elapsed=await until(()=>viewer.eval('window.paintLatency'),'changed JPEG pixel',5000);
        latency.push(elapsed);
      }
      // Retain every sample before enforcing the contract.
      await writeFile(path.join(output,'input-to-paint-latency.json'),JSON.stringify({unit:'ms',boundary:'trusted viewer press to changed decoded JPEG pixel on viewer rAF',samples:latency,max:Math.max(...latency)},null,2));

    }
  }
  await viewer.call('Network.enable');
  await viewer.call('Network.setBlockedURLs',{urls:['*frame.jpg*','*takeover/*/state']});
  await until(()=>viewer.eval(`!document.querySelector('#connection').hidden && document.querySelector('#connection').textContent==='Reconnecting…'`),'disconnect status');
  await viewer.call('Network.setBlockedURLs',{urls:[]});
  await until(()=>viewer.eval(`document.querySelector('#connection').hidden`),'automatic same-URL reconnect');
  await remote.eval(`document.querySelector('#interaction-fixture').remove();document.querySelector('#entry').value='';document.querySelector('#password').value=''`);
  await writeFile(path.join(output,'interaction-result.json'),JSON.stringify({passed:latency.every(ms=>ms<=150),viewports:3,cases:results,latency,reconnect:true},null,2));
  assert.ok(latency.length===20 && latency.every(ms=>ms<=150),'input-to-frame <=150ms; samples: '+JSON.stringify(latency));
}
