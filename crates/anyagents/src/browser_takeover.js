// All geometry is in local CSS pixels and remote CSS pixels, never display pixels.
// The screenshot's natural size / remote DPR defines the remote CSS extent.
export function remoteExtent(naturalWidth, naturalHeight, metrics) {
  const dpr = typeof metrics === 'number' ? metrics : metrics.device_scale_factor || 1;
  // CDP screenshot sessions can have a different raster scale than the page's
  // DPR override. Prefer the CSS viewport captured with this exact frame.
  return { width: metrics.width || naturalWidth / dpr, height: metrics.height || naturalHeight / dpr };
}
export function remotePoint(clientX, clientY, view, naturalWidth, naturalHeight, metrics) {
  const extent = remoteExtent(naturalWidth, naturalHeight, metrics);
  return {
    x: Math.max(0, Math.min(extent.width, (clientX - view.x) / view.scale)),
    y: Math.max(0, Math.min(extent.height, (clientY - view.y) / view.scale)),
  };
}
export function boundedView(view, width, height, remoteWidth, remoteHeight) {
  const bound = (offset, size, extent) => extent <= size ? (size - extent) / 2 : Math.min(0, Math.max(size - extent, offset));
  return { ...view, x: bound(view.x, width, remoteWidth * view.scale), y: bound(view.y, height, remoteHeight * view.scale) };
}
export function zoomAt(view, scale, x, y) {
  return { scale, x: x - (x - view.x) * scale / view.scale, y: y - (y - view.y) * scale / view.scale };
}

// Keep discrete actions ordered while retaining only the newest unsent motion.
// Coalescing never crosses a press/release/key boundary; wheels retain distance.
export function inputDispatcher(request, onError = () => {}) {
  const queue = [];
  let running = false, tail = Promise.resolve();
  async function drain() {
    if (running) return;
    running = true;
    while (queue.length) {
      const item = queue.shift();
      try { item.resolve(await request(item.payload)); }
      catch (error) { item.reject(error); onError(error); }
    }
    running = false;
  }
  return {
    send(payload) {
      const last = queue.at(-1);
      if (last && payload.type === 'mouse' && payload.event_type === 'mouseMoved' &&
          last.payload.type === 'mouse' && last.payload.event_type === 'mouseMoved') {
        last.payload = payload; return last.promise;
      }
      if (last && payload.type === 'wheel' && last.payload.type === 'wheel') {
        last.payload = { ...payload, delta_x: last.payload.delta_x + payload.delta_x, delta_y: last.payload.delta_y + payload.delta_y };
        return last.promise;
      }
      const item = { payload };
      item.promise = new Promise((resolve, reject) => { item.resolve = resolve; item.reject = reject; });
      queue.push(item); tail = item.promise.catch(() => {}); drain();
      return item.promise;
    },
    idle: () => tail,
  };
}

if (typeof document !== 'undefined') {
  const { token } = JSON.parse(document.getElementById('takeover-config').textContent);
  const BASE = '/takeover/' + encodeURIComponent(token);
  const $ = id => document.getElementById(id);
  const canvas = $('screen'), ctx = canvas.getContext('2d'), stage = $('stage'), proxy = $('keyboard-proxy');
  let view = { x: 0, y: 0, scale: 1 }, metrics = { device_scale_factor: 1 }, ready = false, completed = false, fitMode = true;
  let phone = false, remoteFocus = null, keyboardActive = false, composing = false;
  let frameRevision = 0, lastFrame = 0;
  const pointers = new Map();
  let gesture = null, pressed = null, touchPan = false, motion = null, motionFrame = 0, pairFrame = 0;
  let touchTimer = 0, focusSequence = 0;
  // Proxy focus is an acknowledgement of remote ownership, not a new choice.
  document.addEventListener('focusin', e => { if (e.target !== proxy) focusSequence++; });
  // Blurring into a non-focusable local surface has no matching focusin.
  document.addEventListener('focusout', e => { if (e.relatedTarget !== proxy) focusSequence++; });
  const size = () => ({ width: stage.clientWidth, height: stage.clientHeight });
  const extent = () => remoteExtent(canvas.width, canvas.height, metrics);
  const fitScale = () => Math.min(size().width / extent().width, size().height / extent().height);
  const point = (x, y) => remotePoint(x, y, view, canvas.width, canvas.height, metrics);
  function paintView() {
    const s = size(), r = extent();
    view = boundedView(view, s.width, s.height, r.width, r.height);
    canvas.style.width = r.width + 'px'; canvas.style.height = r.height + 'px';
    canvas.style.transform = `translate(${view.x}px, ${view.y}px) scale(${view.scale})`;
    $('fit-btn').textContent = fitMode ? '1:1' : 'Fit';
    placeKeyboard();
  }
  function reset() { fitMode = true; view = { x: 0, y: 0, scale: fitScale() }; paintView(); }
  function zoom(scale, x, y) {
    fitMode = false;
    view = zoomAt(view, Math.min(8, Math.max(fitScale(), scale)), x, y); paintView();
  }
  function reconnect() { $('connection').hidden = false; $('connection').textContent = 'Reconnecting…'; }
  async function request(path, payload) {
    const res = await fetch(BASE + path, payload === undefined ? { signal: AbortSignal.timeout(5000) } : {
      signal: AbortSignal.timeout(5000),
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload),
    });
    if (!res.ok) throw new Error('Remote connection failed');
    const result = await res.json();
    if (result.ok === false) throw new Error(result.error || 'Remote request failed');
    return result;
  }
  const dispatcher = inputDispatcher(payload => completed ? null : request('/input', payload), reconnect);
  const sendInput = payload => dispatcher.send(payload);
  function sendWithFocus(payload) {
    // Capture ownership when the action is accepted, not when its queue drains.
    const sequence = focusSequence;
    return sendInput(payload).then(result => {
      if (!completed && sequence === focusSequence && result?.focus) focusField(result.focus);
      return result;
    });
  }
  function flushMotion() {
    cancelAnimationFrame(motionFrame); motionFrame = 0;
    if (motion) { sendInput(motion).catch(reconnect); motion = null; }
  }
  function move(payload) {
    motion = payload;
    if (!motionFrame) motionFrame = requestAnimationFrame(flushMotion);
  }
  function placeKeyboard() {
    const r = remoteFocus?.rect;
    const active = !!r && remoteFocus.editable && remoteTarget(document.activeElement) && matchMedia('(pointer: coarse)').matches;
    proxy.style.pointerEvents = active ? 'auto' : 'none';
    if (!active) return;
    proxy.style.left = (view.x + r.x * view.scale) + 'px';
    proxy.style.top = (view.y + r.y * view.scale) + 'px';
    proxy.style.width = (r.width * view.scale) + 'px';
    proxy.style.height = (r.height * view.scale) + 'px';
  }
  function openKeyboard() {
    keyboardActive = true;
    proxy.inputMode = remoteFocus?.input_mode || 'text';
    proxy.type = remoteFocus?.input_type === 'password' ? 'password' : 'text';
    placeKeyboard();
    proxy.focus({ preventScroll: true });
  }
  function focusField(focus) {
    remoteFocus = focus;
    // An HTTP acknowledgement can arrive after the person chose local text.
    // Update remote metadata, but never move their new editable focus away.
    if (!remoteTarget(document.activeElement)) {
      keyboardActive = false; proxy.style.pointerEvents = 'none'; return;
    }
    if (!focus?.editable) { keyboardActive = false; proxy.style.pointerEvents = 'none'; proxy.blur(); return; }
    openKeyboard();
    if (!matchMedia('(pointer: coarse)').matches) return;
    const s = size(), r = focus.rect;
    const scale = Math.min(4, Math.max(fitScale(), 1, (s.width - 32) / Math.max(r.width, 200)));
    fitMode = false;
    view = { scale, x: s.width / 2 - (r.x + r.width / 2) * scale, y: Math.min(s.height * .28, 180) - (r.y + r.height / 2) * scale };
    paintView();
  }
  const pair = () => {
    const [a, b] = [...pointers.values()];
    return { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2, distance: Math.max(1, Math.hypot(a.x - b.x, a.y - b.y)) };
  };
  function releasePointer() {
    flushMotion();
    if (!pressed) return;
    const p = pressed; pressed = null;
    sendWithFocus({ type: 'mouse', event_type: 'mouseReleased', ...p.point, button: p.button, buttons: 0 }).catch(reconnect);
  }
  function cancelTouchTimer() { clearTimeout(touchTimer); touchTimer = 0; }
  function cancelPointer() {
    flushMotion();
    if (!pressed) return;
    const p = pressed; pressed = null;
    // A late second finger must end a held press without activating its target.
    sendInput({ type: 'mouseCancel', ...p.point, button: p.button }).catch(reconnect);
  }
  function pressPointer(g) {
    cancelTouchTimer();
    if (g.pointerType !== 'mouse' && matchMedia('(pointer: coarse)').matches) openKeyboard();
    else canvas.focus({ preventScroll: true });
    pressed = { point: g.point, button: g.button, buttons: g.buttons };
    sendInput({ type: 'mouse', event_type: 'mousePressed', ...pressed.point, button: pressed.button, buttons: pressed.buttons }).catch(reconnect);
  }
  stage.addEventListener('pointerdown', e => {
    if (!ready || completed) return;
    e.preventDefault();
    stage.setPointerCapture(e.pointerId);
    pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
    if (pointers.size === 2) {
      cancelTouchTimer(); cancelPointer();
      gesture = { kind: 'pair', ...pair(), last: pair(), view: { ...view } };
    } else if (pointers.size === 1) {
      // Immediate pairs never press. A stationary hold commits after 180ms
      // for press-and-hold challenges; a later pair explicitly cancels it.
      const button = e.button === 2 ? 'right' : e.button === 1 ? 'middle' : 'left';
      const g = gesture = { kind: 'single', x: e.clientX, y: e.clientY, view: { ...view },
        point: point(e.clientX, e.clientY), button, buttons: e.pointerType === 'mouse' ? e.buttons : 1, pointerType: e.pointerType };
      if (e.pointerType !== 'mouse' && touchPan) return;
      if (e.pointerType === 'mouse') pressPointer(g);
      else touchTimer = setTimeout(() => { if (gesture === g && pointers.size === 1) pressPointer(g); }, 180);
    }
  });
  function flushPairMotion() {
    if (!pairFrame) return;
    cancelAnimationFrame(pairFrame); pairFrame = 0;
    if (pointers.size < 2 || gesture?.kind !== 'pair') return;
    const p = pair(), g = gesture;
    if (g.zooming || Math.abs(p.distance / g.distance - 1) > .08) {
      g.zooming = true;
      const scale = Math.min(8, Math.max(fitScale(), g.view.scale * p.distance / g.distance));
      view = zoomAt(g.view, scale, g.x, g.y);
      view.x += p.x - g.x; view.y += p.y - g.y; fitMode = false; paintView();
    } else {
      sendInput({ type: 'wheel', ...point(g.x, g.y), delta_x: (g.last.x - p.x) / view.scale, delta_y: (g.last.y - p.y) / view.scale }).catch(reconnect);
    }
    g.last = p;
  }
  stage.addEventListener('pointermove', e => {
    if (!pointers.has(e.pointerId)) return;
    e.preventDefault(); pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
    if (pointers.size >= 2 && gesture?.kind === 'pair') {
      // Pointer Events deliver each finger separately. Evaluate one native
      // sample together, or a parallel translation looks like a false pinch.
      if (!pairFrame) pairFrame = requestAnimationFrame(flushPairMotion);
    } else {
      if (!pressed && gesture?.kind === 'single' && !touchPan && Math.hypot(e.clientX - gesture.x, e.clientY - gesture.y) >= 8) pressPointer(gesture);
      if (pressed) {
        pressed.point = point(e.clientX, e.clientY);
        move({ type: 'mouse', event_type: 'mouseMoved', ...pressed.point, button: pressed.button, buttons: pressed.buttons });
      } else if (gesture?.kind === 'single' && touchPan) {
        view.x = gesture.view.x + e.clientX - gesture.x; view.y = gesture.view.y + e.clientY - gesture.y; paintView();
      }
    }
  });
  function endPointer(e) {
    if (!pointers.has(e.pointerId)) return;
    cancelTouchTimer(); flushPairMotion();
    if (!pressed && gesture?.kind === 'single' && !touchPan && e.type === 'pointerup') pressPointer(gesture);
    if (pressed) pressed.point = point(e.clientX, e.clientY);
    releasePointer(); pointers.delete(e.pointerId);
    // The remaining finger after a two-finger gesture cannot click/drag.
    gesture = null;
  }
  stage.addEventListener('pointerup', endPointer);
  stage.addEventListener('pointercancel', endPointer);
  stage.addEventListener('lostpointercapture', endPointer);
  addEventListener('blur', () => { focusSequence++; cancelTouchTimer(); flushPairMotion(); releasePointer(); pointers.clear(); gesture = null; });
  stage.addEventListener('contextmenu', e => e.preventDefault());
  stage.addEventListener('wheel', e => {
    e.preventDefault();
    if (e.ctrlKey || e.metaKey) zoom(view.scale * Math.exp(-e.deltaY * .01), e.clientX, e.clientY);
    else sendInput({ type: 'wheel', ...point(e.clientX, e.clientY), delta_x: e.deltaX, delta_y: e.deltaY }).catch(reconnect);
  }, { passive: false });
  $('touch-mode-btn').onclick = () => {
    touchPan = !touchPan;
    $('touch-mode-btn').textContent = touchPan ? 'Touch: pan view' : 'Touch: interact';
    $('touch-mode-btn').setAttribute('aria-pressed', String(touchPan));
  };
  const remoteTarget = target => target === proxy || !(target?.closest?.('#type-input, #hud-panel input, #hud-panel textarea, #hud-panel [contenteditable="true"]'));
  const modifiers = e => (e.altKey ? 1 : 0) | (e.ctrlKey ? 2 : 0) | (e.metaKey ? 4 : 0) | (e.shiftKey ? 8 : 0);
  function clipboardError(error) { $('clipboard-status').textContent = error?.message?.includes('Cut cancelled') ? error.message : 'Allow clipboard access, or use your device’s Paste menu.'; }
  async function clipboardOut(action = 'copy') {
    if (remoteFocus?.input_type === 'password') return;
    const selection = sendInput({ type: 'clipboard', action: 'copy' });
    // Start the native write in the user gesture. WebKit requires this before
    // the asynchronous remote selection arrives, so provide a promised Blob.
    try {
      if (navigator.clipboard?.write && typeof ClipboardItem !== 'undefined') {
        await navigator.clipboard.write([new ClipboardItem({ 'text/plain': selection.then(result => new Blob([result.text || ''], { type: 'text/plain' })) })]);
      } else {
        const result = await selection;
        await navigator.clipboard.writeText(result.text || '');
      }
      const result = await selection;
      if (action === 'cut') await sendInput({ type: 'clipboard', action: 'cut', expected_text: result.text || '', selection_key: result.selection_key });
      $('clipboard-status').textContent = '';
    } catch (error) { clipboardError(error); }
  }
  function pasteText(text) {
    $('clipboard-status').textContent = '';
    return sendWithFocus({ type: 'paste', text });
  }
  let pendingPaste = null;
  document.addEventListener('paste', e => {
    if (!remoteTarget(e.target) || completed) return;
    e.preventDefault();
    // One native paste consumes one shortcut turn, regardless of trust.
    // Later Paste-menu actions must not be swallowed by a timing window.
    const attempt = pendingPaste; pendingPaste = null;
    if (attempt) {
      if (attempt.inserted) return;
      attempt.inserted = true;
    }
    pasteText(e.clipboardData?.getData('text/plain') || '').catch(reconnect);
  });
  for (const action of ['copy', 'cut']) document.addEventListener(action, e => {
    if (!remoteTarget(e.target) || completed) return;
    e.preventDefault(); clipboardOut(action);
  });
  function remoteKey(e) {
    if (e.isComposing || composing || completed || !remoteTarget(e.target)) return;
    const shortcut = e.ctrlKey || e.metaKey, key = e.key.toLowerCase();
    // These belong to the viewer's browser, never the remote page.
    if (shortcut && ['w', 't', 'l'].includes(key)) return;
    // The paste event carries the real local clipboard. Forwarding keydown
    // as well would insert twice or type a literal 'v'.
    if (shortcut && key === 'v') {
      const attempt = pendingPaste = { inserted: false };
      // Begin the permission-sensitive read inside the gesture, but let the
      // native paste event win. Neither path may insert a second time.
      const read = navigator.clipboard?.readText?.().then(text => ({ text }), error => ({ error }));
      setTimeout(async () => {
        if (attempt.inserted || !read) return;
        const result = await read;
        if (attempt.inserted) return;
        if (result.error) { clipboardError(result.error); return; }
        attempt.inserted = true;
        pasteText(result.text).catch(reconnect);
      }, 0);
      return;
    }
    if (shortcut && ['c', 'x'].includes(key)) { e.preventDefault(); clipboardOut(key === 'x' ? 'cut' : 'copy'); return; }
    if (e.target === proxy && !shortcut && e.key.length === 1) return;
    if (['Control', 'Meta', 'Shift', 'Alt'].includes(e.key)) return;
    e.preventDefault();
    sendWithFocus({ type: 'key', key: e.key, modifiers: modifiers(e), commands: shortcut && key === 'a' ? ['selectAll'] : [] }).catch(reconnect);
  }
  window.addEventListener('keydown', remoteKey);
  // Native shortcut paste fires before keyup. End an unmatched fallback turn
  // here too, so a later OS/menu paste remains an independent action.
  window.addEventListener('keyup', e => { if (e.key.toLowerCase() === 'v') pendingPaste = null; });
  window.addEventListener('blur', () => { pendingPaste = null; });
  canvas.focus({ preventScroll: true });
  proxy.addEventListener('compositionstart', () => { composing = true; });
  function commit() {
    if (proxy.value) { sendInput({ type: 'text', text: proxy.value }).catch(reconnect); proxy.value = ''; }
  }
  proxy.addEventListener('compositionend', () => { composing = false; commit(); });
  proxy.addEventListener('input', () => { if (!composing) commit(); });
  proxy.addEventListener('beforeinput', e => {
    if (e.inputType.startsWith('delete')) { e.preventDefault(); sendInput({ type: 'key', key: 'Backspace' }).catch(reconnect); }
  });
  // Do not steal the local keyboard focus when a shortcut pill is tapped.
  for (const id of ['paste-btn', 'copy-btn', 'select-all-btn', 'tab-btn', 'esc-btn']) {
    $(id).addEventListener('pointerdown', e => e.preventDefault());
  }
  $('paste-btn').onclick = async () => {
    try { await pasteText(await navigator.clipboard.readText()); } catch (_) { clipboardError(); }
  };
  $('copy-btn').onclick = () => clipboardOut();
  $('select-all-btn').onclick = () => sendInput({ type: 'key', key: 'a', modifiers: 2, commands: ['selectAll'] }).catch(reconnect);
  $('tab-btn').onclick = () => sendWithFocus({ type: 'key', key: 'Tab' }).catch(reconnect);
  $('esc-btn').onclick = () => sendInput({ type: 'key', key: 'Escape' }).catch(reconnect);
  $('hud-toggle').onclick = () => {
    const expanded = $('hud-panel').hidden;
    $('hud-panel').hidden = !expanded; $('hud-toggle').setAttribute('aria-expanded', String(expanded));
  };
  $('keyboard-btn').onclick = openKeyboard;
  function sendText(submit = false) {
    const input = $('type-input');
    if (input.value) { sendInput({ type: 'text', text: input.value, submit }).catch(reconnect); input.value = ''; }
    else if (submit) sendInput({ type: 'key', key: 'Enter' }).catch(reconnect);
  }
  $('send-text-btn').onclick = () => sendText();
  $('type-input').onkeydown = e => { if (e.key === 'Enter') { e.preventDefault(); sendText(true); } };
  $('enter-btn').onclick = () => sendText(true);
  $('bksp-btn').onclick = () => sendInput({ type: 'key', key: 'Backspace' }).catch(reconnect);
  $('reload-btn').onclick = () => sendInput({ type: 'reload' }).catch(reconnect);
  $('reset-btn').onclick = reset;
  $('fit-btn').onclick = () => {
    if (!fitMode) reset(); else { fitMode = false; view = zoomAt(view, 1, size().width / 2, size().height / 2); paintView(); }
  };
  function setPhone(enabled) { phone = enabled; $('viewport-btn').textContent = 'Phone layout: ' + (phone ? 'on' : 'off'); $('viewport-btn').setAttribute('aria-pressed', String(phone)); }
  $('viewport-btn').onclick = async () => {
    $('viewport-btn').disabled = true;
    try {
      const res = await request('/viewport', { enabled: !phone, width: Math.min(430, Math.max(390, innerWidth)), height: Math.max(600, innerHeight) });
      setPhone(res.phone_layout); frameRevision++; ready = false; fitMode = true;
    } catch (_) { reconnect(); } finally { $('viewport-btn').disabled = false; }
  };
  function finish() { completed = true; proxy.blur(); $('done-overlay').hidden = false; }
  $('done-btn').onclick = async () => {
    $('done-btn').disabled = true;
    try { await dispatcher.idle(); await request('/complete', { note: 'completed by user in takeover UI' }); finish(); }
    catch (_) { reconnect(); $('done-btn').disabled = false; }
  };
  // A normal navigation restores immediately; an abrupt disconnect uses the server's heartbeat lease.
  addEventListener('pagehide', () => { if (phone && !completed) navigator.sendBeacon(BASE + '/viewport', new Blob([JSON.stringify({ enabled: false })], { type: 'application/json' })); });
  addEventListener('resize', () => { if (ready) { if (fitMode) reset(); else paintView(); } });
  if (window.visualViewport) visualViewport.addEventListener('resize', () => {
    const vv = visualViewport;
    $('hud').style.bottom = `calc(${Math.max(0, innerHeight - vv.height - vv.offsetTop) + 10}px + env(safe-area-inset-bottom))`;
    if (document.activeElement === proxy && keyboardActive && remoteFocus?.editable) focusField(remoteFocus);
  });
  async function pollFrame() {
    if (completed) return;
    const revision = frameRevision;
    try {
      const res = await fetch(BASE + '/frame.jpg?t=' + Date.now(), { signal: AbortSignal.timeout(5000) });
      if (!res.ok) throw new Error('frame');
      const image = await createImageBitmap(await res.blob());
      if (revision === frameRevision) {
        const changed = canvas.width !== image.width || canvas.height !== image.height;
        canvas.width = image.width; canvas.height = image.height;
        metrics = { width: Number(res.headers.get('X-Remote-Width')), height: Number(res.headers.get('X-Remote-Height')), device_scale_factor: Number(res.headers.get('X-Remote-Dpr')) || 1 };
        canvas.dataset.remoteWidth = extent().width; canvas.dataset.remoteHeight = extent().height; canvas.dataset.remoteDpr = metrics.device_scale_factor;
        ctx.drawImage(image, 0, 0);
        if (!ready || changed) { ready = true; reset(); }
        else paintView();
        lastFrame = Date.now(); $('connection').hidden = true;
      }
      image.close();
    } catch (_) { reconnect(); }
    // One capture in flight; do not add a whole display frame after decoding.
    setTimeout(pollFrame, 8);
  }
  async function pollState() {
    if (completed) return;
    try {
      const st = await request('/state');
      $('page-url').textContent = st.url + (st.title ? ' — ' + st.title : '');
      $('captcha-badge').textContent = st.captcha_detected ? 'Challenge active' : 'No challenge detected';
      setPhone(!!st.phone_layout);
      if (st.completed) { finish(); return; }
      if (!lastFrame || Date.now() - lastFrame > 3000) reconnect();
    } catch (_) { reconnect(); }
    setTimeout(pollState, 800);
  }
  pollFrame(); pollState();
}
