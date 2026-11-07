import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';

// Execute the shipped handlers, with deterministic frame, clock and HTTP edges.
// Real trusted input and remote DOM effects are also covered by the Chromium suite.
async function mount() {
  class Target {
    listeners = new Map(); style = {}; dataset = {}; value = ''; hidden = false;
    clientWidth = 390; clientHeight = 844; width = 1280; height = 800;
    constructor(id) { this.id = id; }
    addEventListener(type, fn) { const list = this.listeners.get(type) || []; list.push(fn); this.listeners.set(type, list); }
    emit(type, props = {}) { const e = { type, target: this, preventDefault() {}, ...props }; for (const fn of this.listeners.get(type) || []) fn(e); }
    focus() { if (document.activeElement === this) return; document.activeElement = this; document.emit('focusin', { target: this }); }
    blur() { document.activeElement = body; document.emit('focusin', { target: body }); }
    closest() { return this.id === 'type-input' ? this : null; }
    getContext() { return { drawImage() {} }; }
    setPointerCapture() {}
    setAttribute() {}
  }
  const elements = new Map(), element = id => { if (!elements.has(id)) elements.set(id, new Target(id)); return elements.get(id); };
  const document = new Target('document'), body = element('body');
  document.activeElement = body; document.getElementById = element;
  element('takeover-config').textContent = JSON.stringify({ token: 'fixture' });
  const inputs = [], held = [], timers = new Map(); let nextTimer = 0, hold = false;
  const focus = { editable: true, rect: { x: 10, y: 10, width: 280, height: 48 } };
  const context = {
    document, window: new Target('window'), innerWidth: 390, innerHeight: 844,
    matchMedia: () => ({ matches: true }), AbortSignal, performance,
    requestAnimationFrame: fn => { const id = ++nextTimer; timers.set(id, fn); return id; },
    cancelAnimationFrame: id => timers.delete(id),
    setTimeout: (fn, ms) => { const id = ++nextTimer; timers.set(id, { fn, ms }); return id; },
    clearTimeout: id => timers.delete(id),
    addEventListener() {}, navigator: { clipboard: { readText: async () => 'native paste' } },
    createImageBitmap: async () => ({ width: 1280, height: 800, close() {} }),
    fetch: async (url, options) => {
      if (url.includes('/input')) {
        inputs.push(JSON.parse(options.body));
        if (hold) await new Promise(resolve => held.push(resolve));
      }
      return { ok: true, json: async () => ({ ok: true, focus }), blob: async () => ({}), headers: { get: name => ({ 'X-Remote-Width': '1280', 'X-Remote-Height': '800', 'X-Remote-Dpr': '1' })[name] } };
    },
  };
  const source = await readFile(new URL('../crates/anyagents/src/browser_takeover.js', import.meta.url), 'utf8');
  vm.runInNewContext(source.replace(/^export /gm, ''), context);
  const settle = async () => { for (let i = 0; i < 30; i++) await Promise.resolve(); };
  await settle();
  const stage = element('stage');
  const pointer = (type, id, x = 100, y = 200) => stage.emit(type, { pointerId: id, pointerType: 'touch', clientX: x, clientY: y, button: 0, buttons: type === 'pointerup' ? 0 : 1 });
  return { element, inputs, pointer, settle, document, timers, window: context.window, hold: () => { hold = true; }, release: () => { hold = false; held.splice(0).forEach(resolve => resolve()); } };
}

test('two-finger scroll beginning on a button sends no press or release', async () => {
  const ui = await mount();
  ui.pointer('pointerdown', 1);
  ui.pointer('pointermove', 1, 102, 202); // Within the tap/gesture slop.
  ui.pointer('pointerdown', 2, 140, 200);
  ui.pointer('pointermove', 1, 102, 160); ui.pointer('pointermove', 2, 140, 160);
  ui.pointer('pointerup', 2, 140, 160); ui.pointer('pointerup', 1, 102, 160);
  await ui.settle();
  assert.deepEqual(ui.inputs.filter(p => p.type === 'mouse'), []);
});

test('one touch tap sends exactly one press and release', async () => {
  const ui = await mount();
  ui.pointer('pointerdown', 1); ui.pointer('pointerup', 1);
  await ui.settle();
  assert.deepEqual(ui.inputs.map(p => p.event_type), ['mousePressed', 'mouseReleased']);
});

test('touch drag retains the initial press point and releases after motion', async () => {
  const ui = await mount();
  ui.pointer('pointerdown', 1); ui.pointer('pointermove', 1, 160, 200); ui.pointer('pointerup', 1, 180, 200);
  await ui.settle();
  assert.deepEqual(ui.inputs.map(p => p.event_type), ['mousePressed', 'mouseMoved', 'mouseReleased']);
  assert.ok(ui.inputs[0].x < ui.inputs[1].x && ui.inputs[1].x < ui.inputs[2].x);
});

test('cancelled unresolved touch sends no click', async () => {
  const ui = await mount();
  ui.pointer('pointerdown', 1); ui.pointer('pointercancel', 1);
  await ui.settle();
  assert.deepEqual(ui.inputs, []);
});

test('late focus acknowledgement preserves a newly focused local HUD input', async () => {
  const ui = await mount(); ui.hold();
  ui.pointer('pointerdown', 1); ui.pointer('pointerup', 1);
  ui.element('type-input').focus(); ui.release(); await ui.settle();
  assert.equal(ui.document.activeElement.id, 'type-input');
});

test('late focus acknowledgement cannot reclaim focus after local focus moves again', async () => {
  const ui = await mount(); ui.hold();
  ui.pointer('pointerdown', 1); ui.pointer('pointerup', 1);
  ui.element('type-input').focus(); ui.element('hud-toggle').focus();
  ui.release(); await ui.settle();
  assert.equal(ui.document.activeElement.id, 'hud-toggle');
});

test('an explicit paste is not swallowed as a previous shortcut duplicate', async () => {
  const ui = await mount();
  const target = ui.element('keyboard-proxy'); target.focus();
  ui.window.emit('keydown', { target, key: 'v', ctrlKey: true });
  const clipboardData = { getData: () => 'native paste' };
  ui.document.emit('paste', { target, clipboardData, isTrusted: true });
  await ui.settle();
  ui.document.emit('paste', { target, clipboardData: { getData: () => 'explicit paste' }, isTrusted: false });
  await ui.settle();
  assert.deepEqual(ui.inputs.filter(p => p.type === 'paste').map(p => p.text), ['native paste', 'explicit paste']);
});
