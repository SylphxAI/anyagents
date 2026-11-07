import { test } from 'node:test';
import assert from 'node:assert/strict';
import { remotePoint, boundedView, zoomAt } from '../crates/anyagents/src/browser_takeover.js';

test('zoomed/panned screenshot maps to remote CSS pixels for DPR 1, 2, 3', () => {
  for (const dpr of [1, 2, 3]) {
    for (const scale of [.203125, 1, 2.75, 8]) {
      for (const offset of [{ x: 0, y: 300 }, { x: -837, y: -413 }]) {
        for (const target of [{ x: 123, y: 78 }, { x: 800, y: 600 }, { x: 1918, y: 1078 }]) {
          const view = { ...offset, scale };
          const actual = remotePoint(target.x * scale + view.x, target.y * scale + view.y, view, 1920 * dpr, 1080 * dpr, dpr);
          assert.ok(Math.abs(actual.x - target.x) < 1e-9);
          assert.ok(Math.abs(actual.y - target.y) < 1e-9);
        }
      }
    }
  }
});
test('pan clamps every edge and centers letterboxed axes', () => {
  assert.deepEqual(boundedView({ x: -9000, y: 9000, scale: 2 }, 390, 844, 1920, 1080), { x: -3450, y: 0, scale: 2 });
  assert.deepEqual(boundedView({ x: 9000, y: -9000, scale: 2 }, 390, 844, 1920, 1080), { x: 0, y: -1316, scale: 2 });
  assert.deepEqual(boundedView({ x: 77, y: 38, scale: .2 }, 390, 844, 1920, 1080), { x: 3, y: 314, scale: .2 });
});
test('pinch preserves the remote point under the pinch center', () => {
  const view = { x: -70, y: -100, scale: .5 };
  const next = zoomAt(view, 2, 195, 200);
  assert.deepEqual(remotePoint(195, 200, view, 1920, 1080, 1), remotePoint(195, 200, next, 1920, 1080, 1));
});
test('fresh screenshot session raster scale can differ from document DPR', () => {
  for (const device_scale_factor of [1, 2, 3]) {
    const actual = remotePoint(195, 400, { x: -765, y: -140, scale: 1 }, 1920, 1080, { width: 1920, height: 1080, device_scale_factor });
    assert.deepEqual(actual, { x: 960, y: 540 });
  }
});
