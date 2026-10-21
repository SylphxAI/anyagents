// The launcher must run the native binary for the current platform, take its
// arguments verbatim, forward its exit status, and say something useful when
// there is no binary. Run with `node --test packages/anyagents/test`.
"use strict";
const assert = require("node:assert/strict");
const { mkdtempSync, writeFileSync, chmodSync, rmSync } = require("node:fs");
const { tmpdir } = require("node:os");
const { spawnSync } = require("node:child_process");
const path = require("node:path");
const test = require("node:test");

const launcher = path.resolve(__dirname, "..", "bin", "anyagents.js");
const exe = process.platform === "win32" ? "anyagents.exe" : "anyagents";

function run(args, env) {
  return spawnSync(process.execPath, [launcher, ...args], {
    encoding: "utf8",
    env: { ...process.env, ANYAGENTS_BIN: "", ...env },
  });
}

test("runs the binary named by ANYAGENTS_BIN with the same arguments", () => {
  const dir = mkdtempSync(path.join(tmpdir(), "anyagents-launcher-"));
  try {
    const bin = path.join(dir, exe);
    if (process.platform === "win32") {
      writeFileSync(bin, "");
      return;
    }
    writeFileSync(bin, '#!/bin/sh\nprintf "arg:%s\\n" "$@"\nexit "${ANYAGENTS_TEST_STATUS:-0}"\n');
    chmodSync(bin, 0o755);

    const ok = run(["--help", "two words"], { ANYAGENTS_BIN: bin });
    assert.equal(ok.status, 0, ok.stderr);
    assert.equal(ok.stdout, "arg:--help\narg:two words\n");

    const failed = run([], { ANYAGENTS_BIN: bin, ANYAGENTS_TEST_STATUS: "7" });
    assert.equal(failed.status, 7);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("an override that does not exist is not used", () => {
  const res = run(["--help"], { ANYAGENTS_BIN: path.join(tmpdir(), "anyagents-does-not-exist") });
  // Either a real binary was found (this checkout has been built) or the
  // launcher explains how to get one; it never runs the missing path.
  assert.ok(res.status === 0 || /no native binary/.test(res.stderr), res.stderr);
});
