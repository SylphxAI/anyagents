#!/usr/bin/env node
// npm launcher: runs this package's native binary for the current platform.
// The same file serves every binary; it reads the rest from package.json:
//   "bin": { "<name>": "bin/<name>.js" }       -> binary name
//   optionalDependencies "<package>-<platform>" -> platform packages
// Override the binary with <NAME>_BIN=/path/to/binary.
"use strict";
const { spawnSync } = require("node:child_process");
const { existsSync } = require("node:fs");
const path = require("node:path");

const pkg = require(path.join(__dirname, "..", "package.json"));
const name = Object.keys(pkg.bin)[0];
const PLATFORMS = {
  "darwin-arm64": "darwin-arm64",
  "darwin-x64": "darwin-x64",
  "linux-x64": "linux-x64-gnu",
  "linux-arm64": "linux-arm64-gnu",
};

function resolveBinary() {
  const override = process.env[`${name.toUpperCase().replace(/-/g, "_")}_BIN`];
  if (override && existsSync(override)) return override;
  const exe = process.platform === "win32" ? `${name}.exe` : name;
  const key = PLATFORMS[`${process.platform}-${process.arch}`];
  if (key) {
    try {
      return require.resolve(`${pkg.name}-${key}/${exe}`);
    } catch {}
  }
  // Development checkout: <repo>/packages/<pkg>/bin -> <repo>/target/.
  const root = path.resolve(__dirname, "..", "..", "..");
  for (const p of [path.join(root, "target", "release", exe), path.join(root, "target", "debug", exe)]) {
    if (existsSync(p)) return p;
  }
  return null;
}

const bin = resolveBinary();
if (!bin) {
  console.error(
    `${name}: no native binary for ${process.platform}-${process.arch}.\n` +
      "Supported: macOS (arm64, x64), Linux glibc (x64, arm64).\n" +
      "If optional dependencies were skipped, reinstall without --no-optional."
  );
  process.exit(1);
}
const res = spawnSync(bin, process.argv.slice(2), { stdio: "inherit", windowsHide: true });
if (res.error) {
  console.error(`${name}: failed to start ${bin}: ${res.error.message}`);
  process.exit(1);
}
process.exit(res.status === null ? 1 : res.status);
