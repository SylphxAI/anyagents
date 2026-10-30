# @sylphx/anyagents

Run any coding agent harness as a long-lived agent on your own machine or server: install and sign in Claude Code, Codex, OpenCode, Gemini CLI and more, keep every agent running, connect each one to Telegram, and see what each one costs.

The npm package is not published yet. Build from source using the [repository installation instructions](https://github.com/SylphxAI/anyagents#install-from-source).

This package is a thin launcher. Installing it also installs the `anyagents` binary for your platform as an optional dependency (`@sylphx/anyagents-<platform>`), so there is no toolchain and no build step.

Once published:

```bash
npx @sylphx/anyagents --help
# or
npm install -g @sylphx/anyagents
anyagents serve
```

Supported platforms: macOS (arm64 and x64), Linux glibc (x64 and arm64). Linux requires glibc 2.34 or newer and kernel 5.9 or newer. If optional dependencies were skipped during install, reinstall without `--no-optional`. To run a binary you built yourself, set `ANYAGENTS_BIN=/path/to/anyagents`.

Documentation, the design and the source are at [github.com/SylphxAI/anyagents](https://github.com/SylphxAI/anyagents).

MIT licensed.
