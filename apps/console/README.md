# AnyAgents Console

The coordinator serves this React application from assets embedded in the Rust binary. Node.js (20.19+ or 22.12+) is only needed when changing the frontend.

From this directory, run:

```sh
npm ci
npm run build
```

The build runs strict TypeScript checks, then writes `console.js`, `console.css`, and the three HTML templates into `crates/anyagents/src/server/console/`. Commit those generated files alongside source changes; Rust builds do not run npm. Edit the source templates in `templates/`, not the embedded copies. Keep their Rust substitution markers intact and scripts external so the coordinator's `script-src 'self'` policy remains effective. Legacy console script URLs are aliases for the same generated entry point.

The shipped React, Tailwind, and Lucide code is MIT-licensed. Vite's build-only PostCSS dependency uses `source-map-js` (BSD-3-Clause); it is not emitted in the console bundle.
