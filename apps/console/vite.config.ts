import { readdir, readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { compile } from 'tailwindcss';
import { defineConfig } from 'vite';
import solid from 'vite-plugin-solid';

const root = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);
const outDir = resolve(root, '../../crates/anyagents/src/server/console');

export default defineConfig({
  root,
  plugins: [solid(), {
    name: 'console-tailwind',
    enforce: 'pre',
    async transform(css, id) {
      if (id !== resolve(root, 'src/style.css')) return;
      // Compile Tailwind v4 directly from the lockfile; no native optimizer or CDN.
      const compiler = await compile(css, {
        base: root,
        async loadStylesheet(id) {
          const entry = id === 'tailwindcss' ? 'index.css' : id.startsWith('./') ? id.slice(2) : id;
          if (!['index.css', 'theme.css', 'preflight.css', 'utilities.css'].includes(entry)) {
            throw new Error('Unexpected CSS import: ' + id);
          }
          const path = require.resolve('tailwindcss/' + entry);
          return { path, base: dirname(path), content: await readFile(path, 'utf8') };
        },
      });
      const files = (await readdir(resolve(root, 'src'), { recursive: true }))
        .filter(file => file.endsWith('.tsx')).sort();
      const sources = await Promise.all(files.map(file => {
        const path = resolve(root, 'src', file);
        this.addWatchFile(path);
        return readFile(path, 'utf8');
      }));
      return compiler.build(sources.flatMap(source => source.match(/[\w-]+(?::[\w-]+)*/g) ?? []));
    },
  }, {
    name: 'embedded-html',
    apply: 'build',
    async generateBundle() {
      for (const fileName of ['index.html', 'signin.html', 'auth_recovery.html']) {
        const source = await readFile(resolve(root, 'templates', fileName), 'utf8');
        this.emitFile({ type: 'asset', fileName, source });
      }
    },
  }],
  build: {
    outDir,
    emptyOutDir: true,
    target: 'es2022',
    cssMinify: 'esbuild',
    rollupOptions: {
      input: resolve(root, 'src/main.tsx'),
      output: { entryFileNames: 'console.js', inlineDynamicImports: true, assetFileNames: 'console[extname]' },
    },
  },
});
