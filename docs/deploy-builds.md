# Exact-commit deploy builds

A deployment that tracks main between version releases pins one commit and the
SHA-256 of the binary built from it. `.github/workflows/deploy-build.yml`
produces that binary; `release.yml` still owns versions, npm and `v*` tags.

```sh
gh workflow run deploy-build -R SylphxAI/anyagents -f commit=<full main sha>
```

1. **Admission** (`scripts/deploy_build.py admit`). The commit must be a full
   SHA reachable from live main (GitHub compare `ahead`/`identical`, bound to the
   requested SHA) with its own completed, successful `ci` run for event `push`
   on `main` in this repository. Unreadable or incomplete answers refuse.
2. **Build.** `cargo build --release --locked -p anyagents` at that commit on
   `ubuntu-22.04` (glibc 2.35), a GitHub-hosted runner reachable only while the
   repository is public.
3. **Identity proof** (`scripts/deploy_build.py identity`). The built binary is
   started once against an empty data directory; its
   [serving identity](serving-identity.md) must report `source_sha` equal to
   the commit, `binary_sha256` equal to the file's digest, and the workspace
   version. Only then is anything published.
4. **Publication.** Prerelease `deploy-<sha>` (never marked latest) carrying
   `anyagents-linux-x64-gnu`, `anyagents-linux-x64-gnu.sha256` and
   `identity.json` (`asset`, `commit`, `version`, `sha256`, `tag`). A complete
   release is never rebuilt or replaced; a partial one refuses.

A consumer downloads the asset by tag, verifies the pinned SHA-256 before use,
and after starting it compares `binary_sha256` from healthz against the same pin.
