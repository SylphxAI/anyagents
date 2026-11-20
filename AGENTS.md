# AGENTS.md — Golden Parallel Architecture for `SylphxAI/anyagents`

Operational engineering contract for human contributors and coding agents working in parallel across `SylphxAI/anyagents`.

## 1. Bounded Contexts & Crate Boundaries

Keep changes inside the crate that owns the domain. Never pull coordinator concerns down into leaf crates or cross-couple independent adapters.

| Crate / Family | Ownership | Boundary Rule |
| --- | --- | --- |
| `crates/anyagents-core` | Identifiers, address grammar, durable mailbox, SQLite/Postgres store, redaction, host memory/retrieval | Pure domain & persistence; zero HTTP/Axum or vendor-harness coupling |
| `crates/anyagents-harness` | Vendor-neutral `Harness` trait contract, masking, seat contract, scripted fake harness | Thin shared port only; no vendor protocol code |
| `crates/anyagents-harness-stream` | Shared long-lived line-stream child engine for stream-based adapters | Reusable stream transport; no vendor-specific product branching |
| `crates/anyagents-harness-*` | Vendor adapters (`agy`, `claude`, `codex`, `muse`, `omp`, `opencode`, `spiron`) | Strict leaf adapters implementing `anyagents-harness`; zero cross-harness imports |
| `crates/anyagents-agent` | Per-agent delivery loop connecting durable mailbox to a `Harness` | Depends on `anyagents-core` + `anyagents-harness` only |
| `crates/anyagents-keeper` | Out-of-process harness custody that outlives the server | Process-custody boundary only |
| `crates/anyagents-supervisor` | Live supervision keeping one harness process running per agent | Depends on `anyagents-agent`, `anyagents-core`, `anyagents-harness` |
| `crates/anyagents-installer` | Harness catalog, lazy installation, and platform sign-in helpers | Platform install/catalog only; no server routing |
| `crates/anyagents-toolchain` | Declared tool binary manifests, digest verification, typed refusal, child `PATH` | Leaf crate; zero dependency on `anyagents-core` or `anyagents` |
| `crates/anyagents-channel-telegram` | Telegram `getUpdates` long-polling and update deduplication | Channel adapter over `anyagents-core` only |
| `crates/anyagents-gateway` | Loopback protocol gateway between harness APIs and model providers | Leaf protocol proxy; zero dependency on coordinator state |
| `crates/anyagents-guest` | Isolated sandbox guest supervisor (`serve`, `turn`, `mcp-proxy`, `health`) | Sandbox-resident runtime; never imports host workstation or coordinator server code |
| `crates/anyagents-mcp-*` | Standalone stdio MCP server crates (`browser`, `web`, `computer`) | Self-contained leaf workspace members; must build and test independently of `anyagents` |
| `crates/anyagents` | Coordinator binary: Axum HTTP server, CLI, multi-call MCP entry point, and sandbox orchestration | Composition root — wires domain crates and feature submodules together; do not dump new domain logic into root files |
| `tests/invariants` | Crash-injection delivery guarantees and harness conformance suite | Cross-crate behavioral verification |

**Dependency direction:** leaf crates (`anyagents-toolchain`, `anyagents-gateway`, `anyagents-harness-*`, `anyagents-mcp-*`, `anyagents-guest`) must never depend on `anyagents`.

## 2. Vertical Slice / Feature-First + Lightweight Hexagonal Ports

- **Feature-first submodules:** implement new endpoints, background loops, providers, and commands as dedicated vertical-slice submodules (for example, `crates/anyagents/src/server/<feature>.rs` or `crates/anyagents-core/src/store/<feature>.rs`).
- **Per-feature Axum sub-routers:** each HTTP feature module owns its request/response DTOs, handlers, and a `pub(super) fn router() -> Router<AppState>` mounted into the root server via `.merge(...)` or `.nest(...)`.
- **Lightweight hexagonal ports:** isolate external providers (sandbox backends, model gateways, channels, storage engines, harnesses) behind small, focused Rust traits so concrete adapters stay swappable, testable in isolation, and free of coordinator internals.
- **Do not grow the Big Three legacy monoliths:**
  - `crates/anyagents/src/create.rs` (11,739 prod lines at baseline)
  - `crates/anyagents/src/server.rs` (10,036 prod lines at baseline)
  - `crates/anyagents-core/src/store.rs` (9,360 prod lines at baseline)
  - **Hard rule:** new routes, commands, store operations, and feature logic MUST go into dedicated submodules — never appended to `server.rs`, `create.rs`, `store.rs`, or any other baselined monolith. When editing a legacy monolith, prefer extracting cohesive helpers into submodules so the file shrinks over time.

## 3. Module Size Cap (`<= 1,500` Lines) & Baseline Ratchet

- **Hard ceiling:** every new shipped production source file under `crates/` (`.rs`, `.ts`, `.tsx`, `.js`, `.jsx`, `.mjs`, `.cjs`, `.py`) must stay `<= 1,500` production lines.
- **Counting rule** (enforced by `tools/module_size_scan.py`):
  - Excludes non-production directories (`tests/`, `benches/`, `examples/`, `fixtures/`, `target/`, `node_modules/`, `__pycache__/`) and dedicated test files (`tests.rs`, `test.rs`, `*_tests.rs`, `*_test.rs`, `test_*.py`, `*_test.py`, `*.test.*`, `*.spec.*`).
  - For `.rs` files, excludes top-level inline `#[cfg(test)]` and `#[cfg(all(..., test, ...))]` `mod <name> { ... }` blocks (matched lexically, ignoring comments and string/raw-string/char literals). Production code after an inline test module is still counted.
- **Ratchet enforcement:**
  - `tools/module_size_baseline.json` records legacy files currently above `1,500` production lines.
  - Legacy baselined files **may only shrink** (`lines <= baseline`); growing a baselined file by even 1 production line fails CI.
  - Any **new** unbaselined file exceeding `1,500` production lines fails CI immediately.
  - Run `python3 tools/module_size_scan.py` to check locally and `python3 tools/module_size_scan.py --update-baseline` after shrinking or retiring a legacy oversized file.

## 4. Time & Calendar Policy

- Use [`jiff`](https://docs.rs/jiff) for all **new** Rust calendar, timezone, RFC 3339 / ISO 8601, civil-date, and wall-clock arithmetic.
- Do **not** write ad-hoc manual date/calendar math (`% 86_400`, hand-rolled leap-year/month tables, manual UTC offset math).
- Do **not** add new `chrono` dependencies to workspace crates; migrate existing `chrono` calls opportunistically when refactoring surrounding code.
- Use `std::time::Instant` / `tokio::time::Instant` for monotonic timeouts and elapsed-time measurement.

## 5. 100% Open-Source Purity & Vendor Neutrality

- **Zero private or deployment-specific leakage:** never commit company-specific IDs, personal emails, private domains, internal filesystem paths, hardcoded seat/user/group IDs, or private binaries. Purity is continuously enforced by `crates/anyagents/tests/open_source_purity.rs`.
- **Clean integrations only:** zero binary scraping, undocumented private API reverse-engineering, or protocol spoofing.
- **Strict harness neutrality:** treat all third-party coding agent harnesses as equal, vendor-neutral peers — uniform styling, alphabetical or user-configured ordering, and zero vendor favoritism in code, docs, or UI.

## 6. CI Crate-Graph Path-Scoping

`.github/workflows/ci.yml` uses `tools/ci_scope.py` to inspect `Cargo.toml` and `crates/*/Cargo.toml` on every `pull_request` and `merge_group`:

- **Always-on fast checks (`changes`):** `tools/module_size_scan.py`, `tools/test_*.py`, `tests/test_release_admission.py`, `tests/test_deploy_build.py`, plus `npm test` when `packages/` or `package.json` changes.
- **Docs / `AGENTS.md` / `Dockerfile.prebuilt` / `sylphx.toml` / `tools/`-only PRs:** skip Rust compilation and macOS runners while passing `changes` and the required `ci-ok` gate.
- **Leaf-crate PRs** (e.g. `crates/anyagents-guest/**`, `crates/anyagents-mcp-*/**`, `crates/anyagents-gateway/**`, leaf harness adapters): run `cargo clippy` / `cargo nextest` / `cargo test --doc` scoped strictly to the touched crates and their Cargo reverse dependents, skipping macOS and privileged Linux host acceptance unless a macOS/host-platform crate is affected.
- **Core / coordinator / manifest / workflow changes** (`Cargo.toml`, `Cargo.lock`, `crates/anyagents-core/**`, `crates/anyagents/**`, `crates/anyagents-harness/**`, `.github/workflows/ci.yml`) and `push` to `main`: run the full Linux + macOS workspace suite.

## 7. Git, Formatting & Verification Rules

- **Commits:** concise, focused Conventional Commits (`feat:`, `fix:`, `refactor:`, `chore:`, `docs:`). Keep PRs orthogonal and self-contained.
- **Formatting:** Rust 2024 edition (`rustfmt.toml`: `edition = "2024"`, `max_width = 100`). Verify changed Rust files with `rustfmt --edition 2024 --check <files>` (or `cargo fmt --all -- --check` in CI).
- **CI-gated verification:** rely on CI (`ci-ok`) for full workspace clippy (`-D warnings`), nextest, doc tests, and privileged host/seat acceptance. Run fast stdlib checks locally before committing:
  - `python3 tools/module_size_scan.py`
  - `python3 -m unittest discover -s tools -p 'test_*.py' -v`
  - `python3 -m unittest discover -s tests -p 'test_release_admission.py' -v`
  - `python3 -m unittest discover -s tests -p 'test_deploy_build.py' -v`
- **Secrets hygiene:** never commit credentials, API keys, or private tokens. Any synthetic high-entropy test vector (such as an RFC PKCE test vector or synthetic JWT redaction fixture) must be explicitly reviewed and documented in `.desk-secret-allowlist.json`.
