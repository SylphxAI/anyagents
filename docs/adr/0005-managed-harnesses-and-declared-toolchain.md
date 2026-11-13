# 0005: Managed harnesses and a declared toolchain

Status: proposed. Date: 2026-11-12. Part of [0003](0003-runtime-rebuild.md). Builds on [0001](0001-server-first-one-binary.md) decision 1 ("drive the vendor's own binary") and the lazy install described in [architecture](../architecture.md#harnesses-install-update-sign-in).

## Context

AnyAgents runs external programs of two kinds:
- **Vendor harness CLIs**: Claude Code, Codex, OpenCode, Antigravity, Muse, oh-my-pi, Spiron and others.
- **Ordinary tools**: `git`, `gh`, a secrets CLI, `adb`, Chromium, `docker`, `setpriv`, image and PDF converters, a JavaScript runtime.

Today neither kind is identified by data. Each call site infers the binary at the moment of use:
- **Harnesses.**
  - `anyagents-installer::builtin_catalog` marks most entries `Vendored`, meaning "whatever the machine has".
  - `install::program` resolves in this order: an `ANYAGENTS_<ID>_BIN` env var, then the highest `.installed` directory under a home-made version sort, then a `PATH` scan, then the bare name.
  - `ensure_vendored_binary` installs some harnesses with unverified global package-manager installs or `curl | sh`, and scans vendor directories under the user's home.
  - `ensure_seat_accessible` copies a `PATH` binary's whole directory when an OS seat cannot traverse it.
  - Only `npm` entries are versioned, and they track "latest" without a digest.
  - The 6-hourly updater installs the new version and deletes every other version directory immediately, even while sessions run from it. There is no smoke check and no rollback.
  - Vendors that self-update replace binaries in place.
- **Tools.**
  - Most are executed by bare name: `adb` in `computer.rs`, `gh` and `timeout` in `schedule/github.rs`, `git` throughout `worktree*.rs`, `convert` and `pdftotext` in `mcp.rs`, `curl` in learned memory.
  - Chromium is found by probing four hardcoded paths.
  - The JavaScript runtime is found by walking home-relative directories and `PATH`.
  - Checkout provisioning executes an opaque program named by `ANYAGENTS_CHECKOUT_PROVIDER_BIN`, with no contract on what it may mutate.
- **Children.** Harness children inherit whatever `PATH` the server was started with. A worker that rebuilds `PATH` from a seat-redirected `HOME` loses entries entirely.
- **Tests.** One test read the compile-time `CARGO_BIN_EXE_*` variable at runtime and fell back to walking the `target/` layout.

Observed failure classes:
- Harness versions differ across seats of one deployment.
- A guard wrapper binds to a guessed default path for the real tool.
- A capability's tool is absent and fails mid-turn instead of at start.
- Build tools and a build CLI present on the host are missing from a worker's `PATH`.
- A checkout provider writes per-checkout settings into a configuration shared by every checkout.

The common cause is that binary identity is never data and is never verified.

## Decision

1. **Two classes, one rule each.**
   - Harness CLIs and their language runtimes (`node`, `bun`) are **managed**. AnyAgents discovers, downloads, verifies, installs, upgrades, rolls back and leases them. Users never install or update a harness.
   - Every other binary is **declared**. The deployment lists it with an absolute path, an exact version and a sha256, the deployment image provides it, and AnyAgents verifies it.
   - Nothing resolves a binary from ambient `PATH`, `HOME`, env-var overrides or guessed paths.
2. **Harness registry.** `CatalogEntry` gains:
   - `source`: `npm` (tarball verified against registry `dist.integrity`), `github-release` (asset verified against the published checksum file or a minisign/cosign signature), or `vendor-manifest` (a versioned manifest carrying per-platform checksums).
   - `launcher`, a path relative to the install directory.
   - `runtime`: `none`, `node` or `bun`. Runtimes are registry entries of the same shape.
   - `doctor`, the exact-version argv plus an adapter-declared offline handshake.
   - `sign_in` argv and `config_var`.
   - `self_update_off`, the env or flags that disable vendor self-update.

   `VendorScript`, `Vendored`, global installs and `curl | sh` are removed. Latest-tracking is allowed only for sources with a publisher checksum or signature. Any other source must carry `pinned = { version, sha256 }`, or it is refused as `UnverifiableSource`. The catalog ships in the binary, and caller configuration can override any entry by id.
3. **Layout and records.**
   - `<data>/harnesses/<id>/<version>/` is immutable once installed, mode `0755`, and not writable by agent UIDs. Seat UIDs execute it directly, so `seat-bin` staging is deleted.
   - Each version has a `receipt.json` = `{version, source, digest, launcher, installed_at, doctor: {ok, at}, state: ok|bad}`.
   - `<data>/harnesses/<id>/current.json` = `{version, previous}`, swapped by atomic rename, is the only selector.
   - A process holds a lease row `harness_leases(agent, harness, version, acquired_at)` from spawn to exit. The Store owns the schema and typed ops.
4. **Upgrade state machine** (`anyagents-installer::Lifecycle`):
   1. discover the latest version;
   2. stage into `.staging-<ulid>`, verify the digest or signature, rename into place;
   3. doctor: exact version, then the offline handshake in a scratch config dir;
   4. promote: `current = {new, previous = old}`;
   5. confirm: the first real turn on the new version succeeds;
   6. otherwise **roll back** to `previous` and mark the receipt `bad`. A `bad` version is never re-promoted.
   7. GC removes a version only when it is neither current nor previous and holds no lease.

   A running process keeps its absolute launcher path and moves to `current` only when the adapter restarts it between turns. An upgrade never interrupts a turn. The server's existing periodic loop and `anyagents harnesses update` both drive this one state machine.
5. **Sign-in** (the cross-class decision in 0003, with storage owned by [0004](0004-identity-and-credentials.md)). The user command is `anyagents login <harness> [--account <name>] [--replace]`. The interface is:
   - `begin_login(harness, account) -> staging_dir`;
   - AnyAgents runs the `current` launcher's `sign_in` argv with `config_var = staging_dir`;
   - `commit` or `abort` (atomic);
   - at launch, `materialize(agent, harness) -> {config_dir, env}`.

   The toolchain never reads credential bytes, and the credential store never resolves binaries.
6. **Declared toolchain.**
   - The manifest is a caller-owned `<data>/toolchain.toml` (overridable with `--toolchain`); the engine ships no host paths:
     ```toml
     schema = 1
     child_path_tail = ["/usr/local/bin", "/usr/bin", "/bin"]
     [tool.git] path = "/usr/bin/git"; version = "2.47.1"; sha256 = "…"; version_args = ["--version"]
     [tool.adb] path = "/opt/example/platform-tools/adb"; version = "35.0.2"; sha256 = "…"; capability = "computer.mobile"
     [tool.cargo] path = "/opt/example/rust/bin/cargo"; version = "1.91.0"; sha256 = "…"; expose = true
     ```
   - The only way to build a `Command` is `anyagents-toolchain::Toolchain::tool(name) -> Result<&ResolvedTool, ToolchainError>`, then `ResolvedTool::command()`.
   - At startup, and again when a file's `(dev, ino, mtime, size)` changes, AnyAgents checks for each tool: the path is absolute and canonicalized, the sha256 of the target matches, and the `version_args` output contains the exact version.
   - Results form a `ToolchainReport`, served by `anyagents doctor`, the API and `harnesses list`.
7. **Constructed child `PATH`.** AnyAgents maintains `<data>/toolchain/bin/<name>` symlinks to the declared tools marked `expose = true` and to the managed launchers. Every child gets exactly `PATH = <data>/toolchain/bin:<child_path_tail>`, so a worker that rebuilds `PATH` still finds declared tools in the tail.
8. **Typed refusal.**
   - An agent's required set is its harness runtime requirements ∪ the tools of its enabled capabilities (`computer.mobile → adb`, `browser → chromium`, `checkout → git`, `github-watch → gh`) ∪ its explicit `tools = [...]`.
   - `create`, `wake` and restart check that set before spawning. On failure the agent enters `REFUSED` with one of `ToolchainError::{Undeclared, Missing, ChecksumMismatch, VersionMismatch, NotExecutable, ManifestUnreadable, UnverifiableSource, HarnessInstallFailed, NoHealthyVersion}`. The error names the binary, its path and the capability that needed it.
   - An unreadable manifest fails closed. Agents whose required set is satisfied still start.
9. **Fewer binaries.**
   - Uses that need no external program are deleted: `curl` → in-process HTTP, `timeout` → async timeout, `kill` → `libc::kill`.
   - Checkout provisioning uses the native git pool with the declared `git`.
   - Per-checkout settings are written with `extensions.worktreeConfig` and `git config --worktree`, never `--local` on a shared common directory.
   - `ANYAGENTS_CHECKOUT_PROVIDER_BIN` is retired.
10. **Test helpers.** Helper binaries are referenced only through compile-time `env!("CARGO_BIN_EXE_<name>")`, and they live in the package whose tests use them, sharing source by path. Server tests write a temporary `toolchain.toml` and serve a fixture harness registry over loopback HTTP, with tarballs that wrap the `fake_*` binaries and carry real digests. No test sets `ANYAGENTS_*_BIN`.
11. **Deployment image contract.**
    - The image provides every declared tool at its declared path. Every agent UID can execute it, with no `0700` ancestors and nothing under a user home.
    - Image CI regenerates `toolchain.toml` with `anyagents toolchain snapshot` and commits it with the image digest.
    - A guard wrapper declared in place of a tool is pinned by its own sha256. It must exec its real binary from an absolute path fixed at image build, without defaults or `command -v`.
    - The image installs, links and seeds no harness CLI, runtime or harness config directory.
    - The `PATH` given to `anyagents serve` carries no meaning.

## Invariants

| # | Invariant | Test |
|---|---|---|
| 1 | No ambient binary lookup | `clippy.toml` `disallowed-methods` allows `std/tokio::process::Command::new` and `std::env::split_paths` only in `anyagents-toolchain`. `tests/toolchain_purity.rs` fails on `var_os("PATH")`, `which(`, `command -v` and `ANYAGENTS_*_BIN` anywhere in the workspace. |
| 2 | Declared tools are absolute and verified | Unit tests: a relative path gives `ManifestUnreadable`, a wrong digest gives `ChecksumMismatch`, a wrong version gives `VersionMismatch`, and a binary swapped after start is caught on the next spawn. |
| 3 | Missing tool → typed refusal, scoped | An agent with `computer.mobile` and no `adb` declared ends `REFUSED{Undeclared adb}` with no child pid. A sibling without that capability starts. |
| 4 | Child `PATH` is constructed | With the server started under `PATH=/nonexistent`, the child env `PATH` equals `<data>/toolchain/bin:<tail>`, and an exposed fixture tool runs from a seat-UID child. |
| 5 | Fail closed | With a corrupt manifest, no tool-requiring agent starts and the error names the manifest. |
| 6 | Harnesses run only from managed installs | With an empty `PATH` and no host CLIs, every harness resolves under `<data>/harnesses/<id>/<version>/`. |
| 7 | Verified download | A tampered tarball fails verification and leaves no version directory. |
| 8 | Atomic upgrade, lease-safe | A bump during a turn: the turn finishes on vN and the next turn runs vN+1. GC never removes a leased directory. |
| 9 | Rollback | A doctor failure leaves `current` unchanged and the receipt `bad`. A first-turn failure flips back to `previous`. A `bad` version is never re-promoted. |
| 10 | Clean-machine zero-step | In a container with no harness CLIs and no node or bun, `anyagents login <each>` then spawn succeeds for every catalog harness. CI runs this against the fixture registry, and a self-hosted nightly runs it against real registries at US$0. |
| 11 | Auto-update | Publishing vN+1 to the fixture registry and ticking the loop makes `current` vN+1 with no operator action. |
| 12 | Toolchain holds no credentials | The purity test asserts that `anyagents-toolchain` depends on no credential or secret module. |

## Migration (zero-loss)

1. **Observe.**
   - Ship `anyagents-toolchain` in report mode: verification logs a `ToolchainReport` and refuses nothing.
   - `anyagents toolchain snapshot` writes a candidate manifest from what currently resolves, for the deployment to review and commit.
   - Env overrides still work, and each use logs `declared-by-env (deprecated)`.
2. **Adopt harnesses.**
   - Existing `<data>/harnesses/<id>/<version>/` directories are hashed and given receipts. `current` is set to the newest one with a launcher.
   - A harness without a verifiable source yet resolves through a temporary manifest entry (absolute path + sha), never `PATH`.
   - Running processes are untouched, and leases begin at their next spawn. Vendor self-update is disabled per entry.
   - Credentials are not touched; 0004 migrates them.
3. **Enforce per tool.** An `enforce = true` manifest flag flips each tool once every host reports it green. Refusal affects new starts and restarts only; a running agent is never killed.
4. **Remove.**
   - Delete the env overrides, `which`, `machine_has`, `ensure_vendored_binary`, `ensure_seat_accessible`, `version_sort_key`, the JS-runtime and Chromium probes, and the checkout-provider path.
   - Checkouts already made by an external provider keep working, because the git pool reads the same bare repositories.

## Delivery

Streams follow 0003. Each file set has one writer; other streams make only mechanical call-site swaps, which the owner reviews.

- **W1 Store:** `harness_leases` schema and typed ops, in Store's additive PR. The learned-memory `curl` → in-process HTTP change lands inside the Store stream.
- **W1 Toolchain core:**
  - New `crates/anyagents-toolchain/`: manifest, verification, `ResolvedTool`, `ToolchainError`, child-`PATH` builder, report, `toolchain snapshot`.
  - `clippy.toml` and `tests/toolchain_purity.rs`, with per-file allowances that shrink as streams land.
  - Callers in `browser.rs`, `computer*.rs`, `mcp.rs` (tools; the JS runtime comes from Install), `worktree.rs`, `worktree_access.rs` (git pool, `--worktree` config, provider removal), `sandbox.rs`, `schedule/github.rs` and `crates/anyagents-tools/**`.
  - The opencode test's helper lookup changes to `env!` as a mechanical swap reviewed by Harness boundary.
- **W1 Install:**
  - `crates/anyagents-installer/**`: registry sources, verification, receipts, `current.json`, `Lifecycle`, doctor, managed runtimes.
  - `install.rs` rewrite.
  - The fixture registry and tests 6–11.
- **W2 Harness boundary, third PR:**
  - Managed launcher from `current` plus lease acquire and release.
  - Child `PATH` from the toolchain.
  - `self_update_off`.
  - Removal of `ANYAGENTS_*_BIN` from `vendor.rs`, `spiron.rs`, `muse_config.rs`, `opencode_config.rs` and `crates/anyagents-harness*/**`.
  - `setpriv` and `kill` through the toolchain in `crates/anyagents-supervisor/**`.
- **W2 Toolchain identity + doctor:** `doctor.rs`, one `anyagents doctor` combining the credential probes, `ToolchainReport` and install receipts.
- **Credentials (W1/W2):** the `login <harness>` command in `main.rs`, calling Install's launcher API. `harnesses list` reads receipts, a mechanical swap. Agy client discovery in `create.rs` moves onto the managed launcher path.
- **W3 Lifecycle:** the `server.rs` updater loop calls `Lifecycle`. The remaining `server.rs`/`schedule.rs` `Command::new` sites are swapped to the toolchain. The purity test's last allowances are deleted.

## Consequences

- One harness version per deployment, verified, auto-updated and reversible. Seats can no longer drift, because nothing reads a per-seat `PATH` or home.
- A missing or tampered tool is visible at startup and in `doctor`, and refusal is scoped to the agents that need it.
- The deployment image's job shrinks to "provide these exact files". It no longer installs, links or updates harnesses.
- Disk holds at most `current` + `previous` + leased versions per harness.
- A harness whose vendor publishes neither checksums nor signatures cannot track latest; it is pinned until the vendor publishes one.
- First use of a harness on a clean machine downloads it, so the agent waits in `INSTALLING`, as today.

## Alternatives rejected

- **Pin harnesses in the image.** Users would manage harness versions through image rebuilds, every seat would drift between rebuilds, and sign-in would stay split. AnyAgents is the harness supervisor (0001) and owns their lifecycle.
- **Keep `PATH` with a curated order.** It is still inference. It breaks whenever a child rebuilds `PATH`, a seat UID cannot traverse a directory, or a wrapper is linked over a tool.
- **Env-var overrides per binary.** That is the current model: unverified, invisible in `doctor`, and divergent per launch script.
- **Delete superseded versions on upgrade.** That kills in-flight sessions and leaves nothing to roll back to.
- **Trust on first use for unsigned sources.** It gives a digest without provenance. A pinned version and sha256 in caller configuration is the honest equivalent.
- **Manage every tool, including `git` and Chromium.** These depend on system libraries, and image-level guards may wrap them. The image is the right provider, and AnyAgents verifies it.
