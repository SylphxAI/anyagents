# 0004: One credential store with typed, immutable bindings and launch-time materialisation

Status: proposed. Date: 2026-11-12.

Part of [0003](0003-runtime-rebuild.md). Supersedes [0002](0002-runtime-drivers-and-capability-identity.md) decision 1 and the credential part of decision 3.

## Context

ADR 0002 left external credentials to the runtime environment and gave each vendor harness a config directory under `<data>/agents/<id>/config`. Subscription sign-ins went into "profiles": folders at `<data>/profiles/<provider>/<name>/`. A marker file in each agent's config directory (`.anyagents-profile`) pointed at one of them. That convention is not a store, and it produced one class of failure.

- **Bindings are mutable by system actors.** `select_or_bind_profile` rewrites the marker to `default` (or the first profile it finds) when the bound folder is missing. `rebalance_provider_profiles` moves every non-archived agent, paused ones included, and is reached from profile *listing*. `try_rotate_exhausted_profile` rebinds through `configure_agent_runtime` and is reached from the agent *status* projection. A model change across providers forces `<provider>/default`. Reads, listings, pause/wake and quota events can therefore change which account an agent runs as.
- **Many writers, no identity invariant.** About a dozen routines copy credential files in both directions: `seed_harness_credentials` (including a "harvest peers" step that copies other agents' files into the profile), `sync_fresher_bound_agent_credentials`, profile seeding in `sync_and_list_provider_profiles_inner`, `propagate_profile_files_to_bound_agents`, `deduplicate_same_account_profiles`, and the usage probes. Each picks "newer", "first match" or "default". A folder name stops matching the account inside it, and one provider's files appear in another provider's folder.
- **Materialisation is not a pure function of the binding.** Launch keeps whatever credential files the agent directory already holds unless the profile changed. Some adapters then derive their auth document by first match over many candidate paths, and carry endpoint/auth settings over from the previous config file. An agent switched from one harness to another can fail authentication on the same binding where a freshly created agent succeeds.
- **Implicit default and fail-open health.** With no binding, the name `default` wins unless a reading of zero quota is less than a minute old. A missing or expired credential counts as healthy.
- **Tool identity is discovered from HOME.** Seat launches set `HOME` to the agent config directory, so `gh`, `git` and `op` find nothing unless the operator exports per-host variables or shares home sub-paths. The 1Password token is symlinked from a host-home convention. Server-side GitHub watches use ambient `gh` when no token directory is set. When the identity a tool needs is never stated, any fallback in an external helper decides which identity gets used, including a different App.
- **Sign-in is not atomic.** Staging lives inside `profiles/<provider>/` as `.signin-staging-<pid>`. It is removed only on the error return, and several profile iterators don't skip it. Commit writes file by file into a live folder. When the target name is unset, the account is placed in "the first profile without credentials".

The root cause is architectural: AnyAgents already decides which credential an agent uses. It does so implicitly, through folders, markers and heuristics, with no single owner.

## Decision

1. **AnyAgents owns one credential store.** `<data>/credentials/` is mode 0700, owned by the server UID, unreadable by agent seats, and written only by the `credentials` module:
   ```
   <data>/credentials/
     records/<kind>/<provider>/<name>/current -> gen-<n>   # atomic symlink swap; two prior gens kept
     records/<kind>/<provider>/<name>/gen-<n>/record.json  # kind, provider, identity{subject,email?,account_id?},
                                                           #   verified_at, expires_at, refresh_owner, source; no secret
     records/<kind>/<provider>/<name>/gen-<n>/material/    # vendor-native files exactly as declared by the manifest
     .staging/<uuid>/                                      # login/import scratch; never enumerated; swept at start
     audit.jsonl                                           # actor, action, ref, digests before/after, reason, UTC
   ```
   Kinds: `oauth_subscription`, `api_key`, `github_user`, `github_app`, `op_service_account`, `channel_bot`, and `browser_profile`. A `browser_profile` record points at an existing browser profile; its bytes are not moved. This replaces `<data>/profiles/`, `<data>/config/<provider>/`, `<data>/secrets/channels/`, host-home token symlinks and the GitHub token-directory variable.

2. **Bindings are typed rows, immutable to the system.** The Store stream owns `credential_bindings(agent_id, slot, target, set_by, set_at, reason)`. The slot set is closed: `model:<provider>`, `tool:github`, `tool:git_author`, `tool:op`, `channel:<id>`. A target is `record:<kind>/<provider>/<name>` or `pool:<name>`. Defaults per slot and named pools come from caller-owned operator configuration (`<data>/credentials.toml`), never from code:
   ```toml
   [defaults]
   "model:anthropic" = "record:oauth_subscription/anthropic/acct-a"
   "tool:github"     = "record:github_user/github/dev"
   [pools.example]
   members = ["record:oauth_subscription/anthropic/acct-a", "record:oauth_subscription/anthropic/acct-b"]
   policy  = "most-remaining"
   [identities."record:github_user/github/dev"]
   git_author = { name = "Example Dev", email = "dev@example.invalid" }
   ```
   Only one action writes a binding: `agents configure --credential <slot>=<target>`, run by the operator or by an ancestor under the existing authority model, and audited. Pause, wake, restart, maintenance, listing, status reads, quota events and login never write bindings.
   - A harness or model switch that needs a slot with no binding and no default, or whose bound record's provider cannot serve the new pair, is refused unless the same call names the credential.
   - Quota failover exists only through an explicitly bound `pool:`. The member picked at launch is recorded as an observation, and the binding is not changed.
   - Recovering quota on the *same* record (clearing a stale hold once quota is back) stays a lifecycle concern and never rebinds.

3. **Resolution is typed and fails closed.**
   ```rust
   fn need(agent: &Agent, harness: &HarnessId) -> Vec<Slot>;          // manifest + catalog(model) + tool policy
   fn resolve(agent: AgentId, slot: Slot) -> Result<Resolved, CredentialError>;
   enum CredentialError { Unbound, Missing, WrongKind, Expired, Invalid, IdentityMismatch, PoolExhausted, StoreUnreadable }
   ```
   Resolution order: binding, then operator default, then `Unbound`. There are no `default`-named folders, no "first healthy" pick, no alias roots, and no name-based compatibility. Compatibility is a catalog field of the record's provider.

4. **Credentials are materialised at launch, never discovered.** Each harness adapter declares a credential manifest: `files: [(rel_path, slot, transform, mode)]` and `env: [(VAR, slot, field)]`. `materialize(agent, harness) -> {config_dir, env}`:
   1. resolves every needed slot; any error aborts the launch;
   2. deletes every path in the union of all harness manifests from the agent config directory;
   3. writes exactly the resolved material;
   4. writes `.anyagents/credentials.lock.json` (slot, ref, generation, sha256, identity; no secret).

   OAuth records whose refresh the store owns are written access-only (refresh token withheld), and the store refreshes them. For vendors that must refresh themselves, one path, `absorb(agent)`, accepts a refreshed token only when its identity equals the record identity and its expiry is newer, and writes it as a new generation. All other agent→store copying is removed.

   Tools get their identity the same way on every harness:
   - `GH_CONFIG_DIR` points at a server-written hosts file for `tool:github`. For an App record, the installation token is minted by the server and rewritten before expiry.
   - `GIT_CONFIG_GLOBAL` points at a generated file with `credential.helper = anyagents credential git` and `user.name`/`user.email` from `tool:git_author`. That slot defaults to the identity of `tool:github`, so commit author and pusher cannot diverge. The helper answers only with the bound record.
   - `OP_SERVICE_ACCOUNT_TOKEN` comes from `tool:op`.

   Local mode may keep the real `HOME` for non-credential dotfiles, but no credential is ever read from it, from ambient `gh`, from environment API keys, or from another identity.

5. **Login is atomic and promoted into the store.** The only user command is `anyagents login <harness> [--account <name>] [--replace]`; the catalog maps harness to provider. `anyagents login github|op` and `anyagents credentials import <kind> --from <path>` cover tools; there is no HOME scan. `anyagents sign-in <agent>` is a deprecated alias that logs in to the record bound to that agent's model slot. The toolchain interface is `begin_login(harness, account) -> staging_dir`, then the managed launcher runs the harness sign-in argv with its config variable set to the staging directory, then `commit` or `abort`. Commit runs these steps:
   1. accept only the manifest-declared files;
   2. live-probe the identity;
   3. refuse when `--account` names a record holding a different identity (unless `--replace`), or when the identity already exists under another name;
   4. write `gen-<n+1>`, fsync, rename `current`, then remove staging.

   A crash leaves either the old or the new generation. Login never touches bindings. The toolchain never reads credential bytes, and the store never resolves binaries.

6. **One `anyagents doctor` proves every credential live.** The credential part, called by the single doctor command, checks:
   - store permissions and seat denial; no staging older than an hour;
   - every record live-probed through catalog-declared probes (`method`, `url`, `auth_template`, `identity_pointer`), with identity equal to the record and refresh viable;
   - for every agent, `need()` fully resolves and the lock digests equal the store generation, with no undeclared credential files;
   - inside each seat, `gh api user`, `git config user.email` and `op whoami` match the bound identities.

   It exits non-zero on any failure and never prints a secret.

7. **Failures are closed and visible.** A `CredentialError` stops the launch and sets agent health to `credential_blocked{slot, target, reason}`. That health is shown in `agents list/describe`, topology deltas and doctor, and the parent (or operator channel for roots) gets one notice per distinct error. There is no retry loop. A binding change, a new generation or a doctor re-probe re-arms the launch. A tool call inside a running agent fails with the helper's typed reason, never with another identity.

## Invariants

| # | Invariant | Proving test |
|---|---|---|
| I1 | Only explicit configure changes a binding | Property test: random sequences of pause, wake, restart, maintenance tick, profile listing, status read, quota event, login and switch leave `credential_bindings` byte-identical; one configure changes one row and writes one audit line |
| I2 | No implicit default | Delete a bound record: launch refused `Missing`, binding unchanged, health `credential_blocked`. Spawn with no binding and no default: `Unbound` |
| I3 | Materialisation depends only on (binding, generation, manifest) | Switch an agent between two harnesses on one binding, then compare with a fresh agent on the same binding: identical lock digests and credential file sets |
| I4 | No residue | Pre-seed every harness's credential paths with junk; after launch, the manifest-union paths present equal the current manifest exactly |
| I5 | One writer, identity-checked absorb | A vendor writes another account's token: absorb rejects, generation unchanged, audited. Same identity with newer expiry: new generation |
| I6 | Nothing from HOME or ambient state | With `HOME`, `GH_CONFIG_DIR`, `OP_*` and `*_API_KEY` pointing at valid credentials and no binding, `gh`, `git push`, `op` and the model call fail typed. A lint forbids `env::var("HOME")` and host-home lookups in `credentials/` and launch paths |
| I7 | Atomic login | Fault injection (abort or SIGKILL after each step): the store holds the old or the new generation only; `.staging` is never enumerated |
| I8 | Tool identity has no fallback | Fake GitHub: a user binding pushes and authors as that user. Remove the record: push fails `Missing` and no App endpoint is contacted |
| I9 | Doctor is a live proof | A fake provider returning 401, an identity swap, lock drift or stale staging makes doctor exit non-zero naming the record; all-healthy exits zero |
| I10 | Seats cannot read the store | The `agent-run` assertion opens `<data>/credentials` as the seat and expects `EACCES` |
| I11 | Open-source purity | `tests/open_source_purity.rs` also scans manifests, catalog probes and fixtures; identities appear only in `credentials.toml` fixtures under `example.invalid` |

## Migration (zero-loss)

Runs once at server start under the store lock, before the supervisor and maintenance start. The old writers are removed in the same release.

1. **Snapshot.** Archive `<data>/profiles/`, `<data>/config/<provider>/`, `<data>/secrets/`, the browser profile index, any configured GitHub token directory, and the manifest-union paths of every agent config into `<data>/backups/credential-migration-<UTC>.tar` (0600). Write a per-file sha256 manifest and verify the archive against it by reading it back.
2. **Classify.** Every file in every profile folder, *including* left-over staging folders, is classified by manifest shape into (kind, provider), so files sitting under the wrong provider go to their real provider. Unclassifiable files go to `records/unclassified/<origin>/`; nothing is dropped.
3. **Identify.** Live-probe each candidate and group by identity. The freshest valid candidate becomes `current`; the others become retained generations. A candidate that fails the probe is imported as `invalid`. The record name is the old folder name when its contents match that folder's identity; otherwise `<old-name>-<identity-slug>`, with the mismatch reported.
4. **Agent directories.** Credentials found only in an agent directory are imported as records. Same-identity newer tokens are absorbed as generations.
5. **Bindings.** Each agent's marker maps to the record holding that label's identity, because the label is the operator's intent. When there is none, the agent stays unbound and doctor shows `Unbound`; nothing is silently defaulted. A proposed `credentials.toml` with defaults is written for operator review.
6. **Tools.** Token-directory files become `github_user` records. `gh` hosts files and `op` tokens are imported only by an explicit `anyagents credentials import`. Browser profiles become pointer records. Vendor session state is untouched.
7. **Read-back.** Every imported material sha256 equals its source. No record's live-probe result is worse than before migration. Every non-archived agent resolves or is reported `Unbound` with a reason. Doctor passes. Old roots are renamed to `*.migrated-<UTC>`, not deleted; deleting them is a separate explicit operator command.

## Delivery (streams of 0003)

- **Store** (`crates/anyagents-core/**`):
  - PR 1 is additive: the `credential_bindings` schema and typed ops (`bind`, `unbind`, `binding_of`, `record_observation`, audit).
  - PR 2 makes the bindings registry the single writer. No other stream edits anyagents-core.
- **Credentials** (`crates/anyagents/src/credentials/**`, `create.rs`, `main.rs`, `signin.rs`, `channel.rs`), W1:
  - adds `store`, `resolve`, `login` (`begin_login`/`commit`/`abort`), `absorb`, `migrate`, the `anyagents login` / `credentials` CLI and the channel-token move;
  - deletes `select_or_bind_profile`, `list_harness_profiles`, the marker functions, `seed_harness_credentials`, harvest, `sync_fresher_*`, profile seeding in listing, `propagate_*`, `deduplicate_*`, `rebalance_*`, the rotating half of `try_rotate_exhausted_profile`, `resolve_and_commit_staged_signin`, `finalize_*_signin`, the profile copy in `configure_agent_runtime`, `ensure_seat_op_config`, and credential use of shared home paths;
  - provides the same-record quota-recovery function for Lifecycle.
- **Harness boundary** (PR 2 of that stream, W2): per-adapter credential manifests and `materialize`. In `vendor.rs` it adds the residue purge, sets env from `materialize`, and keeps `HOME` redirected only where a vendor requires it. It deletes auth derivation and carried-over endpoint/auth lines in `spiron.rs`, routes `cloud_harness.rs` token lookup through `resolve`, and has `opencode_config.rs`, `muse_config.rs` and `mcp_config.rs` read resolved material only.
- **Install** (W1): `sign_in_at` becomes the managed launcher's sign-in argv run against `begin_login`'s staging directory.
- **Toolchain** (tool identity and doctor, W2): the `anyagents credential git` helper, the gh hosts writer and the op env.
  - `worktree_access.rs` takes the author from `tool:git_author`, and the global author variables are removed.
  - `schedule/github.rs` gets its token from `resolve`, and both the ambient `gh` mode and the token-directory variable are removed.
  - `browser.rs` gets op env from `tool:op`, and `sandbox.rs` gains the store-denial assertion.
  - `doctor.rs` calls the credential probes above.
- **Lifecycle** (`server.rs`, `agent.rs`, W3): removes the maintenance-tick credential sync and the rotate call in the status projection, maps `CredentialError` to `credential_blocked` health and notices, and calls same-record quota recovery.

## Consequences

- An agent's identity per slot can be read in one place and changed in one place. Every change is audited, and nothing in the background changes it.
- Switching harness or model never inherits stale credential files; a switched agent and a fresh agent on the same binding are byte-identical in credentials.
- Credential failures are visible and stop work instead of degrading to another account, ambient login or a different App.
- AnyAgents takes on credential-store scope that ADR 0002 rejected. The scope is bounded: records, typed bindings, materialisation and live probes. Secret storage stays filesystem-based (0600/0700, server UID); keychain or KMS backends can sit behind `store` later.
- Operators must state GitHub, git-author and op identities explicitly once (`login`, `import` or `credentials.toml`). Unconfigured tools fail closed.
- Quota failover changes from automatic rebinding to opt-in pools, and existing rebalance tests are replaced by I1.

## Alternatives rejected

- **Keep profiles and fix each heuristic.** Every fix leaves the many-writer, marker-plus-folder model in place; the failures recur in the next routine.
- **Leave tool credentials to the environment (ADR 0002 decision 1).** Seats need a redirected `HOME`, so the environment can't supply per-agent identity. Per-host exports produce exactly the ambient fallbacks this ADR removes.
- **Automatic rebalancing and rotation that rewrite bindings.** These change identity without an actor. Pools give the same failover while the binding stays fixed and the pick stays visible.
- **Bindings in marker files.** They can't be written atomically together with an audit entry, and readers can't tell an absent marker from an unreadable one. SQLite rows can do both.
- **Environment variables for every credential.** Vendor subscription tools read files from their config directory. Short-lived App tokens need refresh. A credential helper plus server-written files covers both.
- **Copying the whole profile folder into the agent directory.** It copies undeclared artefacts and cross-provider files. Declared manifests are the only safe boundary.
