# AnyAgents competitor comparison — 2026-10-25

Research date: **2026-10-25 UTC**. Decision: keep **AnyAgents**, qualify it as
“the long-lived coding-agent switchboard”, and adopt the three backlog items
below. The naming rationale is in [ADR 0003](adr/0003-keep-anyagents-name.md).

## Method and limits

This is a primary-source documentation comparison, not a hands-on benchmark,
security audit, market-share survey, or exhaustive feature inventory. Sources
were read on the research date and pinned to repository commits; the dates in
the register are **repository snapshot commit dates**, not claims that each
README was edited on that date. “Documents” and “advertises” mean the project's
own source says so, not that we independently verified the behavior. A feature
not mentioned here is **not** evidence that a competitor lacks it. Do not infer
performance, reliability, cost, or security superiority from this comparison.

The AnyAgents baseline is commit
[`90af581`](https://github.com/SylphxAI/anyagents/tree/90af581a0e371153e965d5fd0fc73870cdb35653),
committed 2026-10-23. Its [README][aa-readme] describes the implemented surface;
its [vision][aa-vision] includes future scope. Keep those separate. This review
adopts backlog work, not implemented features or new launch requirements.

## Competitors read and dated sources

Every competitor claim below refers to this register. All sources were
accessed **2026-10-25 UTC**. Commit links freeze the text used in this review;
package metadata is version-specific but remains registry-controlled.

| ID | Project / source read | Snapshot commit date (UTC) | Immutable repository source |
| --- | --- | --- | --- |
| OH | OpenHands / Agent Canvas README | 2026-10-24 23:38:11 | [README at `5c4fc6c`][oh] |
| OC | OpenClaw README | 2026-10-25 00:02:29 | [README at `5895dc9`][oc] |
| NC | NanoClaw README (`qwibitai/nanoclaw` URL) | 2026-10-23 14:08:13 | [README at `b1a9b65`][nc] |
| RF | Ruflo / Claude Flow README (`ruvnet/claude-flow` URL) | 2026-10-24 23:46:31 | [README at `973d0ab`][rf] |
| MA | Mozilla AI any-agent README | 2026-09-21 16:54:58 | [README at `a469723`][ma] |
| NP | Unscoped npm `anyagents` 1.0.0 metadata | Version published 2026-10-23 17:48:38 UTC; registry read 2026-10-25 | [Version metadata][np]; [publication timestamps][np-times] |

The first four cover overlapping runtime, channel and coordination needs.
Mozilla AI any-agent is an adjacent framework abstraction and a naming neighbor,
not presumed to be a drop-in runtime replacement. The npm package is included
only for naming/distribution collision analysis, not as a runtime competitor.

## Comparison and implications

| Project (dated source above) | What the source actually documents | Overlap with AnyAgents / our assessment | Decision |
| --- | --- | --- | --- |
| OpenHands / Agent Canvas [OH][oh] | A self-hosted, always-on developer control center; local, Docker, VM and cloud backends; third-party coding agents including ACP-compatible agents; automations scheduled or triggered by webhooks; a warning that unsandboxed execution has host filesystem access. | This is direct overlap, including multi-harness operation. “Any harness” alone is not a sufficient positioning claim. Our emphasis should be the switchboard contract, vendor-owned sessions, and explicit delivery semantics, not a claim to be the only multi-harness host. | Adopt a bounded ACP compatibility evaluation, [#28](https://github.com/SylphxAI/anyagents/issues/28). Defer a graphical control center, workflow engine and multi-backend management. |
| OpenClaw [OC][oc] | A local Gateway, many chat channels, interchangeable model/harness plugins, onboarding followed by status/dashboard and a first message; inbound messages treated as untrusted, default pairing for unknown DM senders, and a host-execution warning. | Direct overlap in always-on assistants, channels and swappable harnesses. A copyable first-message path and explicit trust warnings are worth borrowing. Channel count is not our first-version objective. | Adopt onboarding validation [#26](https://github.com/SylphxAI/anyagents/issues/26) and trust-boundary documentation/tests [#27](https://github.com/SylphxAI/anyagents/issues/27). Defer additional channels and device apps. |
| NanoClaw [NC][nc] | Container-isolated agents and explicit mounts; per-agent workspaces; a scripted setup path; channels and alternative providers installed through skills; scheduled tasks; an external credential gateway. | Overlap in persistent agents, vendor harnesses and messaging. Explicit isolation and setup checkpoints are useful patterns. Its customization and credential-gateway choices need not become our architecture. | Apply setup and boundary lessons in [#26](https://github.com/SylphxAI/anyagents/issues/26) and [#27](https://github.com/SylphxAI/anyagents/issues/27). Reject an internal credential broker, consistent with [ADR 0002](adr/0002-runtime-drivers-and-capability-identity.md). Do not repeat its claims about other projects' security as established facts. |
| Ruflo / Claude Flow [RF][rf] | A meta-harness for Claude Code and Codex; plugin and MCP installation paths; an optional terminal console showing cost, swarm, events and missions; advertised agent coordination and persistent memory. | Overlap in coordination, MCP and cost visibility. A readable operator surface is relevant, but a coordination layer does not establish the same mailbox or restart guarantees. We did not validate its advertised scale, learning or security claims. | Keep the current server/CLI/MCP boundary. Defer swarm orchestration, learning memory and a new console; no adopted implementation item from this source. |
| Mozilla AI any-agent [MA][ma] | One interface to run and evaluate multiple agent frameworks, with tracing, evaluation and serving documentation. Its README explicitly says soft deprecation and recommends tinyagent for new core-loop projects, while retaining cross-framework evaluation use. | Adjacent, not the same vendor-process supervision product. The similar name creates a plausible discovery risk (our judgment, not measured user confusion). Evaluation as a discipline is useful, but not a reason to reimplement an agent loop. | Clarify runtime versus framework positioning; retain the name with qualifications in [ADR 0003](adr/0003-keep-anyagents-name.md). No new framework abstraction. |

## AnyAgents baseline and bounded differentiation

At the baseline, the [README][aa-readme] reports a durable ordered/idempotent
mailbox, live FIFO steering, supervisor wake/restart behavior, Telegram, native
harness adapters, usage accounting, and an API-key protocol gateway. The
[vision][aa-vision] separates harness, model and authentication and leaves
additional channels, a GUI, and multi-host operation outside the first version.
[ADR 0002][aa-boundary] says local agents share the host user's environment;
container/Pod isolation and secrets use standard runtime primitives. It does
**not** make local same-UID agents a host filesystem security boundary.

Our recommended position is **a small, server-first switchboard for long-lived
vendor coding harnesses, with explicit, tested mailbox and steering semantics**.
That is a focus choice based on our baseline, not proof that no competitor has
those semantics. We have not run comparative crash tests or established a
unique moat. Preserve native subscription sign-in rather than exporting vendor
credentials or making undocumented compatibility promises.

The existing CLI already has `status --json`, sign-in, agent creation and harness
inspection ([main.rs][aa-cli]); lazy installation already exists
([install.rs][aa-install]). Therefore the adopted onboarding item extends and
validates an existing path rather than inventing a second diagnostics service.
Existing identity/webhook tests should be reused for the boundary review; it is
not a declaration that a vulnerability has been found or fixed.

## Adopted improvements — all filed

“Adopted” here means accepted into the backlog for delivery/evaluation, **not
implemented**, and does not block the current first-version scope.

| Filed improvement | Inputs | Concrete acceptance boundary | Priority / disposition |
| --- | --- | --- | --- |
| [#26 — Fresh-install path to a first Telegram reply](https://github.com/SylphxAI/anyagents/issues/26) | OC and NC, dated above | README-linked instructions validated on Linux/macOS; fixture-based smoke path; retry without duplicate agents or lost history; no secrets in troubleshooting output. | First: reduce time to first value using existing CLI commands. |
| [#27 — Runtime and Telegram trust boundaries](https://github.com/SylphxAI/anyagents/issues/27) | OC and NC, dated above | Local/container/seat trust matrix; explicit sender/chat/topic policy; tests distinguish transport authentication from sender authorization; no new broker or false sandbox claim. Any discovered policy gap is filed separately. | First: operator clarity and regression coverage, not an unreviewed security redesign. |
| [#28 — ACP compatibility evaluation](https://github.com/SylphxAI/anyagents/issues/28) | OH, dated above | One versioned harness trace and an ADR choosing adopt/defer/reject; evaluate resume, steering, usage and crash/ack semantics without claiming unsupported guarantees. | Later, timeboxed investigation only; production adapter needs a separate decision. |

Each issue contains both positive and negative acceptance cases. No other
improvement is adopted by this document. The explicit deferrals above avoid
turning a competitor feature list into an unlimited roadmap. Revisit on
completion of these items, before a positioning launch, or when a dated source
materially changes the decision; refresh sources rather than treating this
snapshot as current forever.

[aa-readme]: https://github.com/SylphxAI/anyagents/blob/90af581a0e371153e965d5fd0fc73870cdb35653/README.md
[aa-vision]: https://github.com/SylphxAI/anyagents/blob/90af581a0e371153e965d5fd0fc73870cdb35653/docs/vision.md
[aa-boundary]: https://github.com/SylphxAI/anyagents/blob/90af581a0e371153e965d5fd0fc73870cdb35653/docs/adr/0002-runtime-drivers-and-capability-identity.md
[aa-cli]: https://github.com/SylphxAI/anyagents/blob/90af581a0e371153e965d5fd0fc73870cdb35653/crates/anyagents/src/main.rs
[aa-install]: https://github.com/SylphxAI/anyagents/blob/90af581a0e371153e965d5fd0fc73870cdb35653/crates/anyagents/src/install.rs
[oh]: https://github.com/OpenHands/OpenHands/blob/5c4fc6c19a70dcec630d75f96a26d6af92860399/README.md
[oc]: https://github.com/openclaw/openclaw/blob/5895dc953bae861eb1f410b5f99075c1daeaecc3/README.md
[nc]: https://github.com/qwibitai/nanoclaw/blob/b1a9b65806d2c4d3c6981bd96448068fb6c62606/README.md
[rf]: https://github.com/ruvnet/claude-flow/blob/973d0ab497c9138d7bee1dc3c7a4d3985e37bf9e/README.md
[ma]: https://github.com/mozilla-ai/any-agent/blob/a469723ac4a0518ab0696d57e09e4dc407c826ec/README.md
[np]: https://registry.npmjs.org/anyagents/1.0.0
[np-times]: https://registry.npmjs.org/anyagents
