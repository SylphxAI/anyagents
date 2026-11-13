# 0008: A quality loop: outcomes, evaluation, skills, memory and routing from evidence

Status: proposed. Date: 2026-11-13. The first slice (section 8) ships with this record. Turn outcomes depend on [0006](0006-runtime-state-and-lifecycle.md) W2; memory retrieval follows the hybrid-retrieval change to [host memory](../host-memory.md).

## Context

The owner wants AnyAgents to own skills and memory so that every agent delivers the highest quality: an agent organisation that learns. Outcomes are measured, successful practice becomes skills, failures become memory, and the choice of harness and model follows the data. AnyAgents measures cost precisely and records nothing about results.

What exists, checked against the code and the live database on 2026-11-13:

1. **Cost.** Every vendor turn writes one `usage_records` row: agent, harness, model, four token counts, the vendor-reported cost, billing mode and quota state. `GET /v1/usage` sums them by agent, day, model or harness. Two gaps:
   - `estimated_cost_micros` is always empty. The supervisor writes `None` (`anyagents-supervisor/src/lib.rs`), and there is no price table.
   - Only some harnesses report a cost. From 2026-11-12 00:00 to 2026-11-13 16:50 UTC, all 28,090 Claude Code records and all 165 Muse records carried one. None of the 40,871 Spiron records or the 3,520 AGY records did. Today, cost can be compared within Claude Code, not across the fleet.
2. **Results.** No table says whether work succeeded.
   - `turns` (ADR 0006) has an `outcome` column, but only the one-time runtime backfill writes the table. `begin_turn` and `end_turn` have no production caller. The live table holds one row for each of 7,363 agents, each `completed` or `interrupted`. The live turn writer is still `turn_recovery`, which records sequence numbers, not results.
   - `lifecycle_ops` records who archived an agent, and why, as free text.
   - The turn-ownership guard (`schedule.rs`, `guard_family`) told a parent to archive a child that had reported "if its deliverable is accepted". Archive also ends abandoned work, so acceptance was implied and never recorded.
   - GitHub PR watches (`schedule/github.rs`, [github-watches.md](../github-watches.md)) observe these states:
     - `merged` and `closed`;
     - `review_failed`: the check named by `ANYAGENTS_GITHUB_REVIEW_CHECK` (default `review`) failed;
     - `checks_in_progress` and `checks_completed`.

     `checks_completed` does not distinguish pass from fail: `Checks` keeps only whether a check is pending and whether review failed.

     A watch belongs to the agent that set it, and `Plan::read` cancels it when that agent leaves the live states. Since 2026-10-29, agents have set 9,925 PR watches over about 4,600 distinct watch specs; 1,370 of those specs were ever observed merged or closed. 391 watches were cancelled after observing `merged`, so the merge was seen but never reported to its owner.
3. **Skills.** `host_skills::mount` builds each agent's `skills-view` from the global `<data>/skills` and the agent's own overlay `<agent>/skills`. It mirrors that view into every harness's native skill directory (`.claude/skills`, `.codex/skills`, `.agents/skills` and the others), and the harnesses load skills natively.

   The MCP `skills` tool offers only `list`, `read` and `search`, and only in the `full` and `remote` profiles. Spiron's `coordination` profile omits it, because Spiron loads skills itself. A skill has no version, author or provenance, and no read is recorded. A file an agent writes into its overlay becomes one of its skills, unreviewed.
4. **Memory.** Learned memory is `memory/<topic>.md`. Each session pins a `MEMORY.md` section of at most 150 lines and 4 KiB (`learned_memory.rs`). It is searchable with history through `conversation` ([host-memory.md](../host-memory.md)).

   The maintenance loop also runs LLM "sleep" consolidation (`consolidate_idle_agent_memory`, at most one model call per pass), which rewrites topic files from history. Nothing links a memory, or a retrieval, to the result of the work it informed.
5. **Routing.** Harness, model and profile come from the role template's frontmatter, the parent's choice or the subagent defaults. Nothing records how each choice performed.

So skills, memory and the choice of harness and model cannot improve from evidence. The first missing piece is a trustworthy record of outcomes; everything else in this decision reads it.

## Decision

### 1. Outcomes are facts from sources the measured agent does not control

An outcome is a fact about work, recorded for the agent that owns it. Its source is never the measured agent itself.

| Kind | Meaning | Source of truth | Captured by | When |
|---|---|---|---|---|
| `pr_merged` | a pull request merged | GitHub REST `pulls/{n}` | PR watch `merged` transition | this change |
| `pr_closed` | closed without merging | GitHub REST `pulls/{n}` | PR watch `closed` transition | this change |
| `review_failed` | the configured review check failed on one head | Check Runs and commit statuses | PR watch `review_failed` transition | this change |
| `accepted`, `rework`, `rejected`, `abandoned` | a verdict on a deliverable | the agent's ancestor, a root admin or the operator | `agents` `verdict`, `POST /v1/agents/{name}/verdict`, `anyagents verdict` | this change |
| `ci_passed`, `ci_failed` | CI conclusion on one head | Check Runs and commit-status conclusions | the PR and check watches, once `Checks` keeps conclusions | next |
| `pr_reverted` | a merged change was reverted | the base branch: a later merged change that reverts the merge commit | an observation of merged subjects | next |
| turn `completed`, `failed`, `cancelled`, `recovered` | the harness's terminal turn event | `turns`, once ADR 0006 W2 writes it live | read in place, never copied | after 0006 W2 |
| time to done | from the agent's previous terminal outcome (or its creation) to a success | derived | the report | this change |

Recording rules:

- **Append-only and idempotent.** `outcomes` rows are never updated. A row is unique per `(agent, dedupe_key)`, so observing one fact again adds nothing.
- **A GitHub fact names its pull request, not its watcher.** The key is `github:pr:<owner>/<repo>#<n>:<kind>` (lowercased), plus `@<head>` for a review failure. Every watcher of one pull request records the same key.
- **Facts are recorded per observer; attribution is decided when a report is read.** The policy below can change without rewriting history.
- **A pull request fact is stored before anything acts on it.** It is written before the wake that reports it is claimed, and on first sight even while an earlier wake is still unread. If the write fails, the transition stays unconsumed and the next pass writes it first.
- **A verdict is an event.** Rework followed by acceptance is two rows. Only a strict ancestor, a root admin or the operator may record one. An agent never records an outcome of its own work.
- **The row records what the agent ran with.** Harness, model and role are copied from the agent row in the recording transaction. Profile comes from the agent's `.anyagents-profile` marker until ADR 0004's credential bindings replace it.
- **An outcome lives as long as the agent's usage.** Deleting an agent deletes both.

Attribution:

- **Agent:** the row's agent.
- **Role, harness, model, profile:** the row's copy. Usage rows carry their own harness and model per turn, so the report counts agents whose attributed usage ran on more than one pair.
- **Skills read:** `skill_reads`, with the SHA-256 of the bytes read. Native skill loads are not visible yet (section 3).
- **Memory retrieved:** not recorded yet. Next, each `conversation` or `memory` retrieval records the result ids and raw SHA-256 coordinates it returned (never the text), so a retrieval can be joined to the outcome of the work it served. This lands after the hybrid-retrieval change, so it records the final ranking.
- **Topology:** a fact belongs to one agent. A GitHub fact seen by several watchers counts once, for its *claimant*: the agent whose watch was created first, which in practice is the agent that opened the pull request and watched it before reporting. A lead that watches its child's pull request gets no credit for it, and the lead's own spend stays unattributed until the lead has outcomes of its own. Charging a task child's cost to its parent's outcome is a read-time policy left for later.

**Next, so a watched fact is not lost:** a pull request must stay observed after its watcher leaves. When an agent archives, its watched pull requests move to a host-owned subject list that the GitHub prober observes until they reach a terminal state, still attributed to that agent. This changes when watches end (`Plan::read`), so it is coordinated with ADR 0006 W3.

### 2. Evaluation

`Store::outcome_report` applies these rules on read:

1. A shared GitHub fact counts once, for its claimant. Who may read the report changes what it shows, never whom a fact counts for.
2. An agent with a terminal verdict (`accepted`, `rejected`, `abandoned`) is counted by its verdicts; its pull request facts remain evidence (they appear under `events`). An agent without one is counted by its pull requests. A merged pull request that is later accepted is therefore one success, not two.
3. The cost of a terminal outcome is the agent's usage since its previous counted terminal outcome. Each usage record belongs to at most one outcome across any sequence of windows. Usage after an agent's last outcome in the window is unattributed.
4. Cost per success is reported only when there is a success and every attributed turn carried a vendor cost. Otherwise it is null, never zero.
5. Unattributed usage in the window (work in progress, and work no outcome measures) is reported beside every ratio.

Each group (by harness and model, harness, role, profile, agent or skill) reports:

- `agents`;
- `succeeded`, `failed` and `abandoned`;
- `reworked` and `recovered`;
- `events` by kind, including superseded evidence;
- `cost` (turns, turns with a cost, tokens and reported cost);
- `cost_per_success_micros`;
- `median_ms_to_success`;
- `mixed_agents`.

**Recovery after failure.** Yang et al. (arXiv:2611.03931) report that role instructions change how often a team submits work, and so how often it gets corrective feedback. In their study, a reviewer that withholds approval leaves first-pass accuracy unchanged but lowers recovery after a failed first answer. AnyAgents therefore measures recovery, not only first-pass success:

- `reworked` counts terminal outcomes whose work received a review failure or a rework request before it ended;
- `recovered` counts the successes among them.

The next measure is feedback opportunity: external verdicts per unit of work (review, CI conclusion, merge, parent verdict), and the time to the first one. The aim is to see who may submit and who acts on feedback. A role that never submits shows up as fewer verdicts and less recovery, which a first-pass success rate hides.

**Guards against Goodhart effects:**

- The measured agent never writes its own outcome. GitHub facts come from REST, and verdicts come from ancestors or the operator. Skill reads are agent-reported, so they are covariates, never outcomes.
- Spend outside measurement stays visible as `unattributed`. An agent cannot improve a ratio by avoiding outcomes.
- Every ratio is shown with its counts. There is no composite score and no leaderboard, and no score is put into an agent's prompt.
- Reports are computed on read. A later revert or verdict changes past windows instead of being frozen out.
- `abandoned` is not counted as the agent's failure. Work is often stopped for reasons outside it.
- Comparisons of who did what are observational and confounded. For example, agents that read a skill differ from those that did not. A decision to promote a skill or change a route uses assignment the system controls (section 3), and the owner sets the minimum evidence.
- A paper's numbers are never targets.

### 3. Skill lifecycle

- **Format.** Skills stay in the Agent Skills format: a directory with `SKILL.md`, whose YAML frontmatter carries `name` and `description`, read by progressive disclosure. Every harness keeps reading them natively. AnyAgents adds identity outside the file and never adds frontmatter keys.
- **Version.** A version is the SHA-256 of the skill directory's content. Versions are immutable and stored by digest under `<data>/skills-store/<name>/<digest>/`. The global view projects the promoted version of each name. The first slice already records the digest of each file read.
- **Registry.** A store-owned `skills` table: `(name, version, state, author, parent_version, provenance)`.
  - `state` is `proposed`, `candidate`, `promoted` or `retired`.
  - Every transition is journaled with its actor and evidence, like `lifecycle_ops`.
  - Provenance names the author agent, the outcome ids and raw-history coordinates of the trajectory the skill came from, and the version it changes.
- **Proposal.** A proposal comes only from a trajectory counted as a success, written by the agent or its lead. It is a new version that cites its parent, never an in-place edit of a promoted version: itemised deltas, not rewrites (ACE). The per-agent overlay keeps working for agent-local skills. Reaching other agents requires the registry.
- **Evaluation.** A candidate is offered to a recorded share of new work in its task class. Its success, recovery and cost per success are compared with the same class without it. The owner promotes or retires it on that evidence. As in DSPy, the skill is a parameter judged by an explicit metric. Here the metric is real outcomes, and no optimiser runs against live work.
- **Retirement.** A version is retired when superseded, or when its class does better without it. Retired versions stay readable for provenance.
- **Telemetry.** This change records reads through the MCP `skills` tool. Native loads are invisible today: Claude Code reads `.claude/skills` itself, and Spiron loads skills in its own capability core. Next, adapters report a skill load as a typed event into the same table. Spiron does it through its headless protocol, naming the skill and digest; Claude Code from the `Skill` tool use in its stream output. Until then, skill comparisons cover only MCP readers, and say so.

### 4. Memory

- **Raw history is the source of truth.** That is mailbox bodies, native transcripts and archive pages. A memory change in progress beside this one makes retrieval over it hybrid (BM25, dense and rerank) and turns off or removes LLM sleep consolidation. This decision adds no summariser and depends on neither choice.
- **Failures become memory by labelling raw history, not by rewriting it.** An outcome row, together with the agent's previous terminal outcome, bounds a stretch of that agent's work in time. Retrieval can filter or rank by outcome kind ("the failed attempts at X", "the accepted one") and returns raw coordinates.
- **Lessons cite their evidence.** A lesson is an agent-authored learned-memory topic that cites the outcome id and raw coordinates it draws on. This is Reflexion's reflection after an external failure signal, kept as a cited note. The pinned section stays within 150 lines and 4 KiB.
- **Retrieval telemetry (next).** Each call records the ids it returned (section 1), so retrieved memory is evaluated the same way as skills.

### 5. Routing

- **Task class is declared, not inferred.** It is the role template's name at spawn, plus an optional `class` label. Unclassified work is its own class.
- **Evidence.** For each class, harness, model and profile, routing reads:
  - n;
  - success rate;
  - recovery rate;
  - cost per success, where cost coverage allows;
  - time to success.

  `GET /v1/outcomes` with `group_by` set to `model`, `role` or `profile` is the first read of this table. Grouping by class arrives with the label.
- **Policy.** The declared default (template, parent, subagent defaults) stays authoritative. The router recommends, and the owner turns on automatic choice per class once the evidence suffices. A small, fixed exploration share keeps the alternatives measured. Hard constraints apply first: operator-declared profile reservations (for example, a scarce subscription kept for lead agents), quota blocks and harness capability.
- **Prerequisite.** Cost must be comparable across harnesses. Every adapter reports cost, or a versioned price table fills `estimated_cost_micros`. Until then, only token counts compare across harnesses.

### 6. Prior work: what we absorb

- **Voyager** (Wang et al., 2023, arXiv:2305.16291). Voyager grows a library of executable skills, adds a skill only after it passes self-verification, retrieves skills by description and composes them.
  - We take: only verified successes enter the library; skills are found by their description; larger skills build on smaller ones.
  - We do not take: the same model's self-check as the only verification. Outcomes are external.
- **Agent Skills** (Anthropic, published 2025-10-16; released as an open standard 2025-12-18). A skill is a directory with a `SKILL.md`. Agents preload only its `name` and `description`, and read the body and linked files on demand.
  - We take: the format unchanged, so every harness reads the same skills natively. Versioning and provenance live beside the files.
- **Reflexion** (Shinn et al., 2023, arXiv:2303.11366). After a failure signal, the agent writes a verbal reflection into episodic memory and uses it on the next attempt.
  - We take: failure produces memory only when tied to an external failure signal, and the note cites that outcome.
  - We do not take: a model's own judgement as the signal.
- **DSPy** (Khattab et al., 2023, arXiv:2310.03714). Pipelines declare their modules; prompts and demonstrations are compiled against an explicit metric.
  - We take: a skill or prompt is a parameter, evaluated against an explicit metric on work it was not built from.
  - We do not take: an optimiser running on live work, or a model-judged metric.
- **Agentic Context Engineering** (Zhang et al., 2025, arXiv:2510.04618). Contexts are evolving playbooks, curated through structured incremental updates. This avoids the brevity bias and context collapse that iterative rewriting causes.
  - We take: skills and memory change by itemised, attributable deltas, never by wholesale model rewrites. This is also why raw history stays the source of truth.
- **Roles, feedback and recovery** (Yang et al., 2026, "Beyond Division of Labor: How Roles Shape Feedback and Recovery in Multi-Agent Systems", arXiv:2611.03931).
  - We take: evaluate roles by recovery after failure and by feedback opportunities, not by first-pass success alone; measure who may submit and who can act on feedback.
  - Their figures are not our targets.

### 7. Scope: what not to build

- **No model as judge of outcomes.** A model's opinion of model work is not ground truth. If one is ever used, it is a separately labelled signal and never counted as an outcome.
- **No automatic learning loop.** No model writes, promotes or rewrites skills on its own. No prompt optimiser runs against live work, and there is no fine-tuning or reinforcement learning.
- **No scores.** No composite quality score, no leaderboard, and no score in an agent's prompt.
- **No copies of other records.** Vendor transcripts and turn state are not copied into the outcome store; turn outcomes are read from `turns`.
- **No review content.** Outcomes do not read review comments or diffs. They use only REST states and check conclusions, within the token permissions documented in [github-watches.md](../github-watches.md).
- **No external dependency.** No evaluation service or tracing platform; outcomes live in the same SQLite store as usage.
- **No inference.** Task class and acceptance are not inferred. An archive without a verdict is not acceptance.

### 8. First slice (this change)

- **Schema.** Migration `029_outcomes.sql` (schema version 28) adds two tables:
  - `outcomes`: append-only, unique per `(agent_id, dedupe_key)`, with the harness, model, role and profile snapshot;
  - `skill_reads`: agent, skill, file, SHA-256 and time.

  Neither table has a CHECK on kind or source. The store validates both, and a reader skips kinds it does not know, so a newer writer never breaks an older reader.
- **Capture.**
  - A PR watch's `merged`, `closed` and `review_failed` transitions record an outcome for the watch's owner (`schedule.rs`, `pull_request_outcome`).
  - Verdicts are recorded through the `agents` tool (`action: "verdict"`), `POST /v1/agents/{name}/verdict` and `anyagents verdict <agent> <verdict> [--reason]`.
  - A `skills` read sends the skill, the file and the digest of its bytes to `POST /v1/skills/reads`. A failed telemetry call never fails the read.
- **Read.** The outcome report is available through:
  - `GET /v1/outcomes`;
  - the `agents` tool (`action: "outcomes"`);
  - `anyagents outcomes`.

  All three take the same target, window and caller boundary as usage (`usage_scope`). `group_by` is `model` (harness and model, the default), `harness`, `role`, `profile`, `agent` or `skill`.
- **Prompting.** The turn-ownership guard's reminder to a parent, and the generated agent context, now ask the parent to record a verdict before following up or archiving.

Not in this slice:

- CI conclusions, reverts and turn outcomes;
- PR subjects that outlive their watcher;
- native skill-load events and retrieval telemetry;
- the skill registry, task classes and the router.

## Invariants

| # | Invariant | Test |
|---|---|---|
| Q1 | A fact recorded twice is one row, and every watcher of a pull request records the same key | `store::outcomes::a_repeated_fact_is_one_row_and_snapshots_what_the_agent_ran` |
| Q2 | Only a strict ancestor, a root admin or the operator records a verdict; never the agent itself, a peer or another root | `server::verdicts_need_an_ancestor_and_outcomes_follow_the_usage_boundary`, `tests/mcp.rs::skill_reads_verdicts_and_outcomes_share_the_authenticated_api` |
| Q3 | A pull request fact is stored on first sight while an earlier wake is unread, before its own wake is claimed, and stays one row | `schedule::pull_request_merge_is_recorded_while_an_earlier_wake_waits`, `schedule::github_pr_watch_stays_active_across_checks_and_review_failure_until_merged` |
| Q4 | A shared GitHub fact counts once, for the earliest watcher, whoever reads the report | `store::outcomes::a_shared_pull_request_counts_once_for_the_agent_that_watched_it_first` |
| Q5 | A terminal verdict supersedes pull request evidence; recovery counts successes after rework | `store::outcomes::verdicts_supersede_pull_requests_and_recovery_is_counted` |
| Q6 | Each usage record is attributed to at most one outcome across consecutive windows; usage after the last outcome is unattributed | `store::outcomes::cost_runs_from_the_previous_terminal_outcome_and_is_conserved_across_windows` |
| Q7 | Cost per success is null unless every attributed turn carried a cost | the Q6 test, `tests/mcp.rs::skill_reads_verdicts_and_outcomes_share_the_authenticated_api` |
| Q8 | Outcome reads follow the usage caller boundary | `server::verdicts_need_an_ancestor_and_outcomes_follow_the_usage_boundary` |
| Q9 | Skill reads are recorded only for the calling agent, with a valid name, relative path and digest | `store::outcomes::verdicts_are_events_and_inputs_are_bounded`, `server::skill_reads_are_recorded_only_for_the_calling_agent` |
| Q10 | Unknown kinds are skipped; deleting an agent removes its outcomes and skill reads | `store::outcomes::unknown_kinds_are_skipped_and_deleting_an_agent_removes_its_evidence` |

## Next steps, in order

1. CI conclusions. `Checks` keeps failure conclusions beside the existing pending flag, and a completed head records `ci_passed` or `ci_failed` without changing the watch's transition names.
2. PR subjects that outlive their watcher (section 1), with ADR 0006 W3.
3. Skill-load events from the Spiron and Claude Code adapters (section 3).
4. Turn outcomes, read from `turns`, once ADR 0006 W2 writes it.
5. Reverts of merged subjects.
6. Retrieval telemetry, after hybrid retrieval.
7. The skill registry, the task-class label and the router recommendation.
8. Cost comparability: adapters report cost, or a price table fills `estimated_cost_micros`.

## Consequences

- An owner can read, per harness and model, how much successful work cost and how often failed work recovered. The ratio is withheld wherever the cost data is incomplete, which today means outside Claude Code.
- Verdicts are only as good as the parents who record them. The guard reminder now asks for them, but a fleet that never records one measures only pull requests.
- Lead agents rarely claim pull requests, so most of their spend reads as unattributed. That is accurate until their own work has outcomes.
- Report cost grows with the number of outcome rows and reported agents. It is computed on read, from one snapshot: all outcome rows up to the window's end, a few indexed queries per reported agent, and one grouped scan of the window's usage.

## Alternatives rejected

- **Outcome columns on `turns` or `usage_records`.** A turn is not a unit of work, and a pull request outlives many turns and sessions.
- **One outcome per agent, updated in place.** Rework, re-acceptance and several pull requests per agent are real; updating in place loses them.
- **CHECK constraints on kind.** Each new kind would need a table rebuild. The store validates kinds, and readers skip unknown ones.
- **Crediting every watcher, or the deepest agent in the tree.** Crediting every watcher counts one merge several times. The deepest agent is not always the author, and finding it needs the whole tree at write time.
- **Counting agents, not outcomes, as successes.** A standing agent's tenth merge would count for nothing.
- **Inferring acceptance from archive, or failure from silence.** Archive also ends abandoned and deprioritised work.
- **A model grading transcripts.** See section 7.
