# 0003: Keep AnyAgents and qualify its position

Status: accepted. Date: **2026-10-25**.

## Context and dated evidence

The [2026-10-25 competitor review](../competitor-comparison-2026-10-25.md)
records the primary-source register, scope limits and filed improvements.
The previous [name decision in the vision](../vision.md#name-and-position)
connects AnyAgents to the AnyMD family and uses the scoped launcher name
`@sylphx/anyagents`.

Two naming neighbors merit explicit treatment:

- Mozilla AI's **any-agent** describes a common interface for running and
  evaluating agent frameworks, and now calls itself softly deprecated. Source:
  [README at `a469723`](https://github.com/mozilla-ai/any-agent/blob/a469723ac4a0518ab0696d57e09e4dc407c826ec/README.md),
  repository snapshot dated **2026-09-21**, read **2026-10-25 UTC**. Similar
  wording is a potential discovery risk, not evidence of measured confusion or
  permission to use its branding.
- The unscoped npm **`anyagents`** exists independently. Its
  [1.0.0 metadata](https://registry.npmjs.org/anyagents/1.0.0) describes canonical
  instructions, skills and MCP configuration for Claude Code and Codex and
  points to `vspdi/anyagents`. The [registry publication record](https://registry.npmjs.org/anyagents)
  dates 1.0.0 to **2026-10-23 17:48:38 UTC**; both sources read **2026-10-25 UTC**.
  Do not direct our users to install the unscoped package.

## Decision and reason

**Do not rename the repository, binary, or scoped npm launcher in this review.**
Keep **AnyAgents**, with the descriptor **“the long-lived coding-agent
switchboard”** when introducing it. Keep distribution as **`@sylphx/anyagents`**.

The name fits the existing AnyMD family and the product's harness/model/channel
separation. A rename would change repository links, CLI examples, crate/package
names and discovery surfaces without a tested alternative or evidence it would
improve recognition. The documented collisions are real; the correct immediate
response is qualified positioning and an unmistakable scoped installation path,
not pretending the name is unique or claiming that a new name is available.
This preserves the earlier decision while recording the new comparison.

## Alternatives and consequences

- **Rename immediately:** rejected for now. No alternative has been validated
  for user comprehension, package/domain availability or legal clearance.
- **Use bare `anyagents` on npm:** rejected. It is a different package according
  to the dated registry source above. Keep the CLI name distinct from the
  package installation name in examples.
- **Keep the name without qualification:** rejected. “Any agents” is too broad
  to explain a vendor-harness supervisor rather than a framework abstraction.

No redirects, registry transfers, account changes or repository rename are
performed by this decision. It is not a trademark clearance or exclusivity
claim. In new onboarding material ([#26](https://github.com/SylphxAI/anyagents/issues/26)),
use the scoped launcher and the descriptor. Reconsider a rename if support or
search evidence demonstrates material confusion, a legal concern is raised, or
a validated alternative materially improves comprehension; record a superseding
ADR before any coordinated rename/migration.
