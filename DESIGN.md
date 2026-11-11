---
version: alpha
name: AnyAgents
description: Visual identity for AnyAgents, the open-source, vendor-neutral orchestrator that runs any coding agent harness as a long-lived agent. Governs the GitHub repository, README imagery, documentation, the CLI/TUI, social posts and generated images.
colors:
  surface: "#F5F6F1"
  surface-raised: "#FFFFFF"
  on-surface: "#14171B"
  on-surface-muted: "#525960"
  outline: "#C8CCC2"
  primary: "#3B5A00"
  on-primary: "#FFFFFF"
  secondary: "#252A30"
  on-secondary: "#FFFFFF"
  tertiary: "#0F6A73"
  on-tertiary: "#FFFFFF"
  warning: "#8A5800"
  error: "#B3261E"
  on-error: "#FFFFFF"
  surface-dark: "#0D0F12"
  surface-raised-dark: "#171A1F"
  on-surface-dark: "#E6E8E2"
  on-surface-muted-dark: "#9AA1A8"
  outline-dark: "#2D3239"
  primary-dark: "#C6F24A"
  on-primary-dark: "#12160A"
  tertiary-dark: "#5FD3DC"
  warning-dark: "#F0B85A"
  error-dark: "#FF8A80"
  on-error-dark: "#2B0A07"
typography:
  display: { fontFamily: IBM Plex Sans, fontSize: 48px, fontWeight: 600, lineHeight: 1.08, letterSpacing: -0.02em }
  headline-lg: { fontFamily: IBM Plex Sans, fontSize: 32px, fontWeight: 600, lineHeight: 1.2, letterSpacing: -0.01em }
  headline-md: { fontFamily: IBM Plex Sans, fontSize: 22px, fontWeight: 600, lineHeight: 1.3 }
  body-lg: { fontFamily: IBM Plex Sans, fontSize: 18px, fontWeight: 400, lineHeight: 1.6 }
  body-md: { fontFamily: IBM Plex Sans, fontSize: 16px, fontWeight: 400, lineHeight: 1.6 }
  body-sm: { fontFamily: IBM Plex Sans, fontSize: 14px, fontWeight: 400, lineHeight: 1.5 }
  label-md: { fontFamily: IBM Plex Mono, fontSize: 13px, fontWeight: 500, lineHeight: 1.3, letterSpacing: 0.02em }
  label-sm: { fontFamily: IBM Plex Mono, fontSize: 12px, fontWeight: 500, lineHeight: 1.3, letterSpacing: 0.04em }
  code: { fontFamily: IBM Plex Mono, fontSize: 14px, fontWeight: 400, lineHeight: 1.55 }
  wordmark: { fontFamily: IBM Plex Mono, fontSize: 24px, fontWeight: 600, lineHeight: 1, letterSpacing: -0.01em }
rounded:
  none: 0px
  sm: 4px
  md: 6px
  lg: 10px
  full: 9999px
spacing:
  xs: 4px
  sm: 8px
  md: 16px
  lg: 24px
  xl: 40px
  xxl: 64px
  content-max: 760px
  page-max: 1200px
  tree-indent: 20px
components:
  button-primary: { backgroundColor: "{colors.primary}", textColor: "{colors.on-primary}", typography: "{typography.label-md}", rounded: "{rounded.md}", padding: 10px 16px, height: 36px }
  button-primary-dark: { backgroundColor: "{colors.primary-dark}", textColor: "{colors.on-primary-dark}" }
  button-secondary: { backgroundColor: "{colors.surface-raised}", textColor: "{colors.on-surface}", typography: "{typography.label-md}", rounded: "{rounded.md}", padding: 10px 16px, height: 36px }
  button-secondary-dark: { backgroundColor: "{colors.surface-raised-dark}", textColor: "{colors.on-surface-dark}" }
  input: { backgroundColor: "{colors.surface-raised}", textColor: "{colors.on-surface}", typography: "{typography.code}", rounded: "{rounded.sm}", padding: 8px 12px, height: 36px }
  input-dark: { backgroundColor: "{colors.surface-raised-dark}", textColor: "{colors.on-surface-dark}" }
  card: { backgroundColor: "{colors.surface-raised}", textColor: "{colors.on-surface}", rounded: "{rounded.lg}", padding: 24px }
  card-dark: { backgroundColor: "{colors.surface-raised-dark}", textColor: "{colors.on-surface-dark}" }
  nav: { backgroundColor: "{colors.surface}", textColor: "{colors.on-surface}", typography: "{typography.label-md}", height: 56px, padding: 0 24px }
  nav-dark: { backgroundColor: "{colors.surface-dark}", textColor: "{colors.on-surface-dark}" }
  terminal-block: { backgroundColor: "{colors.surface-dark}", textColor: "{colors.on-surface-dark}", typography: "{typography.code}", rounded: "{rounded.lg}", padding: 16px 20px }
  agent-node: { backgroundColor: "{colors.surface-raised-dark}", textColor: "{colors.on-surface-dark}", typography: "{typography.label-md}", rounded: "{rounded.sm}", padding: 6px 10px }
  status-live: { backgroundColor: "{colors.surface-dark}", textColor: "{colors.primary-dark}", typography: "{typography.label-sm}" }
  status-waiting: { backgroundColor: "{colors.surface-dark}", textColor: "{colors.warning-dark}", typography: "{typography.label-sm}" }
  status-failed: { backgroundColor: "{colors.surface-dark}", textColor: "{colors.error-dark}", typography: "{typography.label-sm}" }
  status-queued: { backgroundColor: "{colors.surface-dark}", textColor: "{colors.tertiary-dark}", typography: "{typography.label-sm}" }
  status-waiting-light: { backgroundColor: "{colors.surface-raised}", textColor: "{colors.warning}", typography: "{typography.label-sm}" }
  caption: { backgroundColor: "{colors.surface}", textColor: "{colors.on-surface-muted}", typography: "{typography.body-sm}" }
  divider-dark: { backgroundColor: "{colors.outline-dark}", height: 1px }
  harness-chip: { backgroundColor: "{colors.surface-raised-dark}", textColor: "{colors.on-surface-muted-dark}", typography: "{typography.label-sm}", rounded: "{rounded.full}", padding: 2px 10px }
---

# AnyAgents

## Overview

AnyAgents is a switchboard for coding agents: one server that keeps many vendors' harnesses running as long-lived agents, with durable mailboxes, groups, schedules and channels. The audience is developers and platform engineers who live in terminals, read READMEs before they install, and distrust marketing. It should feel like well-made infrastructure: calm, exact, legible at a glance, visibly neutral between vendors.

**Concept: signal through a tree.** The brand picture is an agent tree on a dark console, quiet graphite nodes joined by thin lines, with one path lit in Relay Lime to show a message being delivered. Lime means "live, delivered, go"; everything else stays neutral so that no harness or vendor is privileged.

**Mark.** No logo exists yet. Until one is drawn, the mark is the lowercase wordmark `anyagents` set in the `wordmark` style, preceded by a three-node glyph: one node above two, joined by 1.5px lines, the lower-right node filled lime. Use the product name "AnyAgents" (capital A twice) in prose and `anyagents` only for the command, package and wordmark.

**Voice.** Plain, precise, verifiable. Lead with what it does and the command that does it. State guarantees as tested facts ("no message lost across a crash"), never adjectives ("blazing", "magical", "revolutionary"). Name harnesses neutrally and in a stable order; never rank vendors. Short sentences, active voice, sentence-case headings, no emoji.

**Images, posts and illustration.** Three permitted kinds, all on `surface-dark` (#0D0F12) unless placed in light docs:
1. Real terminal or TUI captures at 2x, cropped to the relevant lines, in `terminal-block` framing.
2. Flat vector diagrams of agents, mailboxes and channels: graphite nodes (#171A1F, outline #2D3239), 1.5px connectors in #9AA1A8, one highlighted path in #C6F24A, labels in IBM Plex Mono.
3. Typographic cards for releases and posts: headline in IBM Plex Sans 600, one command or number as the hero in Plex Mono, lime used once.
No photos of people, no robots, humanoids, glowing brains, circuit boards, neon gradients or 3D renders; no vendor logos except in a factual compatibility list where all appear at equal size in monochrome.

## Colors

Dark is the native mode (terminals, README hero, TUI); light exists for docs and GitHub light theme. Dark components inherit size, type and shape from their light counterpart and change only colour. Never mix modes in one view.

- **Paper (#F5F6F1) / Raised (#FFFFFF):** light page and card surfaces, faintly warm off-white.
- **Ink (#14171B):** light-mode text. **Muted Ink (#525960):** metadata, captions, timestamps.
- **Outline (#C8CCC2) / Outline Dark (#2D3239):** 1px borders and dividers; never used for text.
- **Relay Olive (#3B5A00):** light-mode primary: primary buttons, links, active tree path. White text on it.
- **Relay Lime (#C6F24A):** dark-mode primary and the brand accent. Text on lime is always #12160A.
- **Graphite (#252A30):** secondary, for neutral solid controls in light mode with white text.
- **Tide (#0F6A73 light / #5FD3DC dark):** tertiary, for informational notes, selected-but-not-active states and the "queued" lane in diagrams.
- **Amber (#8A5800 light / #F0B85A dark):** waiting, lagging or needs-sign-in states.
- **Error (#B3261E light / #FF8A80 dark):** failed delivery, crashed harness, destructive actions. Text on #B3261E is white; text on #FF8A80 is #2B0A07.
- **Console (#0D0F12) / Console Raised (#171A1F):** dark surfaces. **Bright Ink (#E6E8E2)** is dark-mode text; **Muted Bright (#9AA1A8)** is dark-mode metadata.

**CLI/TUI mapping.** Respect the user's terminal theme: use ANSI roles, not truecolor, by default. Green/bright green = live/delivered, yellow = waiting, red = failed, cyan = info, dim = metadata. When truecolor is explicitly enabled, use the dark tokens above. Honour `NO_COLOR`; every state also carries a text label so meaning never depends on colour.

## Typography

One superfamily, two voices, both SIL Open Font License: **IBM Plex Sans** for headings and prose, **IBM Plex Mono** for everything a user could type or a machine emits (commands, identifiers, labels, status, the wordmark).

- **Display / headline-lg / headline-md:** Plex Sans 600 with slight negative tracking; sentence case.
- **Body (18/16/14px):** Plex Sans 400 at 1.5–1.6 line height; 16px is the docs default.
- **Labels (13/12px):** Plex Mono 500, lowercase, for agent ids, harness names, timestamps and status words. No all-caps.
- **Code (14px):** Plex Mono 400 in `terminal-block` and inline code.

Fallback stacks: `"IBM Plex Sans", system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif` and `"IBM Plex Mono", ui-monospace, "SF Mono", Menlo, Consolas, "Liberation Mono", monospace`. The product ships in English; if CJK text appears, append `"Noto Sans CJK SC", "Noto Sans CJK TC", "Noto Sans CJK JP"` (also OFL). In terminals, never assume ligatures or Nerd Font glyphs.

## Layout

A 4px base unit; use the named steps (4, 8, 16, 24, 40, 64px) and nothing in between. Docs use a single reading column of at most 760px with the page capped at 1200px; side navigation sits left at 240px on wide screens and collapses below 960px. Diagrams and hero images are 1280×640 (GitHub social preview ratio 2:1) with a 64px safe margin.

Arrange information as a tree, not a dashboard: parents above children, indented 20px per level (`tree-indent`), connectors drawn in outline colour. Lists of agents are dense rows (one agent per line: status, id, harness, lag) rather than card grids. In the CLI, align columns, keep lines under 100 characters and print the most important value first.

## Elevation & Depth

Flat. Hierarchy comes from tonal steps and 1px outlines, never drop shadows or blur. Light: Paper page → Raised card with #C8CCC2 border. Dark: Console page → Console Raised card with #2D3239 border. A focused or selected element gets a 2px ring in the mode's primary colour with 2px offset. Menus and dialogs use the raised surface, outline and a 60% Console scrim; no glass effects.

## Shapes

Engineered, slightly softened rectangles. Inputs and tree nodes use 4px, buttons 6px, cards, terminal blocks and image frames 10px. Fully rounded shapes are reserved for small chips (harness names, counts) and status dots (8px circles). Lines are 1px for borders and 1.5px for diagram connectors with square ends; arrows are simple open chevrons.

## Components

- **Buttons:** `button-primary` (olive, white text) or `button-primary-dark` (lime, near-black text) for the one main action per view, labelled with a verb in Plex Mono ("install", "create agent"). `button-secondary` is a raised surface with a 1px outline. Hover shifts 8% in lightness; disabled is 40% opacity.
- **Input:** monospace text, 1px outline, 4px corners; focus shows the primary ring; errors show an error-coloured outline and a one-line message below in `body-sm`.
- **Card:** raised surface, 1px outline, 24px padding; for callouts and feature summaries, never nested.
- **Nav:** 56px bar on the page surface with the wordmark left, text links in Plex Mono right, and a GitHub link last. Active link is underlined 2px in primary.
- **Terminal block:** always dark, even in light docs. Prompt `$` in Muted Bright, command in Bright Ink, output dimmed; one copy button top-right. This is the hero component of the README.
- **Agent node:** the unit of every diagram and TUI tree: a 4px-cornered raised box with a status dot, the agent name in `label-md` and its harness as a `harness-chip`.
- **Status (`status-live`, `status-queued`, `status-waiting`, `status-failed`; `-light` variants on light surfaces):** dot plus lowercase word ("live", "queued", "waiting", "failed"); colour never stands alone.
- **Harness chip:** identical neutral styling for every harness, listed alphabetically or in the user's configured order.

## Do's and Don'ts

- Do open every surface with what AnyAgents does and a runnable command.
- Do keep Relay Lime / Relay Olive to one element group per view; it marks the live path or the main action.
- Do give every harness the same neutral chip; never use vendor brand colours, logos or ordering to imply preference.
- Do pair every colour-coded state with a text label and honour `NO_COLOR` in the CLI.
- Do use real captures and accurate diagrams; every number in an image must be reproducible.
- Don't use robots, humanoid avatars, brains, sparkles, neon gradients or stock photos.
- Don't add drop shadows, glass effects or more than two type weights (400, 600; 500 only for mono labels) in one view.
- Don't write hype adjectives or claims the test suite does not prove.
- Don't borrow colours or styling from other products, including the harnesses AnyAgents runs.
