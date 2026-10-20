# Security Policy

## Supported versions

Security fixes land on the latest published `@sylphx/anyagents` release. We
fix forward — please upgrade to the newest version before reporting.

## Reporting a vulnerability

**Please report privately. Do not open a public issue for security defects.**

Preferred channel — GitHub private vulnerability reporting (does not depend on
email delivery):

1. Go to the repository's **Security** tab.
2. Click **Report a vulnerability** (GitHub private advisory).
3. Include the details below.

This routes straight to the maintainers and lets us collaborate on a fix and a
coordinated advisory in one place.

Please include:

- affected version(s) and platform;
- a clear description and impact;
- minimal steps to reproduce or a proof of concept;
- any suggested remediation.

We aim to acknowledge a report within a few days, agree on severity and a fix
window, and publish a GitHub Security Advisory (GHSA, with a CVE where it
qualifies) once a fixed version ships. We are glad to credit reporters by their
chosen handle.

## Scope

In scope: defects in this package's own code — authentication/authorization
bypass, credential or transcript disclosure, path traversal, command or argument
injection into a supervised harness process, and similar. Issues in
third-party dependencies (a harness, a model provider) should be reported
upstream; tell us if this project's configuration meaningfully amplifies them.

## Design that limits exposure

AnyAgents is local-first and, by default, listens on a Unix socket owned by the
user. Network access is opt-in and needs a token. A few facts bound what a
report can normally touch:

- **Vendor credentials stay with the vendor tool.** Each auth profile has its
  own config directory that AnyAgents owns, and a harness is started with its
  documented config-directory setting pointed there (for example
  `CLAUDE_CONFIG_DIR`, `CODEX_HOME`). The vendor tool writes and reads its own
  sign-in credential; AnyAgents never reads the token, never logs it, and never
  routes a subscription credential through the gateway.
- **The gateway holds no keys on disk.** API keys live in AnyAgents' secret
  store. Each harness process reaches the built-in gateway through a per-agent
  token, and the gateway is bound to loopback so only the processes AnyAgents
  started can use it.
- **The mailbox is durable and local.** Messages, transcripts and usage are
  stored in one embedded database under the data directory (`--data`, default
  `.anyagents`). Protect that directory as you would any file holding your
  agent history.

## Operator hardening checklist

- Keep the data directory on a filesystem only you can read; it holds agent
  transcripts and usage.
- Prefer the default Unix-socket API. When you enable a network listener,
  require a token and place it behind a network you control.
- Treat a harness install as code execution: AnyAgents downloads a harness from
  its vendor's official channel, and you should review the harness you enable.
- Redact agent transcripts, personal paths and tokens before sharing logs in a
  public issue or discussion.
