# Contributing to AnyAgents

## How to Contribute

1. **Reporting Issues:** Search [existing issues](https://github.com/SylphxAI/anyagents/issues) before opening a [bug, feature or documentation report](https://github.com/SylphxAI/anyagents/issues/new/choose).
   - Include the `anyagents version`, the command or MCP tool arguments, and the smallest reproducible example.
   - Describe the expected result and what actually happened. Redact private paths, tokens and agent transcripts before sharing logs.
   - Explain the use case and a testable outcome for feature requests.
   - Use [Discussions](https://github.com/SylphxAI/anyagents/discussions) for questions and ideas; report vulnerabilities through [SECURITY.md](SECURITY.md).

2. **Submitting Pull Requests:**
   - Fork the repository.
   - Create a new branch for your change.
   - Make your changes, adhering to the project's coding style and guidelines.
   - Add tests for your changes and ensure the whole workspace passes.
   - Ensure your commit messages follow the conventional commits standard.
   - Push your branch to your fork.
   - Open a Pull Request against the `main` branch of the [SylphxAI/anyagents](https://github.com/SylphxAI/anyagents) repository.

## Development Setup

AnyAgents is a Rust workspace (`crates/`) that builds one binary, `anyagents`, which is the server, the command line and the per-agent MCP entry point. The npm package (`packages/`) is a small launcher for that binary. The design is in [`docs/`](docs/).

### Prerequisites

- Rust stable, at least the workspace `rust-version` (`rustup`)
- Node.js >= 18 to try the npm launcher against a local build

### Getting Started

```bash
git clone https://github.com/SylphxAI/anyagents.git
cd anyagents
cargo build --release -p anyagents
node packages/anyagents/bin/anyagents.js --help  # the launcher finds target/release/anyagents
```

### Useful Commands

```bash
cargo fmt --all --check             # Formatting
cargo clippy --workspace --all-targets -- -D warnings   # Linting, warnings are errors
cargo test --workspace              # Every workspace test
cargo build --release -p anyagents  # Build the binary the npm launcher runs
```

### Coding Standards

- **Formatting and linting:** `cargo fmt` and `cargo clippy` with `-D warnings`. Run both before submitting.
- **Testing:** Tests live beside each crate, with end-to-end tests in `crates/anyagents/tests/`. Every delivery guarantee in [docs/architecture.md](docs/architecture.md#guarantees) is a tested invariant, not a convention; keep it that way.
- **One process, one binary:** the server, the CLI and the MCP entry point stay in one binary. A new client is a client of the same local API, not a second server.
- **Commits:** Follow conventional commits (e.g., `feat:`, `fix:`, `docs:`, `chore:`, `refactor:`).

### Release Process

Nothing is released yet. When there is a release, it is a pull request that bumps the version in the workspace and `packages/anyagents/package.json` together and adds a `## X.Y.Z` section to [CHANGELOG.md](CHANGELOG.md); merging it publishes the npm launcher and its platform packages.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
