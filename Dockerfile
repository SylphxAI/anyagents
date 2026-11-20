# syntax=docker/dockerfile:1

ARG RUST_IMAGE=rust:1-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim

# Build from console source, independent of the target Rust architecture.
FROM --platform=$BUILDPLATFORM node:22.23.5-bookworm-slim@sha256:e9f565d32859f0cbfb52125eafe8be39390f24dac302dc3cdf566e8ba3ab21ee AS console
WORKDIR /src/apps/console
COPY apps/console/package.json apps/console/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci --no-audit --no-fund
COPY apps/console ./
RUN npm run build

FROM ${RUST_IMAGE} AS builder

WORKDIR /src

# Install native build tooling required by workspace crates without any coding harnesses.
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY tests ./tests
RUN rm -rf crates/anyagents/src/server/console
COPY --from=console /src/crates/anyagents/src/server/console ./crates/anyagents/src/server/console

ARG ANYAGENTS_GIT_SHA

RUN cargo build --release --locked -p anyagents --bin anyagents \
    && (strip /src/target/release/anyagents || true)

FROM ${RUNTIME_IMAGE} AS runtime

# Minimal Coordinator runtime: CA certificates + tini + the `anyagents` binary.
# Browser, web, desktop, code execution, and coding harnesses run exclusively
# inside remote sandboxes (or optional standalone MCP servers).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 anyagents \
    && useradd --system --uid 10001 --gid 10001 --create-home --home-dir /home/anyagents anyagents \
    && mkdir -p /var/lib/anyagents \
    && chown -R 10001:10001 /var/lib/anyagents /home/anyagents

COPY --from=builder /src/target/release/anyagents /usr/local/bin/anyagents

ENV HOME=/home/anyagents \
    ANYAGENTS_DATA=/var/lib/anyagents \
    ANYAGENTS_SANDBOX_MODE=remote_required

WORKDIR /var/lib/anyagents
USER 10001:10001

EXPOSE 8788

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/anyagents"]
CMD ["serve"]
