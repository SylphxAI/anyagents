# syntax=docker/dockerfile:1

ARG RUST_IMAGE=rust:1-bookworm
ARG BUN_IMAGE=oven/bun:1-slim
ARG RUNTIME_IMAGE=debian:bookworm-slim

FROM ${RUST_IMAGE} AS builder

WORKDIR /src

# Install native build tooling required by workspace crates without any coding harnesses.
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY tests ./tests

RUN cargo build --release --locked -p anyagents --bin anyagents

FROM ${BUN_IMAGE} AS bun-bin

FROM ${RUNTIME_IMAGE} AS runtime

# Minimal Thin Coordinator runtime: CA certificates + Node.js + Bun (for the MCP
# `code` JavaScript worker). Deliberately contains ZERO local coding harnesses and
# ZERO headless browsers / Chromium.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates nodejs tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 anyagents \
    && useradd --system --uid 10001 --gid 10001 --create-home --home-dir /home/anyagents anyagents \
    && mkdir -p /var/lib/anyagents \
    && chown -R 10001:10001 /var/lib/anyagents /home/anyagents

COPY --from=bun-bin /usr/local/bin/bun /usr/local/bin/bun
COPY --from=builder /src/target/release/anyagents /usr/local/bin/anyagents

ENV HOME=/home/anyagents \
    ANYAGENTS_DATA_DIR=/var/lib/anyagents \
    ANYAGENTS_SANDBOX_MODE=remote_required

WORKDIR /var/lib/anyagents
USER 10001:10001

EXPOSE 8788

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/anyagents"]
CMD ["serve"]
