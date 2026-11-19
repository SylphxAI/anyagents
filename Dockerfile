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

# Thin Coordinator runtime: CA certificates + Node.js + Bun (for the MCP
# `code` JavaScript worker) + headless Chromium/Sway/WayVNC declared in
# `/var/lib/anyagents/toolchain.toml` so MCP `browser` actions work out of the box
# in Pure Serverless deployments. Deliberately contains ZERO local coding harnesses.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        chromium \
        fonts-noto-core \
        nodejs \
        sway \
        tini \
        wayvnc \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 anyagents \
    && useradd --system --uid 10001 --gid 10001 --create-home --home-dir /home/anyagents anyagents \
    && mkdir -p /var/lib/anyagents \
    && CHROMIUM_PATH="$(readlink -f /usr/bin/chromium)" \
    && CHROMIUM_SHA="$(sha256sum "$CHROMIUM_PATH" | awk '{print $1}')" \
    && CHROMIUM_VER="$(/usr/bin/chromium --version | head -n 1 | tr -d '"')" \
    && SWAY_PATH="$(readlink -f /usr/bin/sway)" \
    && SWAY_SHA="$(sha256sum "$SWAY_PATH" | awk '{print $1}')" \
    && SWAY_VER="$(/usr/bin/sway --version | head -n 1 | tr -d '"')" \
    && WAYVNC_PATH="$(readlink -f /usr/bin/wayvnc)" \
    && WAYVNC_SHA="$(sha256sum "$WAYVNC_PATH" | awk '{print $1}')" \
    && WAYVNC_VER="$(/usr/bin/wayvnc --version 2>&1 | head -n 1 | tr -d '"')" \
    && printf 'schema = 1\nchild_path_tail = ["/usr/local/sbin", "/usr/local/bin", "/usr/sbin", "/usr/bin", "/sbin", "/bin"]\n\n[tool.chromium]\npath = "/usr/bin/chromium"\nversion = "%s"\nsha256 = "%s"\ncapability = "browser"\n\n[tool.sway]\npath = "/usr/bin/sway"\nversion = "%s"\nsha256 = "%s"\ncapability = "browser"\n\n[tool.wayvnc]\npath = "/usr/bin/wayvnc"\nversion = "%s"\nsha256 = "%s"\ncapability = "browser.takeover"\n' \
        "$CHROMIUM_VER" "$CHROMIUM_SHA" \
        "$SWAY_VER" "$SWAY_SHA" \
        "$WAYVNC_VER" "$WAYVNC_SHA" \
        > /var/lib/anyagents/toolchain.toml \
    && chown -R 10001:10001 /var/lib/anyagents /home/anyagents

COPY --from=bun-bin /usr/local/bin/bun /usr/local/bin/bun
COPY --from=builder /src/target/release/anyagents /usr/local/bin/anyagents

ENV HOME=/home/anyagents \
    ANYAGENTS_DATA=/var/lib/anyagents \
    ANYAGENTS_SANDBOX_MODE=remote_required

WORKDIR /var/lib/anyagents
USER 10001:10001

EXPOSE 8788

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/anyagents"]
CMD ["serve"]
