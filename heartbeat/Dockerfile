# https://developers.home-assistant.io/docs/apps/configuration#app-dockerfile
ARG BUILD_FROM=ghcr.io/home-assistant/base:3.23
FROM ${BUILD_FROM}

# uv: copy the static binaries in (reproducible, arch-correct under BuildKit).
COPY --from=ghcr.io/astral-sh/uv:0.11.7 /uv /uvx /usr/local/bin/

# Runtime tools for the heartbeat checks:
#   iproute2   -> `ip route show default`        (gateway discovery)
#   iputils    -> `ping` with -c / -W            (ICMP, needs NET_RAW)
#   bind-tools -> `nslookup`                      (DNS fallback)
#   musl-utils -> `getent`                        (NOT in the base image on musl)
#   ca-certificates -> TLS to the MQTT broker
RUN apk add --no-cache iproute2 iputils bind-tools musl-utils ca-certificates

ENV UV_PYTHON_INSTALL_DIR=/opt/uv/python \
    UV_PYTHON_PREFERENCE=only-managed \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_NO_CACHE=1 \
    UV_PROJECT_ENVIRONMENT=/opt/heartbeat/.venv

WORKDIR /opt/heartbeat

# Managed CPython 3.14 (musl build) — rarely changes, so cache it early.
RUN uv python install 3.14

# Dependencies before source so this layer survives source-only edits.
COPY app/pyproject.toml app/uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Application source, then install the project itself.
COPY app/ ./
RUN uv sync --frozen --no-dev

# s6 service scripts last, so editing them doesn't bust the Python layers.
COPY rootfs /

LABEL \
    org.opencontainers.image.title="Home Assistant App: Heartbeat" \
    org.opencontainers.image.description="Network heartbeat checker (gateway/DNS/MQTT) with SQLite store-and-forward." \
    org.opencontainers.image.source="https://github.com/Voyanti/ha-metadata" \
    org.opencontainers.image.licenses="Apache License 2.0"
