# Single image running both processes: the Go WhatsApp bridge and the Python
# MCP server. entrypoint.sh supervises them.

# --- Stage 1: build the Go bridge (CGO required by go-sqlite3) ---
FROM golang:1.26-trixie AS bridge-build

WORKDIR /src
COPY whatsapp-bridge/go.mod whatsapp-bridge/go.sum ./
RUN go mod download
COPY whatsapp-bridge/main.go ./
RUN CGO_ENABLED=1 go build -o whatsapp-client .

# --- Stage 2: Python deps (git needed for the GitHub-hosted logging libs) ---
FROM python:3.13-slim-trixie AS python-build

RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:0.9.28 /uv /usr/local/bin/uv

# Same path as the runtime stage so the venv's absolute paths stay valid
WORKDIR /app/whatsapp-mcp-server
COPY whatsapp-mcp-server/pyproject.toml whatsapp-mcp-server/uv.lock whatsapp-mcp-server/.python-version ./
ENV UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=/usr/local/bin/python3 \
    UV_COMPILE_BYTECODE=1
RUN uv sync --frozen --no-dev --no-install-project

# --- Stage 3: runtime ---
FROM python:3.13-slim-trixie

# ffmpeg: audio.py converts audio to Opus .ogg before sending voice messages
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Layout mirrors the repo: whatsapp.py resolves messages.db as
# ../whatsapp-bridge/store/messages.db relative to itself, and talks to the
# bridge on localhost:8080. Both work unchanged inside one container.
WORKDIR /app/whatsapp-mcp-server
COPY --from=python-build /app/whatsapp-mcp-server/.venv ./.venv
COPY whatsapp-mcp-server/main.py whatsapp-mcp-server/whatsapp.py whatsapp-mcp-server/audio.py \
     whatsapp-mcp-server/bridge_log_forwarder.py ./

WORKDIR /app/whatsapp-bridge
COPY --from=bridge-build /src/whatsapp-client ./whatsapp-client
# store/ holds whatsapp.db (session keys) + messages.db; mounted as a volume
RUN mkdir -p store

WORKDIR /app
COPY entrypoint.sh ./entrypoint.sh

# Non-root user — uid pinned so the host-side store/ volume can be chowned to match.
# Only store/ is writable: code, binaries and .venv stay root-owned so a bad
# write (e.g. a hostile media filename) can't plant code that runs on restart.
RUN useradd --uid 1000 --create-home appuser && chown appuser:appuser /app/whatsapp-bridge/store
USER appuser

EXPOSE 8000

# Transport for Docker, bind to all interfaces
ENV MCP_TRANSPORT=streamable-http
ENV MCP_HOST=0.0.0.0
ENV MCP_PORT=8000
ENV FASTMCP_HOST=0.0.0.0
ENV FASTMCP_PORT=8000

# Healthcheck: probe socket.gethostname(), NOT 127.0.0.1. A loopback
# probe from inside the container succeeds even when the server is
# bound only to 127.0.0.1 — gethostname() resolves to the container's
# address on the docker network, which is the path nginx actually uses.
# Port resolution mirrors the FastMCP fallback chain
# (FASTMCP_PORT → MCP_PORT → 8000). Bridge liveness is covered by
# entrypoint.sh, which exits the container if the bridge dies.
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import os,socket; socket.create_connection((socket.gethostname(), int(os.getenv('FASTMCP_PORT', os.getenv('MCP_PORT','8000')))), timeout=3).close()" || exit 1

CMD ["/app/entrypoint.sh"]
