# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture

Two-process system. Neither runs the other — both must be running for the MCP integration to work.

1. **Go WhatsApp bridge** (`whatsapp-bridge/main.go`, single file): authenticates to WhatsApp Web via the `whatsmeow` library, persists messages to SQLite, and exposes a small HTTP API on `localhost:8080`:
   - `POST /api/send` — `{recipient, message}` for text, or `{recipient, media_path}` for files (the bridge sniffs the file type and routes images/video/audio/documents appropriately)
   - `POST /api/download` — `{message_id, chat_jid}` returns the local path after fetching the media blob from WhatsApp servers using the stored `media_key`/`url`/`file_sha256`/`file_enc_sha256`

2. **Python MCP server** (`whatsapp-mcp-server/`): `main.py` defines FastMCP tools; `whatsapp.py` is the actual implementation. Read paths query the SQLite DB directly; write/download paths POST to the Go bridge's HTTP API. `audio.py` shells out to `ffmpeg` to convert arbitrary audio into `.ogg` Opus before sending as a voice message.

### Data storage

`whatsapp-bridge/store/` holds two SQLite databases — both written by the Go bridge, the messages DB also read directly by the Python MCP:
- `whatsapp.db` — `whatsmeow`'s own session/device store (auth credentials, keys). Owned by the library.
- `messages.db` — application-level message history. Schema (`chats`, `messages`) is created in `NewMessageStore()` in `main.go`. The Python side opens this file read-only via `MESSAGES_DB_PATH` in `whatsapp.py`. Media blobs are NOT stored — only metadata + the keys needed to re-download.

Deleting either DB forces re-authentication / full history resync.

### Critical coupling points

- `MESSAGES_DB_PATH` in `whatsapp.py` is a hardcoded relative path (`../whatsapp-bridge/store/messages.db`). The MCP server only works when launched from inside `whatsapp-mcp-server/`, which is why the Claude Desktop config uses `uv --directory`.
- `WHATSAPP_API_BASE_URL` is hardcoded to `http://localhost:8080/api`. The bridge port is not configurable from either side without code edits.
- The Go side stores timestamps as SQLite `TIMESTAMP` (ISO-8601 strings), and Python parses them with `datetime.fromisoformat`. This is pre-existing project style — do not refactor to unix timestamps unless asked.

## Common commands

Run the bridge (foreground, shows QR code on first run):
```bash
cd whatsapp-bridge
go run main.go
```

Run the MCP server standalone for testing (normally launched by Claude Desktop / Cursor, not by hand):
```bash
cd whatsapp-mcp-server
uv run main.py
```

Install/sync Python deps (uv-managed; do NOT use bare pip here — this project uses `uv` with a lockfile):
```bash
cd whatsapp-mcp-server
uv sync
```

Tidy Go deps:
```bash
cd whatsapp-bridge
go mod tidy
```

There is no test suite, no linter config, and no build script — the bridge is run with `go run` and the MCP server is launched by the host (Claude Desktop / Cursor) via `uv`.

## Project conventions

- **Don't refactor adjacent code.** This is a fork of `lharries/whatsapp-mcp` and the existing code has style inconsistencies (datetime-strings in SQLite, `Tuple[bool, str]` returns in `whatsapp.py`). Leave them alone unless the change is explicitly requested — staying close to upstream makes future rebases tractable.
- **License**: MIT (see `LICENSE`). This is a public OSS repo, so be mindful of anything sensitive before committing.

## Adding a new MCP tool

1. Add the implementation in `whatsapp-mcp-server/whatsapp.py` (either a direct SQLite query for reads, or an HTTP call to the Go bridge for actions).
2. If it requires a new bridge endpoint, add the `http.HandleFunc` in `whatsapp-bridge/main.go` near the existing `/api/send` and `/api/download` handlers.
3. Register the tool in `whatsapp-mcp-server/main.py` with `@mcp.tool()` and a docstring — FastMCP uses the docstring + type hints as the tool schema sent to Claude, so be precise about argument semantics (especially `recipient` formats: bare phone number vs `@s.whatsapp.net` vs `@g.us` group JID).
4. Restart both processes — Claude Desktop won't re-read tool definitions until the MCP server is relaunched.
