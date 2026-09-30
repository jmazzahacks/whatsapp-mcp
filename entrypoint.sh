#!/usr/bin/env bash
# Runs the Go bridge and the Python MCP server side by side. If either one
# exits, stop the other and exit, so Docker's restart policy restarts both
# together instead of leaving a half-alive container.
set -u

# Bridge uses relative store/ paths, so it must run from its own directory.
# Its output goes through bridge_log_forwarder.py, which echoes each line to
# stdout (docker logs) and ships it to Loki when DEBUG_LOCAL=false.
(cd /app/whatsapp-bridge && exec ./whatsapp-client) \
    > >(cd /app/whatsapp-mcp-server && exec .venv/bin/python bridge_log_forwarder.py) 2>&1 &
BRIDGE_PID=$!

(cd /app/whatsapp-mcp-server && exec .venv/bin/python main.py) &
MCP_PID=$!

stop_all() {
    kill -TERM "$BRIDGE_PID" "$MCP_PID" 2>/dev/null
}
trap stop_all TERM INT

wait -n
STATUS=$?
echo "entrypoint: a process exited (status $STATUS); stopping the other" >&2
stop_all
wait
exit "$STATUS"
