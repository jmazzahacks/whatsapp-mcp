"""Forward the Go bridge's stdout/stderr to Loki via byteforge-loki-logging.

Used only in Docker: entrypoint.sh pipes the bridge's output into this
script. Every line is echoed to stdout unchanged (so `docker logs` keeps
the full bridge output, including the pairing QR code), and in production
(DEBUG_LOCAL=false) also shipped to Loki under logger "bridge".
"""
import logging
import os
import re
import sys

from byteforge_loki_logging import configure_logging

APPLICATION_TAG = "whatsapp-mcp"

# whatsmeow's waLog.Stdout colours lines and tags them "[Module LEVEL]"
ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")
WALOG_LEVEL = re.compile(r"\[[^\]]* (DEBUG|INFO|WARN|ERROR)\]")
LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARN": logging.WARNING,
    "ERROR": logging.ERROR,
}
# Pairing QR code rows are only block characters — noise in Loki
QR_ROW = re.compile(r"^[\s▀▄█]+$")


def level_for(line: str) -> int:
    match = WALOG_LEVEL.search(line)
    if match is None:
        return logging.INFO
    return LEVELS[match.group(1)]


def main() -> None:
    ship_to_loki = os.getenv("DEBUG_LOCAL", "true").lower() != "true"
    logger = logging.getLogger("bridge")
    if ship_to_loki:
        configure_logging(
            application_tag=APPLICATION_TAG,
            debug_local=False,
            local_level=os.getenv("LOG_LEVEL", "INFO"),
        )

    for raw_line in sys.stdin:
        sys.stdout.write(raw_line)
        sys.stdout.flush()
        if not ship_to_loki:
            continue
        line = ANSI_ESCAPE.sub("", raw_line).rstrip()
        if not line or QR_ROW.match(line):
            continue
        logger.log(level_for(line), line)

    logging.shutdown()


if __name__ == "__main__":
    main()
