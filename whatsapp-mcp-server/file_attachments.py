"""Resolve File Friend attachments without exposing arbitrary bridge files."""

import os
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError

from byteforge_mcp_file_friend_api_python import Client, open_reference
from byteforge_mcp_file_friend_models import FileReference

import whatsapp

MAX_FILE_BYTES = 16 * 1024**2
RESOLVE_SECONDS = 120


@dataclass(frozen=True)
class FileSendRequest:
    recipient: str
    media_path: str | None = None
    file_reference: FileReference | None = None
    caption: str = ""


@dataclass(frozen=True)
class FileSendResult:
    success: bool
    message: str


def send_attachment(request: FileSendRequest) -> FileSendResult:
    if not request.recipient.strip():
        return FileSendResult(False, "Recipient must be provided")
    if (request.media_path is not None) == (request.file_reference is not None):
        return FileSendResult(False, "Provide exactly one of media_path or file_reference")
    if request.file_reference is None:
        success, message = whatsapp.send_file(
            request.recipient, request.media_path or "", request.caption
        )
        return FileSendResult(success, message)
    return send_reference(request)


def send_reference(request: FileSendRequest) -> FileSendResult:
    # The origin comes from operator configuration, never from the reference.
    server = os.environ.get("FF_SERVER_URL", "https://ff.mazza.vc")
    outcome = None
    try:
        ref = Client(server, max_bytes=MAX_FILE_BYTES).validate_reference(
            request.file_reference
        )
        if ref.expires_at <= int(time.time()):
            return FileSendResult(False, "Attachment expired; upload it again")
        if ".db" in ref.filename.lower():
            return FileSendResult(False, "Database filenames cannot be sent")
        if request.caption and Path(ref.filename).suffix.lower() == ".ogg":
            return FileSendResult(False, "Voice messages do not support captions")
        with open_reference(
            ref, server_url=server, max_bytes=MAX_FILE_BYTES, timeout=RESOLVE_SECONDS
        ) as verified:
            outcome = send_staged_reference(request, verified)
        return outcome
    except Exception as error:
        # A cleanup error after submission must not be reported as "nothing sent".
        if outcome is not None:
            whatsapp.logger.warning("Attachment download cleanup failed")
            return outcome
        if isinstance(error, HTTPError) and error.code in (404, 410):
            return FileSendResult(False, "Attachment unavailable; upload it again")
        if isinstance(error, TimeoutError):
            return FileSendResult(False, "Attachment download timed out; nothing sent")
        if isinstance(error, ValueError):
            return FileSendResult(False, "Invalid attachment, size limit exceeded, or integrity check failed; nothing sent")
        # SDK exceptions can contain capability URLs. Never log/return their text.
        return FileSendResult(False, "Attachment preparation failed; nothing sent")


def send_staged_reference(request: FileSendRequest, verified: Path) -> FileSendResult:
    store = Path(whatsapp.MESSAGES_DB_PATH).resolve().parent
    # mkdtemp creates a private, unique directory in the existing media store.
    # The bridge retains its path and database guards; no arbitrary /tmp access.
    temporary = tempfile.TemporaryDirectory(prefix="ff-send-", dir=store)
    try:
        staged = Path(temporary.name) / request.file_reference.filename
        shutil.copyfile(verified, staged)
        success, message = whatsapp.send_file(
            request.recipient, str(staged), request.caption
        )
        return FileSendResult(success, message)
    finally:
        try:
            temporary.cleanup()
        except OSError:
            whatsapp.logger.warning("Attachment staging cleanup failed")
