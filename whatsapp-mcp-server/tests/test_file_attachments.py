"""Local-only transfers; WhatsApp submissions are always mocked."""

import asyncio
import hashlib
import json
import os
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from collections.abc import Iterator
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import Mock, patch

import requests
from byteforge_mcp_file_friend_models import FileReference

import file_attachments as attachments
import main
import whatsapp


class DownloadHandler(BaseHTTPRequestHandler):
    body = b"verified attachment bytes"
    status = 200
    seen_headers = []

    def do_GET(self) -> None:
        self.seen_headers.append(self.headers)
        self.send_response(self.status)
        self.send_header("Content-Length", str(len(self.body)))
        if self.status == 302:
            self.send_header("Location", "https://foreign.invalid/private")
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, format: str, *args: object) -> None:
        pass


class AttachmentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), DownloadHandler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def setUp(self) -> None:
        DownloadHandler.status = 200
        DownloadHandler.seen_headers = []
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.store = Path(self.temporary.name)
        origin = f"http://127.0.0.1:{self.server.server_port}"
        self.enterContext(patch.dict(os.environ, {"FF_SERVER_URL": origin, "FF_TOKEN": "never-send-this"}))
        self.enterContext(patch.object(whatsapp, "MESSAGES_DB_PATH", str(self.store / "messages.db")))
        self.post = self.enterContext(patch.object(whatsapp.requests, "post"))
        self.post.side_effect = self.capture_send
        now = int(time.time())
        self.reference = FileReference(
            id="a" * 32,
            url=f"{origin}/f/{'a' * 32}/report.pdf",
            filename="report.pdf",
            sha256=hashlib.sha256(DownloadHandler.body).hexdigest(),
            size=len(DownloadHandler.body), created_at=now, expires_at=now + 3600,
        )
        self.staged = None

    def capture_send(self, url: str, **kwargs: object) -> Mock:
        self.assertEqual(url, whatsapp.WHATSAPP_API_BASE_URL + "/send")
        payload = kwargs["json"]
        self.staged = Path(payload["media_path"])
        self.assertEqual(self.staged.read_bytes(), DownloadHandler.body)
        self.assertEqual(self.staged.name, self.reference.filename)
        self.assertEqual(self.staged.parent.parent, self.store)
        self.assertEqual(self.staged.parent.stat().st_mode & 0o777, 0o700)
        self.assertEqual(payload["message"], "A caption")
        self.assertEqual(kwargs["timeout"], (5, 180))
        return Mock(status_code=200, json=Mock(return_value={"success": True}))

    def send(self, ref: FileReference | None = None) -> attachments.FileSendResult:
        return attachments.send_attachment(attachments.FileSendRequest(
            "123456789", file_reference=ref or self.reference, caption="A caption"
        ))

    def test_verified_transfer_caption_cleanup_and_no_credentials(self) -> None:
        self.assertTrue(self.send().success)
        self.post.assert_called_once()
        self.assertFalse(self.staged.exists())
        self.assertEqual(list(self.store.iterdir()), [])
        headers = DownloadHandler.seen_headers[0]
        self.assertIsNone(headers.get("Authorization"))
        self.assertIn("byteforge-mcp-file-friend-api-python/0.0.2", headers["User-Agent"])

    def test_invalid_references_never_submit(self) -> None:
        cases = [
            replace(self.reference, sha256="0" * 64),
            replace(self.reference, size=self.reference.size + 1),
            replace(self.reference, size=attachments.MAX_FILE_BYTES + 1),
            replace(self.reference, created_at=0, expires_at=1),
            replace(self.reference, url="https://foreign.invalid/f/secret/report.pdf"),
            replace(self.reference, filename="../escape.pdf"),
            replace(self.reference, filename="session.db"),
        ]
        for ref in cases:
            with self.subTest(ref=ref.filename):
                result = self.send(ref)
                self.assertFalse(result.success)
                self.assertNotIn(ref.url, result.message)
        self.post.assert_not_called()
        self.assertEqual(list(self.store.iterdir()), [])

    def test_missing_and_redirected_downloads_never_submit(self) -> None:
        for status in (404, 410, 302):
            with self.subTest(status=status):
                DownloadHandler.status = status
                self.assertFalse(self.send().success)
        self.post.assert_not_called()

    def test_timeout_is_sanitized_before_submission(self) -> None:
        with patch.object(attachments, "open_reference", side_effect=TimeoutError(self.reference.url)):
            result = self.send()
        self.assertIn("nothing sent", result.message)
        self.assertNotIn(self.reference.url, result.message)
        self.post.assert_not_called()

    def test_uncertain_send_is_not_retried_and_staging_is_cleaned(self) -> None:
        self.post.side_effect = requests.Timeout(self.reference.url)
        result = self.send()
        self.assertFalse(result.success)
        self.assertIn("outcome unknown", result.message)
        self.assertNotIn(self.reference.url, result.message)
        self.post.assert_called_once()
        self.assertEqual(list(self.store.iterdir()), [])

    def test_bridge_errors_do_not_leak_response_content(self) -> None:
        self.post.side_effect = None
        self.post.return_value = Mock(status_code=500, text=self.reference.url)
        result = self.send()
        self.assertFalse(result.success)
        self.assertNotIn(self.reference.url, result.message)
        self.assertEqual(list(self.store.iterdir()), [])

    def test_cleanup_failure_preserves_successful_send_outcome(self) -> None:
        original = attachments.open_reference

        @contextmanager
        def failing_cleanup(*args: object, **kwargs: object) -> Iterator[Path]:
            with original(*args, **kwargs) as path:
                yield path
            raise OSError("cleanup failed")

        with patch.object(attachments, "open_reference", failing_cleanup):
            with self.assertLogs(whatsapp.logger, level="WARNING"):
                result = self.send()
        self.assertTrue(result.success)
        self.post.assert_called_once()

    def test_exclusive_source_required(self) -> None:
        for request in (
            attachments.FileSendRequest("123"),
            attachments.FileSendRequest("123", "local.pdf", self.reference),
            attachments.FileSendRequest("", file_reference=self.reference),
        ):
            self.assertFalse(attachments.send_attachment(request).success)
        self.post.assert_not_called()

    def test_existing_path_calls_remain_supported(self) -> None:
        path = self.store / "old.pdf"
        path.write_bytes(b"existing attachment")
        self.post.side_effect = None
        self.post.return_value = Mock(status_code=200, json=Mock(return_value={"success": True}))
        result = asyncio.run(main.send_file("123", str(path)))
        self.assertTrue(result["success"])
        self.assertTrue(path.exists())
        self.assertEqual(self.post.call_args.kwargs["json"]["message"], "")

    def test_voice_caption_rejected_before_download(self) -> None:
        ref = replace(self.reference, filename="voice.ogg", url=self.reference.url.replace("report.pdf", "voice.ogg"))
        self.assertIn("captions", self.send(ref).message)
        self.assertEqual(DownloadHandler.seen_headers, [])
        self.post.assert_not_called()

    def test_real_mcp_schema_and_reference_deserialization(self) -> None:
        tools = asyncio.run(main.mcp.list_tools())
        tool = next(tool for tool in tools if tool.name == "send_file")
        self.assertEqual(tool.inputSchema["required"], ["recipient"])
        schema = json.dumps(tool.inputSchema)
        for field in ("file_reference", "sha256", "expires_at", "caption"):
            self.assertIn(field, schema)
        result = asyncio.run(main.mcp.call_tool("send_file", {
            "recipient": "123", "file_reference": self.reference.to_dict(), "caption": "A caption"
        }))
        self.assertIn('"success": true', str(result))
        self.post.assert_called_once()

    def test_malformed_mcp_reference_does_not_expose_capability(self) -> None:
        with self.assertRaises(main.ToolError) as caught:
            asyncio.run(main.mcp.call_tool("send_file", {
                "recipient": "123", "file_reference": {"url": self.reference.url}
            }))
        self.assertEqual(str(caught.exception), "Invalid file-send arguments")
        self.assertNotIn(self.reference.url, str(caught.exception))
        self.post.assert_not_called()

    def test_unexpected_mcp_error_does_not_expose_capability(self) -> None:
        with patch.object(main, "send_attachment", side_effect=RuntimeError(self.reference.url)):
            with self.assertRaises(main.ToolError) as caught:
                asyncio.run(main.mcp.call_tool("send_file", {
                    "recipient": "123", "file_reference": self.reference.to_dict()
                }))
        self.assertIn("outcome unknown", str(caught.exception))
        self.assertNotIn(self.reference.url, str(caught.exception))

    def test_slow_attachment_does_not_block_mcp_event_loop(self) -> None:
        started = threading.Event()
        release = threading.Event()

        def slow_send(request: attachments.FileSendRequest) -> attachments.FileSendResult:
            started.set()
            release.wait(timeout=2)
            return attachments.FileSendResult(True, "sent")

        async def check() -> None:
            task = asyncio.create_task(main.mcp.call_tool("send_file", {
                "recipient": "123", "file_reference": self.reference.to_dict()
            }))
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 1))
                self.assertFalse(task.done(), "MCP event loop blocked until send completed")
            finally:
                release.set()
                await task

        with patch.object(main, "send_attachment", slow_send):
            asyncio.run(check())


if __name__ == "__main__":
    unittest.main()
