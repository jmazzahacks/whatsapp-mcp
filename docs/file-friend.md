# Sending desktop attachments

The desktop uploads the file to File Friend, then passes the complete returned
reference object to WhatsApp MCP. WhatsApp sends the verified file as an attachment;
it does not send a download link. Desktop filesystem paths are not accessible to a
remote MCP server.

Install the File Friend CLI on the desktop using Python 3.13+ on Unix. This project
pins client 0.0.2 at `c905f2c1d3f12ef466f204b31e3ceb53a2d3cf53`, including the download
User-Agent fix. Supply the uploader's `FF_TOKEN` through your secret mechanism.

```bash
export FF_SERVER_URL=https://ff.mazza.vc
umask 077
ff upload ./report.pdf --ttl 24h > report.reference.json
```

After the upload exits successfully, parse that JSON file and pass the object as
`file_reference` to `send_file`, alongside the recipient and optional `caption`.
Do not pass the JSON file's pathname, a bare URL, or base64 bytes. The object includes
`id`, `url`, `filename`, `sha256`, `size`, `created_at` and `expires_at`; forward it
unchanged. For example, in a client that already has the parsed reference:

```python
arguments = {
    "recipient": "123456789@s.whatsapp.net",
    "file_reference": reference,
    "caption": "Here is the report.",
}
# Invoke the WhatsApp MCP send_file tool with arguments.
```

Existing `send_file(recipient, media_path)` calls still work. Supply exactly one of
`media_path` and `file_reference`. The existing `success`/`message` result shape is
unchanged. Captions are supported for images, videos and documents; `.ogg` files
follow the existing voice-message behavior and reject nonempty captions.

The MCP service uses operator-configured `FF_SERVER_URL`, defaulting to
`https://ff.mazza.vc`. It needs no File Friend upload token, shared mount, database
access or new MCP server. Never derive this origin from a submitted reference.
The runtime and local virtual environment now require Python 3.13; run `uv sync`
inside `whatsapp-mcp-server` after updating.

References are checked for expiry, origin, identity, size and SHA-256. Downloads
are limited to **16 MiB** and **120 seconds**. This is a conservative application
limit, not a promise that WhatsApp accepts every file format. Verified files are
copied into a private temporary directory under the existing bridge media store,
retaining the filename, and removed after the operation. Existing bridge path,
symlink and database-file restrictions remain in force. Filenames containing
`.db` are refused. Source uploads remain available until their TTL or producer
deletion; sending does not delete them.

The bridge bounds upload/send work to 120 seconds; the Python request uses a
5-second connection timeout and 180-second read timeout. Download failures send
nothing. Once submission starts, connection errors can leave the outcome unknown:
check the chat before retrying. There is no automatic send retry or exactly-once
guarantee. Capability URLs and download exception details are not returned in
attachment errors. Send logging omits message content, filenames and upload keys.

Local verification (no real WhatsApp delivery or production File Friend requests):

```bash
cd whatsapp-mcp-server
source .venv/bin/activate
python -m unittest discover -s tests -v
```

Tests exercise the installed SDK against a loopback HTTP fixture and mock bridge
submissions. A real upload/send pilot requires separately approved production
testing and an agreed WhatsApp recipient.
