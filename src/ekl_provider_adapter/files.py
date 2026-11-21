# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Files travelling beside calls (EKP 3.5 §5.5).

A file is base64 in a call's arguments or result, as the publisher's server and the agent's own code see it. Between
agents a large one travels beside the call: the sending side puts its bytes in the network's file store, at an address
the network signs for that call and that receiver, and puts the file's reference, `ekl-file:sha256:<hex>`, where the
base64 was. The receiving side fetches the bytes, checks their SHA-256, and puts the base64 back. So a call stays small
whatever its files, both sides fingerprint the same call, and the relay never carries a file.

Large means a base64 string decoding to more than INLINE_BYTES; smaller ones stay in the call.
"""

import base64
import binascii
import hashlib
import json
import re
from collections.abc import Awaitable, Callable
from typing import Any

import httpx2
import mcp_types as types

INLINE_BYTES = 256 * 1024
MAX_FILE_BYTES = 100 * 1024 * 1024
PREFIX = "ekl-file:sha256:"
REFERENCE = re.compile(r"^ekl-file:sha256:([0-9a-f]{64})$")
_BASE64 = re.compile(r"^[A-Za-z0-9+/]+={0,2}$")
# The first bytes of the file types most services take, for the store's media type; anything else is octet-stream.
_MAGIC: list[tuple[bytes, str]] = [
    (b"\x89PNG\r\n\x1a\n", "image/png"),
    (b"\xff\xd8\xff", "image/jpeg"),
    (b"GIF8", "image/gif"),
    (b"%PDF-", "application/pdf"),
    (b"PK\x03\x04", "application/zip"),
    (b"ID3", "audio/mpeg"),
    (b"OggS", "audio/ogg"),
    (b"fLaC", "audio/flac"),
]

Upload = Callable[[str, int, str], Awaitable[dict[str, Any]]]
Download = Callable[[str], Awaitable[dict[str, Any]]]


class FileUnavailableError(Exception):
    """A file a call refers to couldn't be fetched, or isn't what its reference says."""


def media_type(data: bytes) -> str:
    for magic, kind in _MAGIC:
        if data.startswith(magic):
            return kind
    if data[4:8] == b"ftyp":
        return "video/mp4"
    if data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        return "image/webp"
    if data.startswith(b"RIFF") and data[8:12] == b"WAVE":
        return "audio/wav"
    return "application/octet-stream"


def large(value: str) -> bytes | None:
    """The bytes of a base64 string too large to travel inside a call, or None."""
    if len(value) <= INLINE_BYTES * 4 // 3 or not _BASE64.match(value):
        return None
    try:
        data = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        return None
    return data if len(data) > INLINE_BYTES else None


def references(value: Any) -> bool:
    """Whether a value holds a file reference anywhere."""
    if isinstance(value, str):
        return bool(REFERENCE.match(value))
    if isinstance(value, dict):
        return any(references(v) for v in value.values())
    if isinstance(value, list):
        return any(references(v) for v in value)
    return False


async def _each(value: Any, change: Callable[[str], Awaitable[str]]) -> Any:
    if isinstance(value, str):
        return await change(value)
    if isinstance(value, dict):
        return {k: await _each(v, change) for k, v in value.items()}
    if isinstance(value, list):
        return [await _each(v, change) for v in value]
    return value


class FileTransfer:
    """Moves one call's files: `upload` asks the network for an upload (sha256, size, media type) and answers
    {ref, upload_url}; `download` asks for a download (sha256) and answers {url, size}."""

    def __init__(self, upload: Upload, download: Download, http: httpx2.AsyncClient | None = None) -> None:
        self.upload = upload
        self.download = download
        self.http = http

    async def _client(self) -> httpx2.AsyncClient:
        return self.http or httpx2.AsyncClient(timeout=600, follow_redirects=False)

    async def send(self, value: Any) -> Any:
        """The value with each large file put in the store and replaced by its reference."""

        async def change(text: str) -> str:
            data = large(text)
            if data is None:
                return text
            if len(data) > MAX_FILE_BYTES:
                raise ValueError(f"a file is at most {MAX_FILE_BYTES // (1024 * 1024)} MB")
            sha256 = hashlib.sha256(data).hexdigest()
            slot = await self.upload(sha256, len(data), media_type(data))
            http = await self._client()
            try:
                answer = await http.put(str(slot["upload_url"]), content=data)
            finally:
                if self.http is None:
                    await http.aclose()
            if answer.status_code >= 300:
                raise FileUnavailableError(f"the file store answered {answer.status_code} to an upload")
            return str(slot.get("ref") or PREFIX + sha256)

        return await _each(value, change)

    async def receive(self, value: Any) -> Any:
        """The value with each file reference replaced by the file's bytes in base64, each checked against its
        SHA-256."""

        async def change(text: str) -> str:
            found = REFERENCE.match(text)
            if found is None:
                return text
            sha256 = found.group(1)
            try:
                slot = await self.download(sha256)
                http = await self._client()
                try:
                    answer = await http.get(str(slot["url"]))
                finally:
                    if self.http is None:
                        await http.aclose()
            except (httpx2.HTTPError, KeyError, LookupError) as exc:
                raise FileUnavailableError(f"the file {sha256[:12]}… couldn't be fetched: {exc}") from exc
            if answer.status_code >= 300 or hashlib.sha256(answer.content).hexdigest() != sha256:
                raise FileUnavailableError(f"the file {sha256[:12]}… couldn't be fetched, or isn't what the call says")
            return base64.b64encode(answer.content).decode()

        return await _each(value, change)

    async def send_result(self, result: types.CallToolResult) -> types.CallToolResult:
        """A result with its large files put in the store: in its structured content, and in a text block that is
        that content as JSON, as servers often send it twice."""
        if not result.structured_content or result.is_error:
            return result
        structured = await self.send(result.structured_content)
        if structured == result.structured_content:
            return result
        blocks: list[Any] = []
        for block in result.content:
            mirrored = isinstance(block, types.TextContent) and _same_json(block.text, result.structured_content)
            blocks.append(
                types.TextContent(type="text", text=json.dumps(structured, ensure_ascii=False)) if mirrored else block
            )
        return result.model_copy(update={"structured_content": structured, "content": blocks})


def _same_json(text: str, value: Any) -> bool:
    if len(text) < INLINE_BYTES:
        return False
    try:
        return bool(json.loads(text) == value)
    except ValueError:
        return False
