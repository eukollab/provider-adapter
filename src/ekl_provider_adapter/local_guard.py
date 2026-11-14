# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The connector's local API answers only the publisher's own programs on this machine.

It acts with the agent credential, so a web page the publisher happens to open must never reach it. Browsers send
`Origin` on cross-site requests and can't send a JSON body cross-site without a preflight, which is refused; a page
that rebinds its own name to 127.0.0.1 arrives with a foreign Host. Every request therefore needs a loopback Host,
no Origin, a JSON body of at most 1 MB and, once set, the local token (EKL_LOCAL_TOKEN, which `ekl agent run` and
`ekl agent deploy` write to .env). No CORS header is ever sent.

A connector set up with EKL_LOCAL_TOKEN requires it from the start. One set up before it existed keeps answering
without it until LOCAL_TOKEN_REQUIRED_FROM, then refuses everything until the token is set.

Once listening, the connector writes `.ekl/local-api.json` ({url, pid, started_at}, 0600), so an app finds it on
whatever port it took when 8765 was busy.
"""

import asyncio
import datetime as dt
import hmac
import json
import logging
import os
import re
from collections.abc import Awaitable, Callable, MutableMapping
from pathlib import Path
from typing import Any

from ekl_provider_adapter.private_files import write_private_json

log = logging.getLogger("ekl_provider_adapter.local_api")

MAX_BODY = 1_048_576
TOKEN_VARIABLE = "EKL_LOCAL_TOKEN"
# 60 days after the release that introduced the token: existing installations need it from this day.
LOCAL_TOKEN_REQUIRED_FROM = dt.date(2027, 1, 12)
LOCAL_API_FILE = Path(".ekl") / "local-api.json"
_LOOPBACK_HOST = re.compile(r"^(127\.0\.0\.1|localhost|\[::1\])(?::(\d{1,5}))?$")

Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]


def required_from() -> dt.date:
    given = os.environ.get("EKL_LOCAL_TOKEN_REQUIRED_FROM")
    return dt.date.fromisoformat(given) if given else LOCAL_TOKEN_REQUIRED_FROM


async def _answer(send: Send, status: int, error: str, message: str) -> None:
    body = json.dumps({"error": error, "message": message}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())],
        }
    )
    await send({"type": "http.response.body", "body": body})


class LocalGuard:
    """ASGI middleware in front of the local API."""

    def __init__(self, app: ASGIApp, token: str | None = None, *, today: Callable[[], dt.date] = dt.date.today):
        self.app = app
        self.token = token if token is not None else os.environ.get(TOKEN_VARIABLE, "")
        self._today = today
        self._warned = False

    def _token_problem(self, headers: dict[bytes, bytes]) -> str | None:
        if not self.token:
            if self._today() >= required_from():
                return (
                    f"the local API needs {TOKEN_VARIABLE} from {required_from()}: run ekl agent run once to write it"
                )
            if not self._warned:
                log.warning(
                    "the local API answers without a token until %s; run ekl agent run once to write %s to .env",
                    required_from(),
                    TOKEN_VARIABLE,
                )
                self._warned = True
            return None
        scheme, _, presented = headers.get(b"authorization", b"").decode("latin-1").partition(" ")
        if scheme.lower() != "bearer" or not hmac.compare_digest(presented.strip().encode(), self.token.encode()):
            return f"send Authorization: Bearer ${TOKEN_VARIABLE}, as the connector's .env holds it"
        return None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:  # noqa: PLR0911 - one exit per check
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = {k.lower(): v for k, v in scope.get("headers") or []}
        host = _LOOPBACK_HOST.match(headers.get(b"host", b"").decode("latin-1"))
        server_port = (scope.get("server") or (None, None))[1]
        if host is None or (host.group(2) and server_port is not None and int(host.group(2)) != int(server_port)):
            await _answer(send, 421, "MISDIRECTED", "the local API answers only at 127.0.0.1, localhost or [::1]")
            return
        if scope["method"] == "OPTIONS":
            await _answer(send, 405, "METHOD_NOT_ALLOWED", "the local API answers no preflight")
            return
        if b"origin" in headers:
            await _answer(send, 403, "FORBIDDEN", "the local API refuses requests from web pages")
            return
        if scope["method"] not in ("GET", "HEAD"):
            kind = headers.get(b"content-type", b"").decode("latin-1").split(";")[0].strip().lower()
            if kind != "application/json":
                await _answer(send, 415, "UNSUPPORTED_MEDIA_TYPE", "send Content-Type: application/json")
                return
        problem = self._token_problem(headers)
        if problem:
            await _answer(send, 401, "UNAUTHORIZED", problem)
            return
        length = headers.get(b"content-length")
        if length is not None and length.isdigit() and int(length) > MAX_BODY:
            await _answer(send, 413, "TOO_LARGE", "a request body is at most 1 MB")
            return
        # The body is read here, bounded, and replayed: a streamed body can't exceed the limit unseen.
        chunks: list[bytes] = []
        size = 0
        more = True
        while more:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            chunk = message.get("body", b"")
            size += len(chunk)
            if size > MAX_BODY:
                await _answer(send, 413, "TOO_LARGE", "a request body is at most 1 MB")
                return
            chunks.append(chunk)
            more = bool(message.get("more_body"))
        body = b"".join(chunks)
        sent = False

        async def replay() -> Message:
            nonlocal sent
            if not sent:
                sent = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        await self.app(scope, replay, send)


def local_api_file(agent_file: str | None = None) -> Path:
    """.ekl/local-api.json beside agent.json (EKL_AGENT_FILE, else the current folder)."""
    named = agent_file or os.environ.get("EKL_AGENT_FILE")
    folder = Path(named).resolve().parent if named else Path.cwd()
    return folder / LOCAL_API_FILE


async def announce_when_listening(server: Any, url: str, agent_file: str | None = None) -> Path | None:
    """Write .ekl/local-api.json once the uvicorn server listens, and remove it when it stops."""
    while not getattr(server, "started", False):
        if getattr(server, "should_exit", False):
            return None
        await asyncio.sleep(0.05)
    path = local_api_file(agent_file)
    try:
        write_private_json(path, {"url": url, "pid": os.getpid(), "started_at": dt.datetime.now(dt.UTC).isoformat()})
    except OSError as exc:  # a read-only folder (some containers): the app then needs EKL_AGENT_API_URL
        log.warning("could not write %s (%s); give the app EKL_AGENT_API_URL=%s", path, exc, url)
        return None
    return path
