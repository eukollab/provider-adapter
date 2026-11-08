# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Relay mode: the connector with no public address, for `ekl agent run`.

The network gives the agent a relay address and announces it as the endpoint. Calls to that address come down a
stream of server-sent events this process keeps open, each one a whole HTTP request, which is served by the
connector's own app exactly as if it had arrived over HTTP, and the answer goes back with one POST. Nothing listens
here but the loopback API, so the machine needs only to reach the network: a laptop behind any router will do.
"""

import asyncio
import base64
import json
import logging
import os
import secrets
import time
from collections.abc import AsyncIterator
from typing import Any

import httpx2
from starlette.types import ASGIApp

log = logging.getLogger(__name__)
# Waits before reconnecting after a failure, longest last; a copy refused the lease tries again after the last.
BACKOFF_SECONDS = (1, 2, 5, 10, 30)
# A stream that fails this many times in a row, a ping due every 15 seconds never arriving, gives way to the long poll.
STREAM_TRIES = 3
POLL_SECONDS = 25
# The network keeps this runner's lease only while it hears it is alive: every 10 seconds, against a 30-second lease.
ALIVE_SECONDS = 10


async def relay_address(network_url: str, credential: str, http: httpx2.AsyncClient | None = None) -> str:
    """The relay address the network keeps for this agent key."""
    client = http or httpx2.AsyncClient(timeout=30)
    try:
        response = await client.post(f"{network_url.rstrip('/')}/v1/provider/relay", headers=_auth(credential))
    finally:
        if http is None:
            await client.aclose()
    if response.status_code != 200:
        raise RuntimeError(f"the network refused the agent key ({response.status_code}): {response.text[:200]}")
    return str(response.json()["endpoint"])


def relay_address_now(network_url: str, credential: str) -> str:
    """relay_address, for an app built before any event loop runs (attach is called at import)."""
    response = httpx2.post(f"{network_url.rstrip('/')}/v1/provider/relay", headers=_auth(credential), timeout=30)
    if response.status_code != 200:
        raise RuntimeError(f"the network refused the agent key ({response.status_code}): {response.text[:200]}")
    return str(response.json()["endpoint"])


def relayed() -> bool:
    """Whether this process serves the agent through the network's relay (EKL_RELAY=1, which `ekl agent run` sets)."""
    return os.environ.get("EKL_RELAY") == "1"


def _auth(credential: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {credential}"}


async def _events(response: httpx2.Response) -> AsyncIterator[tuple[str, str]]:
    """(event, data) for each server-sent event of the response."""
    name, data = "message", list[str]()
    async for line in response.aiter_lines():
        if not line:
            if data:
                yield name, "\n".join(data)
            name, data = "message", list[str]()
        elif line.startswith("event:"):
            name = line[6:].strip()
        elif line.startswith("data:"):
            data.append(line[5:].lstrip())


def _described(frame: dict[str, Any], body: bytes) -> str:
    """What a call asked, for the log: the MCP method and, for tools/call, the tool."""
    try:
        request = json.loads(body)
    except ValueError:
        return str(frame.get("method", "?"))
    method = str(request.get("method", "?")) if isinstance(request, dict) else "?"
    if method == "tools/call":
        return f"tools/call {request.get('params', {}).get('name', '?')}"
    return method


class Relay:
    """Reads the agent's calls from the network and serves them with `app`, the connector's own app."""

    def __init__(
        self,
        app: ASGIApp,
        network_url: str,
        credential: str,
        *,
        path: str | None = None,
        http: httpx2.AsyncClient | None = None,
    ) -> None:
        self._network = network_url.rstrip("/")
        self._credential = credential
        # Where the app serves the adapter's endpoint: a call arrives with the network's relay path, which the app
        # does not serve, so it is handed to this path instead. None keeps the frame's path.
        self._path = path
        # A ping comes every 15 seconds: a stream silent for 45 is broken, or held back by a proxy on the way.
        self._http = http or httpx2.AsyncClient(timeout=httpx2.Timeout(30, read=45))
        self._local = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app), base_url="http://relay")
        # One name for this process, so its reconnections keep the lease and a second copy waits.
        self.holder = secrets.token_hex(8)
        self._tasks: set[asyncio.Task[None]] = set()
        self._alive_every = ALIVE_SECONDS
        # The stream this runner holds now: its lease lost (a laptop that slept), it is dropped and taken again.
        self._holding: asyncio.Task[bool] | None = None

    async def run(self) -> None:
        """Serve calls until cancelled, reconnecting whenever the stream ends."""
        failures = silent = 0
        alive = asyncio.create_task(self._alive())
        try:
            while True:
                try:
                    if silent >= STREAM_TRIES:
                        await self._poll()
                        failures = 0
                        continue
                    self._holding = asyncio.create_task(self._stream())
                    try:
                        held = await self._holding
                    except asyncio.CancelledError:
                        current = asyncio.current_task()
                        if current is not None and current.cancelling():
                            raise
                        held = True  # dropped for a lost lease, not stopped: connect again at once
                    finally:
                        self._holding = None
                    silent = 0
                    failures = 0 if held else len(BACKOFF_SECONDS) - 1
                except httpx2.ReadTimeout:
                    # Connected, but no ping came through: something on the way holds the stream back.
                    silent += 1
                    if silent == STREAM_TRIES:
                        log.warning("the network's stream can't be held open from here; polling for calls instead")
                except httpx2.HTTPError as exc:
                    # A deployment of the network ends every stream; reconnecting is routine, not a fault.
                    log.warning("the connection to the network dropped (%s); connecting again", type(exc).__name__)
                    failures = min(failures + 1, len(BACKOFF_SECONDS) - 1)
                if failures:
                    await asyncio.sleep(BACKOFF_SECONDS[failures])
        finally:
            alive.cancel()
            await self._local.aclose()

    async def _alive(self) -> None:
        """Tell the network this runner is alive, so it keeps the agent's lease; a dead machine stops telling it."""
        url = f"{self._network}/v1/provider/relay/alive"
        while True:
            await asyncio.sleep(self._alive_every)
            try:
                response = await self._http.post(url, json={"holder": self.holder}, headers=_auth(self._credential))
            except httpx2.HTTPError as exc:
                log.debug("could not say alive (%s)", type(exc).__name__)
                continue
            if response.status_code == 409 and self._holding is not None and not self._holding.done():
                # Silent too long (the machine slept, the network dropped): the lease lapsed, so this stream gets no
                # calls any more. Drop it and take the lease again.
                log.info("the network stopped hearing from this runner for a while; connecting again")
                self._holding.cancel()

    async def _poll(self) -> None:
        """The long poll, where a stream cannot be held open (a proxy that buffers it): one call per poll."""
        response = await self._http.post(
            f"{self._network}/v1/provider/calls/next",
            json={"holder": self.holder, "wait_seconds": POLL_SECONDS},
            headers=_auth(self._credential),
        )
        if response.status_code == 409:
            await asyncio.sleep(BACKOFF_SECONDS[-1])
            return
        response.raise_for_status()
        for frame in response.json()["calls"]:
            self._start(frame)

    def _start(self, frame: dict[str, Any]) -> None:
        task = asyncio.create_task(self._serve(frame))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _stream(self) -> bool:
        """One stream, until the network ends it. False when another copy holds the lease."""
        url = f"{self._network}/v1/provider/stream"
        params = {"holder": self.holder}
        async with self._http.stream("GET", url, params=params, headers=_auth(self._credential)) as response:
            if response.status_code == 409:
                log.info("another copy of this agent is serving its calls; waiting to take over")
                return False
            if response.status_code != 200:
                body = (await response.aread()).decode(errors="replace")[:200]
                raise httpx2.HTTPStatusError(
                    f"{response.status_code}: {body}", request=response.request, response=response
                )
            async for event, data in _events(response):
                if event == "call":
                    self._start(json.loads(data))
        return True

    async def _serve(self, frame: dict[str, Any]) -> None:
        body = base64.b64decode(frame.get("body", ""))
        started = time.monotonic()
        try:
            response = await self._local.request(
                frame["method"], self._path or frame["path"], headers=frame.get("headers", {}), content=body
            )
            status, headers, content = response.status_code, dict(response.headers), response.content
        except Exception:  # the app failed outright: the caller gets an error, not a silent timeout
            log.exception("serving a relayed call failed")
            status, headers, content = 500, {"content-type": "text/plain"}, b"the agent failed to serve the call"
        said = _described(frame, body)
        # The calls a publisher cares about; a client's handshake around each one stays out of the way.
        level = logging.INFO if said.startswith("tools/") or status >= 500 else logging.DEBUG
        log.log(level, "%s → %s (%.2f s)", said, status, time.monotonic() - started)
        answer = {
            "id": frame["id"],
            "status": status,
            "headers": {k: v for k, v in headers.items() if k.lower() in ("content-type", "mcp-session-id")},
            "body": base64.b64encode(content).decode("ascii"),
        }
        try:
            await self._http.post(
                f"{self._network}/v1/provider/calls/answer", json=answer, headers=_auth(self._credential)
            )
        except httpx2.HTTPError as exc:
            # The caller times out; a retry with the same execution ID finds what the journal kept (C.9.4).
            log.warning("could not return an answer to the network (%s)", type(exc).__name__)
