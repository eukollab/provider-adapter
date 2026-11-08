# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Relay mode: calls read from the network's stream are served by the connector's app and answered by POST."""

import asyncio
import base64
import json
from typing import Any

import httpx2
import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from ekl_provider_adapter import relay as relay_module
from ekl_provider_adapter.relay import Relay, relay_address

pytestmark = pytest.mark.anyio
NETWORK = "http://network.test"
PATH = "/v1/relay/agt_1/mcp"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _network(calls: list[dict[str, Any]], answers: list[dict[str, Any]], answered: asyncio.Event) -> Starlette:
    """The network's side of the relay: one stream carrying the given calls, then a second copy's refusal."""
    streams = 0

    async def relay(request: Request) -> Response:
        assert request.headers["authorization"] == "Bearer eklp_key"
        return JSONResponse({"endpoint": f"{NETWORK}{PATH}"})

    async def stream(request: Request) -> Response:
        nonlocal streams
        streams += 1
        if streams > 1:
            return JSONResponse({"code": "LEASE_HELD"}, status_code=409)

        async def frames() -> Any:
            yield "retry: 2000\n\nevent: ping\ndata: {}\n\n"
            for call in calls:
                yield f"event: call\ndata: {json.dumps(call)}\n\n"

        return StreamingResponse(frames(), media_type="text/event-stream")

    async def answer(request: Request) -> Response:
        answers.append(await request.json())
        if len(answers) == len(calls):
            answered.set()
        return JSONResponse({"delivered": True})

    return Starlette(
        routes=[
            Route("/v1/provider/relay", relay, methods=["POST"]),
            Route("/v1/provider/stream", stream, methods=["GET"]),
            Route("/v1/provider/calls/answer", answer, methods=["POST"]),
        ]
    )


async def _echo(request: Request) -> Response:
    """Stands in for the connector's app: answers with what it was sent."""
    body = await request.json()
    return JSONResponse({"jsonrpc": "2.0", "id": body["id"], "result": {"path": request.url.path}})


def _call(relay_id: str, request_id: int) -> dict[str, Any]:
    body = {"jsonrpc": "2.0", "id": request_id, "method": "tools/call", "params": {"name": "extract"}}
    return {
        "id": relay_id,
        "method": "POST",
        "path": PATH,
        "headers": {"content-type": "application/json", "authorization": "Bearer access"},
        "body": base64.b64encode(json.dumps(body).encode()).decode(),
    }


async def test_relayed_calls_are_served_by_the_app_and_answered() -> None:
    calls, answers, answered = [_call("r1", 1), _call("r2", 2)], list[dict[str, Any]](), asyncio.Event()
    network = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=_network(calls, answers, answered)))
    app = Starlette(routes=[Route(PATH, _echo, methods=["POST"])])
    running = asyncio.create_task(Relay(app, NETWORK, "eklp_key", http=network).run())
    await asyncio.wait_for(answered.wait(), 5)
    running.cancel()
    assert sorted(a["id"] for a in answers) == ["r1", "r2"]
    first = next(a for a in answers if a["id"] == "r1")
    assert first["status"] == 200 and first["headers"]["content-type"] == "application/json"
    assert json.loads(base64.b64decode(first["body"])) == {"jsonrpc": "2.0", "id": 1, "result": {"path": PATH}}


async def test_a_failing_app_answers_an_error_rather_than_leaving_the_caller_waiting() -> None:
    answers, answered = list[dict[str, Any]](), asyncio.Event()
    network = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=_network([_call("r1", 1)], answers, answered)))

    async def broken(_: Request) -> Response:
        raise RuntimeError("boom")

    app = Starlette(routes=[Route(PATH, broken, methods=["POST"])])
    running = asyncio.create_task(Relay(app, NETWORK, "eklp_key", http=network).run())
    await asyncio.wait_for(answered.wait(), 5)
    running.cancel()
    assert answers[0]["status"] == 500


async def test_the_relay_address_comes_from_the_agent_key() -> None:
    network = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=_network([], [], asyncio.Event())))
    assert await relay_address(NETWORK, "eklp_key", network) == f"{NETWORK}{PATH}"


async def test_a_stream_held_back_on_the_way_gives_way_to_the_long_poll(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(relay_module, "BACKOFF_SECONDS", (0, 0, 0, 0, 0))
    answers, answered, polled = list[dict[str, Any]](), asyncio.Event(), list[int]()

    async def calls_next(request: Request) -> Response:
        polled.append(1)
        await asyncio.sleep(0.01)  # the network's poll waits for a call
        return JSONResponse({"calls": [_call("r1", 1)] if len(polled) == 1 else []})

    async def answer(request: Request) -> Response:
        answers.append(await request.json())
        answered.set()
        return JSONResponse({"delivered": True})

    class Silent(httpx2.AsyncBaseTransport):
        """A proxy that never lets the stream through; everything else reaches the network."""

        def __init__(self, network: httpx2.AsyncBaseTransport) -> None:
            self._network = network

        async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
            if request.url.path == "/v1/provider/stream":
                raise httpx2.ReadTimeout("no ping came through", request=request)
            return await self._network.handle_async_request(request)

    network_app = Starlette(
        routes=[
            Route("/v1/provider/calls/next", calls_next, methods=["POST"]),
            Route("/v1/provider/calls/answer", answer, methods=["POST"]),
        ]
    )
    network = httpx2.AsyncClient(transport=Silent(httpx2.ASGITransport(app=network_app)))
    app = Starlette(routes=[Route(PATH, _echo, methods=["POST"])])
    running = asyncio.create_task(Relay(app, NETWORK, "eklp_key", http=network).run())
    await asyncio.wait_for(answered.wait(), 5)
    running.cancel()
    assert answers[0]["id"] == "r1" and answers[0]["status"] == 200


async def test_a_call_relayed_with_the_networks_path_reaches_the_apps_own_endpoint() -> None:
    """The network forwards /v1/relay/<agent>/mcp; the connector serves its endpoint at its own path (/ekl)."""
    calls, answers, answered = [_call("r1", 1)], list[dict[str, Any]](), asyncio.Event()
    network = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=_network(calls, answers, answered)))
    app = Starlette(routes=[Route("/ekl", _echo, methods=["POST"])])
    running = asyncio.create_task(Relay(app, NETWORK, "eklp_key", path="/ekl", http=network).run())
    await asyncio.wait_for(answered.wait(), 5)
    running.cancel()
    assert answers[0]["status"] == 200
    assert json.loads(base64.b64decode(answers[0]["body"]))["result"] == {"path": "/ekl"}


async def test_the_runner_says_it_is_alive_and_reconnects_once_its_lease_is_lost() -> None:
    """The network keeps the lease only while it hears from the runner. A runner that went silent (the machine slept)
    is told so when it speaks again, and drops its old stream to take the lease again at once."""
    streams, alive, reconnected = list[str](), list[dict[str, Any]](), asyncio.Event()

    async def stream(request: Request) -> Response:
        streams.append(request.query_params["holder"])
        if len(streams) > 1:
            reconnected.set()

        async def frames() -> Any:
            yield "event: ping\ndata: {}\n\n"
            await asyncio.sleep(30)  # a stream that stays open: the old connection of a machine that slept

        return StreamingResponse(frames(), media_type="text/event-stream")

    async def said_alive(request: Request) -> Response:
        alive.append(await request.json())
        if len(alive) == 2:
            return JSONResponse({"code": "LEASE_LOST"}, status_code=409)
        return JSONResponse({"lease_seconds": 30, "alive_seconds": 10})

    network_app = Starlette(
        routes=[
            Route("/v1/provider/stream", stream, methods=["GET"]),
            Route("/v1/provider/relay/alive", said_alive, methods=["POST"]),
        ]
    )
    network = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=network_app))
    runner = Relay(Starlette(), NETWORK, "eklp_key", http=network)
    runner._alive_every = 0.05
    running = asyncio.create_task(runner.run())
    await asyncio.wait_for(reconnected.wait(), 5)
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    assert alive[0] == {"holder": runner.holder} and streams == [runner.holder, runner.holder]
