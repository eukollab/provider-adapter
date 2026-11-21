# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""A2A over JSON-RPC as the A2A conformance kit checks it: errors for what isn't offered, input validated, finished
tasks read and listed but never continued or canceled, and a cacheable card."""

from typing import Any

import httpx2
import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from ekl_provider_adapter import a2a_server

pytestmark = pytest.mark.anyio
CARD = {"name": "Echo", "skills": []}


def _app() -> Starlette:
    tasks = a2a_server.Tasks(3)

    async def send(message: dict[str, Any]) -> a2a_server.Outcome:
        return a2a_server.Outcome("completed", data={"echo": message["parts"][0].get("text")})

    async def rpc(request: Request) -> Response:
        return await a2a_server.answer(request, request.headers.get("x-viewer", "ana"), tasks, send)

    async def card(request: Request) -> Response:
        return a2a_server.card_response(request, CARD)

    return Starlette(routes=[Route("/a2a", rpc, methods=["POST"]), Route("/card", card)])


def _client() -> httpx2.AsyncClient:
    return httpx2.AsyncClient(transport=httpx2.ASGITransport(app=_app()), base_url="http://127.0.0.1")


def _message(text: str = "hi", **extra: Any) -> dict[str, Any]:
    return {"messageId": f"m-{text}", "role": "ROLE_USER", "parts": [{"text": text}], **extra}


async def _call(http: httpx2.AsyncClient, method: str, params: Any = None, **headers: str) -> dict[str, Any]:
    body: dict[str, Any] = {"jsonrpc": "2.0", "id": 1, "method": method}
    if params is not None:
        body["params"] = params
    return dict((await http.post("/a2a", json=body, headers=headers)).json())


def _code(answer: dict[str, Any]) -> int:
    return int(answer["error"]["code"])


async def test_what_is_not_offered_gets_the_error_a2a_names() -> None:
    async with _client() as http:
        assert _code(await _call(http, "SendStreamingMessage", {"message": _message()})) == -32004
        assert _code(await _call(http, "SubscribeToTask", {"id": "x"})) == -32004
        assert _code(await _call(http, "CreateTaskPushNotificationConfig", {})) == -32003
        assert _code(await _call(http, "tasks/pushNotificationConfig/get", {})) == -32003
        assert _code(await _call(http, "GetExtendedAgentCard", {})) == -32004
        assert _code(await _call(http, "Nothing", {})) == -32601
        version = await _call(http, "SendMessage", {"message": _message()}, **{"A2A-Version": "99.0"})
        assert _code(version) == -32009
        assert "result" in await _call(http, "SendMessage", {"message": _message()}, **{"A2A-Version": "1.0"})
        parse = await http.post("/a2a", content=b"{nope", headers={"content-type": "application/json"})
        assert parse.json()["error"]["code"] == -32700


async def test_input_is_validated_before_anything_runs() -> None:
    async with _client() as http:
        assert _code(await _call(http, "SendMessage", {})) == -32602
        assert _code(await _call(http, "SendMessage", {"message": {"role": "ROLE_USER", "parts": []}})) == -32602
        file_part = _message(parts=[{"raw": "dGNr", "mediaType": "application/pdf"}])
        assert _code(await _call(http, "SendMessage", {"message": file_part})) == -32005
        odd_media = _message(parts=[{"text": "x", "mediaType": "application/x-tck"}])
        assert _code(await _call(http, "SendMessage", {"message": odd_media})) == -32005
        legacy_file = _message(parts=[{"kind": "file", "file": {"uri": "https://x.example/f"}}])
        assert _code(await _call(http, "message/send", {"message": legacy_file})) == -32005


async def test_a_finished_task_is_read_and_listed_never_continued_or_canceled() -> None:
    async with _client() as http:
        sent = (await _call(http, "SendMessage", {"message": _message("one", contextId="ctx-1")}))["result"]["task"]
        assert sent["status"]["state"] == "TASK_STATE_COMPLETED" and sent["status"]["timestamp"].endswith("Z")
        got = (await _call(http, "GetTask", {"id": sent["id"]}))["result"]
        assert got["id"] == sent["id"] and got["artifacts"][0]["parts"][0] == {"data": {"echo": "one"}}
        legacy = (await _call(http, "tasks/get", {"id": sent["id"]}))["result"]
        assert legacy["kind"] == "task" and legacy["status"]["state"] == "completed"
        assert _code(await _call(http, "CancelTask", {"id": sent["id"]})) == -32002
        assert _code(await _call(http, "CancelTask", {"id": "missing"})) == -32001
        again = {"message": _message("two", taskId=sent["id"])}
        assert _code(await _call(http, "SendMessage", again)) == -32004
        unknown = {"message": _message("three", taskId="missing")}
        assert _code(await _call(http, "SendMessage", unknown)) == -32001
        # Someone else's task is not found, as a missing one.
        assert _code(await _call(http, "GetTask", {"id": sent["id"]}, **{"x-viewer": "bo"})) == -32001


async def test_tasks_are_listed_newest_first_a_page_at_a_time() -> None:
    async with _client() as http:
        ids = []
        for n in range(4):  # one more than are kept
            sent = await _call(http, "SendMessage", {"message": _message(f"t{n}", contextId="ctx" if n else "other")})
            ids.append(sent["result"]["task"]["id"])
        await _call(http, "SendMessage", {"message": _message("bo")}, **{"x-viewer": "bo"})
        first = (await _call(http, "ListTasks", {"pageSize": 1}))["result"]
        assert first["totalSize"] == 2 and first["pageSize"] == 1 and first["nextPageToken"]
        assert [t["id"] for t in first["tasks"]] == [ids[3]] and "artifacts" not in first["tasks"][0]
        rest = (await _call(http, "ListTasks", {"pageSize": 1, "pageToken": first["nextPageToken"]}))["result"]
        assert [t["id"] for t in rest["tasks"]] == [ids[2]] and rest["nextPageToken"] == ""
        full = (await _call(http, "ListTasks", {"contextId": "ctx", "includeArtifacts": True}))["result"]
        assert len(full["tasks"]) == 2 and "artifacts" in full["tasks"][0]
        assert (await _call(http, "ListTasks", {"status": "TASK_STATE_FAILED"}))["result"]["tasks"] == []
        assert _code(await _call(http, "ListTasks", {"pageSize": 0})) == -32602
        assert _code(await _call(http, "ListTasks", {"pageToken": "bogus"})) == -32602
        assert _code(await _call(http, "ListTasks", {"statusTimestampAfter": "yesterday"})) == -32602


async def test_the_card_is_cacheable_and_revalidated() -> None:
    async with _client() as http:
        card = await http.get("/card")
        assert card.json() == CARD and "max-age" in card.headers["cache-control"]
        again = await http.get("/card", headers={"If-None-Match": card.headers["etag"]})
        assert again.status_code == 304
