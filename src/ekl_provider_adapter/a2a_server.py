# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""A2A over JSON-RPC, 1.0 and 0.3, for an agent whose every task ends before its answer is sent.

Each call is one blocking message, so a task is finished by the time anyone sees it: it can be read and listed, never
continued or canceled. Streaming, push notifications and an extended card aren't offered, and each is answered with
the error A2A gives for it, as are an unknown version, a malformed request and a part of a type the agent doesn't
take. The tasks are kept in memory, a bounded number, each shown only to whom it ran for.
"""

import base64
import hashlib
import json
import re
import uuid
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse, Response

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
TASK_NOT_FOUND = -32001
TASK_NOT_CANCELABLE = -32002
PUSH_NOT_SUPPORTED = -32003
UNSUPPORTED_OPERATION = -32004
CONTENT_TYPE_NOT_SUPPORTED = -32005
VERSION_NOT_SUPPORTED = -32009

# An empty or absent A2A-Version means 0.3.
_VERSION = re.compile(r"^(0\.3|1(\.0)?)(\.\d+)?$")
MEDIA_TYPES = ("application/json", "text/plain")
_SEND = {"SendMessage", "message/send"}
_GET = {"GetTask", "tasks/get"}
_CANCEL = {"CancelTask", "tasks/cancel"}
_LIST = {"ListTasks"}
_STREAMING = {"SendStreamingMessage", "SubscribeToTask", "message/stream", "tasks/resubscribe"}
_PUSH = {
    "CreateTaskPushNotificationConfig",
    "GetTaskPushNotificationConfig",
    "ListTaskPushNotificationConfigs",
    "DeleteTaskPushNotificationConfig",
    "tasks/pushNotificationConfig/set",
    "tasks/pushNotificationConfig/get",
    "tasks/pushNotificationConfig/list",
    "tasks/pushNotificationConfig/delete",
}
_EXTENDED_CARD = {"GetExtendedAgentCard", "agent/getAuthenticatedExtendedCard"}
PAGE_SIZE, MAX_PAGE_SIZE = 50, 100


class RpcError(Exception):
    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code, self.message = code, message


@dataclass(frozen=True)
class Outcome:
    """How a message's task ended: its state (completed, failed, rejected), the result, or what the agent said."""

    state: str
    data: Any = None
    said: str = ""


@dataclass(frozen=True)
class Kept:
    viewer: str
    id: str
    context: str
    outcome: Outcome
    at: datetime


Send = Callable[[dict[str, Any]], Awaitable[Outcome]]


class Tasks:
    """The tasks an endpoint answered, the newest last, at most `limit` of them."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.kept: OrderedDict[str, Kept] = OrderedDict()

    def add(self, kept: Kept) -> None:
        self.kept[kept.id] = kept
        while len(self.kept) > self.limit:
            self.kept.popitem(last=False)

    def get(self, viewer: str, task_id: Any) -> Kept:
        found = self.kept.get(str(task_id)) if isinstance(task_id, str) else None
        # A task someone else ran is not found, never forbidden: its existence is theirs too.
        if found is None or found.viewer != viewer:
            raise RpcError(TASK_NOT_FOUND, "task not found")
        return found

    def visible(self, viewer: str) -> list[Kept]:
        return sorted((k for k in self.kept.values() if k.viewer == viewer), key=lambda k: k.at, reverse=True)


async def answer(request: Request, viewer: str, tasks: Tasks, send: Send) -> Response:
    """One JSON-RPC request answered for `viewer`, the person or program the endpoint's tasks are kept for."""
    call_id: Any = None
    try:
        try:
            body = json.loads(await request.body())
        except ValueError as exc:
            raise RpcError(PARSE_ERROR, "the body is not JSON") from exc
        if not isinstance(body, dict):
            raise RpcError(INVALID_REQUEST, "send one JSON-RPC request object")
        call_id = body.get("id") if isinstance(body.get("id"), str | int) else None
        method = body.get("method")
        if body.get("jsonrpc") != "2.0" or not isinstance(method, str) or not method:
            raise RpcError(INVALID_REQUEST, "a JSON-RPC 2.0 request names its method")
        version = request.headers.get("a2a-version", "").strip()
        if version and not _VERSION.match(version):
            raise RpcError(VERSION_NOT_SUPPORTED, f"A2A {version} isn't supported: this agent speaks 1.0 and 0.3")
        params = body.get("params", {})
        if not isinstance(params, dict):
            raise RpcError(INVALID_PARAMS, "params must be an object")
        result = await _method(method, params, viewer, tasks, send)
    except RpcError as exc:
        return JSONResponse({"jsonrpc": "2.0", "id": call_id, "error": {"code": exc.code, "message": exc.message}})
    return JSONResponse({"jsonrpc": "2.0", "id": call_id, "result": result})


async def _method(method: str, params: dict[str, Any], viewer: str, tasks: Tasks, send: Send) -> Any:
    legacy = "/" in method
    if method in _SEND:
        return await _send(params, viewer, tasks, send, legacy)
    if method in _GET:
        return render(tasks.get(viewer, params.get("id")), legacy)
    if method in _CANCEL:
        tasks.get(viewer, params.get("id"))
        raise RpcError(TASK_NOT_CANCELABLE, "the task has finished: a finished task can't be canceled")
    if method in _LIST:
        return _list(params, tasks.visible(viewer))
    if method in _STREAMING:
        raise RpcError(UNSUPPORTED_OPERATION, "streaming isn't offered: send the message with SendMessage")
    if method in _PUSH:
        raise RpcError(PUSH_NOT_SUPPORTED, "push notifications aren't offered")
    if method in _EXTENDED_CARD:
        raise RpcError(UNSUPPORTED_OPERATION, "there is no extended agent card")
    raise RpcError(METHOD_NOT_FOUND, f"{method} isn't an A2A method this agent answers")


async def _send(params: dict[str, Any], viewer: str, tasks: Tasks, send: Send, legacy: bool) -> Any:
    message = params.get("message")
    if not isinstance(message, dict):
        raise RpcError(INVALID_PARAMS, "params.message is required")
    if not isinstance(message.get("messageId"), str) or not message["messageId"]:
        raise RpcError(INVALID_PARAMS, "message.messageId is required")
    parts = message.get("parts")
    if not isinstance(parts, list) or not parts or not all(isinstance(p, dict) for p in parts):
        raise RpcError(INVALID_PARAMS, "message.parts must hold at least one part")
    for part in parts:
        _check_part(part)
    if message.get("taskId") is not None:
        # Every task here has finished, so none takes another message.
        tasks.get(viewer, message["taskId"])
        raise RpcError(UNSUPPORTED_OPERATION, "the task has finished: send a new message without taskId")
    outcome = await send(message)
    context = message.get("contextId")
    kept = Kept(
        viewer,
        uuid.uuid4().hex,
        context if isinstance(context, str) and context else uuid.uuid4().hex,
        outcome,
        datetime.now(UTC),
    )
    tasks.add(kept)
    task = render(kept, legacy)
    return task if legacy else {"task": task}


def _check_part(part: dict[str, Any]) -> None:
    kind = part.get("kind")
    if kind == "file" or "raw" in part or "url" in part or "file" in part:
        raise RpcError(CONTENT_TYPE_NOT_SUPPORTED, "files aren't taken: send text or data")
    media = part.get("mediaType")
    if isinstance(media, str) and media.split(";")[0].strip().lower() not in MEDIA_TYPES:
        raise RpcError(CONTENT_TYPE_NOT_SUPPORTED, f"{media} isn't taken: send text or data")
    if not isinstance(part.get("text"), str) and "data" not in part:
        raise RpcError(INVALID_PARAMS, "each part holds text or data")


def _list(params: dict[str, Any], visible: list[Kept]) -> dict[str, Any]:
    size = params.get("pageSize", PAGE_SIZE)
    if not isinstance(size, int) or isinstance(size, bool) or not 1 <= size <= MAX_PAGE_SIZE:
        raise RpcError(INVALID_PARAMS, f"pageSize is from 1 to {MAX_PAGE_SIZE}")
    found = visible
    if params.get("contextId"):
        found = [k for k in found if k.context == params["contextId"]]
    status = params.get("status")
    if status and status != "TASK_STATE_UNSPECIFIED":
        found = [k for k in found if _state(k.outcome.state, legacy=False) == status]
    after = params.get("statusTimestampAfter")
    if after:
        try:
            since = datetime.fromisoformat(str(after).replace("Z", "+00:00"))
        except ValueError as exc:
            raise RpcError(INVALID_PARAMS, "statusTimestampAfter is an ISO 8601 time") from exc
        since = since if since.tzinfo else since.replace(tzinfo=UTC)
        found = [k for k in found if k.at >= since]
    start = 0
    if params.get("pageToken"):
        ids = [k.id for k in found]
        last = _unpage(str(params["pageToken"]))
        if last not in ids:
            raise RpcError(INVALID_PARAMS, "pageToken is not one this agent gave")
        start = ids.index(last) + 1
    page = found[start : start + size]
    more = start + size < len(found)
    with_artifacts = params.get("includeArtifacts") is True
    rendered = []
    for kept in page:
        task = render(kept, legacy=False)
        if not with_artifacts:
            task.pop("artifacts", None)
        rendered.append(task)
    return {
        "tasks": rendered,
        "nextPageToken": _page(page[-1].id) if more and page else "",
        "pageSize": size,
        "totalSize": len(found),
    }


def _page(task_id: str) -> str:
    return base64.urlsafe_b64encode(task_id.encode()).decode().rstrip("=")


def _unpage(token: str) -> str:
    try:
        return base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)).decode()
    except ValueError:
        return ""


def _state(state: str, legacy: bool) -> str:
    return state.replace("_", "-") if legacy else f"TASK_STATE_{state.upper()}"


def render(kept: Kept, legacy: bool) -> dict[str, Any]:
    """A task as A2A 1.0 or 0.3 shows it: its result as data and as text, for clients that read only text."""

    def part(kind: str, value: Any) -> dict[str, Any]:
        return {"kind": kind, kind: value} if legacy else {kind: value}

    outcome = kept.outcome
    status: dict[str, Any] = {
        "state": _state(outcome.state, legacy),
        "timestamp": kept.at.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
    }
    if outcome.said:
        status["message"] = {
            "messageId": uuid.uuid5(uuid.NAMESPACE_URL, kept.id).hex,
            "role": "agent" if legacy else "ROLE_AGENT",
            "parts": [part("text", outcome.said)],
            **({"kind": "message"} if legacy else {}),
        }
    task: dict[str, Any] = {"id": kept.id, "contextId": kept.context, "status": status}
    if outcome.data is not None:
        task["artifacts"] = [
            {
                "artifactId": uuid.uuid5(uuid.NAMESPACE_OID, kept.id).hex,
                "parts": [part("data", outcome.data), part("text", answer_text(outcome.data))],
            }
        ]
    if legacy:
        task["kind"] = "task"
    return task


def answer_text(data: Any) -> str:
    """A result as text: a text answer as it is, anything else as JSON."""
    if isinstance(data, dict) and set(data) == {"text"} and isinstance(data["text"], str):
        return data["text"]
    return json.dumps(data, ensure_ascii=False)


def card_response(request: Request, card: dict[str, Any]) -> Response:
    """The agent card, cacheable for five minutes and revalidated by its ETag."""
    body = json.dumps(card, ensure_ascii=False, separators=(",", ":")).encode()
    tag = '"' + hashlib.sha256(body).hexdigest()[:32] + '"'
    headers = {"Cache-Control": "public, max-age=300", "ETag": tag}
    if tag in [t.strip() for t in request.headers.get("if-none-match", "").split(",")]:
        return Response(status_code=304, headers=headers)
    return Response(body, media_type="application/json", headers=headers)
