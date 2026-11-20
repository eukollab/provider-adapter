# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The services an agent plans to use, offered to its own code over MCP and A2A on the local API.

An agent framework speaks MCP or A2A natively; these let it use the network without the callbacks' act(): `/mcp` is
an MCP server with one tool per entry of agent.json's `uses`, and `/a2a` an A2A agent with one skill per entry (its
card at `/a2a/.well-known/agent-card.json`). A call is the local API's `service` action, so the connector chooses and
calls the provider exactly as for act("service", ...), within the agent's rules and budget, and checks the receipt.
Both are on 127.0.0.1 behind LocalGuard: the agent's own programs only, with the local token once set.
"""

import contextlib
import json
import re
import uuid
from collections import OrderedDict
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import mcp_types as types
from mcp.server.lowlevel import Server
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import BaseRoute, Route

from ekl_provider_adapter.errors import AgentError

_NAME = re.compile(r"[^a-z0-9_]+")
KEPT_TASKS = 200
OPEN_SCHEMA: dict[str, Any] = {"type": "object"}

Service = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]
Schemas = Callable[["Use"], Awaitable[dict[str, Any] | None]]


@dataclass(frozen=True)
class Use:
    """One service the agent plans to use: its need, the preferred provider, and the name its code calls it by."""

    name: str
    need: str
    prefer: str | None = None


def uses_of(content: dict[str, Any] | None) -> list[Use]:
    """agent.json's `uses`, each named after its need (unique, [a-z0-9_])."""
    found: list[Use] = []
    for entry in (content or {}).get("uses") or []:
        if not isinstance(entry, dict) or not str(entry.get("need") or "").strip():
            continue
        need = str(entry["need"]).strip()
        base = _NAME.sub("_", need.lower()).strip("_")[:60] or "service"
        name, n = base, 2
        while any(u.name == name for u in found):
            name, n = f"{base}_{n}", n + 1
        found.append(Use(name, need, str(entry["prefer"]) if entry.get("prefer") else None))
    return found


def _body(use: Use, arguments: dict[str, Any]) -> dict[str, Any]:
    return {"need": use.need, "prefer": use.prefer, "input": arguments}


def _value(answer: dict[str, Any]) -> dict[str, Any]:
    value = answer.get("value")
    return value if isinstance(value, dict) else {"result": value}


def _said(exc: AgentError) -> str:
    return f"{exc.code}: {exc}" if exc.code else str(exc)


def mcp_surface(uses: list[Use], service: Service, schemas: Schemas) -> Server[Any]:
    """The MCP server at /mcp: a tool per planned service, its input schema the preferred provider's when known."""

    async def list_tools(ctx: Any, params: Any) -> types.ListToolsResult:
        del ctx, params
        tools = []
        for use in uses:
            schema = await schemas(use)
            tools.append(
                types.Tool(
                    name=use.name,
                    description=f"Uses a service on EuKollab: {use.need}",
                    input_schema=dict(schema or OPEN_SCHEMA),
                )
            )
        return types.ListToolsResult(tools=tools)

    async def call_tool(ctx: Any, params: types.CallToolRequestParams) -> types.CallToolResult:
        del ctx
        use = next((u for u in uses if u.name == params.name), None)
        if use is None:
            return types.CallToolResult(content=[types.TextContent(type="text", text="no such service")], is_error=True)
        try:
            answer = await service(_body(use, dict(params.arguments or {})))
        except AgentError as exc:
            return types.CallToolResult(content=[types.TextContent(type="text", text=_said(exc))], is_error=True)
        value = _value(answer)
        ekl = {k: answer.get(k) for k in ("execution_id", "charged", "receipt_checked", "provider", "calls_left")}
        return types.CallToolResult(
            content=[types.TextContent(type="text", text=json.dumps(value, ensure_ascii=False))],
            structured_content=value,
            meta={"ekl": ekl},
        )

    return Server("eukollab-services", on_list_tools=list_tools, on_call_tool=call_tool)


class A2ASurface:
    """The A2A agent at /a2a: a skill per planned service, called with one message whose data part is the input, or
    whose text is (a JSON object, else the need's text), and whose `metadata.skill` names the service (the only one
    when there is one). The answer carries the result as data and as text. A2A 1.0 and 0.3 over JSON-RPC."""

    def __init__(self, uses: list[Use], service: Service, handle: Callable[[], str]) -> None:
        self.uses = uses
        self.service = service
        self.handle = handle
        self.tasks: OrderedDict[str, dict[str, Any]] = OrderedDict()

    def card(self, base: str) -> dict[str, Any]:
        return {
            "name": f"EuKollab services for {self.handle() or 'this agent'}",
            "description": "The services this agent plans to use on EuKollab, called through its connector.",
            "version": "1.0.0",
            "supportedInterfaces": [{"url": f"{base}/a2a", "protocolBinding": "JSONRPC", "protocolVersion": "1.0"}],
            "capabilities": {"streaming": False, "pushNotifications": False},
            "defaultInputModes": ["application/json", "text/plain"],
            "defaultOutputModes": ["application/json", "text/plain"],
            "skills": [
                {"id": u.name, "name": u.need, "description": f"Uses a service on EuKollab: {u.need}", "tags": []}
                for u in self.uses
            ],
        }

    async def card_route(self, request: Request) -> Response:
        return JSONResponse(self.card(f"{request.url.scheme}://{request.headers.get('host', '127.0.0.1')}"))

    async def rpc(self, request: Request) -> Response:
        try:
            body = await request.json()
        except ValueError:
            body = None
        if not isinstance(body, dict):
            return _rpc_error(None, -32600, "not a JSON-RPC request")
        method, call_id = str(body.get("method") or ""), body.get("id")
        params: dict[str, Any] = body["params"] if isinstance(body.get("params"), dict) else {}
        try:
            result = await self._answer(method, params)
        except LookupError as missing:
            return _rpc_error(call_id, -32001, str(missing))
        except ValueError as unsupported:
            return _rpc_error(call_id, -32601 if "supported" in str(unsupported) else -32002, str(unsupported))
        return JSONResponse({"jsonrpc": "2.0", "id": call_id, "result": result})

    async def _answer(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        legacy = "/" in method
        if method in ("SendMessage", "message/send"):
            return await self._send(params, legacy)
        if method in ("GetTask", "tasks/get"):
            task = self.tasks.get(str(params.get("id")))
            if task is None:
                raise LookupError("task not found")
            return task if legacy else {"task": task}
        if method in ("CancelTask", "tasks/cancel"):
            raise ValueError("a finished task can't be cancelled")
        raise ValueError(f"{method or 'no method'} isn't supported")

    async def _send(self, params: dict[str, Any], legacy: bool) -> dict[str, Any]:
        message: dict[str, Any] = params["message"] if isinstance(params.get("message"), dict) else {}
        parts = [p for p in message.get("parts") or [] if isinstance(p, dict)]
        data = next((p["data"] for p in parts if isinstance(p.get("data"), dict)), None)
        text = "\n".join(str(p["text"]) for p in parts if isinstance(p.get("text"), str)).strip()
        if data is None and text.startswith("{"):
            with contextlib.suppress(ValueError):
                found = json.loads(text)
                data = found if isinstance(found, dict) else None
        metadata: dict[str, Any] = message["metadata"] if isinstance(message.get("metadata"), dict) else {}
        named = str(metadata.get("skill") or "")
        use = next((u for u in self.uses if u.name == named), None) or (self.uses[0] if len(self.uses) == 1 else None)
        task_id, context = uuid.uuid4().hex, str(message.get("contextId") or uuid.uuid4().hex)
        if use is None:
            task = _task(task_id, context, "rejected", legacy, said="name the service in metadata.skill")
        else:
            try:
                answer = await self.service(_body(use, data if data is not None else {"text": text}))
                task = _task(task_id, context, "completed", legacy, data=_value(answer))
            except AgentError as exc:
                task = _task(task_id, context, "failed", legacy, said=_said(exc))
        self.tasks[task_id] = task
        while len(self.tasks) > KEPT_TASKS:
            self.tasks.popitem(last=False)
        return task if legacy else {"task": task}


def _task(task_id: str, context: str, state: str, legacy: bool, *, data: Any = None, said: str = "") -> dict[str, Any]:
    def part(kind: str, value: Any) -> dict[str, Any]:
        return {"kind": kind, kind: value} if legacy else {kind: value}

    status: dict[str, Any] = {"state": state.replace("_", "-") if legacy else f"TASK_STATE_{state.upper()}"}
    if said:
        status["message"] = {
            "messageId": uuid.uuid4().hex,
            "role": "agent" if legacy else "ROLE_AGENT",
            "parts": [part("text", said)],
        }
    task: dict[str, Any] = {"id": task_id, "contextId": context, "status": status}
    if data is not None:
        # Clients that read only text get the result too: a text answer as it is, anything else as JSON.
        said_back = data["text"] if isinstance(data, dict) and set(data) == {"text"} else None
        text = said_back if isinstance(said_back, str) else json.dumps(data, ensure_ascii=False)
        task["artifacts"] = [{"artifactId": uuid.uuid4().hex, "parts": [part("data", data), part("text", text)]}]
    if legacy:
        task["kind"] = "task"
    return task


def _rpc_error(call_id: Any, code: int, message: str) -> Response:
    return JSONResponse({"jsonrpc": "2.0", "id": call_id, "error": {"code": code, "message": message}})


def surface_routes(
    uses: list[Use], service: Service, schemas: Schemas, handle: Callable[[], str]
) -> tuple[list[BaseRoute], Callable[[Starlette], contextlib.AbstractAsyncContextManager[None]]]:
    """The routes of both surfaces, and the lifespan the MCP one needs. With no planned services there is nothing to
    offer, and no routes."""
    if not uses:

        @contextlib.asynccontextmanager
        async def nothing(_: Starlette) -> AsyncIterator[None]:
            yield

        return [], nothing
    mcp_app = mcp_surface(uses, service, schemas).streamable_http_app(
        streamable_http_path="/mcp",
        stateless_http=True,
        json_response=True,
        # LocalGuard, in front of the whole local API, already answers only a loopback Host with no Origin, with any
        # port or none; the SDK's own check would refuse a loopback Host without one.
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
    )
    a2a = A2ASurface(uses, service, handle)
    routes: list[BaseRoute] = [
        *mcp_app.routes,
        Route("/a2a/.well-known/agent-card.json", a2a.card_route, methods=["GET"]),
        Route("/a2a", a2a.rpc, methods=["POST"]),
    ]

    @contextlib.asynccontextmanager
    async def lifespan(_: Starlette) -> AsyncIterator[None]:
        async with mcp_app.router.lifespan_context(mcp_app):
            yield

    return routes, lifespan
