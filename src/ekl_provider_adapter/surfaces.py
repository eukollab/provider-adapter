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
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import mcp_types as types
from mcp.server.lowlevel import Server
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import BaseRoute, Route

from ekl_provider_adapter import a2a_server
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
    when there is one). The answer carries the result as data and as text. A2A 1.0 and 0.3 over JSON-RPC
    (a2a_server)."""

    def __init__(self, uses: list[Use], service: Service, handle: Callable[[], str]) -> None:
        self.uses = uses
        self.service = service
        self.handle = handle
        # The local API answers only the agent's own programs, so they all see every task.
        self.tasks = a2a_server.Tasks(KEPT_TASKS)

    def card(self, base: str) -> dict[str, Any]:
        return {
            "name": f"EuKollab services for {self.handle() or 'this agent'}",
            "description": "The services this agent plans to use on EuKollab, called through its connector.",
            "version": "1.0.0",
            "supportedInterfaces": [{"url": f"{base}/a2a", "protocolBinding": "JSONRPC", "protocolVersion": "1.0"}],
            "capabilities": {"streaming": False, "pushNotifications": False},
            "defaultInputModes": list(a2a_server.MEDIA_TYPES),
            "defaultOutputModes": list(a2a_server.MEDIA_TYPES),
            "skills": [
                {"id": u.name, "name": u.need, "description": f"Uses a service on EuKollab: {u.need}", "tags": []}
                for u in self.uses
            ],
        }

    async def card_route(self, request: Request) -> Response:
        base = f"{request.url.scheme}://{request.headers.get('host', '127.0.0.1')}"
        return a2a_server.card_response(request, self.card(base))

    async def rpc(self, request: Request) -> Response:
        return await a2a_server.answer(request, "", self.tasks, self._send)

    async def _send(self, message: dict[str, Any]) -> a2a_server.Outcome:
        parts = [p for p in message["parts"] if isinstance(p, dict)]
        data = next((p["data"] for p in parts if isinstance(p.get("data"), dict)), None)
        text = "\n".join(str(p["text"]) for p in parts if isinstance(p.get("text"), str)).strip()
        if data is None and text.startswith("{"):
            with contextlib.suppress(ValueError):
                found = json.loads(text)
                data = found if isinstance(found, dict) else None
        metadata: dict[str, Any] = message["metadata"] if isinstance(message.get("metadata"), dict) else {}
        named = str(metadata.get("skill") or "")
        use = next((u for u in self.uses if u.name == named), None) or (self.uses[0] if len(self.uses) == 1 else None)
        if use is None:
            return a2a_server.Outcome("rejected", said="name the service in metadata.skill")
        try:
            answer = await self.service(_body(use, data if data is not None else {"text": text}))
        except AgentError as exc:
            return a2a_server.Outcome("failed", said=_said(exc))
        return a2a_server.Outcome("completed", data=_value(answer))


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
