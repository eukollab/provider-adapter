# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The adapter's own MCP endpoint, separate from the server's existing endpoint.

It lists only the tools agent.json offers, in its confirmed version or the one announced, with the schemas the tools
declare (agent.json may supply an output schema a tool lacks), and hands every call to the core. Consumers reach only
this endpoint, so an existing endpoint protected by a secret URL stays private.
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, Protocol

import mcp_types as types
from mcp.server.lowlevel import Server
from mcp.server.transport_security import TransportSecuritySettings
from mcp.shared.exceptions import MCPError
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.routing import BaseRoute, Route

from ekl_provider_adapter.core import AdapterCore


def _bearer(request: Any) -> str | None:
    header = request.headers.get("authorization", "") if request is not None else ""
    scheme, _, token = header.partition(" ")
    return token.strip() or None if scheme.lower() == "bearer" else None


# Tool _meta keys that describe the tool's own interface (OpenAI Apps SDK and MCP Apps): passed through unchanged so
# a client can draw the server's widget. Nothing else in a tool's _meta leaves the adapter.
UI_META_PREFIXES = ("openai/", "ui")


def ui_meta(meta: Any) -> dict[str, Any] | None:
    found = {k: v for k, v in dict(meta or {}).items() if k.startswith(UI_META_PREFIXES)}
    return found or None


def widget_uris(tools: list[types.Tool]) -> set[str]:
    """The UI resources the listed tools name: the only resources the adapter serves."""
    uris = set()
    for tool in tools:
        meta = dict(tool.meta or {})
        template = meta.get("openai/outputTemplate")
        ui = meta.get("ui")
        resource = ui.get("resourceUri") if isinstance(ui, dict) else None
        uris |= {str(u) for u in (template, resource, meta.get("ui/resourceUri")) if isinstance(u, str) and u}
    return uris


class Resources(Protocol):
    """The existing server's resources, as the adapter reaches them."""

    async def list(self) -> list[types.Resource]: ...

    async def read(self, uri: str) -> types.ReadResourceResult: ...


def listed_tools(core: AdapterCore, declared: list[types.Tool]) -> list[types.Tool]:
    """The tools the configuration offers, as the server declares them, with their interface _meta."""
    by_name = {t.name: t for t in declared}
    tools = []
    for cap in core.listed:
        tool = by_name.get(cap.tool)
        input_schema = cap.input_schema or (tool.input_schema if tool else {"type": "object"})
        output_schema = cap.output_schema or (tool.output_schema if tool else None)
        tools.append(
            types.Tool(
                name=cap.tool,
                description=(tool.description if tool else None) or str(getattr(cap, "summary", "")) or None,
                input_schema=dict(input_schema),
                output_schema=dict(output_schema) if output_schema else None,
                meta=ui_meta(tool.meta) if tool else None,
            )
        )
    return tools


def build_server(
    core: AdapterCore, declared: Callable[[], Awaitable[list[types.Tool]]], resources: Resources | None = None
) -> Server[Any]:
    """The low-level MCP server behind the adapter's endpoint. `declared` reads the tools the server declares at
    each listing, so a changed schema shows at once; resources are served only when the server has some to offer."""

    async def list_tools(ctx: Any, params: Any) -> types.ListToolsResult:
        del params
        # The endpoint challenge rides on tools/list, which every MCP revision has: the network sends
        # _meta.ekl.challenge and the proof comes back in the result's _meta, bound to this endpoint and credential.
        challenge = dict((ctx.meta or {}).get("ekl") or {}).get("challenge")
        if challenge and core.local is None:
            # The network is reviewing or checking a version announced from elsewhere (ekl agent sync): list what it
            # holds now, not what the last idle check found.
            await core.load_config()
        tools = listed_tools(core, await declared())
        if challenge:
            try:
                proof = core.answer_challenge(str(challenge))
            except ValueError:
                return types.ListToolsResult(tools=tools, meta={"ekl": {"code": "INVALID_REQUEST"}})
            return types.ListToolsResult(tools=tools, meta={"ekl": {"proof": proof}})
        return types.ListToolsResult(tools=tools)

    async def call_tool(ctx: Any, params: types.CallToolRequestParams) -> types.CallToolResult:
        meta = dict(ctx.meta or {}) if ctx.meta else {}
        return await core.handle(params.name, dict(params.arguments or {}), _bearer(ctx.request), meta)

    async def list_resources(ctx: Any, params: Any) -> types.ListResourcesResult:
        del ctx, params
        assert resources is not None
        allowed = widget_uris(listed_tools(core, await declared()))
        return types.ListResourcesResult(resources=[r for r in await resources.list() if str(r.uri) in allowed])

    async def read_resource(ctx: Any, params: types.ReadResourceRequestParams) -> types.ReadResourceResult:
        del ctx
        assert resources is not None
        # Only the widgets offered tools name, unchanged; a consumer can read nothing else of the server through here.
        if str(params.uri) not in widget_uris(listed_tools(core, await declared())):
            raise MCPError(code=-32002, message="resource not found")
        return await resources.read(str(params.uri))

    if resources is None:
        return Server("ekl-provider-adapter", on_list_tools=list_tools, on_call_tool=call_tool)
    return Server(
        "ekl-provider-adapter",
        on_list_tools=list_tools,
        on_call_tool=call_tool,
        on_list_resources=list_resources,
        on_read_resource=read_resource,
    )


def adapter_routes(
    core: AdapterCore,
    declared: Callable[[], Awaitable[list[types.Tool]]],
    *,
    resources: Resources | None = None,
) -> tuple[list[BaseRoute], Callable[[Starlette], contextlib.AbstractAsyncContextManager[None]]]:
    """The endpoint's routes and a lifespan that loads the confirmed configuration and recovers on start, then
    announces agent.json, watches for new versions and delivers completions in the background."""
    path = core.path
    app = build_server(core, declared, resources).streamable_http_app(
        streamable_http_path=path,
        stateless_http=True,
        json_response=True,
        # A public endpoint: every call is checked by the network, so DNS-rebinding protection for local servers
        # does not apply.
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
        host="0.0.0.0",  # not a bind address: tells the SDK this endpoint is public
    )

    async def metrics(request: Request) -> Response:
        if not core.is_provider_credential(_bearer(request) or ""):
            return PlainTextResponse("unauthorized", status_code=401)
        await core.refresh_metrics()
        return JSONResponse(core.metrics.as_dict())

    routes: list[BaseRoute] = [
        Route(f"{path.rstrip('/')}/metrics", metrics, methods=["GET"]),
        *app.routes,
    ]

    @contextlib.asynccontextmanager
    async def lifespan(_: Starlette) -> AsyncIterator[None]:
        async with app.router.lifespan_context(app):
            await core.load_config()
            await core.recover()
            # Announcing runs beside serving, so a network that cannot be reached never holds up the endpoint.
            # The outbox loop idles without a durable store; a configuration may switch to durable mode later.
            loops = [asyncio.ensure_future(core.watch_config()), asyncio.ensure_future(core.run_outbox())]
            try:
                yield
            finally:
                for loop in loops:
                    loop.cancel()
                await core.drain()

    return routes, lifespan
