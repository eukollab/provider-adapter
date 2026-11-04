# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The connector: the adapter as its own process next to an existing MCP server.

    EKL_PROVIDER_CREDENTIAL=… EKL_NETWORK_URL=… EKL_UPSTREAM_MCP_URL=… ekl-provider-connector --port 8000

It calls the existing server's tools over MCP and needs nothing from that server's process, environment or files:
give it its agent.json (--config, else the file EKL_AGENT_FILE names, else the text EKL_AGENT_JSON holds, else
./agent.json), the provider credential, the server's address (upstream.mcp_url, which may carry the server's own
token), network access to the server and to EuKollab, and, in durable mode, its journal store. It announces agent.json
and serves the version its publisher confirmed. With EKL_AGENT_JSON, one published image runs any agent from
environment variables alone. GET /healthz answers 200 for load balancers and container services.

With EKL_AGENT_CREDENTIAL the connector also lets the agent take part: events go to EKL_CALLBACK_URL in
the publisher's app, and the agent's actions are served on 127.0.0.1 (EKL_AGENT_API_PORT, default 8765) only.
`POST /agent/service` there calls a declared service for an app in any language, from its stand-ins until the
credential is set; `--use-only` runs only that loopback API and the callbacks, for an agent that offers nothing.
"""

import argparse
import asyncio
import contextlib
import logging
import os
from collections.abc import AsyncIterator
from typing import Any

import httpx2
import mcp_types as types
import uvicorn
from mcp import Client
from mcp.client.streamable_http import streamable_http_client
from mcp.shared.exceptions import MCPError
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Route

from ekl_provider_adapter.api import HttpProviderApi, ProviderApi
from ekl_provider_adapter.attach import (
    AdapterApp,
    AgentFileSource,
    agent_for,
    local_file,
    runs_for,
    settings_from_environment,
    storage_for,
)
from ekl_provider_adapter.callbacks import bridge, bridge_runs, local_api
from ekl_provider_adapter.config import DEFAULT_NETWORK_URL
from ekl_provider_adapter.core import AdapterCore, Hooks, Settings
from ekl_provider_adapter.server import adapter_routes
from ekl_provider_adapter.storage import Storage


class Upstream:
    """The existing MCP server, reached as an MCP client. Each call opens its own short session, so one failing
    call cannot wedge the others."""

    def __init__(self, url: str, http: httpx2.AsyncClient | None = None) -> None:
        self.url = url
        self._http = http

    @contextlib.asynccontextmanager
    async def _client(self) -> AsyncIterator[Client]:
        http = self._http or httpx2.AsyncClient(timeout=60)
        try:
            async with Client(streamable_http_client(self.url, http_client=http)) as client:
                yield client
        finally:
            if self._http is None:
                await http.aclose()

    async def call(self, tool: str, arguments: dict[str, Any]) -> types.CallToolResult:
        try:
            async with self._client() as client:
                return await client.call_tool(tool, arguments)
        except MCPError as exc:
            # A protocol-level refusal (unknown tool, invalid params) means the tool never ran.
            return types.CallToolResult(
                content=[types.TextContent(type="text", text=str(exc))], is_error=True, meta={"ekl": {"effect": "none"}}
            )

    async def tools(self) -> list[types.Tool]:
        async with self._client() as client:
            return list((await client.list_tools()).tools)

    async def list(self) -> list[types.Resource]:
        async with self._client() as client:
            return list((await client.list_resources()).resources)

    async def read(self, uri: str) -> types.ReadResourceResult:
        async with self._client() as client:
            return await client.read_resource(uri)


def connector_app(
    agent_file: AgentFileSource | None = None,
    *,
    storage: Storage | None = None,
    hooks: Hooks | None = None,
    credential: str | None = None,
    network_url: str | None = None,
    api: ProviderApi | None = None,
    upstream: Upstream | None = None,
    upstream_url: str | None = None,
    settings: Settings | None = None,
    callback_url: str | None = None,
    runs_api: Any | None = None,
) -> AdapterApp:
    """The connector's public app: the adapter endpoint in front of the upstream server, plus /healthz. With
    EKL_AGENT_CREDENTIAL and a callback URL it also runs the agent's own side, posting events to the publisher's
    app. Arguments override the environment; tests pass their own api, upstream and storage."""
    local = local_file(agent_file)
    url = (
        upstream_url
        or os.environ.get("EKL_UPSTREAM_MCP_URL")
        or (local.agent.upstream.mcp_url if local and local.agent.upstream else None)
    )
    if upstream is None and not url:
        raise ValueError("set upstream.mcp_url in agent.json, or EKL_UPSTREAM_MCP_URL, to your MCP server's address")
    found_credential, found_network = settings_from_environment(local, credential, network_url)
    target = upstream or Upstream(str(url))
    core = AdapterCore(
        local,
        api or HttpProviderApi(found_network, found_credential),
        target.call,
        credential=found_credential,
        endpoint=os.environ.get("EKL_ADAPTER_ENDPOINT"),
        storage=storage_for(local, storage),
        hooks=hooks,
        settings=settings,
    )
    routes, adapter_lifespan = adapter_routes(core, target.tools, resources=target)

    async def healthz(_: Request) -> Response:
        # Liveness only, open to anyone: it says nothing about the agent, the upstream or the network.
        return PlainTextResponse("ok")

    routes = [Route("/healthz", healthz, methods=["GET"]), *routes]
    storage_url = (local.agent.adapter.storage if local else None) or os.environ.get("EKL_ADAPTER_STORAGE")
    own = agent_for(found_network, None, None, storage_url)
    callback = callback_url or os.environ.get("EKL_CALLBACK_URL")
    if own.enabled and callback:
        bridge(own, callback, os.environ.get("EKL_AGENT_CREDENTIAL", ""))
    runs = runs_for(core, local, runs_api, found_network, found_credential)
    if callback:
        # Paid runs are approved by the publisher's app, in any language; without an answer the run keeps waiting.
        bridge_runs(runs, callback, os.environ.get("EKL_AGENT_CREDENTIAL") or found_credential)

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        with contextlib.suppress(Exception):  # the upstream may start later; schemas are read again per listing
            core.tool_schemas.update({t.name: dict(t.output_schema) for t in await target.tools() if t.output_schema})
        async with adapter_lifespan(app):
            await own.start()
            watching = asyncio.ensure_future(runs.watch()) if found_credential or runs_api else None
            try:
                yield
            finally:
                if watching is not None:
                    watching.cancel()
                await own.stop()

    app = AdapterApp(routes=routes, lifespan=lifespan)
    app.state.ekl_core = core
    app.agent = own
    app.runs = runs
    return app


def use_only_app(callback_url: str | None = None) -> Starlette:
    """The connector for an agent that offers nothing: no upstream and no public endpoint, only the
    agent's events posted to the app and its actions on the loopback API. Without EKL_AGENT_CREDENTIAL the declared
    services answer from their stand-ins."""
    network = os.environ.get("EKL_NETWORK_URL") or DEFAULT_NETWORK_URL
    own = agent_for(network, None, None, os.environ.get("EKL_ADAPTER_STORAGE"))
    callback = callback_url or os.environ.get("EKL_CALLBACK_URL")
    if own.enabled and callback:
        bridge(own, callback, os.environ.get("EKL_AGENT_CREDENTIAL", ""))
    app = local_api(own)

    @contextlib.asynccontextmanager
    async def lifespan(_: Starlette) -> AsyncIterator[None]:
        await own.start()
        try:
            yield
        finally:
            await own.stop()

    app.router.lifespan_context = lifespan
    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="EuKollab provider connector")
    parser.add_argument(
        "--config",
        default=None,
        help="agent.json (default: EKL_AGENT_FILE, else EKL_AGENT_JSON, else ./agent.json)",
    )
    parser.add_argument("--upstream", default=None, help="the existing MCP server (default: EKL_UPSTREAM_MCP_URL)")
    parser.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))  # a server, meant to be reachable
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8000")))
    parser.add_argument(
        "--use-only",
        action="store_true",
        help="an agent that offers nothing: serve only its local API and callbacks, on 127.0.0.1",
    )
    options = parser.parse_args()
    # What the network says about the announced agent.json is logged here, where the publisher deploying it looks.
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    private_port = int(os.environ.get("EKL_AGENT_API_PORT", "8765"))
    if options.use_only:
        uvicorn.run(use_only_app(), host="127.0.0.1", port=private_port, log_level="info")
        return
    app = connector_app(options.config, upstream_url=options.upstream)
    # The agent's actions are served beside the public endpoint, on the loopback interface only.
    # Without the credential they still run, so declared services answer from their stand-ins.
    public = uvicorn.Server(uvicorn.Config(app, host=options.host, port=options.port, log_level="info"))
    private = uvicorn.Server(
        uvicorn.Config(local_api(app.agent, app.runs), host="127.0.0.1", port=private_port, log_level="info")
    )

    async def both() -> None:
        await asyncio.gather(public.serve(), private.serve())

    asyncio.run(both())
