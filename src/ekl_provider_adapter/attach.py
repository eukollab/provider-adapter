# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Attach the adapter to a server built with the Python MCP SDK.

    from ekl_provider_adapter import attach
    app = attach(server)      # instead of: app = server.streamable_http_app()

It needs its agent.json (EKL_AGENT_FILE, else EKL_AGENT_JSON, else ./agent.json), EKL_PROVIDER_CREDENTIAL and
EKL_NETWORK_URL. It announces agent.json and serves the version its publisher confirmed, read from the network.

The server keeps its own endpoint and clients. The adapter adds its endpoint beside it and calls the server's tools
through the SDK's normal dispatch: it sees the tool name, arguments, result and request metadata, nothing else.
"""

import asyncio
import base64
import contextlib
import logging
import os
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import mcp_types as types
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import ValidationError
from starlette.applications import Starlette
from starlette.routing import Mount

from ekl_provider_adapter.agent import Agent
from ekl_provider_adapter.agent_journal import AgentJournal
from ekl_provider_adapter.api import HttpProviderApi, ProviderApi
from ekl_provider_adapter.config import DEFAULT_NETWORK_URL, AgentFile, LocalFile, inline_agent_file
from ekl_provider_adapter.core import AdapterCore, Hooks, Settings
from ekl_provider_adapter.network_client import McpNetwork, Network
from ekl_provider_adapter.runs import HttpRunsApi, Runs
from ekl_provider_adapter.server import adapter_routes
from ekl_provider_adapter.storage import Storage, open_storage

log = logging.getLogger("ekl_provider_adapter")

AgentFileSource = str | Path | dict[str, Any] | AgentFile


def settings_from_environment(
    local: LocalFile | None, credential: str | None, network_url: str | None
) -> tuple[str, str]:
    """The provider credential and network URL: given in code, else from the environment; the URL falls back to
    agent.json's `network.url`. Without a credential the adapter cannot ask the network to admit anything, so it
    refuses to start."""
    found = credential or os.environ.get("EKL_PROVIDER_CREDENTIAL")
    if not found:
        raise ValueError("set EKL_PROVIDER_CREDENTIAL to the agent key `ekl agent init` wrote to .env")
    network = (
        network_url or os.environ.get("EKL_NETWORK_URL") or (local.agent.network.url if local else DEFAULT_NETWORK_URL)
    )
    return found, network


def local_file(agent_file: AgentFileSource | None) -> LocalFile | None:
    """The agent.json deployed with the adapter: given in code, named by EKL_AGENT_FILE, held by EKL_AGENT_JSON, or
    ./agent.json. One given, named or held must load; without any, an adapter with no ./agent.json has nothing to
    announce and serves the confirmed configuration only.

    EKL_AGENT_JSON lets one published connector image serve any agent with no build. It is not watched for changes,
    unlike a file: a new value arrives only with a redeploy, which starts the adapter again."""
    if agent_file is not None:
        return LocalFile.read(agent_file)
    named = os.environ.get("EKL_AGENT_FILE")
    if named:
        return LocalFile.read(named)
    inline = os.environ.get("EKL_AGENT_JSON")
    if inline:
        return LocalFile.read(inline_agent_file(inline))
    if Path("agent.json").is_file():
        return LocalFile.read("agent.json")
    log.warning("no agent.json here, and neither EKL_AGENT_FILE nor EKL_AGENT_JSON is set: nothing to announce")
    return None


def storage_for(local: LocalFile | None, storage: Storage | None) -> Storage | None:
    """A store given in code, or named by agent.json or EKL_ADAPTER_STORAGE; durable mode needs one."""
    if storage is not None:
        return storage
    url = (local.agent.adapter.storage if local else None) or os.environ.get("EKL_ADAPTER_STORAGE")
    return open_storage(url) if url else None


class AdapterApp(Starlette):
    """The server's app with the adapter beside it. `agent` is the agent's own side; `runs` approves paid runs."""

    agent: Agent
    runs: Runs


def runs_for(core: AdapterCore, local: LocalFile | None, api: Any | None, network_url: str, credential: str) -> Runs:
    """Run approval for this adapter: the confirmed configuration's payment page once there is one, else the local
    agent.json's."""
    return Runs(
        api or HttpRunsApi(network_url, credential),
        lambda: core.agent if core.configured else (local.agent if local else None),
    )


def agent_for(
    network_url: str, agent_credential: str | None, network: Network | None, storage_url: str | None
) -> Agent:
    """The agent side, turned on only by the agent-operation credential. The provider credential is
    never used for it, so a leaked provider credential still cannot post, connect or spend."""
    credential = agent_credential or os.environ.get("EKL_AGENT_CREDENTIAL")
    if network is None and not credential:
        return Agent(None)
    # A durable Postgres store is shared by every copy, so the agent's journal lives there too.
    shared = storage_url if storage_url and storage_url.startswith(("postgresql://", "postgres://")) else None
    return Agent(network or McpNetwork(network_url, str(credential)), AgentJournal(shared))


def tool_error_result(exc: ToolError) -> types.CallToolResult:
    """The SDK raises ToolError where a remote client would see isError. Invalid arguments never reached the tool,
    so they are declared without effect; anything else is left for the outcome rules to judge."""
    no_effect = isinstance(exc.__cause__, ValidationError)
    return types.CallToolResult(
        content=[types.TextContent(type="text", text=str(exc))],
        is_error=True,
        meta={"ekl": {"effect": "none"}} if no_effect else None,
    )


def attach(
    server: MCPServer,
    agent_file: AgentFileSource | None = None,
    *,
    storage: Storage | None = None,
    hooks: Hooks | None = None,
    credential: str | None = None,
    network_url: str | None = None,
    api: ProviderApi | None = None,
    settings: Settings | None = None,
    streamable_http_path: str = "/mcp",
    agent_credential: str | None = None,
    agent_network: Network | None = None,
    runs_api: Any | None = None,
    **http_options: Any,
) -> AdapterApp:
    """The server's own app with the adapter's endpoint beside it.

    `agent_file` (a path, a parsed dict or a loaded AgentFile) is the agent.json it announces; by default the file
    EKL_AGENT_FILE names, else the text EKL_AGENT_JSON holds, else ./agent.json. Calls are served from the version its
    publisher confirmed. The provider credential and network URL come from the arguments or EKL_PROVIDER_CREDENTIAL
    and EKL_NETWORK_URL. `hooks` registers the outcome lookups and cancellation hooks the configuration names.
    `http_options` go to the server's streamable_http_app.

    The returned app's `agent` lets the agent take part itself. It turns on with EKL_AGENT_CREDENTIAL
    (or `agent_credential`); `agent_network` replaces the network, for tests with `testing.FakeNetwork`. Its `runs`
    approves paid runs: register a handler with `app.runs.on_approval`, or approve later with `app.runs.approve`."""
    try:
        local = local_file(agent_file)
    except ValueError as invalid:  # pydantic's ValidationError is a ValueError
        # The server still serves its own clients, so `ekl agent sync` can read it and write agent.json again
        # ; a file that doesn't validate is never announced.
        log.error("agent.json doesn't validate, so nothing is announced; fix it with ekl agent sync. %s", invalid)
        local = None
    found_credential, found_network = settings_from_environment(local, credential, network_url)

    async def invoke(tool: str, arguments: dict[str, Any]) -> types.CallToolResult:
        try:
            result = await server.call_tool(tool, arguments)
        except ToolError as exc:
            return tool_error_result(exc)
        if not isinstance(result, types.CallToolResult):
            return types.CallToolResult(
                content=[
                    types.TextContent(type="text", text="the tool asked for input, which the network cannot give")
                ],
                is_error=True,
                meta={"ekl": {"effect": "none"}},
            )
        return result

    async def declared() -> list[types.Tool]:
        return list(await server.list_tools())

    class OwnResources:
        """The server's own resources, read in process, for the widgets its tools name."""

        async def list(self) -> list[types.Resource]:
            return [
                types.Resource(uri=str(r.uri), name=r.name, mime_type=r.mime_type)
                for r in await server.list_resources()
            ]

        async def read(self, uri: str) -> types.ReadResourceResult:
            found = await server.read_resource(uri)
            if isinstance(found, types.InputRequiredResult):
                raise ValueError("the resource asked for input")
            contents: list[types.TextResourceContents | types.BlobResourceContents] = [
                types.TextResourceContents(uri=uri, mime_type=c.mime_type, text=c.content, meta=c.meta)
                if isinstance(c.content, str)
                else types.BlobResourceContents(
                    uri=uri, mime_type=c.mime_type, blob=base64.b64encode(c.content).decode(), meta=c.meta
                )
                for c in found
            ]
            return types.ReadResourceResult(contents=contents)

    core = AdapterCore(
        local,
        api or HttpProviderApi(found_network, found_credential),
        invoke,
        credential=found_credential,
        endpoint=os.environ.get("EKL_ADAPTER_ENDPOINT"),
        storage=storage_for(local, storage),
        hooks=hooks,
        settings=settings,
    )
    routes, adapter_lifespan = adapter_routes(core, declared, resources=OwnResources())
    existing = server.streamable_http_app(streamable_http_path=streamable_http_path, **http_options)

    storage_url = (local.agent.adapter.storage if local else None) or os.environ.get("EKL_ADAPTER_STORAGE")
    own = agent_for(found_network, agent_credential, agent_network, storage_url)
    runs = runs_for(core, local, runs_api, found_network, found_credential)

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        core.tool_schemas.update({t.name: dict(t.output_schema) for t in await declared() if t.output_schema})
        async with existing.router.lifespan_context(existing), adapter_lifespan(app):
            await own.start()
            watching = asyncio.ensure_future(runs.watch()) if found_credential or runs_api else None
            try:
                yield
            finally:
                if watching is not None:
                    watching.cancel()
                await own.stop()

    combined = AdapterApp(routes=[*routes, Mount("/", app=existing)], lifespan=lifespan)
    combined.state.ekl_core = core
    combined.agent = own
    combined.runs = runs
    return combined
