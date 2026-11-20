# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The servers behind one connector, whatever protocol each speaks (EKP 3.4 §5.2).

An agent may be an MCP server, an HTTPS API described by OpenAPI, an A2A agent, or several at once. Each is reached
through an upstream that shows its operations as MCP tools and its answers as CallToolResults; this module routes
each tool to the upstream its capability names (`via`). Above it nothing knows which protocol answered: admission,
outcomes, fingerprints and the endpoint consumers call are the same for every agent, so two agents never need a
protocol in common to work together.
"""

import os
from collections.abc import Callable
from typing import Any, Protocol

import mcp_types as types

from ekl_provider_adapter.config import LocalFile, Via

ENVIRONMENT: dict[str, str] = {
    "mcp_url": "EKL_UPSTREAM_MCP_URL",
    "openapi_url": "EKL_UPSTREAM_OPENAPI_URL",
    "api_url": "EKL_UPSTREAM_API_URL",
    "a2a_url": "EKL_UPSTREAM_A2A_URL",
}


class Target(Protocol):
    """One upstream server, as the adapter reaches it."""

    async def call(self, tool: str, arguments: dict[str, Any]) -> types.CallToolResult: ...

    async def tools(self) -> list[types.Tool]: ...

    async def list(self) -> list[types.Resource]: ...

    async def read(self, uri: str) -> types.ReadResourceResult: ...


def addresses(local: LocalFile | None, mcp_url: str | None = None) -> dict[str, str]:
    """Each server's address: given (the MCP server's, from --upstream), else from the environment, else from
    agent.json's upstream section."""
    upstream = local.agent.upstream if local else None
    found: dict[str, str] = {}
    for key, variable in ENVIRONMENT.items():
        value = (mcp_url if key == "mcp_url" else None) or os.environ.get(variable) or getattr(upstream, key, None)
        if value:
            found[key] = str(value)
    return found


class Upstreams:
    """Several upstreams as one: a call goes to the upstream `route` names for the tool, a listing holds every
    upstream's tools (the routed upstream's when two share a name). Resources come only from an MCP server."""

    def __init__(self, by_kind: dict[Via, Target], route: Callable[[str], Via | None] | None = None) -> None:
        if not by_kind:
            raise ValueError("no upstream server")
        self.by_kind = by_kind
        self.route = route or (lambda _: None)

    def _kind(self, tool: str) -> Via:
        routed = self.route(tool)
        if routed is not None:
            return routed
        return "mcp" if "mcp" in self.by_kind else next(iter(self.by_kind))

    async def call(self, tool: str, arguments: dict[str, Any]) -> types.CallToolResult:
        kind = self._kind(tool)
        target = self.by_kind.get(kind)
        if target is None:
            return types.CallToolResult(
                content=[types.TextContent(type="text", text=f"no {kind} server is configured for {tool}")],
                is_error=True,
                meta={"ekl": {"effect": "none"}},
            )
        return await target.call(tool, arguments)

    async def tools(self) -> list[types.Tool]:
        found: dict[str, types.Tool] = {}
        for kind, target in self.by_kind.items():
            for tool in await target.tools():
                if tool.name not in found or self._kind(tool.name) == kind:
                    found[tool.name] = tool
        return list(found.values())

    async def list(self) -> list[types.Resource]:
        mcp = self.by_kind.get("mcp")
        return await mcp.list() if mcp is not None else []

    async def read(self, uri: str) -> types.ReadResourceResult:
        mcp = self.by_kind.get("mcp")
        if mcp is None:
            raise LookupError(f"no MCP server serves resources ({uri})")
        return await mcp.read(uri)
