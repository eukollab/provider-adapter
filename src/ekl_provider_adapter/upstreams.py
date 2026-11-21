# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The servers behind one connector, whatever protocol each speaks (EKP 3.4 §5.2).

An agent may be an MCP server, an HTTPS API described by OpenAPI, an A2A agent, or several at once. Each is reached
through an upstream that shows its operations as MCP tools and its answers as CallToolResults; this module routes
each tool to the upstream its capability names (`via`). Above it nothing knows which protocol answered: admission,
outcomes, fingerprints and the endpoint consumers call are the same for every agent, so two agents never need a
protocol in common to work together.
"""

import asyncio
import logging
import os
import socket
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

import httpx2
import mcp_types as types

from ekl_provider_adapter.config import ORDER, LocalFile, Via

log = logging.getLogger(__name__)

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


@dataclass(frozen=True)
class Plan:
    """How a service is reached: its server and tool, then the same function on the agent's other servers in the
    order they are tried (`also`). `effects` is an external action's: it moves on only from a server never reached."""

    steps: list[tuple[Via, str]]
    effects: bool = False


def unreached(exc: BaseException) -> bool:
    """Whether a call failed before reaching the server: no connection was made, so nothing ran there."""
    seen: list[BaseException] = [exc]
    while seen:
        found = seen.pop()
        if isinstance(found, httpx2.ConnectError | httpx2.ConnectTimeout | ConnectionRefusedError | socket.gaierror):
            return True
        if isinstance(found, BaseExceptionGroup):
            seen.extend(found.exceptions)
        seen.extend(e for e in (found.__cause__, found.__context__) if e is not None)
    return False


def _unavailable(result: types.CallToolResult) -> bool:
    return bool(result.is_error and dict((result.meta or {}).get("ekl") or {}).get("unavailable"))


class Upstreams:
    """Several upstreams as one. A call goes to the server its service names; when that one can't answer, to the same
    function on the agent's other servers in turn (`plan`). A listing holds every server's tools (the planned
    server's when two share a name). Resources come only from an MCP server.

    The agent answers while every service can be reached: a server that doesn't answer is passed over when each
    service it serves has another that does, and its tools are listed as it last listed them, so the agent's
    fingerprint doesn't move while it is away. Without a plan, every server must answer."""

    def __init__(self, by_kind: dict[Via, Target], plan: Callable[[str], Plan | None] | None = None) -> None:
        if not by_kind:
            raise ValueError("no upstream server")
        self.by_kind = by_kind
        self.plan: Callable[[str], Plan | None] = plan or (lambda _: None)
        # The services to keep reachable, and the tools each server last listed.
        self.services: Callable[[], list[str]] = list
        self._listed: dict[Via, list[types.Tool]] = {}

    def _steps(self, tool: str) -> Plan:
        planned = self.plan(tool)
        if planned is not None and planned.steps:
            return planned
        first = next((k for k in ORDER if k in self.by_kind), next(iter(self.by_kind)))
        return Plan([(first, tool)])

    async def call(self, tool: str, arguments: dict[str, Any]) -> types.CallToolResult:
        plan = self._steps(tool)
        steps = [(kind, name) for kind, name in plan.steps if kind in self.by_kind]
        if not steps:
            kind = plan.steps[0][0]
            return types.CallToolResult(
                content=[types.TextContent(type="text", text=f"no {kind} server is configured for {tool}")],
                is_error=True,
                meta={"ekl": {"effect": "none"}},
            )
        for n, (kind, name) in enumerate(steps):
            last = n == len(steps) - 1
            try:
                result = await self.by_kind[kind].call(name, arguments)
            except Exception as exc:
                # An external action moves on only from a server it never reached: anywhere else it may have acted.
                if last or not (unreached(exc) or not plan.effects):
                    raise
                log.warning(
                    "%s: %s server unreachable (%s); trying %s", tool, kind, type(exc).__name__, steps[n + 1][0]
                )
                continue
            if _unavailable(result) and not last and not plan.effects:
                log.warning("%s: %s server unavailable; trying %s", tool, kind, steps[n + 1][0])
                continue
            return result
        raise AssertionError("unreachable: the last step returns or raises")

    async def tools(self) -> list[types.Tool]:
        listed = await asyncio.gather(*(t.tools() for t in self.by_kind.values()), return_exceptions=True)
        down: dict[Via, BaseException] = {}
        for kind, answer in zip(self.by_kind, listed, strict=True):
            if isinstance(answer, BaseException):
                down[kind] = answer
            else:
                self._listed[kind] = answer
        if down:
            needed = [self._steps(tool) for tool in self.services()]
            stranded = (
                not needed
                or any(kind not in self._listed for kind in down)
                or any(all(kind in down for kind, _ in plan.steps if kind in self.by_kind) for plan in needed)
            )
            if stranded:
                raise next(iter(down.values()))
        found: dict[str, types.Tool] = {}
        for kind in self.by_kind:
            for tool in self._listed.get(kind, []):
                if tool.name not in found or self._steps(tool.name).steps[0][0] == kind:
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
