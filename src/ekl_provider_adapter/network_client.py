# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""How the agent reaches EuKollab and the providers it calls.

The network's tools are called over MCP at <network>/mcp with the agent-operation credential, never the provider
credential. Providers are called directly at the endpoint and with the access token EuKollab gave for
them. `Network` is the seam the tests replace with `testing.FakeNetwork`.
"""

import json
from dataclasses import dataclass
from typing import Any, Protocol

import httpx2
import mcp_types as types
from mcp import Client
from mcp.client.streamable_http import streamable_http_client

from ekl_provider_adapter.errors import AgentError, ProviderUnavailable, from_network
from ekl_provider_adapter.fingerprint import result_body

PROTOCOL_VERSION = "2026-07-28"
# A long poll waits up to 30 seconds on the network's side.
TIMEOUT_SECONDS = 60


@dataclass(frozen=True)
class ProviderAnswer:
    """A provider's answer to one call: its structured result, whether it is an error, the adapter's _meta.ekl, and
    the body the output fingerprint covers."""

    value: Any
    is_error: bool
    meta: dict[str, Any]
    text: str
    body: Any


class Network(Protocol):
    """`call` runs a network tool as the agent; `call_provider` calls another agent's tool at its endpoint."""

    async def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]: ...

    async def call_provider(
        self, endpoint: str, token: str, tool: str, arguments: dict[str, Any], ekl: dict[str, Any]
    ) -> ProviderAnswer: ...


def _structured(result: types.CallToolResult) -> dict[str, Any]:
    if result.structured_content:
        return dict(result.structured_content)
    for block in result.content:
        text = getattr(block, "text", None)
        if text:
            try:
                parsed = json.loads(text)
            except ValueError:
                continue
            if isinstance(parsed, dict):
                return parsed
    return {}


class McpNetwork:
    """The network over MCP, with the agent-operation credential."""

    def __init__(self, network_url: str, credential: str):
        self.endpoint = network_url.rstrip("/") + "/mcp"
        self.credential = credential

    async def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        http = httpx2.AsyncClient(headers={"Authorization": f"Bearer {self.credential}"}, timeout=TIMEOUT_SECONDS)
        try:
            async with Client(streamable_http_client(self.endpoint, http_client=http), mode=PROTOCOL_VERSION) as c:
                result = await c.call_tool(tool, {k: v for k, v in arguments.items() if v is not None})
        except Exception as exc:  # the transport failed: nothing is known to have happened
            raise ProviderUnavailable(
                f"EuKollab could not be reached ({type(exc).__name__}).", code="SERVICE_UNAVAILABLE"
            ) from exc
        finally:
            await http.aclose()
        answer = _structured(result)
        if not answer.get("ok"):
            error = answer.get("error") or {}
            raise from_network(
                str(error.get("code", "INVALID_RESPONSE")),
                str(error.get("message") or " ".join(getattr(b, "text", "") for b in result.content) or "refused"),
                error.get("detail"),
            )
        data: dict[str, Any] = answer.get("data") or {}
        return data

    async def call_provider(
        self, endpoint: str, token: str, tool: str, arguments: dict[str, Any], ekl: dict[str, Any]
    ) -> ProviderAnswer:
        http = httpx2.AsyncClient(headers={"Authorization": f"Bearer {token}"}, timeout=120)
        sent = False
        try:
            async with Client(streamable_http_client(endpoint, http_client=http)) as client:
                sent = True  # the session is open: from here the call may reach the provider
                result = await client.call_tool(tool, arguments, meta={"ekl": ekl})  # type: ignore[arg-type]  # _meta is an open mapping in MCP
        except AgentError:
            raise
        except Exception as exc:  # no answer: the outcome is unknown, and the same execution ID finds it later
            raise ProviderUnavailable(
                f"The provider could not be reached ({type(exc).__name__}).", detail={"sent": sent}
            ) from exc
        finally:
            await http.aclose()
        meta = dict((result.meta or {}).get("ekl") or {})
        text = " ".join(getattr(c, "text", "") or "" for c in result.content)
        return ProviderAnswer(result.structured_content, bool(result.is_error), meta, text, result_body(result))
