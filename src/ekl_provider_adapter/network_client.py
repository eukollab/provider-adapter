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
from urllib.parse import quote

import httpx2
import mcp_types as types
from mcp import Client
from mcp.client.streamable_http import streamable_http_client

from ekl_provider_adapter.errors import AgentError, ProviderUnavailable, from_network
from ekl_provider_adapter.fingerprint import result_body

PROTOCOL_VERSION = "2026-07-28"
# A long poll waits up to 30 seconds on the network's side.
TIMEOUT_SECONDS = 60
# The relay's answer when a relayed provider is not running: the MCP client reports only "an error response", so the
# code is read from the HTTP answer itself.
PROVIDER_AWAY = "PROVIDER_AWAY"


@dataclass(frozen=True)
class ProviderAnswer:
    """A provider's answer to one call: its structured result, whether it is an error, the adapter's _meta.ekl, and
    the body the output fingerprint covers."""

    value: Any
    is_error: bool
    meta: dict[str, Any]
    text: str
    body: Any


async def _relay_refusal(response: httpx2.Response, refusals: list[str]) -> None:
    """Note the relay's PROVIDER_AWAY, a 503 whose JSON body carries the code."""
    if response.status_code != 503 or not response.headers.get("content-type", "").startswith("application/json"):
        return
    try:
        body = json.loads(await response.aread())
    except (ValueError, httpx2.HTTPError):
        return
    if isinstance(body, dict) and body.get("code") == PROVIDER_AWAY:
        refusals.append(PROVIDER_AWAY)


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
        # The network's own address: its issuer, its key set and its screening rules are found from it.
        self.base_url = network_url.rstrip("/")
        self.endpoint = self.base_url + "/mcp"
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

    async def _files(self, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        """The network's file store (EKP 3.5 §5.5), over HTTPS with the agent-operation credential."""
        try:
            async with httpx2.AsyncClient(
                headers={"Authorization": f"Bearer {self.credential}"}, timeout=TIMEOUT_SECONDS
            ) as http:
                response = await http.request(method, self.base_url + path, json=body)
        except httpx2.HTTPError as exc:
            raise ProviderUnavailable(
                f"EuKollab could not be reached ({type(exc).__name__}).", code="SERVICE_UNAVAILABLE"
            ) from exc
        try:
            answer: dict[str, Any] = response.json()
        except ValueError:
            answer = {}
        if response.status_code >= 400:
            raise from_network(
                str(answer.get("code") or "SERVICE_UNAVAILABLE"), str(answer.get("message") or "refused"), None
            )
        return answer

    async def file_upload(
        self, execution_id: str, receiver: str, sha256: str, size: int, media_type: str
    ) -> dict[str, Any]:
        """An upload for one of a call's files, for the provider it goes to."""
        body = {"execution_id": execution_id, "receiver": receiver, "sha256": sha256, "size": size}
        return await self._files("POST", "/v1/files", body | {"media_type": media_type})

    async def file_download(self, sha256: str, execution_id: str) -> dict[str, Any]:
        """A download for a file a provider sent back with a call's result."""
        return await self._files("GET", f"/v1/files/{sha256}?execution_id={quote(execution_id, safe='')}")

    async def call_provider(
        self, endpoint: str, token: str, tool: str, arguments: dict[str, Any], ekl: dict[str, Any]
    ) -> ProviderAnswer:
        refusals: list[str] = []
        http = httpx2.AsyncClient(
            headers={"Authorization": f"Bearer {token}"},
            timeout=120,
            event_hooks={"response": [lambda response: _relay_refusal(response, refusals)]},
        )
        sent = False
        try:
            async with Client(streamable_http_client(endpoint, http_client=http)) as client:
                sent = True  # the session is open: from here the call may reach the provider
                result = await client.call_tool(tool, arguments, meta={"ekl": ekl})  # type: ignore[arg-type]  # _meta is an open mapping in MCP
        except AgentError:
            raise
        except Exception as exc:  # no answer: the outcome is unknown, and the same execution ID finds it later
            if PROVIDER_AWAY in refusals:
                # Nothing took the call, so it is known not to have run: another provider may take it.
                raise ProviderUnavailable(
                    "The provider isn't running now; try again later.", code=PROVIDER_AWAY, detail={"sent": False}
                ) from exc
            raise ProviderUnavailable(
                f"The provider could not be reached ({type(exc).__name__}).",
                code="SERVICE_UNAVAILABLE",
                detail={"sent": sent},
            ) from exc
        finally:
            await http.aclose()
        meta = dict((result.meta or {}).get("ekl") or {})
        text = " ".join(getattr(c, "text", "") or "" for c in result.content)
        return ProviderAnswer(result.structured_content, bool(result.is_error), meta, text, result_body(result))
