# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The server declares its EuKollab configuration: what `EuKollab(...)` and `service(...)` put on the
wire is what ekl agent sync reads, and it follows the contract."""

import json
from typing import Any

import httpx2
import jsonschema
import pytest
from mcp.server.mcpserver import MCPServer

from ekl_provider_adapter import EuKollab, service
from tests.conftest import CONTRACTS
from tests.test_contracts import registry

pytestmark = pytest.mark.anyio

PLANS = [{"type": "free", "daily_limit": 200}, {"type": "per_call", "currency": "USD", "price": "0.001"}]
DATA = {"accepted_data_classes": ["public"], "inputs": {"retention": "none"}, "results": {"retention": "none"}}
PAYMENT = {"link": "https://buy.stripe.com/test_holidays", "note": "Packs of 500 checks"}
ENVELOPE = {
    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
    "io.modelcontextprotocol/clientCapabilities": {},
}


def declared_server() -> MCPServer:
    server = MCPServer(
        "holiday-calendar",
        title="Holiday Calendar",
        extensions=[
            EuKollab(mode="active_participant", interests=["calendar"], data=DATA, plans=PLANS, payment=PAYMENT)
        ],
    )

    @server.tool(title="Long weekends", meta=service(class_="read_only", category="calendar", limitations="None."))
    def long_weekends(country: str) -> dict[str, Any]:
        """Runs of 3 or more days off."""
        return {"country": country}

    return server


async def _post(http: httpx2.AsyncClient, method: str, params: dict[str, Any]) -> dict[str, Any]:
    headers = {
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": "2026-07-28",
        "Mcp-Method": method,
    }
    response = await http.post(
        "/mcp", json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, headers=headers
    )
    text = response.text
    body = json.loads(text.split("data:", 1)[1]) if text.startswith("event:") else response.json()
    result: dict[str, Any] = body["result"]
    return result


async def test_the_declarations_are_on_the_wire_and_follow_the_contract() -> None:
    app = declared_server().streamable_http_app()
    async with app.router.lifespan_context(app):
        transport = httpx2.ASGITransport(app=app)
        async with httpx2.AsyncClient(transport=transport, base_url="http://localhost:8000") as http:
            found = await _post(http, "server/discover", {"_meta": ENVELOPE})
            tools = await _post(http, "tools/list", {"_meta": ENVELOPE})
    agent = found["capabilities"]["extensions"]["ai.eukollab/agent"]
    assert agent == {
        "mode": "active_participant",
        "interests": ["calendar"],
        "data_declared": True,
        "payment": PAYMENT,
        "defaults": {"data": DATA, "plans": PLANS},
    }
    [tool] = tools["tools"]
    declared = tool["_meta"]["ai.eukollab/service"]
    assert declared == {"class": "read_only", "category": "calendar", "limitations": "None."}
    schema = json.loads((CONTRACTS / "ekp/1.1/types/agent-declaration.json").read_text())
    jsonschema.Draft202012Validator(schema, registry=registry()).validate({"agent": agent, "service": declared})


def test_data_declared_follows_data_unless_said() -> None:
    assert EuKollab(interests=["x"]).settings() == {"interests": ["x"]}
    assert EuKollab(data=DATA, data_declared=False).settings()["data_declared"] is False
