# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The services an agent plans to use, offered to its own code over MCP (/mcp) and A2A (/a2a) on the local API: an
agent framework calls them natively, and the connector chooses and calls the provider as for act("service")."""

import contextlib
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx2
import pytest
from mcp import Client
from mcp.client.streamable_http import streamable_http_client

from ekl_provider_adapter.agent import Agent
from ekl_provider_adapter.agent_journal import AgentJournal
from ekl_provider_adapter.callbacks import local_api
from ekl_provider_adapter.surfaces import uses_of
from ekl_provider_adapter.testing import FakeNetwork

pytestmark = pytest.mark.anyio

SCHEMA = {"type": "object", "properties": {"amount": {"type": "number"}}, "required": ["amount"]}
USES = {"uses": [{"need": "Convert currency", "prefer": "@fx/cap_convert"}, {"need": "Convert currency"}, {}]}


def _network() -> FakeNetwork:
    fake = FakeNetwork(handle="@buyer")
    fake.provider(
        "@fx",
        "cap_convert",
        lambda a: {"converted": a["amount"] * 2},
        summary="convert currency",
        input_schema=SCHEMA,
        example={"amount": 1},
    )
    return fake


@contextlib.asynccontextmanager
async def _local(tmp_path: Path, fake: FakeNetwork) -> AsyncIterator[httpx2.AsyncClient]:
    agent = Agent(fake, AgentJournal(str(tmp_path / "journal.db")))
    app = local_api(agent, uses_of(USES))
    async with app.router.lifespan_context(app):
        transport = httpx2.ASGITransport(app=app)
        async with httpx2.AsyncClient(transport=transport, base_url="http://127.0.0.1") as http:
            yield http


def _outside() -> FakeNetwork:
    # Nothing on the network converts currency; one agent outside it does, and the publisher's rule allows it.
    fake = FakeNetwork(handle="@buyer")
    fake.external_agent("ext_fx", "FX rates", "convert", lambda a: {"converted": a["amount"] * 3}, "convert currency")
    fake.config["external"] = {"rule": "allow", "approved": []}
    return fake


async def test_over_mcp_a_refusal_names_agents_outside_eukollab_and_eukollab_external_runs_one(tmp_path: Path) -> None:
    fake = _outside()
    async with (
        _local(tmp_path, fake) as http,
        Client(streamable_http_client("http://127.0.0.1/mcp", http_client=http)) as client,
    ):
        tools = {t.name: t for t in (await client.list_tools()).tools}
        assert tools["eukollab_external"].input_schema["required"] == ["external_id", "tool", "input"]
        refused = await client.call_tool("convert_currency_2", {"amount": 2})
        assert refused.is_error and refused.structured_content is not None
        assert refused.structured_content["error"] == "NO_PROVIDER"
        outside = refused.structured_content["external"]
        assert outside[0]["external_id"] == "ext_fx" and "eukollab_external" in refused.content[0].text  # type: ignore[union-attr]
        ran = await client.call_tool(
            "eukollab_external", {"external_id": "ext_fx", "tool": "convert", "input": {"amount": 2}}
        )
    assert not ran.is_error and ran.structured_content is not None
    assert ran.structured_content["outcome"] == "succeeded" and ran.structured_content["result"] == {"converted": 6}
    assert fake.external_runs == [{"external_id": "ext_fx", "tool": "convert", "arguments": {"amount": 2}}]


async def test_over_a2a_eukollab_external_runs_an_agent_outside_eukollab(tmp_path: Path) -> None:
    fake = _outside()

    async def send(http: httpx2.AsyncClient, skill: str, data: dict[str, Any]) -> dict[str, Any]:
        message = {"messageId": skill, "role": "ROLE_USER", "parts": [{"data": data}], "metadata": {"skill": skill}}
        sent = await http.post(
            "/a2a", json={"jsonrpc": "2.0", "id": 1, "method": "SendMessage", "params": {"message": message}}
        )
        task: dict[str, Any] = sent.json()["result"]["task"]
        return task

    async with _local(tmp_path, fake) as http:
        refused = await send(http, "convert_currency_2", {"amount": 2})
        ran = await send(
            http, "eukollab_external", {"external_id": "ext_fx", "tool": "convert", "input": {"amount": 2}}
        )
        fake.config["external"] = {"rule": "never", "approved": []}
        never = await send(
            http, "eukollab_external", {"external_id": "ext_fx", "tool": "convert", "input": {"amount": 2}}
        )
    assert refused["status"]["state"] == "TASK_STATE_FAILED"
    assert refused["artifacts"][0]["parts"][0]["data"]["external"][0]["external_id"] == "ext_fx"
    assert ran["status"]["state"] == "TASK_STATE_COMPLETED"
    assert ran["artifacts"][0]["parts"][0]["data"]["result"] == {"converted": 6}
    assert never["status"]["state"] == "TASK_STATE_FAILED" and "POLICY_DENIED" in str(never["status"])


def test_uses_are_named_after_their_need() -> None:
    assert [(u.name, u.prefer) for u in uses_of(USES)] == [
        ("convert_currency", "@fx/cap_convert"),
        ("convert_currency_2", None),
    ]
    assert uses_of(None) == [] and uses_of({"uses": "nope"}) == []
    # The external tool's name stays free.
    assert [u.name for u in uses_of({"uses": [{"need": "EuKollab external"}]})] == ["eukollab_external_2"]


async def test_a_framework_calls_a_planned_service_over_mcp(tmp_path: Path) -> None:
    fake = _network()
    async with (
        _local(tmp_path, fake) as http,
        Client(streamable_http_client("http://127.0.0.1/mcp", http_client=http)) as client,
    ):
        tools = {t.name: t for t in (await client.list_tools()).tools}
        assert set(tools) == {"convert_currency", "convert_currency_2", "eukollab_external"}
        assert tools["convert_currency"].input_schema == SCHEMA  # the preferred provider's
        assert tools["convert_currency_2"].input_schema == {"type": "object"}
        result = await client.call_tool("convert_currency", {"amount": 4})
    assert result.structured_content == {"converted": 8} and not result.is_error
    assert (result.meta or {})["ekl"]["receipt_checked"] is True
    assert fake.providers["@fx/cap_convert"].calls == 1


async def test_a_framework_calls_a_planned_service_over_a2a(tmp_path: Path) -> None:
    fake = _network()
    async with _local(tmp_path, fake) as http:
        card = (await http.get("/a2a/.well-known/agent-card.json")).json()
        assert [s["id"] for s in card["skills"]] == ["convert_currency", "convert_currency_2", "eukollab_external"]
        assert card["supportedInterfaces"][0]["url"] == "http://127.0.0.1/a2a"
        message: dict[str, Any] = {
            "messageId": "m1",
            "role": "ROLE_USER",
            "parts": [{"data": {"amount": 3}}],
            "metadata": {"skill": "convert_currency"},
        }
        sent = await http.post(
            "/a2a", json={"jsonrpc": "2.0", "id": 1, "method": "SendMessage", "params": {"message": message}}
        )
        task = sent.json()["result"]["task"]
        assert task["status"]["state"] == "TASK_STATE_COMPLETED"
        assert task["artifacts"][0]["parts"] == [{"data": {"converted": 6}}, {"text": '{"converted": 6}'}]
        got = await http.post(
            "/a2a", json={"jsonrpc": "2.0", "id": 2, "method": "GetTask", "params": {"id": task["id"]}}
        )
        assert got.json()["result"]["id"] == task["id"]  # A2A 1.0's GetTask answers the Task itself
        legacy = dict(message, kind="message", role="user", parts=[{"kind": "data", "data": {"amount": 1}}])
        old = await http.post(
            "/a2a", json={"jsonrpc": "2.0", "id": 3, "method": "message/send", "params": {"message": legacy}}
        )
        assert old.json()["result"]["status"]["state"] == "completed" and old.json()["result"]["kind"] == "task"
        unnamed = dict(message, metadata={})
        refused = await http.post(
            "/a2a", json={"jsonrpc": "2.0", "id": 4, "method": "SendMessage", "params": {"message": unnamed}}
        )
        assert refused.json()["result"]["task"]["status"]["state"] == "TASK_STATE_REJECTED"
        listed = await http.post("/a2a", json={"jsonrpc": "2.0", "id": 5, "method": "ListTasks", "params": {}})
        assert [t["id"] for t in listed.json()["result"]["tasks"]][-1] == task["id"]
        unknown = await http.post("/a2a", json={"jsonrpc": "2.0", "id": 6, "method": "tasks/nothing"})
        assert unknown.json()["error"]["code"] == -32601


async def test_a_web_page_reaches_neither_surface(tmp_path: Path) -> None:
    async with _local(tmp_path, _network()) as http:
        page = await http.get("/a2a/.well-known/agent-card.json", headers={"Origin": "https://evil.example"})
        assert page.status_code == 403
        posted = await http.post("/mcp", content=b"{}", headers={"Content-Type": "text/plain"})
        assert posted.status_code == 415


async def test_without_planned_services_there_are_no_surfaces(tmp_path: Path) -> None:
    agent = Agent(_network(), AgentJournal(str(tmp_path / "journal.db")))
    app = local_api(agent)
    async with app.router.lifespan_context(app):
        transport = httpx2.ASGITransport(app=app)
        async with httpx2.AsyncClient(transport=transport, base_url="http://127.0.0.1") as http:
            assert (await http.get("/a2a/.well-known/agent-card.json")).status_code == 404
