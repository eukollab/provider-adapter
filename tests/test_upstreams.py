# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Agents that are not MCP servers: an HTTPS API by its OpenAPI document and an A2A agent, each fronted by the
connector, and an agent that is several at once. A consumer's MCP client calls them through the connector's endpoint
as it would any agent, admitted and settled by the network."""

import json
from typing import Any

import httpx2
import mcp_types as types
import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from ekl_provider_adapter.a2a_upstream import (
    A2AUpstream,
    card_url,
    interface,
    message_part,
    parts_result,
    skill_tool,
    state_of,
    task_result,
)
from ekl_provider_adapter.config import load_agent_file
from ekl_provider_adapter.connector import connector_app
from ekl_provider_adapter.fingerprint import new_salt
from ekl_provider_adapter.openapi_upstream import (
    OpenAPIUpstream,
    UnsupportedSpecError,
    base_url,
    int64_numbers,
    operations,
    parse_document,
    safe_integers,
)
from ekl_provider_adapter.upstreams import Plan, Upstreams
from tests.conftest import CONTRACTS

from .a2a_agent import a2a_app
from .test_connector import CREDENTIAL, ENDPOINT, InProcess, confirmed, homes_server, mcp, network_client, served

pytestmark = pytest.mark.anyio

VECTORS = CONTRACTS / "vectors" / "openapi-tools.json"

SPEC: dict[str, Any] = {
    "openapi": "3.1.0",
    "info": {"title": "Shipping", "version": "1"},
    "servers": [{"url": "https://api.ship.test/{version}", "variables": {"version": {"default": "v1"}}}],
    "paths": {
        "/rates/{country}": {
            "parameters": [{"name": "country", "in": "path", "required": True, "schema": {"type": "string"}}],
            "get": {
                "operationId": "get_rate",
                "summary": "The rate to a country.",
                "parameters": [
                    {"name": "weight", "in": "query", "required": True, "schema": {"type": "number"}},
                    {"name": "X-Region", "in": "header", "schema": {"type": "string"}},
                ],
                "responses": {
                    "200": {"content": {"application/json": {"schema": {"$ref": "#/components/schemas/Rate"}}}}
                },
            },
        },
        "/labels": {
            "post": {
                "operationId": "create label",
                "requestBody": {
                    "required": True,
                    "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Label"}}},
                },
                "responses": {"201": {"content": {"application/json": {"schema": {"type": "array"}}}}},
            }
        },
        "/labels/{id}": {
            "delete": {
                "operationId": "delete_label",
                "parameters": [{"name": "id", "in": "path", "required": True, "schema": {"type": "string"}}],
                "responses": {"204": {"description": "gone"}},
            }
        },
        "/upload": {
            "post": {
                "operationId": "upload",
                "requestBody": {"content": {"multipart/form-data": {"schema": {"type": "object"}}}},
                "responses": {"200": {"description": "ok"}},
            }
        },
        "/session": {
            "get": {
                "operationId": "session",
                "parameters": [{"name": "sid", "in": "cookie", "required": True, "schema": {"type": "string"}}],
                "responses": {"200": {"description": "ok"}},
            }
        },
    },
    "components": {
        "schemas": {
            "Rate": {
                "type": "object",
                "properties": {"price": {"type": "number"}, "note": {"type": "string", "nullable": True}},
                "required": ["price"],
            },
            "Label": {"type": "object", "properties": {"to": {"type": "string"}}, "required": ["to"]},
        }
    },
}


def shipping_api() -> Starlette:
    async def rate(request: Request) -> Response:
        if request.path_params["country"] == "xx":
            return JSONResponse({"error": "unknown country"}, status_code=404)
        weight = float(request.query_params["weight"])
        note = request.headers.get("x-region")
        return JSONResponse({"price": round(weight * 2.5, 2), "note": note})

    async def label(request: Request) -> Response:
        body = await request.json()
        return JSONResponse([{"to": body["to"], "tracking": "T1"}], status_code=201)

    async def delete(_: Request) -> Response:
        return Response(status_code=204)

    return Starlette(
        routes=[
            Route("/v1/rates/{country}", rate),
            Route("/v1/labels", label, methods=["POST"]),
            Route("/v1/labels/{id}", delete, methods=["DELETE"]),
        ]
    )


def api_upstream(spec: dict[str, Any] = SPEC) -> OpenAPIUpstream:
    http = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=shipping_api()))
    return OpenAPIUpstream("https://api.ship.test/openapi.json", http=http, spec=spec)


def a2a_upstream() -> A2AUpstream:
    http = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=a2a_app()))
    return A2AUpstream("http://fx.test", http=http, deadline=5)


def test_operations_map_to_tools_as_the_shared_vectors_say() -> None:
    """The adapter and `ekl` map OpenAPI the same way: both are checked against contracts/vectors."""
    vectors = json.loads(VECTORS.read_text())
    for case in vectors["cases"]:
        found, skipped = operations(case["document"])
        assert [op.tool for op in found] == case["tools"], case["name"]
        assert [{"method": s.method, "path": s.path} for s in skipped] == case["skipped"], case["name"]
    for answer in vectors["answers"]:
        assert safe_integers(answer["answer"]) == answer["carried"], answer["name"]
    schema = {"type": "object", "properties": {"id": {"type": ["integer", "string"], "format": "int64"}}}
    assert int64_numbers({"id": "9223372036854775807", "name": "7"}, schema) == {
        "id": 9223372036854775807,
        "name": "7",
    }


def test_operations_name_parameters_and_bodies() -> None:
    found, skipped = operations(SPEC)
    tools = {op.name: op.tool for op in found}
    assert list(tools) == ["get_rate", "create_label", "delete_label"]
    rate = tools["get_rate"]
    assert rate["inputSchema"]["required"] == ["country", "weight"]
    assert rate["outputSchema"]["properties"]["note"]["type"] == ["string", "null"]
    assert rate["annotations"] == {"readOnlyHint": True}
    assert tools["create_label"]["inputSchema"]["properties"]["body"]["required"] == ["to"]
    assert tools["create_label"]["outputSchema"]["required"] == ["result"]  # an array, carried under result
    assert tools["delete_label"]["outputSchema"]["properties"] == {"status": {"type": "integer"}}
    assert tools["delete_label"]["annotations"]["destructiveHint"] is True
    assert {s.path: s.reason for s in skipped} == {
        "/upload": "its request body is not JSON",
        "/session": "it needs a cookie parameter",
    }
    assert base_url(SPEC, None) == "https://api.ship.test/v1"
    # No server: OpenAPI's default "/", the document's own host, as FastAPI writes it; relative ones read against it.
    assert base_url({"paths": {}}, "http://127.0.0.1:9997/openapi.json") == "http://127.0.0.1:9997/"
    assert base_url({"servers": [{"url": "/api/v3"}]}, "https://petstore.test/api/v3/openapi.json") == (
        "https://petstore.test/api/v3"
    )
    assert base_url({"paths": {}}, None) is None  # read from a file: --api-url says where


def test_swagger_2_and_outside_references_are_refused() -> None:
    with pytest.raises(UnsupportedSpecError, match=r"Swagger 2\.0"):
        operations({"swagger": "2.0", "paths": {}})
    outside = {
        "openapi": "3.0.3",
        "paths": {"/a": {"get": {"operationId": "a", "responses": {"200": {"$ref": "other.yaml#/R"}}}}},
    }
    assert operations(outside)[1][0].reason == "the reference other.yaml#/R points outside the document"
    assert parse_document("openapi: 3.0.0\npaths: {}\n") == {"openapi": "3.0.0", "paths": {}}


async def test_an_api_answers_through_the_connector() -> None:
    agent = AGENT_API
    network = confirmed(agent)
    token = network.token("agt_consumer")
    app = connector_app(agent, credential=CREDENTIAL, api=network_client(network), upstream=api_upstream())
    async with served(app, token) as http, mcp(http, "/ekl") as adapter:
        listed = {t.name: t for t in (await adapter.list_tools()).tools}
        assert set(listed) == {"get_rate", "create_label"}
        meta = {"ekl": {"execution_id": "e1", "salt": new_salt()}}
        result = await adapter.call_tool("get_rate", {"country": "de", "weight": 2, "X-Region": "eu"}, meta=meta)
        assert result.structured_content == {"price": 5.0, "note": "eu"}
        assert (result.meta or {})["ekl"]["outcome"] == "succeeded"
        meta = {"ekl": {"execution_id": "e2", "salt": new_salt()}}
        missing = await adapter.call_tool("get_rate", {"country": "xx", "weight": 1}, meta=meta)
        assert missing.is_error and (missing.meta or {})["ekl"]["outcome"] == "failed_before_effect"
        meta = {"ekl": {"execution_id": "e3", "salt": new_salt()}}
        made = await adapter.call_tool("create_label", {"body": {"to": "Berlin"}}, meta=meta)
        assert made.structured_content == {"result": [{"to": "Berlin", "tracking": "T1"}]}
        await app.state.ekl_core.drain()
    assert network.executions[("agt_consumer", "e1")].state == "completed"


async def test_an_a2a_agent_answers_through_the_connector() -> None:
    network = confirmed(AGENT_A2A)
    token = network.token("agt_consumer")
    app = connector_app(AGENT_A2A, credential=CREDENTIAL, api=network_client(network), upstream=a2a_upstream())
    async with served(app, token) as http, mcp(http, "/ekl") as adapter:
        listed = {t.name for t in (await adapter.list_tools()).tools}
        assert listed == {"convert", "clarify", "greet"}
        meta = {"ekl": {"execution_id": "e1", "salt": new_salt()}}
        result = await adapter.call_tool("convert", {"amount": 10, "from": "EUR", "to": "USD"}, meta=meta)
        assert result.structured_content == {"amount": 11.0, "rate": 1.1}
        assert (result.meta or {})["ekl"]["outcome"] == "succeeded"
        meta = {"ekl": {"execution_id": "e2", "salt": new_salt()}}
        asked = await adapter.call_tool("clarify", {"amount": 1, "from": "EUR", "to": "?"}, meta=meta)
        assert asked.is_error and (asked.meta or {})["ekl"]["outcome"] == "failed_before_effect"
        assert "Which currency" in asked.content[0].text  # type: ignore[union-attr]
        # A text skill reads the message's text, as most A2A agents do, and its text answer is the result.
        meta = {"ekl": {"execution_id": "e3", "salt": new_salt()}}
        greeted = await adapter.call_tool("greet", {"text": "hi"}, meta=meta)
        assert greeted.structured_content == {"text": "Hello!\nYou said: hi"}
        assert (greeted.meta or {})["ekl"]["outcome"] == "succeeded"
        await app.state.ekl_core.drain()
    assert network.executions[("agt_consumer", "e1")].state == "completed"
    asked_execution = network.executions[("agt_consumer", "e2")]
    assert asked_execution.outcome == "failed_before_effect" and asked_execution.funding == "released"  # not charged


async def test_a2a_task_states_and_replies() -> None:
    upstream = a2a_upstream()
    failed = await upstream.call("break", {})
    assert failed.is_error and failed.meta is None  # failed after starting: not proven free of effect
    replied = await upstream.call("reply", {})
    assert replied.structured_content == {"pong": True} and not replied.is_error
    unknown = await upstream.call("nothing", {})
    assert unknown.is_error and (unknown.meta or {})["ekl"]["effect"] == "none"
    asked = await upstream.call("clarify", {})
    assert (asked.meta or {})["ekl"]["code"] == "NEEDS_INPUT"


def test_a2a_0_3_cards_and_states() -> None:
    legacy = {"url": "/rpc", "protocolVersion": "0.3.0", "skills": []}
    assert interface(legacy, "https://a.test/.well-known/agent-card.json") == ("https://a.test/rpc", "0.3.0")
    assert card_url("https://a.test") == "https://a.test/.well-known/agent-card.json"
    assert card_url("https://a.test/card.json") == "https://a.test/card.json"
    assert state_of({"status": {"state": "input-required"}}) == "input_required"
    assert state_of({"status": {"state": "TASK_STATE_INPUT_REQUIRED"}}) == "input_required"
    done = task_result(
        {
            "status": {"state": "completed"},
            "artifacts": [{"parts": [{"kind": "data", "data": {"ok": 1}}, {"kind": "text", "text": "fine"}]}],
        }
    )
    assert done.structured_content == {"ok": 1} and done.content[0].text == "fine"  # type: ignore[union-attr]
    rejected = task_result({"status": {"state": "rejected"}})
    assert rejected.is_error and (rejected.meta or {})["ekl"]["effect"] == "none"


async def test_one_agent_several_servers() -> None:
    """An MCP server and an HTTPS API behind one connector: each tool goes to the server its capability names."""
    network = confirmed(AGENT_BOTH)
    token = network.token("agt_consumer")
    both = Upstreams({"mcp": InProcess(homes_server()), "openapi": api_upstream()})
    app = connector_app(AGENT_BOTH, credential=CREDENTIAL, api=network_client(network), upstream=both)
    async with served(app, token) as http, mcp(http, "/ekl") as adapter:
        assert {t.name for t in (await adapter.list_tools()).tools} == {"search_active_listings", "get_rate"}
        meta = {"ekl": {"execution_id": "e1", "salt": new_salt()}}
        rate = await adapter.call_tool("get_rate", {"country": "de", "weight": 1}, meta=meta)
        assert rate.structured_content == {"price": 2.5, "note": None}
        meta = {"ekl": {"execution_id": "e2", "salt": new_salt()}}
        homes = await adapter.call_tool("search_active_listings", {"area_ref": "x"}, meta=meta)
        assert (homes.structured_content or {})["area"] == "x"
        await app.state.ekl_core.drain()


def test_upstream_kinds_and_via_are_checked() -> None:
    with pytest.raises(ValueError, match="upstream names no server"):
        load_agent_file(AGENT_API | {"upstream": {}})
    with pytest.raises(ValueError, match="reached by a2a, but upstream names no such server"):
        load_agent_file(AGENT_API | {"capabilities": [{"tool": "get_rate", "class": "read_only", "via": "a2a"}]})
    file = load_agent_file(AGENT_BOTH)
    assert [file.via(c) for c in file.capabilities] == ["mcp", "openapi"]
    single = load_agent_file(AGENT_API)
    assert single.via(single.capabilities[0]) == "openapi"


BASE: dict[str, Any] = {
    "ekl": "1.0",
    "mode": "service_provider",
    "execution_mode": "storage_free",
    "agent": {"handle": "@ship", "display_name": "Ship", "purpose": "Shipping rates"},
    "adapter": {"endpoint": ENDPOINT},
}
AGENT_API = BASE | {
    "upstream": {"openapi_url": "https://api.ship.test/openapi.json"},
    "capabilities": [
        {"tool": "get_rate", "class": "read_only"},
        {"tool": "create_label", "class": "transformation"},
    ],
}
AGENT_A2A = BASE | {
    "upstream": {"a2a_url": "http://fx.test"},
    "capabilities": [
        {
            "tool": "convert",
            "class": "read_only",
            "input_schema": {
                "type": "object",
                "properties": {"amount": {"type": "number"}, "from": {"type": "string"}, "to": {"type": "string"}},
                "required": ["amount", "from", "to"],
            },
            "output_schema": {
                "type": "object",
                "properties": {"amount": {"type": "number"}, "rate": {"type": "number"}},
                "required": ["amount", "rate"],
            },
        },
        {"tool": "clarify", "class": "read_only"},
        {"tool": "greet", "class": "read_only"},
    ],
}
AGENT_BOTH = BASE | {
    "upstream": {"mcp_url": "http://127.0.0.1/mcp", "openapi_url": "https://api.ship.test/openapi.json"},
    "capabilities": [
        {"tool": "search_active_listings", "class": "read_only"},
        {"tool": "get_rate", "class": "read_only", "via": "openapi"},
    ],
}


def test_skills_map_to_tools_as_the_shared_vectors_say() -> None:

    vectors = json.loads((VECTORS.parent / "a2a-tools.json").read_text())
    for case in vectors["cases"]:
        assert [skill_tool(s, case["card"]) for s in case["card"]["skills"]] == case["tools"], case["name"]
    for call in vectors["calls"]:
        assert message_part(call["arguments"], call["text"], legacy=False) == call["sent_parts"][0], call["name"]
        answered = parts_result(call["answer_parts"], call["text"])
        assert answered.structured_content == call["structured"], call["name"]


async def test_an_api_that_does_not_answer_lists_nothing_though_its_document_is_held() -> None:
    """The document held (from a file, or read a minute ago) says nothing of the API: listing asks the API itself, with
    any answer counting, so an agent whose API is down is offline."""
    assert [t.name for t in await api_upstream().tools()] == ["get_rate", "create_label", "delete_label"]

    def refused(request: httpx2.Request) -> httpx2.Response:
        raise httpx2.ConnectError("connection refused", request=request)

    down = OpenAPIUpstream(
        "https://api.ship.test/openapi.json",
        http=httpx2.AsyncClient(transport=httpx2.MockTransport(refused)),
        spec=SPEC,
    )
    with pytest.raises(httpx2.ConnectError):
        await down.tools()


class Answers:
    """A server whose calls are scripted: each tool answers, fails with an exception, or says it is unavailable."""

    def __init__(self, tools: dict[str, Any], listed: list[str] | None = None) -> None:
        self.answers = tools
        self.calls: list[str] = []
        self.listing: list[str] | BaseException = listed if listed is not None else list(tools)

    async def call(self, tool: str, arguments: dict[str, Any]) -> types.CallToolResult:
        del arguments
        self.calls.append(tool)
        answer = self.answers[tool]
        if isinstance(answer, BaseException):
            raise answer
        if answer == "unavailable":
            return types.CallToolResult(
                content=[types.TextContent(type="text", text="HTTP 503")],
                is_error=True,
                meta={"ekl": {"unavailable": True}},
            )
        return types.CallToolResult(
            content=[types.TextContent(type="text", text=answer)], structured_content={"by": answer}
        )

    async def tools(self) -> list[types.Tool]:
        if isinstance(self.listing, BaseException):
            raise self.listing
        return [types.Tool(name=n, input_schema={"type": "object"}) for n in self.listing]

    async def list(self) -> list[types.Resource]:
        return []

    async def read(self, uri: str) -> types.ReadResourceResult:
        raise LookupError(uri)


REFUSED_CONNECTION = httpx2.ConnectError("connection refused")


async def test_a_service_falls_back_to_the_same_function_on_another_server() -> None:
    mcp_server = Answers({"convert": REFUSED_CONNECTION})
    a2a_agent = Answers({"convert-currency": "a2a"})
    both = Upstreams({"mcp": mcp_server, "a2a": a2a_agent})
    both.plan = lambda _: Plan([("mcp", "convert"), ("a2a", "convert-currency")])
    result = await both.call("convert", {"amount": 1})
    assert result.structured_content == {"by": "a2a"}
    assert (mcp_server.calls, a2a_agent.calls) == (["convert"], ["convert-currency"])
    # A read-only service also moves on from a server that answered it can't answer now, or failed on the way.
    mcp_server.answers["convert"] = "unavailable"
    assert (await both.call("convert", {})).structured_content == {"by": "a2a"}
    mcp_server.answers["convert"] = httpx2.ReadTimeout("no answer")
    assert (await both.call("convert", {})).structured_content == {"by": "a2a"}


async def test_an_external_action_moves_on_only_from_a_server_it_never_reached() -> None:
    mcp_server = Answers({"send": REFUSED_CONNECTION})
    api = Answers({"send_message": "api"})
    both = Upstreams({"mcp": mcp_server, "openapi": api})
    both.plan = lambda _: Plan([("mcp", "send"), ("openapi", "send_message")], effects=True)
    assert (await both.call("send", {})).structured_content == {"by": "api"}
    # Reached and then lost, or answered unavailable, the message may have gone: no second sending.
    mcp_server.answers["send"] = httpx2.ReadTimeout("no answer")
    with pytest.raises(httpx2.ReadTimeout):
        await both.call("send", {})
    mcp_server.answers["send"] = "unavailable"
    assert (await both.call("send", {})).is_error
    assert api.calls == ["send_message"]


async def test_the_agent_answers_while_every_service_has_a_server_that_does() -> None:
    mcp_server = Answers({"convert": "mcp", "quote": "mcp"})
    a2a_agent = Answers({"convert-currency": "a2a"})
    both = Upstreams({"mcp": mcp_server, "a2a": a2a_agent})
    plans = {"convert": Plan([("mcp", "convert"), ("a2a", "convert-currency")]), "quote": Plan([("mcp", "quote")])}
    both.plan = plans.get
    both.services = lambda: ["convert"]
    listed = {t.name for t in await both.tools()}
    assert listed == {"convert", "quote", "convert-currency"}
    # The MCP server goes away: convert still has A2A, so the agent answers, its listing as the server last gave it.
    mcp_server.listing = REFUSED_CONNECTION
    assert {t.name for t in await both.tools()} == listed
    # A service with no other server makes the agent not answer, as does a server it never heard from.
    both.services = lambda: ["convert", "quote"]
    with pytest.raises(httpx2.ConnectError):
        await both.tools()
    fresh = Upstreams({"mcp": Answers({}, listed=[]), "a2a": a2a_agent})
    fresh.by_kind["mcp"].listing = REFUSED_CONNECTION  # type: ignore[attr-defined]
    fresh.plan, fresh.services = plans.get, lambda: ["convert"]
    with pytest.raises(httpx2.ConnectError):
        await fresh.tools()


def test_also_names_the_agents_other_servers_once() -> None:
    also = {"tool": "get_rate", "class": "read_only", "via": "openapi"}
    load_agent_file(AGENT_BOTH | {"capabilities": [also | {"also": [{"via": "mcp", "tool": "rate"}]}]})
    with pytest.raises(ValueError, match="never its own"):
        load_agent_file(AGENT_BOTH | {"capabilities": [also | {"also": [{"via": "openapi", "tool": "x"}]}]})
    with pytest.raises(ValueError, match="also names a2a, but upstream names no such server"):
        load_agent_file(AGENT_BOTH | {"capabilities": [also | {"also": [{"via": "a2a", "tool": "x"}]}]})
