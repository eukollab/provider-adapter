# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""attach and the connector, end to end over HTTP: a consumer's MCP client, the adapter's endpoint, the server's
own endpoint, and the fake network served over HTTP so the Provider API client is exercised too."""

import asyncio
import base64
import contextlib
import gzip
import hashlib
import hmac
import importlib
import json
import os
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx2
import pytest
from mcp import Client
from mcp.client.streamable_http import streamable_http_client
from mcp.server.mcpserver import MCPServer
from starlette.applications import Starlette

from ekl_provider_adapter import attach, connector
from ekl_provider_adapter.api import HttpProviderApi
from ekl_provider_adapter.attach import local_file
from ekl_provider_adapter.config import strip_local
from ekl_provider_adapter.conformance.network import FakeNetwork, ToolRules, http_app
from ekl_provider_adapter.connector import Upstream, connector_app
from ekl_provider_adapter.core import Settings
from ekl_provider_adapter.fingerprint import fingerprint, new_salt

pytestmark = pytest.mark.anyio
# The module, not the function the package exports under the same name.
ATTACH_MODULE = importlib.import_module("ekl_provider_adapter.attach")

CREDENTIAL = "eklp_test"
ENDPOINT = "http://127.0.0.1/ekl"


def homes_server() -> MCPServer:
    server = MCPServer("homes")

    @server.tool()
    def search_active_listings(area_ref: str, min_beds: int = 0) -> dict[str, Any]:
        """Active listings in an area."""
        return {"area": area_ref, "listings": [f"{min_beds}-bed home"]}

    @server.tool()
    def internal_admin() -> str:
        """Not offered on the network."""
        return "secret"

    return server


AGENT = {
    "ekl": "1.0",
    "mode": "service_provider",
    "execution_mode": "storage_free",
    "agent": {"handle": "@homes", "display_name": "Homes", "purpose": "Home search"},
    "adapter": {"endpoint": ENDPOINT},
    "capabilities": [{"tool": "search_active_listings", "class": "read_only"}],
}


def confirmed(agent: dict[str, Any]) -> FakeNetwork:
    """A network on which the publisher has confirmed this agent.json."""
    network = FakeNetwork()
    network.set_configuration(strip_local(agent))
    return network


def network_client(network: FakeNetwork) -> HttpProviderApi:
    transport = httpx2.ASGITransport(app=http_app(network, CREDENTIAL))
    return HttpProviderApi("http://network", CREDENTIAL, http=httpx2.AsyncClient(transport=transport))


@contextlib.asynccontextmanager
async def served(app: Starlette, token: str | None) -> AsyncIterator[httpx2.AsyncClient]:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    async with app.router.lifespan_context(app):
        transport = httpx2.ASGITransport(app=app)
        async with httpx2.AsyncClient(transport=transport, base_url="http://127.0.0.1", headers=headers) as http:
            yield http


@contextlib.asynccontextmanager
async def mcp(http: httpx2.AsyncClient, path: str) -> AsyncIterator[Client]:
    async with Client(streamable_http_client(f"http://127.0.0.1{path}", http_client=http)) as client:
        yield client


async def test_attach_serves_the_adapter_beside_the_existing_endpoint() -> None:
    network = confirmed(AGENT)
    token = network.token("agt_consumer")
    app = attach(homes_server(), AGENT, credential=CREDENTIAL, api=network_client(network), host="0.0.0.0")
    async with served(app, token) as http:
        async with mcp(http, "/mcp") as existing:
            names = {t.name for t in (await existing.list_tools()).tools}
            assert names == {"search_active_listings", "internal_admin"}  # existing clients see no change
        async with mcp(http, "/ekl") as adapter:
            listed = (await adapter.list_tools()).tools
            assert [t.name for t in listed] == ["search_active_listings"]  # only what agent.json offers
            salt = new_salt()
            arguments = {"area_ref": "loc:ca:dublin", "min_beds": 3}
            meta = {"ekl": {"execution_id": "e1", "salt": salt}}
            result = await adapter.call_tool("search_active_listings", arguments, meta=meta)
            assert result.structured_content == {"area": "loc:ca:dublin", "listings": ["3-bed home"]}
            assert (result.meta or {})["ekl"]["outcome"] == "succeeded"
            refused = await adapter.call_tool("internal_admin", {}, meta=meta)
            assert refused.is_error and (refused.meta or {})["ekl"]["code"] == "NOT_LISTED"
            challenge = "cd" * 32
            proven = await adapter.list_tools(meta={"ekl": {"challenge": challenge}})
            key = hashlib.sha256(CREDENTIAL.encode()).hexdigest().encode()
            expected = hmac.new(key, (challenge + ENDPOINT).encode(), hashlib.sha256)
            assert (proven.meta or {})["ekl"]["proof"] == expected.hexdigest()
            assert "proof" not in ((await adapter.list_tools()).meta or {}).get("ekl", {})
        core = app.state.ekl_core
        await core.drain()
    execution = network.executions[("agt_consumer", "e1")]
    assert execution.state == "completed" and execution.input_fingerprint == fingerprint(salt, arguments)


async def test_without_a_token_nothing_reaches_the_network() -> None:
    network = confirmed(AGENT)
    app = attach(homes_server(), AGENT, credential=CREDENTIAL, api=network_client(network))
    async with served(app, None) as http, mcp(http, "/ekl") as adapter:
        meta = {"ekl": {"execution_id": "e1", "salt": new_salt()}}
        result = await adapter.call_tool("search_active_listings", {"area_ref": "x"}, meta=meta)
    assert result.is_error and network.calls["admit"] == 0


async def test_metrics() -> None:
    network = confirmed(AGENT)
    app = attach(homes_server(), AGENT, credential=CREDENTIAL, api=network_client(network))
    async with served(app, None) as http:
        assert (await http.get("/ekl/metrics")).status_code == 401
        found = await http.get("/ekl/metrics", headers={"Authorization": f"Bearer {CREDENTIAL}"})
        assert found.status_code == 200 and "outbox_depth" in found.json()
        assert (await http.get("/.well-known/ekl-claim")).status_code == 404  # no claim code: the adapter proves it


async def test_the_adapter_announces_its_file_and_serves_once_confirmed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """From EKL_AGENT_FILE, with ${NAME} expanded and local settings stripped, while the endpoint already lists
    its tools and answers the challenge; calls wait for the publisher's confirmation."""
    network = FakeNetwork()
    token = network.token("agt_consumer")
    file = tmp_path / "agent.json"
    file.write_text(json.dumps(AGENT | {"adapter": {"endpoint": "${PUBLIC_MCP}", "path": "/ekl"}}))
    monkeypatch.setenv("EKL_AGENT_FILE", str(file))
    monkeypatch.setenv("PUBLIC_MCP", ENDPOINT)
    fast = Settings(announce_backoff=0.01)
    app = attach(homes_server(), credential=CREDENTIAL, api=network_client(network), settings=fast, host="0.0.0.0")
    async with served(app, token) as http, mcp(http, "/ekl") as adapter:
        for _ in range(100):
            if network.announcements:
                break
            await asyncio.sleep(0.01)
        (sent,) = network.announcements
        assert sent == {"endpoint": ENDPOINT, "config": strip_local(AGENT)}
        assert app.state.ekl_core.announcement.state == "waiting_for_review"
        challenge = "ab" * 32
        listed = await adapter.list_tools(meta={"ekl": {"challenge": challenge}})
        assert [t.name for t in listed.tools] == ["search_active_listings"]
        assert (listed.meta or {})["ekl"]["proof"] == app.state.ekl_core.answer_challenge(challenge)
        meta = {"ekl": {"execution_id": "e1", "salt": new_salt()}}
        waiting = await adapter.call_tool("search_active_listings", {"area_ref": "x"}, meta=meta)
        assert (waiting.meta or {})["ekl"]["code"] == "NOT_CONFIGURED" and network.calls["admit"] == 0
        network.confirm()
        served_now = await adapter.call_tool("search_active_listings", {"area_ref": "x"}, meta=meta)
        assert (served_now.meta or {})["ekl"]["outcome"] == "succeeded"
        await app.state.ekl_core.drain()


async def test_without_the_agent_key_attach_is_the_servers_own_app(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tests, local runs and environments without the key keep the server exactly as it is: no /ekl, nothing
    announced, and the agent's actions say it isn't enabled."""
    monkeypatch.delenv("EKL_PROVIDER_CREDENTIAL", raising=False)
    app = attach(homes_server(), host="0.0.0.0")
    assert not app.agent.enabled
    async with served(app, None) as http:
        async with mcp(http, "/mcp") as own:
            assert "internal_admin" in [t.name for t in (await own.list_tools()).tools]
        assert (await http.get("/ekl/metrics")).status_code == 404
    assert not hasattr(app.state, "ekl_core")


async def test_without_agent_json_the_adapter_lists_what_ekl_announced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ekl agent sync announced the file from the publisher's machine; the deployed adapter has none. The review's
    challenge finds the announced tools and the proof binds the announced endpoint; once confirmed, calls run."""
    for name in ("EKL_AGENT_FILE", "EKL_AGENT_JSON", "EKL_ADAPTER_ENDPOINT"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)
    network = FakeNetwork()
    token = network.token("agt_consumer")
    await network.announce(ENDPOINT, strip_local(AGENT))
    app = attach(homes_server(), credential=CREDENTIAL, api=network_client(network))
    core = app.state.ekl_core
    async with served(app, token) as http, mcp(http, "/ekl") as adapter:
        challenge = "cd" * 32
        listed = await adapter.list_tools(meta={"ekl": {"challenge": challenge}})
        assert [t.name for t in listed.tools] == ["search_active_listings"]
        assert core.endpoint == ENDPOINT
        assert (listed.meta or {})["ekl"]["proof"] == core.answer_challenge(challenge)
        assert len(network.announcements) == 1  # ekl's; the adapter has nothing to announce
        network.confirm()
        meta = {"ekl": {"execution_id": "e1", "salt": new_salt()}}
        result = await adapter.call_tool("search_active_listings", {"area_ref": "x"}, meta=meta)
        assert (result.meta or {})["ekl"]["outcome"] == "succeeded"
        await core.drain()


async def test_a_run_without_its_own_address_never_announces(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A local run with EKL_ADAPTER_ENDPOINT empty, as ekl new agent writes it, announces nothing, even after
    agent.json changes and though the network knows the deployed address: that address isn't this process's."""
    network = confirmed(AGENT)
    file = tmp_path / "agent.json"
    file.write_text(json.dumps(AGENT | {"adapter": {"endpoint": "${EKL_ADAPTER_ENDPOINT}"}}))
    monkeypatch.setenv("EKL_AGENT_FILE", str(file))
    monkeypatch.setenv("EKL_ADAPTER_ENDPOINT", "")
    app = attach(homes_server(), credential=CREDENTIAL, api=network_client(network))
    core = app.state.ekl_core
    await core.load_config()
    assert core.endpoint == ENDPOINT  # the confirmed address still serves the challenge and the path
    assert await core.announce_if_changed()
    file.write_text(
        json.dumps(AGENT | {"adapter": {"endpoint": "${EKL_ADAPTER_ENDPOINT}"}, "mode": "active_participant"})
    )
    assert await core.announce_if_changed()
    assert network.calls["announce"] == 0 and not network.announcements


async def test_announcing_retries_while_the_network_is_unreachable() -> None:
    network = FakeNetwork()
    network.faults.unavailable = True
    fast = Settings(announce_backoff=0.01)
    app = attach(homes_server(), AGENT, credential=CREDENTIAL, api=network_client(network), settings=fast)
    async with served(app, None):
        await asyncio.sleep(0.05)
        assert network.calls["announce"] >= 2 and not network.announcements
        network.faults.unavailable = False
        for _ in range(100):
            if network.announcements:
                break
            await asyncio.sleep(0.01)
    assert len(network.announcements) == 1


async def test_the_connector_reaches_tools_over_mcp() -> None:
    network = confirmed(AGENT)
    network.rules["search_active_listings"] = ToolRules(repeat="status_only")
    token = network.token("agt_consumer")
    upstream_app = homes_server().streamable_http_app(host="0.0.0.0", stateless_http=True, json_response=True)
    async with upstream_app.router.lifespan_context(upstream_app):
        upstream_http = httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=upstream_app), base_url="http://127.0.0.1"
        )
        upstream = Upstream("http://127.0.0.1/mcp", http=upstream_http)
        app = connector_app(
            AGENT | {"upstream": {"mcp_url": "http://127.0.0.1/mcp"}},
            credential=CREDENTIAL,
            api=network_client(network),
            upstream=upstream,
        )
        async with served(app, token) as http, mcp(http, "/ekl") as adapter:
            assert [t.name for t in (await adapter.list_tools()).tools] == ["search_active_listings"]
            meta = {"ekl": {"execution_id": "e1", "salt": new_salt()}}
            result = await adapter.call_tool("search_active_listings", {"area_ref": "x"}, meta=meta)
            assert result.structured_content == {"area": "x", "listings": ["0-bed home"]}
            await app.state.ekl_core.drain()
            again = await adapter.call_tool("search_active_listings", {"area_ref": "x"}, meta=meta)
            assert (again.meta or {})["ekl"]["code"] == "RESULT_UNAVAILABLE"
        await upstream_http.aclose()
    assert network.runs("agt_consumer", "e1") == 1


async def test_the_connector_passes_arguments_through_unchanged() -> None:
    """What the consumer sends is what the server receives: the same keys and the same text, "#" and unit included,
    with nothing of _meta.ekl added to the arguments."""
    received: list[dict[str, Any]] = []
    server = MCPServer("homes")

    @server.tool()
    def get_listing_details(address: str | None = None, listing_ref: str | None = None) -> dict[str, Any]:
        """Everything about one active home."""
        received.append({"address": address, "listing_ref": listing_ref})
        return {"match_status": "found", "address": address}

    agent = AGENT | {"capabilities": [{"tool": "get_listing_details", "class": "read_only"}]}
    network = confirmed(agent)
    token = network.token("agt_consumer")
    upstream_app = server.streamable_http_app(host="0.0.0.0", stateless_http=True, json_response=True)
    async with upstream_app.router.lifespan_context(upstream_app):
        upstream_http = httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=upstream_app), base_url="http://127.0.0.1"
        )
        app = connector_app(
            agent | {"upstream": {"mcp_url": "http://127.0.0.1/mcp"}},
            credential=CREDENTIAL,
            api=network_client(network),
            upstream=Upstream("http://127.0.0.1/mcp", http=upstream_http),
        )
        address = "200 Brannan St #114, San Francisco, CA"
        async with served(app, token) as http, mcp(http, "/ekl") as adapter:
            meta = {"ekl": {"execution_id": "e-args", "salt": new_salt()}}
            result = await adapter.call_tool("get_listing_details", {"address": address}, meta=meta)
            assert result.structured_content == {"match_status": "found", "address": address}
            await app.state.ekl_core.drain()
        await upstream_http.aclose()
    assert received == [{"address": address, "listing_ref": None}]


def widget_server() -> MCPServer:
    """A server with an Apps SDK widget on its offered tool, another on a tool it does not offer, and a resource no
    tool names."""
    server = MCPServer("homes")
    meta = {"openai/outputTemplate": "ui://widget/listings.html", "openai/widgetAccessible": True, "private/x": 1}

    @server.tool(meta=meta)
    def search_active_listings(area_ref: str) -> dict[str, Any]:
        """Active listings in an area."""
        return {"area": area_ref}

    @server.tool(meta={"ui": {"resourceUri": "ui://widget/admin.html"}})
    def internal_admin() -> str:
        """Not offered on the network."""
        return "secret"

    @server.resource("ui://widget/listings.html", mime_type="text/html+skybridge")
    def listings_widget() -> str:
        return "<div id=root></div>"

    @server.resource("ui://widget/admin.html", mime_type="text/html;profile=mcp-app")
    def admin_widget() -> str:
        return "<div>admin</div>"

    @server.resource("file://secrets.txt")
    def secrets_file() -> str:
        return "not for consumers"

    return server


async def test_offered_tools_keep_their_widget_and_only_those_widgets_are_served() -> None:
    """Apps SDK and MCP Apps metadata reaches consumers unchanged, the template a listed tool names can be read, and
    nothing else of the server's resources can, whether the adapter is attached or a connector."""
    network = confirmed(AGENT)
    token = network.token("agt_consumer")
    attached = attach(widget_server(), AGENT, credential=CREDENTIAL, api=network_client(network), host="0.0.0.0")
    upstream_app = widget_server().streamable_http_app(host="0.0.0.0", stateless_http=True, json_response=True)
    async with upstream_app.router.lifespan_context(upstream_app):
        upstream_http = httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=upstream_app), base_url="http://127.0.0.1"
        )
        connected = connector_app(
            AGENT | {"upstream": {"mcp_url": "http://127.0.0.1/mcp"}},
            credential=CREDENTIAL,
            api=network_client(network),
            upstream=Upstream("http://127.0.0.1/mcp", http=upstream_http),
        )
        for app in (attached, connected):
            async with served(app, token) as http, mcp(http, "/ekl") as adapter:
                (tool,) = (await adapter.list_tools()).tools
                assert tool.meta == {
                    "openai/outputTemplate": "ui://widget/listings.html",
                    "openai/widgetAccessible": True,
                }
                listed = [str(r.uri) for r in (await adapter.list_resources()).resources]
                assert listed == ["ui://widget/listings.html"]
                read = await adapter.read_resource("ui://widget/listings.html")
                assert read.contents[0].text == "<div id=root></div>"  # type: ignore[union-attr]
                assert read.contents[0].mime_type == "text/html+skybridge"
                for hidden in ("ui://widget/admin.html", "file://secrets.txt"):
                    with pytest.raises(Exception, match="resource not found"):
                        await adapter.read_resource(hidden)
        await upstream_http.aclose()


def test_agent_json_comes_from_the_argument_then_the_named_file_then_the_environment_then_here(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def handled(handle: str) -> dict[str, Any]:
        return AGENT | {"agent": {**AGENT["agent"], "handle": handle}}

    def loaded(given: dict[str, Any] | None = None) -> str:
        found = local_file(given)
        assert found is not None
        return str(found.config["agent"]["handle"])

    monkeypatch.chdir(tmp_path)
    for name in ("EKL_AGENT_FILE", "EKL_AGENT_JSON"):
        monkeypatch.delenv(name, raising=False)
    assert local_file(None) is None
    (tmp_path / "agent.json").write_text(json.dumps(handled("@here")))
    assert loaded() == "@here"
    text = json.dumps(handled("@inline"))
    for given in (text, base64.b64encode(text.encode()).decode(), base64.urlsafe_b64encode(text.encode()).decode()):
        monkeypatch.setenv("EKL_AGENT_JSON", given)
        assert loaded() == "@inline"
        assert local_file(None).path is None  # type: ignore[union-attr]  # not watched: a redeploy brings a new value
    (tmp_path / "named.json").write_text(json.dumps(handled("@named")))
    monkeypatch.setenv("EKL_AGENT_FILE", str(tmp_path / "named.json"))
    assert loaded() == "@named"
    assert loaded(handled("@given")) == "@given"
    monkeypatch.delenv("EKL_AGENT_FILE")
    for broken in ("not base64 or json!", base64.b64encode(b"[1]").decode(), "{oops"):
        monkeypatch.setenv("EKL_AGENT_JSON", broken)
        with pytest.raises(ValueError, match="EKL_AGENT_JSON"):
            local_file(None)


async def test_the_connector_runs_from_environment_variables_alone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One published image serves any agent: agent.json, the key, the server's and its own address all come from the
    environment, and GET /healthz answers load balancers without credentials or details."""
    monkeypatch.chdir(tmp_path)  # no agent.json here
    monkeypatch.delenv("EKL_AGENT_FILE", raising=False)
    deployed = AGENT | {"adapter": {"endpoint": "http://localhost:8001/ekl"}, "upstream": {"mcp_url": "${UP}"}}
    monkeypatch.setenv("EKL_AGENT_JSON", base64.b64encode(json.dumps(deployed).encode()).decode())
    monkeypatch.setenv("UP", "http://127.0.0.1/mcp")
    monkeypatch.setenv("EKL_PROVIDER_CREDENTIAL", CREDENTIAL)
    monkeypatch.setenv("EKL_NETWORK_URL", "http://network")
    monkeypatch.setenv("EKL_UPSTREAM_MCP_URL", "http://127.0.0.1/mcp")
    monkeypatch.setenv("EKL_ADAPTER_ENDPOINT", ENDPOINT)
    network = FakeNetwork()
    upstream_app = homes_server().streamable_http_app(host="0.0.0.0", stateless_http=True, json_response=True)
    async with upstream_app.router.lifespan_context(upstream_app):
        upstream_http = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=upstream_app))
        fast = Settings(announce_backoff=0.01)
        app = connector_app(
            api=network_client(network), upstream=Upstream("http://127.0.0.1/mcp", http=upstream_http), settings=fast
        )
        async with served(app, None) as http:
            health = await http.get("/healthz")
            assert health.status_code == 200 and health.text == "ok"
            for _ in range(100):
                if network.announcements:
                    break
                await asyncio.sleep(0.01)
        await upstream_http.aclose()
    (sent,) = network.announcements
    assert sent["endpoint"] == ENDPOINT and sent["config"]["adapter"]["endpoint"] == ENDPOINT
    assert sent["config"]["agent"]["handle"] == "@homes" and "upstream" not in sent["config"]


def test_the_connector_image_needs_no_build_per_agent() -> None:
    dockerfile = (Path(__file__).resolve().parents[1] / "Dockerfile").read_text()
    assert dockerfile.rstrip().endswith('CMD ["ekl-provider-connector"]')
    assert "agent.json ." not in dockerfile and "/healthz" in dockerfile
    for variable in (
        "EKL_AGENT_JSON",
        "EKL_PROVIDER_CREDENTIAL",
        "EKL_NETWORK_URL",
        "EKL_UPSTREAM_MCP_URL",
        "EKL_ADAPTER_ENDPOINT",
        "EKL_AGENT_CREDENTIAL",
        "EKL_CALLBACK_URL",
        "EKL_ADAPTER_STORAGE",
        "PORT",
    ):
        assert variable in dockerfile


async def test_a_server_whose_agent_json_doesnt_validate_still_serves_its_own_endpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """ekl agent sync reads the running server to fix agent.json, so a bad file must not stop it."""
    monkeypatch.chdir(tmp_path)
    for name in ("EKL_AGENT_FILE", "EKL_AGENT_JSON"):
        monkeypatch.delenv(name, raising=False)
    paid = {**AGENT["capabilities"][0], "plans": [{"type": "per_call", "currency": "USD", "price": "0.01"}]}
    (tmp_path / "agent.json").write_text(json.dumps(AGENT | {"capabilities": [paid]}))  # storage_free_accepted missing
    app = attach(homes_server(), credential=CREDENTIAL, network_url="http://network")
    assert "agent.json doesn't validate" in caplog.text and "storage_free_accepted" in caplog.text
    async with app.router.lifespan_context(app):
        transport = httpx2.ASGITransport(app=app)
        async with (
            httpx2.AsyncClient(transport=transport, base_url="http://localhost:8000") as http,
            Client(streamable_http_client("http://localhost:8000/mcp", http_client=http)) as client,
        ):
            assert [t.name for t in (await client.list_tools()).tools]


def test_the_connector_runs_the_callbacks_itself(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A deployed connector needs no second service for its callbacks: it runs eukollab_callbacks.py beside itself,
    sent by ekl agent deploy or found beside agent.json, and only when the agent takes part and no other address is
    set."""
    script = "import sys, pathlib; pathlib.Path('ran').write_text(' '.join(sys.argv[1:]))\n"
    monkeypatch.delenv("EKL_CALLBACK_URL", raising=False)
    monkeypatch.delenv("EKL_CALLBACKS_PY", raising=False)
    monkeypatch.delenv("EKL_CALLBACKS_PORT", raising=False)
    (tmp_path / "agent.json").write_text("{}")
    (tmp_path / connector.CALLBACKS_FILE).write_text(script)
    monkeypatch.delenv("EKL_AGENT_CREDENTIAL", raising=False)
    assert connector.start_callbacks(str(tmp_path / "agent.json")) is None
    monkeypatch.setenv("EKL_AGENT_CREDENTIAL", "ekla_test")
    child = connector.start_callbacks(str(tmp_path / "agent.json"))
    assert child is not None and child.wait(timeout=30) == 0
    # 8002, or a free port when another agent's callbacks already listen there.
    port = (tmp_path / "ran").read_text().removeprefix("--port ")
    assert port.isdigit() and os.environ["EKL_CALLBACK_URL"] == f"http://127.0.0.1:{port}/"
    # Already pointed at the publisher's own app: the file is left alone.
    assert connector.start_callbacks(str(tmp_path / "agent.json")) is None
    monkeypatch.delenv("EKL_CALLBACK_URL")
    sent = base64.b64encode(gzip.compress(script.replace("'ran'", "'sent'").encode())).decode()
    monkeypatch.setenv("EKL_CALLBACKS_PY", sent)
    child = connector.start_callbacks(None)
    assert child is not None and child.wait(timeout=30) == 0
    assert (Path(str(child.args[1])).parent / "sent").read_text().startswith("--port ")


async def test_with_ekl_relay_an_attached_server_takes_its_relay_address(monkeypatch: pytest.MonkeyPatch) -> None:
    """`ekl agent run` in a new agent's folder starts its server with EKL_RELAY=1: no public address, and the
    endpoint it announces is the relay address the network keeps for it."""
    relay_url = "https://network.test/v1/relay/agt_homes/mcp"
    monkeypatch.setenv("EKL_RELAY", "1")
    monkeypatch.setattr(ATTACH_MODULE, "relay_address_now", lambda *_: relay_url)
    app = attach(homes_server(), AGENT, credential=CREDENTIAL, api=network_client(confirmed(AGENT)))
    assert app.state.ekl_core.endpoint == relay_url
    monkeypatch.delenv("EKL_RELAY")
    plain = attach(homes_server(), AGENT, credential=CREDENTIAL, api=network_client(confirmed(AGENT)))
    assert plain.state.ekl_core.endpoint != relay_url
