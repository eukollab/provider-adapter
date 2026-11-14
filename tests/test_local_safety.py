# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""What the adapter refuses on the publisher's machine: browsers and foreign hosts on the local API (WP3), the
server's own /mcp when kept private (WP4), secrets in announced fields and files others can read (WP8), tools the local
agent.json doesn't declare (WP10) and input nested too deep (WP15)."""

import asyncio
import contextlib
import datetime as dt
import importlib.util
import json
import os
import socket
import stat
import threading
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx2
import pytest
import uvicorn
from starlette.testclient import TestClient

from ekl_provider_adapter import attach
from ekl_provider_adapter.agent import Agent
from ekl_provider_adapter.agent_journal import AgentJournal
from ekl_provider_adapter.callbacks import local_api
from ekl_provider_adapter.config import load_agent_file
from ekl_provider_adapter.connector import _free_or
from ekl_provider_adapter.jcs import MAX_DEPTH, InputTooDeepError, canonical, too_deep
from ekl_provider_adapter.local_guard import LocalGuard, announce_when_listening
from ekl_provider_adapter.testing import FakeNetwork

from .test_attach_and_connector import AGENT, CREDENTIAL, confirmed, homes_server, mcp, network_client, served

LOCAL = "http://127.0.0.1:8765"
TEMPLATES = Path(__file__).resolve().parents[2] / "devtools" / "templates" / "python"

# A value shaped like a live payment-processor key, built from parts so no scanner mistakes the test for a leak.
SECRET_LIKE = "sk_" + "live_secret_value"


@contextlib.asynccontextmanager
async def from_outside(app: Any, token: str | None = None) -> AsyncIterator[httpx2.AsyncClient]:
    """A client on another machine, as a container's published port sees one."""
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    async with app.router.lifespan_context(app):
        transport = httpx2.ASGITransport(app=app, client=("172.17.0.1", 40000))
        async with httpx2.AsyncClient(transport=transport, base_url="http://127.0.0.1", headers=headers) as http:
            yield http


def _api(tmp_path: Path) -> tuple[FakeNetwork, TestClient]:
    fake = FakeNetwork()
    agent = Agent(fake, AgentJournal(str(tmp_path / "journal.db")))
    return fake, TestClient(local_api(agent), base_url=LOCAL)


def test_ac257_a_cross_site_browser_post_is_refused_and_nothing_reaches_the_network(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("EKL_LOCAL_TOKEN", raising=False)
    fake, client = _api(tmp_path)
    with client:
        # What a page on another site can send without a preflight: text/plain, with its Origin.
        simple = client.post(
            "/agent/post_need",
            content=json.dumps({"text": "Spend my money"}),
            headers={"Content-Type": "text/plain", "Origin": "https://evil.example"},
        )
        typed = client.post("/agent/post_need", content='{"text": "x"}', headers={"Content-Type": "text/plain"})
        with_origin = client.post("/agent/post_need", json={"text": "x"}, headers={"Origin": "https://evil.example"})
        preflight = client.options("/agent/post_need", headers={"Origin": "https://evil.example"})
    assert simple.status_code in (403, 415) and typed.status_code == 415 and with_origin.status_code == 403
    assert preflight.status_code == 405 and "access-control-allow-origin" not in preflight.headers
    assert fake.posts == {}


def test_ac258_a_foreign_host_gets_421(tmp_path: Path) -> None:
    _, client = _api(tmp_path)
    with client:
        answer = client.post("/agent/find", json={"query": "x"}, headers={"Host": "attacker.example:8765"})
    assert answer.status_code == 421


def test_ac260_after_the_cutoff_a_request_without_the_token_gets_401(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("EKL_LOCAL_TOKEN", raising=False)
    fake = FakeNetwork()
    fake.provider("@vat", "cap_vat", lambda _: {"ok": True}, summary="vat")
    app = local_api(Agent(fake, AgentJournal(str(tmp_path / "journal.db"))))
    # An existing installation without a token: answered until the cutoff, refused from it.
    monkeypatch.setenv("EKL_LOCAL_TOKEN_REQUIRED_FROM", (dt.date.today() + dt.timedelta(days=1)).isoformat())
    with TestClient(app, base_url=LOCAL) as client:
        assert client.post("/agent/find", json={"query": "vat"}).status_code == 200
    monkeypatch.setenv("EKL_LOCAL_TOKEN_REQUIRED_FROM", dt.date.today().isoformat())
    with TestClient(local_api(Agent(fake, AgentJournal(str(tmp_path / "j2.db")))), base_url=LOCAL) as client:
        assert client.post("/agent/find", json={"query": "vat"}).status_code == 401
    # A connector set up with the token requires it from the start, compared in constant time.
    monkeypatch.setenv("EKL_LOCAL_TOKEN", "tok_" + "a" * 40)
    monkeypatch.setenv("EKL_LOCAL_TOKEN_REQUIRED_FROM", "2099-01-01")
    with TestClient(local_api(Agent(fake, AgentJournal(str(tmp_path / "j3.db")))), base_url=LOCAL) as client:
        assert client.post("/agent/find", json={"query": "vat"}).status_code == 401
        wrong = client.post("/agent/find", json={"query": "vat"}, headers={"Authorization": "Bearer nope"})
        right = client.post("/agent/find", json={"query": "vat"}, headers={"Authorization": f"Bearer tok_{'a' * 40}"})
    assert wrong.status_code == 401 and right.status_code == 200


def test_a_body_over_1_mb_is_refused(tmp_path: Path) -> None:
    _, client = _api(tmp_path)
    with client:
        answer = client.post(
            "/agent/post_need",
            content=b'{"text": "' + b"x" * 1_100_000 + b'"}',
            headers={"Content-Type": "application/json"},
        )
    assert answer.status_code == 413


def _template(tmp_path: Path) -> Any:
    text = (
        (TEMPLATES / "eukollab_callbacks.py").read_text().replace("{{name}}", "X").replace("{{adapter_version}}", "0")
    )
    (tmp_path / "eukollab_callbacks.py").write_text(text)
    spec = importlib.util.spec_from_file_location("callbacks_ac259", tmp_path / "eukollab_callbacks.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_ac259_with_8765_busy_the_app_reaches_the_connector_through_local_api_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("EKL_AGENT_API_URL", raising=False)
    monkeypatch.setenv("EKL_LOCAL_TOKEN", "local-token")
    monkeypatch.setenv("EKL_AGENT_FILE", str(tmp_path / "agent.json"))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as busy:
        busy.bind(("127.0.0.1", 0))
        busy.listen()
        taken = str(busy.getsockname()[1])
        port = int(_free_or(taken))
        assert str(port) != taken
        fake = FakeNetwork()
        fake.provider("@vat", "cap_vat", lambda _: {"ok": True}, summary="vat check")
        server = uvicorn.Server(
            uvicorn.Config(
                local_api(Agent(fake, AgentJournal(str(tmp_path / "journal.db")))), host="127.0.0.1", port=port
            )
        )

        async def run() -> None:
            await asyncio.gather(server.serve(), announce_when_listening(server, f"http://127.0.0.1:{port}"))

        thread = threading.Thread(target=lambda: asyncio.run(run()), daemon=True)
        thread.start()
        written = tmp_path / ".ekl" / "local-api.json"
        for _ in range(200):
            if written.exists():
                break
            time.sleep(0.02)
        try:
            assert stat.S_IMODE(written.stat().st_mode) == 0o600
            assert stat.S_IMODE(written.parent.stat().st_mode) == 0o700
            assert json.loads(written.read_text())["pid"] == os.getpid()
            found = _template(tmp_path).act("find", {"query": "vat check"})
            assert found["candidates"][0]["handle"] == "@vat"
        finally:
            server.should_exit = True
            thread.join(5)
    # A file naming a process that is gone says the connector isn't running, never falls back to 8765.
    written.write_text(json.dumps({"url": "http://127.0.0.1:1", "pid": 999_999_9, "started_at": ""}))
    module = _template(tmp_path)
    with pytest.raises(module.ActionRefused, match="isn't running"):
        module.act("find", {"query": "x"})


def test_ac261_a_generated_server_keeps_its_own_mcp_private_while_eukollab_calls_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    network = confirmed(AGENT)
    token = network.token("agt_consumer")
    app = attach(homes_server(), AGENT, credential=CREDENTIAL, api=network_client(network), expose_mcp=False)

    async def go() -> None:
        async with from_outside(app, token) as http:
            assert (
                await http.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "initialize"})
            ).status_code == 404
            async with mcp(http, "/ekl") as adapter:
                assert [t.name for t in (await adapter.list_tools()).tools] == ["search_active_listings"]
        again = attach(
            homes_server(), AGENT, credential=CREDENTIAL, api=network_client(network), expose_mcp=False, host="0.0.0.0"
        )
        async with served(again, token) as local, mcp(local, "/mcp") as own:
            # On the same machine (ekl agent sync, the publisher's tests) the server's endpoint still answers.
            assert "internal_admin" in {t.name for t in (await own.list_tools()).tools}

    asyncio.run(go())


def test_ac327_an_existing_deployment_keeps_mcp_with_a_warning_until_the_default_changes(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    network = confirmed(AGENT)
    monkeypatch.setenv("EKL_EXPOSE_MCP_DEFAULT_OFF_FROM", (dt.date.today() + dt.timedelta(days=30)).isoformat())
    app = attach(homes_server(), AGENT, credential=CREDENTIAL, api=network_client(network))
    assert "is private unless attach() passes expose_mcp=True" in caplog.text

    async def status(made: Any) -> int:
        async with from_outside(made) as http:
            answer = await http.post(
                "/mcp",
                json={"jsonrpc": "2.0", "id": 1, "method": "ping"},
                headers={"Accept": "application/json, text/event-stream"},
            )
            return answer.status_code

    assert asyncio.run(status(app)) != 404
    monkeypatch.setenv("EKL_EXPOSE_MCP_DEFAULT_OFF_FROM", dt.date.today().isoformat())
    later = attach(homes_server(), AGENT, credential=CREDENTIAL, api=network_client(network))
    assert asyncio.run(status(later)) == 404


def test_ac274_a_reference_in_an_announced_field_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SECRET", "s3cret")
    with pytest.raises(ValueError, match="SECRET"):
        load_agent_file(AGENT | {"agent": {"handle": "@homes", "summary": "${SECRET}"}})
    monkeypatch.setenv("UP", "http://127.0.0.1:9000/mcp")
    loaded = load_agent_file(AGENT | {"upstream": {"mcp_url": "${UP}"}})
    assert loaded.upstream is not None and loaded.upstream.mcp_url == "http://127.0.0.1:9000/mcp"


def test_a_network_off_this_machine_must_be_https() -> None:
    with pytest.raises(ValueError, match="https"):
        load_agent_file(AGENT | {"network": {"url": "http://eukollab.example"}})


def test_journals_are_owner_only(tmp_path: Path) -> None:
    AgentJournal(str(tmp_path / ".ekl" / "journal.db"))
    assert stat.S_IMODE((tmp_path / ".ekl").stat().st_mode) == 0o700
    assert stat.S_IMODE((tmp_path / ".ekl" / "journal.db").stat().st_mode) == 0o600


def test_ac280_a_tool_only_the_network_lists_cannot_be_called() -> None:
    network = confirmed(
        AGENT | {"capabilities": [*AGENT["capabilities"], {"tool": "internal_admin", "class": "read_only"}]}
    )
    token = network.token("agt_consumer")
    app = attach(homes_server(), AGENT, credential=CREDENTIAL, api=network_client(network), expose_mcp=False)

    async def go() -> None:
        async with served(app, token) as http, mcp(http, "/ekl") as adapter:
            assert [t.name for t in (await adapter.list_tools()).tools] == ["search_active_listings"]
            meta = {"ekl": {"execution_id": "e1", "salt": "ab" * 32}}
            refused = await adapter.call_tool("internal_admin", {}, meta=meta)
            assert refused.is_error and (refused.meta or {})["ekl"]["code"] == "NOT_LISTED"
        assert app.state.ekl_core.undeclared_tools() == ["internal_admin"]

    asyncio.run(go())


def _nested(depth: int) -> Any:
    value: Any = "x"
    for _ in range(depth):
        value = {"a": value}
    return value


def test_ac293_input_nested_65_deep_is_refused_and_the_adapter_keeps_serving() -> None:
    assert not too_deep(_nested(MAX_DEPTH)) and too_deep(_nested(MAX_DEPTH + 1))
    with pytest.raises(InputTooDeepError):
        canonical(_nested(MAX_DEPTH + 1))
    network = confirmed(AGENT)
    token = network.token("agt_consumer")
    app = attach(homes_server(), AGENT, credential=CREDENTIAL, api=network_client(network), expose_mcp=False)

    async def go() -> None:
        async with served(app, token) as http, mcp(http, "/ekl") as adapter:
            meta = {"ekl": {"execution_id": "e1", "salt": "ab" * 32}}
            deep = await adapter.call_tool("search_active_listings", {"area_ref": _nested(65)}, meta=meta)
            assert deep.is_error and (deep.meta or {})["ekl"]["code"] == "INPUT_TOO_DEEP"
            fine = await adapter.call_tool("search_active_listings", {"area_ref": "loc:x"}, meta=meta)
            assert not fine.is_error

    asyncio.run(go())


def test_the_guard_reads_the_token_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EKL_LOCAL_TOKEN", "abc")
    assert LocalGuard(lambda *_: None).token == "abc"  # type: ignore[arg-type, return-value]
    assert httpx2  # the client tests above use it through TestClient


def test_validation_errors_are_logged_by_field_and_kind_only(monkeypatch: pytest.MonkeyPatch) -> None:
    from pydantic import ValidationError  # noqa: PLC0415 - only this test builds one

    from ekl_provider_adapter.config import AgentFile, described  # noqa: PLC0415

    with pytest.raises(ValidationError) as invalid:
        AgentFile.model_validate(AGENT | {"execution_mode": SECRET_LIKE})
    text = described(invalid.value)
    assert "execution_mode" in text and SECRET_LIKE not in text
