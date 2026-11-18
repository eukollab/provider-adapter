# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The exchange, end to end with no network: each agent is its folder's eukollab_callbacks.py, run with the
connector's agent side against testing.FakeNetwork. The seller answers the buyer's need; the buyer calls the seller's
MCP server through the connector's local API and checks the receipt. Run it with `pytest`."""

import asyncio
import contextlib
import importlib.util
import socket
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import uvicorn

from ekl_provider_adapter.agent import Agent
from ekl_provider_adapter.agent_journal import AgentJournal
from ekl_provider_adapter.callbacks import bridge, local_api
from ekl_provider_adapter.testing import FakeNetwork

HERE = Path(__file__).resolve().parent
CREDENTIAL = "ekla_test"


def _load(path: Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _callbacks(who: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """The agent's eukollab_callbacks.py, as the connector runs it, keeping its record of events in tmp_path."""
    monkeypatch.setenv("EKL_AGENT_CREDENTIAL", CREDENTIAL)
    monkeypatch.setenv("EKL_CALLBACKS_DB", str(tmp_path / f"{who}-callbacks.db"))
    return _load(HERE / who / "eukollab_callbacks.py", f"{who}_callbacks")


@contextlib.contextmanager
def _events_to(module: ModuleType, agent: Agent) -> Iterator[None]:
    """The connector posting the agent's events, signed, to its callbacks file."""
    server = module.ThreadingHTTPServer(("127.0.0.1", 0), module.Events)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    bridge(agent, f"http://127.0.0.1:{server.server_port}/", CREDENTIAL)
    try:
        yield
    finally:
        server.shutdown()


@contextlib.contextmanager
def _local_api(agent: Agent, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """The connector's local API on this machine, where the callbacks' act() reaches it."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = int(probe.getsockname()[1])
    api = uvicorn.Server(uvicorn.Config(local_api(agent), host="127.0.0.1", port=port, log_level="warning"))
    serving = threading.Thread(target=lambda: asyncio.run(api.serve()), daemon=True)
    serving.start()
    monkeypatch.setenv("EKL_AGENT_API_URL", f"http://127.0.0.1:{port}")
    for _ in range(200):
        if api.started:
            break
        time.sleep(0.02)
    try:
        yield
    finally:
        api.should_exit = True
        serving.join(5)


def test_the_seller_answers_and_the_buyer_calls_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    server = _load(HERE / "seller" / "server.py", "seller_server")
    seller, buyer = _callbacks("seller", tmp_path, monkeypatch), _callbacks("buyer", tmp_path, monkeypatch)
    seller_side, buyer_side = FakeNetwork(handle="@vat-check"), FakeNetwork(handle="@supplier-desk")
    seller_agent = Agent(seller_side, AgentJournal(str(tmp_path / "seller.db")))
    buyer_agent = Agent(buyer_side, AgentJournal(str(tmp_path / "buyer.db")))

    def validate(arguments: dict[str, Any]) -> Any:
        return server.validate_vat(**arguments)

    buyer_side.provider("@vat-check", "cap_validate_vat", validate, summary="validate eu vat numbers", price="0.05")
    with (
        _events_to(seller, seller_agent),
        _events_to(buyer, buyer_agent),
        _local_api(buyer_agent, monkeypatch),
    ):
        buyer.on_start()  # the buyer posts its need
        need = buyer_side.needs[0]
        seller_side.need(
            need["body"],
            author="@supplier-desk",
            matched="cap_validate_vat",
            service="Validate EU VAT numbers",
            price="$0.05 / call",
        )
        seller_side.run(seller_agent)  # the seller's on_need answers it
        assert seller_side.replies[0]["body"].startswith("I can do this: Validate EU VAT numbers")
        buyer_side.answer(need["post_id"], seller_side.replies[0]["body"], author="@vat-check")
        buyer_side.run(buyer_agent)  # the buyer takes the answer and calls the seller
        for _ in range(250):
            if buyer_side.reports:
                break
            time.sleep(0.02)
    assert buyer_side.reports and buyer_side.reports[0]["receipt"]
    assert buyer_side.providers["@vat-check/cap_validate_vat"].calls == 1
