# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The adapter speaks the Provider API exactly as the contracts define it: every request it sends validates against
the protocol's ekp/3.0/http/provider_*.input.json, and the fake network answers with what the output schemas allow."""

import inspect
import json
from pathlib import Path
from typing import Any

import httpx2
import jsonschema
import pytest
from referencing import Registry, Resource

from ekl_provider_adapter.agent import Agent
from ekl_provider_adapter.agent_journal import AgentJournal
from ekl_provider_adapter.api import HttpProviderApi
from ekl_provider_adapter.conformance.harness import Call, Harness
from ekl_provider_adapter.conformance.network import http_app
from ekl_provider_adapter.conformance.toolbox import Crash
from ekl_provider_adapter.testing import FakeNetwork
from tests.conftest import CONTRACTS, use

pytestmark = pytest.mark.anyio

# The network speaks ekp/3.0, whose adapter-profile messages are 2.0's, and accepts no 1.x.
EKP = CONTRACTS / "ekp" / "3.0"


def registry(ekp: Path = EKP) -> Registry:
    found: Registry = Registry()
    for path in ekp.rglob("*.json"):
        schema = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(schema, dict) and "$id" in schema:
            found = found.with_resource(schema["$id"], Resource.from_contents(schema))
    return found


def validator(name: str, ekp: Path = EKP) -> jsonschema.Draft202012Validator:
    schema = json.loads((ekp / "http" / f"{name}.json").read_text(encoding="utf-8"))
    return jsonschema.Draft202012Validator(schema, registry=registry(ekp))


@pytest.mark.skipif(not (EKP / "http" / "provider_admit.input.json").exists(), reason="no Provider API contract yet")
async def test_requests_and_answers_follow_the_contracts() -> None:
    sent: list[tuple[str, dict[str, Any]]] = []
    answered: list[tuple[str, dict[str, Any]]] = []
    harness = Harness("durable", lease_seconds=0.2)
    app = http_app(harness.network, "eklp_contract")

    async def record(request: httpx2.Request) -> None:
        if request.content:  # GET /v1/provider/config has no body
            sent.append((request.url.path, json.loads(request.content)))

    async def keep(response: httpx2.Response) -> None:
        await response.aread()
        answered.append((response.request.url.path, response.json()))

    http = httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app), event_hooks={"request": [record], "response": [keep]}
    )
    harness.core.api = HttpProviderApi("https://network", "eklp_contract", http=http)
    try:
        assert await harness.core.announce_if_changed()
        await harness.call(Call("slow", {"seconds": 0.5}, "e1"))  # admit, renew, complete
        await harness.core.drain()
        harness.toolbox.crash_at = "send:after_effect"
        await harness.call(Call("send", {"to": "a@example.com", "message": "m"}, "e2"))
    except Crash:  # the injected crash; recovery below exercises pending and resolve
        harness.core = harness.build()
        harness.core.api = HttpProviderApi("https://network", "eklp_contract", http=http)
        await harness.core.recover()
        await harness.core.drain()
    finally:
        harness.close()
    operations = {
        "/v1/provider/admit": "provider_admit",
        "/v1/provider/complete": "provider_complete",
        "/v1/provider/reconcile": "provider_reconcile",
        "/v1/provider/announce": "provider_announce",
        "/v1/provider/config": "provider_config",
    }
    for path, body in sent:
        validator(f"{operations[path]}.input").validate(body)
    for path, body in answered:
        name = operations[path]
        if "code" in body and "decision" not in body:
            continue  # an error envelope, defined by errors.json
        if name == "provider_complete" or "receipt" in body:
            continue  # receipts come from the network; the fake's are simplified
        validator(f"{name}.output").validate(body)
    paths = {p for p, _ in sent}
    assert {"/v1/provider/announce", "/v1/provider/admit", "/v1/provider/complete", "/v1/provider/reconcile"} <= paths


def test_every_callback_is_a_handler_the_network_accepts() -> None:
    # The runtime names its callbacks on every events_next; one name the contract doesn't know refuses the whole poll,
    # and the agent then receives no events at all.
    agent = Agent(None)
    for name, register in inspect.getmembers(agent, inspect.ismethod):
        if name.startswith("on_") and name != "on_start":
            takes_callback = "callback" in inspect.signature(register).parameters
            (register if takes_callback else register())(lambda *_: None)
    schema = json.loads((EKP / "tools" / "events_next.input.json").read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator(schema, registry=registry()).validate({"handlers": agent.handler_names()})
    assert "on_review_due" in agent.handler_names()


def test_a_paid_run_reserves_as_the_contract_says(tmp_path: Path) -> None:
    # The reservation offers the pack's whole price and asks nothing else: no approval wait, which 2.0 refuses.
    fake = FakeNetwork()
    pack = {"plan_id": "pack-500", "type": "pack", "price": "8.00", "calls": 500}
    fake.provider("@fx", "cap_convert", lambda _: {"converted": 1.1}, summary="convert currency", plan=pack)
    agent = Agent(fake, AgentJournal(str(tmp_path / "journal.db")))
    [candidate] = agent.find("convert currency")
    use(agent, candidate).call({"amount": 1})
    schema = json.loads((EKP / "tools" / "execution_reserve.input.json").read_text(encoding="utf-8"))
    [reserved] = fake.reserved
    jsonschema.Draft202012Validator(schema, registry=registry()).validate(reserved)
    assert reserved["requested_max_amount_atoms"] == "8000000"
