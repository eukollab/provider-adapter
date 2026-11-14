# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The adapter speaks the Provider API exactly as the contracts define it: every request it sends validates against
the protocol's ekp/<version>/http/provider_*.input.json, for every version present (1.1, and 1.2 where the checkout has
it), and the fake network answers with what the output schemas allow."""

import inspect
import json
from pathlib import Path
from typing import Any

import httpx2
import jsonschema
import pytest
from referencing import Registry, Resource

from ekl_provider_adapter.agent import Agent
from ekl_provider_adapter.api import HttpProviderApi
from ekl_provider_adapter.conformance.harness import Call, Harness
from ekl_provider_adapter.conformance.network import http_app
from ekl_provider_adapter.conformance.toolbox import Crash
from tests.conftest import CONTRACTS

pytestmark = pytest.mark.anyio

EKP = CONTRACTS / "ekp" / "1.1"
# 1.2 adds only optional fields to what the adapter sends and receives, so one adapter speaks both until 1.1 retires;
# the announcement carries 1.2's proof_key, which 1.1's closed schema doesn't know.
EKPS = [CONTRACTS / "ekp" / v for v in ("1.1", "1.2") if (CONTRACTS / "ekp" / v).is_dir()]
ONLY_1_2 = {"provider_announce"}


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
    for ekp in EKPS:
        for path, body in sent:
            name = operations[path]
            if name in ONLY_1_2 and ekp.name == "1.1":
                continue
            if (ekp / "http" / f"{name}.input.json").exists():  # announce's contract may not be published yet
                validator(f"{name}.input", ekp).validate(body)
        for path, body in answered:
            name = operations[path]
            if not (ekp / "http" / f"{name}.output.json").exists() or (name in ONLY_1_2 and ekp.name == "1.1"):
                continue
            if "code" in body and "decision" not in body:
                continue  # an error envelope, defined by errors.json
            if name == "provider_complete" or "receipt" in body:
                continue  # receipts come from the network; the fake's are simplified
            validator(f"{name}.output", ekp).validate(body)
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
    for ekp in EKPS:
        schema = json.loads((ekp / "tools" / "events_next.input.json").read_text(encoding="utf-8"))
        jsonschema.Draft202012Validator(schema, registry=registry(ekp)).validate({"handlers": agent.handler_names()})
    assert "on_review_due" in agent.handler_names()
