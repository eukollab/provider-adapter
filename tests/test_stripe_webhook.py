# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The optional webhook for the publisher's own Stripe account: a paid Checkout carrying a run's reservation ID
approves what its plan buys, and nothing else approves anything."""

import asyncio
import hashlib
import hmac
import json
import time
from decimal import Decimal
from typing import Any

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from ekl_provider_adapter.config import AgentFile
from ekl_provider_adapter.runs import RunDecision, RunRequest, Runs, RunsApiError, default_decision
from ekl_provider_adapter.stripe_webhook import SignatureError, paid_run, stripe_routes, verify

SECRET = "whsec_test_publisher"
FILE: dict[str, Any] = {
    "ekl": "1.0",
    "mode": "service_provider",
    "execution_mode": "durable",
    "payment": {"link": "https://buy.stripe.com/northwind"},
    "capabilities": [
        {
            "tool": "extract",
            "class": "read_only",
            "plans": [
                {"plan_id": "pack500", "type": "pack", "price": "8.00", "calls": 500},
                {"plan_id": "monthly", "type": "subscription", "price": "20.00", "period": "month"},
            ],
        }
    ],
}
AGENT = AgentFile.model_validate(FILE)


class Api:
    """The network's run-approval endpoints: it remembers each payment request, as the network does."""

    def __init__(self, refuse: bool = False) -> None:
        self.decided: list[tuple[str, RunDecision]] = []
        self.refuse = refuse
        self.runs: dict[str, dict[str, Any]] = {}

    async def get(self, reservation_id: str) -> dict[str, Any]:
        if reservation_id not in self.runs:
            raise RunsApiError("NOT_FOUND", "no such reservation", retryable=False)
        return self.runs[reservation_id]

    async def decide(self, reservation_id: str, decision: RunDecision) -> dict[str, Any]:
        if self.refuse:
            raise RunsApiError("INVALID_STATE", "the run has ended", retryable=False)
        self.decided.append((reservation_id, decision))
        return {}


def _run(reservation_id: str = "rsv_1", plan_id: str = "pack500") -> RunRequest:
    return RunRequest(
        reservation_id=reservation_id,
        execution_id="ex_1",
        consumer={"agent_id": "agt_1"},
        capability_id="cap_extract",
        plan_id=plan_id,
        unit_price="0.016",
    )


def _event(reference: str = "rsv_1", total: int = 800, *, status: str = "paid", kind: str = "") -> bytes:
    session = {"client_reference_id": reference, "amount_total": total, "currency": "usd", "payment_status": status}
    return json.dumps({"type": kind or "checkout.session.completed", "data": {"object": session}}).encode()


def _signed(payload: bytes, secret: str = SECRET, at: int | None = None) -> str:
    stamp = int(time.time()) if at is None else at
    return f"t={stamp},v1=" + hmac.new(secret.encode(), f"{stamp}.".encode() + payload, hashlib.sha256).hexdigest()


def _client(runs: Runs) -> TestClient:
    return TestClient(Starlette(routes=stripe_routes("/ekl", runs, SECRET)))


def test_a_stripe_payment_link_carries_the_run_as_client_reference_id() -> None:
    assert default_decision(AGENT, _run()).fields["payment"]["url"] == (
        "https://buy.stripe.com/northwind?client_reference_id=rsv_1"
    )
    elsewhere = AgentFile.model_validate(FILE | {"payment": {"link": "https://pay.example"}})
    assert default_decision(elsewhere, _run()).fields["payment"]["url"] == "https://pay.example?ref=rsv_1"


def test_the_signature_must_be_stripes_and_recent() -> None:
    payload = _event()
    assert verify(payload, _signed(payload), SECRET)["type"] == "checkout.session.completed"
    for header in (_signed(payload, "whsec_other"), _signed(payload, at=int(time.time()) - 600), "v1=abc", ""):
        with pytest.raises(SignatureError):
            verify(payload, header, SECRET)


def test_only_a_paid_checkout_with_a_reference_names_a_run() -> None:
    assert paid_run(json.loads(_event())) is not None
    assert paid_run(json.loads(_event(status="unpaid"))) is None
    assert paid_run(json.loads(_event(kind="payment_intent.succeeded"))) is None
    assert paid_run(json.loads(_event(reference="not a reservation!"))) is None


def test_a_paid_checkout_approves_what_the_plan_buys() -> None:
    api = Api()
    runs = Runs(api, lambda: AGENT)
    asyncio.run(runs.answer(_run("rsv_1")))
    asyncio.run(runs.answer(_run("rsv_2", "monthly")))
    with _client(runs) as client:
        for reference, total in (("rsv_1", 800), ("rsv_2", 2000)):
            payload = _event(reference, total)
            answer = client.post("/ekl/stripe", content=payload, headers={"Stripe-Signature": _signed(payload)})
            assert answer.status_code == 200 and answer.json()["approved"] is True
    approvals = [(r, d.fields) for r, d in api.decided if d.decision == "approve"]
    assert approvals[0] == ("rsv_1", {"unit": "plan", "standing": {"runs": 500}})
    assert approvals[1][0] == "rsv_2" and "until" in approvals[1][1]["standing"]


def test_a_short_payment_a_forged_event_or_an_ended_run_approves_nothing() -> None:
    api = Api()
    runs = Runs(api, lambda: AGENT)
    asyncio.run(runs.answer(_run()))
    with _client(runs) as client:
        short = _event(total=100)
        assert client.post("/ekl/stripe", content=short, headers={"Stripe-Signature": _signed(short)}).json() == {
            "received": True,
            "approved": False,
        }
        forged = _event()
        assert client.post("/ekl/stripe", content=forged, headers={"Stripe-Signature": "t=1,v1=0"}).status_code == 400
    assert [d.decision for _, d in api.decided] == ["payment_needed"]
    assert asyncio.run(Runs(Api(refuse=True), lambda: AGENT).paid("rsv_1", Decimal(8), "USD")) is False


def test_stripe_delivers_again_when_the_network_could_not_be_asked() -> None:
    class Unreachable(Api):
        async def decide(self, reservation_id: str, decision: RunDecision) -> dict[str, Any]:
            del reservation_id, decision
            raise RunsApiError("SERVICE_UNAVAILABLE", "the network is unreachable", retryable=True)

    runs = Runs(Unreachable(), lambda: AGENT)
    asyncio.run(runs.answer(_run()))  # asked for payment; the network then becomes unreachable
    with _client(runs) as client:
        payload = _event()
        answer = client.post("/ekl/stripe", content=payload, headers={"Stripe-Signature": _signed(payload)})
    assert answer.status_code == 503  # not 2xx: Stripe retries it


def test_without_a_secret_there_is_no_webhook() -> None:
    assert stripe_routes("/ekl", Runs(Api(), lambda: AGENT), None) == []


def test_a_restarted_adapter_reads_its_payment_request_back_before_approving() -> None:
    # An earlier copy asked for the payment: this one reads the request back from the network, and the payment still
    # buys the run's plan. Without a request to match, or paid short of it, nothing is approved, whatever was paid.
    api = Api()
    asked = {"amount": "8.00", "currency": "USD", "url": "https://buy.stripe.com/x"}
    api.runs["rsv_9"] = _run("rsv_9").model_dump() | {"state": "awaiting_approval", "payment": asked}
    api.runs["rsv_8"] = _run("rsv_8").model_dump() | {"state": "awaiting_approval", "payment": asked}
    with _client(Runs(api, lambda: AGENT)) as client:
        answers = {}
        for reference, total in (("rsv_9", 800), ("rsv_7", 800), ("rsv_8", 100)):
            payload = _event(reference, total)
            answer = client.post("/ekl/stripe", content=payload, headers={"Stripe-Signature": _signed(payload)})
            answers[reference] = answer.json()["approved"]
    assert answers == {"rsv_9": True, "rsv_7": False, "rsv_8": False}
    assert [(r, d.fields) for r, d in api.decided] == [("rsv_9", {"unit": "plan", "standing": {"runs": 500}})]


def test_callbacks_that_say_nothing_still_send_the_buyer_to_the_payment_page() -> None:
    # The callbacks file's on_run_approval returns None as written: with a payment page, the run must not just wait.
    async def nothing(run: RunRequest) -> None:
        return None

    api = Api()
    runs = Runs(api, lambda: AGENT)
    runs.on_approval(nothing)
    asyncio.run(runs.answer(_run()))
    [(_, decision)] = api.decided
    assert decision.decision == "payment_needed"
    assert decision.fields["payment"]["url"] == "https://buy.stripe.com/northwind?client_reference_id=rsv_1"
    waiting = Api()
    unpaid = Runs(waiting, lambda: AgentFile.model_validate({**FILE, "payment": None}))
    unpaid.on_approval(nothing)
    asyncio.run(unpaid.answer(_run()))
    assert waiting.decided == []
