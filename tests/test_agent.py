# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The adapter works both ways: the agent's callbacks and actions, run against testing.FakeNetwork."""

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx2
import pytest
from mcp.server.mcpserver import MCPServer
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from ekl_provider_adapter import attach
from ekl_provider_adapter.agent import Agent, Choice, Need, ReviewDue, Untrusted
from ekl_provider_adapter.agent_journal import AgentJournal
from ekl_provider_adapter.callbacks import SIGNATURE_HEADER, bridge, local_api, sign
from ekl_provider_adapter.config import AgentFile
from ekl_provider_adapter.errors import (
    AgentError,
    AgentNotEnabled,
    NoProvider,
    NotAllowed,
    PaymentNeeded,
    ProviderNotFunded,
    RunNotApproved,
    from_network,
)
from ekl_provider_adapter.runs import RunDecision, RunRequest, Runs, default_decision
from ekl_provider_adapter.testing import FakeNetwork

VAT_SCHEMA = {"type": "object", "required": ["vat_number"], "properties": {"vat_number": {"type": "string"}}}
VALID = {"type": "object", "required": ["valid"], "properties": {"valid": {"type": "boolean"}}}


def _agent(fake: FakeNetwork, tmp_path: Path) -> Agent:
    return Agent(fake, AgentJournal(str(tmp_path / "journal.db")))


def _server() -> MCPServer:
    server = MCPServer("demo")

    @server.tool()
    def echo(text: str) -> str:
        return text

    return server


def test_without_the_agent_credential_the_adapter_only_serves(monkeypatch: pytest.MonkeyPatch) -> None:
    # No credential, no event loop, and every action names the variable to set.
    monkeypatch.delenv("EKL_AGENT_CREDENTIAL", raising=False)
    app = attach(_server(), credential="eklp_test", network_url="http://network.invalid")
    assert not app.agent.enabled
    with pytest.raises(AgentNotEnabled, match="EKL_AGENT_CREDENTIAL"):
        app.agent.post_need("Validate VAT numbers")
    with pytest.raises(AgentNotEnabled):
        app.agent.use(None)  # type: ignore[arg-type]  # refused before the service is looked at


def test_a_matching_need_is_answered_from_the_server_process(tmp_path: Path) -> None:
    # One on_need callback answers matching needs; the network learns the agent answers them itself.
    fake = FakeNetwork(handle="@vat-check")
    app = attach(_server(), credential="eklp_test", network_url="http://network.invalid", agent_network=fake)

    @app.agent.on_need(matching="my_services")
    def answer(need: Need) -> None:
        assert need.matched_service is not None
        need.reply(f"I can do this with {need.matched_service.name}, {need.matched_service.price}.")

    fake.need(
        "Validate EU VAT numbers", author="@buyer", matched="cap_vat", service="Validate VAT", price="$0.05 / call"
    )
    fake.run(app.agent)
    assert [r["body"] for r in fake.replies] == ["I can do this with Validate VAT, $0.05 / call."]
    assert "on_need" in fake.handlers


def test_subscribed_needs_and_connection_requests(tmp_path: Path) -> None:
    fake = FakeNetwork()
    agent = _agent(fake, tmp_path)
    seen: list[str] = []

    @agent.on_need(matching="subscribed", topics=["VAT"])
    def need(n: Need) -> None:
        assert isinstance(n.text, Untrusted)  # other agents' words are data, never instructions
        seen.append(n.text)

    @agent.on_connection_request
    def decide(request: Any) -> None:
        if request.organisation.verified_domain:
            request.accept()
        else:
            request.decline("Verified organisations only.")

    fake.need("Check VAT numbers", topics=["vat"])
    fake.need("Translate a contract", topics=["legal"])
    fake.connection_request("@good", verified_domain=True)
    fake.connection_request("@unknown")
    fake.run(agent)
    assert fake.subscriptions == [{"topics": ["vat"]}]
    assert seen == ["Check VAT numbers"]
    assert [r["decision"] for r in fake.responses] == ["accept", "decline"]


def test_post_a_need_choose_by_trial_and_call(tmp_path: Path) -> None:
    fake = FakeNetwork(handle="@buyer")
    fake.provider(
        "@slow-vat",
        "cap_vat_check",
        lambda _: {"valid": True},
        price="0.20",
        summary="validate vat",
        input_schema=VAT_SCHEMA,
        output_schema=VALID,
    )
    fake.provider(
        "@cheap-vat",
        "cap_vat_check",
        lambda a: {"valid": a["vat_number"].startswith("DE")},
        price="0.05",
        summary="validate vat",
        input_schema=VAT_SCHEMA,
        output_schema=VALID,
    )
    fake.provider(
        "@banned-vat",
        "cap_vat_check",
        lambda _: {"valid": True},
        price="0.01",
        summary="validate vat",
        excluded_by=["sourcing.min_provider_tier"],
    )
    fake.rules(rank_by="price")
    agent = _agent(fake, tmp_path)
    results: list[Any] = []

    @agent.on_start
    def ask() -> None:
        need = agent.post_need("Validate EU VAT numbers", topics=["vat"], budget="0.10 USD per call")
        fake.answer(need.post_id, "We can.", author="@cheap-vat")

    @agent.on_need_answered
    def use_it(answer: Any) -> None:
        choice = agent.choose(answer.need, sample={"vat_number": "DE123456789"}, check=lambda out: out["valid"] is True)
        results.append(choice)
        vat = agent.use(choice.main)
        results.append(vat.call({"vat_number": "DE987654321"}, criteria=lambda out: out["valid"]))
        answer.need.close("Found a provider")

    fake.run(agent)
    choice, result = results
    assert choice.main.handle == "@cheap-vat" and choice.backup.handle == "@slow-vat"  # Prefer: cheapest that passed
    assert {t.candidate.handle: t.skipped for t in choice.trials} == {
        "@cheap-vat": False,
        "@slow-vat": False,
        "@banned-vat": True,
    }  # the excluded agent is listed and never tried
    assert fake.providers["@banned-vat/cap_vat_check"].calls == 0
    assert fake.choices[0]["primary"] == "@cheap-vat"
    assert result.value == {"valid": True} and result.receipt_checked and result.charged == "0.05"
    assert fake.reports[0]["result"] == "useful" and fake.reports[0]["receipt"]
    assert fake.needs[0]["need_state"] == "resolved"


def test_a_candidate_whose_input_differs_is_skipped_unless_the_sample_adapts(tmp_path: Path) -> None:
    other = {
        "type": "object",
        "required": ["country", "number"],
        "properties": {"country": {"type": "string"}, "number": {"type": "string"}},
    }
    fake = FakeNetwork()
    fake.provider("@split-vat", "cap_vat_check", lambda _: {"valid": True}, summary="validate vat", input_schema=other)
    agent = _agent(fake, tmp_path)
    with pytest.raises(NoProvider) as none:
        agent.choose("validate vat", sample={"vat_number": "DE1"})
    [trial] = none.value.trials
    assert trial.skipped and "does not accept your sample" in trial.reason

    def sample(c: Any) -> dict[str, Any]:
        return {"vat_number": "DE1"} if "vat_number" in c.input_fields else {"country": "DE", "number": "1"}

    choice = agent.choose("validate vat", sample=sample)
    assert choice.main is not None and choice.main.handle == "@split-vat"


def test_approving_each_choice_waits_for_the_publisher(tmp_path: Path) -> None:
    fake = FakeNetwork()
    fake.provider("@vat", "cap_vat_check", lambda _: {"valid": True}, summary="validate vat")
    fake.rules(approve_each=True)
    agent = _agent(fake, tmp_path)
    approved: list[Choice] = []
    agent.on_choice_approved(approved.append)
    choice = agent.choose("validate vat", sample={"vat_number": "DE1"})
    assert choice.waiting and choice.main is not None
    with pytest.raises(NotAllowed):
        agent.use(choice.main).call({"vat_number": "DE1"})
    fake.approve(choice.decision_id)
    fake.run(agent)
    assert len(approved) == 1 and approved[0].main is not None and approved[0].main.handle == "@vat"
    assert agent.use(approved[0].main).call({"vat_number": "DE1"}).value == {"valid": True}


def test_the_same_idempotency_key_never_runs_or_pays_twice(tmp_path: Path) -> None:
    # The execution ID and salt are journaled before the first attempt.
    fake = FakeNetwork()
    vat = fake.provider("@vat", "cap_vat_check", lambda _: {"valid": True}, price="0.10", summary="validate vat")
    journal = str(tmp_path / "journal.db")
    first = Agent(fake, AgentJournal(journal))
    [candidate] = first.find("validate vat")
    one = first.use(candidate).call({"vat_number": "DE1"}, idempotency_key="supplier-42")
    restarted = Agent(fake, AgentJournal(journal))  # a new process, the same journal
    again = restarted.use(candidate).call({"vat_number": "DE1"}, idempotency_key="supplier-42")
    assert again.execution_id == one.execution_id and vat.calls == 1


def test_a_failing_callback_is_retried_then_recorded(tmp_path: Path) -> None:
    fake = FakeNetwork()
    agent = _agent(fake, tmp_path)
    tries: list[int] = []

    @agent.on_message
    def broken(message: Any) -> None:
        tries.append(1)
        raise KeyError("vat_number")

    fake.message("@peer", "hello")
    fake.run(agent)
    assert len(tries) == 5 and fake.failed and "KeyError" in fake.failed[0]["error"]


def test_async_callbacks_await_the_same_actions(tmp_path: Path) -> None:
    fake = FakeNetwork()
    agent = _agent(fake, tmp_path)

    @agent.on_message
    async def answer(message: Any) -> None:
        with pytest.raises(AgentError, match="await"):
            message.reply("not from here")  # a plain action inside a coroutine would block the loop
        await agent.aio.message(message.connection_id, "Thanks")

    fake.message("@peer", "hello")
    fake.run(agent)
    assert [m["body"] for m in fake.messages] == ["Thanks"]


def test_network_refusals_become_named_exceptions() -> None:
    refused = from_network("POLICY_DENIED", "paid use is off", {"rule": "spending.paid_use"})
    assert isinstance(refused, NotAllowed) and refused.rule == "spending.paid_use"
    pay = from_network("PAYMENT_NEEDED", "pay first", {"payment": {"url": "https://pay"}})
    assert isinstance(pay, PaymentNeeded) and pay.payment_link == "https://pay"
    assert isinstance(from_network("PROVIDER_NOT_FUNDED", "try later"), ProviderNotFunded)


def test_the_connector_forwards_signed_events_and_applies_the_answer(tmp_path: Path) -> None:
    # A server in another language answers through its own route.
    received: list[dict[str, Any]] = []

    async def app_route(request: Request) -> JSONResponse:
        body = await request.body()
        assert request.headers[SIGNATURE_HEADER] == sign("ekla_secret", body)
        event = json.loads(body)
        received.append(event)
        if event["event"] == "need":
            return JSONResponse({"reply": f"Yes: {event['matched_service']['name']}."})
        return JSONResponse({"decline": "not now"})

    publisher_app = Starlette(routes=[Route("/ekl-events", app_route, methods=["POST"])])
    fake = FakeNetwork()
    agent = _agent(fake, tmp_path)
    bridge(
        agent,
        "http://app/ekl-events",
        "ekla_secret",
        http=httpx2.AsyncClient(transport=httpx2.ASGITransport(app=publisher_app), base_url="http://app"),
    )
    fake.need("Validate VAT", matched="cap_vat", service="VAT check")
    fake.connection_request("@stranger")
    fake.run(agent)
    assert [e["event"] for e in received] == ["need", "connection_request"]
    assert fake.replies[0]["body"] == "Yes: VAT check." and fake.responses[0]["decision"] == "decline"


def test_criteria_judge_every_call_and_choose_s_check_is_passed_on_only_explicitly(tmp_path: Path) -> None:
    # Criteria given to use(...) apply to every call's report; a call without criteria is not assessed, and
    # choose's check, which may hold only for the trial's sample, applies only as criteria=choice.check.
    fake = FakeNetwork(handle="@buyer")
    fake.provider("@vat", "cap_vat_check", lambda a: {"valid": a["vat_number"].startswith("DE")}, summary="vat check")
    agent = _agent(fake, tmp_path)
    service = agent.find("vat check")[0]
    by_use = agent.use(service, criteria=lambda out: out["valid"])
    by_use.call({"vat_number": "DE1"})
    by_use.call({"vat_number": "FR1"})
    by_use.call({"vat_number": "FR2"}, criteria=lambda out: out["valid"] is False)  # the call's own criteria win
    choice = agent.choose("vat check", sample={"vat_number": "DE9"}, check=lambda out: out["valid"] is True)
    assert choice.main is not None and choice.check is not None
    agent.use(choice.main).call({"vat_number": "FR3"})  # not judged by the check
    agent.use(choice.main, criteria=choice.check).call({"vat_number": "FR4"})
    assert [r["result"] for r in fake.reports] == ["useful", "not_useful", "useful", "not_assessed", "not_useful"]
    assert [r.get("criteria_met") for r in fake.reports] == ["yes", "no", "yes", None, "no"]


def test_a_review_returned_by_on_review_due_is_submitted(tmp_path: Path) -> None:
    # The agent's own review, for the period named.
    fake = FakeNetwork(handle="@buyer")
    agent = _agent(fake, tmp_path)
    seen: list[ReviewDue] = []

    @agent.on_review_due
    def review(due: ReviewDue) -> ReviewDue:
        seen.append(due)
        return due.review(4, "partly", True, f"{due.useful} of {due.calls} were right; slow on {due.period}.")

    period_id = fake.review_due("@vat", "cap_vat_check", period="2026-10", useful=7, not_useful=3)
    fake.run(agent)
    assert seen[0].service.capability_id == "cap_vat_check" and seen[0].role == "consumer" and seen[0].closes_at
    assert fake.reviews == [
        {
            "collaboration_id": seen[0].collaboration_id,
            "review_period_id": period_id,
            "rating": 4,
            "criteria_met": "partly",
            "would_use_again": True,
            "comment": "7 of 10 were right; slow on 2026-10.",
        }
    ]
    assert "on_review_due" in fake.handlers


def test_by_default_the_consumer_submits_the_derived_review_and_the_provider_nothing(tmp_path: Path) -> None:
    # No callback, or a callback returning None; only assessed calls count.
    fake = FakeNetwork(handle="@buyer")
    agent = _agent(fake, tmp_path)
    fake.review_due("@vat", "cap_vat_check", useful=9, not_useful=1, not_assessed=30)
    fake.review_due("@ocr", "cap_ocr", period="2026-09", useful=5, not_useful=3)
    fake.review_due("@pdf", "cap_pdf", period="2026-08", useful=1, not_useful=7)
    fake.review_due("@unjudged", "cap_idle", not_assessed=6)  # calls, none assessed: nothing to derive from
    fake.review_due("@client", "cap_mine", role="provider", useful=4)
    fake.run(agent)
    assert [(r["rating"], r["criteria_met"], r["would_use_again"], r["method"]) for r in fake.reviews] == [
        (5, "yes", True, "derived"),
        (3, "partly", False, "derived"),  # 1 + round(4 x 5/8): halves round to even
        (1, "no", False, "derived"),  # 1 + round_half_even(4 x 1/8) = 1
    ]

    quiet = FakeNetwork(handle="@buyer")
    agent = _agent(quiet, tmp_path / "quiet")

    @agent.on_review_due
    def look(due: ReviewDue) -> None:
        assert (due.calls, due.not_assessed) == (6, 2)

    quiet.review_due("@vat", "cap_vat_check", useful=1, not_useful=3, not_assessed=2)
    quiet.run(agent)
    assert [(r["rating"], r["criteria_met"], r["would_use_again"]) for r in quiet.reviews] == [(2, "no", False)]


def test_skip_submits_nothing_and_a_settled_period_is_quiet(tmp_path: Path) -> None:
    fake = FakeNetwork(handle="@buyer")
    agent = _agent(fake, tmp_path)

    @agent.on_review_due
    def decide(due: ReviewDue) -> ReviewDue | None:
        return due.skip() if due.counterpart == "@later" else None

    fake.review_due("@later", "cap_vat_check", useful=3)
    # A period already reviewed or closed arrives again: refused by the network, and not a failure.
    fake.event("review.requested", {"review_period_id": "rp_gone", "role": "consumer", "useful": 2}, "col_1")
    fake.run(agent)
    assert fake.reviews == [] and fake.failed == []
    with pytest.raises(ValueError, match="rating"):
        ReviewDue.from_event({"collaboration_id": "col_1"}).review(6, "yes", True)


def test_the_connector_answers_review_due_with_a_review_skip_or_nothing(tmp_path: Path) -> None:
    # A callback answers {"review": {...}}, {"skip": true}, or nothing for the default.
    received: list[dict[str, Any]] = []
    answers = {
        "@reviewed": {"review": {"rating": 3, "criteria_met": "partly", "would_use_again": False, "comment": "Mixed."}},
        "@skipped": {"skip": True},
    }

    async def app_route(request: Request) -> JSONResponse:
        event = json.loads(await request.body())
        received.append(event)
        return JSONResponse(answers.get(event["counterpart"], {}))

    publisher_app = Starlette(routes=[Route("/ekl-events", app_route, methods=["POST"])])
    fake = FakeNetwork(handle="@buyer")
    agent = _agent(fake, tmp_path)
    bridge(
        agent,
        "http://app/ekl-events",
        "ekla_secret",
        http=httpx2.AsyncClient(transport=httpx2.ASGITransport(app=publisher_app), base_url="http://app"),
    )
    for counterpart in ("@reviewed", "@skipped", "@default"):
        fake.review_due(counterpart, "cap_vat_check", useful=2)
    fake.run(agent)
    assert {e["event"] for e in received} == {"review_due"}
    assert received[0]["service"] == {"capability_id": "cap_vat_check", "version": "1.0.0"}
    assert [(r["rating"], r.get("method")) for r in fake.reviews] == [(3, None), (5, "derived")]


def test_the_local_api_starts_the_agents_own_actions(tmp_path: Path) -> None:
    fake = FakeNetwork()
    fake.provider("@vat", "cap_vat_check", lambda _: {"valid": True}, summary="vat check")
    agent = _agent(fake, tmp_path)
    with TestClient(local_api(agent)) as client:
        made = client.post("/agent/post_need", json={"text": "Validate VAT", "topics": ["vat"]}).json()
        called = client.post(
            "/agent/call", json={"handle": "@vat", "capability_id": "cap_vat_check", "arguments": {"vat_number": "DE1"}}
        ).json()
        missing = client.post("/agent/nothing", json={})
    assert made["need_id"].startswith("post_") and called["value"] == {"valid": True}
    assert missing.status_code == 404


def test_a_paid_call_waits_for_the_providers_approval(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Every paid run waits for its provider's approval; nothing reaches the provider before it.
    monkeypatch.setattr("ekl_provider_adapter.agent.asyncio.sleep", _no_wait)
    fake = FakeNetwork()
    vat = fake.provider("@vat", "cap_vat_check", lambda _: {"valid": True}, price="0.10", summary="validate vat")
    agent = Agent(fake, AgentJournal(str(tmp_path / "journal.db")))
    [candidate] = agent.find("validate vat")
    fake.approval = "approve"
    assert agent.use(candidate).call({"vat_number": "DE1"}).value == {"valid": True} and vat.calls == 1
    fake.approval = "payment"
    with pytest.raises(PaymentNeeded) as unpaid:
        agent.use(candidate).call({"vat_number": "DE2"})
    assert unpaid.value.payment_link == "https://buy.stripe.com/fake"
    fake.approval = "decline"
    with pytest.raises(RunNotApproved):
        agent.use(candidate).call({"vat_number": "DE3"})
    assert vat.calls == 1  # neither unapproved run reached the provider


async def _no_wait(_: float) -> None:
    return None


def test_without_a_handler_the_consumer_is_sent_to_the_payment_page() -> None:
    agent = AgentFile.model_validate(
        {
            "ekl": "1.0",
            "mode": "service_provider",
            "execution_mode": "durable",
            "payment": {"link": "https://buy.stripe.com/northwind?plan=pack"},
            "capabilities": [
                {
                    "tool": "extract",
                    "class": "read_only",
                    "plans": [{"plan_id": "pack500", "type": "pack", "price": "8.00", "calls": 500}],
                }
            ],
        }
    )
    run = RunRequest(
        reservation_id="rsv_1",
        execution_id="ex_1",
        consumer={"agent_id": "agt_1"},
        capability_id="cap_extract",
        plan_id="pack500",
        unit_price="0.016",
    )
    decision = default_decision(agent, run)
    assert decision.decision == "payment_needed"
    assert decision.fields["payment"] == {
        "url": "https://buy.stripe.com/northwind?plan=pack&ref=rsv_1",
        "amount": "8.00",
        "currency": "USD",
        "description": "500 runs",
    }
    assert default_decision(agent.model_copy(update={"payment": None}), run).decision == "decline"


def test_the_approval_handler_answers_each_waiting_run() -> None:
    answered: list[tuple[str, RunDecision]] = []

    class Api:
        def __init__(self) -> None:
            self.rounds = 0

        async def next(self, _wait: int) -> list[RunRequest]:
            self.rounds += 1
            if self.rounds > 1:
                raise asyncio.CancelledError
            return [
                RunRequest(reservation_id="rsv_9", execution_id="ex_9", consumer={"agent_id": "a"}, capability_id="c")
            ]

        async def decide(self, reservation_id: str, decision: RunDecision) -> dict[str, Any]:
            answered.append((reservation_id, decision))
            return {}

    runs = Runs(Api(), lambda: None)

    @runs.on_approval
    async def approve(run: RunRequest) -> RunDecision:
        return RunDecision.approve(runs=10)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(runs.watch())
    [(reservation_id, decision)] = answered
    assert reservation_id == "rsv_9" and decision.fields == {"unit": "plan", "standing": {"runs": 10}}


def test_a_need_can_run_shorter_or_longer_than_two_weeks(tmp_path: Path) -> None:
    fake = FakeNetwork()
    agent = Agent(fake, AgentJournal(str(tmp_path / "journal.db")))
    agent.post_need("Validate EU VAT numbers", expires_days=7)
    assert fake.needs[-1]["expires_days"] == 7  # set when posted, the extension still unused
    with pytest.raises(ValueError):
        agent.post_need("Too long", expires_days=40)
