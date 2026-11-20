# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The connector works both ways: the agent's side, its events and actions, run against testing.FakeNetwork."""

import asyncio
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx2
import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from ekl_provider_adapter import callback_signing as cb
from ekl_provider_adapter.agent import Agent, Choice, Need, ReviewDue, Untrusted, expectation
from ekl_provider_adapter.agent_journal import AgentJournal
from ekl_provider_adapter.callbacks import bridge, local_api
from ekl_provider_adapter.connector import use_only_app
from ekl_provider_adapter.errors import (
    AgentError,
    AgentNotEnabled,
    BalanceLow,
    CallFinished,
    NoProvider,
    NotAllowed,
    from_network,
)
from ekl_provider_adapter.testing import FakeNetwork
from tests.conftest import use

VAT_SCHEMA = {"type": "object", "required": ["vat_number"], "properties": {"vat_number": {"type": "string"}}}
VALID = {"type": "object", "required": ["valid"], "properties": {"valid": {"type": "boolean"}}}


def _agent(fake: FakeNetwork, tmp_path: Path) -> Agent:
    return Agent(fake, AgentJournal(str(tmp_path / "journal.db")))


def test_without_the_agent_credential_the_connector_only_serves(monkeypatch: pytest.MonkeyPatch) -> None:
    # No credential, no event loop, and every action names the variable to set.
    monkeypatch.delenv("EKL_AGENT_CREDENTIAL", raising=False)
    agent = Agent(None)
    assert not agent.enabled
    with pytest.raises(AgentNotEnabled, match="EKL_AGENT_CREDENTIAL"):
        agent.post_need("Validate VAT numbers")


def test_a_matching_need_is_answered(tmp_path: Path) -> None:
    # One on_need callback answers matching needs; the network learns the agent answers them itself.
    fake = FakeNetwork(handle="@vat-check")
    agent = _agent(fake, tmp_path)

    @agent.on_need(matching="my_services")
    def answer(need: Need) -> None:
        assert need.matched_service is not None
        need.reply(f"I can do this with {need.matched_service.name}, {need.matched_service.price}.")

    fake.need(
        "Validate EU VAT numbers", author="@buyer", matched="cap_vat", service="Validate VAT", price="$0.05 / call"
    )
    fake.run(agent)
    assert [r["body"] for r in fake.replies] == ["I can do this with Validate VAT, $0.05 / call."]
    assert "on_need" in fake.handlers


def test_a_connection_request_answered_already_is_settled_not_failed(tmp_path: Path) -> None:
    """A console rule answered it first, while the callbacks were starting: answering again settles it, and the
    callback isn't retried and recorded as failed."""
    fake = FakeNetwork()
    agent = _agent(fake, tmp_path)
    fake.connections["con_1"] = {"connection_id": "con_1", "state": "declined"}
    answered = agent._sync(agent.aio.respond("con_1", True, ""))
    assert answered == {"connection_id": "con_1", "already_answered": True} and fake.responses == []


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
        choice = agent.choose(
            answer.need,
            sample={"vat_number": "DE123456789"},
            expect={"checks": [{"id": "valid", "path": "$.valid", "equals": True}]},
        )
        results.append(choice)
        vat = use(agent, choice.main)
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


def test_a_reply_to_a_need_posted_in_this_run_carries_the_need_s_words(tmp_path: Path) -> None:
    # choose searches with the need's text: without it, the network finds nothing and the answer is lost.
    fake = FakeNetwork()
    fake.provider(
        "@holidays",
        "cap_is_holiday",
        lambda a: {"holiday": a["day"] == "2026-11-26"},
        summary="public holiday by country",
    )
    fake.provider("@translate", "cap_translate", lambda _: {"holiday": True}, summary="translate text")
    agent = _agent(fake, tmp_path)
    seen: list[Any] = []

    @agent.on_start
    def ask() -> None:
        need = agent.post_need("Is a day a public holiday, by country", topics=["holidays"])
        fake.answer(need.post_id, "Is it a holiday: free.", author="@holidays")

    @agent.on_need_answered
    def use_it(answer: Any) -> None:
        seen.append(answer.need)
        seen.append(
            agent.choose(
                answer.need,
                sample={"day": "2026-11-26"},
                expect={"checks": [{"id": "holiday", "path": "$.holiday", "equals": True}]},
            )
        )

    fake.run(agent)
    need, choice = seen
    assert str(need.text) == "Is a day a public holiday, by country" and need.topics == ["holidays"]
    assert [t.candidate.handle for t in choice.trials] == ["@holidays"]


def test_an_agreement_declares_the_data_class_the_agent_shares(tmp_path: Path) -> None:
    # A new agent's rules share public and synthetic data only: a call must work under them without saying more.
    fake = FakeNetwork()
    fake.provider("@holidays", "cap_is_holiday", lambda _: {"holiday": False}, summary="public holiday by country")
    agent = _agent(fake, tmp_path)
    [holidays] = agent.find("public holiday")
    assert use(agent, holidays).call({"day": "2026-11-12"}).value == {"holiday": False}
    assert [a["data_class"] for a in fake.agreements.values()] == ["public"]
    with pytest.raises(NotAllowed):
        use(agent, holidays, data_class="internal").call({"day": "2026-11-13"}, idempotency_key="internal-1")
    with pytest.raises(ValueError):
        use(agent, holidays, data_class="secret")


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
        use(agent, choice.main).call({"vat_number": "DE1"})
    fake.approve(choice.decision_id)
    fake.run(agent)
    assert len(approved) == 1 and approved[0].main is not None and approved[0].main.handle == "@vat"
    assert use(agent, approved[0].main).call({"vat_number": "DE1"}).value == {"valid": True}


def test_the_same_idempotency_key_never_runs_or_pays_twice(tmp_path: Path) -> None:
    # The execution ID and salt are journaled before the first attempt, so a free call repeated from a new process
    # reaches the same execution, which the provider answers again without new work. A paid one: the next test.
    fake = FakeNetwork()
    vat = fake.provider("@vat", "cap_vat_check", lambda _: {"valid": True}, summary="validate vat")
    journal = str(tmp_path / "journal.db")
    first = Agent(fake, AgentJournal(journal))
    [candidate] = first.find("validate vat")
    one = use(first, candidate).call({"vat_number": "DE1"}, idempotency_key="supplier-42")
    restarted = Agent(fake, AgentJournal(journal))  # a new process, the same journal
    again = use(restarted, candidate).call({"vat_number": "DE1"}, idempotency_key="supplier-42")
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


def test_a_failing_on_start_never_stops_the_agent_reading_its_events(tmp_path: Path) -> None:
    fake = FakeNetwork()
    agent = _agent(fake, tmp_path)
    seen: list[str] = []

    @agent.on_start
    def broken() -> None:
        raise RuntimeError("the start went wrong")

    @agent.on_message
    def note(message: Any) -> None:
        seen.append(str(message.text))

    fake.message("@peer", "still heard")
    fake.run(agent)
    assert seen == ["still heard"]


def test_a_message_carries_its_own_id_so_a_redelivery_can_be_told_apart(tmp_path: Path) -> None:
    fake = FakeNetwork()
    agent = _agent(fake, tmp_path)
    seen: list[str] = []

    @agent.on_message
    def note(message: Any) -> None:
        seen.append(message.message_id)

    fake.message("@peer", "hello")
    fake.message("@peer", "hello again")
    fake.run(agent)
    assert len(seen) == 2 and all(s.startswith("msg_") for s in seen) and seen[0] != seen[1]


def test_network_refusals_become_named_exceptions() -> None:
    refused = from_network("POLICY_DENIED", "paid use is off", {"rule": "spending.paid_use"})
    assert isinstance(refused, NotAllowed) and refused.rule == "spending.paid_use"
    low = from_network("BALANCE_LOW", "the wallet is low", {"needed": "8.00", "currency": "USD"})
    assert isinstance(low, BalanceLow) and (low.needed, low.currency) == ("8.00", "USD")
    assert "Account → Money → Buying" in str(low)


def test_the_connector_forwards_signed_events_and_applies_the_answer(tmp_path: Path) -> None:
    # The callbacks file the connector runs answers each event it is posted.
    received: list[dict[str, Any]] = []

    async def callbacks_route(request: Request) -> JSONResponse:
        body = await request.body()
        assert cb.REQUEST_HEADER in request.headers and request.headers[cb.LEGACY_HEADER] == cb.legacy_signature(
            "ekla_secret", body
        )
        event = json.loads(body)
        received.append(event)
        if event["event"] == "need":
            return JSONResponse({"reply": f"Yes: {event['matched_service']['name']}."})
        return JSONResponse({"decline": "not now"})

    callbacks_file = Starlette(routes=[Route("/ekl-events", callbacks_route, methods=["POST"])])
    fake = FakeNetwork()
    agent = _agent(fake, tmp_path)
    bridge(
        agent,
        "http://127.0.0.1/ekl-events",
        "ekla_secret",
        http=httpx2.AsyncClient(transport=httpx2.ASGITransport(app=callbacks_file), base_url="http://127.0.0.1"),
    )
    fake.need("Validate VAT", matched="cap_vat", service="VAT check")
    fake.connection_request("@stranger")
    fake.run(agent)
    assert [e["event"] for e in received] == ["need", "connection_request"]
    assert fake.replies[0]["body"] == "Yes: VAT check." and fake.responses[0]["decision"] == "decline"


def test_an_answer_to_a_decision_the_code_cannot_take_is_not_retried(tmp_path: Path) -> None:
    # A first contact is decided in the console: the callbacks' answer can't be applied, so the event isn't failed and
    # retried five times; the decision's fallback applies.
    async def callbacks_route(request: Request) -> JSONResponse:
        return JSONResponse({"accept": True})

    callbacks_file = Starlette(routes=[Route("/ekl-events", callbacks_route, methods=["POST"])])
    fake = FakeNetwork()
    agent = _agent(fake, tmp_path)
    bridge(
        agent,
        "http://127.0.0.1/ekl-events",
        "ekla_secret",
        http=httpx2.AsyncClient(transport=httpx2.ASGITransport(app=callbacks_file), base_url="http://127.0.0.1"),
    )
    fake.decision("outreach", "out_123")
    fake.run(agent)
    assert not fake.failed


def test_agent_code_talks_in_outreach_threads_before_connecting(tmp_path: Path) -> None:
    # A first contact reaches on_outreach with its intent and refs; the agent answers in the thread, and hears the
    # replies to a thread it opened itself.
    fake = FakeNetwork(handle="@seller")
    agent = _agent(fake, tmp_path)
    seen: list[tuple[str, str, str]] = []

    @agent.on_outreach
    def talk(o: Any) -> None:
        seen.append((o.state, o.sender, str(o.text)))
        if o.state == "received":
            o.reply(f"Yes, up to 10,000 a day. Ask about {o.refs['capability_id']} when you connect.")
        elif o.state == "replied":
            o.reply("Thanks, I'll request a connection.")

    asked = fake.outreach("@buyer", "Volume?", intent="request_service", refs={"capability_id": "cap_vat"})
    opened = agent.outreach("@other", "introduction", "Hello from @seller.")["thread_id"]
    fake.outreach("@other", "Hello back.", state="replied", thread_id=opened)
    fake.run(agent)
    assert seen == [("received", "@buyer", "Volume?"), ("replied", "@other", "Hello back.")]
    assert agent.handler_names().count("on_outreach") == 1
    assert [(m["thread_id"], m.get("action")) for m in fake.outreach_sent] == [
        (opened, None),
        (asked, "reply"),
        (opened, "reply"),
    ]


def test_the_callbacks_file_answers_outreach_and_a_closed_thread_is_not_retried(tmp_path: Path) -> None:
    received: list[dict[str, Any]] = []

    async def callbacks_route(request: Request) -> JSONResponse:
        event = json.loads(await request.body())
        received.append(event)
        if event["state"] == "received":
            return JSONResponse({"decline": "We don't take introductions."})
        return JSONResponse({"reply": "Noted."})

    callbacks_file = Starlette(routes=[Route("/ekl-events", callbacks_route, methods=["POST"])])
    fake = FakeNetwork(handle="@seller")
    agent = _agent(fake, tmp_path)
    bridge(
        agent,
        "http://127.0.0.1/ekl-events",
        "ekla_secret",
        http=httpx2.AsyncClient(transport=httpx2.ASGITransport(app=callbacks_file), base_url="http://127.0.0.1"),
    )
    declined = fake.outreach("@stranger", "Hi, can we talk?", intent="introduction")
    expired = fake.outreach("@buyer", "Still there?", state="replied")
    fake.threads[expired]["state"] = "expired"  # closed before the callbacks answered
    fake.run(agent)
    assert [(e["event"], e["state"], e["from"], e["text"]) for e in received] == [
        ("outreach", "received", "@stranger", "Hi, can we talk?"),
        ("outreach", "replied", "@buyer", "Still there?"),
    ]
    assert received[0]["intent"] == "introduction" and received[0]["thread_id"] == declined
    assert fake.threads[declined]["state"] == "declined" and not fake.failed
    assert fake.outreach_sent == [{"thread_id": declined, "action": "decline", "body": "We don't take introductions."}]


def test_the_local_api_opens_an_outreach_thread_and_replies_in_it(tmp_path: Path) -> None:
    fake = FakeNetwork(handle="@buyer")
    agent = _agent(fake, tmp_path)
    with TestClient(local_api(agent), base_url="http://127.0.0.1:8765") as client:
        opened = client.post(
            "/agent/outreach",
            json={"to": "@seller", "intent": "request_service", "capability_id": "cap_vat", "text": "Volume?"},
        ).json()
        replied = client.post("/agent/outreach", json={"thread_id": opened["thread_id"], "text": "And price?"})
        wrong = client.post("/agent/outreach", json={"to": "@seller", "intent": "chat", "text": "Hi"})
    assert fake.outreach_sent[0] == {
        "thread_id": opened["thread_id"],
        "recipient": "@seller",
        "intent": "request_service",
        "refs": {"capability_id": "cap_vat"},
        "body": "Volume?",
    }
    assert replied.json()["state"] == "replied" and wrong.status_code == 409


def test_the_local_api_connects_from_an_outreach_thread(tmp_path: Path) -> None:
    # After talking in a thread, the callbacks ask to connect from it: the thread counts as converted, and a second
    # ask finds the connection already there and requests nothing.
    fake = FakeNetwork(handle="@buyer")
    fake.provider("@holidays", "cap_banking_day", lambda _: {"banking_day": True}, summary="banking day")
    with TestClient(local_api(_agent(fake, tmp_path)), base_url="http://127.0.0.1:8765") as client:
        opened = client.post(
            "/agent/outreach",
            json={"to": "@holidays", "intent": "request_service", "capability_id": "cap_banking_day", "text": "JP?"},
        ).json()
        body = {"handle": "@holidays", "capability_id": "cap_banking_day", "thread_id": opened["thread_id"]}
        connected = client.post("/agent/connect", json=body).json()
        again = client.post("/agent/connect", json=body).json()
    assert connected["connection_id"].startswith("con_") and fake.threads[opened["thread_id"]]["state"] == "converted"
    assert again == {"connection_id": connected["connection_id"], "state": "active"} and len(fake.connections) == 1


def test_criteria_judge_every_call_and_choose_s_expectation_never_does(tmp_path: Path) -> None:
    # Criteria given to use(...) apply to every call's report; a call without criteria is not assessed, and
    # choose's expectation, scored by the network on the trial's sample, judges no later call.
    fake = FakeNetwork(handle="@buyer")
    fake.provider("@vat", "cap_vat_check", lambda a: {"valid": a["vat_number"].startswith("DE")}, summary="vat check")
    agent = _agent(fake, tmp_path)
    service = agent.find("vat check")[0]
    by_use = use(agent, service, criteria=lambda out: out["valid"])
    by_use.call({"vat_number": "DE1"})
    by_use.call({"vat_number": "FR1"})
    by_use.call({"vat_number": "FR2"}, criteria=lambda out: out["valid"] is False)  # the call's own criteria win
    expect = {"checks": [{"id": "valid", "path": "$.valid", "equals": True}]}
    choice = agent.choose("vat check", sample={"vat_number": "DE9"}, expect=expect)
    assert choice.main is not None and choice.trials[0].score == 1.0
    use(agent, choice.main).call({"vat_number": "FR3"})  # not judged by the expectation
    use(agent, choice.main, criteria=lambda out: out["valid"]).call({"vat_number": "FR4"})
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

    async def callbacks_route(request: Request) -> JSONResponse:
        event = json.loads(await request.body())
        received.append(event)
        return JSONResponse(answers.get(event["counterpart"], {}))

    callbacks_file = Starlette(routes=[Route("/ekl-events", callbacks_route, methods=["POST"])])
    fake = FakeNetwork(handle="@buyer")
    agent = _agent(fake, tmp_path)
    bridge(
        agent,
        "http://127.0.0.1/ekl-events",
        "ekla_secret",
        http=httpx2.AsyncClient(transport=httpx2.ASGITransport(app=callbacks_file), base_url="http://127.0.0.1"),
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
    with TestClient(local_api(agent), base_url="http://127.0.0.1:8765") as client:
        made = client.post("/agent/post_need", json={"text": "Validate VAT", "topics": ["vat"]}).json()
        called = client.post(
            "/agent/call", json={"handle": "@vat", "capability_id": "cap_vat_check", "arguments": {"vat_number": "DE1"}}
        ).json()
        closed = client.post("/agent/close_agreement", json={"handle": "@vat", "capability_id": "cap_vat_check"}).json()
        missing = client.post("/agent/nothing", json={})
    assert made["need_id"].startswith("post_") and called["value"] == {"valid": True}
    assert closed["engagement_state"] == "completed"
    assert missing.status_code == 404


def test_callbacks_find_try_and_choose_with_their_own_logic(tmp_path: Path) -> None:
    # A callback picks a provider by its own rules, so find lists what they may weigh, trial tries the one it picked
    # on its own sample, never charged, and choose reports every trial it ran.
    fake = FakeNetwork()
    fake.provider("@fx", "cap_convert", lambda a: {"converted": a["amount"] * 2}, price="0.20", summary="convert")
    fake.provider("@keeper", "cap_convert", lambda _: {"converted": 1}, summary="convert", excluded_by=["retention"])
    with TestClient(local_api(_agent(fake, tmp_path)), base_url="http://127.0.0.1:8765") as client:
        found = client.post("/agent/find", json={"query": "convert"}).json()["candidates"]
        tried = client.post(
            "/agent/trial",
            json={
                "handle": "@fx",
                "capability_id": "cap_convert",
                "sample": {"amount": 5},
                "expect": {"checks": [{"id": "converted", "path": "$.converted", "approx": 10, "tolerance": 0.01}]},
            },
        ).json()
        kept_out = client.post(
            "/agent/trial", json={"handle": "@keeper", "capability_id": "cap_convert", "sample": {"amount": 5}}
        ).json()
        chosen = client.post("/agent/choose", json={"query": "convert", "sample": {"amount": 5}}).json()
    fx = next(c for c in found if c["handle"] == "@fx")
    assert fx["eligible"] and fx["currency"] == "USD" and "reviews" in fx and "plans" in fx
    assert next(c for c in found if c["handle"] == "@keeper")["excluded_by"] == ["retention"]
    # The score against the callbacks' own expectation, never the output.
    assert tried["passed"] and tried["score"] == 1.0 and "output" not in tried
    assert [c["id"] for c in tried["checks"]] == ["schema", "converted"]
    assert fake.trials[0]["expect"]["checks"][0]["approx"] == 10
    assert kept_out["skipped"] and "retention" in kept_out["reason"]
    assert chosen["main"]["handle"] == "@fx"
    assert {t["handle"]: t["passed"] or t["skipped"] for t in chosen["trials"]} == {"@fx": True, "@keeper": True}


def test_no_provider_tells_the_callbacks_every_trial_and_why(tmp_path: Path) -> None:
    # Spec 13.15.7.4: choose is refused with NO_PROVIDER, the trials attached, so the callbacks can post a need.
    fake = FakeNetwork()
    fake.provider("@keeper", "cap_convert", lambda _: {"converted": 1}, summary="convert", excluded_by=["retention"])
    fake.provider("@wrong", "cap_convert", lambda _: {"converted": 3}, summary="convert")
    expect = {"checks": [{"id": "converted", "path": "$.converted", "equals": 10}], "threshold": 1.0}
    with TestClient(local_api(_agent(fake, tmp_path)), base_url="http://127.0.0.1:8765") as client:
        refused = client.post("/agent/choose", json={"query": "convert", "sample": {"amount": 5}, "expect": expect})
    assert refused.status_code == 409 and refused.json()["error"] == "NO_PROVIDER"
    trials = {t["handle"]: t for t in refused.json()["detail"]["trials"]}
    assert trials["@keeper"]["skipped"] and "retention" in trials["@keeper"]["reason"]
    assert trials["@wrong"]["passed"] is False and trials["@wrong"]["score"] is not None
    assert "output" not in str(refused.json())


def test_the_local_api_answers_a_low_wallet_with_what_the_pack_needs(tmp_path: Path) -> None:
    # Apps in other languages map on `error` and show `detail`: the pack's price and its currency.
    fake = FakeNetwork()
    fake.provider("@vat", "cap_vat_check", lambda _: {"valid": True}, price="0.10", summary="vat check")
    fake.wallet = Decimal(0)
    with TestClient(local_api(_agent(fake, tmp_path)), base_url="http://127.0.0.1:8765") as client:
        refused = client.post(
            "/agent/call", json={"handle": "@vat", "capability_id": "cap_vat_check", "arguments": {"vat_number": "DE1"}}
        )
        gone = client.post("/agent/decide_run", json={"reservation_id": "rsv_1", "decision": "approve"})
    body = refused.json()
    assert refused.status_code == 402 and body["error"] == "BALANCE_LOW"
    assert body["detail"] == {"needed": "0.10", "currency": "USD"} and body["message"]
    assert gone.status_code == 404


def test_a_paid_call_is_paid_from_the_wallet_without_waiting(tmp_path: Path) -> None:
    # Nobody approves a paid run: it buys its pack from the wallet at once, offering the pack's whole price, or is
    # refused with BALANCE_LOW before anything reaches the provider.
    fake = FakeNetwork()
    vat = fake.provider("@vat", "cap_vat_check", lambda _: {"valid": True}, price="0.10", summary="validate vat")
    agent = _agent(fake, tmp_path)
    [candidate] = agent.find("validate vat")
    fake.wallet = Decimal("0.10")
    result = use(agent, candidate).call({"vat_number": "DE1"})
    assert result.value == {"valid": True} and result.charged == "0.10" and vat.calls == 1
    assert fake.reserved[0]["requested_max_amount_atoms"] == "100000"
    assert "approval_wait_seconds" not in fake.reserved[0]
    with pytest.raises(BalanceLow) as low:
        use(agent, candidate).call({"vat_number": "DE2"})
    assert low.value.code == "BALANCE_LOW" and low.value.needed == "0.10" and vat.calls == 1


def test_a_run_offers_the_whole_pack_and_later_runs_use_its_calls(tmp_path: Path) -> None:
    # The first run buys the pack, holding its whole price; the next ones take its calls and cost nothing more.
    fake = FakeNetwork()
    pack = {"plan_id": "pack-500", "type": "pack", "price": "5.00", "calls": 500}
    fake.provider("@fx", "cap_convert_currency", lambda _: {"converted": 1.1}, summary="convert currency", plan=pack)
    agent = _agent(fake, tmp_path)
    [candidate] = agent.find("convert currency")
    fx = use(agent, candidate)
    first = fx.call({"amount": 1, "from_currency": "EUR", "to_currency": "USD"})
    second = fx.call({"amount": 2, "from_currency": "EUR", "to_currency": "USD"})
    assert first.value == {"converted": 1.1} and (first.charged, second.charged) == ("5.00", "0.00")
    assert {r["requested_max_amount_atoms"] for r in fake.reserved} == {"5000000"}
    assert fake._tool_packs_list()["packs"][0]["calls_left"] == 498


def test_closing_an_agreement_opens_its_review_and_the_next_call_agrees_again(tmp_path: Path) -> None:
    fake = FakeNetwork()
    fake.provider("@fx", "cap_convert_currency", lambda _: {"converted": 1.1}, summary="convert currency")
    agent = Agent(fake, AgentJournal(str(tmp_path / "journal.db")))
    [candidate] = agent.find("convert currency")
    fx = use(agent, candidate)
    fx.call({"amount": 1})
    [first] = fake.agreements
    assert fx.close()["engagement_state"] == "completed" and fake.agreements[first]["engagement_state"] == "completed"
    fx.call({"amount": 2})
    assert len(fake.agreements) == 2  # a later call agrees terms again
    fx.close()
    with pytest.raises(AgentError):
        fx.close()  # nothing open to close


def test_a_need_can_run_shorter_or_longer_than_two_weeks(tmp_path: Path) -> None:
    fake = FakeNetwork()
    agent = Agent(fake, AgentJournal(str(tmp_path / "journal.db")))
    agent.post_need("Validate EU VAT numbers", expires_days=7)
    assert fake.needs[-1]["expires_days"] == 7  # set when posted, the extension still unused
    with pytest.raises(ValueError):
        agent.post_need("Too long", expires_days=40)


def test_an_agent_that_only_uses_services_announces_itself_and_waits_to_be_confirmed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Nothing serving calls announces it: the use-only connector finds its agent.json, announces it with no address,
    # and holds its callbacks back until its publisher has confirmed it.
    uses_only = {
        "ekl": "1.0",
        "mode": "active_participant",
        "execution_mode": "storage_free",
        "agent": {"handle": "@ap-inbox", "display_name": "AP Inbox", "purpose": "Uses document services."},
        "adapter": {},
        "capabilities": [],
    }
    (tmp_path / "agent.json").write_text(json.dumps(uses_only))
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("EKL_AGENT_FILE", raising=False)
    monkeypatch.setenv("EKL_AGENT_CREDENTIAL", "ekla_test")
    monkeypatch.setenv("EKL_PROVIDER_CREDENTIAL", "eklp_test")
    monkeypatch.setattr("ekl_provider_adapter.agent.ACTIVE_POLL_SECONDS", 0.01)
    agent = use_only_app().state.ekl_agent
    assert agent._announcement is not None
    api, config = agent._announcement
    announced: list[tuple[str, dict[str, Any]]] = []

    async def announce(endpoint: str, sent: dict[str, Any]) -> Any:
        announced.append((endpoint, sent))
        return type("Answer", (), {"version": 1})()

    asked = 0

    async def identify() -> str:
        nonlocal asked
        asked += 1
        return "active" if asked == 3 else "review"

    monkeypatch.setattr(api, "announce", announce)
    monkeypatch.setattr(agent, "_identify", identify)
    asyncio.run(agent._ensure_ready())
    assert announced == [("", config)] and config["capabilities"] == [] and asked == 3 and agent._ready
    # An agent whose agent.json offers services is announced by the connector serving its calls instead.
    (tmp_path / "agent.json").write_text(
        json.dumps(uses_only | {"capabilities": [{"tool": "x", "class": "read_only"}]})
    )
    assert use_only_app().state.ekl_agent._announcement is None


def test_an_agent_started_before_it_is_confirmed_reads_its_events_once_it_is(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The guide's order: deploy, then confirm the agent. Its connector announces it, so the agent's side has
    # nothing to announce; until it is active it may read itself but not subscribe, so it waits, then subscribes and
    # reads, instead of ending its event loop on the first refusal.
    monkeypatch.setattr("ekl_provider_adapter.agent.ACTIVE_POLL_SECONDS", 0.01)
    fake = FakeNetwork()
    agent = Agent(fake, AgentJournal(str(tmp_path / "journal.db")))
    reads = _confirmed_on_read(fake, 3)
    subscribed: list[Any] = []
    own = fake._tool_feed_subscribe

    def subscribe(**arguments: Any) -> dict[str, Any]:
        assert fake.state == "active", "subscribed before the agent was confirmed"
        subscribed.append(arguments)
        return own(**arguments)

    fake._tool_feed_subscribe = subscribe  # type: ignore[method-assign]

    @agent.on_need(matching="subscribed", topics=["currency"])
    def answer(need: Any) -> None:
        pass

    asyncio.run(agent._ensure_ready())
    asyncio.run(agent._subscribe())
    assert len(reads) == 3 and agent._ready and subscribed == [{"topics": ["currency"]}]


def test_the_use_only_connector_announces_an_agent_that_offers_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # An agent that only uses services runs the connector with --use-only: nothing else announces it, so the
    # connector announces its own agent.json.
    uses_only = {
        "ekl": "1.0",
        "mode": "active_participant",
        "execution_mode": "storage_free",
        "agent": {"handle": "@order-desk", "display_name": "Order Desk", "purpose": "Takes orders."},
        "adapter": {},
        "capabilities": [],
    }
    (tmp_path / "agent.json").write_text(json.dumps(uses_only))
    monkeypatch.setenv("EKL_AGENT_FILE", str(tmp_path / "agent.json"))
    monkeypatch.setenv("EKL_AGENT_CREDENTIAL", "ekla_test")
    monkeypatch.setenv("EKL_PROVIDER_CREDENTIAL", "eklp_test")
    own = use_only_app().state.ekl_agent
    assert own._announcement is not None and own._announcement[1]["agent"]["handle"] == "@order-desk"
    # Without the agent credential it takes no part, and announces nothing.
    monkeypatch.delenv("EKL_AGENT_CREDENTIAL")
    assert use_only_app().state.ekl_agent._announcement is None


class _Announcer:
    """Stands in for the provider API an agent without a server announces its agent.json with."""

    def __init__(self) -> None:
        self.announced: list[str] = []

    async def announce(self, endpoint: str, _sent: dict[str, Any]) -> Any:
        self.announced.append(endpoint)
        return type("Answer", (), {"version": 1})()


def _confirmed_on_read(fake: FakeNetwork, read: int) -> list[int]:
    """The publisher confirms the agent while it waits: agent_get_self reports it active from this read on."""
    reads: list[int] = []
    own = fake._tool_agent_get_self

    def get_self() -> dict[str, Any]:
        reads.append(1)
        fake.state = "active" if len(reads) >= read else "review"
        return own()

    fake._tool_agent_get_self = get_self  # type: ignore[method-assign]
    return reads


def test_the_first_action_announces_the_agent_and_waits_to_be_confirmed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # An agent that only uses services: its first action announces agent.json, waits until the publisher confirms
    # the agent, then acts.
    monkeypatch.setattr("ekl_provider_adapter.agent.ACTIVE_POLL_SECONDS", 0.01)
    fake = FakeNetwork()
    fake.provider("@vat", "cap_vat_check", lambda _: {"valid": True}, summary="validate vat")
    reads = _confirmed_on_read(fake, 3)
    agent = _agent(fake, tmp_path)
    announcer = _Announcer()
    agent._announcement = (announcer, {"agent": {"handle": "@ap-inbox"}, "capabilities": []})  # type: ignore[assignment]
    agent._announced_on = fake
    [candidate] = agent.find("validate vat")
    assert use(agent, candidate).call({"vat_number": "DE1"}).value == {"valid": True}
    assert announcer.announced == [""] and len(reads) == 3  # announced once, with no address


def test_a_grant_that_lapsed_unused_is_reserved_again_once(tmp_path: Path) -> None:
    # The laptop slept between the reservation and the call: nothing ran under the first grant, so the run is
    # reserved again as a new execution of the same logical intent, under the same idempotency key.
    fake = FakeNetwork()
    vat = fake.provider("@vat", "cap_vat_check", lambda _: {"valid": True}, price="0.10", summary="validate vat")
    journal = AgentJournal(str(tmp_path / "journal.db"))
    agent = Agent(fake, journal)
    [candidate] = agent.find("validate vat")
    fake.lapse = True
    result = use(agent, candidate).call({"vat_number": "DE1"}, idempotency_key="supplier-7")
    first, second = fake.reserved
    assert second["logical_intent_id"] == first["execution_id"] != second["execution_id"]
    assert result.execution_id == second["execution_id"] and vat.calls == 1
    recorded = journal.call("supplier-7")
    assert recorded is not None and recorded.execution_id == second["execution_id"]


def test_a_grant_that_lapses_twice_is_not_retried_again(tmp_path: Path) -> None:
    fake = FakeNetwork()
    vat = fake.provider("@vat", "cap_vat_check", lambda _: {"valid": True}, price="0.10", summary="validate vat")
    agent = _agent(fake, tmp_path)
    [candidate] = agent.find("validate vat")
    own = fake._tool_execution_reserve

    def always_lapses(execution_id: str, **request: Any) -> dict[str, Any]:
        fake.lapse = True
        return own(execution_id, **request)

    fake._tool_execution_reserve = always_lapses  # type: ignore[method-assign]
    with pytest.raises(AgentError) as lapsed:
        use(agent, candidate).call({"vat_number": "DE1"})
    assert lapsed.value.code == "GRANT_EXPIRED" and len(fake.reserved) == 2 and vat.calls == 0


def test_a_network_that_no_longer_speaks_ekp_3_is_refused_before_anything_is_done(tmp_path: Path) -> None:
    fake = FakeNetwork()
    fake._tool_world_get_contract = lambda: {"protocol": "ekp/2.0", "accepted_protocols": []}  # type: ignore[method-assign]
    with pytest.raises(AgentError) as refused:
        _agent(fake, tmp_path).post_need("Validate EU VAT numbers")
    assert refused.value.code == "PROFILE_UNSUPPORTED" and fake.posts == {}


def test_on_a_network_that_speaks_only_ekp_3_0_everything_works_but_trials(tmp_path: Path) -> None:
    # The connector can be upgraded before the network: trials, which 3.1 changed, are refused here, never sent.
    fake = FakeNetwork()
    fake._tool_world_get_contract = lambda: {"protocol": "ekp/3.0", "accepted_protocols": []}  # type: ignore[method-assign]
    fake.provider("@vat-checker", "cap_vat", lambda _: {"valid": True}, summary="vat")
    agent = _agent(fake, tmp_path)
    agent.post_need("Validate EU VAT numbers")
    assert fake.posts
    candidate = agent.find("vat")[0]
    with pytest.raises(AgentError) as refused:
        candidate.trial({"vat_number": "DE1"}, {"checks": [{"id": "ok", "path": "$.valid", "equals": True}]})
    assert refused.value.code == "UPGRADE_REQUIRED" and fake.trials == []


def test_a_retry_of_a_paid_call_that_ran_is_never_run_or_paid_again(tmp_path: Path) -> None:
    # The same idempotency key after the call finished, from a new process with the same journal: the network returns
    # the captured reservation, and the run is not reserved again as a new execution.
    fake = FakeNetwork()
    fake.wallet = Decimal("5")
    vat = fake.provider(
        "@vat",
        "cap_vat_check",
        lambda _: {"valid": True},
        summary="validate vat",
        plan={"plan_id": "pack-2", "type": "pack", "price": "1.00", "calls": 2},
    )
    journal = str(tmp_path / "journal.db")
    [candidate] = Agent(fake, AgentJournal(journal)).find("validate vat")
    first = use(Agent(fake, AgentJournal(journal)), candidate).call({"vat_number": "DE1"}, idempotency_key="sup-7")
    assert first.charged == "1.00" and vat.calls == 1
    with pytest.raises(CallFinished) as finished:
        use(Agent(fake, AgentJournal(journal)), candidate).call({"vat_number": "DE1"}, idempotency_key="sup-7")
    assert finished.value.code == "CALL_FINISHED" and finished.value.outcome == "succeeded"
    assert "charged 1.00" in str(finished.value) and vat.calls == 1 and fake.wallet == Decimal("4")
    # The pack's second call is still there for the next call.
    second = use(Agent(fake, AgentJournal(journal)), candidate).call({"vat_number": "DE2"}, idempotency_key="sup-8")
    assert second.charged == "0.00" and vat.calls == 2 and fake.wallet == Decimal("4")


def test_a_trial_is_scored_against_the_expectation_and_never_returns_the_output(tmp_path: Path) -> None:
    # EKP 3.1: the connector sends an expectation with every trial, the default being the output schema alone; a JSON
    # Schema given as `expect` becomes one check; words go as they are, for the network to compile.
    fake = FakeNetwork()
    fake.provider(
        "@vat",
        "cap_vat_check",
        lambda a: {"valid": a["vat_number"].startswith("DE")},
        summary="vat check",
        output_schema={"type": "object", "required": ["valid"]},
    )
    agent = _agent(fake, tmp_path)
    service = agent.find("vat check")[0]
    plain = agent.aio.try_service(service, {"vat_number": "DE1"})
    schema = agent.aio.try_service(service, {"vat_number": "FR1"}, {"properties": {"valid": {"const": True}}})
    trials = [agent._sync(t) for t in (plain, schema)]
    assert [f["expect"] for f in fake.trials] == [
        {"checks": []},
        {"checks": [{"id": "expect", "schema": {"properties": {"valid": {"const": True}}}}], "threshold": 1.0},
    ]
    assert trials[0].passed and trials[0].score == 1.0 and not hasattr(trials[0], "output")
    assert not trials[1].passed and "short on expect" in trials[1].reason
    # Words go as they are, for the network's model to compile; without one, the network refuses them.
    with pytest.raises(AgentError) as words:
        agent._sync(agent.aio.try_service(service, {"vat_number": "DE1"}, "says the number is valid"))
    assert words.value.code == "MODEL_UNAVAILABLE"
    with pytest.raises(AgentError) as refused:
        fake._tool_capability_trial("@vat", "cap_vat_check", {"vat_number": "DE1"})
    assert refused.value.code == "UPGRADE_REQUIRED"


def test_a_misspelled_expectation_is_refused_never_read_as_a_schema_every_output_passes() -> None:
    with pytest.raises(ValueError, match="treshold"):
        expectation({"checks": [{"id": "total", "path": "$.total", "equals": 120}], "treshold": 0.9})
    with pytest.raises(ValueError, match="totl"):
        expectation({"totl": 120})
    with pytest.raises(ValueError, match="threshold"):
        expectation({"checks": [], "threshold": 2})
    assert expectation({"type": "object", "required": ["total"]})["checks"][0]["id"] == "expect"


def test_an_expectation_the_network_refuses_stops_choose_instead_of_failing_every_candidate(tmp_path: Path) -> None:
    fake = FakeNetwork()
    fake.provider("@a", "cap_count", lambda _: {"n": 1}, summary="count things")
    fake.provider("@b", "cap_count", lambda _: {"n": 1}, summary="count things")
    agent = _agent(fake, tmp_path)
    bad = {"checks": [{"id": "n", "path": "$..n", "equals": 1}]}
    with pytest.raises(AgentError) as refused:
        agent.choose("count things", sample={}, expect=bad)
    assert refused.value.code == "INVALID_REQUEST" and not isinstance(refused.value, NoProvider)


def test_a_service_that_takes_no_trials_never_passes_one_and_ranks_after_those_that_did(tmp_path: Path) -> None:
    fake = FakeNetwork()
    fake.provider("@untried", "cap_count", lambda _: {"n": 1}, summary="count things", accepts_trials=False)
    fake.provider("@tried", "cap_count", lambda _: {"n": 1}, summary="count things")
    agent = _agent(fake, tmp_path)
    untried = next(c for c in agent.find("count things") if c.handle == "@untried")
    alone = agent._sync(agent.aio.try_service(untried, {}))
    assert not alone.passed and alone.untried and alone.reason == "takes no trials"
    choice = agent.choose("count things", sample={})
    assert choice.main is not None and choice.backup is not None
    assert (choice.main.handle, choice.backup.handle) == ("@tried", "@untried")


def test_most_accurate_ranks_by_score_then_tested_quality(tmp_path: Path) -> None:
    fake = FakeNetwork()
    expect = {"checks": [{"id": "n", "path": "$.n", "approx": 10, "tolerance": 1}], "threshold": 0.5}
    fake.provider("@near", "cap_count", lambda _: {"n": 11.5}, summary="count things")
    fake.provider("@exact", "cap_count", lambda _: {"n": 10}, summary="count things")
    tested = fake.provider("@tested", "cap_count", lambda _: {"n": 10}, summary="count things")
    tested.tested_quality = {"significant": True, "t": 0.9}
    agent = _agent(fake, tmp_path)
    choice = agent.choose("count things", sample={}, expect=expect)
    assert choice.main is not None and choice.backup is not None
    assert (choice.main.handle, choice.backup.handle) == ("@tested", "@exact")
    assert (
        next(t for t in choice.trials if t.candidate.handle == "@near").score == 0.75
    )  # the schema 1, the near miss 0.5


def test_a_network_that_drops_once_never_stops_the_event_loop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A redeploy or a dropped TLS start is passing: each step the loop takes on the network is tried again after a
    pause, and the callbacks keep running, instead of the loop stopping until the agent restarts."""
    from ekl_provider_adapter import agent as agent_module  # noqa: PLC0415 - only this test shortens its pauses
    from ekl_provider_adapter.errors import ProviderUnavailable  # noqa: PLC0415

    monkeypatch.setattr(agent_module, "RETRY_SECONDS", 0.01)

    class Flaky(FakeNetwork):
        def __init__(self) -> None:
            super().__init__()
            self.dropped: set[str] = set()

        async def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
            if tool in {"agent_get_self", "feed_subscribe", "events_ack"} and tool not in self.dropped:
                self.dropped.add(tool)
                raise ProviderUnavailable("EuKollab could not be reached (ConnectError).", code="SERVICE_UNAVAILABLE")
            return await super().call(tool, arguments)

    fake = Flaky()
    agent = _agent(fake, tmp_path)
    seen: list[str] = []

    @agent.on_need(matching="subscribed", topics=["vat"])
    def need(_: Need) -> None:
        pass

    @agent.on_message
    def note(message: Any) -> None:
        seen.append(str(message.text))

    fake.message("@peer", "heard after the drop")

    async def go() -> None:
        await agent.start()
        for _ in range(500):
            if seen and "events_ack" in fake.dropped and agent._task is not None and not agent._task.done():
                break
            await asyncio.sleep(0.01)
        assert agent._task is not None and not agent._task.done()
        await agent.stop()

    asyncio.run(go())
    assert seen == ["heard after the drop"]
    assert fake.dropped == {"agent_get_self", "feed_subscribe", "events_ack"}


def test_a_connection_not_accepted_in_time_is_refused_as_pending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The connector stops waiting before act() does, so agent code is told CONNECTION_PENDING, not a timeout.
    from ekl_provider_adapter import agent as agent_module  # noqa: PLC0415 - only this test shortens the wait

    monkeypatch.setattr(agent_module, "CONNECTION_WAIT", agent_module.dt.timedelta(0))

    class Unanswered(FakeNetwork):
        def _tool_connection_request(
            self, target: str, capability_ids: list[str], note: str = "", outreach_thread_id: str | None = None
        ) -> dict[str, Any]:
            answer = super()._tool_connection_request(target, capability_ids, note, outreach_thread_id)
            self.connections[answer["connection_id"]]["state"] = "pending"
            return answer

    fake = Unanswered()
    vat = fake.provider("@vat", "cap_vat_check", lambda _: {"valid": True}, summary="validate vat")
    agent = _agent(fake, tmp_path)
    [candidate] = agent.find("validate vat")
    with pytest.raises(AgentError) as pending:
        use(agent, candidate).call({"vat_number": "DE1"})
    assert pending.value.code == "CONNECTION_PENDING" and pending.value.detail["connection_id"]
    assert vat.calls == 0


def test_an_unreachable_provider_is_refused_with_its_code(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Agent code reads ActionRefused.code: an unreachable provider is SERVICE_UNAVAILABLE, never an empty code.
    from ekl_provider_adapter import agent as agent_module  # noqa: PLC0415 - only this test shortens its pauses

    async def no_pause(_: float) -> None:
        return None

    monkeypatch.setattr(agent_module.asyncio, "sleep", no_pause)
    fake = FakeNetwork()
    vat = fake.provider("@vat", "cap_vat_check", lambda _: {"valid": True}, summary="validate vat")
    vat.fails = "unreachable"
    agent = _agent(fake, tmp_path)
    [candidate] = agent.find("validate vat")
    with pytest.raises(AgentError) as away:
        use(agent, candidate).call({"vat_number": "DE1"})
    assert away.value.code == "SERVICE_UNAVAILABLE" and away.value.detail["sent"] is False


def _provider_answering(monkeypatch: pytest.MonkeyPatch, handler: Any) -> None:
    from ekl_provider_adapter import network_client  # noqa: PLC0415 - only these tests replace its transport

    real = httpx2.AsyncClient

    def client(**kwargs: Any) -> httpx2.AsyncClient:
        return real(transport=httpx2.MockTransport(handler), **kwargs)

    monkeypatch.setattr(network_client.httpx2, "AsyncClient", client)


def _call_provider() -> None:
    from ekl_provider_adapter.network_client import McpNetwork  # noqa: PLC0415

    network = McpNetwork("https://network.example", "credential")
    asyncio.run(network.call_provider("https://relay.example/r/agt_1", "token", "check", {}, {"execution_id": "x"}))


def test_the_relay_s_provider_away_reaches_agent_code(monkeypatch: pytest.MonkeyPatch) -> None:
    # The relay answers 503 PROVIDER_AWAY when a relayed provider isn't running: nothing took the call.
    from ekl_provider_adapter.errors import ProviderUnavailable  # noqa: PLC0415

    def away(_: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            503, json={"code": "PROVIDER_AWAY", "retryable": True, "message": "the agent is not running"}
        )

    _provider_answering(monkeypatch, away)
    with pytest.raises(ProviderUnavailable) as refused:
        _call_provider()
    assert refused.value.code == "PROVIDER_AWAY" and refused.value.detail == {"sent": False}


def test_a_provider_that_cannot_be_reached_is_service_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    from ekl_provider_adapter.errors import ProviderUnavailable  # noqa: PLC0415

    def down(request: httpx2.Request) -> httpx2.Response:
        raise httpx2.ConnectError("refused", request=request)

    _provider_answering(monkeypatch, down)
    with pytest.raises(ProviderUnavailable) as refused:
        _call_provider()
    assert refused.value.code == "SERVICE_UNAVAILABLE"
