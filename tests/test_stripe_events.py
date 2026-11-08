# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Approving paid runs from the publisher's Stripe account with no webhook: an adapter with no public address reads
paid Checkouts from Stripe itself, only while a run waits for payment, and each approves its run once."""

import asyncio
import contextlib
from typing import Any

import httpx2
import pytest

from ekl_provider_adapter.runs import Runs, RunsApiError
from ekl_provider_adapter.stripe_events import StripeError, StripeEvents
from tests.test_stripe_webhook import AGENT, Api, _run


def _event(event_id: str, reference: str = "rsv_1", total: int = 800, status: str = "paid") -> dict[str, Any]:
    session = {"client_reference_id": reference, "amount_total": total, "currency": "usd", "payment_status": status}
    return {"id": event_id, "type": "checkout.session.completed", "data": {"object": session}}


class Stripe:
    """Stripe's Events API, as much of it as the adapter uses: newest first, in pages."""

    def __init__(self, events: list[dict[str, Any]], *, status: int = 200, page: int = 100) -> None:
        self.events, self.status, self.page = events, status, page
        self.requests: list[httpx2.Request] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        if self.status != 200:
            return httpx2.Response(self.status, json={"error": {"message": "Invalid API Key provided"}})
        newest_first = list(reversed(self.events))
        after = request.url.params.get("starting_after")
        start = next((i + 1 for i, e in enumerate(newest_first) if e["id"] == after), 0) if after else 0
        chunk = newest_first[start : start + self.page]
        return httpx2.Response(200, json={"data": chunk, "has_more": start + self.page < len(newest_first)})


def _events(stripe: Stripe, runs: Runs, **options: Any) -> StripeEvents:
    http = httpx2.AsyncClient(transport=httpx2.MockTransport(stripe))
    return StripeEvents(runs, "rk_test_publisher", http=http, api_url="https://stripe.test", **options)


def test_a_paid_checkout_read_from_stripe_approves_what_the_plan_buys() -> None:
    api = Api()
    runs = Runs(api, lambda: AGENT)
    asyncio.run(runs.answer(_run("rsv_1")))
    stripe = Stripe([_event("evt_1", "rsv_1", 800)])
    assert asyncio.run(_events(stripe, runs).check(since=0)) == 1
    assert [(r, d.fields) for r, d in api.decided if d.decision == "approve"] == [
        ("rsv_1", {"unit": "plan", "standing": {"runs": 500}})
    ]
    request = stripe.requests[0]
    assert request.headers["authorization"] == "Bearer rk_test_publisher"
    assert sorted(request.url.params.get_list("types[]")) == [
        "checkout.session.async_payment_succeeded",
        "checkout.session.completed",
    ]


def test_each_event_approves_once_across_pages_and_other_checkouts_are_left_alone() -> None:
    api = Api()
    runs = Runs(api, lambda: AGENT)
    for reservation in ("rsv_1", "rsv_2", "rsv_3"):
        asyncio.run(runs.answer(_run(reservation)))
    events = [
        _event("evt_1", "rsv_1"),
        _event("evt_2", "order-1234"),  # the publisher's other products
        _event("evt_3", "rsv_2", status="unpaid"),
        _event("evt_4", "rsv_3", total=100),  # below what was asked
    ]
    watcher = _events(Stripe(events, page=2), runs)
    asyncio.run(watcher.check(since=0))
    asyncio.run(watcher.check(since=0))  # the same events listed again
    assert [r for r, d in api.decided if d.decision == "approve"] == ["rsv_1"]


def test_a_short_payment_or_an_ended_run_approves_nothing() -> None:
    api = Api()
    runs = Runs(api, lambda: AGENT)
    asyncio.run(runs.answer(_run("rsv_1")))
    assert asyncio.run(_events(Stripe([_event("evt_1", total=100)]), runs).check(since=0)) == 0
    ended = Runs(Api(refuse=True), lambda: AGENT)
    assert asyncio.run(_events(Stripe([_event("evt_2")]), ended).check(since=0)) == 0


def test_a_wrong_key_is_reported_not_retried_quickly() -> None:
    watcher = _events(Stripe([], status=401), Runs(Api(), lambda: AGENT))
    with pytest.raises(StripeError) as refused:
        asyncio.run(watcher.check(since=0))
    assert not refused.value.retryable and "Invalid API Key" in str(refused.value)
    with pytest.raises(StripeError) as busy:
        asyncio.run(_events(Stripe([], status=429), Runs(Api(), lambda: AGENT)).check(since=0))
    assert busy.value.retryable


def test_stripe_is_asked_at_start_and_then_only_while_a_run_waits_for_payment() -> None:
    api = Api()
    runs = Runs(api, lambda: AGENT)
    stripe = Stripe([])

    async def scenario() -> tuple[int, int]:
        task = asyncio.ensure_future(_events(stripe, runs, every=0.01).watch())
        await asyncio.sleep(0.1)
        at_start = len(stripe.requests)  # the look back at start, then nothing: no run waits
        await runs.answer(_run("rsv_1"))
        stripe.events.append(_event("evt_1", "rsv_1"))
        await asyncio.sleep(0.1)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        return at_start, len(stripe.requests)

    at_start, total = asyncio.run(scenario())
    assert at_start == 1 and total > 1
    assert ("rsv_1", "approve") in [(r, d.decision) for r, d in api.decided]
    assert not runs.awaiting_payment()


class Unreachable(Api):
    """The network, unreachable for the first `failures` decisions."""

    def __init__(self, failures: int) -> None:
        super().__init__()
        self.failures = failures

    async def decide(self, reservation_id: str, decision: Any) -> dict[str, Any]:
        if self.failures:
            self.failures -= 1
            raise RunsApiError("SERVICE_UNAVAILABLE", "the network is unreachable", retryable=True)
        return await super().decide(reservation_id, decision)


def test_a_payment_the_network_could_not_take_yet_is_applied_on_a_later_pass() -> None:
    api = Unreachable(failures=1)
    runs = Runs(api, lambda: AGENT)
    asyncio.run(runs.answer(_run("rsv_1")))
    api.failures = 1  # the answer above went through; the approval's first try does not
    watcher = _events(Stripe([_event("evt_1", "rsv_1", 800)]), runs)
    assert asyncio.run(watcher.check(since=0)) == 0
    # The next listing no longer covers the event: it is applied from what the adapter kept.
    watcher._list = _nothing  # type: ignore[method-assign]
    assert asyncio.run(watcher.check(since=0)) == 1
    assert [r for r, _ in api.decided] == ["rsv_1"]
    assert asyncio.run(watcher.check(since=0)) == 0  # handled once


async def _nothing(since: int) -> list[dict[str, Any]]:
    del since
    return []
