# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Approving paid runs. Every paid run waits for its provider's approval before anything reaches a tool: the network
asks, and the adapter answers through the publisher's approval handler.

    @app.runs.on_approval
    async def approve(run: RunRequest) -> RunDecision:
        if await my_billing.has_paid(run.consumer.handle, run.plan_id):
            return RunDecision.approve(runs=500)
        return RunDecision.payment_needed(f"https://pay.example.com/?ref={run.reservation_id}", "8.00", "500 runs")

Without a handler the adapter asks the consumer to pay on agent.json's `payment.link`, with the run's reservation ID
as `ref` (as `client_reference_id` on a Stripe Payment Link, the one name Stripe keeps), and the publisher's own
payment code approves the run once it is paid: `await app.runs.approve(ref)`. With a Stripe webhook secret the adapter
does that itself (`stripe_webhook`). The payment itself is between the consumer and the provider; the network passes
the request on and never takes part.
"""

import asyncio
import datetime as dt
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Literal
from urllib.parse import urlencode, urlsplit, urlunsplit

import httpx2
from pydantic import BaseModel

from ekl_provider_adapter.config import AgentFile

log = logging.getLogger("ekl_provider_adapter")


class Consumer(BaseModel):
    agent_id: str
    handle: str | None = None
    # A person running the service on the network's site, through their account's consumer agent.
    person: bool = False


class RunRequest(BaseModel):
    """A paid run waiting for the provider's approval. Inputs never travel with it."""

    reservation_id: str
    execution_id: str
    consumer: Consumer
    capability_id: str
    plan_id: str = ""
    unit_price: str = "0"
    currency: str = "USD"
    approval_deadline: str | None = None
    answer_by: str | None = None


@dataclass
class RunDecision:
    """The provider's answer: approve (once or for later runs too), ask for payment first, or decline."""

    decision: Literal["approve", "payment_needed", "decline"]
    fields: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def approve(
        cls, *, runs: int | None = None, until: dt.datetime | None = None, overage: bool = False
    ) -> "RunDecision":
        fields: dict[str, Any] = {"unit": "overage" if overage else "plan"}
        if runs is not None or until is not None:
            fields["standing"] = {
                k: v for k, v in {"runs": runs, "until": until.isoformat() if until else None}.items() if v
            }
        return cls("approve", fields)

    @classmethod
    def payment_needed(
        cls,
        url: str,
        amount: str,
        description: str = "",
        *,
        currency: str = "USD",
        challenge: dict[str, Any] | None = None,
    ) -> "RunDecision":
        payment: dict[str, Any] = {"url": url, "amount": amount, "currency": currency, "description": description}
        if challenge is not None:
            payment["challenge"] = challenge
        return cls("payment_needed", {"payment": payment})

    @classmethod
    def decline(cls, reason: str) -> "RunDecision":
        return cls("decline", {"reason": reason[:200]})


ApprovalHandler = Callable[[RunRequest], Awaitable[RunDecision | None]]


class RunsApiError(Exception):
    def __init__(self, code: str, message: str, *, retryable: bool) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.retryable = retryable


class HttpRunsApi:
    """The two run-approval endpoints of the Provider API, with the provider credential."""

    def __init__(self, network_url: str, credential: str, *, http: httpx2.AsyncClient | None = None) -> None:
        self._base = network_url.rstrip("/")
        self._http = http or httpx2.AsyncClient(timeout=40)
        self._headers = {"Authorization": f"Bearer {credential}"}

    async def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        try:
            response = await self._http.post(f"{self._base}{path}", json=body, headers=self._headers)
            data: dict[str, Any] = response.json()
        except (httpx2.HTTPError, ValueError) as exc:
            raise RunsApiError("SERVICE_UNAVAILABLE", f"the network is unreachable: {exc}", retryable=True) from exc
        if response.status_code >= 400:
            raise RunsApiError(
                str(data.get("code", "REFUSED")),
                str(data.get("message", "")),
                retryable=response.status_code >= 500 or bool(data.get("retryable")),
            )
        return data

    async def next(self, wait: int) -> list[RunRequest]:
        found = await self._post("/v1/provider/runs/next", {"wait": wait})
        return [RunRequest.model_validate(r) for r in found.get("runs") or []]

    async def get(self, reservation_id: str) -> dict[str, Any]:
        """One of the provider's runs, with its state and the payment the provider asked for."""
        found: dict[str, Any] = (await self._post("/v1/provider/runs/get", {"reservation_id": reservation_id}))["run"]
        return found

    async def decide(self, reservation_id: str, decision: RunDecision) -> dict[str, Any]:
        return await self._post(
            "/v1/provider/runs/decide",
            {"reservation_id": reservation_id, "decision": decision.decision} | decision.fields,
        )


def _with_ref(link: str, reservation_id: str) -> str:
    """The payment link with the run's reservation ID, so the provider's payment matches the run. A Stripe Payment
    Link drops every query parameter but `client_reference_id`, which it passes to the publisher's webhook."""
    parts = urlsplit(link)
    host = (parts.hostname or "").lower()
    name = "client_reference_id" if host == "stripe.com" or host.endswith(".stripe.com") else "ref"
    query = "&".join(q for q in (parts.query, urlencode({name: reservation_id})) if q)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))


def plan_of(agent: AgentFile | None, plan_id: str) -> dict[str, Any]:
    """The agent.json plan a run is under, or {} when the file doesn't name it."""
    plans = [
        p
        for cap in (agent.capabilities if agent else [])
        for p in cap.plans or []
        if str(p.get("plan_id") or p.get("type")) == plan_id
    ]
    return plans[0] if plans else {}


def _decimal(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except ArithmeticError:
        return None


def paid_approval(plan: dict[str, Any], now: dt.datetime | None = None) -> RunDecision:
    """What a payment for the plan buys: a pack's runs, or a subscription's period, as a standing approval."""
    if plan.get("type") == "pack" and int(plan.get("calls") or 0) > 1:
        return RunDecision.approve(runs=int(plan["calls"]))
    if plan.get("type") == "subscription":
        days = 365 if plan.get("period") == "year" else 30
        return RunDecision.approve(until=(now or dt.datetime.now(dt.UTC)) + dt.timedelta(days=days))
    return RunDecision.approve()


def default_decision(agent: AgentFile | None, run: RunRequest) -> RunDecision:
    """Without a handler: send the consumer to agent.json's payment page for the plan the run is under. The
    publisher's payment code then approves with app.runs.approve(ref)."""
    if agent is None or agent.payment is None:
        return RunDecision.decline("this provider takes no payments yet")
    plan = plan_of(agent, run.plan_id)
    amount = str(plan.get("price") or run.unit_price)
    what = str(
        plan.get("description")
        or {"pack": f"{plan.get('calls')} runs", "subscription": f"a {plan.get('period', 'month')}ly subscription"}.get(
            str(plan.get("type")), "one run"
        )
    )
    try:
        Decimal(amount)
    except ArithmeticError:
        amount = run.unit_price
    return RunDecision.payment_needed(_with_ref(agent.payment.link, run.reservation_id), amount, what[:200])


class Runs:
    """The adapter's side of run approval: the publisher's handler, its later decisions, and the loop that reads
    waiting runs from the network."""

    def __init__(self, api: HttpRunsApi | Any, agent: Callable[[], AgentFile | None], *, wait: int = 20) -> None:
        self.api = api
        self._agent = agent
        self._wait = wait
        self._handler: ApprovalHandler | None = None
        # The payment each waiting run was asked for, so a payment confirmed later approves what it bought. Kept in
        # memory: a run waits at most fifteen minutes, and entries an hour old are dropped.
        self._asked: dict[str, tuple[RunRequest, dict[str, Any], float]] = {}

    def on_approval(self, handler: ApprovalHandler) -> ApprovalHandler:
        """Register the approval handler. Returning None asks for payment on agent.json's payment page when it has one;
        without one, the run waits to be decided later."""
        self._handler = handler
        return handler

    def awaiting_payment(self) -> bool:
        """Whether a run this copy asked payment for may still be paid: it waits at most fifteen minutes."""
        now = time.monotonic()
        return any(now - at < 1200 for _, _, at in self._asked.values())

    async def approve(
        self, reservation_id: str, *, runs: int | None = None, until: dt.datetime | None = None, overage: bool = False
    ) -> dict[str, Any]:
        """Approve a run, typically once your payment system confirmed the consumer paid: `runs` and `until` let
        the same consumer's later runs of the plan through without asking."""
        return await self.api.decide(reservation_id, RunDecision.approve(runs=runs, until=until, overage=overage))

    async def ask_payment(self, reservation_id: str, url: str, amount: str, description: str = "") -> dict[str, Any]:
        return await self.api.decide(reservation_id, RunDecision.payment_needed(url, amount, description))

    async def decline(self, reservation_id: str, reason: str) -> dict[str, Any]:
        return await self.api.decide(reservation_id, RunDecision.decline(reason))

    async def paid(self, reservation_id: str, amount: Decimal, currency: str) -> bool:
        """A payment the provider's payment system confirmed for this run: approve what it bought, the plan's runs
        or period, once it matches the payment request: the amount at least and the currency asked. A run this copy
        holds no request for (it restarted, or another copy asked) has its request read back from the network; one
        that can't be read back, or never asked for payment, approves nothing, whatever was paid. RunsApiError
        (retryable) when the network could not be asked: the payment is still to be applied."""
        held = self._asked.get(reservation_id)
        run, asked = (held[0], held[1]) if held else await self._read_back(reservation_id)
        if run is None or asked is None:
            log.warning("run %s was paid but has no payment request to match; not approved", reservation_id)
            return False
        if currency.upper() != str(asked.get("currency") or "USD").upper() or amount < Decimal(str(asked["amount"])):
            log.warning("run %s was paid %s %s, less than asked; not approved", reservation_id, amount, currency)
            return False
        plan = plan_of(self._agent(), run.plan_id)
        decision = paid_approval(plan) if plan else RunDecision.approve()
        try:
            await self.api.decide(reservation_id, decision)
        except RunsApiError as exc:
            if exc.retryable:
                raise  # the network didn't decide: the payment is tried again, not dropped
            # Stripe delivers an event more than once, and a run may have ended meanwhile: both are refused. The log
            # names the run, so the provider can find the payment and refund it.
            log.warning("run %s was paid but not approved: %s", reservation_id, exc)
            return False
        self._asked.pop(reservation_id, None)
        return True

    async def _read_back(self, reservation_id: str) -> tuple[RunRequest | None, dict[str, Any] | None]:
        """The run and the payment it was asked, as the network recorded them, while it still waits."""
        getter = getattr(self.api, "get", None)
        if getter is None:
            return None, None
        try:
            found = await getter(reservation_id)
        except RunsApiError as exc:
            if exc.retryable:
                raise
            return None, None
        if found.get("state") != "awaiting_approval" or not found.get("payment"):
            return None, None
        return RunRequest.model_validate(found), dict(found["payment"])

    async def answer(self, run: RunRequest) -> None:
        """Answer one waiting run within its first-answer deadline."""
        try:
            decision = await self._handler(run) if self._handler else None
            agent = self._agent()
            if decision is None and (self._handler is None or (agent is not None and agent.payment is not None)):
                # No answer from the handler: agent.json's payment page, as without a handler. Without a page the
                # run waits for a later decision.
                decision = default_decision(agent, run)
        except Exception:  # a failing handler must not stop the loop; the run then lapses unanswered
            log.exception("the approval handler failed for %s", run.reservation_id)
            return
        if decision is None:
            return
        if decision.decision == "payment_needed":
            now = time.monotonic()
            for stale in [k for k, (_, _, at) in self._asked.items() if now - at > 3600]:
                del self._asked[stale]
            self._asked[run.reservation_id] = (run, decision.fields["payment"], now)
        try:
            await self.api.decide(run.reservation_id, decision)
        except RunsApiError as exc:
            log.warning("could not answer run %s: %s", run.reservation_id, exc)

    async def watch(self) -> None:
        """Read waiting runs from the network and answer each, until cancelled."""
        backoff = 1.0
        while True:
            try:
                waiting = await self.api.next(self._wait)
                backoff = 1.0
            except RunsApiError as exc:
                if not exc.retryable:
                    log.info("run approvals are not available here (%s); checking again later", exc.code)
                await asyncio.sleep(min(backoff, 60.0))
                backoff *= 2
                continue
            await asyncio.gather(*(self.answer(run) for run in waiting))
