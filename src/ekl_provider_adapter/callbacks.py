# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The connector's agent side: the agent's events to its callbacks file, and the actions the file starts.

Events go to the agent's eukollab_callbacks.py, which the connector runs beside itself on 127.0.0.1, as JSON signed
with callback protocol v2 (callback_signing): the file holds EKL_CALLBACK_KEY, derived from the agent credential, never
the credential itself. The file answers with what to do: `{"reply": "…"}`, `{"accept": true}`, `{"decline": "…"}`,
`{"review": {…}}`, `{"skip": true}`, or nothing; the file signs its answer, bound to the request it answers.

A decision the file owes (a connection request, a reply to a need or a message) is journaled
before it is first delivered, and applied at most once: a failed delivery (no connection, a timeout, a 5xx,
IN_PROGRESS, or an answer that isn't signed once the file has answered with v2) leaves it pending and it is sent again,
with the same event ID and the same body, after 2 s, 10 s, 1 min, 5 min and then every 15 min until its deadline. At the
deadline its default applies: a connection request is declined, nothing is replied. An answer
that arrives later is logged and ignored.

For actions the callbacks start themselves (post a need, choose, call a service, or call a service the agent plans to
use with `POST /agent/service`), the connector serves a small HTTP API on 127.0.0.1 only (local_guard), and
`GET /agent/status` says whether it holds the agent-operation credential.
"""

import asyncio
import contextlib
import datetime as dt
import json
import logging
import os
import time
from collections.abc import Awaitable, Callable
from typing import Any

import httpx2
import jsonschema
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from ekl_provider_adapter import callback_signing as cb
from ekl_provider_adapter.agent import (
    CURRENT_EVENT,
    Agent,
    Answer,
    Candidate,
    Choice,
    ConnectionRequest,
    Decision,
    Message,
    Need,
    PlannedService,
    ReviewDue,
)
from ekl_provider_adapter.agent_journal import AgentJournal, PendingDecision
from ekl_provider_adapter.config import is_loopback
from ekl_provider_adapter.errors import AgentError, BalanceLow
from ekl_provider_adapter.local_guard import LocalGuard

log = logging.getLogger("ekl_provider_adapter.callbacks")

# The pauses between deliveries of an undecided event; the last repeats until the deadline.
BACKOFF_SECONDS = (2.0, 10.0, 60.0, 300.0, 900.0)
# How often the deadline worker looks for decisions whose time is up.
DEADLINE_CHECK_SECONDS = 30.0
DELIVERY_TIMEOUT_SECONDS = 30.0
V2_FLAG = "callback_v2"
# Each decision kind's final deadline, configurable: connection requests a day, replies an hour. A console
# decision's comes with it.
CONNECTION_HOURS = "EKL_CONNECTION_DECISION_HOURS"
REPLY_MINUTES = "EKL_REPLY_DECISION_MINUTES"
REPLY_EVENTS = ("need", "need_answered", "message")

Perform = Callable[[dict[str, Any], dict[str, Any]], Awaitable[None]]


def _deadline(event: str, request: dict[str, Any], now: float) -> float:
    if event == "connection_request":
        return now + float(os.environ.get(CONNECTION_HOURS) or 24) * 3600
    if event in REPLY_EVENTS:
        return now + float(os.environ.get(REPLY_MINUTES) or 60) * 60
    given = request.get("deadline")
    if given:
        with contextlib.suppress(ValueError):
            when = dt.datetime.fromisoformat(str(given).replace("Z", "+00:00"))
            return (when if when.tzinfo else when.replace(tzinfo=dt.UTC)).timestamp()
    return now + 900  # an event without its own deadline waits fifteen minutes


class CallbackChannel:
    """Signed delivery to the callbacks file, and the decisions it owes, journaled.

    `performers` carry an applied answer to the network, by event name; `defaults` what the deadline applies. Both
    take the request as sent and the answer, so a decision picked up after a restart is carried the same way."""

    def __init__(
        self,
        url: str,
        credential: str,
        journal: AgentJournal,
        *,
        agent_id: Callable[[], str] = lambda: os.environ.get("EKL_AGENT_ID", ""),
        http: httpx2.AsyncClient | None = None,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        legacy_until: dt.datetime = cb.LEGACY_UNTIL,
    ) -> None:
        if not is_loopback(url):
            # Events carry other agents' text and decisions: they go only to the callbacks file run on this machine.
            raise ValueError(f"callbacks run on this machine only, not at {url}")
        self.url = url
        self._credential = credential
        self._key = cb.callback_key(credential)
        self.journal = journal
        self._agent_id = agent_id
        self._http = http
        self._clock = clock
        self._sleep = sleep
        self._legacy_until = legacy_until
        self.performers: dict[str, Perform] = {}
        self.defaults: dict[str, Perform] = {}
        self._retrying: dict[str, asyncio.Task[None]] = {}
        self._worker: asyncio.Task[None] | None = None

    # --- one delivery -------------------------------------------------------------------------------------------

    @property
    def negotiated(self) -> bool:
        """The file has answered with a valid v2 signature once: from then on only signed answers count."""
        return self.journal.flag(V2_FLAG) == "true"

    def _legacy(self) -> bool:
        return not self.negotiated and dt.datetime.fromtimestamp(self._clock(), dt.UTC) < self._legacy_until

    def body_for(self, event: str, event_id: str, data: dict[str, Any]) -> bytes:
        return json.dumps(
            {"event": event, "event_id": event_id, "agent_id": self._agent_id(), **data}, sort_keys=True, default=str
        ).encode()

    async def post(self, event_id: str, body: bytes) -> dict[str, Any] | None:  # noqa: PLR0911 - one exit per way a delivery fails
        """One delivery of exactly these bytes. The file's answer, or None for a failed delivery: unreachable, a
        timeout, a 5xx, IN_PROGRESS, a refusal, or an answer whose signature doesn't hold for this request."""
        agent_id = str(json.loads(body).get("agent_id") or "")
        t = int(self._clock())
        headers = {
            "Content-Type": "application/json",
            cb.REQUEST_HEADER: cb.request_header(self._key, t, event_id, agent_id, body),
        }
        legacy = self._legacy()
        if legacy:
            headers[cb.LEGACY_HEADER] = cb.legacy_signature(self._credential, body)
        client = self._http or httpx2.AsyncClient(timeout=DELIVERY_TIMEOUT_SECONDS, follow_redirects=False)
        try:
            answer = await client.post(self.url, content=body, headers=headers)
        except httpx2.HTTPError as exc:
            log.warning("event %s not delivered to the callbacks (%s); it is sent again", event_id, type(exc).__name__)
            return None
        finally:
            if self._http is None:
                await client.aclose()
        if answer.status_code >= 300:
            code = "IN_PROGRESS" if answer.status_code == 409 and b"IN_PROGRESS" in answer.content else ""
            log.warning("event %s: the callbacks answered %s %s; it is sent again", event_id, answer.status_code, code)
            return None
        signature = answer.headers.get(cb.RESPONSE_HEADER)
        if signature is not None:
            if not cb.response_valid(
                self._key,
                signature,
                event_id=event_id,
                agent_id=agent_id,
                request_digest=cb.digest(body),
                status=answer.status_code,
                body=answer.content,
                now=self._clock(),
            ):
                log.warning("event %s: the answer is signed for another request or key; not applied", event_id)
                return None
            if not self.negotiated:
                self.journal.set_flag(V2_FLAG, "true")
        elif not legacy:
            log.warning("event %s: the callbacks' answer isn't signed (callback v2); not applied", event_id)
            return None
        else:
            log.warning(
                "event %s: the callbacks answered without a callback v2 signature; accepted until %s, update its "
                "callbacks (ekl add writes a v2 eukollab_callbacks.py)",
                event_id,
                self._legacy_until.date(),
            )
        if not answer.content.strip():
            return {}
        try:
            found = answer.json()
        except ValueError:
            return None
        return found if isinstance(found, dict) else {}

    # --- decisions ----------------------------------------------------------------------------------------------

    async def decide(self, event: str, data: dict[str, Any], event_id: str | None = None) -> dict[str, Any] | None:
        """Journal the decision, deliver it once and, on a verified answer, apply it. Returns the applied answer, or
        None while it is pending (retried in the background) or after its deadline."""
        event_id = event_id or CURRENT_EVENT.get() or f"{event}:{data}"
        body = self.body_for(event, event_id, data)
        row = self.journal.open_decision(
            event_id, event, body.decode(), cb.digest(body), _deadline(event, data, self._clock())
        )
        retrying = self._retrying.get(event_id)
        if row.state == "pending" and retrying is not None and not retrying.done():
            return None  # asked again while its deliveries go on
        return await self._attempt(row)

    async def _attempt(self, row: PendingDecision) -> dict[str, Any] | None:
        if row.state != "pending":
            if not row.done:
                await self._carry(row)
            return row.answer if row.state == "applied" else None
        answer = await self.post(row.event_id, row.body.encode())
        if answer is None:
            self._retry_later(row.event_id)
            return None
        if not self.journal.apply_decision(row.event_id, row.request_digest, answer):
            now = self.journal.decision(row.event_id)
            log.info(
                "event %s: an answer arrived after the decision was %s; ignored",
                row.event_id,
                "made" if now and now.state == "applied" else "defaulted at its deadline",
            )
            return now.answer if now and now.state == "applied" else None
        applied = self.journal.decision(row.event_id)
        assert applied is not None
        await self._carry(applied)
        return answer

    async def _carry(self, row: PendingDecision) -> None:
        """What was decided, to the network; marked done only once that succeeded, so a restart carries it again."""
        request = json.loads(row.body)
        perform = (self.performers if row.state == "applied" else self.defaults).get(row.event)
        if perform is not None:
            await perform(request, row.answer)
        self.journal.mark_done(row.event_id)

    def _retry_later(self, event_id: str) -> None:
        running = self._retrying.get(event_id)
        if running is None or running.done():
            self._retrying[event_id] = asyncio.ensure_future(self._retry(event_id))

    async def _retry(self, event_id: str) -> None:
        for attempt in range(10_000):
            await self._sleep(BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)])
            row = self.journal.decision(event_id)
            if row is None or row.state != "pending" or self._clock() >= row.deadline:
                return
            try:
                answer = await self.post(event_id, row.body.encode())
                if answer is not None:
                    if self.journal.apply_decision(event_id, row.request_digest, answer):
                        applied = self.journal.decision(event_id)
                        assert applied is not None
                        await self._carry(applied)
                    return
            except Exception:  # an unexpected failure must not end the retries: the deadline still decides
                log.exception("event %s: delivery failed", event_id)

    async def apply_defaults(self) -> int:
        """Default every decision whose deadline passed; returns how many."""
        due = self.journal.default_due()
        for row in due:
            log.info("event %s (%s): no answer before its deadline; its default applies", row.event_id, row.event)
            try:
                await self._carry(row)
            except Exception:
                log.exception("event %s: the default could not be applied yet", row.event_id)
        return len(due)

    async def _deadlines(self) -> None:
        while True:
            with contextlib.suppress(Exception):
                await self.apply_defaults()
            await self._sleep(DEADLINE_CHECK_SECONDS)

    async def start(self) -> None:
        """Pick up what a previous run left: pending decisions are delivered again, decided ones carried."""
        for row in self.journal.unfinished():
            if row.state == "pending":
                self._retry_later(row.event_id)
            else:
                with contextlib.suppress(Exception):
                    await self._carry(row)
        if self._worker is None:
            self._worker = asyncio.ensure_future(self._deadlines())

    async def stop(self) -> None:
        for task in [*self._retrying.values(), *([self._worker] if self._worker else [])]:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._retrying.clear()
        self._worker = None

    # --- events that aren't decisions ---------------------------------------------------------------------------

    async def ask(self, event: str, data: dict[str, Any]) -> dict[str, Any]:
        """An event the callbacks are told of without a decision journal (a review period, an approved choice): a failed
        delivery raises, so the network delivers the event again."""
        event_id = CURRENT_EVENT.get() or event
        answer = await self.post(event_id, self.body_for(event, event_id, data))
        if answer is None:
            raise AgentError(f"the callbacks didn't take the {event} event; it comes back", code="CALLBACK_FAILED")
        return answer


def _candidate(c: Candidate | None) -> dict[str, Any] | None:
    if c is None:
        return None
    return {"handle": c.handle, "capability_id": c.capability_id, "summary": str(c.summary), "price": c.price}


def _perform_for(agent: Agent, channel: CallbackChannel) -> None:
    """How each applied answer, and each default at a deadline, reaches the network."""

    async def reply_to(field: str, request: dict[str, Any], answer: dict[str, Any]) -> None:
        if answer.get("reply"):
            await agent.aio.reply(str(request[field]), str(answer["reply"]))

    async def respond(request: dict[str, Any], answer: dict[str, Any]) -> None:
        if answer.get("accept"):
            await agent.aio.respond(str(request["connection_id"]), True, "")
        elif "decline" in answer:
            await agent.aio.respond(str(request["connection_id"]), False, str(answer["decline"] or ""))

    async def decline_late(request: dict[str, Any], _: dict[str, Any]) -> None:
        try:
            await agent.aio.respond(str(request["connection_id"]), False, "no answer before the deadline")
        except AgentError as exc:  # decided meanwhile by a console rule, or withdrawn
            log.info("connection %s not declined at its deadline: %s", request["connection_id"], exc.code)

    async def send_message(request: dict[str, Any], answer: dict[str, Any]) -> None:
        if answer.get("reply"):
            await agent.aio.message(str(request["connection_id"]), str(answer["reply"]))

    async def decide(request: dict[str, Any], answer: dict[str, Any]) -> None:
        d = Decision(
            agent, str(request["kind"]), str(request["subject_id"]), str(request["deadline"]), str(request["fallback"])
        )
        if ("accept" in answer or "decline" in answer) and d.kind not in ("connection_request", "paid_offer"):
            # Answered in the console or with its own tool: an answer here can't be applied, and retrying the event
            # wouldn't change that, so say so once and let the decision's fallback apply.
            log.warning("a %s decision can't be answered from the callbacks; its fallback applies", d.kind)
            return
        if answer.get("accept"):
            await agent.aio.decide(d, True)
        elif "decline" in answer:
            await agent.aio.decide(d, False, str(answer["decline"] or ""))

    async def need_reply(request: dict[str, Any], answer: dict[str, Any]) -> None:
        await reply_to("need_id", request, answer)

    async def answered_reply(request: dict[str, Any], answer: dict[str, Any]) -> None:
        await reply_to("reply_id", request, answer)

    channel.performers.update(
        {
            "need": need_reply,
            "need_answered": answered_reply,
            "connection_request": respond,
            "message": send_message,
            "decision": decide,
        }
    )
    channel.defaults["connection_request"] = decline_late


def bridge(
    agent: Agent,
    url: str,
    secret: str,
    http: httpx2.AsyncClient | None = None,
    channel: CallbackChannel | None = None,
) -> CallbackChannel:
    """Register every callback on `agent`, each forwarding its event to the callbacks file over `channel` (made from
    the URL and the credential when not given) and applying its answer at most once."""
    channel = channel or CallbackChannel(
        url, secret, agent.journal, agent_id=lambda: os.environ.get("EKL_AGENT_ID") or agent.agent_id, http=http
    )

    _perform_for(agent, channel)

    @agent.on_need(matching="my_services")
    async def need(n: Need) -> None:
        service = n.matched_service
        await channel.decide(
            "need",
            {
                "need_id": n.post_id,
                "text": str(n.text),
                "author": n.author,
                "topics": n.topics,
                "matched_service": service.__dict__ if service else None,
            },
        )

    @agent.on_need_answered
    async def answered(a: Answer) -> None:
        await channel.decide(
            "need_answered", {"need_id": a.need.post_id, "reply_id": a.post_id, "text": str(a.text), "author": a.author}
        )

    @agent.on_connection_request
    async def connection(r: ConnectionRequest) -> None:
        await channel.decide(
            "connection_request",
            {
                "connection_id": r.connection_id,
                "requester": r.requester,
                "capability_ids": r.capability_ids,
                "organisation": r.organisation.__dict__,
            },
        )

    @agent.on_message
    async def message(m: Message) -> None:
        await channel.decide(
            "message",
            {
                "connection_id": m.connection_id,
                "message_id": m.message_id,
                "from": m.sender,
                "text": str(m.text),
                "payload": m.payload,
            },
        )

    @agent.on_choice_approved
    async def approved(c: Choice) -> None:
        await channel.ask(
            "choice_approved",
            {"decision_id": c.decision_id, "main": _candidate(c.main), "backup": _candidate(c.backup)},
        )

    @agent.on_decision()
    async def decision(d: Decision) -> None:
        await channel.decide(
            "decision", {"kind": d.kind, "subject_id": d.subject_id, "deadline": d.deadline, "fallback": d.fallback}
        )

    @agent.on_review_due
    async def review_due(r: ReviewDue) -> None:
        what = await channel.ask(
            "review_due",
            {
                "collaboration_id": r.collaboration_id,
                "review_period_id": r.review_period_id,
                "period": r.period,
                "role": r.role,
                "counterpart": r.counterpart,
                "service": r.service.__dict__,
                "calls": r.calls,
                "useful": r.useful,
                "not_useful": r.not_useful,
                "not_assessed": r.not_assessed,
                "charged": r.charged,
                "currency": r.currency,
                "closes_at": r.closes_at,
            },
        )
        # Nothing in the answer leaves the default: the derived review as the consumer, none as the provider.
        if what.get("skip"):
            r.skip()
        elif isinstance(what.get("review"), dict):
            given = what["review"]
            r.review(
                int(given["rating"]),
                str(given["criteria_met"]),
                bool(given["would_use_again"]),
                str(given.get("comment") or ""),
            )

    return channel


async def _service(agent: Agent, handle: str, capability_id: str) -> Candidate:
    for found in await agent.aio.find(capability_id.removeprefix("cap_").replace("_", " "), limit=25):
        if found.handle == handle and found.capability_id == capability_id:
            return found
    raise AgentError(f"{handle} offers no {capability_id} you can use.", code="NOT_FOUND")


def local_api(agent: Agent) -> Starlette:
    """The actions the agent's callbacks start themselves. Bind it to 127.0.0.1 only: it acts with the agent
    credential, and LocalGuard refuses anything but the publisher's own programs."""

    async def post_need(body: dict[str, Any]) -> dict[str, Any]:
        need = await agent.aio.post_need(body["text"], body.get("topics", []), body.get("budget"))
        return {"need_id": need.post_id}

    async def find(body: dict[str, Any]) -> dict[str, Any]:
        found = await agent.aio.find(body["query"], int(body.get("limit", 5)))
        return {"candidates": [_candidate(c) for c in found]}

    async def choose(body: dict[str, Any]) -> dict[str, Any]:
        choice = await agent.aio.choose(body["query"], body.get("sample"))
        return {
            "main": _candidate(choice.main),
            "backup": _candidate(choice.backup),
            "reason": choice.reason,
            "waiting": choice.waiting,
        }

    async def call(body: dict[str, Any]) -> dict[str, Any]:
        service = await _service(agent, body["handle"], body["capability_id"])
        result = await agent.aio.call(service, body["arguments"], None, body.get("idempotency_key"))
        return {
            "value": result.value,
            "execution_id": result.execution_id,
            "charged": result.charged,
            "receipt_checked": result.receipt_checked,
            "safety_flags": result.safety_flags,
        }

    async def message(body: dict[str, Any]) -> dict[str, Any]:
        return await agent.aio.message(body["connection_id"], body["text"])

    async def close_agreement(body: dict[str, Any]) -> dict[str, Any]:
        """The agreement for the service ends and its review period opens on both sides."""
        service = await _service(agent, body["handle"], body["capability_id"])
        return await agent.aio.close_agreement(service, str(body.get("outcome") or "completed"))

    # One planned service per need and preference, so the choice made on the first call is reused.
    planned: dict[tuple[str, str | None], PlannedService] = {}

    async def service(body: dict[str, Any]) -> dict[str, Any]:
        """A service the agent plans to use (`uses` in eukollab.json): chosen with trials on the first call, preferring
        `prefer`, then called. `expect` is a JSON Schema the output must satisfy when the provider is chosen."""
        need, prefer, expect = str(body["need"]), body.get("prefer"), body.get("expect")
        if (need, prefer) not in planned:
            check = (lambda out: not any(jsonschema.Draft202012Validator(expect).iter_errors(out))) if expect else None
            planned[(need, prefer)] = agent.service(need, prefer=prefer, sample=body.get("sample"), check=check)
        declared = planned[(need, prefer)]
        result = await agent.aio.call_planned(declared, body.get("input") or {}, body.get("idempotency_key"))
        return {
            "value": result.value,
            "execution_id": result.execution_id,
            "charged": result.charged,
            "receipt_checked": result.receipt_checked,
            "safety_flags": result.safety_flags,
            "provider": declared.main.handle if declared.main else prefer,
        }

    actions = {
        "post_need": post_need,
        "find": find,
        "choose": choose,
        "call": call,
        "message": message,
        "close_agreement": close_agreement,
        "service": service,
    }

    async def status(_: Request) -> Response:
        # Whether the connector holds the agent-operation credential: without it every action is refused.
        return JSONResponse({"live": agent.enabled})

    async def run(request: Request) -> Response:
        action = actions.get(request.path_params["action"])
        if action is None:
            return JSONResponse({"error": "NOT_FOUND"}, status_code=404)
        try:
            return JSONResponse(await action(await request.json()))
        except BalanceLow as exc:
            # `detail` carries what the next pack needs (`needed`, `currency`), so the callbacks can tell their person.
            return JSONResponse({"error": exc.code, "message": str(exc), "detail": exc.detail}, status_code=402)
        except AgentError as exc:
            return JSONResponse({"error": exc.code, "message": str(exc), "detail": exc.detail}, status_code=409)
        except (KeyError, ValueError) as exc:
            return JSONResponse({"error": "INVALID_REQUEST", "message": str(exc)}, status_code=400)

    return Starlette(
        routes=[Route("/agent/status", status, methods=["GET"]), Route("/agent/{action}", run, methods=["POST"])],
        middleware=[Middleware(LocalGuard)],
    )
