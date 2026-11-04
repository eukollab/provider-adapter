# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The connector's agent side, for servers in any language.

Events go to a route in the publisher's own app (EKL_CALLBACK_URL) as JSON, signed with an HMAC-SHA256 over the
body keyed by the agent credential, in the X-EKL-Signature header (`sha256=<hex>`). The app answers with what to do:
`{"reply": "…"}`, `{"accept": true}`, `{"decline": "…"}`, `{"review": {…}}`, `{"skip": true}`, or nothing. For
actions the app starts itself (post a need, choose, call a service, or call a declared service with
`POST /agent/service`), the connector serves the same calls as a small HTTP API on 127.0.0.1 only, and
`GET /agent/status` says whether it holds the agent-operation credential.
"""

import hashlib
import hmac
import json
from typing import Any

import httpx2
import jsonschema
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from ekl_provider_adapter.agent import (
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
from ekl_provider_adapter.errors import AgentError
from ekl_provider_adapter.runs import RunDecision, RunRequest, Runs

SIGNATURE_HEADER = "X-EKL-Signature"


def sign(secret: str, body: bytes) -> str:
    """The X-EKL-Signature value for an event body. The app checks it by recomputing it over the raw body it
    received, so only a sender holding the agent credential can post events to it."""
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def _candidate(c: Candidate | None) -> dict[str, Any] | None:
    if c is None:
        return None
    return {"handle": c.handle, "capability_id": c.capability_id, "summary": str(c.summary), "price": c.price}


def bridge(agent: Agent, url: str, secret: str, http: httpx2.AsyncClient | None = None) -> None:
    """Register every callback on `agent`, each forwarding its event to the publisher's app and applying its answer."""

    async def ask(event: str, data: dict[str, Any]) -> dict[str, Any]:
        body = json.dumps({"event": event, **data}, sort_keys=True, default=str).encode()
        client = http or httpx2.AsyncClient(timeout=30)
        try:
            answer = await client.post(
                url, content=body, headers={"Content-Type": "application/json", SIGNATURE_HEADER: sign(secret, body)}
            )
        finally:
            if http is None:
                await client.aclose()
        answer.raise_for_status()  # an error leaves the event unacknowledged: it comes back
        if not answer.content.strip():
            return {}
        found = answer.json()
        return found if isinstance(found, dict) else {}

    @agent.on_need(matching="my_services")
    async def need(n: Need) -> None:
        service = n.matched_service
        what = await ask(
            "need",
            {
                "need_id": n.post_id,
                "text": str(n.text),
                "author": n.author,
                "topics": n.topics,
                "matched_service": service.__dict__ if service else None,
            },
        )
        if what.get("reply"):
            await agent.aio.reply(n.post_id, str(what["reply"]))

    @agent.on_need_answered
    async def answered(a: Answer) -> None:
        what = await ask(
            "need_answered", {"need_id": a.need.post_id, "reply_id": a.post_id, "text": str(a.text), "author": a.author}
        )
        if what.get("reply"):
            await agent.aio.reply(a.post_id, str(what["reply"]))

    @agent.on_connection_request
    async def connection(r: ConnectionRequest) -> None:
        what = await ask(
            "connection_request",
            {
                "connection_id": r.connection_id,
                "requester": r.requester,
                "capability_ids": r.capability_ids,
                "organisation": r.organisation.__dict__,
            },
        )
        if what.get("accept"):
            await agent.aio.respond(r.connection_id, True, "")
        elif "decline" in what:
            await agent.aio.respond(r.connection_id, False, str(what["decline"] or ""))

    @agent.on_message
    async def message(m: Message) -> None:
        what = await ask(
            "message", {"connection_id": m.connection_id, "from": m.sender, "text": str(m.text), "payload": m.payload}
        )
        if what.get("reply"):
            await agent.aio.message(m.connection_id, str(what["reply"]))

    @agent.on_choice_approved
    async def approved(c: Choice) -> None:
        await ask(
            "choice_approved",
            {"decision_id": c.decision_id, "main": _candidate(c.main), "backup": _candidate(c.backup)},
        )

    @agent.on_decision()
    async def decision(d: Decision) -> None:
        what = await ask(
            "decision", {"kind": d.kind, "subject_id": d.subject_id, "deadline": d.deadline, "fallback": d.fallback}
        )
        if what.get("accept"):
            await agent.aio.decide(d, True)
        elif "decline" in what:
            await agent.aio.decide(d, False, str(what["decline"] or ""))

    @agent.on_review_due
    async def review_due(r: ReviewDue) -> None:
        what = await ask(
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


async def _service(agent: Agent, handle: str, capability_id: str) -> Candidate:
    for found in await agent.aio.find(capability_id.removeprefix("cap_").replace("_", " "), limit=25):
        if found.handle == handle and found.capability_id == capability_id:
            return found
    raise AgentError(f"{handle} offers no {capability_id} you can use.", code="NOT_FOUND")


def bridge_runs(runs: Runs, url: str, secret: str, http: httpx2.AsyncClient | None = None) -> None:
    """Paid runs waiting for approval go to the publisher's app as `run.approval_requested`, signed like every
    callback. Its answer is the decision: {"decision": "approve", "runs": 500}, {"decision": "payment_needed", "url":
    …, "amount": …, "description": …}, {"decision": "decline", "reason": …}, or nothing to decide later through the
    local API's /agent/decide_run."""

    @runs.on_approval
    async def approval(run: RunRequest) -> RunDecision | None:
        body = json.dumps({"event": "run.approval_requested", **run.model_dump()}, sort_keys=True).encode()
        client = http or httpx2.AsyncClient(timeout=30)
        try:
            answer = await client.post(
                url, content=body, headers={"Content-Type": "application/json", SIGNATURE_HEADER: sign(secret, body)}
            )
        finally:
            if http is None:
                await client.aclose()
        answer.raise_for_status()
        found = answer.json() if answer.content.strip() else {}
        return decision_of(found) if isinstance(found, dict) and found.get("decision") else None


def decision_of(body: dict[str, Any]) -> RunDecision:
    """A decision as the publisher's app writes it, in JSON."""
    kind = str(body["decision"])
    if kind == "approve":
        return RunDecision.approve(runs=body.get("runs"), overage=bool(body.get("overage")))
    if kind == "payment_needed":
        return RunDecision.payment_needed(
            str(body["url"]), str(body["amount"]), str(body.get("description", "")), challenge=body.get("challenge")
        )
    if kind == "decline":
        return RunDecision.decline(str(body.get("reason", "")))
    raise ValueError("decision is approve, payment_needed or decline")


def local_api(agent: Agent, runs: Runs | None = None) -> Starlette:
    """The actions the publisher's app starts itself. Bind it to 127.0.0.1 only: it acts with the agent credential."""

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
        }

    async def message(body: dict[str, Any]) -> dict[str, Any]:
        return await agent.aio.message(body["connection_id"], body["text"])

    # One declared service per need and preference, so the choice made on the first call is reused.
    planned: dict[tuple[str, str | None], PlannedService] = {}

    async def service(body: dict[str, Any]) -> dict[str, Any]:
        """agent.service(need, prefer, sample, expect).call(input) for any language. `expect` is a JSON Schema the
        output must satisfy, in place of Python's check, since a function can't cross languages."""
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
            "stand_in": result.stand_in,
            "provider": declared.main.handle if declared.main else prefer,
        }

    async def decide(body: dict[str, Any]) -> dict[str, Any]:
        """A paid run's decision made later, for example once the publisher's payment system confirmed it."""
        if runs is None:
            raise ValueError("this process approves no runs")
        return await runs.api.decide(str(body["reservation_id"]), decision_of(body))

    actions = {
        "decide_run": decide,
        "post_need": post_need,
        "find": find,
        "choose": choose,
        "call": call,
        "message": message,
        "service": service,
    }

    async def status(_: Request) -> Response:
        # Whether the connector holds the agent-operation credential: a client in another language is live when it
        # does, even without the credential in its own environment, and never answers from a stand-in then.
        return JSONResponse({"live": agent.enabled})

    async def run(request: Request) -> Response:
        action = actions.get(request.path_params["action"])
        if action is None:
            return JSONResponse({"error": "NOT_FOUND"}, status_code=404)
        try:
            return JSONResponse(await action(await request.json()))
        except AgentError as exc:
            return JSONResponse({"error": exc.code, "message": str(exc)}, status_code=409)
        except (KeyError, ValueError) as exc:
            return JSONResponse({"error": "INVALID_REQUEST", "message": str(exc)}, status_code=400)

    return Starlette(
        routes=[Route("/agent/status", status, methods=["GET"]), Route("/agent/{action}", run, methods=["POST"])]
    )
