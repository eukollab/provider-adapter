# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Test an agent's behaviour with no network and no credential.

    from ekl_provider_adapter.testing import FakeNetwork

    fake = FakeNetwork(handle="@my-agent")
    fake.provider("@vat-check", "cap_vat_validate", lambda args: {"valid": True}, price="0.05")
    fake.need("Validate EU VAT numbers", author="@buyer", matched="cap_my_service")
    agent = Agent(fake, AgentJournal(":memory:"))   # or attach(server, agent_network=fake).agent
    fake.run(agent)                                  # delivers every scripted event to the callbacks
    assert fake.replies[0]["body"].startswith("I can")

The fake answers the network's tools the way EuKollab does, in memory: needs, replies, connections, trials, choices,
agreements, calls with receipts and reports, review periods and reviews. Console rules are scripted with
`rules(...)`.
"""

# ruff: noqa: ARG002 - the fake answers each network tool with its real argument names, whether it uses them or not

import asyncio
import datetime as dt
import json
import secrets
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ekl_provider_adapter import stand_ins
from ekl_provider_adapter.agent import Agent
from ekl_provider_adapter.errors import AgentError, NotAllowed, ProviderUnavailable
from ekl_provider_adapter.fingerprint import fingerprint
from ekl_provider_adapter.network_client import ProviderAnswer


def _id(prefix: str) -> str:
    return f"{prefix}_{secrets.token_hex(6)}"


def _now() -> str:
    return dt.datetime.now(dt.UTC).isoformat()


@dataclass
class FakeProvider:
    """Another agent's service as the fake network offers it: `work` answers each call and `calls` counts them."""

    handle: str
    capability_id: str
    work: Callable[[dict[str, Any]], Any]
    price: str = "0"
    summary: str = ""
    input_schema: dict[str, Any] = field(default_factory=dict)
    output_schema: dict[str, Any] = field(default_factory=dict)
    accepts_trials: bool = True
    example: dict[str, Any] | None = None
    excluded_by: list[str] = field(default_factory=list)
    side_effect_class: str = "read_only"
    # How calls fail, to test failover: "unreachable" (nothing reaches it) or "timeout" (it ran, no answer came).
    fails: str = ""
    calls: int = 0


class FakeNetwork:
    """EuKollab in memory, for one agent. Everything it is asked to do is recorded for the test to check."""

    def __init__(self, handle: str = "@my-agent", rank_by: str = "accuracy"):
        self.handle, self.agent_id = handle, _id("agt")
        self.config: dict[str, Any] = {"sourcing": {"rank_by": rank_by, "mode": "auto_within_limits"}}
        self.providers: dict[str, FakeProvider] = {}
        self.posts: dict[str, dict[str, Any]] = {}
        self.replies: list[dict[str, Any]] = []
        self.needs: list[dict[str, Any]] = []
        self.responses: list[dict[str, Any]] = []
        self.messages: list[dict[str, Any]] = []
        self.choices: list[dict[str, Any]] = []
        self.reports: list[dict[str, Any]] = []
        self.reviews: list[dict[str, Any]] = []
        self.subscriptions: list[dict[str, Any]] = []
        self.failed: list[dict[str, Any]] = []
        self.handlers: list[str] = []
        self.connections: dict[str, dict[str, Any]] = {}
        self.agreements: dict[str, dict[str, Any]] = {}
        self.executions: dict[str, dict[str, Any]] = {}
        # Paid runs wait for the provider's approval: "approve" after one read, "payment" (asked, then expired) or
        # "decline". None approves at once, as a standing approval would.
        self.approval: str | None = None
        self.reservations: dict[str, dict[str, Any]] = {}
        self._events: list[dict[str, Any]] = []
        self._open_reviews: set[str] = set()
        self._seq = 0
        self._acked = 0

    # --- scripting ------------------------------------------------------------------------------------------------

    def rules(self, *, rank_by: str | None = None, approve_each: bool | None = None) -> None:
        """The console's sourcing rules: Prefer (accuracy, price, balanced) and Who chooses."""
        if rank_by is not None:
            self.config["sourcing"]["rank_by"] = rank_by
        if approve_each is not None:
            self.config["sourcing"]["mode"] = "approve_each" if approve_each else "auto_within_limits"

    def provider(
        self,
        handle: str,
        capability_id: str,
        work: Callable[[dict[str, Any]], Any],
        *,
        price: str = "0",
        summary: str = "",
        input_schema: dict[str, Any] | None = None,
        output_schema: dict[str, Any] | None = None,
        accepts_trials: bool = True,
        example: dict[str, Any] | None = None,
        excluded_by: list[str] | None = None,
        side_effect_class: str = "read_only",
    ) -> FakeProvider:
        """Another agent's service: `work` turns arguments into a result. `excluded_by` names the console rule that
        excludes it."""
        found = FakeProvider(
            handle,
            capability_id,
            work,
            price,
            summary or capability_id,
            input_schema or {},
            output_schema or {},
            accepts_trials,
            example,
            excluded_by or [],
            side_effect_class,
        )
        self.providers[f"{handle}/{capability_id}"] = found
        return found

    def stand_ins(self, where: Path | None = None) -> list[FakeProvider]:
        """The project's recorded stand-ins (eukollab/fixtures/, or EKL_FIXTURES) as providers, so tests use
        real-shaped answers: each answers with its recorded output for the same input, or its first."""
        added = []
        for fixture in stand_ins.every(where):
            handle, _, name = str(fixture["service"]).partition("/")
            examples = list(fixture.get("examples") or [])

            def work(arguments: dict[str, Any], examples: list[dict[str, Any]] = examples) -> Any:
                return next((e for e in examples if e.get("input") == arguments), examples[0]).get("output")

            added.append(
                self.provider(
                    handle,
                    name if name.startswith("cap_") else f"cap_{name}",
                    work,
                    summary=str(fixture.get("service")),
                    input_schema=fixture.get("input_schema"),
                    output_schema=fixture.get("output_schema"),
                    example={"input": examples[0].get("input")} if examples else None,
                )
            )
        return added

    def _emit(self, kind: str, data: dict[str, Any], resource_id: str = "") -> None:
        self._seq += 1
        self._events.append(
            {
                "seq": self._seq,
                "event_id": _id("evt"),
                "type": kind,
                "resource_id": resource_id,
                "data": data,
                "occurred_at": _now(),
            }
        )

    def _post(
        self, author: str, body: str, post_type: str, topics: list[str], reply_to: str | None = None
    ) -> dict[str, Any]:
        post_id = _id("post")
        post: dict[str, Any] = {
            "post_id": post_id,
            "author": author,
            "author_id": _id("agt"),
            "post_type": post_type,
            "body": body,
            "refs": {},
            "need_state": "open" if post_type == "capability_need" else None,
            "reply_to": reply_to,
            "topics": topics,
            "created_at": _now(),
        }
        self.posts[post_id] = post
        return post

    def need(
        self,
        text: str,
        *,
        author: str = "@someone",
        topics: list[str] | None = None,
        matched: str | None = None,
        service: str = "",
        price: str = "Free",
    ) -> dict[str, Any]:
        """Another agent posts a need. With `matched`, the network matched it to this agent's service of that ID;
        without, it arrives as a feed post (for `matching="subscribed"` or `"all"`)."""
        post = self._post(author, text, "capability_need", [t.lower() for t in topics or []])
        if matched:
            self._emit(
                "need.matched", {"need": post, "capability_id": matched, "service": service or matched, "price": price}
            )
        else:
            self._emit("feed.post", post, post["post_id"])
        return post

    def answer(self, need_id: str, text: str, *, author: str = "@provider") -> dict[str, Any]:
        """Another agent replies to one of this agent's needs."""
        reply = self._post(author, text, "insight", [], reply_to=need_id)
        self.posts[need_id].setdefault("replies", []).append(reply)
        self._emit("post.replied", reply, reply["post_id"])
        return reply

    def connection_request(
        self,
        requester: str,
        *,
        capability_ids: list[str] | None = None,
        organisation: str = "Their organisation",
        tier: str = "member",
        verified_domain: bool = False,
    ) -> str:
        connection_id = _id("con")
        self.connections[connection_id] = {
            "connection_id": connection_id,
            "peer": requester,
            "state": "pending",
            "peer_capabilities": [],
        }
        self._emit(
            "connection.requested",
            {
                "requester": requester,
                "capability_ids": capability_ids or [],
                "organisation": {"name": organisation, "tier": tier, "verified_domain": verified_domain},
            },
            connection_id,
        )
        return connection_id

    def message(self, sender: str, text: str, *, connection_id: str | None = None) -> None:
        self._emit(
            "message.received",
            {
                "from": sender,
                "connection_id": connection_id or _id("con"),
                "message_id": _id("msg"),
                "body": text,
                "content_type": "text/plain",
                "payload": {},
            },
        )

    def approve(self, decision_id: str) -> None:
        """The publisher approves a shortlist that `choose` left waiting."""
        found = next(c for c in self.choices if c["decision_id"] == decision_id)
        found["state"] = "approved"
        self._emit(
            "sourcing.approved",
            {
                "decision_id": decision_id,
                "need_key": found["need_key"],
                "primary": found["primary"],
                "backup": found.get("backup"),
            },
        )

    def decision(self, kind: str, subject_id: str, *, fallback: str = "rule") -> None:
        deadline = (dt.datetime.now(dt.UTC) + dt.timedelta(hours=1)).isoformat()
        self._emit(
            "decision.requested", {"kind": kind, "subject_id": subject_id, "deadline": deadline, "fallback": fallback}
        )

    def review_due(
        self,
        counterpart: str,
        capability_id: str,
        *,
        role: str = "consumer",
        period: str = "closed",
        calls: int | None = None,
        useful: int = 0,
        not_useful: int = 0,
        not_assessed: int = 0,
        charged: str = "0.00",
        collaboration_id: str | None = None,
    ) -> str:
        """A review period opens for one of this agent's agreements: `period` is "closed" or a month ("2026-10").
        `calls`, the receipt-backed calls, defaults to the reports' sum. Returns the period's ID; what the agent
        submits is in `reviews`, a derived one with `method` "derived"."""
        period_id = _id("rp")
        collaboration_id = collaboration_id or _id("col")
        closes = (dt.datetime.now(dt.UTC) + dt.timedelta(days=14)).isoformat()
        self._open_reviews.add(period_id)
        self._emit(
            "review.requested",
            {
                "collaboration_id": collaboration_id,
                "review_period_id": period_id,
                "period": period,
                "role": role,
                "counterpart": counterpart,
                "capability_id": capability_id,
                "capability_version": "1.0.0",
                "calls": useful + not_useful + not_assessed if calls is None else calls,
                "useful": useful,
                "not_useful": not_useful,
                "not_assessed": not_assessed,
                "charged": charged,
                "currency": "USD",
                "review_closes_at": closes,
            },
            collaboration_id,
        )
        return period_id

    def event(self, kind: str, data: dict[str, Any] | None = None, resource_id: str = "") -> None:
        """Any other event, for `on(event_type)` callbacks."""
        self._emit(kind, data or {}, resource_id)

    # --- running the agent against it -----------------------------------------------------------------------------

    def run(self, agent: Agent) -> None:
        """Deliver every scripted event to the agent's callbacks, as its event loop would, then return."""
        asyncio.run(self.drive(agent))

    async def drive(self, agent: Agent) -> None:
        agent._loop = asyncio.get_running_loop()
        await agent._identify()
        await agent._subscribe()
        for callback, _ in agent._callbacks.get("on_start", []):
            await agent._invoke(callback)
        attempts: dict[str, int] = {}
        for _ in range(1000):
            page = await self.call("events_next", {"handlers": agent.handler_names()})
            if not page["events"]:
                return
            acked, failed, retry = await agent._handle_page(page["events"], attempts)
            if acked is not None:
                await self.call("events_ack", {"cursor": acked, "failed": failed or None})
            if not retry and acked is None:
                return

    # --- the network's tools --------------------------------------------------------------------------------------

    async def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        handler = getattr(self, f"_tool_{tool}", None)
        if handler is None:
            raise AgentError(f"FakeNetwork does not know the tool {tool}.", code="NOT_FOUND")
        result: dict[str, Any] = handler(**{k: v for k, v in arguments.items() if v is not None})
        return result

    def _tool_agent_get_self(self) -> dict[str, Any]:
        return {"agent_id": self.agent_id, "handle": self.handle, "state": "active", "config": self.config}

    def _tool_feed_subscribe(self, **subscription: Any) -> dict[str, Any]:
        self.subscriptions.append(subscription)
        return {"subscriptions": self.subscriptions}

    def _tool_events_next(self, handlers: list[str] | None = None, **_: Any) -> dict[str, Any]:
        self.handlers = list(handlers or [])
        pending = [e for e in self._events if e["seq"] > self._acked]
        return {
            "events": pending[:50],
            "next_cursor": pending[-1]["seq"] if pending else self._acked,
            "lease": {"held": True},
        }

    def _tool_events_ack(self, cursor: int, failed: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        self._acked = max(self._acked, cursor)
        self.failed.extend(failed or [])
        return {"acked": self._acked}

    def _tool_post_create(
        self,
        post_type: str,
        body: str,
        refs: dict[str, Any] | None = None,
        topics: list[str] | None = None,
        reply_to: str | None = None,
        expires_days: int | None = None,
    ) -> dict[str, Any]:
        post = self._post(self.handle, body, post_type, topics or [], reply_to)
        if expires_days is not None:
            post["expires_days"] = expires_days
        if reply_to:
            self.replies.append(post)
            if reply_to in self.posts:
                self.posts[reply_to].setdefault("replies", []).append(post)
        elif post_type == "capability_need":
            self.needs.append(post)
        return post

    def _tool_post_set_state(self, post_id: str, state: str, extend_days: int | None = None) -> dict[str, Any]:
        post = self.posts[post_id]
        post["need_state"] = state
        return post

    def _tool_post_thread(self, post_id: str) -> dict[str, Any]:
        return {"replies": [], **self.posts[post_id]}

    def _tool_connection_respond(self, connection_id: str, decision: str, note: str = "") -> dict[str, Any]:
        self.responses.append({"connection_id": connection_id, "decision": decision, "note": note})
        self.connections.setdefault(connection_id, {"connection_id": connection_id})["state"] = (
            "active" if decision == "accept" else "declined"
        )
        return {"connection_id": connection_id, "state": self.connections[connection_id]["state"]}

    def _tool_message_send(self, connection_id: str, body: str, **_: Any) -> dict[str, Any]:
        sent = {"connection_id": connection_id, "body": body, "message_id": _id("msg")}
        self.messages.append(sent)
        return sent

    def _search_item(self, p: FakeProvider) -> dict[str, Any]:
        return {
            "agent_id": "agt_" + p.handle.strip("@").replace("-", "")[:12],
            "handle": p.handle,
            "capability_id": p.capability_id,
            "version": "1.0.0",
            "summary": p.summary,
            "price_per_call": p.price,
            "plans": [
                {
                    "plan_id": "per_call" if p.price not in ("0", "0.00") else "free",
                    "type": "per_call" if p.price not in ("0", "0.00") else "free",
                    "price": p.price,
                }
            ],
            "currency": "USD",
            "accepts_trials": p.accepts_trials,
            "side_effect_class": p.side_effect_class,
            "eligible": not p.excluded_by,
            "excluded_by": p.excluded_by,
            "reputation": {"reviews": 0, "rating": None},
            "data_retention": "none",
        }

    def _tool_agent_search(self, query: str, limit: int = 10) -> dict[str, Any]:
        words = {w for w in query.lower().split() if len(w) > 2}
        found = [
            p
            for p in self.providers.values()
            if not words or words & set((p.summary + " " + p.capability_id).lower().replace("_", " ").split())
        ]
        return {"items": [self._search_item(p) for p in (found or list(self.providers.values()))[:limit]]}

    def _provider(self, handle: str, capability_id: str) -> FakeProvider:
        found = self.providers.get(f"{handle}/{capability_id}")
        if found is None:
            raise AgentError(f"{handle} offers no {capability_id}.", code="NOT_FOUND")
        return found

    def _tool_agent_get_showcase(self, handle: str, capability_id: str) -> dict[str, Any]:
        p = self._provider(handle, capability_id)
        examples = [{"input": p.example}] if p.example else []
        manifest = {"input_schema": p.input_schema, "output_schema": p.output_schema}
        return {"handle": handle, "manifest": manifest, "examples": examples}

    def _tool_capability_trial(self, provider: str, capability_id: str, sample_input: dict[str, Any]) -> dict[str, Any]:
        p = self._provider(provider, capability_id)
        if p.excluded_by:
            raise NotAllowed("excluded by your rules", code="POLICY_DENIED", detail={"rule": p.excluded_by[0]})
        try:
            result = p.work(sample_input)
        except Exception as exc:  # the fake provider failed: a failed trial, as the network reports it
            return {"trial_id": _id("trl"), "status": "failed", "result": None, "error": str(exc)}
        return {"trial_id": _id("trl"), "status": "succeeded", "result": result, "error": None}

    def _tool_sourcing_record(self, need_key: str, primary: str, **record: Any) -> dict[str, Any]:
        waiting = self.config["sourcing"]["mode"] == "approve_each"
        stored = {
            "decision_id": _id("src"),
            "need_key": need_key,
            "primary": primary,
            "state": "awaiting_owner" if waiting else "recorded",
            **record,
        }
        self.choices.append(stored)
        return stored

    def _approved(self, handle: str) -> bool:
        if self.config["sourcing"]["mode"] != "approve_each":
            return True
        return any(c["state"] == "approved" and handle in (c["primary"], c.get("backup")) for c in self.choices)

    def _tool_connection_list(self, state: str = "active") -> dict[str, Any]:
        return {"connections": [c for c in self.connections.values() if c.get("state") == state]}

    def _tool_connection_request(self, target: str, capability_ids: list[str], note: str = "") -> dict[str, Any]:
        if not self._approved(target):
            raise NotAllowed(
                f"{target} is waiting for your publisher's approval.",
                code="POLICY_DENIED",
                detail={"rule": "sourcing.mode"},
            )
        connection_id = _id("con")
        self.connections[connection_id] = {
            "connection_id": connection_id,
            "peer": target,
            "state": "active",
            "peer_capabilities": capability_ids,
        }
        return {"connection_id": connection_id, "state": "pending"}

    def _tool_collaboration_create(self, connection_id: str, capability_id: str, **_: Any) -> dict[str, Any]:
        peer = self.connections[connection_id]["peer"]
        p = self._provider(peer, capability_id)
        made = {
            "collaboration_id": _id("col"),
            "engagement_state": "open",
            "price_per_call": p.price,
            "currency": "USD",
            "terms": {
                "capability_id": capability_id,
                "price_per_call": p.price,
                "hash": "terms-" + secrets.token_hex(4),
            },
            "provider": peer,
        }
        self.agreements[made["collaboration_id"]] = made
        return made

    def _tool_engagement_get(self, collaboration_id: str) -> dict[str, Any]:
        return dict(self.agreements[collaboration_id])

    def _tool_peer_get_access(self, connection_id: str, capability_ids: list[str]) -> dict[str, Any]:
        expires = (dt.datetime.now(dt.UTC) + dt.timedelta(minutes=15)).isoformat()
        return {
            "profile": "adapter",
            "endpoint": f"fake://{self.connections[connection_id]['peer']}",
            "access_token": "fake-token",
            "expires_at": expires,
            "capabilities": [
                {"capability_id": c, "version": "1.0.0", "tool": c.removeprefix("cap_")} for c in capability_ids
            ],
            "contracts": {c: self._contract(self.connections[connection_id]["peer"], c) for c in capability_ids},
        }

    def _contract(self, handle: str, capability_id: str) -> str:
        p = self._provider(handle, capability_id)
        return str(json.dumps({"input": p.input_schema, "output": p.output_schema}, sort_keys=True))

    def _tool_execution_reserve(self, execution_id: str, **_: Any) -> dict[str, Any]:
        record = {"reservation_id": _id("rsv"), "execution_id": execution_id, "state": "held"}
        if self.approval is None:
            return record | {"execution_grant": "fake-grant"}
        record["state"] = "awaiting_approval"
        self.reservations[record["reservation_id"]] = record
        return dict(record)

    def _tool_execution_reservation_get(self, reservation_id: str) -> dict[str, Any]:
        record = self.reservations[reservation_id]
        if self.approval == "approve":
            record |= {"state": "held", "execution_grant": "fake-grant"}
        elif self.approval == "payment":
            record |= {"state": "expired", "payment": {"url": "https://buy.stripe.com/fake", "amount": "8.00"}}
        else:
            record |= {"state": "released", "decision_reason": "no pack bought"}
        return dict(record)

    def _tool_execution_get(self, execution_id: str, provider_agent_id: str = "") -> dict[str, Any]:
        found = self.executions.get(execution_id)
        if found is None:
            raise AgentError("no such execution", code="NOT_FOUND")
        return {"execution": found}

    def _tool_collaboration_report(self, **report: Any) -> dict[str, Any]:
        self.reports.append(report)
        return {"receipt_backed": bool(report.get("receipt"))}

    def _tool_review_submit(self, collaboration_id: str, review_period_id: str = "", **review: Any) -> dict[str, Any]:
        if review_period_id not in self._open_reviews:
            reviewed = any(r["review_period_id"] == review_period_id for r in self.reviews)
            code = "ALREADY_REVIEWED" if reviewed else "REVIEW_NOT_OPEN"
            raise AgentError("This period cannot be reviewed now.", code=code)
        self._open_reviews.discard(review_period_id)
        stored = {"collaboration_id": collaboration_id, "review_period_id": review_period_id, **review}
        self.reviews.append(stored)
        return {"review_id": _id("rev"), "state": "hidden"}

    # --- providers, called directly ------------------------------------------------------------------------------

    async def call_provider(
        self, endpoint: str, token: str, tool: str, arguments: dict[str, Any], ekl: dict[str, Any]
    ) -> ProviderAnswer:
        handle = endpoint.removeprefix("fake://")
        p = self._provider(handle, str(ekl["capability_id"]))
        execution_id, salt = str(ekl["execution_id"]), str(ekl["salt"])
        if p.fails == "unreachable":
            raise ProviderUnavailable("The provider could not be reached (ConnectError).", detail={"sent": False})
        if p.fails == "timeout":
            p.calls += 1
            self.executions[execution_id] = {"execution_id": execution_id, "state": "admitted"}
            raise ProviderUnavailable("The provider could not be reached (ReadTimeout).", detail={"sent": True})
        kept = self.executions.get(execution_id)
        if kept is None:
            # The provider runs each execution once; a repeat returns the kept result.
            p.calls += 1
            value = p.work(arguments)
            kept = {
                "execution_id": execution_id,
                "state": "completed",
                "outcome": "succeeded",
                "value": value,
                "receipt": {
                    "input_fingerprint": fingerprint(salt, arguments),
                    "output_fingerprint": fingerprint(salt, value),
                    "amount": p.price,
                },
            }
            self.executions[execution_id] = kept
        return ProviderAnswer(kept["value"], False, {}, "", kept["value"])
