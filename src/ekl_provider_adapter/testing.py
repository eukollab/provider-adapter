# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The network, in memory, for testing the connector's agent side with no network and no credential.

    from ekl_provider_adapter.testing import FakeNetwork

    fake = FakeNetwork(handle="@my-agent")
    fake.provider("@vat-check", "cap_vat_validate", lambda args: {"valid": True}, price="0.05")
    fake.need("Validate EU VAT numbers", author="@buyer", matched="cap_my_service")
    app = use_only_app(callbacks_address, agent_network=fake)   # the connector, its events to the callbacks file
    fake.run(app.state.ekl_agent)                               # delivers every scripted event to the callbacks
    assert fake.replies[0]["body"].startswith("I can")

The fake answers the network's tools the way EuKollab does, in memory: needs, replies, connections, trials, choices,
agreements, paid calls from a wallet and its packs, calls with receipts and reports, review periods and reviews.
Console rules are scripted with `rules(...)`.
"""

# ruff: noqa: ARG002 - the fake answers each network tool with its real argument names, whether it uses them or not

import asyncio
import datetime as dt
import json
import math
import re
import secrets
import unicodedata
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import jsonschema

from ekl_provider_adapter.agent import Agent
from ekl_provider_adapter.errors import AgentError, BalanceLow, NotAllowed, ProviderUnavailable
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
    # A pack of more than one call, as agent.json publishes it: {"type": "pack", "price": "5.00", "calls": 500}. Its
    # agreements record no per-call price, as the network's do; the agreement's plan carries the pack's price.
    plan: dict[str, Any] | None = None
    calls: int = 0
    # EuKollab's tested quality of the service, as search results carry it; empty until the test sets it.
    tested_quality: dict[str, Any] = field(default_factory=dict)


class FakeNetwork:
    """EuKollab in memory, for one agent. Everything it is asked to do is recorded for the test to check."""

    def __init__(self, handle: str = "@my-agent", rank_by: str = "accuracy"):
        # What agent_get_profile answers about each peer's availability, by handle.
        self.availability: dict[str, list[dict[str, Any]]] = {}
        self.handle, self.agent_id = handle, _id("agt")
        self.config: dict[str, Any] = {
            "sourcing": {"rank_by": rank_by, "mode": "auto_within_limits", "on_pack_end": "compare"}
        }
        # A new agent's rules share public and synthetic data only, as the network's defaults do.
        self.data_classes = {"public", "synthetic"}
        self.providers: dict[str, FakeProvider] = {}
        # Every trial asked for, with the expectation it was scored against (the network keeps it as a trial case).
        self.trials: list[dict[str, Any]] = []
        self.posts: dict[str, dict[str, Any]] = {}
        self.replies: list[dict[str, Any]] = []
        self.needs: list[dict[str, Any]] = []
        self.responses: list[dict[str, Any]] = []
        self.messages: list[dict[str, Any]] = []
        self.outreach_sent: list[dict[str, Any]] = []
        self.threads: dict[str, dict[str, Any]] = {}
        self.choices: list[dict[str, Any]] = []
        self.reports: list[dict[str, Any]] = []
        self.reviews: list[dict[str, Any]] = []
        self.subscriptions: list[dict[str, Any]] = []
        self.failed: list[dict[str, Any]] = []
        self.handlers: list[str] = []
        self.connections: dict[str, dict[str, Any]] = {}
        self.agreements: dict[str, dict[str, Any]] = {}
        self.executions: dict[str, dict[str, Any]] = {}
        # The organization's wallet, in USD: None never runs out; an amount that can't buy the next pack is refused
        # with BALANCE_LOW, as the network does.
        self.wallet: Decimal | None = None
        # The calls left of each pack bought, by provider handle and capability.
        self.packs: dict[tuple[str, str], int] = {}
        # Paid or free runs made to each service, by provider handle and capability: this agent's record with it.
        self.calls_made: dict[tuple[str, str], int] = {}
        # True: the next reservation's grant expires unused, as when the machine sleeps before the call.
        self.lapse = False
        # The agent's state, as agent_get_self reports it: "review" or "added" test an agent not yet confirmed.
        self.state = "active"
        # Every reservation asked for, and what each events_next read declared, to check against.
        self.reserved: list[dict[str, Any]] = []
        self.reads: list[dict[str, Any]] = []
        self.reservations: dict[str, dict[str, Any]] = {}
        self._events: list[dict[str, Any]] = []
        self._open_reviews: set[str] = set()
        self._seq = 0
        self._acked = 0

    # --- scripting ------------------------------------------------------------------------------------------------

    def rules(
        self, *, rank_by: str | None = None, approve_each: bool | None = None, data_classes: list[str] | None = None
    ) -> None:
        """The console's sourcing rules: Prefer (accuracy, price, balanced), Who chooses, and the data classes the
        agent may share."""
        if data_classes is not None:
            self.data_classes = set(data_classes)
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
        plan: dict[str, Any] | None = None,
    ) -> FakeProvider:
        """Another agent's service: `work` turns arguments into a result. `excluded_by` names the console rule that
        excludes it; `plan` is a pack of several calls it sells instead of a per-call price."""
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
            plan=plan,
        )
        self.providers[f"{handle}/{capability_id}"] = found
        return found

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

    def outreach(
        self,
        sender: str,
        text: str,
        *,
        intent: str = "question",
        refs: dict[str, Any] | None = None,
        state: str = "received",
        thread_id: str | None = None,
    ) -> str:
        """A message in an outreach thread: `received` opens one to this agent (which is then its recipient),
        `replied` and `declined` answer one this agent opened."""
        thread_id = thread_id or _id("thr")
        if state == "received":
            self.threads[thread_id] = {"state": "open", "recipient": self.handle, "sender": sender}
        else:
            self.threads.setdefault(thread_id, {"recipient": sender, "sender": self.handle})["state"] = state
        data: dict[str, Any] = {"from": sender, "message_id": _id("msg"), "body": text}
        if state == "received":
            data |= {"intent": intent, "refs": refs or {}}
        self._emit(f"outreach.{state}", data, thread_id)
        return thread_id

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
        await agent._started()  # as the event loop does: a failing on_start is logged, and events still arrive
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

    def _tool_world_get_contract(self) -> dict[str, Any]:
        return {"protocol": "ekp/3.2", "profile": "ekp/3.2", "accepted_protocols": []}

    def _tool_agent_get_profile(self, handle: str) -> dict[str, Any]:
        """A peer's profile with its availability (EKP 1.2 §6.12), as set in `availability`."""
        return {"handle": handle, "availability": list(self.availability.get(handle, []))}

    def _tool_agent_get_self(self) -> dict[str, Any]:
        return {"agent_id": self.agent_id, "handle": self.handle, "state": self.state, "config": self.config}

    def _tool_feed_subscribe(self, **subscription: Any) -> dict[str, Any]:
        self.subscriptions.append(subscription)
        return {"subscriptions": self.subscriptions}

    def _tool_events_next(
        self, handlers: list[str] | None = None, check_in_seconds: int | None = None, **_: Any
    ) -> dict[str, Any]:
        self.handlers = list(handlers or [])
        self.reads.append({"check_in_seconds": check_in_seconds})
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
        if self.connections.get(connection_id, {}).get("state") in ("active", "declined"):
            # As the network answers: a request already answered can't be answered again.
            raise AgentError("no pending request for this agent to answer", code="INVALID_STATE")
        self.responses.append({"connection_id": connection_id, "decision": decision, "note": note})
        self.connections.setdefault(connection_id, {"connection_id": connection_id})["state"] = (
            "active" if decision == "accept" else "declined"
        )
        return {"connection_id": connection_id, "state": self.connections[connection_id]["state"]}

    def _tool_message_send(self, connection_id: str, body: str, **_: Any) -> dict[str, Any]:
        sent = {"connection_id": connection_id, "body": body, "message_id": _id("msg")}
        self.messages.append(sent)
        return sent

    def _tool_outreach_send(
        self, recipient: str, intent: str, body: str, refs: dict[str, Any] | None = None, **_: Any
    ) -> dict[str, Any]:
        thread_id = _id("thr")
        self.threads[thread_id] = {"state": "open", "recipient": recipient, "sender": self.handle}
        self.outreach_sent.append(
            {"thread_id": thread_id, "recipient": recipient, "intent": intent, "refs": refs or {}, "body": body}
        )
        return {"thread_id": thread_id, "state": "open"}

    def _tool_outreach_respond(self, thread_id: str, action: str, body: str = "", **_: Any) -> dict[str, Any]:
        # As the network answers: a closed thread takes no more messages, and only its recipient declines it.
        thread = self.threads.setdefault(thread_id, {"state": "open", "recipient": self.handle, "sender": ""})
        if thread["state"] not in ("open", "replied"):
            raise AgentError(f"thread is {thread['state']}", code="INVALID_STATE")
        if action == "decline" and thread["recipient"] != self.handle:
            raise AgentError("only the recipient can decline", code="INVALID_REQUEST")
        thread["state"] = "declined" if action == "decline" else "replied"
        self.outreach_sent.append({"thread_id": thread_id, "action": action, "body": body})
        return {"thread_id": thread_id, "state": thread["state"]}

    def _search_item(self, p: FakeProvider) -> dict[str, Any]:
        return {
            "agent_id": "agt_" + p.handle.strip("@").replace("-", "")[:12],
            "handle": p.handle,
            "capability_id": p.capability_id,
            "version": "1.0.0",
            "summary": p.summary,
            "price_per_call": p.price,
            "plans": [p.plan]
            if p.plan
            else [
                {"plan_id": "per_call", "type": "pack", "price": p.price, "calls": 1}
                if Decimal(p.price)
                else {"plan_id": "free", "type": "free"}
            ],
            "currency": "USD",
            "accepts_trials": p.accepts_trials,
            "tested_quality": p.tested_quality,
            "side_effect_class": p.side_effect_class,
            "eligible": not p.excluded_by,
            "excluded_by": p.excluded_by,
            "reputation": {"reviews": 0, "rating": None},
            "data_retention": "none",
            "used_before": self._used_before(p.handle, p.capability_id),
        }

    def _used_before(self, handle: str, capability_id: str) -> dict[str, Any] | None:
        """This agent's record with a service, as agent_search gives it; None when it never called it."""
        calls = self.calls_made.get((handle, capability_id), 0)
        if not calls:
            return None
        at = "2026-11-18T10:00:00+00:00"
        return {
            "first_used_at": at,
            "last_used_at": at,
            "calls": calls,
            "calls_left": self.packs.get((handle, capability_id), 0),
            "own_reviews": {"count": 0, "rating": None},
        }

    def _tool_agent_search(self, query: str, limit: int = 10, need_key: str | None = None) -> dict[str, Any]:
        words = {w for w in query.lower().split() if len(w) > 2}
        found = [
            p
            for p in self.providers.values()
            if not words or words & set((p.summary + " " + p.capability_id).lower().replace("_", " ").split())
        ]
        items = [self._search_item(p) for p in (found or list(self.providers.values()))[:limit]]
        shown = {(i["handle"], i["capability_id"]) for i in items}
        earlier: list[dict[str, Any]] = []
        for choice in reversed([c for c in self.choices if need_key and c["need_key"] == need_key]):
            for handle in (choice["primary"], choice.get("backup")):
                capability_id = next(
                    (str(c["capability_id"]) for c in choice.get("candidates") or [] if c.get("handle") == handle), ""
                )
                key = (str(handle), capability_id)
                if not handle or key in shown or any((e["handle"], e["capability_id"]) == key for e in earlier):
                    continue
                p = self.providers.get(f"{handle}/{capability_id}")
                if p is not None:
                    earlier.append(self._search_item(p) | {"still_listed": True})
                else:  # no longer listed: named, never offered
                    earlier.append(
                        {
                            "handle": handle,
                            "capability_id": capability_id,
                            "summary": "",
                            "eligible": False,
                            "excluded_by": ["no longer listed"],
                            "still_listed": False,
                            "used_before": self._used_before(str(handle), capability_id),
                        }
                    )
        return {"items": items, "previously_used": earlier}

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

    def _tool_capability_trial(
        self, provider: str, capability_id: str, sample_input: dict[str, Any], expect: Any = None
    ) -> dict[str, Any]:
        """A trial as EKP 3.1 answers it: a score against `expect`, never the output. The fake scores as the network
        does without a model: checks that need the model are cannot_tell, and words are MODEL_UNAVAILABLE."""
        if expect is None:
            raise AgentError("A trial needs an expectation (EKP 3.1).", code="UPGRADE_REQUIRED")
        p = self._provider(provider, capability_id)
        if p.excluded_by:
            raise NotAllowed("excluded by your rules", code="POLICY_DENIED", detail={"rule": p.excluded_by[0]})
        expectation = validate_expectation(expect)
        if p.input_schema:
            try:
                jsonschema.validate(sample_input, p.input_schema)
            except jsonschema.ValidationError as exc:
                raise AgentError(
                    f"The sample doesn't fit the input schema: {exc.message}", code="INVALID_REQUEST"
                ) from exc
        self.trials.append({"provider": provider, "capability_id": capability_id, "expect": expect})
        trial_id = _id("trl")
        try:
            result = p.work(sample_input)
        except Exception:  # the fake provider received the call and failed: counted, scoring 0
            return {
                "trial_id": trial_id,
                "provider": provider,
                "capability_id": capability_id,
                "status": "failed",
                "score": 0.0,
                "passed": False,
                "threshold": expectation["threshold"],
                "checks": [],
                "error_code": "FAILED_BEFORE_EFFECT",
            }
        scored = score_trial(expectation, result, p.output_schema)
        return {
            "trial_id": trial_id,
            "provider": provider,
            "capability_id": capability_id,
            "status": "succeeded",
        } | scored

    def _tool_sourcing_record(self, need_key: str, primary: str, **record: Any) -> dict[str, Any]:
        waiting = self.config["sourcing"]["mode"] == "approve_each" or (
            record.get("reason") == "pack_end" and self.config["sourcing"].get("on_pack_end") == "ask"
        )
        stored = {
            "decision_id": _id("src"),
            "need_key": need_key,
            "primary": primary,
            "state": "awaiting_owner" if waiting else "recorded",
            **record,
        }
        self.choices.append(stored)
        return stored

    def _tool_sourcing_get(self, need_key: str) -> dict[str, Any]:
        """The latest decision for the need, as the network keeps it across restarts."""
        found = [c for c in self.choices if c["need_key"] == need_key]
        return {"decision": dict(found[-1]) if found else None}

    def _approved(self, handle: str) -> bool:
        if self.config["sourcing"]["mode"] != "approve_each":
            return True
        return any(c["state"] == "approved" and handle in (c["primary"], c.get("backup")) for c in self.choices)

    def _tool_connection_list(self, state: str = "active") -> dict[str, Any]:
        return {"connections": [c for c in self.connections.values() if c.get("state") == state]}

    def _tool_connection_request(
        self, target: str, capability_ids: list[str], note: str = "", outreach_thread_id: str | None = None
    ) -> dict[str, Any]:
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
        if outreach_thread_id:  # as the network does: a connection asked for from a thread converts it
            self.threads[outreach_thread_id]["state"] = "converted"
        return {"connection_id": connection_id, "state": "pending"}

    def _tool_collaboration_create(
        self, connection_id: str, capability_id: str, data_class: str = "synthetic", **_: Any
    ) -> dict[str, Any]:
        if data_class not in self.data_classes:
            raise NotAllowed(
                f"data class {data_class} not allowed", code="POLICY_DENIED", detail={"rule": "data_class"}
            )
        peer = self.connections[connection_id]["peer"]
        p = self._provider(peer, capability_id)
        per_call = "0.00" if p.plan else p.price
        made = {
            "data_class": data_class,
            "collaboration_id": _id("col"),
            "engagement_state": "open",
            "price_per_call": per_call,
            "currency": "USD",
            "terms": {
                "capability_id": capability_id,
                "price_per_call": per_call,
                "hash": "terms-" + secrets.token_hex(4),
            }
            | ({"plan": p.plan} if p.plan else {}),
            "provider": peer,
        }
        if p.plan:
            made["plan"] = p.plan
        self.agreements[made["collaboration_id"]] = made
        return made

    def _tool_engagement_get(self, collaboration_id: str) -> dict[str, Any]:
        found = self.agreements[collaboration_id]
        left = self.packs.get((str(found["provider"]), str(found["terms"]["capability_id"])))
        return dict(found) | {"packs": [{"calls_left": left}] if left is not None else []}

    def _tool_packs_list(self) -> dict[str, Any]:
        return {
            "packs": [
                {"provider": handle, "capability_id": capability_id, "calls_left": left}
                for (handle, capability_id), left in self.packs.items()
            ]
        }

    def _tool_engagement_close(self, collaboration_id: str, outcome: str = "completed") -> dict[str, Any]:
        self.agreements[collaboration_id]["engagement_state"] = outcome
        return {"collaboration_id": collaboration_id, "engagement_state": outcome}

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

    def _tool_execution_reserve(self, execution_id: str, **request: Any) -> dict[str, Any]:
        """A paid run takes a call of a pack already bought, or buys the next pack from the wallet; it never waits.
        The fake pays the pack at once, where the network pays it when the run's succeeded receipt is reported."""
        self.reserved.append({"execution_id": execution_id} | request)
        earlier = next((r for r in self.reservations.values() if r["execution_id"] == execution_id), None)
        if earlier is not None:
            # A retry under the same execution ID returns the reservation as it stands, as the network does.
            return {k: v for k, v in earlier.items() if k != "execution_grant" or earlier["state"] == "held"}
        record: dict[str, Any] = {"reservation_id": _id("rsv"), "execution_id": execution_id, "amount_scale": 6}
        if self.lapse:
            self.lapse = False  # the next reservation is granted at once
            return record | {"state": "expired", "amount_atoms": "0"}
        agreement = self.agreements.get(str(request.get("collaboration_id")), {})
        key = (str(agreement.get("provider", "")), str(request.get("capability_id", "")))
        self.calls_made[key] = self.calls_made.get(key, 0) + 1
        p = self.providers.get(f"{key[0]}/{key[1]}")
        plan = (p.plan if p else None) or {"price": p.price if p else "0", "calls": 1}
        price = Decimal(str(plan.get("price") or "0"))
        if self.packs.get(key, 0) > 0:
            self.packs[key] -= 1
            held = {"funding": "pack", "amount_atoms": "0"}
        else:
            if price > Decimal(int(request.get("requested_max_amount_atoms") or 0)) / 1_000_000:
                raise AgentError("The run would cost more than offered.", code="INVALID_REQUEST")
            if self.wallet is not None and self.wallet < price:
                raise BalanceLow(
                    "Your wallet can't cover this run's pack.",
                    code="BALANCE_LOW",
                    detail={"needed": f"{price:.2f}", "currency": "USD"},
                )
            if self.wallet is not None:
                self.wallet -= price
            self.packs[key] = int(plan.get("calls") or 1) - 1
            held = {"funding": "purchase", "amount_atoms": str(int(price * 1_000_000))}
        record |= {"state": "held", "execution_grant": "fake-grant"} | held
        self.reservations[record["reservation_id"]] = record
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
            raise ProviderUnavailable(
                "The provider could not be reached (ConnectError).", code="SERVICE_UNAVAILABLE", detail={"sent": False}
            )
        if p.fails == "timeout":
            p.calls += 1
            self.executions[execution_id] = {"execution_id": execution_id, "state": "admitted"}
            raise ProviderUnavailable(
                "The provider could not be reached (ReadTimeout).", code="SERVICE_UNAVAILABLE", detail={"sent": True}
            )
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
                },
            }
            self.executions[execution_id] = kept
            for reservation in self.reservations.values():
                if reservation["execution_id"] == execution_id:
                    reservation["state"] = "captured"  # the succeeded receipt pays the run
        return ProviderAnswer(kept["value"], False, {}, "", kept["value"])


# --- the fake's scorer: the deterministic part of the network's (EKP 3.1 §9.4) ------------------------------------
# It scores exactly as the network does without a model, so a test that passes here passes there; the protocol's
# scoring vectors hold both to it. Checks that need the network's model are cannot_tell here.

STEP = 0.05
CHECK_STEP = 0.25
MAX_CHECKS = 20
MAX_EXPECT_BYTES = 64 * 1024
KINDS = ("equals", "one_of", "set_equals", "schema", "approx", "range", "within", "length", "contains")
MODEL_KINDS = ("similar_to", "criterion", "media")
_CHECK_KEYS = frozenset({"id", "path", "weight", "tolerance", "min", *KINDS, *MODEL_KINDS})
_ID = re.compile(r"^[A-Za-z0-9_.-]{1,40}$")
_TOLERANCE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(%?)\s*$")
_TOKEN = re.compile(r"\.([A-Za-z_][\w-]*)|\[(\d+|\*|'[^']*'|\"[^\"]*\")\]")


class _Missing:
    pass


_MISSING = _Missing()


def _invalid(message: str) -> AgentError:
    return AgentError(f"expect: {message}", code="INVALID_REQUEST")


def validate_expectation(expect: Any) -> dict[str, Any]:
    """The expectation as the network accepts it, or INVALID_REQUEST naming what is wrong; words need the network's
    model, which the fake doesn't have (MODEL_UNAVAILABLE)."""
    if isinstance(expect, str):
        if not expect.strip() or len(expect.strip()) > 2000:
            raise _invalid("an expectation in words is 1 to 2,000 characters")
        raise AgentError(
            "An expectation in words needs the network's model; give checks instead.", code="MODEL_UNAVAILABLE"
        )
    if not isinstance(expect, dict) or set(expect) - {"checks", "threshold"}:
        raise _invalid("an object with checks and a threshold, or a sentence")
    if len(json.dumps(expect, default=str)) > MAX_EXPECT_BYTES:
        raise _invalid("an expectation is at most 64 KB")
    threshold = expect.get("threshold", 0.8)
    if not _number(threshold) or not 0 <= float(threshold) <= 1:
        raise _invalid("threshold is a number from 0 to 1")
    checks = expect.get("checks", [])
    if not isinstance(checks, list) or len(checks) > MAX_CHECKS:
        raise _invalid(f"checks is a list of at most {MAX_CHECKS}")
    ids: set[str] = set()
    for check in checks:
        _validate_check(check, ids)
    return {"checks": checks, "threshold": float(threshold)}


def _validate_check(check: Any, ids: set[str]) -> None:  # noqa: PLR0912, PLR0915 - one rule per field
    if not isinstance(check, dict):
        raise _invalid("each check is an object")
    cid = check.get("id")
    if not isinstance(cid, str) or not _ID.match(cid) or cid == "schema" or cid in ids:
        raise _invalid(f"check id {cid!r} must be unique, 1 to 40 of A-Z a-z 0-9 _ . -, and not `schema`")
    ids.add(cid)
    unknown = sorted(set(check) - _CHECK_KEYS)
    if unknown:
        raise _invalid(f"check {cid}: unknown {', '.join(unknown)}")
    kinds = [k for k in (*KINDS, *MODEL_KINDS) if k in check and (k == "equals" or check[k] is not None)]
    if len(kinds) != 1:
        raise _invalid(f"check {cid} needs exactly one kind, not {', '.join(kinds) or 'none'}")
    kind = kinds[0]
    weight = check.get("weight", 1.0)
    if not _number(weight) or not 0 < float(weight) <= 100:
        raise _invalid(f"check {cid}: weight is above 0 and at most 100")
    if "tolerance" in check and kind != "approx":
        raise _invalid(f"check {cid}: tolerance goes with approx")
    if "min" in check and kind != "similar_to":
        raise _invalid(f"check {cid}: min goes with similar_to")
    path = check.get("path")
    if path is not None:
        if not isinstance(path, str) or len(path) > 200:
            raise _invalid(f"check {cid}: path is a JSONPath of at most 200 characters")
        _tokens(path)
    if kind == "approx":
        if not _number(check["approx"]):
            raise _invalid(f"check {cid}: approx is a finite number")
        _tolerance(check.get("tolerance"), float(check["approx"]))
    elif kind == "range":
        bounds = check["range"]
        if not isinstance(bounds, list) or len(bounds) != 2 or not all(_number(b) for b in bounds):
            raise _invalid(f"check {cid}: range is [min, max], two finite numbers")
    elif kind == "within":
        window = check["within"]
        if not isinstance(window, dict) or set(window) != {"of", "days"} or _moment(window["of"]) is None:
            raise _invalid(f"check {cid}: within is {{of: an ISO date or time, days}}")
        if not _number(window["days"]) or not 0 <= float(window["days"]) <= 36500:
            raise _invalid(f"check {cid}: within.days is 0 to 36,500")
    elif kind == "length":
        bounds = check["length"]
        if not isinstance(bounds, dict) or set(bounds) - {"min", "max"}:
            raise _invalid(f"check {cid}: length is {{min, max}}")
        if not all(v is None or (_number(v) and float(v) >= 0) for v in bounds.values()):
            raise _invalid(f"check {cid}: length bounds are numbers of at least 0")
    elif kind in ("one_of", "set_equals", "contains"):
        if not isinstance(check[kind], list):
            raise _invalid(f"check {cid}: {kind} is a list")
        if kind == "contains" and not all(isinstance(w, str) for w in check[kind]):
            raise _invalid(f"check {cid}: contains is a list of texts")
    elif kind == "schema":
        if not isinstance(check["schema"], dict):
            raise _invalid(f"check {cid}: schema is a JSON Schema object")
        try:
            jsonschema.Draft202012Validator.check_schema(check["schema"])
        except jsonschema.SchemaError as exc:
            raise _invalid(f"check {cid}: schema is not a valid JSON Schema ({exc.message})") from exc
        _within_itself(cid, check["schema"])


# The share of the consumer's check weight that must be scored for a trial to pass.
MIN_COVERAGE = 0.5


def _within_itself(cid: str, schema: dict[str, Any]) -> None:
    """As the network: a consumer's schema refers only within itself, and its patterns compile, so it can be applied
    to any output; refused now otherwise, never after the provider has worked."""
    stack: list[Any] = [schema]
    while stack:
        node = stack.pop()
        if isinstance(node, list):
            stack.extend(node)
            continue
        if not isinstance(node, dict):
            continue
        for key, value in node.items():
            if key in ("$ref", "$dynamicRef", "$recursiveRef") and not (
                isinstance(value, str) and _resolves(schema, value)
            ):
                raise _invalid(f"check {cid}: its schema refers to {str(value)[:80]!r}, which is not within it")
            patterns = [value] if key == "pattern" else list(value) if key == "patternProperties" else []
            for pattern in patterns:
                try:
                    re.compile(str(pattern))
                except re.error as exc:
                    raise _invalid(
                        f"check {cid}: its pattern {str(pattern)[:80]!r} is not a regular expression"
                    ) from exc
            stack.append(value)


def _resolves(schema: dict[str, Any], ref: str) -> bool:
    """Whether a local reference (`#`, `#/$defs/x`) points into the schema."""
    if ref == "#":
        return True
    if not ref.startswith("#/"):
        return False
    node: Any = schema
    for part in ref[2:].split("/"):
        key = part.replace("~1", "/").replace("~0", "~")
        if isinstance(node, dict) and key in node:
            node = node[key]
        elif isinstance(node, list) and key.isdigit() and int(key) < len(node):
            node = node[int(key)]
        else:
            return False
    return True


def score_trial(expectation: dict[str, Any], result: Any, output_schema: dict[str, Any] | None) -> dict[str, Any]:
    """Score, passed, threshold and each check as the network answers them, never a value of the output."""
    own = _score_schema(result, output_schema or {})
    given = [_score_check(result, c) for c in expectation.get("checks") or []]
    threshold = float(expectation.get("threshold", 0.8))
    checks = [own, *given]
    if given and all(c["score"] is None for c in given):
        # Nothing the consumer asked could be scored: no score, never a pass on the service's own schema alone.
        score = None
    else:
        scored = [c for c in checks if c["score"] is not None]
        total = sum(c["weight"] for c in scored)
        score = _step(sum(c["weight"] * c["score"] for c in scored) / total, STEP) if total else None
    return {
        "score": score,
        # As the network: a pass needs at least half the consumer's check weight scored.
        "passed": score is not None
        and score >= threshold
        and (
            not given
            or sum(c["weight"] for c in given if c["score"] is not None)
            >= MIN_COVERAGE * sum(c["weight"] for c in given)
        ),
        "threshold": threshold,
        "checks": [
            {
                "id": c["id"],
                "score": None if c["score"] is None else _step(c["score"], CHECK_STEP),
                "reason": c["reason"],
            }
            for c in checks
        ],
    }


def _step(value: float, step: float) -> float:
    """Rounded down to the step: a score never claims more than it earned."""
    return round(math.floor(max(0.0, min(1.0, value)) / step + 1e-9) * step, 2)


def _tokens(path: str) -> list[tuple[str | None, str | None]]:
    rest = path[1:] if path.startswith("$") else "." + path if not path.startswith((".", "[")) else path
    found: list[tuple[str | None, str | None]] = []
    pos = 0
    while pos < len(rest):
        match = _TOKEN.match(rest, pos)
        if match is None:
            raise _invalid(f"path {path!r} is not a JSONPath this network reads")
        found.append((match.group(1), match.group(2)))
        pos = match.end()
    return found


def _at(data: Any, path: str | None) -> Any:
    """The value at a JSONPath (`$.a.b[0]`, `$['a b']`, `$.items[*]` for every item); _MISSING when it isn't there."""
    if not path or path == "$":
        return data
    values: list[Any] = [data]
    spread = False
    for name, index in _tokens(path):
        found: list[Any] = []
        for value in values:
            if name is not None or (index and index[0] in "'\""):
                key = name if name is not None else str(index)[1:-1]
                if isinstance(value, dict) and key in value:
                    found.append(value[key])
            elif index == "*":
                spread = True
                if isinstance(value, list):
                    found.extend(value)
                elif isinstance(value, dict):
                    found.extend(value.values())
            elif isinstance(value, list) and int(str(index)) < len(value):
                found.append(value[int(str(index))])
        values = found
    if spread:
        return values
    return values[0] if values else _MISSING


def _score_schema(result: Any, schema: dict[str, Any]) -> dict[str, Any]:
    # No declared output schema is the empty schema, which every output satisfies.
    if not schema:
        return {"id": "schema", "weight": 1.0, "score": 1.0, "reason": "matched"}
    try:
        ok = not any(jsonschema.Draft202012Validator(schema).iter_errors(result))
    except Exception:  # a schema the fake can't apply says nothing of the output
        return {"id": "schema", "weight": 1.0, "score": None, "reason": "cannot_tell"}
    return {"id": "schema", "weight": 1.0, "score": 1.0 if ok else 0.0, "reason": "matched" if ok else "mismatch"}


def _score_check(result: Any, check: dict[str, Any]) -> dict[str, Any]:
    found = {"id": str(check["id"]), "weight": float(check.get("weight", 1.0))}
    kind = next(k for k in (*KINDS, *MODEL_KINDS) if k in check and (k == "equals" or check[k] is not None))
    if kind == "media":
        return found | {"score": None, "reason": "cannot_tell"}
    value = _at(result, check.get("path"))
    if value is _MISSING:
        return found | {"score": 0.0, "reason": "missing"}
    if kind in MODEL_KINDS:
        return found | {"score": None, "reason": "cannot_tell"}
    got, reason = _deterministic(kind, check, value)
    return found | {"score": got, "reason": reason}


def _deterministic(kind: str, check: dict[str, Any], value: Any) -> tuple[float, str]:  # noqa: PLR0911, PLR0912 - one branch per kind
    if kind == "equals":
        return (1.0, "matched") if _same(value, check["equals"]) else (0.0, "mismatch")
    if kind == "one_of":
        return (1.0, "matched") if any(_same(value, v) for v in check["one_of"]) else (0.0, "mismatch")
    if kind == "set_equals":
        if not isinstance(value, list):
            return 0.0, "wrong_type"
        return (1.0, "matched") if _as_set(value) == _as_set(check["set_equals"]) else (0.0, "mismatch")
    if kind == "schema":
        try:
            ok = not any(jsonschema.Draft202012Validator(check["schema"]).iter_errors(value))
        except Exception:  # as the network: a schema it can't apply is a mismatch
            ok = False
        return (1.0, "matched") if ok else (0.0, "mismatch")
    if kind in ("approx", "range"):
        if not _number(value):
            return 0.0, "wrong_type"
        if kind == "approx":
            centre = float(check["approx"])
            return _near(abs(float(value) - centre), _tolerance(check.get("tolerance"), centre))
        low, high = sorted(float(b) for b in check["range"])
        return _in_range(float(value), low, high)
    if kind == "within":
        when, of = _moment(value), _moment(check["within"]["of"])
        if when is None or of is None:
            return 0.0, "wrong_type"
        return _near(abs((when - of).total_seconds()) / 86400, float(check["within"]["days"]))
    if kind == "length":
        if not isinstance(value, (str, list, dict)):
            return 0.0, "wrong_type"
        bounds = check["length"]
        low = float(bounds.get("min") or 0.0)
        high = float(bounds["max"]) if bounds.get("max") is not None else math.inf
        low, high = min(low, high), max(low, high)
        return _in_range(float(len(value)), low, high)
    text = _norm(value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str))
    return (1.0, "matched") if all(_norm(w) in text for w in check["contains"]) else (0.0, "mismatch")


def _near(distance: float, tolerance: float) -> tuple[float, str]:
    """1 within the tolerance; beyond it, falling linearly to 0 at twice the tolerance."""
    if distance <= tolerance:
        return 1.0, "matched"
    if tolerance <= 0:
        return 0.0, "out_of_range"
    left = 1.0 - (distance - tolerance) / tolerance
    return (left, "near") if left > 0 else (0.0, "out_of_range")


def _in_range(value: float, low: float, high: float) -> tuple[float, str]:
    """1 inside [low, high]; outside, falling linearly to 0 one range-width away (at least 1 for an open or
    single-point range)."""
    if low <= value <= high:
        return 1.0, "matched"
    width = high - low if math.isfinite(high) and high > low else max(abs(low), 1.0)
    distance = low - value if value < low else value - high
    left = 1.0 - distance / width
    return (left, "near") if left > 0 else (0.0, "out_of_range")


def _tolerance(tolerance: Any, approx: float) -> float:
    if tolerance is None:
        return 0.0
    if _number(tolerance) and float(tolerance) >= 0:
        return float(tolerance)
    found = _TOLERANCE.match(tolerance) if isinstance(tolerance, str) else None
    if found is None:
        raise _invalid(f'tolerance {tolerance!r} is a number of at least 0, or a percentage such as "1%"')
    amount = float(found.group(1))
    return abs(approx) * amount / 100 if found.group(2) else amount


def _number(value: Any) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(float(value))
    except OverflowError:
        return False


def _norm(text: str) -> str:
    return " ".join(unicodedata.normalize("NFC", text).casefold().split())


def _canonical(value: Any) -> Any:
    """A value compared as the network compares it, at every depth: text normalised, numbers as numbers."""
    if isinstance(value, str):
        return _norm(value)
    if _number(value):
        number = float(value)
        return int(number) if number.is_integer() else number
    if isinstance(value, dict):
        return {str(k): _canonical(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_canonical(v) for v in value]
    return value


def _same(a: Any, b: Any) -> bool:
    return bool(json.dumps(_canonical(a), sort_keys=True) == json.dumps(_canonical(b), sort_keys=True))


def _as_set(values: list[Any]) -> Counter[str]:
    """The members, order ignored and duplicates counted, as the network compares them."""
    return Counter(json.dumps(_canonical(v), sort_keys=True) for v in values)


def _moment(value: Any) -> dt.datetime | None:
    if not isinstance(value, str):
        return None
    try:
        moment = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return moment if moment.tzinfo else moment.replace(tzinfo=dt.UTC)
