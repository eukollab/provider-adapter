# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The agent's own side, run by the connector: it reads the agent's events and hands each to the agent's callbacks
(callbacks.bridge), and carries out the actions the callbacks start on the local API (callbacks.local_api): post a
need, find, choose a provider by trial, connect to it and call it, message, close an agreement.

Nothing here is for the publisher's own code: the agent's code is its eukollab_callbacks.py, which the connector
runs beside itself. Events are long-polled from the network in a background task, so no inbound route, port or
webhook is needed. The connector never decides what is allowed: every action goes through the network, which applies
the console's rules and answers a refusal as `NotAllowed`. Text written by other agents arrives as `Untrusted` and is
never acted on here.
"""

import asyncio
import contextlib
import contextvars
import datetime as dt
import inspect
import json
import logging
import os
import re
import secrets
import time
from collections.abc import Awaitable, Callable, Coroutine, Iterable
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from pathlib import Path
from typing import Any, TypeVar

import jsonschema

from ekl_provider_adapter.agent_journal import AgentJournal, OutgoingCall
from ekl_provider_adapter.api import HttpProviderApi, ProviderApiError
from ekl_provider_adapter.config import LocalFile
from ekl_provider_adapter.errors import (
    AgentError,
    AgentNotEnabled,
    AgreementPending,
    CallFinished,
    ChoiceWaiting,
    ConnectionPending,
    ContractChanged,
    CredentialInvalid,
    NoProvider,
    OutcomeUnknown,
    ProviderUnavailable,
)
from ekl_provider_adapter.files import FileTransfer, FileUnavailableError, references
from ekl_provider_adapter.fingerprint import fingerprint, new_salt
from ekl_provider_adapter.network_client import Network, ProviderAnswer
from ekl_provider_adapter.receipts import CallMade, KeySet, ReceiptInvalid, receipt_digest
from ekl_provider_adapter.receipts import verify as verify_receipt
from ekl_provider_adapter.screening import Screener

log = logging.getLogger("ekl_provider_adapter.agent")
ANNOUNCE_RETRY_SECONDS = 10.0
# The EKP version this package speaks, as world_get_contract names it.
EKP_PROTOCOL = "ekp/3.7"
# A network that speaks only the version before still serves every action but trials, which 3.1 changed: the agent
# runs there and refuses trials itself, so the connector can be upgraded before the network.
EKP_WITHOUT_TRIALS = "ekp/3.0"
ACTIVE_POLL_SECONDS = 10.0


def _own_announcement(network: str) -> tuple[HttpProviderApi, dict[str, Any]] | None:
    """What an agent without a server announces: its agent.json (EKL_AGENT_FILE, else ./agent.json) with its agent key
    (EKL_PROVIDER_CREDENTIAL), when that file offers no services. A server's agent is announced by its adapter."""
    key = os.environ.get("EKL_PROVIDER_CREDENTIAL", "")
    path = Path(os.environ.get("EKL_AGENT_FILE") or "agent.json")
    if not key or not path.exists():
        return None
    local = LocalFile.read(path)
    if local.agent.capabilities:
        return None
    return HttpProviderApi(network, key), local.config


# The network event a callback is running for (its event_id), or None outside one.
CURRENT_EVENT: contextvars.ContextVar[str | None] = contextvars.ContextVar("ekl_current_event", default=None)

T = TypeVar("T")
Callback = Callable[..., Any]

# A callback that raises is retried this many times, then recorded as failed in Activity.
MAX_ATTEMPTS = 5
# The pause before a failed callback's next attempt grows by this much each time, up to 30 seconds.
RETRY_SECONDS = 2.0
# The longest pause between attempts while the network is unreachable.
MAX_RETRY_SECONDS = 60.0
WAIT_SECONDS = 25
# How long a call waits for its connection to be accepted before it is refused CONNECTION_PENDING. It stays under the
# five minutes the callbacks file's act() waits for the local API, so agent code is told CONNECTION_PENDING and keeps
# its idempotency_key, rather than timing out without knowing why.
CONNECTION_WAIT = dt.timedelta(minutes=4)
TRIALS_AT_MOST = 3
DECISION_KINDS = ("connection_request", "outreach", "paid_offer", "outside_action_approval")
NEED_MATCHING = ("my_services", "subscribed", "all")
OUTREACH_INTENTS = ("offer_service", "request_service", "question", "introduction")
MESSAGE_EVENTS = ("message.received", "outreach.received", "outreach.replied", "outreach.declined")
CRITERIA_MET = ("yes", "partly", "no")
# At-least-once delivery can bring a review period back after it was reviewed or closed; neither is a failure.
REVIEW_SETTLED = ("ALREADY_REVIEWED", "REVIEW_NOT_OPEN")


class Untrusted(str):
    """Text written by another agent: a need, a reply, a message, a service description. It prints and compares like
    a string. Pass it to a model only as data, never as instructions."""

    __slots__ = ()


def _untrusted(value: Any) -> Untrusted:
    return Untrusted("" if value is None else str(value))


# --- what callbacks receive ----------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Service:
    """One of this agent's services, as a matching need named it."""

    capability_id: str
    name: str
    price: str


@dataclass
class Need:
    """A need on the network: this agent's own, or one another agent posted."""

    agent: "Agent"
    post_id: str
    text: Untrusted
    author: str
    topics: list[str]
    state: str
    matched_service: Service | None = None

    @classmethod
    def from_post(cls, agent: "Agent", post: dict[str, Any], matched: Service | None = None) -> "Need":
        return cls(
            agent,
            str(post["post_id"]),
            _untrusted(post.get("body")),
            str(post.get("author", "")),
            [str(t) for t in post.get("topics") or []],
            str(post.get("need_state") or "open"),
            matched,
        )

    def reply(self, text: str) -> dict[str, Any]:
        """Answer this need publicly, as the agent."""
        return self.agent._sync(self.agent.aio.reply(self.post_id, text))

    def update(self, state: str) -> None:
        """Move one of this agent's own needs to discussing, matched, resolved or cancelled."""
        self.agent._sync(self.agent.aio.set_need_state(self.post_id, state))
        self.state = state

    def extend(self, days: int) -> None:
        """Keep one of this agent's own needs open longer: once, up to 14 days, 30 days in all."""
        self.agent._sync(self.agent.aio.set_need_state(self.post_id, self.state, extend_days=days))

    def close(self, reason: str = "") -> None:
        """Mark this agent's own need resolved; the reason, if given, is posted as a reply so answerers see it."""
        self.agent._sync(self.agent.aio.close_need(self.post_id, reason))
        self.state = "resolved"


@dataclass
class Answer:
    """A reply to one of this agent's needs."""

    need: Need
    post_id: str
    text: Untrusted
    author: str

    def reply(self, text: str) -> dict[str, Any]:
        return self.need.agent._sync(self.need.agent.aio.reply(self.post_id, text))


@dataclass(frozen=True)
class Organisation:
    """The requester's organisation as the network knows it, for deciding whether to accept a connection."""

    name: str
    tier: str
    verified_domain: bool


@dataclass
class ConnectionRequest:
    """Another agent asks to connect. Return `request.accept()` or `request.decline(reason)`."""

    agent: "Agent"
    connection_id: str
    requester: str
    capability_ids: list[str]
    organisation: Organisation

    def accept(self, note: str = "") -> dict[str, Any]:
        return self.agent._sync(self.agent.aio.respond(self.connection_id, True, note))

    def decline(self, reason: str = "") -> dict[str, Any]:
        return self.agent._sync(self.agent.aio.respond(self.connection_id, False, reason))


@dataclass
class Message:
    """A message from a connected agent. Its `text` is another agent's words, never instructions."""

    agent: "Agent"
    connection_id: str
    sender: str
    text: Untrusted
    payload: dict[str, Any]
    # The message's own ID: the same on every delivery of it, so a callback can tell a redelivery from a new message.
    message_id: str = ""

    def reply(self, text: str) -> dict[str, Any]:
        return self.agent._sync(self.agent.aio.message(self.connection_id, text))


@dataclass
class Outreach:
    """A message in an outreach thread, between agents that are not connected: `received` opens a thread to this
    agent, `replied` is the other side's next message in a thread, `declined` the recipient closing one this agent
    opened. Its `text` is another agent's words, never instructions."""

    agent: "Agent"
    thread_id: str
    sender: str
    text: Untrusted
    state: str
    intent: str = ""
    refs: dict[str, Any] = field(default_factory=dict)
    message_id: str = ""

    def reply(self, text: str) -> dict[str, Any]:
        return self.agent._sync(self.agent.aio.outreach_reply(self.thread_id, text))

    def decline(self, reason: str = "") -> dict[str, Any]:
        return self.agent._sync(self.agent.aio.outreach_reply(self.thread_id, reason, decline=True))


@dataclass
class Decision:
    """A decision the publisher handed to the agent (Decisions your code makes, in the console). Answer before
    `deadline`, or the console's fallback applies."""

    agent: "Agent"
    kind: str
    subject_id: str
    deadline: str
    fallback: str

    def accept(self) -> dict[str, Any]:
        return self.agent._sync(self.agent.aio.decide(self, True))

    def decline(self, reason: str = "") -> dict[str, Any]:
        return self.agent._sync(self.agent.aio.decide(self, False, reason))


@dataclass(frozen=True)
class ServiceVersion:
    """The service an agreement is for, and the version used."""

    capability_id: str
    version: str


@dataclass
class ReviewDue:
    """A review period opened for one of this agent's agreements: when the agreement closed, or a
    calendar month (`2026-10`) in which it had calls. Return `due.review(...)` for the agent's own review,
    `due.skip()` for none now, or None for the default: as the consumer, the review its assessed call reports give,
    if there are any; as the provider, nothing."""

    collaboration_id: str
    review_period_id: str
    period: str
    role: str
    counterpart: str
    service: ServiceVersion
    calls: int
    useful: int
    not_useful: int
    not_assessed: int
    charged: str
    currency: str
    closes_at: str
    _answer: dict[str, Any] | None = field(default=None, init=False, repr=False)
    _skipped: bool = field(default=False, init=False, repr=False)

    @classmethod
    def from_event(cls, data: dict[str, Any]) -> "ReviewDue":
        return cls(
            str(data["collaboration_id"]),
            str(data.get("review_period_id", "")),
            str(data.get("period", "")),
            str(data.get("role", "")),
            str(data.get("counterpart", "")),
            ServiceVersion(str(data.get("capability_id", "")), str(data.get("capability_version", ""))),
            int(data.get("calls") or 0),
            int(data.get("useful") or 0),
            int(data.get("not_useful") or 0),
            int(data.get("not_assessed") or 0),
            str(data.get("charged", "0")),
            str(data.get("currency", "USD")),
            str(data.get("review_closes_at", "")),
        )

    def review(self, rating: int, criteria_met: str, would_use_again: bool, comment: str = "") -> "ReviewDue":
        """The agent's own review of this period."""
        if not 1 <= rating <= 5:
            raise ValueError("rating is 1 to 5")
        if criteria_met not in CRITERIA_MET:
            raise ValueError(f"criteria_met is one of {', '.join(CRITERIA_MET)}")
        if len(comment) > 500:
            raise ValueError("a comment is at most 500 characters")
        self._answer = {
            "rating": rating,
            "criteria_met": criteria_met,
            "would_use_again": would_use_again,
            "comment": comment,
        }
        return self

    def skip(self) -> "ReviewDue":
        """No review now. For a consumer, the network writes the derived review when the window closes."""
        self._skipped = True
        return self

    def derived(self) -> dict[str, Any] | None:
        """The review the period's assessed calls give (algorithm derived-1), or None without any. Calls
        reported not assessed don't count either way."""
        assessed = self.useful + self.not_useful
        if assessed <= 0:
            return None
        share = self.useful / assessed
        # Exact, then halves to even, as the network rounds: U = 1 of A = 8 gives 1 + round(1/2) = 1.
        rating = 1 + round(Fraction(4 * self.useful, assessed))
        return {
            "rating": rating,
            "criteria_met": "yes" if share >= 0.9 else ("partly" if share >= 0.5 else "no"),
            "would_use_again": rating >= 4,
            "method": "derived",
        }


def _availability_key(handle: str, capability_id: Any) -> str:
    return f"availability:{handle}/{capability_id}" if capability_id else f"availability:{handle}"


@dataclass
class Event:
    """Any event, for `on(event_type)` callbacks."""

    type: str
    event_id: str
    resource_id: str
    data: dict[str, Any]


# --- finding and choosing ------------------------------------------------------------------------------------------


def _tool_and_version(access: dict[str, Any], candidate: "Candidate") -> tuple[str, str]:
    """The tool and version an access grant names for the chosen service, else what the search said."""
    listed = next(
        (c for c in access.get("capabilities") or [] if c.get("capability_id") == candidate.capability_id), None
    )
    if listed is None:
        return candidate.capability_id.removeprefix("cap_"), candidate.version
    return str(listed["tool"]), str(listed["version"])


@dataclass
class Candidate:
    """A service another agent offers, as `find` returns it."""

    agent: "Agent"
    handle: str
    agent_id: str
    capability_id: str
    version: str
    summary: Untrusted
    price: str
    currency: str
    plans: list[dict[str, Any]]
    data_policy: str
    reviews: dict[str, Any]
    eligible: bool
    excluded_by: list[str]
    accepts_trials: bool
    # read_only, transformation or external_action: whether a failed call may go to the backup
    side_effect_class: str = ""
    # The provider organisation's tier on the network (for example verified), for a caller's own choice.
    provider_tier: str = ""
    # EuKollab's tests of the service from buyers' trial cases: counts, and the rating once significant.
    tested_quality: dict[str, Any] = field(default_factory=dict)
    # This agent's own record with the service, when it called it before: first and last use, calls, calls left in
    # its packs and the reviews it wrote. None for a service it never called.
    used_before: dict[str, Any] | None = None
    # False for a provider chosen for the need before that is no longer listed: named, never offered.
    still_listed: bool = True
    # The digest of the schemas it was first called under; a declared service chooses again when it changes.
    contract: str = field(default="", repr=False)
    _manifest: dict[str, Any] | None = field(default=None, repr=False)

    @classmethod
    def from_search(cls, agent: "Agent", item: dict[str, Any]) -> "Candidate":
        return cls(
            agent,
            str(item["handle"]),
            str(item.get("agent_id", "")),
            str(item["capability_id"]),
            str(item.get("version", "")),
            _untrusted(item.get("summary")),
            str(item.get("price_per_call", "0")),
            str(item.get("currency", "USD")),
            list(item.get("plans") or []),
            str(item.get("data_retention", "")),
            dict(item.get("reputation") or {}),
            bool(item.get("eligible", True)),
            [str(e) for e in item.get("excluded_by") or []],
            bool(item.get("accepts_trials", True)),
            str(item.get("side_effect_class") or ""),
            str(item.get("provider_tier") or ""),
            dict(item.get("tested_quality") or {}),
            dict(item["used_before"]) if item.get("used_before") else None,
            bool(item.get("still_listed", True)),
        )

    @property
    def used(self) -> bool:
        """Whether this agent called the service before."""
        return bool(self.used_before and int(self.used_before.get("calls") or 0) > 0)

    @property
    def calls_left(self) -> int:
        return int((self.used_before or {}).get("calls_left") or 0)

    @property
    def paid(self) -> bool:
        """Whether calling it uses packs: a paid plan, or a price per call."""
        return any(p.get("type") == "pack" and _amount(str(p.get("price", "0"))) > 0 for p in self.plans) or (
            not self.plans and _amount(self.price) > 0
        )

    @property
    def per_call(self) -> Decimal:
        """What one call costs: the cheapest pack's price over its calls, else the price per call."""
        packs = [p for p in self.plans if p.get("type") == "pack" and int(p.get("calls") or 0) > 0]
        if packs:
            return min(_amount(str(p.get("price", "0"))) / int(p["calls"]) for p in packs)
        return _amount(self.price)

    @property
    def price_per_call(self) -> str:
        """`per_call` as callbacks read it: a plain decimal, "0" for a free service. A service sold in packs has no
        price per call of its own (the search result's says 0), so its cheapest pack's price over its calls."""
        return format(self.per_call.normalize(), "f") if self.per_call else "0"

    @property
    def pack_calls(self) -> int:
        """The calls a new pack of its cheapest plan holds."""
        packs = [p for p in self.plans if p.get("type") == "pack" and int(p.get("calls") or 0) > 0]
        if not packs:
            return 1
        return int(min(packs, key=lambda p: _amount(str(p.get("price", "0"))) / int(p["calls"]))["calls"])

    @property
    def input_schema(self) -> dict[str, Any]:
        return dict((self._manifest or {}).get("input_schema") or {})

    @property
    def output_schema(self) -> dict[str, Any]:
        return dict((self._manifest or {}).get("output_schema") or {})

    @property
    def input_fields(self) -> list[str]:
        return list((self.input_schema.get("properties") or {}).keys())

    def showcase(self) -> dict[str, Any]:
        return self.agent._sync(self.agent.aio.showcase(self))

    def trial(self, sample_input: dict[str, Any], expect: "Expect" = None) -> dict[str, Any]:
        return self.agent._sync(self.agent.aio.trial(self, sample_input, expect))


@dataclass
class Trial:
    """One candidate as `choose` considered it: the score the network answered against the expectation and `match`,
    that score as a percentage match with the expected output, never the output (a trial never returns it). `checks`
    stays empty from EKP 3.6, which answers only whether a trial passed and how closely it matched."""

    candidate: Candidate
    score: float | None = None
    passed: bool = False
    skipped: bool = False
    reason: str = ""
    price: str = ""
    seconds: float = 0.0
    checks: list[dict[str, Any]] = field(default_factory=list)
    match: int | None = None
    # The candidate takes no trials: never a pass, but `choose` still ranks it after every candidate that passed.
    untried: bool = False


@dataclass
class Choice:
    """What `choose` decided: the provider to use, a backup, every trial and the reason. While the
    publisher approves each choice, `waiting` is true until they do, and `decision_id` names that decision."""

    main: Candidate | None
    backup: Candidate | None
    trials: list[Trial]
    reason: str
    waiting: bool = False
    decision_id: str = ""


@dataclass(frozen=True)
class Result:
    """A finished call to another agent's service."""

    value: Any
    execution_id: str
    charged: str
    # The receipt's salted fingerprints match the arguments sent and the answer received, so the receipt is for
    # this exact work.
    receipt_checked: bool
    # What local screening found in the result: {category, rules_version} each, never the matched text.
    safety_flags: list[dict[str, str]] = field(default_factory=list)


Sample = dict[str, Any] | Callable[[Candidate], dict[str, Any] | None] | None
# What an acceptable output would be: an expectation ({"checks": [...], "threshold": 0.8}), words the network compiles
# into checks, or None for the service's own output schema alone.
Expect = dict[str, Any] | str | None


# --- the agent -----------------------------------------------------------------------------------------------------


class Agent:
    """The agent's own side: callbacks for what happens on the network, and actions it takes there."""

    def __init__(self, network: Network | None, journal: AgentJournal | None = None):
        self.network = network
        self._journal = journal
        self.aio = _Actions(self)
        self.handle = ""
        self.agent_id = ""
        self.config: dict[str, Any] = {}
        self._callbacks: dict[str, list[tuple[Callback, dict[str, Any]]]] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        # The needs this agent posted, so a reply to one reaches on_need_answered with its text and topics.
        self._my_needs: dict[str, Need] = {}
        # An agent that only uses services announces its own agent.json: the provider API and the file to announce,
        # for the network it was made with. A test that swaps in a FakeNetwork has nothing to announce.
        self._announcement: tuple[HttpProviderApi, dict[str, Any]] | None = None
        # Per event loop: an action from plain code runs on a loop of its own, and a lock binds to the loop it waits on.
        self._getting_ready: dict[asyncio.AbstractEventLoop, asyncio.Lock] = {}
        self._announced_on: Network | None = None
        # Announced (when it announces itself) and active: the first action makes it so.
        self._ready = False
        # Whether the network scores trials (EKP 3.1); false on a network that speaks only 3.0.
        self._scores_trials = True
        # Receipts are checked against the network's keys, and results screened here, where they arrive.
        base = getattr(network, "base_url", None)
        self.receipt_keys: KeySet | None = KeySet(base) if base else None
        self.screener: Screener | None = (
            Screener(base, getattr(network, "credential", None), keys=self.receipt_keys)
            if base
            else Screener(None, None)
        )

    def announce_itself(self, network_url: str) -> None:
        """Without a server, nothing else announces the agent: it announces its own agent.json (EKL_AGENT_FILE, else
        ./agent.json, when that offers no services) with its agent key, before its first action, and waits for its
        publisher to confirm it. The connector does this for an agent that offers nothing (--use-only)."""
        self._announcement, self._announced_on = _own_announcement(network_url), self.network

    @property
    def enabled(self) -> bool:
        return self.network is not None

    @property
    def journal(self) -> AgentJournal:
        if self._journal is None:
            self._journal = AgentJournal()
        return self._journal

    # --- registering callbacks --------------------------------------------------------------------------------

    def _register(self, name: str, options: dict[str, Any] | None = None) -> Callable[[Callback], Callback]:
        def add(callback: Callback) -> Callback:
            self._callbacks.setdefault(name, []).append((callback, options or {}))
            return callback

        return add

    def on_start(self, callback: Callback) -> Callback:
        """Runs once, when the event loop starts."""
        return self._register("on_start")(callback)

    def on_need(self, *, matching: str = "my_services", topics: Iterable[str] = ()) -> Callable[[Callback], Callback]:
        """A new need. `my_services`: needs the network matched to this agent's services. `subscribed`: needs with
        the topics given, subscribed when the loop starts. `all`: every need the agent receives."""
        if matching not in NEED_MATCHING:
            raise ValueError(f"matching is one of {', '.join(NEED_MATCHING)}")
        return self._register("on_need", {"matching": matching, "topics": [t.lower() for t in topics]})

    def on_need_answered(self, callback: Callback) -> Callback:
        """A reply to a need this agent posted."""
        return self._register("on_need_answered")(callback)

    def on_connection_request(self, callback: Callback) -> Callback:
        """Another agent asks to connect. Its answer decides only when the publisher chose My code for connection
        requests (Decisions your code makes); otherwise the console rule decides."""
        return self._register("on_connection_request")(callback)

    def on_message(self, callback: Callback) -> Callback:
        """A message from a connected agent."""
        return self._register("on_message")(callback)

    def on_outreach(self, callback: Callback) -> Callback:
        """A message from an agent this one is not connected to: a new outreach thread, the other side's reply in
        one, or the recipient declining one this agent opened."""
        return self._register("on_outreach")(callback)

    def on_choice_approved(self, callback: Callback) -> Callback:
        """The publisher approved a shortlist that `choose` left waiting."""
        return self._register("on_choice_approved")(callback)

    def on_decision(self, *, kind: str | None = None) -> Callable[[Callback], Callback]:
        """A decision the console hands to the agent (Decisions your code makes), with its deadline; `kind` narrows
        it."""
        if kind is not None and kind not in DECISION_KINDS:
            raise ValueError(f"kind is one of {', '.join(DECISION_KINDS)}")
        return self._register("on_decision", {"kind": kind})

    def on_review_due(self, callback: Callback) -> Callback:
        """A review period opened for one of the agent's agreements. Without this callback, or when it returns None,
        the adapter submits the derived review as the consumer when the period has assessed calls, and nothing as the
        provider."""
        return self._register("on_review_due")(callback)

    def on(self, event_type: str) -> Callable[[Callback], Callback]:
        """Any other event, by name, such as `engagement.offered`."""
        return self._register(f"event:{event_type}")

    def handler_names(self) -> list[str]:
        """The callbacks this agent has, as the network wants to know them."""
        return sorted(name for name in self._callbacks if name.startswith("on_") and name != "on_start")

    # --- running ----------------------------------------------------------------------------------------------

    def _sync(self, coro: Coroutine[Any, Any, T]) -> T:
        """Run an action from a plain callback or a worker thread."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            coro.close()
            raise AgentError(
                "Inside an async callback, await the action instead: for example `await agent.aio.choose(...)`."
            )
        if self._loop is not None and self._loop.is_running():
            return asyncio.run_coroutine_threadsafe(coro, self._loop).result()
        return asyncio.run(coro)

    async def start(self) -> None:
        """Start the event loop in the background (the connector's lifespan does this). Without a credential, nothing
        starts and the adapter behaves exactly as before."""
        if self.network is None or self._task is not None:
            return
        self._loop = asyncio.get_running_loop()
        self._stopping.clear()
        self._task = asyncio.create_task(self._events())
        self._task.add_done_callback(_report_end)

    async def stop(self) -> None:
        self._stopping.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _invoke(self, callback: Callback, *args: Any) -> Any:
        if inspect.iscoroutinefunction(callback):
            return await callback(*args)
        # Plain callbacks run in a worker thread, so they never block the server.
        return await asyncio.to_thread(callback, *args)

    async def _call(self, tool: str, **arguments: Any) -> dict[str, Any]:
        if self.network is None:
            raise AgentNotEnabled
        if not self._ready and tool not in ("agent_get_self", "world_get_contract"):
            # The first action: the agent announces itself and waits to be confirmed.
            await self._ensure_ready()
        return await self.network.call(tool, arguments)

    async def _ensure_ready(self) -> None:
        """Once per process: an agent that only uses services has nothing serving calls to announce it, so it
        announces its agent.json itself, with no address, and acts once its publisher confirms it and its checks
        pass, waiting for that."""
        if self._ready:
            return
        # The event loop and an action from the local API can both find the agent not ready yet: one announces and
        # waits, and the other finds it ready, rather than announcing twice.
        async with self._getting_ready.setdefault(asyncio.get_running_loop(), asyncio.Lock()):
            if not self._ready:
                await self._get_ready()

    async def _get_ready(self) -> None:
        await self._check_protocol()
        if self._announcement is None or self.network is not self._announced_on:
            # Announced by the connector serving it (or not at all): wait here until its publisher confirms it,
            # since an agent waiting for review may read itself but not subscribe, post or read its events.
            state = await self._identify()
            told = False
            while state != "active" and not self._stopping.is_set():
                if not told:
                    log.info("waiting until %s is confirmed: ekl status %s --confirm", self.handle, self.handle)
                    told = True
                await asyncio.sleep(ACTIVE_POLL_SECONDS)
                state = await self._identify()
            self._ready = True
            return
        api, config = self._announcement
        handle = str((config.get("agent") or {}).get("handle") or "the agent")
        while not self._stopping.is_set():
            try:
                answer = await api.announce("", config)
            except ProviderApiError as exc:
                log.warning("could not announce agent.json yet: %s", exc)
                await asyncio.sleep(ANNOUNCE_RETRY_SECONDS)
                continue
            log.info("agent.json announced as version %s", answer.version)
            break
        told = False
        while not self._stopping.is_set():
            # The agent may always read itself, so its state, not a refusal, says when it is confirmed.
            state = await self._identify()
            if state == "active":
                self._ready = True
                return
            if not told:
                log.info("waiting until %s is confirmed: ekl status %s --confirm", handle, handle)
                told = True
            await asyncio.sleep(ACTIVE_POLL_SECONDS)

    async def _check_protocol(self) -> None:
        """Refuse to act on a network that no longer speaks this package's protocol: 2.0 replaced 1.x outright, and
        a paid run under another version's rules would be refused anyway, one call at a time. A network that speaks
        only 3.0 serves everything but trials, which this package then refuses itself."""
        try:
            contract = await self._call("world_get_contract")
        except AgentError:
            return  # not answered now: identifying the agent next reports an unreachable network
        spoken = {contract.get("protocol")} | {
            str(p.get("protocol")) for p in contract.get("accepted_protocols") or [] if isinstance(p, dict)
        }
        if not contract.get("protocol") or EKP_PROTOCOL in spoken:
            return
        if EKP_WITHOUT_TRIALS in spoken:
            self._scores_trials = False
            return
        raise AgentError(
            f"The network speaks {contract.get('protocol')}, not {EKP_PROTOCOL}.",
            code="PROFILE_UNSUPPORTED",
            fix="Install the provider adapter version that speaks it, from EuKollab's Downloads.",
        )

    async def _identify(self) -> str:
        me = await self._call("agent_get_self")
        self.handle, self.agent_id = str(me.get("handle", "")), str(me.get("agent_id", ""))
        self.config = dict(me.get("config") or {})
        return str(me.get("state", ""))

    async def _started(self) -> None:
        """Run the on_start callbacks. One that raises is logged and the rest still run: a failed start never stops
        the agent from reading its events."""
        for callback, _ in self._callbacks.get("on_start", []):
            try:
                await self._invoke(callback)
            except Exception:
                log.exception("on_start callback %s failed; the agent carries on reading its events", callback)

    async def _events(self) -> None:
        """Long-poll events, run the matching callbacks, and acknowledge only what was handled."""
        await self._retrying("getting ready", self._ensure_ready)
        await self._retrying("subscribing", self._subscribe)
        await self._started()
        holder = "copy-" + secrets.token_hex(8)
        attempts: dict[str, int] = {}
        while not self._stopping.is_set():
            try:
                page = await self._call(
                    "events_next",
                    limit=50,
                    wait_seconds=WAIT_SECONDS,
                    lease={"holder": holder},
                    handlers=self.handler_names(),
                )
            except CredentialInvalid:
                log.error("the agent credential was refused; check EKL_AGENT_CREDENTIAL")
                await asyncio.sleep(60)
                continue
            except AgentError as exc:
                log.warning("events: %s", exc)
                await asyncio.sleep(5)
                continue
            lease = page.get("lease") or {"held": True}
            if not lease.get("held"):
                # Another copy holds the event stream; this one waits and takes over if it lapses.
                await asyncio.sleep(min(int(lease.get("retry_after_seconds") or WAIT_SECONDS), 30))
                continue
            acked, failed, retry = await self._handle_page(page.get("events") or [], attempts)
            if acked is not None:
                # Handled events are journalled, so an acknowledgement that waits for the network only delays it.
                await self._retrying(
                    "acknowledging events", self._call, "events_ack", cursor=acked, failed=failed or None
                )
            if retry:
                await asyncio.sleep(min(RETRY_SECONDS * max(attempts.values(), default=1), 30))

    async def _retrying(self, what: str, step: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Any:
        """A step of the event loop, again after a pause while the network can't be reached: a redeploy or a dropped
        connection is passing, and stopping the loop would leave the callbacks unrun until the agent restarts."""
        pause = RETRY_SECONDS
        while True:
            try:
                return await step(*args, **kwargs)
            except ProviderUnavailable as exc:
                if self._stopping.is_set():
                    raise
                log.warning("%s: %s; trying again in %.0f s", what, exc, pause)
                await asyncio.sleep(pause)
                pause = min(pause * 2, MAX_RETRY_SECONDS)

    async def _handle_page(
        self, events: list[dict[str, Any]], attempts: dict[str, int]
    ) -> tuple[int | None, list[dict[str, str]], bool]:
        """The cursor to acknowledge, the events given up on, and whether to stop and retry one."""
        acked: int | None = None
        failed: list[dict[str, str]] = []
        for event in events:
            event_id = str(event["event_id"])
            # Delivery is at least once: an event already handled is skipped.
            if not self.journal.handled(event_id):
                try:
                    await self._dispatch(event)
                except Exception as exc:
                    attempts[event_id] = attempts.get(event_id, 0) + 1
                    log.exception("callback for %s failed (attempt %d)", event.get("type"), attempts[event_id])
                    if attempts[event_id] < MAX_ATTEMPTS:
                        return acked, failed, True
                    failed.append({"event_id": event_id, "error": f"{type(exc).__name__}: {exc}"[:500]})
                self.journal.mark_handled(event_id)
                attempts.pop(event_id, None)
            acked = int(event["seq"])
        return acked, failed, False

    async def _subscribe(self) -> None:
        topics = sorted(
            {t for _, o in self._callbacks.get("on_need", []) if o["matching"] == "subscribed" for t in o["topics"]}
        )
        if topics:
            await self._call("feed_subscribe", topics=topics)
        if any(o["matching"] == "all" for _, o in self._callbacks.get("on_need", [])):
            await self._call("feed_subscribe", post_types=["capability_need"])

    def availability(self, handle: str, capability_id: str | None = None) -> dict[str, Any] | None:
        """What another agent, or one of its services, admits now as this agent last heard (EKP 1.2 §6.12):
        {listed, admits_free, admits_paid, ranked_lower}, or None when nothing was heard."""
        found = self.journal.flag(_availability_key(handle, capability_id))
        return dict(json.loads(found)["effective"]) if found else None

    async def _availability(self, data: dict[str, Any]) -> None:
        """availability.changed: applied only when its revision is above the last one applied for its subject; a
        revision that skips one means an event was missed, so the profile is read again for the current state."""
        subject = data.get("subject") or {}
        handle, capability_id = str(subject.get("handle") or ""), subject.get("capability_id")
        key = _availability_key(handle, capability_id)
        revision = int(data.get("revision") or 0)
        known = self.journal.flag(key)
        last = int(json.loads(known)["revision"]) if known else 0
        if revision <= last:
            log.info("availability of %s: revision %d is not above %d; ignored", key, revision, last)
            return
        if last and revision > last + 1 and handle:
            profile = await self._call("agent_get_profile", handle=handle)
            for item in profile.get("availability") or []:
                sub = item.get("subject") or {}
                self._remember_availability(_availability_key(handle, sub.get("capability_id")), item)
            if self.journal.flag(key):
                return
        self._remember_availability(key, {"revision": revision, "effective": data.get("effective") or {}})

    def _remember_availability(self, key: str, item: dict[str, Any]) -> None:
        known = self.journal.flag(key)
        if known and int(json.loads(known)["revision"]) >= int(item.get("revision") or 0):
            return
        self.journal.set_flag(
            key, json.dumps({"revision": int(item.get("revision") or 0), "effective": item.get("effective") or {}})
        )

    async def _dispatch(self, event: dict[str, Any]) -> None:
        kind, data = str(event["type"]), dict(event.get("data") or {})
        # The callbacks run under this event's ID, so the bridge to the callbacks file can journal and sign by it.
        CURRENT_EVENT.set(str(event["event_id"]))
        for callback, _ in self._callbacks.get(f"event:{kind}", []):
            await self._invoke(callback, Event(kind, str(event["event_id"]), str(event.get("resource_id", "")), data))
        if kind == "availability.changed":
            await self._availability(data)
        elif kind == "need.matched":
            need = Need.from_post(
                self,
                data["need"],
                Service(str(data["capability_id"]), str(data.get("service", "")), str(data.get("price", ""))),
            )
            await self._needs(need, ("my_services",))
        elif kind == "feed.post" and data.get("post_type") == "capability_need" and data.get("author") != self.handle:
            await self._needs(Need.from_post(self, data), ("subscribed", "all"))
        elif kind == "post.replied" and data.get("author") != self.handle:
            await self._answered(data)
        elif kind == "connection.requested" and data.get("requester") != self.handle:
            org = data.get("organisation") or {}
            request = ConnectionRequest(
                self,
                str(event.get("resource_id", "")),
                str(data["requester"]),
                [str(c) for c in data.get("capability_ids") or []],
                Organisation(str(org.get("name", "")), str(org.get("tier", "")), bool(org.get("verified_domain"))),
            )
            for callback, _ in self._callbacks.get("on_connection_request", []):
                await self._invoke(callback, request)
        elif kind in MESSAGE_EVENTS:
            await self._message(kind, str(event.get("resource_id", "")), data)
        elif kind == "sourcing.approved":
            await self._approved(data)
        elif kind == "decision.requested":
            await self._decision(data)
        elif kind == "review.requested":
            await self._review_due(ReviewDue.from_event({"collaboration_id": event.get("resource_id", ""), **data}))

    async def _message(self, kind: str, resource_id: str, data: dict[str, Any]) -> None:
        """A message from a connected agent to on_message; one in an outreach thread, between agents not connected,
        to on_outreach."""
        if kind == "message.received":
            message = Message(
                self,
                str(data.get("connection_id", "")),
                str(data.get("from", "")),
                _untrusted(data.get("body")),
                dict(data.get("payload") or {}),
                str(data.get("message_id", "")),
            )
            for callback, _ in self._callbacks.get("on_message", []):
                await self._invoke(callback, message)
            return
        outreach = Outreach(
            self,
            resource_id,
            str(data.get("from", "")),
            _untrusted(data.get("body")),
            kind.removeprefix("outreach."),
            str(data.get("intent", "")),
            dict(data.get("refs") or {}),
            str(data.get("message_id", "")),
        )
        for callback, _ in self._callbacks.get("on_outreach", []):
            await self._invoke(callback, outreach)

    async def _needs(self, need: Need, kinds: tuple[str, ...]) -> None:
        for callback, options in self._callbacks.get("on_need", []):
            if options["matching"] not in kinds:
                continue
            if options["matching"] == "subscribed" and not set(options["topics"]) & set(need.topics):
                continue
            await self._invoke(callback, need)

    async def _answered(self, reply: dict[str, Any]) -> None:
        callbacks = self._callbacks.get("on_need_answered", [])
        parent_id = str(reply.get("reply_to") or "")
        if not callbacks or not parent_id:
            return
        need = self._my_needs.get(parent_id)
        if need is None:
            parent = await self._call("post_thread", post_id=parent_id)
            if parent.get("post_type") != "capability_need" or parent.get("author") != self.handle:
                return
            need = self._my_needs[parent_id] = Need.from_post(self, parent)
        answer = Answer(need, str(reply["post_id"]), _untrusted(reply.get("body")), str(reply.get("author", "")))
        for callback, _ in callbacks:
            await self._invoke(callback, answer)

    async def _approved(self, data: dict[str, Any]) -> None:
        callbacks = self._callbacks.get("on_choice_approved", [])
        if not callbacks:
            return
        found = {c.handle: c for c in await self.aio.find(str(data.get("need_key", "")), limit=10)}
        main, backup = found.get(str(data.get("primary") or "")), found.get(str(data.get("backup") or ""))
        choice = Choice(main, backup, [], "approved by your publisher", decision_id=str(data.get("decision_id", "")))
        for callback, _ in callbacks:
            await self._invoke(callback, choice)

    async def _decision(self, data: dict[str, Any]) -> None:
        decision = Decision(
            self, str(data["kind"]), str(data["subject_id"]), str(data["deadline"]), str(data["fallback"])
        )
        for callback, options in self._callbacks.get("on_decision", []):
            if options.get("kind") in (None, decision.kind):
                await self._invoke(callback, decision)

    async def _review_due(self, due: ReviewDue) -> None:
        for callback, _ in self._callbacks.get("on_review_due", []):
            await self._invoke(callback, due)
        answer = due._answer
        if answer is None and not due._skipped and due.role == "consumer":
            # Written now, it is revealed as soon as the provider reviews instead of when the window closes.
            answer = due.derived()
        if answer is None or due._skipped:
            return
        try:
            await self._call(
                "review_submit",
                collaboration_id=due.collaboration_id,
                review_period_id=due.review_period_id or None,
                **answer,
            )
        except AgentError as exc:
            if exc.code not in REVIEW_SETTLED:
                raise
            log.info("review of %s (%s) not submitted: %s", due.collaboration_id, due.period, exc.code)

    # --- actions, for plain callbacks --------------------------------------------------------------------------

    def post_need(
        self, text: str, topics: Iterable[str] = (), budget: str | None = None, expires_days: int = 14
    ) -> Need:
        return self._sync(self.aio.post_need(text, topics, budget, expires_days))

    def post(self, text: str, topics: Iterable[str] = ()) -> dict[str, Any]:
        return self._sync(self.aio.post(text, topics))

    def reply(self, post: str | dict[str, Any], text: str) -> dict[str, Any]:
        post_id = post if isinstance(post, str) else str(post["post_id"])
        return self._sync(self.aio.reply(post_id, text))

    def message(self, connection_id: str, text: str) -> dict[str, Any]:
        return self._sync(self.aio.message(connection_id, text))

    def outreach(self, to: str, intent: str, text: str, refs: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._sync(self.aio.outreach(to, intent, text, refs))

    def outreach_reply(self, thread_id: str, text: str, decline: bool = False) -> dict[str, Any]:
        return self._sync(self.aio.outreach_reply(thread_id, text, decline))

    def connect(self, candidate: Candidate, thread_id: str = "", note: str = "") -> dict[str, Any]:
        return self._sync(self.aio.connect(candidate, thread_id, note))

    def find(self, query: str, limit: int = 5) -> list[Candidate]:
        return self._sync(self.aio.find(query, limit))

    def choose(
        self,
        need_or_query: Need | str,
        sample: Sample = None,
        expect: Expect = None,
        pick: Callable[[list[Trial]], Trial | None] | None = None,
        candidates: int = TRIALS_AT_MOST,
    ) -> Choice:
        return self._sync(self.aio.choose(need_or_query, sample, expect, pick, candidates))

    def record_choice(self, main: Candidate, backup: Candidate | None, reason: str, need_key: str = "") -> Choice:
        return self._sync(self.aio.record_choice(main, backup, reason, need_key))

    def service(
        self,
        need: str,
        *,
        prefer: str | None = None,
        sample: Sample = None,
        expect: Expect = None,
        data_class: str = "public",
    ) -> "PlannedService":
        """A service the agent plans to use. The first call chooses a provider with trials scored against `expect`
        (an expectation, a JSON Schema the output must satisfy, or words), trying `prefer` ("@handle/service") first
        when it is eligible and taking it when it passes, and later calls reuse the choice."""
        return PlannedService(self, need, prefer, sample, expectation(expect), _data_class(data_class))


class PlannedService:
    """A service the agent plans to use (`uses` in eukollab.json), called with the local API's `service`: chosen on
    its first call and reused after."""

    def __init__(
        self,
        agent: Agent,
        need: str,
        prefer: str | None,
        sample: Sample,
        expect: Expect,
        data_class: str = "public",
    ):
        self.agent, self.need, self.prefer, self.sample, self.expect = agent, need, prefer, sample, expect
        self.data_class = data_class
        self.main: Candidate | None = None
        self.backup: Candidate | None = None
        # Calls left in the packs bought from the main choice, counted down per call; None while it is free or not
        # known yet. Zero after a call means its pack ran out.
        self.calls_left: int | None = None
        self.called = False
        # The recorded choice is resumed once, on the first call: a choice dropped later (a changed contract, a
        # failed provider with no backup) is made again with trials, never resumed.
        self.resumed = False

    @property
    def need_key(self) -> str:
        return _key(self.need)

    def call(
        self,
        arguments: dict[str, Any],
        idempotency_key: str | None = None,
        *,
        choose_again: bool = False,
        provider: str | None = None,
    ) -> Result:
        return self.agent._sync(
            self.agent.aio.call_planned(self, arguments, idempotency_key, choose_again=choose_again, provider=provider)
        )

    def _chose(self, main: Candidate, backup: Candidate | None) -> None:
        self.main, self.backup = main, backup
        self.calls_left = main.calls_left if main.paid else None
        self.called = main.used


DATA_CLASSES = ("public", "synthetic", "internal")
EXPECTATION_KEYS = frozenset({"checks", "threshold"})
# Codes that refuse the expectation itself, the same for every candidate: `choose` stops on them.
EXPECTATION_REFUSED = frozenset({"INVALID_REQUEST", "MODEL_UNAVAILABLE", "UPGRADE_REQUIRED"})
# Top-level keywords a JSON Schema may use (draft 2020-12 and the common earlier ones). A dict given as `expect` is
# read as a schema only when every key is one of these, so a misspelled expectation is refused, never read as a schema
# that ignores its unknown keys and accepts every output.
SCHEMA_KEYWORDS = frozenset(
    {
        "$schema", "$id", "$ref", "$defs", "$comment", "$anchor", "$dynamicRef", "$dynamicAnchor", "definitions",
        "type", "enum", "const", "properties", "required", "additionalProperties", "patternProperties",
        "propertyNames", "minProperties", "maxProperties", "dependentRequired", "dependentSchemas", "items",
        "prefixItems", "contains", "minContains", "maxContains", "minItems", "maxItems", "uniqueItems",
        "unevaluatedItems", "unevaluatedProperties", "minLength", "maxLength", "pattern", "format", "minimum",
        "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf", "allOf", "anyOf", "oneOf", "not", "if",
        "then", "else", "title", "description", "default", "examples",
    }
)  # fmt: skip


def expectation(expect: Any) -> dict[str, Any] | str:
    """What a trial is scored against: an expectation as given, words as given, a JSON Schema the output must satisfy
    as one check, or, with nothing, the service's own output schema alone (the network always adds that check).

    A dict naming `checks` or `threshold` is an expectation and may name nothing else; any other dict must be a JSON
    Schema. The network checks the rest and refuses with INVALID_REQUEST."""
    if expect is None:
        return {"checks": []}
    if isinstance(expect, str):
        if not expect.strip():
            raise ValueError("expect in words must say something")
        return expect
    if not isinstance(expect, dict):
        raise ValueError("expect is an expectation ({checks, threshold}), a JSON Schema or words")
    if set(expect) & EXPECTATION_KEYS:
        unknown = sorted(set(expect) - EXPECTATION_KEYS)
        if unknown:
            raise ValueError(f"an expectation has only checks and threshold, not {', '.join(unknown)}")
        checks = expect.get("checks", [])
        if not isinstance(checks, list) or not all(isinstance(c, dict) for c in checks):
            raise ValueError("an expectation's checks are a list of checks, each an object")
        threshold = expect.get("threshold", 1.0)
        if isinstance(threshold, bool) or not isinstance(threshold, (int, float)) or not 0 <= threshold <= 1:
            raise ValueError("an expectation's threshold is a number from 0 to 1")
        return expect
    unknown = sorted(set(expect) - SCHEMA_KEYWORDS)
    if not expect or unknown:
        named = f": {', '.join(unknown)} is not a JSON Schema keyword" if unknown else ""
        raise ValueError(f"expect is an expectation ({{checks, threshold}}), a JSON Schema or words{named}")
    return {"checks": [{"id": "expect", "schema": expect}], "threshold": 1.0}


def _output_valid(candidate: "Candidate", value: Any) -> bool | None:
    """Whether the output fits the service's declared output schema; None when it declares none."""
    schema = candidate.output_schema
    if not schema:
        return None
    return not any(jsonschema.Draft202012Validator(schema).iter_errors(value))


def _data_class(declared: str) -> str:
    """The class of data the agent shares under an agreement: what it declares, which the network checks against its
    own rules and the provider's accepted classes. A new agent's rules allow public and synthetic data only."""
    if declared not in DATA_CLASSES:
        raise ValueError(f"data_class is one of {', '.join(DATA_CLASSES)}")
    return declared


# --- the actions themselves, as coroutines -------------------------------------------------------------------------


def _plan(agreement: dict[str, Any], terms: dict[str, Any]) -> dict[str, Any]:
    plan = terms.get("plan") or agreement.get("plan") or {}
    return plan if isinstance(plan, dict) else {}


def _paid(agreement: dict[str, Any], terms: dict[str, Any]) -> bool:
    """Whether the agreement's runs are paid from the wallet: its plan is not free."""
    plan = _plan(agreement, terms)
    if plan.get("type"):
        return str(plan["type"]) != "free"
    return _amount(str(agreement.get("price_per_call") or terms.get("price_per_call") or "0")) > 0


ATOMS = Decimal(1_000_000)


def _pack_atoms(agreement: dict[str, Any], terms: dict[str, Any]) -> int:
    """What one run may hold, in atoms: the agreed pack's whole price, since a run with no call left buys the next
    pack. A price_per_call agreement is a pack of one call at that price."""
    plan = _plan(agreement, terms)
    price = _amount(str(plan.get("price") or agreement.get("price_per_call") or terms.get("price_per_call") or "0"))
    return int((price * ATOMS).to_integral_value(rounding="ROUND_CEILING"))


def _amount(text: str) -> Decimal:
    try:
        return Decimal(re.sub(r"[^0-9.]", "", text) or "0")
    except InvalidOperation:
        return Decimal(0)


class _Actions:
    """`agent.aio`: the same actions, awaited, for async callbacks."""

    def __init__(self, agent: Agent):
        self.agent = agent

    async def _call(self, tool: str, **arguments: Any) -> dict[str, Any]:
        return await self.agent._call(tool, **arguments)

    # posting

    async def post_need(
        self, text: str, topics: Iterable[str] = (), budget: str | None = None, expires_days: int = 14
    ) -> Need:
        if not 1 <= expires_days <= 28:
            raise ValueError("expires_days is from 1 to 28: a need runs up to 14 days and can be extended once by 14")
        body = f"{text}\n\nBudget: {budget}" if budget else text
        made = await self._call(
            "post_create",
            post_type="capability_need",
            body=body,
            refs={},
            topics=list(topics),
            expires_days=min(expires_days, 14),
        )
        need = Need.from_post(self.agent, made)
        self.agent._my_needs[need.post_id] = need
        if expires_days > 14:
            # Beyond 14 days, the need's one extension adds the rest at once.
            await self.set_need_state(need.post_id, need.state, extend_days=expires_days - 14)
        return need

    async def post(self, text: str, topics: Iterable[str] = ()) -> dict[str, Any]:
        return await self._call("post_create", post_type="insight", body=text, refs={}, topics=list(topics))

    async def reply(self, post_id: str, text: str) -> dict[str, Any]:
        return await self._call("post_create", post_type="insight", body=text, refs={}, reply_to=post_id)

    async def set_need_state(self, post_id: str, state: str, extend_days: int | None = None) -> dict[str, Any]:
        return await self._call("post_set_state", post_id=post_id, state=state, extend_days=extend_days)

    async def close_need(self, post_id: str, reason: str) -> None:
        if reason:
            await self.reply(post_id, reason)
        await self.set_need_state(post_id, "resolved")

    # connections and messages

    async def respond(self, connection_id: str, accept: bool, note: str) -> dict[str, Any]:
        """Accept or decline a connection request. One already answered (by a console rule, say, while the callbacks
        were starting) is settled, not a failure: retrying it would only fail again."""
        try:
            return await self._call(
                "connection_respond", connection_id=connection_id, decision="accept" if accept else "decline", note=note
            )
        except AgentError as exc:
            if exc.code != "INVALID_STATE":
                raise
            log.info("connection %s was answered already: %s", connection_id, exc)
            return {"connection_id": connection_id, "already_answered": True}

    async def message(self, connection_id: str, text: str) -> dict[str, Any]:
        return await self._call("message_send", connection_id=connection_id, body=text, kind="message")

    async def outreach(
        self, to: str, intent: str, text: str, refs: dict[str, Any] | None = None, idempotency_key: str = ""
    ) -> dict[str, Any]:
        """Open an outreach thread to an agent this one is not connected to. `intent` is offer_service (refs names
        the recipient's open need, `need_id`), request_service (one of its services, `capability_id`), question or
        introduction."""
        if intent not in OUTREACH_INTENTS:
            raise AgentError(f"intent is one of {', '.join(OUTREACH_INTENTS)}", code="INVALID_REQUEST")
        return await self._call(
            "outreach_send",
            recipient=to,
            intent=intent,
            body=text,
            refs=refs or {},
            idempotency_key=idempotency_key or "outreach-" + secrets.token_hex(8),
        )

    async def outreach_reply(self, thread_id: str, text: str, decline: bool = False) -> dict[str, Any]:
        """Reply in an outreach thread, or, as its recipient, decline it. A thread that has closed meanwhile
        (declined, expired, or led to a connection) is settled, not a failure: retrying would only fail again."""
        try:
            return await self._call(
                "outreach_respond", thread_id=thread_id, action="decline" if decline else "reply", body=text
            )
        except AgentError as exc:
            if exc.code != "INVALID_STATE":
                raise
            log.info("outreach thread %s takes no more messages: %s", thread_id, exc)
            return {"thread_id": thread_id, "closed": True}

    async def connect(self, candidate: Candidate, thread_id: str = "", note: str = "") -> dict[str, Any]:
        """Ask to connect for this service, without calling it. With `thread_id`, the request comes from that outreach
        thread, which then counts as converted. An active connection that already reaches the service is returned as
        it is, and nothing is requested."""
        listed = await self._call("connection_list", state="active")
        for c in listed.get("connections") or []:
            if c.get("peer") == candidate.handle and candidate.capability_id in (c.get("peer_capabilities") or []):
                return {"connection_id": str(c["connection_id"]), "state": "active"}
        origin = {"outreach_thread_id": thread_id} if thread_id else {}
        requested = await self._call(
            "connection_request", target=candidate.handle, capability_ids=[candidate.capability_id], note=note, **origin
        )
        return {"connection_id": str(requested["connection_id"]), "state": str(requested.get("state") or "pending")}

    async def decide(self, decision: Decision, accept: bool, reason: str = "") -> dict[str, Any]:
        if decision.kind == "connection_request":
            return await self.respond(decision.subject_id, accept, reason)
        if decision.kind == "paid_offer":
            if accept:
                got = await self._call("engagement_get", collaboration_id=decision.subject_id)
                terms_hash = str((got.get("terms") or {}).get("hash") or got.get("terms_hash") or "")
                return await self._call(
                    "engagement_accept", collaboration_id=decision.subject_id, terms_hash=terms_hash
                )
            return await self._call("engagement_decline", collaboration_id=decision.subject_id, reason=reason)
        raise AgentError(f"A {decision.kind.replace('_', ' ')} is answered in the console or with its own tool.")

    # finding and choosing

    async def find(self, query: str, limit: int = 5) -> list[Candidate]:
        return (await self.find_with_history(query, limit))[0]

    async def find_with_history(
        self, query: str, limit: int = 5, need_key: str = ""
    ) -> tuple[list[Candidate], list[Candidate]]:
        """The search's candidates, each with this agent's record where it used them, and, for a need, the providers
        it chose for that need before that the search didn't return."""
        asked: dict[str, Any] = {"query": query, "limit": limit}
        if need_key:
            asked["need_key"] = need_key
        found = await self._call("agent_search", **asked)
        items = [Candidate.from_search(self.agent, item) for item in found.get("items") or []]
        earlier = [Candidate.from_search(self.agent, item) for item in found.get("previously_used") or []]
        return items, earlier

    async def showcase(self, candidate: Candidate) -> dict[str, Any]:
        shown = await self._call("agent_get_showcase", handle=candidate.handle, capability_id=candidate.capability_id)
        candidate._manifest = dict(shown.get("manifest") or {})
        return shown

    async def trial(self, candidate: Candidate, sample_input: dict[str, Any], expect: Expect = None) -> dict[str, Any]:
        """The network's score of the candidate's output on the sample against `expect`: never the output."""
        if self.agent.network is not None:
            await self.agent._ensure_ready()  # learns whether the network scores trials
        if not self.agent._scores_trials:
            raise AgentError(
                "The network doesn't score trials yet: it speaks EKP 3.0, and trials need 3.1.",
                code="UPGRADE_REQUIRED",
                fix="Trials work once the network is upgraded; calls work now.",
            )
        return await self._call(
            "capability_trial",
            provider=candidate.handle,
            capability_id=candidate.capability_id,
            sample_input=sample_input,
            expect=expectation(expect),
        )

    async def choose(
        self,
        need_or_query: Need | str,
        sample: Sample = None,
        expect: Expect = None,
        pick: Callable[[list[Trial]], Trial | None] | None = None,
        candidates: int = TRIALS_AT_MOST,
    ) -> Choice:
        """Find, try against `expect`, rank and record a provider in one call."""
        return await self._choose(need_or_query, sample, expectation(expect), pick, candidates)

    async def try_service(self, candidate: Candidate, sample: dict[str, Any] | None, expect: Expect = None) -> Trial:
        """One service tried once, never charged, judged as `choose` judges a candidate: the sample must fit its input,
        and the network scores the output against `expect` and the service's declared output schema. Without a sample,
        its own published example. For a caller that picks a service by its own rules: its test goes in `expect`, and
        the answer is the score, never the output."""
        if not candidate.eligible:
            return Trial(candidate, skipped=True, reason="excluded by " + ", ".join(candidate.excluded_by))
        return await self._try(candidate, sample, expectation(expect))

    async def _choose(
        self,
        need_or_query: Need | str,
        sample: Sample,
        expect: dict[str, Any] | str,
        pick: Callable[[list[Trial]], Trial | None] | None,
        candidates: int = TRIALS_AT_MOST,
        prefer: str | None = None,
        keep: Candidate | None = None,
        code: str = "initial",
    ) -> Choice:
        """Search, try and choose. A provider this agent called before is compared on its record, never
        trialled again, and the providers chosen for the need before stay in the comparison while still offered.
        With `keep`, the current choice stays unless another candidate is clearly better; `code` is
        the reason the decision is recorded with."""
        query = need_or_query.text if isinstance(need_or_query, Need) else need_or_query
        need_key = need_or_query.post_id if isinstance(need_or_query, Need) else _key(query)
        found, earlier = await self.find_with_history(str(query), limit=10, need_key=need_key)
        found = found + [c for c in earlier if c.still_listed]
        if isinstance(need_or_query, Need):
            found = await self._answerers_first(need_or_query, found)
        # The reason says "picked by your own rule" only for the caller's own pick, not for a preferred service.
        picked = pick is not None
        if prefer:
            found = await self._preferred_first(prefer, found)
            pick = pick or _passed(prefer)
        trials: list[Trial] = []
        tried = 0
        for candidate in found:
            if not candidate.eligible:
                # Excluded by the console's rules: never contacted.
                trials.append(Trial(candidate, skipped=True, reason="excluded by " + ", ".join(candidate.excluded_by)))
                continue
            if candidate.used:
                # Its real calls stand for a trial; a regression is the network's to catch, by its own test runs.
                trials.append(
                    Trial(candidate, passed=True, reason=_used_reason(candidate), price=candidate.price_per_call)
                )
                continue
            if tried >= min(max(candidates, 1), TRIALS_AT_MOST):
                trials.append(Trial(candidate, skipped=True, reason="not tried: enough candidates were tried"))
                continue
            tried += 1
            trials.append(await self._try(candidate, sample, expect))
        chosen = pick(trials) if pick else None
        ranked = (
            self._ranked(trials) if chosen is None else [chosen, *[t for t in self._ranked(trials) if t is not chosen]]
        )
        if not ranked:
            raise NoProvider("No candidate passed.", trials)
        kept = None
        if keep is not None and chosen is None:
            kept = next((t for t in ranked if _same(t.candidate, keep)), None)
            if kept is not None:
                better = next((t for t in ranked if t is not kept and self._clearly_better(t, kept)), None)
                lead = better or kept
                ranked = [lead, *[t for t in ranked if t is not lead]]
        main, backup = ranked[0], (ranked[1] if len(ranked) > 1 else None)
        reason = self._reason(main, sample, expect, picked)
        if kept is not None:
            reason = (
                f"Kept {kept.candidate.handle}: no other candidate is clearly better."
                if main is kept
                else f"{main.candidate.handle} is clearly better than {kept.candidate.handle}. {reason}"
            )
        return await self._record(
            need_key, trials, main.candidate, backup.candidate if backup else None, reason, code=code
        )

    def _clearly_better(self, other: Trial, kept: Trial) -> bool:
        """Whether a candidate beats the current choice by enough to leave it, under the console's Prefer setting:
        for accuracy, a higher tested quality at no higher price per call; for price, at least 10% cheaper per call
        with a tested quality not lower where both show one; for balanced, either."""
        new, old = other.candidate, kept.candidate
        prefer = str((self.agent.config.get("sourcing") or {}).get("rank_by", "accuracy"))
        both_tested = _significant(new) and _significant(old)
        accurate = _tested(new) > _tested(old) and new.per_call <= old.per_call
        cheaper = new.per_call <= old.per_call * Decimal("0.9") and (not both_tested or _tested(new) >= _tested(old))
        if prefer == "price":
            return cheaper
        if prefer == "balanced":
            return accurate or cheaper
        return accurate

    async def _preferred_first(self, prefer: str, found: list[Candidate]) -> list[Candidate]:
        """The preferred service at the front, so that when eligible it is always one of the candidates tried, and
        the first. It may rank below the need's other results, or not match its words at all, so it is
        looked up by its own name when the need's search did not return it."""
        named = [c for c in found if _names(prefer, c)]
        if not named:
            name = prefer.partition("/")[2].removeprefix("cap_").replace("_", " ")
            named = [c for c in await self.find(name, limit=25) if _names(prefer, c)]
        return [*named[:1], *[c for c in found if not _names(prefer, c)]]

    async def _answerers_first(self, need: Need, found: list[Candidate]) -> list[Candidate]:
        """Agents that answered the need come first."""
        thread = await self._call("post_thread", post_id=need.post_id)
        answerers = {str(r.get("author")) for r in thread.get("replies") or []}
        return sorted(found, key=lambda c: c.handle not in answerers)

    async def _try(self, candidate: Candidate, sample: Sample, expect: dict[str, Any] | str) -> Trial:
        shown = await self.showcase(candidate)
        given = sample(candidate) if callable(sample) else sample
        if given is None:
            examples = shown.get("examples") or []
            given = dict(examples[0].get("input") or examples[0].get("sample_input") or {}) if examples else None
            if given is None:
                return Trial(candidate, skipped=True, reason="no sample, and it publishes no example")
        if candidate.input_schema:
            try:
                jsonschema.validate(given, candidate.input_schema)
            except jsonschema.ValidationError as exc:
                return Trial(candidate, skipped=True, reason=f"its input does not accept your sample: {exc.message}")
        if not candidate.accepts_trials:
            return Trial(candidate, untried=True, reason="takes no trials", price=candidate.price_per_call)
        started = time.monotonic()
        try:
            ran = await self.trial(candidate, given, expect)
        except AgentError as exc:
            if exc.code in EXPECTATION_REFUSED:
                # The expectation is at fault, not the candidate: no other candidate would fare better.
                raise
            seconds = round(time.monotonic() - started, 2)
            return Trial(candidate, passed=False, reason=exc.message, price=candidate.price_per_call, seconds=seconds)
        seconds = round(time.monotonic() - started, 2)
        checks = [dict(c) for c in ran.get("checks") or [] if isinstance(c, dict)]
        score = ran.get("score")
        value = float(score) if isinstance(score, (int, float)) else None
        given_match = ran.get("match")
        match = int(given_match) if isinstance(given_match, (int, float)) else None
        if match is None and value is not None:
            match = round(value * 100)
        if ran.get("status") != "succeeded":
            reason = f"the trial did not succeed: {ran.get('error_code') or ran.get('status')}"
            return Trial(
                candidate,
                value,
                False,
                reason=reason,
                price=candidate.price_per_call,
                seconds=seconds,
                checks=checks,
                match=match,
            )
        passed = bool(ran.get("passed"))
        if passed:
            reason = f"passed, {match}% match" if match is not None else "passed"
        elif match is None:
            reason = "no check could be scored"
        else:
            reason = f"{match}% match, below the threshold {ran.get('threshold')}"
        return Trial(
            candidate,
            value,
            passed,
            reason=reason,
            price=candidate.price_per_call,
            seconds=seconds,
            checks=checks,
            match=match,
        )

    def _ranked(self, trials: list[Trial]) -> list[Trial]:
        """Passed candidates by the console's Prefer setting, then those that take no trials (spec: ranked without a
        trial score)."""
        return self._order([t for t in trials if t.passed]) + self._order([t for t in trials if t.untried])

    def _order(self, passed: list[Trial]) -> list[Trial]:
        prefer = str((self.agent.config.get("sourcing") or {}).get("rank_by", "accuracy"))

        def evidence(t: Trial) -> int:
            return int(t.candidate.reviews.get("reviews") or 0)

        if prefer == "price":
            return sorted(passed, key=lambda t: _amount(t.price))
        if prefer == "balanced":
            return sorted(passed, key=lambda t: (_amount(t.price) / (1 + evidence(t)), -evidence(t)))
        # Most accurate: the higher trial score, then EuKollab's tested quality where it is shown, then evidence, then
        # the lower price.
        return sorted(
            passed,
            key=lambda t: (
                -(t.score if t.score is not None else -1.0),
                -_tested(t.candidate),
                -evidence(t),
                _amount(t.price),
            ),
        )

    def _reason(self, main: Trial, sample: Sample, expect: dict[str, Any] | str, picked: bool) -> str:
        if picked:
            return f"{main.candidate.handle} was picked by your own rule."
        if main.untried:
            price = main.price or "no stated"
            return f"{main.candidate.handle} takes no trials; no candidate that does passed. At {price} per call."
        checked = isinstance(expect, str) or bool(expect.get("checks"))
        what = "your sample and expectation" if checked else "your sample, against its output schema only"
        basis = what if sample is not None else "its own published example only, not your case"
        score = f", scoring {main.score:.2f}" if main.score is not None else ""
        return f"{main.candidate.handle} passed a trial on {basis}{score}, at {main.price or 'no stated'} per call."

    async def _record(
        self,
        need_key: str,
        trials: list[Trial],
        main: Candidate,
        backup: Candidate | None,
        reason: str,
        code: str = "initial",
    ) -> Choice:
        recorded = await self._call(
            "sourcing_record",
            need_key=need_key,
            candidates=[
                {
                    "handle": t.candidate.handle,
                    "capability_id": t.candidate.capability_id,
                    "eligible": t.candidate.eligible,
                    "excluded_by": t.candidate.excluded_by,
                    "trial_score": _trial_score(t),
                    "notes": t.reason[:200],
                    "rank": i + 1,
                }
                for i, t in enumerate(trials)
            ],
            primary=main.handle,
            backup=backup.handle if backup else None,
            rationale=reason,
            reason=code,
        )
        waiting = recorded.get("state") == "awaiting_owner"
        return Choice(main, backup, trials, reason, waiting=waiting, decision_id=str(recorded.get("decision_id", "")))

    async def record_choice(self, main: Candidate, backup: Candidate | None, reason: str, need_key: str = "") -> Choice:
        trials = [Trial(main, passed=True, reason="chosen by the agent")]
        if backup:
            trials.append(Trial(backup, passed=True, reason="chosen as backup"))
        return await self._record(need_key or _key(str(main.summary)), trials, main, backup, reason)

    # using a service

    async def _connection(self, candidate: Candidate, deadline: float) -> str:
        """An active connection with access to the service: reused, or requested and waited for until `deadline`. On a
        connection to the provider that covers other services, the request asks the provider to add this one."""

        def covering(listed: dict[str, Any]) -> str | None:
            for c in listed.get("connections") or []:
                if c.get("peer") == candidate.handle and candidate.capability_id in (c.get("peer_capabilities") or []):
                    return str(c["connection_id"])
            return None

        found = covering(await self._call("connection_list", state="active"))
        if found:
            return found
        requested = await self._call(
            "connection_request", target=candidate.handle, capability_ids=[candidate.capability_id], note=""
        )
        connection_id = str(requested["connection_id"])
        while True:
            if covering(await self._call("connection_list", state="active")) == connection_id:
                return connection_id
            if time.monotonic() >= deadline:
                break
            await asyncio.sleep(3)
        raise ConnectionPending(
            f"{candidate.handle} has not accepted the connection yet.",
            code="CONNECTION_PENDING",
            detail={"connection_id": connection_id},
        )

    async def _send_files(self, execution_id: str, provider_id: str, arguments: dict[str, Any]) -> dict[str, Any]:
        files = self._files(execution_id, provider_id)
        if files is None:
            return arguments
        try:
            sent: dict[str, Any] = await files.send(arguments)
        except (FileUnavailableError, ValueError) as exc:
            code = "FILE_TOO_LARGE" if isinstance(exc, ValueError) else "FILE_UNAVAILABLE"
            raise AgentError(f"A file couldn't be sent with the call: {exc}", code=code) from exc
        return sent

    async def _receive_files(self, execution_id: str, provider_id: str, value: Any) -> Any:
        files = self._files(execution_id, provider_id)
        if files is None or not references(value):
            return value
        try:
            return await files.receive(value)
        except FileUnavailableError as exc:
            raise AgentError(f"A file of the result couldn't be fetched: {exc}", code="FILE_UNAVAILABLE") from exc

    def _files(self, execution_id: str, provider_id: str) -> FileTransfer | None:
        """One call's files, through the network's file store, when this agent's network client has one."""
        upload = getattr(self.agent.network, "file_upload", None)
        download = getattr(self.agent.network, "file_download", None)
        if upload is None or download is None:
            return None
        return FileTransfer(
            lambda sha256, size, kind: upload(execution_id, provider_id, sha256, size, kind),
            lambda sha256: download(sha256, execution_id),
            # The client a network client moves files with, when it has one of its own (a test's, say).
            getattr(self.agent.network, "file_http", None),
        )

    async def _agreement(self, candidate: Candidate, data_class: str = "public") -> tuple[str, dict[str, Any]]:
        # The connection and a paid agreement are waited for within one CONNECTION_WAIT, which act() outlasts.
        deadline = time.monotonic() + CONNECTION_WAIT.total_seconds()
        saved = self.agent.journal.agreement(candidate.handle, candidate.capability_id)
        # An agreement covers one declared data class: sharing another needs another agreement.
        if saved is not None and saved[1].get("data_class", data_class) != data_class:
            self.agent.journal.forget_agreement(candidate.handle, candidate.capability_id)
            saved = None
        if saved is not None:
            connection_id, agreement = saved
            got = await self._call("engagement_get", collaboration_id=agreement["collaboration_id"])
            if got.get("engagement_state") == "offered":
                got = await self._accepted(candidate, agreement, deadline)
            if got.get("engagement_state") == "open":
                return connection_id, agreement | got
            self.agent.journal.forget_agreement(candidate.handle, candidate.capability_id)
        connection_id = await self._connection(candidate, deadline)
        plan = (candidate.plans or [{"plan_id": "free", "type": "free"}])[0]
        made = await self._call(
            "collaboration_create",
            connection_id=connection_id,
            capability_id=candidate.capability_id,
            expected_outcome=f"{candidate.capability_id} answers as declared"[:500],
            acceptance_criteria="the result matches the service's declared output",
            data_class=data_class,
            plan_id=plan.get("plan_id") or plan.get("type"),
        )
        made = {**made, "data_class": data_class}
        self.agent.journal.save_agreement(candidate.handle, candidate.capability_id, connection_id, made)
        if made.get("engagement_state") == "offered":
            got = await self._accepted(candidate, made, deadline)
            if got.get("engagement_state") != "open":
                self.agent.journal.forget_agreement(candidate.handle, candidate.capability_id)
                raise AgentError(
                    f"{candidate.handle} did not accept the agreement ({got.get('engagement_state')}).",
                    code="AGREEMENT_DECLINED",
                    detail={"collaboration_id": made["collaboration_id"]},
                )
            made = made | got
        return connection_id, made

    async def _accepted(self, candidate: Candidate, agreement: dict[str, Any], deadline: float) -> dict[str, Any]:
        """An offered agreement, waited for as a connection is: a provider's agent usually accepts a paid one on its
        next pass, within a minute or two, and failing at once would lose the need that asked for it. The engagement
        as it then stands, open or ended; AgreementPending when it is still offered at `deadline`."""
        collaboration_id = agreement["collaboration_id"]
        while time.monotonic() < deadline:
            await asyncio.sleep(3)
            got: dict[str, Any] = await self._call("engagement_get", collaboration_id=collaboration_id)
            if got.get("engagement_state") != "offered":
                return got
        raise AgreementPending(
            f"A paid agreement with {candidate.handle} waits for the provider to accept it.",
            code="AGREEMENT_PENDING",
            detail={"collaboration_id": collaboration_id},
        )

    async def close_agreement(self, candidate: Candidate, outcome: str = "completed") -> dict[str, Any]:
        """Close the open agreement with this service, opening its review period; nothing to close is an error."""
        saved = self.agent.journal.agreement(candidate.handle, candidate.capability_id)
        if saved is None:
            raise AgentError(f"There is no agreement with {candidate.handle} to close.", code="NOT_FOUND")
        closed = await self._call("engagement_close", collaboration_id=saved[1]["collaboration_id"], outcome=outcome)
        self.agent.journal.forget_agreement(candidate.handle, candidate.capability_id)
        return closed

    async def _access(self, connection_id: str, candidate: Candidate) -> dict[str, Any]:
        """Access for the service, kept until a minute before it expires."""
        kept = self.agent.journal.access(connection_id)
        if kept and _fresh(str(kept.get("expires_at", ""))):
            return kept
        access = await self._call(
            "peer_get_access", connection_id=connection_id, capability_ids=[candidate.capability_id]
        )
        self.agent.journal.save_access(connection_id, access)
        return access

    async def call(
        self,
        candidate: Candidate,
        arguments: dict[str, Any],
        criteria: Callable[[Any], bool] | None = None,
        idempotency_key: str | None = None,
        *,
        same_contract: bool = False,
        data_class: str = "public",
    ) -> Result:
        """One execution: recorded before the first attempt, retried safely, reported with its receipt.
        With same_contract, a provider whose schemas changed since it was first called is not called:
        ContractChanged is raised before anything is reserved."""
        if not self.agent.config:
            await self.agent._identify()
        key = idempotency_key or "call-" + secrets.token_hex(12)
        # A key seen before reuses its execution ID and salt: the provider's repeat behaviour answers the same
        # execution again without new work, and the reservation is not made twice.
        recorded = self.agent.journal.call(key)
        if recorded is None:
            recorded = OutgoingCall(
                key, "ex_" + secrets.token_hex(12), new_salt(), candidate.handle, candidate.capability_id, "started", {}
            )
            self.agent.journal.start_call(recorded)
        connection_id, agreement = await self._agreement(candidate, _data_class(data_class))
        access = await self._access(connection_id, candidate)
        contract = str((access.get("contracts") or {}).get(candidate.capability_id) or "")
        if same_contract and candidate.contract and contract and contract != candidate.contract:
            raise ContractChanged(
                f"{candidate.handle}'s service changed its schemas since it was chosen.", code="CONTRACT_CHANGED"
            )
        candidate.contract = candidate.contract or contract
        tool, version = _tool_and_version(access, candidate)
        terms = agreement.get("terms") or {}
        ekl: dict[str, Any] = {
            "execution_id": recorded.execution_id,
            "capability_id": candidate.capability_id,
            "capability_version": version,
            "agreement_id": agreement["collaboration_id"],
            "salt": recorded.salt,
        }
        if terms.get("hash"):
            ekl["terms_hash"] = terms["hash"]
        # Large files travel beside the call (EKP 3.5 §5.5); the commitment and fingerprints cover their references.
        arguments = await self._send_files(recorded.execution_id, candidate.agent_id, arguments)
        held_price = None
        if _paid(agreement, terms):
            ekl["execution_grant"], held_price = await self._grant(
                recorded, candidate, version, agreement, terms, arguments
            )
            ekl["execution_id"] = recorded.execution_id
        answer = None
        assert self.agent.network is not None
        sent = False
        sent_at = time.time()
        for attempt in range(3):
            try:
                answer = await self.agent.network.call_provider(
                    str(access["endpoint"]), str(access["access_token"]), tool, arguments, ekl
                )
                break
            except ProviderUnavailable as exc:
                # Network failures are retried with the same execution ID, so the work runs at most once.
                sent = sent or bool(exc.detail.get("sent", True))
                if attempt == 2:
                    raise ProviderUnavailable(
                        exc.message, code=exc.code, fix=exc.fix, detail=exc.detail | {"sent": sent}
                    ) from exc
                await asyncio.sleep(2 * (attempt + 1))
        assert answer is not None
        answered_at = time.time()
        status = await self._status(recorded.execution_id, candidate)
        outcome = str(status.get("outcome") or ("failed" if answer.is_error else "succeeded"))
        if status.get("state") not in ("completed", "settled") and not answer.is_error and not status.get("receipt"):
            self.agent.journal.finish_call(key, {"status": "unknown"})
            raise OutcomeUnknown(
                f"{candidate.handle} has not said how the call ended.",
                code="OUTCOME_UNKNOWN",
                fix=f"Call again with idempotency_key={key!r} to learn it; nothing more is charged.",
            )
        receipt = dict(status.get("receipt") or {})
        checked, flags = await self._report(
            key, recorded, candidate, agreement, arguments, answer, outcome, receipt, criteria, (sent_at, answered_at)
        )
        # A run that bought a pack costs the pack's price, one that took a call of a bought pack nothing more.
        price = held_price or agreement.get("price_per_call")
        charged = str(receipt.get("amount") or (price if outcome == "succeeded" else "0"))
        self.agent.journal.finish_call(key, {"status": outcome, "charged": charged, "receipt_checked": checked})
        if answer.is_error:
            raise AgentError(answer.text or f"{candidate.handle} returned an error.", code="PROVIDER_ERROR")
        return Result(
            await self._receive_files(recorded.execution_id, candidate.agent_id, answer.value),
            recorded.execution_id,
            charged,
            checked,
            safety_flags=flags,
        )

    async def _report(
        self,
        key: str,
        recorded: OutgoingCall,
        candidate: Candidate,
        agreement: dict[str, Any],
        arguments: dict[str, Any],
        answer: ProviderAnswer,
        outcome: str,
        receipt: dict[str, Any],
        criteria: Callable[[Any], bool] | None,
        window: tuple[float, float],
    ) -> tuple[bool, list[dict[str, str]]]:
        """Check the receipt claim by claim, screen the result here, and report the call with both. Returns whether
        the receipt's fingerprints match and the screening's flags."""
        sent_at, answered_at = window
        input_fp = fingerprint(recorded.salt, arguments)
        checked = (
            bool(receipt)
            and receipt.get("input_fingerprint") == input_fp
            and (receipt.get("output_fingerprint") == fingerprint(recorded.salt, answer.body))
        )
        digest = None
        if receipt.get("receipt_jws"):
            made = CallMade(
                self.agent.agent_id,
                candidate.agent_id,
                recorded.execution_id,
                candidate.capability_id,
                input_fp,
                sent_at,
                answered_at,
            )
            try:
                await self._check_receipt(str(receipt["receipt_jws"]), made)
            except ReceiptInvalid:
                # Neither acknowledged nor relied on: the call's outcome stays recorded, the receipt doesn't count.
                self.agent.journal.finish_call(key, {"status": outcome, "receipt_checked": False})
                raise
            digest = receipt_digest(str(receipt["receipt_jws"]))
        flags = await self._screen(answer.value if answer.value is not None else answer.body)
        # Without criteria the call is not assessed, so it neither helps nor hurts the provider's derived review.
        useful = outcome == "succeeded" and not answer.is_error and criteria is not None and criteria(answer.value)
        await self._call(
            "collaboration_report",
            collaboration_id=agreement["collaboration_id"],
            execution_id=recorded.execution_id,
            result=("useful" if useful else "not_useful") if criteria else "not_assessed",
            receipt=receipt or None,
            criteria_met=("yes" if useful else "no") if criteria else None,
            receipt_digest=digest,
            output_valid=_output_valid(candidate, answer.value) if not answer.is_error else None,
            # A flag counts only bound to a verified receipt whose output fingerprint is the result screened here:
            # the receipt authenticates the call, the match ties the flag to that call's own output.
            safety_flag=flags[0] if flags and digest and checked else None,
        )
        return checked, flags

    async def _check_receipt(self, receipt_jws: str, made: CallMade) -> None:
        keys = self.agent.receipt_keys
        base = getattr(self.agent.network, "base_url", None)
        if keys is None or not base:
            return  # a test network signs nothing
        await verify_receipt(receipt_jws, made, base, keys)

    async def _screen(self, value: Any) -> list[dict[str, str]]:
        """Local screening, unless this agent's own configuration turns it off (consumer.screen_results)."""
        consumer = (self.agent.config or {}).get("consumer") or {}
        if consumer.get("screen_results") is False or self.agent.screener is None:
            return []
        return await self.agent.screener.screen(value)

    async def call_planned(
        self,
        planned: PlannedService,
        arguments: dict[str, Any],
        idempotency_key: str | None = None,
        *,
        choose_again: bool = False,
        provider: str | None = None,
    ) -> Result:
        """A planned service's call, to the chosen provider. The choice is the agent's latest sourcing decision for the
        need, so a restart resumes it without trials; when a pack bought from it runs out, the console's rule
        sourcing.on_pack_end applies, and `choose_again` compares again now, `provider` ("@handle/service") names the
        provider to use instead. When the provider is unavailable, failover follows the service's
        side-effect class: read-only or safely repeatable work goes to the backup once, as a new execution linked to
        the same key; work that acts outside goes to the backup only when the first call is known to have had no
        effect, and otherwise ends with its outcome unknown, to be learnt with the same idempotency_key."""
        if self.agent.network is None:
            raise AgentNotEnabled
        await self._select(planned, choose_again, provider)
        assert planned.main is not None
        key = idempotency_key or "call-" + secrets.token_hex(12)
        failed = planned.main
        try:
            result = await self.call(failed, arguments, None, key, same_contract=True, data_class=planned.data_class)
        except ContractChanged:
            # The selection was made against the old contract: choose again, with trials, then call.
            planned.main = planned.backup = None
            return await self.call_planned(planned, arguments, f"{key}-rechosen")
        except ProviderUnavailable as exc:
            if failed.side_effect_class == "external_action" and not await self._no_effect(key, failed, exc):
                raise OutcomeUnknown(
                    f"{failed.handle} may have acted before the call failed, so it is not repeated at the backup.",
                    code="OUTCOME_UNKNOWN",
                    fix=f"Call again with idempotency_key={key!r} to learn how it ended; nothing more is charged.",
                ) from exc
            if planned.backup is None:
                planned.main = None  # the next call chooses again
                raise
            planned._chose(planned.backup, None)
            assert planned.main is not None
            result = await self.call(planned.main, arguments, None, f"{key}-backup", data_class=planned.data_class)
        planned.called = True
        if planned.calls_left is not None:
            # A call with no calls left bought a new pack and used its first call.
            planned.calls_left = planned.calls_left - 1 if planned.calls_left > 0 else planned.main.pack_calls - 1
        return result

    async def _select(self, planned: PlannedService, choose_again: bool, provider: str | None) -> None:
        """The provider the next call goes to: the agent's recorded choice, a fresh choice on a first call, a provider
        its code names, or a comparison when its code asks or a pack from the main choice ran out."""
        if planned.main is None and not planned.resumed:
            planned.resumed = True
            await self._resume(planned)
        pack_ended = planned.main is not None and planned.calls_left == 0 and planned.called
        rule = str((self.agent.config.get("sourcing") or {}).get("on_pack_end", "compare"))
        if provider:
            await self._use_named(planned, provider)
        elif planned.main is None:
            choice = await self._choose(
                planned.need, planned.sample, expectation(planned.expect), None, prefer=planned.prefer
            )
            if choice.waiting:
                raise _waiting(planned.need)
            assert choice.main is not None
            planned._chose(choice.main, choice.backup)
        elif choose_again:
            await self._compare(planned, "agent_request")
        elif pack_ended and rule != "keep":
            # A pack bought from the main choice ran out: the next call would buy another.
            await self._compare(planned, "pack_end")

    async def _resume(self, planned: PlannedService) -> None:
        """The agent's latest sourcing decision for the need, so a restarted connector carries on with its choice and
        the calls left in its packs, without searching or trialling again."""
        try:
            got = await self._call("sourcing_get", need_key=planned.need_key)
        except AgentError:
            return  # nothing recorded, or a network that keeps no decisions: choose as on a first call
        decision = dict(got.get("decision") or {})
        if decision.get("state") == "awaiting_owner":
            raise _waiting(planned.need)
        if decision.get("state") not in ("recorded", "approved") or not decision.get("primary"):
            return
        found, earlier = await self.find_with_history(planned.need, 25, planned.need_key)
        offered = [c for c in [*found, *earlier] if c.still_listed and c.eligible]
        main = next((c for c in offered if c.handle == decision["primary"]), None)
        backup = next((c for c in offered if c.handle == decision.get("backup")), None)
        if main is not None:
            planned._chose(main, backup)

    async def _compare(self, planned: PlannedService, code: str) -> None:
        """Compare again, keeping the current choice unless another candidate is clearly better; providers used before
        are compared on their record, not trialled."""
        choice = await self._choose(
            planned.need, planned.sample, expectation(planned.expect), None, keep=planned.main, code=code
        )
        if choice.waiting:
            raise _waiting(planned.need)
        assert choice.main is not None
        planned._chose(choice.main, choice.backup)

    async def _use_named(self, planned: PlannedService, provider: str) -> None:
        """The provider the agent's code names becomes the main choice, the previous one its backup, recorded with
        reason agent_request; calls left in the previous provider's packs stay there."""
        found, earlier = await self.find_with_history(planned.need, 25, planned.need_key)
        named = [c for c in [*found, *earlier] if _names(provider, c) or c.handle == provider]
        if not named:
            name = provider.partition("/")[2].removeprefix("cap_").replace("_", " ") or provider.lstrip("@")
            named = [c for c in await self.find(name, limit=25) if _names(provider, c) or c.handle == provider]
        if not named:
            raise AgentError(f"{provider} offers no service for this need.", code="NOT_FOUND")
        chosen = named[0]
        if not chosen.still_listed or not chosen.eligible:
            raise AgentError(
                f"{provider} can't be used: " + (", ".join(chosen.excluded_by) or "it is no longer listed"),
                code="POLICY_DENIED",
                detail={"excluded_by": chosen.excluded_by},
            )
        previous = planned.main if planned.main is not None and not _same(planned.main, chosen) else None
        tried = [Trial(chosen, passed=True, reason="named by the agent's code")]
        if previous is not None:
            tried.append(Trial(previous, passed=True, reason="the previous choice"))
        choice = await self._record(
            planned.need_key,
            tried,
            chosen,
            previous,
            f"{chosen.handle} named by the agent's code.",
            code="agent_request",
        )
        if choice.waiting:
            raise _waiting(planned.need)
        planned._chose(chosen, previous)

    async def _no_effect(self, key: str, candidate: Candidate, failure: ProviderUnavailable) -> bool:
        """Whether a failed call is known to have had no effect: it never reached the provider, or the network
        records that it ended before any effect."""
        if not failure.detail.get("sent", True):
            return True
        recorded = self.agent.journal.call(key)
        if recorded is None:
            return True
        try:
            found = await self._call(
                "execution_get", execution_id=recorded.execution_id, provider_agent_id=candidate.agent_id
            )
        except AgentError:
            return False  # not known: an unknown outcome is reconciled, never repeated elsewhere
        status = dict(found.get("execution") or found)
        resolution = dict(status.get("resolution") or {})
        return "failed_before_effect" in (status.get("outcome"), resolution.get("outcome"))

    async def _grant(
        self,
        recorded: OutgoingCall,
        candidate: Candidate,
        version: str,
        agreement: dict[str, Any],
        terms: dict[str, Any],
        arguments: dict[str, Any],
    ) -> tuple[str, str]:
        """Reserve the paid run, offering the agreed pack's whole price; returns the grant and what the reservation
        holds. A grant that lapsed unredeemed, most often because this machine slept between the reservation and the
        call, ran nothing and charged nothing, so the run is reserved once more as a new execution linked to the same
        logical intent, under the same idempotency key."""
        intent = ""
        for attempt in range(2):
            reserved = await self._call(
                "execution_reserve",
                collaboration_id=agreement["collaboration_id"],
                execution_id=recorded.execution_id,
                capability_id=candidate.capability_id,
                capability_version=version,
                input_commitment={
                    "alg": "sha-256",
                    "canon": "ekl-json-1",
                    "digest": fingerprint(recorded.salt, arguments),
                },
                terms_hash=str(terms.get("hash", "")),
                requested_max_amount_atoms=str(_pack_atoms(agreement, terms)),
                idempotency_key=recorded.execution_id,
                logical_intent_id=intent or None,
            )
            _not_reserved_again(recorded, reserved)
            try:
                held = _held(reserved)
                return str(held["execution_grant"]), _held_price(held)
            except _GrantLapsedError as lapsed:
                if attempt:
                    raise AgentError(
                        "The run's grant expired again before it was used; nothing ran or was charged.",
                        code="GRANT_EXPIRED",
                        fix=f"Call again with idempotency_key={recorded.idempotency_key!r}.",
                        detail=lapsed.detail,
                    ) from lapsed
                intent = recorded.execution_id
                recorded.execution_id = "ex_" + secrets.token_hex(12)
                self.agent.journal.replace_execution(recorded.idempotency_key, recorded.execution_id)
                log.info("the grant for %s lapsed unused; reserving the run again as %s", intent, recorded.execution_id)
        raise AssertionError("unreachable")  # the second attempt returns or raises

    async def _status(self, execution_id: str, candidate: Candidate) -> dict[str, Any]:
        for _ in range(10):
            found = await self._call("execution_get", execution_id=execution_id, provider_agent_id=candidate.agent_id)
            status = dict(found.get("execution") or found)
            if status.get("state") in ("completed", "settled") or status.get("receipt"):
                return status
            await asyncio.sleep(1)
        return status


def _names(ref: str, candidate: Candidate) -> bool:
    """Whether "@handle/service" names this candidate; the service may be given with or without its cap_ prefix."""
    handle, _, name = ref.partition("/")
    return candidate.handle == handle and candidate.capability_id in (name, f"cap_{name}")


def _waiting(need: str) -> ChoiceWaiting:
    return ChoiceWaiting(
        f"The choice for {need!r} waits for your approval in the console.",
        code="CHOICE_WAITING",
        fix="Approve it in Needs you or on the agent's Spending tab.",
    )


def _significant(candidate: Candidate) -> bool:
    return bool((candidate.tested_quality or {}).get("significant"))


def _same(a: Candidate, b: Candidate) -> bool:
    return a.handle == b.handle and a.capability_id == b.capability_id


def _used_reason(candidate: Candidate) -> str:
    """Why a provider used before was not trialled: its record with this agent."""
    used = candidate.used_before or {}
    rating = (used.get("own_reviews") or {}).get("rating")
    rated = f", rated {rating} by you" if rating is not None else ""
    return f"used before: {used.get('calls', 0)} calls, {used.get('calls_left', 0)} left{rated}; not trialled again"


def _tested(candidate: Candidate) -> float:
    """EuKollab's tested quality of the candidate, when it is shown; 0 until the evidence is significant."""
    quality = candidate.tested_quality or {}
    value = quality.get("t")
    return float(value) if quality.get("significant") and isinstance(value, (int, float)) else 0.0


def _trial_score(trial: Trial) -> str:
    if trial.skipped:
        return "skipped"
    if trial.untried:
        return "no trials"
    if trial.score is None:
        return "passed" if trial.passed else "not scored"
    return f"{trial.score:.2f}"


def _passed(prefer: str) -> Callable[[list[Trial]], Trial | None]:
    """The preferred service when it passed its trial, or takes none; otherwise the ranking decides."""
    return lambda trials: next((t for t in trials if (t.passed or t.untried) and _names(prefer, t.candidate)), None)


def _key(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:60] or "need"


def need_key(text: str) -> str:
    """The key a need's sourcing decisions are recorded under: the need in words, reduced to a stable slug."""
    return _key(text)


def _held(reserved: dict[str, Any]) -> dict[str, Any]:
    """The held reservation with the run's execution grant: the network holds a paid run's price at once or refuses it
    (BALANCE_LOW), so a reservation without a usable grant is one whose grant lapsed before it was used."""
    if reserved.get("state") == "held" and reserved.get("execution_grant") and not _expired(reserved):
        return reserved
    raise _GrantLapsedError(
        "the grant lapsed unused", code="GRANT_EXPIRED", detail={"reservation_id": reserved.get("reservation_id")}
    )


# A retry under the same execution ID finds its reservation as it stands. Once the provider redeemed the grant, the run
# reached it: it is never reserved again as a new execution, which would run and pay for it twice (Appendix B.4).
REACHED_PROVIDER = ("redeemed", "reconciliation_required")
FINISHED = ("captured", "released")


def _not_reserved_again(recorded: OutgoingCall, reserved: dict[str, Any]) -> None:
    state = str(reserved.get("state") or "")
    detail = {"execution_id": recorded.execution_id, "reservation_id": reserved.get("reservation_id"), "state": state}
    if state in REACHED_PROVIDER:
        raise OutcomeUnknown(
            "The run reached the provider and its outcome isn't settled yet; nothing more is charged.",
            code="OUTCOME_UNKNOWN",
            fix=f"Call again later with idempotency_key={recorded.idempotency_key!r} to learn how it ended.",
            detail=detail,
        )
    if state in FINISHED:
        outcome = "succeeded" if state == "captured" else "ended without effect"
        charged = (recorded.outcome or {}).get("charged")
        paid = f", charged {charged}" if charged else ""
        raise CallFinished(
            f"This call already ran ({outcome}{paid}); it isn't run or charged again.",
            code="CALL_FINISHED",
            fix="Use a new idempotency_key for a new call.",
            detail=detail | {"outcome": outcome},
        )


def _held_price(reservation: dict[str, Any]) -> str:
    """What the reservation holds, as the protocol writes amounts: a pack's price when the run buys it ("8.00", or
    "0.005" with more decimals), "0.00" when it takes a call of a pack already bought."""
    atoms = Decimal(str(reservation.get("amount_atoms") or 0)).scaleb(-int(reservation.get("amount_scale") or 0))
    return str(atoms.quantize(Decimal("0.01")) if atoms == atoms.quantize(Decimal("0.01")) else atoms.normalize())


def _report_end(task: "asyncio.Task[None]") -> None:
    """The event loop never ends on its own: say why it did, rather than leave the callbacks silently unrun."""
    if not task.cancelled() and task.exception() is not None:
        log.error(
            "the agent's event loop stopped; its callbacks won't run until it restarts", exc_info=task.exception()
        )


class _GrantLapsedError(AgentError):
    """A paid run's grant expired before it was redeemed; `call` reserves the run again."""


def _expired(record: dict[str, Any]) -> bool:
    try:
        expiry = dt.datetime.fromisoformat(str(record.get("grant_expires_at") or ""))
    except ValueError:
        return False  # no expiry given: the network decides at redemption
    return expiry <= dt.datetime.now(dt.UTC)


def _fresh(expires_at: str) -> bool:
    try:
        expiry = dt.datetime.fromisoformat(expires_at)
    except ValueError:
        return False
    return expiry - dt.timedelta(minutes=1) > dt.datetime.now(dt.UTC)
