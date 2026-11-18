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
from collections.abc import Callable, Coroutine, Iterable
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
from ekl_provider_adapter.fingerprint import fingerprint, new_salt
from ekl_provider_adapter.network_client import Network, ProviderAnswer
from ekl_provider_adapter.receipts import CallMade, KeySet, ReceiptInvalid, receipt_digest
from ekl_provider_adapter.receipts import verify as verify_receipt
from ekl_provider_adapter.screening import Screener

log = logging.getLogger("ekl_provider_adapter.agent")
ANNOUNCE_RETRY_SECONDS = 10.0
# The EKP version this package speaks, as world_get_contract names it.
EKP_PROTOCOL = "ekp/2.0"
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
WAIT_SECONDS = 25
CONNECTION_WAIT = dt.timedelta(minutes=10)
TRIALS_AT_MOST = 3
DECISION_KINDS = ("connection_request", "outreach", "paid_offer", "outside_action_approval")
NEED_MATCHING = ("my_services", "subscribed", "all")
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
class Decision:
    """A decision the publisher handed to the agent (Ask my runtime). Answer before `deadline`, or
    the console's fallback applies."""

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
        )

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

    def trial(self, sample_input: dict[str, Any]) -> dict[str, Any]:
        return self.agent._sync(self.agent.aio.trial(self, sample_input))


@dataclass
class Trial:
    """One candidate as `choose` considered it."""

    candidate: Candidate
    output: Any = None
    passed: bool = False
    skipped: bool = False
    reason: str = ""
    price: str = ""
    seconds: float = 0.0


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
    # The check the trials passed. `use` never applies it by itself, since it may hold only for the trial's own
    # sample; pass it on with `use(choice.main, criteria=choice.check)` when it holds for every input.
    check: Callable[[Any], bool] | None = field(default=None, repr=False)


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
        self._announced_on: Network | None = None
        # Announced (when it announces itself) and active: the first action makes it so.
        self._ready = False
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

    def on_choice_approved(self, callback: Callback) -> Callback:
        """The publisher approved a shortlist that `choose` left waiting."""
        return self._register("on_choice_approved")(callback)

    def on_decision(self, *, kind: str | None = None) -> Callable[[Callback], Callback]:
        """A decision the console hands to the agent (Ask my runtime), with its deadline; `kind` narrows it."""
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
        a paid run under another version's rules would be refused anyway, one call at a time."""
        try:
            contract = await self._call("world_get_contract")
        except AgentError:
            return  # not answered now: identifying the agent next reports an unreachable network
        spoken = {contract.get("protocol")} | {
            str(p.get("protocol")) for p in contract.get("accepted_protocols") or [] if isinstance(p, dict)
        }
        if contract.get("protocol") and EKP_PROTOCOL not in spoken:
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
        await self._ensure_ready()
        await self._subscribe()
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
                await self._call("events_ack", cursor=acked, failed=failed or None)
            if retry:
                await asyncio.sleep(min(RETRY_SECONDS * max(attempts.values(), default=1), 30))

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
        elif kind == "message.received":
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
        elif kind == "sourcing.approved":
            await self._approved(data)
        elif kind == "decision.requested":
            await self._decision(data)
        elif kind == "review.requested":
            await self._review_due(ReviewDue.from_event({"collaboration_id": event.get("resource_id", ""), **data}))

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

    def find(self, query: str, limit: int = 5) -> list[Candidate]:
        return self._sync(self.aio.find(query, limit))

    def choose(
        self,
        need_or_query: Need | str,
        sample: Sample = None,
        check: Callable[[Any], bool] | None = None,
        pick: Callable[[list[Trial]], Trial | None] | None = None,
        candidates: int = TRIALS_AT_MOST,
    ) -> Choice:
        return self._sync(self.aio.choose(need_or_query, sample, check, pick, candidates))

    def record_choice(self, main: Candidate, backup: Candidate | None, reason: str, need_key: str = "") -> Choice:
        return self._sync(self.aio.record_choice(main, backup, reason, need_key))

    def service(
        self,
        need: str,
        *,
        prefer: str | None = None,
        sample: Sample = None,
        check: Callable[[Any], bool] | None = None,
        data_class: str = "public",
    ) -> "PlannedService":
        """A service the agent plans to use. The first call chooses a provider with trials, trying `prefer`
        ("@handle/service") first when it is eligible and taking it when it passes, and later calls reuse the
        choice."""
        return PlannedService(self, need, prefer, sample, check, _data_class(data_class))


class PlannedService:
    """A service the agent plans to use (`uses` in eukollab.json), called with the local API's `service`: chosen on
    its first call and reused after."""

    def __init__(
        self,
        agent: Agent,
        need: str,
        prefer: str | None,
        sample: Sample,
        check: Callable[[Any], bool] | None,
        data_class: str = "public",
    ):
        self.agent, self.need, self.prefer, self.sample, self.check = agent, need, prefer, sample, check
        self.data_class = data_class
        self.main: Candidate | None = None
        self.backup: Candidate | None = None

    def call(self, arguments: dict[str, Any], idempotency_key: str | None = None) -> Result:
        return self.agent._sync(self.agent.aio.call_planned(self, arguments, idempotency_key))


DATA_CLASSES = ("public", "synthetic", "internal")


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
        found = await self._call("agent_search", query=query, limit=limit)
        return [Candidate.from_search(self.agent, item) for item in found.get("items") or []]

    async def showcase(self, candidate: Candidate) -> dict[str, Any]:
        shown = await self._call("agent_get_showcase", handle=candidate.handle, capability_id=candidate.capability_id)
        candidate._manifest = dict(shown.get("manifest") or {})
        return shown

    async def trial(self, candidate: Candidate, sample_input: dict[str, Any]) -> dict[str, Any]:
        return await self._call(
            "capability_trial",
            provider=candidate.handle,
            capability_id=candidate.capability_id,
            sample_input=sample_input,
        )

    async def choose(
        self,
        need_or_query: Need | str,
        sample: Sample = None,
        check: Callable[[Any], bool] | None = None,
        pick: Callable[[list[Trial]], Trial | None] | None = None,
        candidates: int = TRIALS_AT_MOST,
    ) -> Choice:
        """Find, try, judge, rank and record a provider in one call."""
        return await self._choose(need_or_query, sample, check, pick, candidates)

    async def _choose(
        self,
        need_or_query: Need | str,
        sample: Sample,
        check: Callable[[Any], bool] | None,
        pick: Callable[[list[Trial]], Trial | None] | None,
        candidates: int = TRIALS_AT_MOST,
        prefer: str | None = None,
    ) -> Choice:
        query = need_or_query.text if isinstance(need_or_query, Need) else need_or_query
        need_key = need_or_query.post_id if isinstance(need_or_query, Need) else _key(query)
        found = await self.find(str(query), limit=10)
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
            if tried >= min(max(candidates, 1), TRIALS_AT_MOST):
                trials.append(Trial(candidate, skipped=True, reason="not tried: enough candidates were tried"))
                continue
            tried += 1
            trials.append(await self._try(candidate, sample, check))
        chosen = pick(trials) if pick else None
        ranked = (
            self._ranked(trials) if chosen is None else [chosen, *[t for t in self._ranked(trials) if t is not chosen]]
        )
        if not ranked:
            raise NoProvider("No candidate passed.", trials)
        main, backup = ranked[0], (ranked[1] if len(ranked) > 1 else None)
        reason = self._reason(main, sample, picked)
        choice = await self._record(need_key, trials, main.candidate, backup.candidate if backup else None, reason)
        choice.check = check
        return choice

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

    async def _try(self, candidate: Candidate, sample: Sample, check: Callable[[Any], bool] | None) -> Trial:  # noqa: PLR0911 - one return per way a trial ends, as choose reports it
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
            # Kept and ranked without a trial.
            return Trial(candidate, passed=True, reason="takes no trials; ranked without one", price=candidate.price)
        started = time.monotonic()
        try:
            ran = await self.trial(candidate, given)
        except AgentError as exc:
            return Trial(candidate, passed=False, reason=exc.message, price=candidate.price)
        seconds = round(time.monotonic() - started, 2)
        output = ran.get("result")
        if ran.get("status") != "succeeded":
            reason = f"the trial did not succeed: {ran.get('error') or ran.get('status')}"
            return Trial(candidate, output, False, reason=reason, price=candidate.price, seconds=seconds)
        if candidate.output_schema:
            try:
                jsonschema.validate(output, candidate.output_schema)
            except jsonschema.ValidationError as exc:
                reason = f"output does not match its declared schema: {exc.message}"
                return Trial(candidate, output, False, reason=reason, price=candidate.price, seconds=seconds)
        if check is not None:
            try:
                if not check(output):
                    return Trial(candidate, output, False, reason="your check returned False", price=candidate.price)
            except Exception as exc:  # the publisher's own check: its failure is the candidate's failure
                return Trial(candidate, output, False, reason=f"your check raised {exc!r}", price=candidate.price)
        return Trial(candidate, output, True, reason="passed", price=candidate.price, seconds=seconds)

    def _ranked(self, trials: list[Trial]) -> list[Trial]:
        """Passed candidates by the console's Prefer setting."""
        passed = [t for t in trials if t.passed]
        prefer = str((self.agent.config.get("sourcing") or {}).get("rank_by", "accuracy"))

        def evidence(t: Trial) -> int:
            return int(t.candidate.reviews.get("reviews") or 0)

        if prefer == "price":
            return sorted(passed, key=lambda t: _amount(t.price))
        if prefer == "balanced":
            return sorted(passed, key=lambda t: (_amount(t.price) / (1 + evidence(t)), -evidence(t)))
        return sorted(passed, key=lambda t: (t.reason != "passed", -evidence(t), _amount(t.price)))

    def _reason(self, main: Trial, sample: Sample, picked: bool) -> str:
        if picked:
            return f"{main.candidate.handle} was picked by your own rule."
        basis = "your sample and check" if sample is not None else "its own published example only, not your case"
        return f"{main.candidate.handle} passed a trial on {basis}, at {main.price or 'no stated'} per call."

    async def _record(
        self, need_key: str, trials: list[Trial], main: Candidate, backup: Candidate | None, reason: str
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
                    "trial_score": "1/1" if t.passed else ("skipped" if t.skipped else "0/1"),
                    "notes": t.reason[:200],
                    "rank": i + 1,
                }
                for i, t in enumerate(trials)
            ],
            primary=main.handle,
            backup=backup.handle if backup else None,
            rationale=reason,
            reason="initial",
        )
        waiting = recorded.get("state") == "awaiting_owner"
        return Choice(main, backup, trials, reason, waiting=waiting, decision_id=str(recorded.get("decision_id", "")))

    async def record_choice(self, main: Candidate, backup: Candidate | None, reason: str, need_key: str = "") -> Choice:
        trials = [Trial(main, passed=True, reason="chosen by the agent")]
        if backup:
            trials.append(Trial(backup, passed=True, reason="chosen as backup"))
        return await self._record(need_key or _key(str(main.summary)), trials, main, backup, reason)

    # using a service

    async def _connection(self, candidate: Candidate) -> str:
        """An active connection with access to the service: reused, or requested and waited for."""
        listed = await self._call("connection_list", state="active")
        for c in listed.get("connections") or []:
            if c.get("peer") == candidate.handle and candidate.capability_id in (c.get("peer_capabilities") or []):
                return str(c["connection_id"])
        requested = await self._call(
            "connection_request", target=candidate.handle, capability_ids=[candidate.capability_id], note=""
        )
        connection_id = str(requested["connection_id"])
        deadline = time.monotonic() + CONNECTION_WAIT.total_seconds()
        while time.monotonic() < deadline:
            listed = await self._call("connection_list", state="active")
            if any(c.get("connection_id") == connection_id for c in listed.get("connections") or []):
                return connection_id
            await asyncio.sleep(3)
        raise ConnectionPending(
            f"{candidate.handle} has not accepted the connection yet.",
            code="CONNECTION_PENDING",
            detail={"connection_id": connection_id},
        )

    async def _agreement(self, candidate: Candidate, data_class: str = "public") -> tuple[str, dict[str, Any]]:
        saved = self.agent.journal.agreement(candidate.handle, candidate.capability_id)
        # An agreement covers one declared data class: sharing another needs another agreement.
        if saved is not None and saved[1].get("data_class", data_class) != data_class:
            self.agent.journal.forget_agreement(candidate.handle, candidate.capability_id)
            saved = None
        if saved is not None:
            connection_id, agreement = saved
            got = await self._call("engagement_get", collaboration_id=agreement["collaboration_id"])
            if got.get("engagement_state") == "open":
                return connection_id, agreement | got
            if got.get("engagement_state") == "offered":
                raise AgreementPending(
                    f"The agreement with {candidate.handle} waits for the provider to accept it.",
                    code="AGREEMENT_PENDING",
                    detail={"collaboration_id": agreement["collaboration_id"]},
                )
            self.agent.journal.forget_agreement(candidate.handle, candidate.capability_id)
        connection_id = await self._connection(candidate)
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
            raise AgreementPending(
                f"A paid agreement with {candidate.handle} waits for the provider to accept it.",
                code="AGREEMENT_PENDING",
                detail={"collaboration_id": made["collaboration_id"]},
            )
        return connection_id, made

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
        tool = next(
            (
                str(c["tool"])
                for c in access.get("capabilities") or []
                if c.get("capability_id") == candidate.capability_id
            ),
            candidate.capability_id.removeprefix("cap_"),
        )
        version = next(
            (
                str(c["version"])
                for c in access.get("capabilities") or []
                if c.get("capability_id") == candidate.capability_id
            ),
            candidate.version,
        )
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
        return Result(answer.value, recorded.execution_id, charged, checked, safety_flags=flags)

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
            # A flag counts only bound to a verified receipt: without one it isn't sent.
            safety_flag=flags[0] if flags and digest else None,
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
        self, planned: PlannedService, arguments: dict[str, Any], idempotency_key: str | None = None
    ) -> Result:
        """A planned service's call, to the chosen provider. When the provider
        is unavailable, failover follows the service's side-effect class: read-only or safely
        repeatable work goes to the backup once, as a new execution linked to the same key; work that acts outside
        goes to the backup only when the first call is known to have had no effect, and otherwise ends with its
        outcome unknown, to be learnt with the same idempotency_key."""
        if self.agent.network is None:
            raise AgentNotEnabled
        if planned.main is None:
            choice = await self._choose(planned.need, planned.sample, planned.check, None, prefer=planned.prefer)
            if choice.waiting:
                raise ChoiceWaiting(
                    f"The choice for {planned.need!r} waits for your approval in the console.",
                    code="CHOICE_WAITING",
                    fix="Approve it under Network → Sourcing, or with: ekl sourcing approve <decision>",
                )
            planned.main, planned.backup = choice.main, choice.backup
        assert planned.main is not None
        key = idempotency_key or "call-" + secrets.token_hex(12)
        failed = planned.main
        try:
            return await self.call(failed, arguments, None, key, same_contract=True, data_class=planned.data_class)
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
            planned.main, planned.backup = planned.backup, None
            return await self.call(planned.main, arguments, None, f"{key}-backup", data_class=planned.data_class)

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


def _passed(prefer: str) -> Callable[[list[Trial]], Trial | None]:
    """The preferred service when it passed its trial; otherwise the ranking decides."""
    return lambda trials: next((t for t in trials if t.passed and _names(prefer, t.candidate)), None)


def _key(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:60] or "need"


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
