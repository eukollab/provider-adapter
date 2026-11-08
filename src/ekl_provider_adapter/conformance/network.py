# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""A fake network: the Provider API's rules, in memory, with fault injection.

It decides admissions, detects duplicates and conflicts without time limit, honours attempt and refresh request
identities, keeps leases and cancellations, records completions and appended resolutions, and keeps the funding
state each outcome implies. It keeps every agent.json announced as a version and serves the one its publisher
confirmed. Faults let the conformance suite lose responses, take the network away and
request cancellations at chosen moments.
"""

import copy
import datetime as dt
import itertools
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from ekl_provider_adapter.api import (
    PENDING_LIMIT,
    Admission,
    AdmitRequest,
    Announcement,
    CompleteRequest,
    Completion,
    ConfigAnswer,
    Outcome,
    PendingAdmission,
    PendingAdmissions,
    Problem,
    ProviderApiError,
    Renewal,
)

LEASE_SECONDS = 60.0


def iso(moment: float) -> str:
    return dt.datetime.fromtimestamp(moment, dt.UTC).isoformat()


def _wire(body: dict[str, Any]) -> dict[str, Any]:
    """Lease times travel as RFC 3339 date-times, as the network sends them."""
    return {k: iso(v) if k == "lease_expires_at" and isinstance(v, int | float) else v for k, v in body.items()}


RECONCILIATION_WINDOW = 86_400.0


@dataclass
class Grant:
    """What a valid access token stands for."""

    consumer: str
    kind: str = "customer"  # customer, trial, validation
    revoked: bool = False


@dataclass
class Execution:
    """One execution as the network keeps it, keyed by (consumer, execution ID); scenarios assert on it."""

    admission_id: str
    consumer: str
    execution_id: str
    tool: str
    input_fingerprint: str
    attempt_id: str
    kind: str
    lease_expires_at: float
    state: str = "admitted"  # admitted, lapsed, completed
    outcome: str | None = None
    output_fingerprint: str | None = None
    receipt: dict[str, Any] | None = None
    resolution: dict[str, Any] | None = None
    funding: str = "held"  # held, captured, released
    refreshes: dict[str, str] = field(default_factory=dict)
    refresh_attempts: dict[str, str] = field(default_factory=dict)  # refresh request ID -> the attempt that runs it
    cancel: str | None = None  # requested, received, confirmed
    completions: int = 0


@dataclass
class Faults:
    """Faults to inject. The lose_* counts drop that many responses after the network has acted on the request."""

    unavailable: bool = False
    lose_admit_responses: int = 0
    lose_complete_responses: int = 0
    fail_renewals: bool = False


@dataclass
class ToolRules:
    """What the network knows of a tool's capability: its repeat behaviour, refresh allowance and plan."""

    repeat: str = "refresh"
    refreshes_per_execution: int | None = None
    paid: bool = False


class FakeNetwork:
    """Implements ekl_provider_adapter.api.ProviderApi."""

    def __init__(self, *, clock: Callable[[], float] = time.time, lease_seconds: float = LEASE_SECONDS) -> None:
        self.clock = clock
        self.lease_seconds = lease_seconds
        self.grants: dict[str, Grant] = {}
        self.rules: dict[str, ToolRules] = {}
        self.executions: dict[tuple[str, str], Execution] = {}
        self.by_admission: dict[str, Execution] = {}
        self.attempts: dict[str, Admission] = {}
        self.faults = Faults()
        self.daily_limit: int | None = None
        self.external_tools: set[str] = set()
        # Whether `pending` names each admission's attempt, consumer and input fingerprint (newer networks do), and
        # how many admissions it lists before it is cut off.
        self.report_identity = True
        self.pending_limit = PENDING_LIMIT
        self.used_today: dict[str, int] = {}
        # Every agent.json announced or set, version n at index n - 1, and the version its publisher confirmed (0:
        # none yet). `problems` are what the next announcements are answered with.
        self.versions: list[dict[str, Any]] = []
        self.confirmed = 0
        # The version whose validation passed, which calls are admitted under; None follows the confirmed one, as
        # when every confirmed version passes its validation at once.
        self.active: int | None = None
        self.announcements: list[dict[str, Any]] = []
        self.problems: list[Problem] = []
        self.calls: dict[str, int] = {
            "admit": 0,
            "complete": 0,
            "renew": 0,
            "resolve": 0,
            "pending": 0,
            "announce": 0,
            "config": 0,
        }
        self._ids = itertools.count(1)

    # --- set-up -------------------------------------------------------------------------------------------------

    def token(self, consumer: str, *, kind: str = "customer") -> str:
        token = f"tok_{consumer}_{next(self._ids)}"
        self.grants[token] = Grant(consumer, kind)
        return token

    def revoke(self, token: str) -> None:
        self.grants[token].revoked = True

    def request_cancel(self, admission_id: str) -> None:
        self.by_admission[admission_id].cancel = "requested"

    def tick(self) -> None:
        """Leases that ran out without a completion lapse; a lapse never authorizes a second run."""
        now = self.clock()
        for execution in self.by_admission.values():
            if execution.state == "admitted" and execution.lease_expires_at < now:
                execution.state = "lapsed"

    def end_reconciliation_window(self) -> None:
        """Lapsed work settles: read-only and transformation as not_completed (released), anything the caller marks
        external as unknown (held until a resolution)."""
        for execution in self.by_admission.values():
            if execution.state != "lapsed":
                continue
            execution.state = "completed"
            if execution.tool in self.external_tools:
                execution.outcome, execution.funding = "outcome_unknown", "held"
            else:
                execution.outcome, execution.funding = "not_completed", "released"

    def runs(self, consumer: str, execution_id: str) -> int:
        """How many admissions to run one execution were ever issued: one, whatever happens."""
        return sum(1 for e in self.by_admission.values() if (e.consumer, e.execution_id) == (consumer, execution_id))

    # --- the Provider API ---------------------------------------------------------------------------------------

    def _unavailable(self) -> None:
        if self.faults.unavailable:
            raise ProviderApiError("SERVICE_UNAVAILABLE", "the network is down (injected)", retryable=True)

    def _id(self, prefix: str) -> str:
        return f"{prefix}_{next(self._ids)}"

    async def admit(self, request: AdmitRequest) -> Admission:
        self.calls["admit"] += 1
        self._unavailable()
        if request.attempt_id in self.attempts:
            return self._deliver(self.attempts[request.attempt_id])
        decision = self._decide(request)
        self.attempts[request.attempt_id] = decision
        return self._deliver(decision)

    def _deliver(self, decision: Admission) -> Admission:
        if self.faults.lose_admit_responses:
            self.faults.lose_admit_responses -= 1
            raise ProviderApiError("SERVICE_UNAVAILABLE", "response lost (injected)", retryable=True)
        if self.active_version:
            return decision.model_copy(update={"config_version": self.active_version})
        return decision

    def _refused(self, code: str, message: str, *, retryable: bool = False) -> Admission:
        return Admission(decision="refused", code=code, message=message, retryable=retryable)

    def _decide(self, request: AdmitRequest) -> Admission:  # noqa: PLR0911 - one exit per admission rule
        self.tick()
        grant = self.grants.get(request.token)
        if grant is None:
            return self._refused("UNAUTHORIZED", "unknown token")
        if grant.revoked:
            return self._refused("CONNECTION_ENDED", "the connection ended")
        rules = self.rules.get(request.tool, ToolRules())
        key = (grant.consumer, request.execution_id)
        execution = self.executions.get(key)
        if request.intent == "refresh":
            return self._refresh(request, grant, rules, execution)
        if execution is not None:
            if execution.tool != request.tool or execution.input_fingerprint != request.input_fingerprint:
                return self._refused("EXECUTION_CONFLICT", "this execution ID was used with other input")
            if execution.state == "admitted":
                return Admission(
                    decision="duplicate_running",
                    admission_id=execution.admission_id,
                    lease_expires_at=execution.lease_expires_at,
                )
            if execution.state == "lapsed" or (execution.outcome == "outcome_unknown" and execution.resolution is None):
                return self._refused("EXECUTION_LAPSED", "this execution lapsed or is unresolved")
            return Admission(
                decision="duplicate_done",
                admission_id=execution.admission_id,
                outcome=execution.outcome,
                receipt_id=str((execution.receipt or {}).get("receipt_id")),
                repeat=rules.repeat,
            )
        if not self._take_quota(grant.consumer):
            return self._refused("DAILY_LIMIT", "daily limit reached", retryable=True)
        execution = Execution(
            admission_id=self._id("adm"),
            consumer=grant.consumer,
            execution_id=request.execution_id,
            tool=request.tool,
            input_fingerprint=request.input_fingerprint,
            attempt_id=request.attempt_id,
            kind=grant.kind,
            lease_expires_at=self.clock() + self.lease_seconds,
        )
        self.executions[key] = execution
        self.by_admission[execution.admission_id] = execution
        return Admission(
            decision="run",
            admission_id=execution.admission_id,
            snapshot={
                "admission_id": execution.admission_id,
                "kind": grant.kind,
                "capability_id": request.capability_id,
                "version_executed": "1.0.0",
                "input_fingerprint": request.input_fingerprint,
                "lease_expires_at": iso(execution.lease_expires_at),
                "dry_run": grant.kind in {"validation", "trial"},
            },
            lease_expires_at=execution.lease_expires_at,
            dry_run=grant.kind in {"validation", "trial"},
        )

    def _refresh(self, request: AdmitRequest, grant: Grant, rules: ToolRules, execution: Execution | None) -> Admission:
        refresh_request_id = request.refresh_request_id or ""
        if execution is None or execution.outcome != "succeeded" or rules.repeat != "refresh":
            return self._refused("REFRESH_NOT_ALLOWED", "refresh only after a succeeded execution that offers it")
        if refresh_request_id in execution.refreshes:
            refresh_id = execution.refreshes[refresh_request_id]
            if execution.refresh_attempts[refresh_request_id] != request.attempt_id:
                # Granted to another attempt, which runs it once: nothing runs again and no allowance is taken.
                return Admission(
                    decision="refresh_used",
                    refresh_id=refresh_id,
                    refresh_of=execution.admission_id,
                    refresh_request_id=refresh_request_id,
                    refreshes_left=None
                    if rules.refreshes_per_execution is None
                    else rules.refreshes_per_execution - len(execution.refreshes),
                )
            # The same attempt asking again after a lost response: the same decision.
        else:
            limit = rules.refreshes_per_execution
            if limit is not None and len(execution.refreshes) >= limit:
                return self._refused("REFRESH_EXHAUSTED", "no refreshes left for this execution")
            if not self._take_quota(grant.consumer):
                return self._refused("DAILY_LIMIT", "daily limit reached", retryable=True)
            refresh_id = self._id("rfr")
            execution.refreshes[refresh_request_id] = refresh_id
            execution.refresh_attempts[refresh_request_id] = request.attempt_id
        left = (
            None if rules.refreshes_per_execution is None else rules.refreshes_per_execution - len(execution.refreshes)
        )
        return Admission(
            decision="refresh",
            refresh_id=refresh_id,
            refresh_of=execution.admission_id,
            refresh_request_id=refresh_request_id,
            version="1.0.0",
            dry_run=grant.kind in {"validation", "trial"},
            refreshes_left=left,
        )

    def _take_quota(self, consumer: str) -> bool:
        used = self.used_today.get(consumer, 0)
        if self.daily_limit is not None and used >= self.daily_limit:
            return False
        self.used_today[consumer] = used + 1
        return True

    async def complete(self, request: CompleteRequest) -> Completion:
        self.calls["complete"] += 1
        self._unavailable()
        self.tick()
        execution = self.by_admission.get(request.admission_id)
        if execution is None:
            raise ProviderApiError("NOT_FOUND", "no such admission", retryable=False)
        if execution.state == "completed":
            same = (execution.outcome, execution.output_fingerprint) == (request.outcome, request.output_fingerprint)
            if not same:
                raise ProviderApiError("COMPLETION_CONFLICT", "a different completion was recorded", retryable=False)
            return self._answer(Completion(receipt=dict(execution.receipt or {})))
        if execution.state == "lapsed" and self.clock() > execution.lease_expires_at + RECONCILIATION_WINDOW:
            raise ProviderApiError("EXECUTION_LAPSED", "the reconciliation window has ended", retryable=False)
        execution.state = "completed"
        execution.completions += 1
        execution.outcome = request.outcome
        execution.output_fingerprint = request.output_fingerprint
        execution.funding = self._funding(request.outcome)
        if request.outcome == "cancelled":
            execution.cancel = "confirmed"
        execution.receipt = {
            "receipt_id": self._id("rcp"),
            "admission_id": execution.admission_id,
            "execution_id": execution.execution_id,
            "outcome": request.outcome,
            "output_fingerprint": request.output_fingerprint,
            "input_fingerprint": execution.input_fingerprint,
        }
        return self._answer(Completion(receipt=dict(execution.receipt)))

    def _answer(self, completion: Completion) -> Completion:
        if self.faults.lose_complete_responses:
            self.faults.lose_complete_responses -= 1
            raise ProviderApiError("SERVICE_UNAVAILABLE", "response lost (injected)", retryable=True)
        return completion

    @staticmethod
    def _funding(outcome: str) -> str:
        """Definitive failures release; unknown outcomes stay held until a resolution."""
        if outcome == "succeeded":
            return "captured"
        if outcome == "outcome_unknown":
            return "held"
        return "released"

    async def renew(self, admission_id: str, extend_seconds: int, *, cancel_received: bool) -> Renewal:
        self.calls["renew"] += 1
        self._unavailable()
        if self.faults.fail_renewals:
            raise ProviderApiError("SERVICE_UNAVAILABLE", "renewal failed (injected)", retryable=True)
        execution = self.by_admission[admission_id]
        if cancel_received and execution.cancel == "requested":
            execution.cancel = "received"
        execution.lease_expires_at = self.clock() + min(float(extend_seconds), self.lease_seconds)
        return Renewal(lease_expires_at=execution.lease_expires_at, cancel_requested=execution.cancel == "requested")

    async def resolve(self, admission_id: str, resolved_outcome: Outcome, basis: str) -> Completion:
        self.calls["resolve"] += 1
        self._unavailable()
        execution = self.by_admission[admission_id]
        if execution.outcome != "outcome_unknown":
            raise ProviderApiError("NOT_UNKNOWN", "only an unknown outcome is resolved", retryable=False)
        execution.resolution = {"outcome": resolved_outcome, "basis": basis, "receipt_id": self._id("rcp")}
        execution.funding = self._funding(resolved_outcome)
        return Completion(receipt=dict(execution.resolution))

    @property
    def configuration(self) -> dict[str, Any] | None:
        return self.versions[self.confirmed - 1] if self.confirmed else None

    @property
    def config_version(self) -> int:
        return self.confirmed

    @property
    def active_version(self) -> int:
        return self.confirmed if self.active is None else self.active

    def _version_of(self, config: dict[str, Any]) -> int:
        """An unchanged configuration is not a new version."""
        if config in self.versions:
            return self.versions.index(config) + 1
        self.versions.append(copy.deepcopy(config))
        return len(self.versions)

    async def announce(self, endpoint: str, config: dict[str, Any]) -> Announcement:
        self.calls["announce"] += 1
        self._unavailable()
        self.announcements.append({"endpoint": endpoint, "config": copy.deepcopy(config)})
        version = self._version_of(config)
        if version == self.confirmed:
            state = "confirmed"
        elif any(p.level == "error" for p in self.problems):
            state = "invalid"
        else:
            state = "waiting_for_review"
        return Announcement(version=version, state=state, problems=list(self.problems))

    def confirm(self, version: int | None = None) -> None:
        """The publisher confirms a version, the latest announced by default; admit reports it from now on."""
        self.confirmed = version or len(self.versions)

    def set_configuration(self, configuration: dict[str, Any]) -> None:
        """The publisher confirmed this configuration, as if announced and confirmed at once."""
        self.confirm(self._version_of(configuration))

    async def config(self, version: int | None = None) -> ConfigAnswer:
        self.calls["config"] += 1
        self._unavailable()
        number = version or self.confirmed
        if version is not None and not 1 <= version <= self.confirmed:
            raise ProviderApiError("NOT_FOUND", f"no confirmed configuration version {version}", retryable=False)
        found = self.versions[number - 1] if number else None
        waiting = self.versions[-1] if version is None and len(self.versions) > self.confirmed else None
        return ConfigAnswer(
            version=number,
            config=copy.deepcopy(found) if found is not None else None,
            active_version=self.active_version,
            announced=self._as_served(waiting) if waiting is not None else None,
        )

    def _as_served(self, config: dict[str, Any]) -> dict[str, Any]:
        """An announced agent.json as the network returns it: with the endpoint it was announced from."""
        endpoint = next((a["endpoint"] for a in reversed(self.announcements) if a["config"] == config), None)
        served = copy.deepcopy(config)
        if endpoint:
            served["adapter"] = (served.get("adapter") or {}) | {"endpoint": endpoint}
        return served

    async def pending(self) -> PendingAdmissions:
        self.calls["pending"] += 1
        self._unavailable()
        self.tick()
        found = [
            PendingAdmission(
                admission_id=e.admission_id,
                execution_id=e.execution_id,
                capability_id="cap_" + e.tool,
                attempt_id=e.attempt_id if self.report_identity else None,
                consumer=e.consumer if self.report_identity else None,
                input_fingerprint=e.input_fingerprint if self.report_identity else None,
                state=e.state,
                lease_expires_at=e.lease_expires_at,
            )
            for e in self.by_admission.values()
            if e.state in {"admitted", "lapsed"}
        ]
        return PendingAdmissions(pending=found[: self.pending_limit], complete=len(found) <= self.pending_limit)


def http_app(network: FakeNetwork, credential: str) -> Any:
    """The fake network over HTTP, so the suite can drive an adapter running in its own process."""
    from starlette.applications import Starlette  # noqa: PLC0415 - only the HTTP form needs Starlette here
    from starlette.requests import Request  # noqa: PLC0415
    from starlette.responses import JSONResponse  # noqa: PLC0415
    from starlette.routing import Route  # noqa: PLC0415

    async def guarded(request: Request) -> dict[str, Any] | JSONResponse:
        if request.headers.get("authorization") != f"Bearer {credential}":
            return JSONResponse({"code": "UNAUTHORIZED", "message": "provider credential"}, status_code=401)
        body: dict[str, Any] = await request.json()
        return body

    def failure(exc: ProviderApiError) -> JSONResponse:
        status = 503 if exc.retryable else 409
        return JSONResponse({"code": exc.code, "message": str(exc), "retryable": exc.retryable}, status_code=status)

    async def configuration(request: Request) -> JSONResponse:
        if request.headers.get("authorization") != f"Bearer {credential}":
            return JSONResponse({"code": "UNAUTHORIZED", "message": "bad credential"}, status_code=401)
        asked = request.query_params.get("version")
        try:
            answer = await network.config(int(asked) if asked else None)
        except ProviderApiError as exc:
            return failure(exc)
        return JSONResponse(answer.model_dump())

    async def announce(request: Request) -> JSONResponse:
        body = await guarded(request)
        if isinstance(body, JSONResponse):
            return body
        try:
            found = await network.announce(str(body["endpoint"]), dict(body["config"]))
        except ProviderApiError as exc:
            return failure(exc)
        return JSONResponse(found.model_dump())

    async def admit(request: Request) -> JSONResponse:
        body = await guarded(request)
        if isinstance(body, JSONResponse):
            return body
        try:
            found = await network.admit(AdmitRequest.model_validate(body))
        except ProviderApiError as exc:
            return failure(exc)
        return JSONResponse(_wire(found.model_dump(exclude_none=True)))

    async def complete(request: Request) -> JSONResponse:
        body = await guarded(request)
        if isinstance(body, JSONResponse):
            return body
        try:
            found = await network.complete(CompleteRequest.model_validate(body))
        except ProviderApiError as exc:
            return failure(exc)
        return JSONResponse({"receipt": found.receipt, "state": "completed"})

    async def reconcile(request: Request) -> JSONResponse:
        body = await guarded(request)
        if isinstance(body, JSONResponse):
            return body
        try:
            match body.get("operation"):
                case "renew":
                    renewal = await network.renew(
                        body["admission_id"],
                        int(body["extend_seconds"]),
                        cancel_received=bool(body.get("cancel_received")),
                    )
                    return JSONResponse(_wire(renewal.model_dump(exclude_none=True)))
                case "resolve":
                    done = await network.resolve(body["admission_id"], body["resolved_outcome"], body["basis"])
                    return JSONResponse({"receipt": done.receipt})
                case "pending":
                    listed = await network.pending()
                    return JSONResponse(
                        {
                            "pending": [_wire(p.model_dump(exclude_none=True)) for p in listed.pending],
                            "complete": listed.complete,
                        }
                    )
        except ProviderApiError as exc:
            return failure(exc)
        return JSONResponse({"code": "INVALID_REQUEST", "message": "unknown operation"}, status_code=400)

    return Starlette(
        routes=[
            Route("/v1/provider/admit", admit, methods=["POST"]),
            Route("/v1/provider/complete", complete, methods=["POST"]),
            Route("/v1/provider/reconcile", reconcile, methods=["POST"]),
            Route("/v1/provider/config", configuration, methods=["GET"]),
            Route("/v1/provider/announce", announce, methods=["POST"]),
        ]
    )
