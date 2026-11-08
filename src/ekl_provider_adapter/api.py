# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The network's Provider API, as the adapter calls it.

Every call carries the provider credential. The adapter sends identifiers, salted fingerprints and outcomes only;
call arguments and results never leave the publisher's server.
"""

import datetime as dt
from typing import Annotated, Any, Literal, Protocol

import httpx2
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field

Decision = Literal["run", "refresh", "refresh_used", "duplicate_done", "duplicate_running", "refused"]
Outcome = Literal["succeeded", "failed_before_effect", "outcome_unknown", "cancelled", "not_completed"]


class _Message(BaseModel):
    # Newer networks may add fields; the adapter reads only what it knows.
    model_config = ConfigDict(extra="ignore")


def epoch(value: Any) -> float | None:
    """Lease times arrive as RFC 3339 date-times; the adapter compares them with time.time()."""
    if value is None or isinstance(value, int | float):
        return None if value is None else float(value)
    return dt.datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()


class AdmitRequest(_Message):
    """The execution context without its salt, which never reaches EuKollab. Idempotent by `attempt_id`."""

    token: str
    tool: str
    execution_id: str
    capability_id: str
    capability_version: str | None = None
    agreement_id: str | None = None
    terms_hash: str | None = None
    execution_grant: str | None = None
    trial_session: str | None = None
    input_fingerprint: str
    attempt_id: str
    intent: Literal["execute", "refresh"] = "execute"
    refresh_request_id: str | None = None


# Lease times travel as RFC 3339 date-times; the adapter compares them with time.time().
LeaseTime = Annotated[float | None, BeforeValidator(epoch)]


class _Leased(_Message):
    lease_expires_at: LeaseTime = None


class Admission(_Leased):
    """The network's decision on one attempt. Which fields are set depends on the decision, grouped below;
    only `run` and `refresh` let the tool run."""

    decision: Decision
    admission_id: str | None = None
    snapshot: dict[str, Any] | None = None
    dry_run: bool = False
    # refresh
    refresh_id: str | None = None
    refresh_of: str | None = None
    refresh_request_id: str | None = None
    version: str | None = None
    refreshes_left: int | None = None
    # The agent.json version the call is admitted under: the adapter executes it under that version.
    config_version: int | None = None
    # duplicate_done
    outcome: Outcome | None = None
    receipt_id: str | None = None
    repeat: Literal["return_kept", "status_only", "refresh"] | None = None
    # refused
    code: str | None = None
    message: str | None = None
    retryable: bool = False


class CompleteRequest(_Message):
    admission_id: str
    outcome: Outcome
    output_fingerprint: str | None = None
    error_code: str | None = None


class Completion(_Message):
    """The answer to `complete` or `resolve`: the receipt EuKollab signed. Refusals raise ProviderApiError."""

    receipt: dict[str, Any] = Field(default_factory=dict)
    code: str | None = None

    @property
    def receipt_id(self) -> str | None:
        found = self.receipt.get("receipt_id")
        return str(found) if found else None


class Renewal(_Leased):
    """A renewed lease. `cancel_requested` asks the adapter to stop the work; it acknowledges with
    `cancel_received` on its next renewal."""

    cancel_requested: bool = False
    code: str | None = None


class PendingAdmission(_Leased):
    """One of the provider's admissions still admitted or lapsed, listed by `reconcile pending` for recovery."""

    admission_id: str
    execution_id: str
    capability_id: str | None = None
    # Newer networks name the attempt, the consumer and the input fingerprint, so a lost admit response is matched
    # exactly.
    attempt_id: str | None = None
    consumer: str | None = None
    input_fingerprint: str | None = None
    state: str


# The network lists at most this many pending admissions at once.
PENDING_LIMIT = 500


class PendingAdmissions(_Message):
    """The answer to `reconcile pending`. `complete` is false when the list was cut off at its limit, so an entry
    without a match in it may still have been admitted."""

    pending: list[PendingAdmission] = Field(default_factory=list)
    complete: bool


class Problem(_Message):
    """One thing the network found in an announced agent.json, by its field."""

    field: str = ""
    message: str
    level: str = "error"


class Announcement(_Message):
    """The network's answer to an announcement: the version it keeps the file as, and its problems."""

    version: int
    state: str  # waiting_for_review, confirmed or invalid
    problems: list[Problem] = Field(default_factory=list)


class ConfigAnswer(_Message):
    """A confirmed configuration: the validation candidate, or the version asked for by number, with the
    active version, which the network lists and admits calls under."""

    version: int = 0
    config: dict[str, Any] | None = None
    active_version: int = 0
    # The announced agent.json waiting for review, which an adapter deployed without its own file lists.
    announced: dict[str, Any] | None = None


class ProviderApiError(Exception):
    """The network refused a request or could not be reached. `retryable` says whether trying again may help."""

    def __init__(self, code: str, message: str, *, retryable: bool) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.retryable = retryable


class ProviderApi(Protocol):
    """What the adapter needs from the network. The HTTP client implements it; the conformance suite fakes it."""

    async def admit(self, request: AdmitRequest) -> Admission: ...

    async def complete(self, request: CompleteRequest) -> Completion: ...

    async def renew(self, admission_id: str, extend_seconds: int, *, cancel_received: bool) -> Renewal: ...

    async def resolve(self, admission_id: str, resolved_outcome: Outcome, basis: str) -> Completion: ...

    async def pending(self) -> PendingAdmissions: ...

    async def announce(self, endpoint: str, config: dict[str, Any]) -> Announcement: ...

    async def config(self, version: int | None = None) -> ConfigAnswer: ...


class HttpProviderApi:
    """The Provider API over HTTPS with the provider credential."""

    def __init__(self, network_url: str, credential: str, *, http: httpx2.AsyncClient | None = None) -> None:
        self._base = network_url.rstrip("/")
        self._http = http or httpx2.AsyncClient(timeout=10)
        self._headers = {"Authorization": f"Bearer {credential}"}

    async def _post(self, path: str, body: dict[str, Any] | None) -> dict[str, Any]:
        try:
            if body is None:
                response = await self._http.get(f"{self._base}{path}", headers=self._headers)
            else:
                response = await self._http.post(f"{self._base}{path}", json=body, headers=self._headers)
        except httpx2.HTTPError as exc:
            raise ProviderApiError("SERVICE_UNAVAILABLE", f"the network is unreachable: {exc}", retryable=True) from exc
        try:
            data: dict[str, Any] = response.json()
        except ValueError as exc:
            raise ProviderApiError("SERVICE_UNAVAILABLE", "the network answered without JSON", retryable=True) from exc
        if response.status_code >= 500:
            raise ProviderApiError(
                str(data.get("code", "SERVICE_UNAVAILABLE")), str(data.get("message", "")), retryable=True
            )
        if response.status_code >= 400:
            raise ProviderApiError(
                str(data.get("code", "REFUSED")), str(data.get("message", "")), retryable=bool(data.get("retryable"))
            )
        return data

    async def admit(self, request: AdmitRequest) -> Admission:
        return Admission.model_validate(await self._post("/v1/provider/admit", request.model_dump(exclude_none=True)))

    async def complete(self, request: CompleteRequest) -> Completion:
        found = await self._post("/v1/provider/complete", request.model_dump(exclude_none=True))
        return Completion.model_validate(found if "receipt" in found else {"receipt": found})

    async def renew(self, admission_id: str, extend_seconds: int, *, cancel_received: bool) -> Renewal:
        body = {"operation": "renew", "admission_id": admission_id, "extend_seconds": extend_seconds}
        if cancel_received:
            body["cancel_received"] = True
        return Renewal.model_validate(await self._post("/v1/provider/reconcile", body))

    async def resolve(self, admission_id: str, resolved_outcome: Outcome, basis: str) -> Completion:
        body = {
            "operation": "resolve",
            "admission_id": admission_id,
            "resolved_outcome": resolved_outcome,
            "basis": basis,
        }
        found = await self._post("/v1/provider/reconcile", body)
        return Completion.model_validate(found if "receipt" in found else {"receipt": found})

    async def pending(self) -> PendingAdmissions:
        found = await self._post("/v1/provider/reconcile", {"operation": "pending"})
        listed = [PendingAdmission.model_validate(p) for p in found.get("pending", [])]
        complete = found.get("complete")
        if complete is None:
            # An older network does not say, but cuts its list at the same limit: a shorter list holds everything,
            # as the adapter has always assumed, and only a full one may have been cut off.
            complete = len(listed) < PENDING_LIMIT
        return PendingAdmissions(pending=listed, complete=bool(complete))

    async def announce(self, endpoint: str, config: dict[str, Any]) -> Announcement:
        found = await self._post("/v1/provider/announce", {"endpoint": endpoint, "config": config})
        return Announcement.model_validate(found)

    async def config(self, version: int | None = None) -> ConfigAnswer:
        """The validation candidate, or the confirmed version with this number; version 0 and no
        configuration before the first confirmation."""
        found = await self._post("/v1/provider/config" + (f"?version={version}" if version else ""), None)
        return ConfigAnswer.model_validate(found)
