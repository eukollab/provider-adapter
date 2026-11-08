# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The adapter's behaviour for each call to a listed tool, independent of how tools are reached.

`attach` gives it the server's own tools, in process; the connector gives it an upstream MCP server. Either way the
core asks the network before running anything, runs the tool once per admission, and reports the outcome:
best-effort in storage-free mode, through the journal and outbox in durable mode.
"""

import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import os
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from typing import Any, Literal

import jsonschema
import mcp_types as types

from ekl_provider_adapter.api import (
    Admission,
    AdmitRequest,
    Announcement,
    CompleteRequest,
    Outcome,
    PendingAdmission,
    ProviderApi,
    ProviderApiError,
)
from ekl_provider_adapter.config import AgentFile, Capability, LocalFile
from ekl_provider_adapter.fingerprint import fingerprint, result_body
from ekl_provider_adapter.storage import ExecutionRecord, Storage, open_storage

log = logging.getLogger("ekl_provider_adapter")

ToolInvoker = Callable[[str, dict[str, Any]], Awaitable[types.CallToolResult]]
LookupResult = Literal["succeeded", "no_effect"] | None


@dataclass(frozen=True)
class OutcomeQuery:
    """What an outcome lookup is asked about. The idempotency key passed downstream is the admission ID."""

    execution_id: str
    admission_id: str
    arguments: dict[str, Any] | None


OutcomeLookup = Callable[[OutcomeQuery], Awaitable[LookupResult]]
CancellationHook = Callable[[str], Awaitable[bool]]
DAY = 86_400


class NoEffectError(Exception):
    """Raise from an external action to say it failed before anything outside happened, so the call settles as
    failed_before_effect instead of outcome_unknown."""


@dataclass
class Hooks:
    """Publisher hooks, by the names agent.json declares: `effects.outcome_lookup` and `cancellation`.

    An outcome lookup answers "did execution X take effect?" with "succeeded", "no_effect" or None (unknown); it is
    given the execution and admission IDs and, where journaled, the arguments. A cancellation hook stops the work of
    an admission and returns True once it has stopped."""

    outcome_lookup: dict[str, OutcomeLookup] = field(default_factory=dict)
    cancellation: dict[str, CancellationHook] = field(default_factory=dict)


@dataclass
class Metrics:
    """The operating figures the adapter reports for the console."""

    admissions: int = 0
    admission_seconds: float = 0.0
    refused: int = 0
    lapsed: int = 0
    completions_pending: int = 0
    oldest_pending_completion_age: float = 0.0

    @property
    def admission_latency(self) -> float:
        return self.admission_seconds / self.admissions if self.admissions else 0.0

    def as_dict(self) -> dict[str, float]:
        return {
            "admissions": self.admissions,
            "admission_latency_seconds": round(self.admission_latency, 6),
            "refused": self.refused,
            "lapsed_admissions": self.lapsed,
            "outbox_depth": self.completions_pending,
            "oldest_pending_completion_seconds": round(self.oldest_pending_completion_age, 3),
        }


@dataclass
class Settings:
    """Timings, made small by tests."""

    admit_attempts: int = 3
    admit_backoff: float = 0.5
    storage_free_complete_attempts: int = 3
    storage_free_complete_backoff: float = 5.0
    outbox_interval: float = 5.0
    lease_margin: float = 0.3  # renew when this share of the lease is left
    config_interval: float = 60.0  # how often an idle adapter checks for a new configuration
    # The network never checks the endpoint at announcement, only when its publisher opens the review and in
    # validation, so the adapter announces as it starts. While the network cannot be reached the announcement
    # is retried sooner, backing off up to config_interval.
    announce_backoff: float = 2.0
    # In a shared journal, how long past its lease (or, before admission, its last write) an entry must be before it
    # counts as abandoned: a live worker renews its lease well before it runs out.
    abandon_grace: float = 60.0


def _log_announcement(answer: Announcement) -> None:
    """The network's verdict on the announced agent.json, where the publisher deploying it looks: the log."""
    states = {
        "waiting_for_review": "waiting for its publisher to confirm it in the console",
        "confirmed": "the confirmed version",
        "invalid": "refused; fix the problems below and deploy again",
    }
    level = logging.INFO if answer.state == "confirmed" else logging.WARNING
    log.log(level, "agent.json announced as version %s: %s", answer.version, states.get(answer.state, answer.state))
    for problem in answer.problems:
        level = logging.WARNING if problem.level in {"error", "warning"} else logging.INFO
        log.log(level, "agent.json %s %s: %s", problem.level, problem.field or "(file)", problem.message)


def error_result(code: str, message: str, *, retryable: bool = False, **extra: Any) -> types.CallToolResult:
    return types.CallToolResult(
        content=[types.TextContent(type="text", text=f"{code}: {message}")],
        is_error=True,
        meta={"ekl": {"code": code, "message": message, "retryable": retryable, **extra}},
    )


def _with_meta(result: types.CallToolResult, ekl: dict[str, Any]) -> types.CallToolResult:
    meta = dict(result.meta or {})
    meta["ekl"] = {**dict(meta.get("ekl") or {}), **ekl}
    return result.model_copy(update={"meta": meta})


def _dump(result: types.CallToolResult) -> str:
    return json.dumps(result.model_dump(mode="json", by_alias=True, exclude_none=True))


def _load(text: str) -> types.CallToolResult:
    return types.CallToolResult.model_validate(json.loads(text))


class NotConfiguredError(Exception):
    """No confirmed configuration yet: its publisher has not confirmed one, or the network could not be reached."""


class AdapterCore:
    """One adapter instance. Several replicas may share a durable storage.

    `local` is the agent.json deployed with it: announced at start and whenever it changes, and the source
    of the local settings. Calls are served from the configuration its publisher confirmed (load_config),
    applied when admit reports a new version or the idle check finds one. The endpoint lists the offered tools of
    both, so the network sees an announced tool before its publisher confirms it; only the network admits calls."""

    def __init__(
        self,
        local: LocalFile | None,
        api: ProviderApi,
        invoke: ToolInvoker,
        *,
        credential: str,
        endpoint: str | None = None,
        path: str | None = None,
        storage: Storage | None = None,
        hooks: Hooks | None = None,
        tool_schemas: dict[str, dict[str, Any]] | None = None,
        settings: Settings | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.api = api
        self.local = local
        # The endpoint's path is a local setting: known before any configuration arrives.
        self.path = (
            path or (local.agent.adapter.path if local else None) or os.environ.get("EKL_ADAPTER_PATH") or "/ekl"
        )
        self.invoke = invoke
        self.hooks = hooks or Hooks()
        # Output schemas declared by the tools themselves; the configuration's take precedence.
        self.tool_schemas = tool_schemas or {}
        self.settings = settings or Settings()
        self.clock = clock
        self.metrics = Metrics()
        self._credential = credential
        self._given_endpoint = endpoint
        self._given_storage = storage
        self._agent: AgentFile | None = None
        # Without a local agent.json, the announced version waiting for review, read from the network: listed as a
        # local file's would be, so the review and the checks find its tools (C.8.2).
        self._waiting: AgentFile | None = None
        self.storage: Storage | None = None
        # The validation candidate's number, and the active version calls are admitted under. Confirmed
        # versions are kept by number, the candidate's and the active one's at least, so an admitted call runs under
        # the version it was admitted under.
        self.config_version = 0
        self.active_version = 0
        self._versions: dict[int, AgentFile] = {}
        self.announcement: Announcement | None = None
        self._announced: dict[str, Any] | None = None
        self._tasks: set[asyncio.Task[Any]] = set()

    @property
    def agent(self) -> AgentFile:
        """The validation candidate: the latest confirmed configuration, which the endpoint lists."""
        if self._agent is None:
            raise NotConfiguredError("the adapter has no confirmed configuration yet")
        return self._agent

    @property
    def configured(self) -> bool:
        return self._agent is not None

    @property
    def endpoint(self) -> str:
        """The endpoint announced and proved: given in code or EKL_ADAPTER_ENDPOINT, else agent.json's."""
        local = self.local.agent.adapter.endpoint if self.local else None
        waiting = self._waiting.adapter.endpoint if self._waiting else None
        confirmed = self._agent.adapter.endpoint if self._agent else None
        return self._given_endpoint or local or waiting or confirmed or ""

    @property
    def _announced_offered(self) -> list[Capability]:
        """What the announced agent.json offers: the local file's, else the one waiting for review on the network."""
        if self.local:
            return self.local.agent.offered
        return self._waiting.offered if self._waiting else []

    @property
    def listed(self) -> list[Capability]:
        """The tools the endpoint lists: the confirmed configuration's offered tools, then those only the announced
        agent.json offers."""
        confirmed = self._agent.offered if self._agent else []
        names = {c.tool for c in confirmed}
        return confirmed + [c for c in self._announced_offered if c.tool not in names]

    def apply_config(self, agent: AgentFile, version: int) -> None:
        """Use this confirmed configuration from now on. Refused (ValueError) when it needs something this process
        lacks: a durable store, or hooks it names; the previous configuration then stays in force."""
        storage = self._given_storage or self.storage
        if agent.execution_mode == "durable" and storage is None:
            # The store is a local setting: the network strips it from the configuration it keeps.
            url = (self.local.agent.adapter.storage if self.local else None) or os.environ.get("EKL_ADAPTER_STORAGE")
            if not url:
                raise ValueError(
                    "durable mode needs a storage: set EKL_ADAPTER_STORAGE (sqlite:///… or postgresql://…)"
                )
            storage = open_storage(url)
        missing = self._check_hooks(agent)
        if missing:
            raise ValueError("hooks named in the configuration are not registered: " + "; ".join(missing))
        self._agent = agent
        self.storage = storage if agent.execution_mode == "durable" else None
        self.config_version = version
        self._versions[version] = agent

    async def load_config(self) -> bool:
        """Fetch the validation candidate and apply it when it is new, and hold the active version too.
        Returns whether the adapter is configured afterwards."""
        fetch = getattr(self.api, "config", None)
        if fetch is None:
            return self.configured
        try:
            answer = await fetch()
        except Exception as exc:  # unreachable now: keep what we have and try again later
            log.warning("could not load the confirmed configuration from the network: %s", exc)
            return self.configured
        self.active_version = answer.active_version
        if self.local is None:
            try:
                self._waiting = AgentFile.model_validate(answer.announced) if answer.announced else None
            except ValueError as exc:
                log.warning("the announced agent.json waiting for review doesn't load here: %s", exc)
                self._waiting = None
        if answer.config is not None and not (self.configured and answer.version == self.config_version):
            try:
                self.apply_config(AgentFile.model_validate(answer.config), answer.version)
            except ValueError as exc:
                log.error("cannot use confirmed configuration version %s: %s", answer.version, exc)
        keep = {self.config_version, self.active_version}
        self._versions = {n: v for n, v in self._versions.items() if n in keep}
        if self.active_version:
            await self._version(self.active_version)
        return self.configured

    async def _version(self, number: int) -> AgentFile | None:
        """A confirmed version by its number, fetched once when not held."""
        if number in self._versions:
            return self._versions[number]
        try:
            answer = await self.api.config(number)
            found = AgentFile.model_validate(answer.config) if answer.config is not None else None
        except (ProviderApiError, ValueError) as exc:
            log.warning("could not load confirmed configuration version %s: %s", number, exc)
            return None
        if found is not None:
            self._versions[number] = found
        return found

    def _held_capability(self, tool: str) -> Capability | None:
        """The tool's capability in a held version: a call admitted under the active version may name a tool the
        candidate no longer offers."""
        for number in sorted(self._versions, reverse=True):
            cap = self._versions[number].capability(tool)
            if cap is not None:
                return cap
        return None

    def _provides(self, tool: str) -> bool:
        """Whether the server still provides the tool, as far as the adapter knows: the candidate or the local
        agent.json offers it."""
        return (self.configured and self.agent.capability(tool) is not None) or any(
            c.tool == tool for c in self._announced_offered
        )

    async def _admitted_capability(self, cap: Capability, admission: Admission) -> Capability | str:
        """The capability as the version the call was admitted under defines it, or the code of why it cannot run.
        A tool neither the candidate nor the local file offers is no longer provided, whatever the active version
        lists: the call ends before any tool runs, without effect and without charge."""
        if not self._provides(cap.tool):
            return "TOOL_WITHDRAWN"
        number = admission.config_version or self.config_version
        config = await self._version(number)
        if config is None:
            return "VERSION_UNAVAILABLE"
        return config.capability(cap.tool) or "NOT_IN_VERSION"

    # --- announcing agent.json ---------------------------------------------------------------------

    def reload_local(self) -> None:
        """Read agent.json again when its content changed. A file that no longer loads is reported and not announced;
        the previous one stays in use until it is fixed."""
        local = self.local
        if local is None or local.path is None:
            return
        try:
            text = local.path.read_text(encoding="utf-8")
        except OSError as exc:
            log.warning("cannot read %s: %s", local.path, exc)
            return
        if text == local.text:
            return
        try:
            self.local = LocalFile.read(local.path)
        except ValueError as exc:
            log.warning("%s changed but cannot be used, so it is not announced: %s", local.path, exc)
            self.local = replace(local, text=text)  # reported once, until the file changes again

    async def announce_if_changed(self) -> bool:
        """Announce agent.json when it is new or changed since the network last accepted it. Returns False only when
        the network could not be reached, so the caller tries again soon."""
        self.reload_local()
        announce = getattr(self.api, "announce", None)
        if self.local is None or announce is None:
            return True
        # Only this process's own address is announced: the one given at deployment, or agent.json's. The confirmed
        # configuration's address may belong to another deployment of the same agent, such as a run on a laptop.
        own = self._given_endpoint or self.local.agent.adapter.endpoint
        if not own:
            if self._announced is None:
                log.warning(
                    "agent.json has no adapter.endpoint: set it to this adapter's public address to announce it"
                )
                self._announced = {}
            return True
        config = self.local.config
        if self._given_endpoint and config.get("adapter", {}).get("endpoint") != self._given_endpoint:
            # The endpoint given at deployment is the one announced and proved; the file must agree with it.
            config = config | {"adapter": {**config.get("adapter", {}), "endpoint": self._given_endpoint}}
        if config == self._announced:
            return True
        try:
            answer: Announcement = await announce(own, config)
        except ProviderApiError as exc:
            log.warning("could not announce agent.json to the network: %s", exc)
            return not exc.retryable
        self._announced, self.announcement = config, answer
        _log_announcement(answer)
        return True

    async def watch_config(self) -> None:
        """Announce agent.json at start, then on every idle tick announce it again if it changed and load the
        confirmed configuration; admit reports a new confirmed version sooner when calls arrive."""
        backoff = self.settings.announce_backoff
        while True:
            reached = await self.announce_if_changed()
            await self.load_config()
            if reached:
                backoff = self.settings.announce_backoff
                await asyncio.sleep(self.settings.config_interval)
            else:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.settings.config_interval)

    def _check_hooks(self, agent: AgentFile) -> list[str]:
        missing = []
        for cap in agent.offered:
            if cap.effects.outcome_lookup and cap.effects.outcome_lookup not in self.hooks.outcome_lookup:
                missing.append(f"{cap.tool}: outcome lookup {cap.effects.outcome_lookup}")
            if cap.cancellation and cap.cancellation not in self.hooks.cancellation:
                missing.append(f"{cap.tool}: cancellation hook {cap.cancellation}")
        return missing

    # --- the endpoint challenge ------------------------------------------------------------------

    def is_provider_credential(self, presented: str) -> bool:
        return hmac.compare_digest(presented.encode("utf-8"), self._credential.encode("utf-8"))

    def answer_challenge(self, challenge: str) -> str:
        """The endpoint challenge's proof: HMAC-SHA256 keyed with the hex SHA-256 of the provider credential
        (all the network keeps of it), over the challenge hex followed by this endpoint's URL. Only the holder of this
        agent's credential, answering at this endpoint, can produce it."""
        bytes.fromhex(challenge)  # refuse anything but hex
        key = hashlib.sha256(self._credential.encode("utf-8")).hexdigest().encode("ascii")
        return hmac.new(key, (challenge + self.endpoint).encode("utf-8"), hashlib.sha256).hexdigest()

    # --- one call ---------------------------------------------------------------------------------------------

    async def handle(  # noqa: PLR0911, PLR0912 - one exit per admission decision
        self, tool: str, arguments: dict[str, Any], token: str | None, meta: dict[str, Any] | None
    ) -> types.CallToolResult:
        announced_only = (not self.configured or self.agent.capability(tool) is None) and any(
            c.tool == tool for c in self.listed
        )
        if announced_only:
            # Listed from the announced agent.json: its publisher may have confirmed it since the last check, and the
            # network validates a version right after its confirmation.
            await self.load_config()
        if not self.configured:
            return error_result("NOT_CONFIGURED", "the adapter has no confirmed configuration yet", retryable=True)
        cap = self.agent.capability(tool) or self._held_capability(tool)
        if cap is None:
            return error_result("NOT_LISTED", f"{tool} is not offered on the network")
        ekl = dict((meta or {}).get("ekl") or {})
        if not token or not ekl:
            # Nothing runs without the network's say.
            return error_result("UNAUTHORIZED", "present an access token and the execution context in _meta.ekl")
        execution_id, salt = ekl.get("execution_id"), ekl.get("salt")
        if not isinstance(execution_id, str) or not 1 <= len(execution_id) <= 128:
            return error_result("INVALID_REQUEST", "_meta.ekl.execution_id is 1-128 characters")
        try:
            input_fp = fingerprint(str(salt), arguments)
        except (TypeError, ValueError) as exc:
            return error_result("INVALID_REQUEST", f"_meta.ekl.salt or the arguments: {exc}")
        refresh_request_id = ekl.get("refresh_request_id")
        attempt_id = "att_" + uuid.uuid4().hex
        request = AdmitRequest(
            token=token,
            tool=tool,
            execution_id=execution_id,
            capability_id=str(ekl.get("capability_id") or cap.derived_capability_id),
            capability_version=ekl.get("capability_version"),
            agreement_id=ekl.get("agreement_id"),
            terms_hash=ekl.get("terms_hash"),
            execution_grant=ekl.get("execution_grant"),
            trial_session=ekl.get("trial_session"),
            input_fingerprint=input_fp,
            attempt_id=attempt_id,
            intent="refresh" if refresh_request_id else "execute",
            refresh_request_id=refresh_request_id,
        )
        record: ExecutionRecord | None = None
        if self.storage is not None:
            arguments_json = json.dumps(arguments) if cap.data.inputs.journal else None
            record = ExecutionRecord(attempt_id, execution_id, tool, input_fp, arguments_json=arguments_json)
            await asyncio.to_thread(self.storage.record_attempt, record)
        admission = await self._admit(request)
        if isinstance(admission, types.CallToolResult):
            await self._forget(attempt_id)
            return admission
        match admission.decision:
            case "run":
                return await self._run(cap, arguments, attempt_id, str(salt), admission, record)
            case "refresh":
                await self._forget(attempt_id)
                return await self._refresh(cap, arguments, admission)
            case "refresh_used":
                await self._forget(attempt_id)
                return error_result(
                    "REFRESH_USED",
                    "this refresh request was already run once and its result is not kept; ask with a new "
                    "refresh_request_id for another fresh result",
                    refresh_id=admission.refresh_id,
                    refresh_of=admission.refresh_of,
                    refresh_request_id=admission.refresh_request_id,
                )
            case "duplicate_done":
                await self._forget(attempt_id)
                return await self._repeat(cap, admission, has_refresh_request=bool(refresh_request_id))
            case "duplicate_running":
                await self._forget(attempt_id)
                # An error result: MCP clients validate any other result against the tool's output schema, and a
                # call that did not run has no output to give.
                return error_result(
                    "EXECUTION_RUNNING",
                    "this execution is still running; call again with the same ID or read execution_get",
                    retryable=True,
                    status="running",
                    admission_id=admission.admission_id,
                )
            case _:
                await self._forget(attempt_id)
                self.metrics.refused += 1
                return error_result(
                    admission.code or "REFUSED",
                    admission.message or "refused by the network",
                    retryable=admission.retryable,
                )

    async def _admit(self, request: AdmitRequest) -> Admission | types.CallToolResult:
        """Admission with the same attempt_id on every retry, so a lost response returns the same decision and never
        authorizes a second runner. Fails closed when the network cannot be reached."""
        started = self.clock()
        for attempt in range(self.settings.admit_attempts):
            try:
                admission = await self.api.admit(request)
            except ProviderApiError as exc:
                if not exc.retryable or attempt == self.settings.admit_attempts - 1:
                    return error_result(exc.code, str(exc), retryable=exc.retryable)
                await asyncio.sleep(self.settings.admit_backoff * (attempt + 1))
                continue
            self.metrics.admissions += 1
            self.metrics.admission_seconds += max(0.0, self.clock() - started)
            if admission.config_version is not None and admission.config_version > self.config_version:
                self._spawn(self.load_config())  # its publisher confirmed a newer version
            return admission
        return error_result("SERVICE_UNAVAILABLE", "the network could not be reached", retryable=True)

    async def _forget(self, attempt_id: str) -> None:
        if self.storage is not None:
            await asyncio.to_thread(self.storage.forget, attempt_id)

    def _effective_arguments(
        self, cap: Capability, arguments: dict[str, Any], admission: Admission
    ) -> dict[str, Any] | str:
        effective = dict(arguments)
        if admission.dry_run and cap.class_ == "external_action":
            if not cap.effects.dry_run_argument:
                return "DRY_RUN_UNSUPPORTED"
            effective[cap.effects.dry_run_argument] = True
        if cap.effects.idempotency_argument and admission.admission_id:
            # One admission per execution, so the same execution always carries the same key downstream.
            effective[cap.effects.idempotency_argument] = admission.admission_id
        return effective

    async def _run(
        self,
        cap: Capability,
        arguments: dict[str, Any],
        attempt_id: str,
        salt: str,
        admission: Admission,
        record: ExecutionRecord | None,
    ) -> types.CallToolResult:
        admission_id = str(admission.admission_id)
        if self.storage is not None and record is not None:
            marked = await asyncio.to_thread(
                self.storage.mark_running,
                attempt_id,
                admission_id,
                admission.lease_expires_at,
                admission.dry_run,
                record.updated_at,
            )
            if not marked:
                # Recovery took this attempt over while the worker was paused: it settles the admission, and the
                # tool must not run.
                return error_result(
                    "UNAVAILABLE",
                    f"{cap.tool} was not run: recovery took this attempt over before it started",
                    admission_id=admission_id,
                    outcome="failed_before_effect",
                )
        admitted = await self._admitted_capability(cap, admission)
        if isinstance(admitted, str):
            await self._settle(cap, attempt_id, admission_id, "failed_before_effect", None, None, error_code=admitted)
            return error_result(
                "UNAVAILABLE",
                f"{cap.tool} is not available under the version this call was admitted under ({admitted})",
                admission_id=admission_id,
                outcome="failed_before_effect",
            )
        cap = admitted
        if cap.input_schema:
            # The arguments must fit the version the call was admitted under, whatever the server's code accepts now.
            try:
                jsonschema.validate(arguments, cap.input_schema)
            except jsonschema.ValidationError as exc:
                await self._settle(
                    cap, attempt_id, admission_id, "failed_before_effect", None, None, error_code="INVALID_REQUEST"
                )
                return error_result(
                    "INVALID_REQUEST",
                    f"the arguments don't fit {cap.tool} as the version this call was admitted under declares it: "
                    f"{exc.message}",
                    admission_id=admission_id,
                    outcome="failed_before_effect",
                )
        effective = self._effective_arguments(cap, arguments, admission)
        if isinstance(effective, str):
            await self._settle(cap, attempt_id, admission_id, "failed_before_effect", None, None, error_code=effective)
            return error_result(effective, "this capability has no genuine dry run", admission_id=admission_id)
        work = asyncio.ensure_future(self.invoke(cap.tool, effective))
        lease = asyncio.ensure_future(self._hold_lease(cap, admission_id, admission.lease_expires_at, work))
        try:
            outcome, result = await self._outcome(cap, work)
        finally:
            lease.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await lease
        if lease.done() and not lease.cancelled() and lease.result() is True:
            # The hook confirmed the work stopped: whatever the tool returned is not the completed work.
            outcome = "cancelled"
        output_fp = fingerprint(salt, result_body(result)) if outcome == "succeeded" and result is not None else None
        keep = result if outcome == "succeeded" and cap.repeat == "return_kept" else None
        await self._settle(cap, attempt_id, admission_id, outcome, output_fp, keep)
        if result is None:
            return error_result(
                outcome.upper(), "the tool did not complete", admission_id=admission_id, outcome=outcome
            )
        return _with_meta(result, {"outcome": outcome, "admission_id": admission_id})

    async def _outcome(  # noqa: PLR0911 - one exit per outcome rule
        self, cap: Capability, work: "asyncio.Future[types.CallToolResult]"
    ) -> tuple[Outcome, types.CallToolResult | None]:
        """What the tool's behaviour means for this class of capability."""
        external = cap.class_ == "external_action"
        try:
            result = await work
        except asyncio.CancelledError:
            return ("outcome_unknown" if external else "failed_before_effect"), None
        except NoEffectError:
            return "failed_before_effect", None
        except Exception:  # the publisher's tool may raise anything; the caller gets an outcome, not a crash
            log.exception("tool %s raised", cap.tool)
            return ("outcome_unknown" if external else "failed_before_effect"), None
        if result.is_error:
            declared = dict((result.meta or {}).get("ekl") or {}).get("effect")
            if not external or declared == "none":
                return "failed_before_effect", result
            return "outcome_unknown", result
        schema = cap.output_schema or self.tool_schemas.get(cap.tool)
        if schema is not None:
            try:
                jsonschema.validate(result_body(result), schema)
            except jsonschema.ValidationError:
                return ("outcome_unknown" if external else "failed_before_effect"), result
        return "succeeded", result

    async def _settle(
        self,
        cap: Capability,
        attempt_id: str,
        admission_id: str,
        outcome: Outcome,
        output_fp: str | None,
        keep: types.CallToolResult | None,
        *,
        error_code: str | None = None,
    ) -> None:
        completion = CompleteRequest(
            admission_id=admission_id, outcome=outcome, output_fingerprint=output_fp, error_code=error_code
        )
        if self.storage is not None:
            # Outcome and outbox entry in one transaction, before the reply: from here a crash cannot lose it.
            await asyncio.to_thread(
                self.storage.finish,
                attempt_id,
                outcome,
                output_fp,
                _dump(keep) if keep is not None else None,
                completion.model_dump(exclude_none=True),
            )
            self._spawn(self.deliver_outbox())
        else:
            self._spawn(self._complete_best_effort(completion))
        if outcome == "outcome_unknown":
            self._spawn(self._look_up(cap, admission_id))

    async def _complete_best_effort(self, completion: CompleteRequest) -> None:
        """Storage-free mode: a few tries while the process runs. A lost completion lapses."""
        for attempt in range(self.settings.storage_free_complete_attempts):
            try:
                await self.api.complete(completion)
                return
            except ProviderApiError as exc:
                if not exc.retryable:
                    log.warning("completion of %s refused: %s", completion.admission_id, exc)
                    return
                await asyncio.sleep(self.settings.storage_free_complete_backoff * (attempt + 1))
        log.warning("completion of %s not delivered; the admission will lapse", completion.admission_id)

    async def _look_up(self, cap: Capability, admission_id: str) -> None:
        """An unknown outcome is resolved through the publisher's lookup when one is declared; otherwise it stays
        unknown and the network escalates it."""
        name = cap.effects.outcome_lookup
        if not name:
            return
        record = await asyncio.to_thread(self.storage.by_admission, admission_id) if self.storage else None
        arguments = json.loads(record.arguments_json) if record and record.arguments_json else None
        execution_id = record.execution_id if record else admission_id
        try:
            found = await self.hooks.outcome_lookup[name](OutcomeQuery(execution_id, admission_id, arguments))
        except Exception:  # a failing lookup leaves the outcome unknown, as declared
            log.exception("outcome lookup %s failed", name)
            return
        if found is None:
            return
        resolved: Outcome = "succeeded" if found == "succeeded" else "failed_before_effect"
        await self.deliver_outbox()  # the completion must be recorded before a resolution is appended to it
        with contextlib.suppress(ProviderApiError):
            await self.api.resolve(admission_id, resolved, "lookup")

    async def _hold_lease(
        self, cap: Capability, admission_id: str, expires_at: float | None, work: "asyncio.Future[Any]"
    ) -> bool:
        """Keep the lease while the tool works, and act on cancellation. Returns True if the work was
        cancelled through the capability's hook."""
        if expires_at is None:
            return False
        started = self.clock()
        cancel_received = False
        local_expiry = expires_at
        while not work.done():
            remaining = local_expiry - self.clock()
            await asyncio.sleep(max(0.01, remaining * (1 - self.settings.lease_margin)))
            if work.done():
                return False
            if self.clock() - started >= cap.max_duration_seconds:
                return False
            try:
                renewal = await self.api.renew(admission_id, cap.max_duration_seconds, cancel_received=cancel_received)
            except ProviderApiError:
                if self.clock() >= local_expiry:
                    # The network is unreachable and the lease has run out locally: stop where the capability can.
                    self.metrics.lapsed += 1
                    return await self._cancel(cap, admission_id, work)
                continue
            if renewal.lease_expires_at:
                local_expiry = renewal.lease_expires_at
                if self.storage is not None:
                    await asyncio.to_thread(self.storage.renew_lease, admission_id, local_expiry)
            if renewal.cancel_requested and not cancel_received:
                cancel_received = True
                # Acknowledge first (received), then stop the work (confirmed by completing as cancelled).
                with contextlib.suppress(ProviderApiError):
                    await self.api.renew(admission_id, cap.max_duration_seconds, cancel_received=True)
                return await self._cancel(cap, admission_id, work)
        return False

    async def _cancel(self, cap: Capability, admission_id: str, work: "asyncio.Future[Any]") -> bool:
        if not cap.cancellation:
            return False  # not cancellable: the work completes and is reported
        stopped = await self.hooks.cancellation[cap.cancellation](admission_id)
        if stopped:
            work.cancel()
        return stopped

    async def _refresh(self, cap: Capability, arguments: dict[str, Any], admission: Admission) -> types.CallToolResult:
        """A fresh result, outside the original receipt, never completed or charged."""
        effective = self._effective_arguments(cap, arguments, admission)
        if isinstance(effective, str):
            return error_result(effective, "this capability has no genuine dry run")
        try:
            result = await self.invoke(cap.tool, effective)
        except Exception as exc:  # a failed refresh is just a failed fresh result; nothing is recorded
            return error_result("REFRESH_FAILED", str(exc))
        return _with_meta(
            result,
            {
                "refresh": True,
                "refresh_of": admission.refresh_of,
                "refresh_request_id": admission.refresh_request_id,
                "refresh_id": admission.refresh_id,
            },
        )

    async def _repeat(
        self, cap: Capability, admission: Admission, *, has_refresh_request: bool
    ) -> types.CallToolResult:
        status: dict[str, Any] = {
            "outcome": admission.outcome,
            "admission_id": admission.admission_id,
            "receipt_id": admission.receipt_id,
        }
        repeat = admission.repeat or cap.repeat
        if repeat == "return_kept" and self.storage is not None and admission.admission_id:
            record = await asyncio.to_thread(self.storage.by_admission, admission.admission_id)
            if record is not None and record.result_json:
                return _with_meta(_load(record.result_json), {**status, "replay": True})
        if repeat == "refresh" and not has_refresh_request:
            status["refresh_available"] = True
        return error_result("RESULT_UNAVAILABLE", "the result of this execution is not kept", **status)

    # --- background work ---------------------------------------------------------------------------------------

    def _spawn(self, coroutine: Awaitable[Any]) -> None:
        task = asyncio.ensure_future(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def drain(self) -> None:
        """Wait for background completions and lookups; used at shutdown and by tests."""
        while running := [task for task in self._tasks if not task.done()]:
            await asyncio.gather(*running, return_exceptions=True)
            # Let done-callbacks run: a background task may have queued another (an outbox round, a lookup).
            await asyncio.sleep(0)

    async def deliver_outbox(self) -> int:
        """Durable mode: deliver queued completions until acknowledged, idempotently by admission ID."""
        if self.storage is None:
            return 0
        delivered = 0
        entries = await asyncio.to_thread(self.storage.outbox)
        for entry in entries:
            try:
                completion = await self.api.complete(CompleteRequest.model_validate(entry.payload))
            except ProviderApiError as exc:
                await asyncio.to_thread(self.storage.note_delivery_attempt, entry.admission_id)
                if exc.code == "COMPLETION_CONFLICT":
                    log.error("completion of %s conflicts with the network's record", entry.admission_id)
                continue
            await asyncio.to_thread(self.storage.acknowledge, entry.admission_id, completion.receipt_id)
            delivered += 1
        await self.refresh_metrics()
        return delivered

    async def refresh_metrics(self) -> None:
        if self.storage is None:
            return
        entries = await asyncio.to_thread(self.storage.outbox)
        self.metrics.completions_pending = len(entries)
        self.metrics.oldest_pending_completion_age = (
            max(0.0, self.clock() - min(e.created_at for e in entries)) if entries else 0.0
        )

    async def recover(self) -> None:
        """On start: resend completions, then settle executions whose worker has stopped."""
        if self.storage is None or not self.configured:
            return
        await self.deliver_outbox()
        await self.settle_abandoned(at_start=True)

    def _abandoned(self, record: ExecutionRecord, *, at_start: bool) -> bool:
        """Whether the worker of an unfinished entry has stopped. A journal exclusive to this process holds, at start,
        only entries of a process that has ended. In a shared journal another replica may still be working on it, so
        the entry counts only once its lease, or before admission its last write, is past by the grace."""
        if at_start and self.storage is not None and self.storage.exclusive:
            return True
        since = record.lease_expires_at if record.state == "running" and record.lease_expires_at else record.updated_at
        return self.clock() > since + self.settings.abandon_grace

    async def settle_abandoned(self, *, at_start: bool = False) -> None:
        """Settle executions whose worker stopped: read-only and transformation work as failed before effect (it was
        abandoned and its result cannot be recovered); external actions as unknown, then through their lookup. The
        network's pending admissions are read first, and nothing is ever run again."""
        if self.storage is None or not self.configured:
            return
        unfinished = [
            r for r in await asyncio.to_thread(self.storage.unfinished) if self._abandoned(r, at_start=at_start)
        ]
        if not unfinished:
            return
        try:
            listed = await self.api.pending()
        except ProviderApiError:
            return  # try again at the next start or outbox round
        by_attempt: dict[str, list[PendingAdmission]] = {}
        by_execution: dict[str, list[PendingAdmission]] = {}
        for found in listed.pending:
            if found.attempt_id is not None:
                by_attempt.setdefault(found.attempt_id, []).append(found)
            by_execution.setdefault(found.execution_id, []).append(found)
        for record in unfinished:
            cap = self.agent.capability(record.tool)
            if cap is None:
                continue
            claimed = await asyncio.to_thread(self.storage.claim, record.attempt_id, record.updated_at)
            if claimed is None:
                continue  # its worker wrote to it after all, or another replica is settling it
            admission_id = record.admission_id
            if admission_id is None:
                # Journaled before admit, so the tool never ran: if the network admitted it, the response was lost.
                matches = self._lost_admission(
                    record, cap.derived_capability_id, by_attempt, by_execution, listed.complete
                )
                if not matches:
                    # None is undecided; an empty list from a list cut off at its limit may hold it further on. Either
                    # way the entry is kept for the next round, and dropped only when a whole list has no match.
                    if matches is not None and listed.complete:
                        await asyncio.to_thread(self.storage.forget, record.attempt_id)
                    continue
                if len(matches) > 1:
                    # Two consumers used the same execution ID: leave both to lapse rather than guess.
                    log.warning("cannot tell which admission of %s was lost; it will lapse", record.execution_id)
                    continue
                admission_id = matches[0].admission_id
                await asyncio.to_thread(
                    self.storage.mark_running,
                    record.attempt_id,
                    admission_id,
                    matches[0].lease_expires_at,
                    False,
                    claimed,
                )
            outcome: Outcome = "outcome_unknown" if cap.class_ == "external_action" else "failed_before_effect"
            await self._settle(cap, record.attempt_id, admission_id, outcome, None, None, error_code="INTERRUPTED")
        await self.drain()

    @staticmethod
    def _lost_admission(
        record: ExecutionRecord,
        capability_id: str,
        by_attempt: dict[str, list[PendingAdmission]],
        by_execution: dict[str, list[PendingAdmission]],
        complete: bool,
    ) -> list[PendingAdmission] | None:
        """The pending admissions that may be the one whose admit response this entry lost, or None
        when that cannot be decided yet."""
        if by_attempt:
            # A network that reports attempt IDs names the admission exactly: the attempt ID is this adapter's own.
            return by_attempt.get(record.attempt_id, [])
        found = [
            p
            for p in by_execution.get(record.execution_id, [])
            # All three must be reported and equal: the fingerprint is salted per execution by its consumer, so it
            # tells apart attempts that share an execution ID. A network that omits them gives no match, and the
            # attempt is left to lapse rather than adopted on the ID alone.
            if p.capability_id == capability_id and p.input_fingerprint == record.input_fingerprint
        ]
        # Without attempt IDs a single match is unique only in a whole list: another may lie beyond the cut.
        return None if len(found) == 1 and not complete else found

    def purge(self) -> int:
        """Drop kept results and old journal entries after their retention windows."""
        if self.storage is None or not self.configured:
            return 0
        now = self.clock()
        settings = self.agent.adapter
        return self.storage.purge(
            now - settings.result_retention_days * DAY, now - settings.execution_id_retention_days * DAY
        )

    async def run_outbox(self) -> None:
        """Background delivery loop for durable mode, with periodic purges."""
        while True:
            try:
                await self.deliver_outbox()
                await self.settle_abandoned()
                await asyncio.to_thread(self.purge)
            except Exception:  # keep delivering; a broken round is logged and retried
                log.exception("outbox round failed")
            await asyncio.sleep(self.settings.outbox_interval)
