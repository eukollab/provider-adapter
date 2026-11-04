# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Drive an adapter through the conformance scenarios of the adapter profile (EKP §15).

A Harness wires the package's adapter core to the fake network and the reference toolbox, in storage-free or
durable mode, and can "crash" the adapter (drop the core and build a new one on the same storage) at any point.
Scenarios are plain async functions that raise AssertionError when the adapter breaks a rule.
"""

import asyncio
import contextlib
import json
import tempfile
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import mcp_types as types

from ekl_provider_adapter.api import CompleteRequest, Completion, ProviderApiError
from ekl_provider_adapter.attach import tool_error_result
from ekl_provider_adapter.config import AgentFile, LocalFile, strip_local
from ekl_provider_adapter.conformance.network import FakeNetwork, ToolRules
from ekl_provider_adapter.conformance.toolbox import ENDPOINT, Crash, Toolbox, durable_agent, storage_free_agent
from ekl_provider_adapter.core import AdapterCore, Hooks, OutcomeQuery, Settings
from ekl_provider_adapter.fingerprint import fingerprint, new_salt, result_body
from ekl_provider_adapter.storage import SQLiteStorage

CREDENTIAL = "eklp_conformance_credential"
FAST = Settings(
    admit_attempts=3,
    admit_backoff=0.0,
    storage_free_complete_attempts=2,
    storage_free_complete_backoff=0.0,
    outbox_interval=0.05,
    lease_margin=0.5,
)


@dataclass
class Call:
    """One consumer request, so a scenario can repeat it exactly."""

    tool: str
    arguments: dict[str, Any]
    execution_id: str
    salt: str = field(default_factory=new_salt)
    refresh_request_id: str | None = None

    def meta(self) -> dict[str, Any]:
        ekl: dict[str, Any] = {"execution_id": self.execution_id, "salt": self.salt}
        if self.refresh_request_id:
            ekl["refresh_request_id"] = self.refresh_request_id
        return {"ekl": ekl}


class CrashingApi:
    """The network, with a crash injected at the next call of one operation."""

    def __init__(self, network: FakeNetwork) -> None:
        self.network = network
        self.crash_on: str | None = None

    async def admit(self, request: Any) -> Any:
        return await self.network.admit(request)

    async def complete(self, request: CompleteRequest) -> Completion:
        if self.crash_on == "complete:before":
            self.crash_on = None
            raise Crash("complete:before")
        return await self.network.complete(request)

    async def renew(self, admission_id: str, extend_seconds: int, *, cancel_received: bool) -> Any:
        return await self.network.renew(admission_id, extend_seconds, cancel_received=cancel_received)

    async def resolve(self, admission_id: str, resolved_outcome: Any, basis: str) -> Any:
        return await self.network.resolve(admission_id, resolved_outcome, basis)

    async def pending(self) -> Any:
        return await self.network.pending()

    async def announce(self, endpoint: str, config: dict[str, Any]) -> Any:
        return await self.network.announce(endpoint, config)

    async def config(self, version: int | None = None) -> Any:
        return await self.network.config(version)


class CrashingStorage(SQLiteStorage):
    """SQLite with a crash injected right after a chosen write."""

    crash_after: str | None = None

    def acknowledge(self, admission_id: str, receipt_id: str | None) -> None:
        if self.crash_after == "acknowledge:before":
            self.crash_after = None
            raise Crash("acknowledge:before")
        super().acknowledge(admission_id, receipt_id)


class Harness:
    """One adapter under test in one execution mode, with the fake network, the toolbox and a consumer token.

    `call` sends a consumer request through the core; `restart` stands for a crash and a new process on the same
    journal. Each scenario gets a fresh Harness.
    """

    def __init__(self, mode: str, *, lease_seconds: float = 60.0) -> None:
        self.mode = mode
        self.network = FakeNetwork(lease_seconds=lease_seconds)
        self.network.external_tools = {"send", "archive"}
        self.network.rules = {
            "lookup": ToolRules(repeat="refresh", refreshes_per_execution=2),
            "transform": ToolRules(repeat="status_only", paid=True),
            "keep": ToolRules(repeat="return_kept", paid=True),
            "send": ToolRules(repeat="return_kept"),
        }
        self.toolbox = Toolbox()
        self.api = CrashingApi(self.network)
        self.mode = mode
        # The agent.json deployed with the adapter, which its publisher has confirmed; a scenario may change either.
        self.local: LocalFile | None = LocalFile.read(durable_agent() if mode == "durable" else storage_free_agent())
        self.network.set_configuration(self.local.config)
        self._dir = tempfile.TemporaryDirectory()
        self.storage = CrashingStorage(str(Path(self._dir.name) / "journal.db")) if mode == "durable" else None
        self.token = self.network.token("agt_consumer")
        self.core = self.build()

    def build(self) -> AdapterCore:
        server = self.toolbox.server

        async def invoke(tool: str, arguments: dict[str, Any]) -> types.CallToolResult:
            from mcp.server.mcpserver.exceptions import ToolError  # noqa: PLC0415 - kept beside its only use

            try:
                result = await server.call_tool(tool, arguments)
            except ToolError as exc:
                return tool_error_result(exc)
            assert isinstance(result, types.CallToolResult)
            return result

        hooks = Hooks(
            outcome_lookup={"sent_lookup": self.toolbox.sent_lookup},
            cancellation={"stop_slow": self.toolbox.stop_slow},
        )
        core = AdapterCore(
            self.local,
            self.api,
            invoke,
            credential=CREDENTIAL,
            endpoint=ENDPOINT,
            storage=self.storage,
            hooks=hooks,
            settings=FAST,
        )
        if self.network.configuration is not None:
            # What load_config does at start, without awaiting it.
            core.apply_config(AgentFile.model_validate(self.network.configuration), self.network.config_version)
        return core

    async def call(self, call: Call, *, token: str | None = None) -> types.CallToolResult:
        return await self.core.handle(call.tool, call.arguments, token or self.token, call.meta())

    async def restart(self) -> None:
        """The process stopped: a new core on the same storage, recovering as on start."""
        self.core = self.build()
        await self.core.recover()

    def execution(self, call: Call) -> Any:
        return self.network.executions[("agt_consumer", call.execution_id)]

    def close(self) -> None:
        self._dir.cleanup()


def ekl(result: types.CallToolResult) -> dict[str, Any]:
    """What the adapter reports about a call, in the result's `_meta.ekl`."""
    return dict((result.meta or {}).get("ekl") or {})


Scenario = Callable[[Harness], Awaitable[None]]
SCENARIOS: dict[str, tuple[set[str], Scenario]] = {}


def scenario(*modes: str) -> Callable[[Scenario], Scenario]:
    """Register a scenario for the execution modes whose rules it tests; `run_all` runs it once per mode."""

    def register(fn: Scenario) -> Scenario:
        SCENARIOS[fn.__name__] = (set(modes), fn)
        return fn

    return register


# --- access ---------------------------------------------------------------------------------------------------------


@scenario("storage_free", "durable")
async def nothing_runs_without_token_or_context(h: Harness) -> None:
    result = await h.core.handle("lookup", {"area": "x"}, None, Call("lookup", {"area": "x"}, "e1").meta())
    assert ekl(result)["code"] == "UNAUTHORIZED"
    result = await h.core.handle("lookup", {"area": "x"}, h.token, None)
    assert ekl(result)["code"] == "UNAUTHORIZED"
    assert h.network.calls["admit"] == 0 and h.toolbox.lookups == 0


@scenario("storage_free", "durable")
async def refused_calls_never_run(h: Harness) -> None:
    result = await h.call(Call("lookup", {"area": "x"}, "e1"), token="tok_unknown")
    assert ekl(result)["code"] == "UNAUTHORIZED" and h.toolbox.lookups == 0
    h.network.revoke(h.token)
    result = await h.call(Call("lookup", {"area": "x"}, "e2"))
    assert ekl(result)["code"] == "CONNECTION_ENDED" and h.toolbox.lookups == 0


@scenario("storage_free", "durable")
async def unreachable_network_fails_closed(h: Harness) -> None:
    h.network.faults.unavailable = True
    result = await h.call(Call("lookup", {"area": "x"}, "e1"))
    assert ekl(result)["code"] == "SERVICE_UNAVAILABLE" and ekl(result)["retryable"] is True
    assert h.toolbox.lookups == 0


# --- one call --------------------------------------------------------------------------------------------------------


@scenario("storage_free", "durable")
async def a_call_runs_once_and_completes(h: Harness) -> None:
    call = Call("lookup", {"area": "dublin"}, "e1")
    result = await h.call(call)
    await h.core.drain()
    assert ekl(result)["outcome"] == "succeeded"
    execution = h.execution(call)
    assert execution.state == "completed" and execution.completions == 1
    assert execution.output_fingerprint == fingerprint(call.salt, result_body(result))
    assert execution.input_fingerprint == fingerprint(call.salt, call.arguments)


@scenario("storage_free", "durable")
async def the_same_id_with_other_input_conflicts(h: Harness) -> None:
    await h.call(Call("lookup", {"area": "a"}, "e1"))
    result = await h.call(Call("lookup", {"area": "b"}, "e1"))
    assert ekl(result)["code"] == "EXECUTION_CONFLICT" and h.toolbox.lookups == 1


@scenario("storage_free", "durable")
async def a_lost_admit_response_is_retried_without_a_second_runner(h: Harness) -> None:
    h.network.faults.lose_admit_responses = 1
    call = Call("lookup", {"area": "x"}, "e1")
    result = await h.call(call)
    assert ekl(result)["outcome"] == "succeeded"
    assert h.network.runs("agt_consumer", "e1") == 1 and h.toolbox.lookups == 1


@scenario("storage_free", "durable")
async def concurrent_duplicates_run_once(h: Harness) -> None:
    call = Call("slow", {"seconds": 5}, "e1")
    first = asyncio.ensure_future(h.call(call))
    await h.toolbox.slow_started.wait()
    second = await h.call(call)
    assert ekl(second)["status"] == "running" and ekl(second)["code"] == "EXECUTION_RUNNING" and second.is_error
    h.toolbox.slow_release.set()
    assert ekl(await first)["outcome"] == "succeeded"
    assert h.network.runs("agt_consumer", "e1") == 1


@scenario("storage_free", "durable")
async def is_error_settles_without_effect(h: Harness) -> None:
    call = Call("broken", {}, "e1")
    result = await h.call(call)
    await h.core.drain()
    assert ekl(result)["outcome"] == "failed_before_effect"
    assert h.execution(call).funding == "released"


@scenario("storage_free")
async def output_failing_its_schema_is_not_success(h: Harness) -> None:
    call = Call("bad_output", {}, "e1")
    result = await h.call(call)
    await h.core.drain()
    assert ekl(result)["outcome"] == "failed_before_effect" and h.execution(call).output_fingerprint is None


# --- repeats -------------------------------------------------------------------------------------------------------


@scenario("storage_free", "durable")
async def refresh_needs_a_request_id_and_keeps_its_identity(h: Harness) -> None:
    call = Call("lookup", {"area": "x"}, "e1")
    first = await h.call(call)
    await h.core.drain()
    plain = await h.call(call)
    assert ekl(plain)["code"] == "RESULT_UNAVAILABLE" and ekl(plain)["refresh_available"] is True
    assert h.toolbox.lookups == 1
    refresh = Call("lookup", {"area": "x"}, "e1", salt=call.salt, refresh_request_id="r1")
    fresh = await h.call(refresh)
    assert ekl(fresh)["refresh"] is True and ekl(fresh)["refresh_request_id"] == "r1"
    assert result_body(fresh) != result_body(first)
    # The consumer lost the response and asks again with the same refresh request ID: the refresh was granted to
    # the attempt that ran it, so nothing runs again and no allowance is taken.
    lookups = h.toolbox.lookups
    again = await h.call(refresh)
    assert ekl(again)["code"] == "REFRESH_USED" and ekl(again)["refresh_id"] == ekl(fresh)["refresh_id"]
    assert h.toolbox.lookups == lookups and len(h.execution(call).refreshes) == 1
    # The adapter loses an admit response on the way: its retry, by the same attempt, gets the same grant and the
    # tool runs once.
    h.network.faults.lose_admit_responses = 1
    second = await h.call(Call("lookup", {"area": "x"}, "e1", salt=call.salt, refresh_request_id="r2"))
    assert ekl(second)["refresh"] is True and len(h.execution(call).refreshes) == 2
    assert h.toolbox.lookups == lookups + 1
    exhausted = await h.call(Call("lookup", {"area": "x"}, "e1", salt=call.salt, refresh_request_id="r3"))
    assert ekl(exhausted)["code"] == "REFRESH_EXHAUSTED"
    # Refreshes are never completed, charged or receipted.
    assert h.execution(call).completions == 1


@scenario("storage_free")
async def a_paid_status_only_repeat_returns_status_not_work(h: Harness) -> None:
    call = Call("transform", {"text": "abc"}, "e1")
    await h.call(call)
    await h.core.drain()
    repeat = await h.call(call)
    assert ekl(repeat)["code"] == "RESULT_UNAVAILABLE" and ekl(repeat)["receipt_id"]
    assert "refresh_available" not in ekl(repeat)
    assert h.toolbox.transforms == 1 and h.execution(call).funding == "captured"


@scenario("durable")
async def a_kept_result_is_replayed(h: Harness) -> None:
    call = Call("keep", {"text": "abc"}, "e1")
    first = await h.call(call)
    await h.core.drain()
    again = await h.call(call)
    assert ekl(again)["replay"] is True and result_body(again) == result_body(first)
    assert h.network.runs("agt_consumer", "e1") == 1


# --- losses --------------------------------------------------------------------------------------------------


@scenario("storage_free")
async def completion_loss_lapses_without_charge(h: Harness) -> None:
    h.network.lease_seconds = 0.0
    original = h.network.complete

    async def unreachable(request: CompleteRequest) -> Completion:
        del request
        raise ProviderApiError("SERVICE_UNAVAILABLE", "lost (injected)", retryable=True)

    h.network.complete = unreachable  # type: ignore[method-assign]  # the fault: completions never arrive
    call = Call("transform", {"text": "abc"}, "e1")
    result = await h.call(call)
    await h.core.drain()
    h.network.complete = original  # type: ignore[method-assign]
    assert result_body(result) == {"upper": "ABC"}  # the consumer has its result
    h.network.tick()
    h.network.end_reconciliation_window()
    assert h.execution(call).outcome == "not_completed" and h.execution(call).funding == "released"


@scenario("storage_free")
async def response_loss_is_charged_once_and_not_replayed(h: Harness) -> None:
    call = Call("transform", {"text": "abc"}, "e1")
    await h.call(call)  # the response is lost on its way to the consumer
    await h.core.drain()
    again = await h.call(call)
    assert ekl(again)["code"] == "RESULT_UNAVAILABLE"
    assert h.execution(call).funding == "captured" and h.toolbox.transforms == 1


@scenario("durable")
async def response_loss_is_recovered_from_the_journal(h: Harness) -> None:
    call = Call("keep", {"text": "abc"}, "e1")
    first = await h.call(call)
    await h.core.drain()
    again = await h.call(call)
    assert result_body(again) == result_body(first) and h.execution(call).completions == 1


# --- crashes (durable) ---------------------------------------------------------------------------------------------


@scenario("durable")
async def crash_after_admit_settles_without_effect(h: Harness) -> None:
    h.toolbox.crash_at = "keep:start"
    call = Call("keep", {"text": "abc"}, "e1")
    with contextlib.suppress(Crash):
        await h.call(call)
    await h.restart()
    execution = h.execution(call)
    assert execution.outcome == "failed_before_effect" and execution.funding == "released"
    assert h.network.runs("agt_consumer", "e1") == 1


@scenario("durable")
async def a_shared_journal_waits_for_the_lease_before_settling(h: Harness) -> None:
    # Several replicas share the journal: an entry another replica may still be working on is not abandoned until
    # its lease, plus the grace, has passed.
    assert h.storage is not None
    h.storage.exclusive = False
    h.toolbox.crash_at = "keep:start"
    call = Call("keep", {"text": "abc"}, "e1")
    with contextlib.suppress(Crash):
        await h.call(call)
    await h.restart()
    assert h.execution(call).outcome is None  # still that worker's
    later = time.time() + h.network.lease_seconds + h.core.settings.abandon_grace + 1
    h.core.clock = lambda: later
    await h.core.settle_abandoned()
    assert h.execution(call).outcome == "failed_before_effect"
    assert h.network.runs("agt_consumer", "e1") == 1


@scenario("durable")
async def the_irreducible_window_ends_unknown_then_resolved(h: Harness) -> None:
    # The effect is taken and the process stops before anything is written: on restart the outcome is unknown, and
    # the declared outcome lookup finds the effect and appends the resolution.
    h.toolbox.crash_at = "send:after_effect"
    call = Call("send", {"to": "ap@example.com", "message": "hi"}, "e1")
    with contextlib.suppress(Crash):
        await h.call(call)
    await h.restart()
    await h.core.drain()
    execution = h.execution(call)
    assert execution.outcome == "outcome_unknown"
    assert execution.resolution is not None and execution.resolution["outcome"] == "succeeded"
    assert execution.funding == "captured" and h.toolbox.effects == 1


@scenario("durable")
async def unknown_without_resolution_stays_held(h: Harness) -> None:
    # A zero lease lapses the admission at once, so the retry below meets a lapsed execution.
    h.network.lease_seconds = 0.0
    h.toolbox.crash_at = "send:before_effect"
    call = Call("send", {"to": "ap@example.com", "message": "hi"}, "e1")
    lookup = h.toolbox.sent_lookup

    async def unknown(query: OutcomeQuery) -> None:
        del query

    h.toolbox.sent_lookup = unknown  # type: ignore[method-assign]  # the lookup cannot tell
    with contextlib.suppress(Crash):
        await h.call(call)
    await h.restart()
    await h.core.drain()
    h.toolbox.sent_lookup = lookup  # type: ignore[method-assign]
    execution = h.execution(call)
    assert execution.outcome == "outcome_unknown" and execution.funding == "held" and execution.resolution is None
    retry = await h.call(call)
    assert ekl(retry)["code"] == "EXECUTION_LAPSED" and h.toolbox.effects == 0


class LosingApi(CrashingApi):
    """The admission is made, then the process stops before its answer is read: the lost admit response."""

    async def admit(self, request: Any) -> Any:
        await self.network.admit(request)
        raise Crash("admit:response")


async def _lost_admit_response(h: Harness, *, identity: bool) -> None:
    h.network.report_identity = identity
    other = h.network.token("agt_other")
    # Another consumer's admission under the same execution ID, still running, must not be taken for ours.
    await h.call(Call("keep", {"text": "theirs"}, "e1"), token=other)
    await h.core.drain()
    h.network.by_admission[h.network.executions[("agt_other", "e1")].admission_id].state = "admitted"
    h.api = LosingApi(h.network)
    h.core = h.build()
    with contextlib.suppress(Crash):
        await h.call(Call("keep", {"text": "mine"}, "e1"))
    h.api = CrashingApi(h.network)
    await h.restart()
    ours = h.execution(Call("keep", {"text": "mine"}, "e1"))
    if identity:
        # Matched exactly: settled as interrupted, without effect, and never run.
        assert ours.state == "completed" and ours.outcome == "failed_before_effect"
    else:
        # Two admissions share the ID and nothing tells them apart: neither is guessed at; ours will lapse.
        assert ours.state == "admitted" and ours.completions == 0
    assert h.network.executions[("agt_other", "e1")].outcome == "succeeded"


@scenario("durable")
async def a_lost_admit_response_is_matched_exactly_after_a_restart(h: Harness) -> None:
    await _lost_admit_response(h, identity=True)


@scenario("durable")
async def without_identity_in_pending_recovery_does_not_guess(h: Harness) -> None:
    await _lost_admit_response(h, identity=False)


@scenario("durable")
async def a_lost_admission_beyond_a_cut_off_pending_list_is_kept_for_the_next_round(h: Harness) -> None:
    h.api = LosingApi(h.network)
    h.core = h.build()
    call = Call("lookup", {"area": "north"}, "e1")
    with contextlib.suppress(Crash):
        await h.call(call)
    h.api = CrashingApi(h.network)
    h.network.pending_limit = 0
    await h.restart()
    # The list left it out, so it may still be admitted: the entry is kept, and nothing is settled or run.
    assert h.execution(call).state == "admitted" and h.execution(call).completions == 0
    h.network.pending_limit = 500
    await h.restart()
    ours = h.execution(call)
    assert ours.state == "completed" and ours.outcome == "failed_before_effect" and h.toolbox.lookups == 0


@scenario("durable")
async def crash_after_the_journal_write_delivers_on_restart(h: Harness) -> None:
    h.api.crash_on = "complete:before"
    call = Call("keep", {"text": "abc"}, "e1")
    first = await h.call(call)  # the reply was sent; the completion crashed in the background
    await h.core.drain()
    assert h.execution(call).state == "admitted"
    await h.restart()
    assert h.execution(call).completions == 1 and h.execution(call).outcome == "succeeded"
    again = await h.call(call)
    assert result_body(again) == result_body(first)


@scenario("durable")
async def crash_after_complete_resends_idempotently(h: Harness) -> None:
    assert h.storage is not None
    h.storage.crash_after = "acknowledge:before"
    call = Call("keep", {"text": "abc"}, "e1")
    await h.call(call)
    await h.core.drain()
    receipt = h.execution(call).receipt
    await h.restart()
    assert h.execution(call).completions == 1 and h.execution(call).receipt == receipt
    assert not h.storage.outbox()


@scenario("durable")
async def a_conflicting_completion_is_never_acknowledged(h: Harness) -> None:
    assert h.storage is not None
    call = Call("keep", {"text": "abc"}, "e1")
    await h.call(call)
    await h.core.drain()
    admission_id = h.execution(call).admission_id
    try:
        await h.network.complete(CompleteRequest(admission_id=admission_id, outcome="failed_before_effect"))
    except ProviderApiError as exc:
        assert exc.code == "COMPLETION_CONFLICT"
    else:
        raise AssertionError("a conflicting completion was accepted")


# --- leases, revocation, cancellation ------------------------------------------------------------------------------


@scenario("storage_free", "durable")
async def leases_are_renewed_while_work_outlasts_them(h: Harness) -> None:
    h.network.lease_seconds = 0.2
    call = Call("slow", {"seconds": 0.6}, "e1")
    result = await h.call(call)
    await h.core.drain()
    assert ekl(result)["outcome"] == "succeeded" and h.network.calls["renew"] >= 1
    assert h.execution(call).state == "completed"


@scenario("storage_free", "durable")
async def revocation_stops_new_admissions_not_admitted_work(h: Harness) -> None:
    call = Call("slow", {"seconds": 5}, "e1")
    running = asyncio.ensure_future(h.call(call))
    await h.toolbox.slow_started.wait()
    h.network.revoke(h.token)
    refused = await h.call(Call("lookup", {"area": "x"}, "e2"))
    assert ekl(refused)["code"] == "CONNECTION_ENDED"
    h.toolbox.slow_release.set()
    assert ekl(await running)["outcome"] == "succeeded"


@scenario("storage_free", "durable")
async def cancellation_is_requested_received_and_confirmed(h: Harness) -> None:
    h.network.lease_seconds = 0.2
    call = Call("slow", {"seconds": 5}, "e1")
    running = asyncio.ensure_future(h.call(call))
    await h.toolbox.slow_started.wait()
    admission_id = h.execution(call).admission_id
    h.network.request_cancel(admission_id)
    result = await running
    await h.core.drain()
    assert ekl(result)["outcome"] == "cancelled" and h.toolbox.slow_stopped
    assert h.execution(call).cancel == "confirmed" and h.execution(call).funding == "released"


@scenario("storage_free", "durable")
async def cancellation_on_local_lease_expiry_when_the_network_is_away(h: Harness) -> None:
    h.network.lease_seconds = 0.2
    h.network.faults.fail_renewals = True
    call = Call("slow", {"seconds": 5}, "e1")
    result = await h.call(call)
    assert ekl(result)["outcome"] == "cancelled" and h.toolbox.slow_stopped


# --- dry run, challenge, fingerprints ------------------------------------------------------------------------------


@scenario("durable")
async def validation_runs_external_actions_dry(h: Harness) -> None:
    validation = h.network.token("agt_validation", kind="validation")
    call = Call("send", {"to": "ap@example.com", "message": "hi"}, "v1")
    result = await h.call(call, token=validation)
    assert ekl(result)["outcome"] == "succeeded" and result_body(result) == {"sent": False, "dry_run": True}
    assert h.toolbox.effects == 0
    no_dry = await h.call(Call("archive", {"record": "r"}, "v2"), token=validation)
    assert ekl(no_dry)["code"] == "DRY_RUN_UNSUPPORTED"


@scenario("storage_free", "durable")
async def the_endpoint_challenge_is_bound_to_credential_and_endpoint(h: Harness) -> None:
    import hashlib  # noqa: PLC0415 - kept beside its only use
    import hmac  # noqa: PLC0415

    challenge = "ab" * 32
    key = hashlib.sha256(CREDENTIAL.encode()).hexdigest().encode()
    expected = hmac.new(key, (challenge + ENDPOINT).encode(), hashlib.sha256).hexdigest()
    assert h.core.answer_challenge(challenge) == expected


@scenario("durable")
async def the_journal_holds_metadata_unless_arguments_are_declared(h: Harness) -> None:
    assert h.storage is not None
    await h.call(Call("lookup", {"area": "private"}, "e1"))
    await h.call(Call("send", {"to": "ap@example.com", "message": "hi"}, "e2"))
    await h.core.drain()
    rows = h.storage._execute(
        "SELECT tool, arguments_json, result_json FROM ekl_executions"
    )  # the suite inspects the journal directly
    by_tool = {r[0]: (r[1], r[2]) for r in rows}
    assert by_tool["lookup"] == (None, None)  # refresh: nothing about the call is kept
    assert by_tool["send"][0] is not None  # journaled for the outcome lookup, as declared


def _raw_agent(h: Harness) -> dict[str, Any]:
    return durable_agent() if h.mode == "durable" else storage_free_agent()


@scenario("storage_free", "durable")
async def nothing_is_served_before_its_publisher_confirms(h: Harness) -> None:
    """The adapter announces agent.json without its local settings and lists its tools at once, so the
    network can check the endpoint; calls are served only once a version is confirmed."""
    h.network.confirmed = 0
    h.core = h.build()
    assert not h.core.configured and h.local is not None
    assert await h.core.announce_if_changed() and await h.core.announce_if_changed()
    (sent,) = h.network.announcements  # an unchanged file is announced once
    assert sent["endpoint"] == ENDPOINT and "path" not in sent["config"]["adapter"]
    assert h.core.announcement is not None and h.core.announcement.state == "waiting_for_review"
    assert [c.tool for c in h.core.listed] == [c.tool for c in h.local.agent.offered]
    refused = await h.call(Call("lookup", {"area": "dublin"}, "e1"))
    assert ekl(refused)["code"] == "NOT_CONFIGURED"
    h.network.confirm()
    result = await h.call(Call("lookup", {"area": "dublin"}, "e2"))  # loaded when the listed tool is called
    await h.core.drain()
    assert ekl(result)["outcome"] == "succeeded" and h.core.config_version == h.network.config_version


@scenario("storage_free", "durable")
async def a_changed_configuration_is_picked_up_from_admit(h: Harness) -> None:
    """Admit reports the confirmed version; a new one is loaded, and a tool no longer offered is refused."""
    changed = strip_local(_raw_agent(h))
    changed["capabilities"] = [c | {"offered": c["tool"] != "transform"} for c in changed["capabilities"]]
    h.network.set_configuration(changed)
    await h.call(Call("lookup", {"area": "dublin"}, "e1"))  # this admit reports the new version
    await h.core.drain()
    assert h.core.config_version == h.network.config_version
    refused = await h.call(Call("transform", {"text": "x"}, "e2"))
    assert ekl(refused)["code"] == "NOT_LISTED"


@scenario("storage_free", "durable")
async def calls_run_under_the_version_they_were_admitted_under(h: Harness) -> None:
    """A confirmed version that withdraws a tool is the validation candidate while the previous version stays
    active. A call admitted under the active version for a tool neither the candidate nor the local file offers ends
    before the tool runs, without effect and without charge; once the candidate is active its tools are refused."""
    changed = strip_local(_raw_agent(h))
    changed["capabilities"] = [c | {"offered": c["tool"] != "lookup"} for c in changed["capabilities"]]
    h.network.set_configuration(changed)
    h.network.active = 1
    h.core.local = None  # the server and its agent.json no longer have the tool
    await h.core.load_config()
    assert h.core.config_version == 2 and h.core.active_version == 1
    result = await h.call(Call("lookup", {"area": "dublin"}, "e1"))
    await h.core.drain()
    assert ekl(result)["code"] == "UNAVAILABLE" and h.toolbox.lookups == 0
    (execution,) = [e for e in h.network.by_admission.values() if e.execution_id == "e1"]
    assert execution.outcome == "failed_before_effect" and execution.funding == "released"
    h.network.active = None
    await h.core.load_config()
    refused = await h.call(Call("lookup", {"area": "dublin"}, "e2"))
    assert ekl(refused)["code"] == "NOT_LISTED"


@scenario("storage_free")
async def a_changed_agent_file_is_announced_and_applies_once_confirmed(h: Harness) -> None:
    """A changed agent.json is announced as a new version and listed at once, while calls keep the
    confirmed version until its publisher confirms the new one. A file that no longer loads is not announced."""
    path = Path(h._dir.name) / "agent.json"
    raw = _raw_agent(h)
    path.write_text(json.dumps(raw))
    h.local = LocalFile.read(path)
    h.core = h.build()
    await h.core.announce_if_changed()
    assert h.core.announcement is not None and h.core.announcement.state == "confirmed"
    path.write_text("{")
    await h.core.announce_if_changed()
    assert len(h.network.announcements) == 1
    path.write_text(json.dumps(raw | {"capabilities": [*raw["capabilities"], {"tool": "fresh", "class": "read_only"}]}))
    await h.core.announce_if_changed()
    assert h.core.announcement.state == "waiting_for_review" and len(h.network.announcements) == 2
    assert "fresh" in [c.tool for c in h.core.listed]
    refused = await h.call(Call("fresh", {}, "e1"))
    assert ekl(refused)["code"] == "NOT_LISTED" and h.network.calls["admit"] == 0
    h.network.confirm()
    await h.core.load_config()
    assert h.core.agent.capability("fresh") is not None and h.core.config_version == 2


async def run_all(modes: tuple[str, ...] = ("storage_free", "durable")) -> dict[str, str]:
    """Every scenario in every mode it applies to: name → "passed" or the failure."""
    results = {}
    for name, (applies, fn) in SCENARIOS.items():
        for mode in modes:
            if mode not in applies:
                continue
            harness = Harness(mode)
            try:
                await fn(harness)
                results[f"{name}[{mode}]"] = "passed"
            except AssertionError as exc:
                results[f"{name}[{mode}]"] = f"failed: {exc}"
            finally:
                await harness.core.drain()
                harness.close()
    return results
