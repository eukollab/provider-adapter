# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""agent.service(...): stand-ins answer without a credential, the network chooses with one, and
FakeNetwork loads the project's stand-ins as providers."""

import json
from pathlib import Path
from typing import Any

import pytest

from ekl_provider_adapter import Agent, AgentError, AgentNotEnabled, StandInMissing
from ekl_provider_adapter.agent_journal import AgentJournal
from ekl_provider_adapter.testing import FakeNetwork

SCHEMA = {"type": "object", "properties": {"vat_number": {"type": "string"}}, "required": ["vat_number"]}


def _record(folder: Path) -> None:
    folder.mkdir(parents=True)
    (folder / "vat-check.validate.json").write_text(
        json.dumps(
            {
                "ekl_fixture": "1",
                "service": "@vat-check/validate",
                "version": "1.2.0",
                "input_schema": SCHEMA,
                "output_schema": {"type": "object"},
                "examples": [
                    {"input": {"vat_number": "DE123456789"}, "output": {"valid": True}},
                    {"input": {"vat_number": "XX0"}, "output": {"valid": False}},
                ],
            }
        )
    )


@pytest.fixture
def fixtures(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    folder = tmp_path / "eukollab" / "fixtures"
    _record(folder)
    monkeypatch.setenv("EKL_FIXTURES", str(folder))
    return folder


def test_without_a_credential_a_declared_service_answers_from_its_stand_in(
    fixtures: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("EKL_AGENT_CREDENTIAL", raising=False)
    agent = Agent.from_env()
    assert not agent.enabled
    vat = agent.service("validate EU VAT numbers", prefer="@vat-check/validate")
    first = vat.call({"vat_number": "XX0"})
    assert first.value == {"valid": False} and first.stand_in and first.charged == "0"
    assert vat.call({"vat_number": "FR40303265045"}).value == {"valid": True}  # no exact match: the first example
    with pytest.raises(AgentError, match="would not accept"):
        vat.call({"number": "DE1"})
    with pytest.raises(StandInMissing, match="ekl try @tax-id check --record"):
        agent.service("check tax IDs", prefer="@tax-id/check").call({})
    with pytest.raises(StandInMissing, match="prefer="):
        agent.service("anything").call({})
    with pytest.raises(AgentNotEnabled):
        agent.run()


def test_with_the_network_the_preferred_service_is_chosen_once_and_reused(fixtures: Path) -> None:
    fake = FakeNetwork(handle="@ledger")
    fake.provider("@cheap-vat", "cap_validate", lambda _: {"valid": True}, price="0.001", summary="validate vat")
    loaded = fake.stand_ins()
    assert [p.handle for p in loaded] == ["@vat-check"]
    agent = Agent(fake, AgentJournal(":memory:"))
    vat = agent.service(
        "validate vat",
        prefer="@vat-check/validate",
        sample={"vat_number": "DE123456789"},
        check=lambda out: "valid" in out,
    )
    result = vat.call({"vat_number": "XX0"})
    assert not result.stand_in and result.value == {"valid": False}
    assert vat.main is not None and vat.main.handle == "@vat-check"
    vat.call({"vat_number": "DE123456789"})
    assert len(fake.choices) == 1  # chosen once, then reused


def _crowded(excluded_by: list[str] | None = None) -> FakeNetwork:
    """Three services fitting the need better than the preferred one, whose words don't match the need at all."""
    fake = FakeNetwork(handle="@ledger")
    for handle in ("@vat-a", "@vat-b", "@vat-c"):
        fake.provider(handle, "cap_check", lambda _: {"valid": True}, summary="validate vat numbers", price="0.001")
    fake.provider(
        "@vat-check", "cap_validate", lambda _: {"valid": True}, summary="EU tax register", excluded_by=excluded_by
    )
    return fake


def test_the_preferred_service_is_tried_first_even_when_others_fit_the_need_better() -> None:
    fake = _crowded()
    vat = Agent(fake, AgentJournal(":memory:")).service(
        "validate vat numbers", prefer="@vat-check/validate", sample={"vat_number": "DE1"}, check=lambda o: "valid" in o
    )
    vat.call({"vat_number": "DE1"})
    assert vat.main is not None and vat.main.handle == "@vat-check"
    considered = fake.choices[0]["candidates"]
    assert considered[0]["handle"] == "@vat-check" and considered[0]["trial_score"] == "1/1"
    assert [c["trial_score"] for c in considered].count("1/1") == 3  # the preferred one took one of the three trials


def test_a_preferred_service_that_fails_its_trial_is_not_taken() -> None:
    fake = _crowded()
    fake.providers["@vat-check/cap_validate"].work = lambda _: {"wrong": True}
    vat = Agent(fake, AgentJournal(":memory:")).service(
        "validate vat numbers", prefer="@vat-check/validate", sample={"vat_number": "DE1"}, check=lambda o: "valid" in o
    )
    vat.call({"vat_number": "DE1"})
    assert vat.main is not None and vat.main.handle != "@vat-check"
    tried_first = fake.choices[0]["candidates"][0]
    assert tried_first["handle"] == "@vat-check" and tried_first["trial_score"] == "0/1"


def test_an_ineligible_preferred_service_is_skipped_with_its_rule() -> None:
    fake = _crowded(excluded_by=["data_retention"])
    vat = Agent(fake, AgentJournal(":memory:")).service(
        "validate vat numbers", prefer="@vat-check/validate", sample={"vat_number": "DE1"}, check=lambda o: "valid" in o
    )
    vat.call({"vat_number": "DE1"})
    assert vat.main is not None and vat.main.handle != "@vat-check"
    skipped = next(c for c in fake.choices[0]["candidates"] if c["handle"] == "@vat-check")
    assert skipped["trial_score"] == "skipped" and skipped["excluded_by"] == ["data_retention"]
    assert fake.providers["@vat-check/cap_validate"].calls == 0


def test_fake_network_answers_like_the_recording(fixtures: Path) -> None:
    fake = FakeNetwork()
    provider = fake.stand_ins()[0]
    answered: Any = provider.work({"vat_number": "XX0"})
    assert answered == {"valid": False} and provider.input_schema == SCHEMA


@pytest.mark.anyio
async def test_the_connector_serves_a_declared_service_to_any_language(fixtures: Path) -> None:
    import httpx2  # noqa: PLC0415 - only this test talks HTTP

    from ekl_provider_adapter.callbacks import local_api  # noqa: PLC0415

    api = local_api(Agent(None))
    async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=api), base_url="http://127.0.0.1") as c:
        answered = await c.post(
            "/agent/service",
            json={
                "need": "validate EU VAT numbers",
                "prefer": "@vat-check/validate",
                "expect": {"type": "object", "required": ["valid"]},
                "input": {"vat_number": "XX0"},
            },
        )
        assert answered.status_code == 200
        assert answered.json() | {"execution_id": ""} == {
            "value": {"valid": False},
            "execution_id": "",
            "charged": "0",
            "receipt_checked": False,
            "stand_in": True,
            "provider": "@vat-check/validate",
        }
        missing = await c.post("/agent/service", json={"need": "x", "prefer": "@none/x", "input": {}})
        assert missing.status_code == 409 and missing.json()["error"] == "STAND_IN_MISSING"


@pytest.mark.anyio
async def test_the_connector_tries_the_preferred_service_first_too() -> None:
    import httpx2  # noqa: PLC0415 - only these tests talk HTTP

    from ekl_provider_adapter.callbacks import local_api  # noqa: PLC0415

    fake = _crowded()
    api = local_api(Agent(fake, AgentJournal(":memory:")))
    async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=api), base_url="http://127.0.0.1") as c:
        answered = await c.post(
            "/agent/service",
            json={
                "need": "validate vat numbers",
                "prefer": "@vat-check/validate",
                "sample": {"vat_number": "DE1"},
                "expect": {"type": "object", "required": ["valid"]},
                "input": {"vat_number": "DE1"},
            },
        )
    assert answered.status_code == 200
    assert answered.json()["provider"] == "@vat-check" and not answered.json()["stand_in"]


@pytest.mark.anyio
async def test_the_connector_says_whether_it_holds_the_agent_credential() -> None:
    import httpx2  # noqa: PLC0415 - only these tests talk HTTP

    from ekl_provider_adapter.callbacks import local_api  # noqa: PLC0415

    for agent, live in ((Agent(None), False), (Agent(FakeNetwork(), AgentJournal(":memory:")), True)):
        transport = httpx2.ASGITransport(app=local_api(agent))
        async with httpx2.AsyncClient(transport=transport, base_url="http://127.0.0.1") as c:
            answered = await c.get("/agent/status")
        assert answered.status_code == 200 and answered.json() == {"live": live}


def _two_providers(side_effect_class: str, fails: str) -> tuple[FakeNetwork, Any, Any]:
    fake = FakeNetwork(handle="@ledger")
    main = fake.provider(
        "@main-send", "cap_send", lambda _: {"sent": True}, summary="send letters", side_effect_class=side_effect_class
    )
    backup = fake.provider(
        "@backup-send",
        "cap_send",
        lambda _: {"sent": True},
        summary="send letters",
        side_effect_class=side_effect_class,
    )
    main.fails = fails
    return fake, main, backup


@pytest.fixture
def no_waiting(monkeypatch: pytest.MonkeyPatch) -> None:
    async def instant(_: float) -> None:
        return None

    monkeypatch.setattr("ekl_provider_adapter.agent.asyncio.sleep", instant)


@pytest.mark.usefixtures("no_waiting")
def test_work_that_acts_outside_is_not_repeated_at_the_backup_after_a_timeout() -> None:
    fake, main, backup = _two_providers("external_action", "timeout")
    letters = Agent(fake, AgentJournal(":memory:")).service(
        "send letters", prefer="@main-send/cap_send", sample={"to": "a"}, check=lambda out: out["sent"]
    )
    with pytest.raises(AgentError) as failed:
        letters.call({"to": "a"}, idempotency_key="letter-1")
    assert failed.value.code == "OUTCOME_UNKNOWN" and "letter-1" in str(failed.value)
    assert main.calls >= 1 and backup.calls == 0


@pytest.mark.usefixtures("no_waiting")
@pytest.mark.parametrize(("side_effect_class", "fails"), [("read_only", "timeout"), ("external_action", "unreachable")])
def test_repeatable_work_or_a_call_that_never_arrived_goes_to_the_backup_once(
    side_effect_class: str, fails: str
) -> None:
    fake, _, backup = _two_providers(side_effect_class, fails)
    letters = Agent(fake, AgentJournal(":memory:")).service(
        "send letters", prefer="@main-send/cap_send", sample={"to": "a"}, check=lambda out: out["sent"]
    )
    result = letters.call({"to": "a"}, idempotency_key="letter-1")
    assert result.value == {"sent": True} and backup.calls == 1
    assert letters.main is not None and letters.main.handle == "@backup-send"


def test_a_changed_contract_makes_a_declared_service_choose_again(tmp_path: Path) -> None:
    fake = FakeNetwork(handle="@ledger")
    first = fake.provider("@vat-a", "cap_validate", lambda _: {"valid": True}, summary="validate vat", price="0.01")
    fake.provider("@vat-b", "cap_validate", lambda _: {"valid": True}, summary="validate vat", price="0.02")
    journal = AgentJournal(str(tmp_path / "journal.db"))
    vat = Agent(fake, journal).service("validate vat", sample={"vat_number": "DE1"}, check=lambda out: "valid" in out)
    vat.call({"vat_number": "DE1"})
    assert len(fake.choices) == 1 and vat.main is not None
    chosen = vat.main.handle
    changed = first if chosen == "@vat-a" else fake.providers["@vat-b/cap_validate"]
    changed.input_schema = {"type": "object", "required": ["country", "number"]}
    for connection_id in list(fake.connections):
        journal.save_access(connection_id, {"expires_at": ""})  # the cached access runs out
    vat.call({"vat_number": "DE1"})
    assert len(fake.choices) == 2  # chosen again, with trials, against the new contract


@pytest.mark.usefixtures("no_waiting")
def test_with_the_credential_a_failure_raises_and_never_answers_from_a_stand_in(fixtures: Path) -> None:
    fake = FakeNetwork(handle="@ledger")
    (recorded,) = fake.stand_ins()
    recorded.fails = "unreachable"
    vat = Agent(fake, AgentJournal(":memory:")).service(
        "validate vat", prefer="@vat-check/validate", sample={"vat_number": "DE123456789"}, check=lambda o: "valid" in o
    )
    with pytest.raises(AgentError) as failed:
        vat.call({"vat_number": "XX0"})
    assert failed.value.code != "STAND_IN" and recorded.calls == 0
