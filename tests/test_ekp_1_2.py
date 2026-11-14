# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""EKP 1.2 on the adapter's side, against the protocol's own vectors: endpoint proof v2 (WP9), receipts checked claim
by claim (WP10), local screening that sends only a flag (WP31), the depth limit (WP15) and callback v2."""

import asyncio
import json
import time
from pathlib import Path
from typing import Any

import pytest

from ekl_provider_adapter import callback_signing as cb
from ekl_provider_adapter.agent import Agent
from ekl_provider_adapter.agent_journal import AgentJournal
from ekl_provider_adapter.api import Announcement, proof_key
from ekl_provider_adapter.core import AdapterCore
from ekl_provider_adapter.jcs import InputTooDeepError, canonical, too_deep
from ekl_provider_adapter.receipts import CallMade, KeySet, ReceiptInvalid, receipt_digest, verify
from ekl_provider_adapter.screening import Rules, RulesInvalid, Screener, verified_rules
from ekl_provider_adapter.testing import FakeNetwork
from tests.conftest import CONTRACTS

VECTORS = CONTRACTS / "vectors"


def _vector(name: str) -> dict[str, Any]:
    path = VECTORS / name
    if not path.exists():
        pytest.skip(f"no {name} in this checkout")
    found: dict[str, Any] = json.loads(path.read_text())
    return found


def test_callback_v2_matches_the_protocol_vectors() -> None:
    v = _vector("callback-v2.json")
    key = cb.callback_key(v["agent_credential"])
    assert key.hex() == v["callback_key"]
    request, response = v["request"], v["response"]
    body = request["body"].encode()
    assert cb.request_header(key, request["t"], request["event_id"], request["agent_id"], body) == request["header"]
    assert cb.response_valid(
        key,
        response["header"],
        event_id=request["event_id"],
        agent_id=request["agent_id"],
        request_digest=response["request_digest"],
        status=response["status"],
        body=response["body"].encode(),
        now=response["t"],
    )


def _core(credential: str, endpoint: str) -> AdapterCore:
    async def invoke(*_: Any) -> Any:
        raise AssertionError

    return AdapterCore(None, None, invoke, credential=credential, endpoint=endpoint)  # type: ignore[arg-type]


def test_ac279_the_adapter_proves_with_v2_and_drops_v1_once_the_network_holds_the_proof_key() -> None:
    v = _vector("endpoint-proof-v2.json")
    assert proof_key(v["agent_key"]).hex() == v["proof_key"]
    core = _core(v["agent_key"], v["challenge"]["endpoint"])
    proofs = core.challenge_proofs(v["challenge"]["challenge"])
    assert proofs["proof_v2"] == v["challenge"]["proof_v2"] and "proof" in proofs
    core.announcement = Announcement(version=1, state="confirmed", endpoint_proof="v2")
    assert core.challenge_proofs(v["challenge"]["challenge"]) == {"proof_v2": v["challenge"]["proof_v2"]}


def _call(v: dict[str, Any]) -> CallMade:
    c = v["call"]
    return CallMade(
        c["consumer_agent_id"],
        c["provider_agent_id"],
        c["execution_id"],
        c["capability_id"],
        c["input_fingerprint"],
        c["sent_at"],
        c["answered_at"],
    )


def test_ac281_receipts_are_checked_claim_by_claim() -> None:
    v = _vector("receipt-jws.json")
    keys = KeySet(v["call"]["issuer"], keys=v["jwks"])

    async def go() -> None:
        claims = await verify(v["receipt_jws"], _call(v), v["call"]["issuer"], keys)
        assert claims["execution_id"] == v["call"]["execution_id"]
        assert receipt_digest(v["receipt_jws"]) == v["receipt_digest"]
        for case in v["invalid"]:
            with pytest.raises(ReceiptInvalid):
                await verify(case["jws"], _call(v), v["call"]["issuer"], keys)

    asyncio.run(go())


def test_ac281_a_valid_receipt_for_another_execution_or_audience_is_refused() -> None:
    v = _vector("receipt-jws.json")
    keys = KeySet(v["call"]["issuer"], keys=v["jwks"])
    made = _call(v)
    other_execution = CallMade(**{**made.__dict__, "execution_id": "ex_other"})
    other_audience = CallMade(**{**made.__dict__, "consumer_agent_id": "agt_someone_else"})

    async def go() -> None:
        with pytest.raises(ReceiptInvalid, match="execution_id"):
            await verify(v["receipt_jws"], other_execution, v["call"]["issuer"], keys)
        with pytest.raises(ReceiptInvalid, match="another agent"):
            await verify(v["receipt_jws"], other_audience, v["call"]["issuer"], keys)

    asyncio.run(go())


def test_screening_rules_are_accepted_only_signed_current_and_from_the_network() -> None:
    v = _vector("screening-rules-jws.json")
    keys = KeySet(v["issuer"], keys=v["jwks"])

    async def go() -> None:
        rules = await verified_rules(v["rules_jws"], v["issuer"], keys, now=v["now"])
        assert rules.version == v["claims"]["rules_version"]
        for case in v["invalid"]:
            with pytest.raises(RulesInvalid):
                await verified_rules(case["jws"], v["issuer"], keys, now=case.get("now", v["now"]))

    asyncio.run(go())


def _screened_agent(tmp_path: Path, result: Any, config: dict[str, Any] | None = None) -> tuple[FakeNetwork, Agent]:
    fake = FakeNetwork(handle="@buyer")
    fake.provider("@leaky", "cap_leaky", lambda _: result, summary="leaky lookup")
    agent = Agent(fake, AgentJournal(str(tmp_path / "journal.db")))
    rules = Rules("2026-11-12.1", [("prohibited_content", ["example prohibited phrase"])], time.time() + 60)
    agent.screener = Screener(None, None, rules=rules)
    if config is not None:
        fake_self = fake._tool_agent_get_self

        def with_config() -> dict[str, Any]:
            return fake_self() | {"config": config}

        fake._tool_agent_get_self = with_config  # type: ignore[method-assign]
    return fake, agent


def test_ac312_a_private_key_in_a_result_raises_a_leak_flag_and_nothing_of_it_leaves(tmp_path: Path) -> None:
    # Built from parts so the repository's secret scan doesn't take this made-up key for a real one.
    secret = "-----BEGIN " + "PRIVATE KEY-----\nMIIEvQ\n-----END " + "PRIVATE KEY-----"
    fake, agent = _screened_agent(tmp_path, {"text": secret})
    [candidate] = agent.find("leaky lookup")
    result = agent.use(candidate).call({"q": "x"})
    assert result.safety_flags == [{"category": "credential_leak", "rules_version": "local"}]
    sent = json.dumps(fake.reports)
    assert "PRIVATE KEY" not in sent and "MIIEvQ" not in sent


def test_a_rule_phrase_matches_after_normalisation(tmp_path: Path) -> None:
    _, agent = _screened_agent(tmp_path, {"text": "An EXAMPLE Prohibited Phrase, in capitals"})
    [candidate] = agent.find("leaky lookup")
    flags = agent.use(candidate).call({"q": "x"}).safety_flags
    assert flags == [{"category": "prohibited_content", "rules_version": "2026-11-12.1"}]


def test_ac249_only_the_consumers_own_configuration_turns_screening_off(tmp_path: Path) -> None:
    secret = {"text": "-----BEGIN " + "PRIVATE KEY-----", "consumer": {"screen_results": False}}
    _, agent = _screened_agent(tmp_path, secret)  # what the provider sends can't turn it off
    [candidate] = agent.find("leaky lookup")
    assert agent.use(candidate).call({"q": "x"}).safety_flags
    _, off = _screened_agent(
        tmp_path / "off", {"text": "-----BEGIN " + "PRIVATE KEY-----"}, {"consumer": {"screen_results": False}}
    )
    [candidate] = off.find("leaky lookup")
    assert off.use(candidate).call({"q": "x"}).safety_flags == []


def test_the_depth_limit_matches_the_protocol_vectors() -> None:
    depth = _vector("ekl-json-1.json")["depth"]
    for case in depth["accepted"]:
        value = json.loads(case["input"])
        canonical(value)
        assert not too_deep(value)
    for case in depth["refused"]:
        value = json.loads(case["input"])
        assert too_deep(value)
        with pytest.raises(InputTooDeepError):
            canonical(value)


def test_upstream_fingerprint_matches_the_protocol_vectors() -> None:
    from ekl_provider_adapter.fingerprint import upstream_fingerprint  # noqa: PLC0415

    for case in _vector("upstream-fingerprint.json")["cases"]:
        assert upstream_fingerprint(case["tools_list"], set(case["declared"])) == case["upstream_fingerprint"]


def test_the_new_error_codes_raise_named_exceptions() -> None:
    from ekl_provider_adapter.errors import ContentRefused, InputRefused, Unavailable, from_network  # noqa: PLC0415

    refused = from_network("CONTENT_REFUSED", "refused", {"category": "credential_leak"})
    assert isinstance(refused, ContentRefused) and refused.category == "credential_leak"
    for code in ("AGENT_UNAVAILABLE", "NETWORK_PAUSED", "CAPABILITY_PAID_PAUSED"):
        assert isinstance(from_network(code, "unavailable"), Unavailable)
    assert isinstance(from_network("INPUT_TOO_DEEP", "deep"), InputRefused)


def test_the_adapter_reports_its_upstream_fingerprint_at_start() -> None:
    import mcp_types as types  # noqa: PLC0415

    from ekl_provider_adapter.config import LocalFile  # noqa: PLC0415
    from ekl_provider_adapter.fingerprint import upstream_fingerprint  # noqa: PLC0415
    from ekl_provider_adapter.server import report_presence  # noqa: PLC0415

    reported: list[str] = []

    class Api:
        async def presence(self, fingerprint: str, _version: str) -> int:
            reported.append(fingerprint)
            raise asyncio.CancelledError  # one report is enough for the test

    local = LocalFile.read(
        {
            "ekl": "1.0",
            "mode": "service_provider",
            "execution_mode": "storage_free",
            "capabilities": [{"tool": "lookup", "class": "read_only"}],
        }
    )
    core = AdapterCore(local, Api(), None, credential="eklp_x")  # type: ignore[arg-type]
    tools = [types.Tool(name="lookup", input_schema={"type": "object"}), types.Tool(name="hidden", input_schema={})]

    async def declared() -> list[types.Tool]:
        return tools

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(report_presence(core, declared))
    assert reported == [upstream_fingerprint(tools, {"lookup"})]
