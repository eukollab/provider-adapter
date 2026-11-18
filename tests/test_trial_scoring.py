# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The FakeNetwork scores a trial as the network does without a model (PROTOCOL.md §9.4.2), so a test that passes
against it passes on the network: held to the protocol's scoring vectors, and to the rules below."""

import json
from typing import Any

import pytest

from ekl_provider_adapter.errors import AgentError
from ekl_provider_adapter.testing import score_trial, validate_expectation
from tests.conftest import CONTRACTS

VECTORS = [CONTRACTS / "ekp" / "3.1" / "vectors" / "scoring.json", CONTRACTS / "vectors" / "scoring.json"]


def _vectors() -> list[dict[str, Any]]:
    found = next((p for p in VECTORS if p.is_file()), None)
    if found is None:
        return []
    cases: list[dict[str, Any]] = json.loads(found.read_text())["cases"]
    return cases


@pytest.mark.skipif(not _vectors(), reason="the protocol's scoring vectors are not in this checkout")
@pytest.mark.parametrize("case", _vectors(), ids=lambda c: str(c.get("name")))
def test_the_fake_scores_as_the_protocol_vectors_say(case: dict[str, Any]) -> None:
    refused = case.get("refused")
    if refused:
        with pytest.raises(AgentError) as error:
            score_trial(validate_expectation(case["expect"]), case.get("output"), case.get("output_schema"))
        assert error.value.code == refused
        return
    answer = score_trial(validate_expectation(case["expect"]), case["output"], case.get("output_schema"))
    expected = case["answer"]
    assert answer["score"] == expected["score"] and answer["passed"] == expected["passed"]
    if "checks" in expected:
        assert answer["checks"] == expected["checks"]


def _score(expect: dict[str, Any], output: Any, schema: dict[str, Any] | None = None) -> dict[str, Any]:
    return score_trial(validate_expectation(expect), output, schema)


def test_values_compare_alike_at_every_depth() -> None:
    expect = {"checks": [{"id": "a", "equals": {"n": 1, "names": ["ACME  Ltd"]}}]}
    assert _score(expect, {"n": 1.0, "names": ["acme ltd"]})["checks"][1]["reason"] == "matched"
    assert _score({"checks": [{"id": "a", "equals": 120}]}, "120")["checks"][1]["reason"] == "mismatch"


def test_each_check_is_shown_in_steps_of_a_quarter_and_the_score_in_steps_of_a_twentieth() -> None:
    expect = {"checks": [{"id": "n", "path": "$.n", "approx": 100, "tolerance": 10}], "threshold": 0.5}
    answer = _score(expect, {"n": 117})  # 0.3 left of the near band: shown as 0.25
    assert answer["checks"][1] == {"id": "n", "score": 0.25, "reason": "near"}
    assert answer["score"] == 0.65 and answer["passed"]


def test_nothing_the_consumer_asked_could_be_scored_means_no_score_never_a_pass() -> None:
    answer = _score({"checks": [{"id": "tone", "criterion": "polite"}], "threshold": 0.5}, {"text": "hi"})
    assert answer["score"] is None and not answer["passed"]
    # With no checks of its own, the consumer asked only for the output schema.
    assert _score({"checks": []}, {"text": "hi"})["score"] == 1.0


@pytest.mark.parametrize(
    "expect",
    [
        {"checks": [{"id": "a"}]},
        {"checks": [{"id": "a", "equals": 1, "range": [0, 2]}]},
        {"checks": [{"id": "a", "equals": 1, "tolerance": 1}]},
        {"checks": [{"id": "a", "path": "$..n", "equals": 1}]},
        {"checks": [{"id": "a", "approx": 1, "tolerance": "-5%"}]},
        {"checks": [{"id": "a", "approx": 1, "tolerance": "abc"}]},
        {"checks": [{"id": "a", "range": [True, 5]}]},
        {"checks": [{"id": "a", "within": {"of": "17 Nov", "days": 1}}]},
        {"checks": [{"id": "schema", "equals": 1}]},
        {"checks": [{"id": "a", "equals": 1}, {"id": "a", "equals": 2}]},
        {"checks": [], "threshold": 1.5},
    ],
)
def test_an_expectation_the_network_would_refuse_is_refused_here_before_any_call(expect: dict[str, Any]) -> None:
    with pytest.raises(AgentError) as refused:
        validate_expectation(expect)
    assert refused.value.code == "INVALID_REQUEST"


def test_words_need_the_networks_model_which_the_fake_has_not() -> None:
    with pytest.raises(AgentError) as refused:
        validate_expectation("says the total is right")
    assert refused.value.code == "MODEL_UNAVAILABLE"
