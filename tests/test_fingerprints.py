# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Fingerprint vectors: the committed file matches this implementation, and so does the protocol's copy."""

import json
from pathlib import Path

import mcp_types as types
import pytest

from ekl_provider_adapter.conformance.network import FakeNetwork
from ekl_provider_adapter.conformance.vectors import build
from ekl_provider_adapter.core import AdapterCore
from ekl_provider_adapter.fingerprint import fingerprint, new_salt, result_body
from ekl_provider_adapter.jcs import canonical
from tests.conftest import CONTRACTS

HERE = Path(__file__).parent
CONTRACT_VECTORS = CONTRACTS / "vectors" / "fingerprints.json"


def test_the_committed_vectors_are_current() -> None:
    assert json.loads((HERE / "vectors.json").read_text(encoding="utf-8")) == build()


def test_the_published_vectors_match() -> None:
    published = json.loads(CONTRACT_VECTORS.read_text(encoding="utf-8"))
    for case in published["cases"]:
        assert canonical(case["value"]) == case["jcs"], case["name"]
        assert fingerprint(case["salt"], case["value"]) == case["fingerprint"], case["name"]
    selection = published["result_selection"]
    result = types.CallToolResult.model_validate(selection["result"])
    salt = published["cases"][0]["salt"]
    assert fingerprint(salt, result_body(result)) == selection["fingerprint"]


def test_the_published_challenge_proof_matches() -> None:
    vector = json.loads(CONTRACT_VECTORS.read_text(encoding="utf-8"))["challenge"]

    async def unused(tool: str, arguments: dict[str, object]) -> types.CallToolResult:
        raise AssertionError((tool, arguments))

    core = AdapterCore(
        None, FakeNetwork(), unused, credential=vector["provider_credential"], endpoint=vector["endpoint"]
    )
    assert core.answer_challenge(vector["challenge"]) == vector["proof"]


def test_a_salt_is_32_bytes() -> None:
    assert len(bytes.fromhex(new_salt())) == 32
    with pytest.raises(ValueError):
        fingerprint("00", {})
