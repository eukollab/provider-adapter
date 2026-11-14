# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The adapter's own JCS reproduces EKP 1.2's upstream fingerprint vectors (PROTOCOL.md §5.1), which it reports with
its presence; the depth vectors sit on either side of the 64-level limit (§2.5); and the connector's callback
signing matches the informative callback vectors."""

import hashlib
import json
from typing import Any

import pytest

from ekl_provider_adapter.jcs import canonical
from tests.conftest import CONTRACTS

UPSTREAM = CONTRACTS / "vectors" / "upstream-fingerprint.json"
pytestmark = pytest.mark.skipif(not UPSTREAM.exists(), reason="the protocol checkout has no EKP 1.2 vectors")
FIELDS = ("name", "description", "inputSchema", "outputSchema", "annotations")


def upstream_fingerprint(tools: list[dict[str, Any]], declared: list[str]) -> str:
    chosen = sorted((t for t in tools if t["name"] in declared), key=lambda t: t["name"].encode("utf-16-be"))
    return hashlib.sha256(canonical([{f: t.get(f) for f in FIELDS} for t in chosen]).encode("utf-8")).hexdigest()


def cases() -> list[dict[str, Any]]:
    if not UPSTREAM.exists():
        return []
    found: list[dict[str, Any]] = json.loads(UPSTREAM.read_text(encoding="utf-8"))["cases"]
    return found


@pytest.mark.parametrize("case", cases(), ids=[c["name"] for c in cases()])
def test_upstream_fingerprint(case: dict[str, Any]) -> None:
    projected = [{f: t.get(f) for f in FIELDS} for t in case["tools_list"] if t["name"] in case["declared"]]
    assert sorted(json.dumps(p, sort_keys=True) for p in projected) == sorted(
        json.dumps(p, sort_keys=True) for p in case["projection"]
    )
    assert upstream_fingerprint(case["tools_list"], case["declared"]) == case["upstream_fingerprint"]


def _depth(value: Any) -> int:
    if isinstance(value, dict):
        return 1 + max((_depth(v) for v in value.values()), default=0)
    if isinstance(value, list):
        return 1 + max((_depth(v) for v in value), default=0)
    return 0


def test_depth_vectors() -> None:
    depth = json.loads((CONTRACTS / "vectors" / "ekl-json-1.json").read_text(encoding="utf-8"))["depth"]
    assert all(_depth(json.loads(c["input"])) <= depth["max_depth"] for c in depth["accepted"])
    assert all(_depth(json.loads(c["input"])) > depth["max_depth"] for c in depth["refused"])


def test_callback_signing_reproduces_the_callback_vectors() -> None:
    """The connector's callback v2 signing agrees with callback-v2.json (informative: callbacks are outside EKP)."""
    signing = pytest.importorskip("ekl_provider_adapter.callback_signing")
    v = json.loads((CONTRACTS / "vectors" / "callback-v2.json").read_text(encoding="utf-8"))
    key = signing.callback_key(v["agent_credential"])
    assert key.hex() == v["callback_key"]
    req, resp = v["request"], v["response"]
    assert (
        signing.request_header(key, req["t"], req["event_id"], req["agent_id"], req["body"].encode()) == req["header"]
    )
    assert signing.response_valid(
        key,
        resp["header"],
        event_id=req["event_id"],
        agent_id=req["agent_id"],
        request_digest=req["body_sha256"],
        status=resp["status"],
        body=resp["body"].encode(),
        now=resp["t"],
    )
    for case in (c for c in v["invalid"] if c["kind"] == "response"):
        assert not signing.response_valid(
            key,
            case["header"],
            event_id=req["event_id"],
            agent_id=req["agent_id"],
            request_digest=case["request_digest"],
            status=case["status"],
            body=case["body"].encode(),
            now=case["now"],
        ), case["name"]
