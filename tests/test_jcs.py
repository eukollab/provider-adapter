# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""RFC 8785 canonicalisation against the network's ekl-json-1 vectors."""

import hashlib
import json

import pytest

from ekl_provider_adapter.jcs import canonical, parse
from tests.conftest import CONTRACTS

VECTORS = json.loads((CONTRACTS / "vectors" / "ekl-json-1.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("vector", VECTORS["valid"], ids=[v["name"] for v in VECTORS["valid"]])
def test_valid_vectors(vector: dict[str, str]) -> None:
    text = canonical(parse(vector["input"]))
    assert text == vector["canonical"]
    assert hashlib.sha256(text.encode("utf-8")).hexdigest() == vector["sha256"]


@pytest.mark.parametrize(
    "vector", VECTORS.get("invalid", []), ids=[v.get("name", "") for v in VECTORS.get("invalid", [])]
)
def test_invalid_vectors(vector: dict[str, str]) -> None:
    with pytest.raises(ValueError):
        canonical(parse(vector["input"]))
