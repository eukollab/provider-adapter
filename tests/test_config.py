# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""agent.json rules the adapter enforces before anything starts."""

import json
from pathlib import Path
from typing import Any

import pytest

from ekl_provider_adapter.config import LocalFile, load_agent_file

BASE: dict[str, Any] = {"ekl": "1.0", "mode": "service_provider", "execution_mode": "storage_free"}
PACK = {"type": "pack", "price": "0.01"}


def file(*capabilities: dict[str, Any], **changes: Any) -> dict[str, Any]:
    return BASE | changes | {"capabilities": list(capabilities)}


def test_defaults_follow_the_spec() -> None:
    agent = load_agent_file(
        file(
            {"tool": "lookup", "class": "read_only"},
            {"tool": "paid", "class": "transformation", "plans": [PACK], "storage_free_accepted": True},
        )
    )
    assert [c.repeat for c in agent.capabilities] == ["refresh", "status_only"]
    durable = load_agent_file(
        file({"tool": "paid", "class": "transformation", "plans": [PACK]}, execution_mode="durable")
    )
    assert durable.capabilities[0].repeat == "return_kept"


@pytest.mark.parametrize(
    ("capability", "mode", "words"),
    [
        ({"tool": "send", "class": "external_action", "effects": {"escalate": True}}, "storage_free", "durable"),
        ({"tool": "send", "class": "external_action"}, "durable", "idempotency"),
        (
            {"tool": "t", "class": "transformation", "plans": [PACK]},
            "storage_free",
            "storage_free_accepted",
        ),
        (
            {"tool": "t", "class": "read_only", "plans": [PACK], "repeat": "refresh"},
            "durable",
            "quota",
        ),
        ({"tool": "t", "class": "read_only", "repeat": "return_kept"}, "storage_free", "return_kept"),
        (
            {"tool": "t", "class": "read_only", "plans": [{"type": "per_call", "price": "0.01"}]},
            "durable",
            "use a pack",
        ),
        (
            {"tool": "t", "class": "read_only", "plans": [{"type": "subscription", "price": "9"}]},
            "durable",
            "use a pack",
        ),
        ({"tool": "t", "class": "read_only", "plans": [{"type": "pack"}]}, "durable", "a pack needs a price"),
    ],
)
def test_what_the_spec_refuses(capability: dict[str, Any], mode: str, words: str) -> None:
    with pytest.raises(ValueError, match=words):
        load_agent_file(file(capability, execution_mode=mode))


def test_a_payment_page_is_refused_now_that_buyers_pay_from_their_wallet() -> None:
    with pytest.raises(ValueError, match="payment was removed"):
        load_agent_file(file({"tool": "t", "class": "read_only"}, payment={"link": "https://pay.example.com"}))


def test_values_come_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOMES_MCP_URL", "https://mcp.example.com/mcp/secret")
    agent = load_agent_file(file({"tool": "t", "class": "read_only"}, upstream={"mcp_url": "${HOMES_MCP_URL}"}))
    assert agent.upstream is not None and agent.upstream.mcp_url == "https://mcp.example.com/mcp/secret"
    with pytest.raises(ValueError, match="MISSING"):
        load_agent_file(file({"tool": "t", "class": "read_only"}, upstream={"mcp_url": "${MISSING}"}))


def test_defaults_apply_to_each_capability() -> None:
    agent = load_agent_file(file({"tool": "t", "class": "read_only"}, defaults={"data": {"inputs": {"journal": True}}}))
    assert agent.capabilities[0].data.inputs.journal is True


def test_terms_kept_in_a_file_are_announced_with_their_text(tmp_path: Path) -> None:
    # As ekl sync announces them: an announcement without the text would differ from the confirmed version.
    (tmp_path / "terms.md").write_text("FX Rates terms.\n")
    (tmp_path / "agent.json").write_text(
        json.dumps(
            file(
                {"tool": "convert", "class": "read_only"},
                agent={"handle": "@fx", "display_name": "FX", "purpose": "Converts."},
                adapter={},
                terms={"text_file": "terms.md", "version": "1"},
            )
        )
    )
    announced = LocalFile.read(tmp_path / "agent.json").config
    assert announced["terms"] == {"text_file": "terms.md", "version": "1", "text": "FX Rates terms.\n"}
