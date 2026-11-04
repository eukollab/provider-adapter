# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""agent.json rules the adapter enforces before anything starts."""

from typing import Any

import pytest

from ekl_provider_adapter.config import load_agent_file

BASE: dict[str, Any] = {"ekl": "1.0", "mode": "service_provider", "execution_mode": "storage_free"}


def file(*capabilities: dict[str, Any], **changes: Any) -> dict[str, Any]:
    return BASE | changes | {"capabilities": list(capabilities)}


def test_defaults_follow_the_spec() -> None:
    agent = load_agent_file(
        file(
            {"tool": "lookup", "class": "read_only"},
            {"tool": "paid", "class": "transformation", "plans": [{"type": "per_call"}], "storage_free_accepted": True},
        )
    )
    assert [c.repeat for c in agent.capabilities] == ["refresh", "status_only"]
    durable = load_agent_file(
        file({"tool": "paid", "class": "transformation", "plans": [{"type": "per_call"}]}, execution_mode="durable")
    )
    assert durable.capabilities[0].repeat == "return_kept"


@pytest.mark.parametrize(
    ("capability", "mode", "words"),
    [
        ({"tool": "send", "class": "external_action", "effects": {"escalate": True}}, "storage_free", "durable"),
        ({"tool": "send", "class": "external_action"}, "durable", "idempotency"),
        (
            {"tool": "t", "class": "transformation", "plans": [{"type": "per_call"}]},
            "storage_free",
            "storage_free_accepted",
        ),
        (
            {"tool": "t", "class": "read_only", "plans": [{"type": "per_call"}], "repeat": "refresh"},
            "durable",
            "quota",
        ),
        ({"tool": "t", "class": "read_only", "repeat": "return_kept"}, "storage_free", "return_kept"),
    ],
)
def test_what_the_spec_refuses(capability: dict[str, Any], mode: str, words: str) -> None:
    with pytest.raises(ValueError, match=words):
        load_agent_file(file(capability, execution_mode=mode))


def test_values_come_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOMES_MCP_URL", "https://mcp.example.com/mcp/secret")
    agent = load_agent_file(file({"tool": "t", "class": "read_only"}, upstream={"mcp_url": "${HOMES_MCP_URL}"}))
    assert agent.upstream is not None and agent.upstream.mcp_url == "https://mcp.example.com/mcp/secret"
    with pytest.raises(ValueError, match="MISSING"):
        load_agent_file(file({"tool": "t", "class": "read_only"}, upstream={"mcp_url": "${MISSING}"}))


def test_defaults_apply_to_each_capability() -> None:
    agent = load_agent_file(file({"tool": "t", "class": "read_only"}, defaults={"data": {"inputs": {"journal": True}}}))
    assert agent.capabilities[0].data.inputs.journal is True
