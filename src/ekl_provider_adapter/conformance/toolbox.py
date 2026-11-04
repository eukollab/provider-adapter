# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""A reference MCP server with one tool per capability shape the suite exercises, and the agent.json for it.

The tools count their work and their outside effects, and can be told to crash at a chosen point, so scenarios can
assert that nothing ran twice and that effects happened exactly as often as the rules allow.
"""

import asyncio
import contextlib
from typing import Any, Literal

from mcp.server.mcpserver import MCPServer

from ekl_provider_adapter.core import OutcomeQuery

ENDPOINT = "https://provider.example.com/ekl"


class Crash(BaseException):
    """The process stopping at an injected point. A BaseException, so nothing in the adapter can catch it."""


class Toolbox:
    """The reference server and its counters. `crash_at` names the point (`<tool>:<where>`) at which the next call
    stops the process, so a scenario can crash before or after a tool's effect."""

    def __init__(self) -> None:
        self.server = MCPServer("conformance-toolbox")
        self.lookups = 0
        self.transforms = 0
        self.sent: dict[str, dict[str, Any]] = {}
        self.effects = 0
        self.crash_at: str | None = None
        self.slow_started = asyncio.Event()
        self.slow_release = asyncio.Event()
        self.slow_stopped = False
        self._register()

    def _crash(self, point: str) -> None:
        if self.crash_at == point:
            self.crash_at = None
            raise Crash(point)

    def _register(self) -> None:
        server = self.server

        @server.tool()
        def lookup(area: str) -> dict[str, Any]:
            """Homes for sale in an area; each call sees a newer listing, so refreshes differ."""
            self._crash("lookup:start")
            self.lookups += 1
            return {"area": area, "listings": [f"home-{self.lookups}"]}

        @server.tool()
        def transform(text: str) -> dict[str, str]:
            """Upper-cases text (a paid transformation)."""
            self.transforms += 1
            return {"upper": text.upper()}

        @server.tool()
        def keep(text: str) -> dict[str, str]:
            """Reverses text; its exact result is kept for repeats."""
            self._crash("keep:start")
            return {"value": text[::-1]}

        @server.tool()
        def send(to: str, message: str, idempotency_key: str = "", dry_run: bool = False) -> dict[str, Any]:
            """Sends a message. The downstream honours the idempotency key: the same key never sends twice."""
            if dry_run:
                return {"sent": False, "dry_run": True}
            self._crash("send:before_effect")
            if idempotency_key not in self.sent:
                self.sent[idempotency_key] = {"to": to, "message": message}
                self.effects += 1
            self._crash("send:after_effect")
            return {"sent": True, "dry_run": False}

        @server.tool()
        def archive(record: str) -> dict[str, bool]:
            """An external action without a dry run."""
            del record
            return {"archived": True}

        @server.tool()
        def broken() -> dict[str, str]:
            """Always fails."""
            raise ValueError("broken by design")

        @server.tool()
        def bad_output() -> dict[str, int]:
            """Returns output its declared schema refuses."""
            return {"wrong": 1}

        @server.tool()
        async def slow(seconds: float) -> dict[str, bool]:
            """Works until released, stopped, or the time runs out."""
            self.slow_started.set()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.slow_release.wait(), timeout=seconds)
            return {"done": True}

    async def sent_lookup(self, query: OutcomeQuery) -> Literal["succeeded", "no_effect"] | None:
        """Outcome lookup for `send`: did the message keyed by this admission go out?"""
        return "succeeded" if query.admission_id in self.sent else "no_effect"

    async def stop_slow(self, admission_id: str) -> bool:
        """Cancellation hook for `slow`: stops the work and reports it stopped."""
        del admission_id
        self.slow_stopped = True
        self.slow_release.set()
        return True


def _common(mode: str) -> dict[str, Any]:
    return {
        "ekl": "1.0",
        "mode": "service_provider",
        "execution_mode": mode,
        "agent": {"handle": "@conformance", "display_name": "Conformance", "purpose": "Exercises the adapter"},
        "adapter": {"endpoint": ENDPOINT, "path": "/ekl"},
    }


def storage_free_agent() -> dict[str, Any]:
    """A free lookup (refresh) and a paid transformation (status_only, losses accepted)."""
    return _common("storage_free") | {
        "capabilities": [
            {"tool": "lookup", "class": "read_only", "repeat": "refresh"},
            {
                "tool": "transform",
                "class": "transformation",
                "plans": [{"type": "per_call", "price": "0.02"}],
                "storage_free_accepted": True,
            },
            {"tool": "broken", "class": "read_only"},
            {
                "tool": "bad_output",
                "class": "read_only",
                "output_schema": {"type": "object", "required": ["ok"], "properties": {"ok": {"type": "boolean"}}},
            },
            {"tool": "slow", "class": "read_only", "cancellation": "stop_slow", "max_duration_seconds": 30},
        ],
    }


def durable_agent() -> dict[str, Any]:
    """Adds a paid kept result and external actions, with journaled arguments for the outcome lookup."""
    return _common("durable") | {
        "capabilities": [
            {"tool": "lookup", "class": "read_only", "repeat": "refresh"},
            {"tool": "keep", "class": "transformation", "plans": [{"type": "per_call", "price": "0.05"}]},
            {
                "tool": "send",
                "class": "external_action",
                "outside_action": "send_messages",
                "effects": {
                    "idempotency_argument": "idempotency_key",
                    "outcome_lookup": "sent_lookup",
                    "dry_run_argument": "dry_run",
                },
                "data": {"inputs": {"journal": True}},
            },
            {"tool": "archive", "class": "external_action", "effects": {"escalate": True}},
            {"tool": "broken", "class": "read_only"},
            {"tool": "slow", "class": "read_only", "cancellation": "stop_slow", "max_duration_seconds": 30},
        ],
    }
