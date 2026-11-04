# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Declaring the agent's EuKollab configuration in the server's own code.

What MCP's standard fields can't say (plans, data handling, terms, category, limitations, a tool's class where its
annotations leave it open) the server declares itself, so the code is the single source and `ekl agent sync` writes
agent.json from the running server:

    server = MCPServer("holiday-calendar", title="Holiday Calendar", description="Public holidays and long weekends.",
                       extensions=[EuKollab(interests=["calendar"], plans=[{"type": "free", "daily_limit": 200}])])

    @server.tool(title="Long weekends", meta=service(class_="read_only", category="calendar"))
    def long_weekends(...): ...

A server in another language returns the same JSON: the extension's settings in its capabilities, and the tool's
_meta. Nothing here reaches the network directly; the network reads only the agent.json the adapter announces.
"""

from typing import Any

from mcp.server.extension import Extension

AGENT_DECLARATION = "ai.eukollab/agent"
SERVICE_DECLARATION = "ai.eukollab/service"


def _given(**fields: Any) -> dict[str, Any]:
    return {k: v for k, v in fields.items() if v is not None}


class EuKollab(Extension):
    """The agent-wide declaration: the MCP extension `ai.eukollab/agent`, whose settings the server advertises.
    `data` and `plans` apply to every service unless a service declares its own; giving `data` declares how the
    agent handles data, unless `data_declared` says otherwise."""

    identifier = AGENT_DECLARATION

    def __init__(
        self,
        *,
        mode: str | None = None,
        execution_mode: str | None = None,
        display_name: str | None = None,
        purpose: str | None = None,
        interests: list[str] | None = None,
        languages: list[str] | None = None,
        logo: str | None = None,
        website: str | None = None,
        data: dict[str, Any] | None = None,
        data_declared: bool | None = None,
        terms: dict[str, Any] | None = None,
        plans: list[dict[str, Any]] | None = None,
    ) -> None:
        defaults = _given(data=data, plans=plans)
        self._settings = _given(
            mode=mode,
            execution_mode=execution_mode,
            display_name=display_name,
            purpose=purpose,
            interests=interests,
            languages=languages,
            logo=logo,
            website=website,
            data_declared=data_declared if data_declared is not None else (True if data is not None else None),
            terms=terms,
            defaults=defaults or None,
        )

    def settings(self) -> dict[str, Any]:
        return dict(self._settings)


def service(
    *,
    class_: str | None = None,
    offered: bool | None = None,
    name: str | None = None,
    summary: str | None = None,
    category: str | None = None,
    limitations: str | None = None,
    plans: list[dict[str, Any]] | None = None,
    data: dict[str, Any] | None = None,
    repeat: str | None = None,
    refreshes: dict[str, int] | None = None,
    storage_free_accepted: bool | None = None,
    outside_action: str | None = None,
    effects: dict[str, Any] | None = None,
    cancellation: str | None = None,
    max_duration_seconds: int | None = None,
    version: str | None = None,
    example: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """One tool's service declaration, for `@server.tool(meta=service(...))`. `class_` is the service's class
    (`read_only`, `transformation` or `external_action`); declaring it settles a tool whose annotations don't."""
    declared = _given(
        offered=offered,
        name=name,
        summary=summary,
        category=category,
        limitations=limitations,
        plans=plans,
        data=data,
        repeat=repeat,
        refreshes=refreshes,
        storage_free_accepted=storage_free_accepted,
        outside_action=outside_action,
        effects=effects,
        cancellation=cancellation,
        max_duration_seconds=max_duration_seconds,
        version=version,
        example=example,
    )
    if class_ is not None:
        declared["class"] = class_
    return {SERVICE_DECLARATION: declared}
