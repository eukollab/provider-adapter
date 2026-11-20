# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""An A2A agent, reached as if it were an MCP server (EKP 3.4 §5.4).

Each skill on its Agent Card becomes one tool named by the skill's id. Most skills take and give text (their input
modes name no JSON type): such a text skill's tool takes and returns one `text` field, sent as the message's one text
part and answered with the reply's text parts joined. A data skill, one taking JSON, is sent its arguments as one
data part and answers with its first object data part; it declares no schemas, so they come from agent.json, where
the core checks them. Either way the message's metadata names the skill (`skill`) and the call waits for the task to
end. A failed, rejected or cancelled task is a tool error; one that asks for input or authorisation is a tool error
without effect carrying the question, so the caller is not charged. A task still working at the deadline is
cancelled and the call times out.

A2A 1.0 (JSON-RPC, SendMessage) and 0.3 (message/send) are both spoken: the card says which.
"""

import asyncio
import contextlib
import re
import time
import uuid
from typing import Any
from urllib.parse import urljoin

import httpx2
import mcp_types as types

CARD_PATH = "/.well-known/agent-card.json"
_NAME = re.compile(r"[^A-Za-z0-9_.-]+")
POLL_SECONDS = 0.5
DEFAULT_DEADLINE = 60.0
TERMINAL_OK = "completed"
TERMINAL_FAILED = frozenset({"failed", "canceled", "rejected"})
INTERRUPTED = frozenset({"input_required", "auth_required"})
ACCEPTED_OUTPUT = ["application/json", "text/plain"]


class A2AError(RuntimeError):
    """The agent answered with a JSON-RPC error: the request was refused, so nothing ran."""


def skill_tool_name(skill_id: str) -> str:
    return _NAME.sub("_", skill_id).strip("_")[:128] or "skill"


def card_url(url: str) -> str:
    """Where the agent's card is: the URL itself when it names a .json file, else the well-known path under it."""
    return url if url.split("?", maxsplit=1)[0].endswith(".json") else url.rstrip("/") + CARD_PATH


def interface(card: dict[str, Any], fetched_from: str) -> tuple[str, str]:
    """The JSON-RPC address and protocol version the card offers: 1.0 lists supportedInterfaces, 0.3 has url and
    protocolVersion at the top."""
    for entry in card.get("supportedInterfaces") or []:
        if isinstance(entry, dict) and str(entry.get("protocolBinding", "")).upper() in ("JSONRPC", "JSON-RPC"):
            return urljoin(fetched_from, str(entry["url"])), str(entry.get("protocolVersion") or "1.0")
    if card.get("url"):
        transport = str(card.get("preferredTransport") or "JSONRPC").upper()
        if transport not in ("JSONRPC", "JSON-RPC"):
            raise A2AError(f"the agent prefers {transport}; only JSON-RPC is supported")
        return urljoin(fetched_from, str(card["url"])), str(card.get("protocolVersion") or "0.3.0")
    raise A2AError("the agent card offers no JSON-RPC interface")


def is_text_skill(skill: dict[str, Any], card: dict[str, Any]) -> bool:
    """Whether the skill takes text rather than JSON: its input modes, else the card's, name no JSON type (A2A's
    default is text)."""
    modes = skill.get("inputModes") or card.get("defaultInputModes") or []
    return not any("json" in str(mode).lower() for mode in modes)


def skill_tool(skill: dict[str, Any], card: dict[str, Any]) -> dict[str, Any]:
    """A skill as an MCP tool. It has no hints of its own (the publisher settles the class); a data skill has no
    schemas either, so agent.json supplies them."""
    described = str(skill.get("description") or "").strip() or str(skill.get("name") or "").strip()
    tool: dict[str, Any] = {
        "name": skill_tool_name(str(skill.get("id") or skill.get("name") or "")),
        "description": described or None,
        "inputSchema": {"type": "object"},
    }
    if is_text_skill(skill, card):
        asked: dict[str, Any] = {"type": "string", "description": "What to ask the agent"}
        examples = [e for e in skill.get("examples") or [] if isinstance(e, str) and e.strip()]
        if examples:
            asked["examples"] = examples
        tool["inputSchema"] = {
            "type": "object",
            "properties": {"text": asked},
            "required": ["text"],
            "additionalProperties": False,
        }
        tool["outputSchema"] = {
            "type": "object",
            "properties": {"text": {"type": "string", "description": "The agent's answer"}},
            "required": ["text"],
        }
    return tool


def state_of(task: dict[str, Any]) -> str:
    """A task's state in one spelling: TASK_STATE_INPUT_REQUIRED (1.0) and input-required (0.3) are input_required."""
    raw = str((task.get("status") or {}).get("state") or "")
    return raw.lower().removeprefix("task_state_").replace("-", "_")


def _parts(message: dict[str, Any] | None) -> list[dict[str, Any]]:
    return [p for p in ((message or {}).get("parts") or []) if isinstance(p, dict)]


def _data(part: dict[str, Any]) -> Any:
    return part["data"] if "data" in part and part.get("kind") in (None, "data") else None


def _text(part: dict[str, Any]) -> str | None:
    text = part.get("text")
    return text if isinstance(text, str) and part.get("kind") in (None, "text") else None


def task_result(task: dict[str, Any], text_skill: bool = False) -> types.CallToolResult:
    """The outcome of a finished or interrupted task, as a tool result."""
    state = state_of(task)
    status_parts = _parts((task.get("status") or {}).get("message"))
    if state == TERMINAL_OK:
        parts = [p for artifact in task.get("artifacts") or [] for p in _parts(artifact)] or status_parts
        return parts_result(parts, text_skill)
    said = " ".join(t for p in status_parts if (t := _text(p))) or state.replace("_", " ")
    if state in INTERRUPTED:
        return types.CallToolResult(
            content=[types.TextContent(type="text", text=said)],
            is_error=True,
            meta={"ekl": {"effect": "none", "code": "NEEDS_INPUT", "question": said[:2000]}},
        )
    meta = {"ekl": {"effect": "none"}} if state == "rejected" else None
    return types.CallToolResult(content=[types.TextContent(type="text", text=said)], is_error=True, meta=meta)


def parts_result(parts: list[dict[str, Any]], text_skill: bool = False) -> types.CallToolResult:
    """A reply's parts as a tool result: a text skill's answer is its text parts joined, a data skill's its first
    object data part."""
    structured: dict[str, Any] | None = None
    content: list[types.TextContent] = []
    for part in parts:
        data = _data(part)
        if structured is None and isinstance(data, dict):
            structured = data
        text = _text(part)
        if text is not None:
            content.append(types.TextContent(type="text", text=text))
    if text_skill:
        structured = {"text": "\n".join(c.text for c in content)}
    return types.CallToolResult(content=content, structured_content=structured)


class A2AUpstream:
    """The A2A agent at `url` (its base address or its card's). `headers` (EKL_UPSTREAM_HEADERS) carry the agent's
    key and go with every request; redirects are never followed."""

    def __init__(
        self,
        url: str,
        *,
        http: httpx2.AsyncClient | None = None,
        headers: dict[str, str] | None = None,
        deadline: float = DEFAULT_DEADLINE,
    ) -> None:
        self.url = url
        self.headers = dict(headers or {})
        self.deadline = deadline
        self._http = http

    def _client(self) -> httpx2.AsyncClient:
        if self._http is None:
            self._http = httpx2.AsyncClient(timeout=self.deadline + 5, follow_redirects=False)
        return self._http

    async def card(self) -> dict[str, Any]:
        response = await self._client().get(card_url(self.url), headers=self.headers)
        response.raise_for_status()
        found = response.json()
        if not isinstance(found, dict):
            raise A2AError("the agent card is not a JSON object")
        return found

    async def tools(self) -> list[types.Tool]:
        card = await self.card()
        return [types.Tool.model_validate(skill_tool(s, card)) for s in card.get("skills") or [] if isinstance(s, dict)]

    async def list(self) -> list[types.Resource]:
        return []

    async def read(self, uri: str) -> types.ReadResourceResult:
        raise LookupError(f"an A2A upstream serves no resources ({uri})")

    async def _rpc(self, endpoint: str, version: str, method: str, params: dict[str, Any]) -> Any:
        legacy = version.startswith("0.")
        names = (
            {"send": "message/send", "get": "tasks/get", "cancel": "tasks/cancel"}
            if legacy
            else {
                "send": "SendMessage",
                "get": "GetTask",
                "cancel": "CancelTask",
            }
        )
        headers = {**self.headers, "content-type": "application/json"}
        if not legacy:
            headers["A2A-Version"] = ".".join([*version.split("."), "0"][:2])
        body = {"jsonrpc": "2.0", "id": uuid.uuid4().hex, "method": names[method], "params": params}
        response = await self._client().post(endpoint, json=body, headers=headers)
        response.raise_for_status()
        reply = response.json()
        if not isinstance(reply, dict):
            raise A2AError("the agent's answer is not a JSON-RPC response")
        if reply.get("error"):
            error = reply["error"] if isinstance(reply["error"], dict) else {}
            raise A2AError(str(error.get("message") or "the agent refused the request"))
        return reply.get("result")

    async def call(self, tool: str, arguments: dict[str, Any]) -> types.CallToolResult:
        card = await self.card()
        skills = {skill_tool_name(str(s.get("id") or s.get("name") or "")): s for s in card.get("skills") or []}
        if tool not in skills:
            return _refused(f"the agent has no skill {tool}")
        endpoint, version = interface(card, card_url(self.url))
        legacy = version.startswith("0.")
        skill_id = str(skills[tool].get("id") or tool)
        text_skill = is_text_skill(skills[tool], card)
        part = message_part(arguments, text_skill, legacy)
        message: dict[str, Any] = {
            "messageId": uuid.uuid4().hex,
            "role": "user" if legacy else "ROLE_USER",
            "parts": [part],
            "metadata": {"skill": skill_id},
        }
        if legacy:
            message["kind"] = "message"
        configuration = (
            {"blocking": True, "acceptedOutputModes": ACCEPTED_OUTPUT}
            if legacy
            else {"returnImmediately": False, "acceptedOutputModes": ACCEPTED_OUTPUT}
        )
        try:
            result = await self._rpc(endpoint, version, "send", {"message": message, "configuration": configuration})
        except A2AError as refused:
            return _refused(str(refused))
        return await self._settled(endpoint, version, result, text_skill)

    async def _settled(self, endpoint: str, version: str, result: Any, text_skill: bool) -> types.CallToolResult:
        """The call's result once its task ends: a reply message ends it at once, a task is polled until it ends or
        the deadline passes."""
        if not isinstance(result, dict):
            raise A2AError("the agent's answer holds no task or message")
        if "message" in result or result.get("kind") == "message":
            return parts_result(_parts(result.get("message", result)), text_skill)
        task = result.get("task", result)
        started = time.monotonic()
        while state_of(task) not in {TERMINAL_OK, *TERMINAL_FAILED, *INTERRUPTED}:
            if time.monotonic() - started >= self.deadline:
                await self._cancel(endpoint, version, str(task.get("id")))
                raise TimeoutError("the agent's task did not end before the deadline")
            await asyncio.sleep(POLL_SECONDS)
            found = await self._rpc(endpoint, version, "get", {"id": str(task.get("id"))})
            task = found.get("task", found) if isinstance(found, dict) else task
        if state_of(task) in INTERRUPTED:
            # The caller can't answer mid-call, so the task is let go rather than left waiting.
            await self._cancel(endpoint, version, str(task.get("id")))
        return task_result(task, text_skill)

    async def _cancel(self, endpoint: str, version: str, task_id: str) -> None:
        # Best effort: the task ends on the agent's side either way.
        with contextlib.suppress(A2AError, httpx2.HTTPError):
            await self._rpc(endpoint, version, "cancel", {"id": task_id})


def message_part(arguments: dict[str, Any], text_skill: bool, legacy: bool) -> dict[str, Any]:
    """The one part a call sends: a text skill's text field as text, a data skill's arguments as data. 0.3 names each
    part's kind."""
    part: dict[str, Any] = {"text": str(arguments.get("text") or "")} if text_skill else {"data": arguments}
    return {"kind": "text" if text_skill else "data", **part} if legacy else part


def _refused(message: str) -> types.CallToolResult:
    return types.CallToolResult(
        content=[types.TextContent(type="text", text=message)], is_error=True, meta={"ekl": {"effect": "none"}}
    )
