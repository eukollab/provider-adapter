# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The agent's configuration, as far as the adapter needs it.

The adapter deploys with its agent.json, announces it and serves calls from the version its publisher
confirmed, which it downloads with its provider credential. Both are in the same format. The adapter reads
the execution mode, the adapter settings and, per offered capability, the tool, class, repeat behaviour and effect
handling. `${NAME}` values in the local file are read from the environment, so the file holds no secrets.
"""

import base64
import binascii
import copy
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

ENV_REFERENCE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
IDENT = re.compile(r"[^a-z0-9_]+")
DEFAULT_NETWORK_URL = "https://eukollab.ai"

CapabilityClass = Literal["read_only", "transformation", "external_action"]
Repeat = Literal["return_kept", "status_only", "refresh"]


class _Open(BaseModel):
    # agent.json carries the whole description; the adapter models only what it uses.
    model_config = ConfigDict(extra="allow")


class Effects(_Open):
    """How an external action's effect is made safe: an argument the adapter fills with the admission ID for
    the downstream to deduplicate on, an outcome lookup hook, or escalation; and the argument that makes it a dry
    run."""

    idempotency_argument: str | None = None
    outcome_lookup: str | None = None
    escalate: bool = False
    dry_run_argument: str | None = None


class InputData(_Open):
    # Whether the durable journal may keep a call's arguments, for example for an outcome lookup.
    journal: bool = False


class CapabilityData(_Open):
    inputs: InputData = Field(default_factory=InputData)


class Refreshes(_Open):
    """The refresh allowance of a `refresh` capability: this many per execution, within this many hours."""

    per_execution: int = Field(ge=0)
    within_hours: int = Field(ge=1)


class Capability(_Open):
    tool: str
    capability_id: str | None = None
    version: str = "1.0.0"
    class_: CapabilityClass = Field(alias="class")
    repeat: Repeat | None = None
    effects: Effects = Field(default_factory=Effects)
    cancellation: str | None = None
    max_duration_seconds: int = Field(default=60, ge=1)
    output_schema: dict[str, Any] | None = None
    input_schema: dict[str, Any] | None = None
    data: CapabilityData = Field(default_factory=CapabilityData)
    plans: list[dict[str, Any]] | None = None
    refreshes: Refreshes | None = None
    storage_free_accepted: bool = False
    # Kept in the configuration but not offered on the network: the adapter neither lists nor serves it.
    offered: bool = True

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    @property
    def derived_capability_id(self) -> str:
        """The capability ID the network gives this tool when agent.json names none."""
        if self.capability_id:
            return self.capability_id
        name = IDENT.sub("_", self.tool.lower()).strip("_")[:40] or "tool"
        return "cap_" + (name if len(name) >= 2 else name + "_x")

    @property
    def paid(self) -> bool:
        return any(str(p.get("type", "free")) != "free" for p in self.plans or [])


class AdapterSettings(_Open):
    # Required by the format: the network reports a file without it, and the adapter announces nothing until set.
    endpoint: str | None = None
    path: str = "/ekl"
    result_retention_days: int = Field(default=7, ge=1)
    execution_id_retention_days: int = Field(default=30, ge=1)
    storage: str | None = None


class Upstream(_Open):
    mcp_url: str


class Network(_Open):
    url: str = DEFAULT_NETWORK_URL


class Defaults(_Open):
    plans: list[dict[str, Any]] | None = None
    data: CapabilityData | None = None


class Payment(_Open):
    """Where consumers pay the provider for its paid services: its own page, never EuKollab's."""

    link: str
    note: str | None = None


class AgentFile(_Open):
    """agent.json as the adapter reads it. Loading applies the file's defaults to each capability and
    refuses a file whose capabilities miss their requirements, so the adapter never serves one that would."""

    ekl: str
    mode: Literal["service_provider", "active_participant"]
    execution_mode: Literal["storage_free", "durable"]
    adapter: AdapterSettings = Field(default_factory=AdapterSettings)
    upstream: Upstream | None = None
    network: Network = Field(default_factory=Network)
    defaults: Defaults = Field(default_factory=Defaults)
    payment: Payment | None = None
    capabilities: list[Capability]

    @model_validator(mode="after")
    def _apply_defaults_and_check(self) -> "AgentFile":
        for cap in self.capabilities:
            if cap.plans is None:
                cap.plans = list(self.defaults.plans or [{"type": "free"}])
            if "data" not in cap.model_fields_set and self.defaults.data is not None:
                cap.data = self.defaults.data
            if cap.repeat is None:
                cap.repeat = default_repeat(cap, self.execution_mode)
        problems = list(_problems(self))
        if problems:
            raise ValueError("; ".join(problems))
        return self

    def capability(self, tool: str) -> Capability | None:
        return next((c for c in self.offered if c.tool == tool), None)

    @property
    def offered(self) -> list[Capability]:
        return [c for c in self.capabilities if c.offered]


def default_repeat(cap: Capability, mode: str) -> Repeat:
    """Default repeat behaviour: free lookups refresh; paid storage-free work reports status; durable keeps results."""
    if cap.class_ == "external_action":
        return "return_kept" if mode == "durable" else "status_only"
    if cap.paid and not cap.refreshes:
        return "return_kept" if mode == "durable" else "status_only"
    return "refresh"


def _problems(file: AgentFile) -> list[str]:
    found = []
    tools = [c.tool for c in file.capabilities]
    if len(tools) != len(set(tools)):
        found.append("each tool may be listed once")
    for cap in file.offered:
        where = f"capability {cap.tool}"
        if cap.class_ == "external_action" and file.execution_mode != "durable":
            found.append(f"{where}: external actions need execution_mode durable")
        if cap.class_ == "external_action" and cap.repeat == "refresh":
            found.append(f"{where}: an external action's effect is never repeated; use return_kept or status_only")
        if cap.class_ == "external_action" and not (
            cap.effects.idempotency_argument or cap.effects.outcome_lookup or cap.effects.escalate
        ):
            found.append(f"{where}: declare an idempotency argument, an outcome lookup or escalate")
        if cap.repeat == "return_kept" and file.execution_mode != "durable":
            found.append(f"{where}: return_kept needs execution_mode durable")
        if cap.paid and file.execution_mode == "storage_free" and not cap.storage_free_accepted:
            found.append(f"{where}: a paid capability in storage-free mode needs storage_free_accepted")
        if cap.paid and cap.repeat == "refresh" and not cap.refreshes:
            found.append(f"{where}: a paid capability offers refreshes only with a declared quota and window")
    return found


def _expand(value: Any) -> Any:
    if isinstance(value, str):

        def lookup(match: re.Match[str]) -> str:
            name = match.group(1)
            if name not in os.environ:
                raise ValueError(f"agent.json refers to ${{{name}}}, which is not set")
            return os.environ[name]

        return ENV_REFERENCE.sub(lookup, value)
    if isinstance(value, list):
        return [_expand(v) for v in value]
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    return value


# Settings only the adapter reads: the network ignores them, so they are stripped before announcing.
LOCAL_SETTINGS = {"adapter": ("path", "storage"), "upstream": ("mcp_url",), "network": ("url",)}


def strip_local(raw: dict[str, Any]) -> dict[str, Any]:
    """agent.json as the network keeps it."""
    found = copy.deepcopy(raw)
    for section, keys in LOCAL_SETTINGS.items():
        part = found.get(section)
        if isinstance(part, dict):
            for key in keys:
                part.pop(key, None)
            if not part:
                del found[section]
    return found


@dataclass(frozen=True)
class LocalFile:
    """The agent.json deployed with the adapter: what it announces, and the local settings only it reads.

    `config` is the announced form: ${NAME} expanded, local settings stripped. `path` and `text` are kept for a file
    on disk, so a changed file is noticed and announced again."""

    agent: AgentFile
    config: dict[str, Any]
    path: Path | None = None
    text: str | None = None

    @classmethod
    def read(cls, source: str | Path | dict[str, Any] | AgentFile) -> "LocalFile":
        if isinstance(source, AgentFile):
            return cls(source, strip_local(source.model_dump(mode="json", by_alias=True, exclude_unset=True)))
        if isinstance(source, dict):
            raw = _expand(source)
            return cls(AgentFile.model_validate(raw), strip_local(raw))
        path = Path(source)
        text = path.read_text(encoding="utf-8")
        raw = _expand(json.loads(text))
        return cls(AgentFile.model_validate(raw), strip_local(raw), path, text)


def inline_agent_file(text: str) -> dict[str, Any]:
    """agent.json given as text in the environment (EKL_AGENT_JSON): the JSON itself, or base64 of it, which some
    hosting consoles need for a value with quotes and newlines."""
    found = text.strip()
    if not found.startswith("{"):
        try:
            # Standard or URL-safe alphabet, padded or not.
            found = base64.b64decode(found.replace("-", "+").replace("_", "/") + "=" * (-len(found) % 4)).decode()
        except (binascii.Error, UnicodeDecodeError) as exc:
            raise ValueError("EKL_AGENT_JSON is neither agent.json nor base64 of it") from exc
    try:
        raw = json.loads(found)
    except json.JSONDecodeError as exc:
        raise ValueError(f"EKL_AGENT_JSON is not valid JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError("EKL_AGENT_JSON must hold one JSON object, agent.json's content")
    return raw


def load_agent_file(source: str | Path | dict[str, Any]) -> AgentFile:
    """agent.json from a path or an already parsed dict, with ${NAME} references expanded."""
    return LocalFile.read(source).agent
