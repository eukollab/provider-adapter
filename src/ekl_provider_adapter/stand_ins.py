# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Stand-ins: a service's recorded answers, so an agent is written and tested against another agent's
service before it takes part. `ekl try <@handle> <service> --record` writes them to
eukollab/fixtures/<handle>.<service>.json in the project (EKL_FIXTURES names another folder); the connector and the
TypeScript client read the same files.

A stand-in answers only while the agent has no agent-operation credential. Once it has one, calls always go to the
network, so a stand-in can never hide a live failure.
"""

import json
import os
from pathlib import Path
from typing import Any

import jsonschema

from ekl_provider_adapter.errors import AgentError, StandInMissing

DEFAULT_FOLDER = Path("eukollab") / "fixtures"


def folder() -> Path:
    return Path(os.environ.get("EKL_FIXTURES") or DEFAULT_FOLDER)


def _split(ref: str) -> tuple[str, str]:
    handle, _, name = ref.partition("/")
    if not handle.startswith("@") or not name:
        raise AgentError(f"Name the service as @handle/service, not {ref!r}.", code="INVALID_REQUEST")
    return handle, name


def path_for(ref: str, where: Path | None = None) -> Path:
    handle, name = _split(ref)
    return (where or folder()) / f"{handle.removeprefix('@')}.{name}.json"


def load(ref: str, where: Path | None = None) -> dict[str, Any] | None:
    path = path_for(ref, where)
    if not path.exists():
        return None
    found: dict[str, Any] = json.loads(path.read_text())
    return found


def every(where: Path | None = None) -> list[dict[str, Any]]:
    """Every stand-in in the folder, for FakeNetwork."""
    base = where or folder()
    return [json.loads(p.read_text()) for p in sorted(base.glob("*.json"))] if base.is_dir() else []


def answer(ref: str | None, arguments: dict[str, Any], where: Path | None = None) -> Any:
    """The recorded output for these arguments: the example with the same input, or else the first one. The input
    is checked against the schema it was recorded with, so code that would send a wrong shape fails here first."""
    if not ref:
        raise StandInMissing(None)
    fixture = load(ref, where)
    if fixture is None or not fixture.get("examples"):
        raise StandInMissing(ref)
    try:
        jsonschema.validate(arguments, fixture.get("input_schema") or {"type": "object"})
    except jsonschema.ValidationError as exc:
        raise AgentError(
            f"{ref} would not accept this input: {exc.message}",
            code="INVALID_INPUT",
            fix=f"Compare with its schema: ekl show {ref.replace('/', ' ')}",
        ) from exc
    examples = fixture["examples"]
    same = next((e for e in examples if e.get("input") == arguments), examples[0])
    return same.get("output")
