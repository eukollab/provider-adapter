# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Salted fingerprints of a call's input and output.

The salt is chosen by the consumer per execution and shared only with the provider, so the network can compare
fingerprints without being able to test guesses at small inputs.
"""

import hashlib
import secrets
from typing import Any

import mcp_types as types

from ekl_provider_adapter.jcs import canonical

SALT_BYTES = 32


def new_salt() -> str:
    """A fresh salt for one execution, as hex. The consumer chooses it and sends it only to the provider."""
    return secrets.token_hex(SALT_BYTES)


def fingerprint(salt_hex: str, value: Any) -> str:
    """SHA-256 over the 32 raw salt bytes followed by the value's RFC 8785 form, as lowercase hex. The input
    fingerprint covers the arguments as the consumer sent them; the output one covers `result_body`."""
    salt = bytes.fromhex(salt_hex)
    if len(salt) != SALT_BYTES:
        raise ValueError("the salt is 32 bytes, hex")
    return hashlib.sha256(salt + canonical(value).encode("utf-8")).hexdigest()


def result_body(result: types.CallToolResult) -> Any:
    """What the output fingerprint covers: structuredContent when present, otherwise the content array. isError,
    _meta and anything the adapter adds stay out, so both sides hash the same thing."""
    if result.structured_content is not None:
        return result.structured_content
    return [
        block.model_dump(mode="json", by_alias=True, exclude_none=True, exclude={"meta"}) for block in result.content
    ]


UPSTREAM_FIELDS = ("name", "description", "inputSchema", "outputSchema", "annotations")


def upstream_fingerprint(tools: list[types.Tool] | list[dict[str, Any]], declared: set[str]) -> str:
    """EKP 1.2 §5.1: SHA-256 of ekl-json-1 of the declared tools of the server's tools/list, sorted by name, each
    projected to its five interface fields, absent ones null. A tool changed upstream without a new agent.json changes
    it; nothing else of a tool does."""
    projected = []
    for tool in tools:
        wire = tool if isinstance(tool, dict) else tool.model_dump(mode="json", by_alias=True, exclude_none=True)
        if wire.get("name") in declared:
            projected.append({field: wire.get(field) for field in UPSTREAM_FIELDS})
    projected.sort(key=lambda t: str(t["name"]).encode("utf-16-be", "surrogatepass"))
    return hashlib.sha256(canonical(projected).encode("utf-8")).hexdigest()
