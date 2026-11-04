# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Fingerprint test vectors: every implementation, in any language, must reproduce them exactly.

    python -m ekl_provider_adapter.conformance.vectors > fingerprints.json

Each value vector gives a salt, a JSON value, its JCS text and the fingerprint. Each result vector gives a tool
result and the fingerprint of its body, showing that structuredContent wins over content and that isError and
_meta are left out.
"""

import json
import sys
from typing import Any

import mcp_types as types

from ekl_provider_adapter.fingerprint import fingerprint, result_body
from ekl_provider_adapter.jcs import canonical

SALT = "00" * 16 + "ff" * 16

VALUES: list[tuple[str, Any]] = [
    ("empty object", {}),
    ("empty array", []),
    ("null", None),
    ("literals", [True, False, None]),
    ("key order by UTF-16 code units", {"€": 1, "😀": 2, "a": 3, "ﬁ": 4, "\r": 5, "1": 6, "ö": 7}),
    ("integers", [0, 1, -1, 9007199254740991, -9007199254740991]),
    ("negative zero", [-0.0]),
    ("exponents", [1e21, 1e20, 1e-7, 1e-6, 1e30, 2e-3, 0.000000000000000000000000001]),
    ("doubles at the edges", [333333333.33333329, 4.5, 0.1, 5e-324, 1.7976931348623157e308]),
    ("string escapes", {"s": "€$\u000f\nA'B\"\\/\u0000\u001f\u007f\u2028"}),
    ("non-ASCII strings", {"name": "Zoë Ünal", "city": "東京", "emoji": "🏠"}),
    ("nested", {"invoice": {"lines": [{"qty": 10, "amount": 25.0}, {"qty": 20, "amount": 16}], "total": 41.0}}),
    ("arguments of a search", {"area_ref": "loc:ca:dublin", "min_beds": 3, "limit": 2}),
]

RESULTS: list[tuple[str, types.CallToolResult]] = [
    (
        "structuredContent is hashed, not content",
        types.CallToolResult(
            content=[types.TextContent(type="text", text='{"listings": ["home-1"]}')],
            structured_content={"listings": ["home-1"]},
        ),
    ),
    (
        "_meta and isError are excluded",
        types.CallToolResult(
            content=[types.TextContent(type="text", text="done")],
            structured_content={"listings": ["home-1"]},
            is_error=False,
            meta={"ekl": {"outcome": "succeeded", "admission_id": "adm_1"}},
        ),
    ),
    (
        "content array when there is no structuredContent",
        types.CallToolResult(content=[types.TextContent(type="text", text="hello")]),
    ),
]


def build() -> dict[str, Any]:
    """The vectors as published in contracts/vectors/fingerprints.json, computed by this package's own JCS and
    fingerprint code."""
    return {
        "description": (
            "Fingerprint vectors: SHA-256(salt || JCS(value)) with a 32-byte hex salt. Result vectors "
            "hash the result body: structuredContent when present, otherwise the content array; isError and _meta "
            "are excluded."
        ),
        "salt": SALT,
        "values": [
            {"name": name, "value": value, "jcs": canonical(value), "fingerprint": fingerprint(SALT, value)}
            for name, value in VALUES
        ],
        "results": [
            {
                "name": name,
                "result": result.model_dump(mode="json", by_alias=True, exclude_none=True),
                "body_jcs": canonical(result_body(result)),
                "fingerprint": fingerprint(SALT, result_body(result)),
            }
            for name, result in RESULTS
        ],
    }


def main() -> None:
    sys.stdout.write(json.dumps(build(), indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
