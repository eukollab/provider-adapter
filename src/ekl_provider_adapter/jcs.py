# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""RFC 8785 JSON Canonicalization (ekl-json-1), as the network computes it.

Fingerprints only match across implementations when every byte of the canonical form matches, so this follows
RFC 8785 exactly: ECMAScript number serialisation, minimal string escaping and keys ordered by UTF-16 code units.
"""

import json
import math
from decimal import Decimal
from typing import Any

MAX_SAFE_INTEGER = 2**53 - 1


def _number(value: float) -> str:
    """A finite double the way ECMAScript's Number.prototype.toString writes it."""
    if value == 0:
        return "0"  # also -0.0
    _, digit_tuple, exponent = Decimal(repr(abs(value))).normalize().as_tuple()
    digits = "".join(map(str, digit_tuple))
    k, n = len(digits), len(digits) + int(exponent)
    if k <= n <= 21:
        text = digits + "0" * (n - k)
    elif 0 < n <= 21:
        text = digits[:n] + "." + digits[n:]
    elif -6 < n <= 0:
        text = "0." + "0" * -n + digits
    else:
        mantissa = digits if k == 1 else digits[0] + "." + digits[1:]
        text = f"{mantissa}e{'+' if n - 1 > 0 else '-'}{abs(n - 1)}"
    return ("-" if value < 0 else "") + text


def _string(value: str) -> str:
    # RFC 8785 makes invalid Unicode an error. json.loads joins an escaped surrogate pair into one character, so a
    # surrogate left in a string is a lone one.
    if any("\ud800" <= c <= "\udfff" for c in value):
        raise ValueError("a lone surrogate is not valid Unicode")
    # json.dumps with ensure_ascii=False escapes exactly what RFC 8785 requires (quote, backslash, controls with the
    # short forms and lower-case \u00XX) and leaves everything else, including non-ASCII, as is.
    return json.dumps(value, ensure_ascii=False)


def canonical(value: Any) -> str:
    """The canonical text of a JSON value. Refuses what ekl-json-1 refuses: integers beyond the safe range, NaN and
    Infinity, non-string keys and invalid Unicode."""
    if value is None or isinstance(value, bool):
        return json.dumps(value)
    if isinstance(value, int):
        if abs(value) > MAX_SAFE_INTEGER:
            raise ValueError("integer outside the IEEE 754 safe range")
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("NaN and Infinity have no JSON form")
        return _number(value)
    if isinstance(value, str):
        return _string(value)
    if isinstance(value, list | tuple):
        return "[" + ",".join(canonical(v) for v in value) + "]"
    if isinstance(value, dict):
        if not all(isinstance(k, str) for k in value):
            raise ValueError("object keys must be strings")
        keys = sorted(value, key=lambda k: k.encode("utf-16-be", "surrogatepass"))
        return "{" + ",".join(_string(k) + ":" + canonical(value[k]) for k in keys) + "}"
    raise ValueError(f"{type(value).__name__} is not a JSON value")


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    found: dict[str, Any] = {}
    for key, value in pairs:
        if key in found:
            raise ValueError(f"duplicate key {key!r}")
        found[key] = value
    return found


def _constant(name: str) -> Any:
    raise ValueError(f"{name} has no JSON form")


def parse(text: str) -> Any:
    """Parse JSON as ekl-json-1 accepts it: no duplicate keys, no NaN or Infinity."""
    return json.loads(text, object_pairs_hook=_unique, parse_constant=_constant)
