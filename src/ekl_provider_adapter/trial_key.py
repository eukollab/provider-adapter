# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The trial key: how a free trial reaches the publisher's server marked as one.

When a publisher allows free trials, it gives its connector a secret, the trial key (`EKL_TRIAL_KEY`, or, for a
connector EuKollab hosts, the reserved upstream header `EuKollab-Trial-Key`). The connector attaches it only to calls
the network admitted as trials, never to a paid call: an MCP server finds it in the call's `_meta` as
`{"eukollab": {"mode": "trial", "trial_key": ..., "dry_run": ...}}`, an HTTPS API or an A2A agent in the headers
`EuKollab-Mode: trial` and `EuKollab-Trial-Key`. A server that charges, meters or refuses calls without payment lets a
call carrying its key through as an exception; one that doesn't can ignore it. A service that acts in the world still
needs its dry run: the key doesn't replace it.

The core sets the trial for the one call it runs (`trial_call`), and each kind of server reads it while it sends that
call, so no call path carries it to a call it wasn't admitted for.
"""

import contextlib
import contextvars
import hmac
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

ENVIRONMENT = "EKL_TRIAL_KEY"
# The header a hosted connector's key travels in among the server's headers; never sent with an ordinary call.
KEY_HEADER = "EuKollab-Trial-Key"
MODE_HEADER = "EuKollab-Mode"


@dataclass(frozen=True)
class Trial:
    key: str
    dry_run: bool


_current: contextvars.ContextVar[Trial | None] = contextvars.ContextVar("ekl_trial", default=None)


@contextlib.contextmanager
def trial_call(key: str | None, admitted_kind: str, dry_run: bool) -> Iterator[None]:
    """Mark the call started inside as a free trial, with its key, when the network admitted it as one and the
    connector has a key; otherwise nothing is marked."""
    token = _current.set(Trial(key, dry_run)) if key and admitted_kind == "trial" else None
    try:
        yield
    finally:
        if token is not None:
            _current.reset(token)


def current() -> Trial | None:
    return _current.get()


def meta() -> dict[str, Any] | None:
    """The MCP `_meta` a trial carries to the server, or None for any other call."""
    found = current()
    if found is None:
        return None
    return {"eukollab": {"mode": "trial", "trial_key": found.key, "dry_run": found.dry_run}}


def headers() -> dict[str, str]:
    """The headers a trial carries to an HTTPS API or an A2A agent; none for any other call."""
    found = current()
    return {} if found is None else {MODE_HEADER: "trial", KEY_HEADER: found.key}


def split(server_headers: dict[str, str]) -> tuple[dict[str, str], str | None]:
    """The server's headers without the reserved trial-key header, and the key it carried (for hosted connectors)."""
    kept, key = {}, None
    for name, value in server_headers.items():
        if name.lower() == KEY_HEADER.lower():
            key = value
        else:
            kept[name] = value
    return kept, key


def matches(given: str | None, expected: str) -> bool:
    """For a publisher's server: whether a call carries its trial key, compared in constant time."""
    return bool(given) and hmac.compare_digest(str(given).encode(), expected.encode())
