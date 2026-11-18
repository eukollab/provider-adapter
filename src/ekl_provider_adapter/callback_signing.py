# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Callback protocol v2: the keys, MACs and headers the connector and the agent's callbacks file share.

The callbacks file is given k_cb = HMAC-SHA256(credential, "ekl-callback-v2") as EKL_CALLBACK_KEY (hex), never the
credential.
A request MAC covers its time, event ID, agent ID and the body's digest; a response MAC covers the same plus the
digest of the request it answers, its status and its body. The two labels differ, so a signed request can never
pass as a signed response, nor the other way round, and an answer can't be replayed against another request.
"""

import datetime as dt
import hashlib
import hmac

REQUEST_HEADER = "X-EKL-Signature-2"
RESPONSE_HEADER = "X-EKL-Response-Signature-2"
# The v1 header: an HMAC of the body keyed by the credential itself, sent only to apps that never answered with v2.
LEGACY_HEADER = "X-EKL-Signature"
KEY_LABEL = b"ekl-callback-v2"
REQUEST_LABEL = "ekl-cb-req-v2"
RESPONSE_LABEL = "ekl-cb-resp-v2"
# A signed time further than this from the receiver's clock is refused: a captured request can't be replayed later.
WINDOW_SECONDS = 300
# Apps that never answered with v2 keep v1 and unsigned answers until this day, then v1 is gone.
LEGACY_UNTIL = dt.datetime(2027, 2, 11, tzinfo=dt.UTC)
MAX_BODY = 1_048_576


def callback_key(credential: str) -> bytes:
    """k_cb, the 32 bytes both sides MAC with."""
    return hmac.new(credential.encode("utf-8"), KEY_LABEL, hashlib.sha256).digest()


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _mac(key: bytes, *parts: str) -> str:
    return hmac.new(key, "\n".join(parts).encode("utf-8"), hashlib.sha256).hexdigest()


def request_mac(key: bytes, t: int, event_id: str, agent_id: str, body: bytes) -> str:
    return _mac(key, REQUEST_LABEL, str(t), event_id, agent_id, digest(body))


def response_mac(
    key: bytes, t: int, event_id: str, agent_id: str, request_digest: str, status: int, body: bytes
) -> str:
    return _mac(key, RESPONSE_LABEL, str(t), event_id, agent_id, request_digest, str(status), digest(body))


def request_header(key: bytes, t: int, event_id: str, agent_id: str, body: bytes) -> str:
    return f"t={t},id={event_id},v={request_mac(key, t, event_id, agent_id, body)}"


def parse(header: str) -> dict[str, str]:
    """t=…,id=…,d=…,v=… as a dict; an unparsable header gives {}."""
    found: dict[str, str] = {}
    for part in header.split(","):
        name, sep, value = part.strip().partition("=")
        if not sep or name in found:
            return {}
        found[name] = value
    return found


def response_valid(
    key: bytes,
    header: str,
    *,
    event_id: str,
    agent_id: str,
    request_digest: str,
    status: int,
    body: bytes,
    now: float,
) -> bool:
    """Whether a response is signed by the app for this request: its ID, the digest of this exact request body, a
    time within the window, and the MAC over its status and body."""
    fields = parse(header)
    if fields.get("id") != event_id or fields.get("d") != request_digest:
        return False
    try:
        t = int(fields.get("t", ""))
    except ValueError:
        return False
    if abs(now - t) > WINDOW_SECONDS:
        return False
    expected = response_mac(key, t, event_id, agent_id, request_digest, status, body)
    return hmac.compare_digest(expected, fields.get("v", ""))


def legacy_signature(credential: str, body: bytes) -> str:
    return "sha256=" + hmac.new(credential.encode(), body, hashlib.sha256).hexdigest()
