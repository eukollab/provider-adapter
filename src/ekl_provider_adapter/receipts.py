# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Checking a network-signed receipt claim by claim, before the consumer acknowledges it (EKP 1.2 §9.5.1).

A valid signature only says the network signed something. The consumer accepts a receipt only when it is a receipt
(alg ES256, typ ekl-receipt+jwt), signed by a key in the network's key set, issued by its network to this agent, for
this exact call (execution, both agents, capability and input fingerprint), and issued while the call was under way,
at the moment its issued_at says.
Anything else raises ReceiptInvalid and the receipt is not acknowledged.
"""

import datetime as dt
import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any

import httpx2
import jwt
from jwt.algorithms import ECAlgorithm

from ekl_provider_adapter.errors import AgentError

RECEIPT_TYP = "ekl-receipt+jwt"
SKEW_SECONDS = 60
KEY_SET_SECONDS = 600


class ReceiptInvalid(AgentError):  # noqa: N818 - named as the protocol names the refusal
    """A receipt that fails one of the checks: it is neither acknowledged nor relied on."""

    def __init__(self, reason: str) -> None:
        super().__init__(f"The receipt was refused: {reason}.", code="RECEIPT_INVALID")
        self.reason = reason


@dataclass(frozen=True)
class CallMade:
    """What the consumer knows of the call it made, which the receipt must match."""

    consumer_agent_id: str
    provider_agent_id: str
    execution_id: str
    capability_id: str
    input_fingerprint: str
    sent_at: float
    answered_at: float


def receipt_digest(receipt_jws: str) -> str:
    """SHA-256 (hex) of the compact JWS's ASCII bytes: what collaboration_report and abuse_report carry."""
    return hashlib.sha256(receipt_jws.encode("ascii")).hexdigest()


class KeySet:
    """The network's public keys, kept 10 minutes and fetched again for a kid not in them (a rotation)."""

    def __init__(self, network_url: str, http: httpx2.AsyncClient | None = None, keys: dict[str, Any] | None = None):
        self.url = network_url.rstrip("/") + "/.well-known/jwks.json"
        self._http = http
        self._keys: dict[str, Any] = {}
        self._fetched = 0.0
        if keys is not None:
            self._load(keys)

    def _load(self, jwks: dict[str, Any]) -> None:
        self._keys = {
            str(k["kid"]): ECAlgorithm.from_jwk(json.dumps(k)) for k in jwks.get("keys") or [] if k.get("kty") == "EC"
        }
        self._fetched = time.monotonic()

    async def _refresh(self) -> None:
        client = self._http or httpx2.AsyncClient(timeout=10, follow_redirects=False)
        try:
            answer = await client.get(self.url)
            answer.raise_for_status()
            self._load(answer.json())
        except (httpx2.HTTPError, ValueError, KeyError) as exc:
            raise ReceiptInvalid(f"the network's key set could not be read ({type(exc).__name__})") from exc
        finally:
            if self._http is None:
                await client.aclose()

    async def key(self, kid: str) -> Any:
        if kid not in self._keys or time.monotonic() - self._fetched > KEY_SET_SECONDS:
            await self._refresh()
        if kid not in self._keys:
            raise ReceiptInvalid(f"signed with key {kid!r}, which the network doesn't publish")
        return self._keys[kid]


async def verify(receipt_jws: str, call: CallMade, issuer: str, keys: KeySet) -> dict[str, Any]:
    """The receipt's claims when every check holds; ReceiptInvalid naming the first that doesn't."""
    try:
        header = jwt.get_unverified_header(receipt_jws)
    except jwt.PyJWTError as exc:
        raise ReceiptInvalid("not a compact JWS") from exc
    # Type and algorithm are pinned before any key is used: no other token passes as a receipt.
    if header.get("alg") != "ES256" or header.get("typ") != RECEIPT_TYP:
        raise ReceiptInvalid("not an ES256 ekl-receipt+jwt")
    key = await keys.key(str(header.get("kid") or ""))
    try:
        claims: dict[str, Any] = jwt.decode(
            receipt_jws,
            key,
            algorithms=["ES256"],
            audience=call.consumer_agent_id,
            issuer=issuer.rstrip("/"),
            options={"require": ["iss", "aud", "iat"], "verify_iat": False},
        )
    except jwt.InvalidAudienceError as exc:
        raise ReceiptInvalid("issued to another agent") from exc
    except jwt.InvalidIssuerError as exc:
        raise ReceiptInvalid("issued by another network") from exc
    except jwt.PyJWTError as exc:
        raise ReceiptInvalid("the signature doesn't verify") from exc
    expected = {
        "execution_id": call.execution_id,
        "provider_agent_id": call.provider_agent_id,
        "requester_agent_id": call.consumer_agent_id,
        "capability_id": call.capability_id,
        "input_fingerprint": call.input_fingerprint,
    }
    for name, value in expected.items():
        if claims.get(name) != value:
            raise ReceiptInvalid(f"its {name} isn't this call's")
    iat = claims.get("iat")
    if not isinstance(iat, int | float) or not (call.sent_at - SKEW_SECONDS <= iat <= call.answered_at + SKEW_SECONDS):
        raise ReceiptInvalid("issued outside the call")
    # The signed time and the receipt's own issued_at are one moment: a token re-wrapping another receipt fails here.
    try:
        issued = dt.datetime.fromisoformat(str(claims.get("issued_at", "")).replace("Z", "+00:00")).timestamp()
    except ValueError as exc:
        raise ReceiptInvalid("its issued_at isn't a time") from exc
    if int(issued) != int(iat):
        raise ReceiptInvalid("its iat isn't its issued_at")
    return claims
