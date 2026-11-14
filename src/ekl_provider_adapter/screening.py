# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Screening another agent's result where it arrives, on the consumer's machine (EKP 1.2 §15.5).

A direct call's result never reaches the network, so it is screened here: against leak patterns of the adapter's own
(private keys, credentials) and the network's phrase rules, fetched signed from `GET /v1/screening/rules` and kept at
most 24 hours. A match becomes a flag, `{category, rules_version}`: the app sees it in `Result.safety_flags`, and the
network receives it with the call's report, bound to the verified receipt's digest. The content and the matched words
never leave this machine.

Only the consumer decides: `consumer.screen_results` in its own agent.json turns it off (it is on by default), and
nothing a provider declares can.
"""

import re
import time
import unicodedata
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import httpx2
import jwt

from ekl_provider_adapter.receipts import KeySet

RULES_TYP = "ekl-screening-rules+jwt"
RULES_KEPT_SECONDS = 24 * 3600
LEAK_CATEGORY = "credential_leak"
LOCAL_RULES = "local"
# What a result should never carry, whoever wrote it: keys and credentials in their recognisable forms.
LEAK_PATTERNS = (
    re.compile(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |ENCRYPTED |PGP )?PRIVATE KEY(?: BLOCK)?-----"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"\b(?:sk|rk)_live_[0-9A-Za-z]{16,}\b"),
    re.compile(r"\bgh[pousr]_[0-9A-Za-z]{36,}\b"),
    re.compile(r"\bxox[abposr]-[0-9A-Za-z-]{10,}\b"),
    re.compile(r"\bekl[apst]_[0-9A-Za-z_-]{20,}\b"),
    re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b"),
)


def _fold(text: str) -> str:
    return unicodedata.normalize("NFKC", text).casefold()


def strings(value: Any) -> Iterator[str]:
    """Every string in a JSON value, keys included."""
    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, str):
            yield item
        elif isinstance(item, dict):
            stack.extend(item.keys())
            stack.extend(item.values())
        elif isinstance(item, list | tuple):
            stack.extend(item)


@dataclass
class Rules:
    version: str
    rules: list[tuple[str, list[str]]] = field(default_factory=list)  # (category, folded phrases)
    expires: float = 0.0


class RulesInvalid(Exception):  # noqa: N818 - a refusal, as the protocol names it
    pass


async def verified_rules(token: str, issuer: str, keys: KeySet, now: float | None = None) -> Rules:
    """The rules a signed document holds, when alg, typ, kid, signature, iss and exp all check."""
    try:
        header = jwt.get_unverified_header(token)
    except jwt.PyJWTError as exc:
        raise RulesInvalid("not a compact JWS") from exc
    if header.get("alg") != "ES256" or header.get("typ") != RULES_TYP:
        raise RulesInvalid("not an ES256 ekl-screening-rules+jwt")
    try:
        key = await keys.key(str(header.get("kid") or ""))
    except Exception as exc:
        raise RulesInvalid("unknown key") from exc
    moment = time.time() if now is None else now
    try:
        claims = jwt.decode(
            token,
            key,
            algorithms=["ES256"],
            issuer=issuer.rstrip("/"),
            options={"require": ["iss", "iat", "exp"], "verify_exp": False, "verify_iat": False, "verify_aud": False},
        )
    except jwt.InvalidIssuerError as exc:
        raise RulesInvalid("another issuer") from exc
    except jwt.PyJWTError as exc:
        raise RulesInvalid("the signature doesn't verify") from exc
    if moment >= float(claims["exp"]):
        raise RulesInvalid("expired")
    found = [
        (str(r["category"]), [_fold(str(p)) for p in r.get("phrases") or [] if str(p).strip()])
        for r in claims.get("rules") or []
    ]
    return Rules(str(claims.get("rules_version") or ""), found, min(float(claims["exp"]), moment + RULES_KEPT_SECONDS))


class Screener:
    """Leak patterns always; the network's rules once fetched, refetched after 24 hours or their expiry."""

    def __init__(
        self,
        network_url: str | None,
        credential: str | None,
        *,
        http: httpx2.AsyncClient | None = None,
        keys: KeySet | None = None,
        rules: Rules | None = None,
    ) -> None:
        self._url = network_url.rstrip("/") if network_url else None
        self._credential = credential
        self._http = http
        self._keys = keys or (KeySet(network_url, http) if network_url else None)
        self._rules = rules

    async def _current(self) -> Rules | None:
        if self._rules is not None and time.time() < self._rules.expires:
            return self._rules
        if not (self._url and self._credential and self._keys):
            return self._rules if self._rules and time.time() < self._rules.expires else None
        client = self._http or httpx2.AsyncClient(timeout=10, follow_redirects=False)
        try:
            answer = await client.get(
                f"{self._url}/v1/screening/rules", headers={"Authorization": f"Bearer {self._credential}"}
            )
            answer.raise_for_status()
            self._rules = await verified_rules(str(answer.json()["rules"]), self._url, self._keys)
        except (httpx2.HTTPError, ValueError, KeyError, RulesInvalid):
            # Without valid rules the leak patterns still apply; the rules are asked for again on the next result.
            self._rules = None
        finally:
            if self._http is None:
                await client.aclose()
        return self._rules

    async def screen(self, value: Any) -> list[dict[str, str]]:
        """The flags a result raises: one per category, never the text that matched."""
        flags: dict[str, str] = {}
        texts = list(strings(value))
        if any(p.search(t) for t in texts for p in LEAK_PATTERNS):
            flags[LEAK_CATEGORY] = LOCAL_RULES
        rules = await self._current()
        if rules is not None:
            folded = [_fold(t) for t in texts]
            for category, phrases in rules.rules:
                if category not in flags and any(p in t for t in folded for p in phrases):
                    flags[category] = rules.version
        return [{"category": c, "rules_version": v} for c, v in flags.items()]
