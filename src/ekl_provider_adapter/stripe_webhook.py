# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Approving paid runs from the publisher's own Stripe account. With a webhook secret (EKL_STRIPE_WEBHOOK_SECRET, or
`stripe_webhook_secret` to attach) the adapter serves `<path>/stripe` beside its endpoint: point a Stripe webhook for
`checkout.session.completed` and `checkout.session.async_payment_succeeded` there, and each paid Checkout whose
`client_reference_id` is a run's reservation ID approves that run, with the plan's runs or period. The payment stays
on the publisher's account; the network never sees it, only the approval.

The signature is checked as Stripe documents it, with the standard library: no Stripe SDK is needed."""

import hashlib
import hmac
import json
import logging
import re
import time
from decimal import Decimal
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from ekl_provider_adapter.runs import Runs, RunsApiError

log = logging.getLogger("ekl_provider_adapter")

TOLERANCE_SECONDS = 300
PAID_EVENTS = frozenset({"checkout.session.completed", "checkout.session.async_payment_succeeded"})
# Stripe accepts letters, digits, dashes and underscores in client_reference_id, as reservation IDs are written.
_REFERENCE = re.compile(r"^[A-Za-z0-9_-]{1,200}$")
# Currencies Stripe states in whole units rather than hundredths.
_ZERO_DECIMAL = frozenset(
    {"bif", "clp", "djf", "gnf", "jpy", "kmf", "krw", "mga", "pyg", "rwf", "ugx", "vnd", "vuv", "xaf", "xof", "xpf"}
)


class SignatureError(ValueError):
    pass


def verify(payload: bytes, header: str, secret: str, *, at: float | None = None) -> dict[str, Any]:
    """The event, once its Stripe-Signature header proves it came from Stripe within the last five minutes."""
    parts: dict[str, list[str]] = {}
    for item in header.split(","):
        key, _, value = item.strip().partition("=")
        parts.setdefault(key, []).append(value)
    try:
        stamp = int(parts["t"][0])
    except (KeyError, ValueError) as exc:
        raise SignatureError("no timestamp in the signature") from exc
    expected = hmac.new(secret.encode(), f"{stamp}.".encode() + payload, hashlib.sha256).hexdigest()
    if not any(hmac.compare_digest(expected, given) for given in parts.get("v1", [])):
        raise SignatureError("the signature doesn't match")
    if abs((time.time() if at is None else at) - stamp) > TOLERANCE_SECONDS:
        raise SignatureError("the signature is too old")
    event: dict[str, Any] = json.loads(payload)
    return event


def paid_run(event: dict[str, Any]) -> tuple[str, Decimal, str] | None:
    """The reservation ID, amount and currency of a paid Checkout, or None for any other event."""
    if event.get("type") not in PAID_EVENTS:
        return None
    session = (event.get("data") or {}).get("object") or {}
    reference = str(session.get("client_reference_id") or "")
    if session.get("payment_status") != "paid" or not _REFERENCE.match(reference):
        return None
    currency = str(session.get("currency") or "usd").lower()
    total = Decimal(int(session.get("amount_total") or 0))
    return reference, total if currency in _ZERO_DECIMAL else total / 100, currency.upper()


def stripe_route(path: str, runs: Runs, secret: str) -> Route:
    """The webhook endpoint. Stripe retries anything but a 2xx, so an event that approves nothing is still taken."""

    async def webhook(request: Request) -> Response:
        payload = await request.body()
        try:
            event = verify(payload, request.headers.get("stripe-signature", ""), secret)
        except (SignatureError, ValueError) as exc:
            return JSONResponse({"error": str(exc)}, status_code=400)
        found = paid_run(event)
        try:
            approved = await runs.paid(*found) if found else False
        except RunsApiError as exc:  # retryable: Stripe delivers the event again later
            return JSONResponse({"error": str(exc)}, status_code=503)
        return JSONResponse({"received": True, "approved": approved})

    return Route(f"{path.rstrip('/')}/stripe", webhook, methods=["POST"])


def stripe_routes(path: str, runs: Runs, secret: str | None) -> list[Route]:
    return [stripe_route(path, runs, secret)] if secret else []
