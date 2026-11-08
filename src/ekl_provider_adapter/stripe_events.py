# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Approving paid runs from the publisher's own Stripe account without a webhook, for an adapter with no public
address (`ekl agent run`, relay mode). With a Stripe key (EKL_STRIPE_KEY, or `stripe_key` to attach; a restricted key
with read access to Events and Checkout Sessions is enough) the adapter asks Stripe's Events API for paid Checkouts,
every few seconds while a run waits for payment and never otherwise, and approves each run whose reservation ID is a
Checkout's `client_reference_id`, as the webhook would. Once at start it looks back over the time a run can wait, so a
run paid while the adapter was stopped is still approved.

Every request goes from the adapter to Stripe: the payment stays between the consumer and the provider, and the
network only sees the approval."""

import asyncio
import logging
import time
from decimal import Decimal
from typing import Any

import httpx2

from ekl_provider_adapter.runs import Runs, RunsApiError
from ekl_provider_adapter.stripe_webhook import PAID_EVENTS, paid_run

log = logging.getLogger("ekl_provider_adapter")

STRIPE_API = "https://api.stripe.com"
EVERY_SECONDS = 5.0
# A run waits at most fifteen minutes for its payment; looking back an hour at start covers it with room to spare.
LOOKBACK_SECONDS = 3600
# Events are asked for from a little before the last check, because Stripe may list an event a moment after it was
# created; the ones already handled are skipped by their ID.
OVERLAP_SECONDS = 120
# The network writes reservation IDs this way, so Checkouts of the publisher's other products are left alone.
RESERVATION_PREFIX = "rsv_"


class StripeError(Exception):
    def __init__(self, message: str, *, retryable: bool) -> None:
        super().__init__(message)
        self.retryable = retryable


class StripeEvents:
    """Paid Checkouts read from the publisher's Stripe account, each approving the run it names once."""

    def __init__(
        self,
        runs: Runs,
        key: str,
        *,
        http: httpx2.AsyncClient | None = None,
        api_url: str = STRIPE_API,
        every: float = EVERY_SECONDS,
    ) -> None:
        self._runs = runs
        self._headers = {"Authorization": f"Bearer {key}"}
        self._http = http or httpx2.AsyncClient(timeout=30)
        self._url = api_url.rstrip("/") + "/v1/events"
        self._every = every
        self._seen: dict[str, float] = {}
        # Paid events the network could not be asked about yet (it was unreachable): tried again on each pass, as
        # they may fall out of the window the next listing covers.
        self._pending: dict[str, tuple[str, Decimal, str]] = {}
        if key.startswith("sk_"):
            log.warning(
                "EKL_STRIPE_KEY is a secret key; a restricted key that reads Events and Checkout Sessions is enough"
            )

    async def _list(self, since: int) -> list[dict[str, Any]]:
        """Every paid-Checkout event created since `since`, newest first, across Stripe's pages."""
        found: list[dict[str, Any]] = []
        params: dict[str, Any] = {"types[]": sorted(PAID_EVENTS), "created[gte]": since, "limit": 100}
        while True:
            try:
                response = await self._http.get(self._url, params=params, headers=self._headers)
                page: dict[str, Any] = response.json()
            except (httpx2.HTTPError, ValueError) as exc:
                raise StripeError(f"Stripe is unreachable: {exc}", retryable=True) from exc
            if response.status_code >= 400:
                message = str((page.get("error") or {}).get("message") or response.status_code)
                # A wrong key or missing permission stays wrong until the publisher fixes it; still checked again.
                raise StripeError(message, retryable=response.status_code == 429 or response.status_code >= 500)
            events = [e for e in page.get("data") or [] if isinstance(e, dict)]
            found.extend(events)
            if not page.get("has_more") or not events:
                return found
            params["starting_after"] = events[-1].get("id")

    async def check(self, since: int) -> int:
        """Approve the runs paid since `since`; the number approved."""
        approved = 0
        now = time.monotonic()
        for stale in [k for k, at in self._seen.items() if now - at > 2 * LOOKBACK_SECONDS]:
            del self._seen[stale]
        listed: dict[str, tuple[str, Decimal, str]] = dict(self._pending)
        for event in reversed(await self._list(since)):
            event_id = str(event.get("id") or "")
            found = paid_run(event)
            if event_id in self._seen or found is None or not found[0].startswith(RESERVATION_PREFIX):
                continue
            listed[event_id] = found
        for event_id, found in listed.items():
            try:
                done = await self._runs.paid(*found)
            except RunsApiError as exc:
                # The network didn't decide: kept, and applied on a later pass.
                self._pending[event_id] = found
                log.warning("could not approve run %s yet: %s", found[0], exc)
                continue
            # Handled once decided either way: a refused one (the run ended, or another copy approved it) stays so.
            self._pending.pop(event_id, None)
            self._seen[event_id] = now
            approved += done
        return approved

    async def watch(self) -> None:
        """Check Stripe while runs wait for payment, until cancelled."""
        since = int(time.time()) - LOOKBACK_SECONDS
        first, backoff = True, self._every
        while True:
            started = int(time.time())
            if first or self._pending or self._runs.awaiting_payment():
                try:
                    await self.check(since)
                    first, backoff = False, self._every
                except StripeError as exc:
                    if not exc.retryable or backoff == self._every:
                        log.warning("could not read paid Checkouts from Stripe: %s", exc)
                    await asyncio.sleep(min(backoff, 300.0))
                    backoff *= 2
                    continue
            since = started - OVERLAP_SECONDS
            await asyncio.sleep(self._every)
