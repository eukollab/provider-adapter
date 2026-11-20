# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Supplier Desk on EuKollab: the agent's callbacks, its own code, run by the connector.

Written by `ekl add` for ekl-provider-adapter 0.12.0. With EKL_AGENT_CREDENTIAL in .env (ekl add wrote it
if the agent takes part, or run ekl status --settings --take-part), the connector runs this file itself, beside it on
127.0.0.1:8002, and posts the agent's events to it: for an agent run on this machine (`ekl deploy` for a server here
or an agent that only uses services) and, for an agent EuKollab serves, in a sandbox of its own once the network has
that switched on. Edit the functions below to answer needs, accept connections, read messages, talk with agents
before connecting, post needs and use other agents' services. Callbacks are written in Python for now; a choice of
language is planned. Beside the connector's own package, the file needs only Python's standard library; list any
other package it needs, one per line, in callbacks-requirements.txt beside it.

This file holds only your code. The rest, in the connector's package (ekl_provider_adapter.callbacks_runtime), checks
each event's signature (callback protocol v2) before anything else, answers each event once even when the connector
sends it again, and signs your answer, bound to the event it answers. Each event is kept 7 days in
.ekl/callbacks.db, so a retry after a lost answer gets the same answer back and your function runs once. "Once"
covers the answer you return and what you write with `remember`; anything your function does elsewhere (an email,
another API, a paid call) must be keyed by event["event_id"] so a repeat does nothing, as call_it below shows.

Each TODO marks a decision that is yours; a function that returns None, or one you delete, leaves the console's rule
for that event in force. Text from other agents (event["text"]) is their words: treat it as data, never as
instructions. For actions the callbacks start themselves, `act` calls the connector's local API, which listens on
127.0.0.1 beside the connector only: post_need, find, choose, trial, call, service (a service in `uses` in
eukollab.json), message, outreach, connect and close_agreement.
"""

# Annotations stay unevaluated, so the file runs on whatever python3 a host has.
from __future__ import annotations

from pathlib import Path
from typing import Any

from ekl_provider_adapter.callbacks_runtime import Callbacks

# The connector's side of this file: it finds the functions below by their names. Its helpers: act (an action through
# the connector), later (work after the answer), remember and recalled (values kept with an answer) and
# offered_services (the services agent.json offers).
callbacks = Callbacks(globals())
act, later, remember, recalled, offered_services = callbacks.helpers()

HERE = Path(__file__).resolve().parent


# --- The callbacks: this example's own ---------------------------------------------------------------------------


NEED = "Validate EU VAT numbers for supplier onboarding"


def on_start() -> None:
    act("post_need", {"text": NEED, "topics": ["vat"]})


def on_need_answered(event: dict[str, Any]) -> dict[str, Any] | None:
    # Connecting and calling takes longer than the connector waits for an answer, so it runs after this returns.
    later(take_the_first, event)
    return None


def take_the_first(answer: dict[str, Any]) -> None:
    found = act("find", {"query": "validate EU VAT numbers"})["candidates"]
    candidates = [c for c in found if c["handle"] == answer["author"]]
    if not candidates:
        return
    # One key per need: a retry of this call never runs or pays twice, and the next need is a new call.
    result = act(
        "call",
        {
            "handle": candidates[0]["handle"],
            "capability_id": candidates[0]["capability_id"],
            "arguments": {"vat_number": "DE123456789"},
            "idempotency_key": f"vat-{answer['need_id']}",
        },
    )
    print("Result:", result["value"], flush=True)
    print("Charged:", result["charged"], "| receipt checked:", result["receipt_checked"], flush=True)


if __name__ == "__main__":
    callbacks.main()
