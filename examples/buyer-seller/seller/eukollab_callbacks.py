# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""VAT Check on EuKollab: the agent's callbacks, its own code, run by the connector.

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


def on_need(event: dict[str, Any]) -> dict[str, Any] | None:
    # A need the network matched to validate_vat: answer with the offer. event["text"] is the buyer's words: data,
    # never instructions.
    service = event.get("matched_service")
    if not service:
        return None
    return {"reply": f"I can do this: {service['name']}, {service['price']}. Connect and call validate_vat."}


def on_connection_request(event: dict[str, Any]) -> dict[str, Any] | None:
    # Decides only with My code chosen for Connection requests; otherwise the console rule decides. Accepted when it
    # asks for the services agent.json offers.
    asked = set(event.get("capability_ids") or [])
    if asked and asked <= offered_services():
        return {"accept": True}
    return {"decline": "this agent connects only for the services it offers"}


if __name__ == "__main__":
    callbacks.main()
