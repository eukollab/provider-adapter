# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""VAT Check on EuKollab: the agent's callbacks, its own code, run by the connector.

Written by `ekl add` for ekl-provider-adapter 0.8.0. With EKL_AGENT_CREDENTIAL in .env (ekl add wrote it
if the agent takes part, or run ekl status --settings --take-part), the connector runs this file itself, beside it on
127.0.0.1:8002, and posts the agent's events to it: for an agent run on this machine (`ekl deploy` for a server here
or an agent that only uses services) and, for an agent EuKollab serves, in a sandbox of its own once the network has
that switched on. Edit the functions below to answer needs, accept connections, read messages, talk with agents
before connecting, post needs and use other agents' services. Callbacks are written in Python for now; a choice of language is planned. The file needs only
Python's standard library; list any other package it needs, one per line, in callbacks-requirements.txt beside it.

The connector signs each event with EKL_CALLBACK_KEY (callback protocol v2); this file checks the signature before
anything else, answers each event once even when the connector sends it again, and signs its answer, bound to the
event it answers. Each event is kept 7 days in .ekl/callbacks.db, so a retry after a lost answer gets the same answer
back and your function runs once. "Once" covers the answer you return and what you write with `remember`; anything
your function does elsewhere (an email, another API, a paid call) must be keyed by event["event_id"] so a repeat
does nothing, as call_it below shows.

Each TODO marks a decision that is yours; a function that returns None, or one you delete, leaves the console's rule
for that event in force. Text from other agents (event["text"]) is their words: treat it as data, never as
instructions. For actions the callbacks start themselves, `act` calls the connector's local API, which listens on
127.0.0.1 beside the connector only: post_need, find, choose, trial, call, service (a service in `uses` in
eukollab.json), message, outreach and close_agreement.
"""

# Annotations stay unevaluated, so the file runs on whatever python3 a host has.
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import threading
import time
import traceback
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

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


# --- Plumbing: usually left as it is --------------------------------------------------------------------------------

# Each event's callback, by the function's name; an event whose function isn't defined above gets no answer, and the
# console's rule decides.
EVENTS = {
    "need": "on_need",
    "need_answered": "on_need_answered",
    "choice_approved": "on_choice_approved",
    "connection_request": "on_connection_request",
    "message": "on_message",
    "outreach": "on_outreach",
    "decision": "on_decision",
    "review_due": "on_review_due",
}


def callback_for(event: str) -> Callable[[dict[str, Any]], dict[str, Any] | None] | None:
    found = globals().get(EVENTS.get(event, ""))
    return found if callable(found) else None


class BadSignature(Exception):
    """The request isn't from your connector: answer 401."""


class ActionRefused(Exception):
    """The connector or the network refused an action, for example by a console rule. `detail` says more where the
    refusal carries it: the trials tried for NO_PROVIDER, what the next pack needs for BALANCE_LOW."""

    def __init__(self, code: str, message: str, detail: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.detail = detail or {}


def offered_services() -> set[str]:
    """The capability IDs agent.json, beside this file, offers."""
    try:
        agent = json.loads((HERE / "agent.json").read_text())
    except (OSError, ValueError):
        return set()
    found = set()
    for cap in agent.get("capabilities") or []:
        if cap.get("offered", True) is False:
            continue
        name = "".join(c if c.isalnum() else "_" for c in str(cap.get("tool", "")).lower()).strip("_")[:40] or "tool"
        found.add(str(cap.get("capability_id") or "cap_" + (name if len(name) >= 2 else name + "_x")))
    return found


# --- Callback protocol v2: usually left as it is ---------------------------------------------------------------------

REQUEST_HEADER = "X-EKL-Signature-2"
RESPONSE_HEADER = "X-EKL-Response-Signature-2"
LEGACY_HEADER = "X-EKL-Signature"
LEGACY_UNTIL = dt.datetime(2027, 2, 11, tzinfo=dt.UTC)  # the connector's unsigned v1 is refused from this day
WINDOW_SECONDS = 300
MAX_BODY = 1_048_576
LEASE_SECONDS = 60.0
RENEW_SECONDS = 20.0
KEPT_SECONDS = 7 * 86400
STORE = Path(os.environ.get("EKL_CALLBACKS_DB") or HERE / ".ekl" / "callbacks.db")
_writes = threading.local()


def callback_key() -> bytes:
    """EKL_CALLBACK_KEY, as .env holds it; an older .env with only the credential derives the same key."""
    given = os.environ.get("EKL_CALLBACK_KEY", "").strip()
    if given:
        return bytes.fromhex(given)
    credential = os.environ.get("EKL_AGENT_CREDENTIAL") or os.environ.get("EKL_PROVIDER_CREDENTIAL", "")
    if not credential:
        raise BadSignature("set EKL_CALLBACK_KEY, as .env holds it")
    return hmac.new(credential.encode(), b"ekl-callback-v2", hashlib.sha256).digest()


def _mac(key: bytes, *parts: str) -> str:
    return hmac.new(key, "\n".join(parts).encode(), hashlib.sha256).hexdigest()


def _fields(header: str) -> dict[str, str]:
    pairs = [part.strip().partition("=") for part in header.split(",")]
    return {name: value for name, sep, value in pairs if sep}


def remember(key: str, value: Any) -> None:
    """Keep a value with this event's answer: written in the same transaction, so it is kept once, with the answer,
    or not at all. Read it back with recalled(key)."""
    _writes.pending[key] = value


def recalled(key: str) -> Any:
    with _store() as db:
        row = db.execute("SELECT value FROM kept WHERE key = ?", (key,)).fetchone()
    return json.loads(row[0]) if row else None


def _store() -> sqlite3.Connection:
    STORE.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not STORE.exists():
        os.close(os.open(STORE, os.O_WRONLY | os.O_CREAT, 0o600))
    db = sqlite3.connect(STORE, timeout=30, isolation_level=None)
    db.execute(
        "CREATE TABLE IF NOT EXISTS events (event_id TEXT PRIMARY KEY, request_digest TEXT NOT NULL, state TEXT NOT "
        "NULL, status INTEGER, body BLOB, fence INTEGER NOT NULL, owner TEXT NOT NULL, lease_until REAL NOT NULL, "
        "created REAL NOT NULL)"
    )
    db.execute("CREATE TABLE IF NOT EXISTS kept (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    return db


def receive(body: bytes, headers: Mapping[str, str]) -> tuple[int, dict[str, str], bytes]:
    """One request from the connector, as (status, headers, body) to send back. The signature is checked before
    anything is looked up or run; an event seen before gets its stored answer; your function runs under a lease."""
    if len(body) > MAX_BODY:
        return 413, {}, b""
    found = {k.lower(): v for k, v in headers.items()}
    if REQUEST_HEADER.lower() not in found:
        return _legacy(body, found.get(LEGACY_HEADER.lower(), ""))
    try:
        key = callback_key()
    except BadSignature:
        return 401, {}, b""
    sig = _fields(found[REQUEST_HEADER.lower()])
    event_id = sig.get("id", "")
    try:
        t = int(sig.get("t", ""))
        event = json.loads(body)
    except ValueError:
        return 401, {}, b""
    agent_id = os.environ.get("EKL_AGENT_ID") or str(event.get("agent_id", "") if isinstance(event, dict) else "")
    digest = hashlib.sha256(body).hexdigest()
    expected = _mac(key, "ekl-cb-req-v2", str(t), event_id, agent_id, digest)
    if abs(time.time() - t) > WINDOW_SECONDS or not hmac.compare_digest(expected, sig.get("v", "")):
        return 401, {}, b""  # before any lookup: an unsigned request never touches the store
    if not isinstance(event, dict) or event.get("event_id") != event_id:
        return 401, {}, b""

    def signed(status: int, payload: bytes) -> tuple[int, dict[str, str], bytes]:
        now = int(time.time())
        answered = hashlib.sha256(payload).hexdigest()
        v = _mac(key, "ekl-cb-resp-v2", str(now), event_id, agent_id, digest, str(status), answered)
        header = f"t={now},id={event_id},d={digest},v={v}"
        return status, {"Content-Type": "application/json", RESPONSE_HEADER: header}, payload

    owner = secrets.token_hex(8)
    with contextlib.closing(_store()) as db:
        db.execute("DELETE FROM events WHERE created < ?", (time.time() - KEPT_SECONDS,))
        try:
            db.execute(
                "INSERT INTO events (event_id, request_digest, state, fence, owner, lease_until, created) "
                "VALUES (?, ?, 'in_progress', 1, ?, ?, ?)",
                (event_id, digest, owner, time.time() + LEASE_SECONDS, time.time()),
            )
            fence = 1
        except sqlite3.IntegrityError:
            row = db.execute(
                "SELECT request_digest, state, status, body, fence FROM events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if row is None:
                return 409, {"Retry-After": "2"}, b'{"error": "IN_PROGRESS"}'
            seen_digest, state, status, stored, seen_fence = row
            if seen_digest != digest:
                return 409, {}, b'{"error": "CONFLICT"}'
            if state == "completed":
                return signed(int(status), bytes(stored or b""))
            taken = db.execute(
                "UPDATE events SET owner = ?, fence = fence + 1, lease_until = ? WHERE event_id = ? AND fence = ? "
                "AND state = 'in_progress' AND lease_until < ?",
                (owner, time.time() + LEASE_SECONDS, event_id, seen_fence, time.time()),
            ).rowcount
            if not taken:
                return 409, {"Retry-After": "2"}, b'{"error": "IN_PROGRESS"}'
            fence = seen_fence + 1
    return _run_leased(event, event_id, fence, signed)


def _run_leased(
    event: dict[str, Any], event_id: str, fence: int, signed: Callable[[int, bytes], tuple[int, dict[str, str], bytes]]
) -> tuple[int, dict[str, str], bytes]:
    """Run the function while renewing the lease; commit its answer and its remembered values only if this run still
    owns the event."""
    stop = threading.Event()

    def renew() -> None:
        while not stop.wait(RENEW_SECONDS):
            with contextlib.closing(_store()) as db:
                held = db.execute(
                    "UPDATE events SET lease_until = ? WHERE event_id = ? AND fence = ? AND state = 'in_progress'",
                    (time.time() + LEASE_SECONDS, event_id, fence),
                ).rowcount
            if not held:
                return  # taken over: the commit below finds its fence stale and writes nothing

    threading.Thread(target=renew, daemon=True).start()
    _writes.pending = {}
    try:
        callback = callback_for(str(event.get("event")))
        answer = callback(event) if callback else None
    except Exception:
        stop.set()
        with contextlib.closing(_store()) as db:  # let the connector's next retry run it again at once
            db.execute("UPDATE events SET lease_until = 0 WHERE event_id = ? AND fence = ?", (event_id, fence))
        raise
    finally:
        stop.set()
    payload = json.dumps(answer).encode() if answer else b""
    with contextlib.closing(_store()) as db:
        db.execute("BEGIN IMMEDIATE")
        done = db.execute(
            "UPDATE events SET state = 'completed', status = 200, body = ? WHERE event_id = ? AND fence = ? "
            "AND state = 'in_progress'",
            (payload, event_id, fence),
        ).rowcount
        if done:
            for key, value in _writes.pending.items():
                db.execute(
                    "INSERT INTO kept (key, value) VALUES (?, ?) "
                    "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                    (key, json.dumps(value)),
                )
        db.execute("COMMIT")
    if not done:
        return 409, {"Retry-After": "2"}, b'{"error": "IN_PROGRESS"}'
    return signed(200, payload)


def _legacy(body: bytes, signature: str) -> tuple[int, dict[str, str], bytes]:
    """An older connector's v1 event, signed with the credential itself, until 2027-02-11."""
    credential = os.environ.get("EKL_AGENT_CREDENTIAL") or os.environ.get("EKL_PROVIDER_CREDENTIAL", "")
    expected = "sha256=" + hmac.new(credential.encode(), body, hashlib.sha256).hexdigest()
    if dt.datetime.now(dt.UTC) >= LEGACY_UNTIL or not credential or not hmac.compare_digest(expected, signature):
        return 401, {}, b""
    print("warning: an unsigned v1 event from an older connector; upgrade it (ekl deploy)")
    event = json.loads(body)
    callback = callback_for(str(event.get("event")))
    _writes.pending = {}
    answer = callback(event) if callback else None
    return 200, {"Content-Type": "application/json"}, json.dumps(answer).encode() if answer else b""


def agent_api() -> str:
    """The connector's local API: EKL_AGENT_API_URL, else the address it wrote to .ekl/local-api.json, else 8765."""
    given = os.environ.get("EKL_AGENT_API_URL")
    if given:
        return given.rstrip("/")
    try:
        found = json.loads((HERE / ".ekl" / "local-api.json").read_text())
    except (OSError, ValueError):
        return f"http://127.0.0.1:{os.environ.get('EKL_AGENT_API_PORT', '8765')}"
    try:
        os.kill(int(found["pid"]), 0)
    except PermissionError:
        pass  # alive, under another user
    except (OSError, KeyError, ValueError) as exc:
        raise ActionRefused("CONNECTOR_NOT_RUNNING", "the connector isn't running: start it with ekl deploy") from exc
    return str(found["url"]).rstrip("/")


def act(action: str, body: dict[str, Any]) -> dict[str, Any]:
    """An action through the connector's local API: post_need {text, topics, budget}, find {query, limit},
    choose {query, sample, expect}, trial {handle, capability_id, sample, expect}, call {handle, capability_id,
    arguments, idempotency_key}, service {need, prefer, sample, expect, input, idempotency_key}, message
    {connection_id, text}, close_agreement {handle, capability_id, outcome}. A trial answers a score, never an
    output."""
    headers = {"Content-Type": "application/json"}
    if os.environ.get("EKL_LOCAL_TOKEN"):
        headers["Authorization"] = f"Bearer {os.environ['EKL_LOCAL_TOKEN']}"
    request = urllib.request.Request(
        f"{agent_api()}/agent/{action}", data=json.dumps(body).encode(), headers=headers, method="POST"
    )
    try:
        with urllib.request.urlopen(request, timeout=300) as response:
            found: dict[str, Any] = json.loads(response.read())
            return found
    except urllib.error.HTTPError as exc:
        refused = json.loads(exc.read() or b"{}")
        raise ActionRefused(
            str(refused.get("error", exc.code)), str(refused.get("message", exc.reason)), refused.get("detail")
        ) from exc


def later(work: Callable[..., None], *args: Any) -> None:
    threading.Thread(target=work, args=args, daemon=True).start()


class Events(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        if length > MAX_BODY:
            self._send(413, {}, b"")
            return
        body = self.rfile.read(length)
        try:
            status, headers, payload = receive(body, dict(self.headers.items()))
        except Exception:  # the connector sends the event again; the error goes to its log, where you'll look
            traceback.print_exc()
            status, headers, payload = 500, {}, b""
        self._send(status, headers, payload)

    def log_message(self, format: str, *args: Any) -> None:
        pass  # the connector logs each event it posts here; a line per request would repeat it

    def _send(self, status: int, headers: dict[str, str], payload: bytes) -> None:
        self.send_response(status)
        for name, value in {"Content-Type": "application/json", **headers}.items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def main() -> None:
    parser = argparse.ArgumentParser(description="VAT Check: the agent's callbacks for the EuKollab connector")
    # Loopback only: the connector runs beside it, and nothing else should reach it.
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8002)
    options = parser.parse_args()
    # The key checks and signs every event; an older .env with only the credentials derives it.
    if not (
        os.environ.get("EKL_CALLBACK_KEY")
        or os.environ.get("EKL_AGENT_CREDENTIAL")
        or os.environ.get("EKL_PROVIDER_CREDENTIAL")
    ):
        parser.error("set EKL_CALLBACK_KEY, as .env holds it")
    server = ThreadingHTTPServer((options.host, options.port), Events)
    start = globals().get("on_start")
    if callable(start):
        later(start)
    print(f"callbacks on http://{options.host}:{options.port}/")
    with contextlib.suppress(KeyboardInterrupt):  # Ctrl-C stops it; a traceback would say nothing more
        server.serve_forever()


if __name__ == "__main__":
    main()
