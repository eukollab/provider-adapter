# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The connector's side of an agent's eukollab_callbacks.py: everything but the agent's own functions.

The callbacks file holds only the publisher's code, its on_* functions, and binds this to them:

    from ekl_provider_adapter.callbacks_runtime import ActionRefused, Callbacks

    callbacks = Callbacks(globals())
    act, later, remember, recalled, offered_services = callbacks.helpers()
    ...
    if __name__ == "__main__":
        callbacks.main()

This receives the events the connector posts to the file (callback protocol v2): it checks each signature before
anything else, answers each event once even when the connector sends it again (a record kept 7 days in
.ekl/callbacks.db beside the file), runs the event's function under a renewed lease, and signs its answer, bound to
the event it answers. Its helpers: `act`, an action through the connector's local API; `later`, work after the
answer; `remember` and `recalled`, values kept with an answer, written once with it or not at all; and
`offered_services`, the services agent.json beside the file offers. Standard library only, so it runs on whatever
Python the connector runs on.
"""

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
from collections.abc import Callable, Mapping, MutableMapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

# Each event's function, by its name in the callbacks file; an event whose function isn't defined gets no answer,
# and the console's rule decides.
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
REQUEST_HEADER = "X-EKL-Signature-2"
RESPONSE_HEADER = "X-EKL-Response-Signature-2"
LEGACY_HEADER = "X-EKL-Signature"
LEGACY_UNTIL = dt.datetime(2027, 2, 11, tzinfo=dt.UTC)  # the connector's unsigned v1 is refused from this day
WINDOW_SECONDS = 300
MAX_BODY = 1_048_576
LEASE_SECONDS = 60.0
RENEW_SECONDS = 20.0
KEPT_SECONDS = 7 * 86400

Handler = Callable[[dict[str, Any]], "dict[str, Any] | None"]
Answer = tuple[int, dict[str, str], bytes]


class BadSignature(Exception):  # noqa: N818 - the name callbacks files have always used
    """The request isn't from your connector: answer 401."""


class ActionRefused(Exception):  # noqa: N818 - the name callbacks files have always used
    """The connector or the network refused an action, for example by a console rule. `detail` says more where the
    refusal carries it: the trials tried for NO_PROVIDER, what the next pack needs for BALANCE_LOW."""

    def __init__(self, code: str, message: str, detail: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.detail = detail or {}


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


class Callbacks:
    """One agent's callbacks file: its functions (the module's own namespace, read when an event comes, so functions
    defined after this is made are found), its folder and its record of events."""

    def __init__(self, namespace: MutableMapping[str, Any]) -> None:
        self.namespace = namespace
        given = namespace.get("__file__")
        self.here = Path(given).resolve().parent if given else Path.cwd()
        self.store = Path(os.environ.get("EKL_CALLBACKS_DB") or self.here / ".ekl" / "callbacks.db")
        self._writes = threading.local()

    def helpers(
        self,
    ) -> tuple[
        Callable[[str, dict[str, Any]], dict[str, Any]],
        Callable[..., None],
        Callable[[str, Any], None],
        Callable[[str], Any],
        Callable[[], set[str]],
    ]:
        """act, later, remember, recalled and offered_services, bound to this file."""
        return self.act, self.later, self.remember, self.recalled, self.offered_services

    # --- what the functions use ---

    def remember(self, key: str, value: Any) -> None:
        """Keep a value with this event's answer: written in the same transaction, so it is kept once, with the
        answer, or not at all. Read it back with recalled(key)."""
        self._writes.pending[key] = value

    def recalled(self, key: str) -> Any:
        with contextlib.closing(self._store()) as db:
            row = db.execute("SELECT value FROM kept WHERE key = ?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def offered_services(self) -> set[str]:
        """The capability IDs agent.json, beside the callbacks file, offers."""
        try:
            agent = json.loads((self.here / "agent.json").read_text())
        except (OSError, ValueError):
            return set()
        found = set()
        for cap in agent.get("capabilities") or []:
            if cap.get("offered", True) is False:
                continue
            tool = str(cap.get("tool", "")).lower()
            name = "".join(c if c.isalnum() else "_" for c in tool).strip("_")[:40] or "tool"
            found.add(str(cap.get("capability_id") or "cap_" + (name if len(name) >= 2 else name + "_x")))
        return found

    def agent_api(self) -> str:
        """The connector's local API: EKL_AGENT_API_URL, else the address it wrote to .ekl/local-api.json, else
        8765."""
        given = os.environ.get("EKL_AGENT_API_URL")
        if given:
            return given.rstrip("/")
        try:
            found = json.loads((self.here / ".ekl" / "local-api.json").read_text())
        except (OSError, ValueError):
            return f"http://127.0.0.1:{os.environ.get('EKL_AGENT_API_PORT', '8765')}"
        try:
            os.kill(int(found["pid"]), 0)
        except PermissionError:
            pass  # alive, under another user
        except (OSError, KeyError, ValueError) as exc:
            raise ActionRefused(
                "CONNECTOR_NOT_RUNNING", "the connector isn't running: start it with ekl deploy"
            ) from exc
        return str(found["url"]).rstrip("/")

    def act(self, action: str, body: dict[str, Any]) -> dict[str, Any]:
        """An action through the connector's local API: post_need {text, topics, budget}, find {query, limit},
        choose {query, sample, expect}, trial {handle, capability_id, sample, expect}, call {handle, capability_id,
        arguments, idempotency_key}, message {connection_id, text}, outreach {to, intent, text, need_id or
        capability_id, idempotency_key} or {thread_id, text} to answer a thread, connect {handle, capability_id,
        thread_id, note}, close_agreement {handle, capability_id, outcome} and service {need, prefer, sample, expect,
        input, idempotency_key}. A trial answers a score, never an output. A refusal raises ActionRefused with the
        network's code: CONNECTION_PENDING when a provider hasn't accepted the connection yet, before this call
        times out."""
        headers = {"Content-Type": "application/json"}
        if os.environ.get("EKL_LOCAL_TOKEN"):
            headers["Authorization"] = f"Bearer {os.environ['EKL_LOCAL_TOKEN']}"
        request = urllib.request.Request(
            f"{self.agent_api()}/agent/{action}", data=json.dumps(body).encode(), headers=headers, method="POST"
        )
        try:
            with urllib.request.urlopen(request, timeout=300) as response:  # nosec B310
                found: dict[str, Any] = json.loads(response.read())
                return found
        except urllib.error.HTTPError as exc:
            refused = json.loads(exc.read() or b"{}")
            raise ActionRefused(
                str(refused.get("error", exc.code)), str(refused.get("message", exc.reason)), refused.get("detail")
            ) from exc

    def later(self, work: Callable[..., None], *args: Any) -> None:
        """Work after the answer: a thread of its own, as the connector waits only so long for an answer."""
        threading.Thread(target=work, args=args, daemon=True).start()

    # --- receiving events ---

    def callback_for(self, event: str) -> Handler | None:
        found = self.namespace.get(EVENTS.get(event, ""))
        return found if callable(found) else None

    def _store(self) -> sqlite3.Connection:
        self.store.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if not self.store.exists():
            os.close(os.open(self.store, os.O_WRONLY | os.O_CREAT, 0o600))
        db = sqlite3.connect(self.store, timeout=30, isolation_level=None)
        db.execute(
            "CREATE TABLE IF NOT EXISTS events (event_id TEXT PRIMARY KEY, request_digest TEXT NOT NULL, state TEXT "
            "NOT NULL, status INTEGER, body BLOB, fence INTEGER NOT NULL, owner TEXT NOT NULL, lease_until REAL NOT "
            "NULL, created REAL NOT NULL)"
        )
        db.execute("CREATE TABLE IF NOT EXISTS kept (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        return db

    def receive(self, body: bytes, headers: Mapping[str, str]) -> Answer:  # noqa: PLR0911 - each protocol refusal answers at once
        """One request from the connector, as (status, headers, body) to send back. The signature is checked before
        anything is looked up or run; an event seen before gets its stored answer; its function runs under a
        lease."""
        if len(body) > MAX_BODY:
            return 413, {}, b""
        found = {k.lower(): v for k, v in headers.items()}
        if REQUEST_HEADER.lower() not in found:
            return self._legacy(body, found.get(LEGACY_HEADER.lower(), ""))
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

        def signed(status: int, payload: bytes) -> Answer:
            now = int(time.time())
            answered = hashlib.sha256(payload).hexdigest()
            v = _mac(key, "ekl-cb-resp-v2", str(now), event_id, agent_id, digest, str(status), answered)
            header = f"t={now},id={event_id},d={digest},v={v}"
            return status, {"Content-Type": "application/json", RESPONSE_HEADER: header}, payload

        owner = secrets.token_hex(8)
        with contextlib.closing(self._store()) as db:
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
        return self._run_leased(event, event_id, fence, signed)

    def _run_leased(
        self, event: dict[str, Any], event_id: str, fence: int, signed: Callable[[int, bytes], Answer]
    ) -> Answer:
        """Run the function while renewing the lease; commit its answer and its remembered values only if this run
        still owns the event."""
        stop = threading.Event()

        def renew() -> None:
            while not stop.wait(RENEW_SECONDS):
                with contextlib.closing(self._store()) as db:
                    held = db.execute(
                        "UPDATE events SET lease_until = ? WHERE event_id = ? AND fence = ? AND state = 'in_progress'",
                        (time.time() + LEASE_SECONDS, event_id, fence),
                    ).rowcount
                if not held:
                    return  # taken over: the commit below finds its fence stale and writes nothing

        threading.Thread(target=renew, daemon=True).start()
        self._writes.pending = {}
        try:
            callback = self.callback_for(str(event.get("event")))
            answer = callback(event) if callback else None
        except Exception:
            stop.set()
            with contextlib.closing(self._store()) as db:  # let the connector's next retry run it again at once
                db.execute("UPDATE events SET lease_until = 0 WHERE event_id = ? AND fence = ?", (event_id, fence))
            raise
        finally:
            stop.set()
        payload = json.dumps(answer).encode() if answer else b""
        with contextlib.closing(self._store()) as db:
            db.execute("BEGIN IMMEDIATE")
            done = db.execute(
                "UPDATE events SET state = 'completed', status = 200, body = ? WHERE event_id = ? AND fence = ? "
                "AND state = 'in_progress'",
                (payload, event_id, fence),
            ).rowcount
            if done:
                for key, value in self._writes.pending.items():
                    db.execute(
                        "INSERT INTO kept (key, value) VALUES (?, ?) "
                        "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                        (key, json.dumps(value)),
                    )
            db.execute("COMMIT")
        if not done:
            return 409, {"Retry-After": "2"}, b'{"error": "IN_PROGRESS"}'
        return signed(200, payload)

    def _legacy(self, body: bytes, signature: str) -> Answer:
        """An older connector's v1 event, signed with the credential itself, until 2027-02-11."""
        credential = os.environ.get("EKL_AGENT_CREDENTIAL") or os.environ.get("EKL_PROVIDER_CREDENTIAL", "")
        expected = "sha256=" + hmac.new(credential.encode(), body, hashlib.sha256).hexdigest()
        if dt.datetime.now(dt.UTC) >= LEGACY_UNTIL or not credential or not hmac.compare_digest(expected, signature):
            return 401, {}, b""
        print("warning: an unsigned v1 event from an older connector; upgrade it (ekl deploy)")
        event = json.loads(body)
        callback = self.callback_for(str(event.get("event")))
        self._writes.pending = {}
        answer = callback(event) if callback else None
        return 200, {"Content-Type": "application/json"}, json.dumps(answer).encode() if answer else b""

    def server(self, host: str = "127.0.0.1", port: int = 8002) -> ThreadingHTTPServer:
        """The HTTP server the connector posts the agent's events to."""
        return ThreadingHTTPServer((host, port), _events_handler(self))

    def main(self) -> None:
        """Serve the agent's events on the loopback interface, as the connector runs the callbacks file."""
        parser = argparse.ArgumentParser(description="The agent's callbacks for the EuKollab connector")
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
        server = self.server(options.host, options.port)
        start = self.namespace.get("on_start")
        if callable(start):
            self.later(start)
        print(f"callbacks on http://{options.host}:{options.port}/")
        with contextlib.suppress(KeyboardInterrupt):  # Ctrl-C stops it; a traceback would say nothing more
            server.serve_forever()


def _events_handler(callbacks: Callbacks) -> type[BaseHTTPRequestHandler]:
    class Events(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            if length > MAX_BODY:
                self._send(413, {}, b"")
                return
            body = self.rfile.read(length)
            try:
                status, headers, payload = callbacks.receive(body, dict(self.headers.items()))
            except Exception:  # the connector sends the event again; the error goes to its log, where you'll look
                traceback.print_exc()
                status, headers, payload = 500, {}, b""
            self._send(status, headers, payload)

        def log_message(self, *args: Any) -> None:
            pass  # the connector logs each event it posts here; a line per request would repeat it

        def _send(self, status: int, headers: dict[str, str], payload: bytes) -> None:
            self.send_response(status)
            for name, value in {"Content-Type": "application/json", **headers}.items():
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    return Events
