# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""What the agent keeps between restarts: handled event IDs, the execution IDs and salts of its
outgoing calls, its open agreements and its access tokens, and the decisions its callbacks owe. Never the input or
result of an outgoing call.

A decision (a connection request, a paid offer, a reply) is kept from its first delivery to the publisher's
app until it is applied or its deadline passes. Applying it and defaulting it are each one conditional update with
opposite time conditions, read from the database's own clock inside that update, so only one of them can ever
succeed, whatever order a late answer and the deadline worker arrive in.

A server running in durable mode with Postgres shares the journal with its other copies; otherwise it is a small local
SQLite file (`EKL_JOURNAL`, default ./.ekl/journal.db).
"""

import datetime as dt
import json
import os
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ekl_provider_adapter.private_files import private_sqlite

DEFAULT_PATH = "./.ekl/journal.db"
# Handled event IDs are kept a week: long enough for any redelivery, short enough to stay small.
HANDLED_KEPT = dt.timedelta(days=7)

SCHEMA = [
    """CREATE TABLE IF NOT EXISTS ekl_agent_handled (
        event_id TEXT PRIMARY KEY,
        handled_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS ekl_agent_calls (
        idempotency_key TEXT PRIMARY KEY,
        execution_id TEXT NOT NULL,
        salt TEXT NOT NULL,
        provider TEXT NOT NULL,
        capability_id TEXT NOT NULL,
        state TEXT NOT NULL,
        outcome TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS ekl_agent_agreements (
        provider TEXT NOT NULL,
        capability_id TEXT NOT NULL,
        connection_id TEXT NOT NULL,
        agreement TEXT NOT NULL,
        PRIMARY KEY (provider, capability_id))""",
    """CREATE TABLE IF NOT EXISTS ekl_agent_access (
        connection_id TEXT PRIMARY KEY,
        access TEXT NOT NULL,
        expires_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS ekl_agent_decisions (
        event_id TEXT PRIMARY KEY,
        event TEXT NOT NULL,
        body TEXT NOT NULL,
        request_digest TEXT NOT NULL,
        deadline DOUBLE PRECISION NOT NULL,
        state TEXT NOT NULL,
        answer TEXT NOT NULL DEFAULT '',
        done INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS ekl_agent_flags (
        name TEXT PRIMARY KEY,
        value TEXT NOT NULL)""",
]
# Decisions applied or defaulted are kept as long as handled events: a redelivery finds them.
DECISIONS_KEPT = HANDLED_KEPT
# The database's clock, in Unix seconds, so the time a condition compares with is read inside the update itself.
NOW_SQL = {"sqlite": "((julianday('now') - 2440587.5) * 86400.0)", "postgres": "extract(epoch from clock_timestamp())"}


@dataclass
class OutgoingCall:
    """A call to another agent's service, journaled before its first attempt, so a repeat with the same idempotency
    key after a crash reuses the execution ID and salt instead of running or paying twice."""

    idempotency_key: str
    execution_id: str
    salt: str
    provider: str
    capability_id: str
    state: str  # started, finished
    outcome: dict[str, Any]


@dataclass
class PendingDecision:
    """One decision the agent's callbacks owe: the exact request body sent (every retry sends the same bytes), its
    digest, its deadline (Unix seconds) and its state: pending, applied or defaulted. `done` says whether what was
    decided has reached the network."""

    event_id: str
    event: str
    body: str
    request_digest: str
    deadline: float
    state: str
    answer: dict[str, Any]
    done: bool


def _now() -> str:
    return dt.datetime.now(dt.UTC).isoformat()


class AgentJournal:
    """One store, two engines: SQLite (one copy) or Postgres (shared by every copy)."""

    def __init__(self, url: str | None = None) -> None:
        url = url or os.environ.get("EKL_JOURNAL") or DEFAULT_PATH
        self._lock = threading.Lock()
        if url.startswith(("postgresql://", "postgres://")):
            import psycopg  # noqa: PLC0415 - optional dependency, needed only for this backend

            self._psycopg: Any = psycopg
            self._url = url
            self._db: sqlite3.Connection | None = None
            self.marker = "%s"
            self.now_sql = NOW_SQL["postgres"]
        else:
            path = Path(url.removeprefix("sqlite:///"))
            self._db = private_sqlite(path)
            self.marker = "?"
            self.now_sql = NOW_SQL["sqlite"]
        for statement in SCHEMA:
            self._run(statement)

    def _run(self, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        sql = sql.replace("?", self.marker)
        if self._db is None:
            with self._psycopg.connect(self._url, autocommit=True) as db:
                cursor = db.execute(sql, params)
                return list(cursor.fetchall()) if cursor.description else []
        with self._lock:
            cursor = self._db.execute(sql, params)
            return list(cursor.fetchall())

    # --- events ---------------------------------------------------------------------------------------------------

    def handled(self, event_id: str) -> bool:
        return bool(self._run("SELECT 1 FROM ekl_agent_handled WHERE event_id = ?", (event_id,)))

    def mark_handled(self, event_id: str) -> None:
        self._run(
            "INSERT INTO ekl_agent_handled (event_id, handled_at) VALUES (?, ?) ON CONFLICT DO NOTHING",
            (event_id, _now()),
        )
        cutoff = (dt.datetime.now(dt.UTC) - HANDLED_KEPT).isoformat()
        self._run("DELETE FROM ekl_agent_handled WHERE handled_at < ?", (cutoff,))

    # --- outgoing calls -------------------------------------------------------------------------------------------

    def call(self, idempotency_key: str) -> OutgoingCall | None:
        rows = self._run(
            "SELECT idempotency_key, execution_id, salt, provider, capability_id, state, outcome "
            "FROM ekl_agent_calls WHERE idempotency_key = ?",
            (idempotency_key,),
        )
        if not rows:
            return None
        key, execution_id, salt, provider, capability_id, state, outcome = rows[0]
        return OutgoingCall(key, execution_id, salt, provider, capability_id, state, json.loads(outcome or "{}"))

    def start_call(self, call: OutgoingCall) -> None:
        """Written before the first attempt, so a crash never loses the execution ID or its salt."""
        self._run(
            "INSERT INTO ekl_agent_calls (idempotency_key, execution_id, salt, provider, capability_id, state, "
            "created_at) VALUES (?, ?, ?, ?, ?, 'started', ?) ON CONFLICT DO NOTHING",
            (call.idempotency_key, call.execution_id, call.salt, call.provider, call.capability_id, _now()),
        )

    def replace_execution(self, idempotency_key: str, execution_id: str) -> None:
        """A paid call whose grant lapsed unredeemed goes on as a new execution: nothing ran under the old one."""
        self._run(
            "UPDATE ekl_agent_calls SET execution_id = ? WHERE idempotency_key = ?", (execution_id, idempotency_key)
        )

    def finish_call(self, idempotency_key: str, outcome: dict[str, Any]) -> None:
        """The outcome only: whether it ran, what was charged, whether the receipt checked. Never the result."""
        self._run(
            "UPDATE ekl_agent_calls SET state = 'finished', outcome = ? WHERE idempotency_key = ?",
            (json.dumps(outcome, sort_keys=True), idempotency_key),
        )

    # --- agreements and access ------------------------------------------------------------------------------------

    def agreement(self, provider: str, capability_id: str) -> tuple[str, dict[str, Any]] | None:
        rows = self._run(
            "SELECT connection_id, agreement FROM ekl_agent_agreements WHERE provider = ? AND capability_id = ?",
            (provider, capability_id),
        )
        return (rows[0][0], json.loads(rows[0][1])) if rows else None

    def save_agreement(self, provider: str, capability_id: str, connection_id: str, agreement: dict[str, Any]) -> None:
        self._run(
            "INSERT INTO ekl_agent_agreements (provider, capability_id, connection_id, agreement) VALUES (?, ?, ?, ?) "
            "ON CONFLICT (provider, capability_id) DO UPDATE SET connection_id = excluded.connection_id, "
            "agreement = excluded.agreement",
            (provider, capability_id, connection_id, json.dumps(agreement, sort_keys=True)),
        )

    def forget_agreement(self, provider: str, capability_id: str) -> None:
        self._run(
            "DELETE FROM ekl_agent_agreements WHERE provider = ? AND capability_id = ?", (provider, capability_id)
        )

    def access(self, connection_id: str) -> dict[str, Any] | None:
        rows = self._run("SELECT access, expires_at FROM ekl_agent_access WHERE connection_id = ?", (connection_id,))
        if not rows:
            return None
        found: dict[str, Any] = json.loads(rows[0][0])
        return found

    def save_access(self, connection_id: str, access: dict[str, Any]) -> None:
        self._run(
            "INSERT INTO ekl_agent_access (connection_id, access, expires_at) VALUES (?, ?, ?) "
            "ON CONFLICT (connection_id) DO UPDATE SET access = excluded.access, expires_at = excluded.expires_at",
            (connection_id, json.dumps(access, sort_keys=True), str(access.get("expires_at", ""))),
        )

    # --- decisions owed by the agent's callbacks ------------------------------------------------------------------

    def _decision(self, event_id: str) -> PendingDecision | None:
        rows = self._run(
            "SELECT event_id, event, body, request_digest, deadline, state, answer, done FROM ekl_agent_decisions "
            "WHERE event_id = ?",
            (event_id,),
        )
        return _pending(rows[0]) if rows else None

    def open_decision(
        self, event_id: str, event: str, body: str, request_digest: str, deadline: float
    ) -> PendingDecision:
        """The decision for this event, journaled as pending on first sight. A redelivered event finds the row it left,
        with the body first sent, so a retry never sends different bytes under the same event ID."""
        self._run(
            "INSERT INTO ekl_agent_decisions (event_id, event, body, request_digest, deadline, state, created_at) "
            "VALUES (?, ?, ?, ?, ?, 'pending', ?) ON CONFLICT DO NOTHING",
            (event_id, event, body, request_digest, deadline, _now()),
        )
        found = self._decision(event_id)
        assert found is not None
        return found

    def decision(self, event_id: str) -> PendingDecision | None:
        return self._decision(event_id)

    def apply_decision(self, event_id: str, request_digest: str, answer: dict[str, Any]) -> bool:
        """pending → applied, only for an answer to this exact request and only before the deadline, as the
        database's clock reads it now. False: another answer was applied first, the decision was defaulted, or the
        deadline has passed even though the deadline worker hasn't run yet."""
        return bool(
            self._run(
                # nosec B608 below: the only interpolation is NOW_SQL, a constant per engine; values are parameters.
                "UPDATE ekl_agent_decisions SET state = 'applied', answer = ? "  # nosec B608
                "WHERE event_id = ? AND state = 'pending' "
                f"AND request_digest = ? AND {self.now_sql} < deadline RETURNING event_id",
                (json.dumps(answer, sort_keys=True), event_id, request_digest),
            )
        )

    def default_due(self) -> list[PendingDecision]:
        """pending → defaulted for every decision whose deadline has passed: the opposite time condition to
        apply_decision's, so a decision is applied or defaulted, never both."""
        rows = self._run(
            "UPDATE ekl_agent_decisions SET state = 'defaulted' "  # nosec B608 - NOW_SQL is a constant per engine
            f"WHERE state = 'pending' AND {self.now_sql} >= deadline "
            "RETURNING event_id, event, body, request_digest, deadline, state, answer, done"
        )
        return [_pending(r) for r in rows]

    def mark_done(self, event_id: str) -> None:
        self._run("UPDATE ekl_agent_decisions SET done = 1 WHERE event_id = ?", (event_id,))
        cutoff = (dt.datetime.now(dt.UTC) - DECISIONS_KEPT).isoformat()
        self._run("DELETE FROM ekl_agent_decisions WHERE state <> 'pending' AND done = 1 AND created_at < ?", (cutoff,))

    def unfinished(self) -> list[PendingDecision]:
        """Decisions a restart must pick up: still pending, or decided but not yet carried to the network."""
        rows = self._run(
            "SELECT event_id, event, body, request_digest, deadline, state, answer, done FROM ekl_agent_decisions "
            "WHERE state = 'pending' OR done = 0"
        )
        return [_pending(r) for r in rows]

    # --- flags ----------------------------------------------------------------------------------------------------

    def flag(self, name: str) -> str | None:
        rows = self._run("SELECT value FROM ekl_agent_flags WHERE name = ?", (name,))
        return str(rows[0][0]) if rows else None

    def set_flag(self, name: str, value: str) -> None:
        self._run(
            "INSERT INTO ekl_agent_flags (name, value) VALUES (?, ?) "
            "ON CONFLICT (name) DO UPDATE SET value = excluded.value",
            (name, value),
        )


def _pending(row: tuple[Any, ...]) -> PendingDecision:
    event_id, event, body, digest, deadline, state, answer, done = row
    return PendingDecision(
        str(event_id),
        str(event),
        str(body),
        str(digest),
        float(deadline),
        str(state),
        json.loads(answer) if answer else {},
        bool(done),
    )
