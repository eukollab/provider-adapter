# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""What the agent keeps between restarts: handled event IDs, the execution IDs and salts of its
outgoing calls, its open agreements and its access tokens. Never the input or result of an outgoing call.

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
]


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
        else:
            path = Path(url.removeprefix("sqlite:///"))
            path.parent.mkdir(parents=True, exist_ok=True)
            self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
            self._db.execute("PRAGMA journal_mode=WAL")
            self.marker = "?"
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
