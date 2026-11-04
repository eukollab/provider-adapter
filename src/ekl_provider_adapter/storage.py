# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The durable-mode journal: execution records and the completion outbox.

By default a record holds metadata only (attempt and admission identifiers, lease, dry-run status, fingerprints,
outcome, receipt reference). Results are stored only for `return_kept` capabilities and arguments only where
`data.inputs.journal` is declared, so the adapter's behaviour matches the agent's data declaration.

`finish` writes the outcome and the outbox entry in one transaction: once it returns, a crash can no longer lose
the completion. Methods are synchronous; the core runs them in a worker thread.
"""

import json
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

SCHEMA = [
    """CREATE TABLE IF NOT EXISTS ekl_executions (
        attempt_id TEXT PRIMARY KEY,
        execution_id TEXT NOT NULL,
        tool TEXT NOT NULL,
        input_fingerprint TEXT NOT NULL,
        admission_id TEXT UNIQUE,
        state TEXT NOT NULL,
        lease_expires_at DOUBLE PRECISION,
        dry_run BOOLEAN NOT NULL DEFAULT FALSE,
        outcome TEXT,
        output_fingerprint TEXT,
        result_json TEXT,
        arguments_json TEXT,
        receipt_id TEXT,
        created_at DOUBLE PRECISION NOT NULL,
        updated_at DOUBLE PRECISION NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS ekl_executions_state ON ekl_executions (state)",
    """CREATE TABLE IF NOT EXISTS ekl_outbox (
        admission_id TEXT PRIMARY KEY,
        payload TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        created_at DOUBLE PRECISION NOT NULL
    )""",
]

# attempted: journaled before admit; running: admitted; finished: outcome persisted, completion queued;
# reported: the network acknowledged the completion.
STATES = ("attempted", "running", "finished", "reported")


@dataclass
class ExecutionRecord:
    """One attempt to run a call, keyed by the adapter's attempt ID and written before `admit` is sent, so a
    restart knows which admissions may be its own."""

    attempt_id: str
    execution_id: str
    tool: str
    input_fingerprint: str
    state: str = "attempted"
    admission_id: str | None = None
    lease_expires_at: float | None = None
    dry_run: bool = False
    outcome: str | None = None
    output_fingerprint: str | None = None
    result_json: str | None = None
    arguments_json: str | None = None
    receipt_id: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0

    @property
    def result(self) -> dict[str, Any] | None:
        return json.loads(self.result_json) if self.result_json else None


@dataclass
class OutboxEntry:
    """A completion waiting to be delivered: resent until the network acknowledges it, which is safe because
    `complete` is idempotent by admission ID."""

    admission_id: str
    payload: dict[str, Any]
    attempts: int
    created_at: float


class Storage(Protocol):
    """A transactional store shared by every replica of the adapter."""

    # True when only this process can use the store, so that at start every unfinished entry belongs to a process
    # that has ended; False when several replicas share it.
    exclusive: bool

    def record_attempt(self, record: ExecutionRecord) -> None: ...

    def mark_running(
        self, attempt_id: str, admission_id: str, lease_expires_at: float | None, dry_run: bool, journaled_at: float
    ) -> bool: ...

    def renew_lease(self, admission_id: str, lease_expires_at: float) -> None: ...

    def forget(self, attempt_id: str) -> None: ...

    def finish(
        self,
        attempt_id: str,
        outcome: str,
        output_fingerprint: str | None,
        result_json: str | None,
        completion: dict[str, Any],
    ) -> None: ...

    def acknowledge(self, admission_id: str, receipt_id: str | None) -> None: ...

    def note_delivery_attempt(self, admission_id: str) -> None: ...

    def outbox(self) -> list[OutboxEntry]: ...

    def unfinished(self) -> list[ExecutionRecord]: ...

    def claim(self, attempt_id: str, seen_updated_at: float) -> float | None: ...

    def by_admission(self, admission_id: str) -> ExecutionRecord | None: ...

    def purge(self, results_before: float, records_before: float) -> int: ...


_COLUMNS = (
    "attempt_id, execution_id, tool, input_fingerprint, state, admission_id, lease_expires_at, dry_run, outcome, "
    "output_fingerprint, result_json, arguments_json, receipt_id, created_at, updated_at"
)


def _record(row: tuple[Any, ...]) -> ExecutionRecord:
    return ExecutionRecord(
        attempt_id=row[0],
        execution_id=row[1],
        tool=row[2],
        input_fingerprint=row[3],
        state=row[4],
        admission_id=row[5],
        lease_expires_at=row[6],
        dry_run=bool(row[7]),
        outcome=row[8],
        output_fingerprint=row[9],
        result_json=row[10],
        arguments_json=row[11],
        receipt_id=row[12],
        created_at=row[13],
        updated_at=row[14],
    )


class _SqlStorage:
    """The SQL both backends share; subclasses supply connections and the parameter style."""

    marker = "?"
    exclusive = False

    def _execute(self, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        raise NotImplementedError

    def _transaction(self, statements: list[tuple[str, tuple[Any, ...]]]) -> None:
        raise NotImplementedError

    def _sql(self, sql: str) -> str:
        return sql.replace("?", self.marker)

    def record_attempt(self, record: ExecutionRecord) -> None:
        now = time.time()
        # The worker's fence: mark_running succeeds only while the entry still carries this write (a recovery claim
        # rewrites it).
        record.updated_at = now
        self._transaction(
            [
                (
                    f"INSERT INTO ekl_executions ({_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        record.attempt_id,
                        record.execution_id,
                        record.tool,
                        record.input_fingerprint,
                        "attempted",
                        None,
                        None,
                        False,
                        None,
                        None,
                        None,
                        record.arguments_json,
                        None,
                        now,
                        now,
                    ),
                )
            ]
        )

    def mark_running(
        self, attempt_id: str, admission_id: str, lease_expires_at: float | None, dry_run: bool, journaled_at: float
    ) -> bool:
        """Journal the admission before the tool runs, fenced: False when recovery claimed the entry since this worker
        journaled it, so a paused or partitioned worker never invokes a tool that recovery has settled."""
        rows = self._execute(
            "UPDATE ekl_executions SET state = 'running', admission_id = ?, lease_expires_at = ?, dry_run = ?, "
            "updated_at = ? WHERE attempt_id = ? AND state = 'attempted' AND updated_at = ? RETURNING attempt_id",
            (admission_id, lease_expires_at, dry_run, time.time(), attempt_id, journaled_at),
        )
        return bool(rows)

    def forget(self, attempt_id: str) -> None:
        """An attempt the network refused or answered as a duplicate: nothing ran under it."""
        self._transaction([("DELETE FROM ekl_executions WHERE attempt_id = ? AND admission_id IS NULL", (attempt_id,))])

    def renew_lease(self, admission_id: str, lease_expires_at: float) -> None:
        self._transaction(
            [
                (
                    "UPDATE ekl_executions SET lease_expires_at = ?, updated_at = ? WHERE admission_id = ?",
                    (lease_expires_at, time.time(), admission_id),
                )
            ]
        )

    def finish(
        self,
        attempt_id: str,
        outcome: str,
        output_fingerprint: str | None,
        result_json: str | None,
        completion: dict[str, Any],
    ) -> None:
        now = time.time()
        self._transaction(
            [
                (
                    "UPDATE ekl_executions SET state = 'finished', outcome = ?, output_fingerprint = ?, "
                    "result_json = ?, updated_at = ? WHERE attempt_id = ?",
                    (outcome, output_fingerprint, result_json, now, attempt_id),
                ),
                (
                    "INSERT INTO ekl_outbox (admission_id, payload, attempts, created_at) VALUES (?, ?, 0, ?)",
                    (completion["admission_id"], json.dumps(completion), now),
                ),
            ]
        )

    def acknowledge(self, admission_id: str, receipt_id: str | None) -> None:
        self._transaction(
            [
                (
                    "UPDATE ekl_executions SET state = 'reported', receipt_id = ?, updated_at = ? "
                    "WHERE admission_id = ?",
                    (receipt_id, time.time(), admission_id),
                ),
                ("DELETE FROM ekl_outbox WHERE admission_id = ?", (admission_id,)),
            ]
        )

    def note_delivery_attempt(self, admission_id: str) -> None:
        self._transaction([("UPDATE ekl_outbox SET attempts = attempts + 1 WHERE admission_id = ?", (admission_id,))])

    def outbox(self) -> list[OutboxEntry]:
        rows = self._execute("SELECT admission_id, payload, attempts, created_at FROM ekl_outbox ORDER BY created_at")
        return [OutboxEntry(r[0], json.loads(r[1]), int(r[2]), float(r[3])) for r in rows]

    def unfinished(self) -> list[ExecutionRecord]:
        rows = self._execute(
            f"SELECT {_COLUMNS} FROM ekl_executions WHERE state IN ('attempted', 'running') ORDER BY created_at"
        )
        return [_record(r) for r in rows]

    def claim(self, attempt_id: str, seen_updated_at: float) -> float | None:
        """Take over an unfinished entry to settle it. Only one process wins, and only if nothing wrote to the entry
        since it was read: a worker renewing its lease, or finishing, makes the claim fail. Returns the claim's write,
        the fence the claimant's own mark_running uses, or None."""
        stamp = time.time()
        rows = self._execute(
            "UPDATE ekl_executions SET updated_at = ? WHERE attempt_id = ? AND updated_at = ? "
            "AND state IN ('attempted', 'running') RETURNING attempt_id",
            (stamp, attempt_id, seen_updated_at),
        )
        return stamp if rows else None

    def by_admission(self, admission_id: str) -> ExecutionRecord | None:
        rows = self._execute(f"SELECT {_COLUMNS} FROM ekl_executions WHERE admission_id = ?", (admission_id,))
        return _record(rows[0]) if rows else None

    def purge(self, results_before: float, records_before: float) -> int:
        """Drop kept results past result retention, and reported records past the execution-ID window. Duplicate
        detection does not depend on these: the network keeps its own tombstones."""
        before = self._execute("SELECT COUNT(*) FROM ekl_executions")[0][0]
        self._transaction(
            [
                (
                    "UPDATE ekl_executions SET result_json = NULL WHERE result_json IS NOT NULL AND created_at < ?",
                    (results_before,),
                ),
                (
                    "DELETE FROM ekl_executions WHERE state = 'reported' AND created_at < ?",
                    (records_before,),
                ),
            ]
        )
        after = self._execute("SELECT COUNT(*) FROM ekl_executions")[0][0]
        return int(before) - int(after)


class SQLiteStorage(_SqlStorage):
    """For a single instance. The file survives restarts; use PostgreSQL when several replicas run. A lock file keeps a
    second process from opening the same journal, so the store is exclusive to this process."""

    exclusive = True

    def __init__(self, path: str) -> None:
        self._holder = Path(path + ".lock").open("a")  # noqa: SIM115 - held open for the life of the store
        if sys.platform != "win32":
            import fcntl  # noqa: PLC0415 - POSIX only

            try:
                fcntl.flock(self._holder, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                self._holder.close()
                raise RuntimeError(
                    "another adapter process uses this journal; several replicas need PostgreSQL storage"
                ) from None
        self._lock = threading.Lock()
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._db.execute("PRAGMA journal_mode=WAL")
        for statement in SCHEMA:
            self._db.execute(statement)

    def _execute(self, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        with self._lock:
            return list(self._db.execute(sql, params).fetchall())

    def _transaction(self, statements: list[tuple[str, tuple[Any, ...]]]) -> None:
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                for sql, params in statements:
                    self._db.execute(sql, params)
            except Exception:
                self._db.execute("ROLLBACK")
                raise
            self._db.execute("COMMIT")


class PostgresStorage(_SqlStorage):
    """Shared by every replica. Needs the `postgres` extra (psycopg)."""

    marker = "%s"

    def __init__(self, url: str) -> None:
        import psycopg  # noqa: PLC0415 - optional dependency, needed only for this backend

        self._psycopg = psycopg
        self._url = url
        with psycopg.connect(url, autocommit=True) as db:
            for statement in SCHEMA:
                db.execute(statement)

    def _execute(self, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        with self._psycopg.connect(self._url, autocommit=True) as db:
            cursor = db.execute(self._sql(sql), params)
            return list(cursor.fetchall()) if cursor.description else []

    def _transaction(self, statements: list[tuple[str, tuple[Any, ...]]]) -> None:
        with self._psycopg.connect(self._url) as db, db.transaction():
            for sql, params in statements:
                db.execute(self._sql(sql), params)


def open_storage(url: str) -> Storage:
    """`sqlite:///path/to/file.db` or `postgresql://…`."""
    if url.startswith("sqlite:///"):
        return SQLiteStorage(url.removeprefix("sqlite:///"))
    if url.startswith(("postgresql://", "postgres://")):
        return PostgresStorage(url)
    raise ValueError("storage must be sqlite:///<path> or postgresql://…")
