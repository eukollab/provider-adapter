# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Both storage backends behave the same: the journal, the atomic outbox and purges. PostgreSQL runs only when
EKL_ADAPTER_TEST_POSTGRES names a database the test may use."""

import os
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from ekl_provider_adapter.storage import ExecutionRecord, PostgresStorage, SQLiteStorage, Storage


@pytest.fixture(params=["sqlite", "postgres"])
def storage(request: pytest.FixtureRequest) -> Iterator[Storage]:
    if request.param == "sqlite":
        with tempfile.TemporaryDirectory() as directory:
            yield SQLiteStorage(str(Path(directory) / "journal.db"))
        return
    url = os.environ.get("EKL_ADAPTER_TEST_POSTGRES")
    if not url:
        pytest.skip("set EKL_ADAPTER_TEST_POSTGRES to test PostgreSQL")
    found = PostgresStorage(url)
    found._transaction([("DELETE FROM ekl_outbox", ()), ("DELETE FROM ekl_executions", ())])  # a clean slate
    yield found


def test_the_journal_lifecycle(storage: Storage) -> None:
    first = ExecutionRecord("att_1", "e1", "keep", "fp")
    storage.record_attempt(first)
    storage.record_attempt(ExecutionRecord("att_2", "e2", "keep", "fp2"))
    storage.forget("att_2")
    assert [r.attempt_id for r in storage.unfinished()] == ["att_1"]
    assert storage.mark_running("att_1", "adm_1", time.time() + 60, False, first.updated_at)
    storage.finish("att_1", "succeeded", "out", '{"content": []}', {"admission_id": "adm_1", "outcome": "succeeded"})
    assert [e.admission_id for e in storage.outbox()] == ["adm_1"] and not storage.unfinished()
    storage.note_delivery_attempt("adm_1")
    assert storage.outbox()[0].attempts == 1
    storage.acknowledge("adm_1", "rcp_1")
    record = storage.by_admission("adm_1")
    assert record is not None and record.state == "reported" and record.receipt_id == "rcp_1"
    assert record.result == {"content": []} and not storage.outbox()
    assert storage.purge(time.time() + 1, 0) == 0  # results dropped; the record is younger than the window
    assert storage.by_admission("adm_1").result is None  # type: ignore[union-attr]  # found above
    assert storage.purge(time.time() + 1, time.time() + 1) == 1


def test_finish_writes_the_outcome_and_outbox_together(storage: Storage) -> None:
    first, second = ExecutionRecord("att_1", "e1", "keep", "fp"), ExecutionRecord("att_2", "e2", "keep", "fp")
    storage.record_attempt(first)
    storage.mark_running("att_1", "adm_1", None, False, first.updated_at)
    storage.record_attempt(second)
    storage.mark_running("att_2", "adm_2", None, False, second.updated_at)
    storage.finish("att_1", "succeeded", "out", None, {"admission_id": "adm_1", "outcome": "succeeded"})
    with pytest.raises(Exception):  # noqa: B017 - each backend raises its own integrity error
        # The same outbox key again: the whole transaction fails, so the outcome is not written either.
        storage.finish("att_2", "succeeded", "out", None, {"admission_id": "adm_1", "outcome": "succeeded"})
    assert storage.by_admission("adm_2").outcome is None  # type: ignore[union-attr]  # recorded above


def test_only_one_claim_wins_and_a_live_worker_defeats_it(storage: Storage) -> None:
    record = ExecutionRecord("att_1", "e1", "keep", "fp")
    storage.record_attempt(record)
    storage.mark_running("att_1", "adm_1", time.time() + 60, False, record.updated_at)
    seen = storage.unfinished()[0].updated_at
    storage.renew_lease("adm_1", time.time() + 120)  # its worker is alive
    assert storage.claim("att_1", seen) is None
    fresh = storage.unfinished()[0].updated_at
    assert storage.claim("att_1", fresh) is not None
    assert storage.claim("att_1", fresh) is None  # another replica read the same entry: only one settles it


def test_a_sqlite_journal_belongs_to_one_process() -> None:
    with tempfile.TemporaryDirectory() as directory:
        path = str(Path(directory) / "journal.db")
        first = SQLiteStorage(path)
        assert first.exclusive
        with pytest.raises(RuntimeError, match="another adapter process"):
            SQLiteStorage(path)


def test_a_worker_recovery_took_over_never_marks_its_attempt_running(storage: Storage) -> None:
    """The fence: once recovery claims an attempted entry, its original worker, resuming, cannot start the tool."""
    record = ExecutionRecord("att_1", "e1", "keep", "fp")
    storage.record_attempt(record)
    assert storage.claim("att_1", storage.unfinished()[0].updated_at) is not None
    assert not storage.mark_running("att_1", "adm_1", None, False, record.updated_at)
    assert storage.unfinished()[0].state == "attempted"
