# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Recovering a journal entry written before `admit` whose response was lost: which pending
admission is adopted, and when an entry with no match is dropped."""

from collections.abc import Iterator

import httpx2
import pytest

from ekl_provider_adapter.api import (
    PENDING_LIMIT,
    CompleteRequest,
    Completion,
    HttpProviderApi,
    PendingAdmission,
    PendingAdmissions,
)
from ekl_provider_adapter.conformance.harness import CrashingApi, Harness
from ekl_provider_adapter.storage import ExecutionRecord

pytestmark = pytest.mark.anyio
FP = "a" * 64


class ListedApi(CrashingApi):
    """The network with a chosen `pending` answer, recording which admissions the adapter completes."""

    def __init__(self, harness: Harness, listed: PendingAdmissions) -> None:
        super().__init__(harness.network)
        self.listed = listed
        self.completed: list[str] = []

    async def pending(self) -> PendingAdmissions:
        return self.listed

    async def complete(self, request: CompleteRequest) -> Completion:
        self.completed.append(request.admission_id)
        return Completion(receipt={"receipt_id": "rcpt_" + request.admission_id})


@pytest.fixture
def harness() -> Iterator[Harness]:
    found = Harness("durable")
    yield found
    found.close()


def admission(
    admission_id: str, *, attempt_id: str | None, execution_id: str = "e1", capability_id: str | None = "cap_keep"
) -> PendingAdmission:
    return PendingAdmission(
        admission_id=admission_id,
        attempt_id=attempt_id,
        execution_id=execution_id,
        capability_id=capability_id,
        input_fingerprint=FP,
        state="admitted",
        lease_expires_at=None,
    )


async def recover(harness: Harness, *found: PendingAdmission, complete: bool = True) -> ListedApi:
    """A restart with one entry journaled before admit, as attempt `att_mine` of execution `e1`."""
    assert harness.storage is not None
    harness.storage.record_attempt(ExecutionRecord("att_mine", "e1", "keep", FP))
    api = ListedApi(harness, PendingAdmissions(pending=list(found), complete=complete))
    harness.api = api
    await harness.restart()
    await harness.core.drain()
    return api


async def test_the_attempt_id_names_the_admission_exactly(harness: Harness) -> None:
    # Another admission with the same execution ID and fingerprint is not taken for ours.
    api = await recover(
        harness, admission("adm_other", attempt_id="att_other"), admission("adm_mine", attempt_id="att_mine")
    )
    assert api.completed == ["adm_mine"]
    assert harness.storage is not None and not harness.storage.unfinished()


async def test_without_attempt_ids_the_execution_and_fingerprint_decide(harness: Harness) -> None:
    api = await recover(
        harness, admission("adm_mine", attempt_id=None), admission("adm_else", attempt_id=None, execution_id="e2")
    )
    assert api.completed == ["adm_mine"]


async def test_without_attempt_ids_a_network_that_omits_the_capability_gives_no_match(harness: Harness) -> None:
    # The execution ID alone never adopts an admission: the attempt is left to the network's lapse.
    api = await recover(harness, admission("adm_mine", attempt_id=None, capability_id=None))
    assert api.completed == []


async def test_a_single_fallback_match_in_a_cut_off_list_is_not_adopted(harness: Harness) -> None:
    # Without attempt IDs, one match is unique only in a whole list: another may lie beyond the cut.
    api = await recover(harness, admission("adm_mine", attempt_id=None), complete=False)
    assert api.completed == []
    assert harness.storage is not None and [r.attempt_id for r in harness.storage.unfinished()] == ["att_mine"]


async def test_two_matches_without_attempt_ids_adopt_neither(harness: Harness) -> None:
    api = await recover(harness, admission("adm_a", attempt_id=None), admission("adm_b", attempt_id=None))
    assert api.completed == []


async def test_an_unmatched_entry_is_kept_while_the_list_is_cut_off(harness: Harness) -> None:
    api = await recover(harness, admission("adm_other", attempt_id="att_other"), complete=False)
    assert api.completed == []
    assert harness.storage is not None and [r.attempt_id for r in harness.storage.unfinished()] == ["att_mine"]


async def test_an_unmatched_entry_is_dropped_when_the_list_is_complete(harness: Harness) -> None:
    api = await recover(harness, admission("adm_other", attempt_id="att_other"), complete=True)
    assert api.completed == []
    assert harness.storage is not None and not harness.storage.unfinished()


@pytest.mark.parametrize(("count", "complete"), [(1, True), (PENDING_LIMIT, False)])
async def test_an_older_network_that_omits_complete(count: int, complete: bool) -> None:
    # It cut its list at the same limit without saying: only a full list may have been cut off.
    item = {"admission_id": "adm", "execution_id": "e1", "state": "admitted"}
    transport = httpx2.MockTransport(lambda _: httpx2.Response(200, json={"pending": [item] * count}))
    async with httpx2.AsyncClient(transport=transport) as http:
        listed = await HttpProviderApi("https://network.test", "eklp_x", http=http).pending()
    assert len(listed.pending) == count and listed.complete is complete
