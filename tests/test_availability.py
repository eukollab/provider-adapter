# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""availability.changed (EKP 1.2 §6.12): a revision that doesn't increase is ignored, and a gap is filled by reading
the profile again."""

from pathlib import Path
from typing import Any

from ekl_provider_adapter.agent import Agent
from ekl_provider_adapter.agent_journal import AgentJournal
from ekl_provider_adapter.testing import FakeNetwork

SUBJECT = {"kind": "service", "agent_id": "agt_x", "handle": "@invoice-x", "capability_id": "cap_extract"}


def _changed(revision: int, admits_paid: bool) -> dict[str, Any]:
    effective = {"listed": True, "admits_free": True, "admits_paid": admits_paid, "ranked_lower": False}
    return {"subject": SUBJECT, "revision": revision, "effective": effective, "until": None}


def test_a_late_event_is_ignored_and_a_gap_reads_the_profile_again(tmp_path: Path) -> None:
    fake = FakeNetwork()
    agent = Agent(fake, AgentJournal(str(tmp_path / "journal.db")))
    fake.event("availability.changed", _changed(1, True))
    fake.run(agent)
    assert agent.availability("@invoice-x", "cap_extract") == _changed(1, True)["effective"]
    # Revision 3 arrives, 2 was missed: the profile is read again, and its availability is what applies.
    fake.availability["@invoice-x"] = [_changed(3, False)]
    fake.event("availability.changed", _changed(3, False))
    fake.run(agent)
    assert agent.availability("@invoice-x", "cap_extract") == _changed(3, False)["effective"]
    # The missing revision 2 is delivered late, after 3: older than what was applied, so it changes nothing.
    fake.event("availability.changed", _changed(2, True))
    fake.run(agent)
    assert agent.availability("@invoice-x", "cap_extract")["admits_paid"] is False  # type: ignore[index]
