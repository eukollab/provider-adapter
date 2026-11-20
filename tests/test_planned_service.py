# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""A planned service keeps its provider: a restart resumes the recorded choice without
trials, a pack's end follows the console's rule and keeps the provider used before unless another is clearly better,
and the agent's code can compare again or name a provider while calls are left."""

from pathlib import Path
from typing import Any

import pytest

from ekl_provider_adapter.agent import Agent
from ekl_provider_adapter.agent_journal import AgentJournal
from ekl_provider_adapter.errors import AgentError
from ekl_provider_adapter.testing import FakeNetwork

NEED = "convert currency"
SAMPLE = {"amount": 5}
EXPECT = {"checks": [{"id": "converted", "path": "$.converted", "approx": 10, "tolerance": 0.01}]}


def _double(arguments: dict[str, Any]) -> dict[str, Any]:
    return {"converted": arguments["amount"] * 2}


def _network(**config: str) -> FakeNetwork:
    fake = FakeNetwork(handle="@buyer")
    fake.config["sourcing"].update(config)
    fake.provider(
        "@fx", "cap_convert", _double, price="0.60", summary="convert currency",
        plan={"plan_id": "pack_3", "type": "pack", "price": "0.60", "calls": 3},
    )  # fmt: skip
    return fake


def _agent(fake: FakeNetwork, tmp_path: Path, name: str = "journal.db") -> Agent:
    return Agent(fake, AgentJournal(str(tmp_path / name)))


def _rival(fake: FakeNetwork, price: str = "0.60") -> None:
    fake.provider(
        "@fx-two", "cap_convert", _double, price=price, summary="convert currency",
        plan={"plan_id": "pack_3", "type": "pack", "price": price, "calls": 3},
    )  # fmt: skip


def _decisions(fake: FakeNetwork) -> list[tuple[str, str]]:
    return [(str(c.get("reason")), str(c["primary"])) for c in fake.choices]


def test_a_restart_resumes_the_choice_and_a_packs_end_keeps_it_on_a_tie(tmp_path: Path) -> None:
    """A restart resumes the recorded choice; a pack's end keeps the provider on a tie."""
    fake = _network()
    first = _agent(fake, tmp_path).service(NEED, sample=SAMPLE, expect=EXPECT)
    first.call({"amount": 1})
    assert len(fake.trials) == 1 and _decisions(fake) == [("initial", "@fx")]

    # A restarted connector: a new agent and a new planned service, the same network and journal.
    _rival(fake)
    again = _agent(fake, tmp_path).service(NEED, sample=SAMPLE, expect=EXPECT)
    again.call({"amount": 2})
    again.call({"amount": 3})
    assert len(fake.trials) == 1  # resumed: no search-and-trial round
    assert again.calls_left == 0

    # The pack ran out: compare before buying. The rival is trialled; the provider used before is not, and is kept.
    again.call({"amount": 4})
    assert [t["provider"] for t in fake.trials] == ["@fx", "@fx-two"]
    assert _decisions(fake)[-1] == ("pack_end", "@fx")
    assert again.main is not None and again.main.handle == "@fx"
    assert again.calls_left == 2


def test_a_clearly_cheaper_provider_takes_over_at_a_packs_end_when_price_comes_first(tmp_path: Path) -> None:
    fake = _network(rank_by="price")
    planned = _agent(fake, tmp_path).service(NEED, sample=SAMPLE, expect=EXPECT)
    for amount in (1, 2, 3):
        planned.call({"amount": amount})
    _rival(fake, price="0.30")
    planned.call({"amount": 4})
    assert _decisions(fake)[-1] == ("pack_end", "@fx-two")
    assert planned.backup is not None and planned.backup.handle == "@fx"


def test_keep_buys_again_and_ask_waits_for_the_publisher(tmp_path: Path) -> None:
    fake = _network(on_pack_end="keep")
    planned = _agent(fake, tmp_path).service(NEED, sample=SAMPLE, expect=EXPECT)
    for amount in (1, 2, 3):
        planned.call({"amount": amount})
    _rival(fake)
    planned.call({"amount": 4})
    assert len(fake.trials) == 1 and _decisions(fake) == [("initial", "@fx")]

    fake.config["sourcing"]["on_pack_end"] = "ask"
    for amount in (5, 6):
        planned.call({"amount": amount})
    with pytest.raises(AgentError) as waiting:
        planned.call({"amount": 7})
    assert waiting.value.code == "CHOICE_WAITING"
    assert fake.choices[-1]["state"] == "awaiting_owner" and fake.choices[-1]["reason"] == "pack_end"


def test_the_agents_code_compares_again_or_names_a_provider_while_calls_are_left(tmp_path: Path) -> None:
    """The agent's code compares again, or names a provider, while calls are left."""
    fake = _network()
    planned = _agent(fake, tmp_path).service(NEED, sample=SAMPLE, expect=EXPECT)
    planned.call({"amount": 1})
    _rival(fake)
    planned.call({"amount": 2}, choose_again=True)
    assert _decisions(fake)[-1] == ("agent_request", "@fx")  # kept: the rival is no better
    assert [t["provider"] for t in fake.trials] == ["@fx", "@fx-two"]

    planned.call({"amount": 3}, provider="@fx-two/convert")
    assert _decisions(fake)[-1] == ("agent_request", "@fx-two")
    assert planned.backup is not None and planned.backup.handle == "@fx"
    assert fake.packs[("@fx", "cap_convert")] == 1  # its calls left stay in its pack
    with pytest.raises(AgentError) as unknown:
        planned.call({"amount": 4}, provider="@nobody")
    assert unknown.value.code == "NOT_FOUND"


def test_find_marks_providers_used_before_and_keeps_earlier_choices_in_view(tmp_path: Path) -> None:
    fake = _network()
    agent = _agent(fake, tmp_path)
    agent.service(NEED, sample=SAMPLE, expect=EXPECT).call({"amount": 1})
    del fake.providers["@fx/cap_convert"]  # no longer listed
    _rival(fake)
    found, earlier = agent._sync(agent.aio.find_with_history(NEED, 5, "convert-currency"))
    assert [c.handle for c in found] == ["@fx-two"] and found[0].used_before is None
    assert [(c.handle, c.still_listed, c.eligible) for c in earlier] == [("@fx", False, False)]
    assert earlier[0].used_before is not None and earlier[0].used_before["calls"] == 1
