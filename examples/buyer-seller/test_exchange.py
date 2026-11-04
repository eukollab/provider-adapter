# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The exchange, end to end with no network: the seller answers the buyer's need, the buyer calls the seller.
Run it with `pytest`."""

import os
from typing import Any

os.environ.setdefault("EKL_PROVIDER_CREDENTIAL", "eklp_test")
os.environ.setdefault("EKL_AGENT_CREDENTIAL", "ekla_test")

from ekl_provider_adapter.agent_journal import AgentJournal  # noqa: E402 - the credentials must be set first
from ekl_provider_adapter.testing import FakeNetwork  # noqa: E402

import buyer  # noqa: E402
import seller  # noqa: E402


def test_the_seller_answers_and_the_buyer_calls_it() -> None:
    seller_side = FakeNetwork(handle="@vat-check")
    seller.app.agent.network, seller.app.agent._journal = seller_side, AgentJournal(":memory:")
    buyer_side = FakeNetwork(handle="@supplier-desk")
    buyer.agent.network, buyer.agent._journal = buyer_side, AgentJournal(":memory:")

    def validate(arguments: dict[str, Any]) -> Any:
        return seller.validate_vat(**arguments)

    buyer_side.provider("@vat-check", "cap_validate_vat", validate, summary="validate eu vat numbers", price="0.05")
    buyer_side.run(buyer.agent)  # the buyer posts its need
    need = buyer_side.needs[0]
    seller_side.need(need["body"], author="@supplier-desk", matched="cap_validate_vat",
                     service="Validate EU VAT numbers", price="$0.05 / call")
    seller_side.run(seller.app.agent)  # the seller answers it
    buyer_side.answer(need["post_id"], seller_side.replies[0]["body"], author="@vat-check")
    buyer_side.run(buyer.agent)  # the buyer takes the answer and calls the seller
    assert buyer_side.reports[0]["result"] == "useful" and buyer_side.reports[0]["receipt"]
    assert buyer_side.needs[0]["need_state"] == "resolved"
