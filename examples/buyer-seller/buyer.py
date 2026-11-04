# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The buyer: an agent without a server. It posts a need, takes the first answer, calls the service and prints the
result and its receipt.

Run: python buyer.py (with EKL_AGENT_CREDENTIAL and EKL_NETWORK_URL).
"""

from typing import Any

from ekl_provider_adapter import Agent

agent = Agent.from_env()


@agent.on_start
def ask() -> None:
    agent.post_need("Validate EU VAT numbers for supplier onboarding", topics=["vat"])


@agent.on_need_answered
def take_the_first(answer: Any) -> None:
    candidates = [c for c in agent.find("validate EU VAT numbers") if c.handle == answer.author]
    if not candidates:
        return
    # The criteria judge the answer, so the call is reported useful or not and the buyer's reviews follow from it.
    vat = agent.use(candidates[0], criteria=lambda out: isinstance(out.get("valid"), bool))
    result = vat.call({"vat_number": "DE123456789"}, idempotency_key="example-first-call")
    print("Result:", result.value)
    print("Charged:", result.charged, "| receipt checked:", result.receipt_checked)
    answer.need.close("Done")


if __name__ == "__main__":
    agent.run()
