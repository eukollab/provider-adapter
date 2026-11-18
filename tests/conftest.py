# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
import os
from pathlib import Path
from typing import Any

import pytest

from ekl_provider_adapter.agent import Agent, Candidate, Result, _data_class

# The protocol's schemas and vectors: a checkout of eukollab/ekp named by EKP_DIR, else the contracts beside this
# package.
CONTRACTS = Path(os.environ.get("EKP_DIR") or Path(__file__).resolve().parents[2] / "contracts")
if not (CONTRACTS / "ekp").is_dir():
    raise pytest.UsageError(f"no protocol files at {CONTRACTS}: set EKP_DIR to a checkout of eukollab/ekp")


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


class _Use:
    """A call to another agent's service through the agent's side, as the local API's `call` makes it."""

    def __init__(self, agent: Agent, service: Candidate, criteria: Any = None, data_class: str = "public") -> None:
        self.agent, self.service, self.criteria, self.data_class = agent, service, criteria, _data_class(data_class)

    def call(self, arguments: dict[str, Any], criteria: Any = None, idempotency_key: str | None = None) -> Result:
        judge = criteria if criteria is not None else self.criteria
        return self.agent._sync(
            self.agent.aio.call(self.service, arguments, judge, idempotency_key, data_class=self.data_class)
        )

    def close(self, outcome: str = "completed") -> dict[str, Any]:
        return self.agent._sync(self.agent.aio.close_agreement(self.service, outcome))


def use(agent: Agent, service: Candidate, criteria: Any = None, data_class: str = "public") -> _Use:
    return _Use(agent, service, criteria, data_class)
