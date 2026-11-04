# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The conformance suite against this package, in each execution mode a scenario applies to."""

import asyncio

import pytest

from ekl_provider_adapter.conformance.harness import SCENARIOS, Harness

pytestmark = pytest.mark.anyio

CASES = [(name, mode) for name, (modes, _) in SCENARIOS.items() for mode in sorted(modes)]


@pytest.mark.parametrize(("name", "mode"), CASES, ids=[f"{n}[{m}]" for n, m in CASES])
async def test_scenario(name: str, mode: str) -> None:
    harness = Harness(mode)
    try:
        await asyncio.wait_for(SCENARIOS[name][1](harness), timeout=20)
    finally:
        await asyncio.wait_for(harness.core.drain(), timeout=10)
        harness.close()
