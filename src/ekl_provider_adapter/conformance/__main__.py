# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""`python -m ekl_provider_adapter.conformance`: run every scenario in each execution mode it applies to
; the exit status is non-zero when any fails."""

import asyncio
import sys

from ekl_provider_adapter.conformance.harness import run_all

results = asyncio.run(run_all())
for name, verdict in results.items():
    print(f"{verdict:>8}  {name}" if verdict == "passed" else f"FAILED  {name}: {verdict}")
sys.exit(0 if all(v == "passed" for v in results.values()) else 1)
