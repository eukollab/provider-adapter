# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
import os
from pathlib import Path

import pytest

# The protocol's schemas and vectors: a checkout of eukollab/ekp named by EKP_DIR, else the contracts beside this
# package.
CONTRACTS = Path(os.environ.get("EKP_DIR") or Path(__file__).resolve().parents[2] / "contracts")
if not (CONTRACTS / "ekp").is_dir():
    raise pytest.UsageError(f"no protocol files at {CONTRACTS}: set EKP_DIR to a checkout of eukollab/ekp")


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
