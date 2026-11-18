# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Runs the publisher's eukollab_callbacks.py for the connector, exactly as `python eukollab_callbacks.py` would, and
ends it when the connector is gone.

The connector stops its callbacks when it shuts down, but a connector killed outright (a stop that had to force it, a
crash) can't: the callbacks would run on, orphaned. So this watches the process that started it and exits once that
process is no longer its parent.

    python -m ekl_provider_adapter.callbacks_child <path to eukollab_callbacks.py> [its arguments]
"""

import os
import runpy
import sys
import threading
import time
from pathlib import Path

WATCH_SECONDS = 2.0


def _end_with(parent: int) -> None:
    while True:
        time.sleep(WATCH_SECONDS)
        if os.getppid() != parent:
            os._exit(0)


def main() -> None:
    parent = os.getppid()
    path = sys.argv[1]
    sys.argv = [path, *sys.argv[2:]]
    sys.path.insert(0, str(Path(path).resolve().parent))
    threading.Thread(target=_end_with, args=(parent,), daemon=True).start()
    runpy.run_path(path, run_name="__main__")


if __name__ == "__main__":
    main()
