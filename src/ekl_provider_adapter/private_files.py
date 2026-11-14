# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Files that hold the agent's state: readable by their owner only, from the first byte.

Journals hold execution IDs, salts and access tokens; the local API file names a port any local process could call.
Each is created 0600 inside a folder created 0700, so a shared machine's other users never read them.
"""

import contextlib
import json
import os
import sqlite3
from pathlib import Path
from typing import Any


def private_folder(path: Path) -> None:
    """Create the folder 0700. A folder that already exists is left as it is, unless it is the project's own .ekl/,
    which only the agent's tools write."""
    created = not path.exists()
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if created or path.name == ".ekl":
        with contextlib.suppress(OSError):
            path.chmod(0o700)


def private_file(path: Path) -> None:
    """Create the file empty and 0600 if missing, and make an existing one 0600."""
    private_folder(path.parent)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT, 0o600)
    os.close(fd)
    with contextlib.suppress(OSError):
        path.chmod(0o600)


def write_private_json(path: Path, data: Any) -> None:
    """Write then rename, 0600 from the first byte, so a reader never sees a half-written or readable file."""
    private_folder(path.parent)
    tmp = path.with_name(path.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as out:
        json.dump(data, out)
    tmp.replace(path)


def private_sqlite(path: Path) -> sqlite3.Connection:
    """A SQLite database whose file and write-ahead log are owner-only. ":memory:" stays in memory, never a file."""
    if str(path) == ":memory:":
        return sqlite3.connect(":memory:", check_same_thread=False, isolation_level=None)
    private_file(path)
    db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
    db.execute("PRAGMA journal_mode=WAL")
    for suffix in ("-wal", "-shm"):
        side = path.with_name(path.name + suffix)
        if side.exists():
            with contextlib.suppress(OSError):
                side.chmod(0o600)
    return db
