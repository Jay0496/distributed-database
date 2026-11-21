"""Per-node durable SQLite store (scaffold: connection + schema)."""

import os
import sqlite3
import threading
from typing import Any


_SCHEMA = """
CREATE TABLE IF NOT EXISTS raft_state (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS raft_log (
    idx     INTEGER PRIMARY KEY,
    term    INTEGER,
    command BLOB
);

CREATE TABLE IF NOT EXISTS meta (
    key      TEXT PRIMARY KEY,
    mode     TEXT,
    version  INTEGER,
    size     INTEGER,
    checksum TEXT,
    owners   TEXT
);

CREATE TABLE IF NOT EXISTS shards (
    key     TEXT,
    idx     INTEGER,
    version INTEGER,
    blob    BLOB,
    PRIMARY KEY (key, idx)
);
"""


class Storage:
    def __init__(self, path: str):
        self.path = path
        parent = os.path.dirname(os.path.abspath(path))
        if parent and not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            path, check_same_thread=False, timeout=30.0
        )
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.commit()
            except sqlite3.Error:
                pass
            self._conn.close()
"""