"""Per-node durable SQLite store."""

import json
import os
import sqlite3
import threading
from typing import Any, Dict, List, Optional


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

    # ---- raft state ----
    def get_raft_state(self, key: str) -> Optional[str]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT value FROM raft_state WHERE key = ?", (key,)
            )
            row = cur.fetchone()
            return row[0] if row else None

    def set_raft_state(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO raft_state (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
            self._conn.commit()

    # ---- raft log ----
    def append_log(self, term: int, command: bytes) -> int:
        with self._lock:
            cur = self._conn.execute(
                "SELECT COALESCE(MAX(idx), 0) FROM raft_log"
            )
            idx = int(cur.fetchone()[0]) + 1
            self._conn.execute(
                "INSERT INTO raft_log (idx, term, command) VALUES (?, ?, ?)",
                (idx, term, command),
            )
            self._conn.commit()
            return idx

    def get_log(self, idx: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT idx, term, command FROM raft_log WHERE idx = ?", (idx,)
            )
            row = cur.fetchone()
            if row is None:
                return None
            return {"idx": row[0], "term": row[1], "command": bytes(row[2]) if row[2] is not None else b""}

    def get_log_from(self, idx: int) -> List[Dict[str, Any]]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT idx, term, command FROM raft_log WHERE idx >= ? ORDER BY idx ASC",
                (idx,),
            )
            out = []
            for row in cur.fetchall():
                out.append(
                    {
                        "idx": row[0],
                        "term": row[1],
                        "command": bytes(row[2]) if row[2] is not None else b"",
                    }
                )
            return out

    def truncate_log(self, idx: int) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM raft_log WHERE idx >= ?", (idx,)
            )
            self._conn.commit()

    def last_log(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT idx, term, command FROM raft_log ORDER BY idx DESC LIMIT 1"
            )
            row = cur.fetchone()
            if row is None:
                return None
            return {"idx": row[0], "term": row[1], "command": bytes(row[2]) if row[2] is not None else b""}

    # ---- meta ----
    def upsert_meta(
        self,
        key: str,
        mode: str,
        version: int,
        size: int,
        checksum: str,
        owners: List[str],
    ) -> None:
        owners_json = json.dumps(owners)
        with self._lock:
            self._conn.execute(
                "INSERT INTO meta (key, mode, version, size, checksum, owners) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET "
                "mode = excluded.mode, version = excluded.version, "
                "size = excluded.size, checksum = excluded.checksum, "
                "owners = excluded.owners",
                (key, mode, version, size, checksum, owners_json),
            )
            self._conn.commit()

    def get_meta(self, key: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT key, mode, version, size, checksum, owners FROM meta WHERE key = ?",
                (key,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            owners = json.loads(row[5]) if row[5] is not None else []
            return {
                "key": row[0],
                "mode": row[1],
                "version": row[2],
                "size": row[3],
                "checksum": row[4],
                "owners": owners,
            }

    def list_meta_keys(self) -> List[str]:
        with self._lock:
            cur = self._conn.execute("SELECT key FROM meta ORDER BY key ASC")
            return [r[0] for r in cur.fetchall()]

    # ---- shards ----
    def store_shard(self, key: str, idx: int, version: int, blob: bytes) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO shards (key, idx, version, blob) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(key, idx) DO UPDATE SET "
                "version = excluded.version, blob = excluded.blob",
                (key, idx, version, blob),
            )
            self._conn.commit()

    def fetch_shard(self, key: str, idx: int) -> Optional[bytes]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT blob FROM shards WHERE key = ? AND idx = ?", (key, idx)
            )
            row = cur.fetchone()
            if row is None:
                return None
            return bytes(row[0]) if row[0] is not None else b""

    def has_shard(self, key: str, idx: int) -> bool:
        with self._lock:
            cur = self._conn.execute(
                "SELECT 1 FROM shards WHERE key = ? AND idx = ?", (key, idx)
            )
            return cur.fetchone() is not None

    def delete_shard(self, key: str, idx: int) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM shards WHERE key = ? AND idx = ?", (key, idx)
            )
            self._conn.commit()

    def list_for_node(self, node_index: int) -> List[str]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT DISTINCT key FROM shards WHERE idx = ? ORDER BY key ASC",
                (node_index,),
            )
            return [r[0] for r in cur.fetchall()]
