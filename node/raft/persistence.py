"""Raft persistence and finite-state-machine application."""

import contextlib
import json
from dataclasses import dataclass
from typing import Any


@dataclass
class RaftLogEntry:
    term: int
    index: int
    command: bytes = b""


class RaftPersistence:
    """Persistence, log storage and FSM application for RaftNode."""

    def load_from_storage(self) -> None:
        if self.storage is None:
            return
        with self._lock:
            try:
                self.current_term = int(self.storage.get_raft_state("current_term") or 0)
            except Exception:
                self.current_term = 0
            try:
                vf = self.storage.get_raft_state("voted_for")
                self.voted_for = vf if vf not in (None, "") else None
            except Exception:
                self.voted_for = None
            try:
                self.commit_index = int(self.storage.get_raft_state("commit_index") or 0)
            except Exception:
                self.commit_index = 0
            try:
                rows = self.storage.get_log_from(1)
                rows = sorted(rows, key=lambda r: r["idx"])
                new_log = [RaftLogEntry(term=0, index=0, command=b"")]
                for r in rows:
                    cmd = r.get("command", b"")
                    if isinstance(cmd, str):
                        cmd = cmd.encode()
                    elif not isinstance(cmd, (bytes, bytearray)):
                        cmd = bytes(cmd) if cmd is not None else b""
                    new_log.append(
                        RaftLogEntry(
                            term=int(r.get("term", 0)),
                            index=int(r.get("idx", r.get("index", 0))),
                            command=bytes(cmd),
                        )
                    )
                self.log = new_log
            except Exception:
                pass
            self.last_applied = 0
            if self.apply_fn is None and self.storage is not None:
                self.apply_fn = self._apply_entry
            self._replay_committed_locked()

    def _replay_committed_locked(self) -> None:
        target = self.commit_index
        if target > self.last_log_index():
            target = self.last_log_index()
        while self.last_applied < target:
            self.last_applied += 1
            if self.last_applied < len(self.log):
                entry = self.log[self.last_applied]
                if self.apply_fn is not None:
                    with contextlib.suppress(Exception):
                        self.apply_fn(entry)
        if self.commit_index <= self.last_log_index():
            self.last_applied = self.commit_index
        else:
            self.last_applied = self.last_log_index()

    def _default_apply(self, entry) -> None:
        if self.storage is None:
            return
        cmd = getattr(entry, "command", b"")
        if isinstance(cmd, str):
            cmd = cmd.encode()
        elif not isinstance(cmd, (bytes, bytearray)):
            try:
                cmd = bytes(cmd) if cmd is not None else b""
            except Exception:
                cmd = b""
        cmd = bytes(cmd) if cmd is not None else b""
        if not cmd:
            return
        try:
            obj = json.loads(cmd.decode("utf-8"))
        except Exception:
            return
        if not isinstance(obj, dict):
            return
        op = obj.get("op")
        if op == "put":
            with contextlib.suppress(Exception):
                self.storage.upsert_meta(
                    str(obj.get("key")),
                    str(obj.get("mode", "")),
                    int(obj.get("version", 0)),
                    int(obj.get("size", 0)),
                    str(obj.get("checksum", "")),
                    list(obj.get("owners", [])) if obj.get("owners", []) is not None else [],
                )
        elif op == "mode":
            with contextlib.suppress(Exception):
                self.storage.set_raft_state("default_mode", str(obj.get("mode")))
        elif op == "noop":
            pass

    def _apply_entry(self, entry) -> None:
        if self.storage is not None:
            with contextlib.suppress(Exception):
                self._default_apply(entry)
        if self._user_apply_fn is not None:
            with contextlib.suppress(Exception):
                self._user_apply_fn(entry)

    def _persist_term(self) -> None:
        self._persist_state()

    def _persist_voted_for(self) -> None:
        self._persist_state()

    def _persist_state(self) -> None:
        if self.storage is None:
            return
        try:
            self.storage.set_raft_state("current_term", str(self.current_term))
            self.storage.set_raft_state(
                "voted_for", self.voted_for if self.voted_for is not None else ""
            )
            self.storage.set_raft_state("commit_index", str(self.commit_index))
        except Exception:
            pass

    def _persist_append(self, entry) -> int:
        if self.storage is None:
            if getattr(entry, "index", 0) <= 0:
                entry.index = self.last_log_index() + 1
            return entry.index
        try:
            idx = self.storage.append_log(entry.term, bytes(entry.command) if entry.command is not None else b"")
            entry.index = idx
            return idx
        except Exception:
            if getattr(entry, "index", 0) <= 0:
                entry.index = self.last_log_index() + 1
            return entry.index

    def _persist_truncate(self, idx: int) -> None:
        if self.storage is None:
            return
        with contextlib.suppress(Exception):
            self.storage.truncate_log(idx)

    def last_log_index(self) -> int:
        if not self.log:
            return 0
        return self.log[-1].index

    def last_log_term(self) -> int:
        if not self.log:
            return 0
        return self.log[-1].term

    def _term_at(self, idx: int) -> int:
        if 0 <= idx < len(self.log):
            return self.log[idx].term
        return 0

    def _entry_from(self, raw: Any) -> RaftLogEntry:
        if isinstance(raw, RaftLogEntry):
            cmd = raw.command
            if isinstance(cmd, str):
                cmd = cmd.encode()
            elif not isinstance(cmd, (bytes, bytearray)):
                cmd = bytes(cmd) if cmd is not None else b""
            return RaftLogEntry(term=int(raw.term), index=int(raw.index), command=bytes(cmd))
        if isinstance(raw, dict):
            cmd = raw.get("command", b"")
            if isinstance(cmd, str):
                cmd = cmd.encode()
            elif not isinstance(cmd, (bytes, bytearray)):
                cmd = bytes(cmd) if cmd is not None else b""
            return RaftLogEntry(
                term=int(raw.get("term", 0)),
                index=int(raw.get("index", 0)),
                command=bytes(cmd),
            )
        if isinstance(raw, (tuple, list)):
            if len(raw) >= 3:
                cmd = raw[2]
                if isinstance(cmd, str):
                    cmd = cmd.encode()
                elif not isinstance(cmd, (bytes, bytearray)):
                    cmd = bytes(cmd) if cmd is not None else b""
                return RaftLogEntry(term=int(raw[0]), index=int(raw[1]), command=bytes(cmd))
            if len(raw) == 2:
                cmd = raw[1]
                if isinstance(cmd, str):
                    cmd = cmd.encode()
                elif not isinstance(cmd, (bytes, bytearray)):
                    cmd = bytes(cmd) if cmd is not None else b""
                return RaftLogEntry(term=int(raw[0]), index=self.last_log_index() + 1, command=bytes(cmd))
        cmd = raw
        if isinstance(cmd, str):
            cmd = cmd.encode()
        elif not isinstance(cmd, (bytes, bytearray)):
            cmd = bytes(cmd) if cmd is not None else b""
        return RaftLogEntry(term=self.current_term, index=self.last_log_index() + 1, command=bytes(cmd))

    # ---- degraded / failure detection helpers ----

    def set_apply_callback(self, fn) -> None:
        with self._lock:
            self._user_apply_fn = fn
            if self.storage is not None or fn is not None:
                self.apply_fn = self._apply_entry
            else:
                self.apply_fn = None
            self._apply_committed_locked()

    def _apply_committed_locked(self) -> None:
        while self.last_applied < self.commit_index and self.last_applied + 1 < len(self.log):
            self.last_applied += 1
            if self.last_applied < len(self.log):
                entry = self.log[self.last_applied]
                if self.apply_fn is not None:
                    with contextlib.suppress(Exception):
                        self.apply_fn(entry)


def __getattr__(name):
    # Backward-compat delegation to the facade module.
    try:
        from . import node as _node
        return getattr(_node, name)
    except Exception as exc:
        raise AttributeError(name) from exc


__all__ = ["RaftPersistence", "RaftLogEntry"]
