"""Raft log replication and commit advancement."""

import contextlib
import threading
import time

from .persistence import RaftLogEntry


class RaftReplication:
    """AppendEntries handling, replication rounds and commit advancement."""

    def handle_append_entries(
        self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit
    ):
        with self._lock:
            if term < self.current_term:
                return self.current_term, False

            if term > self.current_term:
                self.current_term = term
                self.voted_for = None
                self.state = self.FOLLOWER
                self._leader_id = None
                self._persist_state()

            self._leader_id = leader_id
            self.state = self.FOLLOWER
            self._reset_election_timer()

            prev_log_index = 0 if prev_log_index is None else int(prev_log_index)
            prev_log_term = 0 if prev_log_term is None else int(prev_log_term)

            # validation
            if prev_log_index >= len(self.log):
                return self.current_term, False
            if prev_log_index >= 0 and self._term_at(prev_log_index) != prev_log_term:
                return self.current_term, False

            # truncate conflicting suffix
            if prev_log_index + 1 < len(self.log):
                del self.log[prev_log_index + 1 :]
                self._persist_truncate(prev_log_index + 1)

            # append entries not already present
            for raw in (entries or []):
                e = self._entry_from(raw)
                if e.index <= 0:
                    continue
                if e.index <= self.last_log_index():
                    if self._term_at(e.index) != e.term:
                        del self.log[e.index :]
                        self._persist_truncate(e.index)
                        self._persist_append(e)
                        self.log.append(e)
                else:
                    self._persist_append(e)
                    self.log.append(e)

            # advance commit index
            if leader_commit is not None:
                new_commit = min(int(leader_commit), self.last_log_index())
                if new_commit > self.commit_index:
                    self.commit_index = new_commit
                    self._persist_state()
                    self._apply_committed_locked()

            return self.current_term, True

    def _replicate_once(self, peer: str) -> None:
        max_attempts = 3
        for attempt in range(max_attempts):
            with self._lock:
                if self.state != self.LEADER:
                    return
                term = self.current_term
                ni = self.next_index.get(peer, self.last_log_index() + 1)
                if ni < 1:
                    ni = 1
                prev_idx = ni - 1
                prev_term = self._term_at(prev_idx)
                entries = list(self.log[ni:])
                commit = self.commit_index

            try:
                pt, success = self.transport.append_entries(
                    peer,
                    {
                        "term": term,
                        "leader_id": self.node_id,
                        "prev_log_index": prev_idx,
                        "prev_log_term": prev_term,
                        "entries": entries,
                        "leader_commit": commit,
                    },
                )
            except Exception:
                pt, success = 0, False

            if pt is not None and pt > term:
                with self._lock:
                    if pt > self.current_term:
                        self.current_term = pt
                        self.voted_for = None
                        self.state = self.FOLLOWER
                        self._leader_id = None
                        self._persist_state()
                        self._reset_election_timer()
                return

            with self._lock:
                if self.state != self.LEADER or self.current_term != term:
                    return
                if success:
                    new_match = prev_idx + len(entries)
                    if new_match > self.match_index.get(peer, 0):
                        self.match_index[peer] = new_match
                    self.next_index[peer] = new_match + 1
                    self._advance_commit_index_locked()
                    return
                else:
                    cur = self.next_index.get(peer, self.last_log_index() + 1)
                    self.next_index[peer] = max(1, cur - 1)

            if attempt < max_attempts - 1:
                time.sleep(0.01 * (attempt + 1))

    def _replication_round(self) -> None:
        if not self.is_leader():
            return
        for p in list(self.peers):
            with contextlib.suppress(Exception):
                self._replicate_once(p)

    def _advance_commit_index(self) -> None:
        with self._lock:
            self._advance_commit_index_locked()

    def _advance_commit_index_locked(self) -> None:
        if self.state != self.LEADER:
            return
        majority = (len(self.peers) + 1) // 2 + 1
        for i in range(self.last_log_index(), self.commit_index, -1):
            if i <= 0:
                break
            if self._term_at(i) != self.current_term:
                continue
            count = 1
            for p in self.peers:
                if self.match_index.get(p, 0) >= i:
                    count += 1
            if count >= majority:
                self.commit_index = i
                self._persist_state()
                self._apply_committed_locked()
                break

    def append_command(self, command) -> int | None:
        if isinstance(command, str):
            command = command.encode()
        elif not isinstance(command, (bytes, bytearray)):
            try:
                command = bytes(command)
            except Exception:
                command = str(command).encode()
        with self._lock:
            if self.state != self.LEADER:
                return None
            entry = RaftLogEntry(term=self.current_term, index=0, command=bytes(command))
            idx = self._persist_append(entry)
            self.log.append(entry)
            self._persist_state()
            self._advance_commit_index_locked()
        threading.Thread(target=self._replication_round, daemon=True).start()
        return idx

    def _send_heartbeats(self) -> None:
        self._replication_round()


def __getattr__(name):
    try:
        from . import node as _node
        return getattr(_node, name)
    except Exception as exc:
        raise AttributeError(name) from exc


__all__ = ["RaftReplication"]
