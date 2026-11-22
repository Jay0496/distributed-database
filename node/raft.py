"""Raft consensus node (election + log replication)."""

import json
import random
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional


@dataclass
class RaftLogEntry:
    term: int
    index: int
    command: bytes = b""


_NODE_REGISTRY: Dict[str, "RaftNode"] = {}


class RaftNode:
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

    def __init__(
        self,
        node_id: str,
        peers: List[str],
        storage: Any = None,
        transport: Any = None,
        election_timeout_range: tuple = (0.15, 0.30),
        heartbeat_interval: float = 0.05,
    ):
        self.node_id = node_id
        self.peers = list(peers)
        self.storage = storage
        self.election_timeout_range = tuple(election_timeout_range)
        self.heartbeat_interval = heartbeat_interval

        if transport is None:
            _NODE_REGISTRY[node_id] = self
            self.transport = DirectTransport()
        else:
            self.transport = transport

        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.commit_index = 0
        self.last_applied = 0
        self.state = self.FOLLOWER
        self._leader_id: Optional[str] = None

        self.log: List[RaftLogEntry] = [RaftLogEntry(term=0, index=0, command=b"")]
        self.apply_fn: Optional[Callable[[RaftLogEntry], None]] = None
        self._user_apply_fn: Optional[Callable[[RaftLogEntry], None]] = None
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        self._lock = threading.RLock()
        self._running = False
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.election_deadline = 0.0

        self.load_from_storage()

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
                    try:
                        self.apply_fn(entry)
                    except Exception:
                        pass
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
            try:
                self.storage.upsert_meta(
                    str(obj.get("key")),
                    str(obj.get("mode", "")),
                    int(obj.get("version", 0)),
                    int(obj.get("size", 0)),
                    str(obj.get("checksum", "")),
                    list(obj.get("owners", [])) if obj.get("owners", []) is not None else [],
                )
            except Exception:
                pass
        elif op == "mode":
            try:
                self.storage.set_raft_state("default_mode", str(obj.get("mode")))
            except Exception:
                pass
        elif op == "noop":
            pass

    def _apply_entry(self, entry) -> None:
        if self.storage is not None:
            try:
                self._default_apply(entry)
            except Exception:
                pass
        if self._user_apply_fn is not None:
            try:
                self._user_apply_fn(entry)
            except Exception:
                pass

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
        try:
            self.storage.truncate_log(idx)
        except Exception:
            pass

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

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._reset_election_timer()
        self._thread = threading.Thread(target=self._run, daemon=True, name=f"raft-{self.node_id}")
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        self._stop_event.set()
        t = self._thread
        if t is not None and t.is_alive():
            t.join(timeout=2.0)
        self._thread = None
        try:
            del _NODE_REGISTRY[self.node_id]
        except KeyError:
            pass

    def is_leader(self) -> bool:
        with self._lock:
            return self.state == self.LEADER

    def leader_id(self) -> Optional[str]:
        with self._lock:
            if self.state == self.LEADER:
                return self.node_id
            return self._leader_id

    def handle_request_vote(
        self, term, candidate_id, last_log_index, last_log_term
    ):
        with self._lock:
            if term > self.current_term:
                self.current_term = term
                self.voted_for = None
                self.state = self.FOLLOWER
                self._leader_id = None
                self._persist_state()
                self._reset_election_timer()

            if term < self.current_term:
                return self.current_term, False

            def log_ok():
                my_term = self.last_log_term()
                my_idx = self.last_log_index()
                if last_log_term != my_term:
                    return last_log_term > my_term
                return last_log_index >= my_idx

            granted = False
            if (self.voted_for is None or self.voted_for == candidate_id) and log_ok():
                self.voted_for = candidate_id
                granted = True
                self._persist_state()
                self._reset_election_timer()

            return self.current_term, granted

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
                    try:
                        self.apply_fn(entry)
                    except Exception:
                        pass

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

    def _reset_election_timer(self) -> None:
        timeout = random.uniform(*self.election_timeout_range)
        self.election_deadline = time.monotonic() + timeout

    def _run(self) -> None:
        self._reset_election_timer()
        while self._running:
            if self.state == self.LEADER:
                self._replication_round()
                slept = 0.0
                while slept < self.heartbeat_interval and self._running:
                    time.sleep(min(0.01, self.heartbeat_interval - slept))
                    slept += 0.01
            else:
                now = time.monotonic()
                if now >= self.election_deadline:
                    self._start_election()
                else:
                    wait = min(0.01, self.election_deadline - now)
                    time.sleep(wait if wait > 0 else 0.001)

    def _start_election(self) -> None:
        with self._lock:
            if self.state == self.LEADER:
                return
            self.state = self.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            self._leader_id = None
            term = self.current_term
            self._persist_state()
            self._reset_election_timer()

        last_idx = self.last_log_index()
        last_term = self.last_log_term()

        req = {
            "term": term,
            "candidate_id": self.node_id,
            "last_log_index": last_idx,
            "last_log_term": last_term,
        }

        results = []
        rlock = threading.Lock()

        def worker(peer):
            try:
                pt, granted = self.transport.request_vote(peer, dict(req))
                with rlock:
                    results.append((peer, pt, granted))
            except Exception:
                with rlock:
                    results.append((peer, term, False))

        threads = []
        for p in self.peers:
            t = threading.Thread(target=worker, args=(p,), daemon=True)
            t.start()
            threads.append(t)
        for t in threads:
            t.join(timeout=0.15)

        with self._lock:
            if self.state != self.CANDIDATE or self.current_term != term:
                return
            for peer, pt, granted in results:
                if pt > self.current_term:
                    self.current_term = pt
                    self.voted_for = None
                    self.state = self.FOLLOWER
                    self._leader_id = None
                    self._persist_state()
                    self._reset_election_timer()
                    return
            votes = 1 + sum(1 for _, _, g in results if g)
            majority = (len(self.peers) + 1) // 2 + 1
            if votes >= majority:
                self.state = self.LEADER
                self._leader_id = self.node_id
                self.next_index = {p: self.last_log_index() + 1 for p in self.peers}
                self.match_index = {p: 0 for p in self.peers}
            self._reset_election_timer()

        if self.is_leader():
            self._send_heartbeats()

    def _replication_round(self) -> None:
        if not self.is_leader():
            return
        for p in list(self.peers):
            try:
                self._replicate_once(p)
            except Exception:
                pass

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

    def _send_heartbeats(self) -> None:
        self._replication_round()

    def append_command(self, command) -> Optional[int]:
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


class DirectTransport:
    def __init__(self, nodes=None):
        self.nodes = nodes if nodes is not None else _NODE_REGISTRY

    def _node(self, peer_id):
        n = self.nodes.get(peer_id) if hasattr(self.nodes, "get") else None
        if n is None:
            n = _NODE_REGISTRY.get(peer_id)
        return n

    def request_vote(self, peer_id, req_dict):
        node = self._node(peer_id)
        if node is None:
            return 0, False
        return node.handle_request_vote(
            req_dict["term"],
            req_dict["candidate_id"],
            req_dict["last_log_index"],
            req_dict["last_log_term"],
        )

    def append_entries(self, peer_id, req_dict):
        node = self._node(peer_id)
        if node is None:
            return 0, False
        return node.handle_append_entries(
            req_dict["term"],
            req_dict["leader_id"],
            req_dict["prev_log_index"],
            req_dict["prev_log_term"],
            req_dict.get("entries", []),
            req_dict["leader_commit"],
        )

    def store_shard(self, peer_id, key, shard_index, version, data):
        node = self._node(peer_id)
        if node is None:
            return False
        try:
            node.storage.store_shard(key, int(shard_index), int(version), bytes(data))
            return True
        except Exception:
            return False

    def fetch_shard(self, peer_id, key, shard_index):
        node = self._node(peer_id)
        if node is None:
            return None
        try:
            return node.storage.fetch_shard(key, int(shard_index))
        except Exception:
            return None

    def has_shard(self, peer_id, key, shard_index):
        node = self._node(peer_id)
        if node is None:
            return False
        try:
            return bool(node.storage.has_shard(key, int(shard_index)))
        except Exception:
            return False
