"""Raft leader election (no log replication yet)."""

import random
import threading
import time
from typing import Any, Dict, List, Optional

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
        self.state = self.FOLLOWER
        self._leader_id: Optional[str] = None

        self._lock = threading.RLock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self.election_deadline = 0.0

        if self.storage is not None:
            try:
                v = self.storage.get_raft_state("current_term")
                if v is not None:
                    self.current_term = int(v)
            except Exception:
                pass

    def _load_state(self) -> None:
        pass

    def _persist_term(self) -> None:
        pass

    def _persist_voted_for(self) -> None:
        pass

    def last_log_index(self) -> int:
        if self.storage is not None:
            try:
                ll = self.storage.last_log()
                if ll is not None:
                    return int(ll["idx"])
            except Exception:
                pass
        return 0

    def last_log_term(self) -> int:
        if self.storage is not None:
            try:
                ll = self.storage.last_log()
                if ll is not None:
                    return int(ll["term"])
            except Exception:
                pass
        return 0

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._reset_election_timer()
        self._thread = threading.Thread(target=self._run, daemon=True, name=f"raft-{self.node_id}")
        self._thread.start()

    def stop(self) -> None:
        self._running = False
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

    def handle_request_vote(self, term, candidate_id, last_log_index, last_log_term):
        with self._lock:
            if term > self.current_term:
                self.current_term = term
                self.voted_for = None
                self.state = self.FOLLOWER
                self._leader_id = None
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
                self._reset_election_timer()

            return self.current_term, granted

    def handle_append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        return self.current_term, False

    def _reset_election_timer(self) -> None:
        timeout = random.uniform(*self.election_timeout_range)
        self.election_deadline = time.monotonic() + timeout

    def _run(self) -> None:
        self._reset_election_timer()
        while self._running:
            if self.state == self.LEADER:
                time.sleep(self.heartbeat_interval)
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
                    self._reset_election_timer()
                    return
            votes = 1 + sum(1 for _, _, g in results if g)
            majority = (len(self.peers) + 1) // 2 + 1
            if votes >= majority:
                self.state = self.LEADER
                self._leader_id = self.node_id
            self._reset_election_timer()

    def _send_heartbeats(self) -> None:
        pass


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
