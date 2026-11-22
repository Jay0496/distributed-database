"""Raft leader election logic."""

import contextlib
import random
import threading
import time

from .direct_transport import _NODE_REGISTRY


class RaftElection:
    """Election, RequestVote handling and election timers."""

    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

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
            for _peer, pt, _granted in results:
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
                with contextlib.suppress(Exception):
                    self.start_failure_detector()
            self._reset_election_timer()

        if self.is_leader():
            self._send_heartbeats()

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

    def _reset_election_timer(self) -> None:
        timeout = random.uniform(*self.election_timeout_range)
        self.election_deadline = time.monotonic() + timeout


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
        self._detector_stop.set()
        t = self._detector_thread
        if t is not None and t.is_alive():
            t.join(timeout=2.0)
        self._detector_thread = None
        t = self._thread
        if t is not None and t.is_alive():
            t.join(timeout=2.0)
        self._thread = None
        with contextlib.suppress(KeyError):
            del _NODE_REGISTRY[self.node_id]

    def is_leader(self) -> bool:
        with self._lock:
            return self.state == self.LEADER

    def leader_id(self) -> str | None:
        with self._lock:
            if self.state == self.LEADER:
                return self.node_id
            return self._leader_id

    def _run(self) -> None:
        self._reset_election_timer()
        while self._running:
            if self.state == self.LEADER:
                if self._detector_thread is None or not self._detector_thread.is_alive():
                    self.start_failure_detector()
                self._replication_round()
                slept = 0.0
                while slept < self.heartbeat_interval and self._running:
                    time.sleep(min(0.01, self.heartbeat_interval - slept))
                    slept += 0.01
            else:
                if self._detector_thread is not None:
                    self.stop_failure_detector()
                now = time.monotonic()
                if now >= self.election_deadline:
                    self._start_election()
                else:
                    wait = min(0.01, self.election_deadline - now)
                    time.sleep(wait if wait > 0 else 0.001)

def __getattr__(name):
    try:
        from . import node as _node
        return getattr(_node, name)
    except Exception as exc:
        raise AttributeError(name) from exc


__all__ = ["RaftElection"]
