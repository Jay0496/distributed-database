"""Raft leader election logic."""

import contextlib
import random
import threading
import time


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


def __getattr__(name):
    try:
        from . import node as _node
        return getattr(_node, name)
    except Exception as exc:
        raise AttributeError(name) from exc


__all__ = ["RaftElection"]
