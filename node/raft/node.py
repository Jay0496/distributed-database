"""Raft consensus node (election + log replication)."""

import contextlib
import threading
import time
from collections.abc import Callable
from typing import Any

# Failure detection constants (patchable by tests). Canonical names only;
# legacy names resolve to these via module-level __getattr__ below.
failure_interval = 1.0
failure_threshold = 3.0
heartbeat_rpc_timeout = 1.0


_NODE_REGISTRY: dict[str, "RaftNode"] = {}


from .election import RaftElection  # noqa: E402
from .failure_detector import (  # noqa: E402
    _INTERVAL_LEGACY,
    _THRESHOLD_LEGACY,
    DegradedSet,
    RaftFailureDetector,
)
from .persistence import RaftLogEntry, RaftPersistence  # noqa: E402
from .replication import RaftReplication  # noqa: E402


class RaftNode(
    RaftPersistence, RaftElection, RaftReplication, RaftFailureDetector
):
    def __init__(
        self,
        node_id: str,
        peers: list[str],
        storage: Any = None,
        transport: Any = None,
        election_timeout_range: tuple = (0.15, 0.30),
        heartbeat_interval: float = 0.05,
        peer_ids: list[str] = None,
        peer_id_map: Any = None,
        peers_ids: list[str] = None,
        failure_heartbeat_interval: float = 1.0,
        failure_threshold: float = 3.0,
        degraded_threshold: float = None,
        failure_check_interval: float = None,
        detector_interval: float = None,
        degraded_after: float = None,
        heartbeat_rpc_timeout: float = 1.0,
        degraded_timeout: float = None,
        **kwargs,
    ):
        self.node_id = node_id
        self.peers = list(peers)
        # peer id mapping (address -> node id). Defaults to identity for in-process tests.
        self.peer_ids = list(peer_ids) if peer_ids is not None else None
        self.peer_id_map = dict(peer_id_map) if peer_id_map is not None else None
        if self.peer_ids is None:
            self.peer_ids = list(self.peers)
        self.storage = storage
        self.election_timeout_range = tuple(election_timeout_range)
        self.heartbeat_interval = heartbeat_interval

        if degraded_threshold is None:
            degraded_threshold = failure_threshold
        if failure_check_interval is None:
            failure_check_interval = failure_heartbeat_interval
        if detector_interval is None:
            detector_interval = failure_heartbeat_interval
        if degraded_after is None:
            degraded_after = degraded_threshold
        if degraded_timeout is None:
            degraded_timeout = degraded_threshold

        self.failure_heartbeat_interval = float(failure_heartbeat_interval)
        self.failure_check_interval = float(failure_check_interval)
        self.detector_interval = float(detector_interval)
        self.failure_interval = float(failure_heartbeat_interval)
        self.failure_ping_interval = float(failure_heartbeat_interval)
        self.check_interval = float(failure_heartbeat_interval)
        self.degraded_check_interval = float(failure_heartbeat_interval)
        self.failure_threshold = float(failure_threshold)
        self.degraded_threshold = float(degraded_threshold)
        self.degraded_after = float(degraded_after)
        self.degraded_timeout = float(degraded_timeout)
        self.failure_timeout = float(degraded_threshold)
        self.heartbeat_rpc_timeout = float(heartbeat_rpc_timeout)
        self.heartbeat_timeout = float(heartbeat_rpc_timeout)
        self.ping_timeout = float(heartbeat_rpc_timeout)
        for k, v in (kwargs or {}).items():
            with contextlib.suppress(Exception):
                setattr(self, k, v)
        # alias support
        if getattr(self, "peers_ids", None) and not getattr(self, "peer_ids", None):
            with contextlib.suppress(Exception):
                self.peer_ids = list(self.peers_ids)
        if not getattr(self, "peer_ids", None):
            self.peer_ids = list(self.peers)
        if len(self.peer_ids) != len(self.peers):
            self.peer_ids = list(self.peers)
        if not getattr(self, "peer_id_map", None):
            self.peer_id_map = {}
        self.peer_id_map = dict(self.peer_id_map or {})
        for i, addr in enumerate(self.peers):
            pid = self.peer_ids[i] if i < len(self.peer_ids) else addr
            self.peer_id_map.setdefault(addr, pid)
            self.peer_id_map.setdefault(str(addr), pid)

        if transport is None:
            _NODE_REGISTRY[node_id] = self
            self.transport = DirectTransport()
        else:
            self.transport = transport

        self.current_term = 0
        self.voted_for: str | None = None
        self.commit_index = 0
        self.last_applied = 0
        self.state = self.FOLLOWER
        self._leader_id: str | None = None

        self.log: list[RaftLogEntry] = [RaftLogEntry(term=0, index=0, command=b"")]
        self.apply_fn: Callable[[RaftLogEntry], None] | None = None
        self._user_apply_fn: Callable[[RaftLogEntry], None] | None = None
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        self._lock = threading.RLock()
        self._running = False
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self.election_deadline = 0.0

        self._degraded_lock = threading.RLock()
        self._degraded = DegradedSet()
        self._last_seen: dict[str, float] = {}
        self._detector_stop = threading.Event()
        self._detector_thread: threading.Thread | None = None
        self._supervisor_thread: threading.Thread | None = None

        self.load_from_storage()

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

    def list_keys(self, peer_id):
        node = self._node(peer_id)
        if node is None:
            return []
        try:
            if hasattr(node, "storage") and node.storage is not None:
                return list(node.storage.list_meta_keys())
        except Exception:
            pass
        try:
            if hasattr(node, "list_meta_keys"):
                return list(node.list_meta_keys())
        except Exception:
            pass
        return []

    def heartbeat(self, peer_id, node_id="", term=0):
        node = self._node(peer_id)
        if node is None:
            return False, "", 0
        try:
            if hasattr(node, "_running") and node._running is False:
                return False, "", 0
            lid = None
            if hasattr(node, "leader_id"):
                try:
                    lid = node.leader_id()
                except Exception:
                    lid = None
            if lid is None and hasattr(node, "_leader_id"):
                lid = getattr(node, "_leader_id", None)
            cur_term = 0
            if hasattr(node, "current_term"):
                try:
                    cur_term = int(node.current_term)
                except Exception:
                    cur_term = 0
            # if node is a NodeService-like object
            if hasattr(node, "raft") and getattr(node, "raft", None) is not None:
                r = node.raft
                try:
                    if hasattr(r, "_running") and r._running is False:
                        return False, "", 0
                except Exception:
                    pass
                with contextlib.suppress(Exception):
                    lid = r.leader_id() if hasattr(r, "leader_id") else lid
                with contextlib.suppress(Exception):
                    cur_term = int(r.current_term)
            return True, (lid if lid is not None else ""), int(cur_term or 0)
        except Exception:
            return False, "", 0


def __getattr__(name):
    if name in _INTERVAL_LEGACY:
        return failure_interval
    if name in _THRESHOLD_LEGACY:
        return failure_threshold
    if name in ("HEARTBEAT_RPC_TIMEOUT", "HEARTBEAT_TIMEOUT", "PING_TIMEOUT", "RPC_TIMEOUT", "heartbeat_rpc_timeout", "heartbeat_timeout", "ping_timeout", "rpc_timeout"):
        return heartbeat_rpc_timeout
    raise AttributeError(name)
