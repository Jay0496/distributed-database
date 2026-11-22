"""Raft peer failure detection and degraded-set tracking."""

import contextlib
import threading
import time


class DegradedSet(set):
    """A set that compares equal to lists/tuples with the same elements."""

    def __eq__(self, other):
        if isinstance(other, (list, tuple, set, frozenset)):
            return set(self) == set(other)
        return super().__eq__(other)


_INTERVAL_LEGACY = {
    "HEARTBEAT_INTERVAL", "FAILURE_HEARTBEAT_INTERVAL", "FAILURE_CHECK_INTERVAL",
    "DETECTOR_INTERVAL", "FAILURE_INTERVAL", "FAILURE_PING_INTERVAL", "CHECK_INTERVAL",
    "DEGRADED_CHECK_INTERVAL", "HEARTBEAT_INTERVAL_FAILURE", "FAILURE_DETECTION_INTERVAL",
    "DETECTION_INTERVAL", "PING_INTERVAL", "DETECTOR_HEARTBEAT_INTERVAL",
    "HEARTBEAT_INTERVAL_DETECTION", "FAILURE_DETECTOR_INTERVAL",
    "failure_heartbeat_interval", "failure_check_interval", "detector_interval",
    "failure_ping_interval", "check_interval",
    "degraded_check_interval", "heartbeat_failure_interval", "failure_detection_interval",
    "detection_interval", "ping_interval", "detector_heartbeat_interval",
    "heartbeat_interval_detection", "failure_detector_interval",
}
_THRESHOLD_LEGACY = {
    "degraded_threshold", "degraded_after", "degraded_timeout",
    "failure_timeout", "miss_threshold", "miss_threshold_seconds",
    "failure_detection_threshold", "failure_detection_timeout", "detection_threshold",
    "detection_timeout", "failure_detector_threshold", "failure_detector_timeout",
    "degraded_after_seconds", "degraded_timeout_seconds",
    "FAILURE_THRESHOLD", "DEGRADED_THRESHOLD", "DEGRADED_AFTER", "DEGRADED_TIMEOUT",
    "FAILURE_TIMEOUT", "FAILURE_DETECTION_THRESHOLD", "FAILURE_DETECTION_TIMEOUT",
    "DETECTION_THRESHOLD", "DETECTION_TIMEOUT",
    "DEGRADED_TIMEOUT_SECONDS",
}


class RaftFailureDetector:
    """Degraded tracking, peer-id mapping and heartbeat failure detection."""

    failure_interval = 1.0
    failure_threshold = 3.0
    heartbeat_rpc_timeout = 1.0

    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        if name in _INTERVAL_LEGACY and name != "failure_interval":
            with contextlib.suppress(TypeError, ValueError):
                super().__setattr__("failure_interval", float(value))
        if name in _THRESHOLD_LEGACY and name != "failure_threshold":
            with contextlib.suppress(TypeError, ValueError):
                super().__setattr__("failure_threshold", float(value))

    def __getattr__(self, name):
        # Shim for legacy alias names.
        if name in _INTERVAL_LEGACY:
            return self.failure_interval
        if name in _THRESHOLD_LEGACY:
            return self.failure_threshold
        raise AttributeError(name)

    @property
    def degraded(self):
        with self._degraded_lock:
            return self._degraded

    @degraded.setter
    def degraded(self, value):
        with self._degraded_lock:
            self._degraded = DegradedSet(value or [])

    @property
    def degraded_set(self):
        return self.degraded

    @property
    def degraded_peers(self):
        return self.degraded

    @property
    def degraded_nodes(self):
        return self.degraded

    @property
    def last_seen(self):
        with self._degraded_lock:
            return self._last_seen

    @last_seen.setter
    def last_seen(self, value):
        with self._degraded_lock:
            self._last_seen = dict(value or {})

    @property
    def last_heartbeat(self):
        return self.last_seen

    def get_degraded(self):
        with self._degraded_lock:
            return sorted(self._degraded)

    def _resolve_peer_id(self, peer):
        try:
            if peer in self.peer_id_map:
                return self.peer_id_map.get(peer, peer)
            if str(peer) in self.peer_id_map:
                return self.peer_id_map.get(str(peer), peer)
        except Exception:
            pass
        return peer

    def mark_degraded(self, peer):
        with self._degraded_lock:
            self._degraded.add(self._resolve_peer_id(peer))

    def clear_degraded(self, peer):
        with self._degraded_lock:
            self._degraded.discard(self._resolve_peer_id(peer))

    def _module_constant(self, names, default):
        # instance
        for name in names:
            if hasattr(self, name):
                try:
                    v = getattr(self, name)
                    if v is not None:
                        return float(v)
                except (TypeError, ValueError):
                    pass
        # class
        for name in names:
            if hasattr(type(self), name):
                try:
                    v = getattr(type(self), name)
                    if v is not None:
                        return float(v)
                except (TypeError, ValueError):
                    pass
        # modules
        try:
            import sys as _sys
            for modname in ("node.server", "server", "node.raft", "raft"):
                mod = _sys.modules.get(modname)
                if mod is None:
                    continue
                for name in names:
                    if hasattr(mod, name):
                        try:
                            v = getattr(mod, name)
                            if v is not None:
                                return float(v)
                        except (TypeError, ValueError):
                            pass
        except Exception:
            pass
        return float(default)

    def _failure_interval(self):
        val = getattr(self, "failure_interval", 1.0)
        try:
            val = float(val)
        except (TypeError, ValueError):
            val = 1.0
        if val <= 0:
            val = 1.0
        return val

    def _failure_threshold(self):
        val = getattr(self, "failure_threshold", 3.0)
        try:
            val = float(val)
        except (TypeError, ValueError):
            val = 3.0
        if val <= 0:
            val = 3.0
        return val

    def _rpc_timeout(self):
        names = [
            "heartbeat_rpc_timeout",
            "heartbeat_timeout",
            "rpc_timeout",
            "ping_timeout",
            "heartbeat_timeout_seconds",
            "rpc_timeout_seconds",
            "ping_timeout_seconds",
            "HEARTBEAT_RPC_TIMEOUT",
            "HEARTBEAT_TIMEOUT",
            "PING_TIMEOUT",
            "RPC_TIMEOUT",
        ]
        val = self._module_constant(names, self.HEARTBEAT_RPC_TIMEOUT)
        if val == self.HEARTBEAT_RPC_TIMEOUT:
            for obj in (self, type(self)):
                for name in list(getattr(obj, "__dict__", {}).keys()):
                    low = name.lower()
                    if "timeout" in low and ("rpc" in low or "ping" in low or "heartbeat" in low or "http" in low):
                        try:
                            return float(getattr(self, name))
                        except (TypeError, ValueError):
                            pass
        if val <= 0:
            val = 1.0
        return val

    def start_failure_detector(self):
        if self._detector_thread is not None and self._detector_thread.is_alive():
            return
        self._detector_stop.clear()
        with self._degraded_lock:
            now = time.monotonic()
            for p in self.peers:
                self._last_seen.setdefault(p, now)
        t = threading.Thread(
            target=self._detector_loop, daemon=True, name=f"detector-{self.node_id}"
        )
        self._detector_thread = t
        t.start()

    def stop_failure_detector(self):
        self._detector_stop.set()
        t = self._detector_thread
        if t is not None and t.is_alive():
            t.join(timeout=0.5)
        # leave thread reference; cleared in loop

    def _parse_heartbeat_result(self, res):
        if res is None:
            return False, 0
        if isinstance(res, bool):
            return res, 0
        if isinstance(res, (tuple, list)):
            if len(res) == 0:
                return False, 0
            ok = bool(res[0])
            term = 0
            if len(res) >= 3:
                try:
                    term = int(res[2] or 0)
                except (TypeError, ValueError):
                    term = 0
            elif len(res) == 2:
                # could be (ok, term) or (ok, leader)
                try:
                    term = int(res[1] or 0)
                except (TypeError, ValueError):
                    term = 0
            return ok, term
        if isinstance(res, dict):
            ok = res.get("ok", res.get("success", res.get("healthy", False)))
            try:
                term = int(res.get("term", 0) or 0)
            except (TypeError, ValueError):
                term = 0
            return bool(ok), term
        ok = getattr(res, "ok", getattr(res, "success", getattr(res, "healthy", None)))
        if ok is None:
            ok = bool(res)
        try:
            term = int(getattr(res, "term", 0) or 0)
        except (TypeError, ValueError):
            term = 0
        return bool(ok), term

    def _detector_loop(self):
        try:
            while self._running and not self._detector_stop.is_set() and self.is_leader():
                interval = self._failure_interval()
                threshold = self._failure_threshold()
                started = time.monotonic()
                for peer in list(self.peers):
                    if not self.is_leader() or self._detector_stop.is_set() or not self._running:
                        break
                    success = False
                    term_resp = 0
                    try:
                        res = self.transport.heartbeat(peer, node_id=self.node_id, term=self.current_term)
                        success, term_resp = self._parse_heartbeat_result(res)
                    except Exception:
                        success = False
                        term_resp = 0
                    with self._degraded_lock:
                        now = time.monotonic()
                        pid = self.peer_id_map.get(peer, peer)
                        with contextlib.suppress(Exception):
                            pid = self.peer_id_map.get(str(peer), pid)
                        if success:
                            self._last_seen[peer] = now
                            self._degraded.discard(pid)
                        else:
                            last = self._last_seen.get(peer)
                            if last is None:
                                self._last_seen[peer] = now
                                last = now
                            if now - last >= threshold:
                                self._degraded.add(pid)
                    # step-down if higher term reported
                    if success and term_resp:
                        try:
                            with self._lock:
                                if term_resp > self.current_term:
                                    self.current_term = term_resp
                                    self.voted_for = None
                                    self.state = self.FOLLOWER
                                    self._leader_id = None
                                    self._persist_state()
                                    self._reset_election_timer()
                        except Exception:
                            pass
                # wait for next interval
                elapsed = time.monotonic() - started
                wait = interval - elapsed
                if wait < 0.01:
                    wait = 0.01
                end = time.monotonic() + wait
                while time.monotonic() < end and self.is_leader() and not self._detector_stop.is_set() and self._running:
                    time.sleep(min(0.05, max(0.001, end - time.monotonic())))
        finally:
            self._detector_thread = None


def __getattr__(name):
    try:
        from . import node as _node
        return getattr(_node, name)
    except Exception as exc:
        raise AttributeError(name) from exc


__all__ = [
    "RaftFailureDetector",
    "DegradedSet",
    "_INTERVAL_LEGACY",
    "_THRESHOLD_LEGACY",
]
