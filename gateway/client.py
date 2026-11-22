"""Gateway client: leader discovery with retry."""

import os
import sys
import time

try:
    import grpc
    from gen import kv_pb2, kv_pb2_grpc
except Exception:  # pragma: no cover
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    import grpc
    from gen import kv_pb2, kv_pb2_grpc


class GatewayClient:
    """Client used by the gateway to talk to the cluster leader."""

    def __init__(self, peers):
        if isinstance(peers, str):
            peers = [p.strip() for p in peers.split(",") if p.strip()]
        self.peers = list(peers or [])
        self._channels = {}
        self._stubs = {}
        for addr in self.peers:
            self._ensure_channel(addr)
        self._leader = None
        self._last_resp = None

    # -- channel/stub helpers -------------------------------------------------
    @property
    def channels(self):
        return self._channels

    @channels.setter
    def channels(self, value):
        self._channels = value

    @property
    def stubs(self):
        return self._stubs

    @stubs.setter
    def stubs(self, value):
        self._stubs = value

    def _ensure_channel(self, addr):
        if addr in self._channels:
            return self._stubs.get(addr)
        try:
            ch = grpc.insecure_channel(addr)
            self._channels[addr] = ch
            self._stubs[addr] = kv_pb2_grpc.KVStub(ch)
        except Exception:
            self._channels[addr] = None
            self._stubs[addr] = None
        return self._stubs.get(addr)

    def _stub(self, addr):
        if addr not in self._stubs:
            self._ensure_channel(addr)
        return self._stubs.get(addr)

    def _call_get_leader(self, addr):
        stub = self._stub(addr)
        if stub is None:
            return None
        try:
            return stub.GetLeader(kv_pb2.GetLeaderRequest(), timeout=2.0)
        except Exception:
            return None

    def _map_leader_to_addr(self, leader):
        if not leader:
            return None
        if leader in self.peers:
            return leader
        for p in self.peers:
            host = p.split(":")[0]
            if host == leader or p == leader:
                return p
            if leader in p or p.startswith(leader) or host in leader or leader.startswith(host):
                return p
        if len(self.peers) == 1:
            return self.peers[0]
        return leader

    # -- leader discovery ------------------------------------------------------
    def discover_leader(self):
        """Query every peer's GetLeader; cache and return the leader address."""
        for attempt in range(3):
            responses = []
            for addr in self.peers:
                resp = self._call_get_leader(addr)
                if resp is not None:
                    responses.append((addr, resp))
            counts = {}
            for addr, resp in responses:
                l = getattr(resp, "leader", "") or ""
                if l:
                    counts[l] = counts.get(l, 0) + 1
            if counts:
                leader_id = max(counts.items(), key=lambda kv: kv[1])[0]
                self._leader = self._map_leader_to_addr(leader_id)
                for addr, resp in responses:
                    if (getattr(resp, "leader", "") or "") == leader_id:
                        self._last_resp = resp
                        break
                if self._last_resp is None and responses:
                    self._last_resp = responses[0][1]
                return self._leader
            time.sleep(0.1 * (attempt + 1))
        return self._leader

    def get_leader_stub(self):
        """Return stub for cached leader, rediscovering on failure."""
        if self._leader is None:
            self.discover_leader()
        if self._leader in self._stubs:
            stub = self._stubs.get(self._leader)
            if stub is not None:
                return stub
        self.discover_leader()
        if self._leader in self._stubs:
            stub = self._stubs.get(self._leader)
            if stub is not None:
                return stub
        # fallback: first available peer
        for addr in self.peers:
            stub = self._stubs.get(addr)
            if stub is not None:
                return stub
        return None

    def call_with_retry(self, fn):
        """Invoke fn(stub); rediscover leader and retry on failure."""
        last_exc = None
        for attempt in range(3):
            stub = self.get_leader_stub()
            if stub is None:
                self._leader = None
                self.discover_leader()
                stub = self.get_leader_stub()
            if stub is None:
                last_exc = RuntimeError("no leader available")
                time.sleep(0.05 * (attempt + 1))
                continue
            try:
                return fn(stub)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                self._leader = None
                try:
                    self.discover_leader()
                except Exception:
                    pass
                time.sleep(0.05 * (attempt + 1))
        if last_exc is not None:
            raise last_exc
        return None