"""Gateway client: leader discovery with retry."""

import contextlib
import os
import time

try:
    from .ring import ConsistentHashRing
except Exception:  # pragma: no cover
    ConsistentHashRing = None

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
        self._leader_id = None
        self._leader_addr = None
        self._last_resp = None
        self._mode = None
        self.ring = None
        if ConsistentHashRing is not None:
            try:
                self.ring = ConsistentHashRing()
                for addr in self.peers:
                    self.ring.add_node(addr)
            except Exception:
                self.ring = None

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

    def _auth_token(self):
        token = getattr(self, "token", None)
        if token is None:
            token = os.environ.get("TOKEN", "")
        return token or ""

    def _metadata(self):
        token = self._auth_token()
        if not token:
            return None
        return [("authorization", f"Bearer {token}")]

    def _call_get_leader(self, addr):
        stub = self._stub(addr)
        if stub is None:
            return None
        try:
            md = self._metadata()
            kwargs = {"timeout": 2.0}
            if md is not None:
                kwargs["metadata"] = md
            return stub.GetLeader(kv_pb2.GetLeaderRequest(), **kwargs)
        except Exception:
            return None

    def _map_leader_to_addr(self, leader):
        if not leader:
            return None
        if leader in self.peers:
            return leader
        # leader is usually a node id like "node2"; map to the peer address
        # by its trailing index before falling back to fuzzy host matching.
        import re as _re
        m = _re.search(r"(\d+)\s*$", str(leader))
        if m:
            idx = int(m.group(1))
            if 1 <= idx <= len(self.peers):
                return self.peers[idx - 1]
            if 0 <= idx < len(self.peers):
                return self.peers[idx]
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
            for _addr, resp in responses:
                leader = getattr(resp, "leader", "") or ""
                if leader:
                    counts[leader] = counts.get(leader, 0) + 1
            if counts:
                leader_id = max(counts.items(), key=lambda kv: kv[1])[0]
                self._leader_id = leader_id
                self._leader_addr = self._map_leader_to_addr(leader_id)
                self._leader = self._leader_addr
                for _addr, resp in responses:
                    if (getattr(resp, "leader", "") or "") == leader_id:
                        self._last_resp = resp
                        break
                if self._last_resp is None and responses:
                    self._last_resp = responses[0][1]
                return self._leader_addr
            time.sleep(0.1 * (attempt + 1))
        return self._leader

    def get_leader_stub(self):
        """Return stub for cached leader, rediscovering on failure."""
        if self._leader_addr is None:
            self.discover_leader()
        if self._leader_addr in self._stubs:
            stub = self._stubs.get(self._leader_addr)
            if stub is not None:
                return stub
        self.discover_leader()
        if self._leader_addr in self._stubs:
            stub = self._stubs.get(self._leader_addr)
            if stub is not None:
                return stub
        # fallback: first available peer
        for addr in self.peers:
            stub = self._stubs.get(addr)
            if stub is not None:
                return stub
        return None


    def _leader_response(self):
        stub = self.get_leader_stub()
        if stub is None:
            return None
        try:
            md = self._metadata()
            kwargs = {"timeout": 2.0}
            if md is not None:
                kwargs["metadata"] = md
            return stub.GetLeader(kv_pb2.GetLeaderRequest(), **kwargs)
        except Exception:
            return None

    def status(self):
        """Return dict: leader, degraded, mode, key_count."""
        resp = self._leader_response()
        if resp is None:
            self._leader_id = None
            self._leader_addr = None
            self._leader = None
            self.discover_leader()
            resp = self._leader_response()
        if resp is None:
            return {"leader": self._leader_id, "degraded": [], "mode": None, "key_count": 0}
        if self._leader_id is None:
            lid = getattr(resp, "leader", None)
            self._leader_id = lid
            self._leader_addr = self._map_leader_to_addr(lid)
            self._leader = self._leader_addr
        mode = getattr(resp, "mode", None)
        self._mode = mode

        def _norm(p):
            try:
                s = str(p)
            except Exception:
                return p
            if ":" in s:
                s = s.split(":", 1)[0]
            return s

        degraded = []
        try:
            degraded = sorted({ _norm(p) for p in (getattr(resp, "degraded", []) or []) })
        except Exception:
            degraded = [ _norm(p) for p in (getattr(resp, "degraded", []) or []) ]
        return {
            "leader": self._leader_id if self._leader_id is not None else getattr(resp, "leader", None),
            "degraded": degraded,
            "mode": mode,
            "key_count": int(getattr(resp, "key_count", 0) or 0),
        }



    def set_mode(self, mode) -> bool:
        """Set cluster default mode via leader."""
        def _do(stub):
            md = self._metadata()
            kwargs = {"timeout": 2.0}
            if md is not None:
                kwargs["metadata"] = md
            resp = stub.SetMode(kv_pb2.SetModeRequest(mode=mode), **kwargs)
            return bool(getattr(resp, "ok", False))
        try:
            return bool(self.call_with_retry(_do))
        except Exception:
            return False


    def put(self, key, value, mode=None):
        if isinstance(value, str):
            value = value.encode("utf-8")
        value = bytes(value) if value is not None else b""

        def _do(stub):
            md = self._metadata()
            kwargs = {"timeout": 2.0}
            if md is not None:
                kwargs["metadata"] = md
            resp = stub.Put(kv_pb2.PutRequest(key=key, value=value, mode=mode or ""), **kwargs)
            if not getattr(resp, "ok", False):
                raise RuntimeError(getattr(resp, "error", "put failed") or "put failed")
            return int(getattr(resp, "version", 0) or 0)

        try:
            return self.call_with_retry(_do)
        except Exception:
            return False

    def get(self, key):
        def _do(stub):
            md = self._metadata()
            kwargs = {"timeout": 2.0}
            if md is not None:
                kwargs["metadata"] = md
            resp = stub.Get(kv_pb2.GetRequest(key=key), **kwargs)
            if not getattr(resp, "found", False):
                err = getattr(resp, "error", "") or ""
                if err and err != "not found":
                    raise RuntimeError(err)
                return None
            return bytes(getattr(resp, "value", b"") or b"")

        try:
            return self.call_with_retry(_do)
        except Exception:
            return None

    def call_with_retry(self, fn):
        """Invoke fn(stub); rediscover leader and retry on failure."""
        last_exc = None
        for attempt in range(3):
            stub = self.get_leader_stub()
            if stub is None:
                self._leader_id = None
                self._leader_addr = None
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
                self._leader_id = None
                self._leader_addr = None
                self._leader = None
                with contextlib.suppress(Exception):
                    self.discover_leader()
                time.sleep(0.05 * (attempt + 1))
        if last_exc is not None:
            raise last_exc
        return None