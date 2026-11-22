import os
import sys

try:
    from gateway.client import GatewayClient
except Exception:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from gateway.client import GatewayClient  # type: ignore


class _Resp:
    def __init__(self, leader, mode="rs32", key_count=0, degraded=None):
        self.leader = leader
        self.mode = mode
        self.key_count = key_count
        self.degraded = degraded or []


class _Stub:
    def __init__(self, addr, state):
        self.addr = addr
        self.state = state

    def GetLeader(self, request, timeout=None):
        if self.addr == "p1" and not self.state["p1_alive"]:
            raise RuntimeError("peer down")
        return _Resp(self.state["leader"], mode=self.state["mode"], key_count=self.state["key_count"])

    def SetMode(self, request, timeout=None):
        self.state["mode"] = request.mode
        return type("R", (), {"ok": True, "mode": request.mode})()

    def Put(self, request, timeout=None):
        return type("R", (), {"ok": True, "version": 1})()

    def Get(self, request, timeout=None):
        return type("R", (), {"found": False, "value": b""})()


def test_leader_failover_rediscovery():
    peers = ["p1", "p2"]
    client = GatewayClient(peers)
    state = {"p1_alive": True, "leader": "p1", "mode": "rs32", "key_count": 3}
    client.stubs["p1"] = _Stub("p1", state)
    client.stubs["p2"] = _Stub("p2", state)

    assert client.discover_leader() == "p1"
    st = client.status()
    assert st["leader"] == "p1"
    assert st["mode"] == "rs32"
    assert st["key_count"] == 3

    # leader fails over to p2
    state["p1_alive"] = False
    state["leader"] = "p2"

    calls = {"n": 0}

    def fn(stub):
        calls["n"] += 1
        if getattr(stub, "addr", None) == "p1":
            raise RuntimeError("stale leader")
        return "ok"

    result = client.call_with_retry(fn)
    assert result == "ok"
    assert calls["n"] >= 2
    assert client.discover_leader() == "p2"

    # mode change via new leader
    assert client.set_mode("replication") is True
    assert state["mode"] == "replication"

    # put/get plumbing
    assert client.put("k", "v") == 1
    assert client.get("k") is None
