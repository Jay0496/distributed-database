import time

import gen.kv_pb2 as kv_pb2


def test_degraded_on_peer_failure(cluster):
    ids, nodes, storages, services, leader_id, transport = cluster
    for nd in nodes.values():
        nd.failure_heartbeat_interval = 0.1
        nd.failure_threshold = 0.3
        nd.degraded_after = 0.3
    try:
        target = None
        for nid in ids:
            if nid != leader_id:
                target = nid
                break
        assert target is not None
        orig = transport.heartbeat

        def failing(peer_id, *a, **k):
            if peer_id == target:
                raise RuntimeError("boom")
            return orig(peer_id, *a, **k)

        transport.heartbeat = failing
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and target not in nodes[leader_id].degraded:
            time.sleep(0.05)
        assert target in nodes[leader_id].degraded
        # GetLeader surfaces degraded
        resp = services[leader_id].GetLeader(kv_pb2.GetLeaderRequest(), None)
        assert target in list(resp.degraded)

        # recovery
        transport.heartbeat = orig
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and target in nodes[leader_id].degraded:
            time.sleep(0.05)
        assert target not in nodes[leader_id].degraded
    finally:
        pass


def test_node_stop_marks_degraded(cluster):
    ids, nodes, storages, services, leader_id, transport = cluster
    for nd in nodes.values():
        nd.failure_heartbeat_interval = 0.1
        nd.failure_threshold = 0.3
        nd.degraded_after = 0.3
    try:
        target = None
        for nid in ids:
            if nid != leader_id:
                target = nid
                break
        assert target is not None
        nodes[target].stop()
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and target not in nodes[leader_id].degraded:
            time.sleep(0.05)
        assert target in nodes[leader_id].degraded

        # recovery after restart
        nodes[target].start()
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and target in nodes[leader_id].degraded:
            time.sleep(0.05)
        assert target not in nodes[leader_id].degraded
    finally:
        pass