import os
import sys
import time

try:
    from node.raft import DirectTransport, RaftNode
except Exception:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "node"))
    from raft import DirectTransport, RaftNode  # type: ignore


def make_cluster(n=5, timeout_range=(0.15, 0.30)):
    ids = [f"node{i}" for i in range(n)]
    nodes = {}
    for nid in ids:
        peers = [x for x in ids if x != nid]
        nodes[nid] = RaftNode(nid, peers, election_timeout_range=timeout_range)
    transport = DirectTransport(nodes)
    for nd in nodes.values():
        nd.transport = transport
    return ids, nodes


def cleanup(nodes):
    for n in nodes.values():
        try:
            n.stop()
        except Exception:
            pass


def test_election_converges_single_leader():
    ids, nodes = make_cluster()
    try:
        for n in nodes.values():
            n.start()
        time.sleep(0.5)
        leaders = [nid for nid, n in nodes.items() if n.is_leader()]
        assert len(leaders) == 1, leaders
    finally:
        cleanup(nodes)


def test_leader_failover():
    ids, nodes = make_cluster()
    try:
        for n in nodes.values():
            n.start()
        time.sleep(0.5)
        leaders = [nid for nid, n in nodes.items() if n.is_leader()]
        assert len(leaders) == 1
        old = leaders[0]
        nodes[old].stop()
        # wait for re-election
        deadline = time.monotonic() + 1.0
        new_leader = None
        while time.monotonic() < deadline:
            cur = [nid for nid, n in nodes.items() if nid != old and n.is_leader()]
            if cur:
                new_leader = cur[0]
                break
            time.sleep(0.05)
        assert new_leader is not None and new_leader != old
    finally:
        cleanup(nodes)


def test_no_split_brain_sampled():
    ids, nodes = make_cluster()
    try:
        for n in nodes.values():
            n.start()
        # sample over ~1s
        seen_leaders = set()
        max_concurrent = 0
        end = time.monotonic() + 1.0
        while time.monotonic() < end:
            cur = [nid for nid, n in nodes.items() if n.is_leader()]
            assert len(cur) <= 1, cur
            if cur:
                seen_leaders.add(cur[0])
            time.sleep(0.05)
        # at least one leader elected
        assert len(seen_leaders) >= 1
    finally:
        cleanup(nodes)


def test_higher_term_steps_down():
    ids, nodes = make_cluster()
    try:
        for n in nodes.values():
            n.start()
        time.sleep(0.5)
        leaders = [nid for nid, n in nodes.items() if n.is_leader()]
        assert len(leaders) == 1
        leader = nodes[leaders[0]]
        # inject higher term into a follower
        follower = None
        for nid, n in nodes.items():
            if nid != leaders[0]:
                follower = n
                break
        high_term = leader.current_term + 5
        with follower._lock:
            follower.current_term = high_term
            follower.voted_for = None
        # trigger step-down via direct AE handling or wait for heartbeat round
        # force leader to send heartbeats now
        leader._send_heartbeats()
        assert not leader.is_leader()
        assert leader.current_term >= high_term
    finally:
        cleanup(nodes)


def test_request_vote_log_up_to_date():
    # basic handler check
    ids, nodes = make_cluster(n=3)
    try:
        a, b, c = [nodes[f"node{i}"] for i in range(3)]
        # node0 becomes candidate-ish manually
        term, granted = b.handle_request_vote(1, "nodeX", 0, 0)
        assert granted is True
        # second vote in same term denied
        term2, granted2 = b.handle_request_vote(1, "nodeY", 0, 0)
        assert granted2 is False
    finally:
        cleanup(nodes)
