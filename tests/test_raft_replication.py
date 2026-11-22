import contextlib
import os
import sys
import time

try:
    from node.raft import DirectTransport, RaftLogEntry, RaftNode
except Exception:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "node"))
    from raft import DirectTransport, RaftLogEntry, RaftNode  # type: ignore


def make_cluster(n=5, timeout_range=(0.15, 0.30)):
    ids = [f"node{i}" for i in range(n)]
    nodes = {}
    applied = {nid: [] for nid in ids}
    for nid in ids:
        peers = [x for x in ids if x != nid]
        node = RaftNode(nid, peers, election_timeout_range=timeout_range)
        node.set_apply_callback(lambda e, nid=nid: applied[nid].append(e))
        nodes[nid] = node
    transport = DirectTransport(nodes)
    for nd in nodes.values():
        nd.transport = transport
    return ids, nodes, applied


def cleanup(nodes):
    for n in nodes.values():
        with contextlib.suppress(Exception):
            n.stop()


def find_leader(nodes):
    for nid, n in nodes.items():
        if n.is_leader():
            return nid, n
    return None, None


def wait_for_leader(nodes, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for nid, n in nodes.items():
            if n.is_leader():
                return nid, n
        time.sleep(0.05)
    return find_leader(nodes)


def test_put_replicates_to_majority_and_commits():
    ids, nodes, applied = make_cluster()
    try:
        for n in nodes.values():
            n.start()
        lid, leader = wait_for_leader(nodes)
        assert leader is not None
        time.sleep(0.3)

        idx = leader.append_command(b"put k v")
        assert idx is not None

        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            if all(n.commit_index >= idx for n in nodes.values()):
                break
            time.sleep(0.05)

        for nid, n in nodes.items():
            assert n.commit_index >= idx, (nid, n.commit_index)
            assert any(getattr(e, "command", None) == b"put k v" for e in applied[nid]), nid
    finally:
        cleanup(nodes)


def test_follower_catches_up_via_backtracking():
    ids, nodes, applied = make_cluster()
    try:
        # start all but one follower
        late = "node4"
        for nid, n in nodes.items():
            if nid != late:
                n.start()
        lid, leader = wait_for_leader({k: v for k, v in nodes.items() if k != late})
        assert leader is not None
        time.sleep(0.3)

        idx = leader.append_command(b"put k v")
        assert idx is not None

        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            if leader.commit_index >= idx:
                break
            time.sleep(0.05)
        assert leader.commit_index >= idx

        # start late follower; should catch up
        nodes[late].start()
        deadline = time.monotonic() + 4.0
        while time.monotonic() < deadline:
            if nodes[late].commit_index >= idx:
                break
            time.sleep(0.05)
        assert nodes[late].commit_index >= idx, nodes[late].commit_index
        assert any(getattr(e, "command", None) == b"put k v" for e in applied[late])
    finally:
        cleanup(nodes)


def test_commit_requires_quorum():
    ids, nodes, applied = make_cluster()
    try:
        for n in nodes.values():
            n.start()
        lid, leader = wait_for_leader(nodes)
        assert leader is not None
        time.sleep(0.3)

        # make all but one follower unreachable
        reachable = None
        for nid in ids:
            if nid != lid:
                reachable = nid
                break
        shared = nodes[lid].transport.nodes
        for nid in ids:
            if nid != lid and nid != reachable:
                if nid in shared:
                    del shared[nid]
                try:
                    import node.raft as _rm
                    _rm._NODE_REGISTRY.pop(nid, None)
                except Exception:
                    pass

        idx = leader.append_command(b"put quorum test")
        assert idx is not None

        time.sleep(1.0)
        # with only leader + 1 follower, commit should not advance
        assert leader.commit_index < idx, leader.commit_index
    finally:
        cleanup(nodes)


def test_conflict_handling_truncates_stale_suffix():
    ids, nodes, applied = make_cluster(n=3)
    try:
        a, b, c = [nodes[f"node{i}"] for i in range(3)]
        # build a stale suffix on node b
        sentinel = RaftLogEntry(term=0, index=0, command=b"")
        e1 = RaftLogEntry(term=1, index=1, command=b"cmd1")
        stale = RaftLogEntry(term=1, index=2, command=b"stale")
        b.log = [sentinel, e1, stale]

        new_e2 = RaftLogEntry(term=2, index=2, command=b"new")
        term, ok = b.handle_append_entries(
            2, "leader", 1, 1, [new_e2], 0
        )
        assert ok is True
        assert [e.index for e in b.log] == [0, 1, 2]
        assert b.log[2].command == b"new"
        assert b.log[2].term == 2
    finally:
        cleanup(nodes)