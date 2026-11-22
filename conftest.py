import contextlib
import os
import sys
import time

import pytest

try:
    from node.raft import DirectTransport, RaftNode
    from node.server import NodeService
    from node.storage import Storage
except Exception:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from node.raft import DirectTransport, RaftNode  # type: ignore
    from node.server import NodeService  # type: ignore
    from node.storage import Storage  # type: ignore


@pytest.fixture
def db_path(tmp_path):
    """Convenience path for a temporary SQLite database file."""
    return str(tmp_path / "test.db")


@pytest.fixture
def cluster(tmp_path):
    n = 5
    ids = [f"node{i}" for i in range(n)]
    storages = {}
    nodes = {}
    services = {}
    for nid in ids:
        st = Storage(str(tmp_path / f"{nid}.db"))
        storages[nid] = st
        raft = RaftNode(nid, [x for x in ids if x != nid], storage=st)
        nodes[nid] = raft
        services[nid] = NodeService(nid, st, raft)
    transport = DirectTransport(nodes)
    for nd in nodes.values():
        nd.transport = transport
    for nd in nodes.values():
        nd.start()
    deadline = time.monotonic() + 5.0
    leader_id = None
    while time.monotonic() < deadline:
        for nid, nd in nodes.items():
            if nd.is_leader():
                leader_id = nid
                break
        if leader_id:
            break
        time.sleep(0.05)
    assert leader_id is not None, "no leader elected"
    time.sleep(0.5)
    leader_id = next((nid for nid, nd in nodes.items() if nd.is_leader()), leader_id)
    yield ids, nodes, storages, services, leader_id, transport
    # teardown
    for nd in nodes.values():
        with contextlib.suppress(Exception):
            nd.stop()
    for st in storages.values():
        with contextlib.suppress(Exception):
            st.close()
