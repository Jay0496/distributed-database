"""Smoke tests for the split Raft submodules."""

from node.raft import DirectTransport, RaftLogEntry, RaftNode
from node.raft.election import RaftElection
from node.raft.failure_detector import RaftFailureDetector
from node.raft.persistence import RaftPersistence
from node.raft.replication import RaftReplication
from node.storage import Storage


def test_submodule_mixins_are_importable():
    assert issubclass(RaftNode, RaftPersistence)
    assert issubclass(RaftNode, RaftElection)
    assert issubclass(RaftNode, RaftReplication)
    assert issubclass(RaftNode, RaftFailureDetector)


def test_instantiate_with_storage_and_transport(tmp_path):
    storage = Storage(str(tmp_path / "submodules.db"))
    transport = DirectTransport()
    node = RaftNode("solo", [], storage=storage, transport=transport)
    try:
        entry = RaftLogEntry(term=1, index=1, command=b"x")
        assert entry.command == b"x"
        assert node.transport is transport
        assert node.log[0].index == 0
    finally:
        node.stop()
        storage.close()